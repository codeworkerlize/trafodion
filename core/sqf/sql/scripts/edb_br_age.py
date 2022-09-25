#!/usr/bin/env python

# @@@ START COPYRIGHT @@@
#
# (C) Copyright 2019-2020 Esgyn Corporation
#
# @@@ END COPYRIGHT @@@
import sys
import subprocess
import logging, logging.handlers
import os
import re
import time
import socket
from optparse import OptionParser
from datetime import datetime, date, timedelta
try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET

traf_conf = os.environ.get("TRAF_CONF") if os.environ.get("TRAF_CONF") else '/etc/trafodion/conf'
traf_log = os.environ.get("TRAF_LOG") if os.environ.get("TRAF_CONF") else '/var/log/trafodion'
traf_var = os.environ.get("TRAF_VAR") if os.environ.get("TRAF_CONF") else '/var/lib/trafodion'
trafodion_site_xml = traf_conf + '/trafodion-site.xml'

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)

# Create the Handler for logging data to a file
logger_handler = logging.handlers.RotatingFileHandler(traf_log + '/scheduled_br.log', 'a', 10000000, 10)
logger_handler.setLevel(logging.INFO)


# Create a Formatter for formatting the log messages
customFields = {'component': 'BREI_ACTION', 'host': socket.gethostname()}
logger_formatter = logging.Formatter('%(asctime)s, %(levelname)s, %(component)s, Node: %(host)s,, PIN: %(process)d, Process Name: %(processName)s,,, %(message)s')

# Add the Formatter to the Handler
logger_handler.setFormatter(logger_formatter)

# Add the Handler to the Logger
logger.addHandler(logger_handler)

# Add the custom fields to the logger format
logger = logging.LoggerAdapter(logger, customFields)


def should_i_run():
    # We only need to run this script from one node in the cluster
    # idtmsrv process runs only on one node at any given time
    # So we check if current node has idtmsrv running and decide to run if true
    check_cmd = "sqps|grep idtmsrv|cut -d',' -f1|cut -d' ' -f2 && trafconf -mynid"
    p = subprocess.Popen(check_cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, executable='/bin/bash')
    stdout, stderr = p.communicate()
    if stdout:
        lines = stdout.strip().split("\n")
        if len(lines) != 3:
            return False

        if int(lines[0]) == int(lines[2]):
            return True
        else:
            return False
    else:
        return False


def get_options():
    usage = 'usage: %prog [options]\n'
    usage += '  Backup auto clean up script.'
    parser = OptionParser(usage=usage)
    parser.add_option("-f", action="store_true", dest="force", default=False,
                      help="Force run the script locally")
    parser.add_option("-d", action="store_true", dest="dryrun", default=False,
                      help="Dry run the script with fake backup list")
    parser.add_option("-s", action="store_true", dest="strict", default=False,
                      help="Strictly according to time")

    (options, args) = parser.parse_args()
    return options


class ParseXML(object):
    """ handle *-site.xml with format
        <property><name></name><value></value></proerty>
    """
    def __init__(self, xml_src, src_type='file'):
        if src_type == 'file':
            self.__xml_file = xml_src
            if not os.path.exists(self.__xml_file): logger.error('Cannot find xml file %s' % self.__xml_file)
            try:
                self._tree = ET.parse(self.__xml_file)
            except Exception as e:
                logger.error('failed to parse xml: %s' % e)

        if src_type == 'string':
            self.__xml_string = xml_src
            try:
                self._tree = ET.ElementTree(ET.fromstring(self.__xml_string))
            except Exception as e:
                logger.error('failed to parse xml: %s' % e)

        if src_type == 'tree':
            self._tree = xml_src

        self._root = self._tree.getroot()
        self._properties = self._root.findall('property')
        # name, value list
        self._nvlist = [[elem.text for elem in p] for p in self._properties]

    def get_property(self, name):
        try:
            return [x[1] for x in self._nvlist if x[0] == name][0]
        except:
            return ''


class SQLException(Exception):
    pass


class TagManager(object):
    def __init__(self, dry_run=False, strict=False):
        self.dry_run = dry_run
        self.strict = strict

    @staticmethod
    def _sqlci(cmd):
        sqlci_cmd = 'echo "%s" | sqlci' % cmd.replace("\"", "\\\"")
        p = subprocess.Popen(sqlci_cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, executable='/bin/bash')
        stdout, stderr = p.communicate()
        if p.returncode != 0:
            msg = stderr if stderr else stdout
            logger.error(stderr)
            raise SQLException('Failed to run sqlci command: %s' % msg)
        return stdout

    @staticmethod
    def _extract_sql_error(errorstr):
        errorstr = errorstr.replace('\n', '').replace('\t',' ').replace('  ',' ')
        lines = re.findall(r'\>\>(.*?)\>\>', errorstr)
        if lines:
            return lines[0]
        return ''

    def _get_aging_ts(self, result):
        backup_tags = {}
        # get all backup tags, timestamps from the SQL output
        for line in result.split('\n'):
            try:
                re.search('\d{4}-\d{2}-\d{2}', line).groups()
                tag = line.split()[0]
                ts = line.split()[1]
                backup_tags[tag] = ts
            except AttributeError:
                pass

        retention_days = 3  # default value
        retention_count = 3
        # get retention policy from config file
        if not self.dry_run:
            p = ParseXML(trafodion_site_xml)
            period = p.get_property('backup.retention.period')
            rcount = p.get_property('backup.retention.count')
            if period and period.isdigit():
                retention_days = int(period)
            if rcount and rcount.isdigit():
                retention_count = int(rcount)

        logger.debug('Retention days for backup: %d' % retention_days)
        # find all tags which need to be aged
        aging_tags = {}
        tags_to_search = sorted(backup_tags.items(), key=lambda x: x[1], reverse=True)
        curr_retain_count = 0 
        for tag, ts in tags_to_search:
            unix_ts = time.mktime(time.strptime(ts, '%Y-%m-%d:%H:%M:%S'))
            unix_dt = datetime.fromtimestamp(unix_ts).date()

            current_ts = time.time()
            aging_ts = current_ts - retention_days * 24 * 60 * 60
            aging_dt = datetime.fromtimestamp(aging_ts).date()

            #print ts, unix_dt, aging_dt
            if unix_dt <= aging_dt and curr_retain_count >= retention_count:
                aging_tags[tag] = ts
            else:
                curr_retain_count = curr_retain_count + 1
        
        tags_to_delete = sorted(aging_tags.items(), key=lambda x: x[1], reverse=True)
        return tags_to_delete

    # According to the specified time after the next regular backup of time to decide whether to delete
    def _get_aging_ts_nature(self, result):
        backup_tags = []
        # get all backup tags, timestamps from the SQL output
        for line in result.split('\n'):
            try:
                re.search('\d{4}-\d{2}-\d{2}', line).groups()
                tag = line.split()[0]
                ts = line.split()[1]
                op = line.split()[3]
                backup_tags.append([tag, ts, op])
            except AttributeError:
                pass

        backup_tags.sort(key=lambda x: x[1])

        retention_days = 3  # default value
        # get retention days from config file
        if not self.dry_run:
            p = ParseXML(trafodion_site_xml)
            period = p.get_property('backup.retention.period')
            if period and period.isdigit():
                retention_days = int(period)

        logger.debug('Retention days for backup: %d' % retention_days)
        # find all tags which need to be aged

        aging_ts = date.today() - timedelta(retention_days - 1)
        aging_str = time.strftime("%Y-%m-%d:%H:%M:%S", aging_ts.timetuple())
        aging_tags = {}
        for backup in backup_tags:
            tag = backup[0]
            ts = backup[1]
            op = backup[2]
            if ts < aging_str and op.upper().__contains__("REGULAR"):
                aging_tags[tag] = ts

        tags_to_delete = sorted(aging_tags.items(), key=lambda x: x[1], reverse=True)
        return tags_to_delete

    def _get_backup_list(self):
        command = 'get all backup snapshots, show details;'
        if self.dry_run:
            fake_result = '''
EsgynDB Advanced Conversational Interface 2.7.0
Copyright (c) 2015-2019 Esgyn Corporation
>>
BackupTag                  BackupTime           BackupStatus  BackupOperation
===================================================================================

tag1_00212429427368907709  2020-10-09:10:16:08  VALID         REGULAR
tag2_00212429427607384186  2020-10-09:07:20:07  VALID         REGULAR
tag3_00212429427607384186  2020-10-09:07:20:07           REGULAR
tag4_00212429427607384186  2020-10-09:07:20:07  VALID         INCREMENTAL
tag5_00212429427607384186  2020-10-09:09:20:07  VALID         INCREMENTAL(IMPORTED)
tag6_00212429427607384186  2020-10-09:07:28:07  VALID         INCREMENTAL(IMPORTED)
tag7_00212429427607384186  2020-10-09:10:20:07  VALID         INCREMENTAL
tag8_00212429427607384186  2020-10-09:05:20:07  VALID         INCREMENTAL
tag9_00212429427607384186  2020-10-09:07:20:01  VALID         INCREMENTAL(IMPORTED)
tag10_00212429427607384186  2020-10-10:07:20:07  VALID         INCREMENTAL
tag11_00212429427607384186  2020-10-10:05:20:07  VALID         INCREMENTAL

tag12_00212429427607384186  2020-10-10:15:20:07  VALID         REGULAR
tag13_00212429427607384186  2020-10-10:15:21:07  VALID         INCREMENTAL
tag14_00212429427607384186  2020-10-10:16:20:07  VALID         INCREMENTAL

--- SQL operation complete.
>>

End of MXCI Session
'''
            return fake_result
        logger.debug('Getting backup list from SQL outputs ...')
        try:
            result = self._sqlci(command)
            if "*** ERROR" in result:
                logger.error("Get backup list failed. %s" % self._extract_sql_error(result))
            else:
                logger.debug('Get backup list completed successfully')
            return result
        except Exception as e:
            logger.error('Get backup list failed. %s' % e)
            return ''

    def drop_backup(self):
        if self.strict:
            backup_tags = self._get_aging_ts(self._get_backup_list())
        else:
            backup_tags = self._get_aging_ts_nature(self._get_backup_list())
        for backup_tag, backup_ts in backup_tags:
            if self.dry_run:
                logger.info('[DRY-RUN]: Drop backup on [tag: %s, backup time: %s]' % (backup_tag, backup_ts))
            else:
                logger.info('Drop backup on [tag: %s, backup time: %s] started' % (backup_tag, backup_ts))
                command = "drop backup tag, tag '%s', force;" % backup_tag
                try:
                    result = self._sqlci(command)
                    if "*** ERROR" in result:
                        logger.error("Drop backup on tag %s failed. %s" % (backup_tag, self._extract_sql_error(result)))
                    else:
                        logger.info('Drop backup on tag %s completed successfully' % backup_tag)

                        zk_logger = traf_log + '/cleanZnode.log'
                        tag_time = backup_tag[-20:]
                        command = "sh cleanZKNodes -tt %s -op checktag >> %s 2>&1" % (tag_time, zk_logger)
                        os.system(command)
                except Exception as e:
                    logger.error('Exception: Drop backup on tag %s failed. %s' % (backup_tag, e))


def err(msg):
    sys.stderr.write('[ERROR] ' + msg + '\n')
    sys.exit(1)


def main():
    options = get_options()

    logger.debug('*** script started')
    # If trigger by cron, we only can run this action from one node
    # Check if the scripts needs to run on this node
    if not options.force and not should_i_run() and not options.dryrun:
        logger.debug("Current node is not active node. Skipping job")
        sys.exit(-1)

    try:
        tm = TagManager(dry_run=options.dryrun, strict=options.strict)
        tm.drop_backup()
    except Exception as e:
        logger.error("Error :" + str(e))
        err(str(e))

            
if __name__ == '__main__':
    main()
