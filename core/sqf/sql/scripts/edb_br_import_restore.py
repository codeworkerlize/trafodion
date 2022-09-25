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
import socket
from optparse import OptionParser
from datetime import timedelta, datetime

BACKUP_IMPORT_SUCCESS = 'BACKUP_IMPORT_SUCCESS'
BACKUP_IMPORT_ERROR = 'BACKUP_IMPORT_ERROR'
BACKUP_RESTORE_SUCCESS = 'BACKUP_RESTORE_SUCCESS'
BACKUP_RESTORE_ERROR = 'BACKUP_RESTORE_ERROR'

traf_log = os.environ.get("TRAF_LOG")
logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)

# Create the Handler for logging data to a file
logger_handler = logging.handlers.RotatingFileHandler(traf_log + '/scheduled_br.log', 'a', 10000000, 10)
logger_handler.setLevel(logging.INFO)
 
# Create a Formatter for formatting the log messages
customFields = { 'component': 'BREI_ACTION', 'host': socket.gethostname() }
logger_formatter = logging.Formatter('%(asctime)s, %(levelname)s, %(component)s, Node: %(host)s,, PIN: %(process)d, Process Name: %(processName)s,,,%(message)s')
 
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
    usage += '  Backup Import/Restore action script.'
    parser = OptionParser(usage=usage)
    parser.add_option("-i", action="store_true", dest="importb", default=False,
                      help="Import a backup")
    parser.add_option("-r", action="store_true", dest="restore", default=False,
                      help="Restore a backup")
    parser.add_option("-f", action="store_true", dest="force", default=False,
                      help="Force run the script locally")
    parser.add_option("-t", dest="tag_name", metavar="TAGID",
                      help="The backup tag name")
    parser.add_option("-n", dest="days_ago", type="int", metavar="DAYSAGO",
                      help="The number of days before current day. If backup tag not specified, the last exported tag from this day would be imported.")
    parser.add_option("-d", dest="importdir", metavar="IMPORTDIR",
                      help="Specify the source directory name for import of the backup")
    parser.add_option("-a", dest="effective_after", metavar="EFFECTIVE_AFTER",
                      help="Specify the effective date time after which this script should run. The date should be in 'YYYY-MM-DD HH:mm:ss' format")

    (options, args) = parser.parse_args()
    return options


class SQLException(Exception):
    pass


class RestoreTagManager(object):
    def _exec_cmd(self, cmd):
        logger.debug('executing command %s' % cmd)
        p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, executable='/bin/bash')
        stdout, stderr = p.communicate()
        msg = stdout
        if p.returncode != 0:
            msg = stderr if stderr else stdout
        return p.returncode, msg

    def _sqlci(self, cmd):
        sqlci_cmd = 'echo "%s" | sqlci' % cmd.replace("\"", "\\\"")
        p = subprocess.Popen(sqlci_cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, executable='/bin/bash')
        stdout, stderr = p.communicate()
        if p.returncode != 0:
            msg = stderr if stderr else stdout
            logger.error(stderr)
            raise SQLException('Failed to run sqlci command: %s' % msg)
        return stdout
   
    def _extract_sql_error(self, errorstr):
        errorstr = errorstr.replace('\n', '').replace('\t',' ').replace('  ',' ')
        lines = re.findall(r'\>\>(.*?)\>\>', errorstr)
        if lines:
            return lines[0]
        return ''
 
    def perform_restore(self, options):
        logger.info('Restore of backup %s started' % (options.tag_name))
        command = "restore trafodion, tag '%s';" % (options.tag_name)
        try:
            result = self._sqlci(command)
            if "*** ERROR" in result:
                logger.error("%s: Restore backup %s failed. %s" % (BACKUP_RESTORE_ERROR, options.tag_name, self._extract_sql_error(result)))
                return_code = self._cleanup_backup(options.tag_name)
                raise Exception("Restore backup %s failed. Check log file for details." % (options.tag_name))

            logger.info('%s: Restore backup %s completed successfully.' % (BACKUP_RESTORE_SUCCESS, options.tag_name))

            result = self._sqlci("cleanup metadata;")
            if "*** ERROR" in result:
                logger.error("Tag Name: %s, cleanup metadata failed.", options.tag_name)
                return 0

            logger.info('Tag Name: %s, cleanup metadata successfully.', options.tag_name)
            return 0
        except Exception as e:
            raise e
 
    def _perform_import(self, options):
        return_code = 0
        if not options.tag_name:
            return_code, options.tag_name = self._fetch_last_regular_backup(options)

        if return_code == 1:
            return 1

        if not options.tag_name:
            error_msg = 'Cannot determine tag for the import'
            raise Exception(error_msg)

        logger.info('Import of backup %s from %s started' % (options.tag_name, options.importdir))
        command = "import backup from location '%s', tag '%s';" % (options.importdir, options.tag_name)

        try:
            result = self._sqlci(command)
            if "*** ERROR" in result:
                logger.error("%s: Import of backup %s failed. %s" % (BACKUP_IMPORT_ERROR, options.tag_name, self._extract_sql_error(result)))
                raise Exception("Import of backup %s failed. Check log for details." % (options.tag_name))
            else:
                logger.info('%s: Import of backup %s completed successfully' % (BACKUP_IMPORT_SUCCESS, options.tag_name))
                return 0
        except Exception as e1:
            raise e1

    def _fetch_last_regular_backup(self, options):
        logger.info('Fetching last successful regular backup')
        yesterday_date = (datetime.now() - timedelta(days=options.days_ago)).strftime("%Y-%m-%d")
        try:
            retcode, backup_tag = self._exec_cmd("hdfs dfs -ls /user/trafodion/backups/.export_status/ | grep %s | sort -k5,6 -r | head -1 | awk '{print $8}'" % (yesterday_date))
            if retcode == 0:
                if backup_tag:
                    tokens = backup_tag.split('/')
                    return 0, tokens[-1].replace('\n', '')
                else:
                    raise Exception('Cannot find a valid backup for date ' + yesterday_date)
        except Exception as e1:
            logger.error('Fetch tag of last backup failed : %s' % e1)
            raise e1

    def remove_status_file(self, backup_tag):
        try:
            retcode, msg = self._exec_cmd("hdfs dfs -rm -f /user/trafodion/backups/.export_status/%s" % (backup_tag))
            if retcode == 0:
                logger.debug('Removed export status file for %s' % backup_tag)
                return 0
            else:
                logger.error('Failed to remove export status file : ' + msg)
        except Exception as e1:
            logger.debug('Failed to remove export status file : %s' % (e1))

        return 1

    def _cleanup_backup(self, backup_tag):
        logger.info('Cleanup backup %s started' % backup_tag)
        command = "cleanup backup , tag '%s';" % (backup_tag)
        try:
            result = self._sqlci(command)
            if "*** ERROR" in result:
                logger.error("Cleanup backup %s failed. %s" % ( backup_tag, self._extract_sql_error(result)))
                return 1
            else:
                logger.info('Cleanup of backup %s completed successfully' % (backup_tag))
                return 0
        except Exception as e1:
            logger.error('Cleanup of backup %s failed' % (backup_tag))
            return 1

    def import_restore(self, options):
        return_code = 1
        # Step 1: Perform an import of the backup
        if options.importdir:
            return_code = self._perform_import(options)

        # Step 2: If import from step 1 succeeded, restore the backup
        if return_code == 0 and options.restore:
            return_code = self.perform_restore(options)

        return return_code

def main():
    options = get_options()
    if options.importdir:
         if "$DATE" in options.importdir:
             curr_date = datetime.now()
             dir_vals = options.importdir.split("/")
             for i, dval in enumerate(dir_vals):
                 if "$DATE" in dval:
                     m = re.match("\$DATE([-|+]{1}[0-9]+$)?", dval)
                     if m:
                         selected_date = curr_date
                         if m.group(1):
                             v = m.group(1)
                             if v[0] == '+':
                                 selected_date = curr_date + timedelta(days=int(v[1:]))
                             else:
                                 selected_date = curr_date - timedelta(days=int(v[1:]))

                         dir_vals[i] = selected_date.strftime('%Y-%m-%d')
                     else:
                         err('Invalid $DATE string %s' % dval)

             options.importdir = '/'.join(dir_vals)

    # If trigger by cron, we only can run this action from one node
    # Check if the scripts needs to run on this node
    if not options.force and not should_i_run():
        logger.debug("Current node is not active node. Skipping job")
        sys.exit(-1)

    if options.effective_after:
       try:
          dt = datetime.strptime(options.effective_after, '%Y-%m-%d %H:%M:%S')
          if dt > datetime.now():
             logger.info("The schedule is only effective after %s. Skipping job" % options.effective_after)
             sys.exit(-1)
       except Exception as e:
          logger.error("Error :" + str(e))
          sys.exit(-1)

    def err(msg):
        sys.stderr.write('[ERROR] ' + msg + '\n')
        sys.exit(1)

    ### validate parameters ###
    val = 0
    if options.importb: val += 1
    if options.restore: val += 1

    if options.days_ago is None:
       options.days_ago = 1

    try:
        tm = RestoreTagManager()

        if options.importb:
            if not options.importdir:
                err("Specify the source directory location using the -d option")

            return_code = tm.import_restore(options)
            if return_code == 0:
                tm.remove_status_file(options.tag_name)
        else:
            if options.restore:
                if not options.tag_name:
                    err("Specify the backup tag name using the -t option")

            tm.perform_restore(options)

    except Exception as e:
        logger.error("Error :" + str(e))
        err(str(e))

            
if __name__ == '__main__':
    main()

