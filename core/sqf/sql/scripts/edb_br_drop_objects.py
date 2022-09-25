#!/usr/bin/env python

# @@@ START COPYRIGHT @@@
#
# (C) Copyright 2019 Esgyn Corporation
#
# @@@ END COPYRIGHT @@@
import sys
import subprocess
import logging, logging.handlers
import os
import re
import socket
import time
from optparse import OptionParser
from threading import Thread
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
    parser.add_option("-t", dest="tag_name",  metavar="TAGID",
                      help="The backup tag name")
    parser.add_option("-n", dest="dop", type="int", metavar="DOP",
                      help="The degree of parallelism or threads for the drop script. Defaults to 6 if not specified")
    parser.add_option("-d", action="store_true", dest="dryrun", default=False,
                      help="Dry run the script with fake backup list")

    (options, args) = parser.parse_args()
    return options


class SQLException(Exception):
    pass


class TagManager(object):
    def __init__(self, dop=6, dry_run=False):
        self.dry_run = dry_run
        self.dop = dop

    @staticmethod
    def _exec_cmd(cmd):
        logger.debug('executing command %s' % cmd)
        p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, executable='/bin/bash')
        stdout, stderr = p.communicate()
        msg = stdout
        if p.returncode != 0:
            msg = stderr if stderr else stdout
        return p.returncode, msg

    @staticmethod
    def _sqlci_f(sql_file):
        sqlci_cmd = 'sqlci -i %s' % sql_file
        p = subprocess.Popen(sqlci_cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, executable='/bin/bash')
        stdout, stderr = p.communicate()
        if p.returncode != 0:
            msg = stderr if stderr else stdout
            logger.error(stderr)
            raise SQLException('Failed to run sqlci command: %s' % msg)
        return stdout

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

    @staticmethod
    def _extract_user_objects(object_list):
        exclude_objects = ["SB_HISTOGRAMS", "SB_HISTOGRAM_INTERVALS", "SB_PERSISTENT_SAMPLES"]
        user_objects = []
        #object_list = object_list.replace('\n', '').replace('\t',' ').replace('  ',' ')
        lines = object_list.split('\n')
        user_objects_found = False
        user_objects_end = False
        for line in lines:
            if line.startswith('User objects'):
                user_objects_found = True
            if user_objects_found and not user_objects_end:
                if line.startswith('Restore Stats'):
                    user_objects_end = True
                if user_objects_end:
                    break
               
                #if we have objects in the exclude list, ignore them and continue with the next object 
                excl_obj_found = False
                for excl_obj in exclude_objects:
                    if excl_obj in line:
                       excl_obj_found = True
                       break
                if excl_obj_found:
                   continue

                tokens = line.split(':')
                if len(tokens) > 2:
                    user_objects.append({'object_type': tokens[0].strip(), 'object_name': tokens[2].strip()})
                else:
                    if len(tokens) > 1:
                        user_objects.append({'object_type': tokens[0].strip(), 'object_name': tokens[1].strip()})
        return user_objects

    def _get_all_objects_in_backup(self, options):
        command = "restore trafodion, tag '%s', show objects;" % options.tag_name
        if self.dry_run:
            fake_result = '''
EsgynDB Advanced Conversational Interface 2.7.0
Copyright (c) 2015-2019 Esgyn Corporation
>>restore trafodion, tag 'N003_00212428781724503417', show objects;

MetaData Objects
================

TRAF_RSRVD_3:TRAFODION._BACKUP_N003_00212428781724503417_.COLUMNS
TRAF_RSRVD_3:TRAFODION._BACKUP_N003_00212428781724503417_.COLUMN_PRIVILEGES
TRAF_RSRVD_3:TRAFODION._BACKUP_N003_00212428781724503417_.INDEXES
TRAF_RSRVD_3:TRAFODION._BACKUP_N003_00212428781724503417_.KEYS
TRAF_RSRVD_3:TRAFODION._BACKUP_N003_00212428781724503417_.LIBRARIES
TRAF_RSRVD_3:TRAFODION._BACKUP_N003_00212428781724503417_.LIBRARIES_USAGE
TRAF_RSRVD_3:TRAFODION._BACKUP_N003_00212428781724503417_.OBJECTS
TRAF_RSRVD_3:TRAFODION._BACKUP_N003_00212428781724503417_.OBJECT_PRIVILEGES
TRAF_RSRVD_3:TRAFODION._BACKUP_N003_00212428781724503417_.REF_CONSTRAINTS
TRAF_RSRVD_3:TRAFODION._BACKUP_N003_00212428781724503417_.ROUTINES
TRAF_RSRVD_3:TRAFODION._BACKUP_N003_00212428781724503417_.SB_HISTOGRAMS
TRAF_RSRVD_3:TRAFODION._BACKUP_N003_00212428781724503417_.SB_HISTOGRAM_INTERVALS
TRAF_RSRVD_3:TRAFODION._BACKUP_N003_00212428781724503417_.SB_PERSISTENT_SAMPLES
TRAF_RSRVD_3:TRAFODION._BACKUP_N003_00212428781724503417_.SCHEMA_PRIVILEGES
TRAF_RSRVD_3:TRAFODION._BACKUP_N003_00212428781724503417_.SEQ_GEN
TRAF_RSRVD_3:TRAFODION._BACKUP_N003_00212428781724503417_.TABLES
TRAF_RSRVD_3:TRAFODION._BACKUP_N003_00212428781724503417_.TABLE_CONSTRAINTS
TRAF_RSRVD_3:TRAFODION._BACKUP_N003_00212428781724503417_.TEXT
TRAF_RSRVD_3:TRAFODION._BACKUP_N003_00212428781724503417_.UNIQUE_REF_CONSTR_USAGE
TRAF_RSRVD_3:TRAFODION._BACKUP_N003_00212428781724503417_.VIEWS
TRAF_RSRVD_3:TRAFODION._BACKUP_N003_00212428781724503417_.VIEWS_USAGE

User objects
============

TABLE:   TRAFODION.DBMGR_TEST.ADDRESSES
TABLE:   TRAFODION.DBMGR_TEST.CARD_DETAILS
TABLE:   TRAFODION.DBMGR_TEST.CUSTOMERS
TABLE:   TRAFODION.DBMGR_TEST.INVENTORIES
TABLE:   TRAFODION.DBMGR_TEST.LOBDescChunks__07269567851603674403_0006
TABLE:   TRAFODION.DBMGR_TEST.LOBDescHandle__07269567851603674403_0006
TABLE:   TRAFODION.DBMGR_TEST.LOBMD__07269567851603674403
TABLE:   TRAFODION.DBMGR_TEST.LOGON
TABLE:   TRAFODION.DBMGR_TEST.ORDERENTRY_METADATA
TABLE:   TRAFODION.DBMGR_TEST.ORDERS
TABLE:   TRAFODION.DBMGR_TEST.ORDER_ITEMS
TABLE:   TRAFODION.DBMGR_TEST.PRODUCT_DESCRIPTIONS
TABLE:   TRAFODION.DBMGR_TEST.PRODUCT_INFORMATION
TABLE:   TRAFODION.DBMGR_TEST.SB_HISTOGRAMS
TABLE:   TRAFODION.DBMGR_TEST.SB_HISTOGRAM_INTERVALS
TABLE:   TRAFODION.DBMGR_TEST.SB_PERSISTENT_SAMPLES
TABLE:   TRAFODION.DBMGR_TEST.T1_S
TABLE:   TRAFODION.DBMGR_TEST.WAREHOUSES
TABLE:   TRAFODION.INSURANCE.APP_INFO
TABLE:   TRAFODION.INSURANCE.MEMBER_ENROLLMENT_TABLE
TABLE:   TRAFODION.INSURANCE.MEMBER_INPUT_TABLE
TABLE:   TRAFODION.INSURANCE.MEMBER_PROGRAM_TABLE
TABLE:   TRAFODION.INSURANCE.MEMBER_TABLE
TABLE:   TRAFODION.INSURANCE.SB_HISTOGRAMS
TABLE:   TRAFODION.INSURANCE.SB_HISTOGRAM_INTERVALS
TABLE:   TRAFODION.INSURANCE.SB_PERSISTENT_SAMPLES
TABLE:   TRAFODION.INSURANCE.ZIPCODES
TABLE:   TRAFODION.IO_BLITZZ.SB_HISTOGRAMS
TABLE:   TRAFODION.IO_BLITZZ.SB_HISTOGRAM_INTERVALS
TABLE:   TRAFODION.IO_BLITZZ.SB_PERSISTENT_SAMPLES
TABLE:   TRAFODION.IO_BLITZZ.replication_recovery_metadata
TABLE:   TRAFODION.SOE11.ADDRESSES
TABLE:   TRAFODION.SOE11.CARD_DETAILS
TABLE:   TRAFODION.SOE11.CUSTOMERS
TABLE:   TRAFODION.SOE11.INVENTORIES
TABLE:   TRAFODION.SOE11.LOGON
TABLE:   TRAFODION.SOE11.ORDERENTRY_METADATA
TABLE:   TRAFODION.SOE11.ORDERS
TABLE:   TRAFODION.SOE11.ORDER_ITEMS
TABLE:   TRAFODION.SOE11.PRODUCT_DESCRIPTIONS
TABLE:   TRAFODION.SOE11.PRODUCT_INFORMATION
TABLE:   TRAFODION.SOE11.SB_HISTOGRAMS
TABLE:   TRAFODION.SOE11.SB_HISTOGRAM_INTERVALS
TABLE:   TRAFODION.SOE11.SB_PERSISTENT_SAMPLES
TABLE:   TRAFODION.SOE11.WAREHOUSES
TABLE:   TRAF_1500101:TRAFODION.S1_SCH.DEPT
TABLE:   TRAF_1500101:TRAFODION.S1_SCH.EMP
TABLE:   TRAF_1500101:TRAFODION.S1_SCH.SALES_RECORDS
TABLE:   TRAF_1500101:TRAFODION.S1_SCH.SB_HISTOGRAMS
TABLE:   TRAF_1500101:TRAFODION.S1_SCH.SB_HISTOGRAM_INTERVALS
TABLE:   TRAF_1500101:TRAFODION.S1_SCH.SB_PERSISTENT_SAMPLES
TABLE:   TRAF_1500101:TRAFODION.SENTRYUSER2_SCH.EMPLOYEE
TABLE:   TRAF_1500101:TRAFODION.SENTRYUSER2_SCH.MainTable
TABLE:   TRAF_1500101:TRAFODION.SENTRYUSER2_SCH.ODETAIL
TABLE:   TRAF_1500101:TRAFODION.SENTRYUSER2_SCH.ORDERS
TABLE:   TRAF_1500101:TRAFODION.SENTRYUSER2_SCH.SB_HISTOGRAMS
TABLE:   TRAF_1500101:TRAFODION.SENTRYUSER2_SCH.SB_HISTOGRAM_INTERVALS
TABLE:   TRAF_1500101:TRAFODION.SENTRYUSER2_SCH.SB_PERSISTENT_SAMPLES
TABLE:   TRAF_1500101:TRAFODION.SENTRYUSER2_SCH.T1
TABLE:   TRAF_1500101:TRAFODION.s1_schema.DEPT
TABLE:   TRAF_1500101:TRAFODION.s1_schema.SB_HISTOGRAMS
TABLE:   TRAF_1500101:TRAFODION.s1_schema.SB_HISTOGRAM_INTERVALS
TABLE:   TRAF_1500101:TRAFODION.s1_schema.SB_PERSISTENT_SAMPLES
TABLE:   TRAF_1500101:TRAFODION.s1_schema.emp
TABLE:   TRAF_RSRVD_3:TRAFODION.SEABASE.LOGS_UDF_T
TABLE:   TRAF_RSRVD_3:TRAFODION.SEABASE.R1
TABLE:   TRAF_RSRVD_3:TRAFODION.SEABASE.SB_HISTOGRAMS
TABLE:   TRAF_RSRVD_3:TRAFODION.SEABASE.SB_HISTOGRAM_INTERVALS
TABLE:   TRAF_RSRVD_3:TRAFODION.SEABASE.SB_PERSISTENT_SAMPLES
TABLE:   TRAF_RSRVD_3:TRAFODION.SEABASE.SYNC_T1
TABLE:   TRAF_RSRVD_3:TRAFODION.SEABASE.TEST11
TABLE:   TRAF_RSRVD_3:TRAFODION.SEABASE.TVKAT
INDEX:   TRAFODION.DBMGR_TEST.ORDERS_DT_IX
INDEX:   TRAFODION.SOE11.ADD_CUST_FK
INDEX:   TRAFODION.SOE11.INVENTORIES_PRODUCT_ID_FK
INDEX:   TRAFODION.SOE11.INVENTORIES_WAREHOUSES_FK
INDEX:   TRAFODION.SOE11.ORDERS_CUSTOMER_ID_FK
INDEX:   TRAFODION.SOE11.ORDER_ITEMS_ORDER_ID_FK
INDEX:   TRAFODION.SOE11.ORDER_ITEMS_PRODUCT_ID_FK
INDEX:   TRAF_1500101:TRAFODION.S1_SCH.FK_DEPTNO
INDEX:   TRAF_1500101:TRAFODION.SENTRYUSER2_SCH.EMPLOYEE_956331636_4185
INDEX:   TRAF_1500101:TRAFODION.SENTRYUSER2_SCH.XCUSTNAM
INDEX:   TRAF_1500101:TRAFODION.SENTRYUSER2_SCH.XORDCUS
INDEX:   TRAF_1500101:TRAFODION.SENTRYUSER2_SCH.XORDREP
INDEX:   TRAF_1500101:TRAFODION.s1_schema.FK_DEPTNO
VIEW:    TRAFODION.DBMGR_TEST.INVENTORIES_VW
VIEW:    TRAFODION.INSURANCE.NORMALIZED_APP_INFO
VIEW:    TRAFODION.S1_SCH.DEPT_VW
VIEW:    TRAFODION.S1_SCH.VIEW1
VIEW:    TRAFODION.SEABASE.SYNC_T1_VW
VIEW:    TRAFODION.s1_schema.emp_vw
ROUTINE: TRAFODION.DBMGR_TEST.OPTEXPSTATS
ROUTINE: TRAFODION.DBMGR_TEST.TESTSPJ1
ROUTINE: TRAFODION.INSURANCE.CLAIMPROCESSRULES
ROUTINE: TRAFODION.INSURANCE.DATAENTRYRULES
ROUTINE: TRAFODION.S1_SCH.OPTEXPSTATS
ROUTINE: TRAFODION.S1_SCH.TESTSPJ1
LIBRARY: TRAFODION.DBMGR_TEST.EXPLIB
LIBRARY: TRAFODION.DBMGR_TEST.SPJLIB
LIBRARY: TRAFODION.INSURANCE.DROOLSLIB
LIBRARY: TRAFODION.S1_SCH.SPJLIB
LIBRARY: TRAFODION.S1_SCH.testlib

Restore Stats
=============
  Total:     123
  Metadata:  21
  Tables:    72
  Indexes:   13
  Views:     6
  Routines:  6
  Libraries: 5


--- SQL operation complete.
>>exit;

End of MXCI Session

'''
            return fake_result
        logger.debug('Getting backup object list ...')

        try:
            result = self._sqlci(command)
            if "*** ERROR" in result:
                logger.error("Get backup object list failed. %s" % self._extract_sql_error(result))
            else:
                logger.debug('Get backup object list completed successfully')
            return result
        except Exception as e:
            logger.error('Get backup list failed. %s' % e)
            return ''

    def drop_objects(self, options):
        object_list = self._extract_user_objects(self._get_all_objects_in_backup(options))
        tbl_lib_list = [obj for obj in object_list if obj['object_type'] == 'TABLE' or obj['object_type'] == 'LIBRARY']
        tbl_count = len(tbl_lib_list)  # total table counts
        tbl_piece = tbl_count / self.dop + 1  # table count per one thread

        threads = []
        count = 0
        while count < self.dop:
            # print '*** table index from %d to %d' % (count*tbl_piece, (count+1)*tbl_piece)
            tbl_list = tbl_lib_list[count*tbl_piece:(count+1)*tbl_piece]
            threads.append(Thread(target=self._drop, args=(tbl_list, count,)))
            count += 1

        logger.debug('Start %d threads to drop backup objects ...' % self.dop)
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

    def _drop(self, tbl_list, count):
        file_name = '/tmp/drop_object_%d_%d' % (count, time.time())
        with open(file_name, 'w') as f:
            for tbl in tbl_list:
                obj_name_parts = tbl['object_name'].split(".")
                n_length = len(obj_name_parts)
                for i in range(n_length):
                    if not obj_name_parts[i].startswith("\""):
                       obj_name_parts[i] = "\"" + obj_name_parts[i] + "\""
                obj_name = ".".join(obj_name_parts)
                f.write('DROP %s %s CASCADE;\n' % (tbl['object_type'], obj_name))

        logger.debug('Drop objects started on thread %d' % count)
        try:
            result = self._sqlci_f(file_name)
            errorLogName = "%s/br_drop_objects_%d_%d.log" % (traf_log, count, time.time())
            errorLogFile = open(errorLogName, 'w') 
            print>>errorLogFile,result
            errorLogFile.close()
            if "*** ERROR" in result:
                logger.error('One or more drop commands failed. Check the log file %s for details' % errorLogName)
        except Exception as e:
            logger.error('Drop objects failed on thread %d: %s' % (count, e))

        os.remove(file_name)


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
    if not options.dop:
       options.dop = 6 
    try:
        tm = TagManager(dop=options.dop, dry_run=options.dryrun)
        tm.drop_objects(options)
    except Exception as e:
        logger.error("Error :" + str(e))
        err(str(e))


if __name__ == '__main__':
    main()

