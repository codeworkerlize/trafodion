#!/usr/bin/env python

# @@@ START COPYRIGHT @@@
#
# (C) Copyright 2018-2021 Esgyn Corporation
#
# @@@ END COPYRIGHT @@@
import sys
import subprocess
import logging, logging.handlers
import os
import re
import socket
import time
from datetime import datetime, timedelta
from optparse import OptionParser

REG_BACKUP_SUCCESS = 'REG_BACKUP_SUCCESS'
REG_BACKUP_ERROR = 'REG_BACKUP_ERROR'
INCR_BACKUP_SUCCESS = 'INCR_BACKUP_SUCCESS'
INCR_BACKUP_ERROR = 'INCR_BACKUP_ERROR'
REG_EXPORT_SUCCESS = 'REG_EXPORT_SUCCESS'
REG_EXPORT_ERROR = 'REG_EXPORT_ERROR'
INCR_EXPORT_SUCCESS = 'INCR_EXPORT_SUCCESS'
INCR_EXPORT_ERROR = 'INCR_EXPORT_ERROR'

LAST_SUCCEEDED_REG_BACKUP_FILENAME = '.last_succeeded_reg_backup_time'
LAST_SUCCEEDED_REG_BACKUP_HDFS_LOC = '/user/trafodion/backups/' + LAST_SUCCEEDED_REG_BACKUP_FILENAME

traf_log = os.environ.get("TRAF_LOG")
traf_var = os.environ.get("TRAF_VAR")
traf_home = os.environ.get("TRAF_HOME")

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

#Add the custom fields to the logger format
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


def enable_run_by_parents(options):
    # We need to wait for the parent job to finish
    parents = options.wait_job_id
    timeout = options.timeout
    if len(parents) == 0:
        return

    begin_time = int(time.time())
    while True:
        count = 0
        for father in parents.split(","):
            check_cmd = "edb_pdsh -a ps -ef|grep -v grep|grep -cw '# %s'" % father
            p = subprocess.Popen(check_cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                 shell=True,
                                 executable='/bin/bash')
            stdout, stderr = p.communicate()
            if stdout:
                stdout = int(stdout.strip())
                count = count + stdout
                if stdout != 0:
                    break
        if timeout:
            current_time = int(time.time())
            if current_time - begin_time >= timeout:
                logger.info("Already waiting %s seconds, skip it, parents job ids: [%s]" % (timeout, parents))
                return

        if count == 0:
            logger.info("The parents job has been executed, parents job ids: [%s]" % parents)
            return
        time.sleep(60)


def get_options():
    usage = 'usage: %prog [options]\n'
    usage += '  Backup/Export scheduler action script.'
    parser = OptionParser(usage=usage)
    parser.add_option("-b", action="store_true", dest="backup", default=False,
                      help="Perform a new backup")
    parser.add_option("-e", action="store_true", dest="export", default=False,
                      help="Perform a new export")
    parser.add_option("-f", action="store_true", dest="force", default=False,
                      help="Force run the script locally")
    parser.add_option("-t", dest="tag_name", metavar="TAGID",
                      help="The backup tag name")
    parser.add_option("-s", dest="delay_time", type="int",  metavar="DELAYTIME",
                      help="The time delay in seconds, before retrying a failed regular backup")
    parser.add_option("-c", dest="tag_cmd", metavar="TAGCMD", default='',
                      help="The optional backup command args")
    parser.add_option("-d", dest="exportdir", metavar="EXPORDIR",
                      help="Specify the target directory name for automatic export of the backup")
    parser.add_option("-a", dest="effective_after", metavar="EFFECTIVE_AFTER",
                      help="Specify the effective date time after which this script should run. The date time should be in 'YYYY-MM-DD HH:mm:ss' format")
    parser.add_option("-p", action="store_true", dest="pre_incr_backup", default=False,
                      help="Indicates if an incremental backup should be done before a regular backup")
    parser.add_option("-w", dest="wait_job_id", help="Wait for the jobs to finish")
    parser.add_option("-o", dest="timeout", type="int", metavar="Time Out",
                      help="The time to wait for the parents job to finish, sec")
    (options, args) = parser.parse_args()
    return options

class SQLException(Exception):
    pass

class TagManager(object):
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

    def _extract_output_tag(self, result, tagName):
        result = result.replace('\n', '').replace('\t',' ').replace('  ',' ')
        lines = re.findall(r'%s_\w+' % tagName, result)
        if lines:
            return lines[0]
        return ''

    def _perform_backup(self, backup_type, tag_name, tag_cmd):
        logger.info('%s backup %s started' % (backup_type, tag_name))
        command = "backup trafodion, tag '%s' %s, return tag;" % (tag_name, tag_cmd)
        error_prefix = success_prefix = ''
        if backup_type == 'Incremental':
            error_prefix = INCR_BACKUP_ERROR
            success_prefix = INCR_BACKUP_SUCCESS
        elif backup_type == 'Regular':
            error_prefix = REG_BACKUP_ERROR
            success_prefix = REG_BACKUP_SUCCESS

        try:
            logger.info("Call script cleanLockedObjects before backup")
            retcode, msg = self._exec_cmd("sh %s" % traf_home + "/sql/scripts/cleanLockedObjects")
            if retcode != 0:
                logger.error('Failed to run script cleanLockedObjects, error: %s' % msg)
                return 1, ''

            result = self._sqlci(command)
            if "*** ERROR" in result:
                logger.error("%s: %s backup %s failed. %s" % (error_prefix, backup_type, tag_name, self._extract_sql_error(result)))

                logger.info("Call script cleanLockedObjects after backup")
                retcode, msg = self._exec_cmd("sh %s" % traf_home + "/sql/scripts/cleanLockedObjects")
                if retcode != 0:
                    logger.error('Failed to run script cleanLockedObjects, error: %s' % msg)

                return 1, ''

            real_backup_tag = self._extract_output_tag(result, tag_name)
            logger.info('%s: %s backup %s completed successfully. Generated tag %s' % (success_prefix, backup_type, tag_name, real_backup_tag))
            return 0, real_backup_tag
        except Exception as e:
            logger.error("%s: %s backup %s failed. %s" % (error_prefix, backup_type, tag_name, str(e)))
            return 1, ''

    def perform_export(self, backup_type, backup_tag, exportDir):
        logger.info('Export of %s backup %s to [%s] started' % (backup_type, backup_tag, exportDir))
        command = "export backup to location '%s', tag '%s';" % (exportDir, backup_tag)
        error_prefix = success_prefix = ''
        if backup_type == 'Incremental':
            error_prefix = INCR_EXPORT_ERROR
            success_prefix = INCR_EXPORT_SUCCESS
        elif backup_type == 'Regular':
            error_prefix = REG_EXPORT_ERROR
            success_prefix = REG_EXPORT_SUCCESS

        try:
            result = self._sqlci(command)
            if "*** ERROR" in result:
                logger.warning('Export of %s backup %s to [%s] failed, then try it again' % (backup_type, backup_tag, exportDir))
                command = "export backup to location '%s', tag '%s', override;" % (exportDir, backup_tag)
                result = self._sqlci(command)
                if "*** ERROR" in result:
                    logger.error("%s: Export of %s backup %s failed. %s" % (error_prefix, backup_type, backup_tag, self._extract_sql_error(result)))
                    return 1
                else:
                    logger.info('%s: Export of %s backup %s completed successfully' % (success_prefix, backup_type, backup_tag))
            else:
                logger.info('%s: Export of %s backup %s completed successfully' % (success_prefix, backup_type, backup_tag))
            return 0
        except Exception as e1:
            logger.error("%s: Export of %s backup %s failed. %s" % (error_prefix, backup_type, backup_tag, str(e1)))
            return 1

    def _cleanup_backup(self, backup_type, backup_tag):
        logger.info('Cleanup %s backup %s started' % (backup_type, backup_tag))
        command = "cleanup backup , tag '%s';" % (backup_tag)
        try:
            result = self._sqlci(command)
            if "*** ERROR" in result:
                logger.error("Cleanup %s backup %s failed. %s" % (backup_type, backup_tag, self._extract_sql_error(result)))
                return 1
            else:
                logger.info('Cleanup of %s backup %s completed successfully' % (backup_type, backup_tag))
                return 0
        except Exception as e1:
            logger.error('Cleanup of %s backup %s failed, error: %s' % (backup_type, backup_tag, e1))
            return 1

    def write_hdfs_status_file(self, backup_tag, hdfs_export_dir):
        local_file = traf_var + '/status_' + backup_tag
        hdfs_prefix = False
        raw_dir = hdfs_export_dir

        if hdfs_export_dir.startswith('hdfs://'):
            hdfs_prefix = True
            raw_dir = hdfs_export_dir[len('hdfs://'):]
        dir_tokens = raw_dir.split('/')

        hdfs_dir = dir_tokens[0] + '/user/trafodion/backups/.export_status/'
        if hdfs_prefix:
            hdfs_dir = 'hdfs://' + hdfs_dir

        hdfs_file = hdfs_dir + backup_tag
        retcode, msg = self._exec_cmd('rm -f %s; touch %s' % (local_file, local_file))
        if retcode != 0:
            logger.error('Failed to create export status local file : %s' % msg)
            return

        retcode, msg = self._exec_cmd('hdfs dfs -mkdir -p %s' % hdfs_dir)
        if retcode != 0:
            logger.error('Failed to create export status dir in hdfs: %s' % msg)
            return

        retcode, msg = self._exec_cmd('hdfs dfs -copyFromLocal -f %s %s' % (local_file, hdfs_file))
        if retcode != 0:
            logger.error('Failed to copy export status file to hdfs: %s' % msg)
            return

        retcode, msg = self._exec_cmd('rm -f %s' % local_file)
        if retcode != 0:
            logger.error('Failed to delete export status local file : %s' % msg)
            return
        logger.info('Created export status file in hdfs for tag %s' % backup_tag)

    def write_regular_backup_time_to_hdfs(self, backup_time):
        local_file = traf_var + '/' + LAST_SUCCEEDED_REG_BACKUP_FILENAME
        retcode, msg = self._exec_cmd('echo %s > %s' % (backup_time, local_file))
        if retcode != 0:
            logger.error('Failed to write regular backup time to local file : %s' % msg)
            return

        retcode, msg = self._exec_cmd('hdfs dfs -copyFromLocal -f %s %s' % (local_file, LAST_SUCCEEDED_REG_BACKUP_HDFS_LOC))
        if retcode != 0:
            logger.error('Failed to copy export status file to hdfs: %s' % msg)
            return

    def read_regular_backup_time_from_hdfs(self):
        retcode, msg = self._exec_cmd('hdfs dfs -cat %s' % LAST_SUCCEEDED_REG_BACKUP_HDFS_LOC)
        if retcode != 0:
            logger.error('Failed to read regular backup time from hdfs file')
            return
        return msg

    def new_regular_backup(self, options):

        try:
            # Step 1: If this is a recurring regular backup, perform an incremental backup
            # to capture the mutation files
            #real_pre_incr_backup_tag = None
            #if options.pre_incr_backup:
            #    logger.info("Creating an incremental backup to save all mutation files before a regular backup")
            #    tc = options.tag_cmd + ', INCREMENTAL'
            #    return_code, real_pre_incr_backup_tag = self._perform_backup('Incremental', options.tag_name, tc)

            # Step 2: Perform a regular backup
            return_code, real_backup_tag = self._perform_backup(options.backup_type, options.tag_name, options.tag_cmd)

            if return_code == 1:
                #self._cleanup_backup(options.backup_type, options.tag_name)
                if options.delay_time:
                   logger.info('Sleeping ' + str(options.delay_time) + ' seconds')
                   time.sleep(options.delay_time)

                # if the regular backup fails, retry that once
                return_code, real_backup_tag = self._perform_backup(options.backup_type, options.tag_name, options.tag_cmd)

            #real_impl_incr_backup_tag = None
            # Step 3 : If regular backup succeeded, perform an implicit incremental backup for data consistency
            #if return_code == 0 and real_backup_tag:
            #   options.tag_cmd = options.tag_cmd + ', INCREMENTAL'
            #   incr_return_code, real_impl_incr_backup_tag = self._perform_backup('Incremental', options.tag_name, options.tag_cmd)

            #preExportDir = self.get_export_directory(options)
            # Step 4: If the pre-incremental backup from step 1 succeeded, export it to specified location
            #if real_pre_incr_backup_tag and preExportDir:
            #    incr_return_code = self.perform_export('Incremental', real_pre_incr_backup_tag, preExportDir)
            #    if incr_return_code == 0:
            #        self.write_hdfs_status_file(real_pre_incr_backup_tag, preExportDir)

            if return_code == 0:  # it means a successful regular backup
                # a successful regular backup will always generate a new backup time
                # to be used as the hdfs folder name
                backup_time = datetime.now().strftime('%Y%m%d-%H%M%S')
                # save successful time of the last regular backup to hdfs (a shared place)
                # it is used to generate incremental backup export path
                # generate it after pre-incremental backup exported
                self.write_regular_backup_time_to_hdfs(backup_time)
                # export regular backup
                exportDir = self.get_export_directory(options)
                if real_backup_tag and exportDir:
                    return_code = self.perform_export('', real_backup_tag, exportDir)
                    if return_code == 0:
                        self.write_hdfs_status_file(real_backup_tag, exportDir)

            #exportDir = self.get_export_directory(options)
            # Step 5: If the implicit incremental backup from step 3 succeeded, export it to specified location
            #if real_impl_incr_backup_tag and exportDir:
            #    return_code = self.perform_export('Incremental', real_impl_incr_backup_tag, exportDir)
            #    if return_code == 0:
            #        self.write_hdfs_status_file(real_impl_incr_backup_tag, exportDir)

        except Exception as e:
            raise e

    def new_incremental_backup(self, options):

        try:
            # Step 1: Perform an incremental backup
            return_code, real_backup_tag = self._perform_backup(options.backup_type, options.tag_name, options.tag_cmd)

            # Step 2: If incremental backup from step 1 succeeded, export it to specified location
            exportDir = self.get_export_directory(options)
            if real_backup_tag and exportDir:
                return_code = self.perform_export(options.backup_type, real_backup_tag, exportDir)
                if return_code == 0:
                    self.write_hdfs_status_file(real_backup_tag, exportDir)
        except Exception as e:
            raise e

    def get_export_directory(self, options):
        if options.exportdir:
                if "$DATE" in options.exportdir:
                    curr_date = datetime.now()
                    dir_vals = options.exportdir.split("/")
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
                                raise Exception('Invalid $DATE string %s' % dval)

                    return '/'.join(dir_vals)
                elif "$KEEP" in options.exportdir:
                    curr_date = self.read_regular_backup_time_from_hdfs()
                    # gen a new date if not found in hdfs
                    # it should only happen when the first regular backup is taken on the system
                    if not curr_date:
                        curr_date = datetime.now().strftime('%Y%m%d-%H%M%S')

                    # generate dir based on date
                    return options.exportdir.replace('$KEEP', curr_date).strip()

                else:
                    return options.exportdir
        else:
            return None

def main():
    options = get_options()
                 
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
    if options.backup: val += 1
    if options.export: val += 1

    if val != 1:
        err('Must specify only one operation: <backup | export>')

    # if wait_job_id is not null, we need to wait for the parent job to finish
    if options.wait_job_id:
        logger.info("According to the parent job status to decide whether to perform the job...")
        enable_run_by_parents(options)
        logger.info("Began to perform the job...")

    try:
        tm = TagManager()
        if options.backup:
            if not options.tag_name:
                err("Specify the backup tag name using the -t option")

            options.backup_type = "Regular"
            
            if not options.delay_time:
               options.delay_time = 60

            if "INCREMENTAL" in options.tag_cmd or "incremental" in options.tag_cmd:
                options.backup_type = "Incremental"
            
            if options.backup_type == "Regular":
                tm.new_regular_backup(options)
            else:
                tm.new_incremental_backup(options)

        if options.export:
            if not options.tag_name:
                err("Specify the backup tag name using the -t option")

            if not options.exportdir:
                err("Specify the target directory location using the -d option")
            exportDir = tm.get_export_directory(options)
            return_code = tm.perform_export('', options.tag_name, exportDir)
            if return_code == 0:
                tm.write_hdfs_status_file(options.tag_name, exportDir)

    except Exception as e:
        logger.error("Error :" + str(e))
        err(str(e))

            
if __name__ == '__main__':
    main()

