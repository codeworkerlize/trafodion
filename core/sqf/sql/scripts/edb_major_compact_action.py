#!/usr/bin/env python
# coding=utf-8

# @@@ START COPYRIGHT @@@
#
# (C) Copyright 2018-2020 Esgyn Corporation
#
# @@@ END COPYRIGHT @@@
import json
import sys
import subprocess
import logging, logging.handlers
import os
import re
import socket
import time
from datetime import datetime
from optparse import OptionParser
from threading import Thread, RLock

traf_log = os.environ.get("TRAF_LOG")
traf_var = os.environ.get("TRAF_VAR")
hdfs_dir = '/user/trafodion/crontab/'

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)

# Create the Handler for logging data to a file
logger_handler = logging.handlers.RotatingFileHandler(traf_log + '/scheduled_major_compact.log', 'a', 10000000, 10)
logger_handler.setLevel(logging.INFO)

# Create the Handler for logging data to console
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

# Create a Formatter for formatting the log messages
customFields = {'component': 'MAJOR_COMPACT', 'host': socket.gethostname()}
logger_formatter = logging.Formatter('%(asctime)s, %(levelname)s, %(component)s, Node: %(host)s,, PIN: %(process)d, '
                                     'Process Name: %(processName)s,,,%(message)s')

# Add the Formatter to the Handler
logger_handler.setFormatter(logger_formatter)
console_handler.setFormatter(logger_formatter)

# Add the Handler to the Logger
logger.addHandler(logger_handler)
logger.addHandler(console_handler)

# Add the custom fields to the logger format
logger = logging.LoggerAdapter(logger, customFields)


def get_options():
    usage = 'usage: %prog [options]\n'
    usage += ' Compact all regions in table scheduler action script.'
    parser = OptionParser(usage=usage)
    parser.add_option("-j", dest="job_id", metavar="Job Id", help="Perform a new job")
    parser.add_option("-s", dest="schemas",
                      help="Specify certain schemas, case sensitive, eg: schema1,schema2,")
    parser.add_option("-t", dest="tables",
                      help="Specify certain tables, case sensitive, eg: schema1.table1,schema2.table1,")
    parser.add_option("-r", dest="run_type", default="circle", help="once | circle, default circle")
    parser.add_option("-e", dest="effective_after", help="effective after time 'yyyy-MM-dd HH:mm:ss'")
    parser.add_option("-n", dest="thread_num", type="int", default=1,
                      help="The degree of parallelism or threads for the script. Defaults to 1 if not specified")
    parser.add_option("-f", action="store_true", dest="force", default=False,
                      help="Force run the script locally")
    parser.add_option("-w", dest="wait_job_id", help="Wait for the jobs to finish")
    parser.add_option("-o", dest="timeout", type="int", metavar="Time Out",
                      help="The time to wait for the parents job to finish, sec")
    (options, args) = parser.parse_args()
    return options


class CronManager(object):
    def __init__(self, job_id):
        self.lock = RLock()
        self.cron_state = CronState(job_id)
        self.selected_tables_map = {}
        self.selected_tables = []
        self.update_tables_success = []
        self.update_tables_error = []

    def _exec_cmd(self, cmd):
        logger.debug('executing command %s' % cmd)
        p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True,
                             executable='/bin/bash')
        stdout, stderr = p.communicate()
        msg = stdout
        if p.returncode != 0:
            msg = stderr if stderr else stdout
            logger.error('Failed to run command: %s' % msg)
        return p.returncode, msg

    def _sqlci(self, cmd):
        sqlci_cmd = 'echo "%s" | sqlci' % cmd.replace("\"", "\\\"")
        logger.debug('executing command %s' % sqlci_cmd)
        p = subprocess.Popen(sqlci_cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             shell=True, executable='/bin/bash')
        stdout, stderr = p.communicate()
        if p.returncode != 0:
            msg = stderr if stderr else stdout
            logger.error('Failed to run sqlci command: %s' % msg)
        return stdout

    def _extract_sql_error(self, errorstr):
        errorstr = errorstr.replace('\n', '').replace('\t', ' ').replace('  ', ' ')
        lines = re.findall(r'\>\>(.*?)\>\>', errorstr)
        if lines:
            return lines[0]
        return ''

    def _execute_sql_and_format(self, sql):
        sqlci_cmd = 'sqlci -q "%s" ' % sql.replace("\"", "\\\"")
        cmd = sqlci_cmd + "| sed -n '/^----/,/^---/p' | grep -v \"\-\-\-\" | sed 's/ //g' | sed '/^$/d'"
        return self._exec_cmd(cmd)

    # -1: error, 0 success
    def update_status_with_lock(self, state, table_name):
        try:
            self.lock.acquire()
            if state == -1:
                self.update_tables_error.append(table_name)
            else:
                self.update_tables_success.append(table_name)
        except Exception as e:
            raise e
        finally:
            self.lock.release()

    def _check_options_schemas_and_tables(self):
        if not self.options_schemas and not self.options_tables:
            logger.info("No schemas set in config, will traverse all possible schemas")
            sql = "select distinct schema_name from \"_MD_\".objects " \
                  "where schema_name not like '#_%#_' escape '#' " \
                  "and schema_name not like 'VOLATILE#_SCHEMA#_%' escape '#';"
            retcode, msg = self._execute_sql_and_format(sql)
            if retcode != 0:
                logger.error('Failed to get schemas')
                sys.exit(-1)
            self.options_schemas = [v.strip() for v in msg.strip().split('\n')]
            logger.info("Get schemas from db: %s" % self.options_schemas)

    def _get_selected_tables_from_md(self):
        for schema in self.options_schemas:
            sql = "select catalog_name||'.'||schema_name||'.'||object_name from \"_MD_\".objects where " \
                  "schema_name = '%s' and object_type = 'BT' and object_name not like 'SB_%%';" % schema
            retcode, msg = self._execute_sql_and_format(sql)
            if retcode != 0:
                logger.error("Failed to get the table name in the schema %s by \"_MD_\"" % schema)
                sys.exit(-1)
            msg = msg.strip()
            if msg:
                for name in msg.split('\n'):
                    table_name = name.strip()
                    self.selected_tables_map[table_name] = table_name
            else:
                logger.debug("Get the table name in the schema %s by \"_MD_\" is None" % schema)
        for table_name in self.options_tables:
            table_name = "TRAFODION." + table_name
            self.selected_tables_map[table_name] = table_name

    def _get_selected_tables_from_hbase(self):
        try:
            cmd = "echo \"list\" | hbase shell | sed -n '/^\\s*$/,/^\\s*$/p'"
            retcode, msg = self._exec_cmd(cmd)
            if retcode != 0:
                logger.error("Failed to get the table name by hbase， error: %s" % msg)
                logger.error("Get the table name by hbase again")
                retcode, msg = self._exec_cmd(cmd)
                if retcode != 0 in msg:
                    logger.error("The second failed to get the table name by hbase, cmd: %s, error: %s" % (cmd, msg))
                    sys.exit(-1)

            hbase_tables_arr = msg.strip().split("\n")
            hbase_tables_map = {}
            for hbase_table in hbase_tables_arr:
                strip_split = hbase_table.strip().split(":")
                if len(strip_split) > 1:
                    hbase_tables_map[strip_split[1]] = hbase_table
                else:
                    hbase_tables_map[strip_split[0]] = hbase_table
            for table_name in self.selected_tables_map.keys():
                hbase_name = hbase_tables_map.get(table_name)
                if hbase_name:
                    self.selected_tables_map[table_name] = hbase_name
                else:
                    self.selected_tables_map.pop(table_name)
                    logger.error("Hbase doesn't have this table, table name: %s" % table_name)
        except Exception as e1:
            logger.error("Failed to get the table name by hbase， error: %s" % e1)
            sys.exit(-1)

    def _format_options(self, options):
        start_time, end_time = 0, 0
        try:
            logger.info("Begins to format all the parameters")
            start_time = int(time.time())
            self.options_job_id = options.job_id
            self.options_thread_num = options.thread_num
            self.options_schemas = []
            self.options_tables = []
            if options.schemas:
                for s in options.schemas.split(','):
                    strip = s.strip()
                    if strip.find('TRAFODION.') == 0 or strip.find('trafodion.') == 0:
                        self.options_schemas.append(strip.replace('TRAFODION.', '', 1).replace('trafodion.', '', 1))
                    else:
                        self.options_schemas.append(strip)
            if options.tables:
                for t in options.tables.split(','):
                    strip = t.strip()
                    if strip.count(".") > 1:
                        if strip.find('TRAFODION.') == 0:
                            self.options_tables.append(strip.replace('TRAFODION.', '', 1))
                        elif strip.find('trafodion.') == 0:
                            self.options_tables.append(strip.replace('trafodion.', '', 1))
                    else:
                        self.options_tables.append(strip)

            self._check_options_schemas_and_tables()
            end_time = int(time.time())
            logger.info("Format all parameters successfully")
        except Exception as e1:
            logger.error('Failed to format all the parameters, error: %s' % e1)
            sys.exit(-1)
        finally:
            self.cron_state.set_current_scheduler_state(1, start_time, end_time, 0, 0)

    def _get_tables_name_in_hbase(self):
        start_time, end_time, error_state = 0, 0, 0
        try:
            logger.info("Begins to get the name of the tables about trafodion in hbase")
            start_time = int(time.time())
            self._get_selected_tables_from_md()
            self._get_selected_tables_from_hbase()
            self.selected_tables = self.selected_tables_map.values()
            end_time = int(time.time())
            logger.info("Gets the name of the tables about trafodion in hbase successfully")
        except Exception as e1:
            error_state = -1
            logger.error('Failed to get the name of the tables about trafodion in hbase, error: %s' % e1)
            sys.exit(-1)
        finally:
            if error_state == -1:
                self.cron_state.set_current_scheduler_state(2, start_time, end_time, 0, 0)
            else:
                tables_num = len(self.selected_tables)
                self.cron_state.set_current_scheduler_state(2, start_time, end_time, tables_num, tables_num)

    def _major_compact_tables(self, tables, count):
        try:
            logger.info("Compact all regions in a table on thread '%d', tables: %s" % (count, tables))
            for table_name in tables:
                cmd = "echo \"major_compact '%s'\" |hbase shell" % table_name
                retcode, msg = self._exec_cmd(cmd)
                if retcode != 0 or "ERROR" in msg:
                    logger.error("Failed to compact all regions in a table, error: %s" % msg)
                    logger.info("Compact all regions in a table %s again on thread %s" % (table_name, count))
                    retcode, msg = self._exec_cmd(cmd)
                    if retcode != 0 or "ERROR" in msg:
                        logger.error("The second failed to compact all regions in a table, cmd: %s, error: %s" %
                                     (cmd, msg))
                        self.update_status_with_lock(-1, table_name)
                        continue
                self.update_status_with_lock(0, table_name)
                logger.info("Compact all regions in a table successfully, table: %s" % table_name)
            logger.info("Completed to compact all regions on thread %d" % count)
        except Exception as e1:
            logger.error('Failed to compact all regions for table, error: %s' % e1)

    def _compact_tables_by_threads(self):
        start_time, end_time = 0, 0
        try:
            start_time = int(time.time())
            update_tables_len = len(self.selected_tables)
            piece = update_tables_len / self.options_thread_num + 1
            threads = []
            count = 0
            while count < self.options_thread_num:
                list = self.selected_tables[count * piece:(count + 1) * piece]
                if len(list) == 0:
                    break
                threads.append(Thread(target=self._major_compact_tables, args=(list, count+1)))
                count += 1
            logger.info("Start %d threads to compact all regions in tables ..." % count)
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            end_time = int(time.time())
            logger.info("End of compact all regions in tables, Success: { number: %s, tables: %s }, Failed: { number: "
                        "%s, tables: %s }" % (len(self.update_tables_success), self.update_tables_success,
                                              len(self.update_tables_error), self.update_tables_error))
        except Exception as e1:
            logger.error('Failed to start threads, error: %s' % e1)
        finally:
            self.cron_state.set_current_scheduler_state(3, start_time, end_time, len(self.update_tables_success),
                                                        len(self.selected_tables))

    def start_schedule(self, options):
        try:
            self._format_options(options)
            self._get_tables_name_in_hbase()
            self._compact_tables_by_threads()
            start_time = int(time.time())
            self.cron_state.set_current_scheduler_state(4, start_time, int(time.time()), 0, 0)
            logger.info("Completed the job schedule for compact all regions, job id: %s" % self.options_job_id)
        except Exception as e:
            logger.error("Failed to run the job schedule that compact all regions, job id: %s, error: %s" %
                         (self.options_job_id, e))


class CronState(object):
    def __init__(self, job_id):
        self.job_id = job_id
        self.step_names = ["Initialize", "Get tables information", "Compact regions", "Finalize"]
        self.current_state = ''
        self.pid = os.getpid()
        self.hostname = socket.gethostname()
        self.job_start_time = int(time.time()) * 1000
        self.job_end_time = 0
        self.exec_by_threads_state = "Warning"

    def set_current_scheduler_state(self, step_num, start_time, end_time, complete_num, total_num):
        try:
            logger.debug("Output current running status information")
            if step_num == 3 and complete_num == total_num:
                self.exec_by_threads_state = "Succeed"
            if total_num == 0:
                progress = "%s%%(%s/%s)" % (100, complete_num, total_num)
            else:
                progress = "%s%%(%s/%s)" % (100 * complete_num / total_num, complete_num, total_num)
            if end_time == 0:
                end_time = int(time.time())
                state = "Failed"
                progress = "0%(0/0)"
            else:
                state = "Completed"
            step_name = self.step_names[step_num - 1]
            start_time *= 1000
            end_time *= 1000
            elapsed_time = end_time - start_time
            # pid, hostname, step num, step name, state, start time, end time, elapsed time, progress
            self.current_state = "%s,%s,%s,%s,%s,%s,%s,%s,%s" % (
                self.pid, self.hostname, step_num, step_name, state, start_time, end_time, elapsed_time, progress)
            self._write_hdfs_status(step_num)
        except Exception as e:
            logger.error("Output current running status information failed, error: %s" % e)

    def _exec_cmd(self, cmd):
        logger.debug('executing command %s' % cmd)
        p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True,
                             executable='/bin/bash')
        stdout, stderr = p.communicate()
        msg = stdout
        if p.returncode != 0:
            msg = stderr if stderr else stdout
            logger.error('Failed to run command: %s' % msg)
        return p.returncode, msg

    def _write_hdfs_status(self, step_num):
        local_file = traf_var + "/job_status_" + self.job_id.replace(":", "_")
        hdfs_file = hdfs_dir + self.job_id.replace(":", "_")
        if step_num == 1:
            retcode, msg = self._exec_cmd('rm -f %s' % local_file)
            if retcode != 0:
                raise Exception('Failed to delete job status local file : %s' % msg)

        retcode, msg = self._exec_cmd('echo "%s" >> %s' % (self.current_state, local_file))
        if retcode != 0:
            raise Exception('Failed to create job status local file : %s' % msg)

        retcode, msg = self._exec_cmd('hdfs dfs -mkdir -p %s' % hdfs_dir)
        if retcode != 0:
            raise Exception('Failed to create job status dir in hdfs: %s' % msg)

        retcode, msg = self._exec_cmd('hdfs dfs -copyFromLocal -f %s %s' % (local_file, hdfs_file))
        if retcode != 0:
            raise Exception('Failed to copy job status file to hdfs: %s' % msg)

        if step_num == 4:
            retcode, msg = self._exec_cmd('rm -f %s' % local_file)
            if retcode != 0:
                raise Exception('Failed to delete job status local file : %s' % msg)


def should_i_run():
    # We only need to run this script from one node in the cluster
    # idtmsrv process runs only on one node at any given time
    # So we check if current node has idtmsrv running and decide to run if true
    check_cmd = "sqps|grep idtmsrv|cut -d',' -f1|cut -d' ' -f2 && trafconf -mynid"
    p = subprocess.Popen(check_cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True,
                         executable='/bin/bash')
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


def job_exist(job_id):
    check_cmd = 'ps -ef|grep -v grep|grep "edb_major_compact_action.py"|grep -cE "# %s"' % job_id
    p = subprocess.Popen(check_cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True,
                         executable='/bin/bash')
    stdout, stderr = p.communicate()
    if stdout:
        stdout = stdout.strip()
        if int(stdout) > 2:
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
            logger.info("The parent job has been executed, parents job ids: [%s]" % parents)
            return
        time.sleep(60)


def main():
    options = get_options()

    # If trigger by cron, we only can run this action from one node
    # Check if the scripts needs to run on this node
    if not options.force and not should_i_run():
        logger.debug("Current node is not active node. Skipping job")
        sys.exit(-1)

    if options.job_id and job_exist(options.job_id):
        logger.info("Current job is running. Skipping this job")
        sys.exit(-1)

    def err(msg):
        sys.stderr.write('[ERROR] ' + msg + '\n')
        sys.exit(1)

    # if wait_job_id is not null, we need to wait for the parent job to finish
    if options.wait_job_id:
        logger.info("According to the parent job status to decide whether to perform the job...")
        enable_run_by_parents(options)
        logger.info("Began to perform the job...")

    if options.effective_after:
        try:
            dt = datetime.strptime(options.effective_after, '%Y-%m-%d %H:%M:%S')
            now = datetime.now()
            if dt > now:
                logger.info("The schedule is only effective after %s. Skipping job" % options.effective_after)
                sys.exit(-1)
            if options.run_type == "once" and dt.year != now.year:
                logger.info("The schedule will run only once, in %s. Skipping job" % dt.year)
                sys.exit(-1)
        except Exception as e:
            logger.error("Error :" + str(e))
            sys.exit(-1)
    else:
        if options.run_type == "once":
            err("Specify the schedule effective time using the -e option")

    ### validate parameters ###
    try:
        if not options.job_id:
            options.job_id = "compact:" + str(int(time.time()))
            logger.info("The schedule has no set id, default id is %s" % options.job_id)
        if options.tables:
            for t in options.tables.split(','):
                if t.find('.') == -1:
                    err("Specify the schedule tables using the -t option, eg: schemas1.table1, schemas2.table2,")
        tm = CronManager(options.job_id)
        tm.start_schedule(options)
    except Exception as e:
        logger.error("Error :" + str(e))
        err(str(e))


if __name__ == '__main__':
    main()
