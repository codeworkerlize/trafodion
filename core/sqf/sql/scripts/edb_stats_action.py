#!/usr/bin/env python

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
logger_handler = logging.handlers.RotatingFileHandler(traf_log + '/scheduled_stats.log', 'a', 10000000, 10)
logger_handler.setLevel(logging.INFO)

# Create the Handler for logging data to console
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

# Create a Formatter for formatting the log messages
customFields = {'component': 'STATS_ACTION', 'host': socket.gethostname()}
logger_formatter = logging.Formatter(
    '%(asctime)s, %(levelname)s, %(component)s, Node: %(host)s,, PIN: %(process)d, Process Name: %(processName)s,,,%(message)s')

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
    usage += '  Update statistics scheduler action script.'
    parser = OptionParser(usage=usage)
    parser.add_option("-j", dest="job_id", metavar="Job Id", help="Perform a new job")
    parser.add_option("-d", dest="threshold_time", type="int", default=30,
                      help="Table update statistics time exceeds this threshold from latest update time, default is 30(day)")
    parser.add_option("-p", dest="threshold_size", type="int", default=30,
                      help="Table size exceeds this threshold from last time, suggest: 30-50%, default is 30")
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
        self.current_table_status = {}
        self.old_table_status = {}
        self.selected_tables = []
        self.update_tables = []
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

    # -1: error, 0: success
    def update_status_with_lock(self, state, table_name):
        try:
            self.lock.acquire()
            table_name = table_name.replace("\"", "")
            if state == -1:
                self.update_tables_error.append(table_name)
            else:
                self.update_tables_success.append(table_name)
                current_status = self.current_table_status[table_name]
                current_status["table_stats_time"] = int(time.time())
                self.old_table_status[table_name] = current_status
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
                raise Exception("Failed to get schemas")
            self.options_schemas = [v.strip() for v in msg.strip().split('\n')]
            logger.info("Get schemas from db: %s" % self.options_schemas)

    # state, 1: schema, 2: table
    def _get_tables_name(self, state, param):
        if state == 1:
            logger.debug("Get tables name, param: [schema: %s]", param)
            schema = param[0]
            sql = "select catalog_name||'.'||schema_name||'.'||object_name from \"_MD_\".objects " \
                  "where schema_name = '%s' and object_type = 'BT' and object_name not like 'SB_%%';" % schema
            retcode, msg = self._execute_sql_and_format(sql)
            if retcode != 0:
                raise Exception("Failed to get tables name")
            msg = msg.strip()
            if msg:
                for name in msg.split('\n'):
                    table_name = name.strip()
                    self.current_table_status[table_name] = \
                        {"table_name": table_name,
                         "table_size": 0,
                         "table_rows": 0,
                         "table_stats_time": 0
                         }
            else:
                logger.debug("Get tables name is None, param: [schema: %s]" % param)
        else:
            logger.info("Get tables name, param: [schema: %s, table: %s]" % (param[0], param[1]))
            table_name = "TRAFODION." + param[0] + "." + param[1]
            self.current_table_status[table_name] = \
                {"table_name": table_name,
                 "table_size": 0,
                 "table_rows": 0,
                 "table_stats_time": 0
                 }

    # state, 1: schema, 2: table
    # trafodion.schema.t1,table_size
    def _get_tables_size(self, state, param):
        if state == 1:
            logger.debug("Get tables size, param: [schema: %s]" % param)
            sql = "select 'TRAFODION.'||trim(schema_name) || '.' || trim(object_name)||',', " \
                  "sum(store_file_size+mem_store_size) from table(cluster stats()) " \
                  "where schema_name='%s' and object_name not like 'SB_%%' group by 1 order by 2 desc;" % param[0]
        else:
            logger.debug("Get tables size, param: [schema: %s, table: %s]" % (param[0], param[1]))
            sql = "select 'TRAFODION.'||trim(schema_name) || '.' || trim(object_name)||',', " \
                  "sum(store_file_size+mem_store_size) from table(cluster stats()) " \
                  "where schema_name='%s' and object_name='%s' group by 1 order by 2 desc;" % (param[0], param[1])
        retcode, msg = self._execute_sql_and_format(sql)
        if retcode != 0:
            raise Exception("Failed to get tables size")
        msg = msg.strip()
        if msg:
            self._format_table_status(1, msg)
        else:
            logger.debug("Get tables size is None, param: [schema: %s]" % param)

    # trafodion.schema.t1,table_rows_count
    def _get_tables_rows_count(self, table_name):
        logger.debug("Get the number of table rows, table: %s" % table_name)
        split = table_name.split(".")
        split[1] = "\"" + split[1] + "\""
        split[2] = "\"" + split[2] + "\""
        sql = "select count(*) from %s;" % ".".join(split)
        retcode, msg = self._execute_sql_and_format(sql)
        if retcode != 0:
            raise Exception("Failed to get the number of table rows")
        return msg

    # state, 1: schema, 2: table
    # trafodion.schema.t1,table_stats_time
    def _get_tables_stats_time(self, state, param):
        if state == 1:
            logger.debug("Get the statistical time of the table, param: [schema: %s]" % param)
            sql = "with hist as (" \
                  "select TABLE_UID , DATEDIFF(SECOND, TIMESTAMP '1970-01-01 00:00:00', max(STATS_TIME)) stats_time " \
                  "from \"%s\".SB_HISTOGRAMS group by TABLE_UID) " \
                  "select catalog_name||'.'||schema_name||'.'||object_name||','||" \
                  "cast(isnull(stats_time, 0) as varchar(30)) from \"_MD_\".objects " \
                  "left join hist on TABLE_UID = OBJECT_UID where schema_name = '%s' " \
                  "and object_type = 'BT' and object_name not like 'SB_%%';" % (param[0], param[0])
        else:
            logger.debug("Get the statistical time of the table, param: [schema: %s, table: %s]" % (param[0], param[1]))
            sql = "with hist as (" \
                  "select TABLE_UID , DATEDIFF(SECOND, TIMESTAMP '1970-01-01 00:00:00', max(STATS_TIME)) stats_time " \
                  "from \"%s\".SB_HISTOGRAMS group by TABLE_UID) " \
                  "select catalog_name||'.'||schema_name||'.'||object_name||','||" \
                  "cast(isnull(stats_time, 0) as varchar(30)) from \"_MD_\".objects " \
                  "left join hist on TABLE_UID = OBJECT_UID where schema_name = '%s' " \
                  "and object_type = 'BT' and object_name = '%s';" % (param[0], param[0], param[1])
        retcode, msg = self._execute_sql_and_format(sql)
        if retcode != 0:
            raise Exception("Failed to get the statistical time of the table")
        msg = msg.strip()
        if msg:
            self._format_table_status(2, msg)
        else:
            logger.debug("Get the statistical time of the table is None, param: [schema: %s]" % param)

    # Sort out all the table information
    # state, 1: table size, 2: table stats time
    def _format_table_status(self, state, result):
        try:
            for line in result.strip().split('\n'):
                split = line.split(',')
                table_name = split[0].strip()
                if table_name.find('TRAFODIOND_') == 0:
                    table_name = table_name.split(':')[1]
                table_status_a = split[1].strip()
                table_status = self.current_table_status.get(table_name)
                if not table_status:
                    continue
                if state == 1:
                    table_status["table_size"] = int(table_status_a)
                else:
                    table_status["table_stats_time"] = int(table_status_a)
        except Exception as e1:
            raise Exception('Format the state of the current table, result: %s, error: %s' % (result, e1))

    def _format_options(self, options):
        start_time, end_time = 0, 0
        try:
            logger.info("Begins to format all the parameters")
            start_time = int(time.time())
            self.update_force = False
            self.options_job_id = options.job_id
            self.options_threshold_time = options.threshold_time * 24 * 60 * 60
            self.options_threshold_size = options.threshold_size
            self.options_thread_num = options.thread_num
            self.options_schemas = []
            self.options_tables = []
            if options.threshold_time == options.threshold_size == 0:
                self.update_force = True
                logger.info("Skip the threshold judgment and force an update to the selected table")

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

    def _get_current_table_with_threshold(self):
        for schema in self.options_schemas:
            self._get_tables_name(1, [schema])
            self._get_tables_size(1, [schema])
            self._get_tables_stats_time(1, [schema])
        for table in self.options_tables:
            split = table.split('.')
            schema_name = split[0]
            table_name = split[1]
            self._get_tables_name(2, [schema_name, table_name])
            self._get_tables_size(2, [schema_name, table_name])
            self._get_tables_stats_time(2, [schema_name, table_name])

        # If the table size is 0, look up the number of rows again
        for table_status in self.current_table_status.values():
            if table_status["table_size"] == 0:
                rows = self._get_tables_rows_count(table_status["table_name"])
                table_status["table_rows"] = int(rows.strip())

    def _get_current_table_without_threshold(self):
        for schema in self.options_schemas:
            self._get_tables_name(1, [schema])
        for table in self.options_tables:
            split = table.split('.')
            schema_name = split[0]
            table_name = split[1]
            self._get_tables_name(2, [schema_name, table_name])

    def _get_current_table_status(self):
        start_time, end_time, error_count = 0, 0, 0
        try:
            logger.info("Begins to get the current state of the selected table")
            start_time = int(time.time())
            if self.update_force:
                self._get_current_table_without_threshold()
            else:
                self._get_current_table_with_threshold()
            end_time = int(time.time())
            logger.info("Get the current state of the selected table successfully")
        except Exception as e1:
            error_count = -1
            logger.error('Failed to get the current state of the selected table, error: %s' % e1)
            sys.exit(-1)
        finally:
            if error_count == -1:
                self.cron_state.set_current_scheduler_state(2, start_time, end_time, 0, 0)
            else:
                table_num = len(self.current_table_status)
                self.cron_state.set_current_scheduler_state(2, start_time, end_time, table_num, table_num)

    def _get_old_table_status(self):
        logger.info("Get the old tables status file")
        hdfs_file = hdfs_dir+"stats_status"
        retcode, msg = self._exec_cmd('hdfs dfs -cat %s' % hdfs_file)
        if retcode != 0:
            if "No such file or directory" in msg:
                logger.warning('Failed to get the old tables status file : %s' % msg)
            else:
                logger.error('Failed to get the old tables status file : %s' % msg)
        else:
            self.old_table_status = json.loads(msg, encoding="utf-8")

    def _get_expired_tables_with_threshold(self, success_count):
        self._get_old_table_status()
        for current_table_status in self.current_table_status.values():
            success_count += 1
            table_name = current_table_status["table_name"]
            old_table_status = self.old_table_status.get(table_name)
            split = table_name.split(".")
            split[1] = "\"" + split[1] + "\""
            split[2] = "\"" + split[2] + "\""
            table_name = ".".join(split)
            if not old_table_status:
                self.update_tables.append(table_name)
                logger.debug("Found new table need to update statistics, %s" % table_name)
                continue
            else:
                # 1: size, 2: rows, 3: time
                logger.info("Check the table size or table rows number, table: %s" % table_name)
                old_table_size = old_table_status["table_size"]
                current_table_size = current_table_status["table_size"]
                if current_table_size == 0:
                    current_table_rows = current_table_status["table_rows"]
                    old_table_rows = old_table_status["table_rows"]
                    table_rows_difference = abs(current_table_rows - old_table_rows)
                    if old_table_rows == 0 and table_rows_difference != 0:
                        self.update_tables.append(table_name)
                        logger.debug("The table rows threshold exceeds, table: %s, old rows: %s,"
                                     " current rows: %s" % (table_name, old_table_rows, current_table_rows))
                        continue
                    if old_table_rows != 0 and table_rows_difference / old_table_rows * 100 >= self.options_threshold_size:
                        self.update_tables.append(table_name)
                        logger.debug("The table rows threshold exceeds, table: %s, old rows: %s,"
                                     " current rows: %s" % (table_name, old_table_rows, current_table_rows))
                        continue
                else:
                    table_size_difference = abs(current_table_size - old_table_size)
                    if old_table_size == 0 and table_size_difference != 0:
                        self.update_tables.append(table_name)
                        logger.debug("The table rows threshold exceeds, table: %s, old size: %s,"
                                     " current size: %s" % (table_name, old_table_size, current_table_size))
                        continue
                    if old_table_size != 0 and table_size_difference / old_table_size * 100 > self.options_threshold_size:
                        self.update_tables.append(table_name)
                        logger.debug("The table rows threshold exceeds, table: %s, old size: %s,"
                                     " current size: %s" % (table_name, old_table_size, current_table_size))
                        continue

                logger.info("Check the table statistics update time, table: %s" % table_name)
                table_stats_time = current_table_status["table_stats_time"]
                if int(time.time()) - table_stats_time > self.options_threshold_time:
                    self.update_tables.append(table_name)
                    logger.debug("The table time threshold exceeds, table: %s, old time: %s" %
                                 (table_name, table_stats_time))
                    continue
            logger.info("This table does not need to update statistics, table: %s" % table_name)
        return success_count

    def _get_expired_tables_without_threshold(self, success_count):
        for current_table_status in self.current_table_status.values():
            success_count += 1
            table_name = current_table_status["table_name"]
            split = table_name.split(".")
            split[1] = "\"" + split[1] + "\""
            split[2] = "\"" + split[2] + "\""
            table_name = ".".join(split)
            self.update_tables.append(table_name)
        return success_count

    def _get_expired_tables(self):
        start_time, end_time, success_count = 0, 0, 0
        try:
            logger.info("Begins to get tables who need to update statistics")
            start_time = int(time.time())
            self.selected_tables = self.current_table_status.keys()
            if self.update_force:
                success_count = self._get_expired_tables_without_threshold(success_count)
            else:
                success_count = self._get_expired_tables_with_threshold(success_count)
            end_time = int(time.time())
            logger.info("Completed to get tables who need to update statistics")
            logger.info("Need to update the statistics table, number: %s, tables: %s" %
                        (len(self.update_tables), self.update_tables))
        except Exception as e1:
            logger.error('Failed to get tables who need to update statistics, error: %s' % e1)
            sys.exit(-1)
        finally:
            self.cron_state.set_current_scheduler_state(3, start_time, end_time, success_count, len(self.current_table_status))

    def _update_stats(self, tables, count):
        try:
            logger.debug("Begins to update statistics started on thread '%d', tables: %s" % (count, tables))
            for table_name in tables:
                sql = "update statistics for table %s on every column sample;" % table_name
                result = self._sqlci(sql)
                if "*** ERROR" in result:
                    logger.error("Failed to update the statistics for table %s, error: %s" %
                                 (table_name, self._extract_sql_error(result)))
                    logger.info("Update the statistics for table %s again" % table_name)
                    result = self._sqlci(sql)
                    if "*** ERROR" in result:
                        logger.error("The second failed to update the statistics for table %s, sql: %s, error: %s" %
                                     (table_name, sql, self._extract_sql_error(result)))
                        self.update_status_with_lock(-1, table_name)
                        continue
                self.update_status_with_lock(0, table_name)
                logger.info("Update statistics for table %s successfully on thread %d" % (table_name, count))
            logger.info("Completed to update statistics on thread %d" % count)
        except Exception as e1:
            logger.error("Failed to update statistics for table on thread '%d', error: %s" % (count, e1))

    def _update_stats_by_thread(self):
        start_time, end_time = 0, 0
        try:
            start_time = int(time.time())
            update_tables_len = len(self.update_tables)
            piece = update_tables_len / self.options_thread_num + 1
            threads = []
            count = 0
            while count < self.options_thread_num:
                list = self.update_tables[count * piece:(count + 1) * piece]
                if len(list) == 0:
                    break
                threads.append(Thread(target=self._update_stats, args=(list, count+1)))
                count += 1
            logger.info("Start %d threads to update statistics ..." % count)
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            end_time = int(time.time())
            logger.info("End of update statistics, Success: { number: %s, tables: %s }, Failed: { number: %s, "
                        "tables: %s }" % (len(self.update_tables_success), self.update_tables_success,
                                          len(self.update_tables_error), self.update_tables_error))
        except Exception as e1:
            logger.error('Failed to start threads, error: %s' % e1)
            sys.exit(-1)
        finally:
            self.cron_state.set_current_scheduler_state(4, start_time, end_time, len(self.update_tables_success), len(self.update_tables))

    def write_hdfs_status_file(self):
        start_time, end_time = 0, 0
        try:
            start_time = int(time.time())
            logger.info('Begins to created stats status file in hdfs')
            local_file = traf_var + "/stats_status_" + str(int(time.time()))
            hdfs_file = hdfs_dir + 'stats_status'
            table_status_str = json.dumps(self.old_table_status, encoding="utf-8")
            retcode, msg = self._exec_cmd('echo > %s' % local_file)
            if retcode != 0:
                raise Exception('Failed to create stats status local file : %s' % msg)
            with open(local_file, "w") as file:
                file.write(table_status_str)

            retcode, msg = self._exec_cmd('hdfs dfs -mkdir -p %s' % hdfs_dir)
            if retcode != 0:
                raise Exception('Failed to create stats status dir in hdfs: %s' % msg)

            retcode, msg = self._exec_cmd('hdfs dfs -copyFromLocal -f %s %s' % (local_file, hdfs_file))
            if retcode != 0:
                raise Exception('Failed to copy stats status file to hdfs: %s' % msg)

            retcode, msg = self._exec_cmd('rm -f %s' % local_file)
            if retcode != 0:
                raise Exception('Failed to delete stats status local file : %s' % msg)
            end_time = int(time.time())
            logger.info('Created stats status file in hdfs for job %s successfully' % self.options_job_id)
        except Exception as e:
            logger.error("Failed to create stats status file in hdfs for job %s, error: %s" % (self.options_job_id, e))
            sys.exit(-1)
        finally:
            self.cron_state.set_current_scheduler_state(5, start_time, end_time, 0, 0)

    def start_schedule(self, options):
        try:
            self._format_options(options)
            self._get_current_table_status()
            self._get_expired_tables()
            self._update_stats_by_thread()
            self.write_hdfs_status_file()
            logger.info("Completed the job schedule for updating the statistics, job id: %s" % self.options_job_id)
        except Exception as e:
            logger.error("Failed to run the job schedule that updates the statistics, job id: %s, error: %s" %
                         (self.options_job_id, e))


class CronState(object):
    def __init__(self, job_id):
        self.job_id = job_id
        self.step_names = ["Initialize", "Get tables information", "Check tables", "Update statistics", "Finalize"]
        self.current_state = ''
        self.pid = os.getpid()
        self.hostname = socket.gethostname()
        self.job_start_time = int(time.time()) * 1000
        self.job_end_time = 0
        self.exec_by_threads_state = "Warning"

    def set_current_scheduler_state(self, step_num, start_time, end_time, complete_num, total_num):
        try:
            logger.debug("Output current running status information")
            if step_num == 4 and complete_num == total_num:
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
            self.current_state = "%s,%s,%s,%s,%s,%s,%s,%s,%s" % (self.pid, self.hostname, step_num, step_name, state, start_time, end_time, elapsed_time, progress)
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

        if step_num == 5:
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
    check_cmd = 'ps -ef|grep -v grep|grep "edb_stats_action.py"|grep -cE "# %s"' % job_id
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
            options.job_id = "stats:" + str(int(time.time()))
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
