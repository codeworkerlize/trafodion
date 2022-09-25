#! /usr/bin/env jython
# -*- coding: UTF-8 -*-
#
# @@@ START COPYRIGHT @@@
#
# (C) Copyright 2015-2016 Esgyn Corporation
#
# @@@ END COPYRIGHT @@@

import signal
import datetime
import sys
import time
import os
import copy
import subprocess
from java.lang import Class
from java.sql  import DriverManager, SQLException

def signal_term_handler(signal, frame):
    sys.exit(0)
 
signal.signal(signal.SIGTERM, signal_term_handler)

ppid = os.getppid()

################################################################################
JDBC_URL    = "jdbc:t2jdbc:"
JDBC_DRIVER = "org.trafodion.jdbc.t2.T2Driver"

OFFENDING_QUERIES_CPU = 'offendingQueriesCpu'
OFFENDING_QUERIES_MEM = 'offendingQueriesMem'
OFFENDING_QUERIES_SQL = 'offendingQueriesSql'
# RMS:QID
RMS_STATS_QUERY = 'rmsStatsQuery'
# CANCEL:QID:COMMENT
CANCEL_QUERY = 'cancelQuery'
STATS_ROW_TYPE = 'statsRowType: 15'
OFFENDER_DELAY = 30.0 # for offending queries (cpu, memory and so on...)

#-------------------------------- query statistics --------------------------------------------------------
#
RMS_QUERY_STATISTICS = """
select variable_info from table(statistics(null,cast(_ISO88591'QID=%0.160s' as char(160) character set ISO88591)))
"""
#
#----------------- LSO Offender --------------------------------------------------------
#

LSO_OFFENDER_CPU_QUERY = """
select [first 5] current_timestamp "CURRENT_TIMESTAMP"
        , count(*) no_of_processes
        , sum(cast(tokenstr('diffCpuTime:', variable_info)
                 as NUMERIC(18) )) DIFF_CPU_TIME
        , cast(tokenstr('Qid:', variable_info)
                as varchar(175) CHARACTER SET UTF8) QUERY_ID
 from table (statistics(NULL,'CPU_OFFENDER=-1'))
 group by 4
 order by 3 descending
"""

LSO_OFFENDER_ALLOC_MEM = """
select [first 5] current_timestamp "CURRENT_TIMESTAMP",
    cast(tokenstr('nodeId:', variable_info) as integer) node,
    cast(tokenstr('processId:', variable_info) as integer) pid,
    cast(tokenstr('exeMemHighWMInMB:', variable_info) as integer) EXE_MEM_HIGH_WM_MB,
    cast(tokenstr('exeMemAllocInMB:', variable_info) as integer) EXE_MEM_ALLOC_MB,
    cast(tokenstr('ipcMemHighWMInMB:', variable_info) as integer) IPC_MEM_HIGH_WM_MB,
    cast(tokenstr('ipcMemAllocInMB:', variable_info) as integer) IPC_MEM_ALLOC_MB
  from table(statistics(null, 'ALLOC_MEM_OFFENDER=250'))
  order by 5
"""
LSO_OFFENDER_HIGHWM_MEM = """
select [first 5] current_timestamp "CURRENT_TIMESTAMP",
    cast(tokenstr('nodeId:', variable_info) as integer) node,
    cast(tokenstr('processId:', variable_info) as integer) pid,
    cast(tokenstr('exeMemHighWMInMB:', variable_info) as integer) EXE_MEM_HIGH_WM_MB,
    cast(tokenstr('exeMemAllocInMB:', variable_info) as integer) EXE_MEM_ALLOC_MB,
    cast(tokenstr('ipcMemHighWMInMB:', variable_info) as integer) IPC_MEM_HIGH_WM_MB,
    cast(tokenstr('ipcMemAllocInMB:', variable_info) as integer) IPC_MEM_ALLOC_MB
  from table(statistics(null, 'HIGHWM_MEM_OFFENDER=250'))
  order by 5
"""

LSO_OFFENDER_SQL_QUERY = """
select [first 5] current_timestamp "CURRENT_TIMESTAMP"    -- (1) Now
       , cast(tokenstr('blockedInSQL:', variable_info)             -- (2) Time in SQL
             as NUMERIC(18)) TIME_IN_SECONDS
       , cast(tokenstr('Qid:', variable_info)            -- (3) QID
             as varchar(175) CHARACTER SET UTF8) QUERY_ID
       , cast(tokenstr('State:', variable_info)           -- (4) State
             as char(30)) EXECUTE_STATE 
       , cast(substr(variable_info,            -- (5) SQL Source
             position(' sqlSrc: ' in variable_info) + char_length(' sqlSrc: '),
             char_length(variable_info) - 
                        ( position(' sqlSrc: ' in variable_info) + char_length(' sqlSrc: ') ))
             as char(256) CHARACTER SET UTF8) SOURCE_TEXT
from table (statistics(NULL, 'QUERIES_IN_SQL=30'))
order by 2 descending
"""

################################################################################
queries_cpu = []
time_cpu = 0.0

processes_mem = []
time_mem = 0.0

queries_sql = []
time_sql = 0.0

def offendingQueriesCpu(dbConn, command):
    global queries_cpu
    global time_cpu
    tstart = time.time()

    if time_cpu == 0.0 or ((time_cpu + OFFENDER_DELAY) <= tstart):
        time_cpu = tstart
        queries_cpu = []
        stmt = dbConn.createStatement()
        try:
            resultSet = stmt.executeQuery(LSO_OFFENDER_CPU_QUERY)
            while resultSet.next():
                qid = resultSet.getString("QUERY_ID")
                print qid
                queries_cpu.append(qid)
        except SQLException, msg:
            print msg
        stmt.close()
    else:
        for qid in queries_cpu:
            print qid

def offendingQueriesMem(dbConn, command):
    global processes_mem
    global time_mem
    tstart = time.time()

    if time_mem == 0.0 or ((time_mem + OFFENDER_DELAY) <= tstart):
        time_mem = tstart
        processes_mem = []
        stmt = dbConn.createStatement()
        try:
            resultSet = stmt.executeQuery(LSO_OFFENDER_HIGHWM_MEM)
            while resultSet.next():
                nodeId = resultSet.getInteger("node")
                processId = resultSet.getInteger("pid")
                print '%d.%d' % (nodeId, processId)
                processes_mem.append('%d.%d' % (nodeId, processId))
        except SQLException, msg:
            print msg
        stmt.close()
    else:
        for process in processes_mem:
            print process

def offendingQueriesSql(dbConn, command):
    global queries_sql
    global time_sql
    tstart = time.time()

    if time_sql == 0.0 or ((time_sql + OFFENDER_DELAY) <= tstart):
        queries_sql = tstart
        queries_sql = []
        stmt = dbConn.createStatement()
        try:
            resultSet = stmt.executeQuery(LSO_OFFENDER_SQL_QUERY)
            while resultSet.next():
                qid = resultSet.getString("QUERY_ID")
                print qid
                queries_sql.append(qid)
        except SQLException, msg:
            print msg
        stmt.close()
    else:
        for qid in queries_sql:
            print qid

def rmsStatsQuery(dbConn, line):
    [command, qid] = line.split(':')
    stmt = dbConn.createStatement()

    try:
        resultSet = stmt.executeQuery(RMS_QUERY_STATISTICS % qid)
        while resultSet.next():
            variable_info = resultSet.getString('variable_info')
            if variable_info.startswith(STATS_ROW_TYPE):
                print variable_info
                break
    except SQLException, msg:
        print msg

    stmt.close()

def cancelQuery(dbConn, line):
    [command, qid, comment] = line.split(':')
    CQD = "CONTROL QUERY CANCEL QID %s COMMENT '%s'" % (qid, comment)
    stmt = dbConn.createStatement()
    try:
        stmt.execute(CQD)
        print 'OK'
    except SQLException, msg:
        print msg

    stmt.close()

def process_request(dbConn):
    line = sys.stdin.readline()
    line = line.rstrip("\n")

    if OFFENDING_QUERIES_CPU in line:
      offendingQueriesCpu(dbConn, line)
    elif OFFENDING_QUERIES_MEM in line:
      offendingQueriesMem(dbConn, line)
    elif OFFENDING_QUERIES_SQL in line:
      offendingQueriesSql(dbConn, line)
    elif RMS_STATS_QUERY in line:
      rmsStatsQuery(dbConn, line)
    elif CANCEL_QUERY in line:
      cancelQuery(dbConn, line)
    else:
      print '%s' % line
    
################################################################################
def getConnection(jdbc_url, driverName):
    try:
        Class.forName(driverName).newInstance()
    except Exception, msg:
        print msg
        sys.exit(-1)

    try:
        dbConn = DriverManager.getConnection(jdbc_url)
    except SQLException, msg:
        print >> sys.stderr, msg
        sys.exit(0)

    return dbConn
################################################################################
def main():
    global ppid

    dbConn = getConnection(JDBC_URL, JDBC_DRIVER)

    print 'CONNECTED'

    while True:
        line = sys.stdin.readline()
        line = line.rstrip("\n")

        if "START" in line:
          print 'COMMAND'
        else:
          print '%s' % line
        process_request(dbConn)
        print 'END'
        os_ppid = os.getppid()
        if ppid != os_ppid:
            sys.exit(0)

if __name__ == '__main__':
    main()

###################################################################################################################
