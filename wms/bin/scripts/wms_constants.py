#
# @@@ START COPYRIGHT @@@
#
# (C) Copyright 2015-2016 Esgyn Corporation
#
# @@@ END COPYRIGHT @@@

import logging
import ctypes

PYTHON = '/usr/bin/python2.6'
JYTHON = 'jython'

WMS_CONFIG = '/../../conf/wms-site.xml'
WMS_MANAGER = 'wms_manager.py'
WMS_LOGGER = 'wms_logger.py'
WMS_SQL = 'wms_sql.py'
WMS_SCRIPT_MANAGER = 'wms_script_manager.py'
WMS_TSD = 'wms_tsd.py'
WMS_DEFAULT_SCRIPT = 'wms_default_script.py'
WMS_OFFENDER = 'wms_offender.py'

#CRITICAL       50
#ERROR          40
#WARNING        30
#INFO           20
#DEBUG          10
#UNSET          0

LEVELS = {  'debug':logging.DEBUG,
            'info':logging.INFO,
            'warning':logging.WARNING,
            'error':logging.ERROR,
            'critical':logging.CRITICAL,
            }
DEFAULT_LOG_FILENAME = 'wms.%Y-%m-%d-%H:%M:%S.log'
DEFAULT_LOG_MAX_BYTES = '5000'
DEFAULT_LOG_BACKUP_COUNT = '5'
DEFAULT_LOG_FORMAT = '%(asctime)s, %(%levelname)s, %(pathname)s:%(lineno)d} - %(message)s'
DEFAULT_LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
DEFAULT_LOG_LEVEL = 'info'
DEFAULT_CONSOLE_LOG_LEVEL = 'error'
DEFAULT_CONSOLE_LOG_FORMAT = '%(name)-12s: %(levelname)-8s %(message)s'
DEFAULT_CONSOLE_LOG_DATE_FORMAT = '%H:%M:%S'

DEFAULT_LOG_TCP_HOST = 'localhost'
DEFAULT_LOG_TCP_PORT = '4208'
DEFAULT_SQL_TCP_HOST = 'localhost'
DEFAULT_SQL_TCP_PORT = '4207'

DEFAULT_TSD_HOST = 'localhost'
DEFAULT_TSD_PORT = '5242'
DEFAULT_TSD_TIMEOUT = '30'

DEFAULT_ZK_HOST = 'localhost'
DEFAULT_ZK_PORT = '2181'
DEFAULT_SCRIPT_WAIT_INTERVAL = '30'

WMS_SCRIPT_WAIT_INTERVAL = 'wms.script.wait.interval'

WMS_LOG_TCP_HOST ='wms.log.tcp.host'
WMS_LOG_TCP_PORT = 'wms.log.tcp.port'
WMS_SQL_TCP_HOST = 'wms.sql.tcp.host'
WMS_SQL_TCP_PORT = 'wms.sql.tcp.port'

WMS_LOG_LEVEL = 'wms.log.level'
WMS_LOG_FORMAT = 'wms.log.format'
WMS_LOG_FILENAME = 'wms.log.filename'
WMS_LOG_MAX_BYTES = 'wms.log.max.bytes'
WMS_LOG_BACKUP_COUNT = 'wms.log.backup.count'
WMS_LOG_DATE_FORMAT = 'wms.log.date.format'
WMS_CONSOLE_LOG_LEVEL = 'wms.console.log.level'
WMS_CONSOLE_LOG_FORMAT = 'wms.console.log.format'
WMS_CONSOLE_LOG_DATE_FORMAT = 'wms.console.log.date.format'

WMS_TSD_PORT = 'wms.tsd.port'
WMS_TSD_HOST = 'wms.tsd.host'
WMS_TSD_TIMEOUT = 'wms.tsd.timeout'

WMS_ZOOKEEPER_CLIENTPORT = 'wms.zookeeper.clientport'
WMS_ZOOKEEPER_QUORUM = 'wms.zookeeper.quorum'
WMS_ZOOKEEPER_USER = 'wms.zookeeper.user'

WMS_MMA_FILENAME = 'wms.mma.filename'

WMS_SCRIPTS_DIR = 'WMS_PATH_USER_SCRIPT' 
WMS_SLAS = 'wms/slas'
WMS_RESOURCES = 'wms/resources'
WMS_RESOURCES_QUERIES = WMS_RESOURCES + '/queries'
WMS_RESOURCES_QUERY_MANAGER = WMS_RESOURCES + '/query_manager'
WMS_STATUS_UNKNOWN = 0
WMS_STATUS_STARTING = 1
WMS_STATUS_RUNNING = 2
WMS_STATUS_COMPLETED = 3
WMS_STATUS_REJECTED = 4
WMS_STATUS_CANCELED = 5
WMS_STATUS_WAITING = 6
WMS_STATUS = {
          'STARTING':WMS_STATUS_STARTING,
          'RUNNING':WMS_STATUS_RUNNING,
          'COMPLETED':WMS_STATUS_COMPLETED,
          'REJECTED':WMS_STATUS_REJECTED,
          'CANCELED':WMS_STATUS_CANCELED,
          'WAITING':WMS_STATUS_WAITING,
          }

RTN_TERMINATE = -15
RTN_KILL = -9
RTN_UNKNOWN = 0
RTN_NEW_SCRIPT = 100
RTN_NO_NODE = 200
RTN_NO_SLA = 300
RTN_NO_RESOURCE = 400
RTN_TEXT = {  RTN_TERMINATE:'TERMINATED',
              RTN_KILL:'KILLED',
              RTN_UNKNOWN:'UNKNOWN',
              RTN_NEW_SCRIPT:'SCRIPT CHANGED',
              RTN_NO_NODE:'THERE IS NO NODE',
              RTN_NO_SLA:'THERE IS NO SLA',
              RTN_NO_RESOURCE:'THERE IS NO RESOURCE NODE',
           }

OFFENDING_QUERIES_CPU = 'offendingQueriesCpu'
OFFENDING_QUERIES_MEM = 'offendingQueriesMem'
OFFENDING_QUERIES_SQL = 'offendingQueriesSql'
# RMS:QID
RMS_STATS_QUERY = 'rmsStatsQuery'
# CANCEL:QID:COMMENT
CANCEL_QUERY = 'cancelQuery'

class STATUS(ctypes.Structure):
	_fields_ = [('status', ctypes.c_char * 8)]

class AVERAGE(ctypes.Structure):
	_fields_ = [('memory', ctypes.c_long), ('cpu', ctypes.c_long), ('reserve', 2 * ctypes.c_long)]

class LIMIT(ctypes.Structure):
	_fields_ = [('counter', ctypes.c_short), ('limit', ctypes.c_char * 6120)]

class TROUGHPUT(ctypes.Structure):
	_fields_ = [('counter', ctypes.c_short), ('throughput', ctypes.c_char * 6120)]


