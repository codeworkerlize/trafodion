#
# @@@ START COPYRIGHT @@@
#
# (C) Copyright 2015-2016 Esgyn Corporation
#
# @@@ END COPYRIGHT @@@

import logging, logging.handlers
import os
import xml.etree.ElementTree as ET
import traceback
import socket
import re
from datetime import datetime
from wms_constants import *

conf = {}

def get_config(path):
  tree = ET.parse(path + WMS_CONFIG)
  root = tree.getroot()
  for property in root.findall('property'):
    name = property.find('name').text
    value = property.find('value').text
    conf[ name ] = value

def get_property(propertyName, default):
  value = ''
  try:
    value = conf[propertyName]
  except:
    value = default
  return value

path = os.path.dirname(os.path.abspath(__file__))
get_config(path)

sql_tcp_host = get_property(WMS_SQL_TCP_HOST, DEFAULT_SQL_TCP_HOST)
sql_tcp_port = int(get_property(WMS_SQL_TCP_PORT, DEFAULT_SQL_TCP_PORT))
log_tcp_host = get_property(WMS_LOG_TCP_HOST, DEFAULT_LOG_TCP_HOST)
log_tcp_port = int(get_property(WMS_LOG_TCP_PORT, DEFAULT_LOG_TCP_PORT))

tsd_host = get_property(WMS_TSD_HOST, DEFAULT_TSD_HOST)
tsd_port = int(get_property(WMS_TSD_PORT, DEFAULT_TSD_PORT))
tsd_timeout = int(get_property(WMS_TSD_TIMEOUT, DEFAULT_TSD_TIMEOUT))

script_wait_interval = int(get_property(WMS_SCRIPT_WAIT_INTERVAL, DEFAULT_SCRIPT_WAIT_INTERVAL))
zk_port = get_property(WMS_ZOOKEEPER_CLIENTPORT, DEFAULT_ZK_PORT)
zk_servers = [get_property(WMS_ZOOKEEPER_QUORUM, DEFAULT_ZK_HOST)]
zk_hosts = ''
for zk_server in zk_servers:
  if zk_hosts != '':
    zk_hosts = zk_hosts + ','
  zk_hosts = zk_hosts + zk_server + ':' + zk_port

USER = os.environ['USER']
user = get_property(WMS_ZOOKEEPER_USER, USER)

level_name = get_property(WMS_LOG_LEVEL, DEFAULT_LOG_LEVEL)
level = LEVELS.get(level_name, logging.NOTSET)
fname = get_property(WMS_LOG_FILENAME, DEFAULT_LOG_FILENAME)
#today = datetime.datetime.today()
now = datetime.now()
s = now.strftime(fname)
s = '_'.join( s.split() )
WMS_LOG_PATH = os.environ['TRAF_LOG'] + '/wms'
if not os.path.isdir(WMS_LOG_PATH):
    try:
        original_umask = os.umask(0)
        os.makedirs(WMS_LOG_PATH, mode=0777)
    finally:
        os.umask(original_umask)
filename = WMS_LOG_PATH + '/' + s
filemaxbytes = int(get_property(WMS_LOG_MAX_BYTES, DEFAULT_LOG_MAX_BYTES))
filebackupcount = int(get_property(WMS_LOG_BACKUP_COUNT, DEFAULT_LOG_BACKUP_COUNT))
format = get_property(WMS_LOG_FORMAT,DEFAULT_LOG_FORMAT)
datefmt = get_property(WMS_LOG_DATE_FORMAT,DEFAULT_LOG_DATE_FORMAT)

console_level_name = get_property(WMS_CONSOLE_LOG_LEVEL, DEFAULT_CONSOLE_LOG_LEVEL)
console_level = LEVELS.get(console_level_name, logging.NOTSET)
console_format = get_property(WMS_CONSOLE_LOG_FORMAT,DEFAULT_CONSOLE_LOG_FORMAT)
console_datefmt = get_property(WMS_CONSOLE_LOG_DATE_FORMAT,DEFAULT_CONSOLE_LOG_DATE_FORMAT)

class ContextFilter(logging.Filter):
    """
    This is a filter which injects contextual information into the log.

    """
    def __init__(self, pid=None, procname=None):
        self.pid = pid
        self.procname = procname

    def filter(self, record):
        record.hostname = socket.gethostname()
        record.pid = self.pid
        record.procname = self.procname
        stmp = '%s' % record
        if 'kazoo.client' in stmp or self.procname.endswith('.pyc'):
            return False
        return True

def create_server_logger():
  # add the log message handler to the logger
  file_handler = logging.handlers.RotatingFileHandler(filename, maxBytes=filemaxbytes, backupCount=filebackupcount)
  file_handler.setLevel(level)
  file_formatter = logging.Formatter(format, datefmt)
  file_handler.setFormatter(file_formatter)
  logging.getLogger('').addHandler(file_handler)

  # set up logging to console
  console_handler = logging.StreamHandler()
  console_handler.setLevel(console_level)
  # set a format which is simpler for console use
  formatter = logging.Formatter(console_format, console_datefmt)
  console_handler.setFormatter(formatter)
  # add the handler to the root logger
  logging.getLogger('').addHandler(console_handler)


def create_client_logger(pid=None, procname=None):

  rootLogger = logging.getLogger('')
  rootLogger.setLevel(level)
  socketHandler = logging.handlers.SocketHandler(log_tcp_host, log_tcp_port)
  # don't bother with a formatter, since a socket handler sends the event as
  # an unformatted pickle
  filter = ContextFilter(pid, procname)
  socketHandler.addFilter(filter)
  rootLogger.addHandler(socketHandler)
