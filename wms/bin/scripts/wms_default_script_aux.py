#!/usr/bin/python2.6
#
# @@@ START COPYRIGHT @@@
#
# (C) Copyright 2015-2016 Esgyn Corporation
#
# @@@ END COPYRIGHT @@@

import atexit
import signal
import exceptions
import threading
import Queue
import socket
import logging, logging.handlers
import sys
import time
import os
import json
#import psutil
from optparse import OptionParser
import logging
from kazoo.client import KazooState
from kazoo.client import KazooClient
#from kazoo.handlers.gevent import SequentialGeventHandler
from kazoo.handlers.threading import SequentialThreadingHandler
from kazoo.retry import KazooRetry
from wms_constants import *
from wms_config import create_client_logger, script_wait_interval, zk_port, zk_servers, zk_hosts, user, sql_tcp_host as host, sql_tcp_port as port, my_node, tsd_host, tsd_port
from wms_resources import offendingQueries, cancelQuery, statisticsQuery, tsdGetLastMetricValuesByTag

full_path,script_name = os.path.split(os.path.abspath(__file__))
pid = os.getpid()
create_client_logger(pid, script_name)
logger = logging.getLogger(__name__)

def signal_term_handler(signal, frame):
    sys.exit(0)
 
signal.signal(signal.SIGTERM, signal_term_handler)

global_abort = False
lock = threading.RLock()
returncode = RTN_UNKNOWN
ppid = os.getppid()
sys_resources = {}
sla = {}
queries = {}
starting = Queue.Queue()
running = Queue.Queue()

class Query(object):
  #This class keeps information about Query identified by key
  def __init__(self, key=None, value=None):
    self.key = key
    self.klist = self.key.split(':')
    self.qid = self.klist[0]
    self.host = self.klist[1]
    self.server_process_name = self.klist[2]
    self.server_nid_pid = self.klist[3]
    self.value = value
    self.vlist = self.value.split(':')
    self.est_cost = json.loads(':'.join( self.vlist[1::]))
    self.est_cost = dict((k,float(v)) for k,v in self.est_cost.iteritems())
    self.status = WMS_STATUS.get(self.vlist[0], WMS_STATUS_UNKNOWN) 

def getreturncode():
  global returncode
  with lock:
      rc = returncode
  return rc

def listener(state):
  #logger = logging.getLogger('listener')
  if state == KazooState.LOST:
    #logger.info('Register somewhere that Zookeeper session was lost')
    print  >>sys.stderr, '%d %s Register somewhere that Zookeeper session was lost' % (pid, script_name)
  elif state == KazooState.SUSPENDED:
    logger.info('Handle being disconnected from Zookeeper')
  else:
    logger.info('Handle being connected/reconnected to Zookeeper')

class ContextManager(object):
  def __init__(self, zk_hosts = None, host = None, port = None):
    # Zookeeper hosts
    self.zk_hosts = zk_hosts
    self.logger = logging.getLogger('ContextManager')

    # Create a TCP/IP socket to SQL Server
    self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.server_address = (host, port)

  def __enter__(self):
    #self.zk = KazooClient(handler=SequentialGeventHandler(),hosts=self.zk_hosts)
    self.zk = KazooClient(handler=SequentialThreadingHandler(),hosts=self.zk_hosts)
    self.zk.add_listener(listener)

    self.event = self.zk.start_async()
    # Wait for 30 seconds and see if we're connected
    self.event.wait(timeout=30)
    if not self.zk.connected:
      # Not connected, stop trying to connect
      self.logger.error('Exception : Unable to connect to zookeeper.')
      raise Exception("Unable to connect to zookeeper.")
    self.logger.info('Connected to zookeeper')
    print  >>sys.stderr, '%d %s Connected to zookeeper' % (pid, script_name)

    # Connect wms_sql to the wms sql tcp port 
    print  >>sys.stderr, '%d %s Connecting to wms_sql %s' % (pid, script_name, self.server_address)
    self.sock.connect(self.server_address)
    print  >>sys.stderr, '%d %s Connected to wms_sql ' % (pid, script_name)

    return self.zk, self.sock

  def __exit__(self, exc_type, exc_instance, traceback):
    self.logger.info('got request to Shutdown')
    print  >>sys.stderr, '%d %s got request to Shutdown' % (pid, script_name)
    self.zk.stop()
    self.sock.close()

    if not exc_type:
      return True
    if exc_type == KeyboardInterrupt:
      return True
    if exc_type == SystemExit:
      return True
    return False 

@atexit.register
def goodbye():
    global returncode
    print  >>sys.stderr, '%d %s is now leaving the Python sector. rc %d' % (pid, script_name, returncode)

def abort():
  global ppid
  global global_abort
  global returncode

  with lock:
    labort = global_abort
  os_ppid = os.getppid()
  if ppid != os_ppid or labort != False:
    return True;
  else:
    return False

def getLastMetricValuesByTag(metricName, filters):
  return tsdGetLastMetricValuesByTag(metricName, filters, tsd_host, tsd_port)

def systemLimit():
  limit = 0
  for query in queries:
    if queries[query].status == WMS_STATUS_RUNNING:
      limit += 1
  return limit

def zk_watchers(kr, zk, resources_queries_sla_znode, resources_system_znode, sla_znode, main_full_path, main_script_name):
  # Data Watcher on SLA
  @zk.DataWatch(sla_znode)
  def watch_node(data, stat):
    global global_abort
    global returncode
    if stat == None:
      with lock:
          returncode = RTN_NO_SLA
          global_abort = True;
    else:
      #attributes = data.decode("utf-8").split(':');
      attributes = data.split(':');
      for attribute in attributes:
          key, value = attribute.split('=')
          if key == 'limit' and value == '':
              value = 0
          if key == 'controlScript' and value == '':
              value = WMS_DEFAULT_SCRIPT
          try:
              sla[key] = int(value)
          except ValueError:
              sla[key] = value

      print  >>sys.stderr, '%d sla: %s' % (pid, sla)
      controlScript = sla.get('controlScript', WMS_DEFAULT_SCRIPT)

      try:
          pathscript = os.environ.get('WMS_SCRIPTS_DIR')
          if not os.path.isdir(pathscript):
              pathscript = full_path
          elif not os.path.isfile(pathscript + '/' + controlScript):
              controlScript = WMS_DEFAULT_SCRIPT
      except KeyError:
          pathscript = full_path
          controlScript = WMS_DEFAULT_SCRIPT

      if not os.path.isfile(pathscript + '/' + controlScript):
          pathscript = full_path
          controlScript = WMS_DEFAULT_SCRIPT

      if pathscript != main_full_path or controlScript != main_script_name :
          with lock:
              returncode = RTN_NEW_SCRIPT
              global_abort = True

  #DataWatcher on Resources
  @zk.DataWatch(resources_system_znode)
  def watch_throughput_znode(data, stat):
    global global_abort
    global returncode
    global system_resources
    if stat == None:
      with lock:
          returncode = RTN_NO_RESOURCE
          global_abort = True;
    else:
      #attributes = data.decode("utf-8").split(':');
      system_resources = json.loads(data)
      ##print  >>sys.stderr, '%d %s System : %s' % (pid, script_name , system_resources)

  #Children Watcher on Queries
  @zk.ChildrenWatch(resources_queries_sla_znode)
  def watch_children(children):
    global queries
    query_add = []
    query_del = queries.keys()
    logger.debug('Children are: %s' % children)
    print  >>sys.stderr, '%d %s Children are: %s' % (pid, script_name, children)
    for child in children:
      child = child.encode()
      tznode = resources_queries_sla_znode + '/' + child
      if queries.has_key(child) == False:
        value, znodeStat = kr(zk.get, resources_queries_sla_znode + '/' + child)
        if value.startswith('STARTING:') or value.startswith('RUNNING:'):
            queries[child] = Query(child, value.encode())
            query_add.append(child)
        else:
            kr(zk.delete, resources_queries_sla_znode + '/' + child)
      else:
        query_del.remove(child)

    for query in query_del:
      del queries[query]
    for query in query_add:
      if queries[query].status == WMS_STATUS_STARTING:
        starting.put(query)
      else:
        running.put(query)

def getSystemResources(sla_name):
    mem_values = system_resources['mem_values']
    cpu_values = system_resources['cpu_values']
    throughputs = system_resources['throughputs']
    if sla_name in throughputs:
        throughput = throughputs[sla_name]
    else: 
        throughput = 0
    return mem_values, cpu_values, throughput
    
