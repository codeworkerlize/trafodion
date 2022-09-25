#!/usr/bin/python
#
# @@@ START COPYRIGHT @@@
#
# (C) Copyright 2015-2016 Esgyn Corporation
#
# @@@ END COPYRIGHT @@@

import atexit
import signal
import httplib
import socket
import sys
import os
import time
import json
import struct
from kazoo.client import KazooState
from kazoo.client import KazooClient
#from kazoo.handlers.gevent import SequentialGeventHandler
from kazoo.handlers.threading import SequentialThreadingHandler
from kazoo.retry import KazooRetry
import logging, logging.handlers
from wms_constants import *
from wms_config import script_wait_interval, create_client_logger, zk_port, zk_servers, zk_hosts, user, tsd_host as host, tsd_port as port, tsd_timeout as timeout
import traceback

full_path,proc_name = os.path.split(os.path.abspath(__file__))
pid = os.getpid()
create_client_logger(pid, proc_name)
logger = logging.getLogger(__name__)
tsd_server = ''
u_save = 0.0
n_save = 0.0
s_save = 0.0
i_save = 0.0
w_save = 0.0
x_save = 0.0
y_save = 0.0

def signal_term_handler(signal, frame):
    sys.exit(0)
 
signal.signal(signal.SIGTERM, signal_term_handler)

def listener(state):
  #logger = logging.getLogger('listener')
  if state == KazooState.LOST:
    #logger.info('Register somewhere that Zookeeper session was lost')
    print  >>sys.stderr, 'Register somewhere that Zookeeper session was lost'
  elif state == KazooState.SUSPENDED:
    logger.info('Handle being disconnected from Zookeeper')
  else:
    logger.info('Handle being connected/reconnected to Zookeeper')

class ContextManager(object):
  def __init__(self, zk_hosts = None, host=None, port=None, server=None, timeout=10):
    global tsd_server
    # Zookeeper hosts
    self.zk_hosts = zk_hosts
    self.timeout = timeout
    self.server = '%s:%d' % (host, port)
    tsd_server = self.server
    socket.setdefaulttimeout(self.timeout)
    self.logger = logging.getLogger('ContextManager')

  def __enter__(self):
    #self.zk = KazooClient(handler=SequentialGeventHandler(),hosts=self.zk_hosts)
    self.zk = KazooClient(handler=SequentialThreadingHandler(),hosts=self.zk_hosts)
    self.zk.add_listener(listener)

    self.event = self.zk.start_async()
    # Wait for 30 seconds and see if we're connected
    self.event.wait(timeout=30)
    if not self.zk.connected:
      # Not connected, stop trying to connect
      self.logger.error("Exception : Unable to connect to zookeeper.")
      raise Exception("Unable to connect to zookeeper.")
    self.logger.info("Connected to zookeeper")
    #print  >>sys.stderr, 'Connected to zookeeper'

    self.logger.info ('Connecting to TSD Server [%s] timeout %d' % (self.server, timeout))
    self.conn = httplib.HTTPConnection
    self.conn = self.conn(self.server, timeout=self.timeout)
    self.conn.connect()
    self.peer = self.conn.sock.getpeername()
    self.logger.info ('Connected to TSD Server [%s:%d]' % (self.peer[0], self.peer[1]))
    return self.zk, self.conn

  def __exit__(self, exc_type, exc_instance, traceback):
    self.logger.info('got request to Shutdown')
    print  >>sys.stderr, '%s got request to Shutdown' % proc_name
    if exc_type == socket.error:
      self.logger.error("Couldn't connect to %s: %s" % (self.server, exc_instance))

    self.zk.stop()
    self.conn.close()

    if not exc_type:
      return True
    if exc_type == KeyboardInterrupt:
      return True
    if exc_type == SystemExit:
      return True
    return False 

@atexit.register
def goodbye():
    print  >>sys.stderr, '%s is now leaving the Python sector.' % proc_name

def get_tsd(conn, url):
    global tsd_server
    try:
      logger.debug ('Sending GET request to TSD')
      conn.request('GET', url)
      res = conn.getresponse()
      datapoints = res.read()
      #conn.close()
    except socket.error, e:
      logger.error ("Couldn't GET %s from %s: %s [%ds]" % (url,tsd_server,e,timeout))
      raise
    if res.status not in (200, 202):
      logger.critical ('Status = %d when talking to %s' % (res.status, tsd_server))
      logger.critical ('TSD said:')
      logger.critical (datapoints)
      raise Exception('CRITICAL: status = %d when talking to %s' % (res.status, tsd_server))
    logger.debug((datapoints))
    if not len(datapoints):
      logger.critical ('Query did not return any data point.')
      raise Exception('CRITICAL: Query did not return any data point')
    return datapoints

def get_memory_values(conn):
    url_memory = '/api/query?start=60s-ago&m=avg:30s-avg:proc.meminfo.memfree;start=30m-ago&m=avg:15s-avg:proc.meminfo.memtotal;start=30m-ago&m=avg:15s-avg:proc.meminfo.cached;start=30m-ago&m=avg:15s-avg:proc.meminfo.buffers'
    mem_free = 0.0
    mem_total = 0.0
    mem_cached = 0.0
    mem_buffers = 0.0
    mem_usage = 0.0

    mem_values = {}

    jres = get_tsd(conn, url_memory)
    lmem = json.loads(jres)
    if lmem:
        dps_mfree = lmem[0]['dps']
        dps_mtotal = lmem[1]['dps']
        dps_cached = lmem[2]['dps']
        dps_buffers = lmem[3]['dps']
        #---- mem free
        if dps_mfree:
            keys = dps_mfree.keys()
            mem_values['mem_free'] = float(dps_mfree[max(keys)])
        #----- mem total
        if dps_mtotal:
            keys = dps_mtotal.keys()
            mem_values['mem_total'] = float(dps_mtotal[max(keys)])
        #----- cached
        if dps_cached:
            keys = dps_cached.keys()
            mem_values['mem_cached'] = float(dps_cached[max(keys)])
        #----- buffers
        if dps_buffers:
            keys = dps_buffers.keys()
            mem_values['mem_buffers'] = float(dps_buffers[max(keys)])
        #--------------------
    return mem_values

def get_cpu_values(conn):
    url_cpu = '/api/query?start=30s-ago&m=avg:15s-avg:proc.stat.cpu{type=user|nice|system|idle|iowait|irq|softirq}'

    cpu_values = {}

    u = 0.0
    n = 0.0
    s = 0.0
    i = 0.0
    w = 0.0
    x = 0.0
    y = 0.0

    jres = get_tsd(conn, url_cpu)
  
    lcpu = json.loads(jres)
    if lcpu:
        for dcpu in lcpu:
            type_name = dcpu['tags']['type']
            dps = dcpu['dps']
            if dps:
                keys = dps.keys()
                value = int(dps[max(keys)])
                cpu_values[type_name] = float(value)

    return cpu_values

def get_throughputs(conn):
    thru = {}
    url_thru = '/api/query?start=10m-ago&m=avg:60s-avg:esgyndb.mxosrvr.aggrstat.total_completed_stmts{SLAName=*}'

    jres = get_tsd(conn, url_thru)
    lthru = json.loads(jres)
    if lthru:
        for elem in lthru:
            sla_name = elem['tags']['SLAName']
	    thru[sla_name] = 0
            dps = elem['dps']
	    if dps:
            	keys = dps.keys()
            	value = int(dps[max(keys)])
            	if not sla_name in thru:
                	thru[sla_name] = value
            	else:
                	thru[sla_name] += value
		
    return thru

def main(argv):
  print  >>sys.stderr, '%s is running...' % proc_name
  logger = logging.getLogger(__name__)

  with ContextManager(zk_hosts, host, port, timeout) as [zk, conn]:
      now = int(time.time())
      resources_znode = '/' + user + '/' + WMS_RESOURCES + '/system'
      kr = KazooRetry(max_tries=3, ignore_expire=False)

      while True:
        system_resources = {}
        mem_values = {}
        cpu_values = {}
        throughputs = {}

        mem_values = get_memory_values(conn)
        cpu_values = get_cpu_values(conn)
        throughputs = get_throughputs(conn)
        
        system_resources['mem_values'] = mem_values
        system_resources['cpu_values'] = cpu_values
        system_resources['throughputs'] = throughputs
        jsystem_resources = json.dumps(system_resources)
        kr(zk.set,resources_znode,jsystem_resources)
        
        tcounter = 0
        while (tcounter < script_wait_interval):
            tcounter += 1
            time.sleep(1)

if __name__ == '__main__':
  sys.exit(main(sys.argv))
