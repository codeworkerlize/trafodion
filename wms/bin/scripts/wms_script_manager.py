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
import shlex, subprocess
import threading
from multiprocessing import Queue
#import gevent
import logging
import logging, logging.handlers
import sys
import time
import os
import json
import ConfigParser
import xml.etree.ElementTree as ET
from kazoo.client import KazooState
from kazoo.client import KazooClient
#from kazoo.handlers.gevent import SequentialGeventHandler
from kazoo.handlers.threading import SequentialThreadingHandler
from kazoo.retry import KazooRetry
from wms_constants import *
from wms_config import create_client_logger, script_wait_interval, zk_port, zk_servers, zk_hosts, user, my_node

full_path, proc_name = os.path.split(os.path.abspath(__file__))
pid = os.getpid()
create_client_logger(pid, proc_name)
logger = logging.getLogger(__name__)

abort = False
slas = {}
sla_queue = Queue()

def signal_term_handler(signal, frame):
    sys.exit(0)
 
signal.signal(signal.SIGTERM, signal_term_handler)

def listener(state):
  #logger = logging.getLogger('listener')
  if state == KazooState.LOST:
    #logger.info('Register somewhere that Zookeeper session was lost')
    print 'Register somewhere that Zookeeper session was lost'
  elif state == KazooState.SUSPENDED:
    logger.info('Handle being disconnected from Zookeeper')
  else:
    logger.info('Handle being connected/reconnected to Zookeeper')

class Process(object):
    """This class spawns a subprocess asynchronously"""
    def __call__(self, sla, zk ):
      # store the arguments
      self.sla = sla
      self.zk = zk

      def target(sla, zk ):
          global abort
          global slas
          while True:
              if abort == True:
                  break;
              sla_attrs = {}
              sla_znode = '/' + user + '/' + WMS_SLAS + '/' + sla
              value = zk.get(sla_znode)
              attrs = value[0].split(':');
              for attr in attrs:
                  key, value = attr.split('=')
                  if key == 'limit' and value == '':
                      value = 0
                  if key == 'controlScript' and value == '':
                      value = WMS_DEFAULT_SCRIPT
                  sla_attrs[key] = value
              controlScript = sla_attrs.get('controlScript', WMS_DEFAULT_SCRIPT)
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

#Send information to log which script is being used for the sla....log info
              logger.info('user script: path :%s, script name: %s'  % ( pathscript, controlScript))
              cmd = PYTHON + ' ' + pathscript + '/' + controlScript + ' --sla ' + sla
              proc = subprocess.Popen(shlex.split(cmd))
              if proc.pid > 0:
                  logger.info('spawned %s (pid=%d)' % ( cmd, proc.pid))
                  print  >>sys.stderr, '%d %s spawned %s )' % (proc.pid, threading.currentThread().getName(), cmd)
                  if abort == True:
                    proc.terminate()
                  # the `multiprocessing.Process` process will wait until
                  # the call to the `subprocess.Popen` object is completed
                  slas[sla].proc = proc
                  returncode = proc.wait()
                  slas[sla].proc = None
                  print  >>sys.stderr, '%d %s exited returncode [%d] %s' % (proc.pid, threading.currentThread().getName(), returncode, RTN_TEXT.get(returncode, 'UNKNOWN'))
                  if returncode != RTN_NEW_SCRIPT:
                      break;

      # create the thread which will spawn the Process
      thread = threading.Thread(target=target, args=(self.sla, self.zk) )
      # this call issues the call to `target`, but returns immediately
      thread.start()
      slas[sla].thread = thread
      return thread

class Sla(object):
  """This class keeps information about threads"""
  def __init__(self, sla=None, thread=None, proc=None):
    self.sla = sla
    self.thread = thread
    self.proc = proc

class ContextManager(object):
  def __init__(self, zk_hosts = None):
    # Zookeeper hosts
    self.zk_hosts = zk_hosts
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
    self.run_process = Process()
    print  >>sys.stderr, 'wms_script_manager.py Connected to zookeeper'
    return self.zk, self.run_process

  def __exit__(self, exc_type, exc_instance, traceback):
    self.logger.info('got request to Shutdown')
    print  >>sys.stderr, '%s got request to Shutdown' % proc_name
    global abort
    global sla_queue

    abort = True
    self.zk.stop()
    while sla_queue.empty() == False:
        j = sla_queue.get()
    #for sla in slas:
    #  slas[sla].thread.join()
    if not exc_type:
        return True
    if exc_type == KeyboardInterrupt:
        return True
    if exc_type == Exception:
        return True
    if exc_type == exceptions.TypeError:
        return True
    return False 

@atexit.register
def goodbye():
    print  >>sys.stderr, '%s is now leaving the Python sector.' % proc_name

def main(argv):
  print  >>sys.stderr, '%s is running...' % proc_name 
  logger = logging.getLogger( __name__ )
  logger.info("Zookeeper hosts :%s" % zk_hosts)

  with ContextManager(zk_hosts) as [zk, run_process]:
    kr = KazooRetry(max_tries=3, ignore_expire=False)
    # Determine if a node exists
    parent = '/' + user + '/' + WMS_RESOURCES_QUERIES
    if kr(zk.exists,parent):
      znode = '/' + user + '/' + WMS_RESOURCES_QUERY_MANAGER
      if kr(zk.exists, path=znode):
        kr(zk.delete, path=znode)
      kr(zk.create, path=znode, ephemeral=True)

      @zk.ChildrenWatch(parent)
      def watch_children(children):
        global slas
        sla_add = []
        sla_del = slas.keys()
        logger.debug("Children are: %s" % children)
        for child in children:
          if slas.has_key(child) == False:
            child = child.encode()
            slas[child] = Sla(child)
            sla_add.append(child)
          else:
            sla_del.remove(child)
        for sla in sla_del:    
          del slas[sla]
        for sla in sla_add:
          sla_queue.put(sla)
    else:
      logger.critical("Parent zk node doesn't exist :%s" % parent)
      raise Exception('EXIT')

    while True:
      sla = sla_queue.get()
      run_process(sla, zk)
      while sla_queue.empty():
        time.sleep(1)

if __name__ == "__main__":
    sys.exit(main(sys.argv))

