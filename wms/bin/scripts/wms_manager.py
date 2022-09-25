#!/usr/bin/python
#
# @@@ START COPYRIGHT @@@
#
# (C) Copyright 2015-2016 Esgyn Corporation
#
# @@@ END COPYRIGHT @@@

import atexit
import signal
import shlex, subprocess
import sys
import time
import os
import json
import socket
import threading
import subprocess
import multiprocessing
import re
from wms_constants import *
from wms_config import get_property, create_client_logger

full_path, proc_name = os.path.split(os.path.abspath(__file__))
pid = os.getpid()
create_client_logger(pid, proc_name)
logger = logging.getLogger(__name__)

threads = []
abort = False
procs = {}

def signal_term_handler(signal, frame):
    sys.exit(0)

#kill signal handler without -9
signal.signal(signal.SIGTERM, signal_term_handler)   

#Used to intercept all exceptions 
class ContextManager(object):
  def __init__(self):
    self.logger = logging.getLogger('ContextManager')
  def __enter__(self):
    self.run_process = Process()
    return self.run_process
  def __exit__(self, exc_type, exc_instance, traceback):
    self.logger.info('%s got request to Shutdown' % proc_name)
    print  >>sys.stderr, '%s got request to Shutdown' % proc_name
    global abort
    abort = True

    while procs.keys():
      for key in procs.keys():
        proc = procs[key]
        while proc.poll() is None:
          proc.terminate()
          timer = threading.Timer(5, proc.kill) #Waits for 5 secs for process to terminate
          timer.start()
          proc.wait()
          if timer.is_alive():
            # Process completed naturally - cancel timer
            timer.cancel()
        del procs[key]

    for thrd in threads:
      thrd.join()

    if not exc_type:
      return True
    if exc_type == KeyboardInterrupt:
      return True
    if exc_type == SystemExit:
      return True
    return False 

class Process(object):
    """This class spawns a subprocess asynchronously"""
    def __call__(self, *args):
      # store the arguments
      self.args = args
      self.cmd = ''
      for arg in self.args:
        self.cmd = self.cmd + ' ' + str(arg)
      args = shlex.split(self.cmd)
      # define the target function to be called by
      # `multiprocessing.Process`
      def target(cmd):
        global abort
        global procs
        cnt = 10
        timestamp = time.time()
        
        while cnt > 0:
          if abort == True:
            break
          cnt -= 1
          proc = subprocess.Popen(args )
          if proc.pid > 0:
            logger.info('spawned %s (pid=%d)' % ( cmd, proc.pid))
            #print ('%s spawned %s \(pid=%d\)' % ( threading.currentThread().getName(), cmd, proc.pid))
            procs[timestamp] = proc
          else:
            #print ('process can not be spawned %s' % cmd)
            break 
          # the `multiprocessing.Process` process will wait until
          # the call to the `subprocess.Popen` object is completed
          returncode = proc.wait()
          cnt -= 1
      # this call issues the call to `target`, but returns immediately
      thread = threading.Thread(target=target, args=(self.cmd,))
      #thread.setDaemon(True)
      threads.append(thread)
      thread.start()
      return thread

@atexit.register
def goodbye():
    print  >>sys.stderr, '%s is now leaving the Python sector.' %proc_name

def main(argv):
 
    with ContextManager() as run_process:
 
      # spawn process runs with different arguments
      run_process(PYTHON + ' ' + full_path + '/' + WMS_LOGGER)
      run_process(PYTHON + ' ' + full_path + '/' + WMS_SQL)
      run_process(PYTHON + ' ' + full_path + '/' + WMS_SCRIPT_MANAGER)
      run_process(PYTHON + ' ' + full_path + '/' + WMS_TSD)

      while True:
         time.sleep(1)
     
if __name__ == "__main__":
    sys.exit(main(sys.argv))

