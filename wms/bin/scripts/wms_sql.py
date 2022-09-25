#!/usr/bin/python2.6
#
# @@@ START COPYRIGHT @@@
#
# (C) Copyright 2015-2016 Esgyn Corporation
#
# @@@ END COPYRIGHT @@@

import atexit
import signal
import select
import socket
import shlex, subprocess
import time
import sys
import os
import Queue
import json
from wms_constants import *
from wms_config import get_property, create_client_logger, sql_tcp_host as host,  sql_tcp_port as port, my_node
#import config

full_path, proc_name = os.path.split(os.path.abspath(__file__))
pid = os.getpid()
create_client_logger(pid, proc_name)
logger = logging.getLogger(__name__)

def signal_term_handler(signal, frame):
    sys.exit(0)
 
signal.signal(signal.SIGTERM, signal_term_handler)

class ContextManager(object):
  def __init__(self, host = None, port = None):
    self.host = host
    self.port = port
    # Create a TCP/IP socket
    self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.server.setblocking(0)
    self.logger = logging.getLogger('ContextManager')
    print  >>sys.stderr, '%s is starting up on node: %s using port: %d' % (proc_name, self.host, self.port)
    self.logger.info('Starting up on %s port %d' % (self.host, self.port))
    # Create parameters (cmd and env) for Popen offender
    self.env = os.environ
    #setup LD_PRELOAD environment variable for T2Driver
    self.sq_root = os.environ.get("TRAF_HOME")
    self.mb_type = os.environ.get("SQ_MBTYPE")
    self.java_home = os.environ.get("JAVA_HOME")
    self.ldp = self.java_home + "/jre/lib/amd64/libjsig.so:"  + self.sq_root +"/export/lib" + self.mb_type + "/libseabasesig.so"
    #print self.ldp
    self.env["LD_PRELOAD"] = self.ldp
    self.cmd = 'jython ' + full_path + '/' + WMS_OFFENDER

  def __enter__(self):
    # Bind the socket to the port also reuse the port if it is in TIME_WAIT state
    self.server_address = (self.host, self.port)
    self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    self.server.bind(self.server_address)

    # Listen for incoming connections (max 5 waiting connections)
    self.server.listen(5)
    # Sockets from which we expect to read
    self.inputs = [ self.server ]
    # Sockets to which we expect to write
    self.outputs = [ ]
    # Outgoing message queues (socket:Queue)
    self.message_queues = {}

    self.proc = subprocess.Popen(shlex.split(self.cmd),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    stdin=subprocess.PIPE,
                    close_fds=True,
                    bufsize=0,
                    env=self.env)

    if not self.proc.pid > 0:
      self.logger.info('Popen offender.py failed :' % (self.proc))
      sys.exit(-1)
    # wait max 5 seconds until offender process is ready
    self.wait_remaining_sec = 5
    while self.proc.poll() is None and self.wait_remaining_sec > 0:
        try: 
            time.sleep(1) 
        except:
            pass
        self.wait_remaining_sec -= 1

    if self.proc.poll() is not None:
        self.logger.info('wms_offender.py exited after %d seconds' % (5 - self.wait_remaining_sec))
        print  >>sys.stderr, '%s :wms_offender.py exited after %d seconds' % (proc_name, (5 - self.wait_remaining_sec))
        sys.exit(-1)

    [self.tlast, self.line] = readoutput(self.proc)
    print  >>sys.stderr, '%s State: %s from wms_offender.py time: %d' % (proc_name, self.line, self.tlast)

    # wait max 5 seconds until we get from offender 'CONNECTED' to T2 Driver
    self.wait_remaining_sec = 5
    while 'CONNECTED' not in self.line  and self.wait_remaining_sec > 0:
        time.sleep(1)
        self.wait_remaining_sec -= 1
        [self.tlast, self.line] = readoutput(self.proc)

    if 'CONNECTED' not in self.line:
        self.logger.debug('waited for CONNECTED %d seconds' % (5 - self.wait_remaining_sec))
        print  >>sys.stderr, '%s waited for CONNECTED %d seconds' % (proc_name,(5 - self.wait_remaining_sec))
        sys.exit(-1)

    return self.server, self.inputs, self.outputs, self.message_queues, self.proc

  def __exit__(self, exc_type, exc_instance, traceback):
    self.logger.info('Got request to Shutdown')
    print  >>sys.stderr, '%s got request to Shutdown' % proc_name
    
    for s in self.inputs:
      # Stop listening for input on the connection
      if s in self.inputs:
        self.inputs.remove(s)
        s.shutdown(socket.SHUT_RDWR)
        s.close()
      if s in self.outputs:
        self.outputs.remove(s)
        s.shutdown(socket.SHUT_RDWR)
        s.close()
      # Remove message queue
      if s in self.message_queues:
        del self.message_queues[s]

    if self.proc is not None and self.proc.poll() is None:
      self.proc.terminate()
    
    if not exc_type:
      return True
    if exc_type == KeyboardInterrupt:
      return True
    return False 

@atexit.register
def goodbye():
    print >>sys.stderr, '%s is now leaving the Python sector.' % proc_name

def readoutput(proc):
  tstart = int(round(time.time() * 1000))
  line = ''
  while not line:
      try:
        line = proc.stdout.readline()
        line = line.rstrip("\n")
        if not line:
          time.sleep(0.1)
          continue

      except IOError, (err, msg):
        if err != errno.EAGAIN:
            raise
      except AttributeError:
        print  >>sys.stderr, '%s caught exception, process went away while reading stdout' % proc_name
        sys.exit(200)
      except:
        print >>sys.stderr, '%s uncaught exception in stdout read' % proc_name
        sys.exit(300)
  tend = int(round(time.time() * 1000))
  return (tend-tstart), line

def sendoffender(proc, request):
    lst = []
    # send START to offender
    proc.stdin.write('START\n')
    [tlast, line] = readoutput(proc)
    print  >>sys.stderr, '%s:sendoffender State: %s time: %d' % (proc_name, line, tlast)
    # when offender is ready for new request, sends COMMAND
    if 'COMMAND' in line:
        print  >>sys.stderr, '%s:sendoffender command: %s' % (proc_name, request)
        # new request is sent to offender
        proc.stdin.write('%s\n' % request)
        [tlast, line] = readoutput(proc)
        while True:
            # END from offender means end of data
            if 'END' in line:
                break;
            print  >>sys.stderr, '%s:sendoffender State: %s time: %d' % (proc_name, line, tlast)
            lst.append(line)
            [tlast, line] = readoutput(proc)
    return lst

def offendingQueries(proc):
    offendingQueries = []
    offendingCpu = []
    offendingMem = []
    offendingSql = []

    offendingCpu = sendoffender(proc, OFFENDING_QUERIES_CPU)
    offendingMem = sendoffender(proc, OFFENDING_QUERIES_MEM)
    offendingSql = sendoffender(proc, OFFENDING_QUERIES_SQL)

    offendingQueries.append(offendingCpu)
    offendingQueries.append(offendingMem)
    offendingQueries.append(offendingSql)
    return json.dumps(offendingQueries)

def cancelQuery(proc, qid, comment):
    lst = sendoffender(proc, '%s:%s:%s' % (CANCEL_QUERY, qid, comment))
    str = ':'.join(lst)
    print  >>sys.stderr, '%s:cancelQuery State: %s' % (proc_name, str)
    return str

def rmsStatsQuery(proc, qid):
    rmsStats = {}
    lst = sendoffender(proc, '%s:%s' % (RMS_STATS_QUERY, qid))
    if not lst[0].startswith('java.sql.SQLException:'):
        mstr = lst[0].replace(': ',':')
        rmsStats = dict((p.split(':') for p in shlex.split(mstr)))
    else:
        rmsStats['SqlException'] = lst[0] 
    return json.dumps(rmsStats)

def main(argv):
  logger = logging.getLogger(__name__)

  with ContextManager(host, port) as [server, inputs, outputs, message_queues, proc]:
 
    while inputs:
      # Wait for at least one of the sockets to be ready for processing
      #
      #print >>sys.stderr, '\nWaiting for the next event'
      readable, writable, exceptional = select.select(inputs, outputs, inputs)
      """
      timeout = 1
      readable, writable, exceptional = select.select(inputs, outputs, inputs, timeout)

      if not (readable or writable or exceptional):
        #print >>sys.stderr, '  timed out, do some other work here'
        continue
      # Wait for at least one of the sockets to be ready for processing
      print >>sys.stderr, '\nWaiting for the next event'
      readable, writable, exceptional = select.select(inputs, outputs, inputs)
      """
      # Handle inputs
      for s in readable:

        if s is server:
          # A "readable" server socket is ready to accept a connection
          connection, client_address = s.accept()
          print >>sys.stderr, '%s new connection from %s' %(proc_name, client_address)
          connection.setblocking(0)
          inputs.append(connection)

          # Give the connection a queue for data we want to send
          message_queues[connection] = Queue.Queue()
        else:
          data = s.recv(1024)
          if data:
            # A readable client socket has data
            request_len = data[:6]
            request = data[6:]
            print >>sys.stderr, '%s received request_len %s, request "%s" from %s' % (proc_name, request_len, request, s.getpeername())
            if request == 'offendingQueries':
              data = offendingQueries(proc)
            elif request.startswith('cancel:'):
              # CANCEL:QID:COMMENT
              lparam = request.split(':')
              request = lparam[0]
              qid = lparam[1]
              comment = lparam[2]
              data = cancelQuery(proc, qid, comment)
            elif request.startswith('rms:'):
              # RMS:QID
              lparam = request.split(':')
              request = lparam[0]
              qid = lparam[1]
              data = rmsStatsQuery(proc, qid)

            data = request + data
            data = '%06d%s' % (len(data) + 6, data)
            print >>sys.stderr, '%s sends back "%s" to %s' % (proc_name, data, s.getpeername())
            message_queues[s].put(data)
            # Add output channel for response
            if s not in outputs:
              outputs.append(s)
            else:
              # Interpret empty result as closed connection
              print >>sys.stderr, 'closing', client_address, 'after reading no data'
              # Stop listening for input on the connection
              if s in outputs:
                outputs.remove(s)
                inputs.remove(s)
                s.close()

                # Remove message queue
                del message_queues[s]

      # Handle outputs
      for s in writable:
        try:
          next_msg = message_queues[s].get_nowait()
        except Queue.Empty:
          # No messages waiting so stop checking for writability.
          print >>sys.stderr, '%s output queue for %s is empty' % (proc_name, s.getpeername())
          outputs.remove(s)
        else:
          print >>sys.stderr, '%s sending "%s" to %s' % (proc_name, next_msg, s.getpeername())
          s.send(next_msg)

      # Handle "exceptional conditions"
      for s in exceptional:
        print >>sys.stderr, '%s handling exceptional condition for %s' % (proc_name, s.getpeername())
        # Stop listening for input on the connection
        inputs.remove(s)
        if s in outputs:
          outputs.remove(s)
        s.close()

        # Remove message queue
        del message_queues[s]

if __name__ == "__main__":
  sys.exit(main(sys.argv))
