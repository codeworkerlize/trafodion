#!/usr/bin/python
#
# @@@ START COPYRIGHT @@@
#
# (C) Copyright 2015-2016 Esgyn Corporation
#
# @@@ END COPYRIGHT @@@

import signal
import atexit
import signal
import pickle
import logging
import logging.handlers
import SocketServer
import sys
import os
import struct
from wms_constants import *
from wms_config import get_property, create_server_logger, log_tcp_host as host, log_tcp_port as port

full_path,proc_name = os.path.split(os.path.abspath(__file__))
pid = os.getpid()

def signal_term_handler(signal, frame):
    sys.exit(0)
 
signal.signal(signal.SIGTERM, signal_term_handler)

class ContextManager(object):
  def __init__(self):
    pass

  def __enter__(self):
    create_server_logger()
    self.tcpserver = LogRecordSocketReceiver()
    self.logger = logging.getLogger('ContextManager')
    return self.tcpserver

  def __exit__(self, exc_type, exc_instance, traceback):
    self.logger.info("got request to Shutdown")
    print  >>sys.stderr, '%s got request to Shutdown' % proc_name
    self.tcpserver.server_abort()

    if not exc_type:
      return True
    if exc_type == KeyboardInterrupt:
      return True
    return False 

class LogRecordStreamHandler(SocketServer.StreamRequestHandler):
    """Handler for a streaming logging request.

    This basically logs the record using whatever logging policy is
    configured locally.
    """

    def handle(self):
        """
        Handle multiple requests - each expected to be a 4-byte length,
        followed by the LogRecord in pickle format. Logs the record
        according to whatever policy is configured locally.
        """
        while True:
            chunk = self.connection.recv(4)
            if len(chunk) < 4:
                break
            slen = struct.unpack('>L', chunk)[0]
            chunk = self.connection.recv(slen)
            while len(chunk) < slen:
                chunk = chunk + self.connection.recv(slen - len(chunk))
            obj = self.unPickle(chunk)
            record = logging.makeLogRecord(obj)
            self.handleLogRecord(record)

    def unPickle(self, data):
        return pickle.loads(data)

    def handleLogRecord(self, record):
        # if a name is specified, we use the named logger rather than the one
        # implied by the record.
        if self.server.logname is not None:
            name = self.server.logname
        else:
            name = record.name
        logger = logging.getLogger(name)
        # N.B. EVERY record gets logged. This is because Logger.handle
        # is normally called AFTER logger-level filtering. If you want
        # to do filtering, do it at the client end to save wasting
        # cycles and network bandwidth!
        logger.handle(record)

class LogRecordSocketReceiver(SocketServer.ThreadingTCPServer):
    """
    Simple TCP socket-based logging receiver.
    """

    allow_reuse_address = 1

    def __init__(self, 
                host = host,
                port = port,
                handler = LogRecordStreamHandler):
        print  >>sys.stderr, '%s is starting up on node: %s using port: %s' % (proc_name, host, port)
        SocketServer.ThreadingTCPServer.__init__(self, (host, port), handler)
        self.abort = 0
        self.timeout = 1
        self.logname = None

    def serve_until_stopped(self):
        import select
        abort = 0
        while not abort:
            rd, wr, ex = select.select([self.socket.fileno()],
                                       [], [],
                                       self.timeout)
            if rd:
                self.handle_request()
            abort = self.abort

    def server_abort(self):
         self.abort = 1

@atexit.register
def goodbye():
    print  >>sys.stderr, '%s is now leaving the Python sector.' % proc_name

def main():
  with ContextManager() as tcpserver: 
    print  >>sys.stderr, '%s is running...' % proc_name
    tcpserver.serve_until_stopped()

if __name__ == '__main__':
    main()
