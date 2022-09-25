#!/usr/bin/env python

# @@@ START COPYRIGHT @@@
#
# (C) Copyright 2018-2019 Esgyn Corporation
#
# @@@ END COPYRIGHT @@@
import sys
import getpass
import json
import os
import subprocess
import datetime
import socket
import argparse
import time

parser = argparse.ArgumentParser()
import logging, logging.handlers

traf_log = os.environ.get("TRAF_LOG")
traf_agent = os.environ.get("TRAF_AGENT")

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)
# Create the Handler for logging data to a file
logger_handler = logging.handlers.RotatingFileHandler(traf_log + '/edb_copyto_allnodes.log', 'a', 10000000, 5)
logger_handler.setLevel(logging.INFO)
 
# Create a Formatter for formatting the log messages
customFields = { 'component': 'EDB_COPYTOALL', 'host': socket.gethostname() }
logger_formatter = logging.Formatter('%(asctime)s, %(levelname)s, %(component)s, Node: %(host)s, PIN: %(process)d, Process Name: %(processName)s,,,%(message)s')
 
# Add the Formatter to the Handler
logger_handler.setFormatter(logger_formatter)
 
# Add the Handler to the Logger
logger.addHandler(logger_handler)

# Add the custom fields to the logger format
logger = logging.LoggerAdapter(logger, customFields)

def get_options():
    usage = 'usage: %prog [options]\n'
    usage += '  EsgynDB script to propagate file to all EsgynDB Nodes.'
    parser = OptionParser(usage=usage)
    parser.add_option("-s", "--source", dest="src_file", metavar="SRCFILE", default='',
                      help="The source file to copy to all nodes")
    parser.add_option("-t", "--target", dest="dest_file", metavar="DSTFILE", default='', 
                      help="The destination file. If not specified, the same file name as source will be used.")

    (options, args) = parser.parse_args()
    return options

def _exec_cmd(cmd):
    logger.info('executing command %s' % cmd)
    p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, executable='/bin/bash')
    stdout, stderr = p.communicate()
    msg = stdout
    if p.returncode != 0:
        msg = stderr if stderr else stdout
    return p.returncode, msg

def _copy_to_all_nodes(src_file, dest_file):
   nameparts = src_file.split("/")
   local_file_name = nameparts[-1]
   dest_file_name = dest_file
   tmp_dest_file_name = dest_file

   if traf_agent:
      logger.info('Agent mode. Using hdfs sync.')

      hdfs_dir = '/user/trafodion/.sync/'
      hdfs_file = hdfs_dir + local_file_name

      retcode, msg = _exec_cmd('hdfs dfs -mkdir -p %s' % hdfs_dir)
      if retcode != 0:
         logger.error('Failed to create sync dir in hdfs: %s' % msg)
	 return

      retcode, msg = _exec_cmd('hdfs dfs -copyFromLocal -f %s %s' % (src_file, hdfs_file))
      if retcode !=0 :
         logger.error('Failed to copy file %s to hdfs sync directory: %s' % (src_file, msg))
         return

      if os.path.isfile(dest_file):
         retcode, msg = _exec_cmd('edb_pdsh -a cp -f %s %s.1' % (dest_file, dest_file))
         tmp_dest_file_name = dest_file + ".tmp"         
      else:
         dest_dir = dest_file

         if os.path.isdir(dest_file):
            retcode, msg = _exec_cmd('edb_pdsh -a cp -f %s/%s %s/%s.1' % (dest_dir, local_file_name, dest_dir, local_file_name))
            tmp_dest_file_name = dest_dir + "/" + local_file_name + ".tmp"         
            dest_file_name = dest_dir + "/" + local_file_name
         else:
            if dest_file.startswith("$"):
               dest_file = '\\\\\\' + dest_file
               retcode, msg = _exec_cmd('edb_pdsh -a cp -f %s %s.1' % (dest_file, dest_file))
               tmp_dest_file_name = dest_file + ".tmp"         
               dest_file_name = dest_file
                

      retcode, msg = _exec_cmd('edb_pdsh -a hdfs dfs -copyToLocal %s %s' % (hdfs_file, tmp_dest_file_name))
      if retcode ==0:
         retcode, msg = _exec_cmd('edb_pdsh -a cp -f %s %s' % (tmp_dest_file_name, dest_file_name))
         retcode, msg = _exec_cmd('edb_pdsh -a rm -f %s' % (tmp_dest_file_name))
      else:
         logger.error('Failed to copy %s from hdfs sync directory to local : %s' % (local_file_name, msg))
         return
   else:
      logger.info('Non Agent mode. Using pdcp.')
      retcode, nodes = _exec_cmd('trafconf -wname')
      if retcode !=0:
         logger.error('Failed to get node list: %s' % nodes)
	 return

      if os.path.isfile(dest_file):
         retcode, msg = _exec_cmd('edb_pdsh -a rm -f %s.1' % (dest_file))
         retcode, msg = _exec_cmd('edb_pdsh -a cp -p %s %s.1' % (dest_file, dest_file))
      else:
         retcode, msg = _exec_cmd('edb_pdsh -a rm -f %s.1' % (src_file))
         retcode, msg = _exec_cmd('edb_pdsh -a cp -p %s %s.1' % (src_file, src_file))

      retcode, msg = _exec_cmd('pdcp -p %s %s %s' % (nodes, src_file, dest_file))
      if retcode != 0:
         logger.error('Failed to copy %s to %s : %s' % (src_file, dest_file, msg))
         return
   if os.path.isfile(dest_file):  
      logger.info('%s copied to all nodes' % dest_file)
   else:
      logger.info('%s copied to %s directory on all nodes' % (local_file_name, dest_file))

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("src_file", nargs='?', help="The source file name to copy to all nodes", default = '')
    parser.add_argument("dest_file", nargs='?', help="The destination file name", default='')
    args = parser.parse_args()

    def err(msg):
        sys.stderr.write('[ERROR] ' + msg + '\n')
        sys.exit(1)
    
    ### validate parameters ###
    if not args.src_file:

        err('Must specify source filename')
    
    try:
        if not args.dest_file:
            args.dest_file = args.src_file

        _copy_to_all_nodes(args.src_file, args.dest_file)

    except Exception as e:
        logger.error(str(e))
        err(str(e))
            
if __name__ == '__main__':
    main()
            
