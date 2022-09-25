#!/usr/bin/python2.6
#
# @@@ START COPYRIGHT @@@
#
# (C) Copyright 2015-2016 Esgyn Corporation
#
# @@@ END COPYRIGHT @@@

import signal
from threading import Thread
from optparse import OptionParser
from wms_default_script_aux import *
#sys.path.append(os.environ.get('WMS_INSTALL_DIR')+"/bin/scripts")

full_path,script_name = os.path.split(os.path.abspath(__file__))
create_client_logger(pid, script_name)

def signal_term_handler(signal, frame):
    sys.exit(0)
 
signal.signal(signal.SIGTERM, signal_term_handler)

#process STARTING queries (ADMISSION)
def process_starting(kr, zk, sla_name, logger):
  print  >>sys.stderr, '%d %s starting queue len: %s' % (pid, script_name, starting.qsize() )
  while not abort():
      while not starting.empty():
          query = starting.get()
          if query in queries:
            znode = '/' + user + '/' + WMS_RESOURCES_QUERIES + '/' + sla_name + '/' + query
            if sla['limit'] == 0 or systemLimit() < sla['limit']:
              value = queries[query].value
              value = value.replace('STARTING:','RUNNING:')
              kr(zk.set,znode, value)
              queries[query].status = WMS_STATUS_RUNNING
              queries[query].value = value
              running.put(query)
            else:
              kr(zk.set,znode,'REJECTED:Query Limit reached.')
              queries[query].status = WMS_STATUS_REJECTED
              #kr(zk.set,znode,'WAITING:')
              #queries[query].status = WMS_STATUS_WAITING

      logger.debug('%d %s start wait loop' % (pid, script_name))
      while starting.empty() and not abort():
          time.sleep(0.1)
      print  >>sys.stderr, '%d %s exit wait loop' % (pid, script_name)
      logger.debug('%d %s exit wait loop' % (pid, script_name))


#process RUNNING queries
def process_running(sock, sla_name, logger):
  print  >>sys.stderr, '%d %s entry running queue len: %s' % (pid, script_name, running.qsize())
  while not abort():
      lrunning = []
      while not running.empty() and not abort():
          query = running.get()
          if query in queries:

              limit = systemLimit()
              print >>sys.stderr, 'system limit :', limit
              
              [offendingCpu, offendingMem, offendingSql] = offendingQueries(sock)
              print  >>sys.stderr, 'offendingCpu :', offendingCpu
              print  >>sys.stderr, 'offendingMem :', offendingMem
              print  >>sys.stderr, 'offendingSql :', offendingSql

              [mem_values, cpu_values, throughput] = getSystemResources(sla_name)
              print >>sys.stderr, '%d %s mem_values, cpu_values, throughput' % (pid, script_name), mem_values, cpu_values, throughput

              qid = query.split(':')[0]
              #status = cancelQuery(sock, qid, 'comment')
              #print  >>sys.stderr, status

              rmsinfo = statisticsQuery(sock, qid)
              #print  >>sys.stderr, rmsinfo

              tsd_result = getLastMetricValuesByTag('proc.meminfo.memfree', 'host=*')
              print >>sys.stderr, tsd_result
              tsd_result = getLastMetricValuesByTag('proc.net.bytes', 'direction=in,host=*')
              print >>sys.stderr, tsd_result
              
              time.sleep(1)

              if query in queries:
                lrunning.append(query)

      print  >>sys.stderr, '%d %s start wait loop' % (pid, script_name)
      logger.debug('%d %s start wait loop' % (pid, script_name))
      while running.empty() and not abort():
          time.sleep(1)
      print  >>sys.stderr, '%d %s exit wait loop' % (pid, script_name)
      logger.debug('%d %s exit wait loop' % (pid, 'process_running'))

      for query in lrunning:
          running.put(query)

#The main entry point
def main(argv):

  with ContextManager(zk_hosts, host, port) as [zk, sock]:
    parser = OptionParser(description='WMS scripts called for queries assigned to specific SLA')
    parser.add_option('-S', '--sla', default='NONE', metavar='SERVICE', help='Service under SLA is running.')
    (options, args) = parser.parse_args(args=argv[1:])
    sla_name = options.sla

    print  >>sys.stderr, '%d %s started for sla %s' % (pid, script_name, sla_name )
    logger = logging.getLogger(__name__  + ':' + sla_name)
    logger.info('running for sla %s' % sla_name)
    
    kr = KazooRetry(max_tries=3, ignore_expire=False)
    resources_queries_sla_znode = '/' + user + '/' + WMS_RESOURCES_QUERIES + '/' + sla_name
    resources_system_znode = '/' + user + '/' + WMS_RESOURCES + '/system'
    sla_znode = '/' + user + '/' + WMS_SLAS + '/' + sla_name

    # Check if resources_queries_sla_znode exists
    if kr(zk.exists,resources_queries_sla_znode):
        zk_watchers(kr, zk, resources_queries_sla_znode, resources_system_znode, sla_znode, full_path, script_name)
        #process STARTING, RUNNING queues

        thread1 = Thread(target = process_starting, args = (kr, zk, sla_name, logger, ))
        thread1.start()
        thread2 = Thread(target = process_running, args = (sock, sla_name, logger, ))
        thread2.start()
        thread1.join()
        thread2.join()
        print ("threads finished...exiting")

    #There is no znode SLA
    else:
        logger.critical("Resource sla zk node doesn't exist :%s" % resources_queries_sla_znode)
        with lock:
            returncode = RTN_NO_NODE

    return getreturncode()


if __name__ == "__main__":
    
    sys.exit(main(sys.argv))


