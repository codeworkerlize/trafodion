#!/usr/bin/python2.6
#
# @@@ START COPYRIGHT @@@
#
# (C) Copyright 2015-2016 Esgyn Corporation
#
# @@@ END COPYRIGHT @@@

import os
import sys
import json
import httplib
import socket

full_path,script_name = os.path.split(os.path.abspath(__file__))

MAX_TCP_BUFFER = 512

def read_tcp_buffer(sock, total_len):
  read_len = 0
  expected_len = total_len
  tcp_buffer = ''
  read_data = ''

  try:
    while expected_len > 0:
        if expected_len < MAX_TCP_BUFFER:
            tcp_buffer = sock.recv(expected_len)
        else:
            tcp_buffer = sock.recv(MAX_TCP_BUFFER)
        read_data = read_data + tcp_buffer
        expected_len -= len(tcp_buffer)
  except:
      read_data = ''
  return read_data
 
def offendingQueries(sock):
  offendingQueries = []
  offendigCpu = []
  offendigTse = []
  offendigSlow = []

  print >>sys.stderr, '%d %s is calling offendingQueries' % (os.getpid(), script_name )

  request = 'offendingQueries'
  request_len = len(request) + 6
  message = '%06d%s' % (request_len, request)

  print >>sys.stderr, '%d %s sending "%s"' % (os.getpid(), script_name, message)
  sock.sendall(message)
  #time.sleep(1.5)
  # Look for the response
  data = read_tcp_buffer(sock, 6)
  amount_received = len(data)
  amount_expected = int(data)
  data_received = ''
  print >>sys.stderr, '%d %s amount_expected %d' % (os.getpid(), script_name, amount_expected)
  data_received = read_tcp_buffer(sock, amount_expected - 6)

  request_echo = data_received[:len(request)]

  if request_echo == request:
    data_received = data_received[len(request):]
    offendingQueries = json.loads(data_received)
    offendingCpu = offendingQueries[0]
    offendingTse = offendingQueries[1]
    offendingSlow = offendingQueries[2]

  return offendingCpu, offendingTse, offendingSlow

  #time.sleep(5)

def cancelQuery(sock, qid, comment):
  print >>sys.stderr, '%d %s is calling cancelQuery qid=%s, comment=%s' % (os.getpid(), script_name, qid, comment )

  response = ''
  request = 'cancel' + ':' + qid + ':' + comment
  request_len = len(request) + 6
  message = '%06d%s' % (request_len, request)

  print >>sys.stderr, '%d %s sending "%s"' % (os.getpid(), script_name, message)
  sock.sendall(message)
  #time.sleep(1.5)
  # Look for the response
  data = read_tcp_buffer(sock, 6)
  amount_received = len(data)
  amount_expected = int(data)
  data_received = ''
  print >>sys.stderr, '%d %s amount_expected %d' % (os.getpid(), script_name, amount_expected)

  data_received = read_tcp_buffer(sock, amount_expected - 6)
  request = 'cancel'
  request_echo = data_received[:len(request)]

  if request_echo == request:
    response = data_received[len(request):]

  return response

  #time.sleep(5)

def statisticsQuery(sock, qid):
  print >>sys.stderr, '%d %s is calling statisticsQuery qid=%s' % (os.getpid(), script_name, qid )

  list_of_dicts = []
  response = {}
  request = 'rms' + ':' + qid
  request_len = len(request) + 6
  message = '%06d%s' % (request_len, request)

  print >>sys.stderr, '%d %s sending "%s"' % (os.getpid(), script_name, message)
  sock.sendall(message)
  #time.sleep(1.5)
  # Look for the response
  data = read_tcp_buffer(sock, 6)
  print >>sys.stderr, '%d %s data got ' % (os.getpid(), script_name), data
  amount_received = len(data)
  amount_expected = int(data)
  data_received = ''
  print >>sys.stderr, '%d %s amount_expected %d' % (os.getpid(), script_name, amount_expected)

  data_received = read_tcp_buffer(sock, amount_expected - 6)
  request = 'rms'
  request_echo = data_received[:len(request)]

  if request_echo == request:
    data_received = data_received[len(request):]
    response = json.loads(data_received)

  list_of_dicts.append(response)
  for item in list_of_dicts:
      for key, value in response.iteritems():
          try:
              item[key] = int(value)
          except ValueError:
              try:
                  item[key] = float(value)
              except ValueError:
                  pass

  print >>sys.stderr, response

  return response

def get_tsd(conn, url):
    jret = ''
    errmsg = 'OK'
    try:
        conn.request('GET', url)
        res = conn.getresponse()
        datapoints = res.read()
    except socket.error, (errno, msg):
        errmsg = "errno=%d:Couldn't GET datapoint-%s" % (errno,msg)
        return [errmsg, jret]
    if res.status not in (200, 202):
        errmsg = 'status=%d' % res.status
        jret = json.loads(datapoints)
    if not len(datapoints):
        errmsg = errmsg + ':Query did not return any data point'
    if errmsg == 'OK':
        metric = ''
        tags_list = []
        dps_list = []
        jret = json.loads(datapoints)
        if jret:
            for node in jret:
                metric = node['metric']
                dps = node['dps']
                value = float(dps[max(dps.keys())])
                dps_list.append(value)
                tags = node['tags']
                tags_list.append(tags)
            jret = {}
            jret['metric'] = metric
            jret['tags'] = tags_list
            jret['dps'] = dps_list
    return [errmsg, jret]

def tsdGetLastMetricValuesByTag(metricName, filters, tsd_host, tsd_port):
  dcret = {}  
  errmsg = ''
  jret = ''
  server = '%s:%d' % (tsd_host, tsd_port)
  timeout = 5
  conn = httplib.HTTPConnection
  url = '/api/query?start=60s-ago&m=avg:30s-avg:%s{%s}'

  print >>sys.stderr, '%d %s metricName=%s, filters=%s, tsd_host=%s, tsd_port=%d' % (os.getpid(), script_name, metricName, filters, tsd_host, tsd_port)
  if metricName == None or metricName == '':
      errmsg = 'error=-1:mtericName must be defined'
      dcret['errmsg'] = errmsg
      dcret['jret'] = ''
  elif filters == None or filters == '':
      errmsg = 'error=-1:filters must be defined'
      dcret['errmsg'] = errmsg
      dcret['jret'] = ''
  else:
      errmsg = 'OK'
      try:
        conn = conn(server, timeout=timeout)
        conn.connect()
        peer = conn.sock.getpeername()
        [errmsg, jret] = get_tsd(conn, url % (metricName, filters) )
      except socket.error, (errno, msg) :
        errmsg = 'errno=%d:%s' % ( errno, msg)
      conn.close()
      dcret['errmsg'] = errmsg
      dcret['jret'] = jret
  return json.dumps(dcret)

  #time.sleep(5)
