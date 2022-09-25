#!/bin/bash
# @@@ START COPYRIGHT @@@
#
# (C) Copyright 2019 Esgyn Corporation
#
# @@@ END COPYRIGHT @@@

CONF_FILE=/etc/prometheus/prometheus.yml
PID_INFO=`ps -aux | grep /usr/bin/prometheus | grep -v grep`
OPTS=`cat /etc/prometheus/options`

function isRunning() {
  if [ -n "$PID_INFO" ]; then
    return 0
  else
    return 1
  fi
}

function start() {
  mkdir -p /var/log/prometheus
  nohup /usr/bin/prometheus $OPTS > /var/log/prometheus/prometheus.log 2>&1 &
  # Wait for the process to be properly started before exiting
  sleep 3
  PID_INFO=`ps -aux | grep /usr/bin/prometheus | grep -v grep`
  if [ -n "$PID_INFO" ];then
    return 0
  else
    return 1
  fi
}

function stop() {
  PID_NUM=$(echo "$PID_INFO" | awk '{print $2}')
  kill -9 $PID_NUM > /dev/null 2>&1
  if [ "$?" -ne 0 ];then
    echo -ne "[FAILED]\n"
    exit 1
  fi
}

# main
case "$1" in
  start)
    isRunning
    if [ "$?" -eq 0 ]; then
      echo "Prometheus already running."
      exit 0
    fi

    # start prometheus
    echo -n "Starting prometheus ..."
    start
    if [ "$?" -eq 0 ]; then
      echo -ne "[SUCCESS]\n"
      exit 0
    else
      echo -ne "[FAILED]\n"
      exit 1
    fi
    ;;
  stop)
    isRunning
    if [ "$?" -eq 1 ]; then
      echo "Prometheus is not running."
      exit 0
    fi

    # stop prometheus
    echo -n "Stopping prometheus ..."
    stop
    echo -ne "[SUCCESSED]\n"
    exit 0
    ;;
  *)
    echo "Usage: $0 {start|stop}"
    exit 1
    ;;
esac
