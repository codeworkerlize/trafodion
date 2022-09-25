#!/bin/bash
# @@@ START COPYRIGHT @@@
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# @@@ END COPYRIGHT @@@



help="\
Description: $0 is used to detect if multi cgroup(s) exist any time.\
\nUsage: ./$0 [-h *|[-t 1800 [-p 10]]]\
\nparameters:\
\n  $0: means current script name\
\n  -h: option 1\
\n  *: any char(s) of opiton 1, it will only print the usage of this script then exit.\
\n  -t: option 2\
\n  1800(default): value of option 2, means the total time(seconds) of detecting of multi cgroup(s).\
\n  -p: option 3\
\n  10(default): value of option 3, means to detect if multi cgroup(s) exist per 10 seconds.\
"

check_user()
{
  if [[ "$USER" != "trafodion" ]]; then
    echo -e "$(date): ERROR: it requires trafodion user to run, will exit."
    exit 1
  fi
}

check_user

export TOTAL_TIME=1800 # default 1800 seconds
export TIME_SPACE=10 # default 10 seconds
export scrpt_f=$0.log && :>$scrpt_f # logs

while getopts h:t:p: option
do
  case $option in
  h)
    echo -e "$help"
    exit 0;;
  t) 
    if [[ $OPTARG =~ [0-9] ]]; then
      if test $OPTARG -gt 0; then
        TOTAL_TIME=$OPTARG
      fi
    fi
    continue;;
  p) 
    if [[ $OPTARG =~ [0-9] ]]; then
      if test $OPTARG -gt 0; then
        TIME_SPACE=$OPTARG
      fi
    fi
    continue;;
  *) 
    ;;
  esac
  shift `let $OPTIND-1`
done

var_set()
{
  export tmp_f=/tmp/multi_cgroups.log
  if test ! -e $tmp_f; then
    touch $tmp_f
  fi && :>$tmp_f
  export err_f=err.log
  if test ! -e $err_f; then
    touch $err_f
  fi && :>$err_f
}

var_set

logger()
{
  echo -e "$(date): $1" | tee -a $scrpt_f
}

get_cgroups() 
{
  nid=$(shell -c node info|grep $(hostname)|grep "\["|awk '{print $2}')
  ap=`sqps|grep $nid,|awk '{print $2}'|cut -d, -f2`
  for p in ${ap[@]}; do
    tmp=$p
    for ((i=0; i <${#p}; i++)); do
      tmp=${tmp#0}
    done
    if test -e /proc/$tmp/cgroup; then
      cat /proc/$tmp/cgroup|cut -d'/' -f2 1>$tmp_f 2>/dev/null
    fi
  done
}

monitor_multi_cgroups()
{
  get_cgroups
  m=$(date)
  a=$(cat $tmp_f|uniq)
  if test ${#a[@]} -ne 1; then
    echo -e "$(date): ERROR: find multi cgroup(s) at first." 1>>$err_f 2>&1
    for i in ${a[@]}; do
      echo -e ": $i" 1>>$err_f 2>&1
    done
  else
    logger "DEBUG: $a"
  fi
  (
    start_time=$(date "+%Y-%m-%d %H:%M:%S")
    start_timestamp=$(expr $(date +%s -d "${start_time}"))
    while :; do
      sleep $TIME_SPACE
      n=$(date)
      get_cgroups
      b=$(cat $tmp_f|uniq)  
      if test ${#b[@]} -ne 1; then
        echo -e "$(date): ERROR: find multi cgroup(s) now." 1>>$err_f 2>&1
        for i in ${b[@]}; do
          echo -e ": $i" 1>>$err_f 2>&1
        done
        exit 1
      fi
      if [[ $a != $b ]]; then
        echo -e "$(date): ERROR: cgroup has been changed now. [$m: $a] != [$n: $b]" 1>>$err_f 2>&1
      fi
      echo -n "."
      end_time=$(date "+%Y-%m-%d %H:%M:%S")
      end_timestamp=$(expr $(date +%s -d "${end_time}"))
      if test $(expr $end_timestamp - $start_timestamp) -ge $TOTAL_TIME; then
        echo -e "\n$(date): INFO: it's time up, stop monitoring of multi cgroup(s), please check if the log file \"$err_f\" is empty, if it's size is zero, which means no multi cgroup(s) during this period."
        break
      fi
    done
  ) &
  echo $! 1>mpid.log 2>&1
  logger "INFO: started monitoring of multi cgroup(s), mpid.log is $(cat mpid.log), you can kill it manually"
}

main()
{
  monitor_multi_cgroups
  wait $(cat mpid.log) 2>/dev/null
  logger "\nINFO: >>> make assertion."
  if test 0 -lt $(cat $err_f|wc -l); then
    logger "INFO: find multi cgroup(s), that's not our expected. [FAILURE]"
  else
    logger "INFO: congratulations, only find 1 cgroup. [PASS]"
  fi
}

main

#!END
