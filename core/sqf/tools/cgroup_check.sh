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
Description: $0 is used to assert 1 cgroup of all of processes started by trafodion user and monitor the cgroup (will generate monitoring process in pid.log file, you can manually kill it) to get usage of cpu, memory, cpuacct and blkio then judge if the cgroup is working (limit_usage ? actual_usage on memory) by the regular report, meanwhile, check if any core file is generated in $TRAF_HOME and $(cat /proc/sys/kernel/core_pattern|xargs dirname).\
\nUsage: ./$0 [-h *|[-t 1800 [-p 10]]]\
\nparameters:\
\n  $0: means current script name\
\n  -h: option 1\
\n  *: any char(s) of opiton 1, it will onpy print the usage of this script then exit.\
\n  -t: option 2\
\n  1800(default): value of option 2, means the total time(seconds) of starting monitoring of cgroup, meanwhile, only allow 1 cgroup of all of processes started by trafodion user.\
\n  -p: option 3\
\n  10(default): value of option 3, means to check cgroup per 10 seconds.\
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
  export tmp_f=/tmp/cgroup.log
  if test ! -e $tmp_f; then
    touch $tmp_f
  fi && :>$tmp_f
  export input_f=input.log
  if test ! -e $input_f; then
    touch $input_f
  fi && :>$input_f
}

var_set

logger()
{
  echo "$(date): $1" | tee -a $scrpt_f
}

check_cg() 
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
  if test 1 -ne $(cat $tmp_f|uniq|wc -l); then
    logger "ERROR: it only allows 1 cgroup for all of processes started by $USER user, will exit."
    exit 1
  else
    logger "DEBUG: get cgroup of all of processes started by $USER user: $(cat $tmp_f|uniq)"
  fi
}

monitor_cg()
{
  check_cg
  cg=$(cat $tmp_f|uniq)
  (
    start_time=$(date "+%Y-%m-%d %H:%M:%S")
    start_timestamp=$(expr $(date +%s -d "${start_time}"))
    while :; do
      #cgget -g cpu $cg # get info of cpu
      #cgget -g memory $cg # get info of memory
      #cgget -g cpuacct $cg # get info of cpuacct
      #cgget -g blkio $cg # get info of blkio
      date
      cgget -a $cg
      end_time=$(date "+%Y-%m-%d %H:%M:%S")
      end_timestamp=$(expr $(date +%s -d "${end_time}"))
      if test $(expr $end_timestamp - $start_timestamp) -ge $TOTAL_TIME; then
        #logger "INFO: it's time up, stop monitoring of cgroup, will do analysis for the log file \"$input_f\""
        break
      fi
      sleep $TIME_SPACE
    done 1>>$input_f 2>&1
  ) &
  echo $! 1>pid.log 2>&1
  logger "INFO: started monitoring of cgroup, pid.log is $(cat pid.log), you can kill it manually"
}

do_analysis()
{
  key_date=$(date|awk '{print $5}')
  suffix="TRAF_NODE"
  awk -F: '
  BEGIN {
    key_limit_memory="memory.limit_in_bytes"
    key_usage_memory="memory.usage_in_bytes"
    key_failcnt_memory="memory.failcnt"
    key_limit_memsw="memory.memsw.limit_in_bytes"
    key_usage_memsw="memory.memsw.usage_in_bytes"
    key_failcnt_memsw="memory.memsw.failcnt"
    value_limit_memory=0
    value_usage_memory=0
    value_failcnt_memory=0
    value_limit_memsw=0
    value_usage_memsw=0
    value_failcnt_memsw=0
    current_date=""
    exceed_memory=0
    exceed_memsw=0
    path=""
    assert_memory_usage="PASS"
    assert_memsw_usage="PASS"
    printf(">>> start to analysis the log file (%s)\n", "'$input_f'")
    printf("... column  1: date (execute cmd \"cgget -a path\")\n")
    printf("... column  2: path\n")
    printf("... column  3: memory.limit_in_bytes\n")
    printf("... column  4: memory.usage_in_bytes\n")
    printf("... column  5: memory.failcnt\n")
    printf("... column  6: exceed_memory (column 3 < column 4 and return 1, or return 0)\n")
    printf("... column  7: memory.memsw.limit_in_byes\n")
    printf("... column  8: memory.memsw.usage_in_byes\n")
    printf("... column  9: memory.memsw.failcnt\n")
    printf("... column 10: exceed_memsw (column 7 < column 8 and return 1, or return 0)\n")
    printf("... pls: failcnt - it is an indicator that memory requested has exceeded the limit, and that request was denied, it has probability to generate core(s), please manually check it\n")
  }
  {
    if ($0 ~ "'$key_date'") 
    {
      current_date=$0
    }
    if ($0 ~ "'$suffix'") 
    {
      path=$1
    }
    if ($0 ~ key_limit_memory)
    {
      value_limit_memory=$2   
    }
    if ($0 ~ key_usage_memory)
    {
      value_usage_memory=$2
    }
    if (value_limit_memory < value_usage_memory && value_limit_memory != 0 && value_usage_memory != 0)
    {
      exceed_memory=1
      assert_memory_usage="FAILED"
    } else {
      exceed_memory=0
    }
    if ($0 ~ key_failcnt_memory) 
    {
      value_failcnt_memory=$2
    }
    if ($0 ~ key_limit_memsw)
    {
      value_limit_memsw=$2
    }
    if ($0 ~ key_usage_memsw)
    {
      value_usage_memsw=$2
    }
    if (value_limit_memsw < value_usage_memsw && value_limit_memsw != 0 && value_usage_memsw != 0)
    {
      exceed_memsw=1
      assert_memsw_usage="FAILED"
    } else {
      exceed_memsw=0
    }
    if ($0 ~ key_failcnt_memsw) 
    {
      value_failcnt_memsw=$2
    }
    if (current_date != "" && value_limit_memory != 0 && value_usage_memory != 0 && value_limit_memsw != 0 && value_usage_memsw != 0)
    { 
      printf("%s,%s,%d,%d,%d,%d,%d,%d,%d,%d\n", current_date,path,value_limit_memory,value_usage_memory,value_failcnt_memory,exceed_memory,
                                                                  value_limit_memsw,value_usage_memsw,value_failcnt_memsw,exceed_memsw)
      current_date=""
      value_limit_memory=0
      value_usage_memory=0
      value_limit_memsw=0
      value_usage_memsw=0
      value_failcnt_memory=0
      value_failcnt_memsw=0
      exceed_memory=0
      exceed_memsw=0
      path=""
    }
  }
  END {
    printf(">>> make analysis done.\n");
    printf("assert(memory.limit_in_bytes >= memory.usage_in_bytes): [%s]\n", assert_memory_usage)
    printf("assert(memory.memsw.limit_in_bytes >= memory.memsw_usage_in_bytes): [%s]\n", assert_memsw_usage)
  }
  ' $input_f 2>&1 | tee -a $scrpt_f
  #the following codes is used to find core(s) in system.
  logger "INFO: >>> find core(s), please note the created time of each core, commonly we do not allow any core generated because of out of memory (and swap) limit by settings of cgroup for EsgynDB"
  logger "INFO: 1) search directory $TRAF_HOME"
  if test 0 -lt $(find $TRAF_HOME -name "core.*"|wc -l); then
    find $TRAF_HOME -name "core.*"|xargs ls -alth 2>&1 | tee -a $scrpt_f
  fi
  logger "INFO: done."
  pattern=$(cat /proc/sys/kernel/core_pattern)
  if test -z $pattern; then
    pattern="not specified"
  else
    pattern=$(dirname $pattern)
  fi
  logger "INFO: 2) search user_path ($pattern)"
  if test -e $pattern; then
    ls -alth $pattern 2>&1 | tee -a $scrpt_f
  fi
  logger "INFO: done."
}

main()
{
  monitor_cg
  wait $(cat pid.log) 2>/dev/null
  do_analysis
}

main

#!END
