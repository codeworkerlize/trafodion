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

coreFileName="core\.\*[0-9][0-9]*"
hsErrFileName="hs_err_pid\*[0-9][0-9]*.log"

corePatternStr=`cat /proc/sys/kernel/core_pattern`

if [[ ${corePatternStr} == \|* ]]; then
  echo "Linux core_pattern configured with bug reporting tool(ABRT)"
  exit 0
elif [[ ${corePatternStr} == /* ]]; then
  strToFind=$(dirname "${corePatternStr}")
else 
  strToFind=${TRAF_HOME}
fi

getCoreFileCnt() {
   if [[ -z ${numdays} ]]; then
     coreFileParam="" 
   else 
     if [[ $numdays != 0 ]]; then
      if [[ $numdays == 1 ]]; then
       coreFileParam="-daystart -mtime ${numdays}" 
      else
       coreFileParam="-daystart -mtime +${numdays}" 
      fi
     else
       coreFileParam="-daystart -mtime -1" 
     fi
   fi
   coreFileCnt="`edb_pdsh -a \"find -L ${strToFind} -type f  $coreFileParam -name ${coreFileName}\" | wc -l`"
   echo " Total core files clusterwide   : ${coreFileCnt} "
}

getHsErrFileCnt() {
   if [[ -z ${numdays} ]]; then
      hsErrFileParam=""
   else
     if [[ $numdays != 0 ]]; then
      if [[ $numdays == 1 ]]; then
         hsErrFileParam="-daystart -mtime ${numdays}"
      else
         hsErrFileParam="-daystart -mtime +${numdays}"
      fi
     else
      hsErrFileParam="-daystart -mtime -1"
     fi
   fi
   hsErrFileCnt="`edb_pdsh -a \"find -L ${TRAF_HOME} -type f $hsErrFileParam -name ${hsErrFileName}\" |wc -l`"
   echo " Total hs_err files clusterwide : ${hsErrFileCnt} 
   "
}

getTotalCnt() {
   echo "
   **** Summary ****
   " 
   getCoreFileCnt
   getHsErrFileCnt
}

getOlderCoreFiles() {
    if [[ $numdays != 0 ]]; then
      if [[ $numdays == 1 ]]; then
        ndays="$numdays" 
      else
        ndays="+$numdays" 
      fi
    else 
      ndays="-1" 
    fi
    edb_pdsh -a "find -L ${strToFind} -type f -daystart -mtime $ndays -name ${coreFileName} -exec ls -l --time-style +'%Y-%m-%d %H:%M:%S' '{}' \; " | awk '{print $1,$7,$8,$9}'  2>/dev/null | sort
}

getOlderHsErrFiles() {
    if [[ $numdays != 0 ]]; then
     if [[ $numdays == 1 ]]; then
        ndays="$numdays" 
     else
        ndays="+$numdays" 
     fi
    else
      ndays="-1" 
    fi
    edb_pdsh -a "find -L ${TRAF_HOME} -type f -daystart -mtime $ndays -name ${hsErrFileName} -exec ls -l --time-style +'%Y-%m-%d %H:%M:%S' '{}' \; " | awk '{print $1,$7,$8,$9}'  2>/dev/null | sort
}

getListOfCoreFiles()
{
   echo "
   **** List of core files ****"
   edb_pdsh -a "find -L ${strToFind} -name ${coreFileName} -exec ls -l --time-style +'%Y-%m-%d %H:%M:%S' '{}' \;"  | awk '{print $1,$7,$8,$9}'  2>/dev/null | sort 
}

getListOfHsErrFiles()
{
   echo "
   **** List of hs_err files ****"
   edb_pdsh -a "find -L ${TRAF_HOME} -name ${hsErrFileName} -exec ls -l --time-style +'%Y-%m-%d %H:%M:%S' '{}' \; "  | awk '{print $1, $7,$8,$9}' 2>/dev/null | sort 
}
