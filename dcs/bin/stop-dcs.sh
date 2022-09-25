#!/usr/bin/env bash
#/**
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
# */

# Stop dcs daemons.

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin">/dev/null; pwd`
JPS=$JAVA_HOME/bin/jps

. "$bin"/dcs-config.sh

# start dcs daemons
errCode=$?
if [ $errCode -ne 0 ]
then
  exit $errCode
fi

function usage {

    if [ -z "$MY_CMD" ]; then
        MY_CMD=$0
    fi

cat <<EOF

Usage:

$MY_CMD [ -kill ]
-kill kill DCS processes     It will 'kill -9' all DCS related processes including DCSMaster, DCSServer and MXOSRVR.

EOF

}

CTL_CMD="stop"

while [ $# -gt 0 ];
do
  case $1 in
  -kill)shift
    CTL_CMD="kill"
  ;;
  -h|-help)
  usage
  exit 0
  ;;
  *) "ERROR: Unexpected argument $1"
  echo
  cat <<EOF
Syntax: $0 [-kill]
EOF
  exit 1
  ;;
  esac
  shift
done

"$bin"/dcs-daemons.sh --config "${DCS_CONF_DIR}" --hosts "${DCS_MASTERS}" $CTL_CMD master-backup

master=`$bin/dcs --config "${DCS_CONF_DIR}" org.trafodion.dcs.zookeeper.ZkUtil /$USER/dcs/master|tail -n 1`
errCode=$?
zkerror=`echo $master| grep -i error`
if ( [ ${errCode} -ne 0 ] || [ -n "${zkerror}" ] );
then
  echo "Zookeeper exception occurred, killing all DcsMaster and DcsServers..."
  "$bin"/dcs-daemon.sh --config "${DCS_CONF_DIR}" $CTL_CMD master
  "$bin"/dcs-daemons.sh --config "${DCS_CONF_DIR}" --hosts "${DCS_SERVERS}" $CTL_CMD server 
  exit $errCode
fi

    activeMaster=$($DCS_INSTALL_DIR/bin/getActiveMaster.sh)

    remote_cmd="cd ${DCS_HOME}; $bin/dcs-daemon.sh --config ${DCS_CONF_DIR} $CTL_CMD master"
    if [[ ! -z $activeMaster ]]; then
        edb_pdsh -w "$activeMaster" "$remote_cmd" 2>&1 | sed "s/^/$activeMaster: /"
    else
        edb_pdsh -w "$master" "$remote_cmd" 2>&1 | sed "s/^/$master: /"
    fi

"$bin"/dcs-daemons.sh --config "${DCS_CONF_DIR}" --hosts "${DCS_SERVERS}" $CTL_CMD mds
"$bin"/dcs-daemons.sh --config "${DCS_CONF_DIR}" --hosts "${DCS_SERVERS}" $CTL_CMD server 
"$bin"/dcs-daemons.sh --config "${DCS_CONF_DIR}" stop zookeeper

zkhome=$("$bin"/dcs --config "${DCS_CONF_DIR}" org.trafodion.dcs.util.DcsConfTool  zookeeper.znode.parent)
if [ "" != "$zkhome" -a "kill" == "$CTL_CMD" ]; then
   (echo "rmr $zkhome/dcs/leader"
    echo "rmr $zkhome/dcs/servers"
    echo "rmr $zkhome/dcs/master"
   ) | "$bin"/dcs zkcli
fi
