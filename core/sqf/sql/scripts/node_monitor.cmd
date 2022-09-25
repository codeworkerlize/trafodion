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
#
# Add code to monitor processes/services at a node level
#
# The stdout is the file $TRAF_HOME/sql/scripts/stdout_nmon
#
#---- Begin: Setup the env to run any script - please do not edit this block
if [[ -e /etc/trafodion/trafodion_config ]]; then
        source /etc/trafodion/trafodion_config
fi

cd $TRAF_HOME
. $TRAF_HOME/sqenv.sh
cd - >/dev/null

# Setting this variable so that downstream scripts executed here are aware of the context
export NODE_MONITOR_MODE=1

# Are we also running the CMON (cluster monitor) on this node
lv_stat=`pstat | grep service_monitor | grep '\$CMON '`
if [[ $? == 0 ]]; then
    export CMON_RUNNING=1
else
    ### This check is now WRONG --- COLD_AGENT is either on every node or no nodes
    ### It should be checking lead monitor znode????
    lv_stat=`pstat | grep COLD_AGENT`
    if [[ $? == 0 ]]; then
       export CMON_RUNNING=1
    else
       export CMON_RUNNING=0
    fi
fi

#----  End : Setup the env to run any script - please do not edit this block

if [[ $SECURE_HADOOP == "Y" ]]; then
  $TRAF_HOME/sql/scripts/krb5service watch
fi

if [[ -z ${TRAF_AGENT} || $TRAF_AGENT == "DBMGR" ]]; then
   if [[ ! -z $CLUSTERNAME ]]; then
      $DCS_INSTALL_DIR/bin/dcs-daemon.sh watch master
   fi
   $DBMGR_INSTALL_DIR/bin/dbmgr.sh watch 
fi

if [[ ! -z $CLUSTERNAME ]]; then
   #Start Rest Servers
   $REST_INSTALL_DIR/bin/rest-daemon.sh watch rest
fi

# generate HBase RS' zk nodes file
#get_hbase_rs

# Uncomment to start the SNMP Trap Receiver  
#sudo -E $TRAF_HOME/sql/scripts/snmp_trap_watch

#watch mds
/usr/bin/python <<-EOF
import os
from xml.dom import minidom;
import sys

dcsconfig_dir = os.environ.get('DCS_CONF_DIR')
if not dcsconfig_dir:
    name = os.environ.get('TRAF_CONF')
    dcsconfig_dir=name+"/dcs"
doc = minidom.parse(dcsconfig_dir+"/dcs-site.xml")
props = doc.getElementsByTagName("property")
for prop in props:
    tagName = prop.getElementsByTagName ("name")[0]
    pname=tagName.childNodes[0].data
    tagValue = prop.getElementsByTagName("value")[0]
    pvalue=tagValue.childNodes[0].data
    if pname == "dcs.server.user.program.mds.enabled" and pvalue.lower() == "true":sys.exit(1)
EOF

if [ $? == "1" ]; then
    echo "mds configuration is opened."
    mdsPid=`ps -u $USER -o pid,cmd | grep "mds" | grep -v grep |awk '{print $1}'`
    if [[ ! -n "$mdsPid" ]]; then
    $DCS_INSTALL_DIR/bin/dcs-daemon.sh --config "${DCS_CONF_DIR}" start mds
    fi
fi
