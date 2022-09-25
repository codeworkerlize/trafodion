#!/bin/bash

echo "====================="
echo "control script: $0"
echo "TRAF_HOME: $TRAF_HOME"
echo "Working directory: $PWD"
echo ""

chost="$1" # current host
short_host=${chost%%.*}
export HOSTNAME=$chost

# set up env
cd $TRAF_HOME
source /etc/trafodion/trafodion_config
source ./sqenv.sh

cd $TRAF_HOME/sql/scripts

ldap="$TRAFODION_ENABLE_AUTHENTICATION"
root="$DB_ROOT_USER"
admin="$DB_ADMIN_USER"
instance=$( grep -n $chost $TRAF_CONF/dcs/servers | cut -d: -f1)

########################################################
echo "====================="
echo "Stopping any stale DCS server"
echo ""
${DCS_INSTALL_DIR}/bin/dcs-daemon.sh stop server $instance


########################################################
echo "====================="
echo -n `date`" - Waiting for the Trafodion monitor process..."

while [[ ! -d $TRAF_LOG ]]
do
  sleep 5
done

let monitor_ready=0

# 5 seconds, iterations 240 = 20 minutes
if sqcheckmon -s up -i 240 -d 5
then
   let ++monitor_ready
fi

if [[ $monitor_ready -lt 1 ]]; then
   echo `date`" - Aborting startup!"
   cat $TRAF_LOG/sqcheckmon.log
   exit 1
else
   echo `date`" - Continuing with the Startup..."
   echo
fi

########################################################
echo "====================="
echo "$(date) - Starting Connectivity Servers for local node"
count=$( grep -n $chost $TRAF_CONF/dcs/servers | cut -d' ' -f2)
${DCS_INSTALL_DIR}/bin/dcs-daemon.sh start server $instance $count

echo "Starting LittleJetty"
node_littlejettystart

########################################################
echo "====================="
# If we are not the "master" node, then we are done
# Wait just in case monitors have not yet elected a leader
for time in 5 5 10 10 20 20 30 30
do
  master=$(hbase zkcli ls /trafodion/$TRAF_INSTANCE_ID/monitor/master 2>/dev/null | grep ^\\[)
  if [[ -n "$master" && "$master" != "[]" ]]
  then
    break
  fi
  echo "Waiting $time seconds for monitor leader"
  sleep $time
done
if [[ "$master" != "[$short_host]" ]]
then
  exit 0  ## Remainder is for full cluster start
fi
# Otherwise we are single node and proceed with cluster start-up

########################################################
echo `date`" - Running trstart"
trstart
ret=$?
echo "trstart returned: $ret"


########################################################
echo "====================="
if [[ $ret == 0 && $ldap == "YES" ]]
then
  echo "Associating external users (e.g., LDAP) with DB__ROOT and DB__ADMIN"
  sqlci <<eof > $TRAF_LOG/init_auth.out 2>&1
    initialize authorization;
    alter user DB__ROOT set external name "$root";
    alter user DB__ADMIN set external name "$admin";
    exit;
eof
  ret=$?
  if grep -q 1393 $TRAF_LOG/init_auth.out
  then
    echo "###############################################################################"
    echo "# Warning: Meta-Data not initialized."
    echo "#          Run action command: Initialize EsgynDB MetaData"
    echo "###############################################################################"
    exit 0
  fi
fi

if [[ $ret == 0 ]]
then
  echo "###############################################################################"
  echo "#                                                                             #"
  echo "# If not previously completed, you must run action                            #"
  echo "# command: Initialize EsgynDB MetaData                                        #"
  echo "#                                                                             #"
  echo "###############################################################################"
fi

exit $ret
