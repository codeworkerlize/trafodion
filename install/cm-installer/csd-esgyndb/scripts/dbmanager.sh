#!/bin/bash

chost="$1"

echo "====================="
echo "control script: $0"
echo "ESGYN_PARCEL: $ESGYN_PARCEL"
echo "Working directory: $PWD"


########################################################
if ! grep -q "^${chost}:" $CONF_DIR/dcs_servers${TRAF_INSTANCE_ID}.properties
then
  echo "Error: Database Manager (TRAF_DBM) must be on a EsgynDB Node (TRAF_NODE)"
  exit 1
fi

# support for older parcels
if [[ -z $ESGYN_PARCEL ]]
then
  echo "TRAF_HOME: $TRAF_HOME"
else
  # wait for active parcel link
  export TRAF_HOME="$(ls -d ~trafodion)/esgynDB"
  while [[ ! $ESGYN_PARCEL/traf_home -ef $TRAF_HOME ]]
  do
    sleep 5
  done
fi

# set up env
cd $TRAF_HOME
export HADOOP_TYPE=cloudera
source ./sqenv.sh

########################################################
# Source the common script to use acquire_kerberos_tgt
. $COMMON_SCRIPT

if  [[ -n $esgyndb_principal ]]
then
  # acquire_kerberos_tgt expects that the principal to be kinited is referred to by 
  # SCM_KERBEROS_PRINCIPAL environment variable
  export SCM_KERBEROS_PRINCIPAL=$esgyndb_principal

  # acquire_kerberos_tgt expects that the argument passed to it refers to the keytab file
  acquire_kerberos_tgt esgyndb.keytab

  if [[ -n $KRB5CCNAME ]]
  then
     cp $KRB5CCNAME /tmp/krb5cc_$(id -u)
  fi
fi

########################################################
echo `date`" - Waiting for the Trafodion monitor process..."

# 5 seconds, iterations 240 = 20 minutes
let loop_count=0
let loop_max=240
let monitor_ready=0
while [[ $loop_count -lt $loop_max ]];
do
  grep_out=`pstat | grep "monitor COLD"`
  if [[ $? == '0' ]];then
    let ++monitor_ready
    break
  else
    sleep 5
    let ++loop_count
  fi
done

if [[ $monitor_ready -lt 1 ]]; then
   echo `date`" - Aborting startup! Monitor is not up."
   exit 1
else
   if [[ $loop_count -gt 0 ]]; then
       echo `date`" - Delaying 30 seconds before continuing with Startup"
       sleep 30
   fi 
   echo `date`" - Continuing with Startup ..."
   echo
fi

# if monitor is up, then our config file should be available
source /etc/trafodion/trafodion_config


########################################################
# DBmgr config
TIME_ZONE="$($TRAF_HOME/tools/gettimezone.sh | head -1)"
if [[ -z $TIME_ZONE ]]
then
   TIME_ZONE=UTC
fi

if [[ -n $DCS_MASTER_FLOATING_IP ]]
then
  dcs_master_node="${DCS_MASTER_FLOATING_IP}"
  dcsport=$(cut -d= -f2 $CONF_DIR/dcs_master${TRAF_INSTANCE_ID}.properties | sort -u | head -1)
  dcs_quorum="${DCS_MASTER_FLOATING_IP}:$dcsport"
  bosun_quorum="${DCS_MASTER_FLOATING_IP}:$DBMGR_BOSUN_PORT"
else
  dcs_master_node=$(cut -d: -f1 $CONF_DIR/dcs_master${TRAF_INSTANCE_ID}.properties | sort -u | head -1)
  dcs_list="$(sed 's/dcs.master.port=//' $CONF_DIR/dcs_master${TRAF_INSTANCE_ID}.properties)"
  dcs_quorum=""
  bosun_quorum=""
  for dnode in $dcs_list
  do
    if [[ -z "$dcs_quorum" ]]
    then
      dcs_quorum="$dnode"
    else
      dcs_quorum+=",$dnode"
    fi

    bnode=`echo $dnode | cut -d . -f1`
    if [[ -z "$bosun_quorum" ]]
    then
      bosun_quorum="$bnode:$DBMGR_BOSUN_PORT"
    else
      bosun_quorum+=",$bnode:$DBMGR_BOSUN_PORT"
    fi
  done
fi

echo "Creating/Updating DB Manager configuration file..."
mkdir -p $TRAF_CONF/dbmgr
set +x # do not expose password in command trace

rest_port=$REST_PORT
if [[ $ENABLE_HTTPS == "true" ]]
then
   if [ -z "$KEYSTORE_PASS" ];
   then
    echo `date`" - Error: Keystore password is required when HTTPS is enabled"
    exit 1
   fi
   #Configure DB Manager with https and keystore
   $DBMGR_INSTALL_DIR/bin/configure.py --httpport "$DBMGR_HTTP_PORT" --httpsport "$DBMGR_HTTPS_PORT" \
     --dcsquorum "$dcs_quorum" --enableHTTPS --password '$KEYSTORE_PASS' --keyfile "$HOME/sqcert/server.keystore" \
     --dcsinfoport "$DCS_INFO_PORT" --resthost "$chost" --restport "$REST_HTTPS_PORT" \
     --tsdhost "$chost" --tsdport "$DBMGR_TSD_PORT" --bosunquorum "$bosun_quorum" \
     --timezone "$TIME_ZONE" --adminuser "$DB_ADMIN_USER" --adminpassword '$DB_ADMIN_PASSWORD'
else
   $DBMGR_INSTALL_DIR/bin/configure.py --httpport "$DBMGR_HTTP_PORT" --httpsport "$DBMGR_HTTPS_PORT" \
     --dcsquorum "$dcs_quorum"  \
     --dcsinfoport "$DCS_INFO_PORT" --resthost "$chost" --restport "$REST_PORT" \
     --tsdhost "$chost" --tsdport "$DBMGR_TSD_PORT" --bosunquorum "$bosun_quorum"  \
     --timezone "$TIME_ZONE" --adminuser "$DB_ADMIN_USER" --adminpassword '$DB_ADMIN_PASSWORD'
fi

cp -f $CONF_DIR/remote_instances.json $TRAF_CONF/dbmgr/remote_instances.json

unset DB_ADMIN_PASSWORD
unset KEYSTORE_PASS
set -x

########################################################
echo `date`" - Waiting for Trafodion Foundation Services..."
traf_ready_value=0
while [[ $traf_ready_value == 0 ]];
do
  # 20 minute limit (delay 10 secs, 120 iterations)
  sqregck -r TRAF_FOUNDATION_READY -d 10 -i 120
  traf_ready_value=$?
done
if [[ $traf_ready_value == 1 ]]
then
  echo `date`" - Trafodion Foundation Services are Ready."
else
  echo `date`" - Trafodion Foundation Services are not Ready!"
  exit 1
fi

########################################################
echo `date`" - Waiting for Connection Servers..."
while (( $(sqps | grep -c -e ' mxosrvr') < 2 ))
do
  sleep 11
done

########################################################
# Read the latest HBASE_CLASSPATH after sqgen which removes the hbase_classpath cache file
source ~trafodion/.bashrc
source $TRAF_HOME/conf/trafodion_config

echo `date`" - Starting Bosun"
$MGBLTY_INSTALL_DIR/bosun/bin/runbosun.sh restart noecho

echo `date`" - Starting DB manager"
exec ${DBMGR_INSTALL_DIR}/bin/dbmgr.sh exec

echo `date`" - Error: exec failed"

exit 1
