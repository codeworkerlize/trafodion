#!/bin/bash

chost="$1"

echo "====================="
echo "control script: $0"
echo "ESGYN_PARCEL: $ESGYN_PARCEL"
echo "Working directory: $PWD"


########################################################
if ! grep -q "^${chost}:" $CONF_DIR/dcs_servers${TRAF_INSTANCE_ID}.properties
then
  echo `date`" - Error: Connectivity Server (TRAF_DCS) must be on a EsgynDB Node (TRAF_NODE)"
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

if [[ -n "$DCS_AWS_ID" && ! -e /usr/bin/aws ]]
then
  echo "Error: Missing dependency: /usr/bin/aws"
  echo "       In order to use Connectivity High-Availability on AWS,"
  echo "       AWS command-line interface must be installed."
  echo "       See: https://docs.aws.amazon.com/cli/latest/userguide/awscli-install-linux.html"
  echo "  To continue without Connectivity HA on AWS, remove dcs.aws.access.id configuration value."
  exit 1
fi

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
   echo `date`" - Aborting startup! Monitor is not up"
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

rm -f $TRAF_CONF/dcs/masters
touch $TRAF_CONF/dcs/masters
instance=0
dcsnodes="$(cut -d: -f1 $CONF_DIR/dcs_master${TRAF_INSTANCE_ID}.properties | sort -u)"

for node in $dcsnodes
do
  echo "$node" >> $TRAF_CONF/dcs/masters
  (( instance += 1 ))
  if [[ $chost == $node ]]
  then
    my_instance=$instance
    if [[ $instance == 1 ]]; then
       STATE="MASTER"
    else
       STATE="BACKUP"
    fi
  fi
done

# create sample keepalive config
if [[ $DCS_INTERFACE == "default" ]]
then
  DCS_INTERFACE="$(route | awk '$1=="default" {print $NF;}')"
fi
sed -e "s#{{NET_INTERFACE}}#$DCS_INTERFACE#g" \
    -e "s#{{DCS_PORT}}#$DCS_MASTER_PORT#g" \
    -e "s#{{ROLE}}#$STATE#g" \
    -e "s#{{PRIORITY}}#$(( 101 - my_instance ))#g" \
    -e "s#{{FLOATING_IP}}#$DCS_MASTER_FLOATING_IP#g" \
  $CONF_DIR/keepalived.conf > $TRAF_CONF/keepalived.conf

# create AWS config files
if [[ -n "$DCS_AWS_ID" ]]
then
  mkdir -p $HOME/.aws
  chmod 700 $HOME/.aws
  echo "[default]" > $HOME/.aws/credentials
  set +x # do not expose password in command trace
  echo "aws_access_key_id=$DCS_AWS_ID" >> $HOME/.aws/credentials
  echo "aws_secret_access_key=$DCS_AWS_KEY" >> $HOME/.aws/credentials
  set -x
  echo "[default]" > $HOME/.aws/config
  echo "region=$DCS_AWS_REGION" >> $HOME/.aws/config
  echo "output=text" >> $HOME/.aws/config
  chmod 600 $HOME/.aws/credentials $HOME/.aws/config
fi

########################################################	
# re-start instead of start, just in case	
echo `date`" - Starting REST server..."	
${REST_INSTALL_DIR}/bin/rest-daemon.sh restart rest	

########################################################
echo `date`" - Waiting for Trafodion Foundation Services..."
traf_ready_value=0
while [[ $traf_ready_value == 0 ]];
do
  # 20 minute limit (delay 10 secs, 120 iterations)
  #sqcheck -v -c dtm -d 10 -i 120
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
# stop first in case of stray (non-supervisord) process
echo `date`" - Starting DCS master"
${DCS_INSTALL_DIR}/bin/dcs-daemon.sh stop master $my_instance

exec ${DCS_INSTALL_DIR}/bin/dcs-daemon.sh --foreground start master $my_instance

echo `date`" - Error: exec failed"

exit 1
