#!/bin/bash

if [[ -z $ESGYN_PARCEL ]]
then
  echo "Using TRAF_HOME: $TRAF_HOME"
else
  export TRAF_HOME="$ESGYN_PARCEL/traf_home"
fi

cd $TRAF_HOME
export HADOOP_TYPE=cloudera
source ./sqenv.sh

cd $CONF_DIR

echo "# TRAFCI" > traf-client/client_config
echo "export TRAF_HOME=$TRAF_HOME" >> traf-client/client_config
echo "export JAVA_HOME=$JAVA_HOME" >> traf-client/client_config
echo "export TRAFODION_ENABLE_AUTHENTICATION=$TRAFODION_ENABLE_AUTHENTICATION" >> traf-client/client_config
echo "export DCS_MASTER_PORT=$DCS_MASTER_PORT" >> traf-client/client_config
if [[ $DCS_MASTER_HA == "true" ]]
then
  echo "export DCS_MASTER_FLOATING_IP=$DCS_MASTER_FLOATING_IP" >> traf-client/client_config
fi

if [[ $DCS_MASTER_HA == "true" ]]
then
  DCS_MASTER_QUORUM="${DCS_MASTER_FLOATING_IP}:$DCS_MASTER_PORT"
else
  dcs_list="$(sed 's/dcs.master.port=//' dcs_master${TRAF_INSTANCE_ID}.properties)"
  DCS_MASTER_QUORUM=""
  for dnode in $dcs_list
  do
    if [[ -z "$DCS_MASTER_QUORUM" ]]
    then
      DCS_MASTER_QUORUM="$dnode"
    else
      DCS_MASTER_QUORUM+=",$dnode"
    fi
  done
fi

echo "export DCS_MASTER_QUORUM=$DCS_MASTER_QUORUM" >> traf-client/client_config

dcs_masters_nodelist="$(sed 's/:.*//' dcs_master${TRAF_INSTANCE_ID}.properties)"
DCS_MASTER_NODES=""
for dnode in $dcs_masters_nodelist
do
  if [[ -z "$DCS_MASTER_NODES" ]]
  then
    DCS_MASTER_NODES="$dnode"
  else
    DCS_MASTER_NODES+=",$dnode"
  fi
done

echo "export DCS_MASTER_NODES=$DCS_MASTER_NODES" >> traf-client/client_config
echo "PATH=\${PATH}:$TRAF_HOME/trafci/bin" >> traf-client/client_config

echo "# ODBC" >> traf-client/client_config
echo "export ODBCHOME=/etc/trafodion/conf" >> traf-client/client_config
echo "export ODBCSYSINI=/etc/trafodion/conf" >> traf-client/client_config
echo "export ODBCINI=/etc/trafodion/conf/odbc.ini" >> traf-client/client_config
echo "export AppUnicodeType=utf16" >> traf-client/client_config
echo "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$TRAF_HOME/export/lib$SQ_MBTYPE" >> traf-client/client_config
sed -i -e "s#{{LNXDRVR}}#$TRAF_HOME/export/lib$SQ_MBTYPE#" traf-client/odbcinst.ini

sed -i -e "s#TCP:.*#TCP:$DCS_MASTER_QUORUM#" traf-client/odbc.ini

exit 0
