#!/bin/bash

source ~/.bashrc

#########################################
echo `date`" - Creating HDFS directories if necessary..."

function hdfsdir() {
  mode="$1"
  d="$2"
  if ! hdfs dfs -ls -d $d > /dev/null
  then
    echo "  creating $d"
    hdfs dfs -mkdir -p $d
  fi
  if ! hdfs dfs -chmod $mode $d
  then
    echo "Failed to set permissions on $d"
    exit 1
  fi
}

hdfsdir 755 /user/trafodion
hdfsdir 750 /user/trafodion/bulkload
hdfsdir 750 /user/trafodion/lobs
hdfsdir 770 /user/trafodion/PIT
hdfsdir 770 /user/trafodion/backups
hdfsdir 770 /user/trafodion/backupsys

hbconf=$CONF_DIR/hbase-conf/hbase-site.xml

hbroot=$(sed -n '/>hbase.rootdir</,/value/{s;.*<value>\(.*\)</value>;\1;p}' $hbconf)

hg="$(hdfs dfs -ls -d $hbroot | awk '{print $4;}')"
if ! hdfs dfs -chgrp "$hg" /user/trafodion/bulkload
then
  echo `date`" - Failed to set set hbase group on /user/trafodion/bulkload"
  exit 1
fi

if ! hdfs dfs -chgrp "$hg" /user/trafodion/PIT
then
  echo `date`" - Failed to set set hbase group on /user/trafodion/PIT"
  exit 1
fi

if ! hdfs dfs -chgrp "$hg" /user/trafodion/backups
then
  echo "Failed to set set hbase group on /user/trafodion/backups"
  exit 1
fi

if ! hdfs dfs -chgrp "$hg" /user/trafodion/backupsys
then
  echo "Failed to set set hbase group on /user/trafodion/backupsys"
  exit 1
fi

#########################################
echo `date`" - Registering openTSDB metrics..."

auth=$(sed -n '/>hbase.security.authentication</,/value/{s;.*<value>\(.*\)</value>;\1;p}' $hbconf)

if [[ $auth == "kerberos" ]]
then
  export SECURE_HADOOP="Y"
else
  export SECURE_HADOOP="N"
fi

$TRAF_HOME/mgblty/opentsdb/tools/register_metrics.sh

#########################################
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

#########################################
echo `date`" - Initializing Meta-Data..."
set -x

echo 'initialize Trafodion;' | sqlci 2>&1 | tee $CONF_DIR/init.out

if grep -q -e '1395' -e '1392' $CONF_DIR/init.out
then
  echo `date`" - Re-trying initialize as upgrade"
  echo 'initialize Trafodion, upgrade;' | sqlci 2>&1 | tee $CONF_DIR/init_upgrade.out
else
  if grep -q -e 'ERROR' $CONF_DIR/init.out
  then
    exit 1
  fi
fi

if [[ -f $CONF_DIR/init_upgrade.out ]]
then
  if grep -q -e 'ERROR' $CONF_DIR/init_upgrade.out
  then
    exit 1
  fi
fi

echo `date`" - Initializing Tenant Meta-Data (if applicable)..."
echo 'initialize Trafodion, add tenant usage;' | sqlci 2>&1 | tee $CONF_DIR/init_tenant.out
if grep -q -e 'ERROR' $CONF_DIR/init_tenant.out
then
  exit 1
fi

echo `date`" - Initializing Library Management (if applicable)..."
echo 'initialize Trafodion, upgrade library management;' | sqlci 2>&1 | tee $CONF_DIR/init_libmgmt.out
if grep -q -e 'ERROR' $CONF_DIR/init_libmgmt.out
then
  exit 1
fi

#########################################
ldap="$TRAFODION_ENABLE_AUTHENTICATION"
root="$DB_ROOT_USER"
admin="$DB_ADMIN_USER"
if [[ $ldap == "YES" ]]
then
  # if authentication enabled, above initialization also enables authorization
  echo `date`" - Associating external users (e.g., LDAP) with DB__ROOT and DB__ADMIN"
    sqlci <<eof
        alter user DB__ROOT set external name "$root";
        alter user DB__ADMIN set external name "$admin";
        exit;
eof
  ret=$?
else
  ret=0
fi

echo "$(date) - Restarting Connections"
$DCS_INSTALL_DIR/bin/dcs-daemons.sh stop server
edb_pdsh -a pkill -9 -u $(id -u) mxosrvr
$DCS_INSTALL_DIR/bin/dcs-daemons.sh start server


exit $ret
