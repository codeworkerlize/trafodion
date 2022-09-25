#!/bin/bash
ulimit -l unlimited
ulimit -s 10240
sysctl -w kernel.msgmnb=65536
sysctl -w kernel.msgmax=65536
sysctl -w kernel.shmmax=8192000000

source /root/.bash_profile
$ZOOKEEPER_HOME/bin/zkServer.sh start
/etc/init.d/sshd start -D
$HADOOP_HOME/sbin/start-dfs.sh
$HBASE_HOME/bin/start-hbase.sh
/etc/init.d/mysql start
$HIVE_HOME/bin/hive --service metastore > /var/log/hive-metastore.log 2>&1 &
sleep 10

# modify dcs cloud configs based on whether env HOST_IP is set
if [[ ! -z $HOST_IP ]]; then
    CONTAINER_IP=`cat /etc/hosts|tail -1|awk '{print $1}'`
    sed -i "s/DCS_CLOUD_COMMAND/echo $CONTAINER_IP, $HOST_IP/" $TRAF_HOME/conf/dcs/dcs-site-cloud.xml
    cp $TRAF_CONF/dcs/dcs-site-cloud.xml $TRAF_CONF/dcs/dcs-site.xml
fi

export TRAF_AGENT=CM; $TRAF_HOME/sql/scripts/sqgen
$TRAF_HOME/mgblty/opentsdb/tools/check_tables.sh
$TRAF_HOME/mgblty/opentsdb/tools/registerMetrics.py >/dev/null
$TRAF_HOME/sql/scripts/cleanat_upgrade
export TRAF_AGENT=; gomon.cold
sleep 30 # avoid ERROR 8604
echo "initialize trafodion;" | sqlci
echo "initialize trafodion, upgrade library management;" | sqlci
connstart
echo "ESGYNDB STARTED SUCCESSFULLY"

while true;do sleep 10000000;done
