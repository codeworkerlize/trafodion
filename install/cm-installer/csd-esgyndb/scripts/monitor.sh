#!/bin/bash

action="$1"
chost="$2"
hdd_list="$3"

echo "control script: $0"
echo "ESGYN_PARCEL: $ESGYN_PARCEL"
echo "========================="
echo "Current host: $chost"
echo "Working directory: $PWD"

# Check CSD & Parcel in synch
CSD_VER="_TRAF_VERSION_"
csdmm="$(echo $CSD_VER | cut -d. -f1,2)"
parmm="$(echo $ESGYN_PARCEL_VER | cut -d. -f1,2)"
if [[ "$csdmm" != "$parmm" ]]
then
  echo "Error: EsgynDB service descriptor version does not match EsgynDB parcel version"
  echo "       CSD version: $csdmm"
  echo "    Parcel version: $parmm"
  echo " Check that CSD has been installed (usually in /opt/cloudera/csd) and Cloudera Manager has been re-started"
  exit 1
fi
if [[ -n $ESGYN_CSD_MINIMUM_VER ]]
then
  csdpatch="$(echo $CSD_VER | cut -d. -f3)"
  csdminmm="$(echo $ESGYN_CSD_MINIMUM_VER | cut -d. -f1,2)"
  csdminpatch="$(echo $ESGYN_CSD_MINIMUM_VER | cut -d. -f3)"
  if [[ "$csdminmm" == "$parmm" ]] && (( $csdminpatch > $csdpatch ))
  then
    echo "Error: EsgynDB service descriptor version does not meet required patch level"
    echo "        CSD version: $CSD_VER"
    echo "    Parcel requires: $ESGYN_CSD_MINIMUM_VER"
    echo " Check that CSD has been installed (usually in /opt/cloudera/csd) and Cloudera Manager has been re-started"
    exit 1
  fi
fi


# support for older parcels
if [[ -z $ESGYN_PARCEL ]]
then
  echo "TRAF_HOME: $TRAF_HOME"
else
  # Update standard path to current active parcel
  # trafodion home directory defaults to /opt/trafodion, but not guaranteed
  # Need a location that is write-able by trafodion user
  export TRAF_HOME="$(ls -d ~trafodion)/esgynDB"
  if [[ ! $ESGYN_PARCEL/traf_home -ef $TRAF_HOME ]]
  then
    rm -f $TRAF_HOME
    ln -s $ESGYN_PARCEL/traf_home $TRAF_HOME
    if [[ $? != 0 ]]
    then
      echo "Error: Could not create $TRAF_HOME."
      exit 1
    fi
  fi
  if [[ ! -d $ESGYN_PARCEL/traf_home ]]
  then
    echo "Error: EsgynDB parcel not found: $ESGYN_PARCEL/traf_home"
    exit 1
  fi
fi


NODE_LIST="$(cut -d: -f1 $CONF_DIR/traf_node${TRAF_INSTANCE_ID}.properties | sort -u)"

export COORDINATING_HOST=$(echo "$NODE_LIST" | head -1)
########################################################
# set up env
cd $TRAF_HOME
export HADOOP_TYPE=cloudera
export TRAF_AGENT="CM"
source ./sqenv.sh

echo "Name Server Enabled: $SQ_NAMESERVER_ENABLED"
echo "Name Server Count: $SQ_NAMESERVER_COUNT"

set -x

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
     cp -f $KRB5CCNAME /tmp/krb5cc_$(id -u)
     KRB5CCNAME=/tmp/krb5cc_$(id -u)
  fi
fi

################ Check for non-parcel install
upgrade=0
oldlibs=$(ls /opt/cloudera/parcels/CDH/lib/hbase/lib/hbase-trx* /opt/cloudera/parcels/CDH/lib/hbase/lib/trafodion-utility* 2>/dev/null)
if [[ -n $oldlibs ]]
then
  echo "ERROR: Previous non-parcel installation found."
  echo "       Remove hbase libraries on all nodes and restart HBase."
  echo $oldlibs
  echo
  upgrade=1
fi
if [[ $upgrade == 1 ]]
then
  exit 1
fi

###############################################################################
# Generate license ID to distribute via HDFS

if [[ $chost == "$COORDINATING_HOST" ]]
then
  if hdfs dfs -ls /user/trafodion/cluster_id >/dev/null
  then
    echo "Found hdfs:/user/trafodion/cluster_id"
    cl_id="found"
  else
    decoder -g > /etc/trafodion/esgyndb_id
    hdfs dfs -copyFromLocal -f /etc/trafodion/esgyndb_id /user/trafodion/cluster_id
    hdfs dfs -chmod 0400 /user/trafodion/cluster_id
    cl_id="generated"
  fi
  echo "========================="
fi

########################################################
# install license
#  keep near beginning of script before CM declares role is running successfully
echo "Esgyn License status:"
licensePath="/etc/trafodion/esgyndb_license"
MULTI_TENANCY_ENABLED=0
if [[ -z $EDB_LIC_KEY ]]
then
  rm -f $licensePath
  LicMsg="No license key -- a short grace period allowed to acquire a license.
    Contact Esgyn and send ID file: ${COORDINATING_HOST}:/etc/trafodion/esgyndb_id"
  echo "$LicMsg"
else
  LicMsg=""
  echo -n "$EDB_LIC_KEY" > $licensePath
  decoder -a -f $licensePath

  license_type=$(decoder -t -f $licensePath)
  # check license
  if [[ $cl_id == "generated" && $license_type == "PRODUCT" ]]
  then
    license_ver=$(decoder -v -f $licensePath)
    if (( $license_ver > 1 ))
    then
      echo "Error: License key not valid for new cluster ID."
      echo "       Contact Esgyn and send ID file: ${COORDINATING_HOST}:/etc/trafodion/esgyndb_id"
      exit 1
    fi
  fi
  if [[ $license_type == "INTERNAL" ]]
  then
    echo "Internal license -- skipping node check"
  else
    licensed_nodes=$(decoder -n -f $licensePath)
    config_nodes="$(cut -d: -f1 $CONF_DIR/traf_node${TRAF_INSTANCE_ID}.properties | sort -u | wc -l)"
    if [[ ! $licensed_nodes =~ ^[0-9]+$ ]] || (( $config_nodes > $licensed_nodes ))
    then
      echo "Error: License is not valid for $config_nodes nodes"
      echo "       Contact Esgyn and send ID file: ${COORDINATING_HOST}:/etc/trafodion/esgyndb_id"
      exit 1
    fi
  fi
  decoder -z -f $licensePath
  if [[ $? == 2 ]]
  then
    echo "Error: License has expired"
    echo "       Contact Esgyn and send ID file: ${COORDINATING_HOST}:/etc/trafodion/esgyndb_id"
    exit 1
  fi
  # ID file may not be available on other hosts yet (this check also made below)
  # check here to be visible before start-up time expires
  if [[ -f /etc/trafodion/esgyndb_id && $license_type == "PRODUCT" ]]
  then
    decoder -i -f $licensePath -u /etc/trafodion/esgyndb_id
    if [[ $? != 0 ]]
    then
      echo "Error: License does not match cluster ID"
      echo "       License is invalid for this cluster"
      exit 1
    fi
  fi
  
  #Check if multi-tenancy bit is enabled in license file
  featuresBit=$(decoder -x -f $licensePath)
  if (( ($featuresBit & 4) > 0 )); then
     MULTI_TENANCY_ENABLED=1
  fi  
fi
echo "========================="

########################################################
# hbase group check - in case user not created automatically
hbconf=$CONF_DIR/hbase-conf/hbase-site.xml
hbroot=$(sed -n '/>hbase.rootdir</,/value/{s;.*<value>\(.*\)</value>;\1;p}' $hbconf)
hg="$(hdfs dfs -ls -d $hbroot | awk '{print $4;}')"
if [[ ! $(id -nG trafodion) =~ hbase ]]
then
  echo "Error: trafodion user is not a member of the '$hg' group"
  exit 1
fi

########################################################
# dependencies installed?
$CONF_DIR/scripts/depend_chk.sh $MULTI_TENANCY_ENABLED $DCS_KEEPALIVED || exit 1
echo "========================="

########################################################
# Discover ZK config
# Cloudera agent passes in ZK_QUORUM, since we depend on zookeeper
zknodes=$(echo $ZK_QUORUM | sed -e 's/:[0-9]*,/,/g' -e 's/:[0-9]*$//')
zkport=$(echo $ZK_QUORUM | sed -e 's/^.*:\([0-9]*\)$/\1/')


########################################################
# Discover Sentry config
coreconf=$CONF_DIR/hadoop-conf/core-site.xml
secgrp=$(sed -n '/>hadoop.security.group.mapping</,/value/{s;.*<value>\(.*\)</value>;\1;p}' $coreconf)
if [[ $secgrp =~ LdapGroupsMapping ]]
then
  secmode="LDAP"
elif [[ $secgrp =~ ShellBasedUnixGroupsMapping ]]
then
  secmode="SHELL"
else
  secmode="NONE"
fi

########################################################
# install conf files

echo "Installing config files"
if [[ ! -e /etc/trafodion/conf ]]
then
  ln -s /etc/esgyndb/conf /etc/trafodion/conf
fi

echo "Installing Trafodion config: /etc/trafodion/trafodion_config"

# Note: The existence of TRAF_AGENT changes the logic flow of several
#       operational scripts.
#       When in 'agent' mode, the assumption is that MPI is not used
#       to create the monitor processes and something like the supervisord
#       process is the creator of monitor processes, and operational
#       script operate on a single local node and not accross all nodes
#       in a cluster.
echo "export TRAF_HOME=$TRAF_HOME" > /etc/trafodion/trafodion_config
echo "export TRAF_LOG=$TRAF_LOG" >> /etc/trafodion/trafodion_config
echo "export TRAF_VAR=$TRAF_VAR" >> /etc/trafodion/trafodion_config
echo "export MPI_TMPDIR=$TRAF_VAR" >> /etc/trafodion/trafodion_config
echo "export MONITOR_COMM_PORT=$MONITOR_COMM_PORT" >> /etc/trafodion/trafodion_config
echo "export TRAF_CONF=$CONF_DIR/traf" >> /etc/trafodion/trafodion_config
echo 'export REST_CONF_DIR=$TRAF_CONF/rest' >> /etc/trafodion/trafodion_config
echo 'export DCS_CONF_DIR=$TRAF_CONF/dcs' >> /etc/trafodion/trafodion_config
echo "export TRAF_CLUSTER_ID=$TRAF_CLUSTER_ID" >> /etc/trafodion/trafodion_config
echo export TRAF_CLUSTER_NAME=\"${TRAF_CLUSTER_NAME}\" >> /etc/trafodion/trafodion_config
echo "export TRAF_INSTANCE_ID=$TRAF_INSTANCE_ID" >> /etc/trafodion/trafodion_config
echo export TRAF_INSTANCE_NAME=\"${TRAF_INSTANCE_NAME}\" >> /etc/trafodion/trafodion_config
echo export TRAF_AGENT=\"${TRAF_AGENT}\" >> /etc/trafodion/trafodion_config
echo "export JAVA_HOME=$JAVA_HOME" >> /etc/trafodion/trafodion_config
echo "export HADOOP_TYPE=cloudera" >> /etc/trafodion/trafodion_config
echo "export USER=$(id -un)" >> /etc/trafodion/trafodion_config
echo "export DB_ROOT_USER=$DB_ROOT_USER" >> /etc/trafodion/trafodion_config
echo "export DB_ADMIN_USER=$DB_ADMIN_USER" >> /etc/trafodion/trafodion_config
echo "export TRAFODION_ENABLE_AUTHENTICATION=$TRAFODION_ENABLE_AUTHENTICATION" >> /etc/trafodion/trafodion_config
echo "export ZOOKEEPER_NODES=$zknodes" >> /etc/trafodion/trafodion_config
echo "export ZOOKEEPER_PORT=$zkport" >> /etc/trafodion/trafodion_config
echo "export TRAF_ROOT_ZNODE=$TRAF_ROOT_ZNODE" >> /etc/trafodion/trafodion_config
if [[ $EDB_HIVE_SENTRY == "true" ]]
then
  echo "export SENTRY_SECURITY_FOR_HIVE=TRUE" >> /etc/trafodion/trafodion_config
else
  echo "export SENTRY_SECURITY_FOR_HIVE=FALSE" >> /etc/trafodion/trafodion_config
fi
echo "export SENTRY_SECURITY_GROUP_MODE=$secmode" >> /etc/trafodion/trafodion_config
# picked up from esgyndb_trx parcel
echo "export CLUSTERNAME=CDH" >> /etc/trafodion/trafodion_config
echo "export PARCEL_PATH=$PARCELS_ROOT/CDH" >> /etc/trafodion/trafodion_config

hbconf=$CONF_DIR/hbase-conf/hbase-site.xml
auth=$(sed -n '/>hbase.security.authentication</,/value/{s;.*<value>\(.*\)</value>;\1;p}' $hbconf)
HBASE_RPC_TIMEOUT=$(sed -n '/>hbase.rpc.timeout</,/value/{s;.*<value>\(.*\)</value>;\1;p}' $hbconf)
HBASE_RPC_PROTECTION=$(sed -n '/>hbase.rpc.protection</,/value/{s;.*<value>\(.*\)</value>;\1;p}' $hbconf)
securehadoop="N"

if [[ $auth == "kerberos" ]]
then
  securehadoop="Y"
  jce=$($JAVA_HOME/bin/jrunscript -e 'print (javax.crypto.Cipher.getMaxAllowedKeyLength("RC5"));')
  if (( ! $jce >= 256 ))
  then
    echo "Error: Java not enabled for sufficient crypto key length to support Kerberos"
    echo "   JCE Unlimited required"
    exit 1
  fi

  echo "export KRB5CCNAME=$KRB5CCNAME" >> /etc/trafodion/trafodion_config

  hiveconf=$CONF_DIR/hive-conf/hive-site.xml
  HIVE_KP=$(sed -n '/>hive.server2.authentication.kerberos.principal</,/value/{s;.*<value>\(.*\)</value>;\1;p}' $hiveconf)
  echo "export HIVE_PRINCIPAL=$HIVE_KP" >> /etc/trafodion/trafodion_config
  echo "export HIVESERVER2_URL=$EDB_HIVESERVER2_URL" >> /etc/trafodion/trafodion_config
  echo "export HIVESERVER2_SSL=$EDB_HIVESERVER2_SSL" >> /etc/trafodion/trafodion_config

else
  securehadoop="N"
fi
echo "export SECURE_HADOOP=$securehadoop" >> /etc/trafodion/trafodion_config

########################################################
# Enable CGroup permissions for multi-tenancy
cgroups=$(ps -o cgroup --no-headers $$)
if [[ $cgroups == "-" ]]
then
  echo "Resource static pools not enabled"
  echo "export ESGYN_CG_CPU=''" >> /etc/trafodion/trafodion_config
  echo "export ESGYN_CGP_CPU=''" >> /etc/trafodion/trafodion_config
  echo "export ESGYN_CG_CPUACCT=''" >> /etc/trafodion/trafodion_config
  echo "export ESGYN_CGP_CPUACCT=''" >> /etc/trafodion/trafodion_config
  echo "export ESGYN_CG_MEM=''" >> /etc/trafodion/trafodion_config
  echo "export ESGYN_CGP_MEM=''" >> /etc/trafodion/trafodion_config
else
  if [[ ! $(id -Gn) =~ trafodion ]]
  then
    echo "Error: Current user ($(id -un)) not member of trafodion group."
    echo "   Group membership required to enable tenant cgroup permissions."
    exit 1
  fi
  $DBMGR_INSTALL_DIR/bin/multi_tenant_enable
  cut -d: -f2,3 --output-delimiter=' ' /proc/$$/cgroup | \
    while read cg cgpath
    do
      if [[ $cg =~ cpu$|cpu, ]]
      then
        echo "export ESGYN_CG_CPU=${cg}:$cgpath" >> /etc/trafodion/trafodion_config
        mnt=$(lssubsys -m cpu | cut -f2 -d' ')
        echo "export ESGYN_CGP_CPU=$mnt${cgpath}" >> /etc/trafodion/trafodion_config
      fi
      if [[ $cg =~ cpuacct ]]
      then
        echo "export ESGYN_CG_CPUACCT=${cg}:$cgpath" >> /etc/trafodion/trafodion_config
        mnt=$(lssubsys -m cpuacct | cut -f2 -d' ')
        echo "export ESGYN_CGP_CPUACCT=$mnt${cgpath}" >> /etc/trafodion/trafodion_config
      fi
      if [[ $cg =~ memory ]]
      then
        echo "export ESGYN_CG_MEM=${cg}:$cgpath" >> /etc/trafodion/trafodion_config
        mnt=$(lssubsys -m memory | cut -f2 -d' ')
        echo "export ESGYN_CGP_MEM=$mnt${cgpath}" >> /etc/trafodion/trafodion_config
      fi
    done
fi
echo "========================="

# Safety Valve add-ons
if [[ -f $CONF_DIR/trafodion_config ]]
then
  cat $CONF_DIR/trafodion_config >> /etc/trafodion/trafodion_config
  echo "" >> /etc/trafodion/trafodion_config # make sure we have newline
fi

#########################################################
source /etc/trafodion/trafodion_config

# set cluster ID
if [[ $chost == "$COORDINATING_HOST" ]]
then
  $TRAF_HOME/sql/scripts/xdc -setmyid $TRAF_CLUSTER_ID
fi

# intialize config files from templates
mkdir -p $TRAF_CONF
cp -rf $TRAF_HOME/conf/* $TRAF_CONF/

echo "Installing Trafodion config: $TRAF_CONF/trafodion-site.xml"
cp -f $CONF_DIR/trafodion-site.xml $TRAF_CONF/trafodion-site.xml

# install trafodion bashrc for service user / admin commands
cp -f $TRAF_HOME/sysinstall/home/trafodion/.bashrc ~trafodion/
echo "source ~trafodion/.bashrc" > ~trafodion/.bash_profile

# LDAP config
if [[ $TRAFODION_ENABLE_AUTHENTICATION == "YES" ]]
then
  ldap_host_list=""
  for lhost in $ldap_hosts
  do
    ldap_host_list+="  LdapHostname: $lhost\n"
  done
  IFS=";"
  ldap_uid_list=""
  for lid in $ldap_identifiers
  do
    ldap_uid_list+="  UniqueIdentifier: $lid\n"
  done
  unset IFS
  echo "Installing LDAP config: $TRAF_CONF/.traf_authentication_config"
  sed -e "s#{{ldap_certpath}}#$ldap_certpath#" \
      -e "s#{{ldap_domain}}#$ldap_domain#" \
      -e "s#{{ldap_hosts}}#$ldap_host_list#" \
      -e "s#{{ldap_port}}#$ldap_port#" \
      -e "s#{{ldap_identifiers}}#$ldap_uid_list#" \
      -e "s#{{ldap_search_id}}#$ldap_search_id#" \
      -e "s#{{ldap_pwd}}#$ldap_pwd#" \
      -e "s#{{ldap_encrypt}}#$ldap_encrypt#" \
      -e "s#{{ldap_search_group_base}}#$ldap_search_group_base#" \
      -e "s#{{ldap_search_group_object_class}}#$ldap_search_group_object_class#" \
      -e "s#{{ldap_search_group_member_attribute}}#$ldap_search_group_member_attribute#" \
      -e "s#{{ldap_search_group_name_attribute}}#$ldap_search_group_name_attribute#" \
    $CONF_DIR/traf_authentication_config > $TRAF_CONF/.traf_authentication_config

  if [[ $(grep -c "SECTION:" $TRAF_CONF/.traf_authentication_config) > 2 ]]
  then
    echo "export TRAFAUTH_CONFIGFILE_FORMAT=v2" >> /etc/trafodion/trafodion_config
  else
    echo "export TRAFAUTH_CONFIGFILE_FORMAT=v1" >> /etc/trafodion/trafodion_config
  fi
  echo "Checking LDAP configuration"
  ldapconfigcheck || exit 1
else
  cp $TRAF_HOME/sql/scripts/traf_authentication_config $TRAF_CONF/.traf_authentication_config
fi

# Kerberos
if [[ -s $CONF_DIR/esgyndb.keytab ]]
then
  cp -f $CONF_DIR/esgyndb.keytab $TRAF_CONF/
  echo "export KEYTAB=$TRAF_CONF/esgyndb.keytab" >> /etc/trafodion/trafodion_config
  kerberos=true
else
  kerberos=false
fi

# install DCS/REST conf files

DEF_INTERFACE="$(route | awk '$1=="default" {print $NF;}'|head -n 1)"

xmlprop="  <property>\n    <name>dcs.zookeeper.quorum</name>\n    <value>$zknodes</value>\n  </property>"
xmlprop+="\n  <property>\n    <name>dcs.zookeeper.property.clientPort</name>\n    <value>$zkport</value>\n  </property>"
xmlprop+="\n  <property>\n    <name>zookeeper.znode.parent</name>\n    <value>$TRAF_ROOT_ZNODE/$TRAF_INSTANCE_ID</value>\n  </property>"
sed -e "/>dcs.dns.interface</,+1s/default/$DEF_INTERFACE/" \
    -e "/>dcs.master.floating.ip.external.interface</,+1s/default/$DEF_INTERFACE/" \
    -e "/<\/configuration/i\\${xmlprop}" \
    $CONF_DIR/dcs-site.xml > $TRAF_CONF/dcs/dcs-site.xml

cp -f $CONF_DIR/dcs-env.sh $TRAF_CONF/dcs/
echo "" >> $TRAF_CONF/dcs/dcs-env.sh   # in case of safety valve w/o trailing newline
echo "export DCS_MASTER_PORT=$DCS_MASTER_PORT" >> $TRAF_CONF/dcs/dcs-env.sh
sed -e 's/:.*=/ /' $CONF_DIR/dcs_servers${TRAF_INSTANCE_ID}.properties > $TRAF_CONF/dcs/servers

echo "export ENABLE_HA=$DCS_MASTER_HA" >> /etc/trafodion/trafodion_config
echo "export KEEPALIVED=$DCS_KEEPALIVED" >> /etc/trafodion/trafodion_config

export USE_NEW_EDB_PDSH=${USE_NEW_EDB_PDSH:-1}
echo "export USE_NEW_EDB_PDSH=${USE_NEW_EDB_PDSH}" >> /etc/trafodion/trafodion_config

if [[ $DCS_MASTER_HA == "true" ]]
then
  if [[ $DCS_MASTER_FLOATING_IP == "0.0.0.0" ]]
  then
    echo "Error: Floating IP address required when Connectivity High Availability enabled"
    exit 1
  fi
  echo "export DCS_MASTER_FLOATING_IP=$DCS_MASTER_FLOATING_IP" >> $TRAF_CONF/dcs/dcs-env.sh
fi

echo "Updating REST Server configuration..."
set +x
xmlprop="  <property>\n    <name>rest.zookeeper.quorum</name>\n    <value>$zknodes</value>\n  </property>"
xmlprop+="\n  <property>\n    <name>rest.zookeeper.property.clientPort</name>\n    <value>$zkport</value>\n  </property>"
xmlprop+="\n  <property>\n    <name>zookeeper.znode.parent</name>\n    <value>$TRAF_ROOT_ZNODE/$TRAF_INSTANCE_ID</value>\n  </property>"

if [[ $ENABLE_HTTPS == "true" ]]
then
   if [ -z "$KEYSTORE_PASS" ];
   then
    echo "Error: Keystore password is required when SSL is enabled"
    exit 1
   fi
   pass_in=$KEYSTORE_PASS
   pass_out=$($JAVA_HOME/bin/java -cp $DBMGR_INSTALL_DIR/lib/jetty*.jar org.eclipse.jetty.util.security.Password $pass_in 2>&1)
   obf_key_pass=""

   for p in $pass_out
   do
     if [[ $p =~ ^OBF: ]];
     then
        obf_key_pass=$p
        break
     fi
   done

   xmlprop+="\n  <property>\n    <name>rest.keystore</name>\n    <value>$HOME/sqcert/server.keystore</value>\n  </property>"
   xmlprop+="\n  <property>\n    <name>rest.ssl.password</name>\n    <value>$obf_key_pass</value>\n  </property>"
fi

sed -e "/>rest.dns.interface</,+1s/default/$DEF_INTERFACE/" \
    -e "/<\/configuration/i\\${xmlprop}" \
       $CONF_DIR/rest-site.xml > $TRAF_CONF/rest/rest-site.xml
set -x
echo "========================="

# DBmgr opentsdb config
TIME_ZONE="$($TRAF_HOME/tools/gettimezone.sh | head -1)"
if [[ -z $TIME_ZONE ]]
then
  TIME_ZONE=UTC
fi

OTSDBconf="$TRAF_CONF/mgblty/opentsdb/opentsdb.conf"
sed -i -e "s/tsd.storage.hbase.zk_quorum.*$/tsd.storage.hbase.zk_quorum = ${ZK_QUORUM}/" \
       -e "s@TIMEZONE_NAME@$TIME_ZONE@g" \
       $OTSDBconf
	   
sed -i '/ ------- Properties to limit queuing of RPC to HBase/d' $OTSDBconf
echo "" >> $OTSDBconf
echo "# ------- Properties to limit queuing of RPC to HBase -------" >> $OTSDBconf
if [[ ! -z $HBASE_RPC_TIMEOUT ]]
then
   sed -i '/hbase.rpc.timeout\=/d' $OTSDBconf
   echo "hbase.rpc.timeout=$HBASE_RPC_TIMEOUT" >> $OTSDBconf
fi

if [[ ! -z "$OPENTSDB_RPC_LIMIT" && "$OPENTSDB_RPC_LIMIT" -gt 0 ]]
then
   sed -i '/hbase.region_client.inflight_limit\=/d' $OTSDBconf
   sed -i '/hbase.region_client.pending_limit\=/d' $OTSDBconf

   echo "hbase.region_client.inflight_limit=$OPENTSDB_RPC_LIMIT" >> $OTSDBconf 
   echo "hbase.region_client.pending_limit=$OPENTSDB_RPC_LIMIT" >> $OTSDBconf 
fi

sed -i '/hbase.nsre.high_watermark\=/d' $OTSDBconf
if [[ ! -z "$OPENTSDB_HBASE_NSRE_HIGHWATER" && "$OPENTSDB_HBASE_NSRE_HIGHWATER" -gt 0 ]]
then
   echo "hbase.nsre.high_watermark=$OPENTSDB_HBASE_NSRE_HIGHWATER" >> $OTSDBconf
fi

sed -i '/ ------- Properties to access secure hbase/d' $OTSDBconf
sed -i '/hbase.security.auth.enable\=/d' $OTSDBconf
sed -i '/hbase.security.authentication\=/d' $OTSDBconf
sed -i '/hbase.kerberos.regionserver.principal\=/d' $OTSDBconf
sed -i '/hbase.sasl.clientconfig\=/d' $OTSDBconf
sed -i '/hbase.rpc.protection\=/d' $OTSDBconf

if [[ $kerberos == "true" ]]
then
  HBR_KP=$(sed -n '/>hbase.regionserver.kerberos.principal</,/value/{s;.*<value>\(.*\)</value>;\1;p}' $hbconf)
  echo "" >> $OTSDBconf
  echo "# ------- Properties to access secure hbase -------" >> $OTSDBconf
  echo "hbase.security.auth.enable=true" >> $OTSDBconf
  echo "hbase.security.authentication=kerberos" >> $OTSDBconf
  echo "hbase.kerberos.regionserver.principal=$HBR_KP" >> $OTSDBconf
  echo "hbase.sasl.clientconfig=Client" >> $OTSDBconf
fi
if [[ ! -z $HBASE_RPC_PROTECTION ]]
then
   echo "hbase.rpc.protection=$HBASE_RPC_PROTECTION" >> $OTSDBconf
fi



# DBmgr t-collector config
sed -i -e "s/TSD_PORT=5242/TSD_PORT=$DBMGR_TSD_PORT/g" $TRAF_HOME/mgblty/tcollector/startstop
sed -i -e "s/60010/$DBMGR_HBI_PORT/g" $TRAF_HOME/mgblty/tcollector/collectors/0/hbase_master.py
sed -i -e "s/60030/$DBMGR_HBRS_PORT/g" $TRAF_HOME/mgblty/tcollector/collectors/0/hbase_regionserver.py
echo "========================="

#########################################################
source /etc/trafodion/trafodion_config

if [[ $chost == "$COORDINATING_HOST" ]]
then
  ########################################################
  # cleanup any old meta-data

  # cleanat_upgrade - delete pre-2.3 DTM
  echo "Migrating meta-data, if needed"
  $TRAF_HOME/sql/scripts/cleanat_upgrade
  echo "Checking TSDB tables"
  $TRAF_HOME/mgblty/opentsdb/tools/check_tables.sh
  echo "========================="
fi

# Update opentsdb config
export TRAF_CONF=$CONF_DIR/traf
$TRAF_HOME/mgblty/opentsdb/tools/set_tables.sh
echo "========================="

########################################################
# find info used to generate sqconfig

# discover processors
cores=$(lscpu | grep "CPU(s) list" | awk '{print $4}' | sed -e "s@,@-@" )
maxCores=$(echo $cores | sed 's/.*\-//')

if [[ "$maxCores" -gt "255" ]]; then
   cores="0-255"
fi
processors=$(lscpu | grep "Socket(s)" | awk '{print $2}')

# On some VMs it seems that "Socket(s)" is not listed in
# lscpu so, we will default processors=1 in that case
if [ "$processors" == "" ]; then
   processors=$(lscpu | grep "CPU socket(s)" | awk '{print $3}')
   if [ "$processors" == "" ]; then
      echo "***WARNING: unable to determine number of sockets with 'lscpu'..."
      echo "***WARNING: ...defaulting sqconfig 'processors=1'"
      processors="1"
   fi
fi

########################################################
# generate sqconfig

cd $TRAF_HOME/sql/scripts
echo "PWD=$PWD"
sqconfig=$TRAF_CONF/sqconfig

echo "Generating sqconfig file"
echo "#" > $sqconfig
echo "begin node" >> $sqconfig
ncount=$(echo "$NODE_LIST" | wc -l)
node_id=0
for node in $NODE_LIST
do
   echo "  Node $node_id $node"
   echo "node-id=$node_id;node-name=$node;cores=$cores;processors=$processors;roles=connection,aggregation,storage" >> $sqconfig
   ((node_id++))
done
echo "end node" >> $sqconfig
if [[ "$SQ_NAMESERVER_ENABLED" == "1" ]]; then
echo "" >> $sqconfig
echo "begin name-server" >> $sqconfig
nsnode_id=0
for nsnode in $NODE_LIST
do
   if [[ "$nsnode_id" == "0" ]]; then
     echo "  Name Server Node $nsnode_id $nsnode"
     echo -n "nodes=$nsnode" >> $sqconfig
   else
     if [[ "$nsnode_id" -lt "$SQ_NAMESERVER_COUNT" ]]; then
       echo "  Name Server Node $nsnode_id $nsnode"
       echo -n ",$nsnode" >> $sqconfig
     fi
   fi
   ((nsnode_id++))
done
echo "" >> $sqconfig
echo "end name-server" >> $sqconfig
fi
echo "" >> $sqconfig
echo "begin overflow" >> $sqconfig
IFS=","
for dir in $hdd_list
do
  echo "hdd $dir" >> $sqconfig
done
unset IFS
echo "end overflow" >> $sqconfig

###############################################################################
# Cleanup Trafodion execution environment
#
echo "Removing old monitor.port* files from $MPI_TMPDIR"
echo ""
rm -f $MPI_TMPDIR/monitor.port.*

# Remove old shared segments, queues, etc.
echo "Executing sqipcrm (output to sqipcrm.out)"
echo ""
sqipcrm -local > $TRAF_LOG/sqipcrm.out 2>&1

echo "Removing old ${TRAF_VAR}/sqconfig.db"
echo ""
rm -f ${TRAF_VAR}/sqconfig.db

echo "Removing any stale role znode ($chost)"
echo ""
cleanZKmonZNode $chost

echo "Stopping any stale managability processes"
echo ""
mgblty_stop -m
echo "========================="

###############################################################################
# use sqconfig info to create the Trafodion cluster static configuration
if [[ "$SQ_MSENV_REGEN_DISABLED" == "1" ]]; then
  echo "Warning: 'ms.env' file will not be removed and generated if it exists!"
else
  rm -f $TRAF_VAR/ms.env > /dev/null
fi
# Safety Valve add-ons
if [[ -f $CONF_DIR/ms.env ]]
then
  cp $CONF_DIR/ms.env $TRAF_CONF/ms.env.add
  echo "" >> $TRAF_CONF/ms.env.add  # make sure we have newline
fi
echo "Running sqgen"
export SQ_CLASSPATH=
export CLASSPATH=
sqgen
if [[ $? != 0 ]]
then
  echo "Error: 'sqgen' script failed!"
  exit 1
fi
sed -i '/KRB5CCNAME\=/d' $TRAF_VAR/ms.env
echo "KRB5CCNAME=$KRB5CCNAME" >> $TRAF_VAR/ms.env
echo "========================="

# Dump debug info stderr
export >&2

# Read the latest HBASE_CLASSPATH after sqgen which removes the hbase_classpath cache file
source ~trafodion/.bashrc
echo "export HBASE_CLASSPATH=$HBASE_CLASSPATH" >> /etc/trafodion/trafodion_config
source /etc/trafodion/trafodion_config

###############################################################################
# generate sqcert files if they don't exist

if [[ $ENABLE_HTTPS == "true" ]]
then
  echo "Creating Esgyn server keystore..."
  set +x # do not expose password in command trace
  $TRAF_HOME/sql/scripts/sqcertgen gen_keystore $KEYSTORE_PASS
  set -x 
fi

# update id from coordinating node
for time in 3 3 6 6 9 9 30 40 60 80
do
  if hdfs dfs -ls /user/trafodion/cluster_id 2>/dev/null
  then
    break
  fi
  sleep $time
done
hdfs dfs -copyToLocal /user/trafodion/cluster_id /etc/trafodion/esgyndb_id
chmod 0644 /etc/trafodion/esgyndb_id

if [[ -n $EDB_LIC_KEY && $license_type == "PRODUCT" ]]
then
  decoder -i -f $licensePath -u /etc/trafodion/esgyndb_id
  if [[ $? != 0 ]]
  then
    echo "Error: License does not match cluster ID"
    echo "       License is invalid for this cluster"
    exit 1
  fi
fi

###############################################################################
# start krb5service to check and renew kerberos tickets
if [[ $auth == "kerberos" ]]
then
   $TRAF_HOME/sql/scripts/krb5service restart
fi
echo "========================="

if [[ ! -z "$ESGYNDB_MEMORY_LIMIT" && "$ESGYNDB_MEMORY_LIMIT" -gt 0 ]]
then
   echo "Overriding EsgynDB cgroup memory limits"
   ESGYN_CGROUP_NAME="$(echo $ESGYN_CG_MEM | cut -d '/' -f2)"
   MEM_LIMIT=`expr $ESGYNDB_MEMORY_LIMIT`g
   SWAP_LIMIT=`expr 3 \* $ESGYNDB_MEMORY_LIMIT`g
   cgset -r memory.limit_in_bytes=$MEM_LIMIT $ESGYN_CGROUP_NAME
   cgset -r memory.memsw.limit_in_bytes=$SWAP_LIMIT $ESGYN_CGROUP_NAME
fi


###############################################################################
# Create tenant level cgroup on current node
if [[ $MULTI_TENANCY_ENABLED == "1" ]];then
   $JAVA_HOME/bin/java com.esgyn.common.CGroupHelper initZK
fi

###############################################################################
# start monitor & bring up cluster

if [[ -n "$LicMsg" ]]
then
  echo "$LicMsg"
  echo "========================="
fi

echo "Background job gomon.sh on $chost"
echo "Log: $TRAF_LOG/gomon.log"
mkdir $TRAF_LOG 2>/dev/null
if [[ ! -d $TRAF_LOG ]];then
  echo "Error: $TRAF_LOG not created"
  exit 1
fi
$CONF_DIR/scripts/gomon.sh "$chost" > $TRAF_LOG/gomon.log 2>&1 &

echo "`date`: Working directory: $PWD"
echo "`date`: Starting monitor process"
echo "`date`: TRAF_ROOT_ZNODE=${TRAF_ROOT_ZNODE}"
echo "`date`: MONITOR_COMM_PORT=${MONITOR_COMM_PORT}"

if [[ "$SQ_VALGRIND_MONITOR_ENABLED" == "1" ]]; then
   which valgrind
   valgrind_exists_status=$?
   if [[ $valgrind_exists_status != 0 ]]; then
      echo
      echo "SQ_VALGRIND_MONITOR_ENABLED is enabled and 'valgrind' is not installed."
      echo "Install 'valgrind' first and start node role again."
      echo "Starting node role without 'valgrind'!"
      echo
      exec monitor COLD_AGENT
   else
      valgrind_log_dir=${SQ_VALGRIND_LOG_DIR:-$TRAF_VAR}
      exec valgrind $SQ_VALGRIND_OPTIONS monitor COLD_AGENT > $valgrind_log_dir/monitor.valgrind.$chost.log 2>&1 
   fi
else
   exec monitor COLD_AGENT
fi


echo "Error: exec failed"
exit 1
