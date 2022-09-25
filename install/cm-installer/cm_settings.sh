#!/bin/bash

URL=http://localhost:7180
CLUST="Cluster%201"
USER=admin
PW=""
mode="prompt"
remove=""
auth="user"
if [[ -x $(dirname $0)/jq ]]
then
  JQ="$(dirname $0)/jq"
elif [[ -n $(which jq) ]]
then
  JQ="$(which jq)"
else
  echo "Error: Could not find jq tool. It should be packaged with this script."
  exit 1
fi

usage='
cm_settings.sh - Set Hadoop configuration for EsgynDB via Cloudera Manager

 Options:
    -s <URL>     [Default: http://localhost:7180]
    -c <cluster> [Default: "Cluster 1"] 
    -f           [Do not prompt, set all recommended & mandatory settings]
    -m           [Do not prompt, set only mandatory & health-check default settings]
    -e           [Do not prompt, set only health-check default settings]
    -D           [Delete mandatory settings, allows HBase to run without QIANBASE_TRX]

 Authentication Options:
    -n            [Use ~/.netrc user/password, see curl(1)]

    -u <user>     [Default: admin]
    -p <password> [Default: prompts for password]
            Warning: password on command-line is not secure.
'

while [[ $# -gt 0 ]]; do
    case "$1" in
        --help|-h)
            echo "$usage"
            exit -1
            ;;
        -s)
            URL="$2"
            shift
            ;;
        -c)
            CLUST=$(echo "$2" | sed 's/ /%20/g')
            shift
            ;;
        -u)
            USER="$2"
            shift
            ;;
        -p)
            PW="$2"
            shift
            ;;
        -D)
            remove="DEL"
            ;;
        -f)
            mode="all"
            ;;
        -m)
            [[ $mode != "all" ]] && mode="man"
            ;;
        -e)
            [[ $mode != "all" && $mode != "man" ]] && mode="health"
            ;;
        -n)
            auth="net"
            ;;
        *)
            echo "***ERROR: unknown parameter '$1'"
            echo "$usage"
            exit -1
    esac
    shift
done

# auth options
# -k -- connect even if using self-signed cert
# -n -- use .netrc for password
# -s -- silent download progress
if [[ $auth == "net" ]]
then
  aopt="-k -s -n"
elif [[ -n $PW ]]
then
  aopt="-k -s -u ${USER}:${PW}"
else
  aopt="-k -s -u ${USER}"
fi

session=/tmp/cm_setting.session.$$

# check connection and authenticate to get session cookie
echo "Authenticating to Cloudera Manager"
curl $aopt -c $session "$URL/api/v3/clusters" >/dev/null
if [[ $? != 0 ]]
then
  echo "Error: Cannot connect to Cloudera Manager"
  rm -f $session
  exit 1
fi
if [[ ! -s $session ]]
then
  echo "Error: No session cookie returned ($session)."
  exit 1
else
  chmod 600 $session
fi

cluster="$URL/api/v3/clusters/$CLUST"

# read options
ropt="-k -s -b $session"
# update options
uopt="$ropt -X PUT -H Content-Type:application/json"

# check cluster name
echo "Checking cluster name"
out=$(curl $ropt $cluster | $JQ -r '.name' 2>/dev/null)
if [[ $out == "null" ]]
then
  echo "Error: Did not find cluster at $cluster"
  rm -f $session
  exit 2
fi

# query service names
servlist=$(curl $ropt "$cluster/services" | $JQ '.items[]|{(.type):.name}')
set +x
hbase=$(echo $servlist | $JQ -r '.HBASE' | grep -v null)
hdfs=$(echo $servlist | $JQ -r '.HDFS' | grep -v null)
zk=$(echo $servlist | $JQ -r '.ZOOKEEPER' | grep -v null)
sentry=$(echo $servlist | $JQ -r '.SENTRY' | grep -v null)
esgyndb=$(echo $servlist | $JQ -r '.QIANBASE' | grep -v null)

function rolelist {
  serv="$1"
  tf=$(mktemp)
  curl $ropt "$cluster/services/$serv/roleConfigGroups"

  rm $tf
}

# collect change summary
SUM=""

# query caches
rcache=""
declare -A role_config_groups
declare -A role_group_name

function set_conf {
  serv="$1"
  roletype="$2"
  param="$3"
  match="$4"
  value="$5"

  # get role-groups for service if not already cached
  if [[ $roletype == "Service-Wide" ]]
  then 
    role_config_groups[$roletype]="$roletype"
    role_group_name[$roletype]=""
  elif [[ $rcache != $serv ]]
  then
    tf=$(mktemp)
    curl $ropt "$cluster/services/$serv/roleConfigGroups" | $JQ -r '.items[]|.name,.displayName,.roleType' > $tf
    while read rname
    do
      read display
      role_group_name[$rname]+="($display)"
      read rtype
      role_config_groups[$rtype]+=" $rname"
    done < $tf
    rm $tf
    rcache="$serv"
  fi
  echo ""
  echo "======================================="
  echo "Checking Service: $serv Parameter: $param"
  echo ""
  for rname in ${role_config_groups[$roletype]}
  do
    # get configs for rolegroup
    if [[ $roletype == "Service-Wide" ]]
    then
      confset=$(curl $ropt "$cluster/services/$serv/config?view=full" | $JQ '.items[]|{(.name):.value}')
    else
      confset=$(curl $ropt "$cluster/services/$serv/roleConfigGroups/$rname/config?view=full" | $JQ '.items[]|{(.name):.value}')
    fi
    CurrVal=$(echo "$confset"|grep "\"$param\"" | cut -f2 -d: | sed -e 's/^[[:space:]]*//')
    if [[ $CurrVal =~ '"' ]]
    then
      CurrVal=$(echo $CurrVal | sed -e 's/[^"]*"\(.*\)"$/\1/')
    fi
    # sentry has some interpretation of null to default values
    if [[ $CurrVal == 'null' ]]
    then
      if [[ $param == 'sentry_service_admin_group' ]]
      then
        CurrVal='hive,impala,hue,solr,kafka'
      fi
      if [[ $param == 'sentry_service_allow_connect' ]]
      then
        CurrVal='hive,impala,hue,hdfs,solr,kafka'
      fi
    fi
    if [[ $match == "exact" && $CurrVal == $value ]]
    then
      echo "RoleGroup: $rname ${role_group_name[$rname]}"
      echo "  $param already set to correct value"
      continue
    elif [[ $match =~ "match" && $CurrVal =~ $value ]]
    then
      echo "RoleGroup: $rname ${role_group_name[$rname]}"
      echo "  $param already contains needed value"
      continue
    elif [[ $match =~ "ifnull" && $CurrVal != 'null' ]]
    then
      echo "RoleGroup: $rname ${role_group_name[$rname]}"
      echo "  Already has a value"
      continue
    elif [[ $match == "DEL" && ! $CurrVal =~ $value ]]
    then
      echo "RoleGroup: $rname ${role_group_name[$rname]}"
      echo "  $param already has TRX component removed"
      continue
    elif [[ $match =~ "DEL" && $CurrVal =~ $value ]]
    then
      echo "RoleGroup: $rname ${role_group_name[$rname]}"
      echo "  Current value of $param: $CurrVal"
      value="$( echo $CurrVal | sed -e "s#,\?$value##")"
    else
      echo "RoleGroup: $rname ${role_group_name[$rname]}"
      echo "  Current value of $param: $CurrVal"
      echo ""
    fi
    if [[ $mode == "prompt" ]]
    then
      echo ""
      echo "Proposed value: $value"
      if [[ $match == "exact" || $match =~ "DEL" || $CurrVal == "null" ]]
      then
        read -p "Change? (y/[n]): " change
      else
        read -p "Append? (y/[n]): " change
        if [[ $change =~ ^y ]]
        then
          if [[ $match == "match-list" ]]
          then
            value="${CurrVal},$value"
          else
            value="${CurrVal}$value"
          fi
        fi
      fi
      if [[ ! $change =~ ^y ]]
      then
        echo "Skipping $param"
        continue
      fi
    else # forced mode
      if [[ $CurrVal != "null" ]]
      then
        if [[ $match == "match-list" ]]
        then
          value="${CurrVal},$value"
        fi
      fi
    fi
    echo "Setting Service: $serv RoleGroup: $rname ${role_group_name[$rname]} Parameter: $param"
    DATA='{ "items": [ { "name":"'$param'", "value":"'$value'" } ] }'
    SUM+="${serv}:${role_group_name[$rname]}  $param
"
    SUM+="    $value

"
    if [[ $roletype == "Service-Wide" ]]
    then
      curl $uopt --data "$DATA" $cluster/services/$serv/config
    else
      curl $uopt --data "$DATA" $cluster/services/$serv/roleConfigGroups/$rname/config
    fi
  done
  echo "---------------------------------------"
  echo ""
}


# Mandatory settings
if [[ $mode == "health" ]]
then
  echo "Skipping mandatory HBase settings"
else
  if [[ $remove == "DEL" ]]
  then
    action="DEL"
  else
    action="match"
  fi
  set_conf $hbase REGIONSERVER "hbase_regionserver_config_safety_valve" ${action} "<property><name>hbase.hregion.impl</name><value>org.apache.hadoop.hbase.regionserver.transactional.TransactionalRegion</value></property><property><name>hbase.regionserver.region.split.policy</name><value>org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy</value></property><property><name>hbase.coprocessor.regionserver.classes</name><value>org.apache.hadoop.hbase.coprocessor.transactional.TrxRegionServerObserver</value></property>"
fi

if [[ $remove == "DEL" || $mode == "man" || $mode == "health" ]]
then
  echo "Skipping recommended settings"
else
  set_conf $hbase REGIONSERVER "hbase_regionserver_lease_period" exact "3600000"
  set_conf $hbase REGIONSERVER "hbase_hregion_memstore_flush_size" exact "268435456"
  set_conf $hbase REGIONSERVER "hbase_hregion_memstore_block_multiplier" exact "7"
  set_conf $hbase REGIONSERVER "hbase_hstore_blockingStoreFiles" exact "200"
  set_conf $hbase MASTER "hbase_master_config_safety_valve" match "<property><name>hbase.snapshot.master.timeoutMillis</name><value>600000</value></property><property><name>hbase.rootdir.perms</name><value>750</value></property>"

  set_conf $hbase Service-Wide "hbase_superuser" match-list "trafodion"

  set_conf $zk SERVER "maxClientCnxns" exact "0"

  # not required if hbase_superuser and hbase.rootdir.perms are set
  #set_conf $hdfs Service-Wide "dfs_namenode_acls_enabled" exact "true"

  if [[ -n $sentry ]]
  then
    set_conf $sentry Service-Wide "sentry_service_admin_group" match-list "trafodion"
    set_conf $sentry Service-Wide "sentry_service_allow_connect" match-list "trafodion"
  else
    echo "Sentry not configured."
  fi
fi


# Work-around CSD Health-Check config bug in CM 5.14+
if [[ -z $esgyndb ]]
then
  echo "EsgynDB not configured. Skipping health-check defaults."
else
  set_conf $esgyndb TRAF_NODE "traf_node_host_health_enabled" ifnull "true"
  set_conf $esgyndb TRAF_DCS "traf_dcs_host_health_enabled" ifnull "true"
  set_conf $esgyndb TRAF_DBM "traf_dbm_host_health_enabled" ifnull "true"
  set_conf $esgyndb TRAF_NODE "traf_node_scm_health_enabled" ifnull "true"
  set_conf $esgyndb TRAF_DCS "traf_dcs_scm_health_enabled" ifnull "true"
  set_conf $esgyndb TRAF_DBM "traf_dbm_scm_health_enabled" ifnull "true"
  set_conf $esgyndb TRAF_NODE "unexpected_exits_window" ifnull "5"
  set_conf $esgyndb TRAF_DCS "unexpected_exits_window" ifnull "5"
  set_conf $esgyndb TRAF_DBM "unexpected_exits_window" ifnull "5"
  set_conf $esgyndb TRAF_NODE "unexpected_exits_thresholds" ifnull '{\"warning\":\"never\",\"critical\":\"any\"}'
  set_conf $esgyndb TRAF_DCS "unexpected_exits_thresholds" ifnull '{\"warning\":\"never\",\"critical\":\"any\"}'
  set_conf $esgyndb TRAF_DBM "unexpected_exits_thresholds" ifnull '{\"warning\":\"never\",\"critical\":\"any\"}'
  set_conf $esgyndb TRAF_NODE "traf_node_fd_thresholds" ifnull '{\"warning\":50,\"critical\":70}'
  set_conf $esgyndb TRAF_DCS "traf_dcs_fd_thresholds" ifnull '{\"warning\":50,\"critical\":70}'
  set_conf $esgyndb TRAF_DBM "traf_dbm_fd_thresholds" ifnull '{\"warning\":50,\"critical\":70}'
  set_conf $esgyndb TRAF_NODE "process_swap_memory_thresholds" ifnull '{\"warning\":\"any\",\"critical\":\"never\"}'
  set_conf $esgyndb TRAF_DCS "process_swap_memory_thresholds" ifnull '{\"warning\":\"any\",\"critical\":\"never\"}'
  set_conf $esgyndb TRAF_DBM "process_swap_memory_thresholds" ifnull '{\"warning\":\"any\",\"critical\":\"never\"}'
  set_conf $esgyndb Service-Wide "QIANBASE_TRAF_DBM_healthy_thresholds" ifnull '{\"critical\":\"50.0\",\"warning\":\"70.0\"}'
  set_conf $esgyndb Service-Wide "QIANBASE_TRAF_DCS_healthy_thresholds" ifnull '{\"critical\":\"50.0\",\"warning\":\"70.0\"}'
  set_conf $esgyndb Service-Wide "QIANBASE_TRAF_NODE_healthy_thresholds" ifnull '{\"critical\":\"75.0\",\"warning\":\"90.0\"}'
fi

rm -f $session

echo
echo "----------------------"
echo "Summary of new values"
echo "----------------------"
if [[ -n $SUM ]]
then
  echo "$SUM"
  echo "Be sure to restart cluster services as needed. No restarts needed for EsgynDB health-check changes."
else
  echo "No changes"
fi
echo ""
exit 0
