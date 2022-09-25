#!/bin/bash

#
# This script will start/stop or restart HBase.
# It uses Manager's REST api to do this.
#
#   140725 Guy  Stolen from the trafodion-setup suite.
#   140822 Guy  Added --noproxy '*' to curl to bypass proxy if defined.
#   141010 Guy  Modified to allow stop and stop.
#   141016 Guy  Support for Ambari.   -s for status.
#   141121 Guy  Turns hbase balancer off on startup.
#   141126 Guy  Smarter on balancer as we need a balancer on startup before turning it off.
#   141127 Guy  Option to use killCanaries if available and sudo enabled.
#   141208 Guy  Fix log.
#   150108 Guy  Now monitors the master log file for initialization before doing rebalance.
#   150410 Guy  Now uses http interface to query the log instead of ssh to it and compares dates instead of counting msgs.

MNGR_TYPE="${SYSTEM_MGR_TYPE}"             # One of [C]loudera, [A]mbari
MGR_ADMIN="${SYSTEM_MGR_USR}"
MGR_PASSWORD="${SYSTEM_MGR_PASS}"
MGR_URL="${SYSTEM_MGR_URL}"
MGR_PORT=${SYSTEM_MGR_PORT}
MGR_INSTANCE_NAME="${SYSTEM_MGR_CLUSTER}"
MGR_HBASE_SERVICENAME="${SYSTEM_MGR_SERVICE}"
MASTER_NODE="${SYSTEM_NAMEMGR_URL}"
MASTER_PORT="${SYSTEM_NAMEMGR_PORT}"
MASTER_LOG="${SYSTEM_NAMEMGR_LOGNAME}"
poll_time=10
max_polls=12
UpdateHbaseBal=true
HbaseBal=false
CheckKillCanaries=true
master_init_string='Master has completed initialization'

############################################

scriptName=$(readlink -f $0)
if [ ! -x ${scriptName} ] ; then
    scriptName=$(type -p $0)
    if [ ${#scriptName} -eq 0 ] ; then
        echo "Can't find $0."
        exit 1
    fi
fi
scriptDir=$(dirname ${scriptName})

doCmd="stop start"
while getopts 'hdursb:' parmOpt
do
    case $parmOpt in
    b)  if [ X${OPTARG} == Xf ] || [ X${OPTARG} == XF ] ; then
            UpdateHbaseBal=true
            HbaseBal=false
        elif [ X${OPTARG} == Xt ] || [ X${OPTARG} == XT ] ; then
            UpdateHbaseBal=true
            HbaseBal=true
        else
            echo "Invalid -b option specified, only -bt or -bf allowed."
            exit 0
        fi;;
    d)  doCmd="stop" ;;
    h)  echo "Syntax: $0 -h -d -u -r -s -b{t|f} -k"
        echo "where -h gives this help text."
        echo "      -d is to down (stop) hbase."
        echo "      -u is to up (start) hbase."
        echo "      -r is to restart (default) hbase."
        echo "      -s is to get status of hbase."
        echo "      -b set balancer on startup, (t)rue or (f)alse."
        echo "      -k kill hbase canaries if possible (Cloudera only)."
        exit 0;;
    r)  doCmd="stop start" ;;
    s)  doCmd="";;
    u)  doCmd="start" ;;
    ?)  echo "Invalid option specified.   Only -d,-d,-r,-s and -u are allowed."
        exit 0 ;;
    esac
done

if [ $(echo ${MGR_INSTANCE_NAME} | grep -c ' ' ) -gt 0 ] ; then
    MGR_INSTANCE_NAME=$(echo ${MGR_INSTANCE_NAME} | sed 's/ /%20/g')
fi

echo "-  Current state -"
if [ ${MNGR_TYPE} == "A" ] ; then
    cmd_info=$(curl -sS --noproxy '*' -u ${MGR_ADMIN}:${MGR_PASSWORD} -H 'X-Requested-By: Trafodion' -X GET \
           http://${MGR_URL}:${MGR_PORT}/api/v1/clusters/${MGR_INSTANCE_NAME}/services/${MGR_HBASE_SERVICENAME} | \
           grep -m 1 '"state"' | awk '{print $3}' | tr -d '"')
    echo "Hbase = ${cmd_info}"
    if [ X${cmd_info} = "XSTARTED" ] ; then
        for currComp in HBASE_MASTER HBASE_REGIONSERVER
        do
            cmd_info=$(curl -sS --noproxy '*' -u ${MGR_ADMIN}:${MGR_PASSWORD} -H 'X-Requested-By: Trafodion' -X GET \
                 http://${MGR_URL}:${MGR_PORT}/api/v1/clusters/${MGR_INSTANCE_NAME}/services/${MGR_HBASE_SERVICENAME}/components/${currComp} | \
                 grep -e started_count -e total_count | tr -d '\n"')
            echo "Hbase/${currComp} = ${cmd_info}"
            read count_1 count_2 <<< $(echo ${cmd_info} | tr -d ',' | awk '{print $3, $6}')
            if [ X${count_1} != X${count_2} ] ; then
                echo "Hbase/${currComp} **** Total does not equal started."
                if [ $(echo X${doCmd} | grep -c stop) -ne 1 ] ; then
                    echo "You should stop or restart hbase."
                    exit 0
                fi
            fi
        done
    elif [ X${cmd_info} == XINSTALLED ] ; then
        doCmd=$(echo ${doCmd} | tr ' ' '\n' | grep -v stop | tr '\n' ' ')
    fi
else
    cmd_info=$(curl -sS --noproxy '*' -u ${MGR_ADMIN}:${MGR_PASSWORD} -X GET http://${MGR_URL}:${MGR_PORT}/api/v1/clusters/${MGR_INSTANCE_NAME}/services/hbase)
    echo Hbase = $(echo "${cmd_info}" | grep -e '"serviceState"' -e '"healthSummary"' | tr -d '\n"')
    if [ X$(echo "${cmd_info}" | grep -e '"serviceState"' | tr -d '",' | awk '{print $3}') == XSTARTED ] ; then
        for currComp in HBASE_MASTER HBASE_REGION_SERVERS
        do
            spec_health=$(echo "${cmd_info}" | awk -v wantComp=${currComp} '
                  BEGIN {wantStat=0}
                  /"name"/ {if (index($0,wantComp) > 0) {wantStat=1}}
                  /"summary"/ {if (wantStat==1) {print $3; exit}}
                  ' | tr -d '"')
            echo "Hbase/${currComp} = ${spec_health}"
            if [ X${spec_health} != XGOOD ] ; then
                echo "Hbase/${currComp} **** is not good."
                if [ $(echo X${doCmd} | grep -c stop) -ne 1 ] ; then
                    echo "You should stop or restart hbase."
                    exit 0
                fi
            fi
        done
    elif [ X$(echo "${cmd_info}" | grep -e '"serviceState"' | tr -d '",' | awk '{print $3}') == XSTOPPED ] ; then
        doCmd=$(echo ${doCmd} | tr ' ' '\n' | grep -v stop | tr '\n' ' ')
    fi
fi

#### update balancer if asked for.   Turn on balancer before shutting down.
if [ $(echo X${doCmd} | grep -c stop) -eq 1 ] && [ ${UpdateHbaseBal} == true ] ; then
    echo "-  Setting hbase balancer to true."
    echo balance_switch true | hbase shell
fi

for currCmd in ${doCmd}
do
    echo ""
    echo "-  ${currCmd} hbase - $(date)"
    
    cmdDate=$(date +"%Y%m%d%H%M%S")
    
    if [ ${MNGR_TYPE} == "A" ] ; then
        case ${currCmd} in
        stop)  wantState="INSTALLED";;
        start) wantState="STARTED";;
        esac
        
        cmd_info=$(curl -sS --noproxy '*' -u ${MGR_ADMIN}:${MGR_PASSWORD} -H 'X-Requested-By: Trafodion' -X PUT \
            -d "{\"RequestInfo\": {\"context\" :\"Trafodion ${currCmd} HBASE\"}, \"Body\": {\"ServiceInfo\": {\"state\": \"${wantState}\"}}}" \
            http://${MGR_URL}:${MGR_PORT}/api/v1/clusters/${MGR_INSTANCE_NAME}/services/${MGR_HBASE_SERVICENAME})

        request_id=$(echo "${cmd_info}" | grep '"id"' | tr -d ',' | awk '{print $3}')
        request_st=$(echo "${cmd_info}" | grep '"status"' | tr -d '"' | awk '{print $3}')
        
        if [ X${request_st} != XInProgress ] && [ X${request_st} != XCOMPLETED ] ; then
            echo "${cmd_info}"
            echo "Error during ${currCmd} hbase"
            exit 1
        fi
    else
        cmd_info=$(curl -sS --noproxy '*' -X POST -u ${MGR_ADMIN}:${MGR_PASSWORD} \
            http://${MGR_URL}:${MGR_PORT}/api/v1/clusters/${MGR_INSTANCE_NAME}/services/${MGR_HBASE_SERVICENAME}/commands/${currCmd})
        request_id=$(echo "${cmd_info}" | grep '"id"' | awk '{print $3}' | tr -d ',')
    fi

    # poll until cmd is completed as it can take a while
    echo "-   polling every ${poll_time} seconds until ${currCmd} is completed."

    active=1
    while [ ${active} -ne 0 ]; do
        if [ ${MNGR_TYPE} == "A" ] ; then
            cmd_info=$(curl -sS --noproxy '*' -u ${MGR_ADMIN}:${MGR_PASSWORD} -H 'X-Requested-By: Trafodion' -X GET \
                http://${MGR_URL}:${MGR_PORT}/api/v1/clusters/${MGR_INSTANCE_NAME}/requests/${request_id})
            echo $(date +'%H:%M:%S') - $(echo "${cmd_info}" | grep -e '"progress_percent"' -e '"request_status"' | tr -d '\n')
            active=$(($(echo "${cmd_info}" | grep '"request_status"' | grep -c COMPLETED) - 1))
        else
            cmd_info=$(curl -sS --noproxy '*' -u ${MGR_ADMIN}:${MGR_PASSWORD} \
                http://${MGR_URL}:${MGR_PORT}/api/v1/commands/${request_id})
            echo $(date +'%H:%M:%S') - $(echo "${cmd_info}" | grep -m 1 '"active"')
            active=$(echo "${cmd_info}" | grep -m 1 '"active"' | grep -c true)
        fi
        if [ ${active} -ne 0 ] ; then
            sleep ${poll_time}
        fi
    done
    
    # make sure cmd completed successfully
    if [ ${MNGR_TYPE} == "A" ] ; then
        failures=$(echo "${cmd_info}" | grep '"failed_task_count"' | awk '{print $3}' | tr -d ',')
    else
        failures=$(echo "${cmd_info}" | grep -m 1 '"success"' | grep -c false)
    fi
    if [ ${failures} -ne 0 ]; then
        echo "***ERROR: Unable to ${currCmd} HBase."
        echo "${cmd_info}"
        exit -1
    fi

    echo "-  ${currCmd} hbase completed successfully"
done

#### update balancer if asked for.
if [ $(echo X${doCmd} | grep -c start) -eq 1 ] && [ ${UpdateHbaseBal} == true ] ; then
    if [ $(echo X${HbaseBal} | grep -ci false) -eq 1 ] ; then
        active=1
        poll_count=0
        while [ ${active} -ne 0 ]; do
            echo "-  $(date) - Waiting for hbase master to be initialized (max 2 mins)."
            checkDate=$(curl -sS --noproxy '*' -u ${MGR_ADMIN}:${MGR_PASSWORD} -X GET http://${MASTER_NODE}:${MASTER_PORT}/logs/${MASTER_LOG} | \
                grep -e "${master_init_string}" | tail -1 | tr -d '\- :' | awk -F, '{print $1}')
            if [ ${#checkDate} -gt 0 ] && [ ${checkDate} -gt ${cmdDate} ] ; then
                active=0
            else
                poll_count=$((poll_count + 1))
                if [ ${poll_count} -ge ${max_polls} ] ; then
                    active=0
                fi
                sleep ${poll_time}
            fi
        done
        
        echo "-  Ensuring hbase is balanced."
        hbase shell <<-EOT
			balance_switch true
			balancer
		EOT
		sleep 5
	fi
	echo "-  Setting hbase balancer to ${HbaseBal}"
    echo balance_switch ${HbaseBal} | hbase shell
fi

#### If wanted check if we can run the killcanaries scripts.
if [ ${MNGR_TYPE} == "C" ] && [ ${CheckKillCanaries} == true ] ; then
    killCanaryScript=${scriptDir}/killAllHbaseCanary.sh
    if [ -f ${killCanaryScript} ] && [ $(sudo -l | grep -wc "${killCanaryScript}") -gt 0 ] && [ ${#MY_NODES} -gt 0 ] ; then
        echo "-  Getting rids of hbase canaries.  New ones will be started if needed."
        sudo MY_NODES="${MY_NODES}" ${killCanaryScript}
    fi
fi
