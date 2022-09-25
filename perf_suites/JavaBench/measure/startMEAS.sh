#! /bin/bash
#
####################################
#  History:
#    V 1.0: 100917.  Initial version.
#    V 1.1: 101126.  Fixed fault-tolerance detection.
#    V 1.2: 110110.  Will now use the collectl -D feature which be running all the time.
#                    If using daemons, will verify if running and configured correctly.
#           110817   If nodelist is empty then don't use pdsh since all local.
#           111121   Seems -i is not passed to command when -i is used on command line.
#           120730   Look for extraMEAS.sh and call it if there.  Used for temp stuff.
#    V 1.3  120920   Length for loadid is now maxLoadIDLength from .envMEAS.sh
#           121012   By default wait for minute.  If -n indicated, start now.
#           121114   Fixed problem with bash taking 08 as octal 8 which is an error.
#           130122   Set up scriptName correctly.  Uses new meascreateOut variable.
#           140403   Now uses bash.  whence to type, etc.
#           140820   MEASEXTRA uses different option.   Gives start msg.  -L for loadid.
####################################
startVersion="1.3.0-140820"

scriptName=$(readlink -f $0)
if [ ! -x ${scriptName} ] ; then
    scriptName=$(type -p $0)
    if [ ${#scriptName} -eq 0 ] ; then
        echo "Can't find $0."
        exit 1
    fi
fi
scriptDir=$(dirname ${scriptName})

if [ ! -f ${scriptDir}/envMEAS.sh ] ; then
    echo "Missing ${scriptDir}/envMEAS.sh which contains needed config info."
    exit 0
fi

. ${scriptDir}/envMEAS.sh

######################################################

if [ -z "$1" ] ; then
    echo "$0 [-o] [-n] [-w n#[,n#...]] [-i <interval[:<inter2>]] [-L] loadid"
    echo "   by default, we will use the collectl daemons to process."
    echo "   use -o to create separate output collectl files."
    echo "   use -n to not delay and start now, by default, waits for top of minute."
    echo "   use -w to start measurement on specified nodes only."
    echo "       n# is not the seaquest node but the cluster nodes."
    echo "       if not specified, \"${measnodelist}\" will be measured."
    echo "   loadid is the loadid to give this measure run.   Maximum ${maxLoadIDLength} chars."
    echo "       Use -L loadid if not at end."
    echo "   use -i to specify an interval for measurements (default ${defaultInterval} sec)."
    echo "       first value is interval in seconds for CPU, DISK, MEMORY, NETWORK."
    echo "       second value is interval for process."
    echo ""
    echo "   Measurements will be in ${localMeasLocation}."
    exit 0
fi

#######################################################
#  Real processing starts now.
#
#  NOTES:   Two modes here.
#  Main mode is the command mode which should be done from head node.
#        The script will then kick itself off on all needed nodes.
#  Slave mode is when the script is kicked off all all nodes.
#        The script will then do the actual work.
#
runMode="m"
nodeList="${measnodelist}"

allops=""
for f in "$@"
do
    allops="${allops} \"$f\""
done
unset f

createOut=${meascreateOut}
startWhen=""
FROMTIME=""
TESTMODE=""

while getopts 'zc:b:f:i:M:E:sonw:L:' parmOpt
do
    case $parmOpt in
    b)  INITTIME="${OPTARG}";;
    c)  COLLPATH="${OPTARG}";;
    i)  MEASINTVL="-i${OPTARG}";;
    E)  MEASOPTEXTRA="${OPTARG}";;
    f)  FROMTIME="${OPTARG}";;
    L)  loadID="${OPTARG}" ;;
    M)  MEASOPT="${OPTARG}";;
    n)  startWhen="-n";;
    s)  runMode="s";;
    o)  createOut="-o";;
    w)  nodeList="-w ${OPTARG}";;
    z)  TESTMODE="-z";;
    ?)  echo "Invalid option specified.   Only -w, -o, -n, -e, -L are allowed."
        exit 0;;
    esac
done

shift $(($OPTIND - 1))
if [ -z "$1" ] && [ ${#loadID} -eq 0 ] ; then
    echo "Loadid parameter is required."
    exit 0
elif [ ${#loadID} -eq 0 ] ; then
    loadID=$1
    shift 1
fi
if [ ${#loadID} -gt ${maxLoadIDLength} ] ; then
    echo "Loadid can only be max of ${maxLoadIDLength} characters."
    exit 0
fi
if [ -n "$1" ] ; then
    echo "Extra parameter $1 found.   Please verify command."
    exit 0
fi

INITTIME=${INITTIME:=$(date +%Y%m%d.%H%M%S)}

#  If in master mode..
if [ "m" = ${runMode} ] ; then
    if [ $(uname -n) != ${masterNode} ] ; then
        #SHIP to master node.
        ssh ${masterNode} "${scriptName} ${allops}"
        exit 0
    fi

    # Verify that directories exist.
    if [ ! -d ${localMeasLocation} ] ; then
        mkdir -p ${localMeasLocation}
        if [ ! -d ${localMeasLocation} ] ; then
            echo "Directory ${localMeasLocation} does not exist and can not be created.  Please verify."
            exit 0
        fi
    fi

    # Verify if we already have a running measurement for this loadid.
    if [ -f ${localMeasLocation}/${loadID}.CURRENT ] ; then
        echo "Already have a running measurement for ${loadID}."
        exit 0
    fi

    if [ -z "${createOut}" ] ; then
       if [ -n "${MEASOPT}${MEASOPTEXTRA}${MEASINTVL}" ] ; then
           echo "Can't use any collectl options, MEASOPT, MEASOPTEXTRA, MEASINTVL or -i with default daemon captures."
           exit 0
       fi
    elif  [ -n "${MEASINTVL}" ] ; then
       # Verify if interval is in too many places.
       if [ -n "${MEASOPT}" ] && [ -n "$(echo ${MEASOPT} | grep -F '-i')" ] ; then
           echo "Interval information in both -i parameter and in MEASOPT."
           exit 0
       fi
       if [ -n "${MEASOPTEXTRA}" ] && [ -n "$(echo ${MEASOPTEXTRA} | grep -F '-i')" ] ; then
           echo "Interval information in both -i parameter and in MEASOPTEXTRA."
           exit 0
       fi
    fi

    if [ -n "${createOut}" ] ; then
       # Find out where collectl is.
       COLLPATH=${COLLPATH:=$(type -p collectl)}
       if [ -z "${COLLPATH}" ] ; then
          echo "Can't find collectl."
          exit 0
       fi
       COLLPATH="-c ${COLLPATH}"
    fi

    # Find sqshell and get information.
    shellSQ=$(type -p sqshell)
    if [ -z "${shellSQ}" ] ; then
       echo "Can't find sqshell."
       exit 0
    fi

    # Get information from sqshell.
    sqshell -a <<-EOT > ${localMeasLocation}/${loadID}.CURRENT
	ps {ASE}
	ps {TSE}
	ps {DTM}
	ps {AMP}
	show
	node
	exit
	EOT

    if [ ! -f ${localMeasLocation}/${loadID}.CURRENT ] ; then
        echo "Could not create ${localMeasLocation}/${loadID}.CURRENT.  Please verify."
        exit 0
    fi
    if [ $(grep "Can't attach" ${localMeasLocation}/${loadID}.CURRENT | wc -l) -gt 0 ] ; then
        echo "SQ environment does not appear to be up on this node.   SQ info will not be captured."
        SQENVUP=false
    elif [ $(grep "not been started" ${localMeasLocation}/${loadID}.CURRENT | wc -l) -gt 0 ] ; then
        echo "SQ environment does not appear to be up.   SQ info will not be captured."
        SQENVUP=false
    else
        SQENVUP=$(grep "TRAF_HOME" ${localMeasLocation}/${loadID}.CURRENT | awk '{print $6}')
        grep " ASE " ${localMeasLocation}/${loadID}.CURRENT | awk '{if (NF > 3) print $4 $5}' > ${localMeasLocation}/${loadID}.CURRENTTMP
        NUMASE=$(($(wc -l ${localMeasLocation}/${loadID}.CURRENTTMP | awk '{print $1}')))
        if [ $(grep -c -m 1 'B' ${localMeasLocation}/${loadID}.CURRENTTMP) -gt 0 ] ; then
            ASEFAULT=1
            NUMASE=$((${NUMASE}/2))
        else
            ASEFAULT=0
        fi

        grep " TSE " ${localMeasLocation}/${loadID}.CURRENT | awk '{if (NF > 3) print $4 $5}' > ${localMeasLocation}/${loadID}.CURRENTTMP
        NUMTSE=$(($(wc -l ${localMeasLocation}/${loadID}.CURRENTTMP | awk '{print $1}')))
        if [ $(grep -c -m 1 'B' ${localMeasLocation}/${loadID}.CURRENTTMP) -gt 0 ] ; then
            TSEFAULT=1
            NUMTSE=$((${NUMTSE}/2))
        else
            TSEFAULT=0
        fi

        grep " DTM " ${localMeasLocation}/${loadID}.CURRENT | awk '{if (NF > 3) print $4 $5}' > ${localMeasLocation}/${loadID}.CURRENTTMP
        NUMDTM=$(($(wc -l ${localMeasLocation}/${loadID}.CURRENTTMP | awk '{print $1}')))
        if [ $(grep -c -m 1 'B' ${localMeasLocation}/${loadID}.CURRENTTMP) -gt 0 ] ; then
            DTMFAULT=1
            NUMDTM=$((${NUMDTM}/2))
        else
            DTMFAULT=0
        fi

        grep " AMP " ${localMeasLocation}/${loadID}.CURRENT | awk '{if (NF > 3) print $4 $5}' > ${localMeasLocation}/${loadID}.CURRENTTMP
        NUMAMP=$(($(wc -l ${localMeasLocation}/${loadID}.CURRENTTMP | awk '{print $1}')))
        if [ $(grep -c -m 1 'B' ${localMeasLocation}/${loadID}.CURRENTTMP) -gt 0 ] ; then
            AMPFAULT=1
            NUMAMP=$((${NUMAMP}/2))
        else
            AMPFAULT=0
        fi
        NUMNODES=$(($(grep " Node" ${localMeasLocation}/${loadID}.CURRENT | awk '{if (NF > 3) print $4}' | wc -l)))
    fi
    rm -f ${localMeasLocation}/${loadID}.CURRENT*

    # We're set.   Execute the script on wanted nodes.
    if [ -n "$MEASOPT" ] ; then
        MEASOPT="-M ""${MEASOPT}"""
    fi
    if [ -n "$MEASOPTEXTRA" ] ; then
        MEASOPTEXTRA="-E ""${MEASOPTEXTRA}"""
    fi
    if [ -z "${TESTMODE}" ] ; then
        echo "${loadID}.${INITTIME}" > ${localMeasLocation}/${loadID}.CURRENT
        echo "${nodeList}" >> ${localMeasLocation}/${loadID}.CURRENT
        echo "${SQENVUP} ${NUMASE} ${ASEFAULT} ${NUMTSE} ${TSEFAULT} ${NUMDTM} ${DTMFAULT} ${NUMAMP} ${AMPFAULT} ${NUMNODES}" >> ${localMeasLocation}/${loadID}.CURRENT
        if [ -z "${createOut}" ] ; then
            # We are in daemon mode.
            echo "DAEMON" >> ${localMeasLocation}/${loadID}.CURRENT
        else
            echo "SPECIFIC" >> ${localMeasLocation}/${loadID}.CURRENT
        fi
        if [ -z "${nodeList}" ] ; then
            ${scriptName} -s ${createOut} -b ${INITTIME} ${COLLPATH} ${MEASOPT} ${MEASOPTEXTRA} ${MEASINTVL} ${loadID}
        else
            pdsh ${nodeList} "${scriptName} -s ${createOut} -b ${INITTIME} ${COLLPATH} ${MEASOPT} ${MEASOPTEXTRA} ${MEASINTVL} ${loadID}"
        fi
    else
        echo pdsh ${nodeList} "${scriptName} -s ${createOut} -b ${INITTIME} ${COLLPATH} ${MEASOPT} ${MEASOPTEXTRA} ${MEASINTVL} ${TESTMODE} ${loadID}"
    fi

    if [ -z "${TESTMODE}" ] ; then
        if [ -z "${FROMTIME}" ] ; then
            if [ -z "${createOut}" ] && [ -z "${startWhen}" ] ; then
                currSecs=$(date +%S)
                if [ ${currSecs:0:1} = 0 ] ; then
                    currSecs=${currSecs:1:1}
                fi
                sleepTime=$((59 - ${currSecs} ))
                echo "Sleeping for ${sleepTime} seconds to ensure minute starting point."
                sleep ${sleepTime}
                unset currSecs
                unset sleepTime
            fi
            FROMTIME=$(date +'%Y%m%d %H%M%S')
        fi
        echo "STARTED ${FROMTIME}" >> ${localMeasLocation}/${loadID}.CURRENT
    fi
    echo Loadid ${loadID} started at ${FROMTIME}
    echo V${startVersion} done at $(date)
    exit 0
fi

########################################
# We are in slave mode.
# IF we are in daemon mode, we need to verify that everything is properly setup.
# IF we are in output mode, we need to start the collectl.

COLLPATH=${COLLPATH:=$(type -p collectl)}
if [ -z "${createOut}" ] ; then
    # We are in daemon mode.
    COLLECTPID=$(ps -ef | grep collectl | grep -F -e '-D' | awk '{print $2}')
    if [ -z "${COLLECTPID}" ] ; then
        echo "Daemon collectl is not running.   Please have root start the daemon collectls."
        exit 1
    fi

    #Verify subsys and interval.
    currentFile=$(ls -t ${measDaemonDir}/*.raw* | head -1)
    if [ -z "${currentFile}" ] ; then
        echo "Can't find current collectl daemon file in ${measDaemonDir}."
        exit 1
    fi
    wantedLine=$(${COLLPATH} -p ${currentFile} --showheader | grep -F "SubSys:")

    currSSys=$(echo "${wantedLine}" | awk '{IGNORECASE=1; for (i=1; i<=NF; i++) if (index($i, "SubSys") != 0) {print $(i+1); break}}')
    for charSSys in ${measDaemonSubs} ; do
        if [ $(echo "${currSSys}" | grep -cF "${charSSys}") -eq 0 ] ; then
            echo "Daemon subsys ${currSSys} does not include subsystem ${charSSys}."
            exit 1
        fi
    done

    currInt=$(echo "${wantedLine}" | awk '{IGNORECASE=1; for (i=1; i<=NF; i++) if (index($i, "Interval") != 0) {print $(i+1); break}}')
    if [ "${currInt}" != "${measDaemonInt}" ] ; then
        echo "Daemon interval or ${currInt} is not ${measDaemonInt}."
        exit 1
    fi

else
    # We are in output mode.
    MEASOPT=${MEASOPT:="${defaultMeas}"}
    if [ -n "${MEASOPTEXTRA}" ] ; then
       MEASOPT="${MEASOPT} ${MEASOPTEXTRA}"
    fi
    if [ -z "$(echo ${MEASOPT} | grep -F ' -i')" ] ; then
       MEASINTVL=${MEASINTVL:=${defaultInterval}}
       MEASOPT="${MEASOPT} ${MEASINTVL}"
    fi
fi

if [ ! -d ${localMeasLocation} ] ; then
   mkdir -p ${localMeasLocation}
   if [ ! -d ${localMeasLocation} ] ; then
       echo "Directory ${localMeasLocation} does not exist and can not be created.  Please verify."
       exit 1
   fi
fi

# If needed, start the collectl and save the info.
COLLECTNAM=${loadID}.${INITTIME}
COLLECTNAMINFO=${COLLECTNAM}-$(uname -n)
rm -f ${localMeasLocation}/${COLLECTNAMINFO}*
if [ -z "${TESTMODE}" ] ; then
    echo "${COLLECTNAM}" > ${localMeasLocation}/${COLLECTNAMINFO}.INFO
    echo $(${COLLPATH} --version | head -1) >> ${localMeasLocation}/${COLLECTNAMINFO}.INFO
    
    if [ -n "${createOut}" ] ; then
        # We are in output mode.   Need to start collectl.
        echo ${COLLPATH} -f ${localMeasLocation}/${COLLECTNAM} ${MEASOPT} >> ${localMeasLocation}/${COLLECTNAMINFO}.INFO
        ${COLLPATH} -f ${localMeasLocation}/${COLLECTNAM} ${MEASOPT} <&- >&- 2>&- &
        COLLECTPID=${!}
        echo "collectl ${COLLECTPID} running."
    else
        echo "DAEMON -f ${measDaemonDir} -s ${currSSys} -i ${currInt}" >> ${localMeasLocation}/${COLLECTNAMINFO}.INFO
        echo "collectl daemon ${COLLECTPID} is already running."
    fi
    echo "${COLLECTPID}" >> ${localMeasLocation}/${COLLECTNAMINFO}.INFO
    echo "$(uname -srvmpio)" >> ${localMeasLocation}/${COLLECTNAMINFO}.INFO
else    
    if [ -n "${createOut}" ] ; then
        # We are in output mode.   Need to start collectl.
        echo ${COLLPATH} -f ${localMeasLocation}/${COLLECTNAM} ${MEASOPT}
    else
        echo "collectl daemon ${COLLECTPID} is already running."
    fi
fi

# Check if an extraMEAS.sh script exists.  If it does call it.
if [ -f ${scriptDir}/extraMEAS.sh ] ; then
    ${scriptDir}/extraMEAS.sh -l ${localMeasLocation}/${COLLECTNAMINFO} start
fi    

exit 0
