#! /bin/bash
#
####################################
#  History:
#    V 1.0: 100917.  Initial version.
#    V 1.1: 101126.  Added diskmap processing.
#    V 1.2: 110110.  Can now use collectl daemons
#                    If using daemons, will verify that they are still running.
#           110209   On systems with multi users, map disks is more complicated.
#           110817   If nodelist is empty then don't use pdsh since all local.
#           120105   Fix bug when diskmap is not possible.   It was hanging.
#           120330   Now create diskmap using the diskmap routine.
#           120730   Look for extraMEAS.sh and call it if there.  Used for temp stuff.
#    V 1.3  121105   Take stop time now.  Sleep to be quiet.
#           121114   Fixed problem with bash taking 08 as octal 8 which is an error.
#           130122   Set up scriptName correctly.
#           140820   Gives stop msg.  -L for loadid.
####################################
stopVersion="1.3.0-140820"

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
    echo "$0 [-w n#[,n#...]] [-L] loadid"
    echo "   the default is to use what was used with startMEAS."
    echo "   use -w to stop measurement on specified nodes only."
    echo "       n# is not the seaquest node but the cluster nodes."
    echo "   loadid is the loadid used to start the measurement."
    echo "       Use -L loadid if not at end."
    echo ""
    echo "   Looking for measurements in ${localMeasLocation}."
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

allops=""
for f in "$@"
do
    allops="${allops} \"$f\""
done
unset f

TOTIME=""
TESTMODE=""

while getopts 'c:sw:t:zL:' parmOpt
do
    case $parmOpt in
    c)  COLLECTNAM="${OPTARG}";;
    L)  loadID="${OPTARG}" ;;
    s)  runMode="s";;
    t)  TOTIME="${OPTARG}";;
    w)  parmnodeList="-w ${OPTARG}";;
    z)  TESTMODE="-z";;
    ?)  echo "Invalid option specified.   Only -w, -L are allowed."
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
if [ -n "$1" ] ; then
    echo "Extra parameter $1 found.   Please verify command."
    exit 0
fi

#  If in master mode..
if [ "m" = ${runMode} ] ; then
    if [ $(uname -n) != ${masterNode} ] ; then
        #SHIP to master node.
        ssh ${masterNode} "${scriptName} ${allops}"
        exit 0
    fi

    # Verify if we have a running measurement for this loadid.
    CURRENTfile=${localMeasLocation}/${loadID}.CURRENT
    if [ ! -f ${CURRENTfile} ] ; then
        echo "Can't find info for measurement ${loadID}."
        exit 0
    fi

    # Read info from the current file.
    exec 3< ${CURRENTfile}
    foundCount=0
    while read -r -u 3 parm1 && [ ${foundCount} -lt 4 ] ; do
        case ${foundCount} in
        0) COLLECTNAM="$parm1";;
        1) nodeList="$parm1";;
        2) ENVINFO="$parm1";;
        3) isDaemon="$parm1";;
        ?) echo "Error reading ${CURRENTfile}.  Should contain 2 lines."
           exec 3<&-
           exit 0;;
        esac
        let foundCount=${foundCount}+1
    done
    exec 3>&-

    if [ ${foundCount} -lt 4 ] ; then
        echo "${CURRENTfile} should have at least 4 lines.  Found ${foundCount}."
        exit 0
    fi

    if [ -z "${TOTIME}" ] ; then
        ENDTIME=${ENDTIME:=$(date +"%Y%m%d %H%M%S")}
        currSecs=$(date +%S)
        if [ ${currSecs:0:1} = 0 ] ; then
            currSecs=${currSecs:1:1}
        fi
        sleepTime=$((61 - ${currSecs} ))
        if [ ${sleepTime} -gt 59 ] ; then
            sleepTime=$((sleepTime - 60))
        fi
        echo "Sleeping for ${sleepTime} seconds to be quiet to top of minute."
        sleep ${sleepTime}
        unset currSecs
        unset sleepTime
    else
        ENDTIME="${TOTIME}"
    fi

    if [ -n "${parmnodeList}" ] ; then
        if [ "${parmnodeList}" != "${nodeList}" ] ; then
            echo "Nodelist from CURRENT not matching parameter nodelist."
            echo "${nodeList} <> ${parmnodeList}"
            echo -n "Hit C to keep current, P to keep parameter, CTRL-C to stop.  :"
            read -r parm1
            case ${parm1} in
            P) nodeList=${parmnodeList};;
            p) nodeList=${parmnodeList};;
            ?) ;;
            esac
        fi
    fi

    # We're set.   Execute the script on wanted nodes.
    if [ -z "${TESTMODE}" ] ; then
        if [ -z "${nodeList}" ] ; then
            ${scriptName} -s -c ${COLLECTNAM} ${loadID}
        else
            pdsh ${nodeList} "${scriptName} -s -c ${COLLECTNAM} ${loadID}"
        fi
        echo "ENDED ${ENDTIME}" >> ${CURRENTfile}
        mv ${CURRENTfile} ${localMeasLocation}/${COLLECTNAM}.GLOBAL
    else
        echo pdsh ${nodeList} "${scriptName} -s -c ${COLLECTNAM} ${loadID}"
    fi
    echo Loadid ${loadID} stopped at ${ENDTIME}
    echo V${stopVersion} done at $(date)
    exit 0
fi

########################################
# We are in slave mode.
if [ -z "${COLLECTNAM}" ] ; then
    echo "Internal error: Slave mode should have -n parameter."
    exit 1
fi

currentNODE=$(uname -n)
COLLECTNAMINFO=${localMeasLocation}/${COLLECTNAM}-${currentNODE}.INFO
if [ ! -f ${COLLECTNAMINFO} ] ; then
    echo "Can't find ${COLLECTNAMINFO}.INFO in ${localMeasLocation}."
    exit 1
fi

# Check if an extraMEAS.sh script exists.  If it does call it.
if [ -f ${scriptDir}/extraMEAS.sh ] ; then
    ${scriptDir}/extraMEAS.sh -l ${localMeasLocation}/${COLLECTNAM}-${currentNODE} stop
fi    

#Read the info file.
exec 3< ${COLLECTNAMINFO}
foundCount=0
while read -r -u 3 parm1 && [ ${foundCount} -lt 4 ] ; do
    case ${foundCount} in
    0) CHECKNAME="$parm1";;
    1) COLLECTLVSN="$parm1";;
    2) CMDLINE="$parm1";;
    3) COLLECTPID="$parm1";;
    ?) echo "Error reading ${COLLECTNAMINFO}.  Should contain 4 lines."
       exec 3<&-
       exit 1;;
    esac
    let foundCount=${foundCount}+1
done
exec 3>&-

if [ ${foundCount} -lt 4 ] ; then
    echo "${COLLECTNAMINFO} should have at least 4 lines.  Found ${foundCount}."
    exit 1
fi

if [ ${CHECKNAME} != ${COLLECTNAM} ] ; then
    echo "Name in ${COLLECTNAMEINFO} does not match loadid info ${COLLECTNAM}."
    exit 1
fi

#NOTE:   Before V1.2, COLLECTLVSN and CMDLINE were in the other order.
if [ "${CMDLINE:0:8}" = "collectl" ] ; then
    tempVal="${CMDLINE}"
    CMDLINE="${COLLECTLVSN}"
    COLLECTLVSN="${tempVal}"
    unset tempVal
fi

#We may be running with the daemon collectl.
if [ "${CMDLINE:0:6}" = "DAEMON" ] ; then
    createOut=""
else
    createOut="-o"
fi

COLLECTLSTAT="$(ps --no-heading ${COLLECTPID} | grep -iF collectl 2>&-)"
if [ -n "${COLLECTLSTAT}" ] ; then
    if [ -n "${createOut}" ] ; then
        kill ${COLLECTPID}
        echo "Stopping collectl ${COLLECTPID}."
        sleep 1
        if [ -n "$(ps --no-heading ${COLLECTPID})" ] ; then
            kill ${COLLECTPID}
            echo "Stopping collectl ${COLLECTPID}."
            if [ -n "$(ps --no-heading ${COLLECTPID})" ] ; then
                kill ${COLLECTPID}
                echo "Stopping collectl ${COLLECTPID}."
                if [ -n "$(ps --no-heading ${COLLECTPID})" ] ; then
                    echo "Collectl ${COLLECTPID} just won't die."
                fi
            fi
        fi
    fi
else
    echo "Collectl ${COLLECTPID} was not running."
fi

#######################################
# Create disk map.
${scriptDir}/mapdiskMEAS.sh -s -c ${COLLECTNAMINFO}

exit 0
