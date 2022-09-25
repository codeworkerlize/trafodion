#! /bin/bash
#
####################################
#  History:
#   V 1.3.2 140820.  Initial version taken from loadMEAS.sh.
#   V 1.3.3 140914.  Fixed issue where master node was added twice in the total.
#                    Removed adding of extra time for endtime.
####################################
loadVersion="1.3.2-140821"

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
    echo "$0 [-f \"<from>\"] [-t \"<to>\"] [-w n#[,n#...]]"
    echo "   [-l] [-m] [-i \"info\"] [-q numqueries] [-L] loadid"
    echo "   the default is to use what was used with startMEAS."
    echo "   use -w to load measurement from specified nodes only."
    echo "   use -i to include information for the measurement."
    echo "   use -q to specify a default number of queries for this loadid. (def = 1)"
    echo "   use -m to get the quick summary data in multi-line format."
    echo "   use -l to select the latest measurement when there are multiples."
    echo "       will also use the info from the last load."
    echo "   loadid is the loadid used to start the measurement.  Use -L if not at end."
    echo ""
    echo "Looking for measurements in ${localMeasLocation}."
    echo "   If the measurement does not currently exists, you can specify -f and -t"
    echo "      to create a new one from the daemon files."
    echo "   Specify <from> and <to> to specify times for new measurement."
    echo "   Format \"yyyymmdd hhmmss\""
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
echo Starting at $(date +"%H:%M:%S")
SKIPCOLLECTL=${SKIPCOLLECTL:-0}
TESTMODE=""
WANTLAST=""
runMode="m"

allops=""
for f in "$@"
do
    if [ $(echo $f | grep -c " ") -gt 0 ] ; then
        allops="${allops} \"$f\""
    else
        allops="${allops} $f"
    fi
done
unset f

NUMQUERIES=1
parmnodeList=""
FROMTIME=""
TOTIME=""

while getopts 'c:d:f:i:lL:mn:q:st:w:zZ' parmOpt
do
    case $parmOpt in
    c)  COLLPATH="${OPTARG}";;
    d)  DATETIMES="${OPTARG}" ;;
    f)  FROMTIME="${OPTARG}";;
    i)  MEASINFO="${OPTARG}" ;;
    l)  WANTLAST=1;;
    L)  loadID="${OPTARG}" ;;
    m)  wantCol="-v wantSumCol=1";;
    n)  COLLECTNAM="${OPTARG}" ;;
    q)  NUMQUERIES="${OPTARG}" ;;
    s)  runMode="s" ;;
    t)  TOTIME="${OPTARG}";;
    w)  parmnodeList="-w ${OPTARG}" ;;
    z)  TESTMODE="${TESTMODE}-z";;
    Z)  SKIPCOLLECTL=1;;
    ?)  echo "Invalid option specified.   Only -m,-w,-i,-l,-L,-q,-f and -t are allowed."
        exit 0 ;;
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

currentNODE=$(uname -n)
shortNODEName=$(echo ${currentNODE} | awk -F. '{print $1}')

#  If in master mode..
if [ "m" = ${runMode} ] ; then
    if [ ${currentNODE} != ${masterNode} ] ; then
        #SHIP to master node.
        ssh ${masterNode} "${scriptName} ${allops}"
        exit 0
    fi

    # Verify if we have global info for this loadid.
    # NOTE:  There may be more than 1, if so, prompt for which one to use.
    GLOBALfile=""
    if [ -d ${localMeasLocation} ] ; then
        currentPWD=${PWD}
        cd ${localMeasLocation}
        GLOBALfile=$(echo ${loadID}.*.GLOBAL)
        cd ${currentPWD}
        unset currentPWD
    fi

    if [ $(echo "${GLOBALfile}" | grep -c -F '*') -gt 0 ] ; then
        if [ -n "${FROMTIME}" ] && [ -n "${TOTIME}" ] ; then
            if [ ${#loadID} -gt ${maxLoadIDLength} ] ; then
                echo "Loadid can only be max of ${maxLoadIDLength} characters."
                exit 0
            fi
            echo "Can't find GLOBAL for measurement ${loadID}.  Will create a new one."
            ${scriptDir}/startMEAS.sh -f "${FROMTIME}" ${parmnodeList} ${loadID}
            currentPWD=${PWD}
            cd ${localMeasLocation}
            GLOBALfile=$(echo ${loadID}.*.GLOBAL)
            cd ${currentPWD}
            unset currentPWD
            ${scriptDir}/stopMEAS.sh -t "${TOTIME}" ${parmnodeList} ${loadID}
        else
            echo "Can't find GLOBAL for measurement ${loadID}."
            exit 0
        fi
    fi

    NUMGlobs=$(echo "${GLOBALfile}" | awk '{print NF}')
    if [ ${NUMGlobs} -gt 1 ] ; then
        if [ ${#WANTLAST} -eq 0 ] ; then
            GLOBEntry=0
            while [ ${GLOBEntry} -le 0 ] || [ ${GLOBEntry} -gt ${NUMGlobs} ] ; do
                echo "${GLOBALfile}" | sed "s/ /\n/g" | awk '{print NR,"-", $1}'
                echo ""
                echo -n "Enter Wanted Entry.  CTRL-C to break out. :"
                read -r GLOBEntry
                if [ -z "$(echo ""${GLOBEntry}"" | grep ""^[0-9]*\$"")" ] ; then
                    GLOBEntry=0
                fi
            done
        else
            GLOBEntry=${NUMGlobs}
        fi

        GLOBALfile=$(echo ${GLOBALfile} | awk "{print \$${GLOBEntry}}")
        unset GLOBEntry
    fi
    unset NUMGlobs

    # Read info from the current file.
    foundCount=0
    while read -r parm1 && [ ${foundCount} -lt 6 ] ; do
        case ${foundCount} in
        0) COLLECTNAM="$parm1"
           INITTIME="$(echo $parm1 | awk -F. '{print $(NF-1) $NF}')"
           foundCount=$((foundCount + 1));;
        1) nodeList="$parm1"
           foundCount=$((foundCount + 1));;
        2) ENVINFO="$parm1"
           foundCount=$((foundCount + 1));;
        3) isDaemon="$parm1"
           #If this is from older version, then no DAEMON or SPECIFIC line, so parse for BEGTIME right away.
           if [[ "${isDaemon}" =~ "STARTED [ 0-9]*" ]] ; then
               foundCount=$((foundCount + 1))
               BEGTIME="$(echo $parm1 | awk '{print $2 $3}')"
               isDaemon=0      # Assuming specific file since older version was always specific.
           elif [ "${isDaemon}" = "DAEMON" ] ; then
               isDaemon=1
           else
               isDaemon=0
           fi
           foundCount=$((foundCount + 1));;
        4) BEGTIME="$(echo $parm1 | awk '{print $2 $3}')"
           foundCount=$((foundCount + 1));;
        5) ENDTIME="$(echo $parm1 | awk '{print $2 $3}')"
           foundCount=$((foundCount + 1));;
        ?) echo "Error reading ${GLOBALfile}.  Should contain at least 5 lines."
           exit 0;;
        esac
    done < ${localMeasLocation}/${GLOBALfile}

    if [ ${foundCount} -lt 5 ] ; then
        echo "${GLOBALfile} should have at least 5 lines.  Found ${foundCount}."
        exit 0
    fi

    if [ ${#parmnodeList} -gt 0 ] ; then
        if [ "${parmnodeList}" != "${nodeList}" ] ; then
            parm1=""
            while [ $(echo X${parm1}X | grep -c 'X[cCPp]X') -eq 0 ] ; do
                echo "Nodelist from GLOBAL not matching parameter nodelist."
                echo "${nodeList} <> ${parmnodeList}"
                echo -n "Hit C to keep current, P to keep parameter, CTRL-C to stop."
                read -r parm1
                case ${parm1} in
                P|p) nodeList=${parmnodeList};;
                esac
            done
        fi
    fi

    # We're set.   Execute the script on wanted nodes.
    COLLPATH=${COLLPATH:=$(type -p collectl)}
    if [ ${#TESTMODE} -eq 0 ] ; then
        if [ ${#nodeList} -eq 0 ] ; then
            ${scriptName} -c ${COLLPATH} -d "${INITTIME} ${BEGTIME} ${ENDTIME}" -s -n ${COLLECTNAM} ${TESTMODE} ${loadID}
        else
            pdsh -S ${nodeList} "${scriptName} -c ${COLLPATH} -d \"${INITTIME} ${BEGTIME} ${ENDTIME}\" -s -n ${COLLECTNAM} ${TESTMODE} ${loadID}"
        fi
    else
        echo pdsh ${nodeList} "${scriptName} -c ${COLLPATH} -d \"${INITTIME} ${BEGTIME} ${ENDTIME}\" -s -n ${COLLECTNAM} ${TESTMODE} ${loadID}"
    fi
    if [ $? -ne 0 ] ; then
        echo ""
        echo "One of the slave nodes did not terminate correctly.  Please review above output."
        echo "You may have to cleanup and reload."
        echo ""
    fi

    # We now need to process the output.
    echo Processing global information at $(date +"%H:%M:%S")

    #get the data from the remotes and cleanup.   The process.
    cd ${localMeasLocation}
    COLLECTNAMINFO=${localMeasLocation}/${COLLECTNAM}
    if [ ${#TESTMODE} -eq 0 ] ; then
        if [ ${#nodeList} -gt 0 ] ; then
            rpdcp ${nodeList} "${COLLECTNAMINFO}-*.quick" ${localMeasLocation}
            pdsh ${nodeList} "rm -f ${COLLECTNAMINFO}-*.quick"      # This leaves the copied files on the master node only.
        fi
    else
        echo rpdcp ${nodeList} "${COLLECTNAMINFO}-*.quick" ${localMeasLocation}
        echo pdsh ${nodeList} \"rm -f ${COLLECTNAMINFO}-\*.quick\"
    fi

    echo ""
    echo "Total info for ${loadID}   ${MEASINFO}"
    if [ ${#TESTMODE} -eq 0 ] ; then
        cat ${COLLECTNAMINFO}-*.quick* | awk ${wantCol} -v wantSum=1 -v numQueries=${NUMQUERIES} -f ${scriptDir}/quickMeas.awk
    else
        echo "cat ${COLLECTNAMINFO}-*.quick* | awk ${wantCol} -v wantSum=1 -w numQueries=${NUMQUERIES} -f ${scriptDir}/quickMeas.awk"
    fi
    
    if [ ${#TESTMODE} -eq 0 ] ; then
        if [ ${#nodeList} -eq 0 ] ; then
            rm -f ${COLLECTNAMINFO}-*.quick
        else
            pdsh ${nodeList} "rm -f ${COLLECTNAMINFO}-*.quick*"
        fi
    else
        echo pdsh ${nodeList} \"rm -f ${COLLECTNAMINFO}-\*.quick\*\"
    fi

    echo V${loadVersion} done on $(date)
    
    exit 0
fi

########################################
# We are in slave mode.
if [ ${#measISTRAF} -eq 0 ] ; then
    #Assuming not a trafodion system.
    measISTRAF=0
fi

COLLPATH=${COLLPATH:=$(type -p collectl)}
echo Slave mode at $(date +"%H:%M:%S")

# Verify if a zlib directory exists.   If it does, tell perl to use it.
if [ -d ${scriptDir}/zlib ] ; then
    if [ ${#PERL5LIB} -gt 0 ] ; then
        saveLib=":${PERL5LIB}"
    else
        saveLib=""
    fi
    export PERL5LIB=${scriptDir}/zlib/lib/:${scriptDir}/zlib/arch/${saveLib}
fi

if [ ${#COLLECTNAM} -eq 0 ] || [ ${#DATETIMES} -eq 0 ] ; then
    echo "Internal error: Slave mode should have -n and -d parameter."
    exit 1
fi

cd ${localMeasLocation}
COLLECTNAMINFO=${COLLECTNAM}-${currentNODE}
if [ ! -f ${COLLECTNAMINFO}.INFO ] ; then
    echo "Can't find ${COLLECTNAMINFO}.INFO in ${localMeasLocation}."
    exit 1
fi

read -r INITTIME BEGTIME ENDTIME <<< $(echo "${DATETIMES}")

#Read the info file.
foundCount=0
while read -r parm1 && [ ${foundCount} -lt 5 ] ; do
    case ${foundCount} in
    0) CHECKNAME="$parm1";;
    1) COLLECTLVSN="$parm1";;
    2) CMDLINE="$parm1";;
    3) COLLECTPID="$parm1";;
    4) VERSIONINFO="$parm1";;
    ?) echo "Error reading ${COLLECTNAMINFO}.INFO.  Should contain at least 4 lines."
       exit 1;;
    esac
    let foundCount=${foundCount}+1
done < ${COLLECTNAMINFO}.INFO

if [ ${foundCount} -lt 5 ] ; then
    echo "${COLLECTNAMINFO}.INFO should have at least 5 lines.  Found ${foundCount}."
    exit 1
fi

if [ ${CHECKNAME} != ${COLLECTNAM} ] ; then
    echo "Name in ${COLLECTNAMINFO} does not match loadid info ${COLLECTNAM}."
    exit 1
fi

#NOTE:   Before V1.2, COLLECTLVSN and CMDLINE were in reverse order.
if [ "${CMDLINE:0:8}" = "collectl" ] ; then
    tempVal="${CMDLINE}"
    CMDLINE="${COLLECTLVSN}"
    COLLECTLVSN="${tempVal}"
    unset tempVal
fi

# This is the main processing.
#Check if we are using the daemon collectl.
if [ "${CMDLINE:0:6}" = "DAEMON" ] ; then
    echo Extracting collectl files at $(date +"%H:%M:%S").

    isDaemon=1
    
    endSecs=$(date --utc --date "${ENDTIME:0:8} ${ENDTIME:8:2}:${ENDTIME:10:2}:${ENDTIME:12:2}" +%s)
    endSecs=$((endSecs + 8))
    
    fromToRange="--from ${BEGTIME:0:8}:${BEGTIME:8:2}:${BEGTIME:10:2}:${BEGTIME:12:2}-"$(date --utc --date "1970-01-01 +${endSecs} seconds" +'%Y%m%d:%H:%M:%S')
    fromComp="${BEGTIME:0:8}-${BEGTIME:8:6}"
    toComp="${currentNODE}-${ENDTIME:0:8}-${ENDTIME:8:6}"

    COLLECTRAWNAME=${CHECKNAME}-${shortNODEName}-${fromComp}.raw

    if [ ! -f ${COLLECTRAWNAME} ] ; then
        COLLECTRAWNAME=""
    
        if [ -n "$(ls ${CHECKNAME}-${shortNODEName}-*.raw* 2>/dev/null)" ] ; then
            rm -f ${CHECKNAME}-${shortNODEName}-*.raw*
        fi

        for currFile in ${measDaemonDir}/*.raw* ; do
            modifDate="$(date -r ${currFile} +%Y%m%d-%H%M%S)"
            if [ ! "${modifDate}" '<' "${fromComp}" ] ; then
                creatDate="$(echo ${currFile} | awk -F/ '{print $NF}' | awk -F. '{print $1}')"
                if [ ! "${creatDate}" '>' "${toComp}" ] ; then
                    ${COLLPATH} -p "${currFile}" ${fromToRange} --extract ${loadID}
                fi
            fi
        done
        if [ -n "$(ls ${loadID}*.raw.gz 2>/dev/null)" ] ; then
            gunzip ${loadID}*.raw.gz
        fi
        if [ $(ls ${loadID}*.raw 2>/dev/null | wc -l) -gt 1 ] ; then
            echo Concatenating collectl files at $(date +"%H:%M:%S").
            lastTime=""
            for currFile in ${loadID}*.raw ; do
                if [ ${#lastTime} -eq 0 ] ; then
                    lastTime="$(tail -1 ${currFile})"
                    COLLECTRAWNAME=${currFile}
                else
                    awk -v wantVal="${lastTime}" 'BEGIN {wantPrint=0}
                                                    {if (wantPrint == 1) {print $0; next}
                                                     if ($0 == wantVal) {wantPrint=1; next}
                                                     if ($1 == ">>>") {print "ERR:  Collectl files are not contiguous." > "/dev/stderr"}
                                                     next
                                                    }' ${currFile} >> ${COLLECTRAWNAME}
                    lastTime="$(tail -1 ${currFile})"
                    rm -f ${currFile}
                fi
            done
            unset lastTime
        fi
        if [ $(ls ${loadID}*.raw 2>/dev/null | wc -l) -ne 1 ] ; then
            echo "Can't find any ${measDaemonDir}/*.raw* daemon collectl files for range ${fromToRange}."
            exit 1
        fi
        COLLECTRAWNAME=${CHECKNAME}-${shortNODEName}-${fromComp}.raw
        mv ${loadID}*.raw ${COLLECTRAWNAME}
    fi

    unset fromComp
    unset toComp
    unset modifDate
    unset creatDate
    unset endSecs
    unset toSecs
    unset currFile
else
    isDaemon=0
    COLLECTRAWNAME=$(echo ${COLLECTNAM}-${shortNODEName}-*.raw*)
    if [ ! -f ${COLLECTRAWNAME} ] ; then
        echo "Can't find ${COLLECTRAWNAME} in ${localMeasLocation}."
        exit 1
    fi
    fromToRange=""
fi

# Get the quick info.
# Will have collectl feed AWK which will process it and dump into a file for sqlci.
echo Processing QUICK Info at $(date +"%H:%M:%S")
rm -f ${COLLECTNAMINFO}.quick
if [ ${#TESTMODE} -gt 0 ] ; then
    echo ${COLLPATH} -p "${COLLECTRAWNAME}" ${fromToRange} -on2 -w -P -scdn | tee -a ${COLLECTNAMINFO}.quick
fi
if [ ${SKIPCOLLECTL} -eq 0 ] ; then
    ${COLLPATH} -p "${COLLECTRAWNAME}" ${fromToRange} -on2 -w -P -scdn 2>&- | \
        awk -f ${scriptDir}/quickMeas.awk >> ${COLLECTNAMINFO}.quick
    if [ $? -ne 0 ] ; then
       exit $?
    fi
fi

if [ ${isDaemon} -eq 1 ] ; then
    echo Removing temp collectl file at $(date +"%H:%M:%S")
    rm -f ${COLLECTRAWNAME}
fi

echo Done at $(date +"%H:%M:%S")
exit 0
