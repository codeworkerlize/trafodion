#! /bin/bash
#
####################################
#  History:
#    V 1.2: 120525.  Initial release from a request from Gary.
#    V 1.3  120920   Check that align is being used.
#           130122   Set up scriptName correctly.
#           131126   Quick fix.
####################################

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
    exit 1
fi

. ${scriptDir}/envMEAS.sh

#######################################################
#  Real processing starts now.
#
#  NOTES:   Two modes here.
#  Main mode is the command mode which should be done from and node..
#        The script will then kick itself off on all needed nodes.
#  Slave mode is when the script is kicked off all all nodes.
#        The script will then do the actual work.
#
runMode="m"
nodeList="${measnodelist}"
TESTMODE=""
createOut=""
verbose=""

while getopts 'ovszw:h' parmOpt
do
    case $parmOpt in
    s)  runMode="s";;
    w)  nodeList="-w ${OPTARG}";;
    h)  echo "Syntax: $0 [-w <node>...] [-v] [-o]"
        echo "If -w not specified will use ${nodeList}."
        echo "-v for verbose mode."
        echo "-o to verify for specific collectl file mode (not daemon)."
        exit 0;;
    v)  verbose="-v";;
    z)  TESTMODE="-z";;
    o)  createOut="-o";;
    ?)  echo "Invalid option specified.   Only -w, -v and -o are allowed."
        exit 0;;
    esac
done

#  If in master mode..
if [ "m" = ${runMode} ] ; then
    if [ -z "${TESTMODE}" ] ; then
        if [ -z "${nodeList}" ] ; then
            ${scriptName} -s ${createOut} ${verbose}
        else
            pdsh ${nodeList} -S "${scriptName} -s ${createOut} ${verbose}"
        fi
    else
        echo pdsh ${nodeList} -S "${scriptName} -s ${createOut} ${verbose}"
    fi
    if [ $? -ne 0 ] ; then
        echo "Something failed on a slave node."
        exit 1
    fi
    exit 0
fi

########################################
# We are in slave mode.
# IF we are in daemon mode, we need to verify that everything is properly setup.
# IF we are in output mode, we need to start the collectl.

# Verify that directories exist.
if [ ${#verbose} -gt 0 ] ; then
    echo "Verifying ${localMeasLocation}."
fi
if [ ! -d ${localMeasLocation} ] ; then
    mkdir -p ${localMeasLocation}
    if [ ! -d ${localMeasLocation} ] ; then
        echo "Directory ${localMeasLocation} does not exist and can not be created.  Please verify."
        exit 1
    fi
fi

# Verify if we are allowed to create files here.
if [ -f ${localMeasLocation}/checkMEASFILE.txt ] ; then
    rm -f ${localMeasLocation}/checkMEASFILE.txt
fi
echo "Check" > ${localMeasLocation}/checkMEASFILE.txt
if [ ! -f ${localMeasLocation}/checkMEASFILE.txt ] ; then
    echo "Could not create ${localMeasLocation}/checkMEASFILE.txt, please verify."
    exit 1
fi
rm -f ${localMeasLocation}/checkMEASFILE.txt
if [ -f ${localMeasLocation}/checkMEASFILE.txt ] ; then
    echo "Could not remove ${localMeasLocation}/checkMEASFILE.txt, please verify."
    exit 1
fi

# Verify if we already have a running measurement for this loadid.
if [ ${#verbose} -gt 0 ] ; then
    echo "Verifying if we have current measurement files."
fi
if [ $(ls ${localMeasLocation}/*.CURRENT 2>/dev/null | wc -l) -gt 0 ] ; then
    echo "There are current measurements setup."
    ls ${localMeasLocation}/*.CURRENT  
fi

# Find out where collectl is.
if [ ${#verbose} -gt 0 ] ; then
    echo "Verifying that collectl is present."
fi
COLLPATH=$(type -p collectl)
if [ -z "${COLLPATH}" ] ; then
    echo "Can't find collectl."
    exit 1
fi

if [ ${#createOut} -eq 0 ] ; then
    # We are in daemon mode.
    if [ ${#verbose} -gt 0 ] ; then
        echo "Verifying if collectl daemon is running."
    fi
    COLLECTPID=$(ps -ef | grep collectl | grep -F -e '-D' | awk '{print $2}')
    if [ -z "${COLLECTPID}" ] ; then
        echo "Daemon collectl is not running.   Please have root start the daemon collectls."
        exit 1
    fi

    #Verify subsys and interval.
    if [ ${#verbose} -gt 0 ] ; then
        echo "Verifying that daemon is properly configured."
    fi
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
    
    if [ $(${COLLPATH} -p ${currentFile} --showheader | grep -F "DaemonOpts:" | grep -c -F -e "--align") -eq 0 ] ; then
        echo "Daemon does not have align attribute."
        exit 1
    fi
fi

exit 0
