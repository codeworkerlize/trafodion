######################################################
# Set up the following variables for your env.
####################################
#  History:
#    V 1.0: 100917.  Initial version.
#    V 1.1: 101129.  Added masks for real disk names.
#    V 1.2: 110110.  Will now use the collectl -D feature which be running all the time.
#                    If using daemons, will verify if running and configured correctly.
#           110907   Now counts number of nodes, sets  measnumnodes and FANOUT for PDSH.
#    V 1.3: 130122   Set up scriptName correctly.
#                    Added meascreateOut variable which is normally empty.
#                        set to -o to force new collectl file each time. (Workstation)
####################################
#
# localMeasLoc  == Local local of each node to keep collectl files.
# measnodelist  == List of nodes to be used.  Defaults to $MY_NODES.  If empty, assumes workstation.
export localMeasLocation=${JAVABENCH_HOME=}/COLL_DATA/
measnodelist=""
for currNode in ${SYSTEM_NODE_NAMES}
do
    export measnodelist="${measnodelist} -w ${currNode}"
done
export meascreateOut=""           # set to -o to always force new collectl process/file.  IE:  No daemon.

export defaultMeas="-scdnsmZ --align"
export measMainInt=10
export measSecInt=60
export defaultInterval="-i${measMainInt}:${measSecInt}"
export measHelpDefault="   default options are ${defaultMeas}\n   captures CPU, DISK, NET, MEM and processes specified + INFINIBAND is via daemons."
export measnumnodes=$(echo ${measnodelist} | sed 's/-w//g;s/,/ /g' | wc -w)
if ([ -z "${FANOUT}" ] && [ ${measnumnodes} -gt 32 ]) || ([ -n "${FANOUT}" ] && [ 0${FANOUT} -le ${measnumnodes} ]) ; then
    export FANOUT=${measnumnodes}
fi

# To overwrite masterNode, default is first node in list, comment next line and add your own.
export masterNode=$(echo ${measnodelist} | sed 's/-w//g;s/,/ /g' | awk '{print $1}')
if [ ${#masterNode} -eq 0 ] ; then
    export masterNode=$(uname -n)
fi


export numRowPerTrans=100
export maxCmdLength=299
export maxLinuxVsnLength=99
export measMaxErrs=10
export maxLoadIDLength=15
export maxSysNameLength=10
export maxMeasInfoLength=100
export maxSysRootLength=100
export maxCollectlVsnLength=100
export maxMeasCmdLength=300

export diskMultiMask="p[0-9]\$"
export diskLocalMask="p[0-9]\$"

#For the subs line, make sure these are single chars separated by spaces.  eg:  "c d n m s x Z"
export measDaemonSubs="c d n m s x Z"
export measDaemonConf="/etc/collectl.conf"
export measDaemonDir="/var/log/collectl"
export measDaemonInt="${measMainInt}:${measSecInt}"

scriptName=$(readlink -f $0)
if [ ! -x ${scriptName} ] ; then
    scriptName=$(type -p $0)
    if [ ${#scriptName} -eq 0 ] ; then
        echo "Can't find $0."
        exit 1
    fi
fi
scriptDir=$(dirname ${scriptName})
if [ -f ${scriptDir}/.globMEAS.sh ] ; then
    . ${scriptDir}/.globMEAS.sh
fi
if [ ${#measSYSTEMNAME} -eq 0 ] ; then
    export measSYSTEMNAME=${CLUSTERNAME}
fi
