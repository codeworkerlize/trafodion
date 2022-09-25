#!/bin/bash
# @@@ START COPYRIGHT @@@ 
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# @@@ END COPYRIGHT @@@

logOptions=" -z "
if [[ -z $NODES ]]; then
   NODES=`trafconf -name`
fi
if [[ -z $LOGS_TARDIR ]]; then
   LOGS_TARDIR=/tmp
fi

hbaselog=false
pythonInstall=true

# Script for collecting log files from all the nodes
function usage() {
    prog=`basename $0`
    echo
    echo " This script gathers log files for all components"
    echo ""
    echo "Usage: ./$prog [--hbaselogs]"
    echo "   where --hbaselogs ---> if specified, collects hbase log files" 
    echo
    echo " To collect logs from specfic node or set of nodes "
    echo "   Please set the environment variable NODES"
    echo "     eg: export NODES=\"node1 node2\" "
    echo
}

#Main
#This script captures esgynDB log file and packages them

if [[ $# -gt "1" ]]; then
   echo "***ERROR: Unknown parameter" 
   usage
   exit 1
fi

if [[ -z $NODES ]]; then
  echo "*** Found no nodes via trafconf. Run sqgen..." 
  usage
  exit 1
fi

while [[ $# -gt 0 ]]; do
    case "$1" in
        --hbaselogs)
              logOptions=" -l -z"
              hbaselog=true
              break
            ;;
        --h)
            usage
            exit 1
            ;;
        *)
            if [ "$1" != "--hbaselogs" ] && [ "$1" != "--h" ]; then
                echo "***ERROR: Unknown parameter"
                usage
                exit 1
            fi
    esac
    shift
done

    echo
    echo "*** Nodes used : $NODES" 

    if [[ -n $TRAF_AGENT ]]; then
       pythonInstall=false
    fi

    echo -n "*** Proceed with collecting log files (Y/N), default[Y]: "
    read answer
    if [[ -z ${answer} || "${answer}" =~ ^[Yy]$ ]]; then
       echo "*** Proceeding......." 
    else
       usage
       exit 1
    fi

    lv_currdate=`date +%Y%m%d`
    for node in $NODES
    do
       echo
       echo "*** Collecting log files from node: $node...."
       edb_pdsh -w $node "source $TRAF_HOME/tools/sqtools.sh; export LOGS_TARDIR=$LOGS_TARDIR; sqcollectlogs $logOptions" &
    done
    echo    
    wait
    if [[ "$hbaselog" == "true" ]]; then
      filesToscp="$LOGS_TARDIR/esgyndblogs/*$lv_currdate*.tgz $LOGS_TARDIR/esgyndblogs/hbaselogs*.tgz"
    else
      filesToscp="$LOGS_TARDIR/esgyndblogs/*$lv_currdate*.tgz"
    fi

    if [[ "$pythonInstall" == "true" ]]; then
       download_dir=$LOGS_TARDIR/esgyndbDownloads/$lv_currdate
       mkdir -p $download_dir
       cd $download_dir
       echo "*** Copying files to primary node ..."
       for node in $NODES
       do
          scp $USER@$node:"$filesToscp" .
       done
       cd $LOGS_TARDIR/esgyndbDownloads
       tar -cvzf esgyndblogs-$lv_currdate.tgz $lv_currdate 
       retcode=$?
       if [ "$retcode" == "0" ]; then    
          echo
          echo "***** Please upload $LOGS_TARDIR/esgyndbDownloads/esgyndblogs-$lv_currdate.tgz to esgyn ftp site"
          rm -rf $LOGS_TARDIR/esgyndbDownloads/$lv_currdate
       fi
    fi
    echo
