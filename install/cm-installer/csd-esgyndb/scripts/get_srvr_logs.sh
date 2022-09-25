#!/bin/bash

chost="$1" # current host

source ~/.bashrc

echo "$(date) - Collecting log files from local node"
if [[ $ZIP_HBASELOGS == "YES" ]]
then
  HBASELOGS="-l"
fi
set -x
sqcollectlogs ${HBASELOGS} -z 2>&1  
set +x

exit $ret
