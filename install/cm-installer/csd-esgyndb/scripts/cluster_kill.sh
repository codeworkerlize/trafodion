#!/bin/bash

trafuser=$(id -un)
chost="$1"

source ~/.bashrc

cleanZKmonZNode "$chost"

try=0
while (( $try < 5 ))
do
  (( try+=1 ))
  # kill every process of current user, except for this script (pid: $$) and csd.sh that called us ($PPID)
  set -x
  pgrep -u $trafuser | grep -v -e ^$$\$ -e ^$PPID$ | xargs kill -9 
  set +x
  count=$(ps --no-headers -o cmd -u $trafuser | 
          grep -v -e 'ps --' -e cluster_kill -e grep -e 'wc -l' -e bash -e csd.sh | wc -l)
  if (( $count == 0 ))
  then
    break
  else
    echo "Found $count remaining processes. Trying again..."
    sleep 3
  fi
done

echo "Remaining Processes..."
/bin/ps -fHu $trafuser | grep -v -e /bin/ps -e cluster_kill -e grep -e csd.sh

exit $count
