#!/bin/bash

chost="$1" # current host

source ~/.bashrc


echo "$(date) - Stopping Connectivity Server for local node"
instance=$( grep -n $chost $TRAF_CONF/dcs/servers | cut -d: -f1)
count=$( grep -n $chost $TRAF_CONF/dcs/servers | cut -d' ' -f2)
set -x
${DCS_INSTALL_DIR}/bin/dcs-daemon.sh stop server $instance
set +x

echo "$(date) - Killing mxosrvr processes"
trafuser=$(id -un)
set -x
pkill -9 -u $trafuser mxosrvr
set +x

echo "$(date) - Starting Connectivity Server for local node"
set -x
${DCS_INSTALL_DIR}/bin/dcs-daemon.sh start server $instance $count
ret=$?
set +x

exit $ret
