#!/bin/bash

node_name="$1"

source ~/.bashrc

set -x
nid=$(trafconf --mynid | cut -d: -f2)
sqshell -c kill {abort} \$NMON"${nid## }"
sqshell -c kill {abort} \$RCSVR"${nid## }"

sqshell -c node down "$node_name"

exit 0  # shutdown may kill the sqshell, so report success despite exit code
