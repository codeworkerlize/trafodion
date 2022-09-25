#!/bin/bash

echo "control script: $0"

echo "$(date) - Shutting Down"

# set up env
source ~/.bashrc

echo "TRAF_HOME: $TRAF_HOME"

########################################################
echo "Stopping CMON and NMON"
cmonstop
nmonstop

export EDB_PDSH_FANOUT=1

if  [[ -n $esgyndb_principal ]]
then
   echo "$(date) - Running krb5service stop"
   edb_pdsh -a $TRAF_HOME/sql/scripts/krb5service stop
   edb_pdsh -a pkill -9 -u $(id -u) -f krb5check
fi

echo
echo "$(date) - Stopping DB Manager and manageability processes..."
edb_pdsh -a "mgblty_stop -m" 
edb_pdsh -a "pkill -9 -u $(id -u) -f bosun"

echo
echo "$(date) - Stopping REST"
reststop 

echo
echo "$(date) - Stopping Connectivity Servers"
${DCS_INSTALL_DIR}/bin/dcs-daemons.sh stop server


echo
echo "$(date) - Stopping StrawscanDispatchers"
littlejettystop

echo
echo "$(date) - Stopping Trafodion node role processes"
rcsstop
sqshell -c shutdown

echo
echo "$(date) - Waiting for the Trafodion monitor process to exit ..."

# Iterations 30, delay 5 seconds = 2.5 minutes
if sqcheckmon -s down -i 30 -d 5
then
  echo "$(date) - Trafodion node roles stopped!"
else
  echo "$(date) - Trafodion node roles did not stop!"
fi
