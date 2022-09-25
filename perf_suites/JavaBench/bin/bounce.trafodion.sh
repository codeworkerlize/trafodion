
echo "
===== Bounce Trafodion Environment ===== [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

pdsh -w $SYSTEM_FIRST_NODE sqstop
# do an ckillall just to make sure everything is stopped.
pdsh -w $SYSTEM_FIRST_NODE '$TRAF_HOME/sql/scripts/ckillall'

echo "
Trafodion stopped [ $(date +"%Y-%m-%d %H:%M:%S") ]
      Process Counts :
"
pdsh -w $SYSTEM_FIRST_NODE sq_check_myuserid -c

echo ""
pdsh -w $SYSTEM_FIRST_NODE sqstart
echo "
Trafodion started [ $(date +"%Y-%m-%d %H:%M:%S") ]
Sleeping for 45 seconds to ensure all processes are running
"
time sleep 45s

echo "
      Process Counts :
"
pdsh -w $SYSTEM_FIRST_NODE sq_check_myuserid -c

echo ""
