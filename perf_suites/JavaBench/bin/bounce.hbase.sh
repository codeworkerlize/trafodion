
echo "
===== Bounce Hbase Environment ===== [ $(date +"%Y-%m-%d %H:%M:%S") ]
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

echo "
Bouncing Hbase [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
pdsh -w $SYSTEM_FIRST_NODE "cd ${JAVABENCH_TEST_HOME};. profile; restartHbase.sh"

echo "
Hbase restarted [ $(date +"%Y-%m-%d %H:%M:%S") ]
Sleeping for 10 seconds to ensure everything started
"
time sleep 10s

pdsh -w $SYSTEM_FIRST_NODE sqstart
echo "
Trafodion started [ $(date +"%Y-%m-%d %H:%M:%S") ]
Sleeping for 45 seconds to ensure all processes are running
"
sleep 45s

echo "
      Process Counts :
"
pdsh -w $SYSTEM_FIRST_NODE sq_check_myuserid -c

echo ""
