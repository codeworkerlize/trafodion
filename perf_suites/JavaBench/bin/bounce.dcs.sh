
echo "
===== Bounce DCS Environment ===== [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

pdsh -w $SYSTEM_FIRST_NODE dcsstop
echo "
DCS stopped [ $(date +"%Y-%m-%d %H:%M:%S") ]
Sleeping for 20 seconds to ensure all mxosrvrs are stopped
"
time sleep 20s
echo "
      Process Counts :
"
pdsh -w $SYSTEM_FIRST_NODE sq_check_myuserid -c

echo ""
pdsh -w $SYSTEM_FIRST_NODE dcsstart
echo "
DCS started [ $(date +"%Y-%m-%d %H:%M:%S") ]
Sleeping for 45 seconds to ensure all mxosrvrs are running
"
time sleep 45s

echo "
      Process Counts :
"
pdsh -w $SYSTEM_FIRST_NODE sq_check_myuserid -c

echo ""
