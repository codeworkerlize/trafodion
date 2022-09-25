
echo "
===== dtmci stats ====== [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
pdsh -w $SYSTEM_FIRST_NODE "dtmci stats 0"

echo ""
