
echo "
===== Reset dtm stats ====== [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

pdsh -w $SYSTEM_FIRST_NODE "dtmci stats reset"

echo ""
