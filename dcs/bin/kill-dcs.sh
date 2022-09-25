#!/bin/sh
JPS=$JAVA_HOME/bin/jps
case $1 in 
    (master|DcsMaster)
        #echo "Killing DcsMaster..."
        ${JPS}|grep DcsMaster|awk '{print $1}'|xargs -i kill -9 {}
        #echo "Killed DcsMaster"
    ;;
    (server|DcsServer)
        #echo "Killing DcsServer..."
        ${JPS}|grep DcsServer|awk '{print $1}'|xargs -i kill -9 {}
        #echo "Killed DcsServer"
        $DCS_INSTALL_DIR/bin/kill-dcs.sh mxosrvr
    ;;
    (mxosrvr|MXOSRVR)
        #echo "Killing MXOSRVR..."
        ps -u $USER -f|grep mxosrvr|grep -v "grep\|kill-dcs.sh"|awk '{print $2}'|xargs -i kill -9 {}
        #echo "Killed MXOSRVR"
    ;;
esac
