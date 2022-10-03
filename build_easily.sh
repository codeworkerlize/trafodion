#!/bin/bash

log_dir=$TRAF_HOME/../../"build_logs"

version="debug"
if [ $# = 1 ]; then
	if [ "x$1" = "xrelease" ]; then
		version=$1
	fi
fi

function process(){
    component_name=$1
    cmd=$2
    log_file=$3
    make_path=$4
    printf     "[%-32s]...................................[\e[33m%s\e[0m]\r" "building $component_name" "waiting"
    cd $make_path
    ret=$($cmd > "$log_dir/$log_file" 2>&1;echo $?)
    if [ 0 -eq $ret ]; then
        printf "[%-32s]...................................[\e[32m%s\e[0m]\r\n" "build $component_name" "ok"
    else
        printf "[%-32s]...................................[\e[31m%s\e[0m]\r\n" "build $component_name" "failed"
        printf "\e[31mBUILD FAILED\e[0m\n"
        exit 1
    fi

}

if [ ! -d $log_dir ];then
    mkdir $log_dir
fi

if [ ! -d $log_dir/"foundation" ];then
    mkdir $log_dir/"foundation"
fi

#export LC_ALL=en_US.UTF-8
cd $TRAF_HOME/../
./bldenvchk.sh;
export SQ_BUILD_TYPE=${version}

######  componetname command  logfile  src_path
process "sqroot"     "make setupdir"  "sqroot.log"     "$TRAF_HOME"
process "verhdr"     "make genverhdr" "verhdr.log"     "$TRAF_HOME"
# process "dbsecurity" "make all"       "dbsecurity.log" "$TRAF_HOME/../dbsecurity"
process "seamonster" "make all"       "seamonster.log" "$TRAF_HOME/../seamonster/src"

process "foundation/genverhdr" "make genverhdr"   "foundation/genverhdr.log" "$TRAF_HOME"
process "foundation/make_sqevlog" "make"          "foundation/sqev.log"      "$TRAF_HOME/sqevlog"
# process "foundation/seabed"       "make"          "foundation/seabed.log"    "$TRAF_HOME/src/seabed"
process "common"                  "make"          "common.log"               "$TRAF_HOME/../common"
process "foundation/tm"           "make"          "foundation/tm.log"        "$TRAF_HOME/src/tm"
process "foundation/rc"           "make"          "foundation/rc.log"        "$TRAF_HOME/src/rc"
process "foundation/stfs"         "make -f Makefile.stub"       "foundation/stfs.log" "$TRAF_HOME/src/stfs"
process "foundation/trafconf"     "make"          "foundation/trafconf.log"  "$TRAF_HOME/src/trafconf"
process "foundation/monitor"      "make"          "foundation/monitor.log"   "$TRAF_HOME/monitor/linux"
process "foundation/win"          "make"          "foundation/win.log"       "$TRAF_HOME/src/win"
process "foundation/hbase_utilities" "make"       "foundation/hbase_utilities.log" "$TRAF_HOME/hbase_utilities"
process "foundation/make_sql_1"   "make WROOT=$TRAF_HOME/../sql" "foundation/make_sql_1.log" "$TRAF_HOME/sql"
process "foundation/make_sql_2"   "./makemsg.ksh" "foundation/make_sql_2.log" "$TRAF_HOME/sql/scripts"

process "lib_mgmt"                "make all"      "lib_mgmt"  "$TRAF_HOME/../sql/lib_mgmt"
process "jdbcT4&trafci"           "make trafci" "jdbcT4AndTrafci.log" "$TRAF_HOME/../../core"
process "jdbcT2"                  "make all" "jdbcT2.log" "$TRAF_HOME/../conn/jdbc_type2"
process "wms"                     "make" "wms.log" "$TRAF_HOME/../../wms"
process "rest"                    "make" "rest.log" "$TRAF_HOME/../rest"
process "dcs"                     "make" "dcs.log" "$TRAF_HOME/../../dcs"
process "mxosrvr/odbc"            "make" "mxosrvr.log" "$TRAF_HOME/../conn/odbc/src/odbc"

printf "\e[32mBUILD SUCCESS\e[0m\n"
