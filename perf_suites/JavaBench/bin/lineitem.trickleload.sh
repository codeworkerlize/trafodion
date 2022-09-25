USAGE="usage: lineitem.trickleload.sh
    [ -db|--database|database <<database>> ]
    [ -sch|--schema|schema <<schema>> ]
    [ -sc|--scale|scale <<scalefactor>> ] 
    [ -p|--partitions|partitions <<partitions>> ] 
    [ -c|--compress|compress <<compressiontype>> ]
    [ -s|--source|source <<sourcesystem>> ] 
    [ -o|--options|options <<options>> ]
    [ -id|--testid|testid <<testid>> ]
    [ -n|--nodes|nodes <<number of nodes>>]
    [ -dsn|--data_source_name|data_source_name <<data_source_name>> ]
    [ -r|--rowset|rowset <<rowset>> ]
    [ -loadcmd|--loadcmd|loadcmd <<IN|UP|UL>> ]
    [ -d|--debug|debug ]
    [ -h|--help|help ]
    [ EXAMPLE: lineitem.trickleload.sh -n 6 -dsn nap015 -r 4000 -loadcmd UL]
"
    
#Default Values
BENCHMARK=trickleload
DATABASE=${JAVABENCH_DEFAULT_DATABASE}
CATALOG=TRAFODION
SCHEMA=load_test
SCALE=0
PARTITIONS=$SYSTEM_DEFAULT_PARTITIONS
export TESTID=$(date +%y%m%d.%H%M)
OPTION_COMPRESSION=SNAPPY
#OPTION_COMPRESSION=NONE
SOURCE_SYSTEM=hive
DSN=trafsh
ROWSET=1000
LOADCMD=IN

#setup ODB environment variables
#export ODBHOME=${JAVABENCH_HOME}/clients/trafodion-1.2.0/odb
#export ODBCHOME=${JAVABENCH_HOME}/clients/trafodion-1.2.0/odb/uodbc
#export ODBCSYSINI=${ODBCHOME}/etc
#export ODBCINI=${ODBCSYSINI}/odbc.ini
#export TRAFODBCHOME=${JAVABENCH_HOME}/clients/trafodion-1.2.0/ODBC
#export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:${ODBCHOME}/lib:$TRAFODBCHOME
#export AppUnicodeType=utf16
#export TESTDATA=${JAVABENCH_HOME}/testdata


while [[ $# > 0 ]] ; do
key="$1"; shift;
case $key in
    -db|--database|database)		DATABASE="$1"; shift;;
    -sch|--schema|schema)		SCHEMA="$1"; shift;;
    -sc|--scale|scale)			SCALE="$1"; shift;; 
    -p|--partitions|partitions)		PARTITIONS="$1"; shift;; 
    -c|--compress|compress)		OPTION_COMPRESSION="$1"; shift;;
    -s|--source|source)			SOURCE_SYSTEM="$1"; shift;;
    -id|--testid|testid)		export TESTID="$1"; shift;;
    -o|--options|options)		OPTIONS="$1"; shift;;
    -n|--nodes|nodes)       NODES="$1"; shift;;
    -dsn|--data_source_name|data_source_name)    DSN="$1"; shift;;
    -r|--rowset|rowset)   ROWSET="$1"; shift;;
    -loadcmd|--loadcmd|loadcmd)    LOADCMD="$1"; shift;;
    -d|--debug|debug)			OPTION_DEBUG="TRUE";;
    -h|--help|help)			  echo -e "$USAGE"; exit 1;;
    *)					echo "Invalid input switch: $key"; echo -e "$USAGE"; exit 1;;
esac
done

if [ -z "$LOGDIRECTORY" ]; then
export LOGDIRECTORY="${JAVABENCH_TEST_HOME}/log/${TESTID}.lineitem.${BENCHMARK}.${LOADCMD}.${ROWSET}.${DATABASE}"
if [ ! -f "${LOGDIRECTORY}" ] ; then
mkdir -p ${LOGDIRECTORY}
fi
fi
LOGFILE=${LOGDIRECTORY}/${TESTID}.lineitem.${BENCHMARK}.${DATABASE}.log
SUMMARYFILE=${LOGDIRECTORY}/${TESTID}.lineitem.${BENCHMARK}.${DATABASE}.summary.log


{

echo "
$0

            TESTID = ${TESTID}
          DATABASE = ${DATABASE}
            SCHEMA = ${SCHEMA}
             SCALE = ${SCALE}
        PARTITIONS = ${PARTITIONS}
       COMPRESSION = ${OPTION_COMPRESSION}
     SOURCE_SYSTEM = ${SOURCE_SYSTEM}
             NODES = ${NODES}
             DEBUG = ${OPTION_DEBUG}
           OPTIONS = ${OPTIONS}
               DSN = ${DSN}
            ROWSET = ${ROWSET}
           LOADCMD = ${LOADCMD}
                 ( logs will be found in ${LOGDIRECTORY} )
"

echo "
===== Start ${BENCHMARK} ( TESTID = ${TESTID} ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
STARTTIME=$SECONDS

if [[ ${SCHEMA} = "default" ]] ; then
SCHEMA=$(grep schema ${JAVABENCH_TEST_HOME}/${DATABASE,,}.properties | awk '{ print $3 }')
fi


##########################################################################
CASEID=0
PAR=${NODES}
###    case_${CASEID}: trickleload, regular table, no index, N connection
echo "
===== Create ${BENCHMARK} Table ${CASEID} (regular table, no index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
${CI_COMMAND}<<EOF
set statistics on;
log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).create.table.${BENCHMARK}.${CASEID}.log clear;
drop schema ${SCHEMA} cascade;
create schema ${SCHEMA};
set schema ${SCHEMA};
drop table LINEITEM;
obey ${JAVABENCH_HOME}/sql/create.lineitem.reg.sql;

exit;
EOF


echo "
===== Start ${BENCHMARK} load ${CASEID} (regular table, no index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
echo "
$ODBHOME/odb64luo -u user -p xx -d $DSN -l loadcmd=${LOADCMD}:src=${TESTDATA}/lineitem.tbl:tgt=${CATALOG}.${SCHEMA}.LINEITEM:fs=\|:rows=${ROWSET}:parallel=${PAR}:bad=${LOGDIRECTORY}/$(date +%y%m%d.%H%M).bad.data
"
$ODBHOME/odb64luo -u user -p xx -d $DSN -l loadcmd=${LOADCMD}:src=${TESTDATA}/lineitem.tbl:tgt=${CATALOG}.${SCHEMA}.LINEITEM:fs=\|:rows=${ROWSET}:parallel=${PAR}:bad=${LOGDIRECTORY}/$(date +%y%m%d.%H%M).bad.data 2>&1 | tail -100 | tee ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).load.data.${BENCHMARK}.${CASEID}.log


echo "
===== End ${BENCHMARK} Load ${CASEID} (regular table, no index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

sleep 5


##########################################################################
CASEID=`expr ${CASEID} + 1`
PAR=`expr ${NODES} \* 2`
###    case_${CASEID}: trickleload, regular table, no index, 2N connection
echo "
===== Create ${BENCHMARK} Table ${CASEID} (regular table, no index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
${CI_COMMAND}<<EOF
set statistics on;
log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).create.table.${BENCHMARK}.${CASEID}.log clear;
set schema ${SCHEMA};
drop table LINEITEM;
obey ${JAVABENCH_HOME}/sql/create.lineitem.reg.sql;

exit;
EOF


echo "
===== Start ${BENCHMARK} load ${CASEID} (regular table, no index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
echo "
$ODBHOME/odb64luo -u user -p xx -d $DSN -l loadcmd=${LOADCMD}:src=${TESTDATA}/lineitem.tbl:tgt=${CATALOG}.${SCHEMA}.LINEITEM:fs=\|:rows=${ROWSET}:parallel=${PAR}:bad=${LOGDIRECTORY}/$(date +%y%m%d.%H%M).bad.data
"
$ODBHOME/odb64luo -u user -p xx -d $DSN -l loadcmd=${LOADCMD}:src=${TESTDATA}/lineitem.tbl:tgt=${CATALOG}.${SCHEMA}.LINEITEM:fs=\|:rows=${ROWSET}:parallel=${PAR}:bad=${LOGDIRECTORY}/$(date +%y%m%d.%H%M).bad.data 2>&1 | tail -100 | tee ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).load.data.${BENCHMARK}.${CASEID}.log


echo "
===== End ${BENCHMARK} Load ${CASEID} (regular table, no index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

sleep 5


##########################################################################
CASEID=`expr ${CASEID} + 1`
PAR=`expr ${NODES} \* 4`
###    case_${CASEID}: trickleload, regular table, no index, 4N connection
echo "
===== Create ${BENCHMARK} Table ${CASEID} (regular table, no index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
${CI_COMMAND}<<EOF
set statistics on;
log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).create.table.${BENCHMARK}.${CASEID}.log clear;
set schema ${SCHEMA};
drop table LINEITEM;
obey ${JAVABENCH_HOME}/sql/create.lineitem.reg.sql;

exit;
EOF


echo "
===== Start ${BENCHMARK} load ${CASEID} (regular table, no index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
echo "
$ODBHOME/odb64luo -u user -p xx -d $DSN -l loadcmd=${LOADCMD}:src=${TESTDATA}/lineitem.tbl:tgt=${CATALOG}.${SCHEMA}.LINEITEM:fs=\|:rows=${ROWSET}:parallel=${PAR}:bad=${LOGDIRECTORY}/$(date +%y%m%d.%H%M).bad.data
"

$ODBHOME/odb64luo -u user -p xx -d $DSN -l loadcmd=${LOADCMD}:src=${TESTDATA}/lineitem.tbl:tgt=${CATALOG}.${SCHEMA}.LINEITEM:fs=\|:rows=${ROWSET}:parallel=${PAR}:bad=${LOGDIRECTORY}/$(date +%y%m%d.%H%M).bad.data 2>&1 | tail -100 | tee ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).load.data.${BENCHMARK}.${CASEID}.log


echo "
===== End ${BENCHMARK} Load ${CASEID} (regular table, no index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

sleep 5


##########################################################################
CASEID=`expr ${CASEID} + 1`
PAR=`expr ${NODES} \* 8`
###    case_${CASEID}: trickleload, regular table, no index, 8N connection
echo "
===== Create ${BENCHMARK} Table ${CASEID} (regular table, no index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
${CI_COMMAND}<<EOF
set statistics on;
log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).create.table.${BENCHMARK}.${CASEID}.log clear;
set schema ${SCHEMA};
drop table LINEITEM;
obey ${JAVABENCH_HOME}/sql/create.lineitem.reg.sql;

exit;
EOF


echo "
===== Start ${BENCHMARK} load ${CASEID} (regular table, no index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
echo "
$ODBHOME/odb64luo -u user -p xx -d $DSN -l loadcmd=${LOADCMD}:src=${TESTDATA}/lineitem.tbl:tgt=${CATALOG}.${SCHEMA}.LINEITEM:fs=\|:rows=${ROWSET}:parallel=${PAR}:bad=${LOGDIRECTORY}/$(date +%y%m%d.%H%M).bad.data
"

$ODBHOME/odb64luo -u user -p xx -d $DSN -l loadcmd=${LOADCMD}:src=${TESTDATA}/lineitem.tbl:tgt=${CATALOG}.${SCHEMA}.LINEITEM:fs=\|:rows=${ROWSET}:parallel=${PAR}:bad=${LOGDIRECTORY}/$(date +%y%m%d.%H%M).bad.data 2>&1 | tail -100 | tee ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).load.data.${BENCHMARK}.${CASEID}.log


echo "
===== End ${BENCHMARK} Load ${CASEID} (regular table, no index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

sleep 5



##########################################################################
CASEID=`expr ${CASEID} + 1`
PAR=`expr ${NODES} \* 1`
###    case_${CASEID}: trickleload, aligned table, no index, N connection
echo "
===== Create ${BENCHMARK} Table ${CASEID} (aligned table, no index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
${CI_COMMAND}<<EOF
set statistics on;
log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).create.table.${BENCHMARK}.${CASEID}.log clear;
set schema ${SCHEMA};
drop table LINEITEM;
obey ${JAVABENCH_HOME}/sql/create.lineitem.sql;

exit;
EOF


echo "
===== Start ${BENCHMARK} load ${CASEID} (aligned table, no index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
echo "
$ODBHOME/odb64luo -u user -p xx -d $DSN -l loadcmd=${LOADCMD}:src=${TESTDATA}/lineitem.tbl:tgt=${CATALOG}.${SCHEMA}.LINEITEM:fs=\|:rows=${ROWSET}:parallel=${PAR}:bad=${LOGDIRECTORY}/$(date +%y%m%d.%H%M).bad.data
"

$ODBHOME/odb64luo -u user -p xx -d $DSN -l loadcmd=${LOADCMD}:src=${TESTDATA}/lineitem.tbl:tgt=${CATALOG}.${SCHEMA}.LINEITEM:fs=\|:rows=${ROWSET}:parallel=${PAR}:bad=${LOGDIRECTORY}/$(date +%y%m%d.%H%M).bad.data 2>&1 | tail -100 | tee ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).load.data.${BENCHMARK}.${CASEID}.log


echo "
===== End ${BENCHMARK} Load ${CASEID} (aligned table, no index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

sleep 5



##########################################################################
CASEID=`expr ${CASEID} + 1`
PAR=`expr ${NODES} \* 2`
###    case_${CASEID}: trickleload, aligned table, no index, 2N connection
echo "
===== Create ${BENCHMARK} Table ${CASEID} (aligned table, no index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
${CI_COMMAND}<<EOF
set statistics on;
log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).create.table.${BENCHMARK}.${CASEID}.log clear;
set schema ${SCHEMA};
drop table LINEITEM;
obey ${JAVABENCH_HOME}/sql/create.lineitem.sql;

exit;
EOF


echo "
===== Start ${BENCHMARK} load ${CASEID} (aligned table, no index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
echo "
$ODBHOME/odb64luo -u user -p xx -d $DSN -l loadcmd=${LOADCMD}:src=${TESTDATA}/lineitem.tbl:tgt=${CATALOG}.${SCHEMA}.LINEITEM:fs=\|:rows=${ROWSET}:parallel=${PAR}:bad=${LOGDIRECTORY}/$(date +%y%m%d.%H%M).bad.data
"

$ODBHOME/odb64luo -u user -p xx -d $DSN -l loadcmd=${LOADCMD}:src=${TESTDATA}/lineitem.tbl:tgt=${CATALOG}.${SCHEMA}.LINEITEM:fs=\|:rows=${ROWSET}:parallel=${PAR}:bad=${LOGDIRECTORY}/$(date +%y%m%d.%H%M).bad.data 2>&1 | tail -100 | tee ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).load.data.${BENCHMARK}.${CASEID}.log


echo "
===== End ${BENCHMARK} Load ${CASEID} (aligned table, no index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

sleep 5



##########################################################################
CASEID=`expr ${CASEID} + 1`
PAR=`expr ${NODES} \* 4`
###    case_${CASEID}: trickleload, aligned table, no index, 4N connection
echo "
===== Create ${BENCHMARK} Table ${CASEID} (aligned table, no index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
${CI_COMMAND}<<EOF
set statistics on;
log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).create.table.${BENCHMARK}.${CASEID}.log clear;
set schema ${SCHEMA};
drop table LINEITEM;
obey ${JAVABENCH_HOME}/sql/create.lineitem.sql;

exit;
EOF


echo "
===== Start ${BENCHMARK} load ${CASEID} (aligned table, no index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
echo "
$ODBHOME/odb64luo -u user -p xx -d $DSN -l loadcmd=${LOADCMD}:src=${TESTDATA}/lineitem.tbl:tgt=${CATALOG}.${SCHEMA}.LINEITEM:fs=\|:rows=${ROWSET}:parallel=${PAR}:bad=${LOGDIRECTORY}/$(date +%y%m%d.%H%M).bad.data
"

$ODBHOME/odb64luo -u user -p xx -d $DSN -l loadcmd=${LOADCMD}:src=${TESTDATA}/lineitem.tbl:tgt=${CATALOG}.${SCHEMA}.LINEITEM:fs=\|:rows=${ROWSET}:parallel=${PAR}:bad=${LOGDIRECTORY}/$(date +%y%m%d.%H%M).bad.data 2>&1 | tail -100 | tee ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).load.data.${BENCHMARK}.${CASEID}.log


echo "
===== End ${BENCHMARK} Load ${CASEID} (aligned table, no index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

sleep 5



##########################################################################
CASEID=`expr ${CASEID} + 1`
PAR=`expr ${NODES} \* 8`
###    case_${CASEID}: trickleload, aligned table, no index, 8N connection
echo "
===== Create ${BENCHMARK} Table ${CASEID} (aligned table, no index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
${CI_COMMAND}<<EOF
set statistics on;
log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).create.table.${BENCHMARK}.${CASEID}.log clear;
set schema ${SCHEMA};
drop table LINEITEM;
obey ${JAVABENCH_HOME}/sql/create.lineitem.sql;

exit;
EOF


echo "
===== Start ${BENCHMARK} load ${CASEID} (aligned table, no index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
echo "
$ODBHOME/odb64luo -u user -p xx -d $DSN -l loadcmd=${LOADCMD}:src=${TESTDATA}/lineitem.tbl:tgt=${CATALOG}.${SCHEMA}.LINEITEM:fs=\|:rows=${ROWSET}:parallel=${PAR}:bad=${LOGDIRECTORY}/$(date +%y%m%d.%H%M).bad.data
"

$ODBHOME/odb64luo -u user -p xx -d $DSN -l loadcmd=${LOADCMD}:src=${TESTDATA}/lineitem.tbl:tgt=${CATALOG}.${SCHEMA}.LINEITEM:fs=\|:rows=${ROWSET}:parallel=${PAR}:bad=${LOGDIRECTORY}/$(date +%y%m%d.%H%M).bad.data 2>&1 | tail -100 | tee ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).load.data.${BENCHMARK}.${CASEID}.log


echo "
===== End ${BENCHMARK} Load ${CASEID} (aligned table, no index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

sleep 5



##########################################################################
CASEID=`expr ${CASEID} + 1`
PAR=`expr ${NODES} \* 1`
###    case_${CASEID}: trickleload, aligned table, no index, N connection * 2 instances
echo "
===== Create ${BENCHMARK} Table ${CASEID} (aligned table, no index, ${PAR} connection * 2 instances) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
${CI_COMMAND}<<EOF
set statistics on;
log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).create.table.${BENCHMARK}.${CASEID}.log clear;
set schema ${SCHEMA};
drop table LINEITEM;
obey ${JAVABENCH_HOME}/sql/create.lineitem.sql;

exit;
EOF


echo "
===== Start ${BENCHMARK} load ${CASEID} (aligned table, no index, ${PAR} connection * 2 instances) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
echo "
$ODBHOME/odb64luo -u user -p xx -d $DSN -l loadcmd=${LOADCMD}:src=${TESTDATA}/lineitem.2_instances.tbl:tgt=${CATALOG}.${SCHEMA}.LINEITEM:fs=\|:rows=${ROWSET}:parallel=${PAR}:bad=${LOGDIRECTORY}/$(date +%y%m%d.%H%M).bad.data
"
# Multiple Distributed ODB instances

ODB_NODES=$(echo ${DRIVER_NODE_NAMES}|awk '{print "-w " $1 " -w " $2}')
echo ""
echo "Starting ODB on node \"$(echo $ODB_NODES|awk '{print $2" "$4}')\""
echo ""
    pdsh ${ODB_NODES} "{
    cd ${JAVABENCH_TEST_HOME}
    . ./profile
    export TESTID=${TESTID}
    export CASEID=${CASEID}
    export PAR=${PAR}
    export LOGDIRECTORY=${LOGDIRECTORY}
    export BENCHMARK=${BENCHMARK}
    export DATABASE=${DATABASE}
    export CATALOG=${CATALOG}
    export SCHEMA=${SCHEMA}
    export DSN=${DSN}
    export ROWSET=${ROWSET}
    export LOADCMD=${LOADCMD}
    if [ ! -f "${LOGDIRECTORY}" ] ; then mkdir -p ${LOGDIRECTORY}; fi
    $ODBHOME/odb64luo -u user -p xx -d $DSN -l loadcmd=${LOADCMD}:src=${TESTDATA}/lineitem.2_instances.tbl:tgt=${CATALOG}.${SCHEMA}.LINEITEM:fs=\|:rows=${ROWSET}:parallel=${PAR}:bad=${LOGDIRECTORY}/$(date +%y%m%d.%H%M).bad.data 2>&1 | tail -100 | tee ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).load.data.${BENCHMARK}.${CASEID}.log
		}"


echo "
===== End ${BENCHMARK} Load ${CASEID} (aligned table, no index, ${PAR} connection * 2 instances) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

sleep 5


##########################################################################
CASEID=`expr ${CASEID} + 1`
PAR=`expr ${NODES} \* 2`
###    case_${CASEID}: trickleload, aligned table, no index, 2N connection * 2 instances
echo "
===== Create ${BENCHMARK} Table ${CASEID} (aligned table, no index, ${PAR} connection * 2 instances) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
${CI_COMMAND}<<EOF
set statistics on;
log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).create.table.${BENCHMARK}.${CASEID}.log clear;
set schema ${SCHEMA};
drop table LINEITEM;
obey ${JAVABENCH_HOME}/sql/create.lineitem.sql;

exit;
EOF


echo "
===== Start ${BENCHMARK} load ${CASEID} (aligned table, no index, ${PAR} connection * 2 instances) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
echo "
$ODBHOME/odb64luo -u user -p xx -d $DSN -l loadcmd=${LOADCMD}:src=${TESTDATA}/lineitem.2_instances.tbl:tgt=${CATALOG}.${SCHEMA}.LINEITEM:fs=\|:rows=${ROWSET}:parallel=${PAR}:bad=${LOGDIRECTORY}/$(date +%y%m%d.%H%M).bad.data
"

# Multiple Distributed ODB instances

ODB_NODES=$(echo ${DRIVER_NODE_NAMES}|awk '{print "-w " $1 " -w " $2}')
echo ""
echo "Starting ODB on node \"$(echo $ODB_NODES|awk '{print $2" "$4}')\""
echo ""
    pdsh ${ODB_NODES} "{
    cd ${JAVABENCH_TEST_HOME}
    . ./profile
    export TESTID=${TESTID}
    export CASEID=${CASEID}
    export PAR=${PAR}
    export LOGDIRECTORY=${LOGDIRECTORY}
    export BENCHMARK=${BENCHMARK}
    export DATABASE=${DATABASE}
    export CATALOG=${CATALOG}
    export SCHEMA=${SCHEMA}
    export DSN=${DSN}
    export ROWSET=${ROWSET}
    export LOADCMD=${LOADCMD}
    if [ ! -f "${LOGDIRECTORY}" ] ; then mkdir -p ${LOGDIRECTORY}; fi
    $ODBHOME/odb64luo -u user -p xx -d $DSN -l loadcmd=${LOADCMD}:src=${TESTDATA}/lineitem.2_instances.tbl:tgt=${CATALOG}.${SCHEMA}.LINEITEM:fs=\|:rows=${ROWSET}:parallel=${PAR}:bad=${LOGDIRECTORY}/$(date +%y%m%d.%H%M).bad.data 2>&1 | tail -100 | tee ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).load.data.${BENCHMARK}.${CASEID}.log
		}"


echo "
===== End ${BENCHMARK} Load ${CASEID} (aligned table, no index, ${PAR} connection * 2 instances) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

sleep 5



##########################################################################
CASEID=`expr ${CASEID} + 1`
PAR=`expr ${NODES} \* 4`
###    case_${CASEID}: trickleload, aligned table, no index, 4N connection * 2 instances
echo "
===== Create ${BENCHMARK} Table ${CASEID} (aligned table, no index, ${PAR} connection * 2 instances) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
${CI_COMMAND}<<EOF
set statistics on;
log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).create.table.${BENCHMARK}.${CASEID}.log clear;
set schema ${SCHEMA};
drop table LINEITEM;
obey ${JAVABENCH_HOME}/sql/create.lineitem.sql;

exit;
EOF


echo "
===== Start ${BENCHMARK} load ${CASEID} (aligned table, no index, ${PAR} connection * 2 instances) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
echo "
$ODBHOME/odb64luo -u user -p xx -d $DSN -l loadcmd=${LOADCMD}:src=${TESTDATA}/lineitem.2_instances.tbl:tgt=${CATALOG}.${SCHEMA}.LINEITEM:fs=\|:rows=${ROWSET}:parallel=${PAR}:bad=${LOGDIRECTORY}/$(date +%y%m%d.%H%M).bad.data
"

# Multiple Distributed ODB instances

ODB_NODES=$(echo ${DRIVER_NODE_NAMES}|awk '{print "-w " $1 " -w " $2}')
echo ""
echo "Starting ODB on node \"$(echo $ODB_NODES|awk '{print $2" "$4}')\""
echo ""
    pdsh ${ODB_NODES} "{
    cd ${JAVABENCH_TEST_HOME}
    . ./profile
    export TESTID=${TESTID}
    export CASEID=${CASEID}
    export PAR=${PAR}
    export LOGDIRECTORY=${LOGDIRECTORY}
    export BENCHMARK=${BENCHMARK}
    export DATABASE=${DATABASE}
    export CATALOG=${CATALOG}
    export SCHEMA=${SCHEMA}
    export DSN=${DSN}
    export ROWSET=${ROWSET}
    export LOADCMD=${LOADCMD}
    if [ ! -f "${LOGDIRECTORY}" ] ; then mkdir -p ${LOGDIRECTORY}; fi
    $ODBHOME/odb64luo -u user -p xx -d $DSN -l loadcmd=${LOADCMD}:src=${TESTDATA}/lineitem.2_instances.tbl:tgt=${CATALOG}.${SCHEMA}.LINEITEM:fs=\|:rows=${ROWSET}:parallel=${PAR}:bad=${LOGDIRECTORY}/$(date +%y%m%d.%H%M).bad.data 2>&1 | tail -100 | tee ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).load.data.${BENCHMARK}.${CASEID}.log
		}"


echo "
===== End ${BENCHMARK} Load ${CASEID} (aligned table, no index, ${PAR} connection * 2 instances) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

sleep 5



##########################################################################
CASEID=`expr ${CASEID} + 1`
PAR=`expr ${NODES} \* 8`
###    case_${CASEID}: trickleload, aligned table, no index, 8N connection * 2 instances
echo "
===== Create ${BENCHMARK} Table ${CASEID} (aligned table, no index, ${PAR} connection * 2 instances) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
${CI_COMMAND}<<EOF
set statistics on;
log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).create.table.${BENCHMARK}.${CASEID}.log clear;
set schema ${SCHEMA};
drop table LINEITEM;
obey ${JAVABENCH_HOME}/sql/create.lineitem.sql;

exit;
EOF


echo "
===== Start ${BENCHMARK} load ${CASEID} (aligned table, no index, ${PAR} connection * 2 instances) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
echo "
$ODBHOME/odb64luo -u user -p xx -d $DSN -l loadcmd=${LOADCMD}:src=${TESTDATA}/lineitem.2_instances.tbl:tgt=${CATALOG}.${SCHEMA}.LINEITEM:fs=\|:rows=${ROWSET}:parallel=${PAR}:bad=${LOGDIRECTORY}/$(date +%y%m%d.%H%M).bad.data
"

# Multiple Distributed ODB instances

ODB_NODES=$(echo ${DRIVER_NODE_NAMES}|awk '{print "-w " $1 " -w " $2}')
echo ""
echo "Starting ODB on node \"$(echo $ODB_NODES|awk '{print $2" "$4}')\""
echo ""
    pdsh ${ODB_NODES} "{
    cd ${JAVABENCH_TEST_HOME}
    . ./profile
    export TESTID=${TESTID}
    export CASEID=${CASEID}
    export PAR=${PAR}
    export LOGDIRECTORY=${LOGDIRECTORY}
    export BENCHMARK=${BENCHMARK}
    export DATABASE=${DATABASE}
    export CATALOG=${CATALOG}
    export SCHEMA=${SCHEMA}
    export DSN=${DSN}
    export ROWSET=${ROWSET}
    export LOADCMD=${LOADCMD}
    if [ ! -f "${LOGDIRECTORY}" ] ; then mkdir -p ${LOGDIRECTORY}; fi
    $ODBHOME/odb64luo -u user -p xx -d $DSN -l loadcmd=${LOADCMD}:src=${TESTDATA}/lineitem.2_instances.tbl:tgt=${CATALOG}.${SCHEMA}.LINEITEM:fs=\|:rows=${ROWSET}:parallel=${PAR}:bad=${LOGDIRECTORY}/$(date +%y%m%d.%H%M).bad.data 2>&1 | tail -100 | tee ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).load.data.${BENCHMARK}.${CASEID}.log
		}"


echo "
===== End ${BENCHMARK} Load ${CASEID} (aligned table, no index, ${PAR} connection * 2 instances) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

sleep 5



##########################################################################
CASEID=`expr ${CASEID} + 1`
PAR=`expr ${NODES} \* 1`
###    case_${CASEID}: trickleload, aligned table, 1 index, N connection
echo "
===== Create ${BENCHMARK} Table ${CASEID} (aligned table, 1 index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
${CI_COMMAND}<<EOF
set statistics on;
log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).create.table.${BENCHMARK}.${CASEID}.log clear;
set schema ${SCHEMA};
drop table LINEITEM;
obey ${JAVABENCH_HOME}/sql/create.lineitem.sql;
create index index_1 on LINEITEM (L_ORDERKEY) salt like table HBASE_OPTIONS  ( DATA_BLOCK_ENCODING = 'FAST_DIFF' , COMPRESSION = 'SNAPPY' );

exit;
EOF


echo "
===== Start ${BENCHMARK} load ${CASEID} (aligned table, 1 index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
echo "
$ODBHOME/odb64luo -u user -p xx -d $DSN -l loadcmd=${LOADCMD}:src=${TESTDATA}/lineitem.tbl:tgt=${CATALOG}.${SCHEMA}.LINEITEM:fs=\|:rows=${ROWSET}:parallel=${PAR}:bad=${LOGDIRECTORY}/$(date +%y%m%d.%H%M).bad.data
"

$ODBHOME/odb64luo -u user -p xx -d $DSN -l loadcmd=${LOADCMD}:src=${TESTDATA}/lineitem.tbl:tgt=${CATALOG}.${SCHEMA}.LINEITEM:fs=\|:rows=${ROWSET}:parallel=${PAR}:bad=${LOGDIRECTORY}/$(date +%y%m%d.%H%M).bad.data 2>&1 | tail -100 | tee ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).load.data.${BENCHMARK}.${CASEID}.log


echo "
===== End ${BENCHMARK} Load ${CASEID} (aligned table, 1 index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

sleep 5



##########################################################################
CASEID=`expr ${CASEID} + 1`
PAR=`expr ${NODES} \* 2`
###    case_${CASEID}: trickleload, aligned table, 1 index, 2N connection
echo "
===== Create ${BENCHMARK} Table ${CASEID} (aligned table, 1 index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
${CI_COMMAND}<<EOF
set statistics on;
log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).create.table.${BENCHMARK}.${CASEID}.log clear;
set schema ${SCHEMA};
drop table LINEITEM;
obey ${JAVABENCH_HOME}/sql/create.lineitem.sql;
create index index_1 on LINEITEM (L_ORDERKEY) salt like table HBASE_OPTIONS  ( DATA_BLOCK_ENCODING = 'FAST_DIFF' , COMPRESSION = 'SNAPPY' );

exit;
EOF


echo "
===== Start ${BENCHMARK} load ${CASEID} (aligned table, 1 index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
echo "
$ODBHOME/odb64luo -u user -p xx -d $DSN -l loadcmd=${LOADCMD}:src=${TESTDATA}/lineitem.tbl:tgt=${CATALOG}.${SCHEMA}.LINEITEM:fs=\|:rows=${ROWSET}:parallel=${PAR}:bad=${LOGDIRECTORY}/$(date +%y%m%d.%H%M).bad.data
"

$ODBHOME/odb64luo -u user -p xx -d $DSN -l loadcmd=${LOADCMD}:src=${TESTDATA}/lineitem.tbl:tgt=${CATALOG}.${SCHEMA}.LINEITEM:fs=\|:rows=${ROWSET}:parallel=${PAR}:bad=${LOGDIRECTORY}/$(date +%y%m%d.%H%M).bad.data 2>&1 | tail -100 | tee ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).load.data.${BENCHMARK}.${CASEID}.log


echo "
===== End ${BENCHMARK} Load ${CASEID} (aligned table, 1 index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

sleep 5



##########################################################################
CASEID=`expr ${CASEID} + 1`
PAR=`expr ${NODES} \* 4`
###    case_${CASEID}: trickleload, aligned table, 1 index, 4N connection
echo "
===== Create ${BENCHMARK} Table ${CASEID} (aligned table, 1 index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
${CI_COMMAND}<<EOF
set statistics on;
log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).create.table.${BENCHMARK}.${CASEID}.log clear;
set schema ${SCHEMA};
drop table LINEITEM;
obey ${JAVABENCH_HOME}/sql/create.lineitem.sql;
create index index_1 on LINEITEM (L_ORDERKEY) salt like table HBASE_OPTIONS  ( DATA_BLOCK_ENCODING = 'FAST_DIFF' , COMPRESSION = 'SNAPPY' );

exit;
EOF


echo "
===== Start ${BENCHMARK} load ${CASEID} (aligned table, 1 index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
echo "
$ODBHOME/odb64luo -u user -p xx -d $DSN -l loadcmd=${LOADCMD}:src=${TESTDATA}/lineitem.tbl:tgt=${CATALOG}.${SCHEMA}.LINEITEM:fs=\|:rows=${ROWSET}:parallel=${PAR}:bad=${LOGDIRECTORY}/$(date +%y%m%d.%H%M).bad.data
"

$ODBHOME/odb64luo -u user -p xx -d $DSN -l loadcmd=${LOADCMD}:src=${TESTDATA}/lineitem.tbl:tgt=${CATALOG}.${SCHEMA}.LINEITEM:fs=\|:rows=${ROWSET}:parallel=${PAR}:bad=${LOGDIRECTORY}/$(date +%y%m%d.%H%M).bad.data 2>&1 | tail -100 | tee ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).load.data.${BENCHMARK}.${CASEID}.log


echo "
===== End ${BENCHMARK} Load ${CASEID} (aligned table, 1 index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

sleep 5



##########################################################################
CASEID=`expr ${CASEID} + 1`
PAR=`expr ${NODES} \* 8`
###    case_${CASEID}: trickleload, aligned table, 1 index, 8N connection
echo "
===== Create ${BENCHMARK} Table ${CASEID} (aligned table, 1 index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
${CI_COMMAND}<<EOF
set statistics on;
log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).create.table.${BENCHMARK}.${CASEID}.log clear;
set schema ${SCHEMA};
drop table LINEITEM;
obey ${JAVABENCH_HOME}/sql/create.lineitem.sql;
create index index_1 on LINEITEM (L_ORDERKEY) salt like table HBASE_OPTIONS  ( DATA_BLOCK_ENCODING = 'FAST_DIFF' , COMPRESSION = 'SNAPPY' );

exit;
EOF


echo "
===== Start ${BENCHMARK} load ${CASEID} (aligned table, 1 index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
echo "
$ODBHOME/odb64luo -u user -p xx -d $DSN -l loadcmd=${LOADCMD}:src=${TESTDATA}/lineitem.tbl:tgt=${CATALOG}.${SCHEMA}.LINEITEM:fs=\|:rows=${ROWSET}:parallel=${PAR}:bad=${LOGDIRECTORY}/$(date +%y%m%d.%H%M).bad.data
"

$ODBHOME/odb64luo -u user -p xx -d $DSN -l loadcmd=${LOADCMD}:src=${TESTDATA}/lineitem.tbl:tgt=${CATALOG}.${SCHEMA}.LINEITEM:fs=\|:rows=${ROWSET}:parallel=${PAR}:bad=${LOGDIRECTORY}/$(date +%y%m%d.%H%M).bad.data 2>&1 | tail -100 | tee ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).load.data.${BENCHMARK}.${CASEID}.log


echo "
===== End ${BENCHMARK} Load ${CASEID} (aligned table, 1 index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

sleep 5



##########################################################################
CASEID=`expr ${CASEID} + 1`
PAR=`expr ${NODES} \* 1`
###    case_${CASEID}: trickleload, aligned table, 2 index, N connection
echo "
===== Create ${BENCHMARK} Table ${CASEID} (aligned table, 2 index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
${CI_COMMAND}<<EOF
set statistics on;
log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).create.table.${BENCHMARK}.${CASEID}.log clear;
set schema ${SCHEMA};
drop index index_1;
drop table LINEITEM;
obey ${JAVABENCH_HOME}/sql/create.lineitem.sql;
create index index_1 on LINEITEM (L_ORDERKEY) salt like table HBASE_OPTIONS  ( DATA_BLOCK_ENCODING = 'FAST_DIFF' , COMPRESSION = 'SNAPPY' );
create index index_2 on LINEITEM (L_SHIPDATE) salt like table HBASE_OPTIONS  ( DATA_BLOCK_ENCODING = 'FAST_DIFF' , COMPRESSION = 'SNAPPY' );

exit;
EOF


echo "
===== Start ${BENCHMARK} load ${CASEID} (aligned table, 2 index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
echo "
$ODBHOME/odb64luo -u user -p xx -d $DSN -l loadcmd=${LOADCMD}:src=${TESTDATA}/lineitem.tbl:tgt=${CATALOG}.${SCHEMA}.LINEITEM:fs=\|:rows=${ROWSET}:parallel=${PAR}:bad=${LOGDIRECTORY}/$(date +%y%m%d.%H%M).bad.data
"

$ODBHOME/odb64luo -u user -p xx -d $DSN -l loadcmd=${LOADCMD}:src=${TESTDATA}/lineitem.tbl:tgt=${CATALOG}.${SCHEMA}.LINEITEM:fs=\|:rows=${ROWSET}:parallel=${PAR}:bad=${LOGDIRECTORY}/$(date +%y%m%d.%H%M).bad.data 2>&1 | tail -100 | tee ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).load.data.${BENCHMARK}.${CASEID}.log


echo "
===== End ${BENCHMARK} Load ${CASEID} (aligned table, 2 index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

sleep 5



##########################################################################
CASEID=`expr ${CASEID} + 1`
PAR=`expr ${NODES} \* 2`
###    case_${CASEID}: trickleload, aligned table, 2 index, 2N connection
echo "
===== Create ${BENCHMARK} Table ${CASEID} (aligned table, 2 index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
${CI_COMMAND}<<EOF
set statistics on;
log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).create.table.${BENCHMARK}.${CASEID}.log clear;
set schema ${SCHEMA};
drop index index_1;
drop index index_2;
drop table LINEITEM;
obey ${JAVABENCH_HOME}/sql/create.lineitem.sql;
create index index_1 on LINEITEM (L_ORDERKEY) salt like table HBASE_OPTIONS  ( DATA_BLOCK_ENCODING = 'FAST_DIFF' , COMPRESSION = 'SNAPPY' );
create index index_2 on LINEITEM (L_SHIPDATE) salt like table HBASE_OPTIONS  ( DATA_BLOCK_ENCODING = 'FAST_DIFF' , COMPRESSION = 'SNAPPY' );

exit;
EOF


echo "
===== Start ${BENCHMARK} load ${CASEID} (aligned table, 2 index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
echo "
$ODBHOME/odb64luo -u user -p xx -d $DSN -l loadcmd=${LOADCMD}:src=${TESTDATA}/lineitem.tbl:tgt=${CATALOG}.${SCHEMA}.LINEITEM:fs=\|:rows=${ROWSET}:parallel=${PAR}:bad=${LOGDIRECTORY}/$(date +%y%m%d.%H%M).bad.data
"

$ODBHOME/odb64luo -u user -p xx -d $DSN -l loadcmd=${LOADCMD}:src=${TESTDATA}/lineitem.tbl:tgt=${CATALOG}.${SCHEMA}.LINEITEM:fs=\|:rows=${ROWSET}:parallel=${PAR}:bad=${LOGDIRECTORY}/$(date +%y%m%d.%H%M).bad.data 2>&1 | tail -100 | tee ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).load.data.${BENCHMARK}.${CASEID}.log


echo "
===== End ${BENCHMARK} Load ${CASEID} (aligned table, 2 index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

sleep 5



##########################################################################
CASEID=`expr ${CASEID} + 1`
PAR=`expr ${NODES} \* 4`
###    case_${CASEID}: trickleload, aligned table, 2 index, 4N connection
echo "
===== Create ${BENCHMARK} Table ${CASEID} (aligned table, 2 index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
${CI_COMMAND}<<EOF
set statistics on;
log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).create.table.${BENCHMARK}.${CASEID}.log clear;
set schema ${SCHEMA};
drop index index_1;
drop index index_2;
drop table LINEITEM;
obey ${JAVABENCH_HOME}/sql/create.lineitem.sql;
create index index_1 on LINEITEM (L_ORDERKEY) salt like table HBASE_OPTIONS  ( DATA_BLOCK_ENCODING = 'FAST_DIFF' , COMPRESSION = 'SNAPPY' );
create index index_2 on LINEITEM (L_SHIPDATE) salt like table HBASE_OPTIONS  ( DATA_BLOCK_ENCODING = 'FAST_DIFF' , COMPRESSION = 'SNAPPY' );

exit;
EOF


echo "
===== Start ${BENCHMARK} load ${CASEID} (aligned table, 2 index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
echo "
$ODBHOME/odb64luo -u user -p xx -d $DSN -l loadcmd=${LOADCMD}:src=${TESTDATA}/lineitem.tbl:tgt=${CATALOG}.${SCHEMA}.LINEITEM:fs=\|:rows=${ROWSET}:parallel=${PAR}:bad=${LOGDIRECTORY}/$(date +%y%m%d.%H%M).bad.data
"

$ODBHOME/odb64luo -u user -p xx -d $DSN -l loadcmd=${LOADCMD}:src=${TESTDATA}/lineitem.tbl:tgt=${CATALOG}.${SCHEMA}.LINEITEM:fs=\|:rows=${ROWSET}:parallel=${PAR}:bad=${LOGDIRECTORY}/$(date +%y%m%d.%H%M).bad.data 2>&1 | tail -100 | tee ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).load.data.${BENCHMARK}.${CASEID}.log


echo "
===== End ${BENCHMARK} Load ${CASEID} (aligned table, 2 index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

sleep 5



##########################################################################
CASEID=`expr ${CASEID} + 1`
PAR=`expr ${NODES} \* 8`
###    case_${CASEID}: trickleload, aligned table, 2 index, 8N connection
echo "
===== Create ${BENCHMARK} Table ${CASEID} (aligned table, 2 index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
${CI_COMMAND}<<EOF
set statistics on;
log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).create.table.${BENCHMARK}.${CASEID}.log clear;
set schema ${SCHEMA};
drop index index_1;
drop index index_2;
drop table LINEITEM;
obey ${JAVABENCH_HOME}/sql/create.lineitem.sql;
create index index_1 on LINEITEM (L_ORDERKEY) salt like table HBASE_OPTIONS  ( DATA_BLOCK_ENCODING = 'FAST_DIFF' , COMPRESSION = 'SNAPPY' );
create index index_2 on LINEITEM (L_SHIPDATE) salt like table HBASE_OPTIONS  ( DATA_BLOCK_ENCODING = 'FAST_DIFF' , COMPRESSION = 'SNAPPY' );

exit;
EOF


echo "
===== Start ${BENCHMARK} load ${CASEID} (aligned table, 2 index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
echo "
$ODBHOME/odb64luo -u user -p xx -d $DSN -l loadcmd=${LOADCMD}:src=${TESTDATA}/lineitem.tbl:tgt=${CATALOG}.${SCHEMA}.LINEITEM:fs=\|:rows=${ROWSET}:parallel=${PAR}:bad=${LOGDIRECTORY}/$(date +%y%m%d.%H%M).bad.data
"

$ODBHOME/odb64luo -u user -p xx -d $DSN -l loadcmd=${LOADCMD}:src=${TESTDATA}/lineitem.tbl:tgt=${CATALOG}.${SCHEMA}.LINEITEM:fs=\|:rows=${ROWSET}:parallel=${PAR}:bad=${LOGDIRECTORY}/$(date +%y%m%d.%H%M).bad.data 2>&1 | tail -100 | tee ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).load.data.${BENCHMARK}.${CASEID}.log


echo "
===== End ${BENCHMARK} Load ${CASEID} (aligned table, 2 index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

sleep 5



##########################################################################
CASEID=`expr ${CASEID} + 1`
PAR=`expr ${NODES} \* 1`
###    case_${CASEID}: trickleload, aligned table, 4 index, N connection
echo "
===== Create ${BENCHMARK} Table ${CASEID} (aligned table, 4 index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
${CI_COMMAND}<<EOF
set statistics on;
log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).create.table.${BENCHMARK}.${CASEID}.log clear;
set schema ${SCHEMA};
drop index index_1;
drop index index_2;
drop table LINEITEM;
obey ${JAVABENCH_HOME}/sql/create.lineitem.sql;
create index index_1 on LINEITEM (L_ORDERKEY) salt like table HBASE_OPTIONS  ( DATA_BLOCK_ENCODING = 'FAST_DIFF' , COMPRESSION = 'SNAPPY' );
create index index_2 on LINEITEM (L_SHIPDATE) salt like table HBASE_OPTIONS  ( DATA_BLOCK_ENCODING = 'FAST_DIFF' , COMPRESSION = 'SNAPPY' );
create index index_3 on LINEITEM (L_COMMITDATE) salt like table HBASE_OPTIONS  ( DATA_BLOCK_ENCODING = 'FAST_DIFF' , COMPRESSION = 'SNAPPY' );
create index index_4 on LINEITEM (L_RECEIPTDATE) salt like table HBASE_OPTIONS  ( DATA_BLOCK_ENCODING = 'FAST_DIFF' , COMPRESSION = 'SNAPPY' );

exit;
EOF


echo "
===== Start ${BENCHMARK} load ${CASEID} (aligned table, 4 index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
echo "
$ODBHOME/odb64luo -u user -p xx -d $DSN -l loadcmd=${LOADCMD}:src=${TESTDATA}/lineitem.tbl:tgt=${CATALOG}.${SCHEMA}.LINEITEM:fs=\|:rows=${ROWSET}:parallel=${PAR}:bad=${LOGDIRECTORY}/$(date +%y%m%d.%H%M).bad.data
"

$ODBHOME/odb64luo -u user -p xx -d $DSN -l loadcmd=${LOADCMD}:src=${TESTDATA}/lineitem.tbl:tgt=${CATALOG}.${SCHEMA}.LINEITEM:fs=\|:rows=${ROWSET}:parallel=${PAR}:bad=${LOGDIRECTORY}/$(date +%y%m%d.%H%M).bad.data 2>&1 | tail -100 | tee ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).load.data.${BENCHMARK}.${CASEID}.log


echo "
===== End ${BENCHMARK} Load ${CASEID} (aligned table, 4 index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

sleep 5



##########################################################################
CASEID=`expr ${CASEID} + 1`
PAR=`expr ${NODES} \* 2`
###    case_${CASEID}: trickleload, aligned table, 4 index, 2N connection
echo "
===== Create ${BENCHMARK} Table ${CASEID} (aligned table, 4 index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
${CI_COMMAND}<<EOF
set statistics on;
log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).create.table.${BENCHMARK}.${CASEID}.log clear;
set schema ${SCHEMA};
drop index index_1;
drop index index_2;
drop index index_3;
drop index index_4;
drop table LINEITEM;
obey ${JAVABENCH_HOME}/sql/create.lineitem.sql;
create index index_1 on LINEITEM (L_ORDERKEY) salt like table HBASE_OPTIONS  ( DATA_BLOCK_ENCODING = 'FAST_DIFF' , COMPRESSION = 'SNAPPY' );
create index index_2 on LINEITEM (L_SHIPDATE) salt like table HBASE_OPTIONS  ( DATA_BLOCK_ENCODING = 'FAST_DIFF' , COMPRESSION = 'SNAPPY' );
create index index_3 on LINEITEM (L_COMMITDATE) salt like table HBASE_OPTIONS  ( DATA_BLOCK_ENCODING = 'FAST_DIFF' , COMPRESSION = 'SNAPPY' );
create index index_4 on LINEITEM (L_RECEIPTDATE) salt like table HBASE_OPTIONS  ( DATA_BLOCK_ENCODING = 'FAST_DIFF' , COMPRESSION = 'SNAPPY' );

exit;
EOF


echo "
===== Start ${BENCHMARK} load ${CASEID} (aligned table, 4 index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
echo "
$ODBHOME/odb64luo -u user -p xx -d $DSN -l loadcmd=${LOADCMD}:src=${TESTDATA}/lineitem.tbl:tgt=${CATALOG}.${SCHEMA}.LINEITEM:fs=\|:rows=${ROWSET}:parallel=${PAR}:bad=${LOGDIRECTORY}/$(date +%y%m%d.%H%M).bad.data
"

$ODBHOME/odb64luo -u user -p xx -d $DSN -l loadcmd=${LOADCMD}:src=${TESTDATA}/lineitem.tbl:tgt=${CATALOG}.${SCHEMA}.LINEITEM:fs=\|:rows=${ROWSET}:parallel=${PAR}:bad=${LOGDIRECTORY}/$(date +%y%m%d.%H%M).bad.data 2>&1 | tail -100 | tee ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).load.data.${BENCHMARK}.${CASEID}.log


echo "
===== End ${BENCHMARK} Load ${CASEID} (aligned table, 4 index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

sleep 5



##########################################################################
CASEID=`expr ${CASEID} + 1`
PAR=`expr ${NODES} \* 4`
###    case_${CASEID}: trickleload, aligned table, 4 index, 4N connection
echo "
===== Create ${BENCHMARK} Table ${CASEID} (aligned table, 4 index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
${CI_COMMAND}<<EOF
set statistics on;
log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).create.table.${BENCHMARK}.${CASEID}.log clear;
set schema ${SCHEMA};
drop index index_1;
drop index index_2;
drop index index_3;
drop index index_4;
drop table LINEITEM;
obey ${JAVABENCH_HOME}/sql/create.lineitem.sql;
create index index_1 on LINEITEM (L_ORDERKEY) salt like table HBASE_OPTIONS  ( DATA_BLOCK_ENCODING = 'FAST_DIFF' , COMPRESSION = 'SNAPPY' );
create index index_2 on LINEITEM (L_SHIPDATE) salt like table HBASE_OPTIONS  ( DATA_BLOCK_ENCODING = 'FAST_DIFF' , COMPRESSION = 'SNAPPY' );
create index index_3 on LINEITEM (L_COMMITDATE) salt like table HBASE_OPTIONS  ( DATA_BLOCK_ENCODING = 'FAST_DIFF' , COMPRESSION = 'SNAPPY' );
create index index_4 on LINEITEM (L_RECEIPTDATE) salt like table HBASE_OPTIONS  ( DATA_BLOCK_ENCODING = 'FAST_DIFF' , COMPRESSION = 'SNAPPY' );

exit;
EOF


echo "
===== Start ${BENCHMARK} load ${CASEID} (aligned table, 4 index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
echo "
$ODBHOME/odb64luo -u user -p xx -d $DSN -l loadcmd=${LOADCMD}:src=${TESTDATA}/lineitem.tbl:tgt=${CATALOG}.${SCHEMA}.LINEITEM:fs=\|:rows=${ROWSET}:parallel=${PAR}:bad=${LOGDIRECTORY}/$(date +%y%m%d.%H%M).bad.data
"

$ODBHOME/odb64luo -u user -p xx -d $DSN -l loadcmd=${LOADCMD}:src=${TESTDATA}/lineitem.tbl:tgt=${CATALOG}.${SCHEMA}.LINEITEM:fs=\|:rows=${ROWSET}:parallel=${PAR}:bad=${LOGDIRECTORY}/$(date +%y%m%d.%H%M).bad.data 2>&1 | tail -100 | tee ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).load.data.${BENCHMARK}.${CASEID}.log


echo "
===== End ${BENCHMARK} Load ${CASEID} (aligned table, 4 index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

sleep 5



##########################################################################
CASEID=`expr ${CASEID} + 1`
PAR=`expr ${NODES} \* 8`
###    case_${CASEID}: trickleload, aligned table, 4 index, 8N connection
echo "
===== Create ${BENCHMARK} Table ${CASEID} (aligned table, 4 index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
${CI_COMMAND}<<EOF
set statistics on;
log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).create.table.${BENCHMARK}.${CASEID}.log clear;
set schema ${SCHEMA};
drop index index_1;
drop index index_2;
drop index index_3;
drop index index_4;
drop table LINEITEM;
obey ${JAVABENCH_HOME}/sql/create.lineitem.sql;
create index index_1 on LINEITEM (L_ORDERKEY) salt like table HBASE_OPTIONS  ( DATA_BLOCK_ENCODING = 'FAST_DIFF' , COMPRESSION = 'SNAPPY' );
create index index_2 on LINEITEM (L_SHIPDATE) salt like table HBASE_OPTIONS  ( DATA_BLOCK_ENCODING = 'FAST_DIFF' , COMPRESSION = 'SNAPPY' );
create index index_3 on LINEITEM (L_COMMITDATE) salt like table HBASE_OPTIONS  ( DATA_BLOCK_ENCODING = 'FAST_DIFF' , COMPRESSION = 'SNAPPY' );
create index index_4 on LINEITEM (L_RECEIPTDATE) salt like table HBASE_OPTIONS  ( DATA_BLOCK_ENCODING = 'FAST_DIFF' , COMPRESSION = 'SNAPPY' );

exit;
EOF


echo "
===== Start ${BENCHMARK} load ${CASEID} (aligned table, 4 index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
echo "
$ODBHOME/odb64luo -u user -p xx -d $DSN -l loadcmd=${LOADCMD}:src=${TESTDATA}/lineitem.tbl:tgt=${CATALOG}.${SCHEMA}.LINEITEM:fs=\|:rows=${ROWSET}:parallel=${PAR}:bad=${LOGDIRECTORY}/$(date +%y%m%d.%H%M).bad.data
"

$ODBHOME/odb64luo -u user -p xx -d $DSN -l loadcmd=${LOADCMD}:src=${TESTDATA}/lineitem.tbl:tgt=${CATALOG}.${SCHEMA}.LINEITEM:fs=\|:rows=${ROWSET}:parallel=${PAR}:bad=${LOGDIRECTORY}/$(date +%y%m%d.%H%M).bad.data 2>&1 | tail -100 | tee ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).load.data.${BENCHMARK}.${CASEID}.log


echo "
===== End ${BENCHMARK} Load ${CASEID} (aligned table, 4 index, ${PAR} connection) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

sleep 5



##########################################################################
CASEID=`expr ${CASEID} + 1`
PAR=`expr ${NODES} \* 1`
###    case_${CASEID}: trickleload, aligned table, no index, N connection, 2% error
echo "
===== Create ${BENCHMARK} Table ${CASEID} (aligned table, no index, ${PAR} connection, 2% error) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
${CI_COMMAND}<<EOF
set statistics on;
log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).create.table.${BENCHMARK}.${CASEID}.log clear;
set schema ${SCHEMA};
drop index index_1;
drop index index_2;
drop index index_3;
drop index index_4;
drop table LINEITEM;
obey ${JAVABENCH_HOME}/sql/create.lineitem.sql;

exit;
EOF


echo "
===== Start ${BENCHMARK} load ${CASEID} (aligned table, no index, ${PAR} connection, 2% error) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
echo "
$ODBHOME/odb64luo -u user -p xx -d $DSN -l loadcmd=${LOADCMD}:src=${TESTDATA}/lineitem_2.tbl:tgt=${CATALOG}.${SCHEMA}.LINEITEM:fs=\|:rows=${ROWSET}:parallel=${PAR}:bad=${LOGDIRECTORY}/$(date +%y%m%d.%H%M).bad.data
"

$ODBHOME/odb64luo -u user -p xx -d $DSN -l loadcmd=${LOADCMD}:src=${TESTDATA}/lineitem_2.tbl:tgt=${CATALOG}.${SCHEMA}.LINEITEM:fs=\|:rows=${ROWSET}:parallel=${PAR}:bad=${LOGDIRECTORY}/$(date +%y%m%d.%H%M).bad.data 2>&1 | tail -100 | tee ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).load.data.${BENCHMARK}.${CASEID}.log


echo "
===== End ${BENCHMARK} Load ${CASEID} (aligned table, no index, ${PAR} connection, 2% error) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

sleep 5



##########################################################################
CASEID=`expr ${CASEID} + 1`
PAR=`expr ${NODES} \* 1`
###    case_${CASEID}: trickleload, aligned table, no index, N connection, 5% error
echo "
===== Create ${BENCHMARK} Table ${CASEID} (aligned table, no index, ${PAR} connection, 5% error) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
${CI_COMMAND}<<EOF
set statistics on;
log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).create.table.${BENCHMARK}.${CASEID}.log clear;
set schema ${SCHEMA};
drop table LINEITEM;
obey ${JAVABENCH_HOME}/sql/create.lineitem.sql;

exit;
EOF


echo "
===== Start ${BENCHMARK} load ${CASEID} (aligned table, no index, ${PAR} connection, 5% error) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
echo "
$ODBHOME/odb64luo -u user -p xx -d $DSN -l loadcmd=${LOADCMD}:src=${TESTDATA}/lineitem_5.tbl:tgt=${CATALOG}.${SCHEMA}.LINEITEM:fs=\|:rows=${ROWSET}:parallel=${PAR}:bad=${LOGDIRECTORY}/$(date +%y%m%d.%H%M).bad.data
"

$ODBHOME/odb64luo -u user -p xx -d $DSN -l loadcmd=${LOADCMD}:src=${TESTDATA}/lineitem_5.tbl:tgt=${CATALOG}.${SCHEMA}.LINEITEM:fs=\|:rows=${ROWSET}:parallel=${PAR}:bad=${LOGDIRECTORY}/$(date +%y%m%d.%H%M).bad.data 2>&1 | tail -100 | tee ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).load.data.${BENCHMARK}.${CASEID}.log


echo "
===== End ${BENCHMARK} Load ${CASEID} (aligned table, no index, ${PAR} connection, 5% error) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

} | tee ${LOGFILE}



#####################################################################################
echo "
===== Get test result summary =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
grep -E "Start ${BENCHMARK} load|Target table|Source|Pre-loading time|Loading time|Total records read|Total records inserted|Session Elapsed time" ${LOGFILE} | tee ${SUMMARYFILE}
