USAGE="usage: lineitem.bulkload.sh
    [ -db|--database|database <<database>> ]
    [ -sch|--schema|schema <<schema>> ]
    [ -sc|--scale|scale <<scalefactor>> ] 
    [ -p|--partitions|partitions <<partitions>> ] 
    [ -c|--compress|compress <<compressiontype>> ]
    [ -s|--source|source <<sourcesystem>> ] 
    [ -o|--options|options <<options>> ]
    [ -id|--testid|testid <<testid>> ]
    [ -n|--nodes|nodes <<number of nodes>>]
    [ -d|--debug|debug ]
    [ -h|--help|help ]
"
    
#Default Values
BENCHMARK=bulkload
DATABASE=${JAVABENCH_DEFAULT_DATABASE}
SCHEMA=load_test
SCALE=0
PARTITIONS=$SYSTEM_DEFAULT_PARTITIONS
export TESTID=$(date +%y%m%d.%H%M)
OPTION_COMPRESSION=SNAPPY
#OPTION_COMPRESSION=NONE
SOURCE_SYSTEM=hive

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
    -d|--debug|debug)			OPTION_DEBUG="TRUE";;
    -h|--help|help)			  echo -e "$USAGE"; exit 1;;
    *)					echo "Invalid input switch: $key"; echo -e "$USAGE"; exit 1;;
esac
done

if [ -z "$LOGDIRECTORY" ]; then
export LOGDIRECTORY="${JAVABENCH_TEST_HOME}/log/${TESTID}.lineitem.${BENCHMARK}.${DATABASE}"
if [ ! -f "${LOGDIRECTORY}" ] ; then
mkdir -p ${LOGDIRECTORY}
fi
fi
LOGFILE=${LOGDIRECTORY}/${TESTID}.lineitem.${BENCHMARK}.${DATABASE}.log

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
                 ( logs will be found in ${LOGDIRECTORY} )
"

echo "
===== Start ${BENCHMARK} ( TESTID = ${TESTID} ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
STARTTIME=$SECONDS

if [[ ${SCHEMA} = "default" ]] ; then
SCHEMA=$(grep schema ${JAVABENCH_TEST_HOME}/${DATABASE,,}.properties | awk '{ print $3 }')
fi

###    bulkload no index, regular table
echo "
===== Create ${BENCHMARK} Table 0 ( bulkload no index, regular table ) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
${CI_COMMAND}<<EOF
--set statistics on;
log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).create.table.${BENCHMARK}.0.log clear;
drop schema ${SCHEMA} cascade;
create schema ${SCHEMA};
set schema ${SCHEMA};
drop table LINEITEM;
obey ${JAVABENCH_HOME}/sql/create.lineitem.reg.sql;

exit;
EOF


echo "
===== Start ${BENCHMARK} load 0 ( bulkload no index, regular table ) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

${CI_COMMAND}<<EOF
log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).load.data.${BENCHMARK}.0.log;
set schema $SCHEMA;
control query default COMP_BOOL_226 'ON';
CQD HIVE_MAX_STRING_LENGTH '100';
CQD HBASE_ROWSET_VSBB_SIZE '20000';
obey ${JAVABENCH_HOME}/sql/explain.bulkload.lineitem.sql;
set statistics on;
obey ${JAVABENCH_HOME}/sql/bulkload.lineitem.sql;

exit;
EOF

echo "
===== End ${BENCHMARK} Load 0 ( bulkload no index, regular table ) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

###    bulkload no index
echo "
===== Create ${BENCHMARK} Table 1 ( bulkload no index, aligned table ) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
${CI_COMMAND}<<EOF
--set statistics on;
log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).create.table.${BENCHMARK}.1.log clear;
set schema ${SCHEMA};
drop table LINEITEM;
obey ${JAVABENCH_HOME}/sql/create.lineitem.sql;

exit;
EOF


echo "
===== Start ${BENCHMARK} load 1 ( bulkload no index, aligned table ) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

${CI_COMMAND}<<EOF
log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).load.data.${BENCHMARK}.1.log;
set schema $SCHEMA;
control query default COMP_BOOL_226 'ON';
CQD HIVE_MAX_STRING_LENGTH '100';
CQD HBASE_ROWSET_VSBB_SIZE '20000';
obey ${JAVABENCH_HOME}/sql/explain.bulkload.lineitem.sql;
set statistics on;
obey ${JAVABENCH_HOME}/sql/bulkload.lineitem.sql;

exit;
EOF

echo "
===== End ${BENCHMARK} Load 1 ( bulkload no index, aligned table ) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

###    bulkload no index with 2% error
echo "
===== Create ${BENCHMARK} Table 2 ( bulkload no index, aligned table, with 2% error records ) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
${CI_COMMAND}<<EOF
--set statistics on;
log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).create.table.${BENCHMARK}.2.log clear;
set schema ${SCHEMA};
drop table LINEITEM;
obey ${JAVABENCH_HOME}/sql/create.lineitem.sql;

exit;
EOF

echo "
===== Start ${BENCHMARK} load 2 ( bulkload no index, aligned table, with 2% error records ) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

${CI_COMMAND}<<EOF
log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).load.data.${BENCHMARK}.2.log;
set schema $SCHEMA;
control query default COMP_BOOL_226 'ON';
CQD HIVE_MAX_STRING_LENGTH '100';
CQD HBASE_ROWSET_VSBB_SIZE '20000';
obey ${JAVABENCH_HOME}/sql/explain.bulkload.lineitem_2.sql;
set statistics on;
obey ${JAVABENCH_HOME}/sql/bulkload.lineitem.2.sql;

exit;
EOF

echo "
===== End ${BENCHMARK} Load 2 ( bulkload no index, aligned table, with 2% error records ) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

###    bulkload no index with 5% error
echo "
===== Create ${BENCHMARK} Table 3 ( bulkload no index, aligned table, with 5% error records ) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
${CI_COMMAND}<<EOF
--set statistics on;
log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).create.table.${BENCHMARK}.3.log clear;
set schema ${SCHEMA};
drop table LINEITEM;
obey ${JAVABENCH_HOME}/sql/create.lineitem.sql;

exit;
EOF

echo "
===== Start ${BENCHMARK} load 3 ( bulkload no index, aligned table, with 5% error records ) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

${CI_COMMAND}<<EOF
log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).load.data.${BENCHMARK}.3.log;
set schema $SCHEMA;
control query default COMP_BOOL_226 'ON';
CQD HIVE_MAX_STRING_LENGTH '100';
CQD HBASE_ROWSET_VSBB_SIZE '20000';
obey ${JAVABENCH_HOME}/sql/explain.bulkload.lineitem_5.sql;
set statistics on;
obey ${JAVABENCH_HOME}/sql/bulkload.lineitem.5.sql;

exit;
EOF

echo "
===== End ${BENCHMARK} Load 3 ( bulkload no index, aligned table, with 5% error records ) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"


###    bulkload no index with CQD HIVE_NUM_ESPS_PER_DATANODE '3'
echo "
===== Create ${BENCHMARK} Table 4 ( bulkload no index, aligned table, with CQD HIVE_NUM_ESPS_PER_DATANODE '3' ) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
##cqd_var=`expr ${NODES} \* 4`
${CI_COMMAND}<<EOF
--set statistics on;
log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).create.table.${BENCHMARK}.4.log clear;
set schema ${SCHEMA};
drop table LINEITEM;
obey ${JAVABENCH_HOME}/sql/create.lineitem.sql;

exit;
EOF

echo "
===== Start ${BENCHMARK} load 4 ( bulkload no index, aligned table, with CQD HIVE_NUM_ESPS_PER_DATANODE '3' ) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

${CI_COMMAND}<<EOF

log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).load.data.${BENCHMARK}.4.log;
set schema $SCHEMA;
--CQD HIVE_MAX_ESPS '$cqd_var';
control query default COMP_BOOL_226 'ON';
CQD HIVE_MAX_STRING_LENGTH '100';
CQD HBASE_ROWSET_VSBB_SIZE '20000';
CQD HIVE_NUM_ESPS_PER_DATANODE '3';
obey ${JAVABENCH_HOME}/sql/explain.bulkload.lineitem.sql;
set statistics on;
obey ${JAVABENCH_HOME}/sql/bulkload.lineitem.sql;

exit;
EOF

echo "
===== End ${BENCHMARK} load 4 ( bulkload no index, aligned table, with CQD HIVE_NUM_ESPS_PER_DATANODE '3' ) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"


###    bulkload no index with CQD HIVE_NUM_ESPS_PER_DATANODE '4'
echo "
===== Create ${BENCHMARK} Table 5 ( bulkload no index, aligned table, with CQD HIVE_NUM_ESPS_PER_DATANODE '4' ) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
##cqd_var=`expr ${NODES} \* 5`
${CI_COMMAND}<<EOF
--set statistics on;
log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).create.table.${BENCHMARK}.5.log clear;
set schema ${SCHEMA};
drop table LINEITEM;
obey ${JAVABENCH_HOME}/sql/create.lineitem.sql;

exit;
EOF

echo "
===== Start ${BENCHMARK} load 5 ( bulkload no index, aligned table, with CQD HIVE_NUM_ESPS_PER_DATANODE '4' ) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

${CI_COMMAND}<<EOF

log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).load.data.${BENCHMARK}.5.log;
set schema $SCHEMA;
--CQD HIVE_MAX_ESPS '$cqd_var';
control query default COMP_BOOL_226 'ON';
CQD HIVE_MAX_STRING_LENGTH '100';
CQD HBASE_ROWSET_VSBB_SIZE '20000';
CQD HIVE_NUM_ESPS_PER_DATANODE '4';
obey ${JAVABENCH_HOME}/sql/explain.bulkload.lineitem.sql;
set statistics on;
obey ${JAVABENCH_HOME}/sql/bulkload.lineitem.sql;

exit;
EOF

echo "
===== End ${BENCHMARK} load 5 ( bulkload no index, aligned table, with CQD HIVE_NUM_ESPS_PER_DATANODE '4' ) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"


###    bulkload no index with CQD HIVE_NUM_ESPS_PER_DATANODE '5'
echo "
===== Create ${BENCHMARK} Table 6 ( bulkload no index, aligned table, with CQD HIVE_NUM_ESPS_PER_DATANODE '5' ) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
##cqd_var=`expr ${NODES} \* 6`
${CI_COMMAND}<<EOF
--set statistics on;
log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).create.table.${BENCHMARK}.6.log clear;
set schema ${SCHEMA};
drop table LINEITEM;
obey ${JAVABENCH_HOME}/sql/create.lineitem.sql;

exit;
EOF

echo "
===== Start ${BENCHMARK} load 6 ( bulkload no index, aligned table, with CQD HIVE_NUM_ESPS_PER_DATANODE '5' ) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

${CI_COMMAND}<<EOF
log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).load.data.${BENCHMARK}.6.log;
set schema $SCHEMA;
--CQD HIVE_MAX_ESPS '$cqd_var';
control query default COMP_BOOL_226 'ON';
CQD HIVE_MAX_STRING_LENGTH '100';
CQD HBASE_ROWSET_VSBB_SIZE '20000';
CQD HIVE_NUM_ESPS_PER_DATANODE '5';
obey ${JAVABENCH_HOME}/sql/explain.bulkload.lineitem.sql;
set statistics on;
obey ${JAVABENCH_HOME}/sql/bulkload.lineitem.sql;

exit;
EOF

echo "
===== End ${BENCHMARK} Load 6 ( bulkload no index, aligned table, with CQD HIVE_NUM_ESPS_PER_DATANODE '5' ) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"


###    bulkload no index with CQD HIVE_NUM_ESPS_PER_DATANODE '6'
echo "
===== Create ${BENCHMARK} Table 7 ( bulkload no index, aligned table, with CQD HIVE_NUM_ESPS_PER_DATANODE '6' ) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
##cqd_var=`expr ${NODES} \* 6`
${CI_COMMAND}<<EOF
--set statistics on;
log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).create.table.${BENCHMARK}.7.log clear;
set schema ${SCHEMA};
drop table LINEITEM;
obey ${JAVABENCH_HOME}/sql/create.lineitem.sql;

exit;
EOF

echo "
===== Start ${BENCHMARK} load 7 ( bulkload no index, aligned table, with CQD HIVE_NUM_ESPS_PER_DATANODE '6' ) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

${CI_COMMAND}<<EOF
log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).load.data.${BENCHMARK}.7.log;
set schema $SCHEMA;
--CQD HIVE_MAX_ESPS '$cqd_var';
control query default COMP_BOOL_226 'ON';
CQD HIVE_MAX_STRING_LENGTH '100';
CQD HBASE_ROWSET_VSBB_SIZE '20000';
CQD HIVE_NUM_ESPS_PER_DATANODE '6';
obey ${JAVABENCH_HOME}/sql/explain.bulkload.lineitem.sql;
set statistics on;
obey ${JAVABENCH_HOME}/sql/bulkload.lineitem.sql;

exit;
EOF

echo "
===== End ${BENCHMARK} Load 7 ( bulkload no index, aligned table, with CQD HIVE_NUM_ESPS_PER_DATANODE '6' ) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"



###    bulkload with CQD TRAF_LOAD_ALLOW_RISKY_INDEX_MAINTENANCE 'on', index 1
echo "
===== Create ${BENCHMARK} Table 8 ( bulkload 1 index, aligned table, with CQD TRAF_LOAD_ALLOW_RISKY_INDEX_MAINTENANCE 'on' ) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
${CI_COMMAND}<<EOF
--set statistics on;
log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).create.table.${BENCHMARK}.8.log clear;
set schema ${SCHEMA};
drop table LINEITEM;
obey ${JAVABENCH_HOME}/sql/create.lineitem.sql;
create index index_1 on LINEITEM (L_ORDERKEY) salt like table HBASE_OPTIONS  ( DATA_BLOCK_ENCODING = 'FAST_DIFF' , COMPRESSION = 'SNAPPY' );

exit;
EOF

echo "
===== Start ${BENCHMARK} load 8 ( bulkload 1 index, aligned table, with CQD TRAF_LOAD_ALLOW_RISKY_INDEX_MAINTENANCE 'on' ) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

${CI_COMMAND}<<EOF

log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).load.data.${BENCHMARK}.8.log;
set schema $SCHEMA;
control query default COMP_BOOL_226 'ON';
CQD HIVE_MAX_STRING_LENGTH '100';
CQD HBASE_ROWSET_VSBB_SIZE '20000';
CQD TRAF_LOAD_ALLOW_RISKY_INDEX_MAINTENANCE 'ON';
obey ${JAVABENCH_HOME}/sql/explain.bulkload.lineitem.sql;
set statistics on;
obey ${JAVABENCH_HOME}/sql/bulkload.lineitem.sql;

exit;
EOF

echo "
===== End ${BENCHMARK} Load 8 ( bulkload 1 index, aligned table, with CQD TRAF_LOAD_ALLOW_RISKY_INDEX_MAINTENANCE 'on' ) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

###    bulkload with CQD TRAF_LOAD_ALLOW_RISKY_INDEX_MAINTENANCE 'on', index 2
echo "
===== Create ${BENCHMARK} Table 9 ( bulkload 2 indexes, aligned table, with CQD TRAF_LOAD_ALLOW_RISKY_INDEX_MAINTENANCE 'on' ) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
${CI_COMMAND}<<EOF
--set statistics on;
log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).create.table.${BENCHMARK}.9.log clear;
set schema ${SCHEMA};
drop index index_1;
drop table LINEITEM;
obey ${JAVABENCH_HOME}/sql/create.lineitem.sql;
create index index_1 on LINEITEM (L_ORDERKEY) salt like table HBASE_OPTIONS  ( DATA_BLOCK_ENCODING = 'FAST_DIFF' , COMPRESSION = 'SNAPPY' );
create index index_2 on LINEITEM (L_SHIPDATE) salt like table HBASE_OPTIONS  ( DATA_BLOCK_ENCODING = 'FAST_DIFF' , COMPRESSION = 'SNAPPY' );

exit;
EOF

echo "
===== Start ${BENCHMARK} load 9 ( bulkload 2 indexes, aligned table, with CQD TRAF_LOAD_ALLOW_RISKY_INDEX_MAINTENANCE 'on' ) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

${CI_COMMAND}<<EOF

log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).load.data.${BENCHMARK}.9.log;
set schema $SCHEMA;
control query default COMP_BOOL_226 'ON';
CQD HIVE_MAX_STRING_LENGTH '100';
CQD HBASE_ROWSET_VSBB_SIZE '20000';
CQD TRAF_LOAD_ALLOW_RISKY_INDEX_MAINTENANCE 'ON';
obey ${JAVABENCH_HOME}/sql/explain.bulkload.lineitem.sql;
set statistics on;
obey ${JAVABENCH_HOME}/sql/bulkload.lineitem.sql;

exit;
EOF

echo "
===== End ${BENCHMARK} Load 9 ( bulkload 2 indexes, aligned table, with CQD TRAF_LOAD_ALLOW_RISKY_INDEX_MAINTENANCE 'on' ) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

###    bulkload with CQD TRAF_LOAD_ALLOW_RISKY_INDEX_MAINTENANCE 'on', index 4
echo "
===== Create ${BENCHMARK} Table 10 ( bulkload 4 indexes, aligned table, with CQD TRAF_LOAD_ALLOW_RISKY_INDEX_MAINTENANCE 'on' ) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
${CI_COMMAND}<<EOF
--set statistics on;
log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).create.table.${BENCHMARK}.10.log clear;
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
===== Start ${BENCHMARK} load 10 ( bulkload 4 indexes, aligned table, with CQD TRAF_LOAD_ALLOW_RISKY_INDEX_MAINTENANCE 'on' ) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

${CI_COMMAND}<<EOF

log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).load.data.${BENCHMARK}.10.log;
set schema $SCHEMA;
control query default COMP_BOOL_226 'ON';
CQD HIVE_MAX_STRING_LENGTH '100';
CQD HBASE_ROWSET_VSBB_SIZE '20000';
CQD TRAF_LOAD_ALLOW_RISKY_INDEX_MAINTENANCE 'ON';
obey ${JAVABENCH_HOME}/sql/explain.bulkload.lineitem.sql;
set statistics on;
obey ${JAVABENCH_HOME}/sql/bulkload.lineitem.sql;

exit;
EOF

echo "
===== End ${BENCHMARK} Load 10 ( bulkload 4 indexes, aligned table, with CQD TRAF_LOAD_ALLOW_RISKY_INDEX_MAINTENANCE 'on' ) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

###    bulkload with CQD TRAF_LOAD_ALLOW_RISKY_INDEX_MAINTENANCE 'off', index 1
echo "
===== Create ${BENCHMARK} Table 11 ( bulkload 1 index, aligned table, with CQD TRAF_LOAD_ALLOW_RISKY_INDEX_MAINTENANCE 'off' ) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
${CI_COMMAND}<<EOF
--set statistics on;
log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).create.table.${BENCHMARK}.11.log clear;
set schema ${SCHEMA};
drop index index_1;
drop index index_2;
drop index index_3;
drop index index_4;
drop table LINEITEM;
obey ${JAVABENCH_HOME}/sql/create.lineitem.sql;
create index index_1 on LINEITEM (L_ORDERKEY) salt like table HBASE_OPTIONS  ( DATA_BLOCK_ENCODING = 'FAST_DIFF' , COMPRESSION = 'SNAPPY' );

exit;
EOF

echo "
===== Start ${BENCHMARK} load 11 ( bulkload 1 index, aligned table, with CQD TRAF_LOAD_ALLOW_RISKY_INDEX_MAINTENANCE 'off' ) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

${CI_COMMAND}<<EOF

log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).load.data.${BENCHMARK}.11.log;
set schema $SCHEMA;
control query default COMP_BOOL_226 'ON';
CQD HIVE_MAX_STRING_LENGTH '100';
CQD HBASE_ROWSET_VSBB_SIZE '20000';
CQD TRAF_LOAD_ALLOW_RISKY_INDEX_MAINTENANCE 'OFF';
obey ${JAVABENCH_HOME}/sql/explain.bulkload.lineitem.sql;
set statistics on;
obey ${JAVABENCH_HOME}/sql/bulkload.lineitem.sql;

exit;
EOF

echo "
===== End ${BENCHMARK} Load 11 ( bulkload 1 index, aligned table, with CQD TRAF_LOAD_ALLOW_RISKY_INDEX_MAINTENANCE 'off' ) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

###    bulkload with CQD TRAF_LOAD_ALLOW_RISKY_INDEX_MAINTENANCE 'off', index 2
echo "
===== Create ${BENCHMARK} Table 12 ( bulkload 2 indexes, aligned table, with CQD TRAF_LOAD_ALLOW_RISKY_INDEX_MAINTENANCE 'off' ) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
${CI_COMMAND}<<EOF
--set statistics on;
log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).create.table.${BENCHMARK}.12.log clear;
set schema ${SCHEMA};
drop index index_1;
drop table LINEITEM;
obey ${JAVABENCH_HOME}/sql/create.lineitem.sql;
create index index_1 on LINEITEM (L_ORDERKEY) salt like table HBASE_OPTIONS  ( DATA_BLOCK_ENCODING = 'FAST_DIFF' , COMPRESSION = 'SNAPPY' );
create index index_2 on LINEITEM (L_SHIPDATE) salt like table HBASE_OPTIONS  ( DATA_BLOCK_ENCODING = 'FAST_DIFF' , COMPRESSION = 'SNAPPY' );

exit;
EOF

echo "
===== Start ${BENCHMARK} load 12 ( bulkload 2 indexes, aligned table, with CQD TRAF_LOAD_ALLOW_RISKY_INDEX_MAINTENANCE 'off' ) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

${CI_COMMAND}<<EOF

log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).load.data.${BENCHMARK}.12.log;
set schema $SCHEMA;
control query default COMP_BOOL_226 'ON';
CQD HIVE_MAX_STRING_LENGTH '100';
CQD HBASE_ROWSET_VSBB_SIZE '20000';
CQD TRAF_LOAD_ALLOW_RISKY_INDEX_MAINTENANCE 'OFF';
obey ${JAVABENCH_HOME}/sql/explain.bulkload.lineitem.sql;
set statistics on;
obey ${JAVABENCH_HOME}/sql/bulkload.lineitem.sql;

exit;
EOF

echo "
===== End ${BENCHMARK} Load 12 ( bulkload 2 indexes, aligned table, with CQD TRAF_LOAD_ALLOW_RISKY_INDEX_MAINTENANCE 'off' ) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

###    bulkload with CQD TRAF_LOAD_ALLOW_RISKY_INDEX_MAINTENANCE 'off', index 4
echo "
===== Create ${BENCHMARK} Table 13 ( bulkload 4 indexes, aligned table, with CQD TRAF_LOAD_ALLOW_RISKY_INDEX_MAINTENANCE 'off' ) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
${CI_COMMAND}<<EOF
--set statistics on;
log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).create.table.${BENCHMARK}.13.log clear;
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
===== Start ${BENCHMARK} load 13 ( bulkload 4 indexes, aligned table, with CQD TRAF_LOAD_ALLOW_RISKY_INDEX_MAINTENANCE 'off' ) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

${CI_COMMAND}<<EOF

log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).load.data.${BENCHMARK}.13.log;
set schema $SCHEMA;
control query default COMP_BOOL_226 'ON';
CQD HIVE_MAX_STRING_LENGTH '100';
CQD HBASE_ROWSET_VSBB_SIZE '20000';
CQD TRAF_LOAD_ALLOW_RISKY_INDEX_MAINTENANCE 'OFF';
obey ${JAVABENCH_HOME}/sql/explain.bulkload.lineitem.sql;
set statistics on;
obey ${JAVABENCH_HOME}/sql/bulkload.lineitem.sql;

exit;
EOF

echo "
===== End ${BENCHMARK} Load 13 ( bulkload 4 indexes, aligned table, with CQD TRAF_LOAD_ALLOW_RISKY_INDEX_MAINTENANCE 'off' ) ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

} | tee ${LOGFILE}