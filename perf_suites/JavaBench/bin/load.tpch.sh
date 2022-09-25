USAGE="usage: load.tpch.sh
    [ -db|--database|database <<database>> ]
    [ -sch|--schema|schema <<schema>> ]
    [ -sc|--scale|scale <<scalefactor>> ] 
    [ -p|--partitions|partitions <<partitions>> ] 
    [ -c|--compress|compress <<compressiontype>> ]
    [ -s|--source|source <<sourcesystem>> ] 
    [ -o|--options|options <<options>> ]
    [ -id|--testid|testid <<testid>> ]
    [ -d|--debug|debug ]
    [ -h|--help|help ]
"
    
#Default Values
BENCHMARK=tpch
DATABASE=${JAVABENCH_DEFAULT_DATABASE}
SCHEMA=default
SCALE=100
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
    -d|--debug|debug)			OPTION_DEBUG="TRUE";;
    -h|--help|help)			echo -e "$USAGE"; exit 1;;
    *)					echo "Invalid input switch: $key"; echo -e "$USAGE"; exit 1;;
esac
done

if [ -z "$LOGDIRECTORY" ]; then
export LOGDIRECTORY="${JAVABENCH_TEST_HOME}/log/${TESTID}.load.${BENCHMARK}.${DATABASE}"
if [ ! -f "${LOGDIRECTORY}" ] ; then
mkdir -p ${LOGDIRECTORY}
fi
fi
LOGFILE=${LOGDIRECTORY}/${TESTID}.load.${BENCHMARK}.${DATABASE}.log

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
             DEBUG = ${OPTION_DEBUG}
           OPTIONS = ${OPTIONS}
                 ( logs will be found in ${LOGDIRECTORY} )
"

echo "
===== Start ${BENCHMARK} Load ( TESTID = ${TESTID} ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
STARTTIME=$SECONDS

if [[ ${SCHEMA} = "default" ]] ; then
SCHEMA=$(grep schema ${JAVABENCH_TEST_HOME}/${DATABASE,,}.properties | awk '{ print $3 }')
fi

echo "
===== Create ${BENCHMARK} Tables ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
${CI_COMMAND}<<EOF
set statistics on;
log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).create.tables.${BENCHMARK}.log clear;
set schema $SCHEMA;

obey ${JAVABENCH_HOME}/sql/drop.${BENCHMARK}.${SCALE}.sql;

create schema ${SCHEMA};

create table TPCH_CUSTOMER_${SCALE} ( C_CUSTKEY INT NO DEFAULT NOT NULL , C_NAME CHAR(25) NO DEFAULT NOT NULL , C_ADDRESS CHAR(40) NO DEFAULT NOT NULL , C_NATIONKEY INT NO DEFAULT NOT NULL , C_PHONE CHAR(15) NO DEFAULT NOT NULL , C_ACCTBAL FLOAT(54) NO DEFAULT NOT NULL , C_MKTSEGMENT CHAR(10) NO DEFAULT NOT NULL , C_COMMENT VARCHAR(117) NO DEFAULT NOT NULL , PRIMARY KEY (C_CUSTKEY) )
 salt using ${PARTITIONS} partitions attribute aligned format
 hbase_options ( compression = '${OPTION_COMPRESSION}', data_block_encoding = 'FAST_DIFF')
 ;
create table TPCH_LINEITEM_${SCALE} (   L_ORDERKEY LARGEINT NO DEFAULT NOT NULL , L_PARTKEY INT NO DEFAULT NOT NULL , L_SUPPKEY INT NO DEFAULT NOT NULL , L_LINENUMBER INT NO DEFAULT NOT NULL , L_QUANTITY FLOAT(54) NO DEFAULT NOT NULL , L_EXTENDEDPRICE FLOAT(54) NO DEFAULT NOT NULL , L_DISCOUNT FLOAT(54) NO DEFAULT NOT NULL , L_TAX FLOAT(54) NO DEFAULT NOT NULL , L_RETURNFLAG CHAR(1) NO DEFAULT NOT NULL , L_LINESTATUS CHAR(1) NO DEFAULT NOT NULL , L_SHIPDATE CHAR(10) NO DEFAULT NOT NULL , L_COMMITDATE CHAR(10) NO DEFAULT NOT NULL , L_RECEIPTDATE CHAR(10) NO DEFAULT NOT NULL , L_SHIPINSTRUCT CHAR(25) NO DEFAULT NOT NULL , L_SHIPMODE CHAR(10) NO DEFAULT NOT NULL , L_COMMENT VARCHAR(44) NO DEFAULT NOT NULL , PRIMARY KEY (L_SHIPDATE, L_ORDERKEY, L_LINENUMBER) )
 salt using ${PARTITIONS} partitions on (L_ORDERKEY) attribute aligned format
 hbase_options ( compression = '${OPTION_COMPRESSION}', data_block_encoding = 'FAST_DIFF')
 ;
create table TPCH_ORDERS_${SCALE} ( O_ORDERKEY LARGEINT NO DEFAULT NOT NULL , O_CUSTKEY INT NO DEFAULT NOT NULL , O_ORDERSTATUS CHAR(1) NO DEFAULT NOT NULL , O_TOTALPRICE FLOAT(54) NO DEFAULT NOT NULL , O_ORDERDATE CHAR(10) NO DEFAULT NOT NULL , O_ORDERPRIORITY CHAR(15) NO DEFAULT NOT NULL , O_CLERK CHAR(15) NO DEFAULT NOT NULL , O_SHIPPRIORITY INT NO DEFAULT NOT NULL , O_COMMENT VARCHAR(79) NO DEFAULT NOT NULL , PRIMARY KEY (O_ORDERDATE, O_ORDERKEY) )
 salt using ${PARTITIONS} partitions attribute aligned format
 hbase_options ( compression = '${OPTION_COMPRESSION}', data_block_encoding = 'FAST_DIFF')
 ;
create table TPCH_PART_${SCALE} (   P_PARTKEY INT NO DEFAULT NOT NULL , P_NAME CHAR(55) NO DEFAULT NOT NULL , P_MFGR CHAR(25) NO DEFAULT NOT NULL , P_BRAND CHAR(10) NO DEFAULT NOT NULL , P_TYPE VARCHAR(25) NO DEFAULT NOT NULL , P_SIZE INT NO DEFAULT NOT NULL , P_CONTAINER CHAR(10) NO DEFAULT NOT NULL , P_RETAILPRICE FLOAT(54) NO DEFAULT NOT NULL , P_COMMENT VARCHAR(23) NO DEFAULT NOT NULL , PRIMARY KEY (P_PARTKEY) )
 salt using ${PARTITIONS} partitions attribute aligned format
 hbase_options ( compression = '${OPTION_COMPRESSION}', data_block_encoding = 'FAST_DIFF')
 ;
create table TPCH_PARTSUPP_${SCALE} ( PS_PARTKEY INT NO DEFAULT NOT NULL , PS_SUPPKEY INT NO DEFAULT NOT NULL , PS_AVAILQTY INT NO DEFAULT NOT NULL , PS_SUPPLYCOST FLOAT(54) NO DEFAULT NOT NULL , PS_COMMENT VARCHAR(199) NO DEFAULT NOT NULL , PRIMARY KEY (PS_PARTKEY, PS_SUPPKEY) )
 salt using ${PARTITIONS} partitions attribute aligned format
 hbase_options ( compression = '${OPTION_COMPRESSION}', data_block_encoding = 'FAST_DIFF')
 ;
create table TPCH_REGION_${SCALE} ( R_REGIONKEY INT NO DEFAULT NOT NULL , R_NAME CHAR(25) NO DEFAULT NOT NULL , R_COMMENT VARCHAR(152) NO DEFAULT NOT NULL , PRIMARY KEY (R_REGIONKEY) ) attribute aligned format
 hbase_options ( compression = '${OPTION_COMPRESSION}', data_block_encoding = 'FAST_DIFF')
 ;
create table TPCH_SUPPLIER_${SCALE} ( S_SUPPKEY INT NO DEFAULT NOT NULL , S_NAME CHAR(25) NO DEFAULT NOT NULL , S_ADDRESS CHAR(40) NO DEFAULT NOT NULL , S_NATIONKEY INT NO DEFAULT NOT NULL , S_PHONE CHAR(15) NO DEFAULT NOT NULL , S_ACCTBAL FLOAT(54) NO DEFAULT NOT NULL , S_COMMENT VARCHAR(101) NO DEFAULT NOT NULL , PRIMARY KEY (S_SUPPKEY) )
 salt using ${PARTITIONS} partitions attribute aligned format
 hbase_options ( compression = '${OPTION_COMPRESSION}', data_block_encoding = 'FAST_DIFF')
 ;
create table TPCH_NATION_${SCALE} ( N_NATIONKEY INT NO DEFAULT NOT NULL , N_NAME CHAR(25) NO DEFAULT NOT NULL , N_REGIONKEY INT NO DEFAULT NOT NULL , N_COMMENT VARCHAR(152) NO DEFAULT NOT NULL , PRIMARY KEY (N_NATIONKEY) ) attribute aligned format
 hbase_options ( compression = '${OPTION_COMPRESSION}', data_block_encoding = 'FAST_DIFF')
 ;

exit;
EOF

echo "
===== Load ${BENCHMARK} Data ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

if [[ ${SOURCE_SYSTEM} = hive ]] ; then

${CI_COMMAND}<<EOF
set statistics on;
log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).load.data.${BENCHMARK}.log;
set schema $SCHEMA;

obey ${JAVABENCH_HOME}/sql/loadfromhive.${BENCHMARK}.${SCALE}.sql;

exit;
EOF

else 

#SMALL TABLES
for TABLE in \
	tpch_customer_${SCALE} \
	tpch_lineitem_${SCALE} \
	tpch_nation_${SCALE} \
	tpch_orders_${SCALE} \
	tpch_part_${SCALE} \
	tpch_partsupp_${SCALE} \
	tpch_region_${SCALE} \
	tpch_supplier_${SCALE} \
    ; do
{

java -Ddbconnect.source.properties=${SOURCE_SYSTEM}.properties \
    -Ddbconnect.destination.properties=${DATABASE,,}.properties \
    CopyData source_table javabench.${TABLE} destination_table ${SCHEMA}.${TABLE} \
    streams 128 
    
} | tee ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).copy.data.${TABLE}.log 
done

fi

echo "
===== Update ${BENCHMARK} Statistics ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
${CI_COMMAND}<<EOF
set statistics on;
log ${LOGDIRECTORY}/$(date +%y%m%d.%H%M).updatestats.${BENCHMARK}.log clear;
set schema $SCHEMA;

obey ${JAVABENCH_HOME}/sql/updatestats.${BENCHMARK}.${SCALE}.sql;

exit;
EOF

echo "
===== End ${BENCHMARK} Load ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

} | tee ${LOGFILE}
