USAGE="usage: majorcompact.benchmark.sh
    Performs a major_compact for all tables involved with the orderentry benchmark.
    -bm|--benchmark|benchmark <<benchmark>>
    [ -db|--database|database <<database>> ]
    [ -sch|--schema|schema <<schema>> ]
    [ -sc|--scale|scale <<scalefactor>> ]
    
    Supported Benchmarks are 
        YCSB, DebitCredit, OrderEntry, TPCH, ED
        
    Supported Databases are
        Trafodion, Hbase, Phoenix
"

BENCHMARK="default"
DATABASE="default"
SCHEMA="default"
SCALE="default"

while [[ $# > 0 ]] ; do
key="$1"; shift;
case ${key,,} in
    -bm|--benchmark|benchmark)		BENCHMARK="$1"; shift;;
    -db|--database|database)		DATABASE="$1"; shift;;
    -sch|--schema|schema)		SCHEMA="$1"; shift;;
    -sc|--scale|scale)			SCALE="$1"; shift;;
    -h|--help|help)			echo -e "$USAGE"; exit 1;;
    *)					echo "Invalid input switch: $key"; echo -e "$USAGE"; exit 1;;
esac
done

if [[ ${DATABASE} = "default" ]] ; then
DATABASE=${JAVABENCH_DEFAULT_DATABASE};
fi

case ${BENCHMARK,,} in

    ycsb)
    	PREFIX="YCSB_"
	if [[ ${SCALE} = "default" ]] ; then
	    SCALE=${YCSB_SCALE}
	fi
	SUFFIX="_${SCALE}"
 	;;

    dc|debitcredit)
    	PREFIX="DC_"
	if [[ ${SCALE} = "default" ]] ; then
	    SCALE=${DEBITCREDIT_SCALE}
	fi
	SUFFIX="_${SCALE}"
	;;

    oe|orderentry)
    	PREFIX="OE_"
	if [[ ${SCALE} = "default" ]] ; then
	    SCALE=${ORDERENTRY_SCALE}
	fi
	SUFFIX="_${SCALE}"
	;;

    tpch)
    	PREFIX="TPCH_"
	if [[ ${SCALE} = "default" ]] ; then
	    SCALE=${TPCH_SCALE}
	fi
	SUFFIX="_${SCALE}"
	;;

    ed)
    	PREFIX="ED_TABLE_"
	SUFFIX=""
	;;

    *)		
	echo "Invalid Benchmark Specified. BENCHMARK = ${BENCHMARK}."; 
	echo -e "$USAGE"; exit 1;;

esac

case ${DATABASE,,} in
	trafodion|traf)
		if [[ ${SCHEMA} = "default" ]] ; then
			SCHEMA=$(grep schema ${JAVABENCH_TEST_HOME}/${DATABASE,,}.properties | awk '{ print $3 }')
		fi
		;;
	hbase)
		if [[ ${SCHEMA} = "default" ]] ; then
			SCHEMA=${BENCHMARK^^}
		fi
		;;
	seaquest)
		echo "Compaction not applicable for SeaQuest."; 
		exit 1;;
	phoenix)
		if [[ ${SCHEMA} = "default" ]] ; then
			SCHEMA="PHOENIX"
		fi
		;;
	
	*)		
		echo "Invalid Database Specified. DATABASE = ${DATABASE}."; 
		echo -e "$USAGE"; exit 1;;
esac

echo "Preparing to major_compact ${SCHEMA^^}.${PREFIX^^}*${SUFFIX^^}"

tableList=$(echo "list" | hbase shell | grep "^${SCHEMA^^}" | grep "${PREFIX^^}" | grep "${SUFFIX^^}")

if [ ${#tableList} -eq 0 ] ; then
    echo "No tables were found."
    exit 0
fi

compTableList=$(echo "${tableList}" | sed -e 's/^/major_compact \x27/g' -e 's/$/\x27/g')
echo "${compTableList}" | hbase shell

echo ""
echo "Now verifying that all tables have been flushed."
echo ""

for currTable in ${tableList}
do
    while true ; do
        currStat=$(curl -sS --noproxy '*' http://${SYSTEM_NAMEMGR_URL}:${SYSTEM_NAMEMGR_PORT}/table.jsp?name=${currTable} 2>/dev/null | head -100 | \
            awk 'BEGIN {want=0; lastStat=""}
                 /Table Attributes/ {want=1;next}
                 /Compaction/ {if (want==1) {want=2; next}}
                 /\/td/ {if (want==2) {print lastStat; exit}}
                 {if (want==2) {if (index($0,"NONE") > 0) {print "NONE"; exit} else {if (length($1)>0) {lastStat=$1}}}}
                ')
        echo "Table: ${currTable} - compaction: ${currStat}"
        if [ X${currStat} == XNONE ] ; then
            break
        fi
        sleep 10
    done
done
     
exit 0
