USAGE="usage: majorcompact.orderentry.sh
    Flushes the memcache for all tables involved with the orderentry benchmark.
    [ -db|--database|database <<database>> ]
    [ -sch|--schema|schema <<schema>> ]
    [ -sc|--scale|scale <<scalefactor>> ]
"

DATABASE=trafodion
SCHEMA=default
SCALE=${ORDERENTRY_SCALE}


while [[ $# > 0 ]] ; do
key="$1"; shift;
case $key in
    -h|--help|help)			echo -e "$USAGE"; exit 1;;
    *)					echo "Invalid input switch: $key"; echo -e "$USAGE"; exit 1;;
esac
done

case ${DATABASE,,} in
	trafodion)
		DATABASE=Trafodion
		if [[ ${SCHEMA} = "default" ]] ; then
			SCHEMA=$(grep schema ${JAVABENCH_TEST_HOME}/${DATABASE,,}.properties | awk '{ print $3 }')
		fi
		;;

	seaquest)
		DATABASE=SeaQuest
		if [[ ${SCHEMA} = "default" ]] ; then
			SCHEMA=$(grep schema ${JAVABENCH_TEST_HOME}/${DATABASE,,}.properties | awk '{ print $3 }')
		fi
		;;

	hbase)
		DATABASE="Hbase"
		
		if [[ ${SCHEMA} = "default" ]] ; then
			SCHEMA=${BENCHMARK^^}
		fi
		;;
esac

echo "Preparing to major_compact ${SCHEMA^^}.OE_*_${SCALE}"

tableList=$(echo "list" | hbase shell | grep "${SCHEMA^^}" | grep 'OE_' | grep "_${SCALE}")

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
        sleep 5
    done
done
     
exit 0
