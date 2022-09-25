USAGE="usage: flush.table.sh
    Flushes the memcache for all tables involved with a benchmark.
    Options:
      [ -db|--database|database <<database>> ]
      [ -h|--help|help ]
"

DATABASE=trafodion

while [[ $# > 0 ]] ; do
key="$1"; shift;
case $key in
    -db|--database|database)		DATABASE="$1"; shift;;
    -h|--help|help)			echo -e "$USAGE"; exit 1;;
    *)					echo "Invalid input switch: $key"; echo -e "$USAGE"; exit 1;;
esac
done

echo "Preparing to flush ${DATABASE^^}.*"

tableList=$(echo "list '${DATABASE^^}.*'" | hbase shell 2>/dev/null | \
    awk 'BEGIN {want=0}
         /^TABLE/ {want=1; next}
         /row\(s\)/ {exit}
                  {if (want==1) {print $1}}
        ')

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
