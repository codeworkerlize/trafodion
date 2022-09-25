USAGE="usage: majorcompact.table.sh -tbl|--table|table <<table>>
    Triggers a major compaction for specified table.
    Options:
      [ -db|--database|database <<database>> ]
      [ -sch|--schema|schema <<schema>> ]
      [ -h|--help|help ]
"

DATABASE=default
SCHEMA=default
TABLE=default

while [[ $# > 0 ]] ; do
key="$1"; shift;
case $key in
    -db|--database|database)		DATABASE="$1"; shift;;
    -sch|--schema|schema)		SCHEMA="$1"; shift;;
    -tbl|--table|table)			TABLE="$1"; shift;;
    -h|--help|help)			echo -e "$USAGE"; exit 1;;
    *)					echo "Invalid input switch: $key"; echo -e "$USAGE"; exit 1;;
esac
done

if [[ ${TABLE} = "default" ]] ; then
echo "Required table name not provided."; 
echo -e "$USAGE"; 
exit 1;
fi
tableName=${TABLE^^}

if [[ ${SCHEMA} != "default" ]] ; then
tableName=${SCHEMA^^}.${tableName}
fi

if [[ ${DATABASE} != "default" ]] ; then
tableName=${DATABASE^^}.${tableName}
fi

echo "Preparing to compact ${tableName}"

compTableList=$(echo "${tableName}" | sed -e 's/^/major_compact \x27/g' -e 's/$/\x27/g')
echo "${compTableList}" | hbase shell

echo ""
echo "Now verifying that table has been compacted."
echo ""

for currTable in ${tableName}
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
