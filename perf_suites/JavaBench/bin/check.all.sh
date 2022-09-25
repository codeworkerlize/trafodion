USAGE="usage: check.compaction.all.sh
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

echo "Preparing to check ${DATABASE^^}.*"

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

echo ""
echo "Checking all tables for compaction."
echo ""

for currTable in ${tableList}
do
   
        curl -sS --noproxy '*' http://${SYSTEM_NAMEMGR_URL}:${SYSTEM_NAMEMGR_PORT}/table.jsp?name=${currTable} 2>/dev/null | awk '
	BEGIN { regionsbyregions=0 }
	/<title>Table:/ { print $0 }
	/<h2>Regions by Region Server<\/h2>/  {regionsbyregions=1}
	/<td>/ { if ( regionsbyregions==1 ) { printf $0 } }
	/^<\/tr>/ { if ( regionsbyregions==1 ) { printf "\n" } }
	/<\/table>/ { regionsbyregions=0 }
	END { printf "\n" }
	' | sed 's/<title>//g;s/<\/title>//g;s/<td>//g;s/<\/td>//g;s/".*"//g;s/<a href=//g;s/<\/a>//g;s/>/    /g'

done
     
exit 0
