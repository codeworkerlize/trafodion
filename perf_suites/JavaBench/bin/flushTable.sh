#! /bin/bash
#  Gag   141111   Initial version.
#  Gag   150404   Use variables from Javabench profile. SYSTEM_NAMEMGR_...

if [ $# -eq 0 ] ; then
    echo "Syntax: $0 -l OE | DC | Y | ED  -s <scale>"
    echo "To do major_compact on the tables for the specified load."
    exit 0
fi

wantLoad=""
wantScale=0

while getopts 'hl:s:' parmOpt
do
    case $parmOpt in
    h) echo "Syntax: $0 -l OE | DC | Y | ED  [-s <scale>]"
       echo "To do major_compact on the tables for the specified load."
       echo "If <scale> is not specified, it will use the current SCALE for that load."
       exit 0;;
    l) case ${OPTARG} in
       OE|oe|Oe|Oe) wantLoad=OE_;;
       DC|dc|Dc|dC) wantLoad=DC_;;
       Y|y) wantLoad=YCSB_;;
       ED|ed|eD|Ed) wantLoad=ED_TABLE_;;
       *) echo "Invalid load specified."
          exit 0;;
       esac
       ;;
    s) wantScale=$OPTARG;;
    ?)  echo "Invalid option specified.   Only -l, -s, -h are allowed."
        exit 0 ;;
    esac
done

if [ ${#wantLoad} -eq 0 ] || [ ${wantScale} -eq 0 ] ; then
    echo "You must specify a load to compact and a scale to use."
    exit 0
fi

echo "Preparing to flush ${wantLoad}*_${wantScale}"

tableList=$(echo "list 'TRAFODION.${JAVABENCH_DEFAULT_SCHEMA}.*${wantLoad}.*_${wantScale}'" | hbase shell 2>/dev/null | \
    awk 'BEGIN {want=0}
         /^TABLE/ {want=1; next}
         /row\(s\)/ {exit}
                  {if (want==1) {print $1}}
        ')

if [ ${#tableList} -eq 0 ] ; then
    echo "No tables were found."
    exit 0
fi

compTableList=$(echo "${tableList}" | sed -e 's/^/flush \x27/g' -e 's/$/\x27/g')
echo "${compTableList}" | hbase shell

echo ""
echo "Sleep 15 seconds"
echo ""
sleep 15

echo ""
echo "Now verifying that all tables have been compacted."
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
