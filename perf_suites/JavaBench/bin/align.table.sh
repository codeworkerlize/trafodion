USAGE="usage: align.table.sh
    [ -db|--database|database <<database>> ]
    -tb|--table|table <<tablename>>
    [ -id|--testid|testid <<testid>> ]
    [ -h|--help|help ]"
    
#Default Values
TESTID=$(date +%y%m%d.%H%M)
DATABASE=${JAVABENCH_DEFAULT_DATABASE}
TABLENAME=default

while [[ $# > 0 ]] ; do
key="$1"; shift;
case ${key,,} in
    -db|--database|database)		DATABASE="$1"; shift;;
    -tb|--table|table)			TABLENAME="$1"; shift;;
    -id|--testid|testid)		export TESTID="$1"; shift;;
    -h|--help|help)			echo -e "$USAGE"; exit 1;;
    *)					echo "Invalid input switch: $key"; echo -e "$USAGE"; exit 1;;
esac
done

case ${DATABASE,,} in
	trafodion|traf)		DATABASE="Trafodion";;
	hbase)			DATABASE="HBase";;
	*)		
		echo "Invalid Database Specified. DATABASE = ${DATABASE}. Expected values: [ Trafodion | Hbase ]"; 
		echo -e "$USAGE"; exit 1;;
esac

if [ ${TABLENAME} = default ]; then
	echo "Required Table Name Not Specified."; 
	echo -e "$USAGE"; exit 1;
fi

if [ -z "$LOGDIRECTORY" ]; then
export LOGDIRECTORY="${JAVABENCH_TEST_HOME}/log/${TESTID}.align.table/"
if [ ! -f "${LOGDIRECTORY}" ] ; then
mkdir -p ${LOGDIRECTORY}
fi
fi
LOGFILE=${LOGDIRECTORY}/${TESTID}.align.table.log

{
echo "
$0

           DATABASE = ${DATABASE}
              TABLE = ${TABLENAME}
                ( logs will be found in ${LOGDIRECTORY} )
"

echo "
===== Start Align Table ${TABLENAME} ( TESTID = ${TESTID} ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

STARTTIME=$SECONDS

case ${DATABASE,,} in
	trafodion|traf)		DATABASE="Trafodion";
		# Align Tables
		REGIONSERVERS=($(curl -sS --noproxy '*' http://${SYSTEM_NAMEMGR_URL}:${SYSTEM_NAMEMGR_PORT}/master-status?filter=all#baseStats | grep href | grep ",${SYSTEM_REGIONSERVER_PORT}," | sort -u | sed 's/<\/a>//g'| sed 's/<[^][]*>//g' | sed 's/ //g' | awk '{ printf "%s ", $1; }'))
echo "Region Severs : 
${REGIONSERVERS[*]}
"
echo "Table Name : 
${TABLENAME}
"
		REGIONS=($(curl -sS --noproxy '*' "http://${SYSTEM_NAMEMGR_URL}:${SYSTEM_NAMEMGR_PORT}/table.jsp?name=${TABLENAME}" | grep ${TABLENAME} | grep '<td>' | awk -F "." '{ print $(NF-1) }' | awk '{ printf "%s ", $1; }'))
echo "Regions : 
${REGIONS[*]}
"
		NUMBERofREGIONSERVERS=$(echo ${REGIONSERVERS[*]} | wc -w)
		NUMBERofREGIONS=$(echo ${REGIONS[*]} | wc -w)
		INDX=0
		{
		while (( $INDX < $NUMBERofREGIONS )) ; do
			(( REGION_INDX = $INDX % $NUMBERofREGIONSERVERS ))
			echo "move '${REGIONS[$INDX]}', '${REGIONSERVERS[$REGION_INDX]}'"
			(( INDX = $INDX + 1 ))
		done
		} | ssh ${SYSTEM_FIRST_NODE} hbase shell
		;;
	hbase)		DATABASE="HBase";
		# Align Tables
		REGIONSERVERS=($(curl -sS --noproxy '*' http://${SYSTEM_NAMEMGR_URL}:${SYSTEM_NAMEMGR_PORT}/master-status?filter=all#baseStats | grep href | grep ",${SYSTEM_REGIONSERVER_PORT}," | sort -u | sed 's/<\/a>//g'| sed 's/<[^][]*>//g' | sed 's/ //g' | awk '{ printf "%s ", $1; }'))
		echo "Region Severs : ${REGIONSERVERS[*]}"
		echo "Table Name : ${TABLENAME}"
		REGIONS=($(curl -sS --noproxy '*' "http://${SYSTEM_NAMEMGR_URL}:${SYSTEM_NAMEMGR_PORT}/table.jsp?name=${TABLENAME}" | grep ${TABLENAME} | grep '<td>' | awk -F "." '{ print $(NF-1) }' | awk '{ printf "%s ", $1; }'))
		echo "Regions : ${REGIONS[*]}"
		NUMBERofREGIONSERVERS=$(echo ${REGIONSERVERS[*]} | wc -w)
		NUMBERofREGIONS=$(echo ${REGIONS[*]} | wc -w)
		INDX=0
		{
		while (( $INDX < $NUMBERofREGIONS )) ; do
			(( REGION_INDX = $INDX % $NUMBERofREGIONSERVERS ))
			echo "move '${REGIONS[$INDX]}', '${REGIONSERVERS[$REGION_INDX]}'"
			(( INDX = $INDX + 1 ))
		done
		} | ssh ${SYSTEM_FIRST_NODE} hbase shell
		;;
	*)		
		echo "Invalid Database Specified. DATABASE = ${DATABASE}. Expected values: [ Trafodion | Hbase | SeaQuest ]"; 
		echo -e "$USAGE"; exit 1;;

esac
  
echo "
===== End Align Table ${TABLENAME}  ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

} | tee ${LOGFILE}

