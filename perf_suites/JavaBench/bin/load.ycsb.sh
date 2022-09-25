USAGE="usage: load.ycsb.sh
    [ -db|--database|database <<database>> ]
    [ -sch|--schema|schema <<schema>> ]
    [ -sc|--scale|scale <<scalefactor>> ]
    [ -s|--streams|streams <<streams>> ] 
    [ -p|--partitions|partitions <<partitions>> ] 
    [ -a|--aligned|aligned ]
    [ -m|--monarch|monarch ]
    [ -c|--compress|compress ]
    [ -r|--rangepartition|rangepartition ]
    [ -d|--debug|debug ]
    [ -id|--testid|testid <<testid>> ]
    [ -o|--options|options <<options>> ]
    [ -h|--help|help ]
"
    
#Default Values
BENCHMARK=YCSB
DATABASE=${JAVABENCH_DEFAULT_DATABASE}
SCHEMA=default
SCALE=$YCSB_SCALE
if (( $MAX_MXOSRVR < 32 )) ; then STREAMS=$MAX_MXOSRVR; else STREAMS=32; fi
PARTITIONS=$SYSTEM_DEFAULT_PARTITIONS
export TESTID=$(date +%y%m%d.%H%M)
OPTION_COMPRESS=FALSE
OPTION_ALIGNED=""
OPTION_RANGEPARTITION=FALSE

while [[ $# > 0 ]] ; do
key="$1"; shift;
case ${key,,} in
    -db|--database|database)		DATABASE="$1"; shift;;
    -sch|--schema|schema)		SCHEMA="$1"; shift;;
    -sc|--scale|scale)			SCALE="$1"; shift;;
    -s|--streams|streams)		STREAMS="$1"; shift;;
    -p|--partitions|partitions)		PARTITIONS="$1"; shift;; 
    -a|--aligned|aligned)               OPTION_ALIGNED="aligned"; shift;;
    -m|--monarch|monarch)               OPTION_MONARCH="monarch"; shift;;
    -c|--compress|compress)		OPTION_COMPRESS="TRUE";;
    -r|--rangepartition|rangepartition)	OPTION_RANGEPARTITION="TRUE";;
    -id|--testid|testid)		export TESTID="$1"; shift;;
    -o|--options|options)		OPTIONS="$1"; shift;;
    -d|--debug|debug)			OPTION_DEBUG="TRUE";;
    -h|--help|help)			echo -e "$USAGE"; exit 1;;
    *)					echo "Invalid input switch: $key"; echo -e "$USAGE"; exit 1;;
esac
done

case ${DATABASE,,} in
	trafodion|traf)		DATABASE="Trafodion";;
	hbase)			DATABASE="Hbase";;
	seaquest)		DATABASE="SeaQuest";;
	*)		
		echo "Invalid Database Specified. DATABASE = ${DATABASE}. Expected values: [ Trafodion | Hbase | SeaQuest ]"; 
		echo -e "$USAGE"; exit 1;;

esac

if [ -z "$LOGDIRECTORY" ]; then
export LOGDIRECTORY="${JAVABENCH_TEST_HOME}/log/${TESTID}.load.${BENCHMARK}.${DATABASE}/"
if [ ! -f "${LOGDIRECTORY}" ] ; then
mkdir -p ${LOGDIRECTORY}
fi
fi
LOGFILE=${LOGDIRECTORY}/${TESTID}.load.${BENCHMARK}.${DATABASE}.log

{

echo "
$0

          DATABASE = ${DATABASE}
            SCHEMA = ${SCHEMA}
             SCALE = ${SCALE}
           STREAMS = ${STREAMS}
        PARTITIONS = ${PARTITIONS}
    RANGEPARTITION = ${OPTION_RANGEPARTITION}
            TESTID = ${TESTID}
       COMPRESSION = ${OPTION_COMPRESS}
             DEBUG = ${OPTION_DEBUG}
           OPTIONS = ${OPTIONS}
                 ( logs will be found in ${LOGDIRECTORY} )
"

if [[ ${OPTION_COMPRESS} = "TRUE" ]] ; then
OPTIONS="compression ${OPTIONS}"
fi

echo "
===== Start ${BENCHMARK} Load ( TESTID = ${TESTID} ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
STARTTIME=$SECONDS

case ${DATABASE,,} in
	trafodion)	
	
		if [[ ${SCHEMA} = "default" ]] ; then
		SCHEMA=$(grep schema ${JAVABENCH_TEST_HOME}/${DATABASE,,}.properties | awk '{ print $3 }')
		fi
		
		# create 
		
		if [[ ${OPTION_RANGEPARTITION} = TRUE ]] ; then
		
			java -Xmx1500m -Ddbconnect.properties=${DATABASE,,}.properties \
			   YCSBLoader $SCALE schema $SCHEMA createschema dropcreate ${OPTION_ALIGNED} ${OPTION_MONARCH} ${OPTIONS}

			#split table
			TABLENAME=${SCHEMA^^}.YCSB_TABLE_${SCALE}
			echo "Table Name : ${TABLENAME}"
pdsh -w ${SYSTEM_FIRST_NODE} "cd ${JAVABENCH_TEST_HOME};. profile;java -Xmx1500m GetYcsbSplitTableCommand table ${TABLENAME} partitions ${PARTITIONS} | hbase shell"
		
			# Align Table
			REGIONSERVERS=($(curl -sS --noproxy '*' http://${SYSTEM_NAMEMGR_URL}:${SYSTEM_NAMEMGR_PORT}/master-status?filter=all#baseStats \
			  | grep href | grep ",${SYSTEM_REGIONSERVER_PORT}," | sort -u | sed 's/<\/a>//g'| sed 's/<[^][]*>//g' | sed 's/ //g'))
			echo "Region Severs : ${REGIONSERVERS[*]}"
			REGIONS=($(curl -sS --noproxy '*' "http://${SYSTEM_NAMEMGR_URL}:${SYSTEM_NAMEMGR_PORT}/table.jsp?name=${TABLENAME}" \
			  | grep ${TABLENAME} | grep '<td>' | awk -F "." '{ print $(NF-1) }'))
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


		else  # assume salt partition
		
			java -Xmx1500m -Ddbconnect.properties=${DATABASE,,}.properties \
			   YCSBLoader $SCALE schema $SCHEMA createschema dropcreate salt ${PARTITIONS} ${OPTION_ALIGNED} ${OPTION_MONARCH} ${OPTIONS}
		
		fi
		
		#  load
		
		java -Xmx1500m -Ddbconnect.properties=${DATABASE,,}.properties \
		   YCSBLoader $SCALE schema $SCHEMA load upsert usingload batchsize 1000 streams $STREAMS maintain ${OPTIONS}

		;;

	hbase)
		if [[ ${SCHEMA} = "default" ]] ; then
		SCHEMA=YCSB
		fi
		
		TABLENAME=${SCHEMA}.YCSB_TABLE_${SCALE}
		
		ROWS_PER_SCALE=1000000
		ROWS=$(( $SCALE * $ROWS_PER_SCALE ))
		
		#create table
		pdsh -w $SYSTEM_FIRST_NODE "echo \"disable '${TABLENAME}'
			drop '${TABLENAME}'
			create '${TABLENAME}', {NAME => 'ycsbfmly'}\" | hbase shell"
		
		#split table
pdsh -w ${SYSTEM_FIRST_NODE} "cd ${JAVABENCH_TEST_HOME};. profile;java -Xmx1500m GetYcsbSplitTableCommand table ${TABLENAME} partitions ${PARTITIONS} | hbase shell"
		
			# Align Table
			REGIONSERVERS=($(curl -sS --noproxy '*' http://${SYSTEM_NAMEMGR_URL}:${SYSTEM_NAMEMGR_PORT}/master-status?filter=all#baseStats \
			  | grep href | grep ",${SYSTEM_REGIONSERVER_PORT}," | sort -u | sed 's/<\/a>//g'| sed 's/<[^][]*>//g' | sed 's/ //g'))
			echo "Region Severs : ${REGIONSERVERS[*]}"
			REGIONS=($(curl -sS --noproxy '*' "http://${SYSTEM_NAMEMGR_URL}:${SYSTEM_NAMEMGR_PORT}/table.jsp?name=${TABLENAME}" \
			  | grep ${TABLENAME} | grep '<td>' | awk -F "." '{ print $(NF-1) }'))
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

pdsh -w ${SYSTEM_FIRST_NODE} "cd ${JAVABENCH_TEST_HOME};. profile;java -Xmx1500m -Ddbconnect.properties=${DATABASE,,}.properties YCSBLoader $SCALE schema ${SCHEMA} load streams ${STREAMS}"

		;;
		
	seaquest)

		if [[ ${SCHEMA} = "default" ]] ; then
		SCHEMA=$(grep schema ${JAVABENCH_TEST_HOME}/${DATABASE,,}.properties | awk '{ print $3 }')
		fi
		
		# create & load
		
		java -Xmx1500m -Ddbconnect.properties=${DATABASE,,}.properties \
		   YCSBLoader $SCALE schema $SCHEMA dropcreate load batchsize 1000 streams $STREAMS maintain ${OPTIONS}

		;;
esac

echo "
===== End ${BENCHMARK} Load ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

} | tee ${LOGFILE}
