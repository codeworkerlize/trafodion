USAGE="usage: load.ed.sh
    [ -db|--database|database <<database>> ]
    [ -sch|--schema|schema <<database>> ]
    [ -sc|--scale|scale <<scalefactor>> ]
    [ -s|--streams|streams <<streams>> ] 
    [ -p|--partitions|partitions <<partitions>> ] 
    [ -c|--compress|compress ]
    [ -a|--aligned|aligned ]
    [ -m|--monarch|monarch ]
    [ -d|--debug|debug ]
    [ -id|--testid|testid <<testid>> ]
    [ -o|--options|options <<options>> ]
    [ -h|--help|help ]
"

#Default Values
BENCHMARK=ED
DATABASE=${JAVABENCH_DEFAULT_DATABASE}
SCHEMA=default
TESTID=$(date +%y%m%d.%H%M)
SCALE=$MAX_ED_SCALE
if (( $MAX_MXOSRVR < 32 )) ; then STREAMS=$MAX_MXOSRVR; else STREAMS=32; fi
OPTION_COMPRESS=FALSE
OPTION_ALIGNED=""
OPTION_MONARCH=""
PARTITIONS=$SYSTEM_DEFAULT_PARTITIONS

while [[ $# > 0 ]] ; do
key="$1"; shift;
case ${key,,} in
    -db|--database|database)		DATABASE="$1"; shift;;
    -sch|--schema|schema)		SCHEMA="$1"; shift;;
    -sc|--scale|scale)			SCALE="$1"; shift;;
    -s|--streams|streams)		STREAMS="$1"; shift;;
    -p|--partitions|partitions)		PARTITIONS="$1"; shift;; 
    -c|--compress|compress)		OPTION_COMPRESS="TRUE";;
    -a|--aligned|aligned)               OPTION_ALIGNED="aligned"; shift;;
    -m|--monarch|monarch)               OPTION_MONARCH="monarch"; shift;;
    -id|--testid|testid)		export TESTID="$1"; shift;;
    -o|--options|options)		OPTIONS="$1"; shift;;
    -d|--debug|debug)			OPTION_DEBUG="TRUE";;
    -h|--help|help)			echo -e "$USAGE"; exit 1;;
    *)					echo "Invalid input switch: $key"; echo -e "$USAGE"; exit 1;;
esac
done

case ${DATABASE,,} in
	trafodion|traf)		DATABASE="Trafodion";;
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
               SALT = ${SALT}
            STREAMS = ${STREAMS}
             TESTID = ${TESTID}
        COMPRESSION = ${OPTION_COMPRESS}
            ALIGNED = ${OPTION_ALIGNED}
            MONARCH = ${OPTION_MONARCH}
            OPTIONS = ${OPTIONS}
       OPTION_DEBUG = ${OPTION_DEBUG}
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
		
		# create / load

		java -Ddbconnect.properties=${DATABASE,,}.properties \
		    EDLoader $SCALE all createschema dropcreate load upsert usingload batchsize 1000 streams $STREAMS salt $PARTITIONS maintain ${OPTION_ALIGNED} ${OPTION_MONARCH} ${OPTIONS}

		;;

	seaquest)

		if [[ ${SCHEMA} = "default" ]] ; then
		SCHEMA=$(grep schema ${JAVABENCH_TEST_HOME}/${DATABASE,,}.properties | awk '{ print $3 }')
		fi
		
		# create & load
		
		java -Ddbconnect.properties=${DATABASE,,}.properties \
		    EDLoader $SCALE all createschema dropcreate load batchsize 1000 streams $STREAMS maintain ${OPTIONS}

		;;
esac

echo "
===== End ${BENCHMARK} Load ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

} | tee ${LOGFILE}
