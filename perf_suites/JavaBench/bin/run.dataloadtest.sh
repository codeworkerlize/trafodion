USAGE="usage: run.dataloadtest.sh 
    [ -db|--database|database <<database>> ]
    [ -sch|--schema|schema <<schema>> ]
    [ -sc|--scale|scale <<scalefactor>> ]
    [ -w|--workload|workload <<workload>> ] 
    [ -s|--streams|streams <<streams>> ] 
    [ -p|--partitions|partitions <<partitions>> ] 
    [ -t|--time|time <<minutes>> ]
    [ -id|--testid|testid <<testid>> ]
    [ -o|--options|options <<options>> ]
    [ -d|--debug|debug ]
    [ -h|--help|help ]"

#Default Values
DATABASE=${JAVABENCH_DEFAULT_DATABASE}
SCHEMA=releasetest
SCALE=default
WORKLOAD=ycsb
if (( $MAX_MXOSRVR < 32 )) ; then
STREAMS=$MAX_MXOSRVR
else
STREAMS=32
fi
INTERVALS=10
PARTITIONS=$SYSTEM_DEFAULT_PARTITIONS
export TESTID=$(date +%y%m%d.%H%M)
export DRIVERID=0
OPTION_DEBUG=FALSE

while [[ $# > 0 ]] ; do
key="$1"; shift;
case $key in
    -db|--database|database)	DATABASE="$1"; shift;;
    -sch|--schema|schema)	SCHEMA="$1"; shift;;
    -sc|--scale|scale)		SCALE="$1"; shift;;
    -w|--workload|workload)   WORKLOAD="$1"; shift;;
    -p|--partitions|partitions) PARTITIONS="$1"; shift;;
    -d|--debug|debug)      OPTION_DEBUG="TRUE";;
    -id|--testid|testid)    export TESTID="$1"; shift;;
    -o|--options|options)     OPTIONS="$1"; shift;;
    -s|--streams|streams)    STREAMS="$1"; shift;;
    -t|--time|time)       INTERVALS="$1"; shift;;
    -h|--help|help)       echo -e "$USAGE"; exit 1;;
    *)               echo "Invalid input switch: $key"; echo -e "$USAGE"; exit 1;;
esac
done

if [ -z "$LOGDIRECTORY" ]; then
export LOGDIRECTORY="${JAVABENCH_TEST_HOME}/log/${TESTID}.run.dataloadtest.$WORKLOAD/"
if [ ! -f "${LOGDIRECTORY}" ] ; then
mkdir -p ${LOGDIRECTORY}
fi
fi
LOGFILE=${LOGDIRECTORY}/${TESTID}.run.dataloadtest.$WORKLOAD.log


{

echo "
$0

         DATABASE = ${DATABASE}
           SCHEMA = ${SCHEMA}
            SCALE = ${SCALE}
         WORKLOAD = ${WORKLOAD}
       PARTITIONS = ${PARTITIONS}
          STREAMS = ${STREAMS}
             TIME = ${INTERVALS}
           TESTID = ${TESTID}
          OPTIONS = ${OPTIONS}
     OPTION_DEBUG = ${OPTION_DEBUG}
        ( logs will be found in ${LOGDIRECTORY} )
"

STARTTIME=$SECONDS
echo "
===== Start Data Load Test ( TESTID = ${TESTID} ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

if [[ $OPTION_DEBUG = FALSE ]] ; then
version.info.sh
bounce.dcs.sh
start.measure.sh ${TESTID}
else
echo OPTION_DEBUG $OPTION_DEBUG
fi

case ${WORKLOAD,,} in 

	ycsb)
	
		echo "
===== Data Load Test , $WORKLOAD , $SCALE , $STREAMS, ${INTERVALS} ===== [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

		if [[ ${SCALE} = "default" ]] ; then
		SCALE=320
		fi

		case ${DATABASE,,} in

			seaquest)
				java -Ddbconnect.properties=${DATABASE,,}.properties \
				   YCSBLoader $SCALE schema $SCHEMA \
				   dropcreate 
				
				java -Ddbconnect.properties=${DATABASE,,}.properties \
				   YCSBLoader $SCALE schema $SCHEMA \
				   load batchsize 1000 streams $STREAMS intervals ${INTERVALS} 
				   
				java -Ddbconnect.properties=${DATABASE,,}.properties \
				   YCSBLoader $SCALE schema $SCHEMA \
				   drop 
				;;
			
			
			trafodion)
				java -Ddbconnect.properties=${DATABASE,,}.properties \
				   YCSBLoader $SCALE schema $SCHEMA \
				   dropcreate salt ${PARTITIONS}
				
				java -Ddbconnect.properties=${DATABASE,,}.properties \
				   YCSBLoader $SCALE schema $SCHEMA \
				   load upsert usingload batchsize 1000 streams $STREAMS intervals ${INTERVALS} 
				   
				java -Ddbconnect.properties=${DATABASE,,}.properties \
				   YCSBLoader $SCALE schema $SCHEMA \
				   drop 
				;;
		*)		
			echo "Invalid Database Specified. DATABASE = ${DATABASE}. Expected values: [ Trafodion | Seaquest ]"; 
			exit 1;;
				
		esac
		;;
	ed)
	
		echo "
===== Data Load Test , $WORKLOAD , 30 , $STREAMS, ${INTERVALS} ===== [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

		if [[ ${SCALE} = "default" ]] ; then
		SCALE=30
		fi

		case ${DATABASE,,} in

			seaquest)
				java -Ddbconnect.properties=${DATABASE,,}.properties \
				   EDLoader $SCALE table schema $SCHEMA \
				   dropcreate 
				
				java -Ddbconnect.properties=${DATABASE,,}.properties \
				   EDLoader $SCALE table schema $SCHEMA \
				   load batchsize 1000 streams $STREAMS intervals ${INTERVALS} 
				
				java -Ddbconnect.properties=${DATABASE,,}.properties \
				   EDLoader $SCALE table schema $SCHEMA \
				   drop
				;;
			
			trafodion)
				java -Ddbconnect.properties=${DATABASE,,}.properties \
				   EDLoader $SCALE table schema $SCHEMA \
				   dropcreate salt ${PARTITIONS}
				
				java -Ddbconnect.properties=${DATABASE,,}.properties \
				   EDLoader $SCALE table schema $SCHEMA \
				   load upsert usingload batchsize 1000 streams $STREAMS intervals ${INTERVALS} 
				
				java -Ddbconnect.properties=${DATABASE,,}.properties \
				   EDLoader $SCALE table schema $SCHEMA \
				   drop
				;;
		*)		
			echo "Invalid Database Specified. DATABASE = ${DATABASE}. Expected values: [ Trafodion | Seaquest ]"; 
			exit 1;;
				
		esac
		;;

	debitcredit)

		echo "
===== Data Load Test , $WORKLOAD , 10000 , $STREAMS, ${INTERVALS} ===== [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

		if [[ ${SCALE} = "default" ]] ; then
		SCALE=10000
		fi

		case ${DATABASE,,} in

			seaquest)
				java -Ddbconnect.properties=${DATABASE,,}.properties \
				   DebitCreditLoader $SCALE table account schema $SCHEMA \
				   dropcreate
				
				java -Ddbconnect.properties=${DATABASE,,}.properties \
				   DebitCreditLoader $SCALE table account schema $SCHEMA \
				   load batchsize 1000 streams $STREAMS intervals ${INTERVALS} 
				
				java -Ddbconnect.properties=${DATABASE,,}.properties \
				   DebitCreditLoader $SCALE table account schema $SCHEMA \
				   drop
				;;
			
			trafodion)
				java -Ddbconnect.properties=${DATABASE,,}.properties \
				   DebitCreditLoader $SCALE table account schema $SCHEMA \
				   dropcreate salt ${PARTITIONS}

				java -Ddbconnect.properties=${DATABASE,,}.properties \
				   DebitCreditLoader $SCALE table account schema $SCHEMA \
				   load upsert usingload batchsize 1000 streams $STREAMS intervals ${INTERVALS} 

				java -Ddbconnect.properties=${DATABASE,,}.properties \
				   DebitCreditLoader $SCALE table account schema $SCHEMA \
				   drop
				;;
		*)		
			echo "Invalid Database Specified. DATABASE = ${DATABASE}. Expected values: [ Trafodion | Seaquest ]"; 
			exit 1;;
				
		esac
		;;

	*)		
		echo "Invalid Workload Specified. WORKLOAD = ${WORKLOAD}. Expected values: [ ycsb | ed | debitcredit ]"; 
		exit 1;;

esac

echo ""

if [[ $OPTION_DEBUG = FALSE ]] ; then
stop.measure.sh ${TESTID}
quickMEAS.sh -L ${TESTID} -m -q $(grep '^   Total ,   ' ${LOGFILE} | awk '{ print $23 }')
fi

echo "
===== End Data Load Test ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

} | tee ${LOGFILE}

