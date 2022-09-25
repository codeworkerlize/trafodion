USAGE="usage: run.ed.atomics.sh 
    [ -db|--database|database <<database>> ]
    [ -sch|--schema|schema <<schema>> ]
    [ -w|--workload|workload <<workload>> ]
    [ -id|--testid|testid <<testid>> ]
    [ -sc|--scale|scale <<scalefactor>> ]
    [ -o|--options|options <<options>> ]
    [ -d|--debug|debug ]
    [ -h|--help|help ]"

#Default Values
DATABASE=${JAVABENCH_DEFAULT_DATABASE}
SCHEMA=
OPTIONS=
WORKLOAD=all
TESTID=$(date +%y%m%d.%H%M)
SCALE=$MAX_ED_SCALE
OPTION_DEBUG=FALSE

while [[ $# > 0 ]] ; do
key="$1"; shift;
case $key in
    -db|--database|database)	DATABASE="$1"; shift;;
    -sch|--schema|schema)	SCHEMA="$1";OPTIONS="schema $SCHEMA $OPTIONS"; shift;;
    -w|--workload|workload)	WORKLOAD="$1"; shift;;
    -id|--testid|testid)	export TESTID="$1"; shift;;
    -o|--options|options)	OPTIONS="$1 $OPTIONS"; shift;;
    -sc|--scale|scale)		SCALE="$1"; shift;;
    -d|--debug|debug)		OPTION_DEBUG="TRUE";;
    -h|--help)			echo -e "$USAGE"; exit 1;;
    *)				echo "Invalid input switch: $key"; echo -e "$USAGE"; exit 1;;
esac
done

if [ -z "$LOGDIRECTORY" ]; then
export LOGDIRECTORY="${JAVABENCH_TEST_HOME}/log/${TESTID}.run.ed.atomics.$DATABASE.$WORKLOAD/"
if [ ! -f "${LOGDIRECTORY}" ] ; then
mkdir -p ${LOGDIRECTORY}
fi
fi
LOGFILE=${LOGDIRECTORY}/${TESTID}.run.ed.atomics.$DATABASE.$WORKLOAD.log

{

echo "
$0

         DATABASE = ${DATABASE}
           SCHEMA = ${SCHEMA}
         WORKLOAD = ${WORKLOAD}
           TESTID = ${TESTID}
            SCALE = ${SCALE}
          OPTIONS = ${OPTIONS}
     OPTION_DEBUG = ${OPTION_DEBUG}
"

STARTTIME=$SECONDS
echo "
===== Start ED Atomics Test ( TESTID = ${TESTID} ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
        ( logs will be found in ${LOGDIRECTORY} )
"
if [[ $OPTION_DEBUG = FALSE ]] ; then
version.info.sh
if [[ $DATABASE = "trafodion" ]] ; then
bounce.dcs.sh
echo "
===== Capturing DDL, Stats, and Plans   ( TESTID = ${TESTID} ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
        ( logs will be found in ${JAVABENCH_TEST_HOME}/temp )
"
show.ed.atomics.sh --testid ${TESTID} --scale $SCALE --all
fi
#start.measure.sh ${TESTID}
else
echo OPTION_DEBUG $OPTION_DEBUG
fi

echo "
===== ED Atomics Workload ${WORKLOAD}  ( TESTID = ${TESTID} ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
java -Ddbconnect.properties=${DATABASE,,}.properties EDAtomics ${WORKLOAD} ${SCALE} timer 2 ${OPTIONS}

if [[ $OPTION_DEBUG = FALSE ]] ; then
echo ""
#stop.measure.sh ${TESTID}
#quickMEAS.sh -L ${TESTID} -m 
fi

echo "
===== End ED Atomics Test ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

} | tee ${LOGFILE}
