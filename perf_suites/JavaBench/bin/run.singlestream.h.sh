USAGE="usage: run.singlestream.h.sh
    [ -db|--database|database <<database>> ]
    [ -sch|--schema|schema <<schema>> ]
    [ -sc|--scale|scale <<scale>> ]
    [ -id|--testid|testid <<testid>> ]
    [ -l|--loops|loops <<loops>> ]
    [ -o|--options|options <<options>> ]
    [ -d|--debug|debug ]
    [ -h|--help|help ]"

#Default Values
DATABASE=trafodion
SCHEMA=
SCALE=
OPTIONS=
export TESTID=$(date +%y%m%d.%H%M)
LOOPS=1
export DRIVERID=0
OPTION_DEBUG=FALSE

while [[ $# > 0 ]] ; do
key="$1"; shift;
case $key in
    -db|--database|database)	DATABASE="$1"; shift;;
    -sc|--scale|scale)		SCALE="$1"; shift;;
    -sch|--schema|schema)	SCHEMA="$1";OPTIONS="schema $SCHEMA $OPTIONS"; shift;;
    -id|--testid|testid)	export TESTID="$1"; shift;;
    -l|--loops|loops)		LOOPS="$1"; shift;;
    -o|--options|options)	OPTIONS="$1 $OPTIONS"; shift;;
    -d|--debug|debug)		OPTION_DEBUG="TRUE";;
    -h|--help)			echo -e "$USAGE"; exit 1;;
    *)				echo "Invalid input switch: $key"; echo -e "$USAGE"; exit 1;;
esac
done

if [ -z "$LOGDIRECTORY" ]; then
export LOGDIRECTORY="${JAVABENCH_TEST_HOME}/log/${TESTID}.run.singlestream.h.$SCALE/"
if [ ! -f "${LOGDIRECTORY}" ] ; then
mkdir -p ${LOGDIRECTORY}
fi
fi
LOGFILE=${LOGDIRECTORY}/${TESTID}.run.singlestream.h.$SCALE.log

{

echo "
run.singlestream.h.sh"
echo "      DATABASE = ${DATABASE}"
echo "         SCALE = ${SCALE}"
echo "        SCHEMA = ${SCHEMA}"
echo "        TESTID = ${TESTID}"
echo "         LOOPS = ${LOOPS}"
echo "       OPTIONS = ${OPTIONS}"
echo "  OPTION_DEBUG = ${OPTION_DEBUG}"
echo "        ( logs will be found in ${LOGDIRECTORY} )"

STARTTIME=$SECONDS
echo "
===== Start bench_h test ( TESTID = ${TESTID} ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

if [[ $OPTION_DEBUG = FALSE ]] ; then
version.info.sh
if [[ $DATABASE = "trafodion" ]] ; then
bounce.dcs.sh
echo "
===== Capturing DDL, Stats, and Plans   ( TESTID = ${TESTID} ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
show.h.sh --testid ${TESTID} --all
fi
start.measure.sh ${TESTID}
else
echo OPTION_DEBUG $OPTION_DEBUG
fi

(( QUERIES = LOOPS * 22))

echo "
===== h , singlestream , $LOOPS ===== [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
java -Ddbconnect.properties=${DATABASE,,}.properties \
  WorkloadDriver H scale $SCALE streams 1 transactions $QUERIES ${OPTIONS} default

if [[ $OPTION_DEBUG = FALSE ]] ; then
echo ""
stop.measure.sh ${TESTID}
quickMEAS.sh -L ${TESTID} -m -q $QUERIES
fi

echo "
===== End bench_h test ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

} | tee ${LOGFILE}

