USAGE="usage: run.releasetest.sh
    [ -t|--time|time <<minutes>> ]
    [ -id|--testid|testid <<testid>> ]
    [ -o|--options|options <<options>> ]
    [ -d|--debug|debug ]
    [ -h|--help|help ]"

#Default Values
TESTID=$(date +%y%m%d.%H%M)
INTERVALS=30
OPTION_DEBUG=FALSE

while [[ $# > 0 ]] ; do
key="$1"; shift;
case $key in
    -d|--debug|debug)      OPTION_DEBUG="TRUE"; OPTIONS="--debug ${OPTIONS}";;
    -id|--testid|testid)    TESTID="$1"; shift;;
    -o|--options|options)    OPTIONS="${OPTIONS} options $1"; shift;;
    -t|--time|time)       OPTIONS="--time $1 ${OPTIONS}"; shift;;
    -h|--help|help)       echo -e "$USAGE"; exit 1;;
    *)               echo "Invalid input switch: $key"; echo -e "$USAGE"; exit 1;;
esac
done

if [ -z "$LOGDIRECTORY" ]; then
export LOGDIRECTORY="${JAVABENCH_TEST_HOME}/log/${TESTID}.run.releasetest/"
if [ ! -f "${LOGDIRECTORY}" ] ; then
mkdir -p ${LOGDIRECTORY}
fi
LOGFILE=${LOGDIRECTORY}/${TESTID}.run.releasetest.log
fi


{

echo "
$0

        TESTID = ${TESTID}
       OPTIONS = ${OPTIONS}
  OPTION_DEBUG = ${OPTION_DEBUG}
"

STARTTIME=$SECONDS
echo "
===== Start Release Test ( TESTID = ${TESTID} ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
        ( logs will be found in ${LOGDIRECTORY} )
"

if [[ $OPTION_DEBUG = FALSE ]] ; then
version.info.sh
fi

echo "
===== YCSB Tests =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

run.ycsb.sh --testid ${TESTID}.1 --workload YCSBSingletonSelect ${OPTIONS}

run.ycsb.sh --testid ${TESTID}.2 --workload YCSBSingleton5050   ${OPTIONS}

run.ycsb.sh --testid ${TESTID}.3 --workload YCSBSingletonUpdate ${OPTIONS}


echo "
===== ED Tests =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

run.ed.atomics.sh --testid ${TESTID}.6

echo "
===== Data Loading Tests =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

run.dataloadtest.sh testid ${TESTID}.7 workload ycsb        ${OPTIONS} 

run.dataloadtest.sh testid ${TESTID}.8 workload ed          ${OPTIONS}

run.dataloadtest.sh testid ${TESTID}.9 workload debitcredit ${OPTIONS}

echo "
===== OLTP Tests =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
run.debitcredit.sh --testid ${TESTID}.4 --autocommit ${OPTIONS}

run.debitcredit.sh --testid ${TESTID}.5  ${OPTIONS}

run.orderentry.sh  --testid ${TESTID}.11 ${OPTIONS}

echo "
===== End Release Test ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

releasetest.summary.sh -x ${TESTID}

} | tee ${LOGFILE}



