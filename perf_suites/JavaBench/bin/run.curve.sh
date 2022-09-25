USAGE ()
{
	echo "Usage:   run.curve.sh"
	echo "   -bm|--benchmark|benchmark <<benchmark>>"
	echo "   [ -mt|--multi|multi ]"
        echo "   Other options from run.test.sh available."
	echo ""
	echo -n "   "
	run.test.sh  --help
}

if [ $# -lt 2 ] ; then
	USAGE
	exit 0
fi

BENCHMARK="default"
DATABASE="default"
SCHEMA="default"
SCALE="default"
WORKLOAD="default"
STREAMS="default"
DRIVERS="default"
OPTIONS=""
MULTI=0

while [[ $# > 0 ]] ; do
	key="$1"; shift;
	case ${key,,} in
	    -bm|--benchmark|benchmark)          BENCHMARK="$1"; shift;;
	    -db|--database|database)            DATABASE="$1"; shift;;
	    -sch|--schema|schema)               SCHEMA="$1"; shift;;
	    -sc|--scale|scale)                  SCALE="$1"; shift;;
	    -w|--workload|workload)             WORKLOAD="$1"; shift;;
	    -a|--autocommit|autocommit)         WORKLOAD="Autocommit";;
	    -s|--streams|streams)               STREAMS="$1"; shift;;
	    -dr|--drivers|drivers)              DRIVERS="$1"; shift;;
	    -h|--help|help)                     USAGE; exit 0;;
	    -mt|--multi|multi)                  MULTI=1;;
	    *)                                  OPTIONS="${OPTIONS} ${key}";;
	esac
done

case ${DATABASE,,} in
	trafodion|traf)         DATABASE="Trafodion";;
	hbase)                  DATABASE="Hbase";;
	seaquest)               DATABASE="SeaQuest";;
        default)                DATABASE=${JAVABENCH_DEFAULT_DATABASE};;
        *)      echo "Invalid Database Specified. DATABASE = ${DATABASE}."
                USAGE; exit 1;;

esac

case ${BENCHMARK^^} in
	YCSB|Y)	BENCHMARK=YCSB
		if [ ${SCHEMA} = "default" ] ; then
			case ${DATABASE,,} in
				trafodion)	SCHEMA=$(grep schema ${JAVABENCH_TEST_HOME}/${DATABASE,,}.properties | awk '{ print $3 }');;
				hbase)		SCHEMA=${BENCHMARK^^};;
		                *) echo "Invalid Database Specified. DATABASE = ${DATABASE}."
	                           USAGE; exit 1;;
			esac
		fi
		if [ "${WORKLOAD}" = "default" ] ; then
			WORKLOAD="SingletonSelect Singleton5050 SingletonUpdate"
		fi
		if [ ${SCALE} = "default" ] ; then
			SCALE=${YCSB_SCALE}
		fi
		if [ "${STREAMS}" = "default" ] ; then
			STREAMS=${YCSB_STREAMS}
		fi
		if [ ${DRIVERS} = "default" ] ; then
			MULTI=1
		fi
		;;
	ORDERENTRY|OE)
		BENCHMARK=OrderEntry
		if [ ${SCHEMA} = "default" ] ; then
			SCHEMA=$(grep schema ${JAVABENCH_TEST_HOME}/${DATABASE,,}.properties | awk '{ print $3 }')
		fi
		if [ "${WORKLOAD}" = "default" ] ; then
			WORKLOAD="Transactional"
		fi
		if [ "${STREAMS}" = "default" ] ; then
			STREAMS=${ORDERENTRY_STREAMS}
		fi
		;;
	DEBITCREDIT|DC)
		BENCHMARK=DebitCredit
		if [ ${SCHEMA} = "default" ] ; then
			SCHEMA=$(grep schema ${JAVABENCH_TEST_HOME}/${DATABASE,,}.properties | awk '{ print $3 }')
		fi
		if [ "${WORKLOAD}" = "default" ] ; then
			WORKLOAD="Transactional"
		fi
		if [ "${STREAMS}" = "default" ] ; then
			STREAMS=${DEBITCREDIT_STREAMS}
		fi
		;;
	*)	echo "Benchmark ${BENCHMARK} not supported by this script."
		USAGE; exit 1;;
esac

export TESTID=$(date +%y%m%d.%H%M)
if [ -z "$LOGDIRECTORY" ]; then
	if [ $(echo ${WORKLOAD} | wc -w) -gt 1 ] ; then
		export LOGDIRECTORY="${JAVABENCH_TEST_HOME}/log/${TESTID}.run.curve.${BENCHMARK}.${DATABASE}/"
	else
		export LOGDIRECTORY="${JAVABENCH_TEST_HOME}/log/${TESTID}.run.curve.${BENCHMARK}.${DATABASE}.${WORKLOAD}/"
	fi
	if [ ! -f "${LOGDIRECTORY}" ] ; then
		mkdir -p ${LOGDIRECTORY}
	fi
fi

if [ ${DATABASE,,} != "hbase" ] ; then
	OPTIONS="${OPTIONS} burnin 5"
fi

loop_IDX=1
for WORKLOAD_IDX in ${WORKLOAD} ; do
	LOGFILE=${LOGDIRECTORY}/${TESTID}.run.curve.${BENCHMARK}.${DATABASE}.${WORKLOAD_IDX}.log
        export WTESTID=${TESTID}.${loop_IDX}
	{
	time {
	version.info.sh

        if [ ${DATABASE,,} != "ycsb" ] ; then
		show.${BENCHMARK,,}.sh testid ${WTESTID} all
        fi

	if [ $(echo ${STREAMS} | wc -w) -gt 1 ] ; then
		STRM_LIST="${STREAMS}"
	else
		STRM_IDX=128
		STRM_LIST=""
		while [ ${STRM_IDX} -le ${STREAMS} ] ; do
		     STRM_LIST="${STRM_LIST} ${STRM_IDX}"
		     STRM_IDX=$((STRM_IDX * 2))
		done
	fi

	loopIDX=1
	for STRM_IDX in ${STRM_LIST} ; do
		if [ ${SCALE} = "default" ] ; then
			STRM_SCALE=${STRM_IDX}
		else
			STRM_SCALE=${SCALE}
		fi
		if [ ${MULTI} -eq 0 ] ; then
		    time run.test.sh testid ${WTESTID}.${loopIDX} database ${DATABASE} benchmark ${BENCHMARK} workload ${WORKLOAD_IDX} scale ${STRM_SCALE} streams ${STRM_IDX} bouncehbase majorcompact measure ${OPTIONS}
		else
		    if [ ${DRIVERS} = "default" ] ; then
			numDrivers=$((STRM_IDX / 8))
		    else
			numDrivers=${DRIVERS}
		    fi
		    time run.test.sh testid ${WTESTID}.${loopIDX} database ${DATABASE} benchmark ${BENCHMARK} workload ${WORKLOAD_IDX} scale ${STRM_SCALE} streams ${STRM_IDX} bouncehbase majorcompact measure drivers ${numDrivers} distributedrivers ${OPTIONS}
		fi
		loopIDX=$((loopIDX + 1))
	done
	} 2>&1
	} | tee ${LOGFILE}
	loop_IDX=$((loop_IDX + 1))
done

