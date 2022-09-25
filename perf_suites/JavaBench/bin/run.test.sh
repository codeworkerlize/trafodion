USAGE="usage: run.test.sh 
    -bm|--benchmark|benchmark <<benchmark>>
    [ -db|--database|database <<database>> ]
    [ -sch|--schema|schema <<schema>> ]
    [ -sc|--scale|scale <<scalefactor>> ]
    [ -w|--workload|workload <<workload>> ] 
    [ -a|--autocommit|autocommit ]
    [ -s|--streams|streams <<streams>> ] 
    [ -t|--time|time <<minutes>> ]
    [ -dr|--drivers|drivers <<numberofdrivers>> ]    
    [ -t4|--type4|type4 ]           (default is t4 driver)
    [ -t2|--type2|type2 ]          
    [ --streamsperdriver|streamsperdriver  <<numberofstreamsperdriver>> ]    
    [ --distributedrivers|distributedrivers ]
    [ --distributemode|distributemode <<local|system|driver>> ]
    [ -info|--sysinfo|sysinfo ]
    [ -m|--measure|measure ]
    [ -p|--plans|plans ]
    [ -bd|--bouncedcs|bouncedcs ]
    [ -bt|--bouncetrafodion|bouncetrafodion ]
    [ -bh|--bouncehbase|bouncehbase ]
    [ -bi|--burnin|burnin <<minutes>> ]
    [ --summary|summary ]
    [ -d|--debug|debug ]
    [ -id|--testid|testid <<testid>> ]
    [ -o|--option|option <<options>> ]  (repeat for multiple options)
    [ -h|--help|help ]
"

#Default Values
BENCHMARK="default"
DATABASE="default"
SCHEMA="default"
SCALE="default"
WORKLOAD="default"
STREAMS="default"
INTERVALS=30
DRIVERS="default"
DRIVER_TYPE="default"
DRIVER_LOCATION="default"
STREAMS_PER_DRIVER="default"
OPTION_SYSINFO="FALSE"
OPTION_MEASURE="FALSE"
OPTION_PLANS="FALSE"
OPTION_BOUNCE="NONE"
OPTION_SUMMARY="FALSE"
OPTION_DEBUG="FALSE"
OPTION_BURNIN="default"
OPTION_COMPACT="FALSE"
export TESTID=$(date +%y%m%d.%H%M)
export DRIVERID=0
OPTIONS=

while [[ $# > 0 ]] ; do
key="$1"; shift;
case ${key,,} in
    -bm|--benchmark|benchmark)			BENCHMARK="$1"; shift;;
    -db|--database|database)			DATABASE="$1"; shift;;
    -sch|--schema|schema)			SCHEMA="$1"; shift;;
    -sc|--scale|scale)				SCALE="$1"; shift;;
    -w|--workload|workload)			WORKLOAD="$1"; shift;;
    -a|--autocommit|autocommit)			WORKLOAD="Autocommit";;
    -s|--streams|streams)			STREAMS="$1"; shift;;
    -t|--time|time)				INTERVALS="$1"; shift;;
    -dr|--drivers|drivers)			DRIVERS="$1"; shift;;
    -t2|--type2|type2)				DRIVER_TYPE="TYPE2";;
    -t4|--type4|type4)				DRIVER_TYPE="TYPE4";;
    --streamsperdriver|streamsperdriver)	STREAMS_PER_DRIVER="$1"; shift;;
    --distributedrivers|distributedrivers)	DRIVER_LOCATION="system";;
    --distributemode|distributemode)            DRIVER_LOCATION="$1"; shift;;
    -info|--sysinfo|sysinfo)			OPTION_SYSINFO="TRUE";;
    -m|--measure|measure)			OPTION_MEASURE="TRUE";;
    -p|--plans|plans)				OPTION_PLANS="TRUE";;
    -bd|--bouncedcs|bouncedcs)			OPTION_BOUNCE="DCS";;
    -bt|--bouncetrafodion|bouncetrafodion)	OPTION_BOUNCE="TRAF";;
    -bh|--bouncehbase|bouncehbase)		OPTION_BOUNCE="HBASE";;
    -mc|--majorcompact|majorcompact)		OPTION_COMPACT="TRUE";;
    -bi|--burnin|burnin)			OPTION_BURNIN="$1"; shift;;
    --summary|summary)				OPTION_SUMMARY="TRUE";;
    -d|--debug|debug)				OPTION_DEBUG="TRUE";;
    -id|--testid|testid)			export TESTID="$1"; shift;;
    -o|--option|option)				OPTIONS="$OPTIONS $1"; shift;;
    -h|--help|help)				echo -e "$USAGE"; exit 1;;
    *)						echo "Invalid input switch: $key"; echo -e "$USAGE"; exit 1;;
esac
done

case ${DATABASE,,} in
	trafodion|traf)		DATABASE="Trafodion";;
	hbase)			DATABASE="Hbase";;
	seaquest)		DATABASE="SeaQuest";;
	default)		DATABASE=${JAVABENCH_DEFAULT_DATABASE};;
	*)		
		echo "Invalid Database Specified. DATABASE = ${DATABASE}. Expected values: [ Trafodion | Hbase ]"; 
		echo -e "$USAGE"; exit 1;;

esac

case ${BENCHMARK,,} in
    ycsb)
    	BENCHMARK=YCSB
    
	if [[ ${SCALE} = "default" ]] ; then
	    SCALE=${YCSB_SCALE}
	fi
   	
	if [[ ${STREAMS} = "default" ]] ; then
	    STREAMS=${YCSB_STREAMS}
	fi

	case ${WORKLOAD,,} in
	    select|singletonselect|ycsbsingletonselect)		WORKLOAD="SingletonSelect";;
	    update|singletonupdate|ycsbsingletonupdate)		WORKLOAD="SingletonUpdate";;
	    5050|singleton5050|ycsbsingleton5050)		WORKLOAD="Singleton5050";;
	    2080|singleton2080|ycsbsingleton2080)		WORKLOAD="Singleton2080";;
	    9505|singleton9505|ycsbsingleton9505)		WORKLOAD="Singleton9505";;
	    default)						WORKLOAD="SingletonSelect";;
	    *)		
		echo "Invalid Workload Specified. WORKLOAD = ${WORKLOAD}. Expected values: [ SingletonSelect | SingletonUpdate | Singleton5050 | Singleton2080 | Singleton9505 ]"; 
		echo -e "$USAGE"; exit 1;;
	esac
	;;

    dc|debitcredit)
    	BENCHMARK=DebitCredit
    
	if [[ ${SCALE} = "default" ]] ; then
	    SCALE=${DEBITCREDIT_SCALE}
	fi
   	
	if [[ ${STREAMS} = "default" ]] ; then
	    STREAMS=${DEBITCREDIT_STREAMS}
	fi

	case ${WORKLOAD,,} in
	    transactional)		WORKLOAD="Transactional";;
	    autocommit)			WORKLOAD="Autocommit";;
	    default)			WORKLOAD="Transactional";;
	    *)		
		echo "Invalid Workload Specified. WORKLOAD = ${WORKLOAD}. Expected values: [ Transactional | Autocommit ]"; 
		echo -e "$USAGE"; exit 1;;
	esac
	;;

    oe|orderentry)
    	BENCHMARK=OrderEntry
    
	if [[ ${SCALE} = "default" ]] ; then
	    SCALE=${ORDERENTRY_SCALE}
	fi
   	
	if [[ ${STREAMS} = "default" ]] ; then
	    STREAMS=${ORDERENTRY_STREAMS}
	fi

	case ${WORKLOAD,,} in
	    transactional)		WORKLOAD="Transactional";;
	    autocommit)			WORKLOAD="Autocommit";;
	    default)			WORKLOAD="Transactional";;
	    *)		
		echo "Invalid Workload Specified. WORKLOAD = ${WORKLOAD}. Expected values: [ Transactional | Autocommit ]"; 
		echo -e "$USAGE"; exit 1;;
	esac
	;;

    tpch)
    	BENCHMARK=TPCH
    
	if [[ ${SCALE} = "default" ]] ; then
	    SCALE=${TPCH_SCALE}
	fi
   	
	if [[ ${STREAMS} = "default" ]] ; then
	    STREAMS=${TPCH_STREAMS}
	fi

	;;

    *)		
	echo "Invalid Benchmark Specified. BENCHMARK = ${BENCHMARK}. Expected values: [ YCSB | DebitCredit | OrderEntry ]"; 
	echo -e "$USAGE"; exit 1;;

esac

case ${DATABASE,,} in
	trafodion)
		DATABASE=Trafodion

		if [[ ${SCHEMA} = "default" ]] ; then
			SCHEMA=$(grep schema ${JAVABENCH_TEST_HOME}/${DATABASE,,}.properties | awk '{ print $3 }')
		fi

		if [[ ${DRIVER_TYPE} = "default" ]] ; then
			DRIVER_TYPE="TYPE4"
		fi

		if [[ ${DRIVER_LOCATION} = "default" ]] ; then
			DRIVER_LOCATION="LOCAL"
		fi
		;;

	seaquest)
		DATABASE=SeaQuest

		if [[ ${SCHEMA} = "default" ]] ; then
			SCHEMA=$(grep schema ${JAVABENCH_TEST_HOME}/${DATABASE,,}.properties | awk '{ print $3 }')
		fi

		DRIVER_TYPE="TYPE4"

		if [[ ${DRIVER_LOCATION} = "default" ]] ; then
			DRIVER_LOCATION="LOCAL"
		fi
		;;

	hbase)
		DATABASE="Hbase"
		
		if [[ ${SCHEMA} = "default" ]] ; then
			SCHEMA=${BENCHMARK^^}
		fi
		
		DRIVER_TYPE="TYPE2"
		
		if [[ ${DRIVER_LOCATION} = "default" ]] ; then
			DRIVER_LOCATION="SYSTEM"
		fi
		;;
esac

case ${DRIVER_LOCATION,,} in
    local)  DEFAULT_DRIVERS=1;;
    system) DEFAULT_DRIVERS=$(echo ${SYSTEM_NODE_NAMES} | wc -w)
            DRIVER_NODE_LIST="${SYSTEM_NODE_NAMES}";;
    driver) DEFAULT_DRIVERS=$(echo ${DRIVER_NODE_NAMES} | wc -w)
            DRIVER_NODE_LIST="${DRIVER_NODE_NAMES}";;
    *)
          echo "Invalid distributemode '${DRIVER_LOCATION}' Specified.   Expected values: [ local | system | driver ]";
          echo -e "$USAGE"; exit 1;;
esac


if [[ ${DRIVERS} = "default" ]] ; then
	if [[ ${STREAMS_PER_DRIVER} = "default" ]] ; then
                DRIVERS=${DEFAULT_DRIVERS}
		(( STREAMS_PER_DRIVER  = ${STREAMS} / ${DRIVERS} ))
	else
		(( DRIVERS  = ${STREAMS} / ${STREAMS_PER_DRIVER} ))
	fi
else
	(( STREAMS_PER_DRIVER  = ${STREAMS} / ${DRIVERS} ))
fi
(( STREAMS_LEFTOVER = ${STREAMS} - ( ${STREAMS_PER_DRIVER} * ${DRIVERS} ) ))

if [ -z "${LOGDIRECTORY}" ]; then
	export LOGDIRECTORY="${JAVABENCH_TEST_HOME}/log/${TESTID}.run.${BENCHMARK}.${DATABASE}.${WORKLOAD}/"
	if [ ! -f "${LOGDIRECTORY}" ] ; then
		mkdir -p ${LOGDIRECTORY}
	fi
fi
LOGFILE=${LOGDIRECTORY}/${TESTID}.run.${BENCHMARK}.${DATABASE}.${WORKLOAD}.log

if [[ ${OPTION_DEBUG} = "TRUE" ]] ; then
	OPTION_SYSINFO="FALSE"
	OPTION_MEASURE="FALSE"
	OPTION_PLANS="FALSE"
	OPTION_SUMMARY="FALSE"
fi

{

echo "
$0

           BENCHMARK = ${BENCHMARK}
            DATABASE = ${DATABASE}
            WORKLOAD = ${WORKLOAD}
             STREAMS = ${STREAMS}

              SCHEMA = ${SCHEMA}
               SCALE = ${SCALE}
                TIME = ${INTERVALS}
                
             DRIVERS = ${DRIVERS}
         DRIVER_TYPE = ${DRIVER_TYPE}
     DRIVER_LOCATION = ${DRIVER_LOCATION}
  STREAMS_PER_DRIVER = ${STREAMS_PER_DRIVER}
       EXTRA_STREAMS = ${STREAMS_LEFTOVER}
  
              TESTID = ${TESTID}
             SYSINFO = ${OPTION_SYSINFO}
             MEASURE = ${OPTION_MEASURE}
               PLANS = ${OPTION_PLANS}
              BOUNCE = ${OPTION_BOUNCE}
             SUMMARY = ${OPTION_SUMMARY}
               DEBUG = ${OPTION_DEBUG}
             OPTIONS = ${OPTIONS}
               
       LOG_DIRECTORY = ${LOGDIRECTORY} 
"

if [[ ${WORKLOAD} = "Autocommit" ]] ; then
	OPTIONS="autocommit ${OPTIONS}"
fi

echo ""
echo "===== Start ${BENCHMARK} Test ( TESTID = ${TESTID} ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]"
echo ""
STARTTIME=$SECONDS

if [[ $OPTION_SYSINFO = "TRUE" ]] ; then
	version.info.sh
fi

if [[ ${BENCHMARK,,} = "debitcredit" ]] ; then
	echo ""
	echo "===== Reseting DebitCredit History File   ( TESTID = ${TESTID} ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]"
	echo ""
	load.debitcredit.sh database ${DATABASE,,} schema ${SCHEMA} scale ${SCALE} table history testid ${TESTID} 
fi

if [[ ${DATABASE,,} != "seaquest" ]] ; then
	case ${OPTION_BOUNCE,,} in
	    dcs)	
		echo ""
                echo "===== Bounce DCS   ( TESTID = ${TESTID} ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]"
		echo ""
		bounce.dcs.sh;;
	    traf)
		echo ""
		echo "===== Bounce Trafodion   ( TESTID = ${TESTID} ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]"
                echo ""
		bounce.trafodion.sh;;
	    hbase)
		echo ""
		echo "===== Bounce HBase   ( TESTID = ${TESTID} ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]"
		echo ""
		bounce.hbase.sh;;
	    none)	;;
	    *)		;;	
	esac

	if [[ $OPTION_COMPACT = "TRUE" ]] ; then
		echo ""
		echo "===== Major Compaction   ( TESTID = ${TESTID} ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]"
		echo ""
		time pdsh -w ${SYSTEM_FIRST_NODE} "cd ${JAVABENCH_TEST_HOME};. profile;majorcompact.benchmark.sh benchmark ${BENCHMARK} schema ${SCHEMA} scale ${SCALE};"
	fi
fi

if [[ $OPTION_PLANS = "TRUE" ]] ; then
	echo ""
	echo "===== Capturing DDL, Stats, and Plans   ( TESTID = ${TESTID} ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]"
	echo ""
	show.${BENCHMARK,,}.sh testid ${TESTID} database ${DATABASE,,} schema ${SCHEMA} scale ${SCALE} all
fi

if (( $DRIVERS > 1 )) ; then 
	multidriver.configure.sh
fi

if [[ ${OPTION_BURNIN} != "default" ]] ; then
	echo ""
	echo "===== Burnin system with ${OPTION_BURNIN} minutes of processing   ( TESTID = ${TESTID} ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]"
	echo ""
	java -Ddbconnect.properties=${DATABASE,,}.properties WorkloadDriver ${BENCHMARK} workload ${WORKLOAD} schema ${SCHEMA} scale ${SCALE} intervals ${OPTION_BURNIN} streams ${STREAMS} ${OPTIONS}
fi

if [[ $OPTION_MEASURE = "TRUE" ]] ; then
	echo ""
	echo "===== Start measure   ( TESTID = ${TESTID} ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]"
	echo ""
	dtmstats.reset.sh
	start.measure.sh ${TESTID}
fi

echo ""
echo "===== ${BENCHMARK} , ${WORKLOAD} , ${SCALE} , ${STREAMS} , ${INTERVALS} ===== [ $(date +"%Y-%m-%d %H:%M:%S") ]"
echo ""

if [[ ${DRIVER_LOCATION,,} != "local" ]] ; then
	#######################
	# distributed driver test
	#######################

	if (( ${DRIVERS} > 1 )) ; then 
		#######################
		# Multiple Distributed Drivers
		echo ""
		echo "Starting ${STREAMS} streams over ${DRIVERS} distributed drivers"
		echo ""

		(( DRIVER = 0 ))
		while (( ${DRIVER} < ${DRIVERS} )) ; do
			for NODE in ${DRIVER_NODE_LIST} ; do
				(( DRIVER = ${DRIVER} + 1 ))
				if [ ${STREAMS_LEFTOVER} -gt 0 ] ; then
					(( STREAMS_THIS_DRIVER = ${STREAMS_PER_DRIVER} + 1 ))
					(( STREAMS_LEFTOVER = ${STREAMS_LEFTOVER} - 1 ))
				else
					STREAMS_THIS_DRIVER=${STREAMS_PER_DRIVER}
				fi
				if (( ${DRIVER} <= ${DRIVERS} )) ; then
					#echo "Starting Driver ${DRIVER} on node ${NODE}"
					pdsh -w ${NODE} "{
					cd ${JAVABENCH_TEST_HOME}
					. profile
					export DRIVERID=${DRIVER}
					export TESTID=${TESTID}
					export LOGDIRECTORY=${LOGDIRECTORY}
					if [ ! -f "${LOGDIRECTORY}" ] ; then mkdir -p ${LOGDIRECTORY}; fi
					if [[ ${DRIVER_TYPE} = "TYPE2" ]] ; then
						. profile.t2
						java -Xmx2g -Ddbconnect.properties=${DATABASE,,}.t2.properties WorkloadDriver ${BENCHMARK} workload ${WORKLOAD} schema ${SCHEMA} scale ${SCALE} intervals ${INTERVALS} driver ${DRIVER} of ${DRIVERS} streams ${STREAMS_THIS_DRIVER} ${OPTIONS}
					else
						java -Ddbconnect.properties=${DATABASE,,}.properties WorkloadDriver ${BENCHMARK} workload ${WORKLOAD} schema ${SCHEMA} scale ${SCALE} intervals ${INTERVALS} driver ${DRIVER} of ${DRIVERS} streams ${STREAMS_THIS_DRIVER} ${OPTIONS} 
					fi
					}" > ${LOGDIRECTORY}/driver.${NODE}.${DRIVER}.log &
				fi
			done
		done

		# 
		echo ""
		echo "Starting Master Coordinator"
		echo ""
		java DriverCoordinator ${BENCHMARK} drivers ${DRIVERS} testid ${TESTID} 

		wait

	else
		#######################
		# Single Distributed Driver
		NODE=$(echo ${DRIVER_NODE_LIST} | awk '{print $1}')
		echo ""
		echo "Starting ${STREAMS} streams over single driver on ${NODE}"
		echo ""

		#echo "Starting Driver ${DRIVER} on node ${NODE}"
		pdsh -w ${NODE} "{
		cd ${JAVABENCH_TEST_HOME}
		. profile
		export DRIVERID=${DRIVER}
		export TESTID=${TESTID}
		export LOGDIRECTORY=${LOGDIRECTORY}
		if [ ! -f "${LOGDIRECTORY}" ] ; then mkdir -p ${LOGDIRECTORY}; fi
		if [[ ${DRIVER_TYPE} = "TYPE2" ]] ; then
			. profile.t2
			java -Xmx2g -Ddbconnect.properties=${DATABASE,,}.t2.properties WorkloadDriver ${BENCHMARK} workload ${WORKLOAD} schema ${SCHEMA} scale ${SCALE} intervals ${INTERVALS} streams ${STREAMS} ${OPTIONS}
		else
			java -Ddbconnect.properties=${DATABASE,,}.properties WorkloadDriver ${BENCHMARK} workload ${WORKLOAD} schema ${SCHEMA} scale ${SCALE} intervals ${INTERVALS} streams ${STREAMS} ${OPTIONS}
		fi
		}"
	fi
else

	#######################
	# local driver test
	#######################
	if (( $DRIVERS > 1 )) ; then 
		#######################
		# multiple Local Drivers
		echo ""
		echo "Starting ${STREAMS} streams over ${DRIVERS} local drivers"
		echo ""

		(( DRIVER = 0 ))
		while (( ${DRIVER} < ${DRIVERS} )) ; do
			(( DRIVER = ${DRIVER} + 1 ))
			if [ ${STREAMS_LEFTOVER} -gt 0 ] ; then
				(( STREAMS_THIS_DRIVER = ${STREAMS_PER_DRIVER} + 1 ))
				(( STREAMS_LEFTOVER = ${STREAMS_LEFTOVER} - 1 ))
			else
				STREAMS_THIS_DRIVER=${STREAMS_PER_DRIVER}
			fi
			if (( ${DRIVER} <= ${DRIVERS} )) ; then
				#echo "Starting Driver ${DRIVER}"
				{
				if [[ ${DRIVER_TYPE} = "TYPE2" ]] ; then
					. profile.t2
					java -Xmx2g -Ddbconnect.properties=${DATABASE,,}.t2.properties WorkloadDriver ${BENCHMARK} workload ${WORKLOAD} schema ${SCHEMA} scale ${SCALE} intervals ${INTERVALS} driver ${DRIVER} of ${DRIVERS} streams ${STREAMS_THIS_DRIVER} ${OPTIONS}
				else
					java -Ddbconnect.properties=${DATABASE,,}.properties WorkloadDriver ${BENCHMARK} workload ${WORKLOAD} schema ${SCHEMA} scale ${SCALE} intervals ${INTERVALS} driver ${DRIVER} of ${DRIVERS} streams ${STREAMS_THIS_DRIVER} ${OPTIONS}
				fi
				} > ${LOGDIRECTORY}/driver.${DRIVER}.log &
			fi
		done

		# 
		echo "Starting Master Coordinator"
		java DriverCoordinator ${BENCHMARK} drivers ${DRIVERS} testid ${TESTID} 

		wait

	#######################
	else  # 1 Local Driver
		echo ""
		echo "Starting ${STREAMS} streams over single local driver"
		echo ""

		if [[ ${DRIVER_TYPE} = "TYPE2" ]] ; then
			. profile.t2
			java -Xmx2g -Ddbconnect.properties=${DATABASE,,}.t2.properties WorkloadDriver ${BENCHMARK} workload ${WORKLOAD} schema ${SCHEMA} scale ${SCALE} intervals ${INTERVALS} streams ${STREAMS} ${OPTIONS}
		else
			java -Ddbconnect.properties=${DATABASE,,}.properties WorkloadDriver ${BENCHMARK} workload ${WORKLOAD} schema ${SCHEMA} scale ${SCALE} intervals ${INTERVALS} streams ${STREAMS} ${OPTIONS}
		fi

	#######################
	fi
fi

if [[ $OPTION_MEASURE = "TRUE" ]] ; then
	echo ""
	echo "===== Stop measure   ( TESTID = ${TESTID} ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]"
	echo ""
	dtmstats.get.sh
	echo ""
	stop.measure.sh ${TESTID}
	TOTALTRANS=$(grep '^     Total Transactions : ' ${LOGFILE} | awk '{ transactions = $4 } END { print transactions }')
	quickMEAS.sh -L ${TESTID} -m -q ${TOTALTRANS}
fi

echo ""
echo "===== End ${BENCHMARK} Test ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]"
echo ""

if [[ $OPTION_SUMMARY = "TRUE" ]] ; then
	test.summary.sh testid ${TESTID}
fi

} | tee ${LOGFILE}
