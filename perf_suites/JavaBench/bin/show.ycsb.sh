USAGE="usage: show.ycsb.sh 
    [ -db|--database|database <<database>> ]
    [ -id|--testid|testid <<testid>> ]
    [ -sch|--schema|schema <<schema>> ]
    [ -sc|--scale|scale <<scalefactor>> ]
    [ -ddl|--showddl|showddl ]
    [ -stats|--showstats|showstats ]
    [ -plans|--showplans|showplans ]
    [ -hbase|--showhbase|showhbase ]
    [ --all|all ]
    [ -h|--help|help ]"

#Default Values
BENCHMARK=ycsb
DATABASE=trafodion
TESTID=$(date +%y%m%d.%H%M)
SCHEMA="default"
SCALE=$YCSB_SCALE
OPTION_SHOWDDL=FALSE
OPTION_SHOWSTATS=FALSE
OPTION_SHOWPLANS=FALSE
OPTION_SHOWHBASE=FALSE
OPTION_ALL=FALSE

while [[ $# > 0 ]] ; do
key="$1"; shift;
case $key in
    -db|--database|database)		DATABASE="$1"; shift;;
    -id|--testid|testid)		TESTID="$1"; shift;;
    -sch|--schema|schema)		SCHEMA="$1"; shift;;
    -sc|--scale|scale)			SCALE="$1"; shift;;
    -ddl|--showddl|showddl)		OPTION_SHOWDDL="TRUE";;
    -stats|--showstats|showstats)	OPTION_SHOWSTATS="TRUE";;
    -plans|--showplans|showplans)	OPTION_SHOWPLANS="TRUE";;
    -hbase|--showhbase|showhbase)	OPTION_SHOWHBASE="TRUE";;
    --all|all)				OPTION_ALL="TRUE";;
    -h|--help|help)			echo -e "$USAGE"; exit 1;;
    *)					echo "Invalid input switch: $key"; echo -e "$USAGE"; exit 1;;
esac
done

if [ ${SCHEMA} = "default" ] ; then
    SCHEMA=$(grep schema ${JAVABENCH_TEST_HOME}/${DATABASE,,}.properties | awk '{ print $3 }')
fi
if [ -z "$LOGDIRECTORY" ]; then
export LOGDIRECTORY="${JAVABENCH_TEST_HOME}/log/${TESTID}.show.${BENCHMARK}/"
if [ ! -f "${LOGDIRECTORY}" ] ; then
mkdir -p ${LOGDIRECTORY}
fi
fi

if [[ $OPTION_ALL = TRUE ]] ; then
OPTION_SHOWDDL="TRUE"
OPTION_SHOWSTATS="TRUE"
OPTION_SHOWPLANS="TRUE"
OPTION_SHOWHBASE="TRUE"
fi

echo "
$0

            DATABASE = ${DATABASE}
              SCHEMA = ${SCHEMA}
               SCALE = ${SCALE}
              TESTID = ${TESTID}
      OPTION_SHOWDDL = ${OPTION_SHOWDDL}
    OPTION_SHOWSTATS = ${OPTION_SHOWSTATS}
    OPTION_SHOWPLANS = ${OPTION_SHOWPLANS}
    OPTION_SHOWHBASE = ${OPTION_SHOWHBASE}
           ( logs will be found in ${LOGDIRECTORY} )
"

######################################################
#  SHOWDDL
######################################################

if [[ ${OPTION_SHOWDDL} = "TRUE" ]] ; then

echo "===== showddl ( TESTID = ${TESTID} ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

{
echo "set statistics on;"
echo "log ${LOGDIRECTORY}/${TESTID}.showddl.${BENCHMARK}.log clear;"
echo "showddl ${SCHEMA}.YCSB_TABLE_$SCALE;"
echo "exit;"
} | ${CI_COMMAND} > /dev/null

fi

######################################################
#  SHOWSTATS
######################################################

if [[ ${OPTION_SHOWSTATS} = "TRUE" ]] ; then

echo "===== showstats summary ( TESTID = ${TESTID} ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

{
echo "set statistics on;"
echo "log ${LOGDIRECTORY}/${TESTID}.showstats.${BENCHMARK}.log clear;"
echo "showstats for table $SCHEMA.YCSB_TABLE_$SCALE on existing columns;"
echo "exit;"
} | ${CI_COMMAND} > /dev/null

echo "===== showstats detail  ( TESTID = ${TESTID} ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

{
echo "set statistics on;"
echo "log ${LOGDIRECTORY}/${TESTID}.showstats.${BENCHMARK}.detail.log clear;"
echo "showstats for table $SCHEMA.YCSB_TABLE_$SCALE on existing columns detail;"
echo "exit;"
} | ${CI_COMMAND} > /dev/null

fi

######################################################
#  SHOWPLANS
######################################################

if [[ ${OPTION_SHOWPLANS} = "TRUE" ]] ; then

echo "===== showplans ( TESTID = ${TESTID} ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

{
echo "set statistics on;"
echo "set schema $SCHEMA;"
echo "log ${LOGDIRECTORY}/${TESTID}.showplans.${BENCHMARK}.log clear;"

if [ ! -z "${TEST_CQD_FILE}" ] ; then
echo "obey ${TEST_CQD_FILE};"
fi

echo "explain options 'f' select * from $SCHEMA.YCSB_TABLE_$SCALE where PRIM_KEY = ?;"
echo "explain  select * from $SCHEMA.YCSB_TABLE_$SCALE where PRIM_KEY = ?;"
echo "showplan select * from $SCHEMA.YCSB_TABLE_$SCALE where PRIM_KEY = ?;"

echo "explain options 'f' upsert into $SCHEMA.YCSB_TABLE_$SCALE ( PRIM_KEY, FIELD0, FIELD1, FIELD2, FIELD3, FIELD4, FIELD5, FIELD6, FIELD7, FIELD8, FIELD9 ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"
echo "explain  upsert into $SCHEMA.YCSB_TABLE_$SCALE ( PRIM_KEY, FIELD0, FIELD1, FIELD2, FIELD3, FIELD4, FIELD5, FIELD6, FIELD7, FIELD8, FIELD9 ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"
echo "showplan upsert into $SCHEMA.YCSB_TABLE_$SCALE ( PRIM_KEY, FIELD0, FIELD1, FIELD2, FIELD3, FIELD4, FIELD5, FIELD6, FIELD7, FIELD8, FIELD9 ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"

echo "exit;"
} | ${CI_COMMAND} > /dev/null

fi

######################################################
#  SHOWHBASE
######################################################

if [[ ${OPTION_SHOWHBASE} = "TRUE" ]] ; then

echo "===== showhbase ( TESTID = ${TESTID} ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

TABLENAME=${SCHEMA^^}.YCSB_TABLE_${SCALE}
curl -sS --noproxy '*' "http://${SYSTEM_NAMEMGR_URL}:${SYSTEM_NAMEMGR_PORT}/table.jsp?name=${TABLENAME}" >> ${LOGDIRECTORY}/${TESTID}.showhbase.${BENCHMARK}.log

fi

