USAGE="usage: show.ed.atomics.sh 
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
BENCHMARK=ed
DATABASE=trafodion
TESTID=$(date +%y%m%d.%H%M)
SCHEMA="default"
SCALE=$MAX_ED_SCALE
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
export LOGDIRECTORY="${JAVABENCH_TEST_HOME}/log/${TESTID}.show.${BENCHMARK}.atomics/"
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
echo "log ${LOGDIRECTORY}/${TESTID}.showddl.${BENCHMARK}.atomics.log clear;"
(( INDX = 10 ))
while (( $INDX <= $SCALE )) ; do
echo "showddl $SCHEMA.ED_TABLE_$(printf '%02d' $INDX);"
(( INDX = $INDX + 1 ))
done
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
echo "log ${LOGDIRECTORY}/${TESTID}.showstats.${BENCHMARK}.atomics.log clear;"
(( INDX = 10 ))
while (( $INDX <= $SCALE )) ; do
echo "showstats for table $SCHEMA.ED_TABLE_$(printf '%02d' $INDX) on existing columns;"
(( INDX = $INDX + 1 ))
done
echo "exit;"
} | ${CI_COMMAND} > /dev/null

echo "===== showstats detail  ( TESTID = ${TESTID} ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

{
echo "set statistics on;"
echo "log ${LOGDIRECTORY}/${TESTID}.showstats.${BENCHMARK}.detail.log clear;"
for TABLE in ${TABLES} ; do
echo "showstats for table $SCHEMA.ED_TABLE_$(printf '%02d' $INDX) on existing columns detail;"
done
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
echo "log ${LOGDIRECTORY}/${TESTID}.showplans.${BENCHMARK}.atomics.log clear;"

if [ ! -z "${TEST_CQD_FILE}" ] ; then
echo "obey ${TEST_CQD_FILE};"
fi

echo "-- Summary Plans"
echo "-- count"
(( INDX = 10 ))
while (( $INDX <= $SCALE )) ; do
echo "explain options 'f' select count(*) from $SCHEMA.ED_TABLE_$(printf '%02d' $INDX);"
(( INDX = $INDX + 1 ))
done

echo "-- scan"
(( INDX = 10 ))
while (( $INDX <= $SCALE )) ; do
echo "explain options 'f' select * from $SCHEMA.ED_TABLE_$(printf '%02d' $INDX) where INTEGER_FACT < 0;"
(( INDX = $INDX + 1 ))
done

echo "-- orderby"
(( INDX = 10 ))
while (( $INDX <= $SCALE )) ; do
echo "explain options 'f' select [first 10] * from $SCHEMA.ED_TABLE_$(printf '%02d' $INDX) order by FOREIGN_KEY_01, FOREIGN_KEY_02, FOREIGN_KEY_03, FOREIGN_KEY_04, PRIM_KEY;"
(( INDX = $INDX + 1 ))
done

echo "-- groupby"
(( INDX = 10 ))
while (( $INDX <= $SCALE )) ; do
echo "explain options 'f' select FOREIGN_KEY_10, sum(INTEGER_FACT) as CNT from $SCHEMA.ED_TABLE_$(printf '%02d' $INDX) group by FOREIGN_KEY_10;"
(( INDX = $INDX + 1 ))
done

echo "-- Detail Plans"
echo "-- count"
(( INDX = 10 ))
while (( $INDX <= $SCALE )) ; do
echo "explain select count(*) from $SCHEMA.ED_TABLE_$(printf '%02d' $INDX);"
(( INDX = $INDX + 1 ))
done

echo "-- scan"
(( INDX = 10 ))
while (( $INDX <= $SCALE )) ; do
echo "explain select * from $SCHEMA.ED_TABLE_$(printf '%02d' $INDX) where INTEGER_FACT < 0;"
(( INDX = $INDX + 1 ))
done

echo "-- orderby"
(( INDX = 10 ))
while (( $INDX <= $SCALE )) ; do
echo "explain select [first 10] * from $SCHEMA.ED_TABLE_$(printf '%02d' $INDX) order by FOREIGN_KEY_01, FOREIGN_KEY_02, FOREIGN_KEY_03, FOREIGN_KEY_04, PRIM_KEY;"
(( INDX = $INDX + 1 ))
done

echo "-- groupby"
(( INDX = 10 ))
while (( $INDX <= $SCALE )) ; do
echo "explain select FOREIGN_KEY_10, sum(INTEGER_FACT) as CNT from $SCHEMA.ED_TABLE_$(printf '%02d' $INDX) group by FOREIGN_KEY_10;"
(( INDX = $INDX + 1 ))
done

echo "exit;"
} | ${CI_COMMAND} > /dev/null

fi

######################################################
#  SHOWHBASE
######################################################

if [[ ${OPTION_SHOWHBASE} = "TRUE" ]] ; then

echo "===== showhbase ( TESTID = ${TESTID} ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

(( INDX = 10 ))
while (( $INDX <= $SCALE )) ; do
TABLENAME=${SCHEMA^^}.ED_TABLE_$(printf '%02d' $INDX)
curl -sS --noproxy '*' "http://${SYSTEM_NAMEMGR_URL}:${SYSTEM_NAMEMGR_PORT}/table.jsp?name=${TABLENAME}" >> ${LOGDIRECTORY}/${TESTID}.showhbase.${BENCHMARK}.log
(( INDX = $INDX + 1 ))
done

fi

