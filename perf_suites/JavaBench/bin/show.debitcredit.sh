USAGE="usage: show.debitcredit.sh 
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
BENCHMARK=debitcredit
DATABASE=trafodion
TESTID=$(date +%y%m%d.%H%M)
SCHEMA="default"
SCALE=$DEBITCREDIT_SCALE
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

TABLES="branch teller account history"

######################################################
#  SHOWDDL
######################################################

if [[ ${OPTION_SHOWDDL} = "TRUE" ]] ; then

echo "===== showddl ( TESTID = ${TESTID} ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

{
echo "set statistics on;"
echo "log ${LOGDIRECTORY}/${TESTID}.showddl.${BENCHMARK}.log clear;"
for TABLE in ${TABLES} ; do
echo "showddl ${SCHEMA^^}.DC_${TABLE^^}_$SCALE;"
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
echo "log ${LOGDIRECTORY}/${TESTID}.showstats.${BENCHMARK}.log clear;"
for TABLE in ${TABLES} ; do
echo "showstats for table ${SCHEMA^^}.DC_${TABLE^^}_$SCALE on existing columns;"
done
echo "exit;"
} | ${CI_COMMAND} > /dev/null

echo "===== showstats detail  ( TESTID = ${TESTID} ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

{
echo "set statistics on;"
echo "log ${LOGDIRECTORY}/${TESTID}.showstats.${BENCHMARK}.detail.log clear;"
for TABLE in ${TABLES} ; do
echo "showstats for table ${SCHEMA^^}.DC_${TABLE^^}_$SCALE on existing columns detail;"
done
echo "exit;"
} | ${CI_COMMAND} > /dev/null

fi

######################################################
#  SHOWPLANS
######################################################

if [[ ${OPTION_SHOWPLANS} = "TRUE" ]] ; then

echo "===== showplans explain summary ( TESTID = ${TESTID} ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

{
echo "set statistics on;"
echo "set schema $SCHEMA;"
echo "log ${LOGDIRECTORY}/${TESTID}.showplans.${BENCHMARK}.explain.log clear;"

if [ ! -z "${TEST_CQD_FILE}" ] ; then
echo "obey ${TEST_CQD_FILE};"
fi

echo "explain options 'f' update $SCHEMA.DC_account_$SCALE set ACCOUNT_BALANCE = ACCOUNT_BALANCE + ? where ACCOUNT_ID = ?;"
echo "explain options 'f' update $SCHEMA.DC_teller_$SCALE set TELLER_BALANCE = TELLER_BALANCE + ? where TELLER_ID = ?;"
echo "explain options 'f' update $SCHEMA.DC_branch_$SCALE set BRANCH_BALANCE = BRANCH_BALANCE + ? where BRANCH_ID = ?;"
echo "explain options 'f' insert into $SCHEMA.DC_history_$SCALE ( TRANSACTION_TIME, TELLER_ID, BRANCH_ID,  ACCOUNT_ID, AMOUNT, HISTORY_FILLER ) values ( CURRENT_TIMESTAMP , ?, ?, ?, ?, ? );"
echo "explain options 'f' select ACCOUNT_BALANCE from $SCHEMA.DC_account_$SCALE where ACCOUNT_ID = ?;"
echo "exit;"
} | ${CI_COMMAND} > /dev/null

echo "===== showplans explain detail  ( TESTID = ${TESTID} ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

{
echo "set statistics on;"
echo "set schema $SCHEMA;"
echo "log ${LOGDIRECTORY}/${TESTID}.showplans.${BENCHMARK}.fullexplain.log clear;"

if [ ! -z "${TEST_CQD_FILE}" ] ; then
echo "obey ${TEST_CQD_FILE};"
fi

echo "explain update $SCHEMA.DC_account_$SCALE set ACCOUNT_BALANCE = ACCOUNT_BALANCE + ? where ACCOUNT_ID = ?;"
echo "explain update $SCHEMA.DC_teller_$SCALE set TELLER_BALANCE = TELLER_BALANCE + ? where TELLER_ID = ?;"
echo "explain update $SCHEMA.DC_branch_$SCALE set BRANCH_BALANCE = BRANCH_BALANCE + ? where BRANCH_ID = ?;"
echo "explain insert into $SCHEMA.DC_history_$SCALE ( TRANSACTION_TIME, TELLER_ID, BRANCH_ID,  ACCOUNT_ID, AMOUNT, HISTORY_FILLER ) values ( CURRENT_TIMESTAMP , ?, ?, ?, ?, ? );"
echo "explain select ACCOUNT_BALANCE from $SCHEMA.DC_account_$SCALE where ACCOUNT_ID = ?;"
echo "exit;"
} | ${CI_COMMAND} > /dev/null

echo "===== showplans detail plans  ( TESTID = ${TESTID} ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

{
echo "set statistics on;"
echo "set schema $SCHEMA;"
echo "log ${LOGDIRECTORY}/${TESTID}.showplans.${BENCHMARK}.showplan.log clear;"

if [ ! -z "${TEST_CQD_FILE}" ] ; then
echo "obey ${TEST_CQD_FILE};"
fi

echo "showplan update $SCHEMA.DC_account_$SCALE set ACCOUNT_BALANCE = ACCOUNT_BALANCE + ? where ACCOUNT_ID = ?;"
echo "showplan update $SCHEMA.DC_teller_$SCALE set TELLER_BALANCE = TELLER_BALANCE + ? where TELLER_ID = ?;"
echo "showplan update $SCHEMA.DC_branch_$SCALE set BRANCH_BALANCE = BRANCH_BALANCE + ? where BRANCH_ID = ?;"
echo "showplan insert into $SCHEMA.DC_history_$SCALE ( TRANSACTION_TIME, TELLER_ID, BRANCH_ID,  ACCOUNT_ID, AMOUNT, HISTORY_FILLER ) values ( CURRENT_TIMESTAMP , ?, ?, ?, ?, ? );"
echo "showplan select ACCOUNT_BALANCE from $SCHEMA.DC_account_$SCALE where ACCOUNT_ID = ?;"
echo "exit;"
} | ${CI_COMMAND} > /dev/null

fi

######################################################
#  SHOWHBASE
######################################################

if [[ ${OPTION_SHOWHBASE} = "TRUE" ]] ; then

if [[ ${DATABASE,,} = "trafodion" ]] ; then

echo "===== showhbase ( TESTID = ${TESTID} ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

for TABLE in ${TABLES} ; do
TABLENAME=${SCHEMA^^}.DC_${TABLE^^}_${SCALE}
curl -sS --noproxy '*' "http://${SYSTEM_NAMEMGR_URL}:${SYSTEM_NAMEMGR_PORT}/table.jsp?name=${TABLENAME}" >> ${LOGDIRECTORY}/${TESTID}.showhbase.${BENCHMARK}.log
done

fi

fi

