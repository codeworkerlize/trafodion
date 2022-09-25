USAGE="usage: show.orderentry.sh 
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
BENCHMARK=orderentry
DATABASE=trafodion
TESTID=$(date +%y%m%d.%H%M)
SCHEMA="default"
SCALE=$ORDERENTRY_SCALE
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

TABLES="customer district history item neworder orderline orders stock warehouse"

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
echo "showddl ${SCHEMA^^}.OE_${TABLE^^}_$SCALE;"
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
echo "showstats for table ${SCHEMA^^}.OE_${TABLE^^}_$SCALE on existing columns;"
done
echo "exit;"
} | ${CI_COMMAND} > /dev/null

echo "===== showstats detail  ( TESTID = ${TESTID} ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

{
echo "set statistics on;"
echo "log ${LOGDIRECTORY}/${TESTID}.showstats.${BENCHMARK}.detail.log clear;"
for TABLE in ${TABLES} ; do
echo "showstats for table ${SCHEMA^^}.OE_${TABLE^^}_$SCALE on existing columns detail;"
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
echo "set schema $SCHEMA;"
echo "log ${LOGDIRECTORY}/${TESTID}.showplans.${BENCHMARK}.log clear;"

if [ ! -z "${TEST_CQD_FILE}" ] ; then
echo "obey ${TEST_CQD_FILE};"
fi

echo "values('NEWORDER_SELECT_WAREHOUSE_STMT');"
echo "explain options 'f' select W_TAX from $SCHEMA.OE_warehouse_$SCALE where W_ID = ?;"
echo "explain select W_TAX from $SCHEMA.OE_warehouse_$SCALE where W_ID = ?;"
echo "showplan select W_TAX from $SCHEMA.OE_warehouse_$SCALE where W_ID = ?;"

echo "values('NEWORDER_SELECT_CUSTOMER_STMT');"
echo "explain options 'f' select C_DISCOUNT, C_LAST, C_CREDIT from $SCHEMA.OE_customer_$SCALE where C_W_ID = ? and C_D_ID = ? and C_ID = ?;"
echo "explain select C_DISCOUNT, C_LAST, C_CREDIT from $SCHEMA.OE_customer_$SCALE where C_W_ID = ? and C_D_ID = ? and C_ID = ?;"
echo "showplan select C_DISCOUNT, C_LAST, C_CREDIT from $SCHEMA.OE_customer_$SCALE where C_W_ID = ? and C_D_ID = ? and C_ID = ?;"

echo "values('NEWORDER_SELECT_DISTRICT_STMT');"
echo "explain options 'f' select D_TAX, D_NEXT_O_ID from $SCHEMA.OE_district_$SCALE where D_W_ID = ? and D_ID = ?;"
echo "explain select D_TAX, D_NEXT_O_ID from $SCHEMA.OE_district_$SCALE where D_W_ID = ? and D_ID = ?;"
echo "showplan select D_TAX, D_NEXT_O_ID from $SCHEMA.OE_district_$SCALE where D_W_ID = ? and D_ID = ?;"

echo "values('NEWORDER_UPDATE_DISTRICT_STMT');"
echo "explain options 'f' update $SCHEMA.OE_district_$SCALE set D_NEXT_O_ID = D_NEXT_O_ID + 1 where D_W_ID = ? and D_ID = ?;"
echo "explain update $SCHEMA.OE_district_$SCALE set D_NEXT_O_ID = D_NEXT_O_ID + 1 where D_W_ID = ? and D_ID = ?;"
echo "showplan update $SCHEMA.OE_district_$SCALE set D_NEXT_O_ID = D_NEXT_O_ID + 1 where D_W_ID = ? and D_ID = ?;"

echo "values('NEWORDER_INSERT_ORDER_STMT');"
echo "explain options 'f' insert into $SCHEMA.OE_orders_$SCALE( O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL ) values ( ? , ? , ? , ? , CURRENT_TIMESTAMP, ? , ? );"
echo "explain insert into $SCHEMA.OE_orders_$SCALE( O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL ) values ( ? , ? , ? , ? , CURRENT_TIMESTAMP, ? , ? );"
echo "showplan insert into $SCHEMA.OE_orders_$SCALE( O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL ) values ( ? , ? , ? , ? , CURRENT_TIMESTAMP, ? , ? );"

echo "values('NEWORDER_INSERT_NEWORDER_STMT');"
echo "explain options 'f' insert into $SCHEMA.OE_neworder_$SCALE( NO_W_ID, NO_D_ID, NO_O_ID ) values ( ? , ? , ? );"
echo "explain insert into $SCHEMA.OE_neworder_$SCALE( NO_W_ID, NO_D_ID, NO_O_ID ) values ( ? , ? , ? );"
echo "showplan insert into $SCHEMA.OE_neworder_$SCALE( NO_W_ID, NO_D_ID, NO_O_ID ) values ( ? , ? , ? );"

echo "values('NEWORDER_SELECT_ITEM_STMT');"
echo "explain options 'f' select I_PRICE, I_NAME, I_DATA from $SCHEMA.OE_item_$SCALE where I_ID = ?;"
echo "explain select I_PRICE, I_NAME, I_DATA from $SCHEMA.OE_item_$SCALE where I_ID = ?;"
echo "showplan select I_PRICE, I_NAME, I_DATA from $SCHEMA.OE_item_$SCALE where I_ID = ?;"

echo "values('NEWORDER_SELECT_STOCK_STMT');"
echo "explain options 'f' select S_QUANTITY, S_DATA, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10 from $SCHEMA.OE_stock_$SCALE where S_W_ID = ? and S_I_ID = ?;"
echo "explain select S_QUANTITY, S_DATA, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10 from $SCHEMA.OE_stock_$SCALE where S_W_ID = ? and S_I_ID = ?;"
echo "showplan select S_QUANTITY, S_DATA, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10 from $SCHEMA.OE_stock_$SCALE where S_W_ID = ? and S_I_ID = ?;"

echo "values('NEWORDER_UPDATE_STOCK_STMT');"
echo "explain options 'f' update $SCHEMA.OE_stock_$SCALE set S_QUANTITY = ?, S_YTD = S_YTD + ?, S_ORDER_CNT = S_ORDER_CNT + 1, S_REMOTE_CNT = S_REMOTE_CNT + ? where S_W_ID = ? and S_I_ID = ?;"
echo "explain update $SCHEMA.OE_stock_$SCALE set S_QUANTITY = ?, S_YTD = S_YTD + ?, S_ORDER_CNT = S_ORDER_CNT + 1, S_REMOTE_CNT = S_REMOTE_CNT + ? where S_W_ID = ? and S_I_ID = ?;"
echo "showplan update $SCHEMA.OE_stock_$SCALE set S_QUANTITY = ?, S_YTD = S_YTD + ?, S_ORDER_CNT = S_ORDER_CNT + 1, S_REMOTE_CNT = S_REMOTE_CNT + ? where S_W_ID = ? and S_I_ID = ?;"

echo "values('NEWORDER_INSERT_ORDERLINE_STMT');"
echo "explain options 'f' insert into $SCHEMA.OE_orderline_$SCALE ( OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO ) values ( ?, ?, ?, ?, ?, ?, ?, ?, ? );"
echo "explain insert into $SCHEMA.OE_orderline_$SCALE ( OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO ) values ( ?, ?, ?, ?, ?, ?, ?, ?, ? );"
echo "showplan insert into $SCHEMA.OE_orderline_$SCALE ( OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO ) values ( ?, ?, ?, ?, ?, ?, ?, ?, ? );"

echo "values('PAYMENT_SELECT_WAREHOUSE_STMT');"
echo "explain options 'f' select W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP from $SCHEMA.OE_warehouse_$SCALE where W_ID = ?;"
echo "explain select W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP from $SCHEMA.OE_warehouse_$SCALE where W_ID = ?;"
echo "showplan select W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP from $SCHEMA.OE_warehouse_$SCALE where W_ID = ?;"

echo "values('PAYMENT_SELECT_CUSTOMERID_STMT');"
echo "explain options 'f' select C_ID from $SCHEMA.OE_customer_$SCALE where C_W_ID = ? and C_D_ID = ? and C_LAST = ? order by C_FIRST;"
echo "explain select C_ID from $SCHEMA.OE_customer_$SCALE where C_W_ID = ? and C_D_ID = ? and C_LAST = ? order by C_FIRST;"
echo "showplan select C_ID from $SCHEMA.OE_customer_$SCALE where C_W_ID = ? and C_D_ID = ? and C_LAST = ? order by C_FIRST;"

echo "values('PAYMENT_SELECT_DISTRICT_STMT');"
echo "explain options 'f' select D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP from $SCHEMA.OE_district_$SCALE where D_W_ID = ? and D_ID = ?;"
echo "explain select D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP from $SCHEMA.OE_district_$SCALE where D_W_ID = ? and D_ID = ?;"
echo "showplan select D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP from $SCHEMA.OE_district_$SCALE where D_W_ID = ? and D_ID = ?;"

echo "values('PAYMENT_SELECT_CUSTOMER_STMT');"
echo "explain options 'f' select C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_DATA from $SCHEMA.OE_customer_$SCALE where C_W_ID = ? and C_D_ID = ? and C_ID = ?;"
echo "explain select C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_DATA from $SCHEMA.OE_customer_$SCALE where C_W_ID = ? and C_D_ID = ? and C_ID = ?;"
echo "showplan select C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_DATA from $SCHEMA.OE_customer_$SCALE where C_W_ID = ? and C_D_ID = ? and C_ID = ?;"

echo "values('PAYMENT_UPDATE_CUSTOMER_BC_STMT');"
echo "explain options 'f' update $SCHEMA.OE_customer_$SCALE set C_BALANCE = C_BALANCE + ? , C_YTD_PAYMENT = C_YTD_PAYMENT + ? , C_PAYMENT_CNT = C_PAYMENT_CNT + 1 , C_DATA = ? where C_W_ID = ? and C_D_ID = ? and C_ID = ? ;"
echo "explain update $SCHEMA.OE_customer_$SCALE set C_BALANCE = C_BALANCE + ? , C_YTD_PAYMENT = C_YTD_PAYMENT + ? , C_PAYMENT_CNT = C_PAYMENT_CNT + 1 , C_DATA = ? where C_W_ID = ? and C_D_ID = ? and C_ID = ? ;"
echo "showplan update $SCHEMA.OE_customer_$SCALE set C_BALANCE = C_BALANCE + ? , C_YTD_PAYMENT = C_YTD_PAYMENT + ? , C_PAYMENT_CNT = C_PAYMENT_CNT + 1 , C_DATA = ? where C_W_ID = ? and C_D_ID = ? and C_ID = ? ;"

echo "values('PAYMENT_UPDATE_CUSTOMER_STMT');"
echo "explain options 'f' update $SCHEMA.OE_customer_$SCALE set C_BALANCE = C_BALANCE + ?, C_YTD_PAYMENT = C_YTD_PAYMENT + ?, C_PAYMENT_CNT = C_PAYMENT_CNT + 1 where C_W_ID = ? and C_D_ID = ? and C_ID = ? ;"
echo "explain update $SCHEMA.OE_customer_$SCALE set C_BALANCE = C_BALANCE + ?, C_YTD_PAYMENT = C_YTD_PAYMENT + ?, C_PAYMENT_CNT = C_PAYMENT_CNT + 1 where C_W_ID = ? and C_D_ID = ? and C_ID = ? ;"
echo "showplan update $SCHEMA.OE_customer_$SCALE set C_BALANCE = C_BALANCE + ?, C_YTD_PAYMENT = C_YTD_PAYMENT + ?, C_PAYMENT_CNT = C_PAYMENT_CNT + 1 where C_W_ID = ? and C_D_ID = ? and C_ID = ? ;"

echo "values('PAYMENT_UPDATE_WAREHOUSE_STMT');"
echo "explain options 'f' update $SCHEMA.OE_warehouse_$SCALE set W_YTD = W_YTD + ? where W_ID = ?;"
echo "explain update $SCHEMA.OE_warehouse_$SCALE set W_YTD = W_YTD + ? where W_ID = ?;"
echo "showplan update $SCHEMA.OE_warehouse_$SCALE set W_YTD = W_YTD + ? where W_ID = ?;"

echo "values('PAYMENT_UPDATE_DISTRICT_STMT');"
echo "explain options 'f' update $SCHEMA.OE_district_$SCALE set D_YTD = D_YTD + ? where D_W_ID= ? and D_ID= ?;"
echo "explain update $SCHEMA.OE_district_$SCALE set D_YTD = D_YTD + ? where D_W_ID= ? and D_ID= ?;"
echo "showplan update $SCHEMA.OE_district_$SCALE set D_YTD = D_YTD + ? where D_W_ID= ? and D_ID= ?;"

echo "values('PAYMENT_INSERT_HISTORY_STMT');"
echo "explain options 'f' insert into $SCHEMA.OE_history_$SCALE (H_DATE, H_W_ID, H_D_ID, H_C_ID, H_C_W_ID, H_C_D_ID, H_AMOUNT, H_DATA) values ( CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?, ? );"
echo "explain insert into $SCHEMA.OE_history_$SCALE (H_DATE, H_W_ID, H_D_ID, H_C_ID, H_C_W_ID, H_C_D_ID, H_AMOUNT, H_DATA) values ( CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?, ? );"
echo "showplan insert into $SCHEMA.OE_history_$SCALE (H_DATE, H_W_ID, H_D_ID, H_C_ID, H_C_W_ID, H_C_D_ID, H_AMOUNT, H_DATA) values ( CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?, ? );"

echo "values('ORDERSTATUS_SELECT_CUSTOMERID_STMT');"
echo "explain options 'f' select C_ID from $SCHEMA.OE_customer_$SCALE where C_W_ID = ? and C_D_ID = ? and C_LAST = ? order by C_FIRST;"
echo "explain select C_ID from $SCHEMA.OE_customer_$SCALE where C_W_ID = ? and C_D_ID = ? and C_LAST = ? order by C_FIRST;"
echo "showplan select C_ID from $SCHEMA.OE_customer_$SCALE where C_W_ID = ? and C_D_ID = ? and C_LAST = ? order by C_FIRST;"

echo "values('ORDERSTATUS_SELECT_CUSTOMER_STMT');"
echo "explain options 'f' select C_FIRST, C_MIDDLE, C_LAST, C_BALANCE from $SCHEMA.OE_customer_$SCALE where C_W_ID = ? and C_D_ID = ? and C_ID = ?;"
echo "explain select C_FIRST, C_MIDDLE, C_LAST, C_BALANCE from $SCHEMA.OE_customer_$SCALE where C_W_ID = ? and C_D_ID = ? and C_ID = ?;"
echo "showplan select C_FIRST, C_MIDDLE, C_LAST, C_BALANCE from $SCHEMA.OE_customer_$SCALE where C_W_ID = ? and C_D_ID = ? and C_ID = ?;"

echo "values('ORDERSTATUS_SELECT_ORDERID_STMT');"
echo "explain options 'f' select max(O_ID) as O_ID from $SCHEMA.OE_orders_$SCALE where O_W_ID = ? and O_D_ID = ? and O_C_ID= ?;"
echo "explain select max(O_ID) as O_ID from $SCHEMA.OE_orders_$SCALE where O_W_ID = ? and O_D_ID = ? and O_C_ID= ?;"
echo "showplan select max(O_ID) as O_ID from $SCHEMA.OE_orders_$SCALE where O_W_ID = ? and O_D_ID = ? and O_C_ID= ?;"

echo "values('ORDERSTATUS_SELECT_ORDER_STMT');"
echo "explain options 'f' select O_ENTRY_D, O_CARRIER_ID, O_OL_CNT from $SCHEMA.OE_orders_$SCALE where O_W_ID = ? and O_D_ID = ? and O_ID = ?;"
echo "explain select O_ENTRY_D, O_CARRIER_ID, O_OL_CNT from $SCHEMA.OE_orders_$SCALE where O_W_ID = ? and O_D_ID = ? and O_ID = ?;"
echo "showplan select O_ENTRY_D, O_CARRIER_ID, O_OL_CNT from $SCHEMA.OE_orders_$SCALE where O_W_ID = ? and O_D_ID = ? and O_ID = ?;"

echo "values('ORDERSTATUS_SELECT_ORDERLINE_STMT');"
echo "explain options 'f' select OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D from $SCHEMA.OE_orderline_$SCALE where OL_W_ID = ? and OL_D_ID = ? and OL_O_ID = ? order by OL_NUMBER;"
echo "explain select OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D from $SCHEMA.OE_orderline_$SCALE where OL_W_ID = ? and OL_D_ID = ? and OL_O_ID = ? order by OL_NUMBER;"
echo "showplan select OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D from $SCHEMA.OE_orderline_$SCALE where OL_W_ID = ? and OL_D_ID = ? and OL_O_ID = ? order by OL_NUMBER;"

echo "values('DELIVERY_SELECT_NEWORDER_STMT');"
echo "explain options 'f' select NO_O_ID as O_ID from $SCHEMA.OE_neworder_$SCALE where NO_W_ID = ? and NO_D_ID = ? order by NO_O_ID;"
echo "explain select min(NO_O_ID) as O_ID from $SCHEMA.OE_neworder_$SCALE where NO_W_ID = ? and NO_D_ID = ?;"
echo "showplan select min(NO_O_ID) as O_ID from $SCHEMA.OE_neworder_$SCALE where NO_W_ID = ? and NO_D_ID = ?;"

echo "values('DELIVERY_DELETE_NEWORDER_STMT');"
echo "explain options 'f' delete from $SCHEMA.OE_neworder_$SCALE where NO_W_ID = ? and NO_D_ID = ? and NO_O_ID = ?;"
echo "explain delete from $SCHEMA.OE_neworder_$SCALE where NO_W_ID = ? and NO_D_ID = ? and NO_O_ID = ?;"
echo "showplan delete from $SCHEMA.OE_neworder_$SCALE where NO_W_ID = ? and NO_D_ID = ? and NO_O_ID = ?;"

echo "values('DELIVERY_SELECT_ORDER_STMT');"
echo "explain options 'f' select O_C_ID from $SCHEMA.OE_orders_$SCALE where O_W_ID = ? and O_D_ID = ? and O_ID = ?;"
echo "explain select O_C_ID from $SCHEMA.OE_orders_$SCALE where O_W_ID = ? and O_D_ID = ? and O_ID = ?;"
echo "showplan select O_C_ID from $SCHEMA.OE_orders_$SCALE where O_W_ID = ? and O_D_ID = ? and O_ID = ?;"

echo "values('DELIVERY_UPDATE_ORDER_STMT');"
echo "explain options 'f' update $SCHEMA.OE_orders_$SCALE set O_CARRIER_ID = ? where O_W_ID = ? and O_D_ID = ? and O_ID = ?;"
echo "explain update $SCHEMA.OE_orders_$SCALE set O_CARRIER_ID = ? where O_W_ID = ? and O_D_ID = ? and O_ID = ?;"
echo "showplan update $SCHEMA.OE_orders_$SCALE set O_CARRIER_ID = ? where O_W_ID = ? and O_D_ID = ? and O_ID = ?;"

echo "values('DELIVERY_UPDATE_ORDERLINE_STMT');"
echo "explain options 'f' update $SCHEMA.OE_orderline_$SCALE set OL_DELIVERY_D = CURRENT_TIMESTAMP where OL_W_ID = ? and OL_D_ID = ? and OL_O_ID = ?;"
echo "explain update $SCHEMA.OE_orderline_$SCALE set OL_DELIVERY_D = CURRENT_TIMESTAMP where OL_W_ID = ? and OL_D_ID = ? and OL_O_ID = ?;"
echo "showplan update $SCHEMA.OE_orderline_$SCALE set OL_DELIVERY_D = CURRENT_TIMESTAMP where OL_W_ID = ? and OL_D_ID = ? and OL_O_ID = ?;"

echo "values('DELIVERY_SELECT_ORDERLINE_STMT');"
echo "explain options 'f' select sum(OL_AMOUNT) as O_AMOUNT from $SCHEMA.OE_orderline_$SCALE where OL_W_ID = ? and OL_D_ID = ? and OL_O_ID = ?;"
echo "explain select sum(OL_AMOUNT) as O_AMOUNT from $SCHEMA.OE_orderline_$SCALE where OL_W_ID = ? and OL_D_ID = ? and OL_O_ID = ?;"
echo "showplan select sum(OL_AMOUNT) as O_AMOUNT from $SCHEMA.OE_orderline_$SCALE where OL_W_ID = ? and OL_D_ID = ? and OL_O_ID = ?;"

echo "values('DELIVERY_UPDATE_CUSTOMER_STMT');"
echo "explain options 'f' update $SCHEMA.OE_customer_$SCALE set C_BALANCE = C_BALANCE + ?, C_DELIVERY_CNT = C_DELIVERY_CNT + 1 where C_W_ID = ? and C_D_ID = ? and C_ID = ?;"
echo "explain update $SCHEMA.OE_customer_$SCALE set C_BALANCE = C_BALANCE + ?, C_DELIVERY_CNT = C_DELIVERY_CNT + 1 where C_W_ID = ? and C_D_ID = ? and C_ID = ?;"
echo "showplan update $SCHEMA.OE_customer_$SCALE set C_BALANCE = C_BALANCE + ?, C_DELIVERY_CNT = C_DELIVERY_CNT + 1 where C_W_ID = ? and C_D_ID = ? and C_ID = ?;"

echo "values('STOCKLEVEL_SELECT_DISTRICT_STMT');"
echo "explain options 'f' select D_NEXT_O_ID from $SCHEMA.OE_district_$SCALE where D_W_ID = ? and D_ID = ?;"
echo "explain select D_NEXT_O_ID from $SCHEMA.OE_district_$SCALE where D_W_ID = ? and D_ID = ?;"
echo "showplan select D_NEXT_O_ID from $SCHEMA.OE_district_$SCALE where D_W_ID = ? and D_ID = ?;"

echo "values('STOCKLEVEL_SELECT_STOCKCOUNT_STMT');"
echo "explain options 'f' select count(distinct(S_I_ID)) as CNTDIST from $SCHEMA.OE_orderline_$SCALE , $SCHEMA.OE_stock_$SCALE where OL_W_ID = ? and OL_D_ID = ? and OL_O_ID >= (? - 20) and OL_O_ID < ? and S_W_ID = ? and S_I_ID = OL_I_ID and S_QUANTITY < ?;"
echo "explain select count(distinct(S_I_ID)) as CNTDIST from $SCHEMA.OE_orderline_$SCALE , $SCHEMA.OE_stock_$SCALE where OL_W_ID = ? and OL_D_ID = ? and OL_O_ID >= (? - 20) and OL_O_ID < ? and S_W_ID = ? and S_I_ID = OL_I_ID and S_QUANTITY < ?;"
echo "showplan select count(distinct(S_I_ID)) as CNTDIST from $SCHEMA.OE_orderline_$SCALE , $SCHEMA.OE_stock_$SCALE where OL_W_ID = ? and OL_D_ID = ? and OL_O_ID >= (? - 20) and OL_O_ID < ? and S_W_ID = ? and S_I_ID = OL_I_ID and S_QUANTITY < ?;"

echo "exit;"
} | ${CI_COMMAND} > /dev/null

fi

######################################################
#  SHOWHBASE
######################################################

if [[ ${OPTION_SHOWHBASE} = "TRUE" ]] ; then

echo "===== showhbase ( TESTID = ${TESTID} ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

for TABLE in ${TABLES} ; do
TABLENAME=${SCHEMA^^}.OE_${TABLE^^}_${SCALE}
curl -sS --noproxy '*' "http://${SYSTEM_NAMEMGR_URL}:${SYSTEM_NAMEMGR_PORT}/table.jsp?name=${TABLENAME}" >> ${LOGDIRECTORY}/${TESTID}.showhbase.${BENCHMARK}.log
done

fi

