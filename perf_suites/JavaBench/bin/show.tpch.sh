USAGE="usage: show.tpch.sh 
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
BENCHMARK=tpch
DATABASE=trafodion
TESTID=$(date +%y%m%d.%H%M)
SCHEMA="default"
SCALE=$TPCH_SCALE
OPTION_SHOWDDL=FALSE
OPTION_SHOWSTATS=FALSE
OPTION_SHOWPLANS=FALSE
OPTION_SHOWHBASE=FALSE
OPTION_ALL=FALSE

while [[ $# > 0 ]] ; do
key="$1"; shift;
case $key in
    -db|--database|database)            DATABASE="$1"; shift;;
    -id|--testid|testid)                TESTID="$1"; shift;;
    -sch|--schema|schema)               SCHEMA="$1"; shift;;
    -sc|--scale|scale)                  SCALE="$1"; shift;;
    -ddl|--showddl|showddl)             OPTION_SHOWDDL="TRUE";;
    -stats|--showstats|showstats)       OPTION_SHOWSTATS="TRUE";;
    -plans|--showplans|showplans)       OPTION_SHOWPLANS="TRUE";;
    -hbase|--showhbase|showhbase)       OPTION_SHOWHBASE="TRUE";;
    --all|all)                          OPTION_ALL="TRUE";;
    -h|--help|help)                     echo -e "$USAGE"; exit 1;;
    *)                                  echo "Invalid input switch: $key"; echo -e "$USAGE"; exit 1;;
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

TABLES="nation region part partsupp supplier orders lineitem customer"

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
echo "showddl ${SCHEMA^^}.TPCH_${TABLE^^}_$SCALE;"
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
echo "showstats for table ${SCHEMA^^}.TPCH_${TABLE^^}_$SCALE on existing columns;"
done
echo "exit;"
} | ${CI_COMMAND} > /dev/null

echo "===== showstats detail  ( TESTID = ${TESTID} ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

{
echo "set statistics on;"
echo "log ${LOGDIRECTORY}/${TESTID}.showstats.${BENCHMARK}.detail.log clear;"
for TABLE in ${TABLES} ; do
echo "showstats for table ${SCHEMA^^}.TPCH_${TABLE^^}_$SCALE on existing columns detail;"
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

echo "values('QUERY01');"
echo "prepare cmd from select l_returnflag, l_linestatus, cast(sum(l_quantity) as numeric(18,2)) as sum_qty, cast(sum(l_extendedprice) as numeric(18,2)) as sum_base_price, cast(sum(l_extendedprice * (1 - l_discount)) as numeric(18,2)) as sum_disc_price, cast(sum(l_extendedprice * (1 - l_discount)*(1+l_tax)) as numeric(18,2)) as sum_charge, cast(avg(l_quantity) as numeric(18,3)) as avg_qty, cast(avg(l_extendedprice) as numeric(18,3)) as avg_price, cast(avg(l_discount) as numeric(18,3)) as avg_disc, count(*) as count_order from TRAFODION.JAVABENCH.TPCH_LINEITEM_100 where l_shipdate <= '1998-12-01' group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus;"
echo "explain options 'f' cmd;"

echo "values('QUERY02');"
echo "prepare cmd from select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment from TRAFODION.JAVABENCH.TPCH_PART_100, TRAFODION.JAVABENCH.TPCH_SUPPLIER_100, TRAFODION.JAVABENCH.TPCH_PARTSUPP_100, TRAFODION.JAVABENCH.TPCH_NATION_100, TRAFODION.JAVABENCH.TPCH_REGION_100 where p_partkey = ps_partkey and s_suppkey = ps_suppkey and p_size = 15 and p_type like '%BRASS' and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'EUROPE' and ps_supplycost = (select min(ps_supplycost) from TRAFODION.JAVABENCH.TPCH_PARTSUPP_100, TRAFODION.JAVABENCH.TPCH_SUPPLIER_100, TRAFODION.JAVABENCH.TPCH_NATION_100, TRAFODION.JAVABENCH.TPCH_REGION_100 where p_partkey = ps_partkey and s_suppkey = ps_suppkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'EUROPE') order by s_acctbal desc, n_name, s_name, p_partkey;"
echo "explain options 'f' cmd;"

echo "values('QUERY03');"
echo "prepare cmd from select l_orderkey, sum(l_extendedprice*(1-l_discount)) as revenue, o_orderdate, o_shippriority from TRAFODION.JAVABENCH.TPCH_CUSTOMER_100, TRAFODION.JAVABENCH.TPCH_ORDERS_100, TRAFODION.JAVABENCH.TPCH_LINEITEM_100 where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < '1995-03-15' and l_shipdate > '1995-03-15' group by l_orderkey, o_orderdate, o_shippriority order by revenue desc, o_orderdate;"
echo "explain options 'f' cmd;"

echo "values('QUERY04');"
echo "prepare cmd from select o_orderpriority, count(*) as order_count from TRAFODION.JAVABENCH.TPCH_ORDERS_100 where o_orderdate >= '1993-07-01' and o_orderdate < '1993-10-01' and exists (select * from TRAFODION.JAVABENCH.TPCH_LINEITEM_100 where l_orderkey = o_orderkey and l_commitdate < l_receiptdate) group by o_orderpriority order by o_orderpriority;"
echo "explain options 'f' cmd;"

echo "values('QUERY05');"
echo "prepare cmd from select n_name, sum(l_extendedprice*(1-l_discount)) as revenue from TRAFODION.JAVABENCH.TPCH_CUSTOMER_100, TRAFODION.JAVABENCH.TPCH_ORDERS_100, TRAFODION.JAVABENCH.TPCH_LINEITEM_100, TRAFODION.JAVABENCH.TPCH_SUPPLIER_100, TRAFODION.JAVABENCH.TPCH_NATION_100, TRAFODION.JAVABENCH.TPCH_REGION_100 where c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'EUROPE' and o_orderdate >= '1994-01-01' and o_orderdate < '1995-10-01' group by n_name order by revenue desc;"
echo "explain options 'f' cmd;"

echo "values('QUERY06');"
echo "prepare cmd from select sum(l_extendedprice*l_discount) as revenue from TRAFODION.JAVABENCH.TPCH_LINEITEM_100 where l_shipdate >= '1994-01-01' and l_shipdate < '1995-10-01' and l_discount between (0.06 - 0.01) and (0.06 + 0.01) and l_quantity < 24;"
echo "explain options 'f' cmd;"

echo "values('QUERY07');"
echo "prepare cmd from select supp_nation, cust_nation, l_year, sum(volume) as revenue from (select n1.n_name as supp_nation, n2.n_name as cust_nation, left(l_shipdate,4) as l_year, l_extendedprice * (1 - l_discount) as volume from TRAFODION.JAVABENCH.TPCH_SUPPLIER_100, TRAFODION.JAVABENCH.TPCH_LINEITEM_100, TRAFODION.JAVABENCH.TPCH_ORDERS_100, TRAFODION.JAVABENCH.TPCH_CUSTOMER_100, TRAFODION.JAVABENCH.TPCH_NATION_100 n1, TRAFODION.JAVABENCH.TPCH_NATION_100 n2 where s_suppkey = l_suppkey and o_orderkey = l_orderkey and c_custkey = o_custkey and s_nationkey = n1.n_nationkey and c_nationkey = n2.n_nationkey and ((n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY') or (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')) and l_shipdate between '1995-01-01' and '1996-12-31') as shipping group by supp_nation, cust_nation, l_year order by supp_nation, cust_nation, l_year;"
echo "explain options 'f' cmd;"

echo "values('QUERY08');"
echo "prepare cmd from select o_year, sum(case when nation = 'BRAZIL' then volume else 0 end) / sum(volume) as mkt_share from (select left(l_shipdate,4) as o_year, l_extendedprice * (1 - l_discount) as volume, n2.n_name as nation from TRAFODION.JAVABENCH.TPCH_PART_100, TRAFODION.JAVABENCH.TPCH_SUPPLIER_100, TRAFODION.JAVABENCH.TPCH_LINEITEM_100, TRAFODION.JAVABENCH.TPCH_ORDERS_100, TRAFODION.JAVABENCH.TPCH_CUSTOMER_100, TRAFODION.JAVABENCH.TPCH_NATION_100 n1, TRAFODION.JAVABENCH.TPCH_NATION_100 n2, TRAFODION.JAVABENCH.TPCH_REGION_100 where p_partkey = l_partkey and s_suppkey = l_suppkey and l_orderkey = o_orderkey and o_custkey = c_custkey and c_nationkey = n1.n_nationkey and n1.n_regionkey = r_regionkey and r_name = 'AMERICA' and s_nationkey = n2.n_nationkey and o_orderdate between '1995-01-01' and '1996-12-31' and p_type = 'ECONOMY ANODIZED STEEL') as all_nations group by o_year order by o_year;"
echo "explain options 'f' cmd;"

echo "values('QUERY09');"
echo "prepare cmd from select nation, o_year, sum(amount) as sum_profit from (select n_name as nation, left(l_shipdate,4) as o_year, l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount from TRAFODION.JAVABENCH.TPCH_PART_100, TRAFODION.JAVABENCH.TPCH_SUPPLIER_100, TRAFODION.JAVABENCH.TPCH_LINEITEM_100, TRAFODION.JAVABENCH.TPCH_PARTSUPP_100, TRAFODION.JAVABENCH.TPCH_ORDERS_100, TRAFODION.JAVABENCH.TPCH_NATION_100 where s_suppkey = l_suppkey and ps_suppkey = l_suppkey and ps_partkey = l_partkey and p_partkey = l_partkey and o_orderkey = l_orderkey and s_nationkey = n_nationkey and p_name like '%green%') as profit group by nation, o_year order by nation, o_year desc;"
echo "explain options 'f' cmd;"

echo "values('QUERY10');"
echo "prepare cmd from select c_custkey, c_name, sum(l_extendedprice * (1 - l_discount)) as revenue, c_acctbal, n_name, c_address, c_phone, c_comment from TRAFODION.JAVABENCH.TPCH_CUSTOMER_100, TRAFODION.JAVABENCH.TPCH_ORDERS_100, TRAFODION.JAVABENCH.TPCH_LINEITEM_100, TRAFODION.JAVABENCH.TPCH_NATION_100 where c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate >= '1993-10-01' and o_orderdate < '1994-01-01' and l_returnflag = 'R' and c_nationkey = n_nationkey group by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment order by revenue desc;"
echo "explain options 'f' cmd;"

echo "values('QUERY11');"
echo "prepare cmd from select ps_partkey, sum(ps_supplycost * ps_availqty) as valuea from TRAFODION.JAVABENCH.TPCH_PARTSUPP_100, TRAFODION.JAVABENCH.TPCH_SUPPLIER_100, TRAFODION.JAVABENCH.TPCH_NATION_100 where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'GERMANY' group by ps_partkey having sum(ps_supplycost * ps_availqty) > (select sum(ps_supplycost * ps_availqty) * 0.01 from TRAFODION.JAVABENCH.TPCH_PARTSUPP_100, TRAFODION.JAVABENCH.TPCH_SUPPLIER_100, TRAFODION.JAVABENCH.TPCH_NATION_100 where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'GERMANY') order by valuea desc ;"
echo "explain options 'f' cmd;"

echo "values('QUERY12');"
echo "prepare cmd from select l_shipmode, sum(case when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH' then 1 else 0 end) as high_line_count, sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count from TRAFODION.JAVABENCH.TPCH_ORDERS_100, TRAFODION.JAVABENCH.TPCH_LINEITEM_100 where o_orderkey = l_orderkey and l_shipmode in ('MAIL', 'SHIP') and l_commitdate < l_receiptdate and l_shipdate < l_commitdate and l_receiptdate >= '1994-01-01' and l_receiptdate < '1995-10-01' group by l_shipmode order by l_shipmode;"
echo "explain options 'f' cmd;"

echo "values('QUERY13');"
echo "prepare cmd from select c_count, count(*) as custdist from (select c_custkey, count(o_orderkey) from TRAFODION.JAVABENCH.TPCH_CUSTOMER_100 left outer join TRAFODION.JAVABENCH.TPCH_ORDERS_100 on c_custkey = o_custkey and o_comment not like '%special%requests%' group by c_custkey ) as c_orders (c_custkey, c_count) group by c_count order by custdist desc, c_count desc;"
echo "explain options 'f' cmd;"

echo "values('QUERY14');"
echo "prepare cmd from select 100.00 * sum(case when p_type like 'PROMO%' then l_extendedprice*(1-l_discount) else 0 end) / sum(l_extendedprice*(1-l_discount)) as promo_revenue from TRAFODION.JAVABENCH.TPCH_LINEITEM_100, TRAFODION.JAVABENCH.TPCH_PART_100 where l_partkey = p_partkey and l_shipdate >= '1995-09-01' and l_shipdate < '1995-10-01';"
echo "explain options 'f' cmd;"

echo "values('QUERY15');"
echo "prepare cmd from select s_suppkey, s_name, s_address, s_phone, total_revenue from TRAFODION.JAVABENCH.TPCH_SUPPLIER_100, (select l_suppkey, sum(l_extendedprice*(1-l_discount)) from TRAFODION.JAVABENCH.TPCH_LINEITEM_100 where l_shipdate >= '1996-01-01' and l_shipdate < '1996-04-01' group by l_suppkey) as v(supplier_no, total_revenue) where s_suppkey = supplier_no and cast(total_revenue as numeric(18,2)) = (select cast(max(total_revenue) as numeric(18,2)) from (select l_suppkey, sum(l_extendedprice*(1-l_discount)) from TRAFODION.JAVABENCH.TPCH_LINEITEM_100 where l_shipdate >= '1996-01-01' and l_shipdate < '1996-04-01' group by l_suppkey) as v1(supplier_no, total_revenue)) order by s_suppkey;"
echo "explain options 'f' cmd;"

echo "values('QUERY16');"
echo "prepare cmd from select p_brand, p_type, p_size, count(distinct ps_suppkey) as supplier_cnt from TRAFODION.JAVABENCH.TPCH_PARTSUPP_100, TRAFODION.JAVABENCH.TPCH_PART_100 where p_partkey = ps_partkey and p_brand <> 'Brand#45' and p_type not like 'MEDIUM POLISHED%' and p_size in (49 , 14 , 23 , 45 , 19 , 3 , 36 , 9 ) and ps_suppkey not in (select s_suppkey from TRAFODION.JAVABENCH.TPCH_SUPPLIER_100 where s_comment like '%Customer%Complaints%') group by p_brand, p_type, p_size order by supplier_cnt desc, p_brand, p_type, p_size;"
echo "explain options 'f' cmd;"

echo "values('QUERY17');"
echo "prepare cmd from select sum(l_extendedprice)/7.0 as avg_yearly from TRAFODION.JAVABENCH.TPCH_LINEITEM_100, TRAFODION.JAVABENCH.TPCH_PART_100 where p_partkey = l_partkey and p_brand = 'Brand#23' and p_container = 'MED BOX' and l_quantity < (select 0.2 * avg(l_quantity) from TRAFODION.JAVABENCH.TPCH_LINEITEM_100 where l_partkey = p_partkey);"
echo "explain options 'f' cmd;"

echo "values('QUERY18');"
echo "prepare cmd from select c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity) from TRAFODION.JAVABENCH.TPCH_CUSTOMER_100, TRAFODION.JAVABENCH.TPCH_ORDERS_100, TRAFODION.JAVABENCH.TPCH_LINEITEM_100 where o_orderkey in (select l_orderkey from TRAFODION.JAVABENCH.TPCH_LINEITEM_100 group by l_orderkey having sum(l_quantity) > 300) and c_custkey = o_custkey and o_orderkey = l_orderkey group by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice order by o_totalprice desc, o_orderdate;"
echo "explain options 'f' cmd;"

echo "values('QUERY19');"
echo "prepare cmd from select sum(l_extendedprice*(1-l_discount)) as revenue from TRAFODION.JAVABENCH.TPCH_LINEITEM_100, TRAFODION.JAVABENCH.TPCH_PART_100 where (p_partkey = l_partkey and p_brand = 'Brand#12' and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') and l_quantity >= 300 and l_quantity <= ( 300 + 10 ) and p_size between 1 and 5 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON') or (p_partkey = l_partkey and p_brand = 'Brand#23' and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') and l_quantity >= 300 and l_quantity <= ( 300 + 10 ) and p_size between 1 and 10 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON') or (p_partkey = l_partkey and p_brand = 'Brand#34' and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') and l_quantity >= 300 and l_quantity <= ( 300 + 10 ) and p_size between 1 and 15 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON');"
echo "explain options 'f' cmd;"

echo "values('QUERY20');"
echo "prepare cmd from select s_name, s_address from TRAFODION.JAVABENCH.TPCH_SUPPLIER_100, TRAFODION.JAVABENCH.TPCH_NATION_100 where s_suppkey in (select ps_suppkey from TRAFODION.JAVABENCH.TPCH_PARTSUPP_100 where ps_partkey in (select p_partkey from TRAFODION.JAVABENCH.TPCH_PART_100 where p_name like 'forest%') and ps_availqty > (select 0.5 * sum(l_quantity) from TRAFODION.JAVABENCH.TPCH_LINEITEM_100 where l_partkey = ps_partkey and l_suppkey = ps_suppkey and l_shipdate >= '1994-01-01' and l_shipdate < '1995-10-01')) and s_nationkey = n_nationkey and n_name = 'CANADA' order by s_name;"
echo "explain options 'f' cmd;"

echo "values('QUERY21');"
echo "prepare cmd from select s_name, count(*) as numwait from TRAFODION.JAVABENCH.TPCH_SUPPLIER_100, TRAFODION.JAVABENCH.TPCH_LINEITEM_100 l1, TRAFODION.JAVABENCH.TPCH_ORDERS_100, TRAFODION.JAVABENCH.TPCH_NATION_100 where s_suppkey = l1.l_suppkey and o_orderkey = l1.l_orderkey and o_orderstatus = 'F' and l1.l_receiptdate > l1.l_commitdate and exists (select * from TRAFODION.JAVABENCH.TPCH_LINEITEM_100 l2 where l2.l_orderkey = l1.l_orderkey and l2.l_suppkey <> l1.l_suppkey) and not exists (select * from TRAFODION.JAVABENCH.TPCH_LINEITEM_100 l3 where l3.l_orderkey = l1.l_orderkey and l3.l_suppkey <> l1.l_suppkey and l3.l_receiptdate > l3.l_commitdate) and s_nationkey = n_nationkey and n_name = 'SAUDI ARABIA' group by s_name order by numwait desc, s_name;"
echo "explain options 'f' cmd;"

echo "values('QUERY22');"
echo "prepare cmd from select cntrycode, count(*) as numcust, sum(c_acctbal) as totacctbal from (select substring(c_phone from 1 for 2) as cntrycode, c_acctbal from TRAFODION.JAVABENCH.TPCH_CUSTOMER_100 where substring(c_phone from 1 for 2) in ('31', '10', '24', '20', '16', '29', '22') and c_acctbal > (select avg(c_acctbal) from TRAFODION.JAVABENCH.TPCH_CUSTOMER_100 where c_acctbal > 0.00 and substring(c_phone from 1 for 2) in ('31', '10', '24', '20', '16', '29', '22')) and not exists (select * from TRAFODION.JAVABENCH.TPCH_ORDERS_100 where o_custkey = c_custkey)) as custsale group by cntrycode order by cntrycode;"
echo "explain options 'f' cmd;"

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
TABLENAME=${SCHEMA^^}.TPCH_${TABLE^^}_${SCALE}
curl -sS --noproxy '*' "http://${SYSTEM_NAMEMGR_URL}:${SYSTEM_NAMEMGR_PORT}/table.jsp?name=${TABLENAME}" >> ${LOGDIRECTORY}/${TESTID}.showhbase.${BENCHMARK}.log
done

fi

