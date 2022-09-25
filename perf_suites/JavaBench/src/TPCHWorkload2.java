import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.log4j.Logger;

public class TPCHWorkload2 {

	final int INVALID_SCALEFACTOR = -1;
	final int INVALID_STREAMNUMBER = -1;


	final SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	final SimpleDateFormat dayformat = new SimpleDateFormat("yyyy-MM-dd");

	public int stream_number = INVALID_STREAMNUMBER;
	public int scale_factor = INVALID_SCALEFACTOR;

	public int query_number = 0;

	public String schema = null;
	public String database = null;

	public boolean option_autocommit = true;
	public boolean option_sessionless = false;
	public boolean option_prepareexec = false;
	public boolean option_directexec = true;
	public boolean option_spj = false;
	public boolean option_controlrandom = false;
	public boolean option_default = false;

	RandomGenerator random_generator = new RandomGenerator();

	DatabaseConnector database_connector = new DatabaseConnector();

	String lineitem_table_name = null;
	String part_table_name = null;
	String supplier_table_name = null;
	String partsupp_table_name = null;
	String nation_table_name = null;
	String region_table_name = null;
	String orders_table_name = null;
	String customer_table_name = null;

	final static Logger logger = Logger.getLogger("JavaBench");

	Calendar calendar = null;

	// run time statistics
	public long accumlated_transactions = 0;
	public long accumlated_successes = 0;
	public long accumlated_errors = 0;
	public long accumlated_retries = 0;

	public long accumlated_transaction_response_micro = 0;
	public long maximum_transaction_response_micro = 0;
	public long minimum_transaction_response_micro = 999999999999999999L;
	public long interval_maximum_transaction_response_micro = 0;
	public long interval_minimum_transaction_response_micro = 999999999999999999L;

	public long accumlated_operations = 0;
	public long accumlated_operation_response_micro = 0;
	public long maximum_operation_response_micro = 0;
	public long minimum_operation_response_micro = 999999999999999999L;

	public final int TOTAL_TRANS_TYPES = 22;
	public final String TRANS_TYPE_NAMES[] = {
			"query01","query02","query03","query04","query05","query06","query07","query08","query09","query10",
			"query11","query12","query13","query14","query15","query16","query17","query18","query19","query20",
			"query21","query22"
			};
	public final int QUERY01_TRANS_TYPE = 0;  // Fixed value
	public final int QUERY02_TRANS_TYPE = 1;  // Fixed value
	public final int QUERY03_TRANS_TYPE = 2;  // Fixed value
	public final int QUERY04_TRANS_TYPE = 3;  // Fixed value
	public final int QUERY05_TRANS_TYPE = 4;  // Fixed value
	public final int QUERY06_TRANS_TYPE = 5;  // Fixed value
	public final int QUERY07_TRANS_TYPE = 6;  // Fixed value
	public final int QUERY08_TRANS_TYPE = 7;  // Fixed value
	public final int QUERY09_TRANS_TYPE = 8;  // Fixed value
	public final int QUERY10_TRANS_TYPE = 9;  // Fixed value
	public final int QUERY11_TRANS_TYPE = 10;  // Fixed value
	public final int QUERY12_TRANS_TYPE = 11;  // Fixed value
	public final int QUERY13_TRANS_TYPE = 12;  // Fixed value
	public final int QUERY14_TRANS_TYPE = 13;  // Fixed value
	public final int QUERY15_TRANS_TYPE = 14;  // Fixed value
	public final int QUERY16_TRANS_TYPE = 15;  // Fixed value
	public final int QUERY17_TRANS_TYPE = 16;  // Fixed value
	public final int QUERY18_TRANS_TYPE = 17;  // Fixed value
	public final int QUERY19_TRANS_TYPE = 18;  // Fixed value
	public final int QUERY20_TRANS_TYPE = 19;  // Fixed value
	public final int QUERY21_TRANS_TYPE = 20;  // Fixed value
	public final int QUERY22_TRANS_TYPE = 21;  // Fixed value

	public int[] execution_order = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22 };

	public long[] accumlated_transactions_by_type = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
	public long[] accumlated_errors_by_type = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
	public long[] accumlated_retries_by_type = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
	public long[] accumlated_successes_by_type = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };

	public long[] accumlated_transaction_response_micro_by_type = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
	public long[] maximum_transaction_response_micro_by_type = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
	public long[] minimum_transaction_response_micro_by_type = {
			999999999999999999L, 999999999999999999L, 999999999999999999L, 999999999999999999L, 999999999999999999L,
			999999999999999999L, 999999999999999999L, 999999999999999999L, 999999999999999999L, 999999999999999999L,
			999999999999999999L, 999999999999999999L, 999999999999999999L, 999999999999999999L, 999999999999999999L,
			999999999999999999L, 999999999999999999L, 999999999999999999L, 999999999999999999L, 999999999999999999L,
			999999999999999999L, 999999999999999999L
			};

	long start_transaction_time = 0;
	long transaction_elapsed_micro = 0;
	
	//----------------------------------------------------------------------------------------------------------

	void start_transaction( int trans_type ) {
		accumlated_transactions++;
		accumlated_transactions_by_type[trans_type]++;
	}

	void end_transaction() {
		accumlated_operations = database_connector.accumlated_operations;
		accumlated_operation_response_micro = database_connector.accumlated_operation_response_micro;
		maximum_operation_response_micro = database_connector.maximum_operation_response_micro;
		minimum_operation_response_micro = database_connector.minimum_operation_response_micro;
	}

	void transaction_success ( int trans_type ) {
		accumlated_successes++;
		accumlated_successes_by_type[trans_type]++;
	}

	void transaction_error ( int trans_type ) {
		accumlated_errors++;
		accumlated_errors_by_type[trans_type]++;
	}

	void transaction_retry ( int trans_type ) {
		accumlated_retries++;
		accumlated_retries_by_type[trans_type]++;
	}

	void start_timer() {
		start_transaction_time = System.nanoTime();
	}

	void stop_timer ( int trans_type ) {
		transaction_elapsed_micro = ( System.nanoTime() - start_transaction_time ) / 1000;

		accumlated_transaction_response_micro = accumlated_transaction_response_micro + transaction_elapsed_micro;
		accumlated_transaction_response_micro_by_type[trans_type] = accumlated_transaction_response_micro_by_type[trans_type] + transaction_elapsed_micro;

		if (transaction_elapsed_micro < minimum_transaction_response_micro ) {
			minimum_transaction_response_micro = transaction_elapsed_micro;
		}
		if (transaction_elapsed_micro < interval_minimum_transaction_response_micro ) {
			interval_minimum_transaction_response_micro = transaction_elapsed_micro;
		}
		if (transaction_elapsed_micro < minimum_transaction_response_micro_by_type[trans_type] ) {
			minimum_transaction_response_micro_by_type[trans_type] = transaction_elapsed_micro;
		}
		
		if (transaction_elapsed_micro > maximum_transaction_response_micro ) {
			maximum_transaction_response_micro = transaction_elapsed_micro;
		}
		if (transaction_elapsed_micro > interval_maximum_transaction_response_micro ) {
			interval_maximum_transaction_response_micro = transaction_elapsed_micro;
		}
		if (transaction_elapsed_micro > maximum_transaction_response_micro_by_type[trans_type] ) {
			maximum_transaction_response_micro_by_type[trans_type] = transaction_elapsed_micro;
		}
	}

	void resetInterval (long check_max, long check_min ) {
		if ( check_max >= interval_maximum_transaction_response_micro ) {
			interval_maximum_transaction_response_micro = 0;
		}
		if ( check_min <= interval_minimum_transaction_response_micro ) {
			interval_minimum_transaction_response_micro = 999999999999999999L;
		}
	}

	//----------------------------------------------------------------------------------------------------------

	public void runQuery() throws Exception {

		int trans_type = 0;
		String query_statement = null;
		int max_records = 99999999;
		
		int delta = 0;
		int p_size = 0;
		String p_type = null;
		String r_name = null;
		String mktsegment = null;
		String start_date = null;
		String stop_date = null;
		Double discount = 0.0;
		int quantity = 0;
		String n_name_1 = null;
		String n_name_2 = null;
		String color = null;
		Double fraction = 0.0;
		String shipmode1 = null;
		String shipmode2 = null;
		String word1 = null;
		String word2 = null;
		String brand = null;
		int size1 = 0;
		int size2 = 0;
		int size3 = 0;
		int size4 = 0;
		int size5 = 0;
		int size6 = 0;
		int size7 = 0;
		int size8 = 0;
		String container = null;
		int quantity1 = 0;
		int quantity2 = 0;
		int quantity3 = 0;
		String brand1 = null;
		String brand2 = null;
		String brand3 = null;
		int code1 = 0;
		int code2 = 0;
		int code3 = 0;
		int code4 = 0;
		int code5 = 0;
		int code6 = 0;
		int code7 = 0;
		
		int query_num = execution_order[query_number % TOTAL_TRANS_TYPES];

		query_number++;
		
		switch ( query_num ) {
		case 1 :
			trans_type = QUERY01_TRANS_TYPE;
			// Generate Test Variables.
			if ( option_default ) {
				delta = 90;
			} else {
				delta = random_generator.generateRandomInteger( 60, 120);
			}
			calendar.set(1998, 12, 01);
			calendar.add( Calendar.DATE, -delta );
			String shipdate = dayformat.format(calendar.getTime());
			if (logger.isDebugEnabled()) logger.debug( stream_number + "> dalta = " + delta + "; generated date = " + shipdate );
			// Build Query Statement
			query_statement =
					"select"
						+ " l_returnflag,"
						+ " l_linestatus,"
						+ " cast(sum(l_quantity) as numeric(18,2)) as sum_qty,"
						+ " cast(sum(l_extendedprice) as numeric(18,2)) as sum_base_price,"
						+ " cast(sum(l_extendedprice * (1 - l_discount)) as numeric(18,2)) as sum_disc_price,"
						+ " cast(sum(l_extendedprice * (1 - l_discount)*(1+l_tax)) as numeric(18,2)) as sum_charge,"
						+ " cast(avg(l_quantity) as numeric(18,3)) as avg_qty,"
						+ " cast(avg(l_extendedprice) as numeric(18,3)) as avg_price,"
						+ " cast(avg(l_discount) as numeric(18,3)) as avg_disc,"
						+ " count(*) as count_order"
					+ " from "
						+ " " + lineitem_table_name
					+ " where"
						+ " l_shipdate <= '" + shipdate + "'"
					+ " group by"
						+ " l_returnflag,"
						+ " l_linestatus"
					+ " order by"
						+ " l_returnflag,"
						+ " l_linestatus";
			break;
		case 2 :
			trans_type = QUERY02_TRANS_TYPE;
			max_records = 100;
			// Generate Test Variables.
			if ( option_default ) {
				p_size = 15;
				p_type = "BRASS";
				r_name = "EUROPE";
			} else {
				p_size = random_generator.generateRandomInteger( 1, 50);
				p_type = random_generator.generatePartType3();
				r_name = random_generator.generateRegionName();
			}
			if (logger.isDebugEnabled()) logger.debug( stream_number + "> p_size = " + p_size + "; p_type = " + p_type + "; r_name = " + r_name );
			// Build Query Statement
			query_statement =
				"select"
					+ " s_acctbal,"
					+ " s_name,"
					+ " n_name,"
					+ " p_partkey,"
					+ " p_mfgr,"
					+ " s_address,"
					+ " s_phone,"
					+ " s_comment"
				+ " from "
					+ " " + part_table_name
					+ ", " + supplier_table_name
					+ ", " + partsupp_table_name
					+ ", " + nation_table_name
					+ ", " + region_table_name
				+ " where"
					+ " p_partkey = ps_partkey"
					+ " and s_suppkey = ps_suppkey"
					+ " and p_size = " + p_size
					+ " and p_type like '%" + p_type + "'"
					+ " and s_nationkey = n_nationkey"
					+ " and n_regionkey = r_regionkey"
					+ " and r_name = '" + r_name + "'"
					+ " and ps_supplycost = ("
							 + " select"
								 + " min(ps_supplycost)"
							 + " from"
								+ " " + partsupp_table_name
								+ ", " + supplier_table_name
								+ ", " + nation_table_name
								+ ", " + region_table_name
							 + " where"
								 + " p_partkey = ps_partkey"
								 + " and s_suppkey = ps_suppkey"
								 + " and s_nationkey = n_nationkey"
								 + " and n_regionkey = r_regionkey"
								 + " and r_name = '" + r_name + "'"
						 + " )"
				+ " order by"
					+ " s_acctbal desc,"
					+ " n_name,"
					+ " s_name,"
					+ " p_partkey"
					;
			break;
		case 3 :
			trans_type = QUERY03_TRANS_TYPE;
			max_records = 10;
			// Generate Test Variables.
			if ( option_default ) {
				mktsegment = "BUILDING";
				start_date = "1995-03-15";
			} else {
				mktsegment = random_generator.generateMktSegment();
				calendar.set(1995, 03, 01);
				calendar.add( Calendar.DATE, random_generator.generateRandomInteger(0, 30) );
				start_date = dayformat.format(calendar.getTime());
			}
			if (logger.isDebugEnabled()) logger.debug( stream_number + "> segment = " + mktsegment + "; date = " + start_date );
			// Build Query Statement
			query_statement =
				"select"
						+ " l_orderkey,"
						+ " sum(l_extendedprice*(1-l_discount)) as revenue,"
						+ " o_orderdate,"
						+ " o_shippriority"
					+ " from"
						+ " " + customer_table_name
						+ ", " + orders_table_name
						+ ", " + lineitem_table_name
					+ " where"
						+ " c_mktsegment = '" + mktsegment + "'"
						+ " and c_custkey = o_custkey"
						+ " and l_orderkey = o_orderkey"
						+ " and o_orderdate < '" + start_date + "'"
						+ " and l_shipdate > '" + start_date + "'"
					+ " group by"
						+ " l_orderkey,"
						+ " o_orderdate,"
						+ " o_shippriority"
					+ " order by"
						+ " revenue desc,"
						+ " o_orderdate"
					;
			break;
		case 4 :
			trans_type = QUERY04_TRANS_TYPE;
			// Generate Test Variables.
			if ( option_default ) {
				start_date = "1993-07-01";
				stop_date = "1993-10-01";
			} else {
				calendar.set(1992, 12, 01);
				calendar.add( Calendar.MONTH, random_generator.generateRandomInteger(1, 58) );
				start_date = dayformat.format(calendar.getTime());
				calendar.add( Calendar.MONTH, 3 );
				stop_date = dayformat.format(calendar.getTime());
			}
			if (logger.isDebugEnabled()) logger.debug( stream_number + "> start_date = " + start_date + "; stop_date = " + stop_date );
			// Build Query Statement
			query_statement =
				"select"
						+ " o_orderpriority,"
						+ " count(*) as order_count"
					+ " from"
						+ " " + orders_table_name
					+ " where "
						+ " o_orderdate >= '" + start_date + "'"
						+ " and o_orderdate < '" + stop_date + "'"
						+ " and exists ( "
							+ " select "
								+ " * "
							+ " from "
								+ " " + lineitem_table_name
							+ " where "
								+ " l_orderkey = o_orderkey "
								+ " and l_commitdate < l_receiptdate "
						+ " ) "
					+ " group by "
						+ " o_orderpriority "
					+ " order by "
						+ " o_orderpriority"
				;
			break;
		case 5 :
			trans_type = QUERY05_TRANS_TYPE;
			// Generate Test Variables.
			if ( option_default ) {
				r_name = "EUROPE";
				start_date = "1994-01-01";
				stop_date = "1995-10-01";
			} else {
				r_name = random_generator.generateRegionName();
				calendar.set(1992, 01, 01);
				calendar.add( Calendar.YEAR, random_generator.generateRandomInteger(1, 5) );
				start_date = dayformat.format(calendar.getTime());
				calendar.add( Calendar.YEAR, 1 );
				stop_date = dayformat.format(calendar.getTime());
			}
			if (logger.isDebugEnabled()) logger.debug( stream_number + "> r_name = " + r_name + "; start_date = " + start_date 
					+ "; stop_date = " + stop_date );
			// Build Query Statement
			query_statement =
				"select"
						+ " n_name,"
						+ " sum(l_extendedprice*(1-l_discount)) as revenue"
					+ " from"
						+ " " + customer_table_name
						+ ", " + orders_table_name
						+ ", " + lineitem_table_name
						+ ", " + supplier_table_name
						+ ", " + nation_table_name
						+ ", " + region_table_name
					+ " where"
						+ " c_custkey = o_custkey"
						+ " and l_orderkey = o_orderkey"
						+ " and l_suppkey = s_suppkey"
						+ " and c_nationkey = s_nationkey"
						+ " and s_nationkey = n_nationkey"
						+ " and n_regionkey = r_regionkey"
						+ " and r_name = '" + r_name + "'"
						+ " and o_orderdate >= '" + start_date + "'"
						+ " and o_orderdate < '" + stop_date + "'"
					+ " group by"
						+ " n_name"
					+ " order by"
						+ " revenue desc"
				;
			break;
		case 6 :
			trans_type = QUERY06_TRANS_TYPE;
			// Generate Test Variables.
			if ( option_default ) {
				start_date = "1994-01-01";
				stop_date = "1995-10-01";
				discount = 0.06;
				quantity = 24;
			} else {
				calendar.set(1992, 01, 01);
				calendar.add( Calendar.YEAR, random_generator.generateRandomInteger(1, 5) );
				start_date = dayformat.format(calendar.getTime());
				calendar.add( Calendar.YEAR, 1 );
				stop_date = dayformat.format(calendar.getTime());
				discount = ( random_generator.generateRandomInteger(2, 9)/100.0);
				quantity = random_generator.generateRandomInteger(24,25);
			}
			if (logger.isDebugEnabled()) logger.debug( stream_number + "> start_date = " + start_date + "; stop_date = " + stop_date 
					+ "; discount = " + discount + "; quantity = " + quantity);
			// Build Query Statement
			query_statement =
				"select"
						+ " sum(l_extendedprice*l_discount) as revenue"
					+ " from"
						+ " " + lineitem_table_name
					+ " where"
						+ " l_shipdate >= '" + start_date + "'"
						+ " and l_shipdate < '" + stop_date + "'"
						+ " and l_discount between " + (discount - 0.01 ) + " and " + (discount + 0.01 )
						+ " and l_quantity < " + quantity
				;
			break;
		case 7 :
			trans_type = QUERY07_TRANS_TYPE;
			// Generate Test Variables.
			if ( option_default ) {
				n_name_1 = "FRANCE";
				n_name_2 = "GERMANY";
			} else {
				n_name_1 = random_generator.generateNationName();
				do {
					n_name_2 = random_generator.generateNationName();
				} while ( n_name_1.equals(n_name_2) );
			}
			if (logger.isDebugEnabled()) logger.debug( stream_number + "> n_name_1 = " + n_name_1 + "; n_name_2 = " + n_name_2 );
			// Build Query Statement
			query_statement =
				"select"
						+ " supp_nation,"
						+ " cust_nation,"
						+ " l_year,"
						+ " sum(volume) as revenue"
					+ " from"
						+ " ("
							+ " select"
								+ " n1.n_name as supp_nation,"
								+ " n2.n_name as cust_nation,"
								+ " left(l_shipdate,4) as l_year,"
								+ " l_extendedprice * (1 - l_discount) as volume"
							+ " from"
								+ " " + supplier_table_name
								+ ", " + lineitem_table_name
								+ ", " + orders_table_name
								+ ", " + customer_table_name
								+ ", " + nation_table_name + " n1"
								+ ", " + nation_table_name + " n2"
							+ " where"
								+ " s_suppkey = l_suppkey"
								+ " and o_orderkey = l_orderkey"
								+ " and c_custkey = o_custkey"
								+ " and s_nationkey = n1.n_nationkey"
								+ " and c_nationkey = n2.n_nationkey"
								+ " and ("
									+ " (n1.n_name = '" + n_name_1 + "' and n2.n_name = '" + n_name_2 + "')"
									+ " or (n1.n_name = '" + n_name_2 + "' and n2.n_name = '" + n_name_1 + "')"
								+ " )"
								+ " and l_shipdate between '1995-01-01' and '1996-12-31'"
						+ " ) as shipping"
					+ " group by"
						+ " supp_nation,"
						+ " cust_nation,"
						+ " l_year"
					+ " order by"
						+ " supp_nation,"
						+ " cust_nation,"
						+ " l_year"
				;
			break;
		case 8 :
			trans_type = QUERY08_TRANS_TYPE;
			// Generate Test Variables.
			if ( option_default ) {
				n_name_1 = "BRAZIL";
				r_name = "AMERICA";
				p_type = "ECONOMY ANODIZED STEEL";
			} else {
				n_name_1 = random_generator.generateNationName();
				r_name = random_generator.generateRegionName();
				p_type = random_generator.generatePartType();
			}
			if (logger.isDebugEnabled()) logger.debug( stream_number + "> n_name = " + n_name_1 + "; r_name = " + r_name + "; p_type = " + p_type );
			// Build Query Statement
			query_statement =
					"select"
							+ " o_year,"
							+ " sum(case"
								+ " when nation = '" + n_name_1 + "'"
								+ " then volume"
								+ " else 0"
							+ " end) / sum(volume) as mkt_share"
						+ " from ("
							+ " select"
								+ " left(l_shipdate,4) as o_year,"
								+ " l_extendedprice * (1 - l_discount) as volume,"
								+ " n2.n_name as nation"
							+ " from"
								+ " " + part_table_name
								+ ", " + supplier_table_name
								+ ", " + lineitem_table_name
								+ ", " + orders_table_name
								+ ", " + customer_table_name
								+ ", " + nation_table_name + " n1"
								+ ", " + nation_table_name + " n2"
								+ ", " + region_table_name
							+ " where"
								+ " p_partkey = l_partkey"
								+ " and s_suppkey = l_suppkey"
								+ " and l_orderkey = o_orderkey"
								+ " and o_custkey = c_custkey"
								+ " and c_nationkey = n1.n_nationkey"
								+ " and n1.n_regionkey = r_regionkey"
								+ " and r_name = '" + r_name + "'"
								+ " and s_nationkey = n2.n_nationkey"
								+ " and o_orderdate between '1995-01-01' and '1996-12-31'"
								+ " and p_type = '" + p_type + "'"
							+ " ) as all_nations"
						+ " group by"
							+ " o_year"
						+ " order by"
							+ " o_year"
					;
			break;
		case 9 :
			trans_type = QUERY09_TRANS_TYPE;
			// Generate Test Variables.
			if ( option_default ) {
				color = "green";
			} else {
				color = random_generator.generateColor();
			}
			if (logger.isDebugEnabled()) logger.debug( stream_number + "> color = " + color );
			// Build Query Statement
			query_statement =
					"select"
							+ " nation,"
							+ " o_year,"
							+ " sum(amount) as sum_profit"
						+ " from"
							+ " ("
							+ " select"
								+ " n_name as nation,"
								+ " left(l_shipdate,4) as o_year,"
								+ " l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount"
							+ " from"
								+ " " + part_table_name
								+ ", " + supplier_table_name
								+ ", " + lineitem_table_name
								+ ", " + partsupp_table_name
								+ ", " + orders_table_name
								+ ", " + nation_table_name
							+ " where"
								+ " s_suppkey = l_suppkey"
								+ " and ps_suppkey = l_suppkey"
								+ " and ps_partkey = l_partkey"
								+ " and p_partkey = l_partkey"
								+ " and o_orderkey = l_orderkey"
								+ " and s_nationkey = n_nationkey"
								+ " and p_name like '%" + color + "%'"
							+ " ) as profit"
						+ " group by"
							+ " nation,"
							+ " o_year"
						+ " order by"
							+ " nation,"
							+ " o_year desc"
					;
			break;
		case 10 :
			trans_type = QUERY10_TRANS_TYPE;
			max_records = 20;
			// Generate Test Variables.
			if ( option_default ) {
				start_date = "1993-10-01";
				stop_date = "1994-01-01";
			} else {
				calendar.set(1993, 01, 01);
				calendar.add( Calendar.MONTH, random_generator.generateRandomInteger(1, 24) );
				start_date = dayformat.format(calendar.getTime());
				calendar.add( Calendar.MONTH, 3 );
				stop_date = dayformat.format(calendar.getTime());
			}
			if (logger.isDebugEnabled()) logger.debug( stream_number + "> start_date = " + start_date + "; stop_date = " + stop_date );
			// Build Query Statement
			query_statement =
					"select"
							+ " c_custkey,"
							+ " c_name,"
							+ " sum(l_extendedprice * (1 - l_discount)) as revenue,"
							+ " c_acctbal,"
							+ " n_name,"
							+ " c_address,"
							+ " c_phone,"
							+ " c_comment"
						+ " from"
							+ " " + customer_table_name
							+ ", " + orders_table_name
							+ ", " +  lineitem_table_name
							+ ", " +  nation_table_name
						+ " where"
							+ " c_custkey = o_custkey"
							+ " and l_orderkey = o_orderkey"
							+ " and o_orderdate >= '" + start_date + "'"
							+ " and o_orderdate < '" + stop_date + "'"
							+ " and l_returnflag = 'R'"
							+ " and c_nationkey = n_nationkey"
						+ " group by"
							+ " c_custkey,"
							+ " c_name,"
							+ " c_acctbal,"
							+ " c_phone,"
							+ " n_name,"
							+ " c_address,"
							+ " c_comment"
						+ " order by"
							+ " revenue desc"
					;
			break;
		case 11 :
			trans_type = QUERY11_TRANS_TYPE;
			// Generate Test Variables.
			if ( option_default ) {
				n_name_1 = "GERMANY";
				fraction = 0.0001;
			} else {
				n_name_1 = random_generator.generateNationName();
				fraction = 0.0001 / scale_factor ;
			}
			if (logger.isDebugEnabled()) logger.debug( stream_number + "> n_name = " + n_name_1 + "; fraction = " + fraction  );
			// Build Query Statement
			query_statement =
					"select"
							+ " ps_partkey,"
							+ " sum(ps_supplycost * ps_availqty) as valuea"
						+ " from"
							+ " " +  partsupp_table_name
							+ ", " + supplier_table_name
							+ ", " +  nation_table_name
						+ " where"
							+ " ps_suppkey = s_suppkey"
							+ " and s_nationkey = n_nationkey"
							+ " and n_name = '" + n_name_1 + "'"
						+ " group by"
							+ " ps_partkey having"
								+ " sum(ps_supplycost * ps_availqty) > ("
									+ " select"
										+ " sum(ps_supplycost * ps_availqty) * " + fraction 
									+ " from"
										+ " " +  partsupp_table_name
										+ ", " + supplier_table_name
										+ ", " +  nation_table_name
									+ " where"
										+ " ps_suppkey = s_suppkey"
										+ " and s_nationkey = n_nationkey"
										+ " and n_name = '" + n_name_1 + "'"
								+ " )"
						+ " order by"
							+ " valuea desc"
					;
			break;
		case 12 :
			trans_type = QUERY12_TRANS_TYPE;
			// Generate Test Variables.
			if ( option_default ) {
				shipmode1 = "MAIL";
				shipmode2 = "SHIP";
				start_date = "1994-01-01";
				stop_date = "1995-10-01";
			} else {
				shipmode1 = random_generator.generateShipMode();
				do {
					shipmode2 = random_generator.generateShipMode();
				} while ( shipmode1.equals(shipmode2) );
				calendar.set(1992, 01, 01);
				calendar.add( Calendar.YEAR, random_generator.generateRandomInteger(1, 5) );
				start_date = dayformat.format(calendar.getTime());
				calendar.add( Calendar.YEAR, 1 );
				stop_date = dayformat.format(calendar.getTime());
			}
			if (logger.isDebugEnabled()) logger.debug( stream_number + "> shipmode1 = " + shipmode1 + "; shipmode2 = " + shipmode2 
					+ "; start_date = " + start_date + "; stop_date = " + stop_date);
			// Build Query Statement
			query_statement =
					"select"
							+ " l_shipmode,"
							+ " sum(case"
								+ " when o_orderpriority = '1-URGENT'"
									+ " or o_orderpriority = '2-HIGH'"
								+ " then 1"
								+ " else 0"
							+ " end) as high_line_count,"
							+ " sum(case"
								+ " when o_orderpriority <> '1-URGENT'"
									+ " and o_orderpriority <> '2-HIGH'"
								+ " then 1"
								+ " else 0"
							+ " end) as low_line_count"
						+ " from"
							+ " " + orders_table_name
							+ ", " + lineitem_table_name
						+ " where"
							+ " o_orderkey = l_orderkey"
							+ " and l_shipmode in ('" + shipmode1 + "', '" + shipmode2 + "')"
							+ " and l_commitdate < l_receiptdate"
							+ " and l_shipdate < l_commitdate"
							+ " and l_receiptdate >= '" + start_date + "'"
							+ " and l_receiptdate < '" + stop_date + "'"
						+ " group by"
							+ " l_shipmode"
						+ " order by"
							+ " l_shipmode"
					;
			break;
		case 13 :
			trans_type = QUERY13_TRANS_TYPE;
			// Generate Test Variables.
			if ( option_default ) {
				word1 = "special";
				word2 = "requests";
			} else {
				word1 = random_generator.generateWord1();
				word2 = random_generator.generateWord2();
			}
			if (logger.isDebugEnabled()) logger.debug( stream_number + "> word1 = " + word1 + "; word2 = " + word2 );
			// Build Query Statement
			query_statement =
					"select"
							+ " c_count,"
							+ " count(*) as custdist"
						+ " from ("
							+ " select"
								+ " c_custkey,"
								+ " count(o_orderkey)"
							+ " from"
								+ " " + customer_table_name + " left outer join " + orders_table_name + " on"
									+ " c_custkey = o_custkey"
									+ " and o_comment not like '%" + word1 + "%" + word2 + "%'"
							+ " group by"
								+ " c_custkey"
							+ " ) as c_orders (c_custkey, c_count)"
						+ " group by"
							+ " c_count"
						+ " order by"
							+ " custdist desc,"
							+ " c_count desc"
					;
			break;
		case 14 :
			trans_type = QUERY14_TRANS_TYPE;
			// Generate Test Variables.
			if ( option_default ) {
				start_date = "1995-09-01";
				stop_date = "1995-10-01";
			} else {
				calendar.set(1992, 12, 01);
				calendar.add( Calendar.MONTH, random_generator.generateRandomInteger(1, 60) );
				start_date = dayformat.format(calendar.getTime());
				calendar.add( Calendar.MONTH, 1 );
				stop_date = dayformat.format(calendar.getTime());
			}
			if (logger.isDebugEnabled()) logger.debug( stream_number + "> start_date = " + start_date + "; stop_date = " + stop_date );
			// Build Query Statement
			query_statement =
				"select"
						+ " 100.00 * sum(case"
						+ " when p_type like 'PROMO%'"
							+ " then l_extendedprice*(1-l_discount)"
							+ " else 0"
							+ " end) / sum(l_extendedprice*(1-l_discount)) as promo_revenue"
					+ " from"
						+ " " + lineitem_table_name 
						+ ", " + part_table_name
					+ " where"
						+ " l_partkey = p_partkey"
						+ " and l_shipdate >= '" + start_date + "'"
						+ " and l_shipdate < '" + start_date + "'"
				;
			break;
		case 15 :
			trans_type = QUERY15_TRANS_TYPE;
			// Generate Test Variables.
			if ( option_default ) {
				start_date = "1996-01-01";
				stop_date = "1996-04-01";
			} else {
				calendar.set(1992, 12, 01);
				calendar.add( Calendar.MONTH, random_generator.generateRandomInteger(1, 58) );
				start_date = dayformat.format(calendar.getTime());
				calendar.add( Calendar.MONTH, 3 );
				stop_date = dayformat.format(calendar.getTime());
			}
			if (logger.isDebugEnabled()) logger.debug( stream_number + "> start_date = " + start_date + "; stop_date = " + stop_date );
			// Build Query Statement
			query_statement =
					"select"
							+ " s_suppkey,"
							+ " s_name,"
							+ " s_address,"
							+ " s_phone,"
							+ " total_revenue"
						+ " from"
							+ " " + supplier_table_name
							+ ", ("
								+ " select"
									+ " l_suppkey,"
									+ " sum(l_extendedprice*(1-l_discount))"
								+ " from"
									+ " " + lineitem_table_name
								+ " where"
									+ " l_shipdate >= '" + start_date + "'"
									+ " and l_shipdate < '" + stop_date + "'"
								+ " group by"
									+ " l_suppkey"
							+ " ) as v(supplier_no, total_revenue)"
						+ " where"
							+ " s_suppkey = supplier_no"
							+ " and cast(total_revenue  as numeric(18,2)) = ("
								+ " select"
									+ " cast(max(total_revenue) as numeric(18,2))"
								+ " from"
									+ " ("
										+ " select"
											+ " l_suppkey,"
											+ " sum(l_extendedprice*(1-l_discount))"
										+ " from"
											+ " " + lineitem_table_name
										+ " where"
											+ " l_shipdate >= '" + start_date + "'"
											+ " and l_shipdate < '" + stop_date + "'"
										+ " group by"
											+ " l_suppkey"
									+ " ) as v1(supplier_no, total_revenue)"
								+ " )"
						+ " order by"
							+ " s_suppkey"
					;
			break;
		case 16 :
			trans_type = QUERY16_TRANS_TYPE;
			// Generate Test Variables.
			if ( option_default ) {
				brand = "Brand#45";
				p_type = "MEDIUM POLISHED";
				size1 = 49;
				size2 = 14;
				size3 = 23;
				size4 = 45;
				size5 = 19;
				size6 = 3;
				size7 = 36;
				size8 = 9;
			} else {
				brand = random_generator.generateBrand();
				p_type = random_generator.generatePartType2();
				random_generator.init_random_permutation(50);
				size1 = random_generator.next_random_permutation();
				size2 = random_generator.next_random_permutation();
				size3 = random_generator.next_random_permutation();
				size4 = random_generator.next_random_permutation();
				size5 = random_generator.next_random_permutation();
				size6 = random_generator.next_random_permutation();
				size7 = random_generator.next_random_permutation();
				size8 = random_generator.next_random_permutation();
			}
			if (logger.isDebugEnabled()) logger.debug( stream_number + "> brand = " + brand + "; p_type = " + p_type 
					+ "; size1 = " + size1 + "; size2 = " + size2 + "; size3 = " + size3 + "; size4 = " + size4
					+ "; size5 = " + size5 + "; size6 = " + size6 + "; size7 = " + size7 + "; size8 = " + size8 );
			// Build Query Statement
			query_statement =
					"select"
							+ " p_brand,"
							+ " p_type,"
							+ " p_size,"
							+ " count(distinct ps_suppkey) as supplier_cnt"
						+ " from"
							+ " " + partsupp_table_name
							+ ", " + part_table_name
						+ " where"
							+ " p_partkey = ps_partkey"
							+ " and p_brand <> '"+ brand + "'"
							+ " and p_type not like '"+ p_type + "%'"
							+ " and p_size in (" + size1 + ", " + size2 + ", " + size3 + ", " + size4 
												+ ", " + size5 + ", " + size6 + ", " + size7 + ", " + size8 + ")"
							+ " and ps_suppkey not in ("
								+ " select"
									+ " s_suppkey"
								+ " from"
									+ " " +  supplier_table_name
								+ " where"
									+ " s_comment like '%Customer%Complaints%'"
							+ " )"
						+ " group by"
							+ " p_brand,"
							+ " p_type,"
							+ " p_size"
						+ " order by"
							+ " supplier_cnt desc,"
							+ " p_brand,"
							+ " p_type,"
							+ " p_size"
					;
			break;
		case 17 :
			trans_type = QUERY17_TRANS_TYPE;
			// Generate Test Variables.
			if ( option_default ) {
				brand = "Brand#23";
				container = "MED BOX";
			} else {
				brand = random_generator.generateBrand();
				container = random_generator.generateContainer();
			}
			if (logger.isDebugEnabled()) logger.debug( stream_number + "> brand = " + brand + "; container = " + container );
			// Build Query Statement
			query_statement =
				"select"
						+ " sum(l_extendedprice)/7.0 as avg_yearly"
					+ " from"
						+ " " +  lineitem_table_name
						+ ", " +  part_table_name
					+ " where"
						+ " p_partkey = l_partkey"
						+ " and p_brand = '"+ brand + "'"
						+ " and p_container = '"+ container + "'"
						+ " and l_quantity < ("
							+ " select"
								+ " 0.2 * avg(l_quantity)"
							+ " from"
								+ " " + lineitem_table_name
							+ " where"
								+ " l_partkey = p_partkey"
						+ " )"
				;
			break;
		case 18 :
			trans_type = QUERY18_TRANS_TYPE;
			max_records = 100;
			// Generate Test Variables.
			if ( option_default ) {
				quantity = 300;
			} else {
				quantity = random_generator.generateRandomInteger(312,315);
			}
			if (logger.isDebugEnabled()) logger.debug( stream_number + "> quantity = " + quantity );
			// Build Query Statement
			query_statement =
					"select"
							+ " c_name,"
							+ " c_custkey,"
							+ " o_orderkey,"
							+ " o_orderdate,"
							+ " o_totalprice,"
							+ " sum(l_quantity)"
						+ " from"
							+ " " + customer_table_name
							+ ", " + orders_table_name
							+ ", " + lineitem_table_name
						+ " where"
							+ " o_orderkey in ("
								+ " select"
									+ " l_orderkey"
								+ " from"
									+ " " + lineitem_table_name
								+ " group by"
									+ " l_orderkey having"
										+ " sum(l_quantity) > " + quantity
							+ " )"
							+ " and c_custkey = o_custkey"
							+ " and o_orderkey = l_orderkey"
						+ " group by"
							+ " c_name,"
							+ " c_custkey,"
							+ " o_orderkey,"
							+ " o_orderdate,"
							+ " o_totalprice"
						+ " order by"
							+ " o_totalprice desc,"
							+ " o_orderdate"
					;
			break;
		case 19 :
			trans_type = QUERY19_TRANS_TYPE;
			// Generate Test Variables.
			if ( option_default ) {
				quantity1 = 300;
				quantity2 = 300;
				quantity3 = 300;
				brand1 = "Brand#12";
				brand1 = "Brand#23";
				brand1 = "Brand#34";
			} else {
				quantity1 = random_generator.generateRandomInteger(1,10);
				quantity2 = random_generator.generateRandomInteger(10,20);
				quantity3 = random_generator.generateRandomInteger(20,30);
				brand1 = random_generator.generateBrand();
				brand2 = random_generator.generateBrand();
				brand3 = random_generator.generateBrand();
			}
			if (logger.isDebugEnabled()) logger.debug( stream_number + "> quantity1 = " + quantity1  + "; quantity2 = " + quantity2  + "; quantity3 = " + quantity3
					+ "; brand1 = " + brand1 + "; brand2 = " + brand2 + "; brand3 = " + brand3);
			// Build Query Statement
			query_statement =
					"select"
							+ " sum(l_extendedprice*(1-l_discount)) as revenue"
						+ " from"
							+ " " + lineitem_table_name
							+ ", " + part_table_name
						+ " where"
							+ " ("
								+ " p_partkey = l_partkey"
								+ " and p_brand = '" + brand1 + "'"
								+ " and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')"
								+ " and l_quantity >= " + quantity1 + " and l_quantity <= " + ( quantity1 + 10 )
								+ " and p_size between 1 and 5"
								+ " and l_shipmode in ('AIR', 'AIR REG')"
								+ " and l_shipinstruct = 'DELIVER IN PERSON'"
							+ " )"
							+ " or"
							+ " ("
								+ " p_partkey = l_partkey"
								+ " and p_brand = '" + brand2 + "'"
								+ " and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')"
								+ " and l_quantity >= " + quantity2 + " and l_quantity <= " + ( quantity2 + 10 )
								+ " and p_size between 1 and 10"
								+ " and l_shipmode in ('AIR', 'AIR REG')"
								+ " and l_shipinstruct = 'DELIVER IN PERSON'"
							+ " )"
							+ " or"
							+ " ("
								+ " p_partkey = l_partkey"
								+ " and p_brand = '" + brand3 + "'"
								+ " and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')"
								+ " and l_quantity >= " + quantity3 + " and l_quantity <= " + ( quantity3 + 10 )
								+ " and p_size between 1 and 15"
								+ " and l_shipmode in ('AIR', 'AIR REG')"
								+ " and l_shipinstruct = 'DELIVER IN PERSON'"
							+ " )"
					;
			break;
		case 20 :
			trans_type = QUERY20_TRANS_TYPE;
			// Generate Test Variables.
			if ( option_default ) {
				color = "forest";
				start_date = "1994-01-01";
				stop_date = "1995-10-01";
				n_name_1 = "CANADA";
			} else {
				color = random_generator.generateColor();
				calendar.set(1992, 01, 01);
				calendar.add( Calendar.YEAR, random_generator.generateRandomInteger(1, 5) );
				start_date = dayformat.format(calendar.getTime());
				calendar.add( Calendar.YEAR, 1 );
				stop_date = dayformat.format(calendar.getTime());
				n_name_1 = random_generator.generateNationName();
			}
			if (logger.isDebugEnabled()) logger.debug( stream_number + "> color = " + color + "; start_date = " + start_date  
					+ "; stop_date = " + stop_date + "; n_name = " + n_name_1 );
			// Build Query Statement
			query_statement =
					"select"
							+ " s_name,"
							+ " s_address"
						+ " from"
							+ " " + supplier_table_name
							+ ", " + nation_table_name
						+ " where"
							+ " s_suppkey in ("
								+ " select"
									+ " ps_suppkey"
								+ " from"
									+ " " +  partsupp_table_name
								+ " where"
									+ " ps_partkey in ("
										+ " select"
											+ " p_partkey"
										+ " from"
											+ " " +  part_table_name
										+ " where"
											+ " p_name like '" + color + "%'"
									+ " )"
									+ " and ps_availqty > ("
										+ " select"
											+ " 0.5 * sum(l_quantity)"
										+ " from"
											+ " " + lineitem_table_name
										+ " where"
											+ " l_partkey = ps_partkey"
											+ " and l_suppkey = ps_suppkey"
											+ " and l_shipdate >= '" + start_date + "'"
											+ " and l_shipdate < '" + stop_date + "'"
									+ " )"
							+ " )"
							+ " and s_nationkey = n_nationkey"
							+ " and n_name = '" + n_name_1 + "'"
						+ " order by"
							+ " s_name"
					;
			break;
		case 21 :
			trans_type = QUERY21_TRANS_TYPE;
			max_records = 100;
			// Generate Test Variables.
			if ( option_default ) {
				n_name_1 = "SAUDI ARABIA";
			} else {
				n_name_1 = random_generator.generateNationName();
			}
			if (logger.isDebugEnabled()) logger.debug( stream_number + "> color = " + color + "; start_date = " + start_date  
					+ "; stop_date = " + stop_date + "; n_name = " + n_name_1 );
			// Build Query Statement
			query_statement =
					"select"
							+ " s_name,"
							+ " count(*) as numwait"
						+ " from"
							+ " " + supplier_table_name
							+ ", " + lineitem_table_name + " l1"
							+ ", " + orders_table_name
							+ ", " + nation_table_name
						+ " where"
							+ " s_suppkey = l1.l_suppkey"
							+ " and o_orderkey = l1.l_orderkey"
							+ " and o_orderstatus = 'F'"
							+ " and l1.l_receiptdate > l1.l_commitdate"
							+ " and exists ("
								+ " select"
									+ " *"
								+ " from"
									+ " " + lineitem_table_name + " l2"
								+ " where"
									+ " l2.l_orderkey = l1.l_orderkey"
									+ " and l2.l_suppkey <> l1.l_suppkey"
							+ " )"
							+ " and not exists ("
								+ " select"
									+ " *"
								+ " from"
									+ " " + lineitem_table_name + " l3"
								+ " where"
									+ " l3.l_orderkey = l1.l_orderkey"
									+ " and l3.l_suppkey <> l1.l_suppkey"
									+ " and l3.l_receiptdate >	+l3.l_commitdate"
							+ " )"
							+ " and s_nationkey = n_nationkey"
							+ " and n_name = '" + n_name_1 + "'"
						+ " group by"
							+ " s_name"
						+ " order by"
							+ " numwait desc,"
							+ " s_name"
					;
			break;
		case 22 :
			trans_type = QUERY22_TRANS_TYPE;
			// Generate Test Variables.
			if ( option_default ) {
				code1 = 13;
				code2 = 31;
				code3 = 23;
				code4 = 29;
				code5 = 30;
				code6 = 18;
				code7 = 17;
			} else {
				random_generator.init_random_permutation(25);
				code1 = random_generator.next_random_permutation() + 9;
				code2 = random_generator.next_random_permutation() + 9;
				code3 = random_generator.next_random_permutation() + 9;
				code4 = random_generator.next_random_permutation() + 9;
				code5 = random_generator.next_random_permutation() + 9;
				code6 = random_generator.next_random_permutation() + 9;
				code7 = random_generator.next_random_permutation() + 9;
			}
			if (logger.isDebugEnabled()) logger.debug( stream_number + "> code1 = " + code1 + "; code2 = " + code2  
					+ "; code3 = " + code3 + "; code4 = " + code4 
					+ "; code5 = " + code5 + "; code6 = " + code6 
					+ "; code7 = " + code7 );
			// Build Query Statement
			
			query_statement =
				"select"
						+ " cntrycode,"
						+ " count(*) as numcust,"
						+ " sum(c_acctbal) as totacctbal"
					+ " from ("
						+ " select"
							+ " substring(c_phone from 1 for 2) as cntrycode,"
							+ " c_acctbal"
						+ " from"
							+ " " + customer_table_name
						+ " where"
							+ " substring(c_phone from 1 for 2) in ('" + code1 + "', '" + code2 + "', '"
							    + code3 + "', '" + code4 + "', '" + code5 + "', '" + code6 + "', '" + code7 + "')"
							+ " and c_acctbal > ("
								+ " select"
								+ " avg(c_acctbal)"
							+ " from"
								+ " " + customer_table_name
							+ " where"
								+ " c_acctbal > 0.00"
								+ " and substring(c_phone from 1 for 2) in ('" + code1 + "', '" + code2 + "', '"
							        + code3 + "', '" + code4 + "', '" + code5 + "', '" + code6 + "', '" + code7 + "')"
							+ " )"
							+ " and not exists ("
								+ " select"
									+ " *"
								+ " from"
									+ " " + orders_table_name
								+ " where"
									+ " o_custkey = c_custkey"
							+ " )"
						+ " ) as custsale"
					+ " group by"
						+ " cntrycode"
					+ " order by"
						+ " cntrycode"
				;
			break;
		default: {
				throw new Exception("ERROR : Invalid query number specified ( query_num = " + query_num + " )");
			}
		}

		start_transaction(trans_type);

		if (logger.isDebugEnabled()) logger.debug(stream_number + "> accumlated_transactions " + String.format("%5d", accumlated_transactions ) );

		String transaction_data_string = "H" + scale_factor + " Query " + query_num + " : "
				+ query_statement;

		if (logger.isDebugEnabled()) logger.debug(stream_number + ">  H" + scale_factor + " Query " + query_num);
		if (logger.isDebugEnabled()) logger.debug(stream_number + ">    " + query_statement);


		try {

			start_timer();

			if ( option_sessionless ) {
				database_connector.establishDatabaseConnection();
			}

			int records_returned = 0;

			ResultSet resultset = database_connector.execute_query(query_statement);
			while (  ( records_returned < max_records ) && resultset.next() ) {
				records_returned++;
				String select_record = null;
				switch ( query_num ) {
				case 1 :
					select_record = "( " +
						resultset.getString(1) + ", " +
						resultset.getString(2) + ", " +
						resultset.getDouble(3) + ", " +
						resultset.getDouble(4) + ", " +
						resultset.getDouble(5) + ", " +
						resultset.getDouble(6) + ", " +
						resultset.getDouble(7) + ", " +
						resultset.getDouble(8) + ", " +
						resultset.getDouble(9) + ", " +
						resultset.getInt(10) + " )";
					break;
				case 2 :
					select_record = "( " +
							resultset.getDouble(1) + ", " +
							resultset.getString(2) + ", " +
							resultset.getString(3) + ", " +
							resultset.getInt(4) + ", " +
							resultset.getString(5) + ", " +
							resultset.getString(6) + ", " +
							resultset.getString(7) + ", " +
							resultset.getString(8) + " )";
					break;
				case 3 :
					select_record = "( " +
							resultset.getInt(1) + ", " +
							resultset.getDouble(2) + ", " +
							resultset.getString(3) + ", " +
							resultset.getString(4) + " )";
					break;
				case 4 :
					select_record = "( " +
							resultset.getString(1) + ", " +
							resultset.getInt(2) + " )";
					break;
				case 5 :
					select_record = "( " +
							resultset.getString(1) + ", " +
							resultset.getDouble(2) + " )";
					break;
				case 6 :
					select_record = "( " +
							resultset.getDouble(1) + " )";
					break;
				case 7 :
					select_record = "( " +
							resultset.getString(1) + ", " +
							resultset.getString(2) + ", " +
							resultset.getString(3) + ", " +
							resultset.getDouble(4) + " )";
					break;
				case 8 :
					select_record = "( " +
							resultset.getString(1) + ", " +
							resultset.getDouble(2) + " )";
					break;
				case 9 :
					select_record = "( " +
							resultset.getString(1) + ", " +
							resultset.getString(2) + ", " +
							resultset.getDouble(3) + " )";
					break;
				case 10 :
					select_record = "( " +
							resultset.getInt(1) + ", " +
							resultset.getString(2) + ", " +
							resultset.getDouble(3) + ", " +
							resultset.getDouble(4) + ", " +
							resultset.getString(5) + ", " +
							resultset.getString(6) + ", " +
							resultset.getString(7) + ", " +
							resultset.getString(8) + " )";
					break;
				case 11 :
					select_record = "( " +
							resultset.getInt(1) + ", " +
							resultset.getDouble(2) + " )";
					break;
				case 12 :
					select_record = "( " +
							resultset.getString(1) + ", " +
							resultset.getInt(2) + ", " +
							resultset.getInt(3) + " )";
					break;
				case 13 :
					select_record = "( " +
							resultset.getInt(1) + ", " +
							resultset.getInt(2) + " )";
					break;
				case 14 :
					select_record = "( " +
							resultset.getDouble(1) + " )";
					break;
				case 15 :
					select_record = "( " +
							resultset.getInt(1) + ", " +
							resultset.getString(2) + ", " +
							resultset.getString(3) + ", " +
							resultset.getString(4) + ", " +
							resultset.getDouble(5) + " )";
					break;
				case 16 :
					select_record = "( " +
							resultset.getString(1) + ", " +
							resultset.getString(2) + ", " +
							resultset.getInt(3) + ", " +
							resultset.getInt(4) + " )";
					break;
				case 17 :
					select_record = "( " +
							resultset.getDouble(1) + " )";
					break;
				case 18 :
					select_record = "( " +
							resultset.getString(1) + ", " +
							resultset.getInt(2) + ", " +
							resultset.getInt(3) + ", " +
							resultset.getString(4) + ", " +
							resultset.getDouble(5) + ", " +
							resultset.getDouble(6) + " )";
					break;
				case 19 :
					select_record = "( " +
							resultset.getDouble(1) + " )";
					break;
				case 20 :
					select_record = "( " +
							resultset.getString(1) + ", " +
							resultset.getString(2) + " )";
					break;
				case 21 :
					select_record = "( " +
							resultset.getString(1) + ", " +
							resultset.getInt(2) + " )";
					break;
				case 22 :
					select_record = "( " +
							resultset.getInt(1) + ", " +
							resultset.getInt(2) + ", " +
							resultset.getDouble(3) + " )";
					break;

				}

				if (logger.isDebugEnabled()) logger.debug(stream_number + ">  Query Results: " + select_record);
				// print out results
			}

			if ( option_sessionless ) {
				database_connector.closeDatabaseConnection();
			}

			stop_timer(trans_type);

			logger.info(stream_number + "> Query Number : " + accumlated_transactions 
					+ " ,  Query Type : " + TRANS_TYPE_NAMES[trans_type] 
					+ " , Records Returned : " + records_returned
					+ " , Elasped Time : " + String.format("%10.3f", transaction_elapsed_micro / 1000.0 )
					);

			transaction_success( trans_type );
			end_transaction();

		} catch (SQLException exception) {
			transaction_error( trans_type );
			String tombstone = "";
			tombstone = tombstone + "\n" + ">----------------------------------------------------------";
			tombstone = tombstone + "\n" + "> Unexpected SQLEXCEPTION,  Error Code = " + exception.getErrorCode();
			tombstone = tombstone + "\n" + ">   Stream Number : " + stream_number + " , Query Number : " + accumlated_transactions;
			tombstone = tombstone + "\n" + ">   " + transaction_data_string;
			SQLException nextException = exception;
			do {
				tombstone = tombstone + "\n" + "> " + nextException.getMessage();
			} while ((nextException = nextException.getNextException()) != null);
			tombstone = tombstone + "\n" + ">----------------------------------------------------------";
			logger.error(tombstone, exception);
			end_transaction();
			throw exception;
		}
	}

	//----------------------------------------------------------------------------------------------------------

	public void start() throws Exception {
		if (logger.isDebugEnabled()) logger.debug(stream_number + "> start()");

		// Initialize Random Number Generator
		random_generator.stream_number = stream_number;
		random_generator.option_controlrandom = option_controlrandom;
		random_generator.start();

		// Initialize table names
		lineitem_table_name = "tpch_lineitem_" + scale_factor;
		if ( schema != null ) {
			lineitem_table_name = schema + "." + lineitem_table_name;
		}

		part_table_name = "tpch_part_" + scale_factor;
		if ( schema != null ) {
			part_table_name = schema + "." + part_table_name;
		}

		supplier_table_name = "tpch_supplier_" + scale_factor;
		if ( schema != null ) {
			supplier_table_name = schema + "." + supplier_table_name;
		}

		partsupp_table_name = "tpch_partsupp_" + scale_factor;
		if ( schema != null ) {
			partsupp_table_name = schema + "." + partsupp_table_name;
		}

		nation_table_name = "tpch_nation_" + scale_factor;
		if ( schema != null ) {
			nation_table_name = schema + "." + nation_table_name;
		}

		region_table_name = "tpch_region_" + scale_factor;
		if ( schema != null ) {
			region_table_name = schema + "." + region_table_name;
		}

		orders_table_name = "tpch_orders_" + scale_factor;
		if ( schema != null ) {
			orders_table_name = schema + "." + orders_table_name;
		}

		customer_table_name = "tpch_customer_" + scale_factor;
		if ( schema != null ) {
			customer_table_name = schema + "." + customer_table_name;
		}

		calendar = Calendar.getInstance();

		// Initialize Database Connector
		database_connector.stream_number = stream_number;
		database_connector.database = database;
		database_connector.option_autocommit = option_autocommit;

		if ( ! option_sessionless ) { //  If not sessionless, connect to database
			if (logger.isDebugEnabled()) logger.debug(stream_number + "  !option_sessionless");
			database_connector.establishDatabaseConnection();
		}

		// Determine an execution order - Order set of 11 numbers
		if ( ! option_default ) {
			int[] queries = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22 };
			int numbers_remaining = 22;
			int numbers_picked = 0;
			int picked_number = 0;
			while (numbers_remaining > 0 ) {
				numbers_remaining--;
				picked_number = random_generator.generateRandomInteger(0,numbers_remaining);
				execution_order[numbers_picked++] = queries[picked_number];
				for (int indx = picked_number; indx < numbers_remaining; indx++) {
					queries[indx] = queries[indx + 1];
				}
			}
		}
		logger.info(stream_number + ">  Execution Order : " 
				+ execution_order[0] + " , "
				+ execution_order[1] + " , "
				+ execution_order[2] + " , "
				+ execution_order[3] + " , "
				+ execution_order[4] + " , "
				+ execution_order[5] + " , "
				+ execution_order[6] + " , "
				+ execution_order[7] + " , "
				+ execution_order[8] + " , "
				+ execution_order[9] + " , "
				+ execution_order[10] + " , "
				+ execution_order[11] + " , "
				+ execution_order[12] + " , "
				+ execution_order[13] + " , "
				+ execution_order[14] + " , "
				+ execution_order[15] + " , "
				+ execution_order[16] + " , "
				+ execution_order[17] + " , "
				+ execution_order[18] + " , "
				+ execution_order[19] + " , "
				+ execution_order[20] + " , "
				+ execution_order[21] 
			);

		if (logger.isDebugEnabled()) logger.debug(stream_number + "< start()");

	}


	//----------------------------------------------------------------------------------------------------------

	public void close() throws Exception {

		if ( ! option_sessionless ) { //  If not sessionless, close database connection
			database_connector.closeDatabaseConnection();
		}

	}

	//----------------------------------------------------------------------------------------------------------

}
