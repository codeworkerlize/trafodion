import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import org.apache.log4j.Logger;

public class OrderEntryWorkload2 {

	final int INVALID_SCALEFACTOR = -1;
	final int INVALID_STREAMNUMBER = -1;

	final int MAX_CONNECT_ATTEMPTS = 100;
	final int MAXIMUM_TRANSACTION_RETRY = 10;

	final int ITEM_ROWS_PER_SCALE = 100000;  // Fixed value
	final int WAREHOUSE_ROWS_PER_SCALE = 1;
	final int STOCK_ROWS_PER_SCALE = 100000;
	final int DISTRICT_ROWS_PER_SCALE = 10;
	final int CUSTOMER_ROWS_PER_SCALE = 30000;
	final int HISTORY_ROWS_PER_SCALE = 30000;
	final int ORDERS_ROWS_PER_SCALE = 30000;
	final int ORDERLINE_ROWS_PER_SCALE = 300000;
	final int NEWORDER_ROWS_PER_SCALE = 9000;

	final SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	final SimpleDateFormat hourformat = new SimpleDateFormat("HH:mm:ss");

	public int stream_number = INVALID_STREAMNUMBER;
	public int scale_factor = INVALID_SCALEFACTOR;

	public boolean option_autocommit = false;
	public boolean option_sessionless = false;
	public boolean option_prepareexec = false;
	public boolean option_directexec = false;
	public boolean option_specialexec = false;
	public boolean option_spj = false;
	public boolean option_batch = false;
	public boolean option_controlrandom = false;
	public boolean option_newkey = false;
	public boolean option_isolate = false;
	public boolean option_scaleperstream = true;
	public int scale_per_stream = 1;

	CallableStatement call_stmt = null;

	String customer_table_name = null;
	String district_table_name = null;
	String history_table_name = null;
	String item_table_name = null;
	String neworder_table_name = null;
	String orderline_table_name = null;
	String orders_table_name = null;
	String stock_table_name = null;
	String warehouse_table_name = null;

	public String schema = null;
	public String database = null;

	int prior_key = -1;
   
	//  neworder statements
	PreparedStatement neworder_select_warehouse_stmt = null;
	PreparedStatement neworder_select_customer_stmt = null;
	PreparedStatement neworder_select_district_stmt = null;
	PreparedStatement neworder_update_district_stmt = null;
	PreparedStatement neworder_insert_order_stmt = null;
	PreparedStatement neworder_insert_neworder_stmt = null;
	PreparedStatement neworder_select_item_stmt = null;
	PreparedStatement neworder_select_stock_stmt = null;
	PreparedStatement neworder_update_stock_stmt = null;
	PreparedStatement neworder_insert_orderline_stmt = null;

	// payment statements
	PreparedStatement payment_select_warehouse_stmt = null;
	PreparedStatement payment_select_customerid_stmt = null;
	PreparedStatement payment_select_district_stmt = null;
	PreparedStatement payment_select_customer_stmt = null;
	PreparedStatement payment_update_customer_bc_stmt = null;
	PreparedStatement payment_update_customer_stmt = null;
	PreparedStatement payment_update_warehouse_stmt = null;
	PreparedStatement payment_update_district_stmt = null;
	PreparedStatement payment_insert_history_stmt = null;

	// orderstatus statements
	PreparedStatement orderstatus_select_customerid_stmt = null;
	PreparedStatement orderstatus_select_customer_stmt = null;
	PreparedStatement orderstatus_select_orderid_stmt = null;
	PreparedStatement orderstatus_select_order_stmt = null;
	PreparedStatement orderstatus_select_orderline_stmt = null;

	// delivery statements
	PreparedStatement delivery_select_neworder_stmt = null;
	PreparedStatement delivery_delete_neworder_stmt = null;
	PreparedStatement delivery_select_order_stmt = null;
	PreparedStatement delivery_update_order_stmt = null;
	PreparedStatement delivery_update_orderline_stmt = null;
	PreparedStatement delivery_select_orderline_stmt = null;
	PreparedStatement delivery_update_customer_stmt = null;

	// stocklevel statements
	PreparedStatement stocklevel_select_district_stmt = null;
	PreparedStatement stocklevel_select_stockcount_stmt = null;

	RandomGenerator random_generator = new RandomGenerator();

	DatabaseConnector database_connector = new DatabaseConnector();
	String sql_statement = null;

	final static Logger logger = Logger.getLogger("JavaBench");

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

	public final int TOTAL_TRANS_TYPES = 5;
	public final String TRANS_TYPE_NAMES[] = { "neworder", "payment", "orderstatus", "delivery", "stocklevel" };
	public final int NEWORDER_TRANS_TYPE = 0;  // Fixed value
	public final int PAYMENT_TRANS_TYPE = 1;  // Fixed value
	public final int ORDERSTATUS_TRANS_TYPE = 2;  // Fixed value
	public final int DELIVERY_TRANS_TYPE = 3;  // Fixed value
	public final int STOCKLEVEL_TRANS_TYPE = 4;  // Fixed value
	
	public long[] accumlated_transactions_by_type = { 0, 0, 0, 0, 0 };
	public long[] accumlated_errors_by_type = { 0, 0, 0, 0, 0 };
	public long[] accumlated_retries_by_type = { 0, 0, 0, 0, 0 };
	public long[] accumlated_successes_by_type = { 0, 0, 0, 0, 0 };

	public long[] accumlated_transaction_response_micro_by_type = { 0, 0, 0, 0, 0 };
	public long[] maximum_transaction_response_micro_by_type = { 0, 0, 0, 0, 0 };
	public long[] minimum_transaction_response_micro_by_type = { 999999999999999999L, 999999999999999999L, 999999999999999999L, 999999999999999999L, 999999999999999999L };

	long start_transaction_time = 0;
	
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
		long transaction_elapsed_micro = ( System.nanoTime() - start_transaction_time ) / 1000;

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
		
		if (transaction_elapsed_micro > interval_maximum_transaction_response_micro ) {
			interval_maximum_transaction_response_micro = transaction_elapsed_micro;
		}
		if (transaction_elapsed_micro > maximum_transaction_response_micro ) {
			maximum_transaction_response_micro = transaction_elapsed_micro;
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

	public void runNewOrder() throws Exception {

		int trans_type = NEWORDER_TRANS_TYPE;
		int trans_query_num = 0;
		
		start_transaction(trans_type);
		
		if (logger.isDebugEnabled()) logger.debug(stream_number + ">> accumlated_transactions " + String.format("%5d", accumlated_transactions ) );

		//  Generate Test Variables
		int w_id;
		if ( option_scaleperstream ) {
			w_id = stream_number + 1;
		} else {
			if ( option_newkey ) {
				if ( scale_per_stream == 1 ) {
					throw new Exception("ERROR : option_newkey incompatible with scale_per_stream of 1.");
				}
				do {
					w_id = stream_number * scale_per_stream + random_generator.generateRandomInteger( 0 , ( scale_per_stream - 1 ));
				} while ( w_id == prior_key );
				prior_key = w_id;
			} else {
				w_id = stream_number * scale_per_stream + random_generator.generateRandomInteger( 1 , scale_per_stream );
			}
		}

		int d_id = random_generator.generateRandomInteger(1,10);
		int c_id = random_generator.NURand(1023,1,3000,1234);
		int o_ol_cnt = random_generator.generateRandomInteger(5,15);
		int o_all_local = 1;

		int[] ol_i_id_array = new int[o_ol_cnt];
		int[] ol_supply_w_id_array = new int[o_ol_cnt];
		int[] ol_quantity_array = new int[o_ol_cnt];
		boolean[] ol_remote_array = new boolean[o_ol_cnt];

		for ( int idx = 0; idx < o_ol_cnt; idx++ ) {
			ol_i_id_array[idx] = random_generator.NURand(8191,1,100000,1234);
		}
		Arrays.sort( ol_i_id_array );
		for ( int idx = 0; idx < o_ol_cnt; idx++ ) {
			if ( random_generator.generateRandomInteger(1,100) == 1 && scale_factor != 1 ) {  // 1% of the
				do {
					ol_supply_w_id_array[idx] = random_generator.generateRandomInteger(1,scale_factor);
				} while ( ol_supply_w_id_array[idx] == w_id );
				ol_remote_array[idx]=true;
				o_all_local = 0;
			} else {
				ol_supply_w_id_array[idx] = w_id;
				ol_remote_array[idx]=false;
			}
		}
		for ( int idx = 0; idx < o_ol_cnt; idx++ ) {
			ol_quantity_array[idx] = random_generator.generateRandomInteger(1,10);
		}
		boolean invalid_item = false;
		if ( random_generator.generateRandomInteger(1,100) == 1 ) {  // 1% of the time ... invalid item_id
			invalid_item = true;
			ol_i_id_array[o_ol_cnt-1]=-1;
		}

		String transaction_data_string = "NEWORDER"
				+ " , w_id = " + w_id
				+ " , d_id = " + d_id
				+ " , c_id = " + c_id
				+ " , o_ol_cnt = " + o_ol_cnt
				+ " , invalid_item = " + invalid_item;
		
		for ( int idx = 0; idx < o_ol_cnt; idx++ ) {
			transaction_data_string = transaction_data_string + "\n		"
					+ " ol = " + ( idx + 1 )
					+ " , ol_i_id = " + ol_i_id_array[idx]
					+ " , ol_w_id = " + ol_supply_w_id_array[idx]
					+ " , ol_qty = " + ol_quantity_array[idx]
					+ " , ol_remote = " + ol_remote_array[idx];
		}
		
		if (logger.isDebugEnabled()) logger.debug(stream_number + ">  NewOrder Transaction");
		if (logger.isDebugEnabled()) logger.debug(stream_number + ">    " + transaction_data_string);

		try {

			boolean retry_transaction = true;
			int retry_count = 0;

			start_timer();

			if ( option_sessionless ) {
				database_connector.establishDatabaseConnection();
			}

			while ( ( retry_count < MAXIMUM_TRANSACTION_RETRY ) && retry_transaction ) {

				retry_transaction = false;
				trans_query_num = 0;

				if ( option_spj ) {

					throw new Exception ("ERROR : option_spj not implemented for NewOrder");

				} else {

					ResultSet resultset = null;

					trans_query_num++;
					if (logger.isDebugEnabled()) logger.debug(stream_number + ">  NewOrder - Q" + trans_query_num + " - Select Warehouse, District and Customer Info - neworder_select_warehouse_stmt");
					if ( option_prepareexec ) {
						if ( neworder_select_warehouse_stmt == null ) {
							sql_statement = "select W_TAX, D_TAX, D_NEXT_O_ID, C_DISCOUNT, C_LAST, C_CREDIT"
									+ " from  " + warehouse_table_name + ", " + district_table_name + ", " + customer_table_name
									+ " where W_ID = D_W_ID and D_W_ID = C_W_ID and D_ID = C_D_ID and W_ID = ? and D_ID = ? and C_ID = ?";
							neworder_select_warehouse_stmt = database_connector.prepare_statement( sql_statement );
						}
						neworder_select_warehouse_stmt.setInt(1,w_id);
						neworder_select_warehouse_stmt.setInt(2,d_id);
						neworder_select_warehouse_stmt.setInt(3,c_id);
						resultset = database_connector.execute_prepared_query(neworder_select_warehouse_stmt);
					} else {
						sql_statement = "select W_TAX, D_TAX, D_NEXT_O_ID, C_DISCOUNT, C_LAST, C_CREDIT"
								+ " from  " + warehouse_table_name + ", " + district_table_name + ", " + customer_table_name
								+ " where W_ID = D_W_ID and D_W_ID = C_W_ID and D_ID = C_D_ID and W_ID = " + w_id + " and D_ID = " + d_id + " and C_ID = " +  c_id;
						resultset = database_connector.execute_query( sql_statement );
					}

					if (! resultset.next() ) {       // Expecting exactly 1 row
						if (logger.isDebugEnabled()) logger.debug(stream_number + ">  NewOrder - Q" + trans_query_num + " " + sql_statement + " no rows." );
					}
					double w_tax  = resultset.getDouble("W_TAX");
					double d_tax  = resultset.getDouble("D_TAX");
					int o_id  = resultset.getInt("D_NEXT_O_ID");
					double c_discount  = resultset.getDouble("C_DISCOUNT");
					String c_last  = resultset.getString("C_LAST");
					String c_credit  = resultset.getString("C_CREDIT");
					if (logger.isDebugEnabled()) logger.debug(stream_number + ">    W_TAX : " + w_tax + ", D_TAX : " + d_tax + ", D_NEXT_O_ID : " + o_id+ ", C_DISCOUNT :" + c_discount + ", C_LAST :" + c_last + ", C_CREDIT :" + c_credit);
					resultset.close();
					
					transaction_data_string = transaction_data_string + "\n		"
							+ "o_id = " + o_id;

					trans_query_num++;
					if (logger.isDebugEnabled()) logger.debug(stream_number + ">  NewOrder - Q" + trans_query_num + " - Update District - neworder_update_district_stmt");
					if ( option_prepareexec ) {
						if ( neworder_update_district_stmt == null ) {
							sql_statement = "update " + district_table_name + " set D_NEXT_O_ID = D_NEXT_O_ID + 1  where D_W_ID = ? and D_ID = ?";
							neworder_update_district_stmt = database_connector.prepare_statement( sql_statement );
						}
						neworder_update_district_stmt.setInt(1,w_id);
						neworder_update_district_stmt.setInt(2,d_id);
						database_connector.execute_prepared_update_statement(neworder_update_district_stmt);
					} else {
						sql_statement = "update " + district_table_name + " set D_NEXT_O_ID = D_NEXT_O_ID + 1 "
										+ " where D_W_ID = " + w_id + " and D_ID = " + d_id;
						if ( option_batch ) {
							database_connector.add_batch(sql_statement);
						} else {
							database_connector.execute_statement(sql_statement);
						}
					}

					trans_query_num++;
					if (logger.isDebugEnabled()) logger.debug(stream_number + ">  NewOrder Q" + trans_query_num + " - Insert Order - neworder_insert_order_stmt");
					if ( option_prepareexec ) {
						if ( neworder_insert_order_stmt == null ) {
							sql_statement = "insert into " + orders_table_name + "( O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL ) values ( ? , ? , ? , ? , CURRENT_TIMESTAMP, ? , ? )";
							neworder_insert_order_stmt = database_connector.prepare_statement( sql_statement );
						}
						neworder_insert_order_stmt.setInt(1,o_id);
						neworder_insert_order_stmt.setInt(2,d_id);
						neworder_insert_order_stmt.setInt(3,w_id);
						neworder_insert_order_stmt.setInt(4,c_id);
						neworder_insert_order_stmt.setInt(5,o_ol_cnt);
						neworder_insert_order_stmt.setInt(6,o_all_local);
						database_connector.execute_prepared_update_statement(neworder_insert_order_stmt);
					} else {
						sql_statement = "insert into " + orders_table_name
										+ "( O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL )"
										+ " values ( " + o_id + ", " + d_id + ", " + w_id + ", " + c_id
										+ ", CURRENT_TIMESTAMP, " + o_ol_cnt + ", " + o_all_local + " )";
						if ( option_batch ) {
							database_connector.add_batch(sql_statement);
						} else {
							database_connector.execute_statement(sql_statement);
						}
					}

					trans_query_num++;
					if (logger.isDebugEnabled()) logger.debug(stream_number + ">  NewOrder Q" + trans_query_num + " - Insert Neworder - neworder_insert_neworder_stmt");
					if ( option_prepareexec ) {
						if ( neworder_insert_neworder_stmt == null ) {
							sql_statement = "insert into " + neworder_table_name + "( NO_W_ID, NO_D_ID, NO_O_ID ) values ( ? , ? , ? )";
							neworder_insert_neworder_stmt = database_connector.prepare_statement( sql_statement );
						}
						neworder_insert_neworder_stmt.setInt(1,w_id);
						neworder_insert_neworder_stmt.setInt(2,d_id);
						neworder_insert_neworder_stmt.setInt(3,o_id);
						database_connector.execute_prepared_update_statement(neworder_insert_neworder_stmt);
					} else {
						sql_statement = "insert into " + neworder_table_name + "( NO_W_ID, NO_D_ID, NO_O_ID )"
										+ " values ( " + w_id + ", " + d_id + ", " + o_id + " )";
						if ( option_batch ) {
							database_connector.add_batch(sql_statement);
						} else {
							database_connector.execute_statement(sql_statement);
						}
					}

					int save_trans_query_num = trans_query_num;
					for ( int idx = 0; idx < o_ol_cnt; idx++ ) {

						trans_query_num = save_trans_query_num + 1;
						if (logger.isDebugEnabled()) logger.debug(stream_number + ">    Orderline: Line number " + ( idx + 1 ) );
						if (logger.isDebugEnabled()) logger.debug(stream_number + ">  NewOrder Q" + trans_query_num + " - Select Item - neworder_select_item_stmt");
						if ( option_prepareexec ) {
							if ( neworder_select_item_stmt == null ) {
								sql_statement = "select I_PRICE, I_NAME, I_DATA from " + item_table_name + " where I_ID = ?";
								neworder_select_item_stmt = database_connector.prepare_statement( sql_statement );
							}
							neworder_select_item_stmt.setInt(1,ol_i_id_array[idx]);
							resultset = database_connector.execute_prepared_query(neworder_select_item_stmt);
						} else {
							sql_statement = "select I_PRICE, I_NAME, I_DATA from " + item_table_name + " where I_ID = " + ol_i_id_array[idx];
							resultset = database_connector.execute_query( sql_statement );
						}

						if (! resultset.next() ) {       // Expecting exactly 1 row
							if (! invalid_item && logger.isDebugEnabled()) logger.debug(stream_number + ">  NewOrder - Q" + trans_query_num + " - " + sql_statement + " no rows." );
						}
						double i_price  = resultset.getDouble("I_PRICE");
						String i_name = resultset.getString("I_NAME");
						String i_data = resultset.getString("I_DATA");
						if (logger.isDebugEnabled()) logger.debug(stream_number + ">    I_PRICE:" + i_price + ", I_NAME:" + i_name + ", I_DATA:" + i_data );
						resultset.close();

						trans_query_num++;
						if (logger.isDebugEnabled()) logger.debug(stream_number + ">  NewOrder - Q" + trans_query_num + " - Select stock - neworder_select_stock_stmt");
						if ( option_prepareexec ) {
							if ( neworder_select_stock_stmt == null ) {
								sql_statement = "select S_QUANTITY, S_DATA, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10 from " + stock_table_name + " where S_W_ID = ? and S_I_ID = ?";
								neworder_select_stock_stmt = database_connector.prepare_statement( sql_statement );
							}
							neworder_select_stock_stmt.setInt(1,ol_supply_w_id_array[idx]);
							neworder_select_stock_stmt.setInt(2,ol_i_id_array[idx]);
							resultset = database_connector.execute_prepared_query(neworder_select_stock_stmt);
						} else {
							sql_statement = "select S_QUANTITY, S_DATA, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05,"
									+ " S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10 from " + stock_table_name
									+ " where S_W_ID = " + ol_supply_w_id_array[idx] + " and S_I_ID = " + ol_i_id_array[idx];
							resultset = database_connector.execute_query( sql_statement );
						}

						if (! resultset.next() ) {       // Expecting exactly 1 row
							if (logger.isDebugEnabled()) logger.debug(stream_number + ">  NewOrder - Q" + trans_query_num + " - " + sql_statement + " no rows." );
						}
						int s_quantity = resultset.getInt("S_QUANTITY");
						String s_dist_xx = resultset.getString("S_DIST_" + String.format("%02d",d_id));
						String s_data = resultset.getString("S_DATA");
						if (logger.isDebugEnabled()) logger.debug(stream_number + ">    S_QUANTITY:" + s_quantity + ", S_DIST_XX:" + s_dist_xx + ", S_DATA:" + s_data);
						resultset.close();

						if ( s_quantity > ol_quantity_array[idx] + 10 ) {
							s_quantity = s_quantity - ol_quantity_array[idx];
						} else {
							s_quantity = ( s_quantity - ol_quantity_array[idx] ) + 91;
						}

						int remote_cnt = 0;
						if ( ol_remote_array[idx] == true ) {
							remote_cnt = 1;
						}

						trans_query_num++;
						if (logger.isDebugEnabled()) logger.debug(stream_number + ">  NewOrder - Q" + trans_query_num + " - Update Stock - neworder_update_stock_stmt");
						if ( option_prepareexec ) {
							if ( neworder_update_stock_stmt == null ) {
								sql_statement = "update " + stock_table_name + " set S_QUANTITY = ?, S_YTD = S_YTD + ?, S_ORDER_CNT = S_ORDER_CNT + 1, S_REMOTE_CNT = S_REMOTE_CNT + ? where S_W_ID = ? and S_I_ID = ?";
								neworder_update_stock_stmt = database_connector.prepare_statement( sql_statement );
							}
							neworder_update_stock_stmt.setInt(1,s_quantity);
							neworder_update_stock_stmt.setInt(2,ol_quantity_array[idx]);
							neworder_update_stock_stmt.setInt(3,remote_cnt);
							neworder_update_stock_stmt.setInt(4,ol_supply_w_id_array[idx]);
							neworder_update_stock_stmt.setInt(5,ol_i_id_array[idx]);
							database_connector.execute_prepared_update_statement(neworder_update_stock_stmt);
						} else {
							sql_statement = "update " + stock_table_name
										+ " set S_QUANTITY = " + s_quantity + " , S_YTD = S_YTD + " + ol_quantity_array[idx] + ","
										+ " S_ORDER_CNT = S_ORDER_CNT + 1, S_REMOTE_CNT = S_REMOTE_CNT + " + remote_cnt
										+ " where S_W_ID = " + ol_supply_w_id_array[idx] + " and S_I_ID = " + ol_i_id_array[idx];
							if ( option_batch ) {
								database_connector.add_batch(sql_statement);
							} else {
								database_connector.execute_statement(sql_statement);
							}
						}

						trans_query_num++;
						if (logger.isDebugEnabled()) logger.debug(stream_number + ">  NewOrder - Q" + trans_query_num + " - Insert Orderline - neworder_insert_orderline_stmt");
						if ( option_prepareexec ) {
							if ( neworder_insert_orderline_stmt == null ) {
								sql_statement = "insert into " + orderline_table_name + " ( OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO ) values ( ?, ?, ?, ?, ?, ?, ?, ?, ? )";
								neworder_insert_orderline_stmt = database_connector.prepare_statement( sql_statement );
							}
							neworder_insert_orderline_stmt.setInt(1,w_id);
							neworder_insert_orderline_stmt.setInt(2,d_id);
							neworder_insert_orderline_stmt.setInt(3,o_id);
							neworder_insert_orderline_stmt.setInt(4,( idx + 1 ));
							neworder_insert_orderline_stmt.setInt(5,ol_i_id_array[idx]);
							neworder_insert_orderline_stmt.setInt(6,ol_supply_w_id_array[idx]);
							neworder_insert_orderline_stmt.setInt(7,ol_quantity_array[idx]);
							neworder_insert_orderline_stmt.setDouble(8,ol_quantity_array[idx] * i_price);
							neworder_insert_orderline_stmt.setString(9,s_dist_xx);
							database_connector.execute_prepared_update_statement(neworder_insert_orderline_stmt);
						} else {
							sql_statement = "insert into " + orderline_table_name
										+ " (  OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER, OL_I_ID, "
										+ "OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO ) "
										+ "values ( " + w_id
										+ " , " + d_id
										+ " , " + o_id
										+ " , " + ( idx + 1 )
										+ " , " + ol_i_id_array[idx]
										+ " , " + ol_supply_w_id_array[idx]
										+ " , " + ol_quantity_array[idx]
										+ " , " + String.format("%.2f",(ol_quantity_array[idx] * i_price))
										+ " , '" + s_dist_xx + "' )";
							if ( option_batch ) {
								database_connector.add_batch(sql_statement);
							} else {
								database_connector.execute_statement(sql_statement);
							}
						}
					}

					if ( option_batch ) {  // if batch, it is time to do the work
						database_connector.execute_batch();
					}

					// Commit Transaction
					trans_query_num++;
					if ( !option_autocommit ) {
						// commit transaction
						try {
							database_connector.commit_work();
						} catch (SQLException exception) {
							if ( ( database.equals("trafodion") ) && ( exception.getErrorCode() == -8606) ) {
								// transaction failed on commit ... ok to retry
								transaction_retry( trans_type );
								database_connector.rollback_work();
								retry_count++;
								retry_transaction = true;
								logger.info(stream_number + ">  Transaction retry.  Transaction : " + accumlated_transactions + "  Retry : " + retry_count + " [" + dateformat.format(new Date()) + "]" );
								Thread.sleep( 10 * retry_count );  // Sleep
							} else {
								throw exception;
							}
						}
					}
				}
			}

			if ( option_sessionless ) {
				database_connector.closeDatabaseConnection();
				neworder_select_warehouse_stmt = null;
				neworder_select_customer_stmt = null;
				neworder_select_district_stmt = null;
				neworder_update_district_stmt = null;
				neworder_insert_order_stmt = null;
				neworder_insert_neworder_stmt = null;
				neworder_select_item_stmt = null;
				neworder_select_stock_stmt = null;
				neworder_update_stock_stmt = null;
				neworder_insert_orderline_stmt = null;
				call_stmt = null;
			}

			stop_timer(trans_type);
	
			if ( retry_count >= MAXIMUM_TRANSACTION_RETRY ) {
			
				transaction_error( trans_type );
				String tombstone = "";
				tombstone = tombstone + "\n" + stream_number + ">----------------------------------------------------------";
				tombstone = tombstone + "\n" + stream_number + "> ERROR, Transaction failed to commit after " + retry_count + " retries.  Giving up on the transaction.";
				tombstone = tombstone + "\n" + stream_number + ">   Stream Number : " + stream_number + " , Transaction Number : " + accumlated_transactions;
				tombstone = tombstone + "\n" + stream_number + ">   " + transaction_data_string;
				tombstone = tombstone + "\n" + stream_number + ">----------------------------------------------------------";
				logger.error(tombstone);
				end_transaction();
			} else {
				transaction_success ( trans_type );
				end_transaction();
			}

		} catch (SQLException exception) {
			if ( ( invalid_item == true )
					&& ( database.equals("trafodion") || database.equals("seaquest") )
					&& ( (exception.getErrorCode() == -29050) || exception.getErrorCode() == -29024)) {
				// expected value out of range error; rollback transaction
				transaction_success ( trans_type );
				if ( !option_autocommit ) { database_connector.rollback_work(); }
				end_transaction();
	
			} else {
				
				transaction_error( trans_type );
				String tombstone = "";
				tombstone = tombstone + "\n" + stream_number + ">----------------------------------------------------------";
				tombstone = tombstone + "\n" + stream_number + "> Transaction " + TRANS_TYPE_NAMES[ trans_type ] + ", Query " + trans_query_num;
				tombstone = tombstone + "\n" + stream_number + "> Unexpected SQLEXCEPTION,  Error Code = " + exception.getErrorCode();
				tombstone = tombstone + "\n" + stream_number + ">   Stream Number : " + stream_number + " , Transaction Number : " + accumlated_transactions;
				tombstone = tombstone + "\n" + stream_number + ">   " + transaction_data_string;
				SQLException nextException = exception;
				do {
					tombstone = tombstone + "\n" + stream_number + "> " + nextException.getMessage();
				} while ((nextException = nextException.getNextException()) != null);
				tombstone = tombstone + "\n" + stream_number + ">----------------------------------------------------------";
				logger.error(tombstone, exception);
				System.out.println(tombstone);
				if ( !option_autocommit ) { database_connector.rollback_work(); }
				end_transaction();
				throw exception;
			}
		}
	}

	//----------------------------------------------------------------------------------------------------------

//--	@SuppressWarnings("resource")
	public void runPayment () throws Exception {

		int trans_type = PAYMENT_TRANS_TYPE;
		int trans_query_num = 0;
		start_transaction(trans_type);

		if (logger.isDebugEnabled()) logger.debug(stream_number + ">> accumlated_transactions " + String.format("%5d", accumlated_transactions ) );

		//  Generate Test Variables
		int w_id;
		if ( option_scaleperstream ) {
			w_id = stream_number + 1;
		} else {
			if ( option_newkey ) {
				if ( scale_per_stream == 1 ) {
					throw new Exception("ERROR : option_newkey incompatible with scale_per_stream of 1.");
				}
				do {
					w_id = stream_number * scale_per_stream + random_generator.generateRandomInteger( 0 , ( scale_per_stream - 1 ));
				} while ( w_id == prior_key );
				prior_key = w_id;
			} else {
				w_id = stream_number * scale_per_stream + random_generator.generateRandomInteger( 1 , scale_per_stream );
			}
		}

		int d_id = random_generator.generateRandomInteger(1,10);

		//	 85% customer warehouse/distict is local
		int c_w_id = w_id;
		int c_d_id = d_id;
		boolean remote_customer = false;
		if (( random_generator.generateRandomInteger(1,100) > 85 ) && (scale_factor > 1)) {
			remote_customer = false;
			do {
				c_w_id = random_generator.generateRandomInteger( 1, scale_factor);
				c_d_id = random_generator.generateRandomInteger( 1, 10);
			} while ( c_w_id == w_id && c_d_id == d_id );
		}

		//	 60% search on customer last name, 40% on customer id
		boolean search_on_c_name;
		int c_id = random_generator.NURand(1023,1,3000,1234);
		String c_last = null;
		if ( random_generator.generateRandomInteger(1,100) > 60 ) {
			search_on_c_name = false;
		} else {
			search_on_c_name = true;
			c_last = random_generator.generateRandomLastName(random_generator.NURand(255,0,999,1234));
		}

		double h_amount = random_generator.generateRandomInteger(100, 500000) / 100.0;

		String transaction_data_string = "PAYMENT"
				+ " , w_id = " + w_id
				+ " , d_id = " + d_id
				+ " , remote_customer = " + remote_customer
				+ " , c_w_id = " + c_w_id
				+ " , c_d_id = " + c_d_id
				+ "\n" + stream_number + "		, search_on_c_name = " + search_on_c_name
				+ " , c_id = " + c_id
				+ " , c_last = " + c_last
				+ " , h_amount = " + h_amount;
		
		if (logger.isDebugEnabled()) logger.debug("\n" + stream_number + ">  Payment Transaction");
		if (logger.isDebugEnabled()) logger.debug(stream_number + ">    " + transaction_data_string );

		try {

			boolean retry_transaction = true;
			int retry_count = 0;

			start_timer();

			if ( option_sessionless ) {
				database_connector.establishDatabaseConnection();
			}

			while ( ( retry_count < MAXIMUM_TRANSACTION_RETRY ) && retry_transaction ) {

				retry_transaction = false;
				trans_query_num = 0;

				if ( option_spj ) {
	
					throw new Exception ("ERROR : option_spj not implemented for Payment");
	
				} else {
	
					ResultSet resultset = null;
	
					trans_query_num++;
					if (logger.isDebugEnabled()) logger.debug(stream_number + ">  Payment - Q" + trans_query_num + " - Select Warehouse - payment_select_warehouse_stmt");
					if ( option_prepareexec ) {
						if ( payment_select_warehouse_stmt == null ) {
							sql_statement = "select W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP from " + warehouse_table_name + " where W_ID = ?";
							payment_select_warehouse_stmt = database_connector.prepare_statement( sql_statement );
						}
						payment_select_warehouse_stmt.setInt(1,w_id);
						resultset = payment_select_warehouse_stmt.executeQuery();
					} else {
						sql_statement = "select W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP "
								+ " from " + warehouse_table_name
								+ " where W_ID = " + w_id;
						resultset = database_connector.execute_query( sql_statement );
					}
	
					if (! resultset.next() ) {       // Expecting exactly 1 row
						if (logger.isDebugEnabled()) logger.debug(stream_number + ">  Payment - Q" + trans_query_num + " - " + sql_statement + " no rows." );
					}
					String w_name = resultset.getString("W_NAME");
					String w_street_1 = resultset.getString("W_STREET_1");
					String w_street_2 = resultset.getString("W_STREET_2");
					String w_city  = resultset.getString("W_CITY");
					String w_state  = resultset.getString("W_STATE");
					String w_zip  = resultset.getString("W_ZIP");
					if (logger.isDebugEnabled()) logger.debug(stream_number + ">   W_NAME:" + w_name
						+ ", W_STREET_1:" + w_street_1 + ", W_STREET_2:" + w_street_2
						+ ", W_CITY:" + w_city + ", W_STATE:" + w_state + ", W_ZIP:" + w_zip);
					resultset.close();
	
					trans_query_num++;
					if ( search_on_c_name ) {
						if (logger.isDebugEnabled()) logger.debug(stream_number + ">  Payment - Q" + trans_query_num + " - Select Customer ID - payment_select_customerid_stmt");
						if ( option_prepareexec ) {
							if ( payment_select_customerid_stmt == null ) {
								sql_statement = "select C_ID from " + customer_table_name + " where C_W_ID = ? and C_D_ID = ? and C_LAST = ? order by C_FIRST";
								payment_select_customerid_stmt = database_connector.prepare_scrollable_query( sql_statement );
							}
							payment_select_customerid_stmt.setInt(1,c_w_id);
							payment_select_customerid_stmt.setInt(2,c_d_id);
							payment_select_customerid_stmt.setString(3,c_last);
							resultset = payment_select_customerid_stmt.executeQuery();
						} else {
							sql_statement = "select C_ID from " + customer_table_name
										+ " where C_W_ID = " + c_w_id + " and C_D_ID = " + c_d_id
										+ " and C_LAST = '" + c_last + "' order by C_FIRST";
							resultset = database_connector.execute_scrollable_query( sql_statement );
						}
						// Use the customer that is n/2 rounded up
						int rowcount = 0;
						if ( resultset.last() ) {
							rowcount = resultset.getRow();
							resultset.beforeFirst();
						}
						int row_to_use = (rowcount + 1) / 2;
						do {
						} while (resultset.next() && ( resultset.getRow() != row_to_use ));
						if (resultset.getRow() > 0) {
							c_id = resultset.getInt("C_ID");
						} else {
							if (logger.isDebugEnabled()) logger.debug(stream_number + ">  Payment - Q" + trans_query_num + " - " + sql_statement + " no rows." );
						}
						if (logger.isDebugEnabled()) logger.debug(stream_number + ">  C_ID: " + c_id);
						resultset.close();
					}
	
					trans_query_num++;
					if (logger.isDebugEnabled()) logger.debug(stream_number + ">  Payment - Q" + trans_query_num + " - Select District - payment_select_district_stmt");
					if ( option_prepareexec ) {
						if ( payment_select_district_stmt == null ) {
							sql_statement = "select D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP "
									+ " from " + district_table_name + " where D_W_ID = ? and D_ID = ?";
							payment_select_district_stmt = database_connector.prepare_statement( sql_statement );
						}
						payment_select_district_stmt.setInt(1,w_id);
						payment_select_district_stmt.setInt(2,d_id);
						resultset = payment_select_district_stmt.executeQuery();
					} else {
						sql_statement = "select D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP from " + district_table_name
									+ " where D_W_ID = " + w_id + " and D_ID = " + d_id;
						resultset = database_connector.execute_query( sql_statement );
					}
	
					if (! resultset.next() ) {       // Expecting exactly 1 row
						if (logger.isDebugEnabled()) logger.debug(stream_number + ">  Payment - Q" + trans_query_num + " - " + sql_statement + " no rows." );
					}
					String d_name = resultset.getString("D_NAME");
					String d_street_1 = resultset.getString("D_STREET_1");
					String d_street_2 = resultset.getString("D_STREET_2");
					String d_city  = resultset.getString("D_CITY");
					String d_state  = resultset.getString("D_STATE");
					String d_zip  = resultset.getString("D_ZIP");
					if (logger.isDebugEnabled()) logger.debug(stream_number + ">  D_NAME:" + d_name
						+ ", D_STREET_1:" + d_street_1 + ", D_STREET_2:" + d_street_2
						+ ", D_CITY:" + d_city + ", D_STATE:" + d_state + ", D_ZIP:" + d_zip);
					resultset.close();
	
					trans_query_num++;
					if (logger.isDebugEnabled()) logger.debug(stream_number + ">  Payment - Q" + trans_query_num + " - Select Customer - payment_select_customer_stmt");
					if ( option_prepareexec ) {
						if ( payment_select_customer_stmt == null ) {
							sql_statement = "select C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY,"
									+ " C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_DATA"
									+ " from " + customer_table_name + " where C_W_ID = ? and C_D_ID = ? and C_ID = ?";
							payment_select_customer_stmt = database_connector.prepare_statement( sql_statement );
						}
						payment_select_customer_stmt.setInt(1,c_w_id);
						payment_select_customer_stmt.setInt(2,c_d_id);
						payment_select_customer_stmt.setInt(3,c_id);
						resultset = payment_select_customer_stmt.executeQuery();
					} else {
						sql_statement = "select C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY,"
									+ " C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_DATA"
									+ " from " + customer_table_name
									+ " where C_W_ID = " + c_w_id + " and C_D_ID = " + c_d_id + " and C_ID = " + c_id;
						resultset = database_connector.execute_query( sql_statement );
					}
	
					if (! resultset.next() ) {       // Expecting exactly 1 row
						if (logger.isDebugEnabled()) logger.debug(stream_number + ">  Payment - Q" + trans_query_num + " - " + sql_statement + " no rows." );
					}
					String c_first = resultset.getString("C_FIRST");
					String c_middle = resultset.getString("C_MIDDLE");
					c_last = resultset.getString("C_LAST");
					String c_street_1 = resultset.getString("C_STREET_1");
					String c_street_2 = resultset.getString("C_STREET_2");
					String c_city = resultset.getString("C_CITY");
					String c_state = resultset.getString("C_STATE");
					String c_zip = resultset.getString("C_ZIP");
					String c_phone = resultset.getString("C_PHONE");
					java.sql.Timestamp c_since = resultset.getTimestamp("C_SINCE");
					String c_credit = resultset.getString("C_CREDIT");
					double c_credit_lim = resultset.getDouble("C_CREDIT_LIM");
					double c_discount = resultset.getDouble("C_DISCOUNT");
					double c_balance = resultset.getDouble("C_BALANCE");
					String c_data = resultset.getString("C_DATA");
					if (logger.isDebugEnabled()) logger.debug(stream_number + ">    C_FIRST: " + c_first
						+ ", C_MIDDLE: " + c_middle + ", C_LAST: " + c_last
						+ ", C_STREET_1: " + c_street_1 + ", C_STREET_2: " + c_street_2
						+ ", C_CITY: " + c_city + ", C_STATE: " + c_state
						+ ", C_ZIP: " + c_zip + ", C_PHONE: " + c_phone
						+ ", C_SINCE: " + c_since + ", C_CREDIT: " + c_credit
						+ ", C_CREDIT_LIM: " + c_credit_lim + ", C_DISCOUNT: " + c_discount
						+ ", C_BALANCE: " + c_balance + ", C_DATA: " + c_data);
					resultset.close();
	
					trans_query_num++;
					if ( c_credit.equals("BC")) {
	
						String new_c_data = c_id + c_d_id + c_w_id + d_id + w_id + h_amount + c_data;
						if ( new_c_data.length() > 500 ) {
							new_c_data = new_c_data.substring(0,499);
						}
	
						if (logger.isDebugEnabled()) logger.debug(stream_number + ">  Payment - Q" + trans_query_num + " - Update Customer BC - payment_update_customer_bc_stmt");
						if ( option_prepareexec ) {
							if ( payment_update_customer_bc_stmt == null ) {
								sql_statement = "update " + customer_table_name
										+ " set C_BALANCE = C_BALANCE + ? "
										+ " , C_YTD_PAYMENT = C_YTD_PAYMENT + ? "
										+ " , C_PAYMENT_CNT = C_PAYMENT_CNT +  1 "
										+ " , C_DATA = ? "
										+ " where C_W_ID = ? and C_D_ID = ? and C_ID = ? ";
								payment_update_customer_bc_stmt = database_connector.prepare_statement( sql_statement );
							}
							payment_update_customer_bc_stmt.setDouble(1,h_amount);
							payment_update_customer_bc_stmt.setDouble(2,h_amount);
							payment_update_customer_bc_stmt.setString(3,new_c_data);
							payment_update_customer_bc_stmt.setInt(4,c_w_id);
							payment_update_customer_bc_stmt.setInt(5,c_d_id);
							payment_update_customer_bc_stmt.setInt(6,c_id);
							payment_update_customer_bc_stmt.executeUpdate();
						} else {
							sql_statement = "update " + customer_table_name
										+ " set C_BALANCE = C_BALANCE + " + h_amount
										+ " , C_YTD_PAYMENT = C_YTD_PAYMENT + " + h_amount
										+ " , C_PAYMENT_CNT = C_PAYMENT_CNT +  1 "
										+ " , C_DATA ='" + new_c_data + "' "
										+ " where C_W_ID = " + c_w_id + " and C_D_ID = " + c_d_id + " and C_ID = " + c_id;
							if ( option_batch ) {
								database_connector.add_batch(sql_statement);
							} else {
								database_connector.execute_statement(sql_statement);
							}
						}
	
					} else {
	
						if (logger.isDebugEnabled()) logger.debug(stream_number + ">  Payment - Q" + trans_query_num + " - Update Customer - payment_update_customer_stmt");
						if ( option_prepareexec ) {
							if ( payment_update_customer_stmt == null ) {
								sql_statement = "update " + customer_table_name
										+ " set C_BALANCE = C_BALANCE + ?, C_YTD_PAYMENT = C_YTD_PAYMENT + ?, C_PAYMENT_CNT = C_PAYMENT_CNT +  1 "
										+ " where C_W_ID = ? and C_D_ID = ? and C_ID = ? ";
								payment_update_customer_stmt = database_connector.prepare_statement( sql_statement );
							}
							payment_update_customer_stmt.setDouble(1,h_amount);
							payment_update_customer_stmt.setDouble(2,h_amount);
							payment_update_customer_stmt.setInt(3,c_w_id);
							payment_update_customer_stmt.setInt(4,c_d_id);
							payment_update_customer_stmt.setInt(5,c_id);
							payment_update_customer_stmt.executeUpdate();
						} else {
							sql_statement = "update " + customer_table_name
										+ " set C_BALANCE = C_BALANCE + " + h_amount
										+ ", C_YTD_PAYMENT = C_YTD_PAYMENT + " + h_amount
										+ ", C_PAYMENT_CNT = C_PAYMENT_CNT +  1 "
										+ " where C_W_ID = " + c_w_id + " and C_D_ID = " + c_d_id + " and C_ID = " + c_id;
							if ( option_batch ) {
								database_connector.add_batch(sql_statement);
							} else {
								database_connector.execute_statement(sql_statement);
							}
						}
					}
	
					trans_query_num++;
					if (logger.isDebugEnabled()) logger.debug(stream_number + ">  Payment - Q" + trans_query_num + " - Update Warehouse Amount - payment_update_warehouse_stmt");
					if ( option_prepareexec ) {
						if ( payment_update_warehouse_stmt == null ) {
							sql_statement = "update " + warehouse_table_name + " set W_YTD = W_YTD + ? where W_ID = ?";
							payment_update_warehouse_stmt = database_connector.prepare_statement( sql_statement );
						}
						payment_update_warehouse_stmt.setDouble(1,h_amount);
						payment_update_warehouse_stmt.setInt(2,w_id);
						payment_update_warehouse_stmt.executeUpdate();
					} else {
						sql_statement = "update " + warehouse_table_name + " set W_YTD = W_YTD + " + h_amount + " where W_ID = " + w_id;
						if ( option_batch ) {
							database_connector.add_batch(sql_statement);
						} else {
							database_connector.execute_statement(sql_statement);
						}
					}
	
					trans_query_num++;
					if (logger.isDebugEnabled()) logger.debug(stream_number + ">  Payment - Q" + trans_query_num + " - Update District Amount - payment_update_district_stmt");
					if ( option_prepareexec ) {
						if ( payment_update_district_stmt == null ) {
							sql_statement = "update " + district_table_name + " set D_YTD = D_YTD + ? where D_W_ID= ? and D_ID= ?";
							payment_update_district_stmt = database_connector.prepare_statement( sql_statement );
						}
						payment_update_district_stmt.setDouble(1,h_amount);
						payment_update_district_stmt.setInt(2,w_id);
						payment_update_district_stmt.setInt(3,d_id);
						payment_update_district_stmt.executeUpdate();
					} else {
						sql_statement = "update " + district_table_name + " set D_YTD = D_YTD + " + h_amount
									+ " where D_W_ID = " + w_id + " and D_ID = " + d_id;
						if ( option_batch ) {
							database_connector.add_batch(sql_statement);
						} else {
							database_connector.execute_statement(sql_statement);
						}
					}
	
					trans_query_num++;
					if (logger.isDebugEnabled()) logger.debug(stream_number + ">  Payment - Q" + trans_query_num + " - Insert History - payment_insert_history_stmt");
					if ( option_prepareexec ) {
						if ( payment_insert_history_stmt == null ) {
							sql_statement = "insert into " + history_table_name
									+ " (H_DATE, H_W_ID, H_D_ID, H_C_ID, H_C_W_ID, H_C_D_ID, H_AMOUNT, H_DATA) "
									+ " values ( CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?, ? )";
							payment_insert_history_stmt = database_connector.prepare_statement( sql_statement );
						}
						payment_insert_history_stmt.setInt(1,w_id);
						payment_insert_history_stmt.setInt(2,d_id);
						payment_insert_history_stmt.setInt(3,c_id);
						payment_insert_history_stmt.setInt(4,c_w_id);
						payment_insert_history_stmt.setInt(5,c_d_id);
						payment_insert_history_stmt.setDouble(6,h_amount);
						payment_insert_history_stmt.setString(7,w_name + "    " + d_name);
					} else {
						sql_statement = "insert into " + history_table_name
									+ " (H_DATE, H_W_ID, H_D_ID, H_C_ID, H_C_W_ID, H_C_D_ID, H_AMOUNT, H_DATA) "
									+ " values ( CURRENT_TIMESTAMP," + w_id + "," + d_id + ","
									+ c_id + "," + c_w_id + "," + c_d_id + "," + h_amount + ", '" + w_name + "    " + d_name + "' )";
						if ( option_batch ) {
							database_connector.add_batch(sql_statement);
						} else {
							database_connector.execute_statement(sql_statement);
						}
					}
	
					if ( option_batch ) {  // if batch, it is time to do the work
						database_connector.execute_batch();
					}
	
					// Commit Transaction
					trans_query_num++;
					if ( !option_autocommit ) {
						// commit transaction
						try {
							database_connector.commit_work();
						} catch (SQLException exception) {
							if ( ( database.equals("trafodion") ) && ( exception.getErrorCode() == -8606) ) {
								// transaction failed on commit ... ok to retry
								transaction_retry( trans_type );

								database_connector.rollback_work();
								retry_count++;
								logger.info(stream_number + ">  Transaction retry.  Transaction : " + accumlated_transactions + "  Retry : " + retry_count + " [" + dateformat.format(new Date()) + "]" );
								Thread.sleep( 10 * retry_count );  // Sleep
							} else {
								throw exception;
							}
						}
					}
				}
			}

			if ( option_sessionless ) {
				database_connector.closeDatabaseConnection();
				payment_select_warehouse_stmt = null;
				payment_select_customerid_stmt = null;
				payment_select_district_stmt = null;
				payment_select_customer_stmt = null;
				payment_update_customer_bc_stmt = null;
				payment_update_customer_stmt = null;
				payment_update_warehouse_stmt = null;
				payment_update_district_stmt = null;
				payment_insert_history_stmt = null;
				call_stmt = null;
			}

			stop_timer(trans_type);
		
			if ( retry_count >= MAXIMUM_TRANSACTION_RETRY ) {
				
				transaction_error( trans_type );
				String tombstone = "";
				tombstone = tombstone + "\n" + stream_number + ">----------------------------------------------------------";
				tombstone = tombstone + "\n" + stream_number + "> ERROR, Transaction failed to commit after " + retry_count + " retries.  Giving up on the transaction.";
				tombstone = tombstone + "\n" + stream_number + ">   Stream Number : " + stream_number + " , Transaction Number : " + accumlated_transactions;
				tombstone = tombstone + "\n" + stream_number + ">   " + transaction_data_string;
				tombstone = tombstone + "\n" + stream_number + ">----------------------------------------------------------";
				logger.error(tombstone);
				end_transaction();
			} else {
				transaction_success ( trans_type );
				end_transaction();
			}

		} catch (SQLException exception) {
			
			transaction_error( trans_type );
			String tombstone = "";
			tombstone = tombstone + "\n" + stream_number + ">----------------------------------------------------------";
			tombstone = tombstone + "\n" + stream_number + "> Transaction " + TRANS_TYPE_NAMES[ trans_type ] + ", Query " + trans_query_num;
			tombstone = tombstone + "\n" + stream_number + "> Unexpected SQLEXCEPTION,  Error Code = " + exception.getErrorCode();
			tombstone = tombstone + "\n" + stream_number + ">   Stream Number : " + stream_number + " , Transaction Number : " + accumlated_transactions;
			tombstone = tombstone + "\n" + stream_number + ">   " + transaction_data_string;
			SQLException nextException = exception;
			do {
				tombstone = tombstone + "\n" + stream_number + "> " + nextException.getMessage();
			} while ((nextException = nextException.getNextException()) != null);
			tombstone = tombstone + "\n" + stream_number + ">----------------------------------------------------------";
			logger.error(tombstone, exception);
			System.out.println(tombstone);
			if ( !option_autocommit ) { database_connector.rollback_work(); }
			end_transaction();
			throw exception;
		}
	}

	
	//----------------------------------------------------------------------------------------------------------

//--	@SuppressWarnings("resource")
	public void runOrderStatus () throws Exception {

		int trans_type = ORDERSTATUS_TRANS_TYPE;
		int trans_query_num = 0;
		start_transaction(trans_type);

		if (logger.isDebugEnabled()) logger.debug(stream_number + ">> accumlated_transactions " + String.format("%5d", accumlated_transactions ) );

		//  Generate Test Variables
		int w_id;
		if ( option_scaleperstream ) {
			w_id = stream_number + 1;
		} else {
			if ( option_newkey ) {
				if ( scale_per_stream == 1 ) {
					throw new Exception("ERROR : option_newkey incompatible with scale_per_stream of 1.");
				}
				do {
					w_id = stream_number * scale_per_stream + random_generator.generateRandomInteger( 0 , ( scale_per_stream - 1 ));
				} while ( w_id == prior_key );
				prior_key = w_id;
			} else {
				w_id = stream_number * scale_per_stream + random_generator.generateRandomInteger( 1 , scale_per_stream );
			}
		}

		int d_id = random_generator.generateRandomInteger(1,10);

		//	 60% search on customer last name, 40% on customer id
		boolean search_on_c_name;
		int c_id = random_generator.NURand(1023,1,3000,1234);
		String c_last = null;
		if ( random_generator.generateRandomInteger(1,100) > 60 ) {
			search_on_c_name = false;
		} else {
			search_on_c_name = true;
			c_last = random_generator.generateRandomLastName(random_generator.NURand(255,0,999,1234));
		}

		String transaction_data_string = "ORDERSTATUS"
				+ " , w_id = " + w_id
				+ " , d_id = " + d_id
				+ " , search_on_c_name = " + search_on_c_name
				+ " , c_id = " + c_id
				+ " , c_last = " + c_last;

		if (logger.isDebugEnabled()) logger.debug(stream_number + ">  OrderStatus Transaction");
		if (logger.isDebugEnabled()) logger.debug(stream_number + ">    " + transaction_data_string );

		try {

			boolean retry_transaction = true;
			int retry_count = 0;

			start_timer();

			if ( option_sessionless ) {
				database_connector.establishDatabaseConnection();
			}

			while ( ( retry_count < MAXIMUM_TRANSACTION_RETRY ) && retry_transaction ) {

				retry_transaction = false;
				trans_query_num = 0;

				if ( option_spj ) {
	
					throw new Exception ("ERROR : option_spj not implemented for OrderStatus");
	
				} else {
	
					ResultSet resultset = null;
	
					trans_query_num++;
					if ( search_on_c_name ) {
						if (logger.isDebugEnabled()) logger.debug(stream_number + ">  OrderStatus - Q" + trans_query_num + " - Select Customer ID - orderstatus_select_customerid_stmt");
						if ( option_prepareexec ) {
							if ( orderstatus_select_customerid_stmt == null ) {
								sql_statement = "select C_ID from " + customer_table_name + " where C_W_ID = ? and C_D_ID = ? and C_LAST = ? order by C_FIRST";
								orderstatus_select_customerid_stmt = database_connector.prepare_scrollable_query( sql_statement );
							}
							orderstatus_select_customerid_stmt.setInt(1,w_id);
							orderstatus_select_customerid_stmt.setInt(2,d_id);
							orderstatus_select_customerid_stmt.setString(3,c_last);
							resultset=orderstatus_select_customerid_stmt.executeQuery();
						} else {
							sql_statement = "select C_ID from " + customer_table_name
										+ " where C_W_ID = " + w_id + " and C_D_ID = " + d_id + " and C_LAST = '" + c_last + "' order by C_FIRST";
							resultset = database_connector.execute_query( sql_statement );
						}
	
						// Use the customer that is n/2 rounded up
						int rowcount = 0;
						if ( resultset.last() ) {
							rowcount = resultset.getRow();
							resultset.beforeFirst();
						}
						int row_to_use = (rowcount + 1) / 2;
						do {
						} while (resultset.next() && ( resultset.getRow() != row_to_use ));
						if (resultset.getRow() > 0) {
							c_id = resultset.getInt("C_ID");
						} else {
							if (logger.isDebugEnabled()) logger.debug(stream_number + ">  OrderStatus - Q" + trans_query_num + " - " + sql_statement + " no rows." );
						}
						if (logger.isDebugEnabled()) logger.debug(stream_number + ">  C_ID: " + c_id);
						resultset.close();
					}
	
					trans_query_num++;
					if (logger.isDebugEnabled()) logger.debug(stream_number + ">  OrderStatus - Q" + trans_query_num + " - Select Customer - orderstatus_select_customer_stmt");
					if ( option_prepareexec ) {
						if ( orderstatus_select_customer_stmt == null ) {
							sql_statement = "select C_FIRST, C_MIDDLE, C_LAST, C_BALANCE from " + customer_table_name
									+ " where C_W_ID = ? and C_D_ID = ? and C_ID = ?";
							orderstatus_select_customer_stmt = database_connector.prepare_statement( sql_statement );
						}
						orderstatus_select_customer_stmt.setInt(1,w_id);
						orderstatus_select_customer_stmt.setInt(2,d_id);
						orderstatus_select_customer_stmt.setInt(3,c_id);
						resultset=orderstatus_select_customer_stmt.executeQuery();
					} else {
						sql_statement = "select C_FIRST, C_MIDDLE, C_LAST, C_BALANCE from " + customer_table_name
									+ " where C_W_ID = " + w_id + " and C_D_ID = " + d_id + " and C_ID = " + c_id + "";
						resultset = database_connector.execute_query( sql_statement );
					}
	
					if (! resultset.next() ) {       // Expecting exactly 1 row
						if (logger.isDebugEnabled()) logger.debug(stream_number + ">  OrderStatus - Q" + trans_query_num + " - " + sql_statement + " no rows." );
					}
					String c_first = resultset.getString("C_FIRST");
					String c_middle = resultset.getString("C_MIDDLE");
					c_last = resultset.getString("C_LAST");
					double c_balance = resultset.getDouble("C_BALANCE");
					if (logger.isDebugEnabled()) logger.debug(stream_number + ">   C_ID: " + c_id
						+ ", C_FIRST: " + c_first
						+ ", C_MIDDLE: " + c_middle
						+ ", C_LAST: " + c_last
						+ ", C_BALANCE: " + c_balance);
					resultset.close();
	
					trans_query_num++;
					if (logger.isDebugEnabled()) logger.debug(stream_number + ">  OrderStatus - Q" + trans_query_num + " - Select Order ID - orderstatus_select_orderid_stmt");
					if ( option_prepareexec ) {
						if ( orderstatus_select_orderid_stmt == null ) {
							sql_statement = "select max(O_ID) as O_ID" + " from " + orders_table_name
									+ " where O_W_ID = ? and O_D_ID = ? and O_C_ID= ?";
							orderstatus_select_orderid_stmt = database_connector.prepare_statement( sql_statement );
						}
						orderstatus_select_orderid_stmt.setInt(1,w_id);
						orderstatus_select_orderid_stmt.setInt(2,d_id);
						orderstatus_select_orderid_stmt.setInt(3,c_id);
						resultset=orderstatus_select_orderid_stmt.executeQuery();
					} else {
						sql_statement = "select max(O_ID) as O_ID" + " from " + orders_table_name
									+ " where O_W_ID = " + w_id + " and O_D_ID = " + d_id + " and O_C_ID= " + c_id;
						resultset = database_connector.execute_query( sql_statement );
					}
	
					int save_trans_query_num = trans_query_num;
					if ( resultset.next() ) {
						// Order Exits
						int o_id = resultset.getInt("O_ID");
						if (logger.isDebugEnabled()) logger.debug(stream_number + ">  O_ID: " + o_id);
						resultset.close();
	
						trans_query_num++;
						if (logger.isDebugEnabled()) logger.debug(stream_number + ">  OrderStatus - Q" + trans_query_num + " - Select Order -orderstatus_select_order_stmt ");
						if ( option_prepareexec ) {
							if ( orderstatus_select_order_stmt == null ) {
								sql_statement = "select O_ENTRY_D, O_CARRIER_ID, O_OL_CNT" + " from " + orders_table_name
										+ " where O_W_ID = ? and O_D_ID = ? and O_ID = ?";
								orderstatus_select_order_stmt = database_connector.prepare_statement( sql_statement );
							}
							orderstatus_select_order_stmt.setInt(1,w_id);
							orderstatus_select_order_stmt.setInt(2,d_id);
							orderstatus_select_order_stmt.setInt(3,o_id);
							resultset=orderstatus_select_order_stmt.executeQuery();
						} else {
							sql_statement = "select O_ENTRY_D, O_CARRIER_ID, O_OL_CNT" + " from " + orders_table_name
									+ " where O_W_ID = " + w_id + " and O_D_ID = " + d_id + " and O_ID = " + o_id;
							resultset = database_connector.execute_query( sql_statement );
						}
	
						if (! resultset.next() ) {       // Expecting exactly 1 row
							if (logger.isDebugEnabled()) logger.debug(stream_number + ">  OrderStatus - Q" + trans_query_num + " - " + sql_statement + " no rows." );
						}
						java.sql.Timestamp o_entry_d = resultset.getTimestamp("O_ENTRY_D");
						int o_carrier_id = resultset.getInt("O_CARRIER_ID");
						int o_ol_cnt = resultset.getInt("O_OL_CNT");
						resultset.close();
						if (logger.isDebugEnabled()) logger.debug(stream_number + ">   O_ENTRY_D: " + o_entry_d
							+ ", O_CARRIER_ID: " + o_carrier_id
							+ ", O_OL_CNT: " + o_ol_cnt);
	
						trans_query_num++;
						if (logger.isDebugEnabled()) logger.debug(stream_number + ">  OrderStatus - Q" + trans_query_num + " - Select Order Lines - orderstatus_select_orderline_stmt");
						if ( option_prepareexec ) {
							if ( orderstatus_select_orderline_stmt == null ) {
								sql_statement = "select OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D from "
										+ orderline_table_name
										+ " where OL_W_ID = ? and OL_D_ID = ? and OL_O_ID = ?"
										+ " order by OL_NUMBER";
								orderstatus_select_orderline_stmt = database_connector.prepare_statement( sql_statement );
							}
							orderstatus_select_orderline_stmt.setInt(1,w_id);
							orderstatus_select_orderline_stmt.setInt(2,d_id);
							orderstatus_select_orderline_stmt.setInt(3,o_id);
							resultset=orderstatus_select_orderline_stmt.executeQuery();
						} else {
							sql_statement = "select OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D from "
										+ orderline_table_name
										+ " where OL_W_ID = " + w_id + " and OL_D_ID = " + d_id + " and OL_O_ID = " + o_id
										+ " order by OL_NUMBER";
							resultset = database_connector.execute_query( sql_statement );
						}
	
						while (resultset.next()) {
							int ol_number = resultset.getInt("OL_NUMBER");
							int ol_i_id = resultset.getInt("OL_I_ID");
							int ol_supply_w_id = resultset.getInt("OL_SUPPLY_W_ID");
							int ol_quantity = resultset.getInt("OL_QUANTITY");
							double ol_amount  = resultset.getDouble("OL_AMOUNT");
							java.sql.Timestamp ol_delivery_d  = resultset.getTimestamp("OL_DELIVERY_D");
							if (logger.isDebugEnabled()) logger.debug(stream_number + ">  OL_NUMBER: " + ol_number
								+ ", OL_I_ID: " + ol_i_id
								+ ", OL_SUPPLY_W_ID: " + ol_supply_w_id
								+ ", OL_QUANTITY: " + ol_quantity
								+ ", OL_AMOUNT: " + ol_amount
								+ ", OL_DELIVERY_D: " + ol_delivery_d);
						}
						resultset.close();
	
					} else {
						// No such order
						System.out.println("  No Order Pending for (O_W_ID,O_D_ID,O_C_ID) = (" + w_id + "," + d_id + "," + c_id + ")");
					}
	
					// Commit Transaction
					trans_query_num = save_trans_query_num + 2;
					if ( !option_autocommit ) {
						// commit transaction
						try {
							database_connector.commit_work();
						} catch (SQLException exception) {
							if ( ( database.equals("trafodion") ) && ( exception.getErrorCode() == -8606) ) {
								// transaction failed on commit ... ok to retry
								transaction_retry( trans_type );

								database_connector.rollback_work();
								retry_count++;
								retry_transaction = true;
								logger.info(stream_number + ">  Transaction retry.  Transaction : " + accumlated_transactions + "  Retry : " + retry_count + " [" + dateformat.format(new Date()) + "]" );
								Thread.sleep( 10 * retry_count );  // Sleep
							} else {
								throw exception;
							}
						}
					}
				}
			}

			if ( option_sessionless ) {
				database_connector.closeDatabaseConnection();
				orderstatus_select_customerid_stmt = null;
				orderstatus_select_customer_stmt = null;
				orderstatus_select_orderid_stmt = null;
				orderstatus_select_order_stmt = null;
				orderstatus_select_orderline_stmt = null;
				call_stmt = null;
			}

			stop_timer(trans_type);
		
			if ( retry_count >= MAXIMUM_TRANSACTION_RETRY ) {
				
				transaction_error( trans_type );
				String tombstone = "";
				tombstone = tombstone + "\n" + stream_number + ">----------------------------------------------------------";
				tombstone = tombstone + "\n" + stream_number + "> ERROR, Transaction failed to commit after " + retry_count + " retries.  Giving up on the transaction.";
				tombstone = tombstone + "\n" + stream_number + ">   Stream Number : " + stream_number + " , Transaction Number : " + accumlated_transactions;
				tombstone = tombstone + "\n" + stream_number + ">   " + transaction_data_string;
				tombstone = tombstone + "\n" + stream_number + ">----------------------------------------------------------";
				logger.error(tombstone);
				end_transaction();
			} else {
				transaction_success ( trans_type );
				end_transaction();
			}

		} catch (SQLException exception) {
			if ( ( database.equals("trafodion") || database.equals("seaquest") )
					&& ( (exception.getErrorCode() == -29050) || exception.getErrorCode() == -29024)) {
				// it is possible the customer has no order; rollback and ignore
				if ( !option_autocommit ) { database_connector.rollback_work(); }
				transaction_success ( trans_type );
				end_transaction();
	
			} else {
				
				transaction_error( trans_type );
				String tombstone = "";
				tombstone = tombstone + "\n" + stream_number + ">----------------------------------------------------------";
				tombstone = tombstone + "\n" + stream_number + "> Transaction " + TRANS_TYPE_NAMES[ trans_type ] + ", Query " + trans_query_num;
				tombstone = tombstone + "\n" + stream_number + "> Unexpected SQLEXCEPTION,  Error Code = " + exception.getErrorCode();
				tombstone = tombstone + "\n" + stream_number + ">   Stream Number : " + stream_number + " , Transaction Number : " + accumlated_transactions;
				tombstone = tombstone + "\n" + stream_number + ">   " + transaction_data_string;
				SQLException nextException = exception;
				do {
					tombstone = tombstone + "\n" + stream_number + "> " + nextException.getMessage();
				} while ((nextException = nextException.getNextException()) != null);
				tombstone = tombstone + "\n" + stream_number + ">----------------------------------------------------------";
				logger.error(tombstone, exception);
				System.out.println(tombstone);
				if ( !option_autocommit ) { database_connector.rollback_work(); }
				end_transaction();
				throw exception;
			}
		}
	}

	//----------------------------------------------------------------------------------------------------------

	public void runDelivery () throws Exception {

		int trans_type = DELIVERY_TRANS_TYPE;
		int trans_query_num = 0;
		start_transaction(trans_type);

		if (logger.isDebugEnabled()) logger.debug(stream_number + ">> accumlated_transactions " + String.format("%5d", accumlated_transactions ) );

		//  Generate Test Variables
		int w_id;
		if ( option_scaleperstream ) {
			w_id = stream_number + 1;
		} else {
			if ( option_newkey ) {
				if ( scale_per_stream == 1 ) {
					throw new Exception("ERROR : option_newkey incompatible with scale_per_stream of 1.");
				}
				do {
					w_id = stream_number * scale_per_stream + random_generator.generateRandomInteger( 0 , ( scale_per_stream - 1 ));
				} while ( w_id == prior_key );
				prior_key = w_id;
			} else {
				w_id = stream_number * scale_per_stream + random_generator.generateRandomInteger( 1 , scale_per_stream );
			}
		}

		int o_carrier_id = random_generator.generateRandomInteger(1,10);   // 10 streams per warehouse

		String transaction_data_string = "DELIVERY , w_id = " + w_id + " , o_carrier_id = " + o_carrier_id;

		if (logger.isDebugEnabled()) logger.debug(stream_number + ">  Delivery Transaction");
		if (logger.isDebugEnabled()) logger.debug(stream_number + ">    " + transaction_data_string );

		try {

			boolean retry_transaction = true;
			int retry_count = 0;

			start_timer();

			if ( option_sessionless ) {
				database_connector.establishDatabaseConnection();
			}

			while ( ( retry_count < MAXIMUM_TRANSACTION_RETRY ) && retry_transaction ) {

				retry_transaction = false;
				trans_query_num = 0;

				if ( option_spj ) {

					throw new Exception ("ERROR : option_spj not implemented for Delivery");

				} else {

					ResultSet resultset = null;
					
					int delivery_districts = 0;

					for ( int d_id = 1; d_id <= 10; d_id++ ) {

						trans_query_num++;
						if (logger.isDebugEnabled()) logger.debug(stream_number + "> Delivery (W_ID,D_ID) = (" + w_id + "," + d_id + ")");
						if (logger.isDebugEnabled()) logger.debug(stream_number + "> Select from Neworder - Q" + trans_query_num + " - oldest order id for each districts in our warehouse - delivery_select_neworder_stmt");
						if ( option_prepareexec ) {
							if ( delivery_select_neworder_stmt == null ) {
								sql_statement = "select NO_O_ID from " + neworder_table_name + " where NO_W_ID = ? and NO_D_ID = ? order by NO_O_ID";
								delivery_select_neworder_stmt = database_connector.prepare_statement( sql_statement );
							}
							delivery_select_neworder_stmt.setInt(1,w_id);
							delivery_select_neworder_stmt.setInt(2,d_id);
							resultset = delivery_select_neworder_stmt.executeQuery();
						} else {
							sql_statement = "select NO_O_ID as O_ID" + " from " + neworder_table_name
										+ " where NO_W_ID = " + w_id + " and NO_D_ID = " + d_id + " order by NO_O_ID";
							resultset = database_connector.execute_query( sql_statement );
						}

						int save_trans_query_num = trans_query_num;
						if ( resultset.next() ) {

							// Delivery pending for District
							delivery_districts++;

							int o_id = resultset.getInt("NO_O_ID");
							if (logger.isDebugEnabled()) logger.debug(stream_number + ">  NO_O_ID: " + o_id );
							resultset.close();

							if (logger.isDebugEnabled()) logger.debug(stream_number + ">  DELIVERY , w_id = " + w_id + " , d_id = " + d_id + " , o_id = " + o_id);

							trans_query_num++;
							if (logger.isDebugEnabled()) logger.debug(stream_number + ">  Delete from Neworder - Q" + trans_query_num + " - delivery_delete_neworder_stmt");
							if ( option_prepareexec ) {
								if ( delivery_delete_neworder_stmt == null ) {
									sql_statement = "delete from " + neworder_table_name + " where NO_W_ID = ? and NO_D_ID = ? and NO_O_ID = ?";
									delivery_delete_neworder_stmt = database_connector.prepare_statement( sql_statement );
								}
								delivery_delete_neworder_stmt.setInt(1,w_id);
								delivery_delete_neworder_stmt.setInt(2,d_id);
								delivery_delete_neworder_stmt.setInt(3,o_id);
								delivery_delete_neworder_stmt.execute();
							} else {
								sql_statement = "delete from " + neworder_table_name
											+ " where NO_W_ID = " + w_id + " and NO_D_ID = " + d_id + " and NO_O_ID = " + o_id;
							}

							trans_query_num++;
							if (logger.isDebugEnabled()) logger.debug(stream_number + ">  Select from Order - Q" + trans_query_num + " - delivery_select_order_stmt");
							if ( option_prepareexec ) {
								if ( delivery_select_order_stmt == null ) {
									if (logger.isDebugEnabled()) logger.debug(stream_number + ">  delivery_select_order_stmt");
									sql_statement = "select O_C_ID from " + orders_table_name + " where O_W_ID = ? and O_D_ID = ? and O_ID = ?";
									delivery_select_order_stmt = database_connector.prepare_statement( sql_statement );
								}
								delivery_select_order_stmt.setInt(1,w_id);
								delivery_select_order_stmt.setInt(2,d_id);
								delivery_select_order_stmt.setInt(3,o_id);
								resultset = delivery_select_order_stmt.executeQuery();
							} else {
								sql_statement = "select O_C_ID from " + orders_table_name
											+ " where O_W_ID = " + w_id + " and O_D_ID = " + d_id + " and O_ID = " + o_id;
								resultset = database_connector.execute_query( sql_statement );
							}

							if (! resultset.next() ) {       // Expecting exactly 1 row
								if (logger.isDebugEnabled()) logger.debug(stream_number + ">  Delivery - Q" + trans_query_num + " - " + sql_statement + " no rows." );
							}
							int c_id = resultset.getInt("O_C_ID");
							if (logger.isDebugEnabled()) logger.debug(stream_number + ">  C_ID: " + c_id );
							resultset.close();

							trans_query_num++;
							if (logger.isDebugEnabled()) logger.debug(stream_number + ">  Update Order with O_CARRIER_ID Q" + trans_query_num + " - delivery_update_order_stmt");
							if ( option_prepareexec ) {
								if ( delivery_update_order_stmt == null ) {
									sql_statement = "update " + orders_table_name + " set O_CARRIER_ID = ? where O_W_ID = ? and O_D_ID = ? and O_ID = ?";
									delivery_update_order_stmt = database_connector.prepare_statement( sql_statement );
								}
								delivery_update_order_stmt.setInt(1,o_carrier_id);
								delivery_update_order_stmt.setInt(2,w_id);
								delivery_update_order_stmt.setInt(3,d_id);
								delivery_update_order_stmt.setInt(4,o_id);
								delivery_update_order_stmt.executeUpdate();
							} else {
								sql_statement = "update " + orders_table_name + " set O_CARRIER_ID = " + o_carrier_id + ""
											+ " where O_W_ID = " + w_id + " and O_D_ID = " + d_id + " and O_ID = " + o_id;
								if ( option_batch ) {
									database_connector.add_batch(sql_statement);
								} else {
									database_connector.execute_statement(sql_statement);
								}
							}

							trans_query_num++;
							if (logger.isDebugEnabled()) logger.debug(stream_number + ">  Update OrderLines with Delivery Date - Q" + trans_query_num + " - delivery_update_orderline_stmt");
							if ( option_prepareexec ) {
								if ( delivery_update_orderline_stmt == null ) {
									sql_statement = "update " + orderline_table_name + " set OL_DELIVERY_D = CURRENT_TIMESTAMP"
											+ " where OL_W_ID = ? and OL_D_ID = ? and OL_O_ID = ?";
									delivery_update_orderline_stmt = database_connector.prepare_statement( sql_statement );
								}
								delivery_update_orderline_stmt.setInt(1,w_id);
								delivery_update_orderline_stmt.setInt(2,d_id);
								delivery_update_orderline_stmt.setInt(3,o_id);
								delivery_update_orderline_stmt.executeUpdate();
							} else {
								sql_statement = "update " + orderline_table_name + " set OL_DELIVERY_D = CURRENT_TIMESTAMP"
											+ " where OL_W_ID = " + w_id + " and OL_D_ID = " + d_id + " and OL_O_ID = " + o_id;
								if ( option_batch ) {
									database_connector.add_batch(sql_statement);
								} else {
									database_connector.execute_statement(sql_statement);
								}
							}

							trans_query_num++;
							if (logger.isDebugEnabled()) logger.debug(stream_number + ">  Select from Orderline - Q" + trans_query_num + " - the sum of Orderline amounts for each Delivered Order - delivery_select_orderline_stmt");
							if ( option_prepareexec ) {
								if ( delivery_select_orderline_stmt == null ) {
									sql_statement = "select sum(OL_AMOUNT) as O_AMOUNT from " + orderline_table_name
											+ " where OL_W_ID = ? and OL_D_ID = ? and OL_O_ID = ?";
									delivery_select_orderline_stmt = database_connector.prepare_statement( sql_statement );
								}
								delivery_select_orderline_stmt.setInt(1,w_id);
								delivery_select_orderline_stmt.setInt(2,d_id);
								delivery_select_orderline_stmt.setInt(3,o_id);
								resultset = delivery_select_orderline_stmt.executeQuery();
							} else {
								sql_statement = "select sum(OL_AMOUNT) as O_AMOUNT from " + orderline_table_name
											+ " where OL_W_ID = " + w_id + " and OL_D_ID = " + d_id + " and OL_O_ID = " + o_id;
								resultset = database_connector.execute_query( sql_statement );
							}

							if (! resultset.next() ) {       // Expecting exactly 1 row
								if (logger.isDebugEnabled()) logger.debug(stream_number + ">  Delivery - Q" + trans_query_num + " - " + sql_statement + " no rows." );
							}
							double o_amount = resultset.getDouble("O_AMOUNT");
							if (logger.isDebugEnabled()) logger.debug(stream_number + ">  O_AMOUNT: " + o_amount );
							resultset.close();

							trans_query_num++;
							if (logger.isDebugEnabled()) logger.debug(stream_number + ">  Update each Customer - Q" + trans_query_num + " - delivery_update_customer_stmt");
							if ( option_prepareexec ) {
								if ( delivery_update_customer_stmt == null ) {
									sql_statement = "update " + customer_table_name + " set C_BALANCE = C_BALANCE + ?,"
											+ " C_DELIVERY_CNT = C_DELIVERY_CNT + 1 where C_W_ID = ? and C_D_ID = ? and C_ID = ?";
									delivery_update_customer_stmt = database_connector.prepare_statement( sql_statement );
								}
								delivery_update_customer_stmt.setDouble(1,o_amount);
								delivery_update_customer_stmt.setInt(2,w_id);
								delivery_update_customer_stmt.setInt(3,d_id);
								delivery_update_customer_stmt.setInt(4,c_id);
								delivery_update_customer_stmt.executeUpdate();
							} else {
								sql_statement = "update " + customer_table_name + " set C_BALANCE = C_BALANCE + " + o_amount + ","
											+ " C_DELIVERY_CNT = C_DELIVERY_CNT + 1"
											+ " where C_W_ID = " + w_id + " and C_D_ID = " + d_id + " and C_ID = " + c_id;
								if ( option_batch ) {
									database_connector.add_batch(sql_statement);
								} else {
									database_connector.execute_statement(sql_statement);
								}
							}
						} else {
							// No Delivery Pending for District
							System.out.println("  No Delivery Pending for (W_ID,D_ID) = (" + w_id + "," + d_id + ")");
						}
						trans_query_num = save_trans_query_num + 6;
					}

					if (logger.isDebugEnabled()) logger.debug(stream_number + ">  Deliveries processed for " + delivery_districts + " out of 10 districts.");
				}
				
				if ( option_batch ) {  // if batch, it is time to do the work
					database_connector.execute_batch();
				}

				// Commit Transaction
				trans_query_num++;
				if ( !option_autocommit ) {
					// commit transaction
					try {
						database_connector.commit_work();
					} catch (SQLException exception) {
						if ( ( database.equals("trafodion") ) && ( exception.getErrorCode() == -8606) ) {
							// transaction failed on commit ... ok to retry
							transaction_retry( trans_type );

							database_connector.rollback_work();
							retry_count++;
							retry_transaction = true;
							
							logger.info(stream_number + ">  Transaction retry.  Transaction : " + accumlated_transactions + "  Retry : " + retry_count + " [" + dateformat.format(new Date()) + "]" );
							Thread.sleep( 10 * retry_count );  // Sleep
						} else {
							throw exception;
						}
					}
				}
			}

			if ( option_sessionless ) {
				database_connector.closeDatabaseConnection();
				delivery_select_neworder_stmt = null;
				delivery_delete_neworder_stmt = null;
				delivery_select_order_stmt = null;
				delivery_update_order_stmt = null;
				delivery_update_orderline_stmt = null;
				delivery_select_orderline_stmt = null;
				delivery_update_customer_stmt = null;
				call_stmt = null;
			}

			stop_timer(trans_type);
		
			if ( retry_count >= MAXIMUM_TRANSACTION_RETRY ) {
				
				transaction_error( trans_type );
				String tombstone = "";
				tombstone = tombstone + "\n" + stream_number + ">----------------------------------------------------------";
				tombstone = tombstone + "\n" + stream_number + "> ERROR, Transaction failed to commit after " + retry_count + " retries.  Giving up on the transaction.";
				tombstone = tombstone + "\n" + stream_number + ">   Stream Number : " + stream_number + " , Transaction Number : " + accumlated_transactions;
				tombstone = tombstone + "\n" + stream_number + ">   " + transaction_data_string;
				tombstone = tombstone + "\n" + stream_number + ">----------------------------------------------------------";
				logger.error(tombstone);
				end_transaction();
			} else {
				transaction_success ( trans_type );
				end_transaction();
			}

		} catch (SQLException exception) {
			
			transaction_error( trans_type );
			String tombstone = "";
			tombstone = tombstone + "\n" + stream_number + ">----------------------------------------------------------";
			tombstone = tombstone + "\n" + stream_number + "> Transaction " + TRANS_TYPE_NAMES[ trans_type ] + ", Query " + trans_query_num;
			tombstone = tombstone + "\n" + stream_number + "> Unexpected SQLEXCEPTION,  Error Code = " + exception.getErrorCode();
			tombstone = tombstone + "\n" + stream_number + ">   Stream Number : " + stream_number + " , Transaction Number : " + accumlated_transactions;
			tombstone = tombstone + "\n" + stream_number + ">   " + transaction_data_string;
			SQLException nextException = exception;
			do {
				tombstone = tombstone + "\n" + stream_number + "> " + nextException.getMessage();
			} while ((nextException = nextException.getNextException()) != null);
			tombstone = tombstone + "\n" + stream_number + ">----------------------------------------------------------";
			logger.error(tombstone, exception);
			System.out.println(tombstone);
			if ( !option_autocommit ) { database_connector.rollback_work(); }
			end_transaction();
			throw exception;
		}
	}

//----------------------------------------------------------------------------------------------------------

//--	@SuppressWarnings("resource")
	public void runStockLevel() throws Exception {

		int trans_type = STOCKLEVEL_TRANS_TYPE;
		int trans_query_num = 0;
		start_transaction(trans_type);

		if (logger.isDebugEnabled()) logger.debug(stream_number + ">> accumlated_transactions " + String.format("%5d", accumlated_transactions ) );

		//  Generate Test Variables
		int w_id;
		if ( option_scaleperstream ) {
			w_id = stream_number + 1;
		} else {
			if ( option_newkey ) {
				if ( scale_per_stream == 1 ) {
					throw new Exception("ERROR : option_newkey incompatible with scale_per_stream of 1.");
				}
				do {
					w_id = stream_number * scale_per_stream + random_generator.generateRandomInteger( 0 , ( scale_per_stream - 1 ));
				} while ( w_id == prior_key );
				prior_key = w_id;
			} else {
				w_id = stream_number * scale_per_stream + random_generator.generateRandomInteger( 1 , scale_per_stream );
			}
		}

		int d_id = random_generator.generateRandomInteger(1,10);
		int threshold = random_generator.generateRandomInteger(10,20);

		String transaction_data_string = "STOCKLEVEL"
					+ " , w_id = " + w_id
					+ " , d_id = " + d_id
					+ " , threshold = " + threshold;

		if (logger.isDebugEnabled()) logger.debug(stream_number + ">  StockLevel Transaction");
		if (logger.isDebugEnabled()) logger.debug(stream_number + ">    " + transaction_data_string );

		try {

			boolean retry_transaction = true;
			int retry_count = 0;

			start_timer();

			if ( option_sessionless ) {
				database_connector.establishDatabaseConnection();
			}

			while ( ( retry_count < MAXIMUM_TRANSACTION_RETRY ) && retry_transaction ) {

				retry_transaction = false;
				trans_query_num = 0;
				
				if ( option_spj ) {
	
					throw new Exception ("ERROR : option_spj not implemented for StockLevel");
	
				} else {
	
					ResultSet resultset = null;
	
					trans_query_num++;
					if ( option_prepareexec && ! option_specialexec ) {
						if (logger.isDebugEnabled()) logger.debug(stream_number + ">  StockLevel - Q" + trans_query_num + " - Select District Order - stocklevel_select_district_stmt");
						if ( stocklevel_select_district_stmt == null ) {
							sql_statement = "select D_NEXT_O_ID from " + district_table_name + " where D_W_ID = ? and D_ID = ?";
							stocklevel_select_district_stmt = database_connector.prepare_statement( sql_statement );
						}
						stocklevel_select_district_stmt.setInt(1,w_id);
						stocklevel_select_district_stmt.setInt(2,d_id);
						resultset = stocklevel_select_district_stmt.executeQuery();
	
					} else {
	
						sql_statement = "select D_NEXT_O_ID from " + district_table_name 
								+ " where D_W_ID = " + w_id + " and D_ID = " + d_id;
						resultset = database_connector.execute_query( sql_statement );
						
					}
	
					if (! resultset.next() ) {       // Expecting exactly 1 row
						if (logger.isDebugEnabled()) logger.debug(stream_number + ">  StockLevel - Q" + trans_query_num + " - " + sql_statement + " no rows." );
					}
					int d_next_o_id  = resultset.getInt("D_NEXT_O_ID");
					if (logger.isDebugEnabled()) logger.debug(stream_number + ">  D_NEXT_O_ID:" + d_next_o_id);
					resultset.close();
	
					trans_query_num++;
					if (logger.isDebugEnabled()) logger.debug(stream_number + ">  StockLevel - Q" + trans_query_num + " - Select Stock Count - stocklevel_select_stockcount_stmt");
					if ( option_prepareexec && ! option_specialexec ) {
						if ( stocklevel_select_stockcount_stmt == null ) {
							sql_statement = "select count(distinct(S_I_ID)) as CNTDIST"
									+ " from " + orderline_table_name + " , " + stock_table_name
									+ " where OL_W_ID = ? and OL_D_ID = ?"
									+ " and OL_O_ID >= (? - 20)"
									+ " and OL_O_ID < ?"
									+ " and S_W_ID = ? and S_I_ID = OL_I_ID"
									+ " and S_QUANTITY < ?";
							stocklevel_select_stockcount_stmt = database_connector.prepare_statement( sql_statement );
						}
						stocklevel_select_stockcount_stmt.setInt(1,w_id);
						stocklevel_select_stockcount_stmt.setInt(2,d_id);
						stocklevel_select_stockcount_stmt.setInt(3,d_next_o_id);
						stocklevel_select_stockcount_stmt.setInt(4,d_next_o_id);
						stocklevel_select_stockcount_stmt.setInt(5,w_id);
						stocklevel_select_stockcount_stmt.setInt(6,threshold);
						resultset = stocklevel_select_stockcount_stmt.executeQuery();
	
					} else {
	
						sql_statement = "select count(distinct(S_I_ID)) as CNTDIST"
										+ " from " + orderline_table_name + " , " + stock_table_name
										+ " where OL_W_ID = " + w_id + " and OL_D_ID = " + d_id
										+ " and OL_O_ID < " + d_next_o_id
										+ " and OL_O_ID >= (" + d_next_o_id + " - 20)"
										+ " and S_W_ID =  " + w_id + " and S_I_ID = OL_I_ID"
										+ " and S_QUANTITY < " + threshold;
						resultset = database_connector.execute_query( sql_statement );
					}
	
	//				trans_query_num++;
	//				if ( option_prepareexec ) {
	//				if ( xxx == null ) {
	//	                if (logger.isDebugEnabled()) logger.debug("  stocklevel_stmt");
	//	stocklevel_stmt = prepare_statement(
	//			"select count(distinct(S.S_I_ID)) as CNTDIST"
	//					+ " from " + district_table_name + " D, "
	//					+ orderline_table_name + " L, "
	//					+ stock_table_name + " S"
	//					+ " where D.D_W_ID = ? and D.D_ID = ?"
	//					+ " and L.OL_W_ID = D.D_W_ID and L.OL_D_ID = D.D_ID"
	//					+ " and L.OL_O_ID >= (D.D_NEXT_O_ID - 20)"
	//					+ " and L.OL_O_ID < D.D_NEXT_O_ID"
	//					+ " and S.S_W_ID = L.OL_W_ID and S.S_I_ID = L.OL_I_ID"
	//					+ " and S.S_QUANTITY < ?"
	//			);
	//}
	//
	//					stocklevel_stmt.setInt(1,w_id);
	//					stocklevel_stmt.setInt(2,d_id);
	//					stocklevel_stmt.setInt(3,threshold);
	//					resultset = stocklevel_stmt.executeQuery();
	//
	//				} else if ( option_directexec ) {  // option directexec
	//					resultset = execute_query( dbstatement,
	//						"select count(distinct(S.S_I_ID)) as CNTDIST"
	//							+ " from " + district_table_name + " D, "
	//							+ orderline_table_name + " L, "
	//							+ stock_table_name + " S"
	//							+ " where D.D_W_ID = " + w_id + " and D.D_ID = " + d_id
	//							+ " and L.OL_W_ID = D.D_W_ID and L.OL_D_ID = D.D_ID"
	//							+ " and L.OL_O_ID >= (D.D_NEXT_O_ID - 20)"
	//							+ " and L.OL_O_ID < D.D_NEXT_O_ID"
	//							+ " and S.S_W_ID = L.OL_W_ID and S.S_I_ID = L.OL_I_ID"
	//							+ " and S.S_QUANTITY < " + threshold
	//						);
	
					if (! resultset.next() ) {       // Expecting exactly 1 row
						if (logger.isDebugEnabled()) logger.debug(stream_number + ">  StockLevel - Q" + trans_query_num + " - " + sql_statement + " no rows." );
					}
					int cnt_distinct_stock  = resultset.getInt("CNTDIST");
					if (logger.isDebugEnabled()) logger.debug(stream_number + ">  CNTDIST:" + cnt_distinct_stock);
					resultset.close();
	
					// Commit Transaction
					trans_query_num++;
					if ( !option_autocommit ) {
						// commit transaction
						try {
							database_connector.commit_work();
						} catch (SQLException exception) {
							if ( ( database.equals("trafodion") ) && ( exception.getErrorCode() == -8606) ) {
								// transaction failed on commit ... ok to retry
								transaction_retry( trans_type );
								
								database_connector.rollback_work();
								retry_count++;
								retry_transaction = true;
								logger.info(stream_number + ">  Transaction retry.  Transaction : " + accumlated_transactions + "  Retry : " + retry_count + " [" + dateformat.format(new Date()) + "]" );
								Thread.sleep( 10 * retry_count );  // Sleep
							} else {
								throw exception;
							}
						}
					}
				}
			}

			if ( option_sessionless ) {
				database_connector.closeDatabaseConnection();
//				stocklevel_stmt = null;
				stocklevel_select_district_stmt = null;
				stocklevel_select_stockcount_stmt = null;
				call_stmt = null;
			}

			stop_timer(trans_type);
			
			if ( retry_count >= MAXIMUM_TRANSACTION_RETRY ) {
				
				transaction_error( trans_type );
				String tombstone = "";
				tombstone = tombstone + "\n" + stream_number + ">----------------------------------------------------------";
				tombstone = tombstone + "\n" + stream_number + "> ERROR, Transaction failed to commit after " + retry_count + " retries.  Giving up on the transaction.";
				tombstone = tombstone + "\n" + stream_number + ">   Stream Number : " + stream_number + " , Transaction Number : " + accumlated_transactions;
				tombstone = tombstone + "\n" + stream_number + ">   " + transaction_data_string;
				tombstone = tombstone + "\n" + stream_number + ">----------------------------------------------------------";
				logger.error(tombstone);
				end_transaction();
			} else {
				transaction_success ( trans_type );
				end_transaction();
			}

		} catch (SQLException exception) {
			
			transaction_error( trans_type );
			String tombstone = "";
			tombstone = tombstone + "\n" + stream_number + ">----------------------------------------------------------";
			tombstone = tombstone + "\n" + stream_number + "> Transaction " + TRANS_TYPE_NAMES[ trans_type ] + ", Query " + trans_query_num;
			tombstone = tombstone + "\n" + stream_number + "> Unexpected SQLEXCEPTION,  Error Code = " + exception.getErrorCode();
			tombstone = tombstone + "\n" + stream_number + ">   Stream Number : " + stream_number + " , Transaction Number : " + accumlated_transactions;
			tombstone = tombstone + "\n" + stream_number + ">   " + transaction_data_string;
			SQLException nextException = exception;
			do {
				tombstone = tombstone + "\n" + stream_number + "> " + nextException.getMessage();
			} while ((nextException = nextException.getNextException()) != null);
			tombstone = tombstone + "\n" + stream_number + ">----------------------------------------------------------";
			logger.error(tombstone, exception);
			System.out.println(tombstone);
			if ( !option_autocommit ) { database_connector.rollback_work(); }
			end_transaction();
			throw exception;
		}
	}

	//----------------------------------------------------------------------------------------------------------

	public void start() throws Exception {
		if (logger.isDebugEnabled()) logger.debug(stream_number + "> main()");

		// Initialize Random Number Generator
		random_generator.stream_number = stream_number;
		random_generator.option_controlrandom = option_controlrandom;
		random_generator.start();

		// Initialize table names
		customer_table_name = "oe_customer_" + scale_factor;
		if ( schema != null ) {
			customer_table_name = schema + "." + customer_table_name;
		}

		district_table_name = "oe_district_" + scale_factor;
		if ( schema != null ) {
			district_table_name = schema + "." + district_table_name;
		}

		history_table_name = "oe_history_" + scale_factor;
		if ( schema != null ) {
			history_table_name = schema + "." + history_table_name;
		}

		item_table_name = "oe_item_" + scale_factor;
		if ( schema != null ) {
			item_table_name = schema + "." + item_table_name;
		}

		neworder_table_name = "oe_neworder_" + scale_factor;
		if ( schema != null ) {
			neworder_table_name = schema + "." + neworder_table_name;
		}

		orderline_table_name = "oe_orderline_" + scale_factor;
		if ( schema != null ) {
			orderline_table_name = schema + "." + orderline_table_name;
		}

		orders_table_name = "oe_orders_" + scale_factor;
		if ( schema != null ) {
			orders_table_name = schema + "." + orders_table_name;
		}

		stock_table_name = "oe_stock_" + scale_factor;
		if ( schema != null ) {
			stock_table_name = schema + "." + stock_table_name;
		}

		warehouse_table_name = "oe_warehouse_" + scale_factor;
		if ( schema != null ) {
			warehouse_table_name = schema + "." + warehouse_table_name;
		}

		// Initialize Database Connector
		database_connector.stream_number = stream_number;
		database_connector.database = database;
		database_connector.option_autocommit = option_autocommit;

		if ( ! option_sessionless ) { //  If not sessionless, connect to database
			if (logger.isDebugEnabled()) logger.debug(stream_number + "  !option_sessionless");
			database_connector.establishDatabaseConnection();
		}

		if (logger.isDebugEnabled()) logger.debug(stream_number + "< main()");

	}

	//----------------------------------------------------------------------------------------------------------

	public void close() throws Exception {

		if ( ! option_sessionless ) { //  If not sessionless, close database connection
			database_connector.closeDatabaseConnection();

			neworder_select_warehouse_stmt = null;
			neworder_select_customer_stmt = null;
			neworder_select_district_stmt = null;
			neworder_update_district_stmt = null;
			neworder_insert_order_stmt = null;
			neworder_insert_neworder_stmt = null;
			neworder_select_item_stmt = null;
			neworder_select_stock_stmt = null;
			neworder_update_stock_stmt = null;
			neworder_insert_orderline_stmt = null;

			payment_select_warehouse_stmt = null;
			payment_select_customerid_stmt = null;
			payment_select_district_stmt = null;
			payment_select_customer_stmt = null;
			payment_update_customer_bc_stmt = null;
			payment_update_customer_stmt = null;
			payment_update_warehouse_stmt = null;
			payment_update_district_stmt = null;
			payment_insert_history_stmt = null;

			orderstatus_select_customerid_stmt = null;
			orderstatus_select_customer_stmt = null;
			orderstatus_select_orderid_stmt = null;
			orderstatus_select_order_stmt = null;
			orderstatus_select_orderline_stmt = null;

			delivery_select_neworder_stmt = null;
			delivery_delete_neworder_stmt = null;
			delivery_select_order_stmt = null;
			delivery_update_order_stmt = null;
			delivery_update_orderline_stmt = null;
			delivery_select_orderline_stmt = null;
			delivery_update_customer_stmt = null;

//			stocklevel_stmt = null;
			stocklevel_select_district_stmt = null;
			stocklevel_select_stockcount_stmt = null;
			
			call_stmt = null;
		}
	}

	//----------------------------------------------------------------------------------------------------------
}
