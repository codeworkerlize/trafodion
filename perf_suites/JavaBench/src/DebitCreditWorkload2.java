import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;

public class DebitCreditWorkload2 {

	final int INVALID_SCALEFACTOR = -1;
	final int INVALID_STREAMNUMBER = -1;

	final int MAXIMUM_TRANSACTION_RETRY = 10;

	final int ACCOUNT_ROWS_PER_SCALE = 100000;
//	final int BRANCH_ROWS_PER_SCALE = 1;
	final int TELLER_ROWS_PER_SCALE = 10;

	final SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//	final SimpleDateFormat hourformat = new SimpleDateFormat("HH:mm:ss");

	public int stream_number = INVALID_STREAMNUMBER;
	public int scale_factor = INVALID_SCALEFACTOR;

	public String schema = null;
	public String database = null;

	public boolean option_autocommit = false;
	public boolean option_sessionless = false;
	public boolean option_prepareexec = false;
	public boolean option_directexec = false;
	public boolean option_spj = false;
	public boolean option_batch = false;
	public boolean option_controlrandom = false;
	public boolean option_newkey = false;
	public boolean option_isolate = false;
	public boolean option_scaleperstream = false;
	public int scale_per_stream = 1;

	RandomGenerator random_generator = new RandomGenerator();
	
	DatabaseConnector database_connector = new DatabaseConnector();
	String sql_statement = null;

	PreparedStatement upd_acct_stmt = null;
	PreparedStatement upd_tell_stmt = null;
	PreparedStatement upd_brch_stmt = null;
	PreparedStatement ins_hist_stmt = null;
	PreparedStatement sel_acct_stmt = null;

	CallableStatement call_stmt = null;

	String branch_table_name = null;
	String teller_table_name = null;
	String account_table_name = null;
	String history_table_name = null;
	
	int prior_key = -1;
	
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

	public final int TOTAL_TRANS_TYPES = 1;
	public final String TRANS_TYPE_NAMES[] = { "debitcredit" };
	public final int DEBITCREDIT_TRANS_TYPE = 0;  // Fixed value
	
	public long[] accumlated_transactions_by_type = { 0 };
	public long[] accumlated_errors_by_type = { 0 };
	public long[] accumlated_retries_by_type = { 0 };
	public long[] accumlated_successes_by_type = { 0 };

	public long[] accumlated_transaction_response_micro_by_type = { 0 };
	public long[] maximum_transaction_response_micro_by_type = { 0 };
	public long[] minimum_transaction_response_micro_by_type = { 999999999999999999L };

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

	public void runDebitCredit() throws Exception {
		
		int trans_type = DEBITCREDIT_TRANS_TYPE;
		start_transaction(trans_type);
		
		if (logger.isDebugEnabled()) logger.debug(stream_number + ">> accumlated_transactions " + String.format("%5d", accumlated_transactions ) );

		//  Generate Test Variables.
		int b_id = -1;
		if ( option_scaleperstream ) {
			b_id = stream_number;
		} else {
			if ( option_newkey ) {
				if ( scale_per_stream == 1 ) {
					throw new Exception("ERROR : option_newkey incompatible with scale_per_stream of 1.");
				}
				do {
					b_id = stream_number * scale_per_stream + random_generator.generateRandomInteger( 0 , ( scale_per_stream - 1 ));
				} while ( b_id == prior_key );
				prior_key = b_id;
			} else {
				b_id = stream_number * scale_per_stream + random_generator.generateRandomInteger( 0 , ( scale_per_stream - 1 ));
			}
		}

		int t_id = b_id * TELLER_ROWS_PER_SCALE + random_generator.generateRandomInteger( 0 , ( TELLER_ROWS_PER_SCALE - 1 ) );
		int acct_branch = b_id;
		boolean remote_acct_branch = false;
		if ( ( random_generator.generateRandomInteger ( 0, 100) >= 85) && (scale_factor > 1) && ! option_isolate ) {
			remote_acct_branch = true;
			while ( acct_branch == b_id ) {
				acct_branch = random_generator.generateRandomInteger ( 0, ( scale_factor - 1) );
			}
		}

		int a_id = ( acct_branch * ACCOUNT_ROWS_PER_SCALE ) + random_generator.generateRandomInteger ( 0, ( ACCOUNT_ROWS_PER_SCALE - 1 ) );

		long amt = random_generator.generateRandomInteger( -999, 999 );
		
		// BigDecimal amt = new BigDecimal( random_generator.generateRandomInteger( -999, 999 ) / 100.0 ).setScale(2,BigDecimal.ROUND_DOWN);

		String filler = random_generator.generateRandomAString( 22 );  // History Filler

		long a_balance = 0;
		
		// BigDecimal a_balance = new BigDecimal( 0.00 ).setScale(2,BigDecimal.ROUND_DOWN);

		String transaction_data_string = "DEBITCREDIT"
				+ " , b_id = " + b_id
				+ " , t_id = " + t_id
				+ " , a_id = " + a_id
				+ " , amt = " + amt
				+ " , remote_acct_branch = " + remote_acct_branch
				+ " , filler = " + filler;

		if (logger.isDebugEnabled()) logger.debug(stream_number + ">  DebitCredit Transaction");
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
	
				if ( option_spj ) {
	
					if ( call_stmt == null ) {
						call_stmt = database_connector.prepare_call_statement("  {call " + schema + ".debitcredit(?,?,?,?,?,?)}");
					}
	
					call_stmt.setInt(1,t_id);
					call_stmt.setInt(2,b_id);
					call_stmt.setInt(3,a_id);
					call_stmt.setLong(4,amt);
					call_stmt.setString(5,filler);
					call_stmt.registerOutParameter(6, java.sql.Types.BIGINT);
	
					call_stmt.execute();  // Execute Statement
					a_balance = call_stmt.getLong(6);
//					a_balance = call_stmt.getBigDecimal(6);
					if (logger.isDebugEnabled()) logger.debug(stream_number + ">  a_balance = " + a_balance);
	
				} else {
					
					// Update Account
					if (logger.isDebugEnabled()) logger.debug(stream_number + ">  Update Account " + a_id + " with amount " + amt );
					if ( option_prepareexec ) {
						
						if ( upd_acct_stmt == null ) {
							upd_acct_stmt = database_connector.prepare_statement("update "	+ account_table_name
//									+ " set ACCOUNT_BALANCE = ACCOUNT_BALANCE + ? where ACCOUNT_ID = ? and BRANCH_ID = ?");
									+ " set ACCOUNT_BALANCE = ACCOUNT_BALANCE + ? where ACCOUNT_ID = ?");
						}
						upd_acct_stmt.setLong(1,amt);
						upd_acct_stmt.setInt(2,a_id);
//						upd_acct_stmt.setInt(3,acct_branch);
						database_connector.execute_prepared_update_statement(upd_acct_stmt);
						
					} else if ( option_directexec ) {
						
						sql_statement = "update " + account_table_name
								+ " set ACCOUNT_BALANCE = ACCOUNT_BALANCE + " + amt
								+ " where ACCOUNT_ID = " + a_id;
	
						if ( option_batch ) {
							database_connector.add_batch(sql_statement);
						} else {
							database_connector.execute_statement(sql_statement);
						}
			
					}
	
					// Update Teller
					if (logger.isDebugEnabled()) logger.debug(stream_number + ">  Update Teller " + t_id + " with amount " + amt );
					if ( option_prepareexec ) {
						
						if ( upd_tell_stmt == null ) {
							upd_tell_stmt = database_connector.prepare_statement("update " + teller_table_name
//									+ " set TELLER_BALANCE = TELLER_BALANCE + ? where TELLER_ID = ? and BRANCH_ID = ?");
									+ " set TELLER_BALANCE = TELLER_BALANCE + ? where TELLER_ID = ?");
						}
	
						upd_tell_stmt.setLong(1,amt);
						upd_tell_stmt.setInt(2,t_id);
//						upd_tell_stmt.setInt(3,b_id);
						database_connector.execute_prepared_update_statement(upd_tell_stmt);
						
					} else if ( option_directexec ) {

						sql_statement = "update " + teller_table_name
								+ " set TELLER_BALANCE = TELLER_BALANCE + " + amt
								+ " where TELLER_ID = " + t_id;
						
						if ( option_batch ) {
							database_connector.add_batch(sql_statement);
						} else {
							database_connector.execute_statement(sql_statement);
						}
						
					}
	
					// Update Branch
					if (logger.isDebugEnabled()) logger.debug(stream_number + ">  Update Branch " + b_id + " with amount " + amt );
					if ( option_prepareexec ) {
						
						if ( upd_brch_stmt == null ) {
							upd_brch_stmt = database_connector.prepare_statement("update " + branch_table_name
									+ " set BRANCH_BALANCE = BRANCH_BALANCE + ? where BRANCH_ID = ?");
						}
	
						upd_brch_stmt.setLong(1,amt);
						upd_brch_stmt.setInt(2,b_id);
						database_connector.execute_prepared_update_statement(upd_brch_stmt);
						
					} else if ( option_directexec ) {
						
						sql_statement = "update "
								+ branch_table_name
								+ " set BRANCH_BALANCE = BRANCH_BALANCE + " + amt
								+ " where BRANCH_ID = " + b_id;
	
						if ( option_batch ) {
							database_connector.add_batch(sql_statement);
						} else {
							database_connector.execute_statement(sql_statement);
						}
						
					}
	
					// Insert History
					if (logger.isDebugEnabled()) logger.debug(stream_number + ">  Insert History " );
					if ( option_prepareexec ) {
	
						if ( ins_hist_stmt == null ) {
							ins_hist_stmt = database_connector.prepare_statement("insert into " + history_table_name
									+ " ( BRANCH_ID,  TELLER_ID, ACCOUNT_ID, AMOUNT, TRANSACTION_TIME, HISTORY_FILLER ) values ( ?, ?, ?, ?, CURRENT_TIMESTAMP, ? )");
						}
	
						ins_hist_stmt.setInt(1,b_id);
						ins_hist_stmt.setInt(2,t_id);
						ins_hist_stmt.setInt(3,a_id);
						ins_hist_stmt.setLong(4,amt);
						ins_hist_stmt.setString(5,filler);
						database_connector.execute_prepared_update_statement(ins_hist_stmt);
	
					} else if ( option_directexec ) {
						
						sql_statement = "insert into "
								+ history_table_name
								+ " ( TRANSACTION_TIME, TELLER_ID, BRANCH_ID, ACCOUNT_ID, AMOUNT, HISTORY_FILLER )"
								+ " values ( CURRENT_TIMESTAMP "
								+ ", " + t_id
								+ ", " + b_id
								+ ", " + a_id
								+ ", " + amt
								+ ", '" + filler + "' )";
	
						if ( option_batch ) {
							database_connector.add_batch(sql_statement);
						} else {
							database_connector.execute_statement(sql_statement);
						}

					}

					if ( option_batch ) {  // if batch, it is time to do the work
						database_connector.execute_batch();
					}

					// Select Account Balance
					if (logger.isDebugEnabled()) logger.debug(stream_number + ">  Select New Account Balance " );
					ResultSet resultset = null;
					if ( option_prepareexec ) {
						
						if ( sel_acct_stmt == null ) {
							sel_acct_stmt = database_connector.prepare_statement("select ACCOUNT_BALANCE from " + account_table_name
									+ " where ACCOUNT_ID = ?");
//									+ " where ACCOUNT_ID = ? and BRANCH_ID = ?");
						}
	
						sel_acct_stmt.setInt(1,a_id);
//						sel_acct_stmt.setInt(2,acct_branch);
						resultset = database_connector.execute_prepared_query(sel_acct_stmt);
							
					} else if ( option_directexec ) {
						
						resultset = database_connector.execute_query("select ACCOUNT_BALANCE from " + account_table_name
								+ " where ACCOUNT_ID = " + a_id);
							
					}
					resultset.next();  // Expecting exactly 1 row
					a_balance = resultset.getLong("ACCOUNT_BALANCE");
//					a_balance = resultset.getBigDecimal("ACCOUNT_BALANCE");
					if (logger.isDebugEnabled()) logger.debug(stream_number + ">	ACCOUNT_BALANCE: " + a_balance );
					resultset.close();

					// Commit Transaction
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
				upd_acct_stmt = null;
				upd_tell_stmt = null;
				upd_brch_stmt = null;
				ins_hist_stmt = null;
				sel_acct_stmt = null;
				call_stmt = null;
			}
	
			stop_timer(trans_type);
			
			if ( retry_count >= MAXIMUM_TRANSACTION_RETRY ) {
				transaction_error( trans_type );
				String tombstone = "";
				tombstone = tombstone + "\n" + ">----------------------------------------------------------";
				tombstone = tombstone + "\n" + "> ERROR, Transaction failed to commit after " + retry_count + " retries.  Giving up on the transaction.";
				tombstone = tombstone + "\n" + ">   Stream Number : " + stream_number + " , Transaction Number : " + accumlated_transactions;
				tombstone = tombstone + "\n" + ">   " + transaction_data_string;
				tombstone = tombstone + "\n" + ">----------------------------------------------------------";
				logger.error(tombstone);
				end_transaction();
			} else {
				transaction_success( trans_type);
				end_transaction();
			}
			
		} catch (SQLException exception) {
			transaction_error( trans_type );
			String tombstone = "";
			tombstone = tombstone + "\n" + ">----------------------------------------------------------";
			tombstone = tombstone + "\n" + "> Unexpected SQLEXCEPTION,  Error Code = " + exception.getErrorCode();
			tombstone = tombstone + "\n" + ">   Stream Number : " + stream_number + " , Transaction Number : " + accumlated_transactions;
			tombstone = tombstone + "\n" + ">   " + transaction_data_string;
			SQLException nextException = exception;
			do {
				tombstone = tombstone + "\n" + "> " + nextException.getMessage();
			} while ((nextException = nextException.getNextException()) != null);
			tombstone = tombstone + "\n" + ">----------------------------------------------------------";
			logger.error(tombstone, exception);
			if ( !option_autocommit ) { database_connector.rollback_work(); }
			end_transaction();
			throw exception;
		}
	}

	//----------------------------------------------------------------------------------------------------------

	public void start() throws Exception {
		if (logger.isDebugEnabled()) logger.debug(stream_number + "> DebitCreditWorkload2.start()");

		// Initialize Random Number Generator
		random_generator.stream_number = stream_number;
		random_generator.option_controlrandom = option_controlrandom;
		random_generator.start();
		
		// Initialize table names
		branch_table_name = "dc_branch_" + scale_factor;
		if ( schema != null ) {
			branch_table_name = schema + "." + branch_table_name;
		}

		teller_table_name = "dc_teller_" + scale_factor;
		if ( schema != null ) {
			teller_table_name = schema + "." + teller_table_name;
		}

		account_table_name = "dc_account_" + scale_factor;
		if ( schema != null ) {
			account_table_name = schema + "." + account_table_name;
		}

		history_table_name = "dc_history_" + scale_factor;
		if ( schema != null ) {
			history_table_name = schema + "." + history_table_name;
		}

		// Initialize Database Connector
		database_connector.stream_number = stream_number;
		database_connector.database = database;
		database_connector.option_autocommit = option_autocommit;
		
		if ( ! option_sessionless ) { //  If not sessionless, connect to database
			if (logger.isDebugEnabled()) logger.debug(stream_number + "  !option_sessionless");
			database_connector.establishDatabaseConnection();
		}

		if (logger.isDebugEnabled()) logger.debug(stream_number + "< DebitCreditWorkload2.start()");
		
	}
	
	
	//----------------------------------------------------------------------------------------------------------

	public void close() throws Exception {
		
		if (logger.isDebugEnabled()) logger.debug(stream_number + "> DebitCreditWorkload2.close()");

		if ( ! option_sessionless ) { //  If not sessionless, close database connection
			database_connector.closeDatabaseConnection();
			upd_acct_stmt = null;
			upd_tell_stmt = null;
			upd_brch_stmt = null;
			ins_hist_stmt = null;
			sel_acct_stmt = null;
			call_stmt = null;
		}
		
		if (logger.isDebugEnabled()) logger.debug(stream_number + "< DebitCreditWorkload2.close()");
	}
	
	//----------------------------------------------------------------------------------------------------------

}
