import java.io.File;
import java.io.FileInputStream;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.yahoo.ycsb.Utils;
import com.yahoo.ycsb.generator.ScrambledZipfianGenerator;
import com.yahoo.ycsb.generator.ZipfianGenerator;
import com.yahoo.ycsb.workloads.CoreWorkload;

public class YCSBWorkload2 {

	final int INVALID_SCALEFACTOR = -1;
	final int INVALID_STREAMNUMBER = -1;

	final int ROWS_PER_SCALE = 1000000;

	final SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public int stream_number = INVALID_STREAMNUMBER;
	public int scale_factor = INVALID_SCALEFACTOR;

	public String schema = null;
	public String database = null;

	boolean option_autocommit = true; // only runs autocommit
//	boolean option_sessionless = false;  // no concept of sessionless
	public boolean option_prepareexec = false;
	public boolean option_directexec = false;
//	boolean option_spj = false;  // no concept of spj
	public boolean option_controlrandom = false;
//	public boolean option_newkey = false;
//	public boolean option_isolate = false;
//	public boolean option_scaleperstream = false;
//	public int scale_per_stream = 1;
	public String option_distribution = "zipfian";
	public String option_update_syntax = "upsert";

	RandomGenerator random_generator = new RandomGenerator();
	ZipfianGenerator zipfian_generator = null;
	ScrambledZipfianGenerator scrambled_zipfian_generator = null;

	DatabaseConnector database_connector = new DatabaseConnector();
	String sql_statement = null;

	CoreWorkload ycsbworkload = new CoreWorkload();  // We're going to use the YCSB routines to generate the data

	PreparedStatement singletonselect_stmt = null;
	PreparedStatement singletonupdate_stmt = null;

	CallableStatement call_stmt = null;

	String ycsb_table_name = null;
	
	// hbase specific variables.
	Configuration configuration = null;
	String column_family = null;
    HTable hbase_table =null;
 
//	int prior_key = -1;
	
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

	public final int TOTAL_TRANS_TYPES = 2;
	public final String TRANS_TYPE_NAMES[] = { "singletonselect" , "singletonupdate" };
	public final int SINGLETONSELECT_TRANS_TYPE = 0;  // Fixed value
	public final int SINGLETONUPDATE_TRANS_TYPE = 1;  // Fixed value
	
	public long[] accumlated_transactions_by_type = { 0, 0 };
	public long[] accumlated_errors_by_type = { 0, 0 };
	public long[] accumlated_retries_by_type = { 0, 0 };
	public long[] accumlated_successes_by_type = { 0, 0 };

	public long[] accumlated_transaction_response_micro_by_type = { 0, 0 };
	public long[] maximum_transaction_response_micro_by_type = { 0, 0 };
	public long[] minimum_transaction_response_micro_by_type = { 999999999999999999L, 999999999999999999L };

	long start_transaction_time = 0;
	
	int trans_type = 0;
	int row_id = 0;
	String key_value = null;
	String transaction_data_string = null;
    // HBase Runtime
	Result hresult = null;
    Get hget = null;
    Put hput = null;
    String tombstone = null;
    // JDBC Runtime
	ResultSet resultset = null;
	String select_result = null;
    
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

	public void runSingletonSelect() throws Exception {
		
		trans_type = SINGLETONSELECT_TRANS_TYPE;
		start_transaction(trans_type);
		
		if (logger.isDebugEnabled()) logger.debug(stream_number + ">> accumlated_transactions " + String.format("%5d", accumlated_transactions ) );

		//  Generate Test Variables.
		
		if ( option_distribution.equals("zipfian") ) {
			row_id = zipfian_generator.nextInt();
		} else if ( option_distribution.equals("scrambledzipfian") ) {
			row_id = scrambled_zipfian_generator.nextInt();
		} else {
			// Default Distribution (Uniform using java.util.random)
			row_id = random_generator.generateRandomInteger(0,scale_factor*ROWS_PER_SCALE-1);
		}
	
		key_value = ycsbworkload.buildKeyName(row_id);

		transaction_data_string = "SingletonSelect , scale = " + scale_factor + " , key_value = " + key_value;

		if (logger.isDebugEnabled()) logger.debug(stream_number + ">  YCSB SingletonSelect" + " : " + transaction_data_string);

		if ( database.equals("hbase") ) {

			//  HBase Specific
		
	        if (logger.isDebugEnabled()) logger.debug(stream_number + ">  Doing read from HBase columnfamily " + column_family + ", key: " + key_value);

	        try {

				start_timer();

				hget = new Get(Bytes.toBytes(key_value));

				hget.addFamily(Bytes.toBytes(column_family));

				start_timer();

	            hresult = hbase_table.get(hget);

	            select_result = "( "
	            		+ "field0 = " + Bytes.toString(hresult.getValue(Bytes.toBytes(column_family), Bytes.toBytes("FIELD0"))) + " , "
						+ "field1 = " + Bytes.toString(hresult.getValue(Bytes.toBytes(column_family), Bytes.toBytes("FIELD1"))) + " , "
						+ "field2 = " + Bytes.toString(hresult.getValue(Bytes.toBytes(column_family), Bytes.toBytes("FIELD2"))) + " , "
						+ "field3 = " + Bytes.toString(hresult.getValue(Bytes.toBytes(column_family), Bytes.toBytes("FIELD3"))) + " , "
						+ "field4 = " + Bytes.toString(hresult.getValue(Bytes.toBytes(column_family), Bytes.toBytes("FIELD4"))) + " , "
						+ "field5 = " + Bytes.toString(hresult.getValue(Bytes.toBytes(column_family), Bytes.toBytes("FIELD5"))) + " , "
						+ "field6 = " + Bytes.toString(hresult.getValue(Bytes.toBytes(column_family), Bytes.toBytes("FIELD6"))) + " , "
						+ "field7 = " + Bytes.toString(hresult.getValue(Bytes.toBytes(column_family), Bytes.toBytes("FIELD7"))) + " , "
						+ "field8 = " + Bytes.toString(hresult.getValue(Bytes.toBytes(column_family), Bytes.toBytes("FIELD8"))) + " , "
						+ "field9 = " + Bytes.toString(hresult.getValue(Bytes.toBytes(column_family), Bytes.toBytes("FIELD9")))
						+ " )";

//	            select_result = "( ";
//	            for (KeyValue kv : hresult.raw()) {
//	            	select_result = select_result + Bytes.toString(kv.getQualifier()) + " = " + Bytes.toString(kv.getValue()) + " , ";
//		        }
//	            select_result = select_result + " )";

				stop_timer(trans_type);

				if (logger.isDebugEnabled()) logger.debug(stream_number + ">  Select Result : " + select_result ); 

				transaction_success( trans_type);
				end_transaction();

	        } catch ( Exception exception ) {
				transaction_error( trans_type );
				tombstone = 
						  "\n" + ">----------------------------------------------------------"
						+ "\n" + "> Unexpected Exception doing Hbase get:  " + exception
						+ "\n" + ">   Stream Number : " + stream_number + " , Transaction Number : " + accumlated_transactions
						+ "\n" + ">   " + transaction_data_string
						+ "\n" + ">----------------------------------------------------------"
						+ "\n"
						;
				logger.error(tombstone, exception);
				end_transaction();
				throw new Exception("ERROR : Unexpected Hbase Error.");
	        }

		} else { // jdbc compliant database

			try {
				
				start_timer();

				if ( option_prepareexec ) {
					
					if ( singletonselect_stmt == null ) {
						singletonselect_stmt = database_connector.prepare_statement("select * from " 
								+ ycsb_table_name + " where PRIM_KEY = ?");
					}
					singletonselect_stmt.setString(1, key_value);
					resultset = database_connector.execute_prepared_query(singletonselect_stmt);
					
				} else if ( option_directexec ) {
					
					sql_statement = "select * from " + ycsb_table_name + " where PRIM_KEY = '" + key_value + "'";

					resultset = database_connector.execute_query(sql_statement);
		
				}

				resultset.next();  // Expecting exactly 1 row

	            select_result = "( "
	            		+ "field0 = " + resultset.getString(2) + " , "
						+ "field1 = " + resultset.getString(3) + " , "
						+ "field2 = " + resultset.getString(4) + " , "
						+ "field3 = " + resultset.getString(5) + " , "
						+ "field4 = " + resultset.getString(6) + " , "
						+ "field5 = " + resultset.getString(7) + " , "
						+ "field6 = " + resultset.getString(8) + " , "
						+ "field7 = " + resultset.getString(9) + " , "
						+ "field8 = " + resultset.getString(10) + " , "
						+ "field9 = " + resultset.getString(11)
						+ " )";
	            
	            resultset.close();

				stop_timer(trans_type);

				if (logger.isDebugEnabled()) logger.debug(stream_number + ">  Select Result : " + select_result ); 
		
				transaction_success( trans_type);
				end_transaction();

			} catch (SQLException exception) {
				transaction_error( trans_type );
				tombstone = 
						  "\n" + ">----------------------------------------------------------"
						+ "\n" + "> Unexpected SQLEXCEPTION,  Error Code = " + exception.getErrorCode()
						+ "\n" + ">   Stream Number : " + stream_number + " , Transaction Number : " + accumlated_transactions
						+ "\n" + ">   " + transaction_data_string;
				SQLException nextException = exception;
				do {
					tombstone = tombstone + "\n" + "> " + nextException.getMessage();
				} while ((nextException = nextException.getNextException()) != null);
				tombstone = tombstone + "\n" + ">----------------------------------------------------------" + "\n";
				logger.error(tombstone, exception);
				end_transaction();
				throw exception;
			}

		}
		
	}

	public void runSingletonUpdate() throws Exception {
		
		trans_type = SINGLETONUPDATE_TRANS_TYPE;
		start_transaction(trans_type);
		
		if (logger.isDebugEnabled()) logger.debug(stream_number + ">> accumlated_transactions " + String.format("%5d", accumlated_transactions ) );

		//  Generate Test Variables.

		if ( option_distribution.equals("zipfian") ) {
			row_id = zipfian_generator.nextInt();
		} else if ( option_distribution.equals("scrambledzipfian") ) {
			row_id = scrambled_zipfian_generator.nextInt();
		} else {
			// Default Distribution (Uniform using java.util.random)
			row_id = random_generator.generateRandomInteger(0,scale_factor*ROWS_PER_SCALE-1);
		}
	
		key_value = ycsbworkload.buildKeyName(row_id);

		transaction_data_string = "SingletonUpdate , scale = " + scale_factor + " , key_value = " + key_value;

		if (logger.isDebugEnabled()) logger.debug(stream_number + ">  YCSB SingletonUpdate");
		if (logger.isDebugEnabled()) logger.debug(stream_number + ">    " + transaction_data_string);

		if ( database.equals("hbase") ) {

	        if (logger.isDebugEnabled()) logger.debug(stream_number + ">Doing put to HBase columnfamily " + column_family + ", key: " + key_value);


	        try {

				start_timer();

		        hput = new Put(Bytes.toBytes(key_value));

		        hput.add(Bytes.toBytes(column_family),Bytes.toBytes("FIELD0"),Bytes.toBytes(Utils.ASCIIString(100)));
		        hput.add(Bytes.toBytes(column_family),Bytes.toBytes("FIELD1"),Bytes.toBytes(Utils.ASCIIString(100)));
		        hput.add(Bytes.toBytes(column_family),Bytes.toBytes("FIELD2"),Bytes.toBytes(Utils.ASCIIString(100)));
		        hput.add(Bytes.toBytes(column_family),Bytes.toBytes("FIELD3"),Bytes.toBytes(Utils.ASCIIString(100)));
		        hput.add(Bytes.toBytes(column_family),Bytes.toBytes("FIELD4"),Bytes.toBytes(Utils.ASCIIString(100)));
		        hput.add(Bytes.toBytes(column_family),Bytes.toBytes("FIELD5"),Bytes.toBytes(Utils.ASCIIString(100)));
		        hput.add(Bytes.toBytes(column_family),Bytes.toBytes("FIELD6"),Bytes.toBytes(Utils.ASCIIString(100)));
		        hput.add(Bytes.toBytes(column_family),Bytes.toBytes("FIELD7"),Bytes.toBytes(Utils.ASCIIString(100)));
		        hput.add(Bytes.toBytes(column_family),Bytes.toBytes("FIELD8"),Bytes.toBytes(Utils.ASCIIString(100)));
		        hput.add(Bytes.toBytes(column_family),Bytes.toBytes("FIELD9"),Bytes.toBytes(Utils.ASCIIString(100)));

		        hbase_table.put(hput);

				stop_timer(trans_type);

				transaction_success( trans_type);
				end_transaction();

	        } catch ( Exception exception ) {
				transaction_error( trans_type );
				tombstone = 
						  "\n" + ">----------------------------------------------------------"
						+ "\n" + "> Unexpected Exception doing Hbase put:  " + exception
						+ "\n" + ">   Stream Number : " + stream_number + " , Transaction Number : " + accumlated_transactions
						+ "\n" + ">   " + transaction_data_string
						+ "\n" + ">----------------------------------------------------------"+ "\n";
				logger.error(tombstone, exception);
				end_transaction();
				throw new Exception("ERROR : Unexpected Hbase Error.");
	        }

		} else { // jdbc compliant database

			try {
				
				start_timer();

				if ( option_prepareexec ) {
					
					if ( singletonupdate_stmt == null ) {
						
						if ( option_update_syntax.equals("upsert") ) {

							sql_statement = "upsert into " + ycsb_table_name
										+ " ( PRIM_KEY, FIELD0, FIELD1, FIELD2, FIELD3, FIELD4, FIELD5, FIELD6, FIELD7, FIELD8, FIELD9 )"
										+ " values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

						} else {

							sql_statement = "update " + ycsb_table_name
									+ " set ( FIELD0, FIELD1, FIELD2, FIELD3, FIELD4, FIELD5, FIELD6, FIELD7, FIELD8, FIELD9 )"
									+ " = ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ? ) where PRIM_KEY = ?";

						}
						
						singletonupdate_stmt = database_connector.prepare_statement(sql_statement);
					}
					
					if ( option_update_syntax.equals("upsert") ) {
						singletonupdate_stmt.setString( 11, Utils.ASCIIString(100) );
						singletonupdate_stmt.setString(  1, key_value );
					} else {
						singletonupdate_stmt.setString(  1, Utils.ASCIIString(100) );
						singletonupdate_stmt.setString( 11, key_value );
					}

					singletonupdate_stmt.setString(  2, Utils.ASCIIString(100) );
					singletonupdate_stmt.setString(  3, Utils.ASCIIString(100) );
					singletonupdate_stmt.setString(  4, Utils.ASCIIString(100) );
					singletonupdate_stmt.setString(  5, Utils.ASCIIString(100) );
					singletonupdate_stmt.setString(  6, Utils.ASCIIString(100) );
					singletonupdate_stmt.setString(  7, Utils.ASCIIString(100) );
					singletonupdate_stmt.setString(  8, Utils.ASCIIString(100) );
					singletonupdate_stmt.setString(  9, Utils.ASCIIString(100) );
					singletonupdate_stmt.setString( 10, Utils.ASCIIString(100) );

					database_connector.execute_prepared_update_statement(singletonupdate_stmt);
					
				} else if ( option_directexec ) {
					
					if ( option_update_syntax.equals("upsert") ) {

						sql_statement = "upsert into " + ycsb_table_name
								+ " ( PRIM_KEY, FIELD0, FIELD1, FIELD2, FIELD3, FIELD4, FIELD5, FIELD6, FIELD7, FIELD8, FIELD9 ) values ( "
								+ "'" + key_value + "',"
								+ "'" + Utils.ASCIIString(100) + "',"
								+ "'" + Utils.ASCIIString(100) + "',"
								+ "'" + Utils.ASCIIString(100) + "',"
								+ "'" + Utils.ASCIIString(100) + "',"
								+ "'" + Utils.ASCIIString(100) + "',"
								+ "'" + Utils.ASCIIString(100) + "',"
								+ "'" + Utils.ASCIIString(100) + "',"
								+ "'" + Utils.ASCIIString(100) + "',"
								+ "'" + Utils.ASCIIString(100) + "',"
								+ "'" + Utils.ASCIIString(100) + "'"
								+ " )" ;

					} else {

						sql_statement = "update " + ycsb_table_name
								+ " set ( FIELD0, FIELD1, FIELD2, FIELD3, FIELD4, FIELD5, FIELD6, FIELD7, FIELD8, FIELD9 ) = ( "
								+ "'" + Utils.ASCIIString(100) + "',"
								+ "'" + Utils.ASCIIString(100) + "',"
								+ "'" + Utils.ASCIIString(100) + "',"
								+ "'" + Utils.ASCIIString(100) + "',"
								+ "'" + Utils.ASCIIString(100) + "',"
								+ "'" + Utils.ASCIIString(100) + "',"
								+ "'" + Utils.ASCIIString(100) + "',"
								+ "'" + Utils.ASCIIString(100) + "',"
								+ "'" + Utils.ASCIIString(100) + "',"
								+ "'" + Utils.ASCIIString(100) + "'"
								+ " ) where PRIM_KEY = " + key_value;

					}

					database_connector.execute_statement(sql_statement);
		
				}

				stop_timer(trans_type);

				transaction_success( trans_type);
				end_transaction();

			} catch (SQLException exception) {
				stop_timer(trans_type);
				transaction_error( trans_type );
				tombstone = 
						  "\n" + ">----------------------------------------------------------"
						+ "\n" + "> Unexpected SQLEXCEPTION,  Error Code = " + exception.getErrorCode()
						+ "\n" + ">   Stream Number : " + stream_number + " , Transaction Number : " + accumlated_transactions
						+ "\n" + ">   " + transaction_data_string;
				SQLException nextException = exception;
				do {
					tombstone = tombstone + "\n" + "> " + nextException.getMessage();
				} while ((nextException = nextException.getNextException()) != null);
				tombstone = tombstone + "\n" + ">----------------------------------------------------------" + "\n";
				logger.error(tombstone, exception);
				end_transaction();
				throw exception;
			}

		}
	}

	//----------------------------------------------------------------------------------------------------------

	public void start() throws Exception {
		
		if (logger.isDebugEnabled()) logger.debug(stream_number + "> start()");

		// Initialize Random Number Generator
		random_generator.stream_number = stream_number;
		random_generator.option_controlrandom = option_controlrandom;
		random_generator.start();

		// Initialize Random Number Generators
		if ( option_distribution.equals("zipfian") ) {
			zipfian_generator = new ZipfianGenerator(0,scale_factor*ROWS_PER_SCALE-1);
		} else if ( option_distribution.equals("scrambledzipfian") ) {
			scrambled_zipfian_generator = new ScrambledZipfianGenerator(0,scale_factor*ROWS_PER_SCALE-1);
		}

		
		if (logger.isDebugEnabled()) logger.debug(stream_number + ">  Start " + database);
		if ( database.equals("hbase") ) {

			// Initialize table names
			ycsb_table_name = "YCSB_TABLE_" + scale_factor;
			if ( schema != null ) {
				ycsb_table_name = schema.toUpperCase() + "." + ycsb_table_name;
			}

			//  HBase Specific
			configuration = HBaseConfiguration.create();
			
			// column_family is stored in the props file
			String propFile = System.getProperty("dbconnect.properties");
			if ( propFile == null ) {
				propFile = "dbconnect.properties";
			}
			Properties props = new Properties();
			FileInputStream fs = new FileInputStream(new File(propFile));
			props.load(fs);
			column_family = props.getProperty("column_family");
			fs.close();
			
 			if (logger.isDebugEnabled()) logger.debug(stream_number + "  Open Hbase Table " + ycsb_table_name);
            hbase_table = new HTable(configuration, ycsb_table_name);


		} else { //  jdbc compliant database 
			
			if ( database.equals("seaquest") ) {
				option_update_syntax = "insert";
			}
			// Initialize table names
			ycsb_table_name = "ycsb_table_" + scale_factor;
			if ( schema != null ) {
				ycsb_table_name = schema + "." + ycsb_table_name;
			}

			// Initialize Database Connector
			database_connector.stream_number = stream_number;
			database_connector.database = database;
			database_connector.option_autocommit = option_autocommit;
			
			database_connector.establishDatabaseConnection();
			
		}

		if (logger.isDebugEnabled()) logger.debug(stream_number + "< start()");
		
	}
	
	
	//----------------------------------------------------------------------------------------------------------

	public void close() throws Exception {
		
		if ( database.equals("hbase") ) {
			hbase_table.close();
			configuration.clear();
			
		} else { //  jdbc compliant database
			database_connector.closeDatabaseConnection();
			singletonselect_stmt = null;
			singletonupdate_stmt = null;
			call_stmt = null;
		}
		
	}
	
	//----------------------------------------------------------------------------------------------------------

}
