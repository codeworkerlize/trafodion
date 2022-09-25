
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.yahoo.ycsb.Utils;
import com.yahoo.ycsb.workloads.CoreWorkload;

public class YCSBLoader extends Thread {

	// Single global value for all Streams (Threads)
	public static final int RESPONSE_BUCKETS = 101;
	public static final int MAX_CONNECT_ATTEMPTS = 100;
	public static final int VERY_LONG_TIME = 9999999;

	public static final String[] tables = { "ycsb_table" };

	public static final int ROWS_PER_SCALE = 1000000;

	public static final SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	public static final SimpleDateFormat hourformat = new SimpleDateFormat("HH:mm:ss");
	public static SimpleDateFormat datetimeformat = new SimpleDateFormat("yyyyMMdd.HHmmss");

	public static final Object threadlock = new Object();
	public static final Object stdoutlock = new Object();

	public static volatile boolean option_drop = false;
	public static volatile boolean option_dropcreate = false;
	public static volatile boolean option_createschema = false;
	public static volatile boolean option_create = false;
	public static volatile boolean option_delete = false;
	public static volatile boolean option_maintain = false;
	public static volatile boolean option_check = false;
	public static volatile boolean option_load = false;
	public static volatile boolean option_randomload = false;
	public static volatile boolean option_upsert = false;
	public static volatile boolean option_usingload = false;
	public static volatile boolean option_salt = false;
	public static volatile int saltsize = 1;
	public static volatile boolean option_compression = false;
	public static volatile String compression_type = "LZ4";
	public static volatile boolean option_block_encoding = false;
	public static volatile String block_encoding_type = "FAST_DIFF";
	
	public static volatile boolean option_autocommit = true;
	public static volatile int batchsize = 1;
	public static volatile int commitsize = 1;
	public static volatile boolean option_trace = false;
	public static volatile boolean option_debug = false;
        public static volatile boolean option_aligned = false;
        public static volatile boolean option_monarch = false;
	public static volatile boolean option_responsecurve = false;
	public static volatile boolean option_perstreamsummary = false;

	public static volatile int number_of_streams = 1;
	public static volatile int number_of_intervals = VERY_LONG_TIME;
	public static volatile int length_of_interval = 60;
	public static volatile int test_interval = 0;

	public static volatile int scale_factor;
        public static volatile int start_sf = -1;
        public static volatile int end_sf = -1;

	public static volatile Properties props = new Properties();
	public static volatile String jdbc_driver =  null;
	public static volatile String url = null;
	public static volatile String schema = null;
	public static volatile String database = null;

	// Separate Copies per Stream (Thread)
	public volatile long stream_accumlated_transactions = 0;
	public volatile long stream_accumlated_errors = 0;
	public volatile long stream_accumlated_response_micro = 0;
	public volatile long stream_maximum_response_micro = 0;
	public volatile long stream_minimum_response_micro = 999999999999999999L;
	public volatile int[] stream_response_distribution = new int[RESPONSE_BUCKETS];
	public volatile int stream_number;
	public volatile int start_scale;
	public volatile int stop_scale;

	public volatile boolean signal_ready = false;
	public volatile boolean signal_proceed = false;
	public volatile boolean signal_stop = false;

	static Logger logger = null;

//----------------------------------------------------------------------------------------------------------

	static Connection establishDatabaseConnection() throws Exception {
		if (option_debug) { System.out.println("> establishDatabaseConnection()"); }

		Connection dbconnection = null;

		Class.forName(jdbc_driver);

		// Establish Database Connection
		int attempts = 0;
		while ( dbconnection == null ) {
			attempts++;
			if (option_debug) { System.out.println(">	Attempt :" + attempts); }
			try {
				if (option_debug) { System.out.println("> Database = " + database); }
				if (database.equals("hive")) {
					dbconnection = DriverManager.getConnection(url, props.getProperty("user"), "");
				} else if (database.equals("mysql")) {
					if (option_debug) { System.out.println("> DriverManager.getConnection(" + url + ")"); }
					dbconnection = DriverManager.getConnection(url);
				} else {
					if (option_debug) { System.out.println("> DriverManager.getConnection(" + url + "," + props + ")"); }
					dbconnection = DriverManager.getConnection(url, props);
				}
			} catch (SQLException exception) {
				// try again
				if ( attempts % 10 == 0 ) {
					System.out.println(" Connection Failed ... will retry. Attempts: " + attempts );
				}
				SQLException nextException = exception;
				do {
					if (option_debug) {
						System.out.println("SQLEXCEPTION,  Error Code = " + exception.getErrorCode() );
						System.out.println("ERROR : SQLException : " + nextException.getMessage());
					}
				} while ((nextException = nextException.getNextException()) != null);
				if ( attempts > MAX_CONNECT_ATTEMPTS ) {
					throw new Exception("ERROR : Unable to connect to database (Maximum " + MAX_CONNECT_ATTEMPTS + " attempts failed).");
				}
				Thread.sleep( 1000 * attempts );  // Sleep
			}
		}
		if (option_debug) { System.out.println("Database Connection Established."); }

		if ( !database.equals("hive")) {
			if (option_autocommit) {
				dbconnection.setAutoCommit(true);  // Enable AutoCommit
			} else {
				dbconnection.setAutoCommit(false);
			}
		}

		if ( database.equals("trafodion") || database.equals("seaquest") ) {
			// if CQD file specified, apply CQDs to open session
			String control_cqd_file_name = System.getenv("TEST_CQD_FILE");
			if (( control_cqd_file_name != null ) && ( !control_cqd_file_name.equals(""))) {
				if (option_debug) { System.out.println("CQD File Specified.  Need to apply CQDs to the open session."); }
				if (option_debug) { System.out.println("Establish Statement."); }
				Statement dbstatement = dbconnection.createStatement();
				if (option_debug) { System.out.println("Statement established."); }
				String sql_statement = null;
				if (option_debug) { System.out.println("		Control CQD File:	" + control_cqd_file_name); }
				File control_cqd_file = null;
				control_cqd_file = new File(control_cqd_file_name);
				if (option_debug) { System.out.println("Read from File"); }
				BufferedReader reader = new BufferedReader( new FileReader ( control_cqd_file ) );
				while (( sql_statement = reader.readLine()) != null ) {
					if ( !sql_statement.equals("") ) {
						if (option_trace) { System.out.println(" " + sql_statement); }
						dbstatement.execute(sql_statement);
					}
				}
				reader.close();
				dbstatement.close();
				dbstatement = null;
			}
		}

		if (option_debug) { System.out.println("< establishDatabaseConnection()"); }

		return dbconnection;
	}

	//----------------------------------------------------------------------------------------------------------

	static void createSchema() throws Exception {

		if (option_debug) { System.out.println("> createSchema()"); }

		long taskStartTime = System.currentTimeMillis();
		System.out.println("\n   Create Schema : " + dateformat.format(new Date()));
		
		if ( schema == null) {
			System.out.println("	 Schema was not specified.  Can not create schema.");
			return;
		}

		try {

			Connection dbconnection = establishDatabaseConnection();
			Statement dbstatement = dbconnection.createStatement();
			String sql_statement = "create schema " + schema;

			if ( option_trace ) { System.out.println(" " + sql_statement); }
			dbstatement.execute(sql_statement);

			if ( !option_autocommit ) {
				dbconnection.commit();  // Commit Work
			}

			dbstatement.close();
			dbstatement = null;
			dbconnection.close();
			dbconnection = null;

		} catch (SQLException exception) {
			if ( exception.getErrorCode() == -1022 ) {
				System.out.println("   " + exception.getMessage());
			} else {
				System.out.println("\n----------------------------------------------------------");
				System.out.println("SQLEXCEPTION,  Error Code = " + exception.getErrorCode() );
				SQLException nextException = exception;
				do {
					System.out.println(nextException.getMessage());
				} while ((nextException = nextException.getNextException()) != null);
				System.out.println("----------------------------------------------------------\n");
				throw new Exception("Unexpected SQL Error.");
			}
		}

		System.out.println("	 Elapsed Seconds : " + String.format("%7.3f", (( System.currentTimeMillis() - taskStartTime ) / 1000.0) ));

		if (option_debug) { System.out.println("< createSchema()"); }
	}

//----------------------------------------------------------------------------------------------------------

	static void createTable() throws Exception {

		if (option_debug) { System.out.println("> createTable()"); }

		long taskStartTime = System.currentTimeMillis();
		System.out.println("\n   Create Table : " + dateformat.format(new Date()));

		try {

			Connection dbconnection = establishDatabaseConnection();
			Statement dbstatement = dbconnection.createStatement();
			String sql_statement = null;

			for (String table: tables) {

				String table_name = table + "_" + scale_factor;
				if ( schema != null ) {
					table_name = schema + "." + table_name;
				}

				System.out.println("	 " + table_name + " :	" + dateformat.format(new Date()));

				if ( database.equals("phoenix") ) {
					sql_statement = "create table " + table_name + " ( "
						+ " PRIM_KEY char(25) not null primary key"
						+ ", FIELD0 char(100) default null"
						+ ", FIELD1 char(100) default null"
						+ ", FIELD2 char(100) default null"
						+ ", FIELD3 char(100) default null"
						+ ", FIELD4 char(100) default null"
						+ ", FIELD5 char(100) default null"
						+ ", FIELD6 char(100) default null"
						+ ", FIELD7 char(100) default null"
						+ ", FIELD8 char(100) default null"
						+ ", FIELD9 char(100) default null"
						+ ") ";
				} else if ( database.equals("mysql") ) {
					sql_statement = "create table " + table_name + " ( "
						+ " PRIM_KEY char(25) not null"
						+ ", FIELD0 char(100) default null"
						+ ", FIELD1 char(100) default null"
						+ ", FIELD2 char(100) default null"
						+ ", FIELD3 char(100) default null"
						+ ", FIELD4 char(100) default null"
						+ ", FIELD5 char(100) default null"
						+ ", FIELD6 char(100) default null"
						+ ", FIELD7 char(100) default null"
						+ ", FIELD8 char(100) default null"
						+ ", FIELD9 char(100) default null"
						+ ", primary key (PRIM_KEY) )";
				} else {
					sql_statement = "create table " + table_name + " ( "
						+ " PRIM_KEY char(25) not null"
						+ ", FIELD0 char(100) default null"
						+ ", FIELD1 char(100) default null"
						+ ", FIELD2 char(100) default null"
						+ ", FIELD3 char(100) default null"
						+ ", FIELD4 char(100) default null"
						+ ", FIELD5 char(100) default null"
						+ ", FIELD6 char(100) default null"
						+ ", FIELD7 char(100) default null"
						+ ", FIELD8 char(100) default null"
						+ ", FIELD9 char(100) default null"
						+ ") primary key (PRIM_KEY)";
				}
				if ( option_salt ) {
					if ( database.equals("phoenix") ) {
						sql_statement = sql_statement + " SALT_BUCKETS=" + saltsize;
					} else if ( database.equals("trafodion") ) {
						sql_statement = sql_statement + " salt using " + saltsize + " partitions";
					} else if ( database.equals("mysql") ) {
						sql_statement = sql_statement + " partition by key ()  partitions " + saltsize ;
					}
				}

				if ( database.equals("seaquest") ) {
					sql_statement = sql_statement + " hash partition";
				}

                                if ( option_aligned) {
					sql_statement = sql_statement + " attribute aligned format ";
		                      	if ( option_monarch) {
                                       		sql_statement = sql_statement + " attribute storage monarch ";
					}
                                }
                                if ( option_monarch) {
                                        sql_statement = sql_statement + " attribute storage monarch ";
                                }

				if ( option_compression && option_block_encoding ) {
					sql_statement = sql_statement + " hbase_options ( data_block_encoding = '" + block_encoding_type + "', compression = '" + compression_type + "' )";
				} else if (option_compression) {
					sql_statement = sql_statement + " hbase_options ( compression = '" + compression_type + "' )";
				} else if (option_block_encoding) {
					sql_statement = sql_statement + " hbase_options ( data_block_encoding = '" + block_encoding_type + "' )";
				}

				if ( option_trace ) { System.out.println(" " + sql_statement); }
				try {
					dbstatement.execute(sql_statement);
				} catch (SQLException exception) {
					System.out.println("\n----------------------------------------------------------");
					System.out.println("SQLEXCEPTION on Statement,  Error Code = " + exception.getErrorCode() );
					System.out.println("   " + sql_statement);
					SQLException nextException = exception;
					do {
						System.out.println(nextException.getMessage());
					} while ((nextException = nextException.getNextException()) != null);
					System.out.println("----------------------------------------------------------\n");
					throw new Exception("Unexpected SQL Error.");
				}
			}

			if ( !option_autocommit ) {
				dbconnection.commit();  // Commit Work
			}

			dbstatement.close();
			dbstatement = null;
			dbconnection.close();
			dbconnection = null;

		} catch (SQLException exception) {
			System.out.println("\n----------------------------------------------------------");
			System.out.println("SQLEXCEPTION,  Error Code = " + exception.getErrorCode() );
			SQLException nextException = exception;
			do {
				System.out.println(nextException.getMessage());
			} while ((nextException = nextException.getNextException()) != null);
			System.out.println("----------------------------------------------------------\n");
			throw new Exception("Unexpected SQL Error.");
		}

		System.out.println("	 Elapsed Seconds : " + String.format("%7.3f", (( System.currentTimeMillis() - taskStartTime ) / 1000.0) ));

		if (option_debug) { System.out.println("< createTable()"); }
	}

//----------------------------------------------------------------------------------------------------------

	static void dropTable() throws Exception {

		if (option_debug) { System.out.println("> dropTable()"); }

		long taskStartTime = System.currentTimeMillis();
		System.out.println("\n   Drop Table : " + dateformat.format(new Date()));

		try {

			Connection dbconnection= establishDatabaseConnection();
			Statement dbstatement = dbconnection.createStatement();
			String sql_statement = null;

			for (String table: tables) {

				String table_name = table + "_" + scale_factor;
				if ( schema != null ) {
					table_name = schema + "." + table_name;
				}

				System.out.println("	 " + table_name + " :	" + dateformat.format(new Date()));

				sql_statement = "drop table " + table_name;
				if ( database.equals("trafodion") || database.equals("seaquset") ) {
					sql_statement = sql_statement + " cascade";
				}

				if ( option_trace ) { System.out.println(" " + sql_statement); }
				try {
					dbstatement.execute(sql_statement);
				} catch (SQLException exception) {
					System.out.println("\n----------------------------------------------------------");
					System.out.println("SQLEXCEPTION on Statement,  Error Code = " + exception.getErrorCode() );
					System.out.println("   " + sql_statement);
					SQLException nextException = exception;
					do {
						System.out.println(nextException.getMessage());
					} while ((nextException = nextException.getNextException()) != null);
					System.out.println("----------------------------------------------------------\n");
					if ( database.equals("trafodion") && exception.getErrorCode() == -1389) {
						// expected table does not exist error; ignore-
					} else if ( database.equals("seaquest") && exception.getErrorCode() == -1004) {
						// expected table does not exist error; ignore-
					} else if ( database.equals("mysql") && exception.getErrorCode() == 1051) {
						// expected table does not exist error; ignore-
					} else {
						throw new Exception("ERROR : Unexpected SQL Error.");
					}
				}
			}

			if ( !option_autocommit ) {
				dbconnection.commit();  // Commit Work
			}

			dbstatement.close();
			dbstatement = null;
			dbconnection.close();
			dbconnection = null;

		} catch (SQLException exception) {
			System.out.println("\n----------------------------------------------------------");
			System.out.println("SQLEXCEPTION,  Error Code = " + exception.getErrorCode() );
			SQLException nextException = exception;
			do {
				System.out.println(nextException.getMessage());
			} while ((nextException = nextException.getNextException()) != null);
			System.out.println("----------------------------------------------------------\n");
			throw new Exception("Unexpected SQL Error.");
		}

		System.out.println("	 Elapsed Seconds : " + String.format("%7.3f", (( System.currentTimeMillis() - taskStartTime ) / 1000.0) ));

		if (option_debug) { System.out.println("< dropTable()"); }

	}

//----------------------------------------------------------------------------------------------------------

	static void deleteTable () throws Exception {

		if (option_debug) { System.out.println("> deleteTable()"); }

		long taskStartTime = System.currentTimeMillis();
		System.out.println("\n   Delete Table : " + dateformat.format(new Date()));

		try {

			Connection dbconnection= establishDatabaseConnection();
			Statement dbstatement = dbconnection.createStatement();
			String sql_statement = null;

			for (String table: tables) {

				String table_name = table + "_" + scale_factor;
				if ( schema != null ) {
					table_name = schema + "." + table_name;
				}

				System.out.println("	 " + table_name + " :	" + dateformat.format(new Date()));

				sql_statement = "delete from " + table_name ;

				if ( option_trace ) { System.out.println(" " + sql_statement); }
				try {
					dbstatement.execute(sql_statement);
				} catch (SQLException exception) {
					System.out.println("\n----------------------------------------------------------");
					System.out.println("SQLEXCEPTION on Statement,  Error Code = " + exception.getErrorCode() );
					System.out.println("   " + sql_statement);
					SQLException nextException = exception;
					do {
						System.out.println(nextException.getMessage());
					} while ((nextException = nextException.getNextException()) != null);
					System.out.println("----------------------------------------------------------\n");
					throw new Exception("Unexpected SQL Error.");
				}
			}

			if ( !option_autocommit ) {
				dbconnection.commit();  // Commit Work
			}

			dbstatement.close();
			dbstatement = null;
			dbconnection.close();
			dbconnection = null;

		} catch (SQLException exception) {
			System.out.println("\n----------------------------------------------------------");
			System.out.println("SQLEXCEPTION,  Error Code = " + exception.getErrorCode() );
			SQLException nextException = exception;
			do {
				System.out.println(nextException.getMessage());
			} while ((nextException = nextException.getNextException()) != null);
			System.out.println("----------------------------------------------------------\n");
			throw new Exception("Unexpected SQL Error.");
		}

		System.out.println("	 Elapsed Seconds : " + String.format("%7.3f", (( System.currentTimeMillis() - taskStartTime ) / 1000.0) ));

		if (option_debug) { System.out.println("< deleteTable()"); }

	}

//----------------------------------------------------------------------------------------------------------

	static void maintainTable() throws Exception {

		if (option_debug) { System.out.println("> maintainTable()"); }

		long taskStartTime = System.currentTimeMillis();
		System.out.println("\n   Maintain Table : " + dateformat.format(new Date()));

		try {

			if ( database.equals("seaquest") || database.equals("trafodion") ) {

				Connection dbconnection= establishDatabaseConnection();
				Statement dbstatement = dbconnection.createStatement();
				String sql_statement = null;

				for (String table: tables) {

					String table_name = table + "_" + scale_factor;
					if ( schema != null ) {
						table_name = schema + "." + table_name;
					}

					System.out.println("	 " + table_name + " :	" + dateformat.format(new Date()));

					if (database.equals("trafodion")) {
						sql_statement = "update statistics for table " + table_name + " on every key generate 1 intervals sample ";
						// Workaround for long running and aborting update stats statements. 5/14/2014
						// sql_statement = "update statistics for table " + table_name + " on every column sample";
					} else if (database.equals("seaquest")) {
						sql_statement = "maintain table " + table_name + ", all";
					}

					if ( option_trace ) { System.out.println(" " + sql_statement); }
					try {
						dbstatement.execute(sql_statement);
					} catch (SQLException exception) {
						System.out.println("\n----------------------------------------------------------");
						System.out.println("SQLEXCEPTION on Statement,  Error Code = " + exception.getErrorCode() );
						System.out.println("   " + sql_statement);
						SQLException nextException = exception;
						do {
							System.out.println(nextException.getMessage());
						} while ((nextException = nextException.getNextException()) != null);
						System.out.println("----------------------------------------------------------\n");
						// throw new Exception("Unexpected SQL Error.");   5/1/2014 For now we expect errors from maintain
					}
				}

				if ( !option_autocommit ) {
					dbconnection.commit();  // Commit Work
				}

				dbstatement.close();
				dbstatement = null;
				dbconnection.close();
				dbconnection = null;

			}

		} catch (SQLException exception) {
			System.out.println("\n----------------------------------------------------------");
			System.out.println("SQLEXCEPTION,  Error Code = " + exception.getErrorCode() );
			SQLException nextException = exception;
			do {
				System.out.println(nextException.getMessage());
			} while ((nextException = nextException.getNextException()) != null);
			System.out.println("----------------------------------------------------------\n");
			throw new Exception("Unexpected SQL Error.");
		}

		System.out.println("	 Elapsed Seconds : " + String.format("%7.3f", (( System.currentTimeMillis() - taskStartTime ) / 1000.0) ));

		if (option_debug) { System.out.println("< maintainTable()"); }
	}

//----------------------------------------------------------------------------------------------------------

	static void checkTable() throws Exception {

		if (option_debug) { System.out.println("> checkTable()"); }

		long taskStartTime = System.currentTimeMillis();
		System.out.println("\n   Check Table : " + dateformat.format(new Date()));

		try {

			Connection dbconnection= establishDatabaseConnection();
			Statement dbstatement = dbconnection.createStatement();
			String sql_statement = null;

			for (String table: tables) {

				String table_name = table + "_" + scale_factor;
				if ( schema != null ) {
					table_name = schema + "." + table_name;
				}

				System.out.println("	 " + table_name + " :	" + dateformat.format(new Date()));

				int expected_rows = scale_factor * ROWS_PER_SCALE;

				try {
					sql_statement = "select count(*) as NUM_OF_RECS from " + table_name;
					if ( option_trace ) { System.out.println("  " + sql_statement); }
					ResultSet resultset = dbstatement.executeQuery(sql_statement);
					resultset.next();  // Expecting exactly 1 row
					int actual_rows = resultset.getInt("NUM_OF_RECS");
					resultset.close();
					if ( expected_rows == actual_rows ) {
						System.out.println("	   Table " + table_name + " loaded as expected ( Rows = " + actual_rows + " )" );
					} else {
						System.out.println("	   Table " + table_name + " NOT loaded as expected ( Expected Rows = " + expected_rows + ", Actual Rows = " + actual_rows + " )" );
					}
				} catch (SQLException exception) {
					System.out.println("\n----------------------------------------------------------");
					System.out.println("SQLEXCEPTION on Statement,  Error Code = " + exception.getErrorCode() );
					System.out.println("   " + sql_statement);
					SQLException nextException = exception;
					do {
						System.out.println(nextException.getMessage());
					} while ((nextException = nextException.getNextException()) != null);
					System.out.println("----------------------------------------------------------\n");
					throw new Exception("Unexpected SQL Error.");
				}
			}

			if ( !option_autocommit ) {
				dbconnection.commit();  // Commit Work
			}

			dbstatement.close();
			dbstatement = null;
			dbconnection.close();
			dbconnection = null;

		} catch (SQLException exception) {
			System.out.println("\n----------------------------------------------------------");
			System.out.println("SQLEXCEPTION,  Error Code = " + exception.getErrorCode() );
			SQLException nextException = exception;
			do {
				System.out.println(nextException.getMessage());
			} while ((nextException = nextException.getNextException()) != null);
			System.out.println("----------------------------------------------------------\n");
			throw new Exception("Unexpected SQL Error.");
		}

		System.out.println("	 Elapsed Seconds : " + String.format("%7.3f", (( System.currentTimeMillis() - taskStartTime ) / 1000.0) ));

		if (option_debug) { System.out.println("< checkTable()"); }
	}

//----------------------------------------------------------------------------------------------------------

	static void loadTable() throws Exception {

		if (option_debug) { System.out.println("> loadTable()"); }

		long taskStartTime = System.currentTimeMillis();
		System.out.println("\n   Load Table : " + dateformat.format(new Date()));

		// load data using parallel streams
		YCSBLoader[] stream = new YCSBLoader[number_of_streams];
		int threads_started=0;

                int scale_per_stream;

                if (start_sf > 0) {
                  scale_per_stream  = (end_sf - start_sf + 1) / number_of_streams;
                }
                else {
                  scale_per_stream  = scale_factor / number_of_streams;
                  start_sf = 1;
                  end_sf   = scale_factor;
                }
                if ( scale_per_stream == 0 ) {
                        scale_per_stream = 1;
                }

                int scale_indx = start_sf;

		System.out.println("\n   Starting up streams : " + dateformat.format(new Date()) );
		for ( int strm_idx = 0; strm_idx < number_of_streams; strm_idx++ ) {
			stream[strm_idx] = new YCSBLoader();
			stream[strm_idx].stream_number = strm_idx;
			stream[strm_idx].start_scale = scale_indx;
			stream[strm_idx].stop_scale = scale_indx + scale_per_stream - 1;

			if ( strm_idx < end_sf ) {
				if ( strm_idx == (number_of_streams - 1 )) { // Last stream
					stream[strm_idx].stop_scale = end_sf;
				}

				stream[strm_idx].start();
				threads_started++;
				if (option_trace) { System.out.println("  Thread " + strm_idx + "; Start_Scale " + stream[strm_idx].start_scale + "; Stop_Scale " + stream[strm_idx].stop_scale ); }

				scale_indx = scale_indx + scale_per_stream;
			}
		}
		System.out.println("	  " +  threads_started + " streams (threads) launched.");

		System.out.println("\n   Waiting for streams to get ready : " + dateformat.format(new Date()) );
		int ready = 0;
		int active = threads_started;
		int test_interval = 0;
		long wait_begin_millis = System.currentTimeMillis();
		while ( (ready < threads_started) && (active == threads_started) ) {
			Thread.sleep(1000);
			active = 0;
			ready = 0;
			for ( int strm_idx = 0; strm_idx < threads_started; strm_idx++ ) {
				if ( stream[strm_idx].isAlive() ) { active++; }
				if ( stream[strm_idx].signal_ready ) { ready++; }
			}

			long wait_seconds = ( System.currentTimeMillis() - wait_begin_millis ) / 1000;
			if ( wait_seconds > ( ( test_interval + 1 ) * length_of_interval ) ) {
				test_interval++;
				System.out.println("	  "
						+ "Interval " + String.format("%5d", test_interval )
						+ " , Time " + hourformat.format(new Date())
						+ " , Streams " + String.format("%5d", threads_started )
						+ " , Active " + String.format("%5d", active )
						+ " , Ready " + String.format("%5d", ready ) );
			}
		}
		System.out.println("	  " +  ready + " streams (threads) ready.");

		System.out.println("\n   Telling all streams to proceed : " + dateformat.format(new Date()) );
		// Tell all threads to proceed
		for ( int strm_idx = 0; strm_idx < threads_started; strm_idx++ ) {
			stream[strm_idx].signal_proceed = true;
		}

		System.out.println("\n   Streams running : " + dateformat.format(new Date()) );

		// Let the streams run till finished or time is exceeded
		int running = ready;
		long transactions = 0;
		long errors = 0;
		long response_micro = 0;
		long max_response_micro = 0;
		long min_response_micro = 999999999999999999L;
		long interval_time = System.currentTimeMillis();;
		double duration_seconds = 0;

		// Report Header
		System.out.println("\nXXX , "
			+ String.format("%5s", "Intvl" )
			+ " , " + String.format("%8s", "Time" )
			+ " , " + String.format("%5s", "Secs" )
			+ " , " + String.format("%5s", "Strms" )
			+ " , " + String.format("%4s", "Errs" )
			+ " , " + String.format("%6s", "Opers" )
			+ " , " + String.format("%8s", "OPS" )
			+ " , " + String.format("%10s", "AvgRsp(ms)" )
			+ " , " + String.format("%6s", "TotOps" )
			+ " , " + String.format("%6s", "TotRsp" )
			);

		int loop = 0;
		long prior_operations = 0;
		long interval_operations = 0;
		double prior_seconds = 0;
		double interval_seconds = 0;
		long prior_response = 0;
		long interval_response = 0;
		long begin_time = System.currentTimeMillis();;

		while ( ( running > 0 ) && (test_interval < number_of_intervals) ) {
			loop++;
			Thread.sleep(1000);
			running = 0;
			transactions = 0;
			errors = 0;
			response_micro = 0;
			max_response_micro = 0;
			min_response_micro = 999999999999999999L;

			for ( int idx = 0; idx < threads_started; idx++ ) {
				if (stream[idx].isAlive()) { running++; }
				transactions = transactions + stream[idx].stream_accumlated_transactions;
				errors = errors + stream[idx].stream_accumlated_errors;
				response_micro = response_micro + stream[idx].stream_accumlated_response_micro;
				if (stream[idx].stream_minimum_response_micro < min_response_micro ) {
					min_response_micro = stream[idx].stream_minimum_response_micro;
				}
				if (stream[idx].stream_maximum_response_micro > max_response_micro ) {
					max_response_micro = stream[idx].stream_maximum_response_micro;
				}
				if (option_debug) { System.out.println("      Loop " + loop + ": Stream " + idx
						+ ", ops " + stream[idx].stream_accumlated_transactions
						+ ", Response " + stream[idx].stream_accumlated_response_micro
						+ ", MinResponse " + stream[idx].stream_minimum_response_micro
						+ ", MaxResponse " + stream[idx].stream_maximum_response_micro); }
			}

			if ( ( System.currentTimeMillis() - interval_time ) / 1000.0 > length_of_interval ) {
				test_interval++;
				if ( test_interval == 0 ) {
					begin_time = System.currentTimeMillis();;
				}
				if ( test_interval > 0 ) {
					duration_seconds = ( System.currentTimeMillis() - begin_time ) / 1000.0;
				}
				interval_seconds = duration_seconds - prior_seconds;
				prior_seconds = duration_seconds;
				interval_operations = transactions - prior_operations;
				prior_operations = transactions;
				interval_response = response_micro - prior_response;
				prior_response = response_micro;

				interval_time = System.currentTimeMillis();
				if ( interval_operations > 0 ) {
					synchronized (stdoutlock) {
						System.out.println("XXX , "
							+ String.format("%5d", test_interval )
							+ " , " + hourformat.format(new Date())
							+ " , " + String.format("%5.0f", duration_seconds )
							+ " , " + String.format("%5d", running )
							+ " , " + String.format("%4d", errors )
							+ " , " + String.format("%6d", interval_operations )
							+ " , " + String.format("%8.2f", ( interval_operations * 100 / interval_seconds / 100.0 ))
							+ " , " + String.format("%10.3f", ( interval_response / interval_operations / 1000.0 ))
							+ " , " + String.format("%6d", transactions )
							+ " , " + response_micro
							);
					}

				} else {
					synchronized (stdoutlock) {
						System.out.println("XXX , "
							+ String.format("%5d", test_interval )
							+ " , " + hourformat.format(new Date())
							+ " , " + String.format("%5.0f", duration_seconds )
							+ " , " + String.format("%5d", running )
							+ " , " + String.format("%4d", errors )
							+ " , " + String.format("%6d", 0 )
							+ " , " + String.format("%8.2f", 0.0 )
							+ " , " + String.format("%10.3f", 0.0 )
								+ " , " + String.format("%6d", transactions )
							+ " , " + response_micro
							);
					}
				}
			}
		}

		System.out.println("\n   Telling all streams to stop : " + dateformat.format(new Date()) );
		for ( int idx = 0; idx < threads_started; idx++ ) {
			if (stream[idx].isAlive()) { stream[idx].signal_stop = true; }
		}

		System.out.println("\n   Wait for streams to Stop : " + dateformat.format(new Date()) );
		int interval = 0;
		active = threads_started;
		long begin_wait_time = System.currentTimeMillis();
		while ( active > 0 ) {
			Thread.sleep(100);
			active = 0;
			for ( int idx = 0; idx < threads_started; idx++ ) {
				if (stream[idx].isAlive()) { active++; }
			}
			if ( ( System.currentTimeMillis() - begin_wait_time ) > ( ( interval + 1 ) * length_of_interval ) * 1000 ) {
				interval++;
				System.out.println("      "
						+ "Interval " + String.format("%5d", interval )
						+ " , Streams " + String.format("%5d", threads_started )
						+ " , Active " + String.format("%5d", active ) );
			}
		}


		System.out.println("\n   Gather up final results : " + dateformat.format(new Date()) );
		transactions = 0;
		errors = 0;
		response_micro = 0;
		max_response_micro = 0;
		min_response_micro = 999999999999999999L;
		duration_seconds = ( System.currentTimeMillis() - begin_time ) / 1000.0;
		int[] response_distribution = new int[RESPONSE_BUCKETS];

		if ( option_perstreamsummary ) {
			System.out.println("\n      Summary Per Stream");
			System.out.println("\n   "
					+ String.format("%8s", "Strm" )
					+ " , " + String.format("%10s", "TotalOps" )
					+ " , " + String.format("%10s", "AvgRsp(ms)" )
					+ " , " + String.format("%10s", "MinRsp(ms)" )
					+ " , " + String.format("%10s", "MaxRsp(ms)" )
					+ " , " + String.format("%12s", "TotRsp(ms)" )
					);
		}

		for ( int idx = 0; idx < threads_started; idx++ ) {
			if (stream[idx].isAlive()) { running++; }
			transactions = transactions + stream[idx].stream_accumlated_transactions;
			errors = errors + stream[idx].stream_accumlated_errors;
			response_micro = response_micro + stream[idx].stream_accumlated_response_micro;
			if (stream[idx].stream_minimum_response_micro < min_response_micro ) {
				min_response_micro = stream[idx].stream_minimum_response_micro;
			}
			if (stream[idx].stream_maximum_response_micro > max_response_micro ) {
				max_response_micro = stream[idx].stream_maximum_response_micro;
			}
			for (int bucket = 0; bucket < RESPONSE_BUCKETS; bucket++ ) {
				response_distribution[bucket] = response_distribution[bucket] + stream[idx].stream_response_distribution[bucket];
			}

			if ( option_perstreamsummary ) {
				if ( stream[idx].stream_accumlated_transactions > 0 ) {
					System.out.println("   " +  String.format("%8d", idx )
						+ " , " + String.format("%10d", stream[idx].stream_accumlated_transactions )
						+ " , " + String.format("%10.3f", ( stream[idx].stream_accumlated_response_micro / stream[idx].stream_accumlated_transactions / 1000.0 ) )
						+ " , " + String.format("%10.3f", ( stream[idx].stream_minimum_response_micro / 1000.0 ) )
						+ " , " + String.format("%10.3f", ( stream[idx].stream_maximum_response_micro / 1000.0 ) )
						+ " , " + String.format("%12.3f", ( stream[idx].stream_accumlated_response_micro / 1000.0 ) )
						);
				} else {
					System.out.println("   " +  String.format("%8d", idx )
						+ " , " + String.format("%10d", stream[idx].stream_accumlated_transactions )
						+ " , " + String.format("%10.3f", ( 0.0 ) )
						+ " , " + String.format("%10.3f", ( 0.0 ) )
						+ " , " + String.format("%10.3f", ( 0.0 ) )
						+ " , " + String.format("%12.3f", ( 0.0 ) )
						);
				}
			}
		}

		if (option_responsecurve) {
			System.out.println("\n      Response Time Buckets");
			for (int bucket = 0; bucket < RESPONSE_BUCKETS; bucket++ ) {
				System.out.println("		ResponseDistribution[" + bucket + "] = " + response_distribution[bucket] );
			}
		}

		// Report Header
		System.out.println("\n   Total , "
			+ String.format("%5s", "Intvl" )
			+ " , " + String.format("%8s", "Time" )
			+ " , " + String.format("%5s", "Secs" )
			+ " , " + String.format("%5s", "Strms" )
			+ " , " + String.format("%5s", "Actv" )
			+ " , " + String.format("%5s", "Errs" )
			+ " , " + String.format("%8s", "OPS" )
			+ " , " + String.format("%10s", "AvgRsp(ms)" )
			+ " , " + String.format("%10s", "MinRsp(ms)" )
			+ " , " + String.format("%10s", "MaxRsp(ms)" )
			+ " , " + String.format("%6s", "TotOps" )
			+ " , " + String.format("%6s", "TotRsp" )
			);
		if ( transactions > 0 ) {
			System.out.println("   Total , "
				+ String.format("%5d", test_interval )
				+ " , " + hourformat.format(new Date())
				+ " , " + String.format("%5.0f", duration_seconds )
				+ " , " + String.format("%5d", threads_started )
				+ " , " + String.format("%5d", running )
				+ " , " + String.format("%5d", errors )
				+ " , " + String.format("%8.2f", ( transactions * 100 / duration_seconds / 100.0 ))
				+ " , " + String.format("%10.3f", ( response_micro / transactions / 1000.0 ))
				+ " , " + String.format("%10.3f", ( min_response_micro / 1000.0 ))
				+ " , " + String.format("%10.3f", ( max_response_micro / 1000.0 ))
				+ " , " + transactions
				+ " , " + response_micro
				);
		} else {
			System.out.println("   Total , "
				+ String.format("%5d", test_interval )
				+ " , " + hourformat.format(new Date())
				+ " , " + String.format("%5.0f", duration_seconds )
				+ " , " + String.format("%5d", threads_started )
				+ " , " + String.format("%5d", running )
				+ " , " + String.format("%5d", errors )
				+ " , " + String.format("%8.2f", 0.0 )
				+ " , " + String.format("%10.3f", 0.0 )
				+ " , " + String.format("%10.3f", 0.0 )
				+ " , " + String.format("%10.3f", 0.0 )
				+ " , " + transactions
				+ " , " + response_micro
				);
		}

		System.out.println("	 Elapsed Seconds : " + String.format("%7.3f", (( System.currentTimeMillis() - taskStartTime ) / 1000.0) ));

		if (option_debug) { System.out.println("< loadTable()"); }
	}


//----------------------------------------------------------------------------------------------------------

	public void run() {

		//  Parallel Stream Loader
		if (option_debug) { System.out.println(stream_number + "> run( " + start_scale + " , " + stop_scale + " )"); }

		long count = 0;
		
		try {

			CoreWorkload ycsbworkload = new CoreWorkload();  // We're going to use the YCSB routines to generate the data

			int start_row = (start_scale - 1) * ROWS_PER_SCALE;
			int stop_row = stop_scale * ROWS_PER_SCALE - 1;
			if ( option_debug) { System.out.println(stream_number + ">  Range " + start_row + " to " + stop_row ); }

			// Tell the main thread that I'm ready to proceed
			signal_ready = true;
			while (!signal_proceed){
				Thread.sleep(10);
			}

			if ( database.equals("hbase") ) {

				// Initialize table names
				String table_name = "YCSB_TABLE_" + scale_factor;
				if ( schema != null ) {
					table_name = schema + "." + table_name;
				}

				// hbase specific variables.
				Configuration configuration = null;
				String column_family = null;
			    HTable hbase_table =null;
			    // HBase Runtime
			    Put hput = null;

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
				
	 			if (logger.isDebugEnabled()) logger.debug(stream_number + "  Open Hbase Table " + table_name);
	            hbase_table = new HTable(configuration, table_name);
	            hbase_table.setAutoFlushTo(false);

	            String key_value = null;
	            long statement_start_time = 0;
	            long statement_elapsed_micro = 0;
	            int task_elapsed_milli = 0;
	            
				if ( stop_scale > 0 ) {

					for ( int idx = start_row; idx <= stop_row; idx++ ) {

						if ( signal_stop ) {
							idx = stop_row + 1;  //stop loading
						} else {
							
							count++;

							key_value = ycsbworkload.buildKeyName(idx);
							
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

							statement_start_time = System.nanoTime();

							hbase_table.put(hput);

					        statement_elapsed_micro = ( System.nanoTime() - statement_start_time ) / 1000;
					        
							stream_accumlated_transactions++;
							stream_accumlated_response_micro = stream_accumlated_response_micro + statement_elapsed_micro;
							if (statement_elapsed_micro < stream_minimum_response_micro ) {
								stream_minimum_response_micro = statement_elapsed_micro;
							}
							if (statement_elapsed_micro > stream_maximum_response_micro ) {
								stream_maximum_response_micro = statement_elapsed_micro;
							}
							task_elapsed_milli = (int) (statement_elapsed_micro / 1000);
							if ( task_elapsed_milli < (RESPONSE_BUCKETS - 1) ) {
								++stream_response_distribution[task_elapsed_milli];
							} else {
								++stream_response_distribution[RESPONSE_BUCKETS - 1];
							}

						}
					}

					if ( option_trace ) { System.out.println(stream_number + ">  " + count + " records inserted to " + table_name ); }
				}

				hbase_table.flushCommits();
				hbase_table.close();
				configuration.clear();

			} else { //  jdbc compliant database 
				
				PermutationGenerator permutation = new PermutationGenerator( scale_factor * ROWS_PER_SCALE );

				Connection dbconnection = establishDatabaseConnection();
				String sql_statement = null;

				PreparedStatement prepstatement = null;
				long prior_count = 0;

				if (database.equals("phoenix") && ! option_upsert ) {
					option_upsert = true;  // override it set
				}

				// Initialize Random Number Generator
				Random generator = new Random();
				generator.setSeed(stream_number * start_scale * 10000003 + 1234567);

				for (String table: tables) {

					String table_name = table + "_" + scale_factor;
					if ( schema != null ) {
						table_name = schema + "." + table_name;
					}

					count = 0;  prior_count = 0;

					if ( option_upsert ) {
						sql_statement = "upsert";
					} else {
						sql_statement = "insert";
					}
					if ( option_usingload ) {
						sql_statement = sql_statement + " using load";
					}
					sql_statement = sql_statement + " into " + table_name
								+ " ( PRIM_KEY, FIELD0, FIELD1, FIELD2, FIELD3, FIELD4, FIELD5, FIELD6, FIELD7, FIELD8, FIELD9 )"
								+ " values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

					if ( option_trace ) { System.out.println(stream_number + "> " + sql_statement); }
					prepstatement = dbconnection.prepareStatement(sql_statement);

					int random_prim_key = -1;
					if ( option_randomload ) {
						permutation.positionInt( start_row );
					}

					if ( stop_scale > 0 ) {


						for ( int idx = start_row; idx <= stop_row; idx++ ) {

							if ( signal_stop ) {
								idx = stop_row + 1;  //stop loading
							} else {
								if ( option_randomload ) {
									random_prim_key = permutation.nextInt();
									prepstatement.setString(1,ycsbworkload.buildKeyName(random_prim_key) );
								} else {
									prepstatement.setString(1,ycsbworkload.buildKeyName(idx) );
								}
								prepstatement.setString(2,Utils.ASCIIString(100));
								prepstatement.setString(3,Utils.ASCIIString(100));
								prepstatement.setString(4,Utils.ASCIIString(100));
								prepstatement.setString(5,Utils.ASCIIString(100));
								prepstatement.setString(6,Utils.ASCIIString(100));
								prepstatement.setString(7,Utils.ASCIIString(100));
								prepstatement.setString(8,Utils.ASCIIString(100));
								prepstatement.setString(9,Utils.ASCIIString(100));
								prepstatement.setString(10,Utils.ASCIIString(100));
								prepstatement.setString(11,Utils.ASCIIString(100));
								prepstatement.addBatch();
								if ( ++count % batchsize == 0 ) {
									long statement_start_time = System.nanoTime();
									prepstatement.executeBatch();	// Execute Batch
									long statement_elapsed_micro = ( System.nanoTime() - statement_start_time ) / 1000;
									prepstatement.clearBatch();

									stream_accumlated_transactions = stream_accumlated_transactions + (count - prior_count);
									stream_accumlated_response_micro = stream_accumlated_response_micro + statement_elapsed_micro;
									if (statement_elapsed_micro < stream_minimum_response_micro ) {
										stream_minimum_response_micro = statement_elapsed_micro;
									}
									if (statement_elapsed_micro > stream_maximum_response_micro ) {
										stream_maximum_response_micro = statement_elapsed_micro;
									}
									int task_elapsed_milli = (int) (statement_elapsed_micro / 1000);
									if ( task_elapsed_milli < (RESPONSE_BUCKETS - 1) ) {
										++stream_response_distribution[task_elapsed_milli];
									} else {
										++stream_response_distribution[RESPONSE_BUCKETS - 1];
									}

									prior_count = count;
								}
							}
						}

						if ( count > prior_count ) {
							long statement_start_time = System.nanoTime();
							prepstatement.executeBatch();	// Execute Batch
							long statement_elapsed_micro = ( System.nanoTime() - statement_start_time ) / 1000;
							prepstatement.clearBatch();

							stream_accumlated_transactions = stream_accumlated_transactions + (count - prior_count);
							stream_accumlated_response_micro = stream_accumlated_response_micro + statement_elapsed_micro;
							if (statement_elapsed_micro < stream_minimum_response_micro ) {
								stream_minimum_response_micro = statement_elapsed_micro;
							}
							if (statement_elapsed_micro > stream_maximum_response_micro ) {
								stream_maximum_response_micro = statement_elapsed_micro;
							}
							int task_elapsed_milli = (int) (statement_elapsed_micro / 1000);
							if ( task_elapsed_milli < (RESPONSE_BUCKETS - 1) ) {
								++stream_response_distribution[task_elapsed_milli];
							} else {
								++stream_response_distribution[RESPONSE_BUCKETS - 1];
							}
						}

						if ( option_trace ) { System.out.println(stream_number + ">  " + count + " records inserted to " + table_name ); }
						prepstatement.close();
					}
				}

				dbconnection.close();
				dbconnection = null;
			}

		} catch (SQLException exception) {
			synchronized (stdoutlock) {
				System.out.println("\n" + stream_number + "> ----------------------------------------------------------");
				System.out.println(stream_number + "> SQLEXCEPTION,  Error Code = " + exception.getErrorCode() );
				System.out.println(stream_number + ">   Stream Number : " + stream_number );
				SQLException nextException = exception;
				do {
					System.out.println(stream_number + "> SQLException:  " + nextException.getMessage());
				} while ((nextException = nextException.getNextException()) != null);
					System.out.println(stream_number + "----------------------------------------------------------\n");
			}
		} catch (Exception exception) {
			synchronized (stdoutlock) {
				String tombstone = 
						  "\n" + ">----------------------------------------------------------"
						+ "\n" + "> Unexpected Exception:  " + exception
						+ "\n" + ">   Stream Number : " + stream_number + " , Transaction Number : " + count
						+ "\n" + ">----------------------------------------------------------"
						+ "\n"
						;
				logger.error(tombstone, exception);
			}
		}

		if (option_debug) { System.out.println(stream_number + "< run()"); }

	}



//----------------------------------------------------------------------------------------------------------

	public static void main(String args[]) {

		long testStartTime = System.currentTimeMillis();

		String command_line = "";
		for (int idx = 0; idx < args.length; idx++) {
			command_line = command_line + args[idx] + " ";
		}
		System.out.println("YCSBLoader " + command_line);

		try {

			String testid = System.getenv("TESTID");
			String driverid = System.getenv("DRIVERID");
			String logdirectory = System.getenv("LOGDIRECTORY");

			if ( logdirectory == null ) {
				System.setProperty("logfile.name", "log/" + datetimeformat.format(new Date()) + ".javabench.log4j.log");
			} else {
				System.setProperty("logfile.name", logdirectory + "/" + testid + ".driver_" + driverid + ".log4j.log");
			}
			logger = Logger.getLogger("JavaBench");

			String syntax = "\n"
				+ "YCSBLoader: YCSBLoader <scale_factor> [<load_option>]\n"

				+ "   <scale_factor> : positive integer representing scale of database.\n"

				+ "   <load_option> : \n"
				+ "     schema <schema_name> : Overrides default schema name.\n"
				+ "     streams <number_of_streams> : Load data with <number_of_streams> streams. (Default 1)\n"
				+ "     streamrange <startstream> <stopstream> : To facilitate multiple drivers.\n"
				+ "     intervals <number_of_intervals> : Length of test in intervals. (Default very large number)\n"
				+ "     dropcreate : Will drop and recreate the tables prior to load.\n"
				+ "     drop : Will drop the tables.\n"
				+ "     createschema: Will create the schema.\n"
				+ "     create : Will create the tables new prior to load.\n"
				+ "     load : Will actually load the tables.\n"
				+ "     delete : Will purge (delete) the data from existing tables prior to load.\n"
				+ "     maintain : Will do a maintain,all on the table after load (SeaQuest specific).\n"
				+ "     check : Will check the number of rows (select count(*)) after the load.\n"
				+ "     batchsize <batchsize> : Load using jdbc batches of the given size.\n"
				+ "     commitsize <commitsize> : Disable Autocommit and commit every <commitsize> rows_added.\n"
				+ "     salt <saltsize> : Create the table with salting.\n"
                                + "     aligned : Create each table with aligned format attribute.\n"
                                + "     monarch : Create each table with monarch storage attribute.\n"
				+ "     compression : Create the table with compression.\n"
				+ "     compressiontype <compressiontype> : Overrides default compression type of SNAPPY"
				+ "     blockencoding <blockencodingtype> : Create the table with block_encoding.\n"
				+ "     blockencodingtype <blockencodingtype> : Overrides default endcoding of type of FAST_DIFF"
				+ "     upsert : Use upsert syntax instead of insert syntax.\n"
				+ "     usingload : Use using load syntax in insert/upsert.\n"
				+ "     randomload : Will load in a random permutations of key values.\n"
				+ "     intervallength <length_of_interval> : Lenght of reporting interval in seconds.(Default 10)\n"
				+ "     trace : Enable Statement Tracing.\n"
				+ "     debug : Enable Debug Tracing.\n"
				;

			// Command Interpreter
			if ( args.length < 1 ) {
				System.out.println(syntax);
				throw new Exception("ERROR : Required <scale_factor> arguments not provided.");
			}

			scale_factor = Integer.parseInt(args[0]);
			if ( scale_factor <= 0 ) {
				System.out.println(syntax);
				throw new Exception("ERROR : Invalid scale_factor Provided. ( scale_factor = " + scale_factor + " )");
			}

			String option = null;

			for ( int indx = 1; indx < args.length; indx++ ) {

				option = args[indx];
				switch ( option ) {
					case "schema": schema = args[++indx]; break;
					case "streams": number_of_streams = Integer.parseInt(args[++indx]); break;
                                        case "streamrange": start_sf = Integer.parseInt(args[++indx]);
                                                            end_sf = Integer.parseInt(args[++indx]); break;
					case "dropcreate": option_dropcreate = true; break;
					case "drop": option_drop = true; break;
					case "create": option_create = true; break;
					case "createschema": option_createschema = true; break;
					case "delete": option_delete = true; break;
					case "maintain": option_maintain = true; break;
					case "check": option_check = true; break;
					case "load": option_load = true; break;
					case "batchsize": batchsize = Integer.parseInt(args[++indx]); break;
					case "commitsize": commitsize = Integer.parseInt(args[++indx]); option_autocommit = false; break;
					case "upsert": option_upsert = true; break;
					case "usingload": option_usingload = true; break;
					case "salt": saltsize = Integer.parseInt(args[++indx]); option_salt = true; break;
					case "compression": option_compression = true; break;
					case "compressiontype": compression_type = args[++indx]; break;
					case "blockencoding": option_block_encoding = true; break;
					case "blockencodingtype": block_encoding_type = args[++indx]; break;
					case "randomload": option_randomload = true; compression_type = args[++indx]; break;
					case "intervals": number_of_intervals = Integer.parseInt(args[++indx]); break;
					case "intervallength": length_of_interval = Integer.parseInt(args[++indx]); break;
					case "trace": option_trace = true; break;
					case "debug": option_debug = true; break;
                                        case "aligned": option_aligned = true; break;
                                        case "monarch": option_monarch = true; break;
					default: {
						System.out.println(syntax);
						throw new Exception("ERROR : Invalid option specified ( option = " + option + " )");
					}
				}
			}

			String propFile = System.getProperty("dbconnect.properties");
			if ( propFile == null ) {
				propFile = "dbconnect.properties";
//				throw new Exception("Error : dbconnect.properties is not set. Exiting.");
			}

			// Use dbconnect.properties to get database connection info
			FileInputStream fs = new FileInputStream(new File(propFile));
			props.load(fs);
			jdbc_driver =  props.getProperty("jdbc.drivers");
			url = props.getProperty("url");
			database = props.getProperty("database");
			if (schema == null) {
				schema = props.getProperty("schema");
			}
			fs.close();

			if (option_debug) {
				System.out.println("		jdbc_driver : " + jdbc_driver);
				System.out.println("		url : " + url);
				System.out.println("		database : " + database);
				System.out.println("		schema : " + schema);
			}

			if (database.equals("seaquest") && option_upsert ) {
				throw new Exception("ERROR : Seaquest does not support upsert.");
			}

			if (database.equals("seaquest") && option_salt ) {
				throw new Exception("ERROR : Seaquest does not support salting.");
			}

			if (!database.equals("trafodion") && option_usingload ) {
				System.out.println("Warning : Option using load not supported for this database.  Will default it off.");
				option_usingload = false;
			}

			System.out.println("\nYCSBLoader");

			System.out.println("\n   " + String.format("%16s", "Load Starting" ) + " : " + dateformat.format(new Date()));

			System.out.println("   " + String.format("%16s", "PropertyFile" ) + " : " + propFile);
			System.out.println("   " + String.format("%16s", "Datebase" ) + " : " + database);
			System.out.println("   " + String.format("%16s", "Schema" ) + " : " + schema);
			System.out.println("   " + String.format("%16s", "ScaleFactor" ) + " : " + scale_factor);
			System.out.println("   " + String.format("%16s", "Streams" ) + " : " + number_of_streams);
                        if (start_sf > -1) { System.out.println("   " + String.format("%16s", "Streamrange" ) + " : " + start_sf + " - " + end_sf); }
			if (option_dropcreate) { System.out.println("   " + String.format("%16s", "DropCreate" ) + " : " + option_dropcreate); }
			if (option_drop) { System.out.println("   " + String.format("%16s", "Drop" ) + " : " + option_drop); }
			if (option_createschema) { System.out.println("   " + String.format("%16s", "CreateSchema" ) + " : " + option_createschema); }
			if (option_create) { System.out.println("   " + String.format("%16s", "Create" ) + " : " + option_create); }
			if (option_delete) { System.out.println("   " + String.format("%16s", "Delete" ) + " : " + option_delete); }
			if (option_maintain) { System.out.println("   " + String.format("%16s", "Maintain" ) + " : " + option_maintain); }
			if (option_check) { System.out.println("   " + String.format("%16s", "Check" ) + " : " + option_check); }
			if (option_load) { System.out.println("   " + String.format("%16s", "Load" ) + " : " + option_load); }

			System.out.println("   " + String.format("%16s", "AutoCommit" ) + " : " + option_autocommit);
			System.out.println("   " + String.format("%16s", "BatchSize" ) + " : " + batchsize);
			if (!option_autocommit) { System.out.println("   " + String.format("%16s", "CommitSize" ) + " : " + commitsize); }
			if (option_upsert) { System.out.println("   " + String.format("%16s", "Upsert" ) + " : " + option_upsert); }
			if (option_usingload) { System.out.println("   " + String.format("%16s", "UsingLoad" ) + " : " + option_usingload); }
			if (option_salt) { System.out.println("   " + String.format("%16s", "Salt" ) + " : " + saltsize); }
			if (option_compression) { System.out.println("   " + String.format("%16s", "Compression" ) + " : " + compression_type); }
			if (option_block_encoding) { System.out.println("   " + String.format("%16s", "BlockEncoding" ) + " : " + block_encoding_type); }
			if (option_randomload) { System.out.println("   " + String.format("%16s", "RandomLoad" ) + " : " + option_randomload); }
                        if (option_aligned) { System.out.println("   " + String.format("%16s", "Aligned" ) + " : " + option_aligned); }
                        if (option_monarch) { System.out.println("   " + String.format("%16s", "Monarch" ) + " : " + option_monarch); }

			if ( number_of_intervals != VERY_LONG_TIME ) { System.out.println("   " + String.format("%16s", "Intervals" ) + " : " + number_of_intervals); }
			System.out.println("   " + String.format("%16s", "IntervalLength" ) + " : " + length_of_interval);
			if (option_trace) { System.out.println("   " + String.format("%16s", "Trace" ) + " : " + option_trace); }
			if (option_debug) { System.out.println("   " + String.format("%16s", "Debug" ) + " : " + option_debug); }

			String control_cqd_file_name = System.getenv("TEST_CQD_FILE");
			if ( control_cqd_file_name != null ) {
				System.out.println("		Control CQD File:	" + control_cqd_file_name);
			}

			// turn off all logging ( Phoenix is very noisy. )
			Logger.getRootLogger().setLevel(Level.OFF);

			if (option_createschema) { createSchema(); }

			if (option_drop) { dropTable(); }

			if (option_create) { createTable(); }

			if (option_dropcreate) {
				dropTable();
				createTable();
			}

			if (option_delete) { deleteTable(); }

			if (option_load) { loadTable(); }

			if (option_maintain) { maintainTable(); }

			if (option_check) { checkTable(); }

		} catch (SQLException exception) {
			synchronized (stdoutlock) {
				System.out.println("\n" + "----------------------------------------------------------");
				System.out.println("SQLEXCEPTION : Error Code = " + exception.getErrorCode() );
				SQLException nextException = exception;
				do {
					System.out.println(nextException.getMessage());
				} while ((nextException = nextException.getNextException()) != null);
				System.out.println("----------------------------------------------------------\n");
			}
		} catch (Exception exception) {
			synchronized (stdoutlock) {
				System.out.println("\n" + "----------------------------------------------------------");
				System.out.println("EXCEPTION : " + exception.getMessage());
				System.out.println("----------------------------------------------------------\n");
			}
		}

		System.out.println("\n   " + String.format("%16s", "Test Stopping" ) + " : " + dateformat.format(new Date()));
		System.out.println("   " + String.format("%16s", "Elapsed Seconds" ) + " : " + String.format("%7.3f", (( System.currentTimeMillis() - testStartTime ) / 1000.0) ));
	}
}

