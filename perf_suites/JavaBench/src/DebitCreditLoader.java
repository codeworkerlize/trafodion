
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

import org.apache.log4j.Logger;

public class DebitCreditLoader extends Thread {

	// Single global value for all Streams (Threads)
	public static final int RESPONSE_BUCKETS = 101;
	public static final int MAX_CONNECT_ATTEMPTS = 100;
	public static final int VERY_LONG_TIME = 9999999;

	public static final String[] tables = { "branch", "teller", "account", "history" };

	public static final int ACCOUNT_ROWS_PER_SCALE = 100000;
	public static final int BRANCH_ROWS_PER_SCALE = 1;
	public static final int TELLER_ROWS_PER_SCALE = 10;

	public static final SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	public static final SimpleDateFormat hourformat = new SimpleDateFormat("HH:mm:ss");
	public static SimpleDateFormat datetimeformat = new SimpleDateFormat("yyyyMMdd.HHmmss");

	public static final Object threadlock = new Object();
	public static final Object stdoutlock = new Object();

	public static volatile boolean option_drop = false;
	public static volatile boolean option_dropcreate = false;
	public static volatile boolean option_create = false;
	public static volatile boolean option_createschema = false;
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
	public static volatile boolean option_autocommit = true;
	public static volatile int batchsize = 1;
	public static volatile int commitsize = 1;
	public static volatile boolean option_trace = false;
	public static volatile boolean option_debug = false;
        public static volatile boolean option_aligned = false;
	public static volatile String option_table_name = null;
	public static volatile boolean option_responsecurve = false;
	public static volatile boolean option_perstreamsummary = false;

	public static volatile int number_of_streams = 1;
	public static volatile int number_of_intervals = VERY_LONG_TIME;
	public static volatile int length_of_interval = 60;
	public static volatile int test_interval = 0;

	public static volatile int scale_factor;

	public static volatile Properties props = new Properties();
	public static volatile String jdbc_driver =  null;
	public static volatile String url = null;
	public static volatile String schema = null;
	public static volatile String database = null;

	// Random Permutation Generator
	public static volatile int rpg_total_values = 0;
	public static volatile int rpg_generator = 0;
	public static volatile int rpg_prime = 0;
	public static volatile int rpg_init_value;

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
	public volatile String load_table;

	public volatile boolean signal_ready = false;
	public volatile boolean signal_proceed = false;
	public volatile boolean signal_stop = false;

	static Logger logger = null;

//----------------------------------------------------------------------------------------------------------

	static private int next_random_permutation( int current_value ) {
		if (option_debug) { System.out.println("> next_random_permutation (" + current_value + ")"); }
		/****************************** random_permutation ***********************************
		* generates a unique random permutation of numbers in [1...limit] by generating
		* all elements in the Galois field Z(prime) under multiplication.
		* **********************************************************************/
		do {
			long long_seed = (rpg_generator * current_value) % rpg_prime ;
			current_value = (int) long_seed;
		} while ( current_value > rpg_total_values );

		if (option_debug) { System.out.println("< next_random_permutation(" + current_value + ")"); }
		return(current_value);
	}

//----------------------------------------------------------------------------------------------------------

	static private int position_random_permutation( int minval ) throws Exception {
		if (option_debug) { System.out.println("> position_random_permutation(" + minval + ")"); }

		int seed = rpg_generator;		// initialize seed
		//  We now need to loop until we're at the proper position.
		for ( int idx = 1; idx < minval; idx++ ) {
			seed = next_random_permutation( seed );
		}

		if (option_debug) { System.out.println("< next_random_permutation (" + seed + ")"); }
		return(seed);
	}

//----------------------------------------------------------------------------------------------------------

	static private void init_random_permutation( int totrecs ) throws Exception {
		if (option_debug) { System.out.println("> init_random_permutation(" + totrecs + " )"); }

		rpg_total_values = totrecs;

		// Choose a prime number that is appropriate for the size of the set
		if (totrecs <= 1000) {			 /* 1 thousand */
			rpg_generator = 279;
			rpg_prime = 1009;
		} else if (totrecs <= 10000) {	 /* 10 thousand */
			rpg_generator = 2969;
			rpg_prime = 10007;
		} else if (totrecs <= 100000) {	/* 100 thousand */
			rpg_generator = 21395;
			rpg_prime = 100003;
		} else if (totrecs <= 1000000) {   /* 1 million */
			rpg_generator = 21395;
			rpg_prime = 1000003;
		} else if (totrecs <= 10000000) {  /* 10 million */
			rpg_generator = 213957;
			rpg_prime = 10000019;
		} else if (totrecs <= 100000000){  /* 100 million */
			rpg_generator = 2139575;
			rpg_prime = 100000007;
		} else if (totrecs <= 1000000000){ /* 1 billion */
			rpg_generator = 2139575;
			rpg_prime = 1000000007;
		} else if (totrecs <= 1073741824){ /* 2^30 */
			rpg_generator = 2139575;
			rpg_prime = 1073741827;
		} else {
			throw new Exception("ERROR : init_random_permutation support only upto 1073741824 or 2^30 combinations.");
		}

		if (option_debug) { System.out.println("< init_random_permutation()"); }
	}

//----------------------------------------------------------------------------------------------------------

	static String generateRandomAString (Random generator, int strlength) {
		final String alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
		final int N = alphabet.length();
		String random_string = "";
		for ( int indx = 0; indx < strlength; indx++ ) {
			random_string = random_string + alphabet.charAt(generator.nextInt(N));
		}
		return random_string;
	}


//----------------------------------------------------------------------------------------------------------

	static Connection establishDatabaseConnection() throws Exception {
		if (option_debug) { System.out.println("> establishDatabaseConnection()"); }

		Connection dbconnection = null;

		if (option_debug) { System.out.println(">	jdbc_driver :" + jdbc_driver); }
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

	static void createTable(String table) throws Exception {

		if (option_debug) { System.out.println("> createTable()"); }

		long taskStartTime = System.currentTimeMillis();
		System.out.println("\n   Create Table : " + dateformat.format(new Date()));

		try {

			Connection dbconnection = establishDatabaseConnection();
			Statement dbstatement = dbconnection.createStatement();
			String sql_statement = null;

//			for (String table: tables) {

				String table_name = "dc_" + table + "_" + scale_factor;
				if ( schema != null ) {
					table_name = schema + "." + table_name;
				}

				System.out.println("	 " + table_name + " :	" + dateformat.format(new Date()));

				switch ( table ) {

					case "branch":
						if ( database.equals("hive")) {

							String table_location = "/javabench/dc/dc_branch_" + scale_factor;

							sql_statement = "create external table " + table_name + " ( "
									+ " BRANCH_ID int"
									+ ", BRANCH_BALANCE bigint"
									+ ", BRANCH_FILLER char(88)"
									+ " )"
									+ " row format delimited fields terminated by '|'"
									+ " stored as textfile"
									+ " location '" + table_location + "'";
						} else if ( database.equals("mysql") ) {
							sql_statement = "create table " + table_name + " ( "
									+ " BRANCH_ID int not null primary key"
									+ ", BRANCH_BALANCE bigint"
									+ ", BRANCH_FILLER char(88)"
									+ ")";
						} else {
							sql_statement = "create table " + table_name + " ( "
								+ " BRANCH_ID integer not null"
								+ ", BRANCH_BALANCE largeint"
								+ ", BRANCH_FILLER char(88)"
								+ ", primary key (BRANCH_ID)"
								+ ")";
						}
						if ( option_salt ) {
							if ( database.equals("trafodion") ) {
								sql_statement = sql_statement + " salt using " + saltsize + " partitions on (BRANCH_ID)";
							} else if ( database.equals("mysql") ) {
								sql_statement = sql_statement + " partition by key ()  partitions " + saltsize ;
							}
						}
						break;

					case "teller":

						if ( database.equals("hive")) {

							String table_location = "/javabench/dc/dc_teller_" + scale_factor;

							sql_statement = "create external table " + table_name + " ( "
									+ " TELLER_ID int"
									+ ", BRANCH_ID int"
									+ ", TELLER_BALANCE bigint"
									+ ", TELLER_FILLER char(88)"
									+ " )"
									+ " row format delimited fields terminated by '|'"
									+ " stored as textfile"
									+ " location '" + table_location + "'";

						} else if ( database.equals("mysql") ) {
							sql_statement = "create table " + table_name + " ( "
									+ " TELLER_ID int not null primary key"
									+ ", BRANCH_ID int"
									+ ", TELLER_BALANCE bigint"
									+ ", TELLER_FILLER char(84)"
									+ ")";
						} else {
							sql_statement = "create table " + table_name + " ( "
								+ " TELLER_ID integer not null"
//								+ ", BRANCH_ID integer not null"
								+ ", BRANCH_ID integer"
								+ ", TELLER_BALANCE largeint"
								+ ", TELLER_FILLER char(84)"
//								+ ", primary key (TELLER_ID, BRANCH_ID)"
								+ ", primary key (TELLER_ID)"
								+ ")";
						}
						if ( option_salt ) {
							if ( database.equals("trafodion") ) {
//								sql_statement = sql_statement + " salt using " + saltsize + " partitions on (BRANCH_ID)";
								sql_statement = sql_statement + " salt using " + saltsize + " partitions";
							} else if ( database.equals("mysql") ) {
								sql_statement = sql_statement + " partition by key ()  partitions " + saltsize ;
							}
						}
						break;

					case "account":

						if ( database.equals("hive")) {

							String table_location = "/javabench/dc/dc_account_" + scale_factor;

							sql_statement = "create external table " + table_name + " ( "
									+ " ACCOUNT_ID int"
									+ ", BRANCH_ID int"
									+ ", ACCOUNT_BALANCE bigint"
									+ ", ACCOUNT_FILLER char(88)"
									+ " )"
									+ " row format delimited fields terminated by '|'"
									+ " stored as textfile"
									+ " location '" + table_location + "'";

						} else if ( database.equals("mysql") ) {
							sql_statement = "create table " + table_name + " ( "
									+ " ACCOUNT_ID int not null primary key"
									+ ", BRANCH_ID int"
									+ ", ACCOUNT_BALANCE bigint"
									+ ", ACCOUNT_FILLER char(84)"
									+ ")";
						} else {
							sql_statement = "create table " + table_name + " ( "
								+ " ACCOUNT_ID integer not null"
//								+ ", BRANCH_ID integer not null"
								+ ", BRANCH_ID integer"
								+ ", ACCOUNT_BALANCE largeint"
								+ ", ACCOUNT_FILLER char(84)"
								+ ", primary key (ACCOUNT_ID)"
//								+ ", primary key (ACCOUNT_ID, BRANCH_ID)"
								+ ")";
						}
						if ( option_salt ) {
							if ( database.equals("mysql") ) {
								sql_statement = sql_statement + " partition by key ()  partitions " + saltsize ;
							} else if ( database.equals("seaquest") ) {
								sql_statement = sql_statement + "  hash partition";
							} else if ( database.equals("trafodion") ) {
//								sql_statement = sql_statement + " salt using " + saltsize + " partitions on (BRANCH_ID)";
								sql_statement = sql_statement + " salt using " + saltsize + " partitions";
							}
						}
						break;

					case "history":

						if ( database.equals("hive")) {

							String table_location = "/javabench/dc/dc_history_" + scale_factor;

							sql_statement = "create external table " + table_name + " ( "
									+ " BRANCH_ID  int"
									+ ", TELLER_ID  int"
									+ ", ACCOUNT_ID int"
									+ ", AMOUNT bigint"
									+ ", TRANSACTION_TIME timestamp"
									+ ", HISTORY_FILLER char(22)"
									+ " )"
									+ " row format delimited fields terminated by '|'"
									+ " stored as textfile"
									+ " location '" + table_location + "'";

						} else if ( database.equals("mysql") ) {
							sql_statement = "create table " + table_name + " ( "
								+ "  BRANCH_ID  int not null"
								+ ", TELLER_ID  int not null"
								+ ", ACCOUNT_ID int not null"
								+ ", AMOUNT bigint"
								+ ", TRANSACTION_TIME timestamp not null"
								+ ", HISTORY_FILLER char(22) )";
						} else {
							sql_statement = "create table " + table_name + " ( "
								+ "  BRANCH_ID  integer not null"
								+ ", TELLER_ID  integer not null"
								+ ", ACCOUNT_ID integer not null"
								+ ", AMOUNT largeint not null"
								+ ", TRANSACTION_TIME timestamp not null"
								+ ", HISTORY_FILLER char(22) not null"
								+ ") store by ( BRANCH_ID, TRANSACTION_TIME )";
						}
						if ( option_salt ) {
							if ( database.equals("trafodion") ) {
								sql_statement = sql_statement + " salt using " + saltsize + " partitions on ( BRANCH_ID )";
							} else if ( database.equals("seaquest") ) {
								sql_statement = sql_statement + "  hash partition on ( BRANCH_ID )";
							} else if ( database.equals("mysql") ) {
								sql_statement = sql_statement + " partition by key (BRANCH_ID) partitions " + saltsize ;
							}
						}
						break;

					default:
				}

                                if ( option_aligned) {
                                        sql_statement = sql_statement + " attribute aligned format ";
                                }

				if ( option_compression) {
					sql_statement = sql_statement + " hbase_options ( compression = 'LZ4')";
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
//			}

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

	static void dropTable(String table) throws Exception {

		if (option_debug) { System.out.println("> dropTable()"); }

		long taskStartTime = System.currentTimeMillis();
		System.out.println("\n   Drop Table : " + dateformat.format(new Date()));

		try {

			Connection dbconnection= establishDatabaseConnection();
			Statement dbstatement = dbconnection.createStatement();
			String sql_statement = null;

//			for (String table: tables) {

				String table_name = "dc_" + table + "_" + scale_factor;
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
//			}

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

	static void deleteTable (String table) throws Exception {

		if (option_debug) { System.out.println("> deleteTable()"); }

		long taskStartTime = System.currentTimeMillis();
		System.out.println("\n   Delete Table : " + dateformat.format(new Date()));

		try {

			Connection dbconnection= establishDatabaseConnection();
			Statement dbstatement = dbconnection.createStatement();
			String sql_statement = null;

//			for (String table: tables) {

				String table_name = "dc_" + table + "_" + scale_factor;
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
//			}

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

	static void maintainTable(String table) throws Exception {

		if (option_debug) { System.out.println("> maintainTable()"); }

		long taskStartTime = System.currentTimeMillis();
		System.out.println("\n   Maintain Table : " + dateformat.format(new Date()));

		try {

			if ( database.equals("seaquest") || database.equals("trafodion") ) {

				Connection dbconnection= establishDatabaseConnection();
				Statement dbstatement = dbconnection.createStatement();
				String sql_statement = null;

//				for (String table: tables) {

					if ( ! table.equals("history")) {
						String table_name = "dc_" + table + "_" + scale_factor;
						if ( schema != null ) {
							table_name = schema + "." + table_name;
						}

						System.out.println("	 " + table_name + " :	" + dateformat.format(new Date()));

						if (database.equals("trafodion")) {
							sql_statement = "update statistics for table " + table_name + " on every key generate 1 intervals";
							// Workaround for long running and aborting update stats statements. 5/14/2014
							// sql_statement = "update statistics for table " + table_name + " on every column sample";
							if ( table.equals("account")) {
								sql_statement = sql_statement + " sample 1000 rows";
							}
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
//				}

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

	static void checkTable(String table) throws Exception {

		if (option_debug) { System.out.println("> checkTable()"); }

		long taskStartTime = System.currentTimeMillis();
		System.out.println("\n   Check Table : " + dateformat.format(new Date()));

		try {

			Connection dbconnection= establishDatabaseConnection();
			Statement dbstatement = dbconnection.createStatement();
			String sql_statement = null;

//			for (String table: tables) {

				String table_name = "dc_" + table + "_" + scale_factor;
				if ( schema != null ) {
					table_name = schema + "." + table_name;
				}

				System.out.println("	 " + table_name + " :	" + dateformat.format(new Date()));

				int expected_rows = 0;
				switch ( table ) {
					case "branch":		expected_rows = scale_factor;			break;
					case "teller":		expected_rows = scale_factor * TELLER_ROWS_PER_SCALE;		break;
					case "account":		expected_rows = scale_factor * ACCOUNT_ROWS_PER_SCALE;	break;
					case "history":		expected_rows = 0;						break;
					default:
				}

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
//			}

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

	static void loadTable(String table) throws Exception {

		if (option_debug) { System.out.println("> loadTable()"); }

		long taskStartTime = System.currentTimeMillis();
		System.out.println("\n   Load Table : " + dateformat.format(new Date()));

		// load data using parallel streams
		DebitCreditLoader[] stream = new DebitCreditLoader[number_of_streams];
		int threads_started=0;

		int scale_per_stream = scale_factor / number_of_streams;
		if ( scale_per_stream == 0 ) {
			scale_per_stream = 1;
		}

		int scale_indx = 1;

		System.out.println("\n   Starting up streams : " + dateformat.format(new Date()) );
		for ( int strm_idx = 0; strm_idx < number_of_streams; strm_idx++ ) {
			stream[strm_idx] = new DebitCreditLoader();
			stream[strm_idx].stream_number = strm_idx;
			stream[strm_idx].start_scale = scale_indx;
			stream[strm_idx].stop_scale = scale_indx + scale_per_stream - 1;
			stream[strm_idx].load_table = table;

			if ( strm_idx < scale_factor ) {
				if ( strm_idx == (number_of_streams - 1 )) { // Last stream
					stream[strm_idx].stop_scale = scale_factor;
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

		while (( running > 0 ) && (test_interval < number_of_intervals) ){
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

		try {

			Connection dbconnection = establishDatabaseConnection();
			String sql_statement = null;

			PreparedStatement prepstatement = null;
			long count = 0;
			long prior_count = 0;

			// Initialize Random Number Generator
			Random generator = new Random();
			generator.setSeed(stream_number * start_scale * 10000003 + 1234567);

			// Tell the main thread that I'm ready to proceed
			signal_ready = true;
			while (!signal_proceed){
				Thread.sleep(10);
			}

//			for (String table: tables) {

				String table_name = "dc_" + load_table + "_" + scale_factor;
				if ( schema != null ) {
					table_name = schema + "." + table_name;
				}

				switch ( load_table ) {

				case "branch":

					if (stream_number == 0) {

						count = 0;  prior_count = 0;

						if ( option_upsert ) {
							sql_statement = "upsert";
						} else {
							sql_statement = "insert";
						}
						if ( option_usingload ) {
							sql_statement = sql_statement + " using load";
						}
						sql_statement = sql_statement + " into " + table_name + " ( BRANCH_ID, BRANCH_BALANCE, BRANCH_FILLER ) values (?, ?, ?)";
						if ( option_trace ) { System.out.println("  " + sql_statement); }
						prepstatement = dbconnection.prepareStatement(sql_statement);

						// Load 1 BRANCH record per scale factor
						for ( int idx1 = 0; idx1 <= scale_factor-1; idx1++ ) {
							if ( signal_stop ) {
								idx1 = scale_factor;  //stop loading
							} else {
								prepstatement.setInt(1,idx1);
								prepstatement.setLong(2,0);
								prepstatement.setString(3,generateRandomAString(generator,88));
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

					break;

				case "teller":

					if ( stream_number == 0 ) {

						count = 0;  prior_count = 0;

						if ( option_upsert ) {
							sql_statement = "upsert";
						} else {
							sql_statement = "insert";
						}
						if ( option_usingload ) {
							sql_statement = sql_statement + " using load";
						}
						sql_statement = sql_statement + " into " + table_name + " ( TELLER_ID, BRANCH_ID, TELLER_BALANCE, TELLER_FILLER ) values (?, ?, ?, ?)";
						if ( option_trace ) { System.out.println("  " + sql_statement); }
						prepstatement = dbconnection.prepareStatement(sql_statement);

						// Load 10 TELLER records per scale factor
						for ( int idx1 = 0; idx1 <= scale_factor-1; idx1++ ) {
							for ( int idx2 = 0; idx2 < TELLER_ROWS_PER_SCALE; idx2++ ) {
								if ( signal_stop ) {
									idx1 = scale_factor;  //stop loading
									idx2 = TELLER_ROWS_PER_SCALE;  //stop loading
								} else {
									prepstatement.setInt(1,(idx1 * 10) + idx2);
									prepstatement.setInt(2,idx1);
									prepstatement.setLong(3,0);
									prepstatement.setString(4,generateRandomAString(generator,84));
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

					break;

				case "account":

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
								+ " ( ACCOUNT_ID, BRANCH_ID, ACCOUNT_BALANCE, ACCOUNT_FILLER ) values (?, ?, ?, ?)";
					if ( option_trace ) { System.out.println(stream_number + "> " + sql_statement); }
					prepstatement = dbconnection.prepareStatement(sql_statement);

					int start_row = (start_scale - 1) * ACCOUNT_ROWS_PER_SCALE;
					int stop_row = stop_scale * ACCOUNT_ROWS_PER_SCALE - 1;
					if ( option_debug) { System.out.println(stream_number + ">  Range " + start_row + " to " + stop_row ); }

					int random_prim_key = -1;
					if ( option_randomload ) {
						random_prim_key = position_random_permutation( start_row );
					}

					if ( stop_scale > 0 ) {

						for ( int idx = start_row; idx <= stop_row; idx++ ) {
							if ( signal_stop ) {
								idx = stop_row;  //stop loading
							} else {
								if ( option_randomload ) {
									random_prim_key = next_random_permutation( random_prim_key );
									prepstatement.setInt(1,random_prim_key);
								} else {
									prepstatement.setInt(1,idx);
								}
								prepstatement.setInt(2,idx/ACCOUNT_ROWS_PER_SCALE);
								prepstatement.setLong(3,0);
								prepstatement.setString(4,generateRandomAString(generator,84));
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
					break;

				default:
				}

//			}

			dbconnection.close();
			dbconnection = null;

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
				System.out.println("\n" + stream_number + "> ----------------------------------------------------------");
				System.out.println(stream_number + "> EXCEPTION: " + exception.getMessage());
				System.out.println(stream_number + ">   Stream Number : " + stream_number );
				System.out.println(stream_number + "----------------------------------------------------------\n");
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
		System.out.println("DebitCreditLoader " + command_line);

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
				+ "DebitCredit: DebitCreditLoader <scale_factor> [<load_option>]\n"

				+ "   <scale_factor> : positive integer representing scale of database.\n"

				+ "   <load_option> : \n"
				+ "     schema <schema_name> : Overrides default schema name.\n"
				+ "     table <table_name> : Specify specific table (branch,teller,account,history)"
				+ "     streams <number_of_streams> : Load data with <number_of_streams> streams. (Default 1)\n"
				+ "     streamrange <startstream> <stopstream> : To facilitate multiple drivers.\n"
				+ "     intervals <number_of_intervals> : Length of test in intervals. (Default very large number)\n"
				+ "     createschema: Will create the schema.\n"
				+ "     dropcreate : Will drop and recreate the tables prior to load.\n"
				+ "     drop : Will drop the tables.\n"
				+ "     create : Will create the tables new prior to load.\n"
				+ "     load : Will actually load the tables.\n"
				+ "     delete : Will purge (delete) the data from existing tables prior to load.\n"
				+ "     maintain : Will do a maintain,all on the table after load (SeaQuest specific).\n"
				+ "     check : Will check the number of rows (select count(*)) after the load.\n"
				+ "     batchsize <batchsize> : Load using jdbc batches of the given size.\n"
				+ "     commitsize <commitsize> : Disable Autocommit and commit every <commitsize> rows_added.\n"
				+ "     salt <saltsize> : Create the table with salting.\n"
                                + "     aligned : Create each table with aligned format attribute.\n"
				+ "     compression : Create the table with compression.\n"
				+ "     upsert : Use upsert syntax instead of insert syntax.\n"
				+ "     usingload : Use using load syntax in insert/upsert.\n"
				+ "     randomload : Will load in a random permutations of key values.\n"
				+ "     intervallength <length_of_interval> : Length of reporting interval in seconds.(Default 10)\n"
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
					case "table": option_table_name = args[++indx]; break;
					case "streams": number_of_streams = Integer.parseInt(args[++indx]); break;
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
					case "randomload": option_randomload = true; break;
					case "intervals": number_of_intervals = Integer.parseInt(args[++indx]); break;
					case "intervallength": length_of_interval = Integer.parseInt(args[++indx]); break;
					case "trace": option_trace = true; break;
					case "debug": option_debug = true; break;
                                        case "aligned": option_aligned = true; break;
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

			if (option_table_name != null) {
				if (!option_table_name.equals("branch") && !option_table_name.equals("teller")
						&& !option_table_name.equals("account") && !option_table_name.equals("history")) {
					throw new Exception("ERROR : Invalid table name specified.  Expected branch, teller, account, or history.");
				}
			}

			System.out.println("\nDebitCreditLoader");

			System.out.println("\n   " + String.format("%16s", "Load Starting" ) + " : " + dateformat.format(new Date()));

			System.out.println("   " + String.format("%16s", "PropertyFile" ) + " : " + propFile);
			System.out.println("   " + String.format("%16s", "Datebase" ) + " : " + database);
			System.out.println("   " + String.format("%16s", "Schema" ) + " : " + schema);
			if (option_table_name != null) { System.out.println("   " + String.format("%16s", "Table" ) + " : " + option_table_name); }
			System.out.println("   " + String.format("%16s", "ScaleFactor" ) + " : " + scale_factor);
			System.out.println("   " + String.format("%16s", "Streams" ) + " : " + number_of_streams);
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
			if (option_compression) { System.out.println("   " + String.format("%16s", "Compression" ) + " : " + option_compression); }
			if (option_randomload) { System.out.println("   " + String.format("%16s", "RandomLoad" ) + " : " + option_randomload); }
                        if (option_aligned) { System.out.println("   " + String.format("%16s", "Aligned" ) + " : " + option_aligned); }
			if ( number_of_intervals != VERY_LONG_TIME ) { System.out.println("   " + String.format("%16s", "Intervals" ) + " : " + number_of_intervals); }
			System.out.println("   " + String.format("%16s", "IntervalLength" ) + " : " + length_of_interval);
			if (option_trace) { System.out.println("   " + String.format("%16s", "Trace" ) + " : " + option_trace); }
			if (option_debug) { System.out.println("   " + String.format("%16s", "Debug" ) + " : " + option_debug); }

			String control_cqd_file_name = System.getenv("TEST_CQD_FILE");
			if ( control_cqd_file_name != null ) {
				System.out.println("		Control CQD File:	" + control_cqd_file_name);
			}

			// turn off all logging ( Phoenix is very noisy. )
//			Logger.getRootLogger().setLevel(Level.OFF);

			init_random_permutation( scale_factor * ACCOUNT_ROWS_PER_SCALE );

			if (option_createschema) { createSchema(); }
			
			if ( option_table_name != null ) {
				if (option_drop) { dropTable(option_table_name); }

				if (option_create) { createTable(option_table_name); }

				if (option_dropcreate) {
					dropTable(option_table_name);
					createTable(option_table_name);
				}

				if (option_delete) { deleteTable(option_table_name); }

				if (option_load) { loadTable(option_table_name); }

				if (option_maintain) { maintainTable(option_table_name); }

				if (option_check) { checkTable(option_table_name); }

			} else {
				for (String table: tables) {
					if (option_drop) { dropTable(table); }

					if (option_create) { createTable(table); }

					if (option_dropcreate) {
						dropTable(table);
						createTable(table);
					}

					if (option_delete) { deleteTable(table); }

					if (option_load) { loadTable(table); }

					if (option_maintain) { maintainTable(table); }

					if (option_check) { checkTable(table); }

				}

			}

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

