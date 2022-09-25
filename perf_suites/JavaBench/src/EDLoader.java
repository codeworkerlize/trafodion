
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
//import java.io.PrintStream;
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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class EDLoader extends Thread {

	// Single global value for all Streams (Threads)
	public static final int RESPONSE_BUCKETS = 101;
	public static final int MAX_CONNECT_ATTEMPTS = 100;
	public static final int VERY_LONG_TIME = 9999999;

	public static final String[] tables = { "ed_table" };

	// Powers of 2
	public static final int[] scale_rows = {
		1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024,
		2048, 4096, 8192, 16384, 32768, 65536, 131072,
		262144, 524288, 1048576, 2097152, 4194304, 8388608,
		16777216, 33554432, 67108864, 134217728, 268435456,
		536870912, 1073741824
	};

	public static final SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	public static final SimpleDateFormat hourformat = new SimpleDateFormat("HH:mm:ss");

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
	public static volatile boolean option_responsecurve = false;
	public static volatile boolean option_perstreamsummary = false;
        public static volatile boolean option_aligned = false;
        public static volatile boolean option_monarch = false;

	public static volatile int number_of_streams = 1;
	public static volatile int number_of_intervals = VERY_LONG_TIME;
	public static volatile int length_of_interval = 60;
	public static volatile int test_interval = 0;

	public static volatile int max_scale_factor;
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
	public volatile int start_row;
	public volatile int stop_row;

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

	private static int generateRandomInteger(Random generator, int lowvalue, int highvalue) {
		int randnum = generator.nextInt((highvalue - lowvalue) + 1) + lowvalue;
		return randnum;
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

				String table_name = table + "_" + String.format("%02d",scale_factor);

				if ( database.equals("hive")) {

					String table_location = null;
					if ( schema != null ) {
						table_location = "/javabench/" + schema + "/" + table_name;
						table_name = schema + "_table_" + String.format("%02d",scale_factor);
					} else {
						table_location = "/javabench/ed/" + table_name;
					}

					System.out.println("	 " + table_name + " :	" + dateformat.format(new Date()));

					sql_statement = "create external table "
						+ table_name
						+ " ("
						+ " PRIM_KEY int,"
						+ " PRIM_VALUE string,"
						+ " FOREIGN_KEY_01 int,"
						+ " FOREIGN_KEY_02 int,"
						+ " FOREIGN_KEY_03 int,"
						+ " FOREIGN_KEY_04 int,"
						+ " FOREIGN_KEY_05 int,"
						+ " FOREIGN_KEY_06 int,"
						+ " FOREIGN_KEY_07 int,"
						+ " FOREIGN_KEY_08 int,"
						+ " FOREIGN_KEY_09 int,"
						+ " FOREIGN_KEY_10 int,"
						+ " FOREIGN_KEY_11 int,"
						+ " FOREIGN_KEY_12 int,"
						+ " FOREIGN_KEY_13 int,"
						+ " FOREIGN_KEY_14 int,"
						+ " FOREIGN_KEY_15 int,"
						+ " FOREIGN_KEY_16 int,"
						+ " FOREIGN_KEY_17 int,"
						+ " FOREIGN_KEY_18 int,"
						+ " FOREIGN_KEY_19 int,"
						+ " FOREIGN_KEY_20 int,"
						+ " FOREIGN_KEY_21 int,"
						+ " FOREIGN_KEY_22 int,"
						+ " FOREIGN_KEY_23 int,"
						+ " FOREIGN_KEY_24 int,"
						+ " FOREIGN_KEY_25 int,"
						+ " FOREIGN_KEY_26 int,"
						+ " FOREIGN_KEY_27 int,"
						+ " FOREIGN_KEY_28 int,"
						+ " FOREIGN_KEY_29 int,"
						+ " FOREIGN_KEY_30 int,"
						+ " COLUMN_01 string,"
						+ " COLUMN_02 string,"
						+ " COLUMN_03 string,"
						+ " COLUMN_04 string,"
						+ " COLUMN_05 string,"
						+ " COLUMN_06 string,"
						+ " COLUMN_07 string,"
						+ " COLUMN_08 string,"
						+ " COLUMN_09 string,"
						+ " COLUMN_10 string,"
						+ " COLUMN_11 string,"
						+ " COLUMN_12 string,"
						+ " COLUMN_13 string,"
						+ " COLUMN_14 string,"
						+ " COLUMN_15 string,"
						+ " COLUMN_16 string,"
						+ " COLUMN_17 string,"
						+ " COLUMN_18 string,"
						+ " COLUMN_19 string,"
						+ " COLUMN_20 string,"
						+ " COLUMN_21 string,"
						+ " COLUMN_22 string,"
						+ " COLUMN_23 string,"
						+ " COLUMN_24 string,"
						+ " COLUMN_25 string,"
						+ " COLUMN_26 string,"
						+ " COLUMN_27 string,"
						+ " COLUMN_28 string,"
						+ " COLUMN_29 string,"
						+ " COLUMN_30 string,"
						+ " INTEGER_FACT int"
						+ " )"
						+ " row format delimited fields terminated by '|'"
						+ " stored as textfile"
						+ " location '" + table_location + "'";

				} else {

					table_name = table + "_" + String.format("%02d",scale_factor);
					if ( schema != null ) {
						table_name = schema + "." + table_name;
					}

					System.out.println("	 " + table_name + " :	" + dateformat.format(new Date()));

					sql_statement = "create table "
						+ table_name
						+ " (";

					if ( database.equals("phoenix") ) {
						sql_statement = sql_statement + " PRIM_KEY integer not null primary key,";
					} else {
						sql_statement = sql_statement + " PRIM_KEY integer not null,";
					}

					sql_statement = sql_statement
						+ " PRIM_VALUE varchar(22) not null,"
						+ " FOREIGN_KEY_01 integer not null,"
						+ " FOREIGN_KEY_02 integer not null,"
						+ " FOREIGN_KEY_03 integer not null,"
						+ " FOREIGN_KEY_04 integer not null,"
						+ " FOREIGN_KEY_05 integer not null,"
						+ " FOREIGN_KEY_06 integer not null,"
						+ " FOREIGN_KEY_07 integer not null,"
						+ " FOREIGN_KEY_08 integer not null,"
						+ " FOREIGN_KEY_09 integer not null,"
						+ " FOREIGN_KEY_10 integer not null,"
						+ " FOREIGN_KEY_11 integer not null,"
						+ " FOREIGN_KEY_12 integer not null,"
						+ " FOREIGN_KEY_13 integer not null,"
						+ " FOREIGN_KEY_14 integer not null,"
						+ " FOREIGN_KEY_15 integer not null,"
						+ " FOREIGN_KEY_16 integer not null,"
						+ " FOREIGN_KEY_17 integer not null,"
						+ " FOREIGN_KEY_18 integer not null,"
						+ " FOREIGN_KEY_19 integer not null,"
						+ " FOREIGN_KEY_20 integer not null,"
						+ " FOREIGN_KEY_21 integer not null,"
						+ " FOREIGN_KEY_22 integer not null,"
						+ " FOREIGN_KEY_23 integer not null,"
						+ " FOREIGN_KEY_24 integer not null,"
						+ " FOREIGN_KEY_25 integer not null,"
						+ " FOREIGN_KEY_26 integer not null,"
						+ " FOREIGN_KEY_27 integer not null,"
						+ " FOREIGN_KEY_28 integer not null,"
						+ " FOREIGN_KEY_29 integer not null,"
						+ " FOREIGN_KEY_30 integer not null,"
						+ " COLUMN_01 varchar(22) not null,"
						+ " COLUMN_02 varchar(22) not null,"
						+ " COLUMN_03 varchar(22) not null,"
						+ " COLUMN_04 varchar(22) not null,"
						+ " COLUMN_05 varchar(22) not null,"
						+ " COLUMN_06 varchar(22) not null,"
						+ " COLUMN_07 varchar(22) not null,"
						+ " COLUMN_08 varchar(22) not null,"
						+ " COLUMN_09 varchar(22) not null,"
						+ " COLUMN_10 varchar(22) not null,"
						+ " COLUMN_11 varchar(22) not null,"
						+ " COLUMN_12 varchar(22) not null,"
						+ " COLUMN_13 varchar(22) not null,"
						+ " COLUMN_14 varchar(22) not null,"
						+ " COLUMN_15 varchar(22) not null,"
						+ " COLUMN_16 varchar(22) not null,"
						+ " COLUMN_17 varchar(22) not null,"
						+ " COLUMN_18 varchar(22) not null,"
						+ " COLUMN_19 varchar(22) not null,"
						+ " COLUMN_20 varchar(22) not null,"
						+ " COLUMN_21 varchar(22) not null,"
						+ " COLUMN_22 varchar(22) not null,"
						+ " COLUMN_23 varchar(22) not null,"
						+ " COLUMN_24 varchar(22) not null,"
						+ " COLUMN_25 varchar(22) not null,"
						+ " COLUMN_26 varchar(22) not null,"
						+ " COLUMN_27 varchar(22) not null,"
						+ " COLUMN_28 varchar(22) not null,"
						+ " COLUMN_29 varchar(22) not null,"
						+ " COLUMN_30 varchar(22) not null,"
						+ " INTEGER_FACT integer not null";

					if ( database.equals("seaquest") || database.equals("trafodion") ) {
						sql_statement = sql_statement + " ) primary key (PRIM_KEY) hash partition";
					} else if ( database.equals("mysql") ) {
						sql_statement = sql_statement + ", primary key (PRIM_KEY) )";
					} else {
						sql_statement = sql_statement + ")";
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

					if ( option_compression) {
						sql_statement = sql_statement + " hbase_options ( compression = 'LZ4')";
					}

                                 	if ( option_aligned) {
						sql_statement = sql_statement + " attribute aligned format ";
						if ( option_monarch) {
							sql_statement = sql_statement + ", storage monarch ";
						}
					}
					if ( option_monarch) {
						sql_statement = sql_statement + " attribute storage monarch ";
					}
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

				String table_name = table + "_" + String.format("%02d",scale_factor);
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
					} else if ( database.equals("phoenix") && exception.getErrorCode() == 1012) {
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

				String table_name = table + "_" + String.format("%02d",scale_factor);
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

					String table_name = table + "_" + String.format("%02d",scale_factor);
					if ( schema != null ) {
						table_name = schema + "." + table_name;
					}

					System.out.println("	 " + table_name + " :	" + dateformat.format(new Date()));

					if (database.equals("trafodion")) {
						// sql_statement = "update statistics for table " + table_name + " on prim_key, foreign_key_10 generate 1 intervals";
						// Workaround for long running and aborting update stats statements. 5/14/2014
						 sql_statement = "update statistics for table " + table_name + " on every column";
						if ( scale_factor > 10 ) {
//							sql_statement = sql_statement + " sample 1000 rows";
							sql_statement = sql_statement + " sample";
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

				String table_name = table + "_" + String.format("%02d",scale_factor);
				if ( schema != null ) {
					table_name = schema + "." + table_name;
				}

				System.out.println("	 " + table_name + " :	" + dateformat.format(new Date()));

				int expected_rows = scale_rows[scale_factor];

				try {
					sql_statement = "select count(*) from " + table_name;
					if ( option_trace ) { System.out.println("  " + sql_statement); }
					ResultSet resultset = dbstatement.executeQuery(sql_statement);
					resultset.next();  // Expecting exactly 1 row
					int actual_rows = resultset.getInt(1);
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
		EDLoader[] stream = new EDLoader[number_of_streams];
		int threads_started=0;

		System.out.println("\n   Starting up streams : " + dateformat.format(new Date()) );

		int scale_per_stream = scale_factor / number_of_streams;
		if ( scale_per_stream == 0 ) {
			scale_per_stream = 1;
		}

		if  ( scale_factor <= 10 ) { // use just one stream
			stream[0] = new EDLoader();
			stream[0].stream_number = 0;
			stream[0].start_row = 1;
			stream[0].stop_row = scale_rows[scale_factor];
			stream[0].start();
			threads_started++;
		} else {
			for ( int strm_idx = 0; strm_idx < number_of_streams; strm_idx++ ) {
    			stream[strm_idx] = new EDLoader();
				stream[strm_idx].stream_number = strm_idx;
				stream[strm_idx].start_row = ( strm_idx * ( scale_rows[scale_factor] / number_of_streams ) + 1 );
				if ( strm_idx == (number_of_streams - 1 )) { // Last stream
					stream[strm_idx].stop_row = scale_rows[scale_factor];
				} else {
					stream[strm_idx].stop_row = stream[strm_idx].start_row + ( scale_rows[scale_factor] / number_of_streams ) - 1;
				}
				stream[strm_idx].start();
				threads_started++;
				if (option_trace) { System.out.println("  Thread " + strm_idx + "; Start_Row " + stream[strm_idx].start_row + "; Stop_Row " + stream[strm_idx].stop_row ); }

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

		while ( ( running > 0 )&& (test_interval < number_of_intervals) ) {
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
							+ " , " + String.format("%6d", interval_operations )
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
		if (option_debug) { System.out.println(stream_number + "> run(" + start_row + " , " + stop_row + ")"); }

		try {

			Connection dbconnection = null;
			String sql_statement = null;

			PreparedStatement prepstatement = null;
			long count = 0;
			long prior_count = 0;

			if (database.equals("phoenix") && ! option_upsert ) {
				option_upsert = true;  // override it set
			}

			// Initialize Random Number Generator
			Random generator = new Random();
			generator.setSeed(stream_number * start_row * 10000003 + 1234567);

			// Tell the main thread that I'm ready to proceed
			signal_ready = true;
			while (!signal_proceed){
				Thread.sleep(10);
			}

			for (String table: tables) {

				String table_name = table + "_" + String.format("%02d",scale_factor);
				if ( schema != null ) {
					table_name = schema + "." + table_name;
				}

				if (database.equals("hive")) {
					//  For hive, we will load flat files into Hadoop

					Configuration conf = new Configuration();
					FileSystem fs = FileSystem.get(conf);

					String data_file_name = "/javabench/ed/" + table_name + "/" + table_name + "." + String.format("%02d",stream_number) + ".dat";

					if (option_trace) { System.out.println(stream_number + ">  Writing data to file: " + data_file_name); }

					Path data_file_path = new Path(data_file_name);

					try {
						if (fs.exists (data_file_path)) {
							fs.delete(data_file_path,true);
						}
					} catch (Exception exception) {
						//ignore
					}

					FSDataOutputStream data_file_stream = fs.create(data_file_path);

					for ( int idx = start_row; idx <= stop_row; idx++ ) {

						if ( signal_stop ) {
							idx = stop_row + 1;  //stop loading
						} else {

							StringBuilder ed_record = new StringBuilder(idx + "|");
							ed_record.append(idx + "of" + scale_rows[scale_factor] + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[1]) + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[2]) + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[3]) + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[4]) + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[5]) + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[6]) + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[7]) + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[8]) + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[9]) + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[10]) + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[11]) + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[12]) + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[13]) + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[14]) + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[15]) + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[16]) + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[17]) + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[18]) + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[19]) + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[20]) + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[21]) + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[22]) + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[23]) + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[24]) + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[25]) + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[26]) + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[27]) + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[28]) + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[29]) + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[30]) + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[1]) + "of" + scale_rows[1] + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[2]) + "of" + scale_rows[2] + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[3]) + "of" + scale_rows[3] + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[4]) + "of" + scale_rows[4] + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[5]) + "of" + scale_rows[5] + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[6]) + "of" + scale_rows[6] + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[7]) + "of" + scale_rows[7] + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[8]) + "of" + scale_rows[8] + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[9]) + "of" + scale_rows[9] + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[10]) + "of" + scale_rows[10] + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[11]) + "of" + scale_rows[11] + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[12]) + "of" + scale_rows[12] + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[13]) + "of" + scale_rows[13] + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[14]) + "of" + scale_rows[14] + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[15]) + "of" + scale_rows[15] + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[16]) + "of" + scale_rows[16] + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[17]) + "of" + scale_rows[17] + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[18]) + "of" + scale_rows[18] + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[19]) + "of" + scale_rows[19] + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[20]) + "of" + scale_rows[20] + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[21]) + "of" + scale_rows[21] + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[22]) + "of" + scale_rows[22] + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[23]) + "of" + scale_rows[23] + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[24]) + "of" + scale_rows[24] + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[25]) + "of" + scale_rows[25] + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[26]) + "of" + scale_rows[26] + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[27]) + "of" + scale_rows[27] + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[28]) + "of" + scale_rows[28] + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[29]) + "of" + scale_rows[29] + "|");
							ed_record.append(generateRandomInteger(generator,1,scale_rows[30]) + "of" + scale_rows[30] + "|");
							ed_record.append(0 + "\n");

							long statement_start_time = System.nanoTime();
							data_file_stream.writeBytes(ed_record.toString());
							long statement_elapsed_micro = ( System.nanoTime() - statement_start_time ) / 1000;

							stream_accumlated_transactions++;
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

					}

					data_file_stream.flush();
					data_file_stream.close();

				} else {

					dbconnection= establishDatabaseConnection();

					count = 0;  prior_count = 0;

					if ( option_upsert ) {
						sql_statement = "upsert";
					} else {
						sql_statement = "insert";
					}
					if ( option_usingload ) {
						sql_statement = sql_statement + " using load";
					}
					sql_statement = sql_statement + " into " + table_name + " ("
						+ " PRIM_KEY,"
						+ " PRIM_VALUE,"
						+ " FOREIGN_KEY_01,"
						+ " FOREIGN_KEY_02,"
						+ " FOREIGN_KEY_03,"
						+ " FOREIGN_KEY_04,"
						+ " FOREIGN_KEY_05,"
						+ " FOREIGN_KEY_06,"
						+ " FOREIGN_KEY_07,"
						+ " FOREIGN_KEY_08,"
						+ " FOREIGN_KEY_09,"
						+ " FOREIGN_KEY_10,"
						+ " FOREIGN_KEY_11,"
						+ " FOREIGN_KEY_12,"
						+ " FOREIGN_KEY_13,"
						+ " FOREIGN_KEY_14,"
						+ " FOREIGN_KEY_15,"
						+ " FOREIGN_KEY_16,"
						+ " FOREIGN_KEY_17,"
						+ " FOREIGN_KEY_18,"
						+ " FOREIGN_KEY_19,"
						+ " FOREIGN_KEY_20,"
						+ " FOREIGN_KEY_21,"
						+ " FOREIGN_KEY_22,"
						+ " FOREIGN_KEY_23,"
						+ " FOREIGN_KEY_24,"
						+ " FOREIGN_KEY_25,"
						+ " FOREIGN_KEY_26,"
						+ " FOREIGN_KEY_27,"
						+ " FOREIGN_KEY_28,"
						+ " FOREIGN_KEY_29,"
						+ " FOREIGN_KEY_30,"
						+ " COLUMN_01,"
						+ " COLUMN_02,"
						+ " COLUMN_03,"
						+ " COLUMN_04,"
						+ " COLUMN_05,"
						+ " COLUMN_06,"
						+ " COLUMN_07,"
						+ " COLUMN_08,"
						+ " COLUMN_09,"
						+ " COLUMN_10,"
						+ " COLUMN_11,"
						+ " COLUMN_12,"
						+ " COLUMN_13,"
						+ " COLUMN_14,"
						+ " COLUMN_15,"
						+ " COLUMN_16,"
						+ " COLUMN_17,"
						+ " COLUMN_18,"
						+ " COLUMN_19,"
						+ " COLUMN_20,"
						+ " COLUMN_21,"
						+ " COLUMN_22,"
						+ " COLUMN_23,"
						+ " COLUMN_24,"
						+ " COLUMN_25,"
						+ " COLUMN_26,"
						+ " COLUMN_27,"
						+ " COLUMN_28,"
						+ " COLUMN_29,"
						+ " COLUMN_30,"
						+ " INTEGER_FACT"
						+ " ) values ( "
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?,"
						+ " ?"
						+ " )";

					if ( option_trace ) { System.out.println("  " + sql_statement); }
					prepstatement = dbconnection.prepareStatement(sql_statement);

					int random_prim_key = -1;
					if ( option_randomload ) {
						init_random_permutation(scale_rows[scale_factor]);
						random_prim_key = position_random_permutation( start_row );
					}
					if ( option_debug) { System.out.println(stream_number + ">  Range " + start_row + " to " + stop_row ); }

					for ( int idx = start_row; idx <= stop_row; idx++ ) {

						if ( signal_stop ) {
							idx = stop_row + 1;  //stop loading
						} else {
							if ( option_randomload ) {
								random_prim_key = next_random_permutation( random_prim_key );
								prepstatement.setInt(1,random_prim_key);
							} else {
								prepstatement.setInt(1,idx);
							}
							prepstatement.setString(2,idx + "of" + scale_rows[scale_factor]);
							prepstatement.setInt(3,generateRandomInteger(generator,1,scale_rows[1]));
							prepstatement.setInt(4,generateRandomInteger(generator,1,scale_rows[2]));
							prepstatement.setInt(5,generateRandomInteger(generator,1,scale_rows[3]));
							prepstatement.setInt(6,generateRandomInteger(generator,1,scale_rows[4]));
							prepstatement.setInt(7,generateRandomInteger(generator,1,scale_rows[5]));
							prepstatement.setInt(8,generateRandomInteger(generator,1,scale_rows[6]));
							prepstatement.setInt(9,generateRandomInteger(generator,1,scale_rows[7]));
							prepstatement.setInt(10,generateRandomInteger(generator,1,scale_rows[8]));
							prepstatement.setInt(11,generateRandomInteger(generator,1,scale_rows[9]));
							prepstatement.setInt(12,generateRandomInteger(generator,1,scale_rows[10]));
							prepstatement.setInt(13,generateRandomInteger(generator,1,scale_rows[11]));
							prepstatement.setInt(14,generateRandomInteger(generator,1,scale_rows[12]));
							prepstatement.setInt(15,generateRandomInteger(generator,1,scale_rows[13]));
							prepstatement.setInt(16,generateRandomInteger(generator,1,scale_rows[14]));
							prepstatement.setInt(17,generateRandomInteger(generator,1,scale_rows[15]));
							prepstatement.setInt(18,generateRandomInteger(generator,1,scale_rows[16]));
							prepstatement.setInt(19,generateRandomInteger(generator,1,scale_rows[17]));
							prepstatement.setInt(20,generateRandomInteger(generator,1,scale_rows[18]));
							prepstatement.setInt(21,generateRandomInteger(generator,1,scale_rows[19]));
							prepstatement.setInt(22,generateRandomInteger(generator,1,scale_rows[20]));
							prepstatement.setInt(23,generateRandomInteger(generator,1,scale_rows[21]));
							prepstatement.setInt(24,generateRandomInteger(generator,1,scale_rows[22]));
							prepstatement.setInt(25,generateRandomInteger(generator,1,scale_rows[23]));
							prepstatement.setInt(26,generateRandomInteger(generator,1,scale_rows[24]));
							prepstatement.setInt(27,generateRandomInteger(generator,1,scale_rows[25]));
							prepstatement.setInt(28,generateRandomInteger(generator,1,scale_rows[26]));
							prepstatement.setInt(29,generateRandomInteger(generator,1,scale_rows[27]));
							prepstatement.setInt(30,generateRandomInteger(generator,1,scale_rows[28]));
							prepstatement.setInt(31,generateRandomInteger(generator,1,scale_rows[29]));
							prepstatement.setInt(32,generateRandomInteger(generator,1,scale_rows[30]));
							prepstatement.setString(33,generateRandomInteger(generator,1,scale_rows[1]) + "of" + scale_rows[1]);
							prepstatement.setString(34,generateRandomInteger(generator,1,scale_rows[2]) + "of" + scale_rows[2]);
							prepstatement.setString(35,generateRandomInteger(generator,1,scale_rows[3]) + "of" + scale_rows[3]);
							prepstatement.setString(36,generateRandomInteger(generator,1,scale_rows[4]) + "of" + scale_rows[4]);
							prepstatement.setString(37,generateRandomInteger(generator,1,scale_rows[5]) + "of" + scale_rows[5]);
							prepstatement.setString(38,generateRandomInteger(generator,1,scale_rows[6]) + "of" + scale_rows[6]);
							prepstatement.setString(39,generateRandomInteger(generator,1,scale_rows[7]) + "of" + scale_rows[7]);
							prepstatement.setString(40,generateRandomInteger(generator,1,scale_rows[8]) + "of" + scale_rows[8]);
							prepstatement.setString(41,generateRandomInteger(generator,1,scale_rows[9]) + "of" + scale_rows[9]);
							prepstatement.setString(42,generateRandomInteger(generator,1,scale_rows[10]) + "of" + scale_rows[10]);
							prepstatement.setString(43,generateRandomInteger(generator,1,scale_rows[11]) + "of" + scale_rows[11]);
							prepstatement.setString(44,generateRandomInteger(generator,1,scale_rows[12]) + "of" + scale_rows[12]);
							prepstatement.setString(45,generateRandomInteger(generator,1,scale_rows[13]) + "of" + scale_rows[13]);
							prepstatement.setString(46,generateRandomInteger(generator,1,scale_rows[14]) + "of" + scale_rows[14]);
							prepstatement.setString(47,generateRandomInteger(generator,1,scale_rows[15]) + "of" + scale_rows[15]);
							prepstatement.setString(48,generateRandomInteger(generator,1,scale_rows[16]) + "of" + scale_rows[16]);
							prepstatement.setString(49,generateRandomInteger(generator,1,scale_rows[17]) + "of" + scale_rows[17]);
							prepstatement.setString(50,generateRandomInteger(generator,1,scale_rows[18]) + "of" + scale_rows[18]);
							prepstatement.setString(51,generateRandomInteger(generator,1,scale_rows[19]) + "of" + scale_rows[19]);
							prepstatement.setString(52,generateRandomInteger(generator,1,scale_rows[20]) + "of" + scale_rows[20]);
							prepstatement.setString(53,generateRandomInteger(generator,1,scale_rows[21]) + "of" + scale_rows[21]);
							prepstatement.setString(54,generateRandomInteger(generator,1,scale_rows[22]) + "of" + scale_rows[22]);
							prepstatement.setString(55,generateRandomInteger(generator,1,scale_rows[23]) + "of" + scale_rows[23]);
							prepstatement.setString(56,generateRandomInteger(generator,1,scale_rows[24]) + "of" + scale_rows[24]);
							prepstatement.setString(57,generateRandomInteger(generator,1,scale_rows[25]) + "of" + scale_rows[25]);
							prepstatement.setString(58,generateRandomInteger(generator,1,scale_rows[26]) + "of" + scale_rows[26]);
							prepstatement.setString(59,generateRandomInteger(generator,1,scale_rows[27]) + "of" + scale_rows[27]);
							prepstatement.setString(60,generateRandomInteger(generator,1,scale_rows[28]) + "of" + scale_rows[28]);
							prepstatement.setString(61,generateRandomInteger(generator,1,scale_rows[29]) + "of" + scale_rows[29]);
							prepstatement.setString(62,generateRandomInteger(generator,1,scale_rows[30]) + "of" + scale_rows[30]);
							prepstatement.setInt(63,0);
							prepstatement.addBatch();

							if ( ++count % batchsize == 0 ) {
								long statement_start_time = System.nanoTime();
								prepstatement.executeBatch();	// Execute Batch
								long statement_elapsed_micro = ( System.nanoTime() - statement_start_time ) / 1000;
								prepstatement.clearBatch();

								stream_accumlated_transactions = stream_accumlated_transactions +  (count - prior_count);
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

					dbconnection.close();
					dbconnection = null;

				}
			}

		} catch (SQLException exception) {
			synchronized (stdoutlock) {
				System.out.println("\n" + stream_number + "> ----------------------------------------------------------");
				System.out.println(stream_number + "> SQLEXCEPTION : Error Code = " + exception.getErrorCode() );
				System.out.println(stream_number + ">   Stream Number : " + stream_number );
				SQLException nextException = exception;
				do {
					System.out.println(stream_number + ">  " + nextException.getMessage());
				} while ((nextException = nextException.getNextException()) != null);
				System.out.println(stream_number + "> ----------------------------------------------------------\n");
			}
		} catch (Exception exception) {
			synchronized (stdoutlock) {
				System.out.println("\n" + stream_number + "> ----------------------------------------------------------");
				System.out.println(stream_number + "> EXCEPTION : " + exception.getMessage());
				System.out.println(stream_number + ">   Stream Number : " + stream_number );
			    exception.printStackTrace();
				System.out.println(stream_number + "> ----------------------------------------------------------\n");
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
		System.out.println("EDLoader " + command_line);

		try {

			String testid = System.getenv("TESTID");
			String driverid = System.getenv("DRIVERID");
			String logdirectory = System.getenv("LOGDIRECTORY");

			if ( testid != null ) {
				System.setProperty("logfile.name", logdirectory + "/" + testid + ".driver_" + driverid + ".log4j.log");
			} else {
				System.setProperty("logfile.name", "log/load.ed.log4j.log");
			}
			logger = Logger.getLogger("JavaBench");

			
			String syntax = "\n"
				+ "EDLoader: EDLoader <scale_factor> <load_scope> [<load_option>]\n"
				+ "   <scale_factor> : positive integer representing largest generated table (2^n rows)\n"

				+ "   <load_scope> : \n"
				+ "     all : load all tables in scale \n"
				+ "     table : load largest table in scale \n"

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
				+ "     compression : Create the table with compression.\n"
				+ "     aligned : Create each table with aligned format attribute.\n"
                                + "     monarch : Create each table with monarch storage  attribute.\n"
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

			int max_scale_factor = Integer.parseInt(args[0]);
			if ( max_scale_factor <= 0 ) {
				System.out.println(syntax);
				throw new Exception("ERROR : Invalid scale_factor Provided. ( scale_factor = " + max_scale_factor + " )");
			}

			String load_scope = args[1];
			switch (load_scope) {
				case "all" : break;
				case "table" : break;
				default: {
					System.out.println(syntax);
					throw new Exception("ERROR : Invalid load_scope specified.  ( load_scope = " + load_scope + " )");
				}
			}

			String option = null;

			for ( int indx = 2; indx < args.length; indx++ ) {

				option = args[indx];
				switch ( option ) {
					case "schema": schema = args[++indx]; break;
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
					case "aligned": option_aligned = true; break;
					case "monarch": option_monarch = true; break;
					case "randomload": option_randomload = true; break;
					case "intervals": number_of_intervals = Integer.parseInt(args[++indx]); break;
					case "intervallength": length_of_interval = Integer.parseInt(args[++indx]); break;
					case "trace": option_trace = true; break;
					case "debug": option_debug = true; break;
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

			System.out.println("\nEDLoader");

			System.out.println("\n   " + String.format("%16s", "Load Starting" ) + " : " + dateformat.format(new Date()));

			System.out.println("   " + String.format("%16s", "PropertyFile" ) + " : " + propFile);
			System.out.println("   " + String.format("%16s", "Datebase" ) + " : " + database);
			System.out.println("   " + String.format("%16s", "Schema" ) + " : " + schema);
			System.out.println("   " + String.format("%16s", "ScaleFactor" ) + " : " + scale_factor);
			System.out.println("   " + String.format("%16s", "Streams" ) + " : " + number_of_streams);

			System.out.println("   " + String.format("%16s", "LoadScope" ) + " : " + load_scope);

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
			if (option_aligned) { System.out.println("   " + String.format("%16s", "Aligned" ) + " : " + option_aligned); }
			if (option_monarch) { System.out.println("   " + String.format("%16s", "Monarch" ) + " : " + option_monarch); }
			if (option_randomload) { System.out.println("   " + String.format("%16s", "RandomLoad" ) + " : " + option_randomload); }

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

			int start_scale = 1;

			if ( load_scope.equals("table")) {
				start_scale = max_scale_factor;
			}

			if (option_createschema) { createSchema(); }

			for (int scale_idx = start_scale; scale_idx <= max_scale_factor; scale_idx++) {

				scale_factor = scale_idx;

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

