
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
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class OrderEntryLoader extends Thread {

	// Single global value for all Streams (Threads)
	public static final int RESPONSE_BUCKETS = 101;
	public static final int MAX_CONNECT_ATTEMPTS = 100;

	public static String[] tables = { "item", "warehouse", "stock", "district", "customer", "orders", "orderline", "neworder", "history" };

	public static final int ITEM_ROWS_PER_SCALE = 100000;  // Fixed value
	public static final int WAREHOUSE_ROWS_PER_SCALE = 1;
	public static final int STOCK_ROWS_PER_SCALE = 100000;
	public static final int DISTRICT_ROWS_PER_SCALE = 10;
	public static final int CUSTOMER_ROWS_PER_SCALE = 30000;
	public static final int HISTORY_ROWS_PER_SCALE = 30000;
	public static final int ORDERS_ROWS_PER_SCALE = 30000;
	public static final int ORDERLINE_ROWS_PER_SCALE = 300000;
	public static final int NEWORDER_ROWS_PER_SCALE = 9000;

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
	public static volatile int number_of_streams = 1;
	public static volatile int length_of_interval = 60;
	public static volatile int timer_minutes = 0;
	public static volatile boolean timer_pop = false;

	public static volatile int scale_factor;
	public static volatile int start_sf = -1;
	public static volatile int end_sf = -1;

	public static volatile String customer_table_name = null;
	public static volatile String district_table_name = null;
	public static volatile String history_table_name = null;
	public static volatile String item_table_name = null;
	public static volatile String neworder_table_name = null;
	public static volatile String orderline_table_name = null;
	public static volatile String orders_table_name = null;
	public static volatile String stock_table_name = null;
	public static volatile String warehouse_table_name = null;


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
	public volatile long stream_accumlated_rows_added = 0;
	public volatile long stream_accumlated_response_time = 0;
	public volatile long stream_maximum_response_time = 0;
	public volatile long stream_minimum_response_time = 999999999999999999L;
	public volatile int[] stream_response_distribution = new int[RESPONSE_BUCKETS];
	public volatile int stream_number;
	public volatile int start_scale;
	public volatile int stop_scale;

	public volatile boolean signal_ready = false;
	public volatile boolean signal_proceed = false;
	public volatile boolean signal_stop = false;

	static Logger logger = null;


//----------------------------------------------------------------------------------------------------------

	static int generateRandomInteger(Random generator, int lowvalue, int highvalue) {
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

	static String generateRandomNString (Random generator, int strlength) {
		final String alphabet = "0123456789";
		final int N = alphabet.length();
		String random_string = "";
		for ( int indx = 0; indx < strlength; indx++ ) {
			random_string = random_string + alphabet.charAt(generator.nextInt(N));
		}
		return random_string;
	}

//----------------------------------------------------------------------------------------------------------

	static String generateRandomNStringORIGINAL (Random generator, int orig_str_length) throws Exception {
		String new_string = "";
		if ( generateRandomInteger(generator,1,100) > 10 ) {
			new_string = generateRandomAString(generator,orig_str_length);
		} else {
			// 10% of rows have string ORIGINAL at a random position
			int original_position = generateRandomInteger(generator,1,(orig_str_length-8));
			new_string = generateRandomAString(generator,original_position) + "ORIGINAL" + generateRandomAString(generator,original_position-orig_str_length-8);
		}
		return new_string;
	}

//----------------------------------------------------------------------------------------------------------

	static int NURand(Random generator, int A, int x, int y, int C) throws Exception {
		return (((generateRandomInteger(generator,0,A) | generateRandomInteger(generator,x,y)) + C) % ( y - x + 1)) + x;
	}

//----------------------------------------------------------------------------------------------------------

	static String generateRandomLastName(Random generator, int cust_id) throws Exception
	{
		String[] substrings = { "BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING" };
		String last_name = substrings[(cust_id/100)%100] + substrings[(cust_id/10)%10]  + substrings[cust_id%10];
		return last_name;
	}

//----------------------------------------------------------------------------------------------------------

	static int[] generateRandomIntPermutation(Random generator, int length) throws Exception {
		int[] a = new int[length];
		for (int idx=0; idx<length; idx++) {
			a[idx]=idx;
		}
		int c;
		int idx1;
		int idx2;
		for (int idx=0; idx<100000; idx++) {
			idx1=generateRandomInteger(generator,0,length-1);
			idx2=generateRandomInteger(generator,0,length-1);
			c = a[idx1]; a[idx1] = a[idx2]; a[idx2] = c;
		}
		return a;
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

	static void createTable() throws Exception {

		if (option_debug) { System.out.println("> createTable()"); }

		long taskStartTime = System.currentTimeMillis();
		System.out.println("\n   Create Table : " + dateformat.format(new Date()));

		try {

			Connection dbconnection = establishDatabaseConnection();
			Statement dbstatement = dbconnection.createStatement();
			String sql_statement = null;

			for (String table: tables) {


				switch ( table ) {

					case "item":

						System.out.println("	 " + item_table_name + " :	" + dateformat.format(new Date()));

						sql_statement = "create table " + item_table_name + "( "
							+ "I_ID integer not null, "
							+ "I_IM_ID integer not null, "
							+ "I_NAME varchar(24) not null, "
							+ "I_PRICE numeric(5,2) not null, "
							+ "I_DATA varchar(50) not null";
						if ( database.equals("seaquest") ) {
							sql_statement = sql_statement + " ) primary key (I_ID) hash partition";
						} else if ( database.equals("trafodion") ) {
							sql_statement = sql_statement + " ) primary key (I_ID) ";
						} else if ( database.equals("mysql") ) {
							sql_statement = sql_statement + ", primary key (I_ID) )";
						} else {
							sql_statement = sql_statement + ")";
						}
						if ( option_salt ) {
							if ( database.equals("trafodion") ) {
								sql_statement = sql_statement + " salt using " + saltsize + " partitions";
							} else if ( database.equals("mysql") ) {
								sql_statement = sql_statement + " partition by key ()  partitions " + saltsize ;
							}
						}

						break;

					case "warehouse":

						System.out.println("	 " + warehouse_table_name + " :	" + dateformat.format(new Date()));

						sql_statement = "create table " + warehouse_table_name + "( "
							+ "W_ID integer not null, "
							+ "W_NAME varchar(10) not null, "
							+ "W_STREET_1 varchar(20) not null, "
							+ "W_STREET_2 varchar(20) not null, "
							+ "W_CITY varchar(20) not null, "
							+ "W_STATE char(2) not null, "
							+ "W_ZIP char(9) not null, "
							+ "W_TAX numeric(4,4) not null, "
							+ "W_YTD numeric(12,2) not null";
						if ( database.equals("seaquest") ) {
							sql_statement = sql_statement + " ) primary key (W_ID) hash partition";
						} else if ( database.equals("trafodion") ) {
							sql_statement = sql_statement + " ) primary key (W_ID) ";
						} else if ( database.equals("mysql") ) {
							sql_statement = sql_statement + ", primary key (W_ID) )";
						} else {
							sql_statement = sql_statement + ")";
						}
						if ( option_salt ) {
							if ( database.equals("trafodion") ) {
								sql_statement = sql_statement + " salt using " + saltsize + " partitions";
							} else if ( database.equals("mysql") ) {
								sql_statement = sql_statement + " partition by key ()  partitions " + saltsize ;
							}
						}

						break;

					case "stock":

						System.out.println("	 " + stock_table_name + " :	" + dateformat.format(new Date()));

						sql_statement = "create table " + stock_table_name + "("
							+ "S_I_ID integer not null, "
							+ "S_W_ID integer not null, "
							+ "S_QUANTITY numeric(4) signed not null, "
							+ "S_DIST_01 char(24) not null, "
							+ "S_DIST_02 char(24) not null, "
							+ "S_DIST_03 char(24) not null, "
							+ "S_DIST_04 char(24) not null, "
							+ "S_DIST_05 char(24) not null, "
							+ "S_DIST_06 char(24) not null, "
							+ "S_DIST_07 char(24) not null, "
							+ "S_DIST_08 char(24) not null, "
							+ "S_DIST_09 char(24) not null, "
							+ "S_DIST_10 char(24) not null, "
							+ "S_YTD numeric(8) not null, "
							+ "S_ORDER_CNT numeric(4) not null, "
							+ "S_REMOTE_CNT numeric(4) not null, "
							+ "S_DATA varchar(50) not null";
						if ( database.equals("seaquest") ) {
							sql_statement = sql_statement + " ) primary key (S_W_ID, S_I_ID) hash partition";
						} else if ( database.equals("trafodion") ) {
							sql_statement = sql_statement + " ) primary key (S_W_ID, S_I_ID) ";
						} else if ( database.equals("mysql") ) {
							sql_statement = sql_statement + ", primary key (S_W_ID, S_I_ID) )";
						} else {
							sql_statement = sql_statement + ")";
						}
						if ( option_salt ) {
							if ( database.equals("trafodion") ) {
								sql_statement = sql_statement + " salt using " + saltsize + " partitions on (S_W_ID)";
							} else if ( database.equals("mysql") ) {
								sql_statement = sql_statement + " partition by key ()  partitions " + saltsize ;
							}
						}

						break;

					case "district":

						System.out.println("	 " + district_table_name + " :	" + dateformat.format(new Date()));

						sql_statement = "create table " + district_table_name + "("
							+ "D_ID integer not null, "
							+ "D_W_ID integer not null, "
							+ "D_NAME varchar(10) not null, "
							+ "D_STREET_1 varchar(20) not null, "
							+ "D_STREET_2 varchar(20) not null, "
							+ "D_CITY varchar(20) not null, "
							+ "D_STATE char(2) not null, "
							+ "D_ZIP char(9) not null, "
							+ "D_TAX numeric(4,4) not null, "
							+ "D_YTD numeric(12,2) not null, "
							+ "D_NEXT_O_ID integer not null";
						if ( database.equals("seaquest") ) {
							sql_statement = sql_statement + " ) primary key (D_W_ID, D_ID) hash partition";
						} else if ( database.equals("trafodion") ) {
							sql_statement = sql_statement + " ) primary key (D_W_ID, D_ID) ";
						} else if ( database.equals("mysql") ) {
							sql_statement = sql_statement + ", primary key (D_W_ID, D_ID) )";
						} else {
							sql_statement = sql_statement + ")";
						}
						if ( option_salt ) {
							if ( database.equals("phoenix") ) {
								sql_statement = sql_statement + " SALT_BUCKETS=" + saltsize;
							} else if ( database.equals("trafodion") ) {
								sql_statement = sql_statement + " salt using " + saltsize + " partitions on (D_W_ID)";
							} else if ( database.equals("mysql") ) {
								sql_statement = sql_statement + " partition by key ()  partitions " + saltsize ;
							}
						}

						break;

					case "customer":

						System.out.println("	 " + customer_table_name + " :	" + dateformat.format(new Date()));

						sql_statement = "create table " + customer_table_name + "("
							+ "C_ID integer not null, "
							+ "C_D_ID integer not null, "
							+ "C_W_ID integer not null, "
							+ "C_FIRST varchar(16) not null, "
							+ "C_MIDDLE char(2) not null, "
							+ "C_LAST varchar(16) not null, "
							+ "C_STREET_1 varchar(20) not null, "
							+ "C_STREET_2 varchar(20) not null, "
							+ "C_CITY varchar(20) not null, "
							+ "C_STATE char(2) not null, "
							+ "C_ZIP char(9) not null, "
							+ "C_PHONE char(16) not null, "
							+ "C_SINCE timestamp not null, "
							+ "C_CREDIT char(2) not null, "
							+ "C_CREDIT_LIM numeric(12,2) not null, "
							+ "C_DISCOUNT numeric(4,4) not null, "
							+ "C_BALANCE numeric(12,2) not null, "
							+ "C_YTD_PAYMENT numeric(12,2) not null, "
							+ "C_PAYMENT_CNT numeric(4) not null, "
							+ "C_DELIVERY_CNT numeric(4) not null, "
							+ "C_DATA varchar(500) not null";
						if ( database.equals("seaquest") ) {
							sql_statement = sql_statement + " ) primary key (C_W_ID, C_D_ID, C_ID) hash partition";
						} else if ( database.equals("trafodion") ) {
							sql_statement = sql_statement + " ) primary key (C_W_ID, C_D_ID, C_ID) ";
						} else if ( database.equals("mysql") ) {
							sql_statement = sql_statement + ", primary key (C_W_ID, C_D_ID, C_ID) )";
						} else {
							sql_statement = sql_statement + ")";
						}
						if ( option_salt ) {
							if ( database.equals("phoenix") ) {
								sql_statement = sql_statement + " SALT_BUCKETS=" + saltsize;
							} else if ( database.equals("trafodion") ) {
								sql_statement = sql_statement + " salt using " + saltsize + " partitions on (C_W_ID)";
							} else if ( database.equals("mysql") ) {
								sql_statement = sql_statement + " partition by key ()  partitions " + saltsize ;
							}
						}

						break;

					case "history":

						System.out.println("	 " + history_table_name + " :	" + dateformat.format(new Date()));

						sql_statement = "create table " + history_table_name + "("
							+ "H_DATE timestamp(6) not null, "
							+ "H_W_ID integer not null, "
							+ "H_D_ID integer not null, "
							+ "H_C_W_ID integer not null, "
							+ "H_C_D_ID integer not null, "
							+ "H_C_ID integer not null, "
							+ "H_AMOUNT numeric(6,2) not null, "
							+ "H_DATA varchar(24) not null";
						if ( database.equals("seaquest") ) {
							sql_statement = sql_statement + " ) primary key ( H_DATE, H_W_ID, H_D_ID, H_C_W_ID, H_C_D_ID, H_C_ID ) hash partition";
						} else if ( database.equals("trafodion") ) {
							sql_statement = sql_statement + " ) primary key ( H_DATE, H_W_ID, H_D_ID, H_C_W_ID, H_C_D_ID, H_C_ID ) ";
						} else if ( database.equals("mysql") ) {
							sql_statement = sql_statement + ", primary key ( H_DATE, H_W_ID, H_D_ID, H_C_W_ID, H_C_D_ID, H_C_ID ) )";
						} else {
							sql_statement = sql_statement + ")";
						}
						if ( option_salt ) {
							if ( database.equals("phoenix") ) {
								sql_statement = sql_statement + " SALT_BUCKETS=" + saltsize;
							} else if ( database.equals("trafodion") ) {
								sql_statement = sql_statement + " salt using " + saltsize + " partitions on (H_W_ID)";
							} else if ( database.equals("mysql") ) {
								sql_statement = sql_statement + " partition by key ()  partitions " + saltsize ;
							}
						}

						break;

					case "orders":

						System.out.println("	 " + orders_table_name + " :	" + dateformat.format(new Date()));

						sql_statement = "create table " + orders_table_name + "("
							+ "O_ID integer not null, "
							+ "O_D_ID integer not null, "
							+ "O_W_ID integer not null, "
							+ "O_C_ID integer not null, "
							+ "O_ENTRY_D timestamp not null, "
							+ "O_CARRIER_ID integer, "
							+ "O_OL_CNT numeric(2), "
							+ "O_ALL_LOCAL numeric(1) not null";
						if ( database.equals("seaquest") ) {
							sql_statement = sql_statement + " ) primary key ( O_W_ID, O_D_ID, O_ID ) hash partition";
						} else if ( database.equals("trafodion") ) {
							sql_statement = sql_statement + " ) primary key ( O_W_ID, O_D_ID, O_ID ) ";
						} else if ( database.equals("mysql") ) {
							sql_statement = sql_statement + ", primary key ( O_W_ID, O_D_ID, O_ID ) )";
						} else {
							sql_statement = sql_statement + ")";
						}
						if ( option_salt ) {
							if ( database.equals("phoenix") ) {
								sql_statement = sql_statement + " SALT_BUCKETS=" + saltsize;
							} else if ( database.equals("trafodion") ) {
								sql_statement = sql_statement + " salt using " + saltsize + " partitions on (O_W_ID)";
							} else if ( database.equals("mysql") ) {
								sql_statement = sql_statement + " partition by key ()  partitions " + saltsize ;
							}
						}

						break;

					case "orderline":

						System.out.println("	 " + orderline_table_name + " :	" + dateformat.format(new Date()));

						sql_statement = "create table " + orderline_table_name + "("
							+ "OL_O_ID integer not null, "
							+ "OL_D_ID integer not null, "
							+ "OL_W_ID integer not null, "
							+ "OL_NUMBER integer not null, "
							+ "OL_I_ID integer not null, "
							+ "OL_SUPPLY_W_ID integer not null, "
							+ "OL_DELIVERY_D timestamp, "
							+ "OL_QUANTITY numeric(2) not null, "
							+ "OL_AMOUNT numeric(6,2) not null, "
							+ "OL_DIST_INFO char(24) not null";
						if ( database.equals("seaquest") ) {
							sql_statement = sql_statement + " ) primary key ( OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER ) hash partition";
						} else if ( database.equals("trafodion") ) {
							sql_statement = sql_statement + " ) primary key ( OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER ) ";
						} else if ( database.equals("mysql") ) {
							sql_statement = sql_statement + ", primary key ( OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER ) )";
						} else {
							sql_statement = sql_statement + ")";
						}
						if ( option_salt ) {
							if ( database.equals("phoenix") ) {
								sql_statement = sql_statement + " SALT_BUCKETS=" + saltsize;
							} else if ( database.equals("trafodion") ) {
								sql_statement = sql_statement + " salt using " + saltsize + " partitions on (OL_W_ID)";
							} else if ( database.equals("mysql") ) {
								sql_statement = sql_statement + " partition by key ()  partitions " + saltsize ;
							}
						}

						break;

					case "neworder":

						System.out.println("	 " + neworder_table_name + " :	" + dateformat.format(new Date()));

						sql_statement = "create table " + neworder_table_name + "("
							+ "NO_O_ID integer not null, "
							+ "NO_D_ID integer not null, "
							+ "NO_W_ID integer not null";
						if ( database.equals("seaquest") ) {
							sql_statement = sql_statement + " ) primary key ( NO_W_ID, NO_D_ID, NO_O_ID ) hash partition";
						} else if ( database.equals("trafodion") ) {
							sql_statement = sql_statement + " ) primary key ( NO_W_ID, NO_D_ID, NO_O_ID ) ";
						} else if ( database.equals("mysql") ) {
							sql_statement = sql_statement + ", primary key ( NO_W_ID, NO_D_ID, NO_O_ID ) )";
						} else {
							sql_statement = sql_statement + ")";
						}
						if ( option_salt ) {
							if ( database.equals("phoenix") ) {
								sql_statement = sql_statement + " SALT_BUCKETS=" + saltsize;
							} else if ( database.equals("trafodion") ) {
								sql_statement = sql_statement + " salt using " + saltsize + " partitions on (NO_W_ID)";
							} else if ( database.equals("mysql") ) {
								sql_statement = sql_statement + " partition by key ()  partitions " + saltsize ;
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

				String table_name = "oe_" + table + "_" + scale_factor;
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
					if ( database.equals("trafodion") && ( exception.getErrorCode() == -1389) ) {
						// expected table does not exist error; ignore
					} else if  ( database.equals("seaquest") && ( exception.getErrorCode() == -1004) ) {
						// expected table does not exist error; ignore
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

				String table_name = "oe_" + table + "_" + scale_factor;
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

					String table_name = "oe_" + table + "_" + scale_factor;
					if ( schema != null ) {
						table_name = schema + "." + table_name;
					}

					System.out.println("	 " + table_name + " :	" + dateformat.format(new Date()));

					if (database.equals("trafodion")) {
						sql_statement = "update statistics for table " + table_name + " on every column";
						if ( table.equals("stock")) {
							sql_statement = sql_statement + ", (S_I_ID, S_W_ID)";
						} else if ( table.equals("orderline")) {
							sql_statement = sql_statement + ", (OL_W_ID, OL_I_ID), (OL_D_ID, OL_W_ID), (OL_D_ID, OL_I_ID)";
						}
						if ( ! table.equals("warehouse") && ! table.equals("district") && ! table.equals("item") ) {
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
						// throw new Exception("Unexpected SQL Error.");  5/1/2014 For now we expect errors from maintain
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

				String table_name = "oe_" + table + "_" + scale_factor;
				if ( schema != null ) {
					table_name = schema + "." + table_name;
				}

				System.out.println("	 " + table_name + " :	" + dateformat.format(new Date()));

				int expected_rows = 0;
				switch ( table ) {
					case "item":		expected_rows = ITEM_ROWS_PER_SCALE;					break;
					case "warehouse":	expected_rows = scale_factor * WAREHOUSE_ROWS_PER_SCALE;			break;
					case "stock":		expected_rows = scale_factor * STOCK_ROWS_PER_SCALE;	break;
					case "district":	expected_rows = scale_factor * DISTRICT_ROWS_PER_SCALE;		break;
					case "customer":	expected_rows = scale_factor * CUSTOMER_ROWS_PER_SCALE;	break;
					case "history":		expected_rows = scale_factor * HISTORY_ROWS_PER_SCALE;	break;
					case "orders":		expected_rows = scale_factor * ORDERS_ROWS_PER_SCALE;	break;
					case "orderline":	expected_rows = scale_factor * ORDERLINE_ROWS_PER_SCALE;	break;
					case "neworder":	expected_rows = scale_factor * NEWORDER_ROWS_PER_SCALE;	break;
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
		int threads_started=0;

		System.out.println("\n   Starting up streams : " + dateformat.format(new Date()) );
		OrderEntryLoader[] stream = new OrderEntryLoader[number_of_streams];

		for ( int strm_idx = 0; strm_idx < number_of_streams; strm_idx++ ) {
			stream[strm_idx] = new OrderEntryLoader();
		}

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

		for ( int strm_idx = 0; strm_idx < number_of_streams; strm_idx++ ) {
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

		// Let the streams run till finished
		long rows_added = 0;
		long response_time = 0;
		test_interval = 0;
		long max_response_time = 0;
		long min_response_time = 999999999999999999L;
		long load_begin_millis = System.currentTimeMillis();

		int loop = 0;
		active = threads_started;
		while ( active > 0 ) {
			loop++;
			Thread.sleep(1000);
			active = 0;
			rows_added = 0;
			response_time = 0;
			max_response_time = 0;
			min_response_time = 999999999999999999L;
			for ( int strm_idx = 0; strm_idx < threads_started; strm_idx++ ) {
				if (stream[strm_idx].isAlive()) { active++; }
				rows_added = rows_added + stream[strm_idx].stream_accumlated_rows_added;
				response_time = response_time + stream[strm_idx].stream_accumlated_response_time;
				if (stream[strm_idx].stream_minimum_response_time < min_response_time ) {
					min_response_time = stream[strm_idx].stream_minimum_response_time;
				}
				if (stream[strm_idx].stream_maximum_response_time > max_response_time ) {
					max_response_time = stream[strm_idx].stream_maximum_response_time;
				}
				if (option_debug) { System.out.println("	  Loop " + loop + ": Stream " + strm_idx
						+ ", ops " + stream[strm_idx].stream_accumlated_rows_added
						+ ", Response " + stream[strm_idx].stream_accumlated_response_time
						+ ", MinResponse " + stream[strm_idx].stream_minimum_response_time
						+ ", MaxResponse " + stream[strm_idx].stream_maximum_response_time); }
			}

			double report_seconds = ( System.currentTimeMillis() - load_begin_millis ) / 1000.0;
			if ( report_seconds > ( ( test_interval + 1 ) * length_of_interval ) ) {
				if (test_interval == 0 ) {
					System.out.println("\n   Intvl ,     Time , Strms ,  Actv ,      OPS ,  AvgResp ,  MinResp ,  MaxResp , Seconds , TotOps , TotResp");
				}
				test_interval++;
				if ( response_time > 0 ) {
					System.out.println("   "
							+ String.format("%5d", test_interval )
							+ " , " + hourformat.format(new Date())
							+ " , " + String.format("%5d", threads_started )
							+ " , " + String.format("%5d", active )
							+ " , " + String.format("%8.2f", ( rows_added * 100 / report_seconds / 100.0 ))
							+ " , " + String.format("%8.3f", ( response_time / rows_added / 1000.0 ))
							+ " , " + String.format("%8.3f", ( min_response_time / 1000.0 ))
							+ " , " + String.format("%8.3f", ( max_response_time / 1000.0 ))
							+ " , " + String.format("%7.0f", report_seconds )
							+ " , " + String.format("%6d", rows_added )
							+ " , " + response_time);

				} else {
					System.out.println("   "
							+ String.format("%5d", test_interval )
							+ " , " + hourformat.format(new Date())
							+ " , " + String.format("%5d", threads_started )
							+ " , " + String.format("%5d", active )
							+ " , " + String.format("%8.2f", 0.0 )
							+ " , " + String.format("%8.3f", 0.0 )
							+ " , " + String.format("%8.3f", 0.0 )
							+ " , " + String.format("%8.3f", 0.0 )
							+ " , " + String.format("%7.0f", 0.0 )
							+ " , " + String.format("%6d", rows_added )
							+ " , " + response_time);

				}
			}
		}

		double load_end_secs = ( System.currentTimeMillis() - load_begin_millis ) / 1000.0;

		// Report results
		System.out.println("\n   Gather up final results : " + dateformat.format(new Date()) );
		rows_added = 0;
		response_time = 0;
		max_response_time = 0;
		min_response_time = 999999999999999999L;
		int[] response_distribution = new int[RESPONSE_BUCKETS];
		for ( int strm_idx = 0; strm_idx < threads_started; strm_idx++ ) {
			rows_added = rows_added + stream[strm_idx].stream_accumlated_rows_added;
			response_time = response_time + stream[strm_idx].stream_accumlated_response_time;
			if (stream[strm_idx].stream_minimum_response_time < min_response_time ) {
				min_response_time = stream[strm_idx].stream_minimum_response_time;
			}
			if (stream[strm_idx].stream_maximum_response_time > max_response_time ) {
				max_response_time = stream[strm_idx].stream_maximum_response_time;
			}
			for (int bucket = 0; bucket < RESPONSE_BUCKETS; bucket++ ) {
				response_distribution[bucket] = response_distribution[bucket] + stream[strm_idx].stream_response_distribution[bucket];
			}
			System.out.println("	  Stream " + strm_idx
					+ ", ops " + stream[strm_idx].stream_accumlated_rows_added
					+ ", Response " + stream[strm_idx].stream_accumlated_response_time
					+ ", MinResponse " + stream[strm_idx].stream_minimum_response_time
					+ ", MaxResponse " + stream[strm_idx].stream_maximum_response_time);
		}

//		System.out.println("\n	  Response Time Buckets");
//		for (int bucket = 0; bucket < RESPONSE_BUCKETS; bucket++ ) {
//			System.out.println("		ResponseDistribution[" + bucket + "] = " + response_distribution[bucket] );
//		}

		System.out.println("\n   Total , Intvl ,     Time , Strms ,      OPS ,  AvgResp ,  MinResp ,  MaxResp , Seconds , TotOps , TotResp");
		if ( response_time > 0 ) {
			System.out.println("   Total , "
					+ String.format("%5d", test_interval )
					+ " , " + hourformat.format(new Date())
					+ " , " + String.format("%5d", threads_started )
					+ " , " + String.format("%8.2f", ( rows_added * 100 / load_end_secs / 100.0 ))
					+ " , " + String.format("%8.3f", ( response_time / rows_added / 1000.0 ))
					+ " , " + String.format("%8.3f", ( min_response_time / 1000.0 ))
					+ " , " + String.format("%8.3f", ( max_response_time / 1000.0 ))
					+ " , " + String.format("%7.0f", load_end_secs )
					+ " , " + String.format("%6d", rows_added )
					+ " , " + response_time);
		} else {
			System.out.println("   Total , "
					+ String.format("%5d", test_interval )
					+ " , " + hourformat.format(new Date())
					+ " , " + String.format("%5d", threads_started )
					+ " , " + String.format("%8.2f", 0.0)
					+ " , " + String.format("%8.3f", 0.0)
					+ " , " + String.format("%8.3f", 0.0)
					+ " , " + String.format("%8.3f", 0.0)
					+ " , " + String.format("%7.0f", load_end_secs )
					+ " , " + String.format("%6d", rows_added )
					+ " , " + response_time);
		}

		if (( timer_minutes != 0 ) && ( load_end_secs > timer_minutes * 60 ) ){
			timer_pop = true;
		}

		System.out.println("\n   Shutting down the threads : " + dateformat.format(new Date()) );
		for ( int strm_idx = 0; strm_idx < threads_started; strm_idx++ ) {
			stream[strm_idx].join();
		}

		System.out.println("	 Elapsed Seconds : " + String.format("%7.3f", (( System.currentTimeMillis() - taskStartTime ) / 1000.0) ));

		if (option_debug) { System.out.println("< loadTable()"); }

	}


//----------------------------------------------------------------------------------------------------------

	public void run() {

		//  Parallel Stream Loader
		if (option_debug || option_trace) { System.out.println(stream_number + "> run( " + start_scale + " , " + stop_scale + " )"); }

		try {

			Connection dbconnection = establishDatabaseConnection();
			String sql_statement = null;

			PreparedStatement prepstatement = null;
			long count = 0;
			long prior_count = 0;

			PreparedStatement prepstatement2 = null;
			long count2 = 0;
			long prior_count2 = 0;

			if (database.equals("phoenix") && ! option_upsert ) {
				option_upsert = true;  // override it set
			}

			// Initialize Random Number Generator
			Random generator = new Random();
			generator.setSeed(stream_number * start_scale * 10000003 + 1234567);

			// Tell the main thread that I'm ready to proceed
			signal_ready = true;
			while (!signal_proceed){
				Thread.sleep(10);
			}

			for (String table: tables) {

 				switch ( table ) {

				case "item":

					if ( stream_number == 1 ) {

						count = 0;  prior_count = 0;

						if ( option_upsert ) {
							sql_statement = "upsert";
						} else {
							sql_statement = "insert";
						}
						if ( option_usingload ) {
							sql_statement = sql_statement + " using load";
						}
						sql_statement = sql_statement + " into " + item_table_name
								+ " (I_ID, I_IM_ID, I_NAME, I_PRICE, I_DATA)"
								+ " values (?, ?, ?, ?, ?)";
						if ( option_trace ) { System.out.println(stream_number + ">  " + sql_statement); }
						prepstatement = dbconnection.prepareStatement(sql_statement);

						// Load 100,000 ITEM records
						for ( int i_id = 1; i_id <= 100000; i_id++ ) {
							prepstatement.setInt(1,i_id);
							prepstatement.setInt(2,generateRandomInteger(generator,1,10000));
							prepstatement.setString(3,generateRandomAString(generator,generateRandomInteger(generator,14,24)));
							prepstatement.setDouble(4,(generateRandomInteger(generator,100,10000)/100.0));
							prepstatement.setString(5,generateRandomNStringORIGINAL(generator,generateRandomInteger(generator,26,50)));
							prepstatement.addBatch();
							if ( ++count % batchsize == 0 ) {
								long statement_start_time = System.nanoTime();
								prepstatement.executeBatch();	// Execute Batch
								long statement_elapsed_micro = ( System.nanoTime() - statement_start_time ) / 1000;
								prepstatement.clearBatch();

								stream_accumlated_rows_added = stream_accumlated_rows_added +  (count - prior_count);
								stream_accumlated_response_time = stream_accumlated_response_time + statement_elapsed_micro;
								if (statement_elapsed_micro < stream_minimum_response_time ) {
									stream_minimum_response_time = statement_elapsed_micro;
								}
								if (statement_elapsed_micro > stream_maximum_response_time ) {
									stream_maximum_response_time = statement_elapsed_micro;
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

						if ( count > prior_count ) {
							long statement_start_time = System.nanoTime();
							prepstatement.executeBatch();	// Execute Batch
							long statement_elapsed_micro = ( System.nanoTime() - statement_start_time ) / 1000;
							prepstatement.clearBatch();

							stream_accumlated_rows_added = stream_accumlated_rows_added + (count - prior_count);
							stream_accumlated_response_time = stream_accumlated_response_time + statement_elapsed_micro;
							if (statement_elapsed_micro < stream_minimum_response_time ) {
								stream_minimum_response_time = statement_elapsed_micro;
							}
							if (statement_elapsed_micro > stream_maximum_response_time ) {
								stream_maximum_response_time = statement_elapsed_micro;
							}
							int task_elapsed_milli = (int) (statement_elapsed_micro / 1000);
							if ( task_elapsed_milli < (RESPONSE_BUCKETS - 1) ) {
								++stream_response_distribution[task_elapsed_milli];
							} else {
								++stream_response_distribution[RESPONSE_BUCKETS - 1];
							}
						}

						if ( option_trace ) { System.out.println(stream_number + ">  " + count + " records inserted to " + item_table_name ); }
						prepstatement.close();
					}

					break;

				case "warehouse":

					count = 0;  prior_count = 0;

						if ( option_upsert ) {
							sql_statement = "upsert";
						} else {
							sql_statement = "insert";
						}
						if ( option_usingload ) {
							sql_statement = sql_statement + " using load";
						}
						sql_statement = sql_statement + " into " + warehouse_table_name
						+ " (W_ID, W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_TAX, W_YTD)"
						+ " values (?, ?, ?, ?, ?, ?, ?, ?, ?)";
					if ( option_trace ) { System.out.println(stream_number + ">  " + sql_statement); }
					prepstatement = dbconnection.prepareStatement(sql_statement);

					// Load 1 WAREHOUSE records per scale factor
					for ( int w_id = start_scale; w_id <= stop_scale; w_id++ ) {
						prepstatement.setInt(1,w_id);
						prepstatement.setString(2,generateRandomAString(generator,generateRandomInteger(generator,6,10)));
						prepstatement.setString(3,generateRandomAString(generator,generateRandomInteger(generator,10,20)));
						prepstatement.setString(4,generateRandomAString(generator,generateRandomInteger(generator,10,20)));
						prepstatement.setString(5,generateRandomAString(generator,generateRandomInteger(generator,10,20)));
						prepstatement.setString(6,generateRandomAString(generator,2));
						prepstatement.setString(7,generateRandomNString(generator,4) + "11111");
						prepstatement.setDouble(8,(generateRandomInteger(generator,0,2000)/10000.0));
						prepstatement.setDouble(9,300000.00);
						prepstatement.addBatch();
						if ( ++count % batchsize == 0 ) {
							long statement_start_time = System.nanoTime();
							prepstatement.executeBatch();	// Execute Batch
							long statement_elapsed_micro = ( System.nanoTime() - statement_start_time ) / 1000;
							prepstatement.clearBatch();

							stream_accumlated_rows_added = stream_accumlated_rows_added +  (count - prior_count);
							stream_accumlated_response_time = stream_accumlated_response_time + statement_elapsed_micro;
							if (statement_elapsed_micro < stream_minimum_response_time ) {
								stream_minimum_response_time = statement_elapsed_micro;
							}
							if (statement_elapsed_micro > stream_maximum_response_time ) {
								stream_maximum_response_time = statement_elapsed_micro;
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

					if ( count > prior_count ) {
						long statement_start_time = System.nanoTime();
						prepstatement.executeBatch();	// Execute Batch
						long statement_elapsed_micro = ( System.nanoTime() - statement_start_time ) / 1000;
						prepstatement.clearBatch();

						stream_accumlated_rows_added = stream_accumlated_rows_added + (count - prior_count);
						stream_accumlated_response_time = stream_accumlated_response_time + statement_elapsed_micro;
						if (statement_elapsed_micro < stream_minimum_response_time ) {
							stream_minimum_response_time = statement_elapsed_micro;
						}
						if (statement_elapsed_micro > stream_maximum_response_time ) {
							stream_maximum_response_time = statement_elapsed_micro;
						}
						int task_elapsed_milli = (int) (statement_elapsed_micro / 1000);
						if ( task_elapsed_milli < (RESPONSE_BUCKETS - 1) ) {
							++stream_response_distribution[task_elapsed_milli];
						} else {
							++stream_response_distribution[RESPONSE_BUCKETS - 1];
						}
					}

					if ( option_trace ) { System.out.println(stream_number + ">  " + count + " records inserted to " + warehouse_table_name ); }
					prepstatement.close();
					break;

				case "stock":

					count = 0;  prior_count = 0;

						if ( option_upsert ) {
							sql_statement = "upsert";
						} else {
							sql_statement = "insert";
						}
						if ( option_usingload ) {
							sql_statement = sql_statement + " using load";
						}
						sql_statement = sql_statement + " into " + stock_table_name
						+ " (S_I_ID, S_W_ID, S_QUANTITY, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04,"
						+ " S_DIST_05, S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10,"
						+ " S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DATA)"
						+ " values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
					if ( option_trace ) { System.out.println(stream_number + ">  " + sql_statement); }
					prepstatement = dbconnection.prepareStatement(sql_statement);

					// Load 100,000 STOCK records per scale factor
					for ( int w_id = start_scale; w_id <= stop_scale; w_id++ ) {
						for ( int i_id = 1; i_id <= 100000; i_id++ ) {
							prepstatement.setInt(1,i_id);
							prepstatement.setInt(2,w_id);
							prepstatement.setInt(3,generateRandomInteger(generator,10,100));
							prepstatement.setString(4,generateRandomAString(generator,24));
							prepstatement.setString(5,generateRandomAString(generator,24));
							prepstatement.setString(6,generateRandomAString(generator,24));
							prepstatement.setString(7,generateRandomAString(generator,24));
							prepstatement.setString(8,generateRandomAString(generator,24));
							prepstatement.setString(9,generateRandomAString(generator,24));
							prepstatement.setString(10,generateRandomAString(generator,24));
							prepstatement.setString(11,generateRandomAString(generator,24));
							prepstatement.setString(12,generateRandomAString(generator,24));
							prepstatement.setString(13,generateRandomAString(generator,24));
							prepstatement.setInt(14,0);
							prepstatement.setInt(15,0);
							prepstatement.setInt(16,0);
							prepstatement.setString(17,generateRandomNStringORIGINAL(generator,generateRandomInteger(generator,26,50)));
							prepstatement.addBatch();
							if ( ++count % batchsize == 0 ) {
								long statement_start_time = System.nanoTime();
								prepstatement.executeBatch();	// Execute Batch
								long statement_elapsed_micro = ( System.nanoTime() - statement_start_time ) / 1000;
								prepstatement.clearBatch();

								stream_accumlated_rows_added = stream_accumlated_rows_added +  (count - prior_count);
								stream_accumlated_response_time = stream_accumlated_response_time + statement_elapsed_micro;
								if (statement_elapsed_micro < stream_minimum_response_time ) {
									stream_minimum_response_time = statement_elapsed_micro;
								}
								if (statement_elapsed_micro > stream_maximum_response_time ) {
									stream_maximum_response_time = statement_elapsed_micro;
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

						stream_accumlated_rows_added = stream_accumlated_rows_added + (count - prior_count);
						stream_accumlated_response_time = stream_accumlated_response_time + statement_elapsed_micro;
						if (statement_elapsed_micro < stream_minimum_response_time ) {
							stream_minimum_response_time = statement_elapsed_micro;
						}
						if (statement_elapsed_micro > stream_maximum_response_time ) {
							stream_maximum_response_time = statement_elapsed_micro;
						}
						int task_elapsed_milli = (int) (statement_elapsed_micro / 1000);
						if ( task_elapsed_milli < (RESPONSE_BUCKETS - 1) ) {
							++stream_response_distribution[task_elapsed_milli];
						} else {
							++stream_response_distribution[RESPONSE_BUCKETS - 1];
						}
					}

					if ( option_trace ) { System.out.println(stream_number + ">  " + count + " records inserted to " + stock_table_name ); }
					prepstatement.close();
					break;

				case "district":

					count = 0;  prior_count = 0;

					if ( option_upsert ) {
						sql_statement = "upsert";
					} else {
						sql_statement = "insert";
					}
					if ( option_usingload ) {
						sql_statement = sql_statement + " using load";
					}
					sql_statement = sql_statement + " into " + district_table_name
						+ " (D_ID, D_W_ID, D_NAME, D_STREET_1, D_STREET_2, D_CITY,"
						+ " D_STATE, D_ZIP, D_TAX, D_YTD, D_NEXT_O_ID)"
						+ " values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
					if ( option_trace ) { System.out.println(stream_number + ">  " + sql_statement); }
					prepstatement = dbconnection.prepareStatement(sql_statement);

					// Load 10 DISTRICT records per scale factor
					for ( int w_id = start_scale; w_id <= stop_scale; w_id++ ) {
						for ( int d_id = 1; d_id <= 10; d_id++ ) {
							prepstatement.setInt(1,d_id);
							prepstatement.setInt(2,w_id);
							prepstatement.setString(3,generateRandomAString(generator,generateRandomInteger(generator,6,10)));
							prepstatement.setString(4,generateRandomAString(generator,generateRandomInteger(generator,10,20)));
							prepstatement.setString(5,generateRandomAString(generator,generateRandomInteger(generator,10,20)));
							prepstatement.setString(6,generateRandomAString(generator,generateRandomInteger(generator,10,20)));
							prepstatement.setString(7,generateRandomAString(generator,2));
							prepstatement.setString(8,generateRandomNString(generator,4) + "11111");
							prepstatement.setDouble(9,(generateRandomInteger(generator,0,2000)/10000.0));
							prepstatement.setDouble(10,30000.00);
							prepstatement.setInt(11,3001);
							prepstatement.addBatch();
							if ( ++count % batchsize == 0 ) {
								long statement_start_time = System.nanoTime();
								prepstatement.executeBatch();	// Execute Batch
								long statement_elapsed_micro = ( System.nanoTime() - statement_start_time ) / 1000;
								prepstatement.clearBatch();

								stream_accumlated_rows_added = stream_accumlated_rows_added +  (count - prior_count);
								stream_accumlated_response_time = stream_accumlated_response_time + statement_elapsed_micro;
								if (statement_elapsed_micro < stream_minimum_response_time ) {
									stream_minimum_response_time = statement_elapsed_micro;
								}
								if (statement_elapsed_micro > stream_maximum_response_time ) {
									stream_maximum_response_time = statement_elapsed_micro;
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

						stream_accumlated_rows_added = stream_accumlated_rows_added + (count - prior_count);
						stream_accumlated_response_time = stream_accumlated_response_time + statement_elapsed_micro;
						if (statement_elapsed_micro < stream_minimum_response_time ) {
							stream_minimum_response_time = statement_elapsed_micro;
						}
						if (statement_elapsed_micro > stream_maximum_response_time ) {
							stream_maximum_response_time = statement_elapsed_micro;
						}
						int task_elapsed_milli = (int) (statement_elapsed_micro / 1000);
						if ( task_elapsed_milli < (RESPONSE_BUCKETS - 1) ) {
							++stream_response_distribution[task_elapsed_milli];
						} else {
							++stream_response_distribution[RESPONSE_BUCKETS - 1];
						}
					}

					if ( option_trace ) { System.out.println(stream_number + ">  " + count + " records inserted to " + district_table_name ); }
					prepstatement.close();
					break;

				case "customer":

					count = 0;  prior_count = 0;

					if ( option_upsert ) {
						sql_statement = "upsert";
					} else {
						sql_statement = "insert";
					}
					if ( option_usingload ) {
						sql_statement = sql_statement + " using load";
					}
					sql_statement = sql_statement + " into " + customer_table_name
						+ " (C_ID, C_D_ID, C_W_ID, C_FIRST, C_MIDDLE, C_LAST,"
						+ " C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP,"
						+ " C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT,"
						+ " C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DELIVERY_CNT, C_DATA)"
						+ " values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
					if ( option_trace ) { System.out.println(stream_number + ">  " + sql_statement); }
					prepstatement = dbconnection.prepareStatement(sql_statement);

					// Load 3000 CUSTOMER for each DISTRICT (10 Districts per WAREHOUSE)
					for ( int w_id = start_scale; w_id <= stop_scale; w_id++ ) {
						for ( int d_id = 1; d_id <= 10; d_id++ ) {
							for ( int c_id = 1; c_id <= 3000; c_id++ ) {
								prepstatement.setInt(1,c_id);
								prepstatement.setInt(2,d_id);
								prepstatement.setInt(3,w_id);
								prepstatement.setString(4,generateRandomAString(generator,generateRandomInteger(generator,8,16)));
								prepstatement.setString(5,"OE");
								if ( c_id <= 1000 ) {
									prepstatement.setString(6,generateRandomLastName(generator,c_id - 1));
								} else {
									prepstatement.setString(6,generateRandomLastName(generator,NURand(generator,255,0,999,123)));
								}
								prepstatement.setString(7,generateRandomAString(generator,generateRandomInteger(generator,10,20)));
								prepstatement.setString(8,generateRandomAString(generator,generateRandomInteger(generator,10,20)));
								prepstatement.setString(9,generateRandomAString(generator,generateRandomInteger(generator,10,20)));
								prepstatement.setString(10,generateRandomAString(generator,2));
								prepstatement.setString(11,generateRandomNString(generator,4) + "11111");
								prepstatement.setString(12,generateRandomNString(generator,16));
								Timestamp ts = new Timestamp(new Date().getTime());
								prepstatement.setTimestamp(13,ts);
								if (generateRandomInteger(generator,0,100) < 10) {
									prepstatement.setString(14,"BC");
								} else {
									prepstatement.setString(14,"GC");
								}
								prepstatement.setDouble(15,50000.00);
								prepstatement.setDouble(16,generateRandomInteger(generator,0,5000)/10000.0);
								prepstatement.setDouble(17,-10.0);
								prepstatement.setDouble(18,10.0);
								prepstatement.setInt(19,1);
								prepstatement.setInt(20,0);
								prepstatement.setString(21,generateRandomAString(generator,generateRandomInteger(generator,300,500)));
								prepstatement.addBatch();
								if ( ++count % batchsize == 0 ) {
									long statement_start_time = System.nanoTime();
									prepstatement.executeBatch();	// Execute Batch
									long statement_elapsed_micro = ( System.nanoTime() - statement_start_time ) / 1000;
									prepstatement.clearBatch();

									stream_accumlated_rows_added = stream_accumlated_rows_added +  (count - prior_count);
									stream_accumlated_response_time = stream_accumlated_response_time + statement_elapsed_micro;
									if (statement_elapsed_micro < stream_minimum_response_time ) {
										stream_minimum_response_time = statement_elapsed_micro;
									}
									if (statement_elapsed_micro > stream_maximum_response_time ) {
										stream_maximum_response_time = statement_elapsed_micro;
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

						stream_accumlated_rows_added = stream_accumlated_rows_added + (count - prior_count);
						stream_accumlated_response_time = stream_accumlated_response_time + statement_elapsed_micro;
						if (statement_elapsed_micro < stream_minimum_response_time ) {
							stream_minimum_response_time = statement_elapsed_micro;
						}
						if (statement_elapsed_micro > stream_maximum_response_time ) {
							stream_maximum_response_time = statement_elapsed_micro;
						}
						int task_elapsed_milli = (int) (statement_elapsed_micro / 1000);
						if ( task_elapsed_milli < (RESPONSE_BUCKETS - 1) ) {
							++stream_response_distribution[task_elapsed_milli];
						} else {
							++stream_response_distribution[RESPONSE_BUCKETS - 1];
						}
					}

					if ( option_trace ) { System.out.println(stream_number + ">  " + count + " records inserted to " + customer_table_name ); }
					prepstatement.close();
					break;

				case "history":

					count = 0;  prior_count = 0;

					if ( option_upsert ) {
						sql_statement = "upsert";
					} else {
						sql_statement = "insert";
					}
					if ( option_usingload ) {
						sql_statement = sql_statement + " using load";
					}
					sql_statement = sql_statement + " into " + history_table_name
						+ " (H_C_ID, H_C_D_ID, H_C_W_ID, H_D_ID, H_W_ID, H_DATE, H_AMOUNT, H_DATA)"
						+ " values (?, ?, ?, ?, ?, ?, ?, ?)";
					if ( option_trace ) { System.out.println(stream_number + ">  " + sql_statement); }
					prepstatement = dbconnection.prepareStatement(sql_statement);

					// Load 1 HISTORY record per customer
					for ( int w_id = start_scale; w_id <= stop_scale; w_id++ ) {
						for ( int d_id = 1; d_id <= 10; d_id++ ) {
							for ( int c_id = 1; c_id <= 3000; c_id++ ) {
								// (H_C_ID, H_C_D_ID, H_C_W_ID, H_D_ID, H_W_ID, H_DATE, H_AMOUNT, H_DATA)
								prepstatement.setInt(1,c_id);
								prepstatement.setInt(2,d_id);
								prepstatement.setInt(3,w_id);
								prepstatement.setInt(4,d_id);
								prepstatement.setInt(5,w_id);
								Timestamp ts = new Timestamp(new Date().getTime());
								prepstatement.setTimestamp(6,ts);
								prepstatement.setDouble(7,10.0);
								prepstatement.setString(8,generateRandomAString(generator,generateRandomInteger(generator,12,24)));
								prepstatement.addBatch();
								if ( ++count % batchsize == 0 ) {
									long statement_start_time = System.nanoTime();
									prepstatement.executeBatch();	// Execute Batch
									long statement_elapsed_micro = ( System.nanoTime() - statement_start_time ) / 1000;
									prepstatement.clearBatch();

									stream_accumlated_rows_added = stream_accumlated_rows_added +  (count - prior_count);
									stream_accumlated_response_time = stream_accumlated_response_time + statement_elapsed_micro;
									if (statement_elapsed_micro < stream_minimum_response_time ) {
										stream_minimum_response_time = statement_elapsed_micro;
									}
									if (statement_elapsed_micro > stream_maximum_response_time ) {
										stream_maximum_response_time = statement_elapsed_micro;
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

						stream_accumlated_rows_added = stream_accumlated_rows_added + (count - prior_count);
						stream_accumlated_response_time = stream_accumlated_response_time + statement_elapsed_micro;
						if (statement_elapsed_micro < stream_minimum_response_time ) {
							stream_minimum_response_time = statement_elapsed_micro;
						}
						if (statement_elapsed_micro > stream_maximum_response_time ) {
							stream_maximum_response_time = statement_elapsed_micro;
						}
						int task_elapsed_milli = (int) (statement_elapsed_micro / 1000);
						if ( task_elapsed_milli < (RESPONSE_BUCKETS - 1) ) {
							++stream_response_distribution[task_elapsed_milli];
						} else {
							++stream_response_distribution[RESPONSE_BUCKETS - 1];
						}
					}

					if ( option_trace ) { System.out.println(stream_number + ">  " + count + " records inserted to " + history_table_name ); }
					prepstatement.close();
					break;

				case "orders":

					count = 0;  prior_count = 0;
					count2 = 0;  prior_count2 = 0;

					if ( option_upsert ) {
						sql_statement = "upsert";
					} else {
						sql_statement = "insert";
					}
					if ( option_usingload ) {
						sql_statement = sql_statement + " using load";
					}
					sql_statement = sql_statement + " into " + orders_table_name
						+ " (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL)"
						+ " values (?, ?, ?, ?, ?, ?, ?, ?)";
					if ( option_trace ) { System.out.println(stream_number + ">  " + sql_statement); }
					prepstatement = dbconnection.prepareStatement(sql_statement);

					if ( option_upsert ) {
						sql_statement = "upsert";
					} else {
						sql_statement = "insert";
					}
					if ( option_usingload ) {
						sql_statement = sql_statement + " using load";
					}
					sql_statement = sql_statement + " into " + orderline_table_name
						+ " (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_DELIVERY_D, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO)"
						+ " values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
					if ( option_trace ) { System.out.println(stream_number + ">  " + sql_statement); }
					prepstatement2 = dbconnection.prepareStatement(sql_statement);

					// Load 3000 ORDERS per district
					//  need to generate a random permutation of 3000 integers
					int[] intRandomIntPermutation = null;
					for ( int w_id = start_scale; w_id <= stop_scale; w_id++ ) {
						for ( int d_id = 1; d_id <= 10; d_id++ ) {
							// Redo the permutation for each warehouse/district combo
							intRandomIntPermutation = generateRandomIntPermutation(generator,3000);
							for ( int o_id = 1; o_id <= 3000; o_id++ ) {
								// (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL)
								prepstatement.setInt(1,o_id);
								prepstatement.setInt(2,d_id);
								prepstatement.setInt(3,w_id);
								prepstatement.setInt(4,intRandomIntPermutation[o_id-1]);  // Sequentially from a random permutation of 3000
								Timestamp ts = new Timestamp(new Date().getTime());
								prepstatement.setTimestamp(5,ts);
								if ( o_id < 2100 ) {
									prepstatement.setInt(6,generateRandomInteger(generator,1,10));
								} else {
									prepstatement.setNull(6,java.sql.Types.INTEGER);
								}
								int lineitems_per_order = generateRandomInteger(generator,5,15);
								prepstatement.setInt(7,lineitems_per_order);
								prepstatement.setInt(8,1);
								prepstatement.addBatch();

								// Load ORDERLINE per order
								for ( int ol_number = 1; ol_number <= lineitems_per_order; ol_number++ ) {
									prepstatement2.setInt(1,o_id);
									prepstatement2.setInt(2,d_id);
									prepstatement2.setInt(3,w_id);
									prepstatement2.setInt(4,ol_number);  // line_number
									prepstatement2.setInt(5,generateRandomInteger(generator,1,100000));
									prepstatement2.setInt(6,w_id);
									if ( o_id < 2100 ) {
										prepstatement2.setTimestamp(7,ts);
									} else {
										prepstatement2.setNull(7,java.sql.Types.TIMESTAMP);
									}
									prepstatement2.setInt(8,5);
									if ( o_id < 2100 ) {
										prepstatement2.setDouble(9,0.00);
									} else {
										prepstatement2.setDouble(9,generateRandomInteger(generator,1,999999)/100.0);
									}
									prepstatement2.setString(10,generateRandomAString(generator,24));
									prepstatement2.addBatch();
									if ( ++count2 % batchsize == 0 ) {
										long statement_start_time = System.nanoTime();
										prepstatement2.executeBatch();	// Execute Batch
										long statement_elapsed_micro = ( System.nanoTime() - statement_start_time ) / 1000;
										prepstatement2.clearBatch();

										stream_accumlated_rows_added = stream_accumlated_rows_added +  (count2 - prior_count2);
										stream_accumlated_response_time = stream_accumlated_response_time + statement_elapsed_micro;
										if (statement_elapsed_micro < stream_minimum_response_time ) {
											stream_minimum_response_time = statement_elapsed_micro;
										}
										if (statement_elapsed_micro > stream_maximum_response_time ) {
											stream_maximum_response_time = statement_elapsed_micro;
										}
										int task_elapsed_milli = (int) (statement_elapsed_micro / 1000);
										if ( task_elapsed_milli < (RESPONSE_BUCKETS - 1) ) {
											++stream_response_distribution[task_elapsed_milli];
										} else {
											++stream_response_distribution[RESPONSE_BUCKETS - 1];
										}

										prior_count2 = count2;
									}
								}

								if ( ++count % batchsize == 0 ) {
									long statement_start_time = System.nanoTime();
									prepstatement.executeBatch();	// Execute Batch
									long statement_elapsed_micro = ( System.nanoTime() - statement_start_time ) / 1000;
									prepstatement.clearBatch();

									stream_accumlated_rows_added = stream_accumlated_rows_added +  (count - prior_count);
									stream_accumlated_response_time = stream_accumlated_response_time + statement_elapsed_micro;
									if (statement_elapsed_micro < stream_minimum_response_time ) {
										stream_minimum_response_time = statement_elapsed_micro;
									}
									if (statement_elapsed_micro > stream_maximum_response_time ) {
										stream_maximum_response_time = statement_elapsed_micro;
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

						stream_accumlated_rows_added = stream_accumlated_rows_added + (count - prior_count);
						stream_accumlated_response_time = stream_accumlated_response_time + statement_elapsed_micro;
						if (statement_elapsed_micro < stream_minimum_response_time ) {
							stream_minimum_response_time = statement_elapsed_micro;
						}
						if (statement_elapsed_micro > stream_maximum_response_time ) {
							stream_maximum_response_time = statement_elapsed_micro;
						}
						int task_elapsed_milli = (int) (statement_elapsed_micro / 1000);
						if ( task_elapsed_milli < (RESPONSE_BUCKETS - 1) ) {
							++stream_response_distribution[task_elapsed_milli];
						} else {
							++stream_response_distribution[RESPONSE_BUCKETS - 1];
						}
					}

					if ( option_trace ) { System.out.println(stream_number + ">  " + count + " records inserted to " + orders_table_name ); }
					prepstatement.close();

					if ( count2 > prior_count2 ) {
						long statement_start_time = System.nanoTime();
						prepstatement2.executeBatch();	// Execute Batch
						long statement_elapsed_micro = ( System.nanoTime() - statement_start_time ) / 1000;
						prepstatement2.clearBatch();

						stream_accumlated_rows_added = stream_accumlated_rows_added + (count2 - prior_count2);
						stream_accumlated_response_time = stream_accumlated_response_time + statement_elapsed_micro;
						if (statement_elapsed_micro < stream_minimum_response_time ) {
							stream_minimum_response_time = statement_elapsed_micro;
						}
						if (statement_elapsed_micro > stream_maximum_response_time ) {
							stream_maximum_response_time = statement_elapsed_micro;
						}
						int task_elapsed_milli = (int) (statement_elapsed_micro / 1000);
						if ( task_elapsed_milli < (RESPONSE_BUCKETS - 1) ) {
							++stream_response_distribution[task_elapsed_milli];
						} else {
							++stream_response_distribution[RESPONSE_BUCKETS - 1];
						}
					}

					if ( option_trace ) { System.out.println(stream_number + ">  " + count + " records inserted to " + orderline_table_name ); }
					prepstatement2.close();
					break;

				case "neworder":

					count = 0;  prior_count = 0;

					if ( option_upsert ) {
						sql_statement = "upsert";
					} else {
						sql_statement = "insert";
					}
					if ( option_usingload ) {
						sql_statement = sql_statement + " using load";
					}
					sql_statement = sql_statement + " into " + neworder_table_name
						+ " (NO_O_ID, NO_D_ID, NO_W_ID)"
						+ " values (?, ?, ?)";
					if ( option_trace ) { System.out.println(stream_number + ">  " + sql_statement); }
					prepstatement = dbconnection.prepareStatement(sql_statement);

					// Load NEWORDER per order
					for ( int w_id = start_scale; w_id <= stop_scale; w_id++ ) {
						for ( int d_id = 1; d_id <= 10; d_id++ ) {
							for ( int o_id = 2101; o_id <= 3000; o_id++ ) {
								prepstatement.setInt(1,o_id);
								prepstatement.setInt(2,d_id);
								prepstatement.setInt(3,w_id);
								prepstatement.addBatch();
								if ( ++count % batchsize == 0 ) {
									long statement_start_time = System.nanoTime();
									prepstatement.executeBatch();	// Execute Batch
									long statement_elapsed_micro = ( System.nanoTime() - statement_start_time ) / 1000;
									prepstatement.clearBatch();

									stream_accumlated_rows_added = stream_accumlated_rows_added +  (count - prior_count);
									stream_accumlated_response_time = stream_accumlated_response_time + statement_elapsed_micro;
									if (statement_elapsed_micro < stream_minimum_response_time ) {
										stream_minimum_response_time = statement_elapsed_micro;
									}
									if (statement_elapsed_micro > stream_maximum_response_time ) {
										stream_maximum_response_time = statement_elapsed_micro;
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

						stream_accumlated_rows_added = stream_accumlated_rows_added + (count - prior_count);
						stream_accumlated_response_time = stream_accumlated_response_time + statement_elapsed_micro;
						if (statement_elapsed_micro < stream_minimum_response_time ) {
							stream_minimum_response_time = statement_elapsed_micro;
						}
						if (statement_elapsed_micro > stream_maximum_response_time ) {
							stream_maximum_response_time = statement_elapsed_micro;
						}
						int task_elapsed_milli = (int) (statement_elapsed_micro / 1000);
						if ( task_elapsed_milli < (RESPONSE_BUCKETS - 1) ) {
							++stream_response_distribution[task_elapsed_milli];
						} else {
							++stream_response_distribution[RESPONSE_BUCKETS - 1];
						}
					}

					if ( option_trace ) { System.out.println(stream_number + ">  " + count + " records inserted to " + neworder_table_name ); }
					prepstatement.close();
					break;

				default:
				}

			}

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

			Properties log4j_props = new Properties();
			log4j_props.load(new FileInputStream("log4j.properties"));
			PropertyConfigurator.configure(log4j_props);

			System.out.println("OrderEntryLoader " + command_line);

			String syntax = "\n"
				+ "OrderEntry: OrderEntryLoader <scale_factor> [<load_option>]\n"

				+ "   <scale_factor> : positive integer representing largest generated table (2^n rows)\n"

				+ "   <load_option> : \n"
				+ "     schema <schema_name> : Overrides default schema name.\n"
				+ "     streams <number_of_streams> : Load data with <number_of_streams> streams. (Default 1)\n"
				+ "     streamrange <startstream> <stopstream> : To facilitate multiple drivers.\n"
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
				+ "     compression : Create the table with compression.\n"
				+ "     upsert : Use upsert syntax instead of insert syntax.\n"
				+ "     usingload : Use using load syntax in insert/upsert.\n"
				+ "     randomload : Will load in a random permutations of key values.\n"
				+ "     intervallength <length_of_interval> : Length of reporting interval in seconds.(Default 10)\n"
				+ "     timer <time_minutes> : Load tables until single load exceed time_minutes minutes.\n"
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
					case "randomload": option_randomload = true; break;
					case "intervallength": length_of_interval = Integer.parseInt(args[++indx]); break;
					case "timer": timer_minutes = Integer.parseInt(args[++indx]); break;
					case "trace": option_trace = true; break;
					case "debug": option_debug = true; break;
					case "aligned": option_aligned = true; break;
					case "table": String temptables[] = { args[++indx] };
					              tables = temptables;
					              break;
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
				System.out.println("			jdbc_driver : " + jdbc_driver);
				System.out.println("			url : " + url);
				System.out.println("			database : " + database);
				System.out.println("			schema : " + schema);
			}

			if (database.equals("seaquest") && option_upsert ) {
				throw new Exception("ERROR : Seaquest does not support upsert.");
			}

			if (database.equals("seaquest") && option_salt ) {
				throw new Exception("ERROR : Seaquest does not support salting.");
			}

			// identify the table names
			customer_table_name = "oe_customer_" + scale_factor;
			district_table_name = "oe_district_" + scale_factor;
			history_table_name = "oe_history_" + scale_factor;
			item_table_name = "oe_item_" + scale_factor;
			neworder_table_name = "oe_neworder_" + scale_factor;
			orderline_table_name = "oe_orderline_" + scale_factor;
			orders_table_name = "oe_orders_" + scale_factor;
			stock_table_name = "oe_stock_" + scale_factor;
			warehouse_table_name = "oe_warehouse_" + scale_factor;
			if ( schema != null ) {
				customer_table_name = schema + "." + customer_table_name;
				district_table_name = schema + "." + district_table_name;
				history_table_name = schema + "." + history_table_name;
				item_table_name = schema + "." + item_table_name;
				neworder_table_name = schema + "." + neworder_table_name;
				orderline_table_name = schema + "." + orderline_table_name;
				orders_table_name = schema + "." + orders_table_name;
				stock_table_name = schema + "." + stock_table_name;
				warehouse_table_name = schema + "." + warehouse_table_name;
			}


			System.out.println("\nOrderEntryLoader");

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
			if (option_compression) { System.out.println("   " + String.format("%16s", "Compression" ) + " : " + option_compression); }
			if (option_randomload) { System.out.println("   " + String.format("%16s", "RandomLoad" ) + " : " + option_randomload); }
			if (option_aligned) { System.out.println("   " + String.format("%16s", "Aligned" ) + " : " + option_aligned); }

			System.out.println("   " + String.format("%16s", "IntervalLength" ) + " : " + length_of_interval);
			if (timer_minutes != 0) { System.out.println("   " + String.format("%16s", "TimerMinutes" ) + " : " + timer_minutes); }
			if (option_trace) { System.out.println("   " + String.format("%16s", "Trace" ) + " : " + option_trace); }
			if (option_debug) { System.out.println("   " + String.format("%16s", "Debug" ) + " : " + option_debug); }

			String control_cqd_file_name = System.getenv("TEST_CQD_FILE");
			if ( control_cqd_file_name != null ) {
				System.out.println("		Control CQD File:	" + control_cqd_file_name);
			}

			if (option_createschema) { createSchema(); }

			if (option_drop) { dropTable(); }

			if (option_create) { createTable(); }

			if (option_dropcreate) {
				try {
				  dropTable();
				} catch (SQLException sqlexception) {
					// ignore
				}
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

		System.out.println("\n   Test Stopping : " + dateformat.format(new Date()) + "\n" );

		System.out.println("	 Test Elapsed Seconds : " + String.format("%7.3f", (( System.currentTimeMillis() - testStartTime ) / 1000.0) ));
	}
}

