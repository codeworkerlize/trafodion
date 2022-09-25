
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.apache.log4j.Logger;

public class EDAtomics {


	// Global Variables
	static final int MAX_CONNECT_ATTEMPTS = 100;
	static final int MAXIMUM_SCALE_FACTOR = 30;

	static int max_scale_factor = 1;
    static String workload;
    static boolean option_baseatomics = false;
    static boolean option_joinatomics = false;
    static boolean option_autocommit = true;
    static boolean option_trace = false;
    static boolean option_debug = false;
    static int start_scale = 10;
    static Properties props = new Properties();
    static String jdbc_driver =  null;
    static String url = null;
    static String schema = null;
    static String source_schema = null;
    static String destination_schema = null;
    static String database = null;
    static int timer_minutes = 0;
    static boolean timer_pop = false;
    public static volatile String option_schema = null;

	static Connection dbconnection = null;

	// Initialize Simple Date Format
	static SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	static SimpleDateFormat hourformat = new SimpleDateFormat("HH:mm:ss");
	static SimpleDateFormat datetimeformat = new SimpleDateFormat("yyyyMMdd.HHmmss");

	// Powers of 2
	static int[] scale_rows = {
		1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024,
		2048, 4096, 8192, 16384, 32768, 65536, 131072,
		262144, 524288, 1048576, 2097152, 4194304, 8388608,
		16777216, 33554432, 67108864, 134217728, 268435456,
		536870912, 1073741824
	};

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
			if (option_debug) { System.out.println(">    Attempt :" + attempts); }
			try {
				if (database.equals("hive")) {
					dbconnection = DriverManager.getConnection(url, props.getProperty("user"), "");
				} else {
					dbconnection = DriverManager.getConnection(url, props);
				}
			} catch (SQLException exception) {
				// try again
				if ( attempts % 10 == 0 ) {
					System.out.println(" Connection Failed ... will retry. Attempts: " + attempts );
				}
				if (option_debug) { System.out.println("ERROR : SQLException : " + exception.getMessage()); }
				SQLException nextException = exception;
				do {
					if (option_debug) { System.out.println("ERROR : SQLException : " + nextException.getMessage()); }
				} while ((nextException = nextException.getNextException()) != null);
				if ( attempts > MAX_CONNECT_ATTEMPTS ) {
					throw new Exception("Error : Unable to connect to database (Maximum " + MAX_CONNECT_ATTEMPTS + " attempts failed).");
				}
				Thread.sleep( 10 * attempts );  // Sleep
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


		// if CQD file specified, apply CQDs to open session
		String control_cqd_file_name = System.getenv("TEST_CQD_FILE");
		if (( control_cqd_file_name != null ) && ( !control_cqd_file_name.equals(""))) {
			System.out.println("Control CQD File: " + control_cqd_file_name);
			if (option_debug) { System.out.println("CQD File Specified.  Need to apply CQDs to the open session."); }
			if (option_debug) { System.out.println("Establish Statement."); }
			Statement dbstatement = dbconnection.createStatement();
			if (option_debug) { System.out.println("Statement established."); }
			String sql_statement = null;
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

		if (option_debug) { System.out.println("< establishDatabaseConnection()"); }

		return dbconnection;
	}


//----------------------------------------------------------------------------------------------------------

	static void runAtomic ( String atomic_test , int scale_factor ) throws Exception {
		if (option_debug) { System.out.println("> runAtomic (" + atomic_test + " " + scale_factor + ")"); }
		
		if ( ( !timer_pop ) && (scale_factor <= max_scale_factor) ){
			
			try {
				
				String table_name = "ED_TABLE_" +  String.format("%02d",scale_factor);

				StringBuilder sql_statement = null;
				
				// Check Table
//				System.out.println("\n Atomic Test ( " + atomic_test + " , " + scale_factor + " , " + table_name + " )\n");

				// Start time
				long task_start_time = System.nanoTime();

				switch ( atomic_test ) {
					case "count":
						sql_statement = new StringBuilder( "select count(*) from " );
						if ( schema != null ) {
							sql_statement = sql_statement.append( schema + "." );
						}
						sql_statement = sql_statement.append( table_name );
					    break;
					case "scan":
						sql_statement = new StringBuilder( "select * from " );
						if ( schema != null ) {
							sql_statement = sql_statement.append( schema + "." );
						}
						sql_statement = sql_statement.append( table_name );
						sql_statement = sql_statement.append( " where INTEGER_FACT < 0" );
					    break;
					case "orderby":
						sql_statement = new StringBuilder( "select " );
						if  ( database.equals("seaquest") ||  database.equals("trafodion") || database.equals("seahive"))  {
							sql_statement = sql_statement.append( "[ first 10 ] " );
						}
						sql_statement = sql_statement.append( "* from " );
						if ( schema != null ) {
							sql_statement = sql_statement.append( schema + "." );
						}
						sql_statement = sql_statement.append( table_name );
						sql_statement = sql_statement.append( " order by FOREIGN_KEY_01, FOREIGN_KEY_02, FOREIGN_KEY_03, FOREIGN_KEY_04, PRIM_KEY " );
						if ( database.equals("phoenix") ||  database.equals("hive") ) {
							sql_statement = sql_statement.append( "limit 10" );
						}
					    break;
					case "groupby":
						sql_statement = new StringBuilder( "select FOREIGN_KEY_10, sum(INTEGER_FACT) as CNT from " );
						if ( schema != null ) {
							sql_statement = sql_statement.append( schema + "." );
						}
						sql_statement = sql_statement.append( table_name );
						sql_statement = sql_statement.append( " group by FOREIGN_KEY_10" );
					    break;
					case "innerjoin":
						if (database.equals("phoenix")) {
							throw new Exception("Error : joins not supported in Phoenix.");
						}
						sql_statement = new StringBuilder("select * from " );
						if ( schema != null ) {
							sql_statement = sql_statement.append( schema + "." );
						}
						sql_statement = sql_statement.append( "ED_TABLE_10 lft_tbl inner join " );
						if ( schema != null ) {
							sql_statement = sql_statement.append( schema + "." );
						}
						sql_statement = sql_statement.append( table_name );
						sql_statement = sql_statement.append( " rght_tbl on lft_tbl.PRIM_KEY = rght_tbl.FOREIGN_KEY_10 where lft_tbl.INTEGER_FACT + rght_tbl.INTEGER_FACT < 0" );
					    break;
					case "rightjoin":
						if (database.equals("phoenix")) {
							throw new Exception("Error : joins not supported in Phoenix.");
						}
						sql_statement = new StringBuilder("select * from " );
						if ( schema != null ) {
							sql_statement = sql_statement.append( schema + "." );
						}
						sql_statement = sql_statement.append( "ED_TABLE_10 lft_tbl right join " );
						if ( schema != null ) {
							sql_statement = sql_statement.append( schema + "." );
						}
						sql_statement = sql_statement.append( table_name );
						sql_statement = sql_statement.append( " rght_tbl on lft_tbl.PRIM_KEY = rght_tbl.FOREIGN_KEY_10 where lft_tbl.INTEGER_FACT + rght_tbl.INTEGER_FACT < 0" );
					    break;
					case "leftjoin":
						if (database.equals("phoenix")) {
							throw new Exception("Error : joins not supported in Phoenix.");
						}
						sql_statement = new StringBuilder("select * from " );
						if ( schema != null ) {
							sql_statement = sql_statement.append( schema + "." );
						}
						sql_statement = sql_statement.append( "ED_TABLE_10 lft_tbl left join " );
						if ( schema != null ) {
							sql_statement = sql_statement.append( schema + "." );
						}
						sql_statement = sql_statement.append( table_name );
						sql_statement = sql_statement.append( " rght_tbl on lft_tbl.PRIM_KEY = rght_tbl.FOREIGN_KEY_10 where lft_tbl.INTEGER_FACT + rght_tbl.INTEGER_FACT < 0" );
					    break;
					case "joinx4":
						if (database.equals("phoenix")) {
							throw new Exception("Error : joins not supported in Phoenix.");
						}
						sql_statement = new StringBuilder("select * from " );
						if ( schema != null ) {
							sql_statement = sql_statement.append( schema + "." );
						}
						sql_statement = sql_statement.append( table_name );
						sql_statement = sql_statement.append( " lft_tbl inner join " );
						if ( schema != null ) {
							sql_statement = sql_statement.append( schema + "." );
						}
						sql_statement = sql_statement.append( "ED_TABLE_10 rght_tbl on lft_tbl.PRIM_KEY = rght_tbl.FOREIGN_KEY_" + String.format("%02d",scale_factor) + " where lft_tbl.INTEGER_FACT + rght_tbl.INTEGER_FACT < 0" );
					    break;
					case "joinx5":
						if (database.equals("phoenix")) {
							throw new Exception("Error : joins not supported in Phoenix.");
						}
						sql_statement = new StringBuilder("select * from " );
						if ( schema != null ) {
							sql_statement = sql_statement.append( schema + "." );
						}
						sql_statement = sql_statement.append( table_name );
						sql_statement = sql_statement.append( " lft_tbl right join " );
						if ( schema != null ) {
							sql_statement = sql_statement.append( schema + "." );
						}
						sql_statement = sql_statement.append( "ED_TABLE_10 rght_tbl on lft_tbl.PRIM_KEY = rght_tbl.FOREIGN_KEY_" + String.format("%02d",scale_factor) + " where lft_tbl.INTEGER_FACT + rght_tbl.INTEGER_FACT < 0" );
					    break;
					case "joinx6":
						if (database.equals("phoenix")) {
							throw new Exception("Error : joins not supported in Phoenix.");
						}
						sql_statement = new StringBuilder("select * from " );
						if ( schema != null ) {
							sql_statement = sql_statement.append( schema + "." );
						}
						sql_statement = sql_statement.append( table_name );
						sql_statement = sql_statement.append( " lft_tbl left join " );
						if ( schema != null ) {
							sql_statement = sql_statement.append( schema + "." );
						}
						sql_statement = sql_statement.append( "ED_TABLE_10 rght_tbl on lft_tbl.PRIM_KEY = rght_tbl.FOREIGN_KEY_" + String.format("%02d",scale_factor) + " where lft_tbl.INTEGER_FACT + rght_tbl.INTEGER_FACT < 0" );
					    break;
					case "createselect":
						sql_statement = new StringBuilder("create table " );
						if ( destination_schema != null ) {
							sql_statement = sql_statement.append( destination_schema + "." );
						}
						sql_statement = sql_statement.append( table_name );
						sql_statement = sql_statement.append( " like " );
						if ( destination_schema != null ) {
							sql_statement = sql_statement.append( destination_schema + "." );
						}
						sql_statement = sql_statement.append( "ED_TABLE " );
						if (database.equals("seaquest")) {
							sql_statement = sql_statement.append( " with partitions" );
						}
						sql_statement = sql_statement.append( " as (select * from " );
						if ( source_schema != null ) {
							sql_statement = sql_statement.append( source_schema + "." );
						}
						sql_statement = sql_statement.append( table_name );
						sql_statement = sql_statement.append( " )" );
					    break;
					case "insertselect":
						sql_statement = new StringBuilder("insert into " );
						if ( destination_schema != null ) {
							sql_statement = sql_statement.append( destination_schema + "." );
						}
						sql_statement = sql_statement.append( table_name );
						sql_statement = sql_statement.append( " select * from " );
						if ( source_schema != null ) {
							sql_statement = sql_statement.append( source_schema + "." );
						}
						sql_statement = sql_statement.append( table_name );
					    break;
					default:
					    break;
				}

				if ( option_trace ) { System.out.println("      " + sql_statement.toString()); }
				Statement dbstatement = dbconnection.createStatement();
				if ( atomic_test.equals("createselect") || atomic_test.equals("insertselect") ) {
					dbstatement.execute(sql_statement.toString());
				} else {
					ResultSet resultset = dbstatement.executeQuery(sql_statement.toString());
					int actual_rows = 0;
					while ( resultset.next() ) { actual_rows++; }
					resultset.close();
					if ( option_trace ) { System.out.println("  Table = " + table_name + " , Scale Size = " + scale_rows[scale_factor] + " , Rows Returned = " + actual_rows ); }
				}
				dbstatement.close();

				
				// Stop time
				long task_stop_time = System.nanoTime();
				long task_elapsed_nano = ( task_stop_time - task_start_time );
//					long task_elapsed_milli = task_elapsed_nano / 1000000;
				double task_elapsed_seconds = task_elapsed_nano / 1000000000.0;

				System.out.println("     " + String.format("%10s",atomic_test) 
					+ " ,  " + String.format("%5d",scale_factor) 
					+ " ,  " + hourformat.format(new Date())
					+ " ,  " + String.format("%8.3f",task_elapsed_seconds)
					+ " ,  " + System.currentTimeMillis()
					);
			
				if (( timer_minutes != 0 ) && ( task_elapsed_seconds > timer_minutes * 60 ) ){
					timer_pop = true;
				}

			} catch (SQLException exception) {
				System.out.println("     " + String.format("%10s",atomic_test) 
						+ " ,  " + String.format("%5d",scale_factor)
						+ " ,  " + hourformat.format(new Date())
						+ " ,  ERROR"
						+ " ,  " + System.currentTimeMillis()
						);
				SQLException nextException = exception;
				do {
					System.out.println("SQLException: " + nextException.getMessage());
				} while ((nextException = nextException.getNextException()) != null);
			}

		} else {
			
			System.out.println("     " + String.format("%10s",atomic_test) 
					+ " ,  " + String.format("%5d",scale_factor)
					);

		}

		if (option_debug) { System.out.println("< runAtomic()"); }
	}

//----------------------------------------------------------------------------------------------------------

	public static void main(String args[]) {
		String command_line = "";
		for (int idx = 0; idx < args.length; idx++) {
			command_line = command_line + args[idx] + " ";
		}
		System.out.println("EDAtomics " + command_line);

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

			// Command Interrupter

			String syntax = "\n"
					+ "ED: EDAtomics <atomics_workload> <max_scale_factor> [<run_option>]\n"
					+ "   <atomics_workload> : "
					+ "      count | scan | orderby | groupby | ...\n"
					+ "   <max_scale_factor> : positive integer representing size of largest ED table (2^n rows)\n"
					+ "   <run_option> :  \n"
					+ "     schema <schema_name> : Overrides default schema name.\n"
					+ "     startscale <start_scale> : beginning scale (Default 10).\n"
					+ "     timer <time_minutes> : run test until single statement exceed time_minutes minutes.\n"
					+ "     trace : Enable Statement Tracing.\n"
					+ "     debug : Enable Debug Tracing.\n"
					;

			// Command Interpreter
			if ( args.length < 2 ) {
				System.out.println(syntax);
				throw new Exception("ERROR : Required <atomics_workload> and <scale_factor> Arguments not Provide.");
			}

			workload = args[0];
			switch (workload) {
				case "count":
				case "groupby":
				case "exchange":
				case "scan":
				case "orderby":
				case "innerjoin":
				case "rightjoin":
				case "leftjoin":
				case "joinx4":
				case "joinx5":
				case "joinx6":
				case "createselect":
				case "insertselect":
					break;
				case "all" :
				case "base" :
					option_baseatomics = true;
					break;
				case "joins" :
					option_joinatomics = true;
					break;
				default: {
					System.out.println(syntax);
					throw new Exception("ERROR : Invalid workload specified.  ( workload = " + workload + " )");
				}
			}

			max_scale_factor = Integer.parseInt(args[1]);
			if ( max_scale_factor <= 0 ) {
				System.out.println(syntax);
				throw new Exception("ERROR : Invalid max_scale_factor Provided. ( max_scale_factor = " + max_scale_factor + " )");
			}

			String option = null;
			for ( int indx = 2; indx < args.length; indx++ ) {
				option = args[indx];
				switch ( option ) {
					case "schema": option_schema = args[++indx]; break;
					case "trace": option_trace = true; break;
					case "debug": option_debug = true; break;
					case "timer": timer_minutes = Integer.parseInt(args[++indx]); break;
					case "startscale": start_scale = Integer.parseInt(args[++indx]); break;
					case "sourceschema": source_schema = args[++indx]; break;
					case "destinationschema": destination_schema = args[++indx]; break;
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
			schema = props.getProperty("schema");
			fs.close();
			if (option_schema != null) {
				schema = option_schema;
			}

			// turn off all logging ( Phoenix is very noisy. )
//			Logger.getRootLogger().setLevel(Level.ERROR);

			// establish database connection
			dbconnection = establishDatabaseConnection();

		    System.out.println("\nEDAtomics");
		    System.out.println("\n   Test Starting : " + dateformat.format(new Date()) );

		    System.out.println("        PropertyFile : " + propFile);
		    System.out.println("            Datebase : " + database);
		    System.out.println("              Schema : " + schema);
		    System.out.println("         ScaleFactor : " + max_scale_factor);
			System.out.println("          StartScale : " + start_scale);
		    System.out.println("            Workload : " + workload);
		    if (timer_minutes != 0) {
			System.out.println("        TimerMinutes : " + timer_minutes);
		    }
		    if (option_trace) {
			System.out.println("               Trace : " + option_trace);
		    }
		    if (option_debug) {
			System.out.println("               Debug : " + option_debug);
		    }

		    String[] tests = null;
		    if ( option_baseatomics ) {
		    	tests = new String[]{ "count" , "scan" , "orderby" , "groupby" };
		    } else if ( option_joinatomics ) {
		    	tests = new String[]{ "innerjoin" , "rightjoin" , "leftjoin" };
		    } else {
		    	tests = new String[]{ workload };
		    }		    	
		    
		    for ( int idx = 0; idx < tests.length; idx++ ) {
			    System.out.println("\n    Atomics Test ( " + tests[idx] + " ) : " + dateformat.format(new Date()) );
				System.out.println("\n           Test ,  Scale ,      Time ,   Elapsed , MilliTimeStamp");

				timer_pop = false;
				
				for ( int scale = start_scale; scale <= MAXIMUM_SCALE_FACTOR; scale++ ) {
					
					runAtomic(  tests[idx] , scale );

				}
		    }

		} catch (SQLException exception) {
			SQLException nextException = exception;
			do {
				System.out.println("ERROR : SQLException : " + nextException.getMessage());
			} while ((nextException = nextException.getNextException()) != null);
			exception.printStackTrace(System.out);
		} catch (Exception exception) {
			System.out.println("ERROR Exception: " + exception.getMessage());
			exception.printStackTrace(System.out);
		}

	    System.out.println("\n   Test Stopping : " + dateformat.format(new Date()) + "\n" );
	}
}

