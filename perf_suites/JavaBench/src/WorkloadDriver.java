import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class WorkloadDriver extends Thread {

// Single global value for all Streams (Threads)
	final static int RESPONSE_BUCKETS = 101;

	final static int INVALID_SCALEFACTOR = -1;

	final static SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	final static SimpleDateFormat datetimeformat = new SimpleDateFormat("yyyyMMdd.HHmmss");
	final static SimpleDateFormat hourformat = new SimpleDateFormat("HH:mm:ss");

	final int TRANSACTION_SUCCESSFUL = 1;
	final int TRANSACTION_SUCCESS_NULL = 0;
//	final int TRANSACTION_FAILED = -1;
//	final int TRANSACTION_FAILED_ON_RETRY = -2;

//	final Object threadlock = new Object();
//	volatile static int current_transaction = 0;

	volatile static int total_transactions = 0;

	final static Object stdoutlock = new Object();

	static boolean option_time_test = false;
	static boolean option_transaction_test = false;
	static boolean option_loop_test = false;
    static boolean option_repeat_test = false;

	static int number_of_streams = 1;
	static int start_stream = 0;
	static long num_of_transactions = 1;
	static int number_of_intervals = 1;
	static int length_of_interval = 60;
	static int test_interval = 0;
	static long number_of_loops = 999999999999999999L;
	static int number_of_repeats = 1;
	static int repeat_count = 1;

	static int scale_per_stream = 1;

	static String benchmark = null;
	static String workload = "default";
	static int scale_factor = INVALID_SCALEFACTOR;
	static int think_time = 0;
	static boolean option_sessionless = false;
	static boolean option_prepareexec = false;
	static boolean option_directexec = false;
	static boolean option_specialexec = false;
	static boolean option_spj = false;
	static boolean option_batch = false;
	static boolean option_autocommit = false;
	static boolean option_controlrandom = false;
	static boolean option_scaleperstream = false;
	static boolean option_newkey = false;
	static boolean option_isolate = false;
	static boolean option_default = false;
	static boolean option_display_detail = true;

	static boolean option_multipledrivers = false;
	static String testid = null;
	static int driver_number = 1;
	static int total_number_of_drivers = 1;
	static DatabaseConnector driver_database_connector = null;

	static boolean option_responsecurve = false;
	static boolean option_perstreamsummary = false;

	static Properties props = new Properties();
	String jdbc_driver =  null;
	String url = null;
	static String schema = null;
	static String database = null;

	static String unit_of_measure = null;

	String control_cqd_file_name = null;

	int stream_number;
	
	// Bench workload
    static final Object queryfilelock = new Object();
    static volatile String workload_file_name;
    static volatile File workload_file = null;
    static volatile BufferedReader workload_file_reader = null;
    String query_statement = null;

	// run time statistics
	long stream_accumlated_transactions = 0;
	long stream_accumlated_successes = 0;
	long stream_accumlated_errors = 0;
	long stream_accumlated_retries = 0;

	long stream_max_check = 0;
	long stream_min_check = 999999999999999999L;
	
	long stream_accumlated_transaction_response_micro = 0;
	long stream_maximum_transaction_response_micro = 0;
	long stream_minimum_transaction_response_micro = 999999999999999999L;
	long stream_interval_maximum_transaction_response_micro = 0;
	long stream_interval_minimum_transaction_response_micro = 999999999999999999L;

	long stream_accumlated_operations = 0;
	long stream_accumlated_operation_response_micro = 0;
	long stream_maximum_operation_response_micro = 0;
	long stream_minimum_operation_response_micro = 999999999999999999L;

	static int total_transaction_types = 0;
	static String transaction_type_names[];

	long[] stream_accumlated_transactions_by_type;
	long[] stream_accumlated_errors_by_type;
	long[] stream_accumlated_retries_by_type;
	long[] stream_accumlated_successes_by_type;

	long[] stream_accumlated_transaction_response_micro_by_type;
	long[] stream_maximum_transaction_response_micro_by_type;
	long[] stream_minimum_transaction_response_micro_by_type;

	int[] stream_response_distribution = new int[RESPONSE_BUCKETS];

	boolean signal_ready = false;
	boolean signal_proceed = false;
	boolean signal_stop = false;

	static Logger logger = null;

	static void display_message ( String message ) {
		if ( logger != null ) {	logger.info(message); }
		System.out.println(message);
	}

	static void display_error ( String message, Exception exception ) {
		if ( logger != null ) {	logger.error(message, exception); }
		System.out.println(message);
	}

//----------------------------------------------------------------------------------------------------------

	public void run() {

		if (logger.isDebugEnabled()) logger.debug(stream_number + "> WorkloadDriver.run()");

		RandomGenerator random_generator = new RandomGenerator();

		// Initialize Random Number Generator
		random_generator.stream_number = stream_number;
		random_generator.option_controlrandom = option_controlrandom;

		random_generator.start();

		DebitCreditWorkload2 debitcredit = null;
		OrderEntryWorkload2 orderentry = null;
		YCSBWorkload2 ycsb = null;
		TPCHWorkload2 tpch = null;
		BenchWorkload2 bench = null;

//		int stream_transaction_number = 0;

		try {

			switch ( benchmark.toLowerCase() ) {

			case "debitcredit" :
				debitcredit = new DebitCreditWorkload2();

				debitcredit.stream_number = stream_number;
				debitcredit.scale_factor = scale_factor;
				debitcredit.schema = schema;
				debitcredit.database = database;

				debitcredit.scale_per_stream = scale_per_stream;
				debitcredit.option_sessionless = option_sessionless;
				debitcredit.option_prepareexec = option_prepareexec;
				debitcredit.option_directexec = option_directexec;
				debitcredit.option_spj = option_spj;
				debitcredit.option_batch = option_batch;
				debitcredit.option_autocommit = option_autocommit;
				debitcredit.option_controlrandom = option_controlrandom;
				debitcredit.option_newkey = option_newkey;
				debitcredit.option_isolate = option_isolate;
				debitcredit.option_scaleperstream = option_scaleperstream;

				debitcredit.start();

				total_transaction_types = debitcredit.TOTAL_TRANS_TYPES;
				transaction_type_names = debitcredit.TRANS_TYPE_NAMES;
				break;

			case "orderentry" :
				orderentry = new OrderEntryWorkload2();

				orderentry.stream_number = stream_number;
				orderentry.scale_factor = scale_factor;
				orderentry.schema = schema;
				orderentry.database = database;

				orderentry.scale_per_stream = scale_per_stream;
				orderentry.option_sessionless = option_sessionless;
				orderentry.option_prepareexec = option_prepareexec;
				orderentry.option_directexec = option_directexec;
				orderentry.option_specialexec = option_specialexec;
				orderentry.option_spj = option_spj;
				orderentry.option_batch = option_batch;
				orderentry.option_autocommit = option_autocommit;
				orderentry.option_controlrandom = option_controlrandom;
				orderentry.option_newkey = option_newkey;
				orderentry.option_isolate = option_isolate;
				orderentry.option_scaleperstream = option_scaleperstream;

				orderentry.start();

				total_transaction_types = orderentry.TOTAL_TRANS_TYPES;
				transaction_type_names = orderentry.TRANS_TYPE_NAMES;
				break;

			case "ycsb" :
				ycsb = new YCSBWorkload2();

				ycsb.stream_number = stream_number;
				ycsb.scale_factor = scale_factor;
				ycsb.schema = schema;
				ycsb.database = database;

				ycsb.option_prepareexec = option_prepareexec;
				ycsb.option_directexec = option_directexec;
				ycsb.option_controlrandom = option_controlrandom;

				ycsb.start();

				total_transaction_types = ycsb.TOTAL_TRANS_TYPES;
				transaction_type_names = ycsb.TRANS_TYPE_NAMES;
				break;

			case "tpch" :
				tpch = new TPCHWorkload2();

				tpch.stream_number = stream_number;
				tpch.scale_factor = scale_factor;
				tpch.schema = schema;
				tpch.database = database;
				tpch.option_controlrandom = option_controlrandom;
				tpch.option_default = option_default;

				tpch.start();

				total_transaction_types = tpch.TOTAL_TRANS_TYPES;
				transaction_type_names = tpch.TRANS_TYPE_NAMES;
				break;

			case "bench" :
				bench = new BenchWorkload2();

				bench.stream_number = stream_number;
				bench.schema = schema;
				bench.database = database;
				bench.option_controlrandom = option_controlrandom;
				bench.option_default = option_default;

				bench.start();

				total_transaction_types = bench.TOTAL_TRANS_TYPES;
				transaction_type_names = bench.TRANS_TYPE_NAMES;
				break;
			}

			// Tell the main thread that I'm ready to proceed
			signal_ready = true;
			while (!signal_proceed){
				Thread.sleep(100);
			}

			long loops = 0;

			if (logger.isDebugEnabled()) logger.debug(stream_number + "> WorkloadDriver.run().while ");
			while ( !signal_stop && ( loops < number_of_loops ) ) {

				// Up the transaction count
//				synchronized (threadlock) {
				total_transactions++;
				if ( total_transactions > num_of_transactions ) {
					signal_stop = true;
				} else {

					loops++;

					switch ( benchmark.toLowerCase() ) {

					case "debitcredit" :

						debitcredit.runDebitCredit();

						// gather runtime statistics
						stream_accumlated_transactions = debitcredit.accumlated_transactions;
						stream_accumlated_successes = debitcredit.accumlated_successes;
						stream_accumlated_errors = debitcredit.accumlated_errors;
						stream_accumlated_retries = debitcredit.accumlated_retries;

						stream_accumlated_transaction_response_micro = debitcredit.accumlated_transaction_response_micro;
						stream_maximum_transaction_response_micro = debitcredit.maximum_transaction_response_micro;
						stream_minimum_transaction_response_micro = debitcredit.minimum_transaction_response_micro;
						stream_interval_maximum_transaction_response_micro = debitcredit.interval_maximum_transaction_response_micro;
						stream_interval_minimum_transaction_response_micro = debitcredit.interval_minimum_transaction_response_micro;

						stream_accumlated_operations = debitcredit.accumlated_operations;
						stream_accumlated_operation_response_micro = debitcredit.accumlated_operation_response_micro;
						stream_maximum_operation_response_micro = debitcredit.maximum_operation_response_micro;
						stream_minimum_operation_response_micro = debitcredit.minimum_operation_response_micro;

						stream_accumlated_transactions_by_type = debitcredit.accumlated_transactions_by_type;
						stream_accumlated_errors_by_type = debitcredit.accumlated_errors_by_type;
						stream_accumlated_retries_by_type = debitcredit.accumlated_retries_by_type;
						stream_accumlated_successes_by_type = debitcredit.accumlated_successes_by_type;

						stream_accumlated_transaction_response_micro_by_type = debitcredit.accumlated_transaction_response_micro_by_type;
						stream_maximum_transaction_response_micro_by_type = debitcredit.maximum_transaction_response_micro_by_type;
						stream_minimum_transaction_response_micro_by_type = debitcredit.minimum_transaction_response_micro_by_type;
						
						if ( stream_max_check > 0 ) {
							debitcredit.resetInterval(stream_max_check, stream_min_check);
							stream_max_check = 0;
							stream_min_check = 999999999999999999L;
						}
						break;

					case "orderentry" :

						// Transaction mix is 10:10:1:1:1
						// Transaction mix as per spec.   44.5%:43.1%:4.1%:4.2%:4.1%
						// int idx = random_generator.generateRandomInteger( 1, 23 );
						int idx = random_generator.generateRandomInteger( 1, 1000 );

						//if ( idx <= 10 ) {
						if ( idx <= 445 ) {

							orderentry.runNewOrder ();

						//} else if ( idx <= 20 ) {
						} else if ( idx <= 876 ) {

							orderentry.runPayment ();

						//} else if ( idx == 21 ) {
						} else if ( idx <= 917 ) {

							orderentry.runOrderStatus ();

						//} else if ( idx == 22 ) {
						} else if ( idx <= 959 ) {

							orderentry.runDelivery ();

						//} else if ( idx == 23 ) {
						} else {

							orderentry.runStockLevel ();

						}

						// gather runtime statistics
						stream_accumlated_transactions = orderentry.accumlated_transactions;
						stream_accumlated_successes = orderentry.accumlated_successes;
						stream_accumlated_errors = orderentry.accumlated_errors;
						stream_accumlated_retries = orderentry.accumlated_retries;

						stream_accumlated_transactions = orderentry.accumlated_transactions;
						stream_accumlated_transaction_response_micro = orderentry.accumlated_transaction_response_micro;
						stream_maximum_transaction_response_micro = orderentry.maximum_transaction_response_micro;
						stream_minimum_transaction_response_micro = orderentry.minimum_transaction_response_micro;
						stream_interval_maximum_transaction_response_micro = orderentry.interval_maximum_transaction_response_micro;
						stream_interval_minimum_transaction_response_micro = orderentry.interval_minimum_transaction_response_micro;

						stream_accumlated_operations = orderentry.accumlated_operations;
						stream_accumlated_operation_response_micro = orderentry.accumlated_operation_response_micro;
						stream_maximum_operation_response_micro = orderentry.maximum_operation_response_micro;
						stream_minimum_operation_response_micro = orderentry.minimum_operation_response_micro;

						stream_accumlated_transactions_by_type = orderentry.accumlated_transactions_by_type;
						stream_accumlated_errors_by_type = orderentry.accumlated_errors_by_type;
						stream_accumlated_retries_by_type = orderentry.accumlated_retries_by_type;
						stream_accumlated_successes_by_type = orderentry.accumlated_successes_by_type;

						stream_accumlated_transaction_response_micro_by_type = orderentry.accumlated_transaction_response_micro_by_type;
						stream_maximum_transaction_response_micro_by_type = orderentry.maximum_transaction_response_micro_by_type;
						stream_minimum_transaction_response_micro_by_type = orderentry.minimum_transaction_response_micro_by_type;

						if ( stream_max_check > 0 ) {
							orderentry.resetInterval(stream_max_check, stream_min_check);
							stream_max_check = 0;
							stream_min_check = 999999999999999999L;
						}
						break;

					case "ycsb" :

						switch ( workload.toLowerCase() ) {
						case "singletonselect" :
							ycsb.runSingletonSelect();
							break;
						case "singletonupdate" :
							ycsb.runSingletonUpdate();
							break;
						case "singleton5050" :
							if ( random_generator.generateRandomInteger(1, 1000) <= 500) {
								ycsb.runSingletonSelect();
							} else {
								ycsb.runSingletonUpdate();
							}
							break;
						case "singleton2080" :
							if ( random_generator.generateRandomInteger(1, 1000) <= 200) {
								ycsb.runSingletonSelect();
							} else {
								ycsb.runSingletonUpdate();
							}
							break;
						case "singleton9505" :
							if ( random_generator.generateRandomInteger(1, 1000) <= 950) {
								ycsb.runSingletonSelect();
							} else {
								ycsb.runSingletonUpdate();
							}
							break;
						}

						// gather runtime statistics
						stream_accumlated_transactions = ycsb.accumlated_transactions;
						stream_accumlated_successes = ycsb.accumlated_successes;
						stream_accumlated_errors = ycsb.accumlated_errors;
						stream_accumlated_retries = ycsb.accumlated_retries;

						stream_accumlated_transaction_response_micro = ycsb.accumlated_transaction_response_micro;
						stream_maximum_transaction_response_micro = ycsb.maximum_transaction_response_micro;
						stream_minimum_transaction_response_micro = ycsb.minimum_transaction_response_micro;
						stream_interval_maximum_transaction_response_micro = ycsb.interval_maximum_transaction_response_micro;
						stream_interval_minimum_transaction_response_micro = ycsb.interval_minimum_transaction_response_micro;

						stream_accumlated_operations = ycsb.accumlated_operations;
						stream_accumlated_operation_response_micro = ycsb.accumlated_operation_response_micro;
						stream_maximum_operation_response_micro = ycsb.maximum_operation_response_micro;
						stream_minimum_operation_response_micro = ycsb.minimum_operation_response_micro;

						stream_accumlated_transactions_by_type = ycsb.accumlated_transactions_by_type;
						stream_accumlated_errors_by_type = ycsb.accumlated_errors_by_type;
						stream_accumlated_retries_by_type = ycsb.accumlated_retries_by_type;
						stream_accumlated_successes_by_type = ycsb.accumlated_successes_by_type;

						stream_accumlated_transaction_response_micro_by_type = ycsb.accumlated_transaction_response_micro_by_type;
						stream_maximum_transaction_response_micro_by_type = ycsb.maximum_transaction_response_micro_by_type;
						stream_minimum_transaction_response_micro_by_type = ycsb.minimum_transaction_response_micro_by_type;
						
						if ( stream_max_check > 0 ) {
							ycsb.resetInterval(stream_max_check, stream_min_check);
							stream_max_check = 0;
							stream_min_check = 999999999999999999L;
						}
						break;

					case "tpch" :

						for (int idx1 = 0; idx1 < tpch.TOTAL_TRANS_TYPES; idx1++ ) {

							if ( signal_stop != true ) {

								tpch.runQuery ();

								// gather runtime statistics
								stream_accumlated_transactions = tpch.accumlated_transactions;
								stream_accumlated_successes = tpch.accumlated_successes;
								stream_accumlated_errors = tpch.accumlated_errors;
								stream_accumlated_retries = tpch.accumlated_retries;

								stream_accumlated_transactions = tpch.accumlated_transactions;
								stream_accumlated_transaction_response_micro = tpch.accumlated_transaction_response_micro;
								stream_maximum_transaction_response_micro = tpch.maximum_transaction_response_micro;
								stream_minimum_transaction_response_micro = tpch.minimum_transaction_response_micro;
								stream_interval_maximum_transaction_response_micro = tpch.interval_maximum_transaction_response_micro;
								stream_interval_minimum_transaction_response_micro = tpch.interval_minimum_transaction_response_micro;

								stream_accumlated_operations = tpch.accumlated_operations;
								stream_accumlated_operation_response_micro = tpch.accumlated_operation_response_micro;
								stream_maximum_operation_response_micro = tpch.maximum_operation_response_micro;
								stream_minimum_operation_response_micro = tpch.minimum_operation_response_micro;

								stream_accumlated_transactions_by_type = tpch.accumlated_transactions_by_type;
								stream_accumlated_errors_by_type = tpch.accumlated_errors_by_type;
								stream_accumlated_retries_by_type = tpch.accumlated_retries_by_type;
								stream_accumlated_successes_by_type = tpch.accumlated_successes_by_type;

								stream_accumlated_transaction_response_micro_by_type = tpch.accumlated_transaction_response_micro_by_type;
								stream_maximum_transaction_response_micro_by_type = tpch.maximum_transaction_response_micro_by_type;
								stream_minimum_transaction_response_micro_by_type = tpch.minimum_transaction_response_micro_by_type;
								
								if ( stream_max_check > 0 ) {
									tpch.resetInterval(stream_max_check, stream_min_check);
									stream_max_check = 0;
									stream_min_check = 999999999999999999L;
								}
							}
						}

						break;

					case "bench" :

						// Get a query from the input file
				        synchronized (queryfilelock) {
				        	query_statement = workload_file_reader.readLine();
							while ( ( query_statement == null ) && ( !signal_stop ) ) {
								repeat_count++;
								if ( repeat_count >= number_of_repeats ) {
									signal_stop = true;
								} else {
									// reopen the query input file
									workload_file_reader.close();
									workload_file_reader = new BufferedReader( new FileReader ( workload_file ) );
									query_statement = workload_file_reader.readLine();
								}
							}
				        }

				        if ( signal_stop != true ) {

							bench.runQuery (query_statement);

							// gather runtime statistics
							stream_accumlated_transactions = bench.accumlated_transactions;
							stream_accumlated_successes = bench.accumlated_successes;
							stream_accumlated_errors = bench.accumlated_errors;
							stream_accumlated_retries = bench.accumlated_retries;

							stream_accumlated_transactions = bench.accumlated_transactions;
							stream_accumlated_transaction_response_micro = bench.accumlated_transaction_response_micro;
							stream_maximum_transaction_response_micro = bench.maximum_transaction_response_micro;
							stream_minimum_transaction_response_micro = bench.minimum_transaction_response_micro;
							stream_interval_maximum_transaction_response_micro = bench.interval_maximum_transaction_response_micro;
							stream_interval_minimum_transaction_response_micro = bench.interval_minimum_transaction_response_micro;

							stream_accumlated_operations = bench.accumlated_operations;
							stream_accumlated_operation_response_micro = bench.accumlated_operation_response_micro;
							stream_maximum_operation_response_micro = bench.maximum_operation_response_micro;
							stream_minimum_operation_response_micro = bench.minimum_operation_response_micro;

							stream_accumlated_transactions_by_type = bench.accumlated_transactions_by_type;
							stream_accumlated_errors_by_type = bench.accumlated_errors_by_type;
							stream_accumlated_retries_by_type = bench.accumlated_retries_by_type;
							stream_accumlated_successes_by_type = bench.accumlated_successes_by_type;

							stream_accumlated_transaction_response_micro_by_type = bench.accumlated_transaction_response_micro_by_type;
							stream_maximum_transaction_response_micro_by_type = bench.maximum_transaction_response_micro_by_type;
							stream_minimum_transaction_response_micro_by_type = bench.minimum_transaction_response_micro_by_type;

							if ( stream_max_check > 0 ) {
								bench.resetInterval(stream_max_check, stream_min_check);
								stream_max_check = 0;
								stream_min_check = 999999999999999999L;
							}
						}
						break;

					}
				}

				if ( think_time != 0 ) { //  Think Time
					Thread.sleep(random_generator.generateRandomInteger( 0, think_time * 1000));
				}

			}
			if (logger.isDebugEnabled()) logger.debug(stream_number + "< WorkloadDriver.run().while ");

			if ( debitcredit != null ) { debitcredit.close(); }
			if ( orderentry != null ) { orderentry.close(); }
			if ( ycsb != null ) { ycsb.close(); }
			if ( tpch != null ) { tpch.close(); }
			if ( bench != null ) { bench.close(); }

		} catch (Exception exception) {
			stream_accumlated_errors++;
			synchronized (stdoutlock) {
				String tombstone =
						  "\n" + stream_number + ">----------------------------------------------------------"
						+ "\n" + stream_number + "> Unexpected EXCEPTION : " + exception.getMessage()
						+ "\n" + stream_number + ">   Stream Number : " + stream_number
						+ "\n" + stream_number + ">----------------------------------------------------------"
						+ "\n";
				display_error(tombstone, exception);
			}
		}

		if (logger.isDebugEnabled()) logger.debug(stream_number + "< WorkloadDriver.run()");
	}


//----------------------------------------------------------------------------------------------------------

	public static void main(String[] args) {


		long testStartTime = System.currentTimeMillis();

		String command_line = "";
		for (int idx = 0; idx < args.length; idx++) {
			command_line = command_line + args[idx] + " ";
		}

		try {

			testid = System.getenv("TESTID");
			String driverid = System.getenv("DRIVERID");
			String logdirectory = System.getenv("LOGDIRECTORY");

			if ( logdirectory == null ) {
				System.setProperty("logfile.name", "log/" + datetimeformat.format(new Date()) + ".javabench.log4j.log");
			} else {
				System.setProperty("logfile.name", logdirectory + "/" + testid + ".driver_" + driverid + ".log4j.log");
			}
			logger = Logger.getLogger("JavaBench");

			Properties props = new Properties();
			props.load(new FileInputStream("log4j.properties"));
			PropertyConfigurator.configure(props);

			display_message("WorkloadDriver " + command_line);

			// Command Interrupter

			String syntax = "\n"
					+ "WorkloadDriver: WorkloadDriver <benchmark> [<run_option>]\n"
					+ "   <benchmark> : Benchmark to drive (Valid benchmarks:  DebitCredit, OrderEntry, YCSB, TPCH)\n"
					+ "   <run_option> :  \n"
					+ "     workload <workload> : Overrides default workload.\n"
					+ "     schema <schema_name> : Overrides default schema name.\n"
					+ "     scale <scale_factor> : Scale factor of the workload.\n"
					+ "     streams <number_of_streams> : Execute test with <number_of_streams> streams. (Default 1)\n"
					+ "     transactions <num_of_transactions> : Execute the specified number of transactions. (Default 1)\n"
					+ "     loops <num_of_loops> : Execute the specified number of loops of a multiquery workload. (Default 1)\n"
					+ "     repeat <num_of_repeats> : Repeat a BENCH workload n times. (Default 1)\n"
					+ "     transactions <num_of_transactions> : Execute the specified number of transactions. (Default 1)\n"
					+ "     intervals <number_of_intervals> : Length of test in intervals. (Default 1)\n"
					+ "     intervallength <length_of_interval> : Lenght of reporting (and test) interval in seconds. (Default 10)\n"
					+ "     intervalstart <start_interval> : Initial value for the interval. Use a negative integer to have a rampup period. (Default 0)\n"
					+ "     think <think_time> : Specifies think_time in seconds. Actual think is random on [0..think_time] seconds. (Default 0)\n"
					+ "     sessionless : Each transaction/query is executed within a new session.\n"
					+ "     prepareexec : Each statement is executed with a prior prepare. (Incompatible with sessionless).\n"
					+ "     directexec : Each statement is compiled at time of execution. (Default execution mode).\n"
					+ "     autocommit : Enable Auto Commit.\n"
					+ "     controlrandom : Each stream starts with a fixed seed.\n"
					+ "     default : Sepecified to use default generator data.  (H Workload Only).\n"
					+ "     newkey : Make sure each transaction starts with a new branch.\n"
					+ "     isolate : Each stream is issolated (no data collisions).\n"
					+ "     responsecurve : Prints a distribution of response times at the end.\n"
					+ "     batch : Execute statements in batch mode (if possible).\n"
					+ "     spj : transaction executed as java stored procedure. (Currently not implemented.)\n"
					+ "     driver <x> of <y> : support for multiple concurrent drivers.)\n"
					+ "     testid <testid> : indentifies the test id.  Required with driver of syntax.\n"
					;

			// Command Interpreter
			if ( args.length < 1 ) {
				System.out.println(syntax);
				throw new Exception("ERROR : Required <benchmark> argument not provided.");
			}

			benchmark = args[0];

			String option = null;
			for ( int indx = 1; indx < args.length; indx++ ) {
				option = args[indx];
				switch ( option.toLowerCase() ) {
					case "schema": schema = args[++indx]; break;
					case "workload": workload = args[++indx]; break;
					case "scale" : scale_factor = Integer.parseInt(args[++indx]); break;
					case "streams": number_of_streams = Integer.parseInt(args[++indx]); break;
					case "startstream" : start_stream = Integer.parseInt(args[++indx]); break;
					case "branchesperstream" : scale_per_stream = Integer.parseInt(args[++indx]); break;
					case "transactions": num_of_transactions = Long.parseLong(args[++indx]); option_transaction_test = true; break;
					case "loops": number_of_loops = Integer.parseInt(args[++indx]); option_loop_test = true; break;
					case "repeat": number_of_repeats = Integer.parseInt(args[++indx]); option_repeat_test = true; break;
					case "intervals": number_of_intervals = Integer.parseInt(args[++indx]); option_time_test = true; break;
					case "intervallength": length_of_interval = Integer.parseInt(args[++indx]); break;
					case "intervalstart": test_interval = Integer.parseInt(args[++indx]); break;
					case "think": think_time = Integer.parseInt(args[++indx]); break;
					case "sessionless": option_sessionless = true; break;
					case "prepareexec": option_prepareexec = true; break;
					case "directexec": option_directexec = true; break;
					case "specialexec": option_specialexec = true; break;
					case "detail": option_display_detail = true; break;
					case "responsecurve": option_responsecurve = true; break;
					case "controlrandom": option_controlrandom = true; break;
					case "default": option_default = true; break;
					case "newkey": option_newkey = true; break;
					case "isolate": option_isolate = true; break;
					case "autocommit": option_autocommit = true; break;
					case "batch": option_batch = true; break;
					case "perstreamsummary" : option_perstreamsummary = true; break;
					case "scaleperstream" : option_scaleperstream = true; break;
					case "driver" : option_multipledrivers = true;
							driver_number = Integer.parseInt(args[++indx]);
							if ( ! args[++indx].equals("of") ) {
								throw new Exception("ERROR : Invalid driver of syntax.");
							}
							total_number_of_drivers = Integer.parseInt(args[++indx]);
							break;
					case "testid" : testid = args[++indx]; break;

					default: {
						System.out.println(syntax);
						throw new Exception("ERROR : Invalid option specified ( option = " + option + " )");
					}
				}
			}

			switch ( benchmark.toLowerCase() ) {
				case "debitcredit" :
					if ( scale_factor == INVALID_SCALEFACTOR ) {
						System.out.println(syntax);
						throw new Exception("ERROR : Scale factor required for DebitCredit benchmark.");
					}
					unit_of_measure = "TPS";
					break;
				case "orderentry" :
					if ( scale_factor == INVALID_SCALEFACTOR ) {
						System.out.println(syntax);
						throw new Exception("ERROR : Scale factor required for OrderEntry benchmark.");
					}
					unit_of_measure = "TPM";
					break;
				case "tpch" :
					if ( scale_factor == INVALID_SCALEFACTOR ) {
						System.out.println(syntax);
						throw new Exception("ERROR : Scale factor required for TPCH benchmark.");
					}
					unit_of_measure = "QPH";
					option_autocommit = true;
					break;
				case "bench" :
					unit_of_measure = "QPH";
					option_autocommit = true;
//					workload_file_name = "../queries/" + workload + ".queries.sql";  // Workload
//					workload_file = new File(workload_file_name);
					workload_file = new File(workload);
					workload_file_reader = new BufferedReader( new FileReader ( workload_file ) );

					break;
				case "ycsb" :
					unit_of_measure = "OPS";
					option_autocommit = true;
					switch ( workload.toLowerCase() ) {
						case "ycsbsingletonselect" :
						case "singletonselect" :
							workload = "SingletonSelect";
							break;
						case "ycsbsingletonupdate" :
						case "singletonupdate" :
							workload = "SingletonUpdate";
							break;
						case "ycsbsingleton5050" :
						case "singleton5050" :
							workload = "Singleton5050";
							break;
						case "ycsbsingleton9505" :
						case "singleton9505" :
							workload = "Singleton9505";
							break;
						case "ycsbsingleton2080" :
						case "singleton2080" :
							workload = "Singleton2080";
							break;
						default :
							System.out.println(syntax);
							throw new Exception("ERROR : YCSB Benchmark requires a workload. Expected values SingletonSelect | SingletonUpdate | Singleton5050 | Singleton2080 | Singleton9505");
					}
					break;
				default :
					System.out.println(syntax);
					throw new Exception("ERROR : Invalid benchmark provided. ( benchmark = " + benchmark + " ).");
			}

			if ( workload.equals("autocommit") ) {
				option_autocommit = true;
			}

			if ( option_transaction_test && option_time_test ) {
				System.out.println(syntax);
				throw new Exception("ERROR : Both intervals and transactions can not be specified.");
			}

			if ( option_transaction_test ) {
				number_of_intervals = 99999999;
				number_of_loops = 9999999999999999L;
			} else if ( option_time_test ) {
				num_of_transactions = 9999999999999999L;
				number_of_loops = 9999999999999999L;
			} else if ( option_loop_test ) {
				number_of_intervals = 99999999;
				num_of_transactions = 9999999999999999L;
			} else if ( option_repeat_test ) {
				number_of_intervals = 99999999;
				number_of_loops = 9999999999999999L;
				num_of_transactions = 9999999999999999L;
			} else {
				System.out.println(syntax);
				throw new Exception("ERROR : Either minutes or transactions must be specified.");
			}

			if ( ! option_prepareexec && ! option_directexec ) {
				option_prepareexec = true;  // Default prepareexec
			}

			String propFile = System.getProperty("dbconnect.properties");
			if ( propFile == null ) {
				propFile = "dbconnect.properties";
//				throw new Exception("Error : dbconnect.properties is not set. Exiting.");
			}

			// Use dbconnect.properties to get database connection info
			FileInputStream fs = new FileInputStream(new File(propFile));
			props.load(fs);
			database = props.getProperty("database");
			if (schema == null) {
				schema = props.getProperty("schema");
			}
			fs.close();

			if ( option_batch && database.equals("phoenix") ) {
				throw new Exception("ERROR : Option batch is not supported for Phoenix database");
			}

			if ( option_multipledrivers && (testid == null)) {
				throw new Exception("ERROR : Required testid not provided with multiple driver syntax (driver x of y)");
			}

			// turn off all logging ( Phoenix is very noisy. )
//			Logger.getRootLogger().setLevel(Level.OFF);

			display_message("");
			display_message("   WorkloadDriver");
			display_message("");
			display_message("   " + String.format("%20s", "Test Starting" ) + " : " + dateformat.format(new Date()));

			if ( option_multipledrivers ) {
				display_message("   " + String.format("%20s", "MultipleDrivers" ) + " : " + option_multipledrivers);
				display_message("   " + String.format("%20s", "TestID" ) + " : " + testid);
				display_message("   " + String.format("%20s", "DriverNumber" ) + " : " + driver_number);
				display_message("   " + String.format("%20s", "TotalDrivers" ) + " : " + total_number_of_drivers);
			}
			display_message("   " + String.format("%20s", "Benchmark" ) + " : " + benchmark);
			display_message("   " + String.format("%20s", "Workload" ) + " : " + workload);
			display_message("   " + String.format("%20s", "PropertyFile" ) + " : " + propFile);
			display_message("   " + String.format("%20s", "Datebase" ) + " : " + database);
			display_message("   " + String.format("%20s", "Schema" ) + " : " + schema);
			if ( scale_factor != INVALID_SCALEFACTOR ) { display_message("   " + String.format("%20s", "ScaleFactor" ) + " : " + scale_factor); }
			display_message("   " + String.format("%20s", "Streams" ) + " : " + number_of_streams);
			if ( start_stream != 0) { display_message("   " + String.format("%20s", "StartStream" ) + " : " + start_stream); }
			if ( scale_per_stream != 1) { display_message("   " + String.format("%20s", "ScalePerStream" ) + " : " + scale_per_stream); }
			if (option_time_test) {
				display_message("   " + String.format("%20s", "Intervals" ) + " : " + number_of_intervals);
			} else if (option_transaction_test) {
				display_message("   " + String.format("%20s", "Transactions" ) + " : " + num_of_transactions);
			} else if (option_loop_test) {
				display_message("   " + String.format("%20s", "Loops" ) + " : " + number_of_loops);
			} else if (option_repeat_test) {
				display_message("   " + String.format("%20s", "Repeats" ) + " : " + number_of_repeats);
			}
			display_message("   " + String.format("%20s", "IntervalLength" ) + " : " + length_of_interval);
			if ( test_interval != 0) { display_message("   " + String.format("%20s", "IntervalStart" ) + " : " + test_interval); }
			display_message("   " + String.format("%20s", "ThinkTime" ) + " : " + think_time);
			display_message("   " + String.format("%20s", "AutoCommit" ) + " : " + option_autocommit);
			if (option_sessionless) { display_message("   " + String.format("%20s", "Sessionless" ) + " : " + option_sessionless); }
			if (option_directexec) { display_message("   " + String.format("%20s", "DirectExec" ) + " : " + option_directexec); }
			if (option_prepareexec) { display_message("   " + String.format("%20s", "PrepareExec" ) + " : " + option_prepareexec); }
			if (option_specialexec) { display_message("   " + String.format("%20s", "SpecialExec" ) + " : " + option_specialexec); }
			if (option_controlrandom) { display_message("   " + String.format("%20s", "ControlRandom" ) + " : " + option_controlrandom); }
			if (option_newkey) { display_message("   " + String.format("%20s", "NewKey" ) + " : " + option_newkey); }
			if (option_isolate) { display_message("   " + String.format("%20s", "Isolate" ) + " : " + option_isolate); }
			if (option_scaleperstream) { display_message("   " + String.format("%20s", "ScalePerStream" ) + " : " + option_scaleperstream); }
			if (option_responsecurve) { display_message("   " + String.format("%20s", "ResponseCurve" ) + " : " + option_responsecurve); }
			if (option_batch) { display_message("   " + String.format("%20s", "Batch" ) + " : " + option_batch); }
			if (option_default) { display_message("   " + String.format("%20s", "OptionDefault" ) + " : " + option_default); }

			String control_cqd_file_name = System.getenv("TEST_CQD_FILE");
			if ( control_cqd_file_name != null ) {
				display_message("		Control CQD File:	" + control_cqd_file_name);
			}

			if ( scale_per_stream == 0 ) {
				scale_per_stream = scale_factor / number_of_streams;
			}
			WorkloadDriver[] stream = new WorkloadDriver[number_of_streams];
			int threads_started=0;

			if ( option_multipledrivers ) {
				start_stream = ( driver_number - 1 ) * number_of_streams;
			}

			display_message("");
			display_message("   Starting up streams : " + dateformat.format(new Date()) );
			for ( int idx = 0; idx < number_of_streams; idx++ ) {
				stream[idx] = new WorkloadDriver();
				stream[idx].stream_number = idx + start_stream;
				stream[idx].start();
				threads_started++;
			}
			display_message("      " +  threads_started + " streams (threads) launched.");

			display_message("");
			display_message("   Waiting for streams to get ready : " + dateformat.format(new Date()) );
			// Wait for the threads to get ready
			int ready = 0;
			int active = threads_started;
			int startup_interval = 0;
			long begin_time = System.currentTimeMillis();
			while ( (ready < threads_started) && (active == threads_started) ) {
				Thread.sleep(1000);
				active = 0;
				ready = 0;
				for ( int idx = 0; idx < threads_started; idx++ ) {
					if ( stream[idx].isAlive() ) { active++; }
					if ( stream[idx].signal_ready ) { ready++; }
				}

				double duration_seconds = ( System.currentTimeMillis() - begin_time ) / 1000.0;
				if ( duration_seconds > ( ( startup_interval + 1 ) * length_of_interval ) ) {
					startup_interval++;
					display_message("      "
							+ "Interval " + String.format("%5d", startup_interval )
							+ ", Time " + hourformat.format(new Date())
							+ ", Streams " + String.format("%5d", threads_started )
							+ ", Active " + String.format("%5d", active )
							+ ", Ready " + String.format("%5d", ready ) );
				}
			}
			display_message("      " +  ready + " streams (threads) ready.");

			if ( option_multipledrivers ) {
				display_message("");
				display_message("   Tell Master Coordinator that my streams are ready : " + dateformat.format(new Date()) );
				// tell the master coordinator that all my streams are ready.
				driver_database_connector = new DatabaseConnector();
				driver_database_connector.properties_file = "dbconnect.properties";
				driver_database_connector.stream_number = 0;
				driver_database_connector.database = "trafodion";
				driver_database_connector.option_autocommit = true;
				driver_database_connector.establishDatabaseConnection();

				String driver_state = "READY";

				String sql_statement = "insert into JAVABENCH.CONTROL_TABLE"
						+ " ( TEST_ID, DRIVER_NUMBER, DRIVER_STATE,"
						+ "	TOTAL_STREAMS, ACTIVE_STREAMS, DURATION_SECONDS,"
						+ "	ACCUMLATED_TRANSACTIONS, ACCUMLATED_SUCCESSES, ACCUMLATED_ERRORS,"
						+ "	ACCUMLATED_RETRIES, ACCUMLATED_TRANSACTION_RESPONSE_MICRO,"
						+ "	INTVL_MAXIMUM_TRANSACTION_RESPONSE_MICRO, INTVL_MINIMUM_TRANSACTION_RESPONSE_MICRO,"
						+ "	ACCUMLATED_OPERATIONS, ACCUMLATED_OPERATION_RESPONSE_MICRO,"
						+ "	MAXIMUM_TRANSACTION_RESPONSE_MICRO, MINIMUM_TRANSACTION_RESPONSE_MICRO"
						+ " ) values ( '" + testid + "'," + driver_number + ",'" + driver_state + "',"
						+ ready + ", 0, " + ready +", "
						+ " 0, 0, 0, 0, 0, 0, 999999999999999999, 0, 0, 0, 999999999999999999)";
				driver_database_connector.execute_statement(sql_statement);

				PreparedStatement select_state_stmt = null;
				ResultSet resultset = null;

				display_message("");
				display_message("   Waiting for Go Ahead signal from Master Coordinator : " + dateformat.format(new Date()) );

				// Wait for a go signal from master coordinator
				while ( driver_state.equals("READY") ) {
					Thread.sleep(1000);
					if ( select_state_stmt == null ) {
						sql_statement = "select DRIVER_STATE from JAVABENCH.CONTROL_TABLE"
								+ " where TEST_ID = '" + testid + "' and DRIVER_NUMBER = " + driver_number;
						select_state_stmt = driver_database_connector.prepare_statement (sql_statement);
					}
					resultset = driver_database_connector.execute_prepared_query(select_state_stmt);
					resultset.next();  // Expecting exactly 1 row
					driver_state = resultset.getString("DRIVER_STATE").trim();
				}

				display_message("");
				display_message("   Master Coordinator signal received ( DRIVER_STATE = " + driver_state + " ) : " + dateformat.format(new Date()) );
			}

			display_message("");
			display_message("   Telling all streams to proceed : " + dateformat.format(new Date()) );
			// Tell all threads to proceed
			for ( int idx = 0; idx < threads_started; idx++ ) {
				stream[idx].signal_proceed = true;
			}

			display_message("");
			display_message("   Streams running : " + dateformat.format(new Date()) );

			// Let the streams run till either number of transactions is exceeded or time is exceeded
			int running = threads_started;
//			long interval_time = System.currentTimeMillis();;
			double duration_seconds = 0.0;

			// Report Header
			display_message("");
			String response_time_units;
			if ( unit_of_measure.equals("TPS") || unit_of_measure.equals("OPS") || unit_of_measure.equals("TPM") ) {
				response_time_units = "ms";
			} else {
				response_time_units = "s";
			}
			String intvl_title_str;
			String intvl_units_str;
			String intvl_data_str;
			if (option_display_detail) {
				intvl_title_str = "<-----------------Interval Data------------------->";
				intvl_units_str = String.format("%7s", "Trans" )
						+ ", " + String.format("%9s", unit_of_measure )
						+ ", " + String.format("%9s", "AvgRsp_" + response_time_units )
						+ ", " + String.format("%9s", "MinRsp_" + response_time_units )
						+ ", " + String.format("%9s", "MaxRsp_" + response_time_units );
			} else {
				intvl_title_str = "<-------Interval Data------>";
				intvl_units_str = String.format("%7s", "Trans" )
							+ ", " + String.format("%9s", unit_of_measure )
							+ ", " + String.format("%9s", "AvgRsp_" + response_time_units );
			}
 
			display_message( String.format("%42s", " " )
					+  intvl_title_str + "   "
					+ "<-----------------Accumulated Data----------------->"
					);
			display_message("XXX, "
					+ String.format("%5s", "Intvl" )
					+ ", " + String.format("%8s", "Time" )
					+ ", " + String.format("%5s", "Secs" )
					+ ", " + String.format("%5s", "Strms" )
					+ ", " + String.format("%4s", "Errs" )
					+ ", " + intvl_units_str
					+ ", " + String.format("%8s", "Trans" )
					+ ", " + String.format("%9s", unit_of_measure )
					+ ", " + String.format("%9s", "AvgRsp_" + response_time_units )
					+ ", " + String.format("%9s", "MinRsp_" + response_time_units )
					+ ", " + String.format("%9s", "MaxRsp_" + response_time_units )
	//				+ ", " + String.format("%7s", "TtlTrans" )
	//				+ ", " + String.format("%6s", "TtlResp(us)" )
					);

			double interval_seconds = 0;
			double prior_seconds = 0;

			begin_time = System.currentTimeMillis();;

			// gather run time statistics
			long accumlated_transactions = 0;
			long interval_transactions = 0;
			long prior_transactions = 0;
			long accumlated_successes = 0;
			long accumlated_errors = 0;
			long accumlated_retries = 0;

			long accumlated_transaction_response_micro = 0;
			long interval_transaction_response_micro = 0;
			long prior_transaction_response_micro = 0;
			long intvl_maximum_transaction_response_micro = 0;
			long intvl_minimum_transaction_response_micro = 999999999999999999L;
			long maximum_transaction_response_micro = 0;
			long minimum_transaction_response_micro = 999999999999999999L;

			long accumlated_operations = 0;
			long accumlated_operation_response_micro = 0;
			long maximum_operation_response_micro = 0;
			long minimum_operation_response_micro = 999999999999999999L;

			double throughput = 0.0;
			double average_response = 0.0;
			double minimum_response = 99999999999.9;
			double maximum_response = 0.0;
			double total_response = 0.0;

			double interval_throughput = 0.0;
			double interval_average_response = 0.0;
			double interval_minimum_response = 0.0;
			double interval_maximum_response = 0.0;

			PreparedStatement update_runtime_stats_stmt = null;

			boolean test_stopping = false;
			String search_string = "XXX";

			while ( running > 0 ) {

				Thread.sleep(1000);

				running = 0;
				for ( int idx = 0; idx < threads_started; idx++ ) {
					if (stream[idx].isAlive()) { running++; }
				}

				if ( ( total_transactions >=  num_of_transactions ) && ( ! test_stopping ) ) {
					test_stopping = true;
					display_message("");
					display_message("   Telling all streams to stop : " + dateformat.format(new Date()) );
					for ( int idx = 0; idx < threads_started; idx++ ) {
						if (stream[idx].isAlive()) { stream[idx].signal_stop = true; }
					}
					display_message("");
				}

				if ( ( ( ( System.currentTimeMillis() - begin_time ) / ( 1000 * length_of_interval ) ) > test_interval ) || ( running == 0 ) ) {
					test_interval++;
					if ( test_interval == 0 ) {
						begin_time = System.currentTimeMillis();;
					}
					if ( test_interval > 0 ) {
						duration_seconds = ( System.currentTimeMillis() - begin_time ) / 1000.0;
					}
					interval_seconds = duration_seconds - prior_seconds;
					prior_seconds = duration_seconds;

					// gather run time statistics
					accumlated_transactions = 0;
					accumlated_successes = 0;
					accumlated_errors = 0;
					accumlated_retries = 0;

					accumlated_transaction_response_micro = 0;
					intvl_maximum_transaction_response_micro = 0;
					intvl_minimum_transaction_response_micro = 999999999999999999L;

					accumlated_operations = 0;
					accumlated_operation_response_micro = 0;
					maximum_operation_response_micro = 0;
					minimum_operation_response_micro = 999999999999999999L;

					for ( int idx = 0; idx < threads_started; idx++ ) {
						accumlated_transactions = accumlated_transactions + stream[idx].stream_accumlated_transactions;
						accumlated_successes = accumlated_successes + stream[idx].stream_accumlated_successes;
						accumlated_errors = accumlated_errors + stream[idx].stream_accumlated_errors;
						accumlated_retries = accumlated_retries + stream[idx].stream_accumlated_retries;

						accumlated_transaction_response_micro = accumlated_transaction_response_micro + stream[idx].stream_accumlated_transaction_response_micro;
						if (stream[idx].stream_interval_minimum_transaction_response_micro < intvl_minimum_transaction_response_micro ) {
							intvl_minimum_transaction_response_micro = stream[idx].stream_interval_minimum_transaction_response_micro;
						}
						if (stream[idx].stream_interval_maximum_transaction_response_micro > intvl_maximum_transaction_response_micro ) {
							intvl_maximum_transaction_response_micro = stream[idx].stream_interval_maximum_transaction_response_micro;
						}
						
						stream[idx].stream_max_check = stream[idx].stream_interval_maximum_transaction_response_micro;
						stream[idx].stream_min_check = stream[idx].stream_interval_minimum_transaction_response_micro;

						accumlated_operations = accumlated_operations + stream[idx].stream_accumlated_operations;
						accumlated_operation_response_micro = accumlated_operation_response_micro + stream[idx].stream_accumlated_operation_response_micro;
						if (stream[idx].stream_minimum_operation_response_micro < minimum_operation_response_micro ) {
							minimum_operation_response_micro = stream[idx].stream_minimum_operation_response_micro;
						}
						if (stream[idx].stream_maximum_operation_response_micro > maximum_operation_response_micro ) {
							maximum_operation_response_micro = stream[idx].stream_maximum_operation_response_micro;
						}
					}
						
					interval_transactions = accumlated_transactions - prior_transactions;
					prior_transactions = accumlated_transactions;

					interval_transaction_response_micro = accumlated_transaction_response_micro - prior_transaction_response_micro;
					prior_transaction_response_micro = accumlated_transaction_response_micro;

					throughput = 0.0;
					average_response = 0.0;
					interval_minimum_response = 0.0;
					interval_maximum_response = 0.0;

					if ( duration_seconds > 0 ) {
						if ( unit_of_measure.equals("TPS") || unit_of_measure.equals("OPS") ) {
							throughput =  accumlated_transactions * 100 / duration_seconds / 100.0 ;
						} else if ( unit_of_measure.equals("TPM") ) {
							throughput =  60 * accumlated_transactions * 100 / duration_seconds / 100.0 ;
						} else if ( unit_of_measure.equals("QPH") || unit_of_measure.equals("RPH") ) {
							throughput =  3600 * accumlated_transactions * 100 / duration_seconds / 100.0 ;
						}
					}

					if ( interval_seconds > 0 ) {
						if ( unit_of_measure.equals("TPS") || unit_of_measure.equals("OPS") ) {
							interval_throughput =  interval_transactions * 100 / interval_seconds / 100.0 ;
						} else if ( unit_of_measure.equals("TPM") ) {
							interval_throughput =  60 * interval_transactions * 100 / interval_seconds / 100.0 ;
						} else if ( unit_of_measure.equals("QPH") || unit_of_measure.equals("RPH") ) {
							interval_throughput =  3600 * interval_transactions * 100 / interval_seconds / 100.0 ;
						}
					}
					if ( unit_of_measure.equals("TPS") || unit_of_measure.equals("OPS") || unit_of_measure.equals("TPM") ) {
						if ( accumlated_transactions > 0 ) {
							average_response = ( accumlated_transaction_response_micro / accumlated_transactions / 1000.0 );
							interval_minimum_response = ( intvl_minimum_transaction_response_micro / 1000.0 );
							interval_maximum_response = ( intvl_maximum_transaction_response_micro / 1000.0 );
						}
						if ( interval_transactions > 0 ) {
							interval_average_response = ( interval_transaction_response_micro / interval_transactions / 1000.0 );
						}

					} else if ( unit_of_measure.equals("QPH") || unit_of_measure.equals("RPH") ) {
						if ( accumlated_transactions > 0 ) {
							average_response = ( accumlated_transaction_response_micro / accumlated_transactions / 1000000.0 );
							interval_minimum_response = ( intvl_minimum_transaction_response_micro / 1000000.0 );
							interval_maximum_response = ( intvl_maximum_transaction_response_micro / 1000000.0 );
						}
						if ( interval_transactions > 0 ) {
							interval_average_response = ( interval_transaction_response_micro / interval_transactions / 1000000.0 );
						}

					}

					if (option_display_detail) {
						intvl_data_str = String.format("%7d", interval_transactions )
								+ ", " + String.format("%9.2f", interval_throughput )
								+ ", " + String.format("%9.3f", interval_average_response )
								+ ", " + String.format("%9.3f", interval_minimum_response )
								+ ", " + String.format("%9.2f", interval_maximum_response );
					} else {
						intvl_data_str = String.format("%7d", interval_transactions )
								+ ", " + String.format("%9.2f", interval_throughput )
								+ ", " + String.format("%9.3f", interval_average_response );
					}
					
					if (intvl_minimum_transaction_response_micro < minimum_transaction_response_micro)
						minimum_transaction_response_micro = intvl_minimum_transaction_response_micro;
					if (intvl_maximum_transaction_response_micro > maximum_transaction_response_micro)
						maximum_transaction_response_micro = intvl_maximum_transaction_response_micro;
					if (interval_minimum_response < minimum_response )
						minimum_response = interval_minimum_response;
					if (interval_maximum_response > maximum_response )
						maximum_response = interval_maximum_response;
					synchronized (stdoutlock) {
						display_message(search_string + ", "
							+ String.format("%5d", test_interval )
							+ ", " + hourformat.format(new Date())
							+ ", " + String.format("%5.0f", duration_seconds )
							+ ", " + String.format("%5d", running )
							+ ", " + String.format("%4d", accumlated_errors )
							+ ", " + intvl_data_str
							+ ", " + String.format("%8d", accumlated_transactions )
							+ ", " + String.format("%9.2f", throughput )
							+ ", " + String.format("%9.3f", average_response )
							+ ", " + String.format("%9.3f", minimum_response )
							+ ", " + String.format("%9.2f", maximum_response )
//							+ ", " + String.format("%8d", accumlated_transactions )
//							+ ", " + accumlated_transaction_response_micro
							);
					}

					if ( option_multipledrivers ) {
						// update our runtime stats for master coordinator.
						if ( update_runtime_stats_stmt == null ) {
							String sql_statement = "update JAVABENCH.CONTROL_TABLE"
								+ " set "
								+ " ACTIVE_STREAMS = ?"
								+ ", DURATION_SECONDS = ?"
								+ ", ACCUMLATED_TRANSACTIONS = ?"
								+ ", ACCUMLATED_SUCCESSES = ?"
								+ ", ACCUMLATED_ERRORS = ?"
								+ ", ACCUMLATED_RETRIES = ?"
								+ ", ACCUMLATED_TRANSACTION_RESPONSE_MICRO = ?"
								+ ", INTVL_MAXIMUM_TRANSACTION_RESPONSE_MICRO = ?"
								+ ", INTVL_MINIMUM_TRANSACTION_RESPONSE_MICRO = ?"
								+ ", ACCUMLATED_OPERATIONS = ?"
								+ ", ACCUMLATED_OPERATION_RESPONSE_MICRO = ?"
								+ ", MAXIMUM_TRANSACTION_RESPONSE_MICRO = ?"
								+ ", MINIMUM_TRANSACTION_RESPONSE_MICRO = ?"
								+ " where TEST_ID = '" + testid + "' and DRIVER_NUMBER = " + driver_number;
							update_runtime_stats_stmt = driver_database_connector.prepare_statement (sql_statement);
						}
						update_runtime_stats_stmt.setInt(1,running);
						update_runtime_stats_stmt.setDouble(2,duration_seconds);
						update_runtime_stats_stmt.setLong(3,accumlated_transactions);
						update_runtime_stats_stmt.setLong(4,accumlated_successes);
						update_runtime_stats_stmt.setLong(5,accumlated_errors);
						update_runtime_stats_stmt.setLong(6,accumlated_retries);
						update_runtime_stats_stmt.setLong(7,accumlated_transaction_response_micro);
						update_runtime_stats_stmt.setLong(8,intvl_maximum_transaction_response_micro);
						update_runtime_stats_stmt.setLong(9,intvl_minimum_transaction_response_micro);
						update_runtime_stats_stmt.setLong(10,accumlated_operations);
						update_runtime_stats_stmt.setLong(11,accumlated_operation_response_micro);
						update_runtime_stats_stmt.setLong(12,maximum_transaction_response_micro);
						update_runtime_stats_stmt.setLong(13,minimum_transaction_response_micro);
						driver_database_connector.execute_prepared_update_statement(update_runtime_stats_stmt);

					}

					if ( ( test_interval == number_of_intervals ) &&  ( ! test_stopping ) ) {
						test_stopping = true;
						display_message("");
						display_message("   Telling all streams to stop : " + dateformat.format(new Date()) );
						search_string = "xxx";
						for ( int idx = 0; idx < threads_started; idx++ ) {
							if (stream[idx].isAlive()) { stream[idx].signal_stop = true; }
						}
						display_message("");
					}
 				}
			}

/**			if ( running > 0 ) {

				display_message("");
				display_message("   Telling all streams to stop : " + dateformat.format(new Date()) );
				for ( int idx = 0; idx < threads_started; idx++ ) {
					if (stream[idx].isAlive()) { stream[idx].signal_stop = true; }
				}

				display_message("");
				display_message("   Wait for streams to Stop : " + dateformat.format(new Date()) );
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
						display_message("      "
								+ "Interval " + String.format("%5d", interval )
								+ ", Streams " + String.format("%5d", threads_started )
								+ ", Active " + String.format("%5d", active ) );
					}
				}
			}
**/
			display_message("");
			display_message("   Gather up final results : " + dateformat.format(new Date()) );

			duration_seconds = ( System.currentTimeMillis() - begin_time ) / 1000.0;
			int[] response_distribution = new int[RESPONSE_BUCKETS];

			if ( option_perstreamsummary ) {
				display_message("");
				display_message("      Summary Per Stream");
				display_message("");
				display_message("   "
							+ String.format("%8s", "Strm" )
							+ ", " + String.format("%10s", "TotalOps" )
							+ ", " + String.format("%10s", "AvgRsp(" + response_time_units + ")" )
							+ ", " + String.format("%10s", "MinRsp(" + response_time_units + ")" )
							+ ", " + String.format("%10s", "MaxRsp(" + response_time_units + ")" )
							+ ", " + String.format("%12s", "TotRsp(" + response_time_units + ")" )
							);
			}

			accumlated_transactions = 0;
			accumlated_successes = 0;
			accumlated_errors = 0;
			accumlated_retries = 0;

			accumlated_transaction_response_micro = 0;
			maximum_transaction_response_micro = 0;
			minimum_transaction_response_micro = 999999999999999999L;

			accumlated_operations = 0;
			accumlated_operation_response_micro = 0;
			maximum_operation_response_micro = 0;
			minimum_operation_response_micro = 999999999999999999L;

			for ( int idx = 0; idx < threads_started; idx++ ) {

				accumlated_transactions = accumlated_transactions + stream[idx].stream_accumlated_transactions;
				accumlated_successes = accumlated_successes + stream[idx].stream_accumlated_successes;
				accumlated_errors = accumlated_errors + stream[idx].stream_accumlated_errors;
				accumlated_retries = accumlated_retries + stream[idx].stream_accumlated_retries;

				accumlated_transaction_response_micro = accumlated_transaction_response_micro + stream[idx].stream_accumlated_transaction_response_micro;

				if (stream[idx].stream_minimum_transaction_response_micro < minimum_transaction_response_micro ) {
					minimum_transaction_response_micro = stream[idx].stream_minimum_transaction_response_micro;
				}
				if (stream[idx].stream_maximum_transaction_response_micro > maximum_transaction_response_micro ) {
					maximum_transaction_response_micro = stream[idx].stream_maximum_transaction_response_micro;
				}

				accumlated_operations = accumlated_operations + stream[idx].stream_accumlated_operations;
				accumlated_operation_response_micro = accumlated_operation_response_micro + stream[idx].stream_accumlated_operation_response_micro;
				if (stream[idx].stream_minimum_operation_response_micro < minimum_operation_response_micro ) {
					minimum_operation_response_micro = stream[idx].stream_minimum_operation_response_micro;
				}
				if (stream[idx].stream_maximum_operation_response_micro > maximum_operation_response_micro ) {
					maximum_operation_response_micro = stream[idx].stream_maximum_operation_response_micro;
				}

				if ( option_perstreamsummary ) {
					average_response = 0.0;
					if ( unit_of_measure.equals("TPS") || unit_of_measure.equals("OPS") || unit_of_measure.equals("TPM") ) {
						if ( stream[idx].stream_accumlated_transactions > 0 ) {
							average_response = ( stream[idx].stream_accumlated_transaction_response_micro / stream[idx].stream_accumlated_transactions / 1000.0 );
						}
						display_message("   " +  String.format("%8d", idx )
								+ ", " + String.format("%10d", stream[idx].stream_accumlated_transactions )
								+ ", " + String.format("%10.3f", average_response )
								+ ", " + String.format("%10.3f", ( stream[idx].stream_minimum_transaction_response_micro / 1000.0 ) )
								+ ", " + String.format("%10.3f", ( stream[idx].stream_maximum_transaction_response_micro / 1000.0 ) )
								+ ", " + String.format("%12.3f", ( stream[idx].stream_accumlated_transaction_response_micro / 1000.0 ) )
								);

					} else if ( unit_of_measure.equals("QPH") || unit_of_measure.equals("RPH") ) {
						if ( stream[idx].stream_accumlated_transactions > 0 ) {
							average_response = ( stream[idx].stream_accumlated_transaction_response_micro / stream[idx].stream_accumlated_transactions / 1000000.0 );
						}
						display_message("   " +  String.format("%8d", idx )
								+ ", " + String.format("%10d", stream[idx].stream_accumlated_transactions )
								+ ", " + String.format("%10.3f", average_response )
								+ ", " + String.format("%10.3f", ( stream[idx].stream_minimum_transaction_response_micro / 1000000.0 ) )
								+ ", " + String.format("%10.3f", ( stream[idx].stream_maximum_transaction_response_micro / 1000000.0 ) )
								+ ", " + String.format("%12.3f", ( stream[idx].stream_accumlated_transaction_response_micro / 1000000.0 ) )
								);
					}
				}
			}

			if (option_responsecurve) {
				display_message("");
				display_message("      Response Time Buckets");
				for (int bucket = 0; bucket < RESPONSE_BUCKETS; bucket++ ) {
					display_message("		ResponseDistribution[" + bucket + "] = " + response_distribution[bucket] );
				}
			}

			double ops = 0.0;
			double average_ops_per_trans = 0.0;
			if ( duration_seconds > 0 ) {
				ops =  ( accumlated_operations * 100 / duration_seconds / 100.0 );
			}
			if ( accumlated_transactions > 0 ) {
				average_ops_per_trans = ( 100.0 * accumlated_operations / accumlated_transactions / 100.0 );
			}

			display_message("");
			display_message("   Test Results:");
			display_message("");
			display_message("   " + String.format("%20s", "Total Intervals" ) + " : " + test_interval);
			display_message("   " + String.format("%20s", "Duration (secs)" ) + " : " +  duration_seconds);
			display_message("   " + String.format("%20s", "Concurrency" ) + " : " +  threads_started);
			display_message("");
			display_message("   " + String.format("%20s", "Total Transactions" ) + " : " +  accumlated_transactions);
			display_message("   " + String.format("%20s", "Total Successes" ) + " : " +  accumlated_successes);
			display_message("   " + String.format("%20s", "Total Failures" ) + " : " +  accumlated_errors);
			display_message("   " + String.format("%20s", "Total Retries" ) + " : " +  accumlated_retries);
			display_message("");
			display_message("   " + String.format("%20s", "Throughput (" + unit_of_measure + ")" ) + " : " +  String.format("%.2f", throughput ) );
			display_message("   " + String.format("%20s", "Average Latency (" + response_time_units + ")" ) + " : " +  String.format("%.3f", average_response ) );
			display_message("   " + String.format("%20s", "Minimum Latency (" + response_time_units + ")" ) + " : " +  String.format("%.3f", minimum_response ) );
			display_message("   " + String.format("%20s", "Maximum Latency (" + response_time_units + ")" ) + " : " +  String.format("%.3f", maximum_response ) );
			display_message("   " + String.format("%20s", "Total Latency (us)" ) + " : " + accumlated_transaction_response_micro);
			display_message("");
			display_message("   " + String.format("%20s", "Total Operations" ) + " : " +  accumlated_operations );
			display_message("   " + String.format("%20s", "Avg Opers per Trans" ) + " : " +  String.format("%.2f", average_ops_per_trans ) );
			display_message("   " + String.format("%20s", "OPS" ) + " : " +  String.format("%.2f", ops) );


			// Transaction Detail Report Header
			display_message("");
			display_message("   Summary by Transaction Type:");
			display_message("");
			display_message("   "
						+ String.format("%20s", "TransType" )
						+ ", " + String.format("%10s", "TotalTrans" )
						+ ", " + String.format("%10s", "TotalErrs" )
						+ ", " + String.format("%10s", "TtlRetrys" )
						+ ", " + String.format("%10s", "TtlSuccess" )
						+ ", " + String.format("%10s", "AvgRsp(" + response_time_units + ")" )
						+ ", " + String.format("%10s", "MinRsp(" + response_time_units + ")" )
						+ ", " + String.format("%10s", "MaxRsp(" + response_time_units + ")" )
						+ ", " + String.format("%12s", "TotRsp(" + response_time_units + ")" )
						);

			for (int transaction_type = 0; transaction_type < total_transaction_types; transaction_type++ ) {

				long accumlated_transactions_by_type = 0;
				long accumlated_errors_by_type = 0;
				long accumlated_retries_by_type = 0;
				long accumlated_successes_by_type = 0;

				long accumlated_transaction_response_micro_by_type = 0;
				long maximum_transaction_response_micro_by_type = 0;
				long minimum_transaction_response_micro_by_type = 999999999999999999L;

				for ( int idx = 0; idx < threads_started; idx++ ) {
					accumlated_transactions_by_type = accumlated_transactions_by_type + stream[idx].stream_accumlated_transactions_by_type[transaction_type];
					accumlated_errors_by_type = accumlated_errors_by_type + stream[idx].stream_accumlated_errors_by_type[transaction_type];
					accumlated_retries_by_type = accumlated_retries_by_type + stream[idx].stream_accumlated_retries_by_type[transaction_type];
					accumlated_successes_by_type = accumlated_successes_by_type + stream[idx].stream_accumlated_successes_by_type[transaction_type];

					accumlated_transaction_response_micro_by_type = accumlated_transaction_response_micro_by_type + stream[idx].stream_accumlated_transaction_response_micro_by_type[transaction_type];

					if (stream[idx].stream_minimum_transaction_response_micro_by_type[transaction_type] < minimum_transaction_response_micro_by_type ) {
						minimum_transaction_response_micro_by_type = stream[idx].stream_minimum_transaction_response_micro_by_type[transaction_type];
					}
					if (stream[idx].stream_maximum_transaction_response_micro_by_type[transaction_type] > maximum_transaction_response_micro_by_type ) {
						maximum_transaction_response_micro_by_type = stream[idx].stream_maximum_transaction_response_micro_by_type[transaction_type];
					}

				}

				average_response = 0.0;
				minimum_response = 0.0;
				maximum_response = 0.0;
				total_response = 0.0;
				if ( accumlated_transactions_by_type > 0 ) {
					if ( unit_of_measure.equals("TPS") || unit_of_measure.equals("OPS") || unit_of_measure.equals("TPM") ) {
						average_response =  ( accumlated_transaction_response_micro_by_type / accumlated_transactions_by_type / 1000.0 );
						minimum_response = ( minimum_transaction_response_micro_by_type / 1000.0 );
						maximum_response = ( maximum_transaction_response_micro_by_type / 1000.0 );
						total_response = ( accumlated_transaction_response_micro_by_type / 1000.0 );

					} else if ( unit_of_measure.equals("QPH") || unit_of_measure.equals("RPH") ) {
						average_response =  ( accumlated_transaction_response_micro_by_type / accumlated_transactions_by_type / 1000000.0 );
						minimum_response = ( minimum_transaction_response_micro_by_type / 1000000.0 );
						maximum_response = ( maximum_transaction_response_micro_by_type / 1000000.0 );
						total_response = ( accumlated_transaction_response_micro_by_type / 1000000.0 );

					}
				}
				display_message(
					  "   " + String.format("%20s", transaction_type_names[transaction_type] )
					+ ", " + String.format("%10d", accumlated_transactions_by_type )
					+ ", " + String.format("%10d", accumlated_errors_by_type )
					+ ", " + String.format("%10d", accumlated_retries_by_type )
					+ ", " + String.format("%10d", accumlated_successes_by_type )
					+ ", " + String.format("%10.3f", average_response )
					+ ", " + String.format("%10.3f", minimum_response )
					+ ", " + String.format("%10.3f", maximum_response )
					+ ", " + String.format("%12.3f", total_response )
					);

				if ( option_multipledrivers ) {
					String sql_statement = "insert into JAVABENCH.CONTROL_TYPE_SUMMARY_TABLE ( "
							+ "  TEST_ID"
							+ ", DRIVER_NUMBER"
							+ ", TRANSACTION_TYPE"
							+ ", ACCUMLATED_TRANSACTIONS"
							+ ", ACCUMLATED_SUCCESSES"
							+ ", ACCUMLATED_ERRORS"
							+ ", ACCUMLATED_RETRIES"
							+ ", ACCUMLATED_TRANSACTION_RESPONSE_MICRO"
							+ ", MAXIMUM_TRANSACTION_RESPONSE_MICRO"
							+ ", MINIMUM_TRANSACTION_RESPONSE_MICRO"
							+ " ) values ("
							+ " '" + testid + "'"
							+ ", " + driver_number
							+ ",'" + transaction_type_names[transaction_type] + "'"
							+ ", " + accumlated_transactions_by_type
							+ ", " + accumlated_successes_by_type
							+ ", " + accumlated_errors_by_type
							+ ", " + accumlated_retries_by_type
							+ ", " + accumlated_transaction_response_micro_by_type
							+ ", " + maximum_transaction_response_micro_by_type
							+ ", " + minimum_transaction_response_micro_by_type
							+ " )";
					driver_database_connector.execute_statement(sql_statement);
				}
			}

			if ( option_multipledrivers ) {
				// tell the master coordinator that all my streams are done and final runtime stats.

				String sql_statement = "update JAVABENCH.CONTROL_TABLE set "
						+ " DRIVER_STATE = 'DONE'"
						+ ", ACTIVE_STREAMS = 0"
						+ ", DURATION_SECONDS = " + duration_seconds
						+ ", ACCUMLATED_TRANSACTIONS = " + accumlated_transactions
						+ ", ACCUMLATED_SUCCESSES = " + accumlated_successes
						+ ", ACCUMLATED_ERRORS = " + accumlated_errors
						+ ", ACCUMLATED_RETRIES = " + accumlated_retries
						+ ", ACCUMLATED_TRANSACTION_RESPONSE_MICRO = " + accumlated_transaction_response_micro
						+ ", MAXIMUM_TRANSACTION_RESPONSE_MICRO = " + maximum_transaction_response_micro
						+ ", MINIMUM_TRANSACTION_RESPONSE_MICRO = " + minimum_transaction_response_micro
						+ ", ACCUMLATED_OPERATIONS = " + accumlated_operations
						+ ", ACCUMLATED_OPERATION_RESPONSE_MICRO = " + accumlated_operation_response_micro
//						+ ", MAXIMUM_OPERATION_RESPONSE_MICRO = " + maximum_operation_response_micro
//						+ ", MINIMUM_OPERATION_RESPONSE_MICRO = " + minimum_operation_response_micro
						+ " where TEST_ID = '" + testid + "' and DRIVER_NUMBER = " + driver_number;
				driver_database_connector.execute_statement(sql_statement);

			}

		} catch (Exception exception) {
			synchronized (stdoutlock) {
				String tombstone = "";
				tombstone = tombstone + "\n----------------------------------------------------------";
				tombstone = tombstone + "\n Unexpected EXCEPTION : " + exception.getMessage();
				tombstone = tombstone + "\n----------------------------------------------------------";
				display_error(tombstone, exception);
			}
		}

		display_message("");
		display_message("   " + String.format("%20s", "Test Stopping" ) + " : " + dateformat.format(new Date()));
		display_message("   " + String.format("%20s", "Elapsed Seconds" ) + " : " + String.format("%7.3f", (( System.currentTimeMillis() - testStartTime ) / 1000.0) ));
		display_message("");

	}
}
