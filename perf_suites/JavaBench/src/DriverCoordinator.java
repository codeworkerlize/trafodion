import java.io.FileInputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class DriverCoordinator {

	final static SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	final static SimpleDateFormat hourformat = new SimpleDateFormat("HH:mm:ss");

	static int length_of_interval = 60;
	static int test_interval = 0;

	static String testid = null;
	static int total_number_of_drivers = 0;
	static DatabaseConnector driver_database_connector = null;

	static String benchmark = null;
	static String unit_of_measure = null;

	static Logger logger = null;

	static void display_message ( String message ) {
		logger.info(message);;
		System.out.println(message);
	}

//----------------------------------------------------------------------------------------------------------

	public static void main(String[] args) {


		long testStartTime = System.currentTimeMillis();

		String command_line = "";
		for (int idx = 0; idx < args.length; idx++) {
			command_line = command_line + args[idx] + " ";
		}

		try {

			String testid = System.getenv("TESTID");
			String logdirectory = System.getenv("LOGDIRECTORY");

			System.setProperty("logfile.name", logdirectory + "/" + testid + ".coordinator.log4j.log");
			logger = Logger.getLogger("JavaBench");

			Properties props = new Properties();
			props.load(new FileInputStream("log4j.properties"));
			PropertyConfigurator.configure(props);

			display_message("DriverCoordinator " + command_line);

			// Command Interrupter

			String syntax = "\n"
					+ "DriverCoodinator: DriverCoodinator <benchmark> testid <testid> drivers <num_of_drivers>\n"
					;

			benchmark = args[0];
			if ( benchmark.equals("DebitCredit") ) {
				unit_of_measure = "TPS";
			} else if  ( benchmark.equals("OrderEntry") ) {
				unit_of_measure = "TPM";
			} else if  ( benchmark.equals("YCSB") ) {
				unit_of_measure = "OPS";
			} else {
				System.out.println(syntax);
				throw new Exception("ERROR : Invalid benchamrk provided. ( benchmark = " + benchmark + ". Expected DebitCredit | OrderEntry | YCSB ).");
			}

			// Command Interpreter
			if ( args.length < 4 ) {
				System.out.println(syntax);
				throw new Exception("ERROR : Required syntax not provided.");
			}

			String option = null;
			for ( int indx = 1; indx < args.length; indx++ ) {
				option = args[indx];
				switch ( option ) {
					case "drivers" : total_number_of_drivers = Integer.parseInt(args[++indx]); break;
					case "testid" : testid = args[++indx]; break;
					case "intervallength": length_of_interval = Integer.parseInt(args[++indx]); break;
					default: {
						System.out.println(syntax);
						throw new Exception("ERROR : Invalid option specified ( option = " + option + " )");
					}
				}
			}

			if ( testid == null ) {
				throw new Exception("ERROR : Required testid not provided.");
			}

			if ( total_number_of_drivers == 0 ) {
				throw new Exception("ERROR : Required number of drivers not provided.");
			}

			// turn off all logging ( Phoenix is very noisy. )
//			Logger.getRootLogger().setLevel(Level.OFF);

			display_message("");
			display_message("   DriverCoodinator");
			display_message("");
			display_message("   " + String.format("%20s", "Test Starting" ) + " : " + dateformat.format(new Date()));

			display_message("   " + String.format("%20s", "TestID" ) + " : " + testid);
			display_message("   " + String.format("%20s", "TotalDrivers" ) + " : " + total_number_of_drivers);
			display_message("   " + String.format("%20s", "IntervalLength" ) + " : " + length_of_interval);

			DatabaseConnector driver_database_connector = new DatabaseConnector();
			driver_database_connector.properties_file = "dbconnect.properties";
			driver_database_connector.stream_number = 0;
			driver_database_connector.database = "trafodion";
			driver_database_connector.option_autocommit = true;
			driver_database_connector.establishDatabaseConnection();

			display_message("");
			display_message("   Waiting for Drivers to get ready : " + dateformat.format(new Date()) );

			// Wait for all drivers to get running
			int startup_interval = 0;
			long begin_time = System.currentTimeMillis();
			int drivers_running = 0;

			String sql_statement = null;
			ResultSet resultset = null;
			PreparedStatement select_readycount_stmt = null;

			while ( drivers_running < total_number_of_drivers ) {
				Thread.sleep(1000);

				if ( select_readycount_stmt == null ) {
					sql_statement = "select count(*) as DRIVERS from JAVABENCH.CONTROL_TABLE"
							+ " where TEST_ID = '" + testid + "'";
					select_readycount_stmt = driver_database_connector.prepare_statement (sql_statement);
				}
				resultset = driver_database_connector.execute_prepared_query(select_readycount_stmt);
				resultset.next();  // Expecting exactly 1 row
				drivers_running = resultset.getInt("DRIVERS");
				resultset.close();

				long duration_seconds = ( System.currentTimeMillis() - begin_time ) / 1000;

				if ( duration_seconds > ( ( startup_interval + 1 ) * length_of_interval ) ) {
					startup_interval++;
					display_message("      "
							+ "Interval " + String.format("%5d", startup_interval )
							+ " , Time " + hourformat.format(new Date())
							+ " , Drivers " + String.format("%5d", total_number_of_drivers )
							+ " , Ready " + String.format("%5d", drivers_running ) );
				}
			}
			select_readycount_stmt.close();

			display_message("      " +  drivers_running + " drivers ready.");


			display_message("");
			display_message("   Telling all drivers to proceed : " + dateformat.format(new Date()) );

			sql_statement = "update JAVABENCH.CONTROL_TABLE"
					+ " set DRIVER_STATE = 'RUNNING'"
					+ " where TEST_ID = '" + testid + "'";
			driver_database_connector.execute_statement(sql_statement);


			display_message("");
			display_message("   Drivers running : " + dateformat.format(new Date()) );

			// Let the streams run till they are done
			startup_interval = 0;
			begin_time = System.currentTimeMillis();

//			long interval_time = System.currentTimeMillis();;
//			double duration_seconds = 0;

			// Report Header
			display_message("");
//			display_message( String.format("%48s", " " )
//					+ "<--------Interval Data-------->" + "   "
//                  + "<-------------------Accumulated Data------------------->"
			display_message( String.format("%42s", " " )
					+ "<-----------------Interval Data------------------->"
					+ "   "
					+ "<-----------------Accumulated Data----------------->"
					);
			String response_time_units;
			if ( unit_of_measure.equals("TPS") || unit_of_measure.equals("OPS") || unit_of_measure.equals("TPM") ) {
				response_time_units = "ms";
			} else {
				response_time_units = "s";
			}
			display_message("XXX , "
					+ String.format("%5s", "Intvl" )
					+ ", " + String.format("%8s", "Time" )
					+ ", " + String.format("%5s", "Secs" )
					+ ", " + String.format("%5s", "Strms" )
					+ ", " + String.format("%4s", "Errs" )
					+ ", " + String.format("%7s", "Trans" )
					+ ", " + String.format("%9s", unit_of_measure )
					+ ", " + String.format("%9s", "AvgRsp_" + response_time_units )
					+ ", " + String.format("%9s", "MinRsp_" + response_time_units )
					+ ", " + String.format("%9s", "MaxRsp_" + response_time_units )
					+ ", " + String.format("%8s", "Trans" )
					+ ", " + String.format("%9s", unit_of_measure )
					+ ", " + String.format("%9s", "AvgRsp_" + response_time_units )
					+ ", " + String.format("%9s", "MinRsp_" + response_time_units )
					+ ", " + String.format("%9s", "MaxRsp_" + response_time_units )
	//				+ ", " + String.format("%7s", "TtlTrans" )
	//				+ ", " + String.format("%6s", "TtlResp(us)" )
					);

			long prior_interval_transactions = 0;
			long interval_transactions = 0;
			double prior_seconds = 0;
			double interval_seconds = 0;
			long prior_interval_transaction_response_micro = 0;
			long interval_transaction_response_micro = 0;
			begin_time = System.currentTimeMillis();;

			PreparedStatement select_runningcount_stmt = null;
			PreparedStatement select_stats_stmt = null;

			while ( drivers_running > 0 ) {

				Thread.sleep(5000);

				if ( select_runningcount_stmt == null ) {
					sql_statement = "select count(*) as DRIVERS from JAVABENCH.CONTROL_TABLE"
							+ " where TEST_ID = '" + testid + "' and DRIVER_STATE = 'RUNNING'";
					select_runningcount_stmt = driver_database_connector.prepare_statement (sql_statement);
				}
				resultset = driver_database_connector.execute_prepared_query(select_runningcount_stmt);
				resultset.next();  // Expecting exactly 1 row
				drivers_running = resultset.getInt("DRIVERS");
				resultset.close();

				if ( ( ( System.currentTimeMillis() - begin_time ) / ( 1000 * length_of_interval ) ) > test_interval ) {
					test_interval++;
					if ( test_interval == 0 ) {
						begin_time = System.currentTimeMillis();;
					}
					Thread.sleep(5000);  // allow some time for the threads to report status
//					if ( test_interval > 0 ) {
//						duration_seconds = ( System.currentTimeMillis() - begin_time ) / 1000.0;
//					}

					// gather run time statistics
					if ( select_stats_stmt == null ) {
						sql_statement = "select"
								+ " max(DURATION_SECONDS) as DURATION_SECONDS"
								+ ", sum(ACTIVE_STREAMS) as ACTIVE_STREAMS"
								+ ", sum(ACCUMLATED_TRANSACTIONS) as ACCUMLATED_TRANSACTIONS"
//								+ ", sum(ACCUMLATED_SUCCESSES) as ACCUMLATED_SUCCESSES"
								+ ", sum(ACCUMLATED_ERRORS) as ACCUMLATED_ERRORS"
//								+ ", sum(ACCUMLATED_RETRIES) as ACCUMLATED_RETRIES"
								+ ", sum(ACCUMLATED_TRANSACTION_RESPONSE_MICRO) as ACCUMLATED_TRANSACTION_RESPONSE_MICRO"
								+ ", max(INTVL_MAXIMUM_TRANSACTION_RESPONSE_MICRO) as INTVL_MAXIMUM_TRANSACTION_RESPONSE_MICRO"
								+ ", min(INTVL_MINIMUM_TRANSACTION_RESPONSE_MICRO) as INTVL_MINIMUM_TRANSACTION_RESPONSE_MICRO"
//								+ ", sum(ACCUMLATED_OPERATIONS) as ACCUMLATED_OPERATIONS"
//								+ ", sum(ACCUMLATED_OPERATION_RESPONSE_MICRO) as ACCUMLATED_OPERATION_RESPONSE_MICRO"
								+ ", max(MAXIMUM_TRANSACTION_RESPONSE_MICRO) as MAXIMUM_TRANSACTION_RESPONSE_MICRO"
								+ ", min(MINIMUM_TRANSACTION_RESPONSE_MICRO) as MINIMUM_TRANSACTION_RESPONSE_MICRO"
								+ " from JAVABENCH.CONTROL_TABLE where TEST_ID = '" + testid + "' group by TEST_ID";
						select_stats_stmt = driver_database_connector.prepare_statement (sql_statement);
					}
					resultset = driver_database_connector.execute_prepared_query(select_stats_stmt);
					resultset.next();  // Expecting exactly 1 row
					Double duration_seconds = resultset.getDouble("DURATION_SECONDS");
					interval_seconds = duration_seconds - prior_seconds;
					prior_seconds = duration_seconds;
					int active_streams = resultset.getInt("ACTIVE_STREAMS");
					long accumlated_transactions = resultset.getLong("ACCUMLATED_TRANSACTIONS");
//					long accumlated_successes =  resultset.getLong("ACCUMLATED_SUCCESSES");
					long accumlated_errors =  resultset.getLong("ACCUMLATED_ERRORS");
//					long accumlated_retries =  resultset.getLong("ACCUMLATED_RETRIES");

					long accumlated_transaction_response_micro =  resultset.getLong("ACCUMLATED_TRANSACTION_RESPONSE_MICRO");
					long intvl_maximum_transaction_response_micro =  resultset.getLong("INTVL_MAXIMUM_TRANSACTION_RESPONSE_MICRO");
					long intvl_minimum_transaction_response_micro =  resultset.getLong("INTVL_MINIMUM_TRANSACTION_RESPONSE_MICRO");

//					long accumlated_operations =  resultset.getLong("ACCUMLATED_OPERATIONS");
//					long accumlated_operation_response_micro =  resultset.getLong("ACCUMLATED_OPERATION_RESPONSE_MICRO");
					long maximum_transaction_response_micro =  resultset.getLong("MAXIMUM_TRANSACTION_RESPONSE_MICRO");
					long minimum_transaction_response_micro =  resultset.getLong("MINIMUM_TRANSACTION_RESPONSE_MICRO");
					resultset.close();

					interval_transactions = accumlated_transactions - prior_interval_transactions;
					prior_interval_transactions = accumlated_transactions;
					interval_transaction_response_micro = accumlated_transaction_response_micro - prior_interval_transaction_response_micro;
					prior_interval_transaction_response_micro = accumlated_transaction_response_micro;

//					interval_time = System.currentTimeMillis();

					double throughput = 0.0;
					double interval_throughput = 0.0;
					double average_response = 0.0;
					double interval_average_response = 0.0;
					double intvl_minimum_response = 0.0;
					double intvl_maximum_response = 0.0;
					double minimum_response = 0.0;
					double maximum_response = 0.0;

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
							intvl_minimum_response = ( intvl_minimum_transaction_response_micro / 1000.0 );
							intvl_maximum_response = ( intvl_maximum_transaction_response_micro / 1000.0 );
							minimum_response = ( minimum_transaction_response_micro / 1000.0 );
							maximum_response = ( maximum_transaction_response_micro / 1000.0 );
						}
						if ( interval_transactions > 0 ) {
							interval_average_response = ( interval_transaction_response_micro / interval_transactions / 1000.0 );
						}

					} else if ( unit_of_measure.equals("QPH") || unit_of_measure.equals("RPH") ) {
						if ( accumlated_transactions > 0 ) {
							average_response = ( accumlated_transaction_response_micro / accumlated_transactions / 1000000.0 );
							intvl_minimum_response = ( intvl_minimum_transaction_response_micro / 1000000.0 );
							intvl_maximum_response = ( intvl_maximum_transaction_response_micro / 1000000.0 );
							minimum_response = ( minimum_transaction_response_micro / 1000000.0 );
							maximum_response = ( maximum_transaction_response_micro / 1000000.0 );
						}
						if ( interval_transactions > 0 ) {
							interval_average_response = ( interval_transaction_response_micro / interval_transactions / 1000000.0 );
						}

					}
					display_message("XXX" + " , "
							+ String.format("%5d", test_interval )
							+ ", " + hourformat.format(new Date())
							+ ", " + String.format("%5.0f", duration_seconds )
							+ ", " + String.format("%5d", active_streams )
							+ ", " + String.format("%4d", accumlated_errors )
							+ ", " + String.format("%7d", interval_transactions )
							+ ", " + String.format("%9.2f", interval_throughput )
							+ ", " + String.format("%9.3f", interval_average_response )
							+ ", " + String.format("%9.3f", intvl_minimum_response )
							+ ", " + String.format("%9.2f", intvl_maximum_response )
							+ ", " + String.format("%8d", accumlated_transactions )
							+ ", " + String.format("%9.2f", throughput )
							+ ", " + String.format("%9.3f", average_response )
							+ ", " + String.format("%9.3f", minimum_response )
							+ ", " + String.format("%9.2f", maximum_response )
//							+ ", " + String.format("%8d", accumlated_transactions )
//							+ ", " + accumlated_transaction_response_micro
							);
 				}
			}

			select_runningcount_stmt.close();
			select_stats_stmt.close();

			display_message("   Drivers stopped : " + dateformat.format(new Date()) );

			display_message("");
			display_message("   Gather up final results : " + dateformat.format(new Date()) );

//			duration_seconds = ( System.currentTimeMillis() - begin_time ) / 1000.0;

			// gather run time statistics
			sql_statement = "select"
					+ " max(DURATION_SECONDS) as DURATION_SECONDS"
					+ ", sum(TOTAL_STREAMS) as TOTAL_STREAMS"
					+ ", sum(ACCUMLATED_TRANSACTIONS) as ACCUMLATED_TRANSACTIONS"
					+ ", sum(ACCUMLATED_SUCCESSES) as ACCUMLATED_SUCCESSES"
					+ ", sum(ACCUMLATED_ERRORS) as ACCUMLATED_ERRORS"
					+ ", sum(ACCUMLATED_RETRIES) as ACCUMLATED_RETRIES"
					+ ", sum(ACCUMLATED_TRANSACTION_RESPONSE_MICRO) as ACCUMLATED_TRANSACTION_RESPONSE_MICRO"
//					+ ", max(INTVL_MAXIMUM_TRANSACTION_RESPONSE_MICRO) as INTVL_MAXIMUM_TRANSACTION_RESPONSE_MICRO"
//					+ ", min(INTVL_MINIMUM_TRANSACTION_RESPONSE_MICRO) as INTVL_MINIMUM_TRANSACTION_RESPONSE_MICRO"
					+ ", sum(ACCUMLATED_OPERATIONS) as ACCUMLATED_OPERATIONS"
//					+ ", sum(ACCUMLATED_OPERATION_RESPONSE_MICRO) as ACCUMLATED_OPERATION_RESPONSE_MICRO"
					+ ", max(MAXIMUM_TRANSACTION_RESPONSE_MICRO) as MAXIMUM_TRANSACTION_RESPONSE_MICRO"
					+ ", min(MINIMUM_TRANSACTION_RESPONSE_MICRO) as MINIMUM_TRANSACTION_RESPONSE_MICRO"
					+ " from JAVABENCH.CONTROL_TABLE where TEST_ID = '" + testid + "' group by TEST_ID";
			resultset = driver_database_connector.execute_query(sql_statement);
			resultset.next();  // Expecting exactly 1 row
			Double duration_seconds = resultset.getDouble("DURATION_SECONDS");
			int total_streams = resultset.getInt("TOTAL_STREAMS");
			long accumlated_transactions = resultset.getLong("ACCUMLATED_TRANSACTIONS");
			long accumlated_successes =  resultset.getLong("ACCUMLATED_SUCCESSES");
			long accumlated_errors =  resultset.getLong("ACCUMLATED_ERRORS");
			long accumlated_retries =  resultset.getLong("ACCUMLATED_RETRIES");

			long accumlated_transaction_response_micro =  resultset.getLong("ACCUMLATED_TRANSACTION_RESPONSE_MICRO");

			long accumlated_operations =  resultset.getLong("ACCUMLATED_OPERATIONS");
//			long accumlated_operation_response_micro =  resultset.getLong("ACCUMLATED_OPERATION_RESPONSE_MICRO");
			long maximum_transaction_response_micro =  resultset.getLong("MAXIMUM_TRANSACTION_RESPONSE_MICRO");
			long minimum_transaction_response_micro =  resultset.getLong("MINIMUM_TRANSACTION_RESPONSE_MICRO");
			resultset.close();

			double throughput = 0.0;
			double average_response = 0.0;
			double minimum_response = 0.0;
			double maximum_response = 0.0;
			double total_response = 0.0;
			double ops = 0.0;
			double average_ops_per_trans = 0.0;

			if ( duration_seconds > 0 ) {
				if ( unit_of_measure.equals("TPS") || unit_of_measure.equals("OPS") ) {
					throughput =  accumlated_transactions * 100 / duration_seconds / 100.0 ;
				} else if ( unit_of_measure.equals("TPM") ) {
					throughput =  60 * accumlated_transactions * 100 / duration_seconds / 100.0 ;
				} else if ( unit_of_measure.equals("QPH") || unit_of_measure.equals("RPH") ) {
					throughput =  3600 * accumlated_transactions * 100 / duration_seconds / 100.0 ;
				}
				ops =  ( accumlated_operations * 100 / duration_seconds / 100.0 );
			}
			if ( accumlated_transactions > 0 ) {
				if ( unit_of_measure.equals("TPS") || unit_of_measure.equals("OPS") || unit_of_measure.equals("TPM") ) {
					average_response = ( accumlated_transaction_response_micro / accumlated_transactions / 1000.0 );
					minimum_response = ( minimum_transaction_response_micro / 1000.0 );
					maximum_response = ( maximum_transaction_response_micro / 1000.0 );

				} else if ( unit_of_measure.equals("QPH") || unit_of_measure.equals("RPH") ) {
					average_response = ( accumlated_transaction_response_micro / accumlated_transactions / 1000000.0 );
					minimum_response = ( minimum_transaction_response_micro / 1000000.0 );
					maximum_response = ( maximum_transaction_response_micro / 1000000.0 );
				}
				average_ops_per_trans = ( 100.0 * accumlated_operations / accumlated_transactions / 100.0 );
			}

			display_message("");
			display_message("   Test Results:");
			display_message("");
			display_message("   " + String.format("%20s", "Total Intervals" ) + " : " + test_interval);
			display_message("   " + String.format("%20s", "Duration (secs)" ) + " : " +  duration_seconds);
			display_message("   " + String.format("%20s", "Concurrency" ) + " : " +  total_streams);
			display_message("");
			display_message("   " + String.format("%20s", "Total Transactions" ) + " : " +  accumlated_transactions);
			display_message("   " + String.format("%20s", "Total Successes" ) + " : " +  accumlated_successes);
			display_message("   " + String.format("%20s", "Total Failures" ) + " : " +  accumlated_errors);
			display_message("   " + String.format("%20s", "Total Retries" ) + " : " +  accumlated_retries);
			display_message("");
			display_message("   " + String.format("%20s", "Throughput (" + unit_of_measure + ")" ) + " : " +  String.format("%.2f", throughput ) );
			display_message("   " + String.format("%20s", "Average Latency (" + response_time_units +")" ) + " : " +  String.format("%.3f", average_response ) );
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
						+ " , " + String.format("%10s", "TotalTrans" )
						+ " , " + String.format("%10s", "TotalErrs" )
						+ " , " + String.format("%10s", "TtlRetrys" )
						+ " , " + String.format("%10s", "TtlSuccess" )
						+ " , " + String.format("%10s", "AvgRsp(" + response_time_units + ")" )
						+ " , " + String.format("%10s", "MinRsp(" + response_time_units + ")" )
						+ " , " + String.format("%10s", "MaxRsp(" + response_time_units + ")" )
						+ " , " + String.format("%12s", "TotRsp(" + response_time_units + ")" )
						);

			// gather run time statistics
			sql_statement = "select TRANSACTION_TYPE"
							+ ", sum(ACCUMLATED_TRANSACTIONS) as ACCUMLATED_TRANSACTIONS"
							+ ", sum(ACCUMLATED_SUCCESSES) as ACCUMLATED_SUCCESSES"
							+ ", sum(ACCUMLATED_ERRORS) as ACCUMLATED_ERRORS"
							+ ", sum(ACCUMLATED_RETRIES) as ACCUMLATED_RETRIES"
							+ ", sum(ACCUMLATED_TRANSACTION_RESPONSE_MICRO) as ACCUMLATED_TRANSACTION_RESPONSE_MICRO"
							+ ", max(MAXIMUM_TRANSACTION_RESPONSE_MICRO) as MAXIMUM_TRANSACTION_RESPONSE_MICRO"
							+ ", min(MINIMUM_TRANSACTION_RESPONSE_MICRO) as MINIMUM_TRANSACTION_RESPONSE_MICRO"
							+ " from JAVABENCH.CONTROL_TYPE_SUMMARY_TABLE"
							+ " where TEST_ID = '" + testid + "' group by TEST_ID, TRANSACTION_TYPE";
			resultset = driver_database_connector.execute_query(sql_statement);
			while (resultset.next()) {

				String transaction_type = resultset.getString("TRANSACTION_TYPE");
				long accumlated_transactions_by_type = resultset.getLong("ACCUMLATED_TRANSACTIONS");
				long accumlated_successes_by_type =  resultset.getLong("ACCUMLATED_SUCCESSES");
				long accumlated_errors_by_type =  resultset.getLong("ACCUMLATED_ERRORS");
				long accumlated_retries_by_type =  resultset.getLong("ACCUMLATED_RETRIES");

				long accumlated_transaction_response_micro_by_type =  resultset.getLong("ACCUMLATED_TRANSACTION_RESPONSE_MICRO");
				long maximum_transaction_response_micro_by_type =  resultset.getLong("MAXIMUM_TRANSACTION_RESPONSE_MICRO");
				long minimum_transaction_response_micro_by_type =  resultset.getLong("MINIMUM_TRANSACTION_RESPONSE_MICRO");

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
				System.out.println(
						  "   " + String.format("%20s", transaction_type )
						+ " , " + String.format("%10d", accumlated_transactions_by_type )
						+ " , " + String.format("%10d", accumlated_errors_by_type )
						+ " , " + String.format("%10d", accumlated_retries_by_type )
						+ " , " + String.format("%10d", accumlated_successes_by_type )
						+ " , " + String.format("%10.3f", average_response )
						+ " , " + String.format("%10.3f", minimum_response )
						+ " , " + String.format("%10.3f", maximum_response )
						+ " , " + String.format("%12.3f", total_response )
						);

			}
			resultset.close();


		} catch (Exception exception) {
			display_message("");
			display_message(">---------------------------------------------------------- [" + dateformat.format(new Date()) + "]" );
			display_message("> Unexpected EXCEPTION : " + exception.getMessage() );
			logger.error("Stack Trace", exception);
			display_message(">----------------------------------------------------------" );
		}

		display_message("");
		display_message("   " + String.format("%20s", "Test Stopping" ) + " : " + dateformat.format(new Date()));
		display_message("   " + String.format("%20s", "Elapsed Seconds" ) + " : " + String.format("%7.3f", (( System.currentTimeMillis() - testStartTime ) / 1000.0) ));
		display_message("");

	}
}
