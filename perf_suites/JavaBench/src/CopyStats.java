import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CopyStats { 

// Single global value for all Streams (Threads)

	public static final int MAX_CONNECT_ATTEMPTS = 100;

	public static final int PARALLEL_LOAD_THRESHOLD_ROWS = 100000;
	public static final int DEFAULT_PARALLEL_LOAD_STREAMS = 10;
	public static final int DEFAULT_BATCHSIZE = 1000;

	public static final SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	public static final SimpleDateFormat hourformat = new SimpleDateFormat("HH:mm:ss");

	public static final Object threadlock = new Object();
	public static final Object stdoutlock = new Object();

	public static volatile String source_propFile = null;
	public static volatile String source_table_name = null;
	public static volatile String source_schema_name = null;
	public static volatile String source_database = null;
	public static volatile int source_columnCount;

	public static volatile String destination_propFile = null;
	public static volatile String destination_table_name = null;
	public static volatile String destination_schema_name = "import";
	public static volatile String destination_database = null;
	public static volatile int destination_columnCount;

	public static volatile String load_column = null;
	public static volatile int load_column_type = 0;

	public static volatile boolean parallel_load = false;
	public static volatile int parallel_streams = DEFAULT_PARALLEL_LOAD_STREAMS;
	public static volatile int source_1st_column_max_value = 0;
	public static volatile int source_1st_column_min_value = 0;

	public static volatile boolean option_upsert = false;
	public static volatile boolean option_usingload = false;
	public static volatile boolean option_salt = false;
	public static volatile int saltsize = 1;
	public static volatile boolean option_compression = false;
	public static volatile int batchsize = DEFAULT_BATCHSIZE;
	public static volatile boolean option_trace = false;
	public static volatile boolean option_debug = false;

	public static volatile int length_of_interval = 60;
	public static volatile int timer_minutes = 0;
	public static volatile boolean timer_pop = false;

	public static volatile StringBuilder insert_statement = null;

	// Separate Copies per Stream (Thread)
	public volatile long stream_accumlated_rows_added = 0;
	public volatile long stream_accumlated_response_time = 0;
	public volatile long stream_maximum_response_time = 0;
	public volatile long stream_minimum_response_time = 999999999999999999L;
	public static final int RESPONSE_BUCKETS = 101;
	public volatile int[] stream_response_distribution = new int[RESPONSE_BUCKETS];
	public volatile int stream_number;
	public volatile int start_1st_column_value;
	public volatile int stop_1st_column_value;

	public volatile boolean signal_ready = false;
	public volatile boolean signal_proceed = false;
	public volatile boolean signal_stop = false;



//----------------------------------------------------------------------------------------------------------

	static Connection establishDatabaseConnection(String propFile) throws Exception {
		if (option_debug) { System.out.println("> establishDatabaseConnection()" + " [" + dateformat.format(new Date()) + "]"); }

		FileInputStream fs = new FileInputStream(new File(propFile));
		Properties props = new Properties();
		props.load(fs);
		String jdbc_driver =  props.getProperty("jdbc.drivers");
		String url = props.getProperty("url");
		String database = props.getProperty("database");
		fs.close();

		Connection dbconnection = null;

		Class.forName(jdbc_driver);

		// Establish Database Connection
		int attempts = 0;
		while ( dbconnection == null ) {
			attempts++;
			if (option_debug) { System.out.println(">	Attempt :" + attempts + " [" + dateformat.format(new Date()) + "]"); }
			try {
				if (option_debug) { System.out.println("> Database = " + database + " [" + dateformat.format(new Date()) + "]"); }
				if (database.equals("hive")) {
					dbconnection = DriverManager.getConnection(url, props.getProperty("user"), "");
				} else if (database.equals("mysql")) {
					if (option_debug) { System.out.println("> DriverManager.getConnection(" + url + ")" + " [" + dateformat.format(new Date()) + "]"); }
					dbconnection = DriverManager.getConnection(url);
				} else {
					if (option_debug) { System.out.println("> DriverManager.getConnection(" + url + "," + props + ")" + " [" + dateformat.format(new Date()) + "]"); }
					dbconnection = DriverManager.getConnection(url, props);
				}
			} catch (SQLException exception) {
				// try again
				if ( attempts % 10 == 0 ) {
					System.out.println(" Connection Failed ... will retry. Attempts: " + attempts  + " [" + dateformat.format(new Date()) + "]");
				}
				SQLException nextException = exception;
				do {
					if (option_debug) {
						System.out.println("SQLEXCEPTION,  Error Code = " + exception.getErrorCode()  + " [" + dateformat.format(new Date()) + "]" );
						System.out.println("ERROR : SQLException : " + nextException.getMessage() + " [" + dateformat.format(new Date()) + "]");
					}
				} while ((nextException = nextException.getNextException()) != null);
				if ( attempts > MAX_CONNECT_ATTEMPTS ) {
					throw new Exception("ERROR : Unable to connect to database (Maximum " + MAX_CONNECT_ATTEMPTS + " attempts failed).");
				}
				Thread.sleep( 1000 * attempts );  // Sleep
			}
		}
		if (option_debug) { System.out.println("Database Connection Established." + " [" + dateformat.format(new Date()) + "]"); }

		if ( database.equals("trafodion") || database.equals("seaquest") ) {
			// if CQD file specified, apply CQDs to open session
			String control_cqd_file_name = System.getenv("TEST_CQD_FILE");
			if (( control_cqd_file_name != null ) && ( !control_cqd_file_name.equals(""))) {
				if (option_debug) { System.out.println("CQD File Specified.  Need to apply CQDs to the open session." + " [" + dateformat.format(new Date()) + "]"); }
				if (option_debug) { System.out.println("Establish Statement." + " [" + dateformat.format(new Date()) + "]"); }
				Statement dbstatement = dbconnection.createStatement();
				if (option_debug) { System.out.println("Statement established." + " [" + dateformat.format(new Date()) + "]"); }
				String sql_statement = null;
				System.out.println("		Control CQD File:	" + control_cqd_file_name + " [" + dateformat.format(new Date()) + "]");
				File control_cqd_file = null;
				control_cqd_file = new File(control_cqd_file_name);
				if (option_debug) { System.out.println("Read from File" + " [" + dateformat.format(new Date()) + "]"); }
				BufferedReader reader = new BufferedReader( new FileReader ( control_cqd_file ) );
				while (( sql_statement = reader.readLine()) != null ) {
					if ( !sql_statement.equals("") ) {
						if (option_trace) { System.out.println(" " + sql_statement + " [" + dateformat.format(new Date()) + "]"); }
						dbstatement.execute(sql_statement);
					}
				}
				reader.close();
				dbstatement.close();
				dbstatement = null;
			}
		}

		if (option_debug) { System.out.println("< establishDatabaseConnection()" + " [" + dateformat.format(new Date()) + "]"); }

		return dbconnection;
	}

//----------------------------------------------------------------------------------------------------------

	static void copyTable() throws Exception {

		if (option_debug) { System.out.println("> copyTable()"); }

		long taskStartTime = System.currentTimeMillis();
		System.out.println("\n   Copy Table : " + dateformat.format(new Date()));

		// load data using parallel streams
		int threads_started=0;
		int number_of_streams=0;

		if ( parallel_load ) {
			number_of_streams = parallel_streams;
		} else {
			number_of_streams = 1;
		}

		System.out.println("\n   Starting up streams : " + dateformat.format(new Date()) );
		CopyData[] stream = new CopyData[number_of_streams];

		for ( int strm_idx = 0; strm_idx < number_of_streams; strm_idx++ ) {
			stream[strm_idx] = new CopyData();
		}

		int start_value = 0;
		int spread_per_stream_value = 0;
		if ( parallel_load ) {
			start_value = source_1st_column_min_value;
			spread_per_stream_value = (source_1st_column_max_value - source_1st_column_min_value + 1) / number_of_streams;
		}
		for ( int strm_idx = 0; strm_idx < number_of_streams; strm_idx++ ) {
			stream[strm_idx].stream_number = strm_idx;
			if ( parallel_load ) {
				stream[strm_idx].start_1st_column_value=start_value;
				if (strm_idx < number_of_streams - 1 ) {
					stream[strm_idx].stop_1st_column_value=start_value+spread_per_stream_value;
				} else {
					stream[strm_idx].stop_1st_column_value=source_1st_column_max_value + 1;
				}
				start_value = start_value + spread_per_stream_value;
			}
			stream[strm_idx].start();
			threads_started++;
			if (option_trace) { System.out.println("  Thread " + strm_idx + "; Start_Value " + stream[strm_idx].start_1st_column_value + "; Stop_Value " + stream[strm_idx].stop_1st_column_value ); }
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

		if (option_debug) { System.out.println("< copyTable()"); }
	}

//----------------------------------------------------------------------------------------------------------

	public void run() {

		//  Parallel Stream Loader
		if (option_debug || option_trace) { System.out.println(stream_number + "> run( " + start_1st_column_value + " , " + stop_1st_column_value + " )"); }

		// jdbc
		Connection source_dbconnection = null;
		Connection destination_dbconnection = null;
		PreparedStatement prepared_insert_statement = null;
		
		// hadoop
		Configuration conf = null;
		FileSystem fs = null;
		FSDataOutputStream data_file_stream = null;
		
		try {
			
			source_dbconnection = establishDatabaseConnection(source_propFile);

			if ( destination_database.equals("hadoop")) {
				conf = new Configuration();
				fs = FileSystem.get(conf);

				String data_file_name = "/javabench/" + destination_schema_name + "/" + destination_table_name + "/" + destination_table_name + "." + String.format("%02d",stream_number) + ".dat";

				if (option_trace) { System.out.println(stream_number + ">  Writing data to file: " + data_file_name); }

				Path data_file_path = new Path(data_file_name);

				try {
					if (fs.exists (data_file_path)) {
						fs.delete(data_file_path,true);
					}
				} catch (Exception exception) {
					//ignore
				}

				data_file_stream = fs.create(data_file_path);

			} else { // jdbc

				destination_dbconnection = establishDatabaseConnection(destination_propFile);

				prepared_insert_statement = destination_dbconnection.prepareStatement(insert_statement.toString());

			}

			long count = 0;
			long prior_count = 0;

			// Tell the main thread that I'm ready to proceed
			signal_ready = true;
			while (!signal_proceed){
				Thread.sleep(10);
			}

			if ( ( stream_number == 0 ) || ( parallel_load ) ) {

				String source_select_statement = null;
				if ( parallel_load ) {
					source_select_statement = "select * from " + source_table_name
							+ " where " + load_column + " >= " + start_1st_column_value
							+ " and " + load_column + " < " + stop_1st_column_value;
				} else {
					source_select_statement = "select * from " + source_table_name;
				}
				
				if ( option_trace ) {
					System.out.println(stream_number + "> Source select statement: " + source_select_statement );
				}

				Statement source_dbstatement = source_dbconnection.createStatement();
				source_dbstatement.setFetchSize(batchsize);
				ResultSet source_resultset = source_dbstatement.executeQuery(source_select_statement);

				int source_rows = 0;

				while ( source_resultset.next() ) {
					source_rows++;
					
					if ( destination_database.equals("hadoop")) {

						StringBuilder destination_record = new StringBuilder(source_resultset.getObject(1).toString());
//						if (option_debug) {
//							System.out.println(stream_number + "> Field : 1 , Value : " + source_resultset.getObject(1).toString() );
//						}
						for (int i = 2; i<= source_columnCount; i++) {
							if ( source_resultset.getObject(i) != null ) {
								destination_record.append( "|" + source_resultset.getObject(i).toString());
//								if (option_debug) {
//									System.out.println(stream_number + "> Field : " + i + " , Value : " + source_resultset.getObject(i).toString() );
//								}
							} else {
								destination_record.append( "|");
							}
						}

						long statement_start_time = System.nanoTime();
						data_file_stream.writeBytes(destination_record.toString());
						long statement_elapsed_micro = ( System.nanoTime() - statement_start_time ) / 1000;

						stream_accumlated_rows_added++;
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

					} else {
						
						for (int i = 1; i<= source_columnCount; i++) {
							prepared_insert_statement.setObject(i,source_resultset.getObject(i));
						}
						prepared_insert_statement.addBatch();
						if ( ++count % batchsize == 0 ) {
							long statement_start_time = System.nanoTime();
							prepared_insert_statement.executeBatch();	// Execute Batch
							long statement_elapsed_micro = ( System.nanoTime() - statement_start_time ) / 1000;
							prepared_insert_statement.clearBatch();

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

				if ( ! destination_database.equals("hadoop") && ( count > prior_count ) ) {
					long statement_start_time = System.nanoTime();
					prepared_insert_statement.executeBatch();	// Execute Batch
					long statement_elapsed_micro = ( System.nanoTime() - statement_start_time ) / 1000;
					prepared_insert_statement.clearBatch();

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

				source_resultset.close();
				if ( option_trace ) {
					System.out.println(stream_number + ">  Records Inserted = " + source_rows + " [" + dateformat.format(new Date()) + "]" );
				}

			}
			
			if ( destination_database.equals("hadoop") ) {
				data_file_stream.flush();
				data_file_stream.close();
			} else {
				destination_dbconnection.close();
				destination_dbconnection = null;
			}
			source_dbconnection.close();
			source_dbconnection = null;

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
				exception.printStackTrace(System.out);
			}
		} catch (Exception exception) {
			synchronized (stdoutlock) {
				System.out.println("\n" + stream_number + "> ----------------------------------------------------------");
				System.out.println(stream_number + "> EXCEPTION: " + exception.getMessage());
				System.out.println(stream_number + ">   Stream Number : " + stream_number );
				System.out.println(stream_number + "----------------------------------------------------------\n");
				exception.printStackTrace(System.out);
			}
		}

		if (option_debug) { System.out.println(stream_number + "< run()"); }

	}

//----------------------------------------------------------------------------------------------------------

	public static void main(String[] args) {

		long testStartTime = System.currentTimeMillis();

		String command_line = "";
		for (int idx = 0; idx < args.length; idx++) {
			command_line = command_line + args[idx] + " ";
		}
		System.out.println("CopyData " + command_line);

		try {

			// Command Interrupter

			String syntax = "\n"
					+ "CopyData: CopyData source_table <tablename> destination_table <tablename> [<copy_option>]\n"
					+ "   <copy_option> :  \n"
					+ "     source_schema <<schemaname>> :"
					+ "     destination_schema <<schemaname>> :"
					+ "     trace : Enable Statement Tracing.\n"
					+ "     debug : Enable Debug Tracing.\n"
					+ "     spj : transaction executed as java stored procedure. (Currently not implemented.)\n"
					+ "     upsert : Use upsert syntax instead of insert syntax.\n"
					+ "     usingload : Use using load syntax in insert/upsert.\n"
				;

			// Command Interpreter
			if ( args.length < 4 ) {
				System.out.println(syntax);
				throw new Exception("ERROR : Required arguments not provided.");
			}

			String option = null;
			for ( int indx = 0; indx < args.length; indx++ ) {
				option = args[indx];
				switch ( option ) {
					case "source_table": source_table_name = args[++indx]; break;
					case "source_schema": source_schema_name = args[++indx]; break;
					case "destination_table": destination_table_name = args[++indx]; break;
					case "destination_schema": destination_schema_name = args[++indx]; break;
					case "column": load_column = args[++indx]; break;
					case "batchsize": batchsize = Integer.parseInt(args[++indx]); break;
					case "upsert": option_upsert = true; break;
					case "usingload": option_usingload = true; break;
					case "streams": parallel_streams = Integer.parseInt(args[++indx]); break;
					case "trace": option_trace = true; break;
					case "debug": option_debug = true; break;
					default: {
						System.out.println(syntax);
						throw new Exception("ERROR : Invalid option specified ( option = " + option + " )");
					}
				}
			}

			if ( source_table_name == null ) {
				System.out.println(syntax);
				throw new Exception("ERROR : Required source table name not provided.");
			}

			if ( destination_table_name == null ) {
				System.out.println(syntax);
				throw new Exception("ERROR : Required destination table name not provided.");
			}

			// Use dbconnect.source.properties to get source database connection info
			source_propFile = System.getProperty("dbconnect.source.properties");
			if ( source_propFile == null ) {
				throw new Exception("Error : dbconnect.source.properties is not set. Exiting.");
			}

			FileInputStream source_fs = new FileInputStream(new File(source_propFile));
			Properties source_props = new Properties();
			source_props.load(source_fs);
			source_database = source_props.getProperty("database");
			source_fs.close();

			// Use dbconnect.destination.properties to get destination database connection info
			destination_propFile = System.getProperty("dbconnect.destination.properties");
			if ( destination_propFile == null ) {
				throw new Exception("Error : dbconnect.destination.properties is not set. Exiting.");
			}

			FileInputStream destination_fs = new FileInputStream(new File(destination_propFile));
			Properties distination_props = new Properties();
			distination_props.load(destination_fs);
			destination_database = distination_props.getProperty("database");
			destination_fs.close();

			if ( destination_database.equals("trafodion") ) {
				option_upsert = true;
				option_usingload = true;
			}

			System.out.println("\nCopyData");
			System.out.println("\n   " + String.format("%24s", "CopyData Starting" ) + " : " + dateformat.format(new Date()));

			System.out.println("   " + String.format("%24s", "SourcePropertyFile" ) + " : " + source_propFile);
			System.out.println("   " + String.format("%24s", "DestinationPropertyFile" ) + " : " + destination_propFile);
			System.out.println("   " + String.format("%24s", "SourceTableName" ) + " : " + source_table_name);
			System.out.println("   " + String.format("%24s", "DestinationTableName" ) + " : " + destination_table_name);
			if ( load_column != null ) {System.out.println("   " + String.format("%24s", "OptionColumnName" ) + " : " + load_column);}
			System.out.println("   " + String.format("%24s", "ParallelStreams" ) + " : " + parallel_streams);
			System.out.println("   " + String.format("%24s", "BatchSize" ) + " : " + batchsize);
			if (option_upsert) { System.out.println("   " + String.format("%24s", "Upsert" ) + " : " + option_upsert); }
			if (option_usingload) { System.out.println("   " + String.format("%24s", "UsingLoad" ) + " : " + option_usingload); }
			if (option_trace) { System.out.println("   " + String.format("%24s", "Trace" ) + " : " + option_trace); }
			if (option_debug) { System.out.println("   " + String.format("%24s", "Debug" ) + " : " + option_debug); }
			System.out.println("");

			if ( option_debug ) {
				System.out.println(" Source Table = " + source_table_name + " [" + dateformat.format(new Date()) + "]");
				System.out.println(" Destination Table = " + destination_table_name + " [" + dateformat.format(new Date()) + "]");
			}

			Connection source_dbconnection = establishDatabaseConnection(source_propFile);
			PreparedStatement prepared_source_dbstatement = source_dbconnection.prepareStatement("select * from " + source_table_name);
			ResultSetMetaData source_resultsetmetadata = prepared_source_dbstatement.getMetaData();

			source_columnCount=source_resultsetmetadata.getColumnCount();

			if ( load_column == null ) {
				load_column_type = source_resultsetmetadata.getColumnType(1);
				load_column = source_resultsetmetadata.getColumnName(1);
			} else {
				for ( int i = 1; i<= source_columnCount; i++ ) {
					if ( ( load_column != null ) && ( source_resultsetmetadata.getColumnName(i).equals(load_column) ) ) {
						load_column_type = source_resultsetmetadata.getColumnType(i);
					}
				}
			}


			Connection destination_dbconnection = null;
			PreparedStatement prepared_destination_dbstatement = null;
			ResultSetMetaData destination_resultsetmetadata = null;
			if (option_debug) {	System.out.println(" Column Count = " + source_columnCount + " [" + dateformat.format(new Date()) + "]"); }
			
			if ( ! destination_database.equals("hadoop") ) {

				//  First Check out the source table and destination table - Do they match?

				destination_dbconnection = establishDatabaseConnection(destination_propFile);
				prepared_destination_dbstatement = destination_dbconnection.prepareStatement("select * from " + destination_table_name);
				destination_resultsetmetadata = prepared_destination_dbstatement.getMetaData();
			
				destination_columnCount=destination_resultsetmetadata.getColumnCount();
				if ( source_columnCount != destination_columnCount ) {
					throw new Exception("Error : Source and Destination do not have the same number of columns.  SourceColumns = "
							+ source_columnCount + ",  DestinationColumns = " + destination_columnCount);
				}

				for ( int i = 1; i<= destination_columnCount; i++ ) {
					if (
						( ! source_resultsetmetadata.getColumnName(i).equals(destination_resultsetmetadata.getColumnName(i)) )
						|| ( source_resultsetmetadata.getColumnType(i) != destination_resultsetmetadata.getColumnType(i) )
						|| ( ! source_resultsetmetadata.getColumnTypeName(i).equals(destination_resultsetmetadata.getColumnTypeName(i)) )
						) {
						System.out.println(" Source Column : " + i
								+ ", Column Name = " + source_resultsetmetadata.getColumnName(i)
								+ ", Column Type = " + source_resultsetmetadata.getColumnType(i)
								+ ", Column Type Name = " + source_resultsetmetadata.getColumnTypeName(i)
								 + " [" + dateformat.format(new Date()) + "]");
						System.out.println(" Destination Column : " + i
								+ ", Column Name = " + destination_resultsetmetadata.getColumnName(i)
								+ ", Column Type = " + destination_resultsetmetadata.getColumnType(i)
								+ ", Column Type Name = " + destination_resultsetmetadata.getColumnTypeName(i)
								 + " [" + dateformat.format(new Date()) + "]");
						throw new Exception("Error : Source and Destination Columns do not match.");
					}
				}

				if (destination_database.equals("phoenix") && ! option_upsert ) {
					option_upsert = true;  // override it set
				}

				// Create the insert statement for the loader.
				insert_statement = new StringBuilder ("");
				if (option_upsert) {
					insert_statement = insert_statement.append("upsert ");
				} else {
					insert_statement = insert_statement.append("insert ");
				}

				if (option_usingload) {
					insert_statement = insert_statement.append("using load ");
				}
				insert_statement = insert_statement.append("into " + destination_table_name + " ( ");
				for (int i = 1; i<= source_columnCount; i++) {
					insert_statement = insert_statement.append("\"" + source_resultsetmetadata.getColumnName(i) + "\"");
					if ( i < source_columnCount) {
						insert_statement = insert_statement.append(" , ");

					}
				}
				insert_statement = insert_statement.append(" ) values ( ");
				for (int i = 1; i<= source_columnCount; i++) {
					insert_statement = insert_statement.append(" ? ");
					if ( i < source_columnCount) {
						insert_statement = insert_statement.append(" , ");

					}
				}
				insert_statement = insert_statement.append(" )");
				if ( option_trace ) {
					System.out.println(" Destination Insert Statement " + " [" + dateformat.format(new Date()) + "]");
					System.out.println(insert_statement.toString());
				}

				prepared_destination_dbstatement.close();
				destination_dbconnection.close();

			}


			//  How big is the copy?
			Statement source_dbstatement = source_dbconnection.createStatement();
			ResultSet source_resultset = source_dbstatement.executeQuery("select count(*) from " + source_table_name);
			source_resultset.next();  // Expecting exactly 1 row
			int source_rows = source_resultset.getInt(1);
			source_resultset.close();
			System.out.println("   Source Rows = " + source_rows  + " [" + dateformat.format(new Date()) + "]");

			// If number of rows greater than threshold and 1st column type integer, then get min/max value

			if ( (source_rows > PARALLEL_LOAD_THRESHOLD_ROWS ) && ( (load_column_type == 4) || (load_column_type == 2) ) ) {
				parallel_load = true;
				source_resultset = source_dbstatement.executeQuery("select max(" + load_column + "), min (" + load_column + ") from " + source_table_name);
				source_resultset.next();  // Expecting exactly 1 row
				source_1st_column_max_value = source_resultset.getInt(1);
				source_1st_column_min_value = source_resultset.getInt(2);
				source_resultset.close();
				System.out.println("\n   Parallel load invoked." + " [" + dateformat.format(new Date()) + "]");
				if ( option_trace ) {
					System.out.println("     First Column Min Value = " + source_1st_column_min_value +
							", First Column Max Value = " + source_1st_column_max_value + " [" + dateformat.format(new Date()) + "]");
				}
			}

			source_dbstatement.close();
			prepared_source_dbstatement.close();
			source_dbconnection.close();

			copyTable();

		} catch (SQLException exception) {
				System.out.println("\n" + "----------------------------------------------------------" + " [" + dateformat.format(new Date()) + "]");
				System.out.println("SQLEXCEPTION : Error Code = " + exception.getErrorCode() );
				SQLException nextException = exception;
				do {
					System.out.println(nextException.getMessage());
				} while ((nextException = nextException.getNextException()) != null);
				System.out.println("----------------------------------------------------------\n");
				exception.printStackTrace(System.out);
		} catch (Exception exception) {
				System.out.println("\n----------------------------------------------------------" + " [" + dateformat.format(new Date()) + "]"
					+ "\nEXCEPTION : " + exception.getMessage()
					+ "\n----------------------------------------------------------\n");
				exception.printStackTrace(System.out);
		}

		System.out.println("\n   " + String.format("%20s", "Test Stopping" ) + " : " + dateformat.format(new Date()));
		System.out.println("   " + String.format("%20s", "Elapsed Seconds" ) + " : " + String.format("%7.3f", (( System.currentTimeMillis() - testStartTime ) / 1000.0) ));

	}
}
