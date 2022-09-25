import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.log4j.Logger;

public class BenchWorkload2 {

	final int INVALID_STREAMNUMBER = -1;

	public static final SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	public static final SimpleDateFormat hourformat = new SimpleDateFormat("HH:mm:ss");

	public int stream_number = INVALID_STREAMNUMBER;

	public int query_number = 0;

	public String schema = null;
	public String database = null;

	public boolean option_autocommit = true;
	public boolean option_sessionless = true;
	public boolean option_prepareexec = false;
	public boolean option_directexec = true;
	public boolean option_spj = false;
	public boolean option_controlrandom = false;
	public boolean option_default = false;

	RandomGenerator random_generator = new RandomGenerator();

	DatabaseConnector database_connector = new DatabaseConnector();

	final static Logger logger = Logger.getLogger("JavaBench");

	Calendar calendar = null;

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
	public final String TRANS_TYPE_NAMES[] = { "benchquery" };

	public final int QUERY_TRANS_TYPE = 0;  // Fixed value

	public int[] execution_order = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 };

	public long[] accumlated_transactions_by_type = { 0 };
	public long[] accumlated_errors_by_type = { 0 };
	public long[] accumlated_retries_by_type = { 0 };
	public long[] accumlated_successes_by_type = { 0 };

	public long[] accumlated_transaction_response_micro_by_type = { 0 };
	public long[] maximum_transaction_response_micro_by_type = { 0 };
	public long[] minimum_transaction_response_micro_by_type = { 999999999999999999L };

	long start_transaction_time = 0;
	long transaction_elapsed_micro = 0;

    public static final Object queryfilelock = new Object();
    public static volatile String workload_file_name;
    public static volatile File workload_file = null;
    public static volatile BufferedReader workload_file_reader = null;

    public static volatile boolean option_repeat_test = false;
    public static volatile int option_repeat_count = 1;
    public static volatile int repeat_count = 1;

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
		transaction_elapsed_micro = ( System.nanoTime() - start_transaction_time ) / 1000;

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

	public void runQuery( String query_statement ) throws Exception {

		if (logger.isDebugEnabled()) logger.debug(stream_number + "> BenchWorkload2.runQuery()");

		int	trans_type = QUERY_TRANS_TYPE;
		int max_records = 99999999;

		query_number++;

		start_transaction(trans_type);

		if (logger.isDebugEnabled()) logger.debug(stream_number + "> accumlated_transactions " + String.format("%5d", accumlated_transactions ) );

		String transaction_data_string = "Bench Query : " + query_statement;

		if (logger.isDebugEnabled()) logger.debug(stream_number + "> " + transaction_data_string);

		try {

			start_timer();

			if ( option_sessionless ) {
				database_connector.establishDatabaseConnection();
			}

			int records_returned = 0;

			if (logger.isDebugEnabled()) logger.debug(stream_number + ">   switch token : " + query_statement.substring(0, 4) );
			switch ( query_statement.substring(0, 4) ) {
			case "obey":
				if (logger.isDebugEnabled()) logger.debug(stream_number + ">   obey statement " );
				String[] stream_stmt_tokens = query_statement.split("[ ;]+");
				if (logger.isDebugEnabled()) logger.debug(stream_number + ">  Obey File : " + stream_stmt_tokens[1]);
				BufferedReader obey_file_reader = new BufferedReader( new FileReader ( stream_stmt_tokens[1] ) );
				String obey_sql_statement = null;
				while ( (obey_sql_statement = obey_file_reader.readLine()) != null  ) {
					if (logger.isDebugEnabled()) logger.debug(stream_number + ">  Obey SQL Statement : " + obey_sql_statement);
					switch ( obey_sql_statement.substring(0, 6) ) {
					case "select":
						ResultSet resultset = database_connector.execute_query(obey_sql_statement);
						while (  ( records_returned < max_records ) && resultset.next() ) {
							records_returned++;
						}
						break;
					default: {
						database_connector.execute_statement(obey_sql_statement);
						}
					}
				}
				obey_file_reader.close();
				break;
			case "inse":
				if (logger.isDebugEnabled()) logger.debug(stream_number + ">   insert statement " );
				database_connector.execute_statement(query_statement);
				
				break;
			case "sele":
				if (logger.isDebugEnabled()) logger.debug(stream_number + ">   select statement " );
				ResultSet resultset = database_connector.execute_query(query_statement);
				while (  ( records_returned < max_records ) && resultset.next() ) {
					records_returned++;
				}
				break;
			default: {
				throw new Exception("ERROR : Invalid query ( " + query_statement.substring(0, 3) + " , " + query_statement + " )");
				}
			}

			if ( option_sessionless ) {
				database_connector.closeDatabaseConnection();
			}

			stop_timer(trans_type);

			logger.info(stream_number + "> Query Number : " + accumlated_transactions
					+ " ,  Query Type : " + TRANS_TYPE_NAMES[trans_type]
					+ " , Records Returned : " + records_returned
					+ " , Elasped Time (sec) : " + String.format("%10.6f", ( transaction_elapsed_micro / 1000000.0 ) )
					);

			transaction_success( trans_type );
			end_transaction();

		} catch (SQLException exception) {
			transaction_error( trans_type );
			String tombstone = "";
			tombstone = tombstone + "\n" + ">----------------------------------------------------------";
			tombstone = tombstone + "\n" + "> Unexpected SQLEXCEPTION,  Error Code = " + exception.getErrorCode();
			tombstone = tombstone + "\n" + ">   Stream Number : " + stream_number + " , Query Number : " + accumlated_transactions + " ,  Query Type : " + TRANS_TYPE_NAMES[trans_type];
			tombstone = tombstone + "\n" + ">   " + transaction_data_string;
			SQLException nextException = exception;
			do {
				tombstone = tombstone + "\n" + "> " + nextException.getMessage();
			} while ((nextException = nextException.getNextException()) != null);
			tombstone = tombstone + "\n" + ">----------------------------------------------------------";
			logger.error(tombstone, exception);
			end_transaction();
			throw exception;
		}

		if (logger.isDebugEnabled()) logger.debug(stream_number + "< BenchWorkload2.runQuery()");

	}

	//----------------------------------------------------------------------------------------------------------

	public void start() throws Exception {

		if (logger.isDebugEnabled()) logger.debug(stream_number + "> BenchWorkload2.start()");

		// Initialize Random Number Generator
		random_generator.stream_number = stream_number;
		random_generator.option_controlrandom = option_controlrandom;
		random_generator.start();

		calendar = Calendar.getInstance();

		// Initialize Database Connector
		database_connector.stream_number = stream_number;
		database_connector.database = database;
		database_connector.option_autocommit = option_autocommit;

		if ( ! option_sessionless ) { //  If not sessionless, connect to database
			if (logger.isDebugEnabled()) logger.debug(stream_number + "  !option_sessionless");
			database_connector.establishDatabaseConnection();
		}

		if (logger.isDebugEnabled()) logger.debug(stream_number + "< BenchWorkload2.start()");

	}


	//----------------------------------------------------------------------------------------------------------

	public void close() throws Exception {

		if (logger.isDebugEnabled()) logger.debug(stream_number + "> BenchWorkload2.close()");

		if ( ! option_sessionless ) { //  If not sessionless, close database connection
			database_connector.closeDatabaseConnection();
		}

		if (logger.isDebugEnabled()) logger.debug(stream_number + "< BenchWorkload2.close()");

	}

	//----------------------------------------------------------------------------------------------------------

}
