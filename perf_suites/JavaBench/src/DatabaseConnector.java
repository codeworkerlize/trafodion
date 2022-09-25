import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Properties;

import org.apache.log4j.Logger;

public class DatabaseConnector {
	
	final int INVALID_STREAMNUMBER = -1;
	public int stream_number = INVALID_STREAMNUMBER;

	final int MAX_CONNECT_ATTEMPTS = 100;
	final int REPORT_CONNECT_ATTEMPTS = 10;

	public boolean option_autocommit = false;
	public String properties_file = null;

	Connection dbconnection = null;
	Statement dbstatement = null;
	Statement batch_dbstatement = null;
	Statement scrollable_dbstatement = null;
	
	public String database = null;

	final Logger logger = Logger.getLogger("JavaBench");

	public long accumlated_operations = 0;
	public long accumlated_operation_response_micro = 0;
	public long maximum_operation_response_micro = 0;
	public long minimum_operation_response_micro = 999999999999999999L;

	long operation_start_time = 0;
	long operation_elapsed_micro = 0;
	
	void start_operation () {
		accumlated_operations++;
		operation_start_time = System.nanoTime();
	}
	
	void end_operation () {
		operation_start_time = System.nanoTime();
		operation_elapsed_micro = ( System.nanoTime() - operation_start_time ) / 1000;

		accumlated_operation_response_micro = accumlated_operation_response_micro + operation_elapsed_micro;
		if (operation_elapsed_micro < minimum_operation_response_micro ) {
			minimum_operation_response_micro = operation_elapsed_micro;
		}
		if (operation_elapsed_micro > maximum_operation_response_micro ) {
			maximum_operation_response_micro = operation_elapsed_micro;
		}
	}

	void establishDatabaseConnection() throws Exception {
		
		if (logger.isDebugEnabled()) logger.debug(stream_number + "> establishDatabaseConnection()"); 

		Properties props = new Properties();

		if ( properties_file == null ) {
			// Use dbconnect.properties to get database connection info
			properties_file = System.getProperty("dbconnect.properties");
			if ( properties_file == null ) {
				properties_file = "dbconnect.properties";
			}
		}

		FileInputStream fs = new FileInputStream(new File(properties_file));
		props.load(fs);
		String jdbc_driver =  props.getProperty("jdbc.drivers");
		String url = props.getProperty("url");
		fs.close();

		Class.forName(jdbc_driver);

		// Establish Database Connection
		int attempts = 0;
		while ( dbconnection == null ) {
			attempts++;
			if (logger.isDebugEnabled()) logger.debug(stream_number + ">	Attempt :" + attempts); 
			try {
				if (logger.isDebugEnabled()) logger.debug(stream_number + "> Database = " + database); 
				if (database.equals("hive")) {
					dbconnection = DriverManager.getConnection(url, props.getProperty("user"), "");
				} else if (database.equals("mysql")) {
					if (logger.isDebugEnabled()) logger.debug(stream_number + "> DriverManager.getConnection(" + url + ")"); 
					dbconnection = DriverManager.getConnection(url);
				} else {
					if (logger.isDebugEnabled()) logger.debug(stream_number + "> DriverManager.getConnection(" + url + "," + props + ")"); 
					dbconnection = DriverManager.getConnection(url, props);
				}
			} catch (SQLException exception) {
				// try again
				if ( attempts % REPORT_CONNECT_ATTEMPTS == 0 ) {
					logger.info(stream_number + "> Connection Failed ... will retry. Attempts: " + attempts );
				}
				SQLException nextException = exception;
				do {
					if (logger.isDebugEnabled()) logger.debug(stream_number + "> SQLEXCEPTION,  Error Code = " + exception.getErrorCode() );
					if (logger.isDebugEnabled()) logger.debug(stream_number + "> ERROR : SQLException : " + nextException.getMessage());
					if ( attempts > MAX_CONNECT_ATTEMPTS ) {
						logger.error(stream_number + "> SQLEXCEPTION,  Error Code = " + exception.getErrorCode() );
						logger.error(stream_number + "> ERROR : SQLException : " + nextException.getMessage());
					}
				} while ((nextException = nextException.getNextException()) != null);
				if ( attempts > MAX_CONNECT_ATTEMPTS ) {
					throw new Exception("ERROR : Unable to connect to database (Maximum " + MAX_CONNECT_ATTEMPTS + " attempts failed).");
				}
				Thread.sleep( 1000 * attempts );  // Sleep
			}
		}
		if (logger.isDebugEnabled()) logger.debug(stream_number + ">  Database Connection Established."); 

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
				if (logger.isDebugEnabled()) logger.debug(stream_number + "> CQD File Specified.  Need to apply CQDs to the open session."); 
				if (logger.isDebugEnabled()) logger.debug(stream_number + "> Establish Statement."); 
				Statement dbstatement = dbconnection.createStatement();
				if (logger.isDebugEnabled()) logger.debug(stream_number + "> Statement established."); 
				String sql_statement = null;
				File control_cqd_file = null;
				control_cqd_file = new File(control_cqd_file_name);
				if (logger.isDebugEnabled()) logger.debug(stream_number + "> Read from File"); 
				BufferedReader reader = new BufferedReader( new FileReader ( control_cqd_file ) );
				while (( sql_statement = reader.readLine()) != null ) {
					if ( !sql_statement.equals("") ) {
						if (logger.isDebugEnabled()) logger.debug(stream_number + ">   " + sql_statement);
						dbstatement.execute(sql_statement);
					}
				}
				reader.close();
				dbstatement.close();
				dbstatement = null;
			}
		}

		if (logger.isDebugEnabled()) logger.debug(stream_number + "< establishDatabaseConnection()"); 
	}

	void closeDatabaseConnection() throws SQLException {
		if (logger.isDebugEnabled()) logger.debug(stream_number + "> closeDatabaseConnection()"); 
		if ( dbstatement != null ) { 
			dbstatement.close(); 
			dbstatement = null;
		}
		if ( dbconnection != null ) {
			dbconnection.close();
			dbconnection = null;
		}
		if (logger.isDebugEnabled()) logger.debug(stream_number + ">  Database Connection Closed."); 
		if (logger.isDebugEnabled()) logger.debug(stream_number + "< closeDatabaseConnection().");
	}
	
	PreparedStatement prepare_statement ( String statement ) throws SQLException
	{
		if (logger.isDebugEnabled()) logger.debug(stream_number + "> prepare_statement");
		if (logger.isDebugEnabled()) logger.debug(stream_number + ">    " + statement);

		start_operation();
		PreparedStatement preparedstatement = dbconnection.prepareStatement(statement);
		end_operation();

		if( logger.isInfoEnabled() ) {
			SQLWarning warning = preparedstatement.getWarnings();
			while ( warning != null ) {
				logger.warn(warning.getMessage());
				warning = warning.getNextWarning();
			}
		}
		if (logger.isDebugEnabled()) logger.debug(stream_number + "< prepare_statement");
		return preparedstatement;
	}

	void execute_prepared_update_statement ( PreparedStatement statement ) throws SQLException {
		if (logger.isDebugEnabled()) logger.debug(stream_number + "> execute_prepared_update_statement");
		if (logger.isDebugEnabled()) logger.debug(stream_number + ">   " + statement.toString() );

		start_operation();
		statement.executeUpdate();
		end_operation();

		statement.clearParameters();
		if (logger.isDebugEnabled()) logger.debug(stream_number + "< execute_prepared_update_statement");
	}
	
	ResultSet execute_prepared_query ( PreparedStatement statement ) throws SQLException {
		if (logger.isDebugEnabled()) logger.debug(stream_number + "> execute_prepared_query");
		if (logger.isDebugEnabled()) logger.debug(stream_number + ">   " + statement.toString() );

		start_operation();
		ResultSet resultset = statement.executeQuery();
		end_operation();

		if( logger.isInfoEnabled() ) {
			SQLWarning warning = statement.getWarnings();
			while ( warning != null ) {
				logger.warn(warning.getMessage());
				warning = warning.getNextWarning();
			}
		}
		statement.clearParameters();
		if (logger.isDebugEnabled()) logger.debug(stream_number + "< execute_prepared_query");
		return resultset;
	}
	
	PreparedStatement prepare_scrollable_query ( String statement ) throws SQLException	{
		if (logger.isDebugEnabled()) logger.debug(stream_number + "> prepare_scrollable_statement");
		if (logger.isDebugEnabled()) logger.debug(stream_number + ">    " + statement);

		start_operation();
		PreparedStatement preparedstatement = dbconnection.prepareStatement(statement,ResultSet.TYPE_SCROLL_SENSITIVE,ResultSet.CONCUR_READ_ONLY);
		end_operation();

		if( logger.isInfoEnabled() ) {
			SQLWarning warning = preparedstatement.getWarnings();
			while ( warning != null ) {
				logger.warn(warning.getMessage());
				warning = warning.getNextWarning();
			}
		}
		if (logger.isDebugEnabled()) logger.debug(stream_number + "< prepare_scrollable_statement");
		return preparedstatement;
	}

	CallableStatement prepare_call_statement ( String statement ) throws SQLException
	{
		if (logger.isDebugEnabled()) logger.debug(stream_number + "> prepare_call_statement");
		if (logger.isDebugEnabled()) logger.debug(stream_number + ">    " + statement);

		start_operation();
		CallableStatement call_stmt = dbconnection.prepareCall(statement);
		end_operation();

		if( logger.isInfoEnabled() ) {
			SQLWarning warning = call_stmt.getWarnings();
			while ( warning != null ) {
				logger.warn(warning.getMessage());
				warning = warning.getNextWarning();
			}
		}
		if (logger.isDebugEnabled()) logger.debug(stream_number + "< prepare_call_statement");
		return call_stmt;
	}

	ResultSet execute_scrollable_query ( String statement ) throws SQLException {
		if ( scrollable_dbstatement == null ) { 
			scrollable_dbstatement = dbconnection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,ResultSet.CONCUR_READ_ONLY);
		}
		if (logger.isDebugEnabled()) logger.debug(stream_number + "> execute_query");
		if (logger.isDebugEnabled()) logger.debug(stream_number + ">    " + statement);

		start_operation();
		ResultSet resultset = scrollable_dbstatement.executeQuery(statement);
		end_operation();

		if( logger.isInfoEnabled() ) {
			SQLWarning warning = scrollable_dbstatement.getWarnings();
			while ( warning != null ) {
				logger.warn(warning.getMessage());
				warning = warning.getNextWarning();
			}
		}
		if (logger.isDebugEnabled()) logger.debug(stream_number + "< execute_query");
		return resultset;
	}

	ResultSet execute_query ( String statement ) throws SQLException {
		if ( dbstatement == null ) { dbstatement = dbconnection.createStatement(); }
		if (logger.isDebugEnabled()) logger.debug(stream_number + "> execute_query");
		if (logger.isDebugEnabled()) logger.debug(stream_number + ">    " + statement);

		start_operation();
		ResultSet resultset = dbstatement.executeQuery(statement);
		end_operation();

		if( logger.isInfoEnabled() ) {
			SQLWarning warning = dbstatement.getWarnings();
			while ( warning != null ) {
				logger.warn(warning.getMessage());
				warning = warning.getNextWarning();
			}
		}
		if (logger.isDebugEnabled()) logger.debug(stream_number + "< execute_query");
		return resultset;
	}

	void execute_statement ( String statement ) throws SQLException {
		if ( dbstatement == null ) { dbstatement = dbconnection.createStatement(); }
		if (logger.isDebugEnabled()) logger.debug(stream_number + "> execute_statement");
		if (logger.isDebugEnabled()) logger.debug(stream_number + ">    " + statement);

		start_operation();
		dbstatement.execute(statement);
		end_operation();

		if( logger.isInfoEnabled() ) {
			SQLWarning warning = dbstatement.getWarnings();
			while ( warning != null ) {
				logger.warn(warning.getMessage());
				warning = warning.getNextWarning();
			}
		}
		if (logger.isDebugEnabled()) logger.debug(stream_number + "< execute_statement");
	}

	void add_batch ( String statement ) throws SQLException {
		if ( batch_dbstatement == null ) { batch_dbstatement = dbconnection.createStatement(); }
		if (logger.isDebugEnabled()) logger.debug(stream_number + "> add_batch");
		if (logger.isDebugEnabled()) logger.debug(stream_number + "    " + statement);

		start_operation();
		batch_dbstatement.addBatch(statement);
		end_operation();
	
		if (logger.isDebugEnabled()) logger.debug(stream_number + "< add_batch");
	}

	void execute_batch () throws SQLException, Exception {
		if (logger.isDebugEnabled()) logger.debug( stream_number + ">  Execute Batch" );

		start_operation();
		int[] counts = batch_dbstatement.executeBatch();
		end_operation();

		
		if (logger.isDebugEnabled()) logger.debug( stream_number + ">	Execute Batch:  count.length " + counts.length );
		for ( int idx = 0; idx < counts.length; idx++ ) {
			if ( counts[idx] > 0 ) {
				if (logger.isDebugEnabled()) logger.debug( stream_number + ">      Statement " + idx + " success. Rows updated = " + counts[idx] );
			} else if ( counts[idx] == Statement.SUCCESS_NO_INFO ) {
				if (logger.isDebugEnabled()) logger.debug(stream_number + ">      Statement " + idx + " success. Rows updated = Statement.SUCCESS_NO_INFO" );
			} else if ( counts[idx] == Statement.EXECUTE_FAILED ) {
				throw new Exception("ERROR:  Batch statement " + idx + " failed. Rows updated = Statement.EXECUTE_FAILED");
			}
		}
		batch_dbstatement.clearBatch();
		if (logger.isDebugEnabled()) logger.debug( stream_number + "<  Execute Batch" );
	}
	
	void commit_work () throws SQLException, Exception {
		if (logger.isDebugEnabled()) logger.debug( stream_number + ">  commit_work" );

		dbconnection.commit();

		if (logger.isDebugEnabled()) logger.debug( stream_number + "<  commit_work" );
	}

	void rollback_work () throws SQLException, Exception {
		if (logger.isDebugEnabled()) logger.debug( stream_number + ">  rollback_work" );

		dbconnection.rollback();

		if (logger.isDebugEnabled()) logger.debug( stream_number + "<  rollback_work" );
	}

}