// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2019 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@
package com.esgyn.common;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

/**
 * This program finds orphan query statistics records in the metric query table and updates their status
 * An orphan query stats record is one with a start time but no end stats, because the mxosrvr that published the 
 * begin stats record has crashed and did not publish end stats
 *
 */
public class OrphanQueryUpdater {

	public static void main(String args[]) throws Exception {

		Connection          connection = null;
		Statement           stmt = null;
		Statement           updateStmt = null;
		ResultSet           rs;

		List<String> cmdGetMxosrvrs = Arrays.asList("/bin/bash", "-c", "sqps|grep mxosrvr | cut -d ' ' -f 7");

		String t2DriverClassName = "org.apache.trafodion.jdbc.t2.T2Driver";
		String jdbcUrl = "jdbc:t2jdbc:";

		String SELECT_ACTIVE_QUERIES = "SELECT QUERY_ID, trim(PROCESS_NAME) as PROCESS_NAME, "
				+ "CAST(EXEC_START_UTC_TS AS CHAR(26)) AS START_TIME, "
				+ "QUERY_ELAPSED_TIME AS ELAPSED_TIME_SEC "
				+ "FROM \"_REPOS_\".METRIC_QUERY_TABLE WHERE EXEC_START_UTC_TS > CURRENT_TIMESTAMP - INTERVAL '7' DAY "
				+ "AND EXEC_START_UTC_TS < CURRENT_TIMESTAMP AND EXEC_END_UTC_TS IS NULL;";

		String CQD_STMT = "set parserflags 131072;";

		String UPDATE_ORPHAN_QUERY = "UPDATE TRAFODION.\"_REPOS_\".METRIC_QUERY_TABLE SET QUERY_STATUS = 'UNKNOWN', "
				+ "EXEC_END_UTC_TS = TIMESTAMP '%s' + INTERVAL '%s' second(6) "
				+ "WHERE QUERY_ID='%s' and EXEC_START_UTC_TS = TIMESTAMP '%s';";

		try {
			Class.forName(t2DriverClassName);
			connection = DriverManager.getConnection(jdbcUrl);
			stmt = connection.createStatement();
			stmt.executeUpdate(CQD_STMT);
			updateStmt = connection.createStatement();

			//Find all running mxosrvrs
			String sqpsOutput = CommonHelper.runShellCommand(cmdGetMxosrvrs);
			String[] activeMxosrvrs = sqpsOutput.split("\n");
			//System.out.println(sqpsOutput);
			List<String> activeMxosrvrList = Arrays.asList(activeMxosrvrs);


			//Get list of active queries
			rs = stmt.executeQuery(SELECT_ACTIVE_QUERIES);
			while (rs.next()) {
				String processName = rs.getString("PROCESS_NAME");
				//System.out.println("Query Process Name : " + processName);

				//Check the mxosrvr process name associated with the query is still active, then it means query is still executing
				//If the mxosrvr process name does not exist, then the query is an orphan query
				if(processName != null && processName.length() > 0 && !activeMxosrvrList.contains(processName)) {

					//Query is orphan
					String queryID = rs.getString("QUERY_ID");
					String startTime = rs.getString("START_TIME");
					long elapsedTime = rs.getLong("ELAPSED_TIME_SEC");
					if(elapsedTime < 60) {
						elapsedTime = 100;
					}
					String updateQuery = String.format(UPDATE_ORPHAN_QUERY, startTime, elapsedTime, queryID, startTime);
					try {
						//System.out.println(updateQuery); 
						updateStmt.executeUpdate(updateQuery);
						//System.out.println("Updated orphan query status for qid " + queryID); 
					}catch(Exception ex) {
						System.out.println(ex.getMessage());
					}
				}
			}
			rs.close();

		} catch (Exception e) {
			System.out.println(e.getMessage());
		} finally {
			try {
				if(stmt != null)
					stmt.close();
			} catch (SQLException e) {
			}
			try {
				if(connection != null)
					connection.close();
			} catch (SQLException e) {
			}
		}
	}  
}
