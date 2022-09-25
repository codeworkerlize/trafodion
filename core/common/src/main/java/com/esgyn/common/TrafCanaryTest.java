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

/***
 * This is a canary test program that measures the database connection time and the 
 * time taken to execute a canary query
 * 
 * This is run every 5 minutes to measure the response time of the database stack
 * 
 */
public class TrafCanaryTest {

	public static void main(String args[]) throws Exception {

		Connection  connection = null;
		Statement   stmt = null;
		ResultSet   rs;

		String t2DriverClassName = "org.apache.trafodion.jdbc.t2.T2Driver";
		String jdbcUrl = "jdbc:t2jdbc:";

		String canaryQuery = "SELECT object_name FROM TRAFODION.\"_MD_\".OBJECTS" +
				" WHERE CATALOG_NAME = 'TRAFODION' and SCHEMA_NAME = '_MD_' " +
				" AND OBJECT_NAME = 'OBJECTS' and OBJECT_TYPE = 'BT' " +
				" FOR READ UNCOMMITTED ACCESS ";

		try {
			Class.forName(t2DriverClassName);
			long lstartTime = System.currentTimeMillis();
			connection = DriverManager.getConnection(jdbcUrl);

			long lendTime = System.currentTimeMillis();
			long connTimeInMillis = (lendTime - lstartTime);

			lstartTime = System.currentTimeMillis();
			stmt = connection.createStatement();
			rs = stmt.executeQuery(canaryQuery);
			while (rs.next()) {
				rs.getString("object_name");
			}
			rs.close();
			lendTime = System.currentTimeMillis();
			
			//This program is invoked by tcollector and the following metrics are published into OpenTSDB
			System.out.println(String.format("esgyndb.canary.sqlconnect.time %s %s", lendTime, connTimeInMillis));
			System.out.println(String.format("esgyndb.canary.sqlread.time %s %s", lendTime, (lendTime - lstartTime)));
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
