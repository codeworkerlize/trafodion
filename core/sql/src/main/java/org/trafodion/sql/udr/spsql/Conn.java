/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.trafodion.sql.udr.spsql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Stack;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.PreparedStatement;

public class Conn {
 
  public enum Type {TRAFODION, DB2, HIVE, MYSQL, TERADATA};
  
  HashMap<String, Stack<Connection>> connections = new HashMap<String, Stack<Connection>>();
  HashMap<String, String> connStrings = new HashMap<String, String>();
  HashMap<String, Type> connTypes = new HashMap<String, Type>();
  
  HashMap<String, ArrayList<String>> connInits = new HashMap<String, ArrayList<String>>();
  HashMap<String, ArrayList<String>> preSql = new HashMap<String, ArrayList<String>>();
  HashMap<String, Query> prepareSqls = new HashMap<String, Query>();
  
  private static final Logger LOG = Logger.getLogger(Conn.class);

  Exec exec;
  Timer timer = new Timer();
  boolean trace = false;  
  boolean info = false;
  
  Conn(Exec e) {
    exec = e;  
    trace = exec.getTrace();
    info = exec.getInfo();
  }
  
  /**
   * Execute a SQL query
   */
  public Query executeQuery(Query query, String connName) {
    LOG.debug(null, "Execute Query: " + query.sql);
    try {
      Connection conn = getConnection(connName);
      query.setConnection(conn);
      runPreSql(connName, conn);
      Statement stmt = conn.createStatement();
      query.setStatement(stmt);
      LOG.trace(null, "Starting query");
      timer.start();
      ResultSet rs = stmt.executeQuery(query.sql);
      query.setResultSet(rs);
      timer.stop();
      LOG.debug(null, "Query executed successfully (" + timer.format() + ")");
    } catch (Exception e) {
      LOG.error("Execute Query: " + query.sql);
      query.setError(e);
    }
    return query;
  }
  
  public Query executeQuery(String sql, String connName) {
    return executeQuery(new Query(sql), connName);
  }

  public Query executePreparedQuery(Query query, ArrayList paramList, String connName) {
    LOG.debug(null, "Execute Prepared Query: " + query.sql);
    try {
      Connection conn = query.getConnection();
      runPreSql(connName, conn);
      PreparedStatement pstmt = query.getPreparedStatement();
      ResultSet rs = null;
      for (int i = 0; i < paramList.size(); i++) {
        LOG.trace(null, "  set param " + i + ": " + paramList.get(i));
        pstmt.setObject(i + 1, paramList.get(i));
      }
      LOG.trace(null, "Start executing prepared SQL statement");
      timer.start();
      if (pstmt.execute()) {
        rs = pstmt.getResultSet();        
      } 
      timer.stop();
      query.setResultSet(rs);
      LOG.debug(null, "Prepared SQL statement executed successfully (" + timer.format() + ")");
    } catch (Exception e) {
      LOG.error("Execute Prepared Query: " + query.sql);
      query.setError(e);
    }
    return query;
  }

  public Query executePreparedSql(String preparedSqlName, ArrayList paramList, String connName) {
    Query query = prepareSqls.get(preparedSqlName);
    return executePreparedQuery(query, paramList, connName);
  }  
  
  /**
   * Prepare a SQL query
   */
  public Query prepareQuery(Query query, String connName) {
    LOG.debug(null, "Prepare Query: " + query.sql);
    try {
      Connection conn = getConnection(connName);
      query.setConnection(conn);
      timer.start();
      PreparedStatement stmt = conn.prepareStatement(query.sql);
      query.setPreparedStatement(stmt);
      timer.stop();
      LOG.debug(null, "Prepared statement executed successfully (" + timer.format() + ")");
    } catch (Exception e) {
      LOG.error("Prepare Query: " + query.sql);
      query.setError(e);
    }
    return query;
  }

  /**
   * Prepare a SQL statement
   */
  public Query prepareSql(Query query, String prepareSqlName, String connName) {
    LOG.debug(null, "Prepare Sql: " + query.sql);
    try {
      Connection conn = getConnection(connName);
      query.setConnection(conn);
      timer.start();
      PreparedStatement pstmt = conn.prepareStatement(query.sql);
      timer.stop();
      query.setPreparedStatement(pstmt);
      prepareSqls.put(prepareSqlName, query);
      LOG.debug(null, "Prepared statement executed successfully (" + timer.format() + ")");
    } catch (Exception e) {
      LOG.error("Prepare Sql: " + query.sql);
      query.setError(e);
    }
    return query;
  }
  
  /**
   * Execute a SQL statement
   */
  public Query executeSql(String sql, String connName) {
    LOG.debug(null, "Execute SQL: " + sql);
    Query query = new Query(sql);
    try {
      Connection conn = getConnection(connName);
      query.setConnection(conn);
      runPreSql(connName, conn);
      Statement stmt = conn.createStatement();
      query.setStatement(stmt);
      ResultSet rs = null;
      LOG.trace(null, "Starting SQL statement");
      timer.start();
      if (stmt.execute(sql)) {
        rs = stmt.getResultSet();        
        query.setResultSet(rs);
      } 
      timer.stop();
      LOG.debug(null, "SQL statement executed successfully (" + timer.format() + ")");
    } catch (Exception e) {
      LOG.error("Execute SQL: " + sql);
      query.setError(e);
    }
    return query;
  }
  
  /**
   * Close the query object
   */
  public void closeQuery(Query query, String connName) {
    query.closeStatement(); 
    if (!exec.conf.useOnlyOneConnection) {
      returnConnection(connName, query.getConnection());
    }
  }

  public void closeQuery(Query query) {
    query.closeStatement();
  }
  
  /**
   * Run pre-SQL statements 
   * @throws SQLException 
   */
  void runPreSql(String connName, Connection conn) throws SQLException {
    ArrayList<String> sqls = preSql.get(connName);  
    if (sqls != null) {
      Statement s = conn.createStatement();
      for (String sql : sqls) {
        LOG.trace(null, "Starting pre-SQL statement");
        s.execute(sql);
      }
      s.close();
      preSql.remove(connName);
    }
  }
  
  Connection getConnection(String connName) throws Exception {
    LOG.trace("Get connection " + connName);
    Connection conn = _getConnection(connName);
    String schema = exec.getCurrentSchema();
    LOG.trace("set connection schema " + schema);
    Statement stmt = conn.createStatement();
    String sql = null;
    if (exec.getConnectionType() == Conn.Type.TRAFODION) {
      sql = "SET SCHEMA " + schema;
    } else {
      sql = "USE " + schema;
    }
    stmt.execute(sql);
    stmt.close();
    LOG.trace("Get connection return: " + conn);
    return conn;
  }

  /** 
   * Get a connection
   * @throws Exception 
   */
  synchronized Connection _getConnection(String connName) throws Exception {
    Stack<Connection> connStack = connections.get(connName);
    String connStr = connStrings.get(connName);
    if (connStr == null) {
      throw new Exception("Unknown connection profile: " + connName);
    }
    if (connStack != null && !connStack.empty()) {        // Reuse an existing connection
      if (exec.conf.useOnlyOneConnection) {
        return connStack.peek();
      }
      return connStack.pop();
    }
    Connection c = openConnection(connStr);
    if (exec.manageTransaction(connName)) {
      String sql;
      if (exec.getConnectionType(connName) == Conn.Type.TRAFODION) {
        sql = "BEGIN WORK";
      } else {
        sql = "BEGIN TRANSACTION";
      }
      Statement s = c.createStatement();
      LOG.trace("Executing connection init SQL: " + sql);
      s.execute(sql);
      s.close();
    }
    ArrayList<String> sqls = connInits.get(connName);     // Run initialization statements on the connection
    if (sqls != null) {
      Statement s = c.createStatement();
      for (String sql : sqls) {
        LOG.trace("Executing connection init SQL: " + sql);
        s.execute(sql);
      }
      s.close();
    }
    if (exec.conf.useOnlyOneConnection) {
      return connStack.push(c);
    }
    return c;
  }
  
  /**
   * Open a new connection
   * @throws Exception 
   */
  Connection openConnection(String connStr) throws Exception {
    String driver = "org.apache.hadoop.hive.jdbc.HiveDriver";
    StringBuilder url = new StringBuilder();
    String usr = "";
    String pwd = "";
    if (connStr != null) {
      String[] c = connStr.split(";");
      if (c.length >= 1) {
        driver = c[0];
      }
      if (c.length >= 2) {
        url.append(c[1]);
      }
      else {
        url.append("jdbc:hive://");
      }
      for (int i = 2; i < c.length; i++) {
        if (c[i].contains("=")) {
          url.append(";");
          url.append(c[i]);          
        }
        else if (usr.isEmpty()) {
          usr = c[i];
        }
        else if (pwd.isEmpty()) {
          pwd = c[i];
        }
      }
    }
    Class.forName(driver);
    timer.start();
    Connection conn = DriverManager.getConnection(url.toString().trim(), usr, pwd);
    timer.stop();
    if (info) {
      LOG.info(null, "Open connection: " + url + " (" + timer.format() + ")");
    }
    return conn;
  }
  
  /**
   * Get the database type by profile name
   */
  Conn.Type getTypeByProfile(String name) {
    return connTypes.get(name);
  }
  
  /**
   * Get the database type by connection string
   */
  Conn.Type getType(String connStr) {
    if (connStr.contains("hive.")) {
      return Type.HIVE;
    }
    else if (connStr.contains("db2.")) {
      return Type.DB2;
    }
    else if (connStr.contains("mysql.")) {
      return Type.MYSQL;
    }
    else if (connStr.contains("teradata.")) {
      return Type.TERADATA;
    }
    else if (connStr.contains("trafodion.")) {
      return Type.TRAFODION;
    }
    return Type.HIVE;
  }
  
  /**
   * Return the connection to the pool
   */
  void returnConnection(String name, Connection conn) {
    if (conn != null) {
      connections.get(name).push(conn);
    }
  }
  
  /**
   * Add a new connection string
   */
  public void addConnection(String name, String connStr) {
    connections.put(name, new Stack<Connection>());
    connStrings.put(name, connStr);
    connTypes.put(name, getType(connStr));
  }
  
  /**
   * Clear default connections in the pool
   */
  public void clearDefaultConnections() {
    connections.get("default").clear();
  }

  /**
   * Add initialization statements for the specified connection
   */
  public void addConnectionInit(String name, String connInit) {
    ArrayList<String> a = new ArrayList<String>(); 
    String[] sa = connInit.split(";");
    for (String s : sa) {
      s = s.trim();
      if (!s.isEmpty()) {
        a.add(s);
      }
    }    
    connInits.put(name, a);
  }
  
  /**
   * Add SQL statements to be executed before executing the next SQL statement (pre-SQL)
   */
  public void addPreSql(String name, ArrayList<String> sql) {
    preSql.put(name, sql); 
  }
}
