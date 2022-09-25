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
import java.sql.SQLException;

public class SPSQL {
  private static final Logger LOG = Logger.getLogger(SPSQL.class);

  private static String logFile = null;
  private static Exec exec = null;
  private static boolean trace = true;

  Timer timer = new Timer();

  public SPSQL() throws SQLException {
    try {
      init();
    } catch (Exception e) {
      error("SPSQL init failed with exception:", e);
      throw new SQLException("SPSQL Initialization failed with exception " + e, e);
    }
  }

  public void info(String msg) {
    LOG.info(msg);
  }

  public void warn(String msg) {
    LOG.warn(msg);
  }
  public void trace(String msg) {
    LOG.trace(msg);
  }
  public void debug(String msg) {
    LOG.debug(msg);
  }

  public void error(String msg) {
    LOG.error(msg);
  }

  public void error(String msg, Exception e) {
    LOG.error(msg, e);
  }
    
  private void init() throws Exception {
    if (exec != null) {
      exec.clearConnections();
      return;
    }
    info("======== Starting SPSQL ========");
    exec = new Exec();
    exec.init();
  }

  public void run(String query) throws SQLException {
    trace("Run query: " + query);
    try {
      exec.runQuery(query);
    } catch (SQLException e) {
      throw e;
    } catch (Exception e) {
      String msg = e.getMessage();
      if (msg == null) {
        msg = "Caused by " + e.getClass().getName();
      }
      throw new SQLException(msg, e);
    }
  }
  
  private void initGlobalScope() {
    exec.initGlobalScope();
  }

  private void leaveScope() {
    exec.leaveScope();
  }

  // This function is only called when there are exceptions thrown
  // during execution, so rollback the transaction.
  public void closeConnections() throws SQLException {
    try {
      exec.closeConnections(true /* rollback */);
    } catch (SQLException e) {
      throw e;
    } catch (Exception e) {
      throw new SQLException(e.getMessage(), e);
    }
  }

  public void execute(String query) throws SQLException {
    info("======== START EXECUTE ========");
    String result = " FAILED ";   // assume failure
    timer.start();
    try {
      initGlobalScope();
      run(query);
      leaveScope();
      result = " SUCCEEDED ";
    } catch (SQLException e) {
      error("SQL exception: ", e);
      closeConnections();
      throw e;
    } catch (NumberFormatException e) {
      error("NumberFormatException: ", e);
      closeConnections();
      String errmsg = "number format error";
      if (e.getMessage() != null) {
        errmsg += ": " + e.getMessage();
      }
      throw new SQLException(errmsg, e);
    } catch (Exception e) {
      error("Exception: ", e);
      closeConnections();
      throw new SQLException(e.getMessage(), e);
    } finally {
      timer.stop();
      info("======== END EXECUTE " + result + " (" + timer.format() +") ========");
    }
  }

  public void callProc(String name, Object[] args) throws SQLException {
    info("======== START PROCEDURE " + name + " ========");
    String result = " FAILED ";   // assume failure
    timer.start();
    trace("procedure name: " + name);
    trace("procedure # of args: " + args.length);
    name = exec.meta.removeDoubleQuote(name);
    try {
      initGlobalScope();
      exec.callProc(name, args);
      leaveScope();
      result = " SUCCEEDED ";
    } catch (SQLException e) {
      error("SQL exception: ", e);
      closeConnections();
      throw e;
    } catch (NumberFormatException e) {
      error("NumberFormatException: ", e);
      closeConnections();
      String errmsg = "number format error";
      if (e.getMessage() != null) {
        errmsg += ": " + e.getMessage();
      }
      throw new SQLException(errmsg, e);
    } catch (Exception e) {
      error("Exception: ", e);
      closeConnections();
      throw new SQLException(e.getMessage(), e);
    } finally {
      timer.stop();
      info("======== END PROCEDURE " + name + result + " (" + timer.format() + ") ========");
    }
  }

  public void callFunc(String name, Object[] args) throws SQLException {
    info("======== START FUNCTION " + name + "========");
    String result = " FAILED ";   // assume failure
    timer.start();
    trace("function name: " + name);
    trace("function # of args: " + args.length);
    name = exec.meta.removeDoubleQuote(name);
    try {
      initGlobalScope();
      exec.callFunc(name, args);
      leaveScope();
      result = " SUCCEEDED ";
    } catch (SQLException e) {
      error("SQL exception: ", e);
      closeConnections();
      throw e;
    } catch (NumberFormatException e) {
      error("NumberFormatException: ", e);
      closeConnections();
      String errmsg = "number format error";
      if (e.getMessage() != null) {
        errmsg += ": " + e.getMessage();
      }
      throw new SQLException(errmsg, e);
    } catch (Exception e) {
      error("Exception: ", e);
      closeConnections();
      throw new SQLException(e.getMessage(), e);
    } finally {
      timer.stop();
      info("======== END FUNCTION " + name + result  + " (" + timer.format()+ ") ========");
    }
  }

  public void callTrigger(String name, int iscallbefore, int optype,
			  String oldskvBuffer, String oldsBufSize,
			  String oldrowIds, String oldrowIdsLen,
			  int newrowIdLen,
			  String newrowIds, int newrowIdsLen,
			  String newrows, int newrowsLen,
			  String curExecSql) throws SQLException {
    info("======== START TRIGGER FOR TABLE " + name + " ========");
    String result = " FAILED ";   // assume failure
    timer.start();
    trace("trigger table name: " + name);
    trace("current execute sql is: " + curExecSql);
    name = exec.meta.removeDoubleQuote(name);
    try {
      initGlobalScope();
      exec.callTrigger(name, iscallbefore, optype,
		       oldskvBuffer, oldsBufSize,
		       oldrowIds, oldrowIdsLen,
		       newrowIdLen,
		       newrowIds, newrowIdsLen,
		       newrows, newrowsLen,
		       curExecSql);
      leaveScope();
      result = " SUCCEEDED ";
    } catch (SQLException e) {
      error("SQL exception: ", e);
      closeConnections();
      throw e;
    } catch (NumberFormatException e) {
      error("NumberFormatException: ", e);
      closeConnections();
      String errmsg = "number format error";
      if (e.getMessage() != null) {
        errmsg += ": " + e.getMessage();
      }
      throw new SQLException(errmsg, e);
    } catch (Exception e) {
      error("Exception: ", e);
      closeConnections();
      throw new SQLException(e.getMessage(), e);
    } finally {
      timer.stop();
      info("======== END TRIGGER FOR TABLE " + name + result  + " (" + timer.format() + ") ========");
    }
  }
  
  public static void callSPSQL(String name, Object... args) throws SQLException {
    new SPSQL().callProc(name, args);
  }

  public static void callSPSQLTrigger(String name, int iscallbefore, int optype,
				      String oldskvBuffer, String oldsBufSize,
				      String oldrowIds, String oldrowIdsLen,
				      int newrowIdLen,
				      String newrowIds, int newrowIdsLen,
				      String newrows, int newrowsLen,
				      String curExecSql) throws SQLException {
    new SPSQL().callTrigger(name, iscallbefore, optype,
			    oldskvBuffer, oldsBufSize,
			    oldrowIds, oldrowIdsLen,
			    newrowIdLen,
			    newrowIds, newrowIdsLen,
			    newrows, newrowsLen,
			    curExecSql);
  }
  
  public static void callSPSQLFunc(String name, Object... args) throws SQLException {
    new SPSQL().callFunc(name, args);
  }

  public static void createSPSQL(String query) throws SQLException {
    new SPSQL().execute(query);
  }

  public static void dropSPSQL(String query) throws SQLException {
    new SPSQL().execute(query);
  }

  //Execute anonymous blocks
  public static void executeSPSQL(String query) throws SQLException {
    new SPSQL().execute(query);
  }  
}
