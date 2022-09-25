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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.antlr.v4.runtime.ParserRuleContext;

import org.apache.log4j.PropertyConfigurator;

public class Logger {
  private org.apache.log4j.Logger LOG;

  static {
    System.setProperty("hostName", System.getenv("HOSTNAME"));
    String logDir = System.getenv("TRAF_LOG");
    System.setProperty("TRAF_LOG", logDir);
    String confFile = System.getProperty("trafodion.log4j.configFile");
    String logFile = logDir + "/trafodion.sql.java.${hostName}.log";
    if (confFile == null) {
      System.setProperty("trafodion.sql.log", logFile);
      confFile = System.getenv("TRAF_CONF") + "/log4j.sql.config";
    }
    PropertyConfigurator.configure(confFile);
  }

  public Logger(Class cls) {
    LOG = org.apache.log4j.Logger.getLogger(cls);
  }

  public static Logger getLogger(Class cls) {
    return new Logger(cls);
  }

  /**
   * Trace information
   */
  public void trace(ParserRuleContext ctx, String message) {
    if (ctx != null) {
      LOG.trace("Ln:" + ctx.getStart().getLine() + " " + message);
    }
    else {
      LOG.trace(message);
    }
  }
  
  public void trace(String message) {
    trace(null, message);
  }

  /**
   * Trace values retrived from the database
   */
  public void trace(ParserRuleContext ctx, Var var, ResultSet rs, ResultSetMetaData rm, int idx) throws SQLException {
    if (var.type != Var.Type.ROW) {
      trace(ctx, "COLUMN: " + rm.getColumnName(idx) + ", " + rm.getColumnTypeName(idx));
      trace(ctx, "SET " + var.getName() + " = " + var.toString());  
    }
    else {
      Row row = (Row)var.value;
      int cnt = row.size();
      for (int j = 1; j <= cnt; j++) {
        Var v = row.getValue(j - 1);
        trace(ctx, "COLUMN: " + rm.getColumnName(j) + ", " + rm.getColumnTypeName(j));
        trace(ctx, "SET " + v.getName() + " = " + v.toString());
      }
    }
  }
  
  /**
   * Debug messages
   */
  public void debug(ParserRuleContext ctx, String message) {
    if (ctx != null) {
      LOG.debug("Ln:" + ctx.getStart().getLine() + " " + message);
    }
    else {
      LOG.debug(message);
    }
  }

  public void debug(String message) {
    debug(null, message);
  }

  /**
   * Informational messages
   */
  public void info(ParserRuleContext ctx, String message) {
    if (ctx != null) {
      LOG.info("Ln:" + ctx.getStart().getLine() + " " + message);
    }
    else {
      LOG.info(message);
    }
  }

  public void info(String message) {
    info(null, message);
  }

  /**
   * Warning messages
   */
  public void warn(ParserRuleContext ctx, String message) {
    if (ctx != null) {
      LOG.warn("Ln:" + ctx.getStart().getLine() + " " + message);
    }
    else {
      LOG.warn(message);
    }
  }

  public void warn(String message) {
    warn(null, message);
  }

  /**
   * Error message
   */
  public void error(ParserRuleContext ctx, String message) {
    if (ctx != null) {
      LOG.error("Ln:" + ctx.getStart().getLine() + " " + message);
    }
    else {
      LOG.error(message);
    }
  }

  public void error(ParserRuleContext ctx, String message, Exception e) {
    if (ctx != null) {
      LOG.error("Ln:" + ctx.getStart().getLine() + " " + message, e);
    }
    else {
      LOG.error(message, e);
    }
  }
  
  public void error(String message) {
    error(null, message);
  }

  public void error(String message, Exception e) {
    error(null, message, e);
  }
}
