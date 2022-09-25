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

package org.trafodion.sql.udr.spsql.functions;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.Date;
import java.sql.Timestamp;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;

import org.apache.commons.lang.StringUtils;
import org.antlr.v4.runtime.ParserRuleContext;
import org.trafodion.sql.udr.spsql.*;
import org.trafodion.sql.udr.spsql.Var.Type;

interface FuncCommand {
  void run(HplsqlParser.Expr_func_paramsContext ctx);
}

interface FuncSpecCommand {
  void run(HplsqlParser.Expr_spec_funcContext ctx);
}

/**
 * HPL/SQL functions
 */
public class Function {
  private static final Logger LOG = Logger.getLogger(Function.class);

  Exec exec;
  HashMap<String, FuncCommand> map = new HashMap<String, FuncCommand>();  
  HashMap<String, FuncSpecCommand> specMap = new HashMap<String, FuncSpecCommand>();
  HashMap<String, FuncSpecCommand> specSqlMap = new HashMap<String, FuncSpecCommand>();
  HashMap<String, HplsqlParser.Create_function_stmtContext> userMap = new HashMap<String, HplsqlParser.Create_function_stmtContext>();
  HashMap<String, HplsqlParser.Create_procedure_stmtContext> procMap = new HashMap<String, HplsqlParser.Create_procedure_stmtContext>();
  HashMap<String, HplsqlParser.Create_trigger_stmtContext> betriggerMap = new HashMap<String, HplsqlParser.Create_trigger_stmtContext>();
  HashMap<String, HplsqlParser.Create_trigger_stmtContext> aftriggerMap = new HashMap<String, HplsqlParser.Create_trigger_stmtContext>();
  boolean trace = false; 
  
  // Builtin functions that we should skip checking for user defined
  // functions
  ArrayList<String> builtinNames = new ArrayList<String>(Arrays.asList(
    "ABS", "AES_ENCRYPT", "ACOS", "ADD_MONTHS", "ACOS", "ASCII",
    "ASIN", "ATAN", "ATAN2", "AUTHNAME", "AVG", "BUFFERTOLOB",
    "CEIL", "CHAR", "CHAR_LENGHT", "COALESCE", "CODE_VALUE",
    "CONCAT", "CONVERTTOHEX", "CONVERTTIMESTAMP", "COS", "COSH",
    "COUNT", "CURRENT", "CURRENT_DATE", "CURRENT_TIME",
    "CURRENT_TIMESTAMP", "CURRENT_USER", "DATE_ADD", "DATEADD",
    "DATEDIFF", "DATEFORMAT", "DATE_PART", "DATE_TRUNC", "DAY",
    "DAYNAME", "DAYOFMONTH", "DAYOFYEAR", "DECODE", "DEGREES",
    "DIFF1", "DIFF2", "EMPTY_BLOB", "EMPTY_CLOB", "EXP", "EXPLAIN",
    "EXTERNALTOLOB", "EXTRACT", "FILETOLOB", "FLOOR", "GROUP_CONCAT",
    "HOUR", "INSERT", "IS_IPV4", "INET_ATON", "ISNULL", "JULIANTIMESTAMP",
    "LASTNOTNULL", "LCASE", "LEFT", "LENGTH", "LOCATE", "LOG", "LOG10", "LOWER",
    "LPAD", "LTRIM", "MAX", "MAXIMUM", "MD5", "MIN", "MINUTE", "MOD",
    "MONTH", "MONTHNAME", "MOVINGAVG", "MOVINGCOUNT", "MOVINGMAX",
    "MOVINGMIN", "MOVINGSTDDEV", "MOVINGSUM", "MOVINGVARIANCE",
    "NULLIFZERO", "NVL", "OCTET_LENGTH", "OVERLAY", "PI", "PIVOT",
    "POSITION", "POWER", "QUARTER", "RADIANS", "RANK", "RUNNINGRANK",
    "REPEAT", "REPLACE", "RIGHT", "ROLLUP", "ROUND", "RPAD", "RTRIM",
    "RUNNINGAVG", "RUNNINGCOUNT", "RUNNINGMAX", "RUNNINGMIN",
    "RUNNINGSTDDEV", "RUNNINGSUM", "RUNNINGVARIANCE", "REVERSE",
    "SECOND", "SHA", "SHA2", "SIGN", "SIN", "SINH", "SPACE",
    "SPLIT_PART", "SQRT", "STDDEV", "STRINGTOLOB", "SUBSTR",
    "SUBSTRING", "SUM", "SYSDATE", "SYSTIMESTAMP", "TAN", "TANH",
    "THIS", "TIMESTAMPADD", "TIMESTAMPDIFF", "TO_CHAR", "TO_DATE",
    "TO_TIME", "TO_TIMESTAMP", "TRANSLATE", "TRUNC", "UCASE", "UPPER",
    "RAND", "USER", "SYS_GUID", "UUID", "UUID_SHORT", "SLEEP",
    "VARIANCE", "WEEK", "YEAR", "UNIX_TIMESTAMP", "ZEROIFNULL"));

  public Function(Exec e) {
    exec = e;  
    trace = exec.getTrace();
  }
  
  public boolean isBuiltinFunction(String name) {
    String n = name.toUpperCase();
    for (int i=0; i<builtinNames.size(); i++) {
      if (builtinNames.get(i).equals(n)) {
        LOG.trace(name + " is a builtin function");
        return true;
      }
    }
    LOG.trace(name + " is not a builtin function");
    return false;
  }

  /** 
   * Register functions
   */
  public void register(Function f) {    
  }
  
  /**
   * Remove all user defined routines
   */
  public void clear() {
    userMap.clear();
    procMap.clear();
    betriggerMap.clear();
    aftriggerMap.clear();
  }

  /**
   * Remove all user defined procedures
   */
  public void clearProcs() {
    procMap.clear();
  }

  /**
   * Remove all user defined functions
   */
  public void clearFuncs() {
    userMap.clear();
  }

  public void remove(String name) {
    userMap.remove(name);
    procMap.remove(name);
    betriggerMap.remove(name);
    aftriggerMap.remove(name);
  }

  public void removeFunc(String name) {
    userMap.remove(name);
  }

  public void removeProc(String name) {
    procMap.remove(name);
  }

  public void removeTrigger(String name) {
    betriggerMap.remove(name);
    aftriggerMap.remove(name);
  }
  
  /**
   * Get procedure by name
   */
  public HplsqlParser.Create_procedure_stmtContext getProc(String name) {
    HplsqlParser.Create_procedure_stmtContext proc = procMap.get(name.toUpperCase());
    String qualified = exec.qualify(name.toUpperCase());
    if (proc == null) {
      if (!qualified.equals(name)) {
        LOG.trace("try qualified procedure " + qualified);
        proc = procMap.get(qualified);
      }
      if (proc == null) {
        exec.loadProcedure(qualified);
        proc = procMap.get(qualified);
      }
    }
    return proc;
  }

  public HplsqlParser.Create_function_stmtContext getFunc(String name) {
    LOG.trace("GET FUNC " + name);
    HplsqlParser.Create_function_stmtContext func = userMap.get(name.toUpperCase());
    if (exec.conf.checkBuiltinFunction && isBuiltinFunction(name)) {
      return null;
    }
    String qualified = exec.qualify(name.toUpperCase());
    if (func == null) {
      if (!qualified.equals(name)) {
        LOG.trace("try qualified function " + qualified);
        func = userMap.get(qualified);
      }
      if (func == null) {
        exec.loadFunction(qualified);
        func = userMap.get(qualified);
      }
    }
    return func;
  }

  public HplsqlParser.Create_trigger_stmtContext getTrigger(String trgname, int iscallbefore, int optype) {
    HplsqlParser.Create_trigger_stmtContext trigger = null;
    LOG.trace("The trigger name is: " + trgname);
    if (iscallbefore == 1) {
        trigger = betriggerMap.get(trgname.toUpperCase());
    }
    else {
	trigger = aftriggerMap.get(trgname.toUpperCase());
    }
    
    String qualified = exec.qualify(trgname.toUpperCase());
    if (trigger == null) {
      if (!qualified.equals(trgname)) {
        LOG.trace("try qualified trigger " + qualified);
	if (iscallbefore == 1) {
            trigger = betriggerMap.get(qualified);
	}
	else {
	    trigger = aftriggerMap.get(qualified);
	}
      }
      if (trigger == null) {
	  exec.loadTrigger(qualified);
	  if (iscallbefore == 1) {
              trigger = betriggerMap.get(qualified);
	  }
	  else {
	      trigger = aftriggerMap.get(qualified);
	  }
      }
    }
    return trigger;
  }
  
  public boolean defined(String name) {
    if (userMap.get(name.toUpperCase()) != null) {
      return true;
    }
    return procMap.get(name.toUpperCase()) != null;
  }

  /**
   * Execute a function
   */
  public void exec(String name, HplsqlParser.Expr_func_paramsContext ctx) {
    if (execUser(name, ctx)) {
      return;
    }
    else if (isProc(name) && execProc(name, ctx, null)) {
      return;
    }
    if (name.indexOf(".") != -1) {               // Name can be qualified and spaces are allowed between parts
      String[] parts = name.split("\\.");
      StringBuilder str = new StringBuilder();
      for (int i = 0; i < parts.length; i++) {
        if (i > 0) {
          str.append(".");
        }
        str.append(parts[i].trim());        
      }
      name = str.toString();      
    } 
    if (trace && ctx != null && ctx.parent != null && ctx.parent.parent instanceof HplsqlParser.Expr_stmtContext) {
      trace(ctx, "FUNC " + name);      
    }
    FuncCommand func = map.get(name.toUpperCase());    
    if (func != null) {
      func.run(ctx);
    }    
    else {
      info(ctx, "Function not found: " + name);
      evalNull();
    }
  }
  
  /**
   * User-defined function in a SQL query
   */
  public void execSql(String name, HplsqlParser.Expr_func_paramsContext ctx) {
    if (execUserSql(ctx, name)) {
      return;
    }
    StringBuilder sql = new StringBuilder();
    sql.append(name);
    sql.append("(");
    if (ctx != null) {
      int cnt = ctx.func_param().size();
      for (int i = 0; i < cnt; i++) {
        sql.append(evalPop(ctx.func_param(i).expr()));
        if (i + 1 < cnt) {
          sql.append(", ");
        }
      }
    }
    sql.append(")");
    exec.stackPush(sql);
  }
  
  /**
   * Aggregate or window function in a SQL query
   */
  public void execAggWindowSql(HplsqlParser.Expr_agg_window_funcContext ctx) {
    exec.stackPush(exec.getFormattedText(ctx));
  }
  
  /**
   * Execute a user-defined function
   */
  public boolean execUser(String name, Object[] args) {
    trace("EXEC FUNCTION " + name);
    String qualified = exec.qualify(name.toUpperCase());
    trace("Qualified name " + qualified);
    HplsqlParser.Create_function_stmtContext userCtx = getFunc(qualified);
    if (userCtx == null) {
      return false;
    }
    exec.enterScope(Scope.Type.ROUTINE);
    exec.saveSchema(qualified);
    try {
      setCallParameters(args, userCtx.create_routine_params(), null);
      if (userCtx.declare_block_inplace() != null) {
        visit(userCtx.declare_block_inplace());
      }
      visit(userCtx.single_block_stmt());
      setReturnValue(name, args, userCtx.create_function_return());
    } finally {
      exec.restoreSchema();
      exec.leaveScope();
    }
    return true;
  }

  public void setReturnValue(String name, Object[] args,
                             HplsqlParser.Create_function_returnContext r) {
    String type = r.dtype().getText();
    String len = null;
    String scale = null;
    if (r.dtype_len() != null) {
      len = r.dtype_len().L_INT(0).getText();
      if (r.dtype_len().L_INT(1) != null) {
        scale = r.dtype_len().L_INT(1).getText();
      }
    }
    Var var = new Var(null, type, len, scale, null);
    var.cast(exec.stackPop());
    setOutParameter(name, args[args.length-1], var);
  }

  /**
   * Execute a user-defined function
   */
  public boolean execUser(String name, HplsqlParser.Expr_func_paramsContext ctx) {
    trace(ctx, "EXEC FUNCTION " + name);
    String qualified = exec.qualify(name.toUpperCase());
    trace(ctx, "Qualified name " + qualified);
    HplsqlParser.Create_function_stmtContext userCtx = getFunc(qualified);
    if (userCtx == null) {
      return false;
    }
    ArrayList<Var> actualParams = getActualCallParameters(ctx);
    exec.enterScope(Scope.Type.ROUTINE);
    exec.saveSchema(qualified);
    try {
      setCallParameters(ctx, actualParams, userCtx.create_routine_params(), null, name);
      if (userCtx.declare_block_inplace() != null) {
        visit(userCtx.declare_block_inplace());
      }
      visit(userCtx.single_block_stmt());
    } finally {
      exec.restoreSchema();
      exec.leaveScope();
    }
    return true;
  }
  
  /**
   * Execute a HPL/SQL user-defined function in a query 
   */
  public boolean execUserSql(HplsqlParser.Expr_func_paramsContext ctx, String name) {
    HplsqlParser.Create_function_stmtContext userCtx = getFunc(name.toUpperCase());
    if (userCtx == null) {
      return false;
    }

    int cnt = (ctx == null) ? 0 : ctx.func_param().size();
    if (exec.getConnectionType() == Conn.Type.TRAFODION) {
      StringBuilder sql = new StringBuilder();
      sql.append(name);
      sql.append("(");
      for (int i = 0; i < cnt; i++) {
        sql.append(evalPop(ctx.func_param(i).expr()));
        if (i + 1 < cnt) {
          sql.append(", ");
        }
      }
      sql.append(")");
      exec.stackPush(sql);
      return true;
    }

    StringBuilder sql = new StringBuilder();
    sql.append("hplsql('");
    sql.append(name);
    sql.append("(");
    for (int i = 0; i < cnt; i++) {
      sql.append(":" + (i + 1));
      if (i + 1 < cnt) {
        sql.append(", ");
      }
    }
    sql.append(")'");
    if (cnt > 0) {
      sql.append(", ");
    }
    for (int i = 0; i < cnt; i++) {
      sql.append(evalPop(ctx.func_param(i).expr()));
      if (i + 1 < cnt) {
        sql.append(", ");
      }
    }
    sql.append(")");
    exec.stackPush(sql);
    exec.registerUdf();
    return true;
  }
  
  /**
   * Execute a stored procedure as the entry point of the script (defined by -main option)
   */
  public boolean execProc(String name) {
    if (trace) {
      trace("EXEC PROCEDURE " + name);
    }
    HplsqlParser.Create_procedure_stmtContext procCtx = getProc(name);
    if (procCtx == null) {
      trace("Procedure not found");
      return false;
    }    
    exec.enterScope(Scope.Type.ROUTINE);
    exec.callStackPush(name);
    try {
      if (procCtx.create_routine_params() != null) {
        setCallParameters(procCtx.create_routine_params());
      }
      visit(procCtx.proc_block());
    } finally {
      exec.callStackPop();
      exec.leaveScope();
    }
    return true;
  }
  
  /**
   * Check if the stored procedure with the specified name is defined
   */
  public boolean isProc(String name) {
    if (getProc(name) != null) {
      return true;
    }
    return false;
  }
  
  /**
   * Execute a stored procedure using CALL or EXEC statement passing parameters
   */
  public boolean execProc(String name, HplsqlParser.Expr_func_paramsContext ctx, ParserRuleContext callCtx) {
    if (trace) {
      trace(callCtx, "EXEC PROCEDURE " + name);
    }
    String qualified = exec.qualify(name.toUpperCase());
    HplsqlParser.Create_procedure_stmtContext procCtx = getProc(qualified);
    if (procCtx == null) {
      trace(callCtx, "Procedure not found");
      return false;
    }    
    ArrayList<Var> actualParams = getActualCallParameters(ctx);
    HashMap<String, Var> out = new HashMap<String, Var>();
    exec.enterScope(Scope.Type.ROUTINE);
    exec.saveSchema(qualified);
    exec.callStackPush(name);
    try {
      if (procCtx.declare_block_inplace() != null) {
        visit(procCtx.declare_block_inplace());
      }
      if (procCtx.create_routine_params() != null) {
        setCallParameters(ctx, actualParams, procCtx.create_routine_params(), out, name);
      }
      visit(procCtx.proc_block());
    } finally {
      exec.callStackPop();
      exec.restoreSchema();
      exec.leaveScope();
    }
    for (Map.Entry<String, Var> i : out.entrySet()) {      // Set OUT parameters
      exec.setVariable(i.getKey(), i.getValue());
    }
    return true;
  }
  
  /**
   * Execute a stored procedure from UDR server
   */
  public boolean execProc(String name, Object[] args) {
    trace("EXEC PROCEDURE " + name);
    String qualified = exec.qualify(name.toUpperCase());
    HplsqlParser.Create_procedure_stmtContext procCtx = getProc(qualified);
    if (procCtx == null) {
      trace("Procedure not found");
      return false;
    }    
    HashMap<Integer, Var> out = new HashMap<Integer, Var>();
    exec.enterScope(Scope.Type.ROUTINE);
    exec.saveSchema(qualified);
    exec.callStackPush(name);
    try {
      if (procCtx.declare_block_inplace() != null) {
        visit(procCtx.declare_block_inplace());
      }
      if (procCtx.create_routine_params() != null) {
        setCallParameters(args, procCtx.create_routine_params(), out);
      }
      visit(procCtx.proc_block());
    } finally {
      exec.callStackPop();
      exec.restoreSchema();
      exec.leaveScope();
    }
    setOutParameters(name, procCtx.create_routine_params(), args, out);
    return true;
  }

  /**
   * Execute a stored trigger as the entry point of the script (defined by -main option)
   */
  public boolean execTrigger(String name, int iscallbefore, int optype) {
    if (trace) {
      trace("EXEC TRIGGER " + name);
    }
    HplsqlParser.Create_trigger_stmtContext procCtx = getTrigger(name, iscallbefore, optype);
    if (procCtx == null) {
      trace("Trigger not found");
      return false;
    }    
    exec.enterScope(Scope.Type.ROUTINE);
    exec.callStackPush(name);
    if (procCtx.create_routine_params() != null) {
      setCallParameters(procCtx.create_routine_params());
    }
    visit(procCtx.proc_block());
    exec.callStackPop();
    exec.leaveScope();       
    return true;
  }
  
  /**
   * Check if the stored trigger with the specified name is defined
   */
  public boolean isTrigger(String name, int iscallbefore, int optype) {
    if (getTrigger(name, iscallbefore, optype) != null) {
      return true;
    }
    return false;
  }
  
  /**
   * Execute a stored trigger using CALL or EXEC statement passing parameters
   */
  public boolean execTrigger(String name, int iscallbefore, int optype, HplsqlParser.Expr_func_paramsContext ctx, ParserRuleContext callCtx) {
    if (trace) {
      trace(callCtx, "EXEC TRIGGER " + name);
    }
    
    String qualified = exec.qualify(name.toUpperCase());
    HplsqlParser.Create_trigger_stmtContext procCtx = getTrigger(qualified, iscallbefore, optype);
    if (procCtx == null) {
      trace(callCtx, "Trigger not found");
      return false;
    }    
    ArrayList<Var> actualParams = getActualCallParameters(ctx);
    HashMap<String, Var> out = new HashMap<String, Var>();
    exec.enterScope(Scope.Type.ROUTINE);
    exec.saveSchema(qualified);
    exec.callStackPush(name);
    if (procCtx.declare_block_inplace() != null) {
      visit(procCtx.declare_block_inplace());
    }
    if (procCtx.create_routine_params() != null) {
      setCallParameters(ctx, actualParams, procCtx.create_routine_params(), out, name);
    }
    for (Map.Entry<String, Var> i : out.entrySet()) {      // Set OUT parameters
      exec.setVariable(i.getKey(), i.getValue());
    }
    return true;
  }
  
  /**
   * Execute a stored trigger from UDR server
   */
  public boolean execTrigger(String name, int iscallbefore, int optype, Object[] args) {
    trace("EXEC TRIGGER name is: " + name);
    String qualified = exec.qualify(name.toUpperCase());
    HplsqlParser.Create_trigger_stmtContext procCtx = getTrigger(qualified, iscallbefore, optype);
    if (procCtx == null) {
      trace("Trigger not found");
      return false;
    }    
    HashMap<Integer, Var> out = new HashMap<Integer, Var>();
    exec.enterScope(Scope.Type.ROUTINE);
    exec.saveSchema(qualified);
    exec.callStackPush(name);
    try {
      if (procCtx.declare_block_inplace() != null) {
        visit(procCtx.declare_block_inplace());
      }
      if (procCtx.create_routine_params() != null) {
        setCallParameters(args, procCtx.create_routine_params(), out);
      }
      visit(procCtx.proc_block());
    } finally {
      exec.callStackPop();
      exec.restoreSchema();
      exec.leaveScope();
    }
    if (args != null) {
      setOutParameters(name, procCtx.create_routine_params(), args, out);
    }
    return true;
  }

  /*
   *
   */
  public List<Row[]> processRowsData(Long tabId,
			     String oldskvBuffer, String oldsBufSize,
			     String oldRowIds, String oldRowIdsLen,
			     int newRowIdLen,
			     String newRowIds, int newRowIdsLen,
			     String newRows, int newRowsLen) {
    /*
     * now only support one row, if want to support multi rows, must modify here
     */
    // we should get values from base64
    ByteBuffer bbOldRows = null;
    ByteBuffer bbOldRowIds = null;
    ByteBuffer bbNewRowIds = null;
    ByteBuffer bbNewRows = null;
    if (oldskvBuffer != null && oldRowIds != null) {
        trace("not decode oldskvBuffer is " + oldskvBuffer);
        trace("not decode oldskvBuffer length is " + oldsBufSize);
        bbOldRows = ByteBuffer.wrap(java.util.Base64.getDecoder().decode(oldskvBuffer));
        // not need use for one row
        bbOldRowIds = ByteBuffer.wrap(java.util.Base64.getDecoder().decode(oldRowIds));
      }
    if (newRowIds != null && newRows != null) {
        trace("not decode newRows is " + newRows);
        trace("not decode newRows size is " + newRows.length());
	trace("not decode newRowIds is " + newRowIds);
	trace("not decode newRowIds size is " + newRowIds.length());
        bbNewRowIds = ByteBuffer.wrap(java.util.Base64.getDecoder().decode(newRowIds));
        bbNewRows = ByteBuffer.wrap(java.util.Base64.getDecoder().decode(newRows));
      }

    List<ByteBuffer[]> NewAndOldRows = null;
    try {
      NewAndOldRows = exec.getNewAndOldRows(bbOldRows, oldsBufSize,
					    bbOldRowIds, oldRowIdsLen,
					    bbNewRows,
					    bbNewRowIds,
					    newRowIdLen);
    } catch (Exception e) {
      trace("get new or old values error");
      return null;
    }
    trace("getNewAndOldRows size is " + NewAndOldRows.size());
    
    List<Row[]> arrRow = null;    
    try {
      // the numCols value is 1 for the alignedformat
      arrRow = exec.processRowsData(tabId, NewAndOldRows);
      } catch (Exception e) {
	trace("get old values or new value info error");
	return null;
    }
    trace("processRowsData deal with NewAndOldRows size is " + arrRow.size());
    
    return arrRow;
  }
  
  /*
   * Execute a stored trigger from UDR server
   */
  public boolean execTrigger(String name,
			     int iscallbefore, int optype, boolean isStatement,
			     Row[] arrRow,
			     Object[] args,
			     String curExecSql) {
    trace("EXEC TRIGGER name is: " + name);
    String qualified = exec.qualify(name.toUpperCase());
    HplsqlParser.Create_trigger_stmtContext procCtx = getTrigger(qualified, iscallbefore, optype);
    if (procCtx == null) {
      trace("Trigger not found");
      return false;
    }

    HashMap<Integer, Var> out = new HashMap<Integer, Var>();
    exec.enterScope(Scope.Type.ROUTINE);
    
    if (optype == 1) {
      exec.addVariable(new Var("INSERTING", Type.BOOL, true));
    } else {
      exec.addVariable(new Var("INSERTING", Type.BOOL, false));
    }

    if (optype == 2) {
      exec.addVariable(new Var("DELETING", Type.BOOL, true));
    } else {
      exec.addVariable(new Var("DELETING", Type.BOOL, false));
    }

    if (optype == 3) {
      exec.addVariable(new Var("UPDATING", Type.BOOL, true));
    } else {
      exec.addVariable(new Var("UPDATING", Type.BOOL, false));
    }

    exec.addVariable(new Var("ACTION_SQL", Type.STRING, curExecSql));
    if (arrRow != null) {
      if (arrRow[0] != null) {
	trace("execTrigger add new value");
        exec.addVariable(new Var("NEW", arrRow[0]));
      }
      if (arrRow[1] != null) {
	trace("execTrigger add old value");
        exec.addVariable(new Var("OLD", arrRow[1]));
      }
    }
    
    exec.saveSchema(qualified);
    exec.callStackPush(name);
    if (procCtx.declare_block_inplace() != null) {
      visit(procCtx.declare_block_inplace());
    }
    if (procCtx.create_routine_params() != null && args != null) {
      setCallParameters(args, procCtx.create_routine_params(), out);
    }
    visit(procCtx.proc_block());
    exec.callStackPop();
    exec.restoreSchema();
    exec.leaveScope();
    if (args != null) {
      setOutParameters(name, procCtx.create_routine_params(), args, out);
    }

    return true;
  }
  
  boolean setOutParameter(String name, Object obj, Var var) {
    Class cls = obj.getClass().getComponentType();
    Object val = null;
    if (cls == ResultSet.class) {
      // Handle result sets
      try {
        Var cur = exec.consumeReturnCursor(name);
        if (cur == null) {
          // No result sets to return, this also means we have
          // handled all out params
          return true;
        }
        Query query = (Query)cur.value;
	trace("return result set of " + name);
        val = query.getResultSet();
      } catch (IndexOutOfBoundsException e) {
        // No more result set to return, no more out params to
        // handle
        return true;
      }
    } else if (var == null || var.value == null || var.type == null) {
      val = null;
    } else if (cls == String.class) {
      val = stringValue(var);
    } else if (cls == short.class) {
      val = shortValue(var);
    } else if (cls == int.class) {
      val = intValue(var);
    } else if (cls == float.class) {
      val = floatValue(var);
    } else if (cls == double.class) {
      val = doubleValue(var);
    } else if (cls == Integer.class) {
      val = new Integer(intValue(var));
    } else if (cls == Long.class) {
      val = new Long(longValue(var));
    } else if (cls == Float.class) {
      val = new Float(floatValue(var));
    } else if (cls == Double.class) {
      val = new Double(doubleValue(var));
    } else if (cls == java.sql.Time.class) {
      val = java.sql.Time.valueOf(var.toString().substring(0,8));
    } else {
      val = var.value;
    }
    Array.set(obj, 0, val);
    return false;
  }

  public void setOutParameters(String name,
			       HplsqlParser.Create_routine_paramsContext formal,
			       Object[] args, HashMap<Integer, Var> out) {
    LOG.trace("SET OUT PARAMETERS for " + name);
    for (int i=0; i<args.length; i++) {
      if (isOutParam(args[i])) {
        Var var = out.get(i);
        LOG.trace("set out param #" + i + ": obj=" + args[i] + "var=" + var);
        if (setOutParameter(name, args[i], var)) {
          break;
        }
      }
    }
  }

  public void setCallParameters(Object[] args,
				HplsqlParser.Create_routine_paramsContext formal,
				HashMap<Integer, Var> out) {
    if (args == null || formal == null) {
      return;
    }
    int argCnt = args.length;
    int formalCnt = formal.create_routine_param_item().size();
    for (int i = 0; i < argCnt; i++) {
      if (i >= formalCnt) {
	break;
      }
      HplsqlParser.Create_routine_param_itemContext p = formal.create_routine_param_item(i);
      String name = p.ident().getText();
      String type = p.dtype().getText();
      String len = null;
      String scale = null;   
      if (p.dtype_len() != null) {
        len = p.dtype_len().L_INT(0).getText();
        if (p.dtype_len().L_INT(1) != null) {
          scale = p.dtype_len().L_INT(1).getText();
        }
      }
      Var value = toVar(args[i]);
      Var var = setCallParameter(name, type, len, scale, value);
      trace("SET PARAM " + name + " = " + var.toString());
      if (out != null && (p.T_OUT() != null || p.T_INOUT() != null)) {
        trace("Adding out param #" +i + ": name=" +name + " type=" + type);
	out.put(i, var);
      }
    }
  }

  /**
   * Set parameters for user-defined function call
   */
  public void setCallParameters(HplsqlParser.Expr_func_paramsContext actual, ArrayList<Var> actualValues, 
                                HplsqlParser.Create_routine_paramsContext formal,
                                HashMap<String, Var> out, String name_) {
    int actualCnt = 0;
    if (actualValues != null) {
      actualCnt = actualValues.size();
    }
    int formalCnt = 0;
    if (formal != null) {
      formalCnt = formal.create_routine_param_item().size();
    }
    for (int i = actualCnt; i < formalCnt; i++) { // add default values
      HplsqlParser.Create_routine_param_itemContext p = getCallParameter(actual, formal, i);
      if (p.dtype_default() != null) {
        Var default_ = evalPop(p.dtype_default());
        if (actualValues != null) {
          actualValues.add(default_);
          actualCnt++;
        }
        else {
          actualValues = new ArrayList<Var>(formalCnt);
          actualValues.add(default_);
          actualCnt++;
        }
      }
      else if (p.T_OUT() == null || p.T_IN() != null) {
        // NOTE2ME: We allow OUT param at the end to be omitted, this
        // is required because functions defined in EsgynDB are
        // converted to procedures with the return values converted to
        // OUT params at the end.
        //
        // This will also make it legal to call PROCEDUREs with the
        // OUT params at the end to be omitted, which, I think, is a
        // bonus feature.
        throw new RuntimeException("wrong number of arguments in call to '" + name_ + "'");
      }
    } 
    for (int i = 0; i < actualCnt; i++) {
      if (i >= formalCnt) {
        throw new RuntimeException("wrong number of arguments in call to '" + name_ + "'");
      }
      HplsqlParser.ExprContext a = null;
      if (actual != null && actual.func_param(i) != null) {
        a = actual.func_param(i).expr();
      }
      HplsqlParser.Create_routine_param_itemContext p = getCallParameter(actual, formal, i);
      String name = p.ident().getText();
      String type = p.dtype().getText();
      String len = null;
      String scale = null;   
      if (p.dtype_len() != null) {
        len = p.dtype_len().L_INT(0).getText();
        if (p.dtype_len().L_INT(1) != null) {
          scale = p.dtype_len().L_INT(1).getText();
        }
      }
      // check data type
      if (!Var.compatibleType(Var.defineType(type), actualValues.get(i).type)) {
        throw new RuntimeException("The supplied type for input value " + i + " of " + name_ + " was " + actualValues.get(i).type + " which is not compatible with the expected type " + type.toUpperCase() + ".");
      }
      Var var = setCallParameter(name, type, len, scale, actualValues.get(i));
      if (trace) {
        trace(actual, "SET PARAM " + name + " = " + var.toString());      
      } 
      if (out != null && a.expr_atom() != null && a.expr_atom().ident() != null &&
          (p.T_OUT() != null || p.T_INOUT() != null)) {
        String actualName = a.expr_atom().ident().getText();
        if (actualName != null) {
          out.put(actualName, var);  
        }         
      }
    }
  }
  
  /**
   * Set parameters for entry-point call (Main procedure defined by -main option)
   */
  void setCallParameters(HplsqlParser.Create_routine_paramsContext ctx) {
    int cnt = ctx.create_routine_param_item().size();
    for (int i = 0; i < cnt; i++) {
      HplsqlParser.Create_routine_param_itemContext p = ctx.create_routine_param_item(i);
      String name = p.ident().getText();
      String type = p.dtype().getText();
      String len = null;
      String scale = null;   
      if (p.dtype_len() != null) {
        len = p.dtype_len().L_INT(0).getText();
        if (p.dtype_len().L_INT(1) != null) {
          scale = p.dtype_len().L_INT(1).getText();
        }
      }
      Var value = exec.findVariable(name);
      Var var = setCallParameter(name, type, len, scale, value);
      if (trace) {
        trace(ctx, "SET PARAM " + name + " = " + var.toString());      
      }      
    }
  }
  
  /**
   * Create a function or procedure parameter and set its value
   */
  Var setCallParameter(String name, String type, String len, String scale, Var value) {
    LOG.trace("SET CALL PARAM"
              + " name=" + name
              + " type=" + type
              + " len=" + len
              + " scale=" + scale
              + " value=" + value);
    Var var = new Var(name, type, len, scale, value);
    exec.addVariable(var);
    return var;
  }

  /**
   * Get call parameter definition by name (if specified) or position
   */
  HplsqlParser.Create_routine_param_itemContext getCallParameter(HplsqlParser.Expr_func_paramsContext actual, 
      HplsqlParser.Create_routine_paramsContext formal, int pos) {
    String named = null;
    int out_pos = pos;
    if (actual != null && actual.func_param(pos) != null && actual.func_param(pos).ident() != null) {
      named = actual.func_param(pos).ident().getText(); 
      int cnt = formal.create_routine_param_item().size();
      for (int i = 0; i < cnt; i++) {
        if (named.equalsIgnoreCase(formal.create_routine_param_item(i).ident().getText())) {
          out_pos = i;
          break;
        }
      }
    }
    return formal.create_routine_param_item(out_pos);
  }  
  
  public Var toVar(Object obj) {
    if (isOutParam(obj)) {
      obj = Array.get(obj, 0);
    }
    if (obj == null) {
      return null;
    }
    if (isString(obj)) {
      return new Var((String)obj);
    } else if (isLangInteger(obj)) {
      return new Var(new Long(((Integer)obj).intValue()));
    } else if (isLangLong(obj)) {
      return new Var((Long)obj);
    } else if (isLangFloat(obj)) {
      return new Var(new Double(((Float)obj).floatValue()));
    } else if (isLangDouble(obj)) {
      return new Var((Double)obj);
    } else if (isBigDecimal(obj)) {
      return new Var((BigDecimal)obj);
    } else if (isDate(obj)) {
      return new Var((Date)obj);
    } else if (isTimestamp(obj)) {
      return new Var((Timestamp)obj, 6); // Trafodion support only
                                         // microsecond resolution
    } else if (isTime(obj)) {
      return new Var(Time.valueOf(((java.sql.Time)obj).toString()));
    } else {
      LOG.error("Unsupported class type: " + obj.getClass());
      throw new IllegalArgumentException();
    }
  }

  public String stringValue(Var var) {
    return var.toString();
  }

  public short shortValue(Var var) {
    return ((Long)var.value).shortValue();
  }
  public int intValue(Var var) {
    return var.intValue();
  }

  public long longValue(Var var) {
    return var.longValue();
  }

  public float floatValue(Var var) {
    return ((Double)var.value).floatValue();
  }

  public double doubleValue(Var var) {
    return ((Double)var.value).doubleValue();
  }

  public boolean isOutParam(Object obj) {
    return obj != null && obj.getClass().isArray();
  }

  public boolean isString(Object obj) {
    return (obj instanceof String);
  }

  public boolean isLangInteger(Object obj) {
    return (obj instanceof Integer);
  }

  public boolean isLangLong(Object obj) {
    return (obj instanceof Long);
  }

  public boolean isLangFloat(Object obj) {
    return (obj instanceof Float);
  }

  public boolean isLangDouble(Object obj) {
    return (obj instanceof Double);
  }

  public boolean isBigDecimal(Object obj) {
    return (obj instanceof BigDecimal);
  }

  public boolean isDate(Object obj) {
    return (obj instanceof Date);
  }

  public boolean isTimestamp(Object obj) {
    return (obj instanceof Timestamp);
  }

  public boolean isTime(Object obj) {
    return (obj instanceof java.sql.Time);
  }

  /**
   * Evaluate actual call parameters
   */
  public ArrayList<Var> getActualCallParameters(HplsqlParser.Expr_func_paramsContext actual) {
    if (actual == null || actual.func_param() == null) {
      return null;
    }
    int cnt = actual.func_param().size();
    ArrayList<Var> values = new ArrayList<Var>(cnt);
    for (int i = 0; i < cnt; i++) {
      values.add(evalPop(actual.func_param(i).expr()));
    }
    return values;
  }
  
  // Check for duplicated param names
  void checkRoutineParams(HplsqlParser.Create_routine_paramsContext ctx) {
    int cnt = ctx.create_routine_param_item().size();
    for (int i = 0; i < cnt-1; i++) {
      HplsqlParser.Create_routine_param_itemContext p = ctx.create_routine_param_item(i);
      String name1 = p.ident().getText();
      for (int j=i+1; j < cnt; j++) {
        HplsqlParser.Create_routine_param_itemContext q = ctx.create_routine_param_item(j);
        String name2 = q.ident().getText();
        if (name1.equals(name2)) {
          throw new RuntimeException("Duplicated parameter name "
                                     + name1 + " at position " + (i+1) + " and " + (j+1));
        }
      }
    }
  }

  /**
   * Add a user-defined function
   */
  public void addUserFunction(HplsqlParser.Create_function_stmtContext ctx) {
    String name = ctx.ident().getText();
    if (trace) {
      trace(ctx, "CREATE FUNCTION " + name);
    }
    checkRoutineParams(ctx.create_routine_params());
    userMap.put(name.toUpperCase(), ctx);
  }
  
  /**
   * Add a user-defined procedure
   */
  public void addUserProcedure(HplsqlParser.Create_procedure_stmtContext ctx) {
    String name = ctx.ident(0).getText();
    if (trace) {
      trace(ctx, "CREATE PROCEDURE " + name);
    }
    checkRoutineParams(ctx.create_routine_params());
    procMap.put(name.toUpperCase(), ctx);
  }

  /**
   * Add a user-defined trigger
   */
  public void addUserbeTrigger(HplsqlParser.Create_trigger_stmtContext ctx) {
    String name = ctx.ident(0).getText();
    if (trace) {
      trace(ctx, "CREATE BEFORE TRIGGER " + name);
    }
    betriggerMap.put(name.toUpperCase(), ctx);
  }

  /**
   * Add a user-defined trigger
   */
  public void addUserafTrigger(HplsqlParser.Create_trigger_stmtContext ctx) {
    String name = ctx.ident(0).getText();
    if (trace) {
      trace(ctx, "CREATE AFTER TRIGGER " + name);
    }
    aftriggerMap.put(name.toUpperCase(), ctx);
  }
  
  public void delUserFunction(String name) {
    userMap.remove(name.toUpperCase());
  }

  public void delUserProcedure(String name) {
    procMap.remove(name.toUpperCase());
  }

  public void delUserTrigger(String name) {
    betriggerMap.remove(name.toUpperCase());
    aftriggerMap.remove(name.toUpperCase());
  }
  
  /**
   * Get the number of parameters in function call
   */
  public int getParamCount(HplsqlParser.Expr_func_paramsContext ctx) {
    if (ctx == null) {
      return 0;
    }
    return ctx.func_param().size();
  }
    
  /**
   * Execute a special function
   */
  public void specExec(HplsqlParser.Expr_spec_funcContext ctx) {
    String name = ctx.start.getText().toUpperCase();
    trace(ctx, "SPECIAL FUNCTION " + name);
    if (trace && ctx.parent.parent instanceof HplsqlParser.Expr_stmtContext) {
      trace(ctx, "FUNC " + name);      
    }
    FuncSpecCommand func = specMap.get(name);    
    if (func != null) {
      func.run(ctx);
    }
    else if(ctx.T_MAX_PART_STRING() != null) {
      execMaxPartString(ctx);
    } else if(ctx.T_MIN_PART_STRING() != null) {
      execMinPartString(ctx);
    } else if(ctx.T_MAX_PART_INT() != null) {
      execMaxPartInt(ctx);
    } else if(ctx.T_MIN_PART_INT() != null) {
      execMinPartInt(ctx);
    } else if(ctx.T_MAX_PART_DATE() != null) {
      execMaxPartDate(ctx);
    } else if(ctx.T_MIN_PART_DATE() != null) {
      execMinPartDate(ctx);
    } else if(ctx.T_PART_LOC() != null) {
      execPartLoc(ctx);
    } else {
      evalNull();
    }
  }
  
  /**
   * Execute a special function in executable SQL statement
   */
  public void specExecSql(HplsqlParser.Expr_spec_funcContext ctx) {
    String name = ctx.start.getText().toUpperCase();
    if (trace && ctx.parent.parent instanceof HplsqlParser.Expr_stmtContext) {
      trace(ctx, "FUNC " + name);      
    }
    FuncSpecCommand func = specSqlMap.get(name);    
    if (func != null) {
      func.run(ctx);
    }
    else {
      exec.stackPush(exec.getFormattedText(ctx));
    }
  }
  
  /**
   * Get the current date
   */
  public void execCurrentDate(HplsqlParser.Expr_spec_funcContext ctx) {
    if(trace) {
      trace(ctx, "CURRENT_DATE");
    }
    SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
    String s = f.format(Calendar.getInstance().getTime());
    exec.stackPush(new Var(Var.Type.DATE, Utils.toDate(s))); 
  }
  
  /**
   * Execute MAX_PART_STRING function
   */
  public void execMaxPartString(HplsqlParser.Expr_spec_funcContext ctx) {
    if(trace) {
      trace(ctx, "MAX_PART_STRING");
    }
    execMinMaxPart(ctx, Var.Type.STRING, true /*max*/);
  }
  
  /**
   * Execute MIN_PART_STRING function
   */
  public void execMinPartString(HplsqlParser.Expr_spec_funcContext ctx) {
    if(trace) {
      trace(ctx, "MIN_PART_STRING");
    }
    execMinMaxPart(ctx, Var.Type.STRING, false /*max*/);
  }

  /**
   * Execute MAX_PART_INT function
   */
  public void execMaxPartInt(HplsqlParser.Expr_spec_funcContext ctx) {
    if(trace) {
      trace(ctx, "MAX_PART_INT");
    }
    execMinMaxPart(ctx, Var.Type.BIGINT, true /*max*/);
  }
  
  /**
   * Execute MIN_PART_INT function
   */
  public void execMinPartInt(HplsqlParser.Expr_spec_funcContext ctx) {
    if(trace) {
      trace(ctx, "MIN_PART_INT");
    }
    execMinMaxPart(ctx, Var.Type.BIGINT, false /*max*/);
  }

  /**
   * Execute MAX_PART_DATE function
   */
  public void execMaxPartDate(HplsqlParser.Expr_spec_funcContext ctx) {
    if(trace) {
      trace(ctx, "MAX_PART_DATE");
    }
    execMinMaxPart(ctx, Var.Type.DATE, true /*max*/);
  }
  
  /**
   * Execute MIN_PART_DATE function
   */
  public void execMinPartDate(HplsqlParser.Expr_spec_funcContext ctx) {
    if(trace) {
      trace(ctx, "MIN_PART_DATE");
    }
    execMinMaxPart(ctx, Var.Type.DATE, false /*max*/);
  }
  
  /**
   * Execute MIN or MAX partition function
   */
  public void execMinMaxPart(HplsqlParser.Expr_spec_funcContext ctx, Var.Type type, boolean max) {
    String tabname = evalPop(ctx.expr(0)).toString();
    String sql = "SHOW PARTITIONS " + tabname;    
    String colname = null;    
    int colnum = -1;
    int exprnum = ctx.expr().size();    
    // Column name 
    if (ctx.expr(1) != null) {
      colname = evalPop(ctx.expr(1)).toString();
    } else {
      colnum = 0;
    }
    // Partition filter
    if (exprnum >= 4) {
      sql += " PARTITION (";
      int i = 2;
      while (i + 1 < exprnum) {
        String fcol = evalPop(ctx.expr(i)).toString();
        String fval = evalPop(ctx.expr(i+1)).toSqlString();
        if (i > 2) {
          sql += ", ";
        }
        sql += fcol + "=" + fval;        
        i += 2;
      }
      sql += ")";
    }
    if (trace) {
      trace(ctx, "Query: " + sql);
    }
    if (exec.getOffline()) {
      evalNull();
      return;
    }
    Query query = exec.executeQuery(ctx, sql, exec.conf.defaultConnection);
    if (query.error()) {
      evalNullClose(query, exec.conf.defaultConnection);
      return;
    }
    ResultSet rs = query.getResultSet();
    try {
      String resultString = null;
      Long resultInt = null;
      Date resultDate = null;      
      while (rs.next()) {
        String[] parts = rs.getString(1).split("/");
        // Find partition column by name
        if (colnum == -1) {
          for (int i = 0; i < parts.length; i++) {
            String[] name = parts[i].split("=");
            if (name[0].equalsIgnoreCase(colname)) {
              colnum = i;
              break;
            }
          }
          // No partition column with the specified name exists
          if (colnum == -1) {
            evalNullClose(query, exec.conf.defaultConnection);
            return;
          }
        }
        String[] pair = parts[colnum].split("=");
        if (type == Var.Type.STRING) {
          resultString = Utils.minMaxString(resultString, pair[1], max);          
        } 
        else if (type == Var.Type.BIGINT) {
          resultInt = Utils.minMaxInt(resultInt, pair[1], max);          
        } 
        else if (type == Var.Type.DATE) {
          resultDate = Utils.minMaxDate(resultDate, pair[1], max);
        }
      }
      if (resultString != null) {
        evalString(resultString);
      } 
      else if (resultInt != null) {
        evalInt(resultInt);
      } 
      else if (resultDate != null) {
        evalDate(resultDate);
      } 
      else {
        evalNull();
      }
    } catch (SQLException e) {}  
    exec.closeQuery(query, exec.conf.defaultConnection);
  }
  
  /**
   * Execute PART_LOC function
   */
  public void execPartLoc(HplsqlParser.Expr_spec_funcContext ctx) {
    String tabname = evalPop(ctx.expr(0)).toString();
    String sql = "DESCRIBE EXTENDED " + tabname;    
    int exprnum = ctx.expr().size();   
    boolean hostname = false;
    // Partition filter
    if (exprnum > 1) {
      sql += " PARTITION (";
      int i = 1;
      while (i + 1 < exprnum) {
        String col = evalPop(ctx.expr(i)).toString();
        String val = evalPop(ctx.expr(i+1)).toSqlString();
        if (i > 2) {
          sql += ", ";
        }
        sql += col + "=" + val;        
        i += 2;
      }
      sql += ")";
    }
    // With host name
    if (exprnum % 2 == 0 && evalPop(ctx.expr(exprnum - 1)).intValue() == 1) {
      hostname = true;
    }
    if (trace) {
      trace(ctx, "Query: " + sql);
    }
    if (exec.getOffline()) {
      evalNull();
      return;
    }
    Query query = exec.executeQuery(ctx, sql, exec.conf.defaultConnection);
    if (query.error()) {
      evalNullClose(query, exec.conf.defaultConnection);
      return;
    }
    String result = null;
    ResultSet rs = query.getResultSet();
    try {
      while (rs.next()) {
        if (rs.getString(1).startsWith("Detailed Partition Information")) {
          Matcher m = Pattern.compile(".*, location:(.*?),.*").matcher(rs.getString(2));
          if (m.find()) {
            result = m.group(1);
          }    
        }
      }
    } catch (SQLException e) {}  
    if (result != null) {
      // Remove the host name
      if (!hostname) {
        Matcher m = Pattern.compile(".*://.*?(/.*)").matcher(result); 
        if (m.find()) {
          result = m.group(1);
        }
      }
      evalString(result);
    }    
    else {
      evalNull();
    }
    exec.closeQuery(query, exec.conf.defaultConnection);
  }
  
  /**
   * Evaluate the expression and push the value to the stack
   */
  void eval(ParserRuleContext ctx) {
    exec.visit(ctx);
  }

  /**
   * Evaluate the expression to the specified variable
   */
  void evalVar(Var var) {
    exec.stackPush(var); 
  }

  /**
   * Evaluate the expression to NULL
   */
  void evalNull() {
    exec.stackPush(Var.Null); 
  }
  
  /**
   * Evaluate the expression to specified String value
   */
  void evalString(String string) {
    exec.stackPush(new Var(string)); 
  }
  
  void evalString(StringBuilder string) {
    evalString(string.toString()); 
  }

  /**
   * Evaluate the expression to specified Int value
   */
  void evalInt(Long i) {
    exec.stackPush(new Var(i)); 
  }
  
  void evalInt(int i) {
    evalInt(new Long(i));
  }
  
  /**
   * Evaluate the expression to specified Date value
   */
  void evalDate(Date date) {
    exec.stackPush(new Var(Var.Type.DATE, date)); 
  }
  
  /**
   * Evaluate the expression to NULL and close the query
   */
  void evalNullClose(Query query, String conn) {
    exec.stackPush(Var.Null); 
    exec.closeQuery(query, conn);
    if(trace) {
      query.printStackTrace();
    }
  }
  
  /**
   * Evaluate the expression and pop value from the stack
   */
  Var evalPop(ParserRuleContext ctx) {
    exec.visit(ctx);
    return exec.stackPop();  
  }
  
  Var evalPop(ParserRuleContext ctx, int value) {
    if (ctx != null) {
      return evalPop(ctx);
    }
    return new Var(new Long(value));
  }
  
  /**
   * Execute rules
   */
  Integer visit(ParserRuleContext ctx) {
    return exec.visit(ctx);  
  } 
 
  /**
   * Execute children rules
   */
  Integer visitChildren(ParserRuleContext ctx) {
    return exec.visitChildren(ctx);  
  }  
  
  /**
   * Trace information
   */
  public void trace(ParserRuleContext ctx, String message) {
    if (trace) {
      LOG.trace(ctx, message);
    }
  }
  
  public void trace(String message) {
    trace(null, message);
  }
  
  public void info(ParserRuleContext ctx, String message) {
    LOG.info(ctx, message);
  }
}
