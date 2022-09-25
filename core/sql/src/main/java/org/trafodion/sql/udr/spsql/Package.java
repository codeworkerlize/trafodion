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
import java.util.Map;
import java.util.List;
import java.lang.reflect.Array;

import org.antlr.v4.runtime.ParserRuleContext;
import org.trafodion.sql.udr.spsql.HplsqlParser.Package_spec_itemContext;
import org.trafodion.sql.udr.spsql.HplsqlParser.Package_body_itemContext;
import org.trafodion.sql.udr.spsql.HplsqlParser.Create_function_stmtContext;
import org.trafodion.sql.udr.spsql.HplsqlParser.Create_procedure_stmtContext;
import org.trafodion.sql.udr.spsql.functions.Function;

/**
 * Program package
 */
public class Package {
  
  private static final Logger LOG = Logger.getLogger(Package.class);

  String name;
  ArrayList<Var> vars = new ArrayList<Var>();
  ArrayList<String> publicVars = new ArrayList<String>();
  ArrayList<String> publicFuncs = new ArrayList<String>();
  ArrayList<String> publicProcs = new ArrayList<String>();
  
  HashMap<String, Create_function_stmtContext> func = new HashMap<String, Create_function_stmtContext>();
  HashMap<String, Create_procedure_stmtContext> proc = new HashMap<String, Create_procedure_stmtContext>();
    
  boolean allMembersPublic = false;
    
  Exec exec;
  Function function;
  boolean trace = false;
  
  Package(String name, Exec exec) {
    this.name = name;
    this.exec = exec;
    this.function = new Function(exec);
    this.trace = exec.getTrace();
  }
  
  public Create_procedure_stmtContext getProc(String name) {
    return proc.get(name.toUpperCase());
  }

  public Create_function_stmtContext getFunc(String name) {
    return func.get(name.toUpperCase());
  }

  /**
   * Add a local variable
   */
  void addVariable(Var var) {
    // check same name variable
    for (Var v : vars) {
      if (v.name.equals(var.name)) {
        throw new RuntimeException("at most one declaration for '" + v.name + "' is permitted.");
      }
    }
    vars.add(var);
  }
  
  /**
   * Find the variable by name
   */
  Var findVariable(String name) {
    LOG.trace("FIND VARIABLE IN PACKAGE " + this.name + ": " + name);
    for (Var var : vars) {
      if (name.equalsIgnoreCase(var.getName())) {
        return var;
      }
    }
    LOG.trace("Variable not found in package " + this.name + ": " + name);
    return null;
  }
  
  /**
   * Create the package specification
   */
  void createSpecification(HplsqlParser.Create_package_stmtContext ctx) {
    int cnt = ctx.package_spec().package_spec_item().size();
    for (int i = 0; i < cnt; i++) {
      Package_spec_itemContext c = ctx.package_spec().package_spec_item(i);
      if (c.declare_stmt_item() != null) {
	exec.saveSchema(this.name);
        visit(c);
	exec.restoreSchema();
      }
      else if (c.T_FUNCTION() != null) {
        publicFuncs.add(c.ident().getText().toUpperCase());
      }
      else if (c.T_PROC() != null || c.T_PROCEDURE() != null) {
        publicProcs.add(c.ident().getText().toUpperCase());
      }
    }
  } 
  
  /**
   * Create the package body
   */
  void createBody(HplsqlParser.Create_package_body_stmtContext ctx) {
    int cnt = ctx.package_body().package_body_item().size();
    for (int i = 0; i < cnt; i++) {
      Package_body_itemContext c = ctx.package_body().package_body_item(i);
      if (c.declare_stmt_item() != null) {
	exec.saveSchema(this.name);
        visit(c);
	exec.restoreSchema();
      }
      else if (c.create_function_stmt() != null) {
        addServerFunction(c.create_function_stmt().ident().getText().toUpperCase(), c.create_function_stmt());
        if (exec.signalPeek() != null) {
          return;
        }
        func.put(c.create_function_stmt().ident().getText().toUpperCase(), c.create_function_stmt());
      }
      else if (c.create_procedure_stmt() != null) {
        addServerProcedure(c.create_procedure_stmt().ident(0).getText().toUpperCase(), c.create_procedure_stmt());
        if (exec.signalPeek() != null) {
          return;
        }
        proc.put(c.create_procedure_stmt().ident(0).getText().toUpperCase(), c.create_procedure_stmt());
      }
    }    
  } 
  
  private String funcReturnToSql(HplsqlParser.Create_function_returnContext ctx)
  {
    StringBuilder sql = new StringBuilder();
    sql.append("(r ");
    sql.append(dtypeToSql(ctx.dtype(), ctx.dtype_len(), ctx.dtype_attr()));
    sql.append(")");
    return sql.toString();
  }

  public void addServerFunction(String name,
                                HplsqlParser.Create_function_stmtContext ctx) {
    if (exec.loading) {
      return;
    }
    ArrayList<String> qualified = exec.meta.splitIdentifierToTwoParts(this.name);
    String fullName = qualified.get(0) + ".\"" + qualified.get(1) + "." + name + "\"";
    LOG.trace("CREATE FUNCTION ON SERVER: " + fullName);
    String sql
      = "CREATE FUNCTION "
      + fullName
      + "(" + paramsToSql(ctx) + ")"
      + " RETURNS "
      + funcReturnToSql(ctx.create_function_return())
      + " AS 'NULL'";
    Query query = exec.executeSql(ctx, sql, "default");
    if (query.error()) {
      exec.signal(query);
    }
  }

  private boolean hasUnsupportedType(HplsqlParser.Create_routine_paramsContext params) {
    if (params != null) {
      int paramCount = params.create_routine_param_item().size();
      for (int i = 0; i < paramCount; i++) {
        HplsqlParser.DtypeContext type = params.create_routine_param_item(i).dtype();
        if (type.T_RESULT_SET_LOCATOR() != null) {
	  LOG.warn("RESULT SET LOCATOR used as parameter type");
	  return true;
	} else if (type.T_SYS_REFCURSOR() != null) {
	  LOG.warn("SYS_REFCURSOR used as parameter type");
	  return true;
	} else if (type.T_TYPE() != null || type.T_ROWTYPE() != null) {
	  LOG.warn("Derived type used as parameter type");
	} else if (type.ident() != null) {
	  throw new RuntimeException("unsupported support parameter type:" + type.getText().toUpperCase());
        }
      }
    }
    return false;
  }

  private String dtypeToSql(HplsqlParser.DtypeContext typeCtx,
                            HplsqlParser.Dtype_lenContext lenCtx,
                            List<HplsqlParser.Dtype_attrContext> attrCtxList) {
    StringBuilder sql = new StringBuilder();
    String type;
    String len;
    String attr = "";
    if (typeCtx.T_BIGINT() != null) {
      type = "LARGEINT";
    } else if (typeCtx.T_DOUBLE() != null) {
      type = "DOUBLE PRECISION";
    } else if (typeCtx.T_BINARY_DOUBLE() != null) {
      type = "DOUBLE PRECISION";
    } else if (typeCtx.T_BINARY_FLOAT() != null) {
      type = "FLOAT";
    } else if (typeCtx.T_BINARY_INTEGER() != null) {
      type = "INTEGER";
    } else if (typeCtx.T_SMALLINT() != null) {
      type = "INT";
    } else if (typeCtx.T_SIMPLE_DOUBLE() != null) {
      type = "DOUBLE PRECISION";
    } else if (typeCtx.T_SIMPLE_FLOAT() != null) {
      type = "FLOAT";
    } else if (typeCtx.T_SIMPLE_INTEGER() != null) {
      type = "INTEGER";
    } else if (typeCtx.T_BIT() != null) {
      type = "INT";
    } else if (typeCtx.T_INT2() != null) {
      type = "INT";
    } else if (typeCtx.T_INT4() != null) {
      type = "INT";
    } else if (typeCtx.T_INT8() != null) {
      type = "LARGEINT";
    } else if (typeCtx.T_PLS_INTEGER() != null) {
      type = "INTEGER";
    } else if (typeCtx.T_NUMBER() != null) {
      type = "NUMERIC";
    } else if (typeCtx.T_NVARCHAR() != null) {
      type = "NCHAR VARYING";
    } else if (typeCtx.T_DATETIME() != null) {
      type = "TIMESTAMP";
    } else if (typeCtx.T_SMALLDATETIME() != null) {
      type = "TIMESTAMP";
    } else if (typeCtx.T_STRING() != null) {
      type = "VARCHAR";
    } else if (typeCtx.T_TINYINT() != null) {
      type = "INT";
    } else if (typeCtx.T_VARCHAR2() != null) {
      type = "VARCHAR";
    } else if (typeCtx.T_XML() != null) {
      type = "VARCHAR";
    } else {
      type = typeCtx.getText().toUpperCase();
    }

    if (lenCtx != null) {
      len = lenCtx.getText();
    } else if (type.equals("VARCHAR") ||
               type.equals("NCHAR VARYING") ||
               type.equals("NVARCHAR")) {
      len = "(4000)";
    } else {
      len = "";
    }

    if (attrCtxList != null) {
      for (HplsqlParser.Dtype_attrContext attrCtx : attrCtxList) {
        if (attrCtx.T_CHARACTER() != null &&
            attrCtx.T_SET() != null) {
          attr = " CHARACTER SET " + attrCtx.ident().getText();
        }
      }
    }

    sql.append(type);
    sql.append(len);
    sql.append(attr);
    return sql.toString();
  }

  private String paramToSql(HplsqlParser.Create_routine_param_itemContext param) {
    StringBuilder sql = new StringBuilder();
    String dir = "IN ";
    if (param.T_INOUT() != null ||
        (param.T_OUT() != null && param.T_IN() != null)) {
      dir = "INOUT ";
    } else if (param.T_OUT() != null) {
      dir = "OUT ";
    }
    sql.append(dir);
    sql.append(param.ident().getText());
    sql.append(" ");
    String dtype = dtypeToSql(param.dtype(), param.dtype_len(), param.dtype_attr());
    sql.append(dtype);
    return sql.toString();
  }

  private String paramsToSql(HplsqlParser.Create_routine_paramsContext params) {
    if (params == null) {
      return "";
    }
    int paramCount = params.create_routine_param_item().size();
    if (paramCount == 0) {
      return "";
    }
    StringBuilder sql = new StringBuilder();
    sql.append(paramToSql(params.create_routine_param_item(0)));
    for (int i = 1; i < paramCount; i++) {
      sql.append(",");
      sql.append(paramToSql(params.create_routine_param_item(i)));
    }
    return sql.toString();
  }

  private String paramsToSql(HplsqlParser.Create_procedure_stmtContext ctx) {
    return paramsToSql(ctx.create_routine_params());
  }

  private String paramsToSql(HplsqlParser.Create_function_stmtContext ctx) {
    return paramsToSql(ctx.create_routine_params());
  }

  private String transactionOptionToSql(HplsqlParser.Create_procedure_stmtContext ctx) {
    if (ctx.create_routine_options() != null) {
      int cnt = ctx.create_routine_options().create_routine_option().size();
      for (int i = 0; i < cnt; i++) {
        HplsqlParser.Create_routine_optionContext optCtx
          = ctx.create_routine_options().create_routine_option(i);
        if (optCtx.T_TRANSACTION() != null && optCtx.T_REQUIRED() != null) {
          if (optCtx.T_NO() != null) {
            return " NO TRANSACTION REQUIRED";
          }
          return " TRANSACTION REQUIRED";
        }
      }
    }
    return "";
  }

  public void addServerProcedure(String name,
                                 HplsqlParser.Create_procedure_stmtContext ctx) {
    if (exec.loading) {
      return;
    }
    if (hasUnsupportedType(ctx.create_routine_params())) {
      LOG.warn("There are unsupported parameter type in procedure "
	       + name.toUpperCase()
	       + " the procedure can only be used within SPSQL ");
      return;
    }
    ArrayList<String> qualified = exec.meta.splitIdentifierToTwoParts(this.name);
    String fullName = qualified.get(0) + ".\"" + qualified.get(1) + "." + name + "\"";
    LOG.trace("CREATE PROCEDURE ON SERVER: " + fullName);
    String sql
      = "CREATE PROCEDURE "
      + fullName
      + "(" + paramsToSql(ctx) + ")"
      + " DYNAMIC RESULT SETS 255"
      + " EXTERNAL NAME 'org.trafodion.sql.udr.spsql.SPSQL.callSPSQL'"
      + " LIBRARY UDR_LIBRARY"
      + transactionOptionToSql(ctx)
      + " LANGUAGE JAVA";
    Query query = exec.executeSql(ctx, sql, "default");
    if (query.error()) {
      exec.signal(query);
    }
  }

  /**
   * Execute function from UDR server
   */
  public boolean execFunc(String name, Object[] args) {
    Create_function_stmtContext f = func.get(name.toUpperCase());
    if (f == null) {
      return false;
    }
    LOG.trace("EXEC PACKAGE FUNCTION " + this.name + "." + name);
    HashMap<Integer, Var> out = new HashMap<Integer, Var>();
    exec.enterScope(Scope.Type.ROUTINE, this);
    exec.saveSchema(this.name);
    try {
      function.setCallParameters(args, f.create_routine_params(), out);
      visit(f.single_block_stmt());
      function.setReturnValue(name, args, f.create_function_return());
    } finally {
      exec.restoreSchema();
      exec.leaveScope();
    }
    return true;
  }
  
  /**
   * Execute function
   */
  public boolean execFunc(String name, HplsqlParser.Expr_func_paramsContext ctx) {
    Create_function_stmtContext f = func.get(name.toUpperCase());
    if (f == null) {
      return execProc(name, ctx, false /*trace error if not exists*/);
    }
    if (trace) {
      trace(ctx, "EXEC PACKAGE FUNCTION " + this.name + "." + name);
    }
    ArrayList<Var> actualParams = function.getActualCallParameters(ctx);
    exec.enterScope(Scope.Type.ROUTINE, this);
    exec.saveSchema(this.name);
    try {
      function.setCallParameters(ctx, actualParams, f.create_routine_params(), null, name);
      visit(f.single_block_stmt());
    } finally {
      exec.restoreSchema();
      exec.leaveScope();
    }
    return true;
  }
  
  /**
   * Execute procedure
   */
  public boolean execProc(String name, HplsqlParser.Expr_func_paramsContext ctx, boolean traceNotExists) {
    Create_procedure_stmtContext p = proc.get(name.toUpperCase());
    if (p == null) {
      if (trace && traceNotExists) {
        trace(ctx, "Package procedure not found: " + this.name + "." + name);
      }
      return false;        
    }
    if (trace) {
      trace(ctx, "EXEC PACKAGE PROCEDURE " + this.name + "." + name);
    }
    ArrayList<Var> actualParams = function.getActualCallParameters(ctx);
    HashMap<String, Var> out = new HashMap<String, Var>();
    exec.enterScope(Scope.Type.ROUTINE, this);
    exec.saveSchema(this.name);
    exec.callStackPush(this.name + "." + name);
    try {
      if (p.declare_block_inplace() != null) {
        visit(p.declare_block_inplace());
      }
      if (p.create_routine_params() != null) {
        function.setCallParameters(ctx, actualParams, p.create_routine_params(), out, name);
      }
      visit(p.proc_block());
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
   * Execute procedure
   */
  public boolean execProc(String name, Object[] args) {
    Create_procedure_stmtContext p = proc.get(name.toUpperCase());
    if (p == null) {
      LOG.trace("Package procedure not found: " + this.name + "." + name);
      return false;        
    }
    LOG.trace("EXEC PACKAGE PROCEDURE " + this.name + "." + name);
    HashMap<Integer, Var> out = new HashMap<Integer, Var>();
    exec.enterScope(Scope.Type.ROUTINE, this);
    exec.saveSchema(this.name);
    exec.callStackPush(this.name + "." + name);
    try {
      if (p.declare_block_inplace() != null) {
        visit(p.declare_block_inplace());
      }
      if (p.create_routine_params() != null) {
        function.setCallParameters(args, p.create_routine_params(), out);
      }
      visit(p.proc_block());
    } finally {
      exec.callStackPop();
      exec.restoreSchema();
      exec.leaveScope();
    }
    function.setOutParameters(this.name + "." + name,
			      p.create_routine_params(), args, out);
    return true;
  }

  /**
   * Set whether all members are public (when package specification is missed) or not 
   */
  void setAllMembersPublic(boolean value) {
    allMembersPublic = value;
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
}
