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

import org.trafodion.sql.udr.spsql.*;

public class FunctionString extends Function {
  private static final Logger LOG = Logger.getLogger(FunctionString.class);
  public FunctionString(Exec e) {
    super(e);
  }

  /** 
   * Register functions
   */
  @Override
  public void register(Function f) {
    f.map.put("CONCAT", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { concat(ctx); }});
    f.map.put("CHAR", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { char_(ctx); }});
    f.map.put("INSTR", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { instr(ctx); }});
    f.map.put("LEN", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { len(ctx); }});
    f.map.put("LENGTH", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { length(ctx); }});
    f.map.put("LOWER", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { lower(ctx); }});
    f.map.put("REPLACE", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { replace(ctx); }}); 
    f.map.put("SUBSTR", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { substr(ctx); }});    
    f.map.put("SUBSTRING", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { substr(ctx); }});
    f.map.put("TO_CHAR", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { toChar(ctx); }});
    f.map.put("UPPER", new FuncCommand() { public void run(HplsqlParser.Expr_func_paramsContext ctx) { upper(ctx); }});
    
    f.specMap.put("SUBSTR", new FuncSpecCommand() { public void run(HplsqlParser.Expr_spec_funcContext ctx) { substring(ctx); }});
    f.specMap.put("SUBSTRING", new FuncSpecCommand() { public void run(HplsqlParser.Expr_spec_funcContext ctx) { substring(ctx); }});
    f.specMap.put("TRIM", new FuncSpecCommand() { public void run(HplsqlParser.Expr_spec_funcContext ctx) { trim(ctx); }});

    f.specSqlMap.put("POSITION", new FuncSpecCommand() { public void run(HplsqlParser.Expr_spec_funcContext ctx) { positionSql(ctx); }});
    f.specSqlMap.put("SUBSTR", new FuncSpecCommand() { public void run(HplsqlParser.Expr_spec_funcContext ctx) { substringSql(ctx); }});
    f.specSqlMap.put("SUBSTRING", new FuncSpecCommand() { public void run(HplsqlParser.Expr_spec_funcContext ctx) { substringSql(ctx); }});
    f.specSqlMap.put("TRIM", new FuncSpecCommand() { public void run(HplsqlParser.Expr_spec_funcContext ctx) { trimSql(ctx); }});
  }
  
  /**
   * CONCAT function
   */
  void concat(HplsqlParser.Expr_func_paramsContext ctx) {
    StringBuilder val = new StringBuilder();
    int cnt = getParamCount(ctx);
    boolean nulls = true;
    for (int i = 0; i < cnt; i++) {
      Var c = evalPop(ctx.func_param(i).expr());
      if (!c.isNull(exec.conf.emptyIsNull)) {
        val.append(c.toString());
        nulls = false;
      }
    }
    if (nulls) {
      evalNull();
    }
    else {
      evalString(val);
    }
  }
  
  /**
   * CHAR function
   */
  void char_(HplsqlParser.Expr_func_paramsContext ctx) {
    int cnt = getParamCount(ctx);
    if (cnt != 1) {
      evalNull();
      return;
    }
    String str = evalPop(ctx.func_param(0).expr()).toString(); 
    if (str == null) {
      evalNull();
      return;
    }
    evalString(str);
  }
  
  /**
   * INSTR function
   */
  void instr(HplsqlParser.Expr_func_paramsContext ctx) {
    int cnt = getParamCount(ctx);
    if (cnt < 2) {
      evalNull();
      return;
    }
    String str = evalPop(ctx.func_param(0).expr()).toString();
    if (str == null) {
      evalNull();
      return;
    }
    else if(str.isEmpty()) {
      evalInt(new Long(0));
      return;
    }
    String substr = evalPop(ctx.func_param(1).expr()).toString();
    if (substr == null) {
      evalNull();
      return;
    }
    int pos = 1;
    int occur = 1;
    int idx = 0;
    if (cnt >= 3) {
      Var v = evalPop(ctx.func_param(2).expr());
      if (v.isNull(exec.conf.emptyIsNull)) {
        evalNull();
        return;
      }
      pos = v.intValue();
      if (pos == 0) {
        pos = 1;
      }
    }
    if (cnt >= 4) {
      Var v = evalPop(ctx.func_param(3).expr());
      if (v.isNull(exec.conf.emptyIsNull)) {
        evalNull();
        return;
      }
      occur = v.intValue();
      if (occur < 0) {
        occur = 1;
      }
    }
    for (int i = occur; i > 0; i--) {
      if (pos > 0) {
        idx = str.indexOf(substr, pos - 1);
      }
      else {
        str = str.substring(0, str.length() - pos*(-1));
        idx = str.lastIndexOf(substr);
      }
      if (idx == -1) {
        idx = 0;
        break;
      }
      else {
        idx++;
      }
      if (i > 1) {
        if (pos > 0) {
          pos = idx + 1;
        }
        else {
          pos = (str.length() - idx + 1) * (-1);
        }
      }
    }
    evalInt(new Long(idx));
  }
  
  /**
   * LEN function (excluding trailing spaces)
   */
  void len(HplsqlParser.Expr_func_paramsContext ctx) {
    if (ctx.func_param().size() != 1) {
      evalNull();
      return;
    }
    String str = evalPop(ctx.func_param(0).expr()).toString();
    if (str == null) {
      evalNull();
      return;
    }
    int len = str.trim().length();
    evalInt(new Long(len));
  }
  
  /**
   * LENGTH function
   */
  void length(HplsqlParser.Expr_func_paramsContext ctx) {
    if (ctx.func_param().size() != 1) {
      evalNull();
      return;
    }
    String str = evalPop(ctx.func_param(0).expr()).toString();
    if (str == null) {
      evalNull();
      return;
    }
    int len = str.length();
    evalInt(new Long(len));
  }
  
  /**
   * LOWER function
   */
  void lower(HplsqlParser.Expr_func_paramsContext ctx) {
    if (ctx.func_param().size() != 1) {
      evalNull();
      return;
    }
    String str = evalPop(ctx.func_param(0).expr()).toString();
    if (str == null) {
      evalNull();
      return;
    }
    evalString(str.toLowerCase());
  }
  
  /**
   * POSITION function in SQL statement
   */
  void positionSql(HplsqlParser.Expr_spec_funcContext ctx) {
    StringBuilder sql = new StringBuilder();
    sql.append("POSITION(");
    sql.append(evalPop(ctx.expr(0)).toString());
    sql.append(" IN ");
    sql.append(evalPop(ctx.expr(1)).toString());
    sql.append(")");
    evalString(sql.toString());
  }

  /**
   * REPLACE function
   */
  void replace(HplsqlParser.Expr_func_paramsContext ctx) {
    int cnt = getParamCount(ctx);
    if (cnt < 3) {
      evalNull();
      return;
    }
    String str = evalPop(ctx.func_param(0).expr()).toString(); 
    if (str == null) {
      evalNull();
      return;
    }
    String what = evalPop(ctx.func_param(1).expr()).toString();
    if (what == null) {
      evalString(str);
      return;
    }
    String with = evalPop(ctx.func_param(2).expr()).toString();
    if (with == null) {
      with = "";
    }
    evalString(str.replaceAll(what, with));
  }
  
  /**
   * SUBSTR and SUBSTRING function
   */
  void substr(HplsqlParser.Expr_func_paramsContext ctx) {
    int cnt = getParamCount(ctx);
    if (cnt < 2) {
      evalNull();
      return;
    }
    String str = evalPop(ctx.func_param(0).expr()).toString(); 
    if (str == null) {
      evalNull();
      return;
    }
    Var v = evalPop(ctx.func_param(1).expr());
    if (v.isNull(exec.conf.emptyIsNull)) {
      evalNull();
      return;
    }
    int start = v.intValue();
    int len = str.length();
    if (cnt > 2) {
      v = evalPop(ctx.func_param(2).expr());
      if (v.isNull(exec.conf.emptyIsNull)) {
        evalNull();
        return;
      }
      len = v.intValue();
    }
    substr(str, start, len);
  }
  
  void substr(String str, int start, int len) {
    String s = Utils.substr(str, start, len);
    if (s == null) {
      evalNull();
      return;
    }
    evalString(s);
  }
  
  /**
   * SUBSTRING FROM FOR function
   */
  void substring(HplsqlParser.Expr_spec_funcContext ctx) {
    String str = evalPop(ctx.expr(0)).toString(); 
    if (str == null) {
      evalNull();
      return;
    }
    int start = evalPop(ctx.expr(1)).intValue();
    int len = str.length();
    if (start == 0) {
      start = 1; 
    }
    if (ctx.T_FOR() != null) {
      Var v = evalPop(ctx.expr(2));
      if (v.isNull(exec.conf.emptyIsNull)) {
        evalNull();
        return;
      }
      len = v.intValue();
    }
    substr(str, start, len);
  }
  
  /**
   * SUBSTRING function in SQL statement
   */
  void substringSql(HplsqlParser.Expr_spec_funcContext ctx) {
    StringBuilder sql = new StringBuilder();
    sql.append("SUBSTRING(");
    sql.append(evalPop(ctx.expr(0)).toString());
    sql.append(" FROM ");
    sql.append(evalPop(ctx.expr(1)).toString());
    if (ctx.expr().size() > 2 ) {
      sql.append(" FOR ");
      sql.append(evalPop(ctx.expr(2)).toString());
    }
    sql.append(")");
    evalString(sql.toString());
  }

  /**
   * TRIM function in SQL statement
   */
  void trimSql(HplsqlParser.Expr_spec_funcContext ctx) {
    StringBuilder sql = new StringBuilder();
    sql.append("TRIM(");
    sql.append(evalPop(ctx.expr(0)).toString());
    sql.append(")");
    evalString(sql.toString());
  }

  /**
   * TRIM function
   */
  void trim(HplsqlParser.Expr_spec_funcContext ctx) {
    int cnt = ctx.expr().size();
    if (cnt != 1) {
      evalNull();
      return;
    }
    String str = evalPop(ctx.expr(0)).toString(); 
    if (str == null) {
      evalNull();
      return;
    }
    evalString(str.trim());
  }
  
  /**
   * TO_CHAR function
   */
  void toChar(HplsqlParser.Expr_func_paramsContext ctx) {
    int cnt = getParamCount(ctx);
    if (cnt != 1) {
      evalNull();
      return;
    }
    String str = evalPop(ctx.func_param(0).expr()).toString(); 
    if (str == null) {
      evalNull();
      return;
    }
    evalString(str);
  }
  
  /**
   * UPPER function
   */
  void upper(HplsqlParser.Expr_func_paramsContext ctx) {
    if (ctx.func_param().size() != 1) {
      evalNull();
      return;
    }
    String str = evalPop(ctx.func_param(0).expr()).toString(); 
    if (str == null) {
      evalNull();
      return;
    }
    evalString(str.toUpperCase());
  }
}
