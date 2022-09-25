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
import java.util.List;
import java.util.Stack;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.Interval;

public class Select {

  private static final Logger LOG = Logger.getLogger(Select.class);

  Exec exec = null;
  Stack<Var> stack = null;
  Conf conf;
  
  boolean trace = false; 
  
  Select(Exec e) {
    exec = e;  
    stack = exec.getStack();
    conf = exec.getConf();
    trace = exec.getTrace();
  }
   
  /**
   * Executing or building SELECT statement
   */
  public Integer select(HplsqlParser.Select_stmtContext ctx) { 
    if (ctx.parent instanceof HplsqlParser.StmtContext) {
      exec.stmtConnList.clear();
      trace(ctx, "SELECT");
    }
    boolean oldBuildSql = exec.buildSql; 
    exec.buildSql = true;
    StringBuilder sql = new StringBuilder();
    if (ctx.cte_select_stmt() != null) {
      sql.append(evalPop(ctx.cte_select_stmt()).toString());
      sql.append("\n");
    }
    sql.append(evalPop(ctx.fullselect_stmt()).toString());
    exec.buildSql = oldBuildSql;
    if (!(ctx.parent instanceof HplsqlParser.StmtContext)) {           // No need to execute at this stage
      exec.stackPush(sql);
      return 0;  
    }    
    if (trace && ctx.parent instanceof HplsqlParser.StmtContext) {
      trace(ctx, sql.toString());
    }
    if (exec.getOffline()) {
      trace(ctx, "Not executed - offline mode set");
      return 0;
    }
    String conn = exec.getStatementConnection();
    Query query = exec.executeQuery(ctx, sql.toString(), conn);
    if (query.error()) { 
      exec.signal(query);
      exec.closeQuery(query, conn);
      return 1;
    }
    trace(ctx, "SELECT completed successfully");
    exec.setSqlSuccess();
    try {
      ResultSet rs = query.getResultSet();
      ResultSetMetaData rm = null;
      if (rs != null) {
        rm = rs.getMetaData();
      }
      int into_cnt = getIntoCount(ctx);
      if (into_cnt > 0) {
        trace(ctx, "SELECT INTO statement executed");
        if (rs.next()) {
          if (!rs.isLast()) {
            exec.signal(Signal.Type.SQLEXCEPTION, "more than one row was returned", new SQLException("more than one row was returned"));
            return 1;
          }
          for (int i = 1; i <= into_cnt; i++) {
            String into_name = getIntoVariable(ctx, i - 1);
            Var var = exec.findVariable(into_name);
            if (var != null) {
              if (var.type != Var.Type.ROW) {
                var.setValue(rs, rm, i);
              }
              else {
                var.setValues(rs, rm);
              }
              if (trace) {
                trace(ctx, var, rs, rm, i);
              }
            } 
            else {
              throw new SQLException("Variable not found: " + into_name);
            }
          }
          exec.incRowCount();
          exec.setSqlSuccess();
        }
        else {
          exec.setSqlCode(100);
          exec.signal(Signal.Type.NOTFOUND, "select into statement not found data.");
        }
      }
      // Return result sets for standalone SELECT statement
      else if (ctx.parent instanceof HplsqlParser.StmtContext) {
        int cols = rm.getColumnCount();
        if (trace) {
          trace(ctx, "Standalone SELECT executed: " + cols + " columns in the result set");
        }
        if (exec.implicitResultSet) {
          exec.addReturnCursor(new Var(Var.Type.CURSOR, query));
          // return here, do not close the query
          return 0;
        }

        // Build the string for result header and separator
        StringBuilder header = new StringBuilder();
        StringBuilder sep = new StringBuilder();
        int [] widths = new int[cols+1];
        for (int i = 1; i <= cols; i++) {
          if (i > 1) {
            header.append("  ");
            sep.append("  ");
          }
          String lbl = rm.getColumnLabel(i);
          int len = rm.getColumnDisplaySize(i);
          if (lbl.length() > len) {
            len = lbl.length();
          }
          widths[i] = len;
          header.append(lbl);
          header.append(Utils.repeat(len - lbl.length(), ' '));
          sep.append(Utils.repeat(len, '-'));
        }

        System.out.println("");
        System.out.println(header.toString());
        System.out.println(sep.toString());
        System.out.println("");

        // Build the string for result rows
        while (rs.next()) {
          for (int i = 1; i <= cols; i++) {
            if (i > 1) {
              System.out.print("  ");
            }
            String v = rs.getString(i);
            System.out.print(v);
            if (v != null) {
              System.out.print(Utils.repeat(widths[i] - v.length(), ' '));
            }
            else {
              // 'null' length is 4
              System.out.print(Utils.repeat(widths[i] - 4, ' '));
            }
          }
          System.out.println("");
          exec.incRowCount();
        }
      }
      // Scalar subquery
      else {
        trace(ctx, "Scalar subquery executed, first row and first column fetched only");
        if(rs.next()) {
          exec.stackPush(new Var().setValue(rs, rm, 1));
          exec.setSqlSuccess();          
        }
        else {
          evalNull();
          exec.setSqlCode(100);
        }
      }
    }
    catch (SQLException e) {
      if (query.getException() != null) {
        exec.signal(query);
      } else {
        exec.signal(e);
      }
      exec.closeQuery(query, conn);
      return 1;
    }
    exec.closeQuery(query, conn);
    return 0; 
  }  

  /**
   * Common table expression (WITH clause)
   */
  public Integer cte(HplsqlParser.Cte_select_stmtContext ctx) {
    int cnt = ctx.cte_select_stmt_item().size();
    StringBuilder sql = new StringBuilder();
    sql.append("WITH ");
    for (int i = 0; i < cnt; i++) {
      HplsqlParser.Cte_select_stmt_itemContext c = ctx.cte_select_stmt_item(i);      
      sql.append(c.ident().getText());
      if (c.cte_select_cols() != null) {
        sql.append(" " + exec.getFormattedText(c.cte_select_cols()));
      }
      sql.append(" AS (");
      sql.append(evalPop(ctx.cte_select_stmt_item(i).fullselect_stmt()).toString());
      sql.append(")");
      if (i + 1 != cnt) {
        sql.append(",\n");
      }
    }
    exec.stackPush(sql);
    return 0;
  }
  
  /**
   * Part of SELECT
   */
  public Integer fullselect(HplsqlParser.Fullselect_stmtContext ctx) { 
    int cnt = ctx.fullselect_stmt_item().size();
    StringBuilder sql = new StringBuilder();
    for (int i = 0; i < cnt; i++) {
      String part = evalPop(ctx.fullselect_stmt_item(i)).toString(); 
      sql.append(part);
      if (i + 1 != cnt) {
        sql.append("\n" + getText(ctx.fullselect_set_clause(i)) + "\n");
      }
    }
    exec.stackPush(sql);
    return 0; 
  }
  
  public Integer subselect(HplsqlParser.Subselect_stmtContext ctx) {
    StringBuilder sql = new StringBuilder();
    sql.append(ctx.start.getText());
    exec.append(sql, evalPop(ctx.select_list()).toString(), ctx.start, ctx.select_list().getStart());
    Token last = ctx.select_list().stop;
    if (ctx.into_clause() != null) {
      last = ctx.into_clause().stop;
    }
    if (ctx.from_clause() != null) {
      exec.append(sql, evalPop(ctx.from_clause()).toString(), last, ctx.from_clause().getStart());
      last = ctx.from_clause().stop;
    } 
    else if (conf.dualTable != null) {
      sql.append(" FROM " + conf.dualTable);
    }
    if (ctx.where_clause() != null) {
      exec.append(sql, evalPop(ctx.where_clause()).toString(), last, ctx.where_clause().getStart());
      last = ctx.where_clause().stop;
    }
    if (ctx.group_by_clause() != null) {
      exec.append(sql, evalPop(ctx.group_by_clause()).toString(), last, ctx.group_by_clause().getStart());
      last = ctx.group_by_clause().stop;
    }
    if (ctx.having_clause() != null) {
      exec.append(sql, getText(ctx.having_clause()), last, ctx.having_clause().getStart());
      last = ctx.having_clause().stop;
    }
    if (ctx.qualify_clause() != null) {
      exec.append(sql, getText(ctx.qualify_clause()), last, ctx.qualify_clause().getStart());
      last = ctx.qualify_clause().stop;
    }
    if (ctx.order_by_clause() != null) {
      exec.append(sql, evalPop(ctx.order_by_clause()).toString(), last, ctx.order_by_clause().getStart());
      last = ctx.order_by_clause().stop;
    }
    if (ctx.select_options() != null) {
      Var opt = evalPop(ctx.select_options());
      if (!opt.isNull(exec.conf.emptyIsNull)) {
        sql.append(" " + opt.toString());
      }
    }
    if (ctx.select_list().select_list_limit() != null) {
      sql.append(" LIMIT " + evalPop(ctx.select_list().select_list_limit().expr()));
    }
    exec.stackPush(sql);
    return 0; 
  }
  
  /**
   * SELECT list 
   */
  public Integer selectList(HplsqlParser.Select_listContext ctx) { 
    StringBuilder sql = new StringBuilder();
    if (ctx.select_list_set() != null) {
      sql.append(exec.getText(ctx.select_list_set())).append(" ");
    }
    int cnt = ctx.select_list_item().size();
    for (int i = 0; i < cnt; i++) {
      if (ctx.select_list_item(i).select_list_asterisk() == null) {
        sql.append(evalPop(ctx.select_list_item(i).expr()));
        if (ctx.select_list_item(i).select_list_alias() != null) {
          sql.append(" " + exec.getText(ctx.select_list_item(i).select_list_alias()));
        }
      }
      else {
        sql.append(exec.getText(ctx.select_list_item(i).select_list_asterisk()));
      }
      if (i + 1 < cnt) {
        sql.append(", ");
      }
    }
    exec.stackPush(sql);
    return 0;
  }

  /**
   * FROM clause
   */
  public Integer from(HplsqlParser.From_clauseContext ctx) { 
    StringBuilder sql = new StringBuilder();
    sql.append(ctx.T_FROM().getText()).append(" ");
    sql.append(evalPop(ctx.from_table_clause()));
    int cnt = ctx.from_join_clause().size();
    for (int i = 0; i < cnt; i++) {
      sql.append(evalPop(ctx.from_join_clause(i)));
    }
    exec.stackPush(sql);
    return 0;
  }
  
  /**
   * Single table name in FROM
   */
  public Integer fromTable(HplsqlParser.From_table_name_clauseContext ctx) {     
    StringBuilder sql = new StringBuilder();
    sql.append(evalPop(ctx.table_name()));
    if (ctx.from_alias_clause() != null) {
      sql.append(" ").append(exec.getText(ctx.from_alias_clause()));
    }
    exec.stackPush(sql);
    return 0; 
  }
  
  /**
   * Subselect in FROM
   */
  public Integer fromSubselect(HplsqlParser.From_subselect_clauseContext ctx) {     
    StringBuilder sql = new StringBuilder();
    sql.append("(");
    sql.append(evalPop(ctx.select_stmt()).toString());
    sql.append(")");
    if (ctx.from_alias_clause() != null) {
      sql.append(" ").append(exec.getText(ctx.from_alias_clause()));
    }
    exec.stackPush(sql);
    return 0; 
  }
 
  /**
   * JOIN clause in FROM
   */
  public Integer fromJoin(HplsqlParser.From_join_clauseContext ctx) {
    StringBuilder sql = new StringBuilder();
    if (ctx.T_COMMA() != null) {
      sql.append(", ");
      sql.append(evalPop(ctx.from_table_clause()));
    }
    else if (ctx.from_join_type_clause() != null) {
      sql.append(" ");
      sql.append(exec.getText(ctx.from_join_type_clause()));
      sql.append(" ");
      sql.append(evalPop(ctx.from_table_clause()));
      sql.append(" ");
      sql.append(ctx.T_ON().getText());
      sql.append(" ");
      boolean oldBuildSql = exec.buildSql;
      exec.buildSql = true;
      sql.append(evalPop(ctx.bool_expr()));
      exec.buildSql = oldBuildSql;
    }
    exec.stackPush(sql);
    return 0; 
  }

  /**
   * FROM TABLE (VALUES ...) clause
   */
  public Integer fromTableValues(HplsqlParser.From_table_values_clauseContext ctx) {
    StringBuilder sql = new StringBuilder();
    int rows = ctx.from_table_values_row().size();
    sql.append("(");
    for (int i = 0; i < rows; i++) {
      int cols = ctx.from_table_values_row(i).expr().size();
      int cols_as = 0;
      if (ctx.from_alias_clause() != null) {
        cols_as = ctx.from_alias_clause().L_ID().size();
      }
      sql.append("SELECT ");
      for (int j = 0; j < cols; j++) {
        sql.append(evalPop(ctx.from_table_values_row(i).expr(j)));
        if (j < cols_as) {
          sql.append(" AS ");
          sql.append(ctx.from_alias_clause().L_ID(j));  
        }
        if (j + 1 < cols) {
          sql.append(", ");
        }
      }
      if (conf.dualTable != null) {
        sql.append(" FROM " + conf.dualTable);
      }
      if (i + 1 < rows) {
        sql.append("\nUNION ALL\n");
      }
    }
    sql.append(") ");
    if (ctx.from_alias_clause() != null) {
      sql.append(ctx.from_alias_clause().ident().getText());
    }
    exec.stackPush(sql);
    return 0; 
  }
  
  /**
   * WHERE clause
   */
  public Integer where(HplsqlParser.Where_clauseContext ctx) { 
    boolean oldBuildSql = exec.buildSql; 
    exec.buildSql = true;
    StringBuilder sql = new StringBuilder();
    sql.append(ctx.T_WHERE().getText());
    sql.append(" " + evalPop(ctx.bool_expr()));
    exec.stackPush(sql);
    exec.buildSql = oldBuildSql;
    return 0;
  }

  /**
   * ORDER BY clause
   */
  public Integer orderBy(HplsqlParser.Order_by_clauseContext ctx) {
    StringBuilder sql = new StringBuilder();
    sql.append(ctx.T_ORDER().getText()).append(" ").append(ctx.T_BY().getText()).append(" ");
    int cnt = ctx.order_by_col().size();
    for (int i = 0; i < cnt; i++) {
      sql.append(evalPop(ctx.order_by_col(i).expr()).toString());
      if (ctx.order_by_col(i).T_ASC() != null) {
        sql.append(" " + ctx.order_by_col(i).T_ASC().getText());
      }
      else if (ctx.order_by_col(i).T_DESC() != null) {
        sql.append(" " + ctx.order_by_col(i).T_DESC().getText());
      }
      if (i < cnt - 1) {
        sql.append(", ");
      }
    }
    exec.stackPush(sql);
    return 0;
  }
  
  /**
   * Get INTO clause
   */
  HplsqlParser.Into_clauseContext getIntoClause(HplsqlParser.Select_stmtContext ctx) {
    if (ctx.fullselect_stmt().fullselect_stmt_item(0).subselect_stmt() != null) {
      return ctx.fullselect_stmt().fullselect_stmt_item(0).subselect_stmt().into_clause();
    }
    return null;
  }
  
  /**
   * Get number of elements in INTO or var=col assignment clause
   */
  int getIntoCount(HplsqlParser.Select_stmtContext ctx) {
    HplsqlParser.Into_clauseContext into = getIntoClause(ctx);
    if (into != null) {
      return into.ident().size();
    }
    List<HplsqlParser.Select_list_itemContext> sl = ctx.fullselect_stmt().fullselect_stmt_item(0).subselect_stmt().select_list().select_list_item(); 
    if (sl.get(0).T_EQUAL() != null) {
      return sl.size();
    }
    return 0;
  }
  
  /**
   * Get variable name assigned in INTO or var=col clause by index 
   */
  String getIntoVariable(HplsqlParser.Select_stmtContext ctx, int idx) {
    HplsqlParser.Into_clauseContext into = getIntoClause(ctx);
    if (into != null) {
      return into.ident(idx).getText();
    }
    HplsqlParser.Select_list_itemContext sl = ctx.fullselect_stmt().fullselect_stmt_item(0).subselect_stmt().select_list().select_list_item(idx); 
    if (sl != null) {
      return sl.ident().getText();
    }
    return null;
  }
  
  /** 
   * SELECT statement options - LIMIT n, WITH UR i.e
   */
  public Integer option(HplsqlParser.Select_options_itemContext ctx) { 
    if (ctx.T_LIMIT() != null) {
      exec.stackPush("LIMIT " + evalPop(ctx.expr()));
    }
    else if (ctx.T_FOR() != null && ctx.T_UPDATE() != null) {
      exec.stackPush("FOR UPDATE");
    }
    return 0; 
  }
  
  /**
   * GROUP BY clause
   */
  public Integer groupBy(HplsqlParser.Group_by_clauseContext ctx) {
    StringBuilder sql = new StringBuilder();
    sql.append(ctx.T_GROUP().getText()).append(" ").append(ctx.T_BY().getText()).append(" ");
    int cnt = ctx.expr().size();
    for (int i = 0; i < cnt; i++) {
      sql.append(evalPop(ctx.expr(i)).toString());
      if (i < cnt - 1) {
        sql.append(", ");
      }
    }
    exec.stackPush(sql);
    return 0;
  }
  
  /**
   * Evaluate the expression to NULL
   */
  void evalNull() {
    exec.stackPush(Var.Null); 
  }

  /**
   * Evaluate the expression and pop value from the stack
   */
  Var evalPop(ParserRuleContext ctx) {
    exec.visit(ctx);
    if (!exec.stack.isEmpty()) { 
      return exec.stackPop();
    }
    return Var.Empty;
  }

  /**
   * Get node text including spaces
   */
  String getText(ParserRuleContext ctx) {
    return ctx.start.getInputStream().getText(new Interval(ctx.start.getStartIndex(), ctx.stop.getStopIndex()));
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
  void trace(ParserRuleContext ctx, String message) {
    LOG.trace(ctx, message);
  }
  
  void trace(ParserRuleContext ctx, Var var, ResultSet rs, ResultSetMetaData rm, int idx) throws SQLException {
    LOG.trace(ctx, var, rs, rm, idx);
  }    
}
