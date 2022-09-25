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

import java.sql.SQLException;
import org.trafodion.sql.udr.spsql.*;

public class Cursor {

  String name;
  Query query;
  HplsqlParser.Create_routine_paramsContext cursorParametersContext;

  Cursor(String name, Query query) {
    this.name = name;
    this.query = query;
  }

  public void generateQuery(Exec exec, HplsqlParser.Expr_func_paramsContext ctx) {
    exec.enterScope(Scope.Type.CURSOR);
    exec.function.setCallParameters(ctx, exec.function.getActualCallParameters(ctx), cursorParametersContext, null, name);
    if (query.sqlExpr != null) {
      query.setSql(exec.evalPop(query.sqlExpr).toString());
    }
    else if (query.sqlSelect != null) {
      query.setSql(exec.evalPop(query.sqlSelect).toString());
    }
    exec.leaveScope();
  }

  public Query getQuery() {
    return query;
  }
  
  public void setCursorParametersContext(HplsqlParser.Create_routine_paramsContext cursorParametersContext) {
    this.cursorParametersContext = cursorParametersContext;
  }  
}
