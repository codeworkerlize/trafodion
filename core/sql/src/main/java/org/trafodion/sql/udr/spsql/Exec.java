
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

import java.math.BigDecimal;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Comparator;
import java.util.Collections;
import java.util.Comparator;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Stack;
import java.util.Iterator;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.NotNull;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.trafodion.sql.udr.spsql.Var.Type;
import org.trafodion.sql.udr.spsql.functions.*;
import org.antlr.v4.runtime.Parser;

/**
 * HPL/SQL script executor
 *
 */
public class Exec extends HplsqlBaseVisitor<Integer> {    
  class Trigger {
    private boolean statement;
    private boolean callBefore;
    private boolean sql;
    private Long tableId;
    private int triggerType;
    private String triggerName;

    public boolean getStatement() {
      return statement;
    }
    public void setStatement(boolean isStatement) {
      statement = isStatement;
    }

    public boolean getCallBefore() {
      return callBefore;
    }
    public void setCallBefore(boolean iscallBefore) {
      callBefore = iscallBefore;
    }

    public boolean getSql() {
      return sql;
    }
    public void setSql(boolean isSql) {
      sql = isSql;
    }
	  
    public Long getTableId() {
      return tableId;
    }
    public void setTableId(Long id) {
      tableId = id;
    }

    public int getTriggerType() {
      return triggerType;
    }
    public void setTriggerType(int type) {
      triggerType = type;
    }

    public String getTriggerName() {
      return triggerName;
    }
	  
    public void setTriggerName(String name) {
      triggerName = name;
    }
  }
  
  static class ThrowingErrorListener extends BaseErrorListener {
        public static final ThrowingErrorListener INSTANCE = new ThrowingErrorListener();

        @Override
        public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg, RecognitionException e)
                throws ParseCancellationException {
                    String message = underlineError(recognizer,(Token)offendingSymbol,
                       line, charPositionInLine);
                    throw new ParseCancellationException("line " + line + ":" + charPositionInLine + " " + msg + "\n" + message);
        }
        
        protected String underlineError(Recognizer recognizer,
                                  Token offendingToken, int line,
                                  int charPositionInLine) {
            String message = "";
            CommonTokenStream tokens =
                (CommonTokenStream)recognizer.getInputStream();
            String input = tokens.getTokenSource().getInputStream().toString();
            String[] lines = input.split("\n");
            String errorLine = lines[line - 1];
            message += errorLine;
            message += "\n";
            for (int i=0; i<charPositionInLine; i++) message += " ";
            int start = offendingToken.getStartIndex();
            int stop = offendingToken.getStopIndex();
            if ( start>=0 && stop>=0 ) {
                for (int i=start; i<=stop; i++) message += "^";
            }
            return message;
        }    
  }


  // This property is used to control whether we need to manage
  // transaction of default (T2) connections when transaction
  // compatibility is enabled.
  //
  // For other connections (even if they are actually T2 connections),
  // this property is ignored.
  public static final String TRANSACTION_ATTRIBUTE = "spsql.transaction.attribute";
  public static final int UNKNOWN_ROUTINE_TRANSACTION_ATTRIBUTE = 0;
  public static final int NO_TRANSACTION_REQUIRED = 1;
  public static final int TRANSACTION_REQUIRED = 2;
 
  public static final String VERSION = "HPL/SQL 0.3.31";
  public static final String ERRORCODE = "ERRORCODE";
  public static final String SQLCODE = "SQLCODE";
  public static final String SQLSTATE = "SQLSTATE";
  public static final String HOSTCODE = "HOSTCODE";
  
  private static final Logger LOG = Logger.getLogger(Exec.class);

  Exec exec = null;
  ParseTree tree = null;
  
  public enum OnError {EXCEPTION, SETERROR, STOP}; 

  enum RoutineType { PROCEDURE, FUNCTION, PACKAGE, TRIGGER };

  // Scopes of execution (code blocks) with own local variables, parameters and exception handlers
  Stack<Scope> scopes = new Stack<Scope>();
  Scope globalScope;
  Scope currentScope;
  
  Stack<Var> stack = new Stack<Var>();
  Stack<String> labels = new Stack<String>();
  Stack<String> callStack = new Stack<String>();
  Stack<String> schemaStack = new Stack<String>();
  String currentCatalog;
  String currentSchema;
  
  Stack<Signal> signals = new Stack<Signal>();
  Signal currentSignal;
  Scope currentHandlerScope;
  boolean resignal = false;
  boolean execStatementTrigger = false;
  
  HashMap<String, String> managedTables = new HashMap<String, String>();
  HashMap<String, String> objectMap = new HashMap<String, String>(); 
  HashMap<String, String> objectConnMap = new HashMap<String, String>();
  HashMap<String, ArrayList<Var>> returnCursors = new HashMap<String, ArrayList<Var>>();
  HashMap<String, Package> packages = new HashMap<String, Package>();
  
  Package currentPackageDecl = null;
  
  public ArrayList<String> stmtConnList = new ArrayList<String>();
      
  Arguments arguments = new Arguments();
  public Conf conf;
  Expression expr;
  Function function;  
  Converter converter;
  Meta meta;
  Select select;
  Stmt stmt;
  Conn conn;
  ProcessColumn processColumn;
  
  int rowCount = 0;  
  
  String execString;
  String execFile;  
  String execMain;
  StringBuilder localUdf = new StringBuilder();
  boolean initRoutines = false;
  public boolean buildSql = false;
  public boolean loading = false;      // Loading routines
  // Create and return result set for Standalone SELECT and PRINT
  // statements
  public boolean implicitResultSet = true;
  boolean udfRegistered = false;
  boolean udfRun = false;
    
  boolean dotHplsqlrcExists = false;
  boolean hplsqlrcExists = false;
  
  boolean trace = true; 
  boolean info = true;
  boolean offline = false;
  String jvmName = null;
  
  // SYS CONTEXT
  public SysContext sysContext = null;

  String sqlLoadRoutine = "SELECT TEXT"
    + " FROM TRAFODION.\"_MD_\".OBJECTS"
    + " JOIN TRAFODION.\"_MD_\".TEXT"
    + " ON OBJECT_UID = TEXT_UID"
    + " JOIN TRAFODION.\"_MD_\".ROUTINES"
    + " ON OBJECT_UID = UDR_UID"
    + " WHERE OBJECT_TYPE = ?"
    + " AND UDR_TYPE = ?"
    + " AND CATALOG_NAME = ?"
    + " AND SCHEMA_NAME = ?"
    + " AND OBJECT_NAME = ?"
    + " ORDER BY SUB_ID, SEQ_NUM";

  String sqlLoadPackage = "SELECT TEXT"
    + " FROM TRAFODION.\"_MD_\".OBJECTS"
    + " JOIN TRAFODION.\"_MD_\".TEXT"
    + " ON OBJECT_UID = TEXT_UID"
    + " WHERE OBJECT_TYPE = 'PA'"
    + " AND CATALOG_NAME = ?"
    + " AND SCHEMA_NAME = ?"
    + " AND OBJECT_NAME = ?"
    + " ORDER BY SUB_ID, SEQ_NUM";

  Query queryLoadRoutine = null;
  Query queryLoadPackage = null;

  Exec() {
    exec = this;
    jvmName = ManagementFactory.getRuntimeMXBean().getName();
  }
  
  Exec(Exec exec) {
    this.exec = exec;
  }

  public Integer init() throws Exception {
    // Copied from init() method
    //System.setProperty("log4j.configurationFile", "hive-log4j2.properties");
    conf = new Conf();
    conf.init();    
    conn = new Conn(this);
    meta = new Meta(this);

    // Add default connection, configure file can override this
    exec.conf.defaultConnection = "default";
    exec.conn.addConnection("default", "org.trafodion.jdbc.t4.T4Driver;jdbc:default:connection");

    // Set dual table name
    exec.conf.setOption(Conf.DUAL_TABLE, "DUAL");

    initOptions();
    initSchema();

    // Initialize sys context information
    sysContext = new SysContext();
    sysContext.init();

    // Implicit result set can only be supported for default
    // connection
    if (!exec.conf.defaultConnection.equals("default")) {
      exec.implicitResultSet = false;
    }
    
    expr = new Expression(this);
    select = new Select(this);
    stmt = new Stmt(this);
    converter = new Converter(this);
        
    function = new Function(this);
    new FunctionDatetime(this).register(function);
    new FunctionMisc(this).register(function);
    new FunctionString(this).register(function);
    new FunctionOra(this).register(function);

    LOG.trace("Configuration file: " + conf.getLocation());
    return 0;
  }

  public void initSchema() {
    currentCatalog = System.getProperty("sqlmx.udr.catalog");
    currentSchema = System.getProperty("sqlmx.udr.schema");
    LOG.trace("Default schema " + getCurrentSchema());
  }

  public void initGlobalScope() {
    scopes.clear();
    globalScope = null;
    currentScope = null;

    stack.clear();
    labels.clear();
    callStack.clear();
  
    schemaStack.clear();
    signals.clear();
    currentSignal = null;
    currentHandlerScope = null;
    resignal = false;
    loading = false;
    execStatementTrigger = false;
    queryLoadRoutine = null;
    queryLoadPackage = null;

    returnCursors.clear();

    // Reset SP transaction attribute, it will be set when openning
    // default connections.
    setTransactionAttrs(UNKNOWN_ROUTINE_TRANSACTION_ATTRIBUTE);

    enterGlobalScope();
    addVariable(new Var(ERRORCODE, Var.Type.BIGINT, 0L));
    addVariable(new Var(SQLCODE, Var.Type.BIGINT, 0L));
    addVariable(new Var(SQLSTATE, Var.Type.STRING, "00000"));
    addVariable(new Var(HOSTCODE, Var.Type.BIGINT, 0L)); 
    for (Map.Entry<String, String> v : arguments.getVars().entrySet()) {
      addVariable(new Var(v.getKey(), Var.Type.STRING, v.getValue()));
    }    
    initSchema();
  }

  public void clearConnections() {
    // Trafodion jdbc:default:connection will be automatically closed,
    // so we clear the old default connections
    conn.clearDefaultConnections();
  }

  public void loadPackage(String name) throws RuntimeException {
    loadRoutine(name, RoutineType.PACKAGE);
  }

  public void loadProcedure(String name) throws RuntimeException {
    loadRoutine(name, RoutineType.PROCEDURE);
  }

  public void loadFunction(String name) throws RuntimeException {
    loadRoutine(name, RoutineType.FUNCTION);
  }

  public void loadTrigger(String name) throws RuntimeException {
    loadRoutine(name, RoutineType.TRIGGER);
  }

  public List<Trigger> getTriggerName(String name, int iscallbefore, int optype, boolean statement) throws RuntimeException {
    try {
      return _getTriggerName(name, iscallbefore, optype, statement);
    } catch (Exception e) {
      throw new RuntimeException("getTriggerName " + " table name is: " + name + " " + " iscallbefore: "
				 + iscallbefore + " optype is: " + optype + " failed with exception: " + e.getMessage(), e);
    } finally {

    }
  }

  private List<Trigger> _getTriggerName(String name, int iscallbefore, int optype, boolean statement) throws Exception {
    LOG.trace("getTriggerName, the table name is: " + name);
    String[] qualified = name.split(",");
    if (qualified.length < 2) {
      LOG.trace("Not a valid call trigger: " + name);
      return null;
    }

    boolean issql = true;
    boolean callbefore = false;
    if (iscallbefore == 1)
      callbefore = true;
      
    Long tabid = 0L;    
    tabid = Long.valueOf(qualified[0]);
    List<Trigger> list = new ArrayList();

    for (int i = 1; i < qualified.length; i++)
      {
        Trigger trg = new Trigger();          
	trg.setStatement(statement);
	trg.setCallBefore(callbefore);
	trg.setSql(issql);
	trg.setTableId(tabid);
	trg.setTriggerType(optype);
	trg.setTriggerName(qualified[i]);
	list.add(trg);
      }
    
    return list;
  }
  
  private void loadRoutine(String name, RoutineType type) throws RuntimeException {
    try {
      _loadRoutine(name, type);
    } catch (SQLException e) {
      throw new RuntimeException("Loading "
                                 + type + " "
                                 + name
                                 + " failed with SQLException (error code "
				 + e.getErrorCode()
				 + "): "
                                 + e.getMessage(), e);
    } catch (Exception e) {
      throw new RuntimeException("Loading "
                                 + type + " "
                                 + name
                                 + " failed with exception: "
                                 + e.getMessage(), e);
    }
  }

  private void _loadRoutine(String name, RoutineType type) throws Exception {
    LOG.trace("Loading " + type + ": " + name);
    ArrayList<String> qualified = Meta.splitIdentifier(name);
    if (qualified.size() > 3) {
      LOG.trace("Not a valid package name: " + name);
      return;
    }
    String cat = qualified.get(0);
    String sch = qualified.get(1);
    String nam = qualified.get(2);

    LOG.trace("  cat: " + cat);
    LOG.trace("  sch: " + sch);
    LOG.trace("  nam: " + nam);

    Query query = null;
    if (type == RoutineType.PACKAGE) {
      if (queryLoadPackage == null) {
        queryLoadPackage = prepareQuery(null, new Query(sqlLoadPackage), "default");
      }
      query = queryLoadPackage;
    } else {
      if (queryLoadRoutine == null) {
        queryLoadRoutine = prepareQuery(null, new Query(sqlLoadRoutine), "default");
      }
      query = queryLoadRoutine;
    }
    ArrayList paramList = new ArrayList();
    if (type == RoutineType.TRIGGER) {
      paramList.add("TR");
      paramList.add("TR");
    } else if (type == RoutineType.PROCEDURE) {
      paramList.add("UR");
      paramList.add("P");
    } else if (type == RoutineType.FUNCTION) {
      paramList.add("UR");
      paramList.add("F");
    } else {                    // PACKAGE
      // package does not have any extra parameters
    }
    // Common parameters for all
    paramList.add(cat);
    paramList.add(sch);
    paramList.add(nam);

    LOG.trace("query: " + query + " params: " + paramList.size());
    query = conn.executePreparedQuery(query, paramList, "default");
    if (query.error()) {
      LOG.error("Load " + type + " failed: " + name);
      throw query.getException();
    }
    ResultSet rc = query.getResultSet();
    StringBuilder sb = new StringBuilder();
    while (rc.next()) {
      sb.append(rc.getString(1));
    }
    if (sb.length() != 0) {
      createRoutine(sb.toString(), true);
      LOG.trace("Loaded " + type + ": " + name);
    } else {
      LOG.trace("Load " + type + " not found: " + name);
    }
  }

  public void createRoutine(String query, boolean loading) throws Exception {
    boolean oldLoading = exec.loading;
    exec.loading = loading;
    try {
      ParseTree tree = parseQuery(query);
      LOG.trace("createRoutine begin visit");
      visit(tree);
      LOG.trace("createRoutine end visit");
    } catch (Exception e) {
      LOG.trace("Error when creating routine:\n" + query);
      throw e;
    } finally {
      exec.loading = oldLoading;
    }
  }

  public ParseTree parseQuery(String query) throws Exception {
    ParseTree tree;
    InputStream input = null;
    input = new ByteArrayInputStream(query.getBytes("UTF-8"));
    HplsqlLexer lexer = new HplsqlLexer(new ANTLRInputStream(input));
    lexer.removeErrorListeners();
    lexer.addErrorListener(ThrowingErrorListener.INSTANCE);
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    HplsqlParser parser = new HplsqlParser(tokens);
    parser.removeErrorListeners();
    parser.addErrorListener(ThrowingErrorListener.INSTANCE);
    try {
      tree = parser.program();
    } catch (Exception e) {
      LOG.trace("Error when parsing: " + e.getMessage());
      throw e;
    }
    LOG.trace("loading: " + loading);
    LOG.trace("offline: " + offline);
    LOG.trace("Parser tree: " + tree.toStringTree(parser));
    return tree;
  }

  public void runQuery(String query) throws Exception {
    ParseTree tree = parseQuery(query);
    visit(tree);
    processSignal();
    closeConnections(false);
  }

  void processSignal() throws Exception {
    resignal = false;
    Exception e = null;
    while (!signals.empty()) {
      Signal sig = exec.signals.pop();
      if (sig.type == Signal.Type.SQLEXCEPTION ||
          sig.exception != null) {
        LOG.error("Unhandled exception in SP/SQL");
      }
      if (sig.exception != null) {
        sig.exception.printStackTrace();
	if ( e == null) {
	  e = sig.exception;
	}
      }
      else if (sig.type == Signal.Type.NOTFOUND ||
               sig.type == Signal.Type.UNSUPPORTED_OPERATION) {
        String errmsg = sig.value;
        if (errmsg == null) {
          errmsg = "Error: " + sig.type;
        }
        throw new SQLException(errmsg);
      }
      else if (sig.value != null) {
        LOG.error(sig.value);
      }
    }
    if (e != null) {
      throw e;
    }
  }

  public int getSQLCode() {
    Var var = findVariable(SQLCODE);
    if (var == null) {
      return 0;
    }
    return var.intValue();
  }

  public String getSQLState() {
    Var var = findVariable(SQLSTATE);
    if (var == null) {
      return "00000";
    }
    return var.toString();
  }


  /** 
   * Set a variable using a value from the parameter or the stack 
   */
  public Var setVariable(String name, Var value) {
    LOG.trace("SET VARIABLE " + name);
    String origName = name;
    if (name.startsWith("\"")) {
      name = name.substring(1, name.length() - 1);
    }
    else {
      name = name.toUpperCase();
    }
    if (value == null || value == Var.Empty) {
      if (exec.stack.empty()) {
        return Var.Empty;
      }
      value = exec.stack.pop();
    }
    if (name.startsWith("hplsql.")) {
      exec.setOption(name, value.toString());
      return Var.Empty;
    }
    Var var = findVariable(origName);
    if (var != null) {
      var.cast(value);
    }
    else {
      var = new Var(value);
      var.setName(name);
      if(exec.currentScope != null) {
        exec.currentScope.addVariable(var);
      }
    }    
    return var;
  }
  
  public Var setVariable(String name) {
    return setVariable(name, Var.Empty);
  }
  
  public Var setVariable(String name, String value) {
    return setVariable(name, new Var(value));
  }

  public Var setVariable(String name, int value) {
    return setVariable(name, new Var(new Long(value)));
  }

  /** 
   * Set variable to NULL 
   */
  public Var setVariableToNull(String name) {
    Var var = findVariable(name);
    if (var != null) {
      var.removeValue();
    }
    else {
      var = new Var();
      var.setName(name);
      if(exec.currentScope != null) {
        exec.currentScope.addVariable(var);
      }
    }    
    return var;
  }
  
  /**
   * Add a local variable to the current scope
   */
  public void addVariable(Var var) {
    if (var.getName().startsWith("\"")) {
      var.setName(var.getName().substring(1, var.getName().length() - 1));
    }
    else {
      var.setName(var.getName().toUpperCase());
    }
    LOG.trace("ADD VARIALBE: " + var.getName());
    if (currentPackageDecl != null) {
      currentPackageDecl.addVariable(var);
    }
    else if (exec.currentScope != null) {
      exec.currentScope.addVariable(var);
    }
  }
  
  /**
   * Add a condition handler to the current scope
   */
  public void addHandler(Handler handler) {
    if (exec.currentScope != null) {
      exec.currentScope.addHandler(handler);
    }
  }
  
  /**
   * Add a return cursor visible to procedure callers and clients
   */
  public void addReturnCursor(Var var) {
    String routine = callStackPeek();
    ArrayList<Var> cursors = returnCursors.get(routine);
    if (cursors == null) {
      cursors = new ArrayList<Var>();
      returnCursors.put(routine, cursors);
    }
    cursors.add(var);
  }
  
  /**
   * Get the return cursor defined in the specified procedure
   */
  public Var consumeReturnCursor(String routine) {
    ArrayList<Var> cursors = returnCursors.get(routine.toUpperCase());
    if (cursors == null) {
      return null;
    }
    Var var = cursors.get(0);
    cursors.remove(0);
    return var;
  }
  
  /**
   * Return the count of return cursors for routine
   */
  public int returnCursorCount(String routine) {
    ArrayList<Var> cursors = returnCursors.get(routine.toUpperCase());
    if (cursors == null) {
      return 0;
    }
    return cursors.size();
  }

  /**
   * Push a value to the stack
   */
  public void stackPush(Var var) {
    exec.stack.push(var);  
  }
  
  /**
   * Push a string value to the stack
   */
  public void stackPush(String val) {
    exec.stack.push(new Var(val));  
  }
  
  public void stackPush(StringBuilder val) {
    stackPush(val.toString());  
  }
  
  /**
   * Push a boolean value to the stack
   */
  public void stackPush(Boolean val) {
    exec.stack.push(new Var(val));  
  }

  /**
   * Select a value from the stack, but not remove
   */
  public Var stackPeek() {
    return exec.stack.peek();  
  }
  
  /**
   * Pop a value from the stack
   */
  public Var stackPop() {
    if (!exec.stack.isEmpty()) {
      return exec.stack.pop();
    }
    return Var.Empty;
  }    
  
  public String getCurrentSchema() {
    return currentCatalog + "." + currentSchema;
  }

  public void setCurrentSchema(String catalog, String schema) {
    LOG.trace("set current schema to " + catalog + "." + schema);
    currentCatalog = catalog.toUpperCase();
    currentSchema = schema.toUpperCase();
  }

  public void setCurrentSchema(String schema) {
    ArrayList<String> parts = Meta.splitIdentifierToTwoParts(schema);
    if (parts == null) {
      LOG.trace("set current schema to " + schema.toUpperCase());
      currentSchema = schema.toUpperCase();
      return;
    }
    setCurrentSchema(parts.get(0), parts.get(1));
  }

  public String qualify(String name) {
    ArrayList<String> parts = Meta.splitIdentifier(name);
    if (parts == null) {
      return getCurrentSchema() + "." + name;
    }
    if (parts.size() == 2) {
      return currentCatalog + "." + name;
    }
    return name;
  }

  public void saveSchema(String routine) {
    LOG.trace("SAVE SCHEMA " + getCurrentSchema());
    exec.schemaStack.push(getCurrentSchema());
    ArrayList<String> parts = Meta.splitIdentifier(routine);
    if (parts == null) {
      return;
    }
    if (parts.size() == 2) {
      setCurrentSchema(parts.get(0));
    } else if (parts.size() > 2) {
      setCurrentSchema(parts.get(0), parts.get(1));
    }
  }

  public void restoreSchema() {
    LOG.trace("RESTORE SCHEMA " + getCurrentSchema());
    String schema = exec.schemaStack.pop();
    setCurrentSchema(schema);
  }

  /**
   * Push a value to the call stack
   */
  public void callStackPush(String val) {
    exec.callStack.push(val.toUpperCase());  
  }
  
  /**
   * Select a value from the call stack, but not remove
   */
  public String callStackPeek() {
    if (!exec.callStack.isEmpty()) {
      return exec.callStack.peek();
    }
    return null;
  }
  
  /**
   * Pop a value from the call stack
   */
  public String callStackPop() {
    if (!exec.callStack.isEmpty()) {
      return exec.callStack.pop();
    }
    return null;
  }  
  
  public void callProc(String name, Object[] args) throws Exception {
    ArrayList<String> qualified = exec.meta.splitIdentifierToTwoParts(name);
    boolean executed = false;
    if (qualified != null) {
      Package pack = findPackage(qualified.get(0));
      if (pack != null) {
	executed = pack.execProc(qualified.get(1), args);
      }
    }
    if (!executed) {
      executed = exec.function.execProc(name, args);
    }
    if (!executed) {
      throw new SQLException("Procedure not found: " + name);
    }
    processSignal();
    closeConnections(false);
  }

  public void callFunc(String name, Object[] args) throws Exception {
    ArrayList<String> qualified = exec.meta.splitIdentifierToTwoParts(name);
    boolean executed = false;
    if (qualified != null) {
      Package pack = findPackage(qualified.get(0));
      if (pack != null) {
	executed = pack.execFunc(qualified.get(1), args);
      }
    }
    if (!executed) {
      executed = exec.function.execUser(name, args);
    }
    if (!executed) {
      throw new SQLException("Function not found: " + name);
    }
    processSignal();
    closeConnections(false);
  }

  public boolean isSystemSchema(String schema) {
    if (schema.equals("_CELL_") || schema.equals("HIVE") || schema.equals("SEABASE")
	|| schema.equals("_HBASESTATS_") || schema.equals("_HB_MAP_") || schema.equals("_HV_HIVE_")
	|| schema.equals("_LIBMGR_") || schema.equals("_MD_") || schema.equals("_PRIVMGR_MD_")
	|| schema.equals("_REPOS_") || schema.equals("_DTM_") || schema.equals("_TENANT_MD_"))
      return true;
    else
      return false;
  }

  public void callTrigger(String name, int iscallbefore, int optype, String oldskvBuffer, String oldsBufSize,
			  String oldrowIds, String oldrowIdsLen, int newrowIdLen, String newrowIds, int newrowIdsLen, String newrows,
			  int newrowsLen, String curExecSql) throws Exception {
    LOG.trace("call trigger table name : " + name);
    String[] qualified = name.split(",");
    if (qualified.length < 2) {
      LOG.trace("Not a valid call trigger: " + name);
      return;
    }

    Object[] args = null;
    boolean executed = false;
    boolean isStatement = false;

    if (oldskvBuffer == null && oldrowIds == null &&
	newrowIds == null && newrows == null) {
      isStatement = true;
    }

    if (qualified != null) {
      // get trigger name
      List<Trigger> list = exec.getTriggerName(name, iscallbefore, optype, isStatement);
      if (!list.isEmpty()) {
	List<Row[]> arrRow = null;
	if (isStatement == false) {
	  //process new and old value, first is old row, second is new row
	  arrRow = exec.function.processRowsData(list.get(0).getTableId(),
						 oldskvBuffer, oldsBufSize,
						 oldrowIds, oldrowIdsLen,
						 newrowIdLen,
						 newrowIds, newrowIdsLen,
						 newrows, newrowsLen
						 );
	  if (arrRow == null) {
	    throw new SQLException("deal old and new value error");
	  }
	}

	for (Trigger trg : list) {
	  LOG.trace("call trigger trigger name is : " + trg.getTriggerName());
	  if (isStatement) {
            execStatementTrigger = true;
	    executed = exec.function.execTrigger(trg.getTriggerName(), iscallbefore, optype, trg.getStatement(),
						 null,
						 args,
						 curExecSql);
            execStatementTrigger = false;
	    if (!executed) {
              throw new SQLException("trigger " + trg.getTriggerName() + " exec failed");
	    }
	  } else {
	    for (int j = 0; j < arrRow.size(); j++) {
	      executed = exec.function.execTrigger(trg.getTriggerName(), iscallbefore, optype, trg.getStatement(),
						   arrRow.get(j),
						   args,
						   curExecSql);
	      if (!executed) {
		throw new SQLException("trigger " + trg.getTriggerName() + " exec failed ");
	      }
	    }
	  }
	}
      } else {
      LOG.trace("callTrigger, but Trigger not found");
      }
    }

    processSignal();
    closeConnections(false);
  }
  
  /** 
   * Find an existing variable by name 
   */
  public Var findVariable(String name) throws RuntimeException{
    LOG.trace("FIND VARIABLE: " + name);
    Var var = null;
    String name1 = name;
    String name1a = null;
    String name2 = null;
    Scope cur = exec.currentScope;

    boolean isKeyWord = false;
    String nameTemp = name;
    nameTemp = nameTemp.toUpperCase();
    if (nameTemp.startsWith(":NEW.") ||
	nameTemp.startsWith("NEW.") ||
	nameTemp.startsWith(":OLD.") ||
	nameTemp.startsWith("OLD.")) {
      isKeyWord = true;
      if (execStatementTrigger) {
          throw new RuntimeException("NEW or OLD references not allowed in statement triggers");
      }
    }
    LOG.trace("FIND VARIABLE, is kewword: " + isKeyWord);
    
    Package pack = null;
    Package packCallContext = exec.getPackageCallContext();
    ArrayList<String> qualified = exec.meta.splitIdentifierToTwoParts(name);
    if (qualified != null) {
      name1 = qualified.get(0);
      name2 = qualified.get(1);
      if (!isKeyWord && !conf.disablePackageVariable) {
	pack = findPackage(name1);
	if (pack != null) {        
	  var = pack.findVariable(name2);
	  if (var != null) {
	    return var;
	  }
	}
	// Check for row variable in a package;
	ArrayList<String> q = exec.meta.splitIdentifierToTwoParts(name1);
	if (q != null) {
	  String n1 = q.get(0);
	  String n2 = q.get(1);
	  pack = findPackage(n1);
	  if (pack != null) {
	    var = pack.findVariable(n2);
	    if (var != null && var.type == Var.Type.ROW && var.value != null) {
	      Row row = (Row)var.value;
              if (row.getValue(name2).type == Var.Type.LOB) {
                  throw new RuntimeException("trigger not support clob or blob columns");
              }
	      return row.getValue(name2);
	    }
	  }
	}
      }
    }
    if (name1.startsWith(":")) {
      name1a = name1.substring(1);
    }    
    while (cur != null) {
      var = findVariable(cur.vars, name1);
      if (var == null && name1a != null) {
        var = findVariable(cur.vars, name1a);
      }
      if (var == null && packCallContext != null) {
        var = packCallContext.findVariable(name1);
      }
      if (var != null) {
        if (qualified != null) {
          if (var.type == Var.Type.ROW && var.value != null) {
            Row row = (Row)var.value;
            var = row.getValue(name2);
            if (var.type == Var.Type.LOB) {
                throw new RuntimeException("trigger not support clob or blob columns");
            }
          }
        }
        return var;
      }
      if (cur.type == Scope.Type.ROUTINE) {
        cur = exec.globalScope;
      }
      else {
        cur = cur.parent;
      }
    }    
    LOG.trace("Variable not found: " + name);
    return null;
  }
  
  public Var findVariable(Var name) {
    return findVariable(name.getName());
  }
  
  Var findVariable(ArrayList<Var> vars, String name) {
    for (Var var : vars) {
      String name1 = name;
      if (name1.startsWith("\"")) {
        name1 = name1.substring(1, name1.length() - 1);
        if (name1.equals(var.getName())) {
          return var;
        }
      }
      else {
        if (name1.toUpperCase().equals(var.getName())){
          return var;
        }
      }
    }
    return null;
  }
  
  /**
   * Find a cursor variable by name
   */
  public Var findCursor(String name) {
    Var cursor = exec.findVariable(name);
    if (cursor != null && cursor.type == Type.CURSOR) {
      return cursor;
    }    
    return null;
  }
  
  /**
   * Find the package by name
   */
  public Package findPackage(String name) {
    LOG.trace("FIND PACKAGE: " + name);
    // If name is of pattern TRAFODION.something, assume it not a
    // package name.
    ArrayList<String> parts = Meta.splitIdentifier(name.toUpperCase());
    if (parts != null &&
        parts.size() == 2 &&
        parts.get(0).equals("TRAFODION")) {
      LOG.trace("Not a package name: " + name);
      return null;
    }
    String qualified = qualify(name.toUpperCase());
    Package pkg = packages.get(qualified);
    if (pkg == null) {
      loadPackage(qualified);
      pkg = packages.get(qualified);
    }
    if (pkg == null) {
      LOG.trace("Package not found: " + name);
    }
    return pkg;
  }
  
  public void dropFunction(String name) {
    function.delUserFunction(name);
  }

  public void dropProcedure(String name) {
    function.delUserProcedure(name);
  }

  public void dropPackage(String name) {
    packages.remove(name);
  }

  public void dropTrigger(String name) {
    function.delUserTrigger(name);
  }
  
  public void _dropPackage(String name) {
    ArrayList<String> qualified = exec.meta.splitIdentifier(name.toUpperCase());
    String cat = qualified.get(0);
    String sch = qualified.get(1);
    String pkg = qualified.get(2);
    String sql
      = "SELECT OBJECT_NAME, UDR_TYPE FROM"
      + " TRAFODION.\"_MD_\".OBJECTS"
      + " JOIN TRAFODION.\"_MD_\".ROUTINES"
      + " ON"
      + " OBJECT_UID=UDR_UID"
      + " WHERE"
      + " OBJECT_TYPE='UR'"
      + " AND CATALOG_NAME='" + cat + "'"
      + " AND SCHEMA_NAME='" + sch + "'"
      + " AND OBJECT_NAME LIKE '" + pkg + ".%'";
    Query query = exec.executeSql(sql);
    if (query.error()) {
      exec.signal(query);
      return;
    }
    ResultSet rc = query.getResultSet();
    try {
      while (rc.next()) {
        String obj = rc.getString(1);
        String type = rc.getString(2);
        if (type.equalsIgnoreCase("P")) {
          type = "PROCEDURE";
        } else {
          type = "FUNCTION";
        }
        sql = "DROP " + type
          + " " + cat
          + "." + sch
          + ".\"" + obj + "\"";
        query = exec.executeSql(sql);
        if (query.error()) {
          exec.signal(query);
          return;
        }
      }
    } catch (SQLException e) {
      exec.signal(e);
    }
  }

  /**
   * Enter a new scope
   */
  public void enterScope(Scope scope) {
    exec.scopes.push(scope);
  }
  
  public void enterScope(Scope.Type type) {
    enterScope(type, null);
  }
  
  public void enterScope(Scope.Type type, Package pack) {
    exec.currentScope = new Scope(exec.currentScope, type, pack);
    enterScope(exec.currentScope);
  }
  
  public void enterGlobalScope() {
    globalScope = new Scope(Scope.Type.GLOBAL);
    currentScope = globalScope;
    enterScope(globalScope);
  }

  /**
   * Leave the current scope
   */
  public void leaveScope() {
    if (!exec.signals.empty()) {
      Scope scope = exec.scopes.peek();
      Signal signal = exec.signals.peek();
      if (exec.conf.onError != OnError.SETERROR) {
        runExitHandler();
      }
      if (signal.type == Signal.Type.LEAVE_ROUTINE && scope.type == Scope.Type.ROUTINE) {
        exec.signals.pop();
      }
    }
    exec.currentScope = exec.scopes.pop().getParent();
  }
  
  /**
   * Send a signal
   */
  public void signal(Signal signal) {
    LOG.trace("signal type=" + signal.type
              + " value=" + signal.value
              + " exception=" + signal.exception);
    exec.signals.push(signal);
  }
  
  public void signal(Signal.Type type, String value, Exception exception) {
    signal(new Signal(type, value, exception));
  }
  
  public void signal(Signal.Type type, String value) {
    setSqlCode(-1);
    signal(type, value, null);   
  }
  
  public void signal(Signal.Type type) {
    setSqlCode(-1);
    signal(type, null, null);   
  }
  
  public void signal(Query query) {
    setSqlCode(query.getException());
    signal(Signal.Type.SQLEXCEPTION, query.errorText(), query.getException());
  }
  
  public void signal(Exception exception) {
    setSqlCode(exception);
    signal(Signal.Type.SQLEXCEPTION, exception.getMessage(), exception);
  }
  
  /**
   * Resignal the condition
   */
  public void resignal() {
    resignal(exec.currentSignal);
  }
  
  public void resignal(Signal signal) {
    if (signal != null) {
      exec.resignal = true;
      signal(signal);
    }
  }

  /**
   * Run CONTINUE handlers 
   */
  boolean runContinueHandler() {
    Scope cur = exec.currentScope;    
    exec.currentSignal = exec.signals.pop(); 
    while (cur != null) {
      for (Handler h : cur.handlers) {
        if (h.execType != Handler.ExecType.CONTINUE) {
          continue;
        }
        if ((h.type != Signal.Type.USERDEFINED && h.type == exec.currentSignal.type) ||
            (h.type == Signal.Type.USERDEFINED && h.type == exec.currentSignal.type &&
             h.value.equalsIgnoreCase(exec.currentSignal.value))) {
          LOG.trace(h.ctx, "CONTINUE HANDLER");
          enterScope(Scope.Type.HANDLER);
          exec.currentHandlerScope = h.scope; 
          exec.resignal = false;
          visit(h.ctx.single_block_stmt());
          leaveScope(); 
          exec.currentSignal = null;
          return true;
        }
      }      
      cur = cur.parent;
    } 
    exec.signals.push(exec.currentSignal);
    exec.currentSignal = null;
    return false;
  }
  
  /**
   * Run EXIT handler defined for the current scope 
   */
  boolean runExitHandler() {
    exec.currentSignal = exec.signals.pop();
    for (Handler h : currentScope.handlers) {
      if (h.execType != Handler.ExecType.EXIT) {
        continue;
      }
      if ((h.type != Signal.Type.USERDEFINED && h.type == exec.currentSignal.type) ||
          (h.type == Signal.Type.USERDEFINED && h.type == exec.currentSignal.type &&
           h.value.equalsIgnoreCase(currentSignal.value))) {
        LOG.trace(h.ctx, "EXIT HANDLER");
        enterScope(Scope.Type.HANDLER);
        exec.currentHandlerScope = h.scope; 
        exec.resignal = false;
        visit(h.ctx.single_block_stmt());
        leaveScope(); 
        exec.currentSignal = null;
        return true;
      }        
    }    
    exec.signals.push(exec.currentSignal);
    exec.currentSignal = null;
    return false;
  }
    
  /**
   * Pop the last signal
   */
  public Signal signalPop() {
    if (!exec.signals.empty()) {
      return exec.signals.pop();
    }
    return null;
  }
  
  /**
   * Peek the last signal
   */
  public Signal signalPeek() {
    if (!exec.signals.empty()) {
      return exec.signals.peek();
    }
    return null;
  }
  
  /**
   * Pop the current label
   */
  public String labelPop() {
    if(!exec.labels.empty()) {
      return exec.labels.pop();
    }
    return "";
  }
  
  /**
   * Execute a SQL query (SELECT)
   */
  public Query executeQuery(ParserRuleContext ctx, Query query, String connProfile) {
    if (!exec.offline) {
      exec.rowCount = 0;
      exec.conn.executeQuery(query, connProfile);
      return query;
    }
    setSqlNoData();
    LOG.info(ctx, "Not executed - offline mode set");
    return query;
  }

  public Query executeQuery(ParserRuleContext ctx, String sql, String connProfile) {
    return executeQuery(ctx, new Query(sql), connProfile);
  }
  
  /**
   * Prepare a SQL query (SELECT)
   */
  public Query prepareQuery(ParserRuleContext ctx, Query query, String connProfile) {
    if (!exec.offline) {
      exec.rowCount = 0;
      exec.conn.prepareQuery(query, connProfile);
      return query;
    }
    setSqlNoData();
    LOG.info(ctx, "Not executed - offline mode set");
    return query;
  }

  public Query prepareQuery(ParserRuleContext ctx, String sql, String connProfile) {
    return prepareQuery(ctx, new Query(sql), connProfile);
  }

  /**
   * Prepare a SQL statement
   */
  public Query prepareSql(ParserRuleContext ctx, Query query, String prepareSqlName, String connProfile) {
    if (!exec.offline) {
      exec.rowCount = 0;
      return exec.conn.prepareSql(query, prepareSqlName, connProfile);
    }
    setSqlNoData();
    LOG.info(ctx, "Not executed - offline mode set");
    return query;
  }

  public Query prepareSql(ParserRuleContext ctx, String sql, String prepareSqlName, String connProfile) {
    return prepareSql(ctx, new Query(sql), prepareSqlName, connProfile);
  }  

  /**
   * Execute a SQL statement 
   */
  public Query executePreparedSql(ParserRuleContext ctx, String preparedSqlName, ArrayList paramList, String connProfile) {
    if (!exec.offline) {
      exec.rowCount = 0;
      Query query = conn.executePreparedSql(preparedSqlName, paramList, connProfile);
      exec.rowCount = query.getRowCount();
      return query;
    }
    LOG.info(ctx, "Not executed - offline mode set");
    return new Query("");
  }
  
  /**
   * Execute a SQL statement 
   */
  public Query executeSql(ParserRuleContext ctx, String sql, String connProfile) {
    if (!exec.offline) {
      exec.rowCount = 0;
      Query query = conn.executeSql(sql, connProfile);
      exec.rowCount = query.getRowCount();
      return query;
    }
    LOG.info(ctx, "Not executed - offline mode set");
    return new Query("");
  }  
  
  public Query executeSql(ParserRuleContext ctx, String sql) {
    return executeSql(ctx, sql, exec.conf.defaultConnection);
  }

  public Query executeSql(String sql) {
    return executeSql(null, sql, exec.conf.defaultConnection);
  }

  public int getTransactionAttrs() {
    return Integer.parseInt(System.getProperty(TRANSACTION_ATTRIBUTE));
  }

  public void setTransactionAttrs(int value) {
    System.setProperty(TRANSACTION_ATTRIBUTE, Integer.toString(value));
  }

  /**
   * Manage connection transaction
   */
  public boolean manageTransaction(String connName) {
    if (conf.transactionCompatible) {
      if (connName == "default") {
        return getTransactionAttrs() == NO_TRANSACTION_REQUIRED;
      }
      return true;
    }
    return false;
  }

  /**
   * Close the query object
   */
  public void closeQuery(Query query, String conn) {
    if(!exec.offline) {
      exec.conn.closeQuery(query, conn);
    }
  }

  public void closeConnections(boolean rollback) throws Exception {
    closePrepares();
    closePoolConnections(rollback);
  }

  /**
   * Close the connection in the connection pool
   */
  public void closePoolConnections(boolean rollback) throws Exception {
    for (String connName : exec.conn.connections.keySet()) {
      Stack<Connection> conns = exec.conn.connections.get(connName);
      boolean manageTrans = manageTransaction(connName);
      boolean doClose = !conf.useOnlyOneConnection || connName != "default";
      while (!conns.empty()){
        Connection conn = conns.pop();
        if (manageTrans) {
          if (rollback) {
            LOG.debug("ROLLBACK connection: " + conn);
            Statement stmt = conn.createStatement();
            stmt.execute("ROLLBACK");
            stmt.close();
          } else {
            LOG.debug("COMMIT connection: " + conn);
            Statement stmt = conn.createStatement();
            stmt.execute("COMMIT");
            stmt.close();
          }
        }
        if (doClose) {
          LOG.trace("Closing connection: " + conn);
          conn.close();
        }
      }
    }
  }

  public void closeQuery(Query query) {
    if(!exec.offline) {
      exec.conn.closeQuery(query);
    }
  }

  /**
   * Close one prepared statement
   */
  public void closePrepare(String name) {
    Query query = exec.conn.prepareSqls.get(name);
    closeQuery(query);
    exec.conn.prepareSqls.remove(name);
  }

  /**
   * Close all prepared statements
   */
  public void closePrepares() {
    Iterator iterator = exec.conn.prepareSqls.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry entry = (Map.Entry)iterator.next();
      Query query = (Query)entry.getValue();
      closeQuery(query);
      iterator.remove();
    }
  }
  
  /**
   * Register JARs, FILEs and CREATE TEMPORARY FUNCTION for UDF call
   */
  public void registerUdf() {
    if (udfRegistered) {
      return;
    }
    ArrayList<String> sql = new ArrayList<String>();
    String dir = Utils.getExecDir();
    String hplsqlJarName = "hplsql.jar";
    for(String jarName: new java.io.File(dir).list()) {
      if(jarName.startsWith("hive-hplsql") && jarName.endsWith(".jar")) {
        hplsqlJarName = jarName;
        break;
      }
    }
    sql.add("ADD JAR " + dir + hplsqlJarName);
    sql.add("ADD JAR " + dir + "antlr4-runtime-4.5.jar");
    if(!conf.getLocation().equals("")) {
      sql.add("ADD FILE " + conf.getLocation());
    } else {
      sql.add("ADD FILE " + dir + Conf.SITE_XML);
    }
    if (dotHplsqlrcExists) {
      sql.add("ADD FILE " + dir + Conf.DOT_HPLSQLRC);
    }
    if (hplsqlrcExists) {
      sql.add("ADD FILE " + dir + Conf.HPLSQLRC);
    }
    String lu = createLocalUdf();
    if (lu != null) {
      sql.add("ADD FILE " + lu);
    }
    sql.add("CREATE TEMPORARY FUNCTION hplsql AS 'org.trafodion.sql.udr.spsql.Udf'");
    exec.conn.addPreSql(exec.conf.defaultConnection, sql);
    udfRegistered = true;
  }

  /**
   * Initialize options
   */
  void initOptions() {
    Iterator<Map.Entry<String,String>> i = exec.conf.iterator();
    while (i.hasNext()) {
      Entry<String,String> item = (Entry<String,String>)i.next();
      String key = (String)item.getKey();
      String value = (String)item.getValue();
      setOption(key, value);
    }    
  }
  
  // TODO: Merge this with Conf::setOption
  public void setOption(String key, String value) {
    if (key == null || value == null || !key.startsWith("hplsql.")) {
      return;
    }
    else if (key.compareToIgnoreCase(Conf.CONN_DEFAULT) == 0) {
      exec.conf.defaultConnection = value;
    }
    else if (key.startsWith("hplsql.conn.init.")) {
      exec.conn.addConnectionInit(key.substring(17), value);
    }
    else if (key.startsWith(Conf.CONN_CONVERT)) {
      exec.conf.setConnectionConvert(key.substring(20), value);
    }
    else if (key.startsWith("hplsql.conn.")) {
      exec.conn.addConnection(key.substring(12), value);
    }
    else if (key.startsWith("hplsql.")) {
      exec.conf.setOption(key, value);
    }
  }

  /**
   * Set SQLCODE
   */
  public void setSqlCode(int sqlcode) {
    Long code = new Long(sqlcode);
    Var var = findVariable(SQLCODE);
    if (var != null) {
      var.setValue(code);
    }
    var = findVariable(ERRORCODE);
    if (var != null) {
      var.setValue(code);
    }
  }
  
  public void setSqlCode(Exception exception) {
    if (exception instanceof SQLException) {
      setSqlCode(((SQLException)exception).getErrorCode());
      setSqlState(((SQLException)exception).getSQLState());
    }
    else {
      setSqlCode(-1);
      setSqlState("02000");
    }    
  }
  
  /**
   * Set SQLSTATE
   */
  public void setSqlState(String sqlstate) {
    Var var = findVariable(SQLSTATE);
    if (var != null) {
      var.setValue(sqlstate);
    }
  }
    
  /**
   * Set HOSTCODE
   */
  public void setHostCode(int code) {
    Var var = findVariable(HOSTCODE);
    if (var != null) {
      var.setValue(new Long(code));
    }
  }
  
  /**
   * Set successful execution for SQL
   */
  public void setSqlSuccess() {
    setSqlCode(0);
    setSqlState("00000");
  }
  
  /**
   * Set SQL_NO_DATA as the result of SQL execution
   */
  public void setSqlNoData() {
    setSqlCode(100);
    setSqlState("01000");
  }
  
  /**
   * Compile and run HPL/SQL script 
   */
  public Integer run(String[] args) throws Exception {
    enterGlobalScope(); 
    if (init(args) != 0) {
      return 1;
    }
    Var result = run();
    if (result != null) {
      System.out.println(result.toString());
    }
    leaveScope();
    cleanup();
    printExceptions();    
    return getProgramReturnCode();
  }
  
  /**
   * Run already compiled HPL/SQL script (also used from Hive UDF)
   */
  public Var run() {
    if (tree == null) {
      return null;
    }
    if (execMain != null) {
      initRoutines = true;
      visit(tree);
      initRoutines = false;
      exec.function.execProc(execMain);
    }
    else {
      visit(tree);
    }
    if (!exec.stack.isEmpty()) {
      return exec.stackPop();
    }
    return null;
  }
  
  /**
   * Initialize PL/HQL
   */
  Integer init(String[] args) throws Exception {
    if (!parseArguments(args)) {
      return 1;
    }
    // specify the default log4j2 properties file.
    System.setProperty("log4j.configurationFile", "hive-log4j2.properties");
    conf = new Conf();
    conf.init();    
    conn = new Conn(this);
    meta = new Meta(this);
    initOptions();
    
    expr = new Expression(this);
    select = new Select(this);
    stmt = new Stmt(this);
    converter = new Converter(this);
        
    function = new Function(this);
    new FunctionDatetime(this).register(function);
    new FunctionMisc(this).register(function);
    new FunctionString(this).register(function);
    new FunctionOra(this).register(function);
    addVariable(new Var(ERRORCODE, Var.Type.BIGINT, 0L));
    addVariable(new Var(SQLCODE, Var.Type.BIGINT, 0L));
    addVariable(new Var(SQLSTATE, Var.Type.STRING, "00000"));
    addVariable(new Var(HOSTCODE, Var.Type.BIGINT, 0L)); 
    for (Map.Entry<String, String> v : arguments.getVars().entrySet()) {
      addVariable(new Var(v.getKey(), Var.Type.STRING, v.getValue()));
    }    
    InputStream input = null;
    if (execString != null) {
      input = new ByteArrayInputStream(execString.getBytes("UTF-8"));
    }
    else {
      input = new FileInputStream(execFile);
    }
    HplsqlLexer lexer = new HplsqlLexer(new ANTLRInputStream(input));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    HplsqlParser parser = new HplsqlParser(tokens);
    tree = parser.program();    
    if (trace) {
      System.err.println("Configuration file: " + conf.getLocation());
      System.err.println("Parser tree: " + tree.toStringTree(parser));
    }
    includeRcFile();    
    return 0;
  }
  
  /**
   * Parse command line arguments
   */
  boolean parseArguments(String[] args) {
    boolean parsed = arguments.parse(args);
    if (parsed && arguments.hasVersionOption()) {
      System.err.println(VERSION);
      return false;
    }
    if (!parsed || arguments.hasHelpOption() ||
      (arguments.getExecString() == null && arguments.getFileName() == null)) {
      arguments.printHelp();
      return false;
    }    
    execString = arguments.getExecString();
    execFile = arguments.getFileName();
    execMain = arguments.getMain();    
    if (arguments.hasTraceOption()) {
      trace = true;
    }
    if (arguments.hasOfflineOption()) {
      offline = true;
    }
    if (execString != null && execFile != null) {
      System.err.println("The '-e' and '-f' options cannot be specified simultaneously.");
      return false;
    }   
    return true;
  }
  
  /**
   * Include statements from .hplsqlrc and hplsql rc files
   */
  void includeRcFile() {
    if (includeFile(Conf.DOT_HPLSQLRC, false)) {
      dotHplsqlrcExists = true;
    }
    else {
      if (includeFile(Conf.HPLSQLRC, false)) {
        hplsqlrcExists = true;
      }
    }
    if (udfRun) {
      includeFile(Conf.HPLSQL_LOCALS_SQL, true);
    }
  }
  
  /**
   * Include statements from a file
   */
  boolean includeFile(String file, boolean showError) {
    try {
      String content = FileUtils.readFileToString(new java.io.File(file), "UTF-8");
      if (content != null && !content.isEmpty()) {
        if (trace) {
          LOG.trace(null, "INCLUDE CONTENT " + file + " (non-empty)");
        }
        new Exec(this).include(content);
        return true;
      }
    } 
    catch (Exception e) {
      if (showError) {
        LOG.error("INCLUDE file error: ", e);
      }
      signal(new SQLException("INCLUDE file '"
                              + file + "' error: " + e.getMessage(), e));
    } 
    return false;
  }
  
  /**
   * Execute statements from an include file
   */
  void include(String content) throws Exception {
    ParseTree tree = parseQuery(content);
    visit(tree);    
  }
  
  /**
   * Start executing HPL/SQL script
   */
  @Override 
  public Integer visitProgram(HplsqlParser.ProgramContext ctx) {
    Integer rc = visitChildren(ctx);
    return rc;
  }
  
  /**
   * Enter BEGIN-END block
   */
  @Override  
  public Integer visitBegin_end_block(HplsqlParser.Begin_end_blockContext ctx) { 
    enterScope(Scope.Type.BEGIN_END);
    Integer rc = visitChildren(ctx); 
    leaveScope();
    return rc;
  }
  
  /**
   * Free resources before exit
   */
  void cleanup() {
    for (Map.Entry<String, String> i : managedTables.entrySet()) {
      String sql = "DROP TABLE IF EXISTS " + i.getValue();
      Query query = executeSql(null, sql, exec.conf.defaultConnection);      
      closeQuery(query, exec.conf.defaultConnection);
      if (trace) {
        LOG.trace(null, sql);        
      }      
    }
  }
  
  /**
   * Output information about unhandled exceptions
   */
  void printExceptions() {
    while (!signals.empty()) {
      Signal sig = signals.pop();
      if (sig.type == Signal.Type.SQLEXCEPTION) {
        System.err.println("Unhandled exception in HPL/SQL");
      }
      if (sig.exception != null) {
        sig.exception.printStackTrace(); 
      }
      else if (sig.value != null) {
        System.err.println(sig.value);
      }
    }
  } 
  
  /**
   * Get the program return code
   */
  Integer getProgramReturnCode() {
    Integer rc = 0;
    if (!signals.empty()) {
      Signal sig = signals.pop();
      if ((sig.type == Signal.Type.LEAVE_PROGRAM || sig.type == Signal.Type.LEAVE_ROUTINE) && 
        sig.value != null) {
        try {
          rc = Integer.parseInt(sig.value);
        }
        catch(NumberFormatException e) {
          rc = 1;
        }
      }
    }
    return rc;
  }

  /**
   * Executing a statement
   */
  @Override 
  public Integer visitStmt(HplsqlParser.StmtContext ctx) {
    if (ctx.semicolon_stmt() != null || ctx.null_stmt() != null) {
      return 0;
    }
    if (initRoutines && ctx.create_procedure_stmt() == null && ctx.create_function_stmt() == null) {
      return 0;
    }
    if (exec.resignal) {
      if (exec.currentScope != exec.currentHandlerScope.parent) {
        return 0;
      }
      exec.resignal = false;
    }
    if (!exec.signals.empty() && exec.conf.onError != OnError.SETERROR) {
      if (!runContinueHandler()) {
        return 0;
      }
      // RESIGNAL in continue handler
      if (exec.resignal) {
        return 0;
      }
    }
    Var prev = stackPop();
    if (prev != null && prev.value != null) {
      System.out.println(prev.toString());
    }
    int rc = 0;
    try {
      rc = visitChildren(ctx);
    } catch (Exception e) {
      rc = 1;
      exec.signal(e);
    }
    if (!exec.resignal &&
        !exec.signals.empty() &&
        exec.conf.onError != OnError.SETERROR) {
      runContinueHandler();
    }
    return rc;
  }
  
  /**
   * Executing or building SELECT statement
   */
  @Override 
  public Integer visitSelect_stmt(HplsqlParser.Select_stmtContext ctx) { 
    return exec.select.select(ctx);
  }
  
  @Override 
  public Integer visitCte_select_stmt(HplsqlParser.Cte_select_stmtContext ctx) { 
    return exec.select.cte(ctx); 
  }

  @Override 
  public Integer visitFullselect_stmt(HplsqlParser.Fullselect_stmtContext ctx) { 
    return exec.select.fullselect(ctx);
  }
  
  @Override 
  public Integer visitSubselect_stmt(HplsqlParser.Subselect_stmtContext ctx) { 
    return exec.select.subselect(ctx);
  }  
  
  @Override 
  public Integer visitSelect_list(HplsqlParser.Select_listContext ctx) { 
    return exec.select.selectList(ctx); 
  }
  
  @Override 
  public Integer visitFrom_clause(HplsqlParser.From_clauseContext ctx) { 
    return exec.select.from(ctx); 
  }
  
  @Override 
  public Integer visitFrom_table_name_clause(HplsqlParser.From_table_name_clauseContext ctx) { 
    return exec.select.fromTable(ctx); 
  }
  
  @Override 
  public Integer visitFrom_subselect_clause(HplsqlParser.From_subselect_clauseContext ctx) { 
    return exec.select.fromSubselect(ctx); 
  }
  
  @Override 
  public Integer visitFrom_join_clause(HplsqlParser.From_join_clauseContext ctx) { 
    return exec.select.fromJoin(ctx); 
  }
  
  @Override 
  public Integer visitFrom_table_values_clause(HplsqlParser.From_table_values_clauseContext ctx) { 
    return exec.select.fromTableValues(ctx); 
  }
  
  @Override 
  public Integer visitWhere_clause(HplsqlParser.Where_clauseContext ctx) { 
    return exec.select.where(ctx); 
  }  
  
  @Override 
  public Integer visitSelect_options_item(HplsqlParser.Select_options_itemContext ctx) { 
    return exec.select.option(ctx); 
  }

  @Override
  public Integer visitGroup_by_clause(HplsqlParser.Group_by_clauseContext ctx) {
    return exec.select.groupBy(ctx);
  }

  @Override
  public Integer visitOrder_by_clause(HplsqlParser.Order_by_clauseContext ctx) {
    return exec.select.orderBy(ctx);
  }
  
  /**
   * Column name
   */
  @Override 
  public Integer visitColumn_name(HplsqlParser.Column_nameContext ctx) {
    stackPush(meta.normalizeIdentifierPart(ctx.getText()));
    return 0; 
  }
  
  /**
   * Table name
   */
  @Override 
  public Integer visitTable_name(HplsqlParser.Table_nameContext ctx) {
    String name = ctx.getText();
    String nameUp = name.toUpperCase();
    String nameNorm = meta.normalizeObjectIdentifier(name);
    String actualName = exec.managedTables.get(nameUp);
    String conn = exec.objectConnMap.get(nameUp);
    if (conn == null) {
      conn = conf.defaultConnection;
    }
    stmtConnList.add(conn);    
    if (actualName != null) {
      stackPush(actualName);
      return 0;
    }
    actualName = exec.objectMap.get(nameUp);
    if (actualName != null) {
      stackPush(actualName);
      return 0;
    }
    stackPush(nameNorm);
    return 0; 
  }

  /**
   * SQL INSERT statement
   */
  @Override 
  public Integer visitInsert_stmt(HplsqlParser.Insert_stmtContext ctx) { 
    return exec.stmt.insert(ctx); 
  }
  
  /**
   * INSERT DIRECTORY statement
   */
  @Override 
  public Integer visitInsert_directory_stmt(HplsqlParser.Insert_directory_stmtContext ctx) { 
    return exec.stmt.insertDirectory(ctx); 
  }
    
  /**
   * INVALIDATE statement
   */
  @Override
  public Integer visitInvalidate_stmt(HplsqlParser.Invalidate_stmtContext ctx) {
    if (ctx.T_ALL() != null) {
      if (ctx.T_PROCEDURE() != null) {
        LOG.trace(ctx, "INVALIDATE PROCEDURE ALL");
        exec.function.clearProcs();
      } else if (ctx.T_FUNCTION() != null) {
        LOG.trace(ctx, "INVALIDATE FUNCTION ALL");
        exec.function.clearFuncs();
      } else if (ctx.T_PACKAGE() != null) {
        LOG.trace(ctx, "INVALIDATE PACKAGE ALL");
        exec.packages.clear();
      } else {
        LOG.trace(ctx, "INVALIDATE ALL");
        exec.function.clear();
        exec.packages.clear();
      }
      return 0;
    }
    for (int i=0; i<ctx.ident().size(); i++) {
      String name = qualify(ctx.ident(i).getText().toUpperCase());
      if (ctx.T_PROCEDURE() != null) {
        LOG.trace(ctx, "INVALIDATE PROCEDURE " + name);
        exec.function.removeProc(name);
      } else if (ctx.T_FUNCTION() != null) {
        LOG.trace(ctx, "INVALIDATE FUNCTION " + name);
        exec.function.removeFunc(name);
      } else if (ctx.T_PACKAGE() != null) {
        LOG.trace(ctx, "INVALIDATE PACKAGE " + name);
        exec.packages.remove(name);
      } else if (ctx.T_TRIGGER() != null) {
	LOG.trace(ctx, "INVALIDATE TRIGGER " + name);
	exec.function.removeTrigger(name);
      } else {
        LOG.trace(ctx, "INVALIDATE " + name);
        name = meta.removeDoubleQuote(name);
        ArrayList<String> qualified = meta.splitIdentifierToTwoParts(name);
        if (qualified != null) {
          exec.packages.remove(qualified.get(0));
        }
        exec.function.remove(name);
        exec.packages.remove(name);
      }
    }
    return 0;
  }

  /**
   * EXCEPTION block
   */
  @Override 
  public Integer visitException_block_item(HplsqlParser.Exception_block_itemContext ctx) { 
    if (exec.signals.empty()) {
      return 0;
    }
    if (exec.conf.onError == OnError.SETERROR || exec.conf.onError == OnError.STOP) {
      exec.signals.pop();
      return 0;
    }
    if (ctx.L_ID().toString().equalsIgnoreCase("OTHERS") && exec.signals.peek().type != Signal.Type.LEAVE_ROUTINE) {
      LOG.trace(ctx, "EXCEPTION HANDLER");
      currentSignal = exec.signals.pop();
      enterScope(Scope.Type.HANDLER);
      visit(ctx.block());
      leaveScope();
      currentSignal = null;
    }
    return 0;
  }
    
  /**
   * DECLARE variable statement
   */
  @Override
  public Integer visitDeclare_var_item(HplsqlParser.Declare_var_itemContext ctx) { 
    String type = null;
    Column col = null;
    Row row = null;
    String len = null;
    String scale = null;
    Var default_ = null;
    boolean nullable = true;
    if (ctx.dtype().T_ROWTYPE() != null) {
      row = meta.getRowDataType(ctx, exec.conf.defaultConnection, ctx.dtype().ident().getText());
      if (row == null) {
        type = Var.DERIVED_ROWTYPE;
      }
    }
    else {
      if (ctx.dtype().T_TYPE() != null) {
        col = meta.getColumnDataType(ctx,
                                     exec.conf.defaultConnection,
                                     ctx.dtype().ident().getText());
        if (col == null) {
          type = Var.DERIVED_TYPE; 
        }
        else {
          type = col.type;
          len = String.valueOf(col.len);
          scale = String.valueOf(col.scale);
        }
      }
      else {
        type = getFormattedText(ctx.dtype());
      }
      if (ctx.dtype_len() != null) {
        len = ctx.dtype_len().L_INT(0).getText();
        if (ctx.dtype_len().L_INT(1) != null) {
          scale = ctx.dtype_len().L_INT(1).getText();
        }
      }    
      if (ctx.dtype_default() != null) {
        default_ = evalPop(ctx.dtype_default());
      }
      for (int i=0; i< ctx.dtype_attr().size(); i++) {
        if (ctx.dtype_attr(i).T_NOT() != null
            && ctx.dtype_attr(i).T_NULL() != null) {
          nullable = false;
        }
      }
    }
	  int cnt = ctx.ident().size();        // Number of variables declared with the same data type and default
	  for (int i = 0; i < cnt; i++) {  	    
	    String name = ctx.ident(i).getText();
	    if (row == null) {
	      Var var = new Var(name, type, len, scale, default_);	     
              var.setNullable(nullable);
	      exec.addVariable(var);		
	      if (ctx.T_CONSTANT() != null) {
	        var.setConstant(true);
	      }
	      if (trace) {
	        if (default_ != null) {
	          LOG.trace(ctx, "DECLARE " + name + " " + type + " = " + var.toSqlString());
	        }
	        else {
	          LOG.trace(ctx, "DECLARE " + name + " " + type);
	        }
	      }
	    }
	    else {
	      exec.addVariable(new Var(name, row));
	      if (trace) {
          LOG.trace(ctx, "DECLARE " + name + " " + ctx.dtype().getText());
        }
	    }
	  }	
	  return 0;
  }
  
  /**
   * ALLOCATE CURSOR statement
   */
  @Override 
  public Integer visitAllocate_cursor_stmt(HplsqlParser.Allocate_cursor_stmtContext ctx) { 
    return exec.stmt.allocateCursor(ctx); 
  }

  /**
   * ASSOCIATE LOCATOR statement
   */
  @Override 
  public Integer visitAssociate_locator_stmt(HplsqlParser.Associate_locator_stmtContext ctx) { 
    return exec.stmt.associateLocator(ctx); 
  }

  /**
   * DECLARE cursor statement
   */
  @Override 
  public Integer visitDeclare_cursor_item(HplsqlParser.Declare_cursor_itemContext ctx) { 
    return exec.stmt.declareCursor(ctx); 
  }
  
  /**
   * DESCRIBE statement
   */
  @Override 
  public Integer visitDescribe_stmt(HplsqlParser.Describe_stmtContext ctx) {
    return exec.stmt.describe(ctx);
  }
  
  /**
   * DROP statement
   */
  @Override 
  public Integer visitDrop_stmt(HplsqlParser.Drop_stmtContext ctx) { 
    return exec.stmt.drop(ctx); 
  }
  
  /**
   * OPEN cursor statement
   */
  @Override 
  public Integer visitOpen_stmt(HplsqlParser.Open_stmtContext ctx) { 
    return exec.stmt.open(ctx); 
  }  
  
  /**
   * FETCH cursor statement
   */
  @Override 
  public Integer visitFetch_stmt(HplsqlParser.Fetch_stmtContext ctx) { 
    return exec.stmt.fetch(ctx);
  }

  /**
   * CLOSE cursor statement
   */
  @Override 
  public Integer visitClose_stmt(HplsqlParser.Close_stmtContext ctx) { 
    return exec.stmt.close(ctx); 
  }

  /**
   * CLOSE PREPARE statement
   */
  @Override 
  public Integer visitClose_prepare_stmt(HplsqlParser.Close_prepare_stmtContext ctx) { 
    return exec.stmt.closePrepare(ctx); 
  }
  
  /**
   * CMP statement
   */
  @Override 
  public Integer visitCmp_stmt(HplsqlParser.Cmp_stmtContext ctx) { 
    return new Cmp(exec).run(ctx); 
  }
  
  /**
   * COPY statement
   */
  @Override 
  public Integer visitCopy_stmt(HplsqlParser.Copy_stmtContext ctx) { 
    return new Copy(exec).run(ctx); 
  }
  
  /**
   * COPY FROM FTP statement
   */
  @Override 
  public Integer visitCopy_from_ftp_stmt(HplsqlParser.Copy_from_ftp_stmtContext ctx) { 
    return new Ftp(exec).run(ctx); 
  }
  
  /**
   * COPY FROM LOCAL statement
   */
  @Override 
  public Integer visitCopy_from_local_stmt(HplsqlParser.Copy_from_local_stmtContext ctx) { 
    return new Copy(exec).runFromLocal(ctx); 
  }
  
  /**
   * DECLARE HANDLER statement
   */
  @Override 
  public Integer visitDeclare_handler_item(HplsqlParser.Declare_handler_itemContext ctx) {
    LOG.trace(ctx, "DECLARE HANDLER");
    Handler.ExecType execType = Handler.ExecType.EXIT;
    Signal.Type type = Signal.Type.SQLEXCEPTION;
    String value = null;
    if (ctx.T_CONTINUE() != null) {
      execType = Handler.ExecType.CONTINUE;
    }    
    if (ctx.ident() != null) {
      type = Signal.Type.USERDEFINED;
      value = ctx.ident().getText();
    }
    else if (ctx.T_NOT() != null && ctx.T_FOUND() != null) {
      type = Signal.Type.NOTFOUND;
    }
    addHandler(new Handler(execType, type, value, exec.currentScope, ctx));
    return 0; 
  }
  
  /**
   * DECLARE CONDITION
   */
  @Override 
  public Integer visitDeclare_condition_item(HplsqlParser.Declare_condition_itemContext ctx) { 
    return 0; 
  }
  
  /**
   * DECLARE TEMPORARY TABLE statement 
   */
  @Override 
  public Integer visitDeclare_temporary_table_item(HplsqlParser.Declare_temporary_table_itemContext ctx) { 
    return exec.stmt.declareTemporaryTable(ctx); 
  }
  
  /**
   * CREATE TABLE statement
   */
  @Override 
  public Integer visitCreate_table_stmt(HplsqlParser.Create_table_stmtContext ctx) { 
    return exec.stmt.createTable(ctx); 
  } 
  
  @Override 
  public Integer visitCreate_table_options_hive_item(HplsqlParser.Create_table_options_hive_itemContext ctx) { 
    return exec.stmt.createTableHiveOptions(ctx); 
  }
  
  @Override 
  public Integer visitCreate_table_options_ora_item(HplsqlParser.Create_table_options_ora_itemContext ctx) { 
    return 0; 
  }
  
  @Override 
  public Integer visitCreate_table_options_td_item(HplsqlParser.Create_table_options_td_itemContext ctx) { 
    return 0; 
  }
  
  @Override 
  public Integer visitCreate_table_options_mssql_item(HplsqlParser.Create_table_options_mssql_itemContext ctx) { 
    return 0; 
  }
  
  @Override 
  public Integer visitCreate_table_options_db2_item(HplsqlParser.Create_table_options_db2_itemContext ctx) { 
    return 0; 
  }
  
  @Override 
  public Integer visitCreate_table_options_mysql_item(HplsqlParser.Create_table_options_mysql_itemContext ctx) { 
    return exec.stmt.createTableMysqlOptions(ctx); 
  }
  
  /**
   * CREATE (GLOBAL | LOCAL) TEMPORARY | VOLATILE TABLE statement 
   */
  @Override 
  public Integer visitCreate_temporary_table_stmt(HplsqlParser.Create_temporary_table_stmtContext ctx) {
    return exec.stmt.createTemporaryTable(ctx);
  }
  
  /**
   * ALTER TABLE statement
   */
  @Override 
  public Integer visitAlter_table_stmt(HplsqlParser.Alter_table_stmtContext ctx) { 
    return 0; 
  } 
  
  /**
   * CREATE DATABASE | SCHEMA statement
   */
  @Override 
  public Integer visitCreate_database_stmt(HplsqlParser.Create_database_stmtContext ctx) {
    return exec.stmt.createDatabase(ctx);
  }
  
  /**
   * CREATE FUNCTION statement
   */
  @Override 
  public Integer visitCreate_function_stmt(HplsqlParser.Create_function_stmtContext ctx) {
    exec.function.addUserFunction(ctx);
    return 0; 
  }

  /**
   * CREATE PACKAGE specification statement
   */
  @Override 
  public Integer visitCreate_package_stmt(HplsqlParser.Create_package_stmtContext ctx) { 
    String name = ctx.ident(0).getText().toUpperCase();
    exec.currentPackageDecl = new Package(name, exec);    
    exec.packages.put(name, exec.currentPackageDecl);
    LOG.trace(ctx, "CREATE PACKAGE");
    exec.currentPackageDecl.createSpecification(ctx);
    exec.currentPackageDecl = null;
    return 0; 
  }

  /**
   * CREATE PACKAGE body statement
   */
  @Override 
  public Integer visitCreate_package_body_stmt(HplsqlParser.Create_package_body_stmtContext ctx) { 
    String name = ctx.ident(0).getText().toUpperCase();
    exec.currentPackageDecl = exec.packages.get(name);
    if (exec.currentPackageDecl == null) {
      exec.currentPackageDecl = new Package(name, exec);
      exec.currentPackageDecl.setAllMembersPublic(true);
      exec.packages.put(name, exec.currentPackageDecl);
    }
    LOG.trace(ctx, "CREATE PACKAGE BODY");
    try {
      exec.currentPackageDecl.createBody(ctx);
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage());
    } finally {
      exec.currentPackageDecl = null;
    }
    return 0;
  }

  /**
   * CREATE PROCEDURE statement
   */
  @Override 
  public Integer visitCreate_procedure_stmt(HplsqlParser.Create_procedure_stmtContext ctx) {
    exec.function.addUserProcedure(ctx);
    return 0; 
  }

  /**
   * CREATE TRIGGER statement
   */
  @Override
  public Integer visitCreate_trigger_stmt(HplsqlParser.Create_trigger_stmtContext ctx) {
      LOG.trace("insert into visitCreate_trigger_stmt function");
    if (ctx.T_BEFORE() != null) {
      exec.function.addUserbeTrigger(ctx);
    } else {
      exec.function.addUserafTrigger(ctx);
    }
    return 0;
  }
  
  /**
   * CREATE INDEX statement
   */
  @Override 
  public Integer visitCreate_index_stmt(HplsqlParser.Create_index_stmtContext ctx) { 
    return 0; 
  }
  
  /**
   * Add functions and procedures defined in the current script
   */
  void addLocalUdf(ParserRuleContext ctx) {
    if (exec == this) {                              
      localUdf.append(exec.getFormattedText(ctx));
      localUdf.append("\n");
    }
  }
  
  /**
   * Save local functions and procedures to a file (will be added to the distributed cache) 
   */
  String createLocalUdf() {
    if(localUdf.length() == 0) {
      return null;
    }
    try {
      String file = System.getProperty("user.dir") + "/" + Conf.HPLSQL_LOCALS_SQL; 
      PrintWriter writer = new PrintWriter(file, "UTF-8");
      writer.print(localUdf);
      writer.close();
      return file;
    } 
    catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }
      
  /**
   * Update assignment
   */
  @Override
  public Integer visitUpdate_assignment(HplsqlParser.Update_assignmentContext ctx) {
    StringBuilder sb = new StringBuilder();
    boolean oldBuildSql = exec.buildSql;
    exec.buildSql = true;
    for (int i=0; i<ctx.assignment_stmt_item().size(); i++) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(evalPop(ctx.assignment_stmt_item(i)).toString());
    }
    exec.buildSql = oldBuildSql;
    stackPush(sb.toString());
    return 0;
  }

  /**
   * Assignment statement for single value
   */
  @Override 
  public Integer visitAssignment_stmt_single_item(HplsqlParser.Assignment_stmt_single_itemContext ctx) { 
    String name = ctx.ident().getText();
    visit(ctx.expr());    
    if (buildSql) {
      stackPush(name + " = " + stackPop().toString());
      return 0;
    }
    Var var = setVariable(name);
    if (trace) {
      LOG.trace(ctx, "SET " + name + " = " + var.toSqlString());      
    }    
    return 0;
  }  

  /**
   * Assignment statement for multiple values
   */
  @Override 
  public Integer visitAssignment_stmt_multiple_item(HplsqlParser.Assignment_stmt_multiple_itemContext ctx) { 
    if (buildSql) {
      StringBuilder sb = new StringBuilder();
      sb.append("(");
      int cnt = ctx.ident().size();
      int ecnt = ctx.expr().size();
      for (int i = 0; i < cnt; i++) {
        String name = ctx.ident(i).getText();
        if (i>0) {
          sb.append(", ");
        }
        sb.append(name);
      }
      sb.append(") = (");
      for (int i = 0; i < ecnt; i++) {
        if (i>0) {
          sb.append(", ");
        }
        sb.append(evalPop(ctx.expr(i)).toString());
      }
      sb.append(")");
      stackPush(sb.toString());
      return 0;
    }
    int cnt = ctx.ident().size();
    int ecnt = ctx.expr().size();    
    for (int i = 0; i < cnt; i++) {
      String name = ctx.ident(i).getText();      
      if (i < ecnt) {
        visit(ctx.expr(i));
        Var var = setVariable(name);        
        if (trace) {
          LOG.trace(ctx, "SET " + name + " = " + var.toString());      
        } 
      }      
    }    
    return 0; 
  }
  
  /**
   * Assignment from SELECT statement 
   */
  @Override 
  public Integer visitAssignment_stmt_select_item(HplsqlParser.Assignment_stmt_select_itemContext ctx) { 
    if (buildSql) {
      StringBuilder sb = new StringBuilder();
      sb.append("(");
      int cnt = ctx.ident().size();
      for (int i = 0; i < cnt; i++) {
        String name = ctx.ident(i).getText();
        if (i>0) {
          sb.append(", ");
        }
        sb.append(name);
      }
      sb.append(") = (");
      sb.append(evalPop(ctx.select_stmt()).toString());
      sb.append(")");
      stackPush(sb.toString());
      return 0;
    }
    return stmt.assignFromSelect(ctx); 
  }
  
  /**
   * Evaluate an expression
   */
  @Override 
  public Integer visitExpr(HplsqlParser.ExprContext ctx) { 
    if (exec.buildSql) {
      exec.expr.execSql(ctx);
    }
    else {
      exec.expr.exec(ctx);
    }
    return 0;
  }

  /**
   * Evaluate a boolean expression
   */
  @Override 
  public Integer visitBool_expr(HplsqlParser.Bool_exprContext ctx) {
    if (exec.buildSql) {
      exec.expr.execBoolSql(ctx);
    }
    else {
      exec.expr.execBool(ctx);
    }
    return 0; 
  }
  
  @Override 
  public Integer visitBool_expr_binary(HplsqlParser.Bool_expr_binaryContext ctx) {
    if (exec.buildSql) {
      exec.expr.execBoolBinarySql(ctx);
    }
    else {
      exec.expr.execBoolBinary(ctx);
    }
    return 0; 
  }
  
  @Override 
  public Integer visitBool_expr_unary(HplsqlParser.Bool_expr_unaryContext ctx) {
    if (exec.buildSql) {
      exec.expr.execBoolUnarySql(ctx);
    }
    else {
      exec.expr.execBoolUnary(ctx);
    }
    return 0; 
  }
   
  /**
   * Static SELECT statement (i.e. unquoted) or expression
   */
  @Override 
  public Integer visitExpr_select(HplsqlParser.Expr_selectContext ctx) {
    if (ctx.select_stmt() != null) {
      stackPush(new Var(evalPop(ctx.select_stmt())));
    }
    else {
      visit(ctx.expr());
    }
    return 0; 
  }
  
  /**
   * File path (unquoted) or expression
   */
  @Override 
  public Integer visitExpr_file(HplsqlParser.Expr_fileContext ctx) {
    if (ctx.file_name() != null) {
      stackPush(new Var(ctx.file_name().getText()));
    }
    else {
      visit(ctx.expr());
    }
    return 0; 
  }
  
  /**
   * Cursor attribute %ISOPEN, %FOUND and %NOTFOUND
   */
  @Override 
  public Integer visitExpr_cursor_attribute(HplsqlParser.Expr_cursor_attributeContext ctx) {
    exec.expr.execCursorAttribute(ctx);
    return 0; 
  }
    
  /**
   * Function call
   */
  @Override 
  public Integer visitExpr_func(HplsqlParser.Expr_funcContext ctx) {
    String name = ""; 
    if (ctx.ident() == null){
      name = ctx.reserved_func().getText();
    }
    else {
      name = ctx.ident().getText();
    }
	
    if (exec.buildSql) {
      exec.function.execSql(name, ctx.expr_func_params());
    }
    else {
      Package packCallContext = exec.getPackageCallContext();
      ArrayList<String> qualified = exec.meta.splitIdentifierToTwoParts(name);
      boolean executed = false;
      if (qualified != null) {
        Package pack = findPackage(qualified.get(0));
        if (pack != null) {        
          executed = pack.execFunc(qualified.get(1), ctx.expr_func_params());
        }
      }
      if (!executed && packCallContext != null) {
        executed = packCallContext.execFunc(name, ctx.expr_func_params());
      }
      if (!executed) {        
        exec.function.exec(name, ctx.expr_func_params());
      }
    }
    return 0;
  }
  
  /**
   * Aggregate or window function call
   */
  @Override 
  public Integer visitExpr_agg_window_func(HplsqlParser.Expr_agg_window_funcContext ctx) {
    exec.function.execAggWindowSql(ctx);
    return 0; 
  }
  
  /**
   * Function with specific syntax
   */
  @Override 
  public Integer visitExpr_spec_func(HplsqlParser.Expr_spec_funcContext ctx) { 
    if (exec.buildSql) {
      exec.function.specExecSql(ctx);
    }
    else {
      exec.function.specExec(ctx);
    }
    return 0;
  }  
  
  /**
   * INCLUDE statement
   */
  @Override 
  public Integer visitInclude_stmt(@NotNull HplsqlParser.Include_stmtContext ctx) {
    return exec.stmt.include(ctx); 
  }
    
  /**
   * IF statement (PL/SQL syntax)
   */
  @Override 
  public Integer visitIf_plsql_stmt(HplsqlParser.If_plsql_stmtContext ctx) { 
    return exec.stmt.ifPlsql(ctx); 
  }

  /**
   * IF statement (Transact-SQL syntax)
   */
  @Override  
  public Integer visitIf_tsql_stmt(HplsqlParser.If_tsql_stmtContext ctx) { 
    return exec.stmt.ifTsql(ctx); 
  }
  
  /**
   * IF statement (BTEQ syntax)
   */
  @Override  
  public Integer visitIf_bteq_stmt(HplsqlParser.If_bteq_stmtContext ctx) { 
    return exec.stmt.ifBteq(ctx); 
  }
  
  /**
   * USE statement
   */
  @Override 
  public Integer visitUse_stmt(HplsqlParser.Use_stmtContext ctx) { 
    return exec.stmt.use(ctx); 
  }
  
  /** 
   * VALUES statement
   */
  @Override 
  public Integer visitValues_into_stmt(HplsqlParser.Values_into_stmtContext ctx) { 
    return exec.stmt.values(ctx); 
  }  
  
  /**
   * WHILE statement
   */
  @Override 
  public Integer visitWhile_stmt(HplsqlParser.While_stmtContext ctx) { 
    return exec.stmt.while_(ctx); 
  }  

  /**
   * LOOP statement
   */
  @Override
  public Integer visitLoop_stmt(HplsqlParser.Loop_stmtContext ctx) {
    return exec.stmt.loop_(ctx);
  }

  /**
   * FOR cursor statement
   */
  @Override 
  public Integer visitFor_cursor_stmt(HplsqlParser.For_cursor_stmtContext ctx) { 
    return exec.stmt.forCursor(ctx); 
  }
  
  /**
   * FOR (integer range) statement
   */
  @Override 
  public Integer visitFor_range_stmt(HplsqlParser.For_range_stmtContext ctx) { 
    return exec.stmt.forRange(ctx); 
  }  

  /**
   * EXEC, EXECUTE and EXECUTE IMMEDIATE statement to execute dynamic SQL
   */
  @Override 
  public Integer visitExec_stmt(HplsqlParser.Exec_stmtContext ctx) { 
    Integer rc = exec.stmt.exec(ctx);
    return rc;
  }
  
  /**
   * CALL statement
   */
  @Override 
  public Integer visitCall_stmt(HplsqlParser.Call_stmtContext ctx) {
    String name = ctx.ident().getText();
    Package packCallContext = exec.getPackageCallContext();
    ArrayList<String> qualified = exec.meta.splitIdentifierToTwoParts(name);
    boolean executed = false;
    if (qualified != null) {
      Package pack = findPackage(qualified.get(0));
      if (pack != null) {        
        executed = pack.execProc(qualified.get(1), ctx.expr_func_params(), true /*trace error if not exists*/);
      }
    }
    if (!executed && packCallContext != null) {
      executed = packCallContext.execProc(name, ctx.expr_func_params(), false /*trace error if not exists*/);
    }
    if (!executed) {        
      exec.function.execProc(name, ctx.expr_func_params(), ctx);
    }
    return 0;
  }
    
  /**
   * BEGIN TRANSACTION statement (BEGIN TRANSACTION or BEGIN WORK)
   */
  @Override
  public Integer visitBegin_transaction_stmt(HplsqlParser.Begin_transaction_stmtContext ctx) {
    if (conf.transactionCompatible) {
      // IF NO TRANSACTION REQUIRED is specified, we will
      // automatically start transaction at the beginning of a
      // procedure, and also start transaction right after
      // COMMIT/ROLLBACK, so basically user should not issue BEGIN
      // WORK/TRANSACTION in procedure. But if they do, we will commit
      // the transaction and start a new one.
      if (exec.stmt.commit(null) != 0) {
        return 1;
      }
    }
    return exec.stmt.beginTransaction(ctx);
  }

  /**
   * COMMIT statement
   */
  @Override
  public Integer visitCommit_stmt(HplsqlParser.Commit_stmtContext ctx) {
    if (exec.stmt.commit(ctx) != 0) {
      return 1;
    }
    if (conf.transactionCompatible) {
      return exec.stmt.beginTransaction(null);
    }
    return 0;
  }

  /**
   * ROLLBACK statement
   */
  @Override
  public Integer visitRollback_stmt(HplsqlParser.Rollback_stmtContext ctx) {
    if (exec.stmt.rollback(ctx) != 0) {
      return 1;
    }
    if (conf.transactionCompatible) {
      return exec.stmt.beginTransaction(null);
    }
    return 0;
  }

  /**
   * EXIT statement (leave the specified loop with a condition)
   */
  @Override 
  public Integer visitExit_stmt(HplsqlParser.Exit_stmtContext ctx) { 
    return exec.stmt.exit(ctx); 
  }

  /**
   * BREAK statement (leave the innermost loop unconditionally)
   */
  @Override 
  public Integer visitBreak_stmt(HplsqlParser.Break_stmtContext ctx) { 
    return exec.stmt.break_(ctx);
  }
  
  /**
   * LEAVE statement (leave the specified loop unconditionally)
   */
  @Override 
  public Integer visitLeave_stmt(HplsqlParser.Leave_stmtContext ctx) { 
    return exec.stmt.leave(ctx); 
  }
      
  /** 
   * PRINT statement 
   */
  @Override 
  public Integer visitPrint_stmt(HplsqlParser.Print_stmtContext ctx) { 
	  return exec.stmt.print(ctx); 
  }

  /** 
   * PREPARE statement 
   */
  @Override 
  public Integer visitPrepare_stmt(HplsqlParser.Prepare_stmtContext ctx) { 
    return exec.stmt.prepare(ctx); 
  }
  
  /** 
   * QUIT statement 
   */
  @Override 
  public Integer visitQuit_stmt(HplsqlParser.Quit_stmtContext ctx) { 
    return exec.stmt.quit(ctx); 
  }
  
  /**
   * SIGNAL statement
   */
  @Override 
  public Integer visitSignal_stmt(HplsqlParser.Signal_stmtContext ctx) { 
    return exec.stmt.signal(ctx); 
  }
  
  /**
   * SUMMARY statement
   */
  @Override 
  public Integer visitSummary_stmt(HplsqlParser.Summary_stmtContext ctx) { 
    return exec.stmt.summary(ctx); 
  }  
  
  /**
   * RESIGNAL statement
   */
  @Override 
  public Integer visitResignal_stmt(HplsqlParser.Resignal_stmtContext ctx) {  
    return exec.stmt.resignal(ctx); 
  }
    
  /**
   * RETURN statement
   */
  @Override 
  public Integer visitReturn_stmt(HplsqlParser.Return_stmtContext ctx) {
    return exec.stmt.return_(ctx); 
  }  
  
  /** 
   * SET session options
   */
  @Override 
  public Integer visitSet_current_schema_option(HplsqlParser.Set_current_schema_optionContext ctx) { 
    return exec.stmt.setCurrentSchema(ctx); 
  }
  
  /**
   * TRUNCATE statement
   */
  @Override 
  public Integer visitTruncate_stmt(HplsqlParser.Truncate_stmtContext ctx) { 
    return exec.stmt.truncate(ctx); 
  }
  
  /**
   * MAP OBJECT statement
   */
  @Override 
  public Integer visitMap_object_stmt(HplsqlParser.Map_object_stmtContext ctx) {
    String source = evalPop(ctx.expr(0)).toString();
    String target = null;
    String conn = null;
    if (ctx.T_TO() != null) {
      target = evalPop(ctx.expr(1)).toString();
      exec.objectMap.put(source.toUpperCase(), target);  
    }
    if (ctx.T_AT() != null) {
      if (ctx.T_TO() == null) {
        conn = evalPop(ctx.expr(1)).toString();
      }
      else {
        conn = evalPop(ctx.expr(2)).toString();
      }
      exec.objectConnMap.put(source.toUpperCase(), conn);      
    }
    if (trace) {
      String log = "MAP OBJECT " + source;
      if (target != null) {
        log += " AS " + target;
      }
      if (conn != null) {
        log += " AT " + conn;
      }
      LOG.trace(ctx, log);
    }
    return 0; 
  }
  
  /**
   * UPDATE statement
   */
  @Override 
  public Integer visitUpdate_stmt(HplsqlParser.Update_stmtContext ctx) { 
    return stmt.update(ctx); 
  }
  
  /**
   * DELETE statement
   */
  @Override 
  public Integer visitDelete_stmt(HplsqlParser.Delete_stmtContext ctx) { 
    return stmt.delete(ctx); 
  }
  
  /**
   * MERGE statement
   */
  @Override 
  public Integer visitMerge_stmt(HplsqlParser.Merge_stmtContext ctx) { 
    return stmt.merge(ctx); 
  }
    
  /**
   * Run a Hive command line
   */
  @Override 
  public Integer visitHive(@NotNull HplsqlParser.HiveContext ctx) { 
    LOG.trace(ctx, "HIVE");      
    ArrayList<String> cmd = new ArrayList<String>();
    cmd.add("hive");    
    Var params = new Var(Var.Type.STRINGLIST, cmd);
    stackPush(params);
    visitChildren(ctx);
    stackPop();    
    try { 
      String[] cmdarr = new String[cmd.size()];
      cmd.toArray(cmdarr);
      if(trace) {
        LOG.trace(ctx, "HIVE Parameters: " + Utils.toString(cmdarr, ' '));      
      }     
      if (!offline) {
        Process p = Runtime.getRuntime().exec(cmdarr);      
        new StreamGobbler(p.getInputStream()).start();
        new StreamGobbler(p.getErrorStream()).start(); 
        int rc = p.waitFor();      
        if (trace) {
          LOG.trace(ctx, "HIVE Process exit code: " + rc);      
        } 
      }
    } catch (Exception e) {
      setSqlCode(-1);
      signal(Signal.Type.SQLEXCEPTION, e.getMessage(), e);
      return -1;
    }    
    return 0; 
  }
  
  @Override 
  @SuppressWarnings("unchecked")
  public Integer visitHive_item(HplsqlParser.Hive_itemContext ctx) { 
    Var params = stackPeek();
    ArrayList<String> a = (ArrayList<String>)params.value;
    String param = ctx.getChild(1).getText();
    if (param.equals("e")) {
      a.add("-e");
      a.add(evalPop(ctx.expr()).toString());
    }   
    else if (param.equals("f")) {
      a.add("-f");
      a.add(evalPop(ctx.expr()).toString());
    }
    else if (param.equals("hiveconf")) {
      a.add("-hiveconf");
      a.add(ctx.L_ID().toString() + "=" + evalPop(ctx.expr()).toString());
    }
    return 0;
  }
  
  /**
   * Executing OS command
   */
  @Override 
  public Integer visitHost_cmd(HplsqlParser.Host_cmdContext ctx) { 
    LOG.trace(ctx, "HOST");      
    execHost(ctx, ctx.start.getInputStream().getText(
        new org.antlr.v4.runtime.misc.Interval(ctx.start.getStartIndex(), ctx.stop.getStopIndex())));                
    return 0; 
  }
  
  @Override 
  public Integer visitHost_stmt(HplsqlParser.Host_stmtContext ctx) { 
    LOG.trace(ctx, "HOST");      
    execHost(ctx, evalPop(ctx.expr()).toString());                
    return 0; 
  }
  
  public void execHost(ParserRuleContext ctx, String cmd) { 
    try { 
      if (trace) {
        LOG.trace(ctx, "HOST Command: " + cmd);      
      } 
      Process p = Runtime.getRuntime().exec(cmd);      
      new StreamGobbler(p.getInputStream()).start();
      new StreamGobbler(p.getErrorStream()).start(); 
      int rc = p.waitFor();      
      if (trace) {
        LOG.trace(ctx, "HOST Process exit code: " + rc);
      }
      setHostCode(rc);
    } catch (Exception e) {
      setHostCode(1);
      signal(Signal.Type.SQLEXCEPTION);
    }        
  }
  
  /**
   * Standalone expression (as a statement)
   */
  @Override 
  public Integer visitExpr_stmt(HplsqlParser.Expr_stmtContext ctx) { 	
    visitChildren(ctx); 
 	  return 0;
  }
  
  /**
   * String concatenation operator
   */
  @Override 
  public Integer visitExpr_concat(HplsqlParser.Expr_concatContext ctx) { 
    if (exec.buildSql) {
      exec.expr.operatorConcatSql(ctx);
    }
    else {
      exec.expr.operatorConcat(ctx);
    }
    return 0;
  }
    
  /**
   * Simple CASE expression
   */
  @Override 
  public Integer visitExpr_case_simple(HplsqlParser.Expr_case_simpleContext ctx) { 
    if (exec.buildSql) {
      exec.expr.execSimpleCaseSql(ctx);
    }
    else {
      exec.expr.execSimpleCase(ctx);
    }
    return 0;
  }
  
  /**
   * Searched CASE expression
   */
  @Override 
  public Integer visitExpr_case_searched(HplsqlParser.Expr_case_searchedContext ctx) { 
    if (exec.buildSql) {
      exec.expr.execSearchedCaseSql(ctx);
    }
    else {
      exec.expr.execSearchedCase(ctx);
    }
    return 0;
  }

  /**
   * GET DIAGNOSTICS EXCEPTION statement
   */
  @Override 
  public Integer visitGet_diag_stmt_exception_item(HplsqlParser.Get_diag_stmt_exception_itemContext ctx) { 
    return exec.stmt.getDiagnosticsException(ctx); 
  }  

  /**
   * GET DIAGNOSTICS ROW_COUNT statement
   */
  @Override 
  public Integer visitGet_diag_stmt_rowcount_item(HplsqlParser.Get_diag_stmt_rowcount_itemContext ctx) { 
    return exec.stmt.getDiagnosticsRowCount(ctx);  
  }
  
  /**
   * GRANT statement
   */
  @Override 
  public Integer visitGrant_stmt(HplsqlParser.Grant_stmtContext ctx) { 
    LOG.trace(ctx, "GRANT");
    return 0; 
  }
  
  /**
   * Label
   */
  @Override 
  public Integer visitLabel(HplsqlParser.LabelContext ctx) { 
    if (ctx.L_ID() != null) {
      exec.labels.push(ctx.L_ID().toString());
    }
    else {
      String label = ctx.ident().getText();
      if (label.endsWith(":")) {
        label = label.substring(0, label.length() - 1);
      }
      exec.labels.push(label);
    }
    return 0;
  }
  
  /**
   * Identifier
   */
  @Override 
  public Integer visitIdent(HplsqlParser.IdentContext ctx) { 
    String ident = ctx.getText();
    LOG.trace(ctx, "IDENT " + ident);
    Var var = findVariable(ident);
    if (var != null) {
      if (!exec.buildSql) {
        exec.stackPush(var);
      }
      else {
        exec.stackPush(new Var(ident, Var.Type.STRING, var.toSqlString()));
      }
    }
    else {
      if (exec.conf.executeIdentAsProcedure && !exec.buildSql && exec.function.isProc(ident) && exec.function.execProc(ident, null, ctx)) {
        return 0;
      }
      else {
          if (ident.startsWith(":")) {
              ident = ident.substring(1);
          }
          exec.stackPush(new Var(Var.Type.IDENT, ident));
      }
    }
    return 0;
  }  
  
  /** 
   * Single quoted string literal 
   */
  @Override 
  public Integer visitSingle_quotedString(HplsqlParser.Single_quotedStringContext ctx) { 
    if (exec.buildSql) {
      exec.stackPush(ctx.getText());
    }
    else {
      exec.stackPush(Utils.unquoteString(ctx.getText()));
    }
    return 0;
  }
  
  /**
   * Integer literal, signed or unsigned
   */
  @Override 
  public Integer visitInt_number(HplsqlParser.Int_numberContext ctx) {
    exec.stack.push(new Var(new Long(ctx.getText())));  	  
	  return 0; 
  }
 
  /**
   * Interval expression (INTERVAL '1' DAY i.e)
   */
  @Override 
  public Integer visitExpr_interval(HplsqlParser.Expr_intervalContext ctx) {
    if (exec.buildSql) {
      StringBuilder s = new StringBuilder();
      s.append("INTERVAL ");
      s.append(evalPop(ctx.expr()).toString());
      s.append(" ");
      s.append(ctx.interval_item().getText());
      stackPush(new Var(s.toString()));
      return 0;
    }
    int num = evalPop(ctx.expr()).intValue();
    Interval interval = new Interval().set(num, ctx.interval_item().getText());
    stackPush(new Var(interval));
    return 0; 
  }
  
  /**
   * Decimal literal, signed or unsigned
   */
  @Override 
  public Integer visitDec_number(HplsqlParser.Dec_numberContext ctx) {
    stackPush(new Var(new BigDecimal(ctx.getText())));     
    return 0; 
  }
  
  /**
   * Boolean literal
   */
  @Override 
  public Integer visitBool_literal(HplsqlParser.Bool_literalContext ctx) {
    boolean val = true;
    if (ctx.T_FALSE() != null) {
      val = false;
    }
    stackPush(new Var(new Boolean(val)));     
    return 0; 
  }

  /**
   * NULL constant
   */
  @Override 
  public Integer visitNull_const(HplsqlParser.Null_constContext ctx) { 
    stackPush(new Var());     
    return 0;  
  }

  /**
   * DATE 'YYYY-MM-DD' literal
   */
  @Override 
  public Integer visitDate_literal(HplsqlParser.Date_literalContext ctx) { 
    if (!exec.buildSql) {
      String str = evalPop(ctx.string()).toString();
      stackPush(new Var(Var.Type.DATE, Utils.toDate(str)));
    }
    else {
      stackPush(getFormattedText(ctx));
    }
    return 0; 
  }

  /**
   * TIME 'HH:MI:SS.FFFFFF' literal
   */
  @Override 
  public Integer visitTime_literal(HplsqlParser.Time_literalContext ctx) { 
    if (!exec.buildSql) {
      String str = evalPop(ctx.string()).toString();
      int len = str.length();
      int precision = 0;
      if (len > 8 && len <= 18) {
        precision = len - 9;
        if (precision > 6) {
          precision = 6;
        }
      }
      stackPush(new Var(Utils.toTime(str), precision));
    }
    else {
      stackPush(getFormattedText(ctx));
    }
    return 0; 
  }

  /**
   * TIMESTAMP 'YYYY-MM-DD HH:MI:SS.FFFFFF' literal
   */
  @Override 
  public Integer visitTimestamp_literal(HplsqlParser.Timestamp_literalContext ctx) { 
    if (!exec.buildSql) {
      String str = evalPop(ctx.string()).toString();
      int len = str.length();
      int precision = 0;
      if (len > 19 && len <= 29) {
        precision = len - 20;
        if (precision > 6) {
          precision = 6;
        }
      }
      stackPush(new Var(Utils.toTimestamp(str), precision));
    }
    else {
      stackPush(getFormattedText(ctx));
    }
    return 0; 
  }
  
  /**
   * Get the package context within which the current routine is executed
   */
  Package getPackageCallContext() {
    Scope cur = exec.currentScope;
    while (cur != null) {
      if (cur.type == Scope.Type.ROUTINE) {
        return cur.pack;
      }
      cur = cur.parent;
    }    
    return null;
  }
  
  /**
   * Define the connection profile to execute the current statement
   */
  String getStatementConnection() {
    if (exec.stmtConnList.contains(exec.conf.defaultConnection)) {
      return exec.conf.defaultConnection;
    }
    else if (!exec.stmtConnList.isEmpty()) {
      return exec.stmtConnList.get(0);
    }
    return exec.conf.defaultConnection;
  }
  
  /**
   * Define the connection profile for the specified object
   * @return
   */
  String getObjectConnection(String name) {
    String conn = exec.objectConnMap.get(name.toUpperCase());
    if (conn != null) {
      return conn;
    }
    return exec.conf.defaultConnection;
  }
  
  /**
   * Get the connection (open the new connection if not available)
   * @throws Exception 
   */
  Connection getConnection(String conn) throws Exception {
    if (conn == null || conn.equalsIgnoreCase("default")) {
      conn = exec.conf.defaultConnection;
    }
    return exec.conn.getConnection(conn);
  }
  
  /**
   * Return the connection to the pool
   */
  void returnConnection(String name, Connection conn) {
    exec.conn.returnConnection(name, conn);
  }
  
  /**
   * Define the database type by profile name
   */
  Conn.Type getConnectionType(String conn) {
    return exec.conn.getTypeByProfile(conn);
  }
  
  /**
   * Get the current database type
   */
  public Conn.Type getConnectionType() {
    return getConnectionType(exec.conf.defaultConnection);
  }
  
  /** 
   * Add managed temporary table
   */
  public void addManagedTable(String name, String managedName) {
    exec.managedTables.put(name, managedName);
  }
  
  /**
   * Get node text including spaces
   */
  String getText(ParserRuleContext ctx) {
    return ctx.start.getInputStream().getText(new org.antlr.v4.runtime.misc.Interval(ctx.start.getStartIndex(), ctx.stop.getStopIndex()));
  }
  
  String getText(ParserRuleContext ctx, Token start, Token stop) {
    return ctx.start.getInputStream().getText(new org.antlr.v4.runtime.misc.Interval(start.getStartIndex(), stop.getStopIndex()));
  }
  
  /**
   * Append the text preserving the formatting (space symbols) between tokens
   */
  void append(StringBuilder str, String appendStr, Token start, Token stop) {
    String spaces = start.getInputStream().getText(new org.antlr.v4.runtime.misc.Interval(start.getStartIndex(), stop.getStopIndex()));
    spaces = spaces.substring(start.getText().length(), spaces.length() - stop.getText().length());
    str.append(spaces);
    str.append(appendStr);
  }
  
  void append(StringBuilder str, TerminalNode start, TerminalNode stop) {
    String text = start.getSymbol().getInputStream().getText(new org.antlr.v4.runtime.misc.Interval(start.getSymbol().getStartIndex(), stop.getSymbol().getStopIndex()));
    str.append(text);
  }
   
  /**
   * Get the first non-null node
   */
  TerminalNode nvl(TerminalNode t1, TerminalNode t2) {
    if (t1 != null) {
      return t1;
    }
    return t2;
  }
  
  /**
   * Evaluate the expression and pop value from the stack
   */
  Var evalPop(ParserRuleContext ctx) {
    visit(ctx);
    if (!exec.stack.isEmpty()) { 
      return exec.stackPop();
    }
    return Var.Empty;
  }
  
  Var evalPop(ParserRuleContext ctx, long def) {
    visit(ctx);
    if (!exec.stack.isEmpty()) { 
      return stackPop();
    }
    return new Var(def);
  } 
  
  /**
   * Evaluate the data type and length 
   * 
   */
  String evalPop(HplsqlParser.DtypeContext type, HplsqlParser.Dtype_lenContext len) {
    if (isConvert(exec.conf.defaultConnection)) {
      return exec.converter.dataType(type, len);
    }
    ParserRuleContext stopCtx = (len != null) ? len : type;
    return getText(type, type.getStart(), stopCtx.getStop());
  }
  
  /**
   * Evaluate the expression to NULL
   */
  void evalNull() {
    stackPush(Var.Null); 
  }
  
  /**
   * Get formatted text between 2 tokens
   */
  public String getFormattedText(ParserRuleContext ctx) {
    return ctx.start.getInputStream().getText(
      new org.antlr.v4.runtime.misc.Interval(ctx.start.getStartIndex(), ctx.stop.getStopIndex()));                
  }
  
  /**
   * Flag whether executed from UDF or not
   */
  void setUdfRun(boolean udfRun) {
    this.udfRun = udfRun;
  }
  
  /**
   * Whether on-the-fly SQL conversion is required for the connection 
   */
  boolean isConvert(String connName) {
    return exec.conf.getConnectionConvert(connName);
  }
  
  /**
   * Increment the row count
   */
  public int incRowCount() {
    return exec.rowCount++;
  }
  
  /**
   * Set the row count
   */
  public void setRowCount(int rowCount) {
    exec.rowCount = rowCount;
  }
  
  public Stack<Var> getStack() {
    return exec.stack;
  }
 
  public int getRowCount() {
    return exec.rowCount;
  }

  public Conf getConf() {
    return exec.conf;
  }
  
  public Meta getMeta() {
    return exec.meta;
  }
  
  public boolean getTrace() {
    return exec.trace;
  }
  
  public boolean getInfo() {
    return exec.info;
  }
  
  public boolean getOffline() {
    return exec.offline;
  }

  public ByteBuffer[] getNewRow(ByteBuffer newRow) throws Exception {
    if (newRow == null)
      return null;
    short numCols = newRow.getShort();
    ByteBuffer[] rows = new ByteBuffer[numCols];
    byte[] colName;
    short colNameLen;
    byte[] colValue;
    int colValueLen;

    // the numCols value is 1 for the alignedformat
    for (short colIndex = 0; colIndex < numCols; colIndex++) {
      colNameLen = newRow.getShort();
      colName = new byte[colNameLen];
      newRow.get(colName, 0, colNameLen);
      colValueLen = newRow.getInt();
      colValue = new byte[colValueLen];
      newRow.get(colValue, 0, colValueLen);
      rows[colIndex] = ByteBuffer.wrap(colValue);
      ;
    }

    return rows;
  }

  // newRowIdLen = 0 means only one row
  public boolean isMultiRows(int newRowIdLen) {
    if (newRowIdLen == 0)
      return false;
    else
      return true;
  }

  public Map<String, ByteBuffer[] > fillNewRows(ByteBuffer bbNewRows,
						ByteBuffer bbNewRowIds,
						int newRowIdLen) throws Exception {
    if (bbNewRows == null)
      return null;
    byte rowIDSuffix;
    byte[] colName, colValue, rowId;
    int colValueLen;
    short actRowIdLen;
    short numCols, colNameLen, numRows;
    Map<String, ByteBuffer[]> mapRows = new HashMap<>();

    if (isMultiRows(newRowIdLen)) {
      numRows = bbNewRowIds.getShort();
    } else {
      numRows = 1;
    }
    LOG.trace("fillNewRows process new values, numRows is " + numRows);

    rowId = null;
    for(short rowNum = 0; rowNum < numRows; rowNum++) {
      // rowValues[0] is new, rowValues[1] is old
      ByteBuffer[] rowValues = new ByteBuffer[2];
      rowValues[0] = null;
      rowValues[1] = null;

      if (newRowIdLen != 0) {
	// multi rows get rowid
	rowIDSuffix  = bbNewRowIds.get();
	if (rowIDSuffix == '1')
	  actRowIdLen = (short)(newRowIdLen+1);
	else
	  actRowIdLen = (short)newRowIdLen;
	rowId = new byte[actRowIdLen];
	bbNewRowIds.get(rowId, 0, actRowIdLen);
      } else {
	// make sure bbNewRowIds can convert to byte[], one row get rowid
	if (bbNewRowIds.hasArray())
	  rowId = bbNewRowIds.array();
      }
      LOG.trace("new rowid is " + Arrays.toString(rowId));
      numCols = bbNewRows.getShort();
      for (short colIndex = 0; colIndex < numCols; colIndex++) {
	colNameLen = bbNewRows.getShort();
	colName = new byte[colNameLen];
	bbNewRows.get(colName, 0, colNameLen);
	colValueLen = bbNewRows.getInt();
	colValue = new byte[colValueLen];
	bbNewRows.get(colValue, 0, colValueLen);
	rowValues[0] = ByteBuffer.wrap(colValue);
	break;
      }
      mapRows.put(Arrays.toString(rowId), rowValues);
    }

    return mapRows;
  }

  public List<ByteBuffer[]> fillNewAndOldRows(Map<String, ByteBuffer[]> mapRows) throws Exception {
    if (mapRows == null)
      return null;

    List<ByteBuffer[]> rows = new ArrayList<ByteBuffer[]>();
    for (Map.Entry<String, ByteBuffer[]> entry : mapRows.entrySet()) {
      rows.add(entry.getValue());
    }

    return rows;
  }

  public Map<String, ByteBuffer[]> fillOldRows(Map<String, ByteBuffer[]> mapNewRows,
					       ByteBuffer bbOldRows, String oldRowsLen,
					       ByteBuffer bbOldrowIds, String oldRowIdsLen) throws Exception {
    if (bbOldRows == null)
      return mapNewRows;

    String[] rowsLen = oldRowsLen.split(",");
    String[] idsLen = oldRowIdsLen.split(",");
    if (rowsLen.length < 2 && idsLen.length < 1)
      return mapNewRows;

    int colValueLen;
    short numCols, numRows, oldRowIdLen;
    byte[] oldRowId, oldRowValue;
    Map<String, ByteBuffer[]> mapRows = new HashMap<>();

    int index = 0;
    numRows = Short.valueOf(rowsLen[index]);
    LOG.trace("fillOldRows deal old values, rowNum is " + idsLen.length + ", rowsLen get rowNum is " + numRows);    
    index++;

    for (short rowNum = 0; rowNum < numRows; rowNum++) {
      oldRowIdLen = Short.valueOf(idsLen[rowNum]);
      oldRowId = new byte[oldRowIdLen];
      bbOldrowIds.get(oldRowId, 0, oldRowIdLen);
      numCols = Short.valueOf(rowsLen[index]);
      index++;
      colValueLen = Integer.valueOf(rowsLen[index]);
      //only get first columns value
      index += numCols;
      oldRowValue = new byte[colValueLen];
      bbOldRows.get(oldRowValue, 0, colValueLen);

      if (mapNewRows != null) {
	LOG.trace("old rowid is " + Arrays.toString(oldRowId));
	// if update primary key, will delete than update
	// so the rowid is different, so if only one row, not need judge
	if (mapNewRows.size() == 1 && numRows == 1) {
	  for (Map.Entry<String, ByteBuffer[]> entry : mapNewRows.entrySet()) {
	    entry.getValue()[1] = ByteBuffer.wrap(oldRowValue);
	  }
	} else 	if (mapNewRows.get(Arrays.toString(oldRowId)) != null) {
	  LOG.trace("find new value pair");
	  try {
	    mapNewRows.get(Arrays.toString(oldRowId))[1] = ByteBuffer.wrap(oldRowValue);
	  } catch (Exception e) {
	    throw e;
	  }
	}
      } else {
	LOG.trace("only get old value");
	ByteBuffer[] rowValues = new ByteBuffer[2];
	rowValues[0] = null;
	rowValues[1] = ByteBuffer.wrap(oldRowValue);
	mapRows.put( Arrays.toString(oldRowId), rowValues);
      }
    }

    if (mapNewRows != null) {
      LOG.trace("return new and old value");
      return mapNewRows;
    } else {
      LOG.trace("only return old value");
      return mapRows;
    }
  }
  public List<ByteBuffer[]> getNewAndOldRows(ByteBuffer bbOldRows, String oldRowsLen,
				 ByteBuffer bbOldrowIds, String oldRowIdsLen,
				 ByteBuffer bbNewRows,
				 ByteBuffer bbNewRowIds,
				 int newRowIdLen) throws Exception {
		if (bbNewRows == null && bbOldRows == null)
			return null;

		Map<String, ByteBuffer[]> mapRows = new HashMap<>();
		mapRows = null;
		if (bbNewRows != null) {
		  mapRows = fillNewRows(bbNewRows, bbNewRowIds, newRowIdLen);
		}

		//deal old values
		if (bbOldRows != null) {
		  mapRows = fillOldRows(mapRows,
					bbOldRows, oldRowsLen,
					bbOldrowIds, oldRowIdsLen);
		}

		List<ByteBuffer[]> rows = new ArrayList<ByteBuffer[]>();
		rows = fillNewAndOldRows(mapRows);

		return rows;
	}  
	
  class RowDataInfo {
    private int FF = 0;
    private int valPos = 0;
    private int padding = 0;
    private int BO = 0;
    private int nVar = 0;
    private int[] varArr;
    private int nBitMap;
    private byte[] nullByteMap;
    private ByteBuffer row;
    private List<Column> colList;
    private int nullableColCount;
    private int fixedOffset;
		
    public int getFF() {
      return FF;
    }

    public int getValPos() {
      return valPos;
    }

    public int getPadding() {
      return padding;
    }

    public int getBO() {
      return BO;
    }

    public int getnVar() {
      return nVar;
    }

    public int[] getVarArr() {
      return varArr;
    }

    public int getnBitMap() {
      return nBitMap;
    }

    public byte[] getNullByteMap() {
      return nullByteMap;
    }

    public void setFixedOffset(int fixedOffset) {
      this.fixedOffset = fixedOffset;
    }

    public int getFixedOffset() {
      return fixedOffset;
    }

    public RowDataInfo (ByteBuffer row, List<Column> columnList, int nullableColumnCount) throws Exception {    
      if(row != null) {
	this.row = row;
	this.row.order(ByteOrder.LITTLE_ENDIAN);
	this.colList = columnList;
        this.nullableColCount = nullableColumnCount;
        this.fixedOffset = 0;
	dealRowHead();
      }
    }
		
    public void dealRowHead() throws Exception {
      LOG.trace("dealRowHead start---");
      if (row == null)
	return;

      FF = row.getInt();
      fixedOffset += 4;
      valPos = FF & 0x00003FFF;
      padding = FF & 0x0000C000;
      BO = row.getInt();
      fixedOffset += 4;
      nVar = (BO - 2 * 4) / 4;
      if(BO == 0) {
	nVar = varcharNumberCalculate();
      }

      int i = 0;
      int tmp = nVar;
      if(nVar > 0) {
	varArr = new int[tmp];
	while (tmp > 0) {
	  varArr[i] = row.getInt();
          fixedOffset += 4;
	  i++;
	  tmp--;
	}
      }

      // this is the real bitmapsize
      if (nullableColCount > 0) {
        nBitMap = ((nullableColCount >> 5) + 1) * 0x00000004;
      } else {
        nBitMap = 0;
      }

      // this size include real bitmap size and the padding size
      LOG.trace("nBitMap is " + nBitMap + " BO is " + BO);
      if(nBitMap > 0) {
	nullByteMap = new byte[nBitMap];
	row.get(nullByteMap, 0, nBitMap);
      }
      fixedOffset += nBitMap;

      int ffAlignSize = (((!colList.isEmpty()) && colList.size() > 0)
                            ? colList.get(0).getHbaseDataAligement()
                            : -1);

      if (ffAlignSize > 1) {
          int pad = 0;
          pad = fixedOffset % ffAlignSize;
          if (pad > 0) {
              pad = ffAlignSize - pad;
              fixedOffset += pad;
              row.position(fixedOffset);
          }
      }

      LOG.trace("varPos is " + valPos + " ,fixedOffset is " + fixedOffset);
      LOG.trace("dealRowHead finish---");
    }

    private int varcharNumberCalculate() {
      if(this.nVar > 0) {
	return nVar;
      }

      int varNumber = 0;
      for(int i = 0; i < colList.size(); i++) {
	if("VARCHAR".equals(colList.get(i).getType())) {
	  varNumber++;
	}
      }

      return varNumber;
    }
  }
	
  /**
   * Determines whether a column in a row is null
   * one byte contain 8 column flag
   * @param b
   * @param colIndex
   * @return 1 is null, 0 is not null
   */
  public static boolean colValueIsNull(byte[] byteFlagArray, int colIndex) {
    LOG.trace("colValueIsNull byteFlagArray:" + byteFlagArray + ",colIndex:" + colIndex);
    if(byteFlagArray == null) {
      return true;
    }
    if (colIndex < 0) {
      return false;
    }
    int i = colIndex / 8;
    byte b = byteFlagArray[i];
    int bit = 7 - colIndex % 8;
		
    String flagStr = "" + (byte) ((b >> bit) & 0x1);
    return "1".equals(flagStr);
  }

  /**
   * 
   * @param tabId
   * @param newAndOldRows
   * @return
   * @throws Exception
   */
  public List<Row[]> processRowsData(Long tabId, List<ByteBuffer[]> newAndOldRows) throws Exception {
    List<Row[]> rows = new ArrayList<Row[]>();
    if (newAndOldRows == null || newAndOldRows.size() == 0) {
      Row[] rowValues = new Row[2];
      rowValues[0] = null;
      rowValues[1] = null;
      rows.add(rowValues);
      return rows;
    }
	      
    String sql = "select column_name, sql_data_type, column_size, column_class, "
      + " column_precision, column_scale, fs_data_type, nullable, character_set, "
      + " datetime_start_field, datetime_end_field, "
      + " default_value, default_class, column_class "
      + " from TRAFODION.\"_MD_\".columns"
      + " where object_uid = " + tabId 
      + " order by column_number";

    LOG.trace("dealOnerowData SQL:" + sql);
    String colName;
    String dataType;
    String defaultValue;
    int colSize;
    int precision;
    int scale;
    int detailType = -1;
    int nullAble = -1;
    String characterSet ;
    int datetimeStartField;
    int datetimeEndField;
    int nullableColumnCount = 0;
    int defaultClass = 0;
    String colClass;
    Query query = exec.conn.executeQuery(sql, "default");
    ResultSet rs = query.getResultSet();

    List<Column> columnList = new ArrayList<>();
    List<Column> addCol = new ArrayList<>();
    List<Column> varcharCol = new ArrayList<>();
    try {
      while (rs.next()) {
	colName = rs.getString(1).toUpperCase().trim();
	dataType = rs.getString(2).toUpperCase().trim();
	colSize = rs.getInt(3);
	precision = rs.getInt(5);
	scale = rs.getInt(6);
	detailType = rs.getInt(7);
	nullAble = rs.getInt(8);
	characterSet = rs.getString(9);
	characterSet = characterSet== null ? null : rs.getString(9).trim();
	datetimeStartField = rs.getInt(10);
	datetimeEndField = rs.getInt(11);
        defaultValue = rs.getString(12);
        defaultClass = rs.getInt(13);
        colClass = rs.getString(14).toUpperCase().trim();

	boolean nullAbleFlag = false;
        // defaultClass == 3 means COM_USER_DEFINED_DEFAULT
        if (nullAble == 1 &&
            (defaultClass != 3 || (defaultClass == 3 && defaultValue == null))) {
          nullAbleFlag = true;
          nullableColumnCount++;
        }

	Column col = new Column(colName, dataType, detailType,
				colSize, precision, scale, nullAbleFlag, characterSet,
				datetimeStartField, datetimeEndField);
        LOG.trace(" the defaultClass is " + defaultClass);
        col.setDefaultValue(defaultValue);
        col.setDefaultClass(defaultClass);
        col.setNullColIndex(nullableColumnCount - 1);
        if (dataType.equals("VARCHAR")) {
            if (colClass.equals("A")) {
                col.setAddCol(true);
            } else {
                col.setAddCol(false);
            }
            varcharCol.add(col);
        } else if (colClass.equals("A")) {
            col.setAddCol(true);
            addCol.add(col);
        } else {
            col.setAddCol(false);
            columnList.add(col);
        }
      }

      columnList.sort(new Comparator<Column>() {
        @Override
        public int compare(Column o1, Column o2) {
          return o2.getHbaseDataAligement() - o1.getHbaseDataAligement();
        }
       });
      for (int i = 0; i < addCol.size(); i++) {
          columnList.add(addCol.get(i));
      }
      for (int i = 0; i < varcharCol.size(); i++) {
          columnList.add(varcharCol.get(i));
      }
			
      boolean oldNullFlag = false;
      boolean newNullFlag = false;

      LOG.trace("nullableColumnCount is " + nullableColumnCount);
      for(ByteBuffer[] newOldRow : newAndOldRows) {
	Row[] rowValues = new Row[2];
	ByteBuffer newRowBuf = newOldRow[0];
	ByteBuffer oldRowBuf = newOldRow[1];
				
	RowDataInfo oldHead = new RowDataInfo(oldRowBuf, columnList, nullableColumnCount);
	RowDataInfo newHead = new RowDataInfo(newRowBuf, columnList, nullableColumnCount);
				
	Row newRow = newRowBuf == null ? null : new Row();
	Row oldRow = oldRowBuf == null ? null : new Row();
	rowValues[0] = newRow;
	rowValues[1] = oldRow;
	rows.add(rowValues);
				
	int nullColumnIndex = 0;
	List<Column> oldColumnList = new ArrayList<>();
	List<Column> newColumnList = new ArrayList<>();
				
	for(int i=0; i<columnList.size(); i++) {
	  Column col = columnList.get(i);
	  if("SYSKEY".equalsIgnoreCase(col.getName())) {
	    if(oldRowBuf != null) {
	      oldRowBuf.getLong();
              oldHead.setFixedOffset(oldHead.getFixedOffset() + processColumn.LONG_SIZE);
	    }
	    if(newRowBuf != null) {
	      newRowBuf.getLong();
              newHead.setFixedOffset(newHead.getFixedOffset() + processColumn.LONG_SIZE);
	    }
	    continue;
	  }

          oldNullFlag = false;
          newNullFlag = false;
          
	  if(col.isNull) {
            oldNullFlag = colValueIsNull(oldHead.getNullByteMap(), col.getNullColIndex());
            LOG.trace("oldCol name is " + col.getName() + " is null " + oldNullFlag + " nullColumnIndex is " + nullColumnIndex);
	    newNullFlag = colValueIsNull(newHead.getNullByteMap(), col.getNullColIndex());
            LOG.trace("newCol name is " + col.getName() + " is null " + newNullFlag + " nullColumnIndex is " + nullColumnIndex );
	    nullColumnIndex++;
	  }
					
	  if(oldRowBuf != null) {
	    Column oldCol = new Column(col.getName(), col.getType(), col.getFsDataType(),
				       col.getLen(), col.getPrecision(), col.getScale(), oldNullFlag, col.getCharacterSet(),
				       col.getDateTimeStartField(), col.getDateTimeEndField(),
                                       col.getDefaultValue(), col.getDefaultClass(), col.getAddCol());
	    oldColumnList.add(oldCol);
	    oldRow.addColumn(oldCol);
	  }
	  if(newRowBuf != null) {
	    Column newCol = new Column(col.getName(), col.getType(), col.getFsDataType(),
				       col.getLen(), col.getPrecision(), col.getScale(), newNullFlag, col.getCharacterSet(),
				       col.getDateTimeStartField(), col.getDateTimeEndField(),
                                       col.getDefaultValue(), col.getDefaultClass(), col.getAddCol());
	    newColumnList.add(newCol);
	    newRow.addColumn(newCol);
	  }
	}
        if (trace) {
          for (int i = 0; i < newColumnList.size(); i++) {
            LOG.trace("column name is " + newColumnList.get(i).getName() + " type is " + newColumnList.get(i).getType() +
                      " aligenemt is " + newColumnList.get(i).getHbaseDataAligement() + " ,col.getDefaultClass is " + newColumnList.get(i).getDefaultClass());
          }
        }
	fillColumnData(oldColumnList, newColumnList, oldRowBuf, newRowBuf, oldHead, newHead);
      }
    } catch (SQLException e) {
      exec.signal(e);
    } finally {
      exec.closeQuery(query, "default");
    }

    return rows;
  }

  private void fillColumnData(List<Column> oldColumnList, List<Column> newColumnList, ByteBuffer oldRowBuf,
			      ByteBuffer newRowBuf, RowDataInfo oldHead, RowDataInfo newHead) throws Exception {
    LOG.trace("begin fillColumnData");
    int columnSize = oldColumnList.isEmpty() ? newColumnList.size() : oldColumnList.size();

    ProcessColumn oldProcessColumn = null;
    if (!oldColumnList.isEmpty()) {
        int varcharPos = 0;
        if (oldHead.getnVar() > 0) {
            varcharPos = (oldHead.getVarArr())[0];
        }
        oldProcessColumn = new ProcessColumn(oldHead.getFixedOffset(), true, varcharPos, 0, oldHead.getnVar(), oldHead.getVarArr());
    }

    ProcessColumn newProcessColumn = null;
    if (!newColumnList.isEmpty()) {
        newProcessColumn = new ProcessColumn(newHead.getFixedOffset(), true, 0, 0, newHead.getnVar(), newHead.getVarArr());
    }

    for (int i=0; i<columnSize; i++) {
      Column oldCol = oldColumnList.isEmpty() ? null : oldColumnList.get(i);
      Column newCol = newColumnList.isEmpty() ? null : newColumnList.get(i);

      int colSize = oldCol == null ? newCol.getLen() : oldCol.getLen();
      String colTypeStr = oldCol == null ? newCol.getType() : oldCol.getType();
      int hbaseDataAligement = oldCol == null ? newCol.getHbaseDataAligement() : oldCol.getHbaseDataAligement();
      LOG.trace("column " + i + " is " + colTypeStr + " column size is " + colSize + " aligement is " + hbaseDataAligement);
      if (oldCol != null) {
          oldProcessColumn.FixedAligementOffset(oldCol, oldRowBuf, true);
      }
      if (newCol != null) {
          newProcessColumn.FixedAligementOffset(newCol, newRowBuf, false);
      }

      switch (colTypeStr) {
      case "SIGNED SMALLINT":
      case "UNSIGNED SMALLINT":
	if(oldCol != null) {
            oldProcessColumn.processSmallint(oldCol, oldRowBuf);
	}
	if(newCol != null) {
            newProcessColumn.processSmallint(newCol, newRowBuf);
        }
	break;
      case "INTERVAL" :
	if(oldCol != null) {
            oldProcessColumn.processInterval(oldCol, oldRowBuf);
        }
	if(newCol != null) {
            newProcessColumn.processInterval(newCol, newRowBuf);
        }
	break;
      case "SIGNED TINYINT" :
      case "UNSIGNED TINYINT" :
	if(oldCol != null) {
            oldProcessColumn.processTinyint(oldCol, oldRowBuf);
        }
	if(newCol != null) {
            newProcessColumn.processTinyint(newCol, newRowBuf);
        }
	break;
      case "REAL":
	if(oldCol != null) {
            oldProcessColumn.processReal(oldCol, oldRowBuf);
        }
	if(newCol != null) {
            newProcessColumn.processReal(newCol, newRowBuf);
        }
	break;
      case "SIGNED INTEGER" :
      case "UNSIGNED INTEGER" :
	if(oldCol != null) {
            oldProcessColumn.processInterger(oldCol, oldRowBuf);
        }
	if(newCol != null) {
            newProcessColumn.processInterger(newCol, newRowBuf);
        }
	break;
      case "DOUBLE" :
	if(oldCol != null) {
            oldProcessColumn.processDouble(oldCol, oldRowBuf);
        }
	if(newCol != null) {
            newProcessColumn.processDouble(newCol, newRowBuf);
        }
	break;
      case "DATETIME" :
	if(oldCol != null) {
            oldProcessColumn.processDatetime(oldCol, oldRowBuf);
        }
	if(newCol != null) {
            newProcessColumn.processDatetime(newCol, newRowBuf);
        }
	break;
      case "SIGNED DECIMAL" :
      case "UNSIGNED DECIMAL" :
	if(oldCol != null) {
            oldProcessColumn.processDecimal(oldCol, oldRowBuf);
        }
	if(newCol != null) {
            newProcessColumn.processDecimal(newCol, newRowBuf);
        }
	break;
      case "UNSIGNED NUMERIC" :
      case "SIGNED NUMERIC" :
	if(oldCol != null) {
            oldProcessColumn.processNumeric(oldCol, oldRowBuf);
        }
	if(newCol != null) {
            newProcessColumn.processNumeric(newCol, newRowBuf);
        }
	break;
      case "SIGNED LARGEINT" :
      case "UNSIGNED LARGEINT" :	
	if(oldCol != null) {
            oldProcessColumn.processLargeint(oldCol, oldRowBuf);
        }
	if(newCol != null) {
            newProcessColumn.processLargeint(newCol, newRowBuf);
        }
	break;
      case "CHARACTER":
	if(oldCol != null) {
            oldProcessColumn.processCharacter(oldCol, oldRowBuf);
        }
	if(newCol != null) {
            newProcessColumn.processCharacter(newCol, newRowBuf);
        }
	break;
      case "VARCHAR" :
	if(oldCol != null) {
            oldProcessColumn.processVarchar(oldCol, oldRowBuf);
        }
        if (newCol != null) {
            newProcessColumn.processVarchar(newCol, newRowBuf);
        }
	break;
      case "BLOB":
      case "CLOB":
          if (oldCol != null) {
              oldProcessColumn.processLOB(oldCol, oldRowBuf);
          }
          if (newCol != null) {
              newProcessColumn.processLOB(newCol, newRowBuf);
          }
	break;
      default:
	break;
      }
    }
    LOG.trace("end fillColumnData");
  }
} 
