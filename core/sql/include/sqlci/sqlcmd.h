#pragma once

#include "cli/sqlcli.h"
#include "common/NABoolean.h"
#include "common/charinfo.h"
#include "sqlci/SqlciDefs.h"
#include "sqlci/SqlciNode.h"

class CursorStmt;
class SqlciEnv;
class ComDiagsArea;
class PrepStmt;

extern void HandleCLIErrorInit();
extern void HandleCLIError(int &err, SqlciEnv *sqlci_env, NABoolean displayErr = TRUE, NABoolean *isEOD = NULL,
                           int prepcode = 0, NABoolean getWarningsWithEOF = FALSE);
extern void HandleCLIError(SQLSTMT_ID *stmt, int &err, SqlciEnv *sqlci_env, NABoolean displayErr = TRUE,
                           NABoolean *isEOD = NULL, int prepcode = 0);

void handleLocalError(ComDiagsArea *diags, SqlciEnv *sqlci_env);
long getRowsAffected(SQLSTMT_ID *stmt);
int getDiagsCondCount(SQLSTMT_ID *stmt);
#define MAX_NUM_UNNAMED_PARAMS 128
#define MAX_LEN_UNNAMED_PARAM  300

class Execute;

class SqlCmd : public SqlciNode {
 public:
  enum sql_cmd_type {
    DML_TYPE,
    PREPARE_TYPE,
    EXECUTE_TYPE,
    CURSOR_TYPE,
    GOAWAY_TYPE,
    DESCRIBE_TYPE,
    QUERYCACHE_TYPE,
    USAGE_TYPE,
    QUIESCE_TYPE,
    STORE_EXPLAIN_TYPE
  };

 private:
  sql_cmd_type cmd_type;
  char *sql_stmt;
  int sql_stmt_oct_length;

 public:
  SqlCmd(const sql_cmd_type cmd_type_, const char *argument_);
  ~SqlCmd();
  char *get_sql_stmt() { return sql_stmt; }
  inline int get_sql_stmt_oct_length() { return sql_stmt_oct_length; };

  static short do_prepare(SqlciEnv *, PrepStmt *, char *sqlStmt, NABoolean resetLastExecStmt = TRUE, int rsIndex = 0,
                          int *prepcode = NULL, int *statisticsType = NULL);

  static short updateRepos(SqlciEnv *sqlci_env, SQLSTMT_ID *stmt, char *queryId);

  static short do_execute(SqlciEnv *, PrepStmt *, int numUnnamedParams = 0, char **unnamedParamArray = NULL,
                          CharInfo::CharSet *unnamedParamCharSetArray = NULL, int prepcode = 0);

  static short doExec(SqlciEnv *, SQLSTMT_ID *, PrepStmt *, int numUnnamedParams = 0, char **unnamedParamArray = NULL,
                      CharInfo::CharSet *unnamedParamCharSetArray = NULL, NABoolean handleError = TRUE);
  static short doDescribeInput(SqlciEnv *, SQLSTMT_ID *, PrepStmt *, int num_input_entries, int numUnnamedParams = 0,
                               char **unnamedParamArray = NULL, CharInfo::CharSet *unnamedParamCharSetArray = NULL);
  static short doFetch(SqlciEnv *, SQLSTMT_ID *stmt, PrepStmt *prep_stmt, NABoolean firstFetch = FALSE,
                       NABoolean handleError = TRUE, int prepcode = 0);
  static short doClearExecFetchClose(SqlciEnv *, SQLSTMT_ID *, PrepStmt *, int numUnnamedParams = 0,
                                     char **unnamedParamArray = NULL,
                                     CharInfo::CharSet *unnamedParamCharSetArray = NULL, NABoolean handleError = TRUE);
  static short getHeadingInfo(SqlciEnv *sqlci_env, PrepStmt *prep_stmt, char *headingRow, char *underline);

  static short displayHeading(SqlciEnv *sqlci_env, PrepStmt *prep_stmt);
  static int displayRow(SqlciEnv *sqlci_env, PrepStmt *prep_stmt);

  static void addOutputInfoToPrepStmt(SqlciEnv *sqlci_env, PrepStmt *prep_stmt);

  static short deallocate(SqlciEnv *sqlci_env, PrepStmt *prep_stmt);

  static short executeQuery(const char *query, SqlciEnv *sqlci_env);

  static short setEnviron(SqlciEnv *sqlci_env, int propagate);

  static short showShape(SqlciEnv *sqlci_env, const char *query);

  static char *replacePattern(SqlciEnv *sqlci_env, char *inStr);

  static void clearCLIDiagnostics();

  static short cleanupAfterError(int retcode, SqlciEnv *sqlci_env, SQLSTMT_ID *stmt, SQLDESC_ID *sql_src,
                                 SQLDESC_ID *output_desc, SQLDESC_ID *input_desc, NABoolean resetLastExecStmt);
  static void logDDLQuery(PrepStmt *prepStmt);
};

class DML : public SqlCmd {
 private:
  char *this_stmt_name;
  dml_type type;

  int rsIndex_;

 public:
  DML(const char *argument_, dml_type type_, const char *stmt_name_ = NULL);
  ~DML();
  short process(SqlciEnv *sqlci_env);

  void setResultSetIndex(int i) { rsIndex_ = i; }
  int getResultSetIndex() const { return rsIndex_; }
};

class Prepare : public SqlCmd {
  char *this_stmt_name;
  dml_type type;

 public:
  Prepare(char *stmt_name_, char *argument_, dml_type type_);
  ~Prepare();
  short process(SqlciEnv *sqlci_env);
};

class DescribeStmt : public SqlCmd {
  char *stmtName_;

 public:
  DescribeStmt(char *stmtName, char *argument);
  ~DescribeStmt();
  short process(SqlciEnv *sqlciEnv);
};

class Execute : public SqlCmd {
 private:
  char *using_params[MAX_NUM_UNNAMED_PARAMS];
  CharInfo::CharSet using_param_charsets[MAX_NUM_UNNAMED_PARAMS];
  short num_params;
  char *this_stmt_name;

 public:
  Execute(char *stmt_name_, char *argument_, short flag = 0, SqlciEnv *sqlci_env = NULL);
  ~Execute();
  short process(SqlciEnv *sqlci_env);
  static int storeParams(char *argument_, short &num_params, char *using_params[], CharInfo::CharSet[] = NULL,
                         SqlciEnv *sqlci_env = NULL);
  short getNumParams() const { return num_params; }
  char **getUnnamedParamArray() { return using_params; }
  CharInfo::CharSet *getUnnamedParamCharSetArray() { return using_param_charsets; }
  char *getUnnamedParamValue(short num) { return using_params[num]; }
  CharInfo::CharSet getUnnamedParamCharSet(short num) { return using_param_charsets[num]; }
};

class Cursor : public SqlCmd {
 public:
  enum CursorOperation { DECLARE, OPEN, FETCH, CLOSE, DEALLOC };
  Cursor(char *cursorName, CursorOperation operation, Int16 internalPrepare, char *argument,
         NABoolean internalCursor = FALSE);
  ~Cursor();
  short process(SqlciEnv *sqlci_env);
  // QSTUFF
  inline void setHoldable(NABoolean t) { isHoldable_ = t; }
  inline NABoolean isHoldable() { return isHoldable_; }
  // QSTUFF

  short declareC(SqlciEnv *sqlci_env, char *donemsg, int &retcode);
  short declareCursorStmt(SqlciEnv *sqlci_env, int &retcode);
  short declareCursorStmtForRS(SqlciEnv *sqlci_env, int &retcode);

  short open(SqlciEnv *sqlci_env, char *donemsg, int &retcode);

  short fetch(SqlciEnv *sqlci_env, NABoolean doDisplayRow, char *donemsg, int &retcode);

  short close(SqlciEnv *sqlci_env, char *donemsg, int &retcode);

  short dealloc(SqlciEnv *sqlci_env, char *donemsg, int &retcode);

  void cleanupCursorStmt(SqlciEnv *sqlci_env, CursorStmt *c);

  void setResultSetIndex(int i) { resultSetIndex_ = i; }
  int getResultSetIndex() const { return resultSetIndex_; }

 private:
  char *cursorName_;
  CursorOperation operation_;
  Int16 internalPrepare_;  // if -1, then argument is a SQL statement that
  NABoolean isHoldable_;

  NABoolean internalCursor_;
  int resultSetIndex_;
};
