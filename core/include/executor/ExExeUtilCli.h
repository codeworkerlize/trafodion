
#ifndef EX_EXE_UTIL_CLI_H
#define EX_EXE_UTIL_CLI_H
#include "common/ComTransInfo.h"
#include "comexe/ComQueue.h"

class ContextCli;

class OutputInfo {
 public:
  enum { MAX_OUTPUT_ENTRIES = 100 };

  OutputInfo(int numEntries);
  void dealloc(CollHeap *heap);
  void insert(int index, char *data);
  void insert(int index, char *data, int len);
  void insert(int index, char *data, int len, int type, int *indOffset = nullptr, int *varOffset = nullptr);
  char *get(int index);
  short get(int index, char *&data, int &len);
  short get(int index, char *&data, int &len, int &type, int *indOffset, int *varOffset);

 private:
  int numEntries_;
  char *data_[MAX_OUTPUT_ENTRIES];
  int len_[MAX_OUTPUT_ENTRIES];
  int type_[MAX_OUTPUT_ENTRIES];
};

class ExeCliInterface : public NABasicObject {
 private:
  enum {
    NOT_EXEUTIL_INTERNAL_QUERY = 0x0001,
    CURSOR_OPEN = 0x0002,
  };

 public:
  ExeCliInterface(CollHeap *heap = nullptr, int isoMapping = 0, ContextCli *currContext = nullptr,
                  const char *parentQid = nullptr);

  virtual ~ExeCliInterface();

  int allocStuff(SQLMODULE_ID *&module, SQLSTMT_ID *&stmt, SQLDESC_ID *&sql_src, SQLDESC_ID *&input_desc,
                   SQLDESC_ID *&output_desc, const char *stmtName = nullptr);

  int deallocStuff(SQLMODULE_ID *&module, SQLSTMT_ID *&stmt, SQLDESC_ID *&sql_src, SQLDESC_ID *&input_desc,
                     SQLDESC_ID *&output_desc);

  int executeImmediate(const char *stmt, char *outputBuf = nullptr, int *outputBufLen = nullptr,
                         NABoolean nullTerminate = TRUE, long *rowsAffected = nullptr, NABoolean monitorThis = FALSE,
                         ComDiagsArea **globalDiags = nullptr);

  int executeImmediatePrepare(const char *stmt, char *outputBuf = nullptr, int *outputBufLen = nullptr,
                                long *rowsAffected = nullptr, NABoolean monitorThis = FALSE, char *stmtName = nullptr);

  int executeImmediatePrepare2(const char *stmt, char *uniqueStmtId, int *uniqueStmtIdLen,
                                 SQL_QUERY_COST_INFO *query_cost_info, SQL_QUERY_COMPILER_STATS_INFO *comp_stats_info,
                                 char *outputBuf = nullptr, int *outputBufLen = nullptr, long *rowsAffected = nullptr,
                                 NABoolean monitorThis = FALSE, int *retGenCodeSize = nullptr);

  // retrieve generated code for a previously prepared stmt
  int getGeneratedCode(char *genCodeBuf, int genCodeSize);

  int executeImmediateExec(const char *stmt, char *outputBuf = nullptr, int *outputBufLen = nullptr,
                             NABoolean nullTerminate = TRUE, long *rowsAffected = nullptr,
                             ComDiagsArea **diagsArea = nullptr);

  int prepare(const char *stmtStr, SQLMODULE_ID *module, SQLSTMT_ID *stmt, SQLDESC_ID *sql_src,
                SQLDESC_ID *input_desc, SQLDESC_ID *output_desc, char **outputBuf, Queue *outputVarPtrList = nullptr,
                char **inputBuf = nullptr, Queue *inputVarPtrList = nullptr, char *uniqueStmtId = nullptr,
                int *uniqueStmtIdLen = nullptr, SQL_QUERY_COST_INFO *query_cost_info = nullptr,
                SQL_QUERY_COMPILER_STATS_INFO *comp_stats_info = nullptr, NABoolean monitorThis = FALSE,
                NABoolean doNotCachePlan = FALSE, int *retGenCodeSize = nullptr);

  int setupExplainData(SQLMODULE_ID *module, SQLSTMT_ID *stmt);
  int setupExplainData();
  char *getExplainDataPtr() { return explainData_; }
  int getExplainDataLen() { return explainDataLen_; }

  int exec(char *inputBuf = nullptr, int inputBufLen = 0);
  int fetch();

  // if ignoreIfNotOpen is TRUE, do not return an error is cursor is not open
  int close(NABoolean ignoreIfNotOpen = FALSE);

  int dealloc();

  short clearExecFetchClose(char *inputBuf, int inputBufLen, char *outputBuf = nullptr, int *outputBufLen = 0);

  short clearExecFetchCloseOpt(char *inputBuf, int inputBufLen, char *outputBuf = nullptr, int *outputBufLen = 0,
                               long *rowsAffected = nullptr);

  int executeImmediateCEFC(const char *stmtStr, char *inputBuf, int inputBufLen, char *outputBuf,
                             int *outputBufLen, long *rowsAffected = nullptr);

  int rwrsPrepare(const char *stmStr, int rs_maxsize, NABoolean monitorThis = FALSE);

  int rwrsExec(char *inputRow, int inputRowLen, long *rowsAffected);

  int rwrsClose();

  int cwrsAllocStuff(SQLMODULE_ID *&module, SQLSTMT_ID *&stmt, SQLDESC_ID *&sql_src, SQLDESC_ID *&input_desc,
                       SQLDESC_ID *&output_desc, SQLDESC_ID *&rs_input_maxsize_desc, const char *stmtName = nullptr);

  int cwrsDeallocStuff(SQLMODULE_ID *&module, SQLSTMT_ID *&stmt, SQLDESC_ID *&sql_src, SQLDESC_ID *&input_desc,
                         SQLDESC_ID *&output_desc, SQLDESC_ID *&rs_input_maxsize_desc);

  int cwrsPrepare(const char *stmtStr, int rs_maxsize, NABoolean monitorThis = FALSE);

  int cwrsExec(char *inputRow, int inputRowLen, long *rowsAffected);

  int cwrsClose(long *rowsAffected);

  int setInputValue(short entry, char *ptr, int len);
  // TODO support more data types
  inline int setInputValue(short entry, char *ptr) {
    return setInputValue(entry, ptr, (ptr == nullptr) ? 0 : strlen(ptr));
  }
  inline int setInputValue(short entry, int val) { return setInputValue(entry, (char *)&val, 4); }
  int getPtrAndLen(short entry, char *&ptr, int &len, short **ind = nullptr);
  int getHeadingAndLen(short entry, char *heading, int &len);

  int getNumEntries(int &numInput, int &numOutput);
  int getAttributes(short entry, NABoolean forInput, int &fsDatatype, int &length, int &vcIndLen,
                      int *indOffset, int *varOffset);
  int getDataOffsets(short entry, int forInput, int *indOffset, int *varOffset);

  int getStmtAttr(char *stmtName, int attrName, int *numeric_value, char *string_value);

  short fetchRowsPrologue(const char *sqlStrBuf, NABoolean noExec = FALSE, NABoolean monitorThis = FALSE,
                          char *stmtName = nullptr);
  short fetchRowsEpilogue(const char *sqlStrBuf, NABoolean noClose = FALSE);

  short initializeInfoList(Queue *&infoList, NABoolean infoListIsOutputInfo);

  short fetchAllRows(Queue *&infoList, const char *query, int numOutputEntries = 0, NABoolean varcharFormat = FALSE,
                     NABoolean monitorThis = FALSE, NABoolean initInfoList = FALSE);

  short prepareAndExecRowsPrologue(const char *sqlInitialStrBuf, char *sqlSecondaryStrBuf,
                                   Queue *initialOutputVarPtrList, Queue *continuingOutputVarPtrList,
                                   long &rowsAffected, NABoolean monitorThis = FALSE);

  short execContinuingRows(Queue *secondaryOutputVarPtrs, long &rowsAffected);

  void setOutputPtrsAsInputPtrs(Queue *entry, SQLDESC_ID *target_inputDesc = nullptr);

  int setCharsetTypes();

  int prepareAndExecRowsEpilogue();

  int beginWork();
  int commitWork();
  int rollbackWork();
  int autoCommit(NABoolean v);  // TRUE, set to ON. FALSE, set to OFF.
  int beginXn();
  int commitXn();
  int rollbackXn();
  int statusXn();
  int suspendXn();
  int resumeXn();
  static int saveXnState0();
  static int restoreXnState0();

  int createContext(char *contextHandle);   // out buf will return context handle
  int switchContext(char *contextHandle);   // in buf contains context handle
  int currentContext(char *contextHandle);  // out buf will return context handle
  int deleteContext(char *contextHandle);   // in buf contains context handle

  int retrieveSQLDiagnostics(ComDiagsArea *toDiags);
  ComDiagsArea *allocAndRetrieveSQLDiagnostics(ComDiagsArea *&toDiags);

  CollHeap *getHeap() { return heap_; }

  char *outputBuf() { return outputBuf_; };
  int outputDatalen() { return outputDatalen_; };

  char *inputBuf() { return inputBuf_; };
  int inputDatalen() { return inputDatalen_; };

  NABoolean isAllocated() { return (stmt_ ? TRUE : FALSE); }

  void clearGlobalDiags();

  int getIsoMapping() { return isoMapping_; };
  void setIsoMapping(int isoMapping) { isoMapping_ = isoMapping; };

  int GetRowsAffected(long *rowsAffected);

  int holdAndSetCQD(const char *defaultName, const char *defaultValue, ComDiagsArea *globalDiags = nullptr);

  int holdAndSetCQDs(const char *holdStr, const char *setStr, ComDiagsArea *globalDiags = nullptr);

  int restoreCQD(const char *defaultName, ComDiagsArea *globalDiags = nullptr);

  int getCQDval(const char *defaultName, char *val, ComDiagsArea *globalDiags = nullptr);
  int restoreCQDs(const char *restoreStr, ComDiagsArea *globalDiags = nullptr);

  void setNotExeUtilInternalQuery(NABoolean v) {
    (v ? flags_ |= NOT_EXEUTIL_INTERNAL_QUERY : flags_ &= ~NOT_EXEUTIL_INTERNAL_QUERY);
  };
  NABoolean notExeUtilInternalQuery() { return (flags_ & NOT_EXEUTIL_INTERNAL_QUERY) != 0; };

  void setCursorOpen(NABoolean v) { (v ? flags_ |= CURSOR_OPEN : flags_ &= ~CURSOR_OPEN); };
  NABoolean cursorOpen() { return (flags_ & CURSOR_OPEN) != 0; };

  int setCQS(const char *shape, ComDiagsArea *globalDiags = nullptr);
  int resetCQS(ComDiagsArea *globalDiags = nullptr);

  // methods for routine invocation
  int getRoutine(
      /* IN */ const char *serializedInvocationInfo,
      /* IN */ int invocationInfoLen,
      /* IN */ const char *serializedPlanInfo,
      /* IN */ int planInfoLen,
      /* IN */ int language,
      /* IN */ int paramStyle,
      /* IN */ const char *externalName,
      /* IN */ const char *containerName,
      /* IN */ const char *externalPath,
      /* IN */ const char *librarySqlName,
      /* OUT */ int *handle,
      /* IN/OUT */ ComDiagsArea *diags);

  int invokeRoutine(
      /* IN */ int handle,
      /* IN */ int phaseEnumAsInt,
      /* IN */ const char *serializedInvocationInfo,
      /* IN */ int invocationInfoLen,
      /* OUT */ int *invocationInfoLenOut,
      /* IN */ const char *serializedPlanInfo,
      /* IN */ int planInfoLen,
      /* IN */ int planNum,
      /* OUT */ int *planInfoLenOut,
      /* IN */ char *inputRow,
      /* IN */ int inputRowLen,
      /* OUT */ char *outputRow,
      /* IN */ int outputRowLen,
      /* IN/OUT */ ComDiagsArea *diags);

  int getRoutineInvocationInfo(
      /* IN */ int handle,
      /* IN/OUT */ char *serializedInvocationInfo,
      /* IN */ int invocationInfoMaxLen,
      /* OUT */ int *invocationInfoLenOut,
      /* IN/OUT */ char *serializedPlanInfo,
      /* IN */ int planInfoMaxLen,
      /* IN */ int planNum,
      /* OUT */ int *planInfoLenOut,
      /* IN/OUT */ ComDiagsArea *diags);

  int putRoutine(
      /* IN */ int handle,
      /* IN/OUT */ ComDiagsArea *diags);

  char *sqlStmtStr() { return sqlStmtStr_; }
  ContextCli *getContext() { return currContext_; }

 private:
  struct Attrs {
    int fsDatatype_;
    int nullFlag_;
    int length_;
    int varOffset_;
    int indOffset_;
    int vcIndLen_;
  };

  SQLMODULE_ID *module_;
  SQLSTMT_ID *stmt_;
  SQLDESC_ID *sql_src_;
  SQLDESC_ID *input_desc_;
  SQLDESC_ID *output_desc_;
  char *outputBuf_;
  int isoMapping_;
  int outputDatalen_;

  char *explainData_;
  int explainDataLen_;

  int numInputEntries_;
  int numOutputEntries_;
  struct Attrs *inputAttrs_;
  struct Attrs *outputAttrs_;

  SQLDESC_ID *rs_input_maxsize_desc_;
  int rs_maxsize_;

  char *inputBuf_;
  int inputDatalen_;

  SQLMODULE_ID *moduleWithCK_;
  SQLSTMT_ID *stmtWithCK_;
  SQLDESC_ID *sql_src_withCK_;
  SQLDESC_ID *input_desc_withCK_;
  SQLDESC_ID *output_desc_withCK_;
  char *outputBuf_withCK_;

  // variables to process rowwise rowset
  int rsMaxsize_;      // max number of of rows in a rowset
  char *rsInputBuffer_;  // rwrs buffer passed to sql/cli
  int currRSrow_;      // current number of rows in the rsInputBuffer_

  int numQuadFields_;
  struct SQLCLI_QUAD_FIELDS *quadFields_;

  CollHeap *heap_;
  NABoolean needToDestroyHeap_;  // TRUE if we created this heap ourselves

  ContextCli *currContext_;
  const char *parentQid_;

  int flags_;

  // max length of prepared stmt 1000 bytes.
  char sqlStmtStr_[1001];
};

#endif
