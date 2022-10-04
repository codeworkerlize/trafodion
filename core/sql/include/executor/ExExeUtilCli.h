// @@@ START COPYRIGHT @@@
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// @@@ END COPYRIGHT @@@

#ifndef EX_EXE_UTIL_CLI_H
#define EX_EXE_UTIL_CLI_H
#include "common/ComTransInfo.h"

class ContextCli;

class OutputInfo {
 public:
  enum { MAX_OUTPUT_ENTRIES = 100 };

  OutputInfo(int numEntries);
  void dealloc(CollHeap *heap);
  void insert(int index, char *data);
  void insert(int index, char *data, int len);
  void insert(int index, char *data, int len, int type, int *indOffset = NULL, int *varOffset = NULL);
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
  ExeCliInterface(CollHeap *heap = NULL, Int32 isoMapping = 0, ContextCli *currContext = NULL,
                  const char *parentQid = NULL);

  virtual ~ExeCliInterface();

  int allocStuff(SQLMODULE_ID *&module, SQLSTMT_ID *&stmt, SQLDESC_ID *&sql_src, SQLDESC_ID *&input_desc,
                   SQLDESC_ID *&output_desc, const char *stmtName = NULL);

  int deallocStuff(SQLMODULE_ID *&module, SQLSTMT_ID *&stmt, SQLDESC_ID *&sql_src, SQLDESC_ID *&input_desc,
                     SQLDESC_ID *&output_desc);

  int executeImmediate(const char *stmt, char *outputBuf = NULL, int *outputBufLen = NULL,
                         NABoolean nullTerminate = TRUE, long *rowsAffected = NULL, NABoolean monitorThis = FALSE,
                         ComDiagsArea **globalDiags = NULL);

  int executeImmediatePrepare(const char *stmt, char *outputBuf = NULL, int *outputBufLen = NULL,
                                long *rowsAffected = NULL, NABoolean monitorThis = FALSE, char *stmtName = NULL);

  int executeImmediatePrepare2(const char *stmt, char *uniqueStmtId, int *uniqueStmtIdLen,
                                 SQL_QUERY_COST_INFO *query_cost_info, SQL_QUERY_COMPILER_STATS_INFO *comp_stats_info,
                                 char *outputBuf = NULL, int *outputBufLen = NULL, long *rowsAffected = NULL,
                                 NABoolean monitorThis = FALSE, Int32 *retGenCodeSize = NULL);

  // retrieve generated code for a previously prepared stmt
  int getGeneratedCode(char *genCodeBuf, Int32 genCodeSize);

  int executeImmediateExec(const char *stmt, char *outputBuf = NULL, int *outputBufLen = NULL,
                             NABoolean nullTerminate = TRUE, long *rowsAffected = NULL,
                             ComDiagsArea **diagsArea = NULL);

  int prepare(const char *stmtStr, SQLMODULE_ID *module, SQLSTMT_ID *stmt, SQLDESC_ID *sql_src,
                SQLDESC_ID *input_desc, SQLDESC_ID *output_desc, char **outputBuf, Queue *outputVarPtrList = NULL,
                char **inputBuf = NULL, Queue *inputVarPtrList = NULL, char *uniqueStmtId = NULL,
                int *uniqueStmtIdLen = NULL, SQL_QUERY_COST_INFO *query_cost_info = NULL,
                SQL_QUERY_COMPILER_STATS_INFO *comp_stats_info = NULL, NABoolean monitorThis = FALSE,
                NABoolean doNotCachePlan = FALSE, Int32 *retGenCodeSize = NULL);

  int setupExplainData(SQLMODULE_ID *module, SQLSTMT_ID *stmt);
  int setupExplainData();
  char *getExplainDataPtr() { return explainData_; }
  int getExplainDataLen() { return explainDataLen_; }

  int exec(char *inputBuf = NULL, int inputBufLen = 0);
  int fetch();

  // if ignoreIfNotOpen is TRUE, do not return an error is cursor is not open
  int close(NABoolean ignoreIfNotOpen = FALSE);

  int dealloc();

  short clearExecFetchClose(char *inputBuf, int inputBufLen, char *outputBuf = NULL, int *outputBufLen = 0);

  short clearExecFetchCloseOpt(char *inputBuf, int inputBufLen, char *outputBuf = NULL, int *outputBufLen = 0,
                               long *rowsAffected = NULL);

  int executeImmediateCEFC(const char *stmtStr, char *inputBuf, int inputBufLen, char *outputBuf,
                             int *outputBufLen, long *rowsAffected = NULL);

  int rwrsPrepare(const char *stmStr, int rs_maxsize, NABoolean monitorThis = FALSE);

  int rwrsExec(char *inputRow, Int32 inputRowLen, long *rowsAffected);

  int rwrsClose();

  int cwrsAllocStuff(SQLMODULE_ID *&module, SQLSTMT_ID *&stmt, SQLDESC_ID *&sql_src, SQLDESC_ID *&input_desc,
                       SQLDESC_ID *&output_desc, SQLDESC_ID *&rs_input_maxsize_desc, const char *stmtName = NULL);

  int cwrsDeallocStuff(SQLMODULE_ID *&module, SQLSTMT_ID *&stmt, SQLDESC_ID *&sql_src, SQLDESC_ID *&input_desc,
                         SQLDESC_ID *&output_desc, SQLDESC_ID *&rs_input_maxsize_desc);

  int cwrsPrepare(const char *stmtStr, int rs_maxsize, NABoolean monitorThis = FALSE);

  int cwrsExec(char *inputRow, Int32 inputRowLen, long *rowsAffected);

  int cwrsClose(long *rowsAffected);

  int setInputValue(short entry, char *ptr, int len);
  // TODO support more data types
  inline int setInputValue(short entry, char *ptr) {
    return setInputValue(entry, ptr, (ptr == NULL) ? 0 : strlen(ptr));
  }
  inline int setInputValue(short entry, int val) { return setInputValue(entry, (char *)&val, 4); }
  int getPtrAndLen(short entry, char *&ptr, int &len, short **ind = NULL);
  int getHeadingAndLen(short entry, char *heading, int &len);

  int getNumEntries(int &numInput, int &numOutput);
  int getAttributes(short entry, NABoolean forInput, int &fsDatatype, int &length, int &vcIndLen,
                      int *indOffset, int *varOffset);
  int getDataOffsets(short entry, int forInput, int *indOffset, int *varOffset);

  int getStmtAttr(char *stmtName, int attrName, int *numeric_value, char *string_value);

  short fetchRowsPrologue(const char *sqlStrBuf, NABoolean noExec = FALSE, NABoolean monitorThis = FALSE,
                          char *stmtName = NULL);
  short fetchRowsEpilogue(const char *sqlStrBuf, NABoolean noClose = FALSE);

  short initializeInfoList(Queue *&infoList, NABoolean infoListIsOutputInfo);

  short fetchAllRows(Queue *&infoList, const char *query, int numOutputEntries = 0, NABoolean varcharFormat = FALSE,
                     NABoolean monitorThis = FALSE, NABoolean initInfoList = FALSE);

  short prepareAndExecRowsPrologue(const char *sqlInitialStrBuf, char *sqlSecondaryStrBuf,
                                   Queue *initialOutputVarPtrList, Queue *continuingOutputVarPtrList,
                                   long &rowsAffected, NABoolean monitorThis = FALSE);

  short execContinuingRows(Queue *secondaryOutputVarPtrs, long &rowsAffected);

  void setOutputPtrsAsInputPtrs(Queue *entry, SQLDESC_ID *target_inputDesc = NULL);

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
  Int32 outputDatalen() { return outputDatalen_; };

  char *inputBuf() { return inputBuf_; };
  Int32 inputDatalen() { return inputDatalen_; };

  NABoolean isAllocated() { return (stmt_ ? TRUE : FALSE); }

  void clearGlobalDiags();

  Int32 getIsoMapping() { return isoMapping_; };
  void setIsoMapping(Int32 isoMapping) { isoMapping_ = isoMapping; };

  int GetRowsAffected(long *rowsAffected);

  int holdAndSetCQD(const char *defaultName, const char *defaultValue, ComDiagsArea *globalDiags = NULL);

  int holdAndSetCQDs(const char *holdStr, const char *setStr, ComDiagsArea *globalDiags = NULL);

  int restoreCQD(const char *defaultName, ComDiagsArea *globalDiags = NULL);

  int getCQDval(const char *defaultName, char *val, ComDiagsArea *globalDiags = NULL);
  int restoreCQDs(const char *restoreStr, ComDiagsArea *globalDiags = NULL);

  void setNotExeUtilInternalQuery(NABoolean v) {
    (v ? flags_ |= NOT_EXEUTIL_INTERNAL_QUERY : flags_ &= ~NOT_EXEUTIL_INTERNAL_QUERY);
  };
  NABoolean notExeUtilInternalQuery() { return (flags_ & NOT_EXEUTIL_INTERNAL_QUERY) != 0; };

  void setCursorOpen(NABoolean v) { (v ? flags_ |= CURSOR_OPEN : flags_ &= ~CURSOR_OPEN); };
  NABoolean cursorOpen() { return (flags_ & CURSOR_OPEN) != 0; };

  int setCQS(const char *shape, ComDiagsArea *globalDiags = NULL);
  int resetCQS(ComDiagsArea *globalDiags = NULL);

  // methods for routine invocation
  int getRoutine(
      /* IN */ const char *serializedInvocationInfo,
      /* IN */ Int32 invocationInfoLen,
      /* IN */ const char *serializedPlanInfo,
      /* IN */ Int32 planInfoLen,
      /* IN */ Int32 language,
      /* IN */ Int32 paramStyle,
      /* IN */ const char *externalName,
      /* IN */ const char *containerName,
      /* IN */ const char *externalPath,
      /* IN */ const char *librarySqlName,
      /* OUT */ Int32 *handle,
      /* IN/OUT */ ComDiagsArea *diags);

  int invokeRoutine(
      /* IN */ Int32 handle,
      /* IN */ Int32 phaseEnumAsInt,
      /* IN */ const char *serializedInvocationInfo,
      /* IN */ Int32 invocationInfoLen,
      /* OUT */ Int32 *invocationInfoLenOut,
      /* IN */ const char *serializedPlanInfo,
      /* IN */ Int32 planInfoLen,
      /* IN */ Int32 planNum,
      /* OUT */ Int32 *planInfoLenOut,
      /* IN */ char *inputRow,
      /* IN */ Int32 inputRowLen,
      /* OUT */ char *outputRow,
      /* IN */ Int32 outputRowLen,
      /* IN/OUT */ ComDiagsArea *diags);

  int getRoutineInvocationInfo(
      /* IN */ Int32 handle,
      /* IN/OUT */ char *serializedInvocationInfo,
      /* IN */ Int32 invocationInfoMaxLen,
      /* OUT */ Int32 *invocationInfoLenOut,
      /* IN/OUT */ char *serializedPlanInfo,
      /* IN */ Int32 planInfoMaxLen,
      /* IN */ Int32 planNum,
      /* OUT */ Int32 *planInfoLenOut,
      /* IN/OUT */ ComDiagsArea *diags);

  int putRoutine(
      /* IN */ Int32 handle,
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
  Int32 isoMapping_;
  Int32 outputDatalen_;

  char *explainData_;
  Int32 explainDataLen_;

  Int32 numInputEntries_;
  Int32 numOutputEntries_;
  struct Attrs *inputAttrs_;
  struct Attrs *outputAttrs_;

  SQLDESC_ID *rs_input_maxsize_desc_;
  Int32 rs_maxsize_;

  char *inputBuf_;
  Int32 inputDatalen_;

  SQLMODULE_ID *moduleWithCK_;
  SQLSTMT_ID *stmtWithCK_;
  SQLDESC_ID *sql_src_withCK_;
  SQLDESC_ID *input_desc_withCK_;
  SQLDESC_ID *output_desc_withCK_;
  char *outputBuf_withCK_;

  // variables to process rowwise rowset
  Int32 rsMaxsize_;      // max number of of rows in a rowset
  char *rsInputBuffer_;  // rwrs buffer passed to sql/cli
  Int32 currRSrow_;      // current number of rows in the rsInputBuffer_

  Int32 numQuadFields_;
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
