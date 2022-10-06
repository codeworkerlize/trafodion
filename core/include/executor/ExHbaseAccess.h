// **********************************************************************

// **********************************************************************
#ifndef EX_HBASE_ACCESS_H
#define EX_HBASE_ACCESS_H

//
// Task Definition Block
//
#include "comexe/ComKeyMDAM.h"
#include "comexe/ComKeySingleSubset.h"
#include "comexe/ComTdbHbaseAccess.h"
#include "ex_mdam.h"
#include "ex_queue.h"
#include "executor/ExDDLValidator.h"
#include "executor/ExStats.h"
#include "exp/ExpHbaseDefs.h"
#include "exp/ExpHbaseInterface.h"
#include "key_range.h"
#include "key_single_subset.h"

// -----------------------------------------------------------------------
// Classes defined in this file
// -----------------------------------------------------------------------
class ExHbaseAccessTdb;
class ExHbaseAccessTcb;

// -----------------------------------------------------------------------
// Classes referenced in this file
// -----------------------------------------------------------------------
class ex_tcb;
class ExpHbaseInterface;
class ExHbaseAccessSelectTcb;
class ExHbaseAccessUMDTcb;

#define INLINE_ROWID_LEN 255
// -----------------------------------------------------------------------
// ExHbaseAccessTdb
// -----------------------------------------------------------------------
class ExHbaseAccessTdb : public ComTdbHbaseAccess {
 public:
  // ---------------------------------------------------------------------
  // Constructor is only called to instantiate an object used for
  // retrieval of the virtual table function pointer of the class while
  // unpacking. An empty constructor is enough.
  // ---------------------------------------------------------------------
  ExHbaseAccessTdb() { master_glob_ = NULL; }

  virtual ~ExHbaseAccessTdb() {}

  // ---------------------------------------------------------------------
  // Build a TCB for this TDB. Redefined in the Executor project.
  // ---------------------------------------------------------------------
  virtual ex_tcb *build(ex_globals *globals);

  int execStatementTrigger(bool isBefore);
  void setExMasterStmtGlobals(ExMasterStmtGlobals *master_glob) { master_glob_ = master_glob; }
  ExMasterStmtGlobals *getExMasterStmtGlobals() { return master_glob_; }

 protected:
  ExMasterStmtGlobals *master_glob_;

 private:
  // ---------------------------------------------------------------------
  // !!!!!!! IMPORTANT -- NO DATA MEMBERS ALLOWED IN EXECUTOR TDB !!!!!!!!
  // *********************************************************************
  // The Executor TDB's are only used for the sole purpose of providing a
  // way to supplement the Compiler TDB's (in comexe) with methods whose
  // implementation depends on Executor objects. This is done so as to
  // decouple the Compiler from linking in Executor objects unnecessarily.
  //
  // When a Compiler generated TDB arrives at the Executor, the same data
  // image is "cast" as an Executor TDB after unpacking. Therefore, it is
  // a requirement that a Compiler TDB has the same object layout as its
  // corresponding Executor TDB. As a result of this, all Executor TDB's
  // must have absolutely NO data members, but only member functions. So,
  // if you reach here with an intention to add data members to a TDB, ask
  // yourself two questions:
  //
  // 1. Are those data members Compiler-generated?
  //    If yes, put them in the appropriate ComTdb subclass instead.
  //    If no, they should probably belong to someplace else (like TCB).
  //
  // 2. Are the classes those data members belong defined in the executor
  //    project?
  //    If your answer to both questions is yes, you might need to move
  //    the classes to the comexe project.
  // ---------------------------------------------------------------------
};

class ExHbaseAccessTcb : public ex_tcb {
  friend class ExHbaseTaskTcb;
  friend class ExHbaseScanTaskTcb;
  friend class ExHbaseScanRowwiseTaskTcb;
  friend class ExHbaseGetTaskTcb;
  friend class ExHbaseGetRowwiseTaskTcb;
  friend class ExHbaseScanSQTaskTcb;
  friend class ExHbaseGetSQTaskTcb;
  friend class ExHbaseUMDtrafUniqueTaskTcb;
  friend class ExHbaseUMDnativeUniqueTaskTcb;
  friend class ExHbaseUMDtrafSubsetTaskTcb;
  friend class ExHbaseUMDnativeSubsetTaskTcb;
  friend class ExHbaseAccessUMDTcb;

 public:
  //  ExHbaseAccessTcb( const ExHbaseAccessTdb &tdb,
  ExHbaseAccessTcb(const ComTdbHbaseAccess &tdb, ex_globals *glob);

  ~ExHbaseAccessTcb();

  void freeResources();
  virtual void registerSubtasks();  // register work procedures with scheduler

  virtual ExWorkProcRetcode work();

  ex_queue_pair getParentQueue() const { return qparent_; }
  virtual int numChildren() const { return 0; }
  virtual const ex_tcb *getChild(int /*pos*/) const { return NULL; }
  virtual NABoolean needStatsEntry();
  virtual ExOperStats *doAllocateStatsEntry(CollHeap *heap, ComTdb *tdb);
  virtual ex_tcb_private_state *allocatePstates(int &numElems, int &pstateLength);

  ExHbaseAccessStats *getHbaseAccessStats() {
    if (getStatsEntry())
      return getStatsEntry()->castToExHbaseAccessStats();
    else
      return NULL;
  }

  // Overridden by ExHbaseAccessSelectTcb, for which sampling may be used.
  virtual Float32 getSamplePercentage() const { return -1.0f; }

  int numRowsInDirectBuffer() { return directBufferRowNum_; }

  static void incrErrorCount(ExpHbaseInterface *ehi, long &totalExceptionCount, const char *tabName, const char *rowId);

  static void getErrorCount(ExpHbaseInterface *ehi, long &totalExceptionCount, const char *tabName, const char *rowId);

  void handleException(NAHeap *heap, char *loggingDdata, int loggingDataLen, ComCondition *errorCond);

  static void buildLoggingPath(const char *loggingLocation, char *logId, const char *tableName,
                               char *currCmdLoggingLocation);
  static void buildLoggingFileName(NAHeap *heap, const char *currCmdLoggingLocation, const char *tableName,
                                   const char *loggingFileNamePrefix, int instId, char *&loggingFileName);
  static short setupError(NAHeap *heap, ex_queue_pair &qparent, int retcode, const char *str, const char *str2 = NULL);

  static void extractColNameFields(char *inValPtr, short &colNameLen, char *&colName);

  int compareRowIds();

 protected:
  /////////////////////////////////////////////////////
  // Private methods.
  /////////////////////////////////////////////////////

  inline ExHbaseAccessTdb &hbaseAccessTdb() const { return (ExHbaseAccessTdb &)tdb; }

  inline ex_expr *convertExpr() const { return hbaseAccessTdb().convertExpr_; }

  inline ex_expr *updateExpr() const { return hbaseAccessTdb().updateExpr_; }

  inline ex_expr *mergeInsertExpr() const { return hbaseAccessTdb().mergeInsertExpr_; }

  inline ex_expr *lobDelExpr() const { return hbaseAccessTdb().mergeInsertExpr_; }

  inline ex_expr *mergeInsertRowIdExpr() const { return hbaseAccessTdb().mergeInsertRowIdExpr_; }

  inline ex_expr *mergeUpdScanExpr() const { return hbaseAccessTdb().mergeUpdScanExpr_; }

  inline ex_expr *returnFetchExpr() const { return hbaseAccessTdb().returnFetchExpr_; }

  inline ex_expr *returnUpdateExpr() const { return hbaseAccessTdb().returnUpdateExpr_; }

  inline ex_expr *returnMergeInsertExpr() const { return hbaseAccessTdb().returnMergeInsertExpr_; }

  inline ex_expr *scanExpr() const { return hbaseAccessTdb().scanExpr_; }

  inline ex_expr *rowIdExpr() const { return hbaseAccessTdb().rowIdExpr_; }

  inline ex_expr *encodedKeyExpr() const { return hbaseAccessTdb().encodedKeyExpr_; }

  inline ex_expr *keyColValExpr() const { return hbaseAccessTdb().keyColValExpr_; }

  inline ex_expr *partQualPreCondExpr() const { return hbaseAccessTdb().partQualPreCondExpr_; }

  inline ex_expr *insDelPreCondExpr() const { return hbaseAccessTdb().insDelPreCondExpr_; }

  inline ex_expr *scanPreCondExpr() const { return hbaseAccessTdb().scanPreCondExpr_; }

  inline ex_expr *insConstraintExpr() const { return hbaseAccessTdb().insConstraintExpr_; }

  inline ex_expr *updConstraintExpr() const { return hbaseAccessTdb().updConstraintExpr_; }

  inline ex_expr *hbaseFilterValExpr() const { return hbaseAccessTdb().hbaseFilterExpr_; }

  inline ex_expr *hbTagExpr() const { return hbaseAccessTdb().hbTagExpr_; }

  ex_expr *beginKeyExpr() const { return (keySubsetExeExpr() ? keySubsetExeExpr()->bkPred() : NULL); }

  ex_expr *endKeyExpr() const { return (keySubsetExeExpr() ? keySubsetExeExpr()->ekPred() : NULL); }

  keySingleSubsetEx *keySubsetExeExpr() const { return keySubsetExeExpr_; }

  keyMdamEx *keyMdamExeExpr() const { return keyMdamExeExpr_; }

  keyRangeEx *keyExeExpr() const {
    if (keySubsetExeExpr_)
      return keySubsetExeExpr_;
    else if (keyMdamExeExpr())
      return keyMdamExeExpr();
    else
      return NULL;
  }

  short allocateUpEntryTupps(int tupp1index, int tupp1length, int tupp2index, int tupp2length, NABoolean isVarchar,
                             short *rc);

  short moveRowToUpQueue(const char *row, int len, short *rc, NABoolean isVarchar);
  short moveRowToUpQueue(short *rc);

  short raiseError(int errcode, int *intParam1 = NULL, const char *str1 = NULL, const char *str2 = NULL);

  short setupError(int retcode, const char *str, const char *str2 = NULL);
  short handleError(short &rc);
  short handleDone(ExWorkProcRetcode &rc, long rowsAffected = 0);
  short createColumnwiseRow();
  short createRowwiseRow();
  int createSQRowFromHbaseFormat(long *lastestRowTimestamp = NULL);
  int createSQRowFromHbaseFormatMulti();
  int createSQRowFromAlignedFormat(long *lastestRowTimestamp = NULL);
  NABoolean missingValuesInAlignedFormatRow(ExpTupleDesc *tdesc, char *alignedRow, int alignedRowLen);

  short copyCell();
  int createSQRowDirect(long *lastestRowTimestamp = NULL);
  int setupSQMultiVersionRow();

  short getColPos(char *colName, int colNameLen, int &idx);
  short applyPred(ex_expr *expr, UInt16 tuppIndex = 0, char *tuppRow = NULL);
  void setupPrevRowId();
  short evalKeyColValExpr(HbaseStr &columnToCheck, HbaseStr &colValToCheck);
  short evalPartQualPreCondExpr();
  short evalInsDelPreCondExpr();
  short evalScanPreCondExpr();
  short evalConstraintExpr(ex_expr *expr, UInt16 tuppIndex = 0, char *tuppRow = NULL);
  short evalEncodedKeyExpr();
  short evalRowIdExpr(NABoolean isVarchar);
  short evalRowIdAsciiExpr(NABoolean noVarchar = FALSE);
  short evalRowIdAsciiExpr(const char *inputRowIdVals,
                           char *rowIdBuf,         // input: buffer where rowid is created
                           char *&outputRowIdPtr,  // output: ptr to rowid.
                           int excludeKey, int &outputRowIdLen);
  int setupUniqueRowIdsAndCols(ComTdbHbaseAccess::HbaseGetRows *hgr);
  int setupSubsetRowIdsAndCols(ComTdbHbaseAccess::HbaseScanRows *hsr);
  int genAndAssignSyskey(UInt16 tuppIndex, char *tuppRow);

  short createDirectAlignedRowBuffer(UInt16 tuppIndex, char *tuppRow, Queue *listOfColNames, NABoolean isUpdate,
                                     std::vector<UInt32> *posVec);

  short createDirectRowBuffer(UInt16 tuppIndex, char *tuppRow, Queue *listOfColNames, Queue *listOfOmittedColNames,
                              NABoolean isUpdate = FALSE, std::vector<UInt32> *posVec = NULL, double sampleRate = 0.0);
  short createDirectRowBuffer(Text &colFamily, Text &colName, Text &colVal);
  short createDirectRowwiseBuffer(char *inputRow);

  int initNextKeyRange(sql_buffer_pool *pool = NULL, atp_struct *atp = NULL);
  int setupUniqueKeyAndCols(NABoolean doInit);
  keyRangeEx::getNextKeyRangeReturnType setupSubsetKeys(NABoolean fetchRangeHadRows = FALSE);
  int setupSubsetKeysAndCols();

  int setupListOfColNames(Queue *listOfColNames, LIST(HbaseStr) & columns);
  int setupListOfColNames(Queue *listOfColNames, LIST(NAString) & columns);

  short setupHbaseFilterPreds();
  void setRowID(char *rowId, int rowIdLen);
  void allocateDirectBufferForJNI(UInt32 rowLen);
  void allocateDirectRowBufferForJNI(short numCols, short maxRows = 1);
  short patchDirectRowBuffers();
  short patchDirectRowIDBuffers();
  void allocateDirectRowIDBufferForJNI(short maxRows = 1);
  int copyColToDirectBuffer(BYTE *rowCurPtr, char *colName, short colNameLen, NABoolean prependNullVal, char nullVal,
                            char *colVal, int colValLen);
  short copyRowIDToDirectBuffer(HbaseStr &rowID);

  void fixObjName4PartTbl(char *objName, char *objNameUID, NABoolean replaceNameByUID);

  short numColsInDirectBuffer() {
    if (row_.val != NULL)
      return bswap_16(*(short *)row_.val);
    else
      return 0;
  }

  void setLoadDataIntoMemoryTable(NABoolean v) { loadDataIntoMemoryTable_ = v; }

  int createSQRowDirectFromMemoryTable();
  /////////////////////////////////////////////////////
  //
  // Private data.
  /////////////////////////////////////////////////////

  ex_queue_pair qparent_;
  long matches_;
  atp_struct *workAtp_;

  //  ex_tcb defined pool_, use it
  //  sql_buffer_pool * pool_;

  ExpHbaseInterface *ehi_;

  HBASE_NAMELIST hnl_;
  HbaseStr table_;
  HbaseStr sampletable_;
  HbaseStr rowId_;

  HbaseStr colFamName_;
  HbaseStr colName_;

  HbaseStr colVal_;
  long colTS_;

  Text beginRowId_;
  Text endRowId_;

  LIST(HbaseStr) rowIds_;
  LIST(HbaseStr) columns_;
  LIST(HbaseStr) deletedColumns_;
  LIST(NAString) hbaseFilterColumns_;
  LIST(NAString) hbaseFilterOps_;
  LIST(NAString) hbaseFilterValues_;

  char *asciiRow_;
  char *convertRow_;
  UInt32 convertRowLen_;
  char *updateRow_;
  char *mergeInsertRow_;
  char *rowIdRow_;
  char *rowIdAsciiRow_;
  char *beginRowIdRow_;
  char *endRowIdRow_;
  char *asciiRowMissingCols_;
  long *latestTimestampForCols_;
  long *latestVersionNumForCols_;
  char *beginKeyRow_;
  char *endKeyRow_;
  char *encodedKeyRow_;
  char *keyColValRow_;
  char *hbaseFilterValRow_;
  char *hbTagRow_;

  char *rowwiseRow_;
  int rowwiseRowLen_;
  HbaseStr prevRowId_;
  int prevRowIdMaxLen_;
  NABoolean isEOD_;

  ComTdbHbaseAccess::HbaseScanRows *hsr_;
  ComTdbHbaseAccess::HbaseGetRows *hgr_;

  keySingleSubsetEx *keySubsetExeExpr_;
  keyMdamEx *keyMdamExeExpr_;

  ComEncryption::EncryptionInfo encryptionInfo_;

  int currRowidIdx_;

  HbaseStr rowID_;

  // length of aligned format row created by eval method.
  int insertRowlen_;

  int rowIDAllocatedLen_;
  char *rowIDAllocatedVal_;
  // Direct Buffer to store multiple rowIDs
  // Format for rowID direct buffer
  // numRowIds + rowIDSuffix + rowId + rowIDSuffix + rowId + …
  // rowId len is passed as a parameter to Java functions
  // rowIDSuffix is '0' then the subsequent rowID is of length
  //                             =  passed rowID len
  //                '1' then the subsequent rowID is of length
  //                              = passed rowID len+1
  BYTE *directRowIDBuffer_;
  int directRowIDBufferLen_;
  // Current row num in the Direct Buffers
  short directBufferRowNum_;
  // rowID of the current row
  HbaseStr dbRowID_;
  // Structure to keep track of current position in the rowIDs direct buffer
  HbaseStr rowIDs_;
  // Direct buffer for one or more rows containing the column names and
  // values
  //  Format for Row direct buffer
  //   numCols
  //                __
  //    colNameLen    |
  //    colName       |   For each non-null column value
  //    colValueLen   |
  //    colValue   __ |
  //
  // The colValue will have one byte null indicator 0 or -1 if column is nullabl
  // The numRowIds, numCols, colNameLen and colValueLen are byte swapped because
  // Java always use BIG_ENDIAN format. But, the column value need not be byte
  // swapped because column value is always interpreted in the JNI side.
  //
  BYTE *directRowBuffer_;
  int directRowBufferLen_;
  short directBufferMaxRows_;
  // Structure to keep track of current row
  HbaseStr row_;
  // Structure to keep track of current position in direct row buffer
  HbaseStr rows_;

  struct ColValVec {
    int numVersions_;
    char **versionArray_;
    long *timestampArray_;
    long *versionNumArray_;
  };

  ColValVec *colValVec_;
  int colValVecSize_;
  int colValEntry_;
  Int16 asyncCompleteRetryCount_;
  NABoolean *resultArray_;
  NABoolean asyncOperation_;
  int asyncOperationTimeout_;
  ComDiagsArea *loggingErrorDiags_;
  char *loggingFileName_;
  NABoolean loggingFileCreated_;
  int recordCostTh_;
  ExDDLValidator ddlValidator_;

  NABoolean loadDataIntoMemoryTable_;
  NABoolean readFromMemoryTable_;
};

class ExHbaseTaskTcb : public ExGod {
 public:
  ExHbaseTaskTcb(ExHbaseAccessTcb *tcb, NABoolean rowsetTcb = FALSE);
  virtual ~ExHbaseTaskTcb();

  virtual ExWorkProcRetcode work(short &retval);

  virtual void init(){};

 protected:
  ExHbaseAccessTcb *tcb_;

  // To allow cancel, some tasks will need to return to the
  // scheduler occasionally.
  int batchSize_;
  NABoolean rowsetTcb_;
  int remainingInBatch_;
  int recordCostTh_;

  // To validate DDL on a Trafodion/EsgynDB table if needed
  ExDDLValidator ddlValidator_;
};

class ExHbaseScanTaskTcb : public ExHbaseTaskTcb {
 public:
  ExHbaseScanTaskTcb(ExHbaseAccessSelectTcb *tcb);

  virtual ExWorkProcRetcode work(short &retval);

  virtual void init();

 private:
  enum {
    NOT_STARTED,
    SCAN_OPEN,
    NEXT_ROW,
    NEXT_CELL,
    SCAN_CLOSE,
    CREATE_ROW,
    RETURN_ROW,
    APPLY_PRED,
    HANDLE_ERROR,
    DONE
  } step_;
};

class ExHbaseScanRowwiseTaskTcb : public ExHbaseTaskTcb {
 public:
  ExHbaseScanRowwiseTaskTcb(ExHbaseAccessSelectTcb *tcb);

  virtual ExWorkProcRetcode work(short &retval);

  virtual void init();

 private:
  enum {
    NOT_STARTED,
    SCAN_OPEN,
    NEXT_ROW,
    NEXT_CELL,
    SCAN_CLOSE,
    APPEND_ROW,
    CREATE_ROW,
    RETURN_ROW,
    APPLY_PRED,
    HANDLE_ERROR,
    DONE
  } step_;
};

class ExHbaseScanSQTaskTcb : public ExHbaseTaskTcb {
 public:
  ExHbaseScanSQTaskTcb(ExHbaseAccessSelectTcb *tcb);

  virtual ExWorkProcRetcode work(short &retval);

  int getProbeResult(char *&keyData);

  virtual void init();

 private:
  enum {
    NOT_STARTED,
    SCAN_OPEN,
    NEXT_ROW,
    NEXT_CELL,
    SCAN_CLOSE,
    APPEND_ROW,
    SETUP_MULTI_VERSION_ROW,
    CREATE_ROW,
    RETURN_ROW,
    APPLY_PRED,
    HANDLE_ERROR,
    DONE
  } step_;
};

class ExHbaseGetTaskTcb : public ExHbaseTaskTcb {
 public:
  ExHbaseGetTaskTcb(ExHbaseAccessSelectTcb *tcb);

  virtual ExWorkProcRetcode work(short &retval);

  virtual void init();

 private:
  enum {
    NOT_STARTED,
    GET_OPEN,
    NEXT_ROW,
    NEXT_CELL,
    GET_CLOSE,
    CREATE_ROW,
    APPLY_PRED,
    RETURN_ROW,
    COLLECT_STATS,
    HANDLE_ERROR,
    DONE
  } step_;
};

class ExHbaseGetRowwiseTaskTcb : public ExHbaseTaskTcb {
 public:
  ExHbaseGetRowwiseTaskTcb(ExHbaseAccessSelectTcb *tcb);

  virtual ExWorkProcRetcode work(short &retval);

  virtual void init();

 private:
  enum {
    NOT_STARTED,
    GET_OPEN,
    NEXT_ROW,
    NEXT_CELL,
    GET_CLOSE,
    APPEND_ROW,
    CREATE_ROW,
    APPLY_PRED,
    RETURN_ROW,
    COLLECT_STATS,
    HANDLE_ERROR,
    DONE
  } step_;
};

class ExHbaseGetSQTaskTcb : public ExHbaseTaskTcb {
 public:
  ExHbaseGetSQTaskTcb(ExHbaseAccessTcb *tcb, NABoolean rowsetTcb);

  virtual ExWorkProcRetcode work(short &retval);

  virtual void init();

 private:
  enum {
    NOT_STARTED,
    GET_OPEN,
    NEXT_ROW,
    NEXT_CELL,
    GET_CLOSE,
    APPEND_ROW,
    CREATE_ROW,
    APPLY_PRED,
    RETURN_ROW,
    COLLECT_STATS,
    HANDLE_ERROR,
    DONE,
    ALL_DONE
  } step_;
};

class ExHbaseAccessSelectTcb : public ExHbaseAccessTcb {
 public:
  ExHbaseAccessSelectTcb(const ExHbaseAccessTdb &tdb, ex_globals *glob);

  virtual ExWorkProcRetcode work();

  virtual Float32 getSamplePercentage() const { return samplePercentage_; }

 private:
  enum {
    NOT_STARTED,
    SELECT_INIT,
    PROCESS_PRECONDITION,
    SETUP_SCAN,
    PROCESS_SCAN,
    NEXT_SCAN,
    SETUP_GET,
    PROCESS_GET,
    NEXT_GET,
    SETUP_SCAN_KEY,
    SETUP_GET_KEY,
    PROCESS_SCAN_KEY,
    PROCESS_GET_KEY,
    SELECT_CLOSE,
    SELECT_CLOSE_NO_ERROR,
    HANDLE_ERROR,
    HANDLE_ERROR_NO_CLOSE,
    DONE
  } step_;

 protected:
  Float32 samplePercentage_;  // -1.0 if sampling not used

  ExHbaseScanTaskTcb *scanTaskTcb_;
  ExHbaseScanRowwiseTaskTcb *scanRowwiseTaskTcb_;
  ExHbaseGetTaskTcb *getTaskTcb_;
  ExHbaseGetRowwiseTaskTcb *getRowwiseTaskTcb_;
  ExHbaseScanSQTaskTcb *scanSQTaskTcb_;
  ExHbaseGetSQTaskTcb *getSQTaskTcb_;

  ExHbaseTaskTcb *scanTask_;
  ExHbaseTaskTcb *getTask_;
};

class ExHbaseAccessMdamSelectTcb : public ExHbaseAccessSelectTcb {
 public:
  ExHbaseAccessMdamSelectTcb(const ExHbaseAccessTdb &tdb, ex_globals *glob);

  virtual ExWorkProcRetcode work();

 private:
  enum {
    NOT_STARTED,
    SELECT_INIT,
    PROCESS_PRECONDITION,
    INIT_NEXT_KEY_RANGE,
    GET_NEXT_KEY_RANGE,
    PROCESS_PROBE_RANGE,
    PROCESS_FETCH_RANGE,
    SELECT_CLOSE,
    SELECT_CLOSE_NO_ERROR,
    HANDLE_ERROR,
    HANDLE_ERROR_NO_CLOSE,
    DONE
  } step_;

  // The indicator below tells if we have a key range to fetch
  // with. If we have a key range it could be fetch key range or a
  // probe key range.
  //  keyRangeEx::getNextKeyRangeReturnType keyRangeType_;

  // the key range iterator likes to know if the fetch ranges they
  // have provided were non-empty; we keep track of that here
  NABoolean fetchRangeHadRows_;

  long matchesBeforeFetch_;
};

class ExHbaseAccessDeleteTcb : public ExHbaseAccessTcb {
 public:
  ExHbaseAccessDeleteTcb(const ExHbaseAccessTdb &tdb, ex_globals *glob);

  virtual ExWorkProcRetcode work();

 private:
  enum {
    NOT_STARTED,
    DELETE_INIT,
    SETUP_DELETE,
    GET_NEXT_ROWID,
    GET_NEXT_COL,
    PROCESS_DELETE,
    ADVANCE_NEXT_ROWID,
    GET_NEXT_ROW,
    DELETE_CLOSE,
    HANDLE_ERROR,
    DONE
  } step_;

 protected:
  ComTdbHbaseAccess::HbaseGetRows *hgr_;

  const char *currColName_;
};

class ExHbaseAccessDeleteSubsetTcb : public ExHbaseAccessDeleteTcb {
 public:
  ExHbaseAccessDeleteSubsetTcb(const ExHbaseAccessTdb &tdb, ex_globals *glob);

  virtual ExWorkProcRetcode work();

 private:
  enum {
    NOT_STARTED,
    SCAN_INIT,
    SCAN_OPEN,
    NEXT_ROW,
    NEXT_CELL,
    CREATE_ROW,
    APPLY_PRED,
    SCAN_CLOSE,
    DELETE_ROW,
    HANDLE_ERROR,
    DONE
  } step_;
};

class ExHbaseAccessInsertTcb : public ExHbaseAccessTcb {
 public:
  ExHbaseAccessInsertTcb(const ExHbaseAccessTdb &tdb, ex_globals *glob);

  virtual ExWorkProcRetcode work();

 protected:
  enum {
    NOT_STARTED,
    INSERT_INIT,
    SETUP_INSERT,
    EVAL_INSERT_EXPR,
    EVAL_CONSTRAINT,
    CREATE_MUTATIONS,
    EVAL_ROWID_EXPR,
    CHECK_AND_INSERT,
    PROCESS_INSERT,
    PROCESS_INSERT_AND_CLOSE,
    RETURN_ROW,
    INSERT_CLOSE,
    HANDLE_EXCEPTION,
    HANDLE_ERROR,
    DONE,
    ALL_DONE,
    COMPLETE_ASYNC_INSERT,
    CLOSE_AND_DONE

  } step_;

  //  const char * insRowId_;
  Text insRowId_;

  Text insColFam_;
  Text insColNam_;
  Text insColVal_;
  const long *insColTS_;
};

class ExHbaseAccessInsertRowwiseTcb : public ExHbaseAccessInsertTcb {
 public:
  ExHbaseAccessInsertRowwiseTcb(const ExHbaseAccessTdb &tdb, ex_globals *glob);

  virtual ExWorkProcRetcode work();

 protected:
};

class ExHbaseAccessInsertSQTcb : public ExHbaseAccessInsertTcb {
 public:
  ExHbaseAccessInsertSQTcb(const ExHbaseAccessTdb &tdb, ex_globals *glob);

  virtual ExWorkProcRetcode work();

 private:
  long insColTSval_;
};

class ExHbaseAccessUpsertVsbbSQTcb : public ExHbaseAccessInsertTcb {
 public:
  ExHbaseAccessUpsertVsbbSQTcb(const ExHbaseAccessTdb &tdb, ex_globals *glob);

  virtual ExWorkProcRetcode work();

 protected:
  long insColTSval_;
  int currRowNum_;

  queue_index prevTailIndex_;
  queue_index nextRequest_;  // next request in down queue

  int numRetries_;

  int rowsInserted_;
  int lastHandledStep_;
  int numRowsInVsbbBuffer_;
};

class ExHbaseUMDtrafSubsetTaskTcb : public ExHbaseTaskTcb {
 public:
  ExHbaseUMDtrafSubsetTaskTcb(ExHbaseAccessUMDTcb *tcb);

  virtual ExWorkProcRetcode work(short &retval);

  virtual void init();

 private:
  enum {
    NOT_STARTED,
    SCAN_INIT,
    SCAN_OPEN,
    NEXT_ROW,
    CREATE_FETCHED_ROW,
    CREATE_UPDATED_ROW,
    EVAL_INS_CONSTRAINT,
    EVAL_UPD_CONSTRAINT,
    CREATE_MUTATIONS,
    APPLY_PRED,
    APPLY_MERGE_UPD_SCAN_PRED,
    RETURN_ROW,
    SCAN_CLOSE,
    SCAN_CLOSE_AND_INIT,
    UPDATE_ROW,
    DELETE_ROW,
    UPDATE_TAG,
    UPDATE_TAG_AFTER_ROW_UPDATE,
    EVAL_RETURN_ROW_EXPRS,
    RETURN_UPDATED_ROWS,
    HANDLE_ERROR,
    DONE
  } step_;
};

// UMD: UpdMergeDel on native Hbase table
class ExHbaseUMDnativeSubsetTaskTcb : public ExHbaseUMDtrafSubsetTaskTcb {
 public:
  ExHbaseUMDnativeSubsetTaskTcb(ExHbaseAccessUMDTcb *tcb);

  virtual ExWorkProcRetcode work(short &retval);

  virtual void init();

 private:
  enum {
    NOT_STARTED,
    SCAN_INIT,
    SCAN_OPEN,
    NEXT_ROW,
    NEXT_CELL,
    APPEND_CELL_TO_ROW,
    CREATE_FETCHED_ROWWISE_ROW,
    CREATE_UPDATED_ROWWISE_ROW,
    CREATE_MUTATIONS,
    APPLY_PRED,
    SCAN_CLOSE,
    UPDATE_ROW,
    DELETE_ROW,
    HANDLE_ERROR,
    DONE
  } step_;
};

class ExHbaseUMDtrafUniqueTaskTcb : public ExHbaseTaskTcb {
 public:
  ExHbaseUMDtrafUniqueTaskTcb(ExHbaseAccessUMDTcb *tcb);

  virtual ExWorkProcRetcode work(short &retval);

  virtual void init();

 private:
  enum {
    NOT_STARTED,
    SETUP_UMD,
    GET_NEXT_ROWID,
    NEXT_ROW,
    CREATE_FETCHED_ROW,
    CREATE_UPDATED_ROW,
    CREATE_MERGE_INSERTED_ROW,
    CREATE_MUTATIONS,
    EVAL_INS_CONSTRAINT,
    EVAL_UPD_CONSTRAINT,
    APPLY_PRED,
    APPLY_MERGE_UPD_SCAN_PRED,
    RETURN_ROW,
    GET_CLOSE,
    UPDATE_ROW,
    INSERT_ROW,
    CHECK_AND_INSERT_ROW,
    NEXT_ROW_AFTER_UPDATE,
    DELETE_ROW,
    CHECK_AND_DELETE_ROW,
    CHECK_AND_UPDATE_ROW,
    UPDATE_TAG,
    UPDATE_TAG_AFTER_ROW_UPDATE,
    EVAL_RETURN_ROW_EXPRS,
    RETURN_UPDATED_ROWS,
    COMPLETE_ASYNC_DELETE,
    COMPLETE_ASYNC_UPDATE,
    HANDLE_ERROR,
    DONE
  } step_;

 protected:
  NABoolean rowUpdated_;
  long latestRowTimestamp_;
  HbaseStr columnToCheck_;
  HbaseStr colValToCheck_;

  Int16 asyncCompleteRetryCount_;
  NABoolean *resultArray_;
  NABoolean asyncOperationLocal_;
  int asyncOperationTimeout_;
};

// UMD: unique UpdMergeDel on native Hbase table
class ExHbaseUMDnativeUniqueTaskTcb : public ExHbaseUMDtrafUniqueTaskTcb {
 public:
  ExHbaseUMDnativeUniqueTaskTcb(ExHbaseAccessUMDTcb *tcb);

  virtual ExWorkProcRetcode work(short &retval);

  virtual void init();

 private:
  enum {
    NOT_STARTED,
    SETUP_UMD,
    SCAN_INIT,
    GET_NEXT_ROWID,
    NEXT_ROW,
    NEXT_CELL,
    APPEND_CELL_TO_ROW,
    CREATE_FETCHED_ROW,
    CREATE_UPDATED_ROW,
    CREATE_MUTATIONS,
    APPLY_PRED,
    GET_CLOSE,
    UPDATE_ROW,
    DELETE_ROW,
    HANDLE_ERROR,
    DONE
  } step_;
};

class ExHbaseAccessUMDTcb : public ExHbaseAccessTcb {
 public:
  ExHbaseAccessUMDTcb(const ExHbaseAccessTdb &tdb, ex_globals *glob);

  virtual ExWorkProcRetcode work();

 private:
  enum {
    NOT_STARTED,
    UMD_INIT,
    SETUP_SUBSET,
    PROCESS_SUBSET,
    NEXT_SUBSET,
    SETUP_UNIQUE,
    PROCESS_UNIQUE,
    NEXT_UNIQUE,
    SETUP_SUBSET_KEY,
    PROCESS_SUBSET_KEY,
    SETUP_UNIQUE_KEY,
    PROCESS_UNIQUE_KEY,
    UMD_CLOSE,
    UMD_CLOSE_NO_ERROR,
    HANDLE_ERROR,
    DONE
  } step_;

  ExHbaseUMDtrafSubsetTaskTcb *umdSQSubsetTaskTcb_;
  ExHbaseUMDtrafUniqueTaskTcb *umdSQUniqueTaskTcb_;

  ComTdbHbaseAccess::HbaseScanRows *hsr_;
  ComTdbHbaseAccess::HbaseGetRows *hgr_;

  enum {
    UMD_SUBSET_TASK = 0,
    UMD_UNIQUE_TASK = 1,
    UMD_SUBSET_KEY_TASK = 2,
    UMD_UNIQUE_KEY_TASK = 3,
    UMD_MAX_TASKS = 4
  };
  NABoolean tasks_[UMD_MAX_TASKS];
};

class ExHbaseAccessSQRowsetTcb : public ExHbaseAccessTcb {
 public:
  enum {
    NOT_STARTED,
    RS_INIT,
    SETUP_UMD,
    SETUP_SELECT,
    CREATE_UPDATED_ROW,
    PROCESS_DELETE_AND_CLOSE,
    EVAL_CONSTRAINT,
    PROCESS_UPDATE_AND_CLOSE,
    PROCESS_SELECT,
    NEXT_ROW,
    RS_CLOSE,
    HANDLE_ERROR,
    DONE,
    ALL_DONE,
    ROW_DONE,
    CREATE_ROW,
    APPLY_PRED,
    RETURN_ROW,
    COMPLETE_ASYNC_OPERATION,
    CLOSE_AND_DONE
  } step_;

  ExHbaseAccessSQRowsetTcb(const ExHbaseAccessTdb &tdb, ex_globals *glob);

  virtual ExWorkProcRetcode work();
  int setupRowIds();
  int setupUniqueKey();

 private:
  int currRowNum_;

  queue_index prevTailIndex_;
  queue_index nextRequest_;  // next request in down queue

  int numRetries_;
  int lastHandledStep_;
  int numRowsInVsbbBuffer_;
};

class ExHbaseAccessDDLTcb : public ExHbaseAccessTcb {
 public:
  ExHbaseAccessDDLTcb(const ExHbaseAccessTdb &tdb, ex_globals *glob);

  virtual ExWorkProcRetcode work();

  short createHbaseTable(HbaseStr &table, const char *cf1, const char *cf2, const char *cf3);

  short dropMDtable(const char *name);

 private:
  enum { NOT_STARTED, CREATE_TABLE, DROP_TABLE, HANDLE_ERROR, DONE } step_;
};

class ExHbaseAccessInitMDTcb : public ExHbaseAccessDDLTcb {
 public:
  ExHbaseAccessInitMDTcb(const ExHbaseAccessTdb &tdb, ex_globals *glob);

  virtual ExWorkProcRetcode work();

 private:
  enum { NOT_STARTED, INIT_MD, UPDATE_MD, DROP_MD, HANDLE_ERROR, DONE } step_;
};

class ExHbaseAccessGetTablesTcb : public ExHbaseAccessTcb {
 public:
  ExHbaseAccessGetTablesTcb(const ExHbaseAccessTdb &tdb, ex_globals *glob);

  virtual ExWorkProcRetcode work();

 private:
  enum { NOT_STARTED, GET_TABLE, RETURN_ROW, CLOSE, HANDLE_ERROR, DONE } step_;
};

/////////////////////////////////////////////////////////////////
// ExHbaseCoProcAggrTdb
/////////////////////////////////////////////////////////////////
class ExHbaseCoProcAggrTdb : public ComTdbHbaseCoProcAggr {
 public:
  // ---------------------------------------------------------------------
  // Constructor is only called to instantiate an object used for
  // retrieval of the virtual table function pointer of the class while
  // unpacking. An empty constructor is enough.
  // ---------------------------------------------------------------------
  ExHbaseCoProcAggrTdb() {}

  virtual ~ExHbaseCoProcAggrTdb() {}

  // ---------------------------------------------------------------------
  // Build a TCB for this TDB. Redefined in the Executor project.
  // ---------------------------------------------------------------------
  virtual ex_tcb *build(ex_globals *globals);

 private:
  // ---------------------------------------------------------------------
  // !!!!!!! IMPORTANT -- NO DATA MEMBERS ALLOWED IN EXECUTOR TDB !!!!!!!!
  // *********************************************************************
  // The Executor TDB's are only used for the sole purpose of providing a
  // way to supplement the Compiler TDB's (in comexe) with methods whose
  // implementation depends on Executor objects. This is done so as to
  // decouple the Compiler from linking in Executor objects unnecessarily.
  //
  // When a Compiler generated TDB arrives at the Executor, the same data
  // image is "cast" as an Executor TDB after unpacking. Therefore, it is
  // a requirement that a Compiler TDB has the same object layout as its
  // corresponding Executor TDB. As a result of this, all Executor TDB's
  // must have absolutely NO data members, but only member functions. So,
  // if you reach here with an intention to add data members to a TDB, ask
  // yourself two questions:
  //
  // 1. Are those data members Compiler-generated?
  //    If yes, put them in the appropriate ComTdb subclass instead.
  //    If no, they should probably belong to someplace else (like TCB).
  //
  // 2. Are the classes those data members belong defined in the executor
  //    project?
  //    If your answer to both questions is yes, you might need to move
  //    the classes to the comexe project.
  // ---------------------------------------------------------------------
};

class ExHbaseCoProcAggrTcb : public ExHbaseAccessTcb {
 private:
  enum { NOT_STARTED, COPROC_INIT, COPROC_EVAL, RETURN_ROW, HANDLE_ERROR, DONE } step_;

 public:
  ExHbaseCoProcAggrTcb(const ComTdbHbaseCoProcAggr &tdb, ex_globals *glob);

  //  virtual void registerSubtasks();  // register work procedures with scheduler

  virtual ExWorkProcRetcode work();

  inline ExHbaseCoProcAggrTdb &hbaseAccessTdb() const { return (ExHbaseCoProcAggrTdb &)tdb; }

 private:
  int aggrIdx_;
};

class ExHbaseAccessBulkLoadTaskTcb : public ExHbaseAccessTcb {
 public:
  ExHbaseAccessBulkLoadTaskTcb(const ExHbaseAccessTdb &tdb, ex_globals *glob);

  virtual ExWorkProcRetcode work();

 private:
  enum {
    NOT_STARTED,
    GET_NAME,
    LOAD_CLEANUP,
    COMPLETE_LOAD,
    LOAD_CLOSE,
    LOAD_CLOSE_AND_DONE,
    HANDLE_ERROR,
    HANDLE_ERROR_AND_CLOSE,
    DONE
  } step_;

  Text hBulkLoadPrepPath_;
  Text hBulkLoadSamplePrepPath_;
};

#endif
