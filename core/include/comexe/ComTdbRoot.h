

#ifndef COM_ROOT_H
#define COM_ROOT_H

#include "comexe/ComTdb.h"
#include "comexe/ComTdbStats.h"
#include "comexe/FragDir.h"
#include "comexe/LateBindInfo.h"
#include "comexe/SqlTableOpenInfo.h"  // for SqlTableOpenInfo
#include "common/ComTransInfo.h"
#include "exp/exp_expr.h"  // for InputOutputExpr

class Descriptor;
class TransMode;
class ExFragDir;
class LateNameInfoList;
class TrafQuerySimilarityInfo;
class Queue;

typedef NABasicPtrTempl<long> Int64Ptr;  // Needed for triggersList_

class QCacheInfo {
 public:
  QCacheInfo(long planId, NABasicPtr parameterBuffer)
      : planId_(-1), parameterBuffer_(NULL), tablenameParameterBuffer_(NULL), flags_(0), filler_(0) {}

  long getPlanId() { return planId_; }
  void setPlanId(long planId) { planId_ = planId; }

  NABasicPtr getParameterBuffer() { return parameterBuffer_; }
  void setParameterBuffer(NABasicPtr parameterBuffer) { parameterBuffer_ = parameterBuffer; }

  NABasicPtr getTablenameParameterBuffer() { return tablenameParameterBuffer_; }
  void setTablenameParameterBuffer(NABasicPtr tablenameParameterBuffer) {
    tablenameParameterBuffer_ = tablenameParameterBuffer;
  }

  int unpack(void *base) {
    if (parameterBuffer_.unpack(base)) return -1;

    if (tablenameParameterBuffer_.unpack(base)) return -1;

    return 0;
  }

  Long pack(void *space) {
    parameterBuffer_.pack(space);
    tablenameParameterBuffer_.pack(space);
    return 0;
  }

  void setCacheWasHit(short v) { (v ? flags_ |= CACHE_WAS_HIT : flags_ &= ~CACHE_WAS_HIT); };
  NABoolean cacheWasHit() { return (flags_ & CACHE_WAS_HIT) != 0; };

 private:
  enum { CACHE_WAS_HIT = 0x0001 };

  UInt32 flags_;
  UInt32 filler_;
  long planId_;
  NABasicPtr parameterBuffer_;
  NABasicPtr tablenameParameterBuffer_;
};

// class to hold information related to rowwise rowset.
// Used at runtime.
class RWRSInfo {
  friend class ComTdbRoot;

 public:
  RWRSInfo()
      : rwrsMaxSize_(0),
        rwrsInputSizeIndex_(0),
        rwrsMaxInputRowlenIndex_(0),
        rwrsBufferAddrIndex_(0),
        rwrsPartnNumIndex_(0),
        rwrsMaxInternalRowlen_(0),
        rwrsInternalBufferAddr_(0),
        rwrsDcompressedBufferAddr_(0),
        rwrsDcomBufLen_(0),
        flags_(0) {}

  int rwrsMaxSize() { return rwrsMaxSize_; }
  int rwrsInputSizeIndex() { return rwrsInputSizeIndex_; }
  int rwrsMaxInputRowlenIndex() { return rwrsMaxInputRowlenIndex_; }
  int rwrsBufferAddrIndex() { return rwrsBufferAddrIndex_; }
  int rwrsPartnNumIndex() { return rwrsPartnNumIndex_; }
  int rwrsMaxInternalRowlen() { return rwrsMaxInternalRowlen_; }
  void setRwrsInfo(int maxSize, short inputSizeIndex, short maxInputRowlenIndex, short bufferAddrIndex,
                   short partnNumIndex, int maxInternalRowlen) {
    rwrsMaxSize_ = maxSize;
    rwrsInputSizeIndex_ = inputSizeIndex;
    rwrsMaxInputRowlenIndex_ = maxInputRowlenIndex;
    rwrsBufferAddrIndex_ = bufferAddrIndex;
    rwrsPartnNumIndex_ = partnNumIndex;
    rwrsMaxInternalRowlen_ = maxInternalRowlen;
  };

  void setRWRSInternalBufferAddr(char *intBuf) { rwrsInternalBufferAddr_ = intBuf; }
  char *getRWRSInternalBufferAddr() { return rwrsInternalBufferAddr_; }

  void setRWRSDcompressedBufferAddr(char *dBuf) { rwrsDcompressedBufferAddr_ = dBuf; }
  char *getRWRSDcompressedBufferAddr() { return rwrsDcompressedBufferAddr_; }

  void setRWRSDcompressedBufferLen(int len) { rwrsDcomBufLen_ = len; }
  int getRWRSDcompressedBufferLen() { return rwrsDcomBufLen_; }

  void setUseUserRWRSBuffer(short v) { (v ? flags_ |= USE_USER_RWRS_BUFFER : flags_ &= ~USE_USER_RWRS_BUFFER); };
  NABoolean useUserRWRSBuffer() { return (flags_ & USE_USER_RWRS_BUFFER) != 0; };

  void setRWRSisCompressed(NABoolean v) { (v ? flags_ |= RWRS_IS_COMPRESSED : flags_ &= ~RWRS_IS_COMPRESSED); };
  NABoolean rwrsIsCompressed() { return (flags_ & RWRS_IS_COMPRESSED) != 0; };

  void setDcompressInMaster(NABoolean v) { (v ? flags_ |= DCOMPRESS_IN_MASTER : flags_ &= ~DCOMPRESS_IN_MASTER); };
  NABoolean dcompressInMaster() { return (flags_ & DCOMPRESS_IN_MASTER) != 0; };

  void setUseUnicodeDcompress(NABoolean v) {
    (v ? flags_ |= USE_UNICODE_DCOMPRESS : flags_ &= ~USE_UNICODE_DCOMPRESS);
  };
  NABoolean useUnicodeDcompress() { return (flags_ & USE_UNICODE_DCOMPRESS) != 0; };

  void setPartnNumInBuffer(NABoolean v) { (v ? flags_ |= PARTN_NUM_IN_BUFFER : flags_ &= ~PARTN_NUM_IN_BUFFER); };
  NABoolean partnNumInBuffer() { return (flags_ & PARTN_NUM_IN_BUFFER) != 0; };

 private:
  enum {
    USE_USER_RWRS_BUFFER = 0x0001,
    RWRS_IS_COMPRESSED = 0x0002,
    DCOMPRESS_IN_MASTER = 0x0004,
    USE_UNICODE_DCOMPRESS = 0x0008,
    PARTN_NUM_IN_BUFFER = 0x0010
  };

  // max number of rows in rowwise rowset.
  int rwrsMaxSize_;

  // index into the user params to find the value of the number of
  // actual rows in the rowwise rowset buffer.
  short rwrsInputSizeIndex_;

  // index into the user params to find the value of the max length
  // of each row in the rowwise rowset buffer.
  short rwrsMaxInputRowlenIndex_;

  // index into the user params to find the value of the address
  // of rowwise rowset buffer in user space.
  short rwrsBufferAddrIndex_;

  // index into user params to find the value of the partition number
  // where this rwrs need to be shipped to.
  short rwrsPartnNumIndex_;

  UInt16 flags_;

  int rwrsMaxInternalRowlen_;

  char *rwrsInternalBufferAddr_;

  // valid if RWRS_IS_COMPRESSED and DCOMPRESS_IN_MASTER are TRUE.
  char *rwrsDcompressedBufferAddr_;
  int rwrsDcomBufLen_;

  char fillerRwrs_[32];
};

#include "common/ComSecurityKey.h"
// ---------------------------------------------------------------------
// Template instantiation to produce a 64-bit pointer emulator class
// for ComSecurityKey
// ---------------------------------------------------------------------
typedef NABasicPtrTempl<ComSecurityKey> ComSecurityKeyPtr;

class SecurityInvKeyInfo : public NAVersionedObject {
 public:
  SecurityInvKeyInfo() : NAVersionedObject(-1), numSiks_(0), sikValues_(NULL) {}

  SecurityInvKeyInfo(int numSiks, ComSecurityKey *sikValues) : numSiks_(numSiks), sikValues_(sikValues) {}

  const ComSecurityKey *getSikValues(void) { return sikValues_; }

  const int getNumSiks(void) { return numSiks_; }

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() { setImageVersionID(0, getClassVersionID()); }

  virtual short getClassSize() { return (short)sizeof(SecurityInvKeyInfo); }

  Long pack(void *);
  int unpack(void *base, void *reallocator);

 private:
  int numSiks_;                  // 00 - 03
  char sikFiller_[4];            // 04 - 07
  ComSecurityKeyPtr sikValues_;  // 08 - 15
};

// ---------------------------------------------------------------------
// Template instantiation to produce a 64-bit pointer emulator class
// for SecurityInvKeyInfo
// ---------------------------------------------------------------------
typedef NAVersionedObjectPtrTempl<SecurityInvKeyInfo> SecurityInvKeyInfoPtr;
//
// Task Definition Block
//
class ComTdbRoot : public ComTdb {
  friend class ex_root_tcb;

  // This bits are for rtFlags1_.
  // UPDATE_CURRENT_OF is set if update OR delete where current of.
  // DELETE_CURRENT_OF is set if delete where current of.
  // FROM_SHOWPLAN is set if the statement is for a showplan.
  // UPD_DEL_INSERT is set if this was an update, delete or insert qry.
  // EMBEDDED_UPDATE_OR_DELETE is set if the compiler's operation is an
  //	update of a delete (e.g. select * from (delete from t) as x;)
  // STREAM_SCAN is set if the scan never ends (regular scan turns into
  //	a delta scan (e.g. select * from stream(t);)
  // EMBEDDED_IUD_WITH_LAST1 is set for (a) select [LAST 1] ... insert ... select,
  // used for MTS insert and select [LAST 1] ... delete, used for MTS delete
  // EMBEDDED_INSERT is set for embedded INSERTs.
  // ALTPRI_MASTER -- At runtime, master's priority is lowered to be the
  //    same as ESP after fixup, if "set session default" altpri_master
  //    is set and the query is a parallel query.  This flags forces the
  //    same behavior for non-parallel queries.
  // CHECK_AUTOCOMMIT is set for self-referencing updates using the DP2
  //    locks method to prevent the Halloween problem.  This needs to
  //    raise an error if executed under AUTOCOMMIT OFF.
  // IN_MEMORY_OBJECT_DEFN : an inMemory object definition was used in this
  // query. An error will be returned at runtime, if this plan is executed.
  enum {
    DISPLAY_EXECUTION = 0x0001,
    TRANSACTION_REQD = 0x0002,
    UPDATE_CURRENT_OF = 0x0004,
    DISPLAY_EXECUTION_USING_MSGUI = 0x0008,
    UPD_ABORT_ON_ERROR = 0x0010,
    SELECT_INTO = 0x0020,
    USER_INPUT_VARS = 0x0040,
    DELETE_CURRENT_OF = 0x0080,
    UPD_DEL_INSERT = 0x0100,
    READONLY_TRANS_OK = 0x0200,
    RECOMP_WARN = 0x0400,
    FROM_SHOWPLAN = 0x0800,
    EMBEDDED_UPDATE_OR_DELETE = 0x1000,
    STREAM_SCAN = 0x2000,
    RETRYABLE_STMT = 0x4000,
    UPD_PARTIAL_ON_ERROR = 0x8000,
    SAVEPOINT_ENABLED = 0x10000,
    UPD_ERROR_ON_ERROR = 0x20000,
    PASS_TRANSACTION_IF_EXISTS = 0x40000,
    ODBC_QUERY = 0x80000,
    USER_EXPERIENCE_LEVEL_BEGINNER = 0x100000,  // for EMS event generation for other
                                                // levels than default add other flags
    QCACHE_INFO_IS_CLASS = 0x200000,
    EMBEDDED_IUD_WITH_LAST1 = 0x400000,
    EMBEDDED_INSERT = 0x800000,

    // an inMemory object definition was used in this query.
    // An error will be returned at runtime, if this plan is executed.
    IN_MEMORY_OBJECT_DEFN = 0x1000000,
    ALTPRI_MASTER = 0x2000000,
    CHECK_AUTOCOMMIT = 0x4000000,
    SINGLE_ROW_INPUT = 0x8000000,
    ROWWISE_ROWSET_INPUT = 0x10000000,
    // if AQR(auto query retry) is enabled and could be done at runtime
    AQR_ENABLED = 0x20000000,
    LRU_OPERATION = 0x40000000,
    MAINTENANCE_WINDOW = 0x80000000
  };

  // This bits are for 32-bit rtFlags2_.
  enum {
    DDL = 0x00000004,
    HDFS_ACCESS = 0x00000008,
    EXE_UTIL_RWRS = 0x00000010,
    EMBEDDED_COMPILER = 0x00000020,
    HIVE_WRITE_ACCESS = 0x00000040,
    EXE_LOB_ACCESS = 0x00000080,

    // This flag is set if the plan is compiled with run-time stats.
    AUTO_COLLECT_RTSTATS = 0x00000100,
    USE_HDFS_WRITE_LOCK = 0x00000200,
    USE_REGION_XN = 0x00000400
  };

  // Use these values in 16-bit rtFlags3_
  enum { LOG_IPC_ERRORS_RETRIED = 0x0001, AQR_WNR_DELETE_CONTINUE = 0x0002 };

  // This bits are for 32-bit rtFlags4_.
  enum {
    MAY_ALT_DB = 0x00000001,
    SUSPEND_LOCK = 0x00000002,
    QUERY_LIMIT_DEBUG = 0x00000008,
    CANT_RECLAIM_QUERY = 0x00000010,
    UNC_PROCESS = 0x00000020,
    DP2_XNS_ENABLED = 0x00000040,
    HIVE_ALLOW_SUBDIRS = 0x00000080,
    NO_ESPS_FIXUP = 0x00000100,
    WMS_MONITOR_QUERY = 0x00000200,
    WMS_CHILD_MONITOR_QUERY = 0x00000400,
    QUERY_USES_SM = 0x00000800,
    CHILD_TDB_IS_NULL = 0x00001000,
    EXPLAIN_IN_RMS_IN_TDB = 0x00002000
  };

  // These values are for 32-bit rtFlags5_
  enum {
    PSHOLD_CLOSE_ON_ROLLBACK = 0x00000001,
    PSHOLD_UPDATE_BEFORE_FETCH = 0x00000002,
    QUERY_NO_CANCEL_BROKER = 0x00000004,
    USE_DLOCK_FOR_SHARED_CACHE = 0x00000008
  };

 protected:
  ComTdbPtr childTdb;              // 00-07
  ExCriDescPtr criDesc_;           // 08-15
  InputOutputExprPtr inputExpr_;   // 16-23
  InputOutputExprPtr outputExpr_;  // 24-31
  ExFragDirPtr fragDir_;           // 32-39
  int inputVarsSize_;              // 40-43
  UInt32 rtFlags1_;                // 44-47

  // the transaction related information that was used at
  // sql compile time. Users supply it by SET TRANSACTION
  // statement. If this information is different at query
  // execution time, then the query is recompiled. It is
  // part of similarity check that handles(will handle)
  // late binding.
  TransModePtr transMode_;  // 48-55

  // these are used to compute a row of primary key values.
  // Used when this is a select cursor updatable query.
  // This row is passed back to the caller (CLI) so it could
  // pass it on to the update statement.
  ExExprPtr pkeyExpr_;  // 56-63
  UInt32 pkeyLen_;      // 64-67

  // The next 2 fields are valid if this is an 'update
  // where current of' query.

  // Number of update columns contained in the updateColList_ (array)
  // which is used to contain the updateable columns for cursor declarations
  // and UPDATE CURRENT OF statements.
  int numUpdateCol_;        // 68-71
  Int32Ptr updateColList_;  // 72-79

  ExCriDescPtr workCriDesc_;  // 80-87

  // Maximum number of rows to be returned.
  // Executor stops processing (cancel) after
  // returning these many rows. If set to -1,
  // then all rows are to be returned.
  long firstNRows_;  // 88-95

  // Compile time estimate of the 'cost' of this query (the
  // elapsed time).
 private:
  Float64 p_cost_;  // 96-103

 protected:
  // the list of open information for all the tables in the statement
  // for which the current user need to have access to
  SqlTableOpenInfoPtrPtr stoiList_;  // 104-111

  // the list of open information for all the accessed views
  // in the stmt for which the current user need to have access to
  QueuePtr viewStoiList_;  // 112-119

  // Array containing information needed to do late name
  // resolution at runtime (fixup time).
  LateNameInfoListPtr lateNameInfoList_;  // 120-127

  // contains info used to perform similarity info at runtime.
  TrafQuerySimilarityInfoPtr qsi_;  // 128-135

  // contains the name of the cursor
  NABasicPtr fetchedCursorName_;  // 136-143

  // if fetched CursorName_ is NULL, then the fetched cursor
  // name is contained in a hostvar. The next field contains the
  // position of the hostvar in the input hvar list passed to
  // executor at runtime.
  Int16 fetchedCursorHvar_;  // 144-145

  // the no of tables in the statement for which the current
  // user need to have access to.
  Int16 tableCount_;  // 146-147

  // offset of UniqueExecuteId value in host var tupp
  int uniqueExecuteIdOffset_;  // 148-151

  // the number of temporary tables used in this statement
  Int16 tempTableCount_;  // 152-153

  // base table position in lateNameInfoList_. Used to validate
  // if the updated/deleted tablename used in "upd/del where current of"
  // is the same as the one specified in the declare cursor stmt.
  // Valid if updateCurrentOfQuery() returns TRUE.
  Int16 baseTablenamePosition_;  // 154-155

  // BertBert VV
  // Timeout (.01 seconds) for waiting on a streaming cursor.
  // If streamTimeout_ == 0 then don't wait.
  // If streamTimeout_ < 0 then never timeout
  int streamTimeout_;  // 156-159
  // BertBert ^^

  // Contains information on compound statements. At this point, if
  // this fiels contains a 1 in the rightmost bit that means we have
  // a compound statement in this subtree (which may execute or not in
  // DP2).
  Int16 compoundStmtsInfo_;  // 160-161

  // Trigger-enable: Triggers inlined in the statment are listed
  // in triggersList_, and their count is triggerCount_. Each such
  // trigger has a status bit in triggerStatusVector_ held in the TCB.
  //
  // offset of the triggers status vector

  // the next 3 fields are reserved for triggers project in release 2
  Int16 triggersCount_;       // 162-163
  int triggersStatusOffset_;  // 164-167
  Int64Ptr triggersList_;     // 168-175

  // the next 2 fields are reserved for query caching project
  NABasicPtr qCacheInfo_;  // 176-183
  int cacheVarsSize_;      // 184-187

  UInt32 rtFlags2_;  // 188-191

  // The plan id is used by the EXPLAIN stored procedure. To allow a
  // join with the STATISTICS stored procedure we supply it to the
  // root tdb.
  long explainPlanId_;  // 192-199

  // A list of referenced UDRs and the number of referenced UDRs
  SqlTableOpenInfoPtrPtr udrStoiList_;  // 200-207
  Int16 udrCount_;                      // 208-209

  Int16 queryType_;  // 210-211

  UInt32 planVersion_;  // 212-215

  // size of diagnostic area for non-atomic statements
  int notAtomicFailureLimit_;  // 216-219

  int abendType_;  // 220-223

  // contains pointer to QueryCostInfo class.
  NABasicPtr queryCostInfo_;  // 224-231

  // If this is a CALL, max number of result sets. Otherwise zero.
  Int16 maxResultSets_;  // 232-233

  // number of uninitialized mvs in the list
  Int16 uninitializedMvCount_;  // 234-235

  Int16 unused1_;   // 236-237
  Int16 rtFlags3_;  // 238-239

  // list of uninitialized mvs
  UninitializedMvNamePtr uninitializedMvList_;  // 240-247

  // contains pointer to CompilerStatsInfo class.
  NABasicPtr compilerStatsInfo_;  // 248-255

  NABasicPtr unused2_;  // 256-263

  // contains pointer to RWRSInfo class.
  NABasicPtr rwrsInfo_;  // 264-271

  UInt32 rtFlags4_;  // 272-275
  UInt32 rtFlags5_;  // 276-279

  long cpuLimit_;  // 280-287

  CompilationStatsDataPtr compilationStatsData_;  // 288-295

  Int16 cpuLimitCheckFreq_;     // 296-297
  Int16 cursorType_;            // 298-299
  Int16 subqueryType_;          //  300-301
  Int16 hdfsWriteLockTimeout_;  // 302-303

  SecurityInvKeyInfoPtr sikPtr_;   // 304-311
  Int64Ptr objectUidList_;         // 312-319
  int numObjectUids_;              // 320-323
  long queryHash_;                 // 324-327
  int clientMaxStatementPooling_;  // 328-331
  char fillersComTdbRoot2_[20];    // 332-351

  // if non zero, gives the expiration time stamp for
  // Apache Sentry authorizations (after which, this query
  // must go through re-authorization)
  long sentryAuthExpirationTimeStamp_;  // 344-351

  // predicate to be applied before a row is returned.
  ExExprPtr predExpr_;  // 352-359

  NABasicPtr snapshotscanTempLocation_;  // 360-367
  QueuePtr listOfSnapshotScanTables_;    // 368-375
  Float64 bmoMemLimitPerNode_;           // 376-383
  Float64 estBmoMemPerNode_;             // 384-391
  ComTdbPtr triggerTdb_;                 // 392-399
  Int16 numTrafReplicas_;                // 400-401

 public:
  // this list and their values must be the same as the
  // enum SQLATTRQUERY_TYPE in cli/sqlcli.h
  enum QueryType {
    SQL_OTHER = -1,
    SQL_UNKNOWN = 0,
    SQL_SELECT_UNIQUE = 1,
    SQL_SELECT_NON_UNIQUE = 2,
    SQL_INSERT_UNIQUE = 3,
    SQL_INSERT_NON_UNIQUE = 4,
    SQL_UPDATE_UNIQUE = 5,
    SQL_UPDATE_NON_UNIQUE = 6,
    SQL_DELETE_UNIQUE = 7,
    SQL_DELETE_NON_UNIQUE = 8,
    SQL_CONTROL = 9,
    SQL_SET_TRANSACTION = 10,
    SQL_SET_CATALOG = 11,
    SQL_SET_SCHEMA = 12,
    SQL_CALL_NO_RESULT_SETS = 13,
    SQL_CALL_WITH_RESULT_SETS = 14,
    SQL_SP_RESULT_SET = 15,
    SQL_INSERT_RWRS = 16,

    /* utilities, like DUP, POPULATE, etc...implemented in catman. See
       sqlcomp/parser.cpp for complete list */
    SQL_CAT_UTIL = 17,

    /* complex util statements implemented in executor by converting them
       to multiple sql queries. See optimizer/RelMisc.h, class ExeUtilExpr */
    SQL_EXE_UTIL = 18,

    /* Fast extract. Data is moved from sql tables to flat files by TSE
       processes after formatting. Queries are CPU and I/O intensive */
    SQL_SELECT_UNLOAD = 19,
    SQL_DDL = 20,
    SQL_DDL_WITH_STATUS = 21

  };

  // this list and their values must be the same as the
  // enum SQLATTR_SUBQUERY_TYPE in cli/sqlcli.h
  enum SubqueryType {
    SQL_STMT_NA = 0,
    SQL_STMT_CTAS = 1,
    SQL_STMT_GET_STATISTICS = 3,
    SQL_DESCRIBE_QUERY = 4,
    SQL_DISPLAY_EXPLAIN = 5,
    SQL_STMT_HBASE_LOAD = 6,
    SQL_STMT_HBASE_UNLOAD = 7,
    SQL_STMT_LOB_EXTRACT = 8,
    SQL_STMT_LOB_UPDATE_UTIL = 9,
    SQL_DDL_SHARED_CACHE_OP = 10
  };

  /* FOR SQL_DDL and SQL_DDL_WITH_STATUS */
  enum DDLSafenessType { SQL_SAFE_DDL = 0, SQL_UNSAFE_DDL = 1, SQL_DDL_SAFENESS_UNKNOWN = 2 };

  ComTdbRoot();

  void init(ComTdb *child_tdb, ex_cri_desc *cri_desc, InputOutputExpr *input_expr, InputOutputExpr *output_expr,
            int input_vars_size, ex_expr *pkey_expr, int pkey_len, ex_expr *pred_expr, ex_cri_desc *work_cri_desc,
            ExFragDir *fragDir, TransMode *transMode, char *fetchedCursorName, short fetchedCursorHvar,
            NABoolean delCurrOf, int numUpdateCol, int *updateColList, NABoolean selectInto, short tableCount,
            long firstNRows, NABoolean userInputVars, double cost, SqlTableOpenInfo **stoiList,
            LateNameInfoList *lateNameInfoList, Queue *viewStoiList, TrafQuerySimilarityInfo *qsi, Space *space,
            int uniqueExecuteIdOffset,  //++Triggers -
            int triggersStatusOffset, short triggersCount, long *triggersList, short tempTableCount,
            short baseTablenamePosition, NABoolean updDelInsert, NABoolean retryableStmt, NABoolean streamScan,
            NABoolean embeddedUpdateOrDelete, int streamTimeout, long explainPlanId, NABasicPtr qCacheInfo,
            int cacheVarsSize, SqlTableOpenInfo **udrStoiList, short udrCount, short maxResultSets,
            NABasicPtr queryCostInfo, UninitializedMvName *uninitializedMvList, short uninitializedMvCount,
            NABasicPtr compilerStatsInfo, NABasicPtr rwrsInfo, int numObjectUIDs, long *objectUIDs,
            CompilationStatsData *compilationStatsData, long sentryAuthExpirationTimeStamp, char *snapTmpLocation,
            Queue *listOfSnapshotscanTables, long queryHash);

  ~ComTdbRoot();

  int orderedQueueProtocol() const;

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(1, getClassVersionID());
    ComTdb::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(ComTdbRoot); }

  virtual void setPlanVersion(UInt32 value) { planVersion_ = value; }

  Long pack(void *space);
  int unpack(void *, void *reallocator);

  int describe(Descriptor *desc, short output_desc_flag);

  NABoolean isUpdateCol(const ComTdbRoot *);

  // ---------------------------------------------------------------------
  // Used by the internal SHOWPLAN command to get attributes of a TDB.
  // ---------------------------------------------------------------------
  virtual void displayContents(Space *space, int flag);

  //-------------------------------------------------------------------------
  // GSH : This function is called from within arkcmp if the user requested
  // a display of the query execution. Arkcmp calls this function with flag
  // parameter set to 1 if X Windows based gui display is to be used and flag
  // parameter set to 2 if MS Windows based gui is to be used.
  // The accessor method is called by the executor.
  //-------------------------------------------------------------------------
  void setDisplayExecution(int flag);
  int displayExecution() const;  // accessor method

  inline ExFragDir *getFragDir() { return fragDir_; };

  inline TransMode *getTransMode() { return transMode_; };

  // needed for late name resolution.
  inline InputOutputExpr *inputExpr() const { return inputExpr_; }
  inline InputOutputExpr *outputExpr() const { return outputExpr_; }

  int inputVarsSize() { return inputVarsSize_; };

  // indicates if a transaction is required at run time to execute this query.
  // Set up at code generation time.
  inline void setTransactionReqd() { rtFlags1_ |= TRANSACTION_REQD; };
  inline void setTransactionNotReqd() { rtFlags1_ &= ~TRANSACTION_REQD; };
  inline int transactionReqd() const { return rtFlags1_ & TRANSACTION_REQD; };

  inline NABoolean isEMSEventExperienceLevelBeginner() { return (rtFlags1_ & USER_EXPERIENCE_LEVEL_BEGINNER) != 0; }
  inline void setEMSEventExperienceLevelBeginner(NABoolean v) {
    (v ? rtFlags1_ |= USER_EXPERIENCE_LEVEL_BEGINNER : rtFlags1_ &= ~USER_EXPERIENCE_LEVEL_BEGINNER);
  }

  // indicates if a read-only transaction is allowed at run time for this query
  // without need to recompile.
  // Set up at bind + code generation time.
  inline void setReadonlyTransactionOK() { rtFlags1_ |= READONLY_TRANS_OK; }
  inline int readonlyTransactionOK() const { return rtFlags1_ & READONLY_TRANS_OK; }

  // Inserts into non-audited indexes do not need to run in a transaction,
  // if one does not exist. If one exists (which is the case during a create
  // index operation), need to pass transid to all ESPs during the load
  // index phase, otherwise they will get error 73s returned when they open
  // the index. This information is stored in the root tdb, so that the
  // transid can be passed to the ESPs, as needed.
  inline void setPassTransactionIfOneExists() { rtFlags1_ |= PASS_TRANSACTION_IF_EXISTS; }
  inline int passTransactionIfOneExists() { return rtFlags1_ & PASS_TRANSACTION_IF_EXISTS; }

  // From NADefaults attr RECOMPILATION_WARNINGS, set by codegen
  // to enable late-bind warnings (off by default, as Ansi + Nist do not
  // specify or expect them).
  inline void setRecompWarn() { rtFlags1_ |= RECOMP_WARN; }

  inline int recompWarn() const { return rtFlags1_ & RECOMP_WARN; }

  // REVISIT
  inline NABoolean updatableSelect() { return !!pkeyExpr_; };

  inline NABoolean updateCurrentOfQuery() const { return ((rtFlags1_ & UPDATE_CURRENT_OF) != 0); };

  inline NABoolean deleteCurrentOfQuery() const { return ((rtFlags1_ & DELETE_CURRENT_OF) != 0); };

  inline NABoolean isFromShowplan() { return ((rtFlags1_ & FROM_SHOWPLAN) != 0); };

  inline void setFromShowplan() { rtFlags1_ |= FROM_SHOWPLAN; };

  inline short baseTablenamePosition() const { return baseTablenamePosition_; };

  NABoolean getUserInputVars() const { return ((rtFlags1_ & USER_INPUT_VARS) != 0); };

  inline NABoolean selectIntoQuery() const { return ((rtFlags1_ & SELECT_INTO) != 0); };

  inline NABoolean updDelInsertQuery() const { return ((rtFlags1_ & UPD_DEL_INSERT) != 0); };

  inline NABoolean retryableStmt() const { return ((rtFlags1_ & RETRYABLE_STMT) != 0); };

  NABoolean odbcQuery() const { return ((rtFlags1_ & ODBC_QUERY) != 0); };

  NABoolean qCacheInfoIsClass() const { return ((rtFlags1_ & QCACHE_INFO_IS_CLASS) != 0); };

  NABoolean isEmbeddedIUDWithLast1() const { return ((rtFlags1_ & EMBEDDED_IUD_WITH_LAST1) != 0); }

  NABoolean isEmbeddedInsert() const { return ((rtFlags1_ & EMBEDDED_INSERT) != 0); }

  NABoolean isCheckAutoCommit() const { return ((rtFlags1_ & CHECK_AUTOCOMMIT) != 0); }

  NABoolean inMemoryObjectDefn() { return (rtFlags1_ & IN_MEMORY_OBJECT_DEFN) != 0; }

  NABoolean altpriMaster() const { return ((rtFlags1_ & ALTPRI_MASTER) != 0); }

  NABoolean singleRowInput() const { return ((rtFlags1_ & SINGLE_ROW_INPUT) != 0); }

  NABoolean rowwiseRowsetInput() const { return ((rtFlags1_ & ROWWISE_ROWSET_INPUT) != 0); }

  NABoolean isLRUOperation() const { return ((rtFlags1_ & LRU_OPERATION) != 0); }

  NABoolean ddlQuery() const { return ((rtFlags2_ & DDL) != 0); };

  NABoolean isEmbeddedCompiler() const { return ((rtFlags2_ & EMBEDDED_COMPILER) != 0); };
  NABoolean isLobExtract() const { return ((rtFlags2_ & EXE_LOB_ACCESS) != 0); };

  long sentryAuthExpirationTimeStamp() { return sentryAuthExpirationTimeStamp_; }
  char *getSnapshotScanTempLocation() { return snapshotscanTempLocation_; }
  Queue *getListOfSnapshotScanTables() { return listOfSnapshotScanTables_; }

  char *fetchedCursorName() { return fetchedCursorName_; };
  short fetchedCursorHvar() { return fetchedCursorHvar_; };

  NABoolean isAutoCollectRTStats() const { return ((rtFlags2_ & AUTO_COLLECT_RTSTATS) != 0); };

  LateNameInfoList *getLateNameInfoList() { return lateNameInfoList_; }

  SqlTableOpenInfoPtr *stoiStoiList() { return stoiList_; }

  Queue *getViewStoiList() { return viewStoiList_; }

  TrafQuerySimilarityInfo *querySimilarityInfo() { return qsi_; };

  UninitializedMvName *uninitializedMvList() { return uninitializedMvList_; }
  short uninitializedMvCount() { return uninitializedMvCount_; }

  NABoolean getUpdAbortOnError() { return ((rtFlags1_ & UPD_ABORT_ON_ERROR) != 0); };
  void setUpdAbortOnError(short value) {
    if (value)
      rtFlags1_ |= UPD_ABORT_ON_ERROR;
    else
      rtFlags1_ &= ~UPD_ABORT_ON_ERROR;
  }

  NABoolean getUpdPartialOnError() { return ((rtFlags1_ & UPD_PARTIAL_ON_ERROR) != 0); };
  void setUpdPartialOnError(short value) {
    if (value)
      rtFlags1_ |= UPD_PARTIAL_ON_ERROR;
    else
      rtFlags1_ &= ~UPD_PARTIAL_ON_ERROR;
  }

  NABoolean savepointEnabled() { return ((rtFlags1_ & SAVEPOINT_ENABLED) != 0); };
  void setSavepointEnabled(NABoolean value) {
    if (value)
      rtFlags1_ |= SAVEPOINT_ENABLED;
    else
      rtFlags1_ &= ~SAVEPOINT_ENABLED;
  }

  NABoolean getUpdErrorOnError() { return ((rtFlags1_ & UPD_ERROR_ON_ERROR) != 0); };
  void setUpdErrorOnError(short value) {
    if (value)
      rtFlags1_ |= UPD_ERROR_ON_ERROR;
    else
      rtFlags1_ &= ~UPD_ERROR_ON_ERROR;
  }

  void setOdbcQuery(short value) {
    if (value)
      rtFlags1_ |= ODBC_QUERY;
    else
      rtFlags1_ &= ~ODBC_QUERY;
  }

  void setQCacheInfoIsClass(NABoolean value) {
    if (value)
      rtFlags1_ |= QCACHE_INFO_IS_CLASS;
    else
      rtFlags1_ &= ~QCACHE_INFO_IS_CLASS;
  }

  void setEmbeddedIUDWithLast1(short value) {
    if (value)
      rtFlags1_ |= EMBEDDED_IUD_WITH_LAST1;
    else
      rtFlags1_ &= ~EMBEDDED_IUD_WITH_LAST1;
  }

  void setEmbeddedInsert(short value) {
    if (value)
      rtFlags1_ |= EMBEDDED_INSERT;
    else
      rtFlags1_ &= ~EMBEDDED_INSERT;
  }

  void setInMemoryObjectDefn(NABoolean v) {
    (v ? rtFlags1_ |= IN_MEMORY_OBJECT_DEFN : rtFlags1_ &= ~IN_MEMORY_OBJECT_DEFN);
  }

  void setSingleRowInput(NABoolean value) {
    if (value)
      rtFlags1_ |= SINGLE_ROW_INPUT;
    else
      rtFlags1_ &= ~SINGLE_ROW_INPUT;
  }

  void setRowwiseRowsetInput(NABoolean value) {
    if (value)
      rtFlags1_ |= ROWWISE_ROWSET_INPUT;
    else
      rtFlags1_ &= ~ROWWISE_ROWSET_INPUT;
  }

  void setAltpriMaster(NABoolean value) {
    if (value)
      rtFlags1_ |= ALTPRI_MASTER;
    else
      rtFlags1_ &= ~ALTPRI_MASTER;
  }

  void setCheckAutoCommit(short value) {
    if (value)
      rtFlags1_ |= CHECK_AUTOCOMMIT;
    else
      rtFlags1_ &= ~CHECK_AUTOCOMMIT;
  }

  NABoolean aqrEnabledForSqlcode(int sqlcode);
  NABoolean aqrEnabled() { return (rtFlags1_ & AQR_ENABLED) != 0; }
  void setAqrEnabled(NABoolean v) { (v ? rtFlags1_ |= AQR_ENABLED : rtFlags1_ &= ~AQR_ENABLED); }

  void setLRUOperation(short value) {
    if (value)
      rtFlags1_ |= LRU_OPERATION;
    else
      rtFlags1_ &= ~LRU_OPERATION;
  }

  void setMaintenanceWindow(NABoolean v) { (v ? rtFlags1_ |= MAINTENANCE_WINDOW : rtFlags1_ &= ~MAINTENANCE_WINDOW); }

  NABoolean isMaintenanceWindowOFF() const { return ((rtFlags1_ & MAINTENANCE_WINDOW) == 0); }

  void setDDLQuery(NABoolean v) { (v ? rtFlags2_ |= DDL : rtFlags2_ &= ~DDL); }

  void setEmbeddedCompiler(NABoolean v) { (v ? rtFlags2_ |= EMBEDDED_COMPILER : rtFlags2_ &= ~EMBEDDED_COMPILER); }
  void setLobAccess(NABoolean v) { (v ? rtFlags2_ |= EXE_LOB_ACCESS : rtFlags2_ &= ~EXE_LOB_ACCESS); }
  NABoolean hdfsAccess() const { return ((rtFlags2_ & HDFS_ACCESS) != 0); };

  void setHdfsAccess(NABoolean v) { (v ? rtFlags2_ |= HDFS_ACCESS : rtFlags2_ &= ~HDFS_ACCESS); }

  NABoolean useHdfsWriteLock() const { return ((rtFlags2_ & USE_HDFS_WRITE_LOCK) != 0); };

  void setUseHdfsWriteLock(NABoolean v) { (v ? rtFlags2_ |= USE_HDFS_WRITE_LOCK : rtFlags2_ &= ~USE_HDFS_WRITE_LOCK); }

  void setAutoCollectRTStats(NABoolean v) {
    (v ? rtFlags2_ |= AUTO_COLLECT_RTSTATS : rtFlags2_ &= ~AUTO_COLLECT_RTSTATS);
  }

  NABoolean hiveWriteAccess() { return (rtFlags2_ & HIVE_WRITE_ACCESS) != 0; }
  void setHiveWriteAccess(NABoolean v) { (v ? rtFlags2_ |= HIVE_WRITE_ACCESS : rtFlags2_ &= ~HIVE_WRITE_ACCESS); }

  NABoolean exeUtilRwrs() const { return ((rtFlags2_ & EXE_UTIL_RWRS) != 0); };

  void setExeUtilRwrs(NABoolean v) { (v ? rtFlags2_ |= EXE_UTIL_RWRS : rtFlags2_ &= ~EXE_UTIL_RWRS); }

  NABoolean useRegionXN() { return ((rtFlags2_ & USE_REGION_XN) != 0); }

  void setUseRegionXN(NABoolean v) { (v ? rtFlags2_ |= USE_REGION_XN : rtFlags2_ &= ~USE_REGION_XN); }

  NABoolean logRetriedIpcErrors() const { return ((rtFlags3_ & LOG_IPC_ERRORS_RETRIED) != 0); }

  void setLogRetriedIpcErrors(NABoolean value) {
    if (value)
      rtFlags3_ |= LOG_IPC_ERRORS_RETRIED;
    else
      rtFlags3_ &= ~LOG_IPC_ERRORS_RETRIED;
  }

  NABoolean aqrWnrDeleteContinue() const { return ((rtFlags3_ & AQR_WNR_DELETE_CONTINUE) != 0); }

  void setAqrWnrDeleteContinue(NABoolean value) {
    if (value)
      rtFlags3_ |= AQR_WNR_DELETE_CONTINUE;
    else
      rtFlags3_ &= ~AQR_WNR_DELETE_CONTINUE;
  }

  NABoolean getMayAlterDb() { return ((rtFlags4_ & MAY_ALT_DB) != 0); };

  void setMayAlterDb(NABoolean v) { (v ? rtFlags4_ |= MAY_ALT_DB : rtFlags4_ &= ~MAY_ALT_DB); }

  NABoolean getSuspendMayHoldLock() { return ((rtFlags4_ & SUSPEND_LOCK) != 0); };

  void setSuspendMayHoldLock(NABoolean v) { (v ? rtFlags4_ |= SUSPEND_LOCK : rtFlags4_ &= ~SUSPEND_LOCK); }
  NABoolean getUncProcess() { return ((rtFlags4_ & UNC_PROCESS) != 0); };

  void setUncProcess(NABoolean v) { (v ? rtFlags4_ |= UNC_PROCESS : rtFlags4_ &= ~UNC_PROCESS); }

  NABoolean getDp2XnsEnabled() { return ((rtFlags4_ & DP2_XNS_ENABLED) != 0); };

  void setDp2XnsEnabled(NABoolean v) { (v ? rtFlags4_ |= DP2_XNS_ENABLED : rtFlags4_ &= ~DP2_XNS_ENABLED); }
  NABoolean getWmsMonitorQuery() { return ((rtFlags4_ & WMS_MONITOR_QUERY) != 0); };

  void setWmsMonitorQuery(NABoolean v) { (v ? rtFlags4_ |= WMS_MONITOR_QUERY : rtFlags4_ &= ~WMS_MONITOR_QUERY); }
  NABoolean getWmsChildMonitorQuery() { return ((rtFlags4_ & WMS_CHILD_MONITOR_QUERY) != 0); };

  void setWmsChildMonitorQuery(NABoolean v) {
    (v ? rtFlags4_ |= WMS_CHILD_MONITOR_QUERY : rtFlags4_ &= ~WMS_CHILD_MONITOR_QUERY);
  }
  NABoolean noEspsFixup() { return ((rtFlags4_ & NO_ESPS_FIXUP) != 0); };

  void setNoEspsFixup(NABoolean v) { (v ? rtFlags4_ |= NO_ESPS_FIXUP : rtFlags4_ &= ~NO_ESPS_FIXUP); }

  virtual NABoolean isRoot() const { return TRUE; };

  //++ Triggers -
  inline int getUniqueExecuteIdOffset() { return uniqueExecuteIdOffset_; }
  inline int getTriggersStatusOffset() { return triggersStatusOffset_; }
  inline long *getTriggersList() { return triggersList_; }
  inline short const getTriggersCount() {
    assert((triggersCount_ > 0) == (triggersList_ != 0));
    return triggersCount_;
  }
  //-- Triggers -

  long getFirstNRows() { return firstNRows_; }

  double getCost() { return getDoubleValue((char *)&p_cost_); };

  QueryCostInfo *getQueryCostInfo() { return (QueryCostInfo *)queryCostInfo_.getPointer(); }
  NABasicPtr getQCostInfoPtr() { return queryCostInfo_; }

  QCacheInfo *qcInfo() { return (QCacheInfo *)qCacheInfo_.getPointer(); }
  NABasicPtr getQCInfoPtr() { return qCacheInfo_; }

  CompilerStatsInfo *getCompilerStatsInfo() { return (CompilerStatsInfo *)compilerStatsInfo_.getPointer(); }
  NABasicPtr getCompilerStatsInfoPtr() { return compilerStatsInfo_; }

  RWRSInfo *getRWRSInfo() { return (RWRSInfo *)rwrsInfo_.getPointer(); }
  RWRSInfo *getRWRSInfo() const { return (RWRSInfo *)rwrsInfo_.getPointer(); }
  NABasicPtr getRWRSInfoPtr() { return rwrsInfo_; }

  CompilationStatsData *getCompilationStatsData() { return (CompilationStatsData *)compilationStatsData_.getPointer(); }
  CompilationStatsDataPtr getCompilationStatsDataPtr() { return compilationStatsData_; }
  SecurityInvKeyInfo *getSikInfo() const { return (SecurityInvKeyInfo *)sikPtr_.getPointer(); }
  void setSikInfo(SecurityInvKeyInfo *sikInfo) { sikPtr_ = sikInfo; }

  int getNumberOfUnpackedSecKeys(char *base);
  const ComSecurityKey *getPtrToUnpackedSecurityInvKeys(char *base);

  // ****  information for GUI  *** -------------
  virtual const ComTdb *getChild(int pos) const;
  virtual int numChildren() const { return 1; }
  virtual const char *getNodeName() const { return "EX_ROOT"; };
  virtual int numExpressions() const { return 4; }
  virtual ex_expr *getExpressionNode(int pos) {
    if (pos == 0)
      return inputExpr_;
    else if (pos == 1)
      return outputExpr_;
    else if (pos == 2)
      return pkeyExpr_;
    else if (pos == 3)
      return predExpr_;
    return NULL;
  }

  virtual const char *getExpressionName(int pos) const {
    if (pos == 0)
      return "inputExpr_";
    else if (pos == 1)
      return "outputExpr_";
    else if (pos == 2)
      return "pkeyExpr_";
    else if (pos == 3)
      return "predExpr_";
    return NULL;
  }
  // BertBert VV
  inline NABoolean isEmbeddedUpdateOrDelete(void) { return ((rtFlags1_ & EMBEDDED_UPDATE_OR_DELETE) != 0); }

  inline NABoolean isStreamScan(void) { return ((rtFlags1_ & STREAM_SCAN) != 0); }
  // BertBert ^^

  // These are flags used by compound statements and rowsets. They
  // serve the purpose of letting the executor know certain aspects
  // of compilation. For instance, whether the query has a compound
  // statement or not. The variable compoundStmtsInfo_ gets assigned
  // one of these values. They also get used in rowsetInfo_ of
  //
  enum compoundStatements {
    COMPOUND_STATEMENT_IN_QUERY = 0x0001,  // There is a compound
                                           // statement in query
  };

  inline int getCompoundStmtsInfo() const { return compoundStmtsInfo_; };

  inline void setCompoundStmtsInfo(int info) { compoundStmtsInfo_ = (short)info; };

  // parameterBuffer_ and cacheVarsSize_ are used by Query Caching
  NABasicPtr getParameterBuffer() const {
    if (qCacheInfoIsClass())
      return ((QCacheInfo *)qCacheInfo_.getPointer())->getParameterBuffer();
    else
      return qCacheInfo_;
  }

  inline int getCacheVarsSize() const { return cacheVarsSize_; }

  inline NABoolean thereIsACompoundStatement() const {
    return ((compoundStmtsInfo_ & COMPOUND_STATEMENT_IN_QUERY) != 0);
  }

  inline void setCompoundStatement() { compoundStmtsInfo_ |= COMPOUND_STATEMENT_IN_QUERY; }

  inline SqlTableOpenInfoPtr *getUdrStoiList() const { return udrStoiList_; }

  inline short getUdrCount() const { return udrCount_; }

  inline short getMaxResultSets() const { return maxResultSets_; }

  NABoolean hasCallStmtExpressions() const;

  NABoolean containsUdrInteractions() const;

  inline int getPlanVersion() const { return planVersion_; }

  void setQueryType(QueryType q) { queryType_ = q; }

  int getQueryType() { return (int)queryType_; }

  static const char *getQueryTypeText(int queryType);

  Int16 getSubqueryType() { return subqueryType_; }

  void setSubqueryType(SubqueryType q) { subqueryType_ = q; }

  static const char *getSubqueryTypeText(Int16 subQueryType);

  inline int getNotAtomicFailureLimit() const { return notAtomicFailureLimit_; }

  inline void setNotAtomicFailureLimit(int val) { notAtomicFailureLimit_ = val; }

  inline Cardinality getAccEstRowsAccessed() {
    return (Cardinality)(getCompilerStatsInfo() ? getCompilerStatsInfo()->dp2RowsAccessed() : 0);
  }

  inline Cardinality getAccEstRowsUsed() {
    return (Cardinality)(getCompilerStatsInfo() ? getCompilerStatsInfo()->dp2RowsUsed() : 0);
  }

  NABoolean getPsholdCloseOnRollback() { return ((rtFlags5_ & PSHOLD_CLOSE_ON_ROLLBACK) != 0); };
  void setPsholdCloseOnRollback(short value) {
    if (value)
      rtFlags5_ |= PSHOLD_CLOSE_ON_ROLLBACK;
    else
      rtFlags5_ &= ~PSHOLD_CLOSE_ON_ROLLBACK;
  }
  NABoolean getPsholdUpdateBeforeFetch() { return ((rtFlags5_ & PSHOLD_UPDATE_BEFORE_FETCH) != 0); };
  void setPsholdUpdateBeforeFetch(short value) {
    if (value)
      rtFlags5_ |= PSHOLD_UPDATE_BEFORE_FETCH;
    else
      rtFlags5_ &= ~PSHOLD_UPDATE_BEFORE_FETCH;
  }

  NABoolean mayNotCancel() { return ((rtFlags5_ & QUERY_NO_CANCEL_BROKER) != 0); };
  void setMayNotCancel(short value) {
    if (value)
      rtFlags5_ |= QUERY_NO_CANCEL_BROKER;
    else
      rtFlags5_ &= ~QUERY_NO_CANCEL_BROKER;
  }

  NABoolean useDlockForSharedCache() { return ((rtFlags5_ & USE_DLOCK_FOR_SHARED_CACHE) != 0); };
  void setDlockForSharedCache(short value) {
    if (value)
      rtFlags5_ |= USE_DLOCK_FOR_SHARED_CACHE;
    else
      rtFlags5_ &= ~USE_DLOCK_FOR_SHARED_CACHE;
  }

  enum DDLSafenessType getDDLSafeness() { return ddlSafeness_; }
  void setDDLSafeness(enum DDLSafenessType x) { ddlSafeness_ = x; }

  enum { NO_ABEND = 0, SIGNED_OVERFLOW, ASSERT, INVALID_MEMORY };

  int getAbendType(void) const { return abendType_; }

  void setAbendType(int a) { abendType_ = a; }

  void setCpuLimit(long cpuLimit) { cpuLimit_ = cpuLimit; }

  long getCpuLimit() { return cpuLimit_; }

  void setBmoMemoryLimitPerNode(double limit) { bmoMemLimitPerNode_ = limit; }
  double getBmoMemoryLimitPerNode() { return bmoMemLimitPerNode_; }

  void setEstBmoMemoryPerNode(double estMem) { estBmoMemPerNode_ = estMem; }
  double getEstBmoMemoryPerNode() { return estBmoMemPerNode_; }

  NABoolean getQueryLimitDebug() const { return ((rtFlags4_ & QUERY_LIMIT_DEBUG) != 0); };

  void setQueryLimitDebug() { rtFlags4_ |= QUERY_LIMIT_DEBUG; }

  void setCpuLimitCheckFreq(int f) { cpuLimitCheckFreq_ = f; }

  Int16 getCursorType() { return cursorType_; }
  void setCursorType(Int16 cursorType) { cursorType_ = cursorType; }

  Int16 hdfsWriteLockTimeout() { return hdfsWriteLockTimeout_; }
  void setHdfsWriteLockTimeout(Int16 hdfsWriteLockTimeout) { hdfsWriteLockTimeout_ = hdfsWriteLockTimeout; }

  NABoolean cantReclaimQuery() const { return ((rtFlags4_ & CANT_RECLAIM_QUERY) != 0); };

  void setCantReclaimQuery(NABoolean value) {
    if (value)
      rtFlags4_ |= CANT_RECLAIM_QUERY;
    else
      rtFlags4_ &= ~CANT_RECLAIM_QUERY;
  }

  // For SeaMonster
  // * The "query uses SM" flag means SeaMonster is used somewhere in the
  //   query but not necessarily in this fragment
  NABoolean getQueryUsesSM() const { return (rtFlags4_ & QUERY_USES_SM) ? TRUE : FALSE; }
  void setQueryUsesSM() { rtFlags4_ |= QUERY_USES_SM; }

  NABoolean childTdbIsNull() const { return (rtFlags4_ & CHILD_TDB_IS_NULL) ? TRUE : FALSE; }
  void setChildTdbIsNull() { rtFlags4_ |= CHILD_TDB_IS_NULL; }

  const long *getUnpackedPtrToObjectUIDs(char *base) const {
    return ((long *)(base - (char *)objectUidList_.getPointer()));
  }

  const long *getObjectUIDs() const { return objectUidList_.getPointer(); }

  int getNumObjectUIDs() const { return numObjectUids_; }

  NABoolean explainInRms() const { return (rtFlags4_ & EXPLAIN_IN_RMS_IN_TDB) ? TRUE : FALSE; }
  void setExplainInRms() { rtFlags4_ |= EXPLAIN_IN_RMS_IN_TDB; }

  NABoolean hiveAllowSubdirs() const { return (rtFlags4_ & HIVE_ALLOW_SUBDIRS) ? TRUE : FALSE; }
  void setHiveAllowSubdirs() { rtFlags4_ |= HIVE_ALLOW_SUBDIRS; }

  long getQueryHash() const { return queryHash_; }

  void setClientMaxStatementPooling(int n) { clientMaxStatementPooling_ = n; }

  int getClientMaxStatementPooling() { return clientMaxStatementPooling_; }

  void setTriggerTdb(ComTdb *tdb) { triggerTdb_ = tdb; }
  ComTdb *getTriggerTdb() { return triggerTdb_; }

  void setNumTrafReplicas(Int16 n) { numTrafReplicas_ = n; }

  Int16 getNumTrafReplicas() { return numTrafReplicas_; }

  Int16 getTableCount() const { return tableCount_; }

 protected:
  enum DDLSafenessType ddlSafeness_;
};

/*****************************************************************************
  Description : Return ComTdb* depending on the position argument.
                  Position 0 means the left most child.
  Comments    :
  History     : Yeogirl Yun                                      8/22/95
                 Initial Revision.
*****************************************************************************/
inline const ComTdb *ComTdbRoot::getChild(int pos) const {
  if (pos == 0)
    return childTdb;
  else
    return NULL;
}

#endif
