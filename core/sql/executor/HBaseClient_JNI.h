// **********************************************************************
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
// **********************************************************************
#ifndef HBASE_CLIENT_H
#define HBASE_CLIENT_H
#define INLINE_COLNAME_LEN 256
#define MAX_COLNAME_LEN 32767 

#include <list>
#include "Platform.h"
#include "Collections.h"
#include "NABasicObject.h"
#include "ExStats.h"
#include "JavaObjectInterface.h"
#include "Hbase_types.h"
#include "ExpHbaseDefs.h"
#include "NAMemory.h"
#include "org_trafodion_sql_HTableClient.h"
#include "Triggers.h"

using namespace apache::hadoop::hbase::thrift;
namespace {
  typedef std::vector<Text> TextVec;
}

class ContextCli;

class HBulkLoadClient_JNI;
class BackupRestoreClient_JNI;

class ExDDLValidator;

#define NUM_HBASE_WORKER_THREADS 4

typedef enum {
  HBC_Req_Shutdown = 0
 ,HBC_Req_Drop
} HBaseClientReqType;

class HBaseClientRequest
{
public :
  HBaseClientRequest(NAHeap *heap, HBaseClientReqType reqType); 
  ~HBaseClientRequest(); 
  bool isShutDown() { return reqType_ == HBC_Req_Shutdown; }
  void setFileName(const char *fileName); 
  NAHeap *getHeap() { return heap_; }
public :
  HBaseClientReqType reqType_;
  char *fileName_;
  NAHeap *heap_;
};

// ===========================================================================
// ===== The HTableClient class implements access to the Java 
// ===== HTableClient class.
// ===========================================================================

typedef enum {
  HTC_OK     = JOI_OK
 ,HTC_FIRST  = JOI_LAST
 ,HTC_DONE   = HTC_FIRST
 ,HTC_DONE_RESULT = 1000
 ,HTC_DONE_DATA
 ,HTC_ERROR_INIT_PARAM = HTC_FIRST+1
 ,HTC_ERROR_INIT_EXCEPTION
 ,HTC_ERROR_SETTRANS_EXCEPTION
 ,HTC_ERROR_CLEANUP_EXCEPTION
 ,HTC_ERROR_CLOSE_EXCEPTION
 ,HTC_ERROR_SCANOPEN_PARAM
 ,HTC_ERROR_SCANOPEN_EXCEPTION
 ,HTC_ERROR_FETCHROWS_EXCEPTION
 ,HTC_ERROR_FETCHROWS_INVALID_DDL
 ,HTC_ERROR_SCANCLOSE_EXCEPTION
 ,HTC_ERROR_GETCLOSE_EXCEPTION
 ,HTC_ERROR_DELETEROW_PARAM
 ,HTC_ERROR_DELETEROW_EXCEPTION
 ,HTC_ERROR_CREATE_PARAM
 ,HTC_ERROR_CREATE_EXCEPTION
 ,HTC_ERROR_DROP_PARAM
 ,HTC_ERROR_DROP_EXCEPTION
 ,HTC_ERROR_EXISTS_PARAM
 ,HTC_ERROR_EXISTS_EXCEPTION
 ,HTC_ERROR_COPROC_AGGR_PARAM
 ,HTC_ERROR_COPROC_AGGR_EXCEPTION
 ,HTC_ERROR_GRANT_PARAM
 ,HTC_ERROR_GRANT_EXCEPTION
 ,HTC_ERROR_REVOKE_PARAM
 ,HTC_ERROR_REVOKE_EXCEPTION
 ,HTC_ERROR_LOCK_TIME_OUT_EXCEPTION
 ,HTC_ERROR_LOCK_ROLLBACK_EXCEPTION
 ,HTC_ERROR_DEAD_LOCK_EXCEPTION
 ,HTC_ERROR_LOCK_REGION_MOVE
 ,HTC_ERROR_LOCK_REGION_SPLIT
 ,HTC_ERROR_RPC_TIME_OUT_EXCEPTION
 ,HTC_GET_COLNAME_EXCEPTION
 ,HTC_GET_COLVAL_EXCEPTION
 ,HTC_GET_ROWID_EXCEPTION
 ,HTC_NEXTCELL_EXCEPTION
 ,HTC_ERROR_COMPLETEASYNCOPERATION_EXCEPTION
 ,HTC_ERROR_ASYNC_OPERATION_NOT_COMPLETE
 ,HTC_ERROR_WRITETOWAL_EXCEPTION
 ,HTC_ERROR_WRITEBUFFERSIZE_EXCEPTION
 ,HTC_PREPARE_FOR_NEXTCELL_EXCEPTION
 ,HTC_ERROR_CANCEL_OPERATION
 ,HTC_ERROR_LOCK_NOT_ENOUGH_RESOURCE_EXCEPTION
 ,HTC_LAST
} HTC_RetCode;

class HTableClient_JNI : public JavaObjectInterface
{
private:
  // These enum values need to remain in sync with vars defined in
  // java file TrafExtStorageUtils.java.
  enum
    {
      // synchronous replication
      SYNC_REPL                   = 0x00001,

      // incremental backup
      INCR_BACKUP                 = 0x00002,

      // asynchronous operation
      ASYNC_OPER                  = 0x00004,

      // use region transaction
      USE_REGION_XN               = 0x00008,

      // use dtm xn
      USE_TREX                    = 0x00010,

      // no conflict check at commit
      NO_CONFLICT_CHECK           = 0x00020,

      // UPDATE is UPSERT, used to tell if it is an UPDATE or UPSERT for all put operation
      PUT_IS_UPSERT               = 0x0040,
    };

public:
  enum FETCH_MODE {
      UNKNOWN = 0
    , SCAN_FETCH
    , GET_ROW
    , BATCH_GET
  };

  char *skvBuffer_;
  char *srowIDs_;
  char *srowIDsLen_;
  char *sBufSize_;
  
  HTableClient_JNI(NAHeap *heap, jobject jObj = NULL)
  :  JavaObjectInterface(heap, jObj)
  {
     heap_ = heap;
     tableName_ = NULL;
     jKvValLen_ = NULL;
     jKvValOffset_ = NULL;
     jKvQualLen_ = NULL;
     jKvQualOffset_ = NULL;
     jKvFamLen_ = NULL;
     jKvFamOffset_ = NULL;
     jTimestamp_ = NULL;
     jKvBuffer_ = NULL;
     jKvTag_ = NULL;
     jKvFamArray_ = NULL;
     jKvQualArray_ = NULL;
     jRowIDs_ = NULL;
     jKvsPerRow_ = NULL;
     currentRowNum_ = -1;
     currentRowCellNum_ = -1;
     prevRowCellNum_ = 0;
     numRowsReturned_ = 0;
     numColsInScan_ = 0;
     colNameAllocLen_ = 0;
     inlineColName_[0] = '\0';
     colName_ = NULL;
     numReqRows_ = -1;
     cleanupDone_ = FALSE;
     hbs_ = NULL;
     p_kvValLen_ = NULL;
     p_kvValOffset_ = NULL;
     p_kvFamLen_ = NULL;
     p_kvFamOffset_ = NULL;
     p_kvQualLen_ = NULL;
     p_kvQualOffset_ = NULL;
     p_timestamp_ = NULL;
     jba_kvBuffer_ = NULL;
     jba_kvFamArray_ = NULL;
     jba_kvQualArray_ = NULL;
     jba_rowID_ = NULL;
     fetchMode_ = UNKNOWN;
     p_rowID_ = NULL;
     p_kvsPerRow_ = NULL;
     numCellsReturned_ = 0;
     numCellsAllocated_ = 0;
     rowIDLen_ = 0;

     skvBuffer_ = NULL;
     sBufSize_ = NULL;
     srowIDs_ = NULL;
     srowIDsLen_ = NULL;
     useTrigger_ = FALSE;
  }

  // Destructor
  virtual ~HTableClient_JNI();
  
  HTC_RetCode init();
  
  HTC_RetCode startScan(Int64 transID, Int64 savepointID, Int64 pSavepointId,
                        Int32 isolation, const Text& startRowID, const Text& stopRowID,
                        const LIST(HbaseStr) & cols, Int64 timestamp, bool cacheBlocks,
                        bool smallScanner, Lng32 numCacheRows,
                        NABoolean preFetch,
                        int lockMode,
                        NABoolean skipReadConflict,
			NABoolean skipTransaction,
			const LIST(NAString) *inColNamesToFilter,
			const LIST(NAString) *inCompareOpList,
			const LIST(NAString) *inColValuesToCompare,
                        int numReplications = -1,
                        Float32 dopParallelScanner = 0.0f,
			Float32 samplePercent = -1.0f,
			NABoolean useSnapshotScan = FALSE,
			Lng32 snapTimeout = 0,
			char * snapName = NULL,
			char * tmpLoc = NULL,
			Lng32 espNum = 0,
                        Lng32 versions = 0,
                        Int64 minTS = -1,
                        Int64 maxTS = -1,
                        const char * hbaseAuths = NULL,
                        const char * encryptionInfo = NULL,
                        NABoolean waitOnSelectForUpdate = FALSE,
                        NABoolean firstReadBypassTm = FALSE);

  HTC_RetCode deleteRow(Int64 transID, Int64 savepointID, Int64 pSavepointId,
                        HbaseStr &rowID, const LIST(HbaseStr) *columns, 
                        Int64 timestamp, const char * hbaseAuths,
                        const char * encryptionInfo = NULL);
  HTC_RetCode setWriteBufferSize(Int64 size);
  HTC_RetCode setWriteToWAL(bool vWAL);
  HTC_RetCode coProcAggr(Int64 transID, Int64 svptId, Int64 pSvptId,
                         int isolationLevel, 
                         int lockMode, 
			 int aggrType, // 0:count, 1:min, 2:max, 3:sum, 4:avg
			 const Text& startRow, 
			 const Text& stopRow, 
			 const Text &colFamily,
			 const Text &colName,
			 const NABoolean cacheBlocks,
			 const Lng32 numCacheRows,
			 Text &aggrVal); // returned value
  void setResultInfo( jintArray jKvValLen, jintArray jKvValOffset,
                      jintArray jKvQualLen, jintArray jKvQualOffset,
                      jintArray jKvFamLen, jintArray jKvFamOffset,
                      jlongArray jTimestamp, 
                      jobjectArray jKvBuffer, jobjectArray jKvTag, 
                      jobjectArray jKvFamArray, jobjectArray jKvQualArray, jobjectArray jRowIDs,
                      jintArray jKvsPerRow, jint numCellsReturned, jint numRowsReturned);

  void setOldValueInfo(HbaseStr rowID);
  void cleanupOldValueInfo();
  
  void getResultInfo();
  void cleanupResultInfo();
  HTC_RetCode fetchRows();
  HTC_RetCode nextRow(ExDDLValidator * ddlValidator = NULL);
  HTC_RetCode getColName(int colNo,
                         char **colName,
                         short &colNameLen,
                         Int64 &timestamp);
  HTC_RetCode getColVal(int colNo,
                        BYTE *colVal,
                        Lng32 &colValLen,
                        NABoolean nullable,
                        BYTE &nullVal,
                        BYTE *tag,
                        Lng32 &tagLen,
                        const char * encryptionInfo = NULL);
  HTC_RetCode getColVal(NAHeap *heap,
                        int colNo,
                        BYTE **colVal,
                        Lng32 &colValLen,
                        const char * encryptionInfo = NULL);
  HTC_RetCode getNumCellsPerRow(int &numCells);
  HTC_RetCode getRowID(HbaseStr &rowID, const char * encryptionInfo = NULL);
  HTC_RetCode nextCell(HbaseStr &rowId,
                 HbaseStr &colFamName,
                 HbaseStr &colName,
                 HbaseStr &colVal,
                 Int64 &timestamp);
  HTC_RetCode completeAsyncOperation(int timeout, NABoolean *resultArray, short resultArrayLen);
  HTC_RetCode prepareForNextCell(int idx);
  HTC_RetCode getLockError();

  //  HTC_RetCode codeProcAggrGetResult();

  const char *getTableName();

  // Get the error description.
  static char* getErrorText(HTC_RetCode errEnum);

  void setTableName(const char *tableName)
  {
    Int32 len = strlen(tableName);

    if (tableName_ != NULL)
    {
        NADELETEBASIC(tableName_, heap_);
        tableName_ = NULL;
    }
    tableName_ = new (heap_) char[len+1];
    strcpy(tableName_, tableName);
  } 

  void setHbaseStats(ExHbaseAccessStats *hbs)
  {
    hbs_ = hbs;
  }
  void setNumColsInScan(int numColsInScan) {
     numColsInScan_ = numColsInScan;
  }
  void setNumReqRows(int numReqRows) {
     numReqRows_ = numReqRows;
  }
  void setFetchMode(HTableClient_JNI::FETCH_MODE  fetchMode) {
     fetchMode_ = fetchMode;
  }
  void setNumRowsReturned(int numRowsReturned) {
     numRowsReturned_ = numRowsReturned; 
  }
  HTableClient_JNI::FETCH_MODE getFetchMode() {
     return fetchMode_;
  }

  static void setUseTRex(UInt32 &flags, NABoolean v)
  {(v ? flags |= USE_TREX : flags &= ~USE_TREX); };
  static NABoolean useTRex(UInt32 flags) 
  { return (flags & USE_TREX) != 0; };

  static void setSyncRepl(UInt32 &flags, NABoolean v)
  {(v ? flags |= SYNC_REPL : flags &= ~SYNC_REPL); };
  static NABoolean syncRepl(UInt32 flags) 
  { return (flags & SYNC_REPL) != 0; };

  static void setUseRegionXn(UInt32 &flags, NABoolean v)
  {(v ? flags |= USE_REGION_XN : flags &= ~USE_REGION_XN); };
  static NABoolean useRegionXn(UInt32 flags) 
  { return (flags & USE_REGION_XN) != 0; };

  static void setIncrementalBackup(UInt32 &flags, NABoolean v)
  {(v ? flags |= INCR_BACKUP : flags &= ~INCR_BACKUP); };
  static NABoolean incremantalBackup(UInt32 flags) 
  { return (flags & INCR_BACKUP) != 0; };

  static void setAsyncOper(UInt32 &flags, NABoolean v)
  {(v ? flags |= ASYNC_OPER : flags &= ~ASYNC_OPER); };
  static NABoolean asyncOper(UInt32 flags) 
  { return (flags & ASYNC_OPER) != 0; };

  static void setNoConflictCheck(UInt32 &flags, NABoolean v)
  {(v ? flags |= NO_CONFLICT_CHECK : flags &= ~NO_CONFLICT_CHECK); };
  static NABoolean noConflictCheck(UInt32 flags) 
  { return (flags & NO_CONFLICT_CHECK) != 0; };

  static void setPutIsUpsert(UInt32 &flags, NABoolean v)
  {(v ? flags |= PUT_IS_UPSERT: flags &= ~PUT_IS_UPSERT); };
  static NABoolean isUpsert(UInt32 flags) 
  { return (flags & PUT_IS_UPSERT) != 0; };

  static void setFlags(UInt32 &flags, NABoolean useTRex,
                       NABoolean syncRepl, NABoolean useRegionXn,
                       NABoolean incrBackup, NABoolean asyncOper,
                       NABoolean noConflictCheck, NABoolean updateIsUpsert = FALSE);

  void setUseTrigger(NABoolean v) {useTrigger_ = v;}
  NABoolean getUseTrigger() {return useTrigger_;}

private:
  enum JAVA_METHODS {
    JM_SCAN_OPEN 
   ,JM_DELETE    
   ,JM_COPROC_AGGR
   ,JM_SET_WB_SIZE
   ,JM_SET_WRITE_TO_WAL
   ,JM_FETCH_ROWS
   ,JM_COMPLETE_PUT
   ,JM_LAST
  };
  char *tableName_; 
  jintArray jKvValLen_;
  jintArray jKvValOffset_;
  jintArray jKvQualLen_;
  jintArray jKvQualOffset_;
  jintArray jKvFamLen_;
  jintArray jKvFamOffset_;
  jlongArray jTimestamp_;
  jobjectArray jKvBuffer_;
  jobjectArray jKvTag_;
  jobjectArray jKvFamArray_;
  jobjectArray jKvQualArray_;
  jobjectArray jRowIDs_;
  jintArray jKvsPerRow_;
  jint *p_kvValLen_;
  jint *p_kvValOffset_;
  jint *p_kvQualLen_;
  jint *p_kvQualOffset_;
  jint *p_kvFamLen_;
  jint *p_kvFamOffset_;
  jlong *p_timestamp_;
  jbyteArray jba_kvBuffer_;
  jbyteArray jba_kvFamArray_;
  jbyteArray jba_kvQualArray_;
  jbyteArray jba_rowID_;
  jbyte *p_rowID_;
  jint *p_kvsPerRow_;
  jint numRowsReturned_;
  int currentRowNum_;
  int currentRowCellNum_;
  int numColsInScan_;
  int numReqRows_;
  int numCellsReturned_;
  int numCellsAllocated_;
  int prevRowCellNum_;
  int rowIDLen_;
  char *colName_;
  char inlineColName_[INLINE_COLNAME_LEN+1];
  short colNameAllocLen_;
  FETCH_MODE fetchMode_; 
  NABoolean cleanupDone_;
  ExHbaseAccessStats *hbs_;
  NABoolean useTrigger_;
  static jclass          javaClass_;  
  static JavaMethodInit* JavaMethods_;
  static bool javaMethodsInitialized_;
  // this mutex protects both JaveMethods_ and javaClass_ initialization
  static pthread_mutex_t javaMethodsInitMutex_;
};

// ===========================================================================
// ===== The HBaseClient_JNI class implements access to the Java 
// ===== HBaseClient_JNI class.
// ===========================================================================

// Keep in sync with hbcErrorEnumStr array.
typedef enum {
  HBC_OK     = JOI_OK
 ,HBC_FIRST  = HTC_LAST
 ,HBC_DONE   = HBC_FIRST
 ,HBC_ERROR_INIT_PARAM
 ,HBC_ERROR_INIT_EXCEPTION
 ,HBC_ERROR_GET_HTC_EXCEPTION
 ,HBC_ERROR_REL_HTC_EXCEPTION
 ,HBC_ERROR_CREATE_PARAM
 ,HBC_ERROR_CREATE_EXCEPTION
 ,HBC_ERROR_ALTER_PARAM
 ,HBC_ERROR_ALTER_EXCEPTION
 ,HBC_ERROR_DROP_PARAM
 ,HBC_ERROR_DROP_EXCEPTION
 ,HBC_ERROR_LIST_PARAM
 ,HBC_ERROR_LIST_EXCEPTION
 ,HBC_ERROR_EXISTS_PARAM
 ,HBC_ERROR_EXISTS_EXCEPTION
 ,HBC_ERROR_GRANT_PARAM
 ,HBC_ERROR_GRANT_EXCEPTION
 ,HBC_ERROR_REVOKE_PARAM
 ,HBC_ERROR_REVOKE_EXCEPTION
 ,HBC_ERROR_THREAD_CREATE
 ,HBC_ERROR_THREAD_REQ_ALLOC
 ,HBC_ERROR_THREAD_SIGMASK
 ,HBC_ERROR_ATTACH_JVM
 ,HBC_ERROR_GET_HBLC_EXCEPTION
 ,HBC_ERROR_GET_BRC_EXCEPTION
 ,HBC_ERROR_ROWCOUNT_EST_PARAM
 ,HBC_ERROR_ROWCOUNT_EST_EXCEPTION
 ,HBC_ERROR_ROWCOUNT_EST_FALSE
 ,HBC_ERROR_REL_HBLC_EXCEPTION
 ,HBC_ERROR_REL_BRC_EXCEPTION
 ,HBC_ERROR_GET_CACHE_FRAC_EXCEPTION
 ,HBC_ERROR_GET_LATEST_SNP_PARAM
 ,HBC_ERROR_GET_LATEST_SNP_EXCEPTION
 ,HBC_ERROR_CLEAN_SNP_TMP_LOC_PARAM
 ,HBC_ERROR_CLEAN_SNP_TMP_LOC_EXCEPTION
 ,HBC_ERROR_SET_ARC_PERMS_PARAM
 ,HBC_ERROR_SET_ARC_PERMS_EXCEPTION
 ,HBC_ERROR_STARTGET_PARAM
 ,HBC_ERROR_STARTGET_EXCEPTION
 ,HBC_ERROR_STARTGETS_PARAM
 ,HBC_ERROR_STARTGETS_EXCEPTION
 ,HBC_ERROR_GET_HBTI_PARAM
 ,HBC_ERROR_GET_HBTI_EXCEPTION
 ,HBC_ERROR_CREATE_COUNTER_PARAM
 ,HBC_ERROR_CREATE_COUNTER_EXCEPTION
 ,HBC_ERROR_INCR_COUNTER_PARAM
 ,HBC_ERROR_INCR_COUNTER_EXCEPTION
 ,HBC_ERROR_INSERTROW_PARAM
 ,HBC_ERROR_INSERTROW_EXCEPTION
 ,HBC_ERROR_INSERTROW_DUP_ROWID
 ,HBC_ERROR_INSERTROW_INVALID_DDL
 ,HBC_ERROR_INSERTROWS_PARAM
 ,HBC_ERROR_INSERTROWS_EXCEPTION
 ,HBC_ERROR_INSERTROWS_INVALID_DDL
 ,HBC_ERROR_UPDATEVISIBILITY_PARAM
 ,HBC_ERROR_UPDATEVISIBILITY_EXCEPTION
 ,HBC_ERROR_CHECKANDUPDATEROW_PARAM
 ,HBC_ERROR_CHECKANDUPDATEROW_EXCEPTION
 ,HBC_ERROR_CHECKANDUPDATEROW_NOTFOUND
 ,HBC_ERROR_DELETEROW_PARAM
 ,HBC_ERROR_DELETEROW_EXCEPTION
 ,HBC_ERROR_DELETEROW_INVALID_DDL
 ,HBC_ERROR_DELETEROWS_PARAM
 ,HBC_ERROR_DELETEROWS_EXCEPTION
 ,HBC_ERROR_DELETEROWS_INVALID_DDL
 ,HBC_ERROR_CHECKANDDELETEROW_PARAM
 ,HBC_ERROR_CHECKANDDELETEROW_EXCEPTION
 ,HBC_ERROR_CHECKANDDELETEROW_NOTFOUND
 ,HBC_ERROR_CHECKANDDELETEROW_INVALID_DDL
 ,HBC_ERROR_ADDHDFSCACHE_EXCEPTION
 ,HBC_ERROR_REMOVEHDFSCACHE_EXCEPTION
 ,HBC_ERROR_SHOWHDFSCACHE_EXCEPTION
 ,HBC_ERROR_POOL_NOT_EXIST_EXCEPTION
 ,HBC_ERROR_LISTALL
 ,HBC_ERROR_GETKEYS
 ,HBC_ERROR_REGION_STATS
 ,HBC_ERROR_CREATE_SNAPSHOT_PARAM
 ,HBC_ERROR_CREATE_SNAPSHOT_EXCEPTION
 ,HBC_ERROR_RESTORE_SNAPSHOT_PARAM
 ,HBC_ERROR_RESTORE_SNAPSHOT_EXCEPTION
 ,HBC_ERROR_DELETE_SNAPSHOT_PARAM
 ,HBC_ERROR_DELETE_SNAPSHOT_EXCEPTION
 ,HBC_ERROR_VERIFY_SNAPSHOT_PARAM
 ,HBC_ERROR_VERIFY_SNAPSHOT_EXCEPTION
 ,HBC_ERROR_SAVEPOINT_COMMIT_OR_ROLLBACK
 ,HBC_ERROR_NAMESPACE_PARAM
 ,HBC_ERROR_NAMESPACE_OPER_ERROR
 ,HBC_ERROR_NAMESPACE_NOT_EXIST
 ,HBC_ERROR_TRUNCATE_PARAM
 ,HBC_ERROR_TRUNCATE_EXCEPTION
 ,HBC_ERROR_LOCK_TIME_OUT_EXCEPTION
 ,HBC_ERROR_LOCK_ROLLBACK_EXCEPTION
 ,HBC_ERROR_LOCKREQUIRED_EXCEPTION
 ,HBC_ERROR_DEAD_LOCK_EXCEPTION
 ,HBC_ERROR_LOCK_REGION_MOVE
 ,HBC_ERROR_LOCK_REGION_SPLIT
 ,HBC_ERROR_RPC_TIME_OUT_EXCEPTION
 ,HBC_ERROR_TRIGGER_EXECUTE_EXCEPTION
 ,HBC_ERROR_TRIGGER_PARAMETER_EXCEPTION
 ,HBC_ERROR_RECONNECT_PARAM
 ,HBC_ERROR_RECONNECT_EXCEPTION
 ,HBC_ERROR_GET_NEXT_VALUE_PARAM
 ,HBC_ERROR_GET_NEXT_VALUE_EXCEPTION
 ,HBC_ERROR_DELETE_SEQ_ROW_PARAM
 ,HBC_ERROR_DELETE_SEQ_ROW_EXCEPTION
 ,HBC_ERROR_CANCEL_OPERATION_EXCEPTION
 ,HBC_ERROR_CANCEL_OPERATION
 ,HBC_ERROR_LOCK_NOT_ENOUGH_RESOURCE_EXCEPTION
 ,HBC_ERROR_GET_TABLE_DEF_FOR_BINLOG
 ,HBC_ERROR_GET_TABLE_DEF_FOR_BINLOG_PARAM
 ,HBC_ERROR_GET_TABLE_DEF_FOR_BINLOG_ERROR
 ,HBC_ERROR_PUT_SQL_TO_HBASE
 ,HBC_ERROR_PUT_SQL_TO_HBASE_PARAM
 ,HBC_ERROR_PUT_SQL_TO_HBASE_ERROR
 ,HBC_LAST
} HBC_RetCode;

class HBaseClient_JNI : public JavaObjectInterface
{
public:
  static HBaseClient_JNI* getInstance(NABoolean bigtable);
  static void deleteInstance();

  // Destructor
  virtual ~HBaseClient_JNI();
  
  // Initialize JVM and all the JNI configuration.
  // Must be called.
  HBC_RetCode init();
  
  HBC_RetCode initConnection(const char* zkServers, const char* zkPort, NABoolean bigTable); 
  bool isConnected() 
  {
    return isConnected_;
  }

  HTableClient_JNI* getHTableClient(NAHeap *heap, const char* tableName, 
				    bool useTRex, NABoolean replSync, 
                                    NABoolean incrBackupEnabled,
                                    ExHbaseAccessStats *hbs);
  HBulkLoadClient_JNI* getHBulkLoadClient(NAHeap *heap);
  HBC_RetCode releaseHBulkLoadClient(HBulkLoadClient_JNI* hblc);
  BackupRestoreClient_JNI* getBackupRestoreClient(NAHeap *heap);
  HBC_RetCode releaseBackupRestoreClient(BackupRestoreClient_JNI* brc);
  HBC_RetCode releaseHTableClient(HTableClient_JNI* htc);
  HBC_RetCode create(const char* fileName, HBASE_NAMELIST& colFamilies, NABoolean isMVCC);
  HBC_RetCode create(const char* fileName, NAText*  hbaseOptions, 
                     int numSplits, int keyLength, const char** splitValues, 
                     Int64 transID, NABoolean isMVCC,
                     NABoolean incrBackupEnabled);
  HBC_RetCode alter(const char* fileName, NAText*  hbaseOptions, Int64 transID);
  HBC_RetCode registerTruncateOnAbort(const char* fileName, Int64 transID);
  HBC_RetCode truncate(const char* fileName, NABoolean preserveSplits, Int64 transID);
  HBC_RetCode drop(const char* fileName, bool async, Int64 transID);
  HBC_RetCode drop(const char* fileName, JNIEnv* jenv, Int64 transID); // thread specific
  HBC_RetCode dropAll(const char* pattern, bool async, Int64 transID);
  HBC_RetCode copy(const char* srcTblName, const char* tgtTblName,
                   NABoolean force);
  NAArray<HbaseStr>* listAll(NAHeap *heap, const char* pattern);
  NAArray<HbaseStr>* getRegionStats(NAHeap *heap, const char* tblName);

  // retNames: NULL for create/drop operations.
  //           list of objects for list namespace/namespaceObjs.
  HBC_RetCode namespaceOperation(short oper, const char *nameSpace,
                                 Lng32 numKeyValEntries,
                                 NAText * keyArray, NAText * valArray,
                                 NAHeap * heap, NAArray<HbaseStr>* *retNames);

  // call dtm to commit or rollback a savepoint.
  // isCommit = TRUE, commit. isCommit = FALSE, rollback.
  HBC_RetCode savepointCommitOrRollback(Int64 transId,
                                        Int64 savepointId,
                                        Int64 tgtSavepointId,
                                        bool isCommit);

  HBC_RetCode exists(const char* fileName, Int64 transID);
  HBC_RetCode grant(const Text& user, const Text& tableName, const TextVec& actionCodes); 
  HBC_RetCode revoke(const Text& user, const Text& tableName, const TextVec& actionCodes);
  HBC_RetCode estimateRowCount(const char* tblName, Int32 partialRowSize,
                               Int32 numCols, Int32 retryLimitMilliSeconds, NABoolean useCoprocessor,
                               Int64& rowCount, Int32 & breadCrumb);
  static HBC_RetCode getLatestSnapshot(const char * tabname, char *& snapshotName, NAHeap * heap);
  HBC_RetCode cleanSnpTmpLocation(const char * path);
  HBC_RetCode setArchivePermissions(const char * path);
  HBC_RetCode getBlockCacheFraction(float& frac);
  HBC_RetCode getHbaseTableInfo(const char* tblName, Int32& indexLevels, Int32& blockSize);
  HBC_RetCode getRegionsNodeName(const char* tblName, Int32 partns, ARRAY(const char *)& nodeNames);

  // req processing in worker threads
  HBC_RetCode enqueueRequest(HBaseClientRequest *request);
  HBC_RetCode enqueueShutdownRequest();
  HBC_RetCode enqueueDropRequest(const char *fileName);
  HBC_RetCode doWorkInThread();
  HBC_RetCode startWorkerThreads();
  HBC_RetCode performRequest(HBaseClientRequest *request, JNIEnv* jenv);
  HBaseClientRequest* getHBaseRequest();
  bool workerThreadsStarted() { return (threadID_[0] ? true : false); }
  // Get the error description.
  static char* getErrorText(HBC_RetCode errEnum);
   
  static void logIt(const char* str);

  NABoolean isExistTrigger(BeforeAndAfterTriggers *triggers);
  static NABoolean matchTriggerOperation(Trigger *trigger,
				         ComOperation type);
  static void getTriggerInfo(NAString &triggerInfo,
		             ComOperation type,
		             NABoolean isBefore,
		             BeforeAndAfterTriggers *triggers,
		             NABoolean isStatement);
  HBC_RetCode execTriggers(NAHeap *heap,
			   const char *tableName,
			   ComOperation type,
			   NABoolean isBefore,
			   BeforeAndAfterTriggers *triggers,
			   HbaseStr rowID,
			   HbaseStr row,
			   unsigned char* base64rowVal = NULL,
			   int base64ValLen = 0,
			   unsigned char* base64rowIDVal = NULL,
			   int base64RowLen = 0,
			   short rowIDLen = 0,
			   const char * curExecSql = NULL,
			   NABoolean isStatement = false);

  HBC_RetCode startGet(NAHeap *heap, const char* tableName, bool useTRex, NABoolean replSync, Int32 lockMode,
                       NABoolean skipReadConflict,
                       ExHbaseAccessStats *hbs, Int64 transID, Int64 savepointID, Int64 pSavepointId, Int32 isolationLevel, const HbaseStr& rowID, 
                       const LIST(HbaseStr) & cols, Int64 timestamp, int replicaId, HTableClient_JNI *htc,
                       const char * hbaseAuths = NULL,
                       const char * encryptionInfo = NULL ,
                       NABoolean waitOnSelectForUpdate = FALSE,
                       NABoolean firstReadBypassTm = FALSE);

  HBC_RetCode startGets(NAHeap *heap, const char* tableName, bool useTRex, NABoolean replSync, Int32 lockMode,
                        NABoolean skipReadConflict,
                        NABoolean skipTransactionForBatchGet,
                        ExHbaseAccessStats *hbs, Int64 transID, Int64 savepointID, Int64 pSavepointId, Int32 isolationLevel, const LIST(HbaseStr) *rowIDs, 
                        short rowIDLen, const HbaseStr *rowIDsInDB, 
                        const LIST(HbaseStr) & cols, Int64 timestamp, int replicaId, HTableClient_JNI *htc,
                        const char * hbaseAuths = NULL,
                        const char * encryptionInfo = NULL);
  HBC_RetCode incrCounter( const char * tabName, const char * rowId, const char * famName, 
                           const char * qualName , Int64 incr, Int64 & count);
  HBC_RetCode createCounterTable( const char * tabName,  const char * famName);
  HBC_RetCode insertRow(NAHeap *heap, const char *tableName,
			ExHbaseAccessStats *hbs,
                        Int64 transID, Int64 savepointID, Int64 pSavepointId, HbaseStr rowID,
                        HbaseStr row, Int64 timestamp, bool checkAndPut, 
                        UInt32 flags,
                        const char * encryptionInfo,
			const char * triggers,
			const char * curExecSql,
			short colIndexToCheck,
                        HTableClient_JNI **outHtc,
                        ExDDLValidator * ddlValidator);
  HBC_RetCode insertRows(NAHeap *heap, const char *tableName,
                         ExHbaseAccessStats *hbs,
                         Int64 transID, Int64 savepointID,
                         Int64 pSavepointId,
                         short rowIDLen, HbaseStr rowIDs,
                         HbaseStr rows, Int64 timestamp,
                         Int32 flags,
                         const char * encryptionInfo,
			 const char * triggers,
			 const char * curExecSql,
                         HTableClient_JNI **outHtc,
                         ExDDLValidator * ddlValidator);
  HBC_RetCode updateVisibility(NAHeap *heap, const char *tableName,
                         ExHbaseAccessStats *hbs, bool useTRex, Int64 transID, 
                         HbaseStr rowID,
                         HbaseStr row,
                         HTableClient_JNI **outHtc);
  HBC_RetCode checkAndUpdateRow(NAHeap *heap, const char *tableName,
				ExHbaseAccessStats *hbs,
                                Int64 transID, Int64 savepointID, Int64 pSavepointId,
                                HbaseStr rowID,
				HbaseStr row, 
                                HbaseStr columnToCheck, HbaseStr columnValToCheck,
				Int64 timestamp,
                                Int32 flags,
                                const char * encryptionInfo,
				const char * triggers,
				const char * curExecSql,
				HTableClient_JNI **outHtc);
  HBC_RetCode deleteRow(NAHeap *heap, const char *tableName,
			ExHbaseAccessStats *hbs,
			Int64 transID, Int64 savepointID, Int64 pSavepointId, HbaseStr rowID, 
                        const LIST(HbaseStr) *cols, 
			Int64 timestamp,
                        const char * hbaseAuths,
                        Int32 flags,
                        const char * encryptionInfo,
			const char * triggers,
			const char * curExecSql,
                        HTableClient_JNI **outHtc,
                        ExDDLValidator * ddlValidator);
  HBC_RetCode deleteRows(NAHeap *heap, const char *tableName,
			 ExHbaseAccessStats *hbs,
			 Int64 transID, Int64 savepointID, Int64 pSavepointId,
                         short rowIDLen, HbaseStr rowIDs, 
                         const LIST(HbaseStr) *cols, 
			 Int64 timestamp,
                         const char * hbaseAuths,
                         Int32 flags,
                         const char * encryptionInfo,
			 const char * triggers,
			 const char * curExecSql,
                         HTableClient_JNI **outHtc,
                         ExDDLValidator * ddlValidator);
  HBC_RetCode checkAndDeleteRow(NAHeap *heap, const char *tableName,
				ExHbaseAccessStats *hbs,
				Int64 transID, Int64 savepointID, Int64 pSavepointId, HbaseStr rowID,
                                const LIST(HbaseStr) *cols, 
				HbaseStr columnToCheck, HbaseStr columnValToCheck,
				Int64 timestamp,
                                const char * hbaseAuths,
                                Int32 flags,
                                const char * encryptionInfo,
				const char * triggers,
				const char * curExecSql,
                                HTableClient_JNI **outHtc,
                                ExDDLValidator * ddlValidator);

  HBC_RetCode  getNextValue(NAString& tabName, NAString& rowId,
                           NAString& famName, NAString& qualName,
                           Int64 incrBy, Int64 &nextValue,
                           NABoolean skipWAL);

  HBC_RetCode deleteSeqRow(NAString& tabName, NAString& rowId);

  HBC_RetCode updateTableDefForBinlog(NAString& tabName, NAString& cols, NAString &keys, long ts);
  HBC_RetCode getTableDefForBinlog(NAString& tabName, NAHeap *heap, NAArray<HbaseStr>* *retNames);

  HBC_RetCode putData(Int64 eventID, const char* query, int eventType, const char* schemaName, unsigned char* params, long len);

  NAArray<HbaseStr> *showTablesHDFSCache(NAHeap *heap, const TextVec& tables);

  HBC_RetCode addTablesToHDFSCache(const TextVec& tables, const char* poolName);
  HBC_RetCode removeTablesFromHDFSCache(const TextVec& tables, const char* poolName);
  NAArray<HbaseStr>* getStartKeys(NAHeap *heap, const char *tableName, bool useTRex);
  NAArray<HbaseStr>* getEndKeys(NAHeap *heap, const char * tableName, bool useTRex);
  HBC_RetCode    createSnapshot( const NAString&  tableName, const NAString&  snapshotName);
  HBC_RetCode    restoreSnapshot( const NAString&  snapshotName, const NAString&  tableName);
  HBC_RetCode    deleteSnapshot( const NAString&  snapshotName);
  HBC_RetCode    verifySnapshot( const NAString&  tableName, const NAString&  snapshotName, NABoolean & exist);
  HBC_RetCode    lockRequired(NAHeap *heap, const char *tableName, ExHbaseAccessStats *hbs, Int64 transactionId, Int64 savepointId, Int64 pSavepointId, Int32 lockMode, NABoolean registerRegion, HTableClient_JNI **outHtc);
  HBC_RetCode cancelOperation(Int64 transId);

  void settrigger_operation(int v) {trigger_operation_ = v;}
  int gettrigger_operation() {return trigger_operation_;}

  void setOldValueInfo(char *skvBuffer,
		       char *sbufsize,
		       char *srowIDs,
		       char *srowIDsLen);
  void cleanupOldValueInfo();
  HBC_RetCode    reconnectHBase();
  static int getReplicaId(int numReplications);
  NABoolean noConflictCheckForIndex() { return noConflictCheckForIndex_; }
  void setNoConflictCheckForIndex(NABoolean v) { noConflictCheckForIndex_ = v; }
  HBC_RetCode getLockError();
private:   
  // private default constructor
  HBaseClient_JNI(NAHeap *heap, NABoolean bigTable);
  NAArray<HbaseStr>* getKeys(Int32 funcIndex, NAHeap *heap, const char *tableName, bool useTRex);
private:
  enum JAVA_METHODS {
    JM_CTOR = 0
   ,JM_INIT
   ,JM_GET_HTC
   ,JM_REL_HTC
   ,JM_CREATE
   ,JM_CREATEK
   ,JM_TRUNCABORT
   ,JM_ALTER
   ,JM_DROP
   ,JM_DROP_ALL
   ,JM_LIST_ALL
   ,JM_GET_REGION_STATS
   ,JM_COPY
   ,JM_EXISTS
   ,JM_GRANT
   ,JM_REVOKE
   ,JM_GET_HBLC
   ,JM_GET_BRC
   ,JM_EST_RC
   ,JM_EST_RC_COPROC
   ,JM_REL_HBLC
   ,JM_REL_BRC
   ,JM_GET_CAC_FRC
   ,JM_GET_LATEST_SNP
   ,JM_CLEAN_SNP_TMP_LOC
   ,JM_SET_ARC_PERMS
   ,JM_START_GET
   ,JM_START_GETS
   ,JM_START_DIRECT_GETS
   ,JM_GET_HBTI
   ,JM_CREATE_COUNTER_TABLE  
   ,JM_INCR_COUNTER
   ,JM_GET_REGN_NODES
   ,JM_HBC_DIRECT_INSERT_ROW
   ,JM_HBC_DIRECT_INSERT_ROWS
   ,JM_HBC_DIRECT_UPDATE_TAGS
   ,JM_HBC_DIRECT_CHECKANDUPDATE_ROW
   ,JM_HBC_DELETE_ROW
   ,JM_HBC_DIRECT_DELETE_ROWS
   ,JM_HBC_CHECKANDDELETE_ROW
   ,JM_SHOW_TABLES_HDFS_CACHE
   ,JM_ADD_TABLES_TO_HDFS_CACHE
   ,JM_REMOVE_TABLES_FROM_HDFS_CACHE
   ,JM_HBC_GETSTARTKEYS
   ,JM_HBC_GETENDKEYS
   ,JM_HBC_CREATE_SNAPSHOT
   ,JM_HBC_RESTORE_SNAPSHOT
   ,JM_HBC_DELETE_SNAPSHOT
   ,JM_HBC_VERIFY_SNAPSHOT
   ,JM_SAVEPOINT_COMMIT_OR_ROLLBACK
   ,JM_NAMESPACE_OPERATION
   ,JM_TRUNCATE
   ,JM_LOCKREQUIRED
   ,JM_RECONNECT
   ,JM_GET_NEXT_VALUE
   ,JM_DELETE_SEQ_ROW
   ,JM_CANCLE_OPERATION
   ,JM_UPDATE_TBL_DEF_BINLOG
   ,JM_GET_TBL_DEF_BINLOG
   ,JM_PUT_SQL_TO_HBASE
   ,JM_LAST
  };
  static jclass          javaClass_; 
  static JavaMethodInit* JavaMethods_;
  static bool javaMethodsInitialized_;
  // this mutex protects both JaveMethods_ and javaClass_ initialization
  static pthread_mutex_t javaMethodsInitMutex_;
  bool isConnected_;

  pthread_t threadID_[NUM_HBASE_WORKER_THREADS];
  pthread_mutex_t mutex_;  
  pthread_cond_t workBell_;
  typedef list<HBaseClientRequest *> reqList_t;
  reqList_t reqQueue_; 
  NABoolean bigTable_;
  NABoolean noConflictCheckForIndex_;
  int trigger_operation_;
  char *skvBuffer_;
  char *srowIDs_;
  char *srowIDsLen_;
  char *sBufSize_;
};

// ===========================================================================
// ===== The HBulkLoadClient_JNI class implements access to the Java
// ===== HBulkLoadClient class.
// ===========================================================================

typedef enum {
  HBLC_OK     = JOI_OK
 ,HBLC_FIRST  = HBC_LAST
 ,HBLC_DONE   = HBLC_FIRST
 ,HBLC_ERROR_INIT_PARAM
 ,HBLC_ERROR_INIT_EXCEPTION
 ,HBLC_ERROR_CLEANUP_EXCEPTION
 ,HBLC_ERROR_CLOSE_EXCEPTION
 ,HBLC_ERROR_CREATE_HFILE_PARAM
 ,HBLC_ERROR_CREATE_HFILE_EXCEPTION
 ,HBLC_ERROR_ADD_TO_HFILE_PARAM
 ,HBLC_ERROR_ADD_TO_HFILE_EXCEPTION
 ,HBLC_ERROR_CLOSE_HFILE_PARAM
 ,HBLC_ERROR_CLOSE_HFILE_EXCEPTION
 ,HBLC_ERROR_DO_BULKLOAD_PARAM
 ,HBLC_ERROR_DO_BULKLOAD_EXCEPTION
 ,HBLC_ERROR_BULKLOAD_CLEANUP_PARAM
 ,HBLC_ERROR_BULKLOAD_CLEANUP_EXCEPTION
 ,HBLC_ERROR_INIT_HBLC_PARAM
 ,HBLC_ERROR_INIT_HBLC_EXCEPTION
 ,HBLC_LAST
} HBLC_RetCode;


class HBulkLoadClient_JNI : public JavaObjectInterface
{
public:

  HBulkLoadClient_JNI(NAHeap *heap,jobject jObj = NULL)
  :  JavaObjectInterface(heap, jObj)
  {
    heap_= heap;
  }
  // Destructor
  virtual ~HBulkLoadClient_JNI();

  // Initialize JVM and all the JNI configuration.
  // Must be called.
  HBLC_RetCode init();

  HBLC_RetCode initHFileParams(const HbaseStr &tblName, const Text& hFileLoc, const Text& hfileName, Int64 maxHFileSize,
                               const Text& hFileSampleLoc, const Text& hfileSampleName, float fSampleRate);

  HBLC_RetCode addToHFile( short rowIDLen, HbaseStr &rowIDs, HbaseStr &rows, ExHbaseAccessStats *hbs,
                           const char * encryptionInfo,
                           NAHeap * heap);

  HBLC_RetCode closeHFile(const HbaseStr &tblName);

  HBLC_RetCode doBulkLoad(const HbaseStr &tblName, const Text& location, const Text& tableName, NABoolean quasiSecure, NABoolean snapshot);

  
  HBLC_RetCode  bulkLoadCleanup(const HbaseStr &tblName, const Text& location);
  // Get the error description.
  static char* getErrorText(HBLC_RetCode errEnum);


private:
  enum JAVA_METHODS {
    JM_CTOR = 0
   ,JM_INIT_HFILE_PARAMS
   ,JM_CLOSE_HFILE
   ,JM_DO_BULK_LOAD
   ,JM_BULK_LOAD_CLEANUP
   ,JM_ADD_TO_HFILE_DB
   ,JM_LAST
  };
  static jclass          javaClass_;
  static JavaMethodInit* JavaMethods_;
  static bool javaMethodsInitialized_;
  // this mutex protects both JaveMethods_ and javaClass_ initialization
  static pthread_mutex_t javaMethodsInitMutex_;

};

//===========================================================================
//===== The HBulkLoadClient_JNI class implements access to the Java
//===== HBulkLoadClient class.
//===========================================================================

typedef enum {
  BRC_OK     = JOI_OK
  ,BRC_FIRST  = HBLC_LAST
  ,BRC_DONE   = BRC_FIRST
  ,BRC_ERROR_INIT_PARAM
  ,BRC_ERROR_CREATE_SNAPSHOT_EXCEPTION
  ,BRC_ERROR_RESTORE_SNAPSHOT_EXCEPTION
  ,BRC_ERROR_DELETE_BACKUP_EXCEPTION
  ,BRC_ERROR_GET_BACKUP_TYPE_EXCEPTION
  ,BRC_ERROR_GET_EXTENDED_ATTRIBUTES_EXCEPTION
  ,BRC_ERROR_GET_BACKUP_STATUS_EXCEPTION
  ,BRC_ERROR_LIST_ALL_BACKUPS_EXCEPTION
  ,BRC_ERROR_EXPORT_IMPORT_BACKUP_EXCEPTION
  ,BRC_ERROR_INIT_BRC_EXCEPTION
  ,BRC_ERROR_LOCK_HOLDER_EXCEPTION
  ,BRC_ERROR_OPERATION_LOCK_EXCEPTION
  ,BRC_ERROR_OPERATION_UNLOCK_EXCEPTION
  ,BRC_ERROR_BACKUP_NONFATAL
  ,BRC_ERROR_SET_HIATUS_EXCEPTION
  ,BRC_ERROR_CLEAR_HIATUS_EXCEPTION
  ,BRC_ERROR_GET_LINKED_BACKUP_EXCEPTION
  ,BRC_LAST
} BRC_RetCode;


class BackupRestoreClient_JNI : public JavaObjectInterface
{
public:

  BackupRestoreClient_JNI(NAHeap *heap,jobject jObj = NULL)
       :  JavaObjectInterface(heap, jObj)
  {
    heap_= heap;
  }
  // Destructor
  virtual ~BackupRestoreClient_JNI();

  // Initialize JVM and all the JNI configuration.
  // Must be called.
  BRC_RetCode init();
  BRC_RetCode backupObjects(const TextVec& tables, 
                            const TextVec& incrBackupEnabled,
                            const TextVec& locLocList,
                            const char* backuptag,
                            const char* extendedAttributes,
                            const char* backupType,
                            const int backupThreads,
                            const int progressUpdateDelay);
  BRC_RetCode createSnapshotForIncrBackup(const TextVec& tables);
  NAArray<HbaseStr>* restoreObjects(const char* backuptag, 
                                    const TextVec *schemas,
                                    const TextVec *tables,
                                    const char* restoreToTS,
                                    NABoolean showObjects,
                                    NABoolean saveObjects,
                                    NABoolean restoreSavedObjects,
                                    int parallelThreads,
                                    const int progressUpdateDelay,
                                    NAHeap *heap);
  BRC_RetCode finalizeBackup(
       const char* backuptag,
       const char* backupType);
  
  BRC_RetCode finalizeRestore(
       const char* backuptag,
       const char* backupType);

  BRC_RetCode deleteBackup(const char* backuptag, 
                           NABoolean timestamp = FALSE,
                           NABoolean cascade = FALSE,
                           NABoolean force = FALSE,
                           NABoolean skipLock = FALSE);
  static char* getErrorText(BRC_RetCode errEnum);
  BRC_RetCode exportOrImportBackup(const char* backuptag, 
                                   NABoolean isExport,
                                   NABoolean override,
                                   const char* location,
                                   int parallelThreads,
                                   const int progressUpdateDelay);
  BRC_RetCode listAllBackups(NAArray<HbaseStr>* *backupList,
            NABoolean shortFormat, NABoolean reverseOrder, NAHeap *heap);
  
  NAString getBackupType(const char* backuptag);
  NAString getExtendedAttributes(const char* backuptag);
  NAString getBackupStatus(const char* backuptag);
  NAString getPriorBackupTag(const char* restoreToTimestamp);
  NAString getRestoreToTsBackupTag(const char* restoreToTimestamp);
  NAString lockHolder();
  BRC_RetCode operationLock(const char* backuptag);
  BRC_RetCode operationUnlock(const char* backuptag, NABoolean recoverMeta,
                              NABoolean cleanupLock);

  BRC_RetCode setHiatus(const char* tableName,
                        NABoolean lockOperation,
                        NABoolean createSnapIfNotExist,
                        NABoolean ignoreSnapIfNotExist,
                        const int parallelThreads);
  BRC_RetCode clearHiatus(const char* tableName);
  
  NAArray<HbaseStr>* getLinkedBackupTags(const char* backuptag, NAHeap *heap);

private:
  NAString getLastJavaError();
  
  
  enum JAVA_METHODS {
    JM_CTOR = 0
    ,JM_VALIDATE_BACKUP_OBJECTS
    ,JM_BACKUP_OBJECTS
    ,JM_SNAPSHOT_INCR_BACKUP
    ,JM_RESTORE_OBJECTS
    ,JM_FINALIZE_BACKUP
    ,JM_FINALIZE_RESTORE
    ,JM_LIST_ALL_BACKUPS
    ,JM_DELETE_BACKUP
    ,JM_EXPORT_IMPORT_BACKUP
    ,JM_GET_BACKUP_TYPE
    ,JM_GET_EXTENDED_ATTRIBUTES
    ,JM_GET_BACKUP_STATUS
    ,JM_GET_PRIOR_BACKUP_TAG
    ,JM_GET_RESTORE_TO_TS_BACKUP_TAG
    ,JM_LOCK_HOLDER
    ,JM_OPERATION_LOCK
    ,JM_OPERATION_UNLOCK
    ,JM_SET_HIATUS
    ,JM_CLEAR_HIATUS
    ,JM_GET_LINKED_BACKUPS
    ,JM_LAST
  };
  static jclass          javaClass_;
  static JavaMethodInit* JavaMethods_;
  static bool javaMethodsInitialized_;
  // 	this mutex protects both JaveMethods_ and javaClass_ initialization
  static pthread_mutex_t javaMethodsInitMutex_;
};

jobjectArray convertToByteArrayObjectArray(const LIST(NAString) &vec);
jobjectArray convertToByteArrayObjectArray(const LIST(HbaseStr) &vec);
jobjectArray convertToByteArrayObjectArray(const char **array,
                   int numElements, int elementLen);
jobjectArray convertToByteArrayObjectArray(const TextVec &vec);
jobjectArray convertToStringObjectArray(const TextVec &vec);
jobjectArray convertToStringObjectArray(const HBASE_NAMELIST& nameList);
jobjectArray convertToStringObjectArray(const NAText *text, int arrayLen);
jobjectArray convertToStringObjectArray(const set<string> &setOfNames);
jobjectArray convertMapToStringObjectArray(const map<Lng32,char *> & columnNumberToNameMap,
                                           vector<Lng32> &positionToColumnNumberVector /* out */);

int convertStringObjectArrayToList(NAHeap *heap, jarray j_objArray, 
                                         LIST(Text *)&list);
int convertLongObjectArrayToList(NAHeap *heap, jlongArray j_longArray, LIST(Int64)&list);
int convertIntObjectArrayToVector(jintArray j_intArray, vector<Lng32> &vec);
int convertByteArrayObjectArrayToNAArray(NAHeap *heap, jarray j_objArray, 
             NAArray<HbaseStr> **retArray);
int convertStringObjectArrayToNAArray(NAHeap *heap, jarray j_objArray, 
             NAArray<HbaseStr> **retArray);
void deleteNAArray(CollHeap *heap, NAArray<HbaseStr> *array);
char* getClientInfoFromContext();
#endif


