/**********************************************************************
/ @@@ START COPYRIGHT @@@
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
**********************************************************************/
#ifndef SQLSTATS_H
#define SQLSTATS_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         SqlStats.h
 * Description:  
 * Created:      2/28/06
 * Language:     C++
 *
 *****************************************************************************
 */
#include "seabed/fs.h"

#define STATS_INITAL_SEG_SIZE     4*1024*1024
#define STATS_MAX_SEG_SIZE        64*1024*1024

#define SHARED_CACHE_SEG_SIZE    256*1024*1024

#define MIN_EPOCH_CACHE_SEG_SIZE     256*1024 // 256K
#define EPOCH_CACHE_SEG_SIZE      2*1024*1024 // 2M
#define MAX_EPOCH_CACHE_SEG_SIZE 64*1024*1024 // 64M

// Object lock array size
#define MIN_LOCK_ARRAY_SIZE 10
#define MAX_LOCK_ARRAY_SIZE 10000
#define DML_LOCK_ARRAY_SIZE 256
#define DDL_LOCK_ARRAY_SIZE 128

// Maximum object lock key name length
#define MAX_LOCK_KEY_NAME_LEN  512

#define RMS_STORE_SQL_SOURCE_LEN    254

#if defined(__powerpc) || defined(__aarch64__)
#define RMS_SHARED_MEMORY_ADDR    0x135000000
#else
#define RMS_SHARED_MEMORY_ADDR    0x10000000 
#endif

// make the permission rw by owner only so we cannot attach to other 
// instances' segments
#define RMS_SHMFLAGS	          0600
#define RMS_SEM_NAME		  "/rms."
// outputs and returns semaphore name
extern char *gRmsSemName_;
extern void *gRmsSharedMemoryAddr_;
char *getRmsSemName(); 
void *getRmsSharedMemoryAddr();
long computeSegmentSizeForRMS();

// make the permission rw by owner only so we cannot use other 
// instances' seamphore
#define RMS_SEMFLAGS	          0600
class ExStatisticsArea;
class ProcessStats;
class HashQueue;
class SyncHashQueue;
class ExMasterStats;
class ExRMSStats;
class RecentSikey;
class StatsGlobals;
class ExOperStats;
class ExProcessStats;
class MemoryMonitor;

#include "rts_msg.h"
#include "ComTdb.h"
#include "SQLCLIdev.h"
#include "memorymonitor.h"

#define PID_MAX_DEFAULT     65536
#define PID_MAX_DEFAULT_MAX 131072
#define PID_MAX_DEFAULT_MIN 32768
#define PID_VIOLATION_MAX_COUNT 100

typedef struct GlobalStatsArray
{
  pid_t  processId_;
  SB_Verif_Type  phandleSeqNum_;
  ProcessStats  *processStats_;
} GlobalStatsArray;

class ObjectEpochCache;

// Test method invoked from tdm_arkcmp (as a test program) when 
// proper test options are suppiled to tdm_arkcmp.
void testObjectEpochCache(Int32 argc, char **argv);

class StmtStats
{
public:
  StmtStats(NAHeap *heap, pid_t pid, char *queryId, Lng32 queryIdLen, 
          void *backRef, Lng32 fragId,
          char *sourceStr = NULL, Lng32 sourceStrLen = 0, Lng32 sqlSrcStrLen = 0, 
          NABoolean isMaster = FALSE);
  ~StmtStats();
  void deleteMe();
  void setStatsArea(ExStatisticsArea *stats);
  NAHeap *getHeap() 
    { return heap_; }
  pid_t getPid() 
    { return pid_; }
  Lng32 getFragId()
    { return fragId_; }
  ExStatisticsArea *getStatsArea() 
    { return stats_; }
  Int64 getLastMergedTime() { return lastMergedTime_; }
  //unsigned short getLastMergedStatsType() { return lastMergedStatsType_; }
  ExStatisticsArea *getMergedStats() { return mergedStats_; }
  void setMergedStats(ExStatisticsArea *stats);
//  void setLastMergedStatsType(unsigned short type) { lastMergedStatsType_ = type; }
  NABoolean isMaster() 
  { 
    return (flags_ & IS_MASTER_) != 0; 
  }
  
  void setMaster(NABoolean v)      
  {
    (v ? flags_ |= IS_MASTER_ : flags_ &= ~IS_MASTER_); 
  }

  NABoolean canbeGCed() 
  {
    return (flags_ & CAN_BE_GCED_) != 0; 
  }
  
  void setTobeGCed(NABoolean v = TRUE)
  { 
     (v ? flags_ |= CAN_BE_GCED_ : flags_ &= ~CAN_BE_GCED_); 
  }

  void setMergeReqd(NABoolean v) 
  { 
    (v ? flags_ |= FORCE_MERGE_ : flags_ &= ~FORCE_MERGE_); 
  }

  NABoolean mergeReqd() 
  { 
    return (flags_ & FORCE_MERGE_) != 0; 
  }

  NABoolean deleteStats()
  {
    if (isMaster())
       return TRUE;
    else
       return FALSE;
  }
 
  NABoolean isStmtStatsUsed()
  {
    return (refCount_ > 0); 
  }

  void setStmtStatsUsed(NABoolean v)      
  {
    if (v)
      refCount_++;
    else
    {
      if (refCount_ > 0)
        refCount_--;
    }
  } 

  NABoolean isDeleteError()
  {
    return (flags_ & IS_DELETE_ERROR_) != 0; 
  }

  void setDeleteError(NABoolean v)      
  {
    (v ? flags_ |= IS_DELETE_ERROR_ : flags_ &= ~IS_DELETE_ERROR_); 
  } 
  NABoolean aqrInProgress() { return (flags_ & AQR_IN_PROGRESS_) != 0; }

  void setAqrInProgress(NABoolean v)
  { (v ? flags_ |= AQR_IN_PROGRESS_: flags_ &= ~AQR_IN_PROGRESS_); }
  NABoolean isWMSMonitoredCliQuery()
  {
    return (flags_ & WMS_MONITORED_CLI_QUERY_) != 0; 
  }

  void setWMSMonitoredCliQuery(NABoolean v)      
  {
    (v ? flags_ |= WMS_MONITORED_CLI_QUERY_ : flags_ &= ~WMS_MONITORED_CLI_QUERY_);
  }
  
  NABoolean calledFromRemoveQuery()
  {
    return (flags_ & CALLED_FROM_REMOVE_QUERY) != 0; 
  }

  void setCalledFromRemoveQuery(NABoolean v)      
  {
    (v ? flags_ |= CALLED_FROM_REMOVE_QUERY : flags_ &= ~CALLED_FROM_REMOVE_QUERY);
  }

  char *getQueryId() { return queryId_; }
  Lng32 getQueryIdLen() { return queryIdLen_; }

  ExMasterStats *getMasterStats();
  void reuse(void *backRef);
  void setParentQid(char *parentQid, Lng32 parentQidLen, char *parentQidSystem,
                    Lng32 parentQidSystemLen, short myCpu, short myNodeId);
  inline NABoolean updateChildQid() { return updateChildQid_; }
  void setExplainFrag(void *explainFrag, Lng32 len, Lng32 topNodeOffset);
  RtsExplainFrag *getExplainInfo() { return explainInfo_; }
  void deleteExplainFrag();
  ULng32 getFlags() const { return flags_; }
  NABoolean reportError(pid_t pid);
private:
  enum Flags
  {
    IS_MASTER_     = 0x0001,
    CAN_BE_GCED_   = 0x0002,
    NOT_USED_1     = 0x0004,
    FORCE_MERGE_   = 0x0008,
    STMT_STATS_USED_ = 0x0010, // Unused Flag
    IS_DELETE_ERROR_ = 0x0020,
    AQR_IN_PROGRESS_ = 0x0040,
    WMS_MONITORED_CLI_QUERY_=0x0080, // Is this a CLI query monitored by WMS
    CALLED_FROM_REMOVE_QUERY = 0x0100
  };

  NAHeap *heap_;
  pid_t pid_;
  char *queryId_;
  Lng32 queryIdLen_;
  ExMasterStats *masterStats_;
  ExStatisticsArea *stats_;
  Int64 lastMergedTime_;
  ExStatisticsArea *mergedStats_;
  ULng32 flags_;
  void *backRef_;    // This member is for the debug purposes to analyze halts
                     // It should never be used 
                     // It points to Statement object for master and ExEspStmtGlobals
                     // for Esps
  short refCount_;
  Lng32 fragId_;
  RtsExplainFrag *explainInfo_;
  NABoolean updateChildQid_;
};

#define QUERYID_INFO_EYE_CATCHER "QIDI"

class QueryIdInfo
{
friend class StatsGlobals;
public:
   QueryIdInfo()
   {
     str_cpy_all(eye_catcher_, QUERYID_INFO_EYE_CATCHER, 4);
     ss_ = NULL;
     operStats_ = NULL;
   }
   
   QueryIdInfo(StmtStats *ss, ExOperStats *operStats)
   {
     str_cpy_all(eye_catcher_, QUERYID_INFO_EYE_CATCHER, 4);
     ss_ = ss;
     operStats_ = operStats;
   }
protected:
   char eye_catcher_[4];
   StmtStats *ss_;
   ExOperStats *operStats_;
};

class LockHolder
{
public:
  LockHolder(Int32 nid=-1, Int32 pid=-1)
    :nid_(nid)
    ,pid_(pid)
  {}

  LockHolder(CliGlobals *cliGlobals);

  void reset()
  {
    nid_ = -1;
    pid_ = -1;
  }
  Int32 getNid() const { return nid_; }
  Int32 getPid() const { return pid_; }
  void setNid(Int32 nid) { nid_ = nid; }
  void setPid(Int32 pid) { pid_ = pid; }

  bool valid() const { return (nid_ >= 0 && pid_ > 0); }

  bool operator ==(const LockHolder &other) const {
    return (other.getNid() == nid_ && other.getPid() == pid_);
  }

  bool operator !=(const LockHolder &other) const {
    return (other.getNid() != nid_ || other.getPid() != pid_);
  }

  LockHolder& operator =(const LockHolder &other) {
    nid_ = other.getNid();
    pid_ = other.getPid();
    return *this;
  }

private:
  Int32 nid_;
  Int32 pid_;
};

#define OBJECT_LOCK_NAME_LEN 255
class ObjectLockEntry
{
public:
  ObjectLockEntry(const char *objectName=NULL,
                  ComObjectType objectType=COM_UNKNOWN_OBJECT)
    :objectType_(objectType)
    ,holder_()
    ,inUse_(false)
  {
    setObjectName(objectName);
  }

  ObjectLockEntry& operator =(const ObjectLockEntry &other)
  {
    setObjectName(other.objectName());
    objectType_ = other.objectType();
    holder_ = other.holder();
    inUse_ = other.inUse();
    return *this;
  }

  void reset()
  {
    inUse_ = false;
    objectName_[0] = 0;
    objectType_ = COM_UNKNOWN_OBJECT;
    holder_.reset();
  }

  bool inUse() const { return inUse_; }

  bool match(const char* objectName, ComObjectType objectType) const;
  const char *objectName() const { return objectName_; }
  void setObjectName(const char* objectName)
  {
    if (objectName != NULL) {
      strncpy(objectName_, objectName, sizeof(objectName_));
      // Make sure we do not overrun
      objectName_[sizeof(objectName_)-1] = 0;
    } else {
      objectName_[0] = 0;
    }
  }
  ComObjectType objectType() const { return objectType_; }
  void setObjectType(ComObjectType objectType)
  {
    objectType_ = objectType;
  }
  const LockHolder& holder() const { return holder_; }

  void acquire(const LockHolder& holder)
  {
    holder_ = holder;
    inUse_ = true;
  }
  void release(const LockHolder& holder)
  {
    if (holder_ == holder) {
      reset();
    }
  }
  bool locked() const
  {
    return (inUse_ && objectName_[0] != 0 && holder_.valid());
  }
  bool lockedBy(const LockHolder &holder)
  {
    return locked() && holder == holder_;
  }

private:
  char objectName_[OBJECT_LOCK_NAME_LEN+1];
  ComObjectType objectType_;
  LockHolder holder_;
  bool inUse_;
};

class ObjectLockArray
{
public:
  ObjectLockArray(unsigned short maxEntries, NAHeap *heap)
    :entries_(0)
    ,maxEntries_(maxEntries)
    ,heap_(heap)
    ,mutex_(true, true, true)
    ,locks_(NULL)
  {
    if (maxEntries > 0) {
      locks_ = new (heap_) ObjectLockEntry[maxEntries_];
    }
  }
  ~ObjectLockArray()
  {
    if (locks_ != NULL) {
      NADELETEARRAY(locks_, maxEntries_, ObjectLockEntry, heap_);
      locks_ = NULL;
    }
  }

  NAMutex& mutex() { return mutex_; }
  unsigned short maxEntries() const { return maxEntries_; }
  unsigned short entries() const { return entries_; }
  const ObjectLockEntry *locks() const { return locks_; }
  void copyLocks(const ObjectLockArray &from);
  ObjectLockEntry *findLock(const char *objectName,
                            ComObjectType objectType);
  ObjectLockEntry *addLock(const char *objectName,
                           ComObjectType objectType,
                           const LockHolder &holder);
  bool lockedBy(const char *objectName,
                ComObjectType objectType,
                const LockHolder &holder);
  bool hasConflictLock(const char *objectName,
                       ComObjectType objectType,
                       const LockHolder& holder)
  {
    LockHolder tmpHolder;
    return hasConflictLock(objectName, objectType, holder, tmpHolder);
  }
  bool hasConflictLock(const char *objectName,
                       ComObjectType objectType,
                       const LockHolder& holder,
                       LockHolder &conflictHolder);
  bool acquireLock(const char *objectName,
                   ComObjectType objectType,
                   const LockHolder &holder,
                   LockHolder &conflictHolder,
                   bool &outOfEntry);
  void releaseLock(const char *objectName,
                   ComObjectType objectType,
                   const LockHolder &holder);
  void removeLocksHeldBy(const LockHolder &holder);
  void removeAll()
  {
    NAMutexScope mutex(mutex_);
    entries_ = 0;
  }
  void deepRecycle();
  void appendToStats(ExStatisticsArea *statsArea,
		     short cpu, pid_t pid,
		     const char *objectName,
		     ComObjectType objectType,
                     bool ddlLock);


private:
  unsigned short entries_;
  unsigned short maxEntries_;
  NAHeap *heap_;
  NAMutex mutex_;
  ObjectLockEntry *locks_;
};

class ObjectLockController
{
public:
  typedef ObjectLockReply::LockState Result;

  static const char* resultString(Result result)
  {
    return ObjectLockReply::lockStateLit(result);
  }

  ObjectLockController(const char *objectName,
                       ComObjectType objectType)
    :objectName_(objectName)
    ,objectType_(objectType)
  {}

  Result lockDML(LockHolder &conflictHolder /* OUT */);
  Result lockDDL(const LockHolder &holder,
                 LockHolder &conflictHolder /* OUT */);
  void unlockDDL(const LockHolder &holder);
  bool ddlLockedByMe();

  ObjectLockArray *dmlLockArray(pid_t pid);
  ObjectLockArray *ddlLockArray(pid_t pid);

  ObjectLockArray *dmlLockArray();
  ObjectLockArray *ddlLockArray();

  bool hasConflictDML(const LockHolder &holder);
  bool hasConflictDDL(const LockHolder &holder,
                      LockHolder &conflictHolder /* OUT */);

  static void removeLocksHeldByProcess(pid_t pid);
  static void releaseLocksHeldBy(const LockHolder &holder);
  static void releaseDMLLocksHeldBy(const LockHolder &holder);
  static void releaseDDLLocksHeldBy(const LockHolder &holder);
  static void appendToStats(ExStatisticsArea *statsArea,
                            const char* objectName,
                            ComObjectType objectType);

private:
  const char *objectName_;
  ComObjectType objectType_;
};

class CmpContext;
class DDLScope
{
public:
  DDLScope();
  ~DDLScope();

  void startDDLStmt();
  void endDDLStmt();

private:
  CmpContext *cmpContext_;
  ProcessStats *myProcessStats_;
};

class ProcessStats
{
public:
  ProcessStats(NAHeap *heap, short nid, pid_t pid,
               unsigned short dmlLockArraySize,
               unsigned short ddlLockArraySize);
  ~ProcessStats();
  NAHeap *getHeap() 
  {
     return heap_;
  }

  void setStatsArea(ExStatisticsArea *stats);
  ExStatisticsArea *getStatsArea()
  {
    return stats_;
  }

  void setQueryIdInfo(QueryIdInfo *queryIdInfo)
  { 
    if (queryIdInfo_ != NULL)
       NADELETE(queryIdInfo_, QueryIdInfo, heap_);
    queryIdInfo_ = queryIdInfo; 
  }

  QueryIdInfo *getQueryIdInfo() 
  { return queryIdInfo_; }

  void updateMemStats(NAHeap *exeHeap, NAHeap *ipcHeap);
  inline size_t getExeMemHighWM();
  inline size_t getExeMemAlloc();
  inline size_t getExeMemUsed();
  inline size_t getIpcMemHighWM();
  inline size_t getIpcMemAlloc();
  inline size_t getIpcMemUsed();
  inline ExProcessStats *getExProcessStats() { return exProcessStats_; }
  StmtStats *addQuery(pid_t pid, char *queryId, Lng32 queryIdLen, void *backRef,
                      Lng32 fragId, char *sourceStr, Lng32 sourceStrLen, 
                      Lng32 sqlSourceLen, NABoolean isMaster);
  ObjectLockArray* ddlLockArray() { return &ddlLockArray_; }
  ObjectLockArray* dmlLockArray() { return &dmlLockArray_; }
  bool holdingDDLLocks() const { return holdingDDLLocks_; }
  void setHoldingDDLLocks(bool val) { holdingDDLLocks_ = val; }
  UInt32 ddlStmtLevel() const { return ddlStmtLevel_; }
  void setDDLStmtLevel(UInt32 val) { ddlStmtLevel_ = val; }
private:
  NAHeap *heap_; 
  ExStatisticsArea *stats_ ; 
  QueryIdInfo *queryIdInfo_;
  ExProcessStats *exProcessStats_;
  ObjectLockArray ddlLockArray_;
  ObjectLockArray dmlLockArray_;
  bool holdingDDLLocks_;
  UInt32 ddlStmtLevel_;
};

// The RecentSikey class is to support the CLI call which a compiler will
// use to see if its cache of queries and metadata needs to be updated
// because of a REVOKE.  These RecentSikey objects are organized in a 
// HashQueue in the StatsGlobals. When MXSSCP processes a 
// SecInvalidKeyRequest, it updates the structure, inserting new 
// RecentSikey values if needed, and setting the RecentSikey's 
// revokeTimestamp_.  The StatsGlobals semaphore protects the reading of 
// the table by the CLI as well as the updating by the MXSSCP.
// The MXSSCP performs maintenance on the RecentSikey HashQueue
// periodically, by removing entries that have have a revokeTimestamp_ 
// older than 24 hours.
class RecentSikey
{
public:
  SQL_QIKEY s_;
  Int64     revokeTimestamp_;
};

class StatsGlobals
{
public:
  StatsGlobals(void *baseAddr, short envType, Int64 maxSegSize);
  static void* operator new (size_t size, void* loc = 0);
  bool addProcess(pid_t pid, NAHeap *heap);
  void removeProcess(pid_t pid, NABoolean calledDuringAdd = FALSE);
  ProcessStats *checkProcess(pid_t pid);
  void setStatsArea(pid_t pid, ExStatisticsArea *stats)
  {
    if (statsArray_[pid].processStats_ != NULL)
      statsArray_[pid].processStats_->setStatsArea(stats);
  }
  
  ExStatisticsArea *getStatsArea(pid_t pid)
  {
    if (statsArray_[pid].processStats_ != NULL)
      return statsArray_[pid].processStats_->getStatsArea();
    else
      return NULL;
  }

  void logProcessDeath(short cpu, pid_t pid, const char *reason);

  StmtStats *addQuery(pid_t pid, char *queryId, Lng32 queryIdLen, void *backRef,
                     Lng32 fragId, char *sourceStr = NULL, Lng32 sourceStrLen = 0,
                     NABoolean isMaster = FALSE);

  static StmtStats *addStmtStats(NAHeap * heap,
				 pid_t pid, char *queryId, Lng32 queryIdLen,
                                 char *sourceStr, Lng32 sourceStrLen);

  // returns 0 if query is deleted
  // returns 1 if query is not deleted because MergedStats is present

  short removeQuery(pid_t pid, StmtStats *stmtStats, 
                  NABoolean removeAlways = FALSE, 
                  NABoolean globalScan = FALSE,
                  NABoolean calledFromRemoveProcess = FALSE); 

  // global scan when the stmtList is positioned from begining and searched for pid
  int openStatsSemaphore(Long &semId);
  int openStatsSemaphoreWithRetry(Long &semId);
  int getStatsSemaphore(Long &semId, pid_t pid,
                        NABoolean calledByVerifyAndCleanup = FALSE);
  void releaseStatsSemaphore(Long &semId, pid_t pid);
  int forceReleaseAndGetStatsSemaphore(Long &semId, pid_t pid);

  int releaseAndGetStatsSemaphore(Long &semId, 
       pid_t pid, pid_t releasePid);
  void cleanupDanglingSemaphore(NABoolean checkForSemaphoreHolder);
  void checkForDeadProcesses(pid_t myPid);
  SyncHashQueue *getStmtStatsList() { return stmtStatsList_; }
  ExStatisticsArea *getStatsArea(char *queryId, Lng32 queryIdLen);
  StmtStats *getMasterStmtStats(const char *queryId, Lng32 queryIdLen, short activeQueryNum);
  StmtStats *getStmtStats(char *queryId, Lng32 queryIdLen);
  StmtStats *getStmtStats(pid_t pid, short activeQueryNum);
  StmtStats *getStmtStats(short activeQueryNum);
  StmtStats *getStmtStats(pid_t pid, char *queryId, Lng32 queryIdLen, 
           Lng32 fragId);
  StmtStats *getStmtStats(short cpu, pid_t pid, Int64 timeStamp, Lng32 queryNumber);
  StmtStats *getStmtStats(pid_t pid, char *queryId, Lng32 queryIdLen); 
  ExRMSStats *getRMSStats() { return rmsStats_; }
  ExRMSStats *getRMSStats(NAHeap *heap);
  void doFullGC();
  Lng32 registerQuery(ComDiagsArea &diags, pid_t pid, SQLQUERY_ID *queryId, 
                 Lng32 fragId, Lng32 tdbId, Lng32 explainTdbId, 
                 short statsCollectionType,
                 Lng32 instNum, ComTdb::ex_node_type tdbType,
                 char *tdbName, Lng32 tdbNameLen);
  Lng32 deregisterQuery(ComDiagsArea &diags, pid_t pid, SQLQUERY_ID *queryId, 
                  Lng32 fragId);
  Lng32 updateStats(ComDiagsArea &diags, SQLQUERY_ID *query_id, 
             void *operatorStats,
             Lng32 operatorstatsLen);

  void *getStatsSharedSegAddr() 
  {
    return statsSharedSegAddr_;
  }

  Lng32 getVersion()
  { 
    return version_;
  } 
  void getMemOffender(ExStatisticsArea *statsArea,
                           size_t  memThreshold);
  ExProcessStats *getExProcessStats(pid_t pid);
  enum 
  {
    // CURRENT_SHARED_OBJECTS_VERSION_ is used to ensure that all of the
    // RTS executables use structures in shared memory in the same way.
    // This value should be incremented whenever classes within this file
    // or classes in NAMemory.h are modified. If a merge from another stream
    // occurs and the following value is marked as a conflict, then this
    // value should be incremented. For release 2.4 development, this value
    // starts at 2400. For R2.93, the value starts are 2930 and for R2.5, 
    // the value starts at 2500.
    CURRENT_SHARED_OBJECTS_VERSION_ = 2511
  };
  void setSscpInitialized(NABoolean toggle)
  {
    isSscpInitialized_ = toggle;
  }
  NABoolean IsSscpInitialized()
  {
    return (isSscpInitialized_ == TRUE);
  }
  enum RTSEnvType
  {
    RTS_GLOBAL_ENV = 1,
    RTS_PRIVATE_ENV = 0
  };
  NABoolean globalRtsEnv() { return (NABoolean) rtsEnvType_; };
  inline short getStoreSqlSrcLen() { return storeSqlSrcLen_; }
  inline void setStoreSqlSrcLen(short len) { storeSqlSrcLen_ = len; }
  inline NAHeap *getStatsHeap() { return &statsHeap_; }
  inline pid_t getSemPid() { return semPid_; }
  inline pid_t getSsmpPid();
  inline Int64 getSsmpTimestamp();
  inline pid_t getConfiguredPidMax() { return configuredPidMax_; }
  inline void setSsmpDumpTimestamp(Int64 dumpTime) 
          { ssmpDumpedTimestamp_ = dumpTime; }
  inline Int64 getSsmpDumpTimestamp() 
          { return ssmpDumpedTimestamp_; }
  void setSnapshotInProgress() { snapshotInProgress_ = TRUE;}
  void resetSnapshotInProgress() { snapshotInProgress_ = FALSE;}
  NABoolean isSnapshotInProgress(){ return snapshotInProgress_;}
  
  Int64 getLastGCTime();
  void setLastGCTime(Int64 gcTime) ;
  void incStmtStatsGCed(short inc) ;
  void incSsmpReqMsg(Int64 msgBytes);
  void incSsmpReplyMsg(Int64 msgBytes);
  void incSscpReqMsg(Int64 msgBytes);
  void incSscpReplyMsg(Int64 msgBytes);
  void setSscpOpens(short numSscps);
  void setSscpDeletedOpens(short numSscps);
  static const char *rmsEnvType(RTSEnvType envType);
  void setSscpTimestamp(Int64 timestamp);
  void setSsmpTimestamp(Int64 timestamp);
  void setSscpPid(pid_t pid);
  void setSsmpPid(pid_t pid);
  void setSscpPriority(short pri);
  void setSsmpPriority(short pri);
  void setRMSStatsResetTimestamp(Int64 timestamp); 
  void setNodesInCluster(short numNodes);
  void incProcessRegd();
  void decProcessRegd();
  void incProcessStatsHeaps();
  void decProcessStatsHeaps();
  inline short getCpu() { return cpu_; }
  inline short getNodeId() { return nodeId_; }
  inline void setAbortedSemPid()
      { abortedSemPid_ = semPid_; }
  Int64 getNewestRevokeTimestamp() const { return newestRevokeTimestamp_; }
  void cleanupOldSikeys(Int64 gcInterval);
  Lng32 getSecInvalidKeys(
                          CliGlobals * cliGlobals,
                          Int64 lastCallTimestamp,
                          SQL_QIKEY [],
                          Int32 maxNumSiKeys,
                          Int32 *returnedNumSiKeys);
  Int32 checkLobLock(CliGlobals *cliGlobals,char *&lobLockId);

  void mergeNewSikeys(Int32 numSikeys, 
                    SQL_QIKEY sikeys[]);
  MemoryMonitor *getMemoryMonitor() { return memMonitor_; }
  void createMemoryMonitor();

  void init();
  NABoolean isShmDirty() { return isBeingUpdated_; }
  void setShmDirty() { isBeingUpdated_ = TRUE; }
  void cleanup_SQL(pid_t pidToCleanup, pid_t myPid);
  void verifyAndCleanup(pid_t pidThatDied, SB_Int64_Type seqNum);

  void updateMemStats(pid_t pid, NAHeap *exeMem, NAHeap *ipcHeap);
  SB_Phandle_Type *getSsmpProcHandle() { return &ssmpProcHandle_; }
  SB_Phandle_Type *getSscpProcHandle() { return &sscpProcHandle_; }
  SyncHashQueue *getRecentSikeys() { return recentSikeys_; }
  SyncHashQueue *getLobLocks() { return lobLocks_;}
  void setSsmpProcSemId(Long semId) { ssmpProcSemId_ = semId; } 
  Long &getSsmpProcSemId() { return ssmpProcSemId_; } 
  void setSscpProcSemId(Long semId) { sscpProcSemId_ = semId; } 
  void setSeabedError(Int32 error) { seabedError_ = error; }
  void setRmsDeadLockSecs(Int32 secs) { rmsDeadLockSecs_ = secs; }
  void setDumpRmsDeadLockProcess(NABoolean dump) { dumpRmsDeadLockProcess_ = dump; }
  NABoolean getInitError(pid_t pid, NABoolean &reportError );
  void setObjectEpochCache(ObjectEpochCache *epochCache) { epochCache_ = epochCache; }
  ObjectEpochCache *getObjectEpochCache() ;
  NAHeap *getEpochHeap() ;
  Int32 calLimitLevel();
  void setDMLLockArraySize(unsigned short size) { dmlLockArraySize_ = size; }
  unsigned short dmlLockArraySize() const { return dmlLockArraySize_; }
  void setDDLLockArraySize(unsigned short size) { ddlLockArraySize_ = size; }
  unsigned short ddlLockArraySize() const { return ddlLockArraySize_; }
  ObjectLockArray* dmlLockArray(pid_t pid);
  ObjectLockArray* ddlLockArray(pid_t pid);
  void copyDDLLocks(pid_t pid);
  pid_t maxPid() const { return maxPid_; }
  void populateQueryInvalidateStats(ExStatisticsArea *statsArea);
  void unlockIfHeapMutexLocked();
private:
  void *statsSharedSegAddr_;
  Lng32 version_;             // A field used to prevent downrev compiler or other 
                             // incompatible programs to store objects in the
                             // shared memory
  Long sscpProcSemId_;
  SB_Phandle_Type  sscpProcHandle_;
  Long ssmpProcSemId_;
  SB_Phandle_Type ssmpProcHandle_;
  GlobalStatsArray *statsArray_;
  SyncHashQueue *stmtStatsList_;
  short cpu_;
  pid_t semPid_;    // Pid of the process that holds semaphore lock - This element is used for debugging purpose only
  Int64 semPidCreateTime_; // Creation timestamp - pid recycle workaround. 
  NAHeap statsHeap_;
  NABoolean isSscpInitialized_;
  short rtsEnvType_; // 1 - Global Environment
                     // 0 - Private environment
  short storeSqlSrcLen_;
  ExRMSStats *rmsStats_;
  short nodeId_;
  pid_t abortedSemPid_;
  pid_t errorSemPid_;
  pid_t releasingSemPid_;
  timespec releasingTimestamp_;
  timespec lockingTimestamp_;
  Int32 seabedError_;
  bool seabedPidRecycle_; //if true, then the most recent call to 
                          // cleanupDanglingSemaphore detected the semaphore
                          // holding process is gone, but its pid was 
                          // recycled.
  SyncHashQueue *recentSikeys_;
  Int64 newestRevokeTimestamp_;  // Allows CLI call w/o use if a semaphore.
  NABoolean isBeingUpdated_;
  pid_t pidToCheck_;
  pid_t maxPid_;
  Int64 ssmpDumpedTimestamp_;
  NABoolean snapshotInProgress_;
  MemoryMonitor *memMonitor_;
  pid_t configuredPidMax_;
  SyncHashQueue *lobLocks_;
  Int64 pidViolationCount_;
  Int32 rmsDeadLockSecs_; // tolerate RMS deadlock for these seconds
  NABoolean dumpRmsDeadLockProcess_;
  ObjectEpochCache *epochCache_;
  unsigned short dmlLockArraySize_;
  unsigned short ddlLockArraySize_;
};
StatsGlobals * shareStatsSegment(Int32 &shmid, NABoolean checkForSSMP = TRUE);
StatsGlobals * shareStatsSegmentWithRetry(Int32 &shmid, NABoolean checkForSSMP = TRUE);
short getMasterCpu(char *uniqueStmtId, Lng32 uniqueStmtIdLen, char *nodeName, short maxLen, short &cpu);
short getStmtNameInQid(char *uniqueStmtId, Lng32 uniqueStmtIdLen, char *stmtName, short maxLen);
NABoolean filterStmtStats(ExMasterStats *masterStats, short activeQueryNum, short &queryNum);
short getRTSSemaphore();
void releaseRTSSemaphore();
SB_Phandle_Type *getMySsmpPhandle();
short getDefineNumericValue(char * defineName, short *numValue);

// a helper class to convert object names to keys for use in the
// ObjectEpochCache
class ObjectEpochCacheEntryName
{
public:

  ObjectEpochCacheEntryName(NAHeap *heap,const char * objectName,
    ComObjectType objectType, Int64 redefTime);

  ~ObjectEpochCacheEntryName()
  { };

  Int64 getHash() { return hash_; }
  Int64 getRedefTime() { return redefTime_; }
  const NAString & objectName() { return objectName_; }
  const char * getDLockKey() { return dlockKey_.data(); }

private:

  Int64 hash_;
  Int64 redefTime_;  // zero if not known
  NAString objectName_;
  NAString dlockKey_;
};

class ObjectEpochCacheKey
{
public:
  ObjectEpochCacheKey(const char *objectName, ULng32 hash,
                      ComObjectType objectType=COM_UNKNOWN_OBJECT,
                      NAHeap *heap=0);
 
  ObjectEpochCacheKey(const ObjectEpochCacheKey& key, NAHeap *heap=0)
    :objectName_(key.objectName(), heap)
    ,hash_(key.hash())
    ,objectType_(key.objectType())
  {}
  ~ObjectEpochCacheKey() {}

  const NAString &objectName() const { return objectName_; }
  ComObjectType objectType() const { return objectType_; }
  ULng32 hash() const { return hash_; }

  NABoolean operator==(const ObjectEpochCacheKey& other) const
  {
    return (objectType_ == other.objectType())
        && (hash_ == other.hash())
        && (objectName_ == other.objectName());
  }

private:
  NAString objectName_;
  ULng32 hash_;
  ComObjectType objectType_;
};


// The ObjectEpochCacheEntry represents an object epoch entry stored
// in the ObjectEpochCache.
class ObjectEpochCacheEntry
{
public:

  ObjectEpochCacheEntry(UInt32 epoch, UInt32 flags)
    :epoch_(epoch)
    ,flags_(flags)
  {}

  void update(UInt32 epoch, UInt32 flags)
  {
    epoch_ = epoch;
    flags_ = flags;
  }

  ~ObjectEpochCacheEntry() {}

  UInt32 epoch() { return epoch_; }
  UInt32 flags() { return flags_; }
  bool isLocked()
  {
    return flags_ & ObjectEpochChangeRequest::DDL_IN_PROGRESS;
  }

  // NOTE2ME: Required by NAHashDictionaryIterator, more precisely
  // NAHashBucket<K,V>::getKeyValuePair method, when param `value` is
  // also used.
  NABoolean operator==(const ObjectEpochCacheEntry& other) const
  {
    return (this == &other);
  }

private:

  UInt32 epoch_;
  UInt32 flags_;
};

// The ObjectEpochCache class stores ObjectEpochCacheEntry instances
// in a hash table (NAHashDictionary).
class ObjectEpochCache
{
  static ULng32 hashFunc(const ObjectEpochCacheKey &key) { return key.hash(); }

public:
  enum { OEC_ERROR_OK,
         OEC_ERROR_NO_MEMORY,
         OEC_ERROR_ENTRY_EXISTS,
         OEC_ERROR_ENTRY_NOT_EXISTS,
         OEC_GET_SEMAPHORE_FAILURE
    };
  ObjectEpochCache(StatsGlobals *statsGlobals,
                   NAHeap *heap);
  ~ObjectEpochCache() {};

  short createEpochEntry(ObjectEpochCacheEntry* &entry,
                         const ObjectEpochCacheKey &key,
                         UInt32 epoch,
                         UInt32 flags);
  short getEpochEntry(ObjectEpochCacheEntry* &entry,
                      const ObjectEpochCacheKey &key);
  short updateEpochEntry(ObjectEpochCacheEntry* &entry,
                         const ObjectEpochCacheKey &key,
                         UInt32 newEpoch,
                         UInt32 newFlags);
  short deleteEpochEntry(ObjectEpochCacheKey &key);
  
  long entries() { return epochList_ ? epochList_->entries() : 0; }

  // Append object epoch entries of the object name or all objects (if
  // objectName is NULL) to stats area
  void appendToStats(ExStatisticsArea *statsArea,
                     const char *objectName, bool locked);

  NAHeap *epochHeap() { return epochHeap_; }

private:

  NABoolean hasEpochEntry(const ObjectEpochCacheKey &key);

  NAHeap *epochHeap_;
  NAHashDictionary<ObjectEpochCacheKey, ObjectEpochCacheEntry> *epochList_;
  StatsGlobals *statsGlobals_;
};

class ObjectKey
{
public:
  ObjectKey(const char *objectName,
            ComObjectType objectType,
            NAHeap *heap=0)
  {
    // Mostly copied from ObjectEpochCacheEntryName
    const char *objName = NULL;
    size_t length = strlen(objectName);
    char temp[length+1];
    char * next = temp;

    for (size_t i = 0; i < length; i++)
      {
        if (objectName[i] != '"')  // remove nasty double quotes
          {
            *next = objectName[i];
            next++;
          }
      }
    *next = '\0';

    // Unfortunately, our engine also has many different views of an
    // object name.  If the object name is prefixed with the namespace
    // strip it off before copying to the class.

    char * nsEnd = strchr(temp, ':');
    if (nsEnd)
      objName = nsEnd + 1;
    else
      objName = temp;

    NAString keyName;
    keyName += "_OL_";
    keyName += comObjectTypeLit(objectType);
    keyName += "_";
    keyName += objName;
    strncpy(keyName_, keyName.data(), sizeof(keyName_));
    keyName_[sizeof(keyName_)-1] = 0;
    hash_ = keyName.hash();
    const char s[] = "TRAFODION._";
    internal_ = (strncmp(objName, s, sizeof(s)-1) == 0);
  }
  ObjectKey(const ObjectKey& key, NAHeap *heap=0)
    :hash_(key.hash())
  {
    strncpy(keyName_, key.keyName(), sizeof(keyName_));
    keyName_[sizeof(keyName_)-1] = 0;
  }
  ~ObjectKey() {}

  const char *keyName() const { return keyName_; }
  ULng32 hash() const { return hash_; }
  bool isInternal() const { return internal_; }

  NABoolean operator==(const ObjectKey& other) const
  {
    return (hash_ == other.hash())
      && (!strncmp(keyName_, other.keyName(), sizeof(keyName_)));
  }

private:
  Int64 hash_;
  char keyName_[MAX_LOCK_KEY_NAME_LEN+1];
  bool internal_;
};

// Something like a smart pointer for ObjectLock
class ObjectLockPointer
{
public:
  ObjectLockPointer(const char *objectName,
                    ComObjectType objectType)
    :lock_(objectName, objectType)
  {}
  ~ObjectLockPointer() {}

  ObjectLockController &operator*() { return lock_; }
  ObjectLockController *operator->() { return &lock_; }

private:
  ObjectLockController lock_;
};

class RTSSemaphoreController
{
public:
  RTSSemaphoreController();
  ~RTSSemaphoreController();

private:
  short semRetCode_;
};

#endif
