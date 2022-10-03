/* -*-C++-*-
****************************************************************************
*
* File:             ExpHbaseInterface.h
* Description:  Interface to Hbase world
* Created:        5/26/2013
* Language:      C++
*
*
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
*
****************************************************************************
*/

#ifndef EXP_HBASE_INTERFACE_H
#define EXP_HBASE_INTERFACE_H

#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/time.h>
#include <poll.h>

#include <iostream>

// #include <protocol/TBinaryProtocol.h>
// #include <transport/TSocket.h>
#ifndef __aarch64__
// #include <transport/TTransportUtils.h>
#endif

#include "common/Platform.h"
#include "common/Collections.h"
#include "export/NABasicObject.h"

#include "exp/ExpHbaseDefs.h"

#include "HBaseClient_JNI.h"
#include "MonarchClient_JNI.h"
#include "comexe/ComTdbHbaseAccess.h"
#include "CmpSeabaseDDLincludes.h"
#include "ExDDLValidator.h"
#include "MemoryTableClient.h"

#define INLINE_COLNAME_LEN 256

class ex_globals;
class CliGlobals;

Int64 getTransactionIDFromContext();
Int64 getSavepointIDFromContext(Int64& pSvptId);
Int32 getTransactionIsolationLevelFromContext();
Int32 checkAndWaitSnapshotInProgress(NAHeap* heap);

class ExpHbaseInterface : public NABasicObject
{
 public:
  NAHeap *getHeap() { return (NAHeap*)heap_; }
  static ExpHbaseInterface* newInstance(CollHeap* heap, 
                                        const char* server, 
                                        const char *zkPort, 
                                        ComStorageType storageType,
					NABoolean replSync);

  virtual ~ExpHbaseInterface()
  {}
  
  virtual Lng32 init(ExHbaseAccessStats *hbs = NULL) = 0;
  
  virtual Lng32 cleanup() = 0;

  virtual Lng32 close() = 0;

  virtual Lng32 create(HbaseStr &tblName,
	               HBASE_NAMELIST& colFamNameList,
                       NABoolean isMVCC) = 0;
  
  virtual Lng32 create(HbaseStr &tblName,
		       NAText * hbaseCreateOptionsArray,
                       int numSplits, int keyLength,
                       const char ** splitValues,
                       NABoolean useHbaseXn,
                       NABoolean isMVCC,
                       NABoolean incrBackupEnabled) = 0;

  virtual Lng32 create(HbaseStr &tblName,
                       const NAList<HbaseStr> &cols) = 0;

  virtual Lng32 create(HbaseStr &tblName,
                       int tableType,
                       const NAList<HbaseStr> &cols,
                       NAText * monarchCreateOptionsArray,
                       int numSplits, int keyLength,
                       const char ** splitValues,
                       NABoolean useHbaseXn,
                       NABoolean isMVCC) = 0;
  
  virtual Lng32 alter(HbaseStr &tblName,
		      NAText * hbaseCreateOptionsArray,
                      NABoolean useHbaseXn) = 0;

  // During upsert using load, register truncate on abort will be used
  virtual Lng32 registerTruncateOnAbort(HbaseStr &tblName, 
                                        NABoolean useHbaseXn) = 0;

  // During a drop of seabase table or index, the object is first removed from 
  // seabase metadata. If that succeeds, the corresponding hbase object is dropped.
  // if sync is TRUE, this drop of hbase table is done in another worker thread. 
  // That speeds up the over all drop time. 
  // If a create of the same table comes in later and an error is returned
  // during create, we delay and retry for a fixed number of times since that table
  // may still be dropped by the worked thread.
  virtual Lng32 drop(HbaseStr &tblName, NABoolean async, NABoolean useHbaseXn) = 0;
  virtual Lng32 truncate(HbaseStr &tblName, NABoolean preserveSplits, NABoolean useHbaseXn) = 0;

  // drops all objects from hbase that match the pattern
  virtual Lng32 dropAll(const char * pattern, NABoolean async, NABoolean useHbaseXn) = 0;

  // retrieve all objects from hbase that match the pattern
  virtual NAArray<HbaseStr> *listAll(const char * pattern) = 0;

  // make a copy of srcTblName as tgtTblName
  // if force is true, remove target before copying.
  virtual Lng32 copy(HbaseStr &srcTblName, HbaseStr &tgtTblName,
                     NABoolean force = FALSE);

  virtual Lng32 backupObjects(const std::vector<Text>& tables,
                              const std::vector<Text>& incrBackupEnabled,
                              const std::vector<Text>& lobLocList,
                              const char* backuptag,
                              const char *extendedAttributes,
                              const char* backupType,
                              const int backupThreads,

                              // 0, update for each table.
                              // -1, dont update
                              // N, update every N secs
                              const int progressUpdateDelay);
  
  // creates snapshot fpr specified tables that will be used as the base image 
  // for further incremental backups
  virtual Lng32 createSnapshotForIncrBackup(const std::vector<Text>& tables);
  
  virtual Lng32 setHiatus(const NAString &hiatusObjectName,
                          NABoolean lockOperation,
                          NABoolean createSnapIfNotExist,
                          NABoolean ignoreSnapIfNotExist,
                          const int parallelThreads);
  virtual Lng32 clearHiatus(const NAString &hiatusObjectName);
  

  virtual NAArray<HbaseStr>* restoreObjects(
       const char* backuptag, 
       const std::vector<Text> *schemas,
       const std::vector<Text> *tables, 
       const char* restoreToTS,
       NABoolean showObjects,
       NABoolean saveObjects,
       NABoolean restoreSavedObjects,
       int parallelThreads,

       // 0, update for each table.
       // -1, dont update
       // N, update every N secs 
       const int progressUpdateDelay);

  virtual Lng32 finalizeBackup(
       const char* backuptag,
       const char* backupType);

  virtual Lng32 finalizeRestore(
       const char* backuptag,
       const char* backupType);

  virtual Lng32 deleteBackup(const char* backuptag, 
                             NABoolean timestamp = FALSE,
                             NABoolean cascade = FALSE,
                             NABoolean force = FALSE,
                             NABoolean skipLock = FALSE);

  virtual Lng32 exportOrImportBackup(const char* backuptag, 
                                     NABoolean isExport,
                                     NABoolean override,
                                     const char* location,
                                     int parallelThreads,

                                     // 0, update for each table.
                                     // -1, dont update
                                     // N, update every N secs 
                                     const int progressUpdateDelay);
  
  virtual Lng32 listAllBackups(NAArray<HbaseStr> **backupList,
                               NABoolean shortFormat, NABoolean reverseOrder);
  
  virtual NAString getBackupType(const char* backuptag) { return NAString(""); }
  virtual NAString getExtendedAttributes(const char* backuptag) { return NAString(""); }
  virtual NAString getBackupStatus(const char *backupTag) { return NAString(""); }

  virtual NAString getPriorBackupTag(const char* restoreToTimestamp)
  { return NAString(""); }

  virtual NAString getRestoreToTsBackupTag(const char* restoreToTimestamp)
  { return NAString(""); }

  virtual NAString lockHolder(){return NAString("");}
  virtual Lng32 operationLock(const char* backuptag){return HBASE_BACKUP_OPERATION_ERROR;}
  virtual Lng32 operationUnlock(const char* backuptag, NABoolean recoverMeta, NABoolean cleanupLock)
  {return HBASE_BACKUP_OPERATION_ERROR;}

  virtual NAArray<HbaseStr>* getLinkedBackupTags(const char* backuptag);

  virtual Lng32 exists(HbaseStr &tblName) = 0;

  // returns the next tablename. 100, at EOD.
  virtual Lng32 getTable(HbaseStr &tblName) = 0;

  virtual Lng32 scanOpen(
			 HbaseStr &tblName,
			 const Text& startRow, 
			 const Text& stopRow, 
			 const LIST(HbaseStr) & columns,
			 const int64_t timestamp,
			 const NABoolean useHbaseXn,
                const NABoolean useMemoryScan,
                         const Int32 lockMode,
                         Int32 isolationLevel,
                         const NABoolean skipReadConflict,
                         const NABoolean skipTransaction,
			 const NABoolean replSync,
			 const NABoolean cacheBlocks,
			 const NABoolean smallScanner,
			 const Lng32 numCacheRows,
                         const NABoolean preFetch,
			 const LIST(NAString) *inColNamesToFilter, 
			 const LIST(NAString) *inCompareOpList,
			 const LIST(NAString) *inColValuesToCompare,
                         int numReplications,
                         Float32 dopParallelScanner = 0.0f,
			 Float32 samplePercent = -1.0f,
			 NABoolean useSnapshotScan = FALSE,
			 Lng32 snapTimeout = 0,
			 char * snapName = NULL,
			 char * tmpLoc = NULL,
			 Lng32 espNum=0,
                         HbaseAccessOptions * hao = NULL,
                         const char * hbaseAuths = NULL,
                         const char * encryptionInfo = NULL) = 0;

  virtual Lng32 scanClose() = 0;

  Lng32 fetchAllRows(
		     HbaseStr &tblName,
		     Lng32 numCols,
		     HbaseStr &col1NameStr,
		     HbaseStr &col2NameStr,
		     HbaseStr &col3NameStr,
		     LIST(NAString) &col1ValueList, // output
		     LIST(NAString) &col2ValueList, // output
		     LIST(NAString) &col3ValueList); // output

  // return 1 if table is empty, 0 if not empty. -ve num in case of error
  virtual Lng32 isEmpty(HbaseStr &tblName) = 0;
		     
  virtual Lng32 getRowOpen(
       HbaseStr &tblName,
       const HbaseStr &row, 
       const LIST(HbaseStr) & columns,
       const int64_t timestamp,
       int numReplications,
       const Int32 lockMode,
       Int32 isolationLevel,
       const NABoolean useMemoryScan,
       const NABoolean skipReadConflict = FALSE,
       HbaseAccessOptions * hao = NULL,
       const char * hbaseAuths = NULL,
       const char * encryptionInfo = NULL) = 0;

 virtual Lng32 getRowsOpen(
		HbaseStr &tblName,
		const LIST(HbaseStr) *rows, 
		const LIST(HbaseStr) & columns,
		const int64_t timestamp,
                int numReplications,
                const Int32 lockMode,
                Int32 isolationLevel,
                const NABoolean useMemoryScan,
                const NABoolean skipReadConflict,
                const NABoolean skipTransactionForBatchGet,
                HbaseAccessOptions * hao = NULL,
                const char * hbaseAuths = NULL,
                const char * encryptionInfo = NULL ) = 0;

  virtual Lng32 getRowsOpen(
       HbaseStr tblName,
       short rowIDLen,
       HbaseStr rowIDs,
       const LIST(HbaseStr) & columns,
       int numReplications,
       const Int32 lockMode,
       Int32 isolationLevel,
       const NABoolean useMemoryScan,
       const NABoolean skipReadConflict,
       const NABoolean skipTransactionForBatchGet,
       const char * encryptionInfo = NULL) = 0;
  
  virtual Lng32 nextRow() = 0;
  
  virtual Lng32 nextCell(HbaseStr &rowId,
          HbaseStr &colFamName,
          HbaseStr &colName,
          HbaseStr &colVal,
          Int64 &timestamp) = 0;

  virtual Lng32 completeAsyncOperation(Int32 timeout, NABoolean *resultArray, Int16 resultArrayLen) = 0;

  virtual Lng32 getColVal(int colNo, BYTE *colVal,
                          Lng32 &colValLen, NABoolean nullable, BYTE &nullVal,
                          BYTE *tag, Lng32 &tagLen,
                          const char * encryptionInfo = NULL) = 0;

  virtual Lng32 getColVal(NAHeap *heap, int colNo, BYTE **colVal,
                          Lng32 &colValLen,
                          const char * encryptionInfo = NULL) = 0;

  virtual Lng32 getColName(int colNo,
              char **outColName,
              short &colNameLen,
              Int64 &timestamp) = 0;
 
  virtual Lng32 getNumCellsPerRow(int &numCells) = 0;
 
  virtual Lng32 getRowID(HbaseStr &rowID, const char * encryptionInfo = NULL) = 0;
 
  virtual Lng32 deleteRow(
       HbaseStr tblName,
       HbaseStr row, 
       const LIST(HbaseStr) *columns,
       NABoolean useHbaseXn,
       const NABoolean replSync,
       const NABoolean incrementalBackup,
       NABoolean useRegionXn,
       const int64_t timestamp,
       NABoolean asyncOperation,
       const char * hbaseAuths,
       const char * encryptionInfo = NULL,
       const char * triggers = NULL,
       const char * curExecSql = NULL) = 0;
  
  virtual Lng32 deleteRows(
       HbaseStr tblName,
       short rowIDLen,
       HbaseStr rowIDs,
       const LIST(HbaseStr) *columns,
       NABoolean useHbaseXn,
       const NABoolean replSync,
       const NABoolean incrementalBackup,
       const int64_t timestamp,
       NABoolean asyncOperation,
       const char * hbaseAuths,
       const char * encryptionInfo = NULL,
       const char * triggers = NULL,
       const char * curExecSql = NULL) = 0;
  

  virtual Lng32 checkAndDeleteRow(
				  HbaseStr &tblName,
				  HbaseStr& row, 
                                  const LIST(HbaseStr) *columns,
				  HbaseStr& columnToCheck,
				  HbaseStr& colValToCheck,
                                  NABoolean useHbaseXn,
				  const NABoolean replSync,
                                  const NABoolean incrementalBackup,
                                  NABoolean useRegionXn,
				  const int64_t timestamp,
                                  const char * hbaseAuths,
                                  const char * encryptionInfo = NULL,
				  const char * triggers = NULL,
				  const char * curExecSql = NULL) = 0;

  virtual Lng32 deleteColumns(
		  HbaseStr &tblName,
		  HbaseStr & column) = 0;

  virtual Lng32 execTriggers(
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
		  NABoolean isStatement = true) = 0;
  
  virtual Lng32 insertRow(
		  HbaseStr tblName,
		  HbaseStr rowID, 
		  HbaseStr row,
		  NABoolean useHbaseXn,
		  const NABoolean replSync,
                  const NABoolean incrementalBackup,
                  NABoolean useRegionXn,
		  const int64_t timestamp,
                  NABoolean asyncOperation,
                  const char * encryptionInfo = NULL,
		  const char * triggers = NULL,
		  const char * curExecSql = NULL) = 0;

 virtual Lng32 insertRows(
		  HbaseStr tblName,
                  short rowIDLen,
                  HbaseStr rowIDs,
                  HbaseStr rows,
		  NABoolean useHbaseXn,
		  const NABoolean replSync,
                  const NABoolean incrementalBackup,
		  const int64_t timestamp,
                  NABoolean asyncOperation,
                  const char * encryptionInfo = NULL,
		  const char * triggers = NULL,
		  const char * curExecSql = NULL,
                  NABoolean noConflictCheck = FALSE) = 0; 

  virtual Lng32 lockRequired(
      NAString tblName,
      short lockMode,
      NABoolean useHbaseXn,
      const NABoolean replSync,
      const NABoolean incrementalBackup,
      NABoolean asyncOperation,
      NABoolean noConflictCheck,
      NABoolean registerRegion) = 0;

 virtual Lng32 updateVisibility(
      HbaseStr tblName,
      HbaseStr rowID, 
      HbaseStr row,
      NABoolean useHbaseXn) = 0;
 
 virtual Lng32 setWriteBufferSize(
                 HbaseStr &tblName,
                 Lng32 size)=0;
 
 virtual Lng32 setWriteToWAL(
                 HbaseStr &tblName,
                 NABoolean v)=0;
 
 virtual  Lng32 initHBLC(ExHbaseAccessStats* hbs = NULL)=0;
 virtual  Lng32 initBRC(ExHbaseAccessStats* hbs = NULL)=0;

 virtual Lng32 initHFileParams(HbaseStr &tblName,
                           Text& hFileLoc,
                           Text& hfileName,
                           Int64 maxHFileSize,
                           Text& hFileSampleLoc,
                           Text& hfileSampleName,
                           float fSampleRate) = 0;

 virtual Lng32 addToHFile(short rowIDLen,
                          HbaseStr &rowIDs,
                          HbaseStr &rows,
                          const char * encryptionInfo) = 0;
 
 virtual Lng32 closeHFile(HbaseStr &tblName) = 0;

 virtual Lng32 doBulkLoad(HbaseStr &tblName,
                          Text& location,
                          Text& tableName,
                          NABoolean quasiSecure,
                          NABoolean snapshot) = 0;

 virtual Lng32 bulkLoadCleanup(HbaseStr &tblName,
                          Text& location) = 0;

 virtual Lng32  sentryGetPrivileges(set<string> & groupNames,
                                    const char * tableOrViewName,
                                    bool isView,
                                    map<Lng32,char *> & columnNumberToNameMap,
                                    PrivMgrUserPrivs & userPrivs /* out */)=0;
 virtual Lng32  sentryGetPrivileges(const char * userName,
                                    const char * tableOrViewName,
                                    bool isView,
                                    map<Lng32,char *> & columnNumberToNameMap,
                                    PrivMgrUserPrivs & userPrivs /* out */)=0;
 virtual Lng32  incrCounter( const char * tabName, const char * rowId,
                             const char * famName, const char * qualName ,
                             Int64 incr, Int64 & count)=0;

 virtual Lng32  createCounterTable( const char * tabName,
                                   const char * famName)=0;
  virtual Lng32 checkAndInsertRow(
				  HbaseStr &tblName,
				  HbaseStr& rowID, 
				  HbaseStr& row,
                                  NABoolean useHbaseXn,
				  const NABoolean replSync,
                                  const NABoolean incrementalBackup,
                                  NABoolean useRegionXn,
				  const int64_t timestamp,
                                  NABoolean asyncOperation,
                                  const char * encryptionInfo = NULL,
				  const char * triggers = NULL,
				  const char * curExecSql = NULL,
				  Int16 colIndexToCheck = 0) = 0;

  
  virtual Lng32 checkAndUpdateRow(
				  HbaseStr &tblName,
				  HbaseStr& rowID, 
				  HbaseStr& row,
				  HbaseStr& columnToCheck,
				  HbaseStr& colValToCheck,
                                  NABoolean useHbaseXn,
				  const NABoolean replSync,
                                  const NABoolean incrementalBackup,
                                  NABoolean useRegionXn,
				  const int64_t timestamp,
                                  NABoolean asyncOperation,
                                  const char * encryptionInfo = NULL,
				  const char * triggers = NULL,
				  const char * curExecSql = NULL) = 0;

 
  virtual Lng32 getClose() = 0;

  virtual Lng32 coProcAggr(
			 HbaseStr &tblName,
			 Lng32 aggrType, // 0:count, 1:min, 2:max, 3:sum, 4:avg
			 const Text& startRow, 
			 const Text& stopRow, 
			 const Text &colFamily,
			 const Text &colName,
			 const NABoolean cacheBlocks,
			 const Lng32 numCacheRows,
			 const NABoolean replSync,
			 Text &aggrVal); // returned value

  virtual Lng32 coProcAggr(
                         HbaseStr &tblName,
                         Lng32 aggrType, // 0:count, 1:min, 2:max, 3:sum, 4:avg
                         const Text& startRow,
                         const Text& stopRow,
                         const Text &colFamily,
                         const Text &colName,
                         const NABoolean cacheBlocks,
                         const Lng32 numCacheRows,
                         const NABoolean replSync,
                         Text &aggrVal, // returned value
                         Int32 isolationLevel,
                         Int32 lockMode);

  virtual Lng32 grant(
		      const Text& user, 
		      const Text& tblName,
		      const std::vector<Text> & actionCodes) = 0;

  virtual Lng32 revoke(
		      const Text& user, 
		      const Text& tblName,
		      const std::vector<Text> & actionCodes) = 0;

  virtual NAArray<HbaseStr>* getRegionBeginKeys(const char*) = 0;
  virtual NAArray<HbaseStr>* getRegionEndKeys(const char*) = 0;

  virtual Lng32 estimateRowCount(HbaseStr& tblName,
                                 Int32 partialRowSize,
                                 Int32 numCols,
                                 Int32 retryLimitMilliSeconds,
                                 NABoolean useCoprocessor,
                                 Int64& estRC,
                                 Int32& breadCrumb) = 0;
  virtual Lng32 cleanSnpTmpLocation( const char * path)=0;
  virtual Lng32 setArchivePermissions( const char * tbl)=0;

  virtual Lng32 getBlockCacheFraction(float& frac) = 0;
  virtual Lng32 getHbaseTableInfo(const HbaseStr& tblName,
                                  Int32& indexLevels,
                                  Int32& blockSize) = 0;

  virtual Lng32 getRegionsNodeName(const HbaseStr& tblName,
                                   Int32 partns,
                                   ARRAY(const char *)& nodeNames) = 0;
 
  virtual NAArray<HbaseStr> * showTablesHDFSCache(const std::vector<Text>& tables) = 0;

  virtual Lng32 addTablesToHDFSCache(const std::vector<Text>& tables, const char* poolName) = 0;
  virtual Lng32 removeTablesFromHDFSCache(const std::vector<Text>& tables, const char* poolName) = 0;
  // get regions and size
  virtual NAArray<HbaseStr> *getRegionStats(const HbaseStr& tblName) = 0;

  virtual NAArray<HbaseStr> *getClusterStats(Int32 &numEntries) = 0;

  void settrigger_operation(int v) {trigger_operation_ = v;}
  int gettrigger_operation() {return trigger_operation_;}
  void setUseTrigger(NABoolean v) {useTrigger_ = v;}
  NABoolean getUseTrigger() {return useTrigger_;}
  
  // Snapshots
  virtual Lng32 createSnapshot( const NAString&  tableName, const NAString&  snapshotName) = 0;
  virtual Lng32 restoreSnapshot( const NAString&  snapshotName, const NAString&  tableName) = 0;
  virtual Lng32 deleteSnapshot( const NAString&  snapshotName) = 0;
  virtual Lng32 verifySnapshot( const NAString&  tableName, const NAString&  snapshotName,
                                                NABoolean & exist) = 0;
  // call dtm to commit or rollback a savepoint.
  // isCommit = TRUE, commit. isCommit = FALSE, rollback.
  virtual Lng32 savepointCommitOrRollback(Int64 transId,
                                          Int64 savepointId,
                                          Int64 tgtSavepointId,
                                          NABoolean isCommit) = 0;

  // process namespace related commands.
  //   create: oper=1. Create namespace 'nameSpace'
  //   drop: oper=2.   Drop namespace 'nameSpace'
  //   getNamespaces: oper=3. Return: retNames contain available namespaces
  //   getNamespaceTables: oper=4. Return: retNames contains names of all
  //                                     objects in namespace 'nameSpace'
  virtual Lng32 namespaceOperation
  (short oper, const char *nameSpace,
   Lng32 numKeyValEntries,
   NAText * keyArray, NAText * valArray,
   NAArray<HbaseStr> **retNames) = 0;

  void setWaitOnSelectForUpdate(NABoolean v)
    {(v ? flags_ |= SELECT_FOR_UPDATE_WAIT: flags_ &= ~SELECT_FOR_UPDATE_WAIT); };
  NABoolean waitOnSelectForUpdate() { return (flags_ & SELECT_FOR_UPDATE_WAIT) != 0; };


  void setNoConflictCheckForIndex(NABoolean v)
    {(v ? flags_ |= NO_CONFLICT_CHECK_FOR_IDX: flags_ &= ~NO_CONFLICT_CHECK_FOR_IDX); }
  NABoolean noConflictCheckForIndex() { return (flags_ & NO_CONFLICT_CHECK_FOR_IDX) != 0; };

  void setFirstReadBypassTm(NABoolean v)
    {(v ? flags_ |= FIRST_READ_BYPASS_TM: flags_ &= ~FIRST_READ_BYPASS_TM); }
  NABoolean getFirstReadBypassTm() { return (flags_ & FIRST_READ_BYPASS_TM) != 0; };

  void setPutIsUpsert(NABoolean v)
    {(v ? flags_ |= EXP_PUT_IS_UPSERT: flags_ &= ~EXP_PUT_IS_UPSERT); }
  NABoolean expPutIsUpsert() { return (flags_ & EXP_PUT_IS_UPSERT) != 0; };

  virtual void setDDLValidator(ExDDLValidator * ddlValidator) = 0;

  virtual short  getNextValue(NAString& tabName, NAString& rowId,
                              NAString& famName, NAString& qualName,
                              Int64 incrBy, Int64 &nextValue,
                              NABoolean skipWAL) = 0;

  virtual Lng32  deleteSeqRow(NAString& tabName, NAString& rowId) = 0;

  virtual Lng32  updateTableDefForBinlog(NAString& tabName, NAString& cols , NAString& keyCols, long ts) = 0;
  virtual Lng32 getTableDefForBinlog(NAString& tabName,  NAArray<HbaseStr>* *retNames) = 0;

  virtual Lng32 putData(Int64 eventID, const char* query, int eventType, const char* schemaName, unsigned char* params, long len) = 0;

  virtual void memoryTableCreate() = 0;
  virtual bool memoryTableInsert(HbaseStr &key, HbaseStr &val) = 0;
  virtual void memoryTableRemove(const char* tabName) = 0;
  virtual NABoolean isReadFromMemoryTable() = 0;
  virtual NABoolean isMemoryTableDisabled() = 0;
  virtual NABoolean ismemDBinitFailed() = 0;

protected:
  enum 
    {
      MAX_SERVER_SIZE = 999,
      MAX_PORT_SIZE = 99,
      MAX_CONNECT_PARAM_SIZE = 1024
    };

  enum
    {
      SELECT_FOR_UPDATE_WAIT     = 0x00001,
      NO_CONFLICT_CHECK_FOR_IDX  = 0x00002,
      EXP_PUT_IS_UPSERT          = 0x00004,
      FIRST_READ_BYPASS_TM       = 0x00008
    };  //for flag_

  ExpHbaseInterface(CollHeap * heap,
                    const char * server = NULL,
                    const char * zkPort = NULL);

  CollHeap * heap_;
  ExHbaseAccessStats * hbs_;
  char connectParam1_[MAX_SERVER_SIZE+1];
  char connectParam2_[MAX_PORT_SIZE+1];
  int  trigger_operation_;
  NABoolean useTrigger_;
  UInt32 flags_;
};

char * getHbaseErrStr(Lng32 errEnum);

// ===========================================================================
class ExpHbaseInterface_JNI : public ExpHbaseInterface
{
 public:

  ExpHbaseInterface_JNI(CollHeap* heap,
                        const char* server, bool useTRex, NABoolean replSync, 
                        const char *zkPort, 
                        ComStorageType storageType = COM_STORAGE_HBASE);
  
  virtual ~ExpHbaseInterface_JNI();
  
  virtual Lng32 init(ExHbaseAccessStats *hbs = NULL);
  
  virtual Lng32 cleanup();

  virtual Lng32 close();

  virtual Lng32 create(HbaseStr &tblName,
	               HBASE_NAMELIST& colFamNameList,
                       NABoolean isMVCC);

  virtual Lng32 create(HbaseStr &tblName,
	               NAText* hbaseCreateOptionsArray,
                       int numSplits, int keyLength,
                       const char ** splitValues,
                       NABoolean useHbaseXn,
                       NABoolean isMVCC,
                       NABoolean incrBackupEnabled);

  virtual Lng32 create(HbaseStr &tblName,
                       const NAList<HbaseStr> &cols);

  virtual Lng32 create(HbaseStr &tblName,
                       int tableType,
                       const NAList<HbaseStr> &cols,
                       NAText * monarchCreateOptionsArray,
                       int numSplits, int keyLength,
                       const char ** splitValues,
                       NABoolean useHbaseXn,
                       NABoolean isMVCC);
  
  virtual Lng32 alter(HbaseStr &tblName,
		      NAText * hbaseCreateOptionsArray,
                      NABoolean useHbaseXn);

  virtual Lng32 registerTruncateOnAbort(HbaseStr &tblName, NABoolean useHbaseXn);
  virtual Lng32 truncate(HbaseStr &tblName, NABoolean preserveSplits, NABoolean useHbaseXn);
  virtual Lng32 drop(HbaseStr &tblName, NABoolean async, NABoolean useHbaseXn);
  virtual Lng32 dropAll(const char * pattern, NABoolean async, NABoolean useHbaseXn);

  virtual NAArray<HbaseStr>* listAll(const char * pattern);

  // process namespace related commands.
  //   create: oper=1. Create namespace 'nameSpace'
  //   drop: oper=2.   Drop namespace 'nameSpace'
  //   getNamespaces: oper=3. Return: retNames constains available namespaces
  //   getNamespaceTables: oper=4. Return: retNames contains names of all
  //                                     objects in namespace 'nameSpace'
  virtual Lng32 namespaceOperation
  (short oper, const char *nameSpace,
   Lng32 numKeyValEntries,
   NAText * keyArray, NAText * valArray,
   NAArray<HbaseStr> **retNames);

  // make a copy of srcTblName as tgtTblName
  // if force is true, remove target before copying.
  virtual Lng32 copy(HbaseStr &srcTblName, HbaseStr &tgtTblName,
                     NABoolean force = FALSE);

  virtual Lng32 backupObjects(const std::vector<Text>& tables, 
                              const std::vector<Text>& incrBackupEnabled,
                              const std::vector<Text>& lobLocList,
                              const char* backuptag,
                              const char *extendedAttributes,
                              const char* backupType,
                              const int backupThreads,

                              // 0, update for each table.
                              // -1, dont update
                              // N, update every N secs 
                              const int progressUpdateDelay);

  virtual Lng32 createSnapshotForIncrBackup(const std::vector<Text>& tables);
  
  virtual Lng32 setHiatus(const NAString &hiatusObjectName,
                          NABoolean lockOperation,
                          NABoolean createSnapIfNotExist,
                          NABoolean ignoreSnapIfNotExist,
                          const int parallelThreads);
  virtual Lng32 clearHiatus(const NAString &hiatusObjectName);

  virtual NAArray<HbaseStr>* restoreObjects(
       const char* backuptag, 
       const std::vector<Text> *schemas,
       const std::vector<Text> *tables, 
       const char* restoreToTS,
       NABoolean showObjects,
       NABoolean saveObjects,
       NABoolean restoreSavedObjects,
       int parallelThreads,

       // 0, update for each table.
       // -1, dont update
       // N, update every N secs 
       const int progressUpdateDelay);

  virtual Lng32 finalizeBackup(
       const char* backuptag,
       const char* backupType);

  virtual Lng32 finalizeRestore(
       const char* backuptag,
       const char* backupType);

  virtual Lng32 deleteBackup(const char* backuptag, 
                             NABoolean timestamp = FALSE,
                             NABoolean cascade = FALSE,
                             NABoolean force = FALSE,
                             NABoolean skipLock = FALSE);

  virtual Lng32 exportOrImportBackup(const char* backuptag, 
                                     NABoolean isExport,
                                     NABoolean override,
                                     const char* location,
                                     int parallelThreads,

                                     // 0, update for each table.
                                     // -1, dont update
                                     // N, update every N secs 
                                     const int progressUpdateDelay);
  
  virtual Lng32 listAllBackups(NAArray<HbaseStr> **backupList,
                               NABoolean shortFormat, NABoolean reverseOrder);
  
  virtual NAString getBackupType(const char* backuptag);
  virtual NAString getExtendedAttributes(const char* backuptag);
  virtual NAString getBackupStatus(const char* backuptag);

  virtual NAString getPriorBackupTag(const char* restoreToTimestamp);
  virtual NAString getRestoreToTsBackupTag(const char* restoreToTimestamp);

  virtual NAString lockHolder();
  virtual Lng32 operationLock(const char* backuptag);
  virtual Lng32 operationUnlock(const char* backuptag, NABoolean recoverMeta, 
                                NABoolean cleanupLock);

  virtual NAArray<HbaseStr>* getLinkedBackupTags(const char* backuptag);

  // -1, if table exists. 0, if doesn't. -ve num, error.
  virtual Lng32 exists(HbaseStr &tblName);

  // returns the next tablename. 100, at EOD.
  virtual Lng32 getTable(HbaseStr &tblName);

  virtual Lng32 scanOpen(
			 HbaseStr &tblName,
			 const Text& startRow, 
			 const Text& stopRow, 
			 const LIST(HbaseStr) & columns,
			 const int64_t timestamp,
			 const NABoolean useHbaseXn,
                const NABoolean useMemoryScan,
                         const Int32 lockMode,
                         Int32 isolationLevel,
			 const NABoolean skipReadConflict,
                         const NABoolean skipTransaction,
			 const NABoolean replSync,
			 const NABoolean cacheBlocks,
			 const NABoolean smallScanner,
			 const Lng32 numCacheRows,
                         const NABoolean preFetch,
			 const LIST(NAString) *inColNamesToFilter, 
			 const LIST(NAString) *inCompareOpList,
			 const LIST(NAString) *inColValuesToCompare,
                         int numReplications,
                         Float32 DOPparallelScanner = 0.0f,
			 Float32 samplePercent = -1.0f,
			 NABoolean useSnapshotScan = FALSE,
			 Lng32 snapTimeout = 0,
			 char * snapName = NULL,
			 char * tmpLoc = NULL,
			 Lng32 espNum = 0,
                         HbaseAccessOptions * hao = NULL,
                         const char * hbaseAuthos = NULL,
                         const char * encryptionInfo = NULL);

  virtual Lng32 scanClose();

  // return 1 if table is empty, 0 if not empty. -ve num in case of error
  virtual Lng32 isEmpty(HbaseStr &tblName);

  virtual Lng32 getRowOpen(
		HbaseStr &tblName,
		const HbaseStr &row, 
		const LIST(HbaseStr) & columns,
		const int64_t timestamp,
                int numReplications,
                const Int32 lockMode,
                Int32 isolationLevel,
                const NABoolean useMemoryScan,
                const NABoolean skipReadConflict = FALSE,
                HbaseAccessOptions * hao = NULL,
                const char * hbaseAuths = NULL,
                const char * encryptionInfo = NULL);
 
 virtual Lng32 getRowsOpen(
		HbaseStr &tblName,
		const LIST(HbaseStr) *rows, 
		const LIST(HbaseStr) & columns,
		const int64_t timestamp,
                int numReplications,
                const Int32 lockMode,
                Int32 isolationLevel,
                const NABoolean useMemoryScan,
                const NABoolean skipReadConflict,
                const NABoolean skipTransactionForBatchGet,
                HbaseAccessOptions * hao = NULL,
                const char * hbaseAuths = NULL,
                const char * encryptionInfo = NULL);

 virtual Lng32 getRowsOpen(
          HbaseStr tblName,
          short rowIDLen,
          HbaseStr rowIDs,
          const LIST(HbaseStr) & columns,
          int numReplications,
          const Int32 lockMode,
          Int32 isolationLevel,
          const NABoolean useMemoryScan,
          const NABoolean skipReadConflict,
          const NABoolean skipTransactionForBatchGet,
          const char * encryptionInfo = NULL);

  virtual Lng32 nextRow();

  virtual Lng32 nextCell( HbaseStr &rowId,
          HbaseStr &colFamName,
          HbaseStr &colName,
          HbaseStr &colVal,
          Int64 &timestamp);
 
  virtual Lng32 completeAsyncOperation(Int32 timeout, NABoolean *resultArray, Int16 resultArrayLen);

  virtual Lng32 getColVal(int colNo, BYTE *colVal,
                          Lng32 &colValLen, NABoolean nullable, BYTE &nullVal,
                          BYTE *tag, Lng32 &tagLen,
                          const char * encryptionInfo = NULL);

  virtual Lng32 getColVal(NAHeap *heap, int colNo, BYTE **colVal,
                          Lng32 &colValLen,
                          const char * encryptionInfo = NULL);

  virtual Lng32 getColName(int colNo,
              char **outColName,
              short &colNameLen,
              Int64 &timestamp);

  virtual Lng32 getNumCellsPerRow(int &numCells);

  virtual Lng32 getRowID(HbaseStr &rowID, const char * encryptionInfo = NULL);

  virtual Lng32 deleteRow(
       HbaseStr tblName,
       HbaseStr row, 
       const LIST(HbaseStr) *columns,
       NABoolean useHbaseXn,
       const NABoolean replSync,
       const NABoolean incrementalBackup,
       NABoolean useRegionXn,
       const int64_t timestamp,
       NABoolean asyncOperation,
       const char * hbaseAuths,
       const char * encryptionInfo = NULL,
       const char * triggers = NULL,
       const char * curExecSql = NULL);

  virtual Lng32 deleteRows(
       HbaseStr tblName,
       short rowIDLen,
       HbaseStr rowIDs,
       const LIST(HbaseStr) *columns,
       NABoolean useHbaseXn,
       const NABoolean replSync,
       const NABoolean incrementalBackup,
       const int64_t timestamp,
       NABoolean asyncOperation,
       const char * hbaseAuths,
       const char * encryptionInfo = NULL,
       const char * triggers = NULL,
       const char * curExecSql = NULL);

  virtual Lng32 checkAndDeleteRow(
       HbaseStr &tblName,
       HbaseStr& row, 
       const LIST(HbaseStr) *columns,
       HbaseStr& columnToCheck,
       HbaseStr& colValToCheck,
       NABoolean useHbaseXn,     
       const NABoolean replSync,
       const NABoolean incrementalBackup,
       NABoolean useRegionXn,
       const int64_t timestamp,
       const char * hbaseAuths,
       const char * encryptionInfo = NULL,
       const char * triggers = NULL,
       const char * curExecSql = NULL);

  virtual Lng32 deleteColumns(
		  HbaseStr &tblName,
		  HbaseStr & column);

  virtual Lng32 execTriggers(
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
		  NABoolean isStatement = true);
  
  virtual Lng32 insertRow(
		  HbaseStr tblName,
		  HbaseStr rowID, 
                  HbaseStr row,
		  NABoolean useHbaseXn,
		  const NABoolean replSync,
                  const NABoolean incrementalBackup,
                  NABoolean useRegionXn,
		  const int64_t timestamp,
                  NABoolean asyncOperation,
                  const char * encryptionInfo = NULL,
		  const char * triggers = NULL,
		  const char * curExecSql = NULL);

 virtual Lng32 insertRows(
		  HbaseStr tblName,
                  short rowIDLen,
                  HbaseStr rowIDs,
                  HbaseStr rows,
		  NABoolean useHbaseXn,
		  const NABoolean replSync,
                  const NABoolean incrementalBackup,
		  const int64_t timestamp,
                  NABoolean asyncOperation,
                  const char * encryptionInfo = NULL,
		  const char * triggers = NULL,
		  const char * curExecSql = NULL,
                  NABoolean noConflictCheck = FALSE); 
  
  virtual Lng32 lockRequired(
      NAString tblName,
      short lockMode,
      NABoolean useHbaseXn,
      const NABoolean replSync,
      const NABoolean incrementalBackup,
      NABoolean asyncOperation,
      NABoolean noConflictCheck,
      NABoolean registerRegion);
  
  virtual Lng32 updateVisibility(
       HbaseStr tblName,
       HbaseStr rowID, 
       HbaseStr row,
       NABoolean useHbaseXn);
  
  virtual Lng32 setWriteBufferSize(
                  HbaseStr &tblName,
                  Lng32 size);
  
  virtual Lng32 setWriteToWAL(
                  HbaseStr &tblName,
                  NABoolean v);

virtual  Lng32 initHBLC(ExHbaseAccessStats* hbs = NULL);
virtual  Lng32 initBRC(ExHbaseAccessStats* hbs = NULL);

virtual Lng32 initHFileParams(HbaseStr &tblName,
                           Text& hFileLoc,
                           Text& hfileName,
                           Int64 maxHFileSize,
                           Text& hFileSampleLoc,
                           Text& hfileSampleName,
                           float fSampleRate);
 virtual Lng32 addToHFile(short rowIDLen,
                          HbaseStr &rowIDs,
                          HbaseStr &rows,
                          const char * encryptionInfo);

 virtual Lng32 closeHFile(HbaseStr &tblName);

 virtual Lng32 doBulkLoad(HbaseStr &tblName,
                          Text& location,
                          Text& tableName,
                          NABoolean quasiSecure,
                          NABoolean snapshot);
 
 virtual Lng32 bulkLoadCleanup(HbaseStr &tblName,
                          Text& location);
 virtual Lng32  sentryGetPrivileges(set<string> & groupNames,
                                    const char * tableOrViewName,
                                    bool isView,
                                    map<Lng32,char *> & columnNumberToNameMap,
                                    PrivMgrUserPrivs & userPrivs /* out */);
 virtual Lng32  sentryGetPrivileges(const char * userName,
                                    const char * tableOrViewName,
                                    bool isView,
                                    map<Lng32,char *> & columnNumberToNameMap,
                                    PrivMgrUserPrivs & userPrivs /* out */);
 virtual Lng32  incrCounter( const char * tabName, const char * rowId,
                             const char * famName, const char * qualName ,
                             Int64 incr, Int64 & count);
 virtual Lng32  createCounterTable( const char * tabName,
                                   const char * famName);

  virtual Lng32 checkAndInsertRow(
				  HbaseStr &tblName,
				  HbaseStr& rowID, 
				  HbaseStr& row,
                                  NABoolean useHbaseXn,
				  const NABoolean replSync,
                                  const NABoolean incrementalBackup,
                                  NABoolean useRegionXn,
				  const int64_t timestamp,
                  		  NABoolean asyncOperation,
                                  const char * encryptionInfo = NULL,
				  const char * triggers = NULL,
				  const char * curExecSql = NULL,
				  Int16 colIndexToCheck = 0);



  virtual Lng32 checkAndUpdateRow(
				  HbaseStr &tblName,
				  HbaseStr& rowID, 
				  HbaseStr& row,
				  HbaseStr& columnToCheck,
				  HbaseStr& colValToCheck,
                                  NABoolean useHbaseXn,			
				  const NABoolean replSync,
                                  const NABoolean incrementalBackup,
                                  NABoolean useRegionXn,
				  const int64_t timestamp,
                                  NABoolean asyncOperation,
                                  const char * encryptionInfo = NULL,
				  const char * triggers = NULL,
				  const char * curExecSql = NULL);

  virtual Lng32 coProcAggr(
			 HbaseStr &tblName,
			 Lng32 aggrType, // 0:count, 1:min, 2:max, 3:sum, 4:avg
			 const Text& startRow, 
			 const Text& stopRow, 
			 const Text &colFamily,
			 const Text &colName,
			 const NABoolean cacheBlocks,
			 const Lng32 numCacheRows,
			 const NABoolean replSync,
			 Text &aggrVal); // returned value

  virtual Lng32 coProcAggr(
                         HbaseStr &tblName,
                         Lng32 aggrType, // 0:count, 1:min, 2:max, 3:sum, 4:avg
                         const Text& startRow,
                         const Text& stopRow,
                         const Text &colFamily,
                         const Text &colName,
                         const NABoolean cacheBlocks,
                         const Lng32 numCacheRows,
                         const NABoolean replSync,
                         Text &aggrVal, // returned value
                         Int32 isolationLevel,
                         Int32 lockMode);
 
  virtual Lng32 getClose();

  virtual Lng32 grant(
		      const Text& user, 
		      const Text& tblName,
  		      const std::vector<Text> & actionCodes);

  virtual Lng32 revoke(
		      const Text& user, 
		      const Text& tblName,
  		      const std::vector<Text> & actionCodes);

  virtual NAArray<HbaseStr>* getRegionBeginKeys(const char*);
  virtual NAArray<HbaseStr>* getRegionEndKeys(const char*);

  virtual Lng32 estimateRowCount(HbaseStr& tblName,
                                 Int32 partialRowSize,
                                 Int32 numCols,
                                 Int32 retryLimitMilliSeconds,
                                 NABoolean useCoprocessor,
                                 Int64& estRC,
                                 Int32& breadCrumb);

  virtual Lng32 cleanSnpTmpLocation( const char * path);
  virtual Lng32  setArchivePermissions( const char * tabName) ;

  virtual Lng32 getBlockCacheFraction(float& frac) ;
  virtual Lng32 getHbaseTableInfo(const HbaseStr& tblName,
                                  Int32& indexLevels,
                                  Int32& blockSize) ;
  virtual Lng32 getRegionsNodeName(const HbaseStr& tblName,
                                   Int32 partns,
                                   ARRAY(const char *)& nodeNames) ;

  virtual NAArray<HbaseStr> * showTablesHDFSCache(const std::vector<Text>& tables);
  
  virtual Lng32 addTablesToHDFSCache(const std::vector<Text> & tables, const char* poolName);
  virtual Lng32 removeTablesFromHDFSCache(const std::vector<Text> & tables, const char* poolName);
  virtual NAArray<HbaseStr>* getRegionStats(const HbaseStr& tblName);
  virtual NAArray<HbaseStr>* getClusterStats(Int32 &numEntries);

  virtual Lng32 createSnapshot( const NAString&  tableName, const NAString&  snapshotName);
  virtual Lng32 restoreSnapshot( const NAString&  snapshotName, const NAString&  tableName);
  virtual Lng32 deleteSnapshot( const NAString&  snapshotName);
  virtual Lng32 verifySnapshot( const NAString&  tableName, const NAString&  snapshotName,
                                                NABoolean & exist);

  // call dtm to commit or rollback a savepoint.
  // isCommit = TRUE, commit. isCommit = FALSE, rollback.
  virtual Lng32 savepointCommitOrRollback(Int64 transId,
                                          Int64 savepointId,
                                          Int64 tgtSavepointId,
                                          NABoolean isCommit);

  virtual void setDDLValidator(ExDDLValidator * ddlValidator);

  virtual short  getNextValue(NAString& tabName, NAString& rowId,
                              NAString& famName, NAString& qualName,
                              Int64 incrBy, Int64 &nextValue,
                              NABoolean skipWAL);

  virtual Lng32  deleteSeqRow(NAString& tabName, NAString& rowId);

  virtual Lng32 getLockErrorNum(Int32 retCode);
  virtual Lng32  updateTableDefForBinlog(NAString& tabName, NAString& cols , NAString& keyCols, long ts);
  virtual Lng32 getTableDefForBinlog(NAString& tabName,  NAArray<HbaseStr>* *retNames);

  virtual Lng32 putData(Int64 eventID, const char* query, int eventType, const char* schemaName, unsigned char* params, long len);

  //member function for memory table access
  void memoryTableCreate();
  bool memoryTableInsert(HbaseStr &key, HbaseStr &val);
  void memoryTableRemove(const char* tabName);
  NABoolean isReadFromMemoryTable() { return readFromMemoryTable_; }
  NABoolean isMemoryTableDisabled() { return memoryTableIsDisabled_; }
  NABoolean ismemDBinitFailed() { return memDBinitFailed_; }

private:
  bool  useTRex_;
  NABoolean replSync_;
  HBaseClient_JNI* client_;
  HTableClient_JNI* htc_;
  HBulkLoadClient_JNI* hblc_;
  BackupRestoreClient_JNI* brc_;
  HTableClient_JNI *asyncHtc_;
  Int32  retCode_;
  ComStorageType storageType_;
  MonarchClient_JNI *mClient_;
  MTableClient_JNI *mtc_;
  MTableClient_JNI *asyncMtc_;
  NABoolean bigtable_;
  ExDDLValidator * ddlValidator_;
  MemoryTableClient *mhtc_;
  NABoolean readFromMemoryTable_;
  NABoolean memoryTableIsDisabled_;
  NABoolean memDBinitFailed_;
};

#endif
