/* -*-C++-*-
****************************************************************************
*
* File:             ExpHbaseInterface.h
* Description:  Interface to Hbase world
* Created:        5/26/2013
* Language:      C++
*
*

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

#include "executor/HBaseClient_JNI.h"
#include "comexe/ComTdbHbaseAccess.h"
#include "sqlcomp/CmpSeabaseDDLincludes.h"
#include "executor/ExDDLValidator.h"

#define INLINE_COLNAME_LEN 256

class ex_globals;
class CliGlobals;

long getTransactionIDFromContext();
long getSavepointIDFromContext(long &pSvptId);
int getTransactionIsolationLevelFromContext();
int checkAndWaitSnapshotInProgress(NAHeap *heap);

class ExpHbaseInterface : public NABasicObject {
 public:
  NAHeap *getHeap() { return (NAHeap *)heap_; }
  static ExpHbaseInterface *newInstance(CollHeap *heap, const char *server, const char *zkPort,
                                        ComStorageType storageType, NABoolean replSync);

  virtual ~ExpHbaseInterface() {}

  virtual int init(ExHbaseAccessStats *hbs = NULL) = 0;

  virtual int cleanup() = 0;

  virtual int close() = 0;

  virtual int create(HbaseStr &tblName, HBASE_NAMELIST &colFamNameList, NABoolean isMVCC) = 0;

  virtual int create(HbaseStr &tblName, NAText *hbaseCreateOptionsArray, int numSplits, int keyLength,
                       const char **splitValues, NABoolean useHbaseXn, NABoolean isMVCC,
                       NABoolean incrBackupEnabled) = 0;

  virtual int create(HbaseStr &tblName, const NAList<HbaseStr> &cols) = 0;

  virtual int create(HbaseStr &tblName, int tableType, const NAList<HbaseStr> &cols,
                       NAText *monarchCreateOptionsArray, int numSplits, int keyLength, const char **splitValues,
                       NABoolean useHbaseXn, NABoolean isMVCC) = 0;

  virtual int alter(HbaseStr &tblName, NAText *hbaseCreateOptionsArray, NABoolean useHbaseXn) = 0;

  // During upsert using load, register truncate on abort will be used
  virtual int registerTruncateOnAbort(HbaseStr &tblName, NABoolean useHbaseXn) = 0;

  // During a drop of seabase table or index, the object is first removed from
  // seabase metadata. If that succeeds, the corresponding hbase object is dropped.
  // if sync is TRUE, this drop of hbase table is done in another worker thread.
  // That speeds up the over all drop time.
  // If a create of the same table comes in later and an error is returned
  // during create, we delay and retry for a fixed number of times since that table
  // may still be dropped by the worked thread.
  virtual int drop(HbaseStr &tblName, NABoolean async, NABoolean useHbaseXn) = 0;
  virtual int truncate(HbaseStr &tblName, NABoolean preserveSplits, NABoolean useHbaseXn) = 0;

  // drops all objects from hbase that match the pattern
  virtual int dropAll(const char *pattern, NABoolean async, NABoolean useHbaseXn) = 0;

  // retrieve all objects from hbase that match the pattern
  virtual NAArray<HbaseStr> *listAll(const char *pattern) = 0;

  // make a copy of srcTblName as tgtTblName
  // if force is true, remove target before copying.
  virtual int copy(HbaseStr &srcTblName, HbaseStr &tgtTblName, NABoolean force = FALSE);

  virtual int backupObjects(const std::vector<Text> &tables, const std::vector<Text> &incrBackupEnabled,
                              const std::vector<Text> &lobLocList, const char *backuptag,
                              const char *extendedAttributes, const char *backupType, const int backupThreads,

                              // 0, update for each table.
                              // -1, dont update
                              // N, update every N secs
                              const int progressUpdateDelay);

  // creates snapshot fpr specified tables that will be used as the base image
  // for further incremental backups
  virtual int createSnapshotForIncrBackup(const std::vector<Text> &tables);

  virtual int setHiatus(const NAString &hiatusObjectName, NABoolean lockOperation, NABoolean createSnapIfNotExist,
                          NABoolean ignoreSnapIfNotExist, const int parallelThreads);
  virtual int clearHiatus(const NAString &hiatusObjectName);

  virtual NAArray<HbaseStr> *restoreObjects(const char *backuptag, const std::vector<Text> *schemas,
                                            const std::vector<Text> *tables, const char *restoreToTS,
                                            NABoolean showObjects, NABoolean saveObjects, NABoolean restoreSavedObjects,
                                            int parallelThreads,

                                            // 0, update for each table.
                                            // -1, dont update
                                            // N, update every N secs
                                            const int progressUpdateDelay);

  virtual int finalizeBackup(const char *backuptag, const char *backupType);

  virtual int finalizeRestore(const char *backuptag, const char *backupType);

  virtual int deleteBackup(const char *backuptag, NABoolean timestamp = FALSE, NABoolean cascade = FALSE,
                             NABoolean force = FALSE, NABoolean skipLock = FALSE);

  virtual int exportOrImportBackup(const char *backuptag, NABoolean isExport, NABoolean override,
                                     const char *location, int parallelThreads,

                                     // 0, update for each table.
                                     // -1, dont update
                                     // N, update every N secs
                                     const int progressUpdateDelay);

  virtual int listAllBackups(NAArray<HbaseStr> **backupList, NABoolean shortFormat, NABoolean reverseOrder);

  virtual NAString getBackupType(const char *backuptag) { return NAString(""); }
  virtual NAString getExtendedAttributes(const char *backuptag) { return NAString(""); }
  virtual NAString getBackupStatus(const char *backupTag) { return NAString(""); }

  virtual NAString getPriorBackupTag(const char *restoreToTimestamp) { return NAString(""); }

  virtual NAString getRestoreToTsBackupTag(const char *restoreToTimestamp) { return NAString(""); }

  virtual NAString lockHolder() { return NAString(""); }
  virtual int operationLock(const char *backuptag) { return HBASE_BACKUP_OPERATION_ERROR; }
  virtual int operationUnlock(const char *backuptag, NABoolean recoverMeta, NABoolean cleanupLock) {
    return HBASE_BACKUP_OPERATION_ERROR;
  }

  virtual NAArray<HbaseStr> *getLinkedBackupTags(const char *backuptag);

  virtual int exists(HbaseStr &tblName) = 0;

  // returns the next tablename. 100, at EOD.
  virtual int getTable(HbaseStr &tblName) = 0;

  virtual int scanOpen(HbaseStr &tblName, const Text &startRow, const Text &stopRow, const LIST(HbaseStr) & columns,
                         const int64_t timestamp, const NABoolean useHbaseXn, const NABoolean useMemoryScan,
                         const int lockMode, int isolationLevel, const NABoolean skipReadConflict,
                         const NABoolean skipTransaction, const NABoolean replSync, const NABoolean cacheBlocks,
                         const NABoolean smallScanner, const int numCacheRows, const NABoolean preFetch,
                         const LIST(NAString) * inColNamesToFilter, const LIST(NAString) * inCompareOpList,
                         const LIST(NAString) * inColValuesToCompare, int numReplications,
                         Float32 dopParallelScanner = 0.0f, Float32 samplePercent = -1.0f,
                         NABoolean useSnapshotScan = FALSE, int snapTimeout = 0, char *snapName = NULL,
                         char *tmpLoc = NULL, int espNum = 0, HbaseAccessOptions *hao = NULL,
                         const char *hbaseAuths = NULL, const char *encryptionInfo = NULL) = 0;

  virtual int scanClose() = 0;

  int fetchAllRows(HbaseStr &tblName, int numCols, HbaseStr &col1NameStr, HbaseStr &col2NameStr,
                     HbaseStr &col3NameStr,
                     LIST(NAString) & col1ValueList,   // output
                     LIST(NAString) & col2ValueList,   // output
                     LIST(NAString) & col3ValueList);  // output

  // return 1 if table is empty, 0 if not empty. -ve num in case of error
  virtual int isEmpty(HbaseStr &tblName) = 0;

  virtual int getRowOpen(HbaseStr &tblName, const HbaseStr &row, const LIST(HbaseStr) & columns,
                           const int64_t timestamp, int numReplications, const int lockMode, int isolationLevel,
                           const NABoolean useMemoryScan, const NABoolean skipReadConflict = FALSE,
                           HbaseAccessOptions *hao = NULL, const char *hbaseAuths = NULL,
                           const char *encryptionInfo = NULL) = 0;

  virtual int getRowsOpen(HbaseStr &tblName, const LIST(HbaseStr) * rows, const LIST(HbaseStr) & columns,
                            const int64_t timestamp, int numReplications, const int lockMode, int isolationLevel,
                            const NABoolean useMemoryScan, const NABoolean skipReadConflict,
                            const NABoolean skipTransactionForBatchGet, HbaseAccessOptions *hao = NULL,
                            const char *hbaseAuths = NULL, const char *encryptionInfo = NULL) = 0;

  virtual int getRowsOpen(HbaseStr tblName, short rowIDLen, HbaseStr rowIDs, const LIST(HbaseStr) & columns,
                            int numReplications, const int lockMode, int isolationLevel,
                            const NABoolean useMemoryScan, const NABoolean skipReadConflict,
                            const NABoolean skipTransactionForBatchGet, const char *encryptionInfo = NULL) = 0;

  virtual int nextRow() = 0;

  virtual int nextCell(HbaseStr &rowId, HbaseStr &colFamName, HbaseStr &colName, HbaseStr &colVal,
                         long &timestamp) = 0;

  virtual int completeAsyncOperation(int timeout, NABoolean *resultArray, Int16 resultArrayLen) = 0;

  virtual int getColVal(int colNo, BYTE *colVal, int &colValLen, NABoolean nullable, BYTE &nullVal, BYTE *tag,
                          int &tagLen, const char *encryptionInfo = NULL) = 0;

  virtual int getColVal(NAHeap *heap, int colNo, BYTE **colVal, int &colValLen,
                          const char *encryptionInfo = NULL) = 0;

  virtual int getColName(int colNo, char **outColName, short &colNameLen, long &timestamp) = 0;

  virtual int getNumCellsPerRow(int &numCells) = 0;

  virtual int getRowID(HbaseStr &rowID, const char *encryptionInfo = NULL) = 0;

  virtual int deleteRow(HbaseStr tblName, HbaseStr row, const LIST(HbaseStr) * columns, NABoolean useHbaseXn,
                          const NABoolean replSync, const NABoolean incrementalBackup, NABoolean useRegionXn,
                          const int64_t timestamp, NABoolean asyncOperation, const char *hbaseAuths,
                          const char *encryptionInfo = NULL, const char *triggers = NULL,
                          const char *curExecSql = NULL) = 0;

  virtual int deleteRows(HbaseStr tblName, short rowIDLen, HbaseStr rowIDs, const LIST(HbaseStr) * columns,
                           NABoolean useHbaseXn, const NABoolean replSync, const NABoolean incrementalBackup,
                           const int64_t timestamp, NABoolean asyncOperation, const char *hbaseAuths,
                           const char *encryptionInfo = NULL, const char *triggers = NULL,
                           const char *curExecSql = NULL) = 0;

  virtual int checkAndDeleteRow(HbaseStr &tblName, HbaseStr &row, const LIST(HbaseStr) * columns,
                                  HbaseStr &columnToCheck, HbaseStr &colValToCheck, NABoolean useHbaseXn,
                                  const NABoolean replSync, const NABoolean incrementalBackup, NABoolean useRegionXn,
                                  const int64_t timestamp, const char *hbaseAuths, const char *encryptionInfo = NULL,
                                  const char *triggers = NULL, const char *curExecSql = NULL) = 0;

  virtual int deleteColumns(HbaseStr &tblName, HbaseStr &column) = 0;

  virtual int execTriggers(const char *tableName, ComOperation type, NABoolean isBefore,
                             BeforeAndAfterTriggers *triggers, HbaseStr rowID, HbaseStr row,
                             unsigned char *base64rowVal = NULL, int base64ValLen = 0,
                             unsigned char *base64rowIDVal = NULL, int base64RowLen = 0, short rowIDLen = 0,
                             const char *curExecSql = NULL, NABoolean isStatement = true) = 0;

  virtual int insertRow(HbaseStr tblName, HbaseStr rowID, HbaseStr row, NABoolean useHbaseXn,
                          const NABoolean replSync, const NABoolean incrementalBackup, NABoolean useRegionXn,
                          const int64_t timestamp, NABoolean asyncOperation, const char *encryptionInfo = NULL,
                          const char *triggers = NULL, const char *curExecSql = NULL) = 0;

  virtual int insertRows(HbaseStr tblName, short rowIDLen, HbaseStr rowIDs, HbaseStr rows, NABoolean useHbaseXn,
                           const NABoolean replSync, const NABoolean incrementalBackup, const int64_t timestamp,
                           NABoolean asyncOperation, const char *encryptionInfo = NULL, const char *triggers = NULL,
                           const char *curExecSql = NULL, NABoolean noConflictCheck = FALSE) = 0;

  virtual int lockRequired(NAString tblName, short lockMode, NABoolean useHbaseXn, const NABoolean replSync,
                             const NABoolean incrementalBackup, NABoolean asyncOperation, NABoolean noConflictCheck,
                             NABoolean registerRegion) = 0;

  virtual int updateVisibility(HbaseStr tblName, HbaseStr rowID, HbaseStr row, NABoolean useHbaseXn) = 0;

  virtual int setWriteBufferSize(HbaseStr &tblName, int size) = 0;

  virtual int setWriteToWAL(HbaseStr &tblName, NABoolean v) = 0;

  virtual int initHBLC(ExHbaseAccessStats *hbs = NULL) = 0;
  virtual int initBRC(ExHbaseAccessStats *hbs = NULL) = 0;

  virtual int initHFileParams(HbaseStr &tblName, Text &hFileLoc, Text &hfileName, long maxHFileSize,
                                Text &hFileSampleLoc, Text &hfileSampleName, float fSampleRate) = 0;

  virtual int addToHFile(short rowIDLen, HbaseStr &rowIDs, HbaseStr &rows, const char *encryptionInfo) = 0;

  virtual int closeHFile(HbaseStr &tblName) = 0;

  virtual int doBulkLoad(HbaseStr &tblName, Text &location, Text &tableName, NABoolean quasiSecure,
                           NABoolean snapshot) = 0;

  virtual int bulkLoadCleanup(HbaseStr &tblName, Text &location) = 0;

  virtual int sentryGetPrivileges(set<string> &groupNames, const char *tableOrViewName, bool isView,
                                    map<int, char *> &columnNumberToNameMap,
                                    PrivMgrUserPrivs &userPrivs /* out */) = 0;
  virtual int sentryGetPrivileges(const char *userName, const char *tableOrViewName, bool isView,
                                    map<int, char *> &columnNumberToNameMap,
                                    PrivMgrUserPrivs &userPrivs /* out */) = 0;
  virtual int incrCounter(const char *tabName, const char *rowId, const char *famName, const char *qualName,
                            long incr, long &count) = 0;

  virtual int createCounterTable(const char *tabName, const char *famName) = 0;
  virtual int checkAndInsertRow(HbaseStr &tblName, HbaseStr &rowID, HbaseStr &row, NABoolean useHbaseXn,
                                  const NABoolean replSync, const NABoolean incrementalBackup, NABoolean useRegionXn,
                                  const int64_t timestamp, NABoolean asyncOperation, const char *encryptionInfo = NULL,
                                  const char *triggers = NULL, const char *curExecSql = NULL,
                                  Int16 colIndexToCheck = 0) = 0;

  virtual int checkAndUpdateRow(HbaseStr &tblName, HbaseStr &rowID, HbaseStr &row, HbaseStr &columnToCheck,
                                  HbaseStr &colValToCheck, NABoolean useHbaseXn, const NABoolean replSync,
                                  const NABoolean incrementalBackup, NABoolean useRegionXn, const int64_t timestamp,
                                  NABoolean asyncOperation, const char *encryptionInfo = NULL,
                                  const char *triggers = NULL, const char *curExecSql = NULL) = 0;

  virtual int getClose() = 0;

  virtual int coProcAggr(HbaseStr &tblName,
                           int aggrType,  // 0:count, 1:min, 2:max, 3:sum, 4:avg
                           const Text &startRow, const Text &stopRow, const Text &colFamily, const Text &colName,
                           const NABoolean cacheBlocks, const int numCacheRows, const NABoolean replSync,
                           Text &aggrVal);  // returned value

  virtual int coProcAggr(HbaseStr &tblName,
                           int aggrType,  // 0:count, 1:min, 2:max, 3:sum, 4:avg
                           const Text &startRow, const Text &stopRow, const Text &colFamily, const Text &colName,
                           const NABoolean cacheBlocks, const int numCacheRows, const NABoolean replSync,
                           Text &aggrVal,  // returned value
                           int isolationLevel, int lockMode);

  virtual int grant(const Text &user, const Text &tblName, const std::vector<Text> &actionCodes) = 0;

  virtual int revoke(const Text &user, const Text &tblName, const std::vector<Text> &actionCodes) = 0;

  virtual NAArray<HbaseStr> *getRegionBeginKeys(const char *) = 0;
  virtual NAArray<HbaseStr> *getRegionEndKeys(const char *) = 0;

  virtual int estimateRowCount(HbaseStr &tblName, int partialRowSize, int numCols, int retryLimitMilliSeconds,
                                 NABoolean useCoprocessor, long &estRC, int &breadCrumb) = 0;
  virtual int cleanSnpTmpLocation(const char *path) = 0;
  virtual int setArchivePermissions(const char *tbl) = 0;

  virtual int getBlockCacheFraction(float &frac) = 0;
  virtual int getHbaseTableInfo(const HbaseStr &tblName, int &indexLevels, int &blockSize) = 0;

  virtual int getRegionsNodeName(const HbaseStr &tblName, int partns, ARRAY(const char *) & nodeNames) = 0;

  virtual NAArray<HbaseStr> *showTablesHDFSCache(const std::vector<Text> &tables) = 0;

  virtual int addTablesToHDFSCache(const std::vector<Text> &tables, const char *poolName) = 0;
  virtual int removeTablesFromHDFSCache(const std::vector<Text> &tables, const char *poolName) = 0;
  // get regions and size
  virtual NAArray<HbaseStr> *getRegionStats(const HbaseStr &tblName) = 0;

  virtual NAArray<HbaseStr> *getClusterStats(int &numEntries) = 0;

  void settrigger_operation(int v) { trigger_operation_ = v; }
  int gettrigger_operation() { return trigger_operation_; }
  void setUseTrigger(NABoolean v) { useTrigger_ = v; }
  NABoolean getUseTrigger() { return useTrigger_; }

  // Snapshots
  virtual int createSnapshot(const NAString &tableName, const NAString &snapshotName) = 0;
  virtual int restoreSnapshot(const NAString &snapshotName, const NAString &tableName) = 0;
  virtual int deleteSnapshot(const NAString &snapshotName) = 0;
  virtual int verifySnapshot(const NAString &tableName, const NAString &snapshotName, NABoolean &exist) = 0;
  // call dtm to commit or rollback a savepoint.
  // isCommit = TRUE, commit. isCommit = FALSE, rollback.
  virtual int savepointCommitOrRollback(long transId, long savepointId, long tgtSavepointId,
                                          NABoolean isCommit) = 0;

  // process namespace related commands.
  //   create: oper=1. Create namespace 'nameSpace'
  //   drop: oper=2.   Drop namespace 'nameSpace'
  //   getNamespaces: oper=3. Return: retNames contain available namespaces
  //   getNamespaceTables: oper=4. Return: retNames contains names of all
  //                                     objects in namespace 'nameSpace'
  virtual int namespaceOperation(short oper, const char *nameSpace, int numKeyValEntries, NAText *keyArray,
                                   NAText *valArray, NAArray<HbaseStr> **retNames) = 0;

  void setWaitOnSelectForUpdate(NABoolean v) {
    (v ? flags_ |= SELECT_FOR_UPDATE_WAIT : flags_ &= ~SELECT_FOR_UPDATE_WAIT);
  };
  NABoolean waitOnSelectForUpdate() { return (flags_ & SELECT_FOR_UPDATE_WAIT) != 0; };

  void setNoConflictCheckForIndex(NABoolean v) {
    (v ? flags_ |= NO_CONFLICT_CHECK_FOR_IDX : flags_ &= ~NO_CONFLICT_CHECK_FOR_IDX);
  }
  NABoolean noConflictCheckForIndex() { return (flags_ & NO_CONFLICT_CHECK_FOR_IDX) != 0; };

  void setFirstReadBypassTm(NABoolean v) { (v ? flags_ |= FIRST_READ_BYPASS_TM : flags_ &= ~FIRST_READ_BYPASS_TM); }
  NABoolean getFirstReadBypassTm() { return (flags_ & FIRST_READ_BYPASS_TM) != 0; };

  void setPutIsUpsert(NABoolean v) { (v ? flags_ |= EXP_PUT_IS_UPSERT : flags_ &= ~EXP_PUT_IS_UPSERT); }
  NABoolean expPutIsUpsert() { return (flags_ & EXP_PUT_IS_UPSERT) != 0; };

  virtual void setDDLValidator(ExDDLValidator *ddlValidator) = 0;

  virtual short getNextValue(NAString &tabName, NAString &rowId, NAString &famName, NAString &qualName, long incrBy,
                             long &nextValue, NABoolean skipWAL) = 0;

  virtual int deleteSeqRow(NAString &tabName, NAString &rowId) = 0;

  virtual int updateTableDefForBinlog(NAString &tabName, NAString &cols, NAString &keyCols, long ts) = 0;
  virtual int getTableDefForBinlog(NAString &tabName, NAArray<HbaseStr> **retNames) = 0;

  virtual int putData(long eventID, const char *query, int eventType, const char *schemaName, unsigned char *params,
                        long len) = 0;

  virtual NABoolean isReadFromMemoryTable() = 0;
  virtual NABoolean isMemoryTableDisabled() = 0;
  virtual NABoolean ismemDBinitFailed() = 0;

 protected:
  enum { MAX_SERVER_SIZE = 999, MAX_PORT_SIZE = 99, MAX_CONNECT_PARAM_SIZE = 1024 };

  enum {
    SELECT_FOR_UPDATE_WAIT = 0x00001,
    NO_CONFLICT_CHECK_FOR_IDX = 0x00002,
    EXP_PUT_IS_UPSERT = 0x00004,
    FIRST_READ_BYPASS_TM = 0x00008
  };  // for flag_

  ExpHbaseInterface(CollHeap *heap, const char *server = NULL, const char *zkPort = NULL);

  CollHeap *heap_;
  ExHbaseAccessStats *hbs_;
  char connectParam1_[MAX_SERVER_SIZE + 1];
  char connectParam2_[MAX_PORT_SIZE + 1];
  int trigger_operation_;
  NABoolean useTrigger_;
  UInt32 flags_;
};

char *getHbaseErrStr(int errEnum);

// ===========================================================================
class ExpHbaseInterface_JNI : public ExpHbaseInterface {
 public:
  ExpHbaseInterface_JNI(CollHeap *heap, const char *server, bool useTRex, NABoolean replSync, const char *zkPort,
                        ComStorageType storageType = COM_STORAGE_HBASE);

  virtual ~ExpHbaseInterface_JNI();

  virtual int init(ExHbaseAccessStats *hbs = NULL);

  virtual int cleanup();

  virtual int close();

  virtual int create(HbaseStr &tblName, HBASE_NAMELIST &colFamNameList, NABoolean isMVCC);

  virtual int create(HbaseStr &tblName, NAText *hbaseCreateOptionsArray, int numSplits, int keyLength,
                       const char **splitValues, NABoolean useHbaseXn, NABoolean isMVCC, NABoolean incrBackupEnabled);

  virtual int create(HbaseStr &tblName, const NAList<HbaseStr> &cols);

  virtual int create(HbaseStr &tblName, int tableType, const NAList<HbaseStr> &cols,
                       NAText *monarchCreateOptionsArray, int numSplits, int keyLength, const char **splitValues,
                       NABoolean useHbaseXn, NABoolean isMVCC);

  virtual int alter(HbaseStr &tblName, NAText *hbaseCreateOptionsArray, NABoolean useHbaseXn);

  virtual int registerTruncateOnAbort(HbaseStr &tblName, NABoolean useHbaseXn);
  virtual int truncate(HbaseStr &tblName, NABoolean preserveSplits, NABoolean useHbaseXn);
  virtual int drop(HbaseStr &tblName, NABoolean async, NABoolean useHbaseXn);
  virtual int dropAll(const char *pattern, NABoolean async, NABoolean useHbaseXn);

  virtual NAArray<HbaseStr> *listAll(const char *pattern);

  // process namespace related commands.
  //   create: oper=1. Create namespace 'nameSpace'
  //   drop: oper=2.   Drop namespace 'nameSpace'
  //   getNamespaces: oper=3. Return: retNames constains available namespaces
  //   getNamespaceTables: oper=4. Return: retNames contains names of all
  //                                     objects in namespace 'nameSpace'
  virtual int namespaceOperation(short oper, const char *nameSpace, int numKeyValEntries, NAText *keyArray,
                                   NAText *valArray, NAArray<HbaseStr> **retNames);

  // make a copy of srcTblName as tgtTblName
  // if force is true, remove target before copying.
  virtual int copy(HbaseStr &srcTblName, HbaseStr &tgtTblName, NABoolean force = FALSE);

  virtual int backupObjects(const std::vector<Text> &tables, const std::vector<Text> &incrBackupEnabled,
                              const std::vector<Text> &lobLocList, const char *backuptag,
                              const char *extendedAttributes, const char *backupType, const int backupThreads,

                              // 0, update for each table.
                              // -1, dont update
                              // N, update every N secs
                              const int progressUpdateDelay);

  virtual int createSnapshotForIncrBackup(const std::vector<Text> &tables);

  virtual int setHiatus(const NAString &hiatusObjectName, NABoolean lockOperation, NABoolean createSnapIfNotExist,
                          NABoolean ignoreSnapIfNotExist, const int parallelThreads);
  virtual int clearHiatus(const NAString &hiatusObjectName);

  virtual NAArray<HbaseStr> *restoreObjects(const char *backuptag, const std::vector<Text> *schemas,
                                            const std::vector<Text> *tables, const char *restoreToTS,
                                            NABoolean showObjects, NABoolean saveObjects, NABoolean restoreSavedObjects,
                                            int parallelThreads,

                                            // 0, update for each table.
                                            // -1, dont update
                                            // N, update every N secs
                                            const int progressUpdateDelay);

  virtual int finalizeBackup(const char *backuptag, const char *backupType);

  virtual int finalizeRestore(const char *backuptag, const char *backupType);

  virtual int deleteBackup(const char *backuptag, NABoolean timestamp = FALSE, NABoolean cascade = FALSE,
                             NABoolean force = FALSE, NABoolean skipLock = FALSE);

  virtual int exportOrImportBackup(const char *backuptag, NABoolean isExport, NABoolean override,
                                     const char *location, int parallelThreads,

                                     // 0, update for each table.
                                     // -1, dont update
                                     // N, update every N secs
                                     const int progressUpdateDelay);

  virtual int listAllBackups(NAArray<HbaseStr> **backupList, NABoolean shortFormat, NABoolean reverseOrder);

  virtual NAString getBackupType(const char *backuptag);
  virtual NAString getExtendedAttributes(const char *backuptag);
  virtual NAString getBackupStatus(const char *backuptag);

  virtual NAString getPriorBackupTag(const char *restoreToTimestamp);
  virtual NAString getRestoreToTsBackupTag(const char *restoreToTimestamp);

  virtual NAString lockHolder();
  virtual int operationLock(const char *backuptag);
  virtual int operationUnlock(const char *backuptag, NABoolean recoverMeta, NABoolean cleanupLock);

  virtual NAArray<HbaseStr> *getLinkedBackupTags(const char *backuptag);

  // -1, if table exists. 0, if doesn't. -ve num, error.
  virtual int exists(HbaseStr &tblName);

  // returns the next tablename. 100, at EOD.
  virtual int getTable(HbaseStr &tblName);

  virtual int scanOpen(HbaseStr &tblName, const Text &startRow, const Text &stopRow, const LIST(HbaseStr) & columns,
                         const int64_t timestamp, const NABoolean useHbaseXn, const NABoolean useMemoryScan,
                         const int lockMode, int isolationLevel, const NABoolean skipReadConflict,
                         const NABoolean skipTransaction, const NABoolean replSync, const NABoolean cacheBlocks,
                         const NABoolean smallScanner, const int numCacheRows, const NABoolean preFetch,
                         const LIST(NAString) * inColNamesToFilter, const LIST(NAString) * inCompareOpList,
                         const LIST(NAString) * inColValuesToCompare, int numReplications,
                         Float32 DOPparallelScanner = 0.0f, Float32 samplePercent = -1.0f,
                         NABoolean useSnapshotScan = FALSE, int snapTimeout = 0, char *snapName = NULL,
                         char *tmpLoc = NULL, int espNum = 0, HbaseAccessOptions *hao = NULL,
                         const char *hbaseAuthos = NULL, const char *encryptionInfo = NULL);

  virtual int scanClose();

  // return 1 if table is empty, 0 if not empty. -ve num in case of error
  virtual int isEmpty(HbaseStr &tblName);

  virtual int getRowOpen(HbaseStr &tblName, const HbaseStr &row, const LIST(HbaseStr) & columns,
                           const int64_t timestamp, int numReplications, const int lockMode, int isolationLevel,
                           const NABoolean useMemoryScan, const NABoolean skipReadConflict = FALSE,
                           HbaseAccessOptions *hao = NULL, const char *hbaseAuths = NULL,
                           const char *encryptionInfo = NULL);

  virtual int getRowsOpen(HbaseStr &tblName, const LIST(HbaseStr) * rows, const LIST(HbaseStr) & columns,
                            const int64_t timestamp, int numReplications, const int lockMode, int isolationLevel,
                            const NABoolean useMemoryScan, const NABoolean skipReadConflict,
                            const NABoolean skipTransactionForBatchGet, HbaseAccessOptions *hao = NULL,
                            const char *hbaseAuths = NULL, const char *encryptionInfo = NULL);

  virtual int getRowsOpen(HbaseStr tblName, short rowIDLen, HbaseStr rowIDs, const LIST(HbaseStr) & columns,
                            int numReplications, const int lockMode, int isolationLevel,
                            const NABoolean useMemoryScan, const NABoolean skipReadConflict,
                            const NABoolean skipTransactionForBatchGet, const char *encryptionInfo = NULL);

  virtual int nextRow();

  virtual int nextCell(HbaseStr &rowId, HbaseStr &colFamName, HbaseStr &colName, HbaseStr &colVal, long &timestamp);

  virtual int completeAsyncOperation(int timeout, NABoolean *resultArray, Int16 resultArrayLen);

  virtual int getColVal(int colNo, BYTE *colVal, int &colValLen, NABoolean nullable, BYTE &nullVal, BYTE *tag,
                          int &tagLen, const char *encryptionInfo = NULL);

  virtual int getColVal(NAHeap *heap, int colNo, BYTE **colVal, int &colValLen, const char *encryptionInfo = NULL);

  virtual int getColName(int colNo, char **outColName, short &colNameLen, long &timestamp);

  virtual int getNumCellsPerRow(int &numCells);

  virtual int getRowID(HbaseStr &rowID, const char *encryptionInfo = NULL);

  virtual int deleteRow(HbaseStr tblName, HbaseStr row, const LIST(HbaseStr) * columns, NABoolean useHbaseXn,
                          const NABoolean replSync, const NABoolean incrementalBackup, NABoolean useRegionXn,
                          const int64_t timestamp, NABoolean asyncOperation, const char *hbaseAuths,
                          const char *encryptionInfo = NULL, const char *triggers = NULL,
                          const char *curExecSql = NULL);

  virtual int deleteRows(HbaseStr tblName, short rowIDLen, HbaseStr rowIDs, const LIST(HbaseStr) * columns,
                           NABoolean useHbaseXn, const NABoolean replSync, const NABoolean incrementalBackup,
                           const int64_t timestamp, NABoolean asyncOperation, const char *hbaseAuths,
                           const char *encryptionInfo = NULL, const char *triggers = NULL,
                           const char *curExecSql = NULL);

  virtual int checkAndDeleteRow(HbaseStr &tblName, HbaseStr &row, const LIST(HbaseStr) * columns,
                                  HbaseStr &columnToCheck, HbaseStr &colValToCheck, NABoolean useHbaseXn,
                                  const NABoolean replSync, const NABoolean incrementalBackup, NABoolean useRegionXn,
                                  const int64_t timestamp, const char *hbaseAuths, const char *encryptionInfo = NULL,
                                  const char *triggers = NULL, const char *curExecSql = NULL);

  virtual int deleteColumns(HbaseStr &tblName, HbaseStr &column);

  virtual int execTriggers(const char *tableName, ComOperation type, NABoolean isBefore,
                             BeforeAndAfterTriggers *triggers, HbaseStr rowID, HbaseStr row,
                             unsigned char *base64rowVal = NULL, int base64ValLen = 0,
                             unsigned char *base64rowIDVal = NULL, int base64RowLen = 0, short rowIDLen = 0,
                             const char *curExecSql = NULL, NABoolean isStatement = true);

  virtual int insertRow(HbaseStr tblName, HbaseStr rowID, HbaseStr row, NABoolean useHbaseXn,
                          const NABoolean replSync, const NABoolean incrementalBackup, NABoolean useRegionXn,
                          const int64_t timestamp, NABoolean asyncOperation, const char *encryptionInfo = NULL,
                          const char *triggers = NULL, const char *curExecSql = NULL);

  virtual int insertRows(HbaseStr tblName, short rowIDLen, HbaseStr rowIDs, HbaseStr rows, NABoolean useHbaseXn,
                           const NABoolean replSync, const NABoolean incrementalBackup, const int64_t timestamp,
                           NABoolean asyncOperation, const char *encryptionInfo = NULL, const char *triggers = NULL,
                           const char *curExecSql = NULL, NABoolean noConflictCheck = FALSE);

  virtual int lockRequired(NAString tblName, short lockMode, NABoolean useHbaseXn, const NABoolean replSync,
                             const NABoolean incrementalBackup, NABoolean asyncOperation, NABoolean noConflictCheck,
                             NABoolean registerRegion);

  virtual int updateVisibility(HbaseStr tblName, HbaseStr rowID, HbaseStr row, NABoolean useHbaseXn);

  virtual int setWriteBufferSize(HbaseStr &tblName, int size);

  virtual int setWriteToWAL(HbaseStr &tblName, NABoolean v);

  virtual int initHBLC(ExHbaseAccessStats *hbs = NULL);
  virtual int initBRC(ExHbaseAccessStats *hbs = NULL);

  virtual int initHFileParams(HbaseStr &tblName, Text &hFileLoc, Text &hfileName, long maxHFileSize,
                                Text &hFileSampleLoc, Text &hfileSampleName, float fSampleRate);
  virtual int addToHFile(short rowIDLen, HbaseStr &rowIDs, HbaseStr &rows, const char *encryptionInfo);

  virtual int closeHFile(HbaseStr &tblName);

  virtual int doBulkLoad(HbaseStr &tblName, Text &location, Text &tableName, NABoolean quasiSecure,
                           NABoolean snapshot);

  virtual int bulkLoadCleanup(HbaseStr &tblName, Text &location);
  virtual int sentryGetPrivileges(set<string> &groupNames, const char *tableOrViewName, bool isView,
                                    map<int, char *> &columnNumberToNameMap, PrivMgrUserPrivs &userPrivs /* out */);
  virtual int sentryGetPrivileges(const char *userName, const char *tableOrViewName, bool isView,
                                    map<int, char *> &columnNumberToNameMap, PrivMgrUserPrivs &userPrivs /* out */);
  virtual int incrCounter(const char *tabName, const char *rowId, const char *famName, const char *qualName,
                            long incr, long &count);
  virtual int createCounterTable(const char *tabName, const char *famName);

  virtual int checkAndInsertRow(HbaseStr &tblName, HbaseStr &rowID, HbaseStr &row, NABoolean useHbaseXn,
                                  const NABoolean replSync, const NABoolean incrementalBackup, NABoolean useRegionXn,
                                  const int64_t timestamp, NABoolean asyncOperation, const char *encryptionInfo = NULL,
                                  const char *triggers = NULL, const char *curExecSql = NULL,
                                  Int16 colIndexToCheck = 0);

  virtual int checkAndUpdateRow(HbaseStr &tblName, HbaseStr &rowID, HbaseStr &row, HbaseStr &columnToCheck,
                                  HbaseStr &colValToCheck, NABoolean useHbaseXn, const NABoolean replSync,
                                  const NABoolean incrementalBackup, NABoolean useRegionXn, const int64_t timestamp,
                                  NABoolean asyncOperation, const char *encryptionInfo = NULL,
                                  const char *triggers = NULL, const char *curExecSql = NULL);

  virtual int coProcAggr(HbaseStr &tblName,
                           int aggrType,  // 0:count, 1:min, 2:max, 3:sum, 4:avg
                           const Text &startRow, const Text &stopRow, const Text &colFamily, const Text &colName,
                           const NABoolean cacheBlocks, const int numCacheRows, const NABoolean replSync,
                           Text &aggrVal);  // returned value

  virtual int coProcAggr(HbaseStr &tblName,
                           int aggrType,  // 0:count, 1:min, 2:max, 3:sum, 4:avg
                           const Text &startRow, const Text &stopRow, const Text &colFamily, const Text &colName,
                           const NABoolean cacheBlocks, const int numCacheRows, const NABoolean replSync,
                           Text &aggrVal,  // returned value
                           int isolationLevel, int lockMode);

  virtual int getClose();

  virtual int grant(const Text &user, const Text &tblName, const std::vector<Text> &actionCodes);

  virtual int revoke(const Text &user, const Text &tblName, const std::vector<Text> &actionCodes);

  virtual NAArray<HbaseStr> *getRegionBeginKeys(const char *);
  virtual NAArray<HbaseStr> *getRegionEndKeys(const char *);

  virtual int estimateRowCount(HbaseStr &tblName, int partialRowSize, int numCols, int retryLimitMilliSeconds,
                                 NABoolean useCoprocessor, long &estRC, int &breadCrumb);

  virtual int cleanSnpTmpLocation(const char *path);
  virtual int setArchivePermissions(const char *tabName);

  virtual int getBlockCacheFraction(float &frac);
  virtual int getHbaseTableInfo(const HbaseStr &tblName, int &indexLevels, int &blockSize);
  virtual int getRegionsNodeName(const HbaseStr &tblName, int partns, ARRAY(const char *) & nodeNames);

  virtual NAArray<HbaseStr> *showTablesHDFSCache(const std::vector<Text> &tables);

  virtual int addTablesToHDFSCache(const std::vector<Text> &tables, const char *poolName);
  virtual int removeTablesFromHDFSCache(const std::vector<Text> &tables, const char *poolName);
  virtual NAArray<HbaseStr> *getRegionStats(const HbaseStr &tblName);
  virtual NAArray<HbaseStr> *getClusterStats(int &numEntries);

  virtual int createSnapshot(const NAString &tableName, const NAString &snapshotName);
  virtual int restoreSnapshot(const NAString &snapshotName, const NAString &tableName);
  virtual int deleteSnapshot(const NAString &snapshotName);
  virtual int verifySnapshot(const NAString &tableName, const NAString &snapshotName, NABoolean &exist);

  // call dtm to commit or rollback a savepoint.
  // isCommit = TRUE, commit. isCommit = FALSE, rollback.
  virtual int savepointCommitOrRollback(long transId, long savepointId, long tgtSavepointId, NABoolean isCommit);

  virtual void setDDLValidator(ExDDLValidator *ddlValidator);

  virtual short getNextValue(NAString &tabName, NAString &rowId, NAString &famName, NAString &qualName, long incrBy,
                             long &nextValue, NABoolean skipWAL);

  virtual int deleteSeqRow(NAString &tabName, NAString &rowId);

  virtual int getLockErrorNum(int retCode);
  virtual int updateTableDefForBinlog(NAString &tabName, NAString &cols, NAString &keyCols, long ts);
  virtual int getTableDefForBinlog(NAString &tabName, NAArray<HbaseStr> **retNames);

  virtual int putData(long eventID, const char *query, int eventType, const char *schemaName, unsigned char *params,
                        long len);

  // member function for memory table access
  NABoolean isReadFromMemoryTable() { return readFromMemoryTable_; }
  NABoolean isMemoryTableDisabled() { return memoryTableIsDisabled_; }
  NABoolean ismemDBinitFailed() { return memDBinitFailed_; }

 private:
  bool useTRex_;
  NABoolean replSync_;
  HBaseClient_JNI *client_;
  HTableClient_JNI *htc_;
  HBulkLoadClient_JNI *hblc_;
  BackupRestoreClient_JNI *brc_;
  HTableClient_JNI *asyncHtc_;
  int retCode_;
  ComStorageType storageType_;
  NABoolean bigtable_;
  ExDDLValidator *ddlValidator_;
  NABoolean readFromMemoryTable_;
  NABoolean memoryTableIsDisabled_;
  NABoolean memDBinitFailed_;
};

#endif
