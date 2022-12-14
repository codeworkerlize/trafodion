/* -*-C++-*-
****************************************************************************
*
* File:             ExpHbaseInterface.cpp
* Description:  Interface to Hbase world
* Created:        5/26/2013
* Language:      C++
*
*

*
****************************************************************************
*/

#include "exp/ExpHbaseInterface.h"

#include "arkcmp/CmpContext.h"
#include "cli/Context.h"
#include "cli/Globals.h"
#include "cli/SessionDefaults.h"
#include "common/CmpCommon.h"
#include "common/str.h"
#include "executor/ex_ex.h"
#include "executor/ExStats.h"
#include "executor/ex_stdh.h"
#include "export/NAStringDef.h"
#include "runtimestats/SqlStats.h"

// ===========================================================================
// ===== Class ExpHbaseInterface
// ===========================================================================

ExpHbaseInterface::ExpHbaseInterface(CollHeap *heap, const char *connectParam1, const char *connectParam2) {
  heap_ = heap;
  hbs_ = NULL;
  trigger_operation_ = 0;
  flags_ = 0;

  if ((connectParam1) && (strlen(connectParam1) <= MAX_CONNECT_PARAM_SIZE))
    strcpy(connectParam1_, connectParam1);
  else
    connectParam1_[0] = 0;

  if ((connectParam2) && (strlen(connectParam2) <= MAX_CONNECT_PARAM_SIZE))
    strcpy(connectParam2_, connectParam2);
  else
    connectParam2_[0] = 0;

  useTrigger_ = FALSE;
}

ExpHbaseInterface *ExpHbaseInterface::newInstance(CollHeap *heap, const char *connectParam1, const char *connectParam2,
                                                  ComStorageType storageType, NABoolean replSync) {
  return new (heap) ExpHbaseInterface_JNI(heap, connectParam1, TRUE, replSync, connectParam2);
}

NABoolean isParentQueryCanceled() {
  NABoolean isCanceled = FALSE;
  CliGlobals *cliGlobals = GetCliGlobals();
  StatsGlobals *statsGlobals = cliGlobals->getStatsGlobals();
  const char *parentQid = CmpCommon::context()->sqlSession()->getParentQid();
  if (statsGlobals && parentQid) {
    statsGlobals->getStatsSemaphore(cliGlobals->getSemId(), cliGlobals->myPin());
    StmtStats *ss = statsGlobals->getMasterStmtStats(parentQid, strlen(parentQid), RtsQueryId::ANY_QUERY_);
    if (ss) {
      ExMasterStats *masterStats = ss->getMasterStats();
      if (masterStats && masterStats->getCanceledTime() != -1) isCanceled = TRUE;
    }
    statsGlobals->releaseStatsSemaphore(cliGlobals->getSemId(), cliGlobals->myPin());
  }
  return isCanceled;
}

int checkAndWaitSnapshotInProgress(NAHeap *heap) {
  int retcode = 0;
  CliGlobals *cliGlobals = GetCliGlobals();
  StatsGlobals *statsGlobals = cliGlobals->getStatsGlobals();

  if (statsGlobals == NULL) return 0;

  NABoolean done = FALSE;
  int sleptManySecs = 0;
  do {
    if (!statsGlobals->isSnapshotInProgress()) done = TRUE;

    // Sleep and check again
    if (!done) {
      int maxTimeout = cliGlobals->currContext()->getSessionDefaults()->getOnlineBackupTimeout();

      if (sleptManySecs < maxTimeout) {
        // Sleep for 1.00 second
        // useconds_t usec = 1000000;
        // usleep(usec);
        sleep(1);
        sleptManySecs++;
      } else {
        done = TRUE;
        retcode = -1;

        NAString error_msg(heap);
        error_msg =
            "Backup or Restore Operation is in progress. "
            "Please retry once this operation is complete.";
        cliGlobals->setJniErrorStr(error_msg);
      }
    }

  } while (!done);

  return retcode;
}

int ExpHbaseInterface_JNI::deleteColumns(HbaseStr &tblName, HbaseStr &column) {
  int retcode = 0;

  LIST(HbaseStr) columns(heap_);
  columns.insert(column);
  htc_ = client_->getHTableClient((NAHeap *)heap_, tblName.val, useTRex_, FALSE, FALSE, hbs_);
  if (htc_ == NULL) {
    retCode_ = HBC_ERROR_GET_HTC_EXCEPTION;
    return HBASE_OPEN_ERROR;
  }
  long transID = getTransactionIDFromContext();
  long savepointID;
  long pSavepointId;
  int isolationLevel = getTransactionIsolationLevelFromContext();
  savepointID = getSavepointIDFromContext(pSavepointId);

  int numReqRows = 100;
  retcode = htc_->startScan(transID, savepointID, pSavepointId, isolationLevel, "", "", columns, -1, FALSE, FALSE,
                            numReqRows, FALSE, FALSE, HBaseLockMode::LOCK_U, FALSE, FALSE, NULL, NULL, NULL);
  if (retcode != HTC_OK) return retcode;

  NABoolean done = FALSE;
  HbaseStr rowID;
  do {
    // Added the for loop to consider using deleteRows
    // to delete the column for all rows in the batch
    for (int rowNo = 0; rowNo < numReqRows; rowNo++) {
      retcode = htc_->nextRow();
      if (retcode != HTC_OK) {
        done = TRUE;
        break;
      }
      retcode = htc_->getRowID(rowID);
      if (retcode != HBASE_ACCESS_SUCCESS) {
        done = TRUE;
        break;
      }
      retcode = htc_->deleteRow(transID, savepointID, pSavepointId, rowID, &columns, -1, NULL);
      if (retcode != HTC_OK) {
        done = TRUE;
        break;
      }
    }
  } while (!(done || isParentQueryCanceled()));
  scanClose();
  if (retcode == HTC_DONE) return HBASE_ACCESS_SUCCESS;

  int ret = getLockErrorNum(retcode);
  if (ret != HBASE_ACCESS_SUCCESS) return ret;

  return HBASE_ACCESS_ERROR;
}

int ExpHbaseInterface::fetchAllRows(HbaseStr &tblName, int numInCols, HbaseStr &col1NameStr, HbaseStr &col2NameStr,
                                    HbaseStr &col3NameStr,
                                    LIST(NAString) & col1ValueList,  // output
                                    LIST(NAString) & col2ValueList,  // output
                                    LIST(NAString) & col3ValueList)  // output
{
  int retcode;

  retcode = init(hbs_);
  if (retcode != HBASE_ACCESS_SUCCESS) return retcode;

  int colValLen;
  char *colName;
  short colNameLen;
  long timestamp;
  LIST(HbaseStr) columns(heap_);

  switch (numInCols) {
    case 1:
      columns.insert(col1NameStr);  // copy to new element in the list
      col1ValueList.clear();
      break;
    case 2:
      columns.insert(col1NameStr);  // copy to new element in the list
      columns.insert(col2NameStr);  // copy to new element in the list
      col1ValueList.clear();
      col2ValueList.clear();
      break;
    case 3:
      columns.insert(col1NameStr);  // copy to new element in the list
      columns.insert(col2NameStr);  // copy to new element in the list
      columns.insert(col3NameStr);  // copy to new element in the list
      col1ValueList.clear();
      col2ValueList.clear();
      col3ValueList.clear();
      break;
  }

  // This is an internal interface to access HBase table.
  retcode = scanOpen(tblName, "", "", columns, -1, FALSE, FALSE, HBaseLockMode::LOCK_IS, TransMode::READ_UNCOMMITTED_,
                     /* skipReadConflict */ FALSE,
                     /* skipTransaction */ FALSE, FALSE, FALSE, FALSE, 100, TRUE, NULL, NULL, NULL, -1);
  if (retcode != HBASE_ACCESS_SUCCESS) return retcode;
  while (retcode == HBASE_ACCESS_SUCCESS) {
    retcode = nextRow();
    if (retcode != HBASE_ACCESS_SUCCESS) break;
    int numCols;
    retcode = getNumCellsPerRow(numCols);
    if (retcode == HBASE_ACCESS_SUCCESS) {
      for (int colNo = 0; colNo < numCols; colNo++) {
        retcode = getColName(colNo, &colName, colNameLen, timestamp);
        if (retcode != HBASE_ACCESS_SUCCESS) break;
        BYTE *colVal = NULL;
        colValLen = 0;
        retcode = getColVal((NAHeap *)heap_, colNo, &colVal, colValLen);
        if (retcode != HBASE_ACCESS_SUCCESS) break;
        NAString colValue((char *)colVal, colValLen);
        NADELETEBASIC(colVal, heap_);
        if (colNameLen == col1NameStr.len && memcmp(colName, col1NameStr.val, col1NameStr.len) == 0)
          col1ValueList.insert(colValue);
        else if (colNameLen == col2NameStr.len && memcmp(colName, col2NameStr.val, col2NameStr.len) == 0)
          col2ValueList.insert(colValue);
        else if (colNameLen == col3NameStr.len && memcmp(colName, col3NameStr.val, col3NameStr.len) == 0)
          col3ValueList.insert(colValue);
      }
    }
  }  // while
  scanClose();
  if (retcode == HBASE_ACCESS_EOD) retcode = HBASE_ACCESS_SUCCESS;
  return retcode;
}

int ExpHbaseInterface::copy(HbaseStr &srcTblName, HbaseStr &tgtTblName, NABoolean force) { return -HBASE_COPY_ERROR; }

int ExpHbaseInterface::backupObjects(const std::vector<Text> &tables, const std::vector<Text> &incrBackupEnabled,
                                     const std::vector<Text> &lobLocList, const char *backuptag,
                                     const char *extendedAttributes, const char *backupType, const int backupThreads,
                                     const int progressUpdateDelay) {
  return HBASE_CREATE_SNAPSHOT_ERROR;
}

int ExpHbaseInterface::createSnapshotForIncrBackup(const std::vector<Text> &tables) {
  return HBASE_CREATE_SNAPSHOT_ERROR;
}

int ExpHbaseInterface::setHiatus(const NAString &hiatusObjectName, NABoolean lockOperation,
                                 NABoolean createSnapIfNotExist, NABoolean ignoreSnapIfNotExist,
                                 const int parallelThreads) {
  return HBASE_CREATE_SNAPSHOT_ERROR;
}

int ExpHbaseInterface::clearHiatus(const NAString &hiatusObjectName) { return HBASE_CREATE_SNAPSHOT_ERROR; }

NAArray<HbaseStr> *ExpHbaseInterface::restoreObjects(const char *backuptag, const std::vector<Text> *schemas,
                                                     const std::vector<Text> *tables, const char *restoreToTS,
                                                     NABoolean showObjects, NABoolean saveObjects,
                                                     NABoolean restoreSavedObjects, int parallelThreads,
                                                     const int progressUpdateDelay) {
  return NULL;
}

int ExpHbaseInterface::finalizeBackup(const char *backuptag, const char *backupType) {
  return HBASE_CREATE_SNAPSHOT_ERROR;
}

int ExpHbaseInterface::finalizeRestore(const char *backuptag, const char *backupType) {
  return HBASE_RESTORE_SNAPSHOT_ERROR;
}

int ExpHbaseInterface::deleteBackup(const char *backuptag, NABoolean timestamp, NABoolean cascade, NABoolean force,
                                    NABoolean skipLock) {
  return HBASE_DELETE_BACKUP_ERROR;
}

int ExpHbaseInterface::exportOrImportBackup(const char *backuptag, NABoolean isExport, NABoolean override,
                                            const char *location, int parallelThreads, const int progressUpdateDelay) {
  return HBASE_EXPORT_IMPORT_BACKUP_ERROR;
}

int ExpHbaseInterface::listAllBackups(NAArray<HbaseStr> **backupList, NABoolean shortFormat, NABoolean reverseOrder) {
  return HBASE_BACKUP_NONFATAL_ERROR;
}

NAArray<HbaseStr> *ExpHbaseInterface::getLinkedBackupTags(const char *backuptag) { return NULL; }

int ExpHbaseInterface::coProcAggr(HbaseStr &tblName,
                                  int aggrType,  // 0:count, 1:min, 2:max, 3:sum, 4:avg
                                  const Text &startRow, const Text &stopRow, const Text &colFamily, const Text &colName,
                                  const NABoolean cacheBlocks, const int numCacheRows, const NABoolean replSync,
                                  Text &aggrVal)  // returned value
{
  return -HBASE_OPEN_ERROR;
}

int ExpHbaseInterface::coProcAggr(HbaseStr &tblName,
                                  int aggrType,  // 0:count, 1:min, 2:max, 3:sum, 4:avg
                                  const Text &startRow, const Text &stopRow, const Text &colFamily, const Text &colName,
                                  const NABoolean cacheBlocks, const int numCacheRows, const NABoolean replSync,
                                  Text &aggrVal,  // returned value
                                  int isolationLevel, int lockMode) {
  return -HBASE_OPEN_ERROR;
}

char *getHbaseErrStr(int errEnum) {
  int lv_errEnum;
  if (errEnum < HBASE_MIN_ERROR_NUM || errEnum >= HBASE_MAX_ERROR_NUM)
    lv_errEnum = HBASE_GENERIC_ERROR;
  else
    lv_errEnum = errEnum;
  return (char *)hbaseErrorEnumStr[lv_errEnum - (int)HBASE_MIN_ERROR_NUM];
}

// ===========================================================================
// ===== Class ExpHbaseInterface_JNI
// ===========================================================================

ExpHbaseInterface_JNI::ExpHbaseInterface_JNI(CollHeap *heap, const char *connectParam1, bool useTRex,
                                             NABoolean replSync, const char *connectParam2, ComStorageType storageType)
    : ExpHbaseInterface(heap, connectParam1, connectParam2),
      useTRex_(useTRex),
      replSync_(replSync),
      client_(NULL),
      htc_(NULL),
      hblc_(NULL),
      brc_(NULL),
      asyncHtc_(NULL),
      retCode_(HBC_OK),
      storageType_(storageType),
      ddlValidator_(NULL),
      readFromMemoryTable_(false),
      memoryTableIsDisabled_(false),
      memDBinitFailed_(false) {
  bigtable_ = (storageType_ == COM_STORAGE_BIGTABLE);
  //  HBaseClient_JNI::logIt("ExpHbaseInterface_JNI::constructor() called.");
}

//----------------------------------------------------------------------------
ExpHbaseInterface_JNI::~ExpHbaseInterface_JNI() {
  //  HBaseClient_JNI::logIt("ExpHbaseInterface_JNI::destructor() called.");
  if (client_ != NULL) {
    if (htc_ != NULL) client_->releaseHTableClient(htc_);
    htc_ = NULL;
  }
  if (hblc_ != NULL) hblc_ = NULL;

  if (brc_ != NULL) brc_ = NULL;

  client_ = NULL;
}

//----------------------------------------------------------------------------
int ExpHbaseInterface_JNI::init(ExHbaseAccessStats *hbs) {
  // HBaseClient_JNI::logIt("ExpHbaseInterface_JNI::init() called.");
  // Cannot use statement heap - objects persist accross statements.
  switch (storageType_) {
    case COM_STORAGE_MONARCH:

      break;
    default:
      if (client_ == NULL) {
        HBaseClient_JNI::logIt("ExpHbaseInterface_JNI::init() creating new client.");
        client_ = HBaseClient_JNI::getInstance(bigtable_);
        if (client_->isInitialized() == FALSE) {
          HBC_RetCode retCode = client_->init();
          if (retCode != HBC_OK) return -HBASE_ACCESS_ERROR;
        }

        if (client_->isConnected() == FALSE) {
          HBC_RetCode retCode = client_->initConnection(connectParam1_, connectParam2_, bigtable_);
          if (retCode != HBC_OK) return -HBASE_ACCESS_ERROR;
        }
      }
  }
  hbs_ = hbs;  // save for ExpHbaseInterface_JNI member function use
               // and eventually give to HTableClient_JNI

  return HBASE_ACCESS_SUCCESS;
}

//----------------------------------------------------------------------------
int ExpHbaseInterface_JNI::cleanup() {
  //  HBaseClient_JNI::logIt("ExpHbaseInterface_JNI::cleanup() called.");
  switch (storageType_) {
    case COM_STORAGE_MONARCH:

      break;
    default:
      if (client_) {
        // Return client object to the pool for reuse.
        if (htc_) {
          client_->releaseHTableClient(htc_);
          htc_ = NULL;
        }

        if (asyncHtc_) {
          client_->releaseHTableClient(asyncHtc_);
          asyncHtc_ = NULL;
        }
        if (hblc_) {
          client_->releaseHBulkLoadClient(hblc_);
          hblc_ = NULL;
        }
        if (brc_) {
          client_->releaseBackupRestoreClient(brc_);
          brc_ = NULL;
        }
      }
  }
  return HBASE_ACCESS_SUCCESS;
}

//----------------------------------------------------------------------------
int ExpHbaseInterface_JNI::close() {
  //  HBaseClient_JNI::logIt("ExpHbaseInterface_JNI::close() called.");
  switch (storageType_) {
    case COM_STORAGE_MONARCH:
      break;
    default:
      if (client_ == NULL) return HBASE_ACCESS_SUCCESS;
  }
  return cleanup();
}

//----------------------------------------------------------------------------
int ExpHbaseInterface_JNI::create(HbaseStr &tblName, HBASE_NAMELIST &colFamNameList, NABoolean isMVCC) {
  if (client_ == NULL) {
    if (init(hbs_) != HBASE_ACCESS_SUCCESS) return -HBASE_ACCESS_ERROR;
  }

  retCode_ = client_->create(tblName.val, colFamNameList, isMVCC);
  if (retCode_ == HBC_OK)
    return HBASE_ACCESS_SUCCESS;
  else
    return -HBASE_CREATE_ERROR;
}

int ExpHbaseInterface_JNI::create(HbaseStr &tblName, const NAList<HbaseStr> &cols) { return -HBASE_CREATE_ERROR; }

//----------------------------------------------------------------------------
int ExpHbaseInterface_JNI::create(HbaseStr &tblName, NAText *hbaseCreateOptionsArray, int numSplits, int keyLength,
                                  const char **splitValues, NABoolean useHbaseXn, NABoolean isMVCC,
                                  NABoolean incrBackupEnabled) {
  if (client_ == NULL) {
    if (init(hbs_) != HBASE_ACCESS_SUCCESS) return -HBASE_ACCESS_ERROR;
  }

  long transID;
  if (useHbaseXn)
    transID = 0;
  else
    transID = getTransactionIDFromContext();

  retCode_ = client_->create(tblName.val, hbaseCreateOptionsArray, numSplits, keyLength, splitValues, transID, isMVCC,
                             incrBackupEnabled);
  if (retCode_ == HBC_OK)
    return HBASE_ACCESS_SUCCESS;
  else
    return -HBASE_CREATE_ERROR;
}

//----------------------------------------------------------------------------
int ExpHbaseInterface_JNI::create(HbaseStr &tblName, int tableType, const NAList<HbaseStr> &cols,
                                  NAText *monarchCreateOptionsArray, int numSplits, int keyLength,
                                  const char **splitValues, NABoolean useHbaseXn, NABoolean isMVCC) {
  return -HBASE_CREATE_ERROR;
}

//----------------------------------------------------------------------------
int ExpHbaseInterface_JNI::alter(HbaseStr &tblName, NAText *hbaseCreateOptionsArray, NABoolean useHbaseXn) {
  switch (storageType_) {
    case COM_STORAGE_MONARCH:
      return HBASE_NOT_IMPLEMENTED;
      break;
    default:
      if (client_ == NULL) {
        if (init(hbs_) != HBASE_ACCESS_SUCCESS) return -HBASE_ACCESS_ERROR;
      }

      long transID = 0;
      if (!useHbaseXn) transID = getTransactionIDFromContext();

      retCode_ = client_->alter(tblName.val, hbaseCreateOptionsArray, transID);
  }
  if (retCode_ == HBC_OK)
    return HBASE_ACCESS_SUCCESS;
  else
    return -HBASE_ALTER_ERROR;
}

//----------------------------------------------------------------------------
int ExpHbaseInterface_JNI::registerTruncateOnAbort(HbaseStr &tblName, NABoolean useHbaseXn) {
  switch (storageType_) {
    case COM_STORAGE_MONARCH:
      return HBASE_NOT_IMPLEMENTED;
      break;
    default:
      if (client_ == NULL) {
        if (init(hbs_) != HBASE_ACCESS_SUCCESS) return -HBASE_ACCESS_ERROR;
      }

      long transID;
      if (useHbaseXn)
        transID = 0;
      else
        transID = getTransactionIDFromContext();

      retCode_ = client_->registerTruncateOnAbort(tblName.val, transID);
  }
  if (retCode_ == HBC_OK)
    return HBASE_ACCESS_SUCCESS;
  else
    return -HBASE_DROP_ERROR;
}

int ExpHbaseInterface_JNI::truncate(HbaseStr &tblName, NABoolean preserveSplits, NABoolean useHbaseXn) {
  long transID;
  if (useHbaseXn)
    transID = 0;
  else
    transID = getTransactionIDFromContext();

  switch (storageType_) {
    case COM_STORAGE_MONARCH:
      return HBASE_NOT_IMPLEMENTED;
      break;
    default:
      if (client_ == NULL) {
        if (init(hbs_) != HBASE_ACCESS_SUCCESS) return -HBASE_ACCESS_ERROR;
      }

      retCode_ = client_->truncate(tblName.val, preserveSplits, transID);
  }
  if (retCode_ == HBC_OK)
    return HBASE_ACCESS_SUCCESS;
  else
    return -HBASE_DROP_ERROR;
}

//----------------------------------------------------------------------------
int ExpHbaseInterface_JNI::drop(HbaseStr &tblName, NABoolean async, NABoolean useHbaseXn) {
  long transID;
  if (useHbaseXn)
    transID = 0;
  else
    transID = getTransactionIDFromContext();

  switch (storageType_) {
    case COM_STORAGE_MONARCH:

      break;
    default:
      if (client_ == NULL) {
        if (init(hbs_) != HBASE_ACCESS_SUCCESS) return -HBASE_ACCESS_ERROR;
      }

      retCode_ = client_->drop(tblName.val, async, transID);
  }
  if (retCode_ == HBC_OK)
    return HBASE_ACCESS_SUCCESS;
  else
    return -HBASE_DROP_ERROR;
}

//----------------------------------------------------------------------------
int ExpHbaseInterface_JNI::dropAll(const char *pattern, NABoolean async, NABoolean useHbaseXn) {
  switch (storageType_) {
    case COM_STORAGE_MONARCH:
      return HBASE_NOT_IMPLEMENTED;
      break;
    default:
      if (client_ == NULL) {
        if (init(hbs_) != HBASE_ACCESS_SUCCESS) return -HBASE_ACCESS_ERROR;
      }

      long transID;
      if (useHbaseXn)
        transID = 0;
      else
        transID = getTransactionIDFromContext();

      retCode_ = client_->dropAll(pattern, async, transID);
  }
  if (retCode_ == HBC_OK)
    return HBASE_ACCESS_SUCCESS;
  else
    return -HBASE_DROP_ERROR;
}

//----------------------------------------------------------------------------
NAArray<HbaseStr> *ExpHbaseInterface_JNI::listAll(const char *pattern) {
  NAArray<HbaseStr> *listArray;

  switch (storageType_) {
    case COM_STORAGE_MONARCH:

      break;
    default:
      if (client_ == NULL) {
        if (init(hbs_) != HBASE_ACCESS_SUCCESS) return NULL;
      }
      listArray = client_->listAll((NAHeap *)heap_, pattern);
  }
  return listArray;
}

//----------------------------------------------------------------------------
int ExpHbaseInterface_JNI::namespaceOperation(short oper, const char *nameSpace, int numKeyValEntries, NAText *keyArray,
                                              NAText *valArray, NAArray<HbaseStr> **retNames) {
  if (client_ == NULL) {
    if ((retCode_ = init(hbs_)) != HBASE_ACCESS_SUCCESS) return retCode_;
  }

  retCode_ =
      client_->namespaceOperation(oper, nameSpace, numKeyValEntries, keyArray, valArray, (NAHeap *)heap_, retNames);
  if (retCode_ == HBC_OK)
    return HBASE_ACCESS_SUCCESS;
  else
    return -retCode_;
}

//----------------------------------------------------------------------------
int ExpHbaseInterface_JNI::copy(HbaseStr &srcTblName, HbaseStr &tgtTblName, NABoolean force) {
  switch (storageType_) {
    case COM_STORAGE_MONARCH:
      return HBASE_NOT_IMPLEMENTED;
      break;
    default:
      if (client_ == NULL) {
        if (init(hbs_) != HBASE_ACCESS_SUCCESS) return -HBASE_ACCESS_ERROR;
      }

      retCode_ = client_->copy(srcTblName.val, tgtTblName.val, force);
      if (retCode_ == HBC_OK)
        return HBASE_ACCESS_SUCCESS;
      else
        return -HBASE_COPY_ERROR;
  }
}

//-------------------------------------------------------------------------------
int ExpHbaseInterface_JNI::backupObjects(const std::vector<Text> &tables, const std::vector<Text> &incrBackupEnabled,
                                         const std::vector<Text> &lobLocList, const char *backuptag,
                                         const char *extendedAttributes, const char *backupType,
                                         const int backupThreads, const int progressUpdateDelay) {
  if (brc_ == NULL || client_ == NULL) {
    return -HBASE_ACCESS_ERROR;
  }

  retCode_ = brc_->backupObjects(tables, incrBackupEnabled, lobLocList, backuptag, extendedAttributes, backupType,
                                 backupThreads, progressUpdateDelay);

  if (retCode_ == BRC_OK)
    return HBASE_ACCESS_SUCCESS;
  else if (retCode_ == BRC_ERROR_BACKUP_NONFATAL)
    return -HBASE_BACKUP_NONFATAL_ERROR;
  else
    return -HBASE_CREATE_SNAPSHOT_ERROR;
}

int ExpHbaseInterface_JNI::createSnapshotForIncrBackup(const std::vector<Text> &tables) {
  if (brc_ == NULL || client_ == NULL) {
    return -HBASE_ACCESS_ERROR;
  }

  retCode_ = brc_->createSnapshotForIncrBackup(tables);
  if (retCode_ == BRC_OK)
    return HBASE_ACCESS_SUCCESS;
  else
    return -HBASE_CREATE_SNAPSHOT_ERROR;
}

int ExpHbaseInterface_JNI::setHiatus(const NAString &hiatusObjectName, NABoolean lockOperation,
                                     NABoolean createSnapIfNotExist, NABoolean ignoreSnapIfNotExist,
                                     const int parallelThreads) {
  if (brc_ == NULL || client_ == NULL) {
    return -HBASE_ACCESS_ERROR;
  }

  retCode_ =
      brc_->setHiatus(hiatusObjectName, lockOperation, createSnapIfNotExist, ignoreSnapIfNotExist, parallelThreads);
  if (retCode_ == BRC_OK)
    return HBASE_ACCESS_SUCCESS;
  else
    return -HBASE_CREATE_SNAPSHOT_ERROR;
}

int ExpHbaseInterface_JNI::clearHiatus(const NAString &hiatusObjectName) {
  if (brc_ == NULL || client_ == NULL) {
    return -HBASE_ACCESS_ERROR;
  }

  retCode_ = brc_->clearHiatus(hiatusObjectName);
  if (retCode_ == BRC_OK)
    return HBASE_ACCESS_SUCCESS;
  else
    return -HBASE_CREATE_SNAPSHOT_ERROR;
}

//-------------------------------------------------------------------------------
NAArray<HbaseStr> *ExpHbaseInterface_JNI::restoreObjects(const char *backuptag, const std::vector<Text> *schemas,
                                                         const std::vector<Text> *tables, const char *restoreToTS,
                                                         NABoolean showObjects, NABoolean saveObjects,
                                                         NABoolean restoreSavedObjects, int parallelThreads,
                                                         const int progressUpdateDelay) {
  if (brc_ == NULL || client_ == NULL) {
    return NULL;
  }

  NAArray<HbaseStr> *listArray =
      brc_->restoreObjects(backuptag, schemas, tables, restoreToTS, showObjects, saveObjects, restoreSavedObjects,
                           parallelThreads, progressUpdateDelay, (NAHeap *)heap_);

  return listArray;
}

int ExpHbaseInterface_JNI::finalizeBackup(const char *backuptag, const char *backupType) {
  if (brc_ == NULL || client_ == NULL) {
    return -HBASE_ACCESS_ERROR;
  }

  retCode_ = brc_->finalizeBackup(backuptag, backupType);

  if (retCode_ == BRC_OK)
    return HBASE_ACCESS_SUCCESS;
  else
    return -HBASE_CREATE_SNAPSHOT_ERROR;
}

int ExpHbaseInterface_JNI::finalizeRestore(const char *backuptag, const char *backupType) {
  if (brc_ == NULL || client_ == NULL) {
    return -HBASE_ACCESS_ERROR;
  }

  retCode_ = brc_->finalizeRestore(backuptag, backupType);

  if (retCode_ == BRC_OK)
    return HBASE_ACCESS_SUCCESS;
  else
    return -HBASE_RESTORE_SNAPSHOT_ERROR;
}

int ExpHbaseInterface_JNI::deleteBackup(const char *backuptag, NABoolean timestamp, NABoolean cascade, NABoolean force,
                                        NABoolean skipLock) {
  if (brc_ == NULL || client_ == NULL) {
    return -HBASE_ACCESS_ERROR;
  }

  retCode_ = brc_->deleteBackup(backuptag, timestamp, cascade, force, skipLock);

  if (retCode_ == BRC_OK)
    return HBASE_ACCESS_SUCCESS;
  else
    return -HBASE_DELETE_BACKUP_ERROR;
}

int ExpHbaseInterface_JNI::exportOrImportBackup(const char *backuptag, NABoolean isExport, NABoolean override,
                                                const char *location, int parallelThreads,
                                                const int progressUpdateDelay) {
  if (brc_ == NULL || client_ == NULL) {
    return -HBASE_ACCESS_ERROR;
  }

  retCode_ = brc_->exportOrImportBackup(backuptag, isExport, override, location, parallelThreads, progressUpdateDelay);

  if (retCode_ == BRC_OK)
    return HBASE_ACCESS_SUCCESS;
  else
    return -HBASE_EXPORT_IMPORT_BACKUP_ERROR;
}

int ExpHbaseInterface_JNI::listAllBackups(NAArray<HbaseStr> **backupList, NABoolean shortFormat,
                                          NABoolean reverseOrder) {
  if (brc_ == NULL || client_ == NULL) {
    return -HBASE_ACCESS_ERROR;
  }

  retCode_ = brc_->listAllBackups(backupList, shortFormat, reverseOrder, (NAHeap *)heap_);
  if (retCode_ == BRC_OK)
    return HBASE_ACCESS_SUCCESS;
  else
    return -HBASE_GET_BACKUP_ERROR;
}

NAString ExpHbaseInterface_JNI::getBackupType(const char *backuptag) {
  if (brc_ == NULL || client_ == NULL) {
    return NAString("");
  }

  return brc_->getBackupType(backuptag);
}

NAString ExpHbaseInterface_JNI::getExtendedAttributes(const char *backuptag) {
  if (brc_ == NULL || client_ == NULL) {
    return NAString("");
  }

  return brc_->getExtendedAttributes(backuptag);
}

NAString ExpHbaseInterface_JNI::getBackupStatus(const char *backuptag) {
  if (brc_ == NULL || client_ == NULL) {
    return NAString("");
  }

  return brc_->getBackupStatus(backuptag);
}

NAString ExpHbaseInterface_JNI::getPriorBackupTag(const char *restoreToTimestamp) {
  if (brc_ == NULL || client_ == NULL) {
    return NAString("");
  }

  return brc_->getPriorBackupTag(restoreToTimestamp);
}

NAString ExpHbaseInterface_JNI::getRestoreToTsBackupTag(const char *restoreToTimestamp) {
  if (brc_ == NULL || client_ == NULL) {
    return NAString("");
  }

  return brc_->getRestoreToTsBackupTag(restoreToTimestamp);
}

NAString ExpHbaseInterface_JNI::lockHolder() {
  if (brc_ == NULL || client_ == NULL) {
    return NAString("?");
  }

  return brc_->lockHolder();
}

int ExpHbaseInterface_JNI::operationLock(const char *backuptag) {
  if (brc_ == NULL || client_ == NULL) {
    return -HBASE_ACCESS_ERROR;
  }

  return brc_->operationLock(backuptag);
}

int ExpHbaseInterface_JNI::operationUnlock(const char *backuptag, NABoolean recoverMeta, NABoolean cleanupLock) {
  if (brc_ == NULL || client_ == NULL) {
    return -HBASE_ACCESS_ERROR;
  }

  return brc_->operationUnlock(backuptag, recoverMeta, cleanupLock);
}

NAArray<HbaseStr> *ExpHbaseInterface_JNI::getLinkedBackupTags(const char *backuptag) {
  if (brc_ == NULL || client_ == NULL) {
    return NULL;
  }

  NAArray<HbaseStr> *backupList = brc_->getLinkedBackupTags(backuptag, (NAHeap *)heap_);
  return backupList;
}

//----------------------------------------------------------------------------
int ExpHbaseInterface_JNI::exists(HbaseStr &tblName) {
  switch (storageType_) {
    case COM_STORAGE_MONARCH:

      break;
    default:
      if (client_ == NULL) {
        if (init(hbs_) != HBASE_ACCESS_SUCCESS) return -HBASE_ACCESS_ERROR;
      }

      long transID;
      transID = getTransactionIDFromContext();

      retCode_ = client_->exists(tblName.val, transID);
  }
  if (retCode_ == HBC_OK || retCode_ == MC_OK)
    return -1;  // Found.
  else if (retCode_ == HBC_DONE || retCode_ == MC_DONE)
    return 0;  // Not found
  else
    return -HBASE_ACCESS_ERROR;
}

//----------------------------------------------------------------------------
// returns the next tablename. 100, at EOD.
int ExpHbaseInterface_JNI::getTable(HbaseStr &tblName) { return -HBASE_ACCESS_ERROR; }

//----------------------------------------------------------------------------
int ExpHbaseInterface_JNI::scanOpen(HbaseStr &tblName, const Text &startRow, const Text &stopRow,
                                    const LIST(HbaseStr) & columns, const int64_t timestamp, const NABoolean useHbaseXn,
                                    const NABoolean useMemoryScan, const int lockMode, int isolationLevel,
                                    const NABoolean skipReadConflict, const NABoolean skipTransaction,
                                    const NABoolean replSync, const NABoolean cacheBlocks, const NABoolean smallScanner,
                                    const int numCacheRows, const NABoolean preFetch,
                                    const LIST(NAString) * inColNamesToFilter, const LIST(NAString) * inCompareOpList,
                                    const LIST(NAString) * inColValuesToCompare, int numReplications,
                                    Float32 dopParallelScanner, Float32 samplePercent, NABoolean useSnapshotScan,
                                    int snapTimeout, char *snapName, char *tmpLoc, int espNum, HbaseAccessOptions *hao,
                                    const char *hbaseAuths, const char *encryptionInfo) {
  long transID;

  if (useHbaseXn)
    transID = 0;
  else
    transID = getTransactionIDFromContext();
  switch (storageType_) {
    case COM_STORAGE_MONARCH:

      break;
    default:

      htc_ = client_->getHTableClient((NAHeap *)heap_, tblName.val, useTRex_, replSync, FALSE, hbs_);
      if (htc_ == NULL) {
        retCode_ = HBC_ERROR_GET_HTC_EXCEPTION;
        return HBASE_OPEN_ERROR;
      }
      htc_->setUseTrigger(getUseTrigger());
      // if this scan is running under a transaction, pass that
      // transid even if useHbaseXn is set. This will ensure that selected
      // rows are returned from the transaction cache instead of underlying
      // storage engine.
      long transID;
      transID = getTransactionIDFromContext();
      long savepointID;
      long pSavepointId;
      savepointID = getSavepointIDFromContext(pSavepointId);
      if (isolationLevel == TransMode::IL_NOT_SPECIFIED_) {
        isolationLevel = getTransactionIsolationLevelFromContext();
      }
      if (skipTransaction) {
        transID = 0;
        savepointID = -1;
        pSavepointId = -1;
      }
      retCode_ =
          htc_->startScan(transID, savepointID, pSavepointId, isolationLevel, startRow, stopRow, columns, timestamp,
                          cacheBlocks, smallScanner, numCacheRows, preFetch, lockMode, skipReadConflict,
                          skipTransaction, inColNamesToFilter, inCompareOpList, inColValuesToCompare, numReplications,
                          dopParallelScanner, samplePercent, useSnapshotScan, snapTimeout, snapName, tmpLoc, espNum,
                          (hao && hao->multiVersions()) ? hao->getNumVersions() : 0, hao ? hao->hbaseMinTS() : -1,
                          hao ? hao->hbaseMaxTS() : -1, hbaseAuths, encryptionInfo, FALSE, getFirstReadBypassTm());
  }

  if (retCode_ == HBC_OK) return HBASE_ACCESS_SUCCESS;

  int ret = getLockErrorNum(retCode_);
  if (ret != HBASE_ACCESS_SUCCESS) return ret;

  return -HBASE_OPEN_ERROR;
}

//----------------------------------------------------------------------------
int ExpHbaseInterface_JNI::scanClose() {
  switch (storageType_) {
    case COM_STORAGE_MONARCH:
      return HBASE_NOT_IMPLEMENTED;
      break;
    default:
      if (htc_) {
        client_->releaseHTableClient(htc_);
        htc_ = NULL;
      }
  }
  return HBASE_ACCESS_SUCCESS;
}

//----------------------------------------------------------------------------
int ExpHbaseInterface_JNI::getRowOpen(HbaseStr &tblName, const HbaseStr &row, const LIST(HbaseStr) & columns,
                                      const int64_t timestamp, int numReplications, const int lockMode,
                                      int isolationLevel, const NABoolean useMemoryScan,
                                      const NABoolean skipReadConflict, HbaseAccessOptions *hao, const char *hbaseAuths,
                                      const char *encryptionInfo) {
  long transID = getTransactionIDFromContext();
  long savepointID;
  long pSavepointId;
  savepointID = getSavepointIDFromContext(pSavepointId);

  if (isolationLevel == TransMode::IL_NOT_SPECIFIED_) {
    isolationLevel = getTransactionIsolationLevelFromContext();
  }
  switch (storageType_) {
    case COM_STORAGE_MONARCH:

      break;
    default:

      if (ddlValidator_ && !ddlValidator_->isDDLValidForReads()) {
        htc_ = NULL;
        return HBASE_INVALID_DDL;
      }

      htc_ = new ((NAHeap *)heap_) HTableClient_JNI((NAHeap *)heap_, (jobject)-1);
      htc_->setUseTrigger(getUseTrigger());

      retCode_ = client_->startGet((NAHeap *)heap_, (char *)tblName.val, useTRex_, FALSE, lockMode, skipReadConflict,
                                   hbs_, transID, savepointID, pSavepointId, isolationLevel, row, columns, timestamp,
                                   numReplications, htc_, hbaseAuths, encryptionInfo, waitOnSelectForUpdate(),
                                   getFirstReadBypassTm());
      if (retCode_ == HBC_ERROR_GET_HTC_EXCEPTION) {
        htc_ = NULL;
        return HBASE_OPEN_ERROR;
      }

      int ret = getLockErrorNum(retCode_);
      if (retCode_ != HBASE_ACCESS_SUCCESS) {
        htc_ = NULL;
        return ret;
      }
  }
  return HBASE_ACCESS_SUCCESS;
}

//----------------------------------------------------------------------------
int ExpHbaseInterface_JNI::getRowsOpen(HbaseStr &tblName, const LIST(HbaseStr) * rows, const LIST(HbaseStr) & columns,
                                       const int64_t timestamp, int numReplications, const int lockMode,
                                       int isolationLevel, const NABoolean useMemoryScan,
                                       const NABoolean skipReadConflict, const NABoolean skipTransactionForBatchGet,
                                       HbaseAccessOptions *hao, const char *hbaseAuths, const char *encryptionInfo) {
  long transID = getTransactionIDFromContext();
  long savepointID;
  long pSavepointId;
  savepointID = getSavepointIDFromContext(pSavepointId);
  if (isolationLevel == TransMode::ACCESS_TYPE_NOT_SPECIFIED_) {
    isolationLevel = getTransactionIsolationLevelFromContext();
  }
  switch (storageType_) {
    case COM_STORAGE_MONARCH:

      break;
    default:

      if (ddlValidator_ && !ddlValidator_->isDDLValidForReads()) {
        htc_ = NULL;
        return HBASE_INVALID_DDL;
      }

      htc_ = new ((NAHeap *)heap_) HTableClient_JNI((NAHeap *)heap_, (jobject)-1);
      htc_->setUseTrigger(getUseTrigger());
      retCode_ =
          client_->startGets((NAHeap *)heap_, (char *)tblName.val, useTRex_, FALSE, lockMode, skipReadConflict,
                             skipTransactionForBatchGet, hbs_, transID, savepointID, pSavepointId, isolationLevel, rows,
                             0, NULL, columns, timestamp, numReplications, htc_, hbaseAuths, encryptionInfo);

      if (retCode_ == HBC_ERROR_GET_HTC_EXCEPTION) {
        htc_ = NULL;
        return HBASE_OPEN_ERROR;
      }

      int ret = getLockErrorNum(retCode_);
      if (ret != HBASE_ACCESS_SUCCESS) {
        htc_ = NULL;
        return ret;
      }
  }
  return HBASE_ACCESS_SUCCESS;
}

int ExpHbaseInterface_JNI::deleteRow(HbaseStr tblName, HbaseStr row, const LIST(HbaseStr) * columns,
                                     NABoolean useHbaseXn, const NABoolean replSync, const NABoolean incrementalBackup,
                                     NABoolean useRegionXn, const int64_t timestamp, NABoolean asyncOperation,
                                     const char *hbaseAuths, const char *encryptionInfo, const char *triggers,
                                     const char *curExecSql) {
  long transID;
  long savepointID = -1;
  long pSavepointId = -1;

  if (useHbaseXn)
    transID = 0;
  else {
    transID = getTransactionIDFromContext();
    savepointID = getSavepointIDFromContext(pSavepointId);
  }

  switch (storageType_) {
    case COM_STORAGE_MONARCH:

      break;
    default:
      HTableClient_JNI *htc;
      if (!transID) {
        if (checkAndWaitSnapshotInProgress((NAHeap *)heap_)) return -HBASE_BACKUP_LOCK_TIMEOUT_ERROR;
      }
      UInt32 flags = 0;
      htc->setFlags(flags, useTRex_, replSync, useRegionXn, incrementalBackup, asyncOperation,
                    noConflictCheckForIndex(), FALSE);
      client_->settrigger_operation(gettrigger_operation());
      if ((triggers != NULL || getUseTrigger()) && gettrigger_operation() == COM_DELETE && htc_ != NULL) {
        // if delete each row, we should get need row old value
        htc_->setOldValueInfo(row);

        client_->setOldValueInfo(htc_->skvBuffer_, htc_->sBufSize_, htc_->srowIDs_, htc_->srowIDsLen_);
      }
      client_->setNoConflictCheckForIndex(noConflictCheckForIndex());
      retCode_ =
          client_->deleteRow((NAHeap *)heap_, tblName.val, hbs_, transID, savepointID, pSavepointId, row, columns,
                             timestamp, hbaseAuths, flags, encryptionInfo, triggers, curExecSql, &htc, ddlValidator_);

      if (retCode_ == HBC_ERROR_DELETEROW_INVALID_DDL) return HBASE_INVALID_DDL;

      if (triggers != NULL && gettrigger_operation() == COM_DELETE && htc_ != NULL)  // delete operation
      {
        client_->cleanupOldValueInfo();
      }

      client_->settrigger_operation(COM_UNKNOWN_IUD);

      int ret = getLockErrorNum(retCode_);
      if (ret != HBASE_ACCESS_SUCCESS) {
        return ret;
      } else if (retCode_ == HBC_ERROR_TRIGGER_EXECUTE_EXCEPTION) {
        return TRIGGER_EXECUTE_EXCEPTION;
      } else if (retCode_ == HBC_ERROR_TRIGGER_PARAMETER_EXCEPTION) {
        return TRIGGER_PARAMETER_EXCEPTION;
      } else if (retCode_ != HBC_OK) {
        return -HBASE_ACCESS_ERROR;
      } else {
        if (asyncOperation) asyncHtc_ = htc;
      }
  }
  return HBASE_ACCESS_SUCCESS;
}
//
//----------------------------------------------------------------------------
int ExpHbaseInterface_JNI::deleteRows(HbaseStr tblName, short rowIDLen, HbaseStr rowIDs, const LIST(HbaseStr) * columns,
                                      NABoolean useHbaseXn, const NABoolean replSync, const NABoolean incrementalBackup,
                                      const int64_t timestamp, NABoolean asyncOperation, const char *hbaseAuths,
                                      const char *encryptionInfo, const char *triggers, const char *curExecSql) {
  long transID;
  long savepointID = -1;
  long pSavepointId = -1;

  if (useHbaseXn)
    transID = 0;
  else {
    transID = getTransactionIDFromContext();
    savepointID = getSavepointIDFromContext(pSavepointId);
  }

  switch (storageType_) {
    case COM_STORAGE_MONARCH:

      break;
    default:
      HTableClient_JNI *htc;
      if (!transID) {
        if (checkAndWaitSnapshotInProgress((NAHeap *)heap_)) return -HBASE_BACKUP_LOCK_TIMEOUT_ERROR;
      }
      UInt32 flags = 0;
      htc->setFlags(flags, useTRex_, replSync, 0, incrementalBackup, asyncOperation, noConflictCheckForIndex(), FALSE);
      client_->settrigger_operation(gettrigger_operation());
      if (triggers != NULL && gettrigger_operation() == COM_DELETE && htc_ != NULL) {
        client_->setOldValueInfo(htc_->skvBuffer_, htc_->sBufSize_, htc_->srowIDs_, htc_->srowIDsLen_);
      }

      client_->setNoConflictCheckForIndex(noConflictCheckForIndex());
      retCode_ = client_->deleteRows((NAHeap *)heap_, tblName.val, hbs_, transID, savepointID, pSavepointId, rowIDLen,
                                     rowIDs, columns, timestamp, hbaseAuths, flags, encryptionInfo, triggers,
                                     curExecSql, &htc, ddlValidator_);

      if (retCode_ == HBC_ERROR_DELETEROWS_INVALID_DDL) return HBASE_INVALID_DDL;

      if (triggers != NULL && gettrigger_operation() == COM_DELETE && htc_ != NULL)  // update operation
      {
        client_->cleanupOldValueInfo();
      }

      client_->settrigger_operation(COM_UNKNOWN_IUD);

      int ret = getLockErrorNum(retCode_);
      if (ret != HBASE_ACCESS_SUCCESS)
        return ret;
      else if (retCode_ == HBC_ERROR_TRIGGER_EXECUTE_EXCEPTION) {
        return TRIGGER_EXECUTE_EXCEPTION;
      } else if (retCode_ == HBC_ERROR_TRIGGER_PARAMETER_EXCEPTION) {
        return TRIGGER_PARAMETER_EXCEPTION;
      } else if (retCode_ != HBC_OK) {
        return -HBASE_ACCESS_ERROR;
      } else {
        if (asyncOperation) asyncHtc_ = htc;
      }
  }
  return HBASE_ACCESS_SUCCESS;
}

//----------------------------------------------------------------------------
int ExpHbaseInterface_JNI::checkAndDeleteRow(HbaseStr &tblName, HbaseStr &rowID, const LIST(HbaseStr) * columns,
                                             HbaseStr &columnToCheck, HbaseStr &columnValToCheck, NABoolean useHbaseXn,
                                             const NABoolean replSync, const NABoolean incrementalBackup,
                                             NABoolean useRegionXn, const int64_t timestamp, const char *hbaseAuths,
                                             const char *encryptionInfo, const char *triggers, const char *curExecSql) {
  bool asyncOperation = false;
  long transID;
  long savepointID = -1;
  long pSavepointId = -1;

  if (useHbaseXn)
    transID = 0;
  else {
    transID = getTransactionIDFromContext();
    savepointID = getSavepointIDFromContext(pSavepointId);
  }

  switch (storageType_) {
    case COM_STORAGE_MONARCH:

      break;
    default:
      HTableClient_JNI *htc;
      UInt32 flags = 0;
      htc->setFlags(flags, useTRex_, replSync, useRegionXn, incrementalBackup, asyncOperation, 0, 0);
      retCode_ = client_->checkAndDeleteRow((NAHeap *)heap_, tblName.val, hbs_, transID, savepointID, pSavepointId,
                                            rowID, columns, columnToCheck, columnValToCheck, timestamp, hbaseAuths,
                                            flags, encryptionInfo, triggers, curExecSql, &htc, ddlValidator_);

      if (retCode_ == HBC_ERROR_CHECKANDDELETEROW_INVALID_DDL) return HBASE_INVALID_DDL;

      int ret = getLockErrorNum(retCode_);
      if (ret != HBASE_ACCESS_SUCCESS)
        return ret;
      else if (retCode_ == HBC_ERROR_TRIGGER_EXECUTE_EXCEPTION) {
        return TRIGGER_EXECUTE_EXCEPTION;
      } else if (retCode_ == HBC_ERROR_TRIGGER_PARAMETER_EXCEPTION) {
        return TRIGGER_PARAMETER_EXCEPTION;
      } else if (retCode_ == HBC_ERROR_CHECKANDDELETEROW_NOTFOUND) {
        return HBASE_ROW_NOTFOUND_ERROR;
      } else if (retCode_ != HBC_OK) {
        return -HBASE_ACCESS_ERROR;
      } else {
        if (asyncOperation) asyncHtc_ = htc;
      }
  }
  return HBASE_ACCESS_SUCCESS;
}

int ExpHbaseInterface_JNI::execTriggers(const char *tableName, ComOperation type, NABoolean isBefore,
                                        BeforeAndAfterTriggers *triggers, HbaseStr rowID, HbaseStr row,
                                        unsigned char *base64rowVal, int base64ValLen, unsigned char *base64rowIDVal,
                                        int base64RowLen, short rowIDLen, const char *curExecSql,
                                        NABoolean isStatement) {
  retCode_ = client_->execTriggers((NAHeap *)heap_, tableName, type, isBefore, triggers, rowID, row, base64rowVal,
                                   base64ValLen, base64rowIDVal, base64RowLen, rowIDLen, curExecSql, isStatement);
  if (retCode_ == HBC_ERROR_TRIGGER_EXECUTE_EXCEPTION) {
    return TRIGGER_EXECUTE_EXCEPTION;
  } else if (retCode_ == HBC_ERROR_TRIGGER_PARAMETER_EXCEPTION) {
    return TRIGGER_PARAMETER_EXCEPTION;
  }

  return HBASE_ACCESS_SUCCESS;
}

//
//----------------------------------------------------------------------------
int ExpHbaseInterface_JNI::insertRow(HbaseStr tblName, HbaseStr rowID, HbaseStr row, NABoolean useHbaseXn,
                                     const NABoolean replSync, const NABoolean incrementalBackup, NABoolean useRegionXn,
                                     const int64_t timestamp, NABoolean asyncOperation, const char *encryptionInfo,
                                     const char *triggers, const char *curExecSql) {
  long transID;
  long savepointID = -1;
  long pSavepointId = -1;

  NABoolean checkAndPut = FALSE;

  if (useHbaseXn)
    transID = 0;
  else {
    transID = getTransactionIDFromContext();
    savepointID = getSavepointIDFromContext(pSavepointId);
  }

  switch (storageType_) {
    case COM_STORAGE_MONARCH:

      break;
    default:
      HTableClient_JNI *htc;
      if (!transID) {
        if (checkAndWaitSnapshotInProgress((NAHeap *)heap_)) return -HBASE_BACKUP_LOCK_TIMEOUT_ERROR;
      }
      UInt32 flags = 0;
      htc->setFlags(flags, useTRex_, replSync, useRegionXn, incrementalBackup, asyncOperation,
                    noConflictCheckForIndex(), expPutIsUpsert());
      client_->settrigger_operation(gettrigger_operation());
      if (triggers != NULL && gettrigger_operation() == COM_UPDATE && htc_ != NULL)  // update operation
      {
        htc_->setOldValueInfo(rowID);

        client_->setOldValueInfo(htc_->skvBuffer_, htc_->sBufSize_, htc_->srowIDs_, htc_->srowIDsLen_);
      }

      client_->setNoConflictCheckForIndex(noConflictCheckForIndex());
      retCode_ = client_->insertRow((NAHeap *)heap_, tblName.val, hbs_, transID, savepointID, pSavepointId, rowID, row,
                                    timestamp, checkAndPut, flags, encryptionInfo, triggers, curExecSql,
                                    0,  // checkAndPut is false, so colIndexToCheck is not used
                                    &htc, ddlValidator_);

      if (retCode_ == HBC_ERROR_INSERTROW_INVALID_DDL) return HBASE_INVALID_DDL;

      if (triggers != NULL && gettrigger_operation() == COM_UPDATE && htc_ != NULL)  // update operation
      {
        client_->cleanupOldValueInfo();
      }

      client_->settrigger_operation(COM_UNKNOWN_IUD);

      int ret = getLockErrorNum(retCode_);
      if (ret != HBASE_ACCESS_SUCCESS)
        return ret;
      else if (retCode_ == HBC_ERROR_TRIGGER_EXECUTE_EXCEPTION) {
        return TRIGGER_EXECUTE_EXCEPTION;
      } else if (retCode_ == HBC_ERROR_TRIGGER_PARAMETER_EXCEPTION) {
        return TRIGGER_PARAMETER_EXCEPTION;
      } else if (retCode_ != HBC_OK) {
        return -HBASE_ACCESS_ERROR;
      } else {
        if (asyncOperation) asyncHtc_ = htc;
      }
  }
  return HBASE_ACCESS_SUCCESS;
}

//----------------------------------------------------------------------------
int ExpHbaseInterface_JNI::insertRows(HbaseStr tblName, short rowIDLen, HbaseStr rowIDs, HbaseStr rows,
                                      NABoolean useHbaseXn, const NABoolean replSync, const NABoolean incrementalBackup,
                                      const int64_t timestamp, NABoolean asyncOperation, const char *encryptionInfo,
                                      const char *triggers, const char *curExecSql, NABoolean noConflictCheck) {
  long transID;
  long savepointID = -1;
  long pSavepointId = -1;

  if (useHbaseXn)
    transID = 0;
  else {
    transID = getTransactionIDFromContext();
    savepointID = getSavepointIDFromContext(pSavepointId);
  }

  switch (storageType_) {
    case COM_STORAGE_MONARCH:

      break;
    default:
      HTableClient_JNI *htc;
      if (!transID) {
        if (checkAndWaitSnapshotInProgress((NAHeap *)heap_)) return -HBASE_BACKUP_LOCK_TIMEOUT_ERROR;
      }
      UInt32 flags = 0;
      htc->setFlags(flags, useTRex_, replSync, 0, incrementalBackup, asyncOperation, noConflictCheck, expPutIsUpsert());
      client_->setNoConflictCheckForIndex(noConflictCheckForIndex());
      retCode_ =
          client_->insertRows((NAHeap *)heap_, tblName.val, hbs_, transID, savepointID, pSavepointId, rowIDLen, rowIDs,
                              rows, timestamp, flags, encryptionInfo, triggers, curExecSql, &htc, ddlValidator_);

      if (retCode_ == HBC_ERROR_INSERTROWS_INVALID_DDL) return HBASE_INVALID_DDL;

      int ret = getLockErrorNum(retCode_);
      if (ret != HBASE_ACCESS_SUCCESS)
        return ret;
      else if (retCode_ == HBC_ERROR_TRIGGER_EXECUTE_EXCEPTION) {
        return TRIGGER_EXECUTE_EXCEPTION;
      } else if (retCode_ == HBC_ERROR_TRIGGER_PARAMETER_EXCEPTION) {
        return TRIGGER_PARAMETER_EXCEPTION;
      } else if (retCode_ != HBC_OK) {
        return -HBASE_ACCESS_ERROR;
      } else {
        if (asyncOperation) asyncHtc_ = htc;
      }
  }
  return HBASE_ACCESS_SUCCESS;
}

//----------------------------------------------------------------------------
int ExpHbaseInterface_JNI::lockRequired(NAString tblName, short lockMode, NABoolean useHbaseXn,
                                        const NABoolean replSync, const NABoolean incrementalBackup,
                                        //    const int64_t timestamp,
                                        NABoolean asyncOperation,
                                        //    const char * encryptionInfo,
                                        NABoolean noConflictCheck, NABoolean registerRegion) {
  long transID;
  long savepointID = -1;
  long pSavepointId = -1;

  if (useHbaseXn)
    transID = 0;
  else {
    transID = getTransactionIDFromContext();
    savepointID = getSavepointIDFromContext(pSavepointId);
  }

  if (transID < 1) {
    return HBASE_LOCK_REQUIRED_NOT_INT_TRANSACTION;
  }

  switch (storageType_) {
    case COM_STORAGE_MONARCH:

      break;
    default:
      HTableClient_JNI *htc;
      if (!transID) {
        if (checkAndWaitSnapshotInProgress((NAHeap *)heap_)) return -HBASE_BACKUP_LOCK_TIMEOUT_ERROR;
      }
      UInt32 flags = 0;
      htc->setFlags(flags, useTRex_, replSync, 0, incrementalBackup, asyncOperation, noConflictCheck, FALSE);
      retCode_ = client_->lockRequired((NAHeap *)heap_, tblName, hbs_, transID, savepointID, pSavepointId, lockMode,
                                       registerRegion, &htc);

      int ret = getLockErrorNum(retCode_);
      if (ret != HBASE_ACCESS_SUCCESS)
        return ret;
      else if (retCode_ != HBC_OK) {
        return -HBASE_ACCESS_ERROR;
      } else {
        if (asyncOperation) asyncHtc_ = htc;
      }
  }
  return HBASE_ACCESS_SUCCESS;
}

//
//----------------------------------------------------------------------------
int ExpHbaseInterface_JNI::updateVisibility(HbaseStr tblName, HbaseStr rowID, HbaseStr tagsRow, NABoolean useHbaseXn) {
  HTableClient_JNI *htc;
  long transID;
  NABoolean checkAndPut = FALSE;

  if (useHbaseXn)
    transID = 0;
  else
    transID = getTransactionIDFromContext();
  switch (storageType_) {
    case COM_STORAGE_MONARCH:
      return HBASE_NOT_IMPLEMENTED;
      break;
    default:
      retCode_ = client_->updateVisibility((NAHeap *)heap_, tblName.val, hbs_, useTRex_, transID, rowID, tagsRow, &htc);
      if (retCode_ != HBC_OK) {
        asyncHtc_ = NULL;
        return -HBASE_ACCESS_ERROR;
      } else
        asyncHtc_ = htc;
  }
  return HBASE_ACCESS_SUCCESS;
}

//----------------------------------------------------------------------------
int ExpHbaseInterface_JNI::getRowsOpen(HbaseStr tblName, short rowIDLen, HbaseStr rowIDs,
                                       const LIST(HbaseStr) & columns, int numReplications, const int lockMode,
                                       int isolationLevel, const NABoolean useMemoryScan,
                                       const NABoolean skipReadConflict, const NABoolean skipTransactionForBatchGet,
                                       const char *encryptionInfo) {
  long transID;
  transID = getTransactionIDFromContext();
  long savepointID;
  long pSavepointId;
  savepointID = getSavepointIDFromContext(pSavepointId);
  if (isolationLevel == TransMode::IL_NOT_SPECIFIED_) {
    isolationLevel = getTransactionIsolationLevelFromContext();
  }
  switch (storageType_) {
    case COM_STORAGE_MONARCH:

      break;
    default:

      htc_ = new ((NAHeap *)heap_) HTableClient_JNI((NAHeap *)heap_, (jobject)-1);
      htc_->setUseTrigger(getUseTrigger());
      retCode_ =
          client_->startGets((NAHeap *)heap_, (char *)tblName.val, useTRex_, FALSE, lockMode, skipReadConflict,
                             skipTransactionForBatchGet, hbs_, transID, savepointID, pSavepointId, isolationLevel, NULL,
                             rowIDLen, &rowIDs, columns, -1, numReplications, htc_, NULL, encryptionInfo);

      if (retCode_ == HBC_ERROR_GET_HTC_EXCEPTION) {
        htc_ = NULL;
        return HBASE_OPEN_ERROR;
      }

      int ret = getLockErrorNum(retCode_);
      if (ret != HBASE_ACCESS_SUCCESS) {
        htc_ = NULL;
        return ret;
      }
  }
  return HBASE_ACCESS_SUCCESS;
}

int ExpHbaseInterface_JNI::setWriteBufferSize(HbaseStr &tblName, int size) {
  switch (storageType_) {
    case COM_STORAGE_MONARCH:
      return HBASE_NOT_IMPLEMENTED;
      break;
    default:
      HTableClient_JNI *htc = client_->getHTableClient((NAHeap *)heap_, tblName.val, useTRex_, FALSE, FALSE, hbs_);
      if (htc == NULL) {
        retCode_ = HBC_ERROR_GET_HTC_EXCEPTION;
        return HBASE_OPEN_ERROR;
      }

      retCode_ = htc->setWriteBufferSize(size);

      client_->releaseHTableClient(htc);

      if (retCode_ != HBC_OK) return -HBASE_ACCESS_ERROR;
  }
  return HBASE_ACCESS_SUCCESS;
}
int ExpHbaseInterface_JNI::setWriteToWAL(HbaseStr &tblName, NABoolean WAL) {
  switch (storageType_) {
    case COM_STORAGE_MONARCH:
      return HBASE_NOT_IMPLEMENTED;
      break;
    default:
      HTableClient_JNI *htc = client_->getHTableClient((NAHeap *)heap_, tblName.val, useTRex_, replSync_, FALSE, hbs_);
      if (htc == NULL) {
        retCode_ = HBC_ERROR_GET_HTC_EXCEPTION;
        return HBASE_OPEN_ERROR;
      }

      retCode_ = htc->setWriteToWAL(WAL);

      client_->releaseHTableClient(htc);

      if (retCode_ != HBC_OK) return -HBASE_ACCESS_ERROR;
  }
  return HBASE_ACCESS_SUCCESS;
}

int ExpHbaseInterface_JNI::initHBLC(ExHbaseAccessStats *hbs) {
  switch (storageType_) {
    case COM_STORAGE_MONARCH:
      return HBASE_NOT_IMPLEMENTED;
      break;
    default:
      int rc = init(hbs);
      if (rc != HBASE_ACCESS_SUCCESS) return rc;

      if (hblc_ == NULL) {
        hblc_ = client_->getHBulkLoadClient((NAHeap *)heap_);
        if (hblc_ == NULL) {
          retCode_ = HBLC_ERROR_INIT_HBLC_EXCEPTION;
          return HBASE_INIT_HBLC_ERROR;
        }
      }
  }
  return HBLC_OK;
}

// init and get backup restore client
int ExpHbaseInterface_JNI::initBRC(ExHbaseAccessStats *hbs) {
  int rc = init(hbs);
  if (rc != HBASE_ACCESS_SUCCESS) return rc;

  if (brc_ == NULL) {
    brc_ = client_->getBackupRestoreClient((NAHeap *)heap_);
    if (brc_ == NULL) {
      retCode_ = BRC_ERROR_INIT_BRC_EXCEPTION;
      return HBASE_INIT_BRC_ERROR;
    }
  }

  return BRC_OK;
}

int ExpHbaseInterface_JNI::initHFileParams(HbaseStr &tblName, Text &hFileLoc, Text &hfileName, long maxHFileSize,
                                           Text &hFileSampleLoc, Text &hfileSampleName, float fSampleRate) {
  if (hblc_ == NULL) {
    return -HBASE_ACCESS_ERROR;
  }

  retCode_ =
      hblc_->initHFileParams(tblName, hFileLoc, hfileName, maxHFileSize, hFileSampleLoc, hfileSampleName, fSampleRate);
  // close();
  if (retCode_ == HBLC_OK)
    return HBASE_ACCESS_SUCCESS;
  else
    return -HBASE_CREATE_HFILE_ERROR;
}

int ExpHbaseInterface_JNI::addToHFile(short rowIDLen, HbaseStr &rowIDs, HbaseStr &rows, const char *encryptionInfo) {
  if (hblc_ == NULL || client_ == NULL) {
    return -HBASE_ACCESS_ERROR;
  }

  retCode_ = hblc_->addToHFile(rowIDLen, rowIDs, rows, hbs_, encryptionInfo, (NAHeap *)heap_);
  if (retCode_ == HBLC_OK)
    return HBASE_ACCESS_SUCCESS;
  else
    return -HBASE_ADD_TO_HFILE_ERROR;
}

int ExpHbaseInterface_JNI::closeHFile(HbaseStr &tblName) {
  if (hblc_ == NULL || client_ == NULL) {
    return -HBASE_ACCESS_ERROR;
  }

  retCode_ = hblc_->closeHFile(tblName);
  // close();
  if (retCode_ == HBLC_OK)
    return HBASE_ACCESS_SUCCESS;
  else
    return -HBASE_CLOSE_HFILE_ERROR;
}

int ExpHbaseInterface_JNI::doBulkLoad(HbaseStr &tblName, Text &location, Text &tableName, NABoolean quasiSecure,
                                      NABoolean snapshot) {
  if (hblc_ == NULL || client_ == NULL) {
    return -HBASE_ACCESS_ERROR;
  }

  retCode_ = hblc_->doBulkLoad(tblName, location, tableName, quasiSecure, snapshot);

  if (retCode_ == HBLC_OK)
    return HBASE_ACCESS_SUCCESS;
  else
    return -HBASE_DOBULK_LOAD_ERROR;
}

int ExpHbaseInterface_JNI::bulkLoadCleanup(HbaseStr &tblName, Text &location) {
  if (hblc_ == NULL || client_ == NULL) {
    return -HBASE_ACCESS_ERROR;
  }

  retCode_ = hblc_->bulkLoadCleanup(tblName, location);

  if (retCode_ == HBLC_OK)
    return HBASE_ACCESS_SUCCESS;
  else
    return -HBASE_CLEANUP_HFILE_ERROR;
}

///////////////////
int ExpHbaseInterface_JNI::incrCounter(const char *tabName, const char *rowId, const char *famName,
                                       const char *qualName, long incr, long &count) {
  if (client_ == NULL) {
    retCode_ = init();
    if (retCode_ != HBC_OK) return -HBASE_ACCESS_ERROR;
  }
  retCode_ = client_->incrCounter(tabName, rowId, famName, qualName, incr, count);

  if (retCode_ == HBC_OK)
    return HBASE_ACCESS_SUCCESS;
  else
    return -HBC_ERROR_INCR_COUNTER_EXCEPTION;
}

int ExpHbaseInterface_JNI::createCounterTable(const char *tabName, const char *famName) {
  if (client_ == NULL) {
    retCode_ = init();
    if (retCode_ != HBC_OK) return -HBASE_ACCESS_ERROR;
  }

  retCode_ = client_->createCounterTable(tabName, famName);

  if (retCode_ == HBC_OK)
    return HBASE_ACCESS_SUCCESS;
  else
    return -HBC_ERROR_CREATE_COUNTER_EXCEPTION;
}

int ExpHbaseInterface_JNI::sentryGetPrivileges(set<string> &groupNames, const char *tableOrViewName, bool isView,
                                               map<int, char *> &columnNumberToNameMap,
                                               PrivMgrUserPrivs &userPrivs /* out */) {
  // The bitMaps returned by getSentryPrivileges are returned in a
  // vector with the following structure:
  //
  // 0 - schema privilege bitmap
  // 1 - object privilege bitmap
  // 2 through n+1, where n = # columns - column privilege bitmap
  // n+2 - schema privilege with grant option bitmap
  // n+3 - object privilege with grant option bitmap
  // n+4 through 2n+3, where n = # columns - column privilege with grant option bitmap
  //
  // So, the number of elements in the returned array is 2n+4, where n = # columns.
  size_t n = columnNumberToNameMap.size();  // for brevity
  vector<int> bitMaps(2 * n + 4, 0);
  // We don't pass the columnNumberToNameMap across the JNI to the Java
  // side; we instead extract the column names and pass just those. In
  // the process of course we lose the information about how column numbers
  // map to column names. The purpose of this next vector is to save that
  // information off so we can insert the individual column privilege
  // bitmaps at the right place. (If we had a columnNameToNumber map
  // instead, we would not need these gymnastics.)
  vector<int> positionToColumnNumberVector(columnNumberToNameMap.size(), 0);

  return -retCode_;  // error case
}

//
int ExpHbaseInterface_JNI::sentryGetPrivileges(const char *userName, const char *tableOrViewName, bool isView,
                                               map<int, char *> &columnNumberToNameMap,
                                               PrivMgrUserPrivs &userPrivs /* out */) {
  // at the moment, we don't support Hive views
  if (isView) return -HVC_ERROR_SENTRY_GET_PARAM;

  // The bitMaps returned by getSentryPrivileges are returned in a
  // vector with the following structure:
  //
  // 0 - schema privilege bitmap
  // 1 - object privilege bitmap
  // 2 through n+1, where n = # columns - column privilege bitmap
  // n+2 - schema privilege with grant option bitmap
  // n+3 - object privilege with grant option bitmap
  // n+4 through 2n+3, where n = # columns - column privilege with grant option bitmap
  //
  // So, the number of elements in the returned array is 2n+4, where n = # columns.
  size_t n = columnNumberToNameMap.size();  // for brevity
  vector<int> bitMaps(2 * n + 4, 0);

  // We don't pass the columnNumberToNameMap across the JNI to the Java
  // side; we instead extract the column names and pass just those. In
  // the process of course we lose the information about how column numbers
  // map to column names. The purpose of this next vector is to save that
  // information off so we can insert the individual column privilege
  // bitmaps at the right place. (If we had a columnNameToNumber map
  // instead, we would not need these gymnastics.)
  vector<int> positionToColumnNumberVector(columnNumberToNameMap.size(), 0);

  return -retCode_;  // error case
}

int ExpHbaseInterface_JNI::isEmpty(HbaseStr &tblName) {
  int retcode;

  switch (storageType_) {
    case COM_STORAGE_MONARCH:
      return HBASE_NOT_IMPLEMENTED;
      break;
    default:
      retcode = init(hbs_);
      if (retcode != HBASE_ACCESS_SUCCESS) return -HBASE_OPEN_ERROR;

      LIST(HbaseStr) columns(heap_);

      // This is an internal interface to access HBase table.
      retcode = scanOpen(tblName, "", "", columns, -1, FALSE, FALSE, HBaseLockMode::LOCK_IS,
                         TransMode::READ_UNCOMMITTED_, /* skipReadConflict */ FALSE,
                         /* skipTransaction */ FALSE, FALSE, FALSE, FALSE, 100, TRUE, NULL, NULL, NULL, -1);

      if (retcode != HBASE_ACCESS_SUCCESS) return -HBASE_OPEN_ERROR;

      retcode = nextRow();

      scanClose();
      if (retcode == HBASE_ACCESS_EOD)
        return 1;  // isEmpty
      else if (retcode == HBASE_ACCESS_SUCCESS)
        return 0;  // not empty
  }
  return -HBASE_ACCESS_ERROR;  // error
}

//----------------------------------------------------------------------------
int ExpHbaseInterface_JNI::checkAndInsertRow(HbaseStr &tblName, HbaseStr &rowID, HbaseStr &row, NABoolean useHbaseXn,
                                             const NABoolean replSync, const NABoolean incrementalBackup,
                                             NABoolean useRegionXn, const int64_t timestamp, NABoolean asyncOperation,
                                             const char *encryptionInfo, const char *triggers, const char *curExecSql,
                                             Int16 colIndexToCheck) {
  long transID;
  long savepointID = -1;
  long pSavepointId = -1;

  NABoolean checkAndPut = TRUE;

  if (useHbaseXn)
    transID = 0;
  else {
    transID = getTransactionIDFromContext();
    savepointID = getSavepointIDFromContext(pSavepointId);
  }

  switch (storageType_) {
    case COM_STORAGE_MONARCH:

      break;
    default:
      HTableClient_JNI *htc = NULL;
      if (!transID) {
        if (checkAndWaitSnapshotInProgress((NAHeap *)heap_)) return -HBASE_BACKUP_LOCK_TIMEOUT_ERROR;
      }
      UInt32 flags = 0;
      htc->setFlags(flags, useTRex_, replSync, useRegionXn, incrementalBackup, asyncOperation,
                    noConflictCheckForIndex(), FALSE);

      client_->settrigger_operation(gettrigger_operation());
      if (triggers != NULL && gettrigger_operation() == COM_UPDATE && htc_ != NULL)  // update operation
      {
        htc_->setOldValueInfo(rowID);

        client_->setOldValueInfo(htc_->skvBuffer_, htc_->sBufSize_, htc_->srowIDs_, htc_->srowIDsLen_);
      }

      retCode_ = client_->insertRow((NAHeap *)heap_, tblName.val, hbs_, transID, savepointID, pSavepointId, rowID, row,
                                    timestamp, checkAndPut, flags, encryptionInfo, triggers, curExecSql,
                                    colIndexToCheck, &htc, ddlValidator_);

      if (retCode_ == HBC_ERROR_INSERTROW_INVALID_DDL) return HBASE_INVALID_DDL;

      if (retCode_ == HBC_ERROR_INSERTROW_DUP_ROWID) {
        return HBASE_DUP_ROW_ERROR;
      }

      int ret = getLockErrorNum(retCode_);
      if (ret != HBASE_ACCESS_SUCCESS)
        return ret;
      else if (retCode_ == HBC_ERROR_TRIGGER_EXECUTE_EXCEPTION) {
        return TRIGGER_EXECUTE_EXCEPTION;
      } else if (retCode_ == HBC_ERROR_TRIGGER_PARAMETER_EXCEPTION) {
        return TRIGGER_PARAMETER_EXCEPTION;
      } else if (retCode_ != HBC_OK) {
        return -HBASE_ACCESS_ERROR;
      } else {
        if (asyncOperation) asyncHtc_ = htc;
      }
  }
  return HBASE_ACCESS_SUCCESS;
}

int ExpHbaseInterface_JNI::checkAndUpdateRow(HbaseStr &tblName, HbaseStr &rowID, HbaseStr &row, HbaseStr &columnToCheck,
                                             HbaseStr &colValToCheck, NABoolean useHbaseXn, const NABoolean replSync,
                                             const NABoolean incrementalBackup, NABoolean useRegionXn,
                                             const int64_t timestamp, NABoolean asyncOperation,
                                             const char *encryptionInfo, const char *triggers, const char *curExecSql) {
  long transID;
  long savepointID = -1;
  long pSavepointId = -1;

  if (useHbaseXn)
    transID = 0;
  else {
    transID = getTransactionIDFromContext();
    savepointID = getSavepointIDFromContext(pSavepointId);
  }

  switch (storageType_) {
    case COM_STORAGE_MONARCH:

      break;
    default:
      HTableClient_JNI *htc;
      UInt32 flags = 0;
      htc->setFlags(flags, useTRex_, replSync, useRegionXn, incrementalBackup, asyncOperation,
                    noConflictCheckForIndex(), FALSE);
      retCode_ = client_->checkAndUpdateRow((NAHeap *)heap_, tblName.val, hbs_, transID, savepointID, pSavepointId,
                                            rowID, row, columnToCheck, colValToCheck, timestamp, flags, encryptionInfo,
                                            triggers, curExecSql, &htc);

      int ret = getLockErrorNum(retCode_);
      if (retCode_ != HBASE_ACCESS_SUCCESS) {
        return ret;
      } else if (retCode_ == HBC_ERROR_TRIGGER_EXECUTE_EXCEPTION) {
        return TRIGGER_EXECUTE_EXCEPTION;
      } else if (retCode_ == HBC_ERROR_TRIGGER_PARAMETER_EXCEPTION) {
        return TRIGGER_PARAMETER_EXCEPTION;
      } else if (retCode_ == HBC_ERROR_CHECKANDUPDATEROW_NOTFOUND) {
        return HBASE_ROW_NOTFOUND_ERROR;
      } else if (retCode_ != HBC_OK) {
        return -HBASE_ACCESS_ERROR;
      } else {
        if (asyncOperation) asyncHtc_ = htc;
      }
  }
  return HBASE_ACCESS_SUCCESS;
}

int ExpHbaseInterface_JNI::coProcAggr(HbaseStr &tblName,
                                      int aggrType,  // 0:count, 1:min, 2:max, 3:sum, 4:avg
                                      const Text &startRow, const Text &stopRow, const Text &colFamily,
                                      const Text &colName, const NABoolean cacheBlocks, const int numCacheRows,
                                      const NABoolean replSync,
                                      Text &aggrVal)  // returned value
{
  return coProcAggr(tblName, aggrType, startRow, stopRow, colFamily, colName, cacheBlocks, numCacheRows, replSync,
                    aggrVal, TransMode::IL_NOT_SPECIFIED_, HBaseLockMode::LOCK_NO);
}

int ExpHbaseInterface_JNI::coProcAggr(HbaseStr &tblName,
                                      int aggrType,  // 0:count, 1:min, 2:max, 3:sum, 4:avg
                                      const Text &startRow, const Text &stopRow, const Text &colFamily,
                                      const Text &colName, const NABoolean cacheBlocks, const int numCacheRows,
                                      const NABoolean replSync,
                                      Text &aggrVal,  // returned value
                                      int isolationLevel, int lockMode) {
  switch (storageType_) {
    case COM_STORAGE_MONARCH:

      break;

    default: {
      HTableClient_JNI *htc;
      htc = client_->getHTableClient((NAHeap *)heap_, tblName.val, useTRex_, replSync, FALSE, hbs_);
      if (htc == NULL) {
        retCode_ = HBC_ERROR_GET_HTC_EXCEPTION;
        return HBASE_OPEN_ERROR;
      }

      long transID = getTransactionIDFromContext();
      long svptId = -1;
      long pSvptId = -1;
      svptId = getSavepointIDFromContext(pSvptId);
      retCode_ = htc->coProcAggr(transID, svptId, pSvptId, isolationLevel, lockMode, aggrType, startRow, stopRow,
                                 colFamily, colName, cacheBlocks, numCacheRows, aggrVal);

      client_->releaseHTableClient(htc);
    } break;
  }  // switch

  int ret = getLockErrorNum(retCode_);
  if (ret != HBASE_ACCESS_SUCCESS)
    return ret;
  else if (retCode_ != HBC_OK)
    return -HBASE_ACCESS_ERROR;

  return HBASE_ACCESS_SUCCESS;
}

//----------------------------------------------------------------------------
int ExpHbaseInterface_JNI::getClose() {
  switch (storageType_) {
    case COM_STORAGE_MONARCH:

      break;
    default:
      if (htc_) {
        client_->releaseHTableClient(htc_);
        htc_ = NULL;
      }
  }
  return HBASE_ACCESS_SUCCESS;
}

//----------------------------------------------------------------------------
int ExpHbaseInterface_JNI::grant(const Text &user, const Text &tblName, const std::vector<Text> &actionCodes) {
  switch (storageType_) {
    case COM_STORAGE_MONARCH:
      return HBASE_NOT_IMPLEMENTED;
      break;
    default:
      retCode_ = client_->grant(user, tblName, actionCodes);
      if (retCode_ != HBC_OK) return -HBASE_ACCESS_ERROR;
  }
  return HBASE_ACCESS_SUCCESS;
}

//----------------------------------------------------------------------------
int ExpHbaseInterface_JNI::revoke(const Text &user, const Text &tblName, const std::vector<Text> &actionCodes) {
  switch (storageType_) {
    case COM_STORAGE_MONARCH:
      return HBASE_NOT_IMPLEMENTED;
      break;
    default:
      retCode_ = client_->revoke(user, tblName, actionCodes);
      if (retCode_ != HBC_OK) return -HBASE_ACCESS_ERROR;
  }
  return HBASE_ACCESS_SUCCESS;
}

NAArray<HbaseStr> *ExpHbaseInterface_JNI::getRegionBeginKeys(const char *tblName) {
  NAArray<HbaseStr> *retValue;
  switch (storageType_) {
    case COM_STORAGE_MONARCH:
      break;
    default:
      retValue = client_->getStartKeys((NAHeap *)heap_, tblName, useTRex_);
  }
  return retValue;
}

NAArray<HbaseStr> *ExpHbaseInterface_JNI::getRegionEndKeys(const char *tblName) {
  NAArray<HbaseStr> *retValue;
  switch (storageType_) {
    case COM_STORAGE_MONARCH:
      break;
    default:
      retValue = client_->getEndKeys((NAHeap *)heap_, tblName, useTRex_);
  }
  return retValue;
}

int ExpHbaseInterface_JNI::getColVal(int colNo, BYTE *colVal, int &colValLen, NABoolean nullable, BYTE &nullVal,
                                     BYTE *tag, int &tagLen, const char *encryptionInfo) {
  switch (storageType_) {
    case COM_STORAGE_MONARCH:

      break;
    default:
      HTC_RetCode retCode = HTC_OK;
      if (htc_ != NULL)
        retCode = htc_->getColVal(colNo, colVal, colValLen, nullable, nullVal, tag, tagLen, encryptionInfo);
      else {
        retCode_ = HBC_ERROR_GET_HTC_EXCEPTION;
        return HBASE_OPEN_ERROR;
      }
      if (retCode != HTC_OK) return HBASE_ACCESS_ERROR;
  }
  return HBASE_ACCESS_SUCCESS;
}

int ExpHbaseInterface_JNI::getColVal(NAHeap *heap, int colNo, BYTE **colVal, int &colValLen,
                                     const char *encryptionInfo) {
  switch (storageType_) {
    case COM_STORAGE_MONARCH:

      break;
    default:
      HTC_RetCode retCode = HTC_OK;
      if (htc_ != NULL)
        retCode = htc_->getColVal(heap, colNo, colVal, colValLen, encryptionInfo);
      else {
        retCode_ = HBC_ERROR_GET_HTC_EXCEPTION;
        return HBASE_OPEN_ERROR;
      }
      if (retCode != HTC_OK) return HBASE_ACCESS_ERROR;
  }
  return HBASE_ACCESS_SUCCESS;
}

int ExpHbaseInterface_JNI::getRowID(HbaseStr &rowID, const char *encryptionInfo) {
  switch (storageType_) {
    case COM_STORAGE_MONARCH:

      break;
    default:
      HTC_RetCode retCode = HTC_OK;
      if (htc_ != NULL)
        retCode = htc_->getRowID(rowID);
      else {
        retCode_ = HBC_ERROR_GET_HTC_EXCEPTION;
        return HBASE_OPEN_ERROR;
      }
      if (retCode != HTC_OK) return HBASE_ACCESS_ERROR;
  }
  return HBASE_ACCESS_SUCCESS;
}

int ExpHbaseInterface_JNI::getNumCellsPerRow(int &numCells) {
  switch (storageType_) {
    case COM_STORAGE_MONARCH:

      break;
    default:
      HTC_RetCode retCode = HTC_OK;
      if (htc_ != NULL)
        retCode = htc_->getNumCellsPerRow(numCells);
      else {
        retCode_ = HBC_ERROR_GET_HTC_EXCEPTION;
        return HBASE_OPEN_ERROR;
      }
      if (retCode == HTC_OK)
        return HBASE_ACCESS_SUCCESS;
      else if (retCode == HTC_DONE_DATA)
        return HBASE_ACCESS_NO_ROW;
  }
  return HBASE_ACCESS_ERROR;
}

int ExpHbaseInterface_JNI::getColName(int colNo, char **outColName, short &colNameLen, long &timestamp) {
  switch (storageType_) {
    case COM_STORAGE_MONARCH:

      break;
    default:
      HTC_RetCode retCode = HTC_OK;
      if (htc_ != NULL)
        retCode = htc_->getColName(colNo, outColName, colNameLen, timestamp);
      else {
        retCode_ = HBC_ERROR_GET_HTC_EXCEPTION;
        return HBASE_OPEN_ERROR;
      }
      if (retCode != HTC_OK) return HBASE_ACCESS_ERROR;
  }
  return HBASE_ACCESS_SUCCESS;
}

int ExpHbaseInterface_JNI::nextRow() {
  switch (storageType_) {
    case COM_STORAGE_MONARCH:

      break;
    default:
      HTC_RetCode retCode;
      if (htc_ != NULL)
        retCode = htc_->nextRow(ddlValidator_);
      else {
        retCode_ = HBC_ERROR_GET_HTC_EXCEPTION;
        return HBASE_OPEN_ERROR;
      }

      if (retCode == HTC_OK)
        return HBASE_ACCESS_SUCCESS;
      else if (retCode == HTC_DONE)
        return HBASE_ACCESS_EOD;
      else if (retCode == HTC_DONE_RESULT)
        return HBASE_ACCESS_EOR;

      int ret = getLockErrorNum(retCode);
      if (ret != HBASE_ACCESS_SUCCESS)
        return ret;
      else if (retCode == HTC_ERROR_FETCHROWS_INVALID_DDL)
        return HBASE_INVALID_DDL;
  }
  return -HBASE_ACCESS_ERROR;
}

int ExpHbaseInterface_JNI::nextCell(HbaseStr &rowId, HbaseStr &colFamName, HbaseStr &colName, HbaseStr &colVal,
                                    long &timestamp) {
  switch (storageType_) {
    case COM_STORAGE_MONARCH:

      break;
    default:
      HTC_RetCode retCode;
      if (htc_ != NULL)
        retCode = htc_->nextCell(rowId, colFamName, colName, colVal, timestamp);
      else {
        retCode_ = HBC_ERROR_GET_HTC_EXCEPTION;
        return HBASE_OPEN_ERROR;
      }
      if (retCode == HTC_OK)
        return HBASE_ACCESS_SUCCESS;
      else if (retCode == HTC_DONE)
        return HBASE_ACCESS_EOD;
  }
  return -HBASE_ACCESS_ERROR;
}

int ExpHbaseInterface_JNI::completeAsyncOperation(int timeout, NABoolean *resultArray, Int16 resultArrayLen) {
  switch (storageType_) {
    case COM_STORAGE_MONARCH:

      break;
    default:
      HTC_RetCode retCode;
      if (asyncHtc_ != NULL)
        retCode = asyncHtc_->completeAsyncOperation(timeout, resultArray, resultArrayLen);
      else {
        retCode_ = HBC_ERROR_GET_HTC_EXCEPTION;
        return HBASE_OPEN_ERROR;
      }
      if (retCode == HTC_ERROR_ASYNC_OPERATION_NOT_COMPLETE) return HBASE_RETRY_AGAIN;
      client_->releaseHTableClient(asyncHtc_);
      asyncHtc_ = NULL;
      if (retCode == HTC_OK) return HBASE_ACCESS_SUCCESS;

      int ret = getLockErrorNum(retCode);
      if (ret != HBASE_ACCESS_SUCCESS) return ret;
  }
  return -HBASE_ACCESS_ERROR;
}

// Get an estimate of the number of rows in table tblName. Pass in the
// fully qualified table name and the number of columns in the table.
// The row count estimate is returned in estRC.
int ExpHbaseInterface_JNI::estimateRowCount(HbaseStr &tblName, int partialRowSize, int numCols,
                                            int retryLimitMilliSeconds, NABoolean useCoprocessor, long &estRC,
                                            int &breadCrumb) {
  breadCrumb = 11;
  switch (storageType_) {
    case COM_STORAGE_MONARCH:
      estRC = 0;
      retCode_ = HBASE_ACCESS_SUCCESS;
      break;
    default:
      if (client_ == NULL) {
        breadCrumb = 12;
        if (init(hbs_) != HBASE_ACCESS_SUCCESS) return -HBASE_ACCESS_ERROR;
      }

      estRC = 0;
      retCode_ = client_->estimateRowCount(tblName.val, partialRowSize, numCols, retryLimitMilliSeconds, useCoprocessor,
                                           estRC, breadCrumb /* out */);
  }
  return retCode_;
}

// get nodeNames of regions. this information will be used to co-locate ESPs
int ExpHbaseInterface_JNI::getRegionsNodeName(const HbaseStr &tblName, int partns, ARRAY(const char *) & nodeNames) {
  switch (storageType_) {
    case COM_STORAGE_MONARCH:

      break;
    default:
      if (client_ == NULL) {
        if (init(hbs_) != HBASE_ACCESS_SUCCESS) return -HBASE_ACCESS_ERROR;
      }
      retCode_ = client_->getRegionsNodeName(tblName.val, partns, nodeNames);
      if (retCode_ != HBC_OK) return -HBASE_ACCESS_ERROR;
  }
  return HBASE_ACCESS_SUCCESS;
}

// Get Hbase Table information. This will be generic function to get needed information
// from Hbase layer. Currently index level and blocksize is being requested for use in
// costing code, but can be extended in the future so that we only make one JNI call.
int ExpHbaseInterface_JNI::getHbaseTableInfo(const HbaseStr &tblName, int &indexLevels, int &blockSize) {
  switch (storageType_) {
    case COM_STORAGE_MONARCH:
      retCode_ = HBASE_ACCESS_SUCCESS;
      break;
    default:
      if (client_ == NULL) {
        if (init(hbs_) != HBASE_ACCESS_SUCCESS) return -HBASE_ACCESS_ERROR;
      }
      retCode_ = client_->getHbaseTableInfo(tblName.val, indexLevels, blockSize);
  }
  return retCode_;
}

int ExpHbaseInterface_JNI::cleanSnpTmpLocation(const char *path) {
  if (client_ == NULL) {
    if (init(hbs_) != HBASE_ACCESS_SUCCESS) return -HBASE_ACCESS_ERROR;
  }
  retCode_ = client_->cleanSnpTmpLocation(path);
  return retCode_;
}

int ExpHbaseInterface_JNI::setArchivePermissions(const char *tabName) {
  if (client_ == NULL) {
    if (init(hbs_) != HBASE_ACCESS_SUCCESS) return -HBASE_ACCESS_ERROR;
  }
  retCode_ = client_->setArchivePermissions(tabName);
  return retCode_;
}

int ExpHbaseInterface_JNI::getBlockCacheFraction(float &frac) {
  if (client_ == NULL) return -HBASE_ACCESS_ERROR;

  retCode_ = client_->getBlockCacheFraction(frac);
  return retCode_;
}

NAArray<HbaseStr> *ExpHbaseInterface_JNI::showTablesHDFSCache(const std::vector<Text> &tables) {
  if (client_ == NULL) {
    if (init(hbs_) != HBASE_ACCESS_SUCCESS) return NULL;
  }

  NAArray<HbaseStr> *stats = client_->showTablesHDFSCache((NAHeap *)heap_, tables);
  if (stats == NULL) return NULL;

  return stats;
}

int ExpHbaseInterface_JNI::addTablesToHDFSCache(const std::vector<Text> &tables, const char *poolName) {
  if (client_ == NULL) return -HBASE_ACCESS_ERROR;

  retCode_ = client_->addTablesToHDFSCache(tables, poolName);
  return retCode_;
}

int ExpHbaseInterface_JNI::removeTablesFromHDFSCache(const std::vector<Text> &tables, const char *poolName) {
  if (client_ == NULL) return -HBASE_ACCESS_ERROR;

  retCode_ = client_->removeTablesFromHDFSCache(tables, poolName);
  return retCode_;
}

NAArray<HbaseStr> *ExpHbaseInterface_JNI::getRegionStats(const HbaseStr &tblName) {
  NAArray<HbaseStr> *regionStats;

  switch (storageType_) {
    case COM_STORAGE_MONARCH:
      regionStats = NULL;
      break;
    default:
      if (client_ == NULL) {
        if (init(hbs_) != HBASE_ACCESS_SUCCESS) return NULL;
      }

      regionStats = client_->getRegionStats((NAHeap *)heap_, tblName.val);
  }
  return regionStats;
}

NAArray<HbaseStr> *ExpHbaseInterface_JNI::getClusterStats(int &numEntries) {
  if (client_ == NULL) {
    if (init(hbs_) != HBASE_ACCESS_SUCCESS) return NULL;
  }

  NAArray<HbaseStr> *regionStats = client_->getRegionStats((NAHeap *)heap_, NULL);
  if (regionStats == NULL) return NULL;

  numEntries = regionStats->entries();

  return regionStats;
}

int ExpHbaseInterface_JNI::createSnapshot(const NAString &tableName, const NAString &snapshotName) {
  if (client_ == NULL) {
    if (init(hbs_) != HBASE_ACCESS_SUCCESS) return -HBASE_ACCESS_ERROR;
  }

  retCode_ = client_->createSnapshot(tableName, snapshotName);
  if (retCode_ == HBC_OK)
    return HBASE_ACCESS_SUCCESS;
  else
    return HBASE_CREATE_SNAPSHOT_ERROR;
}

int ExpHbaseInterface_JNI::restoreSnapshot(const NAString &snapshotName, const NAString &tableName) {
  if (client_ == NULL) {
    if (init(hbs_) != HBASE_ACCESS_SUCCESS) return -HBASE_ACCESS_ERROR;
  }

  retCode_ = client_->restoreSnapshot(snapshotName, tableName);
  if (retCode_ == HBC_OK)
    return HBASE_ACCESS_SUCCESS;
  else
    return HBASE_RESTORE_SNAPSHOT_ERROR;
}

int ExpHbaseInterface_JNI::deleteSnapshot(const NAString &snapshotName) {
  if (client_ == NULL) {
    if (init(hbs_) != HBASE_ACCESS_SUCCESS) return -HBASE_ACCESS_ERROR;
  }

  retCode_ = client_->deleteSnapshot(snapshotName);
  if (retCode_ == HBC_OK)
    return HBASE_ACCESS_SUCCESS;
  else
    return HBASE_DELETE_SNAPSHOT_ERROR;
}

int ExpHbaseInterface_JNI::savepointCommitOrRollback(long transId, long savepointId, long tgtSavepointId,
                                                     NABoolean isCommit) {
  if (client_ == NULL) {
    if (init(hbs_) != HBASE_ACCESS_SUCCESS) return -HBASE_ACCESS_ERROR;
  }

  HBC_RetCode retCode;
  retCode = client_->savepointCommitOrRollback(transId, savepointId, tgtSavepointId, isCommit);
  if (retCode == HBC_OK) return HBASE_ACCESS_SUCCESS;

  return -HBASE_ACCESS_ERROR;
}

int ExpHbaseInterface_JNI::verifySnapshot(const NAString &tableName, const NAString &snapshotName, NABoolean &exist) {
  if (client_ == NULL) {
    if (init(hbs_) != HBASE_ACCESS_SUCCESS) return -HBASE_ACCESS_ERROR;
  }

  retCode_ = client_->verifySnapshot(tableName, snapshotName, exist);
  if (retCode_ == HBC_OK)
    return HBASE_ACCESS_SUCCESS;
  else
    return HBASE_VERIFY_SNAPSHOT_ERROR;
}

void ExpHbaseInterface_JNI::setDDLValidator(ExDDLValidator *ddlValidator) { ddlValidator_ = ddlValidator; }

short ExpHbaseInterface_JNI::getNextValue(NAString &tabName, NAString &rowId, NAString &famName, NAString &qualName,
                                          long incrBy, long &nextValue, NABoolean skipWAL) {
  if (client_ == NULL) {
    retCode_ = init();
    if (retCode_ != HBC_OK) return -HBASE_ACCESS_ERROR;
  }

  retCode_ = client_->getNextValue(tabName, rowId, famName, qualName, incrBy, nextValue, skipWAL);

  if (retCode_ == HBC_OK)
    return HBASE_ACCESS_SUCCESS;
  else
    return -HBC_ERROR_GET_NEXT_VALUE_EXCEPTION;
}

int ExpHbaseInterface_JNI::getTableDefForBinlog(NAString &tabName, NAArray<HbaseStr> **retNames) {
  if (client_ == NULL) {
    retCode_ = init();
    if (retCode_ != HBC_OK) return -HBASE_ACCESS_ERROR;
  }
  retCode_ = client_->getTableDefForBinlog(tabName, (NAHeap *)heap_, retNames);

  if (retCode_ == HBC_OK)
    return HBASE_ACCESS_SUCCESS;
  else
    return -HBC_ERROR_GET_TABLE_DEF_FOR_BINLOG_ERROR;
}

int ExpHbaseInterface_JNI::updateTableDefForBinlog(NAString &tabName, NAString &cols, NAString &keyCols, long ts) {
  if (client_ == NULL) {
    retCode_ = init();
    if (retCode_ != HBC_OK) return -HBASE_ACCESS_ERROR;
  }
  retCode_ = client_->updateTableDefForBinlog(tabName, cols, keyCols, ts);

  if (retCode_ == HBC_OK)
    return HBASE_ACCESS_SUCCESS;
  else
    return -HBC_ERROR_DELETE_SEQ_ROW_EXCEPTION;
}

int ExpHbaseInterface_JNI::deleteSeqRow(NAString &tabName, NAString &rowId) {
  if (client_ == NULL) {
    retCode_ = init();
    if (retCode_ != HBC_OK) return -HBASE_ACCESS_ERROR;
  }

  retCode_ = client_->deleteSeqRow(tabName, rowId);

  if (retCode_ == HBC_OK)
    return HBASE_ACCESS_SUCCESS;
  else
    return -HBC_ERROR_DELETE_SEQ_ROW_EXCEPTION;
}

int ExpHbaseInterface_JNI::getLockErrorNum(int retCode) {
  if (retCode == HTC_ERROR_LOCK_ROLLBACK_EXCEPTION || retCode == HBC_ERROR_LOCK_ROLLBACK_EXCEPTION)
    return -HBASE_LOCK_ROLLBACK_ERROR;
  else if (retCode == HTC_ERROR_LOCK_TIME_OUT_EXCEPTION || retCode == HBC_ERROR_LOCK_TIME_OUT_EXCEPTION)
    return -HBASE_LOCK_TIME_OUT_ERROR;
  else if (retCode == HTC_ERROR_DEAD_LOCK_EXCEPTION || retCode == HBC_ERROR_DEAD_LOCK_EXCEPTION)
    return -HBASE_DEAD_LOCK_ERROR;
  else if (retCode == HTC_ERROR_RPC_TIME_OUT_EXCEPTION || retCode == HBC_ERROR_RPC_TIME_OUT_EXCEPTION)
    return -HBASE_RPC_TIME_OUT_ERROR;
  else if (retCode == HTC_ERROR_CANCEL_OPERATION || retCode == HBC_ERROR_CANCEL_OPERATION)
    return -HBASE_CANCEL_OPERATION;
  else if (retCode == HTC_ERROR_LOCK_REGION_MOVE || retCode == HBC_ERROR_LOCK_REGION_MOVE)
    return -HBASE_LOCK_REGION_MOVE_ERROR;
  else if (retCode == HTC_ERROR_LOCK_REGION_SPLIT || retCode == HBC_ERROR_LOCK_REGION_SPLIT)
    return -HBASE_LOCK_REGION_SPLIT_ERROR;
  else if (retCode == HTC_ERROR_LOCK_NOT_ENOUGH_RESOURCE_EXCEPTION ||
           retCode == HBC_ERROR_LOCK_NOT_ENOUGH_RESOURCE_EXCEPTION)
    return -HBASE_LOCK_NOT_ENOUGH_RESOURCE;

  return HBASE_ACCESS_SUCCESS;
}

int ExpHbaseInterface_JNI::putData(long eventID, const char *query, int eventType, const char *schemaName,
                                   unsigned char *params, long len) {
  if (client_ == NULL) {
    retCode_ = init();
    if (retCode_ != HBC_OK) return -HBASE_ACCESS_ERROR;
  }
  retCode_ = client_->putData(eventID, query, eventType, schemaName, params, len);

  if (retCode_ == HBC_OK)
    return HBASE_ACCESS_SUCCESS;
  else
    return -HBC_ERROR_PUT_SQL_TO_HBASE_ERROR;
}
