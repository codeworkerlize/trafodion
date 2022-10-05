// **********************************************************************
// // @@@ START COPYRIGHT @@@
// //
// // Licensed to the Apache Software Foundation (ASF) under one
// // or more contributor license agreements.  See the NOTICE file
// // distributed with this work for additional information
// // regarding copyright ownership.  The ASF licenses this file
// // to you under the Apache License, Version 2.0 (the
// // "License"); you may not use this file except in compliance
// // with the License.  You may obtain a copy of the License at
// //
// //   http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing,
// // software distributed under the License is distributed on an
// // "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// // KIND, either express or implied.  See the License for the
// // specific language governing permissions and limitations
// // under the License.
// //
// // @@@ END COPYRIGHT @@@
// // **********************************************************************
#include "executor/MemoryTableClient.h"
#include "cli/Globals.h"
#include "sqlcomp/SharedCache.h"

const char *fixNamePtr(const char *tableName) {
  const char *colonPos = strchr(tableName, ':');
  if (colonPos)
    return colonPos + 1;
  else
    return tableName;
}

static NAMemory *getSharedHeap() {
  SharedTableDataCache *sharedDataCache = SharedTableDataCache::locate();
  if (sharedDataCache) return sharedDataCache->getSharedHeap();

  return NULL;
}

static MemoryTableDB *getSharedTableDB() {
  SharedTableDataCache *sharedDataCache = SharedTableDataCache::locate();
  if (sharedDataCache) return sharedDataCache->getMemoryTableDB();
  return NULL;
}

static MemoryTableDB *getMemTableDB() {
  MemoryTableDB *memDB = NULL;
  if (memDB = getSharedTableDB()) return memDB;

  return NULL;
}

MemoryTableClient::MemoryTableClient(NAMemory *heap, char *tableName) : heap_(heap), kvArray_(heap_) {
  tableName_ = NULL;
  htableCache_ = NULL;
  numReqRows_ = 0;
  currentRowNum_ = 0;
  numRowsReturned_ = 0;
  fetchStartPos_ = -1;
  fetchMode_ = UNKNOWN;
  isDisabled_ = false;
  memDBinitFailed_ = true;
  init(tableName);
}

void MemoryTableClient::setTableName(const char *tableName) {
  if (tableName == NULL) return;

  int len = strlen(tableName);
  if (tableName_ != NULL) {
    NADELETEBASIC(tableName_, heap_);
    tableName_ = NULL;
  }

  tableName_ = new (heap_) char[len + 1];
  strcpy(tableName_, tableName);
}

void MemoryTableClient::init(char *tableName) {
  setTableName(fixNamePtr(tableName));

  MemoryTableDB *memDB = NULL;
  if (memDB = getMemTableDB()) {
    memDBinitFailed_ = false;
    HTableCache *htableCache = memDB->getHTableCache(tableName_);
    if (htableCache) {
      htableCache_ = htableCache;
      isDisabled_ = false;
    } else if (memDB->contains(tableName_))
      isDisabled_ = true;
  }
}

void MemoryTableClient::create() {
  if (htableCache_ == NULL) {
    MemoryTableDB *memDB = NULL;
    if (memDB = getMemTableDB()) {
      memDB->insert(tableName_);
      htableCache_ = memDB->getHTableCache(tableName_);
    }
  }
}

bool MemoryTableClient::insert(HbaseStr &key, HbaseStr &value) {
  if (htableCache_ == NULL) {
    MemoryTableDB *memDB = NULL;

    if (memDB = getMemTableDB()) {
      memDB->insert(tableName_);
      htableCache_ = memDB->getHTableCache(tableName_);
    }
  }
  return htableCache_->insert(key, value);
}

void MemoryTableClient::remove(const char *tableName) {
  MemoryTableDB *memDB = NULL;

  if (memDB = getMemTableDB()) memDB->remove(tableName);
}

int MemoryTableClient::startGet(const char *tableName, const HbaseStr &rowID) {
  ex_assert((htableCache_ && strcmp((fixNamePtr(tableName)), tableName_) == 0),
            "MemoryTableClient::startGet assert failed");

  numRowsReturned_ = htableCache_->startGet(rowID, kvArray_);
  currentRowNum_ = -1;

  return 0;  // HBASE_ACCESS_SUCCESS
}

int MemoryTableClient::startGets(const char *tableName, const NAList<HbaseStr> *rowIDs) {
  ex_assert((htableCache_ && strcmp((fixNamePtr(tableName)), tableName_) == 0),
            "MemoryTableClient::startGet assert failed");

  numRowsReturned_ = htableCache_->startGets(rowIDs, kvArray_);
  currentRowNum_ = -1;

  return 0;  // HBASE_ACCESS_SUCCESS
}

int MemoryTableClient::startGets(const char *tableName, const HbaseStr &rowIDs, const UInt32 keyLen) {
  ex_assert((htableCache_ && strcmp((fixNamePtr(tableName)), tableName_) == 0),
            "MemoryTableClient::startGet assert failed");

  numRowsReturned_ = htableCache_->startGets(rowIDs, keyLen, kvArray_);
  currentRowNum_ = -1;

  return 0;  // HBASE_ACCESS_SUCCESS
}

HTC_RetCode MemoryTableClient::nextRow() {
  HTC_RetCode retCode;

  switch (fetchMode_) {
    case GET_ROW:
      if (numRowsReturned_ == -1) return HTC_DONE;
      if (currentRowNum_ == -1) {
        currentRowNum_ = 0;
        return HTC_OK;
      } else {
        cleanupResultInfo();
        return HTC_DONE;
      }
      break;

    case BATCH_GET:
      if (numRowsReturned_ == -1) return HTC_DONE_RESULT;
      if (currentRowNum_ == -1) {
        currentRowNum_ = 0;
        return HTC_OK;
      } else if ((currentRowNum_ + 1) >= numRowsReturned_) {
        cleanupResultInfo();
        return HTC_DONE_RESULT;
      }
      break;
    default:
      break;
  }

  if (fetchMode_ == SCAN_FETCH && (currentRowNum_ == -1 || ((currentRowNum_ + 1) >= numRowsReturned_))) {
    if (currentRowNum_ != -1 && (numRowsReturned_ < numReqRows_)) {
      cleanupResultInfo();
      return HTC_DONE;
    }

    retCode = fetchRows();
    currentRowNum_ = 0;

    if (retCode != HTC_OK) {
      cleanupResultInfo();
      return retCode;
    }
  } else {
    currentRowNum_++;
  }
  return HTC_OK;
}

void MemoryTableClient::cleanupResultInfo() {
  numRowsReturned_ = 0;
  currentRowNum_ = 0;
  kvArray_.clear();
}

HTC_RetCode MemoryTableClient::getColVal(BYTE *colVal, int &colValLen) {
  if (kvArray_.entries() == 0) return HTC_DONE_DATA;

  HTableRow *row = kvArray_[currentRowNum_];
  if (row == NULL) return HTC_DONE_DATA;

  int dataLen = row->value.len;
  int copyLen = MINOF(colValLen, dataLen);
  str_cpy_all((char *)colVal, row->value.val, copyLen);

  if (dataLen > colValLen)
    colValLen = dataLen;
  else
    colValLen = copyLen;
  return HTC_OK;
}

HTC_RetCode MemoryTableClient::getRowID(HbaseStr &rowID) {
  HTableRow *row = kvArray_[currentRowNum_];

  rowID.len = row->key->length();
  str_cpy_all(rowID.val, row->key->data(), rowID.len);

  return HTC_OK;
}

int MemoryTableClient::scanOpen(const char *tableName, const Text &startRow, const Text &stopRow,
                                  const int numCacheRows) {
  numReqRows_ = numCacheRows;
  currentRowNum_ = -1;
  if (startRow.size() == 0)  //<min>
  {
    fetchStartPos_ = 0;
  } else {
    setFetchPos(startRow);
  }
  stopRow_ = stopRow;

  return 0;
}

void MemoryTableClient::setFetchPos(const Text &startRow) { fetchStartPos_ = htableCache_->getStartPos(startRow); }

HTC_RetCode MemoryTableClient::fetchRows() {
  numRowsReturned_ = htableCache_->fetchRows(numReqRows_, fetchStartPos_, kvArray_, stopRow_);
  return HTC_OK;
}
