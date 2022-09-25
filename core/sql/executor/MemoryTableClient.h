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
#ifndef MEMORY_TABLE_CLIENT_H
#define MEMORY_TABLE_CLIENT_H

#include "MemoryTableDB.h"
#include "NAMemory.h"
#include "ExpHbaseDefs.h"
#include "HBaseClient_JNI.h"

class MemoryTableClient : public NABasicObject {
public:

  MemoryTableClient(NAMemory *heap, char* tableName);

  void setTableName(const char* tableName);
  void create();
  bool insert(HbaseStr &key, HbaseStr &value);
  void remove(const char* tableName);
  HTableCache *getHTableCache() { return htableCache_; }

  enum FETCH_MODE {
      UNKNOWN = 0
    , SCAN_FETCH
    , GET_ROW
    , BATCH_GET
  };

  void setFetchMode(MemoryTableClient::FETCH_MODE fetchMode)
  { fetchMode_ = fetchMode; }

  Lng32 startGet(const char* tableName,
                 const HbaseStr& rowID);

  Lng32 startGets(const char* tableName,
                  const NAList<HbaseStr> *rowIDs);

  Lng32 startGets(const char* tableName,
                  const HbaseStr &rowIDs,
                  const UInt32 keyLen);

  HTC_RetCode getColVal(BYTE *colVal, Lng32 &colValLen);
  HTC_RetCode nextRow();
  HTC_RetCode getRowID(HbaseStr &rowID);
  void cleanupResultInfo();
  Lng32 scanOpen(const char* tableName, const Text& startRow,
                 const Text& stopRow, const Lng32 numCacheRows);
  void setFetchPos(const Text& startRow);
  HTC_RetCode fetchRows();

  inline bool tableIsDisabled() { return isDisabled_; }
  inline bool memDBinitFailed() { return memDBinitFailed_; }
  NAMemory *getNAMemory() { return heap_; }
private:
  void init(char* tableName);
  NAMemory *heap_;
  char* tableName_;
  HTableCache *htableCache_;
  bool isDisabled_;
  NAList <HTableRow*> kvArray_; 
  Int32 numReqRows_;
  Int32 currentRowNum_;
  Int32 numRowsReturned_;
  Int32 fetchStartPos_;
  Text stopRow_;
  FETCH_MODE fetchMode_;
  bool memDBinitFailed_;
};

#endif
