#ifndef MEMORY_TABLE_DB_H
#define MEMORY_TABLE_DB_H

// -*-C++-*-
// ***************************************************************************
//
// File:         MemoryTableDB.h
// Description:  
//               
//               
// Created:      9/2/2021
// Language:     C++
//
//
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
//
//
// ***************************************************************************
//

#include <string>
#include "common/NAMemory.h"
#include "export/NABasicObject.h"
#include "common/Collections.h"
#include "exp/ExpHbaseDefs.h"
#include "export/NAStringDef.h"
#include "cli/sqlcli.h"
#include "qmscommon/QRLogger.h"

typedef std::string Text;

class MemoryTableDB;
class HTableCache;
class HTableRow;

class MemoryTableDB : public NABasicObject {
public:
  MemoryTableDB(NAMemory *heap);
  NAMemory* getHeap() { return heap_; }

  void insert(char *tableName);
  void remove(const char *tableName);
  HTableCache* getHTableCache(char *tableName);

  NAHashDictionary<NAString, HTableCache> * tableNameToCacheEntryMap() { return tableNameToCacheEntryMap_; }

  int entries() { return tableNameToCacheEntryMap_->entries(); }
  int entriesEnabled() { return tableNameToCacheEntryMap_->entriesEnabled(); }

  bool contains(char *tableName);

private:
  NAHashDictionary<NAString, HTableCache> *tableNameToCacheEntryMap_;
  NAMemory* heap_;
};

class HTableRow : public NABasicObject {
public:
  HTableRow() = default;
  HTableRow(NAMemory *heap)
   :heap_(heap) {
     //QRINFO("NAMemory::trace : HTableRow::HTableRow heap_:%p, this:%p called", heap_, this);
     key = NULL;
   }
  ~HTableRow();

  long size() { return (key?key->length():0) + value.len; }

  NAMemory *heap_;
  NAString *key;
  HbaseStr value; 
};

class HTableCache : public NABasicObject {
public:
  HTableCache(NAMemory *heap);
  HTableCache() = default;
  ~HTableCache();
  NAMemory* getHeap() { return heap_; }
  bool insert(HbaseStr& rowID, HbaseStr& colValue);
  Int32 startGet(const HbaseStr& rowID, NAList<HTableRow*> &kvArray);
  Int32 startGets(const NAList<HbaseStr> *rowIDs, NAList<HTableRow*> &kvArray);
  Int32 startGets(const HbaseStr& rowIDs, const UInt32 keyLen, NAList<HTableRow*> &kvArray);
  Int32 getStartPos(const Text& startRow);
  //NABoolean isLargerOrEqual(const HbaseStr &key1, const Text &key2);
  NABoolean isLargerOrEqual(const NAString *key1, const Text &key2);
  Int32 fetchRows(Int32 numReqRows, Int32 &fetchStartPos,
                  NAList<HTableRow*> &kvArray,
                  const Text& stopRow);


  // recode the date size stored in cache
  template<iteratorEntryType Type>
  pair<long, long> tableSize() {
    long totalSize = 0;
    //long rowCount = 0;
    NAHashDictionaryIteratorNoCopy<NAString, HTableRow> 
       itorForAll(*keyValueMap_, Type);
    
    NAString* key = NULL;
    HTableRow* value = NULL;
    itorForAll.getNext(key, value);
    while(key) {
      if ( value ) {
        totalSize += value->size();
        //rowCount++;
      }
      itorForAll.getNext(key, value);
    }
    return pair<long, long>(totalSize, table_ ? table_->entries() : 0);
  }

private:
  NAMemory* heap_;
  NAHashDictionary<NAString, HTableRow> *keyValueMap_;
  NAArray<HTableRow*> *table_;
};

#endif

