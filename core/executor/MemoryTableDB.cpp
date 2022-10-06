// -*-C++-*-
// *********************************************************************
//
// File:         MemoryTableDB.cpp
// Description:
//
//
// Created:      9/02/2021
// Language:     C++
//
//

// *********************************************************************

#include "executor/MemoryTableDB.h"
#include "cli/Globals.h"
#include <sys/time.h>
#include "sqlcomp/SharedCache.h"

int NAStringHashFunc(const NAString &x) { return x.hash(); }

void fixupVTable(HTableCache *targetTableCache) {
  static HTableCache tableCache;

  if (targetTableCache) memcpy((char *)targetTableCache, (char *)&tableCache, sizeof(void *));
}

void fixupVTable(HTableRow *targetTableRow) {
  static HTableRow tableRow;

  if (targetTableRow) memcpy((char *)targetTableRow, (char *)&tableRow, sizeof(void *));
}

/**************************************************************
 * member function for class MemoryTableDB
 * ************************************************************/
MemoryTableDB::MemoryTableDB(NAMemory *heap) {
  heap_ = heap;
  tableNameToCacheEntryMap_ =
      new (heap_) NAHashDictionary<NAString, HTableCache>(NAStringHashFunc, 101, TRUE, heap_, TRUE, TRUE);
}

bool MemoryTableDB::contains(char *tableName) {
  NAString tName(tableName);
  return tableNameToCacheEntryMap_->contains(&tName, NAStringHashFunc, true);
}

void MemoryTableDB::insert(char *tableName) {
  NAString *tName = new (heap_) NAString(tableName, heap_);
  HTableCache *hTableCache = new (heap_) HTableCache(heap_);
  QRINFO("NAMemory::trace : MemoryTableDB::insert %s %p, %p", tableName, tName, hTableCache);
  tableNameToCacheEntryMap_->insert(tName, hTableCache, NAStringHashFunc);
}

void MemoryTableDB::remove(const char *tableName) {
  NAString tName(tableName);
  QRINFO("NAMemory::trace : MemoryTableDB::remove %s", tableName);
  tableNameToCacheEntryMap_->remove(&tName, NAStringHashFunc, true);
}

HTableCache *MemoryTableDB::getHTableCache(char *tableName) {
  NAString tName(tableName);
  return tableNameToCacheEntryMap_->getFirstValue(&tName, NAStringHashFunc);
}

/**************************************************************
 * member function for class HTableCache
 * ************************************************************/
HTableCache::HTableCache(NAMemory *heap) : heap_(heap) {
  // QRINFO("NAMemory::trace : HTableCache::HTableCache heap_:%p, this:%p called", heap_, this);
  keyValueMap_ = new (heap_) NAHashDictionary<NAString, HTableRow>(NAStringHashFunc, 502, TRUE, heap_, TRUE, TRUE);
  table_ = new (heap_) NAArray<HTableRow *>(heap_);
  // QRINFO("NAMemory::trace : HTableCache::HTableCache heap_:%p, this:%p finish", heap_, this);
}

bool HTableCache::insert(HbaseStr &rowID, HbaseStr &colValue) {
  if (((double)heap_->getAllocSize() / heap_->getTotalSize()) > 0.9) return false;

  HTableRow *row = new (heap_) HTableRow(heap_);

  row->value.val = new (heap_) char[colValue.len];
  memcpy(row->value.val, colValue.val, colValue.len);
  row->value.len = colValue.len;

  // table_->append(row);
  table_->insertAt(table_->entries(), row);

  NAString *key = new (heap_) NAString(rowID.val, rowID.len, heap_);

  row->key = key;

  // QRINFO("NAMemory::trace : HTableCache::insert %p, %p", key, row);
  keyValueMap_->insert(key, row, NAStringHashFunc);

  return true;
}

int HTableCache::startGet(const HbaseStr &rowID, NAList<HTableRow *> &kvArray) {
  NAString key(rowID.val, rowID.len);
  int rowsReturn = -1;

  HTableRow *row = keyValueMap_->getFirstValue(&key, NAStringHashFunc);
  if (row) {
    kvArray.append(row);
    rowsReturn = 1;
  }
  return rowsReturn;
}

int HTableCache::startGets(const NAList<HbaseStr> *rowIDs, NAList<HTableRow *> &kvArray) {
  if (rowIDs->entries() <= 0) return -1;

  int rowsReturn = 0;
  for (int i = 0; i < rowIDs->entries(); i++) {
    NAString key((*rowIDs)[i].val, (*rowIDs)[i].len);

    HTableRow *row = keyValueMap_->getFirstValue(&key, NAStringHashFunc);
    if (row) {
      kvArray.append(row);
      rowsReturn++;
    }
  }
  if (kvArray.entries() == 0) rowsReturn = -1;

  return rowsReturn;
}

int HTableCache::startGets(const HbaseStr &rowIDs, const UInt32 keyLen, NAList<HTableRow *> &kvArray) {
  short numReqRows = *(short *)(&rowIDs)->val;
  short entries = bswap_16(numReqRows);

  if (entries <= 0) return -1;

  // see ExHbaseAccessTcb::copyRowIDToDirectBuffer
  int rowsReturn = 0;
  char *rowId = rowIDs.val;
  rowId += sizeof(short);
  UInt32 rowIdLen = 0;
  char target[keyLen + 1];

  for (short i = 0; i < entries; i++) {
    if (rowId[0] == '0')
      rowIdLen = keyLen;
    else if (rowId[0] == '1')
      rowIdLen = keyLen + 1;
    rowId++;
    memcpy(target, rowId, rowIdLen);
    target[rowIdLen] = '\0';
    NAString key(target, rowIdLen);

    HTableRow *row = keyValueMap_->getFirstValue(&key, NAStringHashFunc);
    kvArray.append(row);
    rowsReturn++;
    rowId += rowIdLen;
  }
  if (kvArray.entries() == 0) rowsReturn = -1;

  return rowsReturn;
}

/*int compareKeys(const HbaseStr &key1, const Text &key2)
{
  NABoolean isLargerEqual = false;
  int len1 = key1.len, len2 = key2.size();
  int cmpLen = len1 > len2 ? len2 : len1;
  int compare_code = memcmp(key1.val, key2.data(), cmpLen);
  if ((compare_code == 0) && (len1 != len2))
  {
    if (len1 > len2)
      compare_code = 1;
    else
      compare_code = -1;
  }
  return compare_code;
}*/

int compareKeys(const NAString *key1, const Text &key2) {
  NABoolean isLargerEqual = false;
  int len1 = key1->length(), len2 = key2.size();
  int cmpLen = len1 > len2 ? len2 : len1;
  int compare_code = memcmp(key1->data(), key2.data(), cmpLen);
  if ((compare_code == 0) && (len1 != len2)) {
    if (len1 > len2)
      compare_code = 1;
    else
      compare_code = -1;
  }
  return compare_code;
}

int HTableCache::getStartPos(const Text &startRow) {
  if (startRow.size() == 0 || startRow.data() == NULL) return 0;

  if (table_->entries() == 0) return 0;

  int low = 0;
  int high = table_->entries() - 1;
  if (compareKeys((*table_)[high]->key, startRow) < 0) return -1;

  while (high != low) {
    int middle = (high + low) / 2;
    HTableRow *row = (*table_)[middle];
    int temp = compareKeys(row->key, startRow);
    if (temp == 0)
      return middle;
    else if (temp > 0)
      high = middle;
    else
      low = middle + 1;
  }
  return low;
}

/*NABoolean HTableCache::isLargerOrEqual(const HbaseStr &key1, const Text &key2)
{
  if (compareKeys(key1, key2) >= 0)
    return TRUE;
  else
    return FALSE;
}*/

NABoolean HTableCache::isLargerOrEqual(const NAString *key1, const Text &key2) {
  if (compareKeys(key1, key2) >= 0)
    return TRUE;
  else
    return FALSE;
}

int HTableCache::fetchRows(int numReqRows, int &fetchStartPos, NAList<HTableRow *> &kvArray,
                             const Text &stopRow) {
  int rowNum = 0;
  bool toTheEnd = false;
  if (stopRow.size() == 0 || stopRow.data() == NULL) toTheEnd = true;
  int start = fetchStartPos;
  kvArray.clear();

  for (int idx = start; idx < table_->entries() && rowNum < numReqRows; idx++) {
    HTableRow *row = (*table_)[idx];
    if (toTheEnd == false && isLargerOrEqual(row->key, stopRow)) break;

    kvArray.append(row);
    rowNum++;
    fetchStartPos++;
  }
  return rowNum;
}

HTableCache::~HTableCache() {
  // QRINFO("NAMemory::trace: HTableCache::~HTableCache %p called, ", this);
  if (keyValueMap_) {
    keyValueMap_->clearAll();
    NADELETEBASIC(keyValueMap_, heap_);
    // NAHashDictionary<NAString, HTableRow>::fixupMyVTable(keyValueMap_);
    // delete keyValueMap_;
  }

  if (table_) {
    table_->clear();
    NAArray<HTableRow *>::fixupMyVTable(table_);
    delete table_;
  }
}

/**************************************************************
 * member function for class HTableRow
 * ************************************************************/
HTableRow::~HTableRow() {
  // QRINFO("NAMemory::trace: HTableRow::~HTableRow %p called, ", this);

  key = NULL;

  if (value.val) {
    NADELETEBASIC(value.val, collHeap());
    value.val = NULL;
  }
  value.len = 0;
}
