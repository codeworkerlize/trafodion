/**********************************************************************
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
**********************************************************************/
/* -*-C++-*-
**************************************************************************
*
* File:         testCollections.cpp
* Description:  functions to test Collection type templates
* Created:      01/09/2020
* Language:     C++
*
**************************************************************************
*/
#include "common/Collections.h"

// return # of failed. 0 means pass.
Int32 getAll(NAHashDictionaryIteratorNoCopy<Int32, Int32> &itor) {
  Int32 *key = NULL;
  Int32 *value = NULL;
  int ct = 0;
  int matched = 0;
  while (itor.getNext(key, value)) {
#if 0
    cout << "key=" << *key 
         << ", value=" << *value
         << endl;
#endif

    if (*key == *value) matched++;

    ct++;
  }

  cout << "Itor: entries=" << ct << ", retrieved=" << ct << ", match count=" << matched << endl;

  return (ct == matched) ? 0 : 1;
}

// return # of failed. 0 means pass.
Int32 testHashDictionaryItorNoCopyFull(NAHashDictionary<Int32, Int32> &table) {
  NAHashDictionaryIteratorNoCopy<Int32, Int32> itor(table, iteratorEntryType::EVERYTHING, NULL, NULL, NULL, hashFunc);

  return getAll(itor);
}

Int32 testHashDictionaryItorNoCopyFullSubset(NAHashDictionary<Int32, Int32> &table) {
  Int32 *key = new Int32(1);
  NAHashDictionaryIteratorNoCopy<Int32, Int32> itorSubset(table, iteratorEntryType::EVERYTHING, key, NULL, NULL,
                                                          hashFunc);

  return getAll(itorSubset);
}

void testHashDirectionaryIteratorNoCopy() {
  NAHashDictionary<Int32, Int32> table(hashFunc);

  Int32 *key = NULL;
  Int32 *value = NULL;
  for (int i = 0; i < 100; i++) {
    key = new Int32(i);
    value = new Int32(i);
    table.insert(key, value, hashFunc);
  }

  Int32 pass = testHashDictionaryItorNoCopyFull(table);
  pass += testHashDictionaryItorNoCopyFullSubset(table);

  if (pass == 0)
    cout << "pass";
  else
    cout << "failed: #passed test=" << pass;

  cout << endl;
}

typedef NAHashBucket<int, int> HashBucketType;

// return # of fails
int testClear(HashBucketType &bucket, int &key) {
  bucket.simulateBadIndex(FALSE);
  bucket.clear(FALSE);

  int failed = (bucket.isComplete()) ? 0 : 1;

  bucket.simulateBadIndex(TRUE);
  bucket.clear(FALSE);

  bucket.simulateBadIndex(FALSE);

  return failed + (bucket.isComplete()) ? 0 : 1;
}

int testContains(HashBucketType &bucket, int &key) {
  bucket.simulateBadIndex(FALSE);
  NABoolean ok = bucket.contains(&key);

  int failed = (ok && bucket.isComplete()) ? 0 : 1;

  bucket.simulateBadIndex(TRUE);
  ok = bucket.contains(&key);

  bucket.simulateBadIndex(FALSE);

  return failed + (!ok && bucket.isComplete()) ? 0 : 1;
}

int testContainsConst(HashBucketType &bucket, int &key) {
  bucket.simulateBadIndex(FALSE);
  NABoolean ok = bucket.contains(&key);

  int failed = (ok && bucket.isComplete()) ? 0 : 1;

  bucket.simulateBadIndex(TRUE);

  const HashBucketType &bucketConst = bucket;
  ok = bucketConst.contains(&key);

  bucket.simulateBadIndex(FALSE);

  return failed + (!ok && bucketConst.isComplete()) ? 0 : 1;
}

// return # of fails
int testGetFirstValue(HashBucketType &bucket, int &key) {
  bucket.simulateBadIndex(FALSE);
  int *validValue = bucket.getFirstValue(&key);

  int failed = (validValue && bucket.isComplete()) ? 0 : 1;

  bucket.simulateBadIndex(TRUE);
  int *nullValue = bucket.getFirstValue(&key);

  bucket.simulateBadIndex(FALSE);

  return failed + (!nullValue && bucket.isComplete()) ? 0 : 1;
}

// return # of fails
int testGetFirstValueAll(HashBucketType &bucket, int &key) {
  bucket.simulateBadIndex(FALSE);
  int *validValue = bucket.getFirstValueAll(&key);

  int failed = (validValue && bucket.isComplete()) ? 0 : 1;

  bucket.simulateBadIndex(TRUE);
  int *nullValue = bucket.getFirstValueAll(&key);

  bucket.simulateBadIndex(FALSE);

  return failed + (!nullValue && bucket.isComplete()) ? 0 : 1;
}

// return # of fails
int testGetKeyValuePair(NAHeap *heap, HashBucketType &bucket, int &key, int &value) {
  bucket.simulateBadIndex(FALSE);

  HashBucketType container(heap);

  // test key!=NULL case
  NABoolean inserted = FALSE;
  bucket.getKeyValuePair(&key, &value, container, &inserted);

  int failed = (inserted && bucket.isComplete()) ? 0 : 1;

  inserted = FALSE;
  bucket.simulateBadIndex(TRUE);
  bucket.getKeyValuePair(&key, &value, container, &inserted);

  failed += (!inserted && bucket.isComplete()) ? 0 : 1;

  // test key==NULL case
  inserted = FALSE;
  bucket.getKeyValuePair(NULL, &value, container, &inserted);

  failed += (inserted && bucket.isComplete()) ? 0 : 1;

  inserted = FALSE;
  bucket.simulateBadIndex(TRUE);
  bucket.getKeyValuePair(NULL, &value, container, &inserted);

  bucket.simulateBadIndex(FALSE);

  return failed + (!inserted && bucket.isComplete()) ? 0 : 1;
}

// return # of fails
int testRemove(HashBucketType &bucket, int &key) {
  bucket.simulateBadIndex(TRUE);

  int entriesEnabled = 0;
  int *removedKey = bucket.remove(&key, entriesEnabled);

  int failed = (!removedKey && bucket.isComplete()) ? 0 : 1;

  bucket.simulateBadIndex(FALSE);
  removedKey = bucket.remove(&key, entriesEnabled);

  bucket.simulateBadIndex(FALSE);

  return failed + (removedKey && bucket.isComplete()) ? 0 : 1;
}

// return # of fails
int testEnable(HashBucketType &bucket, int &key) {
  bucket.simulateBadIndex(FALSE);

  int entriesEnabled = 0;
  int *keyEnabled = bucket.enable(&key, TRUE, entriesEnabled);

  int failed = (keyEnabled && bucket.isComplete()) ? 0 : 1;

  bucket.simulateBadIndex(TRUE);
  keyEnabled = bucket.enable(&key, TRUE, entriesEnabled);

  bucket.simulateBadIndex(FALSE);

  return failed + (!keyEnabled && bucket.isComplete()) ? 0 : 1;
}

// return # of fails
int testDisable(HashBucketType &bucket, int &key) {
  bucket.simulateBadIndex(FALSE);

  int entriesEnabled = 0;
  int *keyEnabled = bucket.disable(&key, TRUE, entriesEnabled);

  int failed = (keyEnabled && bucket.isComplete()) ? 0 : 1;

  bucket.simulateBadIndex(TRUE);
  keyEnabled = bucket.disable(&key, TRUE, entriesEnabled);

  bucket.simulateBadIndex(FALSE);

  return failed + (!keyEnabled && bucket.isComplete()) ? 0 : 1;
}

#include "sqlcomp/SharedCache.h"
// test hash bucket failure is not fatal
int testHashBucketFailureNotFatal() {
#if 0
  // The use of the shared heap should only be done on a workstation
  // as it will destroy the content of the shared cache.
  WaitedLockController controller(SHARED_CACHE_DLOCK_KEY, 0);
  NAHeap* heap = (NAHeap*)SharedCacheDB::makeSharedHeap(&controller, NULL);
#endif

  NAHeap *heap = new NAHeap("simulated_heap", NAMemory::DERIVED_FROM_SYS_HEAP, 524288, 0);

  HashBucketType bucket(heap, FALSE);

  int keys[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  int values[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

  // insert some key-values into the bucket
  for (int i = 0; i < sizeof(keys); i++) {
    bucket.insert(&keys[i], &values[i]);
  }

  int failed = 0;

  // test each method of HashBucketType where an element
  // in the bucket_ array can be undefined. For each test,
  // we expect such an undefined event is handled properly
  // (failed == 0).
  failed += testContains(bucket, keys[1]);
  failed += testContainsConst(bucket, keys[1]);
  failed += testGetFirstValue(bucket, keys[1]);
  failed += testGetFirstValueAll(bucket, keys[1]);
  failed += testGetKeyValuePair(heap, bucket, keys[1], values[1]);
  failed += testEnable(bucket, keys[1]);
  failed += testDisable(bucket, keys[1]);

  // These two tests below remove elements from the bucket and are
  // arranged in this particular order to make sure both work.
  // Do not change the order.
  failed += testRemove(bucket, keys[1]);
  failed += testClear(bucket, keys[1]);

  return failed;
}

void testHashBucket() {
  int ok = testHashBucketFailureNotFatal();

  if (ok == 0)
    cout << "Pass.";
  else
    cout << "Failed: with " << ok << " failuires.";

  cout << endl;
}
