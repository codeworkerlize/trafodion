// @@@ START COPYRIGHT @@@
//
// (c) Copyright 2019 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         SharedCache.cpp
 * Description:
 * Created:      6/05/2019
 * Language:     C++
 *
 *****************************************************************************
 */
#include "sqlcomp/SharedCache.h"
#include "common/str.h"
#include "comexe/ComQueue.h"
#include "executor/ExExeUtilCli.h"
#include "cli/Globals.h"
#include "arkcmp/CmpContext.h"
#include "sqlcomp/CmpSeabaseDDL.h"
#include "sqlcomp/NamedSemaphore.h"
#include "SharedSegment.h"

ULng32 NAStringHashFunc(const NAString &x) { return x.hash(); }

// The singular default shared segment object living in each SQL
// process.
DefaultSharedSegment defaultDescSharedSegment;
DefaultSharedSegment defaultDataSharedSegment;

static DefaultSharedSegment &defaultSharedSegment(bool isDataCache) {
  if (isDataCache)
    return defaultDataSharedSegment;
  else
    return defaultDescSharedSegment;
}

// A cstr to make a brand new SharedCacheDB
SharedCacheDB::SharedCacheDB(const char eyeCatcher[4], NAMemory *sharedHeap, bool isData)
    : sizeOfSharedHeap_(sizeof(*sharedHeap)), sharedHeap_(sharedHeap) {
  strncpy(eyeCatcher_, eyeCatcher, 4);

  for (int i = 0; i < MAX_SHARED_CACHES; i++) sharedCaches_[i] = NULL;
  sizeOfDefaultSegment_ = defaultSharedSegment(isData).getSize();

  sharedSegmentDB_.init();
}

// static
char *SharedCacheDB::locateBaseAddr(int &shmId, SharedCacheDB::CacheID cacheId) {
  char *baseAddr = NULL;

#ifdef USE_RMS_SHARED_SEGMENT_FOR_SHARED_CACHE
  // find my segment id which will be used as a key to
  // access the shared segment
  StatsGlobals *statsGlobals = (StatsGlobals *)shareStatsSegment(shmId);

  if (!statsGlobals) return NULL;

  // the base address of the shared segment
  baseAddr = (char *)(statsGlobals->getStatsSharedSegAddr());

  long rmsSegSize = computeSegmentSizeForRMSProper();

  // Assume that there is an extra memory span toward the end
  // of the shared segment. The length of this extra memory span
  // is included in the total size for the RMS shared segment
  // in ex_sscp_main.cpp.
  baseAddr += rmsSegSize;
#else

  if (cacheId == 1) defaultSharedSegment(true).setIsDataCache();

  defaultSharedSegment(cacheId == 1).init();

#if 0
  defaultSharedSegment(cacheId == 1).display("Default shared segment inited:");
#endif

  assert(defaultSharedSegment(cacheId == 1).isShmidValid());

  baseAddr = (char *)defaultSharedSegment(cacheId == 1).getAddr();
  shmId = defaultSharedSegment(cacheId == 1).getShmid();

#endif

  return baseAddr;
}

// static.
char *SharedCacheDB::collectSharedHeapInfo(int &shmId, size_t &totalHeapSize, SharedCacheDB::CacheID cacheId) {
  char *baseAddr = SharedCacheDB::locateBaseAddr(shmId, cacheId);

  int sz = sizeof(SharedCacheDB);

  size_t size = defaultSharedSegment(cacheId == 1).getSize();

  // Test if the shared cache segment is too small.
  if (size <= sz + sizeof(NAHeap)) return NULL;

  totalHeapSize = size - sz - sizeof(NAHeap);
  baseAddr += sz;

  return baseAddr;
}

// Create the shared heap object. The passed in semaphore provides
// the protection during the allocation of memory from the
// shared segment.
// static
NAMemory *SharedCacheDB::makeSharedHeap(NamedSemaphore *semaphore, SharedCacheDB::CacheID cacheId) {
  if (!semaphore)  // Must have a non-NULL semaphore
                   // ptr to make a shared heap.
    return NULL;

  int shmId = 0;
  size_t totalHeapSize = 0;

  char *sharedHeapAddr = SharedCacheDB::collectSharedHeapInfo(shmId, totalHeapSize, cacheId);

  if (!sharedHeapAddr) return NULL;

  // create NAHeap out of the shared segment
  NAMemory *sharedHeap = new (sharedHeapAddr) NAHeap("meta-data shared segment", shmId,
                                                     sharedHeapAddr,  // the base addr.
                                                     sizeof(NAHeap),  // malloc starts after the
                                                                      // the NAHeap object
                                                     totalHeapSize,
                                                     semaphore,  // must pass in the semaphore to assure
                                                                 // the semaphore  protection exists
                                                     FALSE       // failure is not fatal
  );

  sharedHeap->useSemaphore();

  return sharedHeap;
}

// Create the shared heap object. The passed in controller provides
// the protection during the allocation of memory from the
// shared segment.
// static
NAMemory *SharedCacheDB::makeSharedHeap(LockController *controller, SharedSegmentDB *segmentDB,
                                        SharedCacheDB::CacheID cacheId) {
  if (!controller)  // Must have a non-NULL semaphore
                    // ptr to make a shared heap.
    return NULL;

  int shmId = 0;
  size_t totalHeapSize = 0;

  char *sharedHeapAddr = SharedCacheDB::collectSharedHeapInfo(shmId, totalHeapSize, cacheId);

  if (!sharedHeapAddr) return NULL;

  // create NAHeap out of the shared segment
  NAMemory *sharedHeap = new (sharedHeapAddr) NAHeap("meta-data shared segment", shmId,
                                                     sharedHeapAddr,  // the base addr.
                                                     sizeof(NAHeap),  // malloc starts after the
                                                                      // the NAHeap object
                                                     totalHeapSize,
                                                     controller,  // to assure a distributed lock
                                                                  // protection exists
                                                     FALSE,       // failure is not fatal
                                                     segmentDB);

  sharedHeap->useSemaphore();

  return sharedHeap;
}

// Locate an existing sharedCacheDB object.
// static
SharedCacheDB *SharedCacheDB::locate(char *&baseAddr, SharedCacheDB::CacheID cacheId) {
  int shmId = 0;
  baseAddr = locateBaseAddr(shmId, cacheId);

  if (!baseAddr) return NULL;

  SharedCacheDB *sharedCacheDB = (SharedCacheDB *)(baseAddr);

  if (sharedCacheDB->isMe()) {
    return sharedCacheDB;
  }

  return NULL;
}

SharedCacheDB *SharedCacheDB::locate(SharedCacheDB::CacheID cacheId) {
  char *baseAddr = NULL;
  return locate(baseAddr, cacheId);
}

// locate or create a brand new sharedCacheDB object.
SharedCacheDB *SharedCacheDB::locateOrCreate(NamedSemaphore *semaphore, SharedCacheDB::CacheID cacheId) {
  char *baseAddr = NULL;
  SharedCacheDB *sharedCacheDB = locate(baseAddr, cacheId);

  if (sharedCacheDB) {
    // If the shared cache DB is already in existence, we need to
    // set the semaphore (not NULL) in the shared heap, for the
    // reason that the caller likes to make changes to the heap via
    // allocate/deallocate.
    //
    // If the semaphore is NULL, then we should not set it because
    // that would prevent the heap from allocate/deallocate properly
    // in a writer of the heap, for the following sequence of calls.
    //
    //  {
    //    // writer
    //    NamedSemaphore semaphore;
    //    cacheDB0 = locateOrCreate(&semaphore); // create it
    //
    //    // reader
    //    cacheDB1 = locate(NULL);  // use it via a different name
    //
    //    // update cacheDB0
    //  }
    if (semaphore) sharedCacheDB->getSharedHeap()->setNamedSemaphore(semaphore);

    return sharedCacheDB;
  }

  // If we reach here, we need to create a brand new SharedCacheDB first.
  sharedCacheDB = new (baseAddr) SharedCacheDB(SHAREDCACHE_DB_EYE_CATCHER, NULL, (cacheId == 1));

  // Then we need to make a brand new shared heap. The heap will remember
  // the semaphore.
  sharedCacheDB->sharedHeap_ = makeSharedHeap(semaphore, cacheId);

  return sharedCacheDB;
}

// locate or create a brand new sharedCacheDB object.
SharedCacheDB *SharedCacheDB::locateOrCreate(LockController *controller, SharedCacheDB::CacheID cacheId) {
  char *baseAddr = NULL;
  SharedCacheDB *sharedCacheDB = locate(baseAddr, cacheId);

  if (sharedCacheDB) {
    // If the shared cache DB is already in existence, we need to
    // set the controller (not NULL) in the shared heap, for the
    // reason that the caller likes to make changes to the heap via
    // allocate/deallocate.
    //
    // If the controller is NULL, then we should not set it because
    // that would prevent the heap from allocate/deallocate properly
    // in a writer of the heap, for the following sequence of calls.
    //
    //  {
    //    // writer
    //    'LockController or its subclass' controller;
    //    cacheDB0 = locateOrCreate(&controller); // create it
    //
    //    // reader
    //    cacheDB1 = locate(NULL);  // use it via a different name
    //
    //    // update cacheDB0
    //  }
    if (controller) {
      sharedCacheDB->getSharedHeap()->setLockController(controller);
      QRLogger::log(CAT_SQL_SHARED_CACHE, LL_TRACE, "setLockController %p\n", controller);
    }

    return sharedCacheDB;
  }

  // If we reach here, we need to create a brand new SharedCacheDB first.
  sharedCacheDB = new (baseAddr) SharedCacheDB(SHAREDCACHE_DB_EYE_CATCHER, NULL, (cacheId == 1));

  // Then we need to make a brand new shared heap. The heap will remember
  // the controller.
  sharedCacheDB->sharedHeap_ = makeSharedHeap(controller, sharedCacheDB->getSharedSegmentDB(), cacheId);

  return sharedCacheDB;
}

SharedCache *SharedCacheDB::get(SharedCacheDB::CacheID cacheId) {
  if (cacheId < 0 || cacheId >= MAX_SHARED_CACHES) return NULL;

  return sharedCaches_[cacheId];
}

void SharedCacheDB::set(SharedCacheDB::CacheID cacheId, SharedCache *ptr) {
  if (cacheId >= 0 && cacheId < MAX_SHARED_CACHES) sharedCaches_[cacheId] = ptr;
}

void SharedCacheDB::display(const char *msg, SharedCacheDB::CacheID cid) {
  if (msg) cout << msg << endl;

  SharedCacheDB *db = locate(cid);
  if (db) {
    cout << "SharedCacheDB: address=" << static_cast<void *>(db)
         << ", Type:" << (cid == SharedCacheDB::DESCRIPTOR_CACHE ? "DESCRIPTOR_CACHE" : "TABLEDATA_CACHE")
         << ", sharedHeap:"
         << " address=" << static_cast<void *>(db->getSharedHeap())
         << ", allocSize=" << db->getSharedHeap()->getAllocSize();
  } else
    cout << "Can't locate the SharedCacheDB.";

  cout << endl;
}

// ===========================
// SharedCache related methods
// ===========================
SharedCache::SharedCache(const char eyeCatcher[4], NAMemory *sharedHeap, SharedCacheDB *sharedCacheDB)
    : sharedHeap_(sharedHeap), sharedCacheDB_(sharedCacheDB) {
  strncpy(eyeCatcher_, eyeCatcher, 4);
}

char *SharedCache::createSemaphoreName(char *buf, int len) {
  int shmId = -1;
#ifdef USE_RMS_SHARED_SEGMENT_FOR_SHARED_CACHE
  StatsGlobals *statsGlobals = (StatsGlobals *)shareStatsSegment(shmId);

  if (statsGlobals) {
    snprintf(buf, len, "%s%d.%d", SHARED_CACHE_SEM_NAME_PREFIX, getuid(), shmId);
    return buf;
  }
#else
  shmId = SharedSegment::getKeyForDefaultSegment();
  snprintf(buf, len, "%s%d.%d", SHARED_CACHE_SEM_NAME_PREFIX, getuid(), shmId);
  return buf;
#endif
  return NULL;
}

ULng32 QualifiedNameHashFunc(const QualifiedName &x) { return x.hash(); }

// ======================================================
// SharedDescriptor Cache related methods
// ======================================================
SharedDescriptorCache::SharedDescriptorCache(NAMemory *sharedHeap, SharedCacheDB *sharedCacheDB)
    : SharedCache(DESCRIPTOR_CACHE_EYE_CATCHER, sharedHeap, sharedCacheDB), table_(NULL), sizeOfTable_(0) {
  if (sharedHeap) {
    table_ = new (sharedHeap_, FALSE /*failure is not fatal*/)
        NAHashDictionary<QualifiedName, NAString>(QualifiedNameHashFunc, NAHashDictionary_Default_Size,
                                                  TRUE,  // enforce uniqueness
                                                  sharedHeap_,
                                                  FALSE  // failure is not fatal
        );

    if (IsComplete(table_)) sizeOfTable_ = sizeof(*table_);
  }
}

// Make a shared descriptor cache
SharedDescriptorCache *SharedDescriptorCache::Make() {
  SharedDescriptorCache *sharedDescCache = NULL;

  // First get hold of the SharedCacheDB.
  SharedCacheDB *sharedCacheDB = SharedCacheDB::locate(SharedCacheDB::DESCRIPTOR_CACHE);

  if (sharedCacheDB) {
    // get the shared heap from the SharedCacheDB.
    NAMemory *sharedHeap = sharedCacheDB->getSharedHeap();

    // Create the cache on the shared heap.
    sharedDescCache = new (sharedHeap) SharedDescriptorCache(sharedHeap, sharedCacheDB);

    // Double check the embedded hash dictionary object, and if it is OK
    // memorize the cache object in SharedCacheDB. Later on, one can
    // locate the cache object through an Id-based lookup.
    if (sharedDescCache && IsComplete(sharedDescCache->table_))
      sharedCacheDB->set(SharedCacheDB::DESCRIPTOR_CACHE, sharedDescCache);
  }

  return sharedDescCache;
}

// Destroy a shared cache
NABoolean SharedCache::destroy(SharedCacheDB::CacheID cacheId) {
  NABoolean result = FALSE;
  // First get hold of the SharedCacheDB.
  SharedCacheDB *sharedCacheDB = SharedCacheDB::locate(cacheId);
  if (sharedCacheDB) {
    sharedCacheDB->set(cacheId, NULL);
    result = TRUE;
  }

  sharedCacheDB_ = NULL;

  return result;
}

// Locate a shared descriptor cache
SharedDescriptorCache *SharedDescriptorCache::locate() {
  SharedDescriptorCache *sharedDescCache = NULL;
  SharedCacheDB *sharedCacheDB = SharedCacheDB::locate();

  if (sharedCacheDB) {
    sharedDescCache = (SharedDescriptorCache *)(sharedCacheDB->get(SharedCacheDB::DESCRIPTOR_CACHE));
  }

  return (sharedDescCache && sharedDescCache->isMe()) ? sharedDescCache : NULL;
}

QualifiedName *SharedDescriptorCache::insert(QualifiedName *key, NAString *value, NABoolean *inserted) {
  QualifiedName *result = table_->insert(key, value, QualifiedNameHashFunc, inserted);

  NAString *check = find(*key);

  if (check)
    cout << "In SharedDescriptorCache::insert(), find the inserted value: this=" << static_cast<void *>(check)
         << ", data()'s address=" << static_cast<void *>((char *)(check->data())) << ", length=" << check->length()
         << endl;

  NAMemory *heap = getSharedHeap();
  QRLogger::log(CAT_SQL_SHARED_CACHE, LL_DEBUG,
                "SharedDescriptorCache::insert(): key=%s, value length=%d, inserted=%d, return=%p, heap sizes(bytes): "
                "total=%ld, allocated=%ld, sanity=%d",
                key->getQualifiedNameAsAnsiString().data(), value->length(), (inserted) ? *inserted : -1,
                static_cast<void *>(result), heap->getTotalSize(), heap->getAllocSize(), table_->sanityCheck());

  return result;
}

NAString *SharedDescriptorCache::find(const QualifiedName &name, NABoolean includeDisabled) {
  // No protection to the access of the table_, assuming that
  // the update is infrequent and the user makes sure the
  // update to table_ happens in a maintenance window in which
  // no read is possible.
  if (!table_) return NULL;

  // Log when the hash table is in bad state and return NULL.
  if (table_->sanityCheck() == FALSE) {
    NAMemory *heap = getSharedHeap();
    QRLogger::log(CAT_SQL_SHARED_CACHE, LL_DEBUG,
                  "SharedDescriptorCache::find(): key=%s, heap sizes(bytes): total=%ld, allocated=%ld, sanity=%d",
                  name.getQualifiedNameAsAnsiString().data(), heap->getTotalSize(), heap->getAllocSize(),
                  table_->sanityCheck());

    return NULL;
  }

  NAString *value = table_->getFirstValue(&name, QualifiedNameHashFunc);

  return value;
}

int SharedDescriptorCache::entries(const QualifiedName &inSchName) {
  // No protection to the access of the table_, assuming that
  // the update is infrequent and the user makes sure the
  // update to table_ happens in a maintenance window in which
  // no read is possible.
  if (!table_) return NULL;

  if (inSchName.getSchemaName().isNull()) return table_->entriesEnabled();

  int ct = 0;

  NAHashDictionaryIteratorNoCopy<QualifiedName, NAString> itor(*table_, iteratorEntryType::ENABLED);

  QualifiedName *key = NULL;
  NAString *value = NULL;

  itor.getNext(key, value);
  while (key) {
    if (inSchName.getSchemaName() == key->getSchemaName()) ct++;

    itor.getNext(key, value);
  }

  return ct;
}

int SharedDescriptorCache::entries() {
  QualifiedName pseudoSchemaName("", "", "");
  return entries(pseudoSchemaName);
}

int SharedTableDataCache::entries() {
  if (memoryTableDB_) {
    return memoryTableDB_->entriesEnabled();
  }
  return 0;
}

void SharedDescriptorCache::findAll() {
  if (!table_) return;

  int ctEnabled = 0;
  int ctDisabled = 0;
  long totalSize = 0;

  NAHashDictionaryIteratorNoCopy<QualifiedName, NAString> itorForEnabled(*table_, iteratorEntryType::ENABLED);

  QualifiedName *key = NULL;
  NAString *value = NULL;

  cout << "Getting enabled entries: " << endl;

  itorForEnabled.getNext(key, value);
  while (key) {
    ctEnabled++;

    cout << "key: this=" << static_cast<void *>(key) << ", SQL name=" << key->getQualifiedNameAsAnsiString().data()
         << ", size=" << key->getQualifiedNameAsAnsiString().length() << endl;

    cout << "value: this=" << static_cast<void *>(value)
         << ", data()'s address=" << static_cast<void *>((char *)(value->data())) << ", size=" << value->length()
         << endl;

    if (value) {
      totalSize += value->length();
    }

    itorForEnabled.getNext(key, value);
  }

  cout << "Getting disabled entries: " << endl;

  NAHashDictionaryIteratorNoCopy<QualifiedName, NAString> itorForDisabled(*table_, iteratorEntryType::DISABLED);

  itorForDisabled.getNext(key, value);
  while (key) {
    ctDisabled++;

    cout << "key: this=" << static_cast<void *>(key) << ", SQL name=" << key->getQualifiedNameAsAnsiString().data()
         << ", size=" << key->getQualifiedNameAsAnsiString().length() << endl;

    cout << "value: this=" << static_cast<void *>(value)
         << ", data()'s address=" << static_cast<void *>((char *)(value->data())) << ", size=" << value->length()
         << endl;

    if (value) {
      totalSize += value->length();
    }

    itorForDisabled.getNext(key, value);
  }

  float avg = ((ctEnabled + ctDisabled) > 0) ? (float)totalSize / (ctEnabled + ctDisabled) : 0;

  cout << "FindAll(): finding all pairs of (key, value) from hash table"
       << " Total enabled found=" << ctEnabled << " Total disabled found=" << ctDisabled
       << ", size in bytes: total=" << totalSize << ", avg=" << avg << endl;
}

void SharedTableDataCache::findAll() {
  if (!memoryTableDB_) return;

  int ctEnabled = 0;
  int ctDisabled = 0;
  long totalSize = 0;

  NAHashDictionaryIteratorNoCopy<NAString, HTableCache> itorForEnabled(*(memoryTableDB_->tableNameToCacheEntryMap()),
                                                                       iteratorEntryType::ENABLED);

  NAString *key = NULL;
  HTableCache *value = NULL;

  cout << "Getting enabled entries: " << endl;

  itorForEnabled.getNext(key, value);
  while (key) {
    ctEnabled++;

    cout << "key: this=" << static_cast<void *>(key) << ", SQL name=" << key->data() << ", size=" << key->length()
         << endl;

    if (value) {
      auto tableInfo = value->tableSize<iteratorEntryType::ENABLED>();
      cout << "value: this=" << static_cast<void *>(value)
           << ", data()'s address=" << static_cast<void *>((char *)(value)) << ", size=" << tableInfo.first
           << ", rows=" << tableInfo.second << endl;

      totalSize += tableInfo.first;
    }

    itorForEnabled.getNext(key, value);
  }

  cout << "Getting disabled entries: " << endl;

  NAHashDictionaryIteratorNoCopy<NAString, HTableCache> itorForDisabled(*(memoryTableDB_->tableNameToCacheEntryMap()),
                                                                        iteratorEntryType::DISABLED);

  itorForDisabled.getNext(key, value);
  while (key) {
    ctDisabled++;

    cout << "key: this=" << static_cast<void *>(key) << ", SQL name=" << key->data() << ", size=" << key->length()
         << endl;

    if (value) {
      auto tableInfo = value->tableSize<iteratorEntryType::DISABLED>();
      cout << "value: this=" << static_cast<void *>(value)
           << ", data()'s address=" << static_cast<void *>((char *)(value)) << ", size=" << tableInfo.first
           << ", rows=" << tableInfo.second << endl;

      totalSize += tableInfo.first;
    }

    itorForDisabled.getNext(key, value);
  }

  float avg = ((ctEnabled + ctDisabled) > 0) ? (float)totalSize / (ctEnabled + ctDisabled) : 0;

  cout << "FindAll(): finding all pairs of (key, value) from hash table"
       << " Total enabled found=" << ctEnabled << " Total disabled found=" << ctDisabled
       << ", size in bytes: total=" << totalSize << ", avg=" << avg << endl;
}

char *SharedDescriptorCache::collectSummaryDataForTable(const QualifiedName &tableName) {
  if (!table_) return NULL;

  NAMemory *heap = getSharedHeap();

  if (!heap) return NULL;

  //  112 bytes for the text,
  //  84 bytes for 4 long long values (21 bytes each)
  //  11 bytes for 1 long values (11 bytes each)
  //  total = 112 + 84 + 11 =207
  const int bufLen = 210;
  static THREAD_P char buf[bufLen];

  QualifiedName *key = NULL;
  NAString *value = table_->getFirstValue(&tableName, QualifiedNameHashFunc);

  int len = 0;
  if (value) {
    len = snprintf(buf, bufLen,
                   "a descriptor of %lu bytes is found for the table. heap_sizes(bytes): total=%ld, alloc=%ld, "
                   "free=%ld; heap_starting_address=%p",
                   value->length(), heap->getTotalSize(), heap->getAllocSize(),
                   heap->getTotalSize() - heap->getAllocSize(), static_cast<void *>((char *)(heap)));
  } else {
    len = snprintf(buf, bufLen,
                   "no descriptor is found for the table. heap_sizes(bytes): total=%ld, alloc=%ld, free=%ld; "
                   "heap_starting_address=%p",
                   heap->getTotalSize(), heap->getAllocSize(), heap->getTotalSize() - heap->getAllocSize(),
                   static_cast<void *>((char *)(heap)));
  }

  if (len <= 0) return NULL;

  return buf;
}

// return a buffer filled with computed summary info.
char *SharedDescriptorCache::collectSummaryDataForSchema(const QualifiedName &schemaName) {
  if (!table_) return NULL;

  NAMemory *heap = getSharedHeap();

  if (!heap) return NULL;

  //  138 bytes for the text,
  //  126 bytes for 6 long long values (21 bytes each)
  //  22 bytes for 2 long values (11 bytes each)
  //  1 byte for 1 boolean value
  //
  //  total = 132 + 126 + 22 + 1 = 281
  const int bufLen = 300;
  static THREAD_P char buf[bufLen];

  long totalSizeForAll = 0;
  long totalSizeForEnabled = 0;

  NAHashDictionaryIteratorNoCopy<QualifiedName, NAString> itorForAll(*table_, iteratorEntryType::EVERYTHING);

  QualifiedName *key = NULL;
  NAString *value = NULL;

  itorForAll.getNext(key, value);
  while (key) {
    if (key && key->getCatalogNameAsAnsiString() == schemaName.getCatalogNameAsAnsiString() &&
        key->getSchemaNameAsAnsiString() == schemaName.getSchemaNameAsAnsiString() && value) {
      totalSizeForAll += value->length();
    }

    itorForAll.getNext(key, value);
  }

  NAHashDictionaryIteratorNoCopy<QualifiedName, NAString> itorForEnabled(*table_, iteratorEntryType::ENABLED);

  itorForEnabled.getNext(key, value);
  while (key) {
    if (key && key->getCatalogNameAsAnsiString() == schemaName.getCatalogNameAsAnsiString() &&
        key->getSchemaNameAsAnsiString() == schemaName.getSchemaNameAsAnsiString() && value) {
      totalSizeForEnabled += value->length();
    }

    itorForEnabled.getNext(key, value);
  }

  int len = snprintf(buf, bufLen,
                       "entries: total=%d, enabled=%d; sum_of_length(bytes): total=%ld, enabled=%ld; "
                       "heap_sizes(bytes): total=%ld, alloc=%ld, free=%ld; heap_starting_address=%p; sanity=%d",
                       table_->entries(), table_->entriesEnabled(), totalSizeForAll, totalSizeForEnabled,
                       heap->getTotalSize(), heap->getAllocSize(), heap->getTotalSize() - heap->getAllocSize(),
                       static_cast<void *>((char *)(heap)), table_->sanityCheck());

  if (len <= 0) return NULL;

  return buf;
}

// return a buffer filled with computed summary info.
char *SharedDescriptorCache::collectSummaryDataForAll() {
  if (!table_) return NULL;

  NAMemory *heap = getSharedHeap();

  if (!heap) return NULL;

  //  138 bytes for the text,
  //  126 bytes for 6 long long values (21 bytes each)
  //  22 bytes for 2 long values (11 bytes each)
  //  1 byte for 1 boolean value
  //
  //  total = 132 + 126 + 22 +1 = 281
  const int bufLen = 300;
  static THREAD_P char buf[bufLen];

  long totalSizeForAll = 0;
  long totalSizeForEnabled = 0;

  NAHashDictionaryIteratorNoCopy<QualifiedName, NAString> itorForAll(*table_, iteratorEntryType::EVERYTHING);

  QualifiedName *key = NULL;
  NAString *value = NULL;

  itorForAll.getNext(key, value);
  while (key) {
    if (value) {
      totalSizeForAll += value->length();
    }

    itorForAll.getNext(key, value);
  }

  NAHashDictionaryIteratorNoCopy<QualifiedName, NAString> itorForEnabled(*table_, iteratorEntryType::ENABLED);

  itorForEnabled.getNext(key, value);
  while (key) {
    if (value) {
      totalSizeForEnabled += value->length();
    }

    itorForEnabled.getNext(key, value);
  }

  int len = snprintf(buf, bufLen,
                       "entries: total=%d, enabled=%d; sum_of_length(bytes): total=%ld, enabled=%ld; "
                       "heap_sizes(bytes): total=%ld, alloc=%ld, free=%ld; heap_starting_address=%p; sanity=%d",
                       table_->entries(), table_->entriesEnabled(), totalSizeForAll, totalSizeForEnabled,
                       heap->getTotalSize(), heap->getAllocSize(), heap->getTotalSize() - heap->getAllocSize(),
                       static_cast<void *>((char *)(heap)), table_->sanityCheck());

  if (len <= 0) return NULL;

  return buf;
}

char *SharedDescriptorCache::collectSummaryDataForHeap() {
  if (!table_) return NULL;

  NAMemory *heap = getSharedHeap();

  if (!heap) return NULL;

  //  138 bytes for the text,
  //  126 bytes for 6 long long values (21 bytes each)
  //  22 bytes for 2 long values (11 bytes each)
  //  1 byte for 1 boolean value
  //
  //  total = 132 + 126 + 22 +1 = 281
  const int bufLen = 300;
  static THREAD_P char buf[bufLen];

  int len =
      snprintf(buf, bufLen, "heap_sizes(bytes): total=%ld, alloc=%ld, free=%ld; heap_starting_address=%p; sanity=%d",
               heap->getTotalSize(), heap->getAllocSize(), heap->getTotalSize() - heap->getAllocSize(),
               static_cast<void *>((char *)(heap)), table_->sanityCheck());

  if (len <= 0) return NULL;

  return buf;
}

int SharedDescriptorCache::countAll(enum iteratorEntryType type) {
  if (!table_) return 0;

  int ct = 0;

  NAHashDictionaryIteratorNoCopy<QualifiedName, NAString> itor(*table_, type);

  QualifiedName *key = NULL;
  NAString *value = NULL;

  itor.getNext(key, value);
  while (key) {
    ct++;

    itor.getNext(key, value);
  }

  return ct;
}

NABoolean SharedDescriptorCache::remove(const QualifiedName &name) {
  NAString tmp(name.getQualifiedNameAsAnsiString());
  QualifiedName *key = table_->remove((QualifiedName *)&name, QualifiedNameHashFunc, TRUE /* removeKV */);

  NAMemory *heap = getSharedHeap();
  QRLogger::log(
      CAT_SQL_SHARED_CACHE, LL_DEBUG,
      "SharedDescriptorCache::remove(): name=%s, return=%p, heap sizes(bytes): total=%ld, allocated=%ld, sanity=%d",
      tmp.data(), static_cast<void *>(key), heap->getTotalSize(), heap->getAllocSize(), table_->sanityCheck());

  return (!!key);
}

NABoolean SharedDescriptorCache::enable(const QualifiedName &name) {
  QualifiedName *key = table_->enable((QualifiedName *)&name, QualifiedNameHashFunc);

  QRLogger::log(CAT_SQL_SHARED_CACHE, LL_DEBUG, "SharedDescriptorCache::enable(): name=%s, return=%p, sanity=%d",
                name.getQualifiedNameAsAnsiString().data(), static_cast<void *>(key), table_->sanityCheck());
  return (!!key);
}

NABoolean SharedDescriptorCache::disable(const QualifiedName &name) {
  QualifiedName *key = table_->disable((QualifiedName *)&name, QualifiedNameHashFunc);

  QRLogger::log(CAT_SQL_SHARED_CACHE, LL_DEBUG, "SharedDescriptorCache::disable(): name=%s, return=%p, sanity=%d",
                name.getQualifiedNameAsAnsiString().data(), static_cast<void *>(key), table_->sanityCheck());
  return (!!key);
}

NABoolean SharedDescriptorCache::removeSchema(const QualifiedName &name) {
  QRLogger::log(CAT_SQL_SHARED_CACHE, LL_DEBUG, "SharedDescriptorCache::removeSchema(): name=%s",
                name.getQualifiedNameAsAnsiString().data());

  // Since we can delete elements, we have to use the regular
  // iiterator NAHashDictionaryIterator here. It copies out the
  // elements first.
  NAHashDictionaryIterator<QualifiedName, NAString> itor(*table_, NULL, NULL, STMTHEAP, QualifiedNameHashFunc);

  QualifiedName *key = NULL;
  NAString *value = NULL;

  itor.getNext(key, value);
  while (key) {
    if (name.getSchemaName() == key->getSchemaName()) {
      remove(*key);
    }

    itor.getNext(key, value);
  }

  QRLogger::log(CAT_SQL_SHARED_CACHE, LL_DEBUG, "SharedDescriptorCache::removeSchema(): done. result=1");

  return TRUE;
}

NABoolean SharedDescriptorCache::enableSchema(const QualifiedName &name) {
  QRLogger::log(CAT_SQL_SHARED_CACHE, LL_DEBUG, "SharedDescriptorCache::enableSchema(): name=%s",
                name.getQualifiedNameAsAnsiString().data());

  NAHashDictionaryIteratorNoCopy<QualifiedName, NAString> itor(*table_, iteratorEntryType::EVERYTHING);

  QualifiedName *key = NULL;
  NAString *value = NULL;

  itor.getNext(key, value);
  while (key) {
    if (name.getSchemaName() == key->getSchemaName()) {
      enable(*key);
    }

    itor.getNext(key, value);
  }

  QRLogger::log(CAT_SQL_SHARED_CACHE, LL_DEBUG, "SharedDescriptorCache::enableSchema(): done. result=1");

  return TRUE;
}

NABoolean SharedDescriptorCache::disableSchema(const QualifiedName &name) {
  QRLogger::log(CAT_SQL_SHARED_CACHE, LL_DEBUG, "SharedDescriptorCache::disableSchema(): name=%s",
                name.getQualifiedNameAsAnsiString().data());

  NAHashDictionaryIteratorNoCopy<QualifiedName, NAString> itor(*table_, iteratorEntryType::EVERYTHING);

  QualifiedName *key = NULL;
  NAString *value = NULL;

  itor.getNext(key, value);
  while (key) {
    if (name.getSchemaName() == key->getSchemaName()) {
      disable(*key);
    }

    itor.getNext(key, value);
  }

  QRLogger::log(CAT_SQL_SHARED_CACHE, LL_DEBUG, "SharedDescriptorCache::disableSchema(): done. result=1");
  return TRUE;
}

// turn this macro on to observe the allocation details on
// the shared heap.
#define DEBUG_SHARED_HEAP_ALLOCATION 1

NABoolean SharedDescriptorCache::destroy() {
#ifdef DEBUG_SHARED_HEAP_ALLOCATION
  SharedCacheDB::display("Before SharedDescriptorCache::destroy()");
#endif

  table_->clearAll();

  NABoolean rc = SharedCache::destroy(SharedCacheDB::DESCRIPTOR_CACHE);

  sharedHeap_->reInitialize();  // delete all blocks in the heap

#ifdef DEBUG_SHARED_HEAP_ALLOCATION
  SharedCacheDB::display("After SharedDescriptorCache::destroy()");
#endif

  QRLogger::log(
      CAT_SQL_SHARED_CACHE, LL_DEBUG,
      "SharedDescriptorCache::destroy(): result=%d, heap sizes(bytes): total=%ld, allocated=%ld, sanityCheck=%d", rc,
      sharedHeap_->getTotalSize(), sharedHeap_->getAllocSize(), table_->sanityCheck());

  return rc;
}

SharedDescriptorCache *SharedDescriptorCache::destroyAndMake() {
  SharedDescriptorCache *sharedDescCache = SharedDescriptorCache::locate();

  if (sharedDescCache) sharedDescCache->destroy();

  sharedDescCache = SharedDescriptorCache::Make();

  QRLogger::log(CAT_SQL_SHARED_CACHE, LL_DEBUG, "SharedDescriptorCache::destroyAndMake(): result=%p",
                static_cast<void *>(sharedDescCache));

  if (sharedDescCache) {
    NAMemory *heap = sharedDescCache->getSharedHeap();
    QRLogger::log(CAT_SQL_SHARED_CACHE, LL_DEBUG,
                  "SharedDescriptorCache::destroyAndMake(): heap sizes(bytes): total=%ld, allocated=%ld, sanity=%d",
                  heap->getTotalSize(), heap->getAllocSize(), sharedDescCache->sanityCheck());
  }

  return sharedDescCache;
}

void SharedDescriptorCache::populateWithSynthesizedData(int pairs, size_t maxValueLen) {
  srand((int)19);

  SharedCacheDB *cacheDB = SharedCacheDB::locate();
  assert(cacheDB);

  int threshold = (int)(cacheDB->getSizeOfDefaultSegment() * 0.9);

  char *data = new char[maxValueLen];
  for (int i = 0; i < maxValueLen; i++) {
    data[i] = 'S';
  }

#ifdef DEBUG_SHARED_HEAP_ALLOCATION
  SharedCacheDB::display("Before loading synthesized data");
#endif

  int i;
  int ct = 0;
  long totalSize = 0;
  NABoolean inserted = TRUE;
  for (i = 0; i < pairs; i++) {
    char buf[20];
    NAString idAsName(str_itoa(rand(), buf));
    QualifiedName *key = new (getSharedHeap(), FALSE) QualifiedName(idAsName, "", "", getSharedHeap());

    if (!key) break;

    size_t size = rand() % maxValueLen;
    NAString *value = new (getSharedHeap(), FALSE) NAString(data, size, getSharedHeap());

    if (!value) break;

    totalSize += size;

    if (insert(key, value, &inserted)) {
      if (inserted)
        ct++;
      else
        break;
    }
  }

  int enabled = countAll(iteratorEntryType::ENABLED);
  int disabled = countAll(iteratorEntryType::DISABLED);

  float avgSize = (i == 0) ? 0 : (float)totalSize / i;

#ifdef DEBUG_SHARED_HEAP_ALLOCATION
  SharedCacheDB::display("After loading synthesized data");
#endif

  cout << endl
       << "Loading " << ct << " pairs of (key, value) into hash table "
       << " Actual # loaded=" << ct << ", size in bytes: total=" << totalSize << ", avg=" << avgSize
       << ", enabled=" << enabled << ", disabled=" << disabled << endl
       << endl;

  NABoolean status = enabled == ct && disabled == 0;

  if (status)
    cout << "PASS" << endl;
  else
    cout << "FAILED" << endl;

  delete data;
}

void SharedDescriptorCache::lookUpWithSynthesizedData(int pairs) {
  srand((int)19);

  SharedCacheDB *cacheDB = SharedCacheDB::locate();
  assert(cacheDB);

  int threshold = (int)(cacheDB->getSizeOfDefaultSegment() * 0.9);

  int i;
  int ct = 0;
  long totalSize = 0;
  NAString *value = NULL;
  for (i = 0; i < pairs; i++) {
    // long key(rand());
    char buf[20];
    NAString idAsName(str_itoa(rand(), buf));
    QualifiedName key(idAsName);

    rand();  // call it again to simulate random length for value

    value = table_->getFirstValue(&key, QualifiedNameHashFunc);

    if (value) {
      ct++;
      totalSize += value->length();
    } else {
      // cout << "Missing key=" << key.getQualifiedNameAsAnsiString().data() << endl;
    }
  }

  float avg = (ct > 0) ? (float)totalSize / ct : 0;

  cout << "Lookup " << i << " pairs of (key, value) from hash table "
       << " Total found=" << ct << ", size in bytes: total=" << totalSize << ", avg=" << avg << endl;
}

void SharedDescriptorCache::enableDisableDeleteWithSynthesizedData(int pairs) {
  srand((int)19);

  int i;
  int disabled = 0;
  NABoolean ok = TRUE;
  char buf[20];

  // known entries in table_
  int ct = table_->entries();

  // disable all keys with even length
  for (i = 0; i < pairs; i++) {
    NAString idAsName(str_itoa(rand(), buf));
    QualifiedName key(idAsName);

    rand();  // call it again to simulate random length for value

    if (disable(key)) disabled++;
  }

  // lookup all disabled keys. None should be found
  int disabledFoundViaContain = 0;
  int disabledFoundViaGetFirstValue = 0;
  srand((int)19);
  for (i = 0; i < pairs; i++) {
    NAString idAsName(str_itoa(rand(), buf));
    QualifiedName key(idAsName);

    rand();  // call it again to simulate random length for value

    ok = table_->contains(&key, QualifiedNameHashFunc);
    if (ok) disabledFoundViaContain++;

    NAString *value = table_->getFirstValue(&key, QualifiedNameHashFunc);
    if (value) disabledFoundViaGetFirstValue++;
  }

  int disabledViaCountAll = countAll(iteratorEntryType::DISABLED);
  int enabledViaCountAll = countAll(iteratorEntryType::ENABLED);

  // Enable all disabled keys.
  int reEnabled = 0;
  srand((int)19);
  for (i = 0; i < pairs; i++) {
    NAString idAsName(str_itoa(rand(), buf));
    QualifiedName key(idAsName);

    rand();  // call it again to simulate random length for value

    if (enable(key)) reEnabled++;
  }

  // Verify that all keys are present and enabled.
  int enabledAll = 0;
  srand((int)19);
  for (i = 0; i < pairs; i++) {
    NAString idAsName(str_itoa(rand(), buf));
    QualifiedName key(idAsName);

    rand();  // call it again to simulate random length for value

    NAString *value = table_->getFirstValue(&key, QualifiedNameHashFunc);
    if (value) enabledAll++;
  }

#ifdef DEBUG_SHARED_HEAP_ALLOCATION
  SharedCacheDB::display("Before deleting synthesized data");
#endif

  char *summary = collectSummaryDataForHeap();
  cout << "Heap(after enable/disable): " << endl << summary << endl;

  // now handle deletion
  int removed = 0;
  srand((int)19);
  for (i = 0; i < pairs; i++) {
    NAString idAsName(str_itoa(rand(), buf));
    QualifiedName key(idAsName);

    rand();  // call it again to simulate random length for value

    if (remove(key)) removed++;
  }

  summary = collectSummaryDataForHeap();
  cout << "Heap(after deletion): " << endl << summary << endl;

  // Verify that all remaining keys are present and enabled.
  int remaining = 0;
  srand((int)19);
  for (i = 0; i < pairs; i++) {
    NAString idAsName(str_itoa(rand(), buf));
    QualifiedName key(idAsName);

    rand();  // call it again to simulate random length for value

    NAString *value = find(key);
    if (value) remaining++;
  }

  // For each key, pretend it to be a real SQL qualified name
  // and ask for entries in the cache that matches the schema
  // part in the key. The return count should be 0.
  int ctInSameSchema = 0;
  srand((int)19);
  for (i = 0; i < pairs; i++) {
    QualifiedName key("obj", "sch", "cat");

    rand();  // call it again to simulate random length for value

    ctInSameSchema += entries(key);
  }

#ifdef DEBUG_SHARED_HEAP_ALLOCATION
  SharedCacheDB::display("After deleting synthesized data");
#endif

  cout << "enabledDisableDelete " << ct << " pairs of (key, value) from hash table "
       << ", disabled=" << disabled << ", disabled found:"
       << " via contain()=" << disabledFoundViaContain << ", via getFirstValue()=" << disabledFoundViaGetFirstValue
       << ", via disabledViacountAll()=" << disabledViaCountAll << ", enabledViaCountAll=" << enabledViaCountAll
       << ", reEnabled=" << reEnabled << ", enabledAll=" << enabledAll << ", removed=" << removed
       << ", remaining=" << remaining << ", ctInSameSchema=" << ctInSameSchema << endl;

  NABoolean status = disabledFoundViaContain == 0 && disabledFoundViaGetFirstValue == 0 &&
                     (enabledViaCountAll + disabledViaCountAll) == ct && disabled == reEnabled && enabledAll == ct &&
                     (removed + remaining) == ct && ctInSameSchema == 0;

  if (status)
    cout << "PASS" << endl;
  else
    cout << "FAILED" << endl;
}

void SharedDescriptorCache::testPopulateWithSynthesizedData(int pairs, size_t maxLength) {
  int retcode = 0;
  LOCK_SHARED_CACHE;

  cout << "Test SharedDescriptorCache::testPopulateWithSynthesizedData() with " << pairs
       << " key-value pairs, each is no more than " << maxLength << " bytes." << endl;

  SharedDescriptorCache *sharedDescCache = SharedDescriptorCache::destroyAndMake();

  if (sharedDescCache) {
    sharedDescCache->populateWithSynthesizedData(pairs, maxLength);
  } else
    cout << "sharedDescriptorCache::destroyAndMake() failed: can not create a shared descriptor cache" << endl;

  UNLOCK_SHARED_CACHE;
}

void SharedDescriptorCache::testHashDictionaryEnableDisableDelete(int pairs, size_t maxLength) {
  SharedDescriptorCache::testPopulateWithSynthesizedData(pairs, maxLength);

  int retcode = 0;
  LOCK_SHARED_CACHE;

  SharedDescriptorCache *sharedDescCache = SharedDescriptorCache::locate();

  if (sharedDescCache) {
    cout << "Now perform enable/disable/delete with " << pairs << " key-value pairs, each is no more than " << maxLength
         << " bytes.";

    sharedDescCache->enableDisableDeleteWithSynthesizedData(pairs);

  } else {
    cout << "test EnableDisableDelete failed: can not locate the shared descriptor cache." << endl;
  }

  UNLOCK_SHARED_CACHE;
}

void SharedDescriptorCache::display() {
  void (SharedDescriptorCache::*pFunc1)(int, size_t) = &SharedDescriptorCache::populateWithSynthesizedData;
  void (SharedDescriptorCache::*pFunc2)(int) = &SharedDescriptorCache::lookUpWithSynthesizedData;

  cout << "&sharedDescriptorCache::lookUp=" << (void *&)pFunc1 << endl;
  cout << "&sharedDescriptorCache::populate=" << (void *&)pFunc2 << endl;
}

SharedTableDataCache *SharedTableDataCache::Make() {
  SharedTableDataCache *sharedDataCache = NULL;

  SharedCacheDB *sharedCacheDB = SharedCacheDB::locate(SharedCacheDB::TABLEDATA_CACHE);

  if (sharedCacheDB) {
    NAMemory *sharedHeap = sharedCacheDB->getSharedHeap();
    sharedDataCache = new (sharedHeap) SharedTableDataCache(sharedHeap, sharedCacheDB);

    if (sharedDataCache) sharedCacheDB->set(SharedCacheDB::TABLEDATA_CACHE, sharedDataCache);
  }
  return sharedDataCache;
}

SharedTableDataCache *SharedTableDataCache::destroyAndMake() {
  SharedTableDataCache *sharedDataCache = SharedTableDataCache::locate();

  if (sharedDataCache) sharedDataCache->destroy();

  sharedDataCache = SharedTableDataCache::Make();

  QRLogger::log(CAT_SQL_SHARED_CACHE, LL_DEBUG, "SharedTableDataCache::destroyAndMake(): result=%p",
                static_cast<void *>(sharedDataCache));

  SharedCacheDB::display("After Shared Table Data Cache init...", SharedCacheDB::TABLEDATA_CACHE);

  if (sharedDataCache) {
    NAMemory *heap = sharedDataCache->getSharedHeap();
    QRLogger::log(CAT_SQL_SHARED_CACHE, LL_DEBUG,
                  "SharedTableDataCache::destroyAndMake(): heap sizes(bytes): total=%ld, allocated=%ld, sanity=%d",
                  heap->getTotalSize(), heap->getAllocSize(), sharedDataCache->sanityCheck());
  }

  return sharedDataCache;
}

SharedTableDataCache *SharedTableDataCache::locate() {
  SharedTableDataCache *sharedDataCache = NULL;
  SharedCacheDB *sharedCacheDB = SharedCacheDB::locate(SharedCacheDB::TABLEDATA_CACHE);

  if (sharedCacheDB) sharedDataCache = (SharedTableDataCache *)(sharedCacheDB->get(SharedCacheDB::TABLEDATA_CACHE));

  return (sharedDataCache && sharedDataCache->isMe()) ? sharedDataCache : NULL;
}

NABoolean SharedTableDataCache::destroy() {
  SharedCacheDB::display("Before Shared Table Data Cache clear...", SharedCacheDB::TABLEDATA_CACHE);
  memoryTableDB_->tableNameToCacheEntryMap()->clearAll();

  NABoolean rc = SharedCache::destroy(SharedCacheDB::TABLEDATA_CACHE);

  sharedHeap_->reInitialize();

  SharedCacheDB::display("After Shared Table Data Cache clear...", SharedCacheDB::TABLEDATA_CACHE);

  QRLogger::log(
      CAT_SQL_SHARED_CACHE, LL_DEBUG,
      "SharedTableDataCache::destroy(): result=%d, heap sizes(bytes): total=%ld, allocated=%ld, sanityCheck=%d", rc,
      sharedHeap_->getTotalSize(), sharedHeap_->getAllocSize(), sanityCheck());

  return rc;
}

SharedTableDataCache::SharedTableDataCache(NAMemory *sharedHeap, SharedCacheDB *sharedCacheDB)
    : SharedCache(TABLESDATA_CACHE_EYE_CATCHER, sharedHeap, sharedCacheDB), memoryTableDB_(NULL), sizeOfTable_(0) {
  if (sharedHeap) memoryTableDB_ = new (sharedHeap_, FALSE) MemoryTableDB(sharedHeap_);
  if (memoryTableDB_ && isComplete()) sizeOfTable_ = sizeof(*memoryTableDB_);
}

NABoolean SharedTableDataCache::disable(const QualifiedName &name) {
  NAString nameKey(name.getQualifiedNameAsAnsiString());
  NAString *keyTName = memoryTableDB_->tableNameToCacheEntryMap()->disable(&nameKey, NAStringHashFunc);

  QRLogger::log(CAT_SQL_SHARED_CACHE, LL_DEBUG, "SharedTableDataCache::disable(): name=%s, return=%p, sanity=%d",
                nameKey.data(), static_cast<void *>(keyTName), sanityCheck());
  return ((!!keyTName));
}

NABoolean SharedTableDataCache::contains(const QualifiedName &name, bool checkAll) {
  NAString nameKey(name.getQualifiedNameAsAnsiString());
  NABoolean tableExist1 = memoryTableDB_->tableNameToCacheEntryMap()->contains(&nameKey, NAStringHashFunc, checkAll);

  QRLogger::log(CAT_SQL_SHARED_CACHE, LL_DEBUG, "SharedTableDataCache::disable(): name=%s, sanity=%d", nameKey.data(),
                sanityCheck());
  return (tableExist1);
}

NABoolean SharedTableDataCache::enable(const QualifiedName &name) {
  NAString nameKey(name.getQualifiedNameAsAnsiString());
  NAString *keyTName = memoryTableDB_->tableNameToCacheEntryMap()->enable(&nameKey, NAStringHashFunc);

  QRLogger::log(CAT_SQL_SHARED_CACHE, LL_DEBUG, "SharedTableDataCache::enable(): name=%s, return=%p, sanity=%d",
                nameKey.data(), static_cast<void *>(keyTName), sanityCheck());
  return ((!!keyTName));
}

NABoolean SharedTableDataCache::isEnable(const QualifiedName &name) {
  NAString nameKey(name.getQualifiedNameAsAnsiString());
  return memoryTableDB_->tableNameToCacheEntryMap()->isEnable(&nameKey, NAStringHashFunc);
}

NABoolean SharedTableDataCache::remove(const QualifiedName &name) {
  NAString nameKey(name.getQualifiedNameAsAnsiString());
  NAString *keyTName =
      memoryTableDB_->tableNameToCacheEntryMap()->remove(&nameKey, NAStringHashFunc, TRUE /* removeKV */);

  NAMemory *heap = getSharedHeap();
  QRLogger::log(
      CAT_SQL_SHARED_CACHE, LL_DEBUG,
      "SharedTableDataCache::remove(): name=%s, return=%p, heap sizes(bytes): total=%ld, allocated=%ld, sanity=%d",
      nameKey.data(), static_cast<void *>(keyTName), heap->getTotalSize(), heap->getAllocSize(), sanityCheck());

  return ((!!keyTName));
}

char *SharedTableDataCache::collectSummaryDataForAll(bool showDetails, int &blackBoxLen) {
  if (!memoryTableDB_) return NULL;

  NAMemory *heap = getSharedHeap();

  if (!heap) return NULL;

  int bufLen = 300;
  char *buf = new char[bufLen];

  long totalSizeForAll = 0;
  long totalSizeForEnabled = 0;

  NAHashDictionaryIteratorNoCopy<NAString, HTableCache> itorForAll(*(memoryTableDB_->tableNameToCacheEntryMap()),
                                                                   iteratorEntryType::EVERYTHING);

  NAString *key = NULL;
  HTableCache *value = NULL;

  std::vector<std::tuple<NAString, long, long, NABoolean>> tableInfos;

  itorForAll.getNext(key, value);
  while (key) {
    if (value) {
      auto tableInfo = value->tableSize<iteratorEntryType::EVERYTHING>();
      totalSizeForAll += tableInfo.first;
      NABoolean isEnable = memoryTableDB_->tableNameToCacheEntryMap()->isEnable(key, NAStringHashFunc);
      if (showDetails) tableInfos.push_back(std::make_tuple(*key, tableInfo.second, tableInfo.first, isEnable));
    }
    itorForAll.getNext(key, value);
  }

  int len = snprintf(
      buf, bufLen,
      "\nSummary: Total=%d, Enabled=%d; Shared Memory Heap(%p): Total=%ld, Used=%ld, Free=%ld; Shmaddr=%p; Shmid=%d",
      memoryTableDB_->entries(), memoryTableDB_->entriesEnabled(), static_cast<void *>((char *)(heap)),
      heap->getTotalSize(), heap->getAllocSize(), heap->getTotalSize() - heap->getAllocSize(),
      defaultSharedSegment(TRUE).getAddr(), defaultSharedSegment(TRUE).getShmid());

  if (len <= 0) return NULL;

  int sizeInt32 = sizeof(int);
  // int recodeLen = 180 * tableInfos.size() + ROUND4(len + 1) + sizeInt32 * 2 + 1;
  int recodeLen = sizeInt32 + sizeInt32 + ROUND4(len + 1);
  if (showDetails)
    recodeLen += sizeInt32 + ROUND4(150 + 1) + tableInfos.size() * sizeInt32 + tableInfos.size() * ROUND4(150 + 1) +
                 sizeInt32 + ROUND4(150 + 1);

  char *blackBox = new (STMTHEAP) char[recodeLen];

  char *currPtr = blackBox;

  if (showDetails)
    *(int *)currPtr = tableInfos.size() + 1 + 1 + 1;  // total entries = number of tables + 3
  else
    *(int *)currPtr = 1;

  currPtr += sizeInt32;
  blackBoxLen += sizeInt32;

  *(int *)currPtr = len;
  currPtr += sizeInt32;
  blackBoxLen += sizeInt32;

  strncpy(currPtr, buf, len);
  currPtr[len] = '\0';
  currPtr += ROUND4(len + 1);
  blackBoxLen += ROUND4(len + 1);

  if (showDetails) {
    // if (tableInfos.size()>0)
    {
      len = snprintf(buf, bufLen, "\n\t%-60s %-20s %-20s %-20s", "Table Name", "Status", "Rows", "Size(bytes)");
      *(int *)currPtr = len;
      currPtr += sizeInt32;
      blackBoxLen += sizeInt32;
      strncpy(currPtr, buf, len);
      currPtr[len] = '\0';
      currPtr += ROUND4(len + 1);
      blackBoxLen += ROUND4(len + 1);
    }
    for (auto &&tInfo : tableInfos) {
      int cLen =
          snprintf(buf, bufLen, "\t%-60s %-20s %-10lu%-10s %-10lu", NAString(std::get<0>(tInfo).data(), 60).data(),
                   std::get<3>(tInfo) ? "Enable" : "Disable", std::get<1>(tInfo), "", std::get<2>(tInfo));
      *(int *)currPtr = cLen;
      currPtr += sizeInt32;
      blackBoxLen += sizeInt32;

      strncpy(currPtr, buf, cLen);
      currPtr[cLen] = '\0';
      currPtr += ROUND4(cLen + 1);
      blackBoxLen += ROUND4(cLen + 1);
    }
    len = snprintf(buf, bufLen, "\n\t%82s%-20s %-10lu", "", "Total Size(bytes)", totalSizeForAll);
    *(int *)currPtr = len;
    currPtr += sizeInt32;
    blackBoxLen += sizeInt32;
    strncpy(currPtr, buf, len);
    currPtr[len] = '\0';
    currPtr += ROUND4(len + 1);
    blackBoxLen += ROUND4(len + 1);
  }

  delete buf;
  buf = nullptr;
  blackBox[blackBoxLen + 1] = '\0';
  return blackBox;
}

bool processStr(const char *val, int len, const QualifiedName &schemaName) {
  char cBuf[1000];
  strncpy(cBuf, val, len);
  cBuf[len] = '\0';
  char *c = cBuf;
  int numParts = 0;
  char *parts[4];
  LateNameInfo::extractParts(c, cBuf, numParts, parts, FALSE);
  if (numParts != 3) cout << "erro" << endl;

  NAString catalogNamePart(parts[0]);
  NAString schemaNamePart(parts[1]);
  NAString objectNamePart(parts[2]);

  const char *colonPos = strchr(catalogNamePart, ':');
  if (colonPos) catalogNamePart = NAString(&catalogNamePart.data()[colonPos - catalogNamePart + 1]);

  return (catalogNamePart == schemaName.getCatalogNameAsAnsiString() &&
          schemaNamePart == schemaName.getSchemaNameAsAnsiString());
}

char *SharedTableDataCache::collectSummaryDataForSchema(const QualifiedName &schemaName) {
  if (!memoryTableDB_) return NULL;

  NAMemory *heap = getSharedHeap();

  if (!heap) return NULL;

  const int bufLen = 300;
  static THREAD_P char buf[bufLen];

  long totalSizeForAll = 0;
  long totalSizeForEnabled = 0;

  NAHashDictionaryIteratorNoCopy<NAString, HTableCache> itorForAll(*(memoryTableDB_->tableNameToCacheEntryMap()),
                                                                   iteratorEntryType::EVERYTHING);

  NAString *key = NULL;
  HTableCache *value = NULL;

  itorForAll.getNext(key, value);
  while (key) {
    if (key && processStr(key->data(), key->length(), schemaName) && value)
      totalSizeForAll += value->tableSize<iteratorEntryType::EVERYTHING>().first;
    itorForAll.getNext(key, value);
  }

  int len = snprintf(buf, bufLen,
                       "Summary: Total=%d, Enabled=%d; Sum_of_length(bytes): Total=%ld, Shared Memory Heap(%p): "
                       "Total=%ld, Used=%ld, Free=%ld; Shmaddr=%p; Shmid=%d",
                       memoryTableDB_->entries(), memoryTableDB_->entriesEnabled(), totalSizeForAll,
                       static_cast<void *>((char *)(heap)), heap->getTotalSize(), heap->getAllocSize(),
                       heap->getTotalSize() - heap->getAllocSize(), defaultSharedSegment(TRUE).getAddr(),
                       defaultSharedSegment(TRUE).getShmid());

  if (len <= 0) return NULL;
  return buf;
}

char *SharedTableDataCache::collectSummaryDataForTable(const QualifiedName &tableName) {
  if (!memoryTableDB_) return NULL;

  NAMemory *heap = getSharedHeap();

  if (!heap) return NULL;

  //  112 bytes for the text,
  //  84 bytes for 4 long long values (21 bytes each)
  //  11 bytes for 1 long values (11 bytes each)
  //  total = 112 + 84 + 11 =207
  const int bufLen = 210;
  static THREAD_P char buf[bufLen];

  auto key = tableName.getQualifiedNameAsString();
  HTableCache *value = memoryTableDB_->tableNameToCacheEntryMap()->getFirstValue(&key, NAStringHashFunc);

  int len = 0;
  if (value) {
    auto tableInfo = value->tableSize<iteratorEntryType::EVERYTHING>();
    len = snprintf(buf, bufLen,
                   "a table data cache of %lu bytes/ %lu rows is found for the table. Shared Memory Heap(%p): "
                   "Total=%ld, Used=%ld, Free=%ld; Shmaddr=%p; Shmid=%d",
                   tableInfo.first, tableInfo.second, static_cast<void *>((char *)(heap)), heap->getTotalSize(),
                   heap->getAllocSize(), heap->getTotalSize() - heap->getAllocSize(),
                   defaultSharedSegment(TRUE).getAddr(), defaultSharedSegment(TRUE).getShmid());
  } else
    len = snprintf(buf, bufLen,
                   "no table data cache is found for the table. Shared Memory Heap(%p): Total=%ld, Used=%ld, Free=%ld; "
                   "Shmaddr=%p; Shmid=%d",
                   static_cast<void *>((char *)(heap)), heap->getTotalSize(), heap->getAllocSize(),
                   heap->getTotalSize() - heap->getAllocSize(), defaultSharedSegment(TRUE).getAddr(),
                   defaultSharedSegment(TRUE).getShmid());

  if (len <= 0) return NULL;

  return buf;
}

void testSharedMemorySequentialScan(int argc, char **argv)

{
  // find my segment id which will be used as a key to
  // access the shared segment
  int shmId;
  StatsGlobals *statsGlobals = (StatsGlobals *)shareStatsSegment(shmId);

  // the base address of the shared segment
  void *baseAddr = statsGlobals->getStatsSharedSegAddr();

  // Assume that there is an extra segment toward the end
  // of the shared segment
  char *metaBaseAddr = (char *)baseAddr + STATS_MAX_SEG_SIZE;

  SharedCacheDB *cacheDB = SharedCacheDB::locate();
  assert(cacheDB);

  size_t memoryLen = cacheDB->getSizeOfDefaultSegment();

  char *metaEndAddr = metaBaseAddr + memoryLen - 1;

  char c = 0;
  for (int i = 0; i < memoryLen; i++) {
    c = metaBaseAddr[i];
  }
}

void testSharedMemoryHashDictionaryPopulate() {
#if 0
   // 1000 pairs of 1000 bytes.
   SharedDescriptorCache::testPopulateWithSynthesizedData(1000, 1000);

   // 1000 pairs of 1MB.
   SharedDescriptorCache::testPopulateWithSynthesizedData(1000, 1000000);

   // 10000 pairs of 10MB.
   SharedDescriptorCache::testPopulateWithSynthesizedData(10000, 10000000);
#endif

  // 100 pairs of 100MB.
  SharedDescriptorCache::testPopulateWithSynthesizedData(100, 100000000);
}

void testSharedMemoryHashDictionaryLookup() {
  SharedDescriptorCache *sharedDescCache = SharedDescriptorCache::locate();

  int pairs = 1000;

  if (sharedDescCache) {
    sharedDescCache->lookUpWithSynthesizedData(pairs);
  } else {
    cout << "Lookup failed: can not locate the shared descriptor cache." << endl;
  }
}

void testSharedMemoryHashDictionaryFindAll() {
  SharedDescriptorCache *sharedDescCache = SharedDescriptorCache::locate();

  if (sharedDescCache) {
    sharedDescCache->findAll();
  } else {
    cout << "findAll failed: can not locate the shared descriptor cache." << endl;
  }
}

void testSharedDataMemoryHashDictionaryFindAll() {
  SharedTableDataCache *sharedDataCache = SharedTableDataCache::locate();

  if (sharedDataCache) {
    sharedDataCache->findAll();
  } else {
    cout << "findAll failed: can not locate the shared data cache." << endl;
  }
}

void testSharedMemoryHashDictionaryClean() {
  int retcode = 0;
  LOCK_SHARED_CACHE;

  SharedDescriptorCache *sharedDescCache = SharedDescriptorCache::locate();

  if (sharedDescCache) {
    NABoolean ok = sharedDescCache->destroy();

    cout << "clean: ";

    if (ok)
      cout << " succeeded." << endl;
    else
      cout << " failed." << endl;

  } else {
    cout << "No shared cache to clean: can not locate the shared descriptor cache." << endl;
  }
  UNLOCK_SHARED_CACHE;
}

void testSharedMemoryHashDictionaryDestroy() {
  int retcode = 0;
  LOCK_SHARED_CACHE;

  SharedDescriptorCache *sharedDescCache = SharedDescriptorCache::locate();

  if (sharedDescCache) {
    NABoolean ok = sharedDescCache->destroy();

    cout << "Destroy: ";

    if (ok)
      cout << " succeeded." << endl;
    else
      cout << " failed." << endl;

  } else {
    cout << "No shared cache to destroy: can not locate the shared descriptor cache." << endl;
  }

  UNLOCK_SHARED_CACHE;
}

void testSharedMemoryHashDictionaryEnableDisableDelete() {
  // 1000 pairs of 1000 bytes.
  SharedDescriptorCache::testHashDictionaryEnableDisableDelete(1000, 1000);

  // 1000 pairs of 1MB.
  SharedDescriptorCache::testHashDictionaryEnableDisableDelete(1000, 1000000);

  // 10000 pairs of 10MB.
  SharedDescriptorCache::testHashDictionaryEnableDisableDelete(10000, 10000000);

  // 100 pairs of 100MB.
  SharedDescriptorCache::testHashDictionaryEnableDisableDelete(100, 100000000);
}

int testMemoryExhaustionOnNACollectionCreation() {
  int failed = 0;
  NAHeap *heap = new NAHeap("a_small_heap", NAMemory::DERIVED_FROM_SYS_HEAP, 100, 100);

  NABoolean failureIsFatal = FALSE;

  NACollection<int> *collection = new (heap, FALSE) NACollection<int>(heap, 100000, failureIsFatal);

  if (collection && collection->isComplete()) {
    cout << "Failed: new of NACollection(100000 int) should return NULL" << endl;
    failed = 1;
  }
  return failed;
}

int testMemoryExhaustionOnNACollectionCreationTinyHeap() {
  int failed = 0;
  NAHeap *heap = new NAHeap("a_tiny_heap", NAMemory::DERIVED_FROM_SYS_HEAP, 10, 10);

  NABoolean failureIsFatal = FALSE;

  NACollection<int> *collection = new (heap, FALSE) NACollection<int>(heap, 100000, failureIsFatal);

  if (collection) {
    cout << "Failed: new of NACollection(100000 int) on a tiny NACollection should return NULL" << endl;
    failed = 1;
  }
  return failed;
}

static ULng32 hashFunc(int x) { return x; }

int testMemoryExhaustionOnNACollectionInsertion() {
  int failed = 0;
  NAHeap *heap = new NAHeap("a_small_heap", NAMemory::DERIVED_FROM_SYS_HEAP, 100, 100);

  NABoolean failureIsFatal = FALSE;
  int n = 10;
  NACollection<int> *collection = new (heap, FALSE) NACollection<int>(heap, n, failureIsFatal);

  assert(collection);

  int m = n + 20;
  NABoolean inserted;
  for (CollIndex i = 0; i < m; i++) {
    // resizing of the storage NACollection::arr_ is necessary
    collection->insert(i, i, NULL_COLL_INDEX, &inserted);

    if (!inserted) break;
  }

  if (inserted) {
    failed = 1;
    cout << "Failed: there should be at least failed insertion into NACollection" << endl;
  }
  return failed;
}

int testMemoryExhaustionOnNAHashDictionaryCreation() {
  int failed = 0;
  NAHeap *heap = new NAHeap("a_small_heap", NAMemory::DERIVED_FROM_SYS_HEAP, 100, 100);

  NABoolean failureIsFatal = FALSE;

  NAHashDictionary<int, int> *dictionary =
      new (heap, FALSE) NAHashDictionary<int, int>(hashFunc, 100, FALSE, heap, failureIsFatal);

  if (dictionary && dictionary->isComplete()) {
    cout << "Failed: new of NAHashDictionary<int, int> of 100 elements should return an incomplete object" << endl;
    failed = 1;
  }
  return failed;
}

int testMemoryExhaustionOnNAHashDictionaryCreationOnTinyHeap() {
  int failed = 0;
  NAHeap *heap = new NAHeap("a_small_heap", NAMemory::DERIVED_FROM_SYS_HEAP, 10, 10);

  NABoolean failureIsFatal = FALSE;

  NAHashDictionary<int, int> *dictionary =
      new (heap, FALSE) NAHashDictionary<int, int>(hashFunc, 100, FALSE, heap, failureIsFatal);

  if (dictionary) {
    cout << "Failed: new of NAHashDictionary<int, int> of 100 elements on a tiny heap should return NULL" << endl;
    failed = 1;
  }
  return failed;
}

int testMemoryExhaustionOnNAHashDictionaryInsertion() {
  int failed = 0;
  NAHeap *heap = new NAHeap("a_small_heap", NAMemory::DERIVED_FROM_SYS_HEAP, 150, 150);

  NABoolean failureIsFatal = FALSE;

  int n = 10;
  NAHashDictionary<int, int> *dictionary =
      new (heap, FALSE) NAHashDictionary<int, int>(hashFunc, 10, FALSE, heap, failureIsFatal);

  assert(dictionary);

  int m = n + 200;
  NABoolean inserted;

  int key = 0;
  int value = 0;
  for (CollIndex i = 0; i < m; i++) {
    key = i;
    value = i;
    // resizing of the storage NACollection::arr_ is necessary
    dictionary->insert(&key, &value, NULL, &inserted);

    if (!inserted) break;
  }

  if (inserted) {
    failed = 1;
    cout << "Failed: there should be at least failed insertion into HashDictionary" << endl;
  }
  return failed;
}

int testMemoryExhaustion() {
  int failed = 0;

  failed += testMemoryExhaustionOnNACollectionCreation();
  failed += testMemoryExhaustionOnNACollectionCreationTinyHeap();
  failed += testMemoryExhaustionOnNACollectionInsertion();
  failed += testMemoryExhaustionOnNAHashDictionaryCreation();
  failed += testMemoryExhaustionOnNAHashDictionaryCreationOnTinyHeap();
  failed += testMemoryExhaustionOnNAHashDictionaryInsertion();

  cout << "Memory exhaustion test:";

  if (failed == 0)
    cout << " PASS";
  else
    cout << " FAILED(" << failed << ")";

  cout << endl;

  return failed;
}
