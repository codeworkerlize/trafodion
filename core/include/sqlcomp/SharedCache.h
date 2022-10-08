// @@@ START COPYRIGHT @@@
//
// (c) Copyright 2019 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         SharedCache.h
 * Description:
 * Created:      6/05/2019
 * Language:     C++
 *
 *
 * Contains classes:
 *   SharedCache
 *   SharedCacheDB
 *   SharedDescriptorCache
 *****************************************************************************
 */
#ifndef SHARED_CACHE_H
#define SHARED_CACHE_H

#include <sys/time.h>

#include "sqlcomp/SharedSegment.h"
#include "common/DLock.h"
#include "common/NABoolean.h"
#include "executor/MemoryTableDB.h"
#include "optimizer/ObjectNames.h"
#include "runtimestats/SqlStats.h"
#include "sqlcomp/NamedSemaphore.h"

#define SHARED_CACHE_SEM_NAME_PREFIX "/shared_cache."

// SharedCacheDB models a set of caches located on a shared memory segment, accessable by
// multiple processes in a node. The layout of the shared cache is as follows.
//
//
// | SharedcacheDB DB        | shared heap object |   memory to be use by H   |
// |                         |        H           |                           |
//
// | <------ sizeof(DB)----> | <--- sizeof(H) --> |                           |
//
// |<--------------- address space of the shared memory segment
// |                         of size SHARED_CACHE_SEG_SIZE  ----------------> |
//

class SharedCache;

#define MAX_SHARED_CACHES   10
#define MAX_SHARED_SEGMENTS 20

#define SHAREDCACHE_DB_EYE_CATCHER "SEED"

class SharedSegment;

class SharedCacheDB {
  friend class DefaultSharedSegment;

 public:
  // enums to numerate different shared cache managed by this DB.
  // The max enum value should be MAX_SHARED_CACHES - 1.
  enum CacheID {
    DESCRIPTOR_CACHE = 0,
    TABLEDATA_CACHE = 1,
  };

 public:
  // The shared heap
  NAMemory *getSharedHeap() { return sharedHeap_; };

  // locate and set a particular shared cache through cacheId
  SharedCache *get(CacheID cacheId);
  void set(CacheID cacheId, SharedCache *);

  // Locate a share cacheDB.
  static SharedCacheDB *locate(CacheID cacheId = DESCRIPTOR_CACHE);

  // locate or create a shared cacheDB, with a form of protection.
  static SharedCacheDB *locateOrCreate(NamedSemaphore *semaphore, CacheID cacheId = DESCRIPTOR_CACHE);
  static SharedCacheDB *locateOrCreate(LockController *controller, CacheID cacheId = DESCRIPTOR_CACHE);

  static void display(const char *msg = NULL, CacheID cid = DESCRIPTOR_CACHE);

  size_t getSizeOfDefaultSegment() const { return sizeOfDefaultSegment_; }

  SharedSegmentDB *getSharedSegmentDB() { return &sharedSegmentDB_; }

  // helper routine to create the shared heap
  static NAMemory *makeSharedHeap(LockController *controller, SharedSegmentDB *segmentDB,
                                  CacheID cacheId = DESCRIPTOR_CACHE);
  static NAMemory *makeSharedHeap(NamedSemaphore *semaphore, CacheID cacheId = DESCRIPTOR_CACHE);

 protected:
  // cstr used to create a brand new cache DB
  SharedCacheDB(const char eyeCatcher[4], NAMemory *sharedHeap = NULL, bool isData = false);

  // cstr used to create a cache DB on existing cacheDB base addr.
  SharedCacheDB(){};

  ~SharedCacheDB() {}

  static SharedCacheDB *locate(char *&baseAddr, CacheID cacheId = DESCRIPTOR_CACHE);

  // helper routine to collect info about the shared heap:
  //   the base address (returned),
  //   the total heap size (output via totalHeapSize)
  //   the shared segment Id (input, in shmId)
  static char *collectSharedHeapInfo(int &shmId, size_t &totalHeapSize, CacheID cacheId = DESCRIPTOR_CACHE);

  // helper routine to return the address of the SharedCacheDB object
  static char *locateBaseAddr(int &shmId, CacheID cacheId = DESCRIPTOR_CACHE);

  // Answer the question whether SharedCacheDB is valid
  NABoolean isMe() {
    return (!strncmp(SHAREDCACHE_DB_EYE_CATCHER, eyeCatcher_, 4) && sharedHeap_ &&
            sizeOfSharedHeap_ == sizeof(*sharedHeap_));
  }

 protected:
  // to assure accessing the same C
  char eyeCatcher_[4];

  // The size of the total memory space in default segment that
  // the shared cache can use.  Set to SHARED_CACHE_SEG_SIZE
  // or getenv("SHARED_CACHE_SEG_SIZE") if the env is defined.
  size_t sizeOfDefaultSegment_;

  // to assure accessing the same H of exact size as created
  size_t sizeOfSharedHeap_;

  NAMemory *sharedHeap_;  // point at the shared NAHeap H

  // An array of pointers pointing at different shared caches in
  // the shared segment.
  SharedCache *sharedCaches_[MAX_SHARED_CACHES];

  // The manager of the extra shared segments.
  // The first shared segment is contained within the
  // RMS stats segment, for now.
  SharedSegmentDB sharedSegmentDB_;
};

class SharedCache {
  friend class SharedCacheDB;

 public:
  // The shared heap
  NAMemory *getSharedHeap() { return sharedHeap_; }

  // Create a semaphore name for use with shared cache in buf with
  // length len. Must check return status to make sure the name
  // is properly created on buffer buf with enough length.
  static char *createSemaphoreName(char *buf, int len);

  // Destroy a particular shared cache and deallocate occupied spaces
  // from the shared heap.
  NABoolean destroy(SharedCacheDB::CacheID cacheId);

 protected:
  SharedCache(const char eyeCatcher[4], NAMemory *sharedHeap, SharedCacheDB *sharedCacheDB);
  ~SharedCache() {}

 protected:
  NAMemory *sharedHeap_;  // point at the shared NAHeap H
  char eyeCatcher_[4];
  SharedCacheDB *sharedCacheDB_;  // point at the shared cacheDB
};

// A shared cache storing table meta-data descriptors keyed on the qualified name
// of the table. Each descriptor is the serialized object representing one
// TrafDesc.
#define DESCRIPTOR_CACHE_EYE_CATCHER "SDEC"
#define TABLESDATA_CACHE_EYE_CATCHER "SDAT"

class SharedDescriptorCache : public SharedCache {
 public:
  // Make a shared descriptor cache from the shared segment. This static
  // method is used to create a cache out of a blank shared segment.
  static SharedDescriptorCache *Make();

  static SharedDescriptorCache *destroyAndMake();

  // Locate a shared descriptor cache from the shared segment. This static
  // method is used when such a cache is already present on the shared
  // segment.
  static SharedDescriptorCache *locate();

  // Return
  //   the key (not null): if the insertion is succecssful
  //   NULL: otherwise
  QualifiedName *insert(QualifiedName *key, NAString *value, NABoolean *inserted = NULL);

  // iterate over all cached descrptors in the cache, without making
  // any copies of the (key, value) pairs in the cache.
  void findAll();

  // count all entries in the cache with type 'type', without making
  // any copies of the key-value pairs.
  int countAll(enum iteratorEntryType type);

  // find a particular cached descriptor by name.
  NAString *find(const QualifiedName &name, NABoolean includeDisabled = FALSE);

  // delete/enable/disable by name
  NABoolean remove(const QualifiedName &name);
  NABoolean enable(const QualifiedName &name);
  NABoolean disable(const QualifiedName &name);

  // delete/enable/disable by schema name
  NABoolean removeSchema(const QualifiedName &name);
  NABoolean enableSchema(const QualifiedName &name);
  NABoolean disableSchema(const QualifiedName &name);

  // destroy any elements of the cache and return their occupied
  // memory back to the shared cache. After this call, the object
  // becomes invalid and should not be used.
  NABoolean destroy();

  // Some methods for testing
  void populateWithSynthesizedData(int pairs = 1000, size_t maxLength = 1000);
  void lookUpWithSynthesizedData(int pairs = 1000);
  void enableDisableDeleteWithSynthesizedData(int pairs = 1000);

  static void testPopulateWithSynthesizedData(int pairs = 1000, size_t maxLenth = 1000);

  static void testHashDictionaryEnableDisableDelete(int pairs = 1000, size_t maxLenth = 1000);

  void display();
  char *collectSummaryDataForHeap();

  int entries(const QualifiedName &inSchName);
  int entries();

  char *collectSummaryDataForAll();
  char *collectSummaryDataForSchema(const QualifiedName &schemaName);
  char *collectSummaryDataForTable(const QualifiedName &tableName);

 protected:
  SharedDescriptorCache(NAMemory *sharedHeap, SharedCacheDB *sharedCacheDB);
  ~SharedDescriptorCache() {}

  // Answer the question whether this is a sharedDescriptorCache
  NABoolean isMe() {
    return (!strncmp(DESCRIPTOR_CACHE_EYE_CATCHER, eyeCatcher_, 4) && table_ && sizeOfTable_ == sizeof(*table_));
  }

  NABoolean sanityCheck() { return (table_ && table_->sanityCheck()); }

 protected:
  // A hash table allocated from sharedCache::sharedHeap_
  NAHashDictionary<QualifiedName, NAString> *table_;
  size_t sizeOfTable_;
};

// TODO:
//   need to add interfaces that operate on separate tables
class SharedTableDataCache : public SharedCache {
 public:
  static SharedTableDataCache *Make();

  static SharedTableDataCache *destroyAndMake();

  static SharedTableDataCache *locate();

  NABoolean destroy();

  MemoryTableDB *getMemoryTableDB() { return memoryTableDB_; }

  SharedTableDataCache(NAMemory *sharedHeap, SharedCacheDB *sharedCacheDB);
  ~SharedTableDataCache() {}

  char *collectSummaryDataForAll(bool showDetails, int &blackBoxLen);
  char *collectSummaryDataForSchema(const QualifiedName &schemaName);
  char *collectSummaryDataForTable(const QualifiedName &tableName);

  NABoolean remove(const QualifiedName &name);
  NABoolean enable(const QualifiedName &name);
  NABoolean disable(const QualifiedName &name);
  NABoolean contains(const QualifiedName &name, bool checkAll = false);

  NABoolean isEnable(const QualifiedName &name);
  int entries();
  void findAll();

 protected:
  bool isComplete() { return (memoryTableDB_->tableNameToCacheEntryMap()->isComplete()); }

  NABoolean isMe() {
    return (!strncmp(TABLESDATA_CACHE_EYE_CATCHER, eyeCatcher_, 4) && memoryTableDB_ &&
            sizeOfTable_ == sizeof(*memoryTableDB_));
  }

  NABoolean sanityCheck() { return (memoryTableDB_ && memoryTableDB_->tableNameToCacheEntryMap()->sanityCheck()); }

 protected:
  MemoryTableDB *memoryTableDB_;
  size_t sizeOfTable_;
};

// Must set the macro to 1 to protect shared cache updating
// through distributed lock. This is because the
// NAMemory/NAHeap classes built from Linux shared memory
// expect some kind of protection when the memory is
// allocated/deallocated. For concurrent DDLs, we must use
// distributed lock.
#define USE_DISTRIBUTED_LOCK_FOR_LOCKING 1

// Turn on the following macro to use semaphore based lock
// instead.  Semaphore based locking has the drawback that
// it is local (per node) and therefore we can not arrive
// at cluster level consistency with it, unless the master
// executor provides a dlock based protection (which is true).
//#define USE_SEMAPHORE_FOR_LOCKING 1

// The following section implements the protection through
// a named semaphore, which does not work when meta-data
// can be altered frequently, due to the reason that the
// semaphores are good within nodes and can not provide the
// across-node lock consistency.
#ifdef USE_SEMAPHORE_FOR_LOCKING
// The following macros must be used to lock and unlock
// shared cache for any operations on the cache that involve
// allocation or de-allocation of memory on the shared heap,
// such as insert/delete into the cache, or resize the cache.
//
// The common usage is as follows.
//
// LOCK_SHARED_CACHE;
//
// int retcode = 0;
// LOCK_SHARED_CACHE;
//
// if ( retcode != 0 ) {
//     error handling;
// } else {
//  for ( int i=0; i<n; i++ )
//    sharedCache->insert();
// }
//
// UNLOCK_SHARED_CACHE;
//
#define LOCK_SHARED_CACHE                                                     \
  SemaphoreForSharedCache namedSemaphore;                                     \
  retcode = namedSemaphore.lockSemaphore(true);                               \
  if (retcode == 0) {                                                         \
    SharedCacheDB *sharedDB = SharedCacheDB::locateOrCreate(&namedSemaphore); \
  }

#define UNLOCK_SHARED_CACHE                                     \
  {                                                             \
    SharedCacheDB *sharedDB = SharedCacheDB::locate();          \
    if (sharedDB) sharedDB->getSharedHeap()->setNamedSem(NULL); \
  }                                                             \
  namedSemaphore.lockSemaphore(false)

#define OBSERVE_SHARED_CACHE_LOCK   LOCK_SHARED_CACHE
#define UNOBSERVE_SHARED_CACHE_LOCK UNLOCK_SHARED_CACHE

#elif USE_DISTRIBUTED_LOCK_FOR_LOCKING
// The following macros must be used to distributly lock and unlock
// shared cache for any operations on the cache that involve
// allocation or de-allocation of memory on the shared heap,
// such as insert/delete into the cache, or resize the cache.
//
// When a DLOCK is secured elsewhere, the following OBSERVE macros
// must be used to observe the lock status for any operations on
// the cache that involve allocation or de-allocation of memory
// on the shared heap, such as insert/delete into the cache, or
// resize the cache.
//
// The common usage is as follows.
//
//
// OBSERVE_SHARED_CACHE;
//
//  for ( int i=0; i<n; i++ )
//    sharedCache->insert();
//
// UNOBSERVE_SHARED_CACHE;
//

// In a faked cluster environment (on workstation), we need
// to protect the entire update operation with a semaphore,
// since multiple ESPs are competeting to update the shared
// cache. This is not necessary on a non-faking cluster
// environment since there is only one ESP per node updating
// per shared cache updating statement. Concurrent update is
// serialized by a distributed lock, locked in
// placed in Statement::execute(), and unlocked in
// Statement::fetch() when reaching at EOF_ state. And here
// we just need to observe that the dlock is locked.
//
#define LOCK_SHARED_CACHE                                                          \
  SemaphoreForSharedCache *namedSemaphorePtr = NULL;                               \
  if (CmpCommon::context() && CURRCONTEXT_CLUSTERINFO->getNumVirtualNodes() > 0) { \
    namedSemaphorePtr = new SemaphoreForSharedCache();                             \
    retcode = namedSemaphorePtr->lockSemaphore(true);                              \
  }                                                                                \
  WaitedLockController dlock(SHARED_CACHE_DLOCK_KEY);                              \
  { SharedCacheDB *sharedDB = SharedCacheDB::locateOrCreate(&dlock); }

#define UNLOCK_SHARED_CACHE                                                        \
  if (CmpCommon::context() && CURRCONTEXT_CLUSTERINFO->getNumVirtualNodes() > 0) { \
    namedSemaphorePtr->lockSemaphore(false);                                       \
    delete namedSemaphorePtr;                                                      \
  }                                                                                \
  {                                                                                \
    SharedCacheDB *sharedDB = SharedCacheDB::locate();                             \
    if (sharedDB) sharedDB->getSharedHeap()->setLockController(NULL);              \
  }

// Use a distributed lock observer to guarantee that nobody else
// is updating the shared cache.
// In a non-faking cluster, we still see concurrent updating on shared
// cache and shared memory corruption. This is caused by some known reason,
// like ESPs allocation error that causes two or more ESPs launched on the
// same node updating shared cache or distributed lock timeout due to zk connection
// timeout, as well as some unknown reason. so we need a Semaphore for the
// shared cache.
#define OBSERVE_SHARED_CACHE_LOCK                                             \
  SemaphoreForSharedCache *namedSemaphorePtr = new SemaphoreForSharedCache(); \
  retcode = namedSemaphorePtr->lockSemaphore(true);                           \
  DistributedLockObserver observer(SHARED_CACHE_DLOCK_KEY);                   \
  {                                                                           \
    SharedCacheDB *sharedDB = SharedCacheDB::locateOrCreate(&observer);       \
    if (sharedDB) {                                                           \
      sharedDB->getSharedHeap()->setNamedSem(namedSemaphorePtr);              \
    }                                                                         \
  }

#define UNOBSERVE_SHARED_CACHE_LOCK                                              \
  {                                                                              \
    SharedCacheDB *sharedDB = SharedCacheDB::locate();                           \
    if (sharedDB) {                                                              \
      sharedDB->getSharedHeap()->setLockController(NULL);                        \
      QRLogger::log(CAT_SQL_SHARED_CACHE, LL_TRACE, "setLockController NULL\n"); \
      sharedDB->getSharedHeap()->setNamedSem(NULL);                              \
    }                                                                            \
  }                                                                              \
  if (namedSemaphorePtr) {                                                       \
    namedSemaphorePtr->lockSemaphore(false);                                     \
    delete namedSemaphorePtr;                                                    \
  }

#define OBSERVE_SHARED_DATA_CACHE_LOCK                                                                  \
  SemaphoreForSharedCache *namedSemaphorePtr = new SemaphoreForSharedCache(true);                       \
  retcode = namedSemaphorePtr->lockSemaphore(true);                                                     \
  DistributedLockObserver observer(SHARED_CACHE_DATA_DLOCK_KEY);                                        \
  {                                                                                                     \
    SharedCacheDB *sharedDB = SharedCacheDB::locateOrCreate(&observer, SharedCacheDB::TABLEDATA_CACHE); \
    if (sharedDB) {                                                                                     \
      sharedDB->getSharedHeap()->setNamedSem(namedSemaphorePtr);                                        \
    }                                                                                                   \
  }

#define UNOBSERVE_SHARED_DATA_CACHE_LOCK                                             \
  {                                                                                  \
    SharedCacheDB *sharedDB = SharedCacheDB::locate(SharedCacheDB::TABLEDATA_CACHE); \
    if (sharedDB) {                                                                  \
      sharedDB->getSharedHeap()->setLockController(NULL);                            \
      QRLogger::log(CAT_SQL_SHARED_CACHE, LL_TRACE, "setLockController NULL\n");     \
      sharedDB->getSharedHeap()->setNamedSem(NULL);                                  \
    }                                                                                \
  }                                                                                  \
  if (namedSemaphorePtr) {                                                           \
    namedSemaphorePtr->lockSemaphore(false);                                         \
    delete namedSemaphorePtr;                                                        \
  }

#endif

// Test methods invoked from tdm_arkcmp (as a test program) when
// proper test options are suppiled to tdm_arkcmp.
void testSharedMemorySequentialScan(int argc, char **argv);
void testSharedMemoryHashDictionaryPopulate();
void testSharedMemoryHashDictionaryLookup();
void testSharedMemoryHashDictionaryFindAll();
void testSharedMemoryHashDictionaryClean();
void testSharedMemoryHashDictionaryEnableDisableDelete();
int testMemoryExhaustion();

void testSharedDataMemoryHashDictionaryFindAll();

#endif
