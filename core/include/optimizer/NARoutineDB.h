
#ifndef NAROUTINEDB_H
#define NAROUTINEDB_H

#include "common/Collections.h"
#include "common/NAMemory.h"
#include "optimizer/ObjectNames.h"

class NARoutineDBKey;
class NARoutine;
class NARoutineCacheStats;

//
// External class definitions
//
// class NARoutine;

// Used as inital list size to construct a NARoutineDB object
#define NARoutineDB_INIT_SIZE 23

//
// NARoutineDB
// Lookup class for NARoutine
//
class NARoutineDB : public NAKeyLookup<NARoutineDBKey, NARoutine> {
  friend class NARoutineCacheStatStoredProcedure;

 public:
  NARoutineDB(NAMemory *h = 0);

  void resetAfterStatement();
  NARoutine *get(BindWA *bindWA, const NARoutineDBKey *key);
  void put(NARoutine *routine);
  NABoolean cachingMetaData();
  void moveRoutineToDeleteList(NARoutine *cachedNARoutine, const NARoutineDBKey *key);
  void free_entries_with_QI_key(int numSiKeys, SQL_QIKEY *qiKeyArray);
  void reset_priv_entries();
  void removeNARoutine(QualifiedName &routineName, ComQiScope qiScope, long objUID, NABoolean ddlXns,
                       NABoolean atCommit);

  // Defined member functions.
  void setCachingON() {
    resizeCache(defaultCacheSize_);
    cacheMetaData_ = TRUE;
  }
  void setCachingOFF() {
    flushCache();
    cacheMetaData_ = FALSE;
  }

  int getHighWatermarkCache() { return highWatermarkCache_; }
  int getTotalLookupsCount() { return totalLookupsCount_; }
  int getTotalCacheHits() { return totalCacheHits_; }
  NAString &getMetadata() { return metadata; }
  void setMetadata(const char *meta) { metadata = meta; }

  inline void resizeCache(int sizeInBytes) { maxCacheSize_ = sizeInBytes; };
  inline void refreshCacheInThisStatement() { refreshCacheInThisStatement_ = TRUE; }

  void getCacheStats(NARoutineCacheStats &stats);
  void invalidateSPSQLRoutines(LIST(NARoutine *) & routines);

 private:
  // Remove everything from cache.
  void flushCache();

  // Reduce size of cache if it exceeds limit, by using LRU algorithm.
  NABoolean enforceMemorySpaceConstraints();
  long getModifyTime(NARoutine &routine, int &error);
  long getRedefTime(BindWA *bindWA, NARoutine &routine, int &error);
  long getSchemaRedefTimeFromLabel(NARoutine &routine, int &error);

  NAMemory *heap_;

  NABoolean cacheMetaData_;
  NAString metadata;

  // Maximum, default, and current size of the cache in bytes.
  int maxCacheSize_;
  int defaultCacheSize_;
  int currentCacheSize_;
  // Number of entries in cache.
  int entries_;

  // List of routines to be deleted.
  // This list is used to collect routines that will be deleted after the statement
  // completes to avoid affecting compile time.  The delete of the elements in this
  // list is done in NARoutineDB::resetAfterStatement, which is called indirectly by
  // the CmpStatement destructor.
  LIST(NARoutine *) routinesToDeleteAfterStatement_;

  // This flag indicates that the entries (i.e. NARoutine objects) used during the
  // current statement should be re-read from disk instead of using the entries
  // already in the cache. This helps to refresh the entries used in the current
  // statement.
  NABoolean refreshCacheInThisStatement_;

  // Pointer to current location in the cachedRoutineList_.
  // Used for cache entry replacement purposes
  int replacementCursor_;

  // Statistics counters.
  int highWatermarkCache_;  // High watermark of currentCacheSize_
  int totalLookupsCount_;   // NARoutine entries lookup counter
  int totalCacheHits_;      // cache hit counter

};  // class NARoutineDB

#endif
