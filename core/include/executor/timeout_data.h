/* -*-C++-*-
 *****************************************************************************
 *
 * File:         timeout_data.h
 * Description:  Encapsulation of all the dynamicly set timeout values
 *
 * Created:      1/12/2000
 * Language:     C++
 *
 *

 *
 *****************************************************************************
 */

#ifndef TIMEOUT_DATA_H
#define TIMEOUT_DATA_H

#include "common/IpcMessageType.h"  // for IpcMessageObjSize,  *IpcMessageBufferPtr,
#include "common/NAMemory.h"
#include "export/NABasicObject.h"
//  and *IpcConstMessageBufferPtr

class ComTdbRoot;

// **********************************************************************
//   ====  TimeoutHashTable  ====
// Internal hash table to hold pairs of < tableName, timeoutValue >
// Uses "in place" hashing; i.e., collisions are handled during insert()
// by placing the new entry in the next empty slot in the hash array.
// ( Thus remove() has to do some work to maintain this invariant.)
// **********************************************************************

//////////////////////////////////////////////////////////////////////
//  The TimeoutTableEntry is just an entry in the hash table/array
//  The only complexity is the size of the table name. Short table
//  names are kept in place, but the long ones are allocated on the
//  heap instead.
/////////////////////////////////////////////////////////////////////
class TimeoutTableEntry : public NABasicObject {
 public:
  TimeoutTableEntry() : tableNamePtr_(NULL), heap_(NULL){};
  ~TimeoutTableEntry() { reset(); };

  inline char *tableName()  // return the table name
  {
    if (tableNamePtr_)
      return tableNamePtr_;
    else
      return tableName_;
  };

  void setTableName(char *tableName, CollHeap *heap);

  // deallocate long table name, if needed; set to "unused"
  inline void reset() {
    if (tableNamePtr_) heap_->deallocateMemory(tableNamePtr_);
    init();
  };

  inline void init() {
    tableNamePtr_ = NULL;
    used = FALSE;
  };

  // public data members: directly accessed entry fields
  // ( A lax design here, since this class is only used by TimeoutHashTable )
  int timeoutValue;
  int hashValue;
  NABoolean used;

 private:
  enum { MAX_INTERNAL_TN = 40 };     // long enough for a full guardian file name
  char tableName_[MAX_INTERNAL_TN];  // for the short names (most common)
  char *tableNamePtr_;               // in case the table name is long ( > 40 ; this was
  // meant for those long ANSI table names; not currently used as we only
  // keep guardian file names now.)
  CollHeap *heap_;  // the heap a long tablename is allocated from
};

class TimeoutHashTable : public NABasicObject {
 public:
  TimeoutHashTable(CollHeap *heap, int hashTableSize = 16);

  ~TimeoutHashTable() {
    if (hashArray_ == NULL) return;

    // traverse array, deallocating long names if needed
    for (int u = 0; u < hashTableSize_; u++) hashArray_[u].reset();
    heap_->deallocateMemory(hashArray_);
  };

  void insert(char *tableName, int timeoutValue);

  void remove(char *tableName);

  // find and set the timeout value for this table; return FALSE iff not found
  NABoolean getTimeout(char *tableName, int &timeoutValue);

  // remove all the entries from the hash table
  void clearAll();

  int entries() { return entries_; };

  // Methods for packing/unpacking (used for passing information to ESPs)
  IpcMessageObjSize packedLength();  // return the length of the packed obj
  // pack or unpack, and move the pointer ("buffer") accordingly
  void packIntoBuffer(IpcMessageBufferPtr &buffer);
  void unpackObj(IpcConstMessageBufferPtr &buffer);

 private:
  int entries_;                   // number of entries in this HashQueue
  int hashTableSize_;             // current size of the hash table
  int hashTableOriginalSize_;     // original size of the hash table
  int tempIndex_;                 // temporary, set by getTimeout(), used by remove()
  TimeoutTableEntry *hashArray_;  // the hash table itself
  CollHeap *heap_;                // the heap a HashQueue allocates from

  void resizeHashTableIfNeeded();
  TimeoutTableEntry *allocateHashTable(int size);
  // internal routine: insert entry into H.T., return TRUE iff entry is new
  NABoolean internalInsert(TimeoutTableEntry **hashTable, int hashTableSize, char *tableName, int timeoutValue);
  int getTimeoutHashValue(char *tableName);
};

// **********************************************************************

/////////////////////////////////////////////////////////
//
//   TimeoutData  class
//
/////////////////////////////////////////////////////////
class TimeoutData : public NABasicObject {
 public:
  TimeoutData(CollHeap *heap, int estimatedMaxEntries = 16);  // ctor

  ~TimeoutData(){};  // do nothing dtor

  // for lock timeout
  void setAllLockTimeout(int lockTimeoutValue);
  void resetAllLockTimeout();
  void setTableLockTimeout(char *tableName, int lockTimeoutValue);
  void resetTableLockTimeout(char *tableName);
  NABoolean noLockTimeoutsSet() { return noLockTimeoutsSet_; }
  NABoolean getLockTimeout(char *tableName, int &timeoutValue);

  // for stream timeout
  void setStreamTimeout(int streamTimeoutValue);
  void resetStreamTimeout();
  NABoolean isStreamTimeoutSet() { return streamTimeoutSet_; };
  int getStreamTimeout() { return streamTimeoutValue_; };

  // copy relevant data from this TimeoutData object into another object,
  // The "anotherTD" may need to be allocated (if there's relevant data)
  // lnil: gives the list of relevant tables (other timeouts are skipped)
  void copyData(TimeoutData **anotherTD, CollHeap *heap, ComTdbRoot *rootTdb);

  // Check if another TimeoutData object is up to date with this one
  // ( lnil: gives the list of relevant tables (other timeouts are skipped)
  //   usingStreams: tells if streams are used in this statement )
  NABoolean isUpToDate(TimeoutData *anotherTD, ComTdbRoot *rootTdb);

  // Methods for packing/unpacking (used for passing information to ESPs)
  IpcMessageObjSize packedLength();  // return the length of the packed obj
  // pack or unpack, and move the pointer ("buffer") accordingly
  void packIntoBuffer(IpcMessageBufferPtr &buffer);
  void unpackObj(IpcConstMessageBufferPtr &buffer);

#if _DEBUG  // for debugging only
  int entries() { return timeoutsHT_.entries(); };
#endif

 private:
  // For lock timeouts
  NABoolean noLockTimeoutsSet_;  // flag to speed up checking a common case
  // If noLockTimeoutsSet_ == TRUE, the following 3 fields are not used !!
  NABoolean forAll_;             // Was the timeout set for all tables (i.e. *)
  int forAllTimeout_;            // Lock timeout value for all tables (if set)
  TimeoutHashTable timeoutsHT_;  // List of specified per table lock-timeouts

  // for stream timeout
  NABoolean streamTimeoutSet_;  // Was the stream timeout set ?
  // If streamTimeoutSet_ == FALSE, the following field is ignored
  int streamTimeoutValue_;  // The process-global stream timeout

  // an internal utility method: Check if there's relevant timeout data
  NABoolean anyRelevantTimeoutData(ComTdbRoot *rootTdb);
};

#endif
