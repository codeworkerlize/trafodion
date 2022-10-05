
#ifndef DLOCK_H
#define DLOCK_H

/* -*-C++-*-
 *****************************************************************************
 *
 * File:         <file>
 * Description:  Long held distributed lock
 *
 *
 * Created:      10/7/2019
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "executor/DistributedLock_JNI.h"

class LockController {
 public:
  LockController()
      : distLockInterface_(NULL), lockHeld_(false), lockMethodFailure_(false), isDedicated_(false), heap_(NULL){};
  LockController(bool isDedicated, NAHeap *heap)
      : distLockInterface_(NULL), lockHeld_(false), lockMethodFailure_(false), isDedicated_(isDedicated), heap_(heap){};

  virtual ~LockController();

  virtual bool lockHeld() { return lockHeld_; };
  bool lockMethodFailure() { return lockMethodFailure_; };

 protected:
  // The DistributedLock_JNI object handles one lock at a time. Most
  // locks are held only briefly; for those using the one in the
  // current context is fine. However, sometimes there is a need to
  // hold one for a longer period, with the possibility that other
  // locks may come and go in the interim. For that, a dedicated
  // DistributedLock_JNI object is used.
  bool isDedicated_;  // true if we are using a dedicated DistributedLock_JNI object
  NAHeap *heap_;      // used today only if isDedicated_ is true

  DistributedLock_JNI *distLockInterface_;
  bool lockHeld_;
  bool lockMethodFailure_;
};

// This class is a wrapper class for a long held distributed lock object.
// By definition, a long held distributed lock will return if it fails to
// secure a Zookeeper based distributed lock in two attempts. It does not
// wait with a time-out at all.
//
// Common usage:
//
//    {
//     LongHeldLockController lock(key, text, true or false, heap);
//
//      if ( lock.lockHeld() )
//         perform the business logic;
//      else {
//         if ( lock.lockMethodFailure() )
//           // locking failed due to exception
//         else
//           // somebody S has the lock already, lock.lockData() contains
//           // the pid of S and the 'text' used when S is constructed.
//      }
//    }
//
// If other distributed locks might come and go while this lock is held,
// "true" should be used for the third parameter of the constructor.
// If "false" is supplied, the assumption is no other locks will be created
// while this lock is held.
class LongHeldLockController : public LockController {
 public:
  LongHeldLockController(const char *lockKey, const char *commandText, bool isDedicated, NAHeap *heap);
  ~LongHeldLockController(){};

  const char *lockData() { return lockData_; };

 protected:
  enum { LOCKDATA_SIZE = 500 };
  char lockData_[LOCKDATA_SIZE];  // lock data from competing lock if there is one
};

// This class is a wrapper class for a distributed lock object.
// By definition, a distributed lock will return after it secures
// a lock within a time window 'timeout'.
//
// Common usage:
//
//    {
//     WaitedLockController lock(key, timeout);
//
//      if ( lock.lockHeld() )
//         perform the business logic;
//      else {
//         // timeout
//         handle the timeout;
//      }
//    }
class WaitedLockController : public LockController {
 public:
  WaitedLockController(const char *lockKey, long timeout = 0 /*in ms, ==0: wait indefinitely*/);
  ~WaitedLockController(){};

  // A test routine that locks it down
  // for duration seconds.
  static void lockItDown(long duration);

 protected:
  long timeout_;
};

// for generate stored stats
// we need an independent DistributedLock_JNI instance for DistributedLockWrapper,
// otherwise, it will conflict with sharedcache
class IndependentLockController : public LockController {
 public:
  IndependentLockController(const char *lockKey, long timeout = 0 /*in ms, ==0: wait indefinitely*/,
                            bool isDedicated = true);
  ~IndependentLockController(){};

 protected:
  long timeout_;
};

// An observer class to observe the lock state of a ZK based
// distributed lock.
class DistributedLockObserver : public LockController {
 public:
  DistributedLockObserver(const char *lockKey, UInt32 useCount = 100);
  ~DistributedLockObserver();

  // observe whether a lock is in place.
  bool lockHeld();

  bool listNodes();

  // watch the lock at interval seconds
  static void watchDLocks(const char *lockName, int interval = 1);

  // list the zk nodes for the lock at interval seconds
  static void listDLocks(const char *lockName, int interval = 1);

 protected:
  NAString lockKey_;
  UInt32 useCount_;  // number of times that the cached value in
                     // lockHeld_ will be returned without going to ZK.
  UInt32 ct_;        // current count of the number of times the
                     // cached value has been returned. When this value
                     // reaches useCount_, we will go to ZK to observe
                     // the lock state.
};

#define SHARED_CACHE_DLOCK_KEY          "sharedCache"
#define SHARED_CACHE_DATA_DLOCK_KEY     "sharedDATACache"
#define GENERATE_STORED_STATS_DLOCK_KEY "genStoredDesc_"

#endif
