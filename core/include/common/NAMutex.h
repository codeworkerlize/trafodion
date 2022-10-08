// ***********************************************************************

#ifndef NAMUTEX__H
#define NAMUTEX__H

#include <pthread.h>

class NAMutex {
  friend class NAMutexScope;

 public:
  NAMutex(bool recursive, bool enabled, bool shared = false) { init(recursive, enabled, shared); }
  NAMutex(const NAMutex &other) { init(other.isRecursive_, other.isEnabled_, other.isShared_); }
  ~NAMutex() { pthread_mutex_destroy(&mutex_); }

  bool isEnabled() { return isEnabled_; }
  void enable();

  int lock() { return pthread_mutex_lock(&mutex_); }
  int unlock() { return pthread_mutex_unlock(&mutex_); }
  int trylock() { return pthread_mutex_trylock(&mutex_); }

  void destoryAndInit();

 protected:
  void init(bool recursive, bool enabled, bool shared);

  pthread_mutex_t mutex_;
  bool isRecursive_;
  bool isEnabled_;
  bool isShared_;
};

class NAConditionVariable : public NAMutex {
 public:
  NAConditionVariable() : NAMutex(false, true) { pthread_cond_init(&threadCond_, NULL); }
  ~NAConditionVariable() { pthread_cond_destroy(&threadCond_); }

  void wait();    // called from the thread that owns the condition variable
  void resume();  // called from another thread to wake the owning thread

 private:
  // condition variable to wake up the thread
  pthread_cond_t threadCond_;
};

// -------------------------------------------------------------------------
// A class to do a simple critical section, using a stack variable.
//
// This class is intended to be allocated on the stack, before entering
// a critical section. The end of the section is the end of the scope.
// The C++ runtime will ensure that we will unlock the mutex, even in
// case of exceptions or early return statements. Example:
//
// #include "common/NAMutex.h"
//
// NAMutex mutex;
//
// void criticalFunction()
// {
//   NAMutexScope(mutex);
//
//   /* do some processing */
//
// } /* end of block, mutex destructor gets called and unlocks */
// -------------------------------------------------------------------------
class NAMutexScope {
 public:
  NAMutexScope(NAMutex &mutex);

  ~NAMutexScope();

 private:
  // not written
  NAMutexScope();

  NAMutex &mutex_;
};

// a more complex class, accepting a pointer for the mutex, which can be NULL,
// and allowing the user to temporarily release the mutex
class NAConditionalMutexScope {
 public:
  NAConditionalMutexScope(NAMutex *mutex) : mutex_(mutex), lockCount_(0) { reAcquire(); }

  ~NAConditionalMutexScope();

  // methods to temporarily release the mutex in a mutex scope
  void release();
  void reAcquire();

 private:
  // not written
  NAConditionalMutexScope();

  NAMutex *mutex_;
  int lockCount_;
};

#endif
