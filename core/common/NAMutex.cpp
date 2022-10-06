// ***********************************************************************

#include "NAMutex.h"
#include "common/NAAssert.h"

void NAMutex::init(bool recursive, bool enabled, bool shared) {
  isRecursive_ = recursive;
  isEnabled_ = enabled;
  isShared_ = shared;

  if (enabled) {
    pthread_mutexattr_t Attr;

    pthread_mutexattr_init(&Attr);
    if (recursive) pthread_mutexattr_settype(&Attr, PTHREAD_MUTEX_RECURSIVE);
    if (shared) {
      pthread_mutexattr_setpshared(&Attr, PTHREAD_PROCESS_SHARED);
    }
    int rc = pthread_mutex_init(&mutex_, &Attr);

    assert(rc == 0);
  }
}

void NAMutex::destoryAndInit() {
  bool recursive = isRecursive_;
  bool enabled = isEnabled_;
  bool shared = isShared_;
  pthread_mutex_destroy(&mutex_);
  init(recursive, enabled, shared);
}
void NAMutex::enable() {
  if (!isEnabled_) init(isRecursive_, true, isShared_);
}

void NAConditionVariable::wait() {
  NAMutexScope ms(*this);

  int retcode = pthread_cond_wait(&threadCond_, &mutex_);
  assert(retcode == 0);
}

void NAConditionVariable::resume() {
  // TODO: Do we really need to lock the mutex before calling pthread_cond_signal?
  // See http://pubs.opengroup.org/onlinepubs/7908799/xsh/pthread_cond_signal.html
  NAMutexScope ms(*this);

  int retcode = pthread_cond_signal(&threadCond_);
  assert(retcode == 0);
}

NAMutexScope::NAMutexScope(NAMutex &mutex) : mutex_(mutex) {
  if (mutex.isEnabled()) {
    int rc = mutex.lock();
    assert(rc == 0);
  }
}

NAMutexScope::~NAMutexScope() {
  if (mutex_.isEnabled()) {
    int rc = mutex_.unlock();
    assert(rc == 0);
  }
}

NAConditionalMutexScope::~NAConditionalMutexScope() {
  while (lockCount_ > 0) release();
}

void NAConditionalMutexScope::release() {
  if (mutex_ && mutex_->isEnabled()) {
    assert(lockCount_ > 0);
    int rc = mutex_->unlock();
    assert(rc == 0);
    lockCount_--;
  }
}

void NAConditionalMutexScope::reAcquire() {
  if (mutex_ && mutex_->isEnabled()) {
    int rc = mutex_->lock();
    assert(rc == 0);
    lockCount_++;
  }
}
