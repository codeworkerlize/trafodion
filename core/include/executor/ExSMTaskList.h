
#ifndef EXSM_TASKLIST_H
#define EXSM_TASKLIST_H

#include <pthread.h>

#include "ExSMCommon.h"

class ExSMTask;
class ExSMTaskList;
class ExSMQueue;

class ExSMTaskList {
 public:
  ExSMTaskList();

  virtual ~ExSMTaskList();

  void lock();
  void unlock();

  void addTask(ExSMTask *t);
  void removeTask(ExSMTask *t);

  uint32_t getNumTasks() { return numTasks_; }

  ExSMTask *findTask(const sm_target_t &target, bool doLock, bool doTrace);

 protected:
  ExSMTask **buckets_;
  uint32_t numBuckets_;
  uint32_t numTasks_;
  int64_t numInserts_;
  int64_t numCollisions_;

  pthread_mutex_t taskListMutex_;

};  // class ExSMTaskList

#endif  // EXSM_TASKLIST_H
