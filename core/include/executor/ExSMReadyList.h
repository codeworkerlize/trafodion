
#ifndef EXSM_READYLIST_H
#define EXSM_READYLIST_H

#include <pthread.h>

class ExSMTask;

class ExSMReadyList {
 public:
  ExSMReadyList();
  virtual ~ExSMReadyList();
  void add(ExSMTask *t);
  void remove(ExSMTask *t);
  ExSMTask *getFirst();

 protected:
  ExSMTask *head_;

};  // class ExSMReadyList

#endif  // EXSM_READYLIST_H
