// @@@ START COPYRIGHT @@@
//
// (c) Copyright 2019 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@
#ifndef NAMED_SEMAPHORE_H
#define NAMED_SEMAPHORE_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         NamedSemaphore.h
 * Description:  
 * Created:      6/05/2019
 * Language:     C++
 *
 *****************************************************************************
 */

#include <fcntl.h>
#include <semaphore.h>
#include <errno.h>
#include <time.h>
#include <sys/types.h>
#include <sys/sem.h>
#include "Platform.h"

class NamedSem;

class NamedSemaphore
{
public:
   NamedSemaphore(const char* semName);
   ~NamedSemaphore();

   int getSemaphore();
   int releaseSemaphore();

   inline pid_t getSemPid() { return semPid_; }
   inline sem_t* getSemId() { return semId_; }

   // the status code of last call to
   // openSemaphore() 
   NABoolean lastOpenStatus() { return lastOpenStatus_; }

   NABoolean semaphoreLocked() { return (semPid_ != -1); }

   void display(const char* msg = "");

protected:
   int openSemaphore();
   int closeSemaphore();

protected:
  // the name of the semaphore
  char semName_[NAME_MAX-4];

private:

  // semaphore id
  sem_t* semId_;    

  // Pid of the process that holds semaphore lock 
  // - This element is used for debugging purpose on
  pid_t semPid_;    

  // the last return value from openSemaphore()
  int lastOpenStatus_;

  timespec lockingTimestamp_;
  timespec releasingTimestamp_;
};

//System V semaphore
class NamedSem
{
public:
   NamedSem(key_t semKey);
   ~NamedSem();

   int lockSemaphore(NABoolean lock);

   inline pid_t getSemPid() { return semPid_; }
   inline int getSemId() { return semId_; }

   NABoolean semaphoreLocked() { return (semPid_ != -1); }

protected:
   int getSemaphore(NABoolean create);
   int removeSemaphore();

protected:
  // the key of the semaphore
  key_t semKey_;

private:

  // semaphore id
  int semId_;    

  // Pid of the process that holds semaphore lock 
  // - This element is used for debugging purpose on
  pid_t semPid_;    

};

class SemaphoreForSharedCache : public NamedSem
{

public:
   SemaphoreForSharedCache(bool isCreatedForData = false);
   ~SemaphoreForSharedCache() {}
};

void testNamedSemaphore();
#endif
