// @@@ START COPYRIGHT @@@
//
// (c) Copyright 2019 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         NamedSemaphore.cpp
 * Description:  
 * Created:      06/05/2019
 * Language:     C++
 *
 *****************************************************************************
 */
#include <sys/wait.h>

#include "common/ComSmallDefs.h"
#include "sqlcomp/NamedSemaphore.h"
#include "common/ComRtUtils.h"
#include "runtimestats/SqlStats.h"
#include "sqlcomp/SharedCache.h"
#include "qmscommon/QRLogger.h"

#define SEMFLAGS	          0600

NamedSemaphore::NamedSemaphore(const char* semName) :
    semId_(NULL),
    semPid_(-1),
    lastOpenStatus_(-1)
{
  releasingTimestamp_.tv_sec = 0;
  releasingTimestamp_.tv_nsec = 0;

  lockingTimestamp_.tv_sec = 0;
  lockingTimestamp_.tv_nsec = 0;

  if ( semName )
  {
     if ( semName != semName_ ) {
        if ( strlen(semName) < sizeof(semName_) )
           strcpy(semName_, semName);
        else 
           return;
     }

     openSemaphore();
  } 
}
   
NamedSemaphore::~NamedSemaphore() 
{ 
   closeSemaphore(); 
}

int NamedSemaphore::openSemaphore()
{
  int error = 0;

  sem_t *ln_semId = sem_open(semName_, 0);
  if (ln_semId == SEM_FAILED)
  {
    if (errno == ENOENT)
    {
      ln_semId = sem_open(semName_, O_CREAT, SEMFLAGS, 1);
      if (ln_semId == SEM_FAILED)
        error = errno;
    }
    else
      error = errno;
  }
  lastOpenStatus_ = error;

  if (error == 0)
    semId_ = ln_semId;
  return error; 
}

int NamedSemaphore::closeSemaphore()
{
  if ( semId_ == NULL )
    return -2;

  int error = sem_close(semId_);
  return error; 
}

int NamedSemaphore::getSemaphore()
{
  int error = 0;
  timespec ts;

  if ( semId_ == NULL )
    return -2;

  error = sem_trywait(semId_);

  NABoolean retrySemWait = FALSE;
  NABoolean resetClock = TRUE;
  if (error != 0)
  {
    do
    { 
    retrySemWait = FALSE;
    if (resetClock)
    {
       if ((error = clock_gettime(CLOCK_REALTIME, &ts)) < 0) {
          error = errno;
          return error; 
       }
       ts.tv_sec += 3;
    }
    resetClock = FALSE;
    error = sem_timedwait(semId_, &ts);

    if (error != 0) 
    {
      switch (errno)
      {
        case EINTR: // 4
           retrySemWait = TRUE;
           break;
        case EINVAL: // 22
           error = openSemaphore();
           if (error == 0)
               retrySemWait = TRUE;
           break;
        case ETIMEDOUT: // 110
           retrySemWait = TRUE;
           resetClock = TRUE;
           break;
        default:
           error = errno;
           break;
       }
      }
    }
    while (retrySemWait);
  }
  if (error == 0)
  {
    semPid_ = getpid();
    clock_gettime(CLOCK_REALTIME, &lockingTimestamp_);
  }
  return error;
}

int NamedSemaphore::releaseSemaphore()
{
  int error = 0;

  if (semId_ == NULL)
    return -2;

  error = sem_post(semId_);

  switch (error)
   {
    case EINVAL:
    case EOVERFLOW:
     error = errno;
     break;
  
    case 0:
     semPid_ = -1;
     clock_gettime(CLOCK_REALTIME, &releasingTimestamp_);
     break;

    default:
     assert(0);
     break;
   }

  return error;
}

void NamedSemaphore::display(const char* msg)
{
  fstream& out = getPrintHandle();
  out << msg 
      << ", status of NamedSemaphore:"
      << " this=" << static_cast<void*>(this) 
      << " semName_=" << semName_ 
      << " semId_=" << semId_
      << ", semPid_=" << semPid_
      << ", lastOpenStatus_=" << lastOpenStatus_
      << ", errno (EINTR=" << EINTR
      << ", EINVAL=" << EINVAL
      << ", EAGAIN=" << EAGAIN
      << ", ETIMEDOUT=" << ETIMEDOUT
      << ")"
      << endl;
  out.close();
}

void visitCriticalSection(int i)
{
  NamedSemaphore sp("/testNamedSemaphore1");

  char buf[200];
  sprintf(buf, "visiting critical section for client %d", i);
  sp.display(buf);

  int getStatus = sp.getSemaphore();

  sprintf(buf, "after calling getSemaphore(), getStatus=%d", getStatus);
  sp.display(buf);

  sleep(i); 

  int releaseStatus = sp.releaseSemaphore();

  sprintf(buf, "after calling releaseSemaphore(), releaseStatus=%d", releaseStatus);
  sp.display(buf);
}

void testNamedSemaphore()
{
 // fork n processes, each tries to enter the same critical
 // section protectec by a named semaphore.
 const int n = 6;
 pid_t pids[n];
 pid_t pid;

 for (int i=0; i<n; i++) {

    pid = fork();

    switch (pid) {

      case 0:
         // child process case
         visitCriticalSection(i);
         return;

      case -1:
         // error
         break; 

      default:
       // parent process case
       pids[i] = pid;
       if (i == n-1)
         visitCriticalSection(i);
    }
  }

 for (int i=0; i<n; i++) {
   int state;
   waitpid(pids[i], &state, 0);
 }
}

NamedSem::NamedSem(key_t semKey) :
  semId_(-1),
  semPid_(-1),
  semKey_(semKey)
{
  getSemaphore(true);
}

NamedSem::~NamedSem()
{
}

int NamedSem::getSemaphore(NABoolean create)
{
  int error = 0;
  if(semId_ == -1)
  {
    int semid;
    if((semid = semget(semKey_, 0, 0666 )) == -1)
    {
      if((errno == ENOENT) && create)
      {
        if((semid = semget(semKey_, 1, IPC_CREAT | IPC_EXCL | 0666 )) == -1)
        {
          QRLogger::log(CAT_SQL_SHARED_CACHE, LL_TRACE, "semget with create, errno = %d, semid = %d\n", errno, semid); 
          error = errno;
        }
        if (error == 0) //created successfully
        {
          semId_ = semid;
          QRLogger::log(CAT_SQL_SHARED_CACHE, LL_TRACE, "semctl SETVAL 1\n"); 
          if(semctl(semId_, 0, SETVAL, 1) == -1)
          {
            QRLogger::log(CAT_SQL_SHARED_CACHE, LL_TRACE, "semctl SETVAL 1, errno = %d, semId_ = %d\n", errno, semId_); 
            error = errno;
            removeSemaphore();
          }
        } 
        else if (error == EEXIST) //created by others
        { //do get again
          QRLogger::log(CAT_SQL_SHARED_CACHE, LL_TRACE, "created by others, do semget\n"); 
          if((semid = semget(semKey_, 0, 0666 )) == -1)
          {
            QRLogger::log(CAT_SQL_SHARED_CACHE, LL_TRACE, "do semget again error, errno = %d, semid = %d\n", errno, semid);
            error = errno;
          }
        }
      }
      else
        error = errno;
    }
    if(error == 0)
      semId_ = semid;
  }
  return error;
}

int NamedSem::removeSemaphore()
{
  if(semId_ == -1)
    return -2;
	
  int error = semctl(semId_, 0, IPC_RMID);
  semId_ = -1;
	
  return error;
}

int NamedSem::lockSemaphore(NABoolean lock)
{
  int error = 0;
  
  error = getSemaphore(false);
  if (error != 0)
    return error;
  
  if (semId_ == -1)
    return -2;

  struct sembuf sem_op;
  sem_op.sem_num = 0;
  if(lock)
    sem_op.sem_op = -1;
  else
    sem_op.sem_op = 1;
  sem_op.sem_flg = SEM_UNDO;
	  if (lock)
    QRLogger::log(CAT_SQL_SHARED_CACHE, LL_TRACE, "before lock: value is %d, semId_ is %d, sem_op.sem_op = %d, errno = %d\n",
                                        semctl(semId_,0,GETVAL,0), semId_, sem_op.sem_op, errno);
  else
    QRLogger::log(CAT_SQL_SHARED_CACHE, LL_TRACE, "before unlock: value is %d, semId_ is %d, sem_op.sem_op = %d, errno = %d\n",
                                        semctl(semId_,0,GETVAL,0), semId_, sem_op.sem_op, errno);

  do {
    error = semop(semId_, &sem_op, 1);
  } while (error < 0 && errno == EINTR);

  if(error < 0)
    error = errno;
  if (lock)
    QRLogger::log(CAT_SQL_SHARED_CACHE, LL_TRACE, "lock: value is %d, semId_ is %d, sem_op.sem_op = %d, errno = %d\n",
                                        semctl(semId_,0,GETVAL,0), semId_, sem_op.sem_op, error);
  else
    QRLogger::log(CAT_SQL_SHARED_CACHE, LL_TRACE, "unlock: value is %d, semId_ is %d, sem_op.sem_op = %d,errno = %d\n",
                                        semctl(semId_,0,GETVAL,0), semId_, sem_op.sem_op, error);
  if(error == 0)
    if (lock)
      semPid_ = getpid();
    else
      semPid_ = -1;
  return error;
}

SemaphoreForSharedCache::SemaphoreForSharedCache(bool isCreatedForData)
  : NamedSem(SharedSegment::getKeyForDefaultSegment(isCreatedForData))
{
}
