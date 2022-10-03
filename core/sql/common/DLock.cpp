// @@@ START COPYRIGHT @@@
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// @@@ END COPYRIGHT @@@
//
//
#include <sys/time.h>
#include "common/DLock.h"
#include <unistd.h>
#include "common/ComRtUtils.h"

void genArkcmpInfo(NAString& nidpid);

LockController::~LockController()
{
  if (distLockInterface_ && lockHeld_)
    {
      int rc = distLockInterface_->unlock();
    }
  if (distLockInterface_ && isDedicated_)
    {
      NADELETE(distLockInterface_, DistributedLock_JNI, heap_);
    }
}

LongHeldLockController::LongHeldLockController(const char* lockKey,
  const char* commandText, bool isDedicated, NAHeap * heap) 
: LockController(isDedicated,heap)
{
  if (isDedicated_) // create a dedicated DistributedLock_JNI instance
    {
      DL_RetCode dlr = DL_OK;
      distLockInterface_ = DistributedLock_JNI::newInstance(heap_, dlr /* out, but ignored */);
    }
  else // use the shared one (it's serially reusable)
    distLockInterface_ = DistributedLock_JNI::getInstance();

  if (distLockInterface_)
    {
      char data[LOCKDATA_SIZE];

      // get our process name and nid:pid

      NAString nidPid;
      genArkcmpInfo(nidPid);
      strcpy(data,nidPid.data());
      
      // record what command we are running
      char * next = data+strlen(data);
      snprintf(next,LOCKDATA_SIZE-strlen(data)," is performing %s",commandText);

      if (strlen(data) == LOCKDATA_SIZE-1)
        {
          // command Text is too long; add an elipsis to show that we truncated it
          strcpy(data+LOCKDATA_SIZE-4,"...");
        }

      int rc = distLockInterface_->longHeldLock(lockKey,data,lockData_,LOCKDATA_SIZE);

      if ((rc == DL_OK) && (lockData_[0] == '\0'))  // if call was successful and there is no other lock holder
        lockHeld_ = true;

      if (rc != DL_OK)
        lockMethodFailure_ = true;
    }
  else
    lockMethodFailure_ = true;  // couldn't get distLockInterface
}

WaitedLockController::WaitedLockController(const char* lockKey, long timeout)
: timeout_(timeout)
{
  distLockInterface_ = DistributedLock_JNI::getInstance();

  if (distLockInterface_)
    {
      int rc = distLockInterface_->lock(lockKey, timeout_);
      lockHeld_ = (rc == DL_OK);
    }
  else
    lockMethodFailure_ = true;  // couldn't get distLockInterface
}

IndependentLockController::IndependentLockController(const char* lockKey, long timeout, bool isDedicated)
: LockController(isDedicated,NULL)
, timeout_(timeout)
{
  if (isDedicated)
    {
      DL_RetCode dlr = DL_OK;
      distLockInterface_ = DistributedLock_JNI::newInstance(heap_, dlr /* out, but ignored */);
    }
  else
    distLockInterface_ = DistributedLock_JNI::getInstance();

  if (distLockInterface_)
    {
      int rc = distLockInterface_->lock(lockKey, timeout_);
      lockHeld_ = (rc == DL_OK);
    }
  else
    lockMethodFailure_ = true;  // couldn't get distLockInterface 
}

DistributedLockObserver::DistributedLockObserver
(const char* lockKey, UInt32 useCount) : 
       LockController(), 
       lockKey_(lockKey), 
       useCount_(useCount),
       ct_(0) 
{
  distLockInterface_ = DistributedLock_JNI::getInstance();
}

DistributedLockObserver::~DistributedLockObserver()
{ 
  // set it to false so that in the base class
  // unlock() will not be called.
  lockHeld_ = false;
}

bool DistributedLockObserver::lockHeld()
{
  if (!distLockInterface_)
    return false;

  if (ct_ == 0)
    {
      bool locked = false;
      int rc = distLockInterface_->observe(lockKey_.data(), locked);
      if (rc == DL_OK) {
        lockHeld_ = locked;
      }
    } 

  ct_++;

  if ( ct_ >= useCount_ )
    ct_ = 0;

  return lockHeld_;
}

bool DistributedLockObserver::listNodes()
{
  if (!distLockInterface_)
    return false;

  bool retcode = false;

  int rc = distLockInterface_->listNodes(lockKey_.data());
  if (rc == DL_OK) 
     retcode = true;

  return retcode;
}

// watch the lock state for lockName
void DistributedLockObserver::watchDLocks(const char* lockName, Int32 interval)
{
   // Set the usecount to 0 to observe the lock in every 
   // lockHeld() call.
   DistributedLockObserver observer(lockName, 0);

   while (1) {
      bool locked = observer.lockHeld();
      cout << "lock status=" << locked << endl;
      sleep(interval);
   }
}

// list the zk nodes for lockName. 
void DistributedLockObserver::listDLocks(const char* lockName, Int32 interval)
{
   DistributedLockObserver observer(lockName, 0 /*not used*/);
   while (1) {
      observer.listNodes();
      sleep(interval); // in seconds
   }
}

void lockItDown(long duration)
{
   WaitedLockController lock("testWLC", 0);
   sleep(duration);
}

