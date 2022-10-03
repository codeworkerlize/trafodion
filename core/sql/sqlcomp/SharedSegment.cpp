/**********************************************************************
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
**********************************************************************/

#include <assert.h>
#include <sys/shm.h>
#include <errno.h>
#include <iostream>
#include "seabed/ms.h"
#include "seabed/fserr.h"
#include "SharedSegment.h"
#include "export/NABasicObject.h"
#include "common/NAMemory.h"
#include "sqlcomp/SharedCache.h"
#include <stdlib.h>

// only to get the definition of getRmsSharedMemoryAddr()
// and computeSegmentSizeForRMS();
#include "runtimestats/SqlStats.h" 

using namespace std;

//////////////////////////////////////////////////////////
// SharedSegment class related methods
//////////////////////////////////////////////////////////
SharedSegment::SharedSegment()
: shmid_(INVALID_SHMID),
  errno_(-1),
  isDataCache_(false)
{
  init(INVALID_KEY, 0, false, NULL);
}

SharedSegment::~SharedSegment()
{
}

bool SharedSegment::locateOrCreateSegment()
{
  bool ok = false;

  // check on key_
  if ( key_ != INVALID_KEY ) {

     int shmFlag = SHMFLAGS;
   
     if ((shmid_ = shmget(key_,
                            0,  // size doesn't matter unless we are creating.
                            shmFlag)) == -1)
     {
       if (errno == ENOENT)
       {
         ok = true; // assume the following shmget() is successful.
         //m15636: re-compute default size in case that getenv("SHARED_CACHE_SEG_SIZE")
         //was not properly read in ESPs during defaultDescSharedSegment constructor
         size_ = DefaultSharedSegment::computeDefaultSegmentSize(isDataCache_);
         // Normal case, segment does not exist yet. Try to create.
         if ((shmid_ = shmget(key_, size_, shmFlag | IPC_CREAT)) == -1)
         {
#if 0
           dump("shmget(): did not find an existing one, create a new one");
#endif
           if (enableHugePages_)
           {
             // try again without hugepages
             shmFlag =  shmFlag & ~SHM_HUGETLB;
             if ((shmid_ = shmget(key_, size_, shmFlag | IPC_CREAT)) == -1)
               ok = false;
           }
           else 
             ok = false;
         }
       } // if ENOENT (i.e., attempting creation.)
     } else {
       ok = true;

#if 0
       dump("shmget(): find an existing segment");
#endif
     }
  }

  return ok;
}
  
void* SharedSegment::attachVirtualAddress(const void* addr)
{
  // Need to make sure shmid is valid first. Invoke
  // locateOrCreateSegment() if shmid is not valid initially.
  if ( !isShmidValid() && !locateOrCreateSegment() )
    return NULL;

  // Try to tie the shared segment to 'addr'. We must not use
  // flag SHM_RND here as it allows addr to be rounded down 
  // to the nearest multiple of SHMLBA.
  //
  // We will not use SHM_REMAP flag here to avoid bounce any 
  // existing segments existing on the same address range. 
  // If such a segment exists, a -1 will be returned and the
  // segment becomes unusable.
  int flag = 0;

  if ((addr_= shmat(shmid_, addr, flag)) == (void *)-1 ||
       addr_ != addr ) {
    errno_ = errno;
    addr_ = NULL;
  } else 
    assert(addr_ == addr);

#if 0
    dump("shmat(): attach to an existing segment");
#endif

  return addr_;
}

void
SharedSegment::init(key_t key, size_t sz, bool hpages, void* addr)
{
   key_ = key;
   size_ = sz;
   enableHugePages_ = hpages;
   addr_ = addr;
   shmid_ = INVALID_SHMID;
}

bool SharedSegment::detachVirtualAddress()
{
  if ( !addr_ )
    return false;

  if (shmdt(addr_) == -1) {
    errno_ = errno;
    return false;
  }
  return true;
}

bool SharedSegment::remove()
{
  char buf[50];
  snprintf(buf, sizeof(buf), "ipcrm -m %d", shmid_);
   Int32 rc = system(buf);

   if ( rc != 0 ) {
     errno_ = errno;
     return false;
  }

  shmid_ = INVALID_SHMID;
  return true;
}

// static
key_t SharedSegment::getKeyForDefaultSegment(bool isCreatedForData)
{
   int segid = 0;
   int error = msg_mon_get_my_segid(&segid);
   assert(error == 0);  //XZFIL_ERR_OK;
   key_t key = segid + SHARED_CACHE_SEGMENT_KEY_OFFSET;
   if (isCreatedForData)
     key += SHARED_DATA_SEGMENT_KEY_OFFSET;

   return key;
}

//static 
UInt64 SharedSegment::traf_roundup(UInt64 offset, UInt64 unit)
{
  UInt64 roundedDown = (offset / unit) * unit;
  // if that didn't change anything we're done, the size was
  // a multiple of x already
  if (offset != roundedDown)
    {
      // else we have to round up and make it a multiple of x
      offset = roundedDown + unit;
    }

  return offset;
}

UInt64 SharedSegment::roundupToSHMLBA(UInt64 offset)
{
  return SharedSegment::traf_roundup(offset, SHMLBA);
}

void SharedSegment::display(ostream& out, const char* msg)
{
   out << msg 
        << " key=" << std::hex << getKey() << std::dec
        << ", shmid=" << getShmid()
        << ", addr=" << static_cast<void*>(getAddr())
        << ", size=" << getSize() / (1024 * 1024) << "MB"
        << endl;
}

void SharedSegment::display(const char* msg)
{
   display(cout, msg);
}
 
void SharedSegment::dump(const char* msg)
{
   fstream& out = getPrintHandle();
   display(out, msg);
#ifdef _DEBUG
   displayCurrentStack(out, 1000);
#endif
   out.close();
}
 
//////////////////////////////////////////////////////////
// DefaultSharedSegment related methods
//////////////////////////////////////////////////////////
DefaultSharedSegment::DefaultSharedSegment() : 
   SharedSegment(),
   segmentDB_(NULL)
{
   size_ = DefaultSharedSegment::computeDefaultSegmentSize();
}

DefaultSharedSegment::~DefaultSharedSegment()
{
   // Signal the end of work to the shared cache by detatching 
   // all dynamic shared segments. These segments still exist 
   // in the OS space for other processes to attach to.
   if ( segmentDB_ )
     segmentDB_->detachDynamicSegments();

   // lastly detech the default segment
   detachVirtualAddress();
}

//static
size_t DefaultSharedSegment::computeDefaultSegmentSize(bool isDataCache)
{
  // The shared cache area is to be located after the RMS area.
  size_t maxCacheSegSize = SHARED_CACHE_SEG_SIZE;
  char *envSegSize = nullptr;
  if (isDataCache) {
    maxCacheSegSize = SHARED_DATA_SEG_SIZE;
    envSegSize = getenv("SHARED_DATA_SEG_SIZE");
    if (envSegSize) {
      maxCacheSegSize = (long)str_atoi(envSegSize, str_len(envSegSize));
      maxCacheSegSize *= 1024 * 1024;
    }
  }
  else {
    envSegSize = getenv("SHARED_CACHE_SEG_SIZE");
    if (envSegSize) {
      maxCacheSegSize = (long)str_atoi(envSegSize, str_len(envSegSize));
      maxCacheSegSize *= 1024 * 1024;
    }
  }

  return maxCacheSegSize;
}

//static 
void* DefaultSharedSegment::computeDefaultSegmentAddr(bool isDataCache)
{
  if (isDataCache)
    return (void*)SHARED_DATA_SEGMENT_MOMORY_ADDR;

  UInt64 offset = (UInt64)getRmsSharedMemoryAddr() + 
                  computeSegmentSizeForRMS() +
                  (isDataCache ? computeDefaultSegmentSize() : 0) +
                  SHMLBA; // add SHMLBA (4K on linux) as the gap
                          // between the RMS and default shared 
                          // cache segment.
  return (void*)SharedSegment::roundupToSHMLBA(offset);
}

void DefaultSharedSegment::init()
{
   if ( key_ == INVALID_KEY || (isDataCache_ && key_ == getKeyForDefaultSegment())) {
      key_ = getKeyForDefaultSegment(isDataCache_);

      attachVirtualAddress(computeDefaultSegmentAddr(isDataCache_));

      SharedCacheDB* sharedCacheDB = (SharedCacheDB*)(getAddr());
      if ( sharedCacheDB && sharedCacheDB->isMe() ) {
        segmentDB_ = sharedCacheDB->getSharedSegmentDB();
        segmentDB_->attachDynamicSegments();
      }
   }
}

extern DefaultSharedSegment defaultDescSharedSegment;

///////////////////////////////////////////////
// SharedSegmentDB related methods
///////////////////////////////////////////////
SharedSegmentDB::SharedSegmentDB() : nextKey_(INVALID_KEY)
{
}

SharedSegmentDB::~SharedSegmentDB()
{
}

key_t SharedSegmentDB::getNextKey()
{
  if (nextKey_ == INVALID_KEY)
  {
     nextKey_ = SharedSegment::getKeyForDefaultSegment();
  } 
     
  nextKey_++;

  // nextKey_ has to be different than RMS_SEGMENT_ID_OFFSET 
  // to avoid segment collision.
  if ( nextKey_ == RMS_SEGMENT_ID_OFFSET )
     nextKey_++;

  return nextKey_ ; 
}

SharedSegment* 
SharedSegmentDB::createSharedSegment(size_t size)
{
   key_t key = 0;          // key of the next segment 
   UInt64 offset = 0;      // offset of the new segment
   void* addr = NULL;      // virtual address for the new segment
   size_t prevSegSize = 0; // the seg size of the previous segment in index
   size_t currentSegSize = 0; // the size of the current segment to be created
   SharedSegment* previous = NULL; // point at a segment with index at i-1
   SharedSegment* current = NULL;  // point at a segment with index at i

   for (int i=0; i<MAX_NUM_SHARED_SEGMENTS; i++) {

     current = &sharedSegments_[i];

     // If the current segment is valid (mapped), search for the next one
     if ( current->isShmidValid() )
       continue;

     addr = current->attachVirtualAddress(current->getAddr());

     if ( !addr )
       continue;

     currentSegSize = current->getSize();

     // Check if the segment is large enough. If it is, we are done.
     if ( (size + sizeof(NABlock) < currentSegSize) && 
          current->isShmidValid() )
     {
       memset(addr, currentSegSize, 0); // set the entire memory span to NULL.
       size = currentSegSize;

#if 0
       cout << ", The segment at " << i
            << " is big enough. Details: "
            << " size=" << size
            << ", start addr=" << static_cast<void*>(addr)
            << ", end addr=" << static_cast<void*>((char*)addr+size-1)
            << ", block.addr_=" << static_cast<void*>(current->getAddr())
            << ", block.size_=" << current->getSize()
            << ". Done!" << endl;
#endif

       return current;
     }
   }

   // We have tried all MAX_NUM_SHARED_SEGMENTS and none of them
   // is big enough.
   return NULL;
}
  
// detach the virtual address for a segment at 
// address 'addr' and then remove it.
bool SharedSegmentDB::freeSharedSegment(void* addr)
{
   bool ok = false;

   for (int i=0; i<MAX_NUM_SHARED_SEGMENTS; i++) {
     SharedSegment* seg = &sharedSegments_[i];
     if ( seg && seg->getAddr() == addr ) {

        ok = seg->detachVirtualAddress();
      
        if ( ok )
          ok = seg->remove();

        return ok;
     }
   }

   return false;
}

void SharedSegmentDB::detachDynamicSegments()
{
   for (int i=0; i<MAX_NUM_SHARED_SEGMENTS; i++) {
     SharedSegment* seg = &sharedSegments_[i];
     if ( seg && seg->isShmidValid()) {
        seg->detachVirtualAddress();
     }
   }
}

void SharedSegmentDB::display(const char* msg)
{
   cout << msg << endl;

   defaultDescSharedSegment.display("Default:  ");

   char msgBuf[100];
   for (int i=0; i<MAX_NUM_SHARED_SEGMENTS; i++) {
     SharedSegment* seg = &sharedSegments_[i];
     if ( seg ) {
       sprintf(msgBuf, "Segment %d:", i);
       seg->display(msgBuf);
     }
   }
}

void SharedSegmentDB::init()
{
  // Apply the golden ratio to compute the current segment 
  // size from the previous segment as: 
  //      current seg size = 1.618 * prevous seg size
  //
  // Listed below are the keys, the addresses, and sizes of 
  // first 10 sements as stored in sharedSegments_[]. 
  //
  // Default segment: 
  //             key=33593879, addr=0x139000000, size=50MB
  //
  // Segment: 0, key=33593880, addr=0x13c200000, size=80MB
  // Segment: 1, key=33593881, addr=0x1412e7000, size=130MB
  // Segment: 2, key=33593882, addr=0x1495cd000, size=211MB
  // Segment: 3, key=33593883, addr=0x156998000, size=342MB
  // Segment: 4, key=33593884, addr=0x16c046000, size=554MB
  // Segment: 5, key=33593885, addr=0x18eaba000, size=897MB
  // Segment: 6, key=33593886, addr=0x1c6bd4000, size=1451MB
  // Segment: 7, key=33593887, addr=0x221757000, size=2348MB
  // Segment: 8, key=33593888, addr=0x2b43e2000, size=3799MB
  // Segment: 9, key=33593889, addr=0x3a1bd3000, size=6148MB
  //

   SharedSegment* current = NULL;
   SharedSegment* previous = &defaultDescSharedSegment;
   void* addr = NULL;
   size_t size = 0;
   UInt64 offset = 0;

   for (int i=0; i<MAX_NUM_SHARED_SEGMENTS; i++) {

      offset = size = previous->getSize();

      size *= 1.618;

      offset += (UInt64)(previous->getAddr());

      // The virtual address of this segment is the 'next offset'
      // of the previous segment, round up to SHMLBA(4K) bytes.
      // This assures that the 'addr' computed here can be
      // attached to, without modification, by shmat() call in
      // SharedSegment::attachVirtualAddress().
      addr = (void*)SharedSegment::roundupToSHMLBA(offset);

      current = &sharedSegments_[i];

      current->init(getNextKey(), size, false, addr);

      previous = current;
   }
}

// Attach all valid dynamic segments.  
// This method is used by any process just in need of utilizing 
// the shared cache, since it specifically checks for each 
// segment's valid-ness (i.e., with a valid segment Id). If the 
// segment Id is -1, no attachment will occur.
void SharedSegmentDB::attachDynamicSegments()
{
   SharedSegment* current = NULL;
   for (int i=0; i<MAX_NUM_SHARED_SEGMENTS; i++) {
     current = &sharedSegments_[i];
     if ( current->isShmidValid() )
        current->attachVirtualAddress(current->getAddr());
   }
}

size_t SharedSegmentDB::computeTotalSize(bool validSegment)
{
   size_t size = defaultDescSharedSegment.getSize();
   for (int i=0; i<MAX_NUM_SHARED_SEGMENTS; i++) {

     if ( SIZE_MAX - size < sharedSegments_[i].getSize() )
       return SIZE_MAX;

     if ( validSegment && !sharedSegments_[i].isShmidValid() )
       continue;

     size += sharedSegments_[i].getSize();
   }
   return size;
}

// A static testing method.
void SharedSegmentDB::createAndDestroyNSegments(int numSegments, size_t size)
{
   SharedSegmentDB ssDB;

   cout << "createAndDestroyNSegments(): " 
        << ", numSegments=" << numSegments
        << ", size=" << size
        << endl;

   char msg[100];
   void* addresses[numSegments];

   // First create these many segments
   for (int i=0; i<numSegments; i++) {

     SharedSegment* seg = ssDB.createSharedSegment(size);
     addresses[i] = (seg) ? seg->getAddr() : NULL;
   }

   sprintf(msg, "After creating %d segments", numSegments);
   ssDB.display(msg);

   // Randomly select about half segments to drop
   srandom(11);
   int numDeleted = 0;
   bool indicators[numSegments];
   for (int i=0; i<numSegments; i++) {
     if ( (float)(random()) / RAND_MAX < 0.5) {
       indicators[i] = true;
       numDeleted++;
     } else
       indicators[i] = false;
   }

   // Delete the selected
   for (int i=0; i<numSegments; i++) {
      if ( indicators[i] ) {
         cout << "delete segment at " << i << endl;
         ssDB.freeSharedSegment(addresses[i]);
      }
   }

   sprintf(msg, "After deleting %d segments", numDeleted);
   ssDB.display(msg);


   // Recreate the deleted
   for (int i=0; i<numSegments; i++) {
      if ( indicators[i] ) {
         SharedSegment* segment = ssDB.createSharedSegment(size);
         if (segment)
            cout << "re-create segment at " << i 
                 << ", addr=" << static_cast<void*>(segment->getAddr())
                 << ", segSize=" << segment->getSize();
         else
            cout << "re-create segment at " << i << " failed";
                 
         cout << endl;
      }
   }

   sprintf(msg, "After re-creating %d segments", numDeleted);
   ssDB.display(msg);
}

