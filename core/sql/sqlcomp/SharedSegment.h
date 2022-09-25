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
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         SharedSegment.h
 * Description:  This file contains the declarations for shared memory segment.
 *
 * Created:      11/12/2019
 * Language:     C++
 * Status:       $State: Exp $
 *
 *
 *****************************************************************************
 */


#ifndef __SHARED_SEGMENT__H
#define __SHARED_SEGMENT__H

#define SHARED_CACHE_SEGMENT_KEY_OFFSET 0x2000000
#define SHARED_DATA_SEGMENT_KEY_OFFSET  0x4000000
#define SHMFLAGS 0600
#define INVALID_KEY -1
#define INVALID_SHMID -1

#define SHARED_CACHE_SEG_SIZE    256*1024*1024
#define SHARED_DATA_SEG_SIZE    512*1024*1024

#define SHARED_DATA_SEGMENT_MOMORY_ADDR    0X500000000
// define or undef the following macro to place the shared cache
// in the RMS segment, or in a dedicated memory segment.
//
//#define USE_RMS_SHARED_SEGMENT_FOR_SHARED_CACHtE
#undef USE_RMS_SHARED_SEGMENT_FOR_SHARED_CACHE

#include "NAMemory.h"

class SharedSegment
{
  friend class SharedCache;
  friend class SharedCacheDB;
  friend class SharedSegmentDB;

public:
  SharedSegment();
  ~SharedSegment();

  bool isShmidValid() { return (shmid_ != INVALID_SHMID ); }

  key_t getKey()  { return key_; };
  int  getShmid() { return shmid_; }
  void* getAddr() { return addr_; }
  size_t getSize() { return size_; }
  int getErrno()    { return errno_; }
  UInt64 getNextOffset() { return (UInt64)addr_ + size_; }

  // Compute msg_mon_get_my_segid() + SHARED_CACHE_SEGMENT_KEY_OFFSET
  static key_t getKeyForDefaultSegment(bool isCreatedForData = false);

  void display(ostream& out, const char* msg = "");
  void display(const char* msg = "");
  void dump(const char* msg = "");

protected:
  //void initAsDefaultSegment();

  // shmget(key_) 
  bool locateOrCreateSegment();

  // addr_= shmat(shmid_, addr)
  void* attachVirtualAddress(const void* addr = NULL);

  void init(key_t key, size_t size, 
            bool enableHugePages, void* addr);

  void setKey(key_t k)  { key_ = k; };
  void setSize(size_t s)  { size_ = s; };

  // shmdt(addr_)
  bool detachVirtualAddress();

  void setDataCache() { isDataCache_ = true;}

  // issue "ipcrm -m shmid_" and set shmid_ to -1 afterwards
  bool remove();

  // round up offset to a multiple of SHMLBA (4096 on linux)
  static UInt64 roundupToSHMLBA(UInt64 offset);

protected:

  // round up offset to a multiple of 'unit'
  static UInt64 traf_roundup(UInt64 offset, UInt64 unit);

  key_t key_;   // segment key
  int shmid_;   // segment id
  void* addr_;  // segment address
  size_t size_; // segment size
  bool enableHugePages_;

  int errno_; // the most recent errno (if any)
  bool isDataCache_;
};


class SharedSegmentDB;

// This class models the default segment which is a
// singular object in a SQL process. It performs the
// following important functions.
//
// 1. attach itself and any dynamic allocated segments,
//    through init() method, called from
//    SharedCacheDB::locate();
// 2. detach all dynamic allocated segments, and
//    itself during dstr.
class DefaultSharedSegment : public SharedSegment
{

public:
  DefaultSharedSegment();
  ~DefaultSharedSegment();

  void init();

  // Compute the virtual memory address for default segment,
  static void* computeDefaultSegmentAddr(bool isDataCache = false);

  // Compute x such that x is a multiple of 8 and a least
  // value >=  RMS_SHARED_MEMORY_ADDR + RMS segment size.
  static size_t computeDefaultSegmentSize(bool isDataCache = false);

  void setIsDataCache() { 
    setDataCache();
    size_ = DefaultSharedSegment::computeDefaultSegmentSize(isDataCache_);
  }

protected:

  SharedSegmentDB* segmentDB_;

};

#define MAX_NUM_SHARED_SEGMENTS 20

// A management class managing MAX_NUM_SHARED_SEGMENTS 
// SharedSegment objects
class SharedSegmentDB
{
friend class SharedCacheDB;
friend class DefaultSharedSegment;

public:
  SharedSegmentDB();
  ~SharedSegmentDB();

  SharedSegment* createSharedSegment(size_t size);

  // free a shared segment at address 'addr' by 
  // detach and remove
  //
  // The method returns 
  //    true: free is successful
  //    false:  otherwise
  bool freeSharedSegment(void* addr); 

  // compute a total size for default and all valid dynamic segments
  // The argument 'validSegment' 
  //    when true: only include segments with valid shmid
  //    when false: all segments
  size_t computeTotalSize(bool validSegment = true);

  //void setHeap(NAHeap* heap) { heap_ = heap; }
  void display(const char* msg = "");

  // Testing method: create and destroy <n> segments
  static void createAndDestroyNSegments(int numSegments = 10, size_t size = SHARED_CACHE_SEG_SIZE);

protected:
  // Init sharedSegments_ by calling SharedSegment::init() on each entry.
  // This method should be called only to setup this with fresh new
  // nextkey, size and address for each entry. 
  //
  // When this has been already populated and is to be used again, 
  // call attachAllSegments() instead to establish the virtual addresses
  // for all entries with a valid shmid.
  void init();

  // Attach virtual address for all entries in sharedSegments_ 
  // by calling SharedSegment::attachVirtualAddress() on each.
  void attachDynamicSegments();

  // detach all shared segments.
  void detachDynamicSegments();

  key_t getNextKey();

protected:
  key_t nextKey_;   // next segment key

  // The array of shared segments that this object manages.
  SharedSegment sharedSegments_[MAX_NUM_SHARED_SEGMENTS];

  //NAHeap* heap_;
};

#endif
