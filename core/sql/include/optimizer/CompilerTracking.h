// **********************************************************************
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
// **********************************************************************
#ifndef COMPILERTRACKING_H
#define COMPILERTRACKING_H

#include "common/NAString.h"
#include "common/CmpCommon.h"
#include "optimizer/SchemaDB.h"
#include "optimizer/opt.h"
#include "optimizer/NATable.h"
#include <time.h>
#include <fstream>
#include "sqlcomp/QCache.h"
#include "CmpProcess.h"

//
// the private user table where the data can be loggeed
#define COMPILER_TRACKING_TABLE_NAME_PRIVATE "STATE_TRACKING_COMPILERS_TABLE_PRIVATE"
/************************************************************************
class CompilerTrackingInfo

Singleton class to collect information about this compiler
and to be logged into a file or a private user table on some interval
(every N minutes)

************************************************************************/
class CompilerTrackingInfo {
 public:
  // get the singleton instance
  // static CompilerTrackingInfo * getInstance();
  CompilerTrackingInfo(CollHeap *outHeap = CmpCommon::contextHeap());
  //
  // initialize the singleton
  // static void initGlobalInstance();
  //
  // log all of the data members of this object
  void logCompilerStatusOnInterval(int intervalLengthMins);
  //
  // the cpu time for the longest compilation is updated
  // if the parameter is longer than the current longest
  void updateLongestCompile(int c);
  //
  // the largest statement heap size so far
  // update this right before deleting the CmpStatement
  void updateStmtIntervalWaterMark();
  //
  // the largest system heap size so far
  // update this right before cleanup (in CmpMain.cpp)
  void updateSystemHeapWtrMark();
  //
  // the largest qcache heap size so far
  // update this right before deleting the qcache
  void updateQCacheIntervalWaterMark();
  //
  // the number of successfully compiled queries
  void incrementQueriesCompiled(NABoolean success);
  //
  // compiled successfully but with warnings 2053 or 2078
  void incrementCaughtExceptions() { caughtExceptionCount_++; }
  //
  // when the QueryCache is being deleted, clear the
  // markers we used to determine difference during
  // the interval.
  void clearQCacheCounters();
  //
  // maximum # of characters for compiler info
  enum { MAX_COMPILER_INFO_LEN = 4096 };

  // start the new interval (at the current time, clock())
  // if the previous interval tracking was disabled
  void resetIntervalIfNeeded();

  // methods used internally within the class...
 protected:
  //
  // check to see if an interval has expired
  NABoolean intervalExpired(int intervalLengthMins);
  //
  // start the new interval (at the current time, clock())
  void resetInterval();

  // the duration of the interval in minutes
  int currentIntervalDuration(long endTime);
  //
  // the cpu time for the interval
  int cpuPathLength();
  //
  // getters
  inline long beginIntervalTime() { return beginIntervalTime_; }

  // return the beginning of the interval in unix epoch
  inline long beginIntervalTimeUEpoch() { return beginIntervalTimeUEpoch_; }

  inline long endIntervalTime() { return endIntervalTime_; }

  inline int beginIntervalClock() { return beginIntervalClock_; }
  //
  // the compiler age in minutes
  inline int compilerAge() {
    long seconds = (processInfo_->getProcessDuration() / 1000000);
    long minutes = (seconds / 60);
    return int64ToInt32(minutes);
  }
  //
  // statement heap
  inline size_t stmtHeapCurrentSize() { return CmpCommon::statementHeap()->getAllocSize(); }

  inline size_t stmtHeapIntervalWaterMark();
  //
  // context heap
  inline size_t cxtHeapCurrentSize() { return CmpCommon::contextHeap()->getAllocSize(); }

  inline size_t cxtHeapIntervalWaterMark() { return CmpCommon::contextHeap()->getIntervalWaterMark(); }
  //
  // metadata cache
  inline int metaDataCacheCurrentSize() { return ActiveSchemaDB()->getNATableDB()->currentCacheSize(); }

  inline int metaDataCacheIntervalWaterMark() { return ActiveSchemaDB()->getNATableDB()->intervalWaterMark(); }

  inline int metaDataCacheHits();
  inline int metaDataCacheLookups();
  void resetMetadataCacheCounters();
  //
  // query cache
  inline int qCacheCurrentSize();
  inline int qCacheIntervalWaterMark();
  inline int qCacheHits();
  inline int qCacheLookups();
  inline int qCacheRecompiles();
  void resetQueryCacheCounters();
  //
  // histogram cache
  inline int hCacheCurrentSize() {
    CMPASSERT(NULL != CURRCONTEXT_HISTCACHE);
    return CURRCONTEXT_HISTCACHE->getHeap()->getAllocSize();
  }

  inline int hCacheIntervalWaterMark() {
    CMPASSERT(NULL != CURRCONTEXT_HISTCACHE);
    return CURRCONTEXT_HISTCACHE->getHeap()->getIntervalWaterMark();
  }

  inline int hCacheHits();
  inline int hCacheLookups();
  void resetHistogramCacheCounters();

  inline int systemHeapIntervalWaterMark() { return systemHeapWaterMark_; }

  inline int longestCompile() { return longestCompileClock_; }

  inline int successfulQueryCount() { return successfulQueryCount_; }

  inline int failedQueryCount() { return failedQueryCount_; }

  inline int caughtExceptionCount() { return caughtExceptionCount_; }

  inline int sessionCount();

  inline const char *compilerInfo() { return compilerInfo_; }
  //
  // for printing to file
  enum { CACHE_HEAP_HEADER_LEN = 18, CACHE_HEAP_VALUE_LEN = 16 };
  //
  // just do the printing
  void printToFile();

  // log to a user table
  void logIntervalInPrivateTable();

  // log to a log4cxx appender
  void logIntervalInLog4Cxx();

 private:
  ~CompilerTrackingInfo();

  // The process info contains information such as cpu num, pin, etc
  CmpProcess *processInfo_;
  //
  // timestamp for when this interval began
  long beginIntervalTime_;
  //
  // timestamp for when this interval began in unix epoch
  long beginIntervalTimeUEpoch_;
  //
  // timestamp for when this interval ended
  long endIntervalTime_;
  //
  //  cpu path length for this interval
  int beginIntervalClock_;
  //
  // the most memory used in a CmpStatement so far
  size_t largestStmtIntervalWaterMark_;
  //
  // the most memory used by system heap so far
  int systemHeapWaterMark_;
  //
  // cpu path for the longest compile so far
  int longestCompileClock_;
  //
  // metadata cache counters
  int mdCacheHits_;
  int mdCacheLookups_;
  //
  // query plan cache stats
  QCacheStats currentQCacheStats_;
  int largestQCacheIntervalWaterMark_;
  int qCacheHits_;
  int qCacheLookups_;
  int qCacheRecompiles_;
  //
  // histogram cache counters
  int hCacheHits_;
  int hCacheLookups_;
  //
  // the number of queries compiled during this interval
  int successfulQueryCount_;
  int failedQueryCount_;
  //
  // the number of exceptions caught (2053 and 2078)
  int caughtExceptionCount_;
  //
  // the number of sessions
  int sessionCount_;
  //
  // the length of the last interval (set by CQD)
  int prevInterval_;
  //
  // additional compiler information
  char compilerInfo_[MAX_COMPILER_INFO_LEN];
  //
  // the heap created on
  CollHeap *heap_;
};
#endif
