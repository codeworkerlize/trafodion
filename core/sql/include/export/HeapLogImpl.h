/* -*-C++-*-
 *****************************************************************************
 *
 * File:         HeapLogImpl.h
 * Description:  This file contains implementation classes for HeapLog.
 *               This file is included only by HeapLog.cpp.
 *
 * Created:      3/1/99
 * Language:     C++
 *
 *
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
*
****************************************************************************
*/

#ifndef HEAPLOGIMPL__H
#define HEAPLOGIMPL__H

// Should never exceed the first limit if used properly.
#define MAX_NUM_HEAPS 5000
// The number of slots will expand dynamically as needed.
#define INITIAL_OBJECT_SLOTS 5000

// -----------------------------------------------------------------------
// Entry to track allocations and deallocations.
// -----------------------------------------------------------------------
class HeapLogEntry {
 public:
  int indx;  // object index
  int size;  // object size
  void *addr;  // object address
};

// -----------------------------------------------------------------------
// Each heap is assocated with a HeapLogSegment with multiple entries.
// -----------------------------------------------------------------------
class HeapLogSegment {
  friend class HeapLog;
  friend class HeapLogRoot;

 public:
  HeapLogSegment();
  ~HeapLogSegment();

 private:
  HeapLogEntry *object_;  // Log entries.  One entry per allocation.
  char name_[25];         // name of the heap.

  int slotCount_;   // total slots.
  int usageCount_;  // number of slots that are used.
  int last_;        // the last in-use slot.
  int deleted_;     // the slot reset by the previous delete.
  int totalSize_;   // total size of in-use objects.

  NABoolean free_;  // true if the segment can be re-assigned
                    // to a new heap.
};

// -----------------------------------------------------------------------
// Class to link all log segments.  One log segment per heap.
// -----------------------------------------------------------------------
class HeapLog {
  friend class HeapLogRoot;

 public:
  HeapLog();

  // Reset the log for a new tracking session.
  void reset();
  // Add a log entry to track allocations.
  int addEntry(void *objAddr, int objSize, int heapNum, const char *heapName = NULL);

  // Prepare to fetch from packdata.
  int fetchInit(int flags, char *packdata, int datalen);
  // Fetch a single line from the log.
  // sqlci: 0->called by arkcmp, 1->called by sqlci.
  int fetchLine(char *buf, int sqlci);
  // cleanup after fetch.
  void close();

 private:
  enum {
    // phases for fetch.
    PHASE_CLOSED = 0,
    PHASE_1,
    PHASE_2,
    PHASE_3,
    PHASE_4,
    PHASE_5,
    PHASE_EOF,
    DISPLAY_LEN = 80
  };

  HeapLogSegment header_[MAX_NUM_HEAPS];

  int currHeapNum_;   // Most recently assigned heap number.
  int objIndex_;      // allocation sequence number for objects.
  int disableLevel_;  // to disable logging.
  NABoolean overflow_;  // true if ever exceeds MAX_NUM_HEAPS.

  // Used by fetchLine
  const char **heading_;
  int h_;
  int s_;
  int objCount_;
  int status_;

  char *packdata_;
  int datalen_;
  int currlen_;
};

#endif
