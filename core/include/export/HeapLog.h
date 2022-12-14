/* -*-C++-*-
 *****************************************************************************
 *
 * File:         HeapLog.h
 * Description:  This file contains the declaration of HeapLog class to
 *               track heap allocations and deallocations.
 *
 * Created:      3/1/99
 * Language:     C++
 *
 *

*
****************************************************************************
*/

#ifndef HEAPLOG__H
#define HEAPLOG__H

#include <stddef.h>

#include "common/NABoolean.h"
#include "export/HeapID.h"

// -----------------------------------------------------------------------
// Expand to empty if this is a release build or heaplog is not importable.
// -----------------------------------------------------------------------
#ifndef NA_DEBUG_HEAPLOG
#define HEAPLOG_CONTROL(option)
#define HEAPLOG_ADD_ENTRY(objAddr, objSize, heapNum, heapName)
#define HEAPLOG_DELETE_ENTRY(objAddr, heapNum)
#define HEAPLOG_REINITIALIZE(heapNum)
#define HEAPLOG_DISPLAY(prompt)
#define HEAPLOG_ON()
#define HEAPLOG_OFF()

// -----------------------------------------------------------------------
#else  // debug build and heaplog is importable.
// -----------------------------------------------------------------------
// COntrol the heaplog.
#define HEAPLOG_CONTROL(option) \
  { HeapLogRoot::control(option); }
// Track allocations.
#define HEAPLOG_ADD_ENTRY(objAddr, objSize, heapNum, heapName)                                 \
  {                                                                                            \
    if (HeapLogRoot::track) HeapLogRoot::addEntry(objAddr, objSize, (int &)heapNum, heapName); \
  }
// Track deallocations.
#define HEAPLOG_DELETE_ENTRY(objAddr, heapNum)                                 \
  {                                                                            \
    if (HeapLogRoot::trackDealloc) HeapLogRoot::deleteEntry(objAddr, heapNum); \
  }
// Track heap re-initialization.
#define HEAPLOG_REINITIALIZE(heapNum)                                             \
  {                                                                               \
    if (HeapLogRoot::trackDealloc) HeapLogRoot::deleteLogSegment(heapNum, FALSE); \
  }
#define HEAPLOG_DISPLAY(prompt) \
  { HeapLogRoot::display(prompt); }
#define HEAPLOG_ON()                                     \
  {                                                      \
    if (HeapLogRoot::track) HeapLogRoot::disable(FALSE); \
  }
#define HEAPLOG_OFF()                                   \
  {                                                     \
    if (HeapLogRoot::track) HeapLogRoot::disable(TRUE); \
  }

// Reserved heaps.
const int NA_HEAP_BASIC = 1;

#endif

// Flags set by parser based on syntax clauses.
class LeakDescribe {
 public:
  enum {
    FLAGS = 0x2f,

    FLAG_SQLCI = 0x1,
    FLAG_ARKCMP = 0x2,
    FLAG_BOTH = 0x3,
    FLAG_CONTINUE = 0x8,
    FLAG_OFF = 0x10,
    FLAG_PROMPT = 0x20
  };
};

#ifdef __NOIMPORT_HEAPL
#else
// -----------------------------------------------------------------------
// For both debug and release builds.
// -----------------------------------------------------------------------
typedef enum HeapControlEnum {
  LOG_START = 1,
  LOG_DELETE_ONLY,
  LOG_RESET_START,
  LOG_DISABLE,
  LOG_RESET,
  LOG_RESET_DISABLE
} HeapControlEnum;

// -----------------------------------------------------------------------
// One global log structure per process.
// -----------------------------------------------------------------------
class HeapLog;

class HeapLogRoot {
 public:
  static HeapLog *log;
  // Maximum heap number assigned.
  static int maxHeapNum;
  static int track;
  static int trackDealloc;

  static void control(HeapControlEnum option);

  static void addEntry(void *objAddr, int objSize, int &heapNum, const char *heapName = NULL);
  static void deleteEntry(void *objAddr, int heapNum);

  static void deleteLogSegment(int heapNum, NABoolean setfree);

  static void disable(NABoolean b);

  static void display(NABoolean prompt, int sqlci = 1);

  // called by arkcmp.
  static int getPackSize();
  static void pack(char *buf, int flags);

  // called by executor.
  static int fetchLine(char *buf, int flags, char *packdata = NULL, int datalen = 0);

  // called by heap constructor.
  static int assignHeapNum();

  virtual void pureVirtual() = 0;

 private:
  static void control2(int flags, int mask);
  // Init the log;
  static void initLog();
  // Reset the log.
  static void reset();
};

#endif

#endif
