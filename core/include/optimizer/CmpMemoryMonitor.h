/**********************************************************************
 *
 * File:         ComOSIM.cpp
 * Description:  The memory monitor records and logs memory usage for
 *               different phases in compiler/optimizer.
 *
 * Created:      June 2007
 * Language:     C++
 *
 *

**********************************************************************/

#ifndef __CMPMEMORYMONITOR_H
#define __CMPMEMORYMONITOR_H

#include <fstream>

#include "common/Collections.h"

class MemoryUsage : public NABasicObject {
 public:
  // Constructor for MemoryUsage
  MemoryUsage(const char *phaseName, CollHeap *heap);
  // Another constructor for MemoryUsage
  MemoryUsage(const char *phaseName, size_t stmtHBegin, size_t stmtHEnd, size_t stmtHAllocSize,
              size_t stmtHHighWaterMark, size_t cxtHBegin, size_t cxtHEnd, size_t cxtHAllocSize,
              size_t cxtHHighWaterMark, CollHeap *heap);
  // Update the phase name with additional info.
  void updatePhaseName(char *updatedPhaseName);
  // Record the memory usage information at the beginning of the phase.
  void enter(CollHeap *otherHeap);
  // Record the memory usage information at the end of the phase and
  // compute the difference to get actual allocations.
  void exit(CollHeap *otherHeap);
  // Log memory usage for a phase.
  void logMemoryUsage(ofstream *logFilestream);
  // This operator is a must since we are using NAHashDictionary.
  NABoolean operator==(const MemoryUsage &mu);

 private:
  char *phaseName_;  // phase where memory usage information is logged
  size_t stmtHBegin_;
  size_t stmtHEnd_;
  size_t stmtHAllocSize_;
  size_t stmtHHighWaterMark_;
  size_t cxtHBegin_;
  size_t cxtHEnd_;
  size_t cxtHAllocSize_;
  size_t cxtHHighWaterMark_;
  CollHeap *heap_;
};

class CmpMemoryMonitor : public NABasicObject {
 public:
  // Constructor
  CmpMemoryMonitor(CollHeap *heap);

  // Accessors for isMemMonitor_.
  void setIsMemMonitor(NABoolean isMemMonitor);
  NABoolean getIsMemMonitor();
  // Accessors for isMemMonitorInDetail_.
  void setIsMemMonitorInDetail(NABoolean isMemMonitorInDetail);
  NABoolean getIsMemMonitorInDetail();
  // Accessors for logFilename_
  void setLogFilename(const char *logFilename);
  char *getLogFilename();
  // Accessors for isLogInstantly_.
  void setIsLogInstantly(NABoolean isLogInstantly);
  NABoolean getIsLogInstantly();
  // Record and retrieve Query Text of a query that is being
  // monitored for memory usage.
  void setQueryText(char *queryText);
  char *getQueryText();

  // Cleanup methods
  void fileCleanUp();
  void cleanupPerStatement();

  // Log all the memory usage information.
  void logMemoryUsageAll();

  // Record memory usage at both ends of a phase.
  void enter(const char *phaseName, CollHeap *otherHeap);
  void exit(const char *phaseName, CollHeap *otherHeap, char *updatedPhaseName);

 private:
  NABoolean isMemMonitor_;
  NABoolean isMemMonitorInDetail_;
  char *logFilename_;
  ofstream *logFilestream_;
  NABoolean isLogInstantly_;
  char *queryText_;
  // NAHashDictionary for all MemoryUsage objects. NAHashDictionary is
  // useful for travesing through MemoryUsage objects while updating
  // the usage information.
  NAHashDictionary<NAString, MemoryUsage> *hd_MemoryUsage_;
  // NAList contains the pointers to the same MemoryUsage objects that
  // are added to the NAHashDictionary. But it keeps the order of the
  // memory usage phases as they were encountered.
  NAList<MemoryUsage *> *memUsageList_;
  CollHeap *heap_;
};

// Wrapper functions to use the Memory Monitor.
void MonitorMemoryUsage_QueryText(char *queryText);
void MonitorMemoryUsage_Enter(const char *phaseName, CollHeap *heap = NULL, NABoolean isDetail = FALSE);
void MonitorMemoryUsage_Exit(const char *phaseName, CollHeap *heap = NULL, char *updatedPhaseName = NULL,
                             NABoolean isDetail = FALSE);
void MonitorMemoryUsage_LogAll();

#endif
