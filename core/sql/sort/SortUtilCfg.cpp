
/* -*-C++-*-
******************************************************************************
*
* File:         SortUtilCfg.C
* RCS:          $Id: SortUtilCfg.cpp,v 1.8 1998/07/29 22:02:27  Exp $
*
* Description:  This file contains the implementation of all member functions
*               of the class SortUtilCfg. This class groups together various
*               configurable parameters related to a sorting session.
*
* Created:	07/12/96
* Modified:     $ $Date: 1998/07/29 22:02:27 $ (GMT)
* Language:     C++
* Status:       $State: Exp $
*
*
*
******************************************************************************
*/

#include <iostream>

#include "SortUtilCfg.h"

//------------------------------------------------------------------------
// This file contains the all member function definitions of SortUtilConfig
// class.
//------------------------------------------------------------------------

SortUtilConfig::SortUtilConfig(CollHeap *heap) {
  // sortType_                = 0;
  approxNumRecords_ = 10000L;
  recSize_ = 132L;
  keySize_ = 10L;
  runSize_ = 200;
  mergeOrder_ = 0;
  heapAddr_ = heap;
  minMem_ = 150000L;
  maxMem_ = 10000000L;         // arbitrarily chosen 10MB max memory
  scratchDirListSpec_ = NULL;  // Information about scratchvols to  include or exclude.
  numDirsSpec_ = 0;
  espInstance_ = 0;
  numEsps_ = 0;
  useBufferedWrites_ = TRUE;
  logInfoEvent_ = TRUE;
  logDone_ = FALSE;
  memoryQuotaMB_ = 0;
  initialMemoryQuotaMB_ = 0;
  memoryQuotaUsedBytes_ = 0;
  pressureThreshold_ = 10;
  scratchMgmtOption_ = 0;
  scratchMaxOpens_ = 1;
  minimalSortRecs_ = 0;
  partialSort_ = FALSE;
  preAllocateExtents_ = FALSE;
  sortMergeBlocksPerBuffer_ = 1;
  scratchDiskLogging_ = FALSE;
  scratchIOBlockSize_ = 0;
  scratchIOVectorSize_ = 0;
  ovMode_ = SCRATCH_DISK;
  resizeCifRecord_ = FALSE;
  considerBufferDefrag_ = FALSE;
  estimateErrorPenalty_ = 0;
  pMemoryContingencyMB_ = 0;
  bmoCitizenshipFactor_ = 0;
  sortMemEstInKBPerNode_ = 0;
  bmoMaxMemThresholdMB_ = 0;
  intermediateScratchCleanup_ = TRUE;
  topNSort_ = FALSE;
}

SortUtilConfig::~SortUtilConfig(void) {}

//----------------------------------------------------------------------
// Name         : setSortType
//
// Parameters   : ...
//
// Description  :
//
// Return Value :
//   SORT_SUCCESS if everything goes on well.
//   SORT_FAILURE if any error encounterd.
//
//----------------------------------------------------------------------
NABoolean SortUtilConfig::setSortType(SortType &sorttype) {
  sortType_ = sorttype;
  return SORT_SUCCESS;
}

//----------------------------------------------------------------------
// Name         : setSortMemory
//
// Parameters   : unsigned long min (specify minimum sort memory)
//                unsigned long max (specify maximum sort memory)
//
// Description  :
//
// Return Value : void
//
//----------------------------------------------------------------------
void SortUtilConfig::setSortMemory(int min, int max) {
  if (min > minMem_) minMem_ = min;

  if (max < minMem_)
    maxMem_ = minMem_;
  else
    maxMem_ = max;
}

//----------------------------------------------------------------------
// Name         : setSortMaxMemory
//
// Parameters   : unsigned long max (specify maximum sort memory)
//
// Description  :
//
// Return Value : void
//
//----------------------------------------------------------------------
void SortUtilConfig::setSortMaxMemory(int max) {
  if (max < minMem_)
    maxMem_ = minMem_;
  else
    maxMem_ = max;
}

//----------------------------------------------------------------------
// Name         : getSortMaxMemory
//
// Parameters   : unsigned long& max.
//
// Description  : Get the value of the maximum sort memory.
//
// Return Value : void
//
//----------------------------------------------------------------------
void SortUtilConfig::getSortMaxMemory(int &max) { max = maxMem_; }

//----------------------------------------------------------------------
// Name         : setRecSize
//
// Parameters   : ...
//
// Description  :

// Return Value :
//   SORT_SUCCESS if everything goes on well.
//   SORT_FAILURE if any error encounterd.
//
//----------------------------------------------------------------------
NABoolean SortUtilConfig::setRecSize(int recsize) {
  recSize_ = recsize;
  return SORT_SUCCESS;
}

//----------------------------------------------------------------------
// Name         : getRecSize
//
// Parameters   : ...
//
// Description  :
//
// Return Value :
//   SORT_SUCCESS if everything goes on well.
//   SORT_FAILURE if any error encounterd.
//
//----------------------------------------------------------------------
int SortUtilConfig::getRecSize() const { return recSize_; }

//----------------------------------------------------------------------
// Name         : setKeyInfo
//
// Parameters   : ...
//
// Description  :
//
// Return Value :
//   SORT_SUCCESS if everything goes on well.
//   SORT_FAILURE if any error encounterd.
//
//----------------------------------------------------------------------
NABoolean SortUtilConfig::setKeyInfo(int keysize) {
  keySize_ = keysize;
  return SORT_SUCCESS;
}

void SortUtilConfig::setUseBuffered(NABoolean torf) { useBufferedWrites_ = torf; }
NABoolean SortUtilConfig::getUseBuffered() { return useBufferedWrites_; }
void SortUtilConfig::setLogInfoEvent(NABoolean torf) { logInfoEvent_ = torf; }
NABoolean SortUtilConfig::logInfoEvent() { return logInfoEvent_; }
