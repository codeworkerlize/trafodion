
#include "common/Platform.h"

#include <stdio.h>
#include <stdlib.h>

#include "common/str.h"
#include "Statistics.h"

//----------------------------------------------------------------------
// SortStatistics Constructor.
//----------------------------------------------------------------------

SortStatistics::SortStatistics() {
  memSizeB_ = 0L;
  numRecs_ = 0L;
  recLen_ = 0L;
  runSize_ = 0L;  // number of nodes in the tournament tree
  numRuns_ = 0L;

  numInitRuns_ = 0L;
  firstMergeOrder_ = 0L;
  finalMergeOrder_ = 0L;
  mergeOrder_ = 0L;
  numInterPasses_ = 0L;

  numCompares_ = 0L;
  numDupRecs_ = 0L;
  beginSortTime_ = 0;
  elapsedTime_ = 0;
  ioWaitTime_ = 0;
  scrBlockSize_ = 0L;
  scrNumBlocks_ = 0L;
  scrNumWrites_ = 0L;
  scrNumReads_ = 0L;
  scrNumAwaitio_ = 0L;
}

//----------------------------------------------------------------------
// SortStatistics Desstructor.
//----------------------------------------------------------------------

SortStatistics::~SortStatistics() {}

//----------------------------------------------------------------------
// SortStatistics Retrieval Functions.
//----------------------------------------------------------------------

int SortStatistics::getStatRunSize() const { return runSize_; }
int SortStatistics::getStatNumRuns() const { return numRuns_; }

int SortStatistics::getStatMemSizeB() const { return memSizeB_; }

long SortStatistics::getStatNumRecs() const { return numRecs_; }

int SortStatistics::getStatRecLen() const { return recLen_; }

int SortStatistics::getStatNumInitRuns() const { return numInitRuns_; }

int SortStatistics::getStatFirstMergeOrder() const { return firstMergeOrder_; }

int SortStatistics::getStatFinalMergeOrder() const { return finalMergeOrder_; }

int SortStatistics::getStatMergeOrder() const { return mergeOrder_; }

int SortStatistics::getStatNumInterPasses() const { return numInterPasses_; }

int SortStatistics::getStatNumCompares() const { return numCompares_; }

int SortStatistics::getStatNumDupRecs() const { return numDupRecs_; }

long SortStatistics::getStatBeginSortTime() const { return beginSortTime_; }

long SortStatistics::getStatElapsedTime() const { return elapsedTime_; }

long SortStatistics::getStatIoWaitTime() const { return ioWaitTime_; }

int SortStatistics::getStatScrBlockSize() const { return scrBlockSize_; }

int SortStatistics::getStatScrNumBlocks() const { return scrNumBlocks_; }

int SortStatistics::getStatScrNumWrites() const { return scrNumWrites_; }

int SortStatistics::getStatScrNumReads() const { return scrNumReads_; }

int SortStatistics::getStatScrAwaitIo() const { return scrNumAwaitio_; }
