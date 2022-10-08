
#ifndef STATISTICS_H
#define STATISTICS_H

/* -*-C++-*-
******************************************************************************
*
* File:         Statistics.h
* RCS:          $Id: Statistics.h,v 1.3 1998/08/10 15:33:44  Exp $
*
* Description:  This file contains the definitions of various structures
*               common to more than one class in ArkSort.
*
* Created:      12/12/96
* Modified:     $ $Date: 1998/08/10 15:33:44 $ (GMT)
* Language:     C++
* Status:       $State: Exp $
*
*
*
******************************************************************************
*/

#include "common/Platform.h"

// fix later UNIBR4
#ifdef max
#undef max
#endif
#ifdef min
#undef min
#endif

#include "common/Int64.h"

class SortStatistics {
 public:
  SortStatistics();
  ~SortStatistics();
  int getStatMemSizeB() const;
  long getStatNumRecs() const;
  int getStatRecLen() const;
  int getStatRunSize() const;
  int getStatNumRuns() const;
  int getStatNumInitRuns() const;
  int getStatFirstMergeOrder() const;
  int getStatFinalMergeOrder() const;
  int getStatMergeOrder() const;
  int getStatNumInterPasses() const;
  int getStatNumCompares() const;
  int getStatNumDupRecs() const;
  long getStatBeginSortTime() const;
  long getStatElapsedTime() const;
  long getStatIoWaitTime() const;
  int getStatScrBlockSize() const;
  int getStatScrNumBlocks() const;
  int getStatScrNumWrites() const;
  int getStatScrNumReads() const;
  int getStatScrAwaitIo() const;

  friend class SortUtil;

 private:
  int memSizeB_;
  long numRecs_;
  int recLen_;
  int runSize_;  // number of nodes in the tournament tree
  int numRuns_;

  int numInitRuns_;
  int firstMergeOrder_;
  int finalMergeOrder_;
  int mergeOrder_;
  int numInterPasses_;
  int numCompares_;
  int numDupRecs_;
  long beginSortTime_;
  long ioWaitTime_;   // hr min sec millisec microsec in each respective word
  long elapsedTime_;  // in seconds
  int scrBlockSize_;
  int scrNumBlocks_;
  int scrNumWrites_;
  int scrNumReads_;
  int scrNumAwaitio_;
};

#endif
