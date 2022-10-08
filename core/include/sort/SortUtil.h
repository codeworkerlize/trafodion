
#ifndef SORTUTIL_H
#define SORTUTIL_H

/* -*-C++-*-
******************************************************************************
*
* File:         SortUtil.h
* RCS:          $Id: SortUtil.h,v 1.2.16.1 1998/03/11 22:33:37  Exp $
*
* Description:  This file contains the definition of the SortUtil class. This
*               class is the interface to the callers like Executor for using
*               the sort utility.
*
* Created:      07/12/96
* Modified:     $ $Date: 1998/03/11 22:33:37 $ (GMT)
* Language:     C++
* Status:       $State: Exp $
*
*
******************************************************************************
*/

#include "sort/CommonStructs.h"
#include "sort/Const.h"
#include "sort/ScratchFileMap.h"
#include "sort/ScratchSpace.h"
#include "SortAlgo.h"
#include "SortUtilCfg.h"
#include "Statistics.h"
#include "TourTree.h"
#include "common/ComSpace.h"
#include "export/NABasicObject.h"

class ExBMOStats;

class SortUtil : public NABasicObject {
 public:
  SortUtil(int explainNodeId);
  ~SortUtil();

  NABoolean sortInitialize(SortUtilConfig &config, int topNSize);
  NABoolean sortEnd(void);

  int sortSend(void *record, int len, void *tupp);

  int sortClientOutOfMem(void);

  int sortSendEnd(NABoolean &internalSort);

  int sortReceive(void *record, int &len);
  int sortReceive(void *&record, int &len, void *&tupp);

  void DeleteSortAlgo();

  void getStatistics(SortStatistics *statistics);
  void getSortError(SortError *srterr);
  short getSortError() const;
  short getSortSysError() const { return sortError_.getSysError(); }
  short getSortErrorDetail() const { return sortError_.getErrorDetail(); }
  char *getSortErrorMsg() const { return sortError_.getSortErrorMsg(); }
  void setErrorInfo(short sorterr, short syserr = 0, short syserrdetail = 0, char *errorMsg = NULL);

  void doCleanUp();

  NABoolean consumeMemoryQuota(UInt32 bufferSizeBytes);
  void returnConsumedMemoryQuota(UInt32 bufferSizeBytes);
  void returnExcessMemoryQuota(UInt32 overheadPerRecord = 0);
  inline NABoolean memoryQuotaSystemEnabled(void) {
    // BMO not enabled if negative.
    return (config_ != NULL && config_->initialMemoryQuotaMB_ > 0);
  }
  UInt32 getMaxAvailableQuotaMB(void);
  NABoolean withinMemoryLimitsAndPressure(long reqMembytes);

  SortUtilConfig *config(void) { return config_; }
  SortScratchSpace *getScratch() const { return scratch_; };
  NABoolean scratchInitialize(void);
  void setupComputations(SortUtilConfig &config);
  void setBMOStats(ExBMOStats *stat) { bmoStats_ = stat; };
  UInt32 estimateMemoryToAvoidIntMerge(UInt32 numruns, int sortMergeBlocksPerBuffer);
  UInt32 estimateMergeOrder(UInt32 maxMergeMemory, int sortMergeBlocksPerBuffer);
  int getRunSize() const { return sortAlgo_->getRunSize(); };

 protected:
  int sortReceivePrepare();

 private:
  void reInit();
  short version_;  // The ArkSort version
  SORT_STATE state_;
  SortUtilConfig *config_;
  NABoolean internalSort_;         // indicates if overflowed or not.
  NABoolean sortReceivePrepared_;  // if sort overflowed, prepare the merge tree for receive.
  SortAlgo *sortAlgo_;             // Algorithms are implemented as sub-classes
                                   // of  Sort algorithm base class.
                                   // This implementation  allows extensibility as
                                   // new algorithms can be added easily.  This is
                                   // similiar to the Policy design pattern
                                   // suggested by Enrich Gamma.

  SortScratchSpace *scratch_;  // This object is used for all temporary work space
                               // needed by SortUtil.

  SortStatistics stats_;  // A statistics object which accumulates
                          // statistics related to the sorting session.

  SortError sortError_;  // Error code of Last Sort error if any.

  int explainNodeId_;  // Help runtime reporting.

  UInt32 memoryQuotaUtil_;  // memory quota consumed at the util level.

  UInt32 overheadPerRecord_;  // memory per record consumed by initial algorithm.
                              // The algorithm may change during sort processing.
                              // We need this value to retain the memory quota
                              // when returning any excess quota consumed during
                              // sort processing.
  ExBMOStats *bmoStats_;
};

#endif
