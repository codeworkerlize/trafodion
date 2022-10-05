

#include <iostream>

#include "SortAlgo.h"

//-----------------------------------------------------------------------
//  Constructor.
//-----------------------------------------------------------------------
SortAlgo::SortAlgo(int runsize, int recsize, NABoolean doNotAllocRec, int keysize, SortScratchSpace *scratch,
                   int explainNodeId, ExBMOStats *bmoStats) {
  sendNotDone_ = TRUE_L;
  runSize_ = runsize;
  recSize_ = recsize;
  doNotallocRec_ = doNotAllocRec;
  keySize_ = keysize;
  scratch_ = scratch;
  numCompares_ = 0L;
  internalSort_ = TRUE_L;
  explainNodeId_ = explainNodeId;
  bmoStats_ = bmoStats;
}

//-----------------------------------------------------------------------
// Name         : getNumOfCompares
//
// Parameters   :
//
// Description  : This function retrieves number of comparisons.

//
// Return Value : unsigned long numCompares_
//
//-----------------------------------------------------------------------

int SortAlgo::getNumOfCompares() const { return numCompares_; }

//-----------------------------------------------------------------------
// Name         : getScratch
//
// Parameters   :
//
// Description  : This function retrieves the scratch space pointer.

//
// Return Value : ScratchSpace* scratch
//
//-----------------------------------------------------------------------

SortScratchSpace *SortAlgo::getScratch() const { return scratch_; }

int SortAlgo::getRunSize() const { return runSize_; }

//-----------------------------------------------------------------------
// Name         : keyCompare
//
// Parameters   :
//
// Description  : This function is used to compare two keys and is
//                independent of the sort algorithm itself. Note the use
//                of the overloaded operators for key comparision.
//
// Return Value :
//  KEY1_IS_SMALLER
//  KEY1_IS_GREATER
//  KEYS_ARE_EQUAL
//-----------------------------------------------------------------------

short SortAlgo ::compare(char *key1, char *key2) {
  int result;
  // numCompares_ ++;
  if (key1 && key2) {
    result = str_cmp(key1, key2, (int)keySize_);
    // return (memcmp(key1,key2,(int)keySize_));
    return result;
  } else {
    if (key1 == NULL && key2 == NULL) return KEYS_ARE_EQUAL;
    if (key1 == NULL) return KEY1_IS_SMALLER;
    /*if (key2 == NULL)*/ return KEY1_IS_GREATER;
  };
  return 0;
}
