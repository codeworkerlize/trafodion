
#include "Record.h"

#include <string.h>

#include <fstream>
#include <iostream>

#include "ex_ex.h"

extern void ReleaseTupp(void *tupp);

//----------------------------------------------------------------------
// Record Constructor.
//----------------------------------------------------------------------

Record::Record() {}

//----------------------------------------------------------------------
// Overloaded Record Constructor.
//----------------------------------------------------------------------
Record::Record(int size, NABoolean doNotallocRec, CollHeap *heap) {
  recSize_ = size;
  tupp_ = NULL;
  heap_ = heap;
  if (doNotallocRec) {
    rec_ = NULL;
    allocatedRec_ = FALSE_L;
  } else {
    rec_ = new (heap_) char[recSize_ + 1];
    ex_assert(rec_ != NULL, "Record::Record: rec_ is NULL");
    allocatedRec_ = TRUE_L;
  }
}

Record::Record(void *rec, int reclen, void *tupp, CollHeap *heap, SortError *sorterror) {
  recSize_ = reclen;
  sortError_ = sorterror;
  heap_ = heap;
  tupp_ = tupp;
  allocatedRec_ = FALSE_L;
  rec_ = (char *)rec;
}

//----------------------------------------------------------------------
// Record Destructor.
//----------------------------------------------------------------------
Record::~Record(void) {
  if (allocatedRec_ && rec_ != NULL) {
    NADELETEBASIC(rec_, heap_);
    rec_ = NULL;
  }
}
//----------------------------------------------------------------------
// Allocate space for record.
//----------------------------------------------------------------------
void Record::initialize(int recsize, NABoolean doNotallocRec, CollHeap *heap, SortError *sorterror) {
  recSize_ = recsize;
  sortError_ = sorterror;
  heap_ = heap;
  tupp_ = NULL;

  if (doNotallocRec) {
    rec_ = NULL;
    allocatedRec_ = FALSE_L;
  } else {
    rec_ = new (heap_) char[recsize + 1];
    ex_assert(rec_ != NULL, "Record::initialize: rec_ is NULL");
    allocatedRec_ = TRUE_L;
  }
}

//-----------------------------------------------------------------------
// Name         : getFromScr
//
// Parameters   : ...
//
// Description  : Read a record from the from the scratch file run.
//
// Return Value :
//  0 if Read Succesful.
//  1 if EOF encountered.
//-----------------------------------------------------------------------

RESULT Record::getFromScr(SortMergeNode *sortMergeNode, int reclen, SortScratchSpace *scratch, int &actRecLen,
                          // int keySize,
                          NABoolean waited, Int16 numberOfBytesForRecordSize) {
  RESULT status;
  status = scratch->readSortMergeNode(sortMergeNode, rec_, reclen, actRecLen, /*keySize,*/ waited,
                                      numberOfBytesForRecordSize);
  recSize_ = actRecLen;
  if (status)
    return status;

  else
    return SCRATCH_SUCCESS;
}

//-----------------------------------------------------------------------
// Name         : putFile
//
// Parameters   : ...
//
// Description  : Write a record to the to file. Used during the final
//                merge phase.
//
// Return Value :
//  None.
//-----------------------------------------------------------------------

// void Record::putToFile(ofstream& to)
//{
// to << rec_ << endl;
//}

//-----------------------------------------------------------------------
// Name         : putScr
//
// Parameters   : ...
//
// Description  : Write a record to the scratch file run. Used during
//                run generation.
//
// Return Value :
//  0 if Read Succesful.
//  1 if EOF encountered.
//-----------------------------------------------------------------------
RESULT Record::putToScr(int run, int reclen, SortScratchSpace *scratch, NABoolean waited) {
  RESULT result = scratch->writeRunData(rec_, reclen, run, waited);
  if (tupp_ != NULL) {
    ReleaseTupp(tupp_);
    tupp_ = NULL;
  }
  return result;
}

//-----------------------------------------------------------------------
// Name         : releaseTupp
//
// Parameters   : none
//
// Description  : If the record has a tupp, release it.
//
// Return Value : none
//-----------------------------------------------------------------------
void Record::releaseTupp(void) {
  if (tupp_) {
    ReleaseTupp(tupp_);
    tupp_ = NULL;
  }
}

//-----------------------------------------------------------------------
// Name         : extractKey
//
// Parameters   : ...
//
// Description  : Extract the key portion of the record. Note currently
//                I use the first 10 bytes of the record as key. In
//                actual implementation we need to use the length of the
//                encoded key appended (or prepended) to the record.
//
// Return Value :
//  0 if Read Succesful.
//  1 if EOF encountered.
//-----------------------------------------------------------------------
char *Record::extractKey(int keylen, Int16 offset) { return (rec_ + offset); }

NABoolean Record::setRecord(void *rec, int reclen) {
  if (allocatedRec_)
    memcpy(rec_, rec, (int)reclen);
  else
    rec_ = (char *)rec;
  return SORT_SUCCESS;
}

NABoolean Record::getRecord(void *rec, int reclen) const {
  memcpy(rec, rec_, (int)reclen);
  return SORT_SUCCESS;
}

NABoolean Record::setRecordTupp(void *rec, int reclen, void *tupp) {
  rec_ = (char *)rec;
  tupp_ = tupp;
  return SORT_SUCCESS;
}

NABoolean Record::getRecordTupp(void *&rec, int reclen, void *&tupp) const {
  rec = rec_;
  tupp = tupp_;
  return SORT_SUCCESS;
}
