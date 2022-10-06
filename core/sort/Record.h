
#ifndef RECORD_H
#define RECORD_H

/* -*-C++-*-
******************************************************************************
*
* File:         Record.h
* RCS:          $Id: Record.h,v 1.2.16.2 1998/07/08 21:47:13  Exp $
*
* Description:  This class represents the Record. The actual data is stored
*               in a char* with member fuctions to retrieve and store this
*               data.
*
* Created:	    05/20/96
* Modified:     $ $Date: 1998/07/08 21:47:13 $ (GMT)
* Language:     C++
* Status:       $State: Exp $
*
******************************************************************************
*/

#include "CommonStructs.h"
#include "Const.h"
#include "ScratchSpace.h"
#include "SortError.h"
#include "export/NABasicObject.h"

class Record;

struct RecKeyBuffer {
  char *key_;
  Record *rec_;
};

class Record {
 public:
  Record();
  Record(int size, NABoolean doNotallocRec, CollHeap *heap);
  Record(void *rec, int reclen, void *tupp, CollHeap *heap, SortError *sorterror);
  ~Record(void);

  void initialize(int recsize, NABoolean doNotallocRec, CollHeap *heap, SortError *sorterror);

  char *extractKey(int keylen, Int16 offset);

  NABoolean setRecord(void *rec, int reclen);
  NABoolean getRecord(void *rec, int reclen) const;

  NABoolean setRecordTupp(void *rec, int reclen, void *tupp);
  NABoolean getRecordTupp(void *&rec, int reclen, void *&tupp) const;

  RESULT getFromScr(SortMergeNode *sortMergeNode, int reclen, SortScratchSpace *scratch, int &actRecLen,
                    // int keySize,
                    NABoolean waited = FALSE, Int16 numberOfBytesForRecordSize = 0);

  // void putToFile(ofstream& to);
  RESULT putToScr(int run, int reclen, SortScratchSpace *scratch, NABoolean waited = FALSE);

  void releaseTupp(void);
  int getRecSize() { return recSize_; }

 private:
  char *rec_;  // Pointer to the data which constitutes the actual
               // record data.
  int recSize_;
  void *tupp_;

  NABoolean allocatedRec_;
  SortError *sortError_;
  CollHeap *heap_;
};

#endif
