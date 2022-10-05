
#ifndef SORTTOPN_H
#define SORTTOPN_H

/* -*-C++-*-
******************************************************************************
*
* File:         SortTopN.h
*
*
******************************************************************************
*/

#include "SortAlgo.h"
#include "Record.h"
#include "Const.h"
#include "export/NABasicObject.h"
#include "SortError.h"

class SortUtil;
class ExBMOStats;

class SortTopN : public SortAlgo {  // SortAlgo inherits from NABasicObject

 public:
  SortTopN(int recmax, int sortmaxmem, int recsize, NABoolean doNotallocRec, int keysize,
           SortScratchSpace *scratch, NABoolean iterQuickSort, CollHeap *heap, SortError *sorterror,
           int explainNodeId, ExBMOStats *bmoStats, SortUtil *sortutil);
  ~SortTopN(void);

  int sortSend(void *rec, int len, void *tupp);

  int sortClientOutOfMem(void) { return 0; }

  int sortSendEnd();

  int sortReceive(void *rec, int &len);
  int sortReceive(void *&rec, int &len, void *&tupp);
  UInt32 getOverheadPerRecord(void);
  int generateInterRuns() { return 0; }

 private:
  void buildHeap();
  void satisfyHeap();
  void insertRec(void *rec, int len, void *tupp);
  void sortHeap();
  void siftDown(RecKeyBuffer keysToSort[], long root, long bottom);
  NABoolean swap(RecKeyBuffer *recKeyOne, RecKeyBuffer *recKeyTwo);

  int loopIndex_;
  int recNum_;
  int allocRunSize_;
  NABoolean isHeapified_;
  RecKeyBuffer insertRecKey_;
  RecKeyBuffer *topNKeys_;
  SortError *sortError_;
  CollHeap *heap_;
  SortUtil *sortUtil_;
};

#endif
