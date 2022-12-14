
#ifndef QSORT_H
#define QSORT_H

/* -*-C++-*-
******************************************************************************
*
* File:         Qsort.h
* RCS:          $Id: Qsort.h,v 1.2.16.2 1998/07/08 21:47:10  Exp $
*
* Description:  This class implements the QuickSort Algorithm. It is derived
*               from the SortAlgo base class in compliance with the Strategy
*               Policy design pattern from Gamma. Note that QuickSort is used
*               only for Run generation and not for merging. Replacement
*               selection is the best for merging.
*
* Created:	    05/20/96
* Modified:     $ $Date: 1998/07/08 21:47:10 $ (GMT)
* Language:     C++
* Status:       $State: Exp $
*
******************************************************************************
*/

// -----------------------------------------------------------------------
// Change history:
//
// $Log: Qsort.h,v $
// Revision 1.6  1998/07/08 15:25:36
// Changes in sort to allocate the initial Sort Memory dynamically. If the
// memory needed exceeds the SortMaxMem_ set by the executor, we spill over to
// a scratch file.
//
// Revision 1.5  1998/04/23 15:13:28
// Merge of NSKPORT branch (tag NSK_T4_980420) into FCS branch.
//
// Revision 1.2.16.1  1998/03/11 22:33:13
// merged FCS0310 and NSKPORT_971208
//
// Revision 1.4  1998/01/16 02:34:33
// merged LuShung's changes with the latest sort changes (from Ganesh).
//
// Revision 1.2  1997/04/23 00:29:01
// Merge of MDAM/Costing changes into SDK thread
//
// Revision 1.1.1.1.2.1  1997/04/11 23:23:04
// Checking in partially resolved conflicts from merge with MDAM/Costing
// thread. Final fixes, if needed, will follow later.
//
// Revision 1.4.4.1  1997/04/10 18:30:53
// *** empty log message ***
//
// Revision 1.1.1.1  1997/03/28 01:38:52
// These are the source files from SourceSafe.
//
//
// 8     3/06/97 4:54p Sql.lushung
// A fix for the memory delete problem is make in this version of sort.
// Revision 1.4  1997/01/14 03:22:13
//  Error handling and Statistics are implemented in this version of arksort.
//
// Revision 1.3  1996/12/11 22:53:35
// Change is made in arksort to allocate memory from executor's space.
// Memory leaks existed in arksort code are also fixed.
//
// Revision 1.2  1996/11/13 02:20:05
// Record Length, Key Length, Number of records etc have been changed from
// short/int to unsigned long. This was requested by the Executor group.
//
// Revision 1.1  1996/08/15 14:47:36
// Initial revision
//
// Revision 1.1  1996/08/02 03:39:32
// Initial revision
//
// Revision 1.18  1996/05/20 16:32:34  <author_name>
// Added <description of the change>.
// -----------------------------------------------------------------------

#include "sort/Const.h"
#include "SortAlgo.h"
#include "sort/SortError.h"
#include "TreeNode.h"
#include "export/NABasicObject.h"

class SortUtil;
class ExBMOStats;
//----------------------------------------------------------------------
// This represents the structure used to store the key and record pointers
// to be used for quicksort.
//----------------------------------------------------------------------

void heapSort(RecKeyBuffer keysToSort[], int runsize);
void siftDown(RecKeyBuffer keysToSort[], int root, int bottom);

class Qsort : public SortAlgo {  // SortAlgo inherits from NABasicObject

 public:
  Qsort(int recmax, int sortmaxmem, int recsize, NABoolean doNotallocRec, int keysize, SortScratchSpace *scratch,
        NABoolean iterQuickSort, CollHeap *heap, SortError *sorterror, int explainNodeId, ExBMOStats *bmoStats,
        SortUtil *sortutil);
  ~Qsort(void);

  int sortSend(void *rec, int len, void *tupp);

  int sortClientOutOfMem(void);

  int sortSendEnd();

  int sortReceive(void *rec, int &len);
  int sortReceive(void *&rec, int &len, void *&tupp);

  int generateInterRuns();

  UInt32 getOverheadPerRecord(void);

 private:
  char *median(RecKeyBuffer keysToSort[], long left, long right);
  NABoolean quickSort(RecKeyBuffer keysToSort[], long left, long right);
  NABoolean iterativeQuickSort(RecKeyBuffer keysToSort[], long left, long right);
  void heapSort(RecKeyBuffer keysToSort[], long runsize);
  void siftDown(RecKeyBuffer keysToSort[], long root, long bottom);
  int generateARun();
  NABoolean swap(RecKeyBuffer *recKeyOne, RecKeyBuffer *recKeyTwo);
  void cleanUpMemoryQuota(void);

  int loopIndex_;
  int currentRun_;
  int recNum_;
  int allocRunSize_;
  int sortMaxMem_;
  NABoolean isIterativeSort_;
  Record *rootRecord_;
  RecKeyBuffer *recKeys_;
  SortError *sortError_;
  CollHeap *heap_;
  SortUtil *sortUtil_;
  int initialRunSize_;
};

#endif
