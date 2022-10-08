
#ifndef TREE_H
#define TREE_H

/* -*-C++-*-
******************************************************************************
*
* File:         TourTree.h
* RCS:          $Id: TourTree.h,v 1.2.16.2 1998/07/08 21:47:39  Exp $
*
* Description:  This class represents the tournament tree which is used by
*               Replacement Selection algorithm. The tournament tree is an
*               aggregate of TreeNode objects defined in TreeNode.h. This
*               class is derived from the abstract base class SortAlgo.
*
* Created:	    05/20/96
* Modified:     $ $Date: 1998/07/08 21:47:39 $ (GMT)
* Language:     C++
* Status:       $State: Exp $
*
******************************************************************************
*/

#include "sort/CommonStructs.h"
#include "sort/Const.h"
#include "sort/ScratchSpace.h"
#include "SortAlgo.h"
#include "sort/SortError.h"
#include "TreeNode.h"
#include "export/NABasicObject.h"

class SortUtil;
class Tree : public SortAlgo {  // SortAlgo inherits from NABasciObject

 public:
  Tree(int numruns, int runsize, int recsize, NABoolean doNotallocRec, int keysize, SortScratchSpace *scratch,
       CollHeap *heap, SortError *sorterror, int explainNodeId, ExBMOStats *bmoStats, SortUtil *sortUtil,
       int runnum = 0, NABoolean merge = FALSE_L, NABoolean waited = FALSE_L);

  ~Tree(void);

  int sortSend(void *rec, int len, void *tupp);

  int sortClientOutOfMem(void);

  int sortSendEnd();

  int sortReceive(void *rec, int &len);
  int sortReceive(void *&rec, int &len, void *&tupp);

  int generateInterRuns(void);

  UInt32 getOverheadPerRecord(void);

 private:
  void determineNewWinner();
  RESULT outputWinnerToScr(void);

  TreeNode *rootNode_;
  Record *rootRecord_;
  TreeNode *winner_;
  char *keyOfLastWinner_;
  short height_;
  int numRuns_;
  int maxRuns_;
  int currentRun_;
  int winnerRun_;
  int baseRun_;
  SortError *sortError_;
  CollHeap *heap_;
  SortUtil *sortUtil_;
};

#endif
