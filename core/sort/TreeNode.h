
#ifndef TREENODE_H
#define TREENODE_H
/* -*-C++-*-
******************************************************************************
*
* File:         TreeNode.h
* RCS:          $Id: TreeNode.h,v 1.2.16.2 1998/07/08 21:47:43  Exp $
*
* Description:  This class represents the TreeNode objects which make up the
*               tournament used by Replacement Selection algorithm.
*
* Created:	    05/20/96
* Modified:     $ $Date: 1998/07/08 21:47:43 $ (GMT)
* Language:     C++
* Status:       $State: Exp $
*
*
*
*
******************************************************************************
*/

#include <iostream>
#include "CommonStructs.h"
#include "Record.h"
#include "export/NABasicObject.h"
#include "SortError.h"

class TreeNode : public NABasicObject {
 public:
  TreeNode();
  ~TreeNode();

  void initialize(int nodenum, int associatedrun, TreeNode *fi, TreeNode *fe, Record *rec, CollHeap *heap,
                  SortError *sorterror, SortScratchSpace *scratch, NABoolean merge, NABoolean waited);
  void deallocate();

  // NABoolean setRecord(void* rec, int reclen,  int keylen, Int16 numberOfBytesForRecordSize);
  NABoolean setRecordTupp(void *rec, int reclen, int keylen, void *tupp, Int16 numberOfBytesForRecordSize);
  NABoolean getRecord(void *rec, int reclen);

  RESULT inputScr(int keylen, int reclen, SortScratchSpace *scratch, int &actRecLen,
                  // int keySize,
                  NABoolean waited = FALSE, Int16 numberOfBytesForRecordSize = 0);
  RESULT outputScr(int run, int reclen, SortScratchSpace *scratch, NABoolean waited = FALSE);

  TreeNode *getFe();
  TreeNode *getFi();

  void setLoser(TreeNode *node);
  TreeNode *getLoser();
  char *getKey();
  int getRun();
  void setRun(int run);

  Record *&record() { return record_; }

  SortMergeNode *sortMergeNode_;  // adapter to read the run from scratch.

 private:
  char *key_;
  Record *record_;
  TreeNode *fi_;
  TreeNode *fe_;

  TreeNode *loser_;
  int run_;
  int nodenum_;

  SortError *sortError_;
  CollHeap *heap_;
};

#endif
