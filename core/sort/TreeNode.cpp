
/* -*-C++-*-
******************************************************************************
*
* File:         TreeNode.C
* RCS:          $Id: TreeNode.cpp,v 1.9 1998/07/29 22:02:33  Exp $
*
* Description:  This file contains the implementation of all member functions
*               of class TreeNode.
*
* Created:	    05/25/96
* Modified:     $ $Date: 1998/07/29 22:02:33 $ (GMT)
* Language:     C++
* Status:       $State: Exp $
*
*
*
*
******************************************************************************
*/

#include <iostream>
#include <fstream>
#include "TreeNode.h"

//---------------------------------------------------------------------------
// Class Constructor.
//---------------------------------------------------------------------------
TreeNode::TreeNode() {}

//---------------------------------------------------------------------------
// Class Destructor. Delete all the space pointed to by the ptrs in TreeNode
//---------------------------------------------------------------------------
TreeNode::~TreeNode() {
  if (key_ != NULL) {
    key_ = NULL;
  }
  // No need to delete record_, fi_, fe_.  record_ points to existing Record.
  // fi_ fe_ point to existing TreeNode. Also no need to delete scratch file
  // since it is handled separately. Infact sortMergeNode_->scrBlock1_ must be
  // deallocated in the same location as it got allocated.
  if (sortMergeNode_ != NULL) {
    sortMergeNode_->cleanup();
    NADELETEBASIC(sortMergeNode_, heap_);
    sortMergeNode_ = NULL;
  }
}

//-----------------------------------------------------------------------
// Name         : initialize
//
// Parameters   : ...
//
// Description  : This function is used to initialize the TreeNode members.
//                This is called from the contructor of Tree class when
//                the tournament tree is being built.
//
// Return Value :
//   None.
//
//-----------------------------------------------------------------------

void TreeNode::initialize(int nodenum, int associatedrun, TreeNode *fi, TreeNode *fe, Record *rec, CollHeap *heap,
                          SortError *sorterror, SortScratchSpace *scratch, NABoolean merge, NABoolean waited) {
  key_ = NULL;
  record_ = NULL;
  nodenum_ = nodenum;
  fe_ = fe;
  fi_ = fi;
  run_ = 0;
  record_ = &rec[nodenum];
  loser_ = this;
  heap_ = heap;
  if (merge) {
    sortMergeNode_ = new (heap_) SortMergeNode(associatedrun, scratch);
    if (sortMergeNode_ == NULL) {
      sortError_->setErrorInfo(EScrNoMemory  // sort error
                               ,
                               0  // syserr: the actual FS error
                               ,
                               0  // syserrdetail
                               ,
                               "Treenode::initialize"  // methodname
      );
      return;
    } else {
      RESULT status;

      // initiateReadIO will report any errors
      status = scratch->initiateSortMergeNodeRead(sortMergeNode_, waited);
      if (status == SCRATCH_FAILURE) {
        sorterror = scratch->getSortError();
        sortError_ = sorterror;
        return;
      }
    }
  } else
    sortMergeNode_ = NULL;
  sortError_ = sorterror;
}

// Description  : This function is used to deallocate objects that
//                were allocated in initialize().

void TreeNode::deallocate() {
  // No need to delete scratch file, it is managed outside the scope
  // of this code. Infact sortMergeNode_->scrBlock1_ must be
  // deallocated in the same location as it got allocated.
  if (sortMergeNode_ != NULL) {
    sortMergeNode_->cleanup();
    NADELETEBASIC(sortMergeNode_, heap_);
    sortMergeNode_ = NULL;
  }
}

//-----------------------------------------------------------------------
// Name         : outputScr
//
// Parameters   : ...
//
// Description  : This function is used to output the record
//                represented by this tree node, to the Scratch file.
//                This is used during run generation.
//
// Return Value :
//   None.
//
//----------------------------------------------------------------------
RESULT TreeNode::outputScr(int run, int reclen, SortScratchSpace *scratch, NABoolean waited) {
  RESULT status = SCRATCH_SUCCESS;
  if (scratch->getDiskPool() == NULL) {
    scratch->generateDiskTable(sortError_);
  }
  status = record_->putToScr(run, reclen, scratch, waited);  // putToScr can get an error
  if (status != SCRATCH_SUCCESS) return status;
  key_ = NULL;
  return status;
}

//-----------------------------------------------------------------------
// Name         : inputScr
//
// Parameters   : ...
//
// Description  : This function is used to input the record
//                represented by this tree node, from the scratch file.
//                This is used during merging.
//
// Return Value :
//   0 - If record read successfully.
//----------------------------------------------------------------------
RESULT TreeNode::inputScr(int keylen, int reclen, SortScratchSpace *scratch, int &actRecLen,
                          // int keySize,
                          NABoolean waited, Int16 numberOfBytesForRecordSize) {
  RESULT status;
  status =
      record_->getFromScr(sortMergeNode_, reclen, scratch, actRecLen, /*keySize,*/ waited, numberOfBytesForRecordSize);
  if ((status != END_OF_RUN) && (status == SCRATCH_SUCCESS))
    key_ = record_->extractKey(keylen, numberOfBytesForRecordSize);
  return status;
}
/*
NABoolean TreeNode::setRecord(void *rec, int reclen,
                              int keylen,
                              Int16 numberOfBytesForRecordSize)
{
  NABoolean status;
  status = record_->setRecord(rec, reclen);
  key_ = record_->extractKey(keylen, numberOfBytesForRecordSize);
  return(status);
}
*/
NABoolean TreeNode::setRecordTupp(void *rec, int reclen, int keylen, void *tupp,
                                  Int16 numberOfBytesForRecordSize) {
  NABoolean status;
  status = record_->setRecordTupp(rec, reclen, tupp);
  key_ = record_->extractKey(keylen, numberOfBytesForRecordSize);
  return (status);
}

NABoolean TreeNode::getRecord(void *rec, int reclen) { return record_->getRecord(rec, reclen); }

//-----------------------------------------------------------------------
// Name         : getXXXXXX and setXXXXXX
//
// Parameters   : ...
//
// Description  : All the following get and set functions are used to
//                set the value of the private data members of TreeNode
//                and to get these values whenever required.

// Return Value :
//   0 - If record read successfully.
//   1 - If EOF.
//----------------------------------------------------------------------

TreeNode *TreeNode::getFe() { return fe_; }

TreeNode *TreeNode::getFi() { return fi_; }

void TreeNode::setLoser(TreeNode *node) { loser_ = node; }

TreeNode *TreeNode::getLoser() { return (loser_); }

char *TreeNode::getKey() { return key_; }

int TreeNode::getRun() { return (run_); }

void TreeNode::setRun(int run) { run_ = run; }
