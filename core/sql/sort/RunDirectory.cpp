/**********************************************************************
// @@@ START COPYRIGHT @@@
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// @@@ END COPYRIGHT @@@
**********************************************************************/
/* -*-C++-*-
******************************************************************************
*
* File:         RunDirectory.C
* RCS:          $Id: RunDirectory.cpp,v 1.11 1998/07/29 22:02:16  Exp $
*                               
* Description:  This file contains the implementation of all member functions
*               of class RunDirectory. RunDirectory is used to encapsulate
*               all data and processing related to scratch file run management
*               
* Created:	    04/25/96
* Modified:     $ $Date: 1998/07/29 22:02:16 $ (GMT)
* Language:     C++
* Status:       $State: Exp $
*
******************************************************************************
*/


#include "common/Platform.h"
extern "C" {
}
#define _cc_status short

#include <iostream>
#include <string.h>

#include "RunDirectory.h"

//----------------------------------------------------------------------
// The class constructor.
//----------------------------------------------------------------------

RunDirectory::RunDirectory(short maxRuns, CollHeap* heap, SortError* sorterror)
{
   currentMaxRuns_ = maxRuns;
   mergeOrder_ = 0;

   numRunsGenerated_ = 0;
   numRunsRemaining_ = 0;

   heap_ = heap;   
   //rdListPtr_ = new (heap) RunDirectoryEntry[maxRuns];
   rdListPtr_ = (RunDirectoryEntry*)heap->allocateMemory(maxRuns * sizeof(RunDirectoryEntry));
   sortError_ = sorterror;
}

//----------------------------------------------------------------------
// The class destructor.
//----------------------------------------------------------------------
RunDirectory::~RunDirectory()
{
 if (rdListPtr_ != NULL) {
    heap_->deallocateMemory((void*)rdListPtr_);
    rdListPtr_ = NULL;
  }
}

//-----------------------------------------------------------------------
// Name         : startNewRun
// 
// Parameters   :
//
// Description  : This member function is used whenever new run is being 
//                generated by the sort algorithm. It also returns the 
//                run number of the newly started run.
//   
// Return Value :
//  run number of the newly started run.
//
//-----------------------------------------------------------------------

Lng32 RunDirectory::startNewRun(SBN scrblocknum)
{
  RunDirectoryEntry *tempRdListPtr;
  numRunsGenerated_ += 1;
  if (numRunsGenerated_ > currentMaxRuns_) {
   currentMaxRuns_ += MAXRUNS;
   RunDirectoryEntry *newRDListPtr =
      (RunDirectoryEntry*)heap_->allocateMemory(currentMaxRuns_ *
                                      sizeof(RunDirectoryEntry), FALSE);
   if (newRDListPtr == NULL)
    {
      sortError_->setErrorInfo( EScrNoMemory   //sort error
			       ,0          //syserr: the actual FS error
			       ,0          //syserrdetail
			       ,"RunDirectory::startNewRun"     //methodname
			       );
      return SORT_FAILURE;
    }
   memcpy(newRDListPtr, rdListPtr_,
                        (numRunsGenerated_ - 1) * sizeof(RunDirectoryEntry));
   heap_->deallocateMemory((void*)rdListPtr_);
   rdListPtr_ = newRDListPtr;
  }
  tempRdListPtr = rdListPtr_ + numRunsGenerated_ - 1;

  tempRdListPtr->runNum_     = numRunsGenerated_;
  tempRdListPtr->firstBlock_ = scrblocknum;     
  tempRdListPtr->numBlocks_  = 0;
  tempRdListPtr->numRecs_    = 0;      
  return numRunsGenerated_;
}

//-----------------------------------------------------------------------
// Name         : endCurrentRun
// 
// Parameters   :
//
// Description  : This member function is used whenever a particular 
//                run generation is completed. Information such as 
//                the number of blocks in the run and TIMESTAMP should
//                be saved at this point.
//   
// Return Value :
//  run number of the newly started run.
//
//-----------------------------------------------------------------------
void RunDirectory::endCurrentRun(void)
{
  RunDirectoryEntry *tempRdListPtr;
  
  tempRdListPtr = rdListPtr_ + numRunsGenerated_;
  // TIMESTAMP(tempRdListPtr->endTime_);     
}

//-----------------------------------------------------------------------
// Name         : getTotalNumOfRuns
// 
// Parameters   :
//
// Description  : This member function returns the number of runs generated
//                so far.
//   
// Return Value :
//  The total number of runs generated so far.
//
//-----------------------------------------------------------------------
Lng32 RunDirectory::getTotalNumOfRuns(void)
{
 return numRunsGenerated_;
}

//-----------------------------------------------------------------------
// Name         : mapRunNumberToFirstSBN
// 
// Parameters   :
//
// Description  : This member function is used to get the first block
//                number for a particular run. This is used during 
//                merge phase while reading runs from the scratch file.
//
// Return Value :
//  Scratch block number associated with  the run number specified in the
//  function parameter.
//-----------------------------------------------------------------------

SBN RunDirectory::mapRunNumberToFirstSBN(Lng32 runNumber)
{
  RunDirectoryEntry *tempRdListPtr;
  
  if (runNumber > numRunsGenerated_)
    {
      return EOF;
    }
  else
    {
      tempRdListPtr = rdListPtr_ + runNumber - 1;
      return tempRdListPtr->firstBlock_;
    }

} 










