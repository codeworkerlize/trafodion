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
#ifndef SCRATCHFILEMAP_H
#define SCRATCHFILEMAP_H

/* -*-C++-*-
******************************************************************************
*
* File:         ScratchFileMap.h
* RCS:          $Id: ScratchFileMap.h,v 1.1 2006/11/01 01:44:37  Exp $
*
* Description:  This class is a container class for all the scratch files that
*               are created and used by ArkSort.
*
* Created:	    05/20/96
* Modified:     $ $Date: 2006/11/01 01:44:37 $ (GMT)
* Language:     C++
* Status:       $State: Exp $
*
******************************************************************************
*/

#include "common/Platform.h"

#include <string.h>
#include "CommonStructs.h"
#include "ScratchFile.h"
#include "export/NABasicObject.h"
#include "SortError.h"

class SQScratchFile;
//----------------------------------------------------------------------
// The data structure used to store information about multiple scratch
// file objects.
//----------------------------------------------------------------------

struct FileMap : public NABasicObject {
  short index_;
  int firstScrBlockWritten_;  // First scr block written to this scr file

  SQScratchFile *scrFile_;
};

class ScratchFileMap : public NABasicObject {
  friend class ScratchSpace;
  friend class SortScratchSpace;

 public:
  ScratchFileMap(CollHeap *heap, SortError *sorterror, NABoolean breakEnabled, short maxscrfiles);
  ~ScratchFileMap();
  void setBreakEnabled(NABoolean flag) { breakEnabled_ = flag; };
  void closeFiles(ScratchFile *keepFile = NULL);
  ScratchFile *createNewScrFile(ScratchSpace *scratchSpace, Int32 scratchMgmtOption, Int32 scratchMaxOpens,
                                NABoolean preAllocateExtents, NABoolean asynchReadQueue);

  ScratchFile *mapBlockNumToScrFile(SBN blockNum, int &blockOffset);
  void setFirstScrBlockNum(SBN blockNum);
  SBN getFirstScrBlockNum(ScratchFile *scr);
  int totalNumOfReads();
  int totalNumOfWrites();
  int totalNumOfAwaitio();
  void closeScrFilesUpto(SBN uptoBlockNum);

 private:
  short maxScratchFiles_;
  short numScratchFiles_;      // num of scratch files created thus far
  short currentScratchFiles_;  // most recently referenced scratch file
  FileMap *fileMap_;
  SortError *sortError_;
  CollHeap *heap_;
  NABoolean breakEnabled_;
};

#endif
