
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

#include <string.h>

#include "CommonStructs.h"
#include "ScratchFile.h"
#include "SortError.h"
#include "common/Platform.h"
#include "export/NABasicObject.h"

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
  ScratchFile *createNewScrFile(ScratchSpace *scratchSpace, int scratchMgmtOption, int scratchMaxOpens,
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
