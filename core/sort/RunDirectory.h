
#ifndef RUNDIRECTORY_H
#define RUNDIRECTORY_H

/* -*-C++-*-
******************************************************************************
*
* File:         RunDirectory.h
* RCS:          $Id: RunDirectory.h,v 1.2.16.2 1998/07/08 21:47:16  Exp $
*
* Description:  This class is used to store a directory of runs generated
*               during run generation phase. This directory is used
*               for reading the runs during Merge phase.
*
* Created:	05/20/96
* Modified:     $ $Date: 1998/07/08 21:47:16 $ (GMT)
* Language:     C++
* Status:       $State: Exp $
*
*
******************************************************************************
*/

// -----------------------------------------------------------------------
// Change history:
//
// $Log: RunDirectory.h,v $
// Revision 1.2.16.2  1998/07/08 21:47:16
// Merge of FCS0612 into NSKPORT_971208
//
// Revision 1.5  1998/04/23 15:13:32
// Merge of NSKPORT branch (tag NSK_T4_980420) into FCS branch.
//
// Revision 1.2.16.1  1998/03/11 22:33:18
// merged FCS0310 and NSKPORT_971208
//
// Revision 1.4  1998/01/16 02:34:36
// merged LuShung's changes with the latest sort changes (from Ganesh).
//
// Revision 1.2  1997/04/23 00:29:06
// Merge of MDAM/Costing changes into SDK thread
//
// Revision 1.1.1.1.2.1  1997/04/11 23:23:08
// Checking in partially resolved conflicts from merge with MDAM/Costing
// thread. Final fixes, if needed, will follow later.
//
// Revision 1.3.4.1  1997/04/10 18:31:00
// *** empty log message ***
//
// Revision 1.1.1.1  1997/03/28 01:38:52
// These are the source files from SourceSafe.
//
//
// 7     3/06/97 4:54p Sql.lushung
// A fix for the memory delete problem is make in this version of sort.
// Revision 1.3  1997/01/14 03:22:17
//  Error handling and Statistics are implemented in this version of arksort.
//
// Revision 1.2  1996/12/11 22:53:39
// Change is made in arksort to allocate memory from executor's space.
// Memory leaks existed in arksort code are also fixed.
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

#include "Const.h"
#include "SortError.h"
#include "export/NABasicObject.h"

struct RunDirectoryEntry;

class RunDirectory : public NABasicObject {
 public:
  RunDirectory(short maxRuns, CollHeap *heap, SortError *sorterror);
  ~RunDirectory();

  int startNewRun(SBN);
  int getTotalNumOfRuns(void);
  void endCurrentRun(void);
  void sortRunDirectoryEntries(void);
  SBN mapRunNumberToFirstSBN(int runNumber);

 private:
  int numRunsGenerated_;
  int numRunsRemaining_;
  int currentMaxRuns_;
  int mergeOrder_;  // Number of runs being merged.

  RunDirectoryEntry *rdListPtr_;  // Pointer to an array of <n>
                                  // Run directory entries where
                                  // n is the same as currentMaxRuns_
  SortError *sortError_;
  CollHeap *heap_;
};

//----------------------------------------------------------------------
// This structure is used to store information about each individual
// run.
//----------------------------------------------------------------------
struct RunDirectoryEntry : public NABasicObject {
  int runNum_;
  SBN firstBlock_;
  int numBlocks_;
  int numRecs_;
  short startTime_[3];
  short endTime_[3];
  SortError *sorterror_;
};

#endif
