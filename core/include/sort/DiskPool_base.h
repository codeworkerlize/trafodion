/******************************************************************************
*
* File:         DiskPool_base.h
*
* Description:  This file contains the member function implementation for
*               class DiskPool. This class is used to encapsulate all
*               data and methods about a scratch file.
*
* Created:      01/02/2007
* Language:     C++
* Status:       Re-write to move platform dependent implemenations out of this
* 				base file.
*
*

*
*
******************************************************************************
*/

#ifndef DISKPOOL_BASE_H
#define DISKPOOL_BASE_H

#include <ctype.h>
#include <limits.h>
#include <string.h>

#include <iostream>

#include "sort/CommonStructs.h"
#include "CommonUtil.h"
#include "sort/Const.h"
#include "sort/SortError.h"
#include "SortUtilCfg.h"
#include "common/Platform.h"
#include "export/NABasicObject.h"

//--------------------------------------------------------------------------
//  This is for including the right list header when compileing.
//--------------------------------------------------------------------------
#ifdef USERW
#include <rw/gslist.h>
#else
#include "List.h"
#endif

#ifdef FORDEBUG
#include <iomanip>
#endif

// Each platform must define its own DiskDetails, which is a subclass of NABasicObject
struct DiskDetails;
class ScratchSpace;

class DiskPool : public NABasicObject {
 public:
  DiskPool(CollHeap *heap);
  virtual ~DiskPool() = 0;  // pure virtual

  virtual NABoolean generateDiskTable(const ExScratchDiskDrive *scratchDiskSpecified, int numSpecified,
                                      char *volumeNameMask, answer including = ::right,
                                      NABoolean includeAuditTrailDisks = FALSE) = 0;

  virtual NABoolean returnBestDisk(char **diskname, int espInstance, int numEsps, unsigned short threshold) = 0;

  DiskDetails **getDiskTablePtr() const { return diskTablePtr_; };
  DiskDetails **getLocalDisksPtr() const { return localDisksPtr_; };
  int getNumberOfDisks() { return numberOfDisks_; };
  int getNumberOfLocalDisks() { return numberOfLocalDisks_; };
  void setScratchSpace(ScratchSpace *scratchSpace) { scratchSpace_ = scratchSpace; }

#ifdef FORDEBUG
  virtual NABoolean printDiskTable() = 0;
#endif

 protected:
  virtual void assignWeight(DiskDetails *diskPtr) = 0;

  virtual NABoolean refreshDisk(DiskDetails *diskPtr) = 0;
  virtual NABoolean computeNumScratchFiles(DiskDetails *diskPtr) = 0;
  // index into the diskdetails pointer
  int numberOfDisks_;
  int numberOfLocalDisks_;

  DiskDetails **diskTablePtr_;   // This is a pointer to an array
                                 // of <n> pointers to structures
                                 // of the type DiskDetails.
  DiskDetails **localDisksPtr_;  // Pointer to array of local disks
                                 // pointers

  short currentDisk_;  // This feild is deprecated on NSK.
  ScratchSpace *scratchSpace_;
  CollHeap *heap_;
};

typedef DiskDetails *DiskDetailsPtr;
const int longMaxInPageUnits = 1048576;  // INT_MAX/2048

#endif  // DISKPOOL_BASE_H
