
// diskpool.h -*-C++-*-
#ifndef DISKPOOL_SQ_H
#define DISKPOOL_SQ_H

#include "DiskPool_base.h"
#include "common/Platform.h"

class SQDisk : public DiskPool {
 public:
  SQDisk(SortError *sorterror, CollHeap *heap);
  ~SQDisk();

  virtual

      NABoolean
      generateDiskTable(const ExScratchDiskDrive *scratchDiskSpecified, int numSpecified, char *volumeNameMask,
                        answer including = ::right, NABoolean includeAuditTrailDisks = FALSE);

  virtual NABoolean returnBestDisk(char **diskname, int espInstance, int numEsps, unsigned short threshold);

#ifdef FORDEBUG
  virtual NABoolean printDiskTable() { return TRUE; };
#endif

 private:
  virtual void assignWeight(DiskDetails *diskPtr){};
  virtual NABoolean refreshDisk(DiskDetails *diskPtr) { return TRUE; };
  virtual NABoolean computeNumScratchFiles(DiskDetails *diskptr) { return TRUE; };

  short factorImportanceTotalFreeSpace_;
  short factorImportanceNumScrFiles_;
  SortError *sortError_;
};

//--------------------------------------------------------------------------
//  DiskDetails varies with the implementation so the DiskDetail for NSK
//  is different then the DiskDetail for NT.
//  The ??? means that I have not been able to find a way to retrieve
//  this particular information.
//--------------------------------------------------------------------------
//----------------------------------------------------------------------
// This structure is used to store information about each physical disk
// that are part of the diskpool.
//----------------------------------------------------------------------

enum SQDiskType { SQDT_UNKNOWN, SQDT_DEFAULT };

struct DiskDetails : public NABasicObject {
  // common fields between platforms
  int weight_;
  int freeSpace_;
  short numOpenScratchFiles_;
  //----------------------------------------------
  //  Since we are doing this under NT,
  //  we will use the Win32 data types
  //  to appease the NT gods.
  //----------------------------------------------

  char rootPathName_[PATH_MAX];
  char tempPathName_[PATH_MAX];

  SQDiskType diskType_;
};
#endif
