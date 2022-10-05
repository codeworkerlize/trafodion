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
****************************************************************************
*
* File:         ComResourceInfo.h
* Description:  Compiled information on resources such as scratch files
*               (usable drive letters, placement, etc.)
*

* Created:      12/30/98
* Language:     C++
*
*
*
****************************************************************************
*/

#ifndef SCRATCH_FILES_H
#define SCRATCH_FILES_H

#include "common/Int64.h"
#include "export/NAVersionedObject.h"
#include "common/IpcMessageType.h"

// -----------------------------------------------------------------------
// Contents of this file
// -----------------------------------------------------------------------
class ExScratchFileOptions;

// -----------------------------------------------------------------------
// Forward references
// -----------------------------------------------------------------------

// -----------------------------------------------------------------------
// Constants for invalid cluster number
// -----------------------------------------------------------------------

const int RES_LOCAL_CLUSTER = -1;

// -----------------------------------------------------------------------
// An entry for a specific disk drive. This is not an NAVersionedObject
// because it hangs directly below an ExScratchFileOptions object which
// is versioned. Nevertheless, this object gets stored in module files.
// -----------------------------------------------------------------------
class ExScratchDiskDrive {
 public:
  ExScratchDiskDrive(char *dirName = NULL, int dirNameLen = 0) : dirName_(dirName), dirNameLength_(dirNameLen) {}

  inline const char *getDirName() const { return dirName_; }
  inline int getDirNameLength() const { return dirNameLength_; }
  inline void setDirName(char *s) { dirName_ = s; }
  inline void setDirNameLength(int s) { dirNameLength_ = s; }

  // Although this class is not derived from NAVersionedObject, it still
  // gets compiled as a dependent object of ExScratchFileOptions and it
  // still needs to be packed and unpacked.
  Long pack(void *space);
  int unpack(void *base, void *reallocator);

 private:
  // name of the directory
  NABasicPtr dirName_;
  // length of disk name
  int dirNameLength_;
  char fillersExScratchDiskDrive_[16];
};

// -----------------------------------------------------------------------
// Template instantiation to produce a 64-bit pointer emulator class
// for ExScratchDiskDrive and its dependents.
//
// An ExScratchDiskDrivePtr is a pointer to a contiguous array of
// ExScratchDiskDrive object, representing a list of disk drives.
// Such arrays get stored in the module file. They don't have a
// version by themselves, so their versioning is done via the
// parent object ExScratchFileOptions.
// -----------------------------------------------------------------------
typedef NAOpenObjectPtrTempl<ExScratchDiskDrive> ExScratchDiskDrivePtr;

// -----------------------------------------------------------------------
// Options for scratch files (generated by the compiler)
// -----------------------------------------------------------------------

class ExScratchFileOptions : public NAVersionedObject {
 public:
  ExScratchFileOptions() : NAVersionedObject(-1) { scratchFlags_ = 0; }

  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() { setImageVersionID(0, getClassVersionID()); }

  virtual short getClassSize() { return (short)sizeof(ExScratchFileOptions); }

  // The scratch file options get generated by the generator, so we need
  // pack and unpack procedures for it (they handle the entries as well)
  //
  Long pack(void *space);
  int unpack(void *, void *reallocator);

  void setSpecifiedScratchDirs(ExScratchDiskDrive *s, int numEntries) {
    specifiedScratchDirs_ = s;
    numSpecifiedDirs_ = numEntries;
  }

  void setScratchMgmtOption(int entry) {
    switch (entry) {
      case 5:
        scratchFlags_ |= SCRATCH_MGMT_OPTION_5;
        break;
      case 9:
        scratchFlags_ |= SCRATCH_MGMT_OPTION_9;
        break;
      case 11:
        scratchFlags_ |= SCRATCH_MGMT_OPTION_11;
        break;
      default:
        break;
    };
  }

  int getScratchMgmtOption(void) const {
    if ((scratchFlags_ & SCRATCH_MGMT_OPTION_5) != 0)
      return 5;
    else if ((scratchFlags_ & SCRATCH_MGMT_OPTION_9) != 0)
      return 9;
    else if ((scratchFlags_ & SCRATCH_MGMT_OPTION_11) != 0)
      return 11;
    else
      return 0;
  }

  void setScratchMaxOpensHash(int entry) {
    switch (entry) {
      case 2:
        scratchFlags_ |= SCRATCH_MAX_OPENS_HASH_2;
        break;
      case 3:
        scratchFlags_ |= SCRATCH_MAX_OPENS_HASH_3;
        break;
      case 4:
        scratchFlags_ |= SCRATCH_MAX_OPENS_HASH_4;
        break;
      default:
        break;
    };
  }

  int getScratchMaxOpensHash(void) const {
    if ((scratchFlags_ & SCRATCH_MAX_OPENS_HASH_2) != 0)
      return 2;
    else if ((scratchFlags_ & SCRATCH_MAX_OPENS_HASH_3) != 0)
      return 3;
    else if ((scratchFlags_ & SCRATCH_MAX_OPENS_HASH_4) != 0)
      return 4;
    else
      return 1;
  }

  void setScratchMaxOpensSort(int entry) {
    switch (entry) {
      case 2:
        scratchFlags_ |= SCRATCH_MAX_OPENS_SORT_2;
        break;
      case 3:
        scratchFlags_ |= SCRATCH_MAX_OPENS_SORT_3;
        break;
      case 4:
        scratchFlags_ |= SCRATCH_MAX_OPENS_SORT_4;
        break;
      default:
        break;
    };
  }

  int getScratchMaxOpensSort(void) const {
    if ((scratchFlags_ & SCRATCH_MAX_OPENS_SORT_2) != 0)
      return 2;
    else if ((scratchFlags_ & SCRATCH_MAX_OPENS_SORT_3) != 0)
      return 3;
    else if ((scratchFlags_ & SCRATCH_MAX_OPENS_SORT_4) != 0)
      return 4;
    else
      return 1;
  }

  void setScratchPreallocateExtents(NABoolean entry) {
    (entry ? scratchFlags_ |= SCRATCH_PREALLOCATE_EXTENTS : scratchFlags_ &= ~SCRATCH_PREALLOCATE_EXTENTS);
  }

  NABoolean getScratchPreallocateExtents(void) const { return (scratchFlags_ & SCRATCH_PREALLOCATE_EXTENTS) != 0; };

  void setScratchDiskLogging(NABoolean entry) {
    (entry ? scratchFlags_ |= SCRATCH_DISK_LOGGING : scratchFlags_ &= ~SCRATCH_DISK_LOGGING);
  }

  NABoolean getScratchDiskLogging(void) const { return (scratchFlags_ & SCRATCH_DISK_LOGGING) != 0; };

  inline const ExScratchDiskDrive *getSpecifiedScratchDirs() const { return specifiedScratchDirs_; }

  inline int getNumSpecifiedDirs() const { return numSpecifiedDirs_; }

  // to make the IPC methods happy, this object also supplies some
  // methods to help packing it into an IpcMessageObj
  int ipcPackedLength() const;
  int ipcGetTotalNameLength() const;
  int ipcPackObjIntoMessage(char *buffer) const;
  void ipcUnpackObj(int objSize, const char *buffer, CollHeap *heap, int totalNameLength,
                    char *&newBufferForDependents);

 private:
  enum scratchFlagsType {
    SCRATCH_MGMT_OPTION_5 = 0x0001,
    SCRATCH_MGMT_OPTION_9 = 0x0002,
    SCRATCH_PREALLOCATE_EXTENTS = 0x0004,
    SCRATCH_MAX_OPENS_HASH_2 = 0x0008,
    SCRATCH_MAX_OPENS_HASH_3 = 0x0010,
    SCRATCH_MAX_OPENS_HASH_4 = 0x0020,
    SCRATCH_MAX_OPENS_SORT_2 = 0x0040,
    SCRATCH_MAX_OPENS_SORT_3 = 0x0080,
    SCRATCH_MAX_OPENS_SORT_4 = 0x0100,
    SCRATCH_MGMT_OPTION_11 = 0x0200,
    SCRATCH_DISK_LOGGING = 0x0400
  };

  // a list of disks (for different nodes in the cluster) to
  // be used (or an empty string if the system can decide)
  ExScratchDiskDrivePtr specifiedScratchDirs_;

  int numSpecifiedDirs_;
  UInt32 scratchFlags_;
  char fillersExScratchFileOptions[24];
};

// -----------------------------------------------------------------------
// Template instantiation to produce a 64-bit pointer emulator class
// for ExScratchFileOptions and its dependents
// -----------------------------------------------------------------------
typedef NAVersionedObjectPtrTempl<ExScratchFileOptions> ExScratchFileOptionsPtr;

#endif /* SCRATCH_FILES_H */
