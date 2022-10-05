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
#ifndef SCRATCHFILE_SQ_H
#define SCRATCHFILE_SQ_H

/* -*-C++-*-
******************************************************************************
*
* File:         ScratchFile_sq.h
*
* Description:  This file contains the member function implementation for
*               class SQScratchFile. This class is used to encapsulate all
*               data and methods about a scratch file for SeaQuest.
*
* Created:      01/02/2006
* Language:     C++
* Status:       $State: Exp $
*
*
*
******************************************************************************
*/
#include "common/Platform.h"
#include "stfs/stfslib.h"
#include "ScratchFile_base.h"

typedef enum { PEND_NONE, PEND_READ, PEND_WRITE } EPendingIOType;

class SQScratchFile : public ScratchFile {
 public:
  SQScratchFile(ScratchSpace *scratchSpace, SortError *sorterror, CollHeap *heap = 0, NABoolean breakEnabled = FALSE,
                int scratchMaxOpens = 1, NABoolean asynchReadQueue = TRUE);

  //-----------------------------------------------
  // The destructor.
  //-----------------------------------------------
  virtual ~SQScratchFile(void);

  // This function returns the status of the
  // asynchronous function called right before it,
  // if any, or returns SCRATCH_SUCCESS otherwise.
  // When we say right before it we mean that
  // this function is the next public function of
  // this class called.
  //-----------------------------------------------
  virtual RESULT checkScratchIO(int index, DWORD timeout = 0, NABoolean initiateIO = FALSE);
  virtual void setEventHandler(ExSubtask *eh, IpcEnvironment *ipc, ex_tcb *tcb){};

  virtual RESULT getError(int index);
  //-----------------------------------------------
  // This function will return wether the current
  // position is at the end of the file or not.
  // If the file pointer is equal or greater to
  // the EOF, then this function returns SCRATCH_SUCCESS,
  // else it returns SCRATCH_FAILURE.
  //-----------------------------------------------
  virtual RESULT isEOF(int index, long &iowaittime, int *transfered = NULL) { return SCRATCH_FAILURE; }

  //-----------------------------------------------
  // This function reads length bytes and put it
  // in the char array, starting at the file
  // pointer.
  //-----------------------------------------------
  virtual RESULT readBlock(int index, char *data, int length, long &iowaittime, int *transfered = NULL,
                           int synchronous = 1);

  //-----------------------------------------------
  // This function moves the file pointer to the
  // end of the file.
  //-----------------------------------------------
  virtual RESULT seekEnd(int index, DWORD &eofAddr, long &iowaittime, int *transfered = NULL) {
    return SCRATCH_SUCCESS;
  }

  //-----------------------------------------------
  // This function moves the file pointer to the
  // desired offset from the start. Note that we use O_APPEND flag during file open.
  // For write operation, this essentially moves the file pointer automatically
  // to the end offset. Ofcourse we need seekoffset for read operations.
  //-----------------------------------------------
  virtual RESULT seekOffset(int index, int offset, long &iowaittime, int *transfered = NULL,
                            DWORD seekDirection = 0);

  //-----------------------------------------------
  // This function writes length bytes from the
  // char array into the file, starting from
  // the file pointer.
  //-----------------------------------------------
  virtual RESULT writeBlock(int index, char *data, int length, long &iowaittime, int blockNum = 0,
                            int *transfered = NULL, NABoolean waited = FALSE_L);

  virtual void setPreviousError(int index, RESULT error) { fileHandle_[index].previousError = error; }
  virtual RESULT getPreviousError(int index) { return fileHandle_[index].previousError; }
  // Truncate file and cancel any pending I/O operation
  virtual void truncate(void);
  virtual int getFreeFileHandle(void) {
    if (!fileHandle_[0].IOPending)
      return 0;  // means, a file handle is available for IO
    else
      return -1;  // means, no file handle is free for any new IO.
  }

  NABoolean checkDirectory(char *path);

  int getFileNum(int index) { return fileHandle_[index].fileNum; }

  virtual RESULT executeVectorIO();
  virtual NABoolean isVectorPartiallyFilledAndPending(int index) {
    return ((vectorIndex_ > 0) && (fileHandle_[index].IOPending == FALSE));
  }
  virtual NABoolean isNewVecElemPossible(long byteOffset, int blockSize);

  virtual void copyVectorElements(ScratchFile *newFile);
  virtual int getBlockNumFirstVectorElement() { return blockNumFirstVectorElement_; }

  virtual int lastError() { return lastError_; }

  virtual void reset() {
    vectorIndex_ = 0;
    bytesRequested_ = 0;
    bytesCompleted_ = 0;
    remainingAddr_ = NULL;
    remainingVectorSize_ = 0;
    vectorSeekOffset_ = 0;
    blockNumFirstVectorElement_ = -1;
    type_ = PEND_NONE;
    lastError_ = 0;
    doDiskCheck_ = FALSE;
  }

 private:
  // redriveIO will return if complete all bytes request or timeout occurred.
  int redriveIO(int index, int &count, int timeout = -1);  // AWAITIOX emulation.

  RESULT doSelect(int index, DWORD timeout, EPendingIOType type, int &err);
  int redriveVectorIO(int index);

  //-----------------------------------------------
  // This function is used internally to get the
  // actual error.
  //-----------------------------------------------
  short obtainError();

  //-----------------------------------------------
  // I have no idea what these do.
  //-----------------------------------------------
  int ioWaitTime_;

  // For vector IO
  struct iovec *vector_;  // vector elements
  int vectorSize_;      // max number of vector elements
  int vectorIndex_;     // number of vector elements setup
  ssize_t bytesRequested_;
  ssize_t bytesCompleted_;
  void *remainingAddr_;        // adjusting pointers for redrive IO
  int remainingVectorSize_;  // adjusting pointers for redrive IO
  long vectorSeekOffset_;     // beginning seek offset for vector IO
  long writemmapCursor_;      // Only used in mmap as append cursor.
  long readmmapCursor_;       // Only used in mmap as read cursor.

  // This is the block num of the block that corresponds to
  // first element in the vector. This is especially used
  // to recover from a ENOSPC. The entire vector is copied
  // to another instance of scratchFile and block num from this
  // member is updated in the map file.
  int blockNumFirstVectorElement_;

  // type_ can be any of enum types defined by enum. The following state changes
  // take place.  PEND_NONE-->PEND_READ or PEND_WRITE -->PEND_NONE
  // PEND_NONE:   Vector is empty, No read or write requests is registered.
  // PEND_WRITE:  Vector is registred with atleast one write request. No read
  //              requests can be registred now.
  // PEND_READ:   Vector is registred with atleast one read request. No write
  //              request can be registred now.
  // Notes:       Note that type_ just indicates if vector type. It does not
  //              indicate if an actual IO is in flight. To check if IO is in
  //              flight, check the fileHandle_[index].IOPending flag.
  EPendingIOType type_;   // indicates type of IO vector being builtup.
  int vectorWriteMax_;  // For diagnostics only
  int vectorReadMax_;   // For diagnostics only
  int lastError_;
  NABoolean doDiskCheck_;
#ifdef _DEBUG
  char *envIOPending_;     // for simulating IO pending state.
  int envIOBlockCount_;  // for simulating IO pending state.
#endif
};

#endif  // SCRATCHFILE_SQ_H
