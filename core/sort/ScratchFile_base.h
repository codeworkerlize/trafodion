
#ifndef SCRATCHFILE_BASE_H
#define SCRATCHFILE_BASE_H

/******************************************************************************
 *
 * File:         DiskPool_base.cpp
 *
 * Description:  This file contains the member function implementation for
 *               class ScratchFile. This class is used to encapsulate all
 *               data and methods about a scratch file.
 *
 *
 ******************************************************************************
 */

#include "common/Platform.h"
// 64-bit
// NOTE: We must use "unsigned long" here (even though it violates our rule
//       of not using "long" explicitly) because  DWORD may already be
//       defined as "unsigned long" by a system header file.

#include "CommonStructs.h"
#include "Const.h"
#include "CommonUtil.h"
#include "export/NABasicObject.h"
#include "common/Int64.h"
#include "SortError.h"
#include "ScratchFileConnection.h"
#include "stfs/stfslib.h"

class IpcEnvironment;
class ScratchFileConnection;
class ex_tcb;

// forward references
class ScratchFile;
class ScratchSpace;
class ExSubtask;

enum AsyncIOBufferState { QUEUED = 0, READING, READCOMPLETE, BEING_CONSUMED, IDLE };

class AsyncIOBuffer : public NABasicObject {
 public:
  AsyncIOBuffer(void) { reset(); };
  ~AsyncIOBuffer(void){};
  RESULT retval_;
  AsyncIOBuffer *next_;  // linked into scratchFile's asyncReadQueue.
  char *scrBlock_;
  ScratchFile *currentScrFile_;  // scratch file associated when IO pending, else NULL
  enum AsyncIOBufferState state_;
  int currentIOByteOffset_;  // seekoffset into currentScrFile_ for IO

  // Not used on NSK
  int currentIOByteLength_;  // expected IO size read
  int tag_;

  void reset(void) {
    state_ = IDLE;
    currentScrFile_ = NULL;
    currentIOByteOffset_ = 0;
    currentIOByteLength_ = 0;
    tag_ = 0;
    next_ = NULL;
    retval_ = SCRATCH_SUCCESS;
  }

  virtual void processMisc(void) = 0;

  AsyncIOBufferState state(void) { return state_; }

 protected:
 private:
};

//--------------------------------------------------------------------------
// This is a virtual base class used to basically enforce similar
// functionality between the different classes that inherit from this
// class.  However, this only enforce function prototype, so pay attention
// to all the different classes that inherit from this class and make them
// functionaly the same in the functions below.  Read the comments on the
// NSK and NT versions to see how other verions should behave.
//--------------------------------------------------------------------------
class ScratchFile : public NABasicObject {
  struct FileHandle1 {
    int bufferLen;
    int blockNum;
    char *IOBuffer;
    ScratchFileConnection *scratchFileConn;
    AsyncIOBuffer *associatedAsyncIOBuffer;
    stfs_fhndl_t fileNum;
    NABoolean IOPending;
    RESULT previousError;  // This will be set if an error with I/O on this
                           // scratch file is encountered at the IPC connection layer
    char *mmap;
  };

 public:
  ScratchFile(ScratchSpace *scratchSpace, long fileSize, SortError *sorterror, CollHeap *heap, int numOpens = 1,
              NABoolean breakEnabled = FALSE);
  ScratchFile(NABoolean breakEnabled);
  virtual ~ScratchFile();

  virtual RESULT checkScratchIO(int index, DWORD timeout = 0, NABoolean initiateIO = FALSE) = 0;
  virtual void setEventHandler(ExSubtask *eh, IpcEnvironment *ipc, ex_tcb *tcb) = 0;
  virtual RESULT getError(int index) { return SCRATCH_FAILURE; };
  virtual RESULT isEOF(int index, long &iowaittime, int *transfered = NULL) = 0;
  virtual RESULT readBlock(int index, char *data, int length, long &iowaittime, int *transfered = NULL,
                           int synchronous = 1) = 0;
  virtual RESULT queueReadRequestAndServe(AsyncIOBuffer *ab, long &ioWaitTime);
  virtual RESULT seekEnd(int index, DWORD &eofAddr, long &iowaittime, int *transfered = NULL) = 0;
  virtual RESULT seekOffset(int index, int offset, long &iowaittime, int *transfered = NULL,
                            DWORD seekDirection = 0 /* FILE_BEGIN */) = 0;
  virtual RESULT writeBlock(int index, char *data, int length, long &iowaittime, int blockNum = 0,
                            int *transfered = NULL, NABoolean waited = FALSE_L) = 0;
  virtual RESULT serveAsynchronousReadQueue(long &ioWaitTime, NABoolean onlyIfFreeHandles = FALSE,
                                            NABoolean waited = FALSE);
  virtual RESULT processAsynchronousReadCompletion(int index);
  virtual RESULT completeSomeAsynchronousReadIO(DWORD timeout, int &eventIndex, long &ioWaitTime) {
    return SCRATCH_FAILURE;
  }
  virtual RESULT completeAsynchronousReadIO(int eventIndex, long &ioWaitTime) { return SCRATCH_FAILURE; }
  // Truncate file and cancel any pending I/O operation
  virtual void truncate(void) = 0;

  int getNumOfReads() { return numOfReads_; }
  int getNumOfWrites() { return numOfWrites_; }
  int getNumOfAwaitio() { return numOfAwaitio_; }

  virtual int getFreeFileHandle(void) = 0;
  virtual NABoolean isAnyIOPending(void) { return FALSE; }
  void queueAsynchronousRead(AsyncIOBuffer *ab);
  AsyncIOBuffer *dequeueAsynchronousRead(void);

  SortError *getSortError() const { return sortError_; };
  NABoolean asynchronousReadQueueHasEntries(void) { return asynchronousReadQueueHead_ != NULL; };
  virtual void setPreviousError(int index, RESULT error) = 0;
  virtual RESULT getPreviousError(int index) = 0;
  int getNumOpens(void) { return numOpens_; }

  virtual RESULT executeVectorIO() { return SCRATCH_FAILURE; };
  virtual NABoolean isVectorIOPending(int index) { return FALSE; }
  virtual NABoolean isNewVecElemPossible(long byteOffset, int blockSize) { return FALSE; }
  virtual NABoolean isVectorPartiallyFilledAndPending(int index) { return FALSE; }
  virtual void copyVectorElements(ScratchFile *newFile){};
  virtual int getBlockNumFirstVectorElement() { return -1; };
  virtual void reset() {}
  virtual int lastError() { return -1; }

  NABoolean breakEnabled() { return breakEnabled_; };
  long &bytesWritten() { return bytesWritten_; };
  long resultFileSize() { return resultFileSize_; }
  FileHandle1 fileHandle_[MAX_SCRATCH_FILE_OPENS];
  NABoolean diskError() { return diskError_; };

 protected:
  char fileName_[STFS_PATH_MAX];
  int fileNameLen_;
  ScratchSpace *scratchSpace_;
  SortError *sortError_;
  CollHeap *heap_;
  int numReadsPending_;
  int numBytesTransfered_;  // keep track of count returned by AWAITIOX
  int numOfReads_;
  int numOfWrites_;
  int numOfAwaitio_;
  long bytesWritten_;
  int primaryExtentSize_;
  int secondaryExtentSize_;
  int maxExtents_;
  long resultFileSize_;
  int numOpens_;             // Indicates number of opens on file.
  NABoolean asynchReadQueue_;  // indicates if the user needs asynchIObuffer use

 private:
  // queue is empty if and only if both of the following members are
  // NULL.
  AsyncIOBuffer *asynchronousReadQueueHead_;
  AsyncIOBuffer *asynchronousReadQueueTail_;
  NABoolean breakEnabled_;
  NABoolean diskError_;
};
#endif  // SCRATCHFILE_BASE_H
