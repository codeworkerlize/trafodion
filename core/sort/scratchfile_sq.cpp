

/* -*-C++-*-
******************************************************************************
*
* File:         scratchfile_sq.cpp
*
* Description:  This file contains the member function implementation for
*               class ScratchFile_sq. This class is used to encapsulate all
*               data and methods about a scratch file.
*
* Created:      01/02/2006
* Language:     C++
* Status:       $State: Exp $
*
*
*
*
******************************************************************************
*/
#include "sort/ScratchFile_sq.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#include "sort/ScratchSpace.h"
#include "executor/ExStats.h"
#include "executor/ex_stdh.h"

static long juliantimestamp_() {
  struct timeval tv;

  gettimeofday(&tv, NULL);
  long tUsec = (long)tv.tv_sec * (long)1000000 + (long)tv.tv_usec;
  return tUsec;
}

SQScratchFile::SQScratchFile(ScratchSpace *scratchSpace, SortError *sorterror, CollHeap *heap, NABoolean breakEnabled,
                             int scratchMaxOpens, NABoolean asynchReadQueue)
    : ScratchFile(scratchSpace, SCRATCH_FILE_SIZE, sorterror, heap, scratchMaxOpens, breakEnabled),
      vectorSize_(scratchSpace->getScratchIOVectorSize()),
      vector_(NULL) {
  int error = 0;
  resultFileSize_ = SCRATCH_FILE_SIZE;
  asynchReadQueue_ = asynchReadQueue;
  doDiskCheck_ = FALSE;

  // Inform overflow mode to STFS layer indicating that overflow
  // file be created on SSD or HDD or MMAP media.
  // Error 0 is success, 1 is unknown overflow type
  // error 2 is specified overflow type not configured.
  switch (scratchSpace_->getScratchOverflowMode()) {
    case SCRATCH_SSD:
      error = STFS_set_overflow(STFS_SSD);
      break;

    default:
    case SCRATCH_MMAP:
    case SCRATCH_DISK:
      error = STFS_set_overflow(STFS_HDD);
      break;
  }

  if (scratchSpace_->getScratchDirListSpec()) {
    // construct a ":" separated string and pass it to STFS to override
    // the environment settings done during installation for
    // STFS_HDD_LOCATION or STFS_SSD_LOCATION
    char *scratchDirString =
        (char *)((NAHeap *)heap)->allocateMemory(PATH_MAX * scratchSpace_->getNumDirsSpec(), FALSE);
    short i = 0;
    short nextindex = 0;
    for (i = 0; i < scratchSpace_->getNumDirsSpec(); i++) {
      if (nextindex > 0) scratchDirString[nextindex - 1] = ':';
      str_cpy_all(&scratchDirString[nextindex], (scratchSpace_->getScratchDirListSpec())[i].getDirName(),
                  (scratchSpace_->getScratchDirListSpec())[i].getDirNameLength());

      nextindex += (scratchSpace_->getScratchDirListSpec())[i].getDirNameLength() + 1;
    }
    scratchDirString[nextindex - 1] = '\0';
    STFS_set_scratch_dirs(scratchDirString);
    ((NAHeap *)heap)->deallocateMemory(scratchDirString);
  }

  // Error 0 is success, 1 is unknown overflow type
  // error 2 is specified overflow type not configured.
  if (error) {
    int errorType = EUnexpectErr;
    switch (error) {
      case 1:
        errorType = EUnKnownOvType;
        break;
      case 2:
        errorType = EOvTypeNotConfigured;
        break;
      default:
        errorType = EUnexpectErr;
    }
    sortError_->setErrorInfo(errorType  // sort error
                             ,
                             0  // syserr: the actual FS error
                             ,
                             0  // syserrdetail
                             ,
                             "SQScratchFile::SQScratchFile(2)"  // methodname
    );
    return;
  }

  // XXXXXX is required by mkstemp
  str_sprintf(fileName_, "SCRXXXXXX");

  for (int ii = 0; ii < 5; ii++) {
    if ((scratchSpace_->getScratchOverflowMode() == SCRATCH_DISK) ||
        (scratchSpace_->getScratchOverflowMode() == SCRATCH_MMAP)) {
      fileHandle_[0].fileNum = STFS_mkstemp_instnum(fileName_, scratchSpace_->getEspInstance(), doDiskCheck_);
    } else
      fileHandle_[0].fileNum = STFS_mkstemp(fileName_);
    if (fileHandle_[0].fileNum == STFS_NULL_FHNDL) {
      doDiskCheck_ = TRUE;
      continue;
    } else {
      doDiskCheck_ = FALSE;
      break;
    }
  }
  if (fileHandle_[0].fileNum == STFS_NULL_FHNDL) {
    // mkstemp failed.
    sortError_->setErrorInfo(EUnexpectErr  // sort error
                             ,
                             (short)errno  // syserr: the actual FS error
                             ,
                             0  // syserrdetail
                             ,
                             "SQScratchFile::SQScratchFile(3)"  // methodname
    );
    return;
  }
  fileNameLen_ = str_len(fileName_);

  if (scratchSpace_->getScratchOverflowMode() != SCRATCH_MMAP) {
    // change to non-blocking I/O
    int flags = STFS_fcntl(fileHandle_[0].fileNum, F_GETFL);

    // other flags
    flags |= O_APPEND;
    flags |= O_NOATIME;

    // O_DIRECT is ON by default. User has to explicitly define
    // SCRATCH_NO_ODIRECT to disable O_DIRECT. Note that this
    // flag has significant performance implications.
    if (!getenv("SCRATCH_NO_ODIRECT")) {
      flags |= O_DIRECT;

      // O_DIRECT requires 512 byte buffer boundary alignment.
      // bitwise AND with (512 -1) is same as mod 512 :)
      if ((scratchSpace_->getBlockSize() & 511) != 0) {
        STFS_close(fileHandle_[0].fileNum);
        fileHandle_[0].fileNum = STFS_NULL_FHNDL;
        sortError_->setErrorInfo(EBlockNotAligned  // sort error
                                 ,
                                 0  // syserr: the actual FS error
                                 ,
                                 0  // syserrdetail
                                 ,
                                 "SQScratchFile::SQScratchFile(4)"  // methodname
        );
        return;
      }
    }

    error = STFS_fcntl(fileHandle_[0].fileNum, F_SETFL, flags);

    if (error) {
      STFS_close(fileHandle_[0].fileNum);
      fileHandle_[0].fileNum = STFS_NULL_FHNDL;
      sortError_->setErrorInfo(EUnexpectErr  // sort error
                               ,
                               (short)error  // syserr: the actual FS error
                               ,
                               0  // syserrdetail
                               ,
                               "SQScratchFile::SQScratchFile(5)"  // methodname
      );
      return;
    }
  }

  if (scratchSpace_->getScratchOverflowMode() == SCRATCH_MMAP) {
    // Ensure all blocks are allocated for this file on disk. Any disk
    // space issues can be caught here.
    // Note errno is not set by this call. It returns error.
    error = posix_fallocate(fileHandle_[0].fileNum, 0, SCRATCH_FILE_SIZE);
    if (error != 0) {
      STFS_close(fileHandle_[0].fileNum);
      fileHandle_[0].fileNum = STFS_NULL_FHNDL;
      sortError_->setErrorInfo(EUnexpectErr  // sort error
                               ,
                               (short)error  // syserr: the actual FS error, not errno!

                               ,
                               0  // syserrdetail
                               ,
                               "SQScratchFile::SQScratchFile(6)"  // methodname
      );
      return;
    }

    // Now map the file.
    fileHandle_[0].mmap =
        (char *)mmap(0, resultFileSize_, PROT_READ | PROT_WRITE, MAP_SHARED, fileHandle_[0].fileNum, 0);

    if (fileHandle_[0].mmap == MAP_FAILED) {
      STFS_close(fileHandle_[0].fileNum);
      fileHandle_[0].fileNum = STFS_NULL_FHNDL;
      sortError_->setErrorInfo(EUnexpectErr  // sort error
                               ,
                               (short)errno  // syserr: the actual FS error

                               ,
                               0  // syserrdetail
                               ,
                               "SQScratchFile::SQScratchFile(7)"  // methodname
      );
      return;
    }
  }
  // Memory allocation failure will result in long jump.
  reset();
  if (vectorSize_ > 0) {
    vector_ = (struct iovec *)((NAHeap *)heap_)->allocateAlignedHeapMemory(sizeof(struct iovec) * vectorSize_, 512);
  }

  vectorWriteMax_ = 0;
  vectorReadMax_ = 0;
  writemmapCursor_ = 0;
  readmmapCursor_ = 0;

  // Initialize other members of the fileHandle structure.
  fileHandle_[0].IOBuffer = NULL;
  fileHandle_[0].bufferLen = 0;
  fileHandle_[0].blockNum = -1;
  fileHandle_[0].IOPending = FALSE;
  fileHandle_[0].scratchFileConn = NULL;
  fileHandle_[0].associatedAsyncIOBuffer = NULL;
  fileHandle_[0].previousError = SCRATCH_SUCCESS;

#ifdef _DEBUG
  // Regression testing support for simulating IO Pending state.
  envIOPending_ = getenv("SCRATCH_IO_PENDING");
  if (envIOPending_)
    envIOBlockCount_ = (int)str_atoi(envIOPending_, str_len(envIOPending_));
  else
    envIOBlockCount_ = -1;
#endif
}

//--------------------------------------------------------------------------
// The class destructor.
//--------------------------------------------------------------------------
SQScratchFile::~SQScratchFile() {
  short error;

  // Close the file
  if (fileHandle_[0].fileNum != STFS_NULL_FHNDL) {
    if (scratchSpace_->getScratchOverflowMode() == SCRATCH_MMAP) {
      munmap(fileHandle_[0].mmap, resultFileSize_);
    }
    STFS_close(fileHandle_[0].fileNum);
    STFS_unlink(fileName_);
  }
  if (vector_ != NULL) ((NAHeap *)heap_)->deallocateMemory(vector_);
}

NABoolean SQScratchFile::checkDirectory(char *path) {
  char cwd[1024 + 1];
  char *currentDir = getcwd(cwd, 1024);  // get current working directory
  if (currentDir == NULL) return SORT_FAILURE;
  int answerOfLife = chdir(path);
  chdir(currentDir);  // change current directory
  if (answerOfLife == 0) {
    return SORT_SUCCESS;
  }
  return SORT_FAILURE;
}

//--------------------------------------------------------------------------
// seekOffset
//--------------------------------------------------------------------------
RESULT SQScratchFile::seekOffset(int index, int offset, long &iowaittime, int *transfered, DWORD seekDirection) {
  // Assert that there is no pending IO on this file num.
  ex_assert(fileHandle_[index].IOPending == FALSE, "SQScratchFile::seekOffset, Pending IO on file handle");

  // when performing vector IO, there is no need to perform seek
  // for every vector element. We note down the beginning position
  // of the first vector element here and ignore rest of the seek
  // calls that build the vector.
  // Just before the actual vector read( write is handled by using
  // choosing O_APPEND option during file open) a lseek is performed.
  // In the case of mmap, O_APPEND is achieved by using a writeAppendCursor_.
  if (vectorIndex_ == 0)  // if first element of vector
  {
    // Note that type is set to PEND_READ/PEND_WRITE when first vector element
    // is registered. fileHandle_[index].IOPending is set only when
    // actual vector IO is pending.
    ex_assert(type_ == PEND_NONE, "SQScratchFile::seekOffset1, Write or read vector pending");

    vectorSeekOffset_ = offset;
  }

  return SCRATCH_SUCCESS;
}

// checkScratchIO will attempt Io completion if IO is pending.
// The return codes are restricted to SCRATCH_SUCCESS, IO_NOT_COMPLETE,
// IO_COMPLETE, SCRATCH_FAILURE and DISK_FULL only. Rest of the errors
// is returned as part of sortError structure. Layers above will handle
// the errors as appropriate.
RESULT SQScratchFile::checkScratchIO(int index, DWORD timeout, NABoolean initiateIO) {
  int error = 0;
  NABoolean retry = TRUE;
  RESULT result;

  if (fileHandle_[index].IOPending) {
    while (retry) {
      int count = 0;
      NABoolean readIO = type_ == PEND_READ;     // redriveIO resets type_ if IO completes
      error = redriveIO(index, count, timeout);  // similar to AWAITIOX redriveIO also resets IOPending
      retry = FALSE;

      if (error == 0) {
        numBytesTransfered_ = count;
        if (readIO && asynchReadQueue_) {
          result = processAsynchronousReadCompletion(index);
          if (result != SCRATCH_SUCCESS) {
            return result;
          }
          if (initiateIO) {
            long iowaittime = 0;
            result = serveAsynchronousReadQueue(iowaittime);
            if (result != SCRATCH_SUCCESS) {
              return result;
            }
          }
        }
        return IO_COMPLETE;
      }  // success scenario

      // error already contains errno. Interpret errno.
      switch (error) {
        case EAGAIN:
          return IO_NOT_COMPLETE;
          break;

        case EINTR:
          if (!breakEnabled()) retry = TRUE;
          break;

        // We need to restrict different error states to minimum
        // so that scratchSpace is kept simple. Returning FILE_FULL
        // is one expection being made to keep the interface similar
        // in behavior with NSK and NT versions.
        case ENOSPC:
          return FILE_FULL;
          break;

        default:
          sortError_->setErrorInfo(EAwaitioX  // sort error
                                   ,
                                   (short)error  // syserr: the actual FS error
                                   ,
                                   0  // syserrdetail
                                   ,
                                   "SQScratchFile::checkScratchIO"  // methodname
          );
          return SCRATCH_FAILURE;
      }  // switch
    }    // while retry
  }      // if
  return SCRATCH_SUCCESS;
}

int SQScratchFile::redriveIO(int index, int &count, int timeout) {
  int err = 0;
  RESULT result;
  int retval;

  switch (type_) {
    case PEND_READ:
    case PEND_WRITE:
      result = doSelect(index, timeout, type_, err);
      if (result == SCRATCH_SUCCESS) {
        err = redriveVectorIO(index);
        if (err < 0) err = obtainError();
      }
      break;

    case PEND_NONE:
    default:
      err = 0;  // Nothing to do.
  }

  if (err == 0)  // IO_COMPLETED
  {
    count = bytesCompleted_;
    reset();
  }

  // return errno or 0 to indicate success.
  return err;
}

RESULT SQScratchFile::doSelect(int index, DWORD timeout, EPendingIOType type, int &err) {
  if (scratchSpace_->getScratchOverflowMode() == SCRATCH_MMAP) {
    return SCRATCH_SUCCESS;
  }

  fd_set fds;
  struct timeval tv;
  int retval;

  long tLeft = LONG_MAX;
  if (timeout >= 0) tLeft = timeout * 10000;

  STFS_FH_ZERO(&fds);
  STFS_FH_SET(fileHandle_[index].fileNum, &fds);
  /* Wait up to five seconds. */

  long tBegin = 0;
  if (timeout == -1) {  // infinit wait
    switch (type) {
      case PEND_READ:
        retval = STFS_select(fileHandle_[index].fileNum + 1, &fds, NULL, NULL, NULL);
        break;

      default:
      case PEND_WRITE:
        retval = STFS_select(fileHandle_[index].fileNum + 1, NULL, &fds, NULL, NULL);
        break;
    }
  } else {
    tv.tv_sec = tLeft / 1000000;
    tv.tv_usec = tLeft % 1000000;
    tBegin = juliantimestamp_();
    switch (type) {
      case PEND_READ:
        retval = STFS_select(fileHandle_[index].fileNum + 1, &fds, NULL, NULL, &tv);
        break;

      default:
      case PEND_WRITE:
        retval = STFS_select(fileHandle_[index].fileNum + 1, NULL, &fds, NULL, &tv);
        break;
    }
  }
  if (retval == 0) {  // timeout, nothing available
    return IO_NOT_COMPLETE;
  }
  if (retval < 0)  // error
  {
    err = errno;
    return SCRATCH_FAILURE;
  }

  return SCRATCH_SUCCESS;
}

RESULT SQScratchFile::writeBlock(int index, char *data, int length, long &iowaittime, int blockNum, int *transfered,
                                 NABoolean waited) {
  ex_assert(vectorIndex_ < vectorSize_, "SQScratchFile::writeBlock, vectorIndex is out of bounds");

  // This assert is necessary to catch mixing of write and read operations before
  // Io is actually initiated/completed. The protocol is to do either read or write
  // operation at a time and not build vectors to write and read simulataneously.
  ex_assert(type_ != PEND_READ, "SQScratchFile::writeBlock, Write or read vector is getting mixed");

  vector_[vectorIndex_].iov_base = (void *)data;
  vector_[vectorIndex_].iov_len = length;
  bytesRequested_ += length;
  type_ = PEND_WRITE;
  if (vectorIndex_ == 0) blockNumFirstVectorElement_ = blockNum;

  vectorIndex_++;

  if (vectorIndex_ < vectorSize_) {
    // Note that we return SUCCESS by setting type_ to
    // TRUE. However fileHandle_[index].IOPending is not set to TRUE yet.
    // This helps in the following ways:
    // 1. Because fileHandle_[index].IOPending is FALSE, additional writeBlock
    //   calls are invoked by the caller.
    // 2. When vector is partially filled and the caller calls checkIO(checkALL)
    //   we can flush the partially filled vector. See ScratchSpace::CheckIO.
    return SCRATCH_SUCCESS;
  }

  // Reaching here means, vector is full and it is time to
  // initiate IO.

  if (fileHandle_[index].IOPending) {
    sortError_->setErrorInfo(EScrWrite  // sort error
                             ,
                             0  // syserr: the actual FS error
                             ,
                             0

                             ,
                             "SQScratchFile::writeBlock"  // methodname
    );
    return SCRATCH_FAILURE;
  }

  return executeVectorIO();
}

RESULT SQScratchFile::readBlock(int index, char *data, int length, long &iowaittime, int *transfered, int synchronous) {
  ex_assert(vectorIndex_ < vectorSize_, "SQScratchFile::readBlockV, vectorIndex is out of bounds");

  // This assert is necessary to catch mixing of write and read operations before
  // Io is actually initiated/completed. The protocol is to do either read or write
  // operation at a time and not build vectors to write and read simulataneously.
  ex_assert(type_ != PEND_WRITE, "SQScratchFile::readBlock, Write or read vector is getting mixed");

  vector_[vectorIndex_].iov_base = (void *)data;
  vector_[vectorIndex_].iov_len = length;
  bytesRequested_ += length;
  vectorIndex_++;
  type_ = PEND_READ;

  if (vectorIndex_ < vectorSize_) {
    // Note that we return SUCCESS by setting type_ to
    // TRUE. However fileHandle_[index].IOPending is not set to TRUE yet.
    // This helps in the following ways:
    // 1. Because fileHandle_[index].IOPending is FALSE, additional writeBlock
    //   calls are invoked by the caller.
    // 2. When vector is partially filled and the caller calls checkIO(checkALL)
    //   we can flush the partially filled vector. See ScratchSpace::CheckIO.
    return SCRATCH_SUCCESS;
  }

  // Reaching here means, vector is full and it is time to
  // initiate IO.
  if (fileHandle_[index].IOPending) {
    sortError_->setErrorInfo(EScrRead  // sort error
                             ,
                             0  // syserr: the actual FS error
                             ,
                             0

                             ,
                             "SQScratchFile::readBlock"  // methodname
    );
    return SCRATCH_FAILURE;
  }

  return executeVectorIO();
}

// Execute Vector is used both by readv and writev operations. This method
// is involved when ever a vector is full or when a vector is partially full.
// In the case of partially full, it is either driven by
// HashScratchSpace::checkIO(CheckAll) or when vector elements for read operation
// align only partially on the scratch file
//(see SQScratchFile::isNewVecElemPossible).
// The error codes returned by this method must be consistent with
// similar methods supported by NSK versions. Note that executeVectorIO is
// called from 4 clients , which are:
// 1. WriteBlockV
// 2. ReadBlockV
// 3. scratchSpace::checkIO(checkALL)
// 4. HashScratchSpace::checkIO()
// This method should return SCRATCH_SUCCESS if IO is initiated or
// Io completes. Note that if IO is initiated(IOPending flag is TRUE)
// this method does not return IO_NOT_COMPLETE. Is the job of checkScratchIO()
// to check if IO is completed or not and interpret the error code accordingly.
RESULT SQScratchFile::executeVectorIO() {
  // set this up so that we can redrive IO when checkScratchIO is called.
  remainingAddr_ = (void *)vector_;
  remainingVectorSize_ = vectorIndex_;
  bytesCompleted_ = 0;
  lastError_ = 0;

  if (type_ == PEND_READ) {
    if (scratchSpace_->getScratchOverflowMode() == SCRATCH_MMAP) {
      readmmapCursor_ = vectorSeekOffset_;
    } else {
      // seek to position.
      off_t ret = STFS_lseek(fileHandle_[0].fileNum, vectorSeekOffset_, SEEK_SET);

      // ret == -1 means error, otherwise new offset.
      if (ret < 0) {
        lastError_ = obtainError();
        sortError_->setErrorInfo(EPosition  // sort error
                                 ,
                                 lastError_  // syserr: the actual FS error
                                 ,
                                 0  // syserrdetail
                                 ,
                                 "SQScratchFile::executeVectorIO"  // methodname
        );

        return SCRATCH_FAILURE;
      }
    }
  }

  lastError_ = redriveVectorIO(0);

  // error is either negative or positive. If negative, it could be that partial buffers
  // are written and may require a redrive. If positive, IO is complete.
  if (lastError_ < 0) {
    lastError_ = obtainError();
    if (lastError_ != EAGAIN) {
      sortError_->setErrorInfo((type_ == PEND_READ) ? EScrRead : EScrWrite  // sort error
                               ,
                               lastError_  // syserr: the actual FS error
                               ,
                               0

                               ,
                               "SQScratchFile::executeVectorIO"  // methodname
      );
      return SCRATCH_FAILURE;
    }
    // if (error == EAGAIN), PEND_READ/PEND_WRITE and IOPending are already set TRUE
    // just return SCRATCH_SUCCESS. CheckScratchIo will redrive IO.
    return SCRATCH_SUCCESS;
  } else {
    numBytesTransfered_ = bytesCompleted_;
    if ((type_ == PEND_READ) && asynchReadQueue_) {
      lastError_ = processAsynchronousReadCompletion(0);
      if (lastError_) {
        return SCRATCH_FAILURE;
      }
    }
    // reset all counters since io completed.
    reset();
  }
  return SCRATCH_SUCCESS;
}

// Lowest level method that we have control before firing a read or write
// operation. The return values are typically interpretted by the caller.
// IOPending flags are pushed to this method so that IOPending flag is not
// set or reset at different places in the code.
int SQScratchFile::redriveVectorIO(int index) {
  ssize_t bytesCompleted = 0;
  ExBMOStats *bmoStats = scratchSpace_->bmoStats();
  // Other book keeping
  fileHandle_[index].IOPending = TRUE;

#ifdef _DEBUG
  // Regression testing support for simulating IO Pending state.
  if (envIOPending_ && bmoStats && (type_ == PEND_READ) && (envIOBlockCount_ == bmoStats->getScratchReadCount())) {
    envIOBlockCount_ = -1;  // avoid looping.
    bytesCompleted = -1;
    errno = EAGAIN;
    return -1;
  }
#endif

  if (scratchSpace_->getScratchOverflowMode() == SCRATCH_MMAP) {
    struct iovec *temp = (struct iovec *)remainingAddr_;
    try {
      while (remainingVectorSize_ > 0) {
        if (type_ == PEND_READ) {
          memcpy(temp->iov_base, (fileHandle_[index].mmap + readmmapCursor_), temp->iov_len);
          readmmapCursor_ += temp->iov_len;
          if (bmoStats) bmoStats->incScratchReadCount();
        } else {
          memcpy((fileHandle_[index].mmap + writemmapCursor_), temp->iov_base, temp->iov_len);
          writemmapCursor_ += temp->iov_len;
          if (bmoStats) bmoStats->incScratchWriteCount();
        }
        bytesCompleted_ = +temp->iov_len;
        remainingVectorSize_--;
        temp++;
      }                    // while
      remainingAddr_ = 0;  // Typically this address is invalid after reaching here.
    } catch (...) {
      // caught an unknown fatal exception.
      return -1;
    }
  } else {
    while (remainingVectorSize_ > 0) {
      if (type_ == PEND_READ) {
        if (bmoStats) bmoStats->getScratchIOTimer().start();
        bytesCompleted = readv(fileHandle_[index].fileNum, (struct iovec *)remainingAddr_, remainingVectorSize_);
        if (bmoStats) {
          bmoStats->incScratchReadCount();
          bmoStats->incScratchIOMaxTime(bmoStats->getScratchIOTimer().stop());
        }
      } else {
        if (bmoStats) bmoStats->getScratchIOTimer().start();
        bytesCompleted = writev(fileHandle_[index].fileNum, (struct iovec *)remainingAddr_, remainingVectorSize_);
        if (bmoStats) {
          bmoStats->incScratchWriteCount();
          bmoStats->incScratchIOMaxTime(bmoStats->getScratchIOTimer().stop());
        }
      }

      if (bytesCompleted == -1) {
        // This can happen at the very first call or subsequent interations.
        // note that remaningAddr and remainingVectorSize is already adjusted
        // in previous iterations if they were successful.
        return -1;
      }

      bytesCompleted_ = +bytesCompleted;

      // Now readjust the remaining counters by deducting bytesWritten
      while (bytesCompleted > 0) {
        struct iovec *temp = (struct iovec *)remainingAddr_;

        if (bytesCompleted >= temp->iov_len) {
          // advance to next vector element
          bytesCompleted -= temp->iov_len;
          remainingVectorSize_--;
          temp++;
          remainingAddr_ = (void *)temp;
        } else {
          // adjust the vector element
          temp->iov_len -= bytesCompleted;
          temp->iov_base = (void *)((char *)temp->iov_base + bytesCompleted);
          bytesCompleted = 0;
        }
      }  // while
    }    // while
  }      // not MMAP

  // If we reach here, then remainingVectorSize_ is zero. write/read operation is
  // fully complete.
  fileHandle_[index].IOPending = FALSE;
  return 0;
}

NABoolean SQScratchFile::isNewVecElemPossible(long byteOffset, int blockSize) {
  if (vectorIndex_ == 0)  // vector not populated
    return TRUE;
  else if (type_ = PEND_READ)  // vector populated for READ
    return (byteOffset == (vectorSeekOffset_ + (vectorIndex_ * blockSize)));
  else
    return TRUE;  // vector populated for write.
}

// Truncate file and cancel any pending I/O operation
void SQScratchFile::truncate(void) { ftruncate(fileHandle_[0].fileNum, 0); }

//--------------------------------------------------------------------------
// obtainError
// return the error of the last IO operation.
//--------------------------------------------------------------------------
short SQScratchFile::obtainError() { return errno; }

//--------------------------------------------------------------------------
// getError
// This function calls obtainError to get the actual error and then
// looks at the value to determine wether to return an "ERROR" value or
// assert(0) because the error is very bad.
//--------------------------------------------------------------------------
RESULT SQScratchFile::getError(int index) {
  //------------------------------------------------------------------------
  // call obtain error and then determine the right value to return.
  //------------------------------------------------------------------------
  int error = obtainError();
  switch (error) {
    case 0:
      return SCRATCH_SUCCESS;
    case 1:
    case 42:
      return READ_EOF;
    case 43:
      return DISK_FULL;
    case 45:
      return FILE_FULL;
    default:
      sortError_->setErrorInfo(EUnexpectErr  // sort error
                               ,
                               (short)error  // syserr: the actual FS error
                               ,
                               0  // syserrdetail
                               ,
                               "SQScratchFile::getError"  // methodname
      );
      // cerr << "Some very odd value! Error: " << error << endl;
      return OTHER_ERROR;
  }
}

void SQScratchFile::copyVectorElements(ScratchFile *newFile) {
  // cast it to SQScratchFile.
  SQScratchFile *nFile = static_cast<SQScratchFile *>(newFile);

  ex_assert((vectorIndex_ > 0) && (nFile->vectorIndex_ == 0),
            "SQScratchFile::copyVectorElements, source and target vector incompatible ");

  ex_assert(nFile->type_ == PEND_NONE, "SQScratchFile::copyVectorElements, target file is not new");

  nFile->type_ = PEND_WRITE;
  nFile->bytesRequested_ = bytesRequested_;
  nFile->blockNumFirstVectorElement_ = blockNumFirstVectorElement_;

  int count = nFile->vectorIndex_ = vectorIndex_;
  while (count > 0) {
    nFile->vector_[count - 1].iov_base = vector_[count - 1].iov_base;
    nFile->vector_[count - 1].iov_len = vector_[count - 1].iov_len;
    count--;
  }
}
