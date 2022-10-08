// **********************************************************************

// **********************************************************************

#include "common/ComMemLog.h"

#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

ComMemLog::ComMemLog() : startPtr(0), maxLength(0), currLength(0), loopbackNm(-1), isRead(true) {
  fileName[0] = 0;
  fileHandle = -1;
}

void ComMemLog::buildPath(long pid) {
  int leftLen = NAME_MAX - 1;
  int realLen = snprintf(fileName, leftLen, "/traf_memlog_%ld", pid);
  if (realLen >= leftLen) {
    // log, never happen.
  }

  return;
}

void ComMemLog::setSize() {
  const char *tml = getenv("TRAF_MEMLOG_SIZE");
  if (tml == NULL) {
    maxLength = 0;
  } else {
    maxLength = atol(tml);
  }

  if (maxLength == 0) {
    return;
  } else if (maxLength < DEFAULT_MEMLOG_SIZE) {
    maxLength = DEFAULT_MEMLOG_SIZE;
  }

  return;
}

int ComMemLog::doMap() {
  int status;
  int oflag = 0, prot = 0;
  if (!isRead) {
    oflag = O_CREAT | O_RDWR | O_TRUNC;
    prot = PROT_READ | PROT_WRITE;
  } else {
    oflag = O_RDONLY;
    prot = PROT_READ;
  }

  fileHandle = shm_open(fileName, oflag, S_IRUSR | S_IWUSR);
  if (fileHandle < 0) {
    status = errno;
    // log ?
    return status;
  }

  if (!isRead) {
    if (ftruncate(fileHandle, maxLength) < 0) {
      status = errno;
      // log ?
      return status;
    }
  }

  startPtr = (char *)mmap(NULL, maxLength, prot, MAP_SHARED, fileHandle, 0);
  if (startPtr == MAP_FAILED) {
    startPtr = 0;
    status = errno;
    // log ?
    return status;
  }
  // for string end, keep pace with deinitialize.
  maxLength -= 1;

  return 0;
}

void ComMemLog::initForWrite() {
  loopback();
  return;
}

void ComMemLog::initForRead() {
  long *ptr = (long *)startPtr;
  currLength = *ptr;
  loopbackNm = *(ptr + 1);

  return;
}

// success, return 0; failed, return errno.
int ComMemLog::initialize(long pid, bool read) {
  isRead = read;

  // the init has been done
  if (startPtr != 0) return 0;

  setSize();
  // disable memlog
  if (maxLength <= 0) {
    return 0;
  }

  buildPath(pid ? pid : (long)(getpid()));

  int status = doMap();
  if (status != 0) {
    return status;
  }

  if (isRead) {
    initForRead();
  } else {
    initForWrite();
  }

  return 0;
}

void ComMemLog::updateCurrentLength(long len) {
  currLength += len + 1;
  *(long *)startPtr = currLength;

  return;
}

void ComMemLog::loopback() {
  currLength = sizeof(currLength) + sizeof(loopbackNm);
  long *ptr = (long *)startPtr;

  loopbackNm += 1;
  *ptr = currLength;
  *(ptr + 1) = loopbackNm;

  return;
}

void ComMemLog::memlog(const char *format, ...) {
  if (startPtr == 0) {
    return;
  }

  va_list args;
  va_list args1;
  va_start(args, format);
  va_copy(args1, args);

  long leftLen = maxLength - currLength;
  long realLen = vsnprintf(startPtr + currLength, leftLen, format, args);
  if (realLen > maxLength) {
    // do nothing
  } else {
    if (realLen >= leftLen) {
      loopback();
      leftLen = maxLength - currLength;
      realLen = vsnprintf(startPtr + currLength, leftLen, format, args1);
    }
    updateCurrentLength(realLen);
  }

  va_end(args1);
  va_end(args);
  return;
}

void ComMemLog::memshow() {
  if (startPtr == 0) {
    printf("The memlog is disabled!\n");
    return;
  }

  printf("The Memery Log: %ld\n\n", loopbackNm);

  const char *end = (char *)startPtr + maxLength + 1;
  const char *ptr = (char *)startPtr + currLength;

  // skip incomplete info
  while ((ptr < end) && (*ptr != 0)) {
    ++ptr;
  }

  // print prev loop info
  ++ptr;
  while (ptr < end) {
    long len = strlen(ptr);
    if (len != 0) {
      printf("%s\n", ptr);
      ptr += len + 1;
    } else {
      break;
    }
  }

  // print current loop info
  end = (char *)startPtr + currLength;
  ptr = (char *)startPtr + sizeof(currLength) + sizeof(loopbackNm);
  while (ptr < end) {
    long len = strlen(ptr);
    if (len != 0) {
      printf("%s\n", ptr);
      ptr += len + 1;
    } else {
      break;
    }
  }

  return;
}

void ComMemLog::deinitialize() {
  if (startPtr == 0) {
    return;
  }

  munmap(startPtr, maxLength + 1);
  close(fileHandle);
  if (!isRead) {
    shm_unlink(fileName);
  }

  return;
}

ComMemLog &ComMemLog::instance() {
  static ComMemLog singleOne;
  return singleOne;
}
