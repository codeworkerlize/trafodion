
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         stringBuf.h
 * RCS:          $Id:
 * Description:  A simple buffer class for conversion routines
 *
 *
 * Created:      7/8/98
 * Modified:     $ $Date: 2006/11/01 01:38:09 $ (GMT)
 * Language:     C++
 * Status:       $State: Exp $
 *
 *
 *
 *
 *****************************************************************************
 */

#ifndef _STRING_BUF_H
#define _STRING_BUF_H

#if !defined(MODULE_DEBUG)
#include "common/BaseTypes.h"
#include "common/NAWinNT.h"
#include "common/Platform.h"
#endif

#ifdef _DEBUG
#include <iostream>
#endif  // _DEBUG

template <class T>
class stringBuf {
 public:
  stringBuf(T *buf, int len, CollHeap *heap = 0) : buf_(buf), bufSize_(len), count_(len), alloc_(FALSE), heap_(heap){};

  stringBuf(T *buf, int iBufSize, int iStrLen, CollHeap *heap = 0)
      : buf_(buf), bufSize_(iBufSize), count_(iStrLen <= iBufSize ? iStrLen : iBufSize), alloc_(FALSE), heap_(heap){};

  stringBuf(int len, CollHeap *heap = 0)
      : bufSize_(len),
        count_(len),  // should be set to 0, but keep the existing behavior for now
        alloc_(FALSE),
        heap_(heap) {
    if (len == 0)
      buf_ = 0;
    else {
      alloc_ = TRUE;
      buf_ = (heap) ? (new (heap) T[len]) : new T[len];
    }
    if (buf_ != NULL) {
      if (bufSize_ > 0) buf_[0] = 0;
    }
  };

  ~stringBuf() {
    if (alloc_) {
      if (heap_)
        NADELETEBASIC(buf_, heap_);
      else
        delete[] buf_;
    }
  };

  inline int getBufSize() const { return bufSize_; }

  inline int getStrLen() const { return count_; }  // same as length()

  inline int length() const { return count_; }

  inline void setStrLen(int x) { count_ = (x <= bufSize_) ? x : bufSize_; }  // avoid buffer overrun

  inline void setLength(int x) { count_ = x; }

  inline T *data() const { return buf_; }

  T operator()(int i) const { return buf_[i]; };

  T last() const { return buf_[getStrLen() - 1]; };

  void decrementAndNullTerminate() { /* if (count > 0) */
    buf_[--count_] = 0;
  }

  void zeroOutBuf(int startPos = 0) {
    if (startPos >= 0 && buf_ && bufSize_ > 0 && bufSize_ - startPos > 0) {
      memset((void *)&buf_[startPos], 0, (size_t)((bufSize_ - startPos) * sizeof(T)));
      count_ = startPos;
    }
  };

#ifdef _DEBUG
  ostream &print(ostream &out) {
    int i = 0;
    for (; i < count_; i++) out << int(buf_[i]) << " ";

    out << endl;
    for (i = 0; i < count_; i++) out << hex << int(buf_[i]) << " ";

    out << endl;
    return out;
  };
#endif  // _DEBUG

 private:
  T *buf_;
  int bufSize_;
  int count_;
  NABoolean alloc_;
  CollHeap *heap_;
};

// define the type of char and wchar buffer used in these
// conversion routines.
typedef stringBuf<NAWchar> NAWcharBuf;
typedef stringBuf<unsigned char> charBuf;

//
// Check and optionally allocate space for char or NAWchar typed buffer. The
// following comments apply to both the NAWchar and char version of
// checkSpace().
//
//  case 1: target buffer pointer 'target' is not NULL
//        if has enough space to hold SIZE chars, then 'target'
//        is returned; otherwise, NULL is returned.
//  case 2: target buffer pointer 'target' is NULL
//        a buffer of SIZE is allocated from the heap (if the heap argument
//        is not NULL), or from the C runtime system heap.
//  For either case, SIZE is defined as olen + (addNullAtEnd) ? 1 : 0.
//
//  If addNullAtEnd is TRUE, a NULL will be inserted at the beginning of 'target'
//  if it is not NULL.
//
NAWcharBuf *checkSpace(CollHeap *heap, int olen, NAWcharBuf *&target, NABoolean addNullAtEnd);

charBuf *checkSpace(CollHeap *heap, int olen, charBuf *&target, NABoolean addNullAtEnd);

#endif
