
#ifndef COMSAFEPRINTER_H
#define COMSAFEPRINTER_H
/* -*-C++-*-
******************************************************************************
*
* File:         ComSafePrinter.h
* Description:  Safe versions of some sprintf-style functions. They are
*               "safe" in the sense that they prevent writing beyond the
*               end of the target buffer. Some platforms, but not OSS,
*               provide similar functions in the stdio library.
*
* Created:      June 2003
* Language:     C++
*
*
******************************************************************************
*/

#include <stdarg.h>
#include <stdio.h>

#include "common/Platform.h"

class ComSafePrinter {
 public:
  ComSafePrinter();
  ~ComSafePrinter();
  int snPrintf(char *, size_t, const char *, ...);
  int vsnPrintf(char *, size_t, const char *, va_list);

 protected:
  // We maintain a global temporary FILE for the purpose of buffering
  // sprintf output. We can safely write to this FILE without worrying
  // about buffer overflow, then read a fixed number of bytes from the
  // FILE back into memory. Don't konw for sure, but chances are that
  // when our output strings are small, no I/O will actually be
  // performed because of the stdio library's buffering of FILE data.
  static THREAD_P FILE *outfile_;

 private:
  // Do not implement default constructors or an assignment operator
  ComSafePrinter(const ComSafePrinter &);
  ComSafePrinter &operator=(const ComSafePrinter &);

};  // class ComSafePrinter

#endif  // COMSAFEPRINTER_H
