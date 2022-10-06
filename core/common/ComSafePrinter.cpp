
/* -*-C++-*-
******************************************************************************
*
* File:         ComSafePrinter.cpp
* Description:  Safe versions of some sprintf-style functions. They are
*               "safe" in the sense that they prevent writing beyond the
*               end of the target buffer. Some platforms, but not OSS,
*               provide similar functions in the stdio library.
*
* Created:      June 2003
* Language:     C++
*
*
*
******************************************************************************
*/

#include "ComSafePrinter.h"

#include <limits.h>

#define FILENO _fileno

THREAD_P FILE *ComSafePrinter::outfile_ = NULL;

ComSafePrinter::ComSafePrinter() {}

ComSafePrinter::~ComSafePrinter() {}

int ComSafePrinter::vsnPrintf(char *str, size_t n, const char *format, va_list args) {
  if (!str || n == 0 || !format) {
    return 0;
  }

  int targetLen = (n > INT_MAX ? INT_MAX : (int)n);
  int result = -1;

  // Otherwise we buffer data in a temporary file
  if (!outfile_) {
    outfile_ = tmpfile();
#ifdef _DEBUG
    if (!outfile_) {
      fprintf(stderr, "*** WARNING: ComSafePrinter temp file could not be created");
    }
#endif
  }

  if (outfile_) {
    rewind(outfile_);
    result = vfprintf(outfile_, format, args);
  }

  if (result > 0) {
    int totalLength = result;
    int numWritten = 0;

    if (totalLength < targetLen) {
      numWritten = vsprintf(str, format, args);
      if (numWritten == totalLength) {
        result = numWritten;
      } else {
        result = -1;
      }
    } else {
      rewind(outfile_);
      numWritten = (int)fread(str, sizeof(char), targetLen - 1, outfile_);
      if (numWritten == (targetLen - 1)) {
        str[targetLen - 1] = 0;
        result = numWritten;
      } else {
        result = -1;
      }
    }
  }

  return result;

}  // ComSafePrinter::vsnPrintf()

int ComSafePrinter::snPrintf(char *str, size_t n, const char *format, ...) {
  va_list args;
  va_start(args, format);
  int result = vsnPrintf(str, n, format, args);
  va_end(args);
  return result;
}
