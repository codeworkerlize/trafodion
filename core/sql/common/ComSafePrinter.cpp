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
