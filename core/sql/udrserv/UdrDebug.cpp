
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         UdrDebug.cpp
 * Description:  debug functions for the UDR server
 *
 *
 * Created:      6/20/02
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */
#include "common/Platform.h"
#include "UdrFFDC.h"
#include "UdrDebug.h"

#ifdef UDR_DEBUG
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include "common/NABoolean.h"
FILE *udrDebugFile = stdout;

NABoolean doUdrDebug() {
  static NABoolean doUdrDebugFlag = FALSE;
  static NABoolean firstTime = TRUE;
  if (firstTime) {
    firstTime = FALSE;
    //
    // Note: the "UDR_DEBUG" string is split into two adjacent strings so
    // the preprocessor does not perform macro substitution on UDR_DEBUG.
    //
    if (getenv("UDR_"
               "DEBUG") ||
        getenv("MXUDR_"
               "DEBUG")) {
      doUdrDebugFlag = TRUE;
    }
  }
  return doUdrDebugFlag;
}

void udrDebug(const char *formatString, ...) {
  if (doUdrDebug()) {
    va_list args;
    va_start(args, formatString);
    fprintf(udrDebugFile, "[MXUDR] ");
    vfprintf(udrDebugFile, formatString, args);
    fprintf(udrDebugFile, "\n");
    fflush(udrDebugFile);
  }
}

#endif  // UDR_DEBUG
