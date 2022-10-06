/* -*-C++-*-
 *****************************************************************************
 *
 * File:         LmDebug.cpp
 * Description:  debug functions of the language manager
 *
 *
 * Created:      6/20/02
 * Language:     C++
 *
 *

**********************************************************************/
#include "LmDebug.h"

#include "LmExtFunc.h"
#include "common/Platform.h"

#ifdef LM_DEBUG
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>

#include "common/NABoolean.h"
FILE *lmDebugFile = stdout;

NABoolean doLmDebug() {
  static NABoolean doLmDebugFlag = FALSE;
  static NABoolean firstTime = TRUE;
  if (firstTime) {
    firstTime = FALSE;

    // Note: the "LM_DEBUG" string is split into two adjacent strings so
    // the preprocessor does not perform macro substitution on LM_DEBUG.
    if (getenv("LM_"
               "DEBUG"))
      doLmDebugFlag = TRUE;

    char *debug_log = getenv("LM_DEBUG_LOG");
    if (debug_log != NULL) {
      // Exclude coverage, used for debugging
      lmDebugFile = fopen(debug_log, "a");
      if (!lmDebugFile) lmDebugFile = stdout;
    }
  }

  return doLmDebugFlag;
}

void lmDebug(const char *formatString, ...) {
  if (doLmDebug()) {
    // Exclude coverage, used for debugging
    va_list args;
    va_start(args, formatString);
    fprintf(lmDebugFile, "[LM DEBUG] ");
    vfprintf(lmDebugFile, formatString, args);
    fprintf(lmDebugFile, "\n");
    fflush(lmDebugFile);
  }
}

NABoolean doLmDebugSignalHandlers() {
  static NABoolean doLmDebugSignalHandlersFlag = FALSE;
  static NABoolean firstTime = TRUE;
  if (firstTime) {
    firstTime = FALSE;
    //
    // Note: the "LM_DEBUG" string is split into two adjacent strings so
    // the preprocessor does not perform macro substitution on LM_DEBUG.
    //
    if (getenv("LM_DEBUG_SIGNAL_HANDLERS")) {
      doLmDebugSignalHandlersFlag = TRUE;
    }
  }
  return doLmDebugSignalHandlersFlag;
}

void debugLmSignalHandlers() {
  if (doLmDebugSignalHandlers()) lmPrintSignalHandlers();
}

// Exclude the following function for coverage as this is used only for debugging signal handler
NABoolean doNotRestoreSignalHandlersAfterUDF() {
  static NABoolean doNotRestoreSignalHandlersAfterUDFFlag = FALSE;
  static NABoolean firstTime = TRUE;
  if (firstTime) {
    firstTime = FALSE;
    if (getenv("LM_DO_NOT_ATTEMPT_RESTORE_SIGNAL_HANDLERS_AFTER_UDF")) {
      doNotRestoreSignalHandlersAfterUDFFlag = TRUE;
    }
  }
  return doNotRestoreSignalHandlersAfterUDFFlag;
}

#endif  // LM_DEBUG
