
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         CliImplLmExtFunc.cpp
 * Description:  Functions needed by the language manager
 *               (derived from ../udrserv/UdrImplLmExtFunc.cpp)
 *
 * Created:      4/9/2015
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */
#include "langman/LmExtFunc.h"
#include "common/Platform.h"
#include <stdlib.h>

void lmMakeTFDSCall(const char *msg, const char *file, UInt32 line) {
  // makeTFDSCall(msg, file, line);
  abort();
}

void lmPrintSignalHandlers() {
  // printSignalHandlers();
}

NABoolean lmSetSignalHandlersToDefault() {
  // return setSignalHandlersToDefault();
  return TRUE;
}

NABoolean lmRestoreJavaSignalHandlers() {
  // return restoreJavaSignalHandlers();
  return TRUE;
}

NABoolean lmRestoreUdrTrapSignalHandlers(NABoolean saveJavaSignalHandlers) {
  // return restoreUdrTrapSignalHandlers(saveJavaSignalHandlers);
  return TRUE;
}
