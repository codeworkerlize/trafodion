
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         UdrImplLmExtFunc.cpp
 * Description:  Functins needed by the language manager
 *
 *
 * Created:      5/4/02
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */
#include "LmExtFunc.h"
#include "UdrFFDC.h"
#include "common/Platform.h"

void lmMakeTFDSCall(const char *msg, const char *file, UInt32 line) { makeTFDSCall(msg, file, line); }

void lmPrintSignalHandlers() { printSignalHandlers(); }

NABoolean lmSetSignalHandlersToDefault() { return setSignalHandlersToDefault(); }

NABoolean lmRestoreJavaSignalHandlers() { return restoreJavaSignalHandlers(); }

NABoolean lmRestoreUdrTrapSignalHandlers(NABoolean saveJavaSignalHandlers) {
  return restoreUdrTrapSignalHandlers(saveJavaSignalHandlers);
}
