
#ifndef _LM_EXT_FUNC_H_
#define _LM_EXT_FUNC_H_
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         LmExtFunc.h
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
#include "common/NABoolean.h"
#include "common/Platform.h"

extern void lmMakeTFDSCall(const char *msg, const char *file, UInt32 line);
extern void lmPrintSignalHandlers();
extern NABoolean lmSetSignalHandlersToDefault();
extern NABoolean lmRestoreJavaSignalHandlers();
extern NABoolean lmRestoreUdrTrapSignalHandlers(NABoolean saveJavaSignalHandlers);

#endif
