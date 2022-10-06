
#ifndef _LM_DEBUG_H_
#define _LM_DEBUG_H_
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         LmDebug.h
 * Description:  debug functions of the language manager
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

#include "common/NABoolean.h"
#include "common/Platform.h"

#ifdef LM_DEBUG
#include "common/NABoolean.h"
NABoolean doLmDebug();
void lmDebug(const char *, ...);
#define LM_DEBUG0(msg)                     lmDebug((msg))
#define LM_DEBUG1(msg, a1)                 lmDebug((msg), (a1))
#define LM_DEBUG2(msg, a1, a2)             lmDebug((msg), (a1), (a2))
#define LM_DEBUG3(msg, a1, a2, a3)         lmDebug((msg), (a1), (a2), (a3))
#define LM_DEBUG4(msg, a1, a2, a3, a4)     lmDebug((msg), (a1), (a2), (a3), (a4))
#define LM_DEBUG5(msg, a1, a2, a3, a4, a5) lmDebug((msg), (a1), (a2), (a3), (a4), (a5))
#else
#define LM_DEBUG0(msg)
#define LM_DEBUG1(msg, a1)
#define LM_DEBUG2(msg, a1, a2)
#define LM_DEBUG3(msg, a1, a2, a3)
#define LM_DEBUG4(msg, a1, a2, a3, a4)
#define LM_DEBUG5(msg, a1, a2, a3, a4, a5)
#endif

#if defined(LM_DEBUG)
void debugLmSignalHandlers();
#define LM_DEBUG_SIGNAL_HANDLERS(msg) \
  lmDebug(msg);                       \
  debugLmSignalHandlers()
#else
#define LM_DEBUG_SIGNAL_HANDLERS(msg)
#endif

#ifdef LM_DEBUG
NABoolean doNotRestoreSignalHandlersAfterUDF();
#else
inline NABoolean doNotRestoreSignalHandlersAfterUDF() { return FALSE; }
#endif

#endif  // _LM_DEBUG_H_
