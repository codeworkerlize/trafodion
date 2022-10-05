
#ifndef _UDR_DEBUG_H_
#define _UDR_DEBUG_H_
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         UdrDebug.h
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
#include <Platform.h>

#ifdef UDR_DEBUG
void udrDebug(const char *formatString, ...);
#define UDR_DEBUG0(m)                udrDebug((m))
#define UDR_DEBUG1(m, a)             udrDebug((m), (a))
#define UDR_DEBUG2(m, a, b)          udrDebug((m), (a), (b))
#define UDR_DEBUG3(m, a, b, c)       udrDebug((m), (a), (b), (c))
#define UDR_DEBUG4(m, a, b, c, d)    udrDebug((m), (a), (b), (c), (d))
#define UDR_DEBUG5(m, a, b, c, d, e) udrDebug((m), (a), (b), (c), (d), (e))
#else
#define UDR_DEBUG0(m)
#define UDR_DEBUG1(m, a)
#define UDR_DEBUG2(m, a, b)
#define UDR_DEBUG3(m, a, b, c)
#define UDR_DEBUG4(m, a, b, c, d)
#define UDR_DEBUG5(m, a, b, c, d, e)
#endif

#define UDR_DEBUG_SIGNAL_HANDLERS(m)

#endif
