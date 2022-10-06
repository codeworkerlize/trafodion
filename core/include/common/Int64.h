
#ifndef INT64_H
#define INT64_H
/* -*-C++-*-
**************************************************************************
*
* File:         long.h
* Description:  64-bit integer
* Created:      3/5/96
* Language:     C++
*
*
**************************************************************************
*/

#include <limits.h>
#include "common/Platform.h"

#ifndef LLONG_MAX
#define LLONG_MAX LONG_MAX
#endif

#ifndef LLONG_MIN
#define LLONG_MIN LONG_MIN
#endif

// ***********************************************************************
// Ancillary global functions
// ***********************************************************************

// -----------------------------------------------------------------------
// Convert an unsigned int to long.
// -----------------------------------------------------------------------
long uint32ToInt64(UInt32 value);

// -----------------------------------------------------------------------
// Convert an long to long.
// -----------------------------------------------------------------------
int int64ToInt32(long value);

// -----------------------------------------------------------------------
// Convert the integer from array of two longs, most significant first
// (Guardian-style LARGEINT datatype).
// -----------------------------------------------------------------------
long uint32ArrayToInt64(const UInt32 array[2]);

// -----------------------------------------------------------------------
// Convert an array of two unsigned longs to the integer, most
// significant first.  This routine also takes care of the little
// endian and big endian problems.  Parameter tgt must point to
// an array of two (2) unsigned long elements.
// -----------------------------------------------------------------------
void convertInt64ToUInt32Array(const long &src, UInt32 *tgt);

// -----------------------------------------------------------------------
// Convert the integer from ascii.
// -----------------------------------------------------------------------
int aToInt32(const char *src);

// -----------------------------------------------------------------------
// Convert the integer from ascii.
// -----------------------------------------------------------------------
long atoInt64(const char *src);

// -----------------------------------------------------------------------
// Convert the integer to ascii.
// -----------------------------------------------------------------------
void convertInt64ToAscii(const long &src, char *tgt);

// -----------------------------------------------------------------------
// Convert the unsigned integer to ascii.
// -----------------------------------------------------------------------
void convertUInt64ToAscii(const UInt64 &src, char *tgt);

// -----------------------------------------------------------------------
// Convert the integer to double.
// -----------------------------------------------------------------------
double convertInt64ToDouble(const long &src);

// -----------------------------------------------------------------------
// Convert the integer to double.
// -----------------------------------------------------------------------
double convertUInt64ToDouble(const UInt64 &src);

#endif /* INT64_H */
