
#ifndef COM_SYSUTILS_H
#define COM_SYSUTILS_H

/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ComSysUtils.h
 * Description:
 *
 * Created:      4/10/96 evening
 * Language:     C++
 *
 *
 *
 *****************************************************************************
 */

#include <sys/time.h>

#include "SQLTypeDefs.h"
#include "common/Int64.h"
#include "common/Platform.h"

#ifdef NA_LITTLE_ENDIAN
#define EXTRACT_SIGN_INT64(sign, ptr) sign = *((int *)ptr + 1) & 0x80000000;
#else
#define EXTRACT_SIGN_INT64(sign, ptr) sign = *((int *)ptr) & 0x80000000;
#endif

// **************************************************************************
// *                                                                        *
// *            Structures used by the gettimeofday function.               *
// *                                                                        *
// **************************************************************************
extern "C" {

// Real Unix need not emulate its native "gettimeofday" (see ComSysUtils.cpp)
#define NA_timeval timeval

struct NA_timezone /* Only here for the compiler. */
{};

typedef struct NA_timeval TimeVal;
typedef struct NA_timezone TimeZone;
int NA_gettimeofday(struct NA_timeval *, struct NA_timezone *);

#define GETTIMEOFDAY(tv, tz) NA_gettimeofday(tv, tz)
}

#if defined(NA_LITTLE_ENDIAN)

inline unsigned short reversebytes(unsigned short s) { return (s >> 8 | s << 8); }

inline unsigned short reversebytesUS(unsigned short s) { return (s >> 8 | s << 8); }

inline unsigned int reversebytes(unsigned int x) {
  return ((x << 24) | (x << 8 & 0x00ff0000) | (x >> 8 & 0x0000ff00) | (x >> 24));
}

inline int reversebytes(int x) {
  return ((x << 24) | (x << 8 & 0x00ff0000) | (x >> 8 & 0x0000ff00) | (x >> 24 & 0x000000ff));
}

inline long reversebytes(long xx) {
  union {
    long xx;
    char c[8];
  } source;

  union {
    long xx;
    char c[8];
  } sink;

  source.xx = xx;

  for (int i = 0; i < 8; i++) sink.c[i] = source.c[7 - i];

  return sink.xx;
}

inline UInt64 reversebytes(UInt64 xx) {
  union {
    UInt64 xx;
    char c[8];
  } source;

  union {
    UInt64 xx;
    char c[8];
  } sink;

  source.xx = xx;

  for (int i = 0; i < 8; i++) sink.c[i] = source.c[7 - i];

  return sink.xx;
}

#endif  // NA_LITTLE_ENDIAN
//----------------------------------------------------------------

#endif

void copyInteger(void *destination, int targetLength, void *sourceAddress, int sourceLength);

void copyToInteger1(Int8 *destination, void *sourceAddress, int sourceSize);

void copyToInteger2(short *destination, void *sourceAddress, int sourceSize);

void copyToInteger4(int *destination, void *sourceAddress, int sourceSize);

void copyToInteger8(long *destination, void *sourceAddress, int sourceSize);
