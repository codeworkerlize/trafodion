
/* -*-C++-*-
**************************************************************************
*
* File:         long.C
* Description:  64-bit integer
* Created:      3/5/96
* Language:     C++
*
*
*
*
**************************************************************************
*/

#include "common/Int64.h"

#include "common/NABoolean.h"
#include "common/NAStdlib.h"
#include "common/str.h"

long uint32ToInt64(UInt32 value) { return (long)value; }

int int64ToInt32(long value) {
  UInt32 val32u;
  int val32;

  val32u = (UInt32)value;
  val32 = (int)val32u;

  return val32;
}

double convertInt64ToDouble(const long &src) { return (double)src; }

double convertUInt64ToDouble(const UInt64 &src) { return (double)src; }

long uint32ArrayToInt64(const UInt32 array[2]) {
  long result = uint32ToInt64(array[0]);
  long array1 = uint32ToInt64(array[1]);
  long shift = INT_MAX;  // 2^31 - 1
  shift += 1;            // 2^31
  result *= shift;
  result *= 2;       // 2*32, so result now has array[0] in high word
  result += array1;  // and array[1] in low word
  return result;
}

int aToInt32(const char *src) {
  NABoolean isNeg = FALSE;
  if (*src == '-') {
    isNeg = TRUE;
    src++;
  }

  int tgt = 0;
  while ((*src >= '0') && (*src <= '9')) {
    tgt = tgt * 10 + (*src - '0');
    src++;
  }

  if (isNeg)
    return -tgt;
  else
    return tgt;
}

long atoInt64(const char *src) {
  NABoolean isNeg = FALSE;
  if (*src == '-') {
    isNeg = TRUE;
    src++;
  }

  long tgt = 0;
  while ((*src >= '0') && (*src <= '9')) {
    tgt = tgt * 10 + (*src - '0');
    src++;
  }

  if (isNeg)
    return -tgt;
  else
    return tgt;
}

void convertInt64ToAscii(const long &src, char *tgt) {
#if 0
  long temp = src;  // (src >= 0) ? src : - src;
  char buffer[21];
  char *s = &buffer[21];
  *--s = '\0';
  do {
    char c = (char) (temp % 10);
    if (c < 0)
      c = -c;
    *--s = (char)(c + '0');
    temp /= 10;
  } while (temp != 0);
  if (src < 0)
    *--s = '-';
  strcpy(tgt, s);
#endif
  sprintf(tgt, "%ld", src);
}

void convertUInt64ToAscii(const UInt64 &src, char *tgt) {
  UInt64 temp = src;
  char buffer[21];
  char *s = &buffer[21];
  *--s = '\0';
  do {
    char c = (char)(temp % 10);
    if (c < 0) c = -c;
    *--s = (char)(c + '0');
    temp /= 10;
  } while (temp != 0);
  strcpy(tgt, s);
}

void convertInt64ToUInt32Array(const long &src, UInt32 *tgt) {
  int *tPtr = (int *)&src;
#ifdef NA_LITTLE_ENDIAN
  tgt[0] = tPtr[1];
  tgt[1] = tPtr[0];
#else
  tgt[0] = tPtr[0];
  tgt[1] = tPtr[1];
#endif
}

//
// End of File
//
