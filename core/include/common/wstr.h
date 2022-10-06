
#ifndef WSTR_H
#define WSTR_H

/* -*-C++-*-
 *****************************************************************************
 *
 * File:         <file>
 * Description:
 *
 *
 * Created:      7/10/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include <string.h>

#include "common/NAWinNT.h"
#include "common/Platform.h"
#include "common/unicode_char_set.h"

// -----------------------------------------------------------------------
// Compare w-strings <left> and <right> (using unsigned comparison and
// SQL blank-padding semantics)
// for <length> characters.
// Return a negative value if left < right,
// return 0 if left == right,
// return a positive value if left > right.
// -----------------------------------------------------------------------
int compareWcharWithBlankPadding(const NAWchar *wstr1, UInt32 len1, const NAWchar *wstr2, UInt32 len2);

// -----------------------------------------------------------------------
// Compare strings <left> and <right> (using unsigned comparison).
// for <length> characters.
// Return a negative value if left < right,
// return 0 if left == right,
// return a positive value if left > right.
// -----------------------------------------------------------------------
inline int wc_str_cmp(const NAWchar *left, const NAWchar *right, int length) {
  for (int i = 0; i < length; i++) {
    if (left[i] < right[i]) return -1;
    if (left[i] > right[i]) return 1;
  }
  return 0;
}

// -----------------------------------------------------------------------
// fill string <str>  for <length> bytes with <padchar>
// -----------------------------------------------------------------------

inline void wc_str_pad(NAWchar *str, int length, NAWchar padchar = unicode_char_set::SPACE) {
  for (int i = 0; i < length; i++) str[i] = padchar;
}

// Swap bytes for each NAWchar in the string.
inline void wc_swap_bytes(NAWchar *str, int length) {
  unsigned char *ptr;
  unsigned char temp;

  if (str == 0 || length == 0) return;

  for (int i = 0; i < length; i++) {
    ptr = (unsigned char *)&str[i];
    temp = *ptr;
    *ptr = *(ptr + 1);
    *(ptr + 1) = temp;
  }
}

int na_wstr_cpy_convert(NAWchar *tgt, NAWchar *src, int length, int upshift);

#endif
