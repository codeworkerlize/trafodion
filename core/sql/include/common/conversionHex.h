

/* -*-C++-*-
 *****************************************************************************
 *
 * File:         conversionHex.h
 * RCS:          $Id:
 * Description:  The header file of a set of conversion functions
 *
 * Created:      5/03
 * Language:     C++
 *
 *
 *
 *****************************************************************************
 */

#ifndef _CONVERSION_HEX_H
#define _CONVERSION_HEX_H

#include "common/charinfo.h"
#include "common/wstr.h"

class NAString;
class NAWString;

// the result code for converting a hexadecimal string into the real strin form.
enum hex_conversion_code {
  NOT_SUPPORTED = 1,       // conversion not supported
  INVALID = 2,             // invalid hexadecimal format
  SINGLE_BYTE = 3,         // converted, the result is single-byte
  DOUBLE_BYTE = 4,         // converted, the result is double-byte
  INVALID_CODEPOINTS = 5,  // invalid code points
  CONV_FAILED = 6          // conversion failed
};

//
// A function to convert a hexadecimal string into the real strin form
// Input:
//  str: the source string
//  len: the length of the source string
//  quote: a stop character that the conversion will detect and stop conversion from
//         its first occurrence and on
//  cs: the character set of the string
//  heap: the heap from which the result object will be allocated
//
// Output:
//  return code (see the description for enum hex_conversion_code)
//  result:
//   . value unchanged if result code is not SINGLE_BYTE or DOUBLE_BYTE
//   . the converted string of type NAString* if result code is SINGLE_BYTE
//   . the converted string of type NAWString* if result code is DOUBLE_BYTE
//   The result string should be properly deleted after use.
//
hex_conversion_code verifyAndConvertHex(const NAWchar *str, int len, NAWchar quote, CharInfo::CharSet cs,
                                        CollHeap *heap, void *&result);

#endif
