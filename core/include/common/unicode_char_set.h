

#ifndef _UNICODE_CHAR_SET_H
#define _UNICODE_CHAR_SET_H

////////////////////////////////////////////////////////////////////
// 4/10/98  A simple class devoting to the concept of Unicode
////////////////////////////////////////////////////////////////////

#include <string.h>

#include <stddef.h>
#include <limits.h>

#include "common/Platform.h"

#include "common/NAWinNT.h"
#include "common/NABoolean.h"

// Forward declaration
class NAWString;

class unicode_char_set {
 public:
  enum CHAR_CODE_POINTS { SPACE = 0x0020, PERCENT = 0x0025, UNDERSCORE = 0x005F, ASCIINULL = 0x0000 };

  unicode_char_set(){};

  virtual ~unicode_char_set(){};

  static NAWchar to_upper(const NAWchar);

  static void to_upper(NAWchar *, size_t, NAWString &);

  static int to_upper(NAWchar *, int, NAWchar *, int);

  // For full case mapping, an lower case character may map to
  // more than one upper case character
  static NAWchar *to_upper_full(const NAWchar);

  static NAWchar to_lower(const NAWchar);

  static NAWchar space_char() { return unicode_char_set::SPACE; };

  static NAWchar null_char() { return 0; };

  static NAWchar underscore_char() { return unicode_char_set::UNDERSCORE; };

  static NAWchar percent_char() { return unicode_char_set::PERCENT; };

  static NAWchar maxCharValue() { return USHRT_MAX; };

  static short bytesPerChar() { return sizeof(NAWchar); };

  static NABoolean isValidUCS2CodePoint(NAWchar wc) {
    UInt32 codePointValue = (UInt32)wc;
    return (codePointValue < 0xFFFE);
  };

  /*
  *** Comment out the following functions because they are not yet used. ***

  // Determines if a NAWchar is a UTF-16 high surrogate code point,
  // ranging from 0xd800 to 0xdbff, inclusive.
  static NABoolean isHighSurrogateCodePoint(NAWchar wc)
  {
    UInt32 codePointValue = (UInt32)wc;
    return ( codePointValue >= 0xD800 && codePointValue <= 0xDBFF );
  }

  // Determines if a character is a UTF-16 low surrogate code point,
  // ranging from 0xdc00 to 0xdfff, inclusive.
  static NABoolean isLowSurrogateCodePoint(NAWchar wc)
  {
    UInt32 codePointValue = (UInt32)wc;
    return ( codePointValue >= 0xDC00 && codePointValue <= 0xDFFF );
  }

  // Determines if the specified code units form a UTF-16 surrogate pair.
  static NABoolean isUTF16SurrogatePair(NAWchar hwc, NAWchar lwc)
  {
    return ( isHighSurrogateCodePoint(hwc) && isLowSurrogateCodePoint(lwc) );
  }

  *** End of commented-out code ***
  */

 private:
};

//
// UTF-8 related macro definitions and function declarations
//

#define IS_7_BIT_ASCII_IN_UTF8_CHAR(x)  ((((x)&0x80) >> 7) == 0)
#define IS_NOT_1ST_BYTE_IN_UTF8_CHAR(x) ((((x)&0xC0) >> 6) == 2)
#define IS_1ST_BYTE_IN_UTF8_CHAR(x)     ((((x)&0xC0) >> 6) != 2)

int UTF8CharLenInBytes(const unsigned char firstByteOfTheUtf8Char);  // returns 0 on errors

int IndexOfLastByteOfUTF8CharAtOrBeforePos(const unsigned char *utf8Str, const int utf8StrLenInBytes,
                                             const int bytePos);  // returns -1 on errors

int IndexOfFirstByteOfUTF8CharAtOrBeforePos(const unsigned char *utf8Str, const int utf8StrLenInBytes,
                                              const int bytePos);  // returns -1 on errors

int maxBytesInUTF8String(char *str, int len);

#endif
