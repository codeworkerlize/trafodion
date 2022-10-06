/* -*-C++-*-
 *****************************************************************************
 *
 * File:         nchar_mp.h
 * RCS:          $Id:
 * Description:  The implementation of kanji_char_set and ksc5601_char_set class
 *
 *
 * Created:      12/01/2003
 * Modified:     $ $Date: 2007/10/09 19:38:38 $ (GMT)
 * Language:     C++
 * Status:       $State: Exp $
 *
 *

 *
 *
 *****************************************************************************
 */

#ifndef _NCHAR_MP_H
#define _NCHAR_MP_H

#include <limits.h>
#include <string.h>

#include "common/NABoolean.h"
#include "common/NAWinNT.h"
#include "common/Platform.h"

////////////////////////////////////////////////////////////////////
// A simple class devoting to the concept of MP KANJI charset
////////////////////////////////////////////////////////////////////

class kanji_char_set {
 public:
  kanji_char_set(){};

  virtual ~kanji_char_set(){};

  static NAWchar space_char() { return ' '; };

  static NAWchar null_char() { return 0; };

  static NAWchar underscore_char() {
#ifdef NA_LITTLE_ENDIAN
    // need to flip the two bytes because KANJI_MP data is in multi-byte
    // (big endian order) format
    return 0x5181;
#else
    return 0x8151;
#endif
  };

  static NAWchar percent_char() {
#ifdef NA_LITTLE_ENDIAN
    // need to flip the two bytes because KANJI_MP data is in multi-byte
    // (big endian order) format
    return 0x9381;
#else
    return 0x8193;
#endif
  };

  static NAWchar maxCharValue() { return USHRT_MAX; };

  static short bytesPerChar() { return 2; };

 private:
};

////////////////////////////////////////////////////////////////////
// A simple class devoting to the concept of MP KSC5601 charset
////////////////////////////////////////////////////////////////////

class ksc5601_char_set {
 public:
  ksc5601_char_set(){};

  virtual ~ksc5601_char_set(){};

  static NAWchar space_char() { return ' '; };

  static NAWchar null_char() { return 0; };

  static NAWchar underscore_char() {
#ifdef NA_LITTLE_ENDIAN
    // need to flip the two bytes because KANJI_MP data is in multi-byte
    // (big endian order) format
    return 0xDFA3;
#else
    return 0xA3DF;
#endif
  };

  static NAWchar percent_char() {
#ifdef NA_LITTLE_ENDIAN
    // need to flip the two bytes because KANJI_MP data is in multi-byte
    // (big endian order) format
    return 0xA5A3;
#else
    return 0xA3A5;
#endif
  };

  static NAWchar maxCharValue() { return USHRT_MAX; };

  static short bytesPerChar() { return 2; };

 private:
};

#endif
