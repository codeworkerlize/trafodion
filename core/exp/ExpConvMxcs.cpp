
#include <limits.h>
#include <string.h>
#ifdef NA_MXCS
#undef NA_LITTLE_ENDIAN
#else
#include "common/Platform.h"
#endif

#if defined(NA_LITTLE_ENDIAN) || defined(NA_MXCS)
#include <math.h>
#define MathPow(op1, op2, err) pow(op1, op2)
#else
#include "ieeemath.h"
#endif

#define EXE_CONVERT_STRING_ERROR   -8413
#define EXE_NUMERIC_OVERFLOW       -8411
#define EXE_UNSIGNED_OVERFLOW      -8432
#define EXE_STRING_OVERFLOW        -8402
#define EXE_CONVERT_INTERVAL_ERROR -8422
#define SQL_SMALL_SIZE             2
#define SQL_INT_SIZE               4
#define SQL_LARGE_SIZE             8

#define REC_DATE_YEAR   1
#define REC_DATE_MONTH  2
#define REC_DATE_DAY    3
#define REC_DATE_HOUR   4
#define REC_DATE_MINUTE 5
#define REC_DATE_SECOND 6

// interval datatypes
#define REC_MIN_INTERVAL      195
#define REC_INT_YEAR          195
#define REC_INT_MONTH         196
#define REC_INT_YEAR_MONTH    197
#define REC_INT_DAY           198
#define REC_INT_HOUR          199
#define REC_INT_DAY_HOUR      200
#define REC_INT_MINUTE        201
#define REC_INT_HOUR_MINUTE   202
#define REC_INT_DAY_MINUTE    203
#define REC_INT_SECOND        204
#define REC_INT_MINUTE_SECOND 205
#define REC_INT_HOUR_SECOND   206
#define REC_INT_DAY_SECOND    207
#define REC_MAX_INTERVAL      208

#define REC_NUM_BIG_UNSIGNED 155
#define REC_NUM_BIG_SIGNED   156

#define MSB_CLR_MSK 0x7F
#define MSB_SET_MSK 0x80

#if defined(NA_LITTLE_ENDIAN) || defined(NA_MXCS)

#define BIGN_CLR_SIGN(bignum, len) (((char *)bignum)[len - 1] &= MSB_CLR_MSK);
#define BIGN_SET_SIGN(bignum, len) (((char *)bignum)[len - 1] |= MSB_SET_MSK);
#define BIGN_GET_SIGN(bignum, len) ((char)(((char *)bignum)[len - 1] & MSB_SET_MSK));
#else
#typedef long long;  // 64-bit: get long define below
#include "common/Int64.h"

#define BIGN_CLR_SIGN(bignum, len) (((char *)bignum)[len - 2] &= MSB_CLR_MSK);
#define BIGN_SET_SIGN(bignum, len) (((char *)bignum)[len - 2] |= MSB_SET_MSK);
#define BIGN_GET_SIGN(bignum, len) ((char)(((char *)bignum)[len - 2] & MSB_SET_MSK));

#endif

#if defined(NA_LITTLE_ENDIAN) || defined(NA_MXCS)
#ifndef LLONG_MAX
#define LLONG_MAX _I64_MAX
#endif
// The .NET 2003 headers changed the way LLONG_MIN is defined, which
// causes problems when LLONG_MIN is cast to a double.  Previously
// LLONG_MIN was defined to I64_MIN, which works correctly.  Redefine
// it here so things work correctly.  This only affects NT builds.

#else

#ifndef LLONG_MAX
#define LLONG_MAX 9223372036854775807
#endif

#ifndef LLONG_MIN
#define LLONG_MIN (-9223372036854775807 - 1)
#endif
#endif

static const unsigned short powersOfTen[] = {10,      // 10^1
                                             100,     // 10^2
                                             1000,    // 10^3
                                             10000};  // 10^4

// The following method converts a given BCD string representation (without sign,
// and with the more significant decimal digits in the lower addresses)
// into its equivalent Big Num representation.

static short BigNumHelper_ConvBcdToBigNumHelper(int sourceLength, int targetLength, char *sourceData,
                                                char *targetData) {
  // Recast from bytes to unsigned shorts.
  int targetLengthInShorts = targetLength / 2;
  unsigned short *targetDataInShorts = (unsigned short *)targetData;

  // Initialize the Big Num to zero.
  int i = 0;
  for (i = 0; i < targetLengthInShorts; i++) targetDataInShorts[i] = 0;
  int finalTargetLengthInShorts = 1;

  // Ignore leading zeros in BCD. If all zeros, return.
  int zeros = 0;
  while (zeros < sourceLength && !sourceData[zeros]) zeros++;
  if (zeros == sourceLength) return 0;

  int actualSourceLength = sourceLength - zeros;
  char *actualSourceData = sourceData + zeros;

#if defined(NA_LITTLE_ENDIAN) || defined(NA_MXCS)
  union {
    UInt32 temp;
    struct {
      unsigned short remainder;
      unsigned short carry;
    } tempParts;
  };
#else
  union {
    UInt32 temp;
    struct {
      unsigned short carry;
      unsigned short remainder;
    } tempParts;
  };
#endif

  // Compute the Big Num as follows. First, chunk up the BCD
  // string into chunks of 4-digit unsigned short numbers. Iterate
  // over these chunks, and at each iteration, multiply the current
  // Big Num by 10^4 and then add the numeric value of this chunk
  // to it.
  for (i = 0; i < actualSourceLength; i += 4) {
    // Compute the numeric value of the next 4-digit chunk.
    unsigned short temp1 = 0;
    int j = 0;
    while ((j < 4) && (i + j < actualSourceLength)) {
      temp1 = temp1 * 10 + actualSourceData[i + j];
      j++;
    }
    unsigned short power = powersOfTen[j - 1];

    // Multiply the previous Big Num by 10^4 and add the value
    // of the current chunk to it. It is more efficient to insert the
    // multiplication and addition code here than to call the Helper()
    // methods.
    temp = ((int)targetDataInShorts[0]) * power + temp1;
    targetDataInShorts[0] = tempParts.remainder;
    for (j = 1; j < finalTargetLengthInShorts; j++) {
      temp = ((int)targetDataInShorts[j]) * power + tempParts.carry;
      targetDataInShorts[j] = tempParts.remainder;
    }
    if (tempParts.carry) {
      if (finalTargetLengthInShorts >= targetLengthInShorts) return -1;
      finalTargetLengthInShorts++;
      targetDataInShorts[finalTargetLengthInShorts - 1] = tempParts.carry;
    }
  }

  return 0;
}

// The following method converts a given BCD string representation
// (with sign, and with the more significant decimal digits in the lower
// addresses) into its equivalent Big Num representation.

static short BigNumHelper_ConvBcdToBigNumWithSignHelper(int sourceLength, int targetLength, char *sourceData,
                                                        char *targetData) {
  short result = BigNumHelper_ConvBcdToBigNumHelper(sourceLength - 1, targetLength, sourceData + 1, targetData);

  // Set up sign.
  if (sourceData[0] == '-') BIGN_SET_SIGN(targetData, targetLength);

  return result;
}

// The following method converts a given Big Num (with sign) into
// its equivalent BCD string representation (with the more significant
// decimal digits in the lower addresses).

// The following method converts a given Big Num (without sign) into
// its equivalent BCD string representation (with the more significant decimal
// digits in the lower addresses).

static short BigNumHelper_ConvBigNumToBcdHelper(int sourceLength, int targetLength, char *sourceData,
                                                char *targetData) {
  // Recast from bytes to unsigned shorts.
  int sourceLengthInShorts = sourceLength / 2;
  unsigned short *sourceDataInShorts = (unsigned short *)sourceData;

  unsigned short *tempSourceDataInShorts = new unsigned short[sourceLength / 2];

  int i = 0;
  for (i = 0; i < sourceLengthInShorts; i++) tempSourceDataInShorts[i] = sourceDataInShorts[i];

  // Initialize the BCD to zero.
  for (i = 0; i < targetLength; i++) targetData[i] = 0;
  char *finalTargetData = targetData + targetLength - 1;
  int finalTargetLength = 1;

  // Ignore trailing zeros in the Big Num. If all zeros, return.
  int actualSourceLengthInShorts = sourceLengthInShorts;
  while (actualSourceLengthInShorts > 0 && !tempSourceDataInShorts[actualSourceLengthInShorts - 1])
    actualSourceLengthInShorts--;
  if (!actualSourceLengthInShorts) {
    delete[] tempSourceDataInShorts;
    return 0;
  }

  union {
    UInt32 temp;
    unsigned short temp1[2];
  };

  unsigned short remainder = 0;

  // Compute the final BCD as follows. Keep dividing the Big Num by 10^4
  // until the quotient becomes less than 10^4. At each iteration, compute
  // the 4-digit BCD representation of the remainder, and append it to the
  // left of the final BCD. For the final remainder, compute its BCD
  // representation (which may be less than 4-digits) and append it to the
  // left of the final BCD.

  finalTargetData++;
  finalTargetLength = 0;
  while ((actualSourceLengthInShorts != 1) || (tempSourceDataInShorts[actualSourceLengthInShorts - 1] >= 10000)) {
    // Divide the Big Num by 10^4. It is more efficient to insert
    // the division code than to call SimpleDivideHelper();
    int j = 0;
    for (j = actualSourceLengthInShorts - 1; j >= 0; j--) {
#if defined(NA_LITTLE_ENDIAN) || defined(NA_MXCS)
      temp1[0] = tempSourceDataInShorts[j];
      temp1[1] = remainder;
#else
      temp1[1] = tempSourceDataInShorts[j];
      temp1[0] = remainder;
#endif

      tempSourceDataInShorts[j] = (unsigned short)(temp / 10000);
      remainder = (unsigned short)(temp % 10000);
    }
    if (!tempSourceDataInShorts[actualSourceLengthInShorts - 1]) actualSourceLengthInShorts--;

    // Compute the BCD representation of the remainder and append to the
    // left of the final BCD.
    for (j = 0; j < 4; j++) {
      if (finalTargetLength >= targetLength) {
        delete[] tempSourceDataInShorts;
        return -1;
      }
      finalTargetData--;
      finalTargetLength++;
      finalTargetData[0] = remainder % 10;
      remainder = remainder / 10;
    }
  }

  // Compute the BCD representation of the final remainder and append to the
  // left of the final BCD.
  remainder = tempSourceDataInShorts[0];
  while (remainder) {
    if (finalTargetLength >= targetLength) {
      delete[] tempSourceDataInShorts;
      return -1;
    }
    finalTargetData--;
    finalTargetLength++;
    finalTargetData[0] = remainder % 10;
    remainder = remainder / 10;
  }

  delete[] tempSourceDataInShorts;
  return 0;
}

static short BigNumHelper_ConvBigNumWithSignToBcdHelper(int sourceLength, int targetLength, char *sourceData,
                                                        char *targetData) {
  char sign = BIGN_GET_SIGN(sourceData, sourceLength);

  // Set up sign.
  targetData[0] = (sign) ? '-' : '+';

  // Temporarily clear source sign
  BIGN_CLR_SIGN(sourceData, sourceLength);

  short result = BigNumHelper_ConvBigNumToBcdHelper(sourceLength, targetLength - 1, sourceData, targetData + 1);

  // Restore sign
  if (sign) BIGN_SET_SIGN(sourceData, sourceLength);

  return result;
}

static short convInt64ToDecMxcs(char *target, int targetLen, long source) {
  int flags = 0;
  bool negative = (source < 0);
  int currPos = targetLen - 1;

  if (negative && (currPos >= 0)) {
    if (source == LLONG_MIN) {
      // before we can convert this to a positive number, we have to
      // work on the first digit. Otherwise, we would cause an overflow.
      int temp = (int)(source % 10);
      target[currPos--] = (char)('0' + (temp < 0 ? -temp : temp));
      source /= 10;
    };
    // now make source positive
    source = -source;
  };

  while (source != 0) {
    if (currPos < 0) {
      // target is not long enough - overflow
      return EXE_STRING_OVERFLOW;
    };

    target[currPos--] = '0' + (char)(source % 10);
    source /= 10;
  };

  // zero pad the leading spaces
  while (currPos >= 0) target[currPos--] = '0';

  if (negative) target[0] |= 0200;

  return 0;
}

static short safe_add_digit_to_double(double dvalue,  // Original value
                                      int digit,      // Single digit to be added
                                      double *result) {
  *result = dvalue * 10 + digit;
  return 0;
}

static short convAsciiToFloat64Mxcs(char *target, char *source, int sourceLen) {
  short err = 0;

  double ptarget;

  // skip leading and trailing blanks and adjust source and sourceLen
  // accordingly
  int i = 0;
  for (; i < sourceLen && *source == ' '; i++) source++;

  if (i == sourceLen) {
    // string contains only blanks.
    return EXE_CONVERT_STRING_ERROR;
  };

  sourceLen -= i;

  // we know that we found at least one non-blank character.
  // so we just decrement sourceLen till we scanned all
  // trailing blanks
  while (source[sourceLen - 1] == ' ') {
    sourceLen--;
  };

  enum State { START, SKIP_BLANKS, MANT_BEFORE_SIGN, MANT_AFTER_SIGN, EXP_START, EXPONENT, ERROR_CONV };

  int cp = 0;
  double mantissa = 0;
  double exponent = 0;
  bool negMantissa = false;
  bool negExponent = false;
  int numScale = 0;
  bool validNum = false;

  State state = START;
  bool skipChar;
  while (cp < sourceLen) {
    skipChar = false;
    //      cout << "state = " << state << endl;

    switch (state) {
      case START:
        if ((source[cp] == '+') || (source[cp] == '-')) {
          if (source[cp] == '-') negMantissa = true;
          state = SKIP_BLANKS;
          skipChar = true;
        } else
          state = MANT_BEFORE_SIGN;
        break;

      case SKIP_BLANKS:
        if (source[cp] != ' ')
          state = MANT_BEFORE_SIGN;
        else
          skipChar = true;
        break;

      case MANT_BEFORE_SIGN:
        if (source[cp] == '.') {
          state = MANT_AFTER_SIGN;
        } else if ((source[cp] == 'E') || (source[cp] == 'e')) {
          validNum = false;
          state = EXP_START;
        } else if ((source[cp] < '0') || (source[cp] > '9'))
          state = ERROR_CONV;
        else {
          if (safe_add_digit_to_double(mantissa, source[cp] - '0', &mantissa) != 0) {
            return EXE_NUMERIC_OVERFLOW;
          }
          validNum = true;
        }
        skipChar = true;
        break;

      case MANT_AFTER_SIGN:
        if ((source[cp] == 'E') || (source[cp] == 'e')) {
          if (!validNum)
            state = ERROR_CONV;
          else {
            validNum = false;
            state = EXP_START;
          }
        } else if ((source[cp] < '0') || (source[cp] > '9'))
          state = ERROR_CONV;
        else {
          numScale++;
          if (safe_add_digit_to_double(mantissa, source[cp] - '0', &mantissa) != 0) {
            return EXE_NUMERIC_OVERFLOW;
          }
          validNum = true;
        }
        skipChar = true;
        break;

      case EXP_START: {
        if ((source[cp] == '+') || (source[cp] == '-')) {
          if (source[cp] == '-') negExponent = true;
          skipChar = true;
        }
        state = EXPONENT;
      } break;

      case EXPONENT: {
        if ((source[cp] < '0') || (source[cp] > '9'))
          state = ERROR_CONV;
        else {
          if (safe_add_digit_to_double(exponent, source[cp] - '0', &exponent) != 0) {
            return EXE_NUMERIC_OVERFLOW;
          }
          validNum = true;
        }
        skipChar = true;
      } break;

      case ERROR_CONV: {
        // string contains only blanks.
        return EXE_CONVERT_STRING_ERROR;
      }; break;

      default:
        state = ERROR_CONV;
        break;

    }  // switch state

    if ((state != ERROR_CONV) && (skipChar)) cp++;
  }  // while

  if (!validNum) {
    return EXE_CONVERT_STRING_ERROR;
  };

  double scale = MathPow(10.0, (double)numScale, err);
  if (err) {
    return EXE_NUMERIC_OVERFLOW;
  }

  if (negMantissa) mantissa = -mantissa;

  mantissa = mantissa / scale;

  if (negExponent) exponent = -exponent;
  exponent = MathPow(10.0, exponent, err);
  if (err) {
    return EXE_NUMERIC_OVERFLOW;
  }
  ptarget = mantissa * exponent;

  if (target) {
    memcpy(target, (char *)&ptarget, sizeof(double));
  }

  // conversion was OK
  return 0;
}

///////////////////////////////////////////////////////////////////
// function to convert an ASCII string to DEC or LARGEDEC
// the first character of a LARGEDEC is a sign and is ignored here.
// In fact, call has to pass in &target[1], targetLen - 1, and
// offset = '0', if the conversion is ASCII -> LARGEDEC.
// if conversion is ASCII -> DEC, caller has to pass
// &target[0], targetLen, and offset = 0.
// Also, this function always sets the sign bit for DEC if the
// source is negative. For LARGEDEC this has to be changed
// accordingly.
// The function assumes that source is at least
// sourceLen long. Trailing '\0' is not recongnized
///////////////////////////////////////////////////////////////////
static short convAsciiToDecMxcs(char *target, int targetLen, int targetScale, char *source, int sourceLen,
                                char offset) {
  short negative = 0;             // default is a positive value
  int sourceStart = 0;            // start of source after skipping 0 and ' '
  int sourceScale = 0;            // by default the scale is 0
  int targetPos = targetLen - 1;  // current positon in the target

  // skip leading blanks
  while ((sourceStart < sourceLen) && (source[sourceStart] == ' ')) sourceStart++;

  // only blanks found, error
  if (sourceStart == sourceLen) {
    return EXE_CONVERT_STRING_ERROR;
  };

  // skip trailing blanks. We know that we found already some
  // non-blank character, thus sourceLen is always > sourceStart
  while (source[sourceLen - 1] == ' ') sourceLen--;

  // if the string contains 'e' or 'E' it actually represents
  // a float value. Do a ASCII -> DOUBLE -> DEC
  // use a for-loop to find the 'e'. If we find one, we do the
  // described conversion and return. If we do not find one,
  // we go on with this function.
  // Also, start at the end of the string, if we have an 'e' it
  // is probably closer to the end.
  for (int i = sourceLen - 1; i >= sourceStart; i--) {
    if (source[i] == 'E' || source[i] == 'e') {
      // found a float representation.

      double intermediate;
      if (convAsciiToFloat64Mxcs((char *)&intermediate, &source[sourceStart], sourceLen - sourceStart))

        return EXE_CONVERT_STRING_ERROR;

      // scale the intermediate
      for (int j = 0; j < targetScale; j++) {
        intermediate *= 10.0;

        if ((intermediate < LLONG_MIN) || (intermediate > LLONG_MAX)) {
          return EXE_NUMERIC_OVERFLOW;
        };
      }

      if (convInt64ToDecMxcs(target, targetLen, (long)intermediate)) return EXE_CONVERT_STRING_ERROR;
      return 0;
    }
  }

  // the string does not represent a float value. Go on with
  // the processing
  int sourcePos = sourceLen - 1;  // current position in the string

  // check for sign
  if (source[sourceStart] == '+')
    sourceStart++;
  else if (source[sourceStart] == '-') {
    negative = 1;
    sourceStart++;
  };

  // skip leading zeros
  while ((sourceStart < sourceLen) && (source[sourceStart] == '0')) sourceStart++;

  // only zeros found, target is 0
  if (sourceStart == sourceLen) {
    memset(target, '0' - offset, targetLen);
    return 0;
  };

  // determine the scale of the source
  int index = sourceStart;
  int pointPos = -1;
  while ((index < sourceLen) && pointPos < 0) {
    if (source[index] == '.')
      pointPos = index;
    else
      index++;
  };
  if (pointPos >= 0)
    // determine sourceScale
    sourceScale = sourceLen - pointPos - 1;

  short raiseOverflowWarning = 0;

  // procress digits, start from end
  while ((sourcePos >= sourceStart) && (targetPos >= 0)) {
    // add zeros to adjust scale
    if (targetScale > sourceScale) {
      target[targetPos--] = '0' - offset;
      targetScale--;
    } else if (targetScale < sourceScale) {
      // skip source digits
      // but at least one of the digits skipped is not a '0', then
      // a EXE_NUMERIC_OVERFLOW warning needs to be raised
      if (source[sourcePos] != '0') raiseOverflowWarning = 1;
      sourcePos--;
      sourceScale--;
    } else if (sourcePos == pointPos) {
      // simply skip the decimal point
      sourcePos--;
    } else {
      // if source is not a digit, we have an error
      if (source[sourcePos] < '0' || source[sourcePos] > '9') {
        return EXE_CONVERT_STRING_ERROR;
      };
      // copy source to target
      target[targetPos--] = source[sourcePos--] - offset;
    };
  };

  //  if (raiseOverflowWarning)
  //    ExRaiseSqlWarning(heap, diagsArea, EXE_NUMERIC_OVERFLOW);

  // we might be right on the decimal point. Skip it before test for
  // overflow
  if (sourcePos == pointPos) sourcePos--;

  // if we couldn't copy all digits, we had an overflow
  if (sourcePos >= sourceStart) {
    return EXE_NUMERIC_OVERFLOW;
  };

  // left pad the target with zeros
  while (targetPos >= 0) target[targetPos--] = '0' - offset;

  // add sign
  if (negative) target[0] |= 0200;

  return 0;
};

//////////////////////////////////////////////////////////////////
// function to convert BIGNUM to LARGEDEC
///////////////////////////////////////////////////////////////////
static short convBigNumToLargeDecMxcs(char *target, int targetLen, char *source, int sourceLen) {
  // Convert the Big Num (with sign) into a BCD representation by calling
  // a BigNumHelper method
  short retCode = BigNumHelper_ConvBigNumWithSignToBcdHelper(sourceLen, targetLen, source, target);
  if (retCode == -1) {
    // target is not long enough - overflow
    return EXE_NUMERIC_OVERFLOW;
  };

  return 0;
}

//////////////////////////////////////////////////////////////////
// function to convert a LARGEDEC  to an ASCII string
// Trailing '\0' is not set!
//
// For fixed char target, left pad blanks if leftPad is true, or
// right pad blanks otherwise.
//
// For varchar target, left-justify string without blank pads
// to the right.
//
// convDoIt sets leftPad to true if callers pass in a
// CONV_UNKNOWN_LEFTPAD to convDoIt.
///////////////////////////////////////////////////////////////////
static short convLargeDecToAsciiMxcs(char *target, int targetLen, char *source, int sourceLen, int sourceScale,
                                     char *varCharLen, int varCharLenSize, short leftPad) {
  // trim tail '0'
  // e.g.
  // select -123.4345435435435435000 from dual;
  // The result will be -123.4345435435435435 instead of -123.4345435435435435000
  if (sourceScale > 0 && sourceLen > 0) {
    int nCount = 0;
    char *p = &source[sourceLen - 1];
    while (*p == 0 && p != source) {
      ++nCount;
      --p;
    }
    if (nCount > 0) {
      if (sourceScale <= nCount) {
        sourceLen = sourceLen - sourceScale;
        sourceScale = 0;
      } else {
        sourceScale = sourceScale - nCount;
        sourceLen = sourceLen - nCount;
      }
    }
  }
  if (source[0] < 2) {
    unsigned short realSource[256];
    memcpy((char *)realSource, source, sourceLen);

    if (realSource[0])
      source[0] = '-';
    else
      source[0] = '+';

    int realLength = 1 + (sourceLen - 2) / 5;
    for (int srcPos = sourceLen - 1; srcPos; srcPos--) {
      int r = 0;
      for (int i = 1; i <= realLength; i++) {
        int q = (realSource[i] + r) / 10;
        r = (r + realSource[i]) - 10 * q;
        realSource[i] = (short)q;
        r <<= 16;
      }
      source[srcPos] = (char)(r >>= 16);
    }
  }

  if (varCharLenSize) leftPad = 0;
  // skip leading zeros; start with index == 1; index == 0 is sign
  // stop looking one position before the decimal point. This
  // position is sourceLen - sourceScale - 1
  int currPos = 1;
  while ((currPos < (sourceLen - sourceScale - 1)) && source[currPos] == 0) currPos++;

  int padLen = targetLen;
  if (source[0] == '-') padLen--;
  int requiredDigits = sourceLen - currPos - sourceScale;

  padLen -= requiredDigits;

  if (padLen < 0) requiredDigits -= padLen;

  if (sourceScale) {
    if (padLen > 1) {  // fraction exists
      padLen--;        // for decimal point
      sourceScale = (sourceScale < padLen ? sourceScale : padLen);
      padLen -= sourceScale;
    } else  // no fraction
      sourceScale = 0;
  }

  int targetPos = 0;

  // if target is fixed length and leftPad, left pad blanks
  if (leftPad) {
    for (; targetPos < padLen; targetPos++) target[targetPos] = ' ';
  }

  // fill in sign
  if (source[0] == '-') target[targetPos++] = '-';

  // fill in digits
  int i = 0;
  for (i = 0; i < requiredDigits; i++, targetPos++, currPos++) target[targetPos] = source[currPos] + '0';

  // if we have a scale, add decimal point and some digits
  if (sourceScale) {
    target[targetPos++] = '.';
    for (i = 0; i < sourceScale; i++, targetPos++, currPos++) target[targetPos] = source[currPos] + '0';
  };

  // Right pad blanks for fixed char.
  if (!leftPad && !varCharLenSize)
    while (targetPos < targetLen) target[targetPos++] = ' ';

  // set length field for variable length targets
  if (varCharLenSize) {
    if (varCharLenSize == sizeof(int))
      memcpy(varCharLen, (char *)&targetPos, sizeof(int));
    else {
      short VCLen = (short)targetPos;
      memcpy(varCharLen, (char *)&VCLen, sizeof(short));
    };
  };

  // make sure that the source fits in the target
  // we might skip some digits after the decimal point
  // to make it fit
  if (padLen < 0) {
    // target string is not long enough - overflow
    return EXE_STRING_OVERFLOW;
  };
  return 0;
};

//////////////////////////////////////////////////////////////////////////
// *********************************
//////////////////////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////////////
// function to convert an ASCII string to BIGNUM.
//
// First convert from ASCII to LARGEDEC, then convert from LARGEDEC
// to BIGNUM
///////////////////////////////////////////////////////////////////
static short convAsciiToBigNumMxcs(char *target, int targetLen, int targetType, int targetPrecision, int targetScale,
                                   char *source, int sourceLen) {
  short retCode = 0;
  // Convert from ASCII to an intermediate LARGEDEC, using the function convAsciiToDec().
  // To understand this function call, it will be helpful to review the
  // comments in its definition earlier.
  char *intermediateLargeDec = new char[targetPrecision + 1];
  retCode = convAsciiToDecMxcs(intermediateLargeDec + 1, targetPrecision, targetScale, source, sourceLen, '0');
  if (retCode) {
    delete[] intermediateLargeDec;
    return retCode;
  }

  if ((targetType == REC_NUM_BIG_UNSIGNED) && (intermediateLargeDec[1] & 0200)) {
    delete[] intermediateLargeDec;

    return EXE_UNSIGNED_OVERFLOW;
  }

  // Set the sign byte in the intermediate LARGEDEC.
  if (intermediateLargeDec[1] & 0200) {
    // reset the bit
    intermediateLargeDec[1] &= 0177;
    intermediateLargeDec[0] = '-';
  } else
    intermediateLargeDec[0] = '+';

  // Convert from the intermediate LARGEDEC to BIGNUM using a BigNumHelper method.
  retCode = BigNumHelper_ConvBcdToBigNumWithSignHelper(targetPrecision + 1, targetLen, intermediateLargeDec, target);
  delete[] intermediateLargeDec;

  if (retCode == -1) {
    // target is not long enough - overflow
    return EXE_NUMERIC_OVERFLOW;
  };

  return 0;
}

//////////////////////////////////////////////////////////////////
// function to convert a BIGNUM  to an ASCII string
// Trailing '\0' is not set!
//
// This function first converts the BIGNUM to an intermediate
// LARGEDEC, then calls the previous function convLargeDecToAscii()
///////////////////////////////////////////////////////////////////
static short convBigNumToAsciiMxcs(char *target, int targetLen, char *source, int sourceLen, int sourcePrecision,
                                   int sourceScale) {
  char *intermediateLargeDec = new char[sourcePrecision + 1];  // one extra byte for the sign.
  short retCode = convBigNumToLargeDecMxcs(intermediateLargeDec, sourcePrecision + 1, source, sourceLen);
  if (retCode) return retCode;

  retCode = convLargeDecToAsciiMxcs(target, targetLen, intermediateLargeDec, sourcePrecision + 1, sourceScale, 0, 0,
#ifdef NA_MXCS
                                    0);
#else
                                    1);
#endif

  delete[] intermediateLargeDec;
  return retCode;
};

static short getIntervalStartField(int datatype) {
  switch (datatype) {
    case REC_INT_YEAR:
      return REC_DATE_YEAR;
    case REC_INT_MONTH:
      return REC_DATE_MONTH;
    case REC_INT_YEAR_MONTH:
      return REC_DATE_YEAR;
    case REC_INT_DAY:
      return REC_DATE_DAY;
    case REC_INT_HOUR:
      return REC_DATE_HOUR;
    case REC_INT_DAY_HOUR:
      return REC_DATE_DAY;
    case REC_INT_MINUTE:
      return REC_DATE_MINUTE;
    case REC_INT_HOUR_MINUTE:
      return REC_DATE_HOUR;
    case REC_INT_DAY_MINUTE:
      return REC_DATE_DAY;
    case REC_INT_SECOND:
      return REC_DATE_SECOND;
    case REC_INT_MINUTE_SECOND:
      return REC_DATE_MINUTE;
    case REC_INT_HOUR_SECOND:
      return REC_DATE_HOUR;
    case REC_INT_DAY_SECOND:
      return REC_DATE_DAY;
    default:
      break;
  }
  return REC_DATE_YEAR;
}

static short getIntervalEndField(int datatype) {
  switch (datatype) {
    case REC_INT_YEAR:
      return REC_DATE_YEAR;
    case REC_INT_MONTH:
      return REC_DATE_MONTH;
    case REC_INT_YEAR_MONTH:
      return REC_DATE_MONTH;
    case REC_INT_DAY:
      return REC_DATE_DAY;
    case REC_INT_HOUR:
      return REC_DATE_HOUR;
    case REC_INT_DAY_HOUR:
      return REC_DATE_HOUR;
    case REC_INT_MINUTE:
      return REC_DATE_MINUTE;
    case REC_INT_HOUR_MINUTE:
      return REC_DATE_MINUTE;
    case REC_INT_DAY_MINUTE:
      return REC_DATE_MINUTE;
    case REC_INT_SECOND:
      return REC_DATE_SECOND;
    case REC_INT_MINUTE_SECOND:
      return REC_DATE_SECOND;
    case REC_INT_HOUR_SECOND:
      return REC_DATE_SECOND;
    case REC_INT_DAY_SECOND:
      return REC_DATE_SECOND;
    default:
      break;
  }
  return REC_DATE_YEAR;
}

//////////////////////////////////////////////////////////////////
// This function count the number of digits in a non-negative long.
// The function assumes that targetLen is 1 to 19, inclusive.
///////////////////////////////////////////////////////////////////

static int getDigitCount(long value) {
  static const long decValue[] = {0,
                                  9,
                                  99,
                                  999,
                                  9999,
                                  99999,
                                  999999,
                                  9999999,
                                  99999999,
                                  999999999,
                                  9999999999LL,
                                  99999999999LL,
                                  999999999999LL,
                                  9999999999999LL,
                                  99999999999999LL,
                                  999999999999999LL,
                                  9999999999999999LL,
                                  99999999999999999LL,
                                  999999999999999999LL};

  for (int i = 4; i <= 16; i += 4)
    if (value <= decValue[i]) {
      if (value <= decValue[i - 3]) return (i - 3);
      if (value <= decValue[i - 2]) return (i - 2);
      if (value <= decValue[i - 1])
        return (i - 1);
      else
        return i;
    }
  if (value <= decValue[17]) return 17;
  if (value <= decValue[18]) return 18;
  return 19;
}

///////////////////////////////////////////////////////////////////
// function to convert interval field ASCII string to long
// The function reads at most sourceLen number of characters.
// Or until it encounters a non-digit character and sets the
// sourceLen to the actual length of the string read if it is
// smaller than sourceLen.
// This function is used for ASCII to INTERVAL conversions only.
// Blanks and sign are already removed.
///////////////////////////////////////////////////////////////////
static short convAsciiFieldToInt64Mxcs(long &target, int targetScale, char *source, int &sourceLen, int flags) {
  int currPos = 0;  // current position in the string
  target = 0;       // result
  while ((currPos < sourceLen) && ((source[currPos] >= '0') && (source[currPos] <= '9'))) {
    short thisDigit = source[currPos] - '0';
    if (target > (LLONG_MAX / 10)) {  // next power of 10 causes an overflow
      return EXE_NUMERIC_OVERFLOW;
    }
    target *= 10;
    if (target > LLONG_MAX - thisDigit) {  // adding this digit causes an overflow
      return EXE_NUMERIC_OVERFLOW;
    }
    target += thisDigit;
    currPos++;
  }
  if (!currPos) {
    // No digits were found
    return EXE_CONVERT_STRING_ERROR;
  }
  // return the number of digits read
  sourceLen = currPos;
  return 0;
};

///////////////////////////////////////////////////////////////////
// function to convert an ASCII string to an interval datatype.
///////////////////////////////////////////////////////////////////
static short convAsciiToIntervalMxcs(char *target, int targetLen, int targetDatatype, int leadingPrecision,
                                     int fractionPrecision, char *source, int sourceLen, short allowSignInInterval,
                                     int flags) {
  short retCode = 0;

  // skip leading and trailing blanks and adjust source and sourceLen
  // accordingly
  while ((sourceLen > 0) && (*source == ' ')) {
    source++;
    sourceLen--;
  }

  if (!sourceLen) {
    // string contains no digits
    return EXE_CONVERT_INTERVAL_ERROR;
  };

  short negInterval = 0;
  if ((source[0] == '-') || (source[0] == '+')) {
    if (!allowSignInInterval) {
      // string starts with a sign - not allowed
      return EXE_CONVERT_INTERVAL_ERROR;
    } else {
      if (source[0] == '-') negInterval = 1;
      source += 1;
      sourceLen -= 1;
    }
  }
  // we know that we found at least one non-blank character.
  // so we just decrement sourceLen till we scanned all
  // trailing blanks
  while (source[sourceLen - 1] == ' ') sourceLen--;

  char delimiter[] = "^-^:::";
  unsigned short maxFieldValue[] = {0, 11, 0, 23, 59, 59};

  short start = getIntervalStartField(targetDatatype);
  short end = getIntervalEndField(targetDatatype);

  // convert the first field
  long intermediate;
  int fieldLen = leadingPrecision;
  if (fieldLen > sourceLen) fieldLen = sourceLen;  // so we don't read garbage past end of string

  retCode = convAsciiFieldToInt64Mxcs(intermediate,
                                      0,  // targetScale
                                      source, fieldLen, flags);
  if (retCode) return retCode;

  int strIndex = fieldLen;
  long fieldValue;

  for (int field = start + 1; field <= end; field++) {
    int index = field - REC_DATE_YEAR;

    // Make sure there is still some string left before we look
    // for the delimiter. The '+1' in the test also makes sure
    // there is at least one character after the delimiter.
    // Without the '+1', we get a strange string conversion
    // error, EXE_CONVERT_STRING_ERROR, instead of the more
    // reasonable interval conversion error,
    // EXE_CONVERT_INTERVAL_ERROR, on interval strings like
    // '20-'. Though interval strings like '20-hithere' will
    // still get EXE_CONVERT_STRING_ERROR.
    if (strIndex + 1 >= sourceLen) {
      return EXE_CONVERT_INTERVAL_ERROR;
    }

    // get the delimiter preceding this field
    if (source[strIndex] != delimiter[index]) {
      // HOUR can be preceded by either ':' or ' '
      if ((field != REC_DATE_HOUR) || (source[strIndex] != ' ')) {
        return EXE_CONVERT_INTERVAL_ERROR;
      }
    }

    strIndex++;  // step over delimiter
    fieldLen = 2;
    if (fieldLen > sourceLen - strIndex) fieldLen = sourceLen - strIndex;  // don't go off end of string

    // convert the next field
    retCode = convAsciiFieldToInt64Mxcs(fieldValue,
                                        0,  // targetScale
                                        &source[strIndex], fieldLen, flags);
    if (retCode) return retCode;

    if (fieldValue > maxFieldValue[index]) {
      return EXE_CONVERT_INTERVAL_ERROR;
    }
    strIndex += fieldLen;
    intermediate = intermediate * (maxFieldValue[index] + 1) + fieldValue;
  }

  // adjust the value for fractionPrecision
  for (short i = 0; i < fractionPrecision; i++) {
    intermediate *= 10;
  }

  // if the end field is SECOND, it may have a fractional part
  if ((end == REC_DATE_SECOND) && (source[strIndex] == '.')) {
    int sourcePrecision = (sourceLen - strIndex - 1);
    int fraction = 0;
    if (sourcePrecision) {
      long interm;
      retCode = convAsciiFieldToInt64Mxcs(interm, 0, &source[strIndex + 1],
                                          sourcePrecision,  // sourceLen
                                          flags);
      if (retCode) return retCode;

      if (interm < 0) {
        return EXE_NUMERIC_OVERFLOW;
      } else if (interm > UINT_MAX) {
        return EXE_NUMERIC_OVERFLOW;
      } else {
        fraction = (UInt32)interm;
      }
    };

    // scale up or down if necessary
    while (sourcePrecision < fractionPrecision) {
      fraction *= 10;
      sourcePrecision++;
    }

    int oldFraction = fraction;
    int scaleDownValue = 1;

    while (sourcePrecision > fractionPrecision) {
      fraction /= 10;
      scaleDownValue *= 10;
      sourcePrecision--;
    }

    // Detect if meaningful scale truncation has occured.  If so, raise an error
    if (oldFraction > (fraction * scaleDownValue)) {
      return EXE_CONVERT_INTERVAL_ERROR;
    }

    intermediate += fraction;
  } else {
    // check if the string is completely converted
    if (strIndex != sourceLen) {
      return EXE_CONVERT_INTERVAL_ERROR;
    }
  }

  switch (targetLen) {
    case SQL_SMALL_SIZE:
      if ((intermediate < SHRT_MIN) || (intermediate > SHRT_MAX)) {
        return EXE_CONVERT_INTERVAL_ERROR;
      };
      if (negInterval)
        *(short *)target = -(short)intermediate;
      else
        *(short *)target = (short)intermediate;
      break;

    case SQL_INT_SIZE:
      if ((intermediate < INT_MIN) || (intermediate > INT_MAX)) {
        return EXE_CONVERT_INTERVAL_ERROR;
      };
      if (negInterval)
        *(int *)target = -(int)intermediate;
      else
        *(int *)target = (int)intermediate;
      break;

    case SQL_LARGE_SIZE:
      if (negInterval)
        *(long *)target = -intermediate;
      else
        *(long *)target = intermediate;
      break;

    default:
      return EXE_CONVERT_INTERVAL_ERROR;
  }
  return 0;
};

//////////////////////////////////////////////////////////////////
// function to convert an long  to an ASCII string
// Trailing '\0' is not set!
//
// For fixed char targets, left pad fillers if leftPad is
// true, or right pad fillers (default mode) otherwise.
//
// convDoIt sets leftPad to true if callers pass in a
// CONV_UNKNOWN_LEFTPAD to convDoIt.
//
// In only a few cases of fixed char, leftPad will be set to true.
// e.g., caller wants to convert 7 -> 07        as in month -> ascii
//                           or  8 -> '    8'   as in sqlci display
///////////////////////////////////////////////////////////////////
static short convInt64ToAsciiMxcs(char *target, int targetLen, long source, int scale, char filler, short leadingSign,
                                  short leftPad) {
  int digitCnt = 0;
  short negative = (source < 0);
  short fixRightMost = 0;  // true if need to fix the rightmost digit.

  int padLen = targetLen;
  int requiredDigits = 0;
  int leftMost;   // leftmost digit.
  int rightMost;  // rightmost digit.
  int sign = 0;

  //  long newSource = (negative ? -source : source);
  long newSource = 0;
  if ((negative) && (source == 0x8000000000000000LL))  // = -2 ** 63
  {
    newSource = 0x7fffffffffffffffLL;
    //             123456789012345
    digitCnt = 19;
    fixRightMost = 1;
  } else {
    newSource = (negative ? -source : source);
    digitCnt = getDigitCount(newSource);
  }

  if (leadingSign || negative) {
    sign = 1;
    padLen--;
  }
  // No truncation allowed.
  requiredDigits = digitCnt;
  // Add extra zero's.
  if (scale > requiredDigits) requiredDigits += (scale - requiredDigits);
  padLen -= requiredDigits;
  if (scale) padLen--;  // decimal point
  if (padLen < 0) {
    // target string is not long enough - overflow
    return EXE_STRING_OVERFLOW;
  }

  if (leftPad) {
    leftMost = padLen + sign;
  } else {
    leftMost = sign;
  }

  int currPos;
  // Add filler.
  rightMost = currPos = targetLen - 1;
  if (padLen) {
    int start;
    if (leftPad) {  // Pad to the left.
      start = sign;
    } else {  // Pad to the right
      start = targetLen - padLen;
      rightMost = currPos = start - 1;
    }
    memset(&target[start], filler, padLen);
  }

  // Convert the fraction part and add decimal point.
  if (scale) {
    int low = (currPos - scale);
    for (; currPos > low; currPos--) {
      target[currPos] = (char)((newSource % 10) + '0');
      newSource /= 10;
    }
    target[currPos--] = '.';
  }

  // Convert the integer part.
  for (; currPos >= leftMost; currPos--) {
    target[currPos] = (char)((newSource % 10) + '0');
    newSource /= 10;
  }

  // Add sign.
  if (leadingSign) {
    if (negative)
      target[0] = '-';
    else
      target[0] = '+';
  } else if (negative)
    target[currPos] = '-';

  // Fix the rightmost digit for -2 ** 63.
  if (fixRightMost && target[rightMost] == '7') target[rightMost] = '8';

  if (newSource != 0 || currPos < -1) {  // Sanity check fails.
    return EXE_STRING_OVERFLOW;
  }

  return 0;
};

///////////////////////////////////////////////////////////////////
// function to convert a INTERVAL to a string.
///////////////////////////////////////////////////////////////////
static short convIntervalToAsciiMxcs(char *source, int sourceLen, int leadingPrecision, int fractionPrecision,
                                     short sourceType, char *inTarget, int inTargetLen, short leftPad) {
  short retCode = 0;

  char delimiter[] = "^-^ ::";
  unsigned short maxFieldValue[] = {0, 11, 0, 23, 59, 59};

  int realTargetLen;

  short startField = getIntervalStartField(sourceType);
  short endField = getIntervalEndField(sourceType);

  long value;
  switch (sourceLen) {
    case SQL_SMALL_SIZE: {
      short temp;
      memcpy((char *)&temp, source, sourceLen);
      value = temp;
      break;
    }
    case SQL_INT_SIZE: {
      int temp;
      memcpy((char *)&temp, source, sourceLen);
      value = temp;
      break;
    }
    case SQL_LARGE_SIZE:
      memcpy((char *)&value, source, sourceLen);
      break;
    default:
      return -1;
  }

  char sign = '+';
  if (value < 0) {
    sign = '-';
    value = -value;
  }

  // 1 for -ve sign, each field is 2 + delimiter
  realTargetLen = (sign == '-' ? 1 : 0) + leadingPrecision + (endField - startField) * 3;
  short targetIndex = (short)realTargetLen;
  if (fractionPrecision) realTargetLen += fractionPrecision + 1;  // 1 for '.'

  if (inTargetLen < realTargetLen) {
    return EXE_STRING_OVERFLOW;
  }

  int targetLen = inTargetLen;
  char *target = inTarget;
  if ((leftPad) && (targetLen > realTargetLen)) {
    // left blankpad the target.
    for (int i = 0; i < (targetLen - realTargetLen); i++) {
      target[i] = ' ';
    }

    // switch target and targetLen to be equal to realTargetLen.
    target = inTarget + (targetLen - realTargetLen);
    targetLen = realTargetLen;
  }

  long factor = 1;
  long fieldVal = 0;
  if (fractionPrecision) {
    //    realTargetLen += fractionPrecision + 1; // 1 for '.'
    for (UInt32 i = fractionPrecision; i > 0; i--) {
      factor *= 10;
    }
    fieldVal = value;
    value = value / factor;
    fieldVal -= value * factor;
  }

  while (targetLen < realTargetLen) {
    // cut fraction till it fits. At this point we know that
    // precision = 0 will fit
    fieldVal /= 10;
    fractionPrecision--;
    realTargetLen--;
    if (!fractionPrecision) realTargetLen--;  // remove '.'
  };

  // if target is fixed length, fill target string flushed right
  for (int i = 0; i < (targetLen - realTargetLen); i++, target++) *target = ' ';

  // convert fraction part if any. targetIndex was set earlier
  if (fractionPrecision) {
    targetIndex += 1;  // 1 for '.'
    memset(target + targetIndex, '0', fractionPrecision);
    retCode = convInt64ToAsciiMxcs(target + targetIndex,
                                   fractionPrecision,  // targetLen
                                   (long)fieldVal,
                                   0,    // scale,
                                   '0',  // filler character
                                   0,
                                   1);  // leftPad
    if (retCode) return retCode;

    targetIndex--;
    target[targetIndex] = '.';
  };

  // Now rest of the fields except leading field...
  for (int field = endField; field > startField; field--) {
    int index = field - REC_DATE_YEAR;  // zero-based array index!

    factor = (short)maxFieldValue[index] + 1;
    fieldVal = value;
    value = value / factor;
    fieldVal -= value * factor;
    targetIndex -= 2;

    retCode = convInt64ToAsciiMxcs(target + targetIndex,
                                   2,  // targetLen
                                   (long)fieldVal,
                                   0,    // scale,
                                   '0',  // filler character
                                   0,
                                   1);  // leftPad
    if (retCode) return retCode;

    targetIndex--;
    target[targetIndex] = delimiter[index];
  };

  // leading field
  retCode = convInt64ToAsciiMxcs(target + (sign == '-' ? 1 : 0),
                                 leadingPrecision,  // targetLen
                                 (long)value,
                                 0,    // scale,
                                 ' ',  // filler character
                                 0,
                                 1);  // leftPad
  if (retCode) return retCode;

  if (sign == '-') {
    target[0] = sign;
  }
  //  else
  //  {
  //    target[0] = ' ';
  //  }

  return 0;
};

/////////////////////////////////////////////////////////////////////////////
//
// This method converts given 'source' to 'target' value.
//
// Following conversions are currently supported:
//  Bignum signed or unsigned to fixed ascii string.
//  Interval to fixed ascii string.
//  Fixed ascii string to BigNum signed or unsigned.
//  Fixed ascii string to Interval datatype.
//
//  Conversion from sql data to user string for output:
//  ===================================================
//  source:          pointer to buffer containing source value.
//  sourceLen:       length of data in 'source'
//  sourceType:      FS datatype of source value.
//                   FS datatypes are defined in sqlcli.h and are of the
//                   form: _SQLDT_*.
//                   These datatype are also returned after describe when
//                   SQLDESC_ITEM_ID of SQLDESC_TYPE_FS is used.
//  sourcePrecision: Returned after describe:
//                     For numerics and chars:
//                                    value returned when SQLDESC_PRECISION
//                                    ITEM_ID is retrieved
//                     For intervals: value returned when SQLDESC_INT_LEAD_PREC
//                                    ITEM_ID is retrieved
//  sourceScale:     Returned after describe:
//                     For numerics and chars:
//                                    value returned when SQLDESC_SCALE
//                                    ITEM_ID is retrieved
//                     For intervals: value returned when SQLDESC_PRECISION
//                                    ITEM_ID is retrieved
//
// target:           pointer to buffer where target will be moved in ascii form
// targetLen:        length of target buffer. Must be big enough to hold the
//                   returned value in string format.
//                   For BigNums, targetLen = bignum precision + 1
// targetType:       0
// targetPrecision:  0
// targetScale:      0
//
// flags:            0
//
//  Conversion from user string to sql data for input:
//  ==================================================
//  Save as the previous section with source replacing the target.
//
// ***************************************************************************
//
// MXCS/DBTR callers should set the define NA_MXCS
// before compiling this file.
//
// ***************************************************************************
// Return Code:        0, if conversion was successful.
//                     <sql-error-code>, in case of an error
//
// ***************************************************************************
//
//   This file is maintained by SQL.
//   DO NOT MAKE ANY CHANGES TO THIS FILE ON MXCS/DBTR END without updating
//   the version that is checked in to SQL.
//   Doing that will cause incompatibility and lost code.
//
///////////////////////////////////////////////////////////////////////////////
short convDoItMxcs(char *source, int sourceLen, short sourceType, int sourcePrecision, int sourceScale, char *target,
                   int targetLen, short targetType, int targetPrecision, int targetScale, int flags) {
  short retCode = -1;
  if (sourceType == 0) {
    if ((targetType == REC_NUM_BIG_UNSIGNED) || (targetType == REC_NUM_BIG_SIGNED)) {
      // ascii to bignum conversion
      retCode = convAsciiToBigNumMxcs(target, targetLen, targetType, targetPrecision, targetScale, source, sourceLen);
    } else if ((targetType >= REC_MIN_INTERVAL) && (targetType <= REC_MAX_INTERVAL)) {
      // ascii to interval conversion
      retCode = convAsciiToIntervalMxcs(target, targetLen, targetType, targetPrecision, targetScale, source, sourceLen,
                                        1,  // allowSignInInterval
                                        0);
    }
  } else if (targetType == 0) {
    if ((sourceType == REC_NUM_BIG_UNSIGNED) || (sourceType == REC_NUM_BIG_SIGNED)) {
      retCode = convBigNumToAsciiMxcs(target, targetLen, source, sourceLen, sourcePrecision, sourceScale);
    } else if ((sourceType >= REC_MIN_INTERVAL) && (sourceType <= REC_MAX_INTERVAL)) {
      retCode = convIntervalToAsciiMxcs(source, sourceLen, sourcePrecision, sourceScale, sourceType, target, targetLen,
                                        1);  // leftPad
    }
  }

  return retCode;
}
