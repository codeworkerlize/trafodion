
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         exp_conv.cpp
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

#include <errno.h>
#include <math.h>
#include <stdlib.h>

#include "common/Platform.h"
#include "exp_ieee.h"
#define MathPow(op1, op2, err) pow(op1, op2)
// extern double MathPow(double Base, double Power, short &err);
#define MathLog10(op, err) log10(op)
#include <stdlib.h>

#include "common/BigNumHelper.h"
#include "common/SQLTypeDefs.h"
#include "common/ComMisc.h"
#include "common/NAAssert.h"
#include "common/NLSConversion.h"
#include "common/nawstring.h"
#include "common/str.h"
#include "common/wstr.h"
#include "exp/exp_bignum.h"
#include "exp/exp_clause_derived.h"
#include "exp/exp_datetime.h"
#include "exp/exp_stdh.h"
#include "fenv.h"
#include "float.h"

// forward declarations

static rec_datetime_field getIntervalStartField(int datatype);

long getMaxDecValue(int targetLen);

int getDigitCount(UInt64 value);

void setVCLength(char *VCLen, int VCLenSize, int value);

ex_expr::exp_return_type checkPrecision(long source, int sourceLen, short sourceType, int sourcePrecision,
                                        int sourceScale, short targetType, int targetPrecision, int targetScale,
                                        CollHeap *heap, ComDiagsArea **diagsArea, int flags, char *sourceValue = NULL);

//////////////////////////////////////////////////////////////////
//
// Function to double the value of the varchar length field for
// Unicode related conversion. All unicode related conversions
// involving non-chartype operands reuse the corresponding
// Ascii version. This function is used to adjust the varchar
// length calculated by the Ascii function. 6/3/98
//
//////////////////////////////////////////////////////////////////
void double_varchar_length(char *varCharLen,   // NULL if not a varChar
                           int varCharLenSize  // 0 if not a varChar
) {
  if (varCharLenSize == sizeof(int)) {
    int x;
    str_cpy_all((char *)&x, varCharLen, sizeof(int));
    x *= BYTES_PER_NAWCHAR;
    str_cpy_all(varCharLen, (char *)&x, sizeof(int));
  } else {
    short x;
    str_cpy_all((char *)&x, varCharLen, sizeof(short));
    x *= BYTES_PER_NAWCHAR;
    str_cpy_all(varCharLen, (char *)&x, sizeof(short));
  }
}

//////////////////////////////////////////////////////////////////
//
// A helper function to return charset name
//
/////////////////////////////////////////////////////////////////
static const char *scaleToString(short scale) {
  switch (scale) {
    case SQLCHARSETCODE_ISO88591:
      return "ISO88591";
    case SQLCHARSETCODE_KANJI:
      return "KANJI";
    case SQLCHARSETCODE_KSC5601:
      return "KSC5601";
    case SQLCHARSETCODE_SJIS:
      return "SJIS";
    case SQLCHARSETCODE_UCS2:
      return "UCS2";
    case SQLCHARSETCODE_EUCJP:
      return "EUCJP";
    case SQLCHARSETCODE_BIG5:
      return "BIG5";
    case SQLCHARSETCODE_GB18030:
      return "GB18030";
    case SQLCHARSETCODE_UTF8:
      return "UTF8";
    case SQLCHARSETCODE_MB_KSC5601:
      return "MB_KSC5601";
    case SQLCHARSETCODE_GB2312:
      return "GB2312";
    case SQLCHARSETCODE_GBK:
      return "GBK";
    default:
      return "UNKNOWN";
  }
  return "UNKNOWN";
}
//////////////////////////////////////////////////////////////////
//
// A helper function that multiplies the dvalue by 10 and
// adds the digit. It checks overflow during the computation.
//
// This function returns 0 if not overflow or 1 otherwise.
//
// This function is used by convAsciiToFloat64 to coverting
// a string of digits to double. jdu 6/10/02
//
//////////////////////////////////////////////////////////////////
static int safe_add_digit_to_double(double dvalue,  // Original value
                                    int digit,      // Single digit to be added
                                    double *result) {
  short ov;

  *result = MathReal64Mul(dvalue, (double)10, &ov);

  if (ov != 0)  // result overflowed
    return ov;

  *result = MathReal64Add(*result, (double)digit, &ov);
  return ov;
}

//////////////////////////////////////////////////////////////////
// function to convert a signed BIN (of 2, 4 or 8 bytes) to
// a signed BIN (of 2, 4 or 8 bytes), with multiplication by a
// scaling factor
//
// This is useful when overflows and truncations from scaling
// need to be captured; but it also works with the diagsArea
// error model.
//
// Returns ex_expr::EXPR_OK if (conversion was successful) or (a
// conversion error occurred and the data conversion error flag
// is present).
//
// Returns ex_expr::EXPR_ERROR if (a conversion error occurred) and
// (no data conversion error flag is present).
///////////////////////////////////////////////////////////////////
ex_expr::exp_return_type convBinToBinScaleMult(char *target, short targetType, int targetLen, int targetPrecision,
                                               int targetScale, char *source, short sourceType, int sourceLen,
                                               int sourcePrecision, long scaleFactor, CollHeap *heap,
                                               ComDiagsArea **diagsArea, int *dataConversionErrorFlag, int flags) {
  int tempDataConversionErrorFlag = ex_conv_clause::CONV_RESULT_OK;  // assume success
  ex_expr::exp_return_type rc = ex_expr::EXPR_OK;

  // widen source to Int 64
  long intermediate;

  switch (sourceLen) {
    case SQL_SMALL_SIZE:
      intermediate = *(short *)source;
      break;
    case SQL_INT_SIZE:
      intermediate = *(int *)source;
      break;
    case SQL_LARGE_SIZE:
      intermediate = *(long *)source;
      break;
    default:
      // shouldn't happen
      intermediate = 0;
      break;
  }

  assert(sourceType >= REC_MIN_INTERVAL && sourceType <= REC_MAX_INTERVAL && targetType >= REC_MIN_INTERVAL &&
         targetType <= REC_MAX_INTERVAL);

  // make sure value is small enough to be successfully converted,
  // then convert it else raise an error

  if ((intermediate < LLONG_MAX / scaleFactor) && (intermediate > LLONG_MIN / scaleFactor)) {
    intermediate = intermediate * scaleFactor;
  } else if (dataConversionErrorFlag) {
    if (intermediate > 0) {
      // result too large -- just set it to max
      intermediate = LLONG_MAX;
      tempDataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX;
    } else {
      // result too small -- just set it to min
      intermediate = LLONG_MIN;
      tempDataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN;
    }
  } else {
    ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, 0, targetType, flags);
    return ex_expr::EXPR_ERROR;
  }

  // now move converted intermediate to target

  if (targetLen == SQL_LARGE_SIZE) {
    *(long *)target = intermediate;  // yeah, it's an extra move
    if (dataConversionErrorFlag) *dataConversionErrorFlag = tempDataConversionErrorFlag;
    if (((sourceType >= REC_MIN_INTERVAL) && (sourceType <= REC_MAX_INTERVAL) && (targetType >= REC_MIN_INTERVAL) &&
         (targetType <= REC_MAX_INTERVAL)) &&
        (getIntervalStartField(sourceType) < getIntervalStartField(targetType) ||
         sourcePrecision > targetPrecision))  // truncation is possible
    {
      rc = checkPrecision(intermediate, sourceLen, sourceType, sourcePrecision, targetScale, targetType,
                          targetPrecision, targetScale, heap, diagsArea, flags);
    }
  } else {
    short physicalTargetType = (short)REC_BIN16_SIGNED;
    ConvInstruction index = CONV_BIN64S_BIN16S;

    if (targetLen == SQL_INT_SIZE) {
      physicalTargetType = (short)REC_BIN32_SIGNED;
      index = CONV_BIN64S_BIN32S;
    }

    // In this convDoIt, we're purposely passing in 0 for target precision
    // because we don't want this convDoIt to check for precision validity
    // since this call to convDoIt is converting 2 binary operands, and not
    // intervals.  checkPrecision is called after this convDoIt with the correct
    // interval types
    rc = convDoIt((char *)&intermediate, SQL_LARGE_SIZE, (short)REC_BIN64_SIGNED, sourcePrecision,
                  0,  // sourceScale
                  target, targetLen, physicalTargetType,
                  0,     // target precision
                  0,     // targetScale,
                  NULL,  // varCharLen
                  0,     // varCharLenSize
                  heap, diagsArea, index, dataConversionErrorFlag, flags | CONV_INTERMEDIATE_CONVERSION);

    // if there was no error on the second conversion,
    // then report back any error from the first conversion
    // (see the discussion in convDoIt() case CONV_LARGEDEC_BIN16S
    // for a proof of why this is the correct thing to do)
    if (dataConversionErrorFlag) {
      if (*dataConversionErrorFlag == ex_conv_clause::CONV_RESULT_OK)
        *dataConversionErrorFlag = tempDataConversionErrorFlag;
    }
    if (rc != ex_expr::EXPR_OK) {
      return rc;
    }

    rc = checkPrecision(intermediate, sourceLen, sourceType, sourcePrecision, targetScale, targetType, targetPrecision,
                        targetScale, heap, diagsArea, flags);
  }

  return rc;
}

//////////////////////////////////////////////////////////////////
// function to convert a signed BIN (of 2, 4 or 8 bytes) to
// a signed BIN (of 2, 4 or 8 bytes), with division by a
// scaling factor
//
// This is useful when overflows and truncations from scaling
// need to be captured; but it also works with the diagsArea
// error model.
//
// Returns ex_expr::EXPR_OK if (conversion was successful) or (a
// conversion error occurred and the data conversion error flag
// is present).
//
// Returns ex_expr::EXPR_ERROR if (a conversion error occurred) and
// (no data conversion error flag is present).
///////////////////////////////////////////////////////////////////
ex_expr::exp_return_type convBinToBinScaleDiv(char *target, short targetType, int targetLen, int targetPrecision,
                                              int targetScale, char *source, short sourceType, int sourceLen,
                                              int sourcePrecision, long scaleFactor, CollHeap *heap,
                                              ComDiagsArea **diagsArea, int *dataConversionErrorFlag, int flags) {
  int tempDataConversionErrorFlag = ex_conv_clause::CONV_RESULT_OK;  // assume success
  ex_expr::exp_return_type rc = ex_expr::EXPR_OK;

  // widen source to Int 64
  long intermediate;

  switch (sourceLen) {
    case SQL_SMALL_SIZE:
      intermediate = *(short *)source;
      break;
    case SQL_INT_SIZE:
      intermediate = *(int *)source;
      break;
    case SQL_LARGE_SIZE:
      intermediate = *(long *)source;
      break;
    default:
      // shouldn't happen
      intermediate = 0;
      break;
  }
  assert(sourceType >= REC_MIN_INTERVAL && sourceType <= REC_MAX_INTERVAL && targetType >= REC_MIN_INTERVAL &&
         targetType <= REC_MAX_INTERVAL);

  // divide by the scaleFactor, checking to see if significant
  // truncation occurred

  long intermediate2 = intermediate / scaleFactor;
  long intermediate3 = intermediate2 * scaleFactor;

  if (dataConversionErrorFlag) {
    if (intermediate3 < intermediate) {
      // truncation occurred; result was rounded down
      // (can happen only if intermediate is positive)
      tempDataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN;
    } else if (intermediate3 > intermediate) {
      // truncation occurred; result was rounded up
      // (can happen only if intermediate is negative)
      tempDataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP;
    }
  }

  else {
    // scale truncation occurred - raise error
    if (intermediate3 != intermediate) {
      ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, 0, targetType, flags);
      return ex_expr::EXPR_ERROR;
    }
  }

  intermediate = intermediate2;

  // now move converted intermediate to target

  if (targetLen == SQL_LARGE_SIZE) {
    *(long *)target = intermediate;  // yeah, it's an extra move
    if (dataConversionErrorFlag) *dataConversionErrorFlag = tempDataConversionErrorFlag;
    if (((sourceType >= REC_MIN_INTERVAL) && (sourceType <= REC_MAX_INTERVAL) && (targetType >= REC_MIN_INTERVAL) &&
         (targetType <= REC_MAX_INTERVAL)) &&
        (getIntervalStartField(sourceType) < getIntervalStartField(targetType) ||
         sourcePrecision > targetPrecision))  // truncation is possible
    {
      rc = checkPrecision(intermediate, sourceLen, sourceType, sourcePrecision, targetScale, targetType,
                          targetPrecision, targetScale, heap, diagsArea, flags);
    }
  } else {
    short physicalTargetType = (short)REC_BIN16_SIGNED;
    ConvInstruction index = CONV_BIN64S_BIN16S;

    if (targetLen == SQL_INT_SIZE) {
      physicalTargetType = (short)REC_BIN32_SIGNED;
      index = CONV_BIN64S_BIN32S;
    }

    if (targetLen == SQL_INT_SIZE) physicalTargetType = (short)REC_BIN32_SIGNED;

    // In this convDoIt, we're purposely passing in 0 for target precision
    // because we don't want this convDoIt to check for precision validity
    // since this call to convDoIt is converting 2 binary operands, and not
    // intervals.  checkPrecision is called after this convDoIt with the correct
    // interval types
    rc = convDoIt((char *)&intermediate, SQL_LARGE_SIZE, (short)REC_BIN64_SIGNED, sourcePrecision,
                  0,  // sourceScale
                  target, targetLen, physicalTargetType,
                  0,     // target precision
                  0,     // targetScale,
                  NULL,  // varCharLen
                  0,     // varCharLenSize
                  heap, diagsArea, index, dataConversionErrorFlag, flags | CONV_INTERMEDIATE_CONVERSION);

    // if there was no error on the second conversion,
    // then report back any error from the first conversion
    // (see the discussion in convDoIt() case CONV_LARGEDEC_BIN16S
    // for a proof of why this is the correct thing to do)
    if (dataConversionErrorFlag) {
      if (*dataConversionErrorFlag == ex_conv_clause::CONV_RESULT_OK)
        *dataConversionErrorFlag = tempDataConversionErrorFlag;
    }
    if (rc != ex_expr::EXPR_OK) return rc;

    rc = checkPrecision(intermediate, sourceLen, sourceType, sourcePrecision, targetScale, targetType, targetPrecision,
                        targetScale, heap, diagsArea, flags);
  }
  return rc;
}

//////////////////////////////////////////////////////////////////
// function to convert an long  to an ASCII string
// Trailing '\0' is not set!
//
// For fixed char targets, left pad fillers if leftPad is
// TRUE, or right pad fillers (default mode) otherwise.
//
// convDoIt sets leftPad to TRUE if callers pass in a
// CONV_UNKNOWN_LEFTPAD to convDoIt.
//
// In only a few cases of fixed char, leftPad will be set to TRUE.
// e.g., caller wants to convert 7 -> 07        as in month -> ascii
//                           or  8 -> '    8'   as in sqlci display
///////////////////////////////////////////////////////////////////
ex_expr::exp_return_type convInt64ToAscii(char *target, int targetLen,
                                          int targetPrecision,  // max. characters
                                          int targetScale, long source, int scale, char *varCharLen, int varCharLenSize,
                                          char filler, NABoolean leadingSign, NABoolean leftPad, NABoolean srcIsUInt64,
                                          CollHeap *heap, ComDiagsArea **diagsArea, NABoolean bTrimTailZero = FALSE) {
  // trim tail '0'
  // e.g.
  // select 123.400 from dual;
  // The result will be 123.4 instead of 123.400
  if (bTrimTailZero && scale > 0) {
    if (source != 0) {
      int nCount = 0;
      while (source % 10 == 0 && scale > 0) {
        source /= 10;
        ++nCount;
        --scale;
      }
    } else
      scale = 0;
  }
  int digitCnt = 0;
  int flags = 0;
  NABoolean negative = (srcIsUInt64 ? FALSE : (source < 0));
  NABoolean fixRightMost = FALSE;  // True if need to fix the rightmost digit.

  Int16 trgType;
  if (varCharLenSize)
    trgType = REC_BYTE_V_ASCII;
  else
    trgType = REC_BYTE_F_ASCII;

  int padLen = targetLen;
  int targetChars = targetLen;
  int requiredDigits = 0;
  int leftMost;   // leftmost digit.
  int rightMost;  // rightmost digit.
  int sign = 0;

  //  long newSource = (negative ? -source : source);
  UInt64 newSource = 0;

  if (targetPrecision) {
    // target has a max. number of characters which may be smaller than
    // the target length
    targetChars = targetPrecision;
    padLen = targetPrecision;
    assert(targetLen >= targetPrecision);
  }

  if ((negative) && (source == 0x8000000000000000LL))  // = -2 ** 63
  {
    newSource = 0x7fffffffffffffffLL;
    //             123456789012345
    digitCnt = 19;
    fixRightMost = TRUE;
  } else {
    newSource = (UInt64)(negative ? -source : source);
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
    ExRaiseDetailSqlError(heap, diagsArea, EXE_STRING_OVERFLOW, (char *)&source, sizeof(long), REC_BIN64_SIGNED, scale,
                          trgType, flags, targetLen, targetScale, targetPrecision);
    return ex_expr::EXPR_ERROR;
  }

  if (varCharLenSize) {
    // we do not pad. Instead, we adjust the targetLen
    leftPad = FALSE;
    targetChars -= padLen;
    targetLen = targetChars;
    padLen = 0;
  } else if (targetChars < targetLen) {
    // Fill out the unused bytes after targetChars with blanks.
    str_pad(&target[targetChars], targetLen - targetChars, ' ');
  }

  if (leftPad) {
    leftMost = padLen + sign;
  } else {
    leftMost = sign;
  }

  int currPos;
  // Add filler.
  rightMost = currPos = targetChars - 1;
  if (padLen) {
    int start;
    if (leftPad) {  // Pad to the left.
      start = sign;
    } else {  // Pad to the right
      start = targetChars - padLen;
      rightMost = currPos = start - 1;
    }
    str_pad(&target[start], padLen, filler);
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
    ExRaiseDetailSqlError(heap, diagsArea, EXE_STRING_OVERFLOW, (char *)&source, sizeof(long), REC_BIN64_SIGNED, scale,
                          trgType, flags, targetLen, targetScale);
    return ex_expr::EXPR_ERROR;
  }

  // Set varchar length field for varchar.
  if (varCharLenSize)
    if (varCharLenSize == sizeof(int))
      str_cpy_all(varCharLen, (char *)&targetLen, sizeof(int));
    else {
      short VCLen = (short)targetLen;
      str_cpy_all(varCharLen, (char *)&VCLen, sizeof(short));
    };

  return ex_expr::EXPR_OK;
};

//////////////////////////////////////////////////////////////////
// function to convert an FLOAT64  to an ASCII string
// Trailing '\0' is not set!
// This routine assumes that targetLen is at least
// SQL_REAL_MIN_DISPLAY_SIZE:
// 1 byte sign
// 1 byte for digit in front of decimal point
// 1 byte decimal point
// 1 byte for at least one digit after decimal point
// 5 bytes for exponent (E+DDD)
///////////////////////////////////////////////////////////////////
short convFloat64ToAscii(char *target, int targetLen,
                         int targetPrecision,  // max. characters
                         int targetScale, double source,
                         // maximum # of fraction digits
                         int digits, char *varCharLen, int varCharLenSize, NABoolean leftPad) {
  short err = 0;

  int displaySize = digits + 8;  // Mantissa = digits + 3, E = 1, Exponent = 4
  int targetChars = targetLen;
  assert(displaySize <= SQL_DOUBLE_PRECISION_DISPLAY_SIZE);
  char tempTarget[SQL_DOUBLE_PRECISION_DISPLAY_SIZE + 1];
  // char format[8];

  if (targetPrecision) {
    // target allows fewer characters than targetLen
    assert(targetLen >= targetPrecision);
    targetChars = targetPrecision;
  }

  // the fraction has always between 1 and "digits" digits
  if ((targetChars - 8) < digits) {
    digits = targetChars - 8;
    if (digits < 1) return -1;
  }

  int usedTargetLen = MINOF(displaySize, targetChars);

  if (std::isinf(source)) {
    source = 0;
  }

  double absSource = source;
  NABoolean neg = FALSE;
  if (source < 0) {
    absSource = -source;
    neg = TRUE;
  }

  long expon = 0;
  long intMantissa = 0;
  NABoolean expPos = TRUE;
  if (absSource > 0) {
    double logTen = MathLog10(absSource, err);
    if (err) return -1;

    if (logTen >= 0) {
      expPos = TRUE;
    } else {
      logTen = -logTen;
      expPos = FALSE;
    };

    // replace with a faster way below (4 lines)
    //      while (logTen > 0)
    //	{
    //	  expon++;
    //	  logTen -= 1;
    //	}
    expon = (int)logTen;
    logTen -= expon;
    if (logTen > 0.0) expon++;

    if ((expPos) && (logTen != 0)) expon--;

    NABoolean reduceExpon = FALSE;
    short reduceExponBy = 0;
    if (expon >= DBL_MAX_10_EXP) {
      // if expon is greater than MAX exponent allowed, then reduce it
      // the diff between expon and DBL_MAX_10_EXP.
      // This is needed so the next MathPow call doesn't return
      // an error when it tries to do   10 ** DBL_MAX_10_EXP
      // (which will make it greater than the max double value).
      reduceExponBy = (short)(expon - DBL_MAX_10_EXP + 1);
      expon -= reduceExponBy;
      reduceExpon = TRUE;
    }
    double TenPowerExpon = ((expPos == FALSE) ? MathPow(10.0, (double)expon, err) : MathPow(10.0, (double)-expon, err));
    if (err) return -1;

    if (std::isinf(TenPowerExpon)) return -1;
    double mantissa = absSource * TenPowerExpon;

    if (reduceExpon) {
      // now fix mantissa by multiplying or dividing by 10.
      double tempMathPow = MathPow(10.0, reduceExponBy, err);
      if (std::isinf(tempMathPow)) return -1;
      if (expPos == FALSE)
        mantissa = mantissa * tempMathPow;
      else
        mantissa = mantissa / tempMathPow;

      if (err) return -1;

      // and increase expon to its original value
      expon += reduceExponBy;
    }

    // The multiplication and casting to an long below has caused several
    // problems on Linux.  Performing the multiplication and casting to long
    // on seperate lines seems to fix this problem.
    double doubleIntMantissa = MathPow(10.0, (double)digits, err);
    if (std::isinf(doubleIntMantissa)) return -1;
    doubleIntMantissa = mantissa * doubleIntMantissa;

    if (err) return -1;

    intMantissa = (long)doubleIntMantissa;
  }

  short error;
  error = convInt64ToAscii(tempTarget, digits + 3, 0, targetScale, (neg ? -intMantissa : intMantissa), digits, NULL, 0,
                           ' ', neg, TRUE, FALSE, NULL, NULL);
  if (error) return -1;

  // If the mantissa is 0, *or* if the digit before the decimal point is still
  // a space char (i.e. mantissa is < 0), then be sure to initialize the digit
  // to 0 - callers are dependent on the fact that this space is initialized.

  if ((intMantissa == 0) || (tempTarget[1] == ' ')) {
    // add a 0 before the decimal point of mantissa
    tempTarget[1] = '0';
  }

  tempTarget[digits + 3] = 'E';
  error = convInt64ToAscii(&tempTarget[digits + 4], 4, 0, targetScale, (expPos ? expon : -expon), 0, NULL, 0, '0', TRUE,
                           TRUE, FALSE, NULL, NULL);
  if (error) return -1;

  char *pch = tempTarget;
  while (*pch == ' ') {
    usedTargetLen--;
    pch++;
  }

  if (varCharLenSize) {
    // the target is a varChar. Just move the data left adjusted and
    // set the length field.
    str_cpy_all(target, pch, usedTargetLen);

    if (varCharLenSize == sizeof(int))
      str_cpy_all(varCharLen, (char *)&usedTargetLen, sizeof(int));
    else {
      short VCLen = (short)usedTargetLen;
      str_cpy_all(varCharLen, (char *)&VCLen, sizeof(short));
    };
  } else {
    // if target is larger than usedTargetLen, fill in blanks.
    int padLen = targetLen - usedTargetLen;
    if (leftPad) {
      str_pad(target, padLen, ' ');
      str_cpy_all(&target[padLen], pch, usedTargetLen);
    } else {
      str_cpy_all(target, pch, usedTargetLen);
      str_pad(&target[usedTargetLen], padLen, ' ');
    }
  };
  return 0;
}

//////////////////////////////////////////////////////////////////
// function to convert an long  to a Big Num
///////////////////////////////////////////////////////////////////
ex_expr::exp_return_type convInt64ToBigNum(char *target, int targetLen, long source, NAMemory *heap,
                                           ComDiagsArea **diagsArea, int flags) {
  short retCode = BigNumHelper::ConvInt64ToBigNumWithSignHelper(targetLen, source, target, FALSE);
  if (retCode == -1) {
    // target is not long enough - overflow
    ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, (char *)&source, sizeof(long), REC_BIN64_SIGNED, 0,
                          REC_NUM_BIG_SIGNED, flags);
    return ex_expr::EXPR_ERROR;
  };

  return ex_expr::EXPR_OK;
};

//////////////////////////////////////////////////////////////////
// function to convert a UInt64 to a Big Num
///////////////////////////////////////////////////////////////////
ex_expr::exp_return_type convUInt64ToBigNum(char *target, int targetLen, UInt64 source, NAMemory *heap,
                                            ComDiagsArea **diagsArea, int flags) {
  short retCode = BigNumHelper::ConvInt64ToBigNumWithSignHelper(targetLen, source, target, TRUE);
  if (retCode == -1) {
    // target is not long enough - overflow
    ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, (char *)&source, sizeof(long), REC_BIN64_UNSIGNED, 0,
                          REC_NUM_BIG_SIGNED, flags);
    return ex_expr::EXPR_ERROR;
  };

  return ex_expr::EXPR_OK;
};

//////////////////////////////////////////////////////////////////
// function to convert BIGNUM to LARGEDEC
///////////////////////////////////////////////////////////////////

ex_expr::exp_return_type convBigNumToLargeDec(char *target, int targetLen, char *source, int sourceLen, NAMemory *heap,
                                              ComDiagsArea **diagsArea, int flags) {
  // Convert the Big Num (with sign) into a BCD representation by calling
  // a BigNumHelper method
  short retCode = BigNumHelper::ConvBigNumWithSignToBcdHelper(sourceLen, targetLen, source, target, heap);
  if (retCode == -1) {
    // target is not long enough - overflow
    ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, REC_NUM_BIG_SIGNED, 0,
                          REC_DECIMAL_LSE, flags);
    return ex_expr::EXPR_ERROR;
  };

  return ex_expr::EXPR_OK;
}

//////////////////////////////////////////////////////////////////
// function to convert a LARGEDEC  to an ASCII string
// Trailing '\0' is not set!
//
// For fixed char target, left pad blanks if leftPad is TRUE, or
// right pad blanks otherwise.
//
// For varchar target, left-justify string without blank pads
// to the right.
//
// convDoIt sets leftPad to TRUE if callers pass in a
// CONV_UNKNOWN_LEFTPAD to convDoIt.
///////////////////////////////////////////////////////////////////
ex_expr::exp_return_type convLargeDecToAscii(char *target, int targetLen,
                                             int targetPrecision,  // max chars
                                             char *source, int sourceLen, int sourceScale, char *varCharLen,
                                             int varCharLenSize, NABoolean leftPad, CollHeap *heap,
                                             ComDiagsArea **diagsArea) {
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
  int flags = 0;
  int targetLenInChars = (targetPrecision ? targetPrecision : targetLen);
  Int16 trgType;
  if (varCharLenSize)
    trgType = REC_BYTE_V_ASCII;
  else
    trgType = REC_BYTE_F_ASCII;

  if (source[0] < 2) {
    unsigned short realSource[256];
    str_cpy_all((char *)realSource, source, sourceLen);

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

  if (varCharLenSize) leftPad = FALSE;

  // skip leading zeros; start with index == 1; index == 0 is sign
  // stop looking one position before the decimal point. This
  // position is (sourceLen - sourceScale - 1) when scale is 0.
  // When scale > 0, then position is (sourceLen - sourceScale) because
  // we want to convert to '.001' without heading '0'.
  int currPos = 1;
  int startPos = sourceLen - sourceScale - (sourceScale == 0 ? 1 : 0);
  while ((currPos < startPos) && source[currPos] == 0) currPos++;

  int padLen = targetLenInChars;
  if (source[0] == '-') padLen--;
  int requiredDigits = sourceLen - currPos;  // - sourceScale;

  padLen -= requiredDigits;

  if (sourceScale) padLen--;  // decimal point

  if (padLen < 0) {
    // target string is not long enough - overflow
    ExRaiseDetailSqlError(heap, diagsArea, EXE_STRING_OVERFLOW, source, sourceLen, REC_DECIMAL_LSE, sourceScale,
                          trgType, flags);
    return ex_expr::EXPR_ERROR;
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
  for (i = 0; i < (requiredDigits - sourceScale); i++, targetPos++, currPos++)
    target[targetPos] = source[currPos] + '0';

  // if we have a scale, add decimal point and some digits
  if (sourceScale) {
    target[targetPos++] = '.';
    for (i = 0; i < sourceScale; i++, targetPos++, currPos++) target[targetPos] = source[currPos] + '0';
  };

  // Right pad blanks for fixed char.
  if (!varCharLenSize)
    while (targetPos < targetLen) target[targetPos++] = ' ';

  // set length field for variable length targets
  if (varCharLenSize) {
    if (varCharLenSize == sizeof(int))
      str_cpy_all(varCharLen, (char *)&targetPos, sizeof(int));
    else {
      short VCLen = (short)targetPos;
      str_cpy_all(varCharLen, (char *)&VCLen, sizeof(short));
    };
  };

  return ex_expr::EXPR_OK;
};

//////////////////////////////////////////////////////////////////
// function to convert a BIGNUM  to an ASCII string
// Trailing '\0' is not set!
//
// This function first converts the BIGNUM to an intermediate
// LARGEDEC, then calls the previous function convLargeDecToAscii()
///////////////////////////////////////////////////////////////////
ex_expr::exp_return_type convBigNumToAscii(char *target, int targetLen,
                                           int targetPrecision,  // max chars
                                           char *source, int sourceLen, int sourcePrecision, int sourceScale,
                                           char *varCharLen, int varCharLenSize, NABoolean leftPad, NAMemory *heap,
                                           ComDiagsArea **diagsArea, int flags) {
  const int tempSpaceLen = 257;  // one extra byte for the sign.
  char tempIntermediateLargeDec[tempSpaceLen];
  char *intermediateLargeDec = tempIntermediateLargeDec;

  // If heap exists, allocate temp through it.  Otherwise use static temp space.
  int targetLenToUse = tempSpaceLen;
  if (heap) {
    targetLenToUse = MAXOF(targetLenToUse, sourcePrecision + 1);  // + 1 for sign
    intermediateLargeDec = new (heap) char[targetLenToUse];
  } else {
    if (targetLen > tempSpaceLen) return ex_expr::EXPR_ERROR;
  }

  if (convBigNumToLargeDec(intermediateLargeDec, targetLenToUse, source, sourceLen, heap, diagsArea,
                           flags | CONV_INTERMEDIATE_CONVERSION) != ex_expr::EXPR_OK) {
    return ex_expr::EXPR_ERROR;
  }

  ex_expr::exp_return_type retValue =
      convLargeDecToAscii(target, targetLen, targetPrecision, intermediateLargeDec, targetLenToUse, sourceScale,
                          varCharLen, varCharLenSize, leftPad, heap, diagsArea);

  if (heap) NADELETEBASIC(intermediateLargeDec, (heap));

  return retValue;
};

// convDatetimeToAscii() ============================================
// This function converts datetime values to ASCII string values in
// the DEFAULT format (yyyy-mm-dd hh:mm:ss.msssss).  Only those fields
// present in the datetime value are formated. If the datetime value
// has a fraction portion of the SECOND field, it will be formatted
// according to how much space is available in the destination string
// value.  If the target is a fixed size, available trailing space is
// padded with ' ' (space) characters.
//
// This method has been modified as part of the MP Datetime
// Compatibility project.  It has been modified to handle non-standard
// datetime types and all combinations of conversions.
// ===================================================================
//
ex_expr::exp_return_type convDatetimeToAscii(char *target, int targetLen,
                                             int targetPrecision,  // tgt length in char or 0
                                             char *source, int sourceLen, REC_DATETIME_CODE code,
                                             short fractionPrecision, char *varCharLen, int varCharLenSize,
                                             NABoolean leftPad, CollHeap *heap, ComDiagsArea **diagsArea) {
  // Declare attributes to make use of ExpDatetime convDatetimeToASCII
  //
  ExpDatetime srcDatetimeOpType;
  int targetLenInChars = (targetPrecision ? targetPrecision : targetLen);

  // Setup attribute for the source.
  //
  srcDatetimeOpType.setPrecision(code);
  srcDatetimeOpType.setScale(fractionPrecision);

  // Convert the datetime value to ASCII in the DEFAULT format.
  //
  int realTargetLen = srcDatetimeOpType.convDatetimeToASCII(
      source, target, targetLenInChars, ExpDatetime::DATETIME_FORMAT_DEFAULT, NULL, heap, diagsArea);

  if (realTargetLen < 0) {
    return ex_expr::EXPR_ERROR;
  }

  // if target is fixed length, leftpad or right pad.
  //
  if ((!varCharLenSize) && (realTargetLen < targetLen)) {
    if (leftPad && realTargetLen < targetLenInChars) {
      // right shift result
      int i;
      for (i = 0; i < realTargetLen; i++) {
        target[targetLenInChars - (i + 1)] = target[realTargetLen - (i + 1)];
        //	    target[targetLen - realTargetLen + i] = target[i];
      }

      // and blankpad on the left
      for (i = 0; i < (targetLenInChars - realTargetLen); i++) {
        target[i] = ' ';
      }
      realTargetLen = targetLenInChars;
    }
    // blankpad on the right
    if (realTargetLen < targetLen) {
      for (int i = realTargetLen; i < targetLen; i++) target[i] = ' ';
    }
  }

  // set length field for variable length targets
  //
  if (varCharLenSize) {
    if (varCharLenSize == sizeof(int)) {
      str_cpy_all(varCharLen, (char *)&realTargetLen, sizeof(int));
    } else {
      short VCLen = (short)realTargetLen;
      str_cpy_all(varCharLen, (char *)&VCLen, sizeof(short));
    }
  }

  return ex_expr::EXPR_OK;
}

// convAsciiToDatetime() ============================================
// This function converts an ASCII string in any of the three
// (DEFAULT, EUROPEAN, or USA) formats, to the given datetime type.
// The string must have the same fields as the given datetime type.
// If the string has a fractional portion of the SECOND field and the
// datetime type has no fractional precision, the fraction portion of
// the string is ignored.  If the datetime type has a fractional
// precision and the string has no fraction protion, then the fraction
// is set to zero.  If the string has a fraction portion and the
// datetime type has a fractional precision, then the fraction portion
// of the string is scaled (up or down) to match the datetime type.
//
// This method has been modified as part of the MP Datetime
// Compatibility project.  It has been modified to handle non-standard
// datetime types and all combinations of conversions.
//
// This method has been modified again as an interface to call
// methods in expDatetime class as part of the effort to improve
// IMPORT conversion performance.
// ===================================================================
//
ex_expr::exp_return_type convAsciiToDatetime(char *target, int targetLen, REC_DATETIME_CODE code,
                                             short fractionPrecision, char *source, int sourceLen, CollHeap *heap,
                                             ComDiagsArea **diagsArea, int flags) {
  // Declare attributes to make use of ExpDatetime convAsciiToDatetime
  //
  ExpDatetime dstOpType;
  short ret;

  // Setup attribute for the destination.
  //
  dstOpType.setPrecision(code);
  dstOpType.setScale(fractionPrecision);

  ret = dstOpType.convAsciiToDatetime(source, sourceLen, target, targetLen, ExpDatetime::DATETIME_FORMAT_NONE, heap,
                                      diagsArea, flags);

  if (ret < 0) return ex_expr::EXPR_ERROR;

  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type convUnicodeToDatetime(char *target, int targetLen, REC_DATETIME_CODE code,
                                               short fractionPrecision,
                                               char *source,   // actually is NAWchar *
                                               int sourceLen,  // source length in bytes
                                               CollHeap *heap, ComDiagsArea **diagsArea) {
  const NAWcharBuf wBuf((NAWchar *)source, sourceLen / BYTES_PER_NAWCHAR, heap);
  charBuf *cBuf = NULL;

  cBuf = unicodeToISO88591(wBuf, heap, cBuf);

  if (cBuf) {
    int convertedLen = cBuf->getStrLen();

    if (convertedLen == sourceLen / BYTES_PER_NAWCHAR) {
      ex_expr::exp_return_type ok = convAsciiToDatetime(target, targetLen, code, fractionPrecision,
                                                        (char *)cBuf->data(), cBuf->getStrLen(), heap, diagsArea, 0);

      NADELETE(cBuf, charBuf, heap);

      return ok;
    }
  }

  char hexstr[MAX_OFFENDING_SOURCE_DATA_DISPLAY_LEN];
  ExRaiseSqlError(heap, diagsArea, EXE_CONVERT_DATETIME_ERROR, NULL, NULL, NULL, NULL,
                  stringToHex(hexstr, sizeof(hexstr), source, sourceLen));

  return ex_expr::EXPR_ERROR;
}

// convDatetimeDatetime() ============================================
// This function supports any datetime to datetime conversions with
// missing leading fields coming from the current timestamp and
// missing trailing fields being padded with their minimum value (1
// for date fields and 0 for time fields).  Note that the compiler
// (type synthesis) only allows conversions in which the source and
// destination share some fields.  If the dataConversionErrorFlag is
// non-NULL and data is lost when scaling the fractional portion of
// the source value to match the destination fractional precision,
// then the dataConversionErrorFlag is set to
// CONV_RESULT_ROUNDED_DOWN.
//
// This method has been modified as part of the MP Datetime
// Compatibility project.  It has been modified to handle non-standard
// datetime types and all combinations of conversions.
// ===================================================================
//
ex_expr::exp_return_type convDatetimeDatetime(char *target, int targetLen, REC_DATETIME_CODE targetCode,
                                              int targetScale, char *source, int sourceLen,
                                              REC_DATETIME_CODE sourceCode, int sourceScale, CollHeap *heap,
                                              ComDiagsArea **diagsArea, short validateFlag,
                                              int *dataConversionErrorFlag) {
  if (dataConversionErrorFlag)
    // assume success
    *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_OK;

  // Declare attributes to make use of ExpDatetime convDatetimeDatetime
  //
  ExpDatetime srcDatetimeOpType;

  // Setup attribute for the source.
  //
  srcDatetimeOpType.setPrecision(sourceCode);
  srcDatetimeOpType.setScale((short)sourceScale);

  rec_datetime_field dstStartField;
  rec_datetime_field dstEndField;

  // Get the start and end fields of the given Datetime type.
  //
  if (ExpDatetime::getDatetimeFields(targetCode, dstStartField, dstEndField) != 0) {
    ExRaiseSqlError(heap, diagsArea, EXE_INTERNAL_ERROR);
    return ex_expr::EXPR_ERROR;
  }

  // init variable roundedDown to FALSE.
  // case 10-020917-7718 RG: Build MX020910, Select incorrect rows with
  // param '00:00:10' in predicate.
  //
  // With optimization turned on (c89, level 2), this variable is not initialized.
  // Consequently, it can take a value which is not 0 (TRUE). Since this flag may not
  // get set inside srcDatetimeOpType.convDatetimeDatetime(), this var
  // maintains this random value after the call. This incorrectly sets the
  // dataConversionErroFlag to ex_conv_clause::CONV_RESULT_ROUNDED_DOWN, and can turn
  // off the range-serach exclusion flag. As a result, more rows are turned.
  //
  NABoolean roundedDown = FALSE;

  // Convert the source datetime value to the destination datetime
  // type.
  //
  if (srcDatetimeOpType.convDatetimeDatetime(source, dstStartField, dstEndField, (short)targetScale, target, targetLen,
                                             validateFlag, &roundedDown) != 0) {
    char hexstr[MAX_OFFENDING_SOURCE_DATA_DISPLAY_LEN];
    ExRaiseSqlError(heap, diagsArea, EXE_CONVERT_DATETIME_ERROR, NULL, NULL, NULL, NULL,
                    stringToHex(hexstr, sizeof(hexstr), source, sourceLen));
    return ex_expr::EXPR_ERROR;
  }

  // roundedDown is TRUE when data is lost when scaling the source
  // fractional portion of the SECOND field to match the destination
  // fractional precision.
  //
  if (roundedDown == TRUE && dataConversionErrorFlag)
    *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN;

  return ex_expr::EXPR_OK;
};

///////////////////////////////////////////////////////////////////
// function to convert an ASCII string to double
// The function assumes that source is at least
// sourceLen long. Trailing '\0' is not recongnized
///////////////////////////////////////////////////////////////////
ex_expr::exp_return_type convAsciiToFloat64(char *target, char *source, int sourceLen, CollHeap *heap,
                                            ComDiagsArea **diagsArea, int flags) {
  short err = 0;

  double ptarget;

  // skip leading and trailing blanks and adjust source and sourceLen
  // accordingly
  int i = 0;
  for (; i < sourceLen && *source == ' '; i++) source++;

  if (i == sourceLen) {
    if ((target) && (flags & CONV_TREAT_ALL_SPACES_AS_ZERO)) {
      ptarget = 0;
      str_cpy_all(target, (char *)&ptarget, sizeof(double));
      return ex_expr::EXPR_OK;
    }

    // string contains only blanks.
    char hexstr[MAX_OFFENDING_SOURCE_DATA_DISPLAY_LEN];
    ExRaiseSqlError(heap, diagsArea, EXE_CONVERT_STRING_ERROR, NULL, NULL, NULL, NULL,
                    stringToHex(hexstr, sizeof(hexstr), source, sourceLen));
    return ex_expr::EXPR_ERROR;
  };

  sourceLen -= i;

  // we know that we found at least one non-blank character.
  // so we just decrement sourceLen till we scanned all
  // trailing blanks
  while (source[sourceLen - 1] == ' ') {
    sourceLen--;
  };

  enum State { START, SKIP_BLANKS, MANT_BEFORE_SIGN, MANT_AFTER_SIGN, EXP_START, EXPONENT, ERROR };

  int cp = 0;
  double mantissa = 0;
  double exponent = 0;
  NABoolean negMantissa = FALSE;
  NABoolean negExponent = FALSE;
  int numScale = 0;
  NABoolean validNum = FALSE;

  State state = START;
  NABoolean skipChar;
  while (cp < sourceLen) {
    skipChar = FALSE;
    //      cout << "state = " << state << endl;

    switch (state) {
      case START:
        if ((source[cp] == '+') || (source[cp] == '-')) {
          if (source[cp] == '-') negMantissa = TRUE;
          state = SKIP_BLANKS;
          skipChar = TRUE;
        } else
          state = MANT_BEFORE_SIGN;
        break;

      case SKIP_BLANKS:
        if (source[cp] != ' ')
          state = MANT_BEFORE_SIGN;
        else
          skipChar = TRUE;
        break;

      case MANT_BEFORE_SIGN:
        if (source[cp] == '.') {
          state = MANT_AFTER_SIGN;
        } else if ((source[cp] == 'E') || (source[cp] == 'e')) {
          validNum = FALSE;
          state = EXP_START;
        } else if ((source[cp] < '0') || (source[cp] > '9'))
          state = ERROR;
        else {
          if (safe_add_digit_to_double(mantissa, source[cp] - '0', &mantissa) != 0) {
            ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, REC_BYTE_F_ASCII,
                                  SQLCHARSETCODE_ISO88591, REC_FLOAT64, flags);
            return ex_expr::EXPR_ERROR;
          }
          validNum = TRUE;
        }
        skipChar = TRUE;
        break;

      case MANT_AFTER_SIGN:
        if ((source[cp] == 'E') || (source[cp] == 'e')) {
          if (NOT validNum)
            state = ERROR;
          else {
            validNum = FALSE;
            state = EXP_START;
          }
        } else if ((source[cp] < '0') || (source[cp] > '9'))
          state = ERROR;
        else {
          numScale++;
          if (safe_add_digit_to_double(mantissa, source[cp] - '0', &mantissa) != 0) {
            ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, REC_BYTE_F_ASCII,
                                  SQLCHARSETCODE_ISO88591, REC_FLOAT64, flags);
            return ex_expr::EXPR_ERROR;
          }
          validNum = TRUE;
        }
        skipChar = TRUE;
        break;

      case EXP_START: {
        if ((source[cp] == '+') || (source[cp] == '-')) {
          if (source[cp] == '-') negExponent = TRUE;
          skipChar = TRUE;
        }
        state = EXPONENT;
      } break;

      case EXPONENT: {
        if ((source[cp] < '0') || (source[cp] > '9'))
          state = ERROR;
        else {
          if (safe_add_digit_to_double(exponent, source[cp] - '0', &exponent) != 0) {
            ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, REC_BYTE_F_ASCII,
                                  SQLCHARSETCODE_ISO88591, REC_FLOAT64, flags);
            return ex_expr::EXPR_ERROR;
          }
          validNum = TRUE;
        }
        skipChar = TRUE;
      } break;

      case ERROR: {
        // string contains only blanks.
        char hexstr[MAX_OFFENDING_SOURCE_DATA_DISPLAY_LEN];
        ExRaiseSqlError(heap, diagsArea, EXE_CONVERT_STRING_ERROR, NULL, NULL, NULL, NULL,
                        stringToHex(hexstr, sizeof(hexstr), source, sourceLen));
        return ex_expr::EXPR_ERROR;
      }; break;

      default:
        state = ERROR;
        break;

    }  // switch state

    if ((state != ERROR) && (skipChar)) cp++;
  }  // while

  if (NOT validNum) {
    char hexstr[MAX_OFFENDING_SOURCE_DATA_DISPLAY_LEN];
    ExRaiseSqlError(heap, diagsArea, EXE_CONVERT_STRING_ERROR, NULL, NULL, NULL, NULL,
                    stringToHex(hexstr, sizeof(hexstr), source, sourceLen));
    return ex_expr::EXPR_ERROR;
  };

  int save_errno = errno;
  errno = 0;
  ptarget = strtod(source, NULL);
  if (errno == ERANGE) {
    ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, REC_BYTE_F_ASCII,
                          SQLCHARSETCODE_ISO88591, REC_FLOAT64, flags);
    errno = save_errno;
    return ex_expr::EXPR_ERROR;
  }

  errno = save_errno;
  if (target) {
    str_cpy_all(target, (char *)&ptarget, sizeof(double));
  }

  // conversion was OK
  return ex_expr::EXPR_OK;
};

NABoolean isNegative(char *source, int sourceLen, int &currPos) {
  // skip leading blanks and look for leading sign
  NABoolean done = FALSE;
  NABoolean negative = FALSE;
  while ((NOT done) && (currPos < sourceLen)) {
    if ((source[currPos] == '+') || (source[currPos] == '-')) {
      negative = (source[currPos] == '-');
      done = TRUE;
      currPos++;  // skip pass sign
    } else if (source[currPos] == ' ')
      currPos++;
    else
      done = TRUE;
  }

  return negative;
}

ex_expr::exp_return_type convAsciiToUInt64base(UInt64 &target, int targetScale, char *source, int sourceLen,
                                               CollHeap *heap, ComDiagsArea **diagsArea, int flags) {
  NABoolean SignFound = FALSE;  // did we find a sign (+/-)
  NABoolean DigitsAllowed = TRUE;
  NABoolean negative = FALSE;    // default is a positive value
  NABoolean pointFound = FALSE;  // did we find a decimal point
  int currPos = 0;               // current position in the string
  short digitCnt = 0;            // digits found so far
  int sourceScale = 0;           // default is a scale of 0

  target = 0;
  while (currPos < sourceLen) {
    if ((source[currPos] >= '0') && (source[currPos] <= '9')) {  // process digits
      if (!DigitsAllowed) {
        // syntax error in the input
        char hexstr[MAX_OFFENDING_SOURCE_DATA_DISPLAY_LEN];
        ExRaiseSqlError(heap, diagsArea, EXE_CONVERT_STRING_ERROR, NULL, NULL, NULL, NULL,
                        stringToHex(hexstr, sizeof(hexstr), source, sourceLen));

        return ex_expr::EXPR_ERROR;
      }
      digitCnt++;

      // if we've read enough source characters to achieve the needed
      // targetScale, then stop appending digits to the target value.
      //   They would only be thrown away later.
      if (!pointFound || sourceScale < targetScale) {  // this digit needs to be processed.

        // if we found a decimal point before, another digit increases
        // the source scale
        if (pointFound) sourceScale++;

        int thisDigit = source[currPos] - '0';
        if (digitCnt > 18) {
          // check overflow/underflow
          if (target > (ULLONG_MAX / 10)) {  // next power of 10 causes an overflow
            ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, REC_BYTE_F_ASCII,
                                  SQLCHARSETCODE_ISO88591, REC_BIN64_SIGNED, flags);
            return ex_expr::EXPR_ERROR;
          }
          target *= 10;
          long tempMax = ULLONG_MAX;
          tempMax -= thisDigit;
          // if (target > (LLONG_MAX - thisDigit));
          if (target > tempMax) {  // adding this digit causes an overflow
            ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, REC_BYTE_F_ASCII,
                                  SQLCHARSETCODE_ISO88591, REC_BIN64_SIGNED, flags);
            return ex_expr::EXPR_ERROR;
          }
          target = target + (long)thisDigit;
        }  // if digitCnt > 18, check overflow or underflow
        else {
          // at this point we never consider overflow
          // and target is alway potitive or absolute value
          target = target * 10 + thisDigit;
        }
      }                                   // fi ( sourceScale < targetScale )
    } else if (source[currPos] == ' ') {  // process blanks
      if (digitCnt || pointFound)
        // we found already some digits or a decimal point. A blank now
        // means, that from now on we should not see any digits anymore.
        DigitsAllowed = FALSE;
    } else if ((source[currPos] == '+') || (source[currPos] == '-')) {  // process sign
      // we found already a sign or we found already digits
      // or we found a point already.
      // A sign is an error now!
      char hexstr[MAX_OFFENDING_SOURCE_DATA_DISPLAY_LEN];
      ExRaiseSqlError(heap, diagsArea, EXE_CONVERT_STRING_ERROR, NULL, NULL, NULL, NULL,
                      stringToHex(hexstr, sizeof(hexstr), source, sourceLen));

      return ex_expr::EXPR_ERROR;
    } else if (source[currPos] == '.') {  // process decimal point
      if (pointFound || !DigitsAllowed) {
        // we found a second decimal point. This is an error
        // we are already not allowing anymore digits because of illegal
        // blank space(s)
        char hexstr[MAX_OFFENDING_SOURCE_DATA_DISPLAY_LEN];
        ExRaiseSqlError(heap, diagsArea, EXE_CONVERT_STRING_ERROR, NULL, NULL, NULL, NULL,
                        stringToHex(hexstr, sizeof(hexstr), source, sourceLen));

        return ex_expr::EXPR_ERROR;
      }
      pointFound = TRUE;
    } else {  // process illegal characters
      // if the illegal character is a 'E' or 'e', the input
      // might be a float value. Try to convert the input
      // to double and then to long
      if ((source[currPos] == 'E') || (source[currPos] == 'e')) {
        if (currPos == 0) {
          // 'E' alone is not a valid numeric.
          char hexstr[MAX_OFFENDING_SOURCE_DATA_DISPLAY_LEN];
          ExRaiseSqlError(heap, diagsArea, EXE_CONVERT_STRING_ERROR, NULL, NULL, NULL, NULL,
                          stringToHex(hexstr, sizeof(hexstr), source, sourceLen));

          return ex_expr::EXPR_ERROR;
        }

        double intermediate;
        if (convAsciiToFloat64((char *)&intermediate, source, sourceLen, heap, diagsArea,
                               flags | CONV_INTERMEDIATE_CONVERSION) != ex_expr::EXPR_OK)
          return ex_expr::EXPR_ERROR;

        // scale the intermediate
        for (int i = 0; i < targetScale; i++) intermediate *= 10.0;

        target = (UInt64)intermediate;

        return ex_expr::EXPR_OK;
      } else {
        // illegal character in this input string
        char hexstr[MAX_OFFENDING_SOURCE_DATA_DISPLAY_LEN];
        ExRaiseSqlError(heap, diagsArea, EXE_CONVERT_STRING_ERROR, NULL, NULL, NULL, NULL,
                        stringToHex(hexstr, sizeof(hexstr), source, sourceLen));

        return ex_expr::EXPR_ERROR;
      }
    }
    currPos++;
  }

  // if we didn't find any digits, it is an error
  if (!digitCnt) {
    if ((flags & CONV_TREAT_ALL_SPACES_AS_ZERO) && (NOT(pointFound || SignFound))) {
      target = 0;
      return ex_expr::EXPR_OK;
    }

    char hexstr[MAX_OFFENDING_SOURCE_DATA_DISPLAY_LEN];
    ExRaiseSqlError(heap, diagsArea, EXE_CONVERT_STRING_ERROR, NULL, NULL, NULL, NULL,
                    stringToHex(hexstr, sizeof(hexstr), source, sourceLen));

    return ex_expr::EXPR_ERROR;
  }

  // up- or downscale
  if (sourceScale < targetScale) {
    for (int i = 0; i < (targetScale - sourceScale); i++) {
      if (target > (ULLONG_MAX / 10)) {  // next power of 10 causes an overflow
        ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, REC_BYTE_F_ASCII,
                              SQLCHARSETCODE_ISO88591, REC_BIN64_SIGNED, flags);
        return ex_expr::EXPR_ERROR;
      }
      target *= 10;
    }
  } else {
    for (int i = 0; i < sourceScale - targetScale; i++) target /= 10;
  }

  return ex_expr::EXPR_OK;
}

///////////////////////////////////////////////////////////////////
// function to convert an ASCII string to UInt64
// The function assumes that source is at least
// sourceLen long. Trailing '\0' is not recognized
///////////////////////////////////////////////////////////////////
ex_expr::exp_return_type convAsciiToUInt64(UInt64 &target, int targetScale, char *source, int sourceLen, CollHeap *heap,
                                           ComDiagsArea **diagsArea, int flags) {
  NABoolean negative = FALSE;  // default is a positive value
  int currPos = 0;             // current position in the string

  negative = isNegative(source, sourceLen, currPos);
  if (negative) {
    ExRaiseSqlError(heap, diagsArea, EXE_UNSIGNED_OVERFLOW);
    return ex_expr::EXPR_ERROR;
  }

  ex_expr::exp_return_type ert =
      convAsciiToUInt64base(target, targetScale, &source[currPos], sourceLen - currPos, heap, diagsArea, flags);
  if (ert == ex_expr::EXPR_ERROR) return ert;

  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type convAsciiToInt64(long &target, int targetScale, char *source, int sourceLen, CollHeap *heap,
                                          ComDiagsArea **diagsArea, int flags) {
  int currPos = 0;  // current position in the string

  NABoolean negative = FALSE;  // default is a positive value
  negative = isNegative(source, sourceLen, currPos);

  UInt64 tempTgt = 0;
  ex_expr::exp_return_type ert =
      convAsciiToUInt64base(tempTgt, targetScale, &source[currPos], sourceLen - currPos, heap, diagsArea, flags);
  if (ert == ex_expr::EXPR_ERROR) return ert;
  UInt64 maxLongPlus1 = LLONG_MAX;
  maxLongPlus1++;  // min 64bit long integer is -9223372036854775808
                   // max 64bit long integer is 9223372036854775807
                   // so if negative value convert to non-negative value, should check with LLONG_MAX + 1
  if ((tempTgt > LLONG_MAX && NOT negative) || (tempTgt > maxLongPlus1)) {
    ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, REC_BYTE_F_ASCII,
                          SQLCHARSETCODE_ISO88591, REC_BIN64_SIGNED, flags);
    return ex_expr::EXPR_ERROR;
  }

  if (NOT negative) {
    target = (long)tempTgt;
    return ex_expr::EXPR_OK;
  }
  /* remove below code according to discussion in github
   * https://github.com/apache/trafodion/pull/706
   * with above validation, below checking is no longer needed
   * comment out

  if (-(long)tempTgt < LLONG_MIN)
    {
      ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW,
                            source, sourceLen, REC_BYTE_F_ASCII,
                            SQLCHARSETCODE_ISO88591,
                            REC_BIN64_SIGNED, flags);
      return ex_expr::EXPR_ERROR;
    }
  */
  target = -(long)tempTgt;

  return ex_expr::EXPR_OK;
}

//////////////////////////////////////////////////////////////////
// function to convert an long  to Decimal
///////////////////////////////////////////////////////////////////

ex_expr::exp_return_type convInt64ToDec(char *target, int targetLen, long source, CollHeap *heap,
                                        ComDiagsArea **diagsArea) {
  int flags = 0;
  NABoolean negative = (source < 0);
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
      ExRaiseDetailSqlError(heap, diagsArea, EXE_STRING_OVERFLOW, (char *)&source, sizeof(long), REC_BIN64_SIGNED, 0,
                            REC_DECIMAL_LSE, flags);
      return ex_expr::EXPR_ERROR;
    };

    target[currPos--] = '0' + (char)(source % 10);
    source /= 10;
  };

  // zero pad the leading spaces
  while (currPos >= 0) target[currPos--] = '0';

  if (negative) target[0] |= 0200;

  return ex_expr::EXPR_OK;
}

///////////////////////////////////////////////////////////////////
// function to convert an ASCII string to DEC.
// Conversion is ASCII -> DEC, and caller has to pass
// &target[0], targetLen
// Also, this function always sets the sign bit for DEC if the
// source is negative.
// The function assumes that source is at least
// sourceLen long. Trailing '\0' is not recongnized
///////////////////////////////////////////////////////////////////
ex_expr::exp_return_type convAsciiToDec(char *target, int targetType, int targetLen, int targetScale, char *source,
                                        int sourceLen, char offset, CollHeap *heap, ComDiagsArea **diagsArea,
                                        int flags) {
  NABoolean negative = FALSE;     // default is a positive value
  int sourceStart = 0;            // start of source after skipping 0 and ' '
  int sourceScale = 0;            // by default the scale is 0
  int targetPos = targetLen - 1;  // current positon in the target

  // skip leading blanks
  while ((sourceStart < sourceLen) && (source[sourceStart] == ' ')) sourceStart++;

  // only blanks found, error
  if (sourceStart == sourceLen) {
    if (flags & CONV_TREAT_ALL_SPACES_AS_ZERO) {
      // pad the target with zeros
      while (targetPos >= 0) target[targetPos--] = '0' - offset;

      return ex_expr::EXPR_OK;
    }

    char hexstr[MAX_OFFENDING_SOURCE_DATA_DISPLAY_LEN];
    ExRaiseSqlError(heap, diagsArea, EXE_CONVERT_STRING_ERROR, NULL, NULL, NULL, NULL,
                    stringToHex(hexstr, sizeof(hexstr), source, sourceLen));

    return ex_expr::EXPR_ERROR;
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
      if (convAsciiToFloat64((char *)&intermediate, &source[sourceStart], sourceLen - sourceStart, heap, diagsArea,
                             flags | CONV_INTERMEDIATE_CONVERSION) != ex_expr::EXPR_OK)
        return ex_expr::EXPR_ERROR;

      // scale the intermediate
      for (int j = 0; j < targetScale; j++) {
        intermediate *= 10.0;

        if ((intermediate < LLONG_MIN) || (intermediate > LLONG_MAX)) {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, REC_BYTE_F_ASCII,
                                SQLCHARSETCODE_ISO88591, REC_DECIMAL_UNSIGNED, flags);
          return ex_expr::EXPR_ERROR;
        };
      }

      if (convInt64ToDec(target, targetLen, (long)intermediate, heap, diagsArea) != ex_expr::EXPR_OK)
        return ex_expr::EXPR_ERROR;
      return ex_expr::EXPR_OK;
    }
  }

  // the string does not represent a float value. Go on with
  // the processing
  int sourcePos = sourceLen - 1;  // current position in the string

  // check for sign
  NABoolean foundSign = FALSE;
  if (source[sourceStart] == '+') {
    sourceStart++;
    foundSign = TRUE;
  } else if (source[sourceStart] == '-') {
    negative = TRUE;
    sourceStart++;
    foundSign = TRUE;
  };

  // only sign(+/-) found, error
  if (foundSign && (sourceStart == sourceLen)) {
    char hexstr[MAX_OFFENDING_SOURCE_DATA_DISPLAY_LEN];
    ExRaiseSqlError(heap, diagsArea, EXE_CONVERT_STRING_ERROR, NULL, NULL, NULL, NULL,
                    stringToHex(hexstr, sizeof(hexstr), source, sourceLen));
    return ex_expr::EXPR_ERROR;
  };

  // skip leading zeros
  while ((sourceStart < sourceLen) && (source[sourceStart] == '0')) sourceStart++;

  // only zeros found, target is 0
  if (sourceStart == sourceLen) {
    str_pad(target, targetLen, '0' - offset);
    return ex_expr::EXPR_OK;
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

  NABoolean raiseOverflowWarning = FALSE;

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
      if (source[sourcePos] != '0') raiseOverflowWarning = TRUE;
      sourcePos--;
      sourceScale--;
    } else if (sourcePos == pointPos) {
      // simply skip the decimal point
      sourcePos--;
    } else {
      // if source is not a digit, we have an error
      if (source[sourcePos] < '0' || source[sourcePos] > '9') {
        char hexstr[MAX_OFFENDING_SOURCE_DATA_DISPLAY_LEN];
        ExRaiseSqlError(heap, diagsArea, EXE_CONVERT_STRING_ERROR, NULL, NULL, NULL, NULL,
                        stringToHex(hexstr, sizeof(hexstr), source, sourceLen));

        return ex_expr::EXPR_ERROR;
      };
      // copy source to target
      target[targetPos--] = source[sourcePos--] - offset;
    };
  };

  if (raiseOverflowWarning) ExRaiseSqlWarning(heap, diagsArea, EXE_NUMERIC_OVERFLOW);

  // we might be right on the decimal point. Skip it before test for
  // overflow
  if (sourcePos == pointPos) sourcePos--;

  // if we couldn't copy all digits, we had an overflow
  if (sourcePos >= sourceStart) {
    ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, REC_BYTE_F_ASCII,
                          SQLCHARSETCODE_ISO88591, (short)targetType, flags);
    return ex_expr::EXPR_ERROR;
  };

  // left pad the target with zeros
  while (targetPos >= 0) target[targetPos--] = '0' - offset;

  // add sign
  if (negative) target[0] |= 0200;

  return ex_expr::EXPR_OK;
};

///////////////////////////////////////////////////////////////////
// function to convert a DOUBLE to BIGNUM.
///////////////////////////////////////////////////////////////////
ex_expr::exp_return_type convDoubleToBigNum(char *target, int targetLen, int targetType, int targetPrecision,
                                            int targetScale, double source, CollHeap *heap, ComDiagsArea **diagsArea) {
  SimpleType sourceAttr(REC_FLOAT64, sizeof(double), 0, 0, ExpTupleDesc::SQLMX_FORMAT, 8, 0, 0, 0,
                        Attributes::NO_DEFAULT, 0);
  char *opData[2];
  opData[0] = target;
  opData[1] = (char *)&source;
  NABoolean isSigned = (targetType == REC_NUM_BIG_SIGNED);
  BigNum bn(targetLen, targetPrecision, (short)targetScale, isSigned);
  bn.setTempSpaceInfo(ITM_CAST, 0, targetLen);
  if (bn.castFrom(&sourceAttr, opData, heap, diagsArea) != 0) {
    ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, (char *)&source, 8, REC_FLOAT64, 0, targetType,
                          0 /* flags */);

    return ex_expr::EXPR_ERROR;
  }

  return ex_expr::EXPR_OK;
}

///////////////////////////////////////////////////////////////////
// function to convert an ASCII string to BIGNUM.
//
// First convert from ASCII to LARGEDEC, then convert from LARGEDEC
// to BIGNUM
///////////////////////////////////////////////////////////////////
ex_expr::exp_return_type convAsciiToBigNum(char *target, int targetLen, int targetType, int targetPrecision,
                                           int targetScale, char *source, int sourceLen, NAMemory *heap,
                                           ComDiagsArea **diagsArea, int flags)

{
  // if the string contains 'e' or 'E' it actually represents
  // a float value. Do a ASCII -> DOUBLE -> BIGNUM.
  // use a for-loop to find the 'e'. If we find one, we do the
  // described conversion and return. If we do not find one,
  // we go on with this function.
  // Also, start at the end of the string, if we have an 'e' it
  // is probably closer to the end.
  for (int i = sourceLen - 1; i >= 0; i--) {
    if (source[i] == 'E' || source[i] == 'e') {
      // found a float representation.

      // convert ascii to DOUBLE.
      double intermediateDouble;
      if (convDoIt(source, sourceLen, REC_BYTE_F_ASCII, 0, 0, (char *)&intermediateDouble, 8, REC_FLOAT64, 0, 0, NULL,
                   0, heap, diagsArea) != ex_expr::EXPR_OK)
        return ex_expr::EXPR_ERROR;

      if ((targetType == REC_NUM_BIG_UNSIGNED) && (intermediateDouble < 0)) {
        ExRaiseSqlError(heap, diagsArea, EXE_UNSIGNED_OVERFLOW);
        return ex_expr::EXPR_ERROR;
      }

      // convert DOUBLE to bignum
      SimpleType sourceAttr(REC_FLOAT64, sizeof(double), 0, 0, ExpTupleDesc::SQLMX_FORMAT, 8, 0, 0, 0,
                            Attributes::NO_DEFAULT, 0);
      char *opData[2];
      opData[0] = target;
      opData[1] = (char *)&intermediateDouble;
      BigNum bn(targetLen, targetPrecision, (short)targetScale, (targetType == REC_NUM_BIG_UNSIGNED ? 1 : 0));
      bn.setTempSpaceInfo(ITM_CAST, 0, targetLen);
      if (bn.castFrom(&sourceAttr, opData, heap, diagsArea) != 0) {
        ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, (char *)&intermediateDouble, 8, REC_FLOAT64, 0,
                              (short)targetType, flags);

        return ex_expr::EXPR_ERROR;
      }

      return ex_expr::EXPR_OK;
    }
  }  // for

  // Convert from ASCII to an intermediate LARGEDEC, using the function convAsciiToDec().
  // To understand this function call, it will be helpful to review the
  // comments in its definition earlier.
  char *intermediateLargeDec = new (heap) char[targetPrecision + 1];
  if (convAsciiToDec(intermediateLargeDec + 1, targetType, targetPrecision, targetScale, source, sourceLen, '0', heap,
                     diagsArea, flags | CONV_INTERMEDIATE_CONVERSION) != ex_expr::EXPR_OK) {
    NADELETEBASIC(intermediateLargeDec, (heap));
    return ex_expr::EXPR_ERROR;
  }

  if ((targetType == REC_NUM_BIG_UNSIGNED) && (intermediateLargeDec[1] & 0200)) {
    NADELETEBASIC(intermediateLargeDec, (heap));

    ExRaiseSqlError(heap, diagsArea, EXE_UNSIGNED_OVERFLOW);
    return ex_expr::EXPR_ERROR;
  }

  // Set the sign byte in the intermediate LARGEDEC.
  if (intermediateLargeDec[1] & 0200) {
    // reset the bit
    intermediateLargeDec[1] &= 0177;
    intermediateLargeDec[0] = '-';
  } else
    intermediateLargeDec[0] = '+';

  // Convert from the intermediate LARGEDEC to BIGNUM using a BigNumHelper method.
  short retCode =
      BigNumHelper::ConvBcdToBigNumWithSignHelper(targetPrecision + 1, targetLen, intermediateLargeDec, target);
  NADELETEBASIC(intermediateLargeDec, (heap));

  if (retCode == -1) {
    // target is not long enough - overflow
    ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, REC_BYTE_F_ASCII,
                          SQLCHARSETCODE_ISO88591, (short)targetType, flags);
    return ex_expr::EXPR_ERROR;
  };

  return ex_expr::EXPR_OK;
}

static rec_datetime_field getIntervalStartField(int datatype) {
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

static rec_datetime_field getIntervalEndField(int datatype) {
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

///////////////////////////////////////////////////////////////////
// function to convert interval field ASCII string to long
// The function reads at most sourceLen number of characters.
// Or until it encounters a non-digit character and sets the
// sourceLen to the actual length of the string read if it is
// smaller than sourceLen.
// This function is used for ASCII to INTERVAL conversions only.
// Blanks and sign are already removed.
///////////////////////////////////////////////////////////////////
ex_expr::exp_return_type convAsciiFieldToInt64(long &target, int targetScale, char *source, int &sourceLen,
                                               CollHeap *heap, ComDiagsArea **diagsArea, int flags) {
  int currPos = 0;  // current position in the string
  target = 0;       // result
  while ((currPos < sourceLen) && ((source[currPos] >= '0') && (source[currPos] <= '9'))) {
    short thisDigit = source[currPos] - '0';
    if (target > (LLONG_MAX / 10)) {  // next power of 10 causes an overflow
      ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, REC_BYTE_F_ASCII,
                            SQLCHARSETCODE_ISO88591, REC_BIN64_SIGNED, flags);
      return ex_expr::EXPR_ERROR;
    }
    target *= 10;
    if (target > LLONG_MAX - thisDigit) {  // adding this digit causes an overflow
      ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, REC_BYTE_F_ASCII,
                            SQLCHARSETCODE_ISO88591, REC_BIN64_SIGNED, flags);
      return ex_expr::EXPR_ERROR;
    }
    target += thisDigit;
    currPos++;
  }
  if (!currPos) {
    // No digits were found
    char hexstr[MAX_OFFENDING_SOURCE_DATA_DISPLAY_LEN];
    ExRaiseSqlError(heap, diagsArea, EXE_CONVERT_STRING_ERROR, NULL, NULL, NULL, NULL,
                    stringToHex(hexstr, sizeof(hexstr), source, sourceLen));

    return ex_expr::EXPR_ERROR;
  }
  // return the number of digits read
  sourceLen = currPos;
  return ex_expr::EXPR_OK;
};

///////////////////////////////////////////////////////////////////
// function to convert an ASCII string to an interval datatype.
///////////////////////////////////////////////////////////////////
ex_expr::exp_return_type convAsciiToInterval(char *target, int targetLen, int targetDatatype, int leadingPrecision,
                                             int fractionPrecision, char *source, int sourceLen,
                                             NABoolean allowSignInInterval, CollHeap *heap, ComDiagsArea **diagsArea,
                                             int flags) {
  // skip leading and trailing blanks and adjust source and sourceLen
  // accordingly
  while ((sourceLen > 0) && (*source == ' ')) {
    source++;
    sourceLen--;
  }

  if (!sourceLen) {
    // string contains no digits
    ExRaiseSqlError(heap, diagsArea, EXE_CONVERT_INTERVAL_ERROR);
    return ex_expr::EXPR_ERROR;
  };

  NABoolean negInterval = FALSE;
  if ((source[0] == '-') || (source[0] == '+')) {
    if (NOT allowSignInInterval) {
      // string starts with a sign - not allowed
      ExRaiseSqlError(heap, diagsArea, EXE_CONVERT_INTERVAL_ERROR);
      return ex_expr::EXPR_ERROR;
    } else {
      if (source[0] == '-') negInterval = TRUE;
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

  rec_datetime_field start = getIntervalStartField(targetDatatype);
  rec_datetime_field end = getIntervalEndField(targetDatatype);

  // convert the first field
  long intermediate;
  int fieldLen = leadingPrecision;
  if (fieldLen > sourceLen) fieldLen = sourceLen;  // so we don't read garbage past end of string

  if (convAsciiFieldToInt64(intermediate,
                            0,  // targetScale
                            source, fieldLen, heap, diagsArea,
                            flags | CONV_INTERMEDIATE_CONVERSION) != ex_expr::EXPR_OK)
    return ex_expr::EXPR_ERROR;

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
      ExRaiseSqlError(heap, diagsArea, EXE_CONVERT_INTERVAL_ERROR);
      return ex_expr::EXPR_ERROR;
    }

    // get the delimiter preceding this field
    if (source[strIndex] != delimiter[index]) {
      // HOUR can be preceded by either ':' or ' '
      if ((field != REC_DATE_HOUR) || (source[strIndex] != ' ')) {
        ExRaiseSqlError(heap, diagsArea, EXE_CONVERT_INTERVAL_ERROR);
        return ex_expr::EXPR_ERROR;
      }
    }

    strIndex++;  // step over delimiter
    fieldLen = 2;
    if (fieldLen > sourceLen - strIndex) fieldLen = sourceLen - strIndex;  // don't go off end of string

    // convert the next field
    if (convAsciiFieldToInt64(fieldValue,
                              0,  // targetScale
                              &source[strIndex], fieldLen, heap, diagsArea,
                              flags | CONV_INTERMEDIATE_CONVERSION) != ex_expr::EXPR_OK)
      return ex_expr::EXPR_ERROR;

    if (fieldValue > maxFieldValue[index]) {
      ExRaiseSqlError(heap, diagsArea, EXE_CONVERT_INTERVAL_ERROR);
      return ex_expr::EXPR_ERROR;
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
      if (convDoIt(&source[strIndex + 1],
                   sourcePrecision,  // sourceLen
                   REC_BYTE_F_ASCII,
                   0,  // sourcePrecision
                   0,  // sourceScale
                   (char *)&fraction,
                   4,  // targetLen
                   REC_BIN32_UNSIGNED,
                   0,     // targetPrecision
                   0,     // targetScale
                   NULL,  // varCharLen
                   0,     // varCharLenSize
                   heap, diagsArea, CONV_ASCII_BIN32U, 0, flags | CONV_INTERMEDIATE_CONVERSION) != ex_expr::EXPR_OK)
        return ex_expr::EXPR_ERROR;
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
      ExRaiseSqlError(heap, diagsArea, EXE_CONVERT_INTERVAL_ERROR);
      return ex_expr::EXPR_ERROR;
    }

    intermediate += fraction;
  } else {
    // check if the string is completely converted
    if (strIndex != sourceLen) {
      ExRaiseSqlError(heap, diagsArea, EXE_CONVERT_INTERVAL_ERROR);
      return ex_expr::EXPR_ERROR;
    }
  }

  switch (targetLen) {
    case SQL_SMALL_SIZE:
      if ((intermediate < SHRT_MIN) || (intermediate > SHRT_MAX)) {
        ExRaiseSqlError(heap, diagsArea, EXE_CONVERT_INTERVAL_ERROR);
        return ex_expr::EXPR_ERROR;
      };
      if (negInterval)
        *(Target<short> *)target = -(short)intermediate;
      else
        *(Target<short> *)target = (short)intermediate;
      break;

    case SQL_INT_SIZE:
      if ((intermediate < INT_MIN) || (intermediate > INT_MAX)) {
        ExRaiseSqlError(heap, diagsArea, EXE_CONVERT_INTERVAL_ERROR);
        return ex_expr::EXPR_ERROR;
      };
      if (negInterval)
        *(Target<int> *)target = -(int)intermediate;
      else
        *(Target<int> *)target = (int)intermediate;
      break;

    case SQL_LARGE_SIZE:
      if (negInterval)
        *(Target<long> *)target = -intermediate;
      else
        *(Target<long> *)target = intermediate;
      break;

    default:
      ExRaiseSqlError(heap, diagsArea, EXE_CONVERT_INTERVAL_ERROR);
      return ex_expr::EXPR_ERROR;
  }
  return ex_expr::EXPR_OK;
};

///////////////////////////////////////////////////////////////////
// function to convert a INTERVAL to a string.
///////////////////////////////////////////////////////////////////
ex_expr::exp_return_type convIntervalToAscii(char *source, int sourceLen, int leadingPrecision, int fractionPrecision,
                                             short sourceType, char *inTarget, int inTargetLen,
                                             int inTargetPrecision,  // max chars allowed
                                             char *varCharLen, int varCharLenSize, NABoolean leftPad, CollHeap *heap,
                                             ComDiagsArea **diagsArea) {
  int flags = 0;
  char delimiter[] = "^-^ ::";
  unsigned short maxFieldValue[] = {0, 11, 0, 23, 59, 59};

  int realTargetLen;
  int inTargetChars = inTargetLen;
  Int16 trgType;
  if (varCharLenSize)
    trgType = REC_BYTE_V_ASCII;
  else
    trgType = REC_BYTE_F_ASCII;

  rec_datetime_field startField = getIntervalStartField(sourceType);
  rec_datetime_field endField = getIntervalEndField(sourceType);

  long value;
  switch (sourceLen) {
    case SQL_SMALL_SIZE: {
      short temp;
      str_cpy_all((char *)&temp, source, sourceLen);
      value = temp;
      break;
    }
    case SQL_INT_SIZE: {
      int temp;
      str_cpy_all((char *)&temp, source, sourceLen);
      value = temp;
      break;
    }
    case SQL_LARGE_SIZE:
      str_cpy_all((char *)&value, source, sourceLen);
      break;
    default:
      return ex_expr::EXPR_ERROR;
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

  if (inTargetPrecision) {
    assert(inTargetPrecision <= inTargetLen);
    inTargetChars = inTargetPrecision;
  }

  if (inTargetChars < realTargetLen) {
    ExRaiseDetailSqlError(heap, diagsArea, EXE_STRING_OVERFLOW, source, sourceLen, sourceType, fractionPrecision,
                          trgType, flags);
    return ex_expr::EXPR_ERROR;
  }

  int targetLen = inTargetChars;
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
  if (!varCharLenSize) {
    for (int i = 0; i < (targetLen - realTargetLen); i++, target++) *target = ' ';
    // If target is actually UTF8, we must put filler spaces in rest of target
    if (target < (inTarget + inTargetLen)) str_pad(target, inTarget + inTargetLen - target, ' ');
  };

  // convert fraction part if any. targetIndex was set earlier
  if (fractionPrecision) {
    targetIndex += 1;  // 1 for '.'
    str_pad(target + targetIndex, fractionPrecision, '0');
    if (convInt64ToAscii(target + targetIndex,
                         fractionPrecision,  // targetLen
                         0, 0, (long)fieldVal,
                         0,     // scale,
                         NULL,  // varCharLen
                         0,     // varCharLenSize
                         '0',   // filler character
                         FALSE,
                         TRUE,  // leftPad
                         FALSE, heap, diagsArea) != ex_expr::EXPR_OK)
      return ex_expr::EXPR_ERROR;

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

    if (convInt64ToAscii(target + targetIndex,
                         2,  // targetLen
                         0, 0, (long)fieldVal,
                         0,     // scale,
                         NULL,  // varCharLen
                         0,     // varCharLenSize
                         '0',   // filler character
                         FALSE,
                         TRUE,  // leftPad
                         FALSE, heap, diagsArea) != ex_expr::EXPR_OK)
      return ex_expr::EXPR_ERROR;

    targetIndex--;
    target[targetIndex] = delimiter[index];
  };

  // leading field
  if (convInt64ToAscii(target + (sign == '-' ? 1 : 0),
                       leadingPrecision,  // targetLen
                       0, 0, (long)value,
                       0,     // scale,
                       NULL,  // varCharLen
                       0,     // varCharLenSize
                       ' ',   // filler character
                       FALSE,
                       TRUE,  // leftPad
                       FALSE, heap, diagsArea) != ex_expr::EXPR_OK)
    return ex_expr::EXPR_ERROR;

  if (sign == '-') {
    // Sign goes here.
    // convInt64ToAscii() writes getDigitCount(value) number of digits
    // space padded on the left, between target + 1 and
    // target + leadingPrecision character positions.
    // the following is one position to the left of the most significant
    // digit and this is where sign should be.

    char *sp = (target + leadingPrecision - getDigitCount(value));

    *sp = '-';

    // Further to the left of the sign there can only be just one more
    // character (because we pass target + 1 to convInt64ToAscii). When
    // the sign is to the right of the target, there is this extra character
    // which may contain garbage, so we need to clear it.

    if (sp > target) *target = ' ';
  }
  //  else
  //  {
  //    target[0] = ' ';
  //  }

  // set length field for variable length targets
  if (varCharLenSize) {
    if (varCharLenSize == sizeof(int))
      str_cpy_all(varCharLen, (char *)&realTargetLen, sizeof(int));
    else {
      short VCLen = (short)realTargetLen;
      str_cpy_all(varCharLen, (char *)&VCLen, sizeof(short));
    };
  };

  return ex_expr::EXPR_OK;
};

///////////////////////////////////////////////////////////////////
// function to convert a BIGNUM to long
///////////////////////////////////////////////////////////////////
ex_expr::exp_return_type convBigNumToInt64(long *target, char *source, int sourceLen, NAMemory *heap,
                                           ComDiagsArea **diagsArea, int flags)

{
  short retCode = BigNumHelper::ConvBigNumWithSignToInt64Helper(sourceLen, source, (void *)target, FALSE);
  if (retCode == -1) {
    // target is not long enough - overflow
    ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, REC_NUM_BIG_SIGNED, 0,
                          REC_BIN64_SIGNED, flags);
    return ex_expr::EXPR_ERROR;
  };

  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type convBigNumToUInt64(UInt64 *target, char *source, int sourceLen, NAMemory *heap,
                                            ComDiagsArea **diagsArea, int flags)

{
  short retCode = BigNumHelper::ConvBigNumWithSignToInt64Helper(sourceLen, source, (void *)target, TRUE);
  if (retCode == -1) {
    // target is not long enough - overflow
    ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, REC_NUM_BIG_SIGNED, 0,
                          REC_BIN64_UNSIGNED, flags);
    return ex_expr::EXPR_ERROR;
  };

  return ex_expr::EXPR_OK;
}

///////////////////////////////////////////////////////////////////
// function to convert a BIGNUM to long and scale it, returning
// information about the conversion to the caller (e.g. truncations,
// overflows). The conversion status information is returned in the
// return code instead of in the ComDiagsArea; if the caller wishes
// an exception, then the caller must convert the return code to
// an appropriate exception. In the event of overflow, a maximum
// or minimum value is returned.
///////////////////////////////////////////////////////////////////

ex_conv_clause::ConvResult convBigNumToInt64AndScale(long *target, char *source, int sourceLen, int exponent,
                                                     NAMemory *heap)

{
  short retCode = BigNumHelper::ConvBigNumWithSignToInt64AndScaleHelper(sourceLen, source, target, exponent, heap);

  // The retCode can have 5 possible values (see the helper method for details):
  //   0: the conversion was ok
  //   1: the result was rounded up to LLONG_MIN
  //   2: the result was rounded down to LLONG_MAX
  //   3: the result was rounded up
  //   4: the result was rounded down

  switch (retCode) {
    case 1:
      return ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN;
    case 2:
      return ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX;
    case 3:
      return ex_conv_clause::CONV_RESULT_ROUNDED_UP;
    case 4:
      return ex_conv_clause::CONV_RESULT_ROUNDED_DOWN;
    default:  // retCode == 0
      return ex_conv_clause::CONV_RESULT_OK;
  }
}

///////////////////////////////////////////////////////////////////
// function to convert a LARGEDEC to DEC (signed or unsigned),
// with scaling -- returns a return code indicating whether
// the result is equal to the original or if rounding occurred
///////////////////////////////////////////////////////////////////
ex_conv_clause::ConvResult convLargeDecToDecAndScale(char *target, int targetLen, char *source, int sourceLen,
                                                     NABoolean resultUnsigned, int exponent) {
  ex_conv_clause::ConvResult rc = ex_conv_clause::CONV_RESULT_OK;  // assume success
  NABoolean negative = (source[0] == '-');

  if ((resultUnsigned) && (negative)) {
    // we are trying to convert a negative number to an
    // unsigned target -  set to minimum (unsigned) value
    str_pad(target, targetLen, '0');
    return ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN;
  }

  // at this point, we know that the source is non-negative or
  // that the target is signed

  // if we are scaling up, truncate the source; remember if any
  // non-zero digits were truncated
  NABoolean significantTruncationOccurred = FALSE;
  if (exponent < 0) {
    int truncatedLen = -exponent;

    if (truncatedLen >= sourceLen) truncatedLen = sourceLen - 1;  // everything was truncated!

    sourceLen -= truncatedLen;

    char *truncatedPart = &source[sourceLen];

    // look for a non-zero digit in the truncated part (anywhere)
    int i = truncatedLen - 1;
    for (; (i >= 0) && (truncatedPart[i] == 0); i--)
      ;

    if (i >= 0)  // if we found one...
      significantTruncationOccurred = TRUE;

    exponent = 0;  // to make later logic easier
  }

  // determine length of source (skip leading zeros)
  int sourcePos = 1;
  while ((sourcePos < sourceLen) && (source[sourcePos] == 0)) sourcePos++;

  int copyLen = sourceLen - sourcePos;

  // if source is still too big, overflow
  if (copyLen > targetLen - exponent) {
    // set target to maximum (or minimum) value
    str_pad(target, targetLen, '9');
    if (negative)  // if source is negative
    {
      // note that we know target is signed in this case,
      // so the minimum value is simply -(maximum value)
      target[0] |= 0200;  // turn on sign bit
      return ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN;
    }
    // else we know it's the maximum
    return ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX;
  }

  // source fits in target, copy and left-pad target with zero if
  // necessary
  int targetPos = targetLen - copyLen - exponent;

  str_pad(target, targetPos, '0');
  while (sourcePos < sourceLen) target[targetPos++] = source[sourcePos++] + '0';

  // do any scaling down needed
  while (exponent > 0) {
    target[targetPos++] = '0';
    exponent--;
  }

  if (negative)         // if source was negative
    target[0] |= 0200;  // turn on sign bit

  // change return code if significant truncation occurred
  if (significantTruncationOccurred) {
    if (negative)
      rc = ex_conv_clause::CONV_RESULT_ROUNDED_UP;
    else
      rc = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN;
  }

  return rc;
};

///////////////////////////////////////////////////////////////////
// function to convert a BIGNUM to DEC (signed or unsigned),
// with scaling -- returns a return code indicating whether
// the result is equal to the original or if rounding occurred
//
// We first convert from BIGNUM to LARGEDEC, then convert from LARGEDEC
// to DEC and scale
///////////////////////////////////////////////////////////////////
ex_conv_clause::ConvResult convBigNumToDecAndScale(char *target, int targetLen, char *source, int sourceLen,
                                                   int sourcePrecision, NABoolean resultUnsigned, int exponent,
                                                   NAMemory *heap)

{
  char *intermediateLargeDec = new (heap) char[sourcePrecision + 1];  // one extra byte for the sign.
  BigNumHelper::ConvBigNumWithSignToBcdHelper(sourceLen, sourcePrecision + 1, source, intermediateLargeDec, heap);

  // Convert from large dec to dec and scale.
  ex_conv_clause::ConvResult retValue =
      convLargeDecToDecAndScale(target, targetLen, intermediateLargeDec, sourcePrecision + 1, resultUnsigned, exponent);
  NADELETEBASIC(intermediateLargeDec, (heap));
  return retValue;
}

///////////////////////////////////////////////////////////////////
// function to convert a BIGNUM to BIGNUM
///////////////////////////////////////////////////////////////////
ex_expr::exp_return_type convBigNumToBigNum(char *target, int targetLen, int targetType, int targetPrecision,
                                            char *source, int sourceLen, int sourceType, int sourcePrecision,
                                            NAMemory *heap, ComDiagsArea **diagsArea, int flags) {
  short retCode = ex_expr::EXPR_OK;

  if (sourcePrecision > targetPrecision) {
    // return an error if source precision is greater than target precision
    // and source will not fit into target.
    // For now, do this by first converting source to char and then
    // convert char to target.
    // Need to optimize this so ConvBigNumWithSignToBigNumWithSignHelper
    // can return this error instead of going through the char conversion.

    int intermediateLen = sourcePrecision + 1;  // +1 for sign
    char *intermediate = (char *)heap->allocateMemory(intermediateLen);
    retCode = convBigNumToAscii(intermediate, sourcePrecision + 1, 0, source, sourceLen, sourcePrecision, 0, NULL, 0, 0,
                                heap, diagsArea, 0);
    if (retCode == ex_expr::EXPR_OK) {
      // now convert intermediate to target
      retCode = convAsciiToBigNum(target, targetLen, targetType, targetPrecision, 0, intermediate, sourcePrecision + 1,
                                  heap, diagsArea, 0);
    }

    heap->deallocateMemory((void *)intermediate);

    if (retCode != ex_expr::EXPR_OK) {
      // overflow error. diagsArea already contains the error.
      return ex_expr::EXPR_ERROR;
    }
  };

  if (targetType == REC_NUM_BIG_UNSIGNED) {
    char sign = BIGN_GET_SIGN(source, sourceLen);

    if (sign) {
      ExRaiseSqlError(heap, diagsArea, EXE_UNSIGNED_OVERFLOW);

      return ex_expr::EXPR_ERROR;
    }
  }

  retCode = BigNumHelper::ConvBigNumWithSignToBigNumWithSignHelper(sourceLen, targetLen, source, target);
  if (retCode == -1) {
    // target is not long enough - overflow
    ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, REC_NUM_BIG_SIGNED, 0,
                          REC_NUM_BIG_SIGNED, flags);
    return ex_expr::EXPR_ERROR;
  };

  return ex_expr::EXPR_OK;
}

//////////////////////////////////////////////////////////////////
// function to convert Decimal to long
///////////////////////////////////////////////////////////////////

ex_expr::exp_return_type convDecToInt64(long &target, char *source, int sourceLen, CollHeap *heap,
                                        ComDiagsArea **diagsArea, int flags) {
  //
  // The first bit of the first byte of the decimal value is the sign bit.
  // If the bit is set, the decimal value is a negative number.  Reset
  // the bit before converting the value to binary.
  //
  target = '0' - (source[0] & 0177);

  int currPos = 1;

  // skip leading zeros only if first byte was zero
  if (!target) {
    while ((currPos < sourceLen) && (source[currPos] == '0')) currPos++;
  };

  // Convert the value to a negative binary number.  Check for overflow.
  while (currPos < sourceLen) {
    if (source[currPos] < '0' || source[currPos] > '9') {
      char hexstr[MAX_OFFENDING_SOURCE_DATA_DISPLAY_LEN];
      ExRaiseSqlError(heap, diagsArea, EXE_CONVERT_STRING_ERROR, NULL, NULL, NULL, NULL,
                      stringToHex(hexstr, sizeof(hexstr), source, sourceLen));

      return ex_expr::EXPR_ERROR;
    };
    target *= 10;
    target -= source[currPos++] - '0';
    if (target > 0) {
      ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, REC_DECIMAL_LSE, 0,
                            REC_BIN64_SIGNED, flags);
      return ex_expr::EXPR_ERROR;
    };
  };

  // If the decimal value was positive, negate the value.  Check for overflow.
  if (!(source[0] & 0200)) {
    target = -target;
    if (target < 0) {
      ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, REC_DECIMAL_LSE, 0,
                            REC_BIN64_SIGNED, flags);
      return ex_expr::EXPR_ERROR;
    };
  };

  return ex_expr::EXPR_OK;
};

//////////////////////////////////////////////////////////////////
// function to convert Decimal to Decimal
///////////////////////////////////////////////////////////////////
ex_expr::exp_return_type convDecToDec(char *target, int targetLen, char *source, int sourceLen, CollHeap *heap,
                                      ComDiagsArea **diagsArea) {
  if (targetLen >= sourceLen) {
    int targetIndex = targetLen - sourceLen;
    str_pad(target, (int)targetIndex, '0');
    str_cpy_all(&target[targetIndex], source, sourceLen);
    // Clear the sign bit that was moved to target[targetIndex].
    target[targetIndex] &= 0177;
  } else {
    if ((source[0] & 0177) != '0') {
      // target string is not long enough - overflow
      char hexstr[MAX_OFFENDING_SOURCE_DATA_DISPLAY_LEN];
      ExRaiseSqlError(heap, diagsArea, EXE_CONVERT_STRING_ERROR, NULL, NULL, NULL, NULL,
                      stringToHex(hexstr, sizeof(hexstr), source, sourceLen));

      return ex_expr::EXPR_ERROR;
    };

    int sourceIndex = sourceLen - targetLen;
    for (int i = 1; i < sourceIndex; i++)
      if (source[i] != '0') {
        char hexstr[MAX_OFFENDING_SOURCE_DATA_DISPLAY_LEN];
        ExRaiseSqlError(heap, diagsArea, EXE_CONVERT_STRING_ERROR, NULL, NULL, NULL, NULL,
                        stringToHex(hexstr, sizeof(hexstr), source, sourceLen));

        return ex_expr::EXPR_ERROR;
      };
    str_cpy_all(target, &source[sourceIndex], targetLen);
  }
  //
  // If the source is negative, set the sign bit in the target.
  //
  if (source[0] & 0200) target[0] |= 0200;
  return ex_expr::EXPR_OK;
};

//////////////////////////////////////////////////////////////////
// function to convert DecimalLS to ASCII
///////////////////////////////////////////////////////////////////
ex_expr::exp_return_type convDecLStoAscii(char *target, int targetLen,
                                          int targetPrecision,  // num chars in tgt
                                          char *source, int sourceLen, int sourceScale, CollHeap *heap,
                                          ComDiagsArea **diagsArea, int flags) {
  int curPos = sourceLen - 1;
  int targetLenInChars = (targetPrecision ? targetPrecision : targetLen);
  int targetPos = targetLenInChars - 1;
  // skip blanks, start from end
  while (source[curPos] == ' ' && curPos >= 0) curPos--;

  if (curPos < 0) {
    // only blanks: Error
    ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, REC_DECIMAL_LS, sourceScale,
                          REC_BYTE_F_ASCII, flags);
    return ex_expr::EXPR_ERROR;
  };

  // move data from source to target, put in decimal point where
  // required
  int digitCnt = 0;
  while ((curPos >= 0) && (source[curPos] >= '0') && (source[curPos] <= '9')) {
    // another digit. Make sure we have space in the target
    digitCnt++;
    if (targetPos < 0) {
      ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, REC_DECIMAL_LS, sourceScale,
                            REC_BYTE_F_ASCII, flags);
      return ex_expr::EXPR_ERROR;
    };

    // move digit
    target[targetPos--] = source[curPos--];

    // insert scale
    if (sourceScale == digitCnt) {
      // make sure we have space for scale
      if (targetPos < 0) {
        ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, REC_DECIMAL_LS, sourceScale,
                              REC_BYTE_F_ASCII, flags);
        return ex_expr::EXPR_ERROR;
      };
      // put decimal point in target
      target[targetPos--] = '.';
    };
  };

  // if we have a scale and did not insert the decimal point yet,
  // add zeros until we can put in the point
  while (digitCnt < sourceScale) {
    // Make sure we have space in the target
    if (targetPos < 0) {
      ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, REC_DECIMAL_LS, sourceScale,
                            REC_BYTE_F_ASCII, flags);
      return ex_expr::EXPR_ERROR;
    };

    // put in zero
    target[targetPos--] = '0';
    digitCnt++;

    // insert scale
    if (sourceScale == digitCnt) {
      // make sure we have space for scale
      if (targetPos < 0) {
        ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, REC_DECIMAL_LS, sourceScale,
                              REC_BYTE_F_ASCII, flags);
        return ex_expr::EXPR_ERROR;
      };
      // put decimal point in target
      target[targetPos--] = '.';
    };
  };

  // add negative sign, if we have one
  if ((curPos >= 0) && (source[curPos] == '-')) {
    // make sure we have space for sign
    if (targetPos < 0) {
      ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, REC_DECIMAL_LS, sourceScale,
                            REC_BYTE_F_ASCII, flags);
      return ex_expr::EXPR_ERROR;
    };
    // put sign in target
    target[targetPos--] = '-';
  };

  // left pad  target with blanks
  while (targetPos >= 0) target[targetPos--] = ' ';

  // right pad, if targetLenInChars < targetLen
  targetPos = targetLenInChars;
  while (targetPos < targetLen) target[targetPos++] = ' ';

  return ex_expr::EXPR_OK;
};

//////////////////////////////////////////////////////////////////
// This function returns an int64 with the minimum value that
// can be represented in a decimal type with the characteristics
// specified by the arguements.
// The function assumes that targetLen is 0 to 18, inclusive.
// The function assumes that targetType is one of the following:
//   REC_DECIMAL_UNSIGNED (150)
//   REC_DECIMAL_LSE (152).
///////////////////////////////////////////////////////////////////

long getMinDecValue(int targetLen, short targetType) {
  static const long decValue[] = {0,
                                  -9,
                                  -99,
                                  -999,
                                  -9999,
                                  -99999,
                                  -999999,
                                  -9999999,
                                  -99999999,
                                  -999999999,
                                  -9999999999LL,
                                  -99999999999LL,
                                  -999999999999LL,
                                  -9999999999999LL,
                                  -99999999999999LL,
                                  -999999999999999LL,
                                  -9999999999999999LL,
                                  -99999999999999999LL,
                                  -999999999999999999LL};

  if (targetType == REC_DECIMAL_UNSIGNED) {  // Target is unsigned so the minimum value is zero.
    return 0;
  } else {  // Target is signed so get the minimum value from the array.
    return decValue[targetLen];
  }
}

//////////////////////////////////////////////////////////////////
// This function returns an int64 with the minimum value that
// can be represented in an interval type with the characteristics
// specified by the arguements.
// The function assumes that targetLen is 0 to 18, inclusive.
// The function assumes that targetType is in the range:
//     195 - 207
// - MARIA - 11/06/98
///////////////////////////////////////////////////////////////////

long getMinIntervalValue(int targetPrecision, short targetType) {
  targetType -= REC_MIN_INTERVAL;  // REC_MIN_INTERVAL should be 195

  // a table of all possible minimum interval values grouped by
  // interval type

  // Note: The minimum signed 64-bit value is -9223372036854775808,
  // (which we code as 0x8000000000000000 to avoid compiler whining)
  // so where a target type and precision would demand a value
  // smaller than this, we use this minimum instead.

  static const long MinValue[][19] = {
      {0, -9, -99, -999, -9999, -99999, -999999, -9999999, -99999999, -999999999LL, -9999999999LL, -99999999999LL,
       -999999999999LL, -9999999999999LL, -99999999999999LL, -999999999999999LL, -9999999999999999LL,
       -99999999999999999LL, -999999999999999999LL},  // REC_INT_YEAR 195

      {0, -9, -99, -999, -9999, -99999, -999999, -9999999, -99999999, -999999999, -9999999999LL, -99999999999LL,
       -999999999999LL, -9999999999999LL, -99999999999999LL, -999999999999999LL, -9999999999999999LL,
       -99999999999999999LL, -999999999999999999LL},  // REC_INT_MONTH 196

      {0, -119, -1199, -11999, -119999, -1199999, -11999999, -119999999, -1199999999LL, -11999999999LL, -119999999999LL,
       -1199999999999LL, -11999999999999LL, -119999999999999LL, -1199999999999999LL, -11999999999999999LL,
       -119999999999999999LL, -1199999999999999999LL, LONG_MAX},  // REC_INT_YEAR_MONTH 197

      {0, -9, -99, -999, -9999, -99999, -999999, -9999999, -99999999, -999999999LL, -9999999999LL, -99999999999LL,
       -999999999999LL, -9999999999999LL, -99999999999999LL, -999999999999999LL, -9999999999999999LL,
       -99999999999999999LL, -999999999999999999LL},  // REC_INT_DAY 198

      {0, -9, -99, -999, -9999, -99999, -999999, -9999999, -99999999, -999999999LL, -9999999999LL, -99999999999LL,
       -999999999999LL, -9999999999999LL, -99999999999999LL, -999999999999999LL, -9999999999999999LL,
       -99999999999999999LL, -999999999999999999LL},  // REC_INT_HOUR 199

      {0, -239, -2399, -23999, -239999, -2399999, -23999999, -239999999, -(long)(2399999999LL), -23999999999LL,
       -239999999999LL, -2399999999999LL, -23999999999999LL, -239999999999999LL, -2399999999999999LL,
       -23999999999999999LL, -239999999999999999LL, -2399999999999999999LL, LONG_MAX},  // REC_INT_DAY_HOUR 200

      {0, -9, -99, -999, -9999, -99999, -999999, -9999999, -99999999, -999999999LL, -9999999999LL, -99999999999LL,
       -999999999999LL, -9999999999999LL, -99999999999999LL, -999999999999999LL, -9999999999999999LL,
       -99999999999999999LL, -999999999999999999LL},  // REC_INT_MINUTE 201

      {0, -599, -5999, -59999, -599999, -5999999, -59999999, -599999999, -5999999999LL, -59999999999LL, -599999999999LL,
       -5999999999999LL, -59999999999999LL, -599999999999999LL, -5999999999999999LL, -59999999999999999LL,
       -599999999999999999LL, -5999999999999999999LL, LONG_MAX},  // REC_INT_HOUR_MINUTE 202

      {0, -14399, -143999, -1439999, -14399999, -143999999, -1439999999, -14399999999LL, -143999999999LL,
       -1439999999999LL, -14399999999999LL, -143999999999999LL, -1439999999999999LL, -14399999999999999LL,
       -143999999999999999LL, -1439999999999999999LL, LONG_MAX, LONG_MAX, LONG_MAX},  // REC_INT_DAY_MINUTE 203

      {0, -9, -99, -999, -9999, -99999, -999999, -9999999, -99999999, -999999999LL, -9999999999LL, -99999999999LL,
       -999999999999LL, -9999999999999LL, -99999999999999LL, -999999999999999LL, -9999999999999999LL,
       -99999999999999999LL, -999999999999999999LL},  // REC_INT_SECOND 204

      {
          0, -599, -5999, -59999, -599999, -5999999, -59999999, -599999999, -5999999999LL, -59999999999LL,
          -599999999999LL, -5999999999999LL,
          // 0123456789    01234567890    012345678901
          -59999999999999LL, -599999999999999LL, -5999999999999999LL,
          // 0123456789012    01234567890123    012345678901234
          -59999999999999999LL, -599999999999999999LL, -5999999999999999999LL,
          // 0123456789012345    01234567890123456    012345678901234567
          LONG_MAX
          //  0123012301230123
      },  // REC_INT_MINUTE_SECOND 205

      {0, -35999, -359999, -3599999, -35999999, -359999999, -(long)(3599999999LL), -35999999999LL, -359999999999LL,
       -3599999999999LL, -35999999999999LL,
       //                                              012345678901
       -359999999999999LL, -3599999999999999LL, -35999999999999999LL,
       // 0123456789012     01234567890123     012345678901234
       -359999999999999999LL, -3599999999999999999LL, LONG_MAX,
       // 0123456789012345     01234567890123456    0123012301230123
       LONG_MAX, LONG_MAX},  // REC_INT_HOUR_SECOND 206

      {0, -863999, -8639999, -86399999, -863999999, -8639999999LL, -86399999999LL, -863999999999LL, -8639999999999LL,
       -86399999999999LL, -863999999999999LL,
       //  012345678      0123456789      01234567890      012345678901
       -8639999999999999LL, -86399999999999999LL, -863999999999999999LL,
       //  0123456789012      01234567890123      012345678901234
       -8639999999999999999LL, LONG_MAX, LONG_MAX,
       //  0123456789012345    0123012301230123
       LONG_MAX, LONG_MAX}  // REC_INT_DAY_SECOND 207

  };

  return MinValue[targetType][targetPrecision];
}

//////////////////////////////////////////////////////////////////
// This function count the number of digits in a non-negative long.
// The function assumes that targetLen is 1 to 19, inclusive.
///////////////////////////////////////////////////////////////////

int getDigitCount(UInt64 value) {
  static const UInt64 decValue[] = {0,
                                    9,
                                    99,
                                    999,
                                    9999,
                                    99999,
                                    999999,
                                    9999999,
                                    99999999,
                                    999999999,
                                    9999999999ULL,
                                    99999999999ULL,
                                    999999999999ULL,
                                    9999999999999ULL,
                                    99999999999999ULL,
                                    999999999999999ULL,
                                    9999999999999999ULL,
                                    99999999999999999ULL,
                                    999999999999999999ULL,
                                    9999999999999999999ULL};

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
  if (value <= decValue[19]) return 19;
  return 20;
}

//////////////////////////////////////////////////////////////////
// This function returns an int64 with the maximum value that
// can be represented in a decimal type with the characteristics
// specified by the arguement.
// The function assumes that targetLen is 0 to 18, inclusive.
///////////////////////////////////////////////////////////////////

long getMaxDecValue(int targetLen) {
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

  return decValue[targetLen];
}

//////////////////////////////////////////////////////////////////
// This function returns an int64 with the maximum value that
// can be represented in an interval type with the characteristics
// specified by the arguement.
// The function assumes that targetLen is 0 to 18, inclusive.
// The function also assumes that the targetType is between 195-207
//  - MARIA - 11/06/98
///////////////////////////////////////////////////////////////////

long getMaxIntervalValue(int targetPrecision, short targetType) {
  targetType -= REC_MIN_INTERVAL;  // REC_MIN_INTERVAL should be 195
  // a table of all possible maximum interval values grouped by
  // interval type

  // Note: The maximum signed 64-bit value is 9223372036854775807,
  // so where a target type and precision would demand a value
  // larger than this, we use this maximum instead.

  static const long MaxValue[][19] = {
      {0, 9, 99, 999, 9999, 99999, 999999, 9999999, 99999999, 999999999, 9999999999LL, 99999999999LL, 999999999999LL,
       9999999999999LL, 99999999999999LL, 999999999999999LL, 9999999999999999LL, 99999999999999999LL,
       999999999999999999LL},  // REC_INT_YEAR 195

      {0, 9, 99, 999, 9999, 99999, 999999, 9999999, 99999999, 999999999, 9999999999LL, 99999999999LL, 999999999999LL,
       9999999999999LL, 99999999999999LL, 999999999999999LL, 9999999999999999LL, 99999999999999999LL,
       999999999999999999LL},  // REC_INT_MONTH 196

      {0, 119, 1199, 11999, 119999, 1199999, 11999999, 119999999, 1199999999LL, 11999999999LL, 119999999999LL,
       1199999999999LL, 11999999999999LL, 119999999999999LL, 1199999999999999LL, 11999999999999999LL,
       119999999999999999LL, 1199999999999999999LL, 9223372036854775807LL},  // REC_INT_YEAR_MONTH 197

      {0, 9, 99, 999, 9999, 99999, 999999, 9999999, 99999999, 999999999, 9999999999LL, 99999999999LL, 999999999999LL,
       9999999999999LL, 99999999999999LL, 999999999999999LL, 9999999999999999LL, 99999999999999999LL,
       999999999999999999LL},  // REC_INT_DAY 198

      {0, 9, 99, 999, 9999, 99999, 999999, 9999999, 99999999, 999999999, 9999999999LL, 99999999999LL, 999999999999LL,
       9999999999999LL, 99999999999999LL, 999999999999999LL, 9999999999999999LL, 99999999999999999LL,
       999999999999999999LL},  // REC_INT_HOUR 199

      {0, 239, 2399, 23999, 239999, 2399999, 23999999, 239999999, 2399999999LL, 23999999999LL, 239999999999LL,
       2399999999999LL, 23999999999999LL, 239999999999999LL, 2399999999999999LL, 23999999999999999LL,
       239999999999999999LL, 2399999999999999999LL, 9223372036854775807LL},  // REC_INT_DAY_HOUR 200

      {0, 9, 99, 999, 9999, 99999, 999999, 9999999, 99999999, 999999999LL, 9999999999LL, 99999999999LL, 999999999999LL,
       9999999999999LL, 99999999999999LL, 999999999999999LL, 9999999999999999LL, 99999999999999999LL,
       999999999999999999LL},  // REC_INT_MINUTE 201

      {0, 599, 5999, 59999, 599999, 5999999, 59999999, 599999999, 5999999999LL, 59999999999LL, 599999999999LL,
       5999999999999LL, 59999999999999LL, 599999999999999LL, 5999999999999999LL, 59999999999999999LL,
       599999999999999999LL, 5999999999999999999LL, 9223372036854775807LL},  // REC_INT_HOUR_MINUTE 202

      {0, 14399, 143999, 1439999, 14399999, 143999999, 1439999999LL, 14399999999LL, 143999999999LL, 1439999999999LL,
       14399999999999LL, 143999999999999LL, 1439999999999999LL, 14399999999999999LL, 143999999999999999LL,
       1439999999999999999LL, 9223372036854775807LL, 9223372036854775807LL,
       9223372036854775807LL},  // REC_INT_DAY_MINUTE 203

      {0, 9, 99, 999, 9999, 99999, 999999, 9999999, 99999999, 999999999LL, 9999999999LL, 99999999999LL, 999999999999LL,
       9999999999999LL, 99999999999999LL, 999999999999999LL, 9999999999999999LL, 99999999999999999LL,
       999999999999999999LL},  // REC_INT_SECOND 204

      {
          0, 599, 5999, 59999, 599999, 5999999, 59999999, 599999999, 5999999999LL, 59999999999LL, 599999999999LL,
          5999999999999LL,
          // 0123456789   01234567890   012345678901
          59999999999999LL, 599999999999999LL, 5999999999999999LL,
          // 0123456789012   01234567890123   012345678901234
          59999999999999999LL, 599999999999999999LL, 5999999999999999999LL,
          // 0123456789012345   01234567890123456   012345678901234567
          9223372036854775807LL
          // 0123456789012345678
      },  // REC_INT_MINUTE_SECOND 205

      {0, 35999, 359999, 3599999, 35999999, 359999999, 3599999999LL, 35999999999LL, 359999999999LL, 3599999999999LL,
       35999999999999LL,
       //                                          012345678901
       359999999999999LL, 3599999999999999LL, 35999999999999999LL,
       // 0123456789012    01234567890123    012345678901234
       359999999999999999LL, 3599999999999999999LL, 9223372036854775807LL,
       // 0123456789012345    01234567890123456    012345678901234567
       9223372036854775807LL, 9223372036854775807LL},  // REC_INT_HOUR_SECOND 206

      {0, 863999, 8639999, 86399999, 863999999, 8639999999LL, 86399999999LL, 863999999999LL, 8639999999999LL,
       86399999999999LL, 863999999999999LL,
       // 012345678     0123456789     01234567890     012345678901
       8639999999999999LL, 86399999999999999LL, 863999999999999999LL,
       // 0123456789012     01234567890123     012345678901234
       8639999999999999999LL, 9223372036854775807LL, 9223372036854775807LL,
       // 0123456789012345     01234567890123456
       9223372036854775807LL, 9223372036854775807LL}  // REC_INT_DAY_SECOND 207

  };

  return MaxValue[targetType][targetPrecision];
}

/////////////////////////////////////////////////////////////////////
// Check that the source value will fit in the target in
// the following cases:
//
// Source      Target
// ----------  ----------
// INTEGER     NUMERIC(n)
// NUMERIC(m)  NUMERIC(n), where m > n
// FLOAT       NUMERIC(n)
//////////////////////////////////////////////////////////////////////
ex_expr::exp_return_type checkPrecision(long source, int sourceLen, short sourceType, int sourcePrecision,
                                        int sourceScale, short targetType, int targetPrecision, int targetScale,
                                        CollHeap *heap, ComDiagsArea **diagsArea, int flags,
                                        char *sourceValue /*=NULL*/) {
  if (targetPrecision > 0) {  // special case - don't check precision
    // If the source is not an interval and the target is not an interval
    // nor an integer and the source is either a float, integer, or bigger
    if ((sourceType >= REC_MIN_NUMERIC) && (sourceType <= REC_MAX_NUMERIC) && (targetType >= REC_MIN_BINARY_NUMERIC) &&
        (targetType <= REC_MAX_BINARY_NUMERIC) &&
        ((sourceType < REC_MIN_BINARY_NUMERIC) || (sourceType > REC_MAX_BINARY_NUMERIC) || (sourcePrecision == 0) ||
         (sourcePrecision > targetPrecision))) {
      //
      // We've satisfied the above criteria.  Check that the source value
      // will fit in the target NUMERIC.
      //
      if (targetPrecision <= 18)

        if ((source < getMinDecValue(targetPrecision, targetType)) || (source > getMaxDecValue(targetPrecision))) {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, (char *)&source, sizeof(long), sourceType,
                                sourceScale, targetType, flags, -1, -1, 0, -1, sourceValue);
          return ex_expr::EXPR_ERROR;
        }
    } else if (((sourceType >= REC_MIN_INTERVAL) && (sourceType <= REC_MAX_INTERVAL) &&
                (targetType >= REC_MIN_INTERVAL) && (targetType <= REC_MAX_INTERVAL))) {
      // Always validate interval-to-interval conversions. It is
      // possible that the source was produced by an arithmetic
      // operator. Arithmetic operators do not validate their
      // results, so the cast of the result must do so even if
      // the source and target types, precisions and scales are
      // identical.

      // Do not validate interval-interval conversion if source and target
      // attributes are the same and the source value is MAX or MIN for
      // its binary storage datatype(ex. SHRT_MIN for 2 byte storage).
      // This is done to 'fix' a problem where the source values are
      // sometimes set up to be binary MIN or MAX values when keys
      // are being built. The validation out here treats them to be
      // invalid values and the query fails. This case can happen
      // when partitioning keys are being built to access sql/mp table
      // partitions.
      // Do not validate if the contents are binary MIN or MAX.
      // The contents need to be checked before doing the scale down.
      // This whole check is kind of a half kludge and
      // maybe we can come up with a better solution at a later time.
      if (sourcePrecision == targetPrecision && sourceScale == targetScale && sourceType == targetType) {
        switch (sourceLen) {
          case SQL_SMALL_SIZE:
            if (source == SHRT_MIN || source == SHRT_MAX) return ex_expr::EXPR_OK;
            break;
          case SQL_INT_SIZE:
            if (source == INT_MIN || source == INT_MAX) return ex_expr::EXPR_OK;
            break;
          case SQL_LARGE_SIZE:
            if (source == LLONG_MIN || source == LLONG_MAX) return ex_expr::EXPR_OK;
            break;
          default:
            ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, (char *)&source, sourceLen, sourceType,
                                  sourceScale, targetType, flags);
            return ex_expr::EXPR_ERROR;
        }
      }

      long scaleDownValue = 1;
      for (int i = 1; i <= sourceScale; i++) scaleDownValue *= 10;

      if (((source / scaleDownValue) < getMinIntervalValue(targetPrecision, targetType)) ||
          ((source / scaleDownValue) > getMaxIntervalValue(targetPrecision, targetType))) {
        ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, (char *)&source, sourceLen, sourceType,
                              sourceScale, targetType, flags);
        return ex_expr::EXPR_ERROR;
      }
    }

    //
    // Numeric to Interval
    //
    if (((sourceType >= REC_MIN_NUMERIC) && (sourceType <= REC_MAX_NUMERIC) && (targetType >= REC_MIN_INTERVAL) &&
         (targetType <= REC_MAX_INTERVAL)) &&
        ((sourceType < REC_MIN_BINARY_NUMERIC) ||  // ??
         (sourceType > REC_MAX_BINARY_NUMERIC) ||  // ??
         (sourcePrecision == 0) ||                 // ??
         (sourcePrecision > ((targetType == REC_INT_SECOND) ? (targetPrecision + targetScale) : targetPrecision)))) {
      //(sourcePrecision > targetPrecision)))  {

      if (targetType == REC_INT_SECOND) {
        // The precision of INTERVAL SECOND type is only for the integer
        // part, while the precision of other interval type is the number
        // of digits for entire value. Since interval is represented as
        // numeric, the conversion is really an assignment. Thus, add
        // target precision scale to check for overflow. See
        // IntervalType::getPrecision().
        // Don't get confused, which I did, if you see that the source scale
        // does not match to the target scale. The source could get its
        // scale from the original value at codeGen,
        // see ExpGenerator::matchScales() and ExpGenerator::scaleBy10x().
        // Because of the up or down scaling, the source should have the
        // right scale and can be assigned to target directly by now.
        // Maybe we should use target scale at codeGen instead.

        targetPrecision += targetScale;
      }

      // if hit, either invalid precision or need to expand the matrix
      // in getMinIntervalValue() and getMaxIntervalValue()
      assert(targetPrecision >= 0 && targetPrecision < 19);

      if ((source < getMinIntervalValue(targetPrecision, targetType)) ||
          (source > getMaxIntervalValue(targetPrecision, targetType))) {
        ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, (char *)&source, sourceLen, sourceType,
                              sourceScale, targetType, flags);
        return ex_expr::EXPR_ERROR;
      }
    }

    //
    // Interval to Numeric
    //
    if ((sourceType >= REC_MIN_INTERVAL) && (sourceType <= REC_MAX_INTERVAL) &&
        (targetType >= REC_MIN_BINARY_NUMERIC) && (targetType <= REC_MAX_BINARY_NUMERIC) &&
        (sourcePrecision > targetPrecision)) {
      if ((source < getMinDecValue(targetPrecision, targetType)) || (source > getMaxDecValue(targetPrecision))) {
        ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, (char *)&source, sourceLen, sourceType,
                              sourceScale, targetType, flags);
        return ex_expr::EXPR_ERROR;
      }
    }
  }
  return ex_expr::EXPR_OK;
}

//////////////////////////////////////////////////////////////////
// This function sets the minimum decimal value.  It looks at
// targetType to determine if the target is signed or unsigned.
// If signed, the routine generates a string of 9's and the
// sign bit is set.  If unsigned, a string of 0's is generated.
// The function assumes that targetType is one of the following:
//   REC_DECIMAL_UNSIGNED (150)
//   REC_DECIMAL_LSE (152).
///////////////////////////////////////////////////////////////////

void setMinDecValue(char *target, int targetLen, short targetType) {
  int currPos = 0;
  if (targetType == REC_DECIMAL_UNSIGNED) {  // Target is unsigned so generate zeros.
    while (currPos < targetLen) {
      target[currPos++] = '0';
    }
  } else {  // Assume target is signed so generate nines and set the sign bit.
    while (currPos < targetLen) {
      target[currPos++] = '9';
    }
    target[0] |= 0200;
  }
}

//////////////////////////////////////////////////////////////////
// This function sets the maximum value that can fit into a
// decimal type specified by the arguement.
///////////////////////////////////////////////////////////////////

void setMaxDecValue(char *target, int targetLen) {
  int currPos = 0;
  while (currPos < targetLen) {
    target[currPos++] = '9';
  }
}

//////////////////////////////////////////////////////////////////
// Function to convert an exact numeric to decimal.  Actually,
// the function converts those exact numerics that will fit into an
// int64 to decimal.
///////////////////////////////////////////////////////////////////

ex_expr::exp_return_type convExactToDec(char *target, int targetLen, short targetType, long source, CollHeap *heap,
                                        ComDiagsArea **diagsArea, int *dataConversionErrorFlag, int flags) {
  if (source < getMinDecValue(targetLen, targetType)) {
    if (dataConversionErrorFlag != 0) {
      setMinDecValue(target, targetLen, targetType);
      *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN;
    } else {
      if (targetType == REC_DECIMAL_UNSIGNED) {
        ExRaiseSqlError(heap, diagsArea, EXE_UNSIGNED_OVERFLOW);
      } else {
        ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, (char *)&source, sizeof(source), REC_BIN64_SIGNED,
                              0, targetType, flags);
      }

      return ex_expr::EXPR_ERROR;
    }
  } else if (source > getMaxDecValue(targetLen)) {
    if (dataConversionErrorFlag != 0) {
      setMaxDecValue(target, targetLen);
      *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX;
    } else {
      ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, (char *)&source, sizeof(source), REC_BIN64_SIGNED, 0,
                            targetType, flags);
      return ex_expr::EXPR_ERROR;
    }
  } else {
    // Set target to source.
    if (convInt64ToDec(target, targetLen, source, heap, diagsArea) != ex_expr::EXPR_OK) {
      return ex_expr::EXPR_ERROR;
    }
  }
  return ex_expr::EXPR_OK;
}

///////////////////////////////////////////////
// find the first non blank character
///////////////////////////////////////////////
char *str_find_first_nonpad(char *s, int len, char padChar) {
  for (int i = 0; i < len; i++)
    if (s[i] != padChar) return &s[i];
  return 0;
};

///////////////////////////////////////////////
// find the first non blank wide character
///////////////////////////////////////////////
NAWchar *wc_str_find_first_nonblank(NAWchar *s, int len) {
  for (int i = 0; i < len; i++)
    if (s[i] != unicode_char_set::space_char()) return &s[i];
  return 0;
};

////////////////////////////////////////////////
// find the first non zero(character 0) character
////////////////////////////////////////////////
char *str_find_first_nonzero(char *s, int len) {
  for (int i = 0; i < len; i++)
    if (s[i] != '0') return &s[i];
  return 0;
}

void setVCLength(char *VCLen, int VCLenSize, int value) {
  if (VCLenSize == sizeof(short)) {
    assert(value <= USHRT_MAX);
    unsigned short temp = (unsigned short)value;
    str_cpy_all(VCLen, (char *)&temp, VCLenSize);
  } else
    str_cpy_all(VCLen, (char *)&value, VCLenSize);
};

////////////////////////////////////////////////////////////////////
// the global conversion routine
////////////////////////////////////////////////////////////////////

typedef charBuf *(*unicodeToChar_FP)(const NAWcharBuf &, CollHeap *, charBuf *&, NABoolean, NABoolean);

///////////////////////////////////////////////
// Unicode to single byte string
///////////////////////////////////////////////
ex_expr::exp_return_type unicodeToSByteTarget(unicodeToChar_FP conv_func,  // convert function pointer
                                              char *source, int sourceLen, char *target, int targetLen,
                                              char *varCharLen,  // NULL if the target is of fix length
                                              int varCharLenSize, CollHeap *heap, ComDiagsArea **diagsArea,
                                              int *dataConversionErrorFlag, NABoolean allowInvalidCodePoint = TRUE,
                                              NABoolean doPad = FALSE, char padVal = ' ') {
  ex_expr::exp_return_type retcode = ex_expr::EXPR_OK;
  int copyLen, convertedLen;

  const NAWcharBuf wBuf((NAWchar *)source, sourceLen / BYTES_PER_NAWCHAR, heap);
  charBuf *cBuf = NULL;

  cBuf = (*conv_func)(wBuf, heap, cBuf, TRUE, allowInvalidCodePoint);

  if (cBuf) {
    convertedLen = cBuf->getStrLen();

    copyLen = (convertedLen < targetLen) ? convertedLen : targetLen;

    str_cpy_all(target, (const char *)cBuf->data(), copyLen);

    NADELETE(cBuf, charBuf, heap);
  } else {
    convertedLen = 0;
    copyLen = 0;

    if (allowInvalidCodePoint == FALSE) {
      char hexstr[MAX_OFFENDING_SOURCE_DATA_DISPLAY_LEN];
      ExRaiseSqlError(heap, diagsArea, EXE_INVALID_CHAR_IN_TRANSLATE_FUNC, NULL, NULL, NULL, NULL, "UNICODE",
                      "ISO88591", stringToHex(hexstr, sizeof(hexstr), source, sourceLen));
      retcode = ex_expr::EXPR_ERROR;
    }
  }

  if (varCharLen) setVCLength(varCharLen, varCharLenSize, copyLen);

  if (targetLen < convertedLen) {
    NAWchar *first_nonblank = wc_str_find_first_nonblank((NAWchar *)(&source[targetLen * BYTES_PER_NAWCHAR]),
                                                         sourceLen / BYTES_PER_NAWCHAR - targetLen);

    if (first_nonblank)  // if truncated portion isn't all blanks
    {
      if (dataConversionErrorFlag)  // want conversion flag instead of warning?
      {
        // yes - determine whether target is greater than or less than source
        if (*first_nonblank < unicode_char_set::space_char())
          // note assumption: UNICODE character set
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP;
        else
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN;
      } else
        // no - raise a warning
        ExRaiseSqlWarning(heap, diagsArea, EXE_STRING_OVERFLOW);
    }
  } else {
    if (doPad == TRUE && targetLen > convertedLen) str_pad(&target[convertedLen], targetLen - convertedLen, padVal);
  }
  return retcode;
}

///////////////////////////////////////////////
// Unicode to multiple byte string
///////////////////////////////////////////////
ex_expr::exp_return_type unicodeToMByteTarget(unicodeToChar_FP conv_func,  // conversion function pointer
                                              char *source, int sourceLen, char *target, int targetLen,
                                              int targetPrecision, int targetScale,
                                              char *varCharLen,  // NULL if the target is of fix length
                                              int varCharLenSize, CollHeap *heap, ComDiagsArea **diagsArea,
                                              int *dataConversionErrorFlag, NABoolean allowInvalidCodePoint = TRUE,
                                              NABoolean doPad = FALSE, char padVal = ' ') {
  ex_expr::exp_return_type retcode = ex_expr::EXPR_OK;

  int copyLen = 0, convertedLen = 0;
  int num_input_chars = sourceLen / BYTES_PER_NAWCHAR;

  const NAWcharBuf wBuf((NAWchar *)source, num_input_chars, heap);
  charBuf *cBuf = NULL;

  cBuf = (conv_func)(wBuf, heap, cBuf, FALSE, allowInvalidCodePoint);

  if (cBuf) {
    convertedLen = cBuf->getStrLen();

    copyLen = (convertedLen < targetLen) ? convertedLen : targetLen;

    if (targetScale == SQLCHARSETCODE_UTF8) {
      // Call lightValidateUTF8Str() to ensure we don't copy partial chars
      // or more chars than specified by target precision.
      int max_num_out_chars = copyLen;
      if (targetPrecision && (targetPrecision < num_input_chars)) max_num_out_chars = targetPrecision;
      copyLen = lightValidateUTF8Str((const char *)cBuf->data(), copyLen, max_num_out_chars, FALSE);
    }
    str_cpy_all(target, (const char *)cBuf->data(), copyLen);

    NADELETE(cBuf, charBuf, heap);
  } else {
    convertedLen = 0;
    copyLen = 0;

    if (allowInvalidCodePoint == FALSE) {
      char hexstr[MAX_OFFENDING_SOURCE_DATA_DISPLAY_LEN];
      ExRaiseSqlError(heap, diagsArea, EXE_INVALID_CHAR_IN_TRANSLATE_FUNC);
      if (*diagsArea) {
        if (targetScale == SQLCHARSETCODE_UTF8)
          *(*diagsArea) << DgString0("UNICODE") << DgString1("UTF8")
                        << DgString2(stringToHex(hexstr, sizeof(hexstr), source, sourceLen));
        else
          *(*diagsArea) << DgString0("UNICODE") << DgString1("SJIS")
                        << DgString2(stringToHex(hexstr, sizeof(hexstr), source, sourceLen));
      }
      retcode = ex_expr::EXPR_ERROR;
    }
  }

  if (varCharLen) setVCLength(varCharLen, varCharLenSize, copyLen);

  if (copyLen < convertedLen) {
    // Since WideCharToMultiByte() does not tell how many characters
    // are converted, we simply report a rounded error. We can not tell
    // if it is rounded up or down. 5/10/98
    // TBD - if needed some day: If only blanks are truncated, don't
    // generate an error.

    if (dataConversionErrorFlag)
      *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED;
    else
      ExRaiseSqlWarning(heap, diagsArea, EXE_STRING_OVERFLOW);
  }
  // If requested, blank pad so we don't leave garbage at end of target buf
  if (doPad == TRUE && targetLen > copyLen) str_pad(&target[copyLen], targetLen - copyLen, padVal);

  return retcode;
}

// helper method to convert from one single byte or variable-length encoding
// to another single byte or variable-length encoding. This method does not
// handle UCS2 sources or targets (it's also not designed for UTF-16, if
// we ever support that). It does handle things like ISO8859-1 to UTF-8
// and UTF-8 to UTF-8.

ex_expr::exp_return_type convCharToChar(char *source, int sourceLen, short sourceType, int sourcePrecision,
                                        int sourceScale, char *target, int targetLen, short targetType,
                                        int targetPrecision, int targetScale, CollHeap *heap, ComDiagsArea **diagsArea,
                                        int *dataConversionErrorFlag, int *actualTargetLen, int blankPadResult,
                                        int ignoreTrailingBlanksInSource, int allowInvalidCodePoint,
                                        char padVal = ' ') {
  char *intermediateStr = NULL;
  int intermediateStrLen = 0;
  const int stackBufferLen = 512;
  char stackBuffer[stackBufferLen];
  char *firstUntranslatedChar = NULL;
  UInt32 outputDataLen = 0;
  UInt32 translatedCharCnt = 0;
  int overflowWarningRaised = 0;

  // targetPrecision less than 0 is not correct
  if (targetPrecision < 0) targetPrecision = 0;

  if (sourceScale == SQLCHARSETCODE_UNKNOWN || targetScale == SQLCHARSETCODE_UNKNOWN) {
    // can't convert from or to an unknown charset
    ExRaiseSqlError(heap, diagsArea, EXE_INTERNAL_ERROR);
    return ex_expr::EXPR_ERROR;
  }

  if (sourceScale != SQLCHARSETCODE_UTF8 && targetScale != SQLCHARSETCODE_UTF8) {
    // neither source nor target are UTF-8, we need to do an
    // intermediate conversion to UTF-8
    intermediateStrLen = CharInfo::getMaxConvertedLenInBytes((CharInfo::CharSet)sourceScale, sourceLen, CharInfo::UTF8);
    if (intermediateStrLen <= stackBufferLen)
      intermediateStr = stackBuffer;
    else
      intermediateStr = new (heap) char[intermediateStrLen];
  }

  if (sourceScale != SQLCHARSETCODE_UTF8) {
    // convert from source to intermediate UTF-8 string or directly
    // to the UTF8 output buffer

    char *utf8Tgt = target;
    int utf8TgtLen = targetLen;

    if (intermediateStr) {
      utf8Tgt = intermediateStr;
      utf8TgtLen = intermediateStrLen;
    }

    int rc = 0;
    char *input_to_scan = source;
    int input_length = sourceLen;

    if (sourceLen == 0)
      firstUntranslatedChar = source;
    else
      rc = LocaleToUTF8(cnv_version1, source, sourceLen, utf8Tgt, utf8TgtLen, convertCharsetEnum(sourceScale),
                        firstUntranslatedChar, &outputDataLen, FALSE, &translatedCharCnt);

    if (rc && rc != CNV_ERR_BUFFER_OVERRUN) {
      ExeErrorCode errCode = EXE_INVALID_CHAR_IN_TRANSLATE_FUNC;

      if (rc != CNV_ERR_INVALID_CHAR) errCode = EXE_INTERNAL_ERROR;

      ExRaiseSqlError(heap, diagsArea, errCode);
      if (errCode == EXE_INVALID_CHAR_IN_TRANSLATE_FUNC) {
        char hexstr[MAX_OFFENDING_SOURCE_DATA_DISPLAY_LEN];
        *(*diagsArea) << DgString0(scaleToString(sourceScale)) << DgString1(scaleToString(targetScale))
                      << DgString2(stringToHex(hexstr, sizeof(hexstr), source, sourceLen));
      }
      if (intermediateStr && intermediateStr != stackBuffer) NADELETEBASIC(intermediateStr, heap);
      return ex_expr::EXPR_ERROR;
    }

    if (rc || (targetPrecision && targetPrecision < (int)translatedCharCnt)) {
      int canIgnoreOverflowChars = FALSE;
      int remainingScanStringLen = 0;

      if (rc == 0) {
        // We failed because the target has too many characters. Find
        // the first offending character.
        input_to_scan = utf8Tgt;
        input_length = outputDataLen;
        outputDataLen =
            lightValidateUTF8Str(utf8Tgt, (int)outputDataLen, (int)targetPrecision, ignoreTrailingBlanksInSource);
        assert((int)outputDataLen >= 0);
        firstUntranslatedChar = &utf8Tgt[outputDataLen];
      } else {
        assert(rc == CNV_ERR_BUFFER_OVERRUN);
      }

      if (ignoreTrailingBlanksInSource) {
        // check whether the characters from firstUntranslatedChar onwards are all blanks
        remainingScanStringLen = input_length - (firstUntranslatedChar - input_to_scan);
        canIgnoreOverflowChars = !(str_find_first_nonpad(firstUntranslatedChar, remainingScanStringLen, padVal));
      }

      if (!canIgnoreOverflowChars)
        if (dataConversionErrorFlag)  // want conversion flag instead of warning?
        {
          // yes - determine whether target is greater than or less than source
          if (*(unsigned char *)firstUntranslatedChar < padVal)  // note assumption: ASCII blank is used
            *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP;
          else
            *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN;
        } else {
          // no - raise a warning
          ExRaiseSqlWarning(heap, diagsArea, EXE_STRING_OVERFLOW);
          overflowWarningRaised = 1;
        }
    }

    if (intermediateStr)
      intermediateStrLen = outputDataLen;
    else if (actualTargetLen)
      *actualTargetLen = outputDataLen;
  }  // end sourceScale != SQLCHARSETCODE_UTF8

  if (targetScale != SQLCHARSETCODE_UTF8) {
    // convert from intermediate UTF-8 string or directly from
    // source to a non-UTF8 target
    char *src2 = (intermediateStr ? intermediateStr : source);
    int src2Len = (intermediateStr ? intermediateStrLen : sourceLen);

    if (sourceScale == SQLCHARSETCODE_UTF8)
      src2Len = Attributes::trimFillerSpaces(src2, sourcePrecision, src2Len, (CharInfo::CharSet)sourceScale);
    int rc = 0;

    if (src2Len == 0) {
      outputDataLen = translatedCharCnt = 0;
      firstUntranslatedChar = src2;
    } else
      rc = UTF8ToLocale(
          cnv_version1, src2, src2Len, target, targetLen, convertCharsetEnum(targetScale), firstUntranslatedChar,
          &outputDataLen, FALSE, allowInvalidCodePoint, &translatedCharCnt,
          (allowInvalidCodePoint ? CharInfo::getReplacementCharacter((CharInfo::CharSet)targetScale) : NULL));

    if (rc) {
      int canIgnoreOverflowChars = FALSE;

      if (ignoreTrailingBlanksInSource && rc == CNV_ERR_BUFFER_OVERRUN) {
        int remainingSourceStringLen = src2Len - (firstUntranslatedChar - src2);
        // check whether the characters from firstUntranslatedChar onwards are all blanks
        canIgnoreOverflowChars = !(str_find_first_nonpad(firstUntranslatedChar, remainingSourceStringLen, padVal));
      }

      if (!canIgnoreOverflowChars) {
        // want conversion flag instead of warning?
        if (dataConversionErrorFlag && (rc == CNV_ERR_BUFFER_OVERRUN || rc == CNV_ERR_INVALID_CHAR)) {
          // yes - determine whether target is greater than or less than source
          if (*(unsigned char *)firstUntranslatedChar < padVal)  // note assumption: ASCII blank is used
            *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP;
          else
            *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN;
        } else {
          // no, raise error or warning
          if (rc == CNV_ERR_BUFFER_OVERRUN) {
            if (!overflowWarningRaised) ExRaiseSqlWarning(heap, diagsArea, EXE_STRING_OVERFLOW);
          } else {
            ExeErrorCode errCode = EXE_INVALID_CHAR_IN_TRANSLATE_FUNC;

            if (rc != CNV_ERR_INVALID_CHAR) errCode = EXE_INTERNAL_ERROR;

            ExRaiseSqlError(heap, diagsArea, errCode);
            if (errCode == EXE_INVALID_CHAR_IN_TRANSLATE_FUNC) {
              char hexstr[MAX_OFFENDING_SOURCE_DATA_DISPLAY_LEN];
              *(*diagsArea) << DgString0(scaleToString(sourceScale)) << DgString1(scaleToString(targetScale))
                            << DgString2(stringToHex(hexstr, sizeof(hexstr), source, sourceLen));
            }
            if (intermediateStr && intermediateStr != stackBuffer) NADELETEBASIC(intermediateStr, heap);
            return ex_expr::EXPR_ERROR;
          }
        }
      }
    }

    if (intermediateStr && intermediateStr != stackBuffer) NADELETEBASIC(intermediateStr, heap);

    // max. char count should be enforced only for UTF-8 data values for now
    // and the target is not a UTF-8 value, validate this assumption here
    assert(!(targetPrecision && targetPrecision < (int)translatedCharCnt));
  }  // end targetScale != SQLCHARSETCODE_UTF8
  else if (sourceScale == SQLCHARSETCODE_UTF8) {
    // both source and target charsets are UTF-8; do a byte copy, but also check
    // character length semantics, if necessary
    outputDataLen = targetLen;
    if (sourceLen < targetLen) outputDataLen = sourceLen;

    NABoolean needToIgnoreFillerBlanksInSource =
        (sourcePrecision && DFS2REC::isSQLFixedChar(sourceType) && DFS2REC::isAnyVarChar(targetType));

    int precisionToCheck = sourcePrecision ? sourcePrecision : targetPrecision;
    if (targetPrecision && targetPrecision < sourcePrecision) precisionToCheck = targetPrecision;

    if (needToIgnoreFillerBlanksInSource) {
      if (precisionToCheck > 0)
        outputDataLen = lightValidateUTF8Str(source, outputDataLen, precisionToCheck, ignoreTrailingBlanksInSource);
      if ((int)outputDataLen < 0) {
        // source string is not valid UTF-8
        ExRaiseSqlError(heap, diagsArea, EXE_INVALID_CHAR_IN_TRANSLATE_FUNC);
        char hexstr[MAX_OFFENDING_SOURCE_DATA_DISPLAY_LEN];
        *(*diagsArea) << DgString0(scaleToString(sourceScale)) << DgString1(scaleToString(targetScale))
                      << DgString2(stringToHex(hexstr, sizeof(hexstr), source, sourceLen));
        return ex_expr::EXPR_ERROR;
      }
    }
    str_cpy_all(target, source, outputDataLen);

    if (targetLen < sourceLen ||
        (targetPrecision && targetPrecision < (sourcePrecision ? sourcePrecision : sourceLen))) {
      // Check for truncation of whole or partial characters and for violation of
      // a max. number of characters in the target

      int relevantCharsGotTruncated = FALSE;

      if (!needToIgnoreFillerBlanksInSource)  // If we didn't call this before now
        outputDataLen = lightValidateUTF8Str(target, outputDataLen, precisionToCheck, ignoreTrailingBlanksInSource);

      if ((int)outputDataLen < sourceLen) {
        if ((int)outputDataLen < 0) {
          // source string is not valid UTF-8
          ExRaiseSqlError(heap, diagsArea, EXE_INVALID_CHAR_IN_TRANSLATE_FUNC);
          char hexstr[MAX_OFFENDING_SOURCE_DATA_DISPLAY_LEN];
          *(*diagsArea) << DgString0(scaleToString(sourceScale)) << DgString1(scaleToString(targetScale))
                        << DgString2(stringToHex(hexstr, sizeof(hexstr), source, sourceLen));
          return ex_expr::EXPR_ERROR;
        }

        // not all bytes of the source got copied, check whether any relevant
        // information got truncated
        if (ignoreTrailingBlanksInSource) {
          int remainingSourceStringLen = sourceLen - outputDataLen;
          // check whether the characters from first truncated char onwards are all blanks
          relevantCharsGotTruncated =
              (str_find_first_nonpad(&(source[outputDataLen]), remainingSourceStringLen, padVal) != NULL);
        } else
          relevantCharsGotTruncated = TRUE;
      }

      if (relevantCharsGotTruncated) {
        if (dataConversionErrorFlag)  // want conversion flag instead of warning?
        {
          char *firstUntranslatedChar = &source[outputDataLen];

          // yes - determine whether target is greater than or less than source
          if (*(unsigned char *)firstUntranslatedChar < padVal)  // note assumption: ASCII blank is used
            *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP;
          else
            *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN;
        } else {
          // no - raise a warning
          ExRaiseSqlWarning(heap, diagsArea, EXE_STRING_OVERFLOW);
        }
      }
    }
  }  // end both source and target are UTF-8

  // blank-pad and set remaining output parameters
  if (blankPadResult && (int)outputDataLen < targetLen) {
    str_pad(&target[outputDataLen], targetLen - (int)outputDataLen, padVal);
    outputDataLen = targetLen;
  }

  if (actualTargetLen) *actualTargetLen = outputDataLen;

  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type convHiveDecimalToNumeric(char *sourceData, int srcLen, char *targetData, Attributes *attr,
                                                  CollHeap *heap, ComDiagsArea **diagsArea) {
  // Handle hive 'decimal' values.
  // These are represented in traf as NUMERIC(p,s),
  //  where precision and/or scale is greater than 0.
  //
  // Values from hive are returned after upscaling it.
  //  (Ex: 1.23 will be returned as 123)
  //
  // values in range of 1..18 digits are returned as long.
  // Values beyond that are returned as string.

  // Returned data:
  //  2 bytes precision, 2 bytes scale
  //  If long, then 8 bytes data
  //  If string, then stringlen bytes of data
  //     (stringlen = total len - (2+2)
  //
  int copyLen = 0;
  char *numData = sourceData;
  Int16 extValPrecision = *(Int16 *)numData;
  numData += sizeof(Int16);
  Int16 extValScale = *(Int16 *)numData;
  numData += sizeof(Int16);

  Float64 extDoubleVal = 0;
  int extInt32Val = 0;
  Int16 extInt16Val = 0;
  Int16 extInt8Val = 0;
  char *copyData = NULL;
  NABoolean dataIsNumeric = FALSE;
  if (extValPrecision <= 18)  // short, int, long
  {
    long extInt64Val = *(long *)numData;

    // upscale by actual scale minus returned scale
    int lv_scale_diff = attr->getScale() - extValScale;
    extInt64Val = extInt64Val * pow(10, lv_scale_diff);

    // convert returned value to expected datatype.
    // expected datatype can only be signed as hive doesn't
    // support unsigned decimal datatype.
    switch (attr->getDatatype()) {
      case REC_BIN8_SIGNED: {
        extInt8Val = (Int8)extInt64Val;
        copyData = (char *)&extInt8Val;
        copyLen = sizeof(Int8);
      } break;

      case REC_BIN16_SIGNED: {
        extInt16Val = (Int16)extInt64Val;
        copyData = (char *)&extInt16Val;
        copyLen = sizeof(Int16);
      } break;

      case REC_BIN32_SIGNED: {
        extInt32Val = (int)extInt64Val;
        copyData = (char *)&extInt32Val;
        copyLen = sizeof(int);
      } break;

      case REC_BIN64_SIGNED: {
        copyData = (char *)&extInt64Val;
        copyLen = sizeof(long);
      } break;

      case REC_NUM_BIG_SIGNED: {
        ex_expr::exp_return_type rc =
            convDoIt((char *)&extInt64Val, sizeof(long), REC_BIN64_SIGNED, attr->getPrecision(), attr->getScale(),
                     targetData, attr->getLength(), REC_NUM_BIG_SIGNED, attr->getPrecision(), attr->getScale(), NULL, 0,
                     heap, diagsArea, CONV_BIN64S_BIGNUM, NULL, 0);
        if (rc == ex_expr::EXPR_ERROR) return rc;

        copyLen = -1;
      } break;

      default: {
        // error
        return ex_expr::EXPR_ERROR;
      }

    }  // switch

    if (copyLen > 0) str_cpy_all(targetData, copyData, copyLen);
  } else {
    // data length > 18. This is bignum.
    copyLen = srcLen - (sizeof(Int16) + sizeof(Int16));

    ex_expr::exp_return_type rc =
        convDoIt(numData, copyLen, REC_BYTE_F_ASCII, 0, 0, targetData, attr->getLength(), REC_NUM_BIG_SIGNED,
                 attr->getPrecision(), attr->getScale(), NULL, 0, heap, diagsArea, CONV_ASCII_BIGNUM, NULL, 0);
    if (rc == ex_expr::EXPR_ERROR) return ex_expr::EXPR_ERROR;
  }  // num big signed

  return ex_expr::EXPR_OK;
}

////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////
//                                                                //
//                the global conversion routine                   //
//                                                                //
////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////

ex_expr::exp_return_type convDoIt(char *source, int sourceLen, short sourceType, int sourcePrecision, int sourceScale,
                                  char *target, int targetLen, short targetType, int targetPrecision, int targetScale,
                                  char *varCharLen,    // NULL if not a varChar
                                  int varCharLenSize,  // 0 if not a varChar
                                  CollHeap *heap, ComDiagsArea **diagsArea, ConvInstruction index,
                                  int *dataConversionErrorFlag, int flags) {
  UInt32 rc;

  // convDoIt main code
  static const unsigned short limits[] = {1, 3, 7, 15, 31, 63, 127, 255, 511, 1023, 2047, 4095, 8191, 16383, 32767};

  // To compute 10^scale . scale is the fractional precision for Intervals,
  // valid range 0-6 (the trailing zeroes would "assert" on invalid values)
  static const long tenToPowerOf[] = {1,       10,       100,       1000,       10000,      100000,
                                      1000000, 10000000, 100000000, 1000000000, 10000000000};

  // variable used for float operation. Specified in "pragma refligned..."
  float *floatSrcPtr, *floatTgtPtr;
  double *doubleSrcPtr, *doubleTgtPtr;

  floatSrcPtr = (float *)source;
  doubleSrcPtr = (double *)source;
  floatTgtPtr = (float *)target;
  doubleTgtPtr = (double *)target;

  NABoolean leftPad = FALSE;
  NABoolean allowSignInInterval = FALSE;
  int tempFlags = 0;
  // We will truncate tail '0' in default
  NABoolean bTrimTailZero = TRUE;
  if (flags) {
    if (flags & CONV_LEFT_PAD) leftPad = TRUE;
    if (flags & CONV_ALLOW_SIGN_IN_INTERVAL) allowSignInInterval = TRUE;
    // tempFlags must be passed to any recursive calls to convdoit
    // within this method and methods that call ExRaiseDetailSqlError.
    // Also must be passed to ExRaiseDetailedSqlError.
    if (flags & CONV_CONTROL_LOOPING) tempFlags = CONV_CONTROL_LOOPING;
    if (flags & CONV_INTERMEDIATE_CONVERSION) tempFlags |= CONV_INTERMEDIATE_CONVERSION;

    if (flags & CONV_TREAT_ALL_SPACES_AS_ZERO) tempFlags |= CONV_TREAT_ALL_SPACES_AS_ZERO;
    if (flags & CONV_NOT_TRIM_TAIL_ZERO) bTrimTailZero = FALSE;
  }

  if (index == CONV_UNKNOWN || index == CONV_UNKNOWN_LEFTPAD) {
    ex_conv_clause tempClause;

    if (index == CONV_UNKNOWN_LEFTPAD) leftPad = TRUE;

    index = tempClause.findInstruction(sourceType, sourceLen, targetType, targetLen, sourceScale - targetScale);
  };

  switch (index) {
    case CONV_BPINTU_BPINTU: {
      if (*(unsigned short *)source > limits[targetPrecision - 1]) {
        if (dataConversionErrorFlag != 0) {
          *(unsigned short *)target = limits[targetPrecision - 1];
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX;
        } else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                targetType, tempFlags);
          return ex_expr::EXPR_ERROR;
        }
      } else {
        *(unsigned short *)target = *(unsigned short *)source;
      }
    } break;

    case CONV_BIN16S_BPINTU: {
      if (*(short *)source < 0) {
        if (dataConversionErrorFlag != 0) {
          *(unsigned short *)target = 0;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN;
        } else {
          ExRaiseSqlError(heap, diagsArea, EXE_UNSIGNED_OVERFLOW);
          return ex_expr::EXPR_ERROR;
        }
      } else if (*(unsigned short *)source > limits[targetPrecision - 1]) {
        if (dataConversionErrorFlag != 0) {
          *(unsigned short *)target = limits[targetPrecision - 1];
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX;
        } else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                targetType, tempFlags);
          return ex_expr::EXPR_ERROR;
        }
      } else {
        *(unsigned short *)target = (unsigned short)*(short *)source;
      }
    }

    case CONV_BIN8S_BIN8S: {
      if (dataConversionErrorFlag != 0) {
        *(Int8 *)target = *(Int8 *)source;
      } else {
        if (checkPrecision((long)*(Int8 *)source, sourceLen, sourceType, sourcePrecision, sourceScale, targetType,
                           targetPrecision, targetScale, heap, diagsArea, tempFlags) == ex_expr::EXPR_OK) {
          *(Int8 *)target = *(Int8 *)source;
        } else {
          return ex_expr::EXPR_ERROR;
        }
      }
    } break;

    case CONV_BIN8U_BIN8U: {
      if (dataConversionErrorFlag != 0) {
        *(UInt8 *)target = *(UInt8 *)source;
      } else {
        if (checkPrecision((long)*(UInt8 *)source, sourceLen, sourceType, sourcePrecision, sourceScale, targetType,
                           targetPrecision, targetScale, heap, diagsArea, tempFlags) == ex_expr::EXPR_OK) {
          *(UInt8 *)target = *(UInt8 *)source;
        } else {
          return ex_expr::EXPR_ERROR;
        }
      }
    } break;

    case CONV_BIN8S_BIN8U: {
      if (*(Int8 *)source < 0) {
        if (dataConversionErrorFlag != 0)  // Capture error in variable?
        {
          *(UInt8 *)target = 0;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN;
        } else {
          ExRaiseSqlError(heap, diagsArea, EXE_UNSIGNED_OVERFLOW);
          return ex_expr::EXPR_ERROR;
        }
      } else {
        if (dataConversionErrorFlag != 0) {
          *(UInt8 *)target = *(Int8 *)source;
        } else {
          if (checkPrecision((long)*(Int8 *)source, sourceLen, sourceType, sourcePrecision, sourceScale, targetType,
                             targetPrecision, targetScale, heap, diagsArea, tempFlags) == ex_expr::EXPR_OK) {
            *(UInt8 *)target = *(Int8 *)source;
          } else {
            return ex_expr::EXPR_ERROR;
          }
        }
      }
    } break;

    case CONV_BIN8U_BIN8S: {
      if (*(UInt8 *)source > SCHAR_MAX) {
        if (dataConversionErrorFlag != 0)  // Capture error in variable?
        {
          *(Int8 *)target = SCHAR_MAX;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX;
        } else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                targetType, tempFlags);
          return ex_expr::EXPR_ERROR;
        }
      } else {
        if (dataConversionErrorFlag != 0) {  // Set the target value.
          *(Int8 *)target = *(UInt8 *)source;
        } else {  // Check target precision. Then set target value.
          if (checkPrecision((long)*(UInt8 *)source, sourceLen, sourceType, sourcePrecision, sourceScale, targetType,
                             targetPrecision, targetScale, heap, diagsArea, tempFlags) == ex_expr::EXPR_OK) {
            *(Int8 *)target = *(UInt8 *)source;
          } else {
            return ex_expr::EXPR_ERROR;
          }
        }
      }
    } break;

    case CONV_BIN8S_BIN16S: {
      *(Int16 *)target = *(Int8 *)source;
    } break;

    case CONV_BIN8U_BIN16U: {
      *(UInt16 *)target = *(UInt8 *)source;
    } break;

    case CONV_BIN8U_BIN16S: {
      *(Int16 *)target = *(UInt8 *)source;
    } break;

    case CONV_BIN8S_BIN32S: {
      *(int *)target = *(Int8 *)source;
    } break;

    case CONV_BIN8S_BIN64S: {
      *(long *)target = *(Int8 *)source;
    } break;

    case CONV_BIN8U_BIN32U: {
      *(UInt32 *)target = *(UInt8 *)source;
    } break;

    case CONV_BIN8U_BIN64U: {
      *(UInt64 *)target = *(UInt8 *)source;
    } break;

    case CONV_BIN16U_BIN8S: {
      if (*(UInt16 *)source > SCHAR_MAX) {
        if (dataConversionErrorFlag != 0)  // Capture error in variable?
        {
          *(Int8 *)target = SCHAR_MAX;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX;
        } else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                targetType, tempFlags);
          return ex_expr::EXPR_ERROR;
        }
      } else {
        if (dataConversionErrorFlag != 0) {  // Set the target value.
          *(Int8 *)target = (Int8) * (UInt16 *)source;
        } else {  // Check target precision. Then set target value.
          if (checkPrecision((long)*(UInt16 *)source, sourceLen, sourceType, sourcePrecision, sourceScale, targetType,
                             targetPrecision, targetScale, heap, diagsArea, tempFlags) == ex_expr::EXPR_OK) {
            *(Int8 *)target = (Int8) * (UInt16 *)source;
          } else {
            return ex_expr::EXPR_ERROR;
          }
        }
      }
    } break;

    case CONV_BIN16S_BIN8U: {
      if (*(Int16 *)source < 0) {
        if (dataConversionErrorFlag != 0)  // Capture error in variable?
        {
          *(UInt8 *)target = 0;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN;
        } else {
          ExRaiseSqlError(heap, diagsArea, EXE_UNSIGNED_OVERFLOW);
          return ex_expr::EXPR_ERROR;
        }
      } else if (*(Int16 *)source > UCHAR_MAX) {
        if (dataConversionErrorFlag != 0) {
          *(UInt8 *)target = UCHAR_MAX;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX;
        } else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                targetType, tempFlags);
          return ex_expr::EXPR_ERROR;
        }
      } else {
        if (dataConversionErrorFlag != 0) {
          *(UInt8 *)target = (UInt8) * (Int16 *)source;
        } else {
          if (checkPrecision((long)*(Int16 *)source, sourceLen, sourceType, sourcePrecision, sourceScale, targetType,
                             targetPrecision, targetScale, heap, diagsArea, tempFlags) == ex_expr::EXPR_OK) {
            *(UInt8 *)target = (UInt8) * (Int16 *)source;
          } else {
            return ex_expr::EXPR_ERROR;
          }
        }
      }
    } break;

    case CONV_BIN16S_BIN8S: {
      if (*(Int16 *)source < SCHAR_MIN) {
        if (dataConversionErrorFlag != 0)  // Capture error in variable?
        {
          *(Int8 *)target = SCHAR_MIN;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN;
        } else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                targetType, tempFlags);
          return ex_expr::EXPR_ERROR;
        }
      } else if (*(Int16 *)source > SCHAR_MAX) {
        if (dataConversionErrorFlag != 0)  // Capture error in variable?
        {
          *(Int8 *)target = SCHAR_MAX;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX;
        } else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                targetType, tempFlags);
          return ex_expr::EXPR_ERROR;
        }
      } else {
        if (dataConversionErrorFlag != 0) {  // Set the target value.
          *(Int8 *)target = (Int8) * (Int16 *)source;
        } else {  // Check target precision. Then set target value.
          if (checkPrecision((long)*(Int16 *)source, sourceLen, sourceType, sourcePrecision, sourceScale, targetType,
                             targetPrecision, targetScale, heap, diagsArea, tempFlags) == ex_expr::EXPR_OK) {
            *(Int8 *)target = (Int8) * (Int16 *)source;
          } else {
            return ex_expr::EXPR_ERROR;
          }
        }
      }
    } break;

    case CONV_BIN16U_BIN8U: {
      if (*(UInt16 *)source > UCHAR_MAX) {
        if (dataConversionErrorFlag != 0)  // Capture error in variable?
        {
          *(UInt8 *)target = UCHAR_MAX;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX;
        } else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                targetType, tempFlags);
          return ex_expr::EXPR_ERROR;
        }
      } else {
        if (dataConversionErrorFlag != 0) {  // Set the target value.
          *(UInt8 *)target = (UInt8) * (UInt16 *)source;
        } else {  // Check target precision. Then set target value.
          if (checkPrecision((long)*(UInt16 *)source, sourceLen, sourceType, sourcePrecision, sourceScale, targetType,
                             targetPrecision, targetScale, heap, diagsArea, tempFlags) == ex_expr::EXPR_OK) {
            *(UInt8 *)target = (UInt8) * (UInt16 *)source;
          } else {
            return ex_expr::EXPR_ERROR;
          }
        }
      }
    } break;

    case CONV_NUMERIC_BIN8S:
    case CONV_NUMERIC_BIN8U: {
      // convert source to smallint and detect data conversion error.
      if (dataConversionErrorFlag) *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_OK;
      Int16 temp;
      ex_expr::exp_return_type rc =
          convDoIt(source, sourceLen, sourceType, sourcePrecision, sourceScale, (char *)&temp, sizeof(Int16),
                   (index == CONV_NUMERIC_BIN8S ? REC_BIN16_SIGNED : REC_BIN16_UNSIGNED), targetPrecision, targetScale,
                   NULL,  // varCharLen
                   0,     // varCharLenSize
                   heap, diagsArea, CONV_UNKNOWN, dataConversionErrorFlag, 0);
      if (rc == ex_expr::EXPR_ERROR) return ex_expr::EXPR_ERROR;

      if ((!dataConversionErrorFlag) || (*dataConversionErrorFlag == ex_conv_clause::CONV_RESULT_OK)) {
        // no conversion error when converting to smallint.
        // Now check to see if there is an error converting to tinyint.
        rc = convDoIt((char *)&temp, sizeof(Int16),
                      (index == CONV_NUMERIC_BIN8S ? REC_BIN16_SIGNED : REC_BIN16_UNSIGNED), targetPrecision,
                      targetScale, target, targetLen, targetType, targetPrecision, targetScale,
                      NULL,  // varCharLen
                      0,     // varCharLenSize
                      heap, diagsArea, CONV_UNKNOWN, dataConversionErrorFlag, 0);
        if (rc == ex_expr::EXPR_ERROR) return ex_expr::EXPR_ERROR;
      } else  // data conversion error. Move tinyint max/min to target.
      {
        if (*dataConversionErrorFlag == ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN) {
          if (index == CONV_NUMERIC_BIN8S)
            *(Int8 *)target = SCHAR_MIN;
          else
            *(UInt8 *)target = 0;
        } else if (*dataConversionErrorFlag == ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX) {
          if (index == CONV_NUMERIC_BIN8S)
            *(Int8 *)target = SCHAR_MAX;
          else
            *(UInt8 *)target = UCHAR_MAX;
        }
      }
    } break;

    case CONV_BIN8S_ASCII: {
      if (convInt64ToAscii(target, targetLen, targetPrecision, targetScale, (long)*(Int8 *)source, sourceScale,
                           varCharLen, varCharLenSize,
                           ' ',  // filler character
                           FALSE, leftPad, FALSE, heap, diagsArea, bTrimTailZero) != ex_expr::EXPR_OK)
        return ex_expr::EXPR_ERROR;
    }; break;

    case CONV_BIN8U_ASCII: {
      if (convInt64ToAscii(target, targetLen, targetPrecision, targetScale, (long)*(UInt8 *)source, sourceScale,
                           varCharLen, varCharLenSize,
                           ' ',  // filler character
                           FALSE, leftPad, FALSE, heap, diagsArea, bTrimTailZero) != ex_expr::EXPR_OK)
        return ex_expr::EXPR_ERROR;
    }; break;

    case CONV_BIN16S_BIN16S: {
      if (dataConversionErrorFlag != 0) {
        *(short *)target = *(short *)source;
      } else {
        if (checkPrecision((long)*(short *)source, sourceLen, sourceType, sourcePrecision, sourceScale, targetType,
                           targetPrecision, targetScale, heap, diagsArea, tempFlags) == ex_expr::EXPR_OK) {
          *(short *)target = *(short *)source;
        } else {
          return ex_expr::EXPR_ERROR;
        }
      }
    } break;

    case CONV_BIN16S_BIN16U: {
      if (*(short *)source < 0) {
        if (dataConversionErrorFlag != 0)  // Capture error in variable?
        {
          *(unsigned short *)target = 0;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN;
        } else {
          ExRaiseSqlError(heap, diagsArea, EXE_UNSIGNED_OVERFLOW);
          return ex_expr::EXPR_ERROR;
        }
      } else {
        if (dataConversionErrorFlag != 0) {  // Set the target value.
          *(unsigned short *)target = *(short *)source;
        } else {  // Check target precision. Then set target value.
          if (checkPrecision((long)*(short *)source, sourceLen, sourceType, sourcePrecision, sourceScale, targetType,
                             targetPrecision, targetScale, heap, diagsArea, tempFlags) == ex_expr::EXPR_OK) {
            *(unsigned short *)target = *(short *)source;
          } else {
            return ex_expr::EXPR_ERROR;
          }
        }
      }
    } break;

    case CONV_BIN16S_BIN32S: {
      *(int *)target = *(short *)source;
    } break;

    case CONV_BIN16S_BIN32U: {
      if (*(short *)source < 0) {
        if (dataConversionErrorFlag != 0)  // Capture error in variable?
        {
          *(int *)target = 0;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN;
        } else {
          ExRaiseSqlError(heap, diagsArea, EXE_UNSIGNED_OVERFLOW);
          return ex_expr::EXPR_ERROR;
        }
      } else {  // Set the target value.
        *(int *)target = *(short *)source;
      }
    } break;

    case CONV_BIN16S_BIN64S: {
      *(long *)target = *(short *)source;
    } break;

    case CONV_BIN16S_DECU:
    case CONV_BIN16S_DECS: {
      if (convExactToDec(target, targetLen, targetType, (long)*(short *)source, heap, diagsArea,
                         dataConversionErrorFlag, tempFlags) != ex_expr::EXPR_OK) {
        return ex_expr::EXPR_ERROR;
      }
    } break;

    case CONV_BIN16S_FLOAT32: {
      //      *(float *)target = *(short *)source;
      *floatTgtPtr = *(short *)source;
    } break;

    case CONV_BIN16S_FLOAT64: {
      //      *(double *)target = *(short *)source;
      *doubleTgtPtr = *(short *)source;
    } break;

    case CONV_BIN16S_BIGNUMU: {
      if (*(short *)source < 0) {
        ExRaiseSqlError(heap, diagsArea, EXE_UNSIGNED_OVERFLOW);
        return ex_expr::EXPR_ERROR;
      };
    };
      // no break! from here on CONV_BIN16S_BIGNUMU is identical with
      // CONV_BIN16S_BIGNUM

    case CONV_BIN16S_BIGNUM: {
      return convInt64ToBigNum(target, targetLen, (long)*(short *)source, heap, diagsArea, tempFlags);
    }; break;

    case CONV_BIN16S_ASCII: {
      if (convInt64ToAscii(target, targetLen, targetPrecision, targetScale, (long)*(short *)source, sourceScale,
                           varCharLen, varCharLenSize,
                           ' ',  // filler character
                           FALSE, leftPad, FALSE, heap, diagsArea, bTrimTailZero) != ex_expr::EXPR_OK)
        return ex_expr::EXPR_ERROR;
    }; break;

    case CONV_BIN16U_BPINTU: {
      if (*(unsigned short *)source > limits[targetPrecision - 1]) {
        if (dataConversionErrorFlag != 0) {
          *(unsigned short *)target = limits[targetPrecision - 1];
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX;
        } else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                targetType, tempFlags);
          return ex_expr::EXPR_ERROR;
        }
      } else {
        *(unsigned short *)target = *(unsigned short *)source;
      }
    } break;

    case CONV_BIN16U_BIN16S: {
      if (*(unsigned short *)source > SHRT_MAX) {
        if (dataConversionErrorFlag != 0)  // Capture error in variable?
        {
          *(short *)target = SHRT_MAX;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX;
        } else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                targetType, tempFlags);
          return ex_expr::EXPR_ERROR;
        }
      } else {
        if (dataConversionErrorFlag != 0) {  // Set the target value.
          *(short *)target = *(unsigned short *)source;
        } else {  // Check target precision. Then set target value.
          if (checkPrecision((long)*(unsigned short *)source, sourceLen, sourceType, sourcePrecision, sourceScale,
                             targetType, targetPrecision, targetScale, heap, diagsArea,
                             tempFlags) == ex_expr::EXPR_OK) {
            *(short *)target = *(unsigned short *)source;
          } else {
            return ex_expr::EXPR_ERROR;
          }
        }
      }
    } break;

    case CONV_BIN16U_BIN16U: {
      if (dataConversionErrorFlag != 0) {
        *(unsigned short *)target = *(unsigned short *)source;
      } else {
        if (checkPrecision((long)*(unsigned short *)source, sourceLen, sourceType, sourcePrecision, sourceScale,
                           targetType, targetPrecision, targetScale, heap, diagsArea, tempFlags) == ex_expr::EXPR_OK) {
          *(unsigned short *)target = *(unsigned short *)source;
        } else {
          return ex_expr::EXPR_ERROR;
        }
      }
    } break;

    case CONV_BIN16U_BIN32S: {
      *(int *)target = *(unsigned short *)source;
    }; break;

    case CONV_BIN16U_BIN32U: {
      *(int *)target = *(unsigned short *)source;
    }; break;

    case CONV_BIN16U_BIN64S: {
      *(long *)target = *(unsigned short *)source;
    }; break;

    case CONV_BIN16U_DECS:
      // covers conversion to DECS and DECU (see exp_fixup.cpp)
      {
        if (convExactToDec(target, targetLen, targetType, (long)*(unsigned short *)source, heap, diagsArea,
                           dataConversionErrorFlag, tempFlags) != ex_expr::EXPR_OK) {
          return ex_expr::EXPR_ERROR;
        }
      }
      break;

    case CONV_BIN16U_FLOAT32: {
      *floatTgtPtr = *(unsigned short *)source;
    }; break;

    case CONV_BIN16U_FLOAT64: {
      *doubleTgtPtr = *(unsigned short *)source;
    }; break;

    case CONV_BIN16U_BIGNUM: {
      return convInt64ToBigNum(target, targetLen, (long)*(unsigned short *)source, heap, diagsArea, tempFlags);
    }; break;

    case CONV_BIN16U_ASCII: {
      if (convInt64ToAscii(target, targetLen, targetPrecision, targetScale, (long)*(unsigned short *)source,
                           sourceScale, varCharLen, varCharLenSize,
                           ' ',  // filler character
                           FALSE, leftPad, FALSE, heap, diagsArea, bTrimTailZero) != ex_expr::EXPR_OK)
        return ex_expr::EXPR_ERROR;
    }; break;

    case CONV_BIN32S_BIN16S: {
      if (*(int *)source < SHRT_MIN) {
        if (dataConversionErrorFlag != 0)  // Capture error in variable?
        {
          *(short *)target = SHRT_MIN;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN;
        } else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                targetType, tempFlags);
          return ex_expr::EXPR_ERROR;
        }
      } else if (*(int *)source > SHRT_MAX) {
        if (dataConversionErrorFlag != 0)  // Capture error in variable?
        {
          *(short *)target = SHRT_MAX;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX;
        } else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                targetType, tempFlags);
          return ex_expr::EXPR_ERROR;
        }
      } else {
        if (dataConversionErrorFlag != 0) {  // Set the target value.
          *(short *)target = (short)*(int *)source;
        } else {  // Check target precision. Then set target value.
          if (checkPrecision((long)*(int *)source, sourceLen, sourceType, sourcePrecision, sourceScale, targetType,
                             targetPrecision, targetScale, heap, diagsArea, tempFlags) == ex_expr::EXPR_OK) {
            *(short *)target = (short)*(int *)source;
          } else {
            return ex_expr::EXPR_ERROR;
          }
        }
      }
    } break;

    case CONV_BIN32S_BPINTU: {
      if (*(int *)source < 0) {
        if (dataConversionErrorFlag != 0) {
          *(unsigned short *)target = 0;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN;
        } else {
          ExRaiseSqlError(heap, diagsArea, EXE_UNSIGNED_OVERFLOW);
          return ex_expr::EXPR_ERROR;
        }
      } else if (*(int *)source > (int)limits[targetPrecision - 1]) {
        if (dataConversionErrorFlag != 0) {
          *(unsigned short *)target = limits[targetPrecision - 1];
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX;
        } else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                targetType, tempFlags);
          return ex_expr::EXPR_ERROR;
        }
      } else {
        *(unsigned short *)target = (unsigned short)*(int *)source;
      }
    } break;

    case CONV_BIN32S_BIN16U: {
      if (*(int *)source < 0) {
        if (dataConversionErrorFlag != 0)  // Capture error in variable?
        {
          *(unsigned short *)target = 0;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN;
        } else {
          ExRaiseSqlError(heap, diagsArea, EXE_UNSIGNED_OVERFLOW);
          return ex_expr::EXPR_ERROR;
        }
      } else if (*(int *)source > USHRT_MAX) {
        if (dataConversionErrorFlag != 0) {
          *(unsigned short *)target = USHRT_MAX;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX;
        } else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                targetType, tempFlags);
          return ex_expr::EXPR_ERROR;
        }
      } else {
        if (dataConversionErrorFlag != 0) {
          *(unsigned short *)target = (unsigned short)*(int *)source;
        } else {
          if (checkPrecision((long)*(int *)source, sourceLen, sourceType, sourcePrecision, sourceScale, targetType,
                             targetPrecision, targetScale, heap, diagsArea, tempFlags) == ex_expr::EXPR_OK) {
            *(unsigned short *)target = (unsigned short)*(int *)source;
          } else {
            return ex_expr::EXPR_ERROR;
          }
        }
      }
    } break;

    case CONV_BIN32S_BIN32S: {
      if (dataConversionErrorFlag != 0) {
        *(int *)target = *(int *)source;
      } else {
        if (checkPrecision((long)*(int *)source, sourceLen, sourceType, sourcePrecision, sourceScale, targetType,
                           targetPrecision, targetScale, heap, diagsArea, tempFlags) == ex_expr::EXPR_OK) {
          *(int *)target = *(int *)source;
        } else {
          return ex_expr::EXPR_ERROR;
        }
      }
    } break;

    case CONV_BIN32S_BIN32U: {
      if (*(int *)source < 0) {
        if (dataConversionErrorFlag != 0)  // Capture error in variable?
        {
          *(int *)target = 0;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN;
        } else {
          ExRaiseSqlError(heap, diagsArea, EXE_UNSIGNED_OVERFLOW);
          return ex_expr::EXPR_ERROR;
        }
      } else {
        if (dataConversionErrorFlag != 0) {
          *(int *)target = *(int *)source;
        } else {
          if (checkPrecision((long)*(int *)source, sourceLen, sourceType, sourcePrecision, sourceScale, targetType,
                             targetPrecision, targetScale, heap, diagsArea, tempFlags) == ex_expr::EXPR_OK) {
            *(int *)target = *(int *)source;
          } else {
            return ex_expr::EXPR_ERROR;
          }
        }
      }
    } break;

    case CONV_BIN32S_BIN64S: {
      *(long *)target = *(int *)source;
    } break;

    case CONV_BIN32S_DECU:
    case CONV_BIN32S_DECS: {
      if (convExactToDec(target, targetLen, targetType, (long)*(int *)source, heap, diagsArea, dataConversionErrorFlag,
                         tempFlags) != ex_expr::EXPR_OK) {
        return ex_expr::EXPR_ERROR;
      }
    } break;

    case CONV_BIN32S_FLOAT32: {
      *floatTgtPtr = (float)*(int *)source;
    }; break;

    case CONV_BIN32S_FLOAT64: {
      *doubleTgtPtr = (double)*(int *)source;
    }; break;

    case CONV_BIN32S_BIGNUMU: {
      if (*(int *)source < 0) {
        ExRaiseSqlError(heap, diagsArea, EXE_UNSIGNED_OVERFLOW);
        return ex_expr::EXPR_ERROR;
      };
    };
      // no break! from here on CONV_BIN32S_BIGNUMU is identical with
      // CONV_BIN32S_BIGNUM

    case CONV_BIN32S_BIGNUM: {
      return convInt64ToBigNum(target, targetLen, (long)*(int *)source, heap, diagsArea, tempFlags);
    }; break;

    case CONV_BIN32S_ASCII: {
      if (convInt64ToAscii(target, targetLen, targetPrecision, targetScale, (long)*(int *)source, sourceScale,
                           varCharLen, varCharLenSize,
                           ' ',  // filler character
                           FALSE, leftPad, FALSE, heap, diagsArea, bTrimTailZero) != ex_expr::EXPR_OK)
        return ex_expr::EXPR_ERROR;
    }; break;

    case CONV_BIN32U_BPINTU: {
      if (*(int *)source > (int)limits[targetPrecision - 1]) {
        if (dataConversionErrorFlag != 0) {
          *(unsigned short *)target = limits[targetPrecision - 1];
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX;
        } else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                targetType, tempFlags);
          return ex_expr::EXPR_ERROR;
        }
      } else {
        *(unsigned short *)target = (unsigned short)*(int *)source;
      }
    } break;
    case CONV_BIN32U_BIN16S: {
      if (*(int *)source > SHRT_MAX) {
        if (dataConversionErrorFlag != 0)  // Capture error in variable?
        {
          *(short *)target = SHRT_MAX;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX;
        } else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                targetType, tempFlags);
          return ex_expr::EXPR_ERROR;
        }
      } else {
        if (dataConversionErrorFlag != 0) {  // Set the target value.
          *(short *)target = (short)*(int *)source;
        } else {  // Check target precision. Then set target value.
          if (checkPrecision((long)*(int *)source, sourceLen, sourceType, sourcePrecision, sourceScale, targetType,
                             targetPrecision, targetScale, heap, diagsArea, tempFlags) == ex_expr::EXPR_OK) {
            *(short *)target = (short)*(int *)source;
          } else {
            return ex_expr::EXPR_ERROR;
          }
        }
      }
    } break;

    case CONV_BIN32U_BIN16U: {
      if (*(int *)source > USHRT_MAX) {
        if (dataConversionErrorFlag != 0)  // Capture error in variable?
        {
          *(unsigned short *)target = USHRT_MAX;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX;
        } else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                targetType, tempFlags);
          return ex_expr::EXPR_ERROR;
        }
      } else {
        if (dataConversionErrorFlag != 0) {  // Set the target value.
          *(unsigned short *)target = (unsigned short)*(int *)source;
        } else {  // Check target precision. Then set target value.
          if (checkPrecision((long)*(int *)source, sourceLen, sourceType, sourcePrecision, sourceScale, targetType,
                             targetPrecision, targetScale, heap, diagsArea, tempFlags) == ex_expr::EXPR_OK) {
            *(unsigned short *)target = (unsigned short)*(int *)source;
          } else {
            return ex_expr::EXPR_ERROR;
          }
        }
      }
    } break;

    case CONV_BIN32U_BIN32S: {
      if (*(int *)source > INT_MAX) {
        if (dataConversionErrorFlag != 0)  // Capture error in variable?
        {
          *(int *)target = INT_MAX;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX;
        } else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                targetType, tempFlags);
          return ex_expr::EXPR_ERROR;
        }
      } else {
        if (dataConversionErrorFlag != 0) {  // Set the target value.
          *(int *)target = *(int *)source;
        } else {  // Check target precision. Then set target value.
          if (checkPrecision((long)*(int *)source, sourceLen, sourceType, sourcePrecision, sourceScale, targetType,
                             targetPrecision, targetScale, heap, diagsArea, tempFlags) == ex_expr::EXPR_OK) {
            *(int *)target = *(int *)source;
          } else {
            return ex_expr::EXPR_ERROR;
          }
        }
      }
    } break;

    case CONV_BIN32U_BIN32U: {
      if (dataConversionErrorFlag != 0) {
        *(int *)target = *(int *)source;
      } else {
        if (checkPrecision((long)*(int *)source, sourceLen, sourceType, sourcePrecision, sourceScale, targetType,
                           targetPrecision, targetScale, heap, diagsArea, tempFlags) == ex_expr::EXPR_OK) {
          *(int *)target = *(int *)source;
        } else {
          return ex_expr::EXPR_ERROR;
        }
      }
    } break;

    case CONV_BIN32U_BIN64S: {
      *(long *)target = (long)(*(int *)source);
    } break;

    case CONV_BIN32U_DECS:
      // covers conversion to DECS and DECU (see exp_fixup.cpp)
      {
        if (convExactToDec(target, targetLen, targetType, (long)*(int *)source, heap, diagsArea,
                           dataConversionErrorFlag, tempFlags) != ex_expr::EXPR_OK) {
          return ex_expr::EXPR_ERROR;
        }
      }
      break;

    case CONV_BIN32U_FLOAT32: {
      *floatTgtPtr = (float)*(int *)source;
    } break;

    case CONV_BIN32U_FLOAT64: {
      *doubleTgtPtr = (double)*(int *)source;
    } break;

    case CONV_BIN32U_BIGNUM: {
      return convInt64ToBigNum(target, targetLen, (long)*(int *)source, heap, diagsArea, tempFlags);
    } break;

    case CONV_BIN32U_ASCII: {
      if (convInt64ToAscii(target, targetLen, targetPrecision, targetScale, (long)*(int *)source, sourceScale,
                           varCharLen, varCharLenSize,
                           ' ',  // filler character
                           FALSE, leftPad, FALSE, heap, diagsArea, bTrimTailZero) != ex_expr::EXPR_OK)
        return ex_expr::EXPR_ERROR;
    }; break;

    case CONV_BIN64S_BPINTU: {
      if (*(long *)source < 0) {
        if (dataConversionErrorFlag != 0) {
          *(unsigned short *)target = 0;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN;
        } else {
          ExRaiseSqlError(heap, diagsArea, EXE_UNSIGNED_OVERFLOW);
          return ex_expr::EXPR_ERROR;
        }
      } else if (*(long *)source > (long)limits[targetPrecision - 1]) {
        if (dataConversionErrorFlag != 0) {
          *(unsigned short *)target = limits[targetPrecision - 1];
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX;
        } else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                targetType, tempFlags);
          return ex_expr::EXPR_ERROR;
        }
      } else {
        *(unsigned short *)target = (unsigned short)*(long *)source;
      }
    } break;

    case CONV_BIN64S_BIN16S: {
      if (*(long *)source < SHRT_MIN) {
        if (dataConversionErrorFlag != 0)  // Capture error in variable?
        {
          *(short *)target = SHRT_MIN;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN;
        } else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                targetType, tempFlags);
          return ex_expr::EXPR_ERROR;
        }
      } else if (*(long *)source > SHRT_MAX) {
        if (dataConversionErrorFlag != 0)  // Capture error in variable?
        {
          *(short *)target = SHRT_MAX;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX;
        } else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                targetType, tempFlags, targetLen, targetScale, targetPrecision, sourcePrecision);
          return ex_expr::EXPR_ERROR;
        }
      } else {
        if (dataConversionErrorFlag != 0) {  // Set the target value.
          *(short *)target = (short)*(long *)source;
        } else {  // Check target precision. Then set target value.
          if (checkPrecision(*(long *)source, sourceLen, sourceType, sourcePrecision, sourceScale, targetType,
                             targetPrecision, targetScale, heap, diagsArea, tempFlags) == ex_expr::EXPR_OK) {
            *(short *)target = (short)*(long *)source;
          } else {
            return ex_expr::EXPR_ERROR;
          }
        }
      }
    } break;

    case CONV_BIN64S_BIN16U: {
      if (*(long *)source < 0) {
        if (dataConversionErrorFlag != 0)  // Capture error in variable?
        {
          *(unsigned short *)target = 0;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN;
        } else {
          ExRaiseSqlError(heap, diagsArea, EXE_UNSIGNED_OVERFLOW);
          return ex_expr::EXPR_ERROR;
        }
      } else if (*(long *)source > USHRT_MAX) {
        if (dataConversionErrorFlag != 0)  // Capture error in variable?
        {
          *(unsigned short *)target = USHRT_MAX;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX;
        } else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                targetType, tempFlags);
          return ex_expr::EXPR_ERROR;
        }
      } else {
        if (dataConversionErrorFlag != 0) {  // Set the target value.
          *(unsigned short *)target = (unsigned short)*(long *)source;
        } else {  // Check target precision. Then set target value.
          if (checkPrecision(*(long *)source, sourceLen, sourceType, sourcePrecision, sourceScale, targetType,
                             targetPrecision, targetScale, heap, diagsArea, tempFlags) == ex_expr::EXPR_OK) {
            *(unsigned short *)target = (unsigned short)*(long *)source;
          } else {
            return ex_expr::EXPR_ERROR;
          }
        }
      }
    } break;

    case CONV_BIN64S_BIN32S: {
      if (*(long *)source < INT_MIN) {
        if (dataConversionErrorFlag != 0)  // Capture error in variable?
        {
          *(int *)target = INT_MIN;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN;
        } else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                targetType, tempFlags);
          return ex_expr::EXPR_ERROR;
        }
      } else if (*(long *)source > INT_MAX) {
        if (dataConversionErrorFlag != 0)  // Capture error in variable?
        {
          *(int *)target = INT_MAX;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX;
        } else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                targetType, tempFlags);
          return ex_expr::EXPR_ERROR;
        }
      } else {
        if (dataConversionErrorFlag != 0) {  // Set the target value.
          *(int *)target = (int)*(long *)source;
        } else {  // Check target precision. Then set target value.
          if (checkPrecision(*(long *)source, sourceLen, sourceType, sourcePrecision, sourceScale, targetType,
                             targetPrecision, targetScale, heap, diagsArea, tempFlags) == ex_expr::EXPR_OK) {
            *(int *)target = (int)*(long *)source;
          } else {
            return ex_expr::EXPR_ERROR;
          }
        }
      }
    } break;

    case CONV_BIN64S_BIN32U: {
      if (*(long *)source < 0) {
        if (dataConversionErrorFlag != 0)  // Capture error in variable?
        {
          *(int *)target = 0;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN;
        } else {
          ExRaiseSqlError(heap, diagsArea, EXE_UNSIGNED_OVERFLOW);
          return ex_expr::EXPR_ERROR;
        }
      } else if (*(long *)source > UINT_MAX) {
        if (dataConversionErrorFlag != 0)  // Capture error in variable?
        {
          *(int *)target = UINT_MAX;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX;
        } else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                targetType, tempFlags);
          return ex_expr::EXPR_ERROR;
        }
      } else {
        if (dataConversionErrorFlag != 0) {  // Set the target value.
          *(int *)target = (int)*(long *)source;
        } else {  // Check target precision. Then set target value.
          if (checkPrecision(*(long *)source, sourceLen, sourceType, sourcePrecision, sourceScale, targetType,
                             targetPrecision, targetScale, heap, diagsArea, tempFlags) == ex_expr::EXPR_OK) {
            *(int *)target = (int)*(long *)source;
          } else {
            return ex_expr::EXPR_ERROR;
          }
        }
      }
    } break;

    case CONV_BIN64S_BIN64S: {
      if ((dataConversionErrorFlag != 0) ||
          checkPrecision(*(long *)source, sourceLen, sourceType, sourcePrecision, sourceScale, targetType,
                         targetPrecision, targetScale, heap, diagsArea, tempFlags) == ex_expr::EXPR_OK) {
        *(long *)target = *(long *)source;
      } else {
        return ex_expr::EXPR_ERROR;
      }
    } break;

    case CONV_BIN64S_BIN64U: {
      if (*(long *)source < 0) {
        if (dataConversionErrorFlag != 0)  // Capture error in variable?
        {
          *(UInt64 *)target = 0;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN;
        } else {
          ExRaiseSqlError(heap, diagsArea, EXE_UNSIGNED_OVERFLOW);
          return ex_expr::EXPR_ERROR;
        }
      } else {
        if (dataConversionErrorFlag != 0) {
          *(UInt64 *)target = *(long *)source;
        } else {
          if (checkPrecision(*(long *)source, sourceLen, sourceType, sourcePrecision, sourceScale, targetType,
                             targetPrecision, targetScale, heap, diagsArea, tempFlags) == ex_expr::EXPR_OK) {
            *(UInt64 *)target = *(long *)source;
          } else {
            return ex_expr::EXPR_ERROR;
          }
        }
      }
    } break;

    case CONV_BIN64U_BIN64U: {
      if (dataConversionErrorFlag != 0) {
        *(UInt64 *)target = *(UInt64 *)source;
      } else {
        if (checkPrecision((long)*(UInt64 *)source, sourceLen, sourceType, sourcePrecision, sourceScale, targetType,
                           targetPrecision, targetScale, heap, diagsArea, tempFlags) == ex_expr::EXPR_OK) {
          *(UInt64 *)target = *(UInt64 *)source;
        } else {
          return ex_expr::EXPR_ERROR;
        }
      }
    } break;

    case CONV_BIN64U_BIN64S: {
      if (*(UInt64 *)source > LLONG_MAX) {
        if (dataConversionErrorFlag != 0)  // Capture error in variable?
        {
          *(long *)target = LLONG_MAX;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX;
        } else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                targetType, tempFlags);
          return ex_expr::EXPR_ERROR;
        }
      } else {
        if (dataConversionErrorFlag != 0) {  // Set the target value.
          *(long *)target = *(UInt64 *)source;
        } else {  // Check target precision. Then set target value.
          if (checkPrecision((long)*(UInt64 *)source, sourceLen, sourceType, sourcePrecision, sourceScale, targetType,
                             targetPrecision, targetScale, heap, diagsArea, tempFlags) == ex_expr::EXPR_OK) {
            *(long *)target = *(UInt64 *)source;
          } else {
            return ex_expr::EXPR_ERROR;
          }
        }
      }
    } break;

    case CONV_BIN64S_DECU:
    case CONV_BIN64S_DECS: {
      if (convExactToDec(target, targetLen, targetType, *(long *)source, heap, diagsArea, dataConversionErrorFlag,
                         tempFlags) != ex_expr::EXPR_OK) {
        return ex_expr::EXPR_ERROR;
      }
    } break;

    case CONV_BIN64S_FLOAT32: {
      *floatTgtPtr = (float)*(long *)source;
    }; break;

    case CONV_BIN64S_FLOAT64: {
      *doubleTgtPtr = (double)*(long *)source;
    }; break;

    case CONV_BIN64U_FLOAT32: {
      *floatTgtPtr = (float)*(UInt64 *)source;
    }; break;

    case CONV_BIN64U_FLOAT64: {
      *doubleTgtPtr = (double)*(UInt64 *)source;
    }; break;

    case CONV_BIN64U_BIGNUM: {
      return convUInt64ToBigNum(target, targetLen, *(UInt64 *)source, heap, diagsArea, tempFlags);
    }; break;

    case CONV_BIN64S_BIGNUMU: {
      if (*(long *)source < 0) {
        ExRaiseSqlError(heap, diagsArea, EXE_UNSIGNED_OVERFLOW);
        return ex_expr::EXPR_ERROR;
      };
    };
      // no break! from here on CONV_BIN64S_BIGNUMU is identical with
      // CONV_BIN64S_BIGNUM

    case CONV_BIN64S_BIGNUM: {
      return convInt64ToBigNum(target, targetLen, *(long *)source, heap, diagsArea, tempFlags);
    }; break;

    case CONV_BIN64S_ASCII: {
      if (convInt64ToAscii(target, targetLen, targetPrecision, targetScale, *(long *)source, sourceScale, varCharLen,
                           varCharLenSize,
                           ' ',  // filler character
                           FALSE, leftPad, FALSE, heap, diagsArea, bTrimTailZero) != ex_expr::EXPR_OK)
        return ex_expr::EXPR_ERROR;
    }; break;

    case CONV_BIN64U_ASCII: {
      if (convInt64ToAscii(target, targetLen, targetPrecision, targetScale, *(long *)source, sourceScale, varCharLen,
                           varCharLenSize,
                           ' ',  // filler character
                           FALSE, leftPad, TRUE, heap, diagsArea) != ex_expr::EXPR_OK)
        return ex_expr::EXPR_ERROR;
    }; break;

    case CONV_DECS_BIN32U:
      // This case covers conversions
      //   from {decu, decs}
      //   to {bpintu, bin16u, bin32u, bignum}
      {
        long int64source;
        if (convDecToInt64(int64source, source, sourceLen, heap, diagsArea, tempFlags | CONV_INTERMEDIATE_CONVERSION) !=
            ex_expr::EXPR_OK) {
          return ex_expr::EXPR_ERROR;
        }
        return convDoIt((char *)&int64source, (int)sizeof(long), (short)REC_BIN64_SIGNED,
                        0,  // sourcePrecision
                        0,  // sourceScale
                        target, targetLen, targetType, targetPrecision, targetScale,
                        NULL,  // varCharLen
                        0,     // varCharLenSize
                        heap, diagsArea, CONV_UNKNOWN, dataConversionErrorFlag,
                        tempFlags | CONV_INTERMEDIATE_CONVERSION);
      }
      break;

    case CONV_DECS_BIN32S:
      // This case covers conversions
      //   from {decu, decs}
      //   to {bin16s, bin32s}
      {
        long int64source;
        if (convDecToInt64(int64source, source, sourceLen, heap, diagsArea, tempFlags | CONV_INTERMEDIATE_CONVERSION) !=
            ex_expr::EXPR_OK) {
          return ex_expr::EXPR_ERROR;
        }
        return convDoIt((char *)&int64source, (int)sizeof(long), (short)REC_BIN64_SIGNED,
                        0,  // sourcePrecision
                        0,  // sourceScale
                        target, targetLen, targetType, targetPrecision, targetScale,
                        NULL,  // varCharLen
                        0,     // varCharLenSize
                        heap, diagsArea, CONV_UNKNOWN, dataConversionErrorFlag,
                        tempFlags | CONV_INTERMEDIATE_CONVERSION);
      }
      break;

    case CONV_DECS_BIN64S: {
      long int64source;
      if (convDecToInt64(int64source, source, sourceLen, heap, diagsArea, tempFlags | CONV_INTERMEDIATE_CONVERSION) !=
          ex_expr::EXPR_OK) {
        return ex_expr::EXPR_ERROR;
      }
      return convDoIt((char *)&int64source, (int)sizeof(long), (short)REC_BIN64_SIGNED,
                      0,  // sourcePrecision
                      0,  // sourceScale
                      target, targetLen, targetType, targetPrecision, targetScale,
                      NULL,  // varCharLen
                      0,     // varCharLenSize
                      heap, diagsArea, CONV_BIN64S_BIN64S, dataConversionErrorFlag,
                      tempFlags | CONV_INTERMEDIATE_CONVERSION);
    } break;

    case CONV_DECS_DECU:
    case CONV_DECS_DECS: {
      long intermediate = 0;
      if (convDecToInt64(intermediate, source, sourceLen, heap, diagsArea, tempFlags | CONV_INTERMEDIATE_CONVERSION) !=
          ex_expr::EXPR_OK) {
        return ex_expr::EXPR_ERROR;
      }
      if (convExactToDec(target, targetLen, targetType, intermediate, heap, diagsArea, dataConversionErrorFlag,
                         tempFlags | CONV_INTERMEDIATE_CONVERSION) != ex_expr::EXPR_OK) {
        return ex_expr::EXPR_ERROR;
      }
    } break;

    case CONV_DECS_FLOAT32: {
      long intermediate;
      if (convDecToInt64(intermediate, source, sourceLen, heap, diagsArea, tempFlags | CONV_INTERMEDIATE_CONVERSION) !=
          ex_expr::EXPR_OK)
        return ex_expr::EXPR_ERROR;

      *floatTgtPtr = (float)intermediate;
    }; break;

    case CONV_DECS_FLOAT64: {
      long intermediate;
      if (convDecToInt64(intermediate, source, sourceLen, heap, diagsArea, tempFlags | CONV_INTERMEDIATE_CONVERSION) !=
          ex_expr::EXPR_OK)
        return ex_expr::EXPR_ERROR;

      *doubleTgtPtr = (double)intermediate;
    }; break;

    case CONV_DECS_ASCII: {
      long intermediate;
      if (convDecToInt64(intermediate, source, sourceLen, heap, diagsArea, tempFlags | CONV_INTERMEDIATE_CONVERSION) !=
          ex_expr::EXPR_OK)
        return ex_expr::EXPR_ERROR;

      if (convInt64ToAscii(target, targetLen, targetPrecision, targetScale, intermediate,
                           ((targetType == REC_DECIMAL_LS) ? 0 : sourceScale), varCharLen, varCharLenSize,
                           ((targetType == REC_DECIMAL_LS) ? '0' : ' '), (targetType == REC_DECIMAL_LS),
                           ((targetType == REC_DECIMAL_LS) ? TRUE : leftPad), FALSE, heap, diagsArea,
                           bTrimTailZero) != ex_expr::EXPR_OK)
        return ex_expr::EXPR_ERROR;
    }; break;

    case CONV_FLOAT32_BPINTU: {
      float floatsource = *floatSrcPtr;
      if (floatsource < 0) {
        if (dataConversionErrorFlag != 0) {
          *(unsigned short *)target = 0;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN;
        } else {
          ExRaiseSqlError(heap, diagsArea, EXE_UNSIGNED_OVERFLOW);
          return ex_expr::EXPR_ERROR;
        }
      } else if (floatsource > limits[targetPrecision - 1]) {
        if (dataConversionErrorFlag != 0) {
          *(unsigned short *)target = limits[targetPrecision - 1];
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX;
        } else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                targetType, tempFlags);
          return ex_expr::EXPR_ERROR;
        }
      } else {
        unsigned short unsignedShortSource = (unsigned short)floatsource;
        if (dataConversionErrorFlag != 0) {
          // Convert back and check for a value change.
          float floatsource2 = unsignedShortSource;
          if (floatsource2 > floatsource) {
            *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP;
          } else {
            if (floatsource2 < floatsource) {
              *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN;
            }
          }
        }
        *(unsigned short *)target = unsignedShortSource;
      }
    } break;
    case CONV_FLOAT32_BIN16S: {
      float floatsource = *floatSrcPtr;
      if (floatsource < SHRT_MIN) {
        if (dataConversionErrorFlag != 0)  // Capture error in variable?
        {
          *(short *)target = SHRT_MIN;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN;
        } else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                targetType, tempFlags);
          return ex_expr::EXPR_ERROR;
        }
      } else if (floatsource > SHRT_MAX) {
        if (dataConversionErrorFlag != 0)  // Capture error in variable?
        {
          *(short *)target = SHRT_MAX;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX;
        } else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                targetType, tempFlags);
          return ex_expr::EXPR_ERROR;
        }
      } else {
        long int64source = (long)floatsource;
        if (dataConversionErrorFlag != 0) {
          // Convert back and check for a value change.
          float floatsource2 = int64source;
          if (floatsource2 > floatsource) {
            *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP;
          } else {
            if (floatsource2 < floatsource) {
              *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN;
            }
          }
        }
        if ((dataConversionErrorFlag != 0) ||
            checkPrecision(int64source, sourceLen, sourceType, sourcePrecision, sourceScale, targetType,
                           targetPrecision, targetScale, heap, diagsArea,
                           tempFlags) == ex_expr::EXPR_OK) {  // Set the target value.
          *(short *)target = (short)int64source;
        } else {
          return ex_expr::EXPR_ERROR;
        }
      }
    } break;

    case CONV_FLOAT32_BIN16U: {
      float floatsource = *floatSrcPtr;
      if (floatsource < 0) {
        if (dataConversionErrorFlag != 0)  // Capture error in variable?
        {
          *(unsigned short *)target = 0;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN;
        } else {
          ExRaiseSqlError(heap, diagsArea, EXE_UNSIGNED_OVERFLOW);
          return ex_expr::EXPR_ERROR;
        }
      } else if (floatsource > USHRT_MAX) {
        if (dataConversionErrorFlag != 0)  // Capture error in variable?
        {
          *(unsigned short *)target = USHRT_MAX;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX;
        } else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                targetType, tempFlags);
          return ex_expr::EXPR_ERROR;
        }
      } else {
        long int64source = (long)floatsource;
        if (dataConversionErrorFlag != 0) {
          // Convert back and check for a value change.
          float floatsource2 = int64source;
          if (floatsource2 > floatsource) {
            *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP;
          } else {
            if (floatsource2 < floatsource) {
              *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN;
            }
          }
        }
        if ((dataConversionErrorFlag != 0) ||
            checkPrecision(int64source, sourceLen, sourceType, sourcePrecision, sourceScale, targetType,
                           targetPrecision, targetScale, heap, diagsArea,
                           tempFlags) == ex_expr::EXPR_OK) {  // Set the target value.
          *(unsigned short *)target = (unsigned short)int64source;
        } else {
          return ex_expr::EXPR_ERROR;
        }
      }
    } break;

    case CONV_FLOAT32_BIN32S: {
      float floatsource = *floatSrcPtr;
      if (floatsource < INT_MIN) {
        if (dataConversionErrorFlag != 0)  // Capture error in variable?
        {
          *(int *)target = INT_MIN;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN;
        } else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                targetType, tempFlags);
          return ex_expr::EXPR_ERROR;
        }
      } else if (floatsource > INT_MAX) {
        if (dataConversionErrorFlag != 0)  // Capture error in variable?
        {
          *(int *)target = INT_MAX;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX;
        } else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                targetType, tempFlags);
          return ex_expr::EXPR_ERROR;
        }
      } else {
        long int64source = (long)floatsource;
        if (dataConversionErrorFlag != 0) {
          // Convert back and check for a value change.
          float floatsource2 = int64source;
          if (floatsource2 > floatsource) {
            *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP;
          } else {
            if (floatsource2 < floatsource) {
              *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN;
            }
          }
        }
        if ((dataConversionErrorFlag != 0) ||
            checkPrecision(int64source, sourceLen, sourceType, sourcePrecision, sourceScale, targetType,
                           targetPrecision, targetScale, heap, diagsArea,
                           tempFlags) == ex_expr::EXPR_OK) {  // Set the target value.
          *(int *)target = (int)int64source;
        } else {
          return ex_expr::EXPR_ERROR;
        }
      }
    } break;

    case CONV_FLOAT32_BIN32U: {
      float floatsource = *floatSrcPtr;
      if (floatsource < 0) {
        if (dataConversionErrorFlag != 0)  // Capture error in variable?
        {
          *(int *)target = 0;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN;
        } else {
          ExRaiseSqlError(heap, diagsArea, EXE_UNSIGNED_OVERFLOW);
          return ex_expr::EXPR_ERROR;
        }
      } else if (floatsource > UINT_MAX) {
        if (dataConversionErrorFlag != 0)  // Capture error in variable?
        {
          *(int *)target = UINT_MAX;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX;
        } else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                targetType, tempFlags);
          return ex_expr::EXPR_ERROR;
        }
      } else {
        long int64source = (long)floatsource;
        if (dataConversionErrorFlag != 0) {
          // Convert back and check for a value change.
          float floatsource2 = int64source;
          if (floatsource2 > floatsource) {
            *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP;
          } else {
            if (floatsource2 < floatsource) {
              *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN;
            }
          }
        }
        if ((dataConversionErrorFlag != 0) ||
            checkPrecision(int64source, sourceLen, sourceType, sourcePrecision, sourceScale, targetType,
                           targetPrecision, targetScale, heap, diagsArea,
                           tempFlags) == ex_expr::EXPR_OK) {  // Set the target value.
          *(int *)target = (int)int64source;
        } else {
          return ex_expr::EXPR_ERROR;
        }
      }
    } break;

    case CONV_FLOAT32_BIN64S: {
      float floatsource = *floatSrcPtr;
      if (floatsource < LLONG_MIN) {
        if (dataConversionErrorFlag != 0)  // Capture error in variable?
        {
          *(long *)target = LLONG_MIN;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN;
        } else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                targetType, tempFlags);
          return ex_expr::EXPR_ERROR;
        }
      } else if (floatsource > LLONG_MAX) {
        if (dataConversionErrorFlag != 0)  // Capture error in variable?
        {
          *(long *)target = LLONG_MAX;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX;
        } else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                targetType, tempFlags);
          return ex_expr::EXPR_ERROR;
        }
      } else {
        long int64source = (long)floatsource;
        if (dataConversionErrorFlag != 0) {
          // Convert back and check for a value change.
          float floatsource2 = int64source;
          if (floatsource2 > floatsource) {
            *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP;
          } else {
            if (floatsource2 < floatsource) {
              *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN;
            }
          }
        }
        if ((dataConversionErrorFlag != 0) ||
            checkPrecision(int64source, sourceLen, sourceType, sourcePrecision, sourceScale, targetType,
                           targetPrecision, targetScale, heap, diagsArea,
                           tempFlags) == ex_expr::EXPR_OK) {  // Set the target value.
          *(long *)target = int64source;
        } else {
          return ex_expr::EXPR_ERROR;
        }
      }
    } break;

    case CONV_FLOAT32_BIN64U: {
      float floatsource = *floatSrcPtr;
      if (floatsource < 0) {
        if (dataConversionErrorFlag != 0)  // Capture error in variable?
        {
          *(UInt64 *)target = 0;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN;
        } else {
          ExRaiseSqlError(heap, diagsArea, EXE_UNSIGNED_OVERFLOW);
          return ex_expr::EXPR_ERROR;
        }
      } else if (floatsource > ULLONG_MAX) {
        if (dataConversionErrorFlag != 0)  // Capture error in variable?
        {
          *(UInt64 *)target = ULLONG_MAX;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX;
        } else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                targetType, tempFlags);
          return ex_expr::EXPR_ERROR;
        }
      } else {
        UInt64 int64source = (UInt64)floatsource;
        if (dataConversionErrorFlag != 0) {
          // Convert back and check for a value change.
          float floatsource2 = int64source;
          if (floatsource2 > floatsource) {
            *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP;
          } else {
            if (floatsource2 < floatsource) {
              *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN;
            }
          }
        }
        if ((dataConversionErrorFlag != 0) ||
            checkPrecision(int64source, sourceLen, sourceType, sourcePrecision, sourceScale, targetType,
                           targetPrecision, targetScale, heap, diagsArea,
                           tempFlags) == ex_expr::EXPR_OK) {  // Set the target value.
          *(UInt64 *)target = (UInt64)int64source;
        } else {
          return ex_expr::EXPR_ERROR;
        }
      }
    } break;

    case CONV_FLOAT32_DECU:
    case CONV_FLOAT32_DECS: {
      float floatsource = *floatSrcPtr;
      if (floatsource < getMinDecValue(targetLen, targetType)) {
        if (dataConversionErrorFlag != 0) {
          setMinDecValue(target, targetLen, targetType);
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN;
        } else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                targetType, tempFlags);
          return ex_expr::EXPR_ERROR;
        }
      } else if (floatsource > getMaxDecValue(targetLen)) {
        if (dataConversionErrorFlag != 0) {
          setMaxDecValue(target, targetLen);
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX;
        } else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                targetType, tempFlags);
          return ex_expr::EXPR_ERROR;
        }
      } else {
        long int64source = (long)floatsource;
        if (dataConversionErrorFlag != 0) {
          // Convert back and check for a value change.
          float floatsource2 = int64source;
          if (floatsource2 > floatsource) {
            *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP;
          } else {
            if (floatsource2 < floatsource) {
              *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN;
            }
          }
        }
        if (convInt64ToDec(target, targetLen, int64source, heap, diagsArea) != ex_expr::EXPR_OK) {
          return ex_expr::EXPR_ERROR;
        }
      }
    } break;
    case CONV_FLOAT32_FLOAT32: {
      *floatTgtPtr = *floatSrcPtr;
    }; break;

    case CONV_FLOAT32_FLOAT64: {
      *doubleTgtPtr = (double)*floatSrcPtr;
    }; break;

    case CONV_FLOAT32_ASCII: {
      if (targetLen < SQL_REAL_MIN_DISPLAY_SIZE) {
        ExRaiseDetailSqlError(heap, diagsArea, EXE_STRING_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                              targetType, tempFlags);
        return ex_expr::EXPR_ERROR;
      };

      //    double intermediate = (double) *(float *) source;
      double intermediate = (double)*floatSrcPtr;

      if (convFloat64ToAscii(target, targetLen, targetPrecision, targetScale, intermediate, SQL_REAL_FRAG_DIGITS,
                             varCharLen, varCharLenSize, leftPad) != 0) {
        ExRaiseDetailSqlError(heap, diagsArea, EXE_STRING_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                              targetType, tempFlags);
        return ex_expr::EXPR_ERROR;
      }
    }; break;

    case CONV_FLOAT64_BPINTU: {
      double doublesource = *doubleSrcPtr;
      if (doublesource < 0) {
        if (dataConversionErrorFlag != 0) {
          *(unsigned short *)target = 0;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN;
        } else {
          ExRaiseSqlError(heap, diagsArea, EXE_UNSIGNED_OVERFLOW);
          return ex_expr::EXPR_ERROR;
        }
      } else if (doublesource > limits[targetPrecision - 1]) {
        if (dataConversionErrorFlag != 0) {
          *(unsigned short *)target = limits[targetPrecision - 1];
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX;
        } else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                targetType, tempFlags);
          return ex_expr::EXPR_ERROR;
        }
      } else {
        unsigned short unsignedShortSource = (unsigned short)doublesource;
        if (dataConversionErrorFlag != 0) {
          // Convert back and check for a value change.
          double doublesource2 = unsignedShortSource;
          if (doublesource2 > doublesource) {
            *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP;
          } else {
            if (doublesource2 < doublesource) {
              *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN;
            }
          }
        }
        *(unsigned short *)target = unsignedShortSource;
      }
    } break;

    case CONV_FLOAT64_BIN16S: {
      double doublesource = *doubleSrcPtr;
      if (doublesource < SHRT_MIN) {
        if (dataConversionErrorFlag != 0)  // Capture error in variable?
        {
          *(short *)target = SHRT_MIN;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN;
        } else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                targetType, tempFlags);
          return ex_expr::EXPR_ERROR;
        }
      } else if (doublesource > SHRT_MAX) {
        if (dataConversionErrorFlag != 0)  // Capture error in variable?
        {
          *(short *)target = SHRT_MAX;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX;
        } else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                targetType, tempFlags);
          return ex_expr::EXPR_ERROR;
        }
      } else {
        long int64source = (long)doublesource;
        if (dataConversionErrorFlag != 0) {
          // Convert back and check for a value change.
          double doublesource2 = int64source;
          if (doublesource2 > doublesource) {
            *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP;
          } else {
            if (doublesource2 < doublesource) {
              *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN;
            }
          }
        }
        if ((dataConversionErrorFlag != 0) ||
            checkPrecision(int64source, sourceLen, sourceType, sourcePrecision, sourceScale, targetType,
                           targetPrecision, targetScale, heap, diagsArea,
                           tempFlags) == ex_expr::EXPR_OK) {  // Set the target value.
          *(short *)target = (short)int64source;
        } else {
          return ex_expr::EXPR_ERROR;
        }
      }
    } break;

    case CONV_FLOAT64_BIN16U: {
      double doublesource = *doubleSrcPtr;
      if (doublesource < 0) {
        if (dataConversionErrorFlag != 0)  // Capture error in variable?
        {
          *(unsigned short *)target = 0;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN;
        } else {
          ExRaiseSqlError(heap, diagsArea, EXE_UNSIGNED_OVERFLOW);
          return ex_expr::EXPR_ERROR;
        }
      } else if (doublesource > USHRT_MAX) {
        if (dataConversionErrorFlag != 0)  // Capture error in variable?
        {
          *(unsigned short *)target = USHRT_MAX;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX;
        } else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                targetType, tempFlags);
          return ex_expr::EXPR_ERROR;
        }
      } else {
        long int64source = (long)doublesource;
        if (dataConversionErrorFlag != 0) {
          // Convert back and check for a value change.
          double doublesource2 = int64source;
          if (doublesource2 > doublesource) {
            *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP;
          } else {
            if (doublesource2 < doublesource) {
              *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN;
            }
          }
        }
        if ((dataConversionErrorFlag != 0) ||
            checkPrecision(int64source, sourceLen, sourceType, sourcePrecision, sourceScale, targetType,
                           targetPrecision, targetScale, heap, diagsArea,
                           tempFlags) == ex_expr::EXPR_OK) {  // Set the target value.
          *(unsigned short *)target = (unsigned short)int64source;
        } else {
          return ex_expr::EXPR_ERROR;
        }
      }
    } break;

    case CONV_FLOAT64_BIN32S: {
      double doublesource = *doubleSrcPtr;
      if (doublesource < INT_MIN) {
        if (dataConversionErrorFlag != 0)  // Capture error in variable?
        {
          *(int *)target = INT_MIN;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN;
        } else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                targetType, tempFlags);
          return ex_expr::EXPR_ERROR;
        }
      } else if (doublesource > INT_MAX) {
        if (dataConversionErrorFlag != 0)  // Capture error in variable?
        {
          *(int *)target = INT_MAX;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX;
        } else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                targetType, tempFlags);
          return ex_expr::EXPR_ERROR;
        }
      } else {
        long int64source = (long)doublesource;
        if (dataConversionErrorFlag != 0) {
          // Convert back and check for a value change.
          double doublesource2 = int64source;
          if (doublesource2 > doublesource) {
            *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP;
          } else {
            if (doublesource2 < doublesource) {
              *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN;
            }
          }
        }
        if ((dataConversionErrorFlag != 0) ||
            checkPrecision(int64source, sourceLen, sourceType, sourcePrecision, sourceScale, targetType,
                           targetPrecision, targetScale, heap, diagsArea,
                           tempFlags) == ex_expr::EXPR_OK) {  // Set the target value.
          *(int *)target = (int)int64source;
        } else {
          return ex_expr::EXPR_ERROR;
        }
      }
    } break;

    case CONV_FLOAT64_BIN32U: {
      double doublesource = *doubleSrcPtr;
      if (doublesource < 0) {
        if (dataConversionErrorFlag != 0)  // Capture error in variable?
        {
          *(int *)target = 0;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN;
        } else {
          ExRaiseSqlError(heap, diagsArea, EXE_UNSIGNED_OVERFLOW);
          return ex_expr::EXPR_ERROR;
        }
      } else if (doublesource > UINT_MAX) {
        if (dataConversionErrorFlag != 0)  // Capture error in variable?
        {
          *(int *)target = UINT_MAX;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX;
        } else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                targetType, tempFlags);
          return ex_expr::EXPR_ERROR;
        }
      } else {
        long int64source = (long)doublesource;
        if (dataConversionErrorFlag != 0) {
          // Convert back and check for a value change.
          double doublesource2 = int64source;
          if (doublesource2 > doublesource) {
            *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP;
          } else {
            if (doublesource2 < doublesource) {
              *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN;
            }
          }
        }
        if ((dataConversionErrorFlag != 0) ||
            checkPrecision(int64source, sourceLen, sourceType, sourcePrecision, sourceScale, targetType,
                           targetPrecision, targetScale, heap, diagsArea,
                           tempFlags) == ex_expr::EXPR_OK) {  // Set the target value.
          *(int *)target = (int)int64source;
        } else {
          return ex_expr::EXPR_ERROR;
        }
      }
    } break;

    case CONV_FLOAT64_BIN64S: {
      double doublesource = *doubleSrcPtr;
      if (doublesource < LLONG_MIN) {
        if (dataConversionErrorFlag != 0)  // Capture error in variable?
        {
          *(long *)target = LLONG_MIN;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN;
        } else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                targetType, tempFlags);
          return ex_expr::EXPR_ERROR;
        }
      } else if (doublesource > LLONG_MAX) {
        if (dataConversionErrorFlag != 0)  // Capture error in variable?
        {
          *(long *)target = LLONG_MAX;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX;
        } else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                targetType, tempFlags);
          return ex_expr::EXPR_ERROR;
        }
      } else {
        long int64source = (long)doublesource;
        if (gEnableDoubleRoundFifteen) {
          int64source = (long)doubleRoundFifteen(doublesource);
        }
        if (dataConversionErrorFlag != 0) {
          // Convert back and check for a value change.
          double doublesource2 = int64source;
          if (doublesource2 > doublesource) {
            *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP;
          } else {
            if (doublesource2 < doublesource) {
              *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN;
            }
          }
        }
        if ((dataConversionErrorFlag != 0) ||
            checkPrecision(int64source, sourceLen, sourceType, sourcePrecision, sourceScale, targetType,
                           targetPrecision, targetScale, heap, diagsArea,
                           tempFlags) == ex_expr::EXPR_OK) {  // Set the target value.
          *(long *)target = int64source;
        } else {
          return ex_expr::EXPR_ERROR;
        }
      }
    } break;

    case CONV_FLOAT64_BIN64U: {
      double doublesource = *doubleSrcPtr;
      if (doublesource < 0) {
        if (dataConversionErrorFlag != 0)  // Capture error in variable?
        {
          *(UInt64 *)target = 0;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN;
        } else {
          ExRaiseSqlError(heap, diagsArea, EXE_UNSIGNED_OVERFLOW);
          return ex_expr::EXPR_ERROR;
        }
      } else if (doublesource > ULLONG_MAX) {
        if (dataConversionErrorFlag != 0)  // Capture error in variable?
        {
          *(UInt64 *)target = ULLONG_MAX;
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX;
        } else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                targetType, tempFlags);
          return ex_expr::EXPR_ERROR;
        }
      } else {
        UInt64 int64source = (UInt64)doublesource;
        if (dataConversionErrorFlag != 0) {
          // Convert back and check for a value change.
          double doublesource2 = int64source;
          if (doublesource2 > doublesource) {
            *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP;
          } else {
            if (doublesource2 < doublesource) {
              *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN;
            }
          }
        }
        if ((dataConversionErrorFlag != 0) ||
            checkPrecision(int64source, sourceLen, sourceType, sourcePrecision, sourceScale, targetType,
                           targetPrecision, targetScale, heap, diagsArea,
                           tempFlags) == ex_expr::EXPR_OK) {  // Set the target value.
          *(UInt64 *)target = (UInt64)int64source;
        } else {
          return ex_expr::EXPR_ERROR;
        }
      }
    } break;

    case CONV_FLOAT64_DECS:
    case CONV_FLOAT64_DECU: {
      double doublesource = *doubleSrcPtr;
      if (doublesource < getMinDecValue(targetLen, targetType)) {
        if (dataConversionErrorFlag != 0) {
          setMinDecValue(target, targetLen, targetType);
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN;
        } else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                targetType, tempFlags);
          return ex_expr::EXPR_ERROR;
        }
      } else if (doublesource > getMaxDecValue(targetLen)) {
        if (dataConversionErrorFlag != 0) {
          setMaxDecValue(target, targetLen);
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX;
        } else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                targetType, tempFlags);
          return ex_expr::EXPR_ERROR;
        }
      } else {
        long int64source = (long)doublesource;
        if (dataConversionErrorFlag != 0) {
          // Convert back and check for a value change.
          double doublesource2 = int64source;
          if (doublesource2 > doublesource) {
            *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP;
          } else {
            if (doublesource2 < doublesource) {
              *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN;
            }
          }
        }
        if (convInt64ToDec(target, targetLen, int64source, heap, diagsArea) != ex_expr::EXPR_OK) {
          return ex_expr::EXPR_ERROR;
        }
      }
    } break;

    case CONV_FLOAT64_FLOAT32: {
      short ov = 0;
      double doubleSource = *doubleSrcPtr;
      *floatTgtPtr = MathConvReal64ToReal32(doubleSource, &ov);

      if (dataConversionErrorFlag == 0) {
        if (ov) {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                targetType, tempFlags);
          return ex_expr::EXPR_ERROR;
        }
      } else {
        if (ov == 1)  // overflow
        {
          if (doubleSource < 0)  // < -FLT_MAX
          {
            *floatTgtPtr = -FLT_MAX;
            *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN;
          } else  // > +FLT_MAX
          {
            *floatTgtPtr = +FLT_MAX;
            *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX;
          }
        }                   // overflow
        else if (ov == -1)  // underflow
        {
          if (doubleSource < 0)  // > -FLT_MIN
          {
            *floatTgtPtr = 0;
            *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP;
          } else  // < +FLT_MIN
          {
            *floatTgtPtr = 0;
            *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN;
          }
        }  // underflow
      }    // data conversion flag

    } break;

    case CONV_FLOAT64_FLOAT64: {
      short ov = 0;
      //      double doubleSource = *doubleSrcPtr;
      *doubleTgtPtr = MathConvReal64ToReal64(*doubleSrcPtr, &ov);
      if (dataConversionErrorFlag == 0) {
        if (ov) {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                targetType, tempFlags);
          return ex_expr::EXPR_ERROR;
        }
      } else {
        double doubleTarget = *doubleTgtPtr;

        if (ov == 1)  // overflow
        {
          if (doubleTarget < 0)  // < -DBL_MAX
          {
            *doubleTgtPtr = -DBL_MAX;
            *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN;
          } else  // > +DBL_MAX
          {
            *doubleTgtPtr = +DBL_MAX;
            *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX;
          }
        }                   // overflow
        else if (ov == -1)  // underflow
        {
          if (doubleTarget < 0)  // > -DBL_MIN
          {
            *doubleTgtPtr = 0;
            *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP;
          } else  // < +DBL_MIN
          {
            *doubleTgtPtr = 0;
            *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN;
          }
        }  // underflow
      }    // data conversion flag

    }; break;

    case CONV_FLOAT64_ASCII: {
      if (targetLen < SQL_REAL_MIN_DISPLAY_SIZE) {
        ExRaiseDetailSqlError(heap, diagsArea, EXE_STRING_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                              targetType, tempFlags);
        return ex_expr::EXPR_ERROR;
      };

      if (convFloat64ToAscii(target, targetLen, targetPrecision, targetScale, *doubleSrcPtr,
                             SQL_DOUBLE_PRECISION_FRAG_DIGITS, varCharLen, varCharLenSize, leftPad) != 0) {
        ExRaiseDetailSqlError(heap, diagsArea, EXE_STRING_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                              targetType, tempFlags);
        return ex_expr::EXPR_ERROR;
      }
    }; break;

    case CONV_BIGNUM_BIN16S:
    case CONV_BIGNUM_BIN16U:
    case CONV_BIGNUM_BIN32S:
    case CONV_BIGNUM_BIN32U: {
      long intermediate;
      int tempDataConvErrorFlag;
      ex_expr::exp_return_type rc;

      if (dataConversionErrorFlag) {
        // want to catch conversion errors in a variable
        tempDataConvErrorFlag =
            convBigNumToInt64AndScale(&intermediate, source, sourceLen, targetScale - sourceScale, heap);
      } else {
        // want to use diagsArea to catch errors
        if (convBigNumToInt64(&intermediate, source, sourceLen, heap, diagsArea,
                              tempFlags | CONV_INTERMEDIATE_CONVERSION) != ex_expr::EXPR_OK)
          return ex_expr::EXPR_ERROR;
      }

      rc = convDoIt((char *)&intermediate, (int)sizeof(long), (short)REC_BIN64_SIGNED,
                    0,  // sourcePrecision
                    0,  // sourceScale
                    target, targetLen, targetType, targetPrecision, targetScale,
                    NULL,  // varCharLen
                    0,     // varCharLenSize
                    heap, diagsArea, CONV_UNKNOWN, dataConversionErrorFlag, tempFlags | CONV_INTERMEDIATE_CONVERSION);

      if (dataConversionErrorFlag) {
        // This is tricky (sigh):  We must tell our caller what happened
        // during this data conversion, but to do so requires combining
        // error information from two conversions. The possible situations
        // that can occur are as follows:
        //
        // conversion to Int 64   conversion to final type    desired return
        // --------------------   ------------------------    --------------
        //  round down to max       round down to max       round down to max
        //         (if we rounded down to the max 64-bit number,
        //          we know we will round down further to the max
        //          value when going to any 16 or 32 bit number)
        //
        //  round up to min         round up to min         round up to min
        //         (similar)
        //
        //  round down              no error                round down
        //         (can happen due to scaling; in this case we
        //          know the 64-bit result and the final type
        //          result are the same, so we know the final
        //          result was rounded down from the original)
        //
        //  round down              round down to max       round down to max
        //         (after all, we rounded down to the max value)
        //
        //  round up                no error                round up
        //         (similar to round down/no error case)
        //
        //  round up                round up to max         round up to max
        //         (similar to round down/round down to max case)
        //
        //  no error                no error                no error
        //         (obvious)
        //
        //  no error                round down to max       round down to max
        //         (obvious)
        //
        //  no error                round up to min         round up to min
        //         (obvious)
        //
        // The other cases can never occur.  (Proof: both routines have the
        // characteristic that the only possible errors for positive numbers
        // are rounding down, and for negative numbers rounding up.  Conversion
        // preserves sign apart from conversion to zero, but if the long
        // result is zero there will be no error on the 2nd conversion.
        // Also, round down and round up can occur only due to
        // scaling, and scaling occurs in convDoIt only when converting from
        // Large Decimal, so round down and round up cannot occur in the 2nd
        // conversion.)
        //
        // Now, inspecting the above chart, we see that the desired result
        // is the same as the 2nd conversion, except possibly when the 2nd
        // conversion reports no error. But in all those cases, the desired
        // result is that of the 1st conversion.  So, the following logic
        // implements the above chart.

        if (*dataConversionErrorFlag == ex_conv_clause::CONV_RESULT_OK)
          *dataConversionErrorFlag = tempDataConvErrorFlag;
      }

      return rc;

    }; break;

    case CONV_BIGNUM_BIN64S: {
      long intermediate;
      if (dataConversionErrorFlag) {
        // want conversion errors in a variable
        *dataConversionErrorFlag =
            convBigNumToInt64AndScale(&intermediate, source, sourceLen, targetScale - sourceScale, heap);
      } else {
        // want conversion errors in diagsArea
        if (convBigNumToInt64(&intermediate, source, sourceLen, heap, diagsArea, tempFlags) != ex_expr::EXPR_OK)
          return ex_expr::EXPR_ERROR;

        if (checkPrecision(intermediate, sourceLen, sourceType, sourcePrecision, sourceScale, targetType,
                           targetPrecision, targetScale, heap, diagsArea, tempFlags) != ex_expr::EXPR_OK)
          return ex_expr::EXPR_ERROR;
      }

      *(long *)target = intermediate;
    }; break;

    case CONV_BIGNUM_BIN64U: {
      UInt64 intermediate;
      if (dataConversionErrorFlag) {
        long intermediateTemp;
        // want conversion errors in a variable
        *dataConversionErrorFlag =
            convBigNumToInt64AndScale(&intermediateTemp, source, sourceLen, targetScale - sourceScale, heap);
      } else {
        // want conversion errors in diagsArea
        if (convBigNumToUInt64(&intermediate, source, sourceLen, heap, diagsArea, tempFlags) != ex_expr::EXPR_OK)
          return ex_expr::EXPR_ERROR;

        if (checkPrecision(intermediate, sourceLen, sourceType, sourcePrecision, sourceScale, targetType,
                           targetPrecision, targetScale, heap, diagsArea, tempFlags) != ex_expr::EXPR_OK)
          return ex_expr::EXPR_ERROR;
      }

      *(UInt64 *)target = intermediate;
    }; break;

    case CONV_BIGNUM_DECU:
    case CONV_BIGNUM_DECS: {
      if (dataConversionErrorFlag) {
        *dataConversionErrorFlag =
            convBigNumToDecAndScale(target, targetLen, source, sourceLen, sourcePrecision, (index == CONV_BIGNUM_DECU),
                                    targetScale - sourceScale, heap);
        return ex_expr::EXPR_OK;
      } else {
        int tempDataConversionErrorFlag =
            convBigNumToDecAndScale(target, targetLen, source, sourceLen, sourcePrecision, (index == CONV_BIGNUM_DECU),
                                    0,  // do not scale
                                    heap);
        if ((tempDataConversionErrorFlag == ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN) ||
            (tempDataConversionErrorFlag == ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX)) {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                targetType, tempFlags);
          return ex_expr::EXPR_ERROR;
        }
        return ex_expr::EXPR_OK;
      };
    }; break;

    case CONV_BIGNUM_FLOAT32:
    case CONV_BIGNUM_FLOAT64: {
      // We first convert Big Num to Ascii (which itself is done by first converting
      // to Large Dec, then from Large Dec to Ascii), then from Ascii to Float.
      // (tbd: It would be probably better to convert from Big Num to Float directly)

      int intermediateLen = sourcePrecision + 1;
      char *intermediate = (char *)heap->allocateMemory(intermediateLen);

      int errorMark;
      if (*diagsArea)
        errorMark = (*diagsArea)->getNumber(DgSqlCode::ERROR_);
      else
        errorMark = 0;

      if (convBigNumToAscii(intermediate, intermediateLen, 0, source, sourceLen, sourcePrecision,
                            (dataConversionErrorFlag ? sourceScale : 0),
                            NULL,  // varCharLen
                            0,     // varCharLenSize
                            leftPad, heap, diagsArea, tempFlags | CONV_INTERMEDIATE_CONVERSION) != ex_expr::EXPR_OK) {
        // if a EXE_NUMERIC OVERFLOW error was generated during the conversion,
        // change it to a warning.
        if (*diagsArea) {
          int errorMark2 = (*diagsArea)->getNumber(DgSqlCode::ERROR_);
          int counter = errorMark2 - errorMark;
          while (counter) {
            if (((*diagsArea)->getErrorEntry(errorMark2 - counter + 1))->getSQLCODE() == -EXE_STRING_OVERFLOW) {
              (*diagsArea)->deleteError(errorMark2 - counter);
              ExRaiseSqlWarning(heap, diagsArea, EXE_NUMERIC_OVERFLOW);
            }
            counter--;
          }
        } else {
          heap->deallocateMemory((void *)intermediate);
          return ex_expr::EXPR_ERROR;
        }
      }

      // Convert ascii to float.
      ex_expr::exp_return_type rc =
          convDoIt(intermediate, intermediateLen, (short)REC_BYTE_F_ASCII,
                   0,                        // sourcePrecision
                   SQLCHARSETCODE_ISO88591,  // sourceScale
                   target, targetLen, targetType, targetPrecision, targetScale,
                   NULL,  // varCharLen
                   0,     // varCharLenSize
                   heap, diagsArea, CONV_UNKNOWN, dataConversionErrorFlag, tempFlags | CONV_INTERMEDIATE_CONVERSION);
      heap->deallocateMemory((void *)intermediate);
      return rc;
    }; break;

    case CONV_BIGNUM_ASCII: {
      return convBigNumToAscii(target, targetLen, targetPrecision, source, sourceLen, sourcePrecision, sourceScale,
                               varCharLen, varCharLenSize, leftPad, heap, diagsArea, tempFlags);
    }; break;

    case CONV_BIGNUM_BIGNUM: {
      return convBigNumToBigNum(target, targetLen, targetType, targetPrecision, source, sourceLen, sourceType,
                                sourcePrecision, heap, diagsArea, tempFlags);
    }; break;

    case CONV_DECLS_DECLS: {
      if (targetLen < sourceLen) {
        // overflow
        ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                              targetType, tempFlags);
        return ex_expr::EXPR_ERROR;
      };

      // copy first character (which may or may not be a sign)
      target[0] = source[0];
      int targetIdx = 1;
      if (targetLen > sourceLen) {
        // target is larger, fill in zeros
        str_pad(&target[targetIdx], targetLen - sourceLen, '0');
        targetIdx = targetLen - sourceLen + 1;
      };
      // copy source to target
      str_cpy_all(&target[targetIdx], &source[1], sourceLen - 1);
    }; break;

    case CONV_DECLS_ASCII: {
      return convDecLStoAscii(target, targetLen, targetPrecision, source, sourceLen, sourceScale, heap, diagsArea,
                              tempFlags);
    }; break;

    case CONV_DECLS_DECU: {
      // We convert first from REC_DECIMAL_LS to REC_DECIMAL_LSE and then from
      // REC_DECIMAL_LSE to REC_DECIMAL_UNSIGNED

      // Conversion from REC_DECIMAL_LS to REC_DECIMAL_LSE

      char *intermediateTarget = new (heap) char[targetLen];
      int intermediateTargetLen = targetLen;

      // if the source is REC_DECIMAL_LS, we use the same conversion as for ASCII,
      // but we ignore the targetScale
      if (convAsciiToDec(intermediateTarget, targetType, intermediateTargetLen,
                         ((sourceType == REC_DECIMAL_LS) ? 0 : targetScale), source, sourceLen, 0, heap, diagsArea,
                         tempFlags | CONV_INTERMEDIATE_CONVERSION) != ex_expr::EXPR_OK) {
        NADELETEBASIC(intermediateTarget, heap);
        return ex_expr::EXPR_ERROR;
      }

      if ((targetType == REC_DECIMAL_UNSIGNED) && (intermediateTarget[0] & 0200)) {
        // unsigned decimal can't be negative
        ExRaiseSqlError(heap, diagsArea, EXE_UNSIGNED_OVERFLOW);
        NADELETEBASIC(intermediateTarget, heap);
        return ex_expr::EXPR_ERROR;
      };

      // Conversion from REC_DECIMAL_LSE to REC_DECIMAL_UNSIGNED
      long intermediate = 0;
      if (convDecToInt64(intermediate, intermediateTarget, intermediateTargetLen, heap, diagsArea,
                         tempFlags | CONV_INTERMEDIATE_CONVERSION) != ex_expr::EXPR_OK) {
        NADELETEBASIC(intermediateTarget, heap);
        return ex_expr::EXPR_ERROR;
      }
      if (convExactToDec(target, targetLen, targetType, intermediate, heap, diagsArea, dataConversionErrorFlag,
                         tempFlags | CONV_INTERMEDIATE_CONVERSION) != ex_expr::EXPR_OK) {
        NADELETEBASIC(intermediateTarget, heap);
        return ex_expr::EXPR_ERROR;
      }
      NADELETEBASIC(intermediateTarget, heap);
      return ex_expr::EXPR_OK;
    }; break;
    case CONV_INTERVALY_INTERVALMO:  // convert years to months
    {
      if (convBinToBinScaleMult(target, targetType, targetLen, targetPrecision, targetScale, source, sourceType,
                                sourceLen, sourcePrecision,
                                12,  // multiply years by 12 to get months
                                heap, diagsArea, dataConversionErrorFlag, tempFlags) != ex_expr::EXPR_OK)
        return ex_expr::EXPR_ERROR;
      return ex_expr::EXPR_OK;
    } break;

    case CONV_INTERVALMO_INTERVALY: {
      if (convBinToBinScaleDiv(target, targetType, targetLen, targetPrecision, targetScale, source, sourceType,
                               sourceLen, sourcePrecision,
                               12,  // divide months by 12 to get years
                               heap, diagsArea, dataConversionErrorFlag, tempFlags) != ex_expr::EXPR_OK)
        return ex_expr::EXPR_ERROR;
      return ex_expr::EXPR_OK;
    } break;

    case CONV_INTERVALD_INTERVALH: {
      if (convBinToBinScaleMult(target, targetType, targetLen, targetPrecision, targetScale, source, sourceType,
                                sourceLen, sourcePrecision,
                                24,  // multiply days by 24 to get hours
                                heap, diagsArea, dataConversionErrorFlag, tempFlags) != ex_expr::EXPR_OK)
        return ex_expr::EXPR_ERROR;
      return ex_expr::EXPR_OK;
    } break;

    case CONV_INTERVALD_INTERVALM: {
      if (convBinToBinScaleMult(target, targetType, targetLen, targetPrecision, targetScale, source, sourceType,
                                sourceLen, sourcePrecision,
                                24 * 60,  // multiply days by 24*60 to get minutes
                                heap, diagsArea, dataConversionErrorFlag, tempFlags) != ex_expr::EXPR_OK)
        return ex_expr::EXPR_ERROR;
      return ex_expr::EXPR_OK;
    } break;

    case CONV_INTERVALD_INTERVALS: {
      long scaleFactor = 24 * 60 * 60  // multiply days by 24*60*60 to get seconds
                         * tenToPowerOf[targetScale];

      if (convBinToBinScaleMult(target, targetType, targetLen, targetPrecision, targetScale, source, sourceType,
                                sourceLen, sourcePrecision, scaleFactor, heap, diagsArea, dataConversionErrorFlag,
                                tempFlags) != ex_expr::EXPR_OK)
        return ex_expr::EXPR_ERROR;
      return ex_expr::EXPR_OK;
    } break;

    case CONV_INTERVALH_INTERVALD: {
      if (convBinToBinScaleDiv(target, targetType, targetLen, targetPrecision, targetScale, source, sourceType,
                               sourceLen, sourcePrecision,
                               24,  // divide hours by 24 to get days
                               heap, diagsArea, dataConversionErrorFlag, tempFlags) != ex_expr::EXPR_OK)
        return ex_expr::EXPR_ERROR;
      return ex_expr::EXPR_OK;
    } break;

    case CONV_INTERVALH_INTERVALM: {
      if (convBinToBinScaleMult(target, targetType, targetLen, targetPrecision, targetScale, source, sourceType,
                                sourceLen, sourcePrecision,
                                60,  // multiply hours by 60 to get minutes
                                heap, diagsArea, dataConversionErrorFlag, tempFlags) != ex_expr::EXPR_OK)
        return ex_expr::EXPR_ERROR;
      return ex_expr::EXPR_OK;
    } break;

    case CONV_INTERVALH_INTERVALS: {
      long scaleFactor = 60 * 60  // multiply hours by 60*60 to get seconds
                         * tenToPowerOf[targetScale];

      if (convBinToBinScaleMult(target, targetType, targetLen, targetPrecision, targetScale, source, sourceType,
                                sourceLen, sourcePrecision, scaleFactor, heap, diagsArea, dataConversionErrorFlag,
                                tempFlags) != ex_expr::EXPR_OK)
        return ex_expr::EXPR_ERROR;
      return ex_expr::EXPR_OK;
    } break;

    case CONV_INTERVALM_INTERVALD: {
      if (convBinToBinScaleDiv(target, targetType, targetLen, targetPrecision, targetScale, source, sourceType,
                               sourceLen, sourcePrecision,
                               60 * 24,  // divide minutes by 60*24 to get days
                               heap, diagsArea, dataConversionErrorFlag, tempFlags) != ex_expr::EXPR_OK)
        return ex_expr::EXPR_ERROR;
      return ex_expr::EXPR_OK;
    } break;

    case CONV_INTERVALM_INTERVALH: {
      if (convBinToBinScaleDiv(target, targetType, targetLen, targetPrecision, targetScale, source, sourceType,
                               sourceLen, sourcePrecision,
                               60,  // divide minutes by 60 to get hours
                               heap, diagsArea, dataConversionErrorFlag, tempFlags) != ex_expr::EXPR_OK)
        return ex_expr::EXPR_ERROR;
      return ex_expr::EXPR_OK;
    } break;

    case CONV_INTERVALM_INTERVALS: {
      long scaleFactor = 60  // multiply minutes by 60 to get seconds
                         * tenToPowerOf[targetScale];

      if (convBinToBinScaleMult(target, targetType, targetLen, targetPrecision, targetScale, source, sourceType,
                                sourceLen, sourcePrecision, scaleFactor, heap, diagsArea, dataConversionErrorFlag,
                                tempFlags) != ex_expr::EXPR_OK)
        return ex_expr::EXPR_ERROR;
      return ex_expr::EXPR_OK;
    } break;

    case CONV_INTERVALS_INTERVALD: {
      long scaleFactor = 60 * 60 * 24                  // divide seconds by 60*60*24 to get days
                         * tenToPowerOf[sourceScale];  // divide again by 10**fraction precision

      if (convBinToBinScaleDiv(target, targetType, targetLen, targetPrecision, targetScale, source, sourceType,
                               sourceLen, sourcePrecision, scaleFactor, heap, diagsArea, dataConversionErrorFlag,
                               tempFlags) != ex_expr::EXPR_OK)
        return ex_expr::EXPR_ERROR;
      return ex_expr::EXPR_OK;
    } break;

    case CONV_INTERVALS_INTERVALH: {
      long scaleFactor = 60 * 60                       // divide seconds by 60*60 to get hours
                         * tenToPowerOf[sourceScale];  // divide again by 10**fraction precision

      if (convBinToBinScaleDiv(target, targetType, targetLen, targetPrecision, targetScale, source, sourceType,
                               sourceLen, sourcePrecision, scaleFactor, heap, diagsArea, dataConversionErrorFlag,
                               tempFlags) != ex_expr::EXPR_OK)
        return ex_expr::EXPR_ERROR;
      return ex_expr::EXPR_OK;
    } break;

    case CONV_INTERVALS_INTERVALM: {
      long scaleFactor = 60                            // divide seconds by 60 to get minutes
                         * tenToPowerOf[sourceScale];  // divide again by 10**fraction precision

      if (convBinToBinScaleDiv(target, targetType, targetLen, targetPrecision, targetScale, source, sourceType,
                               sourceLen, sourcePrecision, scaleFactor, heap, diagsArea, dataConversionErrorFlag,
                               tempFlags) != ex_expr::EXPR_OK)
        return ex_expr::EXPR_ERROR;
      return ex_expr::EXPR_OK;
    } break;

    case CONV_INTERVALS_INTERVALS_DIV:  // second(n) to second(m), n > m
    {
      if (convBinToBinScaleDiv(target, targetType, targetLen, targetPrecision, targetScale, source, sourceType,
                               sourceLen, sourcePrecision, tenToPowerOf[sourceScale - targetScale], heap, diagsArea,
                               dataConversionErrorFlag, tempFlags) != ex_expr::EXPR_OK)
        return ex_expr::EXPR_ERROR;

      return ex_expr::EXPR_OK;
    } break;

    case CONV_INTERVALS_INTERVALS_MULT:  // second(n) to second(m), m > n
    {
      if (convBinToBinScaleMult(target, targetType, targetLen, targetPrecision, targetScale, source, sourceType,
                                sourceLen, sourcePrecision, tenToPowerOf[targetScale - sourceScale], heap, diagsArea,
                                dataConversionErrorFlag, tempFlags) != ex_expr::EXPR_OK)
        return ex_expr::EXPR_ERROR;
      return ex_expr::EXPR_OK;
    } break;

    case CONV_INTERVAL_ASCII: {
      return convIntervalToAscii(source, sourceLen,
                                 sourcePrecision,  // leading precision
                                 sourceScale,      // fraction precision
                                 sourceType, target, targetLen,
                                 targetPrecision,  // max chars allowed or 0
                                 varCharLen, varCharLenSize, leftPad, heap, diagsArea);
    }; break;

    case CONV_INTERVAL_UNICODE: {
      charBuf cBuf(targetLen / BYTES_PER_NAWCHAR, heap);

      ex_expr::exp_return_type ok = convIntervalToAscii(source, sourceLen,
                                                        sourcePrecision,  // leading precision
                                                        sourceScale,      // fraction precision
                                                        sourceType, (char *)cBuf.data(),
                                                        cBuf.getBufSize(),  // target buffer size
                                                        0, varCharLen, varCharLenSize, leftPad, heap, diagsArea);

      if (ok != ex_expr::EXPR_OK) return ex_expr::EXPR_ERROR;

      NAWcharBuf *wBuf = NULL;

      wBuf = ISO88591ToUnicode(cBuf, heap, wBuf);

      if (wBuf && cBuf.getStrLen() == wBuf->getStrLen()) {
        str_cpy_all(target, (char *)(wBuf->data()), BYTES_PER_NAWCHAR * (wBuf->getStrLen()));

        if (varCharLenSize != 0 && varCharLen) double_varchar_length(varCharLen, varCharLenSize);

      } else {
        char hexstr[MAX_OFFENDING_SOURCE_DATA_DISPLAY_LEN];
        ExRaiseSqlError(heap, diagsArea, EXE_CONVERT_DATETIME_ERROR, NULL, NULL, NULL, NULL,
                        stringToHex(hexstr, sizeof(hexstr), source, sourceLen));

        ok = ex_expr::EXPR_ERROR;
      }

      NADELETE(wBuf, NAWcharBuf, heap);

      return ok;

    }; break;

    case CONV_DATETIME_DATETIME: {
      return convDatetimeDatetime(target, targetLen, (REC_DATETIME_CODE)targetPrecision, targetScale, source, sourceLen,
                                  (REC_DATETIME_CODE)sourcePrecision, sourceScale, heap, diagsArea,
                                  (short)(flags & CONV_ENABLE_DATETIME_VALIDATION), dataConversionErrorFlag);
    }; break;

    case CONV_DATETIME_ASCII: {
      if (convDatetimeToAscii(target, targetLen, targetPrecision, source, sourceLen, (REC_DATETIME_CODE)sourcePrecision,
                              (short)sourceScale,  // fractionprecision
                              varCharLen, varCharLenSize, leftPad, heap, diagsArea) != ex_expr::EXPR_OK)
        return ex_expr::EXPR_ERROR;
    }; break;

      //
      // 6/5/98: added for datetime to unicode
      //
    case CONV_DATETIME_UNICODE: {
      charBuf cBuf(targetLen / BYTES_PER_NAWCHAR, heap);

      // to ascii version first
      ex_expr::exp_return_type ok = convDatetimeToAscii((char *)cBuf.data(),
                                                        cBuf.getBufSize(),  // target buffer size
                                                        0, source, sourceLen, (REC_DATETIME_CODE)sourcePrecision,
                                                        (short)sourceScale,  // fractionprecision
                                                        varCharLen, varCharLenSize, leftPad, heap, diagsArea);

      if (ok != ex_expr::EXPR_OK) return ex_expr::EXPR_ERROR;

      NAWcharBuf *wBuf = NULL;

      // get the final unicode version
      wBuf = ISO88591ToUnicode(cBuf, heap, wBuf);

      if (wBuf && cBuf.getStrLen() == wBuf->getStrLen()) {
        str_cpy_all(target, (char *)(wBuf->data()), BYTES_PER_NAWCHAR * (wBuf->getStrLen()));

        // set the varchar length if necessary
        if (varCharLenSize != 0 && varCharLen) double_varchar_length(varCharLen, varCharLenSize);

        ok = ex_expr::EXPR_OK;
      } else {
        char hexstr[MAX_OFFENDING_SOURCE_DATA_DISPLAY_LEN];
        ExRaiseSqlError(heap, diagsArea, EXE_CONVERT_DATETIME_ERROR, NULL, NULL, NULL, NULL,
                        stringToHex(hexstr, sizeof(hexstr), source, sourceLen));
        ok = ex_expr::EXPR_ERROR;
      }

      NADELETE(wBuf, NAWcharBuf, heap);

      return ok;

    }; break;

    case CONV_ASCII_BIN8S:
    case CONV_ASCII_BIN8U:
    case CONV_ASCII_BIN16S:
    case CONV_ASCII_BIN16U:
    case CONV_ASCII_BIN32S:
    case CONV_ASCII_BIN32U: {
      long interm;
      if (convAsciiToInt64(interm, targetScale, source, sourceLen, heap, diagsArea,
                           tempFlags | CONV_INTERMEDIATE_CONVERSION) != ex_expr::EXPR_OK)
        return ex_expr::EXPR_ERROR;

      switch (index) {
        case CONV_ASCII_BIN8S: {
          if (interm < SCHAR_MIN) {
            if (dataConversionErrorFlag != 0)  // Capture error in variable?
            {
              *(Target<Int8> *)target = SCHAR_MIN;
              *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN;
            } else {
              ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                    targetType, tempFlags);
              return ex_expr::EXPR_ERROR;
            }
          } else if (interm > SCHAR_MAX) {
            if (dataConversionErrorFlag != 0)  // Capture error in variable?
            {
              *(Target<Int8> *)target = SCHAR_MAX;
              *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX;
            } else {
              ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                    targetType, tempFlags);
              return ex_expr::EXPR_ERROR;
            }
          } else {
            if (dataConversionErrorFlag != 0) {  // Set the target value.
              *(Target<Int8> *)target = (Int8)interm;
            } else {  // Check target precision. Then set target value.
              if ((targetPrecision > 0) &&
                  (checkPrecision(interm, 8, REC_BIN64_SIGNED, 0, 0, targetType, targetPrecision, targetScale, heap,
                                  diagsArea, tempFlags, source) != ex_expr::EXPR_OK)) {
                return ex_expr::EXPR_ERROR;
              }
              *(Target<Int8> *)target = (Int8)interm;
            }
          }
        }; break;
        case CONV_ASCII_BIN8U: {
          if (interm < 0) {
            if (dataConversionErrorFlag != 0)  // Capture error in variable?
            {
              *(Target<unsigned short> *)target = 0;
              *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN;
            } else {
              ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                    targetType, tempFlags);
              return ex_expr::EXPR_ERROR;
            }
          } else if (interm > UCHAR_MAX) {
            if (dataConversionErrorFlag != 0)  // Capture error in variable?
            {
              *(Target<UInt8> *)target = UCHAR_MAX;
              *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX;
            } else {
              ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                    targetType, tempFlags);
              return ex_expr::EXPR_ERROR;
            }
          } else {
            if (dataConversionErrorFlag != 0) {  // Set the target value.
              *(Target<UInt8> *)target = (UInt8)interm;
            } else {  // Check target precision. Then set target value.
              if ((targetPrecision > 0) &&
                  (checkPrecision(interm, 8, REC_BIN64_SIGNED, 0, 0, targetType, targetPrecision, targetScale, heap,
                                  diagsArea, tempFlags, source) != ex_expr::EXPR_OK)) {
                return ex_expr::EXPR_ERROR;
              }
              *(Target<UInt8> *)target = (UInt8)interm;
            }
          }
        }; break;
        case CONV_ASCII_BIN16S: {
          if (interm < SHRT_MIN) {
            if (dataConversionErrorFlag != 0)  // Capture error in variable?
            {
              *(Target<short> *)target = SHRT_MIN;
              *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN;
            } else {
              ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                    targetType, tempFlags);
              return ex_expr::EXPR_ERROR;
            }
          } else if (interm > SHRT_MAX) {
            if (dataConversionErrorFlag != 0)  // Capture error in variable?
            {
              *(Target<short> *)target = SHRT_MAX;
              *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX;
            } else {
              ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                    targetType, tempFlags);
              return ex_expr::EXPR_ERROR;
            }
          } else {
            if (dataConversionErrorFlag != 0) {  // Set the target value.
              *(Target<short> *)target = (short)interm;
            } else {  // Check target precision. Then set target value.
              if ((targetPrecision > 0) &&
                  (checkPrecision(interm, 8, REC_BIN64_SIGNED, 0, 0, targetType, targetPrecision, targetScale, heap,
                                  diagsArea, tempFlags, source) != ex_expr::EXPR_OK)) {
                return ex_expr::EXPR_ERROR;
              }
              *(Target<short> *)target = (short)interm;
            }
          }
        }; break;
        case CONV_ASCII_BIN16U: {
          if (interm < 0) {
            if (dataConversionErrorFlag != 0)  // Capture error in variable?
            {
              *(Target<unsigned short> *)target = 0;
              *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN;
            } else {
              ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                    targetType, tempFlags);
              return ex_expr::EXPR_ERROR;
            }
          } else if (interm > USHRT_MAX) {
            if (dataConversionErrorFlag != 0)  // Capture error in variable?
            {
              *(Target<unsigned short> *)target = USHRT_MAX;
              *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX;
            } else {
              ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                    targetType, tempFlags);
              return ex_expr::EXPR_ERROR;
            }
          } else {
            if (dataConversionErrorFlag != 0) {  // Set the target value.
              *(Target<unsigned short> *)target = (unsigned short)interm;
            } else {  // Check target precision. Then set target value.
              if ((targetPrecision > 0) &&
                  (checkPrecision(interm, 8, REC_BIN64_SIGNED, 0, 0, targetType, targetPrecision, targetScale, heap,
                                  diagsArea, tempFlags, source) != ex_expr::EXPR_OK)) {
                return ex_expr::EXPR_ERROR;
              }
              *(Target<unsigned short> *)target = (unsigned short)interm;
            }
          }
        }; break;
        case CONV_ASCII_BIN32S: {
          if (interm < INT_MIN) {
            if (dataConversionErrorFlag != 0)  // Capture error in variable?
            {
              *(Target<int> *)target = INT_MIN;
              *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN;
            } else {
              ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                    targetType, tempFlags);
              return ex_expr::EXPR_ERROR;
            }
          } else if (interm > INT_MAX) {
            if (dataConversionErrorFlag != 0)  // Capture error in variable?
            {
              *(Target<int> *)target = INT_MAX;
              *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX;
            } else {
              ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                    targetType, tempFlags);
              return ex_expr::EXPR_ERROR;
            }
          } else {
            if (dataConversionErrorFlag != 0) {  // Set the target value.
              *(Target<int> *)target = (int)interm;
            } else {  // Check target precision. Then set target value.
              if ((targetPrecision > 0) &&
                  (checkPrecision(interm, 8, REC_BIN64_SIGNED, 0, 0, targetType, targetPrecision, targetScale, heap,
                                  diagsArea, tempFlags, source) != ex_expr::EXPR_OK)) {
                return ex_expr::EXPR_ERROR;
              }
              *(Target<int> *)target = (int)interm;
            }
          }
        }; break;
        case CONV_ASCII_BIN32U: {
          if (interm < 0) {
            if (dataConversionErrorFlag != 0)  // Capture error in variable?
            {
              *(Target<int> *)target = 0;
              *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP_TO_MIN;
            } else {
              ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                    targetType, tempFlags);
              return ex_expr::EXPR_ERROR;
            }
          } else if (interm > UINT_MAX) {
            if (dataConversionErrorFlag != 0)  // Capture error in variable?
            {
              *(Target<int> *)target = UINT_MAX;
              *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN_TO_MAX;
            } else {
              ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                                    targetType, tempFlags);
              return ex_expr::EXPR_ERROR;
            }
          } else {
            if (dataConversionErrorFlag != 0) {  // Set the target value.
              *(Target<int> *)target = (int)interm;
            } else {  // Check target precision. Then set target value.
              if ((targetPrecision > 0) &&
                  (checkPrecision(interm, 8, REC_BIN64_SIGNED, 0, 0, targetType, targetPrecision, targetScale, heap,
                                  diagsArea, tempFlags, source) != ex_expr::EXPR_OK)) {
                return ex_expr::EXPR_ERROR;
              }
              *(Target<int> *)target = (int)interm;
            }
          }
        }; break;
        default:
          break;
      }
    }; break;

      //
      // 6/3/98: added for Cast() NCHAR extension.
      //
    case CONV_BIN16S_UNICODE:
    case CONV_BIN16U_UNICODE:
    case CONV_BIN32S_UNICODE:
    case CONV_BIN32U_UNICODE:
    case CONV_BIN64S_UNICODE:
    case CONV_FLOAT32_UNICODE:
    case CONV_FLOAT64_UNICODE:
    case CONV_DECS_UNICODE:
    case CONV_BIGNUM_UNICODE: {
      ConvInstruction cci = CONV_UNKNOWN;  // next switch will change.
      switch (index) {
        case CONV_BIN16S_UNICODE:
          cci = CONV_BIN16S_ASCII;
          break;
        case CONV_BIN16U_UNICODE:
          cci = CONV_BIN16U_ASCII;
          break;
        case CONV_BIN32S_UNICODE:
          cci = CONV_BIN32S_ASCII;
          break;
        case CONV_BIN32U_UNICODE:
          cci = CONV_BIN32U_ASCII;
          break;
        case CONV_BIN64S_UNICODE:
          cci = CONV_BIN64S_ASCII;
          break;
        case CONV_FLOAT32_UNICODE:
          cci = CONV_FLOAT32_ASCII;
          break;
        case CONV_FLOAT64_UNICODE:
          cci = CONV_FLOAT64_ASCII;
          break;
        case CONV_DECS_UNICODE:
          cci = CONV_DECS_ASCII;
          break;
        case CONV_BIGNUM_UNICODE:
          cci = CONV_BIGNUM_ASCII;
          break;
        default:
          break;
      }

      // get the ascii version first
      charBuf cBuf(targetLen / BYTES_PER_NAWCHAR, heap);

      ex_expr::exp_return_type ok =
          convDoIt(source, sourceLen, sourceType, sourcePrecision, sourceScale, (char *)cBuf.data(),
                   cBuf.getBufSize(),  // target buffer size
                   REC_BYTE_F_ASCII, targetPrecision, targetScale, varCharLen, varCharLenSize, heap, diagsArea, cci, 0,
                   tempFlags | CONV_INTERMEDIATE_CONVERSION);

      if (ok == ex_expr::EXPR_ERROR) return ok;

      // refine the length for varchar typed target
      int final_length;
      if (varCharLenSize != 0 && varCharLen) {
        if (varCharLenSize == sizeof(short))
          final_length = *(short *)varCharLen;
        else
          final_length = *(int *)varCharLen;

        cBuf.setStrLen(final_length);
      }

      NAWcharBuf *wBuf = NULL;

      // get the final unicode version
      wBuf = ISO88591ToUnicode(cBuf, heap, wBuf);

      if (wBuf && (wBuf->getStrLen() == cBuf.getStrLen())) {
        str_cpy_all(target, (char *)(wBuf->data()), BYTES_PER_NAWCHAR * (wBuf->getStrLen()));

        if (varCharLenSize != 0 && varCharLen) double_varchar_length(varCharLen, varCharLenSize);

        ok = ex_expr::EXPR_OK;
      } else {
        char hexstr[MAX_OFFENDING_SOURCE_DATA_DISPLAY_LEN];
        ExRaiseSqlError(heap, diagsArea, EXE_CONVERT_STRING_ERROR, NULL, NULL, NULL, NULL,
                        stringToHex(hexstr, sizeof(hexstr), source, sourceLen));

        ok = ex_expr::EXPR_ERROR;
      }

      NADELETE(wBuf, NAWcharBuf, heap);

      return ok;
    }; break;

      // 6/2/98: extend Cast() for NCHAR
    case CONV_UNICODE_BIN16S:
    case CONV_UNICODE_BIN16U:
    case CONV_UNICODE_BIN32S:
    case CONV_UNICODE_BIN32U:
    case CONV_UNICODE_BIN64S:
    case CONV_UNICODE_FLOAT32:
    case CONV_UNICODE_FLOAT64:
    case CONV_UNICODE_DEC:
    case CONV_UNICODE_BIGNUM: {
      // We make use of the ascii version. Before that we need
      // to the ascii version.
      const NAWcharBuf wBuf((NAWchar *)source, sourceLen / BYTES_PER_NAWCHAR, heap);
      charBuf *cBuf = NULL;

      cBuf = unicodeToISO88591(wBuf, heap, cBuf);

      // If we have it, make sure all content has been included.
      if (cBuf) {
        if (cBuf->getStrLen() == sourceLen / BYTES_PER_NAWCHAR) {
          ConvInstruction cci = CONV_UNKNOWN;
          switch (index) {
            case CONV_UNICODE_BIN16S:
              cci = CONV_ASCII_BIN16S;
              break;
            case CONV_UNICODE_BIN16U:
              cci = CONV_ASCII_BIN16U;
              break;
            case CONV_UNICODE_BIN32S:
              cci = CONV_ASCII_BIN32S;
              break;
            case CONV_UNICODE_BIN32U:
              cci = CONV_ASCII_BIN32U;
              break;
            case CONV_UNICODE_BIN64S:
              cci = CONV_ASCII_BIN64S;
              break;
            case CONV_UNICODE_FLOAT32:
              cci = CONV_ASCII_FLOAT32;
              break;
            case CONV_UNICODE_FLOAT64:
              cci = CONV_ASCII_FLOAT64;
              break;
            case CONV_UNICODE_DEC:
              cci = CONV_ASCII_DEC;
              break;
            case CONV_UNICODE_BIGNUM:
              cci = CONV_ASCII_BIGNUM;
              break;
            default:
              break;
          }

          ex_expr::exp_return_type ok = convDoIt((char *)cBuf->data(),
                                                 cBuf->getStrLen(),  // source string length in bytes
                                                 REC_BYTE_F_ASCII, sourcePrecision, SQLCHARSETCODE_ISO88591, target,
                                                 targetLen, targetType, targetPrecision, targetScale,
                                                 NULL,  // varCharLen
                                                 0,     // varCharLenSize
                                                 heap, diagsArea, cci, 0, tempFlags | CONV_INTERMEDIATE_CONVERSION);

          NADELETE(cBuf, charBuf, heap);

          return ok;
        }
      }

      char hexstr[MAX_OFFENDING_SOURCE_DATA_DISPLAY_LEN];
      ExRaiseSqlError(heap, diagsArea, EXE_CONVERT_STRING_ERROR, NULL, NULL, NULL, NULL,
                      stringToHex(hexstr, sizeof(hexstr), source, sourceLen));

      return ex_expr::EXPR_ERROR;
    }; break;

    case CONV_ASCII_BIN64S: {
      long intermediate;
      if (convAsciiToInt64(intermediate, targetScale, source, sourceLen, heap, diagsArea, tempFlags) !=
          ex_expr::EXPR_OK)
        return ex_expr::EXPR_ERROR;

      if (checkPrecision(intermediate, 8, REC_BIN64_SIGNED, sourcePrecision, sourceScale, targetType, targetPrecision,
                         targetScale, heap, diagsArea, tempFlags, source) != ex_expr::EXPR_OK)
        return ex_expr::EXPR_ERROR;

      *(long *)target = intermediate;
    }; break;

    case CONV_ASCII_BIN64U: {
      UInt64 intermediate;
      if (convAsciiToUInt64(intermediate, targetScale, source, sourceLen, heap, diagsArea, tempFlags) !=
          ex_expr::EXPR_OK)
        return ex_expr::EXPR_ERROR;

      if (checkPrecision(intermediate, 8, REC_BIN64_SIGNED, sourcePrecision, sourceScale, targetType, targetPrecision,
                         targetScale, heap, diagsArea, tempFlags, source) != ex_expr::EXPR_OK)
        return ex_expr::EXPR_ERROR;

      *(UInt64 *)target = intermediate;
    }; break;

    case CONV_ASCII_DEC: {
      // if the source is REC_DECIMAL_LS, we use the same conversion as for ASCII,
      // but we ignore the targetScale
      if (convAsciiToDec(target, targetType, targetLen, ((sourceType == REC_DECIMAL_LS) ? 0 : targetScale), source,
                         sourceLen, 0, heap, diagsArea, tempFlags) != ex_expr::EXPR_OK)
        return ex_expr::EXPR_ERROR;

      if ((targetType == REC_DECIMAL_UNSIGNED) && (target[0] & 0200)) {
        // unsigned decimal can't be negative
        ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, source, sourceLen, sourceType, sourceScale,
                              targetType, tempFlags);
        return ex_expr::EXPR_ERROR;
      };
    }; break;

    case CONV_ASCII_FLOAT32: {
      double dintermediate;
      if (convAsciiToFloat64((char *)&dintermediate, source, sourceLen, heap, diagsArea,
                             tempFlags | CONV_INTERMEDIATE_CONVERSION) != ex_expr::EXPR_OK)
        return ex_expr::EXPR_ERROR;

      if (convDoIt((char *)&dintermediate, sizeof(double), REC_FLOAT64, targetPrecision, targetScale, target, targetLen,
                   targetType, targetPrecision, 0, NULL, 0, heap, diagsArea, CONV_UNKNOWN, 0,
                   tempFlags | CONV_INTERMEDIATE_CONVERSION) != ex_expr::EXPR_OK)
        return ex_expr::EXPR_ERROR;

    }; break;

    case CONV_ASCII_FLOAT64: {
      return convAsciiToFloat64(target, source, sourceLen, heap, diagsArea, tempFlags);
    }; break;

    case CONV_ASCII_BIGNUM: {
      return convAsciiToBigNum(target, targetLen, targetType, targetPrecision, targetScale, source, sourceLen, heap,
                               diagsArea, tempFlags);

    }; break;

    case CONV_ASCII_DATETIME: {
      return convAsciiToDatetime(target, targetLen, (REC_DATETIME_CODE)targetPrecision,
                                 (short)targetScale,  // fractionprecision
                                 source, sourceLen, heap, diagsArea, flags);
    }; break;

      // 6/2/98: added for Unicode to Datetime conversion.
      // This is treated as a special case as we need to call
      // convAsciiToDateTime().

    case CONV_UNICODE_DATETIME: {
      const NAWcharBuf wBuf((NAWchar *)source, sourceLen / BYTES_PER_NAWCHAR, heap);
      charBuf *cBuf = NULL;

      cBuf = unicodeToISO88591(wBuf, heap, cBuf);

      if (cBuf) {
        if (cBuf->getStrLen() == sourceLen / BYTES_PER_NAWCHAR) {
          ex_expr::exp_return_type ok = convAsciiToDatetime(target, targetLen, (REC_DATETIME_CODE)targetPrecision,
                                                            (short)targetScale,  // fractionprecision
                                                            (char *)cBuf->data(),
                                                            cBuf->getStrLen(),  // in bytes
                                                            heap, diagsArea, 0);

          NADELETE(cBuf, charBuf, heap);

          return ok;
        }
      }

      char hexstr[MAX_OFFENDING_SOURCE_DATA_DISPLAY_LEN];
      ExRaiseSqlError(heap, diagsArea, EXE_CONVERT_DATETIME_ERROR, NULL, NULL, NULL, NULL,
                      stringToHex(hexstr, sizeof(hexstr), source, sourceLen));

      return ex_expr::EXPR_ERROR;

    }; break;

      //
      // 6/5/98: added for unicode to interval
      //
    case CONV_UNICODE_INTERVAL: {
      const NAWcharBuf wBuf((NAWchar *)source, sourceLen / BYTES_PER_NAWCHAR, heap);
      charBuf *cBuf = NULL;

      cBuf = unicodeToISO88591(wBuf, heap, cBuf);

      if (cBuf) {
        if (cBuf->getStrLen() == sourceLen / BYTES_PER_NAWCHAR) {
          ex_expr::exp_return_type ok =
              convAsciiToInterval(target, targetLen, targetType,
                                  targetPrecision,  // leading precision
                                  targetScale,      // fractionprecision
                                  (char *)cBuf->data(),
                                  cBuf->getStrLen(),  // in bytes
                                  allowSignInInterval, heap, diagsArea, tempFlags | CONV_INTERMEDIATE_CONVERSION);

          NADELETE(cBuf, charBuf, heap);

          return ok;
        }
      }

      char hexstr[MAX_OFFENDING_SOURCE_DATA_DISPLAY_LEN];
      ExRaiseSqlError(heap, diagsArea, EXE_CONVERT_DATETIME_ERROR, NULL, NULL, NULL, NULL,
                      stringToHex(hexstr, sizeof(hexstr), source, sourceLen));
      return ex_expr::EXPR_ERROR;
    }; break;

    case CONV_ASCII_INTERVAL: {
      return convAsciiToInterval(target, targetLen, targetType,
                                 targetPrecision,  // leading precision
                                 targetScale,      // fractionprecision
                                 source, sourceLen, allowSignInInterval, heap, diagsArea, tempFlags);
    }; break;

    case CONV_ASCII_F_V:
    case CONV_ASCII_V_V: {
      // check for needed character conversions or validations
      if (requiresNoConvOrVal(sourceLen, sourcePrecision, sourceScale, targetLen, targetPrecision, targetScale,
                              index)) {
        // this case can often be translated into PCODE
        int copyLen = ((targetLen >= sourceLen) ? sourceLen : targetLen);

        str_cpy_all(target, source, copyLen);
        setVCLength(varCharLen, varCharLenSize, copyLen);

        if (copyLen < sourceLen) {
          char *first_nonblank = str_find_first_nonpad(&source[copyLen], sourceLen - copyLen, ' ');

          if (first_nonblank)  // if truncated portion isn't all blanks
          {
            if (dataConversionErrorFlag)  // want conversion flag instead of warning?
            {
              // yes - determine whether target is greater than or less than source
              if (*(unsigned char *)first_nonblank < ' ')  // note assumption: ASCII character set
                *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP;
              else
                *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN;
            } else
              // no - raise a warning
              ExRaiseSqlWarning(heap, diagsArea, EXE_STRING_OVERFLOW);
          }
        }
      } else {
        // this case isn't translated into PCODE at this time
        int convertedDataLen;

        if (convCharToChar(source, sourceLen, sourceType, sourcePrecision, sourceScale, target, targetLen, targetType,
                           targetPrecision, targetScale, heap, diagsArea, dataConversionErrorFlag, &convertedDataLen,
                           FALSE, TRUE, flags & CONV_ALLOW_INVALID_CODE_VALUE) != ex_expr::EXPR_OK)
          return ex_expr::EXPR_ERROR;
        setVCLength(varCharLen, varCharLenSize, convertedDataLen);
      }
    }; break;

      // gb2312 -> utf8
      // JIRA 1720
    case CONV_GBK_F_UTF8_V: {
      int copyLen = 0;
      int convLen = gbkToUtf8(source, sourceLen, target, targetLen);
      if (convLen >= 0) {
        copyLen = convLen;
        if (varCharLen) setVCLength(varCharLen, varCharLenSize, copyLen);
        // if the target length is not enough, instead of truncate, raise a SQL Error
        if (convLen > targetLen) ExRaiseSqlError(heap, diagsArea, EXE_STRING_OVERFLOW);
      } else {
        convLen = 0;
        copyLen = 0;
        if (varCharLen) setVCLength(varCharLen, varCharLenSize, copyLen);
        char hexstr[MAX_OFFENDING_SOURCE_DATA_DISPLAY_LEN];
        ExRaiseSqlError(heap, diagsArea, EXE_CONVERT_STRING_ERROR, NULL, NULL, NULL, NULL,
                        stringToHex(hexstr, sizeof(hexstr), source, sourceLen));

        return ex_expr::EXPR_ERROR;
      }
    }; break;
      // 5/10/98: sjis -> unicode
    case CONV_SJIS_F_UNICODE_F:
    case CONV_SJIS_F_UNICODE_V:
    case CONV_SJIS_V_UNICODE_F:
    case CONV_SJIS_V_UNICODE_V: {
      int copyLen, convertedLen;
      const charBuf cBuf((unsigned char *)source, sourceLen, heap);
      NAWcharBuf *wBuf = NULL;

      wBuf = sjisToUnicode(cBuf, heap, wBuf);

      if (wBuf) {
        convertedLen = BYTES_PER_NAWCHAR * (wBuf->getStrLen());

        copyLen = (convertedLen < targetLen) ? convertedLen : targetLen;

        str_cpy_all(target, (char *)(wBuf->data()), copyLen);

        if (convertedLen > targetLen) {
          NAWchar *first_nonblank = wc_str_find_first_nonblank(&((wBuf->data())[targetLen / BYTES_PER_NAWCHAR]),
                                                               (convertedLen - targetLen) / BYTES_PER_NAWCHAR);

          if (first_nonblank)  // if truncated portion isn't all blanks
          {
            if (dataConversionErrorFlag)  // want conversion flag instead of warning?
            {
              // yes - determine whether target is greater than or less than source
              // note assumption: UNICODE character set
              if (*first_nonblank < unicode_char_set::space_char())
                *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP;
              else
                *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN;
            } else
              // no - raise a warning
              ExRaiseSqlWarning(heap, diagsArea, EXE_STRING_OVERFLOW);
          }
        }

        NADELETE(wBuf, NAWcharBuf, heap);

      } else {
        convertedLen = 0;
        copyLen = 0;
      }
      // There is no need to change the char string storage format
      //      for Unicode string.

      if (varCharLen) setVCLength(varCharLen, varCharLenSize, copyLen);

      if ((index == CONV_SJIS_F_UNICODE_F || index == CONV_SJIS_V_UNICODE_F) && copyLen < targetLen)
        wc_str_pad((NAWchar *)(&target[copyLen]), (targetLen - copyLen) >> 1,
                   flags & CONV_PAD_USE_ZERO ? unicode_char_set::ASCIINULL : unicode_char_set::SPACE);

    }; break;

      // 6/11/98: ascii -> unicode
    case CONV_UTF8_F_UCS2_V:
    case CONV_ASCII_UNICODE_F:
    case CONV_ASCII_UNICODE_V:
    case CONV_ASCII_TO_ANSI_V_UNICODE: {
      int copyLen, convertedLen;

      const charBuf cBuf((unsigned char *)source, sourceLen, heap);
      NAWcharBuf *wBuf = NULL;

      if (sourceScale == SQLCHARSETCODE_ISO88591)
        wBuf = ISO88591ToUnicode(cBuf, heap, wBuf);
      else {
        int convertedDataLen;

        if (convCharToChar(source, sourceLen, sourceType, sourcePrecision, sourceScale, target, targetLen, targetType,
                           targetPrecision, targetScale, heap, diagsArea, dataConversionErrorFlag, &convertedDataLen,
                           FALSE, TRUE, flags & CONV_ALLOW_INVALID_CODE_VALUE) != ex_expr::EXPR_OK)
          return ex_expr::EXPR_ERROR;
        wBuf = new (heap) NAWcharBuf((NAWchar *)target, convertedDataLen / BYTES_PER_NAWCHAR, heap);
      }

      if (index == CONV_ASCII_TO_ANSI_V_UNICODE) targetLen--;  // excluding the null terminator

      if (wBuf) {
        convertedLen = BYTES_PER_NAWCHAR * (wBuf->getStrLen());

        copyLen = (convertedLen < targetLen) ? convertedLen : targetLen;

        str_cpy_all(target, (char *)(wBuf->data()), copyLen);

        if (index == CONV_ASCII_UNICODE_F && convertedLen < targetLen)
          wc_str_pad((NAWchar *)(&target[copyLen]), (targetLen - copyLen) >> 1);

        if (convertedLen > targetLen) {
          NAWchar *first_nonblank = wc_str_find_first_nonblank(&((wBuf->data())[targetLen / BYTES_PER_NAWCHAR]),
                                                               (convertedLen - targetLen) / BYTES_PER_NAWCHAR);

          if (first_nonblank)  // if truncated portion isn't all blanks
          {
            if (dataConversionErrorFlag)  // want conversion flag instead of warning?
            {
              // yes - determine whether target is greater than or less than source
              // note assumption: UNICODE character set
              if (*first_nonblank < unicode_char_set::space_char())
                *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP;
              else
                *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN;
            } else
              // no - raise a warning
              ExRaiseSqlWarning(heap, diagsArea, EXE_STRING_OVERFLOW);
          }
        }

        NADELETE(wBuf, NAWcharBuf, heap);

      } else {
        convertedLen = 0;
        copyLen = 0;
      }

      //      There is no need to change the char string storage format
      //      for Unicode string.

      if (varCharLen && index != CONV_ASCII_TO_ANSI_V_UNICODE) setVCLength(varCharLen, varCharLenSize, copyLen);

      if (index == CONV_ASCII_TO_ANSI_V_UNICODE) {
        ((NAWchar *)target)[copyLen / BYTES_PER_NAWCHAR] = 0;
      }
    }; break;

      // 12/8/97: Unicode -> ASCII
    case CONV_UNICODE_F_ASCII_F:          // unicode -> ascii
    case CONV_UNICODE_V_ASCII_F:          // unicode -> ascii
    case CONV_ANSI_V_UNICODE_TO_ASCII_F:  // ansi unicode -> ascii

    {
      if (targetScale == (int)SQLCHARSETCODE_UTF8 || targetScale == (int)SQLCHARSETCODE_SJIS) {
        return unicodeToMByteTarget((targetScale == (int)SQLCHARSETCODE_UTF8) ? unicodeToUtf8 : unicodeToSjis, source,
                                    sourceLen, target, targetLen, targetPrecision, targetScale, 0 /*varCharLen*/,
                                    0 /*varCharLenSize*/, heap, diagsArea, dataConversionErrorFlag,
                                    flags & CONV_ALLOW_INVALID_CODE_VALUE, TRUE /* do pad */,
                                    flags & CONV_PAD_USE_ZERO ? '\0' : ' ');
      } else
        return unicodeToSByteTarget(unicodeToISO88591, source, sourceLen, target, targetLen, 0, 0, heap, diagsArea,
                                    dataConversionErrorFlag, flags & CONV_ALLOW_INVALID_CODE_VALUE, TRUE /* do pad */,
                                    flags & CONV_PAD_USE_ZERO ? '\0' : ' ');
    }; break;

    case CONV_UNICODE_F_ANSI_V:  // unicode F -> ansi_v
    case CONV_UNICODE_V_ANSI_V:  // unicode V -> ansi_v
    {
      int tempTargetLen = sourceLen;
      char *tempTarget = new (heap) char[tempTargetLen];

      ex_expr::exp_return_type ok =
          convDoIt(source, sourceLen, sourceType, sourcePrecision, sourceScale, tempTarget, tempTargetLen,
                   REC_BYTE_F_ASCII, targetPrecision, targetScale,
                   varCharLen,      // NULL if not a varChar
                   varCharLenSize,  // 0 if not a varChar
                   heap, diagsArea, (index == CONV_UNICODE_F_ANSI_V) ? CONV_UNICODE_F_ASCII_F : CONV_UNICODE_V_ASCII_F,
                   dataConversionErrorFlag, tempFlags | CONV_INTERMEDIATE_CONVERSION);

      if (ok == ex_expr::EXPR_OK) {
        convDoIt(tempTarget,
                 tempTargetLen / BYTES_PER_NAWCHAR,  // the number of bytes in the tempTarget buffer
                 REC_BYTE_F_ASCII,                   // is half that of the original Unicode version.
                 targetPrecision,                    // we already converted to the target precision
                 targetScale,                        // and scale
                 target, targetLen, targetType, targetPrecision, targetScale,
                 varCharLen,      // NULL if not a varChar
                 varCharLenSize,  // 0 if not a varChar
                 heap, diagsArea, CONV_ASCII_TO_ANSI_V, dataConversionErrorFlag,
                 tempFlags | CONV_INTERMEDIATE_CONVERSION);
      }

      NADELETEBASIC(tempTarget, heap);
    }

    break;

    case CONV_ANSI_V_UNICODE_TO_ASCII_V:  // ansi unicode (null terminalted)-> ascii
    {
      NAWchar *w_source = (NAWchar *)source;
      int w_sourceLen = sourceLen / BYTES_PER_NAWCHAR;

      int i = 0;
      while ((i < w_sourceLen) && (w_source[i] != 0)) i++;

      if (i == w_sourceLen) {
        ExRaiseSqlError(heap, diagsArea, EXE_MISSING_NULL_TERMINATOR);
        return ex_expr::EXPR_ERROR;
      };

      sourceLen = i * BYTES_PER_NAWCHAR;
    }

      // fall through

      // 6/11/98: Unicode -> ASCII
    case CONV_UCS2_F_UTF8_V:

    case CONV_UNICODE_F_ASCII_V:  // unicode -> ascii
    case CONV_UNICODE_V_ASCII_V:  // unicode -> ascii
    {
      if (targetScale == (int)SQLCHARSETCODE_UTF8 || targetScale == (int)SQLCHARSETCODE_SJIS) {
        return unicodeToMByteTarget((targetScale == (int)SQLCHARSETCODE_UTF8) ? unicodeToUtf8 : unicodeToSjis, source,
                                    sourceLen, target, targetLen, targetPrecision, targetScale, varCharLen,
                                    varCharLenSize, heap, diagsArea, dataConversionErrorFlag,
                                    flags & CONV_ALLOW_INVALID_CODE_VALUE, FALSE);
      } else
        return unicodeToSByteTarget(unicodeToISO88591, source, sourceLen, target, targetLen, varCharLen, varCharLenSize,
                                    heap, diagsArea, dataConversionErrorFlag, flags & CONV_ALLOW_INVALID_CODE_VALUE);
    }; break;

      // 5/14/98: Unicode -> SJIS (V)
    case CONV_UNICODE_F_SJIS_V:  // unicode ->  sjis
    case CONV_UNICODE_V_SJIS_V:  // unicode ->  sjis
    {
      return unicodeToMByteTarget(unicodeToSjis, source, sourceLen, target, targetLen, targetPrecision, targetScale,
                                  varCharLen, varCharLenSize, heap, diagsArea, dataConversionErrorFlag,
                                  flags & CONV_ALLOW_INVALID_CODE_VALUE);
    }; break;

      // UR2. Revisit in CharSet project. This case may not be valid anymore.
    case CONV_UNICODE_F_SBYTE_LOCALE_F:  // unicode -> single-byte locale
    case CONV_UNICODE_V_SBYTE_LOCALE_F:  // unicode -> single-byte locale
    {
      if (targetScale == (int)SQLCHARSETCODE_UTF8 || targetScale == (int)SQLCHARSETCODE_SJIS) {
        return unicodeToMByteTarget((targetScale == (int)SQLCHARSETCODE_UTF8) ? unicodeToUtf8 : unicodeToSjis, source,
                                    sourceLen, target, targetLen, targetPrecision, targetScale, 0 /*varCharLen*/,
                                    0 /*varCharLenSize*/, heap, diagsArea, dataConversionErrorFlag,
                                    flags & CONV_ALLOW_INVALID_CODE_VALUE, TRUE /* do pad */,
                                    flags & CONV_PAD_USE_ZERO ? '\0' : ' ');
      } else
        return unicodeToSByteTarget(unicodeToISO88591, source, sourceLen, target, targetLen, 0, 0, heap, diagsArea,
                                    dataConversionErrorFlag, flags & CONV_ALLOW_INVALID_CODE_VALUE, TRUE /* do pad */,
                                    flags & CONV_PAD_USE_ZERO ? '\0' : ' ');
    }; break;

      // 5/10/98: Unicode -> SJIS (F)
    case CONV_UNICODE_F_SJIS_F:  // unicode ->  sjis
    case CONV_UNICODE_V_SJIS_F:  // unicode ->  sjis
    {
      return unicodeToMByteTarget(unicodeToSjis, source, sourceLen, target, targetLen, targetPrecision, targetScale, 0,
                                  0, heap, diagsArea, dataConversionErrorFlag, flags & CONV_ALLOW_INVALID_CODE_VALUE,
                                  TRUE /* do pad */, flags & CONV_PAD_USE_ZERO ? '\0' : ' ');
    }; break;

      // UR2. Revisit in CharSet project. This case may not be valid anymore.
    case CONV_UNICODE_F_MBYTE_LOCALE_F:  // unicode -> multi-byte locale
    case CONV_UNICODE_V_MBYTE_LOCALE_F:  // unicode -> multi-byte locale
    {
      if (targetScale == (int)SQLCHARSETCODE_UTF8 || targetScale == (int)SQLCHARSETCODE_SJIS) {
        return unicodeToMByteTarget((targetScale == (int)SQLCHARSETCODE_UTF8) ? unicodeToUtf8 : unicodeToSjis, source,
                                    sourceLen, target, targetLen, targetPrecision, targetScale, varCharLen,
                                    varCharLenSize, heap, diagsArea, dataConversionErrorFlag,
                                    flags & CONV_ALLOW_INVALID_CODE_VALUE, TRUE /* do pad */,
                                    flags & CONV_PAD_USE_ZERO ? '\0' : ' ');
      } else
        return unicodeToMByteTarget(unicodeToISO88591, source, sourceLen, target, targetLen, targetPrecision,
                                    targetScale, varCharLen, varCharLenSize, heap, diagsArea, dataConversionErrorFlag,
                                    flags & CONV_ALLOW_INVALID_CODE_VALUE, TRUE /* do pad */,
                                    flags & CONV_PAD_USE_ZERO ? '\0' : ' ');
    }; break;

    case CONV_ANSI_V_UNICODE_TO_UNICODE_V:  // ANSI VARNCHAR -> unicode (V)
    {
      NAWchar *w_source = (NAWchar *)source;
      int w_sourceLen = sourceLen / BYTES_PER_NAWCHAR;

      int i = 0;
      while ((i < w_sourceLen) && (w_source[i] != 0)) i++;

      if (i == w_sourceLen) {
        ExRaiseSqlError(heap, diagsArea, EXE_MISSING_NULL_TERMINATOR);
        return ex_expr::EXPR_ERROR;
      };

      sourceLen = i * BYTES_PER_NAWCHAR;

      return convDoIt(source, sourceLen, sourceType, sourcePrecision, sourceScale, target, targetLen, targetType,
                      targetPrecision, targetScale, varCharLen, varCharLenSize, heap, diagsArea,
                      CONV_UNICODE_V_V,  // unicode (V) -> unicode (V)
                      dataConversionErrorFlag, tempFlags);
    }; break;

    case CONV_UNICODE_F_V:  // unicode (F) -> unicode (V)
    case CONV_UNICODE_V_V:  // unicode (V) -> unicode (V)
    {
      int copyLen = ((targetLen >= sourceLen) ? sourceLen : targetLen);

      str_cpy_all(target, source, copyLen);

      //      There is no need to change the char string storage format
      //      for Unicode string.
      setVCLength(varCharLen, varCharLenSize, copyLen);

      if (targetLen < sourceLen) {
        NAWchar *first_nonblank =
            wc_str_find_first_nonblank((NAWchar *)(&source[targetLen]), (sourceLen - targetLen) >> 1);

        if (first_nonblank)  // if truncated portion isn't all blanks
        {
          if (dataConversionErrorFlag)  // want conversion flag instead of warning?
          {
            // yes - determine whether target is greater than or less than source
            if (*first_nonblank < unicode_char_set::space_char())
              // note assumption: UNICODE character set
              *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP;
            else
              *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN;
          } else
            // no - raise a warning
            ExRaiseSqlWarning(heap, diagsArea, EXE_STRING_OVERFLOW);
        }
      }
    }; break;

      // 6/26/98: ANSI VARNCHAR support
    case CONV_UNICODE_TO_ANSI_V_UNICODE:         // unicode -> ANSI VARNCHAR
    case CONV_ANSI_V_UNICODE_TO_ANSI_V_UNICODE:  // ANSI VARNCHAR-> ANSI VARNCHAR
    {
      if (index == CONV_ANSI_V_UNICODE_TO_ANSI_V_UNICODE) {
        NAWchar *w_source = (NAWchar *)source;
        int w_sourceLen = sourceLen / BYTES_PER_NAWCHAR;

        int i = 0;
        while ((i < w_sourceLen) && (w_source[i] != 0)) i++;

        if (i == w_sourceLen) {
          ExRaiseSqlError(heap, diagsArea, EXE_MISSING_NULL_TERMINATOR);
          return ex_expr::EXPR_ERROR;
        };

        sourceLen = BYTES_PER_NAWCHAR * i;
      }

      int localVarCharLen = 0;

      ex_expr::exp_return_type ok = convDoIt(source, sourceLen, sourceType, sourcePrecision, sourceScale, target,
                                             targetLen - 2,  // exclude the null terminator (two bytes)
                                             targetType, targetPrecision, targetScale, (char *)&localVarCharLen,
                                             sizeof(localVarCharLen), heap, diagsArea,
                                             CONV_UNICODE_V_V,  // unicode (V) -> unicode (V)
                                             dataConversionErrorFlag, tempFlags);

      if (ok == ex_expr::EXPR_OK) ((NAWchar *)target)[localVarCharLen / BYTES_PER_NAWCHAR] = 0;

      return ok;
    }; break;

    case CONV_ANSI_V_TO_ASCII_F: {
      // find out the number of characters in the source string.
      // Source string is terminated by a null character.
      int i = 0;
      while ((i < sourceLen) && (source[i] != 0)) i++;

      if (i == sourceLen) {
        ExRaiseSqlError(heap, diagsArea, EXE_MISSING_NULL_TERMINATOR);
        return ex_expr::EXPR_ERROR;
      };
      // disregard the null terminator
      sourceLen = i;
    };
    // fall thru
    case CONV_ASCII_F_F:
    case CONV_ASCII_V_F: {
      char padVal = (flags & CONV_PAD_USE_ZERO) ? '\0' : ' ';

      // check for needed character conversions or validations
      if (requiresNoConvOrVal(sourceLen, sourcePrecision, sourceScale, targetLen, targetPrecision, targetScale,
                              index)) {
        int copyLen = ((targetLen >= sourceLen) ? sourceLen : targetLen);

        // this case can often be translated into PCODE
        if (targetLen > copyLen) {
          if (leftPad) {
            str_pad(target, targetLen - copyLen, padVal);
            str_cpy_all(&target[targetLen - copyLen], source, copyLen);
          } else {
            str_cpy_all(target, source, copyLen);
            /* blank pad target */
            str_pad(&target[copyLen], targetLen - copyLen, padVal);
          }
        } else  // targetLen <= copyLen
        {
          str_cpy_all(target, source, copyLen);
          if (copyLen < sourceLen) {
            char *first_nonblank = str_find_first_nonpad(&source[copyLen], sourceLen - copyLen, padVal);

            if (first_nonblank)  // if truncated portion isn't all blanks
            {
              if (dataConversionErrorFlag)  // want conversion flag instead of warning?
              {
                // yes - determine whether target is greater than or less than source
                if (*(unsigned char *)first_nonblank < padVal)  // note assumption: charset uses ASCII blank
                  *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP;
                else
                  *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN;
              } else
                // no - raise a warning
                ExRaiseSqlWarning(heap, diagsArea, EXE_STRING_OVERFLOW);
            }
          }
        }
      } else {
        // this case isn't translated into PCODE at this time
        if (convCharToChar(source, sourceLen, sourceType, sourcePrecision, sourceScale, target, targetLen, targetType,
                           targetPrecision, targetScale, heap, diagsArea, dataConversionErrorFlag, NULL, TRUE, TRUE,
                           flags & CONV_ALLOW_INVALID_CODE_VALUE, padVal) != ex_expr::EXPR_OK)
          return ex_expr::EXPR_ERROR;
      }
    } break;

    case CONV_ANSI_V_UNICODE_TO_UNICODE_F: {
      // calcualte the sourceLen by searching for the NULL
      NAWchar *w_source = (NAWchar *)source;
      int w_sourceLen = sourceLen / BYTES_PER_NAWCHAR;

      int i = 0;
      while ((i < w_sourceLen) && (w_source[i] != 0)) i++;

      if (i == w_sourceLen) {
        ExRaiseSqlError(heap, diagsArea, EXE_MISSING_NULL_TERMINATOR);
        return ex_expr::EXPR_ERROR;
      };
      sourceLen = BYTES_PER_NAWCHAR * i;
    }
      // fall through

      // 12/8/97: added for Unicode
    case CONV_UNICODE_F_F:
    case CONV_UNICODE_V_F: {
      if (targetLen > sourceLen) {
        str_cpy_all(target, source, sourceLen);
        /* blank pad target */
        wc_str_pad((NAWchar *)(&target[sourceLen]), (targetLen - sourceLen) >> 1,
                   flags & CONV_PAD_USE_ZERO ? unicode_char_set::ASCIINULL : unicode_char_set::SPACE);
      } else {
        str_cpy_all(target, source, targetLen);
        if (targetLen < sourceLen) {
          NAWchar *first_nonblank =
              wc_str_find_first_nonblank((NAWchar *)(&source[targetLen]), (sourceLen - targetLen) >> 1);

          if (first_nonblank)  // if truncated portion isn't all blanks
          {
            if (dataConversionErrorFlag)  // want conversion flag instead of warning?
            {
              // yes - determine whether target is greater than or less than source
              // note assumption: UNICODE character set
              if (*first_nonblank < unicode_char_set::space_char())
                *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_UP;
              else
                *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN;
            } else
              // no - raise a warning
              ExRaiseSqlWarning(heap, diagsArea, EXE_STRING_OVERFLOW);
          }
        }
      };
    }; break;

    case CONV_ASCII_TO_ANSI_V: {
      // check for needed character conversions or validations
      if (requiresNoConvOrVal(sourceLen, sourcePrecision, sourceScale, targetLen, targetPrecision, targetScale,
                              index)) {
        // max target length includes one byte (the end byte) for null
        // terminator.
        targetLen--;

        if (targetLen >= sourceLen) {
          str_cpy_all(target, source, sourceLen);

          // put null character at the end
          target[sourceLen] = 0;
        } else {
          str_cpy_all(target, source, targetLen);
          // put null character at the end
          target[targetLen] = 0;
          if ((targetLen < sourceLen) && str_find_first_nonpad(&source[targetLen], sourceLen - targetLen, ' '))
            ExRaiseSqlWarning(heap, diagsArea, EXE_STRING_OVERFLOW);
        }
      } else {
        int convertedDataLen;

        if (convCharToChar(source, sourceLen, sourceType, sourcePrecision, sourceScale, target, targetLen, targetType,
                           targetPrecision, targetScale, heap, diagsArea, dataConversionErrorFlag, &convertedDataLen,
                           FALSE, TRUE, flags & CONV_ALLOW_INVALID_CODE_VALUE) != ex_expr::EXPR_OK)
          return ex_expr::EXPR_ERROR;
        // put null character at the end
        target[convertedDataLen] = 0;
      }
    }; break;

    case CONV_ANSI_V_TO_ANSI_V:
      // max source length includes one byte (the end byte) for null
      // terminator.
      // max target length includes one byte (the end byte) for null
      // terminator.
      targetLen--;

      // fall through to next

    case CONV_ANSI_V_TO_ASCII_V: {
      // max source length includes one byte (the end byte) for null
      // terminator.

      // find out the number of characters in the source string.
      // Source string is terminated by a null character.
      int actSourceLen = 0;
      while ((actSourceLen < sourceLen) && (source[actSourceLen] != 0)) actSourceLen++;

      if (actSourceLen == sourceLen) {
        ExRaiseSqlError(heap, diagsArea, EXE_MISSING_NULL_TERMINATOR);
        return ex_expr::EXPR_ERROR;
      };

      // check for needed character conversions or validations
      if (requiresNoConvOrVal(actSourceLen, sourcePrecision, sourceScale, targetLen, targetPrecision, targetScale,
                              index)) {
        if (targetLen >= actSourceLen) {
          str_cpy_all(target, source, actSourceLen);
          if (index == CONV_ANSI_V_TO_ASCII_V) {
            setVCLength(varCharLen, varCharLenSize, actSourceLen);
          } else {
            target[actSourceLen] = 0;
          }
          setVCLength(varCharLen, varCharLenSize, actSourceLen);
        } else {
          str_cpy_all(target, source, targetLen);

          if (index == CONV_ANSI_V_TO_ASCII_V) {
            setVCLength(varCharLen, varCharLenSize, targetLen);
          } else {
            target[targetLen] = 0;
          }
          if (str_find_first_nonpad(&source[targetLen], actSourceLen - targetLen, ' '))
            ExRaiseSqlWarning(heap, diagsArea, EXE_STRING_OVERFLOW);
        }
      } else {
        int convertedDataLen;

        if (convCharToChar(source, actSourceLen, sourceType, sourcePrecision, sourceScale, target, targetLen,
                           targetType, targetPrecision, targetScale, heap, diagsArea, dataConversionErrorFlag,
                           &convertedDataLen, FALSE, TRUE, flags & CONV_ALLOW_INVALID_CODE_VALUE) != ex_expr::EXPR_OK)
          return ex_expr::EXPR_ERROR;
        if (index == CONV_ANSI_V_TO_ASCII_V) {
          setVCLength(varCharLen, varCharLenSize, convertedDataLen);
        } else {
          target[convertedDataLen] = 0;
        }
      }
    }; break;

    case CONV_BIN64S_DATETIME: {
      NABoolean LastDayFlag = FALSE;
      // numeric to date conversion uses the formula:
      // dt = (year - 1900) * 10000 + month * 100 + day
      long nVal = *(long *)source;
      long year = (nVal / 10000);
      long month = (nVal - (year * 10000)) / 100;
      long day = (nVal - (year * 10000) - (month * 100));
      year = year + 1900;

      if ((year > SHRT_MAX) || (month > SCHAR_MAX) || (day > SCHAR_MAX)) {
        ExRaiseSqlError(heap, diagsArea, EXE_DATETIME_FIELD_OVERFLOW);
        return ex_expr::EXPR_ERROR;
      }

      // date is of the form:  YYMD
      *(short *)target = (short)year;
      target[2] = (char)month;
      target[3] = (char)day;

      // validate the date
      if (ExpDatetime::validateDate(REC_DATE_YEAR, REC_DATE_DAY, target, NULL, FALSE, LastDayFlag) != 0) {
        ExRaiseSqlError(heap, diagsArea, EXE_DATETIME_FIELD_OVERFLOW);
        return ex_expr::EXPR_ERROR;
      }
    } break;

    case CONV_DATETIME_BIN64S: {
      // date to numeric conversion uses the formula:
      // numeric = (year - 1900) * 10000 + month * 100 + day
      int year = *(short *)source;
      int month = source[2];
      int day = source[3];

      *(long *)target = (year - 1900) * 10000 + month * 100 + day;
    } break;

    case CONV_BLOB_BLOB: {
      str_cpy_all(target, source, sourceLen);
      setVCLength(varCharLen, varCharLenSize, sourceLen);
    } break;

    case CONV_BLOB_ASCII_F: {
      // conversion from internal format blob handle to external format
      // By the time it reaches here, the source is already in the right
      // format, either internal or external.
      // It can be just copied to the target.
      str_pad(target, targetLen, ' ');
      str_cpy_all(target, source, sourceLen);
    } break;

    case CONV_BOOL_BOOL: {
      if ((*(Int8 *)source == 1) || (*(Int8 *)source == 0))
        *(Int8 *)target = *(Int8 *)source;
      else {
        char tempBuf[10];
        str_itoa(*(Int8 *)source, tempBuf);
        ExRaiseSqlError(heap, diagsArea, EXE_INVALID_BOOLEAN_VALUE, NULL, NULL, NULL, NULL, tempBuf);
        return ex_expr::EXPR_ERROR;
      }
    } break;

    case CONV_BOOL_ASCII: {
      char boolbuf[10];
      if (*(Int8 *)source == 1)
        strcpy(boolbuf, "TRUE");
      else if (*(Int8 *)source == 0)
        strcpy(boolbuf, "FALSE");
      else {
        char tempBuf[10];
        str_itoa(*(Int8 *)source, tempBuf);
        ExRaiseSqlError(heap, diagsArea, EXE_INVALID_BOOLEAN_VALUE, NULL, NULL, NULL, NULL, tempBuf);
        return ex_expr::EXPR_ERROR;
      }

      // this case isn't translated into PCODE at this time
      int convertedDataLen;

      if (convCharToChar(boolbuf, strlen(boolbuf), REC_BYTE_F_ASCII, 0, SQLCHARSETCODE_ISO88591, target, targetLen,
                         targetType, targetPrecision, targetScale, heap, diagsArea, NULL, &convertedDataLen,
                         ((varCharLenSize > 0) ? FALSE : TRUE), TRUE, FALSE) != ex_expr::EXPR_OK)
        return ex_expr::EXPR_ERROR;
      if (varCharLenSize > 0) setVCLength(varCharLen, varCharLenSize, convertedDataLen);
    } break;

    case CONV_ASCII_BOOL: {
      char srcTempBuf[sourceLen + 1];
      str_cpy_convert(srcTempBuf, source, sourceLen, 1);
      srcTempBuf[sourceLen] = 0;
      int tempLen;
      char *srcTempPtr = srcTempBuf;
      srcTempPtr = str_strip_blanks(srcTempBuf, tempLen, TRUE, TRUE);

      if ((strcmp(srcTempPtr, "TRUE") == 0) || (strcmp(srcTempPtr, "1") == 0))
        *(Int8 *)target = 1;
      else if ((strcmp(srcTempPtr, "FALSE") == 0) || (strcmp(srcTempPtr, "0") == 0))
        *(Int8 *)target = 0;
      else {
        char srcErrBuf[sourceLen + 1 + 2 /*for quotes*/];
        strcpy(srcErrBuf, "'");
        str_cpy_all(&srcErrBuf[1], source, sourceLen);
        srcErrBuf[1 + sourceLen] = 0;
        strcat(srcErrBuf, "'");
        ExRaiseSqlError(heap, diagsArea, EXE_INVALID_BOOLEAN_VALUE, NULL, NULL, NULL, NULL, srcErrBuf);
        return ex_expr::EXPR_ERROR;
      }
    } break;

    case CONV_ARRAY_ARRAY: {
      int copyLen = ((targetLen >= sourceLen) ? sourceLen : targetLen);
      str_cpy_all(target, source, copyLen);

      if (varCharLen) setVCLength(varCharLen, varCharLenSize, copyLen);
    } break;

    case CONV_ROW_ROW: {
      int copyLen = ((targetLen >= sourceLen) ? sourceLen : targetLen);
      str_cpy_all(target, source, copyLen);

      if (varCharLen) setVCLength(varCharLen, varCharLenSize, copyLen);
    } break;

    case CONV_BINARY_TO_BINARY:
    case CONV_VARBINARY_TO_BINARY: {
      int copyLen = ((targetLen >= sourceLen) ? sourceLen : targetLen);
      str_cpy_all(target, source, copyLen);

      if (targetLen > copyLen) {
        /* zero pad target */
        str_pad(&target[copyLen], targetLen - copyLen, '\0');
      } else if (copyLen < sourceLen) {
        char *first_nonzero = str_find_first_nonpad(&source[copyLen], sourceLen - copyLen, '\0');
        if (first_nonzero) {
          if (dataConversionErrorFlag)  // want conversion flag instead of warning?
            *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN;
          else
            // no - raise a warning
            ExRaiseDetailSqlError(heap, diagsArea, (ExeErrorCode)(-EXE_CONVERSION_ERROR), source, sourceLen, sourceType,
                                  sourceScale, targetType, 0, targetLen, targetScale, targetPrecision, sourcePrecision);
        }
      }
    } break;

    case CONV_BINARY_TO_VARBINARY:
    case CONV_VARBINARY_TO_VARBINARY: {
      int copyLen = ((targetLen >= sourceLen) ? sourceLen : targetLen);
      str_cpy_all(target, source, copyLen);
      setVCLength(varCharLen, varCharLenSize, copyLen);

      if (copyLen < sourceLen) {
        char *first_nonzero = str_find_first_nonpad(&source[copyLen], sourceLen - copyLen, '\0');
        if (first_nonzero) {
          if (dataConversionErrorFlag)
            *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN;
          else
            // no - raise a warning
            ExRaiseDetailSqlError(heap, diagsArea, (ExeErrorCode)(-EXE_CONVERSION_ERROR), source, sourceLen, sourceType,
                                  sourceScale, targetType, 0, targetLen, targetScale, targetPrecision, sourcePrecision);
        }
      }
    } break;

    case CONV_OTHER_TO_BINARY: {
      int copyLen = ((targetLen >= sourceLen) ? sourceLen : targetLen);
      str_cpy_all(target, source, copyLen);

      if (targetLen > copyLen) {
        /* zero pad target */
        str_pad(&target[copyLen], targetLen - copyLen, '\0');
      } else if (copyLen < sourceLen) {
        if (dataConversionErrorFlag)
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN;
        else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_CONVERSION_ERROR, source, sourceLen, sourceType, sourceScale,
                                targetType, 0, targetLen, targetScale, targetPrecision, sourcePrecision);
          return ex_expr::EXPR_ERROR;
        }
      }
    } break;

    case CONV_OTHER_TO_VARBINARY: {
      int copyLen = ((targetLen >= sourceLen) ? sourceLen : targetLen);
      str_cpy_all(target, source, copyLen);
      setVCLength(varCharLen, varCharLenSize, copyLen);

      if (copyLen < sourceLen) {
        if (dataConversionErrorFlag)
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN;
        else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_CONVERSION_ERROR, source, sourceLen, sourceType, sourceScale,
                                targetType, 0, targetLen, targetScale, targetPrecision, sourcePrecision);
          return ex_expr::EXPR_ERROR;
        }
      }
    } break;

    case CONV_BINARY_TO_OTHER:
    case CONV_VARBINARY_TO_OTHER: {
      int copyLen = ((targetLen >= sourceLen) ? sourceLen : targetLen);
      str_cpy_all(target, source, copyLen);

      if (varCharLen && (varCharLenSize > 0)) setVCLength(varCharLen, varCharLenSize, copyLen);

      // if source len is > target len, return error. There is no truncation.
      if (sourceLen > targetLen) {
        if (dataConversionErrorFlag)
          *dataConversionErrorFlag = ex_conv_clause::CONV_RESULT_ROUNDED_DOWN;
        else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_CONVERSION_ERROR, source, sourceLen, sourceType, sourceScale,
                                targetType, 0, targetLen, targetScale, targetPrecision, sourcePrecision);
          return ex_expr::EXPR_ERROR;
        }
      }
      // if target len > source len and target datatype is string, blankpad.
      // Otherwise return error.
      else if (targetLen > sourceLen) {
        if (DFS2REC::isCharacterString(targetType)) {
          char padChar = 0;
          if (DFS2REC::isCharacterString(targetType)) padChar = ' ';

          str_pad(target + sourceLen, targetLen - sourceLen, padChar);
        } else {
          ExRaiseDetailSqlError(heap, diagsArea, EXE_CONVERSION_ERROR, source, sourceLen, sourceType, sourceScale,
                                targetType, 0, targetLen, targetScale, targetPrecision, sourcePrecision);
          return ex_expr::EXPR_ERROR;
        }
      }
    } break;

    case CONV_NOT_SUPPORTED:
    default: {
      ExRaiseDetailSqlError(heap, diagsArea, EXE_CONVERT_NOT_SUPPORTED, source, sourceLen, sourceType, sourceScale,
                            targetType, flags, targetLen, targetScale);

      // this conversion is not supported.
      return ex_expr::EXPR_ERROR;
    } break;
  };
  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ex_conv_clause::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  Attributes *tgt = getOperand(0);
  Attributes *src = getOperand(1);
  ex_expr::exp_return_type retcode = ex_expr::EXPR_OK;
  int warningMark = 0;

  if (lastVOAoffset_ > 0) {  // case of cif bulk move

    char *srcData = op_data[1];
    char *tgtData = op_data[0];

    UInt32 copyLength = 0;

    if (*(int *)srcData == -1)  // if aliggned format header is 0xFFFFFFFF
    {                           // case of null instantiated row
      assert(*(Int16 *)&srcData[lastVOAoffset_] == -1);
      copyLength = ((SimpleType *)src)->getLength();
    } else {
      copyLength = ExpTupleDesc::getVarOffset(srcData,
                                              UINT_MAX,        // offset
                                              lastVOAoffset_,  // voaEntryOffset
                                              lastVcIndicatorLength_, lastNullIndicatorLength_);
      copyLength += ExpTupleDesc::getVarLength(srcData + copyLength, lastVcIndicatorLength_);
      copyLength += lastVcIndicatorLength_;
    }
    assert(copyLength > 0 && copyLength <= (UInt32)((SimpleType *)src)->getLength());

#if defined(_DEBUG)
    char *envCifLoggingLocation = getenv("CIF_BM_LOG_LOC");
    //
    if (envCifLoggingLocation) {
      char file2[255];
      snprintf(file2, 255, "%s/bm_logging.log", envCifLoggingLocation);
      FILE *p2 = NULL;
      p2 = fopen(file2, "a");
      if (p2 == NULL) {
        printf("Error in opening file %s\n", file2);
      }
      if (copyLength <= 0 || copyLength >= (UInt32)((SimpleType *)src)->getLength()) {
        fprintf(p2, "%s%d\t%s%d\n", "copylength:", copyLength, "maxLength:", ((SimpleType *)src)->getLength());
      }

      fclose(p2);
    }
#endif

    UInt32 newLen = copyLength;

    str_cpy_all(tgtData, srcData, copyLength);

    if (alignment_ > 0) {
      newLen = ExpAlignedFormat::adjustDataLength(tgtData, copyLength, alignment_, FALSE);
    }
    computedLength_ = newLen;

    return ex_expr::EXPR_OK;
  }

  if (getCheckTruncationFlag() || getNoTruncationWarningsFlag())
    if (*diagsArea)
      warningMark = (*diagsArea)->getNumber(DgSqlCode::WARNING_);
    else
      warningMark = 0;

  switch (getInstruction()) {
    case CONV_COMPLEX_TO_COMPLEX: {
      if (((ComplexType *)tgt)->conv(src, op_data) != 0) {
        ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, this, op_data);
        return ex_expr::EXPR_ERROR;
      }
    }; break;

    case CONV_SIMPLE_TO_COMPLEX: {
      if (((ComplexType *)tgt)->castFrom(src, op_data, heap, diagsArea) != 0) {
        ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, this, op_data);
        return ex_expr::EXPR_ERROR;
      }
    }; break;

    case CONV_COMPLEX_TO_SIMPLE: {
      if (((ComplexType *)src)->castTo(tgt, op_data, heap, diagsArea) != 0) {
        ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, this, op_data);
        return ex_expr::EXPR_ERROR;
      }
    }; break;

    default: {
      int *dataConversionErrorFlag = 0;
      if (getNumOperands() == 3) {
        dataConversionErrorFlag = (int *)op_data[2];
      }

      int convFlags = 0;
      if (treatAllSpacesAsZero()) convFlags |= CONV_TREAT_ALL_SPACES_AS_ZERO;

      if (allowSignInInterval()) convFlags |= CONV_ALLOW_SIGN_IN_INTERVAL;

      if (noDatetimeValidation()) convFlags |= CONV_NO_DATETIME_VALIDATION;

      if (padUseZero()) convFlags |= CONV_PAD_USE_ZERO;

      char *source = op_data[1];
      if (srcIsVarcharPtr()) {
        long ptrVal = *(long *)op_data[1];
        source = (char *)ptrVal;
      }

      retcode = convDoIt(source, src->getLength(op_data[-MAX_OPERANDS + 1]), src->getDatatype(), src->getPrecision(),
                         src->getScale(), op_data[0], tgt->getLength(), tgt->getDatatype(), tgt->getPrecision(),
                         tgt->getScale(), op_data[-MAX_OPERANDS], tgt->getVCIndicatorLength(), heap, diagsArea,
                         getInstruction(), dataConversionErrorFlag, convFlags);

      if ((flags_ & REVERSE_DATA_ERROR_CONVERSION_FLAG) != 0) {
        if (dataConversionErrorFlag && (*dataConversionErrorFlag >= -2 && *dataConversionErrorFlag <= 2)) {
          *dataConversionErrorFlag = -(*dataConversionErrorFlag);
        }
      }

      if (retcode != ex_expr::EXPR_OK && (flags_ & CONV_TO_NULL_WHEN_ERROR) != 0) {
        if (*diagsArea) {
          (*diagsArea)->clearErrorConditionsOnly();

          // deallocate if no more entries in diagsArea
          if ((*diagsArea)->mainSQLCODE() == 0) {
            (*diagsArea)->deAllocate();

            *diagsArea = NULL;
          }
        }

        // move null to target
        if (tgt->getNullFlag()) {
          // clear diags area since this error is not being returned
          if (diagsArea && *diagsArea && ((*diagsArea)->getNumber(DgSqlCode::ERROR_) > 0)) {
            (*diagsArea)->clear();
          }

          ExpTupleDesc::setNullValue(op_data[-2 * MAX_OPERANDS], tgt->getNullBitIndex(), tgt->getTupleFormat());
          retcode = ex_expr::EXPR_OK;
        }
      }
    };
  };  // switch

  // If this conv clause came from either an update or an insert, and if
  // the data types are char types and the target size is smaller than source
  // size, the checkTruncError flag should be set.
  // In this section, if the flag is set, and a warning is returned as the
  // result of the conversion, an error should be issued instead of a warning.
  // This is according to ANSI SQL92

  if (getCheckTruncationFlag() && *diagsArea) {
    int warningMark2 = (*diagsArea)->getNumber(DgSqlCode::WARNING_);
    int counter = warningMark2 - warningMark;
    while (counter) {
      if (((*diagsArea)->getWarningEntry(warningMark2 - counter + 1))->getSQLCODE() == EXE_STRING_OVERFLOW) {
        (*diagsArea)->deleteWarning(warningMark2 - counter);
        ExRaiseDetailSqlError(heap, diagsArea, EXE_STRING_OVERFLOW, op_data[1],
                              src->getLength(op_data[-MAX_OPERANDS + 1]), src->getDatatype(), src->getScale(),
                              tgt->getDatatype(), 0, tgt->getLength(), tgt->getScale(), tgt->getPrecision(),
                              src->getPrecision());
        retcode = ex_expr::EXPR_ERROR;
      } else if (((*diagsArea)->getWarningEntry(warningMark2 - counter + 1))->getSQLCODE() == EXE_NUMERIC_OVERFLOW) {
        (*diagsArea)->deleteWarning(warningMark2 - counter);
        ExRaiseDetailSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, op_data[1],
                              src->getLength(op_data[-MAX_OPERANDS + 1]), src->getDatatype(), src->getScale(),
                              tgt->getDatatype(), 0, tgt->getLength(), tgt->getScale(), tgt->getPrecision(),
                              src->getPrecision());
        retcode = ex_expr::EXPR_ERROR;
      }
      counter--;
    }
  }

  if (getNoTruncationWarningsFlag() && *diagsArea) {
    int warningMark2 = (*diagsArea)->getNumber(DgSqlCode::WARNING_);
    int counter = warningMark2 - warningMark;
    while (counter) {
      if (((*diagsArea)->getWarningEntry(warningMark2 - counter + 1))->getSQLCODE() == EXE_STRING_OVERFLOW) {
        (*diagsArea)->deleteWarning(warningMark2 - counter);
      } else if (((*diagsArea)->getWarningEntry(warningMark2 - counter + 1))->getSQLCODE() == EXE_NUMERIC_OVERFLOW) {
        (*diagsArea)->deleteWarning(warningMark2 - counter);
      }
      counter--;
    }
  }

  // add column name to diagsArea
  if (retcode != ex_expr::EXPR_OK && (((*diagsArea)->mainSQLCODE() == -EXE_CONVERT_STRING_ERROR) ||
                                      ((*diagsArea)->mainSQLCODE() == -EXE_NUMERIC_OVERFLOW) ||
                                      ((*diagsArea)->mainSQLCODE() == -EXE_STRING_OVERFLOW) ||
                                      ((*diagsArea)->mainSQLCODE() == -EXE_CONVERT_DATETIME_ERROR) ||
                                      ((*diagsArea)->mainSQLCODE() == -EXE_CONVERSION_ERROR) ||
                                      ((*diagsArea)->mainSQLCODE() == -EXE_INVALID_BOOLEAN_VALUE))) {
    **diagsArea << DgString1(str_len(src->getColName()) ? NAString("Column Name: ") + src->getColName() : "");
  }

  return retcode;
};

ex_expr::exp_return_type ex_conv_clause::processNulls(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  Attributes *tgtAttr = getOperand(0);

  if (getOperand(1)->getNullFlag() &&  // if input operand is nullable
      (!op_data[1]))                   // and it is null
  {
    if (tgtAttr->getNullFlag())  // if result is nullable
    {
      // move null value to result
      char *rslt = op_data[0];

      // For a null value, DP2 expects the rest of the field to be 0,
      // for SQL/MP tables, but we can do it for all.
      if (tgtAttr->getTupleFormat() == ExpTupleDesc::SQLMX_ALIGNED_FORMAT) {
        // For aligned format the null and variable length and value
        // are not in contiguous memory, thus zero out the variable
        // length (if variable field) and value memory separately.
        rslt = ((tgtAttr->getVCIndicatorLength() > 0) ? op_data[ex_clause::MAX_OPERANDS]
                                                      : op_data[2 * ex_clause::MAX_OPERANDS]);
      }

      str_pad(rslt, tgtAttr->getLength() + tgtAttr->getNullIndicatorLength() + tgtAttr->getVCIndicatorLength(), '\0');

      ExpTupleDesc::setNullValue(op_data[0], tgtAttr->getNullBitIndex(), tgtAttr->getTupleFormat());
    } else  // result is not nullable
    {
      // Attempt to put NULL into column with
      // NOT NULL NONDROPPABLE constraint.
      if (getNumOperands() != 3) {
        ExRaiseSqlError(heap, diagsArea, EXE_ASSIGNING_NULL_TO_NOT_NULL);
        return ex_expr::EXPR_ERROR;
      } else  // data conversion error flag is present
      {       // begin fix to CR 10-040430-8520: regress/core/test005 fails in MP
        // after a query caching fix changed NumericType::errorsCanOccur()
        // to return true if an underflow/overflow can occur when doing a
        // cast or a narrow from, say, an IEEE float to a Tandem REAL type.
        // In the CR, "select * from t where e is null" is forced via CQS
        // to use an index for e. t is an MP table. e is type Tandem REAL.
        // The errorsCanOccur() fix causes ExpGenerator::generateKeyCast()
        // to generate a "narrow ieee null constant to Tandem REAL" instead
        // of "cast ieee null constant to Tandem REAL". At runtime, this
        // method interprets "narrow ieee null constant to Tandem REAL"
        // and incorrectly assumes it found a NULL primary key value (a
        // contradiction). The correct behavior is to declare a conversion
        // failure iff the target is not nullable and the source is null.
        // set it to note that a conversion has failed
        int *dataConversionErrorFlag = (int *)op_data[2 * ex_clause::MAX_OPERANDS + 2];
        *dataConversionErrorFlag = CONV_RESULT_FAILED;
        // end fix to CR 10-040430-8520

        // If target is varchar, it's important to clear out the length
        // so that if it gets used (e.g. conversion error is ignored),
        // then a bogus length would not be accessed
        if (tgtAttr->getVCIndicatorLength() > 0) {
          char *rslt = op_data[ex_clause::MAX_OPERANDS];
          str_pad(rslt, tgtAttr->getVCIndicatorLength(), '\0');
        }

        // Make sure the data portion of target is cleared so
        // that if it is used for partitioning, all data
        // conversion error rows will be repartitioned to the
        // same partition.  Note that is some cases the
        // dataCoversionErrorFlag is not properly used.
        char *rslt = op_data[2 * ex_clause::MAX_OPERANDS];
        str_pad(rslt, tgtAttr->getLength(), '\0');
      }
    }

    return ex_expr::EXPR_NULL;  // indicate that a null input was processed
  }

  // first operand is not null -- set null indicator in result if nullable
  if (tgtAttr->getNullFlag()) {
    ExpTupleDesc::clearNullValue(op_data[0], tgtAttr->getNullBitIndex(), tgtAttr->getTupleFormat());
  }

  return ex_expr::EXPR_OK;  // indicate input is not null
};

////////////////////////////////////////////////////////////////////////
// up- or downscale an exact numeric.
////////////////////////////////////////////////////////////////////////
ex_expr::exp_return_type scaleDoIt(char *operand, int operandLen, int operandType, int operandCurrScale, int newScale,
                                   int typeOfOldScale, CollHeap *heap) {
  // return if there is no difference in scale or if "newScale"
  // is really a character set, used when the old representation
  // of the value was a character type. Note that in that case,
  // convDoIt has already scaled the value to the correct amount.
  if (operandCurrScale == newScale || DFS2REC::isAnyCharacter(typeOfOldScale)) return ex_expr::EXPR_OK;

  long intermediate = 0;
  double intermediateDouble = 0;
  char *intermediateString = 0;
  char *decimalString = 0;
  NABoolean negativeNum = FALSE;

  // variable used for float operation. Specified in "pragma refligned..."
  float *floatOperandPtr;
  double *doubleOperandPtr;

  long limit = 0;
  switch (operandType) {
    case REC_BIN16_SIGNED: {
      intermediate = (long)*(short *)operand;
      limit = (long)((intermediate > 0) ? (SHRT_MAX / 10) : (SHRT_MIN / 10));
    } break;
    case REC_BIN16_UNSIGNED: {
      intermediate = (long)*(unsigned short *)operand;
      limit = (long)(USHRT_MAX / 10);
    } break;
    case REC_BIN32_SIGNED: {
      intermediate = (long)*(int *)operand;
      limit = (long)((intermediate > 0) ? (INT_MAX / 10) : (INT_MIN / 10));
    } break;
    case REC_BIN32_UNSIGNED: {
      intermediate = (long)*(UInt32 *)operand;
      limit = (long)(UINT_MAX / 10);
    } break;
    case REC_BIN64_SIGNED: {
      intermediate = *(long *)operand;
      limit = (long)((intermediate > 0) ? (LLONG_MAX / 10) : (LLONG_MIN / 10));
    } break;
    case REC_FLOAT32: {
      floatOperandPtr = (float *)operand;
      intermediateDouble = *floatOperandPtr;  //(double) *(float*)operand;
    } break;
    case REC_FLOAT64: {
      doubleOperandPtr = (double *)operand;
      intermediateDouble = *doubleOperandPtr;  //*(double*)operand;
    } break;
    case REC_DECIMAL_LSE: {
      case REC_DECIMAL_UNSIGNED:
        // Check if the first bit is set
        negativeNum = operand[0] & 0x80;
        if (negativeNum) operand[0] &= 0x7F;
        decimalString = operand;
    } break;
    case REC_DECIMAL_LS: {
      intermediateString = operand;
    } break;
    case REC_NUM_BIG_SIGNED:
    case REC_NUM_BIG_UNSIGNED: {
      // not yet supported
      return ex_expr::EXPR_ERROR;

      intermediateString = operand;
    } break;
    default:
      return ex_expr::EXPR_ERROR;
  };
  int scale = operandCurrScale - newScale;

  if (intermediate != 0) {
    // do calculations only, if intermediate is not 0.
    if (scale > 0) {
      // operandCurrScale in greater than newScale, we have to downscale
      // we can stop if intermediate is 0
      while (intermediate && scale--) intermediate /= 10;
    } else {
      // we have to upscale.
      while (scale++) {
        if (((intermediate < 0) && (intermediate < limit)) || ((intermediate > 0) && (intermediate > limit))) {
          // the next multiplication causes an overflow
          return ex_expr::EXPR_ERROR;
        };
        intermediate *= 10;
      };
    };
  }  // intermediate != 0
  else if (intermediateDouble != 0) {
    // downscale from exact to float
    scale = operandCurrScale;

    // Check for overflow. TBD.
    while (scale--) intermediateDouble /= 10;
  } else if (intermediateString != 0) {
    if (DFS2REC::isBigNum(operandType)) {
    } else {
      intermediateString = new (heap) char[operandLen];
      memset(intermediateString, '0', operandLen);
      if (scale > 0) {  // downscale
        if (scale <= (operandLen - 1))
          memcpy(&intermediateString[scale + 1], &operand[1], operandLen - scale - 1);
        else {
          NADELETEBASIC(intermediateString, heap);
          return ex_expr::EXPR_ERROR;
        }
      }
      if (scale < 0) {  // upscale;
        scale = -scale;
        if (scale <= (operandLen - 1)) {
          char *first_nonzero = str_find_first_nonzero(&operand[1], scale);
          if (first_nonzero) {
            NADELETEBASIC(intermediateString, heap);
            return ex_expr::EXPR_ERROR;
          }
          str_cpy_all(&intermediateString[1], &operand[1 + scale], operandLen - scale - 1);
        } else {
          NADELETEBASIC(intermediateString, heap);
          return ex_expr::EXPR_ERROR;
        }
      }
    }  // decimal
  }    // intermediateString != 0
  else if (decimalString != 0) {
    decimalString = new (heap) char[operandLen];
    memset(decimalString, '0', operandLen);
    if (scale > 0) {  // downscale
      if (scale <= operandLen)
        str_cpy_all(&decimalString[scale], &operand[0], operandLen - scale);
      else {
        NADELETEBASIC(decimalString, heap);
        return ex_expr::EXPR_ERROR;
      }
    }
    if (scale < 0) {  // upscale;
      scale = -scale;
      if (scale <= operandLen) {
        char *first_nonzero = str_find_first_nonzero(&operand[0], scale);
        if (first_nonzero) {
          NADELETEBASIC(decimalString, heap);
          return ex_expr::EXPR_ERROR;
        }
        str_cpy_all(&decimalString[0], &operand[scale], operandLen - scale);
      } else {
        NADELETEBASIC(decimalString, heap);
        return ex_expr::EXPR_ERROR;
      }
    }
  }
  // scaling was sucessful, put result into operand
  switch (operandType) {
    case REC_BIN16_SIGNED: {
      *(short *)operand = (short)intermediate;
    } break;
    case REC_BIN16_UNSIGNED: {
      *(unsigned short *)operand = (unsigned short)intermediate;
    } break;
    case REC_BIN32_SIGNED: {
      *(int *)operand = (int)intermediate;
    } break;
    case REC_BIN32_UNSIGNED: {
      *(int *)operand = (int)intermediate;
    } break;
    case REC_BIN64_SIGNED: {
      *(long *)operand = intermediate;
    } break;
    case REC_FLOAT32: {
      floatOperandPtr = (float *)operand;
      *floatOperandPtr = (float)intermediateDouble;
    } break;
    case REC_FLOAT64: {
      doubleOperandPtr = (double *)operand;
      *doubleOperandPtr = intermediateDouble;
    } break;

    case REC_DECIMAL_LS: {
      str_cpy_all(&operand[1], &intermediateString[1], operandLen - 1);
      NADELETEBASIC(intermediateString, heap);
    } break;
    case REC_DECIMAL_LSE:
    case REC_DECIMAL_UNSIGNED:
      str_cpy_all(operand, decimalString, operandLen);
      NADELETEBASIC(decimalString, heap);
      if (negativeNum) operand[0] |= 0x80;
      break;
  };
  return ex_expr::EXPR_OK;
};

ex_expr::exp_return_type swapBytes(Attributes *attr, void *ptr) {
  Int16 dataType = attr->getDatatype();
  int storageLength = attr->getStorageLength();
  rec_datetime_field startField;
  rec_datetime_field endField;
  ExpDatetime *dtAttr;

  if (dataType >= REC_MIN_BINARY_NUMERIC && dataType <= REC_MAX_BINARY_NUMERIC) {
    swapBytes(ptr, storageLength);
  } else if (dataType == REC_FLOAT32 || dataType == REC_FLOAT64) {
    swapBytes(ptr, storageLength);
  } else if (dataType >= REC_MIN_INTERVAL && dataType <= REC_MAX_INTERVAL) {
    swapBytes(ptr, storageLength);
  } else if (dataType == REC_NCHAR_F_UNICODE || dataType == REC_NUM_BIG_UNSIGNED || dataType == REC_NUM_BIG_SIGNED) {
    for (int i = 0; i < storageLength; i = i + 2) {
      swapBytes((void *)((char *)ptr + i), 2);
    }
  } else if (dataType == REC_DATETIME) {
    dtAttr = (ExpDatetime *)attr;
    // Get the start and end fields for this Datetime type.
    dtAttr->getDatetimeFields(dtAttr->getPrecision(), startField, endField);
    if (startField == REC_DATE_YEAR)
      // swap the year portion in datetime field
      swapBytes(ptr, 2);
    if (endField == REC_DATE_SECOND) {
      // Swap the fraction part in the seconds
      if (dtAttr->getScale() > 0)  // There is fraction
        swapBytes((char *)ptr + storageLength - 4, 4);
    }
  }
  return ex_expr::EXPR_OK;
}

double doubleRoundFifteen(double doublesource) {
  // integer part
  long integerPart = (long)doublesource;
  int integerPartBit = 0;
  if (integerPart == 0) {
    integerPartBit = 0;
  } else if (integerPart <= 9 && integerPart >= -9) {
    integerPartBit = 1;
  } else {
    integerPartBit = log10(integerPart < 0 ? (-integerPart) : integerPart) + 1;
  }

  // decimal part
  double decimalPart = doublesource - integerPart;
  int decimalPartBit = (15 > integerPartBit ? 15 - integerPartBit : 0);
  double pow10Value = pow(10.0, decimalPartBit);
  decimalPart = round(decimalPart * pow10Value) / pow10Value;

  return (integerPart + decimalPart);
}
