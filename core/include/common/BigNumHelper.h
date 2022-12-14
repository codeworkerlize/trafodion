#ifndef BIG_NUMHELPER_H
#define BIG_NUMHELPER_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         BigNumHelper.h
 * Description:  Definition of class BigNumHelper
 * Created:      03/29/99
 * Language:     C++
 *
 *

 *
 *
 *****************************************************************************
 */

#include "common/Int64.h"
#include "common/NAType.h"

#define MSB_CLR_MSK 0x7F
#define MSB_SET_MSK 0x80

#ifdef NA_LITTLE_ENDIAN
#define BIGN_CLR_SIGN(bignum, len) (((char *)bignum)[len - 1] &= MSB_CLR_MSK);
#define BIGN_SET_SIGN(bignum, len) (((char *)bignum)[len - 1] |= MSB_SET_MSK);
#define BIGN_GET_SIGN(bignum, len) ((char)(((char *)bignum)[len - 1] & MSB_SET_MSK));
#else
#define BIGN_CLR_SIGN(bignum, len) (((char *)bignum)[len - 2] &= MSB_CLR_MSK);
#define BIGN_SET_SIGN(bignum, len) (((char *)bignum)[len - 2] |= MSB_SET_MSK);
#define BIGN_GET_SIGN(bignum, len) ((char)(((char *)bignum)[len - 2] & MSB_SET_MSK));
#endif

class BigNumHelper {
 public:
  // The following method adds two Big Nums (without signs).
  static short AddHelper(int dataLength, char *leftData, char *rightData, char *resultData);

  // The following method subtracts one Big Num from another (without signs).
  static short SubHelper(int dataLength, char *leftData, char *rightData, char *resultData);

  // The following method multiplies two Big Nums (without signs).
  // The assumption is that the result is big enough to hold the
  // product.
  static short MulHelper(int resultLength, int leftLength, int rightLength, char *leftData, char *rightData,
                         char *resultData);

  // The following method divides one Big Num by another (both without signs),
  // only if the divisor fits in an unsigned short. It returns 1 if there is
  // a remainder, 0 if there is no remainder, and -1 if there is an error.
  static short SimpleDivHelper(int dividendLength, int divisorLength, char *dividendData, char *divisorData,
                               char *quotientData);

  // The following method divides one Big Num by another (both without signs).
  // It returns 1 if there is a remainder, 0 if there is no remainder.
  static short DivHelper(int dividendLength, int divisorLength, char *dividendData, char *divisorData,
                         char *quotientData, char *tempData);

  // The following method rounds a Big Num (without signs).
  static short RoundHelper(int sourceLength, int targetLength, char *sourceData, long roundingValue, char *targetData);

  // The following method converts a given Big Num (without sign) into
  // its equivalent BCD string representation (with the more significant decimal
  // digits in the lower addresses).
  static short ConvBigNumToBcdHelper(int sourceLength, int targetLength, char *sourceData, char *targetData,
                                     NAMemory *heap);

  // The following method converts a given BCD string representation (without sign,
  // and with the more significant decimal digits in the lower addresses)
  // into its equivalent Big Num representation. Returns 0 if successful,
  // 1 if successful and result is all zero, -1 if unsuccessful (overflow).
  static short ConvBcdToBigNumHelper(int sourceLength, int targetLength, char *sourceData, char *targetData);

  // The following method converts a given Big Num (with sign) into
  // its equivalent BCD string representation (with the more significant
  // decimal digits in the lower addresses).
  static short ConvBigNumWithSignToBcdHelper(int sourceLength, int targetLength, char *sourceData, char *targetData,
                                             NAMemory *heap);

  // The following method converts a given BCD string representation
  // (with sign, and with the more significant decimal digits in the lower
  // addresses) into its equivalent Big Num representation.
  static short ConvBcdToBigNumWithSignHelper(int sourceLength, int targetLength, char *sourceData, char *targetData);

  // The following method converts a given Big Num (without sign) into
  // its equivalent ASCII string representation (with the more significant
  // decimal digits in the lower addresses).
  static short ConvBigNumToAsciiHelper(int sourceLength, int targetLength, char *sourceData, char *targetData,
                                       NAMemory *heap);

  // The following method converts a given ASCII string representation
  // (without sign, and with the more significant decimal digits in the lower
  // addresses) into its equivalent Big Num representation.
  static short ConvAsciiToBigNumHelper(int sourceLength, int targetLength, char *sourceData, char *targetData);

  // The following method converts a given Big Num (with sign) into
  // its equivalent ASCII string representation (with the more significant
  // decimal digits in the lower addresses).
  static short ConvBigNumWithSignToAsciiHelper(int sourceLength, int targetLength, char *sourceData, char *targetData,
                                               NAMemory *heap);

  // The following method converts a given ASCII string representation
  // (with sign, and with the more significant decimal digits in the lower
  // addresses) into its equivalent Big Num representation.
  static short ConvAsciiToBigNumWithSignHelper(int sourceLength, int targetLength, char *sourceData, char *targetData);

  // Given a desired precision of a Big Num, the following method calculates
  // the required storage length (including the sign). We assume that the
  // precision is > 0.  Storage will always be at a minimum 8 bytes.
  static int ConvPrecisionToStorageLengthHelper(int precision);

  // The following method converts an integer, 10^exponent, to a Big Num
  // representation (without sign). The given exponent should be >= 0.
  static short ConvPowersOfTenToBigNumHelper(int exponent, int targetLength, int *finalTargetLength, char *targetData);

  // The following converts an long to a Big Num (with sign).
  static short ConvInt64ToBigNumWithSignHelper(int targetLength, long sourceData, char *targetData,
                                               NABoolean isUnsigned);

  // The following converts a Big Num (with sign) into an long.
  static short ConvBigNumWithSignToInt64Helper(int sourceLength, char *sourceData, void *targetData,
                                               NABoolean isUnsigned);

  // The following converts a BIGNUM (with sign) into long and scale it,
  // returning information about the conversion to the caller (e.g.
  // truncations, overflows).
  //
  // The return value can have 5 possible values:
  //   0: the conversion was ok
  //   1: the result was rounded up to LLONG_MIN
  //   2: the result was rounded down to LLONG_MAX
  //   3: the result was rounded up
  //   4: the result was rounded down
  static short ConvBigNumWithSignToInt64AndScaleHelper(int sourceLength, char *sourceData, long *targetData,
                                                       int exponent, NAMemory *heap);

  // The following converts a Big Num to a Big Num.
  static short ConvBigNumWithSignToBigNumWithSignHelper(int sourceLength, int targetLength, char *sourceData,
                                                        char *targetData);
};

#endif  // BIG_NUMHELPER_H
