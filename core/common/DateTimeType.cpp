
/* -*-C++-*-
**************************************************************************
*
* File:         DatetimeType.cpp
* Description:  Datetime type
* Created:      2/2/96
* Language:     C++
*
*
**************************************************************************
*/

// -----------------------------------------------------------------------

#include "common/DatetimeType.h"

#include "common/cextdecs.h"
#include <ctype.h>
#include <math.h>

#include "common/NumericType.h"
#include "common/str.h"
#include "exp/exp_clause_derived.h"
#include "exp/exp_datetime.h"

NAString LiteralDate("DATE");
NAString LiteralTime("TIME");
NAString LiteralTimestamp("TIMESTAMP");
NAString LiteralDateTime("DATETIME");
NAString DEFAULT_DATE_DISPLAY_FORMAT("YYYY-MM-DD");

//***********************************************************************
// Static data initializations.
//***********************************************************************

// Note that ANSI 6.1 GR 4 indicates that SECONDS can occasionally
// include one or two extra leap seconds in a minute, so sometimes
// it will be legal for SECONDS to be 60 or 61.  We'll deal with that
// later however, in some DatetimeValue::validateTime() method to
// be called by scanTime(), and adjust the check in DatetimeValue::scanField.
//
// The string literal values are in the ANSI 5.3 format (FORMAT_DEFAULT below).
//
static const UInt32 maxFieldLen[] = {4, 2, 2, 2, 2, 2, DatetimeType::MAX_FRACTION_PRECISION};
static const UInt32 storageLen[] = {2, 1, 1, 1, 1, 1, 4};
static const int minValue[] = {0001, 01, 01, 00, 00, 00, 000000000};
static const int maxValue[] = {9999, 12, 31, 23, 59, 59, 999999999};
static const char minValueString[] = "0001-01-01 00:00:00.000000000";
static const char maxValueString[] = "9999-12-31 23:59:59.999999999";
static const size_t valueStringLen = /*12345678901234567890123456789*/ 29;
static const char precedingPunc[] = "^-- ::.";
//  ^ is placeholder, as there is no
//  punctuation before YEAR, of course.
static const UInt32 daysInMonth[] = {0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

int DatetimeType::getMinValue(int field, int digitsInFraction) {
  // field 6 is the fraction part which requires special handling.
  if (field < 6) return minValue[field];

  if (field == 6) {
    assert(digitsInFraction <= 9);
    return minValue[field];  // return 0
  }

  // should not happen
  assert(0);

  return 0;
}

int DatetimeType::getMaxValue(int field, int digitsInFraction) {
  // field 6 is the fraction part which requires special handling.
  if (field < 6) return maxValue[field];

  if (field == 6) {
    assert(digitsInFraction <= 9);

    int exp = 9 - digitsInFraction;

    // compute a value with digitsInFraction number of 9's.
    return maxValue[field] / (pow(10, exp));
  }

  // should not happen
  assert(0);

  return 999999;
}

// ***********************************************************************
//
//  DatetimeType : Constructors and accessors
//
// ***********************************************************************

DatetimeType::DatetimeType(NAMemory *h, const NAString &adtName, NABoolean allowSQLnull, rec_datetime_field startField,
                           rec_datetime_field endField, UInt32 fractionPrecision)
    : DatetimeIntervalCommonType(h, adtName, NA_DATETIME_TYPE, getStorageSize(startField, endField, fractionPrecision),
                                 allowSQLnull, startField, endField, fractionPrecision, 1 /* no data alignment */
                                 ),
      displayFormat_(h) {
  assert(validate(startField, endField, fractionPrecision) != SUBTYPE_ILLEGAL);
}

short DatetimeType::getFSDatatype() const { return REC_DATETIME; }

NAString DatetimeType::getSimpleTypeName() const { return getTypeName(); }

// ***********************************************************************
//
//  DatetimeType : Virtual methods
//
// ***********************************************************************

NABoolean DatetimeType::isCompatible(const NAType &other, UInt32 *flags) const {
  // DATEs are compatible only with DATEs, TIMEs with TIMEs, TIMESTAMPS w/ TS.
  //
  if (NOT NAType::isCompatible(other, flags)) return FALSE;

  // DATE/TIME, TIMESTAMP/TIME are not compatible. All others are.
  if (getStartField() != ((DatetimeType &)other).getStartField()) return FALSE;

  NABoolean allowIncompOper = ((flags) && ((*flags & NAType::ALLOW_INCOMP_OPER) != 0));
  const DatetimeType *datetime2 = (DatetimeType *)&other;
  if (NOT allowIncompOper) {
    if (getEndField() == datetime2->getEndField())
      return TRUE;
    else
      return FALSE;
  }

  return TRUE;
}

#if defined(NA_LITTLE_ENDIAN)
NABoolean DatetimeType::isEncodingNeeded() const { return TRUE; }
#endif

NABoolean DatetimeType::operator==(const NAType &other) const {
  return NAType::operator==(other) && getStartField() == ((DatetimeType &)other).getStartField() &&
         getEndField() == ((DatetimeType &)other).getEndField() &&
         getFractionPrecision() == ((DatetimeType &)other).getFractionPrecision();
}

NAType::SynthesisPrecedence DatetimeType::getSynthesisPrecedence() const { return SYNTH_PREC_DATETIME; }

// ***********************************************************************
//
//  DatetimeType : Ancillary construction functions (static methods)
//
// ***********************************************************************

// For indexing through the static arrays above.
int DatetimeType::getExtendedEndField(rec_datetime_field endField, UInt32 fractionPrecision) {
  int result = endField;
  if (fractionPrecision > 0) {
    if (endField == REC_DATE_SECOND)
      result++;  // The FRACTION field of the
    else         // static arrays above.
      assert(endField == REC_DATE_FRACTION_MP);
  }

  return result;
}  // DatetimeType::getExtendedEndField

// Return the storage size in bytes for a datetime type.
//
// Datetime field   Digits precision   Storage bytes
//		      (maxFieldLen[])	 (storageLen[])
// --------------   ----------------   -------------
// YEAR                    4                 2
// MONTH                   2                 1
// DAY                     2                 1
// HOUR                    2                 1
// MINUTE                  2                 1
// SECOND                  2                 1
// FRACTION               1-6                4
//
// Thus, DATE [YMD] is size 4,
// TIME(0) [HMS] is size 3,
// TIME(n) [HMSF] is size 7,
// TIMESTAMP(0) [YMDHMS] is size 7,
// TIMESTAMP(n) [YMDHMSF] is size 11.
//
int DatetimeType::getStorageSize(rec_datetime_field startField, rec_datetime_field endField, UInt32 fractionPrecision) {
  int storageSize = 0;
  int end = getExtendedEndField(endField, fractionPrecision);
  for (int field = startField; field <= end; field++) {
    int index = field - REC_DATE_YEAR;
    storageSize += storageLen[index];
  }
  return storageSize;

}  // DatetimeType::getStorageSize

// ***********************************************************************
//
// Convert internal representation to an array of unsigned long.
//
// ***********************************************************************

void DatetimeType::datetimeToLong(void *bufPtr, int values[]) const {
  char *str = (char *)bufPtr;
  short sw;
  int size;
  int start = getStartField();
  int end = getExtendedEndField(getEndField(), getFractionPrecision());
  int val = 0;

  for (int i = start; i <= end; i++) {
    size = storageLen[i - 1];
    switch (size) {
      case sizeof(char):
        val = *str;
        break;
      case sizeof(short):
        memcpy((char *)&sw, str, sizeof(short));
        val = sw;
        break;
      case sizeof(int):
        memcpy((char *)&val, str, sizeof(int));
        break;
      default:
        assert(FALSE);
    }

    // limit values to the max value for SQL/MX datetime, SQL/MP
    // may use binary ones for max values
    UInt32 maxVal = DatetimeType::getMaxValue(i - 1, getFractionPrecision());
    if (val > maxVal) val = maxVal;

    values[i - start] = val;
    str += size;
  }
}

// ***********************************************************************
//
// Returns the number of days for DATE and TIMESTAMP,
// starting from Jan 1, 0001.
//
// ***********************************************************************

int DatetimeType::gregorianDays(const int values[]) {
  int year = values[0] - 1;

  int leapDays = year / 4 + year / 400 - year / 100;
  int days = year * 365 + leapDays;

  int month = values[1];

  for (int i = 1; i < month; i++) days += daysInMonth[i];

  if ((month > 2) && !leapYear(year + 1)) days--;

  days += values[2];
  return days;
}

long DatetimeType::julianTimestampValue(const char *value, const short valueLen, UInt32 fractionPrec) {
  long juliantimestamp = 0;
  const char *datetimeOpData = value;
  short year;
  char month;
  char day;
  char hour;
  char minute;
  char second;
  int fraction = 0;
  str_cpy_all((char *)&year, datetimeOpData, sizeof(year));
  datetimeOpData += sizeof(year);
  month = *datetimeOpData++;
  day = *datetimeOpData++;
  hour = *datetimeOpData++;
  minute = *datetimeOpData++;
  second = *datetimeOpData++;
  if ((datetimeOpData - value) < valueLen) {
    str_cpy_all((char *)&fraction, datetimeOpData, sizeof(fraction));
    if (fractionPrec > 0) {
      if (fractionPrec > DatetimeType::MAX_FRACTION_PRECISION_USEC)
        // Adjust the fractional seconds part to be the number of microseconds
        fraction /= (int)pow(10, (fractionPrec - DatetimeType::MAX_FRACTION_PRECISION_USEC));
      else
        fraction *= (int)pow(10, (DatetimeType::MAX_FRACTION_PRECISION_USEC - fractionPrec));
    }
  }
  short timestamp[] = {year, month, day, hour, minute, second, (short)(fraction / 1000), (short)(fraction % 1000)};
  short error;
  juliantimestamp = COMPUTETIMESTAMP(timestamp, &error);
  if (error) {
    return 0;
  }

  return juliantimestamp;
}

// ***********************************************************************
//
// Returns the number of seconds for TIME and TIMESTAMP.
//
// ***********************************************************************

int DatetimeType::secondsInTime(const int values[]) { return (values[0] * 60 * 60 + values[1] * 60 + values[2]); }

enum DatetimeType::Subtype DatetimeType::validate(rec_datetime_field startField, rec_datetime_field endField,
                                                  UInt32 fractionPrecision) {
  //  if (fractionPrecision > MAX_FRACTION_PRECISION)
  //    return SUBTYPE_ILLEGAL;

  switch (startField) {
    case REC_DATE_YEAR:
      switch (endField) {
        case REC_DATE_DAY:  // YEAR TO DAY == DATE
          return SUBTYPE_SQLDate;

        case REC_DATE_SECOND:  // YEAR TO SECOND == TIMESTAMP
        case REC_DATE_FRACTION_MP:
          return SUBTYPE_SQLTimestamp;

        case REC_DATE_YEAR:
        case REC_DATE_MONTH:
        case REC_DATE_HOUR:
        case REC_DATE_MINUTE:
          return SUBTYPE_SQLMPDatetime;

        default:
          assert(FALSE);
      }  // end switch on endfield
      break;

    case REC_DATE_HOUR:
      switch (endField) {
        case REC_DATE_SECOND:  // HOUR TO SECOND == TIME
        case REC_DATE_FRACTION_MP:
          return SUBTYPE_SQLTime;

        case REC_DATE_YEAR:
        case REC_DATE_MONTH:
        case REC_DATE_DAY:
          return SUBTYPE_ILLEGAL;

        case REC_DATE_HOUR:
        case REC_DATE_MINUTE:
          return SUBTYPE_SQLMPDatetime;

        default:
          assert(FALSE);
      }  // end switch on endfield
      break;

    case REC_DATE_MONTH:
    case REC_DATE_DAY:
    case REC_DATE_MINUTE:
    case REC_DATE_SECOND:
    case REC_DATE_FRACTION_MP:

      if (startField <= endField)
        return SUBTYPE_SQLMPDatetime;
      else
        return SUBTYPE_ILLEGAL;

    default:
      assert(FALSE);
  }  // end switch on startfield

  return SUBTYPE_ILLEGAL;

}  // DatetimeType::validate

DatetimeType *DatetimeType::constructSubtype(NABoolean allowSQLnull, rec_datetime_field startField,
                                             rec_datetime_field endField, UInt32 precision, NAMemory *h) {
  switch (validate(startField, endField, precision)) {
    case SUBTYPE_SQLDate:
      return new (h) SQLDate(h, allowSQLnull);
    case SUBTYPE_SQLTime:
      return new (h) SQLTime(h, allowSQLnull, precision);
    case SUBTYPE_SQLTimestamp:
      return new (h) SQLTimestamp(h, allowSQLnull, precision);
    case SUBTYPE_SQLMPDatetime:
      return new (h) SQLMPDatetime(h, startField, endField, allowSQLnull, precision);

    default:
      return NULL;
  }
}  // DatetimeType::constructSubtype

// ***********************************************************************
//
//  DatetimeType : The datetime data type
//
// ***********************************************************************

int DatetimeType::getDisplayLength() const {
  int field = getStartField() - REC_DATE_YEAR;
  size_t displayLength = maxFieldLen[field];
  while (field < (getEndField() - REC_DATE_YEAR)) displayLength += 1 /* for separator */ + maxFieldLen[++field];
  if (getFractionPrecision() > 0) {
    if (getStartField() == REC_DATE_FRACTION_MP) {
      displayLength = getFractionPrecision();
    } else {
      displayLength += 1 /* for separator */ + getFractionPrecision();
    }
  }
  return displayLength;
}  // DatetimeType::getDisplayLength

int DatetimeType::getDisplayLength(int datatype, int length, int precision, int scale, int heading_len) const {
  int d_len = (int)getDisplayLength();

  if (d_len >= heading_len)
    return d_len;
  else
    return heading_len;
}

NAString DatetimeType::getTypeSQLname(NABoolean terse) const {
  NAString name(getTypeName());

  name += getDatetimeQualifierAsString(TRUE);  // TRUE-- include fractional prec

  getTypeSQLnull(name, terse);
  return name;
}  // DatetimeType::getTypeSQLname

// ***********************************************************************
//
// Type synthesis for binary operators
//
// ***********************************************************************

const NAType *DatetimeType::synthesizeType(enum NATypeSynthRuleEnum synthRule, const NAType &operand1,
                                           const NAType &operand2, NAMemory *h, UInt32 *flags) const {
  //
  // If the second operand's type synthesis rules have higher precedence than
  // this operand's rules, use the second operand's rules.
  //
  if (operand2.getSynthesisPrecedence() > getSynthesisPrecedence())
    return operand2.synthesizeType(synthRule, operand1, operand2, h, flags);
  //
  // Only D-D, I+D, D+I, and D-I are legal.
  // (And union, i.e. supertyping, of course.)
  // ANSI 6.14 and 6.15.
  //
  const DatetimeType *datetime;
  const IntervalType *interval;
  switch (synthRule) {
    case SYNTH_RULE_ADD:
      switch (operand1.getTypeQualifier()) {
        case NA_INTERVAL_TYPE:
          if (operand2.getTypeQualifier() != NA_DATETIME_TYPE) return NULL;
          interval = (IntervalType *)&operand1;
          datetime = (DatetimeType *)&operand2;
          break;
        case NA_DATETIME_TYPE:
          if (operand2.getTypeQualifier() != NA_INTERVAL_TYPE) return NULL;
          datetime = (DatetimeType *)&operand1;
          interval = (IntervalType *)&operand2;
          break;
        default:
          return NULL;
      }
      //
      // INTERVALs are compatible with datetimes if the interval
      // only contains fields that are in the datetime: ANSI 6.14 SR 3.
      //
      if ((datetime->getStartField() > interval->getStartField()) ||
          (datetime->getEndField() < interval->getEndField()))
        return NULL;

      return constructSubtype(datetime->supportsSQLnull() || interval->supportsSQLnull(), datetime->getStartField(),
                              datetime->getEndField(), datetime->getFractionPrecision(), h);

      break;

    case SYNTH_RULE_SUB:
      if ((operand1.getTypeQualifier() != NA_DATETIME_TYPE) ||
          ((operand2.getTypeQualifier() != NA_INTERVAL_TYPE) && (operand2.getTypeQualifier() != NA_DATETIME_TYPE)))
        return NULL;

      if (operand2.getTypeQualifier() == NA_INTERVAL_TYPE) {
        datetime = (DatetimeType *)&operand1;
        interval = (IntervalType *)&operand2;

        //
        // INTERVALs are compatible with datetimes if the interval
        // only contains fields that are in the datetime: ANSI 6.14 SR 3.
        //
        if ((datetime->getStartField() > interval->getStartField()) ||
            (datetime->getEndField() < interval->getEndField()))
          return NULL;

        return constructSubtype(datetime->supportsSQLnull() || interval->supportsSQLnull(), datetime->getStartField(),
                                datetime->getEndField(), datetime->getFractionPrecision(), h);
      } else {
        const DatetimeType *datetime1 = (DatetimeType *)&operand1;
        const DatetimeType *datetime2 = (DatetimeType *)&operand2;

        if ((datetime1->getStartField() != datetime2->getStartField()) ||
            (datetime1->getEndField() != datetime2->getEndField()))
          return NULL;

        NABoolean modeCompatible1 = ((flags) && ((*flags & NAType::MODE_COMPATIBLE_1) != 0));

        NABoolean allowNulls = (((datetime1->supportsSQLnull()) || (datetime2->supportsSQLnull())) ? TRUE : FALSE);

        UInt32 fractionPrecision = MAXOF(datetime1->getFractionPrecision(), datetime2->getFractionPrecision());

        if ((modeCompatible1) && (fractionPrecision == 0) &&
            (datetime1->getSubtype() == DatetimeType::SUBTYPE_SQLDate) &&
            (datetime2->getSubtype() == DatetimeType::SUBTYPE_SQLDate)) {
          // this is DATE subtraction in modespecial4.
          // Result is numeric.
          return new (h) SQLInt(h);
        } else {
          int leadingPrecision =
              IntervalType::computeLeadingPrecision(datetime1->getEndField(), SQLInterval::MAX_LEADING_PRECISION,
                                                    datetime1->getEndField(), fractionPrecision);

          return new (h) SQLInterval(h, allowNulls, datetime1->getEndField(), leadingPrecision,
                                     datetime1->getEndField(), fractionPrecision);
        }
      }
      break;

    case SYNTH_RULE_UNION: {
      if (!operand1.isCompatible(operand2, flags)) return NULL;
      const DatetimeType &op1 = (DatetimeType &)operand1;
      const DatetimeType &op2 = (DatetimeType &)operand2;
      UInt32 fractionPrecision = MAXOF(op1.getFractionPrecision(), op2.getFractionPrecision());
      return constructSubtype(op1.supportsSQLnull() || op2.supportsSQLnull(), op1.getStartField(), op1.getEndField(),
                              fractionPrecision, h);
    }
    default:
      return NULL;
  }
}  // synthesizeType()

// ***********************************************************************
//
// Type synthesis for ternary operators
//
// ***********************************************************************

const NAType *DatetimeType::synthesizeTernary(enum NATypeSynthRuleEnum synthRule, const NAType &operand1,
                                              const NAType &operand2, const NAType &operand3, NAMemory *h) const {
  //
  // If the second operand's type synthesis rules have higher precedence than
  // this operand's rules, use the second operand's rules.
  //
  if (operand2.getSynthesisPrecedence() > getSynthesisPrecedence())
    return operand2.synthesizeTernary(synthRule, operand1, operand2, operand3, h);
  //
  // If the third operand's type synthesis rules have higher precedence than
  // this operand's rules, use the third operand's rules.
  //
  if (operand3.getSynthesisPrecedence() > getSynthesisPrecedence())
    return operand3.synthesizeTernary(synthRule, operand1, operand2, operand3, h);
  //
  // So far, "(D-D) interval_qualifier" is the only ternary operator.
  //
  switch (synthRule) {
    case SYNTH_RULE_SUB: {
      if ((operand1.getTypeQualifier() != NA_DATETIME_TYPE) || (operand2.getTypeQualifier() != NA_DATETIME_TYPE) ||
          (operand3.getTypeQualifier() != NA_INTERVAL_TYPE) || (!operand1.isCompatible(operand2)))
        return NULL;
      const DatetimeType &op1 = (DatetimeType &)operand1;
      const DatetimeType &op2 = (DatetimeType &)operand2;
      const IntervalType &op3 = (IntervalType &)operand3;
      return new (h) SQLInterval(h, op1.supportsSQLnull() || op2.supportsSQLnull(), op3.getStartField(),
                                 op3.getLeadingPrecision(), op3.getEndField(), op3.getFractionPrecision());
    }
    default:
      break;
  }
  return NULL;
}  // synthesizeTernary()

// ***********************************************************************
//
//  DatetimeType : Min and max values
//
// ***********************************************************************

void DatetimeType::getRepresentableValue(const char *inValueString, void *bufPtr, int *bufLen, NAString **stringLiteral,
                                         NAMemory *h) const {
  int startIndex = getStartField() - REC_DATE_YEAR;
  int endIndex = getEndField() - REC_DATE_YEAR;
  int startOff, endOff, i = 0;
  UInt32 fracPrec = 0;

  for (startOff = 0; i < startIndex; i++)
    startOff += maxFieldLen[i] + 1;  // +1 to include the separator after the field

  for (endOff = startOff; i <= endIndex; i++)
    endOff += maxFieldLen[i] + 1;  // +1 to include the separator after the field

  if (getFractionPrecision()) {
    endOff += getFractionPrecision() + 1;  // +1 for the null
  }

  endOff--;               // was at offset of next field, now at offset of the delimiter
  i = endOff - startOff;  // length (endOff is one past last char)

  if (getSubtype() == SUBTYPE_SQLMPDatetime) {
    fracPrec = getFractionPrecision();
    if (getStartField() == REC_DATE_FRACTION_MP) {
      int adjust = (6 - fracPrec) + 2;  // move past "00." (seconds + decimal pt.)
      startOff += adjust;
      i -= adjust;
    }
  }

  char valueString[valueStringLen + 1];
  assert(i <= valueStringLen);

  str_cpy_all(valueString, &inValueString[startOff], i /*length*/);
  valueString[i] = '\0';
  DatetimeValue dtValue(valueString, getStartField(), getEndField(), fracPrec, FALSE);

  assert(*bufLen >= dtValue.getValueLen() && dtValue.isValid());
  *bufLen = dtValue.getValueLen();
  str_cpy_all((char *)bufPtr, (char *)dtValue.getValue(), dtValue.getValueLen());

  if (stringLiteral) {
    *stringLiteral = new (h) NAString(getSimpleTypeName(), h);
    **stringLiteral += "\'";
    **stringLiteral += valueString;
    **stringLiteral += "\'";
    if (getSubtype() == SUBTYPE_SQLMPDatetime) {
      **stringLiteral += getDatetimeQualifierAsString(FALSE);
    }
  }
}  // DatetimeType::getRepresentableValue

void DatetimeType::minRepresentableValue(void *bufPtr, int *bufLen, NAString **stringLiteral, NAMemory *h) const {
  getRepresentableValue(minValueString, bufPtr, bufLen, stringLiteral, h);
}

void DatetimeType::maxRepresentableValue(void *bufPtr, int *bufLen, NAString **stringLiteral, NAMemory *h) const {
  getRepresentableValue(maxValueString, bufPtr, bufLen, stringLiteral, h);
}

NABoolean DatetimeType::createSQLLiteral(const char *buf, NAString *&stringLiteral, NABoolean &isNull,
                                         CollHeap *h) const {
  if (NAType::createSQLLiteral(buf, stringLiteral, isNull, h)) return TRUE;

  // First step is to convert to ASCII, then add datetime syntax around it
  const int RESULT_LEN = 100;
  char result[RESULT_LEN];
  const char *valPtr = buf + getSQLnullHdrSize();
  unsigned short resultLen = 0;
  ComDiagsArea *diags = NULL;

  ex_expr::exp_return_type ok =
      convDoIt((char *)valPtr, getNominalSize(), getFSDatatype(), getPrecision(), getScale(), result, RESULT_LEN,
               REC_BYTE_V_ASCII, 0, SQLCHARSETCODE_UTF8, (char *)&resultLen, sizeof(resultLen), h, &diags,
               ConvInstruction::CONV_UNKNOWN, 0);

  if (ok != ex_expr::EXPR_OK || resultLen == 0) return FALSE;

  stringLiteral = new (h) NAString(result, resultLen, h);
  *stringLiteral += '\'';
  if (getTypeName() == "DATE")
    stringLiteral->prepend(" DATE '");
  else if (getTypeName() == "TIME")
    stringLiteral->prepend(" TIME '");
  else if (getTypeName() == "TIMESTAMP")
    stringLiteral->prepend(" TIMESTAMP '");
  return TRUE;
}

// ***********************************************************************
//
//  DatetimeType : getRecDateTimeCode
//
//  Converts start/end combination into REC_DATETIME_CODE enum.
// ***********************************************************************

int DatetimeType::getRecDateTimeCode(rec_datetime_field startField, rec_datetime_field endField) const {
  switch (startField) {
    case REC_DATE_YEAR:
      switch (endField) {
        case REC_DATE_YEAR:
          return REC_DTCODE_YEAR;
        case REC_DATE_MONTH:
          return REC_DTCODE_YEAR_MONTH;
        case REC_DATE_DAY:
          return REC_DTCODE_DATE;
        case REC_DATE_HOUR:
          return REC_DTCODE_YEAR_HOUR;
        case REC_DATE_MINUTE:
          return REC_DTCODE_YEAR_MINUTE;
        case REC_DATE_SECOND:
          return REC_DTCODE_TIMESTAMP;
      }
    case REC_DATE_MONTH:
      switch (endField) {
        case REC_DATE_MONTH:
          return REC_DTCODE_MONTH;
        case REC_DATE_DAY:
          return REC_DTCODE_MONTH_DAY;
        case REC_DATE_HOUR:
          return REC_DTCODE_MONTH_HOUR;
        case REC_DATE_MINUTE:
          return REC_DTCODE_MONTH_MINUTE;
        case REC_DATE_SECOND:
          return REC_DTCODE_MONTH_SECOND;
      }
    case REC_DATE_DAY:
      switch (endField) {
        case REC_DATE_DAY:
          return REC_DTCODE_DAY;
        case REC_DATE_HOUR:
          return REC_DTCODE_DAY_HOUR;
        case REC_DATE_MINUTE:
          return REC_DTCODE_DAY_MINUTE;
        case REC_DATE_SECOND:
          return REC_DTCODE_DAY_SECOND;
      }
    case REC_DATE_HOUR:
      switch (endField) {
        case REC_DATE_HOUR:
          return REC_DTCODE_HOUR;
        case REC_DATE_MINUTE:
          return REC_DTCODE_HOUR_MINUTE;
        case REC_DATE_SECOND:
          return REC_DTCODE_TIME;
      }
    case REC_DATE_MINUTE:
      switch (endField) {
        case REC_DATE_MINUTE:
          return REC_DTCODE_MINUTE;
        case REC_DATE_SECOND:
          return REC_DTCODE_MINUTE_SECOND;
      }
    case REC_DATE_SECOND:
      switch (endField) {
        case REC_DATE_SECOND:
          return REC_DTCODE_SECOND;
      }
    case REC_DATE_FRACTION_MP:
      switch (endField) {
        case REC_DATE_FRACTION_MP:
          return REC_DTCODE_FRACTION;
      }
  }

  // Invalid combination of start/end fields.
  //
  assert(0);
  return REC_DTCODE_FRACTION;
}
// ***********************************************************************
//
//  DatetimeType :: containsField
//
//  Checks if the given field is contained within the given type.
//  (Note very similar routine in DatetimeValue class.)
// ***********************************************************************

NABoolean DatetimeType::containsField(rec_datetime_field theField) const {
  if (theField >= getStartField() && theField <= getEndField()) {
    return TRUE;
  } else {
    return FALSE;
  }
}

// ***********************************************************************
//
//  DatetimeType :: fieldsOverlap
//
//  Checks if the given types have overlapping fields.
//
// ***********************************************************************
NABoolean DatetimeType::fieldsOverlap(const DatetimeType &other) const {
  if (getEndField() < other.getStartField() || (getStartField() > other.getEndField())) {
    return FALSE;
  } else {
    return TRUE;
  }
}

// ***********************************************************************
//
//  DatetimeType : Encode
//
// ***********************************************************************

double DatetimeType::encode(void *bufPtr) const {
  return -1;  // for now ...##
}  // DatetimeType::encode

// ***********************************************************************
//
//  DatetimeType : Display/debug
//
// ***********************************************************************
void DatetimeType::print(FILE *ofd, const char *indent) /*const*/
{
  fprintf(ofd, "%s ", getTypeSQLname().data());
  NAType::print(ofd, indent);
}  // DatetimeType::print

// ***********************************************************************
//
// DatetimeType::getDatetimeQualifierAsString
// modelled after IntervalType::getIntervalQualifierAsString()
//
// ***********************************************************************
const NAString DatetimeType::getDatetimeQualifierAsString(NABoolean includeFractionalPrec) const {
  NAString fields;
  char precision[8];  // '(' , '%u', ')' '\0'

  if (getSubtype() == SUBTYPE_SQLMPDatetime) {
    fields += " ";
    fields += getFieldName(getStartField());
    if (getStartField() != getEndField() || (getFractionPrecision() > 0 && getEndField() >= REC_DATE_SECOND)) {
      fields += " TO ";  // change endField of SECOND back to FRACTION for display.
      if (getFractionPrecision() > 0 && getEndField() >= REC_DATE_SECOND) {
        fields += getFieldName(REC_DATE_FRACTION_MP);
      } else {
        fields += getFieldName(getEndField());
        includeFractionalPrec = FALSE;
      }
    }
  }

  if (getEndField() >= REC_DATE_SECOND && includeFractionalPrec &&
      (getFractionPrecision() > 0 || (getSubtype() != SUBTYPE_SQLMPDatetime))) {
    sprintf(precision, "(%u)", getFractionPrecision());
    fields += precision;
  }
  return fields;
}

double SQLDate::encode(void *bufPtr) const {
  char *valPtr = (char *)bufPtr;
  double val;

  if (supportsSQLnull()) valPtr += getSQLnullHdrSize();
  int w[10];

  for (int i = 0; i < 10; i++) w[i] = 0;

  datetimeToLong(valPtr, w);
  assert(w[0] <= 9999);
  val = gregorianDays(w);
  return (val * 24.0 * 60 * 60);  // watch out for overflow!
}

// Convert to a SQL Date literal "date 'yyyy-mm-dd'"
NAString *SQLDate::convertToString(double v, NAMemory *h) const {
  // v contains seconds since Jan 1, 0001 (see encode() and gregorianDays()).

  // Julian date for Jan 1, 0001 is 1721423.5. Convert to seconds.
  // Add 1 day to make the day correct.
  double secs = 1721424.5 * 86400;

  // Convert v into Julian date in seconds.
  secs += v;

  UInt32 dtvFields[DatetimeValue::N_DATETIME_FIELDS];
  DatetimeValue::decodeTimestamp(long(secs * 1000000),  // in microseconds
                                 0,                     // fracSec
                                 dtvFields);

  char data[20];

  sprintf(data, "DATE '%4d-%02d-%02d'", dtvFields[DatetimeValue::YEAR], dtvFields[DatetimeValue::MONTH],
          dtvFields[DatetimeValue::DAY]);

  return new (h) NAString(data, h);
}

double SQLDate::encodeSqlLiteral(char *source) const {
  char target[40];
  memset(target, '\0', sizeof(target));

  char *targetPtr = target;
  int targetLen = sizeof(target);

  if (supportsSQLnull()) {
    targetPtr += getSQLnullHdrSize();
    targetLen -= getSQLnullHdrSize();
  }

  // NAMemory* heap = h_;

  ex_expr::exp_return_type ok = convDoIt(source, strlen(source), REC_BYTE_F_ASCII, 0, 0, targetPtr, targetLen,
                                         REC_DATETIME, REC_DTCODE_DATE, 0, NULL, 0,
                                         0,  // heap
                                         0,  // diagsPtr
                                         ConvInstruction::CONV_UNKNOWN,
                                         0,  // data conversionErrorFlag
                                         0   // flags
  );

  if (ok != ex_expr::EXPR_OK) return 0;

  return encode(target);
}

double SQLDate::getMinValue() const {
  char source[20];
  sprintf(source, "0001-01-01");
  return encodeSqlLiteral(source);
}

double SQLDate::getMaxValue() const {
  char source[20];
  sprintf(source, "9999-12-31");
  return encodeSqlLiteral(source);
}

double SQLTime::encode(void *bufPtr) const {
  char *valPtr = (char *)bufPtr;
  double val;

  if (supportsSQLnull()) valPtr += getSQLnullHdrSize();
  int w[6];

  for (int i = 0; i < 6; i++) w[i] = 0;

  datetimeToLong(valPtr, w);
  assert(w[0] <= 24);
  val = secondsInTime(w) + (double)w[3] / pow(10, getFractionPrecision());
  return val;
}

NAString *SQLTime::convertToString(double v, NAMemory *h) const {
  // v contains seconds since Jan 1, 0001 (see encode() and gregorianDays()).

  // Julian date for Jan 1, 2014 0:0:0 sec is 2456658.5. Convert to seconds.
  double secs = 2456658.5 * 86400;

  // Convert v into Julian date in seconds.
  secs += v;

  UInt32 dtvFields[DatetimeValue::N_DATETIME_FIELDS];
  DatetimeValue::decodeTimestamp(long(secs * 1000000),  // in microseconds
                                 0,                     // fracSec
                                 dtvFields);

  char data[30];

  sprintf(data, "TIME '%02d:%02d:%02d.%d'", dtvFields[DatetimeValue::HOUR], dtvFields[DatetimeValue::MINUTE],
          dtvFields[DatetimeValue::SECOND], dtvFields[DatetimeValue::FRACTION]);

  NAString *result = new (h) NAString(data, h);

  return result;
}

double SQLTime::encodeSqlLiteral(char *source) const {
  char target[40];
  memset(target, '\0', sizeof(target));

  char *targetPtr = target;
  int targetLen = sizeof(target);

  if (supportsSQLnull()) {
    targetPtr += getSQLnullHdrSize();
    targetLen -= getSQLnullHdrSize();
  }

  ex_expr::exp_return_type ok = convDoIt(source, strlen(source), REC_BYTE_F_ASCII, 0, 0, targetPtr, targetLen,
                                         REC_DATETIME, REC_DTCODE_TIME, 0, NULL, 0,
                                         0,  // heap
                                         0,  // diagsPtr
                                         ConvInstruction::CONV_UNKNOWN,
                                         0,  // data conversionErrorFlag
                                         0   // flags
  );

  if (ok != ex_expr::EXPR_OK) return 0;

  return encode(target);
}

double SQLTime::getMinValue() const {
  char source[20];
  sprintf(source, "00:00:00");
  return encodeSqlLiteral(source);
}

double SQLTime::getMaxValue() const {
  char source[20];
  sprintf(source, "23:59:59");
  return encodeSqlLiteral(source);
}

double SQLTimestamp::encode(void *bufPtr) const {
  char *valPtr = (char *)bufPtr;
  double val;

  if (supportsSQLnull()) valPtr += getSQLnullHdrSize();
  int w[10];

  for (int i = 0; i < 10; i++) w[i] = 0;

  datetimeToLong(valPtr, w);
  assert(w[0] <= 9999);
  val = gregorianDays(w) * 24.0 * 60 * 60 + secondsInTime(&w[3]);  // watch out for overflow!
  val += (double)w[6] / pow(10, getFractionPrecision());
  return val;
}

// Convert to a SQL Timestamp literal 'timestamp yyyy-mm-dd hh:mm:ss.fraction'
NAString *SQLTimestamp::convertToString(double v, NAMemory *h) const {
  // v contains seconds since Jan 1, 0001 (see encode() and gregorianDays()).

  // Julian date for Jan 1, 0001 is 1721423.5. Convert to seconds.
  // Add 1 day to make the day correct.
  double secs = 1721424.5 * 86400;

  // Convert v into Julian date in seconds.
  secs += v;

  UInt32 dtvFields[DatetimeValue::N_DATETIME_FIELDS];
  DatetimeValue::decodeTimestamp(long(secs * 1000000),  // in microseconds
                                 0,                     // fracSec
                                 dtvFields);

  char date[30];

  sprintf(date, "timestamp '%4d-%02d-%02d", dtvFields[DatetimeValue::YEAR], dtvFields[DatetimeValue::MONTH],
          dtvFields[DatetimeValue::DAY]);

  NAString *result = new (h) NAString(date, h);

  char time[40];
  sprintf(time, " %02d:%02d:%02d.%d'", dtvFields[DatetimeValue::HOUR], dtvFields[DatetimeValue::MINUTE],
          dtvFields[DatetimeValue::SECOND], dtvFields[DatetimeValue::FRACTION]);

  result->append(time);
  return result;
}

double SQLTimestamp::encodeSqlLiteral(char *source) const {
  char target[40];
  memset(target, '\0', sizeof(target));

  char *targetPtr = target;
  int targetLen = sizeof(target);

  if (supportsSQLnull()) {
    targetPtr += getSQLnullHdrSize();
    targetLen -= getSQLnullHdrSize();
  }

  ex_expr::exp_return_type ok = convDoIt(source, strlen(source), REC_BYTE_F_ASCII, 0, 0, targetPtr, targetLen,
                                         REC_DATETIME, REC_DTCODE_TIMESTAMP, 0, NULL, 0,
                                         0,  // heap
                                         0,  // diagsPtr
                                         ConvInstruction::CONV_UNKNOWN,
                                         0,  // data conversionErrorFlag
                                         0   // flags
  );

  if (ok != ex_expr::EXPR_OK) return 0;

  return encode(target);
}

double SQLTimestamp::getMinValue() const {
  char source[40];
  sprintf(source, "0001-01-01 00:00:00");
  return encodeSqlLiteral(source);
}

double SQLTimestamp::getMaxValue() const {
  char source[50];
  // initialize length to size without the fraction part
  int len = 19;
  sprintf(source, "9999-12-31 23:59:59");
  // 0123456789012345678901234567890  = 18 characters

  if (getScale() > 0) {
    char *fraction = source + strlen(source);
    fraction[0] = '.';  // 19 chars
    fraction++;
    for (int i = 0; i < getScale(); i++) {
      fraction[i] = '9';  // getScale() <= 6
    }
  }

  // add the fraction size if any
  len += getScale();
  source[len + 1] = 0;

  return encodeSqlLiteral(source);
}

double SQLMPDatetime::encode(void *bufPtr) const {
  char *valPtr = (char *)bufPtr;
  double val;

  if (supportsSQLnull()) valPtr += getSQLnullHdrSize();
  int w[10];

  w[REC_DATE_YEAR - 1] = 1;  // should use min values here
  w[REC_DATE_MONTH - 1] = 1;
  w[REC_DATE_DAY - 1] = 1;
  for (int i = REC_DATE_HOUR - 1; i < 10; i++) w[i] = 0;

  datetimeToLong(valPtr, &w[getStartField() - 1]);
  assert(w[0] <= 9999);
  val = gregorianDays(w) * 24.0 * 60 * 60 + secondsInTime(&w[3]);  // watch out for overflow!
  val += (double)w[6] / pow(10, getFractionPrecision());
  return val;
}

// ***********************************************************************
//
//  SQLMPDatetime::isSupportedType
//
// ***********************************************************************

NABoolean SQLMPDatetime::isSupportedType(void) const {
  if (getStartField() == REC_DATE_FRACTION_MP)
    return FALSE;
  else
    return TRUE;
}

// ***********************************************************************
//
//  SQLMPDatetime::synthesizeType
//
// ***********************************************************************

const NAType *SQLMPDatetime::synthesizeType(enum NATypeSynthRuleEnum synthRule, const NAType &operand1,
                                            const NAType &operand2, NAMemory *h, UInt32 *flags) const {
  if (!operand1.isSupportedType() || !operand2.isSupportedType())
    return NULL;
  else
    return DatetimeType::synthesizeType(synthRule, operand1, operand2, h, flags);
}

// ***********************************************************************
//
//  SQLMPDatetime::isCompatible
//
// ***********************************************************************

NABoolean SQLMPDatetime::isCompatible(const NAType &other, UInt32 *flags) const {
  if (!this->isSupportedType() || !other.isSupportedType())
    return FALSE;
  else
    return DatetimeType::isCompatible(other, flags);
}

// ***********************************************************************
//
//  DatetimeValue : A datetime value
//
// ***********************************************************************
DatetimeValue::DatetimeValue(const char *strValue, rec_datetime_field startField, rec_datetime_field endField,
                             UInt32 &fractionPrecision /* IN - OUT */
                             ,
                             NABoolean useOldConstructor)
    : value_(NULL), valueLen_(0), startField_(startField), endField_(endField) {
  if (strValue == NULL) return;

  fractionPrecision = 0;
  if (useOldConstructor) {
    oldConstructor(strValue, startField, endField, fractionPrecision);
    return;
  }

  char dstValue[101];

  ComDiagsArea *diags = NULL;

  int trueFP = DatetimeType::MAX_FRACTION_PRECISION;
  int flags = 0;
  flags |= CONV_NO_HADOOP_DATE_FIX;
  short rc = ExpDatetime::convAsciiToDatetime((char *)strValue, strlen(strValue), dstValue, 100, startField, endField,
                                              ExpDatetime::DATETIME_FORMAT_NONE, trueFP, NULL, &diags, flags);
  if (rc) return;

  if (endField == REC_DATE_SECOND) {
    int fp = trueFP;
    for (; fp < DatetimeType::MAX_FRACTION_PRECISION; fp++) {
      if (startField == REC_DATE_YEAR)
        *(int *)&dstValue[7] /= 10;
      else
        *(int *)&dstValue[3] /= 10;
    }
  } else
    trueFP = 0;

  fractionPrecision = trueFP;
  valueLen_ = (unsigned short)DatetimeType::getStorageSize(startField, endField, trueFP);
  unsigned char *value = value_ = new unsigned char[valueLen_];

  memcpy(value, dstValue, valueLen_);

}  // DatetimeValue::DatetimeValue ctor

void DatetimeValue::oldConstructor(const char *strValue, rec_datetime_field startField, rec_datetime_field endField,
                                   UInt32 &fractionPrecision /* OUT */) {
  UInt32 saveMPPrecision = fractionPrecision;

  fractionPrecision = 0;
  //
  // Check that the value is a date, time, or timestamp.
  //

  DatetimeType::Subtype subtype = DatetimeType::validate(startField, endField, 0);
  if (subtype == DatetimeType::SUBTYPE_ILLEGAL) return;
  //
  // Skip leading blanks.
  //
  while (*strValue == ' ' || *strValue == '\t') strValue++;
  //
  // If this contains a YEAR, MONTH or DAY, scan it as a DATE
  //
  DatetimeFormat format = FORMAT_ANY;
  int values[N_DATETIME_FIELDS];
  if (startField <= REC_DATE_DAY && (!scanDate(strValue /* INOUT */
                                               ,
                                               format /* OUT */
                                               ,
                                               values /* OUT */
                                               ))) {  // restore original specified precision

    if (subtype == DatetimeType::SUBTYPE_SQLMPDatetime) {
      fractionPrecision = saveMPPrecision;
    }

    return;
  }
  //
  // If this contains both a DAY and HOUR, scan the separator between them
  //
  if (startField <= REC_DATE_DAY && endField >= REC_DATE_HOUR && (!scanChar(strValue, ' ')) &&
      (!scanChar(strValue, ':')) && (!scanChar(strValue, 'T')))
    return;
  //
  // If this contains an HOUR, MINUTE or SECOND, scan is as time.
  //
  if (endField >= REC_DATE_HOUR && (!scanTime(strValue /* INOUT */
                                              ,
                                              format /* IN */
                                              ,
                                              values /* OUT */
                                              ,
                                              fractionPrecision /* OUT */
                                              ))) {             // restore original specified precision
    if (subtype == DatetimeType::SUBTYPE_SQLMPDatetime) {
      fractionPrecision = saveMPPrecision;
    }

    return;
  }
  //
  // Skip trailing blanks.
  //
  while (*strValue == ' ' || *strValue == '\t') strValue++;
  // Like INTERVAL, actual precision of MPDatetime must match specified precision

  if (subtype == DatetimeType::SUBTYPE_SQLMPDatetime) {
    if (fractionPrecision > saveMPPrecision) {
      fractionPrecision = saveMPPrecision;
      return;
    } else {  // adjust the fractional value based on fractional precision.
      for (; fractionPrecision < saveMPPrecision; fractionPrecision++) {
        values[6] *= 10;
      }
    }
  }
  //
  // If this is the end of the string, the datetime value is valid.
  //
  if (*strValue == '\0') setValue(startField, endField, fractionPrecision, values);
}

DatetimeValue::DatetimeValue(const char *value, int storageSize)
    : value_(new unsigned char[storageSize]), valueLen_((unsigned short)storageSize) {
  memcpy(value_, value, valueLen_);
}  // DatetimeValue ctor

NABoolean DatetimeValue::scanDate(const char *&strValue, DatetimeFormat &format, int values[],
                                  NABoolean validate) const {
  //
  // Determine the datetime format (DEFAULT, USA, or EUROPEAN).
  //
  int firstFieldSize = 0;
  const char *s = strValue;
  while (isdigit(*s)) {
    s++;
    firstFieldSize++;
  }

  //
  // The following check does not apply if there is only one DATE field present.
  // The format cannot be determined because there is no separator.
  //
  static const char separator[] = "-/./";
  char currSeparator = 0;

  if (startField_ == endField_ || startField_ == REC_DATE_DAY) {
    format = FORMAT_DEFAULT;
  } else {
    int i = FORMAT_DEFAULT;
    for (; separator[i] != '\0'; i++)
      if (*s == separator[i]) {
        currSeparator = separator[i];
        break;
      }

    format = (DatetimeFormat)i;

    if ((format == FORMAT_USA) && (firstFieldSize == 4)) {
      currSeparator = '/';
      format = FORMAT_DEFAULT;
    }

    if ((format == FORMAT_DEFAULT) && (firstFieldSize == 2)) {
      currSeparator = '-';
      format = FORMAT_USA;
    }

    if (separator[format] == '\0') return FALSE;
  }
  //
  // Scan the year, month, day, and the separators in between.
  //
  static const DatetimeField dateField[][4] = {
      {YEAR, MONTH, DAY} /* DEFAULT */
      ,
      {MONTH, DAY, YEAR} /* USA */
      ,
      {DAY, MONTH, YEAR} /* EUROPEAN */
      ,
      {YEAR, MONTH, DAY} /* ANY: YYYY/MM/DD */
  };

  DatetimeField fieldToScan = dateField[format][0];
  NABoolean needSeparator = FALSE;

  if (containsField(fieldToScan, startField_, endField_)) {
    if (!scanField(strValue, fieldToScan, values, validate)) return FALSE;
    needSeparator = TRUE;
  }

  fieldToScan = dateField[format][1];

  if (containsField(fieldToScan, startField_, endField_)) {
    if (needSeparator) {
      if (!scanChar(strValue, currSeparator)) return FALSE;
    }
    if (!scanField(strValue, fieldToScan, values, validate)) return FALSE;

    needSeparator = TRUE;
  }

  fieldToScan = dateField[format][2];

  if (containsField(fieldToScan, startField_, endField_)) {
    if (needSeparator) {
      if (!scanChar(strValue, currSeparator)) return FALSE;
    }
    if (!scanField(strValue, fieldToScan, values, validate)) return FALSE;
  }

  //
  // Validate the date.
  //
  if ((validate) && (!validateDate(values))) return FALSE;

  // The format could not be determined because there was no separator.
  if (startField_ == REC_DATE_DAY) {
    format = FORMAT_ANY;
  }

  return TRUE;
}  // DatetimeValue::scanDate

NABoolean DatetimeValue::scanAndReturnFields(const char *&strValue, DatetimeFormat &format, int values[]) const {
  return scanDate(strValue, format, values, FALSE);
}

NABoolean DatetimeValue::scanTime(const char *&strValue, DatetimeFormat format, int values[],
                                  UInt32 &fractionPrecision) const {
  static const char separator[] = "::.:";
  NABoolean needSeparator = FALSE;

  //
  // Scan the hour.
  //
  if (containsField(HOUR, startField_, endField_)) {
    if (!scanField(strValue, HOUR, values)) return FALSE;
    needSeparator = TRUE;
  }
  //
  // If the format is not known yet and the separator is a period, the format
  // is EUROPEAN.
  //
  if (format == FORMAT_ANY && *strValue == '.') format = FORMAT_EUROPEAN;
  //
  // Scan the MINUTE and possible separator.
  //
  if (containsField(MINUTE, startField_, endField_)) {
    if (needSeparator) {
      if (!scanChar(strValue, separator[format])) return FALSE;
    }
    if (!scanField(strValue, MINUTE, values)) return FALSE;

    needSeparator = TRUE;
  }
  //
  // Scan the seconds and possible separator.
  //
  if (containsField(SECOND, startField_, endField_)) {
    if (needSeparator) {
      if (!scanChar(strValue, separator[format])) return FALSE;
    }
    if (!scanField(strValue, SECOND, values)) return FALSE;
  }
  //
  // Scan the decimal point before the fraction if present.
  // Scan the fraction if present.
  //
  fractionPrecision = 0;
  values[FRACTION] = 0;
  if (startField_ == REC_DATE_FRACTION_MP || scanChar(strValue, '.')) {
    if (isdigit(*strValue)) {
      const char *fraction = strValue;
      if (!scanField(strValue, FRACTION, values)) return FALSE;
      fractionPrecision = strValue - fraction;
    }
  }
  //
  // Scan the AM or PM if present.
  //
  if (containsField(HOUR, startField_, endField_)) {
    if (format == FORMAT_ANY || format == FORMAT_USA) {
      if (strncmp(strValue, " AM", 3) == 0 || strncmp(strValue, " am", 3) == 0 || strncmp(strValue, " Am", 3) == 0 ||
          strncmp(strValue, " aM", 3) == 0) {
        if (values[HOUR] > 12) return FALSE;
        if (values[HOUR] == 12) values[HOUR] = 0;
        strValue += 3;
      } else if (strncmp(strValue, " PM", 3) == 0 || strncmp(strValue, " pm", 3) == 0 ||
                 strncmp(strValue, " Pm", 3) == 0 || strncmp(strValue, " pM", 3) == 0) {
        if (values[HOUR] > 12) return FALSE;
        if (values[HOUR] < 12) values[HOUR] += 12;
        strValue += 3;
      }
    }  // FORMAT_ANY or FORMAT_USA
    else if (format == FORMAT_DEFAULT) {
      if (strncmp(strValue, "Z", 1) == 0) {
        strValue += 1;
      }
    }  // FORMAT_DEFAULT
  }    // contains HOUR

  return TRUE;
}  // DatetimeValue::scanTime

NABoolean DatetimeValue::scanField(const char *&strValue, DatetimeField field, int values[], NABoolean validate) const {
  //
  // Compute the value of the datetime field.
  //
  int value = 0;
  UInt32 i = 0;
  UInt32 digits = 0;
  for (; i < maxFieldLen[field] && isdigit(strValue[i]); i++) {
    value = value * 10 + strValue[i] - '0';
    digits++;
  }
#ifdef DONT_USE
  // For Release 1 of MX on NSK, all tables are SQL/MP tables, and datetime
  // fields are SQL/MP datetime fields.
  // If we want to support single digit fields (non-ANSI) we could pass a flag
  // in from the parser indicating that MP text is being parsed, and enable this;
  // however, the ANSI exact check below would have to be skipped.

  static const UInt32 minFieldLen[] = {4, 1, 1, 1, 1, 1, 0};
  if (i < minFieldLen[field])  // too short
    return FALSE;
#endif
  //
  // ANSI 5.3 SR 21 prescribes an EXACT number of digits for each field
  // except for the seconds fraction, which may be zero digits (i.e. absent).
  //
  if (i < maxFieldLen[field] && field != FRACTION)  // too short
    return FALSE;
  if (isdigit(strValue[i]))  // too long
    return FALSE;
  strValue += i;  // just right Goldilocks
  //
  // If the value is too small or too large, the field is invalid.
  //
  if (validate &&
      (value < DatetimeType::getMinValue(field, digits) || value > DatetimeType::getMaxValue(field, digits)))
    return FALSE;
  values[field] = value;
  return TRUE;
}  // DatetimeValue::scanField

NABoolean DatetimeValue::validateDate(const int values[]) const {
  //
  // If the days field is more than the number of days in the month, the date
  // is invalid.
  //
  if (startField_ <= REC_DATE_MONTH && containsField(DAY, startField_, endField_)) {
    if (values[DAY] > daysInMonth[values[MONTH]]) return FALSE;
  }
  //
  // If the month field is February and the year is not a leap year, the
  // maximum number of days is 28.
  //
  if (startField_ == REC_DATE_YEAR && containsField(DAY, startField_, endField_)) {
    if (values[MONTH] == 2 && values[DAY] > 28)
      if (values[YEAR] % 4 != 0 || (values[YEAR] % 100 == 0 && values[YEAR] % 400 != 0)) return FALSE;
  }

  return TRUE;
}  // DatetimeValue::validateDate

// Note how this mirrors the structure of DatetimeType::getStorageSize()
void DatetimeValue::setValue(rec_datetime_field startField, rec_datetime_field endField, UInt32 fractionPrecision,
                             const UInt32 values[]) {
  valueLen_ = (unsigned short)DatetimeType::getStorageSize(startField, endField, fractionPrecision);
  unsigned char *value = value_ = new unsigned char[valueLen_];
  int end = DatetimeType::getExtendedEndField(endField, fractionPrecision);
  for (int field = startField; field <= end; field++) {
    int index = field - REC_DATE_YEAR;
    // This is portable, on endianness
    switch (storageLen[index]) {
      case sizeof(unsigned char): {
        unsigned char v = (unsigned char)values[index];
        memcpy(value, &v, storageLen[index]);
        break;
      }
      case sizeof(unsigned short): {
        unsigned short v = (unsigned short)values[index];
        memcpy(value, &v, storageLen[index]);
        break;
      }
      case sizeof(int):
        memcpy(value, &values[index], storageLen[index]);
        break;
      default:
        assert(FALSE);
    }
    value += storageLen[index];
  }
}  // DatetimeValue::setValue

NAString DatetimeValue::getValueAsString(const DatetimeType &dt) const {
  NAString result;
  if (!isValid()) return result;

  char cbuf[DatetimeType::MAX_FRACTION_PRECISION + 1];  // +1 for sprintf '\0'
  int clen;
  int ulbuf = 0;
  unsigned short usbuf;
  unsigned char ucbuf;
  const unsigned char *value = getValue();
  int end = dt.getExtendedEndField(dt.getEndField(), dt.getFractionPrecision());
  for (int field = dt.getStartField(); field <= end; field++) {
    int index = field - REC_DATE_YEAR;
    if (value - getValue() >= getValueLen()) {
      assert(index == FRACTION);
      ulbuf = 0;  // optional seconds fraction value defaults to zero
    } else {
      // This is portable, on both endianness and alignment of short/long
      switch (storageLen[index]) {
        case sizeof(unsigned char):
          memcpy(&ucbuf, value, storageLen[index]);
          ulbuf = ucbuf;
          break;
        case sizeof(unsigned short):
          memcpy(&usbuf, value, storageLen[index]);
          ulbuf = usbuf;
          break;
        case sizeof(int):
          memcpy(&ulbuf, value, storageLen[index]);
          break;
        default:
          assert(FALSE);
      }
    }
    value += storageLen[index];
    if (index != FRACTION)
      clen = maxFieldLen[index];
    else
      clen = dt.getFractionPrecision();
    if (!result.isNull()) {
      sprintf(cbuf, "%c", precedingPunc[index]);
      result += cbuf;
    }
    sprintf(cbuf, "%0*u", clen, ulbuf);
    result += cbuf;
  }
  return result;
}  // DatetimeValue::getValueAsString

void DatetimeValue::print(const DatetimeType &dt, FILE *ofd, const char *indent) const {
  if (isValid())
    fprintf(ofd, "%s\n", getValueAsString(dt).data());
  else
    fprintf(ofd, "%sInvalid %s\n", indent, dt.getTypeSQLname().data());
}  // DatetimeValue::print

// Decode a Julian timestamp into an array of fields that can be used with
// setValue().
void DatetimeValue::decodeTimestamp(long julianTimestamp, UInt32 fracSec, UInt32 *dtvFields) {
  // Need one extra element in tsFields array; interprettimestamp splits
  // fractional seconds into 2 fields
  short tsFields[N_DATETIME_FIELDS + 1];

  INTERPRETTIMESTAMP(julianTimestamp, tsFields);
  for (int field = YEAR; field <= SECOND; field++) dtvFields[field] = tsFields[field];

  // For fractional seconds, combine milliseconds and microseconds, which are
  // stored in separate fields of the INTERPRETTIMESTAMP output. Scale the result
  // according to the fractional precision of the type.
  dtvFields[FRACTION] = (UInt32)((tsFields[6] * 1000 + tsFields[7]) / (long)pow(10, 6 - MINOF(6, fracSec)));
}

void DatetimeValue::decodeTimestamp(bool useUTCTimestmap, long inTimeVal, bool inMillis, short *tsFields) {
  //
  // The conversion takes the approach of converting to a date in
  // Gregorian calander that returns the value similar to
  // java.util.Date does.
  //
  // Richards' formular to calculate a date/time in either calendars
  // from a value of Julian days is used. The formular can be found at
  // https://en.wikipedia.org/wiki/Julian_day.
  //
  // Some constants used by the method.
  static const int64_t y = 4716;
  static const int64_t j = 1401;
  static const int64_t m = 2;
  static const int64_t n = 12;
  static const int64_t r = 4;
  static const int64_t p = 1461;
  static const int64_t v = 3;
  static const int64_t u = 5;
  static const int64_t s = 153;
  static const int64_t w = 2;
  static const int64_t B = 274277;
  static const int64_t C = -38;

  timespec timeVal;
  long UTC_timeVal;
  if (useUTCTimestmap) {
    if (inMillis)
      UTC_timeVal = inTimeVal / 1000;
    else
      UTC_timeVal = inTimeVal;
  } else {
    // Convert from local time_t to UTC time_t
    // This needs to removed when we can write ORC files in UTC
    time_t localtime;
    if (inMillis)
      localtime = inTimeVal / 1000;
    else
      localtime = inTimeVal;
    struct tm tm_r;
    localtime_r(&localtime, &tm_r);
    UTC_timeVal = timegm(&tm_r);
  }

  timeVal.tv_sec = UTC_timeVal;
  timeVal.tv_nsec = 0;

  long julianTimeVal = ComRtGetJulianFromUTC(timeVal);

  // Julian timestamp in # of days
  int64_t jd = INTERPRETTIMESTAMP(julianTimeVal, tsFields);

  // All steps are the same for either the Gregorian or Julian calendar
  // except the step coded in IF block below.
  int64_t f = jd + j;

  if (jd >= 2299160) {
    // for Gregorian calandar
    f += (((((jd << 2) + B) / 146097) * 3) >> 2) + C;
  }

  int64_t e = r * f + v;
  int64_t g = (e % p) / r;
  int64_t h = u * g + w;

  int64_t month = (char)(((h / s) + m) % n + 1);
  int64_t day = (char)((h % s) / u + 1);
  short year = (e / p) - y + (n + m - month) / n;

  tsFields[YEAR] = year;
  tsFields[MONTH] = month;
  tsFields[DAY] = day;
}

int DatetimeValue::convertToUtcFromDatetime(const char *value, int valueLen, int fractionPrec, time_t &utcTime,
                                            int &nanoSecs) {
  const char *datetimeOpData = value;
  short year;
  char month;
  char day;
  char hour;
  char minute;
  char second;
  int fraction = 0;
  int microSecs = 0;
  int milliSecs;
  str_cpy_all((char *)&year, datetimeOpData, sizeof(year));
  datetimeOpData += sizeof(year);
  month = *datetimeOpData++;
  day = *datetimeOpData++;
  hour = *datetimeOpData++;
  minute = *datetimeOpData++;
  second = *datetimeOpData++;
  if ((datetimeOpData - value) < valueLen) {
    str_cpy_all((char *)&fraction, datetimeOpData, sizeof(fraction));
    if (fractionPrec > 0) {
      if (fractionPrec > DatetimeType::MAX_FRACTION_PRECISION_USEC)
        // Adjust the fractional seconds part to be the number of microseconds
        microSecs = fraction / ((int)pow(10, (fractionPrec - DatetimeType::MAX_FRACTION_PRECISION_USEC)));
      else
        microSecs = fraction / ((int)pow(10, (DatetimeType::MAX_FRACTION_PRECISION_USEC - fractionPrec)));
    }
  } else
    fraction = 0;
  struct tm tmpTm;
  tmpTm.tm_year = year - 1900;
  tmpTm.tm_mon = month - 1;
  tmpTm.tm_mday = day;
  tmpTm.tm_hour = hour;
  tmpTm.tm_min = minute;
  tmpTm.tm_sec = second;
  tmpTm.tm_isdst = -1;
  tmpTm.tm_gmtoff = 0;
  tmpTm.tm_zone = "GMT";
  utcTime = timegm(&tmpTm);
  if (utcTime == -1 && errno > 0) return errno;
  // fixup to convert from prolectic gregorian to hybrid (julian + gregorian that java expects) based on formula in
  // https://en.wikipedia.org/wiki/Gregorian_calendar
  if (utcTime <
      -12219292800) {  // this constant is the equivalent of 1582-10-15 00:00:00, cut off date of Gregorian calandar
    utcTime += 86400 * (year / 100 - year / 400 - 2);  // 86400 is number of seconds in a day formula in parenthesis is
                                                       // "Difference between Gregorian and Julian calendar dates
  }
  nanoSecs = fraction;
  milliSecs = microSecs / 1000;
  utcTime = (utcTime * 1000) + milliSecs;
  return 0;
}

int DatetimeValue::convertToUtcFromDate(const char *value, int valueLen, time_t &utcTime) {
  const char *datetimeOpData = value;
  short year;
  char month;
  char day;
  str_cpy_all((char *)&year, datetimeOpData, sizeof(year));
  datetimeOpData += sizeof(year);
  month = *datetimeOpData++;
  day = *datetimeOpData++;
  struct tm tmpTm;
  tmpTm.tm_year = year - 1900;
  tmpTm.tm_mon = month - 1;
  tmpTm.tm_mday = day;
  tmpTm.tm_hour = 0;
  tmpTm.tm_min = 0;
  tmpTm.tm_sec = 0;
  tmpTm.tm_isdst = -1;
  tmpTm.tm_gmtoff = 0;
  tmpTm.tm_zone = "GMT";
  utcTime = timegm(&tmpTm);
  if (utcTime == -1 && errno > 0) return errno;
  // fixup to convert from prolectic gregorian to hybrid (julian + gregorian that java expects) based on formula in
  // https://en.wikipedia.org/wiki/Gregorian_calendar
  if (utcTime <
      -12219292800) {  // this constant is the equivalent of 1582-10-15 00:00:00, cut off date of Gregorian calandar
    utcTime += 86400 * (year / 100 - year / 400 - 2);  // 86400 is number of seconds in a day formula in parenthesis is
                                                       // "Difference between Gregorian and Julian calendar dates
  }
  utcTime = utcTime / 86400;
  return 0;
}
