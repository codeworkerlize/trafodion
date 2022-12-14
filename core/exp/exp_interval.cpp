
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         exp_interval.C
 * Description:  Interval Type
 *
 *
 * Created:      8/20/96
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "exp/exp_interval.h"

#include "common/Platform.h"

// ***********************************************************************
//
// getIntervalStartField returns the start field for a given interval
// FS datatype.  If the given FS datatype is invalid, the routine returns
// a status of -1.  Otherwise, it returns a status of 0.
//
// ***********************************************************************

short ExpInterval::getIntervalStartField(int fsDatatype, rec_datetime_field &startField) {
  switch (fsDatatype) {
    case REC_INT_YEAR:
    case REC_INT_YEAR_MONTH:
      startField = REC_DATE_YEAR;
      break;
    case REC_INT_MONTH:
      startField = REC_DATE_MONTH;
      break;
    case REC_INT_DAY:
    case REC_INT_DAY_HOUR:
    case REC_INT_DAY_MINUTE:
    case REC_INT_DAY_SECOND:
      startField = REC_DATE_DAY;
      break;
    case REC_INT_HOUR:
    case REC_INT_HOUR_MINUTE:
    case REC_INT_HOUR_SECOND:
      startField = REC_DATE_HOUR;
      break;
    case REC_INT_MINUTE:
    case REC_INT_MINUTE_SECOND:
      startField = REC_DATE_MINUTE;
      break;
    case REC_INT_SECOND:
      startField = REC_DATE_SECOND;
      break;
    default:
      return -1;
  }
  return 0;
}  // getIntervalStartField

// ***********************************************************************
//
// getIntervalEndField returns the end field for a given interval
// FS datatype.  If the given FS datatype is invalid, the routine returns
// a status of -1.  Otherwise, it returns a status of 0.
//
// ***********************************************************************

short ExpInterval::getIntervalEndField(int fsDatatype, rec_datetime_field &endField) {
  switch (fsDatatype) {
    case REC_INT_YEAR:
      endField = REC_DATE_YEAR;
      break;
    case REC_INT_MONTH:
    case REC_INT_YEAR_MONTH:
      endField = REC_DATE_MONTH;
      break;
    case REC_INT_DAY:
      endField = REC_DATE_DAY;
      break;
    case REC_INT_HOUR:
    case REC_INT_DAY_HOUR:
      endField = REC_DATE_HOUR;
      break;
    case REC_INT_MINUTE:
    case REC_INT_HOUR_MINUTE:
    case REC_INT_DAY_MINUTE:
      endField = REC_DATE_MINUTE;
      break;
    case REC_INT_SECOND:
    case REC_INT_MINUTE_SECOND:
    case REC_INT_HOUR_SECOND:
    case REC_INT_DAY_SECOND:
      endField = REC_DATE_SECOND;
      break;
    default:
      return -1;
  }
  return 0;
}  // getIntervalEndField

static const int IntervalFieldStringSize = 3;  // punc plus two digits

int ExpInterval::getDisplaySize(int fsDatatype, short leadingPrecision, short fractionPrecision) {
  size_t result;

  rec_datetime_field startField, endField;

  getIntervalStartField(fsDatatype, startField);
  getIntervalEndField(fsDatatype, endField);

  // 1 for sign
  if (startField == REC_DATE_FRACTION_MP) {
    result = 1 + leadingPrecision;
  } else {
    result = 1 + leadingPrecision + IntervalFieldStringSize * (endField - startField);
    if (fractionPrecision) result += fractionPrecision + 1;  // 1 for "."
  }
  return result;
}
