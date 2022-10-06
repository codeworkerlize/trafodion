
/* -*-C++-*-
**************************************************************************
*
* File:         DTICommonType.C
* Description:  Common interface to Datetime and Interface types
* Created:      06/13/96
* Language:     C++
*
*
**************************************************************************
*/

// -----------------------------------------------------------------------

#include "DTICommonType.h"

#include "common/DatetimeType.h"
#include "common/IntervalType.h"

// ***********************************************************************
//
//  DatetimeIntervalCommonType : static methods
//
// ***********************************************************************

const char *DatetimeIntervalCommonType::getFieldName(rec_datetime_field field) {
  switch (field) {
    case REC_DATE_YEAR:
      return "YEAR";
    case REC_DATE_MONTH:
      return "MONTH";
    case REC_DATE_DAY:
      return "DAY";
    case REC_DATE_HOUR:
      return "HOUR";
    case REC_DATE_MINUTE:
      return "MINUTE";
    case REC_DATE_SECOND:
      return "SECOND";
    case REC_DATE_FRACTION_MP:
      return "FRACTION";
    case REC_DATE_YEARQUARTER_EXTRACT:
      return "YEARQUARTER";
    case REC_DATE_YEARMONTH_EXTRACT:
      return "YEARMONTH";
    case REC_DATE_YEARWEEK_EXTRACT:
      return "YEARWEEK";
    case REC_DATE_YEARQUARTER_D_EXTRACT:
      return "YEARQUARTERD";
    case REC_DATE_YEARMONTH_D_EXTRACT:
      return "YEARMONTHD";
    case REC_DATE_YEARWEEK_D_EXTRACT:
      return "YEARWEEKD";
    case REC_DATE_CENTURY:
      return "CENTURY";
    case REC_DATE_DECADE:
      return "DECADE";
    case REC_DATE_WEEK:
      return "WEEK";
    case REC_DATE_QUARTER:
      return "QUARTER";
    case REC_DATE_EPOCH:
      return "EPOCH";
    case REC_DATE_DOW:
      return "DOW";
    case REC_DATE_DOY:
      return "DOY";
    case REC_DATE_WOM:
      return "WOM";
    default:
      return NULL;
  }
}  // DatetimeIntervalCommonType::getFieldName

// ---------------------------------------------------------------------
// A method which tells if a conversion error can occur when converting
// a value of this type to the target type.
// ---------------------------------------------------------------------
NABoolean DatetimeIntervalCommonType::errorsCanOccur(const NAType &target, NABoolean lax) const {
  if (NAType::errorsCanOccur(target)) {
    return TRUE;
  }

  if (target.getTypeQualifier() == NA_DATETIME_TYPE) {
    if (lax) {
      // datetimes that are compared in a bi-relational predicate
      // are always the same. Binder has already checked for this.
      return FALSE;
    } else {  // lax is FALSE
      // no error can occur if datatypes are the same
      if ((const DatetimeType &)target == (*this)) return FALSE;
    }
  } else {
    // Interval type
    const IntervalType &intervalTarget = (const IntervalType &)target;
    if (*this == intervalTarget) return FALSE;  //  no error can occur if datatypes are the same
  }
  return TRUE;
}
