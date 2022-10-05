
#ifndef DTICOMMONTYPE_H
#define DTICOMMONTYPE_H
/* -*-C++-*-
**************************************************************************
*
* File:         DTICommonType.h
* Description:  Common interface to Datetime and Interface types
* Created:      06/13/96
* Language:     C++
*
*
**************************************************************************
*/

// -----------------------------------------------------------------------

#include "common/NAType.h"
#include "common/NABitVector.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class DatetimeIntervalCommonType;

// ***********************************************************************
//
//  DatetimeIntervalCommonType : Ancestor class to DatetimeType & IntervalType
//
// ***********************************************************************
class DatetimeIntervalCommonType : public NAType {
 public:
  enum DTIFlags { UNSUPPORTED_DDL_DATA_TYPE = 1 };

  DatetimeIntervalCommonType(NAMemory *h, const NAString &adtName, NABuiltInTypeEnum typeEnum, int storageSize,
                             NABoolean allowSQLnull, rec_datetime_field startField, rec_datetime_field endField,
                             UInt32 fractionPrecision, int dataAlignment = 1 /* no data alignment */)
      : NAType(h, adtName, typeEnum, storageSize, allowSQLnull, SQL_NULL_HDR_SIZE, FALSE, /* fixed length */
               0,                                                                         /* length header size */
               dataAlignment),
        startField_(startField),
        endField_(endField),
        fractionPrecision_(fractionPrecision) {
    // clear flags
    DTIFlags_.clear();
  }

  // copy ctor
  DatetimeIntervalCommonType(const DatetimeIntervalCommonType &rhs, NAMemory *h = 0)
      : NAType(rhs, h),
        startField_(rhs.startField_),
        endField_(rhs.endField_),
        fractionPrecision_(rhs.fractionPrecision_) {
    // clear flags
    DTIFlags_.clear();
  }

  // ---------------------------------------------------------------------
  // Accessor functions
  // ---------------------------------------------------------------------

  rec_datetime_field getStartField() const { return startField_; }
  rec_datetime_field getEndField() const { return endField_; }
  UInt32 getFractionPrecision() const { return fractionPrecision_; }

  virtual UInt32 getLeadingPrecision() const { return 0; }

  virtual int getScale() const { return (int)fractionPrecision_; }

  // Used by IntervalType, other print/debug/naming functions
  static const char *getFieldName(rec_datetime_field field);

  inline NABoolean getDTIFlag(CollIndex flag) { return DTIFlags_.contains(flag); }

  // ---------------------------------------------------------------------
  // Mutator functions
  // ---------------------------------------------------------------------

  inline void setDTIFlag(CollIndex flag) { DTIFlags_.insert(flag); }

  // ---------------------------------------------------------------------
  // A method which tells if a conversion error can occur when converting
  // a value of this type to the target type.
  // ---------------------------------------------------------------------
  NABoolean errorsCanOccur(const NAType &target, NABoolean lax = TRUE) const;

 protected:
  // ---------------------------------------------------------------------
  // Start datetime field (REC_DATE_YEAR...REC_DATE_SECOND).
  // ---------------------------------------------------------------------
  rec_datetime_field startField_;

  // ---------------------------------------------------------------------
  // End datetime field (REC_DATE_YEAR...REC_DATE_SECOND).
  // ---------------------------------------------------------------------
  rec_datetime_field endField_;

  // ---------------------------------------------------------------------
  // Fraction precision (0...MAX_FRACTION_PRECISION, 0 if end field is not
  // REC_DATE_SECOND).
  // ---------------------------------------------------------------------
  unsigned short fractionPrecision_;

 private:
  // ---------------------------------------------------------------------
  // Bit vector to contain flags (unsupportedDDLColDefDataType
  // ---------------------------------------------------------------------
  NABitVector DTIFlags_;

};  // class DatetimeIntervalCommonType

#endif /* DTICOMMONTYPE_H */
