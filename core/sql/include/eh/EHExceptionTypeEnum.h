
#ifndef EHEXCEPTIONTYPEENUM_H
#define EHEXCEPTIONTYPEENUM_H
/* -*-C++-*-
******************************************************************************
*
* File:         EHExceptionTypeEnum.h
* Description:  An enumeration type for the different exceptions
*
*
* Created:      5/16/95
* Language:     C++
*
*
*
*
******************************************************************************
*/

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
// enum EHExceptionTypeEnum

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None

// -----------------------------------------------------------------------
// enumeration type for the different exceptions
// -----------------------------------------------------------------------
enum EHExceptionTypeEnum {
  EH_NORMAL = 0,

  EH_ARITHMETIC_OVERFLOW,
  EH_OUT_OF_RANGE,
  EH_BREAK_EXCEPTION,

  EH_PROCESSING_EXCEPTION,
  EH_IO_EXCEPTION,
  EH_INTERNAL_EXCEPTION,

  EH_WPC_FAILED_ON_PASS1,
  EH_WPC_FAILED_ON_PASS2,

  EH_ALL_EXCEPTIONS,  // Not intended to be thrown,
                      // just caught

  EH_LAST_EXCEPTION_TYPE_ENUM
};

#endif  // EHEXCEPTIONTYPEENUM_H
