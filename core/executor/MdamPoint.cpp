
/* -*-C++-*-
********************************************************************************
*
* File:         MdamPoint.C
* Description:  Implimentation for MDAM Point
*
*
* Created:      9/12/96
* Language:     C++
*
*
*
*
********************************************************************************
*/

// -----------------------------------------------------------------------------

#include "executor/ex_stdh.h"
#ifdef NA_MDAM_EXECUTOR_DEBUG
#include <cctype>
#include <cstdio>
#include <iostream>
#endif /* NA_MDAM_EXECUTOR_DEBUG */
#include "MdamEndPoint.h"
#include "MdamPoint.h"
#include "common/NABoolean.h"
#include "common/str.h"

// *****************************************************************************
// Member functions for class MdamPoint
// *****************************************************************************

// Constructor that accepts an MdamEndPoint.
MdamPoint::MdamPoint(MdamEndPoint &endPointRef)
    : tupp_(endPointRef.getPointPtr()->tupp_), inclusion_(endPointRef.getInclusion()) {}

// Determine if v could be within an interval for which this MdamPoint
// is the begin endpoint.
NABoolean MdamPoint::beginContains(const int keyLen, const char *v) const {
  int cmpCode = str_cmp(tupp_.getDataPointer(), v, int(keyLen));
  MdamEnums::MdamOrder tempOrder = MdamEnums::MdamOrder((cmpCode > 0) ? 1 : ((cmpCode == 0) ? 0 : -1));
  if (tempOrder == MdamEnums::MDAM_LESS) {
    return TRUE;
  } else if (tempOrder == MdamEnums::MDAM_GREATER) {
    return FALSE;
  }
  // The two values are equal so consider inclusion.
  else if (included()) {
    return TRUE;
  } else {
    return FALSE;
  };
  return TRUE;  // To get rid of compiler warning.
}

// Determine if v could be within an interval for which this MdamPoint
// is the end endpoint.
NABoolean MdamPoint::endContains(const int keyLen, const char *v) const {
  int cmpCode = str_cmp(tupp_.getDataPointer(), v, int(keyLen));
  MdamEnums::MdamOrder tempOrder = MdamEnums::MdamOrder((cmpCode > 0) ? 1 : ((cmpCode == 0) ? 0 : -1));
  if (tempOrder == MdamEnums::MDAM_LESS) {
    return FALSE;
  } else if (tempOrder == MdamEnums::MDAM_GREATER) {
    return TRUE;
  }
  // The two values are equal so consider inclusion.
  else if (included()) {
    return TRUE;
  } else {
    return FALSE;
  };
  return TRUE;  // To get rid of compiler warning.
}

// Print functions.
#ifdef NA_MDAM_EXECUTOR_DEBUG
void MdamPoint::print(const char *header) const {
  cout << header << endl;
  char *dataPointer = getDataPointer();
  int keyLen = tupp_.getAllocatedSize();
  cout << "  Key length: " << keyLen << endl;
  cout << "  Data pointer: " << (void *)dataPointer << endl;
  cout << "  Inclusion: " << (inclusion_ ? "INCLUDED" : "EXCLUDED") << endl;
}

// void printBrief(char* dataPointer, int keyLen);

void MdamPoint::printBrief() const {
  char *dataPointer = getDataPointer();
  int keyLen = tupp_.getAllocatedSize();

  ::printBrief(dataPointer, keyLen, FALSE /* no end of line wanted */);
}
#endif
