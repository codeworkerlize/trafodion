
/* -*-C++-*-
********************************************************************************
*
* File:         MdamEndPoint.C
* Description:  Implimentation for MDAM End Point
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
#include <iostream>
#endif /* NA_MDAM_EXECUTOR_DEBUG */
#include "MdamEndPoint.h"

// *****************************************************************************
// Member functions for class MdamEndPoint
// *****************************************************************************

// Default constructor.
MdamEndPoint::MdamEndPoint()
    : pointPtr_(0), inclusion_(MdamEnums::MDAM_EXCLUDED), endPointType_(MdamEnums::MDAM_BEGIN) {
  intervalPtr_[0] = 0;
  intervalPtr_[1] = 0;
}

// This constructor is possibly not used.  $$$$
// This constructor is passed a pointer to the interval from which the
// MdamPoint is taken.  Point type is also passed to specify which of the
// two points to use.
MdamEndPoint::MdamEndPoint(MdamInterval *intervalPtr, MdamEnums::MdamEndPointType endPointType,
                           const int logicalIntervalListNumber)
    : pointPtr_(intervalPtr->getPointPtr(endPointType)),
      inclusion_(pointPtr_->getInclusion()),
      endPointType_(endPointType) {
  intervalPtr_[0] = 0;
  intervalPtr_[1] = 0;
  intervalPtr_[logicalIntervalListNumber] = intervalPtr;
}

// Adjust the interval pointer to track the active reference list.
void MdamEndPoint::adjustIntervalPtr(MdamInterval *&intervalPtr, const int logicalIntervalListNumber) const {
  // Check if the endpoint came from the interval list specified by
  // logicalIntervalListNumber.  If not, do nothing.
  if (intervalPtr_[logicalIntervalListNumber] != 0) {
    if (begin()) {
      // A begin endpoint establishes an active reference list.
      intervalPtr = intervalPtr_[logicalIntervalListNumber];
    } else {
      // An end endpoint clears the active reference list.
      intervalPtr = 0;
    };
  };
}

MdamEnums::MdamOrder MdamEndPoint::compare(const MdamEndPoint *other, const int keyLen) const {
  MdamEnums::MdamOrder tempOrderResult = pointPtr_->compare(other->pointPtr_, keyLen);
  if (tempOrderResult != MdamEnums::MDAM_EQUAL) {
    return tempOrderResult;
  }
  // Values are equal so consider endpoint type and inclusion.
  // x), [x, x], (x map to rank 0, 1, 2, 3, respectively.
  static const int rankArray[2][2] = {3, 1, 0, 2};
  int rankThis = rankArray[endPointType_][inclusion_];
  int rankOther = rankArray[other->endPointType_][other->inclusion_];
  if (rankThis < rankOther) return MdamEnums::MDAM_LESS;
  if (rankThis > rankOther) return MdamEnums::MDAM_GREATER;
  return MdamEnums::MDAM_EQUAL;
}

// If an interval were to be formed using this MdamEndPoint as the end endpoint
// and previousEndPoint MdamEndPoint as the begin endpoint, this function determines
// if the resulting interval would be empty.
NABoolean MdamEndPoint::givesNonEmptyInterval(const MdamEndPoint *previousEndPoint, const int keyLen) const {
  // Genesis case 10-980205-3598: changed test to consider value but
  // not inclusion nor endPointType.
  if (pointPtr_->compare(previousEndPoint->pointPtr_, keyLen) != MdamEnums::MDAM_EQUAL ||
      ((previousEndPoint->inclusion_ == MdamEnums::MDAM_INCLUDED) &&
       (
           // Current endpoint is an end endpoint and included, that is, "]".
           (endPointType_ == MdamEnums::MDAM_END && inclusion_ == MdamEnums::MDAM_INCLUDED) ||
           // Current endpoint is a begin endpoint and excluded, that is, "(".
           (endPointType_ == MdamEnums::MDAM_BEGIN && inclusion_ == MdamEnums::MDAM_EXCLUDED)))) {
    return TRUE;
  };

  return FALSE;
}

// Print function.
#ifdef NA_MDAM_EXECUTOR_DEBUG
void MdamEndPoint::print(const char *header) const {
  cout << header << endl;
  pointPtr_->print("point data...");
  cout << "  Inclusion: " << (inclusion_ ? "INCLUDED" : "EXCLUDED") << endl;
  cout << "  End Point Type: " << (endPointType_ ? "END" : "BEGIN") << endl;
  cout << "  Interval pointer 0: " << (void *)intervalPtr_[0] << endl;
  cout << "  Interval pointer 1: " << (void *)intervalPtr_[1] << endl;
}
#endif /* NA_MDAM_EXECUTOR_DEBUG */

// Reset an endpoint's data members to reflect a non-existent point.
// (An endpoint is considered non-existent if pointPtr_ is zero.)
// State is the same as if the endpoint was newly created by the default
// constructor.
void MdamEndPoint::reset() {
  pointPtr_ = 0;
  inclusion_ = MdamEnums::MDAM_EXCLUDED;
  endPointType_ = MdamEnums::MDAM_BEGIN;
  intervalPtr_[0] = 0;
  intervalPtr_[1] = 0;
}

// Reverse inclusion.
void MdamEndPoint::reverseInclusion() {
  if (inclusion_ == MdamEnums::MDAM_INCLUDED) {
    inclusion_ = MdamEnums::MDAM_EXCLUDED;
  } else {
    inclusion_ = MdamEnums::MDAM_INCLUDED;
  };
}

// Set member data items to reflect the specified endpoint.  (The
// point is specified by the pointer to the interval containing the point
// and the endpoint type to indicate one of the two points within.)
// State is the same as if the endpoint was newly-created by the constructor
// that accepts the same arguements.
void MdamEndPoint::set(MdamInterval *intervalPtr, MdamEnums::MdamEndPointType endPointType,
                       const int logicalIntervalListNumber) {
  pointPtr_ = intervalPtr->getPointPtr(endPointType);
  inclusion_ = pointPtr_->getInclusion();
  endPointType_ = endPointType;
  intervalPtr_[0] = 0;
  intervalPtr_[1] = 0;
  intervalPtr_[logicalIntervalListNumber] = intervalPtr;
}
