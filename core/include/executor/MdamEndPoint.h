
#ifndef MDAMENDPOINT_H
#define MDAMENDPOINT_H
/* -*-C++-*-
********************************************************************************
*
* File:         MdamEndPoint.h
* Description:  MDAM End Point
*
*
* Created:      9/11/96
* Language:     C++
*
*
*
*
********************************************************************************
*/

// -----------------------------------------------------------------------------

#include "MdamInterval.h"
#include "common/NABoolean.h"
#include "executor/MdamEnums.h"

// *****************************************************************************
// MdamEndPoint is wrapper for MdamPoint.  It includes data needed to process
// the point out of the context of an interval.  It has a pointer to the
// MdamPoint it wraps and some additional data items:
//   + inclusion_ (copy of the MamPoint attribute),
//   + endPointType_ (specifies if the point represents the beginning or the
//     end of an interval), and
//   + pointer(s) to the containing interval(s).
// *****************************************************************************

// Forward declarations.
class MdamPoint;
class MdamInterval;
// End of forward declarations.

class MdamEndPoint {
 public:
  // Default constructor.
  MdamEndPoint();

  // This constructor is passed a pointer to the interval from which the
  // MdamPoint is taken.  Point type is also passed to specify which of the
  // two points to use.
  MdamEndPoint(MdamInterval *intervalPtr, MdamEnums::MdamEndPointType endPointType,
               const int logicalIntervalListNumber);

  // Destructor.
  ~MdamEndPoint() {}

  // Adjust the interval pointer to track the active reference list.
  void adjustIntervalPtr(MdamInterval *&intervalPtr, const int logicalIntervalListNumber) const;

  // Assign intervalPtr[1] in this to the corresponding value in other.
  // When this function is used, the mutated MdamEndPoint object
  // represents points from both interval lists.  intervalPtr is the
  // only data member that needs to be transferred.  All others are
  // equal or the points wouldn't have been treated as duplicates.
  inline void assignIntervalPtr1(const MdamEndPoint *other);

  // Determine if the point is a begin endpoint.
  inline NABoolean begin() const;

  // Compare two endpoints.  The function determines if the endpoint pointed
  // to by this is {less than | equal to | greater than} the endpoint pointed
  // to by other.
  MdamEnums::MdamOrder compare(const MdamEndPoint *other, const int keyLen) const;

  // Determine if the point is a end endpoint.
  inline NABoolean end() const;

  // Determine if the point exists.
  inline NABoolean exists() const;

  // Get function for inclusion.
  inline MdamEnums::MdamInclusion getInclusion();

  // Get function for pointPtr_.
  inline const MdamPoint *getPointPtr() const;

  // If an interval were to be formed using this MdamEndPoint as the end endpoint
  // and previousEndPoint MdamEndPoint as the begin endpoint, this function determines
  // if the resulting interval would be empty.
  NABoolean givesNonEmptyInterval(const MdamEndPoint *previousEndPoint, const int keyLen) const;

// Print function.
#ifdef NA_MDAM_EXECUTOR_DEBUG
  void print(const char *header = "") const;
#endif /* NA_MDAM_EXECUTOR_DEBUG */

  // Reset an endpoint to its newly-constructed state.
  void reset();

  // Reverse inclusion.
  void reverseInclusion();

  // Set member data items to reflect the specified endpoint.
  void set(MdamInterval *intervalPtr, MdamEnums::MdamEndPointType endPointType, const int logicalIntervalListNumber);

 private:
  // Pointer to the point for which this is a wrapper.
  const MdamPoint *pointPtr_;

  // Copy of the inclusion attribute in MdamPoint.
  // Defines whether the point is included or excluded.
  MdamEnums::MdamInclusion inclusion_;

  // Defines whether the point specifies the beginning or the end
  // of the interval.
  MdamEnums::MdamEndPointType endPointType_;

  // Pointers to the containing intervals.  There is one for each of the
  // two interval lists. Positions zero and one of the array are used
  // for interval lists logical number zero and one, respectively.
  // A zero value means the endpoint did not come from an interval
  // on the corresponding list.  A non-zero value means the endpoint
  // did come from an interval on the corresponding list and points to the
  // interval.  An endpoint is contained in one or both lists.
  MdamInterval *intervalPtr_[2];

};  // class MdamEndPoint

// *****************************************************************************
// Inline member functions for class MdamEndPoint
// *****************************************************************************

// Assign intervalPtr[1] in this to the corresponding value in other.
inline void MdamEndPoint::assignIntervalPtr1(const MdamEndPoint *other) { intervalPtr_[1] = other->intervalPtr_[1]; }

// Determine if the point is a begin endpoint.
inline NABoolean MdamEndPoint::begin() const { return endPointType_ == MdamEnums::MDAM_BEGIN; }

// Determine if the point is a end endpoint.
inline NABoolean MdamEndPoint::end() const { return endPointType_ == MdamEnums::MDAM_END; }

// Determine if the point exists.  The endpoint exists if pointPtr_
// is non-zero.
inline NABoolean MdamEndPoint::exists() const { return pointPtr_ != 0; }

// Get function for inclusion.
inline MdamEnums::MdamInclusion MdamEndPoint::getInclusion() { return inclusion_; }

// Get function for pointPtr_.
inline const MdamPoint *MdamEndPoint::getPointPtr() const { return pointPtr_; }

#endif /* MDAMENDPOINT_H */
