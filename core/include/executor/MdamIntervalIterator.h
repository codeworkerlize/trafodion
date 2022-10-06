
#ifndef MDAMINTERVALITERATOR_H
#define MDAMINTERVALITERATOR_H
/* -*-C++-*-
********************************************************************************
*
* File:         MdamIntervalIterator.h
* Description:  MDAM Interval Iterator
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

#include "MdamEndPoint.h"
#include "MdamIntervalList.h"
#include "MdamIntervalListIterator.h"
#include "executor/MdamEnums.h"

// *****************************************************************************
// MdamIntervalIterator : MDAM Interval Iterator
//
// This iterator returns the next endpoint on each call.  Within an interval
// it returns the begin endpoint followed by the end endpoint. Then it
// continues with the next interval in an interval list until the interval
// list is exhausted.
// So, this iterator makes an interval list appear to be an ordered set of
// endpoints and presents the endpoints one at a time.
// *****************************************************************************

class MdamIntervalIterator {
 public:
  // Constructor.
  MdamIntervalIterator(const MdamIntervalList *intervalListPtr, const int logicalIntervalListNumber);

  // Destructor.
  ~MdamIntervalIterator();

  // Iteration operator returns the next endpoint on each call.
  NABoolean operator()(MdamEndPoint &mdamEndPoint);

 private:
  // Logical interval list number.
  const int logicalIntervalListNumber_;

  // Pointer to the current interval.
  MdamInterval *intervalPtr_;

  // endPointType_ is used to keep track of which of the two endpoints
  // is the current one for iteration purposes.
  MdamEnums::MdamEndPointType endPointType_;

  // Iterator to return intervals from the interval list.
  MdamIntervalListIterator intervalListIterator_;

};  // class MdamIntervalIterator

#endif /* MDAMINTERVALITERATOR_H */
