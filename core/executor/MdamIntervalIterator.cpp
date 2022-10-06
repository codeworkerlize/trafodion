
/* -*-C++-*-
********************************************************************************
*
* File:         MdamIntervalIterator.C
* Description:  Implimentation for MDAM Interval Iterator
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

#include "MdamIntervalIterator.h"

#include "MdamEndPoint.h"
#include "executor/ex_stdh.h"

// *****************************************************************************
// Member functions for class MdamIntervalIterator
// *****************************************************************************

// Constructor.
MdamIntervalIterator::MdamIntervalIterator(const MdamIntervalList *intervalListPtr, const int logicalIntervalListNumber)
    : logicalIntervalListNumber_(logicalIntervalListNumber),
      intervalPtr_(0),
      endPointType_(MdamEnums::MDAM_BEGIN),
      intervalListIterator_(*intervalListPtr) {
  intervalPtr_ = intervalListIterator_();  // Get the first interval.
}

// Destructor.
MdamIntervalIterator::~MdamIntervalIterator() {}

// Iteration operator returns the next endpoint on each call.
NABoolean MdamIntervalIterator::operator()(MdamEndPoint &mdamEndPointRef) {
  if (intervalPtr_ == 0)  // No intervals remaining.
  {
    mdamEndPointRef.reset();
    return FALSE;
  };
  mdamEndPointRef.set(intervalPtr_, endPointType_, logicalIntervalListNumber_);
  if (endPointType_ == MdamEnums::MDAM_BEGIN) {
    endPointType_ = MdamEnums::MDAM_END;  // Advance to end endpoint.
  } else                                  // Must be that endPointType_ == MdamEnums::MDAM_END.
  {
    intervalPtr_ = intervalListIterator_();  // Advance to the next interval.
    endPointType_ = MdamEnums::MDAM_BEGIN;   // Reset to the begin endpoint.
  };
  return TRUE;
}
