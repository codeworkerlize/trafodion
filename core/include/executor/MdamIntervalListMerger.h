
#ifndef MDAMINTERVALLISTMERGER_H
#define MDAMINTERVALLISTMERGER_H
/* -*-C++-*-
********************************************************************************
*
* File:         MdamIntervalListMerger.h
* Description:  MDAM Interval List Merger
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
#include "MdamIntervalIterator.h"
#include "MdamIntervalList.h"
#include "executor/MdamEnums.h"

// *****************************************************************************
// MdamIntervalListMerger : MDAM Interval List Merger
// The iteration operator in this class merges two interval lists.
// It eliminates duplicate points in the process.
// If a point appears as both a begin and an end point,
// it is returned twice.  As each point is returned, state is updated:
//   + number of intervals currently active,
//   + pointers to intervals from which the end points came.
// The two interval lists are identified by logical numbers 0 and 1.
// *****************************************************************************

class MdamIntervalListMerger {
 public:
  // Constructor.
  MdamIntervalListMerger(const MdamIntervalList *intervalListPtr0, const MdamIntervalList *intervalListPtr1,
                         const int keyLen)
      : intervalIterator0_(intervalListPtr0, 0),
        intervalIterator1_(intervalListPtr1, 1),
        activeIntervals_(0),
        previousActiveIntervals_(0),
        endPoint0_(),
        endPoint1_(),
        keyLen_(keyLen) {
    // Get the first endpoint from each of the two interval lists.
    intervalIterator0_(endPoint0_);
    intervalIterator1_(endPoint1_);
  }

  // Destructor.
  ~MdamIntervalListMerger();

  // Iteration operator returns the next endpoint on each call.
  NABoolean operator()(MdamEndPoint &mdamEndPoint);

  // Get the number of active intervals for the most recent iteration call.
  inline int getActiveIntervals() const;

  // Get the number of active intervals for the previous iteration call.
  inline int getPreviousActiveIntervals() const;

 private:
  // Iterators for the two interval lists being merged.
  MdamIntervalIterator intervalIterator0_;
  MdamIntervalIterator intervalIterator1_;

  // Number of intervals currently active.  Possible values are zero to two,
  // inclusive.
  int activeIntervals_;

  // Number of active intervals on the previous call.
  int previousActiveIntervals_;

  // End points.  One from each of the two lists being merged.
  MdamEndPoint endPoint0_;
  MdamEndPoint endPoint1_;

  // Key length.
  int keyLen_;

};  // class MdamIntervalListMerger

// *****************************************************************************
// Inline member functions for class MdamIntervalListMerger
// *****************************************************************************

// Get the number of active intervals for the most recent iteration call.
inline int MdamIntervalListMerger::getActiveIntervals() const { return activeIntervals_; }

// Get the number of active intervals for the previous iteration call.
inline int MdamIntervalListMerger::getPreviousActiveIntervals() const { return previousActiveIntervals_; }

#endif /* MDAMINTERVALLISTMERGER_H */
