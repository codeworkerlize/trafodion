
#ifndef MDAMINTERVAL_H
#define MDAMINTERVAL_H
/* -*-C++-*-
********************************************************************************
*
* File:         MdamInterval.h
* Description:  MDAM Interval
*
*
* Created:      9/11/96
* Language:     C++
*
*
********************************************************************************
*/

// -----------------------------------------------------------------------------

#include "FixedSizeHeapManager.h"
#include "MdamPoint.h"
#include "MdamRefList.h"
#include "common/NABoolean.h"
#include "executor/MdamEnums.h"

// Forward references...
class MdamEndPoint;
class tupp;

// *****************************************************************************
// MdamInterval represents a range of values from beginPoint_ to endPoint_.
// An invarient relation is that beginPoint_ <= endPoint_.  Associated with
// an interval is a reference list.  It specifies the disjuncts the interval
// satisfies.
// *****************************************************************************
class MdamInterval {
  friend class MdamIntervalIterator;

 public:
  // Constructor.
  // $$$$ Possibly testing only.
  inline MdamInterval(const tupp &beginTupp, const MdamEnums::MdamInclusion beginInclusion, const tupp &endTupp,
                      const MdamEnums::MdamInclusion endInclusion, const int disjunctNum,
                      FixedSizeHeapManager &mdamRefListEntryHeap);

  // Constructor without a disjunctNum.  Building of MdamRefList is deferred.
  MdamInterval(const tupp &beginTupp, const MdamEnums::MdamInclusion beginInclusion, const tupp &endTupp,
               const MdamEnums::MdamInclusion endInclusion);

  // Constructor without a disjunctNum.  Building of MdamRefList is deferred.
  MdamInterval(MdamEndPoint &beginEndPoint, MdamEndPoint &endEndPoint);

  // This constructor meets the special requirements of
  // MdamIntervalList::unionSeparateDisjuncts.  These requirements are:
  //  + reverse the inclusion of endEndPoint if it is of type BEGIN, and
  //  + build a reference list.
  MdamInterval(MdamEndPoint &beginEndPoint, MdamEndPoint &endEndPoint, MdamInterval *intervalPtr0,
               MdamInterval *intervalPtr1, const int disjunctNum, FixedSizeHeapManager &mdamRefListEntryHeap);

  // Destructor.
  ~MdamInterval();

  // Operator new.
  inline void *operator new(size_t size, FixedSizeHeapManager &mdamIntervalHeap);

  // Operator new with just size_t.  This should never be called.
  void *operator new(size_t size);

  // Operator delete.  This should never be called.
  void operator delete(void *);

  // Determines if a value is contained within this interval.
  NABoolean contains(const int keyLen, const char *v) const;

  // Create the Reference list for an interval.  The new reference list is
  // a copy of the reference list associated with interval1Ptr.  disjunctNum
  // is inserted into the new list if the interval pointed to by interval2Ptr
  // is active.
  void createRefList(const MdamInterval *interval1Ptr, const MdamInterval *interval2Ptr, const int disjunctNum,
                     FixedSizeHeapManager &mdamRefListEntryHeap);

  // Get function that obtains the first value in an interval.
  // The value is stored at the location specified by s.
  // If the operation is successful, the function returns true.
  // Otherwise, it returns false.  Failure can occur if the begin
  // end point is excluded and incrementing the value results
  // in an error or a value that is outside the interval.
  NABoolean getFirstValue(const int keyLen, char *s) const;

  // Const function to get nextMdamIntervalPtr_.
  inline MdamInterval *getNextMdamIntervalPtr() const;

  // Get function that obtains the next value in an interval.  The
  // value is stored at the location specified by s.  If the operation
  // is successful, the function returns true.  Otherwise, it returns
  // false.  Failure can occur if incrementing the value results in an
  // error or a value that is outside the interval.
  NABoolean getNextValue(const int keyLen, char *s) const;

  // Const function to get the address of the beginning or ending point.
  MdamPoint *getPointPtr(const MdamEnums::MdamEndPointType endPointType);

  // Function to get the address of the MdamRefList.
  inline MdamRefList *getRefListPtr();

  // This function inserts a single disjunct number into the reference list
  // associated with this MdamInterval.
  inline void insertDisjunctNum(const int disjunctNum, FixedSizeHeapManager &mdamRefListEntryHeap);

  // Release resources associated with this interval.
  // This is used prior to returning the interval to the free list.
  inline void release(FixedSizeHeapManager &mdamRefListEntryHeap);

  // Release reference list entries associated with this interval.
  inline void releaseRefListEntries(FixedSizeHeapManager &mdamRefListEntryHeap);

  // Release tupp storage associated with this interval.
  inline void releaseTupps();

// Print functions.
#ifdef NA_MDAM_EXECUTOR_DEBUG
  void print(const char *header = "") const;
  void printBrief() const;
#endif /* NA_MDAM_EXECUTOR_DEBUG */

  // Mutator function to set nextMdamIntervalPtr_.
  void setNextMdamIntervalPtr(MdamInterval *nextMdamIntervalPtr);

 private:
  // MdamPoint that marks the beginning of the interval.
  MdamPoint beginPoint_;

  // MdamPoint that marks the end of the interval.
  MdamPoint endPoint_;

  // MdamRefList associated with the interval.
  MdamRefList mdamRefList_;

  // This forward pointer is used to form a singlely-linked list of intervals.
  MdamInterval *nextMdamIntervalPtr_;

};  // class MdamInterval

// *****************************************************************************
// Inline member functions for class MdamInterval
// *****************************************************************************

// Constructor.
inline MdamInterval::MdamInterval(const tupp &beginTupp, const MdamEnums::MdamInclusion beginInclusion,
                                  const tupp &endTupp, const MdamEnums::MdamInclusion endInclusion,
                                  const int disjunctNum, FixedSizeHeapManager &mdamRefListEntryHeap)
    : beginPoint_(beginTupp, beginInclusion),
      endPoint_(endTupp, endInclusion),
      mdamRefList_(disjunctNum, mdamRefListEntryHeap),
      nextMdamIntervalPtr_(0) {}

// Operator new.
inline void *MdamInterval::operator new(size_t size, FixedSizeHeapManager &mdamIntervalHeap) {
  return mdamIntervalHeap.allocateElement(size);
}

// Function to get the address of the MdamRefList.
inline MdamRefList *MdamInterval::getRefListPtr() { return &mdamRefList_; }

// This function inserts a single disjunct number into the reference list
// associated with this MdamInterval.
inline void MdamInterval::insertDisjunctNum(const int disjunctNum, FixedSizeHeapManager &mdamRefListEntryHeap) {
  mdamRefList_.insert(disjunctNum, mdamRefListEntryHeap);
}

// Const function to get nextMdamIntervalPtr_.
inline MdamInterval *MdamInterval::getNextMdamIntervalPtr() const { return nextMdamIntervalPtr_; }

// Release resources associated with this interval.
// This is used prior to returning the interval to the free list.
inline void MdamInterval::release(FixedSizeHeapManager &mdamRefListEntryHeap) {
  releaseTupps();
  releaseRefListEntries(mdamRefListEntryHeap);
}

// Release reference list entries associated with this interval.
inline void MdamInterval::releaseRefListEntries(FixedSizeHeapManager &mdamRefListEntryHeap) {
  mdamRefList_.deleteEntries(mdamRefListEntryHeap);
}

// Release tupp storage associated with this interval.
inline void MdamInterval::releaseTupps() {
  beginPoint_.release();
  endPoint_.release();
}

#endif /* MDAMINTERVAL_H */
