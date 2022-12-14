/* -*-C++-*-
********************************************************************************
*
* File:         MdamRefListIterator.C
* Description:  Implimentation for MDAM Reference List Iterator
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

#include "MdamRefListIterator.h"

#include "common/NABoolean.h"
#include "executor/ex_stdh.h"

// *****************************************************************************
// Member functions for class MdamRefListIterator
// *****************************************************************************

// Constructor.
MdamRefListIterator::MdamRefListIterator(const MdamRefList *RefListPtr)
    : currentListPtr_(RefListPtr), currentEntryPtr_(0) {
  // Set currentEntryPtr_ to point to the first node.
  if (currentListPtr_->lastEntryPtr_ != 0)  // Non-empty list?
  {
    currentEntryPtr_ = currentListPtr_->lastEntryPtr_->getNextEntryPtr();
  }
}

// Iteration operator returns a boolean that indicates if there was or
// was not a "next" entry and the corresponding disjunct number.
// When the function returns, currentEntryPtr_ points to the next
// unprocessed entry, if any.
NABoolean MdamRefListIterator::operator()(int &disjunctNum) {
  if (currentEntryPtr_ == 0) {
    return FALSE;
  }
  disjunctNum = currentEntryPtr_->getDisjunctNum();
  if (currentEntryPtr_ == currentListPtr_->lastEntryPtr_) {
    currentEntryPtr_ = 0;
  } else {
    currentEntryPtr_ = currentEntryPtr_->getNextEntryPtr();
  }
  return TRUE;
}

// Iteration operator returns a pointer to the next entry.
// When the function returns, currentEntryPtr_ points to the
// reference list entry beyond the one returned.
// So, it is safe for a user of this iterator to delete the returned object.
MdamRefListEntry *MdamRefListIterator::operator()() {
  MdamRefListEntry *tempEntryPtr = currentEntryPtr_;
  if (currentEntryPtr_ != 0) {
    if (currentEntryPtr_ == currentListPtr_->lastEntryPtr_) {
      currentEntryPtr_ = 0;
    } else {
      currentEntryPtr_ = currentEntryPtr_->getNextEntryPtr();
    }
  }
  return tempEntryPtr;
}
