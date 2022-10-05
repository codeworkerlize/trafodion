/* -*-C++-*-
********************************************************************************
*
* File:         MdamRefListEntry.C
* Description:  Implementation for MDAM Reference List Entry
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
#include "MdamRefListEntry.h"

// *****************************************************************************
// Member functions for class MdamRefListEntry
// *****************************************************************************

// Position the beforePtr to prepare for an insertion.
MdamRefListEntry *MdamRefListEntry::positionBeforePtr(const int disjunctNum) {
  // Assume the new node will go between the head and the tail.
  MdamRefListEntry *beforePtr = this;
  // Check if the above assumption is incorrect.
  if (disjunctNum >= nextEntryPtr_->disjunctNum_ && disjunctNum < disjunctNum_) {
    // Move the beforePtr so that it points to the node that immediately
    // preceeds the new node's location.
    // If disjunctNum is already in the list (that is, the insertion is a
    // duplicate), beforePtr will end up pointing to the node with the
    // duplicated value.
    beforePtr = beforePtr->nextEntryPtr_;
    while (disjunctNum >= beforePtr->nextEntryPtr_->disjunctNum_) {
      beforePtr = beforePtr->nextEntryPtr_;
    };
  };
  return beforePtr;
}
