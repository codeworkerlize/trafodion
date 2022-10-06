#ifndef MDAMREFLISTITERATOR_H
#define MDAMREFLISTITERATOR_H
/* -*-C++-*-
********************************************************************************
*
* File:         MdamRefListIterator.h
* Description:  MDAM Reference List Iterator
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

#include "MdamRefList.h"

// *****************************************************************************
// MdamRefListIterator is an iterator class for an Mdam Reference List.
// Two iteration member functions are provided.
// The first returns a boolean to indicate the success of the interation
// call and, if successful, returns the disjunct number by reference.
// The second returns a pointer.  If the iteration call is not successful,
// the pointer is null.
// This iterator is safe to use even if the reference list entries are
// being deleted.
// *****************************************************************************
class MdamRefListIterator {
 public:
  // Constructor.
  MdamRefListIterator(const MdamRefList *RefListPtr);

  // Iteration operator returns a boolean that indicates if there was or
  // was not a "next" entry and the corresponding disjunct number.
  NABoolean operator()(int &disjunctNum_);

  // Iteration operator returns a pointer to the next entry.
  MdamRefListEntry *operator()();

 private:
  // Pointer to the reference list.
  const MdamRefList *currentListPtr_;

  // Pointer to the current entry on the list.
  MdamRefListEntry *currentEntryPtr_;

};  // class MdamRefListIterator

#endif /* MDAMREFLISTITERATOR_H */
