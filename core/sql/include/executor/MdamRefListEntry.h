
#ifndef MDAMREFLISTENTRY_H
#define MDAMREFLISTENTRY_H
/* -*-C++-*-
********************************************************************************
*
* File:         MdamRefListEntry.h
* Description:  MDAM Reference List Entry
*
*
* Created:      5/21/96
* Language:     C++
*
*
********************************************************************************
*/

// -----------------------------------------------------------------------------

#include "FixedSizeHeapManager.h"

// *****************************************************************************
// MDAM Reference List Entry is an entry in an MDAM Reference List.  It
// contains a disjunct number and a forward pointer to form a linked list.
// See class MdamRefList for further information.
// *****************************************************************************

class MdamRefListEntry {
 public:
  // Constructor for use on an empty list.
  // nextEntryPtr_ is set to point to the newly-created node to begin a
  // circularly-linked list.
  MdamRefListEntry(const int disjunctNum) : disjunctNum_(disjunctNum) { nextEntryPtr_ = this; }

  // Constructor for use on a non-empty list.
  // The new node is inserted into the linked list following the node pointed
  // to by beforePtr.
  MdamRefListEntry(const int disjunctNum, MdamRefListEntry *beforePtr)
      : disjunctNum_(disjunctNum), nextEntryPtr_(beforePtr->nextEntryPtr_) {
    beforePtr->nextEntryPtr_ = this;
  }

  // Destructor.
  inline ~MdamRefListEntry();

  // Operator new.
  inline void *operator new(size_t size, FixedSizeHeapManager &mdamRefListEntryHeap);

  // Operator new with just size_t.  This should never be called.
  void *operator new(size_t size) {
    ex_assert(0, "MdamRefListEntry::operator new(size_t) called.");
    return 0;
  }

  // Operator delete.  This should never be called.
  void operator delete(void *) { ex_assert(0, "MdamRefListEntry::operator delete(void *) called."); }

  // Get function for disjunctNum_.
  inline int getDisjunctNum() const;

  // Get function for nextEntryPtr_.
  inline MdamRefListEntry *getNextEntryPtr();

  // This function returns beforePtr to prepare for an insertion.
  // The object for which the function is called must be the last
  // entry of a reference list.
  MdamRefListEntry *positionBeforePtr(const int disjunctNum);

 private:
  // Disjunct number.
  int disjunctNum_;

  // Forward pointer to form a linked list.
  MdamRefListEntry *nextEntryPtr_;

};  // class MdamRefListEntry

// *****************************************************************************
// Inline member functions for class MdamRefListEntry
// *****************************************************************************

// Destructor.
inline MdamRefListEntry::~MdamRefListEntry() {}

// Operator new.
inline void *MdamRefListEntry::operator new(size_t size, FixedSizeHeapManager &mdamRefListEntryHeap) {
  return mdamRefListEntryHeap.allocateElement(size);
}

// Get function for disjunctNum_.
inline int MdamRefListEntry::getDisjunctNum() const { return disjunctNum_; }

// Get function for nextEntryPtr_.
inline MdamRefListEntry *MdamRefListEntry::getNextEntryPtr() { return nextEntryPtr_; }

#endif /* MDAMREFLISTENTRY_H */
