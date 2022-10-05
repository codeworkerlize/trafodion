
#ifndef ELEMDDLCOLREFARRAY_H
#define ELEMDDLCOLREFARRAY_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLColRefArray.h
 * Description:  class for an array of pointers pointing to instances of
 *               class ElemDDLColRef
 *
 *
 * Created:      5/26/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "common/Collections.h"
#include "parser/ElemDDLColRef.h"
#ifndef SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#define SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#endif
#include "parser/SqlParserGlobals.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLColRefArray;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// Definition of class ElemDDLColRefArray
// -----------------------------------------------------------------------
class ElemDDLColRefArray : public LIST(ElemDDLColRef *) {
 public:
  // constructor
  ElemDDLColRefArray(CollHeap *heap = PARSERHEAP());

  // virtual destructor
  virtual ~ElemDDLColRefArray();

  // see if the this ElemDDLColRefArray has other ElemDDLColRefArray
  // as a prefix.
  ComBoolean hasPrefix(ElemDDLColRefArray &other);

  // See if this columnName is in a ElemDDLColRefArray.  Returns the index,
  // -1 if not found.
  int getColumnIndex(const NAString &columnName);

  // see if the this ElemDDLColRefArray contains other ElemDDLColRefArray.
  // The columns need not be in the same order.
  ComBoolean contains(ElemDDLColRefArray &other, int &firstUnmatchedEntry);

  // see if the ElemDDLColRefArray matches the other ElemDDLColRefArray.
  // The columns need not be in the same order.
  ComBoolean matches(ElemDDLColRefArray &other);

  // see if the this ElemDDLColRefArray has the ElemDDLColRef as an entry.
  ComBoolean hasEntry(ElemDDLColRef &colRef);

  // Compare column names and their order with other.
  ComBoolean operator==(ElemDDLColRefArray &other);

  // Compare column names and their order with other.
  ComBoolean operator!=(ElemDDLColRefArray &other);

 private:
};  // class ElemDDLColRefArray

#endif /* ELEMDDLCOLREFARRAY_H */
