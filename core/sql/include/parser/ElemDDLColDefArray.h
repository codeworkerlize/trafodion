#ifndef ELEMDDLCOLDEFARRAY_H
#define ELEMDDLCOLDEFARRAY_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLColDefArray.h
 * Description:  class for an array of pointers pointing to instances of
 *               class ElemDDLColDef
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
#ifndef SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#define SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#endif
#include "parser/SqlParserGlobals.h"
#include "ElemDDLColDef.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLColDefArray;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// Definition of class ElemDDLColDefArray
// -----------------------------------------------------------------------
class ElemDDLColDefArray : public LIST(ElemDDLColDef *) {
 public:
  // constructor
  ElemDDLColDefArray(CollHeap *heap = PARSERHEAP());

  // virtual destructor
  virtual ~ElemDDLColDefArray();

  // See if this columnName is in a ElemDDLColRefArray.  Returns the index,
  // -1 if not found.
  int getColumnIndex(const NAString &internalColumnName);

  // Returns -1 if the colDefParseNodeArray does not contain any division columns.
  // All division columns (if exist) are at the end of the list.
  ComSInt32 getIndexToFirstDivCol() const;

 private:
};  // class ElemDDLColDefArray

#endif /* ELEMDDLCOLDEFARRAY_H */
