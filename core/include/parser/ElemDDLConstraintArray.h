#ifndef ELEMDDLCONSTRAINTARRAY_H
#define ELEMDDLCONSTRAINTARRAY_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLConstraintArray.h
 * Description:  class for an array of pointers pointing to instances of
 *               class ElemDDLConstraint
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

#include "parser/ElemDDLConstraint.h"
#include "common/Collections.h"
#ifndef SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#define SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#endif
#include "parser/SqlParserGlobals.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLConstraintArray;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// Definition of class ElemDDLConstraintArray
// -----------------------------------------------------------------------
class ElemDDLConstraintArray : public LIST(ElemDDLConstraint *) {
 public:
  // constructor
  ElemDDLConstraintArray(CollHeap *heap = PARSERHEAP()) : LIST(ElemDDLConstraint *)(heap) {}

  // copy ctor
  ElemDDLConstraintArray(const ElemDDLConstraintArray &orig, CollHeap *h = PARSERHEAP())
      : LIST(ElemDDLConstraint *)(orig, h) {}

  // virtual destructor
  virtual ~ElemDDLConstraintArray();

 private:
};  // class ElemDDLConstraintArray

#endif /* ELEMDDLCONSTRAINTARRAY_H */
