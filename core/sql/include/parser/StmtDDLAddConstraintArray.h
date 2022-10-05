#ifndef STMTDDLADDCONSTRAINTARRAY_H
#define STMTDDLADDCONSTRAINTARRAY_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLAddConstraintArray.h
 * Description:  class for an array of pointers pointing to instances of
 *               class StmtDDLAddConstraint
 *
 *
 * Created:      6/21/95
 * Language:     C++
 *
 *

 *
 *
 *****************************************************************************
 */

#include "common/Collections.h"
#include "StmtDDLAddConstraint.h"
#ifndef SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#define SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#endif
#include "parser/SqlParserGlobals.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLAddConstraintArray;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// Definition of class StmtDDLAddConstraintArray
// -----------------------------------------------------------------------
class StmtDDLAddConstraintArray : public LIST(StmtDDLAddConstraint *) {
 public:
  // constructor
  StmtDDLAddConstraintArray(CollHeap *heap = PARSERHEAP()) : LIST(StmtDDLAddConstraint *)(heap) {}

  // virtual destructor
  virtual ~StmtDDLAddConstraintArray();

 private:
};  // class StmtDDLAddConstraintArray

#endif /* STMTDDLADDCONSTRAINTARRAY_H */
