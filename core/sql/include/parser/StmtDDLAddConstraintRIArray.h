#ifndef STMTDDLADDCONSTRAINTRIARRAY_H
#define STMTDDLADDCONSTRAINTRIARRAY_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLAddConstraintRIArray.h
 * Description:  class for an array of pointers pointing to instances of
 *               class StmtDDLAddConstraintRI
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
#include "StmtDDLAddConstraintRI.h"
#ifndef SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#define SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#endif
#include "parser/SqlParserGlobals.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLAddConstraintRIArray;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// Definition of class StmtDDLAddConstraintRIArray
// -----------------------------------------------------------------------
class StmtDDLAddConstraintRIArray : public LIST(StmtDDLAddConstraintRI *) {
 public:
  // constructor
  StmtDDLAddConstraintRIArray(CollHeap *heap = PARSERHEAP()) : LIST(StmtDDLAddConstraintRI *)(heap) {}

  // virtual destructor
  virtual ~StmtDDLAddConstraintRIArray();

 private:
};  // class StmtDDLAddConstraintRIArray

#endif /* STMTDDLADDCONSTRAINTRIARRAY_H */
