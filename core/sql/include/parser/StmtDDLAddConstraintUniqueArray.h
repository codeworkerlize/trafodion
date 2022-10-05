#ifndef STMTDDLADDCONSTRAINTUNIQUEARRAY_H
#define STMTDDLADDCONSTRAINTUNIQUEARRAY_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLAddConstraintUniqueArray.h
 * Description:  class for an array of pointers pointing to instances of
 *               class StmtDDLAddConstraintUnique
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
#ifndef SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#define SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#endif
#include "parser/SqlParserGlobals.h"
#include "StmtDDLAddConstraintUnique.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLAddConstraintUniqueArray;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// Definition of class StmtDDLAddConstraintUniqueArray
// -----------------------------------------------------------------------
class StmtDDLAddConstraintUniqueArray : public LIST(StmtDDLAddConstraintUnique *) {
 public:
  // constructor
  StmtDDLAddConstraintUniqueArray(CollHeap *heap = PARSERHEAP()) : LIST(StmtDDLAddConstraintUnique *)(heap) {}

  // virtual destructor
  virtual ~StmtDDLAddConstraintUniqueArray();

 private:
};  // class StmtDDLAddConstraintUniqueArray

#endif /* STMTDDLADDCONSTRAINTUNIQUEARRAY_H */
