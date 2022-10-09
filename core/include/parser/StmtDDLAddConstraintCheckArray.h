#ifndef STMTDDLADDCONSTRAINTCHECKARRAY_H
#define STMTDDLADDCONSTRAINTCHECKARRAY_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLAddConstraintCheckArray.h
 * Description:  class for an array of pointers pointing to instances of
 *               class StmtDDLAddConstraintCheck
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

#include "parser/StmtDDLAddConstraintCheck.h"
#include "common/Collections.h"
#ifndef SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#define SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#endif
#include "parser/SqlParserGlobals.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLAddConstraintCheckArray;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// Definition of class StmtDDLAddConstraintCheckArray
// -----------------------------------------------------------------------
class StmtDDLAddConstraintCheckArray : public LIST(StmtDDLAddConstraintCheck *) {
 public:
  // constructor
  StmtDDLAddConstraintCheckArray(CollHeap *heap = PARSERHEAP()) : LIST(StmtDDLAddConstraintCheck *)(heap) {}

  // virtual destructor
  virtual ~StmtDDLAddConstraintCheckArray();

 private:
};  // class StmtDDLAddConstraintCheckArray

#endif /* STMTDDLADDCONSTRAINTCHECKARRAY_H */
