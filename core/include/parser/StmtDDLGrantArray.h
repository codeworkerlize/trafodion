
#ifndef STMTDDLGRANTARRAY_H
#define STMTDDLGRANTARRAY_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLGrantArray.h
 * Description:  class for an array of pointers pointing to instances of
 *               class StmtDDLGrant
 *
 *
 * Created:      1/11/96
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
#include "parser/StmtDDLGrant.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLGrantArray;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// Definition of class StmtDDLGrantArray
// -----------------------------------------------------------------------
class StmtDDLGrantArray : public LIST(StmtDDLGrant *) {
 public:
  // constructor
  StmtDDLGrantArray(CollHeap *heap = PARSERHEAP()) : LIST(StmtDDLGrant *)(heap) {}

  // virtual destructor
  virtual ~StmtDDLGrantArray();

 private:
};  // class StmtDDLGrantArray

#endif  // STMTDDLGRANTARRAY_H
