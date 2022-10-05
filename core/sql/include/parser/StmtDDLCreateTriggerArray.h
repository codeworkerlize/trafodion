
#ifndef STMTDDLCREATETRIGGERARRAY_H
#define STMTDDLCREATETRIGGERARRAY_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLCreateTriggerArray.h
 * Description:  class for an array of pointers pointing to instances of
 *               class StmtDDLCreateTrigger
 *
 *
 * Created:      3/24/99
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
#include "parser/StmtDDLCreateTrigger.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLCreateTriggerArray;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// Definition of class StmtDDLCreateViewArray
// -----------------------------------------------------------------------
class StmtDDLCreateTriggerArray : public LIST(StmtDDLCreateTrigger *) {
 public:
  // constructor
  StmtDDLCreateTriggerArray(CollHeap *heap = PARSERHEAP()) : LIST(StmtDDLCreateTrigger *)(heap) {}

  // virtual destructor
  virtual ~StmtDDLCreateTriggerArray();

 private:
};  // class StmtDDLCreateTriggerArray

#endif  // STMTDDLCREATETRIGGERARRAY_H
