#ifndef STMTDDLCREATETABLEARRAY_H
#define STMTDDLCREATETABLEARRAY_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLCreateTableArray.h
 * Description:  class for an array of pointers pointing to instances of
 *               class StmtDDLCreateTable
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
#include "parser/StmtDDLCreateTable.h"
#ifndef SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#define SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#endif
#include "parser/SqlParserGlobals.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLCreateTableArray;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// Definition of class StmtDDLCreateTableArray
// -----------------------------------------------------------------------
class StmtDDLCreateTableArray : public LIST(StmtDDLCreateTable *) {
 public:
  // constructor
  StmtDDLCreateTableArray(CollHeap *heap = PARSERHEAP()) : LIST(StmtDDLCreateTable *)(heap) {}

  // virtual destructor
  virtual ~StmtDDLCreateTableArray();

 private:
};  // class StmtDDLCreateTableArray

#endif /* STMTDDLCREATETABLEARRAY_H */
