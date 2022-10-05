
#ifndef STMTDDLCREATEINDEXARRAY_H
#define STMTDDLCREATEINDEXARRAY_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLCreateIndexArray.h
 * Description:  class for an array of pointers pointing to instances of
 *               class StmtDDLCreateIndex
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
#include "parser/StmtDDLCreateIndex.h"
#ifndef SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#define SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#endif
#include "parser/SqlParserGlobals.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLCreateIndexArray;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// Definition of class StmtDDLCreateIndexArray
// -----------------------------------------------------------------------
class StmtDDLCreateIndexArray : public LIST(StmtDDLCreateIndex *) {
 public:
  // constructor
  StmtDDLCreateIndexArray(CollHeap *heap = PARSERHEAP()) : LIST(StmtDDLCreateIndex *)(heap) {}

  // virtual destructor
  virtual ~StmtDDLCreateIndexArray();

 private:
};  // class StmtDDLCreateIndexArray

#endif  // STMTDDLCREATEINDEXARRAY_H
