#ifndef STMTDDLSCHGRANTARRAY_H
#define STMTDDLSCHGRANTARRAY_H
/* -*-C++-*-
/**********************************************************************


    * File:         StmtDDLSchGrantArray.h
    * Description:  class for an array of pointers pointing to instances of
    *               class StmtDDLSchGrant
    *
    * Created:      03/06/2007
    * Language:     C++
**********************************************************************/

#include "common/Collections.h"
#ifndef SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#define SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#endif
#include "parser/SqlParserGlobals.h"
#include "parser/StmtDDLSchGrant.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLSchGrantArray;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// Definition of class StmtDDLSchGrantArray
// -----------------------------------------------------------------------
class StmtDDLSchGrantArray : public LIST(StmtDDLSchGrant *) {
 public:
  // constructor
  StmtDDLSchGrantArray(CollHeap *heap = PARSERHEAP()) : LIST(StmtDDLSchGrant *)(heap) {}

  // virtual destructor
  virtual ~StmtDDLSchGrantArray();

 private:
};  // class StmtDDLSchGrantArray

#endif  // STMTDDLSCHGRANTARRAY_H
