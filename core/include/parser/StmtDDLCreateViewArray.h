
#ifndef STMTDDLCREATEVIEWARRAY_H
#define STMTDDLCREATEVIEWARRAY_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLCreateViewArray.h
 * Description:  class for an array of pointers pointing to instances of
 *               class StmtDDLCreateView
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
#include "parser/StmtDDLCreateView.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLCreateViewArray;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// Definition of class StmtDDLCreateViewArray
// -----------------------------------------------------------------------
class StmtDDLCreateViewArray : public LIST(StmtDDLCreateView *) {
 public:
  // constructor
  StmtDDLCreateViewArray(CollHeap *heap = PARSERHEAP()) : LIST(StmtDDLCreateView *)(heap) {}

  // virtual destructor
  virtual ~StmtDDLCreateViewArray();

 private:
};  // class StmtDDLCreateViewArray

#endif  // STMTDDLCREATEVIEWARRAY_H
