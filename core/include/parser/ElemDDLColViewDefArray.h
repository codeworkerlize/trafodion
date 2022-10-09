
#ifndef ELEMDDLCOLVIEWDEFARRAY_H
#define ELEMDDLCOLVIEWDEFARRAY_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLColViewDefArray.h
 * Description:  class for an array of pointers pointing to instances of
 *               class ElemDDLColViewDef
 *
 *
 * Created:      2/8/96
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#ifndef SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#define SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#endif
#include "parser/ElemDDLColViewDef.h"
#include "common/Collections.h"
#include "parser/SqlParserGlobals.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLColViewDefArray;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// Definition of class ElemDDLColViewDefArray
// -----------------------------------------------------------------------
class ElemDDLColViewDefArray : public LIST(ElemDDLColViewDef *) {
 public:
  // default constructor
  ElemDDLColViewDefArray(CollHeap *heap = PARSERHEAP()) : LIST(ElemDDLColViewDef *)(heap) {}

  // virtual destructor
  virtual ~ElemDDLColViewDefArray();

 private:
};  // class ElemDDLColViewDefArray

#endif  // ELEMDDLCOLVIEWDEFARRAY_H
