#ifndef ELEMDDLGRANTEEARRAY_H
#define ELEMDDLGRANTEEARRAY_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLGranteeArray.h
 * Description:  class for an array of pointers pointing to instances of
 *               class ElemDDLGrantee
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
#include "parser/ElemDDLGrantee.h"
#ifndef SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#define SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#endif
#include "parser/SqlParserGlobals.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLGranteeArray;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// Definition of class ElemDDLGranteeArray
// -----------------------------------------------------------------------
class ElemDDLGranteeArray : public LIST(ElemDDLGrantee *) {
 public:
  // constructor
  ElemDDLGranteeArray(CollHeap *heap = PARSERHEAP());

  // virtual destructor
  virtual ~ElemDDLGranteeArray();

 private:
};  // class ElemDDLGranteeArray

#endif  // ELEMDDLGRANTEEARRAY_H
