
#ifndef ITEMCONSTVALUEARRAY_H
#define ITEMCONSTVALUEARRAY_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ItemConstValueArray.h
 * Description:  class for an array of pointers pointing to instances of
 *               class ConstValue (defined in header file ItemColRef.h)
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
#include "optimizer/ItemColRef.h"
#include "optimizer/ItemExpr.h"
#ifndef SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#define SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#endif
#include "parser/SqlParserGlobals.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ItemConstValueArray;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// Definition of typedef ItemConstValueArray
// -----------------------------------------------------------------------
class ItemConstValueArray : public LIST(ConstValue *) {
 public:
  // constructor
  ItemConstValueArray(CollHeap *heap = PARSERHEAP()) : LIST(ConstValue *)(heap) {}

  // virtual destructor
  virtual ~ItemConstValueArray();

 private:
};  // class ItemConstValueArray

#endif  // ITEMCONSTVALUEARRAY_H
