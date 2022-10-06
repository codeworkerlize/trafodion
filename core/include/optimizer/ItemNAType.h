
#ifndef ITEMNATYPE_H
#define ITEMNATYPE_H
/* -*-C++-*-
******************************************************************************
*
* File:         ItemNAType.h
* Description:  Functions and Aggregate item expressions
*		Aggregates to be used in groupby rel. operators.
*               Functions (and aggregates?) may be built-ins or user-defined.
* Created:      11/04/94
* Language:     C++
*
*
*
*
******************************************************************************
*/

// -----------------------------------------------------------------------

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
#include "common/NAType.h"

class NATypeToItem;

// -----------------------------------------------------------------------
// This class represents a node which "returns" an Item Expression
// and which has a single argument of an NAType.  The binder performs
// the magical conversion.  An instance of this class is used by the parser
// to represent the data_type portion of a cast expression.
// -----------------------------------------------------------------------
class NATypeToItem : public ItemExpr {
 public:
  NATypeToItem(NAType *arg0, OperatorTypeEnum opType = ITM_NATYPE) : ItemExpr(opType, NULL) { natype_pointer = arg0; }

  // virtual destructor
  virtual ~NATypeToItem() { delete natype_pointer; }

  // a virtual function for type propagating the node
  virtual const NAType *synthesizeType();

  virtual ItemExpr *copyTopNode(ItemExpr * = NULL, CollHeap *oheap = 0);

  // get the degree of this node (it is a unary op).
  virtual int getArity() const;

  const NAType *pushDownType(NAType &newType, enum NABuiltInTypeEnum defaultQualifier);

  // get a printable string that identifies the operator
  virtual const NAString getText() const;

 protected:
  NAType *natype_pointer;
};

// This class is currently used during DDL time, when we need
// to validate item expressions without fully binding them. This
// ItemExpr represents a named and typed column in that case.
class NamedTypeToItem : public NATypeToItem {
 public:
  NamedTypeToItem(const char *name, NAType *typ, CollHeap *heap)
      : NATypeToItem(typ, ITM_NAMED_TYPE_TO_ITEM), name_(name, heap) {}

  const NAString &getName() const { return name_; }

  virtual ItemExpr *copyTopNode(ItemExpr * = NULL, CollHeap *oheap = 0);
  virtual const NAString getText() const;

 private:
  NAString name_;
};
#endif
