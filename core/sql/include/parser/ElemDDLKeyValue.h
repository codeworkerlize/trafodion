
#ifndef ELEMDDLKEYVALUE_H
#define ELEMDDLKEYVALUE_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLKeyValue.h
 * Description:  class for Key Value elements in DDL statements
 *
 *
 * Created:      3/29/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "ElemDDLNode.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLKeyValue;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
class ItemExpr;
class ConstValue;

// -----------------------------------------------------------------------
// Key Value elements in DDL statements.
// -----------------------------------------------------------------------
class ElemDDLKeyValue : public ElemDDLNode {
 public:
  // constructor
  ElemDDLKeyValue(ItemExpr *pConstValue);

  // virtual destructor
  virtual ~ElemDDLKeyValue();

  // cast
  virtual ElemDDLKeyValue *castToElemDDLKeyValue();

  // accessors

  virtual int getArity() const;
  virtual ExprNode *getChild(int index);

  inline ConstValue *getKeyValue() const;

  // mutator
  virtual void setChild(int index, ExprNode *pChildNode);

  // methods for tracing
  virtual const NAString getText() const;

  // method for building text
  virtual NAString getSyntax() const;

 private:
  // pointer to child parse node

  enum { INDEX_KEY_VALUE = 0, MAX_ELEM_DDL_KEY_VALUE_ARITY };

  ConstValue *keyValue_;

};  // class ElemDDLKeyValue

// -----------------------------------------------------------------------
// definitions of inline methods for class ElemDDLKeyValue
// -----------------------------------------------------------------------

inline ConstValue *ElemDDLKeyValue::getKeyValue() const { return keyValue_; }

#endif  // ELEMDDLKEYVALUE_H
