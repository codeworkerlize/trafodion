
#ifndef ELEMDDLKEYVALUELIST_H
#define ELEMDDLKEYVALUELIST_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLKeyValueList.h
 * Description:  class for lists of Key Value elements in First Key
 *               clause in DDL statements
 *
 *
 * Created:      4/7/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "parser/ElemDDLList.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLKeyValueList;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// A list of key value elements in First Key clause in DDL statement.
// -----------------------------------------------------------------------
class ElemDDLKeyValueList : public ElemDDLList {
 public:
  // constructor
  ElemDDLKeyValueList(ElemDDLNode *commaExpr, ElemDDLNode *otherExpr)
      : ElemDDLList(ELM_KEY_VALUE_LIST, commaExpr, otherExpr) {}

  // virtual destructor
  virtual ~ElemDDLKeyValueList();

  // cast
  virtual ElemDDLKeyValueList *castToElemDDLKeyValueList();

  // method for tracing
  virtual const NAString getText() const;

  // method for building text
  virtual NAString getSyntax() const;

 private:
};  // class ElemDDLKeyValueList

#endif  // ELEMDDLKEYVALUELIST_H
