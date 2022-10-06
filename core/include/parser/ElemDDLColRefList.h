
#ifndef ELEMDDLCOLREFLIST_H
#define ELEMDDLCOLREFLIST_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLColRefList.h
 * Description:  class for lists of Column Name and Ordering Specification
 *               elements in DDL statements
 *
 * Created:      4/21/95
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
class ElemDDLColRefList;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// definition of class ElemDDLColRefList
// -----------------------------------------------------------------------
class ElemDDLColRefList : public ElemDDLList {
 public:
  // constructor
  ElemDDLColRefList(ElemDDLNode *commaExpr, ElemDDLNode *otherExpr)
      : ElemDDLList(ELM_COL_REF_LIST, commaExpr, otherExpr) {}

  // virtual destructor
  virtual ~ElemDDLColRefList();

  // cast
  virtual ElemDDLColRefList *castToElemDDLColRefList();

  // methods for tracing
  virtual const NAString getText() const;

 private:
};  // class ElemDDLColRefList

#endif  // ELEMDDLCOLREFLIST_H
