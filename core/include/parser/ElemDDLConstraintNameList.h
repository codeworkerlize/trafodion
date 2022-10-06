
#ifndef ELEMDDLCONSTRAINTNAMELIST_H
#define ELEMDDLCONSTRAINTNAMELIST_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLConstraintNameList.h
 * Description:  class for lists of constraint name elements in
 *               DDL statements
 *
 *
 * Created:      9/21/95
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
class ElemDDLConstraintNameList;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// A list of key value elements in First Key clause in DDL statement.
// -----------------------------------------------------------------------
class ElemDDLConstraintNameList : public ElemDDLList {
 public:
  // constructor
  ElemDDLConstraintNameList(ElemDDLNode *commaExpr, ElemDDLNode *otherExpr)
      : ElemDDLList(ELM_CONSTRAINT_NAME_LIST, commaExpr, otherExpr) {}

  // virtual destructor
  virtual ~ElemDDLConstraintNameList();

  // cast
  virtual ElemDDLConstraintNameList *castToElemDDLConstraintNameList();

  // methods for tracing
  virtual const NAString getText() const;

 private:
};  // class ElemDDLConstraintNameList

#endif  // ELEMDDLCONSTRAINTNAMELIST_H
