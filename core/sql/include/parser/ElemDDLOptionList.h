
#ifndef ELEMDDLOPTIONLIST_H
#define ELEMDDLOPTIONLIST_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLOptionList.h
 * Description:  class for lists of options specified in DDL
 *               statements.  Note that class ElemDDLOptionList
 *               is derived from base class ElemDDLList;
 *               therefore, class ElemDDLOptionList also
 *               represents a left linear tree.
 *
 *
 * Created:      9/18/95
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
class ElemDDLOptionList;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// definition of class ElemDDLOptionList
// -----------------------------------------------------------------------
class ElemDDLOptionList : public ElemDDLList {
 public:
  // constructor
  ElemDDLOptionList(ElemDDLNode *commaExpr, ElemDDLNode *otherExpr)
      : ElemDDLList(ELM_OPTION_LIST, commaExpr, otherExpr) {}

  // virtual destructor
  virtual ~ElemDDLOptionList();

  // cast
  virtual ElemDDLOptionList *castToElemDDLOptionList();

  // method for tracing
  virtual const NAString getText() const;

  // method for building text
  virtual NAString getSyntax() const;

 private:
};  // class ElemDDLOptionList

#endif  // ELEMDDLOPTIONLIST_H
