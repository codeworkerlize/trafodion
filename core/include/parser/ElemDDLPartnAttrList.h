
#ifndef ELEMDDLPARTNATTRLIST_H
#define ELEMDDLPARTNATTRLIST_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLPartnAttrList.h
 * Description:  class for lists of partition attribute elements specified in
 *               DDL statements.  Since class ElemDDLPartnAttrList is
 *               derived from the base class ElemDDLList; therefore, the
 *               class ElemDDLPartnAttrList also represents a left linear
 *               tree.
 *
 *               Please note that class ElemDDLPartnAttrList is not derived
 *               from class ElemDDLFileAttr which represents file attribute
 *               elements.
 *
 *
 * Created:      3/25/2003
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
class ElemDDLPartnAttrList;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// definition of class ElemDDLPartnAttrList
// -----------------------------------------------------------------------
class ElemDDLPartnAttrList : public ElemDDLList {
 public:
  // constructor
  ElemDDLPartnAttrList(ElemDDLNode *commaExpr, ElemDDLNode *otherExpr)
      : ElemDDLList(ELM_PARTN_ATTR_LIST, commaExpr, otherExpr) {}

  // virtual destructor
  virtual ~ElemDDLPartnAttrList();

  // cast
  virtual ElemDDLPartnAttrList *castToElemDDLPartnAttrList();

  // methods for tracing
  virtual const NAString getText() const;

  // method for building text
  virtual NAString getSyntax() const;

 private:
};  // class ElemDDLPartnAttrList

#endif  // ELEMDDLPARTNATTRLIST_H
