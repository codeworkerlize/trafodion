
#ifndef ELEMDDLFILEATTRLIST_H
#define ELEMDDLFILEATTRLIST_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLFileAttrList.h
 * Description:  class for lists of file attribute elements specified in
 *               DDL statements.  Since class ElemDDLFileAttrList is
 *               derived from the base class ElemDDLList; therefore, the
 *               class ElemDDLFileAttrList also represents a left linear
 *               tree.
 *
 *               Please note that class ElemDDLFileAttrList is not derived
 *               from class ElemDDLFileAttr which represents file attribute
 *               elements.
 *
 *
 * Created:      9/27/95
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
class ElemDDLFileAttrList;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// definition of class ElemDDLFileAttrList
// -----------------------------------------------------------------------
class ElemDDLFileAttrList : public ElemDDLList {
 public:
  // constructor
  ElemDDLFileAttrList(ElemDDLNode *commaExpr, ElemDDLNode *otherExpr)
      : ElemDDLList(ELM_FILE_ATTR_LIST, commaExpr, otherExpr) {}

  // virtual destructor
  virtual ~ElemDDLFileAttrList();

  // cast
  virtual ElemDDLFileAttrList *castToElemDDLFileAttrList();

  // methods for tracing
  virtual const NAString getText() const;

  // method for building text
  virtual NAString getSyntax() const;

 private:
};  // class ElemDDLFileAttrList

#endif  // ELEMDDLFILEATTRLIST_H
