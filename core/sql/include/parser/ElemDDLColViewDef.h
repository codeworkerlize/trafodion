#ifndef ELEMDDLCOLVIEWDEF_H
#define ELEMDDLCOLVIEWDEF_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLColViewDef.h
 * Description:  class for View Column Definition elements in DDL
 *               Create View statements--Note that class ElemDDLColViewDef
 *               is not derived from class ElemDDLColDef.
 *
 *
 * Created:      2/8/96
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
class ElemDDLColViewDef;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// Column Definition elements in DDL statements.
// -----------------------------------------------------------------------
class ElemDDLColViewDef : public ElemDDLNode {
 public:
  // default constructor
  ElemDDLColViewDef(const NAString &columnName, ElemDDLNode *pColAttrList = NULL);

  // virtual destructor
  virtual ~ElemDDLColViewDef();

  // cast
  virtual ElemDDLColViewDef *castToElemDDLColViewDef();

  //
  // accessors
  //

  virtual int getArity() const;
  virtual ExprNode *getChild(int index);
  inline const NAString &getColumnName() const;
  inline const NAString &getHeading() const;
  inline NABoolean isHeadingSpecified() const;

  // mutator
  virtual void setChild(int index, ExprNode *pChildNode);

  // methods for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString displayLabel2() const;
  virtual NATraceList getDetailInfo() const;
  virtual const NAString getText() const;

 private:
  // ---------------------------------------------------------------------
  // private methods
  // ---------------------------------------------------------------------

  void setColumnAttribute(ElemDDLNode *pColumnAttribute);

  // ---------------------------------------------------------------------
  // private data members
  // ---------------------------------------------------------------------

  NAString columnName_;

  // HEADING
  //
  //   isHeadingSpec_ is used by the parser to check for duplicate
  //   HEADING (either HEADING or NO HEADING) clause.
  //
  //   If one of the following conditions is true:
  //   1. NO HEADING clause is specified.
  //   2. Neither HEADING nor NO HEADING clause is specified.
  //   Then data member heading_ contains an empty string.
  //
  NABoolean isHeadingSpec_;
  NAString heading_;

  //
  // pointers to child parse nodes
  //
  //   Column Attributes list includes only column
  //   heading specification (HEADING clause).
  //
  enum { INDEX_ELEM_DDL_COL_ATTR_LIST = 0, MAX_ELEM_DDL_COL_VIEW_DEF_ARITY };

  ElemDDLNode *children_[MAX_ELEM_DDL_COL_VIEW_DEF_ARITY];

};  // class ElemDDLColViewDef

// -----------------------------------------------------------------------
// definitions of inline methods for class ElemDDLColViewDef
// -----------------------------------------------------------------------

inline const NAString &ElemDDLColViewDef::getColumnName() const { return columnName_; }

inline const NAString &ElemDDLColViewDef::getHeading() const { return heading_; }

inline NABoolean ElemDDLColViewDef::isHeadingSpecified() const { return isHeadingSpec_; }

#endif  // ELEMDDLCOLVIEWDEF_H
