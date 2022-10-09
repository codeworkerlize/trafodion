#ifndef ELEMDDLCONSTRAINTCHECK_H
#define ELEMDDLCONSTRAINTCHECK_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLConstraintCheck.h
 * Description:  class for Check constraint definitions in DDL statements
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

#include "parser/ElemDDLConstraint.h"
#include "parser/ParNameLocList.h"
#include "optimizer/ItemExprList.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLConstraintCheck;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
class ItemExpr;

// -----------------------------------------------------------------------
// definition of class ElemDDLConstraintCheck
// -----------------------------------------------------------------------
class ElemDDLConstraintCheck : public ElemDDLConstraint {
 public:
  // initialize constructor
  ElemDDLConstraintCheck(ItemExpr *pSearchCondition, const ParNameLocList &nameLocList, CollHeap *heap = PARSERHEAP());

  // virtual destructor
  virtual ~ElemDDLConstraintCheck();

  // cast
  virtual ElemDDLConstraintCheck *castToElemDDLConstraintCheck();
  virtual NABoolean isConstraintNotNull() const;
  NABoolean getColumnsNotNull(ItemExprList &);

  //
  // accessors
  //

  virtual int getArity() const;
  virtual ExprNode *getChild(int index);

  inline StringPos getEndPosition() const;

  inline const ParNameLocList &getNameLocList() const;
  inline ParNameLocList &getNameLocList();

  inline ItemExpr *getSearchCondition() const;

  inline StringPos getStartPosition() const;

  //
  // mutators
  //

  virtual void setChild(int index, ExprNode *pChildNode);
  inline void setEndPosition(const StringPos endPos);
  inline void setStartPosition(const StringPos startPos);

  //
  // methods for tracing
  //

  // displayLabel1() is defined in the base class ElemDDLConstraint
  virtual const NAString displayLabel2() const;
  virtual const NAString getText() const;

 private:
  // ---------------------------------------------------------------------
  // private methods
  // ---------------------------------------------------------------------

  ElemDDLConstraintCheck();                                           // DO NOT USE
  ElemDDLConstraintCheck(const ElemDDLConstraintCheck &);             // DO NOT USE
  ElemDDLConstraintCheck &operator=(const ElemDDLConstraintCheck &);  // DO NOT USE

  // ---------------------------------------------------------------------
  // private data member
  // ---------------------------------------------------------------------

  //
  // information about the position of the name within the input
  // string (to help with computing the check constraint search
  // condition text)
  //

  ParNameLocList nameLocList_;

  //
  // positions of the check constraint search condition within
  // the input string (to help with computing the text)
  //

  StringPos startPos_;
  StringPos endPos_;

  //
  // pointer to child parse node
  //

  enum { INDEX_SEARCH_CONDITION = MAX_ELEM_DDL_CONSTRAINT_ARITY, MAX_ELEM_DDL_CONSTRAINT_CHECK_ARITY };

  ItemExpr *searchCondition_;

};  // class ElemDDLConstraintCheck

// -----------------------------------------------------------------------
// definitions of inline methods for class ElemDDLConstraintCheck
// -----------------------------------------------------------------------

//
// accessors
//

inline StringPos ElemDDLConstraintCheck::getEndPosition() const { return endPos_; }

inline const ParNameLocList &ElemDDLConstraintCheck::getNameLocList() const { return nameLocList_; }

inline ParNameLocList &ElemDDLConstraintCheck::getNameLocList() { return nameLocList_; }

inline ItemExpr *ElemDDLConstraintCheck::getSearchCondition() const { return searchCondition_; }

inline StringPos ElemDDLConstraintCheck::getStartPosition() const { return startPos_; }

//
// mutators
//

inline void ElemDDLConstraintCheck::setEndPosition(const StringPos endPos) { endPos_ = endPos; }

inline void ElemDDLConstraintCheck::setStartPosition(const StringPos startPos) { startPos_ = startPos; }

#endif  // ELEMDDLCONSTRAINTCHECK_H
