#ifndef ELEMDDLALTERTABLEMOVE_H
#define ELEMDDLALTERTABLEMOVE_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLAlterTableMove.h
 * Description:  class for Move clause in Alter Table <table-name>
 *               DDL statements
 *
 *
 * Created:      9/20/95
 * Language:     C++
 *
 *

 *
 *
 *****************************************************************************
 */

#include "parser/ElemDDLNode.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLAlterTableMove;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None

// -----------------------------------------------------------------------
// Key Value elements in DDL statements.
// -----------------------------------------------------------------------
class ElemDDLAlterTableMove : public ElemDDLNode {
 public:
  // constructor
  ElemDDLAlterTableMove(ElemDDLNode *sourceLocationList, ElemDDLNode *destLocationList);

  // virtual destructor
  virtual ~ElemDDLAlterTableMove();

  // cast
  virtual ElemDDLAlterTableMove *castToElemDDLAlterTableMove();

  //
  // accessors
  //

  virtual int getArity() const;
  virtual ExprNode *getChild(int index);

  inline ElemDDLNode *getSourceLocationList() const;
  inline ElemDDLNode *getDestLocationList() const;

  // mutator
  virtual void setChild(int index, ExprNode *pChildNode);

  // method for tracing
  virtual const NAString getText() const;

 private:
  // pointers to child parse nodes

  enum { INDEX_SOURCE_LOCATION_LIST = 0, INDEX_DEST_LOCATION_LIST, MAX_ELEM_DDL_ALTER_TABLE_MOVE_ARITY };

  ElemDDLNode *children_[MAX_ELEM_DDL_ALTER_TABLE_MOVE_ARITY];

};  // class ElemDDLAlterTableMove

// -----------------------------------------------------------------------
// definitions of inline methods for class ElemDDLAlterTableMove
// -----------------------------------------------------------------------

inline ElemDDLNode *ElemDDLAlterTableMove::getSourceLocationList() const {
  return children_[INDEX_SOURCE_LOCATION_LIST];
}

inline ElemDDLNode *ElemDDLAlterTableMove::getDestLocationList() const { return children_[INDEX_DEST_LOCATION_LIST]; }

#endif  // ELEMDDLALTERTABLEMOVE_H
