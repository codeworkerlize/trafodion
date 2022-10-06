#ifndef ELEMDDLCONSTRAINTUNIQUE_H
#define ELEMDDLCONSTRAINTUNIQUE_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLConstraintUnique.h
 * Description:  class for Unique constraint definitions in DDL statements
 *
 *
 * Created:      4/14/95
 * Language:     C++
 *
 *

 *
 *
 *****************************************************************************
 */

#ifndef SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#define SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#endif
#include "ElemDDLConstraint.h"
#include "parser/ElemDDLColRefArray.h"
#include "parser/SqlParserGlobals.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLConstraintUnique;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// definition of class ElemDDLConstraintUnique
// -----------------------------------------------------------------------
class ElemDDLConstraintUnique : public ElemDDLConstraint {
 public:
  // constructors
  ElemDDLConstraintUnique(ElemDDLNode *pColRefList = NULL, CollHeap *heap = PARSERHEAP());
  ElemDDLConstraintUnique(OperatorTypeEnum operType, ElemDDLNode *pColRefList = NULL, CollHeap *heap = PARSERHEAP());

  // copy ctor
  ElemDDLConstraintUnique(const ElemDDLConstraintUnique &orig,
                          CollHeap *h = PARSERHEAP());  // not written

  // virtual destructor
  virtual ~ElemDDLConstraintUnique();

  // cast
  virtual ElemDDLConstraintUnique *castToElemDDLConstraintUnique();

  //
  // accessors
  //

  virtual int getArity() const;
  virtual ExprNode *getChild(int index);

  inline ElemDDLNode *getColumnRefList() const;

  // returns the pointer pointing to either a parse node
  // representing a column reference or a left-skewed
  // binary tree representing a (left linear tree) list
  // of parse nodes representing column references.
  //
  // Note that the method returns the NULL pointer value
  // if the constraint is a column constraint and the method
  // is invoked before the construction of the Column
  // Definition parse node.  (During the construction of the
  // Column Definition parse node, a new Column Name parse
  // node is created, for the column constraint, to contain
  // the name of the column.)

  inline ElemDDLColRefArray &getKeyColumnArray();
  inline const ElemDDLColRefArray &getKeyColumnArray() const;

  // mutators
  void setColumnRefList(ElemDDLNode *pColRefList);
  virtual void setChild(int index, ExprNode *pChildNode);

  // methods for tracing
  virtual const NAString displayLabel2() const;
  virtual NATraceList getDetailInfo() const;
  virtual const NAString getText() const;

 private:
  ElemDDLColRefArray keyColumnArray_;

  // pointer to child parse node

  enum { INDEX_COLUMN_NAME_LIST = MAX_ELEM_DDL_CONSTRAINT_ARITY, MAX_ELEM_DDL_CONSTRAINT_UNIQUE_ARITY };

  ElemDDLNode *columnRefList_;

};  // class ElemDDLConstraintUnique

// -----------------------------------------------------------------------
// definitions of inline methods for class ElemDDLConstraintUnique
// -----------------------------------------------------------------------

//
// accessors
//

inline ElemDDLNode *ElemDDLConstraintUnique::getColumnRefList() const { return columnRefList_; }

inline ElemDDLColRefArray &ElemDDLConstraintUnique::getKeyColumnArray() { return keyColumnArray_; }

inline const ElemDDLColRefArray &ElemDDLConstraintUnique::getKeyColumnArray() const { return keyColumnArray_; }

#endif  // ELEMDDLCONSTRAINTUNIQUE_H
