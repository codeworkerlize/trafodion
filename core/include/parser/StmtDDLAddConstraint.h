
#ifndef STMTDDLADDCONSTRAINT_H
#define STMTDDLADDCONSTRAINT_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLAddConstraint.h
 * Description:  base class for (generic) Alter Table <table-name> Add
 *               Constraint statements
 *
 *               The proper name for this class should be
 *               StmtDDLAlterTableAddConstraint because this class is
 *               derived from the base class StmtDDLAlterTable.  The
 *               name StmtDDLAlterTableAddConstraint is too long
 *               however.  So the short name StmtDDLAddConstraint
 *               is used even though it is less clear.
 *
 *               The methods in this class are defined either in this
 *               header file or the source file StmtDDLAlter.C.
 *
 *
 * Created:      6/15/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "StmtDDLAlterTable.h"
#include "optimizer/ObjectNames.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLAddConstraint;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
class ElemDDLConstraint;

// -----------------------------------------------------------------------
// definition of class StmtDDLAddConstraint
// -----------------------------------------------------------------------
class StmtDDLAddConstraint : public StmtDDLAlterTable {
 public:
  // constructors
  inline StmtDDLAddConstraint();
  inline StmtDDLAddConstraint(OperatorTypeEnum operType);

  StmtDDLAddConstraint(OperatorTypeEnum operType, ElemDDLNode *pAddConstraintAction)
      : StmtDDLAlterTable(operType, pAddConstraintAction) {}

  StmtDDLAddConstraint(OperatorTypeEnum operType, const QualifiedName &tableQualName, ElemDDLNode *pAddConstraintAction)
      : StmtDDLAlterTable(operType, tableQualName, pAddConstraintAction) {}

  // virtual destructor
  virtual ~StmtDDLAddConstraint();

  // cast
  virtual StmtDDLAddConstraint *castToStmtDDLAddConstraint();

  //
  // accessors
  //

  inline ElemDDLConstraint *getConstraint() const;
  const NAString getConstraintName() const;
  const QualifiedName &getConstraintNameAsQualifiedName() const;
  QualifiedName &getConstraintNameAsQualifiedName();
  NABoolean isDeferrable() const;
  NABoolean isDroppable() const;

  NABoolean isDroppableSpecifiedExplicitly() const;

  // returns TRUE if the user specified the DROPPABLE clause
  // explicitly; returns FALSE otherwise (including the case
  // when the user specified the NOT DROPPABLE clause explicitly).

  NABoolean isNotDroppableSpecifiedExplicitly() const;

  // returns TRUE if the user specified the NOT DROPPABLE clause
  // explicitly; returns FALSE otherwise (including the case
  // when the user specified the DROPPABLE clause explicitly).

  //
  // mutators
  //

  void setDroppableFlag(const NABoolean setting);

  //
  // methods for tracing
  //

  virtual NATraceList getDetailInfo() const;
  virtual const NAString getText() const;

  //
  // definition of method bindNode() for clas StmtDDLAddConstraint
  //

  virtual ExprNode *bindNode(BindWA *pBindWA);

 private:
};  // class StmtDDLAddConstraint

// -----------------------------------------------------------------------
// definitions of inline methods for class StmtDDLAddConstraint
// -----------------------------------------------------------------------
//
// accessor
//

inline ElemDDLConstraint *StmtDDLAddConstraint::getConstraint() const {
  return getAlterTableAction()->castToElemDDLConstraint();
}

#endif  // STMTDDLADDCONSTRAINT_H
