#ifndef STMTDDLDROPCONSTRAINT_H
#define STMTDDLDROPCONSTRAINT_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLDropConstraint.h
 * Description:  class for parse node representing Drop Alias statements
 *
 *
 * Created:      04/23/96
 * Language:     C++
 *
 *

 *
 *
 *****************************************************************************
 */

#include "common/ComSmallDefs.h"
#include "StmtDDLAlterTable.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLDropConstraint;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None

// -----------------------------------------------------------------------
// Drop Constraint statement
// -----------------------------------------------------------------------
class StmtDDLDropConstraint : public StmtDDLAlterTable {
 public:
  // constructor
  StmtDDLDropConstraint(const QualifiedName &constraintQualifiedName, ComDropBehavior dropBehavior);

  // virtual destructor
  virtual ~StmtDDLDropConstraint();

  // cast
  virtual StmtDDLDropConstraint *castToStmtDDLDropConstraint();

  // accessors

  const NAString getConstraintName() const;
  inline ComDropBehavior getDropBehavior() const;

  inline const QualifiedName &getConstraintNameAsQualifiedName() const;
  inline QualifiedName &getConstraintNameAsQualifiedName();

  // for tracing
  virtual const NAString displayLabel2() const;
  virtual const NAString displayLabel3() const;
  virtual const NAString getText() const;

  // method for binding
  virtual ExprNode *bindNode(BindWA *pBindWA);

 private:
  QualifiedName constraintQualName_;
  ComDropBehavior dropBehavior_;

};  // class StmtDDLDropConstraint

// -----------------------------------------------------------------------
// definitions of inline methods for class StmtDDLDropConstraint
// -----------------------------------------------------------------------

//
// accessors
//
inline QualifiedName &StmtDDLDropConstraint::getConstraintNameAsQualifiedName() { return constraintQualName_; }

inline const QualifiedName &StmtDDLDropConstraint::getConstraintNameAsQualifiedName() const {
  return constraintQualName_;
}

inline ComDropBehavior StmtDDLDropConstraint::getDropBehavior() const { return dropBehavior_; }

#endif  // STMTDDLDROPCONSTRAINT_H
