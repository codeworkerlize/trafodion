
#ifndef STMTDDLALTERTABLESETCONSTRAINT_H
#define STMTDDLALTERTABLESETCONSTRAINT_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLAlterTableSetConstraint.h
 * Description:  class for Alter Table <table-name> Set Constraint ...
 *               DDL statements
 *
 *               The methods in this class are defined either in this
 *               header file or the source file StmtDDLAlter.C.
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

#include "StmtDDLAlterTable.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLAlterTableSetConstraint;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// definition of class StmtDDLAlterTableSetConstraint
// -----------------------------------------------------------------------
class StmtDDLAlterTableSetConstraint : public StmtDDLAlterTable {
 public:
  // constructor
  StmtDDLAlterTableSetConstraint(ElemDDLNode *constraintNameList, NABoolean setting)
      : StmtDDLAlterTable(DDL_ALTER_TABLE_SET_CONSTRAINT, constraintNameList), setting_(setting) {}

  // virtual destructor
  virtual ~StmtDDLAlterTableSetConstraint();

  // cast
  virtual StmtDDLAlterTableSetConstraint *castToStmtDDLAlterTableSetConstraint();

  // accessors
  inline ElemDDLNode *getConstraintNameList() const;
  inline NABoolean getConstraintSetting() const;

  // method for tracing
  virtual const NAString getText() const;

 private:
  NABoolean setting_;

};  // class StmtDDLAlterTableSetConstraint

// -----------------------------------------------------------------------
// definitions of inline methods for class StmtDDLAlterTableSetConstraint
// -----------------------------------------------------------------------

//
// accessor
//

inline ElemDDLNode *StmtDDLAlterTableSetConstraint::getConstraintNameList() const { return getAlterTableAction(); }

inline NABoolean StmtDDLAlterTableSetConstraint::getConstraintSetting() const { return setting_; }

#endif  // STMTDDLALTERTABLESETCONSTRAINT_H
