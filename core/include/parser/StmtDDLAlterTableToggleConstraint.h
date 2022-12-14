#ifndef STMTDDLALTERTABLETOGGLECNSTRNT_H
#define STMTDDLALTERTABLETOGGLECNSTRNT_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLAlterTableToggleConstraint.h
 * Description:  class for Alter Table <table-name>
 *                  DISABLE ALL CONSTRAINTS and
 *                  DISABLE <constraint>
 *                  ENABLE ALL CONSTRAINTS
 *                  ENABLE <constraint>
 *               DDL statements
 *
 *               The methods in this class are defined either in this
 *               header file or the source file StmtDDLAlter.cpp
 *
 *
 * Created:     03/23/07
 * Language:     C++
 *
 *

 *
 *
 *****************************************************************************
 */

#include "parser/StmtDDLAlterTable.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLAlterTableToggleConstraint;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// definition of class StmtDDLAlterTableToggleConstraint
// -----------------------------------------------------------------------
class StmtDDLAlterTableToggleConstraint : public StmtDDLAlterTable {
 public:
  // constructor
  StmtDDLAlterTableToggleConstraint(const QualifiedName &constraintQualifiedName, NABoolean allConstraints,
                                    NABoolean setDisabled, NABoolean validateConstraintFlag);

  // virtual destructor
  virtual ~StmtDDLAlterTableToggleConstraint();

  // cast
  virtual StmtDDLAlterTableToggleConstraint *castToStmtDDLAlterTableToggleConstraint();

  // accessors
  inline const NABoolean getAllConstraints() const;
  inline const NABoolean getDisabledFlag() const;

  const NAString getConstraintName() const;
  inline const QualifiedName &getConstraintNameAsQualifiedName() const;
  inline QualifiedName &getConstraintNameAsQualifiedName();
  inline const NABoolean getValidateConstraintFlag() const;

  // ---------------------------------------------------------------------
  // mutators
  // ---------------------------------------------------------------------

  // This method collects information in the parse sub-tree and copies it
  // to the current parse node.
  void synthesize();
  inline void setAllConstraints(NABoolean allConstraints);
  inline void setDisabledFlag(NABoolean setDisabled);
  inline void setValidateConstraintFlag(NABoolean validateConstraintFlag);

  // methods for tracing
  virtual const NAString displayLabel2() const;
  virtual const NAString getText() const;

  //
  // please do not use the following methods
  //

  StmtDDLAlterTableToggleConstraint();
  StmtDDLAlterTableToggleConstraint(const StmtDDLAlterTableToggleConstraint &);
  StmtDDLAlterTableToggleConstraint &operator=(const StmtDDLAlterTableToggleConstraint &);

 private:
  QualifiedName constraintQualName_;
  NABoolean allConstraints_;
  NABoolean setDisabled_;
  NABoolean validateConstraint_;  // only meaningful if enabling

};  // class StmtDDLAlterTableToggleConstraint

// -----------------------------------------------------------------------
// definitions of inline methods for class StmtDDLAlterTableToggleConstraint
// -----------------------------------------------------------------------
inline const QualifiedName &StmtDDLAlterTableToggleConstraint::getConstraintNameAsQualifiedName() const {
  return constraintQualName_;
}

inline QualifiedName &StmtDDLAlterTableToggleConstraint::getConstraintNameAsQualifiedName() {
  return constraintQualName_;
}

inline const NABoolean StmtDDLAlterTableToggleConstraint::getAllConstraints() const { return allConstraints_; }

inline void StmtDDLAlterTableToggleConstraint::setAllConstraints(NABoolean allConstraints) {
  allConstraints_ = allConstraints;
}

inline const NABoolean StmtDDLAlterTableToggleConstraint::getDisabledFlag() const { return setDisabled_; }

inline void StmtDDLAlterTableToggleConstraint::setDisabledFlag(NABoolean setDisabled) { setDisabled_ = setDisabled; }

inline const NABoolean StmtDDLAlterTableToggleConstraint::getValidateConstraintFlag() const {
  return validateConstraint_;
}

inline void StmtDDLAlterTableToggleConstraint::setValidateConstraintFlag(NABoolean validateConstraint) {
  validateConstraint_ = validateConstraint;
}

#endif  // STMTDDLALTERTABLETOGGLECNSTRNT_H
