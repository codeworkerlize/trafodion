
#ifndef STMTDDLADDCONSTRAINTUNIQUE_H
#define STMTDDLADDCONSTRAINTUNIQUE_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLAddConstraintUnique.h
 * Description:  class for Alter Table <table-name> Add Constraint
 *               statements containing Unique clause
 *
 *               The proper name for this class should be
 *               StmtDDLAlterTableAddConstraintUnique because this class
 *               is derived from the base class StmtDDLAlterTable.  The
 *               name StmtDDLAlterTableAddConstraintUnique is too long
 *               however.  So the short name StmtDDLAddConstraintUnique
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
#include "StmtDDLAddConstraint.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLAddConstraintUnique;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// definition of class StmtDDLAddConstraintUnique
// -----------------------------------------------------------------------
class StmtDDLAddConstraintUnique : public StmtDDLAddConstraint {
 public:
  // constructors
  StmtDDLAddConstraintUnique(ElemDDLNode *pElemDDLConstraintUnique)
      : StmtDDLAddConstraint(DDL_ALTER_TABLE_ADD_CONSTRAINT_UNIQUE, pElemDDLConstraintUnique) {}

  StmtDDLAddConstraintUnique(const QualifiedName &tableQualName, ElemDDLNode *pElemDDLConstraintUnique)
      : StmtDDLAddConstraint(DDL_ALTER_TABLE_ADD_CONSTRAINT_UNIQUE, tableQualName, pElemDDLConstraintUnique) {}

  StmtDDLAddConstraintUnique(OperatorTypeEnum operatorType, ElemDDLNode *pElemDDLConstraintUnique)
      : StmtDDLAddConstraint(operatorType, pElemDDLConstraintUnique) {}

  StmtDDLAddConstraintUnique(OperatorTypeEnum operatorType, const QualifiedName &tableQualName,
                             ElemDDLNode *pElemDDLConstraintUnique)
      : StmtDDLAddConstraint(operatorType, tableQualName, pElemDDLConstraintUnique) {}

  // virtual destructor
  virtual ~StmtDDLAddConstraintUnique();

  // cast
  virtual StmtDDLAddConstraintUnique *castToStmtDDLAddConstraintUnique();

  // method for tracing
  virtual const NAString getText() const;

  // definition of method bindNode() for class StmtDDLAddConstraintUnique
  virtual ExprNode *bindNode(BindWA *pBindWA);

 private:
  // ---------------------------------------------------------------------
  // private methods
  // ---------------------------------------------------------------------

  StmtDDLAddConstraintUnique();  // DO NOT USE
  StmtDDLAddConstraintUnique(const NAString &tableName,
                             ElemDDLNode *);  // DO NOT USE
  StmtDDLAddConstraintUnique(OperatorTypeEnum operatorType, const NAString &tableName,
                             ElemDDLNode *);                                  // DO NOT USE
  StmtDDLAddConstraintUnique(const StmtDDLAddConstraintUnique &);             // DO NOT USE
  StmtDDLAddConstraintUnique &operator=(const StmtDDLAddConstraintUnique &);  // DO NOT USE

};  // class StmtDDLAddConstraintUnique

#endif  // STMTDDLADDCONSTRAINTUNIQUE_H
