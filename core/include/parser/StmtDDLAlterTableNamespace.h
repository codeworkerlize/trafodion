
#ifndef STMTDDLALTERTABLENAMESPACE_H
#define STMTDDLALTERTABLENAMESPACE_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLAlterTableNamespace.h
 * Description:  class for Alter Table <table-name> namespace
 *               DDL statements
 *
 *               The methods in this class are defined either in this
 *               header file or the source file StmtDDLAlter.C.
 *
 *
 * Created:      5/16/2007
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
class StmtDDLAlterTableNamespace;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// definition of class StmtDDLAlterTableNamespace
// -----------------------------------------------------------------------
class StmtDDLAlterTableNamespace : public StmtDDLAlterTable {
 public:
  // constructor
  StmtDDLAlterTableNamespace(const QualifiedName &objectName)
      : StmtDDLAlterTable(DDL_ALTER_TABLE_NAMESPACE), objName_(objectName, PARSERHEAP()) {}

  // virtual destructor
  virtual ~StmtDDLAlterTableNamespace();

  // cast
  virtual StmtDDLAlterTableNamespace *castToStmtDDLAlterTableNamespace();

  // accessors
  inline const QualifiedName &getObjName() const;

  // method for tracing
  virtual const NAString getText() const;

 private:
  QualifiedName objName_;

};  // class StmtDDLAlterTableNamespace

// -----------------------------------------------------------------------
// definitions of inline methods for class StmtDDLAlterTableNamespace
// -----------------------------------------------------------------------

inline const QualifiedName &StmtDDLAlterTableNamespace::getObjName() const { return objName_; }

#endif  // STMTDDLALTERTABLENAMESPACE_H
