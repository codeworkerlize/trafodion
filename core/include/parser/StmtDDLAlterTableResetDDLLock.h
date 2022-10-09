
#ifndef STMTDDLALTERTABLERESETDDLLOCK_H
#define STMTDDLALTERTABLERESETDDLLOCK_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLAlterTableResetDDLLock.h
 * Description:  class for Alter Table reset ddl lock
 *               DDL statements
 *
 * Created:      11/08/2019
 * Language:     C++
 *
 *****************************************************************************
 */

#include "parser/StmtDDLAlterTable.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLAlterTableResetDDLLock;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// definition of class StmtDDLAlterTableResetDDLLock
// -----------------------------------------------------------------------
class StmtDDLAlterTableResetDDLLock : public StmtDDLAlterTable {
 public:
  // constructor
  StmtDDLAlterTableResetDDLLock(const QualifiedName &objectName, bool objectLock = false, bool force = false)
      : StmtDDLAlterTable(DDL_ALTER_TABLE_RESET_DDL_LOCK),
        objName_(objectName, PARSERHEAP()),
        objectLock_(objectLock),
        force_(force) {}

  // virtual destructor
  virtual ~StmtDDLAlterTableResetDDLLock();

  // cast
  virtual StmtDDLAlterTableResetDDLLock *castToStmtDDLAlterTableResetDDLLock();

  // accessors
  inline const QualifiedName &getObjName() const;

  // method for tracing
  virtual const NAString getText() const;

  inline const bool objectLock() const { return objectLock_; }
  inline const bool force() const { return force_; }

 private:
  QualifiedName objName_;
  bool objectLock_;
  bool force_;

};  // class StmtDDLAlterTableResetDDLLock

// -----------------------------------------------------------------------
// definitions of inline methods for class StmtDDLAlterTableResetDDLLock
// -----------------------------------------------------------------------

inline const QualifiedName &StmtDDLAlterTableResetDDLLock::getObjName() const { return objName_; }

#endif  // STMTDDLALTERTABLERESETDDLLOCK_H
