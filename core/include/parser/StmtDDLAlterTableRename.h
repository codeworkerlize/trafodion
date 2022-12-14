
#ifndef STMTDDLALTERTABLERENAME_H
#define STMTDDLALTERTABLERENAME_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLAlterTableRename.h
 * Description:  class for Alter Table <table-name> Rename <new-name>
 *               DDL statements
 *
 *               The methods in this class are defined either in this
 *               header file or the source file StmtDDLAlter.C.
 *
 *
 * Created:      9/19/95
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
class StmtDDLAlterTableRename;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// definition of class StmtDDLAlterTableRename
// -----------------------------------------------------------------------
class StmtDDLAlterTableRename : public StmtDDLAlterTable {
 public:
  // constructor
  StmtDDLAlterTableRename(const NAString &newName, ComBoolean isCascade, ComBoolean skipViewCheck,
                          ComBoolean skipTransactionCheck)
      : StmtDDLAlterTable(DDL_ALTER_TABLE_RENAME),
        newName_(newName, PARSERHEAP()),
        isCascade_(isCascade),
        skipViewCheck_(skipViewCheck),
        skipTransactionCheck_(skipTransactionCheck) {}

  // virtual destructor
  virtual ~StmtDDLAlterTableRename();

  // cast
  virtual StmtDDLAlterTableRename *castToStmtDDLAlterTableRename();

  // accessors
  inline const NAString &getNewName() const;
  inline ComBoolean isCascade() const;
  inline ComBoolean skipViewCheck() const;
  inline ComBoolean skipTransactionCheck() const;

  inline const NAString getNewNameAsAnsiString() const;

  // method for tracing
  virtual const NAString getText() const;

 private:
  NAString newName_;
  ComBoolean isCascade_;
  ComBoolean skipViewCheck_;
  ComBoolean skipTransactionCheck_;

};  // class StmtDDLAlterTableRename

// -----------------------------------------------------------------------
// definitions of inline methods for class StmtDDLAlterTableRename
// -----------------------------------------------------------------------

inline const NAString &StmtDDLAlterTableRename::getNewName() const { return newName_; }

inline const NAString StmtDDLAlterTableRename::getNewNameAsAnsiString() const {
  return QualifiedName(newName_).getQualifiedNameAsAnsiString();
}

inline ComBoolean StmtDDLAlterTableRename::isCascade() const { return isCascade_; }

inline ComBoolean StmtDDLAlterTableRename::skipViewCheck() const { return skipViewCheck_; }

inline ComBoolean StmtDDLAlterTableRename::skipTransactionCheck() const { return skipTransactionCheck_; }

#endif  // STMTDDLALTERTABLERENAME_H
