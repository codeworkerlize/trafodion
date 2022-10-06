#ifndef STMTDDLDROPEXCEPTIONTABLE_H
#define STMTDDLDROPEXCEPTIONTABLE_H

/* -*-C++-*-
******************************************************************************
*
* File:          StmtDDLDropExceptionTable.h
* RCS:           $Id:
* Description:   class for parse node representing drop exception table
*                statement.
*
* Created:       09/12/07
* Language:      C++
*
******************************************************************************
*/

#include "common/ComSmallDefs.h"
#include "parser/StmtDDLNode.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLDropExceptionTable;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None

// -----------------------------------------------------------------------
// Drop Exception table statement
// -----------------------------------------------------------------------
class StmtDDLDropExceptionTable : public StmtDDLNode {
 public:
  // constructor
  StmtDDLDropExceptionTable();
  StmtDDLDropExceptionTable(const QualifiedName &exceptionname, const QualifiedName &objectreference,
                            ComDropBehavior dropBehavior, NABoolean cleanupSpec, NAString *pLogFile);

  StmtDDLDropExceptionTable(const QualifiedName &objectreference, ComDropBehavior dropBehavior, NABoolean cleanupSpec,
                            NAString *pLogFile);
  // virtual destructor
  virtual ~StmtDDLDropExceptionTable();

  // cast
  virtual StmtDDLDropExceptionTable *castToStmtDDLDropExceptionTable();

  // method for binding
  ExprNode *bindNode(BindWA *bindWAPtr);

  // accessors
  inline ComDropBehavior getDropBehavior() const;
  inline const NABoolean isCleanupSpecified() const;
  inline const NABoolean isLogFileSpecified() const;
  inline const NAString &getLogFile() const;
  inline const NAString getExceptionName() const;
  inline const NAString getObjectReference() const;
  inline ComDropType getDropType() const;

  // for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString displayLabel2() const;
  virtual const NAString getText() const;

 private:
  QualifiedName exceptionName_;
  QualifiedName objectReference_;
  ComDropBehavior dropBehavior_;
  NABoolean isCleanupSpec_;
  ComDropType dropType_;
  NAString *pLogFile_;

};  // class StmtDDLDropExceptionTable

// -----------------------------------------------------------------------
// definitions of inline methods for class StmtDDLDropExceptionTable
// -----------------------------------------------------------------------

// accessors

inline ComDropBehavior StmtDDLDropExceptionTable::getDropBehavior() const { return dropBehavior_; }

inline ComDropType StmtDDLDropExceptionTable::getDropType() const { return dropType_; }

inline const NABoolean StmtDDLDropExceptionTable::isCleanupSpecified() const { return isCleanupSpec_; }
inline const NABoolean StmtDDLDropExceptionTable::isLogFileSpecified() const {
  if (pLogFile_) return TRUE;
  return FALSE;
}

inline const NAString &StmtDDLDropExceptionTable::getLogFile() const {
  ComASSERT(pLogFile_ NEQ NULL);
  return *pLogFile_;
}

inline const NAString StmtDDLDropExceptionTable::getExceptionName() const {
  NAString exceptionname = exceptionName_.getQualifiedNameAsAnsiString();
  return exceptionname;
}

inline const NAString StmtDDLDropExceptionTable::getObjectReference() const {
  NAString objectreference = objectReference_.getQualifiedNameAsAnsiString();
  return objectreference;
}

#endif  // STMTDDLDROPEXCEPTIONTABLE_H
