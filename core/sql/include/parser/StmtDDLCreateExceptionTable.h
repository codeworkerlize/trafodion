#ifndef STMTDDLCREATEEXCEPTIONTABLE_H
#define STMTDDLCREATEEXCEPTIONTABLE_H

//**********************************************************************

/*
******************************************************************************
*
* File:          StmtDDLCreateExceptionTable.h
* RCS:           $Id:
* Description:   class for parse node representing create exception table
*                statement.
*
* Created:       08/27/07
* Language:      C++
*
******************************************************************************
*/
#include "common/ComSmallDefs.h"
#include "StmtDDLNode.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLCreateExceptionTable;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None

// -----------------------------------------------------------------------
// Create Exception table statement
// -----------------------------------------------------------------------
class StmtDDLCreateExceptionTable : public StmtDDLNode {
 public:
  // constructor
  StmtDDLCreateExceptionTable();
  StmtDDLCreateExceptionTable(const QualifiedName &exceptionname, const QualifiedName &objectreference);

  // virtual destructor
  virtual ~StmtDDLCreateExceptionTable();

  // cast
  virtual StmtDDLCreateExceptionTable *castToStmtDDLCreateExceptionTable();

  // method for binding
  ExprNode *bindNode(BindWA *bindWAPtr);

  // accessors
  inline const NAString getExceptionName() const;
  inline const NAString getObjectReference() const;

  QualifiedName const &getExceptionNameAsQualifiedName() const { return exceptionName_; }

  QualifiedName const &getObjectReferenceAsQualifiedName() const { return objectReference_; }

  // for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString displayLabel2() const;
  virtual const NAString getText() const;

 private:
  QualifiedName exceptionName_;
  QualifiedName objectReference_;

};  // class StmtDDLCreateExceptionTable

//----------------------------------------------------------------------------
// definitions of inline methods for class StmtDDLCreateExceptionTable
//----------------------------------------------------------------------------

// accessors
inline const NAString StmtDDLCreateExceptionTable::getExceptionName() const {
  NAString exceptionname = exceptionName_.getQualifiedNameAsAnsiString();
  return exceptionname;
}

inline const NAString StmtDDLCreateExceptionTable::getObjectReference() const {
  NAString objectreference = objectReference_.getQualifiedNameAsAnsiString();
  return objectreference;
}

#endif  // STMTDDLCREATEEXCEPTIONTABLE_H
