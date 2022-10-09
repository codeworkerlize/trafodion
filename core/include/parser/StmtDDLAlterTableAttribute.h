#ifndef STMTDDLALTERTABLEATTRIBUTE_H
#define STMTDDLALTERTABLEATTRIBUTE_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLAlterTableAttribute.h
 * Description:  class for Alter Table <table-name> Attribute(s)
 *               DDL statements
 *
 *               The methods in this class are defined either in this
 *               header file or in the source file StmtDDLAlter.C.
 *
 *
 * Created:      1/15/96
 * Language:     C++
 *
 *

 *
 *
 *****************************************************************************
 */

#include "parser/ParDDLFileAttrsAlterTable.h"
#include "parser/StmtDDLAlterTable.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLAlterTableAttribute;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// definition of class StmtDDLAlterTableAttribute
// -----------------------------------------------------------------------
class StmtDDLAlterTableAttribute : public StmtDDLAlterTable {
 public:
  // constructor
  StmtDDLAlterTableAttribute(ElemDDLNode *pFileAttrNode);

  // virtual destructor
  virtual ~StmtDDLAlterTableAttribute();

  // accessor
  inline ParDDLFileAttrsAlterTable &getFileAttributes();
  inline const ParDDLFileAttrsAlterTable &getFileAttributes() const;

  // cast
  virtual StmtDDLAlterTableAttribute *castToStmtDDLAlterTableAttribute();

  // method for tracing
  virtual NATraceList getDetailInfo() const;
  virtual const NAString getText() const;

 private:
  // ---------------------------------------------------------------------
  // private methods
  // ---------------------------------------------------------------------

  void setFileAttributes(ElemDDLFileAttrClause *pFileAttrClause);

  // Copies the information in the specified file
  // attribute clause (pointed to by pFileAttrClause)
  // to data member fileAttributes_ in this object.
  //
  // This method can only be invoked during the
  // construction of this object when the (file)
  // attributes clause appears.

  // ---------------------------------------------------------------------
  // private data members
  // ---------------------------------------------------------------------

  ParDDLFileAttrsAlterTable fileAttributes_;

};  // class StmtDDLAlterTableAttribute

// -----------------------------------------------------------------------
// definitions of inline methods for class StmtDDLAlterTableAttribute
// -----------------------------------------------------------------------

//
// accessors
//

inline const ParDDLFileAttrsAlterTable &StmtDDLAlterTableAttribute::getFileAttributes() const {
  return fileAttributes_;
}

inline ParDDLFileAttrsAlterTable &StmtDDLAlterTableAttribute::getFileAttributes() { return fileAttributes_; }

#endif  // STMTDDLALTERTABLEATTRIBUTE_H
