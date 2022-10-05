#ifndef STMTDDLALTERCATALOG_H
#define STMTDDLALTERCATALOG_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLAlterCatalog.h
 * Description:  base class for Alter Catalog statements
 *
 *
 * Created:      04-02-2007
 * Language:     C++
 *
 *


 *
 *
 *****************************************************************************
 */

#include "StmtDDLNode.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLAlterCatalog;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None

// -----------------------------------------------------------------------
// Create Catalog statement
// -----------------------------------------------------------------------
class StmtDDLAlterCatalog : public StmtDDLNode {
 public:
  // constructors
  StmtDDLAlterCatalog(const NAString &catalogName, NABoolean isEnable, const ElemDDLSchemaName &aSchemaName,
                      CollHeap *heap = PARSERHEAP());

  StmtDDLAlterCatalog(const NAString &catalogName, NABoolean isEnable, NABoolean disableEnableCreates);

  StmtDDLAlterCatalog(NABoolean isEnable);

  StmtDDLAlterCatalog(const NAString &catalogName, NABoolean isEnable, NABoolean disableEnableCreates,
                      const ElemDDLSchemaName &aSchemaName, CollHeap *heap = PARSERHEAP());

  // virtual destructor
  virtual ~StmtDDLAlterCatalog();

  // cast
  virtual StmtDDLAlterCatalog *castToStmtDDLAlterCatalog();

  //
  // accessors
  //

  virtual int getArity() const;
  inline const NAString &getCatalogName() const;
  inline const NAString &getSchemaName() const;
  inline const NABoolean isAllSchemaPrivileges() const;
  inline const NABoolean isEnableStatus() const;
  inline const NABoolean disableEnableCreates() const;
  inline const NABoolean disableEnableAllCreates() const;

  void setCatalogName(const NAString &catalogName);
  void setSchemaName(const ElemDDLSchemaName &aSchemaName);
  void setAllPrivileges(NABoolean isAll);

  // methods for tracing
  virtual const NAString getText() const;
  virtual const NAString displayLabel1() const;

  // method for binding
  ExprNode *bindNode(BindWA *bindWAPtr);

 private:
  // ---------------------------------------------------------------------
  // private methods
  // ---------------------------------------------------------------------

  StmtDDLAlterCatalog(OperatorTypeEnum operatorType, const NAString &catalogName,
                      ElemDDLNode *schemaInfo);                 // DO NOT USE
  StmtDDLAlterCatalog(const StmtDDLAlterCatalog &);             // DO NOT USE
  StmtDDLAlterCatalog &operator=(const StmtDDLAlterCatalog &);  // DO NOT USE

  // ---------------------------------------------------------------------
  // private data members
  // ---------------------------------------------------------------------

  // QualifiedName tableQualName_;
  // Need to add Catalog name
  NAString catalogName_;
  NAString schemaName_;
  SchemaName schemaQualName_;

  NABoolean enableStatus_;
  NABoolean isAllSchemaPrivileges_;
  NABoolean disableEnableCreates_;
  NABoolean disableEnableAllCreates_;

  // pointer to child parse node

  enum { INDEX_ALTER_CATALOG_ACTION = 0, MAX_STMT_DDL_ALTER_CATALOG_ARITY };

};  // class StmtDDLAlterCatalog

// -----------------------------------------------------------------------
// definitions of inline methods for class StmtDDLAlterCatalog
// -----------------------------------------------------------------------
//
// accessors
//

inline const NABoolean StmtDDLAlterCatalog::isAllSchemaPrivileges() const { return isAllSchemaPrivileges_; }

inline const NABoolean StmtDDLAlterCatalog::isEnableStatus() const { return enableStatus_; }

inline const NABoolean StmtDDLAlterCatalog::disableEnableCreates() const { return disableEnableCreates_; }

inline const NABoolean StmtDDLAlterCatalog::disableEnableAllCreates() const { return disableEnableAllCreates_; }

inline const NAString &StmtDDLAlterCatalog::getCatalogName() const { return catalogName_; }

inline const NAString &StmtDDLAlterCatalog::getSchemaName() const { return schemaName_; }

#endif  // STMTDDLALTERCATALOG_H
