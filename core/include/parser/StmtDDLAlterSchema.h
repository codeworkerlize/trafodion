
#ifndef STMTDDLALTERSCHEMA_H
#define STMTDDLALTERSCHEMA_H

/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLAlterSchema.h
 * Description:  Alter Schema Statement (parse node)
 *
 *
 * Created:
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "parser/ElemDDLSchemaName.h"
#include "parser/StmtDDLAlterTableStoredDesc.h"
#include "parser/StmtDDLNode.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLAlterSchema;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// Alter Schema statement
// -----------------------------------------------------------------------
class StmtDDLAlterSchema : public StmtDDLNode {
 public:
  // called for 'drop all tables'
  StmtDDLAlterSchema(const ElemDDLSchemaName &aSchemaNameParseNode, CollHeap *heap = PARSERHEAP());

  // called for 'rename schema'
  StmtDDLAlterSchema(const ElemDDLSchemaName &aSchemaNameParseNode, NAString &renamedSchName,
                     CollHeap *heap = PARSERHEAP());

  // called for 'stored descriptor' processing
  StmtDDLAlterSchema(const ElemDDLSchemaName &aSchemaNameParseNode,
                     const StmtDDLAlterTableStoredDesc::AlterStoredDescType oper, CollHeap *heap = PARSERHEAP());

  void initChecks();

  // virtual destructor
  virtual ~StmtDDLAlterSchema();

  // cast
  virtual StmtDDLAlterSchema *castToStmtDDLAlterSchema();

  //
  // accessors
  //

  inline const NAString &getSchemaName() const;
  inline const SchemaName &getSchemaNameAsQualifiedName() const;
  inline SchemaName &getSchemaNameAsQualifiedName();

  //
  // other public methods
  //

  NABoolean isDropAllTables() { return dropAllTables_; }
  const NABoolean isDropAllTables() const { return dropAllTables_; }

  NABoolean isRenameSchema() { return renameSchema_; }
  const NABoolean isRenameSchema() const { return renameSchema_; }

  NABoolean isAlterStoredDesc() { return alterStoredDesc_; }
  const NABoolean isAlterStoredDesc() const { return alterStoredDesc_; }

  NAString &getRenamedSchemaName() { return renamedSchName_; }
  const NAString &getRenamedSchemaName() const { return renamedSchName_; }

  StmtDDLAlterTableStoredDesc::AlterStoredDescType &getStoredDescOperation() { return storedDescOper_; }

  // method for processing
  ExprNode *bindNode(BindWA *bindWAPtr);

  // method for collecting information
  //  void synthesize();

  // methods for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString getText() const;

 private:
  // ---------------------------------------------------------------------
  // private methods
  // ---------------------------------------------------------------------

  //
  // please do not use the following methods
  //

  StmtDDLAlterSchema();                                       // DO NOT USE
  StmtDDLAlterSchema(const StmtDDLAlterSchema &);             // DO NOT USE
  StmtDDLAlterSchema &operator=(const StmtDDLAlterSchema &);  // DO NOT USE

  // ---------------------------------------------------------------------
  // private data members
  // ---------------------------------------------------------------------

  NAString schemaName_;
  SchemaName schemaQualName_;
  NABoolean dropAllTables_;
  NABoolean renameSchema_;
  NAString renamedSchName_;
  NABoolean alterStoredDesc_;
  StmtDDLAlterTableStoredDesc::AlterStoredDescType storedDescOper_;

};  // class StmtDDLAlterSchema

// -----------------------------------------------------------------------
// definitions of inline methods for class StmtDDLAlterSchema
// -----------------------------------------------------------------------

//
// accessors
//
inline SchemaName &StmtDDLAlterSchema::getSchemaNameAsQualifiedName() { return schemaQualName_; }

inline const SchemaName &StmtDDLAlterSchema::getSchemaNameAsQualifiedName() const { return schemaQualName_; }

inline const NAString &StmtDDLAlterSchema::getSchemaName() const { return schemaName_; }

#endif  // STMTDDLALTERSCHEMA_H
