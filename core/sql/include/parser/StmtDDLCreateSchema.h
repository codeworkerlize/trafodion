
#ifndef STMTDDLCREATESCHEMA_H
#define STMTDDLCREATESCHEMA_H

/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLCreateSchema.h
 * Description:  Create Schema Statement (parse node)
 *
 *
 * Created:      3/9/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "ElemDDLSchemaName.h"
#include "parser/StmtDDLNode.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLCreateSchema;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// Create Schema statement
// -----------------------------------------------------------------------
class StmtDDLCreateSchema : public StmtDDLNode {
 public:
  // initialize constructor
  StmtDDLCreateSchema(const ElemDDLSchemaName &aSchemaName, ComSchemaClass schemaClass, CharType *pCharType,
                      NAString *namespace1 = NULL, NAString *encrypt = NULL, NABoolean storedDesc = FALSE,
                      NABoolean incrBackupEnabled = FALSE, CollHeap *heap = PARSERHEAP());

  // virtual destructor
  virtual ~StmtDDLCreateSchema();

  // cast
  virtual StmtDDLCreateSchema *castToStmtDDLCreateSchema();

  //
  // accessors
  //

  inline const NAString &getAuthorizationID() const;
  inline ComSchemaClass getSchemaClass() const;

  inline const NAString &getSchemaName() const;
  inline const SchemaName &getSchemaNameAsQualifiedName() const;
  inline SchemaName &getSchemaNameAsQualifiedName();

  NABoolean createIfNotExists() { return createIfNotExists_; }
  void setCreateIfNotExists(NABoolean v) { createIfNotExists_ = v; }

  NAString *getNamespace() const { return namespace_; }

  NABoolean useEncryption() const { return useEncryption_; }
  NAString encryptionOptions() const { return encryptionOptions_; }
  NABoolean encryptRowid() const { return encryptRowid_; }
  NABoolean encryptData() const { return encryptData_; }

  NABoolean storedDesc() const { return storedDesc_; }

  NABoolean incrBackupEnabled() const { return incrBackupEnabled_; }

  //
  // other public methods
  //

  // method for processing
  ExprNode *bindNode(BindWA *bindWAPtr);

  // method for collecting information
  void synthesize();

  // methods for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString displayLabel2() const;
  virtual const NAString getText() const;

  const CharType *getCharType() const;

 private:
  // ---------------------------------------------------------------------
  // private methods
  // ---------------------------------------------------------------------

  //
  // please do not use the following methods
  //

  StmtDDLCreateSchema();                                        // DO NOT USE
  StmtDDLCreateSchema(const StmtDDLCreateSchema &);             // DO NOT USE
  StmtDDLCreateSchema &operator=(const StmtDDLCreateSchema &);  // DO NOT USE

  // ---------------------------------------------------------------------
  // private data members
  // ---------------------------------------------------------------------

  NAString schemaName_;
  NAString authorizationID_;
  SchemaName schemaQualName_;
  CharType *pCharType_;
  ComSchemaClass schemaClass_;
  NABoolean createIfNotExists_;

  NAString *namespace_;
  NABoolean useEncryption_;
  NABoolean encryptRowid_;
  NABoolean encryptData_;

  NAString encryptionOptions_;

  NABoolean storedDesc_;

  // if TRUE, then all tables within this schema have incremental backup enabled
  NABoolean incrBackupEnabled_;
};  // class StmtDDLCreateSchema

// -----------------------------------------------------------------------
// definitions of inline methods for class StmtDDLCreateSchema
// -----------------------------------------------------------------------

//
// accessors
//
inline SchemaName &StmtDDLCreateSchema::getSchemaNameAsQualifiedName() { return schemaQualName_; }

inline const SchemaName &StmtDDLCreateSchema::getSchemaNameAsQualifiedName() const { return schemaQualName_; }

inline const NAString &StmtDDLCreateSchema::getAuthorizationID() const { return authorizationID_; }

inline ComSchemaClass StmtDDLCreateSchema::getSchemaClass() const { return schemaClass_; }

inline const NAString &StmtDDLCreateSchema::getSchemaName() const { return schemaName_; }

inline const CharType *StmtDDLCreateSchema::getCharType() const { return pCharType_; }

#endif  // STMTDDLCREATESCHEMA_H
