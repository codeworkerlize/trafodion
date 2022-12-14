#ifndef STMTDDLGIVE_H
#define STMTDDLGIVE_H

/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLGive.h
 * Description:  class for parse nodes representing Give statements
 *
 *
 * Created:      1/28/2015
 * Language:     C++
 *
 *****************************************************************************
 */

#include "common/ComSmallDefs.h"
#include "parser/StmtDDLNode.h"
// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLGiveAll;
class StmtDDLGiveObject;
class StmtDDLGiveSchema;

// -----------------------------------------------------------------------
// Give All statement
// -----------------------------------------------------------------------
class StmtDDLGiveAll : public StmtDDLNode {
 public:
  StmtDDLGiveAll(const NAString &fromID, const NAString &toID);

  // virtual destructor
  virtual ~StmtDDLGiveAll();

  // cast
  virtual StmtDDLGiveAll *castToStmtDDLGiveAll();

  //
  // accessors
  //
  inline const NAString &getFromID() const;
  inline const NAString &getToID() const;

  // for binding
  ExprNode *bindNode(BindWA *bindWAPtr);

  // for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString displayLabel2() const;
  virtual const NAString getText() const;

 private:
  // ---------------------------------------------------------------------
  // private data members
  // ---------------------------------------------------------------------
  NAString fromAuthID_;
  NAString toAuthID_;

};  // class StmtDDLGiveAll

// -----------------------------------------------------------------------
// definitions of inline methods for class StmtDDLGiveAll
// -----------------------------------------------------------------------

//
// accessors
//

inline const NAString &StmtDDLGiveAll::getFromID() const { return fromAuthID_; }

inline const NAString &StmtDDLGiveAll::getToID() const { return toAuthID_; }

// -----------------------------------------------------------------------
// Give Object statement
// -----------------------------------------------------------------------
class StmtDDLGiveObject : public StmtDDLNode {
 public:
  // default constructor
  StmtDDLGiveObject(ComObjectType objectType, const QualifiedName &objectName, const NAString &aUserID);

  // virtual destructor
  virtual ~StmtDDLGiveObject();

  // cast
  virtual StmtDDLGiveObject *castToStmtDDLGiveObject();

  //
  // accessors
  //
  inline const QualifiedName &getObjectNameAsQualifiedName() const;
  inline QualifiedName &getObjectNameAsQualifiedName();
  inline NAString getObjectName() const;

  inline const NAString &getUserID() const;

  // for binding
  ExprNode *bindNode(BindWA *bindWAPtr);

  // for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString displayLabel2() const;
  virtual const NAString getText() const;

 private:
  // ---------------------------------------------------------------------
  // private data members
  // ---------------------------------------------------------------------
  ComObjectType objectType_;
  NAString objectName_;
  QualifiedName objectQualName_;
  NAString userID_;

};  // class StmtDDLGiveObject

// -----------------------------------------------------------------------
// definitions of inline methods for class StmtDDLGiveObject
// -----------------------------------------------------------------------

//
// accessors
//

inline QualifiedName &StmtDDLGiveObject::getObjectNameAsQualifiedName() { return objectQualName_; }

inline const QualifiedName &StmtDDLGiveObject::getObjectNameAsQualifiedName() const { return objectQualName_; }
inline NAString StmtDDLGiveObject::getObjectName() const { return objectName_; }

inline const NAString &StmtDDLGiveObject::getUserID() const { return userID_; }

// -----------------------------------------------------------------------
// Give Schema statement
// -----------------------------------------------------------------------
class StmtDDLGiveSchema : public StmtDDLNode {
 public:
  // default constructor
  StmtDDLGiveSchema(const SchemaName &schemaName, const NAString &authID, ComDropBehavior dropBehavior);

  // virtual destructor
  virtual ~StmtDDLGiveSchema();

  // cast
  virtual StmtDDLGiveSchema *castToStmtDDLGiveSchema();

  //
  // accessors
  //
  inline const NAString &getAuthID() const;
  inline const NAString &getCatalogName() const;
  inline ComDropBehavior getDropBehavior() const;
  inline const NAString &getSchemaName() const;

  // for binding
  ExprNode *bindNode(BindWA *bindWAPtr);

  // for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString displayLabel2() const;
  virtual const NAString getText() const;

 private:
  // ---------------------------------------------------------------------
  // private data members
  // ---------------------------------------------------------------------
  NAString authID_;
  NAString catalogName_;
  ComDropBehavior dropBehavior_;
  NAString schemaName_;

};  // class StmtDDLGiveSchema

// -----------------------------------------------------------------------
// definitions of inline methods for class StmtDDLGiveSchema
// -----------------------------------------------------------------------

//
// accessors
//

inline const NAString &StmtDDLGiveSchema::getAuthID() const { return authID_; }

inline const NAString &StmtDDLGiveSchema::getCatalogName() const { return catalogName_; }

inline ComDropBehavior StmtDDLGiveSchema::getDropBehavior() const { return dropBehavior_; }

inline const NAString &StmtDDLGiveSchema::getSchemaName() const { return schemaName_; }

#endif  // STMTDDLGIVE_H
