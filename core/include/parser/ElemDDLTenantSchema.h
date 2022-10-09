
#ifndef ELEMDDLTENANTSCHEMA_H
#define ELEMDDLTENANTSCHEMA_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLTenantSchema.h
 * Description:  Description of a tenant schema
 *
 * Language:     C++
 *
 *****************************************************************************
 */

#include "parser/ElemDDLNode.h"
#include "common/ComSmallDefs.h"
#include "optimizer/ObjectNames.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLTenantSchema;

// -----------------------------------------------------------------------
// ElemDDLTenantSchema
//
// A temporary parse node to contain a schema name and an optional
// authorization identifier.
// -----------------------------------------------------------------------
class ElemDDLTenantSchema : public ElemDDLNode {
 public:
  // default constructor
  ElemDDLTenantSchema(const SchemaName *schemaName, const NABoolean schemaDefault, CollHeap *h = PARSERHEAP())
      : ElemDDLNode(ELM_TENANT_SCHEMA_ELEM), schemaName_((SchemaName *)schemaName), defaultSchema_(schemaDefault) {}

  // copy ctor
  ElemDDLTenantSchema(const ElemDDLTenantSchema &orig, CollHeap *h = PARSERHEAP());  // not written

  // virtual destructor
  virtual ~ElemDDLTenantSchema() {
    if (schemaName_) delete schemaName_;
  }

  // cast
  virtual ElemDDLTenantSchema *castToElemDDLTenantSchema() { return this; };

  // accessors

  const NABoolean isDefaultSchema() const { return defaultSchema_; }
  const SchemaName *getSchemaName() const { return schemaName_; }

 private:
  SchemaName *schemaName_;
  NABoolean defaultSchema_;

};  // class ElemDDLTenantSchema

class ElemDDLTenantSchemaList : public ElemDDLList {
 public:
  // default constructor
  ElemDDLTenantSchemaList(ElemDDLNode *commaExpr, ElemDDLNode *otherExpr)
      : ElemDDLList(ELM_TENANT_SCHEMA_ELEM, commaExpr, otherExpr) {}

  // virtual destructor
  virtual ~ElemDDLTenantSchemaList(){};

  virtual ElemDDLTenantSchemaList *castToElemDDLTenantSchemaList() { return this; }

 private:
};  // class ElemDDLTenantSchemaList

#endif /* ELEMDDLTENANTSCHEMA_H */
