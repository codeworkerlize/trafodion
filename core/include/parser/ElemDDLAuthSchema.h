
#ifndef ELEMDDLAUTHSCHEMA_H
#define ELEMDDLAUTHSCHEMA_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLAuthSchema.h
 * Description:  Temporary parse node to contain schema name and
 *               schema class
 *
 * Created:      12/10/2014
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#ifndef SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#define SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#endif
#include "parser/ElemDDLSchemaName.h"
#include "common/ComSmallDefs.h"
#include "optimizer/ObjectNames.h"
#include "parser/SqlParserGlobals.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLAuthSchema;

// -----------------------------------------------------------------------
// ElemDDLAuthSchema
//
// A temporary parse node to contain a schema name and an optional
// authorization identifier.
// -----------------------------------------------------------------------
class ElemDDLAuthSchema : public ElemDDLNode {
 public:
  // default constructor
  ElemDDLAuthSchema(const SchemaName &aSchemaName, const ComSchemaClass schemaClass, CollHeap *h = PARSERHEAP())
      : ElemDDLNode(ELM_AUTH_SCHEMA_ELEM),
        schemaName_(aSchemaName, h),
        schemaClass_(schemaClass),
        isSchemaNameSpecified_(TRUE) {}

  ElemDDLAuthSchema(const ComSchemaClass schemaClass, CollHeap *h = PARSERHEAP())
      : ElemDDLNode(ELM_AUTH_SCHEMA_ELEM), schemaClass_(schemaClass), isSchemaNameSpecified_(FALSE) {}

  // copy ctor
  ElemDDLAuthSchema(const ElemDDLAuthSchema &orig, CollHeap *h = PARSERHEAP());  // not written

  // virtual destructor
  virtual ~ElemDDLAuthSchema();

  // cast
  virtual ElemDDLAuthSchema *castToElemDDLAuthSchema();

  // accessors

  const ComSchemaClass getSchemaClass() const { return schemaClass_; }

  const SchemaName &getSchemaName() const { return schemaName_; }

  NABoolean isSchemaNameSpecified() const { return isSchemaNameSpecified_; }

 private:
  SchemaName schemaName_;
  ComSchemaClass schemaClass_;
  NABoolean isSchemaNameSpecified_;

};  // class ElemDDLAuthSchema

#endif /* ELEMDDLAUTHSCHEMA_H */
