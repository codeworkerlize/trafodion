
#ifndef ELEMDDLSCHEMANAME_H
#define ELEMDDLSCHEMANAME_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLSchemaName.h
 * Description:  Temporary parse node to contain schema name and
 *               authorization identifier
 *
 * Created:      4/3/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "optimizer/ObjectNames.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLSchemaName;

// -----------------------------------------------------------------------
// ElemDDLSchemaName
//
// A temporary parse node to contain a schema name and an optional
// authorization identifier.
// -----------------------------------------------------------------------
class ElemDDLSchemaName : public ElemDDLNode {
 public:
  // default constructor
  ElemDDLSchemaName(const SchemaName &aSchemaName, const NAString &anAuthorizationID = "", CollHeap *h = PARSERHEAP())
      : ElemDDLNode(ELM_SCHEMA_NAME_ELEM), schemaName_(aSchemaName, h), authorizationID_(anAuthorizationID, h) {}

  // copy ctor
  ElemDDLSchemaName(const ElemDDLSchemaName &orig, CollHeap *h = PARSERHEAP());  // not written

  // virtual destructor
  virtual ~ElemDDLSchemaName();

  // cast
  virtual ElemDDLSchemaName *castToElemDDLSchemaName();

  // accessors

  const NAString &getAuthorizationID() const { return authorizationID_; }

  const SchemaName &getSchemaName() const { return schemaName_; }

 private:
  SchemaName schemaName_;
  NAString authorizationID_;

};  // class ElemDDLSchemaName

#endif /* ELEMDDLSCHEMANAME_H */
