/**********************************************************************
// @@@ START COPYRIGHT @@@
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// @@@ END COPYRIGHT @@@
**********************************************************************/
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

#include "optimizer/ObjectNames.h"
#include "common/ComSmallDefs.h"
#include "ElemDDLNode.h"

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
class ElemDDLTenantSchema : public ElemDDLNode
{

public:

  // default constructor
  ElemDDLTenantSchema(const SchemaName *schemaName,
                      const NABoolean   schemaDefault,
                      CollHeap         *h=PARSERHEAP())
    : ElemDDLNode(ELM_TENANT_SCHEMA_ELEM),
      schemaName_((SchemaName *)schemaName),
      defaultSchema_(schemaDefault) {}
      
  // copy ctor
  ElemDDLTenantSchema (const ElemDDLTenantSchema & orig, CollHeap * h=PARSERHEAP()) ; // not written

  // virtual destructor
  virtual ~ElemDDLTenantSchema()
  {
    if (schemaName_)
      delete schemaName_;
  }

  // cast
  virtual ElemDDLTenantSchema * castToElemDDLTenantSchema() { return this; };

  // accessors

  const NABoolean isDefaultSchema() const { return defaultSchema_; }
  const SchemaName * getSchemaName() const { return schemaName_; }

private:

  SchemaName *schemaName_;
  NABoolean   defaultSchema_;

}; // class ElemDDLTenantSchema


class ElemDDLTenantSchemaList : public ElemDDLList
{

public:
  
  // default constructor
  ElemDDLTenantSchemaList(ElemDDLNode * commaExpr, ElemDDLNode * otherExpr)
  : ElemDDLList(ELM_TENANT_SCHEMA_ELEM, commaExpr, otherExpr) {}
  
  // virtual destructor
  virtual ~ElemDDLTenantSchemaList() {};

  virtual ElemDDLTenantSchemaList * castToElemDDLTenantSchemaList() { return this; }
private:

}; // class ElemDDLTenantSchemaList

#endif /* ELEMDDLTENANTSCHEMA_H */


