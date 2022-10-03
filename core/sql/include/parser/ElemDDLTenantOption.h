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
#ifndef ELEMDDLTENANTOPTION_H
#define ELEMDDLTENANTOPTION_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLTenantOption.h
 * Description:  Tenant options
 *
 * Language:     C++
 *
 *****************************************************************************
 */

#include "common/ComSmallDefs.h"
#include "ElemDDLNode.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLTenantOption;

// -----------------------------------------------------------------------
// ElemDDLTenantOption
//
// A temporary parse node to contain a schema name and an optional
// authorization identifier.
// -----------------------------------------------------------------------
class ElemDDLTenantOption : public ElemDDLNode
{

public:

  enum TenantOptions { TENANT_OPT_UNKNOWN = 0,
                       TENANT_OPT_ADD_GROUP_LIST = 1,
                       TENANT_OPT_ADD_NODE_LIST = 2,
                       TENANT_OPT_ADD_RGROUP_LIST = 3,
                       TENANT_OPT_ADD_RGROUP_NODES = 4,
                       TENANT_OPT_ADD_SCHEMA_LIST = 5,
                       TENANT_OPT_ADMIN_ROLE = 6,
                       TENANT_OPT_AFFINITY = 7,
                       TENANT_OPT_BALANCE = 8,
                       TENANT_OPT_CLUSTER_SIZE = 9,
                       TENANT_OPT_DEFAULT_SCHEMA = 10,
                       TENANT_OPT_DROP_GROUP_LIST = 11,
                       TENANT_OPT_DROP_NODE_LIST = 12,
                       TENANT_OPT_DROP_RGROUP_LIST = 13,
                       TENANT_OPT_DROP_RGROUP_NODES = 14,
                       TENANT_OPT_DROP_SCHEMA_LIST = 15,
                       TENANT_OPT_REPLACE_RGROUP_LIST = 16,
                       TENANT_OPT_SESSIONS = 17,
                       TENANT_OPT_TENANT_SIZE = 18 };
                         
  // constructors
  // option value
  ElemDDLTenantOption(TenantOptions tenantOption,
                      const char * optionValue,
                      CollHeap *h=PARSERHEAP())
    : ElemDDLNode(ELM_TENANT_OPTION_ELEM),
      tenantOption_(tenantOption),
      optionValue_((char *)optionValue),
      optionList_(NULL),
      nodeList_(NULL),
      schema_(NULL)
    {}

  // schema name
  ElemDDLTenantOption(TenantOptions tenantOption,
                      const SchemaName *schemaName,
                      CollHeap *h=PARSERHEAP())
    : ElemDDLNode(ELM_TENANT_OPTION_ELEM),
      tenantOption_(tenantOption),
      optionValue_(NULL),
      optionList_(NULL),
      nodeList_(NULL),
      schema_((SchemaName *)schemaName) {}

  // option list
  ElemDDLTenantOption(TenantOptions tenantOption,
                      ElemDDLNode * optionList,
                      CollHeap *h=PARSERHEAP())
    : ElemDDLNode(ELM_TENANT_OPTION_ELEM),
      tenantOption_(tenantOption),
      optionValue_(NULL),
      optionList_(optionList),
      nodeList_(NULL),
      schema_(NULL) {}

  // node list
  ElemDDLTenantOption(TenantOptions tenantOption,
                      const ConstStringList *optionList,
                      CollHeap *h=PARSERHEAP())
    : ElemDDLNode(ELM_TENANT_OPTION_ELEM),
      tenantOption_(tenantOption),
      optionValue_(NULL),
      optionList_(NULL),
      nodeList_((ConstStringList *)optionList),
      schema_(NULL) {}

  // option value and node list
  ElemDDLTenantOption(TenantOptions tenantOption,
                      char * optionValue,
                      const ConstStringList *optionList,
                      CollHeap *h=PARSERHEAP())
    : ElemDDLNode(ELM_TENANT_OPTION_ELEM),
      tenantOption_(tenantOption),
      optionValue_(optionValue),
      optionList_(NULL),
      nodeList_((ConstStringList *)optionList),
      schema_(NULL) {}

  // copy ctor
  ElemDDLTenantOption (const ElemDDLTenantOption & orig, CollHeap * h=PARSERHEAP()) ; // not written

  // virtual destructor
  virtual ~ElemDDLTenantOption()
  {
  }

  // cast
  virtual ElemDDLTenantOption * castToElemDDLTenantOption() { return this; };

  // accessors
  const ElemDDLNode     *getGroupList() const   { return optionList_; }
  const ConstStringList *getNodeList() const    { return nodeList_; }
  const NABoolean        getOptionEnum() const  { return tenantOption_; }
  const char            *getOptionValue() const { return optionValue_; }
  const SchemaName      *getSchema() const      { return schema_; }
  const ElemDDLNode     *getRGroupList() const  { return optionList_; }
  const ElemDDLNode     *getSchemaList() const  { return optionList_; }

private:

  TenantOptions    tenantOption_;
  char            *optionValue_;
  SchemaName      *schema_;
  ElemDDLNode     *optionList_;
  ConstStringList *nodeList_;

}; // class ElemDDLTenantOption


class ElemDDLTenantOptionList : public ElemDDLList
{

public:
  
  // default constructor
  ElemDDLTenantOptionList(ElemDDLNode * commaExpr, ElemDDLNode * otherExpr)
  : ElemDDLList(ELM_TENANT_OPTION_ELEM, commaExpr, otherExpr) {}
  
  // virtual destructor
  virtual ~ElemDDLTenantOptionList() {};

  virtual ElemDDLTenantOptionList * castToElemDDLTenantOptionList() { return this; }
private:

}; // class ElemDDLTenantOptionList

#endif /* ELEMDDLTENANTOPTION_H */


