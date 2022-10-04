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
#ifndef ELEMDDLTENANTGROUP_H
#define ELEMDDLTENANTGROUP_H
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

#ifndef SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#define SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#endif
#include "parser/SqlParserGlobals.h"
#include "optimizer/ObjectNames.h"
#include "common/ComSmallDefs.h"
#include "ElemDDLNode.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLTenantGroup;

// -----------------------------------------------------------------------
// ElemDDLTenantGroup
//
// A temporary parse node to contain a group name and an optional
// configuration section
// -----------------------------------------------------------------------
class ElemDDLTenantGroup : public ElemDDLNode {
 public:
  // default constructor
  ElemDDLTenantGroup(const NAString &groupName, const char *pConfig, CollHeap *heap = PARSERHEAP())
      : ElemDDLNode(ELM_TENANT_GROUP_ELEM), groupName_(groupName, heap) {
    if (pConfig == NULL)
      config_ = NAString("", heap);
    else
      config_ = NAString(pConfig, heap);
  }

  // copy ctor
  ElemDDLTenantGroup(const ElemDDLTenantGroup &orig, CollHeap *heap = PARSERHEAP());  // not written

  // virtual destructor
  virtual ~ElemDDLTenantGroup() {}

  // cast
  virtual ElemDDLTenantGroup *castToElemDDLTenantGroup() { return this; };

  // accessors

  const NAString getGroupName() const { return groupName_; }
  const NAString getConfig() const { return config_; }

 private:
  NAString groupName_;
  NAString config_;

};  // class ElemDDLTenantGroup

#if 0
class ElemDDLTenantGroupList : public ElemDDLList
{

public:
  
  // default constructor
  ElemDDLTenantGroupList(ElemDDLNode * commaExpr, ElemDDLNode * otherExpr)
  : ElemDDLList(ELM_TENANT_GROUP_ELEM, commaExpr, otherExpr) {}
  
  // virtual destructor
  virtual ~ElemDDLTenantGroupList() {};

  virtual ElemDDLTenantGroupList * castToElemDDLTenantGroupList() { return this; }
private:

}; // class ElemDDLTenantGroupList
#endif

#endif /* ELEMDDLTENANTGROUP_H */
