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
#ifndef ELEMDDLTENANTRGROUP_H
#define ELEMDDLTENANTRGROUP_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLTenantResourceGroup.h
 * Description:  Description of a resource group for a tenant
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
class ElemDDLTenantResourceGroup;

// -----------------------------------------------------------------------
// ElemDDLTenantResourceGroup
//
// A temporary parse node to contain a group name and an optional
// configuration section
// -----------------------------------------------------------------------
class ElemDDLTenantResourceGroup : public ElemDDLNode {
 public:
  // default constructor
  ElemDDLTenantResourceGroup(const NAString &groupName, ConstStringList *nodeList, CollHeap *heap = PARSERHEAP())
      : ElemDDLNode(ELM_TENANT_RGROUP_ELEM), groupName_(groupName, heap), nodeList_(nodeList) {}

  // copy ctor
  ElemDDLTenantResourceGroup(const ElemDDLTenantResourceGroup &orig, CollHeap *heap = PARSERHEAP());  // not written

  // virtual destructor
  virtual ~ElemDDLTenantResourceGroup() {}

  // cast
  virtual ElemDDLTenantResourceGroup *castToElemDDLTenantResourceGroup() { return this; };

  // accessors

  const NAString getGroupName() const { return groupName_; }
  const ConstStringList *getNodeList() const { return nodeList_; }

 private:
  NAString groupName_;
  ConstStringList *nodeList_;

};  // class ElemDDLTenantResourceGroup

#endif /* ELEMDDLTENANTRGROUP_H */
