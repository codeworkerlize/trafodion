#ifndef STMTDDLRESOURCEGROUP_H
#define STMTDDLRESOURCEGROUP_H
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
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLResourceGroup.h
 * Description:  Class for parse nodes representing create, alter, and drop
 *               resource groups
 *
 * Language:     C++
 *
 *****************************************************************************
 */

#include "common/ComSmallDefs.h"
#include "StmtDDLNode.h"
#include "ElemDDLNode.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLResourceGroup;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None

// -----------------------------------------------------------------------
// Create and drop role statements
// -----------------------------------------------------------------------
class StmtDDLResourceGroup : public StmtDDLNode {
 public:
  enum RGroupAlterType { NOT_ALTER = 0, ALTER_ADD_NODE = 1, ALTER_DROP_NODE = 2, ALTER_OFFLINE = 3, ALTER_ONLINE = 4 };

  // constructors
  // create resource group
  StmtDDLResourceGroup(const NAString &rGroupName, const ConstStringList *nodeList, NAString *owner, CollHeap *heap)
      : StmtDDLNode(DDL_CREATE_RESOURCE_GROUP),
        rGroupName_(rGroupName),
        nodeList_(nodeList),
        owner_(owner),
        alterType_(NOT_ALTER) {}

  // alter resource group
  StmtDDLResourceGroup(const Int32 alterType, const NAString &rGroupName, const ConstStringList *nodeList,
                       CollHeap *heap)
      : StmtDDLNode(DDL_ALTER_RESOURCE_GROUP),
        rGroupName_(rGroupName),
        nodeList_(nodeList),
        owner_(NULL),
        alterType_((RGroupAlterType)alterType) {}

  // drop resource group
  StmtDDLResourceGroup(const NAString &rGroupName, CollHeap *heap)
      : StmtDDLNode(DDL_DROP_RESOURCE_GROUP),
        rGroupName_(rGroupName),
        nodeList_(NULL),
        owner_(NULL),
        alterType_(NOT_ALTER) {}

  // virtual destructor
  virtual ~StmtDDLResourceGroup() {}

  // cast
  virtual StmtDDLResourceGroup *castToStmtDDLResourceGroup() { return this; }

  // for binding
  ExprNode *bindNode(BindWA *bindWAPtr);

  // accessors

  inline const NAString &getGroupName() const { return rGroupName_; }
  inline const ConstStringList *getNodeList() const { return nodeList_; }
  inline const NAString *getOwner() const { return owner_; }

  RGroupAlterType getAlterType() { return alterType_; }

 private:
  // ---------------------------------------------------------------------
  // private data members
  // ---------------------------------------------------------------------

  RGroupAlterType alterType_;
  NAString rGroupName_;
  const ConstStringList *nodeList_;
  NAString *owner_;

};  // class StmtDDLResourceGroup

#endif  // STMTDDLRESOURCEGROUP_H
