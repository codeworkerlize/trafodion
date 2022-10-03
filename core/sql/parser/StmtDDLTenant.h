#ifndef STMTDDLTENANT_H
#define STMTDDLTENANT_H
//******************************************************************************
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
//******************************************************************************
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLTenant.h
 * Description:  class for parse nodes representing register, alter and  
 *                 unregister tenant statements
 *
 *****************************************************************************
 */

#include "ComSmallDefs.h"
#include "StmtDDLNode.h"
#include "ElemDDLTenantGroup.h"
#include "ElemDDLTenantResourceGroup.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLTenant;

// -----------------------------------------------------------------------
// Register, alter, and unregister tenant statements
// -----------------------------------------------------------------------
class StmtDDLTenant : public StmtDDLNode
{

public:

  enum TenantAlterType { NOT_ALTER = 0, 
                         ALTER_DEFAULT_SCHEMA = 1, 
                         ALTER_ADD_SCHEMA = 2,
                         ALTER_DROP_SCHEMA = 3,
                         ALTER_TENANT_OPTIONS = 4};

  // constructors
  // register tenant
  StmtDDLTenant(const NAString & tenantName,
                      ElemDDLNode *optionList,
                      ElemDDLNode * schemaList,
                      CollHeap * heap);

  // alter tenant 
  StmtDDLTenant(const Int32 alterType,
                const NAString & tenantName,
                      ElemDDLNode * optionList,
                      SchemaName * tenantSchema,
                      ElemDDLNode * schemaList,
                      CollHeap * heap);

  // unregister tenant
  StmtDDLTenant(const NAString & tenantName,
                      NABoolean dropDependencies,
                      CollHeap * heap);

  // virtual destructor
  virtual ~StmtDDLTenant();

  // cast
  virtual StmtDDLTenant * castToStmtDDLTenant();

  // for binding
  ExprNode * bindNode(BindWA *bindWAPtr);

  // accessors

  inline const NAString & getTenantName() const { return tenantName_; }
  inline const OperatorTypeEnum getTenantOp() const { return getOperatorType(); }
  inline const TenantAlterType getAlterType() const { return tenantAlterType_; }
  inline const NABoolean dropDependencies() const { return dropDependencies_; }
  inline const ElemDDLNode * getGroupList() const { return groupList_; }
  inline const ElemDDLNode * getRGroupList() const { return rgroupList_; }
  inline const ConstStringList * getNodeList() const { return nodeList_; }
  inline const LIST(SchemaName *) * getSchemaList() const { return schemaList_; }
  inline const NABoolean addGroupList() const { return addGroupList_; }
  inline const NABoolean addRGroupList() const { return addRGroupList_; }
  inline const NABoolean dropRGroupList() const { return dropRGroupList_; }
  inline const NABoolean replaceRGroupList() const { return replaceRGroupList_; }
  inline const NABoolean addNodeList() const { return addNodeList_; }
  inline const NABoolean addSchemaList() const { return addSchemaList_; }
  inline const NABoolean asSizing(); 
  inline const Int32 getAffinity() const { return affinity_; }
  inline const Int32 getSessionLimit() const { return sessionLimit_; }
  inline const Int32 getClusterSize() const { return clusterSize_; }
  inline const Int32 getTenantSize() const { return tenantSize_; }
  inline const NAString *getRoleName() const { return roleName_; }
  inline const SchemaName *getDefaultSchema() const { return defaultSchema_; }
  
  inline const NABoolean isAffinitySizing() const { return tenantAffinitySizing_; }
  inline const NABoolean isDefaultSchemaOpt() const { return defaultSchemaSpecified_; }
  inline const NABoolean isTenantSizeSpecified() const { return tenantSizeSpecified_; }
  inline const NABoolean isZookeeperUpdate() const 
  {
    return (rgroupList_ || nodeList_ || tenantAffinitySizing_ ||
            tenantSizeSpecified_ || sessionLimitSpecified_ || 
            balance_ || defaultSchema_);
  }
  inline const NABoolean doNodeBalance() const 
  { 
    return (rgroupList_ || nodeList_ || tenantAffinitySizing_ ||
            tenantSizeSpecified_ || balance_ );
  }

  // for tracing

private:

  // ---------------------------------------------------------------------
  // private data members
  // ---------------------------------------------------------------------

  NAString tenantName_;
  TenantAlterType tenantAlterType_;
  NABoolean dropDependencies_;
  NAString *roleName_;
  Int32 affinity_;
  NABoolean balance_;
  Int32 clusterSize_;
  Int32 sessionLimit_;
  NABoolean sessionLimitSpecified_;
  NABoolean tenantAffinitySizing_;
  Int32 tenantSize_;
  NABoolean tenantSizeSpecified_;
  SchemaName *defaultSchema_;

  ElemDDLNode *groupList_;
  NABoolean addGroupList_;

  ElemDDLNode *rgroupList_;
  NABoolean addRGroupList_;
  NABoolean dropRGroupList_;
  NABoolean replaceRGroupList_;
  ConstStringList *nodeList_;
  NABoolean addNodeList_;

  NABoolean addSchemaList_;
  LIST(SchemaName *) *schemaList_;
  NABoolean defaultSchemaSpecified_;

  CollHeap *heap_;

}; // class StmtDDLTenant

#endif // STMTDDLTENANT_H