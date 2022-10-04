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
 * File:         AllStmtDDL.h
 * Description:  a header file that includes StmtDDLNode.h and all the files
 *               that define classes derived from StmtDDLNode.
 *
 *
 * Created:      3/30/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */


#include "StmtDDLNode.h"
#include "AllStmtDDLAlterTable.h"
#include "AllStmtDDLCreate.h"
#include "AllStmtDDLDrop.h"
#include "AllStmtDDLAlter.h" // MV - RG
#include "AllStmtDDLGive.h"
#include "StmtDDLAlterCatalog.h"
#include "parser/StmtDDLAlterSchema.h"
#include "StmtDDLAlterTrigger.h"
#include "StmtDDLAlterIndexAttribute.h"
#include "parser/StmtDDLAlterIndexHBaseOptions.h"
#include "StmtDDLAlterView.h"
#include "parser/StmtDDLAlterUser.h"
#include "StmtDDLAlterDatabase.h"
#include "parser/StmtDDLGrant.h"
#include "parser/StmtDDLGrantComponentPrivilege.h"
#include "parser/StmtDDLSchGrant.h"
#include "parser/StmtDDLRevoke.h"
#include "parser/StmtDDLRevokeComponentPrivilege.h"
#include "parser/StmtDDLSchRevoke.h"
#include "parser/StmtDDLRegisterComponent.h"
#include "parser/StmtDDLRegisterUser.h"
#include "parser/StmtDDLUserGroup.h"
#include "parser/StmtDDLTenant.h"
#include "parser/StmtDDLResourceGroup.h"
#include "parser/StmtDDLRegOrUnregHive.h"
#include "parser/StmtDDLCreateRole.h"
#include "parser/StmtDDLRoleGrant.h"
#include "parser/StmtDDLCleanupObjects.h"
#include "parser/StmtDDLAlterSchemaHDFSCache.h"
#include "parser/StmtDDLonHiveObjects.h"
#include "parser/StmtDDLAlterSharedCache.h"

