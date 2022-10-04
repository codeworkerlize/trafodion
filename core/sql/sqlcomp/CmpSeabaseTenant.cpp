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
 * File:         CmpSeabaseTenant.cpp
 *
 * Description:
 *   Non-inline methods for CmpSeabaseDDL class (see CmpSeabaseDDL.h)
 *   Non-inline methods for the following classes:
 *      TenantResources
 *      TenantResourceUsage
 *      TenantResourceUsageList
 *      TenantUsage
 *      TenantInfo
 *      TenantSchemaInfo
 *      TenantSchemaInfoList
 *      TenantNodeInfoList
 *
 *****************************************************************************
 */

#define SQLPARSERGLOBALS_FLAGS  // must precede all #include's
#define SQLPARSERGLOBALS_NADEFAULTS

#include "common/ComUser.h"
#include "sqlcomp/CmpSeabaseTenant.h"
#include "sqlcomp/CmpSeabaseDDL.h"
#include "common/ComSmallDefs.h"
#include "sqlcomp/PrivMgrMD.h"
#include "parser/StmtDDLTenant.h"
#include "sqlcomp/CmpSeabaseDDL.h"
#include "parser/StmtDDLResourceGroup.h"
#include "exp/ExpHbaseInterface.h"
#include "sqlcomp/CmpDDLCatErrorCodes.h"
#include "sqlcomp/PrivMgrCommands.h"
#include "sqlcomp/PrivMgrComponentPrivileges.h"
#include "seabed/ms.h"
#include <string>
// #include "NAClusterInfo.cpp"
#include "cli/Globals.h"
#include "executor/TenantHelper_JNI.h"
#include "sqlcomp/CmpSeabaseDDLauth.h"

// ----------------------------------------------------------------------------
// Implements the INITIALIZE TRAFODION, ADD TENANT USAGE command
// ----------------------------------------------------------------------------
short CmpSeabaseDDL::createSeabaseTenant(ExeCliInterface *cliInterface, NABoolean addCompPrivs,
                                         NABoolean reportNotEnabled) {
  // Check license to see if TENANT exists instead of cqd
  NABoolean enabled = msg_license_multitenancy_enabled();
  if (!enabled) {
    if (reportNotEnabled) {
      *CmpCommon::diags() << DgSqlCode(-3242) << DgString0("Multi-tenant feature is not enabled.");
      return -1;
    } else
      return 0;
  }

  if (!ComUser::isRootUserID()) {
    *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);
    return -1;
  }

  if ((CmpCommon::context()->isUninitializedSeabase()) &&
      (CmpCommon::context()->uninitializedSeabaseErrNum() == -1393)) {
    *CmpCommon::diags() << DgSqlCode(-1393);
    return -1;
  }

  // Create the SEABASE_TENANT_SCHEMA schema
  Lng32 cliRC = 0;
  char queryBuf[strlen(getSystemCatalog()) + strlen(SEABASE_TENANT_SCHEMA) + strlen(DB__ROOT) + 100];

  if (CmpCommon::context()->useReservedNamespace())
    str_sprintf(queryBuf, "create schema if not exists %s.\"%s\" authorization %s namespace '%s' ", getSystemCatalog(),
                SEABASE_TENANT_SCHEMA, DB__ROOT, ComGetReservedNamespace(SEABASE_TENANT_SCHEMA).data());
  else
    str_sprintf(queryBuf, "create schema if not exists %s.\"%s\" authorization %s ", getSystemCatalog(),
                SEABASE_TENANT_SCHEMA, DB__ROOT);

  cliRC = cliInterface->executeImmediate(queryBuf);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  Int32 numTenantTables = sizeof(allMDtenantsInfo) / sizeof(MDTableInfo);
  NAString prefixText(" IF NOT EXISTS \"");
  prefixText += NAString(SEABASE_TENANT_SCHEMA) + "\".";
  for (Int32 i = 0; i < numTenantTables; i++) {
    const MDTableInfo &mdt = allMDtenantsInfo[i];
    Int32 qryArraySize = mdt.sizeOfnewDDL / sizeof(QString);

    // Concatenate the create table text into a single string
    NAString concatenatedQuery;
    for (Int32 j = 0; j < qryArraySize; j++) {
      NAString tempStr = mdt.newDDL[j].str;
      concatenatedQuery += tempStr.strip(NAString::leading, ' ');
    }

    // qualify create table text with (optional) "IF NOT EXISTS" & schema name
    // and place in front of the table name:
    //     "create table <prefixText> tenant-table ..."
    std::string tableDDL(concatenatedQuery.data());
    size_t pos = tableDDL.find_first_of(mdt.newName);
    if (pos == string::npos) {
      NAString errorText("Unexpected error occurred while parsing create text for tenant table ");
      errorText += mdt.newName;
      SEABASEDDL_INTERNAL_ERROR(errorText.data());
      return -CAT_INTERNAL_EXCEPTION_ERROR;
    }
    tableDDL = tableDDL.insert(pos - 1, prefixText.data());

    // Create the table
    cliRC = cliInterface->executeImmediate(tableDDL.c_str());
    if (cliRC < 0) {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      return cliRC;
    }
  }

  // Add system tenants
  CmpSeabaseDDLtenant tenant;
  int32_t numberTenants = sizeof(systemTenants) / sizeof(SystemAuthsStruct);
  for (int32_t i = 0; i < numberTenants; i++) {
    const SystemAuthsStruct &tenantDefinition = systemTenants[i];

    // special Auth includes tenants that are not registered in the metadata,
    // currently there are none
    if (tenantDefinition.isSpecialAuth || tenant.authExists(tenantDefinition.authName)) continue;

    if (tenant.registerStandardTenant(tenantDefinition.authName, tenantDefinition.authID) == -1) return -1;
  }

  // Add system resource groups if they do not exist
  TenantResource resource(cliInterface, STMTHEAP);
  resource.createStandardResources();

  // create default namespace for esgyndb
  str_sprintf(queryBuf, "create trafodion namespace '%d' if not exists", SYSTEM_TENANT_ID);
  cliRC = cliInterface->executeImmediate(queryBuf);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  // added new component privilege MANAGE_TENANTS
  if (addCompPrivs && isAuthorizationEnabled()) {
    NAString MDLoc;
    CONCAT_CATSCH(MDLoc, getSystemCatalog(), SEABASE_MD_SCHEMA);
    NAString privMgrMDLoc;
    CONCAT_CATSCH(privMgrMDLoc, getSystemCatalog(), SEABASE_PRIVMGR_SCHEMA);

    PrivMgrMDAdmin admin(std::string(MDLoc.data()), std::string(privMgrMDLoc.data()), CmpCommon::diags());

    if (admin.initializeComponentPrivileges() != STATUS_GOOD) return -1;
  }

  return 0;
}

// ----------------------------------------------------------------------------
// Implements the INTIALIZE TRAFODION, DROP TENANT USAGE command
// Runs in multiple txn
//   Performs semantic checks (no txn required)
//   Drops each schema in separate transactions
//   Removes remaining tenant objects (admin roles, etc) in a txn
//   Drop each tenant namespace (no txn required)
// ----------------------------------------------------------------------------
short CmpSeabaseDDL::dropSeabaseTenant(ExeCliInterface *cliInterface) {
  if (!ComUser::isRootUserID()) {
    *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);
    return -1;
  }

  bool tenantMDexists = isTenantMetadataInitialized(cliInterface);

  Int32 cliRC = 0;
  NABoolean xnWasStartedHere = FALSE;

  // remove tenant details if tenantMD exists)
  if (tenantMDexists) {
    // Get the list of tenant schemas
    TenantSchemaInfoList *tenantSchemaList = new (STMTHEAP) TenantSchemaInfoList(STMTHEAP);
    if (getTenantSchemaList(cliInterface, tenantSchemaList) == -1) {
      NADELETE(tenantSchemaList, TenantSchemaInfoList, STMTHEAP);
      return -1;
    }

    // Drop each returned schema
    for (Int32 i = 0; i < tenantSchemaList->entries(); i++) {
      TenantSchemaInfo *tenantSchemaInfo = (*tenantSchemaList)[i];
      NAString schemaName = CmpSeabaseDDLtenant::getSchemaName(tenantSchemaInfo->getSchemaUID());

      // schema may already have been dropped, just continue
      if (schemaName == "?") continue;

      cliRC = beginXnIfNotInProgress(cliInterface, xnWasStartedHere);
      if (cliRC < 0) {
        NADELETE(tenantSchemaList, TenantSchemaInfoList, STMTHEAP);
        return -1;
      }

      NAString dropStmt("drop schema if exists ");
      dropStmt += schemaName;
      dropStmt += " cascade ";
      if (cliInterface->executeImmediate(dropStmt) < 0) {
        cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
        NADELETE(tenantSchemaList, TenantSchemaInfoList, STMTHEAP);
        endXnIfStartedHere(cliInterface, xnWasStartedHere, -1);
        return -1;
      }
      if (endXnIfStartedHere(cliInterface, xnWasStartedHere, 0) < 0) {
        NADELETE(tenantSchemaList, TenantSchemaInfoList, STMTHEAP);
        return -1;
      }
    }
    NADELETE(tenantSchemaList, TenantSchemaInfoList, STMTHEAP);
  }

  // Remove admin roles
  std::vector<Int32> adminRoles;
  char adminRoleName[MAX_DBUSERNAME_LEN + 1];
  int32_t length;
  CmpSeabaseDDLauth authInfo;
  if (authInfo.getAdminRoles(adminRoles) == CmpSeabaseDDLauth::STATUS_ERROR) return -1;

  cliRC = beginXnIfNotInProgress(cliInterface, xnWasStartedHere);
  if (cliRC < 0) return -1;

  for (Int32 i = 0; i < adminRoles.size(); i++) {
    Int32 roleID = adminRoles[i];

    // Only drop role if found in metadata
    // Ignore otherwise.
    if (ComUser::getAuthNameFromAuthID(roleID, adminRoleName, MAX_DBUSERNAME_LEN, length) == 0) {
      NAString dropRoleStmt("drop role ");
      dropRoleStmt += adminRoleName;
      cliRC = cliInterface->executeImmediate(dropRoleStmt);

      if (cliRC < 0) {
        cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
        endXnIfStartedHere(cliInterface, xnWasStartedHere, -1);
        return -1;
      }
    }
  }

  char queryBuf[strlen(getSystemCatalog()) + strlen(SEABASE_TENANT_SCHEMA) + 100];

  // drop the tenant schema
  if (tenantMDexists) {
    str_sprintf(queryBuf, "drop schema if exists %s.\"%s\" cascade ", getSystemCatalog(), SEABASE_TENANT_SCHEMA);

    if (cliInterface->executeImmediate(queryBuf) < 0) {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      endXnIfStartedHere(cliInterface, xnWasStartedHere, -1);
      return -1;
    }
  }

  // Remove tenant rows from AUTHS table
  NAString mdLocation;
  CONCAT_CATSCH(mdLocation, CmpSeabaseDDL::getSystemCatalogStatic(), SEABASE_MD_SCHEMA);
  mdLocation += NAString('.') + SEABASE_AUTHS;

  NAString deleteStmt("delete from ");
  deleteStmt += mdLocation;
  deleteStmt += " where auth_type = 'T'";

  PushAndSetSqlParserFlags savedParserFlags(INTERNAL_QUERY_FROM_EXEUTIL);

  cliRC = cliInterface->executeImmediate(deleteStmt.data());
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    endXnIfStartedHere(cliInterface, xnWasStartedHere, -1);
    return -1;
  }

  if (endXnIfStartedHere(cliInterface, xnWasStartedHere, 0) < 0) return -1;

  // Remove namespace
  NAList<NAString *> *tenantNSList = new (STMTHEAP) NAList<NAString *>(STMTHEAP);
  if (getTenantNSList(cliInterface, tenantNSList) == -1) {
    NADELETE(tenantNSList, NAList<NAString *>, STMTHEAP);
    return -1;
  }

  bool hasError = false;
  for (CollIndex i = 0; i < tenantNSList->entries(); i++) {
    NAString *tenantNS = (*tenantNSList)[i];
    if (ComTenant::isTenantNamespace(tenantNS->data())) {
      NAString dropStmt("drop trafodion namespace '");
      dropStmt += tenantNS->data();
      dropStmt += "' if exists";
      // Even if drop fails, continue to get rid of what can be dropped
      if (cliInterface->executeImmediate(dropStmt) < 0) {
        cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
        hasError = true;
      }
    }
  }

  if (hasError && CmpCommon::diags()->contains(-1064)) CmpCommon::diags()->negateAllErrors();

  return 0;
}

// -----------------------------------------------------------------------------
//  method: getTenantNSList
//
//  Get NAString list containing namespaces that are associated with tenants
//
//  return:
//     -1 - unexpected error occurred, ComDiags contains the error
//      0 - call successful (list may be empty)
//  -----------------------------------------------------------------------------
short CmpSeabaseDDL::getTenantNSList(ExeCliInterface *cliInterface, NAList<NAString *> *&tenantNSList) {
  // Caller manages memory for the list, must have allocated an empty list
  assert(tenantNSList && tenantNSList->entries() == 0);

  // Get the list of tenants and their schemas
  char getStmt[100];
  char percent('%');
  Int32 stmtSize =
      snprintf(getStmt, sizeof(getStmt), "get trafodion namespaces, match '%s1%c'", TRAF_NAMESPACE_PREFIX, percent);

  if (stmtSize >= 100) {
    SEABASEDDL_INTERNAL_ERROR("CmpSeabaseDDL::getTenantNSList failed, internal buffer size too small");
    return -1;
  }
  Queue *tableQueue = NULL;
  Int32 cliRC = cliInterface->fetchAllRows(tableQueue, getStmt, 0, false, false, true);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  // If no rows, just return
  if (cliRC == 100 || tableQueue->numEntries() == 0)  // did not find the row
    return 0;

  tableQueue->position();
  for (Int32 i = 0; i < tableQueue->numEntries(); i++) {
    OutputInfo *pCliRow = (OutputInfo *)tableQueue->getNext();

    // column 1:  namespace name
    char *ptr = NULL;
    Lng32 len = 0;
    pCliRow->get(0, ptr, len);
    NAString *nsName = new (STMTHEAP) NAString(STMTHEAP);
    nsName->append(ptr, len);

    tenantNSList->insert(nsName);
  }
  return 0;
}

// -----------------------------------------------------------------------------
// method: getTenantSchemaList
//
// Gets the list of schemas for each tenant and an indicator if the tenant is
// the default tenant.
//
// return:
//    -1 - unexpected error occurred, ComDiags contains the error
//     0 - call successful (list may be empty)
// -----------------------------------------------------------------------------
short CmpSeabaseDDL::getTenantSchemaList(ExeCliInterface *cliInterface, TenantSchemaInfoList *&tenantSchemaList) {
  // Caller manages memory for the list, must have allocated an empty list
  assert(tenantSchemaList && tenantSchemaList->entries() == 0);

  Int32 cliRC = isTenantMetadataInitialized(cliInterface);
  if (cliRC != 1) return cliRC;

  // Get the list of tenants and their schemas
  char selectStmt[500];
  Int32 stmtSize = snprintf(selectStmt, sizeof(selectStmt),
                            "select t.tenant_id, u.usage_uid, case "
                            "when default_schema_uid = u.usage_uid then 1 "
                            "else 0 end from %s.\"%s\".%s t, %s.\"%s\".%s u "
                            "where t.tenant_id = u.tenant_id and u.usage_type = 'S' "
                            "order by 1, 2, 3",
                            getSystemCatalog(), SEABASE_TENANT_SCHEMA, SEABASE_TENANTS, getSystemCatalog(),
                            SEABASE_TENANT_SCHEMA, SEABASE_TENANT_USAGE);

  if (stmtSize >= 500) {
    SEABASEDDL_INTERNAL_ERROR("CmpSeabaseDDL::getTenantSchemaList failed, internal buffer size too small");
    return -1;
  }

  Int32 diagsMark = CmpCommon::diags()->mark();
  Queue *tableQueue = NULL;
  cliRC = cliInterface->fetchAllRows(tableQueue, selectStmt, 0, false, false, true);

  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  // If no rows, just return
  if (cliRC == 100 || tableQueue->numEntries() == 0)  // did not find the row
  {
    CmpCommon::diags()->rewind(diagsMark);
    return 0;
  }

  tableQueue->position();
  for (Int32 i = 0; i < tableQueue->numEntries(); i++) {
    OutputInfo *pCliRow = (OutputInfo *)tableQueue->getNext();

    // column 1:  tenant_id; column 2:  usage_id; column 3: default_schema
    Int32 tenantID(*(Int32 *)pCliRow->get(0));
    Int64 schemaUID(*(Int64 *)pCliRow->get(1));
    Int32 defSch(*(Int32 *)pCliRow->get(2));
    TenantSchemaInfo *tenantSchemaInfo = new (STMTHEAP) TenantSchemaInfo(tenantID, schemaUID, defSch);
    tenantSchemaList->insert(tenantSchemaInfo);
  }
  return 0;
}

// -----------------------------------------------------------------------------
// Method: isTenantMetadataInitialized
//
// Checks to see if tenant tables exist
//
// Return:
//   -1 - unexpected error (ComDiags contains error)
//    0 - Metadata does not exist
//    1 - Metadata exists
// -----------------------------------------------------------------------------
short CmpSeabaseDDL::isTenantMetadataInitialized(ExeCliInterface *cliInterface) {
  Int32 cliRC;

  // If tenant ID exists in cache and it is not one of the default values,
  // assume tenants are initialized
  Int32 tenantID = ComTenant::getCurrentTenantID();
  if (tenantID != SYSTEM_TENANT_ID && tenantID != NA_UserIdDefault) return 1;

  // read metadata to see if tenant schema exists
  // returns: 1, exists. 0, does not exists. -1, error.
  return existsInSeabaseMDTable(cliInterface, getSystemCatalog(), SEABASE_TENANT_SCHEMA, SEABASE_SCHEMA_OBJECTNAME);
};

// -----------------------------------------------------------------------------
// method: isTenantSchema
//
// Returns:
//   -1 = unexpected error
//    0 = not a tenant schema
//   >0 = a tenant schema
// -----------------------------------------------------------------------------
short CmpSeabaseDDL::isTenantSchema(ExeCliInterface *cliInterface, const Int64 &schemaUID) {
  // read tenant_usage to to see if the current schemaUID exists
  char selectStmt[500];
  Int32 stmtSize = snprintf(selectStmt, sizeof(selectStmt),
                            "select count(*) from %s.\"%s\".%s u "
                            "where usage_uid = %ld and usage_type = 'S' ",
                            getSystemCatalog(), SEABASE_TENANT_SCHEMA, SEABASE_TENANT_USAGE, schemaUID);

  if (stmtSize >= 500) {
    SEABASEDDL_INTERNAL_ERROR("CmpSeabaseDDL::isTenantSchema failed, internal buffer size too small");
    return -1;
  }

  Lng32 len = 0;
  Int64 rowCount = 0;
  Lng32 cliRC = cliInterface->executeImmediate(selectStmt, (char *)&rowCount, &len, FALSE);

  // If unexpected error occurred
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  return rowCount;
}

// ----------------------------------------------------------------------------
// Implements the REGISTER TENANT command
// Register tenant runs in multiple transactions:
//   Calls registerTenantPrepare that performs semantic checks and generates a
//     tenantID (no txn required)
//   Creates a namespace for the tenant based on tenantID (no txn required)
//   Calls registerTenant that updates the metadata in a single txn
// ----------------------------------------------------------------------------
void CmpSeabaseDDL::registerSeabaseTenant(ExeCliInterface *cliInterface, StmtDDLTenant *pNode) {
  CmpSeabaseDDLtenant reg_tenant(getSystemCatalog(), getMDSchema());
  Int32 adminRoleID = 0;
  if (reg_tenant.registerTenantPrepare(pNode, adminRoleID) == -1) return;

  char intStr[20];
  NAString tenantNS = str_itoa(reg_tenant.getAuthID(), intStr);
  NAString nsStmt("CREATE TRAFODION NAMESPACE '");
  nsStmt += tenantNS + NAString("' IF NOT EXISTS");
  if (cliInterface->executeImmediate(nsStmt) < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return;
  }

  NABoolean xnWasStartedHere = FALSE;
  short abortXn = 0;
  Int32 cliRC = beginXnIfNotInProgress(cliInterface, xnWasStartedHere);
  if (cliRC < 0)
    abortXn = -1;
  else {
    cliRC = reg_tenant.registerTenant(cliInterface, pNode, adminRoleID);
    if (cliRC < 0) abortXn = -1;
  }
  endXnIfStartedHere(cliInterface, xnWasStartedHere, abortXn);
  if (abortXn) {
    nsStmt = "DROP TRAFODION NAMESPACE '";
    nsStmt += tenantNS + NAString("' IF EXISTS ");
    cliRC = cliInterface->executeImmediate(nsStmt);
  }
}

// ----------------------------------------------------------------------------
// Implements the UNREGISTER TENANT command
// Unregister tenant runs in multiple transactions
//   Calls unregisterTenantPrepare that performs semantic checks (no txn req)
//   For CASCADE - drops each schema in its own transaction.
//   Removes tenant metadata in a transaction which includes dropping roles, etc
//   For CASCADE - drops the tenant namespace (no txn required)
// ----------------------------------------------------------------------------
void CmpSeabaseDDL::unregisterSeabaseTenant(ExeCliInterface *cliInterface, StmtDDLTenant *pNode) {
  CmpSeabaseDDLtenant reg_tenant(getSystemCatalog(), getMDSchema());
  if (reg_tenant.unregisterTenantPrepare(pNode) == -1) return;

  TenantInfo tenantInfo(STMTHEAP);
  if (reg_tenant.getTenantDetails(pNode->getTenantName(), tenantInfo) == CmpSeabaseDDLauth::STATUS_ERROR) return;

  Int32 cliRC;
  NABoolean xnWasStartedHere = FALSE;

  // get list of schemas to drop
  if (pNode->dropDependencies()) {
    NAList<NAString> *schemaList = new (STMTHEAP) NAList<NAString>(STMTHEAP);
    if (tenantInfo.getUsageList()) {
      for (Int32 i = 0; i < tenantInfo.getUsageList()->entries(); i++) {
        TenantUsage usage = (*tenantInfo.getUsageList())[i];
        if (usage.isSchemaUsage()) {
          // See if schema exists, if not skip it and continue
          NAString schemaName = CmpSeabaseDDLtenant::getSchemaName(usage.getUsageID());
          if (schemaName == "?") continue;
          schemaList->insert(schemaName.data());
        }
      }
    }

    // drop (cleanup) schemas, each in its own schema
    for (Int32 i = 0; i < schemaList->entries(); i++) {
      NAString dropStmt("DROP SCHEMA ");
      dropStmt += (*schemaList)[i];
      dropStmt += " CASCADE";
      cliRC = beginXnIfNotInProgress(cliInterface, xnWasStartedHere);
      if (cliRC < 0) return;
      cliRC = cliInterface->executeImmediate(dropStmt);
      endXnIfStartedHere(cliInterface, xnWasStartedHere, cliRC);
      if (cliRC < 0) {
        cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
        return;
      }
    }
  }

  // Remove tenant details
  cliRC = beginXnIfNotInProgress(cliInterface, xnWasStartedHere);
  if (cliRC < 0) return;

  cliRC = reg_tenant.unregisterTenant(cliInterface, pNode);
  endXnIfStartedHere(cliInterface, xnWasStartedHere, cliRC);

  // If tenant not found, go ahead and call unregisterTenant to clean up
  // Zookeeper and cgroups
  bool tenantFound = true;
  if (cliRC == -1) {
    if (CmpCommon::diags()->containsError(-1086))
      tenantFound = false;
    else
      return;
  }

  // remove namespace
  if (pNode->dropDependencies()) {
    char intStr[20];
    NAString tenantNS = str_itoa(reg_tenant.getAuthID(), intStr);
    NAString nsStmt = "DROP TRAFODION NAMESPACE '";
    nsStmt += tenantNS + NAString("' IF EXISTS ");
    cliInterface->executeImmediate(nsStmt);
  }

  // remove tenant from zookeeper and delete its cgroup
  NAString nodeAllocationList = CmpCommon::getDefaultString(NODE_ALLOCATIONS).data();
  if (!CURRCONTEXT_CLUSTERINFO->hasVirtualNodes() || nodeAllocationList.length() == 0) {
    // If tenant is not defined in metadata, cleanup in zookeeper/cgroups and
    // ignore any returned errors
    if (!tenantFound || (reg_tenant.getAuthID() != SYSTEM_TENANT_ID)) {
      TH_RetCode rc = TenantHelper_JNI::getInstance()->unregisterTenant(pNode->getTenantName());
      if (rc != TH_OK && tenantFound) {
        *CmpCommon::diags() << DgSqlCode(CAT_TENANT_ALLOCATIONS) << DgString0(pNode->getTenantName().data())
                            << DgString1(GetCliGlobals()->getJniErrorStr());
      }
    }
  }
}

// ----------------------------------------------------------------------------
// Implements the INTIALIZE TRAFODION, UPGRADE TENANT USAGE command
// ----------------------------------------------------------------------------
short CmpSeabaseDDL::upgradeSeabaseTenant(ExeCliInterface *cliInterface) {
  if (!ComUser::isRootUserID()) {
    *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);
    return -1;
  }

  Lng32 cliRC = 0;

  // upgrade assumes that the basic metadata tables already exists, if not then
  // return error just check for one table for now
  cliRC = existsInSeabaseMDTable(cliInterface, getSystemCatalog(), SEABASE_TENANT_SCHEMA, SEABASE_TENANT_USAGE,
                                 COM_BASE_TABLE_OBJECT);
  if (cliRC < 0) return -1;

  if (cliRC == 0)  // does not exist
  {
    NAString tableName(getSystemCatalog());
    tableName + ".\"" + SEABASE_TENANT_SCHEMA + "\"" + SEABASE_TENANT_USAGE;
    *CmpCommon::diags() << DgSqlCode(-1389) << DgString0(tableName.data());
    return -1;
  }

  // Additional tables may have been added since tenants was installed.
  // create any new tables
  Int32 numTenantTables = sizeof(allMDtenantsInfo) / sizeof(MDTableInfo);
  NAString prefixText(" IF NOT EXISTS \"");
  prefixText += NAString(SEABASE_TENANT_SCHEMA) + "\".";
  for (Int32 i = 0; i < numTenantTables; i++) {
    const MDTableInfo &mdt = allMDtenantsInfo[i];
    Int32 qryArraySize = mdt.sizeOfnewDDL / sizeof(QString);

    // Concatenate the create table text into a single string
    NAString concatenatedQuery;
    for (Int32 j = 0; j < qryArraySize; j++) {
      NAString tempStr = mdt.newDDL[j].str;
      concatenatedQuery += tempStr.strip(NAString::leading, ' ');
    }

    // qualify create table text with (optional) "IF NOT EXISTS" & schema name
    // and place in front of the table name:
    //     "create table <prefixText> tenant-table ..."
    std::string tableDDL(concatenatedQuery.data());
    size_t pos = tableDDL.find_first_of(mdt.newName);
    if (pos == string::npos) {
      NAString errorText("Unexpected error occurred while parsing create text for tenant table ");
      errorText += mdt.newName;
      SEABASEDDL_INTERNAL_ERROR(errorText.data());
      return -CAT_INTERNAL_EXCEPTION_ERROR;
    }
    tableDDL = tableDDL.insert(pos - 1, prefixText.data());

    // Create the table
    cliRC = cliInterface->executeImmediate(tableDDL.c_str());
    if (cliRC < 0) {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      return cliRC;
    }
  }

  // Add system tenant if it does not already exist
  CmpSeabaseDDLtenant tenant;
  int32_t numberTenants = sizeof(systemTenants) / sizeof(SystemAuthsStruct);
  for (int32_t i = 0; i < numberTenants; i++) {
    const SystemAuthsStruct &authDefinition = systemTenants[i];

    // ---------------------
    // For EsgynDB 2.5, upgrade system tenants with (0,0,0) values
    // to (-1, -1, -1) for size, cluster size, affinity, indicating
    // a tenant that stretches the entire cluster.
    // This code can be removed once all system tenants are updated
    // this way.
    TenantInfo systemTenantInfo(STMTHEAP);
    Int32 retcode = TenantInfo::getTenantInfo(authDefinition.authID, systemTenantInfo);

    systemTenantInfo.setTenantName(authDefinition.authName);
    if (retcode < 0)
      return -1;
    else if (retcode == 0) {
      // check for system tenant with zero affinity
      // and update those values to (-1,-1,-1)
      if (systemTenantInfo.getAffinity() == 0) {
        systemTenantInfo.setTenantNodes(new (STMTHEAP) NAASNodes(-1, -1, -1));
        systemTenantInfo.updateTenantInfo(true, NULL);
      }
    }
    // end of 2.5 code to update system tenant size, cluster size, affinity
    // ---------------------

    // See if tenant should be registered
    if (authDefinition.isSpecialAuth || tenant.authExists(authDefinition.authName)) continue;

    if (tenant.registerStandardTenant(authDefinition.authName, authDefinition.authID) == -1) return -1;
  }

  // Add system resource groups if they do not exist
  TenantResource resource(cliInterface, STMTHEAP);
  resource.createStandardResources();

  return 0;
}

// ****************************************************************************
// Methods to create, alter, and drop resource groups
// ****************************************************************************

// -----------------------------------------------------------------------------
// method::createSeabaseRGroup
//
// Handles "CREATE RESOURCE GROUP" command
//
// ComDiags is set up if an error occurs
// -----------------------------------------------------------------------------
void CmpSeabaseDDL::createSeabaseRGroup(ExeCliInterface *cliInterface, StmtDDLResourceGroup *pNode) {
  //  Verify authority
  //  DB__ROOT or user with CREATE component privilege can perform operation
  Int32 currentUser = ComUser::getCurrentUser();
  if (CmpCommon::context()->isAuthorizationEnabled()) {
    if (currentUser != ComUser::getRootUserID()) {
      NAString systemCatalog = CmpSeabaseDDL::getSystemCatalogStatic();
      std::string privMDLoc(systemCatalog.data());

      privMDLoc += std::string(".\"") + std::string(SEABASE_PRIVMGR_SCHEMA) + std::string("\"");

      PrivMgrComponentPrivileges componentPrivileges(privMDLoc, CmpCommon::diags());
      if ((!componentPrivileges.hasSQLPriv(currentUser, SQLOperation::MANAGE_TENANTS, true)) &&
          (!componentPrivileges.hasSQLPriv(currentUser, SQLOperation::MANAGE_RESOURCE_GROUPS, true))) {
        *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);
        return;
      }
    }
  }

  // Verify that the resource group name is not reserved
  NAString groupName = pNode->getGroupName();
  if (groupName.length() >= strlen(RESERVED_NAME_PREFIX) &&
      groupName.operator()(0, strlen(RESERVED_NAME_PREFIX)) == RESERVED_NAME_PREFIX) {
    *CmpCommon::diags() << DgSqlCode(-CAT_AUTH_NAME_RESERVED) << DgString0(groupName.data());
    return;
  }

  // Verify that the name does not include unsupported special characters
  std::string validChars("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.");
  std::string strToScan(groupName.data());
  size_t found = strToScan.find_first_not_of(validChars);
  if (found != string::npos) {
    *CmpCommon::diags() << DgSqlCode(-3127) << DgString0(groupName.data());
    return;
  }

  // See if resource group already exists
  TenantResource group(cliInterface, STMTHEAP);
  NAString whereClause("WHERE resource_name = '");
  whereClause += groupName + NAString("'");
  short retcode = group.fetchMetadata(whereClause, whereClause, false);
  if (retcode == -1) return;
  if (retcode == 0) {
    *CmpCommon::diags() << DgSqlCode(-CAT_AUTHID_ALREADY_EXISTS) << DgString0("Authorization identifier")
                        << DgString1(groupName.data());
    return;
  }

  // Determine owner of resource group
  Int32 grantee = currentUser;
  if (pNode->getOwner()) {
    // authorization clause specified, verify it exists and is a user or non admin role
    NAString authName(*pNode->getOwner());

    // in case of error, diags area is populated
    retcode = ComUser::getAuthIDFromAuthName(authName.data(), grantee, CmpCommon::diags());
    if (retcode != 0) return;

    if (!CmpSeabaseDDLauth::isRoleID(grantee) && !CmpSeabaseDDLauth::isUserID(grantee)) {
      *CmpCommon::diags() << DgSqlCode(-CAT_IS_NOT_CORRECT_AUTHID) << DgString0(authName.data())
                          << DgString1("user or role");
      return;
    }

    // If authorization clause specifies an admin role return an error
    CmpSeabaseDDLauth authInfo;
    authInfo.getAuthDetails(authName.data());
    if (authInfo.isAdminAuthID(grantee)) {
      *CmpCommon::diags() << DgSqlCode(-CAT_AUTHORIZATION_CLAUSE_INCORRECT) << DgString0(groupName.data())
                          << DgString1(authName.data());
      return;
    }
  }

  // Make sure all the nodes exist in cluster
  if (pNode->getNodeList()) {
    NAString invalidNodeNames;
    if (!group.validNodeNames((ConstStringList *)pNode->getNodeList(), invalidNodeNames)) {
      *CmpCommon::diags() << DgSqlCode(-CAT_NODE_NAME_INVALID) << DgString0(invalidNodeNames.data());
      return;
    }
  }

  // generate the RESOURCES row
  Int64 createTime = NA_JulianTimestamp();
  ComUID groupUID;
  groupUID.make_UID();

  group.setCreateTime(createTime);
  group.setCreator(grantee);
  group.setFlags(0);
  group.setIsValid(true);
  group.setRedefTime(createTime);
  group.setResourceName(groupName);
  group.setResourceUID(groupUID.get_value());
  group.setType(TenantResource::RESOURCE_TYPE_GROUP);

  // Generate the resource usages
  TenantResourceUsageList usageList(group.getCli(), (NAHeap *)group.getHeap());
  if (pNode->getNodeList()) {
    for (CollIndex i = 0; i < pNode->getNodeList()->entries(); i++) {
      const NAString *nodeName = (*pNode->getNodeList())[i];

      // generate node resource, if not already exists
      TenantResource node(cliInterface, STMTHEAP);
      node.generateNodeResource((NAString *)nodeName, grantee);

      // add node to usage list
      TenantResourceUsage *usage = new ((NAMemory *)group.getHeap())
          TenantResourceUsage(group.getResourceUID(), group.getResourceName(), node.getResourceUID(), *nodeName,
                              TenantResourceUsage::RESOURCE_USAGE_NODE);
      usageList.insert(usage);
    }

    // insert node usages
    if (usageList.insertUsages() == -1) return;
  }

  // Insert resource group
  if (group.insertRow() == -1) return;
}

// -----------------------------------------------------------------------------
// method::alterSeabaseRGroup
//
// Handles "ALTER RESOURCE GROUP" command
//
// ComDiags is set up if an error occurs
// -----------------------------------------------------------------------------
void CmpSeabaseDDL::alterSeabaseRGroup(ExeCliInterface *cliInterface, StmtDDLResourceGroup *pNode) {
  // See if resource exists
  NAString groupName = pNode->getGroupName();
  TenantResource group(cliInterface, STMTHEAP);
  NAString whereClause("WHERE resource_name = '");
  whereClause += groupName + NAString("'");
  short retcode = group.fetchMetadata(whereClause, whereClause, true);
  if (retcode == -1) return;
  if (retcode == 1) {
    *CmpCommon::diags() << DgSqlCode(-CAT_RESOURCE_GROUP_NOT_EXIST) << DgString0(groupName.data());
    return;
  }

  // Verify authority
  // DB__ROOT, group owner, or user with MANAGE_TENANT or MANAGE_RESOURCE_GROUPS
  // component privileges can perform operation
  Int32 currentUser = ComUser::getCurrentUser();
  if (CmpCommon::context()->isAuthorizationEnabled()) {
    if (currentUser != ComUser::getRootUserID() && currentUser != group.getCreator()) {
      PrivMgrComponentPrivileges componentPrivileges;
      if ((!componentPrivileges.hasSQLPriv(currentUser, SQLOperation::MANAGE_TENANTS, true)) &&
          (!componentPrivileges.hasSQLPriv(currentUser, SQLOperation::MANAGE_RESOURCE_GROUPS, true))) {
        *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);
        return;
      }
    }
  }

  // Perform the alter operation
  switch (pNode->getAlterType()) {
    case StmtDDLResourceGroup::ALTER_ADD_NODE: {
      // Verify that the list of nodes are valid - nodeList is required
      assert(pNode->getNodeList());

      NAString invalidNodeName;
      if (!group.validNodeNames((ConstStringList *)pNode->getNodeList(), invalidNodeName)) {
        *CmpCommon::diags() << DgSqlCode(-CAT_NODE_NAME_INVALID) << DgString0(invalidNodeName.data());
        return;
      }

      // generate additional resource usages
      TenantResourceUsageList *existingUsageList = group.getUsageList();
      TenantResourceUsageList addUsageList(group.getCli(), (NAHeap *)group.getHeap());
      Int32 numUsages = 0;

      for (CollIndex i = 0; i < pNode->getNodeList()->entries(); i++) {
        // If node already assigned, ignore and continue
        // Could return an error?
        const NAString *nodeName = (*pNode->getNodeList())[i];
        TenantResourceUsage *existingUsage =
            existingUsageList->findUsage(nodeName, TenantResourceUsage::RESOURCE_USAGE_NODE);
        if (existingUsage) continue;

        // generate node resource
        TenantResource node(cliInterface, STMTHEAP);
        if (node.generateNodeResource((NAString *)nodeName, group.getCreator()) == -1) return;

        TenantResourceUsage *usage = new ((NAMemory *)group.getHeap())
            TenantResourceUsage(group.getResourceUID(), group.getResourceName(), node.getResourceUID(), *nodeName,
                                TenantResourceUsage::RESOURCE_USAGE_NODE);
        addUsageList.insert(usage);
        numUsages++;
      }

      // insert new usages
      if (numUsages > 0) {
        if (addUsageList.insertUsages() == -1) return;
      }

      // update redefined time of group, not return even error happens
      group.updateRedefTime();

      // If tenant associated with resource group, set up a warning
      NAString tenantNames;
      if (group.getTenantsForResource(tenantNames) == -1) return;
      if (tenantNames.length() > 0) {
        tenantNames.prepend("(");
        tenantNames.append(")");
        *CmpCommon::diags() << DgSqlCode(CAT_BALANCE_TENANTS) << DgString0(groupName.data())
                            << DgString1(tenantNames.data());
      }
      break;
    }
    case StmtDDLResourceGroup::ALTER_DROP_NODE: {
      // Verify that the list of nodes are valid - nodeList is required
      assert(pNode->getNodeList());

      char buf[100];
      Int32 stmtSize = 0;
      Int32 numUsages = 0;
      TenantResourceUsageList *existingUsageList = group.getUsageList();

      // Clause defining rgroup - node usage to be deleted from resource usage
      NAString rgroupNodeClause("WHERE ");

      // Clause containing node list, used to determine if node resource is obsolete
      NAString nodeObsoleteClause("('");

      // List of deleted node names
      NAList<NAString> deletedNodes(STMTHEAP, pNode->getNodeList()->entries());

      for (CollIndex i = 0; i < pNode->getNodeList()->entries(); i++) {
        // If node not assigned, return an error
        const NAString *nodeName = (*pNode->getNodeList())[i];
        TenantResourceUsage *usage = existingUsageList->findUsage(nodeName, TenantResourceUsage::RESOURCE_USAGE_NODE);
        if (usage == NULL) {
          NAString node("Node ");
          node += nodeName->data();
          NAString rf("valid name for resource group ");
          rf += groupName;
          *CmpCommon::diags() << DgSqlCode(-CAT_IS_NOT_CORRECT_AUTHID) << DgString0(node.data())
                              << DgString1(rf.data());
          return;
        }

        if (numUsages > 0) {
          nodeObsoleteClause += "'), ('";
          rgroupNodeClause += " OR ";
        }

        stmtSize = snprintf(buf, sizeof(buf), "(resource_uid = %ld and usage_uid = %ld)", usage->getResourceUID(),
                            usage->getUsageUID());
        assert(stmtSize < 100);

        rgroupNodeClause += buf;
        nodeObsoleteClause += *nodeName;

        deletedNodes.insert(*nodeName);

        numUsages++;
      }

      // cannot drop the last node from resource group
      if (numUsages >= existingUsageList->getNumNodeUsages()) {
        *CmpCommon::diags() << DgSqlCode(-CAT_LAST_NODE) << DgString0(groupName.data());
        return;
      }

      if (numUsages > 0) {
        nodeObsoleteClause += "')";
      }

      // If node is being used by a tenant, rebalance tenant with new list
      NAList<TenantInfo *> tenantInfoList(STMTHEAP);
      NAString tenantNames;
      if (group.getTenantsForNodes(deletedNodes, tenantNames) == -1) return;

      if (tenantNames.length() > 0) {
        if (group.getTenantInfoList(tenantNames, tenantInfoList) == -1) return;

        if (group.alterNodesForRGroupTenants(cliInterface, &tenantInfoList, deletedNodes) == -1) {
          // Need to recover any successful assignments
          if (group.backoutAssignedNodes(cliInterface, tenantInfoList) == -1) {
            // Unable to backout changes, send a warning
            tenantNames.prepend("(");
            tenantNames.append(")");
            *CmpCommon::diags() << DgSqlCode(CAT_BALANCE_TENANTS) << DgString0(groupName.data())
                                << DgString1(tenantNames.data());
          }
          return;
        }
      }

      // delete rgroup - node usages from resource usages
      Int64 rowsDeleted = 0;
      if (numUsages > 0) rowsDeleted = existingUsageList->deleteUsages(rgroupNodeClause);

      if (rowsDeleted < 0) return;

      if (rowsDeleted != numUsages) {
        NAString msg("CmpSeabaseDDL::alterSeabaseRGroup, delete usages inconsistent: ");
        msg += "num usages to delete: ";
        msg += (to_string((long long int)numUsages).c_str());
        msg += " num usages found: ";
        msg += (to_string((long long int)rowsDeleted).c_str());
        SEABASEDDL_INTERNAL_ERROR(msg.data());
        return;
      }

      // Remove "node" resource from resources if "node" is no longer
      // referenced by any other resource group
      Int32 bufSize = 500 + nodeObsoleteClause.length();
      char deleteStmt[bufSize];

      NAString sysCat = CmpSeabaseDDL::getSystemCatalogStatic();
      stmtSize = snprintf(deleteStmt, sizeof(deleteStmt),
                          "delete from %s.\"%s\".%s where resource_name in "
                          "(select uname from (values %s) u(uname) where "
                          "u.uname not in (select usage_name from %s.\"%s\".%s "
                          "where  resource_uid <> %ld)); ",
                          sysCat.data(), SEABASE_TENANT_SCHEMA, SEABASE_RESOURCES, nodeObsoleteClause.data(),
                          sysCat.data(), SEABASE_TENANT_SCHEMA, SEABASE_RESOURCE_USAGE, group.getResourceUID());
      assert(stmtSize < bufSize);

      Int32 cliRC = cliInterface->executeImmediate(deleteStmt);
      if (cliRC < 0) {
        cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
        return;
      }

      // update redefined time of group, not return even error happens
      group.updateRedefTime();

      break;
    }

    case StmtDDLResourceGroup::ALTER_OFFLINE: {
      if (!group.isValid()) return;
      NAString setClause("SET resource_is_valid = 'N'");
      NAString whereClause(" WHERE resource_uid = ");
      whereClause += (to_string((long long int)group.getResourceUID())).c_str();
      if (group.updateRows(setClause.data(), whereClause.data()) == -1) return;
      break;
    }

    case StmtDDLResourceGroup::ALTER_ONLINE: {
      if (group.isValid()) return;
      NAString setClause("SET resource_is_valid = 'Y'");
      NAString whereClause(" WHERE resource_uid = ");
      whereClause += (to_string((long long int)group.getResourceUID())).c_str();
      if (group.updateRows(setClause.data(), whereClause.data()) == -1) return;
      break;
    }

    case StmtDDLResourceGroup::NOT_ALTER:
    default:
      SEABASEDDL_INTERNAL_ERROR("CmpSeabaseDDL::alterSeabaseRGroup failed, invalid alter type");
      return;
  }
}

// -----------------------------------------------------------------------------
// method: dropSeabaseRGroup
//
// Handles "DROP RESOURCE GROUP" command
//
// ComDiags is set up if an error occurs
// -----------------------------------------------------------------------------
void CmpSeabaseDDL::dropSeabaseRGroup(ExeCliInterface *cliInterface, const StmtDDLResourceGroup *pNode) {
  // See if resource already exists
  TenantResource group(cliInterface, STMTHEAP);
  NAString whereClause("WHERE resource_name = '");
  whereClause += pNode->getGroupName() + NAString("'");
  short retcode = group.fetchMetadata(whereClause, whereClause, true);
  if (retcode == -1) return;
  if (retcode == 1) {
    *CmpCommon::diags() << DgSqlCode(-CAT_RESOURCE_GROUP_NOT_EXIST) << DgString0(pNode->getGroupName().data());
    return;
  }

  // Verify authority
  // DB__ROOT, group owner, or user with MANAGE_TENANT or MANAGE_RESOURCE_GROUPS
  // component privileges can perform operation
  Int32 currentUser = ComUser::getCurrentUser();
  if (CmpCommon::context()->isAuthorizationEnabled()) {
    if (currentUser != ComUser::getRootUserID() && currentUser != group.getCreator()) {
      PrivMgrComponentPrivileges componentPrivileges;
      if ((!componentPrivileges.hasSQLPriv(currentUser, SQLOperation::MANAGE_TENANTS, true)) &&
          (!componentPrivileges.hasSQLPriv(currentUser, SQLOperation::MANAGE_RESOURCE_GROUPS, true))) {
        *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);
        return;
      }
    }
  }

  // If any other tenant using resource group, return an error
  NAString tenantNames;
  if (group.getTenantsForResource(tenantNames) == -1) return;
  if (tenantNames.length() > 0) {
    tenantNames.prepend("(");
    tenantNames.append(")");
    *CmpCommon::diags() << DgSqlCode(-CAT_DEPENDENT_OBJECTS_EXIST) << DgString0(tenantNames.data());
    return;
  }

  // TBD - change unregister user/drop role to check for resource groups owners

  whereClause = "where resource_uid = ";
  whereClause += (to_string((long long int)group.getResourceUID())).c_str();
  if (group.getUsageList()->deleteUsages(whereClause) == -1) return;

  if (group.removeObsoleteNodes(group.getUsageList()) == -1) return;

  if (group.deleteRow() == -1) return;
}

// -----------------------------------------------------------------------------
// method:  authIDOwnsResources
//
// input:  authID to check
//
// output:  first five resources
//
// returns:
//    -1 - unexpected error
//   >=0 - number of resources authID owns
// -----------------------------------------------------------------------------
Int64 CmpSeabaseDDL::authIDOwnsResources(Int32 authID, NAString &resourceNames) {
  ExeCliInterface cliInterface(STMTHEAP);
  NAString sysCat = CmpSeabaseDDL::getSystemCatalogStatic();

  // Does user own any resources
  Int32 bufSize = 500;
  char buf[bufSize];
  snprintf(buf, bufSize,
           "SELECT [FIRST 5] resource_name FROM %s.\"%s\".%s"
           " WHERE RESOURCE_CREATOR = %d ",
           sysCat.data(), SEABASE_TENANT_SCHEMA, SEABASE_RESOURCES, authID);

  Int32 len = 0;

  Queue *tableQueue = NULL;
  Int32 cliRC = cliInterface.fetchAllRows(tableQueue, buf, 0, false, false, true);

  tableQueue->position();
  Int64 rowCount = 0;
  for (Int32 i = 0; i < tableQueue->numEntries(); i++) {
    OutputInfo *pCliRow = (OutputInfo *)tableQueue->getNext();

    // column 0:  resource_name
    if (i > 0) resourceNames += ", ";
    resourceNames += (char *)pCliRow->get(0);
  }

  return tableQueue->numEntries();
}

// ****************************************************************************
// Non-inline methods for class:  TenantUsage
//
// This class defines a tenant usage - that is a relationship between a tenant
// and another object such as a schema or role.
// ****************************************************************************

// ----------------------------------------------------------------------------
// Method:  getUsageTypeAsEnum
//
// This static method returns the usage type as a TenantUsageType enum
// returns TENANT_USAGE_UNKNOWN if unable to convert string
// ----------------------------------------------------------------------------
TenantUsage::TenantUsageType TenantUsage::getUsageTypeAsEnum(NAString usageString) {
  if (usageString == "S ") return TenantUsage::TENANT_USAGE_SCHEMA;
  if (usageString == "U ") return TenantUsage::TENANT_USAGE_USER;
  if (usageString == "R ") return TenantUsage::TENANT_USAGE_ROLE;
  if (usageString == "G ") return TenantUsage::TENANT_USAGE_GROUP;
  if (usageString == "RG") return TenantUsage::TENANT_USAGE_RESOURCE;
  return TenantUsage::TENANT_USAGE_UNKNOWN;
}

// ----------------------------------------------------------------------------
// Method:  getUsageTypeAsString
//
// This static method returns the usage type as a 2 character string.
// returns an empty string if unable to convert value
// ----------------------------------------------------------------------------
NAString TenantUsage::getUsageTypeAsString(TenantUsageType usageType) {
  NAString usageString;
  switch (usageType) {
    case TENANT_USAGE_SCHEMA:
      usageString = "S ";
      break;
    case TENANT_USAGE_USER:
      usageString = "U ";
      break;
    case TENANT_USAGE_ROLE:
      usageString = "R ";
      break;
    case TENANT_USAGE_GROUP:
      usageString = "G ";
      break;
    case TENANT_USAGE_RESOURCE:
      usageString = "RG";
      break;
    case TENANT_USAGE_UNKNOWN:
    default:
      usageString = "  ";
      break;
  }
  return usageString;
}

// ****************************************************************************
// Non-inline methods for class:  TenantUsageList
//
// ****************************************************************************

//-----------------------------------------------------------------------------
// Method: deleteUsages
//
// Deletes rows based on the passed in whereClause
// ----------------------------------------------------------------------------
Int32 TenantUsageList::deleteUsages(const char *whereClause) {
  NAString mdLocation;
  CONCAT_CATSCH(mdLocation, CmpSeabaseDDL::getSystemCatalogStatic(), SEABASE_TENANT_SCHEMA);
  mdLocation += NAString('.') + SEABASE_TENANT_USAGE;

  int deleteStmtLen = strlen(whereClause) + 200;
  char deleteStmt[deleteStmtLen];
  Int32 stmtSize = snprintf(deleteStmt, sizeof(deleteStmt), "delete from %s %s", mdLocation.data(), whereClause);
  if (stmtSize >= deleteStmtLen) {
    SEABASEDDL_INTERNAL_ERROR("TenantUsageList::deleteUsages failed, internal buffer size too small");
    return -1;
  }

  ExeCliInterface cliInterface(heap_, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());
  Int32 cliRC = cliInterface.executeImmediate(deleteStmt);
  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }
  return 0;
}

// ----------------------------------------------------------------------------
// method: findTenantUsage
//
// returns the TenantUsage for the passed in tenant and usage ID
//
//  status:
//    TRUE - found usage and contains in tenantUsage
//    FALSE - did not find usage
// ----------------------------------------------------------------------------
NABoolean TenantUsageList::findTenantUsage(const Int64 tenantUsageID, TenantUsage &tenantUsage) const {
  for (Int32 i = 0; i < entries(); i++) {
    TenantUsage usage = operator[](i);
    if (usage.getUsageID() == tenantUsageID) {
      tenantUsage = usage;
      return TRUE;
    }
  }
  return FALSE;
}

// ----------------------------------------------------------------------------
// method: possibleDefSchemaUID
//
// Determines, based on the schema usages found in the TenantUsageList, if
// there is only one schema.  If so, returns the schemaUID to be considered
// for the default schema during CREATE or ALTER tenant. if not, returns 0.
//
// No errors are generated
// ----------------------------------------------------------------------------
Int64 TenantUsageList::possibleDefSchUID() const {
  // If no usages, then no schemas to make default
  if (entries() == 0) return 0;

  Int64 schemaUID = 0;
  for (Int32 i = 0; i < entries(); i++) {
    TenantUsage usage = operator[](i);
    if (usage.getUsageType() == TenantUsage::TENANT_USAGE_SCHEMA) {
      // If schemaUID already found, then can't determine default
      if (schemaUID != 0) return 0;
      schemaUID = usage.getUsageID();
    }
  }

  // Only on schema usage exists, return its schemaUID
  return schemaUID;
}

// -----------------------------------------------------------------------------
// method:  getNewSchemaUsages()
//
// Returns the number of schema usages that have have changed.
// -----------------------------------------------------------------------------
Int32 TenantUsageList::getNewSchemaUsages() {
  Int32 numUsages = 0;
  for (Int32 i = 0; i < entries(); i++) {
    TenantUsage usage = operator[](i);
    if (usage.getUsageType() == TenantUsage::TENANT_USAGE_SCHEMA) {
      if (usage.isUnchanged()) continue;
      numUsages++;
    }
  }
  return numUsages;
}

// ----------------------------------------------------------------------------
// method:  getNumberUsages
//
// skipObsolete - if true, does not count usages marked obosolete
//
// returns the number of usages for a tenant based on usageType
// ----------------------------------------------------------------------------
Int32 TenantUsageList::getNumberUsages(const TenantUsage::TenantUsageType &usageType, bool skipObsolete) const {
  Int32 numUsages = 0;
  for (Int32 i = 0; i < entries(); i++) {
    TenantUsage usage = operator[](i);
    if (usage.getUsageType() == usageType) {
      if (skipObsolete && usage.isObsolete()) continue;
      numUsages++;
    }
  }
  return numUsages;
}

//-----------------------------------------------------------------------------
// Method: selectUsages
//
// Reads the TENANT_USAGE tables to gather usages for a tenant
// Returns:
//   list of tenant usages
//   status
//     -1 = failed (diags area contains error details)
//      0 = succeeded
//
// This method allocates space for the usages list.  The caller is responsible
// for deleting the memory.
// ----------------------------------------------------------------------------
Int32 TenantUsageList::selectUsages(const Int32 tenantID, NAHeap *heap) {
  NAString mdLocation;
  CONCAT_CATSCH(mdLocation, CmpSeabaseDDL::getSystemCatalogStatic(), SEABASE_TENANT_SCHEMA);
  mdLocation += NAString('.') + SEABASE_TENANT_USAGE;

  // Find usages for tenant identified by tenantID
  char selectStmt[500];
  Int32 stmtSize = snprintf(selectStmt, sizeof(selectStmt),
                            "select usage_uid, usage_type, flags "
                            "from %s where tenant_id = %d",
                            mdLocation.data(), tenantID);
  if (stmtSize >= 500) {
    SEABASEDDL_INTERNAL_ERROR("TenantInfo::selectUsages failed, internal buffer size too small");
    return -1;
  }

  Int32 diagsMark = CmpCommon::diags()->mark();
  ExeCliInterface cliInterface(heap, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());
  Queue *tableQueue = NULL;
  Int32 cliRC = cliInterface.fetchAllRows(tableQueue, selectStmt, 0, false, false, true);

  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  // If no rows, just return
  if (cliRC == 100)  // did not find the row
  {
    CmpCommon::diags()->rewind(diagsMark);
    return 0;
  }

  if (tableQueue->numEntries() == 0) {
    return 0;
  }

  tableQueue->position();
  for (Int32 i = 0; i < tableQueue->numEntries(); i++) {
    OutputInfo *pCliRow = (OutputInfo *)tableQueue->getNext();
    TenantUsage tenantUsage;

    // column 1:  usage_uid; column 2:  usage_type; column 3: flags
    tenantUsage.setUsageID(*(Int64 *)pCliRow->get(0));
    NAString usageType = (char *)pCliRow->get(1);
    tenantUsage.setUsageType(TenantUsage::getUsageTypeAsEnum(usageType));
    tenantUsage.setFlags(*(Int64 *)pCliRow->get(2));

    tenantUsage.setTenantID(tenantID);

    // add to list
    insert(tenantUsage);
  }
  return 0;
}

// -----------------------------------------------------------------------------
// method: obsoleteTenantRGroupUsages
//
// Set all resource group usages to obsolete.
// -----------------------------------------------------------------------------
void TenantUsageList::obsoleteTenantRGroupUsages() {
  for (Int32 i = 0; i < entries(); i++) {
    if (operator[](i).getUsageType() == TenantUsage::TENANT_USAGE_RESOURCE) operator[](i).setIsObsolete(true);
  }
}

// -----------------------------------------------------------------------------
// method: setTenantUsageObsolete
//
// Set the specified tenant usage obsolete.
// -----------------------------------------------------------------------------
void TenantUsageList::setTenantUsageObsolete(const Int64 tenantUsageID, const bool obsolete) {
  for (Int32 i = 0; i < entries(); i++) {
    if (operator[](i).getUsageID() == tenantUsageID) {
      operator[](i).setIsObsolete(obsolete);
      return;
    }
  }
}

// ----------------------------------------------------------------------------
// Method:  insertUsages
//
// Inserts a set of rows into the TENANT_USAGE table
//
// Returns:
//    0 - successful
//   -1 - fails
// ----------------------------------------------------------------------------
Int32 TenantUsageList::insertUsages() {
  Int32 stmtSize = (entries() * 30) + 500;
  char stmt[stmtSize];
  Int32 maxSize = 0;

  // Create the values clause
  std::string valuesClause;
  bool addComma = false;
  for (Int32 i = 0; i < entries(); i++) {
    TenantUsage usage = operator[](i);
    if (usage.isUnchanged()) continue;

    if (addComma)
      valuesClause += ", ";
    else
      addComma = true;

    NAString usageType = usage.getUsageTypeAsString(usage.getUsageType());
    maxSize = snprintf(stmt, sizeof(stmt), "(%ld, %d, '%s', %ld)", usage.getUsageID(), usage.getTenantID(),
                       usageType.data(), usage.getFlags());
    if (maxSize >= stmtSize) {
      SEABASEDDL_INTERNAL_ERROR("TenantUsageList::insertUsages failed, internal buffer size too small");
      return -1;
    }
    valuesClause += stmt;
  }

  NAString mdLocation;
  CONCAT_CATSCH(mdLocation, CmpSeabaseDDL::getSystemCatalogStatic(), SEABASE_TENANT_SCHEMA);
  mdLocation += NAString(".") + SEABASE_TENANT_USAGE;

  maxSize = snprintf(stmt, sizeof(stmt), "insert into %s values %s", mdLocation.data(), valuesClause.c_str());

  if (maxSize >= stmtSize) {
    SEABASEDDL_INTERNAL_ERROR("TenantUsageList::insertUsages failed, internal buffer size too small");
    return -1;
  }

  ExeCliInterface cliInterface(heap_, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());
  Int32 cliRC = cliInterface.executeImmediate(stmt);

  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  return 0;
}

// ****************************************************************************
// Non-inline methods for class:  TenantInfo
//
// ****************************************************************************

TenantInfo::TenantInfo(const Int32 tenantID, const Int32 adminRoleID, const Int64 defaultSchemaUID,
                       TenantUsageList *usageList, NAWNodeSet *tenantNodeSet, NAString tenantName, NAHeap *heap)
    : tenantID_(tenantID),
      adminRoleID_(adminRoleID),
      defaultSchemaUID_(defaultSchemaUID),
      usageList_(usageList),
      assignedNodes_(tenantNodeSet),
      origAssignedNodes_(NULL),
      tenantDetails_(NULL),
      tenantName_(tenantName),
      flags_(0),
      heap_(heap) {}

// -----------------------------------------------------------------------------
// method: getAffinity
//
// If AS sizing, return value stored in the NAASNode class
// otherwise return -1, RG tenants do not have the concept of affinity.
// -----------------------------------------------------------------------------
const Int32 TenantInfo::getAffinity() {
  if (assignedNodes_ && assignedNodes_->castToNAASNodes()) return assignedNodes_->castToNAASNodes()->getAffinity();

  return -1;
}

// -----------------------------------------------------------------------------
// method: getClusterSize()
// If AS sizing, return value stored in the NAASNode class
// otherwise return -1, RG tenants do not have the concept of affinity
// -----------------------------------------------------------------------------
const Int32 TenantInfo::getClusterSize() {
  if (assignedNodes_ && assignedNodes_->castToNAASNodes()) return assignedNodes_->castToNAASNodes()->getClusterSize();

  return -1;
}

// -----------------------------------------------------------------------------
// method: getTenantSize()
//
// Returns the number of units allocted to the tenant.  Since both AS and RG
// tenants keep track of the total number of units, tenant size is stored in the
// parent NAWNodeSet class.
//
// If assignedNodes has been allocated, return value stored in NAWNodeSet
// otherwise return -1, both AS and RG store
// -----------------------------------------------------------------------------
const Int32 TenantInfo::getTenantSize() {
  if (assignedNodes_ == NULL) return -1;

  return assignedNodes_->getTenantSize();
}

// ----------------------------------------------------------------------------
// Method: validateTenantSize
//
// Returns:
//   true  = info is valid
//   false = info is invalid (diags area is set up with error)
// ----------------------------------------------------------------------------
bool TenantInfo::validateTenantSize() {
  if (!assignedNodes_) {
    *CmpCommon::diags() << DgSqlCode(-7033) << DgInt0(-1) << DgString0("No nodes have been assigned to this tenant");
    return false;
  }

  const char *errMsg = assignedNodes_->isInvalid(CURRCONTEXT_CLUSTERINFO);

  if (errMsg) {
    *CmpCommon::diags() << DgSqlCode(-7033) << DgInt0(assignedNodes_->getTenantSize()) << DgString0(errMsg);
    return false;
  }
  return true;
}

// ----------------------------------------------------------------------------
// method: balanceTenantNodeAlloc
//
// Based on the requested units and what is available, regenerate (or generate
// for the first time) node-tenant allocations.
//
// Two types of tenant allocations:
//   resource groups (RG) - base unit allocations on resource groups assigned
//                          to the tenant
//   affinity (AS) - base unit allocations on the requested affinity,
//                   clusterSize, and units requested.
//
//   in    : defaultSchema - required for both AS and RG tenants
//         : sessionLimit - any session limit change
//         : isBackout - backing out a previous balance operation
//         : nodeList - list of nodes available for the tenant
//         : origTenantNodeSet - original set of nodes and allocations
//   inout : tenantRUsageList - updated node-tenant allocations.
//         : tenantNodeSet - NAWNodeSet (parent class)
//              NAASNodes (child) for AS tenants
//              NAWSetOfNodeIds (child) for RG tenants
//
// throws an exception if any errors occur
// ----------------------------------------------------------------------------
void TenantInfo::balanceTenantNodeAlloc(const NAString &tenantDefaultSchema, const Int32 sessionLimit, bool isBackout,
                                        NAWNodeSet *origTenantNodeSet, NAWNodeSet *&tenantNodeSet,
                                        TenantResourceUsageList *&tenantRUsageList, bool &registrationComplete) {
  // session limit
  //   -2  - tenant request did not specify a limit, use existing limit
  //   -1  - no limit
  //    0+ - the number of sessions allowed
  char buf[100];
  snprintf(buf, sizeof(buf), "%d", sessionLimit);

  OversubscriptionInfo osi;
  NAClusterInfo *nacl = CURRCONTEXT_CLUSTERINFO;
  NAString nodeName;

  // For NAWSetOfNodeIds, calculate the node-tenant unit allocations.
  // For NAASNodes, calculate the affinity.
  // Register the tenant in zookeeper (trafodion/wms/tenants) and create cgroups
  TH_RetCode rc = TH_OK;

  if (origTenantNodeSet) {
    rc = TenantHelper_JNI::getInstance()->alterTenant(getTenantName(), *origTenantNodeSet, *tenantNodeSet,
                                                      NAString(buf), tenantDefaultSchema, true,
                                                      tenantNodeSet,  // out, updated tenant nodes
                                                      osi,            // out, info on oversubscription
                                                      STMTHEAP);
  } else {
    rc = TenantHelper_JNI::getInstance()->registerTenant(getTenantName(), *tenantNodeSet, NAString(buf),
                                                         tenantDefaultSchema, true,
                                                         tenantNodeSet,  // out, updated tenant nodes
                                                         osi,            // out, info on oversubscription
                                                         STMTHEAP);
  }

  if (rc != TH_OK) {
    *CmpCommon::diags() << DgSqlCode(-CAT_TENANT_ALLOCATIONS) << DgString0(getTenantName())
                        << DgString1(GetCliGlobals()->getJniErrorStr());
    UserException excp(NULL, 0);
    throw excp;
  }

  registrationComplete = true;

  // If a backout operation, just return, don't return over subscription
  // errors and warnings,  or update metadata node-tenant unit allocations.
  if (isBackout) return;

  // add an oversubscription warning or error, if needed
  if (osi.addDiagnostics(CmpCommon::diags(), getTenantName(),
                         CmpCommon::getDefaultNumeric(TENANT_OVERSUBSCRIBE_WARN_NODE_RATIO),
                         CmpCommon::getDefaultNumeric(TENANT_OVERSUBSCRIBE_ERR_NODE_RATIO))) {
    UserException excp(NULL, 0);
    throw excp;
  }

  // set the current node-tenant unit allocations usages in RESOURCE_USAGE
  //   (resource is the node, usage is the tenant, usageValue is the units)
  if (tenantNodeSet->castToNAWSetOfNodeIds()) {
    for (int u = 0; u < tenantNodeSet->getNumNodes(); u++) {
      int nodeId = tenantNodeSet->getDistinctNodeId(u);
      int numUnits = tenantNodeSet->getDistinctWeight(u);

      if (tenantNodeSet->usesDenseNodeIds()) nodeId = nacl->mapLogicalToPhysicalNodeId(nodeId);

      if (!nacl->mapNodeIdToNodeName(nodeId, nodeName)) {
        SEABASEDDL_INTERNAL_ERROR("TenantInfo::balanceTenantNodeAlloc failed, could not map node ID to node name");
        UserException excp(NULL, 0);
        throw excp;
      }

      else {
        tenantRUsageList->updateUsageValue(nodeName, getTenantName(), TenantResourceUsage::RESOURCE_USAGE_TENANT,
                                           numUnits);
      }
    }
  }
}

// ----------------------------------------------------------------------------
// method:  allocTenantNode
//
// Allocates a tenantNodeSet(NAWNodeSet)
//
//    NAWNodeSet -> parent class
//      NAASNodes -> child class used for adaptive segmantation(AS) tenants
//      NAWSetOfNodeIds -> child class used for resource groups (RG) tenants
//
// Input
//    AStenant - if TRUE, AStenant else RG tenant
//    affinity & clusterSize - parameters for AStenant
//    units - number units to allocate, for AS and RG tenants
//    nodeList - for RGtenant, list of all nodes available to tenant
//    skipInvalidWeights - for RG tenants, don't include nodes that have
//                         no units allocated in the tenantNodeSet
//
// Output
//    tenantNodeSet - NAWNodeSet
//
// Throws an exception if any error occur
//-----------------------------------------------------------------------------
void TenantInfo::allocTenantNode(const NABoolean AStenant, const Int32 affinity, const Int32 clusterSize,
                                 const Int32 units, TenantNodeInfoList *nodeList, NAWNodeSet *&tenantNodeSet,
                                 bool skipInvalidWeights) {
  if (getTenantID() == SYSTEM_TENANT_ID)
    tenantNodeSet = new (STMTHEAP) NAASNodes(-1, -1, -1);
  else {
    if (AStenant) {
      tenantNodeSet = new (STMTHEAP) NAASNodes(units, clusterSize, affinity);
    } else {
      NAWSetOfNodeIds *tn = new (STMTHEAP) NAWSetOfNodeIds(STMTHEAP, units);

      // add list of nodes for tenant unit allocation
      for (CollIndex i = 0; i < nodeList->entries(); i++) {
        TenantNodeInfo nodeInfo = (*nodeList)[i];
        Int32 nodeWeight = (nodeInfo.getNodeWeight() > 0) ? nodeInfo.getNodeWeight() : -1;
        if (skipInvalidWeights && nodeWeight == -1) continue;
        tn->addNode(nodeInfo.getLogicalNodeID(), nodeWeight);
      }
      tenantNodeSet = tn;
    }
  }

  // do an initial sanity check on the specified attributes
  const char *invalidMsg = tenantNodeSet->isInvalid(CURRCONTEXT_CLUSTERINFO);

  if (invalidMsg) {
    *CmpCommon::diags() << DgSqlCode(-7033) << DgInt0(tenantNodeSet->getTotalWeight()) << DgString0(invalidMsg);
    UserException excp(NULL, 0);
    throw excp;
  }
}

// ----------------------------------------------------------------------------
// Method: addTenantInfo
//
// Insert details into the TENANTS and TENANT_USAGE table
// Returns:
//    0 = succeeded
//   -1 = failed (diags area is set up with error)
// ----------------------------------------------------------------------------
Int32 TenantInfo::addTenantInfo(const Int32 tenantSize) {
  // Insert a row into TENANTS table
  if (insertRow(tenantSize) != 0) return -1;

  // If usageList specified, insert usages into TENANT_USAGE table
  if (usageList_ && usageList_->entries() > 0) {
    if (usageList_->insertUsages() != 0) return -1;
  }
  return 0;
}

// ----------------------------------------------------------------------------
// Method: dropTenantInfo
//
// Deletes rows from TENANTS and TENANT_USAGE associated with the tenantID
// Returns:
//    0 = succeeded
//   -1 = failed (diags area is set up with error)
// ----------------------------------------------------------------------------
Int32 TenantInfo::dropTenantInfo() {
  // Drop any rows in TENANT_USAGE that have been granted to the current tenant
  std::string whereClause("where tenant_id = ");
  whereClause += PrivMgr::authIDToString(tenantID_);

  if (deleteUsages(whereClause.c_str()) != 0) return -1;

  if (usageList_) NADELETE(usageList_, TenantUsageList, heap_);
  usageList_ = NULL;

  // Drop the TENANTS row
  if (deleteRow() != 0) return -1;

  return 0;
}

// ----------------------------------------------------------------------------
// Method: dropTenantInfo
//
// Deletes rows from TENANTS and TENANT_USAGE associated with the tenantID
// Returns:
//    0 = succeeded
//   -1 = failed (diags area is set up with error)
// ----------------------------------------------------------------------------
Int32 TenantInfo::dropTenantInfo(bool updateTenants, const TenantUsageList *usageList) {
  // Drop all rows in TENANT_USAGE that match the usageList
  if (usageList->entries() > 0) {
    std::string whereClause("where tenant_id =  ");
    whereClause += PrivMgr::authIDToString(tenantID_);
    whereClause += " and usage_uid in (";
    for (Int32 i = 0; i < usageList->entries(); i++) {
      TenantUsage usage = (*usageList)[i];
      if (i > 0) whereClause += ", ";
      whereClause += PrivMgr::UIDToString(usage.getUsageID());
    }
    whereClause += ")";
    if (deleteUsages(whereClause.c_str()) != 0) return -1;
  }

  // Update the TENANTS row
  if (updateTenants)
    if (updateRow() != 0) return -1;

  return 0;
}

Int32 TenantInfo::modifyTenantInfo(const bool updateInfo, const TenantUsageList *usageList) {
  short retcode = 0;

  if (usageList) {
    std::string whereClause("where tenant_id = ");
    whereClause += PrivMgr::authIDToString(tenantID_);
    whereClause += " and usage_uid in (";
    bool rowsToDelete = false;
    TenantUsageList insertUsageList(STMTHEAP);

    for (CollIndex i = 0; i < usageList->entries(); i++) {
      TenantUsage usage = (*usageList)[i];
      if (usage.isUnchanged()) continue;
      if (usage.isNew()) {
        TenantUsage newUsage = usage;
        insertUsageList.insert(newUsage);
      } else if (usage.isObsolete()) {
        if (!rowsToDelete)
          rowsToDelete = true;
        else
          whereClause += ", ";
        whereClause += PrivMgr::UIDToString(usage.getUsageID());
      }
    }
    if (rowsToDelete) {
      whereClause += ")";
      if (deleteUsages(NAString(whereClause.c_str())) == -1) return -1;
    }

    if (insertUsageList.entries() > 0)
      if (insertUsageList.insertUsages() == -1) return -1;
  }

  // Update the TENANTS row
  if (updateInfo)
    if (updateRow() != 0) return -1;

  return 0;
}

// ----------------------------------------------------------------------------
// Method: updateTenantInfo
//
// updates TENANTS
// Returns:
//    0 = succeeded
//   -1 = failed (diags area is set up with error)
// ----------------------------------------------------------------------------
Int32 TenantInfo::updateTenantInfo(const bool updateInfo, TenantUsageList *usageList, const Int32 tenantSize) {
  // add usage, insert row into TENANT_USAGE table
  if (usageList && usageList->entries() > 0)
    if (usageList->insertUsages() != 0) return -1;

  // Update the TENANTS row
  if (updateInfo)
    if (updateRow(tenantSize) != 0) return -1;

  return 0;
}

//-----------------------------------------------------------------------------
// Method: deleteRow
//
// Deletes a row from the TENANTS table
// ----------------------------------------------------------------------------
Int32 TenantInfo::deleteRow() {
  NAString mdLocation;
  CONCAT_CATSCH(mdLocation, CmpSeabaseDDL::getSystemCatalogStatic(), SEABASE_TENANT_SCHEMA);
  mdLocation += NAString('.') + SEABASE_TENANTS;

  char deleteStmt[500];
  Int32 stmtSize =
      snprintf(deleteStmt, sizeof(deleteStmt), "delete from %s where tenant_id = %d", mdLocation.data(), tenantID_);

  if (stmtSize >= 500) {
    SEABASEDDL_INTERNAL_ERROR("TenantInfo::deleteRow failed, internal buffer size too small");
    return -1;
  }

  ExeCliInterface cliInterface(heap_, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());
  Int32 cliRC = cliInterface.executeImmediate(deleteStmt);
  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  // There should be one row.  but if none were found, go ahead are return
  // 0 anyway
  return 0;
}

//-----------------------------------------------------------------------------
// Method: insertRow
//
// Inserts a row into the TENANTS table
// Returns:
//   -1 = failed (diags area contains error details)
//    0 = succeeded
// ----------------------------------------------------------------------------
Int32 TenantInfo::insertRow(const Int32 tenantSize) {
  NAString mdLocation;
  CONCAT_CATSCH(mdLocation, CmpSeabaseDDL::getSystemCatalogStatic(), SEABASE_TENANT_SCHEMA);
  mdLocation += NAString('.') + SEABASE_TENANTS;

  // for now, tenantDetails is not supported, just add an empty string.
  // insertStmt length has to be increased when tenantDetails is supported (+3000)
  NAString details = tenantDetails_ ? tenantDetails_->data() : " ";
  char insertStmt[500];
  Int32 stmtSize =
      snprintf(insertStmt, sizeof(insertStmt), "insert into %s values (%d, %d, %ld, %d, %d, %d, '%s', %ld)",
               mdLocation.data(), tenantID_, adminRoleID_, defaultSchemaUID_, getAffinity(), getClusterSize(),
               ((tenantSize == -1) ? getTenantSize() : tenantSize), details.data(), flags_);
  if (stmtSize >= 500) {
    SEABASEDDL_INTERNAL_ERROR("TenantInfo::insertRow failed, internal buffer size too small");
    return -1;
  }

  ExeCliInterface cliInterface(heap_, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());
  Int32 cliRC = cliInterface.executeImmediate(insertStmt);
  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  return 0;
}

//-----------------------------------------------------------------------------
// Method: selectRow
//
// Select a row from TENANTS table
// Returns:
//   -1 = failed (diags area contains error details)
//    0 = succeeded
//
//-----------------------------------------------------------------------------
Int32 TenantInfo::selectRow(const std::string &whereClause) {
  NAString mdLocation;
  CONCAT_CATSCH(mdLocation, CmpSeabaseDDL::getSystemCatalogStatic(), SEABASE_TENANT_SCHEMA);
  mdLocation += NAString('.') + SEABASE_TENANTS;

  Int32 selectStmtSize = whereClause.length() + 500;
  char selectStmt[selectStmtSize];
  Int32 stmtSize = snprintf(selectStmt, sizeof(selectStmt),
                            "select tenant_id, admin_role_id, default_schema_uid, "
                            " affinity, cluster_size, tenant_size, flags "
                            "from %s %s",
                            mdLocation.data(), whereClause.c_str());

  if (stmtSize >= selectStmtSize) {
    SEABASEDDL_INTERNAL_ERROR("TenantInfo::getTenantInfo failed, internal buffer size too small");
    return -1;
  }

  Int32 diagsMark = CmpCommon::diags()->mark();
  ExeCliInterface cliInterface(heap_, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());
  Queue *tableQueue = NULL;
  Int32 cliRC = cliInterface.fetchAllRows(tableQueue, selectStmt, 0, false, false, true);

  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }
  if (cliRC == 100)  // did not find the row
  {
    CmpCommon::diags()->rewind(diagsMark);
    return 1;
  }

  // should only get one row back
  if (tableQueue->numEntries() != 1) {
    SEABASEDDL_INTERNAL_ERROR("Metadata inconsistency: too many tenants rows");
    return -1;
  }

  tableQueue->position();
  OutputInfo *pCliRow = (OutputInfo *)tableQueue->getNext();

  // column 0: tenant_id            column 1: admin_role_id
  // column 2: default_schema_uid   column 3: affinity
  // column 4: cluster_size         column 5: tenant_size
  // column 6: flags
  setTenantID(*(Int32 *)pCliRow->get(0));
  setAdminRoleID(*(Int32 *)pCliRow->get(1));
  setDefaultSchemaUID(*(Int64 *)pCliRow->get(2));
  Int32 affinity = (*(Int32 *)pCliRow->get(3));
  Int32 tenantSize = (*(Int32 *)pCliRow->get(5));

  // Older code stores system tenant info as 0,0,0.  During upgrade,
  // we change these values to -1,-1,-1 but this is done after we read the
  // tenant row.  Since calling NAASNodes with 0,0,0 causes an assertion,
  // change these values to -1, -1,0.
  if (ComTenant::isSystemTenant(tenantID_))
    setTenantNodes(new (heap_) NAASNodes(-1, -1, 0));
  else if (affinity >= 0) {
    // This sets up the assignedNodes as an NAASNode, this may
    setTenantNodes(new (heap_) NAASNodes(tenantSize, *(Int32 *)pCliRow->get(4), affinity));
  } else {
    setTenantNodes(new (heap_) NAWSetOfNodeIds(STMTHEAP, tenantSize));
  }

  setFlags(*(Int64 *)pCliRow->get(6));

  // get tenant usages
  TenantUsageList *usageList = new (heap_) TenantUsageList(heap_);
  usageList->selectUsages(tenantID_, heap_);
  setTenantUsages(usageList);

  // set up node allocations
  if (assignedNodes_->castToNAWSetOfNodeIds()) {
    TenantResourceUsageList *resourceUsageList = new (STMTHEAP) TenantResourceUsageList(&cliInterface, STMTHEAP);
    if (getNodeList(resourceUsageList) == -1) {
      NADELETE(resourceUsageList, TenantResourceUsageList, STMTHEAP);
      return -1;
    }

    // add node ID's and node weights
    std::map<Int32, Int32> nodeMap;
    if (CURRCONTEXT_CLUSTERINFO->hasVirtualNodes() && CURRCONTEXT_CLUSTERINFO->getTotalNumberOfCPUs() == 1) {
      assignedNodes_->castToNAWSetOfNodeIds()->addNode(0, assignedNodes_->getTotalWeight());
    } else {
      for (CollIndex i = 0; i < resourceUsageList->entries(); i++) {
        TenantResourceUsage *nodeInfo = (*resourceUsageList)[i];
        Int32 nodeID = -1;
        nodeID = CURRCONTEXT_CLUSTERINFO->mapNodeNameToNodeNum(nodeInfo->getResourceName());
        if (nodeID == IPC_CPU_DONT_CARE) {
          *CmpCommon::diags() << DgSqlCode(-CAT_NODE_NAME_INVALID) << DgString0(nodeInfo->getResourceName().data());
          return -1;
        }
        if (nodeInfo->getUsageValue() > 0) nodeMap[nodeID] = nodeInfo->getUsageValue();
      }

      for (std::map<Int32, Int32>::const_iterator it = nodeMap.begin(); it != nodeMap.end(); ++it) {
        assignedNodes_->castToNAWSetOfNodeIds()->addNode(it->first, it->second);
      }
    }
    NADELETE(resourceUsageList, TenantResourceUsageList, STMTHEAP);
  }

  return 0;
}

//-----------------------------------------------------------------------------
// Method: updateRow
//
// Updates a row in the TENANTS table
// Returns:
//   -1 = failed (diags area contains error details)
//    0 = succeeded
//
// May want to take in a set clause to make this more efficient
// ----------------------------------------------------------------------------
Int32 TenantInfo::updateRow(const Int32 tenantSize) {
  NAString mdLocation;
  CONCAT_CATSCH(mdLocation, CmpSeabaseDDL::getSystemCatalogStatic(), SEABASE_TENANT_SCHEMA);
  mdLocation += NAString('.') + SEABASE_TENANTS;

  Int32 affinity = -1;
  Int32 clusterSize = -1;
  if (assignedNodes_->castToNAASNodes()) {
    affinity = getAffinity();
    clusterSize = getClusterSize();
  }

  char updateStmt[500];
  Int32 stmtSize = snprintf(updateStmt, sizeof(updateStmt),
                            "update %s set admin_role_id =  %d, "
                            "default_schema_uid = %ld, affinity = %d, "
                            "cluster_size = %d, tenant_size = %d, "
                            "flags = %ld "
                            "where tenant_id = %d",
                            mdLocation.data(), adminRoleID_, defaultSchemaUID_, affinity, clusterSize,
                            ((tenantSize == -1) ? getTenantSize() : tenantSize), flags_, tenantID_);
  if (stmtSize >= 500) {
    SEABASEDDL_INTERNAL_ERROR("TenantInfo::updateRow failed, internal buffer size too small");
    return -1;
  }

  ExeCliInterface cliInterface(heap_, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());
  Int32 cliRC = cliInterface.executeImmediate(updateStmt);
  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }
  return 0;
}

//-----------------------------------------------------------------------------
// Method: setTenantDetails
//
// Sets the tenant detail string (makes a copy, allocated from the heap)
// ----------------------------------------------------------------------------
const void TenantInfo::setTenantDetails(const char *tenantDetails) {
  if (tenantDetails_) NADELETE(tenantDetails_, NAString, heap_);

  tenantDetails_ = new (heap_) NAString(tenantDetails, heap_);
}

//-----------------------------------------------------------------------------
// Method: setTenantNodes
//
// Sets the assigned nodes, deletes existing object
// ----------------------------------------------------------------------------
const void TenantInfo::setTenantNodes(NAWNodeSet *assignedNodes) {
  // If trying to set to itself, just return
  if (assignedNodes_ && (assignedNodes_ == assignedNodes)) return;
  if (assignedNodes_) delete assignedNodes_;

  assignedNodes_ = assignedNodes;
}

//-----------------------------------------------------------------------------
// Method: setOrigTenantNodes
//
// sets assigned nodes to passed in value.
// ----------------------------------------------------------------------------
const void TenantInfo::setOrigTenantNodes(NAWNodeSet *assignedNodes) {
  // If trying to set to itself, just return
  if (origAssignedNodes_ && (origAssignedNodes_ == assignedNodes)) return;
  if (origAssignedNodes_) delete origAssignedNodes_;

  origAssignedNodes_ = assignedNodes;
}

// -----------------------------------------------------------------------------
// Method:  getNodeList
//
// Reads RESOURCE_USAGE to get the nodes assigned to the tenant
// -----------------------------------------------------------------------------
Int32 TenantInfo::getNodeList(TenantResourceUsageList *&nodeList) {
  assert(nodeList);
  char buf[200];

  NAString whereClause;
  snprintf(buf, sizeof(buf), "where usage_uid = %d and usage_type = 'T'", getTenantID());
  whereClause = buf;

  NAString orderByClause("order by usage_name");
  if (nodeList->fetchUsages(whereClause, orderByClause) == -1) return -1;
  return 0;
}

//-----------------------------------------------------------------------------
// Method: getTenantInfo
//
// Reads the TENANTS and TENANT_USAGE tables to gather attributes for a tenant
// Returns:
//   tenantInfo
//   status
//     -1 = failed (diags area contains error details)
//      0 = succeeded
//    100 = tenant information not found
// ----------------------------------------------------------------------------
Int32 TenantInfo::getTenantInfo(const Int32 authID, TenantInfo &tenantInfo) {
  // Select from tenants table based on where clause
  std::string whereClause("where ");
  whereClause += CmpSeabaseDDLauth::isRoleID(authID) ? "admin_role_id = " : "tenant_id = ";
  whereClause += PrivMgr::authIDToString(authID);
  Int32 retcode = tenantInfo.selectRow(whereClause);
  return retcode;
}

// *****************************************************************************
// Non inline methods for tenantSchemaInfo
// *****************************************************************************

TenantSchemaInfoList::~TenantSchemaInfoList() {
  for (CollIndex i = 0; i < entries(); i++) NADELETE(operator[](i), TenantSchemaInfo, STMTHEAP);
  clear();
}

TenantSchemaInfo *TenantSchemaInfoList::find(Int64 schemaUID) {
  TenantSchemaInfo *returnedInfo = NULL;
  for (CollIndex i = 0; i < entries(); i++) {
    TenantSchemaInfo *tenantSchemaInfo = (*this)[i];
    if (tenantSchemaInfo->getSchemaUID() == schemaUID) {
      returnedInfo = tenantSchemaInfo;
      break;
    }
  }
  return returnedInfo;
}

Int32 TenantSchemaInfoList::getTenantID(Int64 schemaUID) {
  Int32 tenantID = 0;
  for (CollIndex i = 0; i < entries(); i++) {
    TenantSchemaInfo *tenantSchemaInfo = operator[](i);
    if (tenantSchemaInfo->getSchemaUID() == schemaUID) {
      tenantID = tenantSchemaInfo->getTenantID();
      break;
    }
  }
  return tenantID;
}

void TenantSchemaInfoList::getSchemaList(Int32 tenantID, NAList<Int64> &schemaList) {
  for (CollIndex i = 0; i < entries(); i++) {
    TenantSchemaInfo *tenantSchemaInfo = operator[](i);
    if (tenantSchemaInfo->getTenantID() == tenantID) schemaList.insert(tenantSchemaInfo->getSchemaUID());
  }
}

// -----------------------------------------------------------------------------
// method: removeSchemaUsage
//
// Removes the tenant schema from TENANT_USAGE table.
// If the tenant schema is the default schema, update TENANTS with new default
// schema
//
// returns:
//   -1 - failed (ComDiags contains error)
//    0 - succeeded
// -----------------------------------------------------------------------------
Int32 TenantSchemaInfo::removeSchemaUsage() {
  TenantInfo tenantInfo(STMTHEAP);
  CmpSeabaseDDLtenant tenantDefinition;
  if (tenantDefinition.getTenantDetails(getTenantID(), tenantInfo) == CmpSeabaseDDLauth::STATUS_ERROR) return -1;

  // create a usage list with one entry describing the schema to be removed
  TenantUsage tenantUsage(getTenantID(), getSchemaUID(), TenantUsage::TENANT_USAGE_SCHEMA);
  TenantUsageList usageList(STMTHEAP);
  usageList.insert(tenantUsage);

  // If removing default schema, see if there is another applicable schema to be default
  // If there is more than 1 schema left after removing the schema usage, then
  // that schema is set as default, otherwise, the default is set to 0.
  Int64 defSchUID = 0;
  if (isDefSch() && tenantInfo.getNumberSchemaUsages() == 2) {
    const TenantUsageList *existingUsageList = tenantInfo.getUsageList();
    TenantUsage firstUsage = (*existingUsageList)[0];
    TenantUsage lastUsage = (*existingUsageList)[1];
    defSchUID = (firstUsage.getUsageID() == getSchemaUID()) ? lastUsage.getUsageID() : firstUsage.getUsageID();
  }

  tenantInfo.setDefaultSchemaUID(defSchUID);
  if (tenantInfo.dropTenantInfo(isDefSch(), &usageList) == -1) return -1;
  return 0;
}

// *****************************************************************************
// Non inline methods for tenantGroupInfo
// *****************************************************************************

// ****************************************************************************
// Non-inline methods for class:  TenantResource
//
// This class defines a resource group
// ****************************************************************************

// -----------------------------------------------------------------------------
// static method: getResourceTypeAsEnum
//
// Translate the string resourceType into an enum
// No errors are generated
// -----------------------------------------------------------------------------
TenantResource::TenantResourceType TenantResource::getResourceTypeAsEnum(NAString &resourceType) {
  if (resourceType == "N ") return TenantResourceType::RESOURCE_TYPE_NODE;
  if (resourceType == "G ") return TenantResourceType::RESOURCE_TYPE_GROUP;
  return TenantResourceType::RESOURCE_TYPE_UNKNOWN;
}

// -----------------------------------------------------------------------------
// static method: getResourceTypeAsString
//
// Translate the enum resourceType into a string
// No errors are generated
// -----------------------------------------------------------------------------
NAString TenantResource::getResourceTypeAsString(TenantResourceType resourceType) {
  NAString resourceString;
  switch (resourceType) {
    case RESOURCE_TYPE_NODE:
      resourceString = "N ";
      break;
    case RESOURCE_TYPE_GROUP:
      resourceString = "G ";
      break;
    case RESOURCE_TYPE_UNKNOWN:
    default:
      resourceString = " ";
      break;
  }
  return resourceString;
}

// -----------------------------------------------------------------------------
// static method: validNodeName
//
// this method calls mapNodeNameToNodeNum from NAClusterInfo get the list of
// node names for the cluster.  If verifies that the passed in node names are
// valid names
//
// returns:
//    true  - if names exist
//    false - if names do not exist or unexpected error occurs.
//
// It returns the first node name that is not valid
// -----------------------------------------------------------------------------
bool TenantResource::validNodeNames(ConstStringList *usageNameList, NAString &invalidNodeNames) {
  // for development machines, don't verify
  NAString nodeAllocationList = CmpCommon::getDefaultString(NODE_ALLOCATIONS).data();
  if ((getenv("CLUSTERNAME") == NULL) || nodeAllocationList.length() > 0) return true;

  // get the list of valid node names from ?
  // see if names in the usageNameList exist in valid names list
  Int32 numInvalidNodes = 0;
  for (CollIndex i = 0; i < usageNameList->entries(); i++) {
    NAString nodeName = *(*usageNameList)[i];
    size_t pos = nodeName.first('.');
    if (pos != NA_NPOS) nodeName.remove(pos);

    Int32 nodeID = CURRCONTEXT_CLUSTERINFO->mapNodeNameToNodeNum(nodeName);
    if (nodeID == IPC_CPU_DONT_CARE) {
      if (numInvalidNodes > 0)
        invalidNodeNames += ", '";
      else
        invalidNodeNames += "('";
      invalidNodeNames += nodeName;
      invalidNodeNames += "'";
      numInvalidNodes++;
    }
  }
  if (numInvalidNodes > 0) {
    invalidNodeNames += ")";
    return false;
  }
  return true;
}

// -----------------------------------------------------------------------------
// Method: alterNodesForRGroupTenants
//
// Rebalances tenants that lose one or more nodes
// Removes the node-tenant relationship from resource usage.
//
// Input:
//    cliInterface - used to query the metadata
//    tenantInfoList - list of tenants that may need to be balanced
//    deleteNodes - list of nodes to delete
//
// Returns:
//    0 - successful
//   -1 - failed (ComDiags contains error)
// -----------------------------------------------------------------------------
short TenantResource::alterNodesForRGroupTenants(ExeCliInterface *cliInterface,
                                                 const NAList<TenantInfo *> *tenantInfoList,
                                                 const NAList<NAString> &deletedNodes) {
  // TBD - perhaps get node-tenant relationships for all affacted tenants
  // and cache.  Then process each tenant to see if rebalance is needed
  for (CollIndex i = 0; i < tenantInfoList->entries(); i++) {
    TenantInfo *tenantInfo = (*tenantInfoList)[i];
    NAList<NAString> nodesNotFound(STMTHEAP);

    if (getNodesToDropFromTenant(cliInterface, tenantInfo, deletedNodes, nodesNotFound) == -1) return -1;

    // All nodes are available from other rgroups, go to next tenant
    if (nodesNotFound.entries() == 0) continue;

    bool registrationComplete = false;
    Int32 tenantSize = tenantInfo->getTenantNodes()->getTotalWeight();

    tenantInfo->setOrigTenantNodes(NULL);
    tenantInfo->setTenantNodes(NULL);

    // temporary memory
    TenantResourceUsageList *resourceUsageList = NULL;
    TenantNodeInfoList *origNodeList = NULL;
    TenantNodeInfoList *newNodeList = NULL;

    try {
      // Get node-tenant usages before changes are made
      resourceUsageList = new (STMTHEAP) TenantResourceUsageList(cliInterface, STMTHEAP);
      if (tenantInfo->getNodeList(resourceUsageList) == -1) {
        UserException excp(NULL, 0);
        throw excp;
      }

      origNodeList = new (STMTHEAP) TenantNodeInfoList(STMTHEAP);
      if (CmpSeabaseDDLtenant::getNodeDetails(resourceUsageList, origNodeList) == -1) {
        UserException excp(NULL, 0);
        throw excp;
      }

      // create the new node list, don't include obsolete nodes
      bool balanceRequired = false;
      newNodeList = new (STMTHEAP) TenantNodeInfoList(STMTHEAP);
      for (CollIndex j = 0; j < resourceUsageList->entries(); j++) {
        TenantResourceUsage *rUsage = (*resourceUsageList)[j];
        NAString nodeName = rUsage->getResourceName();
        if (deletedNodes.contains(nodeName) && nodesNotFound.contains(nodeName)) {
          rUsage->setIsObsolete(true);
          balanceRequired = true;
        } else {
          Int32 nodeID = origNodeList->getNodeID(rUsage->getResourceName());
          assert(!(nodeID == IPC_CPU_DONT_CARE));
          TenantNodeInfo *newNodeInfo =
              new (STMTHEAP) TenantNodeInfo(rUsage->getResourceName(), nodeID, -1, rUsage->getUsageUID(), -1, STMTHEAP);
          newNodeList->orderedInsert(newNodeInfo);
        }
      }

      // We found some nodes that are no longer available
      if (balanceRequired) {
        // rebalance tenant
        NAString nodeAllocationList = CmpCommon::getDefaultString(NODE_ALLOCATIONS).data();
        if (getenv("CLUSTERNAME") || nodeAllocationList.length() == 0) {
          NAWNodeSet *origTenantNodeSet = NULL;
          tenantInfo->allocTenantNode(FALSE, -1, -1, tenantSize, origNodeList, origTenantNodeSet, true);
          tenantInfo->setOrigTenantNodes(origTenantNodeSet);

          NAWNodeSet *tenantNodeSet = NULL;
          tenantInfo->allocTenantNode(FALSE, -1, -1, tenantSize, newNodeList, tenantNodeSet, false);
          tenantInfo->setTenantNodes(tenantNodeSet);

          // adjust node allocations, etc
          tenantInfo->balanceTenantNodeAlloc(tenantInfo->getDefaultSchemaName(), -2, false, origTenantNodeSet,
                                             tenantNodeSet, resourceUsageList, registrationComplete);
        } else {
          // node allocations is a list of space separated node details
          NAString nodeAllocationList = CmpCommon::getDefaultString(NODE_ALLOCATIONS).data();
          if (nodeAllocationList.length() > 0) {
            Int32 unitsToAllocate = tenantSize;
            tenantSize = CmpSeabaseDDLtenant::predefinedNodeAllocations(unitsToAllocate, resourceUsageList);
          }
        }

        // delete obsolete node-tenant usages
        if (resourceUsageList->modifyUsages() == -1) {
          UserException excp(NULL, 0);
          throw excp;
        }
      }
    }  // try

    catch (...) {
      // if registrationComplete is true, reset node allocation
      // back to the original values
      if (registrationComplete) {
        try {
          // change the new set of allocations back to the original set
          NAWNodeSet *origSet = (NAWNodeSet *)tenantInfo->getOrigTenantNodes();
          NAWNodeSet *newSet = (NAWNodeSet *)tenantInfo->getTenantNodes();
          tenantInfo->balanceTenantNodeAlloc(tenantInfo->getDefaultSchemaName(), -2, /* use existing session limit */
                                             true, /*backing out changes already performed */
                                             newSet, origSet, resourceUsageList, registrationComplete);
          tenantInfo->setTenantNodes(NULL);
          tenantInfo->setOrigTenantNodes(NULL);
          *CmpCommon::diags() << DgSqlCode(-CAT_RESOURCE_GROUP_NO_RESOURCES) << DgString0(getResourceName().data())
                              << DgString1(tenantInfo->getTenantName().data());
        } catch (...) {
          NAString msg("AlterResourceGroup: unable to reset tenant allocations for tenant ");
          msg += tenantInfo->getTenantName();
          SEABASEDDL_INTERNAL_ERROR(msg.data());
        }
      }

      if (origNodeList) NADELETE(origNodeList, TenantNodeInfoList, STMTHEAP);

      if (newNodeList) NADELETE(newNodeList, TenantNodeInfoList, STMTHEAP);

      if (resourceUsageList) NADELETE(resourceUsageList, TenantResourceUsageList, STMTHEAP);

      // At this time, an error should be in the diags area.
      // If there is no error, set up an internal error
      if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_) == 0)
        SEABASEDDL_INTERNAL_ERROR("catch block in TenantResource::alterNodesForRGroupTenants");

      return -1;
    }

    if (newNodeList) NADELETE(newNodeList, TenantNodeInfoList, STMTHEAP);
    if (origNodeList) NADELETE(origNodeList, TenantNodeInfoList, STMTHEAP);
    if (resourceUsageList) NADELETE(resourceUsageList, TenantResourceUsageList, STMTHEAP);

  }  // loop for each tenant
  return 0;
}

// -----------------------------------------------------------------------------
// Method: assignedCurrentTenant
//
// Input:
//   tenantID - tenant to check
//
// Returns:
//   true - resource group has been assigned tenant
//   false - resource group not part of tenant
// -----------------------------------------------------------------------------
bool TenantResource::assignedTenant(const NAString &tenantName) {
  NAString tenantNames;
  if (getTenantsForResource(tenantNames) == -1) return false;
  if (tenantNames.length() == 0) return false;

  bool done = false;
  char delimiter(',');

  std::string tempNames = tenantNames.data();
  size_t pos = tempNames.find(delimiter);
  while (!done) {
    NAString tempName;
    if (pos == string::npos) {
      tempName = tempNames.substr(0, tempNames.length()).c_str();
      done = true;
    } else
      tempName = tempNames.substr(0, pos).c_str();

    tempName = tempName.strip(NAString::leading, ' ');
    if (tempName == tenantName) return true;

    // get next tenant in list
    tempNames.erase(0, pos + 1);
    pos = tempNames.find(delimiter);
  }

  return false;
}

// -----------------------------------------------------------------------------
// Method: backoutAssignedNodes
//
// Input:
//    cliInterface - used to access metadata
//    tenantInfoList - list of tenants
//
// Returns:
//    0 - successful
//   -1 - failed to backout changes
// -----------------------------------------------------------------------------
short TenantResource::backoutAssignedNodes(ExeCliInterface *cliInterface, const NAList<TenantInfo *> &tenantInfoList) {
  for (CollIndex i = 0; i < tenantInfoList.entries(); i++) {
    TenantInfo *tenantInfo = tenantInfoList[i];

    // If node assignments already successful, set them back to original
    if (tenantInfo->getOrigTenantNodes() && tenantInfo->getTenantNodes()) {
      TenantResourceUsageList *resourceUsageList = new (STMTHEAP) TenantResourceUsageList(cliInterface, STMTHEAP);
      try {
        if (tenantInfo->getNodeList(resourceUsageList) == -1) {
          UserException excp(NULL, 0);
          throw excp;
        }

        // set the new node assignments back to the original set.
        // **** add reset weight code ***
        bool registrationComplete = false;
        NAWNodeSet *origSet = (NAWNodeSet *)tenantInfo->getOrigTenantNodes();
        NAWNodeSet *newSet = (NAWNodeSet *)tenantInfo->getTenantNodes();
        tenantInfo->balanceTenantNodeAlloc(tenantInfo->getDefaultSchemaName(), -2, true, newSet, origSet,
                                           resourceUsageList, registrationComplete);
        NADELETE(resourceUsageList, TenantResourceUsageList, STMTHEAP);
        *CmpCommon::diags() << DgSqlCode(-CAT_RESOURCE_GROUP_NO_RESOURCES) << DgString0(getResourceName().data())
                            << DgString1(tenantInfo->getTenantName().data());

      } catch (...) {
        if (resourceUsageList) NADELETE(resourceUsageList, TenantResourceUsageList, STMTHEAP);

        return -1;
      }
    }
  }
  return 0;
}

// -----------------------------------------------------------------------------
// Method:  createStandardResources
//
// Creates the system default resource groups
//   currently there is only one: DB__RGROUP_DEFAULT
// -----------------------------------------------------------------------------
short TenantResource::createStandardResources() {
  // Remove existing rows first - fixes bug where each time upgrade is called,
  // a new row is inserted.  there should be only one row in resources that
  // define RGROUP_DEFAULT. Something to consider redoing at upgrade time. That
  // is, don't insert a new row if it already exists.
  char deleteStmt[500];
  NAString sysCat = CmpSeabaseDDL::getSystemCatalogStatic();
  Int32 stmtSize = snprintf(deleteStmt, sizeof(deleteStmt), "delete from %s.\"%s\".%s where resource_name = '%s'",
                            sysCat.data(), SEABASE_TENANT_SCHEMA, SEABASE_RESOURCES, RGROUP_DEFAULT);
  assert(stmtSize < 500);

  ExeCliInterface *cliInterface = getCli();
  assert(cliInterface);
  Int32 cliRC = cliInterface->executeImmediate(deleteStmt);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  Int64 currTime = NA_JulianTimestamp();
  ComUID groupUID;
  groupUID.make_UID();
  NAString def(RGROUP_DEFAULT);

  setCreateTime(currTime);
  setCreator(ComUser::getCurrentUser());
  setFlags(0);
  setResourceName(def);
  setResourceUID(groupUID.get_value());
  setIsValid(true);
  setRedefTime(currTime);
  setType(TenantResource::RESOURCE_TYPE_GROUP);

  return insertRow();
}

// -----------------------------------------------------------------------------
// method: describe
//
// This method generates SHOWDDL text for a resource group
//
// In:  groupName
//
// Out: SHOWDDL text
//
// returns false if unable to generate text (ComDiags set up with error)
// -----------------------------------------------------------------------------
bool TenantResource::describe(const NAString &groupName, NAString &groupText) {
  // If authorization is not enabled then just return
  if (!CmpCommon::context()->isAuthorizationEnabled()) return true;

  NAString whereClause("WHERE resource_name = '");
  whereClause += groupName + NAString("'");
  short retcode = fetchMetadata(whereClause, whereClause, true);
  if (retcode == -1) return false;

  if (retcode == 1) {
    *CmpCommon::diags() << DgSqlCode(-CAT_RESOURCE_GROUP_NOT_EXIST) << DgString0(groupName.data());
    return false;
  }

  // See if user has privilege to perform command
  bool hasPriv = false;
  int32_t currentUser = ComUser::getCurrentUser();
  if ((currentUser != ComUser::getRootUserID()) && (currentUser != getCreator())) {
    // anyone with MANAGE_TENANTS or MANAGE_RESOURCE_GROUPS can perform SHOWDDL
    // If resource group assigned current tenant, then can perform request
    // othersize, return resource group not exists
    NAString currentTenant(ComTenant::getCurrentTenantName());
    PrivMgrComponentPrivileges componentPrivileges;
    if (!componentPrivileges.hasSQLPriv(currentUser, SQLOperation::MANAGE_TENANTS, true) &&
        !componentPrivileges.hasSQLPriv(currentUser, SQLOperation::MANAGE_RESOURCE_GROUPS, true) &&
        !componentPrivileges.hasSQLPriv(currentUser, SQLOperation::SHOW, true) && !assignedTenant(currentTenant)) {
      *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);
      return false;
    }
  }

  groupText = "CREATE RESOURCE GROUP ";
  groupText += groupName;

  if (usageList_) {
    groupText += " NODES('";
    for (Int32 i = 0; i < usageList_->entries(); i++) {
      if (i > 0) groupText += "', '";
      TenantResourceUsage *usage = (*usageList_)[i];
      if (usage->isNodeUsage()) groupText += usage->getUsageName();
    }
    groupText += "')";
  }

  char ownerName[MAX_DBUSERNAME_LEN + 1];
  int32_t length;
  ComUser::getAuthNameFromAuthID(getCreator(), ownerName, MAX_DBUSERNAME_LEN, length, TRUE,
                                 NULL /*don't update diags*/);
  groupText += " AUTHORIZATION ";
  groupText += ownerName;

  groupText += ";\n";

  if (!isValid()) groupText += "-- resource group is offline\n";
  return true;
}

// -----------------------------------------------------------------------------
// method: generateNodeResource
//
// Checks to see if node resource defined.  If not, it is generated and added
// to the metadata
//
// nodeName - name of node resource
// grantee - owner of node resource
//
// returns:
//   -1 - unexpected error (ComDiags is set up)
//    0 - successful
// -----------------------------------------------------------------------------
short TenantResource::generateNodeResource(NAString *nodeName, const Int32 grantee) {
  // see if node already exists
  NAString whereClause("WHERE resource_name = '");
  whereClause += nodeName->data() + NAString("'");
  short retcode = fetchMetadata(whereClause, whereClause, false);
  if (retcode == -1) return -1;

  if (retcode == 1) {
    Int64 createTime = NA_JulianTimestamp();
    ComUID myUID;
    myUID.make_UID();

    setCreateTime(createTime);
    setCreator(grantee);
    setFlags(0);
    setResourceName(*nodeName);
    setResourceUID(myUID.get_value());
    setIsValid(true);
    setRedefTime(createTime);
    setType(TenantResource::RESOURCE_TYPE_NODE);
    if (insertRow() == -1) return -1;
  }
  return 0;
}

// -----------------------------------------------------------------------------
// method: getNodesToDropFromTenant
//
// Returns the list of nodes that are not available to the tenant based on
// the list of "deletedNodes" minus the current resource group.
//
// input:
//   cliInterface - used to read metadata
//   tenantInfo - description of the tenant
//   deletedNodes - list of nodes to check
//
// output:
//   nodesNotFound - list of nodes not available to the tenant
//
// returns:
//   -1 - unable to retrieve metadata
//    0 - successful
// -----------------------------------------------------------------------------
short TenantResource::getNodesToDropFromTenant(ExeCliInterface *cliInterface, const TenantInfo *tenantInfo,
                                               const NAList<NAString> &deletedNodes, NAList<NAString> &nodesNotFound) {
  // Determine if the tenant has access to all nodes in deletedNodes list
  // from other resource groups.

  // Get list of nodes for tenant from "other" resource groups
  NAString whereClause("where resource_uid in (");
  bool rgroupsExist = false;
  char buf[200];
  const TenantUsageList *usageList = ((TenantInfo *)tenantInfo)->getUsageList();
  for (int i = 0; i < usageList->entries(); i++) {
    TenantUsage usage = (*usageList)[i];
    if (usage.isRGroupUsage() && (!(usage.getUsageID() == getResourceUID()))) {
      snprintf(buf, sizeof(buf), "%s %ld", (rgroupsExist ? ", " : " "), usage.getUsageID());
      rgroupsExist = true;
      whereClause += buf;
    }
  }

  TenantResourceUsageList rgroupUsages(cliInterface, STMTHEAP);
  if (rgroupsExist) {
    whereClause += ") and usage_type = 'N'";
    NAString orderByClause("order by resource_name, usage_name");
    if (rgroupUsages.fetchUsages(whereClause, orderByClause) == -1) return -1;
  }

  // See if any nodes are now unavailable to tenant
  for (int i = 0; i < deletedNodes.entries(); i++) {
    NAString nodeName = deletedNodes[i];
    if (rgroupUsages.findUsage(&nodeName, TenantResourceUsage::RESOURCE_USAGE_NODE)) continue;
    nodesNotFound.insert(nodeName);
  }
  return 0;
}

// -----------------------------------------------------------------------------
// method: getTenantsForNodes
//
// Returns the list of tenant names that meet the passed in list of nodes
//
// status:
//   -1 - unexpected error returned (ComDiags is setup)
//    0 - request successful
// -----------------------------------------------------------------------------
short TenantResource::getTenantsForNodes(const NAList<NAString> &nodeList, NAString &tenantNames) {
  assert(tenantNames.isNull());

  NAString resourceNameList("(");
  for (CollIndex i = 0; i < nodeList.entries(); i++) {
    if (i > 0)
      resourceNameList += "', '";
    else
      resourceNameList += "'";
    resourceNameList += nodeList[i];
  }
  resourceNameList += "')";

  Int32 bufSize = 500 + resourceNameList.length();
  char selectStmt[bufSize];
  NAString sysCat = CmpSeabaseDDL::getSystemCatalogStatic();
  Int32 stmtSize = snprintf(selectStmt, sizeof(selectStmt),
                            "select auth_db_name from %s.\"%s\".%s "
                            "where auth_id in (select distinct usage_uid from %s.\"%s\".%s "
                            "where resource_name in %s) ",
                            sysCat.data(), SEABASE_MD_SCHEMA, SEABASE_AUTHS, sysCat.data(), SEABASE_TENANT_SCHEMA,
                            SEABASE_RESOURCE_USAGE, resourceNameList.data());
  assert(stmtSize < bufSize);

  Int32 diagsMark = CmpCommon::diags()->mark();
  ExeCliInterface *cliInterface = getCli();
  assert(cliInterface);
  Queue *tableQueue = NULL;
  Int32 cliRC = cliInterface->fetchAllRows(tableQueue, selectStmt, 0, false, false, true);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  // return an empty string
  if (cliRC == 100) {
    CmpCommon::diags()->rewind(diagsMark);
    return 0;
  }

  // return an empty string
  if (cliRC == 100) {
    CmpCommon::diags()->rewind(diagsMark);
    return 0;
  }

  tableQueue->position();
  for (Int32 i = 0; i < tableQueue->numEntries(); i++) {
    OutputInfo *pCliRow = (OutputInfo *)tableQueue->getNext();

    // column 0:  tenant name
    if (i > 0) tenantNames += ", ";
    tenantNames += (char *)pCliRow->get(0);
  }

  return 0;
}

// -----------------------------------------------------------------------------
// method: getTenantsForResource
//
// Returns the list of tenant names that have been assigned the resource group.
//
// status:
//   -1 - unexpected error returned (ComDiags is setup)
//    0 - request successful
// -----------------------------------------------------------------------------
short TenantResource::getTenantsForResource(NAString &tenantNames) {
  assert(tenantNames.isNull());

  Int32 bufSize = 500;
  char selectStmt[bufSize];
  NAString sysCat = CmpSeabaseDDL::getSystemCatalogStatic();
  Int32 stmtSize = snprintf(selectStmt, sizeof(selectStmt),
                            "select auth_db_name from %s.\"%s\".%s where auth_id in "
                            "(select distinct tenant_id from %s.\"%s\".%s "
                            "where usage_type = 'RG' and usage_uid = %ld) ",
                            sysCat.data(), SEABASE_MD_SCHEMA, SEABASE_AUTHS, sysCat.data(), SEABASE_TENANT_SCHEMA,
                            SEABASE_TENANT_USAGE, getResourceUID());
  assert(stmtSize < bufSize);

  Int32 diagsMark = CmpCommon::diags()->mark();
  ExeCliInterface *cliInterface = getCli();
  assert(cliInterface);
  Queue *tableQueue = NULL;
  Int32 cliRC = cliInterface->fetchAllRows(tableQueue, selectStmt, 0, false, false, true);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  // return an empty string
  if (cliRC == 100) {
    CmpCommon::diags()->rewind(diagsMark);
    return 0;
  }

  // return an empty string
  if (cliRC == 100) {
    CmpCommon::diags()->rewind(diagsMark);
    return 0;
  }

  tableQueue->position();
  for (Int32 i = 0; i < tableQueue->numEntries(); i++) {
    OutputInfo *pCliRow = (OutputInfo *)tableQueue->getNext();

    // column 0:  tenant name
    if (i > 0) tenantNames += ", ";
    tenantNames += (char *)pCliRow->get(0);
  }
  return 0;
}

// -----------------------------------------------------------------------------
// Method: getTenantInfoList
//
// Input:
//    tenantNames - list of comma separated tenant names
//
// Output:
//    tenantInfoList - a list of <TenantInfo *> for each tenant in tenantNames
//
// Returns:
//     0 - tenant information setup
//    -1 - error occurred (ComDiags is set up with issue)
// -----------------------------------------------------------------------------
short TenantResource::getTenantInfoList(const NAString &tenantNames, NAList<TenantInfo *> &tenantInfoList) {
  if (tenantNames.length() == 0) return 0;

  bool done = false;
  char delimiter(',');

  std::string tempNames = tenantNames.data();
  size_t pos = tempNames.find(delimiter);
  while (!done) {
    NAString tenantName;
    if (pos == string::npos) {
      tenantName = tempNames.substr(0, tempNames.length()).c_str();
      tenantName = tenantName.strip(NAString::leading, ' ');
      done = true;
    } else {
      tenantName = tempNames.substr(0, pos).c_str();
      tenantName = tenantName.strip(NAString::leading, ' ');
    }

    CmpSeabaseDDLtenant tenantAuth;
    TenantInfo tenantInfo(STMTHEAP);
    CmpSeabaseDDLauth::AuthStatus authStatus = tenantAuth.getTenantDetails(tenantName, tenantInfo);
    if (authStatus == CmpSeabaseDDLauth::STATUS_ERROR) return -1;

    // at this time, the tenant name should be valid, if not internal error
    if (authStatus == CmpSeabaseDDLauth::STATUS_NOTFOUND) {
      NAString msg("TenantResource::getTenantInfoList failed, tenant name invalid: ");
      msg += tenantName;
      SEABASEDDL_INTERNAL_ERROR(msg.data());
      return -1;
    }

    // should not be an AS tenant, but ...
    if (tenantInfo.getTenantNodes()->castToNAASNodes()) continue;

    // TBD:  add a copy constructor
    TenantUsageList *usageList = new (STMTHEAP) TenantUsageList(STMTHEAP);
    for (CollIndex j = 0; j < tenantInfo.getUsageList()->entries(); j++) {
      TenantUsage usage = (*tenantInfo.getUsageList())[j];
      usageList->insert(usage);
    }

    TenantInfo *newTenantInfo =
        new (STMTHEAP) TenantInfo(tenantInfo.getTenantID(), tenantInfo.getAdminRoleID(),
                                  tenantInfo.getDefaultSchemaUID(), usageList, NULL, /*tenantNodes*/
                                  tenantName, STMTHEAP);
    newTenantInfo->setTenantNodes(tenantInfo.getTenantNodes()->copy(STMTHEAP));
    tenantInfoList.insert(newTenantInfo);

    // get next tenant in list
    tempNames.erase(0, pos + 1);
    pos = tempNames.find(delimiter);
  }
  return 0;
}

// -----------------------------------------------------------------------------
// method: removeObsoleteNodes
//
// removes node resources that are not longer being used
//
// returns:
//   -1 - unexpected error (ComDiags is setup)
//    0 - successful
// -----------------------------------------------------------------------------
short TenantResource::removeObsoleteNodes(const TenantResourceUsageList *usageList) {
  assert(usageList);
  if (usageList->entries() == 0) return 0;

  // get list of nodes in the resource group being dropped that are used by other resource groups
  NAString valuesClause("(");
  for (CollIndex i = 0; i < usageList->entries(); i++) {
    if (i > 0) valuesClause += "), (";
    TenantResourceUsage *usage = (*usageList)[i];
    valuesClause += (to_string((long long int)usage->getUsageUID()).c_str());
  }
  valuesClause += ")";

  Int32 bufSize = 500 + valuesClause.length();
  char deleteStmt[bufSize];

  NAString sysCat = CmpSeabaseDDL::getSystemCatalogStatic();
  Int32 stmtSize = snprintf(deleteStmt, sizeof(deleteStmt),
                            "delete from %s.\"%s\".%s where resource_uid in "
                            "(select uid from (values %s) u(uid) where "
                            "u.uid not in (select usage_uid from %s.\"%s\".%s "
                            "where  resource_uid <> %ld)); ",
                            sysCat.data(), SEABASE_TENANT_SCHEMA, SEABASE_RESOURCES, valuesClause.data(), sysCat.data(),
                            SEABASE_TENANT_SCHEMA, SEABASE_RESOURCE_USAGE, getResourceUID());
  assert(stmtSize < bufSize);

  ExeCliInterface *cliInterface = getCli();
  assert(cliInterface);
  Int32 cliRC = cliInterface->executeImmediate(deleteStmt);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  return 0;
}

// -----------------------------------------------------------------------------
// method: deleteRow
//
// deletes the row from the RESOURCES table based on the resource_uid
//
// returns:
//    -1 - unexpected error (ComDiags is setup)
//     0 - successful
// -----------------------------------------------------------------------------
short TenantResource::deleteRow() {
  char deleteStmt[500];
  NAString sysCat = CmpSeabaseDDL::getSystemCatalogStatic();
  Int32 stmtSize = snprintf(deleteStmt, sizeof(deleteStmt), "delete from %s.\"%s\".%s where resource_uid = %ld",
                            sysCat.data(), SEABASE_TENANT_SCHEMA, SEABASE_RESOURCES, getResourceUID());
  assert(stmtSize < 500);

  ExeCliInterface *cliInterface = getCli();
  assert(cliInterface);
  Int32 cliRC = cliInterface->executeImmediate(deleteStmt);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  return 0;
}

// -----------------------------------------------------------------------------
// method: fetchMetadata
//
// Fetches metadata by reading RESOURCES and optionally the
// RESOURCE_USAGE table
//
// In: resourceClause - where clause for reading RESOURCES table
//     resourceUsageClause - where clause for reading RESOURCE_USAGE table
//     fetchUsages - optionally return usages from the RESOURCE_USAGE table
// Out: class members are filled in
//
// resourceClause should only return one row from RESOURCES
//
// returns:
//   -1 - unexpected error (ComDiags setup)
//    0 - successful
//    1 - rows does not exist
// -----------------------------------------------------------------------------
short TenantResource::fetchMetadata(const NAString &resourceClause, const NAString &resourceUsageClause,
                                    bool fetchUsages) {
  short retcode = 0;
  retcode = selectRow(resourceClause);
  if (retcode != 0) return retcode;

  if (fetchUsages) {
    assert(usageList_ == NULL);
    usageList_ = new (getHeap()) TenantResourceUsageList(getCli(), getHeap());
    NAString orderByClause("order by usage_name");
    retcode = usageList_->fetchUsages(resourceUsageClause, orderByClause);
  }
  return retcode;
}

// -----------------------------------------------------------------------------
// method: insertRow
//
// Insert the current row into the RESOURCES table
//
// returns:
//    -1 - unexpected error (ComDiags is setup)
//     0 - successful
// -----------------------------------------------------------------------------
short TenantResource::insertRow() {
  NAString isValid = (isValid_) ? "Y" : "N";
  NAString type = getResourceTypeAsString(type_);
  char insertStmt[500];
  NAString sysCat = CmpSeabaseDDL::getSystemCatalogStatic();
  Int32 stmtSize =
      snprintf(insertStmt, sizeof(insertStmt),
               "insert into %s.\"%s\".%s values (%ld, '%s', '%s', %d,"
               " '%s', %ld, %ld, '%s', '%s', %ld)",
               sysCat.data(), SEABASE_TENANT_SCHEMA, SEABASE_RESOURCES, UID_, name_.data(), type.data(), creator_,
               isValid.data(), createTime_, redefTime_, details1_.data(), details2_.data(), flags_);
  if (stmtSize >= 500) {
    SEABASEDDL_INTERNAL_ERROR("TenantInfo::insertRow failed, internal buffer size too small");
    return -1;
  }

  ExeCliInterface *cliInterface = getCli();
  assert(cliInterface);
  Int32 cliRC = cliInterface->executeImmediate(insertStmt);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }
  return 0;
}

// -----------------------------------------------------------------------------
// method: selectRow
//
// reads the RESOURCES table to retrieve group details based on the
// passed in whereClause and sets details in class members
//
// only expects 1 row to be returned
//
// returns:
//   -1 - unexpected error (ComDiags is setup with error)
//    0 - successful
//    1 - no rows returned
// -----------------------------------------------------------------------------
short TenantResource::selectRow(const NAString &whereClause) {
  char selectStmt[500];
  NAString sysCat = CmpSeabaseDDL::getSystemCatalogStatic();
  Int32 stmtSize = snprintf(selectStmt, sizeof(selectStmt),
                            "select resource_uid, resource_name, resource_creator, resource_is_valid,"
                            "resource_create_time, resource_redef_time, "
                            "resource_details1, resource_details2, resource_type, flags  from %s.\"%s\".%s %s",
                            sysCat.data(), SEABASE_TENANT_SCHEMA, SEABASE_RESOURCES, whereClause.data());

  if (stmtSize >= 500) {
    SEABASEDDL_INTERNAL_ERROR("TenantResource::selectRow failed, internal buffer size too small");
    return -1;
  }

  Int32 diagsMark = CmpCommon::diags()->mark();
  ExeCliInterface *cliInterface = getCli();
  assert(cliInterface);
  Queue *tableQueue = NULL;
  Int32 cliRC = cliInterface->fetchAllRows(tableQueue, selectStmt, 0, false, false, true);

  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }
  if (cliRC == 100)  // did not find the row
  {
    CmpCommon::diags()->rewind(diagsMark);
    return 1;
  }

  // should only get one row back
  if (tableQueue->numEntries() != 1) {
    SEABASEDDL_INTERNAL_ERROR("Metadata inconsistency: too many resource group rows");
    return -1;
  }

  tableQueue->position();
  OutputInfo *pCliRow = (OutputInfo *)tableQueue->getNext();

  // column  0: resource_uid           column  1: resource_name
  // column  2: resource_creator       column  3: resource_is_valid
  // column  4: resource_create_time   column  5: resource_redef_time
  // column  6: resource_details1      column  7: resource_details2
  // column  8: resource_type          column  9: flags
  setResourceUID(*(Int64 *)pCliRow->get(0));
  NAString resourceName = (char *)pCliRow->get(1);
  setResourceName(resourceName);
  setCreator(*(Int32 *)pCliRow->get(2));
  NAString isValid = (char *)pCliRow->get(3);
  setIsValid((isValid == "Y ") ? true : false);
  setCreateTime(*(Int64 *)pCliRow->get(4));
  setRedefTime(*(Int64 *)pCliRow->get(5));
  NAString details = (char *)pCliRow->get(6);
  setDetails1(details);
  details = (char *)pCliRow->get(7);
  setDetails2(details);
  NAString type = (char *)pCliRow->get(8);
  setType(TenantResource::getResourceTypeAsEnum(type));
  setFlags(*(Int64 *)pCliRow->get(9));

  return 0;
}

// -----------------------------------------------------------------------------
// method: updateRows
//
// Updates rows in RESOURCE GROUPS table based on the passed in setClause and
// whereClause
//
// returns:
//    -1 - unexpected error (ComDiags is setup)
//     0 - successful
// -----------------------------------------------------------------------------
short TenantResource::updateRows(const char *setClause, const char *whereClause) {
  char updateStmt[1000];
  NAString sysCat = CmpSeabaseDDL::getSystemCatalogStatic();
  Int32 stmtSize = snprintf(updateStmt, sizeof(updateStmt), "update %s.\"%s\".%s %s %s", sysCat.data(),
                            SEABASE_TENANT_SCHEMA, SEABASE_RESOURCES, setClause, whereClause);
  assert(stmtSize < 500);

  ExeCliInterface *cliInterface = getCli();
  assert(cliInterface);
  Int32 cliRC = cliInterface->executeImmediate(updateStmt);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  return 0;
}

// -----------------------------------------------------------------------------
// method: updateRedefTime
//
// Updates REDEF_TIME in RESOURCE GROUPS table
//
// returns:
//    -1 - unexpected error (ComDiags is setup)
//     0 - successful
// -----------------------------------------------------------------------------
short TenantResource::updateRedefTime() {
  char updateStmt[128];
  NAString sysCat = CmpSeabaseDDL::getSystemCatalogStatic();
  Int64 time = NA_JulianTimestamp();
  Int32 stmtSize = snprintf(updateStmt, sizeof(updateStmt),
                            "update %s.\"%s\".%s SET resource_redef_time = %ld where resource_uid = %ld", sysCat.data(),
                            SEABASE_TENANT_SCHEMA, SEABASE_RESOURCES, time, UID_);
  assert(stmtSize < 128);

  ExeCliInterface *cliInterface = getCli();
  assert(cliInterface);
  Int32 cliRC = cliInterface->executeImmediate(updateStmt);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  return 0;
}

// ****************************************************************************
// Non-inline methods for class:  TenantResourceUsage
//
// This class defines a resource group usage
// ****************************************************************************

// -----------------------------------------------------------------------------
// static method: getUsageTypeAsEnum
//
// Translate the string usageType into an enum
// No errors are generated
// -----------------------------------------------------------------------------
TenantResourceUsage::ResourceUsageType TenantResourceUsage::getUsageTypeAsEnum(NAString &usageType) {
  if (usageType == "N ") return TenantResourceUsage::RESOURCE_USAGE_NODE;
  if (usageType == "T ") return TenantResourceUsage::RESOURCE_USAGE_TENANT;
  return TenantResourceUsage::RESOURCE_USAGE_UNKNOWN;
}

// -----------------------------------------------------------------------------
// static method: getUsageTypeAsString
//
// Translate the enum usageType into a string
// No errors are generated
// -----------------------------------------------------------------------------
NAString TenantResourceUsage::getUsageTypeAsString(ResourceUsageType usageType) {
  NAString usageString;
  switch (usageType) {
    case RESOURCE_USAGE_NODE:
      usageString = "N ";
      break;
    case RESOURCE_USAGE_TENANT:
      usageString = "T ";
      break;
    case RESOURCE_USAGE_UNKNOWN:
    default:
      usageString = " ";
      break;
  }
  return usageString;
}

// ****************************************************************************
// Non-inline methods for class:  TenantResourceUsageList
//
// This class defines a list of resource group usage's
// ****************************************************************************

// -----------------------------------------------------------------------------
// method: TenantResourceUsageList destructor
// -----------------------------------------------------------------------------
TenantResourceUsageList::~TenantResourceUsageList() {
  for (CollIndex i = 0; i < entries(); i++) NADELETE(operator[](i), TenantResourceUsage, getHeap());
  clear();
}

// -----------------------------------------------------------------------------
// method:  deleteUsages
//
// delete usages from RESOURCE_USAGE table based on the passed in
// where clause.
//
// returns the number of rows deleted
//   if -1 is returned, then unexpected error occurred (ComDiags contains error)
// -----------------------------------------------------------------------------
Int64 TenantResourceUsageList::deleteUsages(const NAString &whereClause) {
  NAString sysCat = CmpSeabaseDDL::getSystemCatalogStatic();
  Int32 deleteStmtLen = whereClause.length() + 200;
  char deleteStmt[deleteStmtLen];
  Int32 stmtSize = snprintf(deleteStmt, deleteStmtLen, " delete from %s.\"%s\".%s %s ", sysCat.data(),
                            SEABASE_TENANT_SCHEMA, SEABASE_RESOURCE_USAGE, whereClause.data());
  assert(stmtSize < deleteStmtLen);

  ExeCliInterface *cliInterface = getCli();
  assert(cliInterface);
  Int64 rowsAffected = 0;
  Int32 cliRC = cliInterface->executeImmediate(deleteStmt, NULL, NULL, TRUE, &rowsAffected);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  return rowsAffected;
}

// -----------------------------------------------------------------------------
// method: fetchUsages
//
// Selects the list of usages from RESOURCE_USAGE table based on the
// passed in whereClause and ordered by the passed in orderByClause
//
// Stores the usages in class
//
// Returns:
//   -1 - unexpected error (ComDiags has error)
//    0 - successful
// -----------------------------------------------------------------------------
short TenantResourceUsageList::fetchUsages(const NAString &whereClause, const NAString &orderByClause) {
  NAString sysCat = CmpSeabaseDDL::getSystemCatalogStatic();

  // Find usages for tenant identified by tenantID
  char selectStmt[500];
  Int32 stmtSize =
      snprintf(selectStmt, sizeof(selectStmt),
               "select resource_uid, resource_name, usage_uid, "
               " usage_name, usage_type, usage_value, flags "
               "from %s.\"%s\".%s %s %s",
               sysCat.data(), SEABASE_TENANT_SCHEMA, SEABASE_RESOURCE_USAGE, whereClause.data(), orderByClause.data());
  assert(stmtSize < 500);

  Int32 diagsMark = CmpCommon::diags()->mark();
  ExeCliInterface *cliInterface = getCli();
  assert(cliInterface);
  Queue *tableQueue = NULL;
  Int32 cliRC = cliInterface->fetchAllRows(tableQueue, selectStmt, 0, false, false, true);

  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  // If no rows, just return
  if (cliRC == 100)  // did not find the row
  {
    CmpCommon::diags()->rewind(diagsMark);
    return 0;
  }

  tableQueue->position();
  for (Int32 i = 0; i < tableQueue->numEntries(); i++) {
    OutputInfo *pCliRow = (OutputInfo *)tableQueue->getNext();
    TenantResourceUsage *usage = new (getHeap()) TenantResourceUsage;

    // column 0:  resource_uid column 1:  resource_name
    // column 2:  usage_uid    column 3:  usage_name
    // column 4:  usage_type   column 5:  usage_value
    //  column 6:  flags
    usage->setResourceUID(*(Int64 *)pCliRow->get(0));
    NAString resourceName = (char *)pCliRow->get(1);
    usage->setResourceName(resourceName);
    usage->setUsageUID(*(Int64 *)pCliRow->get(2));
    NAString usageName = (char *)pCliRow->get(3);
    usage->setUsageName(usageName);
    NAString usageType = (char *)pCliRow->get(4);
    usage->setUsageType(TenantResourceUsage::getUsageTypeAsEnum(usageType));
    usage->setUsageValue(*(Int64 *)pCliRow->get(5));
    usage->setFlags(*(Int64 *)pCliRow->get(6));

    // add to list
    this->insert(usage);
  }
  return 0;
}

// -----------------------------------------------------------------------------
// method:  findResource
//
// Searches the list of usages to see if passed in resource name exists,
// returns a pointer to the usage associated with the resource.
// A resource can be a Node or Resource Group
//
// Returns NULL if node name is not in the list.
// No errors are expected
// -----------------------------------------------------------------------------
TenantResourceUsage *TenantResourceUsageList::findResource(const NAString *resourceName, const Int64 usageUID,
                                                           const TenantResourceUsage::ResourceUsageType &resourceType) {
  for (CollIndex i = 0; i < entries(); i++) {
    TenantResourceUsage *usage = operator[](i);
    if ((usage->getResourceName() == *resourceName) && (usage->getUsageUID() == usageUID) &&
        (usage->getUsageType() == resourceType))
      return usage;
  }
  return NULL;
}

// -----------------------------------------------------------------------------
// method:  findUsage
//
// Searches the list of usages to see if passed in usage name exists,
// returns a pointer to the usage associated with the resource.
// A resource can be a Node or Resource Group
//
// Returns NULL if node name is not in the list.
// No errors are expected
// -----------------------------------------------------------------------------
TenantResourceUsage *TenantResourceUsageList::findUsage(const NAString *usageName,
                                                        const TenantResourceUsage::ResourceUsageType &usageType) {
  for (CollIndex i = 0; i < entries(); i++) {
    TenantResourceUsage *usage = operator[](i);
    if ((usage->getUsageName() == *usageName) && (usage->getUsageType() == usageType)) return usage;
  }
  return NULL;
}

// -----------------------------------------------------------------------------
// method: getNumNodeUsages
//
// returns the number of node usages in the usage list
// -----------------------------------------------------------------------------
Int32 TenantResourceUsageList::getNumNodeUsages() {
  Int32 numUsages = 0;
  for (CollIndex i = 0; i < entries(); i++) {
    if (operator[](i)->isNodeUsage()) numUsages++;
  }
  return numUsages;
}

// -----------------------------------------------------------------------------
// method: getNumTenantUsages
//
// returns the number of node usages in the usage list
// -----------------------------------------------------------------------------
Int32 TenantResourceUsageList::getNumTenantUsages() {
  Int32 numUsages = 0;
  for (CollIndex i = 0; i < entries(); i++) {
    if (operator[](i)->isTenantUsage()) numUsages++;
  }
  return numUsages;
}

// -----------------------------------------------------------------------------
// method:  modifyUsages
//
// Either inserts a new node-tenant usage or modifies an existing node_tenant
// usage based on the "isNew_" flag
// -----------------------------------------------------------------------------
short TenantResourceUsageList::modifyUsages() {
  short retcode = 0;
  for (Int32 i = 0; i < entries(); i++) {
    TenantResourceUsage *usage = operator[](i);
    if (usage->isNew()) {
      retcode = usage->insertRow(getCli());
      usage->setIsNew(false);
    }
    if (usage->isObsolete())
      retcode = usage->deleteRow(getCli());
    else
      retcode = usage->updateRow(getCli());
    if (retcode == -1) return -1;
  }
  return 0;
}

Int32 TenantResourceUsage::deleteRow(ExeCliInterface *cliInterface) {
  NAString sysCat = CmpSeabaseDDL::getSystemCatalogStatic();
  char deleteStmt[500];
  Int32 stmtSize = snprintf(deleteStmt, sizeof(deleteStmt),
                            "delete from %s.\"%s\".%s where resource_uid = %ld "
                            "and usage_uid = %ld",
                            sysCat.data(), SEABASE_TENANT_SCHEMA, SEABASE_RESOURCE_USAGE, resourceUID_, usageUID_);
  if (stmtSize >= 500) {
    SEABASEDDL_INTERNAL_ERROR("TenantResourceUsage::deleteRow failed, internal buffer size too small");
    return -1;
  }

  if (cliInterface->executeImmediate(deleteStmt) < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  return 0;
}

Int32 TenantResourceUsage::insertRow(ExeCliInterface *cliInterface) {
  NAString sysCat = CmpSeabaseDDL::getSystemCatalogStatic();
  char insertStmt[500];
  Int32 stmtSize =
      snprintf(insertStmt, sizeof(insertStmt),
               "insert into %s.\"%s\".%s values"
               "(%ld, '%s', %ld, '%s', '%s', %ld, %ld)",
               sysCat.data(), SEABASE_TENANT_SCHEMA, SEABASE_RESOURCE_USAGE, resourceUID_, resourceName_.data(),
               usageUID_, usageName_.data(), getUsageTypeAsString(usageType_).data(), usageValue_, flags_);
  if (stmtSize >= 500) {
    SEABASEDDL_INTERNAL_ERROR("TenantResourceUsage::insertRow failed, internal buffer size too small");
    return -1;
  }

  if (cliInterface->executeImmediate(insertStmt) < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  return 0;
}

Int32 TenantResourceUsage::updateRow(ExeCliInterface *cliInterface) {
  NAString sysCat = CmpSeabaseDDL::getSystemCatalogStatic();
  char updateStmt[500];
  Int32 stmtSize = snprintf(updateStmt, sizeof(updateStmt),
                            "update %s.\"%s\".%s set usage_value =  %ld, flags "
                            "= %ld where resource_uid = %ld and usage_uid = %ld",
                            sysCat.data(), SEABASE_TENANT_SCHEMA, SEABASE_RESOURCE_USAGE, usageValue_, flags_,
                            resourceUID_, usageUID_);
  if (stmtSize >= 500) {
    SEABASEDDL_INTERNAL_ERROR("TenantResourceUsage::updateRow failed, internal buffer size too small");
    return -1;
  }

  if (cliInterface->executeImmediate(updateStmt) < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  return 0;
}

// -----------------------------------------------------------------------------
// method: insertUsages
//
// Inserts rows into RESOURCE_USAGE table based on the current list
//
// This list of rows to insert may be large.  Therefore, it is split into
// multiple insert statements based on the statement size.
//
// Returns:
//   -1 - unexpected error (ComDiags has error)
//    0 - successful
// -----------------------------------------------------------------------------
short TenantResourceUsageList::insertUsages() {
  Int32 valuesClauseSize = 1000;
  Int32 maxStmtSize = 1500;
  char stmt[maxStmtSize];
  Int32 stmtSize = 0;

  ExeCliInterface *cliInterface = getCli();
  assert(cliInterface);
  Int32 cliRC = 0;

  std::string valuesClause;
  bool addComma = false;
  NAString sysCat = CmpSeabaseDDL::getSystemCatalogStatic();
  for (Int32 i = 0; i < entries(); i++) {
    TenantResourceUsage *usage = operator[](i);

    NAString nodeName = usage->getUsageName();
    size_t pos = nodeName.first('.');
    if (pos != NA_NPOS) nodeName.remove(pos);

    // Generate the row to insert
    NAString usageType = usage->getUsageTypeAsString(usage->getUsageType());
    stmtSize = snprintf(stmt, sizeof(stmt), "(%ld, '%s', %ld, '%s', '%s', %ld, %ld)", usage->getResourceUID(),
                        usage->getResourceName().data(), usage->getUsageUID(), nodeName.data(), usageType.data(),
                        usage->getUsageValue(), usage->getFlags());
    assert(stmtSize < valuesClauseSize);

    NAString currentValue(stmt);

    // If too many rows to insert in a single statement, insert part of the rows
    // TDB - use row set insert?
    if ((valuesClause.length() + stmtSize + 10) > valuesClauseSize) {
      stmtSize = snprintf(stmt, sizeof(stmt), "insert into %s.\"%s\".%s values %s", sysCat.data(),
                          SEABASE_TENANT_SCHEMA, SEABASE_RESOURCE_USAGE, valuesClause.c_str());
      assert(stmtSize < maxStmtSize);

      if (cliInterface->executeImmediate(stmt) < 0) {
        cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
        return -1;
      }
      valuesClause.clear();
      addComma = false;
    }

    if (addComma)
      valuesClause += ", ";
    else
      addComma = true;
    valuesClause += currentValue;
  }

  // Insert last set of rows
  stmtSize = snprintf(stmt, sizeof(stmt), "insert into %s.\"%s\".%s values %s", sysCat.data(), SEABASE_TENANT_SCHEMA,
                      SEABASE_RESOURCE_USAGE, valuesClause.c_str());
  assert(stmtSize < maxStmtSize);
  if (cliInterface->executeImmediate(stmt) < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  return 0;
}

// -----------------------------------------------------------------------------
// method: updateUsageValue
//
// Updates the column usage_value in the resource_usage table to the specified
// value.
// -----------------------------------------------------------------------------
void TenantResourceUsageList::updateUsageValue(const NAString &resourceName, const NAString &usageName,
                                               TenantResourceUsage::ResourceUsageType usageType, Int64 newValue) {
  for (CollIndex i = 0; i < entries(); i++) {
    TenantResourceUsage *usage = operator[](i);
    if ((usage->getUsageType() == usageType) && (usage->getResourceName() == resourceName) &&
        (usage->getUsageName() == usageName)) {
      usage->setUsageValue(newValue);
      break;
    }
  }
}

// ****************************************************************************
// Non-inline methods for class:  TenantNodeInfoList
//
// This class defines a list of nodes that are assigned to a tenant
// ****************************************************************************

// -----------------------------------------------------------------------------
// method:  contains
//
// Searches the list of nodes for a specific node name.  If found, returns true.
// -----------------------------------------------------------------------------
NABoolean TenantNodeInfoList::contains(const NAString &nodeName) {
  for (CollIndex i = 0; i < entries(); i++) {
    TenantNodeInfo *nodeInfo = (*this)[i];
    if (nodeInfo->getNodeName() == nodeName) return TRUE;
  }
  return FALSE;
}

// -----------------------------------------------------------------------------
// method:  getNodeName
//
// returns the node name for the given node ID
// -----------------------------------------------------------------------------
NAString TenantNodeInfoList::getNodeName(const Int64 logicalNodeID) {
  NAString nodeName;
  for (CollIndex i = 0; i < entries(); i++) {
    TenantNodeInfo *nodeInfo = (*this)[i];
    if (nodeInfo->getLogicalNodeID() == logicalNodeID) return nodeInfo->getNodeName();
  }
  return nodeName;
}

// -----------------------------------------------------------------------------
// method:: getNodeID
//
// Return the nodeID for the given node name.
// If not found, return -1
// -----------------------------------------------------------------------------
Int64 TenantNodeInfoList::getNodeID(const NAString &nodeName) {
  Int64 nodeID = -1;
  for (CollIndex i = 0; i < entries(); i++) {
    TenantNodeInfo *nodeInfo = (*this)[i];
    if (nodeInfo->getNodeName() == nodeName) return nodeInfo->getLogicalNodeID();
  }
  return nodeID;
}

NAString TenantNodeInfoList::listOfNodes() {
  // command searated list of nodes
  NAString msgBuf;
  for (int i = 0; i < entries(); i++) {
    TenantNodeInfo *nodeInfo = (*this)[i];
    if (i > 0) msgBuf += ", ";
    msgBuf += nodeInfo->getNodeName();
  }
  return msgBuf;
}

// -----------------------------------------------------------------------------
// method:  orderedInsert
//
// Add the specified node into the node list of node ID order.
// -----------------------------------------------------------------------------
void TenantNodeInfoList::orderedInsert(TenantNodeInfo *nodeInfo) {
  CollIndex i = 0;
  for (i; i < entries(); i++) {
    TenantNodeInfo *thisNodeInfo = (*this)[i];
    if (thisNodeInfo->getLogicalNodeID() > nodeInfo->getLogicalNodeID()) break;
  }
  TenantNodeInfo *newNodeInfo = new (heap_) TenantNodeInfo(nodeInfo);
  insertAt(i, newNodeInfo);
}

// -----------------------------------------------------------------------------
// method:  removeIfExists
//
// Removes the node from the list based on the node name.
// -----------------------------------------------------------------------------
void TenantNodeInfoList::removeIfExists(const NAString &nodeName) {
  for (CollIndex i = 0; i < entries(); i++) {
    TenantNodeInfo *nodeInfo = (*this)[i];
    if (nodeInfo->getNodeName() == nodeName) {
      remove(nodeInfo);
      break;
    }
  }
}
