//*****************************************************************************
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
//*****************************************************************************

// Needed for parser flag manipulation
#define SQLPARSERGLOBALS_FLAGS
#include "parser/SqlParserGlobalsCmn.h"

#include "sqlcomp/PrivMgr.h"
#include "sqlcomp/PrivMgrCommands.h"
#include "sqlcomp/PrivMgrMDDefs.h"

// c++ includes
#include <string>
#include <algorithm>

// PrivMgr includes
#include "PrivMgrComponents.h"
#include "PrivMgrComponentOperations.h"
#include "sqlcomp/PrivMgrComponentPrivileges.h"
#include "sqlcomp/PrivMgrPrivileges.h"
#include "sqlcomp/PrivMgrSchemaPrivileges.h"
#include "sqlcomp/PrivMgrRoles.h"

// Trafodion includes
#include "common/ComDistribution.h"
#include "cli/sqlcli.h"
#include "executor/ExExeUtilCli.h"
#include "export/ComDiags.h"
#include "comexe/ComQueue.h"
#include "common/CmpCommon.h"
#include "arkcmp/CmpContext.h"
#include "sqlcomp/CmpDDLCatErrorCodes.h"
#include "sqlmxevents/logmxevent_traf.h"
#include "common/ComUser.h"
#include "common/NAUserId.h"
#include "common/str.h"
#include "common/ComSmallDefs.h"

// ==========================================================================
// Contains non inline methods in the following classes
//   PrivMgr
// ==========================================================================

// Specified in expected order of likelihood. See sql/common/ComSmallDefs
// for actual values.
static const literalAndEnumStruct objectTypeConversionTable[] = {
    {COM_BASE_TABLE_OBJECT, COM_BASE_TABLE_OBJECT_LIT},
    {COM_INDEX_OBJECT, COM_INDEX_OBJECT_LIT},
    {COM_VIEW_OBJECT, COM_VIEW_OBJECT_LIT},
    {COM_STORED_PROCEDURE_OBJECT, COM_STORED_PROCEDURE_OBJECT_LIT},
    {COM_USER_DEFINED_ROUTINE_OBJECT, COM_USER_DEFINED_ROUTINE_OBJECT_LIT},
    {COM_UNIQUE_CONSTRAINT_OBJECT, COM_UNIQUE_CONSTRAINT_OBJECT_LIT},
    {COM_NOT_NULL_CONSTRAINT_OBJECT, COM_NOT_NULL_CONSTRAINT_OBJECT_LIT},
    {COM_CHECK_CONSTRAINT_OBJECT, COM_CHECK_CONSTRAINT_OBJECT_LIT},
    {COM_PRIMARY_KEY_CONSTRAINT_OBJECT, COM_PRIMARY_KEY_CONSTRAINT_OBJECT_LIT},
    {COM_REFERENTIAL_CONSTRAINT_OBJECT, COM_REFERENTIAL_CONSTRAINT_OBJECT_LIT},
    {COM_TRIGGER_OBJECT, COM_TRIGGER_OBJECT_LIT},
    {COM_LOCK_OBJECT, COM_LOCK_OBJECT_LIT},
    {COM_LOB_TABLE_OBJECT, COM_LOB_TABLE_OBJECT_LIT},
    {COM_TRIGGER_TABLE_OBJECT, COM_TRIGGER_TABLE_OBJECT_LIT},
    {COM_SYNONYM_OBJECT, COM_SYNONYM_OBJECT_LIT},
    {COM_PRIVATE_SCHEMA_OBJECT, COM_PRIVATE_SCHEMA_OBJECT_LIT},
    {COM_SHARED_SCHEMA_OBJECT, COM_SHARED_SCHEMA_OBJECT_LIT},
    {COM_LIBRARY_OBJECT, COM_LIBRARY_OBJECT_LIT},
    {COM_EXCEPTION_TABLE_OBJECT, COM_EXCEPTION_TABLE_OBJECT_LIT},
    {COM_SEQUENCE_GENERATOR_OBJECT, COM_SEQUENCE_GENERATOR_OBJECT_LIT},
    {COM_PACKAGE_OBJECT, COM_PACKAGE_OBJECT_LIT},
    {COM_UNKNOWN_OBJECT, COM_UNKNOWN_OBJECT_LIT}};

// -----------------------------------------------------------------------
// Default Constructor
// -----------------------------------------------------------------------
PrivMgr::PrivMgr()
    : trafMetadataLocation_("TRAFODION.\"_MD_\""),
      metadataLocation_("TRAFODION.\"_PRIVMGR_MD_\""),
      pDiags_(CmpCommon::diags()),
      authorizationEnabled_(PRIV_INITIALIZED),
      tenantPrivChecks_(true) {
  setFlags();
}

// -----------------------------------------------------------------------
// Construct a PrivMgr object specifying a different metadata location
// -----------------------------------------------------------------------

PrivMgr::PrivMgr(const std::string &metadataLocation, ComDiagsArea *pDiags, PrivMDStatus authorizationEnabled)
    : trafMetadataLocation_("TRAFODION.\"_MD_\""),
      metadataLocation_(metadataLocation),
      pDiags_(pDiags),
      authorizationEnabled_(authorizationEnabled),
      tenantPrivChecks_(true)

{
  if (pDiags == NULL) pDiags = CmpCommon::diags();

  setFlags();
}

PrivMgr::PrivMgr(const std::string &trafMetadataLocation, const std::string &metadataLocation, ComDiagsArea *pDiags,
                 PrivMDStatus authorizationEnabled)
    : trafMetadataLocation_(trafMetadataLocation),
      metadataLocation_(metadataLocation),
      pDiags_(pDiags),
      authorizationEnabled_(authorizationEnabled),
      tenantPrivChecks_(true) {
  if (pDiags == NULL) pDiags = CmpCommon::diags();

  setFlags();
}

// -----------------------------------------------------------------------
// Copy constructor
// -----------------------------------------------------------------------
PrivMgr::PrivMgr(const PrivMgr &other) {
  trafMetadataLocation_ = other.trafMetadataLocation_;
  metadataLocation_ = other.metadataLocation_;
  pDiags_ = other.pDiags_;
  authorizationEnabled_ = other.authorizationEnabled_;
  tenantPrivChecks_ = other.tenantPrivChecks_;
}

// -----------------------------------------------------------------------
// Destructor.
// -----------------------------------------------------------------------

PrivMgr::~PrivMgr() { resetFlags(); }

// ----------------------------------------------------------------------------
// method: getEffectiveGrantor
//
// returns the effective grantor ID and grantor name for grant and revoke
// statements
//
// Input:
//   isGrantedBySpecified - true if grant request included a GRANTED BY clause
//   grantedByName - name specified in GRANTED BY clause
//   owner - owner of schema or object for the grant or revoke subject
//
// Output:
//   effectiveGrantorID - the ID to use for grant and revoke
//   effectiveGrantorName - the name to use for grant and revoke
//
// returns PrivStatus with the results of the operation.  The diags area
// contains error details.
// ----------------------------------------------------------------------------
PrivStatus PrivMgr::getEffectiveGrantor(const bool isGrantedBySpecified, const std::string grantedByName,
                                        const int owner, int &effectiveGrantorID, std::string &effectiveGrantorName) {
  int currentUser = ComUser::getCurrentUser();
  short retcode = 0;

  if (!isGrantedBySpecified) {
    // If the user is DB__ROOT, a grant or revoke operation is implicitly on
    // behalf of the schema owner.  Likewise, if a user has been granted the
    // MANAGE_PRIVILEGES component-level privilege they can grant on
    // behalf of the owner implicitly.  Otherwise, the grantor is the user.
    if (!ComUser::isRootUserID()) {
      PrivMgrComponentPrivileges componentPrivileges(metadataLocation_, pDiags_);

      if (!componentPrivileges.hasSQLPriv(currentUser, SQLOperation::MANAGE_PRIVILEGES, true)) {
        effectiveGrantorName = ComUser::getCurrentUsername();
        effectiveGrantorID = currentUser;
        return STATUS_GOOD;
      }
    }
    // User is DB__ROOT.  Get the effective grantor name.
    char authName[MAX_USERNAME_LEN + 1];
    Int32 actualLen = 0;

    // If owner not found in metadata, ComDiags is populated with
    // error 8732: <owner> is not a registered user or role
    retcode = ComUser::getAuthNameFromAuthID(owner, authName, MAX_USERNAME_LEN, actualLen, FALSE, pDiags_);
    if (retcode != FEOK) return STATUS_ERROR;

    effectiveGrantorID = owner;
    effectiveGrantorName = authName;
    return STATUS_GOOD;
  }

  // GRANTED BY was specified, first see if authorization name is valid.  Then
  // determine if user has authority to use the clause.

  // Get the grantor ID from the grantorName
  retcode = ComUser::getAuthIDFromAuthName(grantedByName.c_str(), effectiveGrantorID, pDiags_);

  // If grantedByName not found in metadata, ComDiags is populated with
  // error 8732: <grantedByName> is not a registered user or role
  if (retcode != 0) return STATUS_ERROR;

  effectiveGrantorName = grantedByName;

  // Name exists, does user have authority?
  // GRANTED BY is allowed if any of the following are true:
  // 1) The user is DB__ROOT.
  // 2) The user is owner of the object.
  // 3) The user has been granted the MANAGE_PRIVILEGES component-level privilege.
  // 4) The grantor is a role and the user has been granted the role.

  if (ComUser::isRootUserID() || currentUser == owner) return STATUS_GOOD;

  PrivMgrComponentPrivileges componentPrivileges(metadataLocation_, pDiags_);

  if (componentPrivileges.hasSQLPriv(currentUser, SQLOperation::MANAGE_PRIVILEGES, true)) return STATUS_GOOD;

  // If the grantor is not a role, user does not have authority.
  if (!isRoleID(effectiveGrantorID)) {
    *pDiags_ << DgSqlCode(-CAT_NOT_AUTHORIZED);
    return STATUS_ERROR;
  }

  // Role specified in BY clause must be granted to the current user for user
  // to have authority.
  if (ComUser::currentUserHasRole(effectiveGrantorID)) return STATUS_GOOD;

  *pDiags_ << DgSqlCode(-CAT_NOT_AUTHORIZED);
  return STATUS_ERROR;
}

// *****************************************************************************
// * Method: getRoleIDsForUserID
// *
// *    Returns the roleIDs for the roles granted to the user.
// *
// *  Parameters:
// *
// *  <userID> is the unique identifier for the user
// *  <roleIDs> passes back the list (potentially empty) of roles granted to the user
// *
// * Returns: PrivStatus
// *
// * STATUS_GOOD: Role list returned
// *           *: Unable to fetch granted roles, see diags.
// *
// *****************************************************************************
PrivStatus PrivMgr::getRoleIDsForUserID(int32_t userID, std::vector<int32_t> &roleIDs)

{
  PrivStatus retcode = STATUS_GOOD;

  PrivMgrRoles roles;
  std::vector<std::string> roleNames;
  std::vector<int32_t> roleDepths;
  std::vector<int32_t> grantees;

  retcode = roles.fetchRolesForAuth(userID, roleNames, roleIDs, roleDepths, grantees);
  return retcode;
}

// *****************************************************************************
// * Method: getGranteeIDsForRoleIDs
// *
// *    Returns the grantees (users and groups) granted to the roles passed in
// *    role list
// *
// *  Parameters:
// *
// *  <roleIDs>    list of roles to check
// *  <granteeIDs> passed back the list (potentially empty) of users granted to
// *               the roleIDs
// *
// * Returns: PrivStatus
// *
// * STATUS_GOOD: Role list returned
// *           *: Unable to fetch granted roles, see diags.
// *
// *****************************************************************************
PrivStatus PrivMgr::getGranteeIDsForRoleIDs(const std::vector<int32_t> &roleIDs, std::vector<int32_t> &granteeIDs,
                                            bool includeSysGrantor) {
  std::vector<int32_t> granteeIDsForRoleIDs;
  PrivMgrRoles roles(" ", metadataLocation_, pDiags_);
  if (roles.fetchGranteesForRoles(roleIDs, granteeIDsForRoleIDs, includeSysGrantor) == STATUS_ERROR)
    return STATUS_ERROR;

  for (size_t i = 0; i < granteeIDsForRoleIDs.size(); i++) {
    int32_t authID = granteeIDsForRoleIDs[i];
    if (std::find(granteeIDs.begin(), granteeIDs.end(), authID) == granteeIDs.end())
      granteeIDs.insert(std::upper_bound(granteeIDs.begin(), granteeIDs.end(), authID), authID);
  }
  return STATUS_GOOD;
}

// ----------------------------------------------------------------------------
// method:  authorizationEnabled
//
// Input:  pointer to the error structure
//
// Returns:
//    PRIV_INITIALIZED means all metadata tables exist
//    PRIV_UNINITIALIZED means no metadata tables exist
//    PRIV_PARTIALLY_INITIALIZED means only part of the metadata tables exist
//    PRIV_INITIALIZE_UNKNOWN means unable to retrieve metadata table info
//
// A cli error is put into the diags area if there is an error
// ----------------------------------------------------------------------------
PrivMgr::PrivMDStatus PrivMgr::authorizationEnabled(std::set<std::string> &existingObjectList) {
  // Will require QI to reset on INITIALIZE AUTHORIZATION [,DROP]
  // get the list of tables from the schema
  // if the catalog name ever allows an embedded '.', this code will need
  // to change.
  std::string metadataLocation = getMetadataLocation();
  size_t period = metadataLocation.find(".");
  std::string catName = metadataLocation.substr(0, period);
  std::string schName = metadataLocation.substr(period + 1);
  char buf[1000];
  sprintf(buf, "get tables in schema %s.%s, no header", catName.c_str(), schName.c_str());

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());
  Queue *schemaQueue = NULL;

  // set pointer in diags area
  int32_t diagsMark = pDiags_->mark();

  int32_t cliRC = cliInterface.fetchAllRows(schemaQueue, buf, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(pDiags_);
    return PRIV_INITIALIZE_UNKNOWN;
  }

  if (cliRC == 100)  // did not find the row
  {
    pDiags_->rewind(diagsMark);
    return PRIV_UNINITIALIZED;
  }

  // Not sure how this can happen but code I cloned had the check
  if (schemaQueue->numEntries() == 0) return PRIV_UNINITIALIZED;

  // Gather the returned list of tables in existingObjectList
  schemaQueue->position();
  for (int idx = 0; idx < schemaQueue->numEntries(); idx++) {
    OutputInfo *row = (OutputInfo *)schemaQueue->getNext();
    std::string theName = row->get(0);
    existingObjectList.insert(theName);
  }

  // Gather the list of expected tables in expectedObjectList
  std::set<string> expectedObjectList;
  size_t numTables = sizeof(privMgrTables) / sizeof(PrivMgrTableStruct);
  for (int ndx_tl = 0; ndx_tl < numTables; ndx_tl++) {
    const PrivMgrTableStruct &tableDefinition = privMgrTables[ndx_tl];
    expectedObjectList.insert(tableDefinition.tableName);
  }

  // Compare the existing with the expected
  std::set<string> diffsObjectList;
  std::set_difference(expectedObjectList.begin(), expectedObjectList.end(), existingObjectList.begin(),
                      existingObjectList.end(), std::inserter(diffsObjectList, diffsObjectList.end()));

  // If the number of existing tables match the expected, diffsObjectList
  // is empty -> return initialized
  if (diffsObjectList.empty()) return PRIV_INITIALIZED;

  // If the number of existing tables does not match the expected,
  // initialization is required -> return not initialized
  if (existingObjectList.size() == diffsObjectList.size()) return PRIV_UNINITIALIZED;

  // Otherwise, mismatch is found, return partially initialized
  return PRIV_PARTIALLY_INITIALIZED;
}

// *****************************************************************************
// * Function:  PrivMgr::getSQLUnusedOpsCount()
// *
// *    Returns the number of unused operations from the hard coded table
// *    in PrivMgrComponentDefs.h for the sql_operations component.
// *
// *****************************************************************************
int32_t PrivMgr::getSQLUnusedOpsCount() {
  int32_t numUnusedOps = 0;
  size_t numOps = sizeof(sqlOpList) / sizeof(ComponentOpStruct);
  for (int i = 0; i < numOps; i++) {
    const ComponentOpStruct &opDefinition = sqlOpList[i];
    if (opDefinition.unusedOp) numUnusedOps++;
  }
  return numUnusedOps;
}

// *****************************************************************************
// *                                                                           *
// * Function: PrivMgr::getSQLOperationCode                                    *
// *                                                                           *
// *    Returns the operation code associated with the specified operation.    *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <operation>                     SQLOperation                    In       *
// *    is the operation.                                                      *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: const char                                                       *
// *                                                                           *
// *  If the operation exists, the corresponding code is returned, otherwise   *
// *  the string "  " is returned.                                             *
// *                                                                           *
// *****************************************************************************
const char *PrivMgr::getSQLOperationCode(SQLOperation operation)

{
  size_t numOps = sizeof(sqlOpList) / sizeof(ComponentOpStruct);
  for (int i = 0; i < numOps; i++) {
    const ComponentOpStruct &opDefinition = sqlOpList[i];
    if ((int32_t)operation == opDefinition.operationID) return opDefinition.operationCode;
  }
  return "  ";
}
//******************** End of PrivMgr::getSQLOperationCode *********************

const char *PrivMgr::getSQLOperationName(SQLOperation operation)

{
  size_t numOps = sizeof(sqlOpList) / sizeof(ComponentOpStruct);
  for (int i = 0; i < numOps; i++) {
    const ComponentOpStruct &opDefinition = sqlOpList[i];
    if ((int32_t)operation == opDefinition.operationID) return opDefinition.operationName;
  }
  return "  ";
}

const char *PrivMgr::getSQLOperationName(std::string operationCode) {
  size_t numOps = sizeof(sqlOpList) / sizeof(ComponentOpStruct);
  for (int i = 0; i < numOps; i++) {
    const ComponentOpStruct &opDefinition = sqlOpList[i];
    if (operationCode == opDefinition.operationCode) return opDefinition.operationName;
  }
  return "  ";
}

// *****************************************************************************
// *                                                                           *
// * Function: PrivMgr::isAuthIDGrantedPrivs                                   *
// *                                                                           *
// *    Determines if the specified authorization ID has been granted one or   *
// * more privileges.                                                          *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <authID>                        const int32_t                   In       *
// *    is the authorization ID.                                               *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: bool                                                             *
// *                                                                           *
// * true: Authorization ID has been granted one or more privileges.           *
// * false: Authorization ID has not been granted any privileges.              *
// *                                                                           *
// *****************************************************************************
bool PrivMgr::isAuthIDGrantedPrivs(const int32_t authID, std::vector<PrivClass> privClasses,
                                   std::vector<int64_t> &objectUIDs, bool &hasComponentPriv) {
  hasComponentPriv = false;
  // Check for empty vector.
  if (privClasses.size() == 0) return false;

  // If authorization is not enabled, no privileges were granted to anyone.
  if (!isAuthorizationEnabled()) return false;

  // Special case of PrivClass::ALL.  Caller does not need to change when
  // new a new PrivClass is added.
  if (privClasses.size() == 1 && privClasses[0] == PrivClass::ALL) {
    PrivMgrPrivileges objectPrivileges(metadataLocation_, pDiags_);

    if (objectPrivileges.isAuthIDGrantedPrivs(authID, objectUIDs)) return true;

    PrivMgrSchemaPrivileges schemaPrivileges(metadataLocation_, pDiags_);

    if (schemaPrivileges.isAuthIDGrantedPrivs(authID, objectUIDs)) return true;

    PrivMgrComponentPrivileges componentPrivileges(metadataLocation_, pDiags_);

    if (componentPrivileges.isAuthIDGrantedPrivs(authID, true)) {
      hasComponentPriv = true;
      return true;
    }

    return false;
  }

  // Called specified one or more specific PrivClass.  Note, ALL is not valid
  // in a list, only by itself.
  for (size_t pc = 0; pc < privClasses.size(); pc++) switch (privClasses[pc]) {
      case PrivClass::OBJECT: {
        PrivMgrPrivileges objectPrivileges(metadataLocation_, pDiags_);

        if (objectPrivileges.isAuthIDGrantedPrivs(authID, objectUIDs)) return true;

        break;
      }
      case PrivClass::SCHEMA: {
        PrivMgrSchemaPrivileges schemaPrivileges(metadataLocation_, pDiags_);

        if (schemaPrivileges.isAuthIDGrantedPrivs(authID, objectUIDs)) return true;

        break;
      }
      case PrivClass::COMPONENT: {
        PrivMgrComponentPrivileges componentPrivileges(metadataLocation_, pDiags_);

        if (componentPrivileges.isAuthIDGrantedPrivs(authID)) {
          hasComponentPriv = true;
          return true;
        }

        break;
      }
      case PrivClass::ALL:
      default: {
        PRIVMGR_INTERNAL_ERROR("Switch statement in PrivMgr::isAuthIDGrantedPrivs()");
        return false;
        break;
      }
    }

  // No grants of any privileges found for this authorization ID.
  return false;
}
//******************* End of PrivMgr::isAuthIDGrantedPrivs *********************

// *****************************************************************************
// *                                                                           *
// * Function: PrivMgr::isSQLAlterOperation                                    *
// *                                                                           *
// *    Determines if a SQL operation is within the subset of alter operations *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <operation>                     SQLOperation                    In       *
// *    is the operation.                                                      *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: bool                                                             *
// *                                                                           *
// * true: operation is an alter operation.                                    *
// * false: operation is not an alter operation.                               *
// *                                                                           *
// *****************************************************************************
bool PrivMgr::isSQLAlterOperation(SQLOperation operation)

{
  if (operation == SQLOperation::ALTER_TABLE || operation == SQLOperation::ALTER_VIEW ||
      operation == SQLOperation::ALTER_SCHEMA || operation == SQLOperation::ALTER_SEQUENCE ||
      operation == SQLOperation::ALTER_TRIGGER || operation == SQLOperation::ALTER_ROUTINE ||
      operation == SQLOperation::ALTER_ROUTINE_ACTION || operation == SQLOperation::ALTER_LIBRARY)
    return true;

  return false;
}
//******************** End of PrivMgr::isSQLAlterOperation *********************

// *****************************************************************************
// *                                                                           *
// * Function: PrivMgr::isSQLCreateOperation                                   *
// *                                                                           *
// *    Determines if a SQL operation is within the subset of create operations*
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <operation>                     SQLOperation                    In       *
// *    is the operation.                                                      *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: bool                                                             *
// *                                                                           *
// * true: operation is a create operation.                                    *
// * false: operation is not a create operation.                               *
// *                                                                           *
// *****************************************************************************
bool PrivMgr::isSQLCreateOperation(SQLOperation operation)

{
  if (operation == SQLOperation::CREATE_TABLE || operation == SQLOperation::CREATE_VIEW ||
      operation == SQLOperation::CREATE_SEQUENCE || operation == SQLOperation::CREATE_TRIGGER ||
      operation == SQLOperation::CREATE_SCHEMA || operation == SQLOperation::CREATE_CATALOG ||
      operation == SQLOperation::CREATE_INDEX || operation == SQLOperation::CREATE_LIBRARY ||
      operation == SQLOperation::CREATE_PROCEDURE || operation == SQLOperation::CREATE_ROUTINE ||
      operation == SQLOperation::CREATE_ROUTINE_ACTION || operation == SQLOperation::CREATE_SYNONYM ||
      operation == SQLOperation::REGISTER_HIVE_OBJECT)
    return true;

  return false;
}
//******************* End of PrivMgr::isSQLCreateOperation *********************

// *****************************************************************************
// *                                                                           *
// * Function: PrivMgr::isSQLDropOperation                                     *
// *                                                                           *
// *    Determines if a SQL operation is within the subset of drop operations. *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <operation>                     SQLOperation                    In       *
// *    is the operation.                                                      *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: bool                                                             *
// *                                                                           *
// * true: operation is a drop operation.                                      *
// * false: operation is not a drop operation.                                 *
// *                                                                           *
// *****************************************************************************
bool PrivMgr::isSQLDropOperation(SQLOperation operation)

{
  if (operation == SQLOperation::DROP_TABLE || operation == SQLOperation::DROP_VIEW ||
      operation == SQLOperation::DROP_SEQUENCE || operation == SQLOperation::DROP_TRIGGER ||
      operation == SQLOperation::DROP_SCHEMA || operation == SQLOperation::DROP_CATALOG ||
      operation == SQLOperation::DROP_INDEX || operation == SQLOperation::DROP_LIBRARY ||
      operation == SQLOperation::DROP_PROCEDURE || operation == SQLOperation::DROP_ROUTINE ||
      operation == SQLOperation::DROP_ROUTINE_ACTION || operation == SQLOperation::DROP_SYNONYM ||
      operation == SQLOperation::UNREGISTER_HIVE_OBJECT)
    return true;

  return false;
}
//******************** End of PrivMgr::isSQLDropOperation **********************

// *****************************************************************************
// *                                                                           *
// * Function: PrivMgr::isSQLManageOperation                                   *
// *                                                                           *
// *    Determines if a SQL operation is within the list of manage operations. *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <operation>                     SQLOperation                    In       *
// *    is the operation.                                                      *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: bool                                                             *
// *                                                                           *
// * true: operation is a manage operation.                                    *
// * false: operation is not a manage operation.                               *
// *                                                                           *
// *****************************************************************************
bool PrivMgr::isSQLManageOperation(SQLOperation operation)

{
  if (operation == SQLOperation::MANAGE || operation == SQLOperation::MANAGE_COMPONENTS ||
      operation == SQLOperation::MANAGE_LIBRARY || operation == SQLOperation::MANAGE_LOAD ||
      operation == SQLOperation::MANAGE_PRIVILEGES || operation == SQLOperation::MANAGE_RESOURCE_GROUPS ||
      operation == SQLOperation::MANAGE_ROLES || operation == SQLOperation::MANAGE_STATISTICS ||
      operation == SQLOperation::MANAGE_TENANTS || operation == SQLOperation::MANAGE_USERS ||
      operation == SQLOperation::MANAGE_GROUPS || operation == SQLOperation::MANAGE_SECURE)
    return true;

  return false;
}
//******************* End of PrivMgr::isSQLManageOperation *********************

// *****************************************************************************
// *                                                                           *
// * Function: PrivMgr::isSQLManageOperation                                   *
// *                                                                           *
// *    Determines if a SQL operation is within the list of manage operations. *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <operation>                     SQLOperation                    In       *
// *    is the operation.                                                      *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: bool                                                             *
// *                                                                           *
// * true: operation is a manage operation.                                    *
// * false: operation is not a manage operation.                               *
// *                                                                           *
// *****************************************************************************
bool PrivMgr::isSQLManageOperation(const char *operationCode)

{
  size_t numOps = sizeof(sqlOpList) / sizeof(ComponentOpStruct);
  for (int i = 0; i < numOps; i++) {
    const ComponentOpStruct &opDefinition = sqlOpList[i];
    if (std::string(opDefinition.operationCode) == std::string(operationCode))
      return (PrivMgr::isSQLManageOperation((SQLOperation)opDefinition.operationID));
  }
  return false;
}
//******************* End of PrivMgr::isSQLManageOperation *********************

// *****************************************************************************
// *                                                                           *
// * Function: PrivMgr::isMetadataObject                                       *
// *                                                                           *
// *    Determines if the internal schema name is a metadata schema            *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: bool                                                             *
// *                                                                           *
// * true: schema is a metadata schema                                         *
// * false: schema is not a metadata schema                                    *
// *                                                                           *
// *****************************************************************************

bool PrivMgr::isMetadataObject(const std::string objectName, std::string &internalSchName) {
  char catName[MAX_SQL_IDENTIFIER_NAME_LEN];
  char schName[MAX_SQL_IDENTIFIER_NAME_LEN];
  char objName[MAX_SQL_IDENTIFIER_NAME_LEN];

  // extract the name parts (from str.h)
  char *src = (char *)objectName.c_str();
  int offset = extractDelimitedName(catName, src);

  // advance to the start of the schema name and get schema name
  src = src + offset + ((src[0] == '\"') ? 2 : 1);
  offset = extractDelimitedName(schName, src);

  // advance to the start of the object name and get object name
  src = src + offset + ((src[0] == '\"') ? 2 : 1);
  offset = extractDelimitedName(objName, src);

  // Code assumes that the catalog name is TRAFODION.  If this changes
  // then an extra check to see if the catalog name is TRAFODION is
  // required.
  //
  // Metadata schemas begin and end with "_"
  int32_t len = strlen(schName);
  internalSchName = schName;

  if ((schName[0] == '_') && (schName[len - 1] == '_')) return true;

  if ((strcmp(objName, HBASE_HIST_NAME) == 0) || (strcmp(objName, HBASE_HISTINT_NAME) == 0) ||
      (strcmp(objName, HBASE_PERS_SAMP_NAME) == 0))
    return true;

  return false;
}

// *****************************************************************************
// *                                                                           *
// * Function: PrivMgr::ObjectEnumToLit                                        *
// *                                                                           *
// *    Returns the two character literal associated with the object type enum.*
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <objectType>                    ComObjectType                   In       *
// *    is the object type enum.                                               *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: const char                                                       *
// *                                                                           *
// *****************************************************************************
const char *PrivMgr::ObjectEnumToLit(ComObjectType objectType)

{
  return comObjectTypeLit(objectType);
}
//********************* End of PrivMgr::ObjectEnumToLit ************************

// *****************************************************************************
// *                                                                           *
// * Function: PrivMgr::ObjectLitToEnum                                        *
// *                                                                           *
// *    Returns the enum associated with the object type literal.              *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <objectType>                    ComObjectType                   In       *
// *    is the object type enum.                                               *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: ComObjectType                                                    *
// *                                                                           *
// *****************************************************************************
ComObjectType PrivMgr::ObjectLitToEnum(const char *objectLiteral)

{
  for (size_t i = 0; i < occurs(objectTypeConversionTable); i++) {
    const literalAndEnumStruct &elem = objectTypeConversionTable[i];
    if (!strncmp(elem.literal_, objectLiteral, 2)) return static_cast<ComObjectType>(elem.enum_);
  }

  return COM_UNKNOWN_OBJECT;
}

//********************* End of PrivMgr::ObjectLitToEnum ************************

// *****************************************************************************
// * Function: isDelimited
// *
// *   This function checks the passed in string for characters other than
// *   alphanumeric and underscore characters.  If so, the name is delimited
// *
// *  Parameters:
// *
// *  <strToScan> string to search for delimited characters
// *
// * Returns: bool
// *
// *  true: the passed in string contains delimited characters
// * false: the passed in string contains no delimited characters
// *****************************************************************************
bool PrivMgr::isDelimited(const std::string &strToScan) {
  char firstChar = strToScan[0];
  if (isdigit(firstChar) || strToScan[0] == '_') return true;
  string validChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_";
  size_t found = strToScan.find_first_not_of(validChars);
  if (found == string::npos) return false;
  return true;
}
//*********************** End of isDelimited ***********************************

// *****************************************************************************
// * Function: buildGrantText
// *
// *    Builds GRANT statement.
// *
// *  Parameters:
// *
// *  <privText>: privileges granted.
// *
// *  <objectGranteeText>: object/schema the privileges are granted on
// *                       and to whom.
// *
// *  <grantorID>: ID of the authID who granted the privilege(s).  If the system
// *               (_SYSTEM) granted the privilege, the command is prefixed
// *               with "--" to prevent execution in a playback script.
// *
// *  <isWGO> if true, add the clause WITH GRANT OPTION to the command.
// *
// *  <grantText> passes back the grant command.
// *****************************************************************************
void PrivMgr::buildGrantText(const std::string &privText, const std::string &objectGranteeText, const int32_t grantorID,
                             const std::string grantorName, bool isWGO, const int32_t objectOwner,
                             std::string &grantText)

{
  if (grantorID == SYSTEM_USER) grantText += "-- ";

  grantText += "GRANT ";
  grantText += privText;

  // remove last ','
  size_t commaPos = grantText.find_last_of(",");

  if (commaPos != std::string::npos) grantText.replace(commaPos, 1, "");

  grantText += objectGranteeText;

  if (isWGO)
    grantText += " WITH GRANT OPTION";
  else

      if (grantorID != objectOwner && grantorID != SYSTEM_USER) {
    grantText += " GRANTED BY ";
    bool delimited = PrivMgr::isDelimited(grantorName);
    if (delimited) grantText += "\"";
    grantText += grantorName;
    if (delimited) grantText += "\"";
  }

  grantText += ";\n";
}

// ****************************************************************************
// * Function: buildPrivText
//*
// * Builds GRANT statements for  SHOWDDL output.
// *
// *   <rowList>: list of rows describing the privileges granted.
// *
// *   <objectInfo>: object details needed to create appropriate text
// *
// *   <privLevel>: privilege level, SCHEMA, OBJECT or COLUMN.
// *
// *   <pDiags_>: is where to report an internal error.
// *
// *   <privilegeText>: returns the set of grant commands describing privileges
// *                    on the object.
// *****************************************************************************
PrivStatus PrivMgr::buildPrivText(const std::vector<PrivMgrMDRow *> rowList, const PrivMgrObjectInfo &objectInfoIn,
                                  PrivLevel privLevel, ComDiagsArea *pDiags_, std::string &privilegeText) {
  PrivMgrObjectInfo objectInfo = (PrivMgrObjectInfo)objectInfoIn;

  // Build a grant statement for each grantor/grantee row returned.
  // TDB: If we support multiple grantees per grant statement,
  //      this code can be improved
  std::string grantStmt;
  std::string grantWGOStmt;
  std::string objectText("ON ");

  // Append object type if not base table or view
  if (objectInfo.getObjectType() != COM_BASE_TABLE_OBJECT && objectInfo.getObjectType() != COM_VIEW_OBJECT)
    objectText += comObjectTypeName(objectInfo.getObjectType());
  if (objectInfo.getObjectType() == COM_SHARED_SCHEMA_OBJECT ||
      objectInfo.getObjectType() == COM_PRIVATE_SCHEMA_OBJECT) {
    // Strip off the schema object name
    std::string nameToStrip(".\"");
    nameToStrip += SEABASE_SCHEMA_OBJECTNAME;
    nameToStrip += "\"";
    std::string adjustedName = objectInfo.getObjectName();
    adjustedName.replace(adjustedName.find(nameToStrip), nameToStrip.length(), "");
    objectText += adjustedName + " TO ";
  } else
    objectText += objectInfo.getObjectName() + " TO ";

  std::string lastGranteeName;
  int32_t lastGranteeID = 0;

  std::vector<std::string> privString;
  std::vector<std::string> WGOString;
  std::vector<bool> hasWGO;
  std::vector<bool> hasPriv;
  std::vector<std::string> columnNames;
  bool mergeStrings = false;

  // Note, this creates entries for DELETE and USAGE that are unused.
  if (privLevel == PrivLevel::COLUMN)
    for (size_t p = FIRST_DML_COL_PRIV; p <= LAST_DML_COL_PRIV; p++) {
      privString.push_back(PrivMgrUserPrivs::convertPrivTypeToLiteral((PrivType)p) + "(");
      WGOString.push_back(privString[p]);
      hasPriv.push_back(false);
      hasWGO.push_back(false);
    }

  for (int32_t i = 0; i < rowList.size(); ++i) {
    std::string objectGranteeText(objectText);

    std::string withoutWGO;
    std::string withWGO;
    int32_t grantorID = 0;
    std::string grantorName;
    PrivMgrRowInfo row = (*rowList[i]).getPrivRowInfo();
    if (privLevel == PrivLevel::SCHEMA || privLevel == PrivLevel::OBJECT) {
      grantorID = row.getGrantorID();
      grantorName = row.getGrantorName();
      PrivObjectBitmap privsBitmap = row.getPrivsBitmap();
      PrivObjectBitmap wgoBitmap = row.getGrantableBitmap();
      bool delimited = PrivMgr::isDelimited(row.getGranteeName());
      if (delimited) objectGranteeText += "\"";
      objectGranteeText += row.getGranteeName();
      if (delimited) objectGranteeText += "\"";

      for (size_t p = FIRST_DML_PRIV; p <= LAST_DML_PRIV; p++)
        if (privsBitmap.test(p)) {
          std::string privTypeString = PrivMgrUserPrivs::convertPrivTypeToLiteral((PrivType)p);
          if (wgoBitmap.test(p))
            withWGO += privTypeString + ", ";
          else
            withoutWGO += privTypeString + ", ";
        }

      for (size_t p = FIRST_DDL_PRIV; p <= LAST_DDL_PRIV; p++)
        if (privsBitmap.test(p)) {
          std::string privTypeString = PrivMgrUserPrivs::convertPrivTypeToLiteral((PrivType)p);
          if (wgoBitmap.test(p))
            withWGO += privTypeString + ", ";
          else
            withoutWGO += privTypeString + ", ";
        }

    }

    else {
      // For column-level privileges we are building a piece of the
      // output for each privilege on every loop.  Privileges are stored
      // per column, but GRANT syntax accepts via a privilege and a
      // list of columns.  For each privilege granted to a grantee, need
      // to list all the columns.  Substrings are merged when the end of the
      // list of grants is reached or there is a new grantor or grantee.
      if (i + 1 == rowList.size())
        mergeStrings = true;
      else {
        PrivMgrRowInfo nextRow = (*rowList[i + 1]).getPrivRowInfo();

        if (nextRow.getGrantorID() != row.getGrantorID() || nextRow.getGranteeID() != row.getGranteeID())
          mergeStrings = true;
      }

      grantorID = row.getGrantorID();
      grantorName = row.getGrantorName();
      PrivColumnBitmap privsBitmap = row.getPrivsBitmap();
      PrivColumnBitmap wgoBitmap = row.getGrantableBitmap();

      // Get name of the grantee.  If we have changed grantees, fetch the
      // name of the grantee.
      if (row.getGranteeID() != lastGranteeID) {
        lastGranteeID = row.getGranteeID();
        lastGranteeName = row.getGranteeName();
      }
      bool delimited = PrivMgr::isDelimited(lastGranteeName);
      if (delimited) objectGranteeText += "\"";
      objectGranteeText += lastGranteeName;
      if (delimited) objectGranteeText += "\"";

      // Get the column name for the row
      const std::vector<std::string> &columnList = objectInfo.getColumnList();
      if (columnList.size() < row.getColumnOrdinal()) {
        std::string errorText("Unable to look up column name for column number ");

        errorText += PrivMgr::authIDToString(row.getColumnOrdinal());
        PRIVMGR_INTERNAL_ERROR(errorText.c_str());
        return STATUS_ERROR;
      }
      std::string columnName(columnList.at(row.getColumnOrdinal()));

      // Build the list of columns granted for each privilege.  WGOString
      // and privString have been pre-populated with PRIVNAME(.
      for (size_t p = FIRST_DML_COL_PRIV; p <= LAST_DML_COL_PRIV; p++)
        if (privsBitmap.test(p)) {
          if (wgoBitmap.test(p)) {
            WGOString[p] += columnName + ", ";
            hasWGO[p] = true;
          } else {
            privString[p] += columnName + ", ";
            hasPriv[p] = true;
          }
        }
      // Check if there are column priv substrings that need to be merged.
      if (mergeStrings) {
        for (size_t p = FIRST_DML_COL_PRIV; p <= LAST_DML_COL_PRIV; p++) {
          if (!isDMLPrivType(static_cast<PrivType>(p))) continue;

          // See if any WGO statement exist
          if (hasWGO[p]) {
            // Replace the trailing comma and space with a parenthesis and trailing comma.
            // Add an additional trailing space for readability.
            std::string columnList = WGOString[p];
            size_t commaPos = columnList.find_last_of(",");
            if (commaPos != std::string::npos) {
              columnList.replace(commaPos, 2, "),");
              columnList += " ";
            }
            withWGO += columnList;
            // Reset to original value
            WGOString[p].assign(PrivMgrUserPrivs::convertPrivTypeToLiteral((PrivType)p) + "(");
            hasWGO[p] = false;
          }

          // See if any WGO statement exist
          if (hasPriv[p]) {
            // Replace the trailing comma and space with a parenthesis and trailing comma.
            // Add an additional trailing space for readability.
            std::string columnList = privString[p];
            size_t commaPos = columnList.find_last_of(",");
            if (commaPos != std::string::npos) {
              columnList.replace(commaPos, 2, "),");
              columnList += " ";
            }
            withoutWGO += columnList;
            // Reset to original value
            privString[p].assign(PrivMgrUserPrivs::convertPrivTypeToLiteral((PrivType)p) + "(");
            hasPriv[p] = false;
          }
        }
        mergeStrings = false;
      }
    }  // End of PrivLevel::COLUMN
    if (!withoutWGO.empty())
      buildGrantText(withoutWGO, objectGranteeText, grantorID, grantorName, false, objectInfo.getObjectOwner(),
                     grantStmt);

    if (!withWGO.empty())
      buildGrantText(withWGO, objectGranteeText, grantorID, grantorName, true, objectInfo.getObjectOwner(),
                     grantWGOStmt);
    privilegeText += grantStmt + grantWGOStmt;
    grantStmt.clear();
    grantWGOStmt.clear();
  }

  return STATUS_GOOD;
}

static void translateObjectName(const std::string inputName, std::string &outputName) {
  char prefix[inputName.length()];
  snprintf(prefix, sizeof(prefix), "%s.\"%s\"", HBASE_SYSTEM_CATALOG, HBASE_EXT_MAP_SCHEMA);
}

// ----------------------------------------------------------------------------
// method: isAuthorizationEnabled
//
// Return true if authorization has been enabled, false otherwise.
//
// ----------------------------------------------------------------------------
bool PrivMgr::isAuthorizationEnabled() {
  // If authorizationEnabled_ not setup in class, go determine status
  std::set<std::string> existingObjectList;
  if (authorizationEnabled_ == PRIV_INITIALIZE_UNKNOWN)
    authorizationEnabled_ = authorizationEnabled(existingObjectList);

  // return true if PRIV_INITIALIZED
  return (authorizationEnabled_ == PRIV_INITIALIZED);
}

// ----------------------------------------------------------------------------
// method: resetFlags
//
// Resets parserflag settings.
//
// At PrivMgr construction time, existing parserflags are saved and additional
// parserflags are turned on.  This is needed so privilege manager
// requests work without requiring special privileges.
//
// The parserflags are restored at class destruction.
//
// Generally, the PrivMgr class is constructed, the operation performed and the
// class destructed.  If some code requires the class to be constructed and
// kept around for awhile, the coder may want reset any parserflags set
// by the constructor between PrivMgr calls. This way code inbetween PrivMgr
// calls won't have any unexpected parser flags set.
//
// If parserflags are reset, then setFlags must be called before the next
// PrivMgr request.
// ----------------------------------------------------------------------------
void PrivMgr::resetFlags() {
  // restore parser flag settings
  // The parserflag requests return a unsigned int return code of 0
  SQL_EXEC_AssignParserFlagsForExSqlComp_Internal(parserFlags_);
}

// ----------------------------------------------------------------------------
// method: setFlags
//
// saves parserflag settings and sets the INTERNAL_QUERY_FROM_EXEUTIL
// parserflag
//
// See comments for PrivMgr::reset for more details
//
// ----------------------------------------------------------------------------
void PrivMgr::setFlags() {
  // set the EXEUTIL parser flag to allow all privmgr internal queries
  // to pass security checks
  // The parserflag requests return a unsigned int return code of 0
  SQL_EXEC_GetParserFlagsForExSqlComp_Internal(parserFlags_);
  SQL_EXEC_SetParserFlagsForExSqlComp_Internal(INTERNAL_QUERY_FROM_EXEUTIL);
}

// ----------------------------------------------------------------------------
// method:  updatePrivBitmap
//
// Update the privileges_bitmap to set a new bit
// The requested bit (from PrivMgrDefs PrivType) is a zero based enum
// The bitextract function uses big-endian where the bits start at 64.
// For example to add ALTER_PRIV:
//    bit 12 in the bitmap
//    bit 11 in the enum definition
//    bit 52 in bitextract
//      64 (number bits) - 11 (from enum) + 1 (adjustment for 0 based enum)
//
// TBD (to make a general purpose method):
//   Add code to update both the privileges_bitmap and the grantable_bitmap
//   Add a whereClause parameter to restrict what gets updated
//   Add a "list" of PrivType if more than 1 privilege is being updated
//   Add option to set (1) or reset (0) privilege bit
// ----------------------------------------------------------------------------
short PrivMgr::updatePrivBitmap(const PrivType privType, int64_t &rowCount) {
  PrivObjectBitmap bitmap;
  bitmap.set(privType);
  int16_t bitExtractLoc = (sizeof(int64_t) * 8) - ((int16_t)privType + 1);

  char buf[200];
  snprintf(buf, sizeof(buf),
           "set privileges_bitmap = privileges_bitmap + %ld,"
           " grantable_bitmap = grantable_bitmap + %ld ",
           bitmap.to_ulong(), bitmap.to_ulong());
  std::string setClause(buf);

  snprintf(buf, sizeof(buf),
           "where grantor_id = %d and "
           "(bitextract(privileges_bitmap, %d, 1) = 0)",
           SYSTEM_USER, bitExtractLoc);
  std::string whereClause(buf);

  rowCount = 0;
  PrivMgrPrivileges privs;
  if (privs.updateObjectPrivRows(setClause, whereClause, rowCount) == STATUS_ERROR) return -1;

  return 0;
}

// ----------------------------------------------------------------------------
// method::log
//
// sends a message to log4cxx implementation designed by SQL
//
// Input:
//    filename - code file that is performing the request
//    message  - the message to log
//    index    - index for logging that loops through a list
//
// Background
//   Privilege manager code sets up a message and calls this log method
//   This method calls SQLMXLoggingArea::logPrivMgrInfo described in
//      sqlmxevents/logmxevent_traf (.h & .cpp)
//   logPrivMgrInfo is a wrapper class around qmscommon/QRLogger (.h & .cpp)
//      log method
//   QRLogger generates a message calls the log method in
//      sqf/commonLogger/CommonLogger (.h & .cpp)
//   CommonLogger interfaces with the log4cxx code which eventually puts
//      a message into a log file called $TRAF_LOG/master_exec_0_pid.log.
//      A new master log is created for each new SQL process started.
//
// Sometimes it is amazing that things actually work with all these levels
// of interfaces.  Perhaps we can skip a few levels...
// ----------------------------------------------------------------------------
void PrivMgr::log(const std::string filename, const std::string message, const int index) {
  std::string logMessage(filename);
  logMessage += ": ";
  logMessage += message;
  if (index >= 0) {
    logMessage += ", index level is ";
    logMessage += to_string((long long int)index);
  }

  SQLMXLoggingArea::logPrivMgrInfo("Privilege Manager", 0, logMessage.c_str(), 0);
}
