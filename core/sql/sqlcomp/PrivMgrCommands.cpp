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

// ==========================================================================
// Contains non inline methods in the following classes
//   PrivMgrObjectInfo
//   PrivMgrCommands
// ==========================================================================

#include "sqlcomp/PrivMgrCommands.h"
#include "sqlcomp/PrivMgrMD.h"
#include "sqlcomp/PrivMgrMDDefs.h"
#include "common/DgBaseType.h"
#include "optimizer/NATable.h"
#include "optimizer/NAColumn.h"

#include "sqlcomp/PrivMgrPrivileges.h"
#include "sqlcomp/PrivMgrSchemaPrivileges.h"

#include "PrivMgrComponents.h"
#include "PrivMgrComponentOperations.h"
#include "sqlcomp/PrivMgrComponentPrivileges.h"
#include "sqlcomp/PrivMgrRoles.h"
#include "sqlcomp/PrivMgrObjects.h"
#include "common/ComSecurityKey.h"
#include "common/ComUser.h"
#include "sqlcomp/CmpSeabaseDDL.h"  // needed in order to access ExpHbaseInterface
#include "exp/ExpHbaseInterface.h"
#include "sqlcomp/CmpDDLCatErrorCodes.h"  // needed for DgSqlCode error msg enum
#include "cli/Globals.h"                  // needed to get access to CLI globals
#include "cli/Context.h"                  // needed to get access to Java exception info
#include "executor/HBaseClient_JNI.h"     // needed for HBase client error msg enum

#include <cstdio>
#include <algorithm>
#include <sstream>
#include <grp.h>
#include <pwd.h>

// ****************************************************************************
// Class: PrivMgrCommands
// ****************************************************************************

// -----------------------------------------------------------------------
// Default Constructor
// -----------------------------------------------------------------------
PrivMgrCommands::PrivMgrCommands() : PrivMgr(){};

// -----------------------------------------------------------------------
// Construct a PrivMgrCommands object for a new component.
// -----------------------------------------------------------------------
PrivMgrCommands::PrivMgrCommands(const std::string trafMetadataLocation, const std::string &metadataLocation,
                                 ComDiagsArea *pDiags, PrivMDStatus authorizationEnabled)
    : PrivMgr(trafMetadataLocation, metadataLocation, pDiags, authorizationEnabled){};

PrivMgrCommands::PrivMgrCommands(const std::string &metadataLocation, ComDiagsArea *pDiags,
                                 PrivMDStatus authorizationEnabled)
    : PrivMgr(metadataLocation, pDiags, authorizationEnabled){};

// -----------------------------------------------------------------------
// Copy constructor
// -----------------------------------------------------------------------
PrivMgrCommands::PrivMgrCommands(const PrivMgrCommands &other) : PrivMgr(other) {}

// -----------------------------------------------------------------------
// Destructor.
// -----------------------------------------------------------------------
PrivMgrCommands::~PrivMgrCommands() {}

// ----------------------------------------------------------------------------
// Assignment operator
// ----------------------------------------------------------------------------
PrivMgrCommands &PrivMgrCommands::operator=(const PrivMgrCommands &other) {
  //  Check for pathological case of X = X.
  if (this == &other) return *this;

  metadataLocation_ = other.metadataLocation_;
  trafMetadataLocation_ = other.trafMetadataLocation_;
  pDiags_ = other.pDiags_;
  parserFlags_ = other.parserFlags_;
  authorizationEnabled_ = other.authorizationEnabled_;
  return *this;
}

// ----------------------------------------------------------------------------
// method: authorizationEnabled
//
// Return true if authorization has been enabled, false otherwise.
//
// ----------------------------------------------------------------------------
bool PrivMgrCommands::authorizationEnabled() {
  if (authorizationEnabled_ == PRIV_INITIALIZED) return true;
  PrivMgrMDAdmin admin(getMetadataLocation(), getDiags());
  return admin.isAuthorizationEnabled();
}

// *****************************************************************************
// *                                                                           *
// * Function: PrivMgrCommands::createComponentOperation                       *
// *                                                                           *
// *    Add an operation for a specified component.                            *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <componentName>                 const std::string &             In       *
// *    is the component name.                                                 *
// *                                                                           *
// *  <operationName>                 const std::string &             In       *
// *    is the operation name to be added.                                     *
// *                                                                           *
// *  <operationCode>                 const std::string &             In       *
// *    is a 2 character code associated with the operation unique to the      *
// *    component.                                                             *
// *                                                                           *
// *  <isSystemOperation>             bool                            In       *
// *    is true if the operation is a system operation.                        *
// *                                                                           *
// *  <operationDescription>          const std::string &             In       *
// *    is a descrption of the operation.  May be empty.                       *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: PrivStatus                                                       *
// *                                                                           *
// * STATUS_GOOD: Operation added.                                             *
// *           *: Operation not added. A CLI error is put into the diags area. *
// *                                                                           *
// *****************************************************************************
PrivStatus PrivMgrCommands::createComponentOperation(const std::string &componentName, const std::string &operationName,
                                                     const std::string &operationCode, bool isSystem,
                                                     const std::string &operationDescription)

{
  PrivStatus privStatus = STATUS_GOOD;

  try {
    PrivMgrComponentOperations componentOperations(getMetadataLocation(), pDiags_);

    privStatus = componentOperations.createOperation(componentName, operationName, operationCode, isSystem,
                                                     operationDescription);
  }

  catch (...) {
    return STATUS_ERROR;
  }

  return privStatus;
}
//************* End of PrivMgrCommands::createComponentOperation ***************

//-----------------------------------------------------------------------------
// method: describeComponents
// returns description of component in the form of REGISTER, CREATE, GRANT statements,
// by a specified component name.
//
// Input:
//     componentName - a unique component name
//
// Output:
//     outlines -output string lines in array
//
// returns true on success.
//-----------------------------------------------------------------------------
bool PrivMgrCommands::describeComponents(const std::string &componentName, std::vector<std::string> &outlines)

{
  PrivStatus privStatus = STATUS_GOOD;
  try {
    std::string componentUIDString;
    int64_t componentUID;
    // generate register component statement
    PrivMgrComponents components(getMetadataLocation(), pDiags_);

    privStatus = components.describeComponents(componentName, componentUIDString, componentUID, outlines);
    // If the component name was not registered
    if (privStatus == STATUS_NOTFOUND) return false;

    // component was registered, try to
    // find it in component_operations, component_privileges tables,
    // and generate create & grant statements.
    PrivMgrComponentPrivileges componentPrivileges(getMetadataLocation(), pDiags_);
    PrivMgrComponentOperations componentOperations(getMetadataLocation(), pDiags_);
    privStatus = componentOperations.describeComponentOperations(componentUIDString, componentName, outlines,
                                                                 &componentPrivileges);
  }

  catch (...) {
    return false;
  }

  return true;
}
//*************** End of PrivMgrCommands::describeComponents *******************

// ----------------------------------------------------------------------------
// method: describePrivileges
//
// returns GRANT statements for privileges associated with the
// specified objectUID
//
// Parameters:
//     objectUID - a unique object identifier
//     objectName - the name of the object
//     privilegeText - the resultant grant text
//
// returns true if successful
// The Trafodion diags area contains any errors that were encountered
// ----------------------------------------------------------------------------
bool PrivMgrCommands::describePrivileges(PrivMgrObjectInfo &objectInfo, std::string &privilegeText) {
  PrivStatus retcode = STATUS_GOOD;
  if (objectInfo.getObjectType() == COM_PRIVATE_SCHEMA_OBJECT ||
      objectInfo.getObjectType() == COM_SHARED_SCHEMA_OBJECT) {
    PrivMgrSchemaPrivileges schemaPrivs(objectInfo, metadataLocation_, pDiags_);
    retcode = schemaPrivs.getPrivTextForSchema(objectInfo, privilegeText);
  } else {
    PrivMgrPrivileges objectPrivs(objectInfo, metadataLocation_, pDiags_);
    retcode = objectPrivs.getPrivTextForObject(objectInfo, privilegeText);
  }
  return (retcode == STATUS_GOOD) ? true : false;
}

// ----------------------------------------------------------------------------
// method: dropAuthorizationMetadata
//
// This method drops all the metadata used by the privilege manager
//
// Returns the status of the request
// The Trafodion diags area contains the error that was encountered
// ----------------------------------------------------------------------------
PrivStatus PrivMgrCommands::dropAuthorizationMetadata(bool doCleanup) {
  PrivMgrMDAdmin metadata(getMetadataLocation(), getDiags());
  std::vector<string> tablesToDrop;
  size_t numTables = sizeof(privMgrTables) / sizeof(PrivMgrTableStruct);
  for (int ndx_tl = 0; ndx_tl < numTables; ndx_tl++) {
    const PrivMgrTableStruct &tableDefinition = privMgrTables[ndx_tl];
    tablesToDrop.push_back(tableDefinition.tableName);
  }
  return metadata.dropMetadata(tablesToDrop, doCleanup);
}

// *****************************************************************************
// *                                                                           *
// * Function: PrivMgrCommands::dropComponentOperation                         *
// *                                                                           *
// *    Removes operation for the specified component from the list of         *
// *  operations for that component.  Granted privileges are automatically     *
// *  removed if the CASCADE option is specified.                              *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <componentName>                 const std::string &             In       *
// *    is the component name.                                                 *
// *                                                                           *
// *  <operationName>                 const std::string &             In       *
// *    is the operation name.                                                 *
// *                                                                           *
// *  <dropBehavior>                  PrivDropBehavior                In       *
// *    indicates whether restrict or cascade behavior is requested.           *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: PrivStatus                                                       *
// *                                                                           *
// * STATUS_GOOD: Operation deleted.                                           *
// *           *: Operation not deleted. A CLI error is put into the           *
// *              diags area.                                                  *
// *                                                                           *
// *****************************************************************************
PrivStatus PrivMgrCommands::dropComponentOperation(const std::string &componentName, const std::string &operationName,
                                                   PrivDropBehavior dropBehavior)

{
  PrivStatus privStatus = STATUS_GOOD;

  try {
    PrivMgrComponentOperations componentOperations(getMetadataLocation(), pDiags_);

    privStatus = componentOperations.dropOperation(componentName, operationName, dropBehavior);
  }

  catch (...) {
    return STATUS_ERROR;
  }

  return privStatus;
}
//************** End of PrivMgrCommands::dropComponentOperation ****************

// ----------------------------------------------------------------------------
// method: getGrantorDetailsForObject
//
// Calls PrivMgr::getEffectiveGrantor to return the effective
// grantor ID and grantor name for object level grant and revoke statements.
//
// Input: see PrivMgr.cpp for description
// Output: see PrivMgr.cpp for description
//
// returns PrivStatus with the results of the operation.  The diags area
// contains error details.
// ----------------------------------------------------------------------------
PrivStatus PrivMgrCommands::getGrantorDetailsForObject(const bool isGrantedBySpecified, const std::string grantedByName,
                                                       const int objectOwner, int &effectiveGrantorID,
                                                       std::string &effectiveGrantorName) {
  PrivMgr grantorDetails(metadataLocation_, pDiags_);
  return grantorDetails.getEffectiveGrantor(isGrantedBySpecified, grantedByName, objectOwner, effectiveGrantorID,
                                            effectiveGrantorName);
}

// ----------------------------------------------------------------------------
// method: getPrivileges
//
// returns privileges (PrivMgrUserPrivs) and the security keys
// (ComSecurityKey) that apply to current user
//
// Input:
//     naTable - a pointer to the object
//     userID - user to gather privs
//     usingSentryUserApi - using Sentry to manage group for a user
//     userPrivileges - returns available privileges
//     secKeySet - the security keys for the object/user
//
// returns true if results were found, false otherwise
// The Trafodion diags area contains any errors that were encountered
// ----------------------------------------------------------------------------
PrivStatus PrivMgrCommands::getPrivileges(NATable *naTable, const int32_t userID, const NABoolean usingSentryUserApi,
                                          PrivMgrUserPrivs &userPrivs, ComSecurityKeySet *secKeySet) {
  PrivMgrDesc privsOfTheUser;
  PrivStatus retcode = STATUS_GOOD;

  // authorization is not enabled, return bitmaps with all bits set
  // With all bits set, privilege checks will always succeed
  if (!authorizationEnabled()) {
    privsOfTheUser.setAllTableGrantPrivileges(true /*priv*/, true /*wgo*/, true /*all_dml*/, true /*all_ddl*/);
    userPrivs.initUserPrivs(privsOfTheUser);
    return STATUS_GOOD;
  }

  // if a native table that is not registered nor has an external table
  // assume no privs.  No privileges, so no security keys are required
  else if ((naTable->isHiveTable() || naTable->isORC() || naTable->isParquet() || naTable->isHbaseCellTable() ||
            naTable->isHbaseRowTable()) &&
           (!naTable->isRegistered() && !naTable->hasExternalTable())) {
    PrivMgrDesc emptyDesc;
    userPrivs.initUserPrivs(emptyDesc);
  }

  // Check for privileges defined in Trafodion metadata
  else {
    int64_t objectUID = (int64_t)naTable->objectUid().get_value();

    PrivMgrDesc schemaPrivsOfTheUser;

    // If we are not storing privileges for the object in NATable, go read MD
    if (naTable->getPrivDescs() == NULL) {
      PrivMgrPrivileges objectPrivs(metadataLocation_, pDiags_);
      retcode = objectPrivs.getPrivsOnObjectForUser(objectUID, naTable->getObjectType(), userID, privsOfTheUser);
      if (retcode != STATUS_GOOD) return retcode;

      userPrivs.initUserPrivs(privsOfTheUser);

      int64_t schemaUID = 0;
      retcode = getPrivsForSchema(objectUID, userID, schemaUID, schemaPrivsOfTheUser);
      if (retcode != STATUS_GOOD) return retcode;

      userPrivs.setSchemaPrivBitmap(schemaPrivsOfTheUser.getSchemaPrivs().getPrivBitmap());
      userPrivs.setSchemaGrantableBitmap(schemaPrivsOfTheUser.getSchemaPrivs().getWgoBitmap());
      if (schemaPrivsOfTheUser.getHasPublicPriv()) userPrivs.setHasPublicPriv(true);

      if (secKeySet != NULL) {
        // The PrivMgrDescList destructor deletes memory
        PrivMgrDescList descList(naTable->getHeap());
        PrivMgrDesc *tableDesc = new (naTable->getHeap()) PrivMgrDesc(privsOfTheUser);
        descList.insert(tableDesc);
        if (!userPrivs.setPrivInfoAndKeys(descList, userID, objectUID, secKeySet)) {
          SEABASEDDL_INTERNAL_ERROR("Could not create security keys");
          return STATUS_ERROR;
        }

        if (schemaUID > 0) {
          PrivMgrDescList schemaDescList(naTable->getHeap());
          PrivMgrDesc *schemaDesc = new (naTable->getHeap()) PrivMgrDesc(schemaPrivsOfTheUser);
          schemaDescList.insert(schemaDesc);
          if (!userPrivs.setPrivInfoAndKeys(schemaDescList, userID, schemaUID, secKeySet)) {
            SEABASEDDL_INTERNAL_ERROR("Could not create security keys");
            return STATUS_ERROR;
          }
        }
      }
    }

    // generate privileges from the stored desc list
    else {
      NAList<int32_t> roleIDs(naTable->getHeap());
      if (ComUser::getCurrentUserRoles(roleIDs, CmpCommon::diags()) != 0) return STATUS_ERROR;

      if (userPrivs.initUserPrivs(roleIDs, naTable->getPrivDescs(), userID, objectUID, secKeySet) == STATUS_ERROR)
        return retcode;
    }
  }

  return STATUS_GOOD;
}

// ----------------------------------------------------------------------------
// method: getPrivileges
//`
// Creates a set of priv descriptors for all user grantees on an object
// Used by Trafodion compiler to store as part of the table descriptor.
//
//  Parameters:
//
//  <objectUID> is the unique identifier of the object
//  <objectType> is the type of object or schema object
//  <privDescs> is the returned list of privileges on the object or schema
//
// Returns: PrivStatus
//
//   STATUS_GOOD: privilege descriptors were built
//             *: unexpected error occurred, see diags.
// ----------------------------------------------------------------------------
PrivStatus PrivMgrCommands::getPrivileges(const int64_t objectUID, const int32_t schemaOwner, const int64_t schemaUID,
                                          ComObjectType schemaObjType, ComObjectType objectType,
                                          PrivMgrDescList &privDescs) {
  PrivStatus retcode = STATUS_GOOD;
  if (authorizationEnabled()) {
    std::vector<PrivMgrDesc> privDescList;

    if (objectType == COM_PRIVATE_SCHEMA_OBJECT || objectType == COM_SHARED_SCHEMA_OBJECT) {
      PrivMgrSchemaPrivileges schemaPrivs(metadataLocation_, pDiags_);
      retcode = schemaPrivs.getPrivsOnSchema(objectUID, schemaOwner, objectType, privDescList);
    } else {
      // get object and column privileges
      PrivMgrPrivileges privInfo(objectUID, metadataLocation_, pDiags_);
      if (privInfo.getPrivsOnObject(objectType, privDescList) == STATUS_ERROR) return STATUS_ERROR;
    }
    if (retcode == STATUS_ERROR) return STATUS_ERROR;

    // copy privDescList to privDescs and set the schema UID
    for (size_t i = 0; i < privDescList.size(); i++) {
      PrivMgrDesc *desc = new (privDescs.getHeap()) PrivMgrDesc(privDescList[i]);
      desc->setSchemaUID(schemaUID);
      privDescs.insert(desc);
    }
  }
  return retcode;
}

// ----------------------------------------------------------------------------
// method: getPrivileges
//
// returns userPrivs for the current user for the specified objectUID
//
// Input:
//     objectUID - a unique object identifier
//     objectType - the type of the object
//     objectName - the name of the object
//     privilegeText - the resultant grant text
//     secKeySet - the security keys for the object/user
//
// returns true if results were found, false otherwise
// The Trafodion diags area contains any errors that were encountered
// ----------------------------------------------------------------------------
PrivStatus PrivMgrCommands::getPrivileges(const int64_t objectUID, ComObjectType objectType, const int32_t userID,
                                          PrivMgrUserPrivs &userPrivs) {
  PrivMgrDesc privsOfTheUser;

  // If authorization is enabled, go get privilege bitmaps from metadata
  if (authorizationEnabled()) {
    PrivMgrPrivileges objectPrivs(trafMetadataLocation_, metadataLocation_, pDiags_);
    PrivStatus retcode = objectPrivs.getPrivsOnObjectForUser(objectUID, objectType, userID, privsOfTheUser);
    if (retcode != STATUS_GOOD) return retcode;

    PrivMgrDesc schemaPrivsOfTheUser;
    int64_t schemaUID;
    retcode = getPrivsForSchema(objectUID, userID, schemaUID, schemaPrivsOfTheUser);
    if (retcode != STATUS_GOOD) return retcode;

    userPrivs.setSchemaPrivBitmap(schemaPrivsOfTheUser.getSchemaPrivs().getPrivBitmap());
    userPrivs.setSchemaGrantableBitmap(schemaPrivsOfTheUser.getSchemaPrivs().getWgoBitmap());
  }

  // authorization is not enabled, return bitmaps with all bits set
  // With all bits set, privilege checks will always succeed
  else
    privsOfTheUser.getTablePrivs().setAllPrivAndWgo(true);

  userPrivs.initUserPrivs(privsOfTheUser);

  return STATUS_GOOD;
}

// *****************************************************************************
// *                                                                           *
// * Function: PrivMgrCommands::getPrivRowsForObject                           *
// *                                                                           *
// *    Returns rows for privileges granted on an object.                      *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <objectUID>                    const int64_t                     In      *
// *    is the unique ID of the object whose grants are being returned.        *
// *                                                                           *
// *  <objectPrivsRows>              std::vector<ObjectPrivsRow> &     In      *
// *    passes back a vector of rows representing the grants for the object.   *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: PrivStatus                                                       *
// *                                                                           *
// * STATUS_GOOD    : Rows were returned                                       *
// * STATUS_NOTFOUND: No rows were returned                                    *
// *               *: Unable to read privileges, see diags.                    *
// *                                                                           *
// *****************************************************************************
PrivStatus PrivMgrCommands::getPrivRowsForObject(const int64_t objectUID, std::vector<ObjectPrivsRow> &objectPrivsRows)

{
  PrivStatus privStatus = STATUS_GOOD;

  try {
    PrivMgrPrivileges objectPrivileges(objectUID, getMetadataLocation(), pDiags_);
    privStatus = objectPrivileges.getPrivRowsForObject(objectUID, objectPrivsRows);
  }

  catch (...) {
    return STATUS_ERROR;
  }

  return privStatus;
}
//*************** End of PrivMgrCommands::getPrivRowsForObject *****************

// *****************************************************************************
// *                                                                           *
// * Function: PrivMgrCommands::getPrivsForSchemas                             *
// *                                                                           *
// * Calculates schema level privileges granted to the user                    *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <objectUID>                     const int64_t                    In      *
// *    the object that requires privileges to be calculated                   *
// *                                                                           *
// *  <userID>                        int32_t                          In      *
// *    is the target userID                                                   *
// *                                                                           *
// *  <schemaUID>                     const int64_t                    Out     *
// *    returns the schemaUID for the associated object                        *
// *                                                                           *
// *  <schemaPrivsOfTheUser>          PrivMgrDesc &                    Out     *
// *    privileges assigned to the user based the schema.                      *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: PrivStatus                                                       *
// *                                                                           *
// * STATUS_GOOD: Privilege bitmap was generated                               *
// *           *: Unexpected error returned reading metadata.                  *
// *              A CLI error is put into the diags area.                      *
// *                                                                           *
// *****************************************************************************
PrivStatus PrivMgrCommands::getPrivsForSchema(const int64_t objectUID, int32_t userID, int64_t &schemaUID,
                                              PrivMgrDesc &schemaPrivsOfTheUser) {
  // Get schema UID and schema type for the object
  char buf[500];
  int stmtLen = snprintf(buf, 500,
                         "WHERE OBJECT_NAME = '__SCHEMA__' "
                         "AND SCHEMA_NAME = (SELECT SCHEMA_NAME FROM "
                         "%s.%s WHERE OBJECT_UID = %ld) "
                         "AND CATALOG_NAME = '%s' "
                         "AND OBJECT_TYPE IN ('PS', 'SS') ",
                         trafMetadataLocation_.c_str(), SEABASE_OBJECTS, objectUID, TRAFODION_SYSCAT_LIT);
  std::string whereClause(buf);

  PrivMgrObjects objects(trafMetadataLocation_, metadataLocation_, pDiags_);
  std::vector<UIDAndType> schemaUIDs;
  PrivStatus privStatus = objects.fetchUIDandTypes(whereClause, schemaUIDs);
  if (schemaUIDs.size() > 0) {
    if (schemaUIDs.size() != 1) {
      SEABASEDDL_INTERNAL_ERROR("Could not retrieve schema privileges for user");
      return STATUS_ERROR;
    }

    // gather privileges for the user
    schemaUID = schemaUIDs[0].UID;
    PrivMgrSchemaPrivileges schemaPrivs(trafMetadataLocation_, metadataLocation_, pDiags_);
    privStatus =
        schemaPrivs.getPrivsOnSchemaForUser(schemaUIDs[0].UID, schemaUIDs[0].objectType, userID, schemaPrivsOfTheUser);
  }
  return privStatus;
}
//**************** End of PrivMgrCommands::getPrivsForSchema *******************

// *****************************************************************************
// *                                                                           *
// * Function: PrivMgrCommands::givePrivForObjects                             *
// *                                                                           *
// *    Gives privileges (grants) associated with one or more objects to       *
// *  a new owner.                                                             *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <currentOwnerID>                const int32_t                    In      *
// *    is the authID of the current owner of the object(s).                   *
// *                                                                           *
// *  <newOwnerID>                    const int32_t                    In      *
// *    is the the authID of the new owner of the object(s).                   *
// *                                                                           *
// *  <newOwnerName>                  const std::string &              In      *
// *    is the name of the new owner of the object(s).                         *
// *                                                                           *
// *  <objectUIDs>                    const std::vector<int64_t> &     In      *
// *    is a list of objects whose privileges are to be given to a new owner.  *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: PrivStatus                                                       *
// *                                                                           *
// * STATUS_GOOD: Component privilege(s) were granted                          *
// *           *: One or more component privileges were not granted.           *
// *              A CLI error is put into the diags area.                      *
// *                                                                           *
// *****************************************************************************
PrivStatus PrivMgrCommands::givePrivForObjects(const int32_t currentOwnerID, const int32_t newOwnerID,
                                               const std::string &newOwnerName, const std::vector<int64_t> &objectUIDs)

{
  PrivStatus privStatus = STATUS_GOOD;

  try {
    PrivMgrPrivileges objectPrivileges(getMetadataLocation(), pDiags_);

    privStatus = objectPrivileges.givePrivForObjects(currentOwnerID, newOwnerID, newOwnerName, objectUIDs);
  }

  catch (...) {
    return STATUS_ERROR;
  }

  return privStatus;
}
//**************** End of PrivMgrCommands::givePrivForObjects ******************

// *****************************************************************************
// *                                                                           *
// * Function: PrivMgrCommands::grantComponentPrivilege                        *
// *                                                                           *
// *    Grants the authority to perform one or more operations on a            *
// *  component to an authID.                                                  *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <componentName>                 const std::string &              In      *
// *    is the component name.                                                 *
// *                                                                           *
// *  <operationNamesList>            const std::vector<std::string> & In      *
// *    is a list of component operations to be granted.                       *
// *                                                                           *
// *  <grantorID>                    const int32_t                     In      *
// *    is the authID granting the privilege.                                  *
// *                                                                           *
// *  <grantorName>                   const std::string &              In      *
// *    is the name of the authID granting the privilege.                      *
// *                                                                           *
// *  <granteeID>                     const int32_t                    In      *
// *    is the the authID being granted the privilege.                         *
// *                                                                           *
// *  <granteeName>                   const std::string &              In      *
// *    is the name of the authID being granted the privilege.                 *
// *                                                                           *
// *  <grantDepth>                    const int32_t                    In      *
// *    is the number of levels this privilege may be granted by the grantee.  *
// *  Initially this is either 0 or -1.                                        *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: PrivStatus                                                       *
// *                                                                           *
// * STATUS_GOOD: Component privilege(s) were granted                          *
// *           *: One or more component privileges were not granted.           *
// *              A CLI error is put into the diags area.                      *
// *                                                                           *
// *****************************************************************************
PrivStatus PrivMgrCommands::grantComponentPrivilege(const std::string &componentName,
                                                    const std::vector<std::string> &operationNamesList,
                                                    const int32_t grantorID, const std::string &grantorName,
                                                    const int32_t granteeID, const std::string &granteeName,
                                                    const int32_t grantDepth)

{
  PrivStatus privStatus = STATUS_GOOD;

  try {
    PrivMgrComponentPrivileges componentPrivileges(getMetadataLocation(), pDiags_);

    privStatus = componentPrivileges.grantPrivilege(componentName, operationNamesList, grantorID, grantorName,
                                                    granteeID, granteeName, grantDepth);
  }

  catch (...) {
    return STATUS_ERROR;
  }

  return privStatus;
}
//************* End of PrivMgrCommands::grantComponentPrivilege ****************

// ----------------------------------------------------------------------------
// method: grantObjectPrivilege
//
// Grants one or more privilege on an object to the grantee (user or TBD role)
//
// Input:
//    objectUID, objectName, objectType - identifies the object
//    granteeUID, granteeName - identifies the grantee
//    grantorUID, grantorName - identifies the grantor
//    privsList - a list of privileges to grant
//    colPrivsArray - an array of column privileges
//    isAllSpecified - grant all privileges for the object type
//    isWGOSpecified - indicates if WITH GRANT OPTION was specified
//
// Returns the status of the request
// The Trafodion diags area contains any errors that were encountered
// ----------------------------------------------------------------------------
PrivStatus PrivMgrCommands::grantObjectPrivilege(const int64_t objectUID, const std::string &objectName,
                                                 const ComObjectType objectType, const int32_t granteeUID,
                                                 const std::string &granteeName, const int32_t grantorUID,
                                                 const std::string &grantorName, const std::vector<PrivType> &privsList,
                                                 const std::vector<ColPrivSpec> &colPrivsArray,
                                                 const bool isAllSpecified, const bool isWGOSpecified) {
  if (!isSecurableObject(objectType)) {
    *pDiags_ << DgSqlCode(-15455) << DgString0("GRANT") << DgString1(objectName.c_str());
    return STATUS_ERROR;
  }

  PrivMgrPrivileges grantCmd(objectUID, objectName, grantorUID, metadataLocation_, pDiags_);
  grantCmd.setTrafMetadataLocation(trafMetadataLocation_);
  grantCmd.setTenantPrivChecks(tenantPrivChecks_);
  return grantCmd.grantObjectPriv(objectType, granteeUID, granteeName, grantorName, privsList, colPrivsArray,
                                  isAllSpecified, isWGOSpecified);
}

PrivStatus PrivMgrCommands::grantObjectPrivilege(const int64_t objectUID, const std::string &objectName,
                                                 const ComObjectType objectType, const int32_t grantorUID,
                                                 const int32_t granteeUID, const PrivMgrBitmap &objectPrivs,
                                                 const PrivMgrBitmap &grantablePrivs) {
  if (!isSecurableObject(objectType)) {
    *pDiags_ << DgSqlCode(-15455) << DgString0("GRANT") << DgString1(objectName.c_str());
    return STATUS_ERROR;
  }

  PrivMgrPrivileges grantCmd(objectUID, objectName, grantorUID, metadataLocation_, pDiags_);
  grantCmd.setTrafMetadataLocation(trafMetadataLocation_);
  grantCmd.setTenantPrivChecks(tenantPrivChecks_);
  return grantCmd.grantObjectPriv(objectType, granteeUID, objectPrivs, grantablePrivs);
}

// *****************************************************************************
// *                                                                           *
// * Function: PrivMgrCommands::grantRole                                      *
// *                                                                           *
// *    Grants a role to an authorization ID.                                  *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <roleIDs>                       const std::vector<int32_t> &     In      *
// *    is a vector of roleIDs.  The caller is responsible for ensuring all    *
// *  IDs are unique.                                                          *
// *                                                                           *
// *  <roleNames>                     const std::vector<std::string> & In      *
// *    is a list of role names.  The elements in <roleIDs> must correspond    *
// *  to the elements in <roleNames>, i.e., roleIDs[n] is the role ID for      *
// *  the role with name roleNames[n].                                         *
// *                                                                           *
// *  <grantorIDs>                    const std::vector<int32_t> &     In      *
// *    is a vector of authIDs granting the role.  The elements of the vector  *
// *  are for the corresponding role entry.                                    *
// *                                                                           *
// *  <grantorNames>                  const std::vector<std::string> & In      *
// *    is a list of names of the authID granting the role in the              *
// *  corresponding position in the role vectors.                              *
// *                                                                           *
// *  <grantorClass                   PrivAuthClass                    In      *
// *    is the auth class of the authID granting the role.  Currently only     *
// *  user is supported.                                                       *
// *                                                                           *
// *  <grantees>                      const std::vector<int32_t> &     In      *
// *    is a vector of authIDs being granted the role.  The caller is          *
// *  responsible for ensuring all IDs are unique.                             *
// *                                                                           *
// *  <granteeNames>                  const std::vector<std::string> & In      *
// *    is a list of grantee names.  The elements in <grantees> must correspond*
// *  to the elements in <granteeNames>, i.e., grantees[n] is the numeric      *
// *  auth ID for authorization ID with the name granteeNames[n].              *
// *                                                                           *
// *  <granteeClasses>                const std::vector<PrivAuthClass> & In    *
// *     is a list of classes for each grantee.  List must correspond to       *
// *  <grantees>.  Currently only user is supported.                           *
// *                                                                           *
// *  <grantDepth>                    const int64_t                    In      *
// *    is the number of levels this role may be granted by the grantee.       *
// *  Initially this is either 0 or -1.                                        *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: PrivStatus                                                       *
// *                                                                           *
// * STATUS_GOOD: Role(s) were granted                                         *
// *           *: One or more roles were not granted.                          *
// *              A CLI error is put into the diags area.                      *
// *                                                                           *
// *****************************************************************************
PrivStatus PrivMgrCommands::grantRole(const std::vector<int32_t> &roleIDs, const std::vector<std::string> &roleNames,
                                      const std::vector<int32_t> &grantorIDs,
                                      const std::vector<std::string> &grantorNames, PrivAuthClass grantorClass,
                                      const std::vector<int32_t> &grantees,
                                      const std::vector<std::string> &granteeNames,
                                      const std::vector<PrivAuthClass> &granteeClasses, const int32_t grantDepth)

{
  PrivStatus privStatus = STATUS_GOOD;

  try {
    PrivMgrRoles roles(trafMetadataLocation_, metadataLocation_, pDiags_);

    privStatus = roles.grantRole(roleIDs, roleNames, grantorIDs, grantorNames, grantorClass, grantees, granteeNames,
                                 granteeClasses, grantDepth);
  }

  catch (...) {
    return STATUS_ERROR;
  }

  return privStatus;
}
//******************** End of PrivMgrCommands::grantRole ***********************

// ----------------------------------------------------------------------------
// method: grantSchemaPrivilege
//
// Grants one or more privilege on a schema to the grantee (user or TBD role)
//
// Input:
//    schemaUID, schemaName, schemaType - identifies the schema
//    granteeUID, granteeName - identifies the grantee
//    grantorUID, grantorName - identifies the grantor
//    schemaOwnerID - identifies the owner of the schema
//    privsList - a list of privileges to grant
//    colPrivsArray - an array of column privileges
//    isAllSpecified - grant all privileges for the object type
//    isWGOSpecified - indicates if WITH GRANT OPTION was specified
//
// Returns the status of the request
// The Trafodion diags area contains any errors that were encountered
// ----------------------------------------------------------------------------
PrivStatus PrivMgrCommands::grantSchemaPrivilege(const int64_t schemaUID, const std::string &schemaName,
                                                 const ComObjectType schemaType, const int32_t grantorID,
                                                 const std::string &grantorName, const int32_t granteeID,
                                                 const std::string &granteeName, const int32_t schemaOwnerID,
                                                 const std::vector<PrivType> &privsList, const bool isAllSpecified,
                                                 const bool isWGOSpecified) {
  if (!isSecurableObject(schemaType)) {
    *pDiags_ << DgSqlCode(-15455) << DgString0("GRANT") << DgString1(schemaName.c_str());
    return STATUS_ERROR;
  }

  PrivMgrSchemaPrivileges grantCmd(schemaUID, schemaName, metadataLocation_, pDiags_);
  grantCmd.setTrafMetadataLocation(trafMetadataLocation_);
  grantCmd.setTenantPrivChecks(tenantPrivChecks_);
  return grantCmd.grantSchemaPriv(schemaType, grantorID, grantorName, granteeID, granteeName, schemaOwnerID, privsList,
                                  isAllSpecified, isWGOSpecified);
}

// ----------------------------------------------------------------------------
// method: initializeAuthorizationMetadata
//
// This method creates all the metadata needed by the privilege manager
//
// Input:
//     Location of the Trafodion OBJECTS table
//     Location of the Trafodion AUTHS table
//
// Returns the status of the request
// The Trafodion diags area contains any errors that were encountered
// ----------------------------------------------------------------------------
PrivStatus PrivMgrCommands::initializeAuthorizationMetadata(std::vector<std::string> tablesCreated, bool initializeMD,
                                                            ExeCliInterface *cliInterface) {
  PrivMgrMDAdmin metadata(getMetadataLocation(), getDiags());
  return metadata.initializeMetadata(tablesCreated, initializeMD, cliInterface);
}

// *****************************************************************************
// *                                                                           *
// * Function: PrivMgrCommands::insertPrivRowsForObject                        *
// *                                                                           *
// *    Writes rows for privileges granted on an object.                       *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <objectUID>                 const int64_t                        In      *
// *    is the unique ID of the object whose grants are being written.         *
// *                                                                           *
// *  <objectPrivsRows>           const std::vector<ObjectPrivsRow> &  In      *
// *    is a vector of rows representing the grants for the object.            *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: PrivStatus                                                       *
// *                                                                           *
// * STATUS_GOOD    : Rows were inserted                                       *
// *               *: Unable to insert, see diags.                             *
// *                                                                           *
// *****************************************************************************
PrivStatus PrivMgrCommands::insertPrivRowsForObject(const int64_t objectUID,
                                                    const std::vector<ObjectPrivsRow> &objectPrivsRows)

{
  PrivStatus privStatus = STATUS_GOOD;

  try {
    PrivMgrPrivileges objectPrivileges(objectUID, getMetadataLocation(), pDiags_);

    privStatus = objectPrivileges.insertPrivRowsForObject(objectUID, objectPrivsRows);
  }

  catch (...) {
    return STATUS_ERROR;
  }

  return privStatus;
}
//************* End of PrivMgrCommands::insertPrivRowsForObject ****************

// ----------------------------------------------------------------------------
// method: isPrivMgrTable
//
// Input:
//   objectName - name of object to check
//
// this method returns true if the passed in object name is a privilege
// manager metadata object, false otherwise.
// ----------------------------------------------------------------------------
bool PrivMgrCommands::isPrivMgrTable(const std::string &objectName) {
  char theQuote = '"';

  // Delimited name issue.  The passed in objectName may enclose name parts in
  // double quotes even if the name part contains only [a-z][A-Z][0-9]_
  // characters. The same is true for the stored metadataLocation_.
  // To allow equality checks to work, we strip off the double quote delimiters
  // from both names. Fortunately, the double quote character is not allowed in
  // any SQL identifier except as delimiters - so this works.
  std::string nameToCheck(objectName);
  nameToCheck.erase(std::remove(nameToCheck.begin(), nameToCheck.end(), theQuote), nameToCheck.end());

  size_t numTables = sizeof(privMgrTables) / sizeof(PrivMgrTableStruct);
  for (int ndx_tl = 0; ndx_tl < numTables; ndx_tl++) {
    const PrivMgrTableStruct &tableDefinition = privMgrTables[ndx_tl];

    std::string mdTable = metadataLocation_;
    mdTable.erase(std::remove(mdTable.begin(), mdTable.end(), theQuote), mdTable.end());

    mdTable += std::string(".") + tableDefinition.tableName;
    if (mdTable == nameToCheck) return true;
  }
  return false;
}

// *****************************************************************************
// *                                                                           *
// * Function: PrivMgrCommands::registerComponent                              *
// *                                                                           *
// *    Registers (adds) a component to the list of components known to        *
// *  Privilege Manager.  Authority to perform operations within the           *
// *  component may be granted to one or more authorization IDs.               *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <componentName>                 const std::string &             In       *
// *    is the name of the component.  Name is assumed to be upper case.       *
// *                                                                           *
// *  <isSystem>                      const bool                      In       *
// *    is true if this is a system level component, false otherwise.  If a    *
// *  component is system level, code in Trafodion can assume is it always     *
// *  defined.                                                                 *
// *                                                                           *
// *  System components may only be unregistered by DB__ROOT.  Only DB__ROOT   *
// *  may register a system component.                                         *
// *                                                                           *
// *  <componentDescription>          const std::string &             In       *
// *    is a description of the component.                                     *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: PrivStatus                                                       *
// *                                                                           *
// * STATUS_GOOD: Component name was registered.                               *
// *           *: Unable to register component name, see diags.                *
// *                                                                           *
// *****************************************************************************
PrivStatus PrivMgrCommands::registerComponent(const std::string &componentName, const bool isSystem,
                                              const std::string &componentDescription)

{
  PrivStatus privStatus = STATUS_GOOD;

  try {
    PrivMgrComponents components(getMetadataLocation(), pDiags_);

    privStatus = components.registerComponent(componentName, isSystem, componentDescription);
  } catch (...) {
    return STATUS_ERROR;
  }

  return privStatus;
}
//**************** End of PrivMgrCommands::registerComponent *******************

// *****************************************************************************
// *                                                                           *
// * Function: PrivMgrCommands::revokeComponentPrivilege                       *
// *                                                                           *
// *    Revokes the authority to perform one or more operations on a           *
// *  component from an authID.                                                *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <componentName>                 const std::string &              In      *
// *    is the component name.                                                 *
// *                                                                           *
// *  <operationNamesList>            const std::vector<std::string> & In      *
// *    is a list of component operations to be revoked.                       *
// *                                                                           *
// *  <grantorID>                     const int32_t                    In      *
// *    is the authID revoking the privilege.                                  *
// *                                                                           *
// *  <granteeName>                   const std::string &              In      *
// *    is the authname the privilege is being revoked from.                   *
// *                                                                           *
// *  <isGOFSpecified>                const bool                       In      *
// *    is true if admin rights are being revoked.                             *
// *                                                                           *
// *  <dropBehavior>                  PrivDropBehavior                 In      *
// *    indicates whether restrict or cascade behavior is requested.           *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: PrivStatus                                                       *
// *                                                                           *
// * STATUS_GOOD: Component privilege(s) were revoked                          *
// *           *: One or more component privileges were not revoked.           *
// *              A CLI error is put into the diags area.                      *
// *                                                                           *
// *****************************************************************************
PrivStatus PrivMgrCommands::revokeComponentPrivilege(const std::string &componentName,
                                                     const std::vector<std::string> &operationNamesList,
                                                     const int32_t grantorID, const std::string &granteeName,
                                                     const bool isGOFSpecified, PrivDropBehavior dropBehavior)

{
  PrivStatus privStatus = STATUS_GOOD;

  try {
    PrivMgrComponentPrivileges componentPrivileges(getMetadataLocation(), pDiags_);
    privStatus = componentPrivileges.revokePrivilege(componentName, operationNamesList, grantorID, granteeName,
                                                     isGOFSpecified, 0, dropBehavior);
  }

  catch (...) {
    return STATUS_ERROR;
  }

  return privStatus;
}
//************* End of PrivMgrCommands::revokeComponentPrivilege ***************

// ----------------------------------------------------------------------------
// method: revokeObjectPrivilege
//
// Revokes one or more privilege on an object to the grantee (user or TBD role)
//
// Input:
//    objectUID, objectName, objectType - identifies the object
//    granteeUID - identifies the grantee
//    grantorUID - identifies the grantor
//    privsList - a list of privileges to revoke
//    isAllSpecified - grant all privileges for the object type
//    isGOFSpecified - indicates if GRANT OPTION FOR  was specified
//
// Returns the status of the request
// The Trafodion diags area contains any errors that were encountered
// ----------------------------------------------------------------------------
PrivStatus PrivMgrCommands::revokeObjectPrivilege(const int64_t objectUID, const std::string &objectName,
                                                  const ComObjectType objectType, const int32_t granteeUID,
                                                  const std::string &granteeName, const int32_t grantorUID,
                                                  const std::string &grantorName, const std::vector<PrivType> &privList,
                                                  const std::vector<ColPrivSpec> &colPrivsArray,
                                                  const bool isAllSpecified, const bool isGOFSpecified) {
  if (!isSecurableObject(objectType)) {
    *pDiags_ << DgSqlCode(-15455) << DgString0("REVOKE") << DgString1(objectName.c_str());
    return STATUS_ERROR;
  }

  // set up privileges class
  PrivMgrPrivileges revokeCmd(objectUID, objectName, grantorUID, metadataLocation_, pDiags_);
  revokeCmd.setTrafMetadataLocation(trafMetadataLocation_);
  revokeCmd.setTenantPrivChecks(tenantPrivChecks_);
  return revokeCmd.revokeObjectPriv(objectType, granteeUID, granteeName, grantorName, privList, colPrivsArray,
                                    isAllSpecified, isGOFSpecified);
}

// ----------------------------------------------------------------------------
// method: revokeObjectPrivilege
//
// Revokes all privileges for an object - called as part of dropping the object
//
// Input:
//    objectUID, objectName - identifies the object
//    grantorUID - identifies the grantor
//
// Returns the status of the request
// The Trafodion diags area contains any errors that were encountered
// ----------------------------------------------------------------------------
PrivStatus PrivMgrCommands::revokeObjectPrivilege(const int64_t objectUID, const std::string &objectName,
                                                  const int32_t grantorUID) {
  // If we are revoking privileges on the authorization tables,
  // just return STATUS_GOOD.  Object_privileges will go away.
  if (isPrivMgrTable(objectName)) return STATUS_GOOD;

  // set up privileges class
  PrivMgrPrivileges revokeCmd(objectUID, objectName, grantorUID, metadataLocation_, pDiags_);
  revokeCmd.setTrafMetadataLocation(trafMetadataLocation_);
  revokeCmd.setTenantPrivChecks(tenantPrivChecks_);
  return revokeCmd.revokeObjectPriv();
}

// *****************************************************************************
// *                                                                           *
// * Function: PrivMgrCommands::revokeRole                                     *
// *                                                                           *
// *    Revokes one or more roles from one or more authorization IDs.          *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <roleIDs>                    const std::vector<int32_t> &        In      *
// *    is a vector of roleIDs.  The caller is responsible for ensuring all    *
// *  IDs are unique.                                                          *
// *                                                                           *
// *  <granteeIDs>                 const std::vector<int32_t> &        In      *
// *    is a vector of granteeIDs.  The caller is responsible for ensuring all *
// *  IDs are unique.                                                          *
// *                                                                           *
// *  <granteeClasses>             const std::vector<PrivAuthClass> &  In      *
// *    is a vector of PrivAuthClass corresponding the granteeID in the same   *
// *  position in the granteeIDs vector.                                       *
// *                                                                           *
// *  <grantorIDs>                    const std::vector<int32_t> &     In      *
// *    is a vector of authIDs that granted the role.  The elements of the     *
// *  vector are for the corresponding role entry.                             *
// *                                                                           *
// *  <isGOFSpecified>             const bool                          In      *
// *    is true if admin rights are being revoked.                             *
// *                                                                           *
// *  <newGrantDepth>              const int32_t                       In      *
// *    is the number of levels this privilege may be granted by the grantee.  *
// *  Initially this is always 0 when revoking.                                *
// *                                                                           *
// *  <dropBehavior>               PrivDropBehavior                    In      *
// *    indicates whether restrict or cascade behavior is requested.           *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: PrivStatus                                                       *
// *                                                                           *
// * STATUS_GOOD: Role(s) were revoked                                         *
// *           *: One or more roles were not revoked.                          *
// *              A CLI error is put into the diags area.                      *
// *                                                                           *
// *****************************************************************************
PrivStatus PrivMgrCommands::revokeRole(const std::vector<int32_t> &roleIDs, const std::vector<int32_t> &granteeIDs,
                                       const std::vector<PrivAuthClass> &granteeClasses,
                                       const std::vector<int32_t> &grantorIDs, const bool isGOFSpecified,
                                       const int32_t newGrantDepth, PrivDropBehavior dropBehavior)

{
  PrivStatus privStatus = STATUS_GOOD;

  try {
    PrivMgrRoles roles(trafMetadataLocation_, metadataLocation_, pDiags_);

    privStatus =
        roles.revokeRole(roleIDs, granteeIDs, granteeClasses, grantorIDs, isGOFSpecified, newGrantDepth, dropBehavior);
  }

  catch (...) {
    return STATUS_ERROR;
  }

  return privStatus;
}
//******************** End of PrivMgrCommands::revokeRole **********************

// ----------------------------------------------------------------------------
// method: revokeSchemaPrivilege
//
// Revokes all privileges from a schema, called when the schema is dropped
//
// Input:
//    schemaUID, schemaName - identifies the oject
//
// Returns the status of the request
// The Trafodion diags area contains any errors that were encountered
// ----------------------------------------------------------------------------
PrivStatus PrivMgrCommands::revokeSchemaPrivilege(const int64_t schemaUID, const std::string &schemaName) {
  // If revoking from PRIVMGR schema, just return
  if (strcmp(schemaName.c_str(), SEABASE_PRIVMGR_SCHEMA) == 0) return STATUS_GOOD;

  PrivMgrSchemaPrivileges revokeCmd(schemaUID, schemaName, metadataLocation_, pDiags_);
  return revokeCmd.revokeSchemaPriv();
}

// ----------------------------------------------------------------------------
// method: revokeSchemaPrivilege
//
// Revokes one or more privilege on a schema to the grantee (user or TBD role)
//
// Input:
//    schemaUID, schemaName, schemaType - identifies the oject
//    granteeUID, granteeName - identifies the grantee
//    grantorUID, grantorName - identifies the grantor
//    schemaOwnerID - identifies the schema owner
//    privsList - a list of privileges to revoke
//    isAllSpecified - grant all privileges for the object type
//    isGOFSpecified - indicates if GRANT OPTION FOR  was specified
//
// Returns the status of the request
// The Trafodion diags area contains any errors that were encountered
// ----------------------------------------------------------------------------
PrivStatus PrivMgrCommands::revokeSchemaPrivilege(const int64_t schemaUID, const std::string &schemaName,
                                                  const ComObjectType schemaType, const int32_t grantorID,
                                                  const std::string &grantorName, const int32_t granteeID,
                                                  const std::string &granteeName, const int32_t schemaOwnerID,
                                                  const std::vector<PrivType> &privList, const bool isAllSpecified,
                                                  const bool isGOFSpecified) {
  if (!isSecurableObject(schemaType)) {
    *pDiags_ << DgSqlCode(-15455) << DgString0("REVOKE") << DgString1(schemaName.c_str());
    return STATUS_ERROR;
  }

  // set up privileges class
  PrivMgrSchemaPrivileges revokeCmd(schemaUID, schemaName, metadataLocation_, pDiags_);
  revokeCmd.setTrafMetadataLocation(trafMetadataLocation_);
  revokeCmd.setTenantPrivChecks(tenantPrivChecks_);
  return revokeCmd.revokeSchemaPriv(schemaType, grantorID, grantorName, granteeID, granteeName, schemaOwnerID, privList,
                                    isAllSpecified, isGOFSpecified);
}

// *****************************************************************************
// *                                                                           *
// * Function: PrivMgrComponents::unregisterComponent                          *
// *                                                                           *
// *    Removes the component from the list of components known to the         *
// *  Privilege Manager.  Operations of the component and privileges           *
// *  granted for those operations are automatically removed if the            *
// *  CASCADE option is specified.                                             *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <componentName>                 const std::string &             In       *
// *    is the name of the component.  Name is assumed to be upper case.       *
// *                                                                           *
// *  <dropBehavior>                  PrivDropBehavior                In       *
// *    indicates whether restrict or cascade behavior is requested.           *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: PrivStatus                                                       *
// *                                                                           *
// * STATUS_GOOD: Component was unregistered.                                  *
// *           *: Unable to unregister component, see diags.                   *
// *                                                                           *
// *****************************************************************************
PrivStatus PrivMgrCommands::unregisterComponent(const std::string &componentName, PrivDropBehavior dropBehavior)

{
  PrivStatus privStatus = STATUS_GOOD;

  try {
    PrivMgrComponents components(getMetadataLocation(), pDiags_);

    privStatus = components.unregisterComponent(componentName, dropBehavior);
  }

  catch (...) {
    return STATUS_ERROR;
  }

  return privStatus;
}
//*************** End of PrivMgrCommands::unregisterComponent ******************

int PrivMgrCommands::recreateRoleRelationship(ExeCliInterface *cliInterface) {
  Queue *cntQueue = NULL;
  char buf[1024 * 5] = {0};
  std::stringstream sb;
  int cliRC = 0;
  NAString sysCat = CmpSeabaseDDL::getSystemCatalogStatic();
  // number roleId in _MD_.auths >= number roleId in _PRIVMGR_MD_.ROLE_USAGE
  str_sprintf(buf,
              "select distinct AUTH_ID,AUTH_DB_NAME from %s.\"%s\".%s where AUTH_ID >= %d and AUTH_ID <= %d except "
              "select distinct ROLE_ID,ROLE_NAME from %s.\"%s\".ROLE_USAGE",
              sysCat.data(), SEABASE_MD_SCHEMA, SEABASE_AUTHS, MIN_ROLEID, MAX_ROLEID, sysCat.data(),
              SEABASE_PRIVMGR_SCHEMA);
  cliRC = cliInterface->fetchAllRows(cntQueue, buf, 0, false, false, true);
  if (cliRC < 0) {
    return cliRC;
  } else if (cliRC == 100 || cntQueue->numEntries() == 0) {
    // all ready!
    return 0;
  }
  cntQueue->position();
  int count = 0;
  for (; count < cntQueue->numEntries(); count++) {
    OutputInfo *pCliRow = (OutputInfo *)cntQueue->getNext();
    sb << '(' << *((int *)pCliRow->get(0))              // ROLE_ID
       << ',' << '\'' << (char *)pCliRow->get(1) << '\''  // ROLE_NAME
       << ',' << SUPER_USER_LIT                           // GRANTEE_ID
       << ',' << '\'' << DB__ROOT << '\''                 // GRANTEE_NAME
       << ',' << '\'' << COM_USER_COLUMN_LIT << '\''      // GRANTEE_AUTH_CLASS
       << ',' << SYSTEM_USER                              // GRANTOR_ID
       << ',' << '\'' << SYSTEM_AUTH_NAME << '\''         // GRANTOR_NAME
       << ',' << '\'' << COM_USER_COLUMN_LIT << '\''      // GRANTOR_AUTH_CLASS
       << ',' << "-1"                                     // GRANT_DEPTH
       << "),";                                           // add ','
  }
  sb << std::ends;
  //"regrant" role with admin option to db__root
  // don't use std::stringstream::str().length()
  if (count) {
    // not str_sprintf
    bzero(buf, sizeof(buf));
    std::string str = sb.str();
    // delete last of ,
    // not use str.erase(str.end() - 1);!
    str.erase(str.find_last_of(","), 1);
    sb.str("");
    snprintf(buf, sizeof(buf) - 1, "upsert into %s.\"%s\".ROLE_USAGE values %s", sysCat.data(), SEABASE_PRIVMGR_SCHEMA,
             str.c_str());

    cliRC = cliInterface->executeImmediate(buf);
  }
  // cliRC = 100 is ok
  return cliRC > 0 ? 0 : cliRC;
}
