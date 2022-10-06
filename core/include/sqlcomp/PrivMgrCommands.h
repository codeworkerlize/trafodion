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
//// @@@ END COPYRIGHT @@@
//*****************************************************************************

#ifndef PRIVMGR_COMMANDS_H
#define PRIVMGR_COMMANDS_H

#include <string>
#include <vector>
#include <bitset>
#include <iterator>
#include <sys/time.h>
#include "sqlcomp/PrivMgrMD.h"
#include "sqlcomp/PrivMgrDefs.h"
#include "sqlcat/TrafDDLdesc.h"
#include "common/ComSecurityKey.h"
#include "cli/SQLCLIdev.h"
#include "PrivMgrUserPrivs.h"

class ComDiagsArea;
class ComSecurityKey;
struct TrafDesc;

// *****************************************************************************
// This file contains classes used by callers of privilege manager
//
//  Privilege manager (PrivMgr) is a component that manages metadata associated
//  with privileges.  This includes granting/revoking object and component (and
//  eventually column) privileges, returning privilege descriptions in the form
//  of grant statements for SHOWDDL, returning privileges on objects for
//  specific users, and commands to support components
// *****************************************************************************

// Contents of file
class PrivMgrCommands;

// Forward references
class PrivMgrPrivileges;
class NATable;

// *****************************************************************************
// *
// * Class:        PrivMgrCommands
// * Description:  This class represents the commands that can be performed
// *               through the privilege manager component
// *
// *****************************************************************************

class PrivMgrCommands : public PrivMgr {
 public:
  // ---------------------------------------------------------------------
  // Constructors/Destructor
  // ---------------------------------------------------------------------
  PrivMgrCommands();
  PrivMgrCommands(const std::string trafMetadataLocation, const std::string &metadataLocation, ComDiagsArea *pDiags,
                  PrivMDStatus authorizationEnabled = PRIV_INITIALIZE_UNKNOWN);
  PrivMgrCommands(const std::string &metadataLocation, ComDiagsArea *pDiags,
                  PrivMDStatus authorizationEnabled = PRIV_INITIALIZE_UNKNOWN);
  PrivMgrCommands(const PrivMgrCommands &rhs);
  virtual ~PrivMgrCommands(void);

  // ------------------------------------------------------------------------
  // Operations:
  // ------------------------------------------------------------------------
  bool authorizationEnabled();

  PrivStatus createComponentOperation(const std::string &componentName, const std::string &operationName,
                                      const std::string &operationCode, bool isSystem,
                                      const std::string &operationDescription);

  bool describeComponents(const std::string &componentName, std::vector<std::string> &outlines);

  bool describePrivileges(PrivMgrObjectInfo &objectInfo, std::string &privilegeText);

  PrivStatus dropAuthorizationMetadata(bool doCleanup);

  PrivStatus dropComponentOperation(const std::string &componentName, const std::string &operationName,
                                    PrivDropBehavior dropBehavior);

  PrivStatus getGrantorDetailsForObject(const bool isGrantedBySpecified, const std::string grantedByName,
                                        const int objectOwner, int &effectiveGrantorID,
                                        std::string &effectiveGrantorName);

  PrivStatus getPrivileges(NATable *naTable, const int32_t granteeUID, const NABoolean usingSentryUserApi,
                           PrivMgrUserPrivs &userPrivileges, NASet<ComSecurityKey> *secKeySet = NULL);

  PrivStatus getPrivileges(const int64_t objectUID, const int32_t schemaOwner, const int64_t schemaUID,
                           ComObjectType schemaObjectType, ComObjectType objectType, PrivMgrDescList &userPrivileges);

  PrivStatus getPrivileges(const int64_t objectUID, ComObjectType objectType, const int32_t granteeUID,
                           PrivMgrUserPrivs &userPrivileges);

  PrivStatus getPrivRowsForObject(const int64_t objectUID, std::vector<ObjectPrivsRow> &objectPrivsRows);

  PrivStatus givePrivForObjects(const int32_t currentOwnerID, const int32_t newOwnerID, const std::string &newOwnerName,
                                const std::vector<int64_t> &objectUIDs);

  PrivStatus grantComponentPrivilege(const std::string &componentName,
                                     const std::vector<std::string> &operationNamesList, const int32_t grantorID,
                                     const std::string &grantorName, const int32_t granteeID,
                                     const std::string &granteeName, const int32_t grantDepth);

  PrivStatus grantObjectPrivilege(const int64_t objectUID, const std::string &objectName,
                                  const ComObjectType objectType, const int32_t granteeUID,
                                  const std::string &granteeName, const int32_t grantorUID,
                                  const std::string &grantorName, const std::vector<PrivType> &privs,
                                  const std::vector<ColPrivSpec> &colPrivsArray, const bool isAllSpecified,
                                  const bool isWGOSpecified);

  PrivStatus grantObjectPrivilege(const int64_t objectUID, const std::string &objectName,
                                  const ComObjectType objectType, const int32_t grantorUID, const int32_t granteeUID,
                                  const PrivMgrBitmap &objectPrivs, const PrivMgrBitmap &grantablePrivs);

  PrivStatus grantRole(const std::vector<int32_t> &roleIDs, const std::vector<std::string> &roleNames,
                       const std::vector<int32_t> &grantorIDs, const std::vector<std::string> &grantorNames,
                       PrivAuthClass grantorClass, const std::vector<int32_t> &granteeIDs,
                       const std::vector<std::string> &granteeNames, const std::vector<PrivAuthClass> &granteeClasses,
                       const int32_t grantDepth);

  PrivStatus grantSchemaPrivilege(const int64_t schemaUID, const std::string &schemaName,
                                  const ComObjectType schemaType, const int32_t grantorID,
                                  const std::string &grantorName, const int32_t granteeID,
                                  const std::string &granteeName, const int32_t schemaOwnerID,
                                  const std::vector<PrivType> &privs, const bool isAllSpecified,
                                  const bool isWGOSpecified);

  PrivStatus initializeAuthorizationMetadata(std::vector<std::string> tablesCreated, bool initializeMD,
                                             ExeCliInterface *cliInterface);

  PrivStatus insertPrivRowsForObject(const int64_t objectUID, const std::vector<ObjectPrivsRow> &objectPrivsRows);

  bool isPrivMgrTable(const std::string &objectName);

  PrivStatus registerComponent(const std::string &componentName, const bool isSystem,
                               const std::string &componentDetails);

  PrivStatus revokeComponentPrivilege(const std::string &componentName,
                                      const std::vector<std::string> &operationNamesList, const int32_t grantorID,
                                      const std::string &granteeName, const bool isGOFSpecified,
                                      PrivDropBehavior dropBehavior);

  PrivStatus revokeObjectPrivilege(const int64_t objectUID, const std::string &objectName,
                                   const ComObjectType objectType, const int32_t granteeUID,
                                   const std::string &granteeName, const int32_t grantorUID,
                                   const std::string &grantorName, const std::vector<PrivType> &privs,
                                   const std::vector<ColPrivSpec> &colPrivsArray, const bool isAllSpecified,
                                   const bool isGOFSpecified);

  PrivStatus revokeObjectPrivilege(const int64_t objectUID, const std::string &objectName, const int32_t grantorUID);

  PrivStatus revokeRole(const std::vector<int32_t> &roleIDs, const std::vector<int32_t> &granteeIDs,
                        const std::vector<PrivAuthClass> &granteeClasses, const std::vector<int32_t> &grantorIDs,
                        const bool isGOFSpecified, const int32_t newGrantDepth, PrivDropBehavior dropBehavior);

  PrivStatus revokeSchemaPrivilege(const int64_t schemaUID, const std::string &schemaName);

  PrivStatus revokeSchemaPrivilege(const int64_t schemaUID, const std::string &schemaName,
                                   const ComObjectType schemaType, const int32_t granteeID,
                                   const std::string &granteeName, const int32_t grantorID,
                                   const std::string &grantorName, const int32_t schemaOwnerID,
                                   const std::vector<PrivType> &privs, const bool isAllSpecified,
                                   const bool isGOFSpecified);

  PrivStatus unregisterComponent(const std::string &componentName, PrivDropBehavior dropBehavior);

  int recreateRoleRelationship(ExeCliInterface *);

  // -------------------------------------------------------------------
  // Accessors:
  // -------------------------------------------------------------------
  inline ComDiagsArea *getDiags(void) { return pDiags_; }
  inline const std::string getMetadataLocation(void) { return metadataLocation_; }
  inline const std::string getTrafMetadataLocation(void) { return trafMetadataLocation_; }

 protected:
  // -------------------------------------------------------------------
  // Mutators:
  // -------------------------------------------------------------------
  inline void setDiags(ComDiagsArea *pDiags) { pDiags_ = pDiags; }
  inline void setMetadataLocation(std::string &metadataLocation) { metadataLocation_ = metadataLocation; }
  inline void setTrafMetadataLocation(std::string &trafMetadataLocation) {
    trafMetadataLocation_ = trafMetadataLocation;
  }

  PrivStatus getPrivsForSchema(const int64_t objectUID, const int32_t userID, int64_t &schemaUID,
                               PrivMgrDesc &schemaPrivsOfTheUser);

 private:
  // -------------------------------------------------------------------
  // Private accessors:
  // -------------------------------------------------------------------
  PrivMgrCommands &operator=(const PrivMgrCommands &other);

};  // class PrivMgrCommands

#endif
