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

#ifndef PRIVMGR_SCHEMAPRIVILEGES_H
#define PRIVMGR_SCHEMAPRIVILEGES_H

#include <bitset>
#include <string>
#include <vector>

#include "PrivMgrMDTable.h"
#include "common/ComSecurityKey.h"
#include "common/ComSmallDefs.h"
#include "sqlcomp/CmpDDLCatErrorCodes.h"
#include "sqlcomp/PrivMgrDefs.h"
#include "sqlcomp/PrivMgrDesc.h"
#include "sqlcomp/PrivMgrMD.h"

// *****************************************************************************
// *
// * File:         PrivMgrSchemaPrivileges.h
// * Description:  This file contains the class that manages schema access
// *               rights by granting, revoking, and obtaining current
// *               privileges
// * Language:     C++
// *
// *****************************************************************************

// *****************************************************************************
// * Class:        PrivMgrSchemaPrivileges
// * Description:  This class represents the access rights for schemas
// *****************************************************************************
class PrivMgrSchemaPrivileges : public PrivMgr {
 public:
  //
  // -------------------------------------------------------------------
  // Constructors and destructor:
  // -------------------------------------------------------------------
  PrivMgrSchemaPrivileges();

  PrivMgrSchemaPrivileges(const std::string &trafMetadataLocation, const std::string &metadataLocation,
                          ComDiagsArea *pDiags = NULL);

  PrivMgrSchemaPrivileges(const std::string &metadataLocation, ComDiagsArea *pDiags = NULL);

  PrivMgrSchemaPrivileges(const int64_t schemaUID, const std::string &schemaName, const std::string &metadataLocation,
                          ComDiagsArea *pDiags = NULL);

  PrivMgrSchemaPrivileges(const PrivMgrObjectInfo &objectInfo, const std::string &metadataLocation,
                          ComDiagsArea *pDiags = NULL);

  PrivMgrSchemaPrivileges(const PrivMgrSchemaPrivileges &other);

  virtual ~PrivMgrSchemaPrivileges();

  // -------------------------------------------------------------------
  // Public functions:
  // -------------------------------------------------------------------
  PrivStatus getPrivBitmaps(const std::string &whereClause, const std::string &orderByClause,
                            std::vector<PrivObjectBitmap> &privBitmaps);

  PrivStatus getPrivsOnSchema(const int64_t schemaUID, const int32_t schemaOwner, const ComObjectType objectType,
                              std::vector<PrivMgrDesc> &privDescs);

  PrivStatus getPrivsOnSchemaForUser(const int64_t objectUID, ComObjectType schemaType, const int32_t userID,
                                     PrivMgrDesc &privsForTheUser);

  PrivStatus getPrivRowsForSchema(const int64_t schemaUID, std::vector<ObjectPrivsRow> &schemaPrivsRows);

  PrivStatus getPrivTextForSchema(const PrivMgrObjectInfo &schemaInfo, std::string &privilegeText);

  PrivStatus getSchemasForRole(const int32_t roleID, std::vector<int64_t> &schemaList);

  PrivStatus getSchemaRowList(const int64_t schemaUID, const int32_t schemaOwnerID,
                              std::vector<PrivMgrMDRow *> &objectRows);

  PrivStatus grantSchemaPriv(const ComObjectType schemaType, const int32_t grantorID, const std::string &grantorName,
                             const int32_t granteeID, const std::string &granteeName, const int32_t schemaOwnerID,
                             const std::vector<PrivType> &privList, const bool isAllSpecified,
                             const bool isWGOSpecified);

  bool isAuthIDGrantedPrivs(const int32_t authID, std::vector<int64_t> &objectUIDs);

  PrivStatus revokeSchemaPriv();

  PrivStatus revokeSchemaPriv(const ComObjectType schemaType, const int32_t grantorID, const std::string &grantorName,
                              const int32_t granteeID, const std::string &granteeName, const int32_t schemaOwnerID,
                              const std::vector<PrivType> &privList, const bool isAllSpecified,
                              const bool isGOFSpecified);

 protected:
  bool convertPrivsToDesc(const bool isAllSpecified, const bool isWGOSpecified, const bool isGOFSpecified,
                          const std::vector<PrivType> privsList, PrivMgrDesc &privsToProcess);

  PrivStatus getPrivsFromAllGrantors(const int64_t schemaUID, const int32_t grantee,
                                     const std::vector<int32_t> &roleIDs, PrivMgrDesc &privs, bool &hasManagePrivPriv);

  PrivStatus getUserPrivs(ComObjectType schemaType, const int32_t grantee, const std::vector<int32_t> &roleIDs,
                          PrivMgrDesc &privs, bool &hasManagePrivPriv);

 private:
  // -------------------------------------------------------------------
  // Private functions:
  // -------------------------------------------------------------------

  bool checkRevokeRestrict(PrivMgrMDRow &rowIn, std::vector<PrivMgrMDRow *> &rowList);

  void deleteListOfAffectedObjects(std::vector<ObjectUsage *> listOfAffectedObjects) {
    while (!listOfAffectedObjects.empty()) delete listOfAffectedObjects.back(), listOfAffectedObjects.pop_back();
  }

  PrivStatus generateSchemaRowList(const int32_t schemaOwnerID);

  PrivStatus getDistinctIDs(const std::vector<PrivMgrMDRow *> &schemaRowList, std::vector<int32_t> &userIDs,
                            std::vector<int32_t> &roleIDs);

  PrivStatus getGrantedPrivs(const int32_t grantorID, const int32_t granteeID, PrivMgrMDRow &row);

  PrivStatus getRolesToCheck(const int32_t grantorID, const std::vector<int32_t> &roleIDs,
                             const ComObjectType schemaType, std::string &rolesToCheck);

  PrivStatus getRowsForGrantee(const int64_t schemaUID, const int32_t granteeID, const std::vector<int32_t> &roleIDs,
                               std::vector<PrivMgrMDRow *> &rowList);

  void getTreeOfGrantors(const int32_t granteeID, std::set<int32_t> &listOfGrantors);

  void reportPrivWarnings(const PrivMgrDesc &origPrivs, const PrivMgrDesc &actualPrivs, const CatErrorCode warningCode);

  void scanPublic(const PrivType pType, const std::vector<PrivMgrMDRow *> &privsList);

  void scanSchemaBranch(const PrivType pType, const int32_t &grantor, const std::vector<PrivMgrMDRow *> &privsList);

  PrivStatus sendSecurityKeysToRMS(const int32_t granteeID, const PrivMgrDesc &listOfRevokedPrivileges);

  PrivStatus updateDependentObjects(const ObjectUsage &objectUsage, const PrivCommand command);

  // -------------------------------------------------------------------
  // Data Members:
  // -------------------------------------------------------------------

  int64_t schemaUID_;
  std::string schemaName_;
  std::string schemaTableName_;
  std::vector<PrivMgrMDRow *> schemaRowList_;
};
#endif  // PRIVMGR_SCHEMAPRIVILEGES_H
