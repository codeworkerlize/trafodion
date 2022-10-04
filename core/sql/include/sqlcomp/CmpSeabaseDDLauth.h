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

#ifndef _CMP_SEABASE_DDL_AUTH_H_
#define _CMP_SEABASE_DDL_AUTH_H_

// *****************************************************************************
// *
// * File:         CmpSeabaseDDLauth.h
// * Description:  Describes the DDL classes for Trafodion user management
// *
// * Contents:
// *   class AuthConflict
// *   class AuthConflictList
// *
// *   class CmpSeabaseDDLauth
// *   class CmpSeabaseDDLuser
// *   class CmpSeabaseDDLgroup
// *   class CmpSeabaseDDLrole
// *   class CmpSeabaseDDLtenant
// *
// *****************************************************************************

#include "common/ComSmallDefs.h"
#include "sqlcomp/PrivMgrDefs.h"
#include "PrivMgrComponentDefs.h"
#include "common/NAUserId.h"
#include "optimizer/ObjectNames.h"
#include "parser/ElemDDLList.h"

#include "cli/sqlcli.h"
#include "comexe/ComQueue.h"
#include "executor/ExExeUtilCli.h"
#include "export/ComDiags.h"

#include <vector>
#include <set>

class StmtDDLRegisterUser;
class StmtDDLAlterUser;
class StmtDDLUserGroup;
class StmtDDLCreateRole;
class StmtDDLTenant;
class NAString;
class TenantInfo;
class TenantUsage;
class TenantUsageList;
class TenantGroupInfo;
class TenantGroupInfoList;
class TenantNodeInfo;
class TenantNodeInfoList;
class TenantResource;
class TenantResourceUsage;
class TenantResourceUsageList;

// ----------------------------------------------------------------------------
// class:  AuthConflict
//
// AuthConflict describes a mismatch between the auth_id stored in the AUTHS
// table and the ID stored in one of these metadata tables:
//   "_MD_".OBJECTS (columns schema_owner & object_owner)
//   "_PRIVMGR_MD_".COLUMN_PRIVILEGES (columns grantor_id & grantee_id)
//   "_PRIVMGR_MD_".OBJECT_PRIVILEGES (columns grantor_id & grantee_id)
//   "_PRIVMGR_MD_".SCHEMA_PRIVILEGES (columns grantor_id & grantee_id)
//
// AuthIDs are generated in the order users/roles are created
//
// A mismatch can happen when restoring objects where the source and
// target systems have the same authNames (users and roles) but different
// IDs; that is, users and roles were created in a different order.
//
// For example:
//    source system
//       register user henry -> map to 33344
//       register user mary  -> map to 33345
//    target system
//       register user mary  -> map to 33344
//       register user henry -> map to 33345
//
//  If a schema owned by henry is restored on the target, user IDs conflict
//
//  An example AuthConflict:
//    source ID -> 33344
//    target ID -> 33345
//    auth name -> HENRY
//    md table  -> OBJECTS
//    md column -> SCHEMA_OWNER
//    row count -> 15 rows found that contain the conflict
// ----------------------------------------------------------------------------
class AuthConflict {
 public:
  AuthConflict() : targetID_(0), sourceID_(0), rowCount_(0) {}

  AuthConflict(Int32 sourceAuthID, NAString authName, Int32 targetAuthID, NAString mdTableName, NAString mdColName)
      : authName_(authName),
        mdColumn_(mdColName),
        mdTable_(mdTableName),
        rowCount_(0),
        sourceID_(sourceAuthID),
        targetID_(targetAuthID) {}

  NABoolean operator==(const AuthConflict &other) {
    if ((sourceID_ == other.sourceID_) && (authName_ == other.authName_) && (targetID_ == other.targetID_) &&
        (mdTable_ == other.mdTable_) && (mdColumn_ == other.mdColumn_))
      return TRUE;
    return FALSE;
  }

  const NAString getAuthName() { return authName_; }
  const NAString getMDColumn() { return mdColumn_; }
  const NAString getMDTable() { return mdTable_; }
  const Int64 getRowCount() { return rowCount_; }
  const Int32 getSourceID() { return sourceID_; }
  const Int32 getTargetID() { return targetID_; }

  void setAuthName(NAString authName) { authName_ = authName; }
  void setMDColName(NAString mdColName) { mdColumn_ = mdColName; }
  void setMDTableName(NAString mdTableName) { mdTable_ = mdTableName; }
  void setRowCount(Int64 rowCount) { rowCount_ = rowCount; }
  void setSourceID(Int32 sourceAuthID) { sourceID_ = sourceAuthID; }
  void setTargetID(Int32 targetAuthID) { targetID_ = targetAuthID; }

 private:
  NAString authName_;
  NAString mdColumn_;
  NAString mdTable_;
  Int64 rowCount_;
  Int32 sourceID_;
  Int32 targetID_;
};

// ----------------------------------------------------------------------------
// class:  AuthConflictList
//
// Defines a list of AuthConflict
// ----------------------------------------------------------------------------
class AuthConflictList : public LIST(AuthConflict) {
 public:
  // constructor
  AuthConflictList(NAHeap *heap = NULL) : LIST(AuthConflict)(heap), heap_(heap) {}

  // virtual destructor
  virtual ~AuthConflictList(){};

  inline const void setHeap(NAHeap *heap) { heap_ = heap; }
  inline NAHeap *getHeap() { return heap_; }

  NAString createInList();
  CollIndex findEntry(const AuthConflict &conflict);
  short generateConflicts(ExeCliInterface *cliInterface);
  CollIndex getIndexForSourceID(Int32 origID);
  short updateObjects(ExeCliInterface *cliInterface);
  short updatePrivs(ExeCliInterface *cliInterface, const char *schName, const char *objName);

 private:
  NAHeap *heap_;
};

// ----------------------------------------------------------------------------
// class:  CmpSeabaseDDLauth
//
// User management class defining commonality between all authorization IDs
// Authorization IDs consist of users, PUBLIC(TDB), roles(TBD), and groups(TBD)
// ----------------------------------------------------------------------------

class CmpSeabaseDDLauth {
 public:
  enum AuthStatus {
    STATUS_UNKNOWN = 10,
    STATUS_GOOD = 11,
    STATUS_WARNING = 12,
    STATUS_NOTFOUND = 13,
    STATUS_ERROR = 14
  };

  CmpSeabaseDDLauth();
  CmpSeabaseDDLauth(const NAString &systemCatalog, const NAString &MDSchema);

  virtual ~CmpSeabaseDDLauth();
  AuthStatus getAuthDetails(const char *pAuthName, bool isExternal = false);
  AuthStatus getAuthDetails(Int32 authID);
  bool authExists(const NAString &authName, bool isExternal = false);
  virtual bool describe(const NAString &authName, NAString &authText);
  AuthStatus getRoleIDs(const Int32 authID, std::vector<int32_t> &roleIDs, std::vector<int32_t> &granteeIDs,
                        bool fetchGroupRoles = true);
  NAString getObjectName(const std::vector<int64_t> objectUIDs);

  // accessors
  Int16 getAuthConfig() const { return authConfig_; }
  Int32 getAuthCreator() const { return authCreator_; }
  Int64 getAuthCreateTime() const { return authCreateTime_; }
  const NAString getAuthDbName() const { return authDbName_; }
  const NAString getAuthExtName() const { return authExtName_; }
  const NAString getAuthPassword() const { return authPassword_; }
  TenantGroupInfoList *getAuthGroupList() { return authGroupList_; }
  Int32 getAuthID() const { return authID_; }
  Int64 getAuthRedefTime() const { return authRedefTime_; }
  Int64 getPasswdModTime() const { return passwdModTime_; }
  ComIdClass getAuthType() const { return authType_; }

  bool isAutoCreated() const { return authAutoCreated_; }
  bool isAuthAdmin() const { return authIsAdmin_; }
  bool isAuthInTenant() const { return authInTenant_; }
  bool isAuthValid() const { return authValid_; }
  bool isPublic() const { return authID_ == PUBLIC_USER; }
  bool isRole() const { return authType_ == COM_ROLE_CLASS; }
  bool isUser() const { return authType_ == COM_USER_CLASS; }
  bool isTenant() const { return authType_ == COM_TENANT_CLASS; }
  bool isUserGroup() const { return authType_ == COM_USER_GROUP_CLASS; }
  bool isAdminAuthID(const Int32 authID);

  static bool isSystemAuth(const ComIdClass authType, const NAString &authName, bool &specialAuth);

  static bool isRoleID(Int32 authID);
  static bool isUserID(Int32 authID);
  static bool isTenantID(Int32 authID);
  static bool isUserGroupID(Int32 authID);

  CmpSeabaseDDLauth::AuthStatus getAdminRoles(std::vector<Int32> &roleList);
  CmpSeabaseDDLauth::AuthStatus getAdminRole(const Int32 authID, Int32 &adminRoleID);

  CmpSeabaseDDLauth::AuthStatus getAuthsForCreator(Int32 authCreator, std::vector<std::string> &userNames,
                                                   std::vector<std::string> &roleNames);

  Int64 getNumberTenants();

  Int64 getSchemaUID(ExeCliInterface *cliInterface, const SchemaName *schemaName);

  // Static function is used to get groups by directly using userName
  // Instanced function is used to get groups by instance with userID which is aways means 'get user groups by self'

  CmpSeabaseDDLauth::AuthStatus getUserGroups(std::vector<int32_t> &groupIDs);
  static Int32 getUserGroups(const char *userName, std::vector<int32_t> &groupIDs);

  inline void setDDLQuery(char *query) { ddlQuery_ = query; }
  inline void setDDLLen(Lng32 len) { ddlLength_ = len; }

  CmpSeabaseDDLauth::AuthStatus getUserGroupMembers(std::vector<int32_t> &);
  static Int32 getUserGroupMembers(const char *, std::vector<int32_t> &);

  // get input error password count
  Int32 getErrPwdCnt(Int32 userid, Int16 &errcnt);
  // update error password count
  // if input right password, the error count will reset 0
  Int32 updateErrPwdCnt(Int32 userid, Int16 errcnt, bool reset = false);

  // get grace counter for expired password
  Int32 getGracePwdCnt(Int32, Int16 &);
  // update grace counter for expired password
  Int32 updateGracePwdCnt(Int32 userid, Int16 graceCnt = 0);

  // too waste to call getAuthDetails()
  void getAuthDbNameByAuthIDs(std::vector<int32_t> &, std::vector<std::string> &);
  void getAuthDbNameByAuthIDs(std::map<int32_t, std::string *> &);

  // protected -> public
  bool verifyAuthority(const SQLOperation operation);

  // insert default password for users whose are exist on AUTHS but have not password on TEXT into table TEXT
  static void initAuthPwd(ExeCliInterface *cliInterface);

  static Int32 getMaxUserIdInUsed(ExeCliInterface *cliInterface);

  // change member-type from protected to public to avoid calling getAuthDetails
  void setAuthID(const Int32 authID) { authID_ = authID; }

 protected:
  bool isAuthNameReserved(const NAString &authName);
  bool isAuthNameValid(const NAString &authName);
  bool isPasswordValid(const NAString &password);

  Int32 getUniqueAuthID(const Int32 minValue, const Int32 maxValue);

  // mutators
  void setAuthConfig(const Int16 authConfig) { authConfig_ = authConfig; }
  void setAuthCreator(const Int32 authCreator) { authCreator_ = authCreator; }
  void setAuthCreateTime(const Int64 authCreateTime) { authCreateTime_ = authCreateTime; }
  void setAuthDbName(const NAString &authDbName) { authDbName_ = authDbName; }
  void setAuthExtName(const NAString &authExtName) { authExtName_ = authExtName; }
  void setAuthPassword(const NAString &authPassword) { authPassword_ = authPassword; }
  void setAuthRedefTime(const Int64 authRedefTime) { authRedefTime_ = authRedefTime; }
  void setPasswdModTime(const Int64 passwdModTime) { passwdModTime_ = passwdModTime; }
  void setAuthType(ComIdClass authType) { authType_ = authType; }
  void setAuthValid(bool isValid) { authValid_ = isValid; }
  void setAuthIsAdmin(bool isAdmin) { authIsAdmin_ = isAdmin; }
  void setAuthInTenant(bool inTenant) { authInTenant_ = inTenant; }
  void setAutoCreated(bool autoCreated) { authAutoCreated_ = autoCreated; }
  void setAuthGroupList(TenantGroupInfoList *groupList) { authGroupList_ = groupList; }

  bool createStandardAuth(const std::string authName, const int32_t authID);

  // metadata access methods
  void deleteRow(const NAString &authName);
  void insertRow(void);
  short updateSeabaseXDCDDLTable();

  void updateRow(NAString &setClause);
  AuthStatus updateRedefTime();
  AuthStatus selectAuthIDs(const NAString &whereClause, std::set<int32_t> &authIDs);
  AuthStatus selectAuthIDAndNames(const NAString &whereClause,
                                  std::map<const char *, int32_t, Char_Compare> &nameAndIDs,
                                  bool consistentCheck = true);

  AuthStatus selectAuthNames(const NAString &whereClause, std::vector<std::string> &userNames,
                             std::vector<std::string> &roleNames, std::vector<std::string> &tenantNames);
  AuthStatus selectAllWhere(const NAString &whereClause, std::vector<Int32> &roleIDs);
  AuthStatus selectExactRow(const NAString &cmd);
  Int64 selectCount(const NAString &whereClause);
  Int32 selectMaxAuthID(const NAString &whereClause);

  // function is static for using function point
  static Int32 addGroupMember(Int32, Int32);
  static Int32 removeGroupMember(Int32, Int32);
  NABoolean getAuthNameAndAuthID(ElemDDLList *, std::map<const char *, int32_t, Char_Compare> &);
  NABoolean getAuthNameAndAuthID(std::vector<char *> &, std::map<const char *, int32_t, Char_Compare> &);

  NAString systemCatalog_;
  NAString MDSchema_; /* Qualified metadata schema */
  ComAuthenticationType currentAuthType_;

 private:
  Int16 authConfig_;
  Int32 authCreator_;
  Int64 authCreateTime_;
  NAString authDbName_;
  NAString authExtName_;
  NAString authPassword_;
  Int32 authID_;
  Int64 authRedefTime_;
  Int64 passwdModTime_;
  ComIdClass authType_;
  bool authValid_;
  bool authIsAdmin_;
  bool authInTenant_;
  bool authAutoCreated_;
  TenantGroupInfoList *authGroupList_;

  // for DDL XDC, we do not own this memory
  char *ddlQuery_;
  Lng32 ddlLength_;
};

// ----------------------------------------------------------------------------
// class:  CmpSeabaseDDLuser
//
// Class that manages user authorization IDs
//
// Child class of CmpSeabaseDDLauth
// ----------------------------------------------------------------------------
class CmpSeabaseDDLuser : public CmpSeabaseDDLauth {
 public:
  CmpSeabaseDDLuser();
  CmpSeabaseDDLuser(const NAString &systemCatalog, const NAString &MDSchema);

  // Execute level methods
  void alterUser(StmtDDLAlterUser *pNode);
  void registerUser(StmtDDLRegisterUser *pNode);
  Int32 registerUserInternal(const NAString &extUserName, const NAString &dbUserName, const char *config,
                             bool autoCreated, Int32 authCreator, const NAString &authPassword);

  void unregisterUser(StmtDDLRegisterUser *pNode);
  void registerStandardUser(const std::string userName, const int32_t userID);

  CmpSeabaseDDLauth::AuthStatus getUserDetails(const char *pUserName, bool isExternal = false);
  CmpSeabaseDDLauth::AuthStatus getUserDetails(Int32 userID);

  CmpSeabaseDDLauth::AuthStatus getAllJoinedUserGroup(std::vector<int32_t> &);

  void exitAllJoinedUserGroup();

  bool describe(const NAString &authName, NAString &authText);

  static NAString getDefAuthInfo(Int32 uid);

 protected:
  void reflashPasswordModTime();
};

// ----------------------------------------------------------------------------
// class:  CmpSeabaseDDLgroup
//
// Class that manages group authorization IDs
//
// Child class of CmpSeabaseDDLauth
// ----------------------------------------------------------------------------
class CmpSeabaseDDLgroup : public CmpSeabaseDDLauth {
 public:
  CmpSeabaseDDLgroup();
  CmpSeabaseDDLgroup(const NAString &systemCatalog, const NAString &MDSchema);

  void alterGroup(StmtDDLUserGroup *pNode);
  void registerGroup(StmtDDLUserGroup *pNode);
  Int32 registerGroupInternal(const NAString &groupName, const NAString &config, bool autoCreated);
  void unregisterGroup(StmtDDLUserGroup *pNode);

  bool describe(const NAString &authName, NAString &authText);

  CmpSeabaseDDLauth::AuthStatus getGroupDetails(const char *pGroupName);

  void removeAllGroupMember();

 protected:
};

// ----------------------------------------------------------------------------
// class:  CmpSeabaseDDLrole
//
// Class that manages role authorization IDs
//
// Child class of CmpSeabaseDDLauth
// ----------------------------------------------------------------------------
class CmpSeabaseDDLrole : public CmpSeabaseDDLauth {
 public:
  CmpSeabaseDDLrole();
  CmpSeabaseDDLrole(const NAString &systemCatalog);
  CmpSeabaseDDLrole(const NAString &systemCatalog, const NAString &MDSchema);

  Int32 createAdminRole(const NAString &roleName, const NAString &tenantName, const Int32 roleOwner,
                        const char *roleOwnerName);

  void createRole(StmtDDLCreateRole *pNode);

  bool createStandardRole(const std::string authName, const int32_t authID);

  bool describe(const NAString &roleName, NAString &roleText);

  Int32 dropAdminRole(const NAString &roleName, const Int32 tenantID);

  void dropRole(StmtDDLCreateRole *pNode);

  void dropStandardRole(const std::string roleName);

  CmpSeabaseDDLauth::AuthStatus getRoleDetails(const char *pRoleName);

  bool getRoleIDFromRoleName(const char *roleName, Int32 &roleID);

 protected:
};

// ----------------------------------------------------------------------------
// class:  CmpSeabaseDDLtenant
//
// Class that manages tenant authorization IDs
//
// Child class of CmpSeabaseDDLauth
// ----------------------------------------------------------------------------
class CmpSeabaseDDLtenant : public CmpSeabaseDDLauth {
 public:
  CmpSeabaseDDLtenant();
  CmpSeabaseDDLtenant(const NAString &systemCatalog, const NAString &MDSchema);
  virtual ~CmpSeabaseDDLtenant(){};

  // Execute level methods
  Int32 registerStandardTenant(const std::string tenantName, const int32_t tenantID);

  void alterTenant(ExeCliInterface *cliInterface, StmtDDLTenant *pNode);

  Int32 registerTenant(ExeCliInterface *cliInterface, StmtDDLTenant *pNode, Int32 adminRoleID);

  Int32 registerTenantPrepare(StmtDDLTenant *pNode, Int32 &adminRoleID);

  Int32 unregisterTenant(ExeCliInterface *cliInterface, StmtDDLTenant *pNode);

  Int32 unregisterTenantPrepare(StmtDDLTenant *pNode);

  bool describe(const NAString &authName, NAString &authText);

  bool isDetached() const { return isDetached_; }
  void setIsDetached(bool isDetached) { isDetached_ = isDetached; }

  static NAString getNodeName(const Int64 nodeID);
  static NAString getRGroupName(const Int64 rgroupUID);
  static NAString getSchemaName(const Int64 schUID);

  Int64 getDefSchemaUID(TenantInfo &tenantInfo, const TenantUsageList *usageList);

  static short getNodeDetails(const TenantResourceUsageList *usageList, TenantNodeInfoList *&nodeInfoList);

  Int32 getNodesForRGroups(const TenantUsageList *usageList, TenantResourceUsageList *&nodeList);

  CmpSeabaseDDLauth::AuthStatus getTenantDetails(const char *pTenantName, TenantInfo &tenantInfo);

  CmpSeabaseDDLauth::AuthStatus getTenantDetails(Int32 tenantID, TenantInfo &tenantInfo);

  CmpSeabaseDDLauth::AuthStatus getTenantDetails(Int64 schemaUID, TenantInfo &tenantInfo);

  Int32 getTenantSchemas(ExeCliInterface *cliInterface, NAList<Int64> *&schemaUIDList);

  TenantGroupInfoList *getTenantGroupList(TenantInfo &tenantInfo, NAHeap *heap);

  static Int32 predefinedNodeAllocations(Int32 unitsToAllocate, TenantResourceUsageList *&resourceUsageList);

 protected:
  void addGroups(const StmtDDLTenant *pNode, const Int32 tenantID, const TenantUsageList *tenantGroupList,
                 TenantUsageList *&usageList);

  void addResources(const StmtDDLTenant *pNode, ExeCliInterface *cliInterface, TenantInfo &tenantInfo,
                    TenantNodeInfoList *&outNodeList, TenantUsageList *&usageList,
                    TenantResourceUsageList *&resourceUsageList);

  void alterResources(ExeCliInterface *cliInterface, TenantInfo &tenantInfo, TenantNodeInfoList *&outNodeList,
                      TenantResourceUsageList *&resourceUsageList);

  void dropResources(const StmtDDLTenant *pNode, ExeCliInterface *cliInterface, TenantInfo &tenantInfo,
                     TenantNodeInfoList *&outNodeList, TenantUsageList *&usageList,
                     TenantResourceUsageList *&resourceUsageList);

  void dropGroups(const StmtDDLTenant *pNode, TenantInfo &tenantInfo, TenantUsageList *&usageList);

  void dropResourceGroup(const StmtDDLTenant *pNode, TenantUsageList *&usageList);

  short findDetachedTenant(const StmtDDLTenant *pNode, Int32 &adminRoleID, Int32 &tenantID);

  short getNodeDetails(ExeCliInterface *cliInterface, TenantNodeInfoList *&nodesToAdd);

  short getTenantResource(const NAString &resourceName, const bool isNodeResource, const bool &fetchUsages,
                          TenantResource &tenantResource);

  void createSchemas(ExeCliInterface *cliInterface, const StmtDDLTenant *pNode, const Int32 &tenantID,
                     const TenantUsageList *origUsageList, NAString schemaOwner, Int64 &defSchUID,
                     TenantUsageList *&usageList);

  void dropSchemas(ExeCliInterface *cliInterface, const Int32 tenantID, const NAList<NAString> *schemaList);

  short generateSchemaUsages(const Int32 &tenantID, const Int32 &adminRoleID, TenantUsageList *&existingUsageList);

  short generateTenantID(Int32 &tenantID);

  CmpSeabaseDDLauth::AuthStatus getSchemaDetails(ExeCliInterface *cliInterface, const SchemaName *schemaName,
                                                 Int64 &schUID, ComObjectType &schType, Int32 &schOwner);

 private:
  bool isDetached_;
};

#endif  // _CMP_SEABASE_DDL_AUTH_H_
