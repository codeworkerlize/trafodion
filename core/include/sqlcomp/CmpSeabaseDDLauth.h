

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

#include <set>
#include <vector>

#include "PrivMgrComponentDefs.h"
#include "cli/sqlcli.h"
#include "comexe/ComQueue.h"
#include "common/ComSmallDefs.h"
#include "common/NAUserId.h"
#include "executor/ExExeUtilCli.h"
#include "export/ComDiags.h"
#include "optimizer/ObjectNames.h"
#include "parser/ElemDDLList.h"
#include "sqlcomp/PrivMgrDefs.h"

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

  AuthConflict(int sourceAuthID, NAString authName, int targetAuthID, NAString mdTableName, NAString mdColName)
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
  const long getRowCount() { return rowCount_; }
  const int getSourceID() { return sourceID_; }
  const int getTargetID() { return targetID_; }

  void setAuthName(NAString authName) { authName_ = authName; }
  void setMDColName(NAString mdColName) { mdColumn_ = mdColName; }
  void setMDTableName(NAString mdTableName) { mdTable_ = mdTableName; }
  void setRowCount(long rowCount) { rowCount_ = rowCount; }
  void setSourceID(int sourceAuthID) { sourceID_ = sourceAuthID; }
  void setTargetID(int targetAuthID) { targetID_ = targetAuthID; }

 private:
  NAString authName_;
  NAString mdColumn_;
  NAString mdTable_;
  long rowCount_;
  int sourceID_;
  int targetID_;
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
  CollIndex getIndexForSourceID(int origID);
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
  AuthStatus getAuthDetails(int authID);
  bool authExists(const NAString &authName, bool isExternal = false);
  virtual bool describe(const NAString &authName, NAString &authText);
  AuthStatus getRoleIDs(const int authID, std::vector<int32_t> &roleIDs, std::vector<int32_t> &granteeIDs,
                        bool fetchGroupRoles = true);
  NAString getObjectName(const std::vector<int64_t> objectUIDs);

  // accessors
  Int16 getAuthConfig() const { return authConfig_; }
  int getAuthCreator() const { return authCreator_; }
  long getAuthCreateTime() const { return authCreateTime_; }
  const NAString getAuthDbName() const { return authDbName_; }
  const NAString getAuthExtName() const { return authExtName_; }
  const NAString getAuthPassword() const { return authPassword_; }
  TenantGroupInfoList *getAuthGroupList() { return authGroupList_; }
  int getAuthID() const { return authID_; }
  long getAuthRedefTime() const { return authRedefTime_; }
  long getPasswdModTime() const { return passwdModTime_; }
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
  bool isAdminAuthID(const int authID);

  static bool isSystemAuth(const ComIdClass authType, const NAString &authName, bool &specialAuth);

  static bool isRoleID(int authID);
  static bool isUserID(int authID);
  static bool isTenantID(int authID);
  static bool isUserGroupID(int authID);

  CmpSeabaseDDLauth::AuthStatus getAdminRoles(std::vector<int> &roleList);
  CmpSeabaseDDLauth::AuthStatus getAdminRole(const int authID, int &adminRoleID);

  CmpSeabaseDDLauth::AuthStatus getAuthsForCreator(int authCreator, std::vector<std::string> &userNames,
                                                   std::vector<std::string> &roleNames);

  long getNumberTenants();

  long getSchemaUID(ExeCliInterface *cliInterface, const SchemaName *schemaName);

  // Static function is used to get groups by directly using userName
  // Instanced function is used to get groups by instance with userID which is aways means 'get user groups by self'

  CmpSeabaseDDLauth::AuthStatus getUserGroups(std::vector<int32_t> &groupIDs);
  static int getUserGroups(const char *userName, std::vector<int32_t> &groupIDs);

  inline void setDDLQuery(char *query) { ddlQuery_ = query; }
  inline void setDDLLen(int len) { ddlLength_ = len; }

  CmpSeabaseDDLauth::AuthStatus getUserGroupMembers(std::vector<int32_t> &);
  static int getUserGroupMembers(const char *, std::vector<int32_t> &);

  // get input error password count
  int getErrPwdCnt(int userid, Int16 &errcnt);
  // update error password count
  // if input right password, the error count will reset 0
  int updateErrPwdCnt(int userid, Int16 errcnt, bool reset = false);

  // get grace counter for expired password
  int getGracePwdCnt(int, Int16 &);
  // update grace counter for expired password
  int updateGracePwdCnt(int userid, Int16 graceCnt = 0);

  // too waste to call getAuthDetails()
  void getAuthDbNameByAuthIDs(std::vector<int32_t> &, std::vector<std::string> &);
  void getAuthDbNameByAuthIDs(std::map<int32_t, std::string *> &);

  // protected -> public
  bool verifyAuthority(const SQLOperation operation);

  // insert default password for users whose are exist on AUTHS but have not password on TEXT into table TEXT
  static void initAuthPwd(ExeCliInterface *cliInterface);

  static int getMaxUserIdInUsed(ExeCliInterface *cliInterface);

  // change member-type from protected to public to avoid calling getAuthDetails
  void setAuthID(const int authID) { authID_ = authID; }

 protected:
  bool isAuthNameReserved(const NAString &authName);
  bool isAuthNameValid(const NAString &authName);
  bool isPasswordValid(const NAString &password);

  int getUniqueAuthID(const int minValue, const int maxValue);

  // mutators
  void setAuthConfig(const Int16 authConfig) { authConfig_ = authConfig; }
  void setAuthCreator(const int authCreator) { authCreator_ = authCreator; }
  void setAuthCreateTime(const long authCreateTime) { authCreateTime_ = authCreateTime; }
  void setAuthDbName(const NAString &authDbName) { authDbName_ = authDbName; }
  void setAuthExtName(const NAString &authExtName) { authExtName_ = authExtName; }
  void setAuthPassword(const NAString &authPassword) { authPassword_ = authPassword; }
  void setAuthRedefTime(const long authRedefTime) { authRedefTime_ = authRedefTime; }
  void setPasswdModTime(const long passwdModTime) { passwdModTime_ = passwdModTime; }
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
  AuthStatus selectAllWhere(const NAString &whereClause, std::vector<int> &roleIDs);
  AuthStatus selectExactRow(const NAString &cmd);
  long selectCount(const NAString &whereClause);
  int selectMaxAuthID(const NAString &whereClause);

  // function is static for using function point
  static int addGroupMember(int, int);
  static int removeGroupMember(int, int);
  NABoolean getAuthNameAndAuthID(ElemDDLList *, std::map<const char *, int32_t, Char_Compare> &);
  NABoolean getAuthNameAndAuthID(std::vector<char *> &, std::map<const char *, int32_t, Char_Compare> &);

  NAString systemCatalog_;
  NAString MDSchema_; /* Qualified metadata schema */
  ComAuthenticationType currentAuthType_;

 private:
  Int16 authConfig_;
  int authCreator_;
  long authCreateTime_;
  NAString authDbName_;
  NAString authExtName_;
  NAString authPassword_;
  int authID_;
  long authRedefTime_;
  long passwdModTime_;
  ComIdClass authType_;
  bool authValid_;
  bool authIsAdmin_;
  bool authInTenant_;
  bool authAutoCreated_;
  TenantGroupInfoList *authGroupList_;

  // for DDL XDC, we do not own this memory
  char *ddlQuery_;
  int ddlLength_;
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
  int registerUserInternal(const NAString &extUserName, const NAString &dbUserName, const char *config,
                           bool autoCreated, int authCreator, const NAString &authPassword);

  void unregisterUser(StmtDDLRegisterUser *pNode);
  void registerStandardUser(const std::string userName, const int32_t userID);

  CmpSeabaseDDLauth::AuthStatus getUserDetails(const char *pUserName, bool isExternal = false);
  CmpSeabaseDDLauth::AuthStatus getUserDetails(int userID);

  CmpSeabaseDDLauth::AuthStatus getAllJoinedUserGroup(std::vector<int32_t> &);

  void exitAllJoinedUserGroup();

  bool describe(const NAString &authName, NAString &authText);

  static NAString getDefAuthInfo(int uid);

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
  int registerGroupInternal(const NAString &groupName, const NAString &config, bool autoCreated);
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

  int createAdminRole(const NAString &roleName, const NAString &tenantName, const int roleOwner,
                      const char *roleOwnerName);

  void createRole(StmtDDLCreateRole *pNode);

  bool createStandardRole(const std::string authName, const int32_t authID);

  bool describe(const NAString &roleName, NAString &roleText);

  int dropAdminRole(const NAString &roleName, const int tenantID);

  void dropRole(StmtDDLCreateRole *pNode);

  void dropStandardRole(const std::string roleName);

  CmpSeabaseDDLauth::AuthStatus getRoleDetails(const char *pRoleName);

  bool getRoleIDFromRoleName(const char *roleName, int &roleID);

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
  int registerStandardTenant(const std::string tenantName, const int32_t tenantID);

  void alterTenant(ExeCliInterface *cliInterface, StmtDDLTenant *pNode);

  int registerTenant(ExeCliInterface *cliInterface, StmtDDLTenant *pNode, int adminRoleID);

  int registerTenantPrepare(StmtDDLTenant *pNode, int &adminRoleID);

  int unregisterTenant(ExeCliInterface *cliInterface, StmtDDLTenant *pNode);

  int unregisterTenantPrepare(StmtDDLTenant *pNode);

  bool describe(const NAString &authName, NAString &authText);

  bool isDetached() const { return isDetached_; }
  void setIsDetached(bool isDetached) { isDetached_ = isDetached; }

  static NAString getNodeName(const long nodeID);
  static NAString getRGroupName(const long rgroupUID);
  static NAString getSchemaName(const long schUID);

  long getDefSchemaUID(TenantInfo &tenantInfo, const TenantUsageList *usageList);

  static short getNodeDetails(const TenantResourceUsageList *usageList, TenantNodeInfoList *&nodeInfoList);

  int getNodesForRGroups(const TenantUsageList *usageList, TenantResourceUsageList *&nodeList);

  CmpSeabaseDDLauth::AuthStatus getTenantDetails(const char *pTenantName, TenantInfo &tenantInfo);

  CmpSeabaseDDLauth::AuthStatus getTenantDetails(int tenantID, TenantInfo &tenantInfo);

  CmpSeabaseDDLauth::AuthStatus getTenantDetails(long schemaUID, TenantInfo &tenantInfo);

  int getTenantSchemas(ExeCliInterface *cliInterface, NAList<long> *&schemaUIDList);

  TenantGroupInfoList *getTenantGroupList(TenantInfo &tenantInfo, NAHeap *heap);

  static int predefinedNodeAllocations(int unitsToAllocate, TenantResourceUsageList *&resourceUsageList);

 protected:
  void addGroups(const StmtDDLTenant *pNode, const int tenantID, const TenantUsageList *tenantGroupList,
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

  short findDetachedTenant(const StmtDDLTenant *pNode, int &adminRoleID, int &tenantID);

  short getNodeDetails(ExeCliInterface *cliInterface, TenantNodeInfoList *&nodesToAdd);

  short getTenantResource(const NAString &resourceName, const bool isNodeResource, const bool &fetchUsages,
                          TenantResource &tenantResource);

  void createSchemas(ExeCliInterface *cliInterface, const StmtDDLTenant *pNode, const int &tenantID,
                     const TenantUsageList *origUsageList, NAString schemaOwner, long &defSchUID,
                     TenantUsageList *&usageList);

  void dropSchemas(ExeCliInterface *cliInterface, const int tenantID, const NAList<NAString> *schemaList);

  short generateSchemaUsages(const int &tenantID, const int &adminRoleID, TenantUsageList *&existingUsageList);

  short generateTenantID(int &tenantID);

  CmpSeabaseDDLauth::AuthStatus getSchemaDetails(ExeCliInterface *cliInterface, const SchemaName *schemaName,
                                                 long &schUID, ComObjectType &schType, int &schOwner);

 private:
  bool isDetached_;
};

#endif  // _CMP_SEABASE_DDL_AUTH_H_
