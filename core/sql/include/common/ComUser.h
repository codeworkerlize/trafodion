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

/*
 *******************************************************************************
 *
 * File:         ComUser.h
 * Description:  This file describes classes associated with user
 * and tenant  management
 *
 * Contents:
 *   class ComUser
 *   class ComUserVerifyObj
 *   class ComUserVerifyAuth
 *   class ComTenant
 *
 *****************************************************************************
 */
#ifndef _COM_USER_H_
#define _COM_USER_H_

#include "common/NAUserId.h"
#include "common/ComSmallDefs.h"
#include "cli/sqlcli.h"
#include <vector>

class ComDiagsArea;

// ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// class:  ComUser
//
// Class that manages authorization IDs
//
// Authorization IDs consist of users, roles, special IDs (PUBLIC and _SYSTEM),
// and groups
// ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
class ComUser {
 public:
  // AuthTypeChar lists the types of authorization IDs currently supported
  enum AuthTypeChar {
    AUTH_PUBLIC = 'P',
    AUTH_SYSTEM = 'S',
    AUTH_USER = 'U',
    AUTH_ROLE = 'R',
    AUTH_GROUP = 'G',
    AUTH_TENANT = 'T',
    AUTH_UNKNOWN = '?'
  };

  // static accessors
  static Int32 getCurrentUser();
  static const char *getCurrentUsername();
  static const char *getCurrentExternalUsername();
  static bool isManagerUserID(Int32 userID);
  static bool isManagerUser() { return isManagerUserID(getCurrentUser()); };
  static Int32 getSessionUser();
  static const char *getSessionUsername();
  static bool isRootUserID();
  static bool isRootUserID(Int32 userID);
  static bool isPublicUserID(Int32 userID) { return (userID == PUBLIC_USER); };
  static bool isSystemUserID(Int32 userID) { return (userID == SYSTEM_USER); };
  static bool isUserID(Int32 userID) { return (userID >= MIN_USERID && userID <= MAX_USERID); };
  static Int32 getRootUserID() { return SUPER_USER; }
  static Int32 getPublicUserID() { return PUBLIC_USER; }
  static Int32 getSystemUserID() { return SYSTEM_USER; }
  static const char *getRootUserName() { return DB__ROOT; }
  static const char *getPublicUserName() { return PUBLIC_AUTH_NAME; }
  static const char *getSystemUserName() { return SYSTEM_AUTH_NAME; }
  static char getAuthType(Int32 authID);
  static Int16 getUserNameFromUserID(Int32 userID, char *userName, Int32 maxLen, Int32 &actualLen, ComDiagsArea *diags);
  static Int16 getUserIDFromUserName(const char *userName, Int32 &userID, ComDiagsArea *diags);
  static Int16 getAuthNameFromAuthID(Int32 authID, char *authName, Int32 maxLen, Int32 &actualLen,
                                     NABoolean clearDiags = FALSE, ComDiagsArea *diags = NULL);
  static Int16 getAuthIDFromAuthName(const char *authName, Int32 &authID, ComDiagsArea *diags);
  static bool currentUserHasRole(Int32 roleID, ComDiagsArea *diags = NULL, bool resetRoleList = false);
  static Int16 getCurrentUserRoles(NAList<Int32> &roleIDs, ComDiagsArea *diags);
  static Int16 getCurrentUserRoles(NAList<Int32> &roleIDs, NAList<Int32> &granteeIDs, ComDiagsArea *diags);
  static Int16 getUserGroups(const char *userName, bool allowMDAccess, NAList<Int32> &groupIDs, ComDiagsArea *diags);
  static bool currentUserHasElevatedPrivs(ComDiagsArea *diags);
  static Int16 userHasElevatedPrivs(const Int32 userID, const std::vector<int32_t> &roleIDs, bool &hasElevatedPrivs,
                                    ComDiagsArea *diags);
  static Int16 getSystemRoleList(char *roleList, Int32 &actualLen, const Int32 maxLen, const char delimiter = '\'',
                                 const char separator = ',', const bool includeSpecialAuths = false);
  static bool roleIDsCached();

 private:
  // default constructor
  ComUser();
};

class ComTenant {
 public:
  static Int32 getCurrentTenantID();
  static NAString getCurrentTenantIDAsString();
  static const char *getCurrentTenantName();
  static const char *getSystemTenantName() { return DB__SYSTEMTENANT; };
  static const Int32 getSystemTenantID() { return SYSTEM_TENANT_ID; };
  static const bool isSystemTenant(Int32 tenantID) { return (tenantID == SYSTEM_TENANT_ID); };
  static const Int32 getTenantAffinity() { return 0; };     // TBD
  static const Int32 getTenantNodeSize() { return 0; };     // TBD
  static const Int32 getTenantClusterSIze() { return 0; };  // TBD
  static const bool isTenantNamespace(const char *ns);

 private:
  ComTenant();
};
#endif
