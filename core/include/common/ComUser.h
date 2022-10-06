

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

#include <vector>

#include "cli/sqlcli.h"
#include "common/ComSmallDefs.h"
#include "common/NAUserId.h"

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
  static int getCurrentUser();
  static const char *getCurrentUsername();
  static const char *getCurrentExternalUsername();
  static bool isManagerUserID(int userID);
  static bool isManagerUser() { return isManagerUserID(getCurrentUser()); };
  static int getSessionUser();
  static const char *getSessionUsername();
  static bool isRootUserID();
  static bool isRootUserID(int userID);
  static bool isPublicUserID(int userID) { return (userID == PUBLIC_USER); };
  static bool isSystemUserID(int userID) { return (userID == SYSTEM_USER); };
  static bool isUserID(int userID) { return (userID >= MIN_USERID && userID <= MAX_USERID); };
  static int getRootUserID() { return SUPER_USER; }
  static int getPublicUserID() { return PUBLIC_USER; }
  static int getSystemUserID() { return SYSTEM_USER; }
  static const char *getRootUserName() { return DB__ROOT; }
  static const char *getPublicUserName() { return PUBLIC_AUTH_NAME; }
  static const char *getSystemUserName() { return SYSTEM_AUTH_NAME; }
  static char getAuthType(int authID);
  static Int16 getUserNameFromUserID(int userID, char *userName, int maxLen, int &actualLen, ComDiagsArea *diags);
  static Int16 getUserIDFromUserName(const char *userName, int &userID, ComDiagsArea *diags);
  static Int16 getAuthNameFromAuthID(int authID, char *authName, int maxLen, int &actualLen,
                                     NABoolean clearDiags = FALSE, ComDiagsArea *diags = NULL);
  static Int16 getAuthIDFromAuthName(const char *authName, int &authID, ComDiagsArea *diags);
  static bool currentUserHasRole(int roleID, ComDiagsArea *diags = NULL, bool resetRoleList = false);
  static Int16 getCurrentUserRoles(NAList<int> &roleIDs, ComDiagsArea *diags);
  static Int16 getCurrentUserRoles(NAList<int> &roleIDs, NAList<int> &granteeIDs, ComDiagsArea *diags);
  static Int16 getUserGroups(const char *userName, bool allowMDAccess, NAList<int> &groupIDs, ComDiagsArea *diags);
  static bool currentUserHasElevatedPrivs(ComDiagsArea *diags);
  static Int16 userHasElevatedPrivs(const int userID, const std::vector<int32_t> &roleIDs, bool &hasElevatedPrivs,
                                    ComDiagsArea *diags);
  static Int16 getSystemRoleList(char *roleList, int &actualLen, const int maxLen, const char delimiter = '\'',
                                 const char separator = ',', const bool includeSpecialAuths = false);
  static bool roleIDsCached();

 private:
  // default constructor
  ComUser();
};

class ComTenant {
 public:
  static int getCurrentTenantID();
  static NAString getCurrentTenantIDAsString();
  static const char *getCurrentTenantName();
  static const char *getSystemTenantName() { return DB__SYSTEMTENANT; };
  static const int getSystemTenantID() { return SYSTEM_TENANT_ID; };
  static const bool isSystemTenant(int tenantID) { return (tenantID == SYSTEM_TENANT_ID); };
  static const int getTenantAffinity() { return 0; };     // TBD
  static const int getTenantNodeSize() { return 0; };     // TBD
  static const int getTenantClusterSIze() { return 0; };  // TBD
  static const bool isTenantNamespace(const char *ns);

 private:
  ComTenant();
};
#endif
