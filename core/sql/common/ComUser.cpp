/* -*-C++-*-
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

 *****************************************************************************
 *
 * File:         ComUser
 * Description:  Implements methods for user management
 *
 * Contains methods for classes:
 *   ComUser
 *   ComUserVerifyObj
 *   ComUserVerifyAuth
 *
 *****************************************************************************
 */

#define   SQLPARSERGLOBALS_FLAGS        // must precede all #include's
#define   SQLPARSERGLOBALS_NADEFAULTS

#include <algorithm>

#include "ComUser.h"
#include "CmpSeabaseDDL.h"
#include "CmpDDLCatErrorCodes.h"

#include "cli/SQLCLIdev.h"
#include "executor/ExExeUtilCli.h"
#include "cli/Context.h"

// ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// Class ComUser methods
// ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

// ----------------------------------------------------------------------------
// Constructors
// ----------------------------------------------------------------------------
ComUser::ComUser()
{ }

// ----------------------------------------------------------------------------
// method:  getCurrentUser
//
// Returns currentUser from the Cli context
// ----------------------------------------------------------------------------
Int32 ComUser::getCurrentUser(void)
{
 // If not initalized yet, call InitGlobals, asserts if not successful
  if (GetCliGlobals() == NULL)
    SQL_EXEC_InitGlobals();
  
  Int32 *pUserID = GetCliGlobals()->currContext()->getDatabaseUserID();
  return *pUserID;
}

// ----------------------------------------------------------------------------
// method:  getCurrentUsername
//
// Returns a pointer to the current database username
// ----------------------------------------------------------------------------
const char * ComUser::getCurrentUsername()
{
  // If not initalized yet, call InitGlobals, asserts if not successful
  if (GetCliGlobals() == NULL)
    SQL_EXEC_InitGlobals();

  return GetCliGlobals()->currContext()->getDatabaseUserName();
}

// ----------------------------------------------------------------------------
// method:  getCurrentExternalUsername
//
// Returns a pointer to the current database username
// ----------------------------------------------------------------------------
const char * ComUser::getCurrentExternalUsername()
{
  // If not initalized yet, call InitGlobals, asserts if not successful
  if (GetCliGlobals() == NULL)
    SQL_EXEC_InitGlobals();

  return GetCliGlobals()->currContext()->getExternalUserName();
}

// ----------------------------------------------------------------------------
// method:  isManagerUserID
//
// Returns is an admin user?
// ----------------------------------------------------------------------------
bool ComUser::isManagerUserID(Int32 uid)
{
    if ((ADMIN_USER_ID == uid) || (ROOT_USER_ID == uid))
        return true;
    //TODO: Supports checking root role
    //return currentUserHasRole(ROOT_ROLE_ID);
    return false;
}

// ----------------------------------------------------------------------------
// method:  getSessionUser
//
// Returns sessionUser from the Cli context
// ----------------------------------------------------------------------------
Int32 ComUser::getSessionUser(void)
{
  // If not initalized yet, call InitGlobals, asserts if not successful
  if (GetCliGlobals() == NULL)
    SQL_EXEC_InitGlobals();

  Int32 *pSessionID = GetCliGlobals()->currContext()->getSessionUserID();
  return *pSessionID;
}

// ----------------------------------------------------------------------------
// method:  getSessionUsername
//
// Returns sessionUser from the Cli context
// ----------------------------------------------------------------------------
const char * ComUser::getSessionUsername(void)
{
  // If not initalized yet, call InitGlobals, asserts if not successful
  if (GetCliGlobals() == NULL)
    SQL_EXEC_InitGlobals();

  // At this time, the session user is the same as the database user
  // When invoker and definer rights is implemented, this will change
  return GetCliGlobals()->currContext()->getDatabaseUserName();
}

// ----------------------------------------------------------------------------
// method:  isRootUserID
//
// Returns true if current userID is the root user
// ----------------------------------------------------------------------------
bool ComUser::isRootUserID()
{
  return isRootUserID(getCurrentUser());
}

// ----------------------------------------------------------------------------
// method:  isRootUserID
//
// Returns true if passed in userID is the root user
// ----------------------------------------------------------------------------
bool ComUser::isRootUserID(Int32 userID)
{
  return (userID == SUPER_USER) ? true : false;
}

// ----------------------------------------------------------------------------
// method: getAuthType
//
// returns the type of authorization ID given the passed in authID
// ----------------------------------------------------------------------------
char ComUser::getAuthType(Int32 authID)
{

   if (authID == PUBLIC_USER)
      return AUTH_PUBLIC;

   if (authID == SYSTEM_USER)
      return AUTH_SYSTEM;

   if (authID >= MIN_USERID && authID <= MAX_USERID)
      return AUTH_USER;

   if (authID >= MAX_USERID && authID <= MAX_ROLEID)
      return AUTH_ROLE;

   if (authID >= MAX_ROLEID && authID <= MAX_TENANTID)
      return AUTH_TENANT;

   return AUTH_UNKNOWN;
}

// -----------------------------------------------------------------------
// getUserNameFromUserID
//
// Reads the USERS table to get the user name associated with the passed
// in userID
//
//   <userID>                        Int32                           In
//      is the numeric ID to be mapped to a name.
//
//   <userName>                      char *                          In
//      passes back the name that the numeric ID mapped to.  If the ID does
//      not map to a name, the ASCII equivalent of the ID is passed back.
//
//   <maxLen>                        Int32                           In
//      is the size of <authName>.
//
//   <actualLen>                     Int32 &                         Out
//      is the size of the auth name in the table.  If larger than <maxLen>,
//      caller needs to supply a larger buffer.
//
//    <diags>                       ComDiagsArea *                   Out
//      if not NULL, returns errors from the context's diags area
//
// Returns:
//   0             -- Found. User name written to userName. Length
//                    returned in actualLen.
//  -1             -- Unexpected error
// -----------------------------------------------------------------------
Int16 ComUser::getUserNameFromUserID(Int32 userID,
                                     char *userName,
                                     Int32 maxLen,
                                     Int32 &actualLen,
                                     ComDiagsArea *diags)
{
  // If not initalized yet, call InitGlobals, asserts if not successful
  if (GetCliGlobals() == NULL)
    SQL_EXEC_InitGlobals();
  
  actualLen = 0; 
  Int16 result = 0;
  RETCODE retcode = 
    GetCliGlobals()->currContext()->getAuthNameFromID(userID, userName,
                                                      maxLen, actualLen);
  
  if (retcode != SUCCESS)
  {
    if (diags)
      diags->mergeAfter(*(GetCliGlobals()->currContext()->getDiagsArea()));
    return -1;
  }
  
  // On success, CLI does not return the length of the string in
  // actualLen. This function however is supposed to return a value in
  // actualLen even when the mapping is successful. The value returned
  // should not account for a null terminator.
  if (result == 0)
    actualLen = strlen(userName);
  
  return result;
}

// ----------------------------------------------------------------------------
// method: getUserIDFromUserName
//
// Reads the AUTHS table to get the userID associated with the passed
// in user name
//
//  Returns:  
//    0 -- found
//   -1 -- unexpected error occurred, if diags is not NULL, returns
//         errors from the context's diags area
// ----------------------------------------------------------------------------
Int16 ComUser::getUserIDFromUserName(const char *userName, 
                                     Int32 &userID,
                                     ComDiagsArea *diags)
{
  // If not initalized yet, call InitGlobals, asserts if not successful
  if (GetCliGlobals() == NULL)
    SQL_EXEC_InitGlobals();

  RETCODE retcode =
    GetCliGlobals()->currContext()->getAuthIDFromName((char *)userName, userID);

  if (retcode != SUCCESS)
  {
    if (diags)
      diags->mergeAfter(*(GetCliGlobals()->currContext()->getDiagsArea()));
    return  -1;
  }

  if (userID < MIN_USERID || userID > MAX_USERID)
  {
    if (diags)
      *diags << DgSqlCode(-CAT_IS_NOT_CORRECT_AUTHID)
             << DgString0(userName)
             << DgString1("user");
    return -1;
  }

  return 0;
}


// ----------------------------------------------------------------------------
// method: getAuthIDFromAuthName
//
// Reads the AUTHS table to get the authID associated with the passed
// in authorization name
//
//  Returns:  0    -- found
//           -1    -- not found or unexpected error
//
// The passed in diags area is populated with an error if -1 is returned
// ----------------------------------------------------------------------------
Int16 ComUser::getAuthIDFromAuthName(const char *authName, 
                                     Int32 &authID,
                                     ComDiagsArea *diags)
{
  // If not initalized yet, call InitGlobals, asserts if not successful
  if (GetCliGlobals() == NULL)
    SQL_EXEC_InitGlobals();

  RETCODE retcode =
    GetCliGlobals()->currContext()->getAuthIDFromName((char *)authName, authID);

  if (retcode != SUCCESS)
  {
    if (diags)
      diags->mergeAfter(*(GetCliGlobals()->currContext()->getDiagsArea()));
    authID = NA_UserIdDefault;
    return -1;
  }
 
  return 0;
}


// ----------------------------------------------------------------------------
//  method:      getAuthNameFromAuthID
//
//  Maps an integer authentication ID to a name.  If the number cannot be
//  mapped to a name, the numeric ID is converted to a string.
//
//   <authID>                        Int32                           In
//      is the numeric ID to be mapped to a name.
//
//   <authName>                      char *                          In
//      passes back the name that the numeric ID mapped to.  If the ID does
//      not map to a name, the ASCII equivalent of the ID is passed back.
//
//   <maxLen>                        Int32                           In
//      is the size of <authName>.
//
//   <actualLen>                     Int32 &                         Out
//      is the size of the auth name in the table.  If larger than <maxLen>,
//      caller needs to supply a larger buffer.
//
//   <clearDiags>                    NABoolean                      In
//      if TRUE, then the diags areas is cleared of any errors that may have
//      occurred during the lookup. 
//
//    <diags>                       ComDiagsArea *                   Out
//      if not NULL, returns errors from the context's diags area
//
//   Returns: Int16
//
//     0             -- Found. User name written to userName. Length
//                      returned in actualLen.
//    -1             -- Unexpected error or not found
//                      (returns the authID in string format)
// ----------------------------------------------------------------------------
Int16 ComUser::getAuthNameFromAuthID(Int32   authID,
                                     char  * authName,
                                     Int32   maxLen,
                                     Int32 & actualLen,
                                     NABoolean clearDiags,
                                     ComDiagsArea *diags)

{
  // If not initalized yet, call InitGlobals, asserts if not successful
  if (GetCliGlobals() == NULL)
    SQL_EXEC_InitGlobals();
  
  actualLen = 0;
  
  RETCODE retcode =
    GetCliGlobals()->currContext()->getAuthNameFromID(authID, authName, maxLen, actualLen);
  
  if (clearDiags && actualLen > 0)
    SQL_EXEC_ClearDiagnostics(NULL);

  if (retcode != SUCCESS)  
  {
    if (diags)
      diags->mergeAfter(*(GetCliGlobals()->currContext()->getDiagsArea()));
    return -1;
  }

  return 0;
}


// ----------------------------------------------------------------------------
// method: currentUserHasRole
//
// Searches the list of roles stored for the user to see if passed in role ID
// is found.  If resetRoleList is true, then metadata will be re-read to 
// retrieve the latest roles.
//
//  Returns:
//
//    true - role found
//    false - role not found
//
//    if diags is not NULL, returns errors from the context's diags area
// ----------------------------------------------------------------------------
bool ComUser::currentUserHasRole(Int32 roleID, 
                                 ComDiagsArea *diags,
                                 bool resetRoleList)
{
  // If not initalized yet, call InitGlobals, asserts if not successful
  if (GetCliGlobals() == NULL)
    SQL_EXEC_InitGlobals();

  // Get list of active roles from CLI cache
  Int32 numRoles = 0;
  Int32 *cachedRoleIDs = 0;
  Int32 *cachedGranteeIDs = 0;
  RETCODE retcode =
    GetCliGlobals()->currContext()->getRoleList(numRoles, cachedRoleIDs, cachedGranteeIDs);

  if (retcode != SUCCESS)
  {
    if (diags)
      diags->mergeAfter(*(GetCliGlobals()->currContext()->getDiagsArea()));
    return false;
  }

  for (Int32 i = 0; i < numRoles; i++)
  {
    Int32 userRole = cachedRoleIDs[i];
    if (userRole == roleID)
      return true;
  }

  // If the current user does not have the role, reset role list and try again
  if (resetRoleList)
  {
    SQL_EXEC_ResetRoleList_Internal();
    if (SQL_EXEC_GetRoleList(numRoles, cachedRoleIDs, cachedGranteeIDs) < 0)
      return false;

    for (Int32 i = 0; i < numRoles; i++)
    {
      Int32 userRole = cachedRoleIDs[i];
      if (userRole == roleID)
        return true;
    }
  }

  return false;
}

// ----------------------------------------------------------------------------
// method: getCurrentUserRoles
//
// Gets the active roles for the current user as a list of Int32's
// There should be at least one role in this list (PUBLIC)
//
//  Returns:
//
//   -1 -- unexpected error getting roles, if diags is not NULL, returns
//         errors from the context's diags area
//    0 -- successful
// ----------------------------------------------------------------------------
Int16 ComUser::getCurrentUserRoles(NAList <Int32> &roleIDs,
                                   ComDiagsArea *diags)
{

  // If not initalized yet, call InitGlobals, asserts if not successful
  if (GetCliGlobals() == NULL)
    SQL_EXEC_InitGlobals();

  Int32 numRoles = 0;
  Int32 *cachedRoleIDs = NULL;
  Int32 *cachedGranteeIDs = NULL;
  RETCODE retcode =
    GetCliGlobals()->currContext()->getRoleList(numRoles, cachedRoleIDs, cachedGranteeIDs);

  if (retcode != SUCCESS)
  {
    if (diags)
      diags->mergeAfter(*(GetCliGlobals()->currContext()->getDiagsArea()));
    return -1;
  }

  // There should be at least 1 role assigned (PUBLIC)
  if (numRoles == 0)
  {
    // Add a warning that roles were not considered
    if (diags)
      *diags << DgSqlCode(CAT_UNABLE_TO_RETRIEVE_ROLES);
    return 0;
  }

  for (Int32 i = 0; i < numRoles; i++)
  {
    // could be duplicates if role comes from user and user's groups
    if (!roleIDs.contains(cachedRoleIDs[i]))
      roleIDs.insert (cachedRoleIDs[i]);
  }
  return 0;
}


// ----------------------------------------------------------------------------
// method: getCurrentUserRoles
//
// Gets the active roles and grantees for the current user as a list of Int32's
// There should be at least one role in this list (PUBLIC)
//
// Returns:
//   -1 -- unexpected error getting roles, if diags is not NULL, returns
//         errors from the context's diags area
//    0 -- successful
// ----------------------------------------------------------------------------
Int16 ComUser::getCurrentUserRoles(NAList<Int32> &roleIDs, 
                                   NAList<Int32> &granteeIDs,
                                   ComDiagsArea *diags)
{
  // If not initalized yet, call InitGlobals, asserts if not successful
  if (GetCliGlobals() == NULL)
    SQL_EXEC_InitGlobals();

  Int32 numRoles = 0;
  Int32 *cachedRoleIDs = NULL;
  Int32 *cachedGranteeIDs = NULL;
  RETCODE retcode =
    GetCliGlobals()->currContext()->getRoleList(numRoles, cachedRoleIDs, cachedGranteeIDs);

  if (retcode != SUCCESS)
  {
   if (diags)
      diags->mergeAfter(*(GetCliGlobals()->currContext()->getDiagsArea()));
    return -1;
  }

  // There should be at least 1 role assigned (PUBLIC)
  if (numRoles == 0)
  {
    // Add a warning that roles were not considered
    if (diags)
      *diags << DgSqlCode(CAT_UNABLE_TO_RETRIEVE_ROLES);
    return 0;
  }

  for (Int32 i = 0; i < numRoles; i++)
  {
    roleIDs.insert (cachedRoleIDs[i]);
    granteeIDs.insert (cachedGranteeIDs[i]);
  }

  return 0;
}

// ----------------------------------------------------------------------------
// method: getUserGroups
//
// returns the list of groupIDs for the requested user
//
// if unexpected issue occurred trying to get groups, 
//   -1 is returned and ComDiags is setup with error.
// ----------------------------------------------------------------------------
Int16 ComUser::getUserGroups (
  const char * authName, 
  bool allowMDAccess,
  NAList <Int32> &groupIDs,
  ComDiagsArea *diags)
{
  ULng32 flagBits = GetCliGlobals()->currContext()->getSqlParserFlags();

  if (allowMDAccess)
    GetCliGlobals()->currContext()->setSqlParserFlags(0x20000);

  std::vector<int32_t> groupList;
  Int16 retcode = CmpSeabaseDDLauth::getUserGroups(authName, groupList);

  if (allowMDAccess)
    GetCliGlobals()->currContext()->assignSqlParserFlags(flagBits);

  if (retcode != 0)
  {
    if (diags)
      diags->mergeAfter(*(GetCliGlobals()->currContext()->getDiagsArea()));
    return retcode;
  }

  for (size_t i = 0; i < groupList.size(); i++)
    groupIDs.insert (groupList[i]);

  return 0;
}

// ----------------------------------------------------------------------------
// method: getSystemRoleList
//
// Returns the list of system roles
// Params:
//   (out) roleList - the list of roles, space is managed by the caller
//   (out) actualLen - the length of the returned role list
//   ( in) maxLen - the size of the roleList allocated by the caller
//   ( in) delimited - delimiter to use (defaults to single quote)
//   ( in) separator - specified what separator to use (defaults to comma)
//   ( in) includeSpecialAuths - includes the special auths (PUBLIC and _SYSTEM) 
//
//  Returns:  0    -- found
//           -1    -- space allocated for role list is too small 
// ----------------------------------------------------------------------------
Int16 ComUser::getSystemRoleList (
  char * roleList,
  Int32 &actualLen,
  const Int32 maxLen,
  const char delimiter,
  const char separator,
  const bool includeSpecialAuths)
{
  assert (roleList);
  Int32 numberRoles = sizeof(systemRoles)/sizeof(SystemAuthsStruct);
  NAString generatedRoleList;
  char roleName[MAX_AUTHNAME_LEN + 4];
  char currentSeparator = ' ';
  for (Int32 i = 0; i < numberRoles; i++)
  {
    const SystemAuthsStruct &roleDefinition = systemRoles[i];
    if (!includeSpecialAuths && roleDefinition.isSpecialAuth)
      continue;
 
    // str_sprintf does not support the %c format
    sprintf(roleName, "%c%c%s%c",
                currentSeparator, delimiter, roleDefinition.authName, delimiter);
    generatedRoleList += roleName;
    currentSeparator = separator;
  }
 
  actualLen = generatedRoleList.length();
  if (actualLen > maxLen)
    return -1;
 
  str_cpy_all(roleList, generatedRoleList.data(), actualLen);
  roleList[actualLen] = 0; // null terminate string
  return 0;
}

// ----------------------------------------------------------------------------
// method: currentUserHasElevatedPrivs
//
//  returns:
//     true  the current user is elevated
//     false otherwise
//
//   (for unexpected errors return errors from the context's diags area)
// ----------------------------------------------------------------------------
bool ComUser::currentUserHasElevatedPrivs(ComDiagsArea *diags)
{
  Int32 userID = getCurrentUser();
  if ( userID == ROOT_USER_ID || userID == ADMIN_USER_ID ||
       currentUserHasRole(ROOT_ROLE_ID, diags) )
    return true;

  Int32 adminRoleID = NA_UserIdDefault;
  Int16 retcode = ComUser::getAuthIDFromAuthName(DB__ADMINROLE, adminRoleID, diags);

  // DB__ADMINROLE should always exist
  if (retcode != 0)
  {
    if (diags)
      diags->mergeAfter(*(GetCliGlobals()->currContext()->getDiagsArea()));
    return false;
  }

  if (currentUserHasRole(adminRoleID, diags))
    return true;
  return false;
}

// ----------------------------------------------------------------------------
// method: userHasElevatedPrivs
//
//    <diags>                       ComDiagsArea *                   Out
//      if not NULL, returns errors from the context's diags area
//
//  returns:
//    hasElevatedPrivs:
//      true - has elevated privileges, false otherwise
//
//    0 -- success
//   -1 -- unexpected error occurred, if diags is not NULL, returns
//         errors from the context's diags area
// ----------------------------------------------------------------------------
Int16 ComUser::userHasElevatedPrivs(
  const Int32 userID,
  const std::vector <int32_t> &roleIDs,
  bool &hasElevatedPrivs,
  ComDiagsArea *diags)
{
  hasElevatedPrivs = false;

  if ( userID == ROOT_USER_ID || userID == ADMIN_USER_ID ||
       std::find (roleIDs.begin(), roleIDs.end(), ROOT_ROLE_ID) != roleIDs.end() )
  {
    hasElevatedPrivs = true;
    return 0;
  }

  Int32 adminRoleID = NA_UserIdDefault;
  Int16 retcode = ComUser::getAuthIDFromAuthName(DB__ADMINROLE, adminRoleID, diags);
  if (retcode != 0)
    return -1;

  if (std::find (roleIDs.begin(), roleIDs.end(), adminRoleID) != roleIDs.end())
  {
    hasElevatedPrivs = true;
    return 0;
  }
  return 0;
}


// ----------------------------------------------------------------------------
// method: roleIDsCached
//
// returns:
//    true - roles are cached, false - roles are not cached
// ----------------------------------------------------------------------------
bool ComUser::roleIDsCached()
{
 // If not initalized yet, call InitGlobals, asserts if not successful
  if (GetCliGlobals() == NULL)
    SQL_EXEC_InitGlobals();

  return GetCliGlobals()->currContext()->roleIDsCached();
}

// ----------------------------------------------------------------------------
// method:  getCurrentTenantID
//
// Returns current tenant from the Cli context
// ----------------------------------------------------------------------------
Int32 ComTenant::getCurrentTenantID(void)
{
  // If not initalized yet, call InitGlobals, asserts if not successful
  if (GetCliGlobals() == NULL)
    SQL_EXEC_InitGlobals();

  return GetCliGlobals()->currContext()->getTenantID();
}

// ----------------------------------------------------------------------------
// method:  getCurrentTenantIDAsString
//
// Returns current tenant from the Cli context
// ----------------------------------------------------------------------------
NAString ComTenant::getCurrentTenantIDAsString()
{
  Int32 tenantID = getCurrentTenantID();
  assert((tenantID >= MIN_TENANTID) && (tenantID <= MAX_TENANTID));

  char intStr[20];
  NAString ns = str_itoa(tenantID, intStr);

  return ns;
}

// ----------------------------------------------------------------------------
// method:  getCurrentTenantName
//
// Returns a pointer to the current tenant name
// ----------------------------------------------------------------------------
const char *ComTenant::getCurrentTenantName()   
{
  // If not initalized yet, call InitGlobals, asserts if not successful
  if (GetCliGlobals() == NULL)
    SQL_EXEC_InitGlobals();

  return GetCliGlobals()->currContext()->getTenantName();   
}

// -----------------------------------------------------------------------------
// method:  isTenantNamespace
//
// Returns true if namespace has tenant format, false otherwise
// tenant format: TRAF_<num>  where <num> between MIN_TENANTID and MAX_TENANTID
// -----------------------------------------------------------------------------
const bool ComTenant::isTenantNamespace(const char *ns)
{
  // ns should already be upper cased and have extra spaces removed
  NAString nsStr(ns);

  // if ns does not start with TRAF_RESERVED_NAMESPACE_PREFIX, return false
  if (nsStr.length() >= TRAF_NAMESPACE_PREFIX_LEN)
  {
    NAString nas(nsStr(0, TRAF_NAMESPACE_PREFIX_LEN));
    if (nas != TRAF_NAMESPACE_PREFIX)
      return false;
  }
  else
    return false;

  // convert text after TRAF_RESERVED_NAMESPACE_PREFIX to numeric
  // str_atoi returns -1 if contains non numeric characters
  nsStr = nsStr.remove(0, TRAF_NAMESPACE_PREFIX_LEN);
  Int32 nsValue  = str_atoi(nsStr.data(), nsStr.length()); 
  if (nsValue < MIN_TENANTID || nsValue > MAX_TENANTID)
    return false;
  return true;
}
