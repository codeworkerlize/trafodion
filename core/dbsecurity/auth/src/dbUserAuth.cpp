//******************************************************************************
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
//******************************************************************************

// LCOV_EXCL_START -- lets be a little paranoid and not let any in-line funcs
//                    from the header files slip into the coverage count

#include "dbUserAuth.h"
#include "token.h"
#include "tokenkey.h"
#include "ldapconfignode.h"
#include "ComEncryption.h"

#include <stdio.h>
#include <stddef.h>
#include <stdlib.h>
#include <limits.h>
#include <string.h>
#include <string>
#include <vector>
#include <set>
#include <exception>
#include <ctype.h>
#include <chrono>

using namespace std::chrono;


#include <iostream>
#include <sys/types.h>
#include "../../../sqf/inc/cextdecs/cextdecs.h"

// Used to disable and enable dumps via setrlimit in a release env.
#ifndef DBSECURITY_DEBUG
#include <sys/resource.h>
#endif

#include "authEvents.h"
#include "ldapconfignode.h"

//#include "common/evl_sqlog_eventnum.h"
#include "sqevlog/evl_sqlog_writer.h"
#include "seabed/int/types.h"
#include "seabed/pctl.h"
#include "seabed/ms.h"
#include "seabed/fserr.h"
class ComDiagsArea;
class Queue;
#include "SQLCLIdev.h"
#ifndef Lng32
typedef int             Lng32;
#endif
#include "ComSmallDefs.h"
#include "NAUserId.h"
#include "ExExeUtilCli.h"
#include "DefaultConstants.h"
#include "CmpCommon.h"
#include "cextdecs.h"

#include "common/sq_license.h"

#define NA_JulianTimestamp()            JULIANTIMESTAMP(0,0,0,-1)

enum {INTERNAL_QUERY_FROM_EXEUTIL   = 0x20000};
// LCOV_EXCL_STOP

#define ZFIL_ERR_BADPARMVALUE 590
#define ZFIL_ERR_BOUNDSERR 22
#define ZFIL_ERR_OK 0
#define ZFIL_ERR_TENANT 2
#define ZFIL_ERR_SECVIOL 48
#define ZFIL_ERR_REGISTER 1076
#ifndef SEABASE_AUTHS
#define SEABASE_AUTHS            "AUTHS"
#endif 

using namespace std;

class UserCacheContents;
class ClientContents;

static void authenticateUser(
   DBUserAuthContents  & self,
   const char          * username,
   const char          * password,
   const char          * tenantName,
   ClientContents      & clientInfo,
   AUTHENTICATION_INFO & authenticationInfo,
   PERFORMANCE_INFO    & performanceInfo);
   
#if 0
void cacheUserInfo(
   const char    * username,
   const char    * databaseUsername,
   int32_t         userID,
   bool            isValid,
   int64_t         redefTime);   
#endif
   
static LDAuthStatus executeLDAPAuthentication(
   DBUserAuthContents &            self,
   const char *                    username,
   const char *                    password,
   short                           configNumber,
   PERFORMANCE_INFO &              performanceInfo);

inline static const UserCacheContents * fetchFromCacheByUsername(const char * username);

inline static const UserCacheContents * fetchFromCacheByUserID(long userID);

static UA_Status findConfigSection(
  int32_t numGroups,
  const SQLSEC_GroupInfo * groupList,
  const char * extUsername,
  std::vector<AuthEvent> & authEvents,
  short & configNumber,
  std::string &configName);

static void logAuthEvents(std::vector<AuthEvent>  & authEvents);

static std::string getConfigName(const short configNumber);

static void logAuthenticationOutcome(
   const char             * external_user_name,
   const char             * internal_user_name,
   const int32_t            user_id,
   const int32_t            configNumber,
   const std::set<string> & groupList,
   ClientContents         & clientInfo,
   const AUTH_OUTCOME       outcome,
   int32_t                  sqlError = 0);
                                    
static void logAuthenticationRetries(
   int          nodeID,
   const char * username);
 
static void logToEventTable(
   int                    nodeID,
   DB_SECURITY_EVENTID    eventID, 
   posix_sqlog_severity_t severity, 
   const char *           msg);                                    

static void prepareUserNameForQuery(
   char       *nameForQuery,
   const char *extName);

static void stripAllTrailingBlanks(
   char   * buf,
   size_t   len);

static void timeDiff (
   struct timespec t1,
   struct timespec t2,
   struct timespec &tDiff);
   
inline static void upshiftString(
   const char * inputString,
   char       * outputString); 
   
static void copyUsersInfo(
   USERS_INFO & target,
   const USERS_INFO &source);

static void writeLog(const char * text);     

class ClientContents
{
public:
   char            clientName[257];
   char            clientUserName[257];
   char            applicationName[257];
};


class BlackBox
{
public:
   USERS_INFO      usersInfo;
   int             error;   //ACH do we need this?
   UA_Status       errorDetail;
   bool            isAuthenticated;
};

class DBUserAuthContents
{
public:
   BlackBox                   bb;
   int                        nodeID;
   ComAuthenticationType      authenticationType;
   bool                       authorizationEnabled;
   std::vector<AuthEvent>     authEvents;

   int bypassAuthentication(
      AUTHENTICATION_INFO & authenticationInfo);

   std::vector<AuthEvent> &getAuthEvents() { return authEvents; }

   void deserialize(Token &token);
   void reset();
};

class UserCacheContents
{
public:
   char            externalUsername[257];
   char            databaseUsername[257];
   int32_t         userID;
   bool            isValid;
   int64_t         redefTime;
};

static std::vector<UserCacheContents> userCache;

static DBUserAuth * me = NULL;

//static class member function from old source can't get member of class DBUserAuth
static ComAuthenticationType gAuthType = COM_NOT_AUTH;

#pragma page "DBUserAuth::CloseConnection"
// *****************************************************************************
// *                                                                           *
// * Function: DBUserAuth::CloseConnection                                     *
// *                                                                           *
// *    Closes any open LDAP connections.                                      *
// *                                                                           *
// *****************************************************************************

void DBUserAuth::CloseConnection()

{

   LDAPConfigNode::CloseConnection();

}
//******************** End of DBUserAuth::CloseConnection **********************

#pragma page "DBUserAuth::FreeInstance"
// *****************************************************************************
// *                                                                           *
// * Function: DBUserAuth::FreeInstance                                        *
// *                                                                           *
// *    Destroys the singleton instance of a DBUserAuth object.                *
// *                                                                           *
// *****************************************************************************

void DBUserAuth::FreeInstance()

{

   if (me != NULL)
      delete me;

}
//********************* End of DBUserAuth::FreeInstance *************************

#pragma page "DBUserAuth::GetInstance"
// *****************************************************************************
// *                                                                           *
// * Function: DBUserAuth::GetInstance                                         *
// *    Constructs or returns the singleton instance of a DBUserAuth object.   *
// *                                                                           *
// *****************************************************************************

DBUserAuth * DBUserAuth::GetInstance()

{

   if (me != NULL)
      return me;

   me = new DBUserAuth();
   return me;

}
//********************** End of DBUserAuth::GetInstance ************************

#pragma page "DBUserAuth::GetBlackBox"
// *****************************************************************************
// *                                                                           *
// * Function: DBUserAuth::GetBlackBox                                         *
// *                                                                           *
// *    Returns the black box data associated with this session.  If there is  *
// * no black box associated with this session, an empty string is returned.   *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <blackBox>                      char *                          Out      *
// *    is the external username (not the database username).                  *
// *                                                                           *
// *  <pretendTokenIsNull>            bool                            In       *
// *    is a minivan boolean used in testing.                                  *
// *                                                                           *
// *****************************************************************************
void DBUserAuth::GetBlackBox(
   char *blackBox,
   bool  pretendTokenIsNull)

{

Token *token = Token::Obtain();

//
// The only way Token::Obtain could return a NULL is if we were unable to
// allocate memory (a couple hundred bytes), which either means there is a
// serious memory leak somewhere or the heap is corrupted.  Neither is very
// likely, but good coding practice dictates we check for NULL anyway.  So
// why would we ever want to pretend the token is NULL?  Just to take a
// different branch and execute a few lines?  Count on it.
//

   if (pretendTokenIsNull)
      token = NULL;

//
// If token is NULL, either real or pretend, return an empty black box.
//

   if (token == NULL)
   {
      blackBox[0] = 0;
      return;
   }

   token->getData(blackBox);

}
//********************** End of DBUserAuth::GetBlackBox ************************


#pragma page "DBUserAuth::GetBlackBoxSize"
// *****************************************************************************
// *                                                                           *
// * Function: DBUserAuth::GetBlackBoxSize                                     *
// *    Returns the size of the black box data associated with this session.   *
// * If there is no black box associated with this session, zero is returned.  *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameter:                                                               *
// *                                                                           *
// *  <pretendTokenIsNull>            bool                            In       *
// *    is a minivan boolean used in testing.                                  *
// *                                                                           *
// *****************************************************************************
size_t DBUserAuth::GetBlackBoxSize(bool  pretendTokenIsNull)

{

Token *token = Token::Obtain();

   if (pretendTokenIsNull)
      token = NULL;

   if (token == NULL)
      return 0;

   return token->getDataSize();

}
//******************** End of DBUserAuth::GetBlackBoxSize **********************

#pragma page "DBUserAuth::FormatStatusMsg"
// *****************************************************************************
// *                                                                           *
// * Function: DBUserAuth::FormatStatusMsg                                     *
// *    Formats a status error or warning as a message.                        *
// *                                                                           *
// *                                                                           *
// * If there is no black box associated with this session, zero is returned.  *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <status>                        UA_Status                       In       *
// *    is the status code to be formatted.                                    *
// *                                                                           *
// *  <statusMsg>                     char *                          Out      *
// *    returns the formatted message.                                         *
// *                                                                           *
// *****************************************************************************
void DBUserAuth::FormatStatusMsg(
   UA_Status status,
   char *    statusMsg)

{

   switch (status)
   {
      case UA_STATUS_OK:
         strcpy(statusMsg,"OK, no warnings");
         break;
      case UA_STATUS_ERR_INVALID:
         strcpy(statusMsg,"Username or password is invalid");
         break;
      case UA_STATUS_ERR_SYSTEM:
         strcpy(statusMsg,"Resource or internal error occurred");
         break;
      case UA_STATUS_ERR_REGISTER:
         strcpy(statusMsg,"User is not registered");
      case UA_STATUS_ERR_TENANT:
         strcpy(statusMsg,"Invalid tenant specified");
         break;
      case UA_STATUS_ERR_REGISTER_FAILED:
         strcpy(statusMsg,"Automatic user registration failed");
         break;
      case UA_STATUS_ERROR_SYSTEM_TENANT:
         strcpy(statusMsg,"Specifying system tenant is not allowed");
         break;
      default:
         strcpy(statusMsg,"Resource or internal error occurred");
         break;
   }

}
//******************** End of DBUserAuth::FormatStatusMsg **********************

#pragma page "DBUserAuth::CheckExternalGroupnameDefined"
// *****************************************************************************
// *                                                                           *
// * Function: DBUserAuth::CheckExternalGroupnameDefined                        *
// *                                                                           *
// *    Determines if an external groupname of the form expected by the         *
// *    identity is defined on the identity store.                              *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <externalGroupname>              const char *                    In       *
// *    is the external groupname to be checked on the identity.                *
// *                                                                           *
// *  <configurationOrdinal>          AuthenticationConfiguration     In       *
// *    specifies which configuration to use when searching for the groupname.  *
// *    If Unknown, LDAPConfigNode will use the default specified in the       *
// *    config file (.traf_authentication_config).                             *
// *                                                                           *
// *  <foundConfigurationOrdinal>     AuthenticationConfiguration &   Out      *
// *    passes back the configuration where the group was found, or zero if the *
// *    group was not found.                                                    *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: CheckUserResult                                                  *
// *                                                                           *
// *   GroupExists       -- Group was found on LDAP server                       *
// *   GroupDoesNotExist -- Group was not found on LDAP server                   *
// *   ErrorInCheck     -- An error prevented checking                         *
// *                                                                           *
// *****************************************************************************
DBUserAuth::CheckUserResult DBUserAuth::CheckExternalGroupnameDefined(
   const char * externalGroupname,
   const char * configurationName,
   short &      foundConfigurationNumber)

{

#ifdef __SQ_LDAP_NO_SECURITY_CHECKING
  return CheckUserResult_UserExists;
#else

   std::vector<AuthEvent> authEvents;

   if (COM_LDAP_AUTH != gAuthType)
   {
      foundConfigurationNumber = DefaultConfiguration;
      return UserExists;
   }

   LDAPConfigNode *searchNode;
   short configNumber = DefaultConfiguration;

   searchNode = LDAPConfigNode::GetLDAPConnection(authEvents,configurationName,configNumber,SearchConnection);

   if (searchNode == NULL)
   {
      if (authEventExists(authEvents, DBS_AUTH_CONFIG_SECTION_NOT_FOUND))
        return InvalidConfig;
      return ErrorDuringCheck;
   }

   LDSearchStatus searchStatus = searchNode->lookupGroup(authEvents,externalGroupname);

   if (searchStatus == LDSearchNotFound)
      return UserDoesNotExist;

   // didn't get "found" or "not found", so we have problems!
   if (searchStatus != LDSearchFound)
      return ErrorDuringCheck;

   // External Groupname was found.  But where?
   foundConfigurationNumber = searchNode->getConfigNumber();

   return UserExists;

#endif

}
//************* End of DBUserAuth::CheckExternalGroupnameDefined ***************

#pragma page "DBUsrAuth::CheckExternalUsernameDefinedConfigName"
// *****************************************************************************
// *                                                                           *
// * Function: DBUsrAuth::CheckExternalUsernameDefinedConfigName                   *
// *                                                                           *
// *    Determines if an external username of the form expected by the         *
// *    identity during authentication is defined on the identity store.       *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <externalUsername>              const char *                    In       *
// *    is the external username to be checked on the identity.                *
// *                                                                           *
// *  <configurationOrdinal>          AuthenticationConfiguration     In       *
// *    specifies which configuration to use when searching for the username.  *
// *    If Unknown, LDAPConfigNode will use the default specified in the       *
// *    config file (.traf_authentication_config).                             *
// *                                                                           *
// *  <foundConfigurationOrdinal>     AuthenticationConfiguration &   Out      *
// *    passes back the configuration where the user was found, or zero if the *
// *    user was not found.                                                    *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: CheckUserResult                                                  *
// *                                                                           *
// *   UserExists       -- User was found on LDAP server                       *
// *   UserDoesNotExist -- User was not found on LDAP server                   *
// *   ErrorInCheck     -- An error prevented checking                         *
// *                                                                           *
// *****************************************************************************
DBUserAuth::CheckUserResult DBUserAuth::CheckExternalUsernameDefinedConfigName(
   const char * externalUsername,
   const char * configurationName,
   short      & foundConfigurationNumber)

{

#ifdef __SQ_LDAP_NO_SECURITY_CHECKING
  return CheckUserResult_UserExists;
#else

   std::vector<AuthEvent> authEvents;

   if (COM_LDAP_AUTH != gAuthType)
   {
      foundConfigurationNumber = DefaultConfiguration;
      return UserExists;
   }

   string userDN = "";       // User DN, used to bind to LDAP serer
   LDAPConfigNode *searchNode;
   short configNumber = DefaultConfiguration;

   searchNode = LDAPConfigNode::GetLDAPConnection(authEvents,configurationName,configNumber,SearchConnection);

   if (searchNode == NULL)
   {
      if (authEventExists(authEvents, DBS_AUTH_CONFIG_SECTION_NOT_FOUND))
        return InvalidConfig;
      return ErrorDuringCheck;
   }

   LDSearchStatus searchStatus = searchNode->lookupUser(authEvents,externalUsername,userDN);

   if (searchStatus == LDSearchNotFound)
      return UserDoesNotExist;

   // didn't get "found" or "not found", so we have problems!
   if (searchStatus != LDSearchFound)
      return ErrorDuringCheck;

   // External username was found.  But where?
   foundConfigurationNumber = searchNode->getConfigNumber();

   return UserExists;

#endif

}
//******* End of DBUserAuth::CheckExternalUsernameDefinedConfigName ************

#pragma page "DBUsrAuth::CheckExternalUsernameDefinedConfigNum"
// *****************************************************************************
// *                                                                           *
// * Function: DBUsrAuth::CheckExternalUsernameDefinedConfigNum                   *
// *                                                                           *
// *    Determines if an external username of the form expected by the         *
// *    identity during authentication is defined on the identity store.       *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <externalUsername>              const char *                    In       *
// *    is the external username to be checked on the identity.                *
// *                                                                           *
// *  <configurationOrdinal>          AuthenticationConfiguration     In       *
// *    specifies which configuration to use when searching for the username.  *
// *    If Unknown, LDAPConfigNode will use the default specified in the       *
// *    config file (.traf_authentication_config).                             *
// *                                                                           *
// *  <foundConfigurationOrdinal>     AuthenticationConfiguration &   Out      *
// *    passes back the configuration where the user was found, or zero if the *
// *    user was not found.                                                    *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: CheckUserResult                                                  *
// *                                                                           *
// *   UserExists       -- User was found on LDAP server                       *
// *   UserDoesNotExist -- User was not found on LDAP server                   *
// *   ErrorInCheck     -- An error prevented checking                         *
// *                                                                           *
// *****************************************************************************
DBUserAuth::CheckUserResult DBUserAuth::CheckExternalUsernameDefinedConfigNum(
   const char * externalUsername,
   short        configurationNumber,
   string     & configurationName)

{

#ifdef __SQ_LDAP_NO_SECURITY_CHECKING
  return CheckUserResult_UserExists;
#else

   std::vector<AuthEvent> authEvents;
   configurationName = "";

   if (COM_LDAP_AUTH != gAuthType)
   {
      return UserExists;
   }

   string userDN = "";       // User DN, used to bind to LDAP serer
   LDAPConfigNode *searchNode;

   searchNode = LDAPConfigNode::GetLDAPConnection(authEvents,NULL,configurationNumber,SearchConnection);

   if (searchNode == NULL) 
   {
      if (authEventExists(authEvents, DBS_AUTH_CONFIG_SECTION_NOT_FOUND))
        return InvalidConfig;
      return ErrorDuringCheck;
   }

   LDSearchStatus searchStatus = searchNode->lookupUser(authEvents,externalUsername,userDN);

   if (searchStatus == LDSearchNotFound)
      return UserDoesNotExist;

   // didn't get "found" or "not found", so we have problems!
   if (searchStatus != LDSearchFound)
      return ErrorDuringCheck;

   // External username was found. Return back the configuration name 
   configurationName = searchNode->getConfigName();

   return UserExists;

#endif

}
//*********** End of DBUserAuth::CheckExternalUsernameDefinedConfig ************
#pragma page "DBUserAuth::CheckExternalUsernameDefined"
// *****************************************************************************
// *                                                                           *
// * Function: DBUsrAuth::CheckExternalUsernameDefined                         *
// *                                                                           *
// *    Determines if an external username of the form expected by the         *
// *    identity during authentication is defined on the identity store.       *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <externalUsername>              const char *                    In       *
// *    is the external username to be checked on the identity.                *
// *                                                                           *
// *  <configurationOrdinal>          AuthenticationConfiguration     In       *
// *    specifies which configuration to use when searching for the username.  *
// *    If Unknown, LDAPConfigNode will use the default specified in the       *
// *    config file (.traf_authentication_config).                             *
// *                                                                           *
// *  <foundConfigurationOrdinal>     AuthenticationConfiguration &   Out      *
// *    passes back the configuration where the user was found, or zero if the *
// *    user was not found.                                                    *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: CheckUserResult                                                  *
// *                                                                           *
// *   UserExists       -- User was found on LDAP server                       *
// *   UserDoesNotExist -- User was not found on LDAP server                   *
// *   ErrorInCheck     -- An error prevented checking                         *
// *                                                                           *
// *****************************************************************************
DBUserAuth::CheckUserResult DBUserAuth::CheckExternalUsernameDefined(
   const char * externalUsername,
   const char * configurationName,
   short &      foundConfigurationNumber)

{

#ifdef __SQ_LDAP_NO_SECURITY_CHECKING
  return CheckUserResult_UserExists;
#else

   std::vector<AuthEvent> authEvents;

   if (COM_LDAP_AUTH != gAuthType)
   {
      foundConfigurationNumber = DefaultConfiguration;
      return UserExists;
   }

   if (configurationName && strcmp(configurationName, "ALL") != 0)
      return CheckExternalUsernameDefinedConfigName(
                  externalUsername,
                  configurationName,
                  foundConfigurationNumber);
   else
   {
      LDAPConfigNode *searchNode;
      string userDN = "";       // User DN, used to bind to LDAP server
      short numSections = 0;
      LDAPConfigNode::GetNumberOfSections(authEvents, numSections);

      for (short confignum=0; confignum < numSections; confignum++)
      {

         searchNode = LDAPConfigNode::GetLDAPConnection(authEvents,NULL,confignum,SearchConnection);

         if (searchNode == NULL) 
            continue;

         LDSearchStatus searchStatus = searchNode->lookupUser(authEvents,externalUsername,userDN);

         if (searchStatus != LDSearchFound)
         {
            searchNode->CloseConnection();
            searchNode->FreeInstance(confignum,SearchConnection);
            continue;
         }

         // External username was found.
         foundConfigurationNumber = confignum;
         searchNode->CloseConnection();
         searchNode->FreeInstance(confignum,SearchConnection);
         return UserExists;

      }
      return UserDoesNotExist;
   }
#endif
}

#pragma page "void DBUserAuth::DBUserAuth"
// *****************************************************************************
// *                                                                           *
// * Function: DBUserAuth::DBUserAuth                                          *
// *    This function constructs a DBUserAuth object and its contents.         *
// *                                                                           *
// *****************************************************************************
DBUserAuth::DBUserAuth()
: self(*new DBUserAuthContents())

{

   self.reset();

char processName[MS_MON_MAX_PROCESS_NAME+1]; // +1 for trailing null
bool authorizationEnabled = false;
bool authorizationReady = false;
bool auditingEnabled = false;
int32_t cliRC = 0;

        
// obtain our node ID from Seabed
    msg_mon_get_my_info(&self.nodeID,              // nodeID
                        NULL,                      // mon process-id
                        processName,               // mon name
                        MS_MON_MAX_PROCESS_NAME,   // mon name-len
                        NULL,                      // mon process-type
                        NULL,                      // mon zone-id
                        NULL,                      // os process-id
                        NULL);                     // os thread-id
//
// Get authentication and authorization settings

   cliRC = SQL_EXEC_GetAuthState((Int32&)self.authenticationType,
                                 authorizationEnabled,
                                 authorizationReady,
                                 auditingEnabled);

   self.authorizationEnabled = authorizationEnabled && authorizationReady;
   self.authenticationType = CmpCommon::getAuthenticationType();
}
//********************** End of DBUserAuth::DBUserAuth *************************


#pragma page "void DBUserAuth::~DBUserAuth"
// *****************************************************************************
// *                                                                           *
// * Function: DBUserAuth::~DBUserAuth                                         *
// *    This function destroysa DBUserAuth object and its contents.            *
// *                                                                           *
// *****************************************************************************
DBUserAuth::~DBUserAuth()

{

   delete &self;

}
//********************** End of DBUserAuth::~DBUserAuth ************************


#pragma page "DBUserAuth::getAuthFunction"
// *****************************************************************************
// *                                                                           *
// * Function: DBUserAuth::getAuthFunction                                     *
// *                                                                           *
// *    Returns the token verification function set by MXOSRVR.                *
// *                                                                           *
// *****************************************************************************
AuthFunction DBUserAuth::getAuthFunction()

{

   return validateTokenFunc;

}
//******************* End of DBUserAuth::getAuthFunction ***********************

#pragma page "getConfigName"
// *****************************************************************************
// * Function: DBUserAuth::getConfigName                                       *
// *                                                                           *
// *    Returns the section name corresponding to the config number provided   *
// *                                                                           *
// *****************************************************************************
std::string DBUserAuth::getConfigName(short configNumber)
{

   std::vector<AuthEvent> authEvents;

   if (COM_LDAP_AUTH != gAuthType)
   {
      return "";
   }
   std::string configName;

   LDAPConfigNode::GetConfigName(authEvents, configNumber,configName);
   return configName;

}

#pragma page "DBUserAuth::getTenantID"
// *****************************************************************************
// *                                                                           *
// * Function: DBUserAuth::getTenantID                                         *
// *                                                                           *
// *    Returns the tenant ID for the user.                                    *
// *                                                                           *
// *****************************************************************************
int32_t DBUserAuth::getTenantID() const

{

   return self.bb.usersInfo.tenantID;

}
//************************ End of DBUserAuth::getTenantID **********************

#pragma page "DBUserAuth::getUserID"
// *****************************************************************************
// *                                                                           *
// * Function: DBUserAuth::getUserID                                           *
// *                                                                           *
// *    Returns the database user ID for the user.                             *
// *                                                                           *
// *****************************************************************************
int32_t DBUserAuth::getUserID() const

{

   return self.bb.usersInfo.effectiveUserID;

}
//************************ End of DBUserAuth::getUserID ************************

#pragma page "DBUserAuth::getDBUserName"
// *****************************************************************************
// *                                                                           *
// * Function: DBUserAuth::getDBUserName                                       *
// *                                                                           *
// *    Returns the database username for the user.                           *
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <databaseUsername>              char *                          Out      *
// *    returns the user name as a null terminated string.                     *
// *  <maxLen>                        size_t                          In       *
// *    maximum size of the null terminated string.                            *
// *                                                                           *
// *****************************************************************************
void DBUserAuth::getDBUserName(char *databaseUsername, size_t maxLen) const

{

   memset(databaseUsername, '\0', maxLen);
   strncpy(databaseUsername,self.bb.usersInfo.dbUsername, maxLen - 1);
   databaseUsername[maxLen -1] = '\0';

}
//********************** End of DBUserAuth::getDBUserName **********************

#pragma page "DBUserAuth::getExternalUsername"
// *****************************************************************************
// *                                                                           *
// * Function: DBUserAuth::getExternalUsername                                 *
// *                                                                           *
// *    Returns the external username for the user.                            *
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <externalUsername>              char *                          Out      *
// *    passes back the external username as a null terminated string.         *
// *                                                                           *
// *  <maxLen>                        size_t                          In       *
// *    maximum size of the null terminated string.                            *
// *                                                                           *
// *****************************************************************************
void DBUserAuth::getExternalUsername(
   char * externalUsername,
   size_t maxLen) const

{

   memset(externalUsername,'\0',maxLen);
   strncpy(externalUsername,self.bb.usersInfo.extUsername,maxLen - 1);
   externalUsername[maxLen - 1] = '\0';

}
//****************** End of DBUserAuth::getExternalUsername ********************


#pragma page "DBUserAuth::getGroupListForUser"
// *****************************************************************************
// *                                                                           *
// * Function: DBUserAuth::getGroupList                                        *
// *                                                                           *
// *    Returns the list of groups for the requested user.                     *
// *    If the requested user is the current user and self is not initialized  *
// *    then it is initialized.  This can occur when this function is called   *
// *    via sqlci.                                                             *
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <databaseUsername>       const char *                         In         *
// *    the database username                                                  *
// *                                                                           *
// *  <externalUsername>       const char *                         In         *
// *    the name used to connect to LDAP, usually the same as databaseUsername *
// *                                                                           *
// *  <currentUsername>        const char *                         In         *
// *    the user current executing the request                                 *
// *                                                                           *
// *  <userID>                 int32_t                              In         *
// *    the user ID for the requested user                                     *
// *                                                                           *
// *  <configNumber>           short                                In         *
// *    the AD/LDAP configuration section to look at                           *
// *                                                                           *
// *  <groupList>              set<string>                         Out         *
// *    passes back the groupList for the user                                 *
// *                                                                           *
// *****************************************************************************
DBUserAuth::CheckUserResult DBUserAuth::getGroupListForUser(
   const char *  databaseUsername,
   const char *  externalUsername,
   const char *  currentUsername,
   int32_t       userID,
   short         configNumber,
   std::set<std::string> & groupList) const
{
   std::vector<AuthEvent> authEvents;

   groupList.clear();
   char eventMsg[MAX_EVENT_MSG_SIZE];

   if (COM_LDAP_AUTH != self.authenticationType)
   {
      snprintf(eventMsg, MAX_EVENT_MSG_SIZE,
               "LDAP authentication is not enabled, no LDAP groups returned for user %s",externalUsername);
      insertAuthEvent(authEvents,DBS_LDAP_NOT_ENABLED,eventMsg, LL_ERROR);
      logAuthEvents(authEvents);
      return LDAPNotEnabled;
   }
   
   if (self.bb.isAuthenticated &&
       (strcmp(databaseUsername, currentUsername) == 0))
   {  
      getGroupList(groupList);
      return GroupListFound;
   }

   LDAPConfigNode *searchNode;

   searchNode = LDAPConfigNode::GetLDAPConnection(authEvents,NULL,configNumber,SearchConnection);

   if (searchNode == NULL)
   {
      if (authEvents.size() == 0)
      {
         snprintf(eventMsg, MAX_EVENT_MSG_SIZE,
                  "Unable to get group list for user %s, call to get connection failed",
                  externalUsername);
         insertAuthEvent(authEvents,DBS_NO_LDAP_AUTH_CONNECTION,eventMsg, LL_ERROR);
      }

      logAuthEvents(authEvents);

      if (authEventExists(authEvents, DBS_AUTH_CONFIG_SECTION_NOT_FOUND))
        return InvalidConfig;
      return ErrorDuringCheck;
   }

   LDSearchStatus status = searchNode->lookupGroupList(authEvents,externalUsername,groupList);
   if (status != LDSearchFound && status != LDSearchNotFound)
   {
      if (authEvents.size() == 0)
      {
         snprintf(eventMsg, MAX_EVENT_MSG_SIZE,
                  "Unable to get group list for user %s, call to lookup group list failed",
                  externalUsername);
         insertAuthEvent(authEvents,DBS_LDAP_SEARCH_ERROR,eventMsg, LL_ERROR);
      }

      logAuthEvents(authEvents);
      if (authEventExists(authEvents, DBS_AUTH_CONFIG_SECTION_NOT_FOUND))
        return InvalidConfig;
      return ErrorDuringCheck;
   }
  
   // If getting groups for the current user and the user is not authenticated
   // yet, store attributes in self.  Therefore, the next time the current user
   // gets its group list, can just return the stored list.
   if (!self.bb.isAuthenticated && 
       (strcmp(databaseUsername, currentUsername) == 0))
   {
      if (self.bb.usersInfo.dbUsername)
        delete self.bb.usersInfo.dbUsername;
      self.bb.usersInfo.dbUsername = new char [strlen(databaseUsername) + 1];
      strcpy(self.bb.usersInfo.dbUsername, databaseUsername);
      if (self.bb.usersInfo.extUsername)
        delete self.bb.usersInfo.extUsername;
      self.bb.usersInfo.extUsername = new char [strlen(externalUsername) + 1];
      strcpy(self.bb.usersInfo.extUsername, externalUsername);
      self.bb.usersInfo.effectiveUserID = userID;
      self.bb.usersInfo.sessionUserID = userID;
      self.bb.usersInfo.configNumber = configNumber; 

      std::set<std::string>::iterator it;
      for (it = groupList.begin(); it != groupList.end(); ++it)
         self.bb.usersInfo.groupList.insert(*it);

      self.bb.isAuthenticated = true;
   }

   return GroupListFound;
}
//****************** End of DBUserAuth::getGroupListForUser ********************

#pragma page "DBUserAuth::getGroupList"

// *****************************************************************************
// *                                                                           *
// * Function: DBUserAuth::getGroupList                                        *
// *                                                                           *
// *    Returns the list of groups for the user.                               *
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <groupList>              set<string>                         Out         *
// *    passes back the groupList for the user                                 *
// *                                                                           *
// *****************************************************************************
void DBUserAuth::getGroupList(
   set<string> & groupList) const

{

   groupList = self.bb.usersInfo.groupList;

}
//****************** End of DBUserAuth::getGroupList ***************************

#pragma page "DBUserAuth::getTenantName"
// *****************************************************************************
// *                                                                           *
// * Function: DBUserAuth::getTenantName                                       *
// *                                                                           *
// *    Returns the tenant name for the user.                                  *
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <tenantName>                    char *                          Out      *
// *    passes back the tenant name as a null terminated string.               *
// *                                                                           *
// *  <maxLen>                        size_t                          In       *
// *    maximum size of the null terminated string.                            *
// *                                                                           *
// *****************************************************************************
void DBUserAuth::getTenantName(
   char * tenantName,
   size_t maxLen) const

{

   memset(tenantName,'\0',maxLen);
   strncpy(tenantName,self.bb.usersInfo.tenantName,maxLen - 1);
   tenantName[maxLen - 1] = '\0';

}
//****************** End of DBUserAuth::getTenantName **************************


#pragma page "DBUserAuth::getTokenKeyAsString"
// *****************************************************************************
// *                                                                           *
// * Function: DBUserAuth::getTokenKeyAsString                                 *
// *    Returns the Token Key as a formatted ASCII string.                     *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <tokenKeyString>                char *                          Out      *
// *    returns the Token Key as a formatted ASCII string.                     *
// *                                                                           *
// *****************************************************************************

void DBUserAuth::getTokenKeyAsString(char *tokenKeyString) const

{

Token *token = Token::Obtain();

// LCOV_EXCL_START
   if (token == NULL)
   {
      tokenKeyString[0] = 0;
      return;
   }
// LCOV_EXCL_STOP

   token->getTokenKeyAsString(tokenKeyString);

}
//***************** End of DBUserAuth::getTokenKeyAsString *********************




#pragma page "Token::getTokenKeySize"
// *****************************************************************************
// *                                                                           *
// * Function: DBUserAuth::getTokenKeySize                                     *
// *                                                                           *
// *    Returns the token key size.  Only used in testing.                     *
// *                                                                           *
// *****************************************************************************
size_t DBUserAuth::getTokenKeySize() const

{

Token *token = Token::Obtain();

   if (token == NULL)
      return 0;
      
   return token->getTokenKeySize();

}
//******************* End of DBUserAuth:getTokenKeySize ************************


#pragma page "DBUserAuth::setAuthFunction"
// *****************************************************************************
// *                                                                           *
// * Function: DBUserAuth::setAuthFunction                                     *
// *                                                                           *
// *    Sets the token verification function.                                  *
// *                                                                           *
// *****************************************************************************
void DBUserAuth::setAuthFunction(AuthFunction somefunc)

{

   validateTokenFunc = somefunc;

}
//******************** End of DBUserAuth::setAuthFunction **********************


#pragma page "DBUserAuth::verify"
// *****************************************************************************
// *                                                                           *
// * Function: DBUserAuth::verify                                              *
// *                                                                           *
// *    Verifies a username and password on a database instance.               *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <username>                      const char *                    In       *
// *    is the external username (not the database username).                  *
// *                                                                           *
// *  <credentials>                   char *                          In/Out   *
// *    is the password for the user or a token that represents the user       *
// *  session.  On return is a token.                                          *
// *                                                                           *
// *  <errorDetail>                   UA_Status &                     Out      *
// *    passes back which input parameter is at fault on failure.              *
// *                                                                           *
// *  <authenticationInfo             AUTHENTICATION_INFO &           Out      *
// *    passes back user and error data related to the authentication.         *
// *                                                                           *
// *  <client_info>                   const CLIENT_INFO &             In       *
// *    is the client related information needed for audit logging.            *
// *                                                                           *
// *  <performanceInfo>               PERFORMANCE_INFO &              Out      *
// *    passes back elapsed time for authentication suboperations.             *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: int                                                              *
// *                                                                           *
// *   0 - Authentication completed.                                           *
// *  22 - Bounds error.  Number of param in error returned in errorDetail.    *
// * 590 - NULL param.  Number of param in error returned in errorDetail.      *
// *                                                                           *
// *****************************************************************************
int DBUserAuth::verify(
   const char           * externalUsername,        
   char                 * credentials,     
   const char           * tenantName,
   UA_Status            & errorDetail,     
   AUTHENTICATION_INFO  & authenticationInfo,  
   const CLIENT_INFO    & client_info,     
   PERFORMANCE_INFO     & performanceInfo) 


{

char buf[500];

   memset((void *)&performanceInfo, '\0', sizeof(performanceInfo));


   if (externalUsername == NULL)
   {
      errorDetail = UA_STATUS_PARAM1;
      return ZFIL_ERR_BADPARMVALUE;
   }

   if (credentials == NULL)
   {
      errorDetail = UA_STATUS_PARAM2;
      return ZFIL_ERR_BADPARMVALUE;
   }

   if (strlen(externalUsername) > MAX_EXTUSERNAME_LEN)
   {
      errorDetail = UA_STATUS_PARAM1;
      return ZFIL_ERR_BOUNDSERR;
   }

   if (strlen(credentials) > MAX_EXTPASSWORD_LEN)
   {
      errorDetail = UA_STATUS_PARAM2;
      return ZFIL_ERR_BOUNDSERR;
   }
   
static ClientContents clientInfo;

   memset((void *)&clientInfo, '\0', sizeof(clientInfo));

   if (client_info.client_name != NULL)
      strncpy(clientInfo.clientName,
              client_info.client_name,
              sizeof(clientInfo.clientName) - 1);
   if (client_info.client_user_name != NULL)
       strncpy(clientInfo.clientUserName,
               client_info.client_user_name,
               sizeof(clientInfo.clientUserName) - 1);
   if (client_info.application_name != NULL)
       strncpy(clientInfo.applicationName,
               client_info.application_name,
               sizeof(clientInfo.applicationName) - 1);
   

   errorDetail = UA_STATUS_OK;
   self.reset();

Token *token = Token::Verify(credentials,validateTokenFunc);

// If we have a valid token, we need to deserialize the black box data
   if (token != NULL)
   {
      self.deserialize(*token);  //ACH load auth info from black box data.

      //Need to set performance info
      return ZFIL_ERR_OK;
   }

//
// Credentials are not a valid token; must be a password.  Proceed with normal
// authentication.  First, obtain a token to store authentication results.
//

   token = Token::Obtain();

   if (token == NULL)
      return ZFIL_ERR_BOUNDSERR; //ACH better error?

   token->reset();
   if (self.bb.usersInfo.extUsername)
     delete self.bb.usersInfo.extUsername;
   self.bb.usersInfo.extUsername = new char [strlen(externalUsername) + 1];
   strcpy(self.bb.usersInfo.extUsername, externalUsername);

//
// Ok, let's actually verify the username and password.
//

   authenticateUser(self,externalUsername,credentials,tenantName,
                    clientInfo,authenticationInfo,performanceInfo);
   

// Convert password to a token key.  Note this is done for all authentications,
// both successful and unsuccessful.

   token->getTokenKey(authenticationInfo.tokenKey);
   authenticationInfo.tokenKeySize = token->getTokenKeySize();

// Save authentication results in black box within token container
   token->setData((char *)&self.bb,sizeof(self.bb));   
   
   return ZFIL_ERR_OK;

}
//************************* End of DBUserAuth::verify **************************

#pragma page "DBUserAuth::verifyChild"
// *****************************************************************************
// *                                                                           *
// * Function: DBUserAuth::verifyChild                                         *
// *                                                                           *
// *    Verifies the token key sent by a child MXOSRVR and returns the         *
// * blackbox data.                                                            *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <credentials>                   const char *                    In       *
// *    is the token sent by the child server.                                 *
// *                                                                           *
// *  <data>                          char *                          Out      *
// *    passes back the authentication data (black box) associated with        *
// *    this token.                                                            *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: size_t                                                           *
// *                                                                           *
// *    The number of bytes in <data>.                                         *
// *                                                                           *
// *****************************************************************************
size_t DBUserAuth::verifyChild(
   const char  * credentials,
   char        * data)

{

// Compare the token key sent with the one stored in this server
Token *token = Token::Verify(credentials);

   if (token == NULL)
      return 0;

   token->getData(data);

   return token->getDataSize();

}
//********************* End of DBUserAuth::verifyChild **********************

// DBUserAuthContents functions

#pragma page "DBUserAuthContents::bypassAuthentication"
// *****************************************************************************
// *                                                                           *
// * Function: DBUserAuthContents::bypassAuthentication                        *
// *                                                                           *
// *    Skip authentication, populate fields as if DB__ROOT logged on          *
// *  successfully.                                                            *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <authenticationInfo             AUTHENTICATION_INFO &           Out      *
// *    passes back user and error data related to the authentication.         *
// *                                                                           *
// *****************************************************************************
int DBUserAuthContents::bypassAuthentication(
   AUTHENTICATION_INFO & authenticationInfo)

{

   Token *token = Token::Obtain();

   bb.error = ZFIL_ERR_OK;
   bb.isAuthenticated = true;
   bb.errorDetail = UA_STATUS_OK;
   bb.usersInfo.effectiveUserID = ROOT_USER_ID;
   bb.usersInfo.sessionUserID = ROOT_USER_ID;
   bb.usersInfo.tenantID = SYSTEM_TENANT_ID;
   bb.usersInfo.groupList.clear();
   bb.usersInfo.dbUsername = new char[strlen(DB__ROOT) + 1];
   strcpy(bb.usersInfo.dbUsername, DB__ROOT);
   bb.usersInfo.tenantName = new char[strlen(DB__SYSTEMTENANT) + 1];
   strcpy(bb.usersInfo.tenantName, DB__SYSTEMTENANT);
   token->setData((char *)&bb,sizeof(bb));
   
   authenticationInfo.error = bb.error;
   authenticationInfo.errorDetail = bb.errorDetail;   
   copyUsersInfo(authenticationInfo.usersInfo, bb.usersInfo);
   authenticationInfo.tokenKeySize = token->getTokenKeySize();
   token->getTokenKey(authenticationInfo.tokenKey);
   
   return ZFIL_ERR_OK;

}
//************* End of DBUserAuthContents::bypassAuthentication ****************



#pragma page "DBUserAuthContents::deserialize"
// *****************************************************************************
// *                                                                           *
// * Function: DBUserAuthContents::deserialize                                 *
// *    Sets all member values from a serialized token blackbox.               *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <token>                         Token &                         In       *
// *    is a reference to the token containing the data to be deserialized.    *
// *                                                                           *
// *****************************************************************************

void DBUserAuthContents::deserialize(Token &token)

{

size_t blackBoxSize = token.getDataSize();

char *blackBox = new char[blackBoxSize];

   token.getData(blackBox);

   memcpy((char *)(&bb),blackBox,blackBoxSize);

   delete [] blackBox;

}
//***************** End of DBUserAuthContents::deserialize *********************

#pragma page "DBUserAuthContents::reset"
// *****************************************************************************
// *                                                                           *
// * Function: DBUserAuthContents::reset                                       *
// *    Resets all the member values to their default.                         *
// *                                                                           *
// *****************************************************************************
void DBUserAuthContents::reset()

{

   bb.usersInfo.effectiveUserID = 0;//ACH can we reserve value for NO_SUCH_USER?
   bb.usersInfo.sessionUserID = 0;//ACH can we reserve value for NO_SUCH_USER?
   bb.usersInfo.tenantID = 0;//ACH can we reserve value for NO_SUCH_TENANT?
   if (bb.usersInfo.extUsername)
   {
      delete bb.usersInfo.extUsername;
      bb.usersInfo.extUsername = NULL;
   }

   if (bb.usersInfo.dbUsername)
   {
      delete bb.usersInfo.dbUsername;
      bb.usersInfo.dbUsername = NULL;
   }
  
   if (bb.usersInfo.dbUserPassword)
   {
      delete bb.usersInfo.dbUserPassword;
      bb.usersInfo.dbUserPassword = NULL;
   }
   
   if (bb.usersInfo.tenantName)
   {
      delete bb.usersInfo.tenantName;
      bb.usersInfo.tenantName = NULL;
   }

   if (bb.usersInfo.defaultSchema)
   {
     delete bb.usersInfo.defaultSchema;
     bb.usersInfo.defaultSchema = NULL;
   }

   bb.usersInfo.groupList.clear();
   bb.usersInfo.redefTime = 0;
   bb.usersInfo.tenantRedefTime = 0;
   bb.isAuthenticated = false;
   bb.error = 0;
   bb.errorDetail = UA_STATUS_OK;

}
//********************* End of DBUserAuthContents::reset ***********************

// Begin private functions


#pragma page "authenticateUser"
// *****************************************************************************
// *                                                                           *
// * Function: authenticateUser                                                *
// *                                                                           *
// *    Validates user is registered on Trafodion, and the username and        *
// *  password are valid on the directory server. In multi-tenant environments *
// *  verifies the one of the user's groups matches one of the tenant's groups *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <self>                          DBUserAuthContents &            In/Out   *
// *    is a reference to a DBUserAuthContents object.  Results of the         *
// *  authentication are stored here.                                          *
// *                                                                           *
// *  <externalUsername>              const char *                    In       *
// *    is the username.  Username must be registered (in AUTHS table) and     *
// *  defined on LDAP server.                                                  *
// *                                                                           *
// *  <password>                      const char *                    In       *
// *    is the password for username.  Password is authenticated on LDAP server*
// *                                                                           *
// *  <tenantName>                    const chr *                     In       *
// *    is the tenant name specifed at connection time.                        *
// *                                                                           *
// *  <client_info>                   const CLIENT_INFO &             In       *
// *    is the client related information needed for audit logging.            *
// *                                                                           *
// *  <authenticationInfo             AUTHENTICATION_INFO &           Out      *
// *    passes back user and error data related to the authentication.         *
// *                                                                           *
// *  <performanceInfo>               PERFORMANCE_INFO &              Out      *
// *    passes back elapsed time for authentication suboperations.             *
// *                                                                           *
// *****************************************************************************

static void authenticateUser(
   DBUserAuthContents &  self,
   const char *          externalUsername,
   const char *          password,
   const char *          tenantName,
   ClientContents &      clientInfo,
   AUTHENTICATION_INFO & authenticationInfo,
   PERFORMANCE_INFO &    performanceInfo)

{
   char * tenantToUse = (char *) tenantName;
   char systemTenant[strlen(DB__SYSTEMTENANT) + 1];
   strcpy(systemTenant,DB__SYSTEMTENANT);

   // Just in case the tenant name is not passed, set it to the system tenant.
   // On dev machines, the tenant name is not passed
   bool isSystemTenant = (strcmp(tenantToUse, DB__SYSTEMTENANT) == 0);
   if (strlen(tenantToUse) == 0)
   {
      isSystemTenant = true;
      tenantToUse = systemTenant;
   }

   self.bb.isAuthenticated = false;
   bool mtEnabled = msg_license_multitenancy_enabled();

   UA_Status retCode;
   bool isValid;
   USERS_INFO usersInfo;

   char eventMsg[MAX_EVENT_MSG_SIZE];
   /*
   if (!self.authorizationEnabled)
   */
   if (CmpCommon::isUninitializedSeabase())
   {
      self.bypassAuthentication(authenticationInfo);
      return;
   }

    //check license first & first
    if ((COM_LOCAL_AUTH == self.authenticationType) && (!CmpCommon::isModuleOpen(LM_LOCAL_AUTH)))
    {
       self.getAuthEvents().clear();
       authenticationInfo.error = ZFIL_ERR_SECVIOL;
       self.bb.error = ZFIL_ERR_SECVIOL;
       self.bb.errorDetail = UA_STATUS_NOT_LICENSE;
       authenticationInfo.errorDetail = self.bb.errorDetail;
       logAuthenticationOutcome(externalUsername, usersInfo.dbUsername,
                                usersInfo.sessionUserID,
                                usersInfo.configNumber,
                                usersInfo.groupList,
                                clientInfo,
                                AUTH_NO_LICENSE);
      return;
    }

   //
   // First step; is the user registered and valid?  
   //
   SQLSEC_AuthDetails authDetails;
   int32_t sqlError = 0;
   bool userRegistered = false;

   int64_t startTime = JULIANTIMESTAMP();
   sqlError = SQL_EXEC_GetUserAttrs (externalUsername, tenantToUse, &usersInfo, &authDetails);
   performanceInfo.sqlUserTime = JULIANTIMESTAMP() - startTime;

   //
   // Set up the external name even if the user is not registered.
   //
   usersInfo.extUsername = new char [strlen(externalUsername) + 1];
   strcpy(usersInfo.extUsername, externalUsername);
   userRegistered = (authDetails.auth_id > 0);

   if (sqlError == -ZFIL_ERR_REGISTER) 
    {
      // 
      // return if not registered and TRAF_AUTO_REGISTER_USER is off
      //
      authenticationInfo.error = self.bb.error = ZFIL_ERR_REGISTER;
      authenticationInfo.errorDetail = self.bb.errorDetail = UA_STATUS_ERR_REGISTER;

      logAuthEvents(self.authEvents);
      logAuthenticationOutcome(externalUsername, usersInfo.dbUsername,
                               usersInfo.sessionUserID, -1, 
                               usersInfo.groupList, clientInfo, 
                               AUTH_NOT_REGISTERED, sqlError);
      return;
   }

   if (sqlError < 0)
   {
      // 
      // return if error reading metadata
      //
      authenticationInfo.error = self.bb.error = ZFIL_ERR_SECVIOL;
      authenticationInfo.errorDetail = self.bb.errorDetail = UA_STATUS_ERR_INVALID;

      logAuthEvents(self.authEvents);
      logAuthenticationOutcome(externalUsername, usersInfo.dbUsername,
                               usersInfo.sessionUserID, -1, 
                               usersInfo.groupList, clientInfo, 
                               AUTH_MD_NOT_AVAILABLE, sqlError);
      return;
   }

   snprintf(eventMsg, MAX_EVENT_MSG_SIZE,
            "user ID: %ld, user name: %s, isValid: %d, "
            "tenant ID: %ld, tenant name: %s (%s), "
            "isSystem: %d, isValid: %d",
            authDetails.auth_id, usersInfo.dbUsername, authDetails.auth_is_valid,
            authDetails.tenant_id, tenantToUse, usersInfo.tenantName,
            isSystemTenant, authDetails.tenant_is_valid);
   insertAuthEvent(self.authEvents,DBS_DEBUGMSG,eventMsg, LL_DEBUG);
   logAuthEvents(self.authEvents);

   if (mtEnabled && authDetails.num_tenants > 1 && isSystemTenant)
   {
      //
      // return if more than one tenant, and tenantName is the system 
      // tenant, tenant name must be specified
      //
      authenticationInfo.error = self.bb.error = ZFIL_ERR_TENANT;
      authenticationInfo.errorDetail = self.bb.errorDetail = UA_STATUS_ERROR_SYSTEM_TENANT;
      authenticationInfo.usersInfo.tenantName = new char [strlen(usersInfo.tenantName) + 1];
      strcpy(authenticationInfo.usersInfo.tenantName,usersInfo.tenantName);

      logAuthEvents(self.authEvents);
      logAuthenticationOutcome(externalUsername, usersInfo.dbUsername,
                               usersInfo.sessionUserID, usersInfo.configNumber,
                               usersInfo.groupList, clientInfo, 
                               AUTH_NO_TENANT , sqlError);
      return;
   }

   if (userRegistered && !authDetails.auth_is_valid)

   {
      //
      // User is registered, but is the account still valid?  Users can be
      // marked offline by ALTER USER command or possibly in the future when
      // we detect a registered user is no longer defined on the directory server.
      //
      authenticationInfo.error = self.bb.error = ZFIL_ERR_SECVIOL;
      authenticationInfo.errorDetail = self.bb.errorDetail = UA_STATUS_ERROR_BAD_AUTH;
      stripAllTrailingBlanks(usersInfo.tenantName,256);
      authenticationInfo.usersInfo.tenantName = new char [strlen(usersInfo.tenantName) + 1];
      strcpy(authenticationInfo.usersInfo.tenantName,usersInfo.tenantName);

      logAuthEvents(self.authEvents);
      logAuthenticationOutcome(externalUsername,
                               usersInfo.dbUsername,
                               usersInfo.sessionUserID,
                               usersInfo.configNumber,
                               usersInfo.groupList,
                               clientInfo,AUTH_USER_INVALID);
      return;
   }

   if (mtEnabled && !authDetails.tenant_is_valid)
   {
      //
      // Tenant is registered, but is it still valid?  Tenants can be
      // marked offline by ALTER TENANT command
      //
      authenticationInfo.error = self.bb.error = ZFIL_ERR_SECVIOL;
      authenticationInfo.errorDetail = self.bb.errorDetail = UA_STATUS_ERROR_BAD_AUTH;
      stripAllTrailingBlanks(usersInfo.tenantName,256);
      authenticationInfo.usersInfo.tenantName = new char [strlen(usersInfo.tenantName) + 1];
      strcpy(authenticationInfo.usersInfo.tenantName,usersInfo.tenantName);

      logAuthEvents(self.authEvents);
      logAuthenticationOutcome(externalUsername,
                               usersInfo.dbUsername,
                               usersInfo.sessionUserID,
                               usersInfo.configNumber,
                               usersInfo.groupList,
                               clientInfo,AUTH_TENANT_INVALID);
      return;
   }

   LDAuthStatus authStatus = LDAuthSuccessful;
   string configName = "";

   //
   // Next, let's see if username and password match
   //
   if (COM_LDAP_AUTH == self.authenticationType)
   {
      //
      // Let's check on the credentials next.
      //
      if (strlen(password) == 0)
      {
         logAuthEvents(self.authEvents);
         // zero len password is a non-auth bind in LDAP, so we treat it
         // as a failed authorization
         authenticationInfo.error = self.bb.error = ZFIL_ERR_SECVIOL;
         authenticationInfo.errorDetail = self.bb.errorDetail = UA_STATUS_ERR_INVALID;
         logAuthenticationOutcome(usersInfo.extUsername,
                                  usersInfo.dbUsername,
                                  usersInfo.sessionUserID,
                                  usersInfo.configNumber,
                                  usersInfo.groupList,
                                  clientInfo,AUTH_USER_INVALID);
         return;
      }


      //
      // Next step, see if the user is defined on the LDAP server.
      //

      short configNum = -1;

      DBUserAuth::CheckUserResult chkUserRslt = DBUserAuth::UserDoesNotExist;
      if (!userRegistered)
      {
         //
         //  find an ldap configuration section where the user exists
         //  if num_groups > 0, search only sections associated with the
         //  list of groups. Otherwise, search all sections.
         //
         startTime = JULIANTIMESTAMP();
         retCode = findConfigSection (authDetails.num_groups,
                                      authDetails.group_list,
                                      externalUsername,
                                      self.authEvents,
                                      usersInfo.configNumber, 
                                      configName);

         if (retCode == UA_STATUS_OK)
         {
            //
            // found the LDAP location where the user resides, verify password
            //
            authStatus = executeLDAPAuthentication(self,usersInfo.extUsername,
                                                   password,
                                                   usersInfo.configNumber,
                                                   performanceInfo);
         }
         else if (retCode == UA_STATUS_ERR_MANUALLY_REGISTER)
         {
            //
            // the user is defined in more than one section, report an error
            // *** note - this check is not enabled right now, the first
            // *** section where the user is defined is returned.
            //
            authenticationInfo.error = self.bb.error = ZFIL_ERR_SECVIOL;
            authenticationInfo.errorDetail = self.bb.errorDetail = retCode;

            logAuthEvents(self.authEvents);
            logAuthenticationOutcome(externalUsername,
                                     usersInfo.dbUsername,
                                     usersInfo.sessionUserID,
                                     usersInfo.configNumber,
                                     usersInfo.groupList,
                                     clientInfo,AUTH_USER_INVALID);
            return;
         }
         else
            //
            //  username and password do not match
            //
            authStatus = LDAuthRejected;

         performanceInfo.findConfigTime = JULIANTIMESTAMP() - startTime;
      } // If User is not registered

      else
        authStatus = executeLDAPAuthentication(self,usersInfo.extUsername,
                                               password,
                                               usersInfo.configNumber,
                                               performanceInfo);
 
      if (authStatus != LDAuthSuccessful)
      {
         //
         // Rejected!
         //
         // Either the provided password does not match the one stored on the LDAP 
         // server or we had internal problems with the server.
         //
         // No soup for you.
         //
         authenticationInfo.error = ZFIL_ERR_SECVIOL;
         self.bb.error = ZFIL_ERR_SECVIOL;
         if (authStatus == LDAuthRejected)
            self.bb.errorDetail = UA_STATUS_ERR_INVALID;
         else
         {
            self.bb.errorDetail = UA_STATUS_ERR_SYSTEM;
         }
         authenticationInfo.errorDetail = self.bb.errorDetail;

         logAuthEvents(self.authEvents);
         logAuthenticationOutcome(externalUsername,usersInfo.dbUsername,
                                  usersInfo.sessionUserID, usersInfo.configNumber,
                                  usersInfo.groupList, clientInfo,
                                 (authStatus = LDAuthRejected) ? AUTH_REJECTED : AUTH_FAILED);
 
         return;
      }
      //set user's group list
      usersInfo.groupList = self.bb.usersInfo.groupList;
   }
   else if (COM_LOCAL_AUTH == self.authenticationType)
   //else if (strlen(password) != 0 || strlen(usersInfo.dbUserPassword) != 0) login user must provide password
   {
      int rc = 0;

      Int16 errcnt = 0;
      Int16 pwderrcnt = (Int16)CmpCommon::getDefaultNumeric(PASSWORD_ERROR_COUNT);
      rc = SQL_EXEC_GetAuthErrPwdCnt(usersInfo.sessionUserID, errcnt);
      if (rc < 0 || errcnt >= pwderrcnt)
      {
         self.getAuthEvents().clear();
         authenticationInfo.error = ZFIL_ERR_SECVIOL;
         self.bb.error = ZFIL_ERR_SECVIOL;
         self.bb.errorDetail = UA_STATUS_PASSWORD_ERROR_COUNT;
         authenticationInfo.errorDetail = self.bb.errorDetail;
         logAuthenticationOutcome(externalUsername, usersInfo.dbUsername,
                                  usersInfo.sessionUserID,
                                  usersInfo.configNumber,
                                  usersInfo.groupList,
                                  clientInfo,
                                  AUTH_PASSWORD_ERROR_COUNT);

         return;
      }

      // base64 maybe make encrypted data longer
      char dePassword[2 * (MAX_PASSWORD_LENGTH + EACH_ENCRYPTION_LENGTH) + 1] = {0};

      Int64 lifetime = (Int64)CmpCommon::getDefaultNumeric(PASSWORD_LIFE_TIME);

      if (lifetime > 0)
      {
         // 60 * 60 * 1000 * 1000 usecs
         lifetime = (usersInfo.passwdModTime + lifetime * 24 * 3600000000LL -
                     NA_JulianTimestamp());

         if (lifetime < 1) //password expire
         {
            // grace login
            Int16 counter = (Int16)CmpCommon::getDefaultNumeric(TRAF_PASSWORD_GRACE_COUNTER);
            if (counter)
            {
                Int16 graceCounter;
                if (SQL_EXEC_GetAuthGracePwdCnt(usersInfo.sessionUserID, graceCounter) == 0)
                {
                    if (graceCounter < counter)
                    {
                        authenticationInfo.error = counter - graceCounter;
                        authenticationInfo.errorDetail = UA_STATUS_PASSWORD_GRACE_COUNT;
                        SQL_EXEC_UpdateAuthGracePwdCnt(usersInfo.sessionUserID, graceCounter + 1);
                        logAuthenticationOutcome(externalUsername, usersInfo.dbUsername,
                                                 usersInfo.sessionUserID,
                                                 usersInfo.configNumber,
                                                 usersInfo.groupList,
                                                 clientInfo,
                                                 AUTH_PASSWORD_GRACE_COUNT);
                        /*
                       go here and let user login with password.
                       if user continues to input wrong password,
                       they will see "Username or password is invalid" first.
                       */
                        goto LOGIN_WITH_PASSWORD;
                    }
                }
            }
            self.getAuthEvents().clear();
            authenticationInfo.error = ZFIL_ERR_SECVIOL;
            self.bb.error = ZFIL_ERR_SECVIOL;
            self.bb.errorDetail = UA_STATUS_PASSWORD_EXPIRE;
            authenticationInfo.errorDetail = self.bb.errorDetail;
            logAuthenticationOutcome(externalUsername, usersInfo.dbUsername,
                                     usersInfo.sessionUserID,
                                     usersInfo.configNumber,
                                     usersInfo.groupList,
                                     clientInfo,
                                     AUTH_PASSWORD_EXPIRE);

            return;
         }

#define WARN_TIME 7
         double life = (long double)lifetime / (long double)(24 * 3600000000LL);
         if (life - (double)WARN_TIME < 0.000001) //password will expire
         {
            authenticationInfo.error = lifetime;
            authenticationInfo.errorDetail = UA_STATUS_PASSWORD_WILL_EXPIRE;
         }
      }
LOGIN_WITH_PASSWORD:
      if (strlen(password) < MAX_PASSWORD_LENGTH + 1)
      {
         int cryptlen = 0;
         rc = ComEncryption::encrypt_ConvertBase64_Data(ComEncryption::AES_128_CBC,
                                                        (unsigned char *)(password),
                                                        strlen(password),
                                                        (unsigned char *)AUTH_PASSWORD_KEY,
                                                        (unsigned char *)AUTH_PASSWORD_IV,
                                                        (unsigned char *)dePassword,
                                                        cryptlen);
      }

      if (!password || 0 == strlen(password) || rc || strcmp(dePassword, usersInfo.dbUserPassword) != 0 ||
          strlen(password) > MAX_PASSWORD_LENGTH)
      {
         SQL_EXEC_UpdateAuthErrPwdCnt(usersInfo.sessionUserID, errcnt + 1, false);

         self.getAuthEvents().clear();
         authenticationInfo.error = ZFIL_ERR_SECVIOL;
         self.bb.error = ZFIL_ERR_SECVIOL;
         self.bb.errorDetail = UA_STATUS_ERR_INVALID;
         authenticationInfo.errorDetail = self.bb.errorDetail;
         logAuthenticationOutcome(externalUsername, usersInfo.dbUsername,
                                  usersInfo.sessionUserID,
                                  usersInfo.configNumber,
                                  usersInfo.groupList,
                                  clientInfo,
                                  AUTH_REJECTED,
                                  AUTH_NO_PASSWORD);

         return;
      }
      SQL_EXEC_UpdateAuthErrPwdCnt(usersInfo.sessionUserID, 0, true);
   }
   else if (COM_NO_AUTH == self.authenticationType)
   {
       //no need password and no check password lifecycle
   }
   if (COM_LDAP_AUTH == self.authenticationType && !userRegistered)
   {
      //
      // if user is not registered, do it now
      //
      startTime = JULIANTIMESTAMP();
      sqlError = SQL_EXEC_RegisterUser(externalUsername, configName.c_str(), &usersInfo, &authDetails);
      if (authDetails.auth_id == 0 || usersInfo.dbUsername == NULL || sqlError < 0)
      {
         //
         //  Unable to register user, return an error
         //
         authenticationInfo.error = self.bb.error = sqlError;
         authenticationInfo.errorDetail = self.bb.errorDetail = UA_STATUS_ERR_REGISTER_FAILED;

         logAuthEvents(self.authEvents);
         logAuthenticationOutcome(externalUsername, usersInfo.dbUsername,
                                  usersInfo.sessionUserID, usersInfo.configNumber,
                                  usersInfo.groupList, clientInfo, 
                                  AUTH_NOT_REGISTERED, sqlError);
         return;
      }
      performanceInfo.registerUserTime = JULIANTIMESTAMP() - startTime;
   }

   if (authDetails.tenant_group_check && mtEnabled && !isSystemTenant)
   {
       //
       // Next - if tenant_group_check is requested, verify that one of the
       // user's group's match one of the tenant's group's.
       // System tenant check already performed so skip
       //
       SQLSEC_GroupInfo *tenantGroupList = authDetails.group_list;
       startTime = JULIANTIMESTAMP();
       std::set<std::string> userGroupList = usersInfo.groupList;
       char upperGroupName[129] = {0};
       bool foundGroup = false;
       std::set<std::string>::iterator it;
       for (it = userGroupList.begin(); it != userGroupList.end(); ++it)
       {
           std::string groupValue = *it;
           upshiftString(groupValue.c_str(), upperGroupName);
           std::string userGroup(upperGroupName);
           for (int j = 0; j < authDetails.num_groups; j++)
           {
               std::string tenantGroup = tenantGroupList[j].group_name;
               if (0 == tenantGroup.compare(userGroup))
               {
                    foundGroup = true;
                    if (COM_LDAP_AUTH == self.authenticationType)
                    {
                        foundGroup = (usersInfo.configNumber == tenantGroupList[j].group_config_num) ? true : false;
                    }
                    if (foundGroup)
                    {
                        break;
                    }
               }
           }
       }
       performanceInfo.tenantGroupCheck = JULIANTIMESTAMP() - startTime;
       if (!foundGroup)
       {
           //
           //  User is not associated with specified tenant, return an error
           //
           authenticationInfo.error = self.bb.error = ZFIL_ERR_TENANT;
           authenticationInfo.errorDetail = self.bb.errorDetail = UA_STATUS_ERR_TENANT;

           authenticationInfo.usersInfo.tenantName = new char[strlen(usersInfo.tenantName) + 1];
           strcpy(authenticationInfo.usersInfo.tenantName, usersInfo.tenantName);

           sqlError = -1079;
           logAuthenticationOutcome(usersInfo.extUsername, usersInfo.dbUsername,
                                    usersInfo.sessionUserID, usersInfo.configNumber,
                                    usersInfo.groupList, clientInfo,
                                    AUTH_TENANT_INVALID, sqlError);
           return;
       }
   }

   //
   // Logging retries when the authentication was successful allows 
   // operations to be aware of potential problems.
   // Retries are logged later for unsuccessful authentications.
   //
   logAuthenticationRetries(self.nodeID,externalUsername);

   //
   // save details in authenticationInfo
   //
   if (authenticationInfo.errorDetail!= UA_STATUS_PASSWORD_WILL_EXPIRE &&
        authenticationInfo.errorDetail != UA_STATUS_PASSWORD_GRACE_COUNT)
   {
       authenticationInfo.error = ZFIL_ERR_OK;
       authenticationInfo.errorDetail = UA_STATUS_OK;
   }
   copyUsersInfo(authenticationInfo.usersInfo, usersInfo);

   //
    // copy user info to black box
   //
   self.bb.error = ZFIL_ERR_OK;
   self.bb.errorDetail = UA_STATUS_OK;
   self.bb.isAuthenticated = true;
   copyUsersInfo(self.bb.usersInfo, usersInfo);

   //
   // log execution times
   //
   int64_t totalLDAP = performanceInfo.searchConnectionTime +
                       performanceInfo.searchTime +
                       performanceInfo.authenticationConnectionTime +
                       performanceInfo.authenticationTime +
                       performanceInfo.groupLookupTime +
                       performanceInfo.retryLookupTime;

   snprintf(eventMsg, MAX_EVENT_MSG_SIZE,
            "Authentication counters -> SQL metadata lookup: %ld, LDAP communication: %ld, " 
            "LDAP configuration search: %ld, Register user: %ld",
            performanceInfo.sqlUserTime, totalLDAP,
            performanceInfo.findConfigTime, performanceInfo.registerUserTime);
   insertAuthEvent(self.authEvents,DBS_DEBUGMSG,eventMsg, LL_INFO);
   snprintf(eventMsg, MAX_EVENT_MSG_SIZE,
            "LDAP counters -> User search connection: %ld, User search: %ld, "
            "User authenticate connection: %ld, User authenticate: %ld, "
            "Group lookup: %ld, Retry searches: %ld, Retries: %d",
            performanceInfo.searchConnectionTime,
            performanceInfo.searchTime, performanceInfo.authenticationConnectionTime,
            performanceInfo.authenticationTime, performanceInfo.groupLookupTime, 
            performanceInfo.retryLookupTime, performanceInfo.numberRetries);
   insertAuthEvent(self.authEvents,DBS_DEBUGMSG,eventMsg, LL_INFO);

   //
   // Log the successful authentication and any other info/debug events
   //
   logAuthEvents(self.authEvents);
   logAuthenticationOutcome(externalUsername,usersInfo.dbUsername,
                            usersInfo.sessionUserID, usersInfo.configNumber,
                            usersInfo.groupList,
                            clientInfo,AUTH_OK);

   return;
}
//************************** End of authenticateUser ***************************


#if 0
// *****************************************************************************
// *                                                                           *
// * Function: cacheUserInfo                                                   *
// *                                                                           *
// *    Stores information for the user in local cache.                        *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <externalUsername>              const char *                    In       *
// *    is the external username.                                              *
// *                                                                           *
// *  <databaseUsername>              const char *                    In       *
// *    is the database username associated with the external username.        *
// *                                                                           *
// *  <userID>                        int32_t                         In       *
// *    passes back the numeric ID of the database user associated             *
// *  with the external username.                                              *
// *                                                                           *
// *  <isValid>                       bool                            In       *
// *    is true if the user is online, false if offline.                       *
// *                                                                           *
// *  <redefTime>                     int64_t                         In       *
// *    is the time the user record was last written.                          *
// *                                                                           * 
// *****************************************************************************
void cacheUserInfo(
   const char    * externalUsername,
   const char    * databaseUsername,
   int32_t         userID,
   bool            isValid,
   int64_t         redefTime)
   
{

UserCacheContents userInfo;

   strcpy(userInfo.externalUsername,externalUsername);
   strcpy(userInfo.databaseUsername,databaseUsername);
   userInfo.userID = userID;
   userInfo.isValid = isValid;
   userInfo.redefTime = redefTime;
   
   userCache.push_back(userInfo);
   
}
//*************************** End of cacheUserInfo *****************************
#endif

#pragma page "executeLDAPAuthentication"
// *****************************************************************************
// *                                                                           *
// * Function: executeLDAPAuthentication                                       *
// *                                                                           *
// *    Sends request to LDAP server (via LDAPConfigNode class) to authenticate*
// *  the provided username and password.                                      *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <self>                          DBUserAuthContents &            In/Out   *
// *    is a reference to a DBUserAuthContents object.  Results of the         *
// *    authentication are stored here.                                        *
// *                                                                           *
// *  <username>                      const char *                    In       *
// *    is the username.  Username must be  defined on LDAP server.            *
// *                                                                           *
// *  <password>                      const char *                    In       *
// *    is the password for username.  Password is authenticated on LDAP server*
// *                                                                           *
// *  <configName>                    const char *                    In       *
// *    is the LDAP configuration to use, either primary or secondary.         *
// *                                                                           *
// *****************************************************************************

static LDAuthStatus executeLDAPAuthentication(
   DBUserAuthContents &            self,
   const char *                    username,
   const char *                    password,
   short                           configNumber,
   PERFORMANCE_INFO &              performanceInfo)

{

   LDAPConfigNode::ClearRetryCounts();
   self.getAuthEvents().clear();
   
   //
   // First get a search connection for the specified configuration. 
   // If we can't get a search connection, could be network problem or a
   // bad configuration.  Either way, tough luck for the user.
   //

   int64_t startTime = JULIANTIMESTAMP();
   LDAPConfigNode *searchNode = LDAPConfigNode::GetLDAPConnection(self.authEvents,NULL,configNumber,
                                                               SearchConnection);

   performanceInfo.searchConnectionTime = JULIANTIMESTAMP() - startTime;

   char eventMsg[MAX_EVENT_MSG_SIZE];

   if (searchNode == NULL)
   {
      self.bb.error = ZFIL_ERR_SECVIOL;
      self.bb.errorDetail = UA_STATUS_ERR_SYSTEM;
      snprintf(eventMsg, MAX_EVENT_MSG_SIZE,
               "Failed to get LDAP connection for user %s",username);
      insertAuthEvent(self.authEvents,DBS_NO_LDAP_SEARCH_CONNECTION,eventMsg, LL_ERROR);

      return LDAuthResourceFailure;
   }

   string userDN = "";       // User DN, used to bind to LDAP serer

   startTime = JULIANTIMESTAMP();

   LDSearchStatus searchStatus = searchNode->lookupUser(self.authEvents,username,userDN);
                                                     
   performanceInfo.searchTime = JULIANTIMESTAMP() - startTime; 
                                                       
   if (searchStatus == LDSearchNotFound)
   {
      self.bb.error = ZFIL_ERR_SECVIOL;
      self.bb.errorDetail = UA_STATUS_ERR_INVALID;
      snprintf(eventMsg, MAX_EVENT_MSG_SIZE,
               "Failed LDAP search for user %s",username);
      insertAuthEvent(self.authEvents,DBS_NO_LDAP_SEARCH_CONNECTION,eventMsg, LL_ERROR);

      return LDAuthRejected;
   }

   if (searchStatus != LDSearchFound)
   {
      self.bb.error = ZFIL_ERR_SECVIOL;
      self.bb.errorDetail = UA_STATUS_ERR_SYSTEM;
      snprintf(eventMsg, MAX_EVENT_MSG_SIZE,
               "Failed LDAP search for user %s",username);
      insertAuthEvent(self.authEvents,DBS_NO_LDAP_SEARCH_CONNECTION,eventMsg, LL_ERROR);
      return LDAuthResourceFailure;
   }

   // (searchStatus == LDSearchFound)
   // ACH: Should we compare UUIDOnLDAP and dsUUID and give "not registered"
   //       (or some other) error if they don't match?

   //
   // User is defined here and there.  But is their password correct?
   // Let's get an authentication connection to check on the password
   //
   startTime = JULIANTIMESTAMP();

   LDAPConfigNode *authNode = LDAPConfigNode::GetLDAPConnection(self.authEvents,NULL,configNumber,
                                                             AuthenticationConnection);

   performanceInfo.authenticationConnectionTime = JULIANTIMESTAMP() - startTime;
   
   if (authNode == NULL)
   {
      self.bb.error = ZFIL_ERR_SECVIOL;
      self.bb.errorDetail = UA_STATUS_ERR_SYSTEM;
      snprintf(eventMsg, MAX_EVENT_MSG_SIZE,
               "Failed LDAP search on password for user %s",username);
      insertAuthEvent(self.authEvents,DBS_NO_LDAP_SEARCH_CONNECTION,eventMsg, LL_ERROR);
      return LDAuthResourceFailure;
   }

   //
   // Non-blank password, a user we know about, let's validate that password!
   //
   LDAuthStatus authStatus = authNode->authenticateUser(self.authEvents,userDN.c_str(),password,self.bb.usersInfo.groupList,performanceInfo);
                                          
   return authStatus;                                       

}
//********************** End of executeLDAPAuthentication **********************


// *****************************************************************************
// *                                                                           *
// * Function: fetchFromCacheByUsername                                        *
// *                                                                           *
// *    Searches local cache for user information.  If found, a record with    *
// * the data is returned, otherwise NULL is returned.                         *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <externalUsername>              const char *                    In       *
// *    is the external username to search for in cache.                       *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: const UserCacheContents *                                        *
// *                                                                           *
// *     NULL - External username not found in cache.                          *
// * Not NULL - External username was found, cached record returned.           *
// *                                                                           *
// *****************************************************************************
inline static const UserCacheContents * fetchFromCacheByUsername(const char * externalUsername)

{

size_t cacheCount = userCache.size();
char upperUsername[129] = {0};

   upshiftString(externalUsername,upperUsername);

   for (size_t index = 0; index < cacheCount; index++)
      if (strcmp(userCache[index].externalUsername,upperUsername) == 0)
         return &userCache[index]; 
   
   return NULL;

}
//********************** End of fetchFromCacheByUsername ***********************




// *****************************************************************************
// *                                                                           *
// * Function: fetchFromCacheByUserID                                          *
// *                                                                           *
// *    Searches local cache for user information.  If found, a record with    *
// * the data is returned, otherwise NULL is returned.                         *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <userID>                        long                            In       *
// *    is the database userID to search for in cache.                         *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: const UserCacheContents *                                        *
// *                                                                           *
// *     NULL - Database userID not found in cache.                            *
// * Not NULL - Database userID was found, cached record returned.             *
// *                                                                           *
// *****************************************************************************
inline static const UserCacheContents * fetchFromCacheByUserID(long userID)

{

size_t cacheCount = userCache.size();

   for (size_t index = 0; index < cacheCount; index++)
      if (userCache[index].userID == userID)
         return &userCache[index]; 
   
   return NULL;

}
//*********************** End of fetchFromCacheByUserID ************************




// *****************************************************************************
// *                                                                           *
// * Function: findConfigSection                                               *
// *                                                                           *
// *    Searches the configurated LDAP sections for the user.                  *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <numGroups>                     int32_t                         In       *
// *    Number groups in grouplist, if 0, then no groups specified             *
// *                                                                           *
// *  <groupList>                     SQLSEC_GroupInfo                In       *
// *    If specified, use sections defined in the groupList to search          *
// *    for user. Otherwise, search all sections                               *
// *                                                                           *
// *  <extUsername>                   const char *                    In       *
// *    User name                                                              *
// *                                                                           *
// *  <authEvents>                    std::vector<AuthEvent> &        Out      *
// *    returns any events                                                     *
// *                                                                           *
// *  <configNumber>                  short                           Out      *
// *    returns configuration number for LDAP section to use                   *
// *                                                                           *
// *  <configName>                    std::string                     Out      *
// *    returns configuration name for LDAP section to use                     *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: UA_STATUS                                                        *
// *    passes back result of search                                           *
// *                                                                           *
// *****************************************************************************
#pragma page "findConfigSection"

static UA_Status findConfigSection(
  int32_t numGroups,
  const SQLSEC_GroupInfo * groupList,
  const char * extUsername,
  std::vector<AuthEvent> & authEvents,
  short & configNumber,
  std::string &configName)
{
   std::set <short> sections;
   configNumber = -1;

   char eventMsg[MAX_EVENT_MSG_SIZE+1];

   if (numGroups > 0)
   {
      // Get the section numbers of the tenant groups
      for (int i = 0; i < numGroups; i++)
         sections.insert(groupList[i].group_config_num);
   }
   else
   {           
      // Get all section numbers in config file
      short numSections = 0;
      LDAPConfigNode::GetNumberOfSections(authEvents, numSections);
      for (short i=0; i < numSections; i++)
         sections.insert(i);
   }

   bool foundConfig = false;
   short tempConfig = -1;
   std::string tempConfigName;

   // Check in each defined LDAP section if the user exists
   std::set<short>::iterator it;
   for (it = sections.begin(); it != sections.end(); ++it)
   {
      tempConfig = *it;
      tempConfigName.clear();
      DBUserAuth::CheckUserResult chkUserRslt = 
         DBUserAuth::CheckExternalUsernameDefinedConfigNum(extUsername,
                                                           tempConfig,
                                                           tempConfigName);

      snprintf(eventMsg, MAX_EVENT_MSG_SIZE, "User %s %s in config %s (%d)",
               extUsername, 
               (chkUserRslt == DBUserAuth::UserExists ? "found" : "not found"),
               tempConfigName.c_str(), tempConfig);
      insertAuthEvent(authEvents,DBS_DEBUGMSG,eventMsg, LL_DEBUG);
      if (chkUserRslt == DBUserAuth::UserExists)
      {
         // If the same user is defined in multiple AD/LDAP configs report an 
         // error.  The user needs to be manually registered. DBSecurity is unable
         // to determine which AD/LDAP to use.
         //if (foundConfig)
         //  return UA_STATUS_ERR_MANUALLY_REGISTER;

         foundConfig = true;
         configNumber = tempConfig;
         configName = tempConfigName;
         break;
      }
   }

   if (foundConfig)
     return UA_STATUS_OK;

   return UA_STATUS_ERR_INVALID;
}



#pragma page "logAuthEvents"
// *****************************************************************************
// *                                                                           *
// * Function: logAuthEvents                                                   *
// *                                                                           *
// * Logs resource failure errors encountered during authentication.           *
// * Resource failure errors include problems with the                         *
// * configuration in .traf_authrntication_config, network issues,             *
// * LDAP server issues, or coding blunders.                                   *
// *                                                                           *
// * Resource failures have already been inserted into authEvents structure    *
// *                                                                           *
// * Logs info and debug messages if enabled.                                  *
// *                                                                           *
// *****************************************************************************
static void logAuthEvents(std::vector<AuthEvent> &authEvents)

{
   size_t eventCount = authEvents.size();

   for (size_t i = 0; i < eventCount; i++)
   {
     AuthEvent authEvent = authEvents[i];
     authEvent.setCallerName("mxosrvr");
     authEvent.logAuthEvent();
   }
}
//************************ End of logAuthEvents **********************

#pragma page "logAuthenticationOutcome"
// *****************************************************************************
// *                                                                           *
// * Function: logAuthenticationOutcome                                        *
// *                                                                           *
// *    Logs the outcome.                                                      *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <external_user_name>            char *                          In       *
// *    is the external username.                                              *
// *                                                                           *
// *  <internal_user_name>            char *                          In       *
// *    internal user name                                                     *
// *                                                                           *
// *  <userID>                        long                            In       *
// *    is the numeric ID of the database user associated                      *
// *  with the external username.                                              *
// *                                                                           *
// *  <configNumber>                  int                             In       *
// *    is the LDAP configuration used to authenticate the user                *
// *                                                                           *
// *  <groupList>                     set<string>                     In       *
// *    is the list of groups assigned to the user                             *
// *                                                                           *
// *  <clientInfo>                    ClientContents                  In       *
// *    is the client related information needed for audit logging.            *
// *                                                                           *
// *  <outcome>                       string &                        In       *
// *    the outcome of the authentication                                      *
// *                                                                           *
// *  <sqlError>                       int                            In       *
// *    is any SQL error encountered                                           *
// *                                                                           *
// *****************************************************************************
static void logAuthenticationOutcome(
   const char             * external_user_name,
   const char             * internal_user_name,
   const int32_t            user_id,
   const int32_t            configNumber,
   const std::set<string> & groupList,
   ClientContents         & clientInfo,
   const AUTH_OUTCOME       outcome,
   int32_t                  sqlError)
{
   string internalUser("??");
   if (internal_user_name)
     internalUser = internal_user_name;

   logLevel severity = (outcome == AUTH_OK || outcome == AUTH_USER_REGISTERED) ? LL_INFO : LL_ERROR; 

   // Currently this code has hard coded error messages in many places, this
   // need to be fixed in order to allow errors to be reported in different 
   // languages.
   string outcomeDesc = getAuthOutcome(outcome, sqlError);
   
   std::vector<AuthEvent> authEvents;
   std::string configName;
   if (!LDAPConfigNode::GetConfigName(authEvents, configNumber,configName))
     configName = "not available";

   int bufSize(MAX_EVENT_MSG_SIZE);
   char buf[bufSize];
   if (COM_LDAP_AUTH == gAuthType)
   {
       snprintf(buf, bufSize,
                "Authentication type: LDAP,"
                "authentication request: externalUser %s, "
                "databaseUser %s, userID %u, "
                "configName %s (configNumber %d), "
                "clientName %s, clientUserName %s, "
                "result %d (%s)",
                external_user_name, 
                internalUser.c_str(), user_id,
                configName.c_str(), configNumber,
                clientInfo.clientName, clientInfo.clientUserName,
                (int)outcome, outcomeDesc.c_str());
   }
   else
   {
       snprintf(buf, bufSize,
                "Authentication type: LOCAL,"
                "authentication request: externalUser %s, "
                "databaseUser %s, userID %u, "
                "clientName %s, clientUserName %s, "
                "result %d (%s)",
                external_user_name,
                internalUser.c_str(), user_id,
                clientInfo.clientName, clientInfo.clientUserName,
                (int)outcome, outcomeDesc.c_str());
   }
   std::string msg(buf);
   AuthEvent authEvent (DBS_AUTHENTICATION_ATTEMPT,msg,severity); 
   authEvent.setCallerName("mxosrvr");
   authEvent.logAuthEvent();

#if 0
   // If there are too many groups, then "buf" is too small too hold the values
   // commenting out code until this fixed.
   if (groupList.size())
   {
     snprintf(buf, bufSize, "User %s is a member of: ",
                external_user_name) ;

     std::set<std::string>::iterator it;

     for (it=groupList.cbegin(); it != groupList.cend(); ++it)
     {
        string groupNameStr = *it;
        if (groupNameStr.size() + strlen(buf) < bufSize) 
        {
          strcat(buf, " ");
          strcat(buf, groupNameStr.c_str());
        }
     }
     std::string msg(buf);
     AuthEvent authEvent (DBS_AUTHENTICATION_ATTEMPT,msg,severity); 
     authEvent.setCallerName("mxosrvr");
     authEvent.logAuthEvent();
   }
#endif
}
//*********************** End of logAuthenticationOutcome **********************



#pragma page "logAuthenticationRetries"
// *****************************************************************************
// *                                                                           *
// * Function: logAuthenticationRetries                                        *
// *                                                                           *
// *    Logs retries encountered during authentication.                        *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <nodeID>                        int                             In       *
// *    is the ID of the node this process is executing on.  The node ID is    *
// *  needed to log to the EVENT_TEXT_TABLE table.                             *
// *                                                                           *
// *  <username>                      char *                          In       *
// *    is the name of the user that we attempted to authenticate.             *
// *                                                                           *
// *****************************************************************************
static void logAuthenticationRetries(
   int          nodeID,
   const char * username)

{
   size_t bindRetryCount = LDAPConfigNode::GetBindRetryCount();
   size_t searchRetryCount = LDAPConfigNode::GetSearchRetryCount();

   // If there were no retries, there is nothing to log.  
   if (bindRetryCount == 0 && searchRetryCount == 0)
      return;
      
   char buf[MAX_EVENT_MSG_SIZE];

   // Log if the search (name lookup) operation had to be retried.          
   if (searchRetryCount > 0)
   { 
      snprintf(buf,MAX_EVENT_MSG_SIZE,
               "Authentication for user %s required %d search retries. ",
              username,searchRetryCount);
       
      std::string msg(buf);
      AuthEvent authEvent (DBS_AUTH_RETRY_SEARCH, msg, LL_INFO);
      authEvent.setCallerName("mxosrvr");
      authEvent.logAuthEvent();
   }
   
   // Log if the bind (password authentication) operation had to be retried.          
   if (bindRetryCount > 0)
   { 
      snprintf(buf,MAX_EVENT_MSG_SIZE,
               "Authentication for user %s required %d bind retries. ",
              username,bindRetryCount);
       
      std::string msg(buf);
      AuthEvent authEvent (DBS_AUTH_RETRY_BIND, msg, LL_INFO);
      authEvent.setCallerName("mxosrvr");
      authEvent.logAuthEvent();
   }
}
//*********************** End of logAuthenticationRetries **********************



#pragma page "logToEventTable"
// *****************************************************************************
// *                                                                           *
// * Function: logToEventTable                                                 *
// *                                                                           *
// *    Logs to the EVENT_TEXT_TABLE table in the INSTANCE_REPOSITORY schema.  *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <nodeID>                        int                             In       *
// *    is the ID of the node this process is executing on.                    *
// *                                                                           *
// *  <eventID>                       DB_SECURITY_EVENTID             In       *
// *    is the ID of the event to be logged.                                   *
// *                                                                           *
// *  <severity>                      posix_sqlog_severity_t          In       *
// *    is the severity of the event.                                          *
// *                                                                           *
// *  <msg>                           char *                          In       *
// *    is the message text to be logged.                                      *
// *                                                                           *
// *****************************************************************************
static void logToEventTable(
   int                    nodeID,
   DB_SECURITY_EVENTID    eventID, 
   posix_sqlog_severity_t severity, 
   const char *           msg)

{
}
//**************************** End of logToEventTable **************************




#pragma page "prepareUserNameForQuery"
// *****************************************************************************
// *                                                                           *
// * Function: prepareUserNameForQuery                                         *
// *                                                                           *
// *    Converts a username into uppercase and escapes any single quote        *
// * chars to match SQL string literal rules (by replacing each single         *
// *  quote char with a pair of single quote chars).                           *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <nameForQuery>                  char *                          Out      *
// *    is the converted username.                                             *
// *                                                                           *
// *  <extName>                       const char *                    In       *
// *    is the external username to be converted.                              *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: int                                                              *
// *                                                                           *
// *  0  - Validation successful.                                              *
// * 22  - Bounds error.  Number of param in error returned in status.         *
// * 590 - NULL param.  Number of param in error returned in status.           *
// *                                                                           *
// *****************************************************************************
static void prepareUserNameForQuery(
   char       * nameForQuery,
   const char * extName)

{

   while (*extName != '\0')
   {
      if (*extName == '\'')
      {
         // replace original single quote with a pair of single quotes
         *(nameForQuery++) = '\'';
         *(nameForQuery++) = '\'';
      }
      else
         *(nameForQuery++) = toupper(*extName);

      extName++;
   }
   *nameForQuery = '\0';

}
//*********************** End of prepareUserNameForQuery ***********************

#pragma page "stripAllTrailingBlanks"
// *****************************************************************************
// *                                                                           *
// * Function: stripAllTrailingBlanks                                          *
// *                                                                           *
// *  Removes trailing blanks (replaces with null). String may have embedded   *
// *  NULLs; trailing blanks are replaced with NULL until first non-blank,     *
// *  non-null character.                                                      *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <buf>                          char *                           In/Out   *
// *    is a character buffer whose trailing blanks are to be trimmed.         *
// *                                                                           *
// *  <len>                          size_t                           In       *
// *    is the size of <buf>.                                                  *
// *                                                                           *
// *                                                                           *
// *                                                                           *
// *****************************************************************************
static void stripAllTrailingBlanks(
   char   * buf,
   size_t   len)

{

char *ptr = &buf[len];

    while (*ptr == ' ' || *ptr == 0)
    {
       *ptr = 0;
       ptr--;
    }

}
//****************** End of trimLeadingandTrailingBlanks ***********************

#pragma page "timeDiff"
// *****************************************************************************
// *                                                                           *
// * Function: timeDiff                                                        *
// *                                                                           *
// *  Find the difference between two timestamps                               *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <t1>                          timespec                          In       *
// *    First timestamp                                                        *
// *                                                                           *
// *  <t2>                          timespec                          In       *
// *    Second timestamp                                                       *
// *                                                                           *
// *  <tDiff>                       timespec                          Out      *
// *    Difference between t1 and t2                                           *
// *****************************************************************************

static void timeDiff (
   struct timespec t1,
   struct timespec t2,
   struct timespec &tDiff)

{
   if ( (t2.tv_nsec - t1.tv_nsec )  < 0 )
   {
      tDiff.tv_sec = t2.tv_sec - t1.tv_sec - 1;
      tDiff.tv_nsec = 1000000000 + t2.tv_nsec - t1.tv_nsec;
   }
   else
   {
      tDiff.tv_sec = t2.tv_sec - t1.tv_sec;
        tDiff.tv_nsec = t2.tv_nsec - t1.tv_nsec;
   }
}
//*************************** End of timeDiff **********************************

// *****************************************************************************
// *                                                                           *
// * Function: upshiftString                                                   *
// *                                                                           *
// *  Convert all characters of a string to upper case.                        *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <inputString>                 const char *                      In       *
// *    is the string to be converted to upper case.                           *
// *                                                                           *
// *  <outputString>                char *                            Out      *
// *    passes back the converted string.                                      *
// *                                                                           *
// *****************************************************************************
inline static void upshiftString(
   const char * inputString,
   char       * outputString)
   
{

   while (*inputString != '\0')
   {
      *(outputString++) = toupper(*inputString);

      inputString++;
   }
   *outputString = '\0';

}
//************************* End of upshiftString *******************************

// *****************************************************************************
// *                                                                           *
// * Function: copyUsersInfo                                                   *
// *                                                                           *
// *  copies USER_INFO structure - make a copy constructor?                    *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <target>                      USERS_INFO &                      Out      *
// *    is the copied structure                                                *
// *                                                                           *
// *  <source>                      const USERS_INFO &                In       *
// *    source values to copy                                                  *
// *                                                                           *
// *****************************************************************************
static void copyUsersInfo(
   USERS_INFO & target,
   const USERS_INFO &source)
{
   if (target.dbUsername)
     delete target.dbUsername;
   if (source.dbUsername)
   {
      target.dbUsername = new char [strlen(source.dbUsername) + 1];
      strcpy(target.dbUsername, source.dbUsername);
   }
   else
      target.dbUsername = NULL;

   if (target.extUsername)
      delete target.extUsername;
   if (source.extUsername)
   {
      target.extUsername = new char [strlen(source.extUsername) + 1];
      strcpy(target.extUsername, source.extUsername);
   }
   else
      target.extUsername = NULL;

   if (target.tenantName)
      delete target.tenantName;
   if (source.tenantName)
   {
      target.tenantName = new char [strlen(source.tenantName) + 1];
      strcpy(target.tenantName, source.tenantName);
   }
   else
      target.tenantName = NULL;

   if (target.defaultSchema)
      delete target.defaultSchema;
   if (source.defaultSchema)
   {
      target.defaultSchema = new char [strlen(source.defaultSchema) + 1];
      strcpy(target.defaultSchema, source.defaultSchema);
   }
   else
      target.defaultSchema = NULL;

   if (target.dbUserPassword)
       delete target.dbUserPassword;
   if (source.dbUserPassword)
       {
           target.dbUserPassword = new char [strlen(source.dbUserPassword) + 1];
           strcpy(target.dbUserPassword, source.dbUserPassword);
       }
   else
       target.dbUserPassword = NULL;
   
   target.effectiveUserID = source.effectiveUserID;
   target.sessionUserID = source.sessionUserID;
   target.tenantID = source.tenantID;
   target.redefTime = source.redefTime;
   target.passwdModTime = source.passwdModTime;
   target.tenantRedefTime = source.tenantRedefTime;
   target.tenantAffinity = source.tenantAffinity;
   target.tenantSize = source.tenantSize;
   target.tenantClusterSize = source.tenantClusterSize;
   target.groupList = source.groupList;
   target.configNumber = source.configNumber;
}
//************************* End of copyUsersInfo *******************************


// *****************************************************************************
// *                                                                           *
// * Function: writeLog                                                        *
// *                                                                           *
// *  Logging for testing ODBC authentications.                                *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <text>                        const char *                      In       *
// *    is the string to be written to the log file.                           *
// *                                                                           *
// *****************************************************************************
static void writeLog(const char * text)

{
}
//*************************** End of writeLog **********************************

void DBUserAuth::setAuthenticationType(int type)
{
    gAuthType = (ComAuthenticationType)type;
}

DBUserAuth::CheckUserResult DBUserAuth::getMembersOnLDAPGroup(
    const char *configurationName,
    short configurationNumber,
    const char *groupName,
    short &foundConfigurationNumber,
    std::vector<char *> &members)

{

#ifdef __SQ_LDAP_NO_SECURITY_CHECKING
    return NoGroupsDefined;
#else

    std::vector<AuthEvent> authEvents;
    LDAPConfigNode *searchNode = NULL;

    short numSections = 0;

    if (COM_LDAP_AUTH != gAuthType)
    {
        return NoGroupsDefined;
    }

    LDSearchStatus ret = LDSearchNotFound;
    foundConfigurationNumber = -2;

    if (configurationName && strcmp(configurationName, "ALL") != 0 && configurationNumber != -2)
    {
        searchNode = LDAPConfigNode::GetLDAPConnection(authEvents, configurationName, configurationNumber, SearchConnection);
        if (NULL != searchNode)
        {
            ret = searchNode->searchMemberForGroup(authEvents, groupName, members);
            if (LDSearchResourceFailure == ret)
            {
                searchNode->CloseConnection();
                searchNode->FreeInstance(configurationNumber, SearchConnection);
            }
            else if (LDSearchFound == ret)
            {
                goto END;
            }
        }
    }
    LDAPConfigNode::GetNumberOfSections(authEvents, numSections);

    for (short confignum = 0; confignum < numSections; confignum++)
    {

        searchNode = LDAPConfigNode::GetLDAPConnection(authEvents, NULL, confignum, SearchConnection);

        if (searchNode == NULL)
            continue;

        ret = searchNode->searchMemberForGroup(authEvents, groupName, members);

        if (LDSearchResourceFailure == ret)
        {
            searchNode->CloseConnection();
            searchNode->FreeInstance(confignum, SearchConnection);
        }
        else if (LDSearchFound == ret)
        {
            foundConfigurationNumber = confignum;
            break;
        }
    }
END:
    logAuthEvents(authEvents);
    return ret == LDSearchFound ? GroupListFound : NoGroupsDefined;
#endif
}

void DBUserAuth::getValidLDAPEntries(
    const char *configurationName,
    short configurationNumber,
    std::vector<char *> &inList,
    std::vector<char *> &validList,
    bool isGroup)

{

#ifdef __SQ_LDAP_NO_SECURITY_CHECKING
    return;
#else

    std::vector<AuthEvent> authEvents;
    LDAPConfigNode *searchNode = NULL;

    short numSections = 0;

    if (COM_LDAP_AUTH != gAuthType)
    {
        return;
    }

    if (configurationName && strcmp(configurationName, "ALL") != 0 && configurationNumber != -2)
    {
        searchNode = LDAPConfigNode::GetLDAPConnection(authEvents, configurationName, configurationNumber, SearchConnection);
        if (NULL != searchNode)
        {
            if (LDSearchResourceFailure == searchNode->getValidLDAPEntries(authEvents, inList, validList, isGroup))
            {
                searchNode->CloseConnection();
                searchNode->FreeInstance(configurationNumber, SearchConnection);
            }
            else
            {
                goto END;
            }
        }
    }
    LDAPConfigNode::GetNumberOfSections(authEvents, numSections);

    for (short confignum = 0; confignum < numSections; confignum++)
    {
        searchNode = LDAPConfigNode::GetLDAPConnection(authEvents, NULL, confignum, SearchConnection);

        if (searchNode == NULL)
            continue;

        if (LDSearchResourceFailure == searchNode->getValidLDAPEntries(authEvents, inList, validList, isGroup))
        {
            searchNode->CloseConnection();
            searchNode->FreeInstance(configurationNumber, SearchConnection);
        }
        else if (validList.empty())
        {
            //next?
        }
        else
        {
            goto END;
        }
    }
END:
    logAuthEvents(authEvents);
#endif
}