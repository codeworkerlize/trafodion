#ifndef _DBUSERAUTH_H
#define _DBUSERAUTH_H
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
#include <limits.h>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include "dbsecurity/auth.h"
#include "authEvents.h"

enum LDAPAuthResultEnum {
  AuthResult_Successful,  // User was authenticated on LDAP server
  AuthResult_NotFound,    // User was not found in search
  AuthResult_Rejected,    // User was rejected by LDAP server
  AuthResult_ResourceError
};  // An error prevented authentication

enum LDAPCheckUserResultEnum {
  CheckUserResult_UserExists,        // User was found on LDAP server
  CheckUserResult_UserDoesNotExist,  // User was not found on LDAP server
  CheckUserResult_ErrorInCheck
};  // An error prevented checking, see error detail for reason

enum { MAX_EXTPASSWORD_LEN = 128, MAX_EXTUSERNAME_LEN = 128 };

class DBUserAuthContents;

#if 0
void cacheUserInfo(
   const char    * username,
   const char    * databaseUsername,
   const char    * logonRoleName,
   int32_t         userID,
   bool            isValid,
   int64_t         redefTime);
#endif

class DBUserAuth {
 public:
  enum AuthenticationConfiguration {
    DefaultConfiguration = -2,
  };

  enum CheckUserResult {
    UserExists = 2,    // User was found on identity store
    UserDoesNotExist,  // User was not found on identity store
    ErrorDuringCheck,  // An error internal error occurred during checking
    InvalidConfig,
    GroupListFound,
    NoGroupsDefined,
    LDAPNotEnabled
  };

  // ComAuthenticationType should include "ComSmallDefs.h"
  static void setAuthenticationType(int);

  static void CloseConnection();

  static LDAPAuthResultEnum AuthenticateExternalUser(const char *externalUsername, const char *password);

  static CheckUserResult CheckExternalGroupnameDefined(const char *externalGroupname, const char *configurationName,
                                                       short &foundConfigurationNumber);

  static CheckUserResult CheckExternalUsernameDefined(const char *externalUsername, const char *configurationName,
                                                      short &foundConfigurationNumber);

  static CheckUserResult CheckExternalUsernameDefinedConfigName(const char *externalUsername,
                                                                const char *configurationName,
                                                                short &foundConfigurationNumber);

  static CheckUserResult CheckExternalUsernameDefinedConfigNum(const char *externalUsername, short configurationNumber,
                                                               std::string &configurationName);

  static void GetBlackBox(char *blackBox, bool pretendTokenIsNull = false);

  static size_t GetBlackBoxSize(bool pretendTokenIsNull = false);
  static void FormatStatusMsg(UA_Status status, char *statusMsg);

  static void FreeInstance();

  static DBUserAuth *GetInstance();

  AuthFunction getAuthFunction();

  int32_t getTenantID() const;

  int32_t getUserID() const;

  std::string getConfigName(const short configNumber);

  void getExternalUsername(char *externalUsername, size_t maxLen) const;

  void getDBUserName(char *databaseUsername, size_t maxLen) const;

  void getGroupList(std::set<std::string> &groupList) const;

  DBUserAuth::CheckUserResult getGroupListForUser(const char *databaseUsername, const char *externalUsername,
                                                  const char *currentUsername, int32_t userID, short configNumber,
                                                  std::set<std::string> &groupList) const;

  void getTenantName(char *tenantName, size_t maxLen) const;

  void getTokenKeyAsString(char *tokenKeyString) const;

  size_t getTokenKeySize() const;

  void setAuthFunction(AuthFunction somefunc);

  int verify(const char *username, char *credentials, const char *tenantName, UA_Status &errorDetail,
             AUTHENTICATION_INFO &authenticationInfo, const CLIENT_INFO &client_info,
             PERFORMANCE_INFO &performanceInfo);

  size_t verifyChild(const char *credentials, char *blackbox);

  DBUserAuth::CheckUserResult getMembersOnLDAPGroup(const char *, short, const char *, short &, std::vector<char *> &);

  void getValidLDAPEntries(const char *, short, std::vector<char *> &, std::vector<char *> &, bool);

 private:
  DBUserAuthContents &self;

  DBUserAuth();
  ~DBUserAuth();
  DBUserAuth(DBUserAuth &);

  AuthFunction validateTokenFunc;
};
#endif /* _DBUSERAUTH_H */
