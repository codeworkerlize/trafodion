#ifndef _AUTH_H
#define _AUTH_H
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

#include <stdint.h>
#include <string>
#include <set>

#define USERTOKEN_ID_1 '\3'     // User token identifier, must be a sequence
#define USERTOKEN_ID_2 '\4'     // not allowed in password

#define DEFINERTOKEN_ID_1 '\5'  // Identifies that the password is generated
#define DEFINERTOKEN_ID_2 '\6'  // by UDR and contains the original token.

#define DEFINERTOKEN_NO_OF_ITEMS 5 

#define MAX_PASSWORD_LENGTH 128
#define EACH_ENCRYPTION_LENGTH 16
#define AUTH_PASSWORD_KEY "esgyn12345678909"
#define AUTH_PASSWORD_IV "esgyndb1"
#define AUTH_DEFAULT_WORD "esgynDB123"

// Define a function pointer to be used by child MXOSRVRs to verify
// and fetch the authentication data from the parent MXOSRVR

typedef int (*AuthFunction) (char *, int, unsigned char *, int, int *, unsigned char *);

enum UA_Status{
   UA_STATUS_OK = 0,//Returned with error 0, user logged on, no warnings
   
   UA_STATUS_ERR_INVALID = 1,//Username or password is invalid
   UA_STATUS_ERR_REGISTER = 2, // User is not registered
   UA_STATUS_ERR_TENANT = 3, // User is not authorizated for tenant
   UA_STATUS_ERR_REGISTER_FAILED = 4,
   UA_STATUS_ERR_MANUALLY_REGISTER = 5,
   UA_STATUS_ERR_SETAUTHID = 6,
   UA_STATUS_ERROR_BAD_AUTH = 7, // user or tenant is not registered or invalid in MD
   UA_STATUS_ERROR_SYSTEM_TENANT = 8, // can't specify system tenant
   UA_STATUS_PASSWORD_WILL_EXPIRE = 9,
   UA_STATUS_PASSWORD_EXPIRE = 10,
   UA_STATUS_PASSWORD_ERROR_COUNT = 11,
   UA_STATUS_PASSWORD_GRACE_COUNT = 12,
   UA_STATUS_NOT_LICENSE = 13,
   UA_STATUS_ERR_SYSTEM = 20,//Resource or internal error occurred

   UA_STATUS_PARAM1 = 1,
   UA_STATUS_PARAM2 = 2,
   UA_STATUS_PARAM3 = 3,
   UA_STATUS_PARAM4 = 4,
   UA_STATUS_PARAM5 = 5
};

// Define a struct to populate the fields needed by authentication audit

typedef struct client_info
{
   char *client_name;
   char *client_user_name;
   char *application_name;
   client_info():client_name(0),
                 client_user_name(0),
                 application_name(0)
                 {};
} CLIENT_INFO;

typedef struct performance_info
{
   int64_t sqlUserTime;
   int64_t searchConnectionTime;
   int64_t searchTime;
   int64_t authenticationConnectionTime;
   int64_t authenticationTime;
   int64_t tenantGroupCheck;
   int64_t groupLookupTime;
   int64_t retryLookupTime;
   int32_t numberRetries;
   int64_t findConfigTime;
   int64_t registerUserTime;

   performance_info():sqlUserTime(0),
                      searchConnectionTime(0),
                      searchTime(0),
                      authenticationConnectionTime(0),
                      authenticationTime(0),
                      tenantGroupCheck(0),
                      groupLookupTime(0),
                      retryLookupTime(0),
                      numberRetries(0),
                      findConfigTime(0),
                      registerUserTime()
                      {}
} PERFORMANCE_INFO;

typedef struct users_info
{
   /* user information */
   int32_t     effectiveUserID;
   int32_t     sessionUserID;
   char *      extUsername; 
   char *      dbUsername;
   char *      dbUserPassword;
   int64_t     redefTime;
   int64_t     passwdModTime;
   short       configNumber;

   /* client information*/
   char *      client_host_name;
   char *      client_user_name;
   char *      client_addr;

   /* tenant information */
   char *      tenantName;
   char *      defaultSchema;
   int32_t     tenantID;
   int64_t     tenantRedefTime;
   int32_t     tenantAffinity;
   int32_t     tenantSize;
   int32_t     tenantClusterSize;
   std::set<std::string> groupList;

   /* constructor */
   users_info():extUsername(NULL),
                dbUsername(NULL),
                dbUserPassword(NULL),
                redefTime(0LL),
                passwdModTime(0LL),
                tenantName(NULL),
                defaultSchema(NULL),
                client_host_name(NULL),
                client_user_name(NULL),
                client_addr(NULL),
                effectiveUserID(0),
                sessionUserID(0),
                tenantID(0),
                tenantRedefTime(0LL),
                tenantAffinity(0),
                tenantSize(0),
                tenantClusterSize(0),
                configNumber(-1)
                {};
  /* destructor */
  ~users_info()
  {
    if (extUsername)   {delete extUsername; extUsername = NULL;}
    if (dbUsername)    {delete dbUsername; dbUsername = NULL;}
    if (dbUserPassword) {delete dbUserPassword; dbUserPassword = NULL;}
    if (tenantName)    {delete tenantName; tenantName = NULL;}
    if (defaultSchema) {delete defaultSchema; defaultSchema = NULL;}
    if (client_host_name) {delete client_host_name; client_host_name = NULL;}
    if (client_user_name) {delete client_user_name; client_user_name = NULL;}
    if (client_addr)   {delete client_addr; client_addr = NULL;}
  }

  /* reset structure */
  void reset()
  {
    effectiveUserID = 0;
    tenantID = 0;
  }

} USERS_INFO;

typedef struct authentication_info
{
   USERS_INFO  usersInfo;
   int         error;
   UA_Status   errorDetail;
   char        tokenKey[387];
   int32_t     tokenKeySize;
} AUTHENTICATION_INFO;

#endif /* _AUTH_H */

