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
//******************************************************************************
//                                                                             *
//     This program is used to test authenticating with an LDAP server         *
//  configured in a Trafodion instance. It may also be used to test the        *
//  correctness of the code that communicates with the LDAP server.            *
//                                                                             *
//   Trafodion does not to be started to run this program.                     *
//                                                                             *
//******************************************************************************
#include "ldapconfignode.h"
#include "authEvents.h"
#include "auth.h"

#include <stdio.h>
#include <string.h>
#include <string>
#include <stdlib.h>
#include <iostream>
#include <unistd.h>
#include <getopt.h>
#include <termios.h>
#include <sys/types.h>
#include <unistd.h>
#include <time.h>
#include <vector>
#include <chrono>

using namespace std;
using namespace std::chrono;

enum Operation {
   Authenticate = 2,
   Lookup = 3
};

AUTH_OUTCOME authenticateLDAPUser(
   std::vector<AuthEvent> & authEvents,
   const char *             username,
   const char *             password,
   const char *             configName,
   short &                  configNumber,
   char *                   searchHostName,
   char *                   authHostName,
   bool                     verbose);

void doAuthenticate(
   std::vector<AuthEvent> & authEvents,
   const char *             username,
   const char *             password,
   const char *             configName,
   short                    configNumber,
   bool                     verbose);

void doCanaryCheck(
   std::vector<AuthEvent> & authEvents,
   const char *             username,
   const char *             configName,
   short                    configNumber,
   bool                     verbose,
   AUTH_OUTCOME             exitCode);

void doGroupCheck(
   std::vector<AuthEvent> & authEvents,
   const char *             groupname,
   const char *             configName,
   short                    configNumber,
   bool                     verbose,
   AUTH_OUTCOME             exitCode);

LDSearchStatus lookupLDAPGroup(
   std::vector<AuthEvent> & authEvents,
   const char *             groupname,
   const char *             configName,
   short &                  configNumber,
   char *                   searchHostName,
   PERFORMANCE_INFO &       performanceInfo);


LDSearchStatus lookupLDAPUser(
   std::vector<AuthEvent> & authEvents,
   const char *             username,
   const char *             configName,
   short &                  configNumber,
   char *                   searchHostName,
   PERFORMANCE_INFO &       performanceInfo);

LDSearchStatus lookupLDAPUserAll(
   std::vector<AuthEvent> & authEvents,
   const char *             username,
   const char *             configName,
   short      &             configNumber,
   char       *             searchHostName,
   string     &             userDN,
   PERFORMANCE_INFO &       performanceInfo);

void printTime();

void reportResults(
    Operation                      operation,
    std::vector<AuthEvent>       & authEvents,
    const char                   * username,
    AUTH_OUTCOME                   outcome,
    const short                    configNumber,
    int                            verbose);

void setEcho(bool enable);

// *****************************************************************************
// *                                                                           *
// * Function: authenticateLDAPUser                                            *
// *                                                                           *
// *    Authenticate an LDAP user with either the primary or secondary LDAP    *
// * configuration.                                                            *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <authEvents>                    std::vector<AuthEvent> &        Out      *
// *    detailed event results of request                                      *
// *                                                                           *
// *  <username>                      const char *                    In       *
// *    is the external username to be checked on the directory server.        *
// *                                                                           *
// *  <password>                      const char *                    In       *
// *    is the password of the external user                                   *
// *                                                                           *
// *  <short>                         LDAPConfigNumber                In       *
// *    is the configuration number to use.                                    *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: AUTH_OUTCOME                                                   *
// *                                                                           *
// *    AUTH_OK                      -- User was authenticated on LDAP server  *
// *    AUTH_REJECTED                -- User was rejected by LDAP server       *
// *    AUTH_FAILED                  -- An error prevented authentication      *
// *                                                                           *
// *****************************************************************************
AUTH_OUTCOME authenticateLDAPUser(
   std::vector<AuthEvent> &       authEvents,
   const char *                   username,
   const char *                   password,
   const char *                   configName,
   short &                        configNumber,
   char *                         searchHostName,
   char *                         authHostName,
   std::set<std::string> &        groupList,
   bool                           verbose)

{

   PERFORMANCE_INFO performanceInfo;
   memset((void *)&performanceInfo, '\0', sizeof(performanceInfo));

   string userDN = "";       // User DN, used to bind to LDAP server
   LDAPConfigNode *searchNode;
   LDSearchStatus searchStatus;

   // Zero len password is a non-auth bind in LDAP, so we treat it
   // as a failed authorization.
   if (strlen(password) == 0)
      return AUTH_REJECTED;

   high_resolution_clock::time_point startTime = time_point_cast<microseconds>(system_clock::now());
   high_resolution_clock::time_point endTime = startTime;  
   if (configName)
   {
      searchNode = LDAPConfigNode::GetLDAPConnection(authEvents,configName,configNumber,SearchConnection,searchHostName);
      endTime = time_point_cast<microseconds>(system_clock::now());
      performanceInfo.searchConnectionTime = duration_cast<microseconds>(endTime - startTime).count();

      if (searchNode == NULL)
         return AUTH_FAILED;

      configNumber = searchNode->getConfigNumber();
      startTime = time_point_cast<microseconds>(system_clock::now());
      searchStatus = searchNode->lookupUser(authEvents,username,userDN);
      endTime = time_point_cast<microseconds>(system_clock::now());
      performanceInfo.searchTime = duration_cast<microseconds>(endTime - startTime).count();
   }
   else
   {
//      configName = NULL;
//      configNumber = -2;
      searchStatus = lookupLDAPUserAll(authEvents,username,configName,configNumber,searchHostName,userDN,performanceInfo);

   }

   if (searchStatus == LDSearchNotFound)
      return AUTH_REJECTED;

   if (searchStatus != LDSearchFound)
      return AUTH_FAILED;


   // User is defined there.  But is their password correct?

   startTime = time_point_cast<microseconds>(system_clock::now());
   LDAPConfigNode *authNode = LDAPConfigNode::GetLDAPConnection(authEvents,configName,configNumber,
                                                                AuthenticationConnection,
                                                                authHostName);
   endTime = time_point_cast<microseconds>(system_clock::now());
   performanceInfo.authenticationConnectionTime = duration_cast<microseconds>(endTime - startTime).count();

   if (authNode == NULL)
      return AUTH_FAILED;

   // User exists, let's validate that non-blank password!
   startTime = time_point_cast<microseconds>(system_clock::now());
   LDAuthStatus authStatus = authNode->authenticateUser(authEvents,userDN.c_str(),password,groupList,performanceInfo);
   endTime = time_point_cast<microseconds>(system_clock::now());
   performanceInfo.authenticationTime = duration_cast<microseconds>(endTime - startTime).count();

   if (verbose)
   {
      cout << endl;
      cout << "Performance details (in microseconds): " << endl;
      cout << "  User search connection time:       " << performanceInfo.searchConnectionTime << endl;
      cout << "  User search time:                  " << performanceInfo.searchTime << endl;
      cout << "  User authenticate connection time: " << performanceInfo.authenticationConnectionTime << endl;
      cout << "  User authenticate time:            " << performanceInfo.authenticationTime << endl;
      cout << "  Group lookup time:                 " << performanceInfo.groupLookupTime << endl;
      cout << endl;
   }

   if (authStatus == LDAuthSuccessful)
      return AUTH_OK;

   if (authStatus == LDAuthRejected)
      return AUTH_REJECTED;

   return AUTH_FAILED;

}
//************************ End of authenticateLDAPUser *************************

// *****************************************************************************
// *                                                                           *
// * Function: doAuthenticate                                                  *
// *                                                                           *
// *    Authenticate an LDAP user with either the primary or secondary LDAP    *
// * configuration.                                                            *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <authEvents>                    std::vector<AuthEvent> &        Out      *
// *    detailed event results of request                                      *
// *                                                                           *
// *  <username>                      const char *                    In       *
// *    is the external username to be checked on the directory server.        *
// *                                                                           *
// *  <password>                      const char *                    In       *
// *    is the password of the external user                                   *
// *                                                                           *
// *  <short>                         LDAPConfigNumber  In                     *
// *    is the type of configuration to use.                                   *
// *                                                                           *
// *  <verbose>                       bool                            In       *
// *    if true, additional output is produced.                                *
// *                                                                           *
// *****************************************************************************
void doAuthenticate(
   std::vector<AuthEvent> &       authEvents,
   const char *                   username,
   const char *                   password,
   const char *                   configName,
   short                          configNumber,
   bool                           verbose)

{

   LDAPConfigNode::ClearRetryCounts();

   char searchHostName[256];
   char authHostName[256];
   std::set<std::string> groupList;

   searchHostName[0] = authHostName[0] = 0;

   AUTH_OUTCOME rc = authenticateLDAPUser(authEvents,username,password,
                                          configName,configNumber,
                                          searchHostName,authHostName,
                                          groupList, verbose);

   if (verbose)
   {
      cout << "Search host name: " << searchHostName << endl;
      cout << "Auth host name: " << authHostName << endl;
   }

   // How'd it go?  In addition to a thumbs up/down, print diagnostics if
   // requested.  This matches production behavior; we log retries if
   // successful, and we log all internal errors as well as number of retries
   // if authentication failed due to a resource error.  Resource errors include
   // problems with the LDAP servers and parsing the LDAP connection configuration
   // file (.traf_authentication_config).
   reportResults(Authenticate,authEvents,username,rc,configNumber,verbose);
std::set<std::string>::iterator it;
   cout << endl;
   for (it=groupList.cbegin(); it != groupList.cend(); ++it)
   {
       string groupNameStr = *it;
       cout << "Member of group: " << groupNameStr.c_str() << endl;
   }

}
//*************************** End of doAuthenticate ****************************

// *****************************************************************************
// *                                                                           *
// * Function: doCanaryCheck                                                   *
// *                                                                           *
// *    Lookup a user on an LDAP server to test configuration and connection.  *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <authEvents>                    std::vector<AuthEvent> &        Out      *
// *    detailed event results of request                                      *
// *                                                                           *
// *  <username>                      const char *                    In       *
// *    is the external username to be checked on the directory server.        *
// *                                                                           *
// *  <LDAPConfigNumber>              short                           In       *
// *    is the type of configuration to use.                                   *
// *                                                                           *
// *  <verbose>                       bool                            In       *
// *    if true, additional output is produced.                                *
// *                                                                           *
// *****************************************************************************
void doCanaryCheck(
   std::vector<AuthEvent> &       authEvents,
   const char *                   username,
   const char *                   configName,
   short                          configNumber,
   bool                           verbose,
   AUTH_OUTCOME                   exitCode)

{
   exitCode = AUTH_OK;

   char searchHostName[256];

   searchHostName[0] = 0;

   PERFORMANCE_INFO performanceInfo;
   LDSearchStatus searchStatus = lookupLDAPUser(authEvents,username,configName,configNumber,searchHostName,performanceInfo);

   if (verbose)
   {
      cout << "Search host name: " << searchHostName << endl;
      cout << endl;
      cout << "Performance details (in microseconds): " << endl;
      cout << "  User search connection time:    " << performanceInfo.searchConnectionTime << endl;
      cout << "  User search time:               " << performanceInfo.searchTime << endl;
      cout << endl;
   }

   switch (searchStatus)
   {
      case LDSearchFound:
         exitCode = AUTH_OK;
         break;
      case LDSearchNotFound:
         exitCode = AUTH_REJECTED;
         break;
      case LDSearchResourceFailure:
         exitCode = AUTH_FAILED;
         break;
      default:
         exitCode = AUTH_FAILED;
   }


   reportResults(Lookup,authEvents,username,exitCode,configNumber,verbose);

}
//*************************** End of doCanaryCheck *****************************

// *****************************************************************************
// *                                                                           *
// * Function: doGroupCheck                                                   *
// *                                                                           *
// *    Lookup a group on an LDAP server.                                      *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <authEvents>                    std::vector<AuthEvent> &        Out      *
// *    detailed event results of request                                      *
// *                                                                           *
// *  <groupname>                      const char *                    In      *
// *    is the external groupname to be checked on the directory server.       *
// *                                                                           *
// *  <LDAPConfigNumber>              short                           In       *
// *    is the type of configuration to use.                                   *
// *                                                                           *
// *  <verbose>                       bool                            In       *
// *    if true, additional output is produced.                                *
// *                                                                           *
// *****************************************************************************
void doGroupCheck(
   std::vector<AuthEvent> &       authEvents,
   const char *                   groupname,
   const char *                   configName,
   short                          configNumber,
   bool                           verbose,
   AUTH_OUTCOME                   exitCode)

{
   exitCode = AUTH_OK;

   char searchHostName[256];

   searchHostName[0] = 0;
   PERFORMANCE_INFO performanceInfo;

   high_resolution_clock::time_point startTime = time_point_cast<microseconds>(system_clock::now());
   LDSearchStatus searchStatus = lookupLDAPGroup(authEvents,groupname,configName,configNumber,searchHostName,performanceInfo);
   high_resolution_clock::time_point endTime = time_point_cast<microseconds>(system_clock::now());
   int64_t groupLookupTime = duration_cast<microseconds>(endTime - startTime).count();

   if (verbose)
   {
      cout << "Search host name: " << searchHostName << endl;
      cout << endl;
      cout << "Performance details (in microseconds): " << endl;
      cout << "  Group search connection time:  " << performanceInfo.searchConnectionTime << endl;
      cout << "  Group search time:             " << performanceInfo.searchTime << endl;
      cout << "  Group lookup time:             " << groupLookupTime << endl;
      cout << endl;
   }

   switch (searchStatus)
   {
      case LDSearchFound:
         exitCode = AUTH_OK;
         break;
      case LDSearchNotFound:
         exitCode = AUTH_REJECTED;
         break;
      case LDSearchResourceFailure:
         exitCode = AUTH_FAILED;
         break;
      default:
         exitCode = AUTH_FAILED;
   }


   reportResults(Lookup,authEvents,groupname,exitCode,configNumber,verbose);

}
//*************************** End of doGroupCheck ******************************

// *****************************************************************************
// *                                                                           *
// * Function: lookupLDAPGroup                                                 *
// *                                                                           *
// *    Determines if the groupname is defined on an LDAP server in the        *
// *  specified configuration.                                                 *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <authEvents>                    std::vector<AuthEvent> &        Out      *
// *    detailed event results of request                                      *
// *                                                                           *
// *  <groupname>                     const char *                    In       *
// *    is the external groupname to be checked on the directory server.       *
// *                                                                           *
// *  <LDAPConfigNumber>              short                           In       *
// *    specifies which configuration (i.e.,which set of LDAP servers) to use  *
// *  when searching for the groupname.                                        *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: LDSearchStatus                                                   *
// *                                                                           *
// *   LDSearchFound            -- Group was found on LDAP server               *
// *   LDSearchNotFound         -- Group was not found on LDAP server           *
// *   LDSearchResourceFailure  -- An error prevented checking                 *
// *                                                                           *
// *****************************************************************************
LDSearchStatus lookupLDAPGroup(
   std::vector<AuthEvent> &        authEvents,
   const char *                    groupname,
   const char *                    configName,
   short &                         configNumber,
   char *                          searchHostName,
   PERFORMANCE_INFO &              performanceInfo)

{
   LDAPConfigNode *searchNode;
   LDSearchStatus searchStatus = LDSearchNotFound;

   high_resolution_clock::time_point startTime;
   high_resolution_clock::time_point endTime;

   std::vector<short int> configNumbers;
   bool foundConfig=false;

   // Verify configuration
   if (configName || configNumber >= 0)
   {
      // User specified configuration, use that
      startTime = time_point_cast<microseconds>(system_clock::now());
      searchNode = LDAPConfigNode::GetLDAPConnection(authEvents,configName,configNumber,SearchConnection,
                                                     searchHostName);
      endTime = time_point_cast<microseconds>(system_clock::now());
      performanceInfo.searchConnectionTime = duration_cast<microseconds>(endTime - startTime).count();

      if (searchNode == NULL)
         return LDSearchResourceFailure;

      configNumber=searchNode->getConfigNumber();
      startTime = endTime;
      searchStatus = searchNode->lookupGroup(authEvents,groupname);
      endTime = time_point_cast<microseconds>(system_clock::now());
      performanceInfo.searchTime = duration_cast<microseconds>(endTime - startTime).count();
      return searchStatus;
   }
   else
   {
      // configuration not specified, go find one
      short numSections = 0;
      LDAPConfigNode::GetNumberOfSections(authEvents, numSections);
      for (short confignum=0; confignum < numSections; confignum++)
      {
         high_resolution_clock::time_point currentStartTime = time_point_cast<microseconds>(system_clock::now());
         searchNode = LDAPConfigNode::GetLDAPConnection(authEvents,NULL,confignum,SearchConnection,searchHostName);
         high_resolution_clock::time_point currentEndTime = time_point_cast<microseconds>(system_clock::now());
         performanceInfo.searchConnectionTime += duration_cast<microseconds>(currentEndTime - currentStartTime).count();

         if (searchNode == NULL)
           return LDSearchResourceFailure;

         currentStartTime = currentEndTime;
         searchStatus = searchNode->lookupGroup(authEvents,groupname);
         currentEndTime = time_point_cast<microseconds>(system_clock::now());
         performanceInfo.searchTime += duration_cast<microseconds>(currentEndTime - currentStartTime).count();

         if (searchStatus == LDSearchFound)
         {
            configNumbers.push_back (confignum);
            if (!foundConfig)
            {
               foundConfig=true;
               configNumber = confignum;
            }
         }
      }
   }

   if (configNumbers.size() > 1)
   {
      std::string configStr;
      char intStr[20];
      vector<short int>::iterator it;
      bool addComma = false;
      for (it=configNumbers.begin(); it!= configNumbers.end(); ++it)
      {
         short num = *it;
         if (addComma) configStr += ", ";
         else addComma = true;
         sprintf(intStr,"%d",num);
         configStr += intStr;
      }
      char eventMsg[MAX_EVENT_MSG_SIZE];
      snprintf(eventMsg, MAX_EVENT_MSG_SIZE,
               "Group %s exists in multiple configurations including "
               "configNumbers (%s) searching configNumber %d" ,
               groupname, configStr.c_str(), configNumber);
      insertAuthEvent(authEvents,DBS_AUTH_CONFIG,eventMsg, LL_INFO);
   }

   if (foundConfig)
     return LDSearchFound;

   return searchStatus;

}
//************************* End of lookupLDAPGroup *****************************

// *****************************************************************************
// *                                                                           *
// * Function: lookupLDAPUserAll                                               *
// *                                                                           *
// *    Determines if the username is present on any LDAP server defined in    *
// *    the configuration.                                                     *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <authEvents>                    std::vector<AuthEvent> &        Out      *
// *    detailed event results of request                                      *
// *                                                                           *
// *  <username>                      const char *                    In       *
// *    is the external username to be checked on the directory server.        *
// *                                                                           *
// *  <LDAPConfigNumber>              short                           In       *
// *    specifies which configuration (i.e.,which set of LDAP servers) to use  *
// *  when searching for the username.                                         *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: LDSearchStatus                                                   *
// *                                                                           *
// *   LDSearchFound            -- User was found on LDAP server               *
// *   LDSearchNotFound         -- User was not found on LDAP server           *
// *   LDSearchResourceFailure  -- An error prevented checking                 *
// *                                                                           *
// *****************************************************************************
LDSearchStatus lookupLDAPUserAll(
   std::vector<AuthEvent> &        authEvents,
   const char *                    username,
   const char *                    configName,
   short      &                    configNumber,
   char *                          searchHostName,
   string     &                    userDN,
   PERFORMANCE_INFO &              performanceInfo)
{
   LDAPConfigNode *searchNode;
   LDSearchStatus searchStatus = LDSearchNotFound;
   short numSections = 0;
   LDAPConfigNode::GetNumberOfSections(authEvents, numSections);
   high_resolution_clock::time_point startTime = time_point_cast<microseconds>(system_clock::now());
   high_resolution_clock::time_point endTime; 

   int64_t searchConnectionTime = 0;
   int64_t searchTime = 0;
   std::vector<short int> configNumbers;
   bool foundConfig=false;
   if (configNumber >= 0)
   {
      searchNode = LDAPConfigNode::GetLDAPConnection(authEvents,NULL,configNumber,SearchConnection,searchHostName);
      endTime = time_point_cast<microseconds>(system_clock::now());
      searchConnectionTime = duration_cast<microseconds>(endTime - startTime).count();

      if (searchNode == NULL)
         searchStatus = LDSearchResourceFailure;
      else
      { 
         high_resolution_clock::time_point currentStartTime = time_point_cast<microseconds>(system_clock::now());
         searchStatus = searchNode->lookupUser(authEvents,username,userDN);
         endTime = time_point_cast<microseconds>(system_clock::now());
         searchTime = duration_cast<microseconds>(endTime - currentStartTime).count();
      }
   }

   else
   {
      for (short confignum=0; confignum < numSections; confignum++)
      {
         string currentUserDN;
         high_resolution_clock::time_point currentStartTime = time_point_cast<microseconds>(system_clock::now());
         searchNode = LDAPConfigNode::GetLDAPConnection(authEvents,NULL,confignum,SearchConnection,searchHostName);
         high_resolution_clock::time_point currentEndTime = time_point_cast<microseconds>(system_clock::now()); 
         searchConnectionTime += duration_cast<microseconds>(currentEndTime - currentStartTime).count();

         if (searchNode == NULL) 
            continue;

         currentStartTime = time_point_cast<microseconds>(system_clock::now());
         searchStatus = searchNode->lookupUser(authEvents,username,currentUserDN);
         currentEndTime = time_point_cast<microseconds>(system_clock::now()); 
         searchTime += duration_cast<microseconds>(currentEndTime - currentStartTime).count();

         if (searchStatus == LDSearchFound)
         {
            configNumbers.push_back (confignum);
            if (!foundConfig)
            {
               foundConfig=true;
               configNumber = confignum;
               userDN = currentUserDN;
            }
    
         }
         else
         {
            searchNode->CloseConnection();
            searchNode->FreeInstance(confignum,SearchConnection);
         }
      }
   }

   performanceInfo.searchConnectionTime = searchConnectionTime;
   performanceInfo.searchTime = searchTime;
   endTime = time_point_cast<microseconds>(system_clock::now());
   performanceInfo.findConfigTime = duration_cast<microseconds>(endTime - startTime).count();

   if (configNumbers.size() > 1)
   {
      std::string configStr;
      char intStr[20];
      vector<short int>::iterator it;
      bool addComma = false;
      for (it=configNumbers.begin(); it!= configNumbers.end(); ++it)
      {
         short num = *it;
         if (addComma) configStr += ", ";
         else addComma = true;
         sprintf(intStr,"%d",num);
         configStr += intStr;
      }
      char eventMsg[MAX_EVENT_MSG_SIZE];
      snprintf(eventMsg, MAX_EVENT_MSG_SIZE,
               "User %s exists in multiple configurations including "
               "configNumbers (%s) authenticating with configNumber %d" ,
               username, configStr.c_str(), configNumber);
      insertAuthEvent(authEvents,DBS_AUTH_CONFIG,eventMsg, LL_INFO);
   }


   if (searchNode != NULL)
   {
      searchNode->CloseConnection();
      searchNode->FreeInstance(configNumber,SearchConnection);
   }
   if (foundConfig)
     return LDSearchFound;

   return searchStatus;
}

// *****************************************************************************
// *                                                                           *
// * Function: lookupLDAPUser                                                  *
// *                                                                           *
// *    Determines if the username is defined on an LDAP server in the         *
// *  specified configuration.                                                 *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <authEvents>                    std::vector<AuthEvent> &        Out      *
// *    detailed event results of request                                      *
// *                                                                           *
// *  <username>                      const char *                    In       *
// *    is the external username to be checked on the directory server.        *
// *                                                                           *
// *  <LDAPConfigNumber>              short                           In       *
// *    specifies which configuration (i.e.,which set of LDAP servers) to use  *
// *  when searching for the username.                                         *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: LDSearchStatus                                                   *
// *                                                                           *
// *   LDSearchFound            -- User was found on LDAP server               *
// *   LDSearchNotFound         -- User was not found on LDAP server           *
// *   LDSearchResourceFailure  -- An error prevented checking                 *
// *                                                                           *
// *****************************************************************************
LDSearchStatus lookupLDAPUser(
   std::vector<AuthEvent> &        authEvents,
   const char *                    username,
   const char *                    configName,
   short &                         configNumber,
   char *                          searchHostName,
   PERFORMANCE_INFO &              performanceInfo)
{
   LDAPConfigNode *searchNode;
   LDSearchStatus searchStatus = LDSearchNotFound;
   string userDN = "";
   high_resolution_clock::time_point startTime;
   high_resolution_clock::time_point endTime;

   if (configName)
   {
      startTime = time_point_cast<microseconds>(system_clock::now());
      searchNode = LDAPConfigNode::GetLDAPConnection(authEvents,configName,configNumber,SearchConnection,
                                                     searchHostName);
      endTime = time_point_cast<microseconds>(system_clock::now());
      performanceInfo.searchConnectionTime = duration_cast<microseconds>(endTime - startTime).count();

      if (searchNode == NULL)
         return LDSearchResourceFailure;

      configNumber=searchNode->getConfigNumber();
      startTime = time_point_cast<microseconds>(system_clock::now());
      LDSearchStatus searchStatus = searchNode->lookupUser(authEvents,username,userDN);
      endTime = time_point_cast<microseconds>(system_clock::now());
      performanceInfo.searchTime = duration_cast<microseconds>(endTime - startTime).count();

      return searchStatus;
   }
   else
      searchStatus = lookupLDAPUserAll(authEvents,username,configName,configNumber,searchHostName,userDN,performanceInfo);
   return  searchStatus;
}
//************************** End of lookupLDAPUser *****************************

// *****************************************************************************
// *                                                                           *
// * Function: printTime                                                       *
// *                                                                           *
// *    Displays the current time in format YYYY-MM-DD hh:mm:ss.               *
// *                                                                           *
// *****************************************************************************
void printTime()

{

time_t t = time(NULL);
struct tm tm = *localtime(&t);

   printf("%4d-%02d-%02d %02d:%02d:%02d\n",tm.tm_year + 1900,tm.tm_mon + 1,
          tm.tm_mday,tm.tm_hour,tm.tm_min,tm.tm_sec);

}
//***************************** End of printTime *******************************


// *****************************************************************************
// *                                                                           *
// * Function: printUsage                                                      *
// *                                                                           *
// *    Displays usage information for this program.                           *
// *                                                                           *
// *****************************************************************************
void printUsage()

{

   cout << "Usage:ldapcheck [option]..." << endl;
   cout << "option ::= --help|-h            display usage information" << endl;
   cout << "           --username=<LDAP-username>" << endl;
   cout << "           --password[=<password>]" << endl;
   cout << "  or       --groupname=<LDAP-groupname>" << endl;
   cout << "           --confignumber=<config-section-number>" << endl;
   cout << "           --configname=<config-section-name>" << endl;
   cout << "           --verbose            Display non-zero retry counts and LDAP errors" << endl;

}
//***************************** End of printUsage ******************************



// *****************************************************************************
// *                                                                           *
// * Function: reportResults                                                   *
// *                                                                           *
// * Logs and optionally displays any retries, errors, and outcome of an       *
// * authentication or lookup operation.                                       *
// *                                                                           *
// *****************************************************************************
void reportResults(
    Operation                      operation,
    std::vector<AuthEvent>       & authEvents,
    const char                   * username,
    AUTH_OUTCOME                   outcome,
    const short                    configNumber,
    int                            verbose)

{

   std::string msg;

   // Report any retries
   size_t bindRetryCount = LDAPConfigNode::GetBindRetryCount();
   size_t searchRetryCount = LDAPConfigNode::GetSearchRetryCount();

   char operationString[20];

   if (operation == Authenticate)
      strcpy(operationString,"Authentication");
   else
      strcpy(operationString,"Lookup");


   // Log and optionally display event for:
   //    search (name lookup) operation that was retried
   if (searchRetryCount > 0)
   {
      char searchRetryMessage[5100];

      sprintf(searchRetryMessage,"%s required %d search retries. ",
              operationString,searchRetryCount);
      msg = searchRetryMessage;
      AuthEvent authEvent (DBS_AUTH_RETRY_SEARCH, msg, LL_INFO);
      authEvent.setCallerName("ldapcheck");
      authEvent.logAuthEvent();
      if (verbose)
         cout << searchRetryMessage << endl;
   }

   // Log and optionally display event for:
   //   any bind (password authentication) operation that was retried
   if (bindRetryCount > 0)
   {
      char bindRetryMessage[5100];

      sprintf(bindRetryMessage,"%s required %d bind retries. ",
              operationString,bindRetryCount);
      msg = bindRetryMessage;
      AuthEvent authEvent (DBS_AUTH_RETRY_BIND, msg, LL_INFO);
      authEvent.setCallerName("ldapcheck");
      authEvent.logAuthEvent();
      if (verbose)
         cout << bindRetryMessage << endl;
   }

  // report any errors and retries

  //  Walk the list of errors encountered during the attempt to authenticate
  //  the username, or username and password, and for each error, log the event
  //  ID and text. If verbose is true and logLevel is above the error
  //  level, send message to standard out.
  std::string callerName ("ldapcheck");

  for (size_t i = 0; i < authEvents.size(); i++)
  {
     AuthEvent authEvent = authEvents[i];
     authEvent.setCallerName(callerName);
     authEvent.logAuthEvent();
     if (verbose)
       cout  << authEvent.getEventText().c_str() << endl;
  }

   std::string configName;
   if (!LDAPConfigNode::GetConfigName(authEvents, configNumber,configName))
     configName = "not available";

  // Finally, report the outcome.
   std::string outcomeDesc = getAuthOutcome(outcome,0);
   char buf[MAX_EVENT_MSG_SIZE];
   snprintf(buf, MAX_EVENT_MSG_SIZE,
                "%s request: externalName %s, "
                "configName '%s' (configNumber %d), "
                "result %d (%s)",
                operationString,
                username,configName.c_str(), configNumber,
                (int)outcome, outcomeDesc.c_str());
   msg = buf;
   AuthEvent authEvent (DBS_AUTHENTICATION_ATTEMPT,msg, LL_INFO);
   authEvent.setCallerName("ldapcheck");
   authEvent.logAuthEvent();
   cout  << authEvent.getEventText().c_str() << endl;

}
//*************************** End of reportResults *****************************



// *****************************************************************************
// *                                                                           *
// * Function: setEcho                                                         *
// *                                                                           *
// *    Enables or disables echoing of input characters.                       *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <enable>                        bool                            In       *
// *    is true if echo should be enabled, false if it should be disabled.     *
// *                                                                           *
// *****************************************************************************
void setEcho(bool enable)

{

termios tty;

   tcgetattr(STDIN_FILENO,&tty);

   if (enable)
      tty.c_lflag |= ECHO;
   else
      tty.c_lflag &= ~ECHO;

   tcsetattr(STDIN_FILENO,TCSANOW,&tty);

}
//****************************** End of setEcho ********************************


#pragma page "main"
// *****************************************************************************
// *                                                                           *
// * Function: main                                                            *
// *                                                                           *
// *    Parses the arguments and attempt to authentication the username and    *
// * password.                                                                 *
// *                                                                           *
// * Trafodion does not to be started to run this program.                     *
// *                                                                           *
// *****************************************************************************
int main(int argc,char *argv[])

{
   // ldapcheck needs a username.  If not supplied, issue an error
   // and print usage information.
   if (argc <= 1)
   {
      cout << "Username required to check LDAP" << endl;
      printUsage();
      exit(1);
   }

   // Help!
   if (strcmp(argv[1],"-h") == 0 || strcmp(argv[1],"--help") == 0)
   {
      printUsage();
      exit(0);
   }

   int c;
   int verbose = 0;
   string password;
   char username[129];
   char groupname[129];
   bool usernameSpecified = false;
   bool passwordSpecified = false;
   bool groupnameSpecified = false;
   bool configNumberSpecified = false;
   short configNumber = -2;
   int anum=0;
   char inconfigName[129]={0};
   char *configName = NULL;
   bool configNameSpecified = false;
   int loopCount = 1;
   int delayTime = 0;
   bool looping = false;

   // Walk the list of options.  Username and password are required, although
   // the password can be left blank and prompted for.
   while (true)
   {
      static struct option long_options[] =
      {
         {"verbose",   no_argument,&verbose,1},
         {"loop",      optional_argument,0,'l'},
         {"delay",     optional_argument,0,'d'},
         {"password",  optional_argument,0,'p'},
         {"username",  optional_argument,0,'u'},
         {"confignumber", optional_argument,0,'c'},
         {"configname", optional_argument,0,'n'},
         {"groupname", optional_argument,0,'g'},
         {0, 0, 0, 0}
      };
      int option_index = 0;

      c = getopt_long(argc,argv,"u:p::",long_options,&option_index);

      // No more options, break out of loop
      if (c == -1)
         break;

      switch (c)
      {
         case 0:
            break;

         case 'u':
            if (usernameSpecified)
            {
               cout << "Username already specified" << endl;
               exit(1);
            }
            if (groupnameSpecified)
            {
               cout << "cannot specify groupname with username" << endl;
               exit(1);
            }
            if (strlen(optarg) >= sizeof(username))
            {
               cout << "Username limited to 128 characters" << endl;
               exit(1);
            }
            strcpy(username,optarg);
            usernameSpecified = true;
            break;

         case 'p':
            if (passwordSpecified)
            {
               cout << "Password already specified" << endl;
               exit(1);
            }
            if (optarg)
            {
               if (strlen(optarg) > 64)
               {
                  cout << "Password limited to 64 characters" << endl;
                  exit(1);
               }
               password = optarg;
            }
            else
               do
               {
                  cout << "Password: ";
                  setEcho(false);
                  getline(cin,password);
                  setEcho(true);
                  cout << endl;
                  if (password.size() > 64)
                     cout << "Password limited to 64 characters" << endl;
                }
                while (password.size() > 64);
            passwordSpecified = true;
            break;

         case 'g':
            if (groupnameSpecified)
            {
               cout << "groupname already specified" << endl;
               exit(1);
            }
            if (usernameSpecified)
            {
               cout << "cannot specify username with groupname" << endl;
               exit(1);
            }
            if (optarg == NULL)
            {
               cout << "Invalid argument" << endl;
               exit (1);
            }
            if (strlen(optarg) >= sizeof(groupname))
            {
               cout << "Groupname limited to 128 characters" << endl;
               exit(1);
            }
            strcpy(groupname,optarg);
            groupnameSpecified = true;
            break;

         case 'l':
            loopCount = atoi(optarg);
            if (loopCount <= 0)
            {
               cout << "Loop count must be at least 1" << endl;
               exit(1);
            }
            looping = true;
         case 'd':
            delayTime = atoi(optarg);
            if (delayTime <= 0)
            {
               cout << "Delay time must be at least 1" << endl;
               exit(1);
            }
         case '?':
            // getopt_long already printed an error message.
            break;

         case 'c':
            if (configNumberSpecified)
            {
               cout << "Confignumber already specified" << endl;
               exit(1);
            }
            if (configNameSpecified)
            {
               cout << "Configname already specified" << endl;
               exit(1);
            }
            errno=0;
            long temp;
            char *endptr;
            temp = strtol(optarg, &endptr, 10);
            if (errno == 0 && !(endptr == NULL || *endptr != 0 || endptr == optarg) )
              anum = (int)temp;
            else
            {
               cout << "Confignumber limited to 0-15" << endl;
               exit(1);
            }
            if (anum  < 0 || anum > 15)
            {
               cout << "Confignumber limited to 0-15" << endl;
               exit(1);
            }
            configNumber = anum;
            configNumberSpecified = true;
            break;

         case 'n':
            if (configNameSpecified)
            {
               cout << "Configname already specified" << endl;
               exit(1);
            }
            if (configNumberSpecified)
            {
               cout << "Confignumber already specified" << endl;
               exit(1);
            }
            if (strlen(optarg) >= sizeof(inconfigName))
            {
               cout << "Configname limited to 128 characters" << endl;
               exit(1);
            }
            strcpy(inconfigName,optarg);
            configName = inconfigName;
            configNameSpecified = true;
            break;

         default:
            cout << "Internal error,option " << c << endl;
            exit(1);
      }
   }

   // If there are any remaining command line arguments, report an error
   if (optind < argc)
   {
      cout << "Unrecognized text" << endl;
      while (optind < argc)
          cout << argv[optind++] << " ";
      cout << endl;
      printUsage();
      exit(1);
   }

   // Verify a username was supplied, or we have nothing to do
   if (!usernameSpecified && !groupnameSpecified)
   {
      cout << "Username or groupname required" << endl;
      printUsage();
      exit(1);
   }

   // allocate authEvents to store any issues
   std::vector<AuthEvent>  authEvents;
   authInitEventLog();

   if (groupnameSpecified)
   {
      AUTH_OUTCOME exitCode = AUTH_OK;
      while (loopCount--)
      {
         if (verbose && looping)
            printTime();
         doGroupCheck(authEvents,groupname,configName,configNumber,verbose,exitCode);
         if (loopCount > 0)
            sleep(delayTime);
      }

      // For the LDAP Canary check mode of ldapcheck, we return one of
      // three exit codes (AUTH_OUTCOME)
      //
      // AUTH_OK:       LDAP configuration and server(s) good
      // AUTH_REJECTED: Group was not defined in LDAP
      // AUTH_FAILED:   Could not communicate with LDAP server(s)
      //    (check LDAP configuration or server(s))
      exit((int)exitCode);
   }

   // If no password is supplied, we just perform a name lookup.  This was
   // added to provide a canary check for the LDAP server without having to
   // supply a valid password.
   if (!passwordSpecified)
   {
      AUTH_OUTCOME exitCode = AUTH_OK;
      while (loopCount--)
      {
         if (verbose && looping)
            printTime();
         doCanaryCheck(authEvents,username,configName,configNumber,verbose,exitCode);
         if (loopCount > 0)
            sleep(delayTime);
      }

      // For the LDAP Canary check mode of ldapcheck, we return one of
      // three exit codes (AUTH_OUTCOME)
      //
      // AUTH_OK:       LDAP configuration and server(s) good
      // AUTH_REJECTED: User was not defined in LDAP
      // AUTH_FAILED:   Could not communicate with LDAP server(s)
      //    (check LDAP configuration or server(s))
      exit((int)exitCode);
   }

   // We have a username and password.  Let's authenticate!
   while (loopCount--)
   {
      if (verbose && looping)
         printTime();
      doAuthenticate(authEvents,username,password.c_str(),configName,configNumber,verbose);
      if (loopCount > 0)
         sleep(delayTime);
   }
   exit(0);

}
//******************************** End of main *********************************
