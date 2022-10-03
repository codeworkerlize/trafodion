#if ! defined(LDAPCONFIGNODE_H)
#define LDAPCONFIGNODE_H
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
#include <string>
#include <set>
#include <list>
#include "auth.h"
#include "authEvents.h"
#include "../../../sqf/inc/cextdecs/cextdecs.h"

using namespace std;

#pragma page "Class LDAPConfigNode"
// *****************************************************************************
// *                                                                           *
// *  Class       LDAPConfigNode                                               *
// *                                                                           *
// *              This class represents server configuration for a LDAP server *
// *              node. It also contains server connection handle and status   *
// *              the connection is been made.                                 *
// *                                                                           *
// *              The class is implemented using the singleton pattern,        *
// *              but with a twist that there are four distinct instances,     *
// *              one for each search node and one for each authentication     *
// *              node in each configuration, primary and secondary.           *
// *                                                                           *
// *              In the future it would be desirable to make this a true      *
// *              singleton class with one instance containing the nodes for   *
// *              all potential configurations.                                *
// *                                                                           *
// *              This class is also implemented as an Envelope, with all      *
// *              of its contents contained in the implementation file.        *
// *                                                                           *
// *  Qualities                                                                *
// *              Abstract:    No                                              *
// *              Assignable:  No                                              *
// *              Copyable:    No                                              *
// *              Derivable:   Yes                                             *
// *                                                                           *
// *****************************************************************************

enum LDAuthStatus {
   LDAuthSuccessful        = 0,
   LDAuthRejected          = 1,
   LDAuthResourceFailure   = 4
};

// results for searching the LDAP Server for a user
enum LDSearchStatus {
  LDSearchFound            = 0,
  LDSearchNotFound         = 1,
  LDSearchResourceFailure  = 2
};

enum LDAPConnectionType{ 
   AuthenticationConnection = 100,
   SearchConnection = 101
};

class ConfigNodeContents;

class LDAPConfigNode
{
public:

   static void ClearRetryCounts();

   static void CloseConnection();
   
   static void FreeInstance(
      short                configNumber,
      LDAPConnectionType   connectionType);

   static size_t GetBindRetryCount();

   static LDAPConfigNode *GetLDAPConnection(
      std::vector<AuthEvent> & authEvents,
      const char             * configName,
      short                    configNumber,
      LDAPConnectionType       connectionType,
      char                   * hostName = NULL);

   static LDAPConfigNode * GetInstance(
      std::vector<AuthEvent> & authEvents,
      short                    configNumber,
      LDAPConnectionType       connectionType);

   static bool GetNumberOfSections(
      std::vector<AuthEvent> & authEvents,
      short                  & numSections);

   static bool GetConfigName(
      std::vector<AuthEvent> & authEvents,
      short                    configNumber,
      std::string            & configName);

   static size_t GetSearchRetryCount();

   static void Refresh(std::vector<AuthEvent> & authEvents);

   static const char * TestGetConfigFilename();

   LDAuthStatus authenticateUser(
      std::vector<AuthEvent> & authEvents,
      const char             * username, 
      const char             * password,
      std::set<string>       & groupList,
      PERFORMANCE_INFO       & performanceInfo);
      
   std::string getConfigName() const;

   short getConfigNumber() const;

   LDSearchStatus lookupGroup(
      std::vector<AuthEvent> & authEvents,
      const char             * inputName);

   LDSearchStatus lookupGroupList(
      std::vector<AuthEvent> & authEvents,
      const char             * username,
      std::set<string>       & groupList,
      bool              isUserDN = false);

   LDSearchStatus lookupUser(
      std::vector<AuthEvent> & authEvents,
      const char             * inputName, 
      string                 & userDN);

   LDSearchStatus searchMemberForGroup(
       std::vector<AuthEvent> &,
       const char *,
       std::vector<char *> &);

    LDSearchStatus getValidLDAPEntries(
    std::vector<AuthEvent> &,
    std::vector<char *> &,
    std::vector<char *> &,
    bool);

   LDSearchStatus getLDAPEntryNames(
       std::vector<AuthEvent> &,
       std::string *,
       std::string *,
       std::list<char *> *);

   //equal searchMemberForGroup for all LDAP group
   LDSearchStatus searchGroupRelationship(
       std::vector<AuthEvent> &,
       list<char *> *,
       map<char *, list<char *> *> *);

   bool getLDAPUserDNSuffix(char **, char **);

   //return this->self.host_->LDAPConfig_ without headerfile
   void* GetConfiguration();

private:

ConfigNodeContents &self;

   LDAPConfigNode();

   LDAPConfigNode(
      short              configNumber,
      LDAPConnectionType connectionType);

   LDAPConfigNode( const LDAPConfigNode & other );

   LDAPConfigNode & operator = ( const LDAPConfigNode & other );

   virtual ~LDAPConfigNode();

   static bool GetConfiguration(
      std::vector<AuthEvent> & authEvents,
      short                  & configNumber);

   static void GetDefaultConfiguration();

   bool initialize(
      std::vector<AuthEvent> & authEvents,
      char                   * hostName);

   void *searchObject(
       std::vector<AuthEvent> &,
       const char *,
       const char *,
       const char *,
       char **);
};

#endif
