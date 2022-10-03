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


#include "auth.h"
#include "authEvents.h"
#include "ldapconfignode.h"
#include "ldapconfigfile.h"
#include <sys/stat.h>
#include <algorithm>

// can't find JULIANTIMESTAMP @ compilation, comment out for now
//#include "../../../sqf/inc/cextdecs/cextdecs.h"




// These defines affect openLDAP header files and must appear before s
// those includes.

#undef HAVE_LDAPSSL_INIT
#undef HAVE_LDAP_INIT
#undef HAVE_LDAP_START_TLS_S
#undef LDAP_SET_REBIND_PROC_ARGS

#define HAVE_LDAP_INITIALIZE 1
#define HAVE_LDAP_SET_OPTION 1
#define LDAP_DEPRECATED 1
#define HAVE_LDAP_PARSE_RESULT 1
#define HAVE_LDAP_CONTROLS_FREE 1
#define HAVE_LDAP_SASL_BIND 1
#define LDAP_SASL_SIMPLE 1
//#define OPENLDAP_DEBUG 1

#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <stdlib.h>
#include <stddef.h>
#include <ctype.h>
#include <string>
#include <vector>
#include <memory>
#include <sys/time.h>
#include <sys/param.h>
#include <unistd.h>
#include <netdb.h>
#include <errno.h>
#include <ctime>
#include <netdb.h>
#include <chrono>

using namespace std::chrono;

// LCOV_EXCL_STOP

enum LD_Status {
   LD_STATUS_OK = 200,
   LD_STATUS_RESOURCE_FAILURE = 201
};

enum NodeState {
   NODE_NOT_INITIALIZED = 1000,
   NODE_ACTIVE = 1001
};

enum LDAP_VERSIONS { LDAP_VERSION_2 = 2, LDAP_VERSION_3 = 3};

#define INSERT_EVENT(authEvents,eventID,eventText,severity) insertAuthEvent(authEvents, eventID,eventText,severity)

static size_t numBindRetries = 0;
static size_t numSearchRetries = 0;

#define MAX_ITEM_IN_FILTER 50

#define APPEND_CUSTOMFILTER_TO_FILTER_FOR_GROUP(filter)                                   \
    {                                                                                     \
        string *customFilter = &(this->self.host_->LDAPConfig_->searchGroupCustomFilter); \
        if (!customFilter->empty())                                                       \
        {                                                                                 \
            if ('(' != customFilter->at(0))                                               \
                filter << '(' << *customFilter << ')';                                    \
            else                                                                          \
                filter << *customFilter;                                                  \
        }                                                                                 \
    }

static char *strupr(char *str)
{
    if (NULL == str)
        return str;
    for (int i = 0; str[i] != 0; i++)
        str[i] = toupper(str[i]);
    return str;
}

static char *DN2RDN(char *dn)
{
    //cn=abc,ou=test,dc=abc -> abc
    const char *comma = strchr(dn, ',');
    const char *equal= strchr(dn, '=');
    if (NULL != comma && NULL != equal)
    {
        size_t len = comma - equal -1;
        if (len > 0)
        {
            char *tmp = new char[len + 1];
            strncpy(tmp, equal + 1, len);
            tmp[len] = 0;
            return strupr(tmp);
        }
    }
    return NULL;
}

static char *GetRDN(char *dn)
{
    //cn=abc,ou=test,dc=abc -> cn
    const char *equal = strchr(dn, '=');
    if (NULL != equal)
    {
        size_t len = equal - dn;
        if (len > 0)
        {
            char *tmp = new char[len + 1];
            strncpy(tmp, dn, len);
            tmp[len] = 0;
            return strupr(tmp);
        }
    }
    return NULL;
}

char *formatDN(char *dn)
{
    char *str = NULL;
    char *new_dn = NULL;
    ldap_dn_normalize(dn, LDAP_DN_FORMAT_LDAPV3,
                      &str, LDAP_DN_FORMAT_LDAPV3);
    new_dn = new char[strlen(str) + 1];
    strcpy(new_dn, str);
    ldap_memfree(str);
    return new_dn;
}

//copy from openldap\slapd\dn.c
#define DN_SEPARATOR(c) ((c) == ',')
static int dnIsSuffix(
    const struct berval *dn,
    const struct berval *suffix)
{
    int d;

    d = dn->bv_len - suffix->bv_len;

    /* empty suffix matches any dn */
    if (suffix->bv_len == 0)
    {
        return 1;
    }

    /* suffix longer than dn */
    if (d < 0)
    {
        return 0;
    }

    /* no rdn separator or escaped rdn separator */
    if (d > 1 && !DN_SEPARATOR(dn->bv_val[d - 1]))
    {
        return 0;
    }

    /* no possible match or malformed dn */
    if (d == 1)
    {
        return 0;
    }

    /* compare */
    return (strncasecmp(dn->bv_val + d, suffix->bv_val, suffix->bv_len) == 0);
}

static bool isValidDN(char *dn, const char *validRdn, const char *base)
{
    struct berval dn1, dn2;
    bool ret = false;
    char *rdn = GetRDN(dn);
    if (NULL == rdn)
        return false;
    if (0 != strcasecmp(rdn, validRdn))
        goto END;
    dn1.bv_val = dn;
    dn1.bv_len = strlen(dn);
    dn2.bv_val = (char *)base;
    dn2.bv_len = strlen(base);
    ret = dnIsSuffix(&dn1, &dn2);
END:
    if (rdn)
        delete[] rdn;
    return ret;
}

class ConfigHostContents
{
public:
   ConfigHostContents();
   ConfigHostContents(ConfigHostContents &rhs);
   ConfigHostContents& operator=( const ConfigHostContents& rhs );
   void refresh(LDAPHostConfig &);

   LDAPHostConfig      *LDAPConfig_;
   long                 lastHostIndex_;
   string               lastHostName_;
   vector<string>       excludedHostNames;
};

class ConfigNodeContents
{
public:
   ConfigNodeContents(
      short                          configNumber,
      LDAPConnectionType             connectionType);

   short                              configNumber_;
   LDAPConnectionType                 connectionType_;
   LDAP *                             searchLD_;    // OpenLDAP connection for search
   LDAP *                             authLD_;      // OpenLDAP connection for auth
   NodeState                          status_;
   ConfigHostContents *               host_;
};

static void addExcludedHostName(
   std::vector<AuthEvent> & authEvents,
   ConfigNodeContents     & self,
   const char             * hostName);

static LDAuthStatus bindUser(
   std::vector<AuthEvent> & authEvents,
   ConfigNodeContents     & self,
   const char             * username,
   const char             * password,
   bool                     reconnect,
   int                    & LDAPError);

static LDSearchStatus getLDAPGroupsForUser(
   std::vector<AuthEvent> & authEvents,
   ConfigNodeContents     & self,
   const char             * username,
   std::set<string>       & groupList);

static bool checkLDAPGroup(
   std::vector<AuthEvent> & authEvents,
   ConfigNodeContents     & self,
   const char             * groupname);

static int closeConnection(ConfigNodeContents  & self);

static inline bool connectToHost(
   std::vector<AuthEvent> & authEvents,
   ConfigNodeContents     & self,
   const char             * hostName,
   bool                     isLoadBalanceHost,
   LDAPURLDesc            & url);

static LD_Status connectToURL(
   std::vector<AuthEvent> & authEvents,
   ConfigNodeContents     & self,
   LDAPURLDesc            & url);

static void convertUsername(string & username);

static short getConfigNumberForName(
      const char * configName);

static bool getEffectiveHostName(
   const char * hostName,
   char *     effectiveHostName);

static bool getNonExcludedHostName(
   const char *          hostName,
   char *                effectiveHostName,
   const vector<string> &excludedHosts,
   bool                  isLoadBalanceHost,
   bool                  shouldExcludeBadHosts,
   int                   retryCount,
   int                   retryDelay);

static LD_Status initConnection(
   std::vector<AuthEvent>  & authEvents,
   ConfigNodeContents      & self,
   char *                    hostName,
   bool                      skipHost = false);

static bool isHostNameExcluded(
   const char *           hostName,
   const vector<string> & excludedHosts);

inline static void logConfigFileError(
   std::vector<AuthEvent>  & authEvents,
   LDAPConfigFileErrorCode   fileCode,
   const char*               filePath,
   int                       lastLineNumber,
   string                  & lastLine);

static bool readLDAPConfigFile(std::vector<AuthEvent> & authEvents);

static LDSearchStatus searchGroup(
   std::vector<AuthEvent>  & authEvents,
   ConfigNodeContents      & self,
   const char              * inputName);

static LDSearchStatus searchUser(
   std::vector<AuthEvent>  & authEvents,
   ConfigNodeContents      & self,
   const char              * inputName,
   string                  & userDN);

static LDSearchStatus searchUserByDN(
   std::vector<AuthEvent>  & authEvents,
   ConfigNodeContents      & self,
   const string            & userDN);

static bool selfCheck(
   ConfigNodeContents   & self,
   bool                   isInitialized);

inline static bool shouldReadLDAPConfig();
// Should have an array of configurations.  Mgr hands out.
static LDAPConfigFile configFile;
static time_t lastRefreshTime = 0;
static LDAPFileContents config;
static ConfigHostContents hostContent;
// The following static pointers hold the connection node for each of the two
// connection/configuration combinations
// There are "search" and "authenticate" connection types
static LDAPConfigNode *searchNode = NULL;
static LDAPConfigNode *authNode = NULL;

#pragma page "ConfigHostContents::ConfigHostContents"
// *****************************************************************************
// *                                                                           *
// * Function: ConfigHostContents::ConfigHostContents                          *
// *                                                                           *
// *    Constructor of a ConfigHostContents object.                            *
// *                                                                           *
// *****************************************************************************

ConfigHostContents::ConfigHostContents()
: lastHostIndex_(0),
  LDAPConfig_(NULL)

{

}
//**************** End of ConfigHostContents::ConfigHostContents ***************
ConfigHostContents::ConfigHostContents(ConfigHostContents &rhs)

{

   lastHostIndex_ = rhs.lastHostIndex_;
   LDAPConfig_ = rhs.LDAPConfig_;
   lastHostName_ = rhs.lastHostName_;
   excludedHostNames = rhs.excludedHostNames;

}

ConfigHostContents& ConfigHostContents::operator=(const ConfigHostContents& rhs)

{

   lastHostIndex_ = rhs.lastHostIndex_;
   LDAPConfig_ = rhs.LDAPConfig_;
   lastHostName_ = rhs.lastHostName_;
   excludedHostNames = rhs.excludedHostNames;

}

#pragma page "ConfigHostContents::refresh"
// *****************************************************************************
// *                                                                           *
// * Function: ConfigHostContents::refresh                                     *
// *                                                                           *
// *    Refreshes the fields of a ConfigHostContents instance.                 *
// *                                                                           *
// *****************************************************************************

void ConfigHostContents::refresh(LDAPHostConfig &LDAPHostConfig)

{

   LDAPConfig_ = &LDAPHostConfig;
   lastHostIndex_ = 0;
   lastHostName_.clear();
   excludedHostNames.clear();

}
//********************* End of ConfigHostContents::refresh *********************

#pragma page "ConfigNodeContents::ConfigNodeContents"
// *****************************************************************************
// *                                                                           *
// * Function: ConfigNodeContents::ConfigNodeContents                          *
// *                                                                           *
// *    constructor of a ConfigNodeContents object.                            *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameter:                                                               *
// *                                                                           *
// *  <short>                   LDAPConfigNumber                       In      *
// *    specifies the configuration the node should use.                       *
// *                                                                           *
// *  <connectionType>          LDAPConnectionType                     In      *
// *    is the type of connection (auth or search) the node will be used for.  *
// *                                                                           *
// *****************************************************************************

ConfigNodeContents::ConfigNodeContents(
   short                configNumber,
   LDAPConnectionType   connectionType)
: configNumber_(configNumber),
  connectionType_(connectionType),
  searchLD_(NULL),
  authLD_(NULL),
  status_(NODE_NOT_INITIALIZED)

{

}
//**************** End of ConfigNodeContents::ConfigNodeContents ***************



#pragma page "LDAPConfigNode::ClearRetryCounts"
// *****************************************************************************
// *                                                                           *
// * Function: LDAPConfigNode::ClearRetryCounts                                *
// *                                                                           *
// *    Resets the retry counts to zero.                                       *
// *                                                                           *
// *****************************************************************************
void LDAPConfigNode::ClearRetryCounts()

{

   numBindRetries = 0;
   numSearchRetries = 0;

}
//****************** End of LDAPConfigNode::ClearRetryCounts *******************



#pragma page "LDAPConfigNode::CloseConnection"
// *****************************************************************************
// *                                                                           *
// * Function: LDAPConfigNode::CloseConnection                                 *
// *                                                                           *
// *    Closes any currently opened connection.  No error returned to caller   *
// * if close operation fails or if no connections are open.                   *
// *                                                                           *
// *****************************************************************************

void LDAPConfigNode::CloseConnection()

{

   if (authNode != NULL)
      closeConnection(authNode->self);

   if (searchNode != NULL)
      closeConnection(searchNode->self);

}
//****************** End of LDAPConfigNode::CloseConnection ********************




#pragma page "LDAPConfigNode::FreeInstance"
// *****************************************************************************
// *                                                                           *
// * Function: LDAPConfigNode::FreeInstance                                    *
// *                                                                           *
// *    Destroys the singleton instance of a LDAPConfigNode object.            *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <short>                         LDAPConfigNumber                In       *
// *    is the configuration number of the node to be freed.                   *
// *    be freed.                                                              *
// *                                                                           *
// *  <connectionType>                LDAPConnectionType              In       *
// *    is the connection type (auth or search) of the node to be freed.       *
// *                                                                           *
// *****************************************************************************
// LCOV_EXCL_START  -- not called in normal testing

void LDAPConfigNode::FreeInstance(
   short                    configNumber,
   LDAPConnectionType       connectionType)

{

   if (connectionType == AuthenticationConnection)
   {
      if (authNode != NULL)
      {
         delete authNode;
         authNode = NULL;
      }
   }
   else
   {
      if (searchNode != NULL)
      {
         delete searchNode;
         searchNode = NULL;
      }
   }
}
// LCOV_EXCL_STOP
//******************** End of LDAPConfigNode::FreeInstance *********************




// *****************************************************************************
// *                                                                           *
// * Function: GetBindRetryCount                                               *
// *                                                                           *
// *    Returns the number of times this instance has retried a bind operation *
// * since the last initialize() call.                                         *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Returns:  size_t                                                         *
// *                                                                           *
// *****************************************************************************
size_t LDAPConfigNode::GetBindRetryCount()

{

   return numBindRetries;

}
//****************** End of LDAPConfigNode::GetBindRetryCount ******************

#pragma page "LDAPConfigNode::GetNumberOfSections"
//
//
bool LDAPConfigNode::GetNumberOfSections(
   std::vector<AuthEvent> & authEvents,
   short                  & numSections)

{
   numSections = 0;
   if (!configFile.isInitialized())
   {
      if (!readLDAPConfigFile(authEvents))
         return false;
   }
   // Configuration was successfully read, get the number of sections
   numSections = config.section.size();

   return true;

}

//
// *****************************************************************************
// *                                                                           *
// * Function: string::GetConfigName                                           *
// *                                                                           *
// *   Gives the section name for the given configuration number               *
// *                                                                           *
// *****************************************************************************
bool LDAPConfigNode::GetConfigName(
   std::vector<AuthEvent> & authEvents,
   short                    configNumber,
   std::string            & configName)

{

   configName.clear();

   if (!configFile.isInitialized())
   {
      if (!readLDAPConfigFile(authEvents))
         return false;
   }

   short numSections = 0;
   LDAPConfigNode::GetNumberOfSections(authEvents, numSections);

   if (configNumber < 0 || configNumber >= numSections)
      return false;
   else
      configName = config.section[configNumber];

   return true;

}

//***************** End of LDAPConfigNode::GetConfigName *********************

#pragma page "LDAPConfigNode::GetConfiguration"
// *****************************************************************************
// *                                                                           *
// * Function: LDAPConfigNode::GetConfiguration                                *
// *                                                                           *
// *    Gets an LDAP configuration based on the configuration type.  Data is   *
// * either read from a configuration file (.traf_authentication_config) or    *
// * internal values are used.                                                 *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: bool                                                             *
// *                                                                           *
// *  true - LDAP configuration nodes created.                                 *
// * false - Unable to create LDAP configuration nodes.                        *
// *                                                                           *
// *****************************************************************************
bool LDAPConfigNode::GetConfiguration(
   std::vector<AuthEvent> & authEvents,
   short                  & configNumber)

{


// If we have not yet read the config file, attempt to read it.  If that
// fails, return false.  We could create an internal configurations and hope
// for the best.  If so, we would need to report the problem accessing the
// LDAP configuration file.
   if (!configFile.isInitialized())
   {
      if (!readLDAPConfigFile(authEvents))
         return false;
   }
   // Configuration was successfully read, setup host cache
   hostContent.refresh(config.sectionConfig[configNumber]);

   return true;

}
//****************** End of LDAPConfigNode::GetConfiguration *******************



#pragma page "LDAPConfigNode::GetInstance"
// *****************************************************************************
// *                                                                           *
// * Function: LDAPConfigNode::GetInstance                                     *
// *                                                                           *
// *    Constructs or returns the singleton instance of a LDAPConfigNode       *
// * object.  There are four subtypes of LDAPConfigNodes, one for each         *
// * connection type and one for each configuration.  Future work could        *
// * merge the two connection types into one instance and manage the           *
// * configurations using a map.                                               *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <authEvents>                    std::vector<AuthEvent> &        Out      *
// *    detailed event results of request                                      *
// *                                                                           *
// *  <configurationNumber>           short                           In       *
// *    is the configuration name of the node to be obtained.  If name         *
// *  is NULL, a configNumber is chosen based on the setting                   *
// *  in the configuration file data.                                          *
// *                                                                           *
// *  <connectionType>                LDAPConnectionType              In       *
// *    is the connection type (auth or search) of the node to be obtained.    *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: LDAPConfigNode *                                                 *
// *                                                                           *
// * NULL - Unable to obtain the node (not likely)                             *
// *    * - pointer to node matching the config and connection type.           *
// *                                                                           *
// *****************************************************************************

LDAPConfigNode * LDAPConfigNode::GetInstance(
   std::vector<AuthEvent> & authEvents,
   short                    configNumber,
   LDAPConnectionType       connectionType)

{

   if (!GetConfiguration(authEvents,configNumber))
      return NULL;

//
// There are two types of nodes
//
// 1) Authentication (Bind) Node
// 2) Search (Lookup) Node
//
// We may have already create a node of the type desired. Check the static
// pointers, otherwise, instantiate a new instance.
//

   if (connectionType == AuthenticationConnection)
   {
      if (authNode != NULL)
          FreeInstance(configNumber, AuthenticationConnection);

      authNode  = new LDAPConfigNode(configNumber,AuthenticationConnection);
      return authNode;
   }

   if (connectionType == SearchConnection)
   {
      if (searchNode != NULL)
          FreeInstance(configNumber, AuthenticationConnection);

      searchNode  = new LDAPConfigNode(configNumber,SearchConnection);
      return searchNode;
   }

// We should not get here.

// LCOV_EXCL_START
   return NULL;
// LCOV_EXCL_STOP

}
//******************** End of LDAPConfigNode::GetInstance **********************

// *****************************************************************************
// *                                                                           *
// * Function: LDAPConfigNode::GetLDAPConnection                               *
// *                                                                           *
// *    Obtains and initializes a connection node.                             *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <authEvents>                    std::vector<AuthEvent> &        Out      *
// *    detailed event results of request                                      *
// *                                                                           *
// *  <configType>                    LDAPConfigType                  In       *
// *    is the configuration type of the node to be obtained.  If configType   *
// *  is UnknownConfiguration, a configType is chosen based on the setting     *
// *  in the configuration file data.                                          *
// *                                                                           *
// *  <connectionType>                LDAPConnectionType              In       *
// *    is the connection type (auth or search) of the node to be obtained.    *
// *                                                                           *
// *  <hostName>                     char *                           [Out]    *
// *    if specified (non-NULL), passes back the name of the host if the       *
// *  connection is successful.                                                *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: LDAPConfigNode *                                                 *
// *                                                                           *
// * NULL - Unable to obtain or initialize the node                            *
// *    * - Valid connection node                                              *
// *                                                                           *
// *****************************************************************************
LDAPConfigNode *LDAPConfigNode::GetLDAPConnection(
   std::vector<AuthEvent> & authEvents,
   const char *             configName,
   short                    configNumber,
   LDAPConnectionType       connectionType,
   char *                   hostName)

{

// If we have not yet read the config file, attempt to read it.  If that
// fails, return NULL.  We could create an internal configurations and hope
// for the best.  If so, we would need to report the problem accessing the
// LDAP configuration file.
//
   if (!configFile.isInitialized())
   {
      if (!readLDAPConfigFile(authEvents))
         return NULL;
   }

//
// Verify correct section number is passed in 
//
   short numSections = 0;
   LDAPConfigNode::GetNumberOfSections(authEvents, numSections);
   if (!configName && (configNumber >= 0) && (configNumber >= numSections))
   {
      char eventMsg[MAX_EVENT_MSG_SIZE];
      snprintf(eventMsg,  MAX_EVENT_MSG_SIZE, 
               "****** Invalid configuration number %d specified\n",
               configNumber);
      INSERT_EVENT(authEvents, DBS_AUTH_CONFIG_SECTION_NOT_FOUND,eventMsg,LL_ERROR);
      return NULL;
   }

   if (configName || configNumber == -2)
   {
//
//    Find the number corresponding to the configuration name provided
//
      configNumber = getConfigNumberForName(configName);
      if (configNumber == -1)
      {
         char eventMsg[MAX_EVENT_MSG_SIZE];
         snprintf(eventMsg,  MAX_EVENT_MSG_SIZE, 
                  "****** Invalid configuration name '%s' specified\n",
                  configName);
         INSERT_EVENT(authEvents, DBS_AUTH_CONFIG_SECTION_NOT_FOUND,eventMsg,LL_ERROR);
         return NULL;
       }
   }

LDAPConfigNode *node = LDAPConfigNode::GetInstance(authEvents,configNumber,connectionType);

   if (node == NULL || !node->initialize(authEvents,hostName))
   {
      // if node is not NULL, extract any events and put in authEvents
      return NULL;
   }

   return node;

}
//******************* End of LDAPConfigNode::GetLDAPConnection *****************

// *****************************************************************************
// *                                                                           *
// * Function: GetSearchRetryCount                                             *
// *                                                                           *
// *    Returns the number of times this instance has retried a search         *
// * operation since the last initialize call.                               *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Returns:  size_t                                                         *
// *                                                                           *
// *****************************************************************************
size_t LDAPConfigNode::GetSearchRetryCount()

{

   return numSearchRetries;

}
//**************** End of LDAPConfigNode::GetSearchRetryCount ******************



#pragma page "LDAPConfigNode::Refresh"
// *****************************************************************************
// *                                                                           *
// * Function: LDAPConfigNode::Refresh                                         *A
// *                                                                           *
// *    Closes any currently opened connections and rereads configuration file *
// *                                                                           *
// *****************************************************************************

void LDAPConfigNode::Refresh(std::vector<AuthEvent> &authEvents)

{

   CloseConnection();
   readLDAPConfigFile(authEvents);

}
//********************* End of LDAPConfigNode::Refresh *************************



#pragma page "LDAPConfigNode::LDAPConfigNode"
// *****************************************************************************
// *                                                                           *
// * Function: LDAPConfigNode::LDAPConfigNode                                  *
// *                                                                           *
// *    constructor of LDAPConfigNode object.                                  *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameter:                                                               *
// *                                                                           *
// *  <short>                   LDAPConfigNumber                       In      *
// *    is the configuration number of the node.                               *
// *                                                                           *
// *  <connectionType>          LDAPConnectionType                     In      *
// *    is the type of connection (auth or search) the node will be used for.  *
// *                                                                           *
// *****************************************************************************

LDAPConfigNode::LDAPConfigNode(
   short                    configNumber,
   LDAPConnectionType       connectionType)
: self(*new ConfigNodeContents(configNumber,connectionType))
{
}
//******************* End of LDAPConfigNode::LDAPConfigNode ********************


#pragma page "LDAPConfigNode::~LDAPConfigNode"
// *****************************************************************************
// *                                                                           *
// * Function: LDAPConfigNode::~LDAPConfigNode                                 *
// *                                                                           *
// *    Destructor of LDAPConfigNode.  Not called for database authentications *
// * until the process is stopped, so clearing pointers is well, pointless.    *
// * In case any application chooses to load/unload/load, we need to reset     *
// * the static pointers.                                                      *
// *                                                                           *
// *****************************************************************************
// LCOV_EXCL_START
LDAPConfigNode::~LDAPConfigNode()
{

   closeConnection(self);


   if (self.connectionType_ == AuthenticationConnection)
      authNode = NULL;
   else
      searchNode = NULL;

}
// LCOV_EXCL_STOP
//******************* End of LDAPConfigNode::~LDAPConfigNode *******************


// *****************************************************************************
// *                                                                           *
// * Function: authenticateUser                                                *
// *                                                                           *
// *    Performs a LDAP user authentication by calling LDAP bind APIs.         *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <authEvents>                    std::vector<AuthEvent> &        Out      *
// *    detailed event results of request                                      *
// *                                                                           *
// *  <username>                      const char *                    In       *
// *    is the external username. Must be defined on LDAP server.              *
// *                                                                           *
// *  <password>                      const char *                    In       *
// *    is the password.  Cannot be blank.                                     *
// *                                                                           *
// *  <groupList>                     set<string> &                   Out      *
// *    passes back the group list for the user.                               *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Returns:  LDAuthStatus                                                   *
// *                                                                           *
// *       LDAuthSuccessful: Username and password match values on server.     *
// *         LDAuthRejected: Either username or password does not match        *
// *                         the values on the server.                         *
// *  LDAuthResourceFailure: Problem communicating with LDAP server.  Details  *
// *                         in stdout.  For non-Live Feed authentications     *
// *                         error details are also in repository.             *
// *                                                                           *
// *****************************************************************************
LDAuthStatus LDAPConfigNode::authenticateUser(
   std::vector<AuthEvent> & authEvents,
   const char             * username, 
   const char             * password,
   std::set<string>       & groupList,
   PERFORMANCE_INFO       & performanceInfo)
{
   int LDAPError = LDAP_SUCCESS;
   LD_Status status = LD_STATUS_OK;
   char eventMsg[MAX_EVENT_MSG_SIZE];
   //int64_t startTime = JULIANTIMESTAMP();
   high_resolution_clock::time_point startTime = time_point_cast<microseconds>(system_clock::now());
   high_resolution_clock::time_point endTime = startTime;
   LDAuthStatus authStatus = bindUser(authEvents,self,username,password,true,LDAPError);
   //performanceInfo.userLookupTime = JULIANTIMESTAMP() - startTime;
   endTime = time_point_cast<microseconds>(system_clock::now());
   performanceInfo.authenticationTime = duration_cast<microseconds>(endTime - startTime).count();

   if ((authStatus == LDAuthSuccessful) && !(self.host_->LDAPConfig_->searchGroupBase.empty()))
   {
       //switch to SearchConnection
       LDAPConfigNode *searchNode = LDAPConfigNode::GetLDAPConnection(authEvents,NULL,self.configNumber_,
                                                               SearchConnection);
      //startTime = JULIANTIMESTAMP();
      startTime = time_point_cast<microseconds>(system_clock::now());
      LDSearchStatus status = searchNode->lookupGroupList(authEvents,username,groupList,true);
      //LDSearchStatus status = getLDAPGroupsForUser(authEvents,self,username,groupList);
      //performanceInfo.groupLookupTime = JULIANTIMESTAMP() - startTime;
      endTime = time_point_cast<microseconds>(system_clock::now());
      performanceInfo.groupLookupTime = duration_cast<microseconds>(endTime - startTime).count();

      // Should we retry if we are not able to communicate with LDAP? We were successful
      // in binding the user
      if (status == LDSearchResourceFailure)
      {
        searchNode->CloseConnection();
        return LDAuthResourceFailure;
      }
   }

   if (!self.host_->LDAPConfig_->preserveConnection)
   {
      closeConnection(self);
      searchNode->CloseConnection();
   }

   // Unless we encountered a resource error, return the results
   // of the authentication.
   if (authStatus != LDAuthResourceFailure)
      return authStatus;

   // We had a problem communicating with the LDAP server.  Retry as many times
   // as requested, pausing between attempts as configured.
   int retry_count = self.host_->LDAPConfig_->retryCount;
   startTime = time_point_cast<microseconds>(system_clock::now());

   //startTime = JULIANTIMESTAMP();
   while (retry_count--)
   {
      numBindRetries++;
      closeConnection(self);
      sleep(self.host_->LDAPConfig_->retryDelay);
      status = initConnection(authEvents,self,NULL,true);
      if (status == LD_STATUS_OK)
      {
         authStatus = bindUser(authEvents,self,username,password,true,LDAPError);

         //using SearchConnection to do search LDAP group
         LDAPConfigNode *searchNode = LDAPConfigNode::GetLDAPConnection(authEvents, NULL, self.configNumber_,
                                                                        SearchConnection);
         if ((authStatus == LDAuthSuccessful) && !(self.host_->LDAPConfig_->searchGroupBase.empty()))
         {
            endTime = time_point_cast<microseconds>(system_clock::now());
            performanceInfo.retryLookupTime = duration_cast<microseconds>(endTime - startTime).count();
            performanceInfo.numberRetries = numBindRetries; 

            startTime = time_point_cast<microseconds>(system_clock::now());
            LDSearchStatus status = searchNode->lookupGroupList(authEvents,username,groupList,true);
            //LDSearchStatus status = getLDAPGroupsForUser(authEvents,self,username,groupList);
            endTime = time_point_cast<microseconds>(system_clock::now());
            performanceInfo.groupLookupTime = duration_cast<microseconds>(endTime - startTime).count();

            if (status == LDSearchResourceFailure)
            {
              searchNode->CloseConnection();
              //performanceInfo.lookupRetryTime = JULIANTIMESTAMP() - startTime;
              return LDAuthResourceFailure;
            }
         }

         if (!self.host_->LDAPConfig_->preserveConnection)
         {
            closeConnection(self);
            searchNode->CloseConnection();
         }

         // If the retry was successful, then return the results of the
         // authentication.  Otherwise, keep at it until we run out of retries.
         if (authStatus != LDAuthResourceFailure)
         {
            //performanceInfo.lookupRetryTime = JULIANTIMESTAMP() - startTime;
            return authStatus;
         }
      }
      else
      {
      // Should we call initialize and try to read the config file again ?
      //   Refresh(authEvents);
      }

   } 

   //performanceInfo.lookupRetryTime = JULIANTIMESTAMP() - startTime;

   endTime = time_point_cast<microseconds>(system_clock::now());
   performanceInfo.retryLookupTime = duration_cast<microseconds>(endTime - startTime).count();
   performanceInfo.numberRetries = numBindRetries; 

   // We tried, but the LDAP server(s) did not want to cooperate.
   // Log how hard we tried so we don't get blamed.
   if (self.host_->LDAPConfig_->retryCount)
      snprintf(eventMsg, MAX_EVENT_MSG_SIZE, "Failed to authenticate LDAP user %s after %d retries\n",
              username,self.host_->LDAPConfig_->retryCount);
   else
      snprintf(eventMsg, MAX_EVENT_MSG_SIZE, "Failed to authenticate LDAP user %s\n",username);

   INSERT_EVENT(authEvents,DBS_UNKNOWN_AUTH_STATUS_ERROR,eventMsg,LL_ERROR);
   return LDAuthResourceFailure;

}
//****************** End of LDAPConfigNode::authenticateUser *******************


// *****************************************************************************
// *                                                                           *
// * Function: string::getConfigName                                           *
// *                                                                           *
// *    Returns the configuration name assigned to this node.                  *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Returns:  string                                                          *
// *                                                                           *
// *****************************************************************************
string LDAPConfigNode::getConfigName() const

{

   if (self.configNumber_ < 0)
      return "";
   else
      return config.section[self.configNumber_];

}

//***************** End of LDAPConfigNode::getConfigNumber *********************

// *****************************************************************************
// *                                                                           *
// * Function: short::getConfigNumber                                          *
// *                                                                           *
// *    Returns the configuration number assigned to this node.  Useful when   *
// * defaulting and the number is chosen within the class.                     *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Returns:  short                                                          *
// *                                                                           *
// *****************************************************************************
short LDAPConfigNode::getConfigNumber() const

{

   return self.configNumber_;

}

//***************** End of LDAPConfigNode::getConfigNumber *********************


// *****************************************************************************
// *                                                                           *
// * Function: LDAPConfigNode::initialize                                      *
// *                                                                           *
// *    This member function calls LDAP APIs to initialize a connection.       *
// *    This signature of initConnection takes param which is the name of a    *
// *    file that holds the configuration information.   That file is read and *
// *    this configuration's attributes are set accordingly.   The connection  *
// *    is then "initialzied" using those attributes                           *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <authEvents>                    std::vector<AuthEvent> &        Out      *
// *    detailed event results of request                                      *
// *  <hostName>                     char *                           [Out]    *
// *    if specified (non-NULL), passes back the name of the host if the       *
// *  connection is successful.                                                *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Returns:                                                                 *
// *                                                                           *
// *  TRUE: Node initialized and connection is opened.                         *
// *  FALSE: Initialization failed.                                            *
// *                                                                           *
// *****************************************************************************

bool LDAPConfigNode::initialize(
   std::vector<AuthEvent> & authEvents,
   char * hostName)

{

// Verify node has been setup correctly before we attempt to initialize the
// connection and setup the rest of the node.
   if (!selfCheck(self,false))
   {
      INSERT_EVENT(authEvents,DBS_UNKNOWN_AUTH_STATUS_ERROR,"Self check failed in initialize,LL_ERROR",LL_ERROR);
      return false;
   }

bool doRead = shouldReadLDAPConfig();  //ACH pass in self and config

   if (self.status_ == NODE_ACTIVE)
   {
      if (!doRead)
         return true;

      // Preserve connection was enabled, but we are re-reading the LDAP
      // config file, which could change connection settings.
      closeConnection(self);
   }

   if (doRead)
   {
      // If we cannot read the LDAP configuration file (.traf_authentication_config),
      // we can't initialize the node.
      if (!readLDAPConfigFile(authEvents))
         return false;

      // If we have refreshed the configuration, refresh all host config values
         hostContent.refresh(config.sectionConfig[self.configNumber_]);
   }

   self.host_ = &hostContent;

LD_Status retCode = initConnection(authEvents,self,hostName);

   if (retCode == LD_STATUS_OK)
      return true;

int retry_count = self.host_->LDAPConfig_->retryCount;

   while (retry_count-- > 0)
   {
      if (self.connectionType_ == AuthenticationConnection)
         numBindRetries++;
      else
         numSearchRetries++;
      sleep(self.host_->LDAPConfig_->retryDelay);
      retCode = initConnection(authEvents,self,hostName);
      if (retCode == LD_STATUS_OK)
         return true;
   }

char eventMsg[MAX_EVENT_MSG_SIZE];

   if (self.host_->LDAPConfig_->retryCount > 0)
      snprintf(eventMsg, MAX_EVENT_MSG_SIZE, "Unable to establish initial LDAP connection after %d retries, error %d\n",
              self.host_->LDAPConfig_->retryCount,retCode);
   else
      snprintf(eventMsg, MAX_EVENT_MSG_SIZE, "Unable to establish initial LDAP connection, error %d\n",
              retCode);

   INSERT_EVENT(authEvents,DBS_NO_LDAP_SEARCH_CONNECTION,eventMsg,LL_ERROR);

   return false;

}
//********************* End of LDAPConfigNode::initialize **********************



// *****************************************************************************
// *                                                                           *
// * Function: LDAPConfigNode::lookupGroup                                      *
// *                                                                           *
// *    Searches for a group on the directory server(s) associated with        *
// * this node.                                                                *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <authEvents>                    std::vector<AuthEvent> &        Out      *
// *    detailed event results of request                                      *
// *                                                                           *
// *  <inputName>                     const char *                    In       *
// *    is the external groupname to lookup.                                   *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: LDSearchStatus                                                   *
// *                                                                           *
// *            LDSearchFound: Groupname was found on the server.              *
// *         LDSearchNotFound: Groupname was not found on the server.          *
// *  LDSearchResourceFailure: Problem communicating with LDAP server.         *
// *****************************************************************************
LDSearchStatus LDAPConfigNode::lookupGroup(
   std::vector<AuthEvent> & authEvents,
   const char             * inputName)

{
int rc = 0;
char eventMsg[MAX_EVENT_MSG_SIZE];

LDSearchStatus searchStatus = searchGroup(authEvents,self,inputName);

// Unless we could not lookup the user due to a resource error, return
// the results of the search.
   if (searchStatus != LDSearchResourceFailure)
      return searchStatus;

//
// We had a problem communicating with the LDAP server.  Retry as many times
// as requested, pausing between attempts as configured.
//

int retry_count = self.host_->LDAPConfig_->retryCount;

   while (retry_count-- > 0)
   {
      numSearchRetries++;
      closeConnection(self);
      sleep(self.host_->LDAPConfig_->retryDelay);
      LD_Status rc = initConnection(authEvents,self,NULL,true);
      if (rc == LD_STATUS_OK)
      {
         searchStatus = searchGroup(authEvents,self,inputName);

         if (!self.host_->LDAPConfig_->preserveConnection)
            closeConnection(self);

         // Again, if we did not get a resource failure, then return the
         // results of the search.  Otherwise, keep at it until we
         // exhaust the number of retries.
         if (searchStatus != LDSearchResourceFailure)
            return searchStatus;
      }
      else
      {
      // Should we call initialize and try to read the config file again ?
      }

   }

//
// OK, we gave it our best shot, but we could not communicate with the
// configured directory server(s).  Log how hard we tried.
//

   if (self.host_->LDAPConfig_->retryCount > 0)
      snprintf(eventMsg,  MAX_EVENT_MSG_SIZE, "Failed to search for LDAP group %s after %d retries\n",
              inputName,self.host_->LDAPConfig_->retryCount);
   else
      snprintf(eventMsg,  MAX_EVENT_MSG_SIZE, "Failed to search for LDAP group %s\n",inputName);

   INSERT_EVENT(authEvents,DBS_NO_LDAP_SEARCH_CONNECTION,eventMsg,LL_ERROR);
   return LDSearchResourceFailure;

}
//********************* End of LDAPConfigNode::lookupGroup **********************


// *****************************************************************************
// *                                                                           *
// * Function: LDAPConfigNode::lookupGroupList                                 *
// *                                                                           *
// *    Gets the specified user distinguished name and then calls              *
// *    getLDAPGroupsForUser to get the users groups.                          *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <authEvents>                    std::vector<AuthEvent> &        Out      *
// *    detailed event results of request                                      *
// *                                                                           *
// *  <username>                      const char *                    In       *
// *    is the external groupname to lookup.                                   *
// *                                                                           *
// *  <groupList>                     std::set<std::string &          Out      *
// *    is the list of groups for the user, it can be empty                    *
// *                                                                           *
// *  <isUserDN>                     bool                             In       *
// *    the username is LDAP DN                                                *
// *    set isUserDN = true to avoid calling function getLDAPGroupsForUser     *
// *    in this cpp file                                                       *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: LDSearchStatus                                                   *
// *                                                                           *
// *****************************************************************************
LDSearchStatus LDAPConfigNode::lookupGroupList(
   std::vector<AuthEvent> & authEvents,
   const char             * username,
   std::set<std::string>  & groupList,
   bool                     isUserDN)
{
   std::string userDN = "";   
   LDSearchStatus searchStatus = LDSearchNotFound;
   if (isUserDN)
   {
       userDN = username;
   }
   else
   {
       searchStatus = searchUser(authEvents, self, username, userDN);
       if (searchStatus != LDSearchFound)
       {
           if (!self.host_->LDAPConfig_->preserveConnection)
               closeConnection(self);
           return searchStatus;
       }
   }

   if (!(self.host_->LDAPConfig_->searchGroupBase.empty()))
   {
      searchStatus =  getLDAPGroupsForUser(authEvents, self, userDN.c_str(), groupList);
      if (!self.host_->LDAPConfig_->preserveConnection)
         closeConnection(self);
   }

   return searchStatus;

}
//******************* End of LDAPConfigNode::lookupGroupList *******************


// *****************************************************************************
// *                                                                           *
// * Function: LDAPConfigNode::lookupUser                                      *
// *                                                                           *
// *    Searches for a username on the directory server(s) associated with     *
// * this node.                                                                *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <authEvents>                    std::vector<AuthEvent> &        Out      *
// *    detailed event results of request                                      *
// *                                                                           *
// *  <inputName>                     const char *                    In       *
// *    is the external username to lookup.                                    *
// *                                                                           *
// *  <userDN>                        string &                        Out      *
// *    passes back the distingushed name for this user.                       *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: LDSearchStatus                                                   *
// *                                                                           *
// *            LDSearchFound: Username was found on the server.               *
// *         LDSearchNotFound: Username was not found on the server.           *
// *  LDSearchResourceFailure: Problem communicating with LDAP server.         *
// *                           Details in stdout.  For non-Live Feed           *
// *                           authentications, error details are also in      *
// *                           repository.  For Live Feed authentications and  *
// *                           SQL username lookups, data is only stdout.      *
// *                                                                           *
// *****************************************************************************
LDSearchStatus LDAPConfigNode::lookupUser(
   std::vector<AuthEvent> & authEvents,
   const char             * inputName,
   string                 & userDN)

{

int rc = 0;
char eventMsg[MAX_EVENT_MSG_SIZE];

LDSearchStatus searchStatus = searchUser(authEvents,self,inputName,userDN);

   if (!self.host_->LDAPConfig_->preserveConnection)
      closeConnection(self);

// Unless we could not lookup the user due to a resource error, return
// the results of the search.
   if (searchStatus != LDSearchResourceFailure)
      return searchStatus;

//
// We had a problem communicating with the LDAP server.  Retry as many times
// as requested, pausing between attempts as configured.
//

int retry_count = self.host_->LDAPConfig_->retryCount;

   while (retry_count-- > 0)
   {
      numSearchRetries++;
      closeConnection(self);
      sleep(self.host_->LDAPConfig_->retryDelay);
      LD_Status rc = initConnection(authEvents,self,NULL,true);
      if (rc == LD_STATUS_OK)
      {
         searchStatus = searchUser(authEvents,self,inputName,userDN);

         if (!self.host_->LDAPConfig_->preserveConnection)
            closeConnection(self);

         // Again, if we did not get a resource failure, then return the
         // results of the search.  Otherwise, keep at it until we
         // exhaust the number of retries.
         if (searchStatus != LDSearchResourceFailure)
            return searchStatus;
      }
      else
      {
      // Should we call initialize and try to read the config file again ?
      }

   }

//
// OK, we gave it our best shot, but we could not communicate with the
// configured directory server(s).  Log how hard we tried.
//

   if (self.host_->LDAPConfig_->retryCount > 0)
      snprintf(eventMsg,  MAX_EVENT_MSG_SIZE, "Failed to search for LDAP user %s after %d retries\n",
              inputName,self.host_->LDAPConfig_->retryCount);
   else
      snprintf(eventMsg,  MAX_EVENT_MSG_SIZE, "Failed to search for LDAP user %s\n",inputName);

   INSERT_EVENT(authEvents,DBS_NO_LDAP_SEARCH_CONNECTION,eventMsg,LL_ERROR);
   return LDSearchResourceFailure;

}
//********************* End of LDAPConfigNode::lookupUser **********************

// *****************************************************************************
// *                                                                           *
// *  Begin file static functions.                                             *
// *                                                                           *
// *****************************************************************************


// *****************************************************************************
// *                                                                           *
// * Function: addExcludedHostName                                             *
// *                                                                           *
// *  This function adds a host name to the list of excluded host names, and   *
// *  removes old entries if the list is too large.                            *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <authEvents>                    std::vector<AuthEvent> &        Out      *
// *    detailed event results of request                                      *
// *                                                                           *
// *  <self>                         ConfigNodeContents &             In       *
// *    is a reference to an instance of a ConfigNodeContents object.          *
// *                                                                           *
// *  <hostName>                     const char *                     In       *
// *    is name of the host to be excluded.                                    *
// *                                                                           *
// *****************************************************************************
static void addExcludedHostName(
   std::vector<AuthEvent> & authEvents,
   ConfigNodeContents     & self,
   const char             * hostName)


{

   char eventMsg[MAX_EVENT_MSG_SIZE];

   // If the size of the excluded host list is being limited, clear out
   // older excluded hosts to make room for the newest entry.
   if (self.host_->LDAPConfig_->maxExcludeListSize > 0)
      while (self.host_->excludedHostNames.size() >= self.host_->LDAPConfig_->maxExcludeListSize)
      {
         snprintf(eventMsg, MAX_EVENT_MSG_SIZE, "Exclude list full, LDAP server %s removed from exclude list\n",
                 self.host_->excludedHostNames[0].c_str());
         INSERT_EVENT(authEvents,DBS_UNKNOWN_AUTH_STATUS_ERROR,eventMsg,LL_ERROR);

         self.host_->excludedHostNames.erase(self.host_->excludedHostNames.begin());
      }

   self.host_->excludedHostNames.push_back(hostName);

   snprintf(eventMsg, MAX_EVENT_MSG_SIZE, "LDAP server %s added to exclude list\n",hostName);
   INSERT_EVENT(authEvents,DBS_UNKNOWN_AUTH_STATUS_ERROR,eventMsg,LL_ERROR);

}
//************************ End of addExcludedHostName **************************




// *****************************************************************************
// *                                                                           *
// * Function: bindUser                                                        *
// *                                                                           *
// * Performs a LDAP user authentication by calling LDAP bind.                 *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <authEvents>                    std::vector<AuthEvent> &        Out      *
// *    detailed event results of request                                      *
// *                                                                           *
// *  <self>                         ConfigNodeContents &             In       *
// *    is a reference to an instance of a ConfigNodeContents object.          *
// *                                                                           *
// *  <username>                     const char *                     In       *
// *    is the name of the user to attempt to bind (logon).                    *
// *                                                                           *
// *  <password>                     const char *                     In       *
// *    is the password of the user being logged on.                           *
// *                                                                           *
// *  <reconnect>                    bool                             In       *
// *    is true, if the bind operation fails, attempt to reconnect (once).     *
// *                                                                           *
// *  <LDAPError>                    int &                            Out      *
// *    passes back the LDAP error associated the the last suboperation        *
// *  of the bind operation.                                                   *
// *                                                                           *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns:  LDAuthStatus                                                    *
// *                                                                           *
// * LDAuthSuccessful: username and password are good.                         *
// * LDAuthRejected: password does not match username.                         *
// * LDAuthResourceFailure: LDAP operation failed unexpectedly.                *
// *                                                                           *
// *****************************************************************************
static LDAuthStatus bindUser(
   std::vector<AuthEvent> & authEvents,
   ConfigNodeContents     & self,
   const char             * username,
   const char             * password,
   bool                     reconnect,
   int                    & LDAPError)

{

int rc, msgid, err;
struct timeval timeout;
LDAP *ld;
LDAPMessage *result;
char eventMsg[MAX_EVENT_MSG_SIZE];

int parserc;
LDAPControl **psrvctrls = NULL;
struct berval userpw;
char *errorTextString;
bool isInitialized = reconnect;

   LDAPError = LDAP_SUCCESS;

   while (true)
   {
      if (!selfCheck(self,isInitialized))
      {
         INSERT_EVENT(authEvents,DBS_UNKNOWN_AUTH_STATUS_ERROR,"Self check failed in bindUser",LL_ERROR);

         return LDAuthResourceFailure;
      }

      if (self.connectionType_ == AuthenticationConnection)
         ld = self.authLD_;
      else
         ld = self.searchLD_;
      userpw.bv_val = (char *)password;
      userpw.bv_len = (userpw.bv_val != 0) ? strlen (userpw.bv_val) : 0;
      LDAPError = ldap_sasl_bind(ld,username,LDAP_SASL_SIMPLE,&userpw,psrvctrls,
                                 0,&msgid);

      if (LDAPError != LDAP_SUCCESS || msgid == -1)
      {
// LCOV_EXCL_START
         if (LDAPError == LDAP_SERVER_DOWN && reconnect)
         {
            // reconnect & retry
            closeConnection(self);
            LD_Status status = initConnection(authEvents,self,NULL,true);
            if (status != LD_STATUS_OK)
            {
               INSERT_EVENT(authEvents,DBS_UNKNOWN_AUTH_STATUS_ERROR,"LDAP Auth Error in bindUser; unable to connect to server",LL_ERROR);
               return LDAuthResourceFailure;
            }
            reconnect = false;
            continue;
         }
         snprintf(eventMsg,  MAX_EVENT_MSG_SIZE, "LDAP Auth Error in bindUser; error code: %ld, ", (long) LDAPError);
         errorTextString = ldap_err2string(LDAPError);
         strncat(eventMsg, errorTextString, (MAX_EVENT_MSG_SIZE - (strlen(eventMsg)+4)));
         strcat(eventMsg,"\n");
         INSERT_EVENT(authEvents,DBS_NO_LDAP_AUTH_CONNECTION,eventMsg,LL_ERROR);
         return LDAuthResourceFailure;
// LCOV_EXCL_STOP
      }

      // Use value set by LDAP_OPT_TIMEOUT, pass NULL as argument
      // timeout.tv_sec = 5;
      // timeout.tv_usec = 0;   // set timeout to 5 sec

      rc = ldap_result (ld, msgid, 1, NULL, &result);
      if (rc == -1 || rc == 0)    // timeout if rc =0, server down if rc = -1
      {
         // retry if timeout
         if (reconnect)
         {
            // reconnect & retry
            closeConnection(self);
            LD_Status status = initConnection(authEvents,self,NULL,true);
            if (status != LD_STATUS_OK)
            {
// LCOV_EXCL_START
               INSERT_EVENT(authEvents,DBS_UNKNOWN_AUTH_STATUS_ERROR,"LDAP Auth Error in bindUser; unable to connect to server",LL_ERROR);
               return LDAuthResourceFailure;
// LCOV_EXCL_STOP
            }
            reconnect = false;
            continue;
         }
         ldap_get_option(ld, LDAP_OPT_ERROR_NUMBER, &err);
         snprintf(eventMsg,  MAX_EVENT_MSG_SIZE, "LDAP Auth Error in bindUser; error code: %ld, ", (long)err);
         errorTextString = ldap_err2string(err);
         strncat(eventMsg, errorTextString, (MAX_EVENT_MSG_SIZE - (strlen(eventMsg)+4)));
         strcat(eventMsg, "\n");
         INSERT_EVENT(authEvents,DBS_NO_LDAP_AUTH_CONNECTION,eventMsg,LL_ERROR);
         LDAPError = err;
         return LDAuthResourceFailure;
      }


      LDAPControl **ctrls;
      parserc = ldap_parse_result (ld, result, &rc, 0, 0, 0, &ctrls, 1);
      if (parserc != LDAP_SUCCESS)
      {
// LCOV_EXCL_START
         char *p = ldap_err2string(parserc);
         if (p != NULL)
         {
            strcpy(eventMsg, "LDAP Auth Error in bindUser; Failed to get bind result: ");
            strncat(eventMsg, p, (MAX_EVENT_MSG_SIZE - (strlen(eventMsg)+4)) );
            strcat(eventMsg, "\n");
         }
         else
            strcpy(eventMsg, "LDAP Auth Error in bindUser; Failed to get bind result.\n");
         INSERT_EVENT(authEvents,DBS_UNKNOWN_AUTH_STATUS_ERROR,eventMsg,LL_ERROR);
         LDAPError = parserc;
         return LDAuthResourceFailure;
// LCOV_EXCL_STOP
      }

// *****************************************************************************
// *                                                                           *
// *    Password policy is not supported.  Support would include notify users  *
// * that their authentication failed because their password has expired or    *
// * warning that is about to expire, or that they are in a grace period and   *
// * need to change their password before a certain date or some number of     *
// * authentications.  The code is still present, but beyond the check is not  *
// * executed. If supported is ever added, the structure needs to be           *
// * initialized and passed back to callers.                                   *
// *                                                                           *
// *****************************************************************************
// Error code returned from password policy control
#define PSWPOLICY_PASSWORDEXPIRED   0
#define PSWPOLICY_ACCOUNTLOCKED     1
#define PSWPOLICY_FIRST_AFTER_RESET 2
#define PSWPOLICY_NOERROR           65535

      if (ctrls)
      {
// LCOV_EXCL_START
         class LdapPasswordPolicy {
         public:
            int error;  // error code returned from password policy control
            int expire;
            int grace;
         };
         // pre-set password policy (not supported)
         //if (pwdPolicy != NULL)
         //{
         //   pwdPolicy->error = PSWPOLICY_NOERROR;
         //   pwdPolicy->expire = -1;
         //   pwdPolicy->grace = -1;
         //}
         LDAPControl *ctrl = ldap_find_control( LDAP_CONTROL_PASSWORDPOLICYRESPONSE, ctrls );
         if (ctrl)
         {
            LdapPasswordPolicy pwdPolicy;
            ldap_parse_passwordpolicy_control(ld,ctrl,&pwdPolicy.expire,
                                              &pwdPolicy.grace,
                                              (LDAPPasswordPolicyError *)&pwdPolicy.error );
         }
// LCOV_EXCL_STOP
      }

      switch (rc)
      {
         case LDAP_SUCCESS:
            return LDAuthSuccessful;
            break;
         case LDAP_NO_SUCH_OBJECT:
         case LDAP_INVALID_CREDENTIALS:
            return LDAuthRejected;
            break;
         default:
// LCOV_EXCL_START
            snprintf(eventMsg,  MAX_EVENT_MSG_SIZE, "LDAP Auth Error in bindUser; error code: %ld, ", (long)rc);
            errorTextString = ldap_err2string(rc);
            strncat(eventMsg, errorTextString, (MAX_EVENT_MSG_SIZE - (strlen(eventMsg)+4)));
            strcat(eventMsg, "\n");
            INSERT_EVENT(authEvents,DBS_NO_LDAP_AUTH_CONNECTION,eventMsg,LL_ERROR);
            LDAPError = rc;
            return LDAuthResourceFailure;
            break;
// LCOV_EXCL_STOP
      }
   }  // while (true)

}
//***************************** End of bindUser ********************************



// *****************************************************************************
// *                                                                           *
// * Function: closeConnection                                                 *
// *    This function calls LDAP APIs to close a connection.                   *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <self>                         ConfigNodeContents &             In       *
// *    is a reference to an instance of a ConfigNodeContents object.          *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Returns: OpenLDAP error code                                             *
// *                                                                           *
// *****************************************************************************
static int closeConnection(ConfigNodeContents  & self)

{

int rc = LDAP_SUCCESS;

   if (self.connectionType_ == AuthenticationConnection && self.authLD_ != NULL)
   {
      rc = ldap_unbind(self.authLD_);
      self.authLD_ = NULL;
      self.status_ = NODE_NOT_INITIALIZED;
      return rc;
   }

   if (self.connectionType_ == SearchConnection && self.searchLD_ != NULL)
   {
      rc = ldap_unbind(self.searchLD_);
      self.searchLD_ = NULL;
      self.status_ = NODE_NOT_INITIALIZED;
      return rc;
   }

   return rc;

}
//*************************** End of closeConnection ***************************


// *****************************************************************************
// *                                                                           *
// * Function: connectToHost                                                   *
// *                                                                           *
// *  This function attempts to connect to an LDAP host.                       *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <authEvents>                    std::vector<AuthEvent> &        Out      *
// *    detailed event results of request                                      *
// *                                                                           *
// *  <self>                         ConfigNodeContents &             In       *
// *    is a reference to an instance of a ConfigNodeContents object.          *
// *                                                                           *
// *  <hostName>                     const char *                     In       *
// *    is a string containing the host name.                                  *
// *                                                                           *
// *  <isLoadBalanceHost>            bool                             In       *
// *    is true if <hostName> is a load balancing host.                        *
// *                                                                           *
// *  <url>                          LDAPURLDesc &                    In       *
// *    is a reference to a LDAP URL Description.                              *
// *                                                                           *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: bool                                                             *
// *                                                                           *
// * true:  a "good" host name was found.                                      *
// * false: Could not find a host name not on the exclude list. :(             *
// *                                                                           *
// *****************************************************************************
static inline bool connectToHost(
   std::vector<AuthEvent> & authEvents,
   ConfigNodeContents & self,
   const char *         hostName,
   bool                 isLoadBalanceHost,
   LDAPURLDesc &        url)

{

char effectiveHostName[MAX_HOSTNAME_LENGTH + 1];

// If we can't get a good host, don't even bother trying to connect (waste of
// time!), just return false and let the caller deal with it.
   if (!getNonExcludedHostName(hostName,effectiveHostName,
                               self.host_->excludedHostNames,
                               isLoadBalanceHost,
                               self.host_->LDAPConfig_->excludeBadHosts,
                               self.host_->LDAPConfig_->retryCount,
                               self.host_->LDAPConfig_->retryDelay))
      return false;

// We have a good host.  Let's try to connect.
   url.lud_host = effectiveHostName;
   LD_Status status = connectToURL(authEvents,self,url);
   if (status == LD_STATUS_OK)
   {
      self.host_->lastHostName_ = url.lud_host;
      return true;
   }

// Could not connect to that host.  If we are excluding bad hosts, add it
// to the exclude host name list.
   if (self.host_->LDAPConfig_->excludeBadHosts)
      addExcludedHostName(authEvents,self,url.lud_host);

   return false;

}
//**************************** End of connectToHost ****************************


// *****************************************************************************
// *                                                                           *
// * Function: connectToURL                                                    *
// *                                                                           *
// *    This function calls LDAP APIs to initialize a connection.              *
// *    It will also make an LDAP call to bind with search user so to make     *
// *    sure later a user search can be performed.                             *
// *    This is called by the outer initConnection member function for each    *
// *    url configured.                                                        *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <authEvents>                    std::vector<AuthEvent> &        Out      *
// *    detailed event results of request                                      *
// *                                                                           *
// *  <self>                         ConfigNodeContents &             In       *
// *    is a reference to an instance of a ConfigNodeContents object.          *
// *                                                                           *
// *  <url>                          LDAPURLDesc &                    In       *
// *    is a reference to a LDAP URL Description.                              *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: LD_Status                                                        *
// *                                                                           *
// * LD_STATUS_OK: a connection was made                                       *
// * LD_STATUS_RESOURCE_FAILURE: a connection could not be made                *
// *                                                                           *
// *****************************************************************************
static LD_Status connectToURL(
   std::vector<AuthEvent> & authEvents,
   ConfigNodeContents & self,
   LDAPURLDesc        & url)

{

int version;
int debug = 0;
int rc;
char eventMsg[MAX_EVENT_MSG_SIZE];
char *errorTextString;

LDAP *ld = NULL;
struct timeval tv;

   char *ldapuri = ldap_url_desc2str( &url );
   rc = ldap_initialize (&ld, ldapuri);
   if (rc != LDAP_SUCCESS)
   {
// LCOV_EXCL_START
      snprintf(eventMsg,  MAX_EVENT_MSG_SIZE, "ldap_initialize failed for LDAP server %s. Error: %d, ",url.lud_host, rc);
      errorTextString = ldap_err2string(rc);
      strncat(eventMsg, errorTextString, (MAX_EVENT_MSG_SIZE - (strlen(eventMsg)+4)));
      strcat(eventMsg, "\n");
      INSERT_EVENT(authEvents,DBS_NO_LDAP_SEARCH_CONNECTION,eventMsg,LL_ERROR);
      return LD_STATUS_RESOURCE_FAILURE;
// LCOV_EXCL_STOP
   }

   if (ld == NULL)
   {
// LCOV_EXCL_START
      snprintf(eventMsg,  MAX_EVENT_MSG_SIZE, "Failed to initialize the connection to LDAP server %s.  Error: ld is NULL", url.lud_host);
      INSERT_EVENT(authEvents,DBS_NO_LDAP_SEARCH_CONNECTION,eventMsg,LL_ERROR);
      return LD_STATUS_RESOURCE_FAILURE;
// LCOV_EXCL_STOP
   }

   version = LDAP_VERSION_3;
   (void) ldap_set_option (ld, LDAP_OPT_PROTOCOL_VERSION, &version);

// set dereference alias entry to LDAP_DEREF_ALWAYS.
// disable this option if LDAP server has alias entries cause performace issues.
int ldapderef = LDAP_DEREF_ALWAYS;

   (void) ldap_set_option (ld, LDAP_OPT_DEREF, &ldapderef);

   // set password policy controls
   LDAPControl *ctrls[2], c;
   c.ldctl_oid = (char *) LDAP_CONTROL_PASSWORDPOLICYREQUEST;
   c.ldctl_value.bv_val = NULL;
   c.ldctl_value.bv_len = 0;
   c.ldctl_iscritical = 0;
   ctrls[0] = &c;
   ctrls[1] = NULL;
   ldap_set_option( ld, LDAP_OPT_SERVER_CONTROLS, ctrls );


   // Note: set timeout value to -1 means wait until it finishes
   //       Default is -1 - no limit.

   if (self.host_->LDAPConfig_->timeout > 0)
   {
      tv.tv_sec = self.host_->LDAPConfig_->timeout;
      tv.tv_usec = 0;
      (void) ldap_set_option (ld, LDAP_OPT_TIMEOUT, &tv);
   }

   // Note: set timelimit value to 0 means wait until it finishes
   //       Default is LDAP_NO_LIMIT (0) - no limit.

   if (self.host_->LDAPConfig_->timeLimit > 0)
   {
      tv.tv_sec = self.host_->LDAPConfig_->timeLimit;
      tv.tv_usec = 0;
      (void) ldap_set_option (ld, LDAP_OPT_TIMELIMIT, &tv);
   }

   // Note: If not set then LDAP NETWORK_TIMEOUT
   //       Default is -1 - infinite timeout

   if (self.host_->LDAPConfig_->networkTimeout > 0)
   {
      tv.tv_sec = self.host_->LDAPConfig_->networkTimeout;
      tv.tv_usec = 0;
      (void) ldap_set_option (ld, LDAP_OPT_NETWORK_TIMEOUT, &tv);
   }

   (void) ldap_set_option (ld, LDAP_OPT_REFERRALS, LDAP_OPT_ON);
   (void) ldap_set_option (ld, LDAP_OPT_RESTART, LDAP_OPT_ON);

// LCOV_EXCL_START
   // Regular SSL mode
   if (self.host_->LDAPConfig_->SSL_Level == YES_SSL)
   {
      // set for SSL port, not for TLS port
      int tls = LDAP_OPT_X_TLS_HARD;
      (void) ldap_set_option (ld, LDAP_OPT_X_TLS, &tls);
   }
// LCOV_EXCL_STOP

   // Setup certificate file
   if ((self.host_->LDAPConfig_->SSL_Level == YES_TLS ||
       self.host_->LDAPConfig_->SSL_Level == YES_SSL) &&
       config.TLS_CACERTFilename.size() != 0)
   {
      int demand = LDAP_OPT_X_TLS_DEMAND;
      rc = ldap_set_option(ld,LDAP_OPT_X_TLS_REQUIRE_CERT,&demand);
      if (rc != LDAP_SUCCESS)
      {
         snprintf(eventMsg,  MAX_EVENT_MSG_SIZE, "Require TLS certificate failed for LDAP server %s.  Error: %d, ", url.lud_host, rc);
         errorTextString = ldap_err2string(rc);
         strncat(eventMsg, errorTextString, (MAX_EVENT_MSG_SIZE - (strlen(eventMsg)+4)));
         strcat(eventMsg, "\n");
         INSERT_EVENT(authEvents,DBS_NO_LDAP_SEARCH_CONNECTION,eventMsg,LL_ERROR);
         return LD_STATUS_RESOURCE_FAILURE;
      }

      rc = ldap_set_option(NULL,LDAP_OPT_X_TLS_CACERTFILE,
                           config.TLS_CACERTFilename.c_str());
      if (rc != LDAP_SUCCESS)
      {
         snprintf(eventMsg,  MAX_EVENT_MSG_SIZE, "Set TLS certificate file failed for LDAP server %s.  Error: %d, ", url.lud_host, rc);
         errorTextString = ldap_err2string(rc);
         strncat(eventMsg, errorTextString, (MAX_EVENT_MSG_SIZE - (strlen(eventMsg)+4)));
         strcat(eventMsg, "\n");
         INSERT_EVENT(authEvents,DBS_NO_LDAP_SEARCH_CONNECTION,eventMsg,LL_ERROR);
         return LD_STATUS_RESOURCE_FAILURE;
      }
   }

   // startTLS
   if (self.host_->LDAPConfig_->SSL_Level == YES_TLS)
   {
      rc = ldap_start_tls_s (ld, NULL, NULL);
      if (rc != LDAP_SUCCESS)
      {
         snprintf(eventMsg,  MAX_EVENT_MSG_SIZE, "StartTLS failed for LDAP server %s.  Error: %d, ", url.lud_host, rc);
         errorTextString = ldap_err2string(rc);
         strncat(eventMsg, errorTextString, (MAX_EVENT_MSG_SIZE - (strlen(eventMsg)+4)));
         strcat(eventMsg, "\n");
         INSERT_EVENT(authEvents,DBS_NO_LDAP_SEARCH_CONNECTION,eventMsg,LL_ERROR);
         return LD_STATUS_RESOURCE_FAILURE;
      }
   }

   if (debug)
   {
// LCOV_EXCL_START
      ber_set_option( NULL, LBER_OPT_DEBUG_LEVEL, &debug );
      ldap_set_option( NULL, LDAP_OPT_DEBUG_LEVEL, &debug );
// LCOV_EXCL_STOP
   }

int LDAPError = LDAP_SUCCESS;
LDAuthStatus authStatus;

   if (self.connectionType_ == AuthenticationConnection)
      self.authLD_ = ld;
   else
   {
      self.searchLD_ = ld;

      if (self.host_->LDAPConfig_->searchDN.size())
      {
         authStatus = bindUser(authEvents,self,
                               self.host_->LDAPConfig_->searchDN.c_str(),
                               self.host_->LDAPConfig_->searchPwd.c_str(),
                               false,
                               LDAPError);

        if (authStatus != LDAuthSuccessful)
        {
           snprintf(eventMsg,  MAX_EVENT_MSG_SIZE, "Initial bind with search user failed for LDAP server %s. Error: %d, ", url.lud_host, LDAPError);
           errorTextString = ldap_err2string(LDAPError);
           strncat(eventMsg, errorTextString, (MAX_EVENT_MSG_SIZE - (strlen(eventMsg)+4)));
           strcat(eventMsg, "\n");
           INSERT_EVENT(authEvents,DBS_NO_LDAP_AUTH_CONNECTION,eventMsg,LL_ERROR);
           return LD_STATUS_RESOURCE_FAILURE;
        }
     }
   }

   self.status_ = NODE_ACTIVE;
   return LD_STATUS_OK;

}

//***************************** End of connectToURL ****************************



// *****************************************************************************
// *                                                                           *
// * Function: convertUsername                                                 *
// *                                                                           *
// *    Scans string and for all reserved characters inserts the escape        *
// * character (\).                                                            *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <username>                     string &                         In/Out   *
// *    is a string containing the username to be converted.                   *
// *                                                                           *
// *                                                                           *
// *****************************************************************************
static void convertUsername(string & username)
{

const char *reserveChar = ",+\"\\<>;#=";
char newName[513]; // Must be twice as big as largest name, plus 1
char *nPtr = newName;
const char *uPtr = username.c_str();

unsigned int uLen = strlen(uPtr);

   for (int i = 0; i < uLen; i++)
   {
      if (strchr(reserveChar, *uPtr) != NULL)
      {
         // found a reserved character
         *nPtr++ = '\\';
      }
      *nPtr++ = *uPtr++;
   }

   *nPtr = '\0';
   username = newName;

}
//************************* End of convertUsername *****************************


// *****************************************************************************
// *                                                                           *
// * Function: getEffectiveHostName                                            *
// *                                                                           *
// *  This function resolves a host name and returns the effective host name.  *
// *  e.g. load balancer host name abc.com                                     *
// *       effective host name server1.abc.com                                 *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <hostName>                     const char *                     In       *
// *    is a string containing the host name.                                  *
// *                                                                           *
// *                                                                           *
// *  <effectiveHostName>            char *                           Out      *
// *    passes back a string containing the effective host name.               *
// *                                                                           *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: bool                                                             *
// *                                                                           *
// * true:  the name was resolved                                              *
// * false: error resolving the name, use the original name.                   *
// *                                                                           *
// *****************************************************************************
static bool getEffectiveHostName(
   const char * hostName,
   char *       effectiveHostName)


{

struct hostent *hstnm;

   hstnm = gethostbyname(hostName);
   if (!hstnm)
   {
      strcpy(effectiveHostName,hostName);
      return false;
   }

// May need to support IPv6 in the future, this is good enough for now.
   hstnm = gethostbyaddr((const void *)*hstnm->h_addr_list,4,AF_INET);
   if (!hstnm)
   {
      strcpy(effectiveHostName,hostName);
      return false;
   }

   strcpy(effectiveHostName,hstnm->h_name);
   return true;

}
//********************** End of getEffectiveHostName ***************************



// *****************************************************************************
// *                                                                           *
// * Function: initConnection                                                  *
// *                                                                           *
// *    This function calls LDAP APIs to initialize a connection.              *
// *    It will also make an LDAP call to bind with search user so to make     *
// *    sure later a user search can be performed.                             *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <authEvents>                    std::vector<AuthEvent> &        Out      *
// *    detailed event results of request                                      *
// *                                                                           *
// *  <self>                         ConfigNodeContents &             In       *
// *    is a reference to an instance of a ConfigNodeContents object.          *
// *                                                                           *
// *  <hostName>                     char *                           [Out]    *
// *    if specified (non-NULL), passes back the name of the host if the       *
// *  connection is successful.                                                *
// *                                                                           *
// *  <skipHost>                     bool                             [In]     *
// *    if specified and true, the last connected host is added to the         *
// *  excluded hosts list and the connection is attempted with the next        *
// *  host in the list.                                                        *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: LD_Status                                                        *
// *                                                                           *
// * LD_STATUS_OK: a connection was made                                       *
// * LD_STATUS_RESOURCE_FAILURE: a connection could not be made                *
// *                                                                           *
// *****************************************************************************

static LD_Status initConnection(
   std::vector<AuthEvent>  & authEvents,
   ConfigNodeContents & self,
   char *               hostName,
   bool                 skipHost)

{

// Bailout if the section requested was not present in the config file
   if (!self.host_->LDAPConfig_->sectionRead)
      return LD_STATUS_RESOURCE_FAILURE;//ACH Need to log details.  User registration type not defined in .traf_authentication_config.

LDAPURLDesc url;

   memset( &url, 0, sizeof(url));

   if (self.host_->LDAPConfig_->SSL_Level == NO_SSL ||
       self.host_->LDAPConfig_->SSL_Level == YES_TLS)
      url.lud_scheme = (char *) "ldap";
// LCOV_EXCL_START
   else
      url.lud_scheme = (char *) "ldaps";
// LCOV_EXCL_STOP

   url.lud_port = self.host_->LDAPConfig_->portNumber;
   url.lud_scope = LDAP_SCOPE_DEFAULT;

vector<string> hostNames;

//ACH why make a copy?
   hostNames = self.host_->LDAPConfig_->hostName;

// If the caller has encountered a problem with the current connection,
// mark that host as bad and move on to the next one in the list.
   if (skipHost)
   {
      addExcludedHostName(authEvents,self,self.host_->lastHostName_.c_str());

      self.host_->lastHostIndex_ = (self.host_->lastHostIndex_ + 1) % hostNames.size();
   }

// Just in case lastHostIndex_ is overwritten, check for large value and reset
   if (self.host_->lastHostIndex_ >= hostNames.size())
      self.host_->lastHostIndex_ = 0;

// Start from the host that we last made a good connection
int lastHostIndex = self.host_->lastHostIndex_;

   for (int hostIndex = lastHostIndex; hostIndex < hostNames.size(); hostIndex++)
      if (connectToHost(authEvents,self,(char *)hostNames[hostIndex].c_str(),
                        self.host_->LDAPConfig_->isLoadBalancer[hostIndex],url))
      {
         self.host_->lastHostIndex_ = hostIndex;
         if (hostName != NULL)
            strcpy(hostName,url.lud_host);
         return LD_STATUS_OK;
      }

// Start from the first Host Name and try the remaining hosts in the list
   for (int hostIndex = 0; hostIndex < lastHostIndex; hostIndex++)
      if (connectToHost(authEvents,self,(char *)hostNames[hostIndex].c_str(),
                        self.host_->LDAPConfig_->isLoadBalancer[hostIndex],url))
      {
         self.host_->lastHostIndex_ = hostIndex;
         if (hostName != NULL)
            strcpy(hostName,url.lud_host);
         return LD_STATUS_OK;
      }

   return LD_STATUS_RESOURCE_FAILURE;

}
//**************************** End of initConnection ***************************

// *****************************************************************************
// *                                                                           *
// * Function: isHostNameExcluded                                              *
// *                                                                           *
// *  This function determines if a host name is on the naughty list.          *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <hostName>                     const char *                     In       *
// *    is a string containing the host name.                                  *
// *                                                                           *
// *  <excludedHosts>                const vector<string> &           In       *
// *    is a list of hosts that should not be used.                            *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: bool                                                             *
// *                                                                           *
// * true:  host name is on excluded list.   :(                                *
// * false: host name is not on the excluded list.                             *
// *                                                                           *
// *****************************************************************************
static bool isHostNameExcluded(
   const char *           hostName,
   const vector<string> & excludedHosts)

{

   for (size_t index = 0; index < excludedHosts.size(); index++)
      if (excludedHosts[index].compare(hostName) == 0)
         return true;

   return false;

}
//************************** End of isHostNameExcluded *************************


// *****************************************************************************
// *                                                                           *
// * Function: logConfigFileError                                              *
// *                                                                           *
// *    This function logs the error encountered while processing the          *
// *  LDAP connection config file (.traf_authentication_config).               *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <authEvents>                    std::vector<AuthEvent> &        Out      *
// *    detailed event results of request                                      *
// *                                                                           *
// *  <fileCode>                     LDAPConfigFileErrorCode          In       *
// *    is a reference to an instance of a ConfigNodeContents object.          *
// *                                                                           *
// *  <filePath>                     const char*                      In       *
// *    is the filepath of the .traf_authentication_config.                    *
// *                                                                           *
// *  <lastLineNumber>                int                             In       *
// *    is the number of the last line that was read.                          *
// *                                                                           *
// *  <lastLine>                      string &                        In       *
// *    is the text of the last line that was read.                            *
// *                                                                           *
// *****************************************************************************
inline static void logConfigFileError(
   std::vector<AuthEvent> & authEvents,
   LDAPConfigFileErrorCode   fileCode,
   const char*               filePath,
   int                       lastLineNumber,
   string                  & lastLine)

{

char eventMsg[MAX_EVENT_MSG_SIZE];

   switch (fileCode)
   {
      case LDAPConfigFile_OK:
         return;
         break;
      case LDAPConfigFile_NoFileProvided:
      case LDAPConfigFile_FileNotFound:
         snprintf(eventMsg,  MAX_EVENT_MSG_SIZE, "****** %s file not found\n",filePath);
         break;
      case LDAPConfigFile_BadAttributeName:
         snprintf(eventMsg, MAX_EVENT_MSG_SIZE, "****** Unrecognized attribute in %s configuration file.  Line %d %s",
                 filePath,lastLineNumber,lastLine.c_str());
         break;
      case LDAPConfigFile_MissingValue:
         snprintf(eventMsg, MAX_EVENT_MSG_SIZE, "****** Missing required value in %s configuration file.  Line %d %s",
                 filePath,lastLineNumber,lastLine.c_str());
         break;
      case LDAPConfigFile_ValueOutofRange:
         snprintf(eventMsg, MAX_EVENT_MSG_SIZE, "****** Value out of range in %s configuration file.  Line %d %s",
                 filePath,lastLineNumber,lastLine.c_str());
         break;
      case LDAPConfigFile_CantOpenFile:
         snprintf(eventMsg, MAX_EVENT_MSG_SIZE, "****** Unable to open %s configuration file errno: %d", filePath,lastLineNumber);
         break;
      case LDAPConfigFile_CantReadFile:
         snprintf(eventMsg, MAX_EVENT_MSG_SIZE, "****** Unable to read %s configuration file",filePath);
         break;
      case LDAPConfigFile_MissingCACERTFilename:
         snprintf(eventMsg, MAX_EVENT_MSG_SIZE, "****** TLS requested but no TLS CACERTFilename was provided");
         break;
      case LDAPConfigFile_MissingHostName:
         snprintf(eventMsg, MAX_EVENT_MSG_SIZE, "****** Missing host name in %s",filePath);
         break;
      case LDAPConfigFile_MissingUniqueIdentifier:
         snprintf(eventMsg, MAX_EVENT_MSG_SIZE, "****** Missing unique identifier in %s",filePath);
         break;
      case LDAPConfigFile_MissingSection:
         snprintf(eventMsg, MAX_EVENT_MSG_SIZE, "****** Missing directory server configuration in %s",filePath);
         break;
      case LDAPConfigFile_ParseError:
         snprintf(eventMsg, MAX_EVENT_MSG_SIZE, "****** Internal error parsing %s",filePath);
         break;
      case LDAPConfigFile_CantOpenLDAPRC:
         snprintf(eventMsg, MAX_EVENT_MSG_SIZE, "****** Unable to open .ldaprc to determine TLS CACERTFilename");
         break;
      case LDAPConfigFile_MissingLDAPRC:
         snprintf(eventMsg, MAX_EVENT_MSG_SIZE, "****** Missing .ldaprc and TLS_CACERTFilename not provided; cannot determine TLS CACERT filename");
         break;
      default:
         snprintf(eventMsg, MAX_EVENT_MSG_SIZE, "****** Error parsing %s configuration file",filePath);
   }

   INSERT_EVENT(authEvents,DBS_AUTH_CONFIG,eventMsg,LL_ERROR);

}
//************************** End of logConfigFileError *************************

// *****************************************************************************
// *                                                                           *
// * Function: short::getConfigNumberForName                                          *
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <configurationName>             const char *                    In       *
// *    is the configuration name of the node to be obtained.  If name         *
// *  is NULL, a configNumber is chosen based on the setting                   *
// *  in the configuration file data.                                          *
// *                                                                           *
// *    Returns the configuration number corresponding to the name.            *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Returns:  short                                                          *
// *                                                                           *
// *****************************************************************************
static short getConfigNumberForName(
   const char * configName)

{

// If the config name is not known (not specified, user requests default),
// use the value from the config file defaults section.
short configNumber = -1;

   if (configName == NULL || configName[0] == 0)
   {
         configNumber = config.defaultSectionNumber;
   }
   else
   {
// Find the number corresponding to the configuration name provided

vector<string>::iterator it;

      it = find(config.section.begin(), config.section.end(), configName);
      if (it != config.section.end())
         configNumber = it - config.section.begin();

   }
   return configNumber;

}
//************ End of LDAPConfigNode::getConfigNumberForName *******************

// *****************************************************************************
// *                                                                           *
// * Function: getNonExcludedHostName                                          *
// *                                                                           *
// *  This function gets a host name to use for LDAP operations.               *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <hostName>                     const char *                     In       *
// *    is a string containing the host name.                                  *
// *                                                                           *
// *  <effectiveHostName>            char *                           Out      *
// *    passes back a string containing the effective host name.               *
// *                                                                           *
// *  <excludedHosts>                const vector<string> &           In       *
// *    is a list of hosts that should not be used.                            *
// *                                                                           *
// *  <isLoadBalanceHost>            bool                             In       *
// *    is true if <hostName> is a load balancing host.                        *
// *                                                                           *
// *  <shouldExcludeBadHosts>        bool                             In       *
// *    is true if the <effectiveHostName> should not be in <excludedHost>.    *
// *                                                                           *
// *  <retryCount>                   int                              In       *
// *    is the number of times a load balancer should be called to get a       *
// *  non-excluded host name if <shouldExcludeBadHost> is true.                *
// *                                                                           *
// *  <retryDelay>                   int                              In       *
// *    is the time (in seconds) to delay between retry the load balancer.     *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: bool                                                             *
// *                                                                           *
// * true:  a "good" host name was found.                                      *
// * false: Could not find a host name not on the exclude list. :(             *
// *                                                                           *
// *****************************************************************************
static bool getNonExcludedHostName(
   const char *           hostName,
   char *                 effectiveHostName,
   const vector<string> & excludedHosts,
   bool                   isLoadBalanceHost,
   bool                   shouldExcludeBadHosts,
   int                    retryCount,
   int                    retryDelay)

{

// If this is a load balance host (e.g. abc.com, get the effective
// host name.  This name will then be used directly and any error messages
// will include the actual host name.  If not the load balancer, use the
// host name that was passed in.
   if (isLoadBalanceHost)
      getEffectiveHostName(hostName,effectiveHostName);
   else
      strcpy(effectiveHostName,hostName);

   if (!shouldExcludeBadHosts)
      return true;

// If the effective host name is not on the list of excluded hosts, use it!
   if (!isHostNameExcluded(effectiveHostName,excludedHosts))
      return true;

// The host is on the excluded list.  If this is not a load balancing host
// there is nothing that can be done here.  Return false and hope caller's
// retry logic enables a successful connection.
   if (!isLoadBalanceHost)
      return false;

// Poke the load balancing host until we get a non-excluded name or
// exhaust the retries.
   for (size_t i = 0; i < retryCount; i++)
   {
       sleep(retryDelay);
       getEffectiveHostName(hostName,effectiveHostName);
       if (isHostNameExcluded(effectiveHostName,excludedHosts))
          continue;
       return true;
   }

   return false;

}
//************************ End of getNonExcludedHostName ***********************


// *****************************************************************************
// *                                                                           *
// * Function: readLDAPConfigFile                                              *
// *                                                                           *
// *    Calls LDAPConfigFile class to read and parse the LDAP configuration    *
// *  file.  If successful, the results are cached and the current time is     *
// *  recorded.  If unsuccessful, an error is logged to stdout (and eventually *
// *  the repository.)                                                         *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: bool                                                             *
// *                                                                           *
// * true:  LDAP config file was read and parsed successfully                  *
// * false: Error while reading/parsing LDAP config file                       *
// *                                                                           *
// *****************************************************************************
static bool readLDAPConfigFile(std::vector<AuthEvent> &authEvents)

{

string configFilename;
LDAPFileContents contents;
int lastLineNumber = -1;
string lastLine;

LDAPConfigFileErrorCode fileCode = configFile.read(configFilename,
                                                   config,
                                                   lastLineNumber,
                                                   lastLine);

   if (fileCode != LDAPConfigFile_OK)
   {
      logConfigFileError(authEvents,fileCode,configFilename.c_str(),lastLineNumber,lastLine);
      return false;
   }

// Only update lastRefreshTime if reading and parsing was successful.
   lastRefreshTime = time(0);
   return true;

}
//************************** End of readLDAPConfigFile *************************


// *****************************************************************************
// *                                                                           *
// * Function: searchUser                                                      *
// *                                                                           *
// *  This function performs a LDAP user search based on the                   *
// *  input username, it also returns userDN, domainName, domainAttr           *
// *  about the user that was found.                                           *
// *  This funtion is called by ld_auth_user to look for the user first,       *
// *  then based on the information returned to perform user bind.             *
// *                                                                           *
// *  The processing steps of this function:                                   *
// *                                                                           *
// *  if UIF exists, parse domainName, userName                                *
// *                                                                           *
// *  if User Identifier exists                                                *
// *    Search by User Identifier, get DN                                       *
// *  else                                                                     *
// *    Search by Unique Identifier, get DN                                    *
// *                                                                           *
// *  if domainName entered by user                                            *
// *    find the node matches to domainName, do bind with DN                   *
// *  else                                                                     *
// *    if Domain Attribute exists                                             *
// *      Find the node matches to domainName in Domain Attribute,             *
// *       do bind with DN                                                     *
// *    else                                                                   *
// *      do bind with DN in highest priority node?                            *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <authEvents>                    std::vector<AuthEvent> &        Out      *
// *    detailed event results of request                                      *
// *                                                                           *
// *  <self>                          ConfigNodeContents &            In       *
// *    is a reference to a ConfigNodeContents.                                *
// *                                                                           *
// *  <inputName>                     const char *                    In       *
// *    is the name supplied by the user.                                      *
// *                                                                           *
// *  <userDN>                        string &                        Out      *
// *    passes back the distingushed name for the user.                        *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: LDSearchStatus                                                   *
// *                                                                           *
// *****************************************************************************
static LDSearchStatus searchUser(
   std::vector<AuthEvent>  & authEvents,
   ConfigNodeContents   & self,
   const char           * inputName,
   string               & userDN)

{

LDSearchStatus searchStatus = LDSearchNotFound;
vector<string> uniqueIdentifiers;
string uniqueIdentifier;
string username = inputName;

   convertUsername(username);

   uniqueIdentifiers = self.host_->LDAPConfig_->uniqueIdentifier;
   for (int j = 0; j < uniqueIdentifiers.size(); j++)
   {
      uniqueIdentifier = uniqueIdentifiers[j];
      size_t pos = uniqueIdentifier.find('=', 0);  // look for =
      uniqueIdentifier.insert(pos + 1,username);    // insert username to make the DN
      searchStatus = searchUserByDN(authEvents,self,uniqueIdentifier);
      if (searchStatus == LDSearchFound )
      {
         // user found
         userDN = uniqueIdentifier;
         return LDSearchFound ;
      }

      if (searchStatus == LDSearchNotFound)// continue to search
         continue;

// any other errors, stop the search and return
// LCOV_EXCL_START
      return searchStatus;
// LCOV_EXCL_STOP
   }

   return searchStatus;

}
//****************************** End of searchUser *****************************

// *****************************************************************************
// *                                                                           *
// * Function: searchUserByDN                                                  *
// *                                                                           *
// *  Performs a LDAP user search by calling LDAP search APIs.                 *
// *  The search call uses a DN as the search base.                            *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <authEvents>                    std::vector<AuthEvent> &        Out      *
// *    detailed event results of request                                      *
// *                                                                           *
// *  <self>                          ConfigNodeContents &            In       *
// *    is a reference to a ConfigNodeContents.                                *
// *                                                                           *
// *  <userDN>                        string &                        In       *
// *    is the distingushed name for the user.                                 *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Returns:  error code                                                     *
// *                                                                           *
// *****************************************************************************
static LDSearchStatus searchUserByDN(
   std::vector<AuthEvent>  & authEvents,
   ConfigNodeContents   & self,
   const string         & userDN)

{

int rc;
LDAP *ld;
LDAPMessage *res = NULL, *entry = NULL;
auto_ptr<LDAPMessage> test(NULL);
char *filter = NULL;
char *attrs[3];
char *attr, **vals;
BerElement *ptr = 0;
char createTimestamp[16];
char eventMsg[MAX_EVENT_MSG_SIZE];

// Use "createTimestamp" as our "unique ID" for the user on the LDAP server.
   strcpy(createTimestamp, "createTimestamp");
   attrs[0] = createTimestamp;
   attrs[1] = NULL;

// *****************************************************************************
// *                                                                           *
// *    When we search for the username on the LDAP server, there are four     *
// * possible error returns:                                                   *
// *                                                                           *
// *    1) Username is found (LDAP_SUCCESS)                                    *
// *       This is the most common. We continue on with authentication.        *
// *                                                                           *
// *    2) Username is not found (LDAP_NO_SUCH_OBJECT)                         *
// *       Could be a typo or someone trying to gain unauthorized access.      *
// *       We stop the authentication process.                                 *
// *                                                                           *
// *    3) Could not communicate with server (LDAP_SERVER_DOWN)                *
// *       Could be a transient problem, maybe a network issue.                *
// *       We retry once within this function, then if it fails                *
// *       again, log the error, and return an internal resource error.        *
// *       This triggers retry logic.                                          *
// *                                                                           *
// *    4) Some other error (e.g. LDAP_OTHER)                                  *
// *       We receive one of several dozen other possible errors.              *
// *       We log the error and return an internal resource error.             *
// *       This triggers retry logic.                                          *
// *                                                                           *
// *****************************************************************************

int reconnect = 1;

   while (true)
   {
      if (!selfCheck(self,true))
      {
         INSERT_EVENT(authEvents,DBS_UNKNOWN_AUTH_STATUS_ERROR,"Self check failed in searchUserByDN",LL_ERROR);

         return LDSearchResourceFailure;
      }

      ld = self.searchLD_;
      rc = ldap_search_ext_s(ld,
                             userDN.c_str(),   // use the DN for base
                             LDAP_SCOPE_BASE,  // search only the base
                             filter,           // no filter
                             attrs,            // return attributes
                             0,                // get both attribute and value
                             NULL,             // server control
                             NULL,             // client control
                             NULL,             // timeout
                             0,                // result size limit
                             &res );           // search result

      // The username was found on the LDAP server, break out of loop.
      if (rc == LDAP_SUCCESS)
         break;

      if (res != NULL)
         ldap_msgfree(res);

      // Username not found on the LDAP server; similar to bad password, we
      // do not retry this error.
      // Return and report to the user - No soup for you.
      if (rc == LDAP_NO_SUCH_OBJECT)
         return LDSearchNotFound;

      //Retry server down error once.
      if (rc == LDAP_SERVER_DOWN && reconnect != 0)
      {
         // reconnect & retry
         closeConnection(self);
         LD_Status status = initConnection(authEvents,self,NULL,true);
         // If the reconnect failed, report the resource error and return a
         // resource failure error so we will retry per configuration settings.
         if (status != LD_STATUS_OK)
         {
            snprintf(eventMsg,  MAX_EVENT_MSG_SIZE, "LDAP search error.   Unable to connect to server\n");
            INSERT_EVENT(authEvents,DBS_LDAP_SEARCH_ERROR,eventMsg,LL_ERROR);
            return LDSearchResourceFailure;
         }
         reconnect--;
         continue;
      }

      // For all other search errors, report an error and return a resource
      // failure (LDAP server or network problem) so we will retry per
      // configuration settings.
      snprintf(eventMsg,  MAX_EVENT_MSG_SIZE, "LDAP search error.  Error code: %d, %s\n",
                      rc, ldap_err2string(rc));
      INSERT_EVENT(authEvents,DBS_LDAP_SEARCH_ERROR,eventMsg,LL_ERROR);
      return LDSearchResourceFailure;
   }

// Check the number of entries found.  We only need one.  If none were
// returned, the user is not defined on the directory server.
int numberFound = ldap_count_entries(ld,res);

   if (numberFound <= 0)
   {
      if (res != NULL)
         ldap_msgfree(res);

      return LDSearchNotFound;
   }

// Get the first entry returned.  If there was no entry returned, that means
// the user is not defined on the directory server. We already check for
// number of entries, but let's be extra careful.
   entry = ldap_first_entry(ld, res);
   if (entry == NULL)
   {
      //this should not happen
      if (res != NULL)
         ldap_msgfree(res);

      // log error message
      return LDSearchNotFound;
   }

   attr = ldap_first_attribute(ld, entry, &ptr);
   if (attr == NULL)
   {
      //this should not happen
      if (res != NULL)
         ldap_msgfree(res);

      // log error message
      snprintf(eventMsg,  MAX_EVENT_MSG_SIZE, "LDAP search error.   Attribute %s does not exist in the entry %s", attrs[0], userDN.c_str());
      INSERT_EVENT(authEvents,DBS_LDAP_SEARCH_ERROR,eventMsg,LL_ERROR);
      return LDSearchResourceFailure;
   }

// OpenLDAP allocates memory and then requires the caller to free it using
// specific OpenLDAP APIs.

   ldap_memfree(attr);

   if (ptr != NULL)
      ber_free(ptr, 0);

   if (res != NULL)
      ldap_msgfree(res);

   return LDSearchFound;

}
//*************************** End of searchUserByDN ****************************

// *****************************************************************************
// *                                                                           *
// * Function: selfCheck                                                       *
// *                                                                           *
// *    Determines if this instance is internally consistent.  Based on the    *
// * node type and the status, dependent values are checked.                   *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <self>                          ConfigNodeContents &            In       *
// *    is a reference to a ConfigNodeContents.                                *
// *                                                                           *
// *  <isInitialized>                 bool                            In       *
// *    if true, instance must be initialized to be consistent.                *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Returns:                                                                 *
// *                                                                           *
// *  TRUE: Node is consistent                                                 *
// *  FALSE: Node is inconsistent                                              *
// *                                                                           *
// *****************************************************************************
static bool selfCheck(
   ConfigNodeContents   & self,
   bool                   isInitialized)

{

//
// We only support two configurations.  Got to be one or the other.
//

//   if (self.configType_ != LDAPConfigNode::PrimaryConfiguration &&
//       self.configType_ != LDAPConfigNode::SecondaryConfiguration)
//      return false;

//
// We only support two types of connections.  Got to be one or the other.
//

   if (self.connectionType_ != AuthenticationConnection &&
       self.connectionType_ != SearchConnection)
      return false;

//
// If the node has not (yet) been initialized, that is a consistent state.
// However, if the caller expects the node to have been initialized, we have problem.
//

   if (self.status_ == NODE_NOT_INITIALIZED)
   {
      if (isInitialized)
         return false;
      else
         return true;
   }

//
// There are only two states.  Anything else is a problem. This catches cases
// where the class data has been overwritten.
//

   if (self.status_ != NODE_ACTIVE)
      return false;

//
// We have an active node.  For active nodes, the ld for the connection type
// must be setup.
//

   if (self.connectionType_ == AuthenticationConnection && self.authLD_ == NULL)
      return false;
   if (self.connectionType_ == SearchConnection && self.searchLD_ == NULL)
      return false;

//
// All data members are consistent with an active state.  Check the
// LDAP Config for consistency.
//
   return self.host_->LDAPConfig_->selfCheck(isInitialized);

}
//***************************** End of selfCheck *******************************


// *****************************************************************************
// *                                                                           *
// * Function: shouldReadLDAPConfig                                            *
// *                                                                           *
// *  Determines if the LDAP config file needs to be (re)read or if we can use *
// *  the data we have already read.                                           *
// *                                                                           *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: bool                                                             *
// *                                                                           *
// * true:  LDAP config file should be read                                    *
// * false: Use existing data                                                  *
// *                                                                           *
// *****************************************************************************
inline static bool shouldReadLDAPConfig()

{

// If we have never successfully read and parsed the LDAP configuration file,
// then we need to read.
   if (lastRefreshTime == 0)
      return true;

// We have previously read the file.  See if we need to refresh.

// Zero indicates never refresh, so don't read.
   if (config.refreshTime == 0)
      return false;

// If not enough time has elapsed since we last read the LDAP config file
// then we don't need to re-read the file.
   if (time(0) - lastRefreshTime < config.refreshTime)
      return false;

// Time to freshen up!
   return true;

}
//************************ End of shouldReadLDAPConfig *************************

#pragma page "void LdapConnection::getLdapGroupsForUser"
// *****************************************************************************
// *                                                                           *
// * Function: getLdapGroupsForUser                                            *
// *                                                                           *
// * Get the list of groups for the specified username.                        *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <authEvents>                    std::vector<AuthEvent> &        Out      *
// *    detailed event results of request                                      *
// *                                                                           *
// *  <self>                          ConfigNodeContents &            In       *
// *    is a reference to a ConfigNodeContents.                                *
// *                                                                           *
// *  <username>                      const char *                    In       *
// *    is the username of the supplied by the caller.                         *
// *                                                                           *
// *  <groupList                      std::set<string>                Out      *
// *    list of groups for the user.                                           *
// *                                                                           *
// * Returns: LDSearchStatus                                                   *
// *                                                                           *
// *****************************************************************************
LDSearchStatus getLDAPGroupsForUser(
   std::vector<AuthEvent> & authEvents,
   ConfigNodeContents     & self,
   const char             * username, 
   std::set<string>       & groupList)
{
  int rc;
  LDAPMessage *res = NULL, *entry = NULL;
  BerElement  *ber = NULL;
  char filter[513];
  char attrName[64];
  char *attrs[2];
  int groupFound = 0;
  char *dn = NULL;
  LDAP *ld = NULL;
  char *attr;
  char **vals;
  char eventMsg[MAX_EVENT_MSG_SIZE];

  if (self.connectionType_ == AuthenticationConnection) 
     ld = self.authLD_;
  else
     ld = self.searchLD_;

  groupList.clear();

  if (self.host_->LDAPConfig_->searchGroupBase.empty())
      return LDSearchNotFound;

  if (self.host_->LDAPConfig_->searchGroupNameAttr.empty())
      attrs[0] = (char *)"cn";
  else
  {
      strcpy(attrName, self.host_->LDAPConfig_->searchGroupNameAttr.c_str());
      attrs[0]= attrName;
  }
  attrs[1] = NULL;

  memset(filter,0,sizeof(filter));
  snprintf(filter, sizeof(filter), "(& (objectClass=%s)(%s=%s))",
          self.host_->LDAPConfig_->searchGroupObjectClass.c_str(),
          self.host_->LDAPConfig_->searchGroupMemberAttr.c_str(),
          username);


  rc = ldap_search_ext_s(ld,
                         self.host_->LDAPConfig_->searchGroupBase.c_str(),    // base
                         LDAP_SCOPE_SUBTREE,    // search only the base
                         filter,                // filter
                         attrs,                 // return attributes
                         0,                     // get both attribute and value
                         NULL,                  // server control
                         NULL,                  // client control
                         NULL,                  // timeout
                         0,                     // result size limit 0 means no limit
                         &res );                // search result

  if (rc == LDAP_NO_SUCH_OBJECT)
  {
    snprintf(eventMsg, MAX_EVENT_MSG_SIZE, "No groups returned for user %s: ",username);
    INSERT_EVENT(authEvents,DBS_DEBUGMSG,eventMsg,LL_DEBUG);
     return LDSearchFound;
  }

  if ( rc != LDAP_SUCCESS )
  {
    if (res != NULL)
      ldap_msgfree(res);

    char *ldap_error = ldap_err2string(rc);
    //ldap_perror(ld, "ldap_search_ext_s");
    //abort();

    int bufSize = strlen(ldap_error) + 100;
    char buf [bufSize];
    snprintf (buf, bufSize, "ldap_search_ext_s failed: %s", ldap_error);
    INSERT_EVENT(authEvents,DBS_LDAP_GROUP_SEARCH_ERROR ,buf,LL_ERROR);
    return LDSearchResourceFailure;
  }

  for (entry = ldap_first_entry(ld, res);
       entry != NULL;
       entry = ldap_next_entry(ld, entry))
  {

     attr = ldap_first_attribute (ld, entry, &ber);
     if (attr == NULL)
     {
        if (res != NULL)
           ldap_msgfree(res);
        INSERT_EVENT(authEvents,DBS_LDAP_GROUP_SEARCH_ERROR ,"ldap attribute is null",LL_ERROR);
        return LDSearchResourceFailure;
     }

     vals = ldap_get_values(ld, entry, attr);

     if (vals[0] != NULL)
     {
         groupList.insert(vals[0]);
         ldap_value_free(vals);
     }

     ldap_memfree(attr);
     if (ber != NULL) 
        ber_free(ber,0);
   }

  if (res != NULL)
    ldap_msgfree(res);

  std::set<std::string>::iterator it;

  snprintf(eventMsg, MAX_EVENT_MSG_SIZE, "Groups returned for user %s: ",username);
  string groupNameStr;
  for (it=groupList.cbegin(); it!= groupList.cend(); ++it)
  {
    groupNameStr += *it;
    groupNameStr += " ";
  }

  int32_t maxGroupSize = MAX_EVENT_MSG_SIZE - strlen(eventMsg) - 10;
  if (groupNameStr.size() > maxGroupSize)
  {
     groupNameStr.erase(maxGroupSize);
     groupNameStr += "...";
  }
  
  strncat(eventMsg, groupNameStr.c_str(), maxGroupSize + 3);
  INSERT_EVENT(authEvents,DBS_DEBUGMSG,eventMsg,LL_DEBUG);

  return LDSearchFound;
}
//************************ End of getLDAPGroupsForUser *************************

#pragma page "void LdapConnection::searchGroup"
// *****************************************************************************
// *                                                                           *
// * Function: searchGroup                                                     *
// *                                                                           *
// *  Performs a LDAP group search by calling LDAP search APIs.                *
// *  The search call uses a group base provided by the user                   *
// *  as the search base.                                                      *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <authEvents>                    std::vector<AuthEvent> &        Out      *
// *    detailed event results of request                                      *
// *                                                                           *
// *  <self>                          ConfigNodeContents &            In       *
// *    is a reference to a ConfigNodeContents.                                *
// *                                                                           *
// *  <groupName>                     const char *                    In       *
// *    is the group name supplied by the user.                                *
// *                                                                           *

// *****************************************************************************
// *                                                                           *
// *  Returns:  error code                                                     *
// *                                                                           *
// *****************************************************************************

static LDSearchStatus searchGroup(
   std::vector<AuthEvent> & authEvents,
   ConfigNodeContents     & self,
   const char             * groupName) 
{
  int rc;
  LDAPMessage *res = NULL, *entry = NULL;
  BerElement  *ber = NULL;
  char filter[513];
  char *attrs[2];
  string groupAttr;
  char *dn = NULL;
  LDAP *ld = NULL;
  char eventMsg[MAX_EVENT_MSG_SIZE];

int reconnect = 1;

   while (true)
   {
      if (!selfCheck(self,true))
      {
         INSERT_EVENT(authEvents,DBS_UNKNOWN_AUTH_STATUS_ERROR,"Self check failed in searchUserByDN",LL_ERROR);

         return LDSearchResourceFailure;
      }


      if (self.host_->LDAPConfig_->searchGroupBase.empty())
         return LDSearchNotFound;

      if (self.host_->LDAPConfig_->searchGroupNameAttr.empty())
         groupAttr = "cn";
      else
         groupAttr = self.host_->LDAPConfig_->searchGroupNameAttr;

      memset(filter,0,sizeof(filter));
      snprintf(filter, sizeof(filter),"(& (objectClass=%s)(%s=%s))",
              self.host_->LDAPConfig_->searchGroupObjectClass.c_str(),
              groupAttr.c_str(),
              groupName);

      attrs[0] = (char *)groupAttr.c_str();
      attrs[1] = NULL;

      ld = self.searchLD_;

      rc = ldap_search_ext_s(ld,
                             self.host_->LDAPConfig_->searchGroupBase.c_str(),    // base
                             LDAP_SCOPE_SUBTREE,    // search only the base
                             filter,                // filter
                             attrs,                 // return attributes
                             0,                     // get both attribute and value
                             NULL,                  // server control
                             NULL,                  // client control
                             NULL,                  // timeout
                             0,                     // result size limit
                             &res );                // search result

      // The username was found on the LDAP server, break out of loop.
      if (rc == LDAP_SUCCESS)
         break;

      if (res != NULL)
         ldap_msgfree(res);

      if (rc == LDAP_NO_SUCH_OBJECT)
         return LDSearchNotFound;

      //Retry server down error once.
      if (rc == LDAP_SERVER_DOWN && reconnect != 0)
      {
         // reconnect & retry
         closeConnection(self);
         LD_Status status = initConnection(authEvents,self,NULL,true);
         // If the reconnect failed, report the resource error and return a
         // resource failure error so we will retry per configuration settings.
         if (status != LD_STATUS_OK)
         {
            snprintf(eventMsg,  MAX_EVENT_MSG_SIZE, "LDAP search error.   Unable to connect to server\n");
            INSERT_EVENT(authEvents,DBS_LDAP_SEARCH_ERROR,eventMsg,LL_ERROR);
            return LDSearchResourceFailure;
         }
         reconnect--;
         continue;
      }

      // For all other search errors, report an error and return a resource
      // failure (LDAP server or network problem) so we will retry per
      // configuration settings.
      snprintf(eventMsg,  MAX_EVENT_MSG_SIZE, "LDAP Group search error.  Error code: %d, %s\n",
                      rc, ldap_err2string(rc));
      INSERT_EVENT(authEvents,DBS_LDAP_SEARCH_ERROR,eventMsg,LL_ERROR);
      return LDSearchResourceFailure;
   }

// Check the number of entries found.  We only need one.  If none were
// returned, the group is not defined on the directory server.
int numberFound = ldap_count_entries(ld,res);
   if (numberFound <= 0)
   {
      if (res != NULL)
         ldap_msgfree(res);

      return LDSearchNotFound;
   }

// Get the first entry returned.  If there was no entry returned, that means
// the group is not defined on the directory server. We already check for
// number of entries, but let's be extra careful.
   entry = ldap_first_entry(ld, res);
   if (entry == NULL)
   {
      //this should not happen
      if (res != NULL)
         ldap_msgfree(res);

      // log error message
      return LDSearchNotFound;
   }

// OpenLDAP allocates memory and then requires the caller to free it using
// specific OpenLDAP APIs.

   if (res != NULL)
      ldap_msgfree(res);

   return LDSearchFound;

}

//************************ End of searchGroup *************************

#pragma page "void LdapConnection::searchMemberForGroup"

LDSearchStatus LDAPConfigNode::searchMemberForGroup(
    std::vector<AuthEvent> &authEvents,
    const char *groupName,
    std::vector<char *> &members)
{
    LDSearchStatus retStatus = LDSearchResourceFailure;
    LDAPMessage *res = NULL, *entry = NULL;
    stringstream filter;
    char *attrs[2];
    string groupAttr;
    string memberAttr;
    LDAP *ld = NULL;

    char *userRDN = NULL;
    char *userBase = NULL;

    members.clear();

    timeval timeout = {this->self.host_->LDAPConfig_->timeout, 0};

    if (!getLDAPUserDNSuffix(&userRDN, &userBase))
        goto END;

    if (this->self.host_->LDAPConfig_->searchGroupBase.empty())
        goto END;

    if (this->self.host_->LDAPConfig_->searchGroupNameAttr.empty())
        groupAttr = "cn";
    else
        groupAttr = this->self.host_->LDAPConfig_->searchGroupNameAttr;

    if (this->self.host_->LDAPConfig_->searchGroupMemberAttr.empty())
        memberAttr = "member";
    else
        memberAttr = this->self.host_->LDAPConfig_->searchGroupMemberAttr;

    //(& (objectClass=groupOfNames)(cn=abc)(searchGroupCustomFilter))
    filter.str("");
    filter << "(& (objectClass=" << this->self.host_->LDAPConfig_->searchGroupObjectClass
           << ")(" << groupAttr << '=' << groupName << ')';

    APPEND_CUSTOMFILTER_TO_FILTER_FOR_GROUP(filter);
    filter << ')';

    attrs[0] = (char *)memberAttr.c_str();
    attrs[1] = NULL;

    {
        string filter_ = filter.str();
        res = (LDAPMessage *)this->searchObject(authEvents, "searchMemberForGroup",
                                                this->self.host_->LDAPConfig_->searchGroupBase.c_str(),
                                                filter_.c_str(), attrs);
    }
    if (NULL == res)
        goto END;

    ld = this->self.searchLD_;
    //I only care about the first one returned entry
    entry = ldap_first_entry(ld, res);
    if (NULL != entry)
    {
        BerElement *ber = NULL;
        //Only one attribute
        char *key = ldap_first_attribute(ld, entry, &ber);
        if (NULL != key)
        {
            struct berval **vals = ldap_get_values_len(ld, entry, key);
            if (NULL != vals)
            {
                for (int i = 0; NULL != vals[i]; i++)
                {
                    //if null or emtpy then continue
                    if (NULL == vals[i]->bv_val || 0 == vals[i]->bv_len)
                        continue;
                    //cn=abc,ou=test,dc=....
                    if (isValidDN(vals[i]->bv_val, userRDN, userBase))
                    {
                        char *tmp = DN2RDN(vals[i]->bv_val);
                        if (NULL == tmp)
                            continue;
                        members.push_back(tmp);
                    }
                }
                ldap_value_free_len(vals);
            }
            ldap_memfree(key);
        }
    }
    retStatus = LDSearchFound;
END:
    if (res != NULL)
        ldap_msgfree(res);
    if (NULL != userRDN)
        delete[] userRDN;
    if (NULL != userBase)
        delete[] userBase;
    filter.str("");
    return retStatus;
}

#pragma page "void LdapConnection::getLDAPEntryNames"

LDSearchStatus LDAPConfigNode::getLDAPEntryNames(
    std::vector<AuthEvent> &authEvents,
    std::string *base,
    std::string *filter,
    std::list<char *> *rdns)
{
    LDSearchStatus retStatus = LDSearchResourceFailure;

    LDAP *ld = NULL;
    LDAPMessage *res = NULL;

    rdns->clear();

    res = (LDAPMessage *)this->searchObject(authEvents, "getLDAPEntryNames", base->c_str(), filter->c_str(), NULL);

    if (NULL == res)
        goto END;

    ld = this->self.searchLD_;

    for (LDAPMessage *entry = ldap_first_entry(ld, res); entry != NULL; entry = ldap_next_entry(ld, entry))
    {
        char *dn = ldap_get_dn(ld, entry);
        if (NULL != dn)
        {
            char *tmp = DN2RDN(dn);
            ldap_memfree(dn);
            if (NULL == tmp)
                continue;
            rdns->push_back(tmp);
        }
    }
    retStatus = LDSearchFound;
END:
    if (res != NULL)
        ldap_msgfree(res);

    return retStatus;
}

#pragma page "void LdapConnection::searchGroupRelationship"

LDSearchStatus LDAPConfigNode::searchGroupRelationship(
    std::vector<AuthEvent> &authEvents,
    list<char *> *groupNames,
    map<char *, list<char *> *> *relationships)
{
    LDSearchStatus retStatus = LDSearchResourceFailure;
    LDAPMessage *res = NULL, *entry = NULL;
    stringstream filter;
    char *attrs[2];
    string groupAttr;
    string memberAttr;
    char *userRDN = NULL;
    char *userBase = NULL;
    LDAP *ld = NULL;
    list<char *>::iterator itor;
    int groupNamesSize = (NULL != groupNames) ? groupNames->size() : 0;

    relationships->clear();

    if (!getLDAPUserDNSuffix(&userRDN, &userBase))
        goto END;

    if (this->self.host_->LDAPConfig_->searchGroupBase.empty())
        goto END;

    if (this->self.host_->LDAPConfig_->searchGroupNameAttr.empty())
        groupAttr = "cn";
    else
        groupAttr = this->self.host_->LDAPConfig_->searchGroupNameAttr;

    if (this->self.host_->LDAPConfig_->searchGroupMemberAttr.empty())
        memberAttr = "member";
    else
        memberAttr = this->self.host_->LDAPConfig_->searchGroupMemberAttr;

    if (NULL != groupNames)
        itor = groupNames->begin();

    attrs[0] = (char *)memberAttr.c_str();
    attrs[1] = NULL;

    do
    {
        filter.str("");
        //(& (objectClass=groupOfNames)(|(cn=ABC)(cn=123))(searchGroupCustomFilter))
        filter << "(& (objectClass=" << this->self.host_->LDAPConfig_->searchGroupObjectClass << ")(|";
        if (NULL != groupNames && groupNamesSize)
        {
            for (int j = 0; j < MAX_ITEM_IN_FILTER && itor != groupNames->end(); j++, groupNamesSize--, itor++)
                filter << '(' << groupAttr << '=' << *itor << ')';
        }
        else
            filter << '(' << groupAttr << "=*)";
        filter << ')';
        APPEND_CUSTOMFILTER_TO_FILTER_FOR_GROUP(filter);
        filter << ')';

        {
            string filter_ = filter.str();
            res = (LDAPMessage *)this->searchObject(authEvents, "searchGroupRelationship",
                                                    this->self.host_->LDAPConfig_->searchGroupBase.c_str(), filter_.c_str(), attrs);
        }

        if (NULL == res)
            goto END;

        ld = this->self.searchLD_;

        for (LDAPMessage *entry = ldap_first_entry(ld, res); entry != NULL; entry = ldap_next_entry(ld, entry))
        {
            char *rdn = NULL;
            BerElement *ber = NULL;
            list<char *> *members = NULL;
            char *dn = ldap_get_dn(ld, entry);
            if (NULL != dn)
            {
                rdn = DN2RDN(dn);
                ldap_memfree(dn);
                if (NULL == rdn)
                    continue;
            }
            else
                continue;
            members = new list<char *>();
            relationships->insert(make_pair(rdn, members));
            //Only one attribute
            char *key = ldap_first_attribute(ld, entry, &ber);
            if (NULL != key)
            {
                struct berval **vals = ldap_get_values_len(ld, entry, key);
                if (NULL != vals)
                {
                    for (int i = 0; NULL != vals[i]; i++)
                    {
                        //if null or emtpy then continue
                        if (NULL == vals[i]->bv_val || 0 == vals[i]->bv_len)
                            continue;
                        if (isValidDN(vals[i]->bv_val, userRDN, userBase))
                        {
                            char *tmp = DN2RDN(vals[i]->bv_val);
                            if (NULL == tmp)
                                continue;
                            members->push_back(tmp);
                        }
                    }
                    ldap_value_free_len(vals);
                }
                ldap_memfree(key);
            }
        }
    } while (groupNamesSize > 0);
    retStatus = LDSearchFound;
END:
    if (res != NULL)
        ldap_msgfree(res);
    if (NULL != userRDN)
        delete[] userRDN;
    if (NULL != userBase)
        delete[] userBase;
    if (LDSearchResourceFailure == retStatus)
    {
        for (map<char *, list<char *> *>::iterator itor2 = relationships->begin();
             itor2 != relationships->end(); itor2++)
        {
            delete[] itor2->first;
            list<char *> *tmp = itor2->second;
            for (list<char *>::iterator itor3 = tmp->begin(); itor3 != tmp->end(); itor3++)
                delete[] * itor3;
            delete tmp;
        }
        relationships->clear();
    }
    filter.str("");
    return retStatus;
}

#pragma page "void LdapConnection::getValidLDAPEntries"

LDSearchStatus LDAPConfigNode::getValidLDAPEntries(
    std::vector<AuthEvent> &authEvents,
    std::vector<char *> &inList,
    std::vector<char *> &validList,
    bool isGroup)
{
    LDAPMessage *res = NULL;
    stringstream prefix_filter;
    stringstream filter;
    LDAP *ld = NULL;

    validList.clear();

    std::string base;
    std::string rdn;

    if (isGroup)
    {
        std::string objectClass;
        base = this->self.host_->LDAPConfig_->searchGroupBase;
        if (base.empty())
            goto ERROR;

        objectClass = this->self.host_->LDAPConfig_->searchGroupObjectClass;
        if (objectClass.empty())
            objectClass = "groupOfNames";

        rdn = this->self.host_->LDAPConfig_->searchGroupNameAttr;
        if (rdn.empty())
            rdn = "cn";

        prefix_filter.str("");
        //(& (objectClass=%s)(searchGroupCustomFilter)(|(cn=abc)(cn=123)))
        prefix_filter << "(& (objectClass=" << objectClass << ')';
        APPEND_CUSTOMFILTER_TO_FILTER_FOR_GROUP(prefix_filter);
    }
    else
    {
        char *rdn_, *base_;
        if (!getLDAPUserDNSuffix(&rdn_, &base_))
            goto ERROR;
        rdn.append(rdn_);
        base.append(base_);
        delete[] rdn_;
        delete[] base_;
        //(|(cn=abc)(cn=123))
    }
    prefix_filter << "(|";

    for (std::vector<char *>::iterator itor = inList.begin();
         itor != inList.end();)
    {
        volatile size_t itemNumInFilter = MAX_ITEM_IN_FILTER;
        filter.str("");
        filter << prefix_filter.str();
        for (; itemNumInFilter != 0 && itor != inList.end(); itemNumInFilter--, itor++)
            filter << '(' << rdn << '=' << *itor << ')';
        //add )
        {
            filter << ')';
            if (isGroup)
                filter << ')';
        }
        {
            string filter_ = filter.str();
            res = (LDAPMessage *)this->searchObject(authEvents, "getValidLDAPEntries",
                                                    base.c_str(),
                                                    filter_.c_str(), NULL);
        }

        if (NULL == res)
            goto ERROR;

        ld = this->self.searchLD_;

        for (LDAPMessage *entry = ldap_first_entry(ld, res); entry != NULL; entry = ldap_next_entry(ld, entry))
        {
            char *dn = ldap_get_dn(ld, entry);
            if (NULL != dn)
            {
                char *tmp = DN2RDN(dn);
                ldap_memfree(dn);
                if (NULL == tmp)
                    continue;
                validList.push_back(tmp);
            }
        }
    }
    return LDSearchFound;
ERROR:
    for (int i = 0; i < validList.size(); i++)
        delete[] validList[i];
    validList.clear();
    filter.str("");
    prefix_filter.str("");
    return LDSearchResourceFailure;
}

#pragma page "void LdapConnection::searchObject"

void *LDAPConfigNode::searchObject(
    std::vector<AuthEvent> &authEvents,
    const char *who,
    const char *base,
    const char *filter,
    char **attrs)
{
    int rc;
    LDAPMessage *res = NULL;
    LDAP *ld = NULL;
    char eventMsg[MAX_EVENT_MSG_SIZE];

    int reconnect = 3;

    bool dontNeedToRetAttr = (NULL == attrs);

    timeval timeout = {this->self.host_->LDAPConfig_->timeout, 0};

    while (true)
    {
        if (!selfCheck(this->self, true))
        {
            snprintf(eventMsg, MAX_EVENT_MSG_SIZE, "Self check failed in %s\n", who);
            INSERT_EVENT(authEvents, DBS_UNKNOWN_AUTH_STATUS_ERROR, eventMsg, LL_ERROR);
            rc = LDAP_CONNECT_ERROR;
            goto END;
        }

        ld = this->self.searchLD_;

        rc = ldap_search_ext_s(ld,
                               base,               // base
                               LDAP_SCOPE_SUBTREE, // search only the base
                               filter,             // filter
                               attrs,              // return attributes
                               dontNeedToRetAttr,  // get both attribute and value?
                               NULL,               // server control
                               NULL,               // client control
                               &timeout,           // timeout
                               0,                  // result size limit
                               &res);              // search result

        //Retry server down error once.
        if (rc == LDAP_SERVER_DOWN && reconnect != 0)
        {
            // reconnect & retry
            closeConnection(this->self);
            LD_Status status = initConnection(authEvents, this->self, NULL, true);
            // If the reconnect failed, report the resource error and return a
            // resource failure error so we will retry per configuration settings.
            if (status != LD_STATUS_OK)
            {
                snprintf(eventMsg, MAX_EVENT_MSG_SIZE, "LDAP %s error.   Unable to connect to server\n", who);
                INSERT_EVENT(authEvents, DBS_LDAP_SEARCH_ERROR, eventMsg, LL_ERROR);
                goto END;
            }
            reconnect--;
            continue;
        }
        break;
    }
END:
    if (LDAP_SUCCESS != rc)
    {
        // For all other search errors, report an error and return a resource
        // failure (LDAP server or network problem) so we will retry per
        // configuration settings.
        snprintf(eventMsg, MAX_EVENT_MSG_SIZE, "LDAP %s error.  Error code: %d, %s\n",
                 who, rc, ldap_err2string(rc));
        INSERT_EVENT(authEvents, DBS_LDAP_SEARCH_ERROR, eventMsg, LL_ERROR);
    }

    return (void *)res;
}

void* LDAPConfigNode::GetConfiguration()
{
    return (void*)this->self.host_->LDAPConfig_;
}

bool LDAPConfigNode::getLDAPUserDNSuffix(char **userRdn, char **userBase)
{
    bool ret = false;
    string userBase_;
    string userRdn_;
    //FIXME: only support to first uniqueIdentifier
    vector<string> uniqueIdentifiers = this->self.host_->LDAPConfig_->uniqueIdentifier;
    for (int i = 0; i < uniqueIdentifiers.size(); i++)
    {
        if (!uniqueIdentifiers[i].empty())
        {
            //cn=,ou=test,dc=....
            size_t comma = uniqueIdentifiers[i].find_first_of(',');
            size_t equal = uniqueIdentifiers[i].find_first_of('=');
            if (comma > 0)
                userBase_ = uniqueIdentifiers[i].substr(comma + 1, uniqueIdentifiers[i].length() - comma);
            if (equal > 0)
                userRdn_ = uniqueIdentifiers[i].substr(0, equal);
        }
        if (!userBase_.empty() && !userRdn_.empty())
            break;
    }
    if (userBase_.empty() || userBase_.empty())
        goto ERROR;
    *userBase = formatDN((char *)userBase_.c_str());
    //*userRdn = strdup(userRdn_.c_str());
    *userRdn = new char[userRdn_.length() + 1];
    strcpy(*userRdn, userRdn_.c_str());
    return true;
ERROR:
    return false;
}