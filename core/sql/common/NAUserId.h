/**********************************************************************
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
**********************************************************************/
#ifndef NAUSERID_H
#define NAUSERID_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         NAUserId.h
 * Description:  describes defines and structures for authorization IDs
 * Created:      8/19/2002
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */
#include "Platform.h"

// ----------------------------------------------------------------------------
// standard defines:
#define MAX_DBUSERNAME_LEN 128
#define MAX_USERNAME_LEN 128
#define MAX_AUTHNAME_LEN 128
#define MAX_AUTHID_AS_STRING_LEN 20
#define NA_UserIdDefault 0
#define MAX_SLA_NAME_LEN 32
#define MAX_PROFILE_NAME_LEN 32
// ----------------------------------------------------------------------------
#define MAX_CLIENT_HOSTNAME_LEN 128
#define MAX_CLIENT_OSNAME_LEN   128
#define NA_CLIENT_HOSTNAME      "localhost"
#define NA_CLIENT_OSUSERNAME    "trafodion"
#define NA_CLIENT_IP_ADDRESS    "127.0.0.1"
// ----------------------------------------------------------------------------
// Authorization range definitions:
// Authorization ID's include users, roles, and tenants (maybe groups later)
#define MIN_USERID          33332 /* reserve 1 to 33333 for system users */
#define MAX_USERID         799999

#define MIN_USERGROUP      800000
#define ROOT_USERGROUP     800100
#define MAX_USERGROUP      999999

#define MIN_ROLEID        1000000
#define MAX_ROLEID_RANGE1 1490000
#define MAX_ROLEID        1499999

#define MIN_TENANTID      1500000 /* reserve first 100 for system tenants */
#define ROOT_TENANTID     1500100
#define MAX_TENANTID      1599999

// ----------------------------------------------------------------------------
// For roles and other non-user authIDS, use the following structure to create 
// new system objects
// At this time, we don't have any system user groups
struct SystemAuthsStruct
{
   const char *authName;
   bool       isSpecialAuth;
   int32_t    authID;
};

// ----------------------------------------------------------------------------
// Definitions for system users:
// For new system tenants, generate the username and the userID; change
// CmpSeabaseDDL::updateSeabaseAuths to register the new (standard) user
#define DB__ROOT         "DB__ROOT"
#define DB__ADMIN        "DB__ADMIN"
#define SUPER_USER_LIT   "33333"
#define DEFAULT_EXTERNAL_NAME  "NOT AVAILABLE"

// If a new system defined user is added, subtract one from ADMIN_USER_ID and
// be sure to change MIN_USERID to the smaller value
#define LOWER_INVALID_ID -3
#define SYSTEM_USER  -2
#define PUBLIC_USER  -1
#define ADMIN_USER_ID 33332
#define ROOT_USER_ID  33333
#define SUPER_USER    33333  

// -----------------------------------------------------------------------------
// Definitions for system roles:
// For new system roles, add a define and include it in the systemRoles constant
// When authorization is enabled, these roles are created, no additional changes
// to the code is required.
#define SYSTEM_AUTH_NAME "_SYSTEM"
#define PUBLIC_AUTH_NAME "PUBLIC"
#define DB__HIVEROLE     "DB__HIVEROLE"
#define DB__HBASEROLE    "DB__HBASEROLE"
#define DB__ROOTROLE     "DB__ROOTROLE"
#define DB__ADMINROLE    "DB__ADMINROLE"
#define DB__SERVICESROLE "DB__SERVICESROLE"
#define DB__LIBMGRROLE   "DB__LIBMGRROLE"

// Most system roles do not have a predefined range of IDs, so for new roles
// just specify NA_UserIdDefault in the systemRoles struct. Role code creates
// roles for each non special system role in the list.  If NA_UserIdDefault is
// specified, the code generates a UniqueID.
#define ROOT_ROLE_ID     1000000
#define HIVE_ROLE_ID     1490000 
#define HBASE_ROLE_ID    1490001

static const SystemAuthsStruct systemRoles[] 
{ { DB__HIVEROLE, false, HIVE_ROLE_ID },
  { DB__HBASEROLE, false, HBASE_ROLE_ID },
  { DB__ROOTROLE, false, ROOT_ROLE_ID },
  { DB__ADMINROLE, false, NA_UserIdDefault },
  { DB__SERVICESROLE, false, NA_UserIdDefault },
  { DB__LIBMGRROLE, false, NA_UserIdDefault },
  { PUBLIC_AUTH_NAME, true, PUBLIC_USER },
  { SYSTEM_AUTH_NAME, true, SYSTEM_USER } };

#define NUMBER_SPECIAL_SYSTEM_ROLES 2;

// ----------------------------------------------------------------------------
// Definitions for system tenants:
// For new system tenants, generate the tenant name and the tenantID; add an
// entry to systemTenants structure.  
// When the multi-tenant feature is enabled, these tenants are registered, no
// additional code changes are required.

// Most system objects begin with DB__ but DB__SYSTEMTENANT references the 
// associated cgroup and the Esgyn cgroup is ESGYNDB.
#define DB__SYSTEMTENANT  "ESGYNDB"

// If a new system defined tenant is added, add one to 
// SYSTEM_TENANT_ID systemTenants structure
#define SYSTEM_TENANT_ID MIN_TENANTID

static const SystemAuthsStruct systemTenants[]
{ {DB__SYSTEMTENANT, false, SYSTEM_TENANT_ID} }; 

#endif  /*  NAUSERID_H*/
