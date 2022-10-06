//*****************************************************************************
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
//// @@@ END COPYRIGHT @@@
//*****************************************************************************

#ifndef PRIVMGR_MD_DEFS_H
#define PRIVMGR_MD_DEFS_H

#include "CmpSeabaseDDLmdcommon.h"

// *****************************************************************************
// *
// * File:         PrivMgrMDDefs.h
// * Description:  definitions of all the objects managed by PrivMgr component
// *    Table DDL for objects defined in the "_PRIVMGR_MD_" schema
// *    Old table definitions from previous upgrades
// *    List of PrivMgr definitions in MDUpgradeInfo format
// *
// ****************************************************************************

// List of tables used during upgrade
#define PRIVMGR_OBJECT_PRIVILEGES_OLD    PRIVMGR_OBJECT_PRIVILEGES "_OLD_MD"
#define PRIVMGR_COLUMN_PRIVILEGES_OLD    PRIVMGR_COLUMN_PRIVILEGES "_OLD_MD"
#define PRIVMGR_COMPONENTS_OLD           PRIVMGR_COMPONENTS "_OLD_MD"
#define PRIVMGR_COMPONENT_OPERATIONS_OLD PRIVMGR_COMPONENT_OPERATIONS "_OLD_MD"
#define PRIVMGR_COMPONENT_PRIVILEGES_OLD PRIVMGR_COMPONENT_PRIVILEGES "_OLD_MD"
#define PRIVMGR_ROLE_USAGE_OLD           PRIVMGR_ROLE_USAGE "_OLD_MD"
#define PRIVMGR_SCHEMA_PRIVILEGES_OLD    PRIVMGR_SCHEMA_PRIVILEGES "_OLD_MD"

// enums for errors encountered during upgrade failures
enum upgradeErrNum {
  ERR_REVOKE_PRIVS = -1,
  ERR_DROP_NEW_TBLS = -2,
  ERR_RESTORE_ORIG_TBLS = -3,
  ERR_GRANT_PRIVS = -4,
  ERR_DONE = -5,
  ERR_RESTORE_FAILED = -6
};

// The PrivMgrTableStruct is used to describe a PrivMgr metadata table
struct PrivMgrTableStruct {
  const char *tableName;
  const QString *tableDDL;
  const bool isIndex;
};

// Trafodion creates HBase tables that concatenate the catalog, schema, and
// object name together.  The max HBase name can only be 255.  As long as
// we create Trafodion objects in HBase the same way, the object_name variables
// stored in the PrivMgr tables cannot exceed 255.  If we decide to change
// our naming convention, this size could change.

// ----------------------------------------------------------------------------
// START - current definitions in alphabetic order
// ----------------------------------------------------------------------------
static const QString columnPrivilegesDDL[] = {
    " ( \
   object_uid largeint not null not serialized, \
   object_name varchar(600 bytes) character set utf8 not null not serialized, \
   grantee_id int not null not serialized, \
   grantee_name varchar(256 bytes) character set utf8 not null not serialized, \
   grantor_id int not null not serialized, \
   grantor_name varchar(256 bytes) character set utf8 not null not serialized, \
   column_number int not null not serialized, \
   privileges_bitmap largeint not null not serialized, \
   grantable_bitmap largeint not null not serialized, \
   primary key (object_uid, grantee_id, grantor_id, column_number) \
   ) attribute hbase format;"};

static const QString componentsDDL[] = {
    " ( \
   component_uid largeint not null not serialized primary key, \
   component_name varchar(128 bytes) character set ISO88591 not null not serialized, \
   is_system char(2) character set iso88591 not null not serialized, \
   component_description varchar(80 bytes) character set ISO88591 default null not serialized \
   )  attribute hbase format;"};

static const QString componentOperationsDDL[] = {
    " ( \
  component_uid largeint not null not serialized, \
  operation_code char(2 bytes) character set ISO88591 not null not serialized, \
  operation_name varchar(256 bytes) character set ISO88591 not null not serialized, \
  is_system char(2) character set iso88591 not null not serialized, \
  operation_description char(80 bytes) character set ISO88591 default null not serialized, \
  primary key (component_uid, operation_code) \
  ) attribute hbase format;"};

static const QString componentPrivilegesDDL[] = {
    " ( \
  grantee_id int not null not serialized, \
  grantor_id int not null not serialized, \
  component_uid largeint not null not serialized, \
  operation_code char(2 bytes) character set ISO88591 not null not serialized, \
  grantee_name varchar(256 bytes) character set utf8 not null not serialized, \
  grantor_name varchar(256 bytes) character set utf8 not null not serialized, \
  grant_depth int not null not serialized, \
  primary key (grantee_id, grantor_id, component_uid, operation_code) \
  ) attribute hbase format;"};

static const QString objectPrivilegesDDL[] = {
    " ( \
  object_uid largeint not null not serialized, \
  object_name varchar(600 bytes) character set utf8 not null not serialized, \
  object_type char (2 bytes)not null not serialized, \
  grantee_id largeint not null not serialized, \
  grantee_name varchar(256 bytes) character set utf8 not null not serialized, \
  grantee_type char (2 bytes) not null not serialized, \
  grantor_id largeint not null not serialized, \
  grantor_name varchar(256 bytes) character set utf8 not null not serialized, \
  grantor_type char (2 bytes) not null not serialized, \
  privileges_bitmap largeint not null not serialized, \
  grantable_bitmap largeint not null not serialized, \
  primary key (object_uid, grantor_id, grantee_id) \
  ) attribute hbase format;"};

static const QString roleUsageDDL[] = {
    " ( \
  role_id int not null not serialized, \
  role_name varchar(256 bytes) character set utf8 not null not serialized, \
  grantee_id int not null not serialized, \
  grantee_name varchar(256 bytes) character set utf8 not null not serialized, \
  grantee_auth_class char (2 bytes) character set utf8 not null not serialized, \
  grantor_id int not null not serialized, \
  grantor_name varchar(256 bytes) character set utf8 not null not serialized, \
  grantor_auth_class char (2 bytes) character set utf8 not null not serialized, \
  grant_depth int not null not serialized, \
  primary key (role_id, grantor_id, grantee_id) \
  ) attribute hbase format;"};

static const QString schemaPrivilegesDDL[] = {
    " ( \
  schema_uid largeint not null not serialized, \
  schema_name varchar(600 bytes) character set utf8 not null not serialized, \
  grantee_id int not null not serialized, \
  grantee_name varchar(256 bytes) character set utf8 not null not serialized, \
  grantor_id int not null not serialized, \
  grantor_name varchar(256 bytes) character set utf8 not null not serialized, \
  privileges_bitmap largeint not null not serialized, \
  grantable_bitmap largeint not null not serialized, \
  primary key (schema_uid, grantor_id, grantee_id) \
  ) attribute hbase format;"};

// ----------------------------------------------------------------------------
// The PrivMgrTableStruct describes each table
// ----------------------------------------------------------------------------
static const PrivMgrTableStruct privMgrTables[] = {{PRIVMGR_OBJECT_PRIVILEGES, objectPrivilegesDDL, false},
                                                   {PRIVMGR_COLUMN_PRIVILEGES, columnPrivilegesDDL, false},
                                                   {PRIVMGR_COMPONENTS, componentsDDL, false},
                                                   {PRIVMGR_COMPONENT_OPERATIONS, componentOperationsDDL, false},
                                                   {PRIVMGR_COMPONENT_PRIVILEGES, componentPrivilegesDDL, false},
                                                   {PRIVMGR_ROLE_USAGE, roleUsageDDL, false},
                                                   {PRIVMGR_SCHEMA_PRIVILEGES, schemaPrivilegesDDL, false}};

// ----------------------------------------------------------------------------
// START_OLD_PRIVMGR_MD_V210
//    (major version 2, minor version 1, update version 0)
// ----------------------------------------------------------------------------
static const QString oldMDv210ColumnPrivilegesDDL[] = {
    " ( \
   object_uid largeint not null, \
   object_name varchar(600 bytes) character set utf8 not null, \
   grantee_id int not null not serialized, \
   grantee_name varchar(256 bytes) character set utf8 not null, \
   grantor_id int not null, \
   grantor_name varchar(256 bytes) character set utf8 not null, \
   column_number int not null, \
   privileges_bitmap largeint not null, \
   grantable_bitmap largeint not null, \
   primary key (object_uid, grantee_id, grantor_id, column_number) \
   ) attribute hbase format;"};

static const QString oldMDv210ComponentsDDL[] = {
    " ( \
   component_uid largeint not null primary key , \
   component_name varchar(128 bytes) character set utf8 not null, \
   is_system char(2) character set utf8 not null, \
   component_description varchar(80 bytes) character set ISO88591 default null \
   )  attribute hbase format;"};

static const QString oldMDv210ComponentOperationsDDL[] = {
    " ( \
  component_uid largeint not null, \
  operation_code char(2 bytes) character set ISO88591 not null, \
  operation_name varchar(256 bytes) character set utf8 not null, \
  is_system char(2) character set utf8 not null, \
  operation_description char(80 bytes) character set ISO88591 default null, \
  primary key (component_uid, operation_code) \
  ) attribute hbase format;"};

static const QString oldMDv210ComponentPrivilegesDDL[] = {
    " ( \
  grantee_id int not null, \
  grantor_id int not null, \
  component_uid largeint not null, \
  operation_code char(2 bytes) character set ISO88591 not null, \
  grantee_name varchar(256 bytes) character set utf8 not null, \
  grantor_name varchar(256 bytes) character set utf8 not null, \
  grant_depth int not null, \
  primary key (grantee_id, grantor_id, component_uid, operation_code) \
  ) attribute hbase format;"};

static const QString oldMDv210ObjectPrivilegesDDL[] = {
    " ( \
  object_uid largeint not null, \
  object_name varchar(600 bytes) character set utf8 not null, \
  object_type char (2 bytes)not null, \
  grantee_id largeint not null, \
  grantee_name varchar(256 bytes) character set utf8 not null, \
  grantee_type char (2 bytes) not null, \
  grantor_id largeint not null, \
  grantor_name varchar(256 bytes) character set utf8 not null, \
  grantor_type char (2 bytes) not null, \
  privileges_bitmap largeint not null, \
  grantable_bitmap largeint not null, \
  primary key (object_uid, grantor_id, grantee_id) \
  ) attribute hbase format;"};

static const QString oldMDv210RoleUsageDDL[] = {
    " ( \
  role_id int not null, \
  role_name varchar(256 bytes) character set utf8 not null, \
  grantee_id int not null, \
  grantee_name varchar(256 bytes) character set utf8 not null, \
  grantee_auth_class char (2 bytes) character set utf8 not null, \
  grantor_id int not null, \
  grantor_name varchar(256 bytes) character set utf8 not null, \
  grantor_auth_class char (2 bytes) character set utf8 not null, \
  grant_depth int not null, \
  primary key (role_id, grantor_id, grantee_id) \
  ) attribute hbase format;"};

static const QString oldMDv210SchemaPrivilegesDDL[] = {
    " ( \
  schema_uid largeint not null, \
  schema_name varchar(600 bytes) character set utf8 not null, \
  grantee_id int not null, \
  grantee_name varchar(256 bytes) character set utf8 not null, \
  grantor_id int not null, \
  grantor_name varchar(256 bytes) character set utf8 not null, \
  privileges_bitmap largeint not null, \
  grantable_bitmap largeint not null, \
  primary key (schema_uid, grantor_id, grantee_id) \
  ) attribute hbase format;"};

// ----------------------------------------------------------------------------
// Upgrade structure for PrivMgr metadata
//    MDUpgradeInfo is defined in CmpSeabaseDDLmdcommon.h
// ----------------------------------------------------------------------------
static const MDUpgradeInfo allPrivMgrUpgradeInfo[] = {
    // PRIVMGR_OBJECT_PRIVILEGES
    {PRIVMGR_OBJECT_PRIVILEGES, PRIVMGR_OBJECT_PRIVILEGES_OLD, objectPrivilegesDDL, sizeof(objectPrivilegesDDL),
     oldMDv210ObjectPrivilegesDDL, sizeof(oldMDv210ObjectPrivilegesDDL), NULL, 0, TRUE, NULL, NULL, NULL, FALSE, FALSE,
     FALSE, FALSE, FALSE, FALSE},

    // PRIVMGR_COLUMN_PRIVILEGES
    {PRIVMGR_COLUMN_PRIVILEGES, PRIVMGR_COLUMN_PRIVILEGES_OLD, columnPrivilegesDDL, sizeof(columnPrivilegesDDL),
     oldMDv210ColumnPrivilegesDDL, sizeof(oldMDv210ColumnPrivilegesDDL), NULL, 0, TRUE, NULL, NULL, NULL, FALSE, FALSE,
     FALSE, FALSE, FALSE, FALSE},

    // PRIVMGR_COMPONENTS
    {PRIVMGR_COMPONENTS, PRIVMGR_COMPONENTS_OLD, componentsDDL, sizeof(componentsDDL), oldMDv210ComponentsDDL,
     sizeof(oldMDv210ComponentsDDL), NULL, 0, TRUE, "component_uid, component_name, is_system, component_description",
     "component_uid, component_name, cast (is_system as char(2) character set iso88591), component_description", NULL,
     FALSE, FALSE, FALSE, FALSE, FALSE, FALSE},

    // PRIVMGR_COMPONENT_OPERATIONS
    {PRIVMGR_COMPONENT_OPERATIONS, PRIVMGR_COMPONENT_OPERATIONS_OLD, componentOperationsDDL,
     sizeof(componentOperationsDDL), oldMDv210ComponentOperationsDDL, sizeof(oldMDv210ComponentOperationsDDL), NULL, 0,
     TRUE, "component_uid, operation_code, operation_name, is_system, operation_description",
     "component_uid, operation_code, operation_name, cast (is_system as char(2) character set iso88591), "
     "operation_description",
     NULL, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE},

    // PRIVMGR_COMPONENT_PRIVILEGES
    {PRIVMGR_COMPONENT_PRIVILEGES, PRIVMGR_COMPONENT_PRIVILEGES_OLD, componentPrivilegesDDL,
     sizeof(componentPrivilegesDDL), oldMDv210ComponentPrivilegesDDL, sizeof(oldMDv210ComponentPrivilegesDDL), NULL, 0,
     TRUE, NULL, NULL, NULL, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE},

    // PRIVMGR_ROLE_USAGE
    {PRIVMGR_ROLE_USAGE, PRIVMGR_ROLE_USAGE_OLD, roleUsageDDL, sizeof(roleUsageDDL), oldMDv210RoleUsageDDL,
     sizeof(oldMDv210RoleUsageDDL), NULL, 0, TRUE, NULL, NULL, NULL, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE},

    // PRIVMGR_SCHEMA_PRIVILEGES
    {PRIVMGR_SCHEMA_PRIVILEGES, PRIVMGR_SCHEMA_PRIVILEGES_OLD, schemaPrivilegesDDL, sizeof(schemaPrivilegesDDL),
     oldMDv210SchemaPrivilegesDDL, sizeof(oldMDv210SchemaPrivilegesDDL), NULL, 0, TRUE, NULL, NULL, NULL, FALSE, FALSE,
     FALSE, FALSE, FALSE, FALSE}};

#endif  // PRIVMGR_MD_DEFS_H
