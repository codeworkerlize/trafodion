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

#ifndef PRIVMGR_DEFS_H
#define PRIVMGR_DEFS_H

#include <bitset>
#include <iterator>
#include <map>

#include "common/ComSmallDefs.h"
#include "common/NAUserId.h"

// *****************************************************************************
// *
// * File:         PrivMgrDef.h
// * Description:  This file contains common definitions used by the
// *               privilege manager component
// *
// *****************************************************************************

#define PRIVMGR_INTERNAL_ERROR(text) \
  *pDiags_ << DgSqlCode(-CAT_INTERNAL_EXCEPTION_ERROR) << DgString0(__FILE__) << DgInt0(__LINE__) << DgString1(text)

#define PRIVMGR_OBJECT_PRIVILEGES    "OBJECT_PRIVILEGES"
#define PRIVMGR_COLUMN_PRIVILEGES    "COLUMN_PRIVILEGES"
#define PRIVMGR_COMPONENTS           "COMPONENTS"
#define PRIVMGR_COMPONENT_OPERATIONS "COMPONENT_OPERATIONS"
#define PRIVMGR_COMPONENT_PRIVILEGES "COMPONENT_PRIVILEGES"
#define PRIVMGR_ROLE_USAGE           "ROLE_USAGE"
#define PRIVMGR_SCHEMA_PRIVILEGES    "SCHEMA_PRIVILEGES"

// Returns the result of the operation
enum PrivStatus { STATUS_UNKNOWN = 20, STATUS_GOOD = 21, STATUS_WARNING = 22, STATUS_NOTFOUND = 23, STATUS_ERROR = 24 };

enum PrivMgrTableEnum {
  OBJECT_PRIVILEGES_ENUM = 30,
  COLUMN_PRIVILEGES_ENUM = 31,
  SCHEMA_PRIVILEGES_ENUM = 32,
  COMPONENTS_ENUM = 33,
  COMPONENT_OPERATIONS_ENUM = 34,
  COMPONENT_PRIVILEGES_ENUM = 35,
  ROLE_USAGES_ENUM = 36,
  OBJECTS_ENUM = 37,
  UNKNOWN_ENUM = 38
};

inline const char *privStatusEnumToLit(PrivStatus privStatus) {
  std::string result;
  switch (privStatus) {
    case STATUS_GOOD:
      result = "GOOD";
      break;
    case STATUS_WARNING:
      result = "WARNING";
      break;
    case STATUS_NOTFOUND:
      result = "NOTFOUND";
      break;
    case STATUS_ERROR:
      result = "ERROR";
      break;
    default:
      result = "UNKNOWN";
  }
  return result.c_str();
}

enum class PrivClass { ALL = 2, OBJECT = 3, COMPONENT = 4, SCHEMA = 5 };

// Defines the list of supported privileges and their order that are stored
// for all privileges_bitmap and grantable_bitmap columns defined in the
// [COLUMN|OBJECT|SCHEMA]_PRIVILEGES tables.
// Not all privileges are applicable for the privilege level (column, object, schema)
// and object type (schema, tables, views, etc).
// Privileges are stored in LARGEINT columns but are treated as bitmaps.  We
// can handle up to 64 privilege types.  If have more than 64 types,  then
// metadata needs to be upgraded to store values in two columns.
enum PrivType {
  SELECT_PRIV = 0,  // DML PRIVS START HERE
  INSERT_PRIV,
  DELETE_PRIV,
  UPDATE_PRIV,
  USAGE_PRIV,
  REFERENCES_PRIV,
  EXECUTE_PRIV,
  // space for additional DML privs, e.g.TRIGGER_PRIV
  CREATE_PRIV = 10,  // DDL PRIVS START HERE
  ALTER_PRIV,
  DROP_PRIV,
  CREATE_LIBRARY_PRIV = 13,  // DDL SUB_PRIVS START HERE
  CREATE_PACKAGE_PRIV,
  CREATE_ROUTINE_PRIV,
  CREATE_SEQUENCE_PRIV,
  CREATE_TABLE_PRIV,
  CREATE_VIEW_PRIV,
  // space for additional create privs
  ALTER_LIBRARY_PRIV = 25,
  ALTER_PACKAGE_PRIV,
  ALTER_ROUTINE_PRIV,
  ALTER_SEQUENCE_PRIV,
  ALTER_TABLE_PRIV,
  ALTER_VIEW_PRIV,
  // space for additional alter privs
  DROP_LIBRARY_PRIV = 37,
  DROP_PACKAGE_PRIV,
  DROP_ROUTINE_PRIV,
  DROP_SEQUENCE_PRIV,
  DROP_TABLE_PRIV,
  DROP_VIEW_PRIV,
  // space for additional drop privs
  // space for other possible privs, e.g. manage_statistics
  ALL_DML = 60,
  ALL_DDL,
  ALL_PRIVS
};

class ColPrivSpec {
 public:
  PrivType privType;
  int32_t columnOrdinal;
  bool grantorHasWGO;
};

inline bool isColumnPrivType(PrivType privType) {
  return (privType == PrivType::SELECT_PRIV || privType == PrivType::INSERT_PRIV ||
          privType == PrivType::REFERENCES_PRIV || privType == PrivType::UPDATE_PRIV);
}

inline bool isLibraryPrivType(PrivType privType) {
  return (privType == PrivType::USAGE_PRIV || privType == PrivType::UPDATE_PRIV || privType == PrivType::DROP_PRIV ||
          privType == PrivType::ALTER_PRIV || privType == PrivType::ALL_DML || privType == PrivType::ALL_DDL);
}

inline bool isTablePrivType(PrivType privType) {
  return (privType == PrivType::SELECT_PRIV || privType == PrivType::INSERT_PRIV || privType == PrivType::DELETE_PRIV ||
          privType == PrivType::REFERENCES_PRIV || privType == PrivType::UPDATE_PRIV ||
          privType == PrivType::DROP_PRIV || privType == PrivType::ALTER_PRIV || privType == PrivType::ALL_DML ||
          privType == PrivType::ALL_DDL);
}

inline bool isUDRPrivType(PrivType privType) {
  return (privType == PrivType::EXECUTE_PRIV || privType == PrivType::DROP_PRIV || privType == PrivType::ALTER_PRIV ||
          privType == PrivType::ALL_DML || privType == PrivType::ALL_DDL);
}

inline bool isSequenceGeneratorPrivType(PrivType privType) {
  return (privType == PrivType::USAGE_PRIV || privType == PrivType::DROP_PRIV || privType == PrivType::ALTER_PRIV ||
          privType == PrivType::ALL_DML || privType == PrivType::ALL_DDL);
}

enum class PrivDropBehavior { CASCADE = 2, RESTRICT = 3 };

enum class PrivLevel { UNKNOWN = 0, GLOBAL = 2, CATALOG = 3, SCHEMA = 4, OBJECT = 5, COLUMN = 6 };

// NOTE: These values need to match the corresponding values in
// common/ComSmallDefs.h, ComIdClass.
enum class PrivAuthClass { UNKNOWN = 0, ROLE = 1, USER = 2 };

const static int32_t FIRST_DML_PRIV = SELECT_PRIV;
const static int32_t FIRST_DML_COL_PRIV = SELECT_PRIV;
const static int32_t FIRST_PRIV = SELECT_PRIV;
const static int32_t LAST_PRIMARY_DML_PRIV = UPDATE_PRIV;
const static int32_t LAST_DML_PRIV = EXECUTE_PRIV;
const static int32_t LAST_DML_COL_PRIV = REFERENCES_PRIV;
const static int32_t FIRST_DDL_PRIV = CREATE_PRIV;
const static int32_t LAST_DDL_PRIV = DROP_PRIV;
const static int32_t LAST_PRIV = LAST_DDL_PRIV;

const static int32_t NBR_DML_PRIVS = LAST_DML_PRIV - FIRST_DML_PRIV + 1;
// This calculation includes non-column-level privileges.  There are only four
// column-level privileges, but DELETE and USAGE are include so bit indexing works.
const static int32_t NBR_DML_COL_PRIVS = LAST_DML_COL_PRIV - FIRST_DML_COL_PRIV + 1;
const static int32_t NBR_DDL_PRIVS = LAST_DDL_PRIV - FIRST_DDL_PRIV + 1;
const static int32_t NBR_OF_PRIVS = LAST_DDL_PRIV + 1;

// Defines the privileges and grantable bitmaps as PrivMgrBitmap
// using PrivMgrBitmap = std::bitset<NBR_OF_PRIVS>;
#define PrivMgrBitmap std::bitset<NBR_OF_PRIVS>
typedef std::bitset<NBR_OF_PRIVS> PrivObjectBitmap;
typedef std::bitset<NBR_OF_PRIVS> PrivColumnBitmap;
typedef std::bitset<NBR_OF_PRIVS> PrivSchemaBitmap;
typedef std::map<size_t, PrivColumnBitmap> PrivColList;
typedef std::map<size_t, std::bitset<NBR_OF_PRIVS> >::const_iterator PrivColIterator;

inline bool isDMLPrivType(PrivType privType) {
  if ((privType >= FIRST_DML_PRIV && privType <= LAST_DML_PRIV) || privType == ALL_DML) return true;

  return false;
}

inline bool isDDLPrivType(PrivType privType) {
  if ((privType >= FIRST_DDL_PRIV && privType <= LAST_DDL_PRIV) || privType == ALL_DDL) return true;

  return false;
}

// object types for grantable objects
#define BASE_TABLE_OBJECT_LIT           "BT"
#define LIBRARY_OBJECT_LIT              "LB"
#define VIEW_OBJECT_LIT                 "VI"
#define USER_DEFINED_ROUTINE_OBJECT_LIT "UR"
#define SEQUENCE_GENERATOR_OBJECT_LIT   "SG"

#define UNKNOWN_GRANTOR_TYPE_LIT "  "
#define SYSTEM_GRANTOR_LIT       "S "
#define USER_GRANTOR_LIT         "U "

#define UNKNOWN_GRANTEE_TYPE_LIT "  "
#define PUBLIC_GRANTEE_LIT       "P "
#define USER_GRANTEE_LIT         "U "

#define MAX_SQL_IDENTIFIER_NAME_LEN 256

#endif
