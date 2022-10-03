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

/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ExExeUtilGet.cpp
 * Description:
 *
 *
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "ComCextdecs.h"
#include  "cli_stdh.h"
#include  "ex_stdh.h"
#include  "sql_id.h"
#include  "ex_transaction.h"
#include  "ComTdb.h"
#include  "ex_tcb.h"
#include  "ComSqlId.h"
#include  "ComMisc.h"
#include  "ComUser.h"
#include  "dbUserAuth.h"

#include  "ExExeUtil.h"
#include  "ex_exe_stmt_globals.h"
#include  "exp_expr.h"
#include  "exp_clause_derived.h"
#include  "ExpLOBinterface.h"
#include  "ComRtUtils.h"
#include  "CmpCommon.h"
#include  "CmpContext.h"

#include  "sqlcmd.h"
#include  "SqlciEnv.h"

#include  "GetErrorMessage.h"
#include  "ErrorMessage.h"
#include  "HBaseClient_JNI.h"

#include "CmpDDLCatErrorCodes.h"
#include "PrivMgrDefs.h"
#include "PrivMgrCommands.h"
#include "PrivMgrComponentPrivileges.h"

#include "ExpHbaseInterface.h"
#include "sql_buffer_size.h"

#include "NAType.h"

//******************************************************************************
//                                                                             *
//  These definitions were stolen from CatWellKnownTables.h
//
// Size of CHAR(128) CHARACTER SET UCS2 NOT NULL column is 256 bytes.
#define EX_MD_XXX_NAME_CHAR_LEN "128"
//
// Character type columns in metadata tables are generally case-sensitive
#define ISO_CHAR_ATTR    " CHARACTER SET ISO88591 CASESPECIFIC "
#define UCS2_CHAR_ATTR   " CHARACTER SET UCS2 CASESPECIFIC "
//
// Add explicit collate default to avoid inherit it from table or schema
#define ISO_CHAR_ATTR_2    " CHARACTER SET ISO88591 COLLATE DEFAULT CASESPECIFIC "
#define UCS2_CHAR_ATTR_2   " CHARACTER SET UCS2 COLLATE DEFAULT CASESPECIFIC "
//
// Most - if not all - columns are NNND
#define NNND_ATTR  " NOT NULL NOT DROPPABLE "
//                                                                             *
//******************************************************************************

///////////////////////////////////////////////////////////////////
ex_tcb * ExExeUtilGetMetadataInfoTdb::build(ex_globals * glob)
{
  ExExeUtilGetMetadataInfoTcb * exe_util_tcb;

  if ((groupBy() || orderBy()) ||
      (queryType() == ComTdbExeUtilGetMetadataInfo::OBJECTS_ON_TABLE_) ||
      (queryType() == ComTdbExeUtilGetMetadataInfo::OBJECTS_IN_SCHEMA_))
    exe_util_tcb =
      new(glob->getSpace()) ExExeUtilGetMetadataInfoComplexTcb(*this, glob);
  //else if (getVersion())
  //  exe_util_tcb =
  //    new(glob->getSpace()) ExExeUtilGetMetadataInfoVersionTcb(*this, glob);
  else if (queryType() == ComTdbExeUtilGetMetadataInfo::HBASE_OBJECTS_ ||
           queryType() == ComTdbExeUtilGetMetadataInfo::MONARCH_OBJECTS_ ||
           queryType() == ComTdbExeUtilGetMetadataInfo::BIGTABLE_OBJECTS_ )
    exe_util_tcb =
      new(glob->getSpace()) ExExeUtilGetHbaseObjectsTcb(*this, glob);
  else if (queryType() == ComTdbExeUtilGetMetadataInfo::EXTERNAL_NAMESPACES_ ||
           queryType() == ComTdbExeUtilGetMetadataInfo::TRAFODION_NAMESPACES_ ||
           queryType() == ComTdbExeUtilGetMetadataInfo::SYSTEM_NAMESPACES_ ||
           queryType() == ComTdbExeUtilGetMetadataInfo::ALL_NAMESPACES_ ||
           queryType() == ComTdbExeUtilGetMetadataInfo::OBJECTS_IN_NAMESPACE_ ||
           queryType() == ComTdbExeUtilGetMetadataInfo::NAMESPACE_CONFIG_)
    exe_util_tcb =
      new(glob->getSpace()) ExExeUtilGetNamespaceObjectsTcb(*this, glob);
  else
    exe_util_tcb =
      new(glob->getSpace()) ExExeUtilGetMetadataInfoTcb(*this, glob);

  exe_util_tcb->registerSubtasks();

  return (exe_util_tcb);
}

////////////////////////////////////////////////////////////////
// Constructor for class ExExeUtilGetMetadataInfoTcb
///////////////////////////////////////////////////////////////
ExExeUtilGetMetadataInfoTcb::ExExeUtilGetMetadataInfoTcb(
     const ComTdbExeUtilGetMetadataInfo & exe_util_tdb,
     ex_globals * glob)
     : ExExeUtilTcb( exe_util_tdb, NULL, glob)
{
  // Allocate the private state in each entry of the down queue
  qparent_.down->allocatePstate(this);

  step_ = INITIAL_;
  vStep_ = VIEWS_INITIAL_;

  // allocate space to hold the metadata query that will be used to retrieve
  // metadata info. 10K is big enough for it.
  queryBuf_ = new(glob->getDefaultHeap()) char[10000];

  // buffer where output will be formatted
  outputBuf_ = new(glob->getDefaultHeap()) char[4096];

  headingBuf_ = new(glob->getDefaultHeap()) char[1000];

  for (Int32 i = 0; i < NUM_MAX_PARAMS_; i++)
    {
      param_[i] = NULL;
    }

  patternStr_ = new(glob->getDefaultHeap()) char[1000];

  numOutputEntries_ = 0;
  returnRowCount_ = 0;
}

ExExeUtilGetMetadataInfoTcb::~ExExeUtilGetMetadataInfoTcb()
{
  NADELETEBASIC(queryBuf_, getGlobals()->getDefaultHeap());
  NADELETEBASIC(outputBuf_, getGlobals()->getDefaultHeap());
  NADELETEBASIC(headingBuf_, getGlobals()->getDefaultHeap());
  NADELETEBASIC(patternStr_, getGlobals()->getDefaultHeap());
}

static NABoolean userPartOfGroup(
  const char * groupList, 
  Int32 groupToCheck)
{
  char cpyList [strlen(groupList) + 1];
  strcpy(cpyList, groupList);
  char * str = (char *) cpyList;
  str ++; // skip (
  char *tok = strtok(str, ",)");
  while (tok != NULL)
  {
    Int32 groupInList = atoi(tok);
    if (groupInList == groupToCheck)
      return TRUE;
    tok = strtok(NULL, ",)");
  }
  return FALSE;
}

// ----------------------------------------------------------------------------
// method: getBitExtractsForObjectType
//
// fix for mantis-11307: [DDL]the showddl schema is not displayed correctly
// 
// This method generates the select list that displays privileges based on the
// object type.  Bits are stored in the privileges_bitmap column in each 
// xxxxx_privilege table. The following bits are currently supported:
//   63 - select (S)
//   62 - insert (I)
//   61 - delete (D)
//   60 - update (U)
//   59 - usage (G)
//   58 - references (R)
//   57 - execute (E)
//   56 - 54 (reserved for future DML privileges)
//   53 - create (C)
//   52 - alter (A)
//   51 - drop (D)
//   50 and below are reserved for future privileges
//
// If the privilege is not applicable or the user is not granted the privilege 
// then '-' is displayed in the corresponding bit position.
// ----------------------------------------------------------------------------
static void getBitExtractsForObjectType(
  const NAString objectLit,
  NAString &bitExtracts)
{
  // Base tables are not granted usage, execute, or create. 
  if (objectLit == COM_BASE_TABLE_OBJECT_LIT)
    {
      // bits for select (63), insert (62), delete (61), update (60)
      bitExtracts = 
        " case when bitextract(privileges_bitmap,63,1) = 1 then 'S' else '-' end || "
        " case when bitextract(privileges_bitmap,62,1) = 1 then 'I' else '-' end || "
        " case when bitextract(privileges_bitmap,61,1) = 1 then 'D' else '-' end || "
        " case when bitextract(privileges_bitmap,60,1) = 1 then 'U' else '-' end || ";
      // no bit for usage (59)
      bitExtracts += "'-' || ";
      // bit for references (58)
      bitExtracts += 
        " case when bitextract(privileges_bitmap,58,1) = 1 then 'R' else '-' end || ";
      // no bits for execute (57) and create (53) 
      bitExtracts += "'--' || ";
      // bits for alter (52), drop (51)
      bitExtracts += 
        " case when bitextract(privileges_bitmap,52,1) = 1 then 'A' else '-' end ||  "
        " case when bitextract(privileges_bitmap,51,1) = 1 then 'D'else '-' end ";
    }

  // Views are not granted delete, usage, execute, or create.
  else if (objectLit == COM_VIEW_OBJECT_LIT)
    {
      //bits for select (63), insert (62)
      bitExtracts = 
        " case when bitextract(privileges_bitmap,63,1) = 1 then 'S' else '-' end || "
        " case when bitextract(privileges_bitmap,62,1) = 1 then 'I' else '-' end || ";
      // no bit for delete (61)
      bitExtracts += "'-' || ";
      // bit for update (60)
      bitExtracts +=
        " case when bitextract(privileges_bitmap,60,1) = 1 then 'U' else '-' end || ";
      // no bit for usage (59)
      bitExtracts += "'-' || ";
      // bit for references (58)
      bitExtracts += 
        " case when bitextract(privileges_bitmap,58,1) = 1 then 'R' else '-' end || ";
      // no bits for execute (57) and create (53)
      bitExtracts += "'--' || ";
      // bits for alter (52) and drop (51)
      bitExtracts += 
        " case when bitextract(privileges_bitmap,52,1) = 1 then 'A' else '-' end ||  "
        " case when bitextract(privileges_bitmap,51,1) = 1 then 'D'else '-' end ";
    }
   
  // Sequence generators are granted usage, alter, and drop
  else if (objectLit == COM_SEQUENCE_GENERATOR_OBJECT_LIT)
    {
      // no bits for select (63), insert (62), delete (61), update (60)
      bitExtracts = "'----' || "; 
      // bit for usage (59)
      bitExtracts +=
        " case when bitextract(privileges_bitmap,59,1) = 1 then 'G' else '-' end || ";
      // no bits for references (58), execute (57), create (53)
      bitExtracts += "'---' || ";
      // bits for alter (52) and drop (51)
      bitExtracts += 
        " case when bitextract(privileges_bitmap,52,1) = 1 then 'A' else '-' end ||  "
        " case when bitextract(privileges_bitmap,51,1) = 1 then 'D'else '-' end ";
    }
  
  // Libraries are granted update, usage, alter, and drop
  else if (objectLit == COM_LIBRARY_OBJECT_LIT)
    {
      // no bits for select (63), insert (62), delete (61)
      bitExtracts = "'---' || "; 
      // bits for update (60) and usage (59)
      bitExtracts += 
        " case when bitextract(privileges_bitmap,60,1) = 1 then 'U' else '-' end || "
        " case when bitextract(privileges_bitmap,59,1) = 1 then 'G' else '-' end || ";
      // no bits for references (58), execute (57), create (53)
      bitExtracts += "'---' || "; 
      // bits for alter (52) and drop (51)
      bitExtracts += 
        " case when bitextract(privileges_bitmap,52,1) = 1 then 'A' else '-' end ||  "
        " case when bitextract(privileges_bitmap,51,1) = 1 then 'D'else '-' end ";
    }

  // Routines are granted execute, alter, and drop
  else /*COM_USER_DEFINED_ROUTINE_OBJECT_LIT*/
    {
      // no bits for select (63), insert (62), delete (61), update (60), usage (59), references (58)
      bitExtracts = "'------' || "; 
      // bit for execute(57)
      bitExtracts += 
        " case when bitextract(privileges_bitmap,57,1) = 1 then 'E' else '-' end || ";
      // no bit for create (53)
      bitExtracts += "'-' || ";
      // bits for alter (52) and drop (51)
      bitExtracts += 
        " case when bitextract(privileges_bitmap,52,1) = 1 then 'A' else '-' end ||  "
        " case when bitextract(privileges_bitmap,51,1) = 1 then 'D'else '-' end ";
    }
}

static const QueryString getUsersForRoleQuery[] =
{
  {" select translate(rtrim(RU.grantee_name) using ucs2toutf8) "},
  {"   from %s.\"%s\".%s RU "},
  {" where (RU.grantor_ID != -2) and "},
  {"       (RU.role_name = '%s') %s "},
  {" order by 1"},
  {" ; "}
};


static const QueryString getRolesForUserQuery[] =
{
  {" select translate(rtrim(RU.role_name) using ucs2toutf8) "},
  {"   from %s.\"%s\".%s RU "},
  {" where (RU.grantor_ID != -2) and "},
  {"       (RU.grantee_name='%s' %s) "},
  {" union select * from (values ('PUBLIC')) "},
  {" order by 1 "},
  {" ; "}
};

static const QueryString getPrivsForAuthsQuery[] =
{
  {" select distinct translate(rtrim(object_name) using ucs2toutf8), "},
  {"    case when bitextract(privileges_bitmap,63,1) = 1 then 'S' "},
  {"      else '-' end || "},
  {"     case when bitextract(privileges_bitmap,62,1) = 1 then 'I' "},
  {"      else '-' end || "},
  {"     case when bitextract(privileges_bitmap,61,1) = 1 then 'D' "},
  {"      else '-' end || "},
  {"     case when bitextract(privileges_bitmap,60,1) = 1 then 'U' "},
  {"      else '-' end || "},
  {"     case when bitextract(privileges_bitmap,59,1) = 1 then 'G' "},
  {"      else '-' end || "},
  {"     case when bitextract(privileges_bitmap,58,1) = 1 then 'R' "},
  {"      else '-' end || "},
  {"     case when bitextract(privileges_bitmap,57,1) = 1 then 'E' "},
  {"      else '-' end || "},
  {"     case when bitextract(privileges_bitmap,53,1) = 1 then 'C' "},
  {"        else '-' end ||  "},
  {"     case when bitextract(privileges_bitmap,52,1) = 1 then 'A' "},
  {"        else '-' end ||  "},
  {"     case when bitextract(privileges_bitmap,51,1) = 1 then 'D' "},
  {"       else '-' end as privs "},
  {" from %s.\"%s\".%s %s "},
  {" union "},
  {"  (select translate(rtrim(schema_name) using ucs2toutf8), "},
  {"    case when bitextract(privileges_bitmap,63,1) = 1 then 'S' "},
  {"      else '-' end || "},
  {"     case when bitextract(privileges_bitmap,62,1) = 1 then 'I' "},
  {"      else '-' end || "},
  {"     case when bitextract(privileges_bitmap,61,1) = 1 then 'D' "},
  {"      else '-' end || "},
  {"     case when bitextract(privileges_bitmap,60,1) = 1 then 'U' "},
  {"      else '-' end || "},
  {"     case when bitextract(privileges_bitmap,59,1) = 1 then 'G' "},
  {"      else '-' end || "},
  {"     case when bitextract(privileges_bitmap,58,1) = 1 then 'R' "},
  {"      else '-' end || "},
  {"     case when bitextract(privileges_bitmap,57,1) = 1 then 'E' "},
  {"      else '-' end || "},
  {"     case when bitextract(privileges_bitmap,53,1) = 1 then 'C' "},
  {"        else '-' end ||  "},
  {"     case when bitextract(privileges_bitmap,52,1) = 1 then 'A' "},
  {"        else '-' end ||  "},
  {"     case when bitextract(privileges_bitmap,51,1) = 1 then 'D' "},
  {"       else '-' end as privs "},
  {"   from %s.\"%s\".%s %s "},
  {" %s ) order by 1 " },
  {" ; "}
};

static const QueryString getPrivsForColsQuery[] =
{ 
  {" union "}, // for column privileges
  {"  (select translate(rtrim(object_name) using ucs2toutf8) || ' <Column> ' || "},
  {"          translate(rtrim(column_name) using ucs2toutf8), "},
  {"    case when bitextract(privileges_bitmap,63,1) = 1 then 'S' "},
  {"      else '-' end || "},
  {"     case when bitextract(privileges_bitmap,62,1) = 1 then 'I' "},
  {"      else '-' end || "},
  {"     case when bitextract(privileges_bitmap,61,1) = 1 then 'D' "},
  {"      else '-' end || "},
  {"     case when bitextract(privileges_bitmap,60,1) = 1 then 'U' "},
  {"      else '-' end || "},
  {"     case when bitextract(privileges_bitmap,59,1) = 1 then 'G' "},
  {"      else '-' end || "},
  {"     case when bitextract(privileges_bitmap,58,1) = 1 then 'R' "},
  {"      else '-' end || "},
  {"     case when bitextract(privileges_bitmap,57,1) = 1 then 'E' "},
  {"      else '-' end || "},
  {"     case when bitextract(privileges_bitmap,53,1) = 1 then 'C' "},
  {"        else '-' end ||  "},
  {"     case when bitextract(privileges_bitmap,52,1) = 1 then 'A' "},
  {"        else '-' end ||  "},
  {"     case when bitextract(privileges_bitmap,51,1) = 1 then 'D' "},
  {"       else '-' end as privs "},
  {"   from %s.\"%s\".%s p, %s.\"%s\".%s c "},
  {"   where p.object_uid = c.object_uid "},
  {"     and p.column_number = c.column_number "},
  {"     and grantee_id %s ) "},
};

static const QueryString getPrivsForHiveColsQuery[] =
{ 
  {" union "}, // for privileges on hive objects
  {"  (select translate(rtrim(o.catalog_name) using ucs2toutf8) || '.' || "},
  {"          translate(rtrim(o.schema_name) using ucs2toutf8) || '.'  || "},
  {"          translate(rtrim(o.object_name) using ucs2toutf8) || '.'  || "},
  {"                        ' <Column> ' ||                    "},
  {"          translate(rtrim(column_name) using ucs2toutf8), "},
  {"    case when bitextract(privileges_bitmap,63,1) = 1 then 'S' "},
  {"      else '-' end || "},
  {"     case when bitextract(privileges_bitmap,62,1) = 1 then 'I' "},
  {"      else '-' end || "},
  {"     case when bitextract(privileges_bitmap,61,1) = 1 then 'D' "},
  {"      else '-' end || "},
  {"     case when bitextract(privileges_bitmap,60,1) = 1 then 'U' "},
  {"      else '-' end || "},
  {"     case when bitextract(privileges_bitmap,59,1) = 1 then 'G' "},
  {"      else '-' end || "},
  {"     case when bitextract(privileges_bitmap,58,1) = 1 then 'R' "},
  {"      else '-' end || "},
  {"     case when bitextract(privileges_bitmap,57,1) = 1 then 'E' "},
  {"      else '-' end || "},
  {"     case when bitextract(privileges_bitmap,53,1) = 1 then 'C' "},
  {"        else '-' end ||  "},
  {"     case when bitextract(privileges_bitmap,52,1) = 1 then 'A' "},
  {"        else '-' end ||  "},
  {"     case when bitextract(privileges_bitmap,51,1) = 1 then 'D' "},
  {"       else '-' end as privs "},
  {"   from %s.\"%s\".%s p, %s.\"%s\".%s o,                        "},
  {"        table(hivemd(columns)) c                               "},
  {"   where p.object_uid = o.object_uid "},
  {"         and o.catalog_name = upper(c.catalog_name) "},
  {"         and o.schema_name = upper(c.schema_name) "},
  {"         and o.object_name = upper(c.table_name) "},
  {"     and p.column_number = c.column_number "},
  {"     and grantee_id %s ) "},
};

static const QueryString getComponents[] =
{
  {" select distinct translate(rtrim(component_name) using ucs2toutf8)  "},
  {"   from %s.\"%s\".%s c, %s.\"%s\".%s p %s"},
  {" order by 1 "},
  {" ; "}
};

static const QueryString getComponentPrivileges[] = 
{
  {" select distinct translate(rtrim(operation_name) using ucs2toutf8) "},
  {" from %s.\"%s\".%s c, %s.\"%s\".%s o, "},
  {"      %s.\"%s\".%s p "},
  {" where (c.component_uid=o.component_uid) "},
  {"   and (o.component_uid=p.component_uid) "},
  {"   and (o.operation_code=p.operation_code) "},
  {"   and (o.is_system <> 'U') "},
  {"   and (c.component_name='%s') %s "},
  {" order by 1 "},
  {" ; "}
};

static const QueryString getCatalogsQuery[] =
{
  {" select * from (values ('TRAFODION'), ('HIVE')) "},
  {" order by 1 desc "},
  {" ; "}
};

static const QueryString getTrafTablesInSchemaQuery[] =
{
  {" select %sobject_name%s from "},
  {"   %s.\"%s\".%s O %s"},
  {"  where catalog_name = '%s' and "},
  {"        schema_name = '%s'  and "},
  {"        object_type = 'BT' and "},
  {"  bitand(flags, 8192) = 0 %s "},
  {"  order by 1 "},
  {"  ; "}
};

static const QueryString getTrafTablesInSchemaWithNamespaceQuery[] =
{
  {" select distinct trim(case when T.text is not null then trim(T.text) || ':' "},
  {"             else '' end || %sO.object_name%s)  from "},
  {"   %s.\"%s\".%s O left join %s.\"%s\".%s T "},
  {"  on O.object_uid = T.text_uid and T.text_type = %s %s "},
  {"  where O.catalog_name = '%s' and "},
  {"        O.schema_name = '%s'  and "},
  {"        O.object_type = 'BT' %s  "},
  {"  order by 1 "},
  {"  ; "}
};

static const QueryString getTrafIndexesInSchemaQuery[] =
{
  {" select distinct %sO.object_name%s from "},
  {"  %s.\"%s\".%s I, "},
  {"   %s.\"%s\".%s O %s "},
  {"  where O.catalog_name = '%s' and "},
  {"        O.schema_name = '%s'  and "},
  {"        O.object_type = 'IX' and "},
  {"        bitand(I.flags, 4) = 0 and "},
  {"        O.object_uid = I.index_uid %s "},
  {"  order by 1 "},
  {"  ; "}
};

static const QueryString getTrafIndexesInSchemaWithNamespaceQuery[] =
{
  {" select distinct trim(case when T.text is not null then trim(T.text) || ':' "},
  {"             else '' end || %sO.object_name%s)  from "},
  {"  %s.\"%s\".%s I, "},
  {"   %s.\"%s\".%s O left join %s.\"%s\".%s T "},
  {"  on O.object_uid = T.text_uid and T.text_type = %s %s"},
  {"  where O.catalog_name = '%s' and "},
  {"        O.schema_name = '%s'  and "},
  {"        O.object_type = 'IX' and "},
  {"        O.object_uid = I.index_uid %s "},
  {"  order by 1 "},
  {"  ; "}
};

static const QueryString getTrafIndexesOnTableQuery[] =
{
  {" select distinct trim(case when T.text is not null then trim(T.text) || ':' "},
  {"             else '' end || %sO2.object_name%s)  from "},
  {"   %s.\"%s\".%s O left join  "},
  {"   %s.\"%s\".%s T   "},
  {"    on O.object_uid = T.text_uid and T.text_type = %s "},
  {"  join %s.\"%s\".%s I  "},
  {"    on I.base_table_uid = O.object_uid "},
  {"  join %s.\"%s\".%s O2 "},
  {"   on I.index_uid = O2.object_uid %s "},
  {"  where O.catalog_name = '%s' "},
  {"    and O.schema_name = '%s' "},
  {"    and O.object_name = '%s' %s "},
  {" order by 1 "},
  {" ; "}
};

static const QueryString getTrafIndexesForAuth[] =
{
  {" select distinct trim(T2.catalog_name) || '.\"' || trim(T2.schema_name) || '\".' || trim(T2.object_name) "},
  {" from %s.\"%s\".%s I, "},
  {"      %s.\"%s\".%s T, "},
  {"      %s.\"%s\".%s T2 %s "},
  {"  where T.catalog_name = '%s' "},
  {"    and I.base_table_uid = T.object_uid "},
  {"    and I.index_uid = T2.object_uid %s "},
  {" order by 1 "},
  {" ; "}
};

static const QueryString getTrafPackagesInSchemaQuery[] =
{
  {" select distinct object_name  from "},
  {"   %s.\"%s\".%s T %s "},
  {"  where T.catalog_name = '%s' and "},
  {"        T.schema_name = '%s'  and "},
  {"        object_type = 'PA' %s "},
  {"  order by 1 "},
  {"  ; "}
};

static const QueryString getTrafProceduresInSchemaQuery[] =
{
  {" select distinct T.object_name  from "},
  {"   %s.\"%s\".%s T, %s.\"%s\".%s R %s"},
  {"  where T.catalog_name = '%s' and "},
  {"        T.schema_name = '%s'  and "},
  {"        T.object_type = 'UR'  and "},
  {"        T.object_uid = R.udr_uid  and "},
  {"        R.udr_type = 'P ' %s "},
  {"  order by 1 "},
  {"  ; "}
};

static const QueryString getTrafTriggersInSchemaQuery[] =
  {
   {" select object_name  from "},
   {"   %s.\"%s\".%s T, %s.\"%s\".%s R "},
   {"  where T.catalog_name = '%s' and "},
   {"        T.schema_name = '%s'  and "},
   {"        T.object_type = 'TR'  and "},
   {"        T.object_uid = R.udr_uid  and "},
   {"        R.udr_type = 'TR' %s "},
   {"  order by 1 "},
   {"  ; "}
  };

static const QueryString getTrafLibrariesInSchemaQuery[] =
{
  {" select distinct T.object_name  from "},
  {" %s.\"%s\".%s T "},
  {"  where T.catalog_name = '%s' "},
  {"    and T.schema_name = '%s' "},
  {"    and T.object_type = 'LB' %s "},
  {"  order by 1 "},
  {"  ; "}
};

static const QueryString getTrafLibrariesForAuthQuery[] =
{
  {" select distinct trim(T.catalog_name) || '.\"' || "},
  {"     trim(T.schema_name) || '\".' || trim(T.object_name) "},
  {"  from %s.\"%s\".%s T "},
  {"  where T.catalog_name = '%s' and T.object_type = 'LB' %s "},
  {"  order by 1 "},
  {"  ; "}
};

static const QueryString getTrafRoutinesForAuthQuery[] =
{
  {" select distinct trim(T.catalog_name) || '.\"' || "},
  {"     trim(T.schema_name) || '\".' || trim(T.object_name) "},
  {"  from %s.\"%s\".%s T, %s.\"%s\".%s R %s  "},
  {"  where T.catalog_name = '%s' and "},
  {"        T.object_type = 'UR' and "},
  {"        T.object_uid = R.udr_uid and "},
  {"        R.udr_type = '%s' %s "},
  {"  order by 1 "},
  {"  ; "}
};

static const QueryString getTrafFunctionsInSchemaQuery[] =
{
  {" select distinct T.object_name  from "},
  {"   %s.\"%s\".%s T, %s.\"%s\".%s R %s "},
  {"  where T.catalog_name = '%s' and "},
  {"        T.schema_name = '%s'  and "},
  {"        T.object_type = 'UR'  and "},
  {"        T.object_uid = R.udr_uid  and "},
  {"        R.udr_type = 'F ' %s "},
  {"  order by 1 "},
  {"  ; "}
};

static const QueryString getTrafTableFunctionsInSchemaQuery[] =
{
  {" select distinct T.object_name  from "},
  {"   %s.\"%s\".%s T, %s.\"%s\".%s R %s "},
  {"  where T.catalog_name = '%s' and "},
  {"        T.schema_name = '%s'  and "},
  {"        T.object_type = 'UR'  and "},
  {"        T.object_uid = R.udr_uid  and "},
  {"        R.udr_type = 'T ' %s "},
  {"  order by 1 "},
  {"  ; "}
};

static const QueryString getTrafProceduresForLibraryQuery[] =
{
  {" select distinct T1.schema_name || '.' || T1.object_name  from "},
  {"   %s.\"%s\".%s T, %s.\"%s\".%s R, %s.\"%s\".%s T1 %s "},
  {"where T.catalog_name = '%s' and T.schema_name = '%s' "},
  {"  and T.object_name = '%s' and T.object_type = 'LB' "},
  {"  and T.object_uid = R.library_uid and R.udr_uid = T1.object_uid "},
  {"  and %s %s "},
  {"order by 1 "},
  {"  ; "}
};

static const QueryString getTrafSequencesInSchemaQuery[] =
{
  {" select distinct T.object_name  from "},
  {"   %s.\"%s\".%s T %s "},
  {"  where T.catalog_name = '%s' and "},
  {"        T.schema_name = '%s'  and "},
  {"        object_type = 'SG' %s "},
  {"  order by 1 "},
  {"  ; "}
};

static const QueryString getTrafSequencesInCatalogQuery[] =
{
  {" select distinct trim(T.schema_name) || '.' || T.object_name  from "},
  {"   %s.\"%s\".%s T %s "},
  {"  where T.catalog_name = '%s' and "},
  {"        object_type = 'SG' %s "},
  {"  order by 1"},
  {"  ; "}
};

static const QueryString getTrafViewsInCatalogQuery[] =
{
  {" select distinct T.schema_name || '.' || "},
  {" T.object_name from "},
  {"   %s.\"%s\".%s T %s"},
  {"  where object_type = 'VI' and "},
  {"            catalog_name = '%s' %s "},
  {" order by 1 "},
  {"  ; "}
};

static const QueryString getTrafViewsInSchemaQuery[] =
{
  {" select distinct T.object_name from "},
  {"   %s.\"%s\".%s T %s"},
  {"  where T.catalog_name = '%s' and "},
  {"        T.schema_name = '%s' and "},
  {"        T.object_type = 'VI' %s "},
  {" order by 1 "},
  {"  ; "}
};


static const QueryString getTrafObjectsInViewQuery[] =
{
  {" select distinct trim(T.catalog_name),trim(T.schema_name),trim(T.object_name), "},
  {" trim(T.object_type) "},
  {"   from %s.\"%s\".%s VU,  %s.\"%s\".%s T "},
  {"  where VU.using_view_uid = "},
  {"     (select T2.object_uid from  %s.\"%s\".%s T2 %s"},
  {"         where T2.catalog_name = '%s' and "},
  {"                   T2.schema_name = '%s' and "},
  {"                   T2.object_name = '%s' %s ) "},
  {"     and VU.used_object_uid = T.object_uid "},
  {" order by 1,2,3,4 "},
  {"  ; "}
};

static const QueryString getTrafViewsOnObjectQuery[] =
{
  {" select distinct trim(T.catalog_name),trim(T.schema_name),trim(T.object_name) "},
  {"   from %s.\"%s\".%s T "},
  {"   where T.object_uid in "},
  {"   (select using_view_uid from  %s.\"%s\".%s VU "},
  {"    where used_object_uid in  "},
  {"     (select T1.object_uid from  "},
  {"        %s.\"%s\".%s T1 %s"},
  {"      where T1.catalog_name = '%s' "},
  {"          and T1.schema_name = '%s' "},
  {"          and T1.object_name = '%s' "},
  {"          %s %s "},
  {"     ) "},
  {"   ) "},
  {" order by 1,2,3 "},
  {" ; "}
};

static const QueryString getTrafSchemasInCatalogQuery[] =
{
  {" select distinct schema_name "},
  {"   from %s.\"%s\".%s T %s "},
  {"  where T.catalog_name = '%s' %s "},
  {" order by 1 "},
  {"  ; "}
};

static const QueryString getTrafSchemasForAuthIDQuery[] =
{
  {" select distinct T.catalog_name, T.schema_name "},
  {"   from %s.\"%s\".%s T %s "},
  {"   where catalog_name in ('HIVE', 'TRAFODION', 'HBASE') %s "},
  {" order by 1, 2 "},
  {"  ; "}
};

static const QueryString getTrafUsers[] = 
{
  {" select distinct auth_db_name,auth_ext_name "},
  {"   from %s.\"%s\".%s "},
  {"  where auth_type = 'U' %s "},
  {" order by 1 "},
  {"  ; "}
};

static const QueryString getTrafGroups[] =
{
  {" select distinct auth_db_name "},
  {"   from %s.\"%s\".%s "},
  {"  where auth_type = 'G' %s "},
  {" order by 1 "},
  {"  ; "}
};

static const QueryString getTrafGroupTenantUsage[] =
{
  {" select distinct translate(rtrim(auth_db_name) using ucs2toutf8) "},
  {"   from %s.\"%s\".%s, %s.\"%s\".%s "},
  {"  where usage_type = 'G'  and tenant_id = "},
  {"    (select auth_id from %s.\"%s\".%s where auth_db_name = '%s' %s) "},
  {"    and usage_uid = auth_id "},
  {"  ; "}
};

static const QueryString getTrafRoles[] = 
{
  {" select distinct auth_db_name "},
  {"   from %s.\"%s\".%s "},
  {"  where auth_type = 'R' %s "},
  {" union select * from (values ('PUBLIC')) "},
  {" order by 1 "},
  {"  ; "}
};

static const QueryString getTrafActiveRoles[] = 
{
  {" select roles.aname, grantees.aname from "},
  {" (select auth_id, auth_db_name from %s.\"%s\".%s) roles(id, aname), "},
  {" (select auth_id, auth_db_name from %s.\"%s\".%s) grantees(id, aname), "},
  {" (values %s) ids(roleid, granteeid) "},
  {" where ids.roleid = roles.id and ids.granteeid = grantees.id union"},
  {" (select pub.x, pub.y from  (values ('PUBLIC', '*')) as pub(x, y)) "},
  {"  order by 1; "}
};
  
static const QueryString getTrafResourceUsages1[] =
{
  {" select distinct resource_name "},
  {"   from %s.\"%s\".%s "},
  {" where usage_type = 'T' and usage_name = '%s' %s "}, 
  {" order by 1 "},
  {"  ; "}
};

static const QueryString getTrafResourceUsages2[] =
{
  {" select distinct usage_name "},
  {"   from %s.\"%s\".%s "},
  {" where usage_type = 'N' and resource_name = '%s' %s "},
  {" order by 1 "},
  {"  ; "}
};

static const QueryString getTrafResources[] =
{
  {" select distinct resource_name "},
  {"   from %s.\"%s\".%s "},
  {" where resource_type = '%s' %s"},
  {" order by 1 "},
  {"  ; "}
};

static const QueryString getTrafTenantUsages[] =
{
  {" select distinct auth_db_name "},
  {" from %s.\"%s\".%s where auth_id in "},
  {" (select distinct tenant_id from %s.\"%s\".%s where usage_uid = "},
  {" (select resource_uid from %s.\"%s\".%s where resource_name = '%s' )) "}, 
  {"  %s order by 1 "},
  {"  ; "}
};

static const QueryString getTrafTenants[] =
{
  {" select distinct auth_db_name "},
  {"   from %s.\"%s\".%s "},
  {"  where auth_type = 'T' %s "},
  {" order by 1 "},
  {"  ; "}
};

static const QueryString getTrafPrivsOnObject[] = 
{
  {" select distinct x.grantee, x.privs from ("}, 
  {"  select '(object-grant) ' || grantee_name, %s from %s.\"%s\".%s "},
  //{"  select grantee_name, %s from %s.\"%s\".%s "},
  {"  where object_uid = %s %s"},
  {" union ("},
  {"  select '(column-grant) ' || grantee_name, %s from %s.\"%s\".%s "},
  //{"  select grantee_name, %s from %s.\"%s\".%s "},
  {"  where object_uid = %s %s) "},
  {" union ("},
  {"   select '(schema-grant) ' || grantee_name, %s from %s.\"%s\".%s "},
  //{"   select grantee_name, %s from %s.\"%s\".%s "},
  {"   where schema_uid = %s %s )) x(grantee,privs)"},
  {" where x.privs <> '----------' "},
  {" order by 1 "},
  {" ; "}
};

static const QueryString getTrafObjectsPlusTypeForUser[] = 
{
  {" select distinct trim(T.catalog_name), trim(T.schema_name), trim(T.object_name), "},
  {"        object_type "},
  {" from %s.\"%s\".%s T %s "},
  {" where T.catalog_name in ('TRAFODION', 'HIVE') "},
  {" and T.object_type in %s %s "},
  {" order by 4,1,2,3  "},
  {" ; "}
};

static const QueryString getTrafObjectsForUser[] = 
{
  {" select distinct trim(T.catalog_name), trim(T.schema_name), trim(T.object_name) "},
  {" from %s.\"%s\".%s T %s"},
  {" where T.catalog_name in ('TRAFODION', 'HIVE') "},
  {" and T.object_type in %s %s "},
  {" order by 1,2,3  "},
  {" ; "}
};

static const QueryString getHiveRegObjectsInCatalogQuery[] =
{
  {" select trim(O.a) ||  "                                    },
  {" case when G.b is null and O.t != 'SS' then ' (inconsistent)' else '' end "},
  {" from "                                                    },
  {"  (select object_type, case when object_type = 'SS' "      },
  {"   then lower(trim(catalog_name) || '.' || trim(schema_name)) "},
  {"   else lower(trim(catalog_name) || '.' || "               },
  {"    trim(schema_name) || '.' || trim(object_name)) end "   },
  {"   from %s.\"%s\".%s where catalog_name = 'HIVE' and "     },
  {"                           %s %s) O(t, a) "                },
  {"  left join "                                              },
  {"   (select '%s' || '.' || trim(y) from "                   },
  {"    (get %s in catalog %s, no header) x(y)) G(b)"          },
  {"   on O.a = G.b  "                                         },
  {" order by 1 "                                              },
  {"; "                                                        }
};

static const QueryString getHBaseRegTablesInCatalogQuery[] =
{
  {" select '\"' || trim(O.s) || '\"' || '.' || trim(O.o) ||  "},
  {" case when G.b is null then ' (inconsistent)' else '' end "},
  {" from "                                                    },
  {"  (select trim(schema_name), trim(object_name)            "},
  {"   from %s.\"%s\".%s where catalog_name = 'HBASE'     "    },
  {"      and object_type = 'BT' %s) O(s, o) "                 },
  {"  left join "                                              },
  {"   (select trim(y) from "                                  },
  {"    (get external hbase objects) x(y)) G(b)"               },
  {"   on O.o = G.b  "                                         },
  {" group by 1 order by 1 "                                   },
  {"; "                                                        }
};

static const QueryString getHiveExtTablesInCatalogQuery[] =
{
  {" select trim(O.a) ||  "                                    },
  {" case when G.b is null then ' (inconsistent)' else '' end "},
  {" from "                                                    },
  {"  (select '%s' || '.' || "                                 },
  {"   lower(trim(substring(schema_name, 5, "                  },
  {"                char_length(schema_name)-5))) "            },
  {"    || '.' || lower(trim(object_name)) "                   },
  {"   from %s.\"%s\".%s where object_type = '%s' "            },
  {"    and schema_name like '|_HV|_%%|_' escape '|' %s) O(a)" },
  {"  left join "                                              },
  {"   (select '%s' || '.' || trim(y) from "                   },
  {"    (get %s in catalog %s, no header) x(y)) G(b) "         },
  {"   on O.a = G.b  "                                         },
  {" order by 1 "                                              },
  {"; "                                                        }
};

static const QueryString getHBaseMapTablesInCatalogQuery[] =
{
  {" select distinct trim(case when T.text is not null then trim(T.text) || ':' "},
  {"             else '' end || O.object_name)  from "},
  {"   %s.\"%s\".%s O left join %s.\"%s\".%s T "},
  {"  on O.object_uid = T.text_uid and T.text_type = 9 "},
  {"  where O.catalog_name = 'TRAFODION' and "},
  {"        O.schema_name = '_HB_MAP_'  and "},
  {"        O.object_type = 'BT'  "},
  {"  order by 1 "},
  {"  ; "}
};


Lng32 ExExeUtilGetMetadataInfoTcb::getUsingView(Queue * infoList,
					       NABoolean isShorthandView,
					       char* &viewName, Lng32 &len)
{
  Lng32 cliRC = 0;

  while (1)
    {
      switch (vStep_)
	{
	case VIEWS_INITIAL_:
	  {
	    infoList->position();

	    vStep_ = VIEWS_FETCH_PROLOGUE_;
	  }
	break;

	case VIEWS_FETCH_PROLOGUE_:
	  {
	    if (infoList->atEnd())
	      {
		vStep_ = VIEWS_DONE_;
		break;
	      }

	    OutputInfo * vi = (OutputInfo*)infoList->getCurr();
            QualifiedName qualTableName(vi->get(2), vi->get(1), vi->get(0));

	    char query[2000];
            str_sprintf(query, "get all views on view %s.\"%s\".\"%s\", no header;",
                        qualTableName.getCatalogName().data(),
                        qualTableName.getSchemaName().data(),
                        qualTableName.getObjectName().data());
	    cliRC = cliInterface()->fetchRowsPrologue(query);
	    if (cliRC < 0)
	      {
                cliInterface()->allocAndRetrieveSQLDiagnostics(diagsArea_);
		vStep_ = VIEWS_ERROR_;
		break;
	      }

	    vStep_ = VIEWS_FETCH_ROW_;
	  }
	break;

	case VIEWS_FETCH_ROW_:
	  {
	    cliRC = cliInterface()->fetch();
	    if (cliRC < 0)
	      {
                cliInterface()->allocAndRetrieveSQLDiagnostics(diagsArea_);
		vStep_ = VIEWS_ERROR_;
		break;
	      }

	    if (cliRC == 100)
	      {
		vStep_ = VIEWS_FETCH_EPILOGUE_;
		break;
	      }

	    cliInterface()->getPtrAndLen(1, viewName, len);
	    return 0;
	  }
	break;

	case VIEWS_ERROR_:
	  {
	    vStep_ = VIEWS_INITIAL_;
	    return cliRC;
	  }
	break;

	case VIEWS_FETCH_EPILOGUE_:
	  {
	    cliRC = cliInterface()->fetchRowsEpilogue(0);
	    if (cliRC < 0)
	      {
                cliInterface()->allocAndRetrieveSQLDiagnostics(diagsArea_);
		vStep_ = VIEWS_ERROR_;
		break;
	      }

	    infoList->advance();

	    vStep_ = VIEWS_FETCH_PROLOGUE_;
	  }
	break;

	case VIEWS_DONE_:
	  {
	    // all done
	    vStep_ = VIEWS_INITIAL_;
	    return 100;
	  }
	break;

	}
    }
}

Lng32 ExExeUtilGetMetadataInfoTcb::getUsedObjects(Queue * infoList,
						 NABoolean isShorthandView,
						 char* &inViewName, Lng32 &inLen)
{
  Lng32 cliRC = 0;

  while (1)
    {
      switch (vStep_)
	{
	case VIEWS_INITIAL_:
	  {
	    infoList->position();

	    vStep_ = VIEWS_FETCH_PROLOGUE_;
	  }
	break;

	case VIEWS_FETCH_PROLOGUE_:
	  {
	    if (infoList->atEnd())
	      {
		vStep_ = VIEWS_DONE_;
		break;
	      }

	    OutputInfo * vi = (OutputInfo*)infoList->getCurr();

        QualifiedName qualObjName(vi->get(2), vi->get(1), vi->get(0));
	    char * objTyp = vi->get(3);

	    if ((objTyp) && (strcmp(objTyp, "BT") == 0))
	      {
		infoList->advance();

		vStep_ = VIEWS_FETCH_PROLOGUE_;
		break;
	      }

	    char query[2000];
	    char objectStr[20];

	    if (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::TABLES_IN_VIEW_)
	      strcpy(objectStr, "tables");
	    else if (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::VIEWS_IN_VIEW_)
	      strcpy(objectStr, "views");
	    else if (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::OBJECTS_IN_VIEW_)
	      strcpy(objectStr, "objects");
	    //else if (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::TABLES_IN_MV_)
	    //  strcpy(objectStr, "tables");
	    //else if (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::MVS_IN_MV_)
	    //  strcpy(objectStr, "mvs");
	    //else if (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::OBJECTS_IN_MV_)
	    //  strcpy(objectStr, "objects");

	    char inStr[10];
	    if (isShorthandView)
	      strcpy(inStr, "view");
	    else
	      strcpy(inStr, "mv");

            str_sprintf(query, "get all %s in %s %s.%s.%s, no header",
                        objectStr, inStr, 
                        qualObjName.getCatalogName().data(),
                        qualObjName.getUnqualifiedSchemaNameAsAnsiString().data(),
                        qualObjName.getUnqualifiedObjectNameAsAnsiString().data());

	    if (getMItdb().getPattern())
	      {
		strcat(query, ", match '");
		strcat(query, getMItdb().getPattern());
		strcat(query, "'");
	      }

	    strcat(query, ";");

	    cliRC = cliInterface()->fetchRowsPrologue(query);
	    if (cliRC < 0)
	      {
                cliInterface()->allocAndRetrieveSQLDiagnostics(diagsArea_);
		vStep_ = VIEWS_ERROR_;
		break;
	      }

	    vStep_ = VIEWS_FETCH_ROW_;
	  }
	break;

	case VIEWS_FETCH_ROW_:
	  {
	    cliRC = cliInterface()->fetch();
	    if (cliRC < 0)
	      {
                cliInterface()->allocAndRetrieveSQLDiagnostics(diagsArea_);
		vStep_ = VIEWS_ERROR_;
		break;
	      }

	    if (cliRC == 100)
	      {
		vStep_ = VIEWS_FETCH_EPILOGUE_;
		break;
	      }

            NAString viewName;
            char * ptr = NULL;
            Lng32 len = 0;
            
            cliInterface()->getPtrAndLen(1, inViewName, inLen);

	    return 0;
	  }
	break;

	case VIEWS_ERROR_:
	  {
	    vStep_ = VIEWS_INITIAL_;
	    return cliRC;
	  }
	break;

	case VIEWS_FETCH_EPILOGUE_:
	  {
	    cliRC = cliInterface()->fetchRowsEpilogue(0);
	    if (cliRC < 0)
	      {
                cliInterface()->allocAndRetrieveSQLDiagnostics(diagsArea_);
		vStep_ = VIEWS_ERROR_;
		break;
	      }

	    infoList->advance();

	    vStep_ = VIEWS_FETCH_PROLOGUE_;
	  }
	break;

	case VIEWS_DONE_:
	  {
	    // all done
	    vStep_ = VIEWS_INITIAL_;
	    return 100;
	  }
	break;

	}
    }
}

short ExExeUtilGetMetadataInfoTcb::displayHeading()
{
  if (getMItdb().noHeader())
    {
      return 0;
    }
  // make sure there is enough space to move header
  if ((qparent_.up->getSize() - qparent_.up->getLength()) < 7)
    return 1;	//come back later

  switch (getMItdb().queryType_)
    {
    case ComTdbExeUtilGetMetadataInfo::USERS_FOR_MEMBERS_:
    {
        str_sprintf(headingBuf_, "Members of User Group %s",
                    getMItdb().getParam1());
    }
    break;
    case ComTdbExeUtilGetMetadataInfo::CATALOGS_:
      {
	str_sprintf(headingBuf_, "Catalogs");
      }
    break;

    case ComTdbExeUtilGetMetadataInfo::SCHEMAS_IN_CATALOG_:
      {
	str_sprintf(headingBuf_, "Schemas in Catalog %s",
		    getMItdb().getCat());
      }
    break;

    case ComTdbExeUtilGetMetadataInfo::INVALID_VIEWS_IN_CATALOG_:
      {
	str_sprintf(headingBuf_, "Invalid Views in Catalog %s",
		    getMItdb().getCat());
      }
    break;

    case ComTdbExeUtilGetMetadataInfo::TABLES_IN_CATALOG_:
      {
	str_sprintf(headingBuf_, "Tables in Catalog %s",
		    getMItdb().getCat());
      }
    break;

    case ComTdbExeUtilGetMetadataInfo::VIEWS_IN_CATALOG_:
      {
	str_sprintf(headingBuf_, "Views in Catalog %s",
		    getMItdb().getCat());
      }
    break;

    case ComTdbExeUtilGetMetadataInfo::OBJECTS_IN_CATALOG_:
    case ComTdbExeUtilGetMetadataInfo::OBJECTNAMES_IN_CATALOG_:
      {
	str_sprintf(headingBuf_, "Objects in Catalog %s",
		    getMItdb().getCat());
      }
    break;

    case ComTdbExeUtilGetMetadataInfo::HIVE_REG_TABLES_IN_CATALOG_:
      {
	str_sprintf(headingBuf_, "Hive Registered Tables in Catalog %s",
		    getMItdb().getCat());
      }
    break;

    case ComTdbExeUtilGetMetadataInfo::HBASE_REG_TABLES_IN_CATALOG_:
      {
	str_sprintf(headingBuf_, "HBase Registered Tables in Catalog %s",
		    getMItdb().getCat());
      }
    break;

    case ComTdbExeUtilGetMetadataInfo::HBASE_OBJECTS_:
      {
	str_sprintf(headingBuf_, "External HBase objects");
      }
    break;

    case ComTdbExeUtilGetMetadataInfo::HIVE_REG_VIEWS_IN_CATALOG_:
      {
	str_sprintf(headingBuf_, "Hive Registered Views in Catalog %s",
		    getMItdb().getCat());
      }
    break;

    case ComTdbExeUtilGetMetadataInfo::HIVE_REG_SCHEMAS_IN_CATALOG_:
      {
	str_sprintf(headingBuf_, "Hive Registered Schemas in Catalog %s",
		    getMItdb().getCat());
      }
    break;

    case ComTdbExeUtilGetMetadataInfo::HIVE_REG_OBJECTS_IN_CATALOG_:
      {
	str_sprintf(headingBuf_, "Hive Registered Objects in Catalog %s",
		    getMItdb().getCat());
      }
    break;

    case ComTdbExeUtilGetMetadataInfo::HIVE_EXT_TABLES_IN_CATALOG_:
      {
	str_sprintf(headingBuf_, "Hive External Tables in Catalog %s",
		    getMItdb().getCat());
      }
    break;

    case ComTdbExeUtilGetMetadataInfo::HBASE_MAP_TABLES_IN_CATALOG_:
      {
	str_sprintf(headingBuf_, "HBase Mapped Tables in Catalog %s",
		    getMItdb().getCat());
      }
    break;

    case ComTdbExeUtilGetMetadataInfo::TABLES_IN_SCHEMA_:
      {
	str_sprintf(headingBuf_, "Tables in Schema %s.%s",
		    getMItdb().getCat(), getMItdb().getSch());
      }
    break;

    case ComTdbExeUtilGetMetadataInfo::INDEXES_IN_SCHEMA_:
      {
	str_sprintf(headingBuf_, "Indexes in Schema %s.%s",
		    getMItdb().getCat(), getMItdb().getSch());
      }
    break;

    case ComTdbExeUtilGetMetadataInfo::VIEWS_IN_SCHEMA_:
      {
	str_sprintf(headingBuf_, "Views in Schema %s.%s",
		    getMItdb().getCat(), getMItdb().getSch());
      }
    break;

    case ComTdbExeUtilGetMetadataInfo::OBJECTS_IN_SCHEMA_:
    case ComTdbExeUtilGetMetadataInfo::OBJECTNAMES_IN_SCHEMA_:
      {
	str_sprintf(headingBuf_, "Objects in Schema %s.%s",
		    getMItdb().getCat(), getMItdb().getSch());
      }
    break;

    case ComTdbExeUtilGetMetadataInfo::INVALID_VIEWS_IN_SCHEMA_:
      {
	str_sprintf(headingBuf_, "Invalid Views in Schema %s.%s",
		    getMItdb().getCat(), getMItdb().getSch());
      }
    break;

    case ComTdbExeUtilGetMetadataInfo::LIBRARIES_IN_SCHEMA_:
      {
	str_sprintf(headingBuf_, "Libraries in Schema %s.%s",
		    getMItdb().getCat(), getMItdb().getSch());
      }
    break;

    case ComTdbExeUtilGetMetadataInfo::PACKAGES_IN_SCHEMA_:
      {
	str_sprintf(headingBuf_, "Packages in Schema %s.%s",
		    getMItdb().getCat(), getMItdb().getSch());
      }
    break;

    case ComTdbExeUtilGetMetadataInfo::PROCEDURES_IN_SCHEMA_:
      {
	str_sprintf(headingBuf_, "Procedures in Schema %s.%s",
		    getMItdb().getCat(), getMItdb().getSch());
      }
    break;

    case ComTdbExeUtilGetMetadataInfo::FUNCTIONS_IN_SCHEMA_:
      {
	str_sprintf(headingBuf_, "Functions in Schema %s.%s",
		    getMItdb().getCat(), getMItdb().getSch());
      }
    break;

    case ComTdbExeUtilGetMetadataInfo::TABLE_FUNCTIONS_IN_SCHEMA_:
      {
	str_sprintf(headingBuf_, "Table_mapping functions in Schema %s.%s",
		    getMItdb().getCat(), getMItdb().getSch());
      }
    break;

    case ComTdbExeUtilGetMetadataInfo::INDEXES_ON_TABLE_:
      {
	str_sprintf(headingBuf_, "Indexes on Table %s.%s",
		    getMItdb().getSch(), getMItdb().getObj());
      }
    break;

    case ComTdbExeUtilGetMetadataInfo::PRIVILEGES_ON_SCHEMA_:
      {
	str_sprintf(headingBuf_, "Privileges on Schema %s.%s",
		    getMItdb().getCat(), getMItdb().getSch());
      }
    break;

    case ComTdbExeUtilGetMetadataInfo::PRIVILEGES_ON_TABLE_:
      {
	str_sprintf(headingBuf_, "Privileges on Table %s.%s",
		    getMItdb().getSch(), getMItdb().getObj());
      }
    break;

    case ComTdbExeUtilGetMetadataInfo::PRIVILEGES_ON_VIEW_:
      {
	str_sprintf(headingBuf_, "Privileges on View %s.%s",
		    getMItdb().getSch(), getMItdb().getObj());
      }
    break;

    case ComTdbExeUtilGetMetadataInfo::PRIVILEGES_ON_SEQUENCE_:
      {
        str_sprintf(headingBuf_, "Privileges on Sequence %s.%s",
                    getMItdb().getSch(), getMItdb().getObj());
      }
    break;

    case ComTdbExeUtilGetMetadataInfo::PRIVILEGES_ON_LIBRARY_:
      {
        str_sprintf(headingBuf_, "Privileges on Library %s.%s",
                    getMItdb().getSch(), getMItdb().getObj());
      }
    break;

    case ComTdbExeUtilGetMetadataInfo::PRIVILEGES_ON_ROUTINE_:
      {
        str_sprintf(headingBuf_, "Privileges on Routine %s.%s",
                    getMItdb().getSch(), getMItdb().getObj());
      }
    break;

    case ComTdbExeUtilGetMetadataInfo::VIEWS_ON_TABLE_:
    case ComTdbExeUtilGetMetadataInfo::VIEWS_ON_VIEW_:
      {
	str_sprintf(headingBuf_,
		    (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::VIEWS_ON_TABLE_
		     ? "Views on Table %s.%s" : "Views ON View %s.%s"),
		    getMItdb().getSch(), getMItdb().getObj());
      }
    break;

    case ComTdbExeUtilGetMetadataInfo::PARTITIONS_FOR_TABLE_:
      {
	str_sprintf(headingBuf_, "Partitions for Table %s.%s",
		    getMItdb().getSch(), getMItdb().getObj());
      }
    break;

    case ComTdbExeUtilGetMetadataInfo::PARTITIONS_FOR_INDEX_:
      {
	str_sprintf(headingBuf_, "Partitions for Index %s.%s",
		    getMItdb().getSch(), getMItdb().getObj());
      }
    break;

    case ComTdbExeUtilGetMetadataInfo::OBJECTS_ON_TABLE_:
      {
	str_sprintf(headingBuf_, "Objects on Table %s.%s",
		    getMItdb().getSch(), getMItdb().getObj());
      }
    break;

    case ComTdbExeUtilGetMetadataInfo::SEQUENCES_IN_SCHEMA_:
      {
	str_sprintf(headingBuf_, "Sequences in schema %s.%s",
		    getMItdb().getCat(), getMItdb().getSch());
      }
    break;

    case ComTdbExeUtilGetMetadataInfo::SEQUENCES_IN_CATALOG_:
      {
	str_sprintf(headingBuf_, "Sequences in catalog %s",
		    getMItdb().getCat());
      }
    break;

    case ComTdbExeUtilGetMetadataInfo::TABLES_IN_VIEW_:
    case ComTdbExeUtilGetMetadataInfo::VIEWS_IN_VIEW_:
    case ComTdbExeUtilGetMetadataInfo::OBJECTS_IN_VIEW_:
      {
	str_sprintf(headingBuf_,
		    (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::TABLES_IN_VIEW_ ?
		     "Tables in View %s.%s" :
		     (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::VIEWS_IN_VIEW_ ?
		      "Views in View %s.%s" :
		      "Objects in View %s.%s")),
		    getMItdb().getSch(), getMItdb().getObj());
      }
    break;

    case ComTdbExeUtilGetMetadataInfo::ACTIVE_ROLES_:
        str_sprintf(headingBuf_,"Active Roles                            Role Grantees");
    break;

    case ComTdbExeUtilGetMetadataInfo::ROLES_:
        str_sprintf(headingBuf_,"Roles");
    break;

    case ComTdbExeUtilGetMetadataInfo::RGROUPS_:
        str_sprintf(headingBuf_,"Resource Groups");
    break;

    case ComTdbExeUtilGetMetadataInfo::NODES_:
        str_sprintf(headingBuf_,"Tenant Nodes");
    break;

    case ComTdbExeUtilGetMetadataInfo::NODES_FOR_TENANT_:
        str_sprintf(headingBuf_,"Nodes for Tenant %s", getMItdb().getParam1());
   break;

    case ComTdbExeUtilGetMetadataInfo::TENANTS_:
        str_sprintf(headingBuf_,"Tenants");
    break;

    case ComTdbExeUtilGetMetadataInfo::NODES_IN_RGROUP_:
        str_sprintf(headingBuf_,"Nodes in Resource Group %s", getMItdb().getParam1());
    break;

    case ComTdbExeUtilGetMetadataInfo::TENANTS_ON_RGROUP_:
        str_sprintf(headingBuf_,"Tenants on Resource Group %s ", getMItdb().getParam1());
    break;

    case ComTdbExeUtilGetMetadataInfo::GROUPS_:
        str_sprintf(headingBuf_,"User Groups");
    break;

    case ComTdbExeUtilGetMetadataInfo::GROUPS_FOR_USER_:
        str_sprintf(headingBuf_,"User Groups for User %s", getMItdb().getParam1());
    break;

    case ComTdbExeUtilGetMetadataInfo::GROUPS_FOR_TENANT_:
        str_sprintf(headingBuf_,"User Groups for Tenant %s", getMItdb().getParam1());
    break;

    case ComTdbExeUtilGetMetadataInfo::ROLES_FOR_ROLE_:
        str_sprintf(headingBuf_,"Roles granted Role %s",getMItdb().getParam1());
    break;

    case ComTdbExeUtilGetMetadataInfo::USERS_FOR_ROLE_:
        str_sprintf(headingBuf_,"Users granted Role %s",getMItdb().getParam1());
    break;

    case ComTdbExeUtilGetMetadataInfo::USERS_:
      {
        str_sprintf(headingBuf_,
                    (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::USERS_
                     ? "Users" : "Current User")
        );
      }
    break;

    case ComTdbExeUtilGetMetadataInfo::FUNCTIONS_FOR_USER_:
        str_sprintf(headingBuf_,"Functions for User %s",getMItdb().getParam1());
    break;

    case ComTdbExeUtilGetMetadataInfo::FUNCTIONS_FOR_ROLE_:
        str_sprintf(headingBuf_,"Functions for Role %s",getMItdb().getParam1());
    break;

    case ComTdbExeUtilGetMetadataInfo::INDEXES_FOR_USER_:
        str_sprintf(headingBuf_,"Indexes for User %s",getMItdb().getParam1());
    break;

    case ComTdbExeUtilGetMetadataInfo::INDEXES_FOR_ROLE_:
        str_sprintf(headingBuf_,"Indexes for Role %s",getMItdb().getParam1());
    break;

    case ComTdbExeUtilGetMetadataInfo::LIBRARIES_FOR_USER_:
        str_sprintf(headingBuf_,"Libraries for User %s", getMItdb().getParam1());
    break;

    case ComTdbExeUtilGetMetadataInfo::LIBRARIES_FOR_ROLE_:
        str_sprintf(headingBuf_,"Libraries for Role %s", getMItdb().getParam1());
    break;

    case ComTdbExeUtilGetMetadataInfo::OBJECTS_FOR_USER_:
        str_sprintf(headingBuf_,"Objects for User %s", getMItdb().getParam1());
    break;

    case ComTdbExeUtilGetMetadataInfo::PROCEDURES_FOR_LIBRARY_:
        str_sprintf(headingBuf_,"Procedures for Library %s.%s",getMItdb().getSch(), getMItdb().getObj());
    break;

    case ComTdbExeUtilGetMetadataInfo::FUNCTIONS_FOR_LIBRARY_:
        str_sprintf(headingBuf_,"Functions for Library %s.%s",getMItdb().getSch(), getMItdb().getObj());
    break;

    case ComTdbExeUtilGetMetadataInfo::TABLE_FUNCTIONS_FOR_LIBRARY_:
        str_sprintf(headingBuf_,"Table_mapping Functions for Library %s.%s",getMItdb().getSch(), getMItdb().getObj());
    break;

    case ComTdbExeUtilGetMetadataInfo::PACKAGES_FOR_USER_:
        str_sprintf(headingBuf_,"Packages for User %s",getMItdb().getParam1());
    break;

    case ComTdbExeUtilGetMetadataInfo::PRIVILEGES_FOR_USER_:
        str_sprintf(headingBuf_,"Privileges for User %s",getMItdb().getParam1());
    break;

    case ComTdbExeUtilGetMetadataInfo::PRIVILEGES_FOR_ROLE_:
        str_sprintf(headingBuf_,"Privileges for Role %s",getMItdb().getParam1());
    break;

    case ComTdbExeUtilGetMetadataInfo::PROCEDURES_FOR_USER_:
        str_sprintf(headingBuf_,"Procedures for User %s",getMItdb().getParam1());
    break;

    case ComTdbExeUtilGetMetadataInfo::PROCEDURES_FOR_ROLE_:
        str_sprintf(headingBuf_,"Procedures for Role %s",getMItdb().getParam1());
    break;

    case ComTdbExeUtilGetMetadataInfo::ROLES_FOR_USER_:
        str_sprintf(headingBuf_,"Roles for User %s",getMItdb().getParam1());
    break;

    case ComTdbExeUtilGetMetadataInfo::ROLES_FOR_GROUP_:
        str_sprintf(headingBuf_,"Roles for Group %s",getMItdb().getParam1());
    break;

    case ComTdbExeUtilGetMetadataInfo::SCHEMAS_FOR_ROLE_:
        str_sprintf(headingBuf_,"Schemas for Role %s",getMItdb().getParam1());
    break;

    case ComTdbExeUtilGetMetadataInfo::SCHEMAS_FOR_USER_:
        str_sprintf(headingBuf_,"Schemas for User %s",getMItdb().getParam1());
    break;

    case ComTdbExeUtilGetMetadataInfo::TABLE_FUNCTIONS_FOR_USER_:
        str_sprintf(headingBuf_,"Table mapping functions for User %s",getMItdb().getParam1());
    break;

    case ComTdbExeUtilGetMetadataInfo::TABLE_FUNCTIONS_FOR_ROLE_:
        str_sprintf(headingBuf_,"Table mapping functions for Role %s",getMItdb().getParam1());
    break;

    case ComTdbExeUtilGetMetadataInfo::TABLES_FOR_USER_:
        str_sprintf(headingBuf_,"Tables for User %s",getMItdb().getParam1());
    break;

    case ComTdbExeUtilGetMetadataInfo::TABLES_FOR_ROLE_:
        str_sprintf(headingBuf_,"Tables for Role %s",getMItdb().getParam1());
    break;

    case ComTdbExeUtilGetMetadataInfo::VIEWS_FOR_USER_:
        str_sprintf(headingBuf_,"Views for User %s",getMItdb().getParam1());
    break;

    case ComTdbExeUtilGetMetadataInfo::VIEWS_FOR_ROLE_:
        str_sprintf(headingBuf_,"Views for Role %s",getMItdb().getParam1());
    break;

    case ComTdbExeUtilGetMetadataInfo::COMPONENTS_:
        str_sprintf(headingBuf_, "Components");
    break;

    case ComTdbExeUtilGetMetadataInfo::COMPONENT_PRIVILEGES_:
       {	 
      	 if (getMItdb().getParam1())
            str_sprintf(headingBuf_, "Privilege information on Component %s for %s",
                        getMItdb().getObj(),getMItdb().getParam1());
         
         else
            str_sprintf(headingBuf_, "Operation information on Component %s",
                        getMItdb().getObj());
         break;
       }

    case ComTdbExeUtilGetMetadataInfo::TRIGGERS_IN_SCHEMA_:
      {
	str_sprintf(headingBuf_, "Triggers in Schema %s.%s",
		    getMItdb().getCat(), getMItdb().getSch());
      }
      break;

// Not supported at this time
#if 0
    case ComTdbExeUtilGetMetadataInfo::TRIGGERS_FOR_USER_:
        str_sprintf(headingBuf_,"Triggers for User %s",getMItdb().getParam1());
    break;

    case ComTdbExeUtilGetMetadataInfo::INDEXES_ON_MV_:
      {
	str_sprintf(headingBuf_, "Indexes on MV %s.%s",
		    getMItdb().getSch(), getMItdb().getObj());
      }
    break;

    case ComTdbExeUtilGetMetadataInfo::PRIVILEGES_ON_MV_:
      {
	str_sprintf(headingBuf_, "Privileges on MV %s.%s",
		    getMItdb().getSch(), getMItdb().getObj());
      }
    break;

     case ComTdbExeUtilGetMetadataInfo::IUDLOG_TABLE_ON_TABLE_:
      {
	str_sprintf(headingBuf_, "Iudlog tables  for Table %s.%s",
		    getMItdb().getSch(), getMItdb().getObj());
      }
    break;
    case ComTdbExeUtilGetMetadataInfo::RANGELOG_TABLE_ON_TABLE_:
      {
	str_sprintf(headingBuf_, "Rangelog table for Table %s.%s",
		    getMItdb().getSch(), getMItdb().getObj());
      }
    break;
     case ComTdbExeUtilGetMetadataInfo::TRIGTEMP_TABLE_ON_TABLE_:
      {
	str_sprintf(headingBuf_, "Trigger temp table for Table %s.%s",
		    getMItdb().getSch(), getMItdb().getObj());
      }
      break;
     case ComTdbExeUtilGetMetadataInfo::IUDLOG_TABLE_ON_MV_:
      {
	str_sprintf(headingBuf_, "Iudlog table  for MV %s.%s",
		    getMItdb().getSch(), getMItdb().getObj());
      }
    break;
    case ComTdbExeUtilGetMetadataInfo::RANGELOG_TABLE_ON_MV_:
      {
	str_sprintf(headingBuf_, "Rangelog table for MV %s.%s",
		    getMItdb().getSch(), getMItdb().getObj());
      }
    break;
     case ComTdbExeUtilGetMetadataInfo::TRIGTEMP_TABLE_ON_MV_:
      {
	str_sprintf(headingBuf_, "Trigger temp table for MV %s.%s",
		    getMItdb().getSch(), getMItdb().getObj());
      }
    break;
    case ComTdbExeUtilGetMetadataInfo::IUDLOG_TABLES_IN_SCHEMA_:
      {
	str_sprintf(headingBuf_, "Iud log tables in schema %s.%s",
		    getMItdb().getCat(), getMItdb().getSch());
      }
    break;
    case ComTdbExeUtilGetMetadataInfo::RANGELOG_TABLES_IN_SCHEMA_:
      {
	str_sprintf(headingBuf_, "Range log tables in schema %s.%s",
		    getMItdb().getCat(), getMItdb().getSch());
      }
    break;

    case ComTdbExeUtilGetMetadataInfo::TRIGTEMP_TABLES_IN_SCHEMA_:
      {
	str_sprintf(headingBuf_, "Trigger temp tables in schema %s.%s",
		    getMItdb().getCat(), getMItdb().getSch());
      }
    break;

    case ComTdbExeUtilGetMetadataInfo::SYNONYMS_IN_SCHEMA_:
      {
	str_sprintf(headingBuf_, "Synonyms in Schema %s.%s",
		    getMItdb().getCat(), getMItdb().getSch());
      }
    break;
    case ComTdbExeUtilGetMetadataInfo::SYNONYMS_FOR_USER_:
        str_sprintf(headingBuf_,"Synonyms for User %s",getMItdb().getParam1());
    break;

    case ComTdbExeUtilGetMetadataInfo::SYNONYMS_ON_TABLE_:
      {
	str_sprintf(headingBuf_, "Synonyms on Table %s.%s",
		    getMItdb().getSch(), getMItdb().getObj());
      }
    break;


    case ComTdbExeUtilGetMetadataInfo::MVS_IN_SCHEMA_:
      {
	str_sprintf(headingBuf_, "MVs in Schema %s.%s",
		    getMItdb().getCat(), getMItdb().getSch());
      }
    break;

    case ComTdbExeUtilGetMetadataInfo::MVS_ON_TABLE_:
    case ComTdbExeUtilGetMetadataInfo::MVS_ON_MV_:
      {
	str_sprintf(headingBuf_,
		    (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::MVS_ON_TABLE_
		     ? "MVs on Table %s.%s" : "MVs ON MV %s.%s"),
		    getMItdb().getSch(), getMItdb().getObj());
      }
    break;
    case ComTdbExeUtilGetMetadataInfo::TABLES_IN_MV_:
    case ComTdbExeUtilGetMetadataInfo::MVS_IN_MV_:
    case ComTdbExeUtilGetMetadataInfo::OBJECTS_IN_MV_:
      {
	str_sprintf(headingBuf_,
		    (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::TABLES_IN_MV_ ?
		     "Tables in MV %s.%s" :
		     (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::MVS_IN_MV_ ?
		      "MVs in MV %s.%s" :
		      "Objects in MV %s.%s")),
		    getMItdb().getSch(), getMItdb().getObj());
      }
    break;

    case ComTdbExeUtilGetMetadataInfo::MVS_FOR_USER_:
        str_sprintf(headingBuf_,"Materialized Views for User %s",getMItdb().getParam1());
    break;

    case ComTdbExeUtilGetMetadataInfo::MVGROUPS_FOR_USER_:
        str_sprintf(headingBuf_,"Materialized View Groups for User %s",getMItdb().getParam1());
    break;

#endif

    default:
      str_sprintf(headingBuf_, "Add to ExExeUtilGetMetadataInfoTcb::displayHeading");
      break;
    }

  moveRowToUpQueue(headingBuf_);
  str_pad(outputBuf_, strlen(headingBuf_), '=');
  outputBuf_[strlen(headingBuf_)] = 0;
  moveRowToUpQueue(outputBuf_);

  moveRowToUpQueue(" ");

  return 0;
} // ExExeUtilGetMetadataInfoTcb::displayHeading

// ----------------------------------------------------------------------------
// getAuthID
//
// Reads the "_MD_".auths table to get the auth_id from the passed in authName.
// If relationship not found for any reason, return 0, otherwise return
// the authID.
//
// TBD - should replace this with a call to currContext->getAuthIDFromName
//       this function checks for special authID and looks at cache before
//       calling metadata.  Currently there is an issue because privilege 
//       error are returned when trying to read AUTHS table.  Need to set 
//       parserflag 131072.
// ----------------------------------------------------------------------------
Int32 ExExeUtilGetMetadataInfoTcb::getAuthID(
  const char *authName,
  const char *catName,
  const char *schName, 
  const char *objName)
{
  if (strcmp(authName, PUBLIC_AUTH_NAME) == 0)
    return PUBLIC_USER;

  short rc      = 0;
  Lng32 cliRC   = 0;

  sprintf(queryBuf_, "select auth_id from %s.\"%s\".%s where auth_db_name = '%s' ",
          catName, schName, objName, authName);

  if (initializeInfoList(infoList_)) return NA_UserIdDefault;

  numOutputEntries_ = 1;
  cliRC = fetchAllRows(infoList_, queryBuf_, numOutputEntries_, FALSE, rc);
  if (cliRC < 0) 
  {
    cliInterface()->allocAndRetrieveSQLDiagnostics(diagsArea_);
    return NA_UserIdDefault;
  }

  infoList_->position();
  OutputInfo * vi = (OutputInfo*)infoList_->getCurr();
  if (vi)
    return *(Lng32*)vi->get(0);
  return NA_UserIdDefault;
}

// ----------------------------------------------------------------------------
// method: getCurrentUserRoles
//
// Creates a list of current user roles, and current user role's grantees
// It is returned in a format that can be used in an SQL IN clause.
// ----------------------------------------------------------------------------
Int32 ExExeUtilGetMetadataInfoTcb::getCurrentUserRoles(
  ContextCli * currContext,
  NAString &authList,
  NAString &granteeList)
{
  if (!CmpCommon::context()->isAuthorizationEnabled())
    return 0;

  // always include the current user in the list of auth IDs
  char authIDAsChar[sizeof(Int32)+10];
  str_sprintf(authIDAsChar, "(%d", *currContext->getDatabaseUserID());
  authList += authIDAsChar;

  // get roles from cache
  Int32 numRoles = 0;
  Int32 *cachedRoleIDs = NULL;
  Int32 *cachedGranteeIDs = NULL;
  currContext->getRoleList(numRoles, cachedRoleIDs, cachedGranteeIDs);
  if (numRoles == 0)
    return numRoles; 

  granteeList = ("(");

  // the number of entries in roleList is the same as granteeList
  for (Int32 i = 0; i < numRoles; i++)
  {
    authList += ", ";
    str_sprintf(authIDAsChar, "%d", cachedRoleIDs[i]);
    authList += authIDAsChar;
    if (i > 0)
      granteeList += ", ";
    str_sprintf(authIDAsChar, "%d", cachedGranteeIDs[i]);
    granteeList += authIDAsChar;
  }
  authList += ")";
  granteeList += ")";
  return numRoles;
}

// ----------------------------------------------------------------------------
// method: addFromClause
//
// This method adds a table-ref to the FROM clause to retrieve privilege
// details for the user and the user's roles.  
//
// authList: list of authIDs for the specified user and their roles
//
// schemaNamePred: determines how SCHEMA_PRIVILEGES information is extracted
//
//   false - schema uids are unioned with object_uids from other priv tables.
//     In this case, all schema and object uids are returned and the uids
//     are used to retrieve the corresponding object_name. 
//
//   true - uids are retrieved by joining SCHEMA_PRIVILEGES with OBJECTS 
//     In this case, the schema names are returned from SCHEMA_PRIVILEGES and
//     joined with objects table to get all uids in the schema.  This list of
//     uids are unioned with object and column privileges.  
//
// ----------------------------------------------------------------------------
static NAString addFromClause(
  const NAString &authList,
  const bool &schemaNamePred)
{

  char buf [authList.length()*3 + MAX_SQL_IDENTIFIER_NAME_LEN*9 + 200];
  NAString cmd;

  if (schemaNamePred)
    snprintf(buf, sizeof(buf), ", (select ob.object_uid "
             "from (select schema_name from %s.\"%s\".%s "
             "where grantee_id in %s) as sp join "
             "(select catalog_name, schema_name, object_name, object_uid "
             "from %s.\"%s\".%s ) ob "
             "on replace (sp.schema_name, '\"', '')  = (ob.catalog_name || '.' || ob.schema_name) "
             "union(select object_uid from %s.\"%s\".%s where grantee_id in %s "
             "union (select object_uid from %s.\"%s\".%s where grantee_id in %s  ))) as p(p_uid) ",
             TRAFODION_SYSTEM_CATALOG, SEABASE_PRIVMGR_SCHEMA, 
             PRIVMGR_SCHEMA_PRIVILEGES, authList.data(),
             TRAFODION_SYSTEM_CATALOG, SEABASE_MD_SCHEMA, SEABASE_OBJECTS,
             TRAFODION_SYSTEM_CATALOG, SEABASE_PRIVMGR_SCHEMA, 
             PRIVMGR_OBJECT_PRIVILEGES, authList.data(),
             TRAFODION_SYSTEM_CATALOG, SEABASE_PRIVMGR_SCHEMA, 
             PRIVMGR_COLUMN_PRIVILEGES, authList.data());

  else
    snprintf(buf, sizeof(buf), 
             ", (select schema_uid from %s.\"%s\".%s where grantee_id in %s "
             "union (select object_uid from %s.\"%s\".%s where grantee_id in %s "
             "union (select object_uid from %s.\"%s\".%s where grantee_id in %s "
             " ))) as p(object_uid) ", 
             TRAFODION_SYSTEM_CATALOG, SEABASE_PRIVMGR_SCHEMA, 
             PRIVMGR_SCHEMA_PRIVILEGES, authList.data(),
             TRAFODION_SYSTEM_CATALOG, SEABASE_PRIVMGR_SCHEMA, 
             PRIVMGR_OBJECT_PRIVILEGES, authList.data(),
             TRAFODION_SYSTEM_CATALOG, SEABASE_PRIVMGR_SCHEMA, 
             PRIVMGR_COLUMN_PRIVILEGES, authList.data());

  cmd = buf;

  return cmd;
}

// ----------------------------------------------------------------------------
// method: addWhereClause
//
// This method adds an extra predicate to the where clause to limit the rows
// returned to the objects that the user and their roles have been granted
// privs.
//
// schemaNamePred:  determines how the predicate is generated
// uidColumn: name of the UID columns to do the predicate
//
// ----------------------------------------------------------------------------
static NAString addWhereClause(
  const bool &schemaNamePred,
  const char *uidColumn)
{
  NAString cmd; 
  if (schemaNamePred)
    cmd += "and p.p_uid = ";
  else
    cmd += " and p.object_uid = ";

  cmd += uidColumn;
  return cmd;
}

// ----------------------------------------------------------------------------
// getGrantedPrivCmd
//
// Generates syntax that limits the result set to those objects where the 
// current user has at least one privilege assigned. The syntax unions grantees
// from object_privileges, column_privileges, and schema_privileges. The 
// grantee list (authList) includes the current user and the current users 
// roles.
// 
//   includeSchemaObjs: return all object UIDs in schemas where user has privs
//     otherwise just return schema_uids where user has privs
//   qualifier: correlation name for the join column to the privs table
//   uidColumn: name of the privilege table join column, defaults to object_uid 
//   qualified2: correlation name for the objects table when looking up
//     schema privileges.  If not specified, then qualifier is used.
// ---------------------------------------------------------------------------- 
NAString ExExeUtilGetMetadataInfoTcb::getGrantedPrivCmd(
  const NAString &authList,
  const char * cat,
  const bool &includeSchemaObjs = false,
  const NAString &qualifier,
  const NAString &uidColumn,
  const char * qualifier2)
{
  char buf [authList.length()*3 + MAX_SQL_IDENTIFIER_NAME_LEN*9 + 200];
  NAString cmd;

  if (includeSchemaObjs)
    {
      // Get privileges granted to the user (and their roles) from object and 
      // column privileges. If the user (or one of their roles) has been granted
      // any privilege on the object's schema, then also return the object.
      snprintf(buf, sizeof(buf), "and (%s%s in "
               "(select object_uid from %s.\"%s\".%s where grantee_id in %s union "
               "(select object_uid from %s.\"%s\".%s  where grantee_id in %s))"
               " or exists (select schema_uid from %s.\"%s\".%s p"
               "   where ((%scatalog_name || '.' || %sschema_name) = "
               "     replace (p.schema_name, '\"', ''))"  
               "   and grantee_id in %s ))",
               qualifier.data(), uidColumn.data(),
               cat, SEABASE_PRIVMGR_SCHEMA, PRIVMGR_OBJECT_PRIVILEGES, authList.data(),
               cat, SEABASE_PRIVMGR_SCHEMA, PRIVMGR_COLUMN_PRIVILEGES, authList.data(),
               cat, SEABASE_PRIVMGR_SCHEMA, PRIVMGR_SCHEMA_PRIVILEGES, 
               (qualifier2) ? qualifier2 : qualifier.data(), 
               (qualifier2) ? qualifier2 : qualifier.data(), authList.data());
          cmd = buf;
    }

  else
    {
      // Get privileges granted to the user (and their roles) from the object, 
      // column, and schema privileges.
      snprintf(buf, sizeof(buf), "and (%s%s in "
               "(select object_uid from %s.\"%s\".%s where grantee_id in %s union "
               "(select object_uid from %s.\"%s\".%s where grantee_id in %s) union "
               "(select schema_uid from %s.\"%s\".%s where grantee_id in %s)))",
               qualifier.data(), uidColumn.data(),
               cat, SEABASE_PRIVMGR_SCHEMA, PRIVMGR_OBJECT_PRIVILEGES, authList.data(),
               cat, SEABASE_PRIVMGR_SCHEMA, PRIVMGR_COLUMN_PRIVILEGES, authList.data(),
               cat, SEABASE_PRIVMGR_SCHEMA, PRIVMGR_SCHEMA_PRIVILEGES, authList.data());
      cmd = buf;
    }
  return cmd;
}

// ----------------------------------------------------------------------------
// getGroupList
//
// If authentication is enabled, gets the group list from DBSECURITY.
// If authentication is not enabled, gets the group list from the cqd
//    USER_GROUPNAMES.  If the cqd is not set, an empty string is returned
//
// If unexpected error occurs, an empty string is returned.
// If none found, or an unexpected error occurs, NULL is returned.
//
// The returned group list is returned in a format that can be
// used in a query "in" clause.
//
// For example:
//   (800101, 800108)
// ----------------------------------------------------------------------------
void ExExeUtilGetMetadataInfoTcb::getGroupList(
  const char * userName,
  NAString &groupIDString)
{
  NAList<Int32> groupIDs(getHeap());
  Int32 retcode = ComUser::getUserGroups(userName, true, groupIDs, NULL);
  if (retcode != 0 || groupIDs.entries() == 0)
    return;

  char authIDAsChar[sizeof(Int32)+10];

  groupIDString += "(";
  for (Int32 i = 0; i < groupIDs.entries(); i++)
  {
    if (i > 0)
      groupIDString += ", ";
    str_sprintf(authIDAsChar, "%d", groupIDs[i]);
    groupIDString += authIDAsChar;
   }
   groupIDString += ")";
 }

// ----------------------------------------------------------------------------
// getRoleList
//
// Reads the "_PRIVMGR_MD_".role_usage table to return the list of role IDs
// granted to the user specified in userID.
//
// If none found, or an unexpected error occurs, NULL is returned.
// The function allocates memory for the returned role list, the caller is
// responsible for deleting this memory.
//
// The returned role list includes the roles granted, plus the userID passed
// in, plus the special role PUBLIC.  It is returned in a format that can be
// used in a query "in" clause.  
//
// If the advanced feature is enabled, the returned list also contains any
// roles a user has through its groups.
//
// For example:
//   (-1, 33334, 1000004, 1000056, 800101)
// ----------------------------------------------------------------------------
char * ExExeUtilGetMetadataInfoTcb::getRoleList(
  bool &containsRootRole,
  const Int32 userID,
  const char *catName,
  const char *schName,
  const char *objName)
{
  // Always include PUBLIC
  NAString roleList("(-1");
  containsRootRole = false;

  short rc      = 0;
  Lng32 cliRC   = 0;

  // If userID is a user, then include roles assigned to their user groups
  NABoolean enabled = (msg_license_multitenancy_enabled() || msg_license_advanced_enabled());
  if (CmpCommon::getDefault(ALLOW_ROLE_GRANTS_TO_GROUPS) == DF_OFF ||
      (CmpCommon::getDefault(ALLOW_ROLE_GRANTS_TO_GROUPS) == DF_SYSTEM && getenv("SQLMX_REGRESS")))
     enabled = FALSE;

  if (CmpSeabaseDDLauth::isUserID(userID) &&  enabled)
    sprintf(queryBuf_, "select distinct role_id from %s.\"%s\".%s ru left join "
                       " (get groups for user \"%s\") as g(gname) on ru.grantee_name = g.gname"
                       "  where gname is not null or (gname is null and grantee_id = %d)",
            catName, schName, objName, getMItdb().getParam1(),userID);
  else
    sprintf(queryBuf_, "select role_id from %s.\"%s\".%s where grantee_id = %d ",
            catName, schName, objName, userID);
  
  if (initializeInfoList(infoList_)) return NULL;

  numOutputEntries_ = 1;
  cliRC = fetchAllRows(infoList_, queryBuf_, numOutputEntries_, FALSE, rc);
  if (cliRC < 0)
    {
      cliInterface()->allocAndRetrieveSQLDiagnostics(diagsArea_);
      return NULL;
    }

  char buf[30];
  infoList_->position();
  while (NOT infoList_->atEnd())
    {
      OutputInfo * vi = (OutputInfo*)infoList_->getCurr();
      if (vi)
        {
          int roleID = *(Lng32*)vi->get(0);
          if (roleID == ROOT_ROLE_ID)
            containsRootRole = true;
          str_sprintf(buf, ", %d", roleID);
          roleList += buf;
        }
          infoList_->advance();
    }
  str_sprintf(buf, ", %d)", userID);
  roleList += buf;


  char * list = new (getHeap()) char [roleList.length() + 1];
  strcpy(list, roleList.data());
  list[roleList.length()] = 0;

  return list;
}

// ----------------------------------------------------------------------------
// getObjectUID
//
// Reads the "_MD_".objects table to get the object_uid from the passed in 
// object.
//   if not found for any reason, return -1
//   otherwise returns the UID
//
//  catName, schName, objName is OBJECTS table location
//  targetName is the object to lookup
//  type is the type(s) of object
//    for most object, it is a single value, 
//    for schema objects is include ('PS', 'SS') - public or shared schema
// ----------------------------------------------------------------------------
Int64 ExExeUtilGetMetadataInfoTcb::getObjectUID(
  const char *catName,
  const char *schName,
  const char *objName,
  const char *targetName,
  const char *type)
{
  short rc      = 0;
  Lng32 cliRC   = 0;

  sprintf(queryBuf_, "select object_uid from %s.\"%s\".%s where "
          " catalog_name  = '%s' and "
          " schema_name = '%s' and "
          " object_name = '%s' and object_type %s ",
          catName, schName,objName,
          getMItdb().getCat(), getMItdb().getSch(),
          targetName, type) ;

  if (initializeInfoList(infoList_)) return NA_UserIdDefault;

  numOutputEntries_ = 1;
  cliRC = fetchAllRows(infoList_, queryBuf_, numOutputEntries_, FALSE, rc);
  if (cliRC < 0)
  {
    cliInterface()->allocAndRetrieveSQLDiagnostics(diagsArea_);
    return -1;
  }

  infoList_->position();
  OutputInfo * vi = (OutputInfo*)infoList_->getCurr();
  if (vi)
    return *(Int64*)vi->get(0);
  return -1;
}


// ----------------------------------------------------------------------------
// method:  checkUserPrivs
//
//  return TRUE to add privilege checks to queries
//  return FALSE to return all details independent of privileges
// ----------------------------------------------------------------------------
NABoolean ExExeUtilGetMetadataInfoTcb::checkUserPrivs(
  ContextCli * currContext,
  const ComTdbExeUtilGetMetadataInfo::QueryType queryType)
{
  // if no authorization, everyone sees everything
  if (!CmpCommon::context()->isAuthorizationEnabled())
    return FALSE;

  // Root user sees everything
  if (ComUser::isRootUserID())
    return FALSE;

  Int32 numRoles;
  Int32 *roleList;
  Int32 *granteeList;
  if (currContext->getRoleList(numRoles, roleList, granteeList) == SUCCESS)
  {
    char authIDAsChar[sizeof(Int32)+10];
    NAString auths;
    for (Int32 i = 0; i < numRoles; i++)
    {
      if (roleList[i] == ROOT_ROLE_ID)
        return FALSE;
    }
  }

  // any user granted the SHOW component privilege sees everything
  std::string privMDLoc = TRAFODION_SYSTEM_CATALOG;
  privMDLoc += ".\"";
  privMDLoc += SEABASE_PRIVMGR_SCHEMA;
  privMDLoc += "\"";
  PrivMgrComponentPrivileges componentPrivileges(privMDLoc,getDiagsArea());

  if (componentPrivileges.hasSQLPriv(ComUser::getCurrentUser(),SQLOperation::SHOW,true)) return FALSE;

  // Check component privilege based on QueryType
  switch (queryType)
  {
    // if user has MANAGE_ROLES, can perform role operations
    case ComTdbExeUtilGetMetadataInfo::ROLES_:
    case ComTdbExeUtilGetMetadataInfo::ROLES_FOR_ROLE_:
    case ComTdbExeUtilGetMetadataInfo::ROLES_FOR_USER_:
    case ComTdbExeUtilGetMetadataInfo::ROLES_FOR_GROUP_:
    {
      if (componentPrivileges.hasSQLPriv(ComUser::getCurrentUser(),SQLOperation::MANAGE_ROLES, true))
        return FALSE;
      break;
    }

    // if user has MANAGE_USERS, can perform user operations
    case ComTdbExeUtilGetMetadataInfo::USERS_:
    case ComTdbExeUtilGetMetadataInfo::USERS_FOR_ROLE_:
    {
      if (componentPrivileges.hasSQLPriv(ComUser::getCurrentUser(),SQLOperation::MANAGE_USERS,true))
        return FALSE;
      break;
    }

    // if user has MANAGE_GROUPS, can perform user operations
    case ComTdbExeUtilGetMetadataInfo::GROUPS_:
    case ComTdbExeUtilGetMetadataInfo::USERS_FOR_MEMBERS_:
    case ComTdbExeUtilGetMetadataInfo::GROUPS_FOR_USER_:
    {
      if (componentPrivileges.hasSQLPriv(ComUser::getCurrentUser(),SQLOperation::MANAGE_GROUPS,true))
        return FALSE;
      break;
    }

    // if user has MANAGE_COMPONENTS, can perform component operations
    case ComTdbExeUtilGetMetadataInfo::COMPONENTS_:
    case ComTdbExeUtilGetMetadataInfo::COMPONENT_OPERATIONS_:
    case ComTdbExeUtilGetMetadataInfo::COMPONENT_PRIVILEGES_:
    {
      if (componentPrivileges.hasSQLPriv(ComUser::getCurrentUser(),SQLOperation::MANAGE_COMPONENTS,true))
        return FALSE;
      break;
    }

    // if user has MANAGE_LIBRARIES, can perform library operations
    case ComTdbExeUtilGetMetadataInfo::PROCEDURES_FOR_LIBRARY_:
    case ComTdbExeUtilGetMetadataInfo::FUNCTIONS_FOR_LIBRARY_:
    case ComTdbExeUtilGetMetadataInfo::TABLE_FUNCTIONS_FOR_LIBRARY_:
    {
      if (componentPrivileges.hasSQLPriv(ComUser::getCurrentUser(),SQLOperation::MANAGE_LIBRARY,true))
        return FALSE;
      break;
    }

   // if user has MANAGE_RESOURCE_GROUPS or MANAGE_TENANTS, can perform RGroup stuff
   case ComTdbExeUtilGetMetadataInfo::RGROUPS_:
   case ComTdbExeUtilGetMetadataInfo::NODES_:
   case ComTdbExeUtilGetMetadataInfo::NODES_IN_RGROUP_:
   case ComTdbExeUtilGetMetadataInfo::TENANTS_ON_RGROUP_:
    {
      if (componentPrivileges.hasSQLPriv
           (ComUser::getCurrentUser(),SQLOperation::MANAGE_RESOURCE_GROUPS,true) ||
          componentPrivileges.hasSQLPriv
           (ComUser::getCurrentUser(), SQLOperation::MANAGE_TENANTS, true))
        return FALSE;
      break;
    }

   // If user has MANAGE_TENANTS, can perform tenant stuff
   case ComTdbExeUtilGetMetadataInfo::NODES_FOR_TENANT_:
   case ComTdbExeUtilGetMetadataInfo::TENANTS_:
   case ComTdbExeUtilGetMetadataInfo::GROUPS_FOR_TENANT_:
    {
      if (componentPrivileges.hasSQLPriv
           (ComUser::getCurrentUser(), SQLOperation::MANAGE_TENANTS, true))
        return FALSE;
      break;
    }

   default:
     break;
  }
  return TRUE;
}

// ----------------------------------------------------------------------------
// method:  colPrivsFrag
//
// This method was added to address a performance issue.  When determining if 
// the user has column level privileges, we need to get the column name from 
// Hive.  The call to get the column name (hivemd) is very expensive.  So this
// method checks to see if the requested user has been granted any column
// level privileges on a hive table.  If so, we will go ahead and do the
// mapping (call hivemd).  If not, then we will not include the hivemd 
// fragment for the query.
//
// Since we are scanning the column privileges table anyway, we also see if 
// the requested user (or their roles) has been granted any privileges.  If so,
// we include the column privileges check in the query. 
//
// For Sentry enabled installations, we won't store Hive privileges in 
// EsgynDB metadata.  By avoiding the hivemd calls, we save a lot of time
// in processing the request.
//
//  returns additional union(s) for the getPrivForAuth query
//  returns:
//     0 - successful
//    -1 - unexpected error occurred
// ----------------------------------------------------------------------------
Int32 ExExeUtilGetMetadataInfoTcb::colPrivsFrag(
  const char *authName,
  const char * cat,
  const NAString &privWhereClause,
  NAString &colPrivsStmt)
{
  // if no authorization, skip
  if (!CmpCommon::context()->isAuthorizationEnabled())
    return 0;

  short rc      = 0;
  Lng32 cliRC   = 0;

  // See if privileges granted on Hive object or to the user/user's roles
  NAString likeClause("like 'HIVE.%'");
  sprintf(queryBuf_, "select "
                     "sum(case when (object_name %s and grantee_id %s) then 1 else 0 end), "
                     "sum(case when grantee_id %s then 1 else 0 end) "
                     "from %s.\"%s\".%s",
          likeClause.data(), privWhereClause.data(), privWhereClause.data(),
          cat, SEABASE_PRIVMGR_SCHEMA,
          PRIVMGR_COLUMN_PRIVILEGES);

  if (initializeInfoList(infoList_)) return -1;

  numOutputEntries_ = 2;
  cliRC = fetchAllRows(infoList_, queryBuf_, numOutputEntries_, FALSE, rc);
  if (cliRC < 0)
  {
    cliInterface()->retrieveSQLDiagnostics(getDiagsArea());
    return -1;
  }

  bool hasHive = false;
  bool hasGrants = false;
  infoList_->position();
  OutputInfo * vi = (OutputInfo*)infoList_->getCurr();
  if (vi && vi->get(0))
  {
    if (*(Int64*)vi->get(0) > 0)
      hasHive = true;
    if(*(Int64*)vi->get(1) > 0)
      hasGrants = true;
  }

  Int32 len = privWhereClause.length() + 500;
  char msg[len];
  snprintf(msg, len, "ExExeUtilGetMetadataUtilTcb::colPrivsFrag, user: %s, "
                     "grantees: %s, union col privs: %d, union hive cols: %d",
           authName,
           privWhereClause.data(),
           hasGrants, (hasHive && hasGrants));
  QRLogger::log(CAT_SQL_EXE, LL_DEBUG, "%s", msg);

  // Attach union with column privileges clause
  if (hasGrants)
  {
    const QueryString * grants = getPrivsForColsQuery;
    Int32 sizeOfGrants = sizeof(getPrivsForColsQuery);
    Int32 qryArraySize = sizeOfGrants / sizeof(QueryString);
    char * gluedQuery;
    Int32 gluedQuerySize;

    glueQueryFragments(qryArraySize, grants, gluedQuery, gluedQuerySize);
    char buf[strlen(gluedQuery) + privWhereClause.length() + MAX_SQL_IDENTIFIER_NAME_LEN*6 + 200];
    snprintf(buf, sizeof(buf), gluedQuery,
             cat, SEABASE_PRIVMGR_SCHEMA, PRIVMGR_COLUMN_PRIVILEGES,
             cat, SEABASE_MD_SCHEMA, SEABASE_COLUMNS,
             privWhereClause.data());
    colPrivsStmt = buf;
    NADELETEBASIC(gluedQuery, getMyHeap());
    if (hasHive)
    {
      // attach union with hivemd columns clause
      const QueryString * hive = getPrivsForHiveColsQuery;
      Int32 sizeOfHive = sizeof(getPrivsForHiveColsQuery);
      qryArraySize = sizeOfHive / sizeof(QueryString);
      glueQueryFragments(qryArraySize, hive, gluedQuery, gluedQuerySize);
      snprintf(buf, sizeof(buf), gluedQuery,
               cat, SEABASE_PRIVMGR_SCHEMA, PRIVMGR_COLUMN_PRIVILEGES,
               cat, SEABASE_MD_SCHEMA, SEABASE_OBJECTS,
               privWhereClause.data());
      colPrivsStmt += buf;
      NADELETEBASIC(gluedQuery, getMyHeap());
    }
  }
  return 0;
}

//////////////////////////////////////////////////////
// work() for ExExeUtilGetMetadataInfoTcb
//////////////////////////////////////////////////////
short ExExeUtilGetMetadataInfoTcb::work()
{
  short retcode = 0;
  Lng32 cliRC = 0;
  ex_expr::exp_return_type exprRetCode = ex_expr::EXPR_OK;


  // if no parent request, return
  if (qparent_.down->isEmpty())
    return WORK_OK;

  // if no room in up queue, won't be able to return data/status.
  // Come back later.
  if (qparent_.up->isFull())
    return WORK_OK;

  ex_queue_entry * pentry_down = qparent_.down->getHeadEntry();
  ExExeUtilPrivateState & pstate =
    *((ExExeUtilPrivateState*) pentry_down->pstate);

  // Get the globals stucture of the master executor.
  ExExeStmtGlobals *exeGlob = getGlobals()->castToExExeStmtGlobals();
  ExMasterStmtGlobals *masterGlob = exeGlob->castToExMasterStmtGlobals();
  ContextCli * currContext = masterGlob->getStatement()->getContext();

  while (1)
    {
      switch (step_)
	{
	case INITIAL_:
	  {
	    step_ = DISABLE_CQS_;

	    headingReturned_ = FALSE;

	    numOutputEntries_ = 1;
            returnRowCount_ = 0 ;

	    objectUid_[0] = 0;
	  }
	break;

	case DISABLE_CQS_:
	  {
	    if (disableCQS())
	      {
		step_ = HANDLE_ERROR_;
		break;
	      }

            // assume for now that everything is an HBase object

            // In the future, we may wish to check the TDB to see
            // what kind of object is being queried, and pick the
            // relevant step as a result. One thing that should be
            // kept in mind though is that we want nice semantics
            // when the object doesn't exist. Today, when a catalog
            // does not exist, for example, GET SCHEMAS simply
            // returns nothing. Similarly, when a catalog exists
            // but the schema does not, GET TABLES returns nothing.

            step_ = CHECK_ACCESS_;
          }
        break;

        case CHECK_ACCESS_:
          {
            // If internal query, allow operation
            if ((currContext->getSqlParserFlags() & 0x20000) != 0)
            {
              step_ = SETUP_HBASE_QUERY_;
              break;
            }

            // The following get requests have been changed to support current
            // user access.  If in list, just return
            if (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::ROLES_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::CATALOGS_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::ACTIVE_ROLES_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::ROLES_FOR_ROLE_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::ROLES_FOR_USER_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::ROLES_FOR_GROUP_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::USERS_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::USERS_FOR_ROLE_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::RGROUPS_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::NODES_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::NODES_FOR_TENANT_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::NODES_IN_RGROUP_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::TENANTS_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::TENANTS_ON_RGROUP_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::GROUPS_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::GROUPS_FOR_USER_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::GROUPS_FOR_TENANT_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::COMPONENTS_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::COMPONENT_OPERATIONS_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::COMPONENT_PRIVILEGES_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::FUNCTIONS_FOR_LIBRARY_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::FUNCTIONS_FOR_ROLE_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::FUNCTIONS_FOR_USER_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::FUNCTIONS_IN_SCHEMA_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::HBASE_OBJECTS_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::HBASE_REG_TABLES_IN_CATALOG_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::HIVE_EXT_TABLES_IN_CATALOG_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::HIVE_REG_OBJECTS_IN_CATALOG_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::HIVE_REG_SCHEMAS_IN_CATALOG_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::HIVE_REG_TABLES_IN_CATALOG_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::HIVE_REG_VIEWS_IN_CATALOG_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::INDEXES_FOR_ROLE_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::INDEXES_FOR_USER_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::INDEXES_IN_SCHEMA_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::INDEXES_ON_TABLE_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::LIBRARIES_FOR_ROLE_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::LIBRARIES_FOR_USER_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::LIBRARIES_IN_SCHEMA_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::NODES_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::OBJECTS_FOR_USER_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::OBJECTS_IN_CATALOG_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::OBJECTS_IN_SCHEMA_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::OBJECTS_IN_VIEW_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::OBJECTS_ON_TABLE_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::PACKAGES_FOR_USER_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::PACKAGES_IN_SCHEMA_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::PRIVILEGES_FOR_ROLE_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::PRIVILEGES_FOR_USER_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::PRIVILEGES_ON_LIBRARY_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::PRIVILEGES_ON_ROUTINE_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::PRIVILEGES_ON_SEQUENCE_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::PRIVILEGES_ON_TABLE_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::PRIVILEGES_ON_VIEW_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::PROCEDURES_FOR_LIBRARY_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::PROCEDURES_FOR_ROLE_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::PROCEDURES_FOR_USER_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::PROCEDURES_IN_SCHEMA_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::RGROUPS_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::ROLES_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::ROLES_FOR_ROLE_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::ROLES_FOR_USER_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::ROLES_FOR_GROUP_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::SCHEMAS_FOR_ROLE_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::SCHEMAS_FOR_USER_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::SCHEMAS_IN_CATALOG_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::SEQUENCES_IN_SCHEMA_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::SEQUENCES_IN_CATALOG_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::TABLE_FUNCTIONS_FOR_LIBRARY_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::TABLE_FUNCTIONS_FOR_ROLE_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::TABLE_FUNCTIONS_FOR_USER_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::TABLE_FUNCTIONS_IN_SCHEMA_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::TABLES_FOR_ROLE_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::TABLES_FOR_USER_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::TABLES_IN_CATALOG_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::TABLES_IN_SCHEMA_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::TABLES_IN_VIEW_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::TENANTS_ ||
		getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::TRIGGERS_IN_SCHEMA_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::USERS_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::USERS_FOR_ROLE_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::VIEWS_FOR_ROLE_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::VIEWS_FOR_USER_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::VIEWS_IN_CATALOG_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::VIEWS_IN_SCHEMA_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::VIEWS_IN_VIEW_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::VIEWS_ON_TABLE_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::VIEWS_ON_VIEW_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::USERS_FOR_MEMBERS_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::PASSWORD_POLICY_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::AUTH_TYPE_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::AUTHZ_STATUS_
                )

                {
	           step_ = SETUP_HBASE_QUERY_;
                   break;
                }

            // remaining operations require SHOW privilege to perform
            std::string privMDLoc = TRAFODION_SYSTEM_CATALOG;
            privMDLoc += ".\"";
            privMDLoc += SEABASE_PRIVMGR_SCHEMA;
            privMDLoc += "\"";
            PrivMgrComponentPrivileges componentPrivileges(privMDLoc,getDiagsArea());
            if (componentPrivileges.isAuthorizationEnabled())
              {
                if (!componentPrivileges.hasSQLPriv(ComUser::getCurrentUser(),SQLOperation::SHOW,true))
                  {
                    ExRaiseSqlError(getHeap(), &diagsArea_, -CAT_NOT_AUTHORIZED);
                    step_ = HANDLE_ERROR_;
                    break;
                  }
              }
	    step_ = SETUP_HBASE_QUERY_;
	  }
	break;

	case SETUP_HBASE_QUERY_:
	  {
	    const QueryString * qs = NULL;
	    Int32 sizeOfqs = 0;
            NAString userQuery;

	    char ausStr[1000];
            ausStr[0] = '\0';
	    char catSchValue[ComMAX_2_PART_EXTERNAL_UTF8_NAME_LEN_IN_BYTES+50];
            catSchValue[0] = '\0';
	    char endQuote[10];
            endQuote[0] = '\0';

	    if (getMItdb().returnFullyQualNames())
	      {
		str_sprintf(catSchValue, "'\"%s\".\"%s\".\"' || ",
			    getMItdb().getCat(), getMItdb().getSch());

		str_sprintf(endQuote, "|| '\"' ");
	      }

	    char cat[100];
	    char sch[100];
            char pmsch[100];
            char tsch[100];
	    char tab[100];
	    char col[100];
            char indexes[100];
	    char view[100];
	    char view_usage[100];
	    char text[100];
            char auths[100];
            char role_usage[100];
            char resources[100];
            char resource_usage[100];
            char objPrivs[100];
            char schPrivs[100];
            char colPrivs[100];
            char components[100];
            char componentOperations[100];
            char componentPrivileges[100];
            char routine[100];
            char library_usage[100];
            char tenant_usage[100];
            char tenants[100];
            char hiveObjType[100];
            char hiveGetType[10];
            char hiveSysCat[10];

            char textType[10];

	    strcpy(cat, TRAFODION_SYSTEM_CATALOG);

	    strcpy(sch, SEABASE_MD_SCHEMA);
	    strcpy(pmsch, SEABASE_PRIVMGR_SCHEMA);
	    strcpy(tsch, SEABASE_TENANT_SCHEMA);
	    strcpy(tab, SEABASE_OBJECTS);
	    strcpy(col, SEABASE_COLUMNS);
	    strcpy(view, SEABASE_VIEWS);
	    strcpy(view_usage, SEABASE_VIEWS_USAGE);
            strcpy(indexes, SEABASE_INDEXES);
            strcpy(text, SEABASE_TEXT);
            strcpy(auths, SEABASE_AUTHS);
            strcpy(tenant_usage, SEABASE_TENANT_USAGE);
            strcpy(tenants, SEABASE_TENANTS);
            strcpy(objPrivs, "OBJECT_PRIVILEGES");
            strcpy(colPrivs, "COLUMN_PRIVILEGES");
            strcpy(schPrivs, "SCHEMA_PRIVILEGES");
            strcpy(role_usage, "ROLE_USAGE");
            strcpy(resource_usage, SEABASE_RESOURCE_USAGE);
            strcpy(resources, SEABASE_RESOURCES);
            strcpy(components, "COMPONENTS");
            strcpy(componentOperations, "COMPONENT_OPERATIONS");
            strcpy(componentPrivileges, "COMPONENT_PRIVILEGES");
            strcpy(routine, SEABASE_ROUTINES);
            strcpy(library_usage, SEABASE_LIBRARIES_USAGE);
            strcpy(hiveSysCat, HIVE_SYSTEM_CATALOG_LC);

            // Determine if need to restrict data to user visable data only.
            NABoolean doPrivCheck = checkUserPrivs(currContext, getMItdb().queryType_);  
            NAString privWhereClause;
            NAString privFromClause;

            // helper strings 
            NAString var;
            NAString var1;
            NAString groupList;

            // If authorization enabled, get role and role grantee list from cache
            NAString authList;
            NAString granteeList;
            if (CmpCommon::context()->isAuthorizationEnabled())
              {
                Int32 numRoles = getCurrentUserRoles(currContext, authList, granteeList);
                if (numRoles == 0)
                {
                  ExRaiseSqlWarning(getHeap(), &diagsArea_, (ExeErrorCode)1057);
                }
              }

            // If request to get privilege information but authorization tables were not initialized,
	    if(getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::COMPONENTS_
	      ||getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::COMPONENT_OPERATIONS_
	      ||getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::COMPONENT_PRIVILEGES_
              ||getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::FUNCTIONS_FOR_ROLE_
              ||getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::FUNCTIONS_FOR_USER_
              ||getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::FUNCTIONS_FOR_ROLE_
              ||getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::INDEXES_FOR_USER_
              ||getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::INDEXES_FOR_ROLE_
              ||getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::LIBRARIES_FOR_ROLE_
              ||getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::LIBRARIES_FOR_USER_
              ||getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::OBJECTS_FOR_USER_
              ||getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::ROLES_FOR_USER_
              ||getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::ROLES_FOR_GROUP_
              ||getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::PROCEDURES_FOR_ROLE_
              ||getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::PROCEDURES_FOR_USER_
              ||getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::TABLE_FUNCTIONS_FOR_USER_
              ||getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::TABLE_FUNCTIONS_FOR_ROLE_
              ||getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::TABLES_FOR_USER_
              ||getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::TABLES_FOR_ROLE_
              ||getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::VIEWS_FOR_ROLE_
              ||getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::VIEWS_FOR_USER_
              ||getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::USERS_FOR_ROLE_
              ||getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::PRIVILEGES_FOR_ROLE_
              ||getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::PRIVILEGES_FOR_USER_
              ||getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::PRIVILEGES_ON_LIBRARY_
              ||getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::PRIVILEGES_ON_ROUTINE_
              ||getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::PRIVILEGES_ON_SEQUENCE_
              ||getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::PRIVILEGES_ON_TABLE_
              ||getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::PRIVILEGES_ON_VIEW_
              //||getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::USERS_FOR_MEMBERS_
              //||getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::PASSWORD_POLICY_
              )
	    {
               if (!CmpCommon::context()->isAuthorizationEnabled())
               {
                  ExRaiseSqlError(getHeap(), &diagsArea_, -CAT_AUTHORIZATION_NOT_ENABLED);
                  step_ = HANDLE_ERROR_;
                  break;
               }
            }

            // If request to get tenant information but multi-tenancy feature is not enabled
            if (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::TENANTS_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::TENANTS_ON_RGROUP_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::GROUPS_FOR_TENANT_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::RGROUPS_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::NODES_FOR_TENANT_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::NODES_IN_RGROUP_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::NODES_ )
            {
               NABoolean enabled = msg_license_multitenancy_enabled();
               if (!enabled)
               {
                  ExRaiseSqlError(getHeap(), &diagsArea_, -4222,
                         NULL, NULL, NULL,
                         "multi-tenant");
                  step_ = HANDLE_ERROR_;
                  break;
               }
            }

            // If request to get advanced feature details
            if (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::GROUPS_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::GROUPS_FOR_USER_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::GROUPS_FOR_TENANT_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::USERS_FOR_MEMBERS_)
            {
               NABoolean enabled = msg_license_advanced_enabled();
               if (!enabled)
               {
                  ExRaiseSqlError(getHeap(), &diagsArea_, -4222,
                         NULL, NULL, NULL,
                         "advanced");
                  step_ = HANDLE_ERROR_;
                  break;
               }
            }


	    switch (getMItdb().queryType_)
	      {
              case ComTdbExeUtilGetMetadataInfo::AUTHZ_STATUS_:
              {
                  size_t strLen = 0;
                  strLen = strlen("Current Authorization Status");
                  strcpy(outputBuf_, "Current Authorization Status");
                  outputBuf_[strLen] = 0;
                  moveRowToUpQueue(outputBuf_);
                  str_pad(outputBuf_, strLen, '=');
                  outputBuf_[strLen] = 0;
                  moveRowToUpQueue(outputBuf_);
                  moveRowToUpQueue("");
                  sprintf(outputBuf_, "Authorization %s.", CmpCommon::context()->isAuthorizationEnabled() ? "enabled" : "disabled");
                  moveRowToUpQueue(outputBuf_);
                  moveRowToUpQueue(" ");
                  str_pad(outputBuf_, 23, '=');
                  outputBuf_[23] = 0;
                  moveRowToUpQueue(outputBuf_);
                  moveRowToUpQueue(" ");
                  step_ = ENABLE_CQS_;
                  //continue is ok
                  continue;
              }
              break;
              //don't use "xxx\nxxx\n" to outputing
              case ComTdbExeUtilGetMetadataInfo::USERS_FOR_MEMBERS_:
              {
                  if (doPrivCheck)
                  {
                      ExRaiseSqlError(getHeap(), &diagsArea_, -CAT_NOT_AUTHORIZED);
                      step_ = HANDLE_ERROR_;
                  }
                  qs = getTrafUsers;
                  sizeOfqs = sizeof(getTrafUsers);
                  privWhereClause.clear();
                  privWhereClause = "and AUTH_ID in (";

                  std::vector<int32_t> memberList;
                  if (-1 == CmpSeabaseDDLauth::getUserGroupMembers(getMItdb().getParam1(), memberList))
                  {
                      ExRaiseSqlError(getHeap(), &diagsArea_, -CAT_IS_NOT_A_GROUP,
                                      NULL, NULL, NULL,
                                      getMItdb().getParam1());
                      step_ = HANDLE_ERROR_;
                  }
                  else
                  {
                      if (memberList.empty())
                      {
                          step_ = ENABLE_CQS_;
                          //continue is ok
                          continue;
                      }
                      else
                      {
                          for (std::vector<int32_t>::iterator itor = memberList.begin();
                               itor != memberList.end(); itor++)
                          {
                              //for gcc 4.4.x
                              long long int tmp = *itor;
                              privWhereClause += NAString(std::to_string(tmp));
                              privWhereClause += ",";
                          }
                      }
                      //don't forget last of char is ","
                  }
                  //replace last of "," to ")"
                  privWhereClause.replace(privWhereClause.length() - 1, 1, ')');
                  param_[0] = cat;
                  param_[1] = sch;
                  param_[2] = auths;
                  param_[3] = (char *)privWhereClause.data();
                  numOutputEntries_ = 2;
              }
              break;

              case ComTdbExeUtilGetMetadataInfo::PASSWORD_POLICY_:
              {
                  //everyone can use this statement
                  if (COM_LDAP_AUTH == CmpCommon::getAuthenticationType())
                  {
                      // The relationship of group members is stored on LDAP server when using LDAP authenticaton
                      ExRaiseSqlError(getHeap(), &diagsArea_, -CAT_NOT_ALLOWED_OPERATION_ON_LDAP_AUTH,
                                      NULL, NULL, NULL);
                      step_ = HANDLE_ERROR_;
                      continue;
                  }
                  /*

                  Password check policy for User
                  Vaild characters in password:
                  Digit chars   [0-9]
                  English chars [a-z] or [A-Z]
                  Symbol chars  [-_@./]
                  Length for password:
                  %d - %d chars
                  Password must have chars:
                  Upper chars
                  Lower chars
                  Password must not have chars:
                  Symbol chars
                  Password must contents:
                  Upper chars %d
                  Lower chars %d
                  Digital chars any

                */
                  char sb[64] = {0};
                  ComPwdPolicySyntax syntax = CmpCommon::getPasswordCheckSyntax();
                  if (syntax.max_password_length == 0xFF)
                  {
                      moveRowToUpQueue("Not check for password.");
                      step_ = ENABLE_CQS_;
                      //continue is ok
                      continue;
                  }
                  {
                      //headerline
                      size_t headerlineLen = strlen("Password check policy for User");
                      strcpy(outputBuf_, "Password check policy for User");
                      outputBuf_[headerlineLen] = 0;
                      moveRowToUpQueue(outputBuf_);
                      str_pad(outputBuf_, headerlineLen, '=');
                      moveRowToUpQueue(outputBuf_);
                      moveRowToUpQueue("");
                  }
                  moveRowToUpQueue("Vaild characters in password:");
                  moveRowToUpQueue("* Digit chars   [0-9]");
                  moveRowToUpQueue("* English chars [a-z] or [A-Z]");
                  moveRowToUpQueue("* Symbol chars  [-_@./]");
                  str_sprintf(sb, "Length for password %d - %d chars", syntax.min_password_length, syntax.max_password_length);
                  moveRowToUpQueue(sb);
                  if (syntax.digital_chars_number == 0xFF && syntax.lower_chars_number == 0xFF &&
                      syntax.symbol_chars_number == 0xFF && syntax.upper_chars_number == 0xFF)
                  {
                      moveRowToUpQueue("Password complexity rules:");
                      moveRowToUpQueue("* Contains at least three or more of them: upper character, lower character, digit and special symbols.");
                  }
                  else
                  {
                      if (syntax.digital_chars_number < 0xee || syntax.lower_chars_number < 0xee ||
                          syntax.symbol_chars_number < 0xee || syntax.upper_chars_number < 0xee)
                      {
                          moveRowToUpQueue("Password must contents character:");
                          if (syntax.digital_chars_number < 0xee)
                              moveRowToUpQueue("* Digital");
                          if (syntax.symbol_chars_number < 0xee)
                              moveRowToUpQueue("* Symbol Char");
                          if (syntax.upper_chars_number < 0xee)
                              moveRowToUpQueue("* Upper Char");
                          if (syntax.lower_chars_number < 0xee)
                              moveRowToUpQueue("* Lower Char");
                      }
                      if (syntax.digital_chars_number == 0xee || syntax.lower_chars_number == 0xee ||
                          syntax.symbol_chars_number == 0xee || syntax.upper_chars_number == 0xee)
                      {
                          moveRowToUpQueue("Password must not contents character:");
                          if (syntax.digital_chars_number == 0xee)
                              moveRowToUpQueue("* Digital");
                          if (syntax.symbol_chars_number == 0xee)
                              moveRowToUpQueue("* Symbol Char");
                          if (syntax.upper_chars_number == 0xee)
                              moveRowToUpQueue("* Upper Char");
                          if (syntax.lower_chars_number == 0xee)
                              moveRowToUpQueue("* Lower Char");
                      }
                      if (syntax.digital_chars_number < 0xee || syntax.lower_chars_number < 0xee ||
                          syntax.symbol_chars_number < 0xee || syntax.upper_chars_number < 0xee)
                      {
                          moveRowToUpQueue("Password complexity rules:");
                          if (syntax.digital_chars_number == 0)
                              moveRowToUpQueue("* Contains ANY digital");
                          else if (syntax.digital_chars_number < 0xee)
                          {
                              str_sprintf(sb, "* Contains %d at least digitals", syntax.digital_chars_number);
                              moveRowToUpQueue(sb);
                          }
                          if (syntax.symbol_chars_number == 0)
                              moveRowToUpQueue("* Contains ANY symbol char");
                          else if (syntax.symbol_chars_number < 0xee)
                          {
                              str_sprintf(sb, "* Contains %d at least symbol chars", syntax.symbol_chars_number);
                              moveRowToUpQueue(sb);
                          }
                          if (syntax.upper_chars_number == 0)
                              moveRowToUpQueue("* Contains ANY upper char");
                          else if (syntax.upper_chars_number < 0xee)
                          {
                              str_sprintf(sb, "* Contains %d at least upper chars", syntax.upper_chars_number);
                              moveRowToUpQueue(sb);
                          }
                          if (syntax.lower_chars_number == 0)
                              moveRowToUpQueue("* Contains ANY lower char");
                          else if (syntax.lower_chars_number < 0xee)
                          {
                              str_sprintf(sb, "* Contains %d at least lower chars", syntax.lower_chars_number);
                              moveRowToUpQueue(sb);
                          }
                      }
                  }
                  {
                      //footline
                      moveRowToUpQueue(" ");
                      str_pad(outputBuf_, 23, '=');
                      outputBuf_[23] = 0;
                      moveRowToUpQueue(outputBuf_);
                      moveRowToUpQueue(" ");
                  }
                  step_ = ENABLE_CQS_;
                  //continue is ok
                  continue;
              }
              break;

              case ComTdbExeUtilGetMetadataInfo::AUTH_TYPE_:
              {
                  const char *pStr = NULL;
                  size_t strLen = 0;
                  //headerline
                  {
                      strLen = strlen("Current Authentication Type");
                      strcpy(outputBuf_, "Current Authentication Type");
                      outputBuf_[strLen] = 0;
                      moveRowToUpQueue(outputBuf_);
                      str_pad(outputBuf_, strLen, '=');
                      outputBuf_[strLen] = 0;
                      moveRowToUpQueue(outputBuf_);
                      moveRowToUpQueue("");
                  }
                  ComAuthenticationType authType;
                  {
                      Int32 authenticationType;
                      bool authorizationEnabled, authorizationReady, auditingEnabled;
                      GetCliGlobals()->currContext()->getAuthState(authenticationType, authorizationEnabled,
                                                                   authorizationReady, auditingEnabled);
                      authType = (ComAuthenticationType)authenticationType;
                  }
                  switch (authType)
                  {
                  case COM_NOT_AUTH:
                  {
                      pStr = "The authentication is disabled";
                  }
                  break;
                  case COM_LDAP_AUTH:
                  {
                      pStr = "LDAP";
                  }
                  break;
                  case COM_NO_AUTH:
                  {
                      pStr = "LOCAL with non-password login";
                  }
                  break;
                  case COM_LOCAL_AUTH:
                  default:
                  {
                      pStr = "LOCAL";
                  }
                  break;
                  }
                  strLen = strlen(pStr);
                  strcpy(outputBuf_, pStr);
                  outputBuf_[strLen] = 0;
                  moveRowToUpQueue(outputBuf_);
                  moveRowToUpQueue(" ");
                  //footline
                  {
                      str_pad(outputBuf_, 23, '=');
                      outputBuf_[23] = 0;
                      moveRowToUpQueue(outputBuf_);
                      moveRowToUpQueue(" ");
                  }
                  step_ = ENABLE_CQS_;
                  //continue is ok
                  continue;
              }

              break;
              case ComTdbExeUtilGetMetadataInfo::CATALOGS_:
                {
                  // any user can get list of catalogs, no priv checks required
                  qs = getCatalogsQuery;
                  sizeOfqs = sizeof(getCatalogsQuery);
                }
              break;

	      case ComTdbExeUtilGetMetadataInfo::TABLES_IN_SCHEMA_:
		{
                  if (getMItdb().withNamespace())
                    {
                      qs = getTrafTablesInSchemaWithNamespaceQuery;
                      sizeOfqs = sizeof(getTrafTablesInSchemaWithNamespaceQuery);
                      strcpy(textType, "9");
                    }
                  else
                    {
                      qs = getTrafTablesInSchemaQuery;
                      sizeOfqs = sizeof(getTrafTablesInSchemaQuery);
                    }
                  
                  if (doPrivCheck)
                    {
                      privFromClause = addFromClause(authList, true);
                      privWhereClause = addWhereClause(true, "O.object_uid");
                    }

                  param_[0] = catSchValue;
                  param_[1] = endQuote;
                  param_[2] = cat;
                  param_[3] = sch;
                  param_[4] = tab;
                  if (getMItdb().withNamespace())
                    {
                      param_[5] = cat;
                      param_[6] = sch;
                      param_[7] = text;
                      param_[8] = textType;
                      param_[9] = (char *)privFromClause.data();
                      param_[10] = getMItdb().cat_;
                      param_[11] = getMItdb().sch_;
                      param_[12] = (char *)privWhereClause.data();
                    }
                  else
                    {
                      param_[5] = (char *)privFromClause.data();
                      param_[6] = getMItdb().cat_;
                      param_[7] = getMItdb().sch_;
                      param_[8] = (char *)privWhereClause.data();
                    }
		}
	      break;
	      
	      case ComTdbExeUtilGetMetadataInfo::INDEXES_IN_SCHEMA_:
		{
                  if (getMItdb().withNamespace())
                    {
                      qs = getTrafIndexesInSchemaWithNamespaceQuery;
                      sizeOfqs = sizeof(getTrafIndexesInSchemaWithNamespaceQuery);
                      strcpy(textType, "9");
                    }
                  else
                    {
                      qs = getTrafIndexesInSchemaQuery;
                      sizeOfqs = sizeof(getTrafIndexesInSchemaQuery);
                    }

                  if (doPrivCheck)
                    {
                      privFromClause = addFromClause(authList, true);
                      privWhereClause = addWhereClause(true, "I.base_table_uid");
                    }

                  param_[0] = catSchValue;
                  param_[1] = endQuote;
                  param_[2] = cat;
                  param_[3] = sch;
                  param_[4] = indexes;
                  param_[5] = cat;
                  param_[6] = sch;
                  param_[7] = tab;
                  if (getMItdb().withNamespace())
                    {
                      param_[8] = cat;
                      param_[9] = sch;
                      param_[10] = text;
                      param_[11] = textType;
                      param_[12] = (char *)privFromClause.data();
                      param_[13] = getMItdb().cat_;
                      param_[14] = getMItdb().sch_;
                      param_[15] = (char *)privWhereClause.data();
                    }
                  else
                    {
                      param_[8] = (char *)privFromClause.data();
                      param_[9] = getMItdb().cat_;
                      param_[10] = getMItdb().sch_;
                      param_[11] = (char *)privWhereClause.data();
                    }
		}
	      break;
	      
	      case ComTdbExeUtilGetMetadataInfo::VIEWS_IN_CATALOG_:
		{
		  qs = getTrafViewsInCatalogQuery;
		  sizeOfqs = sizeof(getTrafViewsInCatalogQuery);

                  if (doPrivCheck)
                    {
                      privFromClause = addFromClause(authList, true /*do union*/);
                      privWhereClause = addWhereClause(true, "T.object_uid");
                    }

		  param_[0] = cat;
		  param_[1] = sch;
		  param_[2] = tab;
                  param_[3] = (char *)privFromClause.data();
		  param_[4] = getMItdb().cat_;
                  param_[5] = (char *)privWhereClause.data();
		}
	      break;
	      
              case ComTdbExeUtilGetMetadataInfo::HIVE_REG_TABLES_IN_CATALOG_:
              case ComTdbExeUtilGetMetadataInfo::HIVE_REG_VIEWS_IN_CATALOG_:
              case ComTdbExeUtilGetMetadataInfo::HIVE_REG_SCHEMAS_IN_CATALOG_:
              case ComTdbExeUtilGetMetadataInfo::HIVE_REG_OBJECTS_IN_CATALOG_:
              {
                qs = getHiveRegObjectsInCatalogQuery;
                sizeOfqs = sizeof(getHiveRegObjectsInCatalogQuery);

                if (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::HIVE_REG_TABLES_IN_CATALOG_)
                {
                   strcpy(hiveGetType, "tables");
                   str_sprintf(hiveObjType, " (object_type = '%s') ",
                                COM_BASE_TABLE_OBJECT_LIT);
                    }
                  else if (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::HIVE_REG_VIEWS_IN_CATALOG_)
                   {
                      strcpy(hiveGetType, "views");
                      str_sprintf(hiveObjType, " (object_type = '%s') ",
                                  COM_VIEW_OBJECT_LIT);
                    }
                  else if (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::HIVE_REG_SCHEMAS_IN_CATALOG_)
                    {
                      strcpy(hiveGetType, "schemas");
                      str_sprintf(hiveObjType, " (object_type = '%s') ",
                                  COM_SHARED_SCHEMA_OBJECT_LIT);
                    }
                  else
                    {
                      strcpy(hiveGetType, "objects");
                      str_sprintf(hiveObjType, " (object_type = '%s' or object_type = '%s' or object_type = '%s' ) ",
                                  COM_BASE_TABLE_OBJECT_LIT, 
                                  COM_VIEW_OBJECT_LIT,
                                  COM_SHARED_SCHEMA_OBJECT_LIT);
                    }
                    
                  if (doPrivCheck)
                    privWhereClause = getGrantedPrivCmd(authList, cat, false);

                  param_[0] = cat;
                  param_[1] = sch;
                  param_[2] = tab;
                  param_[3] = hiveObjType;
                  param_[4] = (char *)privWhereClause.data();
                  param_[5] = hiveSysCat;
                  param_[6] = hiveGetType,
                  param_[7] = hiveSysCat;

		}
	      break;

	     case ComTdbExeUtilGetMetadataInfo::HBASE_REG_TABLES_IN_CATALOG_:
               {
                 qs = getHBaseRegTablesInCatalogQuery;
                 sizeOfqs = sizeof(getHBaseRegTablesInCatalogQuery);

                if (doPrivCheck)
                   privWhereClause = getGrantedPrivCmd(authList, cat, false);

                 param_[0] = cat;
                 param_[1] = sch;
                 param_[2] = tab;
                 param_[3] = (char *)privWhereClause.data();
               }
	      break;
	      
              case ComTdbExeUtilGetMetadataInfo::HIVE_EXT_TABLES_IN_CATALOG_:
                {
                  qs = getHiveExtTablesInCatalogQuery;
                  sizeOfqs = sizeof(getHiveExtTablesInCatalogQuery);

                  if (doPrivCheck)
                    privWhereClause = getGrantedPrivCmd(authList, cat, false);

                  strcpy(hiveObjType, COM_BASE_TABLE_OBJECT_LIT);
                  strcpy(hiveGetType, "tables");

                  param_[0] = hiveSysCat;
                  param_[1] = cat;
                  param_[2] = sch;
                  param_[3] = tab;
                  param_[4] = hiveObjType;
                  param_[5] = (char *)privWhereClause.data();
                  param_[6] = hiveSysCat;
                  param_[7] = hiveGetType,
                  param_[8] = hiveSysCat;

                }
              break;


	      case ComTdbExeUtilGetMetadataInfo::HBASE_MAP_TABLES_IN_CATALOG_:
		{
		  qs = getHBaseMapTablesInCatalogQuery;
		  sizeOfqs = sizeof(getHBaseMapTablesInCatalogQuery);
		  param_[0] = cat;
		  param_[1] = sch;
		  param_[2] = tab;
                  param_[3] = cat;
                  param_[4] = sch;
                  param_[5] = text;

		}
	      break;
	      
	      case ComTdbExeUtilGetMetadataInfo::VIEWS_IN_SCHEMA_:
		{
		  qs = getTrafViewsInSchemaQuery;
		  sizeOfqs = sizeof(getTrafViewsInSchemaQuery);

                  if (doPrivCheck)
                    {
                      privFromClause = addFromClause(authList, true);
                      privWhereClause = addWhereClause(true, "T.object_uid");
                    }

		  param_[0] = cat;
		  param_[1] = sch;
		  param_[2] = tab;
                  param_[3] = (char *)privFromClause.data();
		  param_[4] = getMItdb().cat_;
		  param_[5] = getMItdb().sch_;
                  param_[6] = (char *)privWhereClause.data();
		}
	      break;

	      case ComTdbExeUtilGetMetadataInfo::TABLES_IN_VIEW_:
	      case ComTdbExeUtilGetMetadataInfo::VIEWS_IN_VIEW_:
	      case ComTdbExeUtilGetMetadataInfo::OBJECTS_IN_VIEW_:
		{
                  qs = getTrafObjectsInViewQuery;
                  sizeOfqs = sizeof(getTrafObjectsInViewQuery);

                 // If user has privs on the view, they can see referenced objects
                 // even if they don't have privileges on the referenced objects
                 if (doPrivCheck)
                    {
                       privFromClause = addFromClause(authList, true);
                       privWhereClause = addWhereClause(true, "T2.object_uid");
                    }

                  param_[0] = cat;
                  param_[1] = sch;
                  param_[2] = view_usage;
                  param_[3] = cat;
                  param_[4] = sch;
                  param_[5] = tab;
                  param_[6] = cat;
                  param_[7] = sch;
                  param_[8] = tab;
                  param_[9] = (char *)privFromClause.data();
                  param_[10] = getMItdb().cat_;
                  param_[11] = getMItdb().sch_;
                  param_[12] = getMItdb().obj_;
                  param_[13] = (char *)privWhereClause.data();

                  numOutputEntries_ = 4;
		}
	      break;

              case ComTdbExeUtilGetMetadataInfo::INDEXES_ON_TABLE_:
                {
                  qs = getTrafIndexesOnTableQuery;
                  sizeOfqs = sizeof(getTrafIndexesOnTableQuery);
                 if (doPrivCheck)
                    {
                      privFromClause = addFromClause(authList, true);
                      privWhereClause = addWhereClause(true, "O.object_uid");
                    }

                  if (getMItdb().withNamespace())
                    strcpy(textType, "9");
                  else
                    strcpy(textType, "-1");

                  param_[0] = catSchValue;
                  param_[1] = endQuote;
                  param_[2] = cat;
                  param_[3] = sch;
		  param_[4] = tab;
                  param_[5] = cat;
                  param_[6] = sch;
		  param_[7] = text;
                  param_[8] = textType;
		  param_[9] = cat;
		  param_[10] = sch;
		  param_[11] = indexes;
		  param_[12] = cat;
		  param_[13] = sch;
		  param_[14] = tab;
                  param_[15] = (char *)privFromClause.data();
		  param_[16] = getMItdb().cat_;
		  param_[17] = getMItdb().sch_;
		  param_[18] = getMItdb().obj_;
                  param_[19] = (char *)privWhereClause.data();

                }
                break;

	      case ComTdbExeUtilGetMetadataInfo::VIEWS_ON_TABLE_:
	      case ComTdbExeUtilGetMetadataInfo::VIEWS_ON_VIEW_:
		{
                  qs = getTrafViewsOnObjectQuery;
                  sizeOfqs = sizeof(getTrafViewsOnObjectQuery);

                  // If user has privs on object, they can see referencing views
                  // even if they don't have privileges on the referencing views
                  if (doPrivCheck)
                    {
                      privFromClause = addFromClause(authList, true);
                      privWhereClause = addWhereClause(true, "T1.OBJECT_UID");
                    }

                  param_[0] = cat;
                  param_[1] = sch;
                  param_[2] = tab;
                  param_[3] = cat;
                  param_[4] = sch;
                  param_[5] = view_usage;
                  param_[6] = cat;
                  param_[7] = sch;
                  param_[8] = tab;
                  param_[9] = (char *)privFromClause.data();
                  param_[10] = getMItdb().cat_;
                  param_[11] = getMItdb().sch_;
                  param_[12] = getMItdb().obj_;
                  if (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::VIEWS_ON_TABLE_)
                    var = " and T1.object_type = 'BT' ";
                  param_[13] = (char *)var.data();
                  param_[14] = (char *)privWhereClause.data();

                  numOutputEntries_ = 3;
		}
	      break;

	      case ComTdbExeUtilGetMetadataInfo::SCHEMAS_IN_CATALOG_:
		{
		  qs = getTrafSchemasInCatalogQuery;
		  sizeOfqs = sizeof(getTrafSchemasInCatalogQuery);

                  if (doPrivCheck)
                    {
                      privFromClause = addFromClause(authList, false);
                      privWhereClause = addWhereClause(false, "T.object_uid");
                    }

		  param_[0] = cat;
		  param_[1] = sch;
		  param_[2] = tab;
                  param_[3] = (char *) privFromClause.data();
		  param_[4] = getMItdb().cat_;
                  param_[5] = (char *) privWhereClause.data();
		}
	      break;

              case ComTdbExeUtilGetMetadataInfo::SCHEMAS_FOR_USER_:
                {
                  qs = getTrafSchemasForAuthIDQuery;
                  sizeOfqs = sizeof(getTrafSchemasForAuthIDQuery);

                  Int32 authID = *currContext->getDatabaseUserID();
                  if (!(strcmp(getMItdb().getParam1(), currContext->getDatabaseUserName()) == 0))
                    authID = getAuthID(getMItdb().getParam1(), cat, sch, auths);

                  // if incorrect auth type, return error
                  if (!CmpSeabaseDDLauth::isUserID(authID))
                    {
                      ExRaiseSqlError(getHeap(), &diagsArea_, -CAT_IS_NOT_A_USER, 
                        NULL, NULL, NULL,
                        getMItdb().getParam1());
                      step_ = HANDLE_ERROR_;
                      break;
                    }

                  // Cannot get schemas for user other than the current user
                  if (doPrivCheck && authID != ComUser::getCurrentUser())
                    {
                      ExRaiseSqlError(getHeap(), &diagsArea_, -CAT_NOT_AUTHORIZED);
                      step_ = HANDLE_ERROR_;
                      break;
                    }
                
                  // Get list of roles assigned to user, return all schemas
                  // owned by user and user's roles
                  bool hasRootRole = false;
                  if (authID == ComUser::getCurrentUser())
                    {
                      if (!ComUser::isRootUserID() && !ComUser::currentUserHasRole(ROOT_ROLE_ID))
                      {
                        privFromClause = addFromClause(authList, false);
                        privWhereClause = addWhereClause(false, "T.object_uid");
                      }
                      else
                        hasRootRole = true;
                    }
                  else
                    {
                      char *userRoleList = getRoleList(hasRootRole, authID, cat, pmsch, role_usage);
                      if (!userRoleList)
                        {
                          // Unable to read metadata
                          ExRaiseSqlError(getHeap(), &diagsArea_, -8001);
                          step_ = HANDLE_ERROR_;
                          break;
                        }

                      if (!hasRootRole && !ComUser::isRootUserID(authID))
                      {
                        privFromClause = addFromClause(userRoleList, false);
                        privWhereClause = addWhereClause(false, "T.object_uid");
                      }

                      NADELETEBASIC(userRoleList, getHeap());
                    }

                  // get schemas from hive, if elevated user
                  if ((CmpCommon::getDefault(TRAF_GET_HIVE_OBJECTS) == DF_ON) &&
                      (ComUser::isRootUserID(authID) || hasRootRole))
                        privWhereClause += " union select 'HIVE', upper(trim(schema_name)) from table(hivemd(schemas))"; 

                  param_[0] = cat;
                  param_[1] = sch;
                  param_[2] = tab;
                  param_[3] = (char *) privFromClause.data();
                  param_[4] = (char *) privWhereClause.data();

                  numOutputEntries_ = 2;
		}
	      break;

              case ComTdbExeUtilGetMetadataInfo::SCHEMAS_FOR_ROLE_:
                {
                  qs = getTrafSchemasForAuthIDQuery;
                  sizeOfqs = sizeof(getTrafSchemasForAuthIDQuery);

                  Int32 authID = *currContext->getDatabaseUserID();
                  if (!(strcmp(getMItdb().getParam1(), currContext->getDatabaseUserName()) == 0))
                    authID = getAuthID(getMItdb().getParam1(), cat, sch, auths);

                  // if incorrect auth type, return error
                  if (!CmpSeabaseDDLauth::isRoleID(authID) && !ComUser::isPublicUserID(authID))
                    {
                      ExRaiseSqlError(getHeap(), &diagsArea_, -CAT_IS_NOT_A_ROLE,
                          NULL, NULL, NULL,
                          getMItdb().getParam1());
                      step_ = HANDLE_ERROR_;
                      break;
                    }

                  // Cannot get schemas if current user not granted role
                  if (doPrivCheck && !ComUser::currentUserHasRole(authID))
                    {
                      ExRaiseSqlError(getHeap(), &diagsArea_, -CAT_NOT_AUTHORIZED);
                      step_ = HANDLE_ERROR_;
                      break;
                    }

                  // Special case - authID is DB__ROOTROLE, then return all schemas
                  if (authID == ROOT_ROLE_ID)
                    {
                      if (CmpCommon::getDefault(TRAF_GET_HIVE_OBJECTS) == DF_ON)
                        privWhereClause += " union select 'HIVE', upper(trim(schema_name)) from table(hivemd(schemas))"; 
                    }

                  else
                    {
                      // Return schemas that are owned by the specified role -> authID = roleID
                      char buf[30];
                      str_sprintf(buf, "(%d)", authID);
                      privFromClause = addFromClause(buf, false);
                      privWhereClause = addWhereClause(false, "T.object_uid");
                    }

                  // get schemas from hive, if elevated user

                  param_[0] = cat;
                  param_[1] = sch;
                  param_[2] = tab;
                  param_[3] = (char *) privFromClause.data();
                  param_[4] = (char *) privWhereClause.data();

                  numOutputEntries_ = 2;
                }
                break;

              case ComTdbExeUtilGetMetadataInfo::USERS_:
                {
                  qs = getTrafUsers;
                  sizeOfqs = sizeof(getTrafUsers);

                  if (doPrivCheck)
                  {
                    char buf[authList.length() + 100];
                    str_sprintf(buf, " and auth_id in %s", authList.data());
                    privWhereClause = buf;
                    if (msg_license_multitenancy_enabled())
                      {
                        str_sprintf (buf, " or auth_creator in %s and auth_type = 'U'", authList.data());
                        privWhereClause += buf;
                      }
                  }

                  param_[0] = cat;
                  param_[1] = sch;
                  param_[2] = auths;
                  param_[3] = (char *) privWhereClause.data();
                  numOutputEntries_ = 2;
		}
                break;

              case ComTdbExeUtilGetMetadataInfo::PACKAGES_IN_SCHEMA_:
                {
                  qs = getTrafPackagesInSchemaQuery;
                  sizeOfqs = sizeof(getTrafPackagesInSchemaQuery);

                  if (doPrivCheck)
                    {
                       privFromClause = addFromClause(authList, true);
                       privWhereClause = addWhereClause(true, "T.object_uid");
                    }

		  param_[0] = cat;
		  param_[1] = sch;
		  param_[2] = tab;
                  param_[3] = (char *) privFromClause.data();
		  param_[4] = getMItdb().cat_;
		  param_[5] = getMItdb().sch_;
                  param_[6] = (char *) privWhereClause.data();
                }
                break ;

              case ComTdbExeUtilGetMetadataInfo::PROCEDURES_IN_SCHEMA_:
                {
                  qs = getTrafProceduresInSchemaQuery;
                  sizeOfqs = sizeof(getTrafProceduresInSchemaQuery);

                  if (doPrivCheck)
                    {
                      privFromClause = addFromClause(authList, true);
                      privWhereClause = addWhereClause(true, "T.object_uid");
                    }

		  param_[0] = cat;
		  param_[1] = sch;
		  param_[2] = tab;
                  param_[3] = cat;
		  param_[4] = sch;
		  param_[5] = routine;
                  param_[6] = (char *) privFromClause.data();
		  param_[7] = getMItdb().cat_;
		  param_[8] = getMItdb().sch_;
                  param_[9] = (char *) privWhereClause.data();
                }
                break ;

              case ComTdbExeUtilGetMetadataInfo::TRIGGERS_IN_SCHEMA_:
		{
		  qs = getTrafTriggersInSchemaQuery;
		  sizeOfqs = sizeof(getTrafTriggersInSchemaQuery);

		  if (doPrivCheck)
		    privWhereClause = getGrantedPrivCmd(authList, cat, true, NAString ("T."));

		  param_[0] = cat;
		  param_[1] = sch;
		  param_[2] = tab;
		  param_[3] = cat;
		  param_[4] = sch;
		  param_[5] = routine;
		  param_[6] = getMItdb().cat_;
		  param_[7] = getMItdb().sch_;
		  param_[8] = (char *) privWhereClause.data();
		}
		break ;

               case ComTdbExeUtilGetMetadataInfo::LIBRARIES_IN_SCHEMA_:
                {
                  qs = getTrafLibrariesInSchemaQuery;
                  sizeOfqs = sizeof(getTrafLibrariesInSchemaQuery);

                  if (doPrivCheck)
                  {
                    // Only return libraries where the current user has been
                    // granted privilege on library of routine in the library
                    // See LIBRARIES_FOR_USER for more details on query
                    Int32 stmtSize = ((authList.length() * 4) + 
                                      (strlen(cat)*5) + 
                                      (strlen(pmsch)*4) +
                                      (strlen(objPrivs)*2) + (strlen(schPrivs)*2) + 
                                      strlen(sch) + strlen(library_usage) + 1000);
                    char buf[stmtSize];
                    snprintf
                      (buf, stmtSize, "and T.object_uid in "
                       "(select object_uid from %s.\"%s\".%s where grantee_id in %s "
                       " or exists (select schema_uid from %s.\"%s\".%s p"
                       "   where ((T.catalog_name || '.' || T.schema_name) = "
                       "     replace (p.schema_name, '\"', ''))"
                       "   and grantee_id in %s ) "
                       " union (select using_library_uid from %s.\"%s\".%s "
                       "   where used_udr_uid in "
                       "     (select object_uid from %s.\"%s\".%s where grantee_id in %s "
                       "      or exists (select schema_uid from %s.\"%s\".%s p"
                       "        where ((T.catalog_name || '.' || T.schema_name) = "
                       "          replace (p.schema_name, '\"', ''))"
                       "        and grantee_id in %s )))) ",
                       cat, pmsch, objPrivs, authList.data(),
                       cat, pmsch, schPrivs, authList.data(),
                       cat, sch, library_usage, 
                       cat, pmsch, objPrivs, authList.data(), 
                       cat, pmsch, schPrivs, authList.data());
                    privWhereClause = buf;
                  }

                  param_[0] = cat;
                  param_[1] = sch;
                  param_[2] = tab;
                  param_[3] = getMItdb().cat_;
                  param_[4] = getMItdb().sch_;
                  param_[5] = (char *) privWhereClause.data();
                }
                break ;

              case ComTdbExeUtilGetMetadataInfo::FUNCTIONS_IN_SCHEMA_:
                {
                  qs = getTrafFunctionsInSchemaQuery;
                  sizeOfqs = sizeof(getTrafFunctionsInSchemaQuery);

                  if (doPrivCheck)
                    {
                      privFromClause = addFromClause(authList, true);
                      privWhereClause = addWhereClause(true, "T.object_uid");
                    }

		  param_[0] = cat;
		  param_[1] = sch;
		  param_[2] = tab;
                  param_[3] = cat;
		  param_[4] = sch;
		  param_[5] = routine;
                  param_[6] = (char *) privFromClause.data();
		  param_[7] = getMItdb().cat_;
		  param_[8] = getMItdb().sch_;
                  param_[9] = (char *) privWhereClause.data();
                }
                break ;

	      case ComTdbExeUtilGetMetadataInfo::TABLE_FUNCTIONS_IN_SCHEMA_:
                {
                  qs = getTrafTableFunctionsInSchemaQuery;
                  sizeOfqs = sizeof(getTrafTableFunctionsInSchemaQuery);

                  if (doPrivCheck)
                    {
                      privFromClause = addFromClause(authList, true);
                      privWhereClause = addWhereClause(true, "T.object_uid");
                    }

		  param_[0] = cat;
		  param_[1] = sch;
		  param_[2] = tab;
                  param_[3] = cat;
		  param_[4] = sch;
		  param_[5] = routine;
                  param_[6] = (char *) privFromClause.data();
		  param_[7] = getMItdb().cat_;
		  param_[8] = getMItdb().sch_;
                  param_[9] = (char *) privWhereClause.data();
                }
                break ;

              case ComTdbExeUtilGetMetadataInfo::PROCEDURES_FOR_USER_:
              case ComTdbExeUtilGetMetadataInfo::FUNCTIONS_FOR_USER_:
              case ComTdbExeUtilGetMetadataInfo::TABLE_FUNCTIONS_FOR_USER_:
                {
                  qs = getTrafRoutinesForAuthQuery;
                  sizeOfqs = sizeof(getTrafRoutinesForAuthQuery);

                  // Get the authID associated with the specified user
                  Int32 authID = *currContext->getDatabaseUserID();
                  if (!(strcmp(getMItdb().getParam1(), currContext->getDatabaseUserName()) == 0))
                    authID = getAuthID(getMItdb().getParam1(), cat, sch, auths);

                  // If not a user, we are done, don't return data
                  if (!CmpSeabaseDDLauth::isUserID(authID))
                    {
                      ExRaiseSqlError(getHeap(), &diagsArea_, -CAT_IS_NOT_A_USER,
                          NULL, NULL, NULL,
                          getMItdb().getParam1());
                      step_ = HANDLE_ERROR_;
                      break;
                    }

                  // Non elevated user cannot view routines for another user
                  if (doPrivCheck && authID != ComUser::getCurrentUser())
                    {
                      ExRaiseSqlError(getHeap(), &diagsArea_, -CAT_NOT_AUTHORIZED);
                      step_ = HANDLE_ERROR_;
                      break;
                    }
                  
                  // Determine routine type
                  if (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::PROCEDURES_FOR_USER_)
                    var = COM_PROCEDURE_TYPE_LIT;
                  else if (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::FUNCTIONS_FOR_USER_)
                    var = COM_SCALAR_UDF_TYPE_LIT;
                  else
                    var = COM_TABLE_UDF_TYPE_LIT;

                  // Limit results to privileges allowed for specified user

                  if (authID == ComUser::getCurrentUser())
                  {
                    if (!ComUser::isRootUserID() && !ComUser::currentUserHasRole(ROOT_ROLE_ID))
                      {
                        privFromClause = addFromClause(authList, true);
                        privWhereClause = addWhereClause(true, "T.object_uid");
                      }
                  }
                  else
                    {
                      // getting privileges for anotheruser
                      bool hasRootRole = false;
                      char *userRoleList = getRoleList(hasRootRole, authID, cat, pmsch, role_usage);
                      if (!userRoleList)
                        {
                          // Unable to read metadata
                          ExRaiseSqlError(getHeap(), &diagsArea_, -8001);
                          step_ = HANDLE_ERROR_;
                          break;
                        }

                      if (!ComUser::isRootUserID(authID) && !hasRootRole)
                        {
                          privFromClause = addFromClause(userRoleList, true);
                          privWhereClause = addWhereClause(true, "T.object_uid");
                        }
                      NADELETEBASIC(userRoleList, getHeap());
                    }

                  param_[0] = cat;
                  param_[1] = sch;
                  param_[2] = tab;
                  param_[3] = cat;
                  param_[4] = sch;
                  param_[5] = routine;
                  param_[6] = (char *) privFromClause.data();
                  param_[7] = getMItdb().cat_;
                  param_[8] = (char *)var.data();
                  param_[9] = (char *) privWhereClause.data();
                }
                break ;

              case ComTdbExeUtilGetMetadataInfo::PROCEDURES_FOR_ROLE_:
              case ComTdbExeUtilGetMetadataInfo::FUNCTIONS_FOR_ROLE_:
              case ComTdbExeUtilGetMetadataInfo::TABLE_FUNCTIONS_FOR_ROLE_:
                {
                  qs = getTrafRoutinesForAuthQuery;
                  sizeOfqs = sizeof(getTrafRoutinesForAuthQuery);

                  // Get the authID associated with the specified role
                  Int32 authID = *currContext->getDatabaseUserID();
                  if (!(strcmp(getMItdb().getParam1(), currContext->getDatabaseUserName()) == 0))
                    authID = getAuthID(getMItdb().getParam1(), cat, sch, auths);

                  // If not a role, we are done, don't return data
                  if (!CmpSeabaseDDLauth::isRoleID(authID) && !ComUser::isPublicUserID(authID)) 
                    {
                      ExRaiseSqlError(getHeap(), &diagsArea_, -CAT_IS_NOT_A_ROLE, 
                          NULL, NULL, NULL,
                          getMItdb().getParam1());
                      step_ = HANDLE_ERROR_;
                      break;
                    }

                  // non elevated user has to be granted role
                  if (doPrivCheck && !ComUser::currentUserHasRole(authID))
                    {
                      // No priv
                      ExRaiseSqlError(getHeap(), &diagsArea_, -CAT_NOT_AUTHORIZED);
                      step_ = HANDLE_ERROR_;
                      break;
                    }

                  // determine routine type
                  if (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::PROCEDURES_FOR_ROLE_)
                    var = COM_PROCEDURE_TYPE_LIT;
                  else if (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::FUNCTIONS_FOR_ROLE_)
                    var = COM_SCALAR_UDF_TYPE_LIT;
                  else 
                    var = COM_TABLE_UDF_TYPE_LIT;

                  if (authID != ROOT_ROLE_ID)
                    {
                      // Only return rows where role (authID) has been granted privs
                      char buf[30];
                      str_sprintf(buf, "(%d)", authID);
                      privFromClause = addFromClause(buf, true);
                      privWhereClause = addWhereClause(true, "T.object_uid");
                    }

                  param_[0] = cat;
                  param_[1] = sch;
                  param_[2] = tab;
                  param_[3] = cat;
                  param_[4] = sch;
                  param_[5] = routine;
                  param_[6] = (char *) privFromClause.data();
                  param_[7] = getMItdb().cat_;
                  param_[8] = (char *)var.data();
                  param_[9] = (char *) privWhereClause.data();
                }
                break ;
                
              case ComTdbExeUtilGetMetadataInfo::PROCEDURES_FOR_LIBRARY_:
              case ComTdbExeUtilGetMetadataInfo::FUNCTIONS_FOR_LIBRARY_:
              case ComTdbExeUtilGetMetadataInfo::TABLE_FUNCTIONS_FOR_LIBRARY_:
                {
                  qs = getTrafProceduresForLibraryQuery;
                  sizeOfqs = sizeof(getTrafProceduresForLibraryQuery);

                  if (doPrivCheck)
                    {
                      privFromClause = addFromClause(authList, true);
                      privWhereClause = addWhereClause(true, "T1.object_uid");
                    }

                  param_[0] = cat;
                  param_[1] = sch;
                  param_[2] = tab;
                  param_[3] = cat;
                  param_[4] = sch;
                  param_[5] = routine;
                  param_[6] = cat;
                  param_[7] = sch;
                  param_[8] = tab;
                  param_[9] = (char *) privFromClause.data();
                  param_[10] = getMItdb().cat_;
                  param_[11] = getMItdb().sch_;
                  param_[12] = getMItdb().obj_;
                  if (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::PROCEDURES_FOR_LIBRARY_)
                    var = " R.udr_type = 'P ' ";
                  else if (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::FUNCTIONS_FOR_LIBRARY_)
                    var = " R.udr_type = 'F ' ";
                  else
                    var = " R.udr_type = 'T ' ";
                  param_[13] = (char *) var.data();
                  param_[14] = (char *) privWhereClause.data();
                }
                break ;
                
              case ComTdbExeUtilGetMetadataInfo::ROLES_:
                {
                  qs = getTrafRoles;

                  sizeOfqs = sizeof(getTrafRoles);

                  if (doPrivCheck)
                  {
                    // return roles granted to current user
                    char buf[authList.length() + 100];
                    str_sprintf(buf, " and auth_id in %s", authList.data());
                    privWhereClause = buf;
                    if (msg_license_multitenancy_enabled())
                      {
                        str_sprintf (buf, " or (auth_creator in %s and auth_type = 'R')", authList.data());
                        privWhereClause += buf;
                      }
                  }

                  param_[0] = cat;
                  param_[1] = sch;
                  param_[2] = auths;
                  param_[3] = (char *) privWhereClause.data();
                }
              break;

              case ComTdbExeUtilGetMetadataInfo::ACTIVE_ROLES_:
                {
                  qs = getTrafActiveRoles;
                  sizeOfqs = sizeof(getTrafActiveRoles);

                  privWhereClause =  "(-1, -2)";
                  char buf[100];
                  Int32 numRoles;
                  Int32 *roleList;
                  Int32 *granteeList;
                  if (currContext->getRoleList(numRoles, roleList, granteeList) == SUCCESS)
                  {
                    // should be at least one role (PUBLIC)
                    if (numRoles == 0)
                      {
                        ExRaiseSqlWarning(getHeap(), &diagsArea_, (ExeErrorCode)1057);
                      }
                    else
                    {
                      char authIDAsChar[sizeof(Int32)+10];
                      for (Int32 i = 0; i < numRoles; i++)
                      {
                        sprintf(buf, ", (%d, %d)",
                                roleList[i], granteeList[i]); 
                        privWhereClause += buf;
                      }
                    }
                  }
                  param_[0] = cat;
                  param_[1] = sch;
                  param_[2] = auths;
                  param_[3] = cat;
                  param_[4] = sch;
                  param_[5] = auths;
                  param_[6] = (char *) privWhereClause.data();

                  numOutputEntries_ = 2;

                }
              break;

              case ComTdbExeUtilGetMetadataInfo::NODES_FOR_TENANT_:
                {
                  qs = getTrafResourceUsages1;
                  sizeOfqs = sizeof(getTrafResourceUsages1);

                  if (doPrivCheck)
                  {
                    // If not the current tenant, return error
                    char buf[authList.length() + 100];
                    if (strcmp(getMItdb().getParam1(), currContext->getTenantName()) != 0)
                    {
                      NAString msg (" You are only authorized to get nodes for the current tenant (");
                      msg += currContext->getTenantName();
                      msg += ").";
                      ExRaiseSqlError(getHeap(), &diagsArea_, -4218,
                          NULL, NULL, NULL, "NODES FOR TENANT", msg.data());
                      step_ = HANDLE_ERROR_;
                      break;
                    }
                  }

                  param_[0] = cat;
                  param_[1] = tsch;
                  param_[2] = resource_usage;
                  param_[3] = getMItdb().getParam1();
                  param_[4] = (char *) privWhereClause.data();
                }
                break;

              case ComTdbExeUtilGetMetadataInfo::NODES_IN_RGROUP_:
                {
                  qs = getTrafResourceUsages2;
                  sizeOfqs = sizeof(getTrafResourceUsages2);

                  if (doPrivCheck)
                  {
                    // current user must be the rgroup owner or the rgroup
                    // must be assigned to the current tenant
                    char buf[COL_MAX_TABLE_LEN*3 + 200];
                    str_sprintf(buf, " and (resource_uid in (select resource_uid "
                                " from %s.\"%s\".%s where resource_creator = %d) or "
                                " (resource_uid in (select usage_uid from %s.\"%s\".%s "
                                " where tenant_id = %d))) ",
                                cat, tsch, resources, ComUser::getCurrentUser(),
                                cat, tsch, tenant_usage,
                                ComTenant::getCurrentTenantID());
                     privWhereClause = buf;
                  }

                  param_[0] = cat;
                  param_[1] = tsch;
                  param_[2] = resource_usage;
                  param_[3] = getMItdb().getParam1();
                  param_[4] = (char *) privWhereClause.data();
                }
                break;

              case ComTdbExeUtilGetMetadataInfo::RGROUPS_:
                {
                  qs = getTrafResources;
                  sizeOfqs = sizeof(getTrafResources);

                  NAString resourceType("G");
                  if (doPrivCheck)
                  {
                     char buf[COL_MAX_TABLE_LEN*3 + 200];
                     str_sprintf(buf, " and (resource_creator = %d or "
                                 "(resource_uid in (select usage_uid "
                                 "from %s.\"%s\".%s "
                                 "where tenant_id = %d))) ", ComUser::getCurrentUser(),
                                 cat, tsch, tenant_usage,
                                 ComTenant::getCurrentTenantID());
                     privWhereClause = buf;
                  }

                  param_[0] = cat;
                  param_[1] = tsch;
                  param_[2] = resources;
                  param_[3] = (char *) resourceType.data();
                  param_[4] = (char *) privWhereClause.data();
                }
                break;

              case ComTdbExeUtilGetMetadataInfo::NODES_:
                {
                  qs = getTrafResources;
                  sizeOfqs = sizeof(getTrafResources);

                  NAString resourceType("N");
                  if (doPrivCheck)
                  {
                     char buf[COL_MAX_TABLE_LEN*3 + 200];
                     str_sprintf(buf, " and resource_uid in (select resource_uid from %s.\"%s\".%s "
                                 "where usage_uid = %d)",
                                 cat, tsch, resource_usage, 
                                 ComTenant::getCurrentTenantID());
                     privWhereClause = buf;
                  }

                  param_[0] = cat;
                  param_[1] = tsch;
                  param_[2] = resources;
                  param_[3] = (char *) resourceType.data();
                  param_[4] = (char *) privWhereClause.data();
                }
                break;

              case ComTdbExeUtilGetMetadataInfo::TENANTS_:
                {
                  qs = getTrafTenants;

                  if (doPrivCheck)
                    {
                      char buf[1000];
                      str_sprintf(buf, " and auth_id in (select tenant_id from %s.\"%s\".%s "
                                  "where tenant_id = %d or admin_role_id in %s)",
                                  cat, tsch, tenants, ComTenant::getCurrentTenantID(), authList.data());
                      privWhereClause += buf;
                    }
                   
                  sizeOfqs = sizeof(getTrafTenants);
                  param_[0] = cat;
                  param_[1] = sch;
                  param_[2] = auths;
                  param_[3] = (char *) privWhereClause.data();
                }
              break;

              case ComTdbExeUtilGetMetadataInfo::TENANTS_ON_RGROUP_:
                {
                  qs = getTrafTenantUsages;
                  sizeOfqs = sizeof(getTrafTenantUsages);

                  if (doPrivCheck)
                    {
                      char buf[1000];
                      str_sprintf(buf, " and (auth_id = %d or "
                                  " (%d in (select distinct resource_creator from "
                                  "%s.\"%s\".%s where resource_name = '%s')))",
                                  ComTenant::getCurrentTenantID(),
                                  ComUser::getCurrentUser(),
                                  cat, tsch, resources, getMItdb().getParam1());
                      privWhereClause += buf;
                    }
                  param_[0] = cat;
                  param_[1] = sch;
                  param_[2] = auths;           
                  param_[3] = cat;
                  param_[4] = tsch;
                  param_[5] = tenant_usage;           
                  param_[6] = cat;
                  param_[7] = tsch;
                  param_[8] = resources;           
                  param_[9] = getMItdb().getParam1();
                  param_[10] = (char *) privWhereClause.data();
                }
              break;

              case ComTdbExeUtilGetMetadataInfo::GROUPS_:
                {
                  qs = getTrafGroups;
                  sizeOfqs = sizeof(getTrafGroups);
                  if (doPrivCheck)
                    {
                      groupList.clear();
                      getGroupList(ComUser::getCurrentUsername(), groupList);
                      if (groupList.isNull())
                        // force the query to return no data to match behavior
                        // of other get commands
                        privWhereClause += "and auth_id = 0";
                      else
                        {
                          char buf[groupList.length() + 100];
                          str_sprintf(buf, " and auth_id in %s", groupList.data());
                          privWhereClause = buf;
                        }
                    }

                  param_[0] = cat;
                  param_[1] = sch;
                  param_[2] = auths;
                  param_[3] = (char *) privWhereClause.data();
                }
              break;

              case ComTdbExeUtilGetMetadataInfo::GROUPS_FOR_USER_:
                {
                  qs = getTrafGroups;
                  sizeOfqs = sizeof(getTrafGroups);

                  Int32 authID = *currContext->getDatabaseUserID();
                  if (!(strcmp(getMItdb().getParam1(), currContext->getDatabaseUserName()) == 0))
                    authID = getAuthID(getMItdb().getParam1(), cat, sch, auths);
                  if (!CmpSeabaseDDLauth::isUserID(authID))
                    {
                      ExRaiseSqlError(getHeap(), &diagsArea_, -CAT_IS_NOT_A_USER,
                          NULL, NULL, NULL,
                          getMItdb().getParam1());
                      step_ = HANDLE_ERROR_;
                      break;
                    }

                  //delete this when local auth is available
                  // User groups are defined in LDAP; DB__ROOT has no special privileges;
                  // therefore, DB__ROOT is treated the same as any other user.
                  // Same for users that have been assigned DB__ROOTROLE
                  //delete end

                  groupList.clear();
                  if (doPrivCheck)
                    {
                      if (strcmp(getMItdb().getParam1(), currContext->getDatabaseUserName()) != 0)
                        {
                          ExRaiseSqlError(getHeap(), &diagsArea_, -CAT_NOT_AUTHORIZED);
                          step_ = HANDLE_ERROR_;
                          break;
                        }

                      getGroupList(currContext->getDatabaseUserName(), groupList);
                    }
                  else
                     getGroupList(getMItdb().getParam1(), groupList);

                  if (groupList.isNull())
                    // force the query to return no data to match behavior
                    // of other get commands
                    privWhereClause += "and auth_id = 0";
                  else
                    {
                      char buf[groupList.length() + 100];
                      str_sprintf(buf, " and auth_id in %s", groupList.data());
                      privWhereClause = buf;
                    }

                  param_[0] = cat;
                  param_[1] = sch;
                  param_[2] = auths;
                  param_[3] = (char *) privWhereClause.data();
                }
              break;


              case ComTdbExeUtilGetMetadataInfo::GROUPS_FOR_TENANT_:
                {
                  qs = getTrafGroupTenantUsage;
                  sizeOfqs = sizeof(getTrafGroupTenantUsage);
                  if (doPrivCheck)
                    {
                      char buf[authList.length() + 100];
                      str_sprintf(buf, " and auth_id  = %d ", 
                                  ComTenant::getCurrentTenantID());
                      privWhereClause += buf;
                    }

                  NAString tenantName =  getMItdb().getParam1();

                  param_[0] = cat;
                  param_[1] = tsch;
                  param_[2] = tenant_usage;
                  param_[3] = cat;
                  param_[4] = sch;
                  param_[5] = auths;
                  param_[6] = cat;
                  param_[7] = sch;
                  param_[8] = auths;
                  param_[9] = (char *) tenantName.data();
                  param_[10] = (char *) privWhereClause.data();
                }
              break;

              case ComTdbExeUtilGetMetadataInfo::USERS_FOR_ROLE_:
                {
                  qs = getUsersForRoleQuery;
                  sizeOfqs = sizeof(getUsersForRoleQuery);

                  Int32 authID = *currContext->getDatabaseUserID();
                  if (!(strcmp(getMItdb().getParam1(), currContext->getDatabaseUserName()) == 0))
                    authID = getAuthID(getMItdb().getParam1(), cat, sch, auths);

                  if (!CmpSeabaseDDLauth::isRoleID(authID) && !ComUser::isPublicUserID(authID))
                    {
                      ExRaiseSqlError(getHeap(), &diagsArea_, -CAT_IS_NOT_A_ROLE,
                          NULL, NULL, NULL,
                          getMItdb().getParam1());
                      step_ = HANDLE_ERROR_;
                      break;
                    }

                  if (doPrivCheck)
                    {
                      // If user not granted role, return an error
                      if (!ComUser::currentUserHasRole(authID))
                        {
                           ExRaiseSqlError(getHeap(), &diagsArea_, -CAT_NOT_AUTHORIZED);
                           step_ = HANDLE_ERROR_;
                           break;
                         }

                       // limit users to the current user only
                       privWhereClause = " and grantee_name = CURRENT_USER ";
                     }

                  param_[0] = cat;
                  param_[1] = pmsch;
                  param_[2] = role_usage;
                  param_[3] = getMItdb().getParam1();
                  param_[4] = (char *) privWhereClause.data();
                }
              break;
                
              case ComTdbExeUtilGetMetadataInfo::ROLES_FOR_USER_:
              case ComTdbExeUtilGetMetadataInfo::ROLES_FOR_GROUP_:
                {
                  qs = getRolesForUserQuery;
                  sizeOfqs = sizeof(getRolesForUserQuery);

                  const char * currUser = currContext->getDatabaseUserName();
                  NABoolean rqstForCurrUser = (strcmp(getMItdb().getParam1(), currUser) == 0);
                  Int32 authID = (rqstForCurrUser) ? *currContext->getDatabaseUserID() :
                                 getAuthID(getMItdb().getParam1(), cat, sch, auths);

                  if (CmpSeabaseDDLauth::isUserID(authID)) 
                    {
                      if (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::ROLES_FOR_GROUP_)
                        {
                          ExRaiseSqlError(getHeap(), &diagsArea_, -CAT_IS_NOT_CORRECT_AUTHID,
                              NULL, NULL, NULL,
                              getMItdb().getParam1(), "user");
                          step_ = HANDLE_ERROR_;
                          break;
                        }
                    }

                  else if (CmpSeabaseDDLauth::isUserGroupID(authID)) 
                    {
                      if(getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::ROLES_FOR_USER_)
                        {
                          ExRaiseSqlError(getHeap(), &diagsArea_, -CAT_IS_NOT_CORRECT_AUTHID,
                              NULL, NULL, NULL,
                              getMItdb().getParam1(), "group");
                          step_ = HANDLE_ERROR_;
                          break;
                        }
                    }
                  else
                    {
                      ExRaiseSqlError(getHeap(), &diagsArea_, -CAT_IS_NOT_CORRECT_AUTHID,
                          NULL, NULL, NULL,
                          getMItdb().getParam1(), "user or group");
                      step_ = HANDLE_ERROR_;
                      break;
                    }

                  // include the list of groups
                  groupList.clear();
                  
                  if (CmpCommon::getDefault(ALLOW_ROLE_GRANTS_TO_GROUPS) == DF_ON ||
                       (CmpCommon::getDefault(ALLOW_ROLE_GRANTS_TO_GROUPS) == DF_SYSTEM &&
                        !getenv("SQLMX_REGRESS")
                       )
                      )
                    getGroupList((CmpSeabaseDDLauth::isUserID(authID)) ? getMItdb().getParam1() : currUser,
                                 groupList);

                  if (doPrivCheck)  
                    {
                      if (!rqstForCurrUser)
                        {
                          // User not the current user, return an error
                          if (CmpSeabaseDDLauth::isUserID(authID))
                            {
                              ExRaiseSqlError(getHeap(), &diagsArea_, -CAT_NOT_AUTHORIZED);
                              step_ = HANDLE_ERROR_;
                              break;
                            } 
 
                          // Group requested part of user's list of groups
                          else
                            {
                               if (groupList.isNull() || !userPartOfGroup(groupList, authID))
                                {
                                  ExRaiseSqlError(getHeap(), &diagsArea_, -CAT_NOT_AUTHORIZED);
                                  step_ = HANDLE_ERROR_;
                                  break;
                                }     
                            }
                        }
                    }

                  if (!groupList.isNull())
                    groupList.prepend(" or grantee_id in ");

                  param_[0] = cat;
                  param_[1] = pmsch;
                  param_[2] = role_usage;
                  param_[3] = getMItdb().getParam1();
                  param_[4] = (char *)groupList.data();
                }
              break;
              
              case ComTdbExeUtilGetMetadataInfo::PRIVILEGES_FOR_ROLE_:
                {
                  qs = getPrivsForAuthsQuery;
                  sizeOfqs = sizeof(getPrivsForAuthsQuery);

                  // Get the authID for the request
                  Int32 authID = *currContext->getDatabaseUserID();
                  if (!(strcmp(getMItdb().getParam1(), currContext->getDatabaseUserName()) == 0))
                    authID = getAuthID(getMItdb().getParam1(), cat, sch, auths);

                  char buf[authList.length() + 100];

                  // Verify that requested authID is actually a role
                  if (!CmpSeabaseDDLauth::isRoleID(authID) && !ComUser::isPublicUserID(authID))
                    {
                      ExRaiseSqlError(getHeap(), &diagsArea_, -CAT_IS_NOT_A_ROLE,
                          NULL, NULL, NULL,
                          getMItdb().getParam1());
                      step_ = HANDLE_ERROR_;
                      break;
                    }

                  // Non elevated users need to be granted role
                  if (doPrivCheck && !ComUser::currentUserHasRole(authID))
                    {
                      ExRaiseSqlError(getHeap(), &diagsArea_, -CAT_NOT_AUTHORIZED);
                      step_ = HANDLE_ERROR_;
                      break;
                    }

                  // Return qualified rows, return all if ROOT_ROLE_ID
                  if (authID != ROOT_ROLE_ID)
                    {
                      str_sprintf(buf, " = %d ", authID);
                      privWhereClause = buf;

                      // This request performs a union between 4 entities:
                      //  1. object_privileges table
                      //  2. schema_privileges table
                      //  3. column privileges table
                      //  4. hive metadata tables to retrieve column details
                      // The call to colPrivsFrag returns the required the union 
                      // statement(s) for items 3 and 4. See colPrivsFrag for details
                      if (colPrivsFrag(getMItdb().getParam1(), cat, privWhereClause, var1) < 0)
                      {
                          step_ = HANDLE_ERROR_;
                          break;
                      }
                      privWhereClause.prepend(" where grantee_id ");
                    }

                  // Union privileges between object, column and schema
                  // object privs
                  param_[0] = cat;
                  param_[1] = pmsch;
                  param_[2] = objPrivs;
                  param_[3] = (char *) privWhereClause.data();

                  // schema privs
                  param_[4] = cat;
                  param_[5] = pmsch;
                  param_[6] = schPrivs;
                  param_[7] = (char *) privWhereClause.data();

                  // column privs
                  param_[8] = (char *) var1.data();

                  numOutputEntries_ = 2;
                }
              break;

              case ComTdbExeUtilGetMetadataInfo::PRIVILEGES_FOR_USER_:
                {
                  qs = getPrivsForAuthsQuery;
                  sizeOfqs = sizeof(getPrivsForAuthsQuery);

                  // Get the authID for the request
                  Int32 authID = *currContext->getDatabaseUserID();
                  if (!(strcmp(getMItdb().getParam1(), currContext->getDatabaseUserName()) == 0))
                    authID = getAuthID(getMItdb().getParam1(), cat, sch, auths);

                  // Verify that authID is a user
                  if (!CmpSeabaseDDLauth::isUserID(authID))
                    {
                      ExRaiseSqlError(getHeap(), &diagsArea_, -CAT_IS_NOT_A_USER,
                          NULL, NULL, NULL,
                          getMItdb().getParam1());
                      step_ = HANDLE_ERROR_;
                      break;
                    }

                  // Non elevated user cannot get privileges for another user
                  char buf[authList.length() + 100];
                  if (doPrivCheck && authID != ComUser::getCurrentUser())
                    {
                      ExRaiseSqlError(getHeap(), &diagsArea_, -CAT_NOT_AUTHORIZED);
                      step_ = HANDLE_ERROR_;
                      break;
                    }

                  if (authID == ComUser::getCurrentUser())
                    {
                      // Limit results to privileges allowed for specified user
                      if (!ComUser::isRootUserID() && !ComUser::currentUserHasRole(ROOT_ROLE_ID))
                        {
                          str_sprintf(buf, " in %s ", authList.data());
                          privWhereClause = buf;
                        }
                    }
                  else
                    {
                      // Get role list for requested user
                      bool hasRootRole = false;
                      char *userRoleList = getRoleList(hasRootRole, authID, cat, pmsch, role_usage);
                      if (!userRoleList)
                        {
                          // Unable to read metadata
                          ExRaiseSqlError(getHeap(), &diagsArea_, -8001);
                          step_ = HANDLE_ERROR_;
                          break;
                        }

                      if (!ComUser::isRootUserID(authID) && !hasRootRole)
                        {
                          str_sprintf(buf, " in %s ", userRoleList);
                          privWhereClause = buf;
                        }
                      NADELETEBASIC(userRoleList, getHeap());
                    }

                  // for DB__ROOT and DB__ROOTROLE, skip privilege checks
                  if (privWhereClause.length() > 0)
                    {
                      // This request performs a union between 4 entities:
                      //  1. object_privileges table
                      //  2. schema_privileges table
                      //  3. column privileges table
                      //  4. hive metadata tables to retrieve column details
                      // The call to colPrivsFrag returns the required the union 
                      // statement(s) for items 3 and 4. See colPrivsFrag for details
                      if (colPrivsFrag(getMItdb().getParam1(), cat, privWhereClause, var1) < 0)
                        {
                          step_ = HANDLE_ERROR_;
                          break;
                        }
                        privWhereClause.prepend(" where grantee_id ");
                    }

                  // Union privileges between object, column and schema
                  // object privs
                  param_[0] = cat;
                  param_[1] = pmsch;
                  param_[2] = objPrivs;
                  param_[3] = (char *) privWhereClause.data();

                  // schema privs
                  param_[4] = cat;
                  param_[5] = pmsch;
                  param_[6] = schPrivs;
                  param_[7] = (char *) privWhereClause.data();

                  // column privs
                  param_[8] = (char *) var1.data();

                  numOutputEntries_ = 2;
                }
              break;



              case ComTdbExeUtilGetMetadataInfo::PRIVILEGES_ON_TABLE_:
              case ComTdbExeUtilGetMetadataInfo::PRIVILEGES_ON_VIEW_:
              case ComTdbExeUtilGetMetadataInfo::PRIVILEGES_ON_SEQUENCE_:
              case ComTdbExeUtilGetMetadataInfo::PRIVILEGES_ON_LIBRARY_:
              case ComTdbExeUtilGetMetadataInfo::PRIVILEGES_ON_ROUTINE_:
              {
                qs = getTrafPrivsOnObject;
                sizeOfqs = sizeof(getTrafPrivsOnObject);

                // Determine the type of object
                if (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::PRIVILEGES_ON_TABLE_)
                  var = COM_BASE_TABLE_OBJECT_LIT;
                else if (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::PRIVILEGES_ON_VIEW_)
                  var = COM_VIEW_OBJECT_LIT;
                else if (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::PRIVILEGES_ON_SEQUENCE_)
                  var = COM_SEQUENCE_GENERATOR_OBJECT_LIT;
                else if (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::PRIVILEGES_ON_LIBRARY_)
                  var = COM_LIBRARY_OBJECT_LIT;
                else
                  var = COM_USER_DEFINED_ROUTINE_OBJECT_LIT;

                getBitExtractsForObjectType(var, var1);
                char buf[authList.length() + 100];

                Int32 authID = *currContext->getDatabaseUserID();
                if (getMItdb().getParam1())
                  {
                    if (!(strcmp(getMItdb().getParam1(), currContext->getDatabaseUserName()) == 0))
                      authID = getAuthID(getMItdb().getParam1(), cat, sch, auths);
                  }

                if (doPrivCheck)
                  {
                    if (getMItdb().getParam1())
                      {
                        if ((CmpSeabaseDDLauth::isRoleID(authID) && !ComUser::currentUserHasRole(authID)) ||
                            (CmpSeabaseDDLauth::isUserID(authID) && authID != ComUser::getCurrentUser()))
                          {
                            ExRaiseSqlError(getHeap(), &diagsArea_, -CAT_NOT_AUTHORIZED);
                            step_ = HANDLE_ERROR_;
                            break;
                          }     
                      }

                    // return privs for the current user and their roles
                    if (!ComUser::isRootUserID() && !ComUser::currentUserHasRole(ROOT_ROLE_ID))
                      {
                        str_sprintf(buf, " and grantee_id in %s ", authList.data());
                        privWhereClause = buf;
                      }
                  }
                else
                  {
                    // Only return rows for specified user
                    // DB__ROOT and users granted DB__ROOTROLE can see everything
                    if (getMItdb().getParam1())
                      {
                        if (strcmp(getMItdb().getParam1(), currContext->getDatabaseUserName()) == 0)
                          {
                            if (!ComUser::isRootUserID() && !ComUser::currentUserHasRole(ROOT_ROLE_ID))
                              {
                                str_sprintf(buf, " and grantee_id in %s ", authList.data());
                                privWhereClause = buf;
                              }
                          }
                        else
                          {
                            bool hasRootRole = false;
                            char *userRoleList = getRoleList(hasRootRole, authID, 
                                                             cat, pmsch, role_usage);
                            if (!userRoleList)
                              {
                                // Unable to read metadata
                                ExRaiseSqlError(getHeap(), &diagsArea_, -8001);
                                step_ = HANDLE_ERROR_;
                                break;
                              }
                          
                            if (!ComUser::isRootUserID(authID) && !hasRootRole)
                              {
                                str_sprintf(buf, " and grantee_id in %s ", userRoleList);
                                privWhereClause = buf;
                              }

                            NADELETEBASIC(userRoleList, getHeap());
                          }
                      }
                  }
 
                str_sprintf(buf, " = '%s'", var.data());
                NAString typeClause = buf;
                Int64 objectUID = getObjectUID(cat,sch,tab,getMItdb().getObj(), typeClause.data());
                
                // To match behavior of other GET commands, if the object is not found, then
                // return no rows. 
                if (objectUID == -1)
                  {
                    step_ = DONE_;
                    break;
                  }
                NAString oUID = Int64ToNAString(objectUID);
                typeClause = "in ('PS', 'SS')";

                Int64 schemaUID = getObjectUID(cat,sch,tab,"__SCHEMA__", typeClause.data());

                param_[0] = (char *)var1.data();
                param_[1] = cat;
                param_[2] = pmsch;
                param_[3] = objPrivs;
                param_[4] = (char *)oUID.data();
                param_[5] = (char *)privWhereClause.data();

                param_[6] = (char *)var1.data();
                param_[7] = cat;
                param_[8] = pmsch;
                param_[9] = colPrivs;
                param_[10] = (char *)oUID.data();
                param_[11] = (char *)privWhereClause.data();

                param_[12] = (char *)var1.data();
                param_[13] = cat;
                param_[14] = pmsch;
                param_[15] = schPrivs;
                param_[16] = (char *)Int64ToNAString(schemaUID).data();
                param_[17] = (char *)privWhereClause.data();

                numOutputEntries_ = 2;
                break;
              }

              case ComTdbExeUtilGetMetadataInfo::INDEXES_FOR_USER_:
                {
                  qs = getTrafIndexesForAuth;
                  sizeOfqs = sizeof(getTrafIndexesForAuth);

                  // Get the authID associated with the specified user
                  Int32 authID = *currContext->getDatabaseUserID();
                  if (!(strcmp(getMItdb().getParam1(), currContext->getDatabaseUserName()) == 0))
                    authID = getAuthID(getMItdb().getParam1(), cat, sch, auths);

                  // If not a user, we are done, don't return data
                  if (!CmpSeabaseDDLauth::isUserID(authID))
                    {
                      ExRaiseSqlError(getHeap(), &diagsArea_, -CAT_IS_NOT_A_USER, 
                          NULL, NULL, NULL,
                          getMItdb().getParam1());
                      step_ = HANDLE_ERROR_;
                      break;
                    }

                  // Non elevated user cannot view indexes for another user
                  if (doPrivCheck && authID != ComUser::getCurrentUser())
                    {
                      ExRaiseSqlError(getHeap(), &diagsArea_, -CAT_NOT_AUTHORIZED);
                      step_ = HANDLE_ERROR_;
                      break;
                    }

                  // Limit results to privileges allowed for specified user
                  
                  if (authID == ComUser::getCurrentUser())
                    {
                      if (!ComUser::isRootUserID() && !ComUser::currentUserHasRole(ROOT_ROLE_ID))
                        {
                          privFromClause = addFromClause(authList, true);
                          privWhereClause = addWhereClause(true, "T.object_uid");
                        }
                    }
                  else
                    {
                      bool hasRootRole = false;
                      char *userRoleList = getRoleList(hasRootRole, authID, cat, pmsch, role_usage);
                      if (!userRoleList)
                        {
                          // Unable to read metadata
                          ExRaiseSqlError(getHeap(), &diagsArea_, -8001);
                          step_ = HANDLE_ERROR_;
                          break;
                        }

                      if (!ComUser::isRootUserID(authID) && !hasRootRole)
                        {
                          privFromClause = addFromClause(userRoleList, true);
                          privWhereClause = addWhereClause( true, "T.object_uid");
                        }

                      NADELETEBASIC(userRoleList, getHeap());
                    }

                  param_[0] = cat;
                  param_[1] = sch;
                  param_[2] = indexes;
                  param_[3] = cat;
                  param_[4] = sch;
                  param_[5] = tab;
                  param_[6] = cat;
                  param_[7] = sch;
                  param_[8] = tab;
                  param_[9] = (char *)privFromClause.data();
                  param_[10] = getMItdb().cat_;
                  param_[11] = (char *)privWhereClause.data();
                }
                break;

              case ComTdbExeUtilGetMetadataInfo::INDEXES_FOR_ROLE_:
                {
                  qs = getTrafIndexesForAuth;
                  sizeOfqs = sizeof(getTrafIndexesForAuth);

                  // Get the authID associated with the specified role
                  Int32 authID = *currContext->getDatabaseUserID();
                  if (!(strcmp(getMItdb().getParam1(), currContext->getDatabaseUserName()) == 0))
                    authID = getAuthID(getMItdb().getParam1(), cat, sch, auths);

                  // Verify that the authID is actually a role
                  if (!CmpSeabaseDDLauth::isRoleID(authID) && !ComUser::isPublicUserID(authID))
                    {
                      ExRaiseSqlError(getHeap(), &diagsArea_, -CAT_IS_NOT_A_ROLE, 
                          NULL, NULL, NULL,
                          getMItdb().getParam1());
                      step_ = HANDLE_ERROR_;
                      break;
                    }

                  // Non elevated users need to be granted role
                  if (doPrivCheck && !ComUser::currentUserHasRole(authID))
                    {
                      ExRaiseSqlError(getHeap(), &diagsArea_, -CAT_NOT_AUTHORIZED);
                      step_ = HANDLE_ERROR_;
                      break;
                    }

                  if (authID != ROOT_ROLE_ID)
                    {
                      // Only return indexes that role (authID) has been granted privs
                      char buf[30];
                      str_sprintf(buf, "(%d)", authID);
                      privFromClause = addFromClause(buf, true);
                      privWhereClause = addWhereClause(true, "T.object_uid");
                    }

                  param_[0] = cat;
                  param_[1] = sch;
                  param_[2] = indexes;
                  param_[3] = cat;
                  param_[4] = sch;
                  param_[5] = tab;
                  param_[6] = cat;
                  param_[7] = sch;
                  param_[8] = tab;
                  param_[9] = (char *)privFromClause.data();
                  param_[10] = getMItdb().cat_;
                  param_[11] = (char *)privWhereClause.data();
                }
                break;


              case ComTdbExeUtilGetMetadataInfo::TABLES_FOR_USER_:
              case ComTdbExeUtilGetMetadataInfo::VIEWS_FOR_USER_:
                {
                  qs = getTrafObjectsForUser;
                  sizeOfqs = sizeof(getTrafObjectsForUser);

                  // Get the authID associated with the specified user
                  Int32 authID = *currContext->getDatabaseUserID();
                  if (!(strcmp(getMItdb().getParam1(), currContext->getDatabaseUserName()) == 0))
                    authID = getAuthID(getMItdb().getParam1(), cat, sch, auths);

                  // Verify that the authID is actually a user
                  if (!CmpSeabaseDDLauth::isUserID(authID))
                    {
                      ExRaiseSqlError(getHeap(), &diagsArea_, -CAT_IS_NOT_A_USER, 
                          NULL, NULL, NULL,
                          getMItdb().getParam1());
                      step_ = HANDLE_ERROR_;
                      break;
                    }
  
                  // Non elevated user cannot view objects for another user
                  if (doPrivCheck && authID != ComUser::getCurrentUser())
                    {
                      ExRaiseSqlError(getHeap(), &diagsArea_, -CAT_NOT_AUTHORIZED);
                      step_ = HANDLE_ERROR_;
                      break;
                    }

                  var = "('";
                  if (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::TABLES_FOR_USER_)
                    var += COM_BASE_TABLE_OBJECT_LIT;
                  else
                    var += COM_VIEW_OBJECT_LIT;
                  var += "')";

                  // Limit results to privileges allowed for specified user
                  bool hasRootRole = false;
                  if (authID == ComUser::getCurrentUser())
                  {
                    if (!ComUser::isRootUserID() && !ComUser::currentUserHasRole(ROOT_ROLE_ID))
                      {
                        privFromClause = addFromClause(authList, true);
                        privWhereClause = addWhereClause(true, "T.object_uid");
                      }
                  }

                  // Getting objects for a user other than the current user
                  else
                    {
                      char *userRoleList = getRoleList(hasRootRole, authID, cat, pmsch, role_usage);
                      if (!userRoleList)
                        {
                          // Unable to read metadata
                          ExRaiseSqlError(getHeap(), &diagsArea_, -8001);
                          step_ = HANDLE_ERROR_;
                          break;
                        }

                      if (!ComUser::isRootUserID(authID) && !hasRootRole)
                        {
                          privFromClause = addFromClause(userRoleList, true);
                          privWhereClause = addWhereClause(true, "T.object_uid");
                        }
                      NADELETEBASIC(userRoleList, getHeap());

                   }

                  // For elevated users, also get objects from HIVE
                  if ((CmpCommon::getDefault(TRAF_GET_HIVE_OBJECTS) == DF_ON) &&
                      (ComUser::isRootUserID(authID) || hasRootRole))
                    {
                      if (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::TABLES_FOR_USER_)
                        privWhereClause += "union select 'HIVE',  upper(trim(schema_name)), "
                                           "upper(trim(table_name)) from table(hivemd(tables))";
                      else
                        privWhereClause += "union select 'HIVE', upper(trim(schema_name)), "
                                           "upper(trim(view_name)) from table(hivemd(views))";
                    }

                  param_[0] = cat;
                  param_[1] = sch;
                  param_[2] = tab;
                  param_[3] = (char *)privFromClause.data();
                  param_[4] = (char *)var.data();
                  param_[5] = (char *)privWhereClause.data();

                  numOutputEntries_ = 3;
                }
              break;

              case ComTdbExeUtilGetMetadataInfo::TABLES_FOR_ROLE_:
              case ComTdbExeUtilGetMetadataInfo::VIEWS_FOR_ROLE_:
                {
                  qs = getTrafObjectsForUser;
                  sizeOfqs = sizeof(getTrafObjectsForUser);

                  // Get the authID associated with the specified user
                  Int32 authID = *currContext->getDatabaseUserID();
                  if (!(strcmp(getMItdb().getParam1(), currContext->getDatabaseUserName()) == 0))
                    authID = getAuthID(getMItdb().getParam1(), cat, sch, auths);

                  // Verify that the specified authID is actually a role
                  if (!CmpSeabaseDDLauth::isRoleID(authID) && !ComUser::isPublicUserID(authID)) 
                    {
                      ExRaiseSqlError(getHeap(), &diagsArea_, -CAT_IS_NOT_A_ROLE, 
                          NULL, NULL, NULL,
                          getMItdb().getParam1());
                      step_ = HANDLE_ERROR_;
                      break;
                    }

                  // Non elevated users must be granted the specified role
                  if (doPrivCheck && !ComUser::currentUserHasRole(authID))
                    {
                      ExRaiseSqlError(getHeap(), &diagsArea_, -CAT_NOT_AUTHORIZED);
                      step_ = HANDLE_ERROR_;
                      break;
                    }

                  var = "('";
                  if (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::TABLES_FOR_ROLE_)
                    var += COM_BASE_TABLE_OBJECT_LIT;
                  else
                    var += COM_VIEW_OBJECT_LIT;
                  var += "')";

                  if (authID == ROOT_ROLE_ID)
                   {
                      if (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::TABLES_FOR_ROLE_)
                        privWhereClause += "union select 'HIVE',  upper(trim(schema_name)), "
                                           "upper(trim(table_name)) from table(hivemd(tables))";
                      else
                        privWhereClause += "union select 'HIVE', upper(trim(schema_name)), "
                                           "upper(trim(view_name)) from table(hivemd(views))";
                    }

                  else
                    {
                      // Only return objects where role (authID) has been granted privs 
                      char buf[30];
                      str_sprintf(buf, "(%d)", authID);
                      {
                        privFromClause = addFromClause(buf, true);
                        privWhereClause = addWhereClause(true, "T.object_uid");
                      }
                    }

                  param_[0] = cat;
                  param_[1] = sch;
                  param_[2] = tab;
                  param_[3] = (char *)privFromClause.data();
                  param_[4] = (char *)var.data();
                  param_[5] = (char *)privWhereClause.data();

                  numOutputEntries_ = 3;
                }
              break;

              case ComTdbExeUtilGetMetadataInfo::LIBRARIES_FOR_USER_:
                {
                  qs = getTrafLibrariesForAuthQuery;
                  sizeOfqs = sizeof(getTrafLibrariesForAuthQuery);

                  Int32 authID = *currContext->getDatabaseUserID();
                  if (!(strcmp(getMItdb().getParam1(), currContext->getDatabaseUserName()) == 0))
                    authID = getAuthID(getMItdb().getParam1(), cat, sch, auths);

                  // Verify that the specified authID is actually a user
                  if (!CmpSeabaseDDLauth::isUserID(authID))
                    {
                      ExRaiseSqlError(getHeap(), &diagsArea_, -CAT_IS_NOT_A_USER, 
                          NULL, NULL, NULL,
                          getMItdb().getParam1());
                      step_ = HANDLE_ERROR_;
                      break;
                    }

                  // Non elevated user cannot view libraries for another user
                  if (doPrivCheck && authID != ComUser::getCurrentUser())
                    {
                      ExRaiseSqlError(getHeap(), &diagsArea_, -CAT_NOT_AUTHORIZED);
                      step_ = HANDLE_ERROR_;
                      break;
                    }
                  
                  // Return libraries that are owned by the user/user's roles
                  // or that the user/user's role have been granted privileges
                    
                  char *userRoleList = NULL;
                  bool hasRootRole = false;
                  if (authID == ComUser::getCurrentUser())
                    {
                      userRoleList = (char *)authList.data();
                      hasRootRole = ComUser::currentUserHasRole(ROOT_ROLE_ID);
                    }
                  else
                    {
                      userRoleList = getRoleList(hasRootRole, authID, cat, pmsch, role_usage);
                      if (!userRoleList)
                        {
                          // Unable to read metadata 
                          ExRaiseSqlError(getHeap(), &diagsArea_, -8001);
                          step_ = HANDLE_ERROR_;
                          break;
                        }
                     }                     
                   
                  // restrict libraries to those where requested user has privs
                  // we could pre-retrieve the list of direct grants to the library
                  // for the requested user similar to the way the role is
                  // retrieved to simplify this query.  The union can also be
                  // replaced with an OR but the union generated a better plan
                  // Sample query generated:
                  //  select distinct object_name
                  //  from "_MD_".objects o
                  //  where o.object_type = 'LB'
                  //  and o.object_uid in
                  //    (select p.object_uid
                  //     from "_PRIVMGR_MD_".object_privileges p
                  //     where grantee_id in (33336, -1)
                  //        or exists (select schema_uid
                  //                   from "_PRIVMGR_MD_".schema_privileges p
                  //                   where ((o.catalog_name || '.' || o.schema_name) = p.schema_name)
                  //                     and grantee_id in (33336, -1))
                  //     union
                  //      (select using_library_uid from "_MD_".libraries_usage
                  //       where used_udr_uid in
                  //      (select p.object_uid
                  //       from "_PRIVMGR_MD_".object_privileges p
                  //       where grantee_id in (33336, -1)
                  //          or exists (select schema_uid
                  //                     from "_PRIVMGR_MD_".schema_privileges p
                  //                     where ((o.catalog_name || '.' || o.schema_name) = p.schema_name))
                  //                       and grantee_id in (33336, -1))
                  //    )) order by 1;


                  if (!ComUser::isRootUserID(authID) && !hasRootRole)
                  {
                    Int32 stmtSize = ((strlen(userRoleList)*4) + 
                                      (strlen(cat)*5) + 
                                      (strlen(pmsch)*4) +
                                      (strlen(objPrivs)*2) + (strlen(schPrivs)*2) + 
                                      strlen(sch) + strlen(library_usage) + 1000);
                    char buf[stmtSize];
                    snprintf
                      (buf, sizeof(buf), "and T.object_uid in "
                       "(select object_uid from %s.\"%s\".%s where grantee_id in %s "
                       " or exists (select schema_uid from %s.\"%s\".%s p"
                       "   where ((T.catalog_name || '.' || T.schema_name) = "
                       "     replace (p.schema_name, '\"', ''))"
                       "   and grantee_id in %s ) "
                       " union (select using_library_uid from %s.\"%s\".%s "
                       "   where used_udr_uid in "
                       "     (select object_uid from %s.\"%s\".%s where grantee_id in %s "
                       "      or exists (select schema_uid from %s.\"%s\".%s p"
                       "        where ((T.catalog_name || '.' || T.schema_name) = "
                       "          replace (p.schema_name, '\"', ''))"
                       "        and grantee_id in %s )))) ",
                       cat, pmsch, objPrivs, userRoleList, 
                       cat, pmsch, schPrivs, userRoleList,
                       cat, sch, library_usage, 
                       cat, pmsch, objPrivs, userRoleList, 
                       cat, pmsch, schPrivs, userRoleList);
                    privWhereClause = buf;
                  }

                  if (authID != ComUser::getCurrentUser())
                    NADELETEBASIC(userRoleList, getHeap());

                  param_[0] = cat;
                  param_[1] = sch;
                  param_[2] = tab;
                  param_[3] = getMItdb().cat_;
                  param_[4] = (char *) privWhereClause.data();
                }
                break ;

              case ComTdbExeUtilGetMetadataInfo::LIBRARIES_FOR_ROLE_:
                {
                  qs = getTrafLibrariesForAuthQuery;
                  sizeOfqs = sizeof(getTrafLibrariesForAuthQuery);

                  // Get the authID associated with the specified role
                  Int32 authID = *currContext->getDatabaseUserID();
                  if (!(strcmp(getMItdb().getParam1(), currContext->getDatabaseUserName()) == 0))
                    authID = getAuthID(getMItdb().getParam1(), cat, sch, auths);

                  // Verify that the specified authID is actually a role
                  if (!CmpSeabaseDDLauth::isRoleID(authID) && !ComUser::isPublicUserID(authID)) 
                    {
                      ExRaiseSqlError(getHeap(), &diagsArea_, -CAT_IS_NOT_A_ROLE, 
                          NULL, NULL, NULL,
                          getMItdb().getParam1());
                      step_ = HANDLE_ERROR_;
                      break;
                    }

                  // Non elevated users must be granted role
                  if (doPrivCheck && !ComUser::currentUserHasRole(authID))
                    {
                      ExRaiseSqlError(getHeap(), &diagsArea_, -CAT_NOT_AUTHORIZED);
                      step_ = HANDLE_ERROR_;
                      break;
                    }

                  if (authID != ROOT_ROLE_ID)
                  {
                    // Only return libraries where the current user has been
                    // granted privilege on library or routine in the library
                    // See LIBRARIES_FOR_USER for more details on query
                    Int32 stmtSize = ((sizeof(authID)*5) +
                                      (strlen(cat)*5) + 
                                      (strlen(pmsch)*4) +
                                      (strlen(objPrivs)*2) + (strlen(schPrivs)*2) + 
                                      strlen(sch) + strlen(library_usage) + 1000);
                    char buf[stmtSize];
                    snprintf
                      (buf, sizeof(buf), "and T.object_uid in "
                       "(select object_uid from %s.\"%s\".%s where grantee_id = %d "
                       " or exists (select schema_uid from %s.\"%s\".%s p"
                       "   where ((T.catalog_name || '.' || T.schema_name) = "
                       "     replace (p.schema_name, '\"', ''))"
                       "   and grantee_id = %d ) "
                       " union (select using_library_uid from %s.\"%s\".%s "
                       "   where used_udr_uid in "
                       "     (select object_uid from %s.\"%s\".%s where grantee_id = %d "
                       "      or exists (select schema_uid from %s.\"%s\".%s p"
                       "        where ((T.catalog_name || '.' || T.schema_name) = "
                       "          replace (p.schema_name, '\"', ''))"
                       "        and grantee_id = %d)))) ",
                       cat, pmsch, objPrivs, authID,
                       cat, pmsch, schPrivs, authID,
                       cat, sch, library_usage,
                       cat, pmsch, objPrivs, authID,
                       cat, pmsch, schPrivs, authID);
                    privWhereClause = buf;
                  }

                  param_[0] = cat;
                  param_[1] = sch;
                  param_[2] = tab;
                  param_[3] = getMItdb().cat_;
                  param_[4] = (char *) privWhereClause.data();
                }
                break ;

              case ComTdbExeUtilGetMetadataInfo::OBJECTS_FOR_USER_:
                {
                  qs = getTrafObjectsPlusTypeForUser;
                  sizeOfqs = sizeof(getTrafObjectsPlusTypeForUser);

                  Int32 authID = *currContext->getDatabaseUserID();
                  if (!(strcmp(getMItdb().getParam1(), currContext->getDatabaseUserName()) == 0))
                    authID = getAuthID(getMItdb().getParam1(), cat, sch, auths);

                  // Verify that the specified authID is actually a user
                  if (!CmpSeabaseDDLauth::isUserID(authID))
                    {
                      ExRaiseSqlError(getHeap(), &diagsArea_, -CAT_IS_NOT_A_USER,
                          NULL, NULL, NULL,
                          getMItdb().getParam1());
                      step_ = HANDLE_ERROR_;
                      break;
                    }

                  // Non elevated user cannot view objects for another user
                  if (doPrivCheck && authID != ComUser::getCurrentUser())
                    {
                      ExRaiseSqlError(getHeap(), &diagsArea_, -CAT_NOT_AUTHORIZED);
                      step_ = HANDLE_ERROR_;
                      break;
                    }

                  // Return securable objects that are owned by the user/user's roles
                  // or that the user/user's role have been granted privileges
                  var = "('BT', 'VI', 'PA', 'LB', 'SG', 'UR')";

                  // Limit results to privileges allowed for specified user
                  bool hasRootRole = false;
                  if (authID == ComUser::getCurrentUser())
                  {
                    if (!ComUser::isRootUserID() && !ComUser::currentUserHasRole(ROOT_ROLE_ID))
                      {
                        privFromClause = addFromClause(authList, true);
                        privWhereClause = addWhereClause(true, "T.object_uid");
                      }
                  }

                  // Getting objects for a user other than the current user
                  else
                    {
                      char *userRoleList = getRoleList(hasRootRole, authID, cat, pmsch, role_usage);
                      if (!userRoleList)
                        {
                          // Unable to read metadata
                          ExRaiseSqlError(getHeap(), &diagsArea_, -8001);
                          step_ = HANDLE_ERROR_;
                          break;
                        }

                      if (!ComUser::isRootUserID(authID) && !hasRootRole)
                        {
                          privFromClause = addFromClause(userRoleList, true);
                          privWhereClause = addWhereClause(true, "T.object_uid");
                        }
                      NADELETEBASIC(userRoleList, getHeap());

                   }

                  // get objects from hive, if elevated user
                  if ((CmpCommon::getDefault(TRAF_GET_HIVE_OBJECTS) == DF_ON) &&
                      (ComUser::isRootUserID(authID) || hasRootRole))
                        privWhereClause += "union select 'HIVE', upper(trim(schema_name)), "
                         "upper(trim(table_name)), 'BT' "
                         "from table(hivemd(object names))";

                  param_[0] = cat;
                  param_[1] = sch;
                  param_[2] = tab;
                  param_[3] = (char *)privFromClause.data();
                  param_[4] = (char *)var.data();
                  param_[5] = (char *)privWhereClause.data();

                  numOutputEntries_ = 4;
                }
              break;


              case ComTdbExeUtilGetMetadataInfo::COMPONENTS_:
              {
                qs = getComponents;
                sizeOfqs = sizeof(getComponents);

                if (doPrivCheck)
                  {
                     char buf[authList.length() + 100];
                     str_sprintf(buf, " where c.component_uid = p.component_uid and p.grantee_id in %s", authList.data());
                     privWhereClause = buf;
                  }

                param_[0] = cat;
                param_[1] = pmsch;
                param_[2] = components;
                param_[3] = cat;
                param_[4] = pmsch;
                param_[5] = componentPrivileges;
                param_[6] = (char *) privWhereClause.data();
              }
              break;

              case ComTdbExeUtilGetMetadataInfo::COMPONENT_PRIVILEGES_:
              {
                 qs = getComponentPrivileges;
                 sizeOfqs = sizeof(getComponentPrivileges);

                 // Get privileges for auth name
                 if (getMItdb().getParam1()) 
                 {
                    // Get the authID associated with the request's auth name
                    Int32 authID = *currContext->getDatabaseUserID();
                    if (!(strcmp(getMItdb().getParam1(), currContext->getDatabaseUserName()) == 0))
                      authID = getAuthID(getMItdb().getParam1(), cat, sch, auths);

                    // if incorrect auth type, return error
                    if (!CmpSeabaseDDLauth::isRoleID(authID) && !CmpSeabaseDDLauth::isUserID(authID) &&
                        !ComUser::isPublicUserID(authID))
                    {
                      ExRaiseSqlError(getHeap(), &diagsArea_, -CAT_IS_NOT_CORRECT_AUTHID,
                          NULL, NULL, NULL,
                          getMItdb().getParam1(), "user or role");
                      step_ = HANDLE_ERROR_;
                      break;
                    }

                    if (doPrivCheck)
                    {
                      // Asking for privileges for an auth not assigned to current user
                      if ((CmpSeabaseDDLauth::isRoleID(authID) && !ComUser::currentUserHasRole(authID)) ||
                          (CmpSeabaseDDLauth::isUserID(authID) && authID != ComUser::getCurrentUser()))
                      {
                        ExRaiseSqlError(getHeap(), &diagsArea_, -CAT_NOT_AUTHORIZED);
                        step_ = HANDLE_ERROR_;
                        break;
                      }

                      // get privileges for current user, if cascade include roles
                      if (CmpSeabaseDDLauth::isUserID(authID) && getMItdb().cascade())
                      {
                        privWhereClause += "and grantee_id in ";
                        privWhereClause += authList.data();
                      }
                      else
                      {
                        privWhereClause += "and grantee_name = '";
                        privWhereClause += getMItdb().getParam1();
                        privWhereClause += "'";
                      }
                    }
                    else // is elevated user
                    { 
                      bool hasRootRole = false;
                      char *userRoleList = getRoleList(hasRootRole, authID, cat, pmsch, role_usage);
                      if (!userRoleList)
                      {
                        // Unable to read metadata 
                        ExRaiseSqlError(getHeap(), &diagsArea_, -8001);
                         step_ = HANDLE_ERROR_;
                        break;
                      }

                      if (!ComUser::isRootUserID(authID) && !hasRootRole)
                      {
                        // if cascade, get role list for specified user
                        if (CmpSeabaseDDLauth::isUserID(authID) && getMItdb().cascade())
                        {
                          privWhereClause += "and p.grantee_id in ";
                          privWhereClause += userRoleList;
                        }
                        else
                        {
                          privWhereClause += "and grantee_name = '";
                          privWhereClause += getMItdb().getParam1();
                          privWhereClause += "'";
                        }
                      }
                      NADELETEBASIC(userRoleList, getHeap());
                    }
                 }

                 // no specific authname specified, get current users results
                 else
                 {
                    if (!ComUser::isRootUserID() && !ComUser::currentUserHasRole(ROOT_ROLE_ID))
                    {
                      // Limit results to current user and current users roles
                      if (getMItdb().cascade())
                      {
                         privWhereClause += " and p.grantee_id in ";
                         privWhereClause += authList.data();
                      }
                      // limit results to current user
                      else
                      {
                         privWhereClause += " and p.grantee_name = '";
                         privWhereClause += currContext->getDatabaseUserName();
                         privWhereClause += "'";
                      }
                   }
                 }

                 param_[0] = cat;
                 param_[1] = pmsch;
                 param_[2] = components;
                 param_[3] = cat;
                 param_[4] = pmsch;
                 param_[5] = componentOperations;
                 param_[6] = cat;
                 param_[7] = pmsch;
                 param_[8] = componentPrivileges;
                 param_[9] = getMItdb().getObj();
                 param_[10] = (char *) privWhereClause.data();
              }
              break;

              case ComTdbExeUtilGetMetadataInfo::SEQUENCES_IN_CATALOG_:
                {
                  qs = getTrafSequencesInCatalogQuery;
                  sizeOfqs = sizeof(getTrafSequencesInCatalogQuery);

                  if (doPrivCheck)
                    {
                      privFromClause = addFromClause(authList, true);
                      privWhereClause = addWhereClause(true, "T.object_uid");
                    }

		  param_[0] = cat;
		  param_[1] = sch;
		  param_[2] = tab;
                  param_[3] = (char *) privFromClause.data();
		  param_[4] = getMItdb().cat_;
                  param_[5] = (char *) privWhereClause.data();
                }
                break ;

              case ComTdbExeUtilGetMetadataInfo::SEQUENCES_IN_SCHEMA_:
                {
                  qs = getTrafSequencesInSchemaQuery;
                  sizeOfqs = sizeof(getTrafSequencesInSchemaQuery);

                  if (doPrivCheck)
                    {
                      privFromClause = addFromClause(authList, true);
                      privWhereClause = addWhereClause(true, "T.object_uid");
                    }

		  param_[0] = cat;
		  param_[1] = sch;
		  param_[2] = tab;
                  param_[3] = (char *) privFromClause.data();
		  param_[4] = getMItdb().cat_;
		  param_[5] = getMItdb().sch_;
                  param_[6] = (char *) privWhereClause.data();
                }
                break ;

	      default:
		{
                  ExRaiseSqlError(getHeap(), &diagsArea_, -4218, 
                       NULL, NULL, NULL,
                       "GET");
		  step_ = HANDLE_ERROR_;
		}
		break;
	      }
         
             if (step_ == HANDLE_ERROR_)
               break;

             if (step_ == DONE_)
               break;

	     Int32 qryArraySize = sizeOfqs / sizeof(QueryString);
	     char * gluedQuery;
	     Lng32 gluedQuerySize;
	     glueQueryFragments(qryArraySize, qs, 
				gluedQuery, gluedQuerySize);
	     
	     str_sprintf(queryBuf_, gluedQuery,
			 param_[0], param_[1], param_[2], param_[3], param_[4],
			 param_[5], param_[6], param_[7], param_[8], param_[9],
			 param_[10], param_[11], param_[12], param_[13], param_[14], param_[15],
                         param_[16], param_[17], param_[18], param_[19], param_[20],
                         param_[21]);
             NADELETEBASIC(gluedQuery, getMyHeap());	     
	     step_ = FETCH_ALL_ROWS_;
	  }
	  break;

	case FETCH_ALL_ROWS_:
	  {
	    if (initializeInfoList(infoList_))
	      {
		step_ = HANDLE_ERROR_;
		break;
	      }
 
	    if (fetchAllRows(infoList_, queryBuf_, numOutputEntries_,
			     FALSE, retcode) < 0)
	      {
                cliInterface()->allocAndRetrieveSQLDiagnostics(diagsArea_);
		step_ = HANDLE_ERROR_;
		break;
	      }

	    infoList_->position();

        //Check for valid LDAP Group or User
        if (COM_LDAP_AUTH == CmpCommon::getAuthenticationType())
        {
            //USERS_FOR_MEMBERS_ is not need
            if (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::GROUPS_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::USERS_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::GROUPS_FOR_TENANT_ ||
                getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::GROUPS_FOR_USER_)
            {
                if (NOT (DISABLE_EXTERNAL_AUTH_CHECKS & GetCliGlobals()->currContext()->getSqlParserFlags()) &&
                    DF_ON != CmpCommon::getDefault(TRAF_IGNORE_EXTERNAL_LDAP_EXISTS_CHECK))
                {
                    DBUserAuth *ldapConn = DBUserAuth::GetInstance();
                    if (NULL != ldapConn)
                    {
                        std::vector<char *> tmpList;
                        std::vector<char *> outList;
                        std::map<char *, char *, Char_Compare> names;
                        bool isUser = (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::USERS_);
                        while(NOT infoList_->atEnd())
                        {
                            OutputInfo * vi = (OutputInfo*)infoList_->getCurr();
                            if (isUser)
                            {
                                //0 - auth_db_name
                                //1 - auth_ext_name
                                tmpList.push_back(vi->get(1));
                                //key auth_ext_name value auth_db_name
                                names.insert(std::make_pair(vi->get(1), vi->get(0)));
                            }
                            else
                                tmpList.push_back(vi->get(0));
                            infoList_->advance();
                        }
                        ldapConn->getValidLDAPEntries(NULL,-2,tmpList,outList,
                            (getMItdb().queryType_ != ComTdbExeUtilGetMetadataInfo::USERS_));
                        //initializeInfoList(infoList_);
                        //reconstructed result
                        if (!outList.empty())
                        {
                            std::vector<char*> tmpQueue;
                            for (std::vector<char *>::iterator itor = outList.begin(); itor != outList.end(); itor++)
                            {
                                char *dbName = NULL;
                                if (isUser)
                                {
                                    //use DBname
                                    if (0 != names.count(*itor))
                                        dbName = names.at(*itor);
                                    else
                                        dbName = NULL;
                                }
                                else
                                    dbName = *itor;
                                if (dbName)
                                {
                                    //don't use strdup
                                    char *tmp = new (getHeap()) char[strlen(dbName) + 1];
                                    strcpy(tmp, dbName);
                                    tmpQueue.push_back(tmp);
                                }
                                //entries in outList are created by 'new char[len]'
                                //NADELETEBASICARRAY(*itor, STMTHEAP);
                                delete[] *itor;
                            }
                            initializeInfoList(infoList_);
                            //sort
                            {
                                std::sort(tmpQueue.begin(), tmpQueue.end(), CmpCommon::char_comp);
                                for (std::vector<char *>::iterator itor = tmpQueue.begin(); itor != tmpQueue.end(); itor++)
                                {
                                    OutputInfo *oi = new (STMTHEAP) OutputInfo(1);
                                    oi->insert(0, *itor);
                                    infoList_->insert(oi);
                                }
                            }
                        }
                        else
                            initializeInfoList(infoList_);
                    }
                    else
                    {
                        initializeInfoList(infoList_);
                    }
                    infoList_->position();
                }
            }
        }

	    step_ = RETURN_ROW_;
	  }
	break;

	case RETURN_ROW_:
	  {
	    if (infoList_->atEnd())
	      {
		step_ = ENABLE_CQS_;
		break;
	      }

	    if (qparent_.up->isFull())
	      return WORK_OK;

	    OutputInfo * vi = (OutputInfo*)infoList_->getCurr();

            NAString outputStr;
	    char * ptr = vi->get(0);
	    short len = (short)(ptr ? strlen(ptr) : 0);
 
	    exprRetCode = ex_expr::EXPR_TRUE;

            if (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::ACTIVE_ROLES_)
            {
              if (strlen(ptr) > 40)
                outputStr.append(ptr, 40);
              else 
                outputStr.append(ptr);
              while (outputStr.length() < 40)
                outputStr.append(' ');
              outputStr.append(vi->get(1),40);
              char * outputCharStr = new (getHeap())char[outputStr.length() + 1];
              strcpy(outputCharStr, outputStr.data());
              ptr = outputCharStr;
              len = strlen(outputCharStr);
            }
         
            if ((getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::PRIVILEGES_FOR_USER_) ||
                (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::PRIVILEGES_FOR_ROLE_) ||
                (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::PRIVILEGES_ON_VIEW_) ||
                (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::PRIVILEGES_ON_TABLE_) || 
                (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::PRIVILEGES_ON_SEQUENCE_) || 
                (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::PRIVILEGES_ON_LIBRARY_) || 
                (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::PRIVILEGES_ON_ROUTINE_))

            {
              // output:  privileges<4spaces>object name
              outputStr = vi->get(1);
              outputStr += "    ";
              outputStr += ptr;
              ptr = (char *)outputStr.data();
              len = outputStr.length();
            }

            if ((getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::OBJECTS_FOR_USER_) ||
                (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::TABLES_FOR_USER_) ||
                (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::VIEWS_FOR_USER_) ||
                (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::TABLES_FOR_ROLE_) ||
                (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::VIEWS_FOR_ROLE_))
              {
                // combine columns from query into a single row, also prepare row
                // for expression evaluation called later
                // Objects row format:  <object type(2) space(1) fully qualified name>
                // Tables/views row format: <fully qualified name>
                if (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::OBJECTS_FOR_USER_)
                  {
                    outputStr  = vi->get(3);
                    outputStr += " ";
                  }

                QualifiedName qn(vi->get(2), vi->get(1), vi->get(0));
                outputStr += qn.getCatalogName();
                outputStr += ".";
                outputStr += qn.getUnqualifiedSchemaNameAsAnsiString();
                outputStr += ".";
                outputStr += qn.getUnqualifiedObjectNameAsAnsiString();
                ptr = (char *)outputStr.data();
                len = outputStr.length();
              }

            if ((getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::SCHEMAS_FOR_USER_) ||
                (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::SCHEMAS_FOR_ROLE_))
              {
                // Return fully qualified schema names as delimited when appropriate
                SchemaName sn (vi->get(1), vi->get(0));
                outputStr += sn.getSchemaNameAsAnsiString();
                ptr = (char*)outputStr.data();
                len = outputStr.length();
              }

// Not supported at this time
#if 0
	    if ((getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::TRIGTEMP_TABLE_ON_TABLE_ ) || 
		(getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::TRIGTEMP_TABLE_ON_MV_ ))
	      {
		//append __TEMP to the name
		short len1 = strlen(ptr);
		char *nameString = new (getHeap())char[len1+7+1];
	        memset(nameString,'\0',len1+7+1);
		ComBoolean isQuoted = FALSE;
	
		str_cpy_all(nameString, vi->get(0),len1);
		if ( '"' == nameString[ len1 - 1 ] )
		  {
		    isQuoted = TRUE;
		  }
		if (isQuoted)
		  str_cpy_all(&nameString[len1-1],"__TEMP\"",8);
		else
		  str_cpy_all(&nameString[len1],"__TEMP",6);
		
		ptr = nameString;
		if (isQuoted)
		  len = len1+7;
		else
		  len = len1+6;

			    
	      }
#endif

	    if (((getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::TABLES_IN_VIEW_)
		 //|| (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::TABLES_IN_MV_)
                ) &&
		(vi->get(3) && (strcmp(vi->get(3), "BT") != 0)))
	      exprRetCode = ex_expr::EXPR_FALSE;
	    else if ((getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::VIEWS_IN_VIEW_) &&
		(vi->get(3) && (strcmp(vi->get(3), "VI") != 0)))
	      exprRetCode = ex_expr::EXPR_FALSE;
	    //else if ((getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::MVS_IN_MV_) &&
            //         (vi->get(3) && (strcmp(vi->get(3), "MV") != 0)))
	    //  exprRetCode = ex_expr::EXPR_FALSE;

	    if (exprRetCode == ex_expr::EXPR_TRUE)
	      exprRetCode = evalScanExpr(ptr, len, TRUE);
	    if (exprRetCode == ex_expr::EXPR_FALSE)
	      {
		// row does not pass the scan expression,
		// move to the next row.
		infoList_->advance();
		break;
	      }
	    else if (exprRetCode == ex_expr::EXPR_ERROR)
	     {
		step_ = HANDLE_ERROR_;
		break;
	      }

	    if (NOT headingReturned_)
	      {
		step_ = DISPLAY_HEADING_;
		break;
	      }

            // if returned table name is an external name, convert it to the native name.
            // Do it for tables_in_view and objects_in_view operations only.
            NAString nativeName;
            NAString objectName;
	    if ((getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::TABLES_IN_VIEW_) ||
                (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::OBJECTS_IN_VIEW_) ||
                (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::VIEWS_IN_VIEW_))
              {
                QualifiedName qn(vi->get(2), vi->get(1), vi->get(0));
                objectName = qn.getCatalogName() + "." 
                  + qn.getUnqualifiedSchemaNameAsAnsiString() + "."
                  + qn.getUnqualifiedObjectNameAsAnsiString();

                if ((strcmp(vi->get(3), "BT") == 0) &&
                    (ComIsTrafodionExternalSchemaName(qn.getSchemaName())))
                  {
                    objectName =
                      ComConvertTrafNameToNativeName
                      (qn.getCatalogName(),
                       qn.getUnqualifiedSchemaNameAsAnsiString(),
                       qn.getUnqualifiedObjectNameAsAnsiString());
                  }

                ptr = (char*)objectName.data();
                len = objectName.length();
              }

	    if ((getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::VIEWS_ON_TABLE_) ||
                (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::VIEWS_ON_VIEW_))
              {
                QualifiedName qn(vi->get(2), vi->get(1), vi->get(0));
                objectName = qn.getCatalogName() + "." 
                  + qn.getUnqualifiedSchemaNameAsAnsiString() + "."
                  + qn.getUnqualifiedObjectNameAsAnsiString();

                ptr = (char*)objectName.data();
                len = objectName.length();
              }

	    short rc = 0;
	    if (moveRowToUpQueue(ptr, len, &rc))
            {
	      return rc;
            }

	    infoList_->advance();
            incReturnRowCount();
	  }
	break;

	case DISPLAY_HEADING_:
	  {
	    retcode = displayHeading();
	    if (retcode == 1)
	      return WORK_OK;
	    else if (retcode < 0)
	      {
		step_ = HANDLE_ERROR_;
		break;
	      }

	    headingReturned_ = TRUE;

	    step_ = RETURN_ROW_;
	  }
	break;

	case ENABLE_CQS_:
	  {
	    if (restoreCQS())
	      {
		step_ = HANDLE_ERROR_;
		break;
	      }

	    if ((getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::VIEWS_ON_TABLE_) ||
		//(getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::MVS_ON_TABLE_) ||
		//(getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::MVS_ON_VIEW_) ||
		(getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::VIEWS_ON_VIEW_)) 

	      step_ = GET_USING_VIEWS_;
	    else if ((getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::TABLES_IN_VIEW_) ||
		     (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::VIEWS_IN_VIEW_) ||
		     //(getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::TABLES_IN_MV_) ||
		     //(getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::MVS_IN_MV_) ||
		     //(getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::OBJECTS_IN_MV_) ||
		     (getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::OBJECTS_IN_VIEW_))
	      step_ = GET_USED_OBJECTS_;
	    else
	      step_ = DONE_;
	  }
	break;

	case GET_USING_VIEWS_:
	  {
	    if (qparent_.up->isFull())
	      return WORK_OK;

	    if (NOT getMItdb().allObjs())
	      {
		step_ = DONE_;
		break;
	      }

	    char * viewName = NULL;
	    Lng32 len = 0;
	    if ((getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::VIEWS_ON_TABLE_) ||
		(getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::VIEWS_ON_VIEW_))
	      cliRC = getUsingView(infoList_, TRUE, viewName, len);
	    else
	      cliRC = getUsingView(infoList_, FALSE, viewName, len);

	    if (cliRC == 100)
	      {
		step_ = DONE_;
		break;
	      }
	    else if (cliRC < 0)
	      {
		step_ = HANDLE_ERROR_;
		break;
	      }

	    exprRetCode = evalScanExpr(viewName, len, TRUE);
	    if (exprRetCode == ex_expr::EXPR_FALSE)
	      {
		// row does not pass the scan expression,
		// move to the next row.
		break;
	      }
	    else if (exprRetCode == ex_expr::EXPR_ERROR)
	      {
		step_ = HANDLE_ERROR_;
		break;
	      }

	    short rc = 0;
	    moveRowToUpQueue(viewName, len, &rc);
	  }
	break;

	case GET_USED_OBJECTS_:
	  {
	    if (qparent_.up->isFull())
	      return WORK_OK;

	    if (NOT getMItdb().allObjs())
	      {
		step_ = DONE_;
		break;
	      }

	    char * viewName = NULL;
	    Lng32 len = 0;
	    if ((getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::TABLES_IN_VIEW_) ||
		(getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::VIEWS_IN_VIEW_) ||
		(getMItdb().queryType_ == ComTdbExeUtilGetMetadataInfo::OBJECTS_IN_VIEW_))
	      cliRC = getUsedObjects(infoList_, TRUE, viewName, len);
	    else
	      cliRC = getUsedObjects(infoList_, FALSE, viewName, len);

	    if (cliRC == 100)
	      {
		step_ = DONE_;
		break;
	      }
	    else if (cliRC < 0)
	      {
		step_ = HANDLE_ERROR_;
		break;
	      }

	    exprRetCode = evalScanExpr(viewName, len, TRUE);
	    if (exprRetCode == ex_expr::EXPR_FALSE)
	      {
		// row does not pass the scan expression,
		// move to the next row.
		break;
	      }
	    else if (exprRetCode == ex_expr::EXPR_ERROR)
	      {
		step_ = HANDLE_ERROR_;
		break;
	      }

	    short rc = 0;
	    moveRowToUpQueue(viewName, len, &rc);
	  }
	break;

	case HANDLE_ERROR_:
	  {
	    restoreCQS();

	    retcode = handleError();
	    if (retcode == 1)
	      {
		return WORK_OK;
	      } // if (retcode
	    
	    step_ = DONE_;
	  }
	break;

	case DONE_:
	  {
            if (NOT getMItdb().noHeader() && getReturnRowCount() > 0)
            {
              short rc = 0;
              char returnMsg[256];
              memset(returnMsg, 0, 256);
              sprintf(returnMsg, "\n=======================\n %d row(s) returned", getReturnRowCount());
              moveRowToUpQueue(returnMsg, strlen(returnMsg), &rc);
            }

	    retcode = handleDone();
	    if (retcode == 1)
	      return WORK_OK;

	    step_ = INITIAL_;

	    return WORK_OK;
	  }
	break;

	}

    }

  return 0;
}

////////////////////////////////////////////////////////////////
// Constructor for class ExExeUtilGetMetadataInfoComplexTcb
///////////////////////////////////////////////////////////////
ExExeUtilGetMetadataInfoComplexTcb::ExExeUtilGetMetadataInfoComplexTcb(
     const ComTdbExeUtilGetMetadataInfo & exe_util_tdb,
     ex_globals * glob)
     : ExExeUtilGetMetadataInfoTcb( exe_util_tdb, glob)
{
}

ExExeUtilGetMetadataInfoComplexTcb::~ExExeUtilGetMetadataInfoComplexTcb()
{
}

//////////////////////////////////////////////////////
// work() for ExExeUtilGetMetadataInfoComplexTcb
//////////////////////////////////////////////////////
short ExExeUtilGetMetadataInfoComplexTcb::work()
{
  short retcode = 0;
  Lng32 cliRC = 0;
  ex_expr::exp_return_type exprRetCode = ex_expr::EXPR_OK;

  // if no parent request, return
  if (qparent_.down->isEmpty())
    return WORK_OK;

  // if no room in up queue, won't be able to return data/status.
  // Come back later.
  if (qparent_.up->isFull())
    return WORK_OK;

  ex_queue_entry * pentry_down = qparent_.down->getHeadEntry();
  ExExeUtilPrivateState & pstate =
    *((ExExeUtilPrivateState*) pentry_down->pstate);

  // Get the globals stucture of the master executor.
  ExExeStmtGlobals *exeGlob = getGlobals()->castToExExeStmtGlobals();
  ExMasterStmtGlobals *masterGlob = exeGlob->castToExMasterStmtGlobals();
  ContextCli * currContext = masterGlob->getStatement()->getContext();

  while (1)
    {
      switch (step_)
	{
	case INITIAL_:
	  {
	    step_ = SETUP_QUERY_;
            returnRowCount_ = 0 ;
	  }
	break;

	case SETUP_QUERY_:
	  {
	    patternStr_[0] = '\0';
	    if (getMItdb().getPattern())
	      {
		str_sprintf(patternStr_, ", match '%s' ",
			    getMItdb().getPattern());
	      }

	    step_ = FETCH_ALL_ROWS_;

	    char rfn[200];
	    if (getMItdb().returnFullyQualNames())
	      strcpy(rfn, ", return full names ");
	    else
	      strcpy(rfn, " ");

	    switch (getMItdb().queryType_)
	      {
	      case ComTdbExeUtilGetMetadataInfo::VIEWS_ON_TABLE_:
		{
		  str_sprintf(queryBuf_, "select * from (get all views on table \"%s\".\"%s\".\"%s\", no header %s %s) x(a) group by a order by 1",
			      getMItdb().getCat(), getMItdb().getSch(), getMItdb().getObj(),
			      rfn,
			      patternStr_);
		}
	      break;

	      case ComTdbExeUtilGetMetadataInfo::VIEWS_ON_VIEW_:
		{
		  str_sprintf(queryBuf_, "select * from (get all views on view \"%s\".\"%s\".\"%s\", no header %s %s) xxx(aaa) group by aaa order by 1",
			      getMItdb().getCat(), getMItdb().getSch(), getMItdb().getObj(),
			      rfn,
			      patternStr_);
		}
	      break;

	      case ComTdbExeUtilGetMetadataInfo::TABLES_IN_VIEW_:
		{
		  str_sprintf(queryBuf_, "select * from (get all tables in view \"%s\".\"%s\".\"%s\", no header %s) xxx(aaa) group by aaa order by 1",
			      getMItdb().getCat(), getMItdb().getSch(), getMItdb().getObj(),
			      patternStr_);
		}
	      break;

	      case ComTdbExeUtilGetMetadataInfo::VIEWS_IN_VIEW_:
		{
		  str_sprintf(queryBuf_, "select * from (get all views in view \"%s\".\"%s\".\"%s\", no header %s) xxx(aaa) group by aaa order by 1",
			      getMItdb().getCat(), getMItdb().getSch(), getMItdb().getObj(),
			      patternStr_);
		}
	      break;

	      case ComTdbExeUtilGetMetadataInfo::OBJECTS_IN_VIEW_:
		{
		  str_sprintf(queryBuf_, "select * from (get all objects in view \"%s\".\"%s\".\"%s\", no header %s) xxx(aaa) group by aaa order by 1",
			      getMItdb().getCat(), getMItdb().getSch(), getMItdb().getObj(),
			      patternStr_);
		}
	      break;

	      case ComTdbExeUtilGetMetadataInfo::OBJECTS_ON_TABLE_:
		{
		  step_ = FETCH_ALL_ROWS_FOR_OBJECTS_;
		}
	      break;

	      case ComTdbExeUtilGetMetadataInfo::OBJECTS_IN_SCHEMA_:
		{
		  step_ = FETCH_ALL_ROWS_IN_SCHEMA_;
		}
	      break;

// not supported at this time
#if 0
	      case ComTdbExeUtilGetMetadataInfo::MVS_ON_TABLE_:
		{
		  str_sprintf(queryBuf_, "select * from (get all mvs on table \"%s\".\"%s\".\"%s\", no header %s) xxx(aaa) group by aaa order by 1",
			      getMItdb().getCat(), getMItdb().getSch(), getMItdb().getObj(),
			      patternStr_);
		}
	      break;

	      case ComTdbExeUtilGetMetadataInfo::MVS_ON_MV_:
		{
		  str_sprintf(queryBuf_, "select * from (get all mvs on mv \"%s\".\"%s\".\"%s\", no header %s) xxx(aaa) group by aaa order by 1",
			      getMItdb().getCat(), getMItdb().getSch(), getMItdb().getObj(),
			      patternStr_);
		}
	      break;

	      case ComTdbExeUtilGetMetadataInfo::TABLES_IN_MV_:
		{
		  str_sprintf(queryBuf_, "select * from (get all tables in mv \"%s\".\"%s\".\"%s\", no header %s) xxx(aaa) group by aaa order by 1",
			      getMItdb().getCat(), getMItdb().getSch(), getMItdb().getObj(),
			      patternStr_);
		}
	      break;

	      case ComTdbExeUtilGetMetadataInfo::MVS_IN_MV_:
		{
		  str_sprintf(queryBuf_, "select * from (get all mvs in mv \"%s\".\"%s\".\"%s\", no header %s) xxx(aaa) group by aaa order by 1",
			      getMItdb().getCat(), getMItdb().getSch(), getMItdb().getObj(),
			      patternStr_);
		}
	      break;

	      case ComTdbExeUtilGetMetadataInfo::OBJECTS_IN_MV_:
		{
		  str_sprintf(queryBuf_, "select * from (get all objects in mv \"%s\".\"%s\".\"%s\", no header %s) xxx(aaa) group by aaa order by 1",
			      getMItdb().getCat(), getMItdb().getSch(), getMItdb().getObj(),
			      patternStr_);
		}
	      break;

#endif
	      default:
		{
                  ExRaiseSqlError(getHeap(), &diagsArea_, -4298, 
                            NULL, NULL, NULL, "GET");
		  step_ = HANDLE_ERROR_;
		}
	      break;

	      } // switch
	  }
	break;

	case FETCH_ALL_ROWS_:
	  {
	    if (initializeInfoList(infoList_))
	      {
		step_ = HANDLE_ERROR_;
		break;
	      }

	    if (fetchAllRows(infoList_, queryBuf_, 1, FALSE, retcode) < 0)
	      {
		step_ = HANDLE_ERROR_;

		break;
	      }

	    infoList_->position();

	    step_ = DISPLAY_HEADING_;
	  }
	break;

	case FETCH_ALL_ROWS_FOR_OBJECTS_:
	  {
	    if (initializeInfoList(infoList_))
	      {
		step_ = HANDLE_ERROR_;
		break;
	      }

	    char ausStr[20];
	    strcpy(ausStr, "");
	    if (getMItdb().systemObjs())
	      strcpy(ausStr, "SYSTEM");
	    else if (getMItdb().allObjs())
	      strcpy(ausStr, "ALL");

	    // Get indexes on table
	    str_sprintf(queryBuf_, "get indexes on table \"%s\".\"%s\".\"%s\" %s",
			getMItdb().getCat(), getMItdb().getSch(), getMItdb().getObj(),
			patternStr_);

	    if (fetchAllRows(infoList_, queryBuf_, 1, FALSE, retcode) < 0)
	      {
		step_ = HANDLE_ERROR_;

		break;
	      }

	    NABoolean rowsFound = FALSE;

	    // insert a NULL entry, this will cause a blank row to be returned
	    if (retcode != 100) // some rows were found
	      {
		infoList_->insert((new(getHeap()) OutputInfo(1)));
		rowsFound = TRUE;
	      }

	    // Get views on table
	    str_sprintf(queryBuf_, "get %s views on table \"%s\".\"%s\".\"%s\" %s",
			ausStr,
			getMItdb().getCat(), getMItdb().getSch(), getMItdb().getObj(),
			patternStr_);

	    if (fetchAllRows(infoList_, queryBuf_, 1, FALSE, retcode) < 0)
	      {
		step_ = HANDLE_ERROR_;

		break;
	      }

	    // insert a NULL entry, this will cause a blank row to be returned
	    if (retcode != 100) // some rows were found
	      {
		infoList_->insert((new(getHeap()) OutputInfo(1)));
		rowsFound = TRUE;
	      }

	    // Get mvs on table
	    str_sprintf(queryBuf_, "get %s mvs on table \"%s\".\"%s\".\"%s\" %s",
			ausStr,
			getMItdb().getCat(), getMItdb().getSch(), getMItdb().getObj(),
			patternStr_);

	    if (fetchAllRows(infoList_, queryBuf_, 1, FALSE, retcode) < 0)
	      {
                // if error is 4218 (command not supported), ignore it.
                if (getDiagsArea() != NULL) {
                   if (getDiagsArea()->mainSQLCODE() != -4218)
                   {
                    step_ = HANDLE_ERROR_;
                    break;
                  }
                  getDiagsArea()->clear();
                }
	      }

	    // insert a NULL entry, this will cause a blank row to be returned
	    if (retcode != 100)
	      {
		infoList_->insert((new(getHeap()) OutputInfo(1)));
		rowsFound = TRUE;
	      }

	    // Get synonyms on table
	    str_sprintf(queryBuf_, "get synonyms on table \"%s\".\"%s\".\"%s\" %s",
			getMItdb().getCat(), getMItdb().getSch(), getMItdb().getObj(),
			patternStr_);

	    if (fetchAllRows(infoList_, queryBuf_, 1, FALSE, retcode) < 0)
	      {
                // if error is 4218 (command not supported), ignore it.
                if (getDiagsArea() != NULL) {
                  if (getDiagsArea()->mainSQLCODE() != -4218)
                  {
                    step_ = HANDLE_ERROR_;
                    
                    break;
                  }
                  getDiagsArea()->clear();
                }
	      }

	    // insert a NULL entry, this will cause a blank row to be returned
	    if (retcode != 100)
	      {
		infoList_->insert((new(getHeap()) OutputInfo(1)));
		rowsFound = TRUE;
	      }

	    if (rowsFound)
	      infoList_->removeTail();

	    infoList_->position();

	    step_ = RETURN_ROW_;
	  }
	break;

	case FETCH_ALL_ROWS_IN_SCHEMA_:
	  {
	    if (initializeInfoList(infoList_))
	      {
		step_ = HANDLE_ERROR_;
		break;
	      }

	    NABoolean systemObjs = FALSE;
        
            // Our predecessor product allows options to return user objects,
            // system objects, or all objects (ausStr). These options have not
            // been implemented fully in Trafodion/Esgyn.  For now, ignore the 
            // aus option.
#if 0
	    char ausStr[20];
	    strcpy(ausStr, "");
	    if (getMItdb().systemObjs())
	      {
		strcpy(ausStr, "SYSTEM");
		systemObjs = TRUE;
	      }
	    else if (getMItdb().allObjs())
	      strcpy(ausStr, "ALL");
#endif

            NABoolean rowsFound = FALSE;

            const int numTypes = 9;
            const char* objectTypes[numTypes] = 
              {"tables", "views", "indexes", "sequences", "libraries", 
               "procedures", "functions", "table_mapping functions", "packages"
               /*, "mvs", "synonyms", "triggers"*/};

            for (int ot = 0; ot < numTypes; ot++)
              {
                NAString objectType(objectTypes[ot]);
                if (QualifiedName::isHive(getMItdb().getCat()) &&
                    (objectType != "tables" && objectType != "views"))
                  continue;


                // get objects for objectType
                str_sprintf(queryBuf_, "get %s in schema \"%s\".\"%s\" %s",
                            objectType.data(),
                            getMItdb().getCat(), getMItdb().getSch(),
                            patternStr_);
               
                retcode = 100;
                if (NOT systemObjs)
                  {
                    if (fetchAllRows(infoList_, queryBuf_, 1, FALSE, retcode) < 0)
                      {
                        step_ = HANDLE_ERROR_;
                    
                        break;
                      }
                  }
                
                // insert a NULL entry, this will cause a blank row to be returned
                if (retcode != 100) // some rows were found
                  {
                    infoList_->insert((new(getHeap()) OutputInfo(1)));
                    rowsFound = TRUE;
                  }
              } // list of objects

	    if (rowsFound)
	      infoList_->removeTail();

	    infoList_->position();

	    step_ = RETURN_ROW_;
	  }
	break;

	case DISPLAY_HEADING_:
	  {
	    if (infoList_->atEnd())
	      {
		step_ = DONE_;
		break;
	      }

	    retcode = displayHeading();
	    if (retcode == 1)
	      return WORK_OK;
	    else if (retcode < 0)
	      {
		step_ = HANDLE_ERROR_;
		break;
	      }

	    step_ = RETURN_ROW_;
	  }
	break;

	case RETURN_ROW_:
	  {
	    if (infoList_->atEnd())
	      {
		step_ = DONE_;
		break;
	      }

	    if (qparent_.up->isFull())
	      return WORK_OK;

	    OutputInfo * vi = (OutputInfo*)infoList_->getCurr();

	    short rc = 0;
	    char * ptr = vi->get(0);
	    short len = (short)(ptr ? strlen(ptr) : 0);

	    if (ptr)
	      moveRowToUpQueue(ptr, len, &rc);
	    else
	      moveRowToUpQueue(" ", 0, &rc);

	    infoList_->advance();
            incReturnRowCount();
	  }
	break;

	case HANDLE_ERROR_:
	  {
	    retcode = handleError();
	    if (retcode == 1)
	      return WORK_OK;

	    step_ = DONE_;
	  }
	break;

	case DONE_:
	  {
            if ((getMItdb().queryType_ != ComTdbExeUtilGetMetadataInfo::OBJECTS_IN_SCHEMA_) &&
                (NOT getMItdb().noHeader() && getReturnRowCount() > 0))
            {
              short rc = 0;
              char returnMsg[256];
              memset(returnMsg, 0, 256);
              sprintf(returnMsg, "\n=======================\n %d row(s) returned", getReturnRowCount());
              moveRowToUpQueue(returnMsg, strlen(returnMsg), &rc);
            }
	    retcode = handleDone();
	    if (retcode == 1)
	      return WORK_OK;

	    step_ = INITIAL_;

	    return WORK_OK;
	  }
	break;

	}
    }

  return 0;
}

////////////////////////////////////////////////////////////////
// Constructor for class ExExeUtilGetHbaseObjectsTcb
///////////////////////////////////////////////////////////////
ExExeUtilGetHbaseObjectsTcb::ExExeUtilGetHbaseObjectsTcb(
     const ComTdbExeUtilGetMetadataInfo & exe_util_tdb,
     ex_globals * glob)
     : ExExeUtilGetMetadataInfoTcb( exe_util_tdb, glob)
{
  ComStorageType storageType;

  switch (((ComTdbExeUtilGetMetadataInfo *)getTdb())->queryType()) {
     case ComTdbExeUtilGetMetadataInfo::MONARCH_OBJECTS_:
        storageType = COM_STORAGE_MONARCH;
        break;
     case ComTdbExeUtilGetMetadataInfo::BIGTABLE_OBJECTS_:
        storageType = COM_STORAGE_BIGTABLE;
        break;
     default: 
        storageType = COM_STORAGE_HBASE;
        break;
  }
  ehi_ = ExpHbaseInterface::newInstance(glob->getDefaultHeap(),
					(char*)exe_util_tdb.server(), 
					(char*)exe_util_tdb.zkPort(),
                                        storageType,
                                        FALSE); //replSync

  hbaseName_ = NULL;
  hbaseNameBuf_ = new(getGlobals()->getDefaultHeap()) 
    char[ComMAX_3_PART_EXTERNAL_UTF8_NAME_LEN_IN_BYTES+6+1];
  outBuf_ = new(getGlobals()->getDefaultHeap())
    char[ComMAX_3_PART_EXTERNAL_UTF8_NAME_LEN_IN_BYTES+6+1];

  hbaseTables_ = NULL;
}

ExExeUtilGetHbaseObjectsTcb::~ExExeUtilGetHbaseObjectsTcb()
{
  if (ehi_)
    delete ehi_;

  if (hbaseNameBuf_)
    NADELETEBASIC(hbaseNameBuf_, getGlobals()->getDefaultHeap());

  if (outBuf_)
    NADELETEBASIC(outBuf_, getGlobals()->getDefaultHeap());
}

//////////////////////////////////////////////////////
// work() for ExExeUtilGetHbaseObjectsTcb
//////////////////////////////////////////////////////
short ExExeUtilGetHbaseObjectsTcb::work()
{
  short retcode = 0;
  Lng32 cliRC = 0;
  ex_expr::exp_return_type exprRetCode = ex_expr::EXPR_OK;

 // if no parent request, return
  if (qparent_.down->isEmpty())
    return WORK_OK;

  // if no room in up queue, won't be able to return data/status.
  // Come back later.
  if (qparent_.up->isFull())
    return WORK_OK;
  
  ex_queue_entry * pentry_down = qparent_.down->getHeadEntry();
  ExExeUtilPrivateState & pstate =
    *((ExExeUtilPrivateState*) pentry_down->pstate);
  
  // Get the globals stucture of the master executor.
  ExExeStmtGlobals *exeGlob = getGlobals()->castToExExeStmtGlobals();
  ExMasterStmtGlobals *masterGlob = exeGlob->castToExMasterStmtGlobals();
  ContextCli * currContext = masterGlob->getStatement()->getContext();
  
  while (1)
    {
      switch (step_)
	{
	case INITIAL_:
	  {
            if (ehi_ == NULL)
              {
                step_ = HANDLE_ERROR_;
                break;
              }

            step_ = SETUP_HBASE_QUERY_;
	  }
	break;

        case SETUP_HBASE_QUERY_:
          {
            // Since HBase tables are native and Trafodion does not manage them
            // limit who can view these objects
            if (((currContext->getSqlParserFlags() & 0x20000) == 0) &&
                !ComUser::isRootUserID() && 
                !ComUser::currentUserHasRole(ROOT_ROLE_ID) &&
                !ComUser::currentUserHasRole(HBASE_ROLE_ID))
              {
                step_ = DONE_;
                break;
              }
            hbaseTables_ = ehi_->listAll("");
            if (! hbaseTables_)
              {
                step_ = DONE_;
                break;
              }

            currIndex_ = 0;

            if (currIndex_ == hbaseTables_->entries())
              {
                step_ = DONE_;
                break;
              }

            step_ = DISPLAY_HEADING_;
          }
          break;

        case DISPLAY_HEADING_:
          {
            retcode = displayHeading();
            if (retcode == 1)
              return WORK_OK;
            else if (retcode < 0)
              {
                step_ = HANDLE_ERROR_;
                break;
              }

            headingReturned_ = TRUE;

            step_ = PROCESS_NEXT_ROW_;
          }
        break;

        case PROCESS_NEXT_ROW_:
          {
            if (currIndex_ == hbaseTables_->entries())
              {
                step_ = DONE_;
                break;
              }

            HbaseStr *hbaseName = &hbaseTables_->at(currIndex_);
            if (hbaseName->len > ComMAX_3_PART_EXTERNAL_UTF8_NAME_LEN_IN_BYTES+6)
                hbaseName->len = ComMAX_3_PART_EXTERNAL_UTF8_NAME_LEN_IN_BYTES+6; 
            strncpy(hbaseNameBuf_, hbaseName->val, hbaseName->len);
            hbaseNameBuf_[hbaseName->len] = 0;
            hbaseName_ = hbaseNameBuf_;

            Lng32 numParts = 0;
            char *parts[4];
            if (LateNameInfo::extractParts(hbaseName_, outBuf_, numParts, parts, FALSE))
              {
                step_ = HANDLE_ERROR_;
                break;
              }

            currIndex_++;

            if (getMItdb().allObjs())
              {
                step_ = EVAL_EXPR_;
                break;
              }

            NABoolean sysObj = FALSE;
            NABoolean externalObj = FALSE;

            // only trafodion objects will be returned. They are names that
            // start with TRAFODION.
            NAString catalogNamePart((parts[0] ? parts[0] : ""));
            // extract namespace from catalogNamePart
            NAString nameSpace;
            char * catName = (char*)catalogNamePart.data();
            char * colonPos = strchr((char *)catalogNamePart.data(), ':');
            if (colonPos)
              {
                nameSpace = NAString(catName, (colonPos-catName));
                catalogNamePart = NAString(&catName[colonPos - catName + 1]);
              }

            if ((NOT nameSpace.isNull()) &&
                ((nameSpace.length() < TRAF_NAMESPACE_PREFIX_LEN) ||
                 (NAString(nameSpace(0,TRAF_NAMESPACE_PREFIX_LEN)) != 
                  TRAF_NAMESPACE_PREFIX)))
              {
                externalObj = TRUE;
              }
            else if (catalogNamePart != TRAFODION_SYSCAT_LIT)
              {
                externalObj = TRUE;
              }

            NAString schemaNamePart;
            if (numParts >= 2)
              schemaNamePart = parts[1];
            NAString objectNamePart;
            if (numParts >= 3)
              objectNamePart = parts[2];
            
            if (ComIsTrafodionReservedSchema("", catalogNamePart, schemaNamePart))
              {
                sysObj = TRUE;
              }
            
            if ((getMItdb().externalObjs()) &&
                (externalObj))
              {
                step_ = EVAL_EXPR_;
                break;
              }
            else if ((getMItdb().systemObjs()) &&
                     (sysObj))
              {
                step_ = EVAL_EXPR_;
                break;
              }
            else if ((getMItdb().userObjs()) &&
                     ((NOT sysObj) && (NOT externalObj)))
             {
                step_ = EVAL_EXPR_;
                break;
              }
 
            step_ = PROCESS_NEXT_ROW_;
          }
          break;

        case EVAL_EXPR_:
          {
            exprRetCode = evalScanExpr(hbaseName_, strlen(hbaseName_), TRUE);
	    if (exprRetCode == ex_expr::EXPR_FALSE)
	      {
		// row does not pass the scan expression,
		// move to the next row.
		step_ = PROCESS_NEXT_ROW_;
		break;
	      }
            
            step_ = RETURN_ROW_;
          }
          break;

        case RETURN_ROW_:
          {
	    if (qparent_.up->isFull())
	      return WORK_OK;

	    short rc = 0;
	    moveRowToUpQueue(hbaseName_, 0, &rc);

            step_ = PROCESS_NEXT_ROW_;
          }
          break;

	case HANDLE_ERROR_:
	  {
	    retcode = handleError();
	    if (retcode == 1)
	      return WORK_OK;

	    step_ = DONE_;
	  }
	break;

	case DONE_:
	  {
            if (hbaseTables_ != NULL) {
               deleteNAArray(getHeap(), hbaseTables_);
               hbaseTables_ = NULL;
            }
	    retcode = handleDone();
	    if (retcode == 1)
	      return WORK_OK;

	    step_ = INITIAL_;

	    return WORK_OK;
	  }
	break;
        }
    }

  return WORK_OK;
}


////////////////////////////////////////////////////////////////
// Constructor for class ExExeUtilGetNamespaceObjectsTcb
///////////////////////////////////////////////////////////////
ExExeUtilGetNamespaceObjectsTcb::ExExeUtilGetNamespaceObjectsTcb(
     const ComTdbExeUtilGetMetadataInfo & exe_util_tdb,
     ex_globals * glob)
     : ExExeUtilGetMetadataInfoTcb( exe_util_tdb, glob)
{
  int jniDebugPort = 0;
  int jniDebugTimeout = 0;
  ComStorageType storageType = COM_STORAGE_HBASE;
  ehi_ = ExpHbaseInterface::newInstance(glob->getDefaultHeap(),
					(char*)exe_util_tdb.server(), 
					(char*)exe_util_tdb.zkPort(),
                                        storageType,
                                        FALSE); //replSync

  hbaseName_ = NULL;
  hbaseNameMaxLen_ = 1028;
  hbaseNameBuf_ = new(getGlobals()->getDefaultHeap()) char[hbaseNameMaxLen_];
  namespaceObjects_ = NULL;

  step_ = INITIAL_;
}

ExExeUtilGetNamespaceObjectsTcb::~ExExeUtilGetNamespaceObjectsTcb()
{
  if (ehi_)
    delete ehi_;

  if (hbaseNameBuf_)
    NADELETEBASIC(hbaseNameBuf_, getGlobals()->getDefaultHeap());
}

//////////////////////////////////////////////////////
// work() for ExExeUtilGetNamespaceObjectsTcb
//////////////////////////////////////////////////////
short ExExeUtilGetNamespaceObjectsTcb::work()
{
  short retcode = 0;
  Lng32 cliRC = 0;
  ex_expr::exp_return_type exprRetCode = ex_expr::EXPR_OK;

 // if no parent request, return
  if (qparent_.down->isEmpty())
    return WORK_OK;

  // if no room in up queue, won't be able to return data/status.
  // Come back later.
  if (qparent_.up->isFull())
    return WORK_OK;
  
  ex_queue_entry * pentry_down = qparent_.down->getHeadEntry();
  ExExeUtilPrivateState & pstate =
    *((ExExeUtilPrivateState*) pentry_down->pstate);
  
  // Get the globals stucture of the master executor.
  ExExeStmtGlobals *exeGlob = getGlobals()->castToExExeStmtGlobals();
  ExMasterStmtGlobals *masterGlob = exeGlob->castToExMasterStmtGlobals();
  ContextCli * currContext = masterGlob->getStatement()->getContext();
  
  while (1)
    {
      switch (step_)
	{
	case INITIAL_:
	  {
            if (ehi_ == NULL)
              {
                step_ = HANDLE_ERROR_;
                break;
              }

            // Verify user has privs
            if (!ComUser::currentUserHasElevatedPrivs(NULL))
              {
                NAString privMDLoc = NAString(TRAFODION_SYSTEM_CATALOG) + NAString(".\"") +
                                     NAString(SEABASE_PRIVMGR_SCHEMA) + NAString("\"");
                PrivMgrComponentPrivileges componentPrivileges(std::string(privMDLoc.data()),getDiagsArea());

                if (!componentPrivileges.hasSQLPriv(ComUser::getCurrentUser(),SQLOperation::SHOW,true))
                  {
                    ExRaiseSqlError(getHeap(), &diagsArea_, -1017);
                    step_ = HANDLE_ERROR_;
                    break;
                  }
               }

            if (getMItdb().queryType() == ComTdbExeUtilGetMetadataInfo::EXTERNAL_NAMESPACES_ ||
                getMItdb().queryType() == ComTdbExeUtilGetMetadataInfo::TRAFODION_NAMESPACES_ ||
                getMItdb().queryType() == ComTdbExeUtilGetMetadataInfo::SYSTEM_NAMESPACES_ ||
                getMItdb().queryType() == ComTdbExeUtilGetMetadataInfo::ALL_NAMESPACES_)
              step_ = GET_NAMESPACES_;
            else
              step_ = CHECK_NAMESPACE_EXISTS_;

            currIndex_ = 0;
	  }
	break;

        case CHECK_NAMESPACE_EXISTS_:
          {

            retcode = ehi_->namespaceOperation(COM_CHECK_NAMESPACE_EXISTS,
                                               getMItdb().getObj(),
                                               0, NULL, NULL,
                                               NULL);
            
            if (retcode == -HBC_ERROR_NAMESPACE_NOT_EXIST)
              {
                ExRaiseSqlError(getHeap(), &diagsArea_, -1067,
                       NULL, NULL, NULL,
                       getMItdb().getObj());
                step_ = HANDLE_ERROR_;
                break;
              }

            if (getMItdb().queryType() == ComTdbExeUtilGetMetadataInfo::NAMESPACE_CONFIG_)
              step_ = GET_NAMESPACE_CONFIG_;
            else
              step_ = GET_OBJECTS_IN_NAMESPACE_;
          }
          break;

        case GET_NAMESPACES_:
          {
            retcode = ehi_->namespaceOperation(COM_GET_NAMESPACES, NULL, 
                                               0, NULL, NULL,
                                               &namespaceObjects_);

	    if (ExHbaseAccessTcb::setupError(
                     (NAHeap*)getMyHeap(), qparent_, retcode, 
                     "ExpHbaseInterface::namespaceOperation"))
	      {
		step_ = HANDLE_ERROR_;
		break;
	      }
            
            if (! namespaceObjects_ || (namespaceObjects_->entries() == 0))
              {
                step_ = DONE_;
                break;
              }

            step_ = PROCESS_NEXT_ROW_;
          }
          break;

        case GET_OBJECTS_IN_NAMESPACE_:
          {
            retcode = ehi_->namespaceOperation(COM_GET_NAMESPACE_OBJECTS, 
                                               getMItdb().getObj(),
                                               0, NULL, NULL,
                                               &namespaceObjects_);

	    if (ExHbaseAccessTcb::setupError(
                     (NAHeap*)getMyHeap(), qparent_, retcode, 
                     "ExpHbaseInterface::namespaceOperation"))
	      {
		step_ = HANDLE_ERROR_;
		break;
	      }
            
            if (! namespaceObjects_ || (namespaceObjects_->entries() == 0))
              {
                step_ = DONE_;
                break;
              }

            step_ = PROCESS_NEXT_ROW_;
          }
          break;

        case GET_NAMESPACE_CONFIG_:
          {
            retcode = ehi_->namespaceOperation(COM_GET_NAMESPACE_CONFIG, 
                                               getMItdb().getObj(),
                                               0, NULL, NULL,
                                               &namespaceObjects_);

	    if (ExHbaseAccessTcb::setupError(
                     (NAHeap*)getMyHeap(), qparent_, retcode, 
                     "ExpHbaseInterface::namespaceOperation"))
	      {
		step_ = HANDLE_ERROR_;
		break;
	      }
            
            if (! namespaceObjects_ || (namespaceObjects_->entries() == 0))
              {
                step_ = DONE_;
                break;
              }

            step_ = PROCESS_NEXT_ROW_;
          }
          break;

        case PROCESS_NEXT_ROW_:
          {
            if (getMItdb().queryType() == ComTdbExeUtilGetMetadataInfo::EXTERNAL_NAMESPACES_ ||
                getMItdb().queryType() == ComTdbExeUtilGetMetadataInfo::TRAFODION_NAMESPACES_ ||
                getMItdb().queryType() == ComTdbExeUtilGetMetadataInfo::SYSTEM_NAMESPACES_ ||
                getMItdb().queryType() == ComTdbExeUtilGetMetadataInfo::ALL_NAMESPACES_)
              step_ = PROCESS_NEXT_NAMESPACE_;
            else
              step_ = PROCESS_NEXT_OBJECT_IN_NAMESPACE_;
          }

        case PROCESS_NEXT_NAMESPACE_:
          {
            if (currIndex_ == namespaceObjects_->entries())
              {
                step_ = DONE_;
                break;
              }

            HbaseStr *hbaseName = &namespaceObjects_->at(currIndex_);
            if (hbaseName->len > hbaseNameMaxLen_)
              {
                hbaseNameMaxLen_ = hbaseName->len;
                NADELETEBASIC(hbaseNameBuf_, getGlobals()->getDefaultHeap());
                hbaseNameBuf_ = new(getGlobals()->getDefaultHeap()) char[hbaseNameMaxLen_];
              }

            currIndex_++;

            strncpy(hbaseNameBuf_, hbaseName->val, hbaseName->len);
            hbaseNameBuf_[hbaseName->len] = 0;
            hbaseName_ = hbaseNameBuf_;
            Lng32 hbaseNameLen = strlen(hbaseName_);

            NABoolean isTraf = FALSE;
            NABoolean isSys = FALSE;
            NABoolean isExt = FALSE;
            if (hbaseNameLen >= TRAF_NAMESPACE_PREFIX_LEN && 
                (strncmp(hbaseName_, TRAF_NAMESPACE_PREFIX, TRAF_NAMESPACE_PREFIX_LEN) == 0))
              isTraf = TRUE;
            else if ((strcmp(hbaseName_, "default") == 0) || (strcmp(hbaseName_, "hbase") == 0))
              isSys = TRUE;
            else if (hbaseNameLen < TRAF_NAMESPACE_PREFIX_LEN || 
                     (strncmp(hbaseName_, TRAF_NAMESPACE_PREFIX, TRAF_NAMESPACE_PREFIX_LEN) != 0))
              isExt = TRUE;

            if (((getMItdb().queryType() == ComTdbExeUtilGetMetadataInfo::TRAFODION_NAMESPACES_) &&
                 (NOT isTraf)) ||
                ((getMItdb().queryType() == ComTdbExeUtilGetMetadataInfo::EXTERNAL_NAMESPACES_) &&
                 (NOT isExt)) ||
                ((getMItdb().queryType() == ComTdbExeUtilGetMetadataInfo::SYSTEM_NAMESPACES_) &&
                 (NOT isSys)))
              {
                step_ = PROCESS_NEXT_ROW_;
                break;
              }

            if (getMItdb().getScanExpr())
              step_ = EVAL_EXPR_;
            else
              step_ = RETURN_ROW_;
          }
          break;

        case PROCESS_NEXT_OBJECT_IN_NAMESPACE_:
          {
            if (currIndex_ == namespaceObjects_->entries())
              {
                step_ = DONE_;
                break;
              }

            currIndex_++;

            HbaseStr *hbaseName = &namespaceObjects_->at(currIndex_);
            if (hbaseName->len > hbaseNameMaxLen_)
              {
                hbaseNameMaxLen_ = hbaseName->len;
                NADELETEBASIC(hbaseNameBuf_, getGlobals()->getDefaultHeap());
                hbaseNameBuf_ = new(getGlobals()->getDefaultHeap()) char[hbaseNameMaxLen_];
              }

            strncpy(hbaseNameBuf_, hbaseName->val, hbaseName->len);
            hbaseNameBuf_[hbaseName->len] = 0;
            hbaseName_ = hbaseNameBuf_;
            Lng32 hbaseNameLen = strlen(hbaseName_);

            if (getMItdb().getScanExpr())
              step_ = EVAL_EXPR_;
            else
              step_ = RETURN_ROW_;
          }
          break;

        case EVAL_EXPR_:
          {
            exprRetCode = evalScanExpr(hbaseName_, strlen(hbaseName_), TRUE);
	    if (exprRetCode == ex_expr::EXPR_FALSE)
	      {
		// row does not pass the scan expression,
		// move to the next row.
		step_ = PROCESS_NEXT_ROW_;
		break;
	      }
            
            step_ = RETURN_ROW_;
          }
          break;

        case RETURN_ROW_:
          {
	    if (qparent_.up->isFull())
	      return WORK_OK;

	    short rc = 0;
	    moveRowToUpQueue(hbaseName_, 0, &rc);

            step_ = PROCESS_NEXT_ROW_;
          }
          break;

	case HANDLE_ERROR_:
	  {
	    retcode = handleError();
	    if (retcode == 1)
	      return WORK_OK;

	    step_ = DONE_;
	  }
	break;

	case DONE_:
	  {
            if (namespaceObjects_ != NULL) 
              {
                deleteNAArray(getHeap(), namespaceObjects_);
                namespaceObjects_ = NULL;
              }
	    retcode = handleDone();
	    if (retcode == 1)
	      return WORK_OK;

	    step_ = INITIAL_;

	    return WORK_OK;
	  }
	break;
        }
    }

  return WORK_OK;
}

////////////////////////////////////////////////////////////////////////
// Redefine virtual method allocatePstates, to be used by dynamic queue
// resizing, as well as the initial queue construction.
////////////////////////////////////////////////////////////////////////
ex_tcb_private_state * ExExeUtilGetMetadataInfoTcb::allocatePstates(
     Lng32 &numElems,      // inout, desired/actual elements
     Lng32 &pstateLength)  // out, length of one element
{
  PstateAllocator<ExExeUtilGetMetadataInfoPrivateState> pa;

  return pa.allocatePstates(this, numElems, pstateLength);
}


/////////////////////////////////////////////////////////////////////////////
// Constructor and destructor for ExeUtil_private_state
/////////////////////////////////////////////////////////////////////////////
ExExeUtilGetMetadataInfoPrivateState::ExExeUtilGetMetadataInfoPrivateState()
{
}

ExExeUtilGetMetadataInfoPrivateState::~ExExeUtilGetMetadataInfoPrivateState()
{
};



///////////////////////////////////////////////////////////////////
// class ExExeUtilShowSetTdb
///////////////////////////////////////////////////////////////
ex_tcb * ExExeUtilShowSetTdb::build(ex_globals * glob)
{
  ExExeUtilShowSetTcb * exe_util_tcb;

  exe_util_tcb = new(glob->getSpace()) ExExeUtilShowSetTcb(*this, glob);

  exe_util_tcb->registerSubtasks();

  return (exe_util_tcb);
}

ExExeUtilShowSetTcb::ExExeUtilShowSetTcb(const ComTdbExeUtilShowSet & exe_util_tdb,
					 ex_globals * glob)
     : ExExeUtilTcb(exe_util_tdb, NULL, glob),
       step_(EMPTY_)
{
}

short ExExeUtilShowSetTcb::work()
{
  // if no parent request, return
  if (qparent_.down->isEmpty())
    return WORK_OK;

  ex_queue_entry * pentry_down = qparent_.down->getHeadEntry();

  ContextCli *currContext =
      getGlobals()->castToExExeStmtGlobals()->castToExMasterStmtGlobals()->
        getStatement()->getContext();

  SessionDefaults * sd = currContext->getSessionDefaults();

  while (1)
    {
      switch (step_)
	{
	case EMPTY_:
	  {
	    sd->position();

	    step_ = RETURN_HEADER_;
	  }
	break;

	case RETURN_HEADER_:
	  {
	    // if no room in up queue for 2 display rows,
	    // won't be able to return data/status.
	    // Come back later.
	    if ((qparent_.up->getSize() - qparent_.up->getLength()) < 3)
	      return -1;

	    moveRowToUpQueue("  ");
	    moveRowToUpQueue("Current SESSION DEFAULTs");

	    step_ = RETURNING_DEFAULT_;
	  }
	break;

	case RETURNING_DEFAULT_:
	  {
	    // if no room in up queue, won't be able to return data/status.
	    // Come back later.
	    if (qparent_.up->isFull())
	      return WORK_OK;

	    char * attributeString = NULL;
	    char * attributeValue  = NULL;
	    Lng32 isCQD;
	    Lng32 fromDefaultsTable;
	    Lng32 isSSD;
	    Lng32 isExternalized = 0;
	    Lng32 eof = 0;
	    while ((NOT eof) && (NOT isExternalized))
	      {
		eof = sd->getNextSessionDefault(attributeString,
						attributeValue,
						isCQD,
						fromDefaultsTable,
						isSSD,
						isExternalized);

		if (ssTdb().getType() == ComTdbExeUtilShowSet::ALL_)
		  isExternalized = TRUE;
	      }

	    if (eof)
	      {
		step_ = DONE_;
		break;
	      }

	    char formattedStr[2000];
	    strcpy(formattedStr, "  ");
	    byte_str_cpy(&formattedStr[2], 28, attributeString,
			 strlen(attributeString),' ');
	    formattedStr[2+28] = 0;

	    if (attributeValue)
	      strcat(formattedStr, attributeValue);

	    moveRowToUpQueue(formattedStr);
	  }
	break;

	case DONE_:
	  {
	    if (qparent_.up->isFull())
	      return WORK_OK;

	    // all ok. Return EOF.
	    ex_queue_entry * up_entry = qparent_.up->getTailEntry();

	    up_entry->upState.parentIndex =
	      pentry_down->downState.parentIndex;

	    up_entry->upState.setMatchNo(0);
	    up_entry->upState.status = ex_queue::Q_NO_DATA;

	    // insert into parent
	    qparent_.up->insert();

	    step_ = EMPTY_;

	    qparent_.down->removeHead();

	    return WORK_OK;
	  }
	  break;

	} // switch
    }

  return 0;
}


///////////////////////////////////////////////////////////////////
ex_tcb * ExExeUtilGetUIDTdb::build(ex_globals * glob)
{
  ex_tcb * exe_util_tcb;

  exe_util_tcb = new(glob->getSpace()) ExExeUtilGetUIDTcb(*this, glob);

  exe_util_tcb->registerSubtasks();

  return (exe_util_tcb);
}

////////////////////////////////////////////////////////////////
// Constructor for class ExExeUtilGetUIDTcb
///////////////////////////////////////////////////////////////
ExExeUtilGetUIDTcb::ExExeUtilGetUIDTcb(
     const ComTdbExeUtilGetUID & exe_util_tdb,
     ex_globals * glob)
     : ExExeUtilTcb( exe_util_tdb, NULL, glob)
{
  // Allocate the private state in each entry of the down queue
  qparent_.down->allocatePstate(this);

  step_ = INITIAL_;

}

ExExeUtilGetUIDTcb::~ExExeUtilGetUIDTcb()
{
}

//////////////////////////////////////////////////////
// work() for ExExeUtilGetUIDTcb
//////////////////////////////////////////////////////
short ExExeUtilGetUIDTcb::work()
{
  //  short rc = 0;
  Lng32 cliRC = 0;

  // if no parent request, return
  if (qparent_.down->isEmpty())
    return WORK_OK;

  // if no room in up queue, won't be able to return data/status.
  // Come back later.
  if (qparent_.up->isFull())
    return WORK_OK;

  ex_queue_entry * pentry_down = qparent_.down->getHeadEntry();
  ExExeUtilPrivateState & pstate =
    *((ExExeUtilPrivateState*) pentry_down->pstate);

  // Get the globals stucture of the master executor.
  ExExeStmtGlobals *exeGlob = getGlobals()->castToExExeStmtGlobals();
  ExMasterStmtGlobals *masterGlob = exeGlob->castToExMasterStmtGlobals();
  ContextCli * currContext = masterGlob->getStatement()->getContext();

  while (1)
    {
      switch (step_)
	{
	case INITIAL_:
	  {
	    step_ = RETURN_UID_;
	  }
	break;

	case RETURN_UID_:
	  {
	    if (qparent_.up->isFull())
	      return WORK_OK;

	    moveRowToUpQueue((const char*)&(getUIDTdb().uid_),
			     sizeof(getUIDTdb().uid_),
			     NULL, FALSE);

	    step_ = DONE_;
	  }
	break;
      case DONE_:
	  {
	    if (qparent_.up->isFull())
	      return WORK_OK;

	    // Return EOF.
	    ex_queue_entry * up_entry = qparent_.up->getTailEntry();

	    up_entry->upState.parentIndex =
	      pentry_down->downState.parentIndex;

	    up_entry->upState.setMatchNo(0);
	    up_entry->upState.status = ex_queue::Q_NO_DATA;

	    // insert into parent
	    qparent_.up->insert();

	    qparent_.down->removeHead();

	    step_ = INITIAL_;
	    return WORK_OK;
	  }

	break;

	default:
	  break;

	}

    }

  return 0;
}


////////////////////////////////////////////////////////////////////////
// Redefine virtual method allocatePstates, to be used by dynamic queue
// resizing, as well as the initial queue construction.
////////////////////////////////////////////////////////////////////////
ex_tcb_private_state * ExExeUtilGetUIDTcb::allocatePstates(
     Lng32 &numElems,      // inout, desired/actual elements
     Lng32 &pstateLength)  // out, length of one element
{
  PstateAllocator<ExExeUtilGetUIDPrivateState> pa;

  return pa.allocatePstates(this, numElems, pstateLength);
}

/////////////////////////////////////////////////////////////////////////////
// Constructor and destructor for ExeUtil_private_state
/////////////////////////////////////////////////////////////////////////////
ExExeUtilGetUIDPrivateState::ExExeUtilGetUIDPrivateState()
{
}

ExExeUtilGetUIDPrivateState::~ExExeUtilGetUIDPrivateState()
{
};

///////////////////////////////////////////////////////////////////
ex_tcb * ExExeUtilGetQIDTdb::build(ex_globals * glob)
{
  ex_tcb * exe_util_tcb;

  exe_util_tcb = new(glob->getSpace()) ExExeUtilGetQIDTcb(*this, glob);

  exe_util_tcb->registerSubtasks();

  return (exe_util_tcb);
}

////////////////////////////////////////////////////////////////
// Constructor for class ExExeUtilGetQIDTcb
///////////////////////////////////////////////////////////////
ExExeUtilGetQIDTcb::ExExeUtilGetQIDTcb(
     const ComTdbExeUtilGetQID & exe_util_tdb,
     ex_globals * glob)
     : ExExeUtilTcb( exe_util_tdb, NULL, glob)
{
  // Allocate the private state in each entry of the down queue
  qparent_.down->allocatePstate(this);

  step_ = INITIAL_;

}

ExExeUtilGetQIDTcb::~ExExeUtilGetQIDTcb()
{
}

//////////////////////////////////////////////////////
// work() for ExExeUtilGetQIDTcb
//////////////////////////////////////////////////////
short ExExeUtilGetQIDTcb::work()
{
  short retcode = 0;
  Lng32 cliRC = 0;

  // if no parent request, return
  if (qparent_.down->isEmpty())
    return WORK_OK;

  // if no room in up queue, won't be able to return data/status.
  // Come back later.
  if (qparent_.up->isFull())
    return WORK_OK;

  ex_queue_entry * pentry_down = qparent_.down->getHeadEntry();
  ExExeUtilPrivateState & pstate =
    *((ExExeUtilPrivateState*) pentry_down->pstate);

  // Get the globals stucture of the master executor.
  ExExeStmtGlobals *exeGlob = getGlobals()->castToExExeStmtGlobals();
  ExMasterStmtGlobals *masterGlob = exeGlob->castToExMasterStmtGlobals();
  ContextCli * currContext = masterGlob->getStatement()->getContext();

  while (1)
    {
      switch (step_)
	{
	case INITIAL_:
	  {
	    step_ = RETURN_QID_;
	  }
	break;

	case RETURN_QID_:
	  {
	    if (qparent_.up->isFull())
	      return WORK_OK;

            /* get statement from context */
            SQLMODULE_ID module;
            init_SQLMODULE_ID(&module);

            SQLSTMT_ID stmtId;
            memset (&stmtId, 0, sizeof(SQLSTMT_ID));

            // Allocate a SQL statement
            init_SQLSTMT_ID(&stmtId, SQLCLI_CURRENT_VERSION, stmt_name, 
                            &module, getQIDTdb().getStmtName(), NULL, NULL, 
                            strlen(getQIDTdb().getStmtName()));

            Statement * stmt = currContext->getStatement(&stmtId);
            
            /* stmt must exist */
            if (!stmt)
              {
                ExRaiseSqlError(getHeap(), &diagsArea_, -CLI_STMT_NOT_EXISTS);
                step_ = ERROR_;
                break;
              }
            
	    moveRowToUpQueue(stmt->getUniqueStmtId());

	    step_ = DONE_;
	  }
	break;

	case ERROR_:
	  {
	    retcode = handleError();
	    if (retcode == 1)
	      return WORK_OK;

	    step_ = DONE_;
	  }
	break;

	case DONE_:
	  {
	    retcode = handleDone();
	    if (retcode == 1)
	      return WORK_OK;

	    step_ = INITIAL_;
	    return WORK_OK;
	  }

	break;

	default:
	  break;

	}

    }

  return 0;
}


////////////////////////////////////////////////////////////////////////
// Redefine virtual method allocatePstates, to be used by dynamic queue
// resizing, as well as the initial queue construction.
////////////////////////////////////////////////////////////////////////
ex_tcb_private_state * ExExeUtilGetQIDTcb::allocatePstates(
     Lng32 &numElems,      // inout, desired/actual elements
     Lng32 &pstateLength)  // out, length of one element
{
  PstateAllocator<ExExeUtilGetQIDPrivateState> pa;

  return pa.allocatePstates(this, numElems, pstateLength);
}

/////////////////////////////////////////////////////////////////////////////
// Constructor and destructor for ExeUtil_private_state
/////////////////////////////////////////////////////////////////////////////
ExExeUtilGetQIDPrivateState::ExExeUtilGetQIDPrivateState()
{
}

ExExeUtilGetQIDPrivateState::~ExExeUtilGetQIDPrivateState()
{
};

///////////////////////////////////////////////////////////////////
ex_tcb * ExExeUtilGetErrorInfoTdb::build(ex_globals * glob)
{
  ExExeUtilGetErrorInfoTcb * exe_util_tcb;

  exe_util_tcb =
    new(glob->getSpace()) ExExeUtilGetErrorInfoTcb(*this, glob);

  exe_util_tcb->registerSubtasks();

  return (exe_util_tcb);
}

////////////////////////////////////////////////////////////////
// Constructor for class ExExeUtilGetErrorInfoTcb
///////////////////////////////////////////////////////////////
ExExeUtilGetErrorInfoTcb::ExExeUtilGetErrorInfoTcb(
     const ComTdbExeUtilGetErrorInfo & exe_util_tdb,
     ex_globals * glob)
     : ExExeUtilTcb( exe_util_tdb, NULL, glob)
{
  // Allocate the private state in each entry of the down queue
  qparent_.down->allocatePstate(this);

  step_ = INITIAL_;

  // buffer where output will be formatted
  outputBuf_ = new(glob->getDefaultHeap()) char[4096];
}

//////////////////////////////////////////////////////
// work() for ExExeUtilGetErrorInfoTcb
//////////////////////////////////////////////////////
short ExExeUtilGetErrorInfoTcb::work()
{
  short retcode = 0;
  Lng32 cliRC = 0;
  ex_expr::exp_return_type exprRetCode = ex_expr::EXPR_OK;

  // if no parent request, return
  if (qparent_.down->isEmpty())
    return WORK_OK;

  // if no room in up queue, won't be able to return data/status.
  // Come back later.
  if (qparent_.up->isFull())
    return WORK_OK;

  ex_queue_entry * pentry_down = qparent_.down->getHeadEntry();
  ExExeUtilPrivateState & pstate =
    *((ExExeUtilPrivateState*) pentry_down->pstate);

  // Get the globals stucture of the master executor.
  ExExeStmtGlobals *exeGlob = getGlobals()->castToExExeStmtGlobals();
  ExMasterStmtGlobals *masterGlob = exeGlob->castToExMasterStmtGlobals();
  ContextCli * currContext = masterGlob->getStatement()->getContext();

  while (1)
    {
      switch (step_)
	{
	case INITIAL_:
	  {
	    step_ = RETURN_TEXT_;
	  }
	break;

	case RETURN_TEXT_:
	  {
	    if ((qparent_.up->getSize() - qparent_.up->getLength()) < 10)
	      return WORK_OK;

	    Lng32 warnNum = ABS(geiTdb().errNum_);
	    Lng32 errNum = -geiTdb().errNum_;
	    ErrorType errType = (ErrorType)geiTdb().errorType_;

	    char sqlstateErr[10];
	    char sqlstateWarn[10];
      if (errType == MAIN_ERROR) {
	      ComSQLSTATE(errNum, sqlstateErr);
	      ComSQLSTATE(warnNum, sqlstateWarn);
      }

	    NAWchar * errorMsg;
	    NABoolean msgNotFound = 
	      GetErrorMessage(errType, errNum, errorMsg, ERROR_TEXT);

	    Lng32 bufSize = 2 * ErrorMessage::MSG_BUF_SIZE + 16;
	    char * isoErrorMsg = new(getGlobals()->getDefaultHeap()) 
	      char[bufSize];

	    moveRowToUpQueue("");

	    if ((! msgNotFound) || (errNum == 0))
	      {	      

		UnicodeStringToLocale
		  (CharInfo::ISO88591,
		   errorMsg, NAWstrlen(errorMsg), isoErrorMsg, bufSize);

        if (errNum != 0)
        {
          GetErrorMessage(errType, errNum, errorMsg, CAUSE_TEXT);
          strcat(isoErrorMsg, "\n");
          int len = strlen(isoErrorMsg);
          UnicodeStringToLocale(CharInfo::ISO88591,
                                errorMsg, NAWstrlen(errorMsg), isoErrorMsg + len, bufSize - len);

          GetErrorMessage(errType, errNum, errorMsg, EFFECT_TEXT);
          strcat(isoErrorMsg, "\n");
          len = strlen(isoErrorMsg);
          UnicodeStringToLocale(CharInfo::ISO88591,
                                errorMsg, NAWstrlen(errorMsg), isoErrorMsg + len, bufSize - len);

          GetErrorMessage(errType, errNum, errorMsg, RECOVERY_TEXT);
          strcat(isoErrorMsg, "\n");
          len = strlen(isoErrorMsg);
          UnicodeStringToLocale(CharInfo::ISO88591,
                                errorMsg, NAWstrlen(errorMsg), isoErrorMsg + len, bufSize - len);
        }

    if (errType == MAIN_ERROR) {
		  str_sprintf(outputBuf_, "*** SQLSTATE (Err): %s SQLSTATE (Warn): %s", 
			    sqlstateErr, sqlstateWarn);
		  moveRowToUpQueue(outputBuf_);
    }

		str_sprintf(outputBuf_, "%s", isoErrorMsg);
		moveRowToUpQueue(outputBuf_);
	      }
	    else
	      {
		str_sprintf(outputBuf_, "*** WARNING[%d]",
			    warnNum);
		moveRowToUpQueue(outputBuf_);

		str_sprintf(outputBuf_, "*** ERROR[16001] The error number %d is not used in SQL.",
			    warnNum);
		moveRowToUpQueue(outputBuf_);
	      }

	    NADELETEBASIC(isoErrorMsg, getGlobals()->getDefaultHeap());

	    step_ = DONE_;
	  }
	  break;

	case DONE_:
	  {
	    if (qparent_.up->isFull())
	      return WORK_OK;

	    // Return EOF.
	    ex_queue_entry * up_entry = qparent_.up->getTailEntry();

	    up_entry->upState.parentIndex =
	      pentry_down->downState.parentIndex;

	    up_entry->upState.setMatchNo(0);
	    up_entry->upState.status = ex_queue::Q_NO_DATA;

	    // insert into parent
	    qparent_.up->insert();

	    qparent_.down->removeHead();

	    step_ = INITIAL_;
	    return WORK_OK;
	  } // case
	  break;
	} // switch
    } // while
}

////////////////////////////////////////////////////////////////////////
// Redefine virtual method allocatePstates, to be used by dynamic queue
// resizing, as well as the initial queue construction.
////////////////////////////////////////////////////////////////////////
ex_tcb_private_state * ExExeUtilGetErrorInfoTcb::allocatePstates(
     Lng32 &numElems,      // inout, desired/actual elements
     Lng32 &pstateLength)  // out, length of one element
{
  PstateAllocator<ExExeUtilGetErrorInfoPrivateState> pa;

  return pa.allocatePstates(this, numElems, pstateLength);
}


/////////////////////////////////////////////////////////////////////////////
// Constructor and destructor for ExeUtil_private_state
/////////////////////////////////////////////////////////////////////////////
ExExeUtilGetErrorInfoPrivateState::ExExeUtilGetErrorInfoPrivateState()
{
}

ExExeUtilGetErrorInfoPrivateState::~ExExeUtilGetErrorInfoPrivateState()
{
}




///////////////////////////////////////////////////////////////////
ex_tcb * ExExeUtilRegionStatsTdb::build(ex_globals * glob)
{
  ExExeUtilRegionStatsTcb * exe_util_tcb;

  if (displayFormat())
    exe_util_tcb = new(glob->getSpace()) ExExeUtilRegionStatsFormatTcb(*this, glob);
  else if (clusterView())
    exe_util_tcb = new(glob->getSpace()) ExExeUtilClusterStatsTcb(*this, glob);
  else
    exe_util_tcb = new(glob->getSpace()) ExExeUtilRegionStatsTcb(*this, glob);
  
  exe_util_tcb->registerSubtasks();

  return (exe_util_tcb);
}

////////////////////////////////////////////////////////////////
// Constructor for class ExExeUtilRegionStatsTcb
///////////////////////////////////////////////////////////////
ExExeUtilRegionStatsTcb::ExExeUtilRegionStatsTcb(
     const ComTdbExeUtilRegionStats & exe_util_tdb,
     ex_globals * glob)
     : ExExeUtilTcb( exe_util_tdb, NULL, glob)
{
  statsBuf_ = new(glob->getDefaultHeap()) char[sizeof(ComTdbRegionStatsVirtTableColumnStruct)];
  statsBufLen_ = sizeof(ComTdbRegionStatsVirtTableColumnStruct);

  stats_ = (ComTdbRegionStatsVirtTableColumnStruct*)statsBuf_;

  inputNameBuf_ = NULL;
  if (exe_util_tdb.inputExpr_)
    {
      inputNameBuf_ = new(glob->getDefaultHeap()) char[exe_util_tdb.inputRowlen_];
    }

  int jniDebugPort = 0;
  int jniDebugTimeout = 0;
  ehi_ = ExpHbaseInterface::newInstance(glob->getDefaultHeap(),
					(char*)"", //exe_util_tdb.server(), 
					(char*)"", //exe_util_tdb.zkPort(),
                                        COM_STORAGE_HBASE, 
                                        FALSE);
  regionInfoList_ = NULL;
  
  tableName_ = new(glob->getDefaultHeap()) char[2000];

  tableNameForUID_ = new(glob->getDefaultHeap()) char[2000];

  // get hbase rootdir location. Max linux pathlength is 1024.
  hbaseRootdir_ = new(glob->getDefaultHeap()) char[1030];
  strcpy(hbaseRootdir_, "/hbase");

  step_ = INITIAL_;
}

ExExeUtilRegionStatsTcb::~ExExeUtilRegionStatsTcb()
{
  if (statsBuf_)
    NADELETEBASIC(statsBuf_, getGlobals()->getDefaultHeap());

  if (ehi_)
    delete ehi_;

  statsBuf_ = NULL;
}

//////////////////////////////////////////////////////
// work() for ExExeUtilRegionStatsTcb
//////////////////////////////////////////////////////
Int64 ExExeUtilRegionStatsTcb::getEmbeddedNumValue
(char* &sep, char endChar, NABoolean adjustLen)
{
  Int64 num = -1;
  char * sepEnd = strchr(sep+1, endChar);
  if (sepEnd)
    {
      char longBuf[30];

      Lng32 len = sepEnd - sep - 1;
      str_cpy_all(longBuf, (sep+1), len);
      longBuf[len] = 0;                
      
      num = str_atoi(longBuf, len);

      sep += len + 1;

      if ((adjustLen) && (num == 0))
        num = 1024;
    }

  return num;
}

short ExExeUtilRegionStatsTcb::collectStats(char * inTableName, char * INtableNameForUID, NABoolean replaceUID)
{
  char * tableName = inTableName;
  char * tableNameForUID = INtableNameForUID;

  NABoolean isHbase = FALSE;
  if (getDLStdb().getCatName() &&
      (NAString(getDLStdb().getCatName()) == HBASE_SYSTEM_CATALOG))
    isHbase = TRUE;

  char * colonPos = NULL;
  NAString nameSpace;
  if (NOT isHbase)
    {
      // extract namespace, if present
      colonPos = strchr(tableName, ':');
      if (colonPos)
        {
          nameSpace = NAString(inTableName, (colonPos-inTableName));
          tableName = &inTableName[colonPos - inTableName + 1];
          tableNameForUID = &INtableNameForUID[colonPos - INtableNameForUID + 1];
        }
    }

  // populate catName_, schName_, objName_.
  if (extractParts(tableName,
                   &catName_, &schName_, &objName_,
                   TRUE))
    {
      return -1;
    }

  // collect stats from ehi.
  HbaseStr tblName;
  
  if (replaceUID)
  {
    if (! colonPos)
      extNameForHbase_ = NAString(tableNameForUID);
    else
      extNameForHbase_ = nameSpace + ":" + NAString(tableNameForUID);
  }
  else
  {
    if (NAString(catName_) == HBASE_SYSTEM_CATALOG)
      extNameForHbase_ = NAString(objName_);
    else if (! colonPos)
      extNameForHbase_ =
        NAString(catName_) + "." + NAString(schName_) + "." + NAString(objName_);
    else
      extNameForHbase_ = nameSpace + ":" +
        NAString(catName_) + "." + NAString(schName_) + "." + NAString(objName_);
  }

  tblName.val = (char*)extNameForHbase_.data();
  tblName.len = extNameForHbase_.length();
  
  regionInfoList_ = ehi_->getRegionStats(tblName);
  if (! regionInfoList_)
    {
      return -1;
    }
 
  currIndex_ = 0;
  return 0;
}

short ExExeUtilRegionStatsTcb::populateStats
(Int32 currIndex)
{
  str_pad(stats_->catalogName, sizeof(stats_->catalogName), ' ');
  str_cpy_all(stats_->catalogName, catName_, strlen(catName_));

  str_pad(stats_->schemaName, sizeof(stats_->schemaName), ' ');
  str_cpy_all(stats_->schemaName, schName_, strlen(schName_));

  str_pad(stats_->objectName, sizeof(stats_->objectName), ' ');
  str_cpy_all(stats_->objectName, objName_, strlen(objName_));
  
  str_pad(stats_->regionServer, sizeof(stats_->regionServer), ' ');

  str_pad(stats_->regionName, sizeof(stats_->regionName), ' ');
  stats_->regionNum       = currIndex_+1;
  
  char regionInfoBuf[5000];
  Int32 len = 0;
  char *regionInfo = regionInfoBuf;
  char *val = regionInfoList_->at(currIndex).val;
  len = regionInfoList_->at(currIndex).len; 
  if (len >= sizeof(regionInfoBuf))
     len = sizeof(regionInfoBuf)-1;
  strncpy(regionInfoBuf, val, len);
  regionInfoBuf[len] = '\0';
  stats_->numStores                = 0;
  stats_->numStoreFiles            = 0;
  stats_->storeFileUncompSize      = 0;
  stats_->storeFileSize            = 0;
  stats_->memStoreSize             = 0;
  
  char longBuf[30];
  char * sep1 = strchr(regionInfo, '|');
  if (sep1)
    {
      str_cpy_all(stats_->regionServer, regionInfo, 
                  (Lng32)(sep1 - regionInfo)); 
    }

  char * sepStart = sep1+1;
  sep1 = strchr(sepStart, '|');
  if (sep1)
    {
      str_cpy_all(stats_->regionName, sepStart, 
                  (Lng32)(sep1 - sepStart)); 
    }
  
  sepStart = sep1;
  stats_->numStores = getEmbeddedNumValue(sepStart, '|', FALSE);
  stats_->numStoreFiles = getEmbeddedNumValue(sepStart, '|', FALSE);
  stats_->storeFileUncompSize = getEmbeddedNumValue(sepStart, '|', FALSE);
  stats_->storeFileSize = getEmbeddedNumValue(sepStart, '|', FALSE);
  stats_->memStoreSize = getEmbeddedNumValue(sepStart, '|', FALSE);
  stats_->readRequestsCount = getEmbeddedNumValue(sepStart, '|', FALSE);
  stats_->writeRequestsCount = getEmbeddedNumValue(sepStart, '|', FALSE);
 
  return 0;
}

short ExExeUtilRegionStatsTcb::work()
{
  short retcode = 0;
  Lng32 cliRC = 0;
  ex_expr::exp_return_type exprRetCode = ex_expr::EXPR_OK;

  // if no parent request, return
  if (qparent_.down->isEmpty())
    return WORK_OK;
  
  // if no room in up queue, won't be able to return data/status.
  // Come back later.
  if (qparent_.up->isFull())
    return WORK_OK;
  
  ex_queue_entry * pentry_down = qparent_.down->getHeadEntry();
  ExExeUtilPrivateState & pstate =
    *((ExExeUtilPrivateState*) pentry_down->pstate);

  // Get the globals stucture of the master executor.
  ExExeStmtGlobals *exeGlob = getGlobals()->castToExExeStmtGlobals();
  ExMasterStmtGlobals *masterGlob = exeGlob->castToExMasterStmtGlobals();
  ContextCli * currContext = masterGlob->getCliGlobals()->currContext();

  while (1)
    {
      switch (step_)
	{
	case INITIAL_:
	  {
            if (ehi_ == NULL)
              {
                step_ = HANDLE_ERROR_;
                break;
              }

            if (getDLStdb().inputExpr())
              {
                step_ = EVAL_INPUT_;
                break;
              }

            if (getDLStdb().replaceNameByUID())
              strcpy(tableNameForUID_, getDLStdb().getDataUIDName());
            strcpy(tableName_, getDLStdb().getTableName());

	    step_ = COLLECT_STATS_;
	  }
	break;

        case EVAL_INPUT_:
          {
	    workAtp_->getTupp(getDLStdb().workAtpIndex())
	      .setDataPointer(inputNameBuf_);

	    ex_expr::exp_return_type exprRetCode =
	      getDLStdb().inputExpr()->eval(pentry_down->getAtp(), workAtp_);
	    if (exprRetCode == ex_expr::EXPR_ERROR)
	      {
		step_ = HANDLE_ERROR_;
		break;
	      }

            short len = *(short*)inputNameBuf_;
            str_cpy_all(tableName_, &inputNameBuf_[2], len);
            tableName_[len] = 0;

            step_ = COLLECT_STATS_;
          }
          break;

        case COLLECT_STATS_:
          {
            if (collectStats(tableName_, tableNameForUID_, getDLStdb().replaceNameByUID()) < 0)
              {
                ExRaiseSqlError(getHeap(), &diagsArea_, -8451,
                     NULL, NULL, NULL,
                     getSqlJniErrorStr());
                step_ = HANDLE_ERROR_;
                break;
              }

            currIndex_ = 0;

            step_ = POPULATE_STATS_BUF_;
          }
          break;

	case POPULATE_STATS_BUF_:
	  {
            if (currIndex_ == regionInfoList_->entries())
              {
                step_ = DONE_;
                break;
              }
            
            if (populateStats(currIndex_))
              {
                step_ = HANDLE_ERROR_;
                break;
              }

	    step_ = EVAL_EXPR_;
	  }
	break;

        case EVAL_EXPR_:
          {
            exprRetCode = evalScanExpr((char*)stats_, statsBufLen_, FALSE);
	    if (exprRetCode == ex_expr::EXPR_FALSE)
	      {
		// row does not pass the scan expression,
		// move to the next row.
                currIndex_++;

		step_ = POPULATE_STATS_BUF_;
		break;
	      }
            
            step_ = RETURN_STATS_BUF_;
          }
          break;

	case RETURN_STATS_BUF_:
	  {
	    if (qparent_.up->isFull())
	      return WORK_OK;

	    short rc = 0;
	    if (moveRowToUpQueue((char*)stats_, statsBufLen_, &rc, FALSE))
	      return rc;

            currIndex_++;

            step_ = POPULATE_STATS_BUF_;
	  }
	break;

	case HANDLE_ERROR_:
	  {
	    retcode = handleError();
	    if (retcode == 1)
	      return WORK_OK;
	    
	    step_ = DONE_;
	  }
	break;
	
	case DONE_:
	  {
            if (regionInfoList_ != NULL) {
               deleteNAArray(getHeap(), regionInfoList_);
               regionInfoList_ = NULL;
            }
	    retcode = handleDone();
	    if (retcode == 1)
	      return WORK_OK;
	    
	    step_ = INITIAL_;
	    
	    return WORK_CALL_AGAIN;
	  }
	break;


	} // switch

    } // while

  return WORK_OK;
}

/////////////////////////////////////////////////////////////////////////////
// Constructor and destructor for ExeUtil_private_state
/////////////////////////////////////////////////////////////////////////////
ExExeUtilRegionStatsPrivateState::ExExeUtilRegionStatsPrivateState()
{
}

ExExeUtilRegionStatsPrivateState::~ExExeUtilRegionStatsPrivateState()
{
};


////////////////////////////////////////////////////////////////
// Constructor for class ExExeUtilRegionStatsFormatTcb
///////////////////////////////////////////////////////////////
ExExeUtilRegionStatsFormatTcb::ExExeUtilRegionStatsFormatTcb(
     const ComTdbExeUtilRegionStats & exe_util_tdb,
     ex_globals * glob)
     : ExExeUtilRegionStatsTcb( exe_util_tdb, glob)
{
  statsTotalsBuf_ = new(glob->getDefaultHeap()) char[sizeof(ComTdbRegionStatsVirtTableColumnStruct)];

  statsTotals_ = (ComTdbRegionStatsVirtTableColumnStruct*)statsTotalsBuf_;

  initTotals();

  step_ = INITIAL_;
}

static NAString removeTrailingBlanks(char * name, Lng32 maxLen)
{
  NAString nas;
  
  if (! name)
    return nas;

  Lng32 i = maxLen;
  while ((i > 0) && (name[i-1] == ' '))
    {
      i--;
    }

  if (i > 0)
    nas = NAString(name, i);

  return nas;
}

short ExExeUtilRegionStatsFormatTcb::initTotals()
{
  statsTotals_->numStores                 = 0;
  statsTotals_->numStoreFiles             = 0;
  statsTotals_->readRequestsCount         = 0;
  statsTotals_->writeRequestsCount        = 0;
  statsTotals_->storeFileUncompSize      = 0;
  statsTotals_->storeFileSize            = 0;
  statsTotals_->memStoreSize             = 0;

  return 0;
}

short ExExeUtilRegionStatsFormatTcb::computeTotals()
{
  str_pad(statsTotals_->catalogName, sizeof(statsTotals_->catalogName), ' ');
  str_cpy_all(statsTotals_->catalogName, catName_, strlen(catName_));

  str_pad(statsTotals_->schemaName, sizeof(statsTotals_->schemaName), ' ');
  str_cpy_all(statsTotals_->schemaName, schName_, strlen(schName_));

  str_pad(statsTotals_->objectName, sizeof(statsTotals_->objectName), ' ');
  str_cpy_all(statsTotals_->objectName, objName_, strlen(objName_));

  str_pad(statsTotals_->regionServer, sizeof(statsTotals_->regionServer), ' ');

  str_pad(statsTotals_->regionName, sizeof(statsTotals_->regionName), ' ');

  for (Int32 currIndex = 0; currIndex < regionInfoList_->entries(); currIndex++)
    {
      if (populateStats(currIndex))
        return -1;

      statsTotals_->numStores           += stats_->numStores;
      statsTotals_->numStoreFiles       += stats_->numStoreFiles;
      statsTotals_->storeFileUncompSize += stats_->storeFileUncompSize;
      statsTotals_->storeFileSize       += stats_->storeFileSize;
      statsTotals_->memStoreSize        += stats_->memStoreSize;  
      statsTotals_->readRequestsCount   += stats_->readRequestsCount;
      statsTotals_->writeRequestsCount  += stats_->writeRequestsCount;
    }
  
  return 0;
}

short ExExeUtilRegionStatsFormatTcb::work()
{
  short retcode = 0;
  Lng32 cliRC = 0;

  // if no parent request, return
  if (qparent_.down->isEmpty())
    return WORK_OK;
  
  // if no room in up queue, won't be able to return data/status.
  // Come back later.
  if (qparent_.up->isFull())
    return WORK_OK;
  
  ex_queue_entry * pentry_down = qparent_.down->getHeadEntry();
  ExExeUtilPrivateState & pstate =
    *((ExExeUtilPrivateState*) pentry_down->pstate);

  // Get the globals stucture of the master executor.
  ExExeStmtGlobals *exeGlob = getGlobals()->castToExExeStmtGlobals();
  ExMasterStmtGlobals *masterGlob = exeGlob->castToExMasterStmtGlobals();
  ContextCli * currContext = masterGlob->getCliGlobals()->currContext();

  while (1)
    {
      switch (step_)
	{
	case INITIAL_:
	  {
            if (ehi_ == NULL)
              {
                step_ = HANDLE_ERROR_;
                break;
              }
            
            initTotals();
            
            if (getDLStdb().inputExpr())
              {
                step_ = EVAL_INPUT_;
                break;
              }

            if (getDLStdb().replaceNameByUID())
              strcpy(tableNameForUID_, getDLStdb().getDataUIDName());
            strcpy(tableName_, getDLStdb().getTableName());

	    step_ = COLLECT_STATS_;
	  }
	break;

        case EVAL_INPUT_:
          {
	    workAtp_->getTupp(getDLStdb().workAtpIndex())
	      .setDataPointer(inputNameBuf_);

	    ex_expr::exp_return_type exprRetCode =
	      getDLStdb().inputExpr()->eval(pentry_down->getAtp(), workAtp_);
	    if (exprRetCode == ex_expr::EXPR_ERROR)
	      {
		step_ = HANDLE_ERROR_;
		break;
	      }

            short len = *(short*)inputNameBuf_;
            str_cpy_all(tableName_, &inputNameBuf_[2], len);
            tableName_[len] = 0;

            step_ = COLLECT_STATS_;
          }
          break;

        case COLLECT_STATS_:
          {
            if (collectStats(tableName_, tableNameForUID_, getDLStdb().replaceNameByUID()) < 0)
              {
                ExRaiseSqlError(getHeap(), &diagsArea_, -8451,
                     NULL, NULL, NULL,
                     getSqlJniErrorStr());
                step_ = HANDLE_ERROR_;
                break;
              }

            currIndex_ = 0;

            step_ = COMPUTE_TOTALS_;
          }
          break;

        case COMPUTE_TOTALS_:
          {
            if (computeTotals())
              {
                step_ = HANDLE_ERROR_;
                break;
              }

            step_ = RETURN_SUMMARY_;
          }
          break;

        case RETURN_SUMMARY_:
          {
	    // make sure there is enough space to move header
	    if (isUpQueueFull(14))
	      {
		return WORK_CALL_AGAIN; // come back later
	      }

            ULng32 neededSize = SqlBufferNeededSize(14, 250);
            if (! pool_->get_free_buffer(neededSize))
              {
                return WORK_CALL_AGAIN;
              }
            
            char buf[1000];
	    short rc = 0;

            str_sprintf(buf, " ");
	    if (moveRowToUpQueue(buf, strlen(buf), &rc))
	      return rc;

            str_sprintf(buf, "Stats Summary");
	    if (moveRowToUpQueue(buf, strlen(buf), &rc))
	      return rc;
 
            str_sprintf(buf, "=============");
	    if (moveRowToUpQueue(buf, strlen(buf), &rc))
	      return rc;

            str_sprintf(buf, " ");
	    if (moveRowToUpQueue(buf, strlen(buf), &rc))
	      return rc;

            NAString objName = extNameForHbase_;
            str_sprintf(buf, "  ObjectName:              %s", objName.data());
	    if (moveRowToUpQueue(buf, strlen(buf), &rc))
	      return rc;

            str_sprintf(buf, "  NumRegions:              %d", regionInfoList_->entries());
	    if (moveRowToUpQueue(buf, strlen(buf), &rc))
	      return rc;

            str_sprintf(buf, "  RegionsLocation:         %s/data/default", 
                        hbaseRootdir_);
	    if (moveRowToUpQueue(buf, strlen(buf), &rc))
	      return rc;

            str_sprintf(buf, "  TotalNumStores:          %d", statsTotals_->numStores);
	    if (moveRowToUpQueue(buf, strlen(buf), &rc))
	      return rc;

            str_sprintf(buf, "  TotalNumStoreFiles:      %d", statsTotals_->numStoreFiles);
	    if (moveRowToUpQueue(buf, strlen(buf), &rc))
	      return rc;

            str_sprintf(buf, "  TotalUncompressedSize:   %ld", statsTotals_->storeFileUncompSize);
	    if (moveRowToUpQueue(buf, strlen(buf), &rc))
	      return rc;

           str_sprintf(buf, "  TotalStoreFileSize:      %ld", statsTotals_->storeFileSize);
	    if (moveRowToUpQueue(buf, strlen(buf), &rc))
	      return rc;

            str_sprintf(buf, "  TotalMemStoreSize:       %ld", statsTotals_->memStoreSize);
	    if (moveRowToUpQueue(buf, strlen(buf), &rc))
	      return rc;

            str_sprintf(buf, "  TotalReadRequestsCount:  %ld", statsTotals_->readRequestsCount);
	    if (moveRowToUpQueue(buf, strlen(buf), &rc))
	      return rc;

            str_sprintf(buf, "  TotalWriteRequestsCount: %ld", statsTotals_->writeRequestsCount);
	    if (moveRowToUpQueue(buf, strlen(buf), &rc))
	      return rc;

            step_ = RETURN_DETAILS_;
            return WORK_RESCHEDULE_AND_RETURN;
          }
          break;

        case RETURN_DETAILS_:
          {

            if ((getDLStdb().summaryOnly()) ||
                (regionInfoList_->entries() == 0))
              {
                step_ = DONE_;
                break;
              }

	    // make sure there is enough space to move header
	    if (isUpQueueFull(4))
	      {
		return WORK_CALL_AGAIN; // come back later
	      }

            ULng32 neededSize = SqlBufferNeededSize(4, 250);
            if (! pool_->get_free_buffer(neededSize))
              {
                return WORK_CALL_AGAIN;
              }

            char buf[1000];
	    short rc = 0;

            str_sprintf(buf, " ");
	    if (moveRowToUpQueue(buf, strlen(buf), &rc))
	      return rc;

            str_sprintf(buf, "Stats Details");
	    if (moveRowToUpQueue(buf, strlen(buf), &rc))
	      return rc;
 
            str_sprintf(buf, "=============");
	    if (moveRowToUpQueue(buf, strlen(buf), &rc))
	      return rc;

            str_sprintf(buf, " ");
	    if (moveRowToUpQueue(buf, strlen(buf), &rc))
	      return rc;

            currIndex_ = 0;
            step_ = POPULATE_STATS_BUF_;

            return WORK_RESCHEDULE_AND_RETURN;
          }
          break;

	case POPULATE_STATS_BUF_:
	  {
            if (currIndex_ == regionInfoList_->entries())
              {
                step_ = DONE_;
                break;
              }
            
            if (populateStats(currIndex_))
              {
                step_ = HANDLE_ERROR_;
                break;
              }

            step_ = RETURN_REGION_INFO_;
          }
          break;

        case RETURN_REGION_INFO_:
          {
	    // make sure there is enough space to move header
	    if (isUpQueueFull(10))
	      {
		return WORK_CALL_AGAIN; // come back later
	      }

            ULng32 neededSize = SqlBufferNeededSize(4, 100);
            if (! pool_->get_free_buffer(neededSize))
              {
                return WORK_CALL_AGAIN;
              }

            char buf[1000];
	    short rc = 0;

            str_sprintf(buf, "  RegionServer:       %s", 
                        removeTrailingBlanks(stats_->regionServer, STATS_NAME_MAX_LEN).data());
	    if (moveRowToUpQueue(buf, strlen(buf), &rc))
	      return rc;
  
            str_sprintf(buf, "  RegionNum:          %d", currIndex_+1);
	    if (moveRowToUpQueue(buf, strlen(buf), &rc))
	      return rc;

            str_sprintf(buf, "  RegionName:         %s", 
                        removeTrailingBlanks(stats_->regionName, STATS_REGION_NAME_MAX_LEN).data());
	    if (moveRowToUpQueue(buf, strlen(buf), &rc))
	      return rc;
            
            str_sprintf(buf, "  NumStores:          %d", stats_->numStores);
	    if (moveRowToUpQueue(buf, strlen(buf), &rc))
	      return rc;

            str_sprintf(buf, "  NumStoreFiles:      %d", stats_->numStoreFiles);
	    if (moveRowToUpQueue(buf, strlen(buf), &rc))
	      return rc;

            if (stats_->storeFileUncompSize == 0)
              str_sprintf(buf, "  UncompressedSize:   %ld (less than 1MB)", stats_->storeFileUncompSize);
            else
              str_sprintf(buf, "  UncompressedSize:   %ld Bytes", stats_->storeFileUncompSize);
	    if (moveRowToUpQueue(buf, strlen(buf), &rc))
	      return rc;

            if (stats_->storeFileSize == 0)
              str_sprintf(buf, "  StoreFileSize:      %ld (less than 1MB)", stats_->storeFileSize);
            else
              str_sprintf(buf, "  StoreFileSize:      %ld Bytes", stats_->storeFileSize);
	    if (moveRowToUpQueue(buf, strlen(buf), &rc))
	      return rc;

            if (stats_->memStoreSize == 0)
              str_sprintf(buf, "  MemStoreSize:       %ld (less than 1MB)", stats_->memStoreSize);
            else
              str_sprintf(buf, "  MemStoreSize:       %ld Bytes", stats_->memStoreSize);              
	    if (moveRowToUpQueue(buf, strlen(buf), &rc))
	      return rc;

            str_sprintf(buf, "  ReadRequestsCount:  %ld", stats_->readRequestsCount);
	    if (moveRowToUpQueue(buf, strlen(buf), &rc))
	      return rc;

            str_sprintf(buf, "  WriteRequestsCount: %ld", stats_->writeRequestsCount);
	    if (moveRowToUpQueue(buf, strlen(buf), &rc))
	      return rc;

            str_sprintf(buf, "   ");
	    if (moveRowToUpQueue(buf, strlen(buf), &rc))
	      return rc;

            currIndex_++;

            step_ = POPULATE_STATS_BUF_;

            return WORK_RESCHEDULE_AND_RETURN;
          }
          break;

	case HANDLE_ERROR_:
	  {
	    retcode = handleError();
	    if (retcode == 1)
	      return WORK_OK;
	    
	    step_ = DONE_;
	  }
	break;
	
	case DONE_:
	  {
            if (regionInfoList_ != NULL) {
               deleteNAArray(getHeap(), regionInfoList_);
               regionInfoList_ = NULL;
            }
	    retcode = handleDone();
	    if (retcode == 1)
	      return WORK_OK;
	    
	    step_ = INITIAL_;
	    
	    return WORK_CALL_AGAIN;
	  }
	break;


	} // switch

    } // while

  return WORK_OK;
}

////////////////////////////////////////////////////////////////
// Constructor for class ExExeUtilClusterStatsTcb
///////////////////////////////////////////////////////////////
ExExeUtilClusterStatsTcb::ExExeUtilClusterStatsTcb(
     const ComTdbExeUtilRegionStats & exe_util_tdb,
     ex_globals * glob)
     : ExExeUtilRegionStatsTcb( exe_util_tdb, glob)
{
  statsBuf_ = new(glob->getDefaultHeap()) char[sizeof(ComTdbClusterStatsVirtTableColumnStruct)];
  statsBufLen_ = sizeof(ComTdbClusterStatsVirtTableColumnStruct);

  stats_ = (ComTdbClusterStatsVirtTableColumnStruct*)statsBuf_;

  ehi_ = ExpHbaseInterface::newInstance(glob->getDefaultHeap(),
					(char*)"", 
					(char*)"", 
                                        COM_STORAGE_HBASE,
                                        FALSE);
  regionInfoList_ = NULL;
  
  // get hbase rootdir location. Max linux pathlength is 1024.
  hbaseRootdir_ = new(glob->getDefaultHeap()) char[1030];
  strcpy(hbaseRootdir_, "/hbase");

  step_ = INITIAL_;

  currObjectRegionNum_ = 1;
}

ExExeUtilClusterStatsTcb::~ExExeUtilClusterStatsTcb()
{
  if (statsBuf_)
    NADELETEBASIC(statsBuf_, getGlobals()->getDefaultHeap());

  if (ehi_)
    delete ehi_;

  statsBuf_ = NULL;
}

short ExExeUtilClusterStatsTcb::collectStats()
{
  numRegionStatsEntries_ = 0;
  currObjectRegionNum_ = 1;
  regionInfoList_ = ehi_->getClusterStats(numRegionStatsEntries_);
  if (! regionInfoList_)
    {
      return 1; // EOD
    }
 
  currIndex_ = 0;

  return 0;
}

// RETURN: 1, not a TRAFODION region. 0, is a TRAFODION region.
//        -1, error.
short ExExeUtilClusterStatsTcb::populateStats
(Int32 currIndex, NABoolean nullTerminate)
{
  str_pad(stats_->catalogName, sizeof(stats_->catalogName), ' ');
  str_pad(stats_->schemaName, sizeof(stats_->schemaName), ' ');
  str_pad(stats_->objectName, sizeof(stats_->objectName), ' ');

  str_pad(stats_->regionServer, sizeof(stats_->regionServer), ' ');

  str_pad(stats_->regionName, sizeof(stats_->regionName), ' ');
   
  char regionInfoBuf[5000];
  Int32 len = 0;
  char *regionInfo = regionInfoBuf;
  char *val = regionInfoList_->at(currIndex).val;
  len = regionInfoList_->at(currIndex).len; 
  if (len >= sizeof(regionInfoBuf))
     len = sizeof(regionInfoBuf)-1;
  strncpy(regionInfoBuf, val, len);
  regionInfoBuf[len] = '\0';
  stats_->numStores                = 0;
  stats_->numStoreFiles            = 0;
  stats_->storeFileUncompSize      = 0;
  stats_->storeFileSize            = 0;
  stats_->memStoreSize             = 0;
  
  char longBuf[30];
  char * sep1 = strchr(regionInfo, '|');
  if (sep1)
    {
      str_cpy_all(stats_->regionServer, regionInfo, 
                  (Lng32)(sep1 - regionInfo)); 

      if (nullTerminate)
        stats_->regionServer[sep1 - regionInfo] = 0;
    }

  char * sepStart = sep1+1;
  sep1 = strchr(sepStart, '|');
  if (sep1)
    {
      str_cpy_all(stats_->regionName, sepStart, 
                  (Lng32)(sep1 - sepStart)); 

      if (nullTerminate)
        stats_->regionName[sep1 - sepStart] = 0;
    }

  char tableName[3*STATS_NAME_MAX_LEN + 3];
  sepStart = sep1+1;
  sep1 = strchr(sepStart, '|');
  if (sep1)
    {
      str_cpy_all(tableName, sepStart, 
                  (Lng32)(sep1 - sepStart)); 

      tableName[sep1 - sepStart] = 0;

      char tableNameBuf[3*STATS_NAME_MAX_LEN + 30];

      Lng32 numParts = 0;
      char *parts[4];
      LateNameInfo::extractParts(tableName, tableNameBuf, numParts, parts, FALSE);

      if (numParts == 3)
        {
          str_cpy_all(stats_->catalogName, parts[0], strlen(parts[0]));
          if (nullTerminate)
            stats_->catalogName[strlen(parts[0])] = 0;

          str_cpy_all(stats_->schemaName, parts[1], strlen(parts[1]));
          if (nullTerminate)
            stats_->schemaName[strlen(parts[1])] = 0;
      
          str_cpy_all(stats_->objectName, parts[2], strlen(parts[2]));
          if (nullTerminate)
            stats_->objectName[strlen(parts[2])] = 0;
        }

      if (numParts != 3) 
        {
          // this is not a trafodion region, skip it.
          return 1;
        }

      // extract namespace from catalogNamePart
      NAString nameSpace;
      char * catName = parts[0];
      char * colonPos = strchr(parts[0], ':');
      if (colonPos)
        {
          //nameSpace = NAString(catName, (colonPos-catName));
          Lng32 catNameLen = strlen(catName)-((colonPos-catName)+1);
          str_cpy_all(stats_->catalogName, &catName[colonPos - catName + 1],
                      catNameLen);
          if (nullTerminate)
            stats_->catalogName[catNameLen] = 0;
        }
      
      if (str_cmp(stats_->catalogName, TRAFODION_SYSCAT_LIT, strlen(TRAFODION_SYSCAT_LIT)) != 0)
        {
          // this is not a trafodion region, skip it.
          return 1;
        }
    }
 
  if (currObjectName_ != NAString(stats_->objectName))
    {
      currObjectRegionNum_ = 1;
      currObjectName_ = stats_->objectName;
    }
  stats_->regionNum = currObjectRegionNum_++;
 
  sepStart = sep1;
  stats_->numStores = getEmbeddedNumValue(sepStart, '|', FALSE);
  stats_->numStoreFiles = getEmbeddedNumValue(sepStart, '|', FALSE);
  stats_->storeFileUncompSize = getEmbeddedNumValue(sepStart, '|', FALSE);
  stats_->storeFileSize = getEmbeddedNumValue(sepStart, '|', FALSE);
  stats_->memStoreSize = getEmbeddedNumValue(sepStart, '|', FALSE);
  stats_->readRequestsCount = getEmbeddedNumValue(sepStart, '|', FALSE);
  stats_->writeRequestsCount = getEmbeddedNumValue(sepStart, '|', FALSE);
  
  return 0;
}

short ExExeUtilClusterStatsTcb::work()
{
  short retcode = 0;
  Lng32 cliRC = 0;
  ex_expr::exp_return_type exprRetCode = ex_expr::EXPR_OK;

  // if no parent request, return
  if (qparent_.down->isEmpty())
    return WORK_OK;
  
  // if no room in up queue, won't be able to return data/status.
  // Come back later.
  if (qparent_.up->isFull())
    return WORK_OK;
  
  ex_queue_entry * pentry_down = qparent_.down->getHeadEntry();
  ExExeUtilPrivateState & pstate =
    *((ExExeUtilPrivateState*) pentry_down->pstate);

  // Get the globals stucture of the master executor.
  ExExeStmtGlobals *exeGlob = getGlobals()->castToExExeStmtGlobals();
  ExMasterStmtGlobals *masterGlob = exeGlob->castToExMasterStmtGlobals();
  ContextCli * currContext = masterGlob->getCliGlobals()->currContext();

  while (1)
    {
      switch (step_)
	{
	case INITIAL_:
	  {
            if (ehi_ == NULL)
              {
                step_ = HANDLE_ERROR_;
                break;
              }

	    step_ = COLLECT_STATS_;
	  }
	break;

       case COLLECT_STATS_:
          {
            retcode = collectStats();
            if (retcode == 1) // EOD
              {
                step_ = DONE_;
                break;
              }
            else if (retcode < 0)
              {
                ExRaiseSqlError(getHeap(), &diagsArea_, -8451,
                     NULL, NULL, NULL,
                     getSqlJniErrorStr());
                step_ = HANDLE_ERROR_;
                break;
              }

            currIndex_ = 0;

            step_ = POPULATE_STATS_BUF_;
          }
          break;

	case POPULATE_STATS_BUF_:
	  {
            if (currIndex_ == numRegionStatsEntries_) //regionInfoList_->entries())
              {
                step_ = COLLECT_STATS_;
                break;
              }
            
            retcode = populateStats(currIndex_);
            if (retcode == 1) // not TRAFODION region, skip it
              {
                currIndex_++;

                step_ = POPULATE_STATS_BUF_;
                break;
              }
            else if (retcode < 0)
              {
                step_ = HANDLE_ERROR_;
                break;
              }

	    step_ = EVAL_EXPR_;
	  }
	break;

        case EVAL_EXPR_:
          {
            exprRetCode = evalScanExpr((char*)stats_, statsBufLen_, FALSE);
	    if (exprRetCode == ex_expr::EXPR_FALSE)
	      {
		// row does not pass the scan expression,
		// move to the next row.
                currIndex_++;

		step_ = POPULATE_STATS_BUF_;
		break;
	      }
            
            step_ = RETURN_STATS_BUF_;
          }
          break;

	case RETURN_STATS_BUF_:
	  {
	    if (qparent_.up->isFull())
	      return WORK_OK;

	    short rc = 0;
	    if (moveRowToUpQueue((char*)stats_, statsBufLen_, &rc, FALSE))
	      return rc;

 
            currIndex_++;

            step_ = POPULATE_STATS_BUF_;
	  }
	break;

	case HANDLE_ERROR_:
	  {
	    retcode = handleError();
	    if (retcode == 1)
	      return WORK_OK;
	    
	    step_ = DONE_;
	  }
	break;
	
	case DONE_:
	  {
            if (regionInfoList_ != NULL) {
               deleteNAArray(getHeap(), regionInfoList_);
               regionInfoList_ = NULL;
            }
	    retcode = handleDone();
	    if (retcode == 1)
	      return WORK_OK;
	    
	    step_ = INITIAL_;
	    
	    return WORK_CALL_AGAIN;
	  }
	break;


	} // switch

    } // while

  return WORK_OK;
}




