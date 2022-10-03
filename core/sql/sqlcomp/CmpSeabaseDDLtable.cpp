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
 * File:         CmpSeabaseDDLtable.cpp
 * Description:  Implements ddl operations for Seabase tables.
 *
 *
 * Created:     6/30/2013
 * Language:     C++
 *
 *
 *****************************************************************************
 */

#include <sys/time.h>

#include "CmpSeabaseDDLincludes.h"
#include "CmpSeabaseDDLauth.h"
#include "ElemDDLColDefault.h"
#include "NumericType.h"
#include "CompositeType.h"
#include "ComUser.h"
#include "keycolumns.h"
#include "ElemDDLColRef.h"
#include "ElemDDLColName.h"
#include "ElemDDLPartitionClause.h"
#include "ElemDDLPartitionList.h"
#include "ElemDDLStoreOptions.h"

#include "CmpDDLCatErrorCodes.h"
#include "Globals.h"
#include "CmpMain.h"
#include "Context.h"
#include "PrivMgrCommands.h"
#include "PrivMgrMDDefs.h"
#include "PrivMgrRoles.h"
#include "PrivMgrComponentPrivileges.h"

#include "StmtDDLAlterTableHDFSCache.h"
#include "StmtDDLonHiveObjects.h"
#include "StmtDDLAlterSharedCache.h"
#include "StmtDDLAlterTableResetDDLLock.h"
#include "StmtDDLCreateTable.h"

#include "RelExeUtil.h"

#include "TrafDDLdesc.h"

#include "CmpDescribe.h"
#include "SharedCache.h"
#include "NamedSemaphore.h"


#include "DLock.h"  // for distributed locking

#include "NATestpoint.h"

pthread_t lob_ddl_thread_id = 0;

static bool checkSpecifiedPrivs(
   ElemDDLPrivActArray & privActsArray,  
   const char * externalObjectName,
   ComObjectType objectType,
   NATable * naTable,
   std::vector<PrivType> & objectPrivs,
   std::vector<ColPrivSpec> & colPrivs); 
   
static bool hasValue(
   const std::vector<ColPrivSpec> & container,
   PrivType value);                              
                             
static bool hasValue(
   const std::vector<PrivType> & container,
   PrivType value); 
   
static bool isMDGrantRevokeOK(
   const std::vector<PrivType> & objectPrivs,
   const std::vector<ColPrivSpec> & colPrivs,
   bool isGrant);   
   
static bool isValidPrivTypeForObject(
   ComObjectType objectType,
   PrivType privType);                               

void CmpSeabaseDDL::convertVirtTableColumnInfoToDescStruct( 
     const ComTdbVirtTableColumnInfo * colInfo,
     const ComObjectName * objectName,
     TrafDesc * column_desc)
{
  char * col_name = new(STMTHEAP) char[strlen(colInfo->colName) + 1];
  strcpy(col_name, colInfo->colName);
  column_desc->columnsDesc()->colname = col_name;
  column_desc->columnsDesc()->colnumber = colInfo->colNumber;
  column_desc->columnsDesc()->datatype  = colInfo->datatype;
  column_desc->columnsDesc()->length    = colInfo->length;
  if (!(DFS2REC::isInterval(colInfo->datatype)))
    column_desc->columnsDesc()->scale     = colInfo->scale;
  else
    column_desc->columnsDesc()->scale = 0;
  column_desc->columnsDesc()->precision = colInfo->precision;
  column_desc->columnsDesc()->datetimestart = (rec_datetime_field) colInfo->dtStart;
  column_desc->columnsDesc()->datetimeend = (rec_datetime_field) colInfo->dtEnd;
  if (DFS2REC::isDateTime(colInfo->datatype) || DFS2REC::isInterval(colInfo->datatype))
    column_desc->columnsDesc()->datetimefractprec = colInfo->scale;
  else
    column_desc->columnsDesc()->datetimefractprec = 0;
  if (DFS2REC::isInterval(colInfo->datatype))
    column_desc->columnsDesc()->intervalleadingprec = colInfo->precision;
  else
    column_desc->columnsDesc()->intervalleadingprec = 0 ;
  column_desc->columnsDesc()->setNullable(colInfo->nullable);
  column_desc->columnsDesc()->setUpshifted(colInfo->upshifted);
  column_desc->columnsDesc()->character_set = (CharInfo::CharSet) colInfo->charset;
  switch (colInfo->columnClass)
    {
    case COM_USER_COLUMN:
      column_desc->columnsDesc()->colclass = 'U';
      break;
    case COM_SYSTEM_COLUMN:
      column_desc->columnsDesc()->colclass = 'S';
      break;
    default:
      CMPASSERT(0);
    }
  column_desc->columnsDesc()->setDefaultClass(colInfo->defaultClass);
  column_desc->columnsDesc()->colFlags = colInfo->colFlags;
  
  
  column_desc->columnsDesc()->pictureText =
    (char *)STMTHEAP->allocateMemory(340);
  NAType::convertTypeToText(column_desc->columnsDesc()->pictureText,  //OUT
                            column_desc->columnsDesc()->datatype,
                            column_desc->columnsDesc()->length,
                            column_desc->columnsDesc()->precision,
                            column_desc->columnsDesc()->scale,
                            column_desc->columnsDesc()->datetimeStart(),
                            column_desc->columnsDesc()->datetimeEnd(),
                            column_desc->columnsDesc()->datetimefractprec,
                            column_desc->columnsDesc()->intervalleadingprec,
                            column_desc->columnsDesc()->isUpshifted(),
                            column_desc->columnsDesc()->isCaseInsensitive(),
                            (CharInfo::CharSet)column_desc->columnsDesc()->character_set,
                            (CharInfo::Collation) 1, // default collation
                            NULL, // displayDataType
                            0); // displayCaseSpecific
  

  column_desc->columnsDesc()->offset    = -1; // not present in colInfo
  column_desc->columnsDesc()->setCaseInsensitive(FALSE); // not present in colInfo
  column_desc->columnsDesc()->encoding_charset = (CharInfo::CharSet) column_desc->columnsDesc()->character_set ; // not present in colInfo so we go with the column's charset here. 
  column_desc->columnsDesc()->collation_sequence = (CharInfo::Collation)1; // not present in colInfo, so we go with default collation here (used in buildEncodeTree for some error handling)
  column_desc->columnsDesc()->defaultvalue = NULL ; // not present in colInfo
  column_desc->columnsDesc()->initDefaultValue = NULL ; // not present in colInfo
  column_desc->columnsDesc()->computed_column_text = NULL; // not present in colInfo
}

TrafDesc * CmpSeabaseDDL::convertVirtTableColumnInfoArrayToDescStructs(
     const ComObjectName * objectName,
     const ComTdbVirtTableColumnInfo * colInfoArray,
     Lng32 numCols)
{
  TrafDesc * prev_column_desc  = NULL;
  TrafDesc * first_column_desc = NULL;
  for (Int32 i = 0; i < numCols; i++)
  {
    const ComTdbVirtTableColumnInfo* colInfo = &(colInfoArray[i]);

    // TrafAllocateDDLdesc() requires that HEAP (STMTHEAP) 
    // be used for operator new herein
    TrafDesc * column_desc = TrafAllocateDDLdesc(DESC_COLUMNS_TYPE, NULL);
    if (prev_column_desc != NULL)
      prev_column_desc->next = column_desc;
    else
      first_column_desc = column_desc;      
    
    prev_column_desc = column_desc;
    convertVirtTableColumnInfoToDescStruct(colInfo, objectName, column_desc);
  }

  return first_column_desc;
}

TrafDesc * CmpSeabaseDDL::convertVirtTableKeyInfoArrayToDescStructs(
     const ComTdbVirtTableKeyInfo *keyInfoArray,
     const ComTdbVirtTableColumnInfo *colInfoArray,
     Lng32 numKeys)
{
  TrafDesc * prev_key_desc  = NULL;
  TrafDesc * first_key_desc = NULL;
  for (Int32 i = 0; i < numKeys; i++)
    {
      const ComTdbVirtTableColumnInfo * colInfo = &(colInfoArray[keyInfoArray[i].tableColNum]);
      TrafDesc * key_desc = TrafAllocateDDLdesc(DESC_KEYS_TYPE, NULL);
      if (prev_key_desc != NULL)
        prev_key_desc->next = key_desc;
      else
       first_key_desc = key_desc;      
      
      prev_key_desc = key_desc;
      
      key_desc->keysDesc()->tablecolnumber = keyInfoArray[i].tableColNum;
      key_desc->keysDesc()->keyseqnumber = i;
      key_desc->keysDesc()->setDescending(keyInfoArray[i].ordering != 0 ? TRUE : FALSE);
    }

  return first_key_desc;
}

void CmpSeabaseDDL::createSeabaseTableLike(ExeCliInterface * cliInterface,
                                           StmtDDLCreateTable * createTableNode,
                                           NAString &currCatName, NAString &currSchName)
{
  Lng32 retcode = 0;

  ComObjectName tgtTableName(createTableNode->getTableName(), COM_TABLE_NAME);
  ComAnsiNamePart currCatAnsiName(currCatName);
  ComAnsiNamePart currSchAnsiName(currSchName);
  tgtTableName.applyDefaults(currCatAnsiName, currSchAnsiName);
  const NAString extTgtTableName = tgtTableName.getExternalName(TRUE);
  
  ComObjectName srcTableName(createTableNode->getLikeSourceTableName(), COM_TABLE_NAME);
  srcTableName.applyDefaults(currCatAnsiName, currSchAnsiName);
  
  NABoolean srcSchNameSpecified = 
    (NOT createTableNode->getOrigLikeSourceTableName().getSchemaName().isNull());

  NAString srcCatNamePart = srcTableName.getCatalogNamePartAsAnsiString();
  NAString srcSchNamePart = srcTableName.getSchemaNamePartAsAnsiString(TRUE);
  NAString srcObjNamePart = srcTableName.getObjectNamePartAsAnsiString(TRUE);
  NAString extSrcTableName = srcTableName.getExternalName(TRUE);
  NAString srcTabName = srcTableName.getExternalName(TRUE);

  retcode = lookForTableInMD(cliInterface, 
                             srcCatNamePart, srcSchNamePart, srcObjNamePart,
                             srcSchNameSpecified, FALSE,
                             srcTableName, srcTabName, extSrcTableName);
  if (retcode < 0)
    {
      processReturn();

      return;
    }

  CorrName cn(srcObjNamePart,
              STMTHEAP,
              srcSchNamePart,
              srcCatNamePart);

  ElemDDLColRefArray &keyArray = 
    (createTableNode->getIsConstraintPKSpecified() ?
     createTableNode->getPrimaryKeyColRefArray() :
     (createTableNode->getStoreOption() == COM_KEY_COLUMN_LIST_STORE_OPTION ?
      createTableNode->getKeyColumnArray() :
      createTableNode->getPrimaryKeyColRefArray()));

  NAString keyClause;
  if ((keyArray.entries() > 0) &&
      ((createTableNode->getIsConstraintPKSpecified()) ||
       (createTableNode->getStoreOption() == COM_KEY_COLUMN_LIST_STORE_OPTION)))
    {
      if (createTableNode->getIsConstraintPKSpecified())
        keyClause = " primary key ( ";
      else if (createTableNode->getStoreOption() == COM_KEY_COLUMN_LIST_STORE_OPTION)
        keyClause = " store by ( ";
      
      for (CollIndex i = 0; i < keyArray.entries(); i++) 
        {
          NAString colName = keyArray[i]->getColumnName();
          
          // Generate a delimited identifier if source colName is delimited
          // Launchpad bug: 1383531
          colName/*InExternalFormat*/ = ToAnsiIdentifier (colName/*InInternalFormat*/);
         
          keyClause += colName;

          if (i < (keyArray.entries() - 1))
            keyClause += ", ";
        }

      keyClause += ")";
    }

  // Check for other common options that are currently not supported
  // with CREATE TABLE LIKE. Those could all be passed into
  // CmpDescribeSeabaseTable as strings if we wanted to support them.

  if (NOT keyClause.isNull())
    {
      *CmpCommon::diags() << DgSqlCode(-3111)
                          << DgString0("PRIMARY KEY/STORE BY");
      return;
    }

  if (createTableNode->isPartitionSpecified() ||
      createTableNode->isPartitionBySpecified())
    {
      *CmpCommon::diags() << DgSqlCode(-3111)
                          << DgString0("PARTITION BY");
      return;
    }

  if (createTableNode->isDivisionClauseSpecified())
    {
      *CmpCommon::diags() << DgSqlCode(-3111)
                          << DgString0("DIVISION BY");
      return;
    }

  if (createTableNode->isHbaseOptionsSpecified())
    {
      *CmpCommon::diags() << DgSqlCode(-3111)
                          << DgString0("HBASE table options");
      return;
    }

  ParDDLLikeOptsCreateTable &likeOptions = createTableNode->getLikeOptions();

  if (NOT likeOptions.getLikeOptHiveOptions().isNull())
    {
      *CmpCommon::diags() << DgSqlCode(-3242)
                          << DgString0("Hive options cannot be specified for this table.");
      return;
    }

  char * buf = NULL;
  ULng32 buflen = 0;

    retcode = CmpDescribeSeabaseTable(cn, 3/*createlike*/, buf, buflen, STMTHEAP, createTableNode->isPartition(),
                                      NULL, NULL,
                                      likeOptions.getIsWithHorizontalPartitions(),
                                      likeOptions.getIsWithoutSalt(),
                                      likeOptions.getIsWithoutDivision(),
                                      likeOptions.getIsWithoutRowFormat(),
                                      likeOptions.getIsWithoutLobColumns(),
                                      likeOptions.getIsWithoutNamespace(),
                                      likeOptions.getIsLikeOptWithoutRegionReplication(),
                                      likeOptions.getIsLikeOptWithoutIncrBackup(),
                                      likeOptions.getIsLikeOptColumnLengthLimit(),
                                      TRUE);
  if (retcode)
    return;

  NAString query = createTableNode->isPartition() ?
                   "create partition table " :
                   "create table ";
  query += extTgtTableName;
  query += " ";

  NABoolean done = FALSE;
  Lng32 curPos = 0;
  while (NOT done)
    {
      short len = *(short*)&buf[curPos];
      NAString frag(&buf[curPos+sizeof(short)],
                    len - ((buf[curPos+len-1]== '\n') ? 1 : 0));

      query += frag;
      curPos += ((((len+sizeof(short))-1)/8)+1)*8;

      if (curPos >= buflen)
        done = TRUE;
    }

  if (NOT keyClause.isNull())
    {
      // add the keyClause
      query += keyClause;
    }

  const NAString * saltClause = likeOptions.getSaltClause();
  if (saltClause)
    {
      query += saltClause->data();
    }

  if (createTableNode->isSplitBySpecified())
    {
      query += " ";
      query += createTableNode->getSplitByClause();
    }
  // send any user CQDs down 
  Lng32 retCode = sendAllControls(FALSE, FALSE, TRUE);

  Lng32 cliRC = 0;

  // turn off pthread lob creation
  NAString value("0"); // value 0 turns off pthread
  ActiveSchemaDB()->getDefaults().holdOrRestore("traf_lob_parallel_ddl", 1);
  ActiveSchemaDB()->getDefaults().validateAndInsert(
       "traf_lob_parallel_ddl", value, FALSE);      
  
  BindWA bindWA(ActiveSchemaDB(), CmpCommon::context(), FALSE/*inDDL*/);
  NATable *naTable = bindWA.getNATableInternal((CorrName&)cn);
  cliRC = cliInterface->executeImmediate((char*)query.data());
  if (cliRC < 0)
    {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());

      goto label_return;
    }

  if (likeOptions.getIsLikeOptWithData())
    {
      NAString insertData = "upsert using load into ";
      insertData += extTgtTableName;
      insertData += " select * from ";
      insertData += extSrcTableName;
      cliRC = cliInterface->executeImmediate((char*)insertData.data());
      if (cliRC < 0)
        {
          cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
          goto label_return;
        }
    }

    if ((NOT likeOptions.getIsWithoutIndexes()) && (naTable->getIndexList().entries() > 1) && naTable->isSeabaseTable())
    {
      NAString newIndexNm;
      NAString indexQuery;
      char *indexBuf = NULL;
      ULng32 indexBuflen = 0;
      NABoolean finish1 = FALSE;
      curPos = 0;
      NAString tgtCatNamePart = tgtTableName.getCatalogNamePartAsAnsiString();
      NAString tgtSchNamePart = tgtTableName.getSchemaNamePartAsAnsiString(TRUE);
      NAString tgtObjNamePart = tgtTableName.getObjectNamePartAsAnsiString(TRUE);
      CorrName tgtcn(tgtObjNamePart,
                     STMTHEAP,
                     tgtSchNamePart,
                     tgtCatNamePart);
      retcode = CmpDescribeSeabaseTable(cn, 3, indexBuf, indexBuflen, STMTHEAP, createTableNode->isPartition(),
                                        NULL, NULL,
                                        likeOptions.getIsWithHorizontalPartitions(),
                                        likeOptions.getIsWithoutSalt(),
                                        likeOptions.getIsWithoutDivision(),
                                        likeOptions.getIsWithoutRowFormat(),
                                        likeOptions.getIsWithoutLobColumns(),
                                        likeOptions.getIsWithoutNamespace(),
                                        likeOptions.getIsLikeOptWithoutRegionReplication(),
                                        likeOptions.getIsLikeOptWithoutIncrBackup(),
                                        likeOptions.getIsLikeOptColumnLengthLimit(),
                                        TRUE, FALSE, NULL, 0, NULL, NULL, NULL, FALSE, &tgtcn, 1/*get index*/);
      if (retcode)
        return;
      while (NOT finish1)
      {
        short len = *(short*)&indexBuf[curPos];
        NAString frag(&indexBuf[curPos+sizeof(short)],
        len - ((indexBuf[curPos+len-1]== '\n') ? 1 : 0));
        indexQuery += frag;
        //if one statement end execute it
        if (frag == NAString(";"))
        {
          cliRC = cliInterface->executeImmediate((char*)indexQuery.data());
          if (cliRC < 0)
          {
            cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
            goto label_return;
          }
          else
            indexQuery = "";
        }
        curPos += ((((len+sizeof(short))-1)/8)+1)*8;
        if (curPos >= indexBuflen)
        finish1 = TRUE;
      }
    }

    if ((NOT likeOptions.getIsWithoutConstraints()) && naTable->isSeabaseTable())
    {
      NABoolean finish2 = FALSE;
      char *constraintBuf = NULL;
      ULng32 constraintBuflen = 0;
      NAString constraintQuery;
      curPos = 0;
      NAString tgtCatNamePart = tgtTableName.getCatalogNamePartAsAnsiString();
      NAString tgtSchNamePart = tgtTableName.getSchemaNamePartAsAnsiString(TRUE);
      NAString tgtObjNamePart = tgtTableName.getObjectNamePartAsAnsiString(TRUE);
      CorrName tgtcn(tgtObjNamePart,
                     STMTHEAP,
                     tgtSchNamePart,
                     tgtCatNamePart);
      retcode = CmpDescribeSeabaseTable(cn, 3, constraintBuf, constraintBuflen, STMTHEAP, createTableNode->isPartition(),
                                        NULL, NULL,
                                        likeOptions.getIsWithHorizontalPartitions(),
                                        likeOptions.getIsWithoutSalt(),
                                        likeOptions.getIsWithoutDivision(),
                                        likeOptions.getIsWithoutRowFormat(),
                                        likeOptions.getIsWithoutLobColumns(),
                                        likeOptions.getIsWithoutNamespace(),
                                        likeOptions.getIsLikeOptWithoutRegionReplication(),
                                        likeOptions.getIsLikeOptWithoutIncrBackup(),
                                        likeOptions.getIsLikeOptColumnLengthLimit(),
                                        TRUE, FALSE, NULL, 0, NULL, NULL, NULL, FALSE, &tgtcn, 2/*get constraint*/);
      if (retcode)
        return;
      while(NOT finish2)
      {
        short len = *(short*)&constraintBuf[curPos];
        NAString frag(&constraintBuf[curPos+sizeof(short)],
        len - ((constraintBuf[curPos+len-1]== '\n') ? 1 : 0));
        constraintQuery += frag;
        //if one statement end execute it
        if (frag == NAString(";"))
        {
          cliRC = cliInterface->executeImmediate((char*)constraintQuery.data());
          if (cliRC < 0)
          {
            cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
            goto label_return;
          }
          else
            constraintQuery = "";
        }
        curPos += ((((len+sizeof(short))-1)/8)+1)*8;
        if (curPos >= constraintBuflen)
          finish2 = TRUE;
      }
    }

  {
    //update shared data cache for create table like
    BindWA bindWA(ActiveSchemaDB(), CmpCommon::context(), FALSE/*inDDL*/);
    NATable *srcTable = bindWA.getNATableInternal(cn);
    if (srcTable && srcTable->readOnlyEnabled())
      {
        NAString tgtCatNamePart = tgtTableName.getCatalogNamePartAsAnsiString();
        NAString tgtSchNamePart = tgtTableName.getSchemaNamePartAsAnsiString(TRUE);
        NAString tgtObjNamePart = tgtTableName.getObjectNamePartAsAnsiString(TRUE);
        insertIntoSharedCacheList(tgtCatNamePart, tgtSchNamePart, tgtObjNamePart,
                                  COM_BASE_TABLE_OBJECT,
                                  SharedCacheDDLInfo::DDL_UPDATE,
                                  SharedCacheDDLInfo::SHARED_DATA_CACHE);
      }
  }

 label_return:
  ActiveSchemaDB()->getDefaults().holdOrRestore("traf_lob_parallel_ddl", 2);

  return;
}

// ----------------------------------------------------------------------------
// Method: createSeabaseTableExternal
//
// This method creates a Trafodion table that represents a Hive or HBase table 
//
// in:
//   cliInterface - references to the cli execution structure
//   createTableNode - representation of the CREATE TABLE statement
//   tgtTableName - the Trafodion external table name to create
//   srcTableName - the native source table
//
// returns:  0 - successful, -1 error
//
// any error detected is added to the diags area
// ---------------------------------------------------------------------------- 
short CmpSeabaseDDL::createSeabaseTableExternal(
  ExeCliInterface &cliInterface,
  StmtDDLCreateTable * createTableNode,
  const ComObjectName &tgtTableName,
  const ComObjectName &srcTableName) 
{
  Int32 retcode = 0;

  NABoolean isHive = tgtTableName.isExternalHive(); 

  // go create the schema - if it does not already exist.
  NAString createSchemaStmt ("CREATE SCHEMA IF NOT EXISTS ");
  createSchemaStmt += tgtTableName.getCatalogNamePartAsAnsiString();
  createSchemaStmt += ".";
  createSchemaStmt += tgtTableName.getSchemaNamePartAsAnsiString();
  if (isAuthorizationEnabled())
    {
      createSchemaStmt += " AUTHORIZATION ";
      createSchemaStmt += (isHive) ? DB__HIVEROLE : DB__HBASEROLE; 
    }

  Lng32 cliRC = cliInterface.executeImmediate((char*)createSchemaStmt.data());
  if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      return -1;
    }

  const NAString catalogNamePart = tgtTableName.getCatalogNamePartAsAnsiString();
  const NAString schemaNamePart = tgtTableName.getSchemaNamePartAsAnsiString(TRUE);
  const NAString objectNamePart = tgtTableName.getObjectNamePartAsAnsiString(TRUE);

  // Make sure current user has privileges
  Int32 objectOwnerID = SUPER_USER;
  Int32 schemaOwnerID = SUPER_USER;
  Int64 schemaUID = 0;
  ComSchemaClass schemaClass;
  retcode = verifyDDLCreateOperationAuthorized(&cliInterface,
                                               SQLOperation::CREATE_TABLE,
                                               catalogNamePart,
                                               schemaNamePart,
                                               schemaClass,
                                               objectOwnerID,
                                               schemaOwnerID,
                                               &schemaUID);
  if (retcode != 0)
  {
     handleDDLCreateAuthorizationError(retcode,catalogNamePart,schemaNamePart);
     return -1;
  }

  if (createTableNode->mapToHbaseTable())
      return 0;

  const NAString extTgtTableName = tgtTableName.getExternalName(TRUE);

  const NAString srcCatNamePart = srcTableName.getCatalogNamePartAsAnsiString();
  const NAString srcSchNamePart = srcTableName.getSchemaNamePartAsAnsiString(TRUE);
  const NAString srcObjNamePart = srcTableName.getObjectNamePartAsAnsiString(TRUE);
  CorrName cnSrc(srcObjNamePart, STMTHEAP, srcSchNamePart, srcCatNamePart);

  // build the structures needed to create the table
  // tableInfo contains data inserted into OBJECTS and TABLES
  ComTdbVirtTableTableInfo * tableInfo = new(STMTHEAP) ComTdbVirtTableTableInfo[1];
  tableInfo->tableName = NULL;
  tableInfo->createTime = 0;
  tableInfo->redefTime = 0;
  tableInfo->objUID = 0;
  tableInfo->objDataUID = 0;
  tableInfo->schemaUID = schemaUID;
  tableInfo->isAudited = 1;
  tableInfo->validDef = 1;
  tableInfo->hbaseCreateOptions = NULL;
  tableInfo->numSaltPartns = 0;
  tableInfo->numTrafReplicas = 0;
  tableInfo->rowFormat = COM_UNKNOWN_FORMAT_TYPE;
  if (isHive)
    {
      tableInfo->objectFlags = SEABASE_OBJECT_IS_EXTERNAL_HIVE;
    }
  else
    {
      tableInfo->objectFlags = SEABASE_OBJECT_IS_EXTERNAL_HBASE;
      if (createTableNode->isImplicitExternal())
        tableInfo->objectFlags |= SEABASE_OBJECT_IS_IMPLICIT_EXTERNAL;
    }
  tableInfo->tablesFlags = 0;

  if (isAuthorizationEnabled())
    {
      if (tgtTableName.isExternalHive())
        {
          tableInfo->objOwnerID = HIVE_ROLE_ID;
          tableInfo->schemaOwnerID = HIVE_ROLE_ID;
        }
      else
        {
          tableInfo->objOwnerID = HBASE_ROLE_ID;
          tableInfo->schemaOwnerID = HBASE_ROLE_ID;
        }
    }
  else
    {
      tableInfo->objOwnerID = SUPER_USER;
      tableInfo->schemaOwnerID = SUPER_USER;
    }

  // Column information
  Lng32 datatype, length, precision, scale, dtStart, dtEnd, nullable, upshifted;
  NAString charset;
  CharInfo::Collation collationSequence = CharInfo::DefaultCollation;
  ULng32 hbaseColFlags;

  NABoolean alignedFormat = FALSE;
  Lng32 serializedOption = -1;

  Int32 numCols = 0;
  ComTdbVirtTableColumnInfo * colInfoArray = NULL;
        
  ElemDDLColDefArray &colArray = createTableNode->getColDefArray();
  ElemDDLColRefArray &keyArray =
    (createTableNode->getIsConstraintPKSpecified() ?
     createTableNode->getPrimaryKeyColRefArray() :
     (createTableNode->getStoreOption() == COM_KEY_COLUMN_LIST_STORE_OPTION ?
      createTableNode->getKeyColumnArray() :
      createTableNode->getPrimaryKeyColRefArray()));

  // Get a description of the source table
  BindWA bindWA(ActiveSchemaDB(), CmpCommon::context(), TRUE/*inDDL*/);
  NATable *naTable = bindWA.getNATable(cnSrc);
  if (naTable == NULL || bindWA.errStatus())
    {
      *CmpCommon::diags()
        << DgSqlCode(-4082)
        << DgTableName(cnSrc.getExposedNameAsAnsiString());
      return -1;
    }

  if ((naTable->isHiveTable()) &&
      (naTable->getViewText()) &&
      (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)))
    {
      *CmpCommon::diags()
        << DgSqlCode(-3242)
        << DgString0("Cannot create external table on a native Hive view.");
      
      return -1;
    }

  // cqd HIVE_USE_EXT_TABLE_ATTRS:
  //  if OFF, col or key attrs cannot be specified during ext table creation.
  //  if ON,  col attrs could be specified.
  //  if ALL, col and key attrs could be specified
  NABoolean extTableAttrsSpecified = FALSE;
  if (colArray.entries() > 0)
    {
      if (CmpCommon::getDefault(HIVE_USE_EXT_TABLE_ATTRS) == DF_OFF)
        {
          *CmpCommon::diags()
            << DgSqlCode(-3242)
            << DgString0("Cannot specify column attributes for external tables.");
          return -1;
        }

      extTableAttrsSpecified = TRUE;
      CmpSeabaseDDL::setMDflags
        (tableInfo->tablesFlags, MD_TABLES_HIVE_EXT_COL_ATTRS);
    }
  
  if (keyArray.entries() > 0)
    {
      if (CmpCommon::getDefault(HIVE_USE_EXT_TABLE_ATTRS) != DF_ALL)
        {
          *CmpCommon::diags()
            << DgSqlCode(-3242)
            << DgString0("Cannot specify key attribute for external tables.");
          return -1;
        }

      extTableAttrsSpecified = TRUE;
      CmpSeabaseDDL::setMDflags
        (tableInfo->tablesFlags, MD_TABLES_HIVE_EXT_KEY_ATTRS);
     }
  
  // convert column array from NATable into a ComTdbVirtTableColumnInfo struct
  NAColumnArray naColArray;
  const NAColumnArray &origColArray = naTable->getNAColumnArray();
  NABoolean includeVirtHiveCols =
    (CmpCommon::getDefault(HIVE_EXT_TABLE_INCLUDE_VIRT_COLS) == DF_ON);

  // eliminate Hive virtual columns, unless requested via CQD
  for (CollIndex c=0; c<origColArray.entries(); c++)
    if (!origColArray[c]->isHiveVirtualColumn() || includeVirtHiveCols)
      naColArray.insert(origColArray[c]);

  numCols = naColArray.entries();

  // make sure all columns specified in colArray are part of naColArray
  if (colArray.entries() > 0)
    {
      for (CollIndex colIndex = 0; colIndex < colArray.entries(); colIndex++)
        {
          const ElemDDLColDef *edcd = colArray[colIndex];          
          
          if (naColArray.getColumnPosition((NAString&)edcd->getColumnName()) < 0)
            {
              // not found. return error.
              *CmpCommon::diags() << DgSqlCode(-1009) 
                                  << DgColumnName(ToAnsiIdentifier(edcd->getColumnName()));
	 
              return -1;
             }
        }
    }

  colInfoArray = new(STMTHEAP) ComTdbVirtTableColumnInfo[numCols];

  NAString format = CmpCommon::getDefaultString(TRAF_COMPOSITE_DATATYPE_FORMAT);
  NABoolean compColsInExplodedFormat = FALSE;
  for (CollIndex index = 0; index < numCols; index++)
    {
      const NAColumn *naCol = naColArray[index];
      const NAType * type = naCol->getType();
      
      // if colArray has been specified, then look for this column in
      // that array and use the type specified there.
      Int32 colIndex = -1;
      if ((colArray.entries() > 0) &&
          ((colIndex = colArray.getColumnIndex(naCol->getColName())) >= 0))
        {
          ElemDDLColDef *edcd = colArray[colIndex];
          const NAType * edcdType = edcd->getColumnDataType();

          if ((isHive) &&
              (CmpCommon::getDefault(HIVE_COMPOSITE_DATATYPE_SUPPORT) != DF_ALL) &&
              (edcdType->isComposite()))
            {
              NAString errStr("Cannot specify explicit external attributes for composite column ");
              errStr += naCol->getColName() + ".";
              *CmpCommon::diags() << DgSqlCode(-3242) <<
                DgString0(errStr);
              return -1;
            }

          type = edcdType;
        }
      
      // call:  CmpSeabaseDDL::getTypeInfo to get column details
      retcode = getTypeInfo(type, alignedFormat, serializedOption,
                            datatype, length, precision, scale, dtStart, dtEnd, 
                            upshifted, nullable,
                            charset, collationSequence, hbaseColFlags);
      
      if (retcode)
        return -1;

      if (length > CmpCommon::getDefaultNumeric(TRAF_MAX_CHARACTER_COL_LENGTH))
        {
          *CmpCommon::diags()
            << DgSqlCode(-4247)
            << DgInt0(length)
            << DgInt1(CmpCommon::getDefaultNumeric(TRAF_MAX_CHARACTER_COL_LENGTH)) 
            << DgString0(naCol->getColName().data());
          
          return -1;
        }

      colInfoArray[index].colName = naCol->getColName().data(); 
      colInfoArray[index].colNumber = index;
      colInfoArray[index].columnClass = COM_USER_COLUMN;
      colInfoArray[index].datatype = datatype;
      colInfoArray[index].length = length;
      colInfoArray[index].nullable = nullable;
      colInfoArray[index].charset = 
        (SQLCHARSET_CODE)CharInfo::getCharSetEnum(charset);
      colInfoArray[index].precision = precision;
      colInfoArray[index].scale = scale;
      colInfoArray[index].dtStart = dtStart;
      colInfoArray[index].dtEnd = dtEnd;
      colInfoArray[index].upshifted = upshifted;
      colInfoArray[index].colHeading = NULL;
      colInfoArray[index].hbaseColFlags = naCol->getHbaseColFlags();
      colInfoArray[index].defaultClass = COM_NULL_DEFAULT;
      colInfoArray[index].defVal = NULL;
      colInfoArray[index].hbaseColFam = naCol->getHbaseColFam();
      colInfoArray[index].hbaseColQual = naCol->getHbaseColQual();
      strcpy(colInfoArray[index].paramDirection, COM_UNKNOWN_PARAM_DIRECTION_LIT);
      colInfoArray[index].isOptional = FALSE;
      colInfoArray[index].colFlags = 0;

      if (type->isComposite())
        {
          const CompositeType *compType = (CompositeType*)type;
          const NAString &fieldsInfoStr = compType->getCompDefnStr();
          
          char * comp_str = new((STMTHEAP)) 
            char[fieldsInfoStr.length() +1];
          str_cpy_all(comp_str, (char*)fieldsInfoStr.data(), 
                      fieldsInfoStr.length());
          comp_str[fieldsInfoStr.length()] = 0;
          colInfoArray[index].compDefnStr = comp_str;
      
          // composite type. Set the flag.
          colInfoArray[index].colFlags |= SEABASE_COLUMN_IS_COMPOSITE;
          
          if (format == "EXPLODED")
            compColsInExplodedFormat = TRUE;
        } // composite
    } // for

  if (compColsInExplodedFormat || isHive)
    tableInfo->tablesFlags |= MD_TABLES_COMP_COLS_IN_EXPLODED_FORMAT;

  ComTdbVirtTableKeyInfo * keyInfoArray = NULL;
  Lng32 numKeys = 0;
  numKeys = keyArray.entries();
  if (numKeys > 0)
    {
      if (isHive)
        {
          *CmpCommon::diags()
            << DgSqlCode(-4222)
            << DgString0("\"PRIMARY KEY on external hive table\"");
          
          return -1;
        }

      keyInfoArray = new(STMTHEAP) ComTdbVirtTableKeyInfo[numKeys];
      if (buildKeyInfoArray(NULL, (NAColumnArray*)&naColArray, &keyArray, 
                            colInfoArray, keyInfoArray, TRUE))
        {
          *CmpCommon::diags()
            << DgSqlCode(-CAT_UNABLE_TO_CREATE_OBJECT)
            << DgTableName(extTgtTableName);
          
          return -1;
        }
    }

  NABoolean registerHiveObject =
    (cnSrc.isHive() &&
     CmpCommon::getDefault(HIVE_NO_REGISTER_OBJECTS) == DF_OFF);

  // if source table is a hive table and not already registered, register
  // it in traf metadata
  if (registerHiveObject)
    {
      char buf[2000];
      str_sprintf(buf, "register internal hive table if not exists %s.\"%s\".\"%s\"",
                  srcCatNamePart.data(), 
                  srcSchNamePart.data(), 
                  srcObjNamePart.data());
      
      Lng32 cliRC = cliInterface.executeImmediate(buf);
      if (cliRC < 0)
        {
          cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
          return -1;
        }
    } // ishive

  Int64 objUID = -1;
  cliRC = 0;

  // Update traf MD tables with info about this table.
  // But do not insert priv info if object is to be registered. 
  // That will happen during hive object registration.
  if (updateSeabaseMDTable(&cliInterface,
                           catalogNamePart, schemaNamePart, objectNamePart,
                           COM_BASE_TABLE_OBJECT,
                           COM_NO_LIT,
                           tableInfo,
                           numCols,
                           colInfoArray,
                           numKeys,
                           keyInfoArray,
                           0, NULL,
                           objUID /*returns generated UID*/,
                           (NOT registerHiveObject)))
    {
      *CmpCommon::diags()
        << DgSqlCode(-CAT_UNABLE_TO_CREATE_OBJECT)
        << DgTableName(extTgtTableName);
      return -1;
    }

  cliRC = updateObjectValidDef(&cliInterface,
                               catalogNamePart, schemaNamePart, objectNamePart,
                               COM_BASE_TABLE_OBJECT_LIT, COM_YES_LIT);

  if (cliRC < 0)
    {
      *CmpCommon::diags()
        << DgSqlCode(-CAT_UNABLE_TO_CREATE_OBJECT)
        << DgTableName(extTgtTableName);
      return -1;
    }

  // remove cached definition - this code exists in other create stmte,
  // is it required?
  CorrName cnTgt(objectNamePart, STMTHEAP, schemaNamePart, catalogNamePart);
  ActiveSchemaDB()->getNATableDB()->removeNATable
    (cnTgt,
     ComQiScope::REMOVE_MINE_ONLY,
     COM_BASE_TABLE_OBJECT,
     createTableNode->ddlXns(), FALSE);

  return 0;
}

short CmpSeabaseDDL::updatePKeyInfo(
                                    StmtDDLAddConstraintPK *addPKNode,
                                    const char * catName,
                                    const char * schName,
                                    const char * objName,
                                    const Int32 ownerID,
                                    const Int32 schemaOwnerID,
                                    Lng32 numKeys,
                                    Int64 * outPkeyUID,
                                    Int64 * outTableUID,
                                    const ComTdbVirtTableKeyInfo * keyInfoArray,
                                    ExeCliInterface *cliInterface)
{
  Lng32 retcode = 0;
  Lng32 cliRC = 0;

  char buf[4000];

  // update primary key constraint info
  NAString pkeyStr;

  // if this was a user named primary key constraint, pass the ddl node.
  // Otherwise pass in NULL in which case catName/schName/objName will be
  // used to generate an internal name.
  NABoolean namedPKey = FALSE;
  if (addPKNode && addPKNode->getConstraint()->castToElemDDLConstraint() &&
      (! addPKNode->getConstraint()->castToElemDDLConstraint()->getConstraintName().isNull()))
    namedPKey = TRUE;
  if (genUniqueName((namedPKey ? addPKNode : NULL), NULL, pkeyStr,  
                    catName, schName, objName))
    {
      return -1;
    }

  Int64 createTime = NA_JulianTimestamp();

  ComUID comUID;
  comUID.make_UID();
  Int64 pkeyUID = comUID.get_value();
  if (outPkeyUID)
    *outPkeyUID = pkeyUID;

  ComObjectName pkeyName(pkeyStr);
  const NAString catalogNamePart = pkeyName.getCatalogNamePartAsAnsiString();
  const NAString schemaNamePart = pkeyName.getSchemaNamePartAsAnsiString(TRUE);
  const NAString objectNamePart = pkeyName.getObjectNamePartAsAnsiString(TRUE);
  NAString quotedSchName;
  ToQuotedString(quotedSchName, NAString(schemaNamePart), FALSE);
  NAString quotedObjName;
  ToQuotedString(quotedObjName, NAString(objectNamePart), FALSE);

  // COM_PRIMARY_KEY_CONSTRAINT_OBJECT_LIT not hava a entity hbase
  str_sprintf(buf, "insert into %s.\"%s\".%s values ('%s', '%s', '%s', '%s', %ld, %ld, %ld, '%s', '%s', %d, %d, 0)",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS,
              catalogNamePart.data(), quotedSchName.data(), quotedObjName.data(),
              COM_PRIMARY_KEY_CONSTRAINT_OBJECT_LIT,
              pkeyUID,
              createTime, 
              createTime,
              " ",
              COM_NO_LIT,
              ownerID,
              schemaOwnerID);
  cliRC = cliInterface->executeImmediate(buf);
  
  if (cliRC < 0)
    {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());

      *CmpCommon::diags() << DgSqlCode(-1423)
                          << DgString0(SEABASE_OBJECTS);

      return -1;
    }

  Int64 tableUID = 
    getObjectUID(cliInterface,
                 catName, schName, objName,
                 COM_BASE_TABLE_OBJECT_LIT);

  if (outTableUID)
    *outTableUID = tableUID;

  Int64 validatedTime = NA_JulianTimestamp();

  Int64 flags = 0;

  // if pkey is specified to be not_serialized, then set it.
  // if this is an hbase mapped table and pkey serialization is not specified, 
  // then set to not_serialized.
  NABoolean notSerializedPK = FALSE;
  if ((addPKNode->getAlterTableAction()->castToElemDDLConstraintPK()->ser() == 
       ComPkeySerialization::COM_NOT_SERIALIZED) ||
      (ComIsHbaseMappedSchemaName(schName) && 
       (addPKNode->getAlterTableAction()->castToElemDDLConstraintPK()->ser() == 
        ComPkeySerialization::COM_SER_NOT_SPECIFIED)))
    notSerializedPK = TRUE;
  
  if (notSerializedPK)
    {
      CmpSeabaseDDL::setMDflags
        (flags, CmpSeabaseDDL::MD_TABLE_CONSTRAINTS_PKEY_NOT_SERIALIZED_FLG);
    }
 
  Int64 indexUID = 0;
  str_sprintf(buf, "insert into %s.\"%s\".%s values (%ld, %ld, '%s', '%s', '%s', '%s', '%s', '%s', %ld, %d, %ld, %ld )",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_TABLE_CONSTRAINTS,
              tableUID, pkeyUID,
              COM_PRIMARY_KEY_CONSTRAINT_LIT,
              COM_NO_LIT,
              COM_NO_LIT,
              COM_NO_LIT,
              COM_YES_LIT,
              COM_YES_LIT,
              validatedTime,
              numKeys,
              indexUID,
              flags);
  cliRC = cliInterface->executeImmediate(buf);
  
  if (cliRC < 0)
    {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());

      *CmpCommon::diags() << DgSqlCode(-1423)
                          << DgString0(SEABASE_TABLE_CONSTRAINTS);

      return -1;
    }

  if (keyInfoArray)
    {
      for (Lng32 i = 0; i < numKeys; i++)
        {
          str_sprintf(buf, "insert into %s.\"%s\".%s values (%ld, '%s', %d, %d, %d, %d, 0)",
                      getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_KEYS,
                      pkeyUID,
                      keyInfoArray[i].colName,
                      i+1,
                      keyInfoArray[i].tableColNum,
                      0,
                      0);
          
          cliRC = cliInterface->executeImmediate(buf);
          if (cliRC < 0)
            {
              cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
              
              *CmpCommon::diags() << DgSqlCode(-1423)
                                  << DgString0(SEABASE_KEYS);

              return -1;
            }
        }
    }

  return 0;
}

// ----------------------------------------------------------------------------
// Method: getPKeyInfoForTable
//
// This method reads the metadata to get the primary key constraint name and UID
// for a table.
//
// Params:
//   In:  catName, schName, objName describing the table
//   In:  cliInterface - pointer to the cli handle
//   Out:  constrName and constrUID
//
// Returns 0 if found, -1 otherwise
// ComDiags is set up with error
// ---------------------------------------------------------------------------- 
short CmpSeabaseDDL::getPKeyInfoForTable (
                                          const char *catName,
                                          const char *schName,
                                          const char *objName,
                                          ExeCliInterface *cliInterface,
                                          NAString &constrName,
                                          Int64 &constrUID,
                                          NABoolean errorIfNotExists)
{
  char query[4000];
  constrUID = -1;

  // get constraint info
  str_sprintf(query, "select O.object_name, C.constraint_uid "
                     "from %s.\"%s\".%s O, %s.\"%s\".%s C "
                     "where O.object_uid = C.constraint_uid "
                     "  and C.constraint_type = '%s' and C.table_uid = "
                     "   (select object_uid from %s.\"%s\".%s "
                     "    where catalog_name = '%s' "
                     "      and schema_name = '%s' " 
                     "      and object_name = '%s')",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS,
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_TABLE_CONSTRAINTS,
              COM_PRIMARY_KEY_CONSTRAINT_LIT,
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS,
              catName, schName, objName);

  Queue * constrInfoQueue = NULL;
  Lng32 cliRC = cliInterface->fetchAllRows(constrInfoQueue, query, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0)
    {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());

      processReturn();

      return -1;
    }

  //if errorIfNotExists set false, do not raise error 
  if (constrInfoQueue->numEntries() == 0 &&
      errorIfNotExists == FALSE)
  {
    constrUID = 0;
    return 0;
  }

  assert (constrInfoQueue->numEntries() == 1);
  constrInfoQueue->position();
  OutputInfo * vi = (OutputInfo*)constrInfoQueue->getNext();
  char * pConstrName = (char*)vi->get(0);
  constrName = pConstrName;
  constrUID = *(Int64*)vi->get(1);

  return 0;
}

short CmpSeabaseDDL::constraintErrorChecks(
                                           ExeCliInterface * cliInterface,
                                           StmtDDLAddConstraint *addConstrNode,
                                           NATable * naTable,
                                           ComConstraintType ct,
                                           NAList<NAString> &keyColList)
{

  const NAString &addConstrName = addConstrNode->
    getConstraintNameAsQualifiedName().getQualifiedNameAsAnsiString();

  if (naTable && naTable->isHbaseMapTable() &&
      ((ct == COM_UNIQUE_CONSTRAINT) || 
       (ct == COM_FOREIGN_KEY_CONSTRAINT) ||
       (ct == COM_PRIMARY_KEY_CONSTRAINT)))
     {
      *CmpCommon::diags() << DgSqlCode(-3242)
                          << DgString0("Cannot specify unique, referential or check constraint for an HBase mapped table.");
      
      processReturn();
      
      return -1;
    }

  // make sure that there is no other constraint on this table with this name.
  NABoolean foundConstr = FALSE;
  const CheckConstraintList &checkList = naTable->getCheckConstraints();
  for (Int32 i = 0; i < checkList.entries(); i++)
    {
      CheckConstraint *checkConstr = (CheckConstraint*)checkList[i];
      
      const NAString &tableConstrName = 
        checkConstr->getConstraintName().getQualifiedNameAsAnsiString();
      
      if (addConstrName == tableConstrName)
        {
          foundConstr = TRUE;
        }
    } // for
  
  if (NOT foundConstr)
    {
      const AbstractRIConstraintList &ariList = naTable->getUniqueConstraints();
      for (Int32 i = 0; i < ariList.entries(); i++)
        {
          AbstractRIConstraint *ariConstr = (AbstractRIConstraint*)ariList[i];
          
          const NAString &tableConstrName = 
            ariConstr->getConstraintName().getQualifiedNameAsAnsiString();
          
          if (addConstrName == tableConstrName)
            {
              foundConstr = TRUE;
            }
        } // for
    }

  if (NOT foundConstr)
    {
      const AbstractRIConstraintList &ariList = naTable->getRefConstraints();
      for (Int32 i = 0; i < ariList.entries(); i++)
        {
          AbstractRIConstraint *ariConstr = (AbstractRIConstraint*)ariList[i];
          
          const NAString &tableConstrName = 
            ariConstr->getConstraintName().getQualifiedNameAsAnsiString();
          
          if (addConstrName == tableConstrName)
            {
              foundConstr = TRUE;
            }
        } // for
    }
  
  if (NOT foundConstr)
    {
      const NAString &constrCatName = addConstrNode->
        getConstraintNameAsQualifiedName().getCatalogName();
      const NAString &constrSchName = addConstrNode->
        getConstraintNameAsQualifiedName().getSchemaName();
      const NAString &constrObjName = addConstrNode->
        getConstraintNameAsQualifiedName().getObjectName();

      // check to see if this constraint was defined on some other table and
      // exists in metadata
      Lng32 retcode = existsInSeabaseMDTable(cliInterface, 
                                             constrCatName, constrSchName, constrObjName,
                                             COM_UNKNOWN_OBJECT, FALSE, FALSE);
      if (retcode == 1) // exists
        {
          foundConstr = TRUE;
        }
    }

  if (foundConstr)
    {
      *CmpCommon::diags()
        << DgSqlCode(-1043)
        << DgConstraintName(addConstrName);
      
      processReturn();
      
      return -1;
    }
  
  if ((ct == COM_UNIQUE_CONSTRAINT) || 
      (ct == COM_FOREIGN_KEY_CONSTRAINT) ||
      (ct == COM_PRIMARY_KEY_CONSTRAINT))
    {
      const NAColumnArray & naColArray = naTable->getNAColumnArray();
      
      // Now process each column defined in the parseColList to see if
      // it exists in the table column list and it isn't a duplicate.
      NABitVector seenIt; 
      NAString keyColNameStr;
      for (CollIndex i = 0; i < keyColList.entries(); i++)
        {
          NAColumn * nac = naColArray.getColumn(keyColList[i]);
          if (! nac)
            {
              *CmpCommon::diags() << DgSqlCode(-1009)
                                  << DgColumnName( ToAnsiIdentifier(keyColList[i]));
              return -1;
            }
          if (nac->isSystemColumn())
            {
              *CmpCommon::diags() << DgSqlCode((ct == COM_FOREIGN_KEY_CONSTRAINT) ?
                                               -CAT_SYSTEM_COL_NOT_ALLOWED_IN_RI_CNSTRNT :
                                               -CAT_SYSTEM_COL_NOT_ALLOWED_IN_UNIQUE_CNSTRNT)
                                  << DgColumnName(ToAnsiIdentifier(keyColList[i]))
                                  << DgTableName(addConstrNode->getTableName());
              return -1;
            }

	  // If column is a LOB column , error
	  Lng32 datatype = nac->getType()->getFSDatatype();
          if (nac->getType()->getFSDatatype() == REC_BLOB || 
              nac->getType()->getFSDatatype() == REC_CLOB ||
              nac->getType()->getFSDatatype() == REC_ROW ||
              nac->getType()->getFSDatatype() == REC_ARRAY)
            //Cannot allow LOB, ROW or ARRAY in primary or clustering key 
            {
              NAString type;
              if (nac->getType()->getFSDatatype() == REC_BLOB ||
                  nac->getType()->getFSDatatype() == REC_CLOB)
                type = "BLOB/CLOB";
              else if (nac->getType()->getFSDatatype() == REC_ROW)
                type = "ROW";
              else
                type = "ARRAY";
              *CmpCommon::diags() << DgSqlCode(-CAT_LOB_COL_CANNOT_BE_INDEX_OR_KEY)
                                  << DgColumnName( ToAnsiIdentifier(keyColList[i]))
                                  << DgString0(type);
              return -1;
            }

          Lng32 colNumber = nac->getPosition();
          
          // If the column has already been found, return error
          if( seenIt.contains(colNumber)) 
            {
              *CmpCommon::diags() << DgSqlCode(-CAT_REDUNDANT_COLUMN_REF_PK)
                                  << DgColumnName( ToAnsiIdentifier(keyColList[i]));
              return -1;
            }
          
          seenIt.setBit(colNumber);
        }

      if (ct == COM_UNIQUE_CONSTRAINT)      
        {
          // Compare the column list from parse tree to the unique and primary
          // key constraints already defined for the table.  The new unique 
          // constraint list must be distinct.  The order of the existing constraint
          // does not have to be in the same order as the new constraint.
          //
          if (naTable->getCorrespondingConstraint(keyColList,
                                                  TRUE, // unique constraint
                                                  NULL))
            {
              *CmpCommon::diags() << DgSqlCode(-CAT_DUPLICATE_UNIQUE_CONSTRAINT_ON_SAME_COL);
              
              return -1;
            }
        }
    }

  return 0;
}

// If addConstrNode is a user named constraint, use it.
// Otherwise generate an internal constraint name.
// If unnamedConstrNum is greater than 0, use it to generate name and
// increment it before returning.
// Otherwise, generate a name with a randone suffix
short CmpSeabaseDDL::genUniqueName(StmtDDLAddConstraint *addConstrNode,
                                   Int32 *unnamedConstrNum,
                                   NAString &uniqueName,
                                   const char * catName,
                                   const char * schName,
                                   const char * objName)
{
  ComObjectName tableName((addConstrNode ? addConstrNode->getTableName() : ""),
                           COM_TABLE_NAME);

  ElemDDLConstraint *constraintNode = 
    (addConstrNode ? (addConstrNode->getConstraint())->castToElemDDLConstraint()
     : NULL);

  ComString specifiedConstraint;
  ComString constraintName;

  if (!constraintNode || constraintNode->getConstraintName().isNull())
    {
      char buf[1000];
      if (tableName.isValid())
        {
          specifiedConstraint.append(tableName.getCatalogNamePartAsAnsiString());
          specifiedConstraint.append(".");
          specifiedConstraint.append(tableName.getSchemaNamePartAsAnsiString());
          specifiedConstraint.append(".");
        }
      else
        {
          // use catName, schName, objName. 
          assert(catName && schName && objName);
          specifiedConstraint.append(catName);
          specifiedConstraint.append(".");
          specifiedConstraint.append("\"");
          specifiedConstraint.append( schName); 
          specifiedConstraint.append("\"");
          specifiedConstraint.append(".");          
        }

      if (unnamedConstrNum  && (*unnamedConstrNum > 0))
        {
          str_sprintf(buf, "\"%s%d_%s\"", 
                      TRAF_UNNAMED_CONSTR_PREFIX,  
                      *unnamedConstrNum, 
                      (tableName.isValid() ?
                       tableName.getObjectNamePartAsAnsiString(TRUE).data() : 
                       objName));
          specifiedConstraint.append(buf);
          (*unnamedConstrNum)++;
        }
      else
        {
          ComString oName;
          if (tableName.isValid())
            oName = tableName.getObjectNamePart();
          else 
            {
              oName = "\"";
              oName += objName;
              oName += "\"";
            }
          Lng32 status = ToInternalIdentifier ( 
               oName // in/out - from external- to internal-format
               , TRUE  // in - NABoolean upCaseInternalNameIfRegularIdent
               , TRUE  // in - NABoolean acceptCircumflex
                                                );
          ComDeriveRandomInternalName ( 
               ComGetNameInterfaceCharSet()
               , /*internalFormat*/oName          // in  - const ComString &
               , /*internalFormat*/constraintName // out - ComString &
               , STMTHEAP
                                        );
          
          // Generate a delimited identifier if objectName was delimited
          constraintName/*InExternalFormat*/ = ToAnsiIdentifier (constraintName/*InInternalFormat*/);
          
          specifiedConstraint.append(constraintName);          
        }
    }
  else
    {
      // User named constraint cannot start with TRAF_UNNAMED_CONSTR_PREFIX.
      // Check for it. 
      const NAString &constrName = constraintNode->
        getConstraintNameAsQualifiedName().getObjectName();
      if ((constrName.length() >= (strlen(TRAF_UNNAMED_CONSTR_PREFIX))) &&
          (constrName.index(TRAF_UNNAMED_CONSTR_PREFIX) == 0) &&
          (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)))
        {
          *CmpCommon::diags() << DgSqlCode(-3242)
                              << DgString0(NAString("The prefix \"") + NAString(TRAF_UNNAMED_CONSTR_PREFIX) + NAString("\" is reserved for unnamed constraints. It cannot be specified for a named constraint."));
          
          return -1;      
        }  

      specifiedConstraint = constraintNode->
        getConstraintNameAsQualifiedName().getQualifiedNameAsAnsiString();
    }

  uniqueName = specifiedConstraint;

  ComObjectName constrName(uniqueName);
  if (unnamedConstrNum && (*unnamedConstrNum > 0) && (NOT constrName.isValid()))
    {
      NAString constrType;
      if (constraintNode)
        {
          if (constraintNode->getOperatorType() == ELM_CONSTRAINT_UNIQUE_ELEM)
            constrType = "UNIQUE ";
          else if (addConstrNode->getOperatorType() == ELM_CONSTRAINT_REFERENTIAL_INTEGRITY_ELEM)
            constrType = "REFERENTIAL ";
        }
      *CmpCommon::diags()
        << DgSqlCode(-3242)
        << DgString0(NAString("An invalid name(") + uniqueName + NAString(") was generated for this unnamed " + constrType + "constraint. Reissue the query after providing an explicit name for this constraint. "));
      return -1;
    }
  
  return 0;
}

short CmpSeabaseDDL::updateConstraintMD(
                                        NAList<NAString> &keyColList,
                                        NAList<NAString> &keyColOrderList,
                                        NAString &uniqueStr,
                                        Int64 tableUID,
                                        Int64 constrUID,
                                        NATable * naTable,
                                        ComConstraintType ct,
                                        NABoolean enforced,
                                        ExeCliInterface *cliInterface)
{
  Lng32 retcode = 0;
  Lng32 cliRC = 0;

  char buf[4000];

  const NAColumnArray & naColArray = naTable->getNAColumnArray();

  Int64 createTime = NA_JulianTimestamp();

  ComObjectName uniqueName(uniqueStr);
  const NAString catalogNamePart = uniqueName.getCatalogNamePartAsAnsiString();
  const NAString schemaNamePart = uniqueName.getSchemaNamePartAsAnsiString(TRUE);
  const NAString objectNamePart = uniqueName.getObjectNamePartAsAnsiString(TRUE);
  NAString quotedSchName;
  ToQuotedString(quotedSchName, NAString(schemaNamePart), FALSE);
  NAString quotedObjName;
  ToQuotedString(quotedObjName, NAString(objectNamePart), FALSE);

  // Constraint not have a entity hbase table
  str_sprintf(buf, "insert into %s.\"%s\".%s values ('%s', '%s', '%s', '%s', %ld, %ld, %ld, '%s', '%s', %d, %d, 0)",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS,
              catalogNamePart.data(), quotedSchName.data(), quotedObjName.data(),
              ((ct == COM_UNIQUE_CONSTRAINT) ? COM_UNIQUE_CONSTRAINT_OBJECT_LIT :
               ((ct == COM_FOREIGN_KEY_CONSTRAINT) ? COM_REFERENTIAL_CONSTRAINT_OBJECT_LIT : COM_CHECK_CONSTRAINT_OBJECT_LIT)),
              constrUID,
              createTime, 
              createTime,
              " ",
              COM_NO_LIT,
              naTable->getOwner(),
              naTable->getSchemaOwner());
  cliRC = cliInterface->executeImmediate(buf);
  
  if (cliRC < 0)
    {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      return -1;
    }

  Int64 indexUID = 0;
  str_sprintf(buf, "insert into %s.\"%s\".%s values (%ld, %ld, '%s', '%s', '%s', '%s', '%s', '%s', %ld, %d, %ld, 0 )",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_TABLE_CONSTRAINTS,
              tableUID, constrUID,
              ((ct == COM_UNIQUE_CONSTRAINT) ? COM_UNIQUE_CONSTRAINT_LIT :
               ((ct == COM_FOREIGN_KEY_CONSTRAINT) ? COM_FOREIGN_KEY_CONSTRAINT_LIT : COM_CHECK_CONSTRAINT_LIT)),
              COM_NO_LIT,
              COM_NO_LIT,
              COM_NO_LIT,
              (enforced ? COM_YES_LIT : COM_NO_LIT),
              COM_YES_LIT,
              createTime,
              keyColList.entries(),
              indexUID);
  cliRC = cliInterface->executeImmediate(buf);
  
  if (cliRC < 0)
    {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      return -1;
    }

  for (Lng32 i = 0; i < keyColList.entries(); i++)
    {
      NAColumn * nac = naColArray.getColumn(keyColList[i]);
      Lng32 colNumber = nac->getPosition();

      str_sprintf(buf, "insert into %s.\"%s\".%s values (%ld, '%s', %d, %d, %d, %d, 0)",
                  getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_KEYS,
                  constrUID,
                  keyColList[i].data(),
                  i+1,
                  colNumber,
                  (keyColOrderList[i] == "DESC" ? 1 : 0),
                  0);
      
      cliRC = cliInterface->executeImmediate(buf);
      if (cliRC < 0)
        {
          cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());

          return -1;
        }
    }


  return 0;
}

short CmpSeabaseDDL::updateRIConstraintMD(
                                          Int64 ringConstrUID,
                                          Int64 refdConstrUID,
                                          ExeCliInterface *cliInterface)
{
  Lng32 retcode = 0;
  Lng32 cliRC = 0;

  char buf[4000];

  str_sprintf(buf, "insert into %s.\"%s\".%s values (%ld, %ld, '%s', '%s', '%s', 0 )",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_REF_CONSTRAINTS,
              ringConstrUID, refdConstrUID,
              COM_FULL_MATCH_OPTION_LIT,
              COM_RESTRICT_UPDATE_RULE_LIT,
              COM_RESTRICT_DELETE_RULE_LIT);
  cliRC = cliInterface->executeImmediate(buf);
  
  if (cliRC < 0)
    {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      return -1;
    }

  str_sprintf(buf, "insert into %s.\"%s\".%s values (%ld, %ld, 0)",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_UNIQUE_REF_CONSTR_USAGE,
              refdConstrUID, ringConstrUID);
  cliRC = cliInterface->executeImmediate(buf);
  
  if (cliRC < 0)
    {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      return -1;
    }

  return 0;
}

short CmpSeabaseDDL::updateIndexInfo(
                                     NAList<NAString> &ringKeyColList,
                                     NAList<NAString> &ringKeyColOrderList,
                                     NAList<NAString> &refdKeyColList,
                                     NAString &uniqueStr,
                                     Int64 constrUID,
                                     const char * catName,
                                     const char * schName,
                                     const char * objName,
                                     NATable * naTable,
                                     NABoolean isUnique, // TRUE: uniq constr. FALSE: ref constr.
                                     NABoolean noPopulate,
                                     NABoolean isEnforced,
                                     NABoolean sameSequenceOfCols,
                                     ExeCliInterface *cliInterface)
{
  // Now we need to determine if an index has to be created for
  // the unique or ref constraint.  
  NABoolean createIndex = TRUE;
  NAString existingIndexName;

  if (naTable->getCorrespondingIndex(ringKeyColList, 
                                     TRUE, // explicit index only
                                     isUnique, //TRUE, look for unique index.
                                     TRUE, //isUnique, //TRUE, look for primary key.
                                     (NOT isUnique), // TRUE, look for any index or pkey
                                     TRUE, // exclude system computed cols like salt, division
                                     sameSequenceOfCols,
                                     &existingIndexName))
    createIndex = FALSE;

  // if constraint is not to be enforced, then do not create an index.
  if (createIndex && (NOT isEnforced))
    return 0;

  ComObjectName indexName(createIndex ? uniqueStr : existingIndexName);
  const NAString catalogNamePart = indexName.getCatalogNamePartAsAnsiString();
  const NAString schemaNamePart = indexName.getSchemaNamePartAsAnsiString(TRUE);
  const NAString objectNamePart = indexName.getObjectNamePartAsAnsiString(TRUE);
  NAString quotedSchName;
  ToQuotedString(quotedSchName, NAString(schemaNamePart), FALSE);
  NAString quotedObjName;
  ToQuotedString(quotedObjName, NAString(objectNamePart), FALSE);

  char buf[5000];
  Lng32 cliRC;

  Int64 tableUID = naTable->objectUid().castToInt64();
  
  if (createIndex)
    {
      NAString keyColNameStr;
      for (CollIndex i = 0; i < ringKeyColList.entries(); i++)
        {
          keyColNameStr += "\"";
          keyColNameStr += ringKeyColList[i];
          keyColNameStr += "\" ";
          keyColNameStr += ringKeyColOrderList[i];
          if (i+1 < ringKeyColList.entries())
            keyColNameStr += ", ";
        }

      char noPopStr[100];
      if (noPopulate)
        strcpy(noPopStr, " no populate ");
      else
        strcpy(noPopStr, " ");

      if (naTable->isPartitionV2Table())
      {
        /* begin partition table create global index add by kai.deng */
        if (isUnique)
          str_sprintf(buf, "create unique index \"%s\" on \"%s\".\"%s\".\"%s\" ( %s ) %s global",
                      quotedObjName.data(),
                      catName, schName, objName,
                      keyColNameStr.data(),
                      noPopStr);
        else
          str_sprintf(buf, "create index \"%s\" on \"%s\".\"%s\".\"%s\" ( %s ) %s global",
                      quotedObjName.data(),
                      catName, schName, objName,
                      keyColNameStr.data(),
                      noPopStr);
        /* end partition table create global index add by kai.deng */
      }
      else
      {
        if (isUnique)
          str_sprintf(buf, "create unique index \"%s\" on \"%s\".\"%s\".\"%s\" ( %s ) %s",
                      quotedObjName.data(),
                      catName, schName, objName,
                      keyColNameStr.data(),
                      noPopStr);
        else
          str_sprintf(buf, "create index \"%s\" on \"%s\".\"%s\".\"%s\" ( %s ) %s",
                      quotedObjName.data(),
                      catName, schName, objName,
                      keyColNameStr.data(),
                      noPopStr);
      }

      cliRC = cliInterface->executeImmediate(buf);
      
      if (cliRC < 0)
        {
          cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
          return -1;
        }

      // update indexes table and mark this index as an implicit index.
      str_sprintf(buf, "update %s.\"%s\".%s set is_explicit = 0 where base_table_uid = %ld and index_uid = (select object_uid from %s.\"%s\".%s where catalog_name = '%s' and schema_name = '%s' and object_name = '%s' and object_type = 'IX') ",
                  getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_INDEXES,
                  tableUID,
                  getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS,
                  catName, schemaNamePart.data(), objectNamePart.data());
     cliRC = cliInterface->executeImmediate(buf);
      if (cliRC < 0)
        {
          cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());

          return -1;
        }

      if (noPopulate)
        {
          if (updateObjectValidDef(cliInterface, 
                                   catalogNamePart, schemaNamePart, objectNamePart,
                                   COM_INDEX_OBJECT_LIT,
                                   COM_YES_LIT))
            {
              return -1;
            }
        }

      // Add entry to list of DDL operations
      if (naTable && naTable->isStoredDesc() && (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)))
        insertIntoSharedCacheList(catalogNamePart, schemaNamePart, objectNamePart,
                                  COM_INDEX_OBJECT, SharedCacheDDLInfo::DDL_INSERT);
    }

  // update table_constraints table with the uid of this index.
  Int64 indexUID = 
    getObjectUID(cliInterface,
                 catName, schemaNamePart, objectNamePart,
                 COM_INDEX_OBJECT_LIT);
  if (indexUID < 0)
    {
      // primary key. Clear diags area since getObjectUID sets up diags entry.
      CmpCommon::diags()->clear();
    }

  str_sprintf(buf, "update %s.\"%s\".%s set index_uid = %ld where table_uid = %ld and constraint_uid = %ld and constraint_type = '%s'",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_TABLE_CONSTRAINTS,
              indexUID,
              tableUID, constrUID,
              (isUnique ? COM_UNIQUE_CONSTRAINT_LIT : COM_FOREIGN_KEY_CONSTRAINT_LIT));
  cliRC = cliInterface->executeImmediate(buf);
  
  if (cliRC < 0)
    {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      return -1;
    }

  return 0;
}

// if this object is locked due to BR and ddl operations are disallowed
// during BR, return an error.
short CmpSeabaseDDL::isLockedForBR(Int64 objUID, Int64 schUID,
                                   ExeCliInterface *cliInterface,
                                   const CorrName &corrName)
{
  if (objUID <= 0)
    return 0;

  const NAString &objName = corrName.getExposedNameAsAnsiString();

  Int16 brLockedObjectsAccess = 
    CmpCommon::getDefaultNumeric(TRAF_BR_LOCKED_OBJECTS_ACCESS);
  Int16 brLockedObjectsQueryType = 
    CmpCommon::getDefaultNumeric(TRAF_BR_LOCKED_OBJECTS_QUERY_TYPE);
  if ((brLockedObjectsAccess != 0) && // check for locked objects.
      (brLockedObjectsQueryType < 2)) // DDL query is not allowed.
    {
      NAString brTag;

      if (schUID <= 0)
        {
          schUID = getObjectUID(cliInterface,
                                corrName.getQualifiedNameObj().getCatalogName().data(),
                                corrName.getQualifiedNameObj().getSchemaName().data(),
                                SEABASE_SCHEMA_OBJECTNAME,
                                NULL, NULL, "object_type in ('SS', 'PS')",
                                NULL, FALSE, FALSE);
        }

      if (schUID > 0)
        {
          // first check if schema is locked
          if (getTextFromMD(cliInterface, schUID, COM_LOCKED_OBJECT_BR_TAG, 0,
                            brTag))
            {
              return -1;
            } 
          
          if (NAString(brTag).contains("(BACKUP)"))
            {
              *CmpCommon::diags() << DgSqlCode(-4264)
                                  << DgString0(objName)
                                  << DgString1(brTag);
              return -1;
            }       
        }

      // and then check if object is locked
      if (getTextFromMD(cliInterface, objUID, COM_LOCKED_OBJECT_BR_TAG, 0,
                        brTag))
        {
          return -1;
        } 
      
      if (NAString(brTag).contains("(BACKUP)"))
        {
          *CmpCommon::diags() << DgSqlCode(-4264)
                              << DgString0(objName)
                              << DgString1(brTag);
          return -1;
        }          
    }
  
  return 0;
}  

//RETURN: -1 in case of error.  -2, if specified object doesnt exist in MD.
short CmpSeabaseDDL::setupAndErrorChecks
(NAString &tabName, QualifiedName &origTableName, 
 NAString &currCatName, NAString &currSchName,
 NAString &catalogNamePart, NAString &schemaNamePart, NAString &objectNamePart,
 NAString &extTableName, NAString &extNameForHbase,
 CorrName &cn,
 NATable* *naTable,
 NABoolean volTabSupported,
 NABoolean hbaseMapSupported,
 ExeCliInterface *cliInterface,
 const ComObjectType objectType,
 SQLOperation operation,
 NABoolean isExternal,
 NABoolean processHiatus,
 NABoolean createHiatusIfNotExist)
{
  Lng32 cliRC = 0;
  Lng32 retcode = 0;

  ComObjectName tableName(tabName, COM_TABLE_NAME);
  ComAnsiNamePart currCatAnsiName(currCatName);
  ComAnsiNamePart currSchAnsiName(currSchName);
  tableName.applyDefaults(currCatAnsiName, currSchAnsiName);

  NABoolean schNameSpecified = (NOT origTableName.getSchemaName().isNull());

  NABoolean hbmapExtTable = FALSE;
  if ((tableName.getCatalogNamePartAsAnsiString() == HBASE_SYSTEM_CATALOG) &&
      (tableName.getSchemaNamePartAsAnsiString(TRUE) == HBASE_MAP_SCHEMA) &&
      (hbaseMapSupported))
    hbmapExtTable = TRUE;

  if (isExternal || hbmapExtTable)
    {
      // Convert the native name to its Trafodion form
      tabName = ComConvertNativeNameToTrafName
        (tableName.getCatalogNamePartAsAnsiString(),
         tableName.getSchemaNamePartAsAnsiString(TRUE),
         tableName.getObjectNamePartAsAnsiString());
                               
      ComObjectName adjustedName(tabName, COM_TABLE_NAME);
      tableName = adjustedName;
    }

  catalogNamePart = tableName.getCatalogNamePartAsAnsiString();
  schemaNamePart = tableName.getSchemaNamePartAsAnsiString(TRUE);
  objectNamePart = tableName.getObjectNamePartAsAnsiString(TRUE);
  extTableName = tableName.getExternalName(TRUE);

  if ((isSeabaseReservedSchema(tableName)) &&
      (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)))
    {
      *CmpCommon::diags() << DgSqlCode(-CAT_CANNOT_ALTER_DEFINITION_METADATA_SCHEMA);
      processReturn();
      return -1;
    }

  if (((catalogNamePart == HBASE_SYSTEM_CATALOG) &&
       ((schemaNamePart == HBASE_CELL_SCHEMA) ||
        (schemaNamePart == HBASE_ROW_SCHEMA))) &&
      (operation == SQLOperation::ALTER_TABLE))
    {
      // not supported
      *CmpCommon::diags() << DgSqlCode(-3242)
                          << DgString0("This feature is not available for an HBase ROW or CELL format table.");
      
      processReturn();
      
      return -1;          
    }

  retcode = lookForTableInMD(cliInterface, 
                             catalogNamePart, schemaNamePart, objectNamePart,
                             (schNameSpecified && (NOT hbmapExtTable)),
                             hbmapExtTable,
                             tableName, tabName, extTableName,
                             objectType);
  if (retcode < 0)
    {
      processReturn();

      return -1;
    }

  if (retcode == 0) // doesn't exist
    {
      if (objectType == COM_BASE_TABLE_OBJECT)
        *CmpCommon::diags() << DgSqlCode(-1127)
                            << DgTableName(extTableName);
      else 
        *CmpCommon::diags() << DgSqlCode(-1389)
                            << DgString0(extTableName);

      processReturn();
      
      return -2;
    }

  Int32 objectOwnerID = 0;
  Int32 schemaOwnerID = 0;
  Int64 objDataUID = 0;
  if (naTable)
    {
      ActiveSchemaDB()->getNATableDB()->useCache();
      
      BindWA bindWA(ActiveSchemaDB(), CmpCommon::context(), TRUE/*inDDL*/);
      cn = CorrName(objectNamePart, STMTHEAP, schemaNamePart, catalogNamePart);
      
      *naTable = bindWA.getNATableInternal(cn); 
      if (*naTable == NULL || bindWA.errStatus())
        {
          if (!CmpCommon::diags()->contains(-4082))
            *CmpCommon::diags()
              << DgSqlCode(-4082)
              << DgTableName(cn.getExposedNameAsAnsiString());
          
          processReturn();
          
          return -1;
        }

      objectOwnerID = (*naTable)->getOwner();
      schemaOwnerID = (*naTable)->getSchemaOwner();
      objDataUID = (*naTable)->objDataUID().castToInt64();

      // Make sure user has the privilege to perform the alter column
      if (!isDDLOperationAuthorized(operation,
                                    objectOwnerID, schemaOwnerID,
                                    (int64_t)(*naTable)->objectUid().get_value(), 
                                    COM_BASE_TABLE_OBJECT, (*naTable)->getPrivInfo()))
        {
          *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);
          
          processReturn ();
          
          return -1;
        }

      Int64 schUID = (*naTable)->getSchemaUid().castToInt64();
      if (isLockedForBR((*naTable)->objectUid().castToInt64(), 
                        schUID,
                        cliInterface,
                        cn))
        {
          processReturn();
          return -1;
        }

      // return an error if trying to alter a column from a volatile table
      if ((NOT volTabSupported) && (naTable && (*naTable)->isVolatileTable()))
        {
          *CmpCommon::diags() << DgSqlCode(-CAT_REGULAR_OPERATION_ON_VOLATILE_OBJECT);
          
          processReturn ();
          
          return -1;
        }
      
      if ((NOT hbaseMapSupported) && (naTable && (*naTable)->isHbaseMapTable()))
        {
          // not supported
          *CmpCommon::diags() << DgSqlCode(-3242)
                              << DgString0("This feature not available for an HBase mapped table.");
          
          processReturn();
          
          return -1;
        }
    }

  extNameForHbase = genHBaseObjName
    (catalogNamePart, schemaNamePart, objectNamePart, 
     (naTable && *naTable ? (*naTable)->getNamespace() : NAString("")),
     objDataUID);

  hiatusObjectName_.clear();

  //for partition table(V2) ,no need to processHiatus
  //because no related hbase table exists
  if (processHiatus && naTable && (*naTable) && (*naTable)->incrBackupEnabled() &&
      NOT (*naTable)->isPartitionV2Table())
    {
      if (setHiatus(extNameForHbase, FALSE, createHiatusIfNotExist))
        {
          // setHiatus has set the diags area
          processReturn();
          
          return -1;
        }

      hiatusObjectName_ = extNameForHbase;
    }

  return 0;
}

static void resetHbaseSerialization(NABoolean hbaseSerialization,
                                    NAString &hbVal)
{
  if (hbaseSerialization)
    {
      ActiveSchemaDB()->getDefaults().validateAndInsert
        ("hbase_serialization", hbVal, FALSE);
    }
}

struct createTablePthreadArgs 
{
  ExpHbaseInterface *ehi; 
  HbaseStr *table;
  std::vector<NAString> *colFamVec;
  NAList<HbaseCreateOption*> * hbaseCreateOptions;
  int numSplits;
  int keyLength;
  char** encodedKeysBuffer;
  NABoolean doRetry;
  NABoolean ddlXns;
  NABoolean incrBackupEnabled;
  NABoolean createIfNotExists;
  NABoolean isMVCC;
  Int64 transID;
};

void *CmpSeabaseDDLcreateTable__pthreadCreate(void * _args)
{
  Int32 rc = 0;

  struct createTablePthreadArgs *args = 
    (struct createTablePthreadArgs *)_args;

  rc = CmpSeabaseDDL::createHbaseTable(args->ehi,
                                       args->table,
                                       *args->colFamVec,
                                       args->hbaseCreateOptions,
                                       args->numSplits,
                                       args->keyLength,
                                       args->encodedKeysBuffer,
                                       args->doRetry,
                                       args->ddlXns,
                                       args->incrBackupEnabled,
                                       args->createIfNotExists,
                                       args->isMVCC);
  
  pthread_exit (NULL);
}

struct dropTablePthreadArgs 
{
  ExpHbaseInterface *ehi; 
  char tabName[1000];
  NABoolean ddlXns;
  Int64 transID;
};

void *CmpSeabaseDDLdropTable__pthreadDrop(void * _args)
{
  Int32 rc = 0;

  struct dropTablePthreadArgs *args = (struct dropTablePthreadArgs *)_args;

  HbaseStr table;
  table.val = args->tabName;
  table.len = strlen(args->tabName);
  NAHeap *heap = GetCliGlobals()->currContext()->exHeap();

  ExpHbaseInterface * ehi = NULL;
  ehi = ExpHbaseInterface::newInstance
    (heap, NULL, NULL, COM_STORAGE_HBASE, FALSE);
  ehi->init(NULL);

  rc = CmpSeabaseDDL::dropHbaseTable(ehi,
                                     &table, //args->table,
                                     FALSE,
                                     args->ddlXns);

  delete ehi;

  pthread_exit (NULL);
}

// RETURN:  -1, no need to cleanup.  -2, caller need to call cleanup 
//          0, all ok, 1 table exists (if not exists specified).

short CmpSeabaseDDL::createSeabaseTable2(
                                         ExeCliInterface &cliInterface,
                                         StmtDDLCreateTable * createTableNode,
                                         NAString &currCatName, NAString &currSchName,
                                         NABoolean isCompound,
                                         Int64 &outObjUID,
                                         NABoolean &schemaStoredDesc,
                                         NABoolean &schemaIncBack)
{
  Lng32 retcode = 0;
  Lng32 cliRC = 0;

  ComObjectName tableName(createTableNode->getTableName());

  ComAnsiNamePart currCatAnsiName(currCatName);
  ComAnsiNamePart currSchAnsiName(currSchName);

  NABoolean isCreatingReadOnlyTable = FALSE;

  // external table to be mapped to an existing hbase table
  NABoolean hbaseMapFormat = FALSE;

  // format of data in hbase mapped table: native or string.
  // See ComRowFormat in common/ComSmallDefs.h for details.
  NABoolean hbaseMappedDataFormatIsString = FALSE;

  // Make some additional checks if creating an external hive table
  ComObjectName *srcTableName = NULL;
  if ((createTableNode->isExternal()) &&
      (NOT createTableNode->mapToHbaseTable()))
    {
      // The schema name of the target table, if specified,  must match the 
      // schema name of the source table
      NAString origSchemaName = 
        createTableNode->getOrigTableNameAsQualifiedName().getSchemaName();

      srcTableName = new(STMTHEAP) ComObjectName
          (createTableNode->getLikeSourceTableName(), COM_TABLE_NAME);

      srcTableName->applyDefaults(currCatAnsiName, currSchAnsiName);

      if (srcTableName->getCatalogNamePartAsAnsiString() == HBASE_SYSTEM_CATALOG)
        {
          *CmpCommon::diags()
            << DgSqlCode(-3242)
            << DgString0("Cannot create external table on a native HBase table without the MAP TO option.");
          
          return -1;
        }

      // Convert the native table name to its trafodion name
      NAString tabName = ComConvertNativeNameToTrafName 
        (srcTableName->getCatalogNamePartAsAnsiString(),
         srcTableName->getSchemaNamePartAsAnsiString(),
         tableName.getObjectNamePartAsAnsiString());
                               
      ComObjectName adjustedName(tabName, COM_TABLE_NAME);
      NAString type = "HIVE";
      tableName = adjustedName;

      // Verify that the name with prepending is not too long
      if (tableName.getSchemaNamePartAsAnsiString(TRUE).length() >
          ComMAX_ANSI_IDENTIFIER_INTERNAL_LEN)
        {
          *CmpCommon::diags()
            << DgSqlCode(-CAT_EXTERNAL_SCHEMA_NAME_TOO_LONG)
            << DgString0(type.data())
            << DgTableName(tableName.getSchemaNamePartAsAnsiString(FALSE))
            << DgInt0(ComMAX_ANSI_IDENTIFIER_INTERNAL_LEN - sizeof(HIVE_EXT_SCHEMA_PREFIX));
          return -1;
        }

      if ((origSchemaName.length() > 0)&&
          (origSchemaName != srcTableName->getSchemaNamePart().getExternalName()))
      {
        *CmpCommon::diags()
          << DgSqlCode(-CAT_EXTERNAL_NAME_MISMATCH)
          << DgString0 (type.data())
          << DgTableName(origSchemaName)
          << DgString1((srcTableName->getSchemaNamePart().getExternalName()));
        return -1;
      }
              
      // For now the object name of the target table must match the
      // object name of the source table
      if (tableName.getObjectNamePart().getExternalName() !=
          srcTableName->getObjectNamePart().getExternalName())
        {
          *CmpCommon::diags()
            << DgSqlCode(-CAT_EXTERNAL_NAME_MISMATCH)
            << DgString0 (type.data())
            << DgTableName(tableName.getObjectNamePart().getExternalName())
            << DgString1((srcTableName->getObjectNamePart().getExternalName()));
          return -1;
        }
    } // external hive table

  ParDDLFileAttrsCreateTable &fileAttribs =
    createTableNode->getFileAttributes();

  NAString objectNamePart = tableName.getObjectNamePartAsAnsiString(TRUE);

  if (fileAttribs.isReadOnlySpecified() && fileAttribs.readOnlyEnabled())
    isCreatingReadOnlyTable = TRUE;

  if (isCreatingReadOnlyTable && CmpCommon::getDefault(TRAF_ENABLE_DATA_LOAD_IN_SHARED_CACHE) == DF_OFF) {
    *CmpCommon::diags() << DgSqlCode(-3242)
                        << DgString0("The data caching function is disabled");
    return -1;
  }

  // Make some additional checks if creating an external hbase mapped table
  NAString mappedHbaseNameSpace;
  NAString tableNameSpace;
  NABoolean checkForValidHBaseName = TRUE;
  if ((createTableNode->isExternal()) &&
      (createTableNode->mapToHbaseTable()))
    {
      if (CmpCommon::getDefault(TRAF_HBASE_MAPPED_TABLES) == DF_OFF)
        {
          *CmpCommon::diags() << DgSqlCode(-3242)
                              << DgString0("HBase mapped tables not supported.");
          
          return -1;
        }

      if (isCreatingReadOnlyTable) {
        *CmpCommon::diags() << DgSqlCode(-3242)
                            << DgString0("HBase mapped tables can not build as a read only table");
          
        return -1;
      }
      
      srcTableName = new(STMTHEAP) ComObjectName
          (createTableNode->getLikeSourceTableName(), COM_TABLE_NAME);

      ComAnsiNamePart hbCat(HBASE_SYSTEM_CATALOG);
      ComAnsiNamePart hbSch(HBASE_SYSTEM_SCHEMA);
      srcTableName->applyDefaults(hbCat, hbSch);
      
      hbaseMapFormat = TRUE;
      hbaseMappedDataFormatIsString = 
        createTableNode->isHbaseDataFormatString();

      ComAnsiNamePart trafCat(TRAFODION_SYSCAT_LIT);
      ComAnsiNamePart trafSch(NAString(HBASE_EXT_MAP_SCHEMA), 
                              ComAnsiNamePart::INTERNAL_FORMAT);

      tableName.setCatalogNamePart(trafCat);
      tableName.setSchemaNamePart(trafSch);

      // For now the object name of the target table must match the
      // object name of the source table
      
      char * tableIntName = (char*)tableName.getObjectNamePart().getInternalName().data();
      char * tableNameColonPos = strchr(tableIntName, ':');

      char * srcIntName = (char*)srcTableName->getObjectNamePart().getInternalName().data();
      char * srcTableNameColonPos = strchr(srcIntName, ':');

      NAString srcNamePart(srcIntName);
      NAString srcNamePartWithoutNamespace = srcNamePart;

      if (srcTableNameColonPos)
        {
          mappedHbaseNameSpace = NAString(srcIntName, (srcTableNameColonPos-srcIntName));
          
          srcNamePartWithoutNamespace = NAString(&srcIntName[srcTableNameColonPos - srcIntName + 1]);
        }

      if (tableNameColonPos)
        {
          tableNameSpace = NAString(tableIntName, (tableNameColonPos-tableIntName));
        }

      if (NOT tableNameSpace.isNull())
        {
          if ((mappedHbaseNameSpace.isNull()) ||
              (mappedHbaseNameSpace != tableNameSpace))
            {
              *CmpCommon::diags() << DgSqlCode(-3242)
                                  << DgString0("Namespace specified as part of external tablename must be same as the namespace specified in HBase tablename.");
              
              processReturn();
              return -1;
            }
        }

      NAString fileAttrNameSpace;
      if (fileAttribs.isNamespaceSpecified() == TRUE)
        {
          fileAttrNameSpace = fileAttribs.getNamespace();

          // remove TRAF_ prefix to get the hbase namespace
          if (fileAttrNameSpace.index(TRAF_NAMESPACE_PREFIX) == 0)
            fileAttrNameSpace = fileAttrNameSpace.remove(0, TRAF_NAMESPACE_PREFIX_LEN);
          
          if ((mappedHbaseNameSpace.isNull()) ||
              (fileAttrNameSpace != mappedHbaseNameSpace))
            {
              *CmpCommon::diags() << DgSqlCode(-3242)
                                  << DgString0("Namespace specified in the 'namespace' attribute must be the same as the namespace specified in HBase tablename.");
              
              processReturn();
              return -1;
            }
        }      

      checkForValidHBaseName = FALSE;
      if ((tableNameColonPos && (objectNamePart != srcNamePart)) ||
          (!tableNameColonPos && (objectNamePart != srcNamePartWithoutNamespace)))
        {
          *CmpCommon::diags()
            << DgSqlCode(-CAT_EXTERNAL_NAME_MISMATCH)
            << DgString0 ("HBASE")
            << DgTableName(tableName.getObjectNamePart().getExternalName())
            << DgString1(srcNamePart.data());
          return -1;
        }

    } // external hbase mapped table

  const NAString catalogNamePart = tableName.getCatalogNamePartAsAnsiString();
  const NAString schemaNamePart = tableName.getSchemaNamePartAsAnsiString(TRUE);
  const NAString extTableName = tableName.getExternalName(TRUE);

  const ComStorageType storageType = fileAttribs.storageType();
 
  NABoolean isMonarch = (storageType == COM_STORAGE_MONARCH);
  ExpHbaseInterface * ehi = allocEHI(storageType);
  if (ehi == NULL)
    {
      processReturn();
      return -1;
    }

  if ((isSeabaseReservedSchema(tableName)) &&
      (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)))
    {
      *CmpCommon::diags() << DgSqlCode(-CAT_CREATE_TABLE_NOT_ALLOWED_IN_SMD)
                          << DgTableName(extTableName);
      deallocEHI(ehi); 

      processReturn();

      return -1;
    }

  retcode = lockObjectDDL(createTableNode);
  if (retcode < 0) {
    deallocEHI(ehi);
    processReturn();
    return -1;
  }

  NABoolean useSwitchCompiler = (CmpCommon::getDefault(TRAF_SWITCH_COMPILER) == DF_ON);

  if (useSwitchCompiler &&
      switchCompiler(CmpContextInfo::CMPCONTEXT_TYPE_META)) {
    deallocEHI(ehi);
    processReturn();
    return -1;
  }

  retcode = existsInSeabaseMDTable(&cliInterface, 
                                   catalogNamePart, schemaNamePart, objectNamePart,
                                   COM_BASE_TABLE_OBJECT, FALSE, 
                                   checkForValidHBaseName);
  if (useSwitchCompiler) {
    switchBackCompiler();
  }
  if (retcode < 0)
    {
      deallocEHI(ehi); 

      processReturn();

      return -1;
    }

  if (retcode == 1) // already exists
    {
      if (NOT createTableNode->createIfNotExists())
        {
          if (createTableNode->isVolatile())
            *CmpCommon::diags() << DgSqlCode(-1390)
                                << DgString0(objectNamePart);
          else
            *CmpCommon::diags() << DgSqlCode(-1390)
                                << DgString0(extTableName);
        }

      deallocEHI(ehi); 

      processReturn();

      return 1;
    }

  if (! Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL) &&
       (! createTableNode->isVolatile()) &&
       (! createTableNode->isExternal()))
    {
      retcode = ddlSetObjectEpoch(createTableNode,
                                  STMTHEAP,
                                  false, /* allowReads */
                                  false /* isContinue */);
      if (retcode < 0)
        {
          deallocEHI(ehi);
          processReturn();
          return -1;
        }
    }

  if (createTableNode->isVolatile() && isCreatingReadOnlyTable) {
    *CmpCommon::diags() << DgSqlCode(-3242)
                        << DgString0("Volatile tables can not build as a read only table");
    return -1;
  }

  // If creating an external table, go perform operation
  if (createTableNode->isExternal())
    {
      if (isCreatingReadOnlyTable) {
        *CmpCommon::diags() << DgSqlCode(-3242)
                            << DgString0("External tables can not build as a read only table");
          
        return -1;
      }
      retcode = createSeabaseTableExternal
        (cliInterface, createTableNode, tableName, *srcTableName);
      if (retcode != 0 && CmpCommon::diags()->getNumber(DgSqlCode::ERROR_) == 0)
        SEABASEDDL_INTERNAL_ERROR("creating external HIVE table");

      if (NOT hbaseMapFormat)
        {
          deallocEHI(ehi);
          processReturn();
          return retcode;
        }
    }

  // make sure the table to be mapped exists
  if (hbaseMapFormat)
    {
      HbaseStr hbaseTable;
      NAString objNameWithNamespace;
      if (NOT mappedHbaseNameSpace.isNull())
        objNameWithNamespace = mappedHbaseNameSpace + ":";
      objNameWithNamespace += objectNamePart;

      hbaseTable.val = (char*)objNameWithNamespace.data();
      hbaseTable.len = objNameWithNamespace.length();
      if (ehi->exists(hbaseTable) == 0) // does not exist in hbase
        {
          *CmpCommon::diags() << DgSqlCode(-4260)
                              << DgString0(objNameWithNamespace);
          
          deallocEHI(ehi); 
          processReturn();
          
          return -1;
        }
    }

  ElemDDLColDefArray &colArray = createTableNode->getColDefArray();
  ElemDDLColRefArray &keyArray =
    (createTableNode->getIsConstraintPKSpecified() ?
     createTableNode->getPrimaryKeyColRefArray() :
     (createTableNode->getStoreOption() == COM_KEY_COLUMN_LIST_STORE_OPTION ?
      createTableNode->getKeyColumnArray() :
      createTableNode->getPrimaryKeyColRefArray()));

  if ((NOT ((createTableNode->isExternal()) && 
            (createTableNode->mapToHbaseTable()))) &&
      ((createTableNode->getIsConstraintPKSpecified()) &&
       (createTableNode->getAddConstraintPK()) &&
       (createTableNode->getAddConstraintPK()->getAlterTableAction()->castToElemDDLConstraintPK()->notSerialized())))
    {
      *CmpCommon::diags() << DgSqlCode(-3242)
                          << DgString0("NOT SERIALIZED option cannot be specified for primary key of this table.");
      
      return -1;
    }

  Int32 objectOwnerID = SUPER_USER;
  Int32 schemaOwnerID = SUPER_USER;
  Int64 schemaUID = -1;
  Int64 schemaObjectFlags = 0;
  ComSchemaClass schemaClass;

  retcode = verifyDDLCreateOperationAuthorized(&cliInterface,
                                               SQLOperation::CREATE_TABLE,
                                               catalogNamePart, 
                                               schemaNamePart,
                                               schemaClass,
                                               objectOwnerID,
                                               schemaOwnerID,
                                               &schemaUID,
                                               &schemaObjectFlags);
  if (retcode != 0)
  {
     handleDDLCreateAuthorizationError(retcode,catalogNamePart,schemaNamePart);
     deallocEHI(ehi); 
     processReturn();
     return -1;
  }

  // If the schema name specified is external HIVE or HBase name, users cannot 
  // create them.
  if (ComIsTrafodionExternalSchemaName(schemaNamePart) &&
      (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)) &&
      (NOT hbaseMapFormat))
    {
      // error.
      *SqlParser_Diags << DgSqlCode(-CAT_CREATE_TABLE_NOT_ALLOWED_IN_SMD)
                       << DgTableName(extTableName.data());
      return -1;
    }

  // get namespace of this schema, if it was created in a non-default ns.
  NAString schemaNamespace;
  if (getTextFromMD(&cliInterface, schemaUID, COM_OBJECT_NAMESPACE, 0,
                    schemaNamespace))
    {
      processReturn();
      return -1;
    }

  NABoolean schemaEncryptRowid = FALSE;
  NABoolean schemaEncryptData  = FALSE;
  if (isMDflagsSet(schemaObjectFlags, MD_OBJECTS_ENCRYPT_ROWID_FLG))
    schemaEncryptRowid = TRUE;

  if (isMDflagsSet(schemaObjectFlags, MD_OBJECTS_ENCRYPT_DATA_FLG))
    schemaEncryptData = TRUE;
  
  if (createTableNode->getIsLikeOptionSpecified())
    {
      createSeabaseTableLike(&cliInterface,
                             createTableNode, currCatName, currSchName);
      deallocEHI(ehi);
      processReturn();
      return -1;
    }

  schemaStoredDesc = FALSE;
  if (isMDflagsSet(schemaObjectFlags, MD_OBJECTS_STORED_DESC))
    schemaStoredDesc = TRUE;

  NABoolean schemaIncrBackupEnabled = FALSE;
  if (isMDflagsSet(schemaObjectFlags, MD_OBJECTS_INCR_BACKUP_ENABLED))
  {
    schemaIncrBackupEnabled = TRUE;
    schemaIncBack = TRUE;
  }   
  // For shared schemas, histogram tables should be owned by the schema owner, 
  // not the first user to run UPDATE STATISTICS in the schema.  
  if (schemaClass == COM_SCHEMA_CLASS_SHARED && isHistogramTable(objectNamePart))
    objectOwnerID = schemaOwnerID;

  // check if SYSKEY is specified as a column name.
  for (Lng32 i = 0; i < colArray.entries(); i++)
    {
      if ((CmpCommon::getDefault(TRAF_ALLOW_RESERVED_COLNAMES) == DF_OFF) &&
          (ComTrafReservedColName(colArray[i]->getColumnName())))
        {
          *CmpCommon::diags() << DgSqlCode(-CAT_RESERVED_COLUMN_NAME)
                              << DgString0(colArray[i]->getColumnName());
          
          deallocEHI(ehi);
          processReturn();
          return -1;
        }
    }

  NABoolean implicitPK = FALSE;

  NAString syskeyColName("SYSKEY");
  SQLLargeInt * syskeyType = new(STMTHEAP) SQLLargeInt(STMTHEAP, TRUE, FALSE);
  ElemDDLColDef syskeyColDef(NULL, &syskeyColName, syskeyType, NULL,
                             STMTHEAP);
  ElemDDLColRef edcr("SYSKEY", COM_ASCENDING_ORDER);
  syskeyColDef.setColumnClass(COM_SYSTEM_COLUMN);

  NAString hbRowIdColName("ROW_ID");
  SQLVarChar * hbRowIdType = new(STMTHEAP) SQLVarChar(STMTHEAP, 1000, FALSE);
  ElemDDLColDef hbRowIdColDef(NULL, &hbRowIdColName, hbRowIdType, NULL,
                              STMTHEAP);
  ElemDDLColRef hbcr("ROW_ID", COM_ASCENDING_ORDER);
  hbRowIdColDef.setColumnClass(COM_SYSTEM_COLUMN);

  CollIndex numReplicaAndSaltCols = 0;
  CollIndex numDivCols = 0;
  NAString splitByClause;

  if (((createTableNode->getStoreOption() == COM_KEY_COLUMN_LIST_STORE_OPTION) &&
       (NOT createTableNode->getIsConstraintPKSpecified())) ||
      (keyArray.entries() == 0))
    {
      if (hbaseMapFormat)
        {
          // for now, return error if pkey is not specified.
          *CmpCommon::diags() << DgSqlCode(-4259);
          deallocEHI(ehi); 
          processReturn();
          return -1;
        }

      NABoolean syskeySpecifiedInStoreByClause = FALSE;
      for (int i = 0; ((NOT syskeySpecifiedInStoreByClause) &&
                       (i < keyArray.entries())); i++)
        {
          if (keyArray[i]->getColumnName() == syskeyColName)
            syskeySpecifiedInStoreByClause = TRUE;
        }
      
      if (NOT syskeySpecifiedInStoreByClause)
        {
          keyArray.insert(&edcr);
        }

      colArray.insertAt(0, &syskeyColDef);
      implicitPK = TRUE;
    }

  int numSaltPartns = 0; // # of "_SALT_" values
  int numSaltSplits = 0; // # of initial region splits for salted table
  int numInitialSaltRegions = -1; // # of regions in SALT clause or -1
  int numTrafReplicas = 0;
  int numSplits = 0;     // # of initial region splits, SALT/REPLICA or SPLIT BY

  Lng32 numSaltPartnsFromCQD = 
    CmpCommon::getDefaultNumeric(TRAF_NUM_OF_SALT_PARTNS);

   if (createTableNode->getReplicateClause())
   {
     numTrafReplicas = createTableNode->getReplicateClause()->getNumTrafReplicas();
     if (createTableNode->isSplitBySpecified() || hbaseMapFormat)
       {
         if (hbaseMapFormat)
           *CmpCommon::diags() << DgSqlCode(-1207) << DgString0(extTableName) << DgString1("MAP");
         else
           // replicate option not supported on split by table
           *CmpCommon::diags() << DgSqlCode(-1207) << DgString0(extTableName) << DgString1("SPLIT BY");
          deallocEHI(ehi); 
          processReturn();
          return -1;
       }
   }
  
  if ((createTableNode->getSaltOptions()) ||
      ((numSaltPartnsFromCQD > 0) &&
       (NOT implicitPK) &&
       (NOT createTableNode->isSplitBySpecified())))
    {
      if (hbaseMapFormat)
        {
          // salt option not supported on hbase map table
          *CmpCommon::diags() << DgSqlCode(-4259);
          deallocEHI(ehi); 
          processReturn();
          return -1;
        }

      if (isCreatingReadOnlyTable) {
        *CmpCommon::diags() << DgSqlCode(-3242)
                            << DgString0("Partition tables can not build as a read only table");
        return -1;
      }

      // add a system column SALT INTEGER NOT NULL with a computed
      // default value HASH2PARTFUNC(<salting cols> FOR <num salt partitions>)
      ElemDDLSaltOptionsClause * saltOptions = createTableNode->getSaltOptions();
      ElemDDLColRefArray *saltArray = createTableNode->getSaltColRefArray();
      NAString saltExprText("HASH2PARTFUNC(");
      NABoolean firstSaltCol = TRUE;
      char numSaltPartnsStr[20];
      
      if (saltArray == NULL || saltArray->entries() == 0)
        {
          // if no salting columns are specified, use all key columns
          saltArray = &keyArray;
        }
      else
        {
          // Validate that salting columns refer to real key columns
          for (CollIndex s=0; s<saltArray->entries(); s++)
            if (keyArray.getColumnIndex((*saltArray)[s]->getColumnName()) < 0)
              {
                *CmpCommon::diags() << DgSqlCode(-1195) 
                                    << DgColumnName((*saltArray)[s]->getColumnName());
                deallocEHI(ehi); 
                processReturn();
                return -1;
              }
        }

      for (CollIndex i=0; i<saltArray->entries(); i++)
        {
          const NAString &colName = (*saltArray)[i]->getColumnName();
          ComAnsiNamePart cnp(colName, ComAnsiNamePart::INTERNAL_FORMAT);
          Lng32      colIx    = colArray.getColumnIndex(colName);
          if (colIx < 0)
            {
              *CmpCommon::diags() << DgSqlCode(-1009)
                                  << DgColumnName(colName);
              
              deallocEHI(ehi); 
              processReturn();
              return -1;
            }

          NAType         *colType = colArray[colIx]->getColumnDataType();
          NAString       typeText;
          short          rc       = colType->getMyTypeAsText(&typeText, FALSE);

          // don't include SYSKEY in the list of salt columns
          if (colName != "SYSKEY")
            {
              if (firstSaltCol)
                firstSaltCol = FALSE;
              else
                saltExprText += ",";
              saltExprText += "CAST(";
              if (NOT cnp.isDelimitedIdentifier())
                saltExprText += "\"";
              saltExprText += cnp.getExternalName();
              if (NOT cnp.isDelimitedIdentifier())
                saltExprText += "\"";
              saltExprText += " AS ";
              saltExprText += typeText;
              if (!colType->supportsSQLnull())
                saltExprText += " NOT NULL";
              saltExprText += ")";
              if (colType->getTypeQualifier() == NA_NUMERIC_TYPE &&
                  !(((NumericType *) colType)->isExact()))
                {
                  *CmpCommon::diags() << DgSqlCode(-1120);
                  deallocEHI(ehi); 
                  processReturn();
                  return -1;
                }
            }
          else if (saltArray != &keyArray || saltArray->entries() == 1)
            {
              // SYSKEY was explicitly specified in salt column or is the only column,
              // this is an error
              *CmpCommon::diags() << DgSqlCode(-1195) 
                                  << DgColumnName((*saltArray)[i]->getColumnName());
              deallocEHI(ehi); 
              processReturn();
              return -1;
            }

          // For now, disallow an UPSHIFT column as a SALT column, as there are
          // bugs in the compiler concerning how SALT is computed for such columns.
          // See Mantis 10189.
          if (colType && (colType->getTypeQualifier() == NA_CHARACTER_TYPE))
            {
              CharType * charColType = (CharType *)colType;
              if (charColType->isUpshifted() && (CmpCommon::getDefault(SALT_ON_UPSHIFT_ALLOWED) == DF_OFF))
                {
                  *CmpCommon::diags() << DgSqlCode(-CAT_UPSHIFT_PLUS_SALT_NOT_SUPPORTED); 
                  deallocEHI(ehi); 
                  processReturn();
                  return -1;
                }
            }

        }

      if (saltOptions)
        {
          // SALT clause takes precedence over CQD
          numSaltPartns = saltOptions->getNumPartitions();
          numInitialSaltRegions = saltOptions->getNumInitialRegions();
        }
      else
        {
          // CQD is another way to salt all tables by default
          numSaltPartns = numSaltPartnsFromCQD;
          numInitialSaltRegions =
            CmpCommon::getDefaultNumeric(TRAF_NUM_OF_SALT_REGIONS);
        }

      if (numInitialSaltRegions <= 0 || numInitialSaltRegions > numSaltPartns)
        numInitialSaltRegions = numSaltPartns;
      numSaltSplits = numInitialSaltRegions - 1;

      saltExprText += " FOR ";
      sprintf(numSaltPartnsStr,"%d", numSaltPartns);
      saltExprText += numSaltPartnsStr;
      saltExprText += ")";
      if (numSaltPartns <= 1 || numSaltPartns > 1024)
        {
          // number of salt partitions is out of bounds
          *CmpCommon::diags() << DgSqlCode(-CAT_INVALID_NUM_OF_SALT_PARTNS) 
                              << DgInt0(2)
                              << DgInt1(1024);
          deallocEHI(ehi); 
          processReturn();
          return -1;
        }
      if ((numInitialSaltRegions != numSaltPartns) && 
          createTableNode->getReplicateClause())
        {
          // replicate option not supported on table with initial salt regions less than final
           *CmpCommon::diags() << DgSqlCode(-1207) << DgString0(extTableName) 
                               << DgString1("SALT USING <n1> PARTITIONS IN <n2> REGIONS");
          deallocEHI(ehi); 
          processReturn();
          return -1;
        }

      NAString saltColName(ElemDDLSaltOptionsClause::getSaltSysColName());
      SQLInt * saltType = new(STMTHEAP) SQLInt(STMTHEAP, FALSE, FALSE);
      ElemDDLColDefault *saltDef = 
        new(STMTHEAP) ElemDDLColDefault(
             ElemDDLColDefault::COL_COMPUTED_DEFAULT);
      saltDef->setComputedDefaultExpr(saltExprText);
      ElemDDLColDef * saltColDef =
        new(STMTHEAP) ElemDDLColDef(NULL, &saltColName, saltType, saltDef,
                                    STMTHEAP);

      ElemDDLColRef * edcrs = 
        new(STMTHEAP) ElemDDLColRef(saltColName, COM_ASCENDING_ORDER);

      saltColDef->setColumnClass(COM_SYSTEM_COLUMN);

      // add this new salt column at the end
      // and also as key column 0. salt will be key column 1 later, if there is a replica col.
      colArray.insert(saltColDef);
      keyArray.insertAt(0, edcrs);
      numReplicaAndSaltCols++;

      if (createTableNode->isSplitBySpecified())
        {
          // salt and split by are not allowed together - for now
          *CmpCommon::diags() << DgSqlCode(-1206);
          deallocEHI(ehi); 
          processReturn();
          return -1;
        }
    }

     if (numTrafReplicas > 0)
     {
       char replicaStr[8]; 
       snprintf(replicaStr,sizeof(replicaStr),"%d", numTrafReplicas);
       NAString replicaExprText(replicaStr);
       NAString replicaColName(ElemDDLReplicateClause::getReplicaSysColName());
       SQLSmall * replicaType = new(STMTHEAP) SQLSmall(STMTHEAP, FALSE, FALSE);
       ItemExpr * replicaItem = new(STMTHEAP) ConstValue(numTrafReplicas);
       ElemDDLColDefault *replicaDef = 
         new(STMTHEAP) ElemDDLColDefault(ElemDDLColDefault::COL_DEFAULT,replicaItem);
       replicaDef->setDefaultExprString(replicaExprText);
       ElemDDLColDef * replicaColDef =
         new(STMTHEAP) ElemDDLColDef(NULL, &replicaColName, replicaType, 
                                     replicaDef, STMTHEAP);

       ElemDDLColRef * edcrr = 
         new(STMTHEAP) ElemDDLColRef(replicaColName, COM_ASCENDING_ORDER);

      replicaColDef->setColumnClass(COM_SYSTEM_COLUMN);

      // add this new replica column at the end
      // and also as key column 0.
      colArray.insert(replicaColDef);
      keyArray.insertAt(0, edcrr);
      numReplicaAndSaltCols++;
     }
  
  // is hbase data stored in varchar format
  if (hbaseMapFormat && hbaseMappedDataFormatIsString)
    {
      // cannot specify serialized primary key

      if (createTableNode->getAddConstraintPK()->getAlterTableAction()->
          castToElemDDLConstraintPK()->serialized())
        {
          *CmpCommon::diags() << DgSqlCode(-3242)
                              << DgString0("SERIALIZED option cannot be specified for primary key of this table.");
          
          return -1;
        }

      // must have only one varchar primary key col
      if (keyArray.entries() > 1)
        {
          *CmpCommon::diags() << DgSqlCode(-3242)
                              << DgString0("Only one column can be specified as the primary key of this table.");
          
          return -1;
        }

      Lng32 tableColNum = 
        (Lng32)colArray.getColumnIndex(keyArray[0]->getColumnName());
      if (tableColNum < 0)
        {
          *CmpCommon::diags() << DgSqlCode(-3242)
                              << DgString0("Specified primary key column does not exist.");
          
          return -1;
        }

      NAType *colType = colArray[tableColNum]->getColumnDataType();
      if (NOT DFS2REC::isAnyVarChar(colType->getFSDatatype()))
        {
          *CmpCommon::diags() << DgSqlCode(-3242)
                              << DgString0("Primary key column must be specified as varchar datatype for this table.");
          
          return -1;
        }
    }

  if (createTableNode->isSplitBySpecified())
    splitByClause = createTableNode->getSplitByClause();

  // create table in seabase

  NABoolean alignedFormat = FALSE;
  if (fileAttribs.isRowFormatSpecified() == TRUE)
    {
      if (fileAttribs.getRowFormat() == ElemDDLFileAttrRowFormat::eALIGNED)
        {
          alignedFormat = TRUE;
        }
    }
  else if(CmpCommon::getDefault(TRAF_ALIGNED_ROW_FORMAT) == DF_ON)
    {
      if ( NOT isSeabaseReservedSchema(tableName))
        {
          // aligned format default does not apply to hbase map tables
          if (NOT hbaseMapFormat)
            alignedFormat = TRUE;
        }
    }

  if (!alignedFormat && isCreatingReadOnlyTable) {
    *CmpCommon::diags() << DgSqlCode(-3242)
                        << DgString0("Hbase format tables can not build as a read only table");
    deallocEHI(ehi);
    processReturn();
    return -1;
  }

  if (hbaseMapFormat && alignedFormat)
    {
      // not supported
      *CmpCommon::diags() << DgSqlCode(-3242)
                          << DgString0("Aligned format cannot be specified for an HBase mapped table.");

      deallocEHI(ehi); 
      processReturn();
      
      return -1;
    }

  // if table is being created with an explicit 'namespace', use that.
  // otherwise if schema was created with an explicit 'namespace', use that.
  // otherwise if cqd is set, use that.
  NAString tableNamespace;
  if ((fileAttribs.isNamespaceSpecified() == TRUE) &&
      ((createTableNode->isVolatile()) ||
       (isMonarch)))
    {
      // cannot specify namespace for volatile or monarch tables
      *CmpCommon::diags() << DgSqlCode(-1063);
      deallocEHI(ehi);
      processReturn();
      return -1;
    }
  
  if (createTableNode->mapToHbaseTable())
    {
      if (NOT mappedHbaseNameSpace.isNull())
        {
          tableNamespace = mappedHbaseNameSpace;
        }
    }
  else if (fileAttribs.isNamespaceSpecified() == TRUE)
    {
      tableNamespace = fileAttribs.getNamespace();
    }
  else if (NOT schemaNamespace.isNull())
    {
      tableNamespace = schemaNamespace;
    }
  else if ((NOT ComIsTrafodionReservedSchemaName(schemaNamePart)) &&
           (NOT isHistogramTable(objectNamePart)))
    {
      NAString ns = CmpCommon::getDefaultString(TRAF_OBJECT_NAMESPACE);
      if ((NOT ns.isNull()) &&
          (NOT ComValidateNamespace(&ns, TRUE, tableNamespace)))
        {
          *CmpCommon::diags() << DgSqlCode(-1065);
          deallocEHI(ehi);
          processReturn();
          return -1;
        }
    } else if (ComIsTrafodionReservedSchemaName(schemaNamePart) &&
               isHistogramTable(objectNamePart))
    {
       tableNamespace=ComGetReservedNamespace(schemaNamePart);
    }
    // SEABASE_MD_SCHEMA has no namespace info in TEXT, so we need specify it 
    else if (ComIsTrafodionReservedSchemaName(schemaNamePart) &&
             ComIsPartitionMDTable(objectNamePart))
    {
      tableNamespace = TRAF_RESERVED_NAMESPACE1;
    }

  if ((NOT tableNamespace.isNull()) &&
      (isMonarch))
    {
      // monarch tables cannot be created in a namespace.
      tableNamespace.clear();
    }

  if ((NOT tableNamespace.isNull()) &&
      (tableNamespace.index(TRAF_RESERVED_NAMESPACE_PREFIX) == 0) &&
      (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)) &&
      (NOT ((schemaNamePart == SEABASE_SYSTEM_SCHEMA) ||
            (createTableNode->isVolatile()))))
    {
      // cannot create in metadata namespace
      *CmpCommon::diags() << DgSqlCode(-1072)
                          << DgString0(tableNamespace.data());
      return -1;
    }

   // Cannot create table in another tenant's space, unless you have the  privilege
   bool enabled = msg_license_multitenancy_enabled();
   if (enabled)
     {
       if ((schemaNamespace != tableNamespace) && !isSeabaseReservedSchema(tableName))

         {
           short tenantSchema = isTenantSchema(&cliInterface, schemaUID);
           if (tenantSchema == -1)
             return -1;

           if (isAuthorizationEnabled() && 
               tenantSchema && 
               !ComUser::isRootUserID() && 
               !createTableNode->mapToHbaseTable())
             {
               NAString privMgrMDLoc;
               CONCAT_CATSCH(privMgrMDLoc,getSystemCatalog(),SEABASE_PRIVMGR_SCHEMA);
               PrivMgrComponentPrivileges compPrivs(std::string(privMgrMDLoc.data()),CmpCommon::diags());
               if (!compPrivs.hasSQLPriv(ComUser::getCurrentUser(),
                                         SQLOperation::MANAGE_TENANTS,
                                         true))
                 {
                   *CmpCommon::diags() << DgSqlCode(-CAT_WRONG_NAMESPACE)
                                       << DgString0(tableNamespace.data())
                                       << DgString1(schemaNamePart.data())
                                       << DgString2(schemaNamespace.data());
                   return -1;
                 }
             }
         }
     }

  // check if specified namespace exists
  if (NOT tableNamespace.isNull())
    {
      retcode = ehi->namespaceOperation(COM_CHECK_NAMESPACE_EXISTS,
                                        tableNamespace.data(),
                                        0, NULL, NULL,
                                        NULL);
      if (retcode == -HBC_ERROR_NAMESPACE_NOT_EXIST)
        {
          *CmpCommon::diags() << DgSqlCode(-1067)
                              << DgString0(tableNamespace.data());
          deallocEHI(ehi);
          processReturn();
          return -1;
        }
    }

  const NAString &defaultColFam = fileAttribs.getColFam();

  const ComReplType xnRepl = fileAttribs.xnRepl();
  if (xnRepl == COM_REPL_ASYNC)
    {
      *CmpCommon::diags()
        << DgSqlCode(-1425)
        << DgTableName(extTableName)
        << DgString0("Reason: Asynchronous replication not yet supported.");
      
      processReturn();
      
      return -1;
    }
  
  NABoolean encryptSpec = fileAttribs.isEncryptionSpecified();
  NAString *encryptionOptions = fileAttribs.getEncryptionOptions();
  NABoolean rowIdEncrypt = FALSE;
  NABoolean dataEncrypt  = FALSE;

  // encryption cannot be specified for non-aligned or hbase mapped tables
  if ((encryptSpec && encryptionOptions) &&
      ((NOT alignedFormat) || (hbaseMapFormat)))
    {
      *CmpCommon::diags() << DgSqlCode(-3242)
                          << DgString0("Encryption cannot be specified for non-aligned format or HBase mapped tables.");

      deallocEHI(ehi); 
      processReturn();
      return -1;
    }

  // if table is being created with an explicit 'no encrypt' clause,
  // then do not encrypt even if schema is created with encrypt clause.
  // if table is being created with an explicit 'encrypt', use that.
  // otherwise if schema was created with an explicit 'encrypt', use that.
  if (encryptSpec && (encryptionOptions == NULL)) // explicit 'no encrypt'
    {
      encryptSpec = FALSE;
    }
  else if (encryptSpec)
    {
      rowIdEncrypt = fileAttribs.encryptRowid();
      dataEncrypt  = fileAttribs.encryptData();
    }
  else if (schemaEncryptRowid || schemaEncryptData)
    {
      rowIdEncrypt = schemaEncryptRowid;

      dataEncrypt = schemaEncryptData;

      encryptSpec = TRUE;
    }
  else if ((NOT ComIsTrafodionReservedSchemaName(schemaNamePart)) &&
           (NOT isHistogramTable(objectNamePart)))
    {
       NAString enc = CmpCommon::getDefaultString(TRAF_OBJECT_ENCRYPTION);
       if (NOT enc.isNull())
         {
           if (NOT ComEncryption::validateEncryptionOptions(&enc, rowIdEncrypt, dataEncrypt))
             {
               *CmpCommon::diags() << DgSqlCode(-3066)
                                   << DgString0("Invalid encryption options specified in default TRAF_OBJECT_ENCRYPTION. Valid options must be 'r', 'd' or a combination of them.");
               
               deallocEHI(ehi);
               processReturn();
               
               return -1;
             }
           
           encryptSpec = TRUE;
         }
    }

  if ((NOT alignedFormat) || (hbaseMapFormat) || (isMonarch))
    {
      rowIdEncrypt = FALSE;
      dataEncrypt = FALSE;
      encryptSpec = FALSE;
    }

  if (rowIdEncrypt)
    {
      *CmpCommon::diags() << DgSqlCode(-3242)
                          << DgString0("Order preserving encryption of rowid(primary key) not yet available.");
      
      deallocEHI(ehi); 
      processReturn();
      
      return -1;
    }
  NABoolean incrBackupEnabledByUser = fileAttribs.isIncrBackupSpecified() ? fileAttribs.incrBackupEnabled() : schemaIncrBackupEnabled;
  NABoolean incrBackupEnabled = 
    ((incrBackupEnabledByUser ||
      (CmpCommon::getDefault(TRAF_INCREMENTAL_BACKUP) == DF_ON)) &&
     (((NOT ComIsTrafodionReservedSchemaName(schemaNamePart)) ||
        schemaNamePart == SEABASE_XDC_MD_SCHEMA) &&
      (NOT isHistogramTable(objectNamePart))));
 
  if ((createTableNode->isVolatile()) &&
      (incrBackupEnabled))
    {
      if (fileAttribs.incrBackupEnabled())
        {
          *CmpCommon::diags()
            << DgSqlCode(-3242)
            << DgString0("Cannot specify 'incremental backup' for volatile tables.");
          return -1;
        }

      incrBackupEnabled = FALSE;
    }

  // allow nullable clustering key or unique constraints based on the
  // CQD settings. If a volatile table is being created and cqd
  // VOLATILE_TABLE_FIND_SUITABLE_KEY is ON, then allow it.
  // If ALLOW_NULLABLE_UNIQUE_KEY_CONSTRAINT is set, then allow it.
  NABoolean allowNullableUniqueConstr = FALSE;
  if ((CmpCommon::getDefault(VOLATILE_TABLE_FIND_SUITABLE_KEY) != DF_OFF) &&
      (createTableNode->isVolatile()))
    allowNullableUniqueConstr = TRUE;
  
  if ((createTableNode->getIsConstraintPKSpecified()) &&
      (createTableNode->getAddConstraintPK()->getAlterTableAction()->castToElemDDLConstraintPK()->isNullableSpecified()))
    allowNullableUniqueConstr = TRUE;

  //If the column was store by nullable key, and was not 'NNND', it could be nullable.
  if (!allowNullableUniqueConstr && createTableNode->getChild(1/*StmtDDLCreateTable::INDEX_ATTRIBUTE_LIST*/))
    {
      ElemDDLNode *pCreateTableAttrList = 
	createTableNode->getChild(1)->castToElemDDLNode();
      ComASSERT(pCreateTableAttrList NEQ NULL);
      ElemDDLStoreOptKeyColumnList *sbkcl = NULL;
      if (pCreateTableAttrList->castToElemDDLOptionList() NEQ NULL)
        {
          for (CollIndex i = 0; i < pCreateTableAttrList->entries(); i++)
            {
              ElemDDLNode * pTableOption = (*pCreateTableAttrList)[i];
              if (pTableOption->castToElemDDLStoreOptKeyColumnList() NEQ NULL)
                {
                  sbkcl = pTableOption->castToElemDDLStoreOptKeyColumnList();
                  break;
                }
            }
        }
      else
        {
          if (pCreateTableAttrList->castToElemDDLStoreOptKeyColumnList() NEQ NULL)
              sbkcl =  pCreateTableAttrList->castToElemDDLStoreOptKeyColumnList();
        }
      if (sbkcl && sbkcl->isNullableSpecified())
        allowNullableUniqueConstr = TRUE;
    }

  int numIterationsToCompleteColumnList = 1;
  Lng32 numCols = 0;
  Lng32 numKeys = 0;
  ComTdbVirtTableColumnInfo * colInfoArray = NULL;
  ComTdbVirtTableKeyInfo * keyInfoArray = NULL;
  Lng32 identityColPos = -1;

  std::vector<NAString> userColFamVec;
  std::vector<NAString> trafColFamVec;

  // if hbase map format, turn off global serialization default.
  NABoolean hbaseSerialization = FALSE;
  NAString hbVal;
  if (hbaseMapFormat)
    {
      if (CmpCommon::getDefault(HBASE_SERIALIZATION) == DF_ON)
        {
          NAString value("OFF");
          hbVal = "ON";
          ActiveSchemaDB()->getDefaults().validateAndInsert(
               "hbase_serialization", value, FALSE);
          hbaseSerialization = TRUE;
        }
    }

  // build colInfoArray and keyInfoArray, this may take two
  // iterations if we need to add a divisioning column
  for (int iter=0; iter < numIterationsToCompleteColumnList; iter++)
    {
      numCols = colArray.entries();
      numKeys = keyArray.entries();

      colInfoArray = new(STMTHEAP) ComTdbVirtTableColumnInfo[numCols];
      keyInfoArray = new(STMTHEAP) ComTdbVirtTableKeyInfo[numKeys];

      if (buildColInfoArray(COM_BASE_TABLE_OBJECT,
                            FALSE, // not a metadata, histogram or repository object
                            &colArray, colInfoArray, implicitPK,
                            alignedFormat, &identityColPos,
                            (hbaseMapFormat ? NULL : &userColFamVec), 
                            &trafColFamVec, 
                            defaultColFam.data()))
        {
          resetHbaseSerialization(hbaseSerialization, hbVal);

          processReturn();

          return -1;
        }

      if (buildKeyInfoArray(&colArray, NULL,
                            &keyArray, colInfoArray, keyInfoArray, 
                            allowNullableUniqueConstr))
        {
          resetHbaseSerialization(hbaseSerialization, hbVal);

          processReturn();

          return -1;
        }

      if (iter == 0 && createTableNode->isDivisionClauseSpecified())
        {
          // We need the colArray to be able to bind the divisioning
          // expression, check it and compute its type. Once we have the
          // type, we will add a divisioning column of that type and
          // also add that column to the key. Then we will need to go
          // through this loop once more and create the updated colArray.
          numIterationsToCompleteColumnList = 2;
          NAColumnArray *naColArrayForBindingDivExpr = new(STMTHEAP) NAColumnArray(STMTHEAP);
          NAColumnArray *keyColArrayForBindingDivExpr = new(STMTHEAP) NAColumnArray(STMTHEAP);
          ItemExprList * divExpr = createTableNode->getDivisionExprList();
          ElemDDLColRefArray *divColNamesFromDDL = createTableNode->getDivisionColRefArray();

          CmpSeabaseDDL::convertColAndKeyInfoArrays(
               numCols,
               colInfoArray,
               numKeys,
               keyInfoArray,
               naColArrayForBindingDivExpr,
               keyColArrayForBindingDivExpr);

          for (CollIndex d=0; d<divExpr->entries(); d++)
            {
              NABoolean exceptionOccurred = FALSE;
              ComColumnOrdering divKeyOrdering = COM_ASCENDING_ORDER;
              ItemExpr *boundDivExpr =
                bindDivisionExprAtDDLTime((*divExpr)[d],
                                          keyColArrayForBindingDivExpr,
                                          STMTHEAP);
              if (!boundDivExpr)
                {
                  resetHbaseSerialization(hbaseSerialization, hbVal);

                  processReturn();

                  return -1;
                }

              if (boundDivExpr->getOperatorType() == ITM_INVERSE)
                {
                  divKeyOrdering = COM_DESCENDING_ORDER;
                  boundDivExpr = boundDivExpr->child(0);
                  if (boundDivExpr->getOperatorType() == ITM_INVERSE)
                    {
                      // in rare cases we could have two inverse operators
                      // stacked on top of each other, indicating ascending
                      divKeyOrdering = COM_ASCENDING_ORDER;
                      boundDivExpr = boundDivExpr->child(0);
                    }
                }

              try 
                {
                  // put this into a try/catch block because it could throw
                  // an exception when type synthesis fails and that would leave
                  // the transaction begun by the DDL operation in limbo
                  boundDivExpr->synthTypeAndValueId();
                }
              catch (...)
                {
                  // diags area should be set, if not, set it
                  if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_) == 0)
                    *CmpCommon::diags() << DgSqlCode(-4243)
                              << DgString0("(expression with unknown type)");
                  exceptionOccurred = TRUE;
                }

              if (exceptionOccurred ||
                  boundDivExpr->getValueId() == NULL_VALUE_ID)
                {
                  resetHbaseSerialization(hbaseSerialization, hbVal);

                  processReturn();

                  return -1;
                }

              if (validateDivisionByExprForDDL(boundDivExpr))
                {
                  resetHbaseSerialization(hbaseSerialization, hbVal);

                  processReturn();

                  return -1;
                }

              // Add a divisioning column to the list of columns and the key
              char buf[16];
              snprintf(buf, sizeof(buf), "_DIVISION_%d_", d+1);
              NAString divColName(buf);
              // if the division column name was specified in the DDL, use that instead
              if (divColNamesFromDDL && divColNamesFromDDL->entries() > d)
                divColName = (*divColNamesFromDDL)[d]->getColumnName();
              NAType * divColType =
                boundDivExpr->getValueId().getType().newCopy(STMTHEAP);
              ElemDDLColDefault *divColDefault = 
                new(STMTHEAP) ElemDDLColDefault(
                     ElemDDLColDefault::COL_COMPUTED_DEFAULT);
              NAString divExprText;
              boundDivExpr->unparse(divExprText, PARSER_PHASE, COMPUTED_COLUMN_FORMAT);
              divColDefault->setComputedDefaultExpr(divExprText);
              ElemDDLColDef * divColDef =
                new(STMTHEAP) ElemDDLColDef(NULL, &divColName, divColType, divColDefault,
                                            STMTHEAP);

              ElemDDLColRef * edcrs = 
                new(STMTHEAP) ElemDDLColRef(divColName, divKeyOrdering);

              divColDef->setColumnClass(COM_SYSTEM_COLUMN);
              divColDef->setDivisionColumnFlag(TRUE);
              divColDef->setDivisionColumnSequenceNumber(d);

              // add this new divisioning column to the end of the row
              // and also to the key, right after any existing salt and divisioning columns
              colArray.insert(divColDef);
              keyArray.insertAt(numReplicaAndSaltCols+numDivCols, edcrs);
              numDivCols++;
            }
        }

     } // iterate 1 or 2 times to get all columns, including divisioning columns

  if (hbaseSerialization)
    {
      ActiveSchemaDB()->getDefaults().validateAndInsert
        ("hbase_serialization", hbVal, FALSE);
    }

  Int32 keyLength = 0;

  for(CollIndex i = 0; i < keyArray.entries(); i++) 
    {
      const NAString &colName = keyArray[i]->getColumnName();
      Lng32      colIx    = colArray.getColumnIndex(colName);
      if (colIx < 0)
        {
          *CmpCommon::diags() << DgSqlCode(-1009)
                              << DgColumnName(colName);

          deallocEHI(ehi); 

          processReturn();

          return -1;
        }

      NAType *colType = colArray[colIx]->getColumnDataType();
      if (colType->getFSDatatype() == REC_BLOB || 
          colType->getFSDatatype() == REC_CLOB ||
          colType->getFSDatatype() == REC_ROW ||
          colType->getFSDatatype() == REC_ARRAY)
	//Cannot allow LOB, ROW or ARRAY in primary or clustering key 
	{
          NAString type;
          if (colType->getFSDatatype() == REC_BLOB ||
              colType->getFSDatatype() == REC_CLOB)
            type = "BLOB/CLOB";
          else if (colType->getFSDatatype() == REC_ROW)
            type = "ROW";
          else
            type = "ARRAY";
	  *CmpCommon::diags() << DgSqlCode(-CAT_LOB_COL_CANNOT_BE_INDEX_OR_KEY)
                              << DgColumnName(colName)
                              << DgString0(type);

          deallocEHI(ehi); 

          processReturn();

          return -1;
	}
      keyLength += colType->getEncodedKeyLength();
    }

  //check the key length: with(out) salt, 2 extra bytes needed.
  if (numSaltPartns > 0 && keyLength + 2 >= MAX_HBASE_ROWKEY_LEN_SALT_PARTIONS)
  {
    *CmpCommon::diags() << DgSqlCode(-CAT_ROWKEY_LEN_TOO_LARGE_USING_SALT_PARTIONS)
                        << DgInt0(keyLength + 2)
                        << DgInt1(MAX_HBASE_ROWKEY_LEN_SALT_PARTIONS);

    deallocEHI(ehi);
    processReturn();
    return -1;

  }
  else if (keyLength > MAX_HBASE_ROWKEY_LEN)
  {
    *CmpCommon::diags() << DgSqlCode(-CAT_ROWKEY_LEN_TOO_LARGE)
                        << DgInt0(keyLength)
                        << DgInt1(MAX_HBASE_ROWKEY_LEN);

    deallocEHI(ehi);
    processReturn();
    return -1;
  }

  if (hbaseMapFormat)
    {
      for(CollIndex i = 0; i < colArray.entries(); i++) 
        {
          if (DFS2REC::isLOB(colInfoArray[i].datatype))
            {
              *CmpCommon::diags() << DgSqlCode(-3242)
                                  << DgString0("LOB columns cannot be specified for an HBase mapped table.");
              
              deallocEHI(ehi); 
              processReturn();
              return -1;
            }

          const NAString colName = colInfoArray[i].colName;
          Lng32      colIx    = keyArray.getColumnIndex(colName);
          if (colIx < 0) // not a key col
            {
              if (colInfoArray[i].defaultClass != COM_NULL_DEFAULT)
                {
                  *CmpCommon::diags() << DgSqlCode(-3242)
                                      << DgString0("Non-key columns of an HBase mapped table must be nullable with default value of NULL.");
                  
                  deallocEHI(ehi); 
                  
                  processReturn();
                  
                  return -1;
                }

              // must have a default value if not nullable
              if (! colInfoArray[i].nullable)
                {
                  if (colInfoArray[i].defaultClass == COM_NO_DEFAULT)
                    {
                      *CmpCommon::diags() << DgSqlCode(-3242)
                                          << DgString0("Non-key non-nullable columns of an HBase mapped table must have a default value.");
                      
                      deallocEHI(ehi); 
                      
                      processReturn();
                      
                      return -1;
                    }

                }
            }          
        } // for
    }

  char ** encodedKeysBuffer = NULL;
  if (numSaltSplits > 0 || numTrafReplicas > 0 || splitByClause) {

    TrafDesc * colDescs = 
      convertVirtTableColumnInfoArrayToDescStructs(&tableName,
                                                   colInfoArray,
                                                   numCols) ;
    TrafDesc * keyDescs = 
      convertVirtTableKeyInfoArrayToDescStructs(keyInfoArray,
                                                colInfoArray,
                                                numKeys) ;

    if (createEncodedKeysBuffer(encodedKeysBuffer/*out*/,
                                numSplits /*out*/,
                                colDescs, keyDescs,
                                numSaltPartns,
                                numSaltSplits,
                                &splitByClause,
                                numTrafReplicas,
                                numKeys, 
                                keyLength, FALSE))
      {
        processReturn();
        
        return -1;
      }
  }

  NABoolean isPartitionV2Spec = createTableNode->isPartitionByClauseV2Spec();
  Int32 *partionColIdxArray = NULL;
  Int32 *subpartionColIdxArray = NULL;
  if (isPartitionV2Spec)
  {
    //validate that partition by columns exist
    const ElemDDLColRefArray & partitionColArray =
                     createTableNode->getPartitionV2ColRefArray();
    const ElemDDLColRefArray &subpartitionColArray =
                     createTableNode->getSubpartitionColRefArray();
    Int32 pColLength = 0, subpColLength = 0;
    Int32 partionColCnt = partitionColArray.entries();
    Int32 subpartionColCnt = subpartitionColArray.entries();
    if (partionColCnt > 0)
      partionColIdxArray = new (STMTHEAP) Lng32[partionColCnt];
    if (subpartionColCnt > 0)
      subpartionColIdxArray = new (STMTHEAP) Lng32[subpartionColCnt];

    //vallidate partition by value
    TrafDesc * colDescs = convertVirtTableColumnInfoArrayToDescStructs(&tableName,
                                                                       colInfoArray,
                                                                       numCols);
    ComTdbVirtTableKeyInfo * pColInfoArray = NULL;
    ComTdbVirtTableKeyInfo * subpColInfoArray = NULL;
    pColInfoArray = new (STMTHEAP) ComTdbVirtTableKeyInfo[partitionColArray.entries()];
    if (buildKeyInfoArray(&colArray, NULL,
                          (ElemDDLColRefArray*)&partitionColArray,
                          colInfoArray, pColInfoArray,
                          true, &pColLength))
    {
      processReturn();
      return -1;
    }
    TrafDesc * pColDescs = 
      convertVirtTableKeyInfoArrayToDescStructs(pColInfoArray,
                                                colInfoArray,
                                                partitionColArray.entries());
    TrafDesc * subpColDescs = NULL;
    if (subpartitionColArray.entries() > 0)
    {
      subpColInfoArray = new (STMTHEAP) ComTdbVirtTableKeyInfo[subpartitionColArray.entries()];
      if (buildKeyInfoArray(&colArray, NULL,
                            (ElemDDLColRefArray*)&subpartitionColArray,
                            colInfoArray, subpColInfoArray,
                            true, &subpColLength))
      {
        processReturn();
        return -1;
      }
      subpColDescs = 
        convertVirtTableKeyInfoArrayToDescStructs(subpColInfoArray,
                                                  colInfoArray,
                                                  subpartitionColArray.entries());

    }

    //partition/subpartition columns should be subset of primary key
    if (!implicitPK)
    {
      NABoolean contain = false;
      int unmatchedIdx = 0;
      ElemDDLColRefArray *partCol = CONST_CAST(ElemDDLColRefArray*, &partitionColArray);
      contain = keyArray.contains(*partCol, unmatchedIdx);
      if (contain && subpartitionColArray.entries() > 0)
      {
        partCol = CONST_CAST(ElemDDLColRefArray*, &subpartitionColArray);
        contain = keyArray.contains(*partCol, unmatchedIdx);
      }

      if (NOT contain)
      {
        *CmpCommon::diags() << DgSqlCode(-1198);
        return -1;
      }
    }

    ElemDDLPartitionV2Array &partitionV2Array
                            = createTableNode->getPartitionV2Array();
    NAHashDictionary<NAString, Int32> *partitionNameMap
             = new(STMTHEAP) NAHashDictionary<NAString, Int32>(NAString::hash, 101, TRUE, STMTHEAP);

    Int32 rc = validatePartitionByExpr((&(partitionV2Array)),
                                       colDescs,
                                       pColDescs,
                                       partitionColArray.entries(),
                                       pColLength,
                                       partitionNameMap);
    if (rc)
    {
      if (partitionNameMap)
        delete partitionNameMap;
      return -1;
    }

    if (createTableNode->getPartitionType() == COM_RANGE_PARTITION)
    {
      rc = validatePartitionRange((&(partitionV2Array)),
                                  partitionColArray.entries());
      if (rc)
      {
        if (partitionNameMap)
          delete partitionNameMap;
        return -1;
      }
    }

    if (subpartitionColArray.entries() > 0)
    {
      for (Int32 i = 0; i < partitionV2Array.entries(); i++)
      {
        if (partitionV2Array[i]->getSubpartitionArray() &&
            partitionV2Array[i]->getSubpartitionArray()->entries() > 0)
        {
          rc = validatePartitionByExpr(partitionV2Array[i]->getSubpartitionArray(),
                                       colDescs,
                                       subpColDescs,
                                       subpartitionColArray.entries(),
                                       subpColLength,
                                       partitionNameMap);
          if (rc)
          {
            if (partitionNameMap)
              delete partitionNameMap;
            return -1;
          }

          rc = validatePartitionRange(partitionV2Array[i]->getSubpartitionArray(),
                                      subpartitionColArray.entries());
          if (rc)
          {
            if (partitionNameMap)
              delete partitionNameMap;
            return -1;
          }
        }
      }
    }

    if (partitionNameMap)
      delete partitionNameMap;

    for(CollIndex i = 0; i < partionColCnt; i++)
    {
      partionColIdxArray[i] = pColInfoArray[i].tableColNum;
    }
    for(CollIndex i = 0; i < subpartionColCnt; i++)
    {
      subpartionColIdxArray[i] = subpColInfoArray[i].tableColNum;
    }

  }

  ComTdbVirtTableTableInfo * tableInfo = new(STMTHEAP) ComTdbVirtTableTableInfo[1];
  tableInfo->tableName = NULL;
  tableInfo->createTime = 0;
  tableInfo->redefTime = 0;
  tableInfo->objUID = 0;
  tableInfo->objDataUID = 0;
  tableInfo->schemaUID = schemaUID;
  tableInfo->isAudited = (fileAttribs.getIsAudit() ? 1 : 0);
  tableInfo->validDef = 1;
  tableInfo->hbaseCreateOptions = NULL;
  tableInfo->objectFlags = 0;
  tableInfo->tablesFlags = 0;
  
  if (fileAttribs.isOwnerSpecified())
    {
      // Fixed bug:  if BY CLAUSE specified an unregistered user, then the object
      // owner is set to 0 in metadata.  Once 0, the table could not be dropped.
      NAString owner = fileAttribs.getOwner();
      Int16 retcode =  (ComUser::getUserIDFromUserName(owner.data(),objectOwnerID, CmpCommon::diags()));
      if (retcode != 0)
         {
           *CmpCommon::diags() << DgSqlCode (-CAT_INTERNAL_EXCEPTION_ERROR)
                               << DgString0(__FILE__)
                               << DgInt0(__LINE__)
                               << DgString1("verifying grantee");
           processReturn();
           return -1;
         }
     if (schemaClass == COM_SCHEMA_CLASS_PRIVATE && 
         objectOwnerID != schemaOwnerID)
     {
        *CmpCommon::diags() << DgSqlCode(-CAT_BY_CLAUSE_IN_PRIVATE_SCHEMA);
     
        deallocEHI(ehi);
        processReturn();
        return -1;
     }
  }
  tableInfo->objOwnerID = objectOwnerID;
  tableInfo->schemaOwnerID = schemaOwnerID;

  NAString extNameForHbase;
  NAString binlogTableName;
  binlogTableName = schemaNamePart + "." + objectNamePart;
  if (NOT hbaseMapFormat) 
    {
      extNameForHbase = genHBaseObjName(
           catalogNamePart, schemaNamePart, objectNamePart,
           (NOT isMonarch ? tableNamespace : ""));
    }

  NABoolean lobV2 = (CmpCommon::getDefault(TRAF_LOB_VERSION2) == DF_ON);
  if (lobV2)
    {
      NAString hbaseResLobColumnsStr = genHBaseObjName
        (getSystemCatalog(), NAString(SEABASE_MD_SCHEMA), 
         NAString(SEABASE_LOB_COLUMNS), NAString(TRAF_RESERVED_NAMESPACE1));
      
      HbaseStr hbaseLobColumns;
      hbaseLobColumns.val = (char*)hbaseResLobColumnsStr.data();
      hbaseLobColumns.len = hbaseResLobColumnsStr.length();
      
      // if MD LOB_COLUMNS doesnt exist, return an error.
      // But dont return error is LOB_COLUMNS is being created.
      cliRC = ehi->exists(hbaseLobColumns); 
      if ((cliRC == 0) && // does not exist, cannot create lob version2
          (NOT (extNameForHbase == hbaseResLobColumnsStr)))
        {
          *CmpCommon::diags()
            << DgSqlCode(-3242)
            << DgString0("LOB V2 cannot be created due to missing metadata table LOB_COLUMNS. Do 'initialize trafodion, create lob metadata' to create it before running the command.");

          deallocEHI(ehi);
          return -1;
        }
    }

  Int32 numLOBdatafiles = -1;
  if (lobV2)
    {
      numLOBdatafiles = 0;
      if (CmpCommon::getDefaultNumeric(TRAF_LOB_HBASE_DATA_MAXLEN_DDL) != -1)
        numLOBdatafiles = CmpCommon::getDefaultNumeric(NUMBER_OF_LOBV2_DATAFILES);
    }

  for (Int32 i = 0; i < colArray.entries(); i++)
    {
      ElemDDLColDef *column = colArray[i];
      
      Lng32 datatype = column->getColumnDataType()->getFSDatatype();
      
      if ((datatype == REC_BLOB) ||
          (datatype == REC_CLOB))
        {
          if (isCreatingReadOnlyTable) {
            *CmpCommon::diags() << DgSqlCode(-3242)
                                << DgString0("Tables with LOB columns can not build as a read only table");
            deallocEHI(ehi);
            return -1;
          }
          if (lobV2)
            CmpSeabaseDDL::setMDflags
              (tableInfo->tablesFlags, MD_TABLES_LOB_VERSION2);
          
          break;
        }
    } // for

  tableInfo->numSaltPartns = numSaltPartns;
  tableInfo->numInitialSaltRegions = numInitialSaltRegions;
  tableInfo->hbaseSplitClause = splitByClause;
  tableInfo->numTrafReplicas = numTrafReplicas ;
  if (hbaseMapFormat && hbaseMappedDataFormatIsString)
    tableInfo->rowFormat = COM_HBASE_STR_FORMAT_TYPE;
  else if (alignedFormat)
    tableInfo->rowFormat = COM_ALIGNED_FORMAT_TYPE;
  else
    tableInfo->rowFormat = COM_HBASE_FORMAT_TYPE;
  tableInfo->xnRepl = xnRepl;
  if (encryptSpec)
    {
      if (rowIdEncrypt)
        CmpSeabaseDDL::setMDflags(tableInfo->objectFlags, MD_OBJECTS_ENCRYPT_ROWID_FLG);

      if (dataEncrypt)
        CmpSeabaseDDL::setMDflags(tableInfo->objectFlags, MD_OBJECTS_ENCRYPT_DATA_FLG);
    }
  if (incrBackupEnabled)
    {
      CmpSeabaseDDL::setMDflags(tableInfo->objectFlags, MD_OBJECTS_INCR_BACKUP_ENABLED);
    }
 
  if (isCreatingReadOnlyTable)
    {
      CmpSeabaseDDL::setMDflags(tableInfo->objectFlags, MD_OBJECTS_READ_ONLY_ENABLED);
    }

  tableInfo->storageType = storageType;
  tableInfo->tableNamespace = 
    (NOT tableNamespace.isNull() ? tableNamespace.data() : NULL);

  //  NAList<HbaseCreateOption*> hbaseCreateOptions(STMTHEAP);
  NAList<HbaseCreateOption*> *hbaseCreateOptions = new STMTHEAP NAList<HbaseCreateOption*>(STMTHEAP);
  NAString hco;

  short retVal = setupHbaseOptions(createTableNode->getHbaseOptionsClause(), 
                                   numSplits, extTableName, 
                                   *hbaseCreateOptions, hco);
  if (retVal)
  {
    deallocEHI(ehi);
    processReturn();
    return -1;
  }

  if (alignedFormat)
    {
      hco += "ROW_FORMAT=>ALIGNED ";
    }

  tableInfo->hbaseCreateOptions = (hco.isNull() ? NULL : hco.data());

  tableInfo->defaultColFam = NULL;
  tableInfo->allColFams = NULL;

  Int64 objUID = -1;
  //  if (Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL))
  {
    const char* v = ActiveSchemaDB()->getDefaults().
      getValue(TRAF_CREATE_TABLE_WITH_UID);
    if ((v) and (strlen(v) > 0))
      {
        objUID = str_atoi(v, strlen(v));
      }
  }

  if (isPartitionV2Spec)
    CmpSeabaseDDL::setMDflags(tableInfo->objectFlags, MD_PARTITION_TABLE_V2);

  if (createTableNode->isPartition())
    CmpSeabaseDDL::setMDflags(tableInfo->objectFlags, MD_PARTITION_V2);
 
  Int32 hbaseDropCreate = CmpCommon::getDefaultNumeric(TRAF_HBASE_DROP_CREATE);
  bool updPrivs = (hbaseDropCreate != 0);
  if (updateSeabaseMDTable(&cliInterface, 
                           catalogNamePart, schemaNamePart, objectNamePart,
                           COM_BASE_TABLE_OBJECT,
                           COM_NO_LIT,
                           tableInfo,
                           numCols,
                           colInfoArray,
                           numKeys,
                           keyInfoArray,
                           0, NULL,
                           objUID, updPrivs))
    {
      *CmpCommon::diags()
        << DgSqlCode(-CAT_UNABLE_TO_CREATE_OBJECT)
        << DgTableName(extTableName);

      deallocEHI(ehi); 
      processReturn();
      return -1;
    }

  outObjUID = objUID;
  
  // for default, data uid is same as objuid，
  // else if you change it by exchange partiton or other way
  if (USE_UUID_AS_HBASE_TABLENAME)
  {
    extNameForHbase = genHBaseObjName(
           catalogNamePart, schemaNamePart, objectNamePart,
           (NOT isMonarch ? tableNamespace : ""), outObjUID);
  }

  // update TEXT table with column families.
  // Column families are stored separated by a blank space character.
  NAString allColFams;
  NABoolean addToTextTab = FALSE;
  if (defaultColFam != SEABASE_DEFAULT_COL_FAMILY)
    addToTextTab = TRUE;
  else if (userColFamVec.size() > 1)
    addToTextTab = TRUE;
  else if ((userColFamVec.size() == 1) && (userColFamVec[0] != SEABASE_DEFAULT_COL_FAMILY))
    addToTextTab = TRUE;
  if (addToTextTab)
    {
      allColFams = defaultColFam + " ";
      
      for (int i = 0; i < userColFamVec.size(); i++)
        {
          allColFams += userColFamVec[i];
          allColFams += " ";
        }

      cliRC = updateTextTable(&cliInterface, objUID, 
                              COM_HBASE_COL_FAMILY_TEXT, 0,
                              allColFams);
      if (cliRC < 0)
        {
          *CmpCommon::diags()
            << DgSqlCode(-CAT_UNABLE_TO_CREATE_OBJECT)
            << DgTableName(extTableName);
          
          deallocEHI(ehi); 
          processReturn();
          return -1;
        }
   }

  if (createTableNode->getAddConstraintPK())
    {
      if (updatePKeyInfo(createTableNode->getAddConstraintPK(), 
                         catalogNamePart, schemaNamePart, objectNamePart,
                         objectOwnerID,
                         schemaOwnerID,
                         keyArray.entries(),
                         NULL,
                         NULL,
                         keyInfoArray,
                         &cliInterface))
        {
          return -1;
        }
    }

  char buf[4000];
  if (identityColPos >= 0)
    {
      ElemDDLColDef *colDef = colArray[identityColPos];
      
      NAString seqName;
      SequenceGeneratorAttributes::genSequenceName
        (catalogNamePart, schemaNamePart, objectNamePart, colDef->getColumnName(),
         seqName);
      
      if (colDef->getSGOptions())
        {
          colDef->getSGOptions()->setFSDataType((ComFSDataType)colDef->getColumnDataType()->getFSDatatype());
          
          if (colDef->getSGOptions()->validate(2/*identity*/))
            {
              deallocEHI(ehi); 
              
              processReturn();
              
              return -1;
            }
        }
      
      SequenceGeneratorAttributes sga;
      colDef->getSGOptions()->genSGA(sga);
      
      NAString idOptions;
      sga.display(NULL, &idOptions, TRUE);
      
      str_sprintf(buf, "create internal sequence %s.\"%s\".\"%s\" %s",
                  catalogNamePart.data(), schemaNamePart.data(), seqName.data(),
                  idOptions.data());
      
      cliRC = cliInterface.executeImmediate(buf);
      if (cliRC < 0)
        {
          cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
          
          deallocEHI(ehi); 
          
          processReturn();
          
          return -1;
        }
      
      CorrName cn(objectNamePart, STMTHEAP, schemaNamePart, catalogNamePart);
      ActiveSchemaDB()->getNATableDB()->removeNATable
        (cn,
         ComQiScope::REMOVE_MINE_ONLY, COM_BASE_TABLE_OBJECT,
         createTableNode->ddlXns(), FALSE);
      
      Int64 flags = 0;
      if (xnRepl == COM_REPL_SYNC)
        CmpSeabaseDDL::setMDflags(flags, MD_SEQGEN_REPL_SYNC_FLG);

      // update datatype and replication flags for this sequence
      str_sprintf(buf, "update %s.\"%s\".%s set fs_data_type = %d, flags = bitor(flags, %ld) where seq_type = '%s' and seq_uid = (select object_uid from %s.\"%s\".\"%s\" where catalog_name = '%s' and schema_name = '%s' and object_name = '%s' and object_type = '%s') ",
                  getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_SEQ_GEN,
                  colDef->getColumnDataType()->getFSDatatype(),
                  flags,
                  COM_INTERNAL_SG_LIT,
                  getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS,
                  catalogNamePart.data(), schemaNamePart.data(), seqName.data(),
                  COM_SEQUENCE_GENERATOR_OBJECT_LIT);
      
      Int64 rowsAffected = 0;
      cliRC = cliInterface.executeImmediate(buf, NULL, NULL, FALSE, &rowsAffected);
      if (cliRC < 0)
        {
          cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
          
          deallocEHI(ehi); 
          
          processReturn();
          
          return -1;
        }
    }

  void* res = NULL;
  int s = 0;

  Lng32 j = 0;


  NABoolean ddlXns = createTableNode->ddlXns();
  

  NABoolean isMVCC = TRUE;
  if (CmpCommon::getDefault(TRAF_TRANS_TYPE) == DF_SSCC)
    isMVCC = FALSE;

  if ((NOT extNameForHbase.isNull()) &&
      (hbaseDropCreate != 0) &&
      (NOT isPartitionV2Spec)) //for partitionv2 table, do not create hbase table
    {
      HbaseStr hbaseTable;
      hbaseTable.val = (char*)extNameForHbase.data();
      hbaseTable.len = extNameForHbase.length();
      if (isMonarch)
        {
          NAList<HbaseStr> monarchCols(STMTHEAP);
          for (Lng32 i = 0; i < colArray.entries(); i++)
            {
              NAString * nas = new(STMTHEAP) NAString();
              
              NAString colFam(colInfoArray[i].hbaseColFam);
              NAString colQual;
              HbaseAccess::convNumToId(
                   colInfoArray[i].hbaseColQual,
                   strlen(colInfoArray[i].hbaseColQual),
                   colQual);
              *nas = colFam + ":" + colQual;
              
              HbaseStr hbs;
              hbs.val = (char*)nas->data();
              hbs.len = nas->length();
              
              monarchCols.insert(hbs);
            }
          
          MonarchTableType tableType;
          if (implicitPK)
            tableType = MonarchTableType::HASH_PARTITIONED; 
          else
            tableType = MonarchTableType::RANGE_PARTITIONED;
          
          retcode = createMonarchTable(ehi, &hbaseTable, tableType, monarchCols,
                                       //&hbaseCreateOptions, 
                                       hbaseCreateOptions, 
                                       numSplits, keyLength,
                                       encodedKeysBuffer);
        } // monarch
      else
        {
          // For tables with incremental attributes, when TM_SNAPSHOT_TABLE_CREATES is set to 0,
          // there is no need to create a snapshot, but regular backup must be performed.
          Lng32 enableSnapshot = CmpCommon::getDefaultNumeric(TM_SNAPSHOT_TABLE_CREATES);
          NABoolean snapShotOption = FALSE;
          if (incrBackupEnabled && enableSnapshot)
            snapShotOption = TRUE;

          if (hbaseDropCreate == 2)
            {
              // create table in parallel pthread
              createTablePthreadArgs *args = 
                new CTXTHEAP createTablePthreadArgs;
   
              std::vector<NAString> *tcfv = new CTXTHEAP std::vector<NAString>;
              *tcfv = trafColFamVec;

              HbaseStr *hbt = new CTXTHEAP HbaseStr;
              hbt->val = new CTXTHEAP char[hbaseTable.len+1];
              strcpy(hbt->val, hbaseTable.val);
              hbt->len = hbaseTable.len;
              
              args->ehi = ehi;
              args->table = hbt; //&hbaseTable;
              args->colFamVec = tcfv; //trafColFamVec;
              args->hbaseCreateOptions = hbaseCreateOptions;
              args->numSplits = numSplits;
              args->keyLength = keyLength;
              args->encodedKeysBuffer = encodedKeysBuffer;
              args->doRetry = FALSE;
              args->ddlXns = FALSE; //ddlXns;
              args->incrBackupEnabled = snapShotOption;
              args->createIfNotExists = FALSE;
              args->isMVCC = isMVCC;
              args->transID = 0; //getTransactionIDFromContext();

              s = pthread_create(&lob_ddl_thread_id, NULL,
                                 CmpSeabaseDDLcreateTable__pthreadCreate, 
                                 (void *) args);
              if (s != 0) 
                {
                  *CmpCommon::diags() << DgSqlCode(-CAT_CREATE_OBJECT_ERROR)
                                      << DgTableName(extTableName);
                  deallocEHI(ehi); 	   
                  processReturn();
                  
                  return -1;
                }              
            }
          else
            retcode = createHbaseTable(ehi, &hbaseTable, trafColFamVec,
                                       hbaseCreateOptions, 
                                       numSplits, keyLength,
                                       encodedKeysBuffer,
                                       FALSE, ddlXns, snapShotOption,
                                       FALSE, isMVCC);
        } // hbase

      if (retcode == -1)
        {
          deallocEHI(ehi); 
          
          processReturn();
          
          return -2;
        }
    }

    if (isCreatingReadOnlyTable)
      insertIntoSharedCacheList(catalogNamePart, schemaNamePart, objectNamePart,
                                COM_BASE_TABLE_OBJECT,
                                SharedCacheDDLInfo::DDL_UPDATE,
                                SharedCacheDDLInfo::SHARED_DATA_CACHE);

    //create table update binlog meta
    if (!ComIsTrafodionReservedSchemaName(schemaNamePart) && !(createTableNode->isExternal()) && (NOT createTableNode->mapToHbaseTable()) ) 
    {
      NAString binlogCols;
      NAString binlogKeys;
      HbaseStr *oldCols = NULL;
      HbaseStr *oldKeys = NULL;
      NAArray<HbaseStr> *defStringArray = NULL;
      getTableDefForBinlogOp( &colArray, &keyArray, binlogCols,binlogKeys, createTableNode->getIsConstraintPKSpecified());
      retcode = ehi->updateTableDefForBinlog(binlogTableName, binlogCols, binlogKeys, 0);
    }

    Lng32 retcode1 = 0;
    if (!ComIsTrafodionReservedSchemaName(schemaNamePart) && incrBackupEnabled == true )
      retcode1 = updateSeabaseXDCDDLTable(&cliInterface, catalogNamePart.data(),
                                          schemaNamePart.data(), objectNamePart.data(),
                                          COM_BASE_TABLE_OBJECT_LIT);
    if (retcode < 0 || retcode1 < 0)
    {
      *CmpCommon::diags() << DgSqlCode(-8448)
                        << DgString0((char*)"ExpHbaseInterface::create()")
                        << DgString1(retcode < 0 ? "cannot update binlog table info"
                                                 : "cannot update XDCDDLTable");
      deallocEHI(ehi); 
      processReturn();
      return -2;
    }

  // new partition by clause
  if (isPartitionV2Spec)
  {
    Lng32 ret;
    NABoolean isStoredDesc = false;

    //create partition tables
    if((schemaStoredDesc || createTableNode->getFileAttributes().storedDesc() ||
      (CmpCommon::getDefault(TRAF_STORE_OBJECT_DESC) == DF_ON)) &&
      (NOT isHistogramTable(objectNamePart)))
    {
      updateObjectFlags(&cliInterface, outObjUID, MD_OBJECTS_STORED_DESC, 0);
      isStoredDesc = true;
    }

    ElemDDLPartitionV2Array &partitionV2Array =
                createTableNode->getPartitionV2Array();
    if (partitionV2Array.entries() > 0)
    {
      for (CollIndex i = 0; i < partitionV2Array.entries(); ++i)
      {
        if (partitionV2Array[i]->hasSubpartition() &&
            partitionV2Array[i]->getSubpartitionArray()
            ->entries() > 0)
        {
          ElemDDLPartitionV2Array *subpartArray =
                 partitionV2Array[i]->getSubpartitionArray();
          for (CollIndex j = 0; j < subpartArray->entries(); ++j)
          {
            ret = createSeabasePartitionTable(&cliInterface,
                                 (*subpartArray)[j]->getPartitionName(),
                                 catalogNamePart, schemaNamePart, objectNamePart);
            if (ret < 0)
              break; 
          }
        }
        else
        {
          ret = createSeabasePartitionTable(&cliInterface,
                                      partitionV2Array[i]->getPartitionName(),
                                      catalogNamePart, schemaNamePart, objectNamePart);
        }
        if (ret < 0)
        {
          cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
          deallocEHI(ehi); 
          processReturn();
          
          return -1;
        }
      }
      updateSeabaseMDPartition(&cliInterface, createTableNode,
                               catalogNamePart.data(), schemaNamePart.data(),
                               objectNamePart.data(), (Int64)outObjUID,
                               partionColIdxArray, subpartionColIdxArray);
      if (isStoredDesc)
      {
        UInt32 op = 0;
        setCacheAndStoredDescOp(op, UPDATE_REDEF_TIME);
        setCacheAndStoredDescOp(op, UPDATE_SHARED_CACHE);

        for (CollIndex i = 0; i < partitionV2Array.entries(); ++i)
        {
          if (partitionV2Array[i]->hasSubpartition() &&
              partitionV2Array[i]->getSubpartitionArray()
              ->entries() > 0)
          {
            ElemDDLPartitionV2Array *subpartArray =
                 partitionV2Array[i]->getSubpartitionArray();
            for (CollIndex j = 0; j < subpartArray->entries(); ++j)
            {
              char partitionTableName[256];
              getPartitionTableName(partitionTableName, 256, objectNamePart,
                                    (*subpartArray)[j]->getPartitionName());
              Int64 objUid = getObjectUID(&cliInterface,
                                          catalogNamePart, schemaNamePart,
                                          partitionTableName, COM_BASE_TABLE_OBJECT_LIT,
                                          NULL, NULL, NULL, false, false);
              UpdateObjRefTimeParam updObjRef(-2, objUid, isStoredDesc, FALSE);
              ret = updateCachesAndStoredDesc(&cliInterface,
                                              catalogNamePart,
                                              schemaNamePart,
                                              partitionTableName,
                                              COM_BASE_TABLE_OBJECT,
                                              op, NULL, &updObjRef, SharedCacheDDLInfo::DDL_INSERT);
              if (ret < 0)
                break; 
            }
          }
          else
          {
            char partitionTableName[256];
            getPartitionTableName(partitionTableName, 256, objectNamePart,
                                  partitionV2Array[i]->getPartitionName());
            Int64 objUid = getObjectUID(&cliInterface,
                                        catalogNamePart, schemaNamePart,
                                        partitionTableName, COM_BASE_TABLE_OBJECT_LIT,
                                        NULL, NULL, NULL, false, false);
            UpdateObjRefTimeParam updObjRef(-2, objUid, isStoredDesc, FALSE);
            ret = updateCachesAndStoredDesc(&cliInterface,
                                            catalogNamePart,
                                            schemaNamePart,
                                            partitionTableName,
                                            COM_BASE_TABLE_OBJECT,
                                            op, NULL, &updObjRef, SharedCacheDDLInfo::DDL_INSERT);
          }
          if (ret < 0)
          {
            cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
            deallocEHI(ehi); 
            processReturn();
            return -1;
          }
        }
      }

    }
  }

  // if not a compound create, update valid def to true.
  if (NOT ((createTableNode->getAddConstraintUniqueArray().entries() > 0) ||
           (createTableNode->getAddConstraintRIArray().entries() > 0) ||
           (createTableNode->getAddConstraintCheckArray().entries() > 0)))
    {
      cliRC = updateObjectValidDef(&cliInterface, 
                                   catalogNamePart, schemaNamePart, objectNamePart, 
                                   COM_BASE_TABLE_OBJECT_LIT, COM_YES_LIT);
      if (cliRC < 0)
        {
          *CmpCommon::diags()
            << DgSqlCode(-CAT_UNABLE_TO_CREATE_OBJECT)
            << DgTableName(extTableName);
          
          deallocEHI(ehi); 
          processReturn();
          return -2;
        }
    }

  if ((NOT isCompound) && (hbaseDropCreate != 2))
    {
      CorrName cn(objectNamePart, STMTHEAP, schemaNamePart, catalogNamePart);
      ActiveSchemaDB()->getNATableDB()->removeNATable
        (cn, 
         ComQiScope::REMOVE_MINE_ONLY, 
         COM_BASE_TABLE_OBJECT,
         createTableNode->ddlXns(), FALSE);
    }

  processReturn();

  return 0;

 label_error:
  if (hbaseSerialization)
    {
      ActiveSchemaDB()->getDefaults().validateAndInsert
        ("hbase_serialization", hbVal, FALSE);
    }
  return -1;
} // createSeabaseTable2

void CmpSeabaseDDL::createSeabaseTable(
                                       StmtDDLCreateTable * createTableNode,
                                       NAString &currCatName, NAString &currSchName,
                                       NABoolean isCompound,
                                       Int64 *retObjUID,
                                       NABoolean * retGenStoredDesc,
                                       NABoolean * retIncBack)
{
  Lng32 cliRC = 0;

  NABoolean xnWasStartedHere = FALSE;
  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, 
  CmpCommon::context()->sqlSession()->getParentQid());

  ComObjectName tableName(createTableNode->getTableName());
  ComAnsiNamePart currCatAnsiName(currCatName);
  ComAnsiNamePart currSchAnsiName(currSchName);
  tableName.applyDefaults(currCatAnsiName, currSchAnsiName);
  const NAString catalogNamePart = tableName.getCatalogNamePartAsAnsiString();
  const NAString schemaNamePart = tableName.getSchemaNamePartAsAnsiString(TRUE);
  const NAString objectNamePart = tableName.getObjectNamePartAsAnsiString(TRUE);
  
  // cannot do pthread lob creation if autocommit is off.
  if (NOT GetCliGlobals()->currContext()->getTransaction()->autoCommit())
    {
      NAString value("0"); // value 0 turns off pthread
      ActiveSchemaDB()->getDefaults().holdOrRestore("traf_lob_parallel_ddl", 1);
      ActiveSchemaDB()->getDefaults().validateAndInsert(
           "traf_lob_parallel_ddl", value, FALSE);      
    }

  if (beginXnIfNotInProgress(&cliInterface, xnWasStartedHere))
    return;

  Int64 objUID = 0;
  NABoolean schemaStoredDesc = FALSE;
  NABoolean schemaIncBack = FALSE;
  short rc =
    createSeabaseTable2(cliInterface, createTableNode, currCatName, currSchName,
                        isCompound, objUID, schemaStoredDesc, schemaIncBack);
  ActiveSchemaDB()->getDefaults().holdOrRestore("traf_lob_parallel_ddl", 2);

  if ((CmpCommon::diags()->getNumber(DgSqlCode::ERROR_)) &&
      (rc < 0))
    {
      endXnIfStartedHere(&cliInterface, xnWasStartedHere, -1);

      if (rc == -2) // cleanup before returning error..
        {
           cleanupObjectAfterError(cliInterface,
                                  catalogNamePart, schemaNamePart, objectNamePart,
                                   COM_BASE_TABLE_OBJECT,
                                   NULL,
                                   createTableNode->ddlXns());
        }

      return;
    }

  if (retObjUID)
    *retObjUID = objUID;

  // If table already exists, just return
  if (rc == 1)
  {
    ddlResetObjectEpochs(TRUE /*doAbort*/, TRUE /*clear list*/);
    endXnIfStartedHere(&cliInterface, xnWasStartedHere, 0);

    // Release all DDL object locks
    LOGTRACE(CAT_SQL_LOCK, "Release all DDL object locks");
    CmpCommon::context()->releaseAllDDLObjectLocks();

    return;
  }

  CmpCommon::context()->ddlObjsList().updateObjUID(tableName.getExternalName(), objUID);

  ParDDLFileAttrsCreateTable &fileAttribs =
    createTableNode->getFileAttributes();
  
  NABoolean genStoredDesc = 
    ((schemaStoredDesc || fileAttribs.storedDesc() ||
      (CmpCommon::getDefault(TRAF_STORE_OBJECT_DESC) == DF_ON)) &&
     (NOT isHistogramTable(objectNamePart)));

  if (createTableNode->isVolatile())
    {
      if (fileAttribs.storedDesc())
        {
          *CmpCommon::diags()
            << DgSqlCode(-3242)
            << DgString0("Cannot specify 'stored desc' for volatile tables.");

          endXnIfStartedHere(&cliInterface, xnWasStartedHere, -1);
          processReturn();
          return;
        }

      genStoredDesc = FALSE;
    }

  if (retGenStoredDesc)
    *retGenStoredDesc = genStoredDesc;

  if (retIncBack)
    *retIncBack= schemaIncBack;

  if (NOT isCompound)
    {
      if (updateObjectRedefTime(&cliInterface, 
                                catalogNamePart, schemaNamePart, objectNamePart,
                                COM_BASE_TABLE_OBJECT_LIT, -1, objUID,
                                genStoredDesc, FALSE))
        {
          endXnIfStartedHere(&cliInterface, xnWasStartedHere, -1);
          
          processReturn();
       
          return;
        }
      
      // Add entry to list of DDL operations
      if (genStoredDesc && 
          (! createTableNode->isExternal()) && 
          (! Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)))
        insertIntoSharedCacheList(catalogNamePart, schemaNamePart, objectNamePart,
                                  COM_BASE_TABLE_OBJECT, SharedCacheDDLInfo::DDL_INSERT);
        if (!xnInProgress(&cliInterface))
           finalizeSharedCache();
    }

  endXnIfStartedHere(&cliInterface, xnWasStartedHere, 0);

  return;
}

Lng32 CmpSeabaseDDL::createSeabasePartitionTable(ExeCliInterface * cliInterface,
                                           const NAString &partName,
                                           const NAString &currCatName,
                                           const NAString &currSchName,
                                           const NAString &baseTableName)
{
  Lng32 ret;

  char partitionTableName[256];
  getPartitionTableName(partitionTableName, 256, baseTableName, partName);

  char likeTable[1000];
  snprintf(likeTable, sizeof(likeTable), "%s.%s.%s like %s.%s.%s;",
           currCatName.data(), currSchName.data(),
           ToAnsiIdentifier(partitionTableName).data(), currCatName.data(),
           currSchName.data(), ToAnsiIdentifier(baseTableName).data());
  NAString likeQuery(likeTable);
  NAString query = "create partition table ";
  query += likeQuery;

  ret = cliInterface->executeImmediate((char*)query.data());
  return ret;
}

Lng32 CmpSeabaseDDL::createSeabasePartitionTableWithInfo(ExeCliInterface * cliInterface,
                                           const NAString &partName,
                                           const NAString &currCatName,
                                           const NAString &currSchName,
                                           const NAString &baseTableName,
                                           Int64 objUID,
                                           Int32 partPositoin,
                                           NAString &PKey,
                                           NAString &pEntityNmae,
                                           NAString &PKcolName,
                                           NAString &PK,
                                           int i,
                                           char * partitionTableName)
{
  Lng32 ret;

  getPartitionTableName(partitionTableName, 256, baseTableName, partName);

  char sqlBuff[1000];
  snprintf(sqlBuff, sizeof(sqlBuff), "%s.%s.%s like %s.%s.%s;",
           currCatName.data(), currSchName.data(),
           ToAnsiIdentifier(partitionTableName).data(), currCatName.data(),
           currSchName.data(), ToAnsiIdentifier(baseTableName).data());
  NAString likeQuery(sqlBuff);
  NAString query = "create partition table ";
  query += likeQuery;

  ret = cliInterface->executeImmediate((char*)query.data());
  if (ret < 0)
  {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return ret;
  }

  {
    cliInterface->holdAndSetCQD("TRAF_UPSERT_THROUGH_PARTITIONS", "ON");
    {
      // where (%s) < (%s) with mult column will get error
      str_sprintf(sqlBuff, "upsert using load into %s.\"%s\".\"%s\" select * from %s.\"%s\".\"%s\" where ",
                  currCatName.data(), currSchName.data(), partitionTableName,
                  currCatName.data(), currSchName.data(), pEntityNmae.data());

      NAString loadQuery;
      loadQuery.format("%s (%s) %s (%s)", sqlBuff, PKcolName.data(), (i == 0 ? "<" : ">="), PK.data());

      ret = cliInterface->executeImmediate((char*)loadQuery.data());
      if (ret < 0)
      {
        cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
        return ret;
      }
    }
    cliInterface->restoreCQD("TRAF_UPSERT_THROUGH_PARTITIONS");
  }

  Int64 partitionUID = getObjectUID(cliInterface, currCatName.data(), currSchName.data(), partitionTableName, COM_BASE_TABLE_OBJECT_LIT);

  snprintf(sqlBuff, sizeof(sqlBuff), "update %s.\"%s\".%s set PARTITION_ORDINAL_POSITION = PARTITION_ORDINAL_POSITION + 1 "
               " where PARTITION_ORDINAL_POSITION >= %d and PARENT_UID = %ld;",
               getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_PARTITIONS,
               partPositoin, objUID);
  ret = cliInterface->executeImmediate(sqlBuff);
  if (ret < 0)
  {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return ret;
  }

  ret = updateSeabaseTablePartitions(cliInterface, objUID, partName.data(), partitionTableName,
                                      partitionUID, COM_PRIMARY_PARTITION_LIT, (partPositoin), PKey.data());

  if (ret < 0)
  {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return ret;
  }
  return ret;
}

// unnamed user constraints are assigned a generated name of the format:
//  TRAF_UNNAMED_CONSTR_PREFIX<num>_<tab>
// "TRAF_UNNAMED_CONSTR_PREFIX" is a predefined reserved string 
//        "_Traf_UnNamed_Constr_"
// <num> is a number from 1 onwards
// <tab> is the name of the table this constr is being created for.
// Ex: _Traf_UnNamed_Constr_12_T
//
// Go through the current unnamed constraints defined on
// the naTable, finds out the max <num> and returns "<num>+1"
// If no unnamed constrs, returns inNum.
// If an error, returns -1
static Int32 extractNumVal(const NAString &constrName)
{
  Int32 numVal = -1;

  // constrName must be atleast "prefix + 3"(1 digit for num, 1 byte for '_',
  // and 1 byte for <tab>)
  if ((constrName.length() >= (strlen(TRAF_UNNAMED_CONSTR_PREFIX) + 3)) &&
      (constrName.index(TRAF_UNNAMED_CONSTR_PREFIX) == 0))
    {
      size_t numStart = strlen(TRAF_UNNAMED_CONSTR_PREFIX);
      size_t numEnd   = constrName.first('_', numStart);
      if ((numEnd != NA_NPOS) && (numEnd < (constrName.length()-1)))
        {
          numVal = str_atoi(&constrName.data()[numStart], (numEnd-numStart));
        }
    }

  return numVal;
}

static Int32 getNextNumVal(const NAString &constrName, Int32 inNum)
{
  Int32 retNum = inNum;
  Int32  numVal = extractNumVal(constrName);
  if ((numVal > 0) && (numVal > inNum))
    retNum = numVal;

  return retNum;
}

Int32 CmpSeabaseDDL::getNextUnnamedConstrNum(NATable *naTable)
{
  Int32 nextNum = 0;

  const CheckConstraintList &ccList = naTable->getCheckConstraints();
  for (Int32 i = 0; i < ccList.entries(); i++)
    {
      CheckConstraint *cc = (CheckConstraint*)ccList[i];
      const NAString &constrName = cc->getConstraintName().getObjectName();
      nextNum = getNextNumVal(constrName, nextNum);
    } // for
  
  const AbstractRIConstraintList &ucList = naTable->getUniqueConstraints();
  for (Int32 i = 0; i < ucList.entries(); i++)
    {
      AbstractRIConstraint *uc = (AbstractRIConstraint*)ucList[i];
      const NAString &constrName = uc->getConstraintName().getObjectName();
      nextNum = getNextNumVal(constrName, nextNum);
    } // for

  const AbstractRIConstraintList &rcList = naTable->getRefConstraints();
  for (Int32 i = 0; i < rcList.entries(); i++)
    {
      AbstractRIConstraint *rc = (AbstractRIConstraint*)rcList[i];
      const NAString &constrName = rc->getConstraintName().getObjectName();
      nextNum = getNextNumVal(constrName, nextNum);      
    } // for

  nextNum++;

  return nextNum;
}

void CmpSeabaseDDL::addConstraints(
                                   ComObjectName &tableName,
                                   ComAnsiNamePart &currCatAnsiName,
                                   ComAnsiNamePart &currSchAnsiName,
                                   StmtDDLNode * ddlNode,
                                   StmtDDLAddConstraintPK * pkConstr,
                                   StmtDDLAddConstraintUniqueArray &uniqueConstrArr,
                                   StmtDDLAddConstraintRIArray &riConstrArr,
                                   StmtDDLAddConstraintCheckArray &checkConstrArr,
                                   Int32 inUnnamedConstrNum,
                                   NABoolean isCompound,
                                   NABoolean isPartitionV2)
{
  Lng32 cliRC = 0;

  const NAString catalogNamePart = tableName.getCatalogNamePartAsAnsiString();
  const NAString schemaNamePart = tableName.getSchemaNamePartAsAnsiString(TRUE);
  const NAString objectNamePart = tableName.getObjectNamePartAsAnsiString(TRUE);
  const NAString extTableName = tableName.getExternalName(TRUE);

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, 
  CmpCommon::context()->sqlSession()->getParentQid());

  char buf[5000];

  Int32 unnamedConstrNum = inUnnamedConstrNum;
  if (pkConstr)
    {
      NAString uniqueName;
      if (genUniqueName(pkConstr, NULL, uniqueName))
        {
          goto label_return;
        }          
      
      ComObjectName constrName(uniqueName);

      constrName.applyDefaults(currCatAnsiName, currSchAnsiName);
      const NAString constrCatalogNamePart = constrName.getCatalogNamePartAsAnsiString();
      const NAString constrSchemaNamePart = constrName.getSchemaNamePartAsAnsiString(TRUE);
      const NAString constrObjectNamePart = constrName.getObjectNamePartAsAnsiString(TRUE);
      
      ElemDDLConstraintPK *constraintNode = 
        ( pkConstr->getConstraint() )->castToElemDDLConstraintPK();
      ElemDDLColRefArray &keyColumnArray = constraintNode->getKeyColumnArray();
      
      NAString keyColNameStr;
      for (CollIndex i = 0; i < keyColumnArray.entries(); i++)
        {
          keyColNameStr += "\"";
          keyColNameStr += keyColumnArray[i]->getColumnName();
          keyColNameStr += "\"";

          if (keyColumnArray[i]->getColumnOrdering() == COM_DESCENDING_ORDER)
            keyColNameStr += "DESC";
          else
            keyColNameStr += "ASC";
 
          if (i+1 < keyColumnArray.entries())
            keyColNameStr += ", ";
        }
      
      
      str_sprintf(buf, "alter table \"%s\".\"%s\".\"%s\" add constraint \"%s\".\"%s\".\"%s\" primary key %s (%s)",
                  catalogNamePart.data(), schemaNamePart.data(), objectNamePart.data(),
                  constrCatalogNamePart.data(), constrSchemaNamePart.data(), constrObjectNamePart.data(),
                  (constraintNode->isNullableSpecified() ? " nullable " : ""),
                  keyColNameStr.data());
      
      cliRC = cliInterface.executeImmediate(buf);
      if (cliRC < 0)
        {
          cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
          
          processReturn();
          
          goto label_return;
        }
    }
  
  if (uniqueConstrArr.entries() > 0)
    {
      for (Lng32 i = 0; i < uniqueConstrArr.entries(); i++)
        {
          StmtDDLAddConstraintUnique *uniqConstr = 
            uniqueConstrArr[i];

          NAString uniqueName;
          if (genUniqueName(uniqConstr, &unnamedConstrNum, uniqueName))
            {
              goto label_return;
            }          

          ComObjectName constrName(uniqueName);

          constrName.applyDefaults(currCatAnsiName, currSchAnsiName);
          const NAString constrCatalogNamePart = constrName.getCatalogNamePartAsAnsiString();
          const NAString constrSchemaNamePart = constrName.getSchemaNamePartAsAnsiString(TRUE);
          const NAString constrObjectNamePart = constrName.getObjectNamePartAsAnsiString(TRUE);

          ElemDDLConstraintUnique *constraintNode = 
            ( uniqConstr->getConstraint() )->castToElemDDLConstraintUnique();
          ElemDDLColRefArray &keyColumnArray = constraintNode->getKeyColumnArray();
 
          NAString keyColNameStr;
          for (CollIndex i = 0; i < keyColumnArray.entries(); i++)
            {
              keyColNameStr += "\"";
              keyColNameStr += keyColumnArray[i]->getColumnName();
              keyColNameStr += "\"";

              if (keyColumnArray[i]->getColumnOrdering() == COM_DESCENDING_ORDER)
                keyColNameStr += "DESC";
              else
                keyColNameStr += "ASC";

              if (i+1 < keyColumnArray.entries())
                keyColNameStr += ", ";
            }

          str_sprintf(buf, "alter table \"%s\".\"%s\".\"%s\" add constraint \"%s\".\"%s\".\"%s\" unique (%s)",
                      catalogNamePart.data(), schemaNamePart.data(), objectNamePart.data(),
                      constrCatalogNamePart.data(), constrSchemaNamePart.data(), constrObjectNamePart.data(),
                      keyColNameStr.data());
                      
          cliRC = cliInterface.executeImmediate(buf);
          if (cliRC < 0)
            {
              cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
              
              processReturn();
              
              goto label_return;
            }

        } // for
    } // if

  if (riConstrArr.entries() > 0)
    {
      for (Lng32 i = 0; i < riConstrArr.entries(); i++)
        {
          StmtDDLAddConstraintRI *refConstr = riConstrArr[i];

          ComObjectName refdTableName(refConstr->getReferencedTableName(), COM_TABLE_NAME);
          refdTableName.applyDefaults(currCatAnsiName, currSchAnsiName);
          const NAString refdCatNamePart = refdTableName.getCatalogNamePartAsAnsiString();
          const NAString refdSchNamePart = refdTableName.getSchemaNamePartAsAnsiString(TRUE);
          const NAString refdObjNamePart = refdTableName.getObjectNamePartAsAnsiString(TRUE);

          NAString uniqueName;
          if (genUniqueName(refConstr, &unnamedConstrNum, uniqueName))
            {
              goto label_return;
            }          
          
          ComObjectName constrName(uniqueName);

          constrName.applyDefaults(currCatAnsiName, currSchAnsiName);
          const NAString constrCatalogNamePart = constrName.getCatalogNamePartAsAnsiString();
          const NAString constrSchemaNamePart = constrName.getSchemaNamePartAsAnsiString(TRUE);
          const NAString constrObjectNamePart = constrName.getObjectNamePartAsAnsiString(TRUE);
          const NAString &addConstrName = constrName.getExternalName();

          ElemDDLConstraintRI *constraintNode = 
            ( refConstr->getConstraint() )->castToElemDDLConstraintRI();
          ElemDDLColNameArray &ringColumnArray = constraintNode->getReferencingColumns();
 
          NAString ringColNameStr;
          for (CollIndex i = 0; i < ringColumnArray.entries(); i++)
            {
              ringColNameStr += "\"";
              ringColNameStr += ringColumnArray[i]->getColumnName();
              ringColNameStr += "\"";
              if (i+1 < ringColumnArray.entries())
                ringColNameStr += ", ";
            }

          ElemDDLColNameArray &refdColumnArray = constraintNode->getReferencedColumns();
 
          NAString refdColNameStr;
          if (refdColumnArray.entries() > 0)
            refdColNameStr = "(";
          for (CollIndex i = 0; i < refdColumnArray.entries(); i++)
            {
              refdColNameStr += "\"";
              refdColNameStr += refdColumnArray[i]->getColumnName();
              refdColNameStr += "\"";         
              if (i+1 < refdColumnArray.entries())
                refdColNameStr += ", ";
            }
          if (refdColumnArray.entries() > 0)
            refdColNameStr += ")";

          // Update the epoch value of the referenced table to lock out other user access
          CorrName cn2(refdObjNamePart.data(),
                       STMTHEAP,
                       refdSchNamePart.data(),
                       refdCatNamePart.data());

          // If this is a circular constraint, then skip this check
          //  (the referencing table is the same as the referenced table)
          NATable *refdNaTable = NULL;
          if (cn2.getExposedNameAsAnsiString() != extTableName)
            {
	      if (lockObjectDDL(cn2.getQualifiedNameObj(),
				COM_BASE_TABLE_OBJECT,
				cn2.isVolatile(),
				cn2.isExternal()) < 0) {
		processReturn();
		goto label_return;
	      }

              BindWA bindWA(ActiveSchemaDB(), CmpCommon::context(), TRUE /*inDDL*/);
              refdNaTable = bindWA.getNATable(cn2);
              if (refdNaTable == NULL)
              {
                processReturn();
                goto label_return;
              }

              if (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL))
                {
                  if (ddlSetObjectEpoch(refdNaTable, STMTHEAP, TRUE /*allow reads*/) < 0)
                    {
                      processReturn();
                      goto label_return;
                    }
                }
            }

          str_sprintf(buf, "alter table \"%s\".\"%s\".\"%s\" add constraint \"%s\".\"%s\".\"%s\" foreign key (%s) references \"%s\".\"%s\".\"%s\" %s %s",
                      catalogNamePart.data(), schemaNamePart.data(), objectNamePart.data(),
                      constrCatalogNamePart.data(), constrSchemaNamePart.data(), constrObjectNamePart.data(),
                      ringColNameStr.data(),
                      refdCatNamePart.data(), refdSchNamePart.data(), refdObjNamePart.data(),
                      (refdColumnArray.entries() > 0 ? refdColNameStr.data() : " "),
                      (NOT constraintNode->isEnforced() ? " not enforced " : ""));
                      
          cliRC = cliInterface.executeImmediate(buf);
          if (cliRC < 0)
            {
              cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
              
              processReturn();
              
            }

          if (NOT isCompound)
            {
              
              // remove natable for the table being referenced
              ActiveSchemaDB()->getNATableDB()->removeNATable
                (cn2,
                 ComQiScope::REMOVE_FROM_ALL_USERS, 
                 COM_BASE_TABLE_OBJECT,
                 ddlNode->ddlXns(), FALSE);
            }

          // Add entry to list of DDL operations that referenced table changed
          if (refdNaTable && refdNaTable->isStoredDesc() && !Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL))
            insertIntoSharedCacheList(refdCatNamePart, refdSchNamePart, refdObjNamePart,
                                      COM_BASE_TABLE_OBJECT, SharedCacheDDLInfo::DDL_UPDATE);
          if (cliRC < 0)
            goto label_return;

          if (NOT constraintNode->isEnforced())
            {
              *CmpCommon::diags()
                << DgSqlCode(1313)
                << DgString0(addConstrName);
            }
          
        } // for
    } // if

  if (checkConstrArr.entries() > 0)
    {
      for (Lng32 i = 0; i < checkConstrArr.entries(); i++)
        {
          StmtDDLAddConstraintCheck *checkConstr = checkConstrArr[i];

          NAString uniqueName;
          if (genUniqueName(checkConstr, &unnamedConstrNum, uniqueName))
            {
              goto label_return;
            }          
          
          ComObjectName constrName(uniqueName);
          constrName.applyDefaults(currCatAnsiName, currSchAnsiName);
          const NAString constrCatalogNamePart = constrName.getCatalogNamePartAsAnsiString();
          const NAString constrSchemaNamePart = constrName.getSchemaNamePartAsAnsiString(TRUE);
          const NAString constrObjectNamePart = constrName.getObjectNamePartAsAnsiString(TRUE);

          NAString constrText;
          getCheckConstraintText(checkConstr, constrText, isPartitionV2);
          str_sprintf(buf, "alter table \"%s\".\"%s\".\"%s\" add constraint \"%s\".\"%s\".\"%s\" check %s",
                      catalogNamePart.data(), schemaNamePart.data(), objectNamePart.data(),
                      constrCatalogNamePart.data(), constrSchemaNamePart.data(), constrObjectNamePart.data(),
                      constrText.data()
                      );
                      
          cliRC = cliInterface.executeImmediate(buf);
          if (cliRC < 0)
            {
              cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
              
              processReturn();
              
              goto label_return;
            }
        }
    }

 label_return:

  if (NOT isCompound)
    {
      // remove NATable cache entries for this table
      CorrName cn(objectNamePart.data(),
                  STMTHEAP,
                  schemaNamePart.data(),
                  catalogNamePart.data());
      
      // remove NATable for this table
      ActiveSchemaDB()->getNATableDB()->removeNATable
        (cn,
         ComQiScope::REMOVE_FROM_ALL_USERS, 
         COM_BASE_TABLE_OBJECT,
         ddlNode->ddlXns(), FALSE);
    }

  return;
}

void CmpSeabaseDDL::createSeabaseTableCompound(
                                       StmtDDLCreateTable * createTableNode,
                                       NAString &currCatName, NAString &currSchName)
{
  Lng32 cliRC = 0;
  Lng32 retcode = 0;
  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, 
  CmpCommon::context()->sqlSession()->getParentQid());

  ComObjectName tableName(createTableNode->getTableName());
  ComAnsiNamePart currCatAnsiName(currCatName);
  ComAnsiNamePart currSchAnsiName(currSchName);
  tableName.applyDefaults(currCatAnsiName, currSchAnsiName);
  const NAString catalogNamePart = tableName.getCatalogNamePartAsAnsiString();
  const NAString schemaNamePart = tableName.getSchemaNamePartAsAnsiString(TRUE);
  const NAString objectNamePart = tableName.getObjectNamePartAsAnsiString(TRUE);
  const NAString extTableName = tableName.getExternalName(TRUE);

  BindWA bindWA(ActiveSchemaDB(), CmpCommon::context(), TRUE/*inDDL*/);
  CorrName cn(objectNamePart, STMTHEAP, schemaNamePart, catalogNamePart);
  NATable *naTable = NULL; 

  NABoolean xnWasStartedHere = FALSE;
  Int64 objUID = 0;
  NABoolean genStoredDesc = FALSE;
  NABoolean schemaIncBack = FALSE;

  ParDDLFileAttrsCreateTable &fileAttribs =
    createTableNode->getFileAttributes();

  if ((createTableNode->isVolatile()) &&
      ((createTableNode->getAddConstraintUniqueArray().entries() > 0) ||
       (createTableNode->getAddConstraintRIArray().entries() > 0) ||
       (createTableNode->getAddConstraintCheckArray().entries() > 0)))
    {
      *CmpCommon::diags() << DgSqlCode(-1283);
      
      processReturn();
      
      goto label_error;
    }

  if ((createTableNode->mapToHbaseTable()) &&
      ((createTableNode->getAddConstraintUniqueArray().entries() > 0) ||
       (createTableNode->getAddConstraintRIArray().entries() > 0) ||
       (createTableNode->getAddConstraintCheckArray().entries() > 0)))
    {
      *CmpCommon::diags() << DgSqlCode(-3242)
                          << DgString0("Cannot specify unique, referential or check constraints for an HBase mapped table.");
      
      processReturn();
      
      goto label_error;
    }

  // if 'create if not exists' and table exists, just return.
  if (createTableNode->createIfNotExists())
    {
      retcode = existsInSeabaseMDTable(
           &cliInterface, 
           catalogNamePart, schemaNamePart, objectNamePart,
           COM_UNKNOWN_OBJECT, FALSE);
      if (retcode == 1) // exists
        return;
    }

  // turn off pthread lob creation
  {
    NAString value("0"); // value 0 turns off pthread
    ActiveSchemaDB()->getDefaults().holdOrRestore("traf_lob_parallel_ddl", 1);
    ActiveSchemaDB()->getDefaults().validateAndInsert(
         "traf_lob_parallel_ddl", value, FALSE);      
  }

  createSeabaseTable(createTableNode, currCatName, currSchName, TRUE, &objUID,
                     &genStoredDesc, &schemaIncBack);
  if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_))
    {
      goto label_error;
    }

  cliRC = cliInterface.holdAndSetCQD("TRAF_NO_CONSTR_VALIDATION", "ON");
  if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      
      processReturn();
      
      goto label_error;
    }

  cliRC = cliInterface.holdAndSetCQD("DONOT_WRITE_XDC_DDL", "ON");
  if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

      processReturn();

      goto label_error;
    }


  // create unnamed constraints in deterministic format if table is being 
  // created for xDC replication. Constr start at 1.
  addConstraints(tableName, currCatAnsiName, currSchAnsiName,
                 createTableNode,
                 NULL,
                 createTableNode->getAddConstraintUniqueArray(),
                 createTableNode->getAddConstraintRIArray(),
                 createTableNode->getAddConstraintCheckArray(),
                 ((fileAttribs.isXnReplSpecified() || fileAttribs.isIncrBackupSpecified() || schemaIncBack == true) ? 1 : 0),
                 TRUE, createTableNode->isPartitionByClauseV2Spec());

  cliRC = cliInterface.restoreCQD("DONOT_WRITE_XDC_DDL");
  if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_))
    {
      if (cliInterface.statusXn() == 0) // xn in progress
        {
          rollbackXn(&cliInterface);
        }

      *CmpCommon::diags() << DgSqlCode(-1029)
                          << DgTableName(extTableName);
      
      processReturn();
      
      goto label_error;
    }

  cliRC = cliInterface.restoreCQD("traf_no_constr_validation");
  ActiveSchemaDB()->getDefaults().holdOrRestore("traf_lob_parallel_ddl", 2);

  if (beginXnIfNotInProgress(&cliInterface, xnWasStartedHere))
    goto label_error;

  cliRC = updateObjectValidDef(&cliInterface, 
                               catalogNamePart, schemaNamePart, objectNamePart, 
                               COM_BASE_TABLE_OBJECT_LIT, COM_YES_LIT);
  if (cliRC < 0)
    {
      *CmpCommon::diags()
        << DgSqlCode(-CAT_UNABLE_TO_CREATE_OBJECT)
        << DgTableName(extTableName);
      
      endXnIfStartedHere(&cliInterface, xnWasStartedHere, cliRC);

      goto label_error;
    }

  if (updateObjectRedefTime(&cliInterface, 
                            catalogNamePart, schemaNamePart, objectNamePart,
                            COM_BASE_TABLE_OBJECT_LIT, -1, objUID,
                            genStoredDesc, FALSE))
    {
      endXnIfStartedHere(&cliInterface, xnWasStartedHere, -1);
      
      goto label_error;
    }
  
  if (genStoredDesc)
    {
      if (alterSeabaseIndexStoredDesc(cliInterface, catalogNamePart, schemaNamePart, objectNamePart, TRUE) < 0)
        {
          endXnIfStartedHere(&cliInterface, xnWasStartedHere, -1);
     
          goto label_error;
        }

      // Add entry to list of DDL operations
      if (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL))
        insertIntoSharedCacheList(catalogNamePart, schemaNamePart, objectNamePart,
                                  COM_BASE_TABLE_OBJECT, SharedCacheDDLInfo::DDL_INSERT);


      // Add indexes to list of DDL operations
      char buf[4000];

      // get list of indexes
      str_sprintf(buf,
                  "select o.catalog_name, o.schema_name, o.object_name, keytag "
                  "from %s.\"%s\".%s i, %s.\"%s\".%s o "
                  "where base_table_uid = "
                  " (select object_uid from %s.\"%s\".%s "
                  "  where catalog_name = '%s' and schema_name = '%s' "
                  "    and object_name = '%s' and object_type = 'BT'"
                  " ) "
                  "and index_uid = o.object_uid and keytag > 0",
                  getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_INDEXES,
                  getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS,
                  getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS,
                  catalogNamePart.data(),schemaNamePart.data(), objectNamePart.data());

      Queue * indexes = NULL;
      cliRC = cliInterface.fetchAllRows(indexes, buf, 0, FALSE, FALSE, TRUE);
      if (cliRC < 0)
        {
          cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

          endXnIfStartedHere(&cliInterface, xnWasStartedHere, -1);
      
          goto label_error;
        }

      if (indexes && indexes->numEntries() > 0)
      {
        indexes->position();
        for (int i = 0; i < indexes->numEntries(); i++)
        {
          OutputInfo * idx = (OutputInfo*) indexes->getNext();

          // skip clustering index
          if (*(Int32*)idx->get(3) == 0)
           continue;
  
          NAString ixCatName(idx->get(0));
          NAString ixSchName(idx->get(1));
          NAString ixName(idx->get(2));
          QualifiedName indexName (ixName, ixSchName, ixCatName);

          // Add entry to list of DDL operations
          if (genStoredDesc && (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)))
            insertIntoSharedCacheList(ixCatName, ixSchName, ixName,
                                      COM_INDEX_OBJECT, SharedCacheDDLInfo::DDL_INSERT);
        }
      }
    }

  endXnIfStartedHere(&cliInterface, xnWasStartedHere, cliRC);
  
  ActiveSchemaDB()->getNATableDB()->removeNATable
    (cn, 
     ComQiScope::REMOVE_FROM_ALL_USERS, 
     COM_BASE_TABLE_OBJECT,
     createTableNode->ddlXns(), FALSE);

  return;

 label_error:
  cliRC = cliInterface.restoreCQD("traf_no_constr_validation");
  ActiveSchemaDB()->getDefaults().holdOrRestore("traf_lob_parallel_ddl", 2);

  if (NOT createTableNode->isVolatile())
    {
      cleanupObjectAfterError(cliInterface, 
                              catalogNamePart, schemaNamePart, objectNamePart,
                              COM_BASE_TABLE_OBJECT, NULL,
                              createTableNode->ddlXns());
      return;
    }
}

// return: 0, does not exist. 1, exists. -1, error.
short CmpSeabaseDDL::lookForTableInMD(
     ExeCliInterface *cliInterface,
     NAString &catNamePart, NAString &schNamePart, NAString &objNamePart,
     NABoolean schNameSpecified, NABoolean hbaseMapSpecified,
     ComObjectName &tableName, NAString &tabName, NAString &extTableName,
     const ComObjectType objectType, NABoolean checkForValidHbaseName)
{
  short retcode = 0;

  if ((schNamePart == HBASE_EXT_MAP_SCHEMA) &&
      (NOT hbaseMapSpecified) &&
      (! Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)))
    {
      *CmpCommon::diags() << DgSqlCode(-4261)
                          << DgSchemaName(schNamePart);
    
      return -1;
    }

  if (NOT hbaseMapSpecified)
    retcode = existsInSeabaseMDTable
      (cliInterface, 
       catNamePart, schNamePart, objNamePart,
       objectType, //COM_BASE_TABLE_OBJECT,
       (Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL) 
        ? FALSE : TRUE),
       checkForValidHbaseName, TRUE);
  if (retcode < 0)
    {
      return -1; // error
    }
  
  if (retcode == 1)
    return 1; // exists

  if ((retcode == 0) && // does not exist
      (NOT schNameSpecified))
    {
      retcode = existsInSeabaseMDTable
        (cliInterface, 
         catNamePart, HBASE_EXT_MAP_SCHEMA, objNamePart,
         objectType,
         (Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL) 
          ? FALSE : TRUE),
         checkForValidHbaseName, TRUE);
      if (retcode < 0)
        {
          return -1; // error
        }
      
      if (retcode != 0) // exists
        {
          schNamePart = HBASE_EXT_MAP_SCHEMA;
          ComAnsiNamePart mapSchAnsiName
            (schNamePart, ComAnsiNamePart::INTERNAL_FORMAT);
          tableName.setSchemaNamePart(mapSchAnsiName);
          extTableName = tableName.getExternalName(TRUE);
          tabName = tableName.getExternalName();
          return 1; // exists
        }
    }

  return 0; // does not exist
}

// RETURN:  -1, no need to cleanup.  -2, caller need to call cleanup 
//                  0, all ok.
short CmpSeabaseDDL::dropSeabaseTable2(
                                       ExeCliInterface *cliInterface,
                                       StmtDDLDropTable * dropTableNode,
                                       NAString &currCatName, NAString &currSchName)
{
  Lng32 cliRC = 0;
  Lng32 retcode = 0;

  NAString tabName = (NAString&)dropTableNode->getTableName();

  ComObjectName tableName(tabName, COM_TABLE_NAME);
  ComObjectName volTabName; 
  ComAnsiNamePart currCatAnsiName(currCatName);
  ComAnsiNamePart currSchAnsiName(currSchName);

  NABoolean catNameSpecified = 
    (NOT dropTableNode->getOrigTableNameAsQualifiedName().getCatalogName().isNull());
  NABoolean schNameSpecified = 
    (NOT dropTableNode->getOrigTableNameAsQualifiedName().getSchemaName().isNull());

  Int32 hbaseDropCreate = CmpCommon::getDefaultNumeric(TRAF_HBASE_DROP_CREATE);

  tableName.applyDefaults(currCatAnsiName, currSchAnsiName);

  NABoolean isHive = FALSE;
  if (tableName.getCatalogNamePartAsAnsiString() == HIVE_SYSTEM_CATALOG)
    isHive = TRUE;
     
  NABoolean hbmapExtTable = FALSE;
  if (((tableName.getCatalogNamePartAsAnsiString() == HBASE_SYSTEM_CATALOG) &&
       (tableName.getSchemaNamePartAsAnsiString(TRUE) == HBASE_MAP_SCHEMA)) ||
      (dropTableNode->isExternal()))
    {
      if ((tableName.getCatalogNamePartAsAnsiString() == HBASE_SYSTEM_CATALOG) ||
          (dropTableNode->isExternal() && (NOT isHive)))
        hbmapExtTable = TRUE;
        
      // Convert the native name to its Trafodion form
      tabName = ComConvertNativeNameToTrafName
        (tableName.getCatalogNamePartAsAnsiString(),
         tableName.getSchemaNamePartAsAnsiString(),
         tableName.getObjectNamePartAsAnsiString());
                               
      ComObjectName adjustedName(tabName, COM_TABLE_NAME);
      tableName = adjustedName;

      if (dropTableNode->isExternal() && (NOT isHive))
        {
          NABoolean hiveName = FALSE;
          if (ComIsTrafodionExternalSchemaName(
                   tableName.getSchemaNamePartAsAnsiString(),
                   &hiveName))
            {
              if (hiveName)
                hbmapExtTable = FALSE;
            }
        }
    }

  NAString catalogNamePart = tableName.getCatalogNamePartAsAnsiString();
  NAString schemaNamePart = tableName.getSchemaNamePartAsAnsiString(TRUE);
  NAString objectNamePart = tableName.getObjectNamePartAsAnsiString(TRUE);
  NAString extTableName = tableName.getExternalName(TRUE);

  // allowExternalTables: true to allow an NATable entry to be dropped for an external table
  BindWA bindWA(ActiveSchemaDB(), CmpCommon::context(), TRUE/*inDDL*/);

  if ((isSeabaseReservedSchema(tableName)) &&
      (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)))
    {
      *CmpCommon::diags() << DgSqlCode(-CAT_USER_CANNOT_DROP_SMD_TABLE)
                          << DgTableName(extTableName);

      processReturn();

      return -1;
    }

  NABoolean isVolatile = FALSE;

  if ((dropTableNode->isVolatile()) &&
      (CmpCommon::context()->sqlSession()->volatileSchemaInUse()))
    {
      volTabName = tableName;
      isVolatile = TRUE;
    }
  
  if ((NOT dropTableNode->isVolatile()) &&
      (CmpCommon::context()->sqlSession()->volatileSchemaInUse()))
    {

      // updateVolatileQualifiedName qualifies the object name with a 
      // volatile catalog and schema name (if a volatile schema exists)
      QualifiedName *qn =
        CmpCommon::context()->sqlSession()->
        updateVolatileQualifiedName
        (dropTableNode->getOrigTableNameAsQualifiedName().getObjectName());
      
      // don't believe it is possible to get a null pointer returned
      if (qn == NULL)
        {
          *CmpCommon::diags()
            << DgSqlCode(-CAT_UNABLE_TO_DROP_OBJECT)
            << DgTableName(dropTableNode->getOrigTableNameAsQualifiedName().
                           getQualifiedNameAsAnsiString(TRUE));

          processReturn();

          return -1;
        }
      
        volTabName = qn->getQualifiedNameAsAnsiString();
        volTabName.applyDefaults(currCatAnsiName, currSchAnsiName);

        NAString vtCatNamePart = volTabName.getCatalogNamePartAsAnsiString();
        NAString vtSchNamePart = volTabName.getSchemaNamePartAsAnsiString(TRUE);
        NAString vtObjNamePart = volTabName.getObjectNamePartAsAnsiString(TRUE);

        retcode = existsInSeabaseMDTable(cliInterface, 
                                         vtCatNamePart, vtSchNamePart, vtObjNamePart,
                                         COM_BASE_TABLE_OBJECT);

        if (retcode < 0)
          {
            processReturn();
            
            return -1;
          }

        if (retcode == 1)
          {
            // table found in volatile schema
            // Validate volatile table name.
            if (CmpCommon::context()->sqlSession()->
                validateVolatileQualifiedName
                (dropTableNode->getOrigTableNameAsQualifiedName()))
              {
                // Valid volatile table. Drop it.
                tabName = volTabName.getExternalName(TRUE);

                catalogNamePart = vtCatNamePart;
                schemaNamePart = vtSchNamePart;
                objectNamePart = vtObjNamePart;

                isVolatile = TRUE;
              }
            else
              {
                // volatile table found but the name is not a valid
                // volatile name. Look for the input name in the regular
                // schema.
                // But first clear the diags area.
                CmpCommon::diags()->clear();
              }
          }
        else
          {
            CmpCommon::diags()->clear();
          }
      }
 
  retcode = lockObjectDDL(dropTableNode->getTableNameAsQualifiedName(),
			  COM_BASE_TABLE_OBJECT,
			  dropTableNode->isVolatile(),
			  dropTableNode->isExternal());
  if (retcode < 0) {
    processReturn();
    return -1;
  }

  retcode = lookForTableInMD(cliInterface, 
                             catalogNamePart, schemaNamePart, objectNamePart,
                             (schNameSpecified && (NOT hbmapExtTable)),
                             hbmapExtTable,
                             tableName, tabName, extTableName, 
                             COM_BASE_TABLE_OBJECT, FALSE);
  if (retcode < 0)
    {
      processReturn();

      return -1;
    }

  if (retcode == 0) // does not exist
    {
      if (NOT dropTableNode->dropIfExists())
        {
          CmpCommon::diags()->clear();

          if (isVolatile)
            *CmpCommon::diags() << DgSqlCode(-CAT_OBJECT_DOES_NOT_EXIST_IN_TRAFODION)
                                << DgString0(objectNamePart);
          else if (hbmapExtTable)
            *CmpCommon::diags() << DgSqlCode(-1386)
                                << DgString0(objectNamePart);
          else
            *CmpCommon::diags() << DgSqlCode(-CAT_OBJECT_DOES_NOT_EXIST_IN_TRAFODION)
                                << DgString0(extTableName);
        }

      processReturn();

      return -1;
    }

  // Check to see if the user has the authority to drop the table
  ComObjectName verifyName;
  if (isVolatile)
     verifyName = volTabName;
  else
     verifyName = tableName;

  if (CmpCommon::getDefault(TRAF_RELOAD_NATABLE_CACHE) == DF_OFF)
    ActiveSchemaDB()->getNATableDB()->useCache();

  // save the current parserflags setting
  ULng32 savedParserFlags = Get_SqlParser_Flags (0xFFFFFFFF);
  Set_SqlParser_Flags(ALLOW_VOLATILE_SCHEMA_IN_TABLE_NAME);

  objectNamePart = tableName.getObjectNamePartAsAnsiString(TRUE);

  CorrName cn(objectNamePart,
              STMTHEAP,
              schemaNamePart,
              catalogNamePart);

  if (dropTableNode->isExternal())
    bindWA.setExternalTableDrop(TRUE);
  NATable *naTable = bindWA.getNATableInternal(cn, TRUE, NULL, TRUE); 

  bindWA.setExternalTableDrop(FALSE);
 
  // Restore parser flags settings to what they originally were
  Set_SqlParser_Flags (savedParserFlags);
  if (naTable == NULL || bindWA.errStatus())
    {
      if (NOT dropTableNode->dropIfExists())
        {
         if (!CmpCommon::diags()->contains(-4082))
           {
             if (isVolatile)
               *CmpCommon::diags() << DgSqlCode(-CAT_OBJECT_DOES_NOT_EXIST_IN_TRAFODION)
                                   << DgString0(objectNamePart);
             else
               *CmpCommon::diags() << DgSqlCode(-CAT_OBJECT_DOES_NOT_EXIST_IN_TRAFODION)
                                   << DgString0(extTableName);
           }
        }
      else
        CmpCommon::diags()->clear();

      processReturn();

      return -1;
    }

  if (naTable->isPartitionEntityTable() && NOT dropTableNode->isPartition())
  {
    *CmpCommon::diags() << DgSqlCode(-4265)
                              << DgString0(extTableName);
    processReturn();
    return -1;
  }

  if (naTable->isPartitionV2Table())
  {
  const NAPartitionArray &partitionArray = naTable->getPartitionArray();
  for (int i = 0; i < partitionArray.entries(); i++)
    {
      NAString partitionBtName(partitionArray[i]->getPartitionEntityName());
      cliRC = dropSeabasePartitionTable(cliInterface, catalogNamePart, schemaNamePart, partitionBtName, dropTableNode->getDropBehavior() == COM_CASCADE_DROP_BEHAVIOR);
      if (cliRC < 0)
      {
        cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());

        *CmpCommon::diags() << DgSqlCode(-4266)
                          << DgString0(extTableName)
                          << DgString1("drop partition base table");
        processReturn();
        return -1;
      }
    }
  }

  Int64 objUID = naTable->objectUid().castToInt64();
  Int64 dataUID = naTable->objDataUID().castToInt64();
  Int64 schUID = naTable->getSchemaUid().castToInt64();
  Int64 originuid = objUID;
  if (isLockedForBR(objUID, schUID, cliInterface, 
                    cn))
    {
      return -1;
    }

  if ((dropTableNode->isVolatile()) && 
      (NOT CmpCommon::context()->sqlSession()->isValidVolatileSchemaName(schemaNamePart)))
    {
      *CmpCommon::diags() << DgSqlCode(-1279);

      processReturn();

      return -1;
    }

  // Table exists, update the epoch value to lock out other user access
  if (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL))
    {
      retcode = ddlSetObjectEpoch(naTable, STMTHEAP);
      if (retcode < 0)
        {
          processReturn();
          return -1;
        }
    }

  // Make sure user has necessary privileges to perform drop
  if (!isDDLOperationAuthorized(SQLOperation::DROP_TABLE,
                                naTable->getOwner(), naTable->getSchemaOwner(),
                                (int64_t)naTable->objectUid().get_value(), 
                                COM_BASE_TABLE_OBJECT, naTable->getPrivInfo()))
  {
     *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);

     processReturn ();
     
     return -1;
  }


  NABoolean isMonarch = naTable->isMonarch();
  ExpHbaseInterface * ehi = allocEHI(naTable->storageType());
  if (ehi == NULL)
    {
      processReturn();
      return -1;
    }

  // if this table does not exist in hbase but exists in metadata, return error.
  // This is an internal inconsistency which needs to be fixed by running cleanup.

  // If this is an external (native HIVE or HBASE) table, then skip
  NAString tableNamespace = naTable->getNamespace();
  const NAString extNameForHbase = genHBaseObjName(
       catalogNamePart, schemaNamePart, objectNamePart,
       tableNamespace, dataUID);
  
  if (!isSeabaseExternalSchema(catalogNamePart, schemaNamePart))
    {
      HbaseStr hbaseTable;
      NAString nameForUID;

      hbaseTable.val = (char*)extNameForHbase.data(),
      hbaseTable.len = extNameForHbase.length();

      if ((NOT isVolatile)&& (ehi->exists(hbaseTable) == 0) && NOT naTable->isPartitionV2Table()) // does not exist in hbase
        {
          *CmpCommon::diags() << DgSqlCode(-4254)
                              << DgString0(extTableName);
      
          deallocEHI(ehi); 
          processReturn();

          return -1;
        }
    }
  if (naTable->isPartitionV2Table())
  {
    cliRC = deleteSeabasePartitionMDData(cliInterface, objUID);
    if (cliRC < 0)
    {
      *CmpCommon::diags() << DgSqlCode(-4266)
                       << DgString0(extTableName)
                       << DgString1("delete partition property");
      processReturn();
      return -1;
    }
  }

  if (naTable->isPartitionEntityTable() && dropTableNode->isPartition())
  {
    cliRC = updateSeabasePartitionbtMDDataPosition(cliInterface, naTable->objectUid().castToInt64());
    if (cliRC < 0)
    {
      *CmpCommon::diags() << DgSqlCode(-4266)
                        << DgString0(extTableName)
                        << DgString1("update MD partitions");
      processReturn();
      return -1;
    }

    cliRC = deleteSeabasePartitionbtMDData(cliInterface, naTable->objectUid().castToInt64());
    if (cliRC < 0)
    {
      *CmpCommon::diags() << DgSqlCode(-4266)
                        << DgString0(extTableName)
                        << DgString1("delete MD partitions");
      processReturn();
          return -1;
        }
    }
  
  /* process hiatus */
  if (naTable && naTable->incrBackupEnabled() && NOT naTable->isPartitionV2Table())
    {
      hiatusObjectName_ = extNameForHbase;

      //If backups already dropped, there is a chance a backup
      //record may not exist. In this case ignore the error.
      if (setHiatus(hiatusObjectName_, FALSE, FALSE, TRUE))
        {
          deallocEHI(ehi);
          // setHiatus has set the diags area
          processReturn();
          return -1;
        }
    }
  
  Queue * usingViewsQueue = NULL;
  if (dropTableNode->getDropBehavior() == COM_RESTRICT_DROP_BEHAVIOR)
    {
      NAString usingObjName;
      cliRC = getUsingObject(cliInterface, objUID, usingObjName);
      if (cliRC < 0)
        {
          deallocEHI(ehi); 
          processReturn();
          
          return -1;
        }

      if (cliRC != 100) // found an object
        {
          *CmpCommon::diags() << DgSqlCode(-CAT_DEPENDENT_VIEW_EXISTS)
                              << DgTableName(usingObjName);

          deallocEHI(ehi); 
          processReturn();

          return -1;
        }
    }
  else if (dropTableNode->getDropBehavior() == COM_CASCADE_DROP_BEHAVIOR)
    {
      cliRC = getAllUsingViews(cliInterface, 
                               catalogNamePart, schemaNamePart, objectNamePart,
                               usingViewsQueue);
      if (cliRC < 0)
        {
          deallocEHI(ehi); 
          processReturn();
          
          return -1;
        }
    }

  const AbstractRIConstraintList &uniqueList = naTable->getUniqueConstraints();
      
  // return error if cascade is not specified and a referential constraint exists on
  // any of the unique constraints.
  
  if (dropTableNode->getDropBehavior() == COM_RESTRICT_DROP_BEHAVIOR)
    {
      for (Int32 i = 0; i < uniqueList.entries(); i++)
        {
          AbstractRIConstraint *ariConstr = uniqueList[i];
          
          if (ariConstr->getOperatorType() != ITM_UNIQUE_CONSTRAINT)
            continue;
          
          UniqueConstraint * uniqConstr = (UniqueConstraint*)ariConstr;

          if (uniqConstr->hasRefConstraintsReferencingMe())
            {
              const ComplementaryRIConstraint * rc = uniqConstr->getRefConstraintReferencingMe(0);
              
              if (rc->getTableName() != naTable->getTableName())
                {
                  const NAString &constrName = 
                    (rc ? rc->getConstraintName().getObjectName() : " ");
                  *CmpCommon::diags() << DgSqlCode(-1059)
                                      << DgConstraintName(constrName);
                  
                  deallocEHI(ehi); 
                  processReturn();
                  
                  return -1;
                }
            }
        }
    }

  // Drop referencing objects
  char query[4000];
  // drop trigger in this table
  memset(query, 0, 4000);
  str_sprintf(query, "select '\"' || trim(catalog_name) || '\"' || '.' || '\"' || trim(schema_name) || '\"' || '.' || '\"' || trim(object_name) || '\"' \
                      from %s.\"%s\".%s O, %s.\"%s\".%s R where \
                      O.object_uid = R.udr_uid and R.library_uid = %ld and R.udr_type = 'TR'; ",
	      getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS,
	      getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_ROUTINES,
	      objUID);

  Queue * triggerss = NULL;
  cliRC = cliInterface->fetchAllRows(triggerss, query, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0)
    {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      deallocEHI(ehi);
      processReturn();
      return -2;
    }

  triggerss->position();
  {
      PushAndSetSqlParserFlags savedParserFlags(INTERNAL_QUERY_FROM_EXEUTIL);
      for (int i = 0; i < triggerss->numEntries(); i++)
        {
          OutputInfo * trigger = (OutputInfo*) triggerss->getNext();
          char* objname = trigger->get(0);
          memset(query, 0, 4000);
          str_sprintf(query, "drop trigger %s;",
              objname);      
          cliRC = cliInterface->executeImmediate(query);
          if (cliRC < 0)
            {
              cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
              deallocEHI(ehi);
              processReturn();
              return -2;
           }
        }
  }
  memset(query, 0, 4000);

  // drop the views.
  // usingViewsQueue contain them in ascending order of their create
  // time. Drop them from last to first.
  if (usingViewsQueue)
    {
      Int64 redefTime = NA_JulianTimestamp();
      for (int idx = usingViewsQueue->numEntries()-1; idx >= 0; idx--)
        {
          OutputInfo * vi = (OutputInfo*)usingViewsQueue->get(idx);
          char * viewName = vi->get(0);
          Int64 usingViewUID = *(Int64*)vi->get(1);
 
          if (dropOneTableorView(*cliInterface,viewName,COM_VIEW_OBJECT,false))
            {
              deallocEHI(ehi); 
              processReturn();
              
              return -1;
            }

          // add to list of affected DDL operations
          DDLObjInfo ddlObj(viewName, usingViewUID, COM_VIEW_OBJECT);
          ddlObj.setQIScope(REMOVE_FROM_ALL_USERS);
          ddlObj.setRedefTime(redefTime);
          ddlObj.setDDLOp(FALSE);
          CmpCommon::context()->ddlObjsList().insertEntry(ddlObj);
        }
    }

  // drop all referential constraints referencing me.
  for (Int32 i = 0; i < uniqueList.entries(); i++)
    {
      AbstractRIConstraint *ariConstr = uniqueList[i];
      
      if (ariConstr->getOperatorType() != ITM_UNIQUE_CONSTRAINT)
        continue;

      UniqueConstraint * uniqConstr = (UniqueConstraint*)ariConstr;

      // We will only reach here is cascade option is specified.
      // drop all constraints referencing me.
      if (uniqConstr->hasRefConstraintsReferencingMe())
        {
          for (Lng32 j = 0; j < uniqConstr->getNumRefConstraintsReferencingMe(); j++)
            {
              const ComplementaryRIConstraint * rc = 
               uniqConstr->getRefConstraintReferencingMe(j);

              // Update the epoch value of the referenced table to lock out other user access
              CorrName cn2(rc->getTableName().getObjectName().data(),
                           STMTHEAP,
                           rc->getConstraintName().getSchemaName().data(),
                           rc->getConstraintName().getCatalogName().data());

	      if (lockObjectDDL(cn2.getQualifiedNameObj(),
				COM_BASE_TABLE_OBJECT,
				isVolatile,
				cn2.isExternal()) < 0) {
		deallocEHI(ehi);
		processReturn();
		return -1;
	      }

              NABoolean selfRef = (cn2.getExposedNameAsAnsiString() == extTableName);
              NATable *refdNaTable = bindWA.getNATable(cn2);
              if (refdNaTable && !selfRef && !Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL))
                {
                  if (ddlSetObjectEpoch(refdNaTable, STMTHEAP, TRUE /*allow reads*/) < 0)
                    {
                      deallocEHI(ehi); 
                      processReturn();
                      return -1;
                    }
                }

              str_sprintf(query, "alter table \"%s\".\"%s\".\"%s\" drop constraint \"%s\".\"%s\".\"%s\"",
                          rc->getTableName().getCatalogName().data(),
                          rc->getTableName().getSchemaName().data(),
                          rc->getTableName().getObjectName().data(),
                          rc->getConstraintName().getCatalogName().data(),
                          rc->getConstraintName().getSchemaName().data(),
                          rc->getConstraintName().getObjectName().data());

              cliRC = cliInterface->executeImmediate(query);

              if (cliRC < 0)
                {
                  cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
                  deallocEHI(ehi); 
                  processReturn();
                  
                  return -2;
                }
              
              // update referencing constraint
              if (!selfRef)
                {
                  if (refdNaTable && refdNaTable->isStoredDesc() &&
                      !Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL))
                    insertIntoSharedCacheList(rc->getTableName().getCatalogName(),
                                              rc->getTableName().getSchemaName().data(),
                                              rc->getTableName().getObjectName().data(),
                                              COM_BASE_TABLE_OBJECT, SharedCacheDDLInfo::DDL_UPDATE);
                }
            } // for
        } // if
    } // for

  for (Int32 i = 0; i < uniqueList.entries(); i++)
    {
      AbstractRIConstraint *ariConstr = uniqueList[i];
      
      if (ariConstr->getOperatorType() != ITM_UNIQUE_CONSTRAINT)
        continue;

      UniqueConstraint * uniqConstr = (UniqueConstraint*)ariConstr;

      const NAString& constrCatName = 
        uniqConstr->getConstraintName().getCatalogName();

      const NAString& constrSchName = 
        uniqConstr->getConstraintName().getSchemaName();
      
      NAString constrObjName = 
        (NAString) uniqConstr->getConstraintName().getObjectName();
      
      // Get the constraint UID
      Int64 constrUID = -1;

      // If the table being dropped is from a metadata schema, setup 
      // an UniqueConstraint entry for the table being dropped describing its 
      // primary key.  This is temporary until metadata is changed to create 
      // primary keys with a known name.
      if (isSeabasePrivMgrMD(catalogNamePart, schemaNamePart) ||
          isSeabaseMD(catalogNamePart, schemaNamePart, objectNamePart) ||
          isTrafBackupSchema(catalogNamePart, schemaNamePart))
        {
          assert (uniqueList.entries() == 1);
          assert (uniqueList[0]->getOperatorType() == ITM_UNIQUE_CONSTRAINT);
          UniqueConstraint * uniqConstr = (UniqueConstraint*)uniqueList[0];
          assert (uniqConstr->isPrimaryKeyConstraint());
          NAString adjustedConstrName;
          if (getPKeyInfoForTable (catalogNamePart.data(),
                                   schemaNamePart.data(),
                                   objectNamePart.data(),
                                   cliInterface,
                                   constrObjName,
                                   constrUID) == -1)
            {
              deallocEHI(ehi); 
              processReturn();
              return -1;
            }
            
        }

      // Read the metadata to get the constraint UID
      else
        {
            constrUID = getObjectUID(cliInterface,
                                     constrCatName.data(), constrSchName.data(), constrObjName.data(),
                                     (uniqConstr->isPrimaryKeyConstraint() ?
                                      COM_PRIMARY_KEY_CONSTRAINT_OBJECT_LIT :
                                      COM_UNIQUE_CONSTRAINT_OBJECT_LIT));                
          if (constrUID == -1)
            {
              deallocEHI(ehi); 
              processReturn();
              return -1;
            }
        }

      if (deleteConstraintInfoFromSeabaseMDTables(cliInterface,
                                                  naTable->objectUid().castToInt64(),
                                                  0,
                                                  constrUID,
                                                  0,
                                                  constrCatName,
                                                  constrSchName,
                                                  constrObjName,
                                                  (uniqConstr->isPrimaryKeyConstraint() ?
                                                   COM_PRIMARY_KEY_CONSTRAINT_OBJECT :
                                                   COM_UNIQUE_CONSTRAINT_OBJECT)))
        {
          deallocEHI(ehi); 
          processReturn();
          
          return -1;
        }
     }

  // drop all referential constraints from metadata
  const AbstractRIConstraintList &refList = naTable->getRefConstraints();
  
  for (Int32 i = 0; i < refList.entries(); i++)
    {
      AbstractRIConstraint *ariConstr = refList[i];
      
      if (ariConstr->getOperatorType() != ITM_REF_CONSTRAINT)
        continue;

      RefConstraint * refConstr = (RefConstraint*)ariConstr;

      // if self referencing constraint, then it was already dropped as part of
      // dropping 'ri constraints referencing me' earlier.
      if (refConstr->selfRef())
        continue;

      const NAString& constrCatName = 
        refConstr->getConstraintName().getCatalogName();

      const NAString& constrSchName = 
        refConstr->getConstraintName().getSchemaName();
      
      const NAString& constrObjName = 
        refConstr->getConstraintName().getObjectName();
      
      Int64 constrUID = getObjectUID(cliInterface,
                                     constrCatName.data(), constrSchName.data(), constrObjName.data(),
                                     COM_REFERENTIAL_CONSTRAINT_OBJECT_LIT);             
      if (constrUID < 0)
        {
          deallocEHI(ehi); 
          processReturn();
          
          return -1;
        }

      NATable *otherNaTable = NULL;
      CorrName otherCN(refConstr->getUniqueConstraintReferencedByMe().getTableName());

      if (lockObjectDDL(otherCN.getQualifiedNameObj(),
                        COM_BASE_TABLE_OBJECT,
                        otherCN.isVolatile(),
                        otherCN.isExternal()) < 0) {
        deallocEHI(ehi);
        processReturn();
        return -1;
      }

      otherNaTable = bindWA.getNATable(otherCN);
      if (otherNaTable == NULL || bindWA.errStatus())
        {
          deallocEHI(ehi); 
          
          processReturn();
          
          return -1;
        }
      
      if (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL))
        {
          if (ddlSetObjectEpoch(otherNaTable, STMTHEAP, TRUE /*allow reads*/) < 0)
            {
              deallocEHI(ehi);
              processReturn();
              return -1;
            }
        }

      Int64 schUID = otherNaTable->getSchemaUid().castToInt64();
      if (isLockedForBR(otherNaTable->objectUid().castToInt64(), 
                        schUID,
                        cliInterface,
                        otherCN))
        {
          processReturn();
          return -1;
        }

      AbstractRIConstraint * otherConstr = 
        refConstr->findConstraint(&bindWA, refConstr->getUniqueConstraintReferencedByMe());

      if (otherConstr == NULL)
        {
          NAString msg ("Unable to get referenced unique constraint for table: ");
          msg += otherNaTable->getTableName().getQualifiedNameAsAnsiString();
          SEABASEDDL_INTERNAL_ERROR(msg.data());
          processReturn();
          return -1;
        }

      const NAString& otherSchName = 
        otherConstr->getConstraintName().getSchemaName();
      
      const NAString& otherConstrName = 
        otherConstr->getConstraintName().getObjectName();
      
      Int64 otherConstrUID = getObjectUID(cliInterface,
                                          constrCatName.data(), otherSchName.data(), otherConstrName.data(),
                                          COM_UNIQUE_CONSTRAINT_OBJECT_LIT );
      if (otherConstrUID < 0)
        {
          CmpCommon::diags()->clear();
          otherConstrUID = getObjectUID(cliInterface,
                                        constrCatName.data(), otherSchName.data(), otherConstrName.data(),
                                        COM_PRIMARY_KEY_CONSTRAINT_OBJECT_LIT );
          if (otherConstrUID < 0)
            {
              deallocEHI(ehi); 
              processReturn();
              
              return -1;
            }
        }
      
      if (deleteConstraintInfoFromSeabaseMDTables(cliInterface,
                                                  naTable->objectUid().castToInt64(),
                                                  otherNaTable->objectUid().castToInt64(),
                                                  constrUID,
                                                  otherConstrUID,
                                                  constrCatName,
                                                  constrSchName,
                                                  constrObjName,
                                                  COM_REFERENTIAL_CONSTRAINT_OBJECT))
        {
          deallocEHI(ehi); 
          processReturn();
          
          return -1;
        }    

      if (updateObjectRedefTime
          (cliInterface,
           otherNaTable->getTableName().getCatalogName(),
           otherNaTable->getTableName().getSchemaName(),
           otherNaTable->getTableName().getObjectName(),
           COM_BASE_TABLE_OBJECT_LIT, -1, 
           otherNaTable->objectUid().castToInt64(),
           otherNaTable->isStoredDesc(), otherNaTable->storedStatsAvail()))
        {
          processReturn();
          deallocEHI(ehi);
          
          return -1;      
        }

      // Add entry to list of DDL operations
      if (otherNaTable->isStoredDesc() && 
          (! Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)))
        {
          insertIntoSharedCacheList(otherNaTable->getTableName().getCatalogName(), 
                                    otherNaTable->getTableName().getSchemaName(), 
                                    otherNaTable->getTableName().getObjectName(),
                                    COM_BASE_TABLE_OBJECT, SharedCacheDDLInfo::DDL_UPDATE);
          // Remove item from shared cache (if auto commit is off)
          if (!GetCliGlobals()->currContext()->getTransaction()->autoCommit())
          {
              int32_t diagsMark = CmpCommon::diags()->mark();

              str_sprintf(query, "alter trafodion metadata shared cache for table %s update internal",
                          otherNaTable->getTableName().getQualifiedNameAsAnsiString().data());

              NAString msg(query);
              msg.prepend("Removing object for drop table: ");
              QRLogger::log(CAT_SQL_EXE, LL_WARN, "%s", msg.data());

              cliRC = cliInterface->executeImmediate(query);

              // If unable to remove from shared cache, it may not exist, continue 
              // If the operation is later rolled back, this table will not be added
              // back to shared cache - TBD
              CmpCommon::diags()->rewind(diagsMark);
          }
        }
    }

  // drop all check constraints from metadata if 'no check' is not specified.
  if (NOT (dropTableNode->getDropBehavior() == COM_NO_CHECK_DROP_BEHAVIOR) &&
      NOT (naTable->isPartitionEntityTable() && dropTableNode->isPartition()))
    {
      const CheckConstraintList &checkList = naTable->getCheckConstraints();
      for (Int32 i = 0; i < checkList.entries(); i++)
        {
          CheckConstraint *checkConstr = checkList[i];
          
          const NAString& constrCatName = 
            checkConstr->getConstraintName().getCatalogName();
          
          const NAString& constrSchName = 
            checkConstr->getConstraintName().getSchemaName();
          
          const NAString& constrObjName = 
            checkConstr->getConstraintName().getObjectName();
          
          Int64 constrUID = getObjectUID(cliInterface,
                                         constrCatName.data(), constrSchName.data(), constrObjName.data(),
                                         COM_CHECK_CONSTRAINT_OBJECT_LIT);
          if (constrUID < 0)
            {
              deallocEHI(ehi); 
              processReturn();
              
              return -1;
            }
          
          if (deleteConstraintInfoFromSeabaseMDTables(cliInterface,
                                                      naTable->objectUid().castToInt64(),
                                                      0,
                                                      constrUID,
                                                      0,
                                                      constrCatName,
                                                      constrSchName,
                                                      constrObjName,
                                                      COM_CHECK_CONSTRAINT_OBJECT))
            {
              deallocEHI(ehi); 
              processReturn();
              
              return -1;
            }
        }
    }

  const NAFileSetList &indexList = naTable->getIndexList();

  Queue * indexInfoQueue = NULL;

  std::map<NAString, NAString> tempIndexMap;
  if (hbaseDropCreate != 2) 
    {
      // first drop all index objects from metadata.
      if (getAllIndexes(cliInterface, objUID, TRUE, indexInfoQueue))
        {
          deallocEHI(ehi); 
          processReturn();
          return -1;
        }
      
      SQL_QIKEY *qiKeys = new (STMTHEAP) SQL_QIKEY[indexInfoQueue->numEntries()];
      indexInfoQueue->position();
      for (int idx = 0; idx < indexInfoQueue->numEntries(); idx++)
        {
          OutputInfo * vi = (OutputInfo*)indexInfoQueue->getNext(); 
          NAString * indexNameForUID = NULL;
          
          NAString idxCatName = (char*)vi->get(0);
          NAString idxSchName = (char*)vi->get(1);
          NAString idxObjName = (char*)vi->get(2);
          
          // set up a qiKey for this index, later we will removed the
          // index cache entry from concurrent processes
          Int64 objUID = *(Int64*)vi->get(3);
          qiKeys[idx].ddlObjectUID = objUID;
          qiKeys[idx].operation[0] = 'O';
          qiKeys[idx].operation[1] = 'R';
          
          NAString qCatName = "\"" + idxCatName + "\"";
          NAString qSchName = "\"" + idxSchName + "\"";
          NAString qObjName = "\"" + idxObjName + "\"";
          
          ComObjectName coName(qCatName, qSchName, qObjName);
          NAString ansiName = coName.getExternalName(TRUE);
          
          // Add entry to list of DDL operations
          if (naTable->isStoredDesc() && (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)))
            insertIntoSharedCacheList(idxCatName, idxSchName, idxObjName,
                                      COM_INDEX_OBJECT, SharedCacheDDLInfo::DDL_DELETE);

          if (dropSeabaseObject(ehi, ansiName,
                                idxCatName, idxSchName, COM_INDEX_OBJECT, 
                                dropTableNode->ddlXns(),
                                TRUE, FALSE, isMonarch, tableNamespace, TRUE, NULL, &indexNameForUID, 1))
            {
              NADELETEBASIC (qiKeys, STMTHEAP);
              
              deallocEHI(ehi); 
              processReturn();
              
              return -1;
            }
            if (indexNameForUID)
            {
              tempIndexMap[ansiName] = *indexNameForUID;
            }
          
        } // for
      
      // Remove index entries from other processes cache
      // Fix for bug 1396774 & bug 1396746
      if (indexInfoQueue->numEntries() > 0)
        SQL_EXEC_SetSecInvalidKeys(indexInfoQueue->numEntries(), qiKeys);
      NADELETEBASIC (qiKeys, STMTHEAP);
    }

  // if there is an identity column, drop sequence corresponding to it.
  NABoolean found = FALSE;
  Lng32 idPos = 0;
  NAColumn *col = NULL;
  while ((NOT found) && (idPos < naTable->getColumnCount()))
    {

      col = naTable->getNAColumnArray()[idPos];
      if (col->isIdentityColumn())
        {
          found = TRUE;
          continue;
        }

      idPos++;
    }

  if (found)
    {
      NAString seqName;
      SequenceGeneratorAttributes::genSequenceName
        (catalogNamePart, schemaNamePart, objectNamePart, col->getColName(),
         seqName);
      
      char buf[4000];
      str_sprintf(buf, "drop sequence %s.\"%s\".\"%s\"",
                  catalogNamePart.data(), schemaNamePart.data(), seqName.data());
      
      cliRC = cliInterface->executeImmediate(buf);
      if (cliRC < 0 && cliRC != -CAT_OBJECT_DOES_NOT_EXIST_IN_TRAFODION)
        {
          cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
          
          deallocEHI(ehi); 
          
          processReturn();
          
          return -1;
        }

      CorrName seqn(seqName, STMTHEAP, schemaNamePart, catalogNamePart);
      seqn.setSpecialType(ExtendedQualName::SG_TABLE);
      ActiveSchemaDB()->getNATableDB()->removeNATable
        (seqn, 
         ComQiScope::REMOVE_FROM_ALL_USERS, COM_SEQUENCE_GENERATOR_OBJECT,
         dropTableNode->ddlXns(), FALSE);
    }

  // drop SB_HISTOGRAMS and SB_HISTOGRAM_INTERVALS entries, if any
  // if the table that we are dropping itself is not a SB_HISTOGRAMS 
  // or SB_HISTOGRAM_INTERVALS table or sampling tables
  // TBD: need to change once we start updating statistics for external
  // tables
  if (! (tableName.isExternalHive() || tableName.isExternalHbase() ||
         (hbaseDropCreate == 2)))
    {
      if ((!ComIsTrafodionReservedSchemaName(schemaNamePart)) &&
          objectNamePart != "SB_HISTOGRAMS" && 
          objectNamePart != "SB_HISTOGRAM_INTERVALS" &&
          objectNamePart != "SB_PERSISTENT_SAMPLES" &&
          strncmp(objectNamePart.data(),TRAF_SAMPLE_PREFIX,sizeof(TRAF_SAMPLE_PREFIX)) != 0)
          
      {
        if (dropSeabaseStats(cliInterface,
                             catalogNamePart.data(),
                             schemaNamePart.data(),
                             objUID))
        {
          deallocEHI(ehi); 
          processReturn();
          return -1;
        }
    }
  }

  // if metadata drop succeeds, drop indexes from hbase.
  if (indexInfoQueue)
    {
      indexInfoQueue->position();
      for (int idx = 0; idx < indexInfoQueue->numEntries(); idx++)
        {
          OutputInfo * vi = (OutputInfo*)indexInfoQueue->getNext(); 
          
          NAString idxCatName = (char*)vi->get(0);
          NAString idxSchName = (char*)vi->get(1);
          NAString idxObjName = (char*)vi->get(2);
          Int64 objUID = *(Int64*)vi->get(3);
          
          NAString qCatName = "\"" + idxCatName + "\"";
          NAString qSchName = "\"" + idxSchName + "\"";
          NAString qObjName = "\"" + idxObjName + "\"";
          
          ComObjectName coName(qCatName, qSchName, qObjName);
          NAString ansiName = coName.getExternalName(TRUE);

          std::map<NAString, NAString>::iterator indexInfo;

          indexInfo = tempIndexMap.find(ansiName);

          NAString *tmp = &indexInfo->second;
          
          if (dropSeabaseObject(ehi, ansiName,
                                idxCatName, idxSchName, COM_INDEX_OBJECT, 
                                dropTableNode->ddlXns(),
                                FALSE, TRUE, isMonarch, tableNamespace, TRUE, NULL, &tmp, 2))
            {
              deallocEHI(ehi); 
              processReturn();
              
              return -2;
            }
          
          CorrName cni(qObjName, STMTHEAP, qSchName, qCatName);
          ActiveSchemaDB()->getNATableDB()->removeNATable
            (cni,
             ComQiScope::REMOVE_FROM_ALL_USERS, COM_INDEX_OBJECT,
             dropTableNode->ddlXns(), FALSE, objUID);
          cni.setSpecialType(ExtendedQualName::INDEX_TABLE);
          ActiveSchemaDB()->getNATableDB()->removeNATable
            (cni,
             ComQiScope::REMOVE_MINE_ONLY, COM_INDEX_OBJECT,
             dropTableNode->ddlXns(), FALSE);

          if (naTable->readOnlyEnabled() && (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL) || Get_SqlParser_Flags(CONTEXT_IN_DROP_SCHEMA)))
          {
            insertIntoSharedCacheList(qCatName,
                                      qSchName,
                                      qObjName,
                                      COM_BASE_TABLE_OBJECT,
                                      SharedCacheDDLInfo::DDL_DELETE,
                                      SharedCacheDDLInfo::SHARED_DATA_CACHE);

            if (!GetCliGlobals()->currContext()->getTransaction()->autoCommit() || Get_SqlParser_Flags(CONTEXT_IN_DROP_SCHEMA))
            {
              int32_t diagsMark = CmpCommon::diags()->mark();

              str_sprintf(query, "alter trafodion data shared cache for index %s delete internal",
                          ansiName.data());

              NAString msg(query);
              msg.prepend("Removing shared data cache for drop index: ");
              QRLogger::log(CAT_SQL_EXE, LL_WARN, "%s", msg.data());

              cliRC = cliInterface->executeImmediate(query);

              CmpCommon::diags()->rewind(diagsMark);
            }
          }
          
        } // for
    }

  void* res = NULL;
  int s = 0;

  // If blob/clob columns are present, drop all the dependent files.
  const NAColumnArray &nacolArr =  naTable->getNAColumnArray();
  Lng32 numCols = nacolArr.entries();
  


  Lng32 j = 0;


  //delete HDFS cache entries for this table, if any
  //this need to be done before hbase side dropping
  TextVec tableList;
  tableList.push_back(extNameForHbase.data());
  //pass empty string as pool name indicating delete table of all pools.
  ehi->removeTablesFromHDFSCache(tableList, "");

  //Finally drop the table
  NABoolean dropFromMD = TRUE;
  NABoolean dropFromHbase = (NOT tableName.isExternalHbase());

  if (hbaseDropCreate == 0)
    dropFromHbase = FALSE;

  // internal table add X lock
  if (!dropTableNode->isExternal() && lockRequired(ehi, naTable, catalogNamePart, schemaNamePart, objectNamePart, HBaseLockMode::LOCK_X, FALSE/* useHbaseXN */, FALSE/* replSync */, FALSE/* incrementalBackup */, FALSE/* asyncOperation */, TRUE/* noConflictCheck */, TRUE/* registerRegion */))
  {
    deallocEHI(ehi); 
    processReturn();
    
    return -2;
  }
      
  bool updPrivs = (hbaseDropCreate != 0);

  if (hbaseDropCreate == 2)
    {
      // delete MD entries in this thread.
      // Drop hbase object in pthread
      dropFromHbase = FALSE;
    }

  if (dropSeabaseObject(ehi, tabName,
                        currCatName, currSchName, COM_BASE_TABLE_OBJECT,
                        dropTableNode->ddlXns(),
                        dropFromMD, dropFromHbase, isMonarch, tableNamespace, 
                        updPrivs, NULL))
    {
      deallocEHI(ehi); 
      processReturn();
      
      return -2;
    }

  if (naTable->incrBackupEnabled() &&
      !ComIsTrafodionReservedSchemaName(schemaNamePart))
  {
    short retcode = updateSeabaseXDCDDLTable(cliInterface, catalogNamePart.data(), schemaNamePart.data(),
                                             tableName.getObjectNamePartAsAnsiString().data(),
                                             comObjectTypeLit(COM_BASE_TABLE_OBJECT));
    if (retcode < 0)
    {
      deallocEHI(ehi);
      processReturn();
      return -2;
    }
  }

  if (hbaseDropCreate == 2) // pthread drop
    {
      dropTablePthreadArgs *args = new CTXTHEAP dropTablePthreadArgs();

      ComObjectName tableName(tabName);
      ComAnsiNamePart currCatAnsiName(currCatName);
      ComAnsiNamePart currSchAnsiName(currSchName);
      tableName.applyDefaults(currCatAnsiName, currSchAnsiName);
      
      const NAString catalogNamePart = tableName.getCatalogNamePartAsAnsiString();
      const NAString schemaNamePart = tableName.getSchemaNamePartAsAnsiString(TRUE);
      const NAString objectNamePart = tableName.getObjectNamePartAsAnsiString(TRUE);
      NAString extTableName = tableName.getExternalName(TRUE);
      
      const NAString extNameForHbase = genHBaseObjName(
           catalogNamePart, schemaNamePart, objectNamePart,
           tableNamespace);

      args->ehi = ehi;
      //      strcpy(args->tabName, tabName.data());
      strcpy(args->tabName, extNameForHbase.data());
      args->ddlXns = FALSE;
      args->transID = 0;

      s = pthread_create(&lob_ddl_thread_id, NULL,
                         CmpSeabaseDDLdropTable__pthreadDrop, 
                         (void *) args);
      if (s != 0) 
        {
          *CmpCommon::diags() << DgSqlCode(-CAT_CREATE_OBJECT_ERROR)
                              << DgTableName(extTableName);
          deallocEHI(ehi); 	   
          processReturn();
          
          return -1;
        }      
    }

  deallocEHI(ehi); 
  processReturn();

  CorrName cn2(objectNamePart, STMTHEAP, schemaNamePart, catalogNamePart);
  if (hbaseDropCreate != 2)
    {
      ActiveSchemaDB()->getNATableDB()->removeNATable
        (cn2,
         ComQiScope::REMOVE_FROM_ALL_USERS, COM_BASE_TABLE_OBJECT,
         dropTableNode->ddlXns(), FALSE);
    }

  // if hive external table, remove hive entry from NATable cache as well
  if ((dropTableNode->isExternal()) &&
      (dropTableNode->getTableNameAsQualifiedName().isHive()))
    {
      CorrName hcn(dropTableNode->getTableNameAsQualifiedName());
      ActiveSchemaDB()->getNATableDB()->removeNATable
        (hcn,
         ComQiScope::REMOVE_FROM_ALL_USERS, COM_BASE_TABLE_OBJECT,
         dropTableNode->ddlXns(), FALSE);
    }

  for (Int32 i = 0; i < refList.entries(); i++)
    {
      AbstractRIConstraint *ariConstr = refList[i];
      
      if (ariConstr->getOperatorType() != ITM_REF_CONSTRAINT)
        continue;
      
      RefConstraint * refConstr = (RefConstraint*)ariConstr;
      CorrName otherCN(refConstr->getUniqueConstraintReferencedByMe().getTableName());
      
      ActiveSchemaDB()->getNATableDB()->removeNATable
        (otherCN,
         ComQiScope::REMOVE_FROM_ALL_USERS, COM_BASE_TABLE_OBJECT,
         dropTableNode->ddlXns(), FALSE);
    }

  for (Int32 i = 0; i < uniqueList.entries(); i++)
    {
      UniqueConstraint * uniqConstr = (UniqueConstraint*)uniqueList[i];

      // We will only reach here is cascade option is specified.
      // drop all constraints referencing me.
      if (uniqConstr->hasRefConstraintsReferencingMe())
        {
          for (Lng32 j = 0; j < uniqConstr->getNumRefConstraintsReferencingMe(); j++)
            {
              const ComplementaryRIConstraint * rc = 
                uniqConstr->getRefConstraintReferencingMe(j);

              // remove this ref constr entry from natable cache
              CorrName cnr(rc->getTableName().getObjectName().data(), STMTHEAP, 
                           rc->getTableName().getSchemaName().data(),
                           rc->getTableName().getCatalogName().data());
              ActiveSchemaDB()->getNATableDB()->removeNATable
                (cnr,
                 ComQiScope::REMOVE_FROM_ALL_USERS, COM_BASE_TABLE_OBJECT,
                 dropTableNode->ddlXns(), FALSE);
            } // for
          
        } // if
    } // for

  // Add entry to list of DDL operations
  if (naTable->isStoredDesc() && (! Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)))
    {
      insertIntoSharedCacheList(tableName.getCatalogNamePartAsAnsiString(), 
                                tableName.getSchemaNamePartAsAnsiString(),
                                tableName.getObjectNamePartAsAnsiString(),
                                COM_BASE_TABLE_OBJECT, 
                                SharedCacheDDLInfo::DDL_DELETE);

      // Remove item from shared cache (if auto commit is off)
      if (!GetCliGlobals()->currContext()->getTransaction()->autoCommit())
        {
          int32_t diagsMark = CmpCommon::diags()->mark();

          str_sprintf(query, "alter trafodion metadata shared cache for table %s delete internal",
                      naTable->getTableName().getQualifiedNameAsAnsiString().data());

          NAString msg(query);
          msg.prepend("Removing object for drop table: ");
          QRLogger::log(CAT_SQL_EXE, LL_WARN, "%s", msg.data());

          cliRC = cliInterface->executeImmediate(query);

          // If unable to remove from shared cache, it may not exist, continue 
          // If the operation is later rolled back, this table will not be added
          // back to shared cache - TBD
          CmpCommon::diags()->rewind(diagsMark);
        }
    }

  // if is drop drop schema , drop cache immediately
  if (naTable->readOnlyEnabled() && (! Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL) || Get_SqlParser_Flags(CONTEXT_IN_DROP_SCHEMA))) {
      insertIntoSharedCacheList(tableName.getCatalogNamePartAsAnsiString(),
                                tableName.getSchemaNamePartAsAnsiString(),
                                tableName.getObjectNamePartAsAnsiString(),
                                COM_BASE_TABLE_OBJECT,
                                SharedCacheDDLInfo::DDL_DELETE,
                                SharedCacheDDLInfo::SHARED_DATA_CACHE);

    if (!GetCliGlobals()->currContext()->getTransaction()->autoCommit() || Get_SqlParser_Flags(CONTEXT_IN_DROP_SCHEMA)) {
      int32_t diagsMark = CmpCommon::diags()->mark();

      str_sprintf(query, "alter trafodion data shared cache for table %s delete internal",
                      naTable->getTableName().getQualifiedNameAsAnsiString().data());

      NAString msg(query);
      msg.prepend("Removing shared data cache for drop table: ");
      QRLogger::log(CAT_SQL_EXE, LL_WARN, "%s", msg.data());

      cliRC = cliInterface->executeImmediate(query);

      CmpCommon::diags()->rewind(diagsMark);
    }
  }

  return 0;
}

void CmpSeabaseDDL::dropSeabaseTable(
                                     StmtDDLDropTable * dropTableNode,
                                     NAString &currCatName, NAString &currSchName)
{
  Lng32 cliRC = 0;

  NABoolean xnWasStartedHere = FALSE;
  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, 
  CmpCommon::context()->sqlSession()->getParentQid());

  // cannot do pthread lob drop if autocommit is off.
  if (NOT GetCliGlobals()->currContext()->getTransaction()->autoCommit())
    {
      NAString value("0"); // value 0 turns off pthread
      ActiveSchemaDB()->getDefaults().holdOrRestore("traf_lob_parallel_ddl", 1);
      ActiveSchemaDB()->getDefaults().validateAndInsert(
           "traf_lob_parallel_ddl", value, FALSE);      
    }

  if (beginXnIfNotInProgress(&cliInterface, xnWasStartedHere))
    return;

  short rc =
    dropSeabaseTable2(&cliInterface, dropTableNode, currCatName, currSchName);
  ActiveSchemaDB()->getDefaults().holdOrRestore("traf_lob_parallel_ddl", 2);

  if ((CmpCommon::diags()->getNumber(DgSqlCode::ERROR_)) &&
      (rc < 0))
    {
      endXnIfStartedHere(&cliInterface, xnWasStartedHere, -1);

      if (rc == -2) // cleanup before returning error..
        {
          ComObjectName tableName(dropTableNode->getTableName());
          ComAnsiNamePart currCatAnsiName(currCatName);
          ComAnsiNamePart currSchAnsiName(currSchName);
          tableName.applyDefaults(currCatAnsiName, currSchAnsiName);
          const NAString catalogNamePart = tableName.getCatalogNamePartAsAnsiString();
          const NAString schemaNamePart = tableName.getSchemaNamePartAsAnsiString(TRUE);
          const NAString objectNamePart = tableName.getObjectNamePartAsAnsiString(TRUE);

          cleanupObjectAfterError(cliInterface,
                                  catalogNamePart, schemaNamePart, objectNamePart,
                                  COM_BASE_TABLE_OBJECT, NULL,
                                  dropTableNode->ddlXns());
        }

      return;
    }

  endXnIfStartedHere(&cliInterface, xnWasStartedHere, 0);

}

Lng32 CmpSeabaseDDL::dropSeabasePartitionTable(ExeCliInterface * cliInterface,
                            NAString &currCatName, NAString &currSchName, const NAString &btTableName, NABoolean needCascade)
{
  Lng32 ret;
  char buf[4000];
  memset(buf, 0, 4000);
  str_sprintf(buf, "DROP PARTITION TABLE %s.\"%s\".%s %s",
          currCatName.data(), currSchName.data(), ToAnsiIdentifier(btTableName).data(), needCascade ? "cascade" : "");

  ret = cliInterface->executeImmediate(buf);
  if (ret >= 0)
  {
    // if this function is called in akrcmp, you will need to do shared cache ops
    // in top level manually
    insertIntoSharedCacheList(currCatName,
                              currSchName,
                              btTableName,
                              COM_BASE_TABLE_OBJECT,
                              SharedCacheDDLInfo::DDL_DELETE);
  }
  return ret;
}

Lng32 CmpSeabaseDDL::dropSeabasePartitionTable(ExeCliInterface * cliInterface,
                             StmtDDLAlterTableDropPartition * alterDropPartition,
                             NAPartition * partInfo,
                             NAString &currCatName,
                             NAString &currSchName)
{
  Lng32 ret = 0;

  // If drop first level partition, we need to drop all subpartition of this first level partition.
  if (!alterDropPartition->isSubpartition() && partInfo->hasSubPartition())
  {
    const NAPartitionArray *subPartArray = partInfo->getSubPartitions();
    for (int i = 0; i < subPartArray->entries(); ++i)
    {
      ret = dropSeabasePartitionTable(cliInterface, currCatName,
                                      currSchName, (*subPartArray)[i]->getPartitionEntityName());
      if (ret < 0)
        return ret;
    }
  }

  return dropSeabasePartitionTable(cliInterface, currCatName,
                                   currSchName, partInfo->getPartitionEntityName());
}

Lng32 CmpSeabaseDDL::updateSeabasePartitionbtMDDataPosition(ExeCliInterface * cliInterface,
                            const Int64 partitionUID)
{
  Lng32 ret;
  char buf[4000];
  memset(buf, 0, 4000);
  str_sprintf(buf, "UPDATE %s.\"%s\".%s SET PARTITION_ORDINAL_POSITION = PARTITION_ORDINAL_POSITION - 1 \
              WHERE PARENT_UID = (select PARENT_UID from  %s.\"%s\".%s where PARTITION_UID = %ld) and \
              PARTITION_ORDINAL_POSITION > (select PARTITION_ORDINAL_POSITION from  %s.\"%s\".%s where PARTITION_UID = %ld)",
          getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_PARTITIONS,
          getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_PARTITIONS, partitionUID,
          getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_PARTITIONS, partitionUID);

  ret = cliInterface->executeImmediate(buf);
  return ret;
}

Lng32 CmpSeabaseDDL::deleteSeabasePartitionbtMDData(ExeCliInterface * cliInterface,
                            const Int64 partitionUID)
{
  Lng32 ret;
  char buf[4000];
  memset(buf, 0, 4000);
  char partitionUIDStr[20];
  str_sprintf(buf, "DELETE FROM %s.\"%s\".%s WHERE PARTITION_UID = %ld",
        getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_PARTITIONS, partitionUID);

  ret = cliInterface->executeImmediate(buf);
  return ret;
}

Lng32 CmpSeabaseDDL::deleteSeabasePartitionMDData(ExeCliInterface * cliInterface,
      const Int64 tableUID)
{
  Lng32 ret;
  char buf[4000];
  memset(buf, 0, 4000);
  str_sprintf(buf, "DELETE FROM %s.\"%s\".%s WHERE TABLE_UID = %ld",
        getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_TABLE_PARTITION, tableUID);

  ret = cliInterface->executeImmediate(buf);
  return ret;
}

short CmpSeabaseDDL::invalidateStats(Int64 tableUID)
{
  SQL_QIKEY qiKey;

  strncpy(qiKey.operation,COM_QI_STATS_UPDATED_LIT,sizeof(qiKey.operation));
  qiKey.ddlObjectUID = tableUID;
  return SQL_EXEC_SetSecInvalidKeys(1, &qiKey);
}

// This method implements the ObjectEpochCacheEntry initialization protocol:
//
// Algorithm:
//
// Get a distributed lock on the object name
// Create the cache entries on all nodes with epoch 0
// If some node already has an entry and it is later
//   Try creating again, using the latest epoch value
// Release the distributed lock

Int32 CmpSeabaseDDL::createObjectEpochCacheEntry(ObjectEpochCacheEntryName * name)
{
  if (CmpCommon::getDefault(TRAF_LOCK_DDL) == DF_OFF)
    return 0;

  Int32 retcode = -EXE_OEC_UNABLE_TO_GET_DIST_LOCK; // assume failure
  WaitedLockController lock(name->getDLockKey(),1);
  if (lock.lockHeld())
  {
    Int32 objectNameLength = name->objectName().length();
    const char * objectName = name->objectName().data();
    UInt32 expectedEpoch = 0;
    UInt32 expectedFlags = ObjectEpochChangeRequest::NO_DDL_IN_PROGRESS;
    UInt32 maxExpectedEpochFound = 0;
    UInt32 maxExpectedFlagsFound = 0;
    retcode = SQL_EXEC_SetObjectEpochEntry(ObjectEpochChangeRequest::CREATE_OBJECT_EPOCH_ENTRY,
      objectNameLength,objectName,name->getRedefTime(),name->getHash(),expectedEpoch,expectedFlags,
      expectedEpoch,expectedFlags,
      /* out */ &maxExpectedEpochFound,/* out */ &maxExpectedFlagsFound);

    if (retcode == -EXE_OEC_UNEXPECTED_STATE)
    {
      // This must be a node-up situation, and entries exist on other nodes that are later
      // than the initial value of zero (because a DDL operation is in progress or has been
      // done since the time the cluster came up). We might have created an entry with
      // different epoch values, so we should retry with a force request to make them
      // all the latest values.

      UInt32 dummyMaxExpectedEpochFound = 0;
      UInt32 dummyMaxExpectedFlagsFound = 0;
      retcode = SQL_EXEC_SetObjectEpochEntry(ObjectEpochChangeRequest::FORCE,
        objectNameLength,objectName,name->getRedefTime(),name->getHash(),
        maxExpectedEpochFound,maxExpectedFlagsFound,
        maxExpectedEpochFound,maxExpectedFlagsFound,  // no, that's not a typo; this line repeats!
        /* out */ &dummyMaxExpectedEpochFound,/* out */ &dummyMaxExpectedFlagsFound);
    }

    if (retcode != 0)
    {
      // retrieve the cli diags
      CmpCommon::diags()->mergeAfter(*(GetCliGlobals()->currContext()->getDiagsArea()));
    }
  }
  else
  {
    // couldn't get distributed lock; mention that in diags area  
    *CmpCommon::diags() << DgSqlCode(retcode) // -EXE_OEC_UNABLE_TO_GET_DIST_LOCK
       << DgString0(name->objectName().data());
  }

  return retcode;
}

// This method implements the ObjectEpochCacheEntry start DDL protocol:
//
// Algorithm:
//
// Get a distributed lock on the object name
// Modify object epoch cache entry to show a DDL operation is in progress
// If some node has an unexpected value
//   Report that error
// Release the distributed lock

Int32 CmpSeabaseDDL::modifyObjectEpochCacheStartDDL(ObjectEpochCacheEntryName * name,
  UInt32 expectedEpoch, 
  bool readsAllowed)
{
  if (CmpCommon::getDefault(TRAF_LOCK_DDL) == DF_OFF)
    return 0;

  Int32 retcode = -EXE_OEC_UNABLE_TO_GET_DIST_LOCK; // assume failure
  WaitedLockController lock(name->getDLockKey(),1);
  if (lock.lockHeld())
  {
    Int32 objectNameLength = name->objectName().length();
    const char * objectName = name->objectName().data();
    UInt32 newFlags = ObjectEpochChangeRequest::DDL_IN_PROGRESS;
    if (!readsAllowed)
      newFlags |= ObjectEpochChangeRequest::READS_DISALLOWED;
    UInt32 maxExpectedEpochFound = 0;
    UInt32 maxExpectedFlagsFound = 0;
    // update the flags in the ObjectEpochCacheEntry; leave the epoch unchanged until operation complete
    retcode = SQL_EXEC_SetObjectEpochEntry(ObjectEpochChangeRequest::START_OR_CONTINUE_DDL_OPERATION,
      objectNameLength,objectName,name->getRedefTime(),name->getHash(),
      expectedEpoch,ObjectEpochChangeRequest::NO_DDL_IN_PROGRESS,
      expectedEpoch,newFlags,
      /* out */ &maxExpectedEpochFound,/* out */ &maxExpectedFlagsFound);

    if (retcode == EXE_OEC_CACHE_FULL)
    {
      // We got a "cache full" warning. This can happen only if 
      // CQD TRAF_OBJECT_EPOCH_CACHE_BEHAVIOR is set to '1'. It happens
      // because we tried to create an object epoch cache entry on
      // the fly after failing to earlier (also due to a cache full
      // condition). A warning should already be reported in the 
      // diags area, so we can safely ignore this one.
      retcode = 0;
    }
    if (retcode < 0)
    {
      // retrieve the cli diags
      CmpCommon::diags()->mergeAfter(*(GetCliGlobals()->currContext()->getDiagsArea()));
    }
  }
  else
  {
    // couldn't get distributed lock; mention that in diags area  
    *CmpCommon::diags() << DgSqlCode(retcode) // -EXE_OEC_UNABLE_TO_GET_DIST_LOCK
       << DgString0(name->objectName().data());
  }

  // Note: Test point delay of more than Zookeeper timeout (about 30 seconds)
  // will cause the Zookeeper lock to be dropped, resulting in an 8149 error here.
  CmpCommon::context()->executeTestPoint(TESTPOINT_0);

  return retcode;
}

// This method implements the ObjectEpochCacheEntry continue DDL protocol. This
// is used for operations that allow DML reads during the first phase of the DDL
// operation. When we come to the time when we want to lock out reads too, this
// method is called.
//
// Algorithm:
//
// Get a distributed lock on the object name
// Modify object epoch cache entry to show reads are disallowed
// If some node has an unexpected value
//   Report that error
// Release the distributed lock

Int32 CmpSeabaseDDL::modifyObjectEpochCacheContinueDDL(ObjectEpochCacheEntryName * name,
  UInt32 expectedEpoch)
{
  if (CmpCommon::getDefault(TRAF_LOCK_DDL) == DF_OFF)
    return 0;

  Int32 retcode = -EXE_OEC_UNABLE_TO_GET_DIST_LOCK; // assume failure
  WaitedLockController lock(name->getDLockKey(),10);  // longer timeout because we'd prefer not to abort DDL
  if (lock.lockHeld())
  {
    Int32 objectNameLength = name->objectName().length();
    const char * objectName = name->objectName().data();
    UInt32 maxExpectedEpochFound = 0;
    UInt32 maxExpectedFlagsFound = 0;
    // update the flags in the ObjectEpochCacheEntry; leave the epoch unchanged until operation complete
    retcode = SQL_EXEC_SetObjectEpochEntry(ObjectEpochChangeRequest::START_OR_CONTINUE_DDL_OPERATION,
      objectNameLength,objectName,name->getRedefTime(),name->getHash(),
      expectedEpoch,ObjectEpochChangeRequest::DDL_IN_PROGRESS,
      expectedEpoch,ObjectEpochChangeRequest::DDL_IN_PROGRESS | ObjectEpochChangeRequest::READS_DISALLOWED,
      /* out */ &maxExpectedEpochFound,/* out */ &maxExpectedFlagsFound);
    if (retcode == EXE_OEC_CACHE_FULL)
    {
      // We got a "cache full" warning. This can happen only if 
      // CQD TRAF_OBJECT_EPOCH_CACHE_BEHAVIOR is set to '1'. It happens
      // because we tried to create an object epoch cache entry on
      // the fly after failing to earlier (also due to a cache full
      // condition). A warning should already be reported in the 
      // diags area, so we can safely ignore this one.
      retcode = 0;
    }
    if (retcode < 0)
    {
      // retrieve the cli diags
      CmpCommon::diags()->mergeAfter(*(GetCliGlobals()->currContext()->getDiagsArea()));
    }
  }
  else
  {
    // couldn't get distributed lock; mention that in diags area  
    *CmpCommon::diags() << DgSqlCode(retcode) // -EXE_OEC_UNABLE_TO_GET_DIST_LOCK
       << DgString0(name->objectName().data());
  }

  return retcode;
}

// This method implements the ObjectEpochCacheEntry end DDL protocol. This is
// called after the last commit of the DDL operation, and after QI keys have
// been sent, and shared metadata cache has been updated (when that logic is
// available).
//
// Algorithm:
//
// Get a distributed lock on the object name
// Modify object epoch cache entry to increment epoch and reset flags
// If some node has an unexpected value
//   Report that error
// Release the distributed lock

Int32 CmpSeabaseDDL::modifyObjectEpochCacheEndDDL(ObjectEpochCacheEntryName * name,
  UInt32 expectedEpoch)
{
  if (CmpCommon::getDefault(TRAF_LOCK_DDL) == DF_OFF)
    return 0;

  Int32 retcode = -EXE_OEC_UNABLE_TO_GET_DIST_LOCK; // assume failure
  WaitedLockController lock(name->getDLockKey(),10);  // longer timeout because we want this to succeed
  if (lock.lockHeld())
  {
    Int32 objectNameLength = name->objectName().length();
    const char * objectName = name->objectName().data();
    UInt32 maxExpectedEpochFound = 0;
    UInt32 maxExpectedFlagsFound = 0;
    // update the epoch in the ObjectEpochCacheEntry; reset the flags to show no DDL in progress
    retcode = SQL_EXEC_SetObjectEpochEntry(ObjectEpochChangeRequest::COMPLETE_DDL_OPERATION,
      objectNameLength,objectName,name->getRedefTime(),name->getHash(),
      expectedEpoch,ObjectEpochChangeRequest::DDL_IN_PROGRESS | ObjectEpochChangeRequest::READS_DISALLOWED,
      expectedEpoch+1,ObjectEpochChangeRequest::NO_DDL_IN_PROGRESS,
      /* out */ &maxExpectedEpochFound,/* out */ &maxExpectedFlagsFound);

    if (retcode == EXE_OEC_CACHE_FULL)
    {
      // We got a "cache full" warning. This can happen only if 
      // CQD TRAF_OBJECT_EPOCH_CACHE_BEHAVIOR is set to '1'. It happens
      // because we tried to create an object epoch cache entry on
      // the fly after failing to earlier (also due to a cache full
      // condition). A warning should already be reported in the 
      // diags area, so we can safely ignore this one.
      retcode = 0;
    }
    if (retcode < 0)
    {
      // retrieve the cli diags
      CmpCommon::diags()->mergeAfter(*(GetCliGlobals()->currContext()->getDiagsArea()));
    }
  }
  else
  {
    // couldn't get distributed lock; mention that in diags area  
    *CmpCommon::diags() << DgSqlCode(retcode) // -EXE_OEC_UNABLE_TO_GET_DIST_LOCK
       << DgString0(name->objectName().data());
  }

  return retcode;
}

// This method implements the ObjectEpochCacheEntry abort DDL protocol. This is
// called after a DDL operation has been rolled back. The original epoch is
// left unchanged, so that DML plans compiled on the old metadata can still be 
// used.
//
// Algorithm:
//
// Get a distributed lock on the object name
// Modify object epoch cache entry to reset flags
// If some node has an unexpected value
//   Report that error
// Release the distributed lock
//
// If the force parameter is false, the protocol validates the expected epoch
// and flags. If the force parameter is true, these are ignored. Normal abort
// processing will use force == false. CLEANUP will use force == true.

Int32 CmpSeabaseDDL::modifyObjectEpochCacheAbortDDL(ObjectEpochCacheEntryName * name,
  UInt32 expectedEpoch,bool force)
{
  if (CmpCommon::getDefault(TRAF_LOCK_DDL) == DF_OFF)
    return 0;

  Int32 retcode = -EXE_OEC_UNABLE_TO_GET_DIST_LOCK; // assume failure
  WaitedLockController lock(name->getDLockKey(),10);
  if (lock.lockHeld())
  {
    Int32 objectNameLength = name->objectName().length();
    const char * objectName = name->objectName().data();
    UInt32 maxExpectedEpochFound = 0;
    UInt32 maxExpectedFlagsFound = 0;
    Int32 operation = ObjectEpochChangeRequest::ABORT_DDL_OPERATION;
    if (force)
      operation = ObjectEpochChangeRequest::FORCE;
    // reset the flags to show now DDL in progress; epoch stays the same as before
    retcode = SQL_EXEC_SetObjectEpochEntry(operation,
      objectNameLength,objectName,name->getRedefTime(),name->getHash(),
      expectedEpoch,ObjectEpochChangeRequest::DDL_IN_PROGRESS | ObjectEpochChangeRequest::READS_DISALLOWED,
      expectedEpoch,ObjectEpochChangeRequest::NO_DDL_IN_PROGRESS,
      /* out */ &maxExpectedEpochFound,/* out */ &maxExpectedFlagsFound);
    if (retcode == EXE_OEC_CACHE_FULL)
    {
      // We got a "cache full" warning. This can happen only if 
      // CQD TRAF_OBJECT_EPOCH_CACHE_BEHAVIOR is set to '1'. It happens
      // because we tried to create an object epoch cache entry on
      // the fly after failing to earlier (also due to a cache full
      // condition). A warning should already be reported in the 
      // diags area, so we can safely ignore this one.
      retcode = 0;
    }
    if (retcode < 0)
    {
      CmpCommon::diags()->mergeAfter(*(GetCliGlobals()->currContext()->getDiagsArea()));
    }
  }
  else
  {
    // couldn't get distributed lock; mention that in diags area  
    *CmpCommon::diags() << DgSqlCode(retcode) // -EXE_OEC_UNABLE_TO_GET_DIST_LOCK
       << DgString0(name->objectName().data());
  }

  return retcode;
}


// ----------------------------------------------------------------------------
// method: alterSeabaseTableResetDDLLock
//
// Resets the object Epoch and turns off the DDL_IN_PROGRESS flags for the 
// requested table. 
//
// This statement runs in a transaction - although it doesn't need a transaction
//
// TBD - should this method first check to see if there is an active DDL lock 
// on the table?  Perhaps we can get the process ID of the last operation to
// lock the table and see if the process is still alive.
// ----------------------------------------------------------------------------
void CmpSeabaseDDL::alterSeabaseTableResetDDLLock (
     ExeCliInterface *cliInterface,
     StmtDDLAlterTableResetDDLLock * alterTableNode,
     NAString &currCatName, NAString &currSchName)
{
  ComObjectName tableName(alterTableNode->getTableName(), COM_TABLE_NAME);
  ComAnsiNamePart currCatAnsiName(currCatName);
  ComAnsiNamePart currSchAnsiName(currSchName);
  tableName.applyDefaults(currCatAnsiName, currSchAnsiName);
  NAString extTableName = tableName.getExternalName();

  // Need to be elevated user, schema owner, or have ALTER TABLE privilege
  if (!isDDLOperationAuthorized(SQLOperation::ALTER_TABLE,
                                0 /*tblOwner*/, 0 /*schOwner*/, 0 /*uid*/,
                                COM_BASE_TABLE_OBJECT, NULL /*privinfo*/))
    {
      *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);
  
      processReturn ();
      return;
    }

  if (alterTableNode->objectLock()) {
    if (CmpCommon::getDefault(TRAF_OBJECT_LOCK) != DF_ON) {
      processReturn ();
      return;
    }
    LOGDEBUG(CAT_SQL_LOCK, "%s object lock for table %s",
             alterTableNode->force() ? "Force reset" : "Reset",
             extTableName.data());
    NAString one("1");
    ActiveSchemaDB()->getDefaults().holdOrRestore("TRAF_OBJECT_LOCK_DDL_RETRY_LIMIT", 1);
    ActiveSchemaDB()->getDefaults().holdOrRestore("TRAF_OBJECT_LOCK_FORCE", 1);
    ActiveSchemaDB()->getDefaults().validateAndInsert(
          "TRAF_OBJECT_LOCK_DDL_RETRY_LIMIT", one, FALSE);
    if (alterTableNode->force()) {
      NAString on("ON");
      ActiveSchemaDB()->getDefaults().validateAndInsert(
            "TRAF_OBJECT_LOCK_FORCE", on, FALSE);
    }

    if (lockObjectDDL(tableName,
                      COM_BASE_TABLE_OBJECT,
                      alterTableNode->isVolatile(),
                      alterTableNode->isExternal()) < 0) {
      LOGERROR("Failed to reset object lock for table %s", extTableName.data());
    }

    ActiveSchemaDB()->getDefaults().holdOrRestore("TRAF_OBJECT_LOCK_DDL_RETRY_LIMIT", 2);
    ActiveSchemaDB()->getDefaults().holdOrRestore("TRAF_OBJECT_LOCK_FORCE", 2);
    processReturn();
    return;
  }

  // Just blindly update the object epoch cache independent on whether the
  // object actually exists
  ObjectEpochCacheEntryName oecName(STMTHEAP,
                                    extTableName,
                                    COM_BASE_TABLE_OBJECT,
                                    0 /*redefTime*/);
  CliGlobals *cliGlobals = GetCliGlobals();
  StatsGlobals *statsGlobals = cliGlobals->getStatsGlobals();
  ObjectEpochCache *objectEpochCache = statsGlobals->getObjectEpochCache();
  ObjectEpochCacheEntry *oecEntry = NULL;

  ObjectEpochCacheKey key(extTableName, oecName.getHash(), COM_BASE_TABLE_OBJECT);

  short retcode = objectEpochCache->getEpochEntry(oecEntry /* out */,key);
  if (retcode == ObjectEpochCache::OEC_ERROR_OK)
    {
      // TDB - any error handling required?
      modifyObjectEpochCacheAbortDDL(&oecName, oecEntry->epoch(), true);

      char msgBuf[extTableName.length() + 200];
      snprintf (msgBuf, sizeof(msgBuf), "Reset DDL lock, Name: %s, Epoch: %d, Flags: %d",
                extTableName.data(), oecEntry->epoch(), oecEntry->flags());
      QRLogger::log(CAT_SQL_EXE, LL_WARN, "%s", msgBuf);
#ifndef NDEBUG
      char * oec = getenv("TEST_OBJECT_EPOCH");
      if (oec)
        cout << msgBuf << endl;
#endif
    }
}


// ----------------------------------------------------------------------------
// method: ddlSetObjectEpoch
//
// Wrapper method to start a DDL operation for alter and drop requests
//
// Returns:
//    < 0 - error occurred (diags area populated)
//      0 - successful, no DDL lock obtained (e.g. because it's a metadata table)
//      1 - successful, DDL lock obtained
// ----------------------------------------------------------------------------
short CmpSeabaseDDL::ddlSetObjectEpoch(const NATable *naTable,
                                       NAHeap *heap,
                                       bool allowReads,
                                       bool isContinue)
{
  if (CmpCommon::getDefault(TRAF_LOCK_DDL) == DF_OFF)
    return 0;

  short retcode = 0;
  if (naTable == NULL)
    {
      SEABASEDDL_INTERNAL_ERROR("Bad NATable pointer in ddlSetObjectEpoch");
      return -1;
    }

  return ddlSetObjectEpochHelper(naTable->getTableName(),
                                 naTable->getObjectType(),
                                 naTable->objectUid().get_value(),
                                 naTable->getRedefTime(),
                                 naTable->isVolatileTable(),
                                 naTable->isTrafExternalTable(),
                                 heap,
                                 allowReads,
                                 isContinue);
}

// ----------------------------------------------------------------------------
// method: ddlSetObjectEpoch
//
// Wrapper method to start a DDL operation for create requests
//
// Returns:
//    < 0 - error occurred (diags area populated)
//      0 - successful, no DDL lock obtained (e.g. because it's a metadata table)
//      1 - successful, DDL lock obtained
// ----------------------------------------------------------------------------
short CmpSeabaseDDL::ddlSetObjectEpoch(const StmtDDLCreateTable *createTableNode,
                                       NAHeap *heap,
                                       bool allowReads,
                                       bool isContinue)
{
  short retcode = 0;

  CMPASSERT(createTableNode != NULL);

  return ddlSetObjectEpochHelper(createTableNode->getTableNameAsQualifiedName(),
                                 COM_BASE_TABLE_OBJECT,
                                 0, /* objectUID */
                                 0, /* redefTime */
                                 createTableNode->isVolatile(),
                                 createTableNode->isExternal(),
                                 heap,
                                 allowReads,
                                 isContinue);
}

// ----------------------------------------------------------------------------
// method: ddlSetObjectEpochHelper
//
// Helper method to start a DDL operation 
//
// It determines if there is already an DDL operation in progress, if not
// it will mark the entry as DDL_IN_PROGRESS.
//
// Returns:
//    < 0 - error occurred (diags area populated)
//      0 - successful, no DDL lock obtained (e.g. because it's a metadata table)
//      1 - successful, DDL lock obtained
// ----------------------------------------------------------------------------
short CmpSeabaseDDL::ddlSetObjectEpochHelper(const QualifiedName &qualName,
                                             ComObjectType objectType,
                                             Int64 objectUID,
                                             Int64 redefTime,
                                             bool isVolatile,
                                             bool isExternal,
                                             NAHeap *heap,
                                             bool allowReads,
                                             bool isContinue)
{
  if (CmpCommon::getDefault(TRAF_LOCK_DDL) == DF_OFF)
    return 0;

  short retcode = 0;

  if (isVolatile)
    return 0;

  // TDB - add support for native HIVE and HBASE tables
  if (isExternal)
    return 0;

  // If a reserved schema, just return
  if (isSeabaseReservedSchema(qualName.getCatalogName(), 
                              qualName.getSchemaName()))
    return 0;

  // If special tables, just return
  if (isLOBDependentNameMatch(qualName.getObjectName()))
    return 0;

  // if not TRAFODION catalog, just return
  if (qualName.getCatalogName() != TRAFODION_SYSTEM_CATALOG)
    return 0;

  // If not a base table, just return
  // TODO: support other object types (UDR)
  if (objectType != COM_BASE_TABLE_OBJECT)
    return 0;

  NAString tableName = qualName.getQualifiedNameAsAnsiString();
  ObjectEpochCacheEntryName oecName(heap, tableName, objectType, 0);

  CliGlobals *cliGlobals = GetCliGlobals();
  StatsGlobals *statsGlobals = cliGlobals->getStatsGlobals();
  if (!statsGlobals)
    {
      // statsGlobals can be NULL if sqlci is started before sqstart completes.
      // Rather than have an annoying core, we'll return an error.
      retcode = -4401;
      *CmpCommon::diags() << DgSqlCode(-4401) << DgString0(tableName.data())
        << DgString1("RMS Segment is not yet available");
      return retcode;
    }

  ObjectEpochCache *objectEpochCache = statsGlobals->getObjectEpochCache();
  ObjectEpochCacheEntry *oecEntry = NULL;

  ObjectEpochCacheKey key(tableName, oecName.getHash(), COM_BASE_TABLE_OBJECT);

  UInt32 currentEpoch(0);
  UInt32 currentFlags(0);
  retcode = objectEpochCache->getEpochEntry(oecEntry /* out */,key);
  if (retcode == ObjectEpochCache::OEC_ERROR_OK)
    {
      // oecEntry points to the ObjectEpochCacheEntry
      currentEpoch = oecEntry->epoch();
      currentFlags = oecEntry->flags();
    }
  else if (retcode == ObjectEpochCache::OEC_ERROR_ENTRY_NOT_EXISTS)
    {
      // No entry currently exists in the ObjectEpochCache for this name,
      // so create one
      retcode = createObjectEpochCacheEntry(&oecName);
      if (retcode < 0)
        {
          // unexpected error; diags area has been populated
          return retcode;
        }
      else
        {
          retcode = objectEpochCache->getEpochEntry(oecEntry /* out */,key);
          if (retcode == ObjectEpochCache::OEC_ERROR_OK)
          {
            currentEpoch = oecEntry->epoch();
            currentFlags = oecEntry->flags();
          }
          else if ((retcode == ObjectEpochCache::OEC_ERROR_ENTRY_NOT_EXISTS) &&
                   (CmpCommon::getDefaultLong(TRAF_OBJECT_EPOCH_CACHE_FULL_BEHAVIOR) == 2))
          {
            // When the Object Epoch Cache is full, and CQD TRAF_OBJECT_EPOCH_CACHE_FULL_BEHAVIOR
            // is set to 2 (meaning, ignore the cache full condition), then we will arrive here
            // if we were unable to create an object epoch cache entry above. We'll treat this
            // as a "found" condition but use zero epoch and flags in this case.
            currentEpoch = 0;
            currentFlags = 0;
            retcode = 0; 
          }
          else
          {
            // Unexpected error
            char buf[100];
            sprintf(buf,"Internal error, %d returned on getEpochEntry call",(int)retcode);
            *CmpCommon::diags() << DgSqlCode(-4401) << DgString0(tableName.data())
              << DgString1(buf);
            return -4401;
          }
        }
      }
  else
    return -1;

  // If object has already been processed for this operation, see if request is okay
  // disallowReadsSet  allowReads
  //   Y                  Y           error,  cannot go from disallowed to allowed
  //   Y                  N           okay, flags match - return
  //   N                  Y           okay, flags match - return
  //   N                  N           okay, continue to update disallowReadsSet to TRUE
  Int32 ddlListEntry = CmpCommon::context()->ddlObjsList().findEntry(tableName);
  if (ddlListEntry >= 0)
    {
      NABoolean disallowReadsSet = (currentFlags & ObjectEpochChangeRequest::READS_DISALLOWED);
      if (disallowReadsSet)
        {
          if (allowReads)
            {
              *CmpCommon::diags() << DgSqlCode (-CAT_DDL_OP_IN_PROGRESS)
                                  << DgString0(tableName.data());
              return -CAT_DDL_OP_IN_PROGRESS;
            }
          else
            return 0;
        }
      else
        if (allowReads)
          return 0;
    }

  // If object has not yet been seen (ddlListEntry < 0), it is not 
  // not continuing an existing DDL requst (!isContinue), and there is an 
  // existing DDL operation in progress, return an error.  
  // Some other process has this object locked.
  if (ddlListEntry < 0 && (currentFlags & ObjectEpochChangeRequest::DDL_IN_PROGRESS))
    {
      if ((currentFlags & ObjectEpochChangeRequest::READS_DISALLOWED) ||
          (!isContinue))
        {
          *CmpCommon::diags() << DgSqlCode (-CAT_DDL_OP_IN_PROGRESS)
                              << DgString0(tableName.data());
          return -CAT_DDL_OP_IN_PROGRESS;
        }
    }

  if (isContinue)
    retcode = modifyObjectEpochCacheContinueDDL(&oecName, currentEpoch);
  else
    retcode = modifyObjectEpochCacheStartDDL(&oecName, currentEpoch, allowReads);
    
  if (retcode != 0)
    return -1;

  if (!isContinue)
    {
      // Add to list of objects
      DDLObjInfo ddlObj(tableName, objectUID, objectType);
      ddlObj.setQIScope(REMOVE_FROM_ALL_USERS);
      ddlObj.setRedefTime(redefTime);
      ddlObj.setEpoch(currentEpoch);
      ddlObj.setDDLOp(TRUE);

      CmpCommon::context()->ddlObjsList().insert(ddlObj);
    }

  char msgBuf[tableName.length() + 200];
  snprintf (msgBuf, sizeof(msgBuf), "%s, Name: %s, Epoch: %d, "
                    " Flags: %d, Reads allowed: %d",
            (isContinue ? "DDL Continue" : "DDL Start"), tableName.data(),
            currentEpoch, currentFlags, allowReads);
  QRLogger::log(CAT_SQL_EXE, LL_WARN, "%s", msgBuf);
#ifndef NDEBUG
  char * oec = getenv("TEST_OBJECT_EPOCH");
  if (oec)
    cout << msgBuf << endl;
#endif

  return 1;  // DDL lock obtained
}

short CmpSeabaseDDL::lockObjectDDL(const NATable *naTable)
{
  CMPASSERT(naTable != NULL);
  return lockObjectDDL(naTable->getTableName(),
		       naTable->getObjectType(),
		       naTable->isVolatileTable(),
		       naTable->isTrafExternalTable());
 }

short CmpSeabaseDDL::lockObjectDDL(const StmtDDLCreateTable *createTableNode)
{
  CMPASSERT(createTableNode != NULL);
  return lockObjectDDL(createTableNode->getTableNameAsQualifiedName(),
		       COM_BASE_TABLE_OBJECT,
		       createTableNode->isVolatile(),
		       createTableNode->isExternal());
}

short CmpSeabaseDDL::lockObjectDDL(const StmtDDLAlterTable *alterTableNode)
{
  CMPASSERT(alterTableNode != NULL);
  return lockObjectDDL(alterTableNode->getTableNameAsQualifiedName(),
		       COM_BASE_TABLE_OBJECT,
		       alterTableNode->isVolatile(),
		       alterTableNode->isExternal());
}

short CmpSeabaseDDL::lockObjectDDL(const ComObjectName &objectName,
				   ComObjectType objectType,
				   bool isVolatile,
				   bool isExternal)
{
  QualifiedName qualName(objectName);
  return lockObjectDDL(qualName, objectType, isVolatile, isExternal);
}

short CmpSeabaseDDL::lockObjectDDL(const QualifiedName &qualName,
				   ComObjectType objectType,
				   bool isVolatile,
				   bool isExternal)
{
  short retcode = 0;

  if (CmpCommon::getDefault(TRAF_OBJECT_LOCK) == DF_OFF)
    return 0;

  if (Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL))
    return 0;

  // If not a base table, just return
  // TODO: support other object types (UDR)
  if (objectType != COM_BASE_TABLE_OBJECT)
    return 0;

  if (isVolatile)
    return 0;

  // TDB - add support for native HIVE and HBASE tables
  if (isExternal)
    return 0;

  LOGDEBUG(CAT_SQL_LOCK,
           "LOCK OBJECT DDL: %s",
           qualName.getQualifiedNameAsAnsiString().data());
  CMPASSERT(!qualName.getCatalogName().isNull());
  CMPASSERT(!qualName.getSchemaName().isNull());
  CMPASSERT(!qualName.getObjectName().isNull());

  if (!ComIsUserTable(qualName)) {
    return 0;
  }

  // If special tables, just return
  if (isLOBDependentNameMatch(qualName.getObjectName()))
    return 0;

  NAString tabName = qualName.getQualifiedNameAsAnsiString();
  Int32 cliRC = SQL_EXEC_LockObjectDDL(tabName.data(),
                                       COM_BASE_TABLE_OBJECT);
  if (cliRC < 0) {
    LOGERROR(CAT_SQL_EXE, "DDL lock failed for table %s", tabName.data());
    CmpCommon::diags()->mergeAfter(*(GetCliGlobals()->currContext()->getDiagsArea()));
    GetCliGlobals()->currContext()->getDiagsArea()->clear();
    return -1;
  }
  return 1;
}

short CmpSeabaseDDL::unlockObjectDDL(const QualifiedName &qualName,
                                     ComObjectType objectType,
                                     bool isVolatile,
                                     bool isExternal)
{
  short retcode = 0;

  if (CmpCommon::getDefault(TRAF_OBJECT_LOCK) == DF_OFF)
    return 0;

  if (Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL))
    return 0;

  // If not a base table, just return
  // TODO: support other object types (UDR)
  if (objectType != COM_BASE_TABLE_OBJECT)
    return 0;

  if (isVolatile)
    return 0;

  // TDB - add support for native HIVE and HBASE tables
  if (isExternal)
    return 0;

  LOGDEBUG(CAT_SQL_LOCK,
           "UNLOCK OBJECT DDL: %s",
           qualName.getQualifiedNameAsAnsiString().data());
  CMPASSERT(!qualName.getCatalogName().isNull());
  CMPASSERT(!qualName.getSchemaName().isNull());
  CMPASSERT(!qualName.getObjectName().isNull());

  if (!ComIsUserTable(qualName)) {
    return 0;
  }

  // If special tables, just return
  if (isLOBDependentNameMatch(qualName.getObjectName()))
    return 0;

  NAString tabName = qualName.getQualifiedNameAsAnsiString();
  Int32 cliRC = SQL_EXEC_UnLockObjectDDL(tabName.data(),
                                         COM_BASE_TABLE_OBJECT);
  if (cliRC < 0) {
    LOGERROR(CAT_SQL_EXE, "DDL unlock failed for table %s", tabName.data());
    CmpCommon::diags()->mergeAfter(*(GetCliGlobals()->currContext()->getDiagsArea()));
    GetCliGlobals()->currContext()->getDiagsArea()->clear();
    return -1;
  }
  return 1;
}

//for partition v2
short CmpSeabaseDDL::lockObjectDDL(const NATable *naTable,
                                   NAString &catalogNamePart,
                                   NAString &schemaNamePart,
                                   NABoolean isPartitionV2)
{
  short retcode = 0;

  if (isPartitionV2 == false)
    return lockObjectDDL(naTable);
  else
  {
    CMPASSERT(naTable != NULL);
    const NAPartitionArray & partitionArray = naTable->getPartitionArray();
    for (Lng32 i = 0; i < partitionArray.entries(); i++)
    {
      if (partitionArray[i]->hasSubPartition())
      {
        const NAPartitionArray *subpArray = partitionArray[i]->getSubPartitions();
        for (Lng32 j = 0; j < subpArray->entries(); j++)
        {
          QualifiedName qn((*subpArray)[j]->getPartitionEntityName(),
                           schemaNamePart,
                           catalogNamePart);
     
          retcode = lockObjectDDL(qn, COM_BASE_TABLE_OBJECT,
                                  naTable->isVolatileTable(),
                                  naTable->isTrafExternalTable());
          if (retcode < 0)
            return retcode;
        }
      }
      else
      {
        QualifiedName qn(partitionArray[i]->getPartitionEntityName(),
                         schemaNamePart,
                         catalogNamePart);
     
        retcode = lockObjectDDL(qn, COM_BASE_TABLE_OBJECT,
                                naTable->isVolatileTable(),
                                naTable->isTrafExternalTable());
        if (retcode < 0)
          return retcode;
      }
    }
  }
  return retcode;
}

// ----------------------------------------------------------------------------
// renameSeabaseTable
//
//    ALTER TABLE <table> RENAME TO <new table>
//
// This statement is not run in a transaction by default.
// It cannot be run in a user transaction
// This statement may recreate the table, so it employs a read and read/write
// DDL lock.
//     A MARK FOR REPLACE UID, a better method needs to be used
// ----------------------------------------------------------------------------
void CmpSeabaseDDL::renameSeabaseTable(
                                       StmtDDLAlterTableRename * renameTableNode,
                                       NAString &currCatName, NAString &currSchName)
{
  Lng32 retcode = 0;
  Lng32 cliRC = 0;

  // variables needed to find identity column (have to be declared before
  // the first goto or C++ will moan because of the initializers)
  NABoolean found = FALSE;
  Lng32 idPos = 0;
  NAColumn *col = NULL;

  retcode = lockObjectDDL(renameTableNode);
  if (retcode < 0) {
    processReturn();
    return;
  }

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, 
                               CmpCommon::context()->sqlSession()->getParentQid());

  NAString tabName = renameTableNode->getTableName();
  NAString catalogNamePart;
  NAString schemaNamePart;
  NAString objectNamePart;
  NAString extTableName;
  NAString extNameForHbase;
  NAString binlogTableName;
  NAString newbinlogTableName;
  NATable * naTable = NULL;
  NAArray<HbaseStr> *defStringArray = NULL;
  HbaseStr *oldCols = NULL;
  HbaseStr *oldKeys = NULL;
  NAString colColsString;
  char fullColBuf[1024*20]; //some table may have 1024 columns
  char fullKeyBuf[128*20];
  NAString fullColBufString;
  NAString fullKeyBufString;

  CorrName cn;
  retcode = 
    setupAndErrorChecks(tabName, 
                        renameTableNode->getOrigTableNameAsQualifiedName(),
                        currCatName, currSchName,
                        catalogNamePart, schemaNamePart, objectNamePart,
                        extTableName, extNameForHbase, cn,
                        &naTable, 
                        FALSE, FALSE,
                        &cliInterface,
                        COM_BASE_TABLE_OBJECT,
                        SQLOperation::ALTER_TABLE,
                        FALSE,
                        TRUE/*process hiatus*/);
  if (retcode < 0)
    {
      processReturn();

      return;
    }

  ComObjectName newTableName(renameTableNode->getNewNameAsAnsiString());
  newTableName.applyDefaults(catalogNamePart, schemaNamePart);
  const NAString newObjectNamePart = newTableName.getObjectNamePartAsAnsiString(TRUE);
  const NAString newExtTableName = newTableName.getExternalName(TRUE);

  newbinlogTableName = schemaNamePart + "." + newObjectNamePart;

  const NAString newExtNameForHbase = genHBaseObjName
    (catalogNamePart, schemaNamePart, newObjectNamePart, 
     naTable->getNamespace());
  
  CorrName newcn(newObjectNamePart,
              STMTHEAP,
              schemaNamePart,
              catalogNamePart);
  
  retcode = lockObjectDDL(newcn.getQualifiedNameObj(),
			  COM_BASE_TABLE_OBJECT,
			  renameTableNode->isVolatile(),
			  renameTableNode->isExternal());
  if (retcode < 0) {
    processReturn();
    return;
  }

  BindWA bindWA(ActiveSchemaDB(), CmpCommon::context(), TRUE/*inDDL*/);

  NABoolean hasStatsDesc = FALSE;
  if (naTable && naTable->isStoredDesc())
  {
    TrafTableStatsDesc *statsDesc = NULL;
    NAHeap *descHeap = NULL;
    TrafDesc *trafDesc = ActiveSchemaDB()->getNATableDB()->
                         getTableDescFromCacheOrText(naTable->getExtendedQualName(),
                                                  naTable->objectUid().get_value(),
                                                  &descHeap);
    if (trafDesc && trafDesc->tableDesc()->table_stats_desc &&
        trafDesc->tableDesc()->table_stats_desc->tableStatsDesc())
      statsDesc = trafDesc->tableDesc()->table_stats_desc->tableStatsDesc();
    if (statsDesc)
      hasStatsDesc = TRUE;
    if (trafDesc)
    {
      NADELETEBASIC(trafDesc, descHeap);
    }
  }

  // If renaming as part of upgrade, check actual metadata for existence
  if ((isSeabasePrivMgrMD(catalogNamePart, schemaNamePart)) &&
      (Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)))
    {
      retcode = existsInSeabaseMDTable(&cliInterface,
                                       catalogNamePart, schemaNamePart, newObjectNamePart,
                                       COM_BASE_TABLE_OBJECT);
      if (retcode < 0)
        {
          processReturn();

          return;
        }
      if (retcode == 1)
        {
          // an object already exists with the new name
          *CmpCommon::diags() << DgSqlCode(-1390)
                              << DgString0(newExtTableName);
      
          processReturn();
      
          return;
        }
    }
  else
    {
      NATable *newNaTable = bindWA.getNATable(newcn); 
      if (newNaTable != NULL && (NOT bindWA.errStatus()))
        {
          // an object already exists with the new name
          *CmpCommon::diags() << DgSqlCode(-1390)
                              << DgString0(newExtTableName);
      
          processReturn();
      
          return;
        }
      else if (newNaTable == NULL &&
               bindWA.errStatus() &&
               (!CmpCommon::diags()->contains(-4082) || CmpCommon::diags()->getNumber() > 1))
        {
          // there is some error other than the usual -4082, object
          // does not exist

          // If there is also -4082 error, remove that as it is misleading
          // to the user. The user would see, "new name does not exist" 
          // and wonder, what is wrong with that?

          for (CollIndex i = CmpCommon::diags()->returnIndex(-4082);
               i != NULL_COLL_INDEX;
               i = CmpCommon::diags()->returnIndex(-4082))
            {
              CmpCommon::diags()->deleteError(i);
            }
  
          if (CmpCommon::diags()->getNumber() > 0) // still anything there?
            {
              processReturn();  // error is already in the diags
              return;
            }
        }
    }

  CmpCommon::diags()->clear();
  
  // cannot rename a view
  if (naTable->getViewText())
    {
      *CmpCommon::diags()
        << DgSqlCode(-1427)
        << DgString0("Reason: Operation not allowed on a view.");
      
      processReturn();
      
      return;
    }

  // cascade option not supported
  if (renameTableNode->isCascade())
    {
      *CmpCommon::diags() << DgSqlCode(-1427)
                          << DgString0("Reason: Cascade option not supported.");
      
      processReturn();
      return;
    }

  const CheckConstraintList &checkList = naTable->getCheckConstraints();
  if (checkList.entries() > 0)
    {
      *CmpCommon::diags()
        << DgSqlCode(-1427)
        << DgString0("Reason: Operation not allowed if check constraints are present. Drop the constraints and recreate them after rename.");
      
      processReturn();
      
      return;
    }
    
  // cannot rename if there are referential constraint referencing this table.
  const AbstractRIConstraintList &uniqueList = naTable->getUniqueConstraints();
  for (Int32 i = 0; i < uniqueList.entries(); i++)
    {
      AbstractRIConstraint *ariConstr = uniqueList[i];
      
      if (ariConstr->getOperatorType() != ITM_UNIQUE_CONSTRAINT)
        continue;

      UniqueConstraint * uniqConstr = (UniqueConstraint*)ariConstr;

      // We will only reach here is cascade option is specified.
      // drop all constraints referencing me.
      if (uniqConstr->hasRefConstraintsReferencingMe())
        {
          *CmpCommon::diags()
            << DgSqlCode(-1427)
            << DgString0("Reason: Operation not allowed if there are referential constraint referencing this table. Drop the referential constraints and recreate them after rename.");
          
          processReturn();
          
          return;
        }
    }

  // Get a read only DDL lock on table
  retcode = ddlSetObjectEpoch(naTable, STMTHEAP, TRUE /*readsAllowed*/);
  if (retcode < 0)
    {
      processReturn();
      return;
    }

  // if there are unnamed constraints on this table in 'UnNamed_Constr' format
  // and this table is enabled for xDC replication,
  // drop and recreate those constraints.
  NAList<NAString> dropConstrList(STMTHEAP);
  NAList<NAString> addConstrList(STMTHEAP);
  // if we USE_UUID_AS_HBASE_TABLENAME, we only update name, for unnamed, we can skip it 
  if (!USE_UUID_AS_HBASE_TABLENAME && NOT naTable->isPartitionV2Table() &&
      (naTable->xnRepl() != COM_REPL_NONE || naTable->incrBackupEnabled() ) &&
      ((naTable->getUniqueConstraints().entries() > 0) || // unique constraints
       (naTable->getRefConstraints().entries() > 0))) // ref constraints
    {
      Space space;
      char buf[15000];

      if (naTable->getUniqueConstraints().entries() > 0)
        {
          for (Int32 i = 0; i < uniqueList.entries(); i++)
            {
              AbstractRIConstraint *ariConstr = uniqueList[i];

              if (ariConstr->getOperatorType() != ITM_UNIQUE_CONSTRAINT)
                continue;

              UniqueConstraint * uniqConstr = (UniqueConstraint*)ariConstr;
              if (uniqConstr->isPrimaryKeyConstraint())
                continue;

              const NAString &constrName = uniqConstr->getConstraintName().getObjectName();
              
              if ((constrName.length() >= (strlen(TRAF_UNNAMED_CONSTR_PREFIX))) &&
                  (constrName.index(TRAF_UNNAMED_CONSTR_PREFIX) == 0))
                { 
                  const NAString& ansiTableName = 
                    uniqConstr->getDefiningTableName().getQualifiedNameAsAnsiString(TRUE);
                  
                  const NAString &ansiConstrName =
                    uniqConstr->getConstraintName().getQualifiedNameAsAnsiString(TRUE);

                  // generate DROP CONSTRAINT stmt
                  sprintf(buf,  "ALTER TABLE %s DROP CONSTRAINT %s",
                          ansiTableName.data(),
                          ansiConstrName.data());
                  dropConstrList.insert(NAString(buf));

                  // generate ADD CONSTRAINT stmt
                  if (CmpGenUniqueConstrStr(ariConstr, &space, buf, FALSE, FALSE))
                    {
                      // internal error. Assert.
                      // Should DDL lock be removed here?
                      CMPASSERT(0);
                      return;
                    }

                  NAString nas(buf);
                  char constrNumBuf[20];

                  // constr name is of the format:
                  //  "_Traf_UnNamed_Constr_1_T"
                  // Replace it with:
                  //  "_Traf_UnNamed_Constr_1_TRENAMED"
                  Int32 constrNum = extractNumVal(constrName);
                  if (constrNum <= 0)
                    {
                      // internal error. Assert.
                      // Should DDL lock be removed here?
                      CMPASSERT(0);
                      return;
                    }

                  NAString stringToSearch = NAString("\"") + NAString(TRAF_UNNAMED_CONSTR_PREFIX) + str_itoa(constrNum, constrNumBuf) + NAString("_") + objectNamePart + NAString("\"");
                  NAString stringToReplace = NAString("\"") + NAString(TRAF_UNNAMED_CONSTR_PREFIX) + str_itoa(constrNum, constrNumBuf) + NAString("_") + newObjectNamePart + NAString("\"");

                  size_t pos = nas.index(stringToSearch.data(), stringToSearch.length(),
                                         0, NAString::exact);
                  if (pos == NA_NPOS)
                    {
                      // internal error. Assert.
                      // Should DDL lock be removed here?
                      CMPASSERT(0);
                      return;
                    } 

                  // replace constraint name with new constraint name
                  nas.replace(pos, stringToSearch.length(),
                              stringToReplace, stringToReplace.length());

                  addConstrList.insert(nas);
                }
            }
        }

      if (naTable->getRefConstraints().entries() > 0)
        {
          const AbstractRIConstraintList &ariList = naTable->getRefConstraints();
          for (Int32 i = 0; i < ariList.entries(); i++)
            {
              AbstractRIConstraint *ariConstr = ariList[i];
              if (ariConstr->getOperatorType() != ITM_REF_CONSTRAINT)
                continue;

              RefConstraint * refConstr = (RefConstraint*)ariConstr;
              const NAString &constrName = refConstr->getConstraintName().getObjectName();
              
              if ((constrName.length() >= (strlen(TRAF_UNNAMED_CONSTR_PREFIX))) &&
                  (constrName.index(TRAF_UNNAMED_CONSTR_PREFIX) == 0))
                { 
                  const NAString& ansiTableName = 
                    refConstr->getDefiningTableName().getQualifiedNameAsAnsiString(TRUE);
                  
                  const NAString &ansiConstrName =
                    refConstr->getConstraintName().getQualifiedNameAsAnsiString(TRUE);

                  // generate DROP CONSTRAINT stmt
                  sprintf(buf, "ALTER TABLE %s DROP CONSTRAINT %s",
                          ansiTableName.data(),
                          ansiConstrName.data());                      
                  dropConstrList.insert(NAString(buf));

                  // generate ADD CONSTRAINT stmt
                  if (CmpGenRefConstrStr(ariConstr, &space, buf, FALSE, FALSE))
                    {
                      // internal error. Assert.
                      // Should DDL lock be removed here?
                      CMPASSERT(0);
                      return;
                    }

                  NAString nas(buf);
                  char constrNumBuf[20];

                  // constr name is of the format:
                  //  "_Traf_UnNamed_Constr_1_T"
                  // Replace it with:
                  //  "_Traf_UnNamed_Constr_1_TRENAMED"                  
                  //  
                  Int32 constrNum = extractNumVal(constrName);
                  if (constrNum <= 0)
                    {
                      // internal error. Assert.
                      // Should DDL lock be removed here?
                      CMPASSERT(0);
                      return;
                    }
                  
                  NAString stringToSearch = NAString("\"") + NAString(TRAF_UNNAMED_CONSTR_PREFIX) + str_itoa(constrNum, constrNumBuf) + NAString("_") + objectNamePart + NAString("\"");
                  NAString stringToReplace = NAString("\"") + NAString(TRAF_UNNAMED_CONSTR_PREFIX) + str_itoa(constrNum, constrNumBuf) + NAString("_") + newObjectNamePart + NAString("\"");

                  size_t pos = nas.index(stringToSearch.data(), stringToSearch.length(),
                                         0, NAString::exact);
                  if (pos == NA_NPOS)
                    {
                      // internal error. Assert.
                      // Should DDL lock be removed here?
                      CMPASSERT(0);
                      return;
                    } 

                  // replace constraint name with new constraint name
                  nas.replace(pos, stringToSearch.length(),
                              stringToReplace, stringToReplace.length());

                  addConstrList.insert(nas);
                }
            } // for
        } // ref constrs
    }

  Int64 objUID = getObjectUID(&cliInterface,
                              catalogNamePart.data(), schemaNamePart.data(), 
                              objectNamePart.data(),
                              COM_BASE_TABLE_OBJECT_LIT);
  if (objUID < 0)
    {

      ddlResetObjectEpochs(TRUE, TRUE);
      processReturn();

      // Release all DDL object locks
      LOGTRACE(CAT_SQL_LOCK, "Release all DDL object locks");
      CmpCommon::context()->releaseAllDDLObjectLocks();
      return;
    }

  if (!renameTableNode->skipViewCheck())
    {
      // cannot rename if views are using this table
      Queue * usingViewsQueue = NULL;
      cliRC = getUsingViews(&cliInterface, objUID, usingViewsQueue);
      if (cliRC < 0)
        {
          ddlResetObjectEpochs(TRUE, TRUE);
          processReturn();
      
          // Release all DDL object locks
          LOGTRACE(CAT_SQL_LOCK, "Release all DDL object locks");
          CmpCommon::context()->releaseAllDDLObjectLocks();
          return;
        }
  
      if (usingViewsQueue->numEntries() > 0)
        {
          *CmpCommon::diags() << DgSqlCode(-1427)
                              << DgString0("Reason: Operation not allowed if dependent views exist. Drop the views and recreate them after rename.");
      
          ddlResetObjectEpochs(TRUE, TRUE);
          processReturn();

          // Release all DDL object locks
          LOGTRACE(CAT_SQL_LOCK, "Release all DDL object locks");
          CmpCommon::context()->releaseAllDDLObjectLocks();
          return;
        }
    }

  // this operation cannot be done if a xn is already in progress.
  if (!renameTableNode->skipTransactionCheck())
  {
    if (xnInProgress(&cliInterface) || (!GetCliGlobals()->currContext()->getTransaction()->autoCommit()))
    {
      *CmpCommon::diags() << DgSqlCode(-20125)
        << DgString0("This ALTER");

      ddlResetObjectEpochs(TRUE, TRUE);
      processReturn();

      // Release all DDL object locks
      LOGTRACE(CAT_SQL_LOCK, "Release all DDL object locks");
      CmpCommon::context()->releaseAllDDLObjectLocks();
      return;
    }
  }

  NABoolean ddlXns = renameTableNode->ddlXns();

  HbaseStr hbaseTable;
  hbaseTable.val = (char*)extNameForHbase.data();
  hbaseTable.len = extNameForHbase.length();
  
  HbaseStr newHbaseTable;
  newHbaseTable.val = (char*)newExtNameForHbase.data();
  newHbaseTable.len = newExtNameForHbase.length();

  ExpHbaseInterface * ehi = allocEHI(naTable->storageType());
  if (ehi == NULL)
    {
      ddlResetObjectEpochs(TRUE, TRUE);
      processReturn();
      
      // Release all DDL object locks
      LOGTRACE(CAT_SQL_LOCK, "Release all DDL object locks");
      CmpCommon::context()->releaseAllDDLObjectLocks();
      return;
    }

  NABoolean xnWasStartedHere = FALSE;
  if (beginXnIfNotInProgress(&cliInterface, xnWasStartedHere,
                             FALSE /*clearDDLList*/, FALSE /*clearObjectLocks*/))
  {
    ddlResetObjectEpochs(TRUE, TRUE);

    // Release all DDL object locks
    LOGTRACE(CAT_SQL_LOCK, "Release all DDL object locks");
    CmpCommon::context()->releaseAllDDLObjectLocks();
    return;
  }

  // Upgrade DDL lock to read/write
  if (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL))
    {
      retcode = ddlSetObjectEpoch(naTable, STMTHEAP, false /*readsAllowed*/, true /*continue*/);
      if (retcode < 0)
        {
          ddlResetObjectEpochs(TRUE, TRUE);
          goto label_error;
        }
    }

  // skip ConstrList
  if (!USE_UUID_AS_HBASE_TABLENAME && NOT naTable->isPartitionV2Table())
  {
    // drop constraints
    for (Int32 i = 0; i < dropConstrList.entries(); i++)
    {
      cliRC = cliInterface.executeImmediate(dropConstrList[i]);
      if (cliRC < 0)
        {
          cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
          processReturn();
          goto label_error_2;
        }
    }

    // add constraints
    for (Int32 i = 0; i < addConstrList.entries(); i++)
    {
      cliRC = cliInterface.executeImmediate(addConstrList[i]);
      if (cliRC < 0)
        {
          cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
          processReturn();
          goto label_error_2;
        }
    }
  }

  cliRC = updateObjectName(&cliInterface,
                           objUID,
                           catalogNamePart.data(), schemaNamePart.data(),
                           newObjectNamePart.data());
  if (cliRC < 0)
    {
      processReturn();
      goto label_error_2;
    }

  //if partition table rename every patition
  if (naTable->isPartitionV2Table())
  {
    char buf[2500];
    char *newPartEntityName = new (STMTHEAP) char[256];
    NAPartitionArray naPartArry = naTable->getNAPartitionArray();
    HbaseStr hbasePartTable;
    HbaseStr newHbasePartTable;
    NAString oldExtPartNameForHbase;
    NAString newExtPartNameForHbase;
		CorrName partCn;
    for (int i = 0; i < naPartArry.entries(); i++)
    {
      if (naPartArry[i]->hasSubPartition())
      {
        //doesn't care now
      }
      else
      {
        NAString oldPartEntityName = naPartArry[i]->getPartitionEntityName();
        getPartitionTableName(newPartEntityName, 256, newObjectNamePart, naPartArry[i]->getPartitionName());
        partCn = CorrName(oldPartEntityName, STMTHEAP, schemaNamePart, catalogNamePart);
        retcode = lockObjectDDL(partCn.getQualifiedNameObj(),
                                COM_BASE_TABLE_OBJECT,
                                partCn.isVolatile(),
                                partCn.isExternal());
        if (retcode < 0) {
          processReturn();
          return;
        }
        //1. update every partition object name
        Int64 partObjUID = getObjectUID(&cliInterface,
                              catalogNamePart.data(), schemaNamePart.data(),
                              oldPartEntityName.data(),
                              COM_BASE_TABLE_OBJECT_LIT);
        if (partObjUID < 0)
        {
          processReturn();
          goto label_error_2;
        }
        cliRC = updateObjectName(&cliInterface,
                                 partObjUID,
                                 catalogNamePart.data(), schemaNamePart.data(),
                                 newPartEntityName);
        if (cliRC < 0)
        {
          processReturn();
          goto label_error_2;
        }

        str_sprintf(buf, "update %s.\"%s\".%s set partition_entity_name = '%s' where parent_uid = %ld and partition_name = '%s'",
                    getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_PARTITIONS,
                    newPartEntityName, naPartArry[i]->getParentUID(), naPartArry[i]->getPartitionName());
        cliRC = cliInterface.executeImmediate(buf);
        if (cliRC < 0)
        {
          cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
          processReturn();
          goto label_error_2;
        }

        //2. rename hbase table for every partition
        if (!USE_UUID_AS_HBASE_TABLENAME)
        {
          oldExtPartNameForHbase = genHBaseObjName(catalogNamePart, schemaNamePart, oldPartEntityName, naTable->getNamespace());
          hbasePartTable.val = (char*)oldExtPartNameForHbase.data();
          hbasePartTable.len = oldExtPartNameForHbase.length();
          newExtPartNameForHbase = genHBaseObjName(catalogNamePart, schemaNamePart, newPartEntityName, naTable->getNamespace());
          newHbasePartTable.val = (char*)newExtPartNameForHbase.data();
          newHbasePartTable.len = newExtPartNameForHbase.length();

          retcode = ehi->copy(hbasePartTable, newHbasePartTable);
          if (retcode < 0)
          {
            *CmpCommon::diags() << DgSqlCode(-8448)
                                << DgString0((char*)"ExpHbaseInterface::copy()")
                                << DgString1(getHbaseErrStr(-retcode))
                                << DgInt0(-retcode)
                                << DgString2((char*)GetCliGlobals()->getJniErrorStr());
            processReturn();
            cliRC = -1;
            goto label_error;
          }
          retcode = dropHbaseTable(ehi, &hbasePartTable, FALSE, ddlXns);
          if (retcode < 0)
          {
            cliRC = -1;
            goto label_error;
          }
        }
        //3. update object redef time
        cliRC = updateObjectRedefTime(&cliInterface,
                                      catalogNamePart, schemaNamePart, newPartEntityName,
                                      COM_BASE_TABLE_OBJECT_LIT, -1, partObjUID,
                                      naTable->isStoredDesc(),
                                      hasStatsDesc == FALSE ?
                                      naTable->storedStatsAvail() : TRUE);
        if (cliRC < 0)
        {
          processReturn();
          goto label_error;
        }
        //4. update shared cache
        if (naTable->isStoredDesc() && isSeabase(catalogNamePart) &&
            (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)) )
        {
          // remove old name
          insertIntoSharedCacheList(catalogNamePart, schemaNamePart, oldPartEntityName,
                                    COM_BASE_TABLE_OBJECT, SharedCacheDDLInfo::DDL_DELETE);

          // add new name
          insertIntoSharedCacheList(catalogNamePart, schemaNamePart, newPartEntityName,
                                    COM_BASE_TABLE_OBJECT, SharedCacheDDLInfo::DDL_INSERT);
        }
        //5. remove natable
        ActiveSchemaDB()->getNATableDB()->removeNATable
        (partCn,
         ComQiScope::REMOVE_FROM_ALL_USERS, COM_BASE_TABLE_OBJECT,
         renameTableNode->ddlXns(), FALSE);
      }
    }
  }
  // skip identity column
  if (!USE_UUID_AS_HBASE_TABLENAME && NOT naTable->isPartitionV2Table())
  {
    // if there is an identity column, rename the sequence corresponding to it
    found = FALSE;
    while ((NOT found) && (idPos < naTable->getColumnCount()))
    {

      col = naTable->getNAColumnArray()[idPos];
      if (col->isIdentityColumn())
        {
          found = TRUE;
          continue;
        }

      idPos++;
    }

    if (found)
    {
      NAString oldSeqName;
      SequenceGeneratorAttributes::genSequenceName
        (catalogNamePart, schemaNamePart, objectNamePart, col->getColName(),
         oldSeqName);
  
      NAString newSeqName;
      SequenceGeneratorAttributes::genSequenceName
        (catalogNamePart, schemaNamePart, newObjectNamePart, col->getColName(),
         newSeqName);

      Int64 seqUID = getObjectUID(&cliInterface,
                                  catalogNamePart.data(), schemaNamePart.data(), 
                                  oldSeqName.data(),
                                  COM_SEQUENCE_GENERATOR_OBJECT_LIT);
      if (seqUID < 0)
        {
          processReturn();
          goto label_error_2;
        }

      cliRC = updateObjectName(&cliInterface,
                               seqUID,
                               catalogNamePart.data(), schemaNamePart.data(),
                               newSeqName.data());
      if (cliRC < 0)
        {
          processReturn();
          goto label_error_2;
        }
    }
  }

  if (lockRequired(ehi, const_cast<NATable *>(naTable), catalogNamePart, schemaNamePart, objectNamePart, HBaseLockMode::LOCK_X, FALSE/* useHbaseXN */, FALSE
    /* replSync */, FALSE/* incrementalBackup */, FALSE/* asyncOperation */, TRUE/* noConflictCheck */, TRUE/* registerRegion */))
  {
    processReturn();
    cliRC = -1;
    goto label_error_2;
  }

  // do not need rename hbase table
  if (!USE_UUID_AS_HBASE_TABLENAME && NOT naTable->isPartitionV2Table())
  {
    // rename the underlying hbase object
    retcode = ehi->copy(hbaseTable, newHbaseTable);
    if (retcode < 0)
    {
      *CmpCommon::diags() << DgSqlCode(-8448)
                          << DgString0((char*)"ExpHbaseInterface::copy()")
                          << DgString1(getHbaseErrStr(-retcode))
                          << DgInt0(-retcode)
                          << DgString2((char*)GetCliGlobals()->getJniErrorStr());
      
      processReturn();
      
      cliRC = -1;
      goto label_error;
    }

    retcode = dropHbaseTable(ehi, &hbaseTable, FALSE, ddlXns);
    if (retcode < 0)
    {
      cliRC = -1;
      goto label_error; 
    }
  }

  if (naTable->incrBackupEnabled())
  {
    //delete the original table from xdc_ddl
    cliRC = deleteFromXDCDDLTable(&cliInterface,
                                  catalogNamePart.data(), schemaNamePart.data(),
                                  objectNamePart.data(), COM_BASE_TABLE_OBJECT_LIT);
    //insert new table to xdc_ddl
    cliRC = updateSeabaseXDCDDLTable(&cliInterface,
                                     catalogNamePart.data(), schemaNamePart.data(),
                                     newObjectNamePart.data(), COM_BASE_TABLE_OBJECT_LIT);
    if (cliRC < 0)
    {
      processReturn();
      goto label_error;
    }
  }
  //update binlog metadata
  //update the table column definition string in binlog
  binlogTableName = schemaNamePart + "." + objectNamePart;
  if (!ComIsTrafodionReservedSchemaName(schemaNamePart) && naTable->isSeabaseTable() == TRUE) {
    //first , get the current string
    retcode = ehi->getTableDefForBinlog(binlogTableName, &defStringArray);
    if (retcode < 0 )
    {
        *CmpCommon::diags() << DgSqlCode(-3242)
                            << DgString0("Cannot getTableDefforBinlog.");
        processReturn();
        return;
    }
    oldCols = &defStringArray->at(0);
    oldKeys = &defStringArray->at(1);
    snprintf(fullColBuf, oldCols->len, "%s", oldCols->val);
    snprintf(fullKeyBuf, oldKeys->len, "%s", oldKeys->val);
    fullColBufString = fullColBuf;
    fullKeyBufString = fullKeyBuf;
    //update the meta for binlog_reader
    retcode = ehi->updateTableDefForBinlog(newbinlogTableName, fullColBufString , fullKeyBufString, 0);
  } 
  cliRC = updateObjectRedefTime(&cliInterface,
                                catalogNamePart, schemaNamePart, newObjectNamePart,
                                COM_BASE_TABLE_OBJECT_LIT, -1, objUID,
                                naTable->isStoredDesc(),
                                hasStatsDesc == FALSE ?
                                naTable->storedStatsAvail() : TRUE);
  if (cliRC < 0)
    {
      processReturn();
      goto label_error;
    }

 if (naTable->isStoredDesc() && isSeabase(catalogNamePart) &&
     (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)) )
    {
      // remove old name
      insertIntoSharedCacheList(catalogNamePart, schemaNamePart, objectNamePart,
                                COM_BASE_TABLE_OBJECT, SharedCacheDDLInfo::DDL_DELETE);

      // add new name
      insertIntoSharedCacheList(catalogNamePart, schemaNamePart, newObjectNamePart,
                                COM_BASE_TABLE_OBJECT, SharedCacheDDLInfo::DDL_INSERT);
    }

  ActiveSchemaDB()->getNATableDB()->removeNATable
    (cn,
     ComQiScope::REMOVE_FROM_ALL_USERS, COM_BASE_TABLE_OBJECT,
     renameTableNode->ddlXns(), FALSE);

  endXnIfStartedHere(&cliInterface, xnWasStartedHere, 0);

  deallocEHI(ehi); 

  //Since table is renamed, createSnapshot internally.
  //Its a noop if table is not incremental. 
  if (createSnapshotForIncrBackup(
           &cliInterface, naTable->incrBackupEnabled(),
           naTable->getNamespace(),
           catalogNamePart, schemaNamePart, newObjectNamePart, naTable->objDataUID().castToInt64()))
   {
      processReturn();
      cliRC = -1;
   }
  
  ddlResetObjectEpochs(FALSE, TRUE);

  // Release all DDL object locks
  LOGTRACE(CAT_SQL_LOCK, "Release all DDL object locks");
  CmpCommon::context()->releaseAllDDLObjectLocks();
  return;

 label_error:  // come here after HBase copy
  retcode = dropHbaseTable(ehi, &newHbaseTable, FALSE, FALSE);

 label_error_2:  // come here after beginXnIfNotInProgress but before HBase copy
  endXnIfStartedHere(&cliInterface, xnWasStartedHere, cliRC);
  
  if (ehi != NULL)
    deallocEHI(ehi); 

  ddlResetObjectEpochs(TRUE, TRUE);

  // Release all DDL object locks
  LOGTRACE(CAT_SQL_LOCK, "Release all DDL object locks");
  CmpCommon::context()->releaseAllDDLObjectLocks();
  return;
}

// ----------------------------------------------------------------------------
// alterSeabaseTableRenamePartition
// 	 ALTER TABLE <table> RENAME PARTITION <partNm1> TO <partNm2>
//
// This statement runs in a transaction
//     A MARK FOR REPLACE UID, a better method needs to be used
// ----------------------------------------------------------------------------
void CmpSeabaseDDL::alterSeabaseTableRenamePartition(StmtDDLAlterTableRenamePartition* alterTableRenamePartNode,
                                                                 NAString &currCatName,
                                                                 NAString &currSchName)
{
  Lng32 cliRC = 0;
  Lng32 retcode = 0;
  NABoolean isForClause = alterTableRenamePartNode->getIsForClause();
  UInt32 op = 0;

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());

  retcode = lockObjectDDL(alterTableRenamePartNode);
  if (retcode < 0) {
    processReturn();
    return;
  }

  NAString tabName = alterTableRenamePartNode->getTableName();
  NAString catalogNamePart;
  NAString schemaNamePart;
  NAString objectNamePart;
  NAString extTableName;
  NAString extNameForHbase;
  CorrName cn;
  NATable *naTable = NULL;

  //error check
  retcode = setupAndErrorChecks(tabName,
                              alterTableRenamePartNode->getOrigTableNameAsQualifiedName(),
                              currCatName,
                              currSchName,
                              catalogNamePart,
                              schemaNamePart,
                              objectNamePart,
                              extTableName,
                              extNameForHbase,
                              cn,
                              &naTable,
                              FALSE, FALSE,
                              &cliInterface,
                              COM_BASE_TABLE_OBJECT,
                              SQLOperation::ALTER_TABLE,
                              FALSE,
                              TRUE/*process hiatus*/);
  if (retcode < 0)
  {
    processReturn();
    return;
  }

  //base table is partition table
  if (!naTable->isPartitionV2Table())
  {
    *CmpCommon::diags() << DgSqlCode(-8304) << DgString0(tabName);
    processReturn();
    return;
  }

  Int64 objectUid = naTable->objectUid().castToInt64();
  UpdateObjRefTimeParam updObjRefParam(-1, objectUid, naTable->isStoredDesc(), naTable->storedStatsAvail());
  RemoveNATableParam removeNATableParam(ComQiScope::REMOVE_FROM_ALL_USERS, alterTableRenamePartNode->ddlXns(), FALSE);

  char *oldFullPartName;
  NAString oldPtNm;
  NAString newPtNm;
  NAPartitionArray naPartArry = naTable->getNAPartitionArray();
  NABoolean found = FALSE;
  //get old partition full name
  if (isForClause)
  {
    const LIST(NAString) *partName = NULL;
    NAString oldPartEntityName;
    partName = alterTableRenamePartNode->getPartNameList();
    oldPartEntityName = (*partName)[0];
    oldFullPartName = CONST_CAST(char*, oldPartEntityName.data());
    //get partitionName_ from naPartArry
    for(int i = 0; i < naPartArry.entries(); i++)
    {
      if ( naPartArry[i]->getPartitionEntityName() == oldPartEntityName )
      {
        found = TRUE;
        oldPtNm = naPartArry[i]->getPartitionName();
        break;
      }
    }
    if (!found)
    {
      //should raise an error
      *CmpCommon::diags() << DgSqlCode(-8306) << DgString0("The partition number is invalid or out-of-range");
      processReturn();
      return;
    }
  }
  else
  {
    //get old partition name from natable
    oldPtNm = alterTableRenamePartNode->getOldPartName();
    for(int ii = 0; ii < naPartArry.entries(); ii++)
    {
      if ( naPartArry[ii]->getPartitionName() == oldPtNm )
      {
        found = TRUE;
        oldFullPartName = CONST_CAST(char*, naPartArry[ii]->getPartitionEntityName());
        break;
      }
    }
    if (!found)
    {
      //should raise an error
      *CmpCommon::diags() << DgSqlCode(-2301) << DgString0(oldPtNm);
      processReturn();
      return;
    }
  }

  //get new partition name
  newPtNm = alterTableRenamePartNode->getNewPartName();

  //partition name check
  if (!isExistsPartitionName(naTable, oldPtNm))
  {
    *CmpCommon::diags() << DgSqlCode(-2301) << DgString0(oldPtNm);
    processReturn();
    return;
  }

  if (oldPtNm == newPtNm)
  {
    *CmpCommon::diags() << DgSqlCode(-8307)
			<< DgString0(newPtNm)
			<< DgString1("the old partition name:");
    processReturn();
    return;
  }

  if (isExistsPartitionName(naTable, newPtNm))
  {
    *CmpCommon::diags() << DgSqlCode(-8307)
			<< DgString0(newPtNm)
			<< DgString1("that of any other partition of the object");
    processReturn();
    return;
  }

  //add ddl lock on partition table
  QualifiedName qn(NAString(oldFullPartName), schemaNamePart, catalogNamePart);
  retcode = lockObjectDDL(qn, COM_BASE_TABLE_OBJECT,
                          naTable->isVolatileTable(),
                          naTable->isTrafExternalTable());
  if (retcode < 0)
  {
    processReturn();
    return;
  }

  CorrName cnPt(NAString(oldFullPartName), STMTHEAP,
                schemaNamePart, catalogNamePart);

  NABoolean ddlXns = alterTableRenamePartNode->ddlXns();
  ExpHbaseInterface * ehi = allocEHI(naTable->storageType());
  if (ehi == NULL)
  {
    processReturn();
    return;
  }

  NABoolean xnWasStartedHere = FALSE;
  if (beginXnIfNotInProgress(&cliInterface, xnWasStartedHere, TRUE /*clearDDLList*/, FALSE /*clearObjectLocks*/))
  {
    processReturn();
    return;
  }

  //update table_partitions
  cliRC = updateTablePartitionsPartitionName(&cliInterface, currCatName, currSchName, oldPtNm,
                                             newPtNm, objectUid);
  if (cliRC < 0)
  {
    processReturn();
    goto label_error;
  }

  //after rename should remove natable of partition and base table from shared cache
  ActiveSchemaDB()->getNATableDB()->removeNATable
    (cnPt,
     ComQiScope::REMOVE_FROM_ALL_USERS, COM_BASE_TABLE_OBJECT,
     alterTableRenamePartNode->ddlXns(), FALSE);

  setCacheAndStoredDescOp(op, UPDATE_REDEF_TIME);
  setCacheAndStoredDescOp(op, REMOVE_NATABLE);
  if (naTable->incrBackupEnabled())
    setCacheAndStoredDescOp(op, UPDATE_XDC_DDL_TABLE);
  if (naTable->isStoredDesc())
    setCacheAndStoredDescOp(op, UPDATE_SHARED_CACHE);

  cliRC = updateCachesAndStoredDesc(&cliInterface, catalogNamePart, schemaNamePart, objectNamePart,
                         COM_BASE_TABLE_OBJECT, op, &removeNATableParam, &updObjRefParam, SharedCacheDDLInfo::DDL_UPDATE);
  if (cliRC < 0)
  {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_rollback;
  }

  endXnIfStartedHere(&cliInterface, xnWasStartedHere, 0, FALSE/*modifyEpochs*/, FALSE /*clearObjectLocks*/);
  deallocEHI(ehi);
  return;
label_rollback:
  endXnIfStartedHere(&cliInterface, xnWasStartedHere, -1, FALSE/*modifyEpochs*/, FALSE /*clearObjectLocks*/);
  if (ehi)
	  deallocEHI(ehi);
	return;
label_error:
  if (ehi)
	  deallocEHI(ehi);

  ActiveSchemaDB()->getNATableDB()->removeNATable
  (cn,
   ComQiScope::REMOVE_FROM_ALL_USERS, COM_BASE_TABLE_OBJECT,
   alterTableRenamePartNode->ddlXns(), FALSE);

  endXnIfStartedHere(&cliInterface, xnWasStartedHere, -1, FALSE/*modifyEpochs*/, FALSE /*clearObjectLocks*/);
  return;
}

// ----------------------------------------------------------------------------
// alterSeabaseTableRenamePartition
// 	 ALTER TABLE <table> RENAME PARTITION <partNm1> TO <partNm2>
//
// This statement runs in a transaction
// ----------------------------------------------------------------------------
void CmpSeabaseDDL::alterSeabaseTableSplitPartition(StmtDDLAlterTableSplitPartition* alterTableSplitPartNode,
                                                                 NAString &currCatName,
                                                                 NAString &currSchName)
{

#define doQueryInTX                                                 \
  {                                                                 \
    cliRC = cliInterface.executeImmediate((char*)queryStr.data());  \
    if (cliRC < 0) {                                                \
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());      \
      goto label_rollback;                                          \
    }                                                               \
  }

  Lng32 cliRC = 0;
  Lng32 retcode = 0;
  Int64 flags = 0;
  NAString tabName = alterTableSplitPartNode->getTableName();
  NAString catalogNamePart, schemaNamePart, objectNamePart;
  NAString extTableName, extNameForHbase;
  NATable *naTable = NULL;
  CorrName cn;
  NAString partEntityName;
  NAString lastPartExpr;
  NAString PKcolName;
  UInt32 op = 0;

  const LIST(NAString) *splitedPartNameList = alterTableSplitPartNode->getSplitedPartNameList();

  short * SPPOSITION = alterTableSplitPartNode->getStatus();

  ItemExpr * valueExpr = alterTableSplitPartNode->splitedKey();

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());

  cliRC = lockObjectDDL(alterTableSplitPartNode);
  if (cliRC < 0)
  {
    processReturn();
    return;
  }

  //error check
  cliRC = setupAndErrorChecks(tabName,
                              alterTableSplitPartNode->getOrigTableNameAsQualifiedName(),
                              currCatName,
                              currSchName,
                              catalogNamePart,
                              schemaNamePart,
                              objectNamePart,
                              extTableName,
                              extNameForHbase,
                              cn,
                              &naTable,
                              FALSE, FALSE,
                              &cliInterface,
                              COM_BASE_TABLE_OBJECT,
                              SQLOperation::ALTER_TABLE,
                              FALSE,
                              TRUE/*process hiatus*/);
  if (cliRC < 0)
  {
    processReturn();
    return;
  }

  if (!naTable->isPartitionV2Table())
  {
    *CmpCommon::diags() << DgSqlCode(-8304) << DgString0(tabName);
    processReturn();
    return;
  }

  if (alterTableSplitPartNode->getPartEntityType() == StmtDDLAlterTable::OPT_SUBPARTITION_ENTITY)
  {
    *CmpCommon::diags() << DgSqlCode(-8306) << DgString0("sub-partition is not support yet for exchange partition");
    processReturn();
    return;
  }

  NAPartition * naPartition = alterTableSplitPartNode->getNAPartition();
  ComASSERT(naPartition);
  
  BindWA bindWA(ActiveSchemaDB(), CmpCommon::context(), TRUE/*inDDL*/);

  TableDesc* tableDesc = NULL;
  tableDesc = bindWA.createTableDesc(naTable, cn, FALSE, NULL);
  if (!tableDesc)
  {
    // An internal executor error occurred.
    *CmpCommon::diags() << DgSqlCode(-8001);
    processReturn();
    return;
  }

  Int64 baseObjUID = naTable->objectUid().castToInt64();
  RemoveNATableParam removeNATableParam(ComQiScope::REMOVE_FROM_ALL_USERS, alterTableSplitPartNode->ddlXns(), FALSE);

  //only support range partition for now
  if (naTable->partitionType() != COM_RANGE_PARTITION)
  {
    // An internal executor error occurred.
    *CmpCommon::diags() << DgSqlCode(-8001);
    processReturn();
    return;
  }

  // checkout splited values is true
  partEntityName = naPartition->getPartitionEntityName();

  NAString splitkey;
  NABoolean dummyNegate;

  if (valueExpr->getOperatorType() == ITM_CONSTANT)
    if (((ConstValue*)valueExpr)->isNull())
      splitkey = "MAXVALUE";
    else
    {
      const NAType* type = ((ConstValue*)valueExpr)->getType();
      if (type && type->getTypeQualifier() == NA_CHARACTER_TYPE)
        splitkey = valueExpr->castToConstValue(dummyNegate)->getText();
      else
        splitkey = valueExpr->castToConstValue(dummyNegate)->getConstStr();
    }
  else if (valueExpr->getOperatorType() == ITM_ITEM_LIST)
  {
    splitkey = ((ItemList*)valueExpr)->getConstantTextNullReplace(NAString("MAXVALUE"), TRUE);
  }
  else
  {
    valueExpr->unparse(splitkey, PARSER_PHASE, USER_FORMAT);
  }

  char tgtPartTableName[256];
  naTable->getParitionColNameAsString(PKcolName, FALSE);
  Int32 partPosition = naPartition->getPartPosition();

  NABoolean xnWasStartedHere = FALSE;
  if (beginXnIfNotInProgress(&cliInterface, xnWasStartedHere,
                             TRUE /*clearDDLList*/, FALSE /*clearObjectLocks*/))
  {
    processReturn();
    return;
  }

  NAString partKey;
  for (int i = 0; i < naPartition->getBoundaryValueList().entries(); i++)
  {
    partKey += naPartition->getBoundaryValueList()[i]->highValue;
    if (i != naPartition->getBoundaryValueList().entries() - 1)
      partKey += ",";
  }

  short baseID = 0;
  short model = 0;
  if (SPPOSITION[0] == SPPOSITION[1])
    baseID = 0;
  else if (SPPOSITION[0] == 1)
    baseID = 1;
  else if (SPPOSITION[1] == 1)
    baseID = 0;

  QualifiedName splitedPart(partEntityName, schemaNamePart, catalogNamePart, STMTHEAP);
  cliRC = lockObjectDDL(splitedPart, COM_BASE_TABLE_OBJECT, FALSE, FALSE);
  if (cliRC < 0)
  {
    processReturn();
    return;
  }

  std::string partitionTableName;
  Int64 partitionUID = 0;

  UpdateObjRefTimeParam updObjRefParamBasetable;
  UpdateObjRefTimeParam updObjRefParamNewtable;
  UpdateObjRefTimeParam updObjRefParamSplitedtable;

  cliInterface.holdAndSetCQD("TRAF_UPSERT_THROUGH_PARTITIONS", "ON");
  cliInterface.holdAndSetCQD("TRAF_PARTITIONV2_CONSTRAINT_RAISE_ERROR", "OFF");
  {
    char partitionTableNamestr[256];
    NAString queryStr;
    getPartitionTableName(partitionTableNamestr, 256, objectNamePart, (*splitedPartNameList)[baseID]);
    partitionTableName = std::string(partitionTableNamestr);
    partitionTableName += std::to_string(reinterpret_cast<long>(this));
    // create a new table
    queryStr.format("create partition table %s.\"%s\".%s like %s.\"%s\".%s;",
                    catalogNamePart.data(), schemaNamePart.data(), ToAnsiIdentifier(partitionTableName.data()).data(),
                    catalogNamePart.data(), schemaNamePart.data(), ToAnsiIdentifier(objectNamePart).data());

    doQueryInTX;

    partitionUID = getObjectUID(&cliInterface, catalogNamePart.data(), schemaNamePart.data(), partitionTableName.data(), COM_BASE_TABLE_OBJECT_LIT);

    QualifiedName splitedPartnew(NAString(partitionTableName), schemaNamePart, catalogNamePart, STMTHEAP);
    cliRC = lockObjectDDL(splitedPartnew, COM_BASE_TABLE_OBJECT, FALSE, FALSE);
    if (cliRC < 0)
    {
      processReturn();
      return;
    }

    // load date into new created table
    queryStr.format("upsert using load into %s.\"%s\".\"%s\" select * from %s.\"%s\".\"%s\" where (%s) %s (%s)",
                    catalogNamePart.data(), schemaNamePart.data(), partitionTableName.data(),
                    catalogNamePart.data(), schemaNamePart.data(), partEntityName.data(),
                    PKcolName.data(),
                    (baseID == 0 ? "<" : ">="),
                    splitkey.data());
    doQueryInTX;

    // Delete data that does not meet the criteria from the original table
    queryStr.format("delete from %s.\"%s\".\"%s\" where (%s) %s (%s)",
                    catalogNamePart.data(), schemaNamePart.data(), partEntityName.data(),
                    PKcolName.data(),
                    (baseID == 0 ? "<" : ">="),
                    splitkey.data());
    doQueryInTX;

    // increse position
    queryStr.format("update %s.\"%s\".%s set PARTITION_ORDINAL_POSITION = PARTITION_ORDINAL_POSITION + 1 "
                    " where PARTITION_ORDINAL_POSITION >%s%d and PARENT_UID = %ld;",
                    getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_PARTITIONS,
                    (baseID == 0 ? "= " : " "),
                    partPosition, baseObjUID);
    doQueryInTX;

    // update partition info
    if (SPPOSITION[0] == SPPOSITION[1])
    {
      queryStr.format("update %s.\"%s\".%s set PARTITION_NAME = '%s' where PARTITION_NAME = '%s' and PARENT_UID = %ld;",
               getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_PARTITIONS,
               (*splitedPartNameList)[1].data(), naPartition->getPartitionName(), baseObjUID);
      doQueryInTX;
    }
    else if (SPPOSITION[0] == 1)
    {
      queryStr.format("update %s.\"%s\".%s set PARTITION_EXPRESSION = '%s' where PARTITION_NAME = '%s' and PARENT_UID = %ld;",
               getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_PARTITIONS,
               splitkey.data(), naPartition->getPartitionName(), baseObjUID);
      doQueryInTX;
    }

    cliRC = updateSeabaseTablePartitions(&cliInterface, baseObjUID, (*splitedPartNameList)[baseID].data(), partitionTableName.data(),
                                         partitionUID, COM_PRIMARY_PARTITION_LIT,
                                         (baseID == 0 ? partPosition : partPosition+1), 
                                         (baseID == 0 ? splitkey.data() : partKey.data()));
    if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      goto label_rollback;
    }

  }
  cliInterface.restoreCQD("TRAF_UPSERT_THROUGH_PARTITIONS");
  cliInterface.restoreCQD("TRAF_PARTITIONV2_CONSTRAINT_RAISE_ERROR");

  updObjRefParamBasetable = UpdateObjRefTimeParam(-1, baseObjUID, naTable->isStoredDesc(), naTable->storedStatsAvail());
  updObjRefParamNewtable = UpdateObjRefTimeParam(-1, partitionUID, naTable->isStoredDesc(), naTable->storedStatsAvail());
  updObjRefParamSplitedtable = UpdateObjRefTimeParam(-1, naPartition->getPartitionUID(), naTable->isStoredDesc(), naTable->storedStatsAvail());

  setCacheAndStoredDescOp(op, UPDATE_REDEF_TIME);
  setCacheAndStoredDescOp(op, REMOVE_NATABLE);
  if (naTable->incrBackupEnabled())
    setCacheAndStoredDescOp(op, UPDATE_XDC_DDL_TABLE);
  if (naTable->isStoredDesc())
    setCacheAndStoredDescOp(op, UPDATE_SHARED_CACHE);

  // new created table
  cliRC = updateCachesAndStoredDesc(&cliInterface, catalogNamePart, schemaNamePart, NAString(partitionTableName),
    COM_BASE_TABLE_OBJECT, op, &removeNATableParam, &updObjRefParamNewtable, SharedCacheDDLInfo::DDL_UPDATE);
  if (cliRC < 0)
  {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_rollback;
  }

  // splited table
  cliRC = updateCachesAndStoredDesc(&cliInterface, catalogNamePart, schemaNamePart, partEntityName,
    COM_BASE_TABLE_OBJECT, op, &removeNATableParam, &updObjRefParamSplitedtable, SharedCacheDDLInfo::DDL_UPDATE);
  if (cliRC < 0)
  {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_rollback;
  }

  // partition table
  cliRC = updateCachesAndStoredDesc(&cliInterface, catalogNamePart, schemaNamePart, objectNamePart,
    COM_BASE_TABLE_OBJECT, op, &removeNATableParam, &updObjRefParamBasetable, SharedCacheDDLInfo::DDL_UPDATE);
  if (cliRC < 0)
  {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_rollback;
  }

  endXnIfStartedHere(&cliInterface, xnWasStartedHere, 0, FALSE/*modifyEpochs*/, FALSE /*clearObjectLocks*/);
  {
    processReturn();
    return;
  }

label_rollback:
  endXnIfStartedHere(&cliInterface, xnWasStartedHere, -1, FALSE/*modifyEpochs*/, FALSE /*clearObjectLocks*/);
  return;

}

// ----------------------------------------------------------------------------
// alterSeabaseTableStoredDesc
//    ALTER TABLE <table> <options> STORED DESC
//
// This statement runs in a transaction
// ----------------------------------------------------------------------------
void CmpSeabaseDDL::alterSeabaseTableStoredDesc(
     StmtDDLAlterTableStoredDesc * alterStoredDesc,
     NAString &currCatName, NAString &currSchName)
{
  Lng32 retcode = 0;
  Lng32 cliRC = 0;

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, 
                               CmpCommon::context()->sqlSession()->getParentQid());

  NAString tabName = alterStoredDesc->getTableName();
  NAString catalogNamePart;
  NAString schemaNamePart;
  NAString objectNamePart;
  NAString extTableName;
  NAString extNameForHbase;

  // Altering stored descriptor attributes for schema object
  if (isSchemaObjectName(tabName))
    {
      cliRC = alterSchemaTableDesc(&cliInterface, alterStoredDesc->getType(), 
                                   tabName, alterStoredDesc->ddlXns());                           
      if (cliRC < 0)
        {
          processReturn ();
        }
      return;
    }

  retcode = lockObjectDDL(alterStoredDesc);
  if (retcode < 0) {
    processReturn();
    return;
  }

  NATable * naTable = NULL;
  CorrName cn;
  retcode = 
    setupAndErrorChecks(tabName, 
                        alterStoredDesc->getOrigTableNameAsQualifiedName(),
                        currCatName, currSchName,
                        catalogNamePart, schemaNamePart, objectNamePart,
                        extTableName, extNameForHbase, cn,
                        &naTable, 
                        FALSE, TRUE,
                        &cliInterface,
                        COM_BASE_TABLE_OBJECT,
                        SQLOperation::ALTER_TABLE,
                        FALSE);
  if (retcode < 0)
    {
      processReturn();
      return;
    }

  // Table exists, update the epoch value to lock out other user access
  if (! Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL))
    {
      retcode = ddlSetObjectEpoch(naTable, STMTHEAP);
      if (retcode < 0)
        {
          processReturn();
          return;
        }
    }

  Int64 objUID = naTable->objectUid().castToInt64();
  NABoolean storedDesc = naTable->isStoredDesc();

  if (naTable->isPartitionV2Table())
  {
    retcode = lockObjectDDL(naTable, catalogNamePart, schemaNamePart, true);
    if (retcode < 0)
    {
      processReturn();
      return;
    }
  }

  // Add entry to DDL list to update shared cache at txn commit time
  SharedCacheDDLInfo::DDLOperation op = SharedCacheDDLInfo::DDL_UNKNOWN;
  NABoolean addToSharedCache = TRUE;
  switch (alterStoredDesc->getType())
    {
      case StmtDDLAlterTableStoredDesc::GENERATE_DESC:
      case StmtDDLAlterTableStoredDesc::GENERATE_STATS:
      case StmtDDLAlterTableStoredDesc::GENERATE_STATS_FORCE:
      case StmtDDLAlterTableStoredDesc::GENERATE_STATS_INTERNAL:
        op = SharedCacheDDLInfo::DDL_INSERT;
        break;
      case StmtDDLAlterTableStoredDesc::DELETE_DESC:
        op = SharedCacheDDLInfo::DDL_DELETE;
        break;
      case StmtDDLAlterTableStoredDesc::DELETE_STATS:
        op = SharedCacheDDLInfo::DDL_UPDATE;
        break;
      case StmtDDLAlterTableStoredDesc::DISABLE:
        op = SharedCacheDDLInfo::DDL_DISABLE;
        break;
      case StmtDDLAlterTableStoredDesc::ENABLE:
        op = SharedCacheDDLInfo::DDL_ENABLE;
        break;
      default:
        addToSharedCache = FALSE;
    }

  if (addToSharedCache)
    insertIntoSharedCacheList(catalogNamePart, schemaNamePart, objectNamePart,
                              COM_BASE_TABLE_OBJECT, op);

  if (naTable->hasSecondaryIndexes()) // user indexes
    {
      const NAFileSetList &naFsList = naTable->getIndexList();
      for (Lng32 i = 0; i < naFsList.entries(); i++)
        {
          const NAFileSet * naFS = naFsList[i];

          // skip clustering index
          if (naFS->getKeytag() == 0)
                continue;

          const QualifiedName &indexName = naFS->getFileSetName();
  
          if (addToSharedCache)
            insertIntoSharedCacheList(indexName.getCatalogNameAsAnsiString(), 
                                      indexName.getSchemaNameAsAnsiString(),
                                      indexName.getObjectName(),
                                      COM_INDEX_OBJECT, op);
       }
    }

  if (naTable->isPartitionV2Table() && addToSharedCache)
  {
    const NAPartitionArray & partitionArray = naTable->getPartitionArray();
    for (Lng32 i = 0; i < partitionArray.entries(); i++)
    {
      if (partitionArray[i]->hasSubPartition())
      {
        const NAPartitionArray *subpArray = partitionArray[i]->getSubPartitions();
        for (Lng32 j = 0; j < subpArray->entries(); j++)
        {
          insertIntoSharedCacheList(catalogNamePart, schemaNamePart,
                                    (*subpArray)[j]->getPartitionEntityName(),
                                    COM_BASE_TABLE_OBJECT, op);
        }
      }
      else
      {
        insertIntoSharedCacheList(catalogNamePart, schemaNamePart,
                                  partitionArray[i]->getPartitionEntityName(),
                                  COM_BASE_TABLE_OBJECT, op);
      }
    }
  }

  if (alterStoredDesc->getType() == StmtDDLAlterTableStoredDesc::GENERATE_DESC)
    {
      // switch compiler and compile metadata queries in META context.
      if (switchCompiler(CmpContextInfo::CMPCONTEXT_TYPE_META))
        return;

      cliRC = 
        updateObjectRedefTime(&cliInterface, 
                              catalogNamePart, schemaNamePart, objectNamePart,
                              COM_BASE_TABLE_OBJECT_LIT,
                              -1, objUID, TRUE, FALSE);

      if (cliRC < 0)
        {
          // switch compiler back
          switchBackCompiler();
          
          processReturn ();
          
          return;
        }

      // if this table has secondary indexes, create a stored desc for each
      // index. This is used when index is accessed as a table.
      if (naTable->hasSecondaryIndexes()) // user indexes
        {
          const NAFileSetList &naFsList = naTable->getIndexList();
          
          for (Lng32 i = 0; i < naFsList.entries(); i++)
            {
              const NAFileSet * naFS = naFsList[i];
              
              // skip clustering index
              if (naFS->getKeytag() == 0)
                continue;
              
              Int64 indexUID = naFS->getIndexUID();
              const QualifiedName &indexName = naFS->getFileSetName();
              cliRC = 
                updateObjectRedefTime(&cliInterface, 
                                      indexName.getCatalogName().data(),
                                      indexName.getSchemaName().data(),
                                      indexName.getObjectName().data(),
                                      COM_INDEX_OBJECT_LIT,
                                      -1, indexUID, TRUE, FALSE);   
              if (cliRC < 0)
                {
                  switchBackCompiler();
                  processReturn ();
                  
                  return;
                }      
            } // for
        } // secondary indexes

      if (naTable->isPartitionV2Table())
      {
        const NAPartitionArray & partitionArray = naTable->getPartitionArray();
        for (Lng32 i = 0; i < partitionArray.entries(); i++)
        {
          if (partitionArray[i]->hasSubPartition())
          {
            const NAPartitionArray *subpArray = partitionArray[i]->getSubPartitions();
            for (Lng32 j = 0; j < subpArray->entries(); j++)
            {
              cliRC = updateObjectRedefTime(&cliInterface, 
                                            catalogNamePart, schemaNamePart, 
                                            (*subpArray)[j]->getPartitionEntityName(),
                                            COM_BASE_TABLE_OBJECT_LIT,
                                            -1, (*subpArray)[j]->getPartitionUID(), TRUE, FALSE);

              if (cliRC < 0)
              {
                // switch compiler back
                switchBackCompiler();
                processReturn ();
                return;
              }
            }
          }
          else
          {
            cliRC = updateObjectRedefTime(&cliInterface, 
                                          catalogNamePart, schemaNamePart, 
                                          partitionArray[i]->getPartitionEntityName(),
                                          COM_BASE_TABLE_OBJECT_LIT,
                                          -1, partitionArray[i]->getPartitionUID(), TRUE, FALSE);

            if (cliRC < 0)
            {
              // switch compiler back
              switchBackCompiler();
              processReturn ();
              return;
            }
          }
        }
      }


      if ((naTable->hasLobColumn()) && (naTable->lobV2()))
        {
          char lobChunksTableNameBuf[1024];
          str_sprintf(lobChunksTableNameBuf, "%s%020ld", 
                      LOB_CHUNKS_V2_PREFIX, objUID);
          
          Int64 lobUID = getObjectUID(
               &cliInterface,
               catalogNamePart.data(), schemaNamePart.data(), 
               lobChunksTableNameBuf, COM_BASE_TABLE_OBJECT_LIT);
          if (lobUID >= 0)
            {          
              cliRC = 
                updateObjectRedefTime(&cliInterface, 
                                      catalogNamePart, schemaNamePart, 
                                      lobChunksTableNameBuf,
                                      COM_BASE_TABLE_OBJECT_LIT,
                                      -1, lobUID, TRUE, FALSE);
              
              if (cliRC < 0)
                {
                  switchBackCompiler();
                  processReturn ();
                  return;
                }
            }
        }


      switchBackCompiler();
    }
  else if ((alterStoredDesc->getType() == StmtDDLAlterTableStoredDesc::GENERATE_STATS) ||
           (alterStoredDesc->getType() == StmtDDLAlterTableStoredDesc::GENERATE_STATS_FORCE) ||
           (alterStoredDesc->getType() == StmtDDLAlterTableStoredDesc::GENERATE_STATS_INTERNAL))
    {
      // switch compiler and compile metadata queries in META context.
      if (switchCompiler(CmpContextInfo::CMPCONTEXT_TYPE_META))
        return;

      cliRC = 
        updateObjectStats(
             &cliInterface, 
             catalogNamePart, schemaNamePart, objectNamePart,
             COM_BASE_TABLE_OBJECT_LIT,
             (alterStoredDesc->getType() == StmtDDLAlterTableStoredDesc::GENERATE_STATS_INTERNAL ? -2:-1),//GENERATE_STATS_INTERNAL don't update redef time
             objUID, TRUE,
             (alterStoredDesc->getType() == StmtDDLAlterTableStoredDesc::GENERATE_STATS_FORCE ? TRUE : FALSE));

      // if this table has secondary indexes, create stored stats for each
      // index. This is used when index is accessed as a table.
      if (naTable->hasSecondaryIndexes()) // user indexes
        {
          const NAFileSetList &naFsList = naTable->getIndexList();
          
          for (Lng32 i = 0; i < naFsList.entries(); i++)
            {
              const NAFileSet * naFS = naFsList[i];
              
              // skip clustering index
              if (naFS->getKeytag() == 0)
                continue;
              
              Int64 indexUID = naFS->getIndexUID();
              const QualifiedName &indexName = naFS->getFileSetName();
              cliRC = 
                updateObjectStats(
                     &cliInterface, 
                     indexName.getCatalogName().data(),
                     indexName.getSchemaName().data(),
                     indexName.getObjectName().data(),
                     COM_INDEX_OBJECT_LIT,
                     -1, indexUID, TRUE,
                     (alterStoredDesc->getType() == StmtDDLAlterTableStoredDesc::GENERATE_STATS_FORCE ? TRUE : FALSE));
              if (cliRC < 0)
                {
                  switchBackCompiler();
                  processReturn ();
                  
                  return;
                }      
            } // for
        } // secondary indexes

      if (naTable->isPartitionV2Table())
      {
        const NAPartitionArray & partitionArray = naTable->getPartitionArray();
        for (Lng32 i = 0; i < partitionArray.entries(); i++)
        {
          if (partitionArray[i]->hasSubPartition())
          {
            const NAPartitionArray *subpArray = partitionArray[i]->getSubPartitions();
            for (Lng32 j = 0; j < subpArray->entries(); j++)
            {
              cliRC = updateObjectStats(&cliInterface, 
                                        catalogNamePart, schemaNamePart,
                                        (*subpArray)[j]->getPartitionEntityName(),
                                        COM_BASE_TABLE_OBJECT_LIT,
                                        (alterStoredDesc->getType() == StmtDDLAlterTableStoredDesc::GENERATE_STATS_INTERNAL ? -2:-1),
                                        (*subpArray)[j]->getPartitionUID(), TRUE,
                                        (alterStoredDesc->getType() == StmtDDLAlterTableStoredDesc::GENERATE_STATS_FORCE ? TRUE : FALSE));

              if (cliRC < 0)
              {
                // switch compiler back
                switchBackCompiler();
                processReturn ();
                return;
              }
            }
          }
          else
          {
            cliRC = updateObjectStats(&cliInterface, 
                                      catalogNamePart, schemaNamePart,
                                      partitionArray[i]->getPartitionEntityName(),
                                      COM_BASE_TABLE_OBJECT_LIT,
                                      (alterStoredDesc->getType() == StmtDDLAlterTableStoredDesc::GENERATE_STATS_INTERNAL ? -2:-1),
                                      partitionArray[i]->getPartitionUID(), TRUE,
                                      (alterStoredDesc->getType() == StmtDDLAlterTableStoredDesc::GENERATE_STATS_FORCE ? TRUE : FALSE));


            if (cliRC < 0)
            {
              // switch compiler back
              switchBackCompiler();
              processReturn ();
              return;
            }
          }
        }
      }

      // switch compiler back
      switchBackCompiler();

      if (cliRC < 0)
        {
          processReturn ();
          
          return;
        }

      //M-20032
      NAString dLockName(GENERATE_STORED_STATS_DLOCK_KEY);
      NAString objectID = Int64ToNAString(objUID);
      dLockName.append(objectID);
      int dLockTimeout = 60000; //60 seconds
      dlockForStoredDesc_ = new IndependentLockController(dLockName.data(), dLockTimeout);
      if (!dlockForStoredDesc_->lockHeld())
        {     
          *CmpCommon::diags() << DgSqlCode(-EXE_OEC_UNABLE_TO_GET_DIST_LOCK)
                              << DgString0(dLockName.data());
          return;
        }   
    }
  else if (alterStoredDesc->getType() == StmtDDLAlterTableStoredDesc::DELETE_DESC)
    {
      cliRC = deleteFromTextTable
        (&cliInterface, objUID, COM_STORED_DESC_TEXT, 0);
      if (cliRC < 0)
       {
         processReturn ();
         return;
       }    

     Int64 flags = MD_OBJECTS_DISABLE_STORED_DESC |  MD_OBJECTS_STORED_DESC;
     cliRC = updateObjectFlags(&cliInterface, objUID, flags, TRUE);
     if (cliRC < 0)
       {
         processReturn ();
         return;
       }    

     // if this table has indexes, delete stored desc for each index
      if (naTable->hasSecondaryIndexes()) // user indexes
        {
          const NAFileSetList &naFsList = naTable->getIndexList();
          
          for (Lng32 i = 0; i < naFsList.entries(); i++)
            {
              const NAFileSet * naFS = naFsList[i];
              
              // skip clustering index
              if (naFS->getKeytag() == 0)
                continue;
              
              Int64 indexUID = naFS->getIndexUID();
              cliRC = deleteFromTextTable
                (&cliInterface, indexUID, COM_STORED_DESC_TEXT, 0);
              if (cliRC < 0)
                {
                  processReturn ();
                  return;
                }    
              
              Int64 flags = MD_OBJECTS_DISABLE_STORED_DESC | MD_OBJECTS_STORED_DESC;
              cliRC = updateObjectFlags(&cliInterface, indexUID, flags, TRUE);
              if (cliRC < 0)
                {
                  processReturn ();
                  return;
                }    
            } // for
        } // secondary indexes

      if (naTable->isPartitionV2Table())
      {
        const NAPartitionArray & partitionArray = naTable->getPartitionArray();
        for (Lng32 i = 0; i < partitionArray.entries(); i++)
        {
          if (partitionArray[i]->hasSubPartition())
          {
            const NAPartitionArray *subpArray = partitionArray[i]->getSubPartitions();
            for (Lng32 j = 0; j < subpArray->entries(); j++)
            {
             cliRC = deleteFromTextTable(&cliInterface, (*subpArray)[j]->getPartitionUID(),
                                         COM_STORED_DESC_TEXT, 0);
             if (cliRC < 0)
             {
               processReturn ();
               return;
             }

             Int64 flags = MD_OBJECTS_DISABLE_STORED_DESC | MD_OBJECTS_STORED_DESC;
             cliRC = updateObjectFlags(&cliInterface, (*subpArray)[j]->getPartitionUID(), flags, TRUE);
             if (cliRC < 0)
             {
               processReturn ();
               return;
             }
            }
          }
          else
          {
            cliRC = deleteFromTextTable(&cliInterface, partitionArray[i]->getPartitionUID(),
                                        COM_STORED_DESC_TEXT, 0);
            if (cliRC < 0)
            {
              processReturn ();
              return;
            }
            Int64 flags = MD_OBJECTS_DISABLE_STORED_DESC | MD_OBJECTS_STORED_DESC;
            cliRC = updateObjectFlags(&cliInterface, partitionArray[i]->getPartitionUID(), flags, TRUE);
            if (cliRC < 0)
            {
              processReturn ();
              return;
            }
          }
        }
      }

       if ((naTable->hasLobColumn()) && (naTable->lobV2()))
       {
         Int64 lobUID = -1;
         char lobChunksTableNameBuf[1024];
         str_sprintf(lobChunksTableNameBuf, "%s%020ld",
                         LOB_CHUNKS_V2_PREFIX, objUID);

         lobUID = getObjectUID(
                &cliInterface,
                catalogNamePart.data(), schemaNamePart.data(),
                lobChunksTableNameBuf, COM_BASE_TABLE_OBJECT_LIT);
         if (lobUID >= 0)
         {
           cliRC = deleteFromTextTable
             (&cliInterface, lobUID, COM_STORED_DESC_TEXT, 0);
           if (cliRC < 0)
           {
             processReturn ();
             return;
           }
 
           Int64 flags = MD_OBJECTS_DISABLE_STORED_DESC;
           cliRC = updateObjectFlags(&cliInterface, lobUID, flags, TRUE);
           if (cliRC < 0)
           {
             processReturn ();
             return;
           }
         }
       }

    }
  else if (alterStoredDesc->getType() == StmtDDLAlterTableStoredDesc::DELETE_STATS)
    {
      cliRC = 
        updateObjectRedefTime(&cliInterface, 
                              catalogNamePart, schemaNamePart, objectNamePart,
                              COM_BASE_TABLE_OBJECT_LIT,
                              -1, objUID, TRUE, FALSE);
      if (cliRC < 0)
        {
          processReturn ();
          
          return;
        }
    }
  else if (alterStoredDesc->getType() == StmtDDLAlterTableStoredDesc::ENABLE ||
           alterStoredDesc->getType() == StmtDDLAlterTableStoredDesc::DISABLE)
    {
      Int64 flags = MD_OBJECTS_DISABLE_STORED_DESC;
      NABoolean reset = StmtDDLAlterTableStoredDesc::ENABLE ? TRUE : FALSE;
      cliRC = updateObjectFlags(&cliInterface, objUID, flags, reset);
      if (cliRC < 0)
      {
        processReturn ();
        return;
      }
 
      if (naTable->isPartitionV2Table())
      {
        const NAPartitionArray & partitionArray = naTable->getPartitionArray();
        for (Lng32 i = 0; i < partitionArray.entries(); i++)
        {
          if (partitionArray[i]->hasSubPartition())
          {
            const NAPartitionArray *subpArray = partitionArray[i]->getSubPartitions();
            for (Lng32 j = 0; j < subpArray->entries(); j++)
            {
             updateObjectFlags(&cliInterface, (*subpArray)[j]->getPartitionUID(), flags, reset);
             if (cliRC < 0)
             {
               processReturn ();
               return;
             }
            }
          }
          else
          {
            cliRC = updateObjectFlags(&cliInterface, partitionArray[i]->getPartitionUID(), flags, reset);
            if (cliRC < 0)
            {
              processReturn ();
              return;
            }
          }
        }
      }
    }
  else if (alterStoredDesc->getType() == StmtDDLAlterTableStoredDesc::CHECK_DESC)
    {
      // switch compiler and compile metadata queries in META context.
      if (switchCompiler(CmpContextInfo::CMPCONTEXT_TYPE_META))
        return;

      checkAndGetStoredObjectDesc(&cliInterface, objUID, NULL, FALSE, NULL);

      // switch compiler back
      switchBackCompiler();
      
      processReturn();
 
      return;
    }
  else if (alterStoredDesc->getType() == StmtDDLAlterTableStoredDesc::CHECK_STATS)
    {
      // switch compiler and compile metadata queries in META context.
      if (switchCompiler(CmpContextInfo::CMPCONTEXT_TYPE_META))
        return;

      checkAndGetStoredObjectDesc(&cliInterface, objUID, NULL, TRUE, NULL);

      // switch compiler back
      switchBackCompiler();

      processReturn();
 
      return;
    }

   
  //StmtDDLAlterTableStoredDesc::GENERATE_STATS_INTERNAL will not remove natable
  if ((alterStoredDesc->getType() == StmtDDLAlterTableStoredDesc::GENERATE_DESC) ||
      (alterStoredDesc->getType() == StmtDDLAlterTableStoredDesc::GENERATE_STATS) ||
      (alterStoredDesc->getType() == StmtDDLAlterTableStoredDesc::GENERATE_STATS_FORCE) ||
      (alterStoredDesc->getType() == StmtDDLAlterTableStoredDesc::DELETE_DESC) ||
      (alterStoredDesc->getType() == StmtDDLAlterTableStoredDesc::DELETE_STATS) ||
      (alterStoredDesc->getType() == StmtDDLAlterTableStoredDesc::ENABLE) ||
      (alterStoredDesc->getType() == StmtDDLAlterTableStoredDesc::DISABLE))
    {
      ActiveSchemaDB()->getNATableDB()->removeNATable
        (cn,
         ComQiScope::REMOVE_FROM_ALL_USERS, COM_BASE_TABLE_OBJECT,
         alterStoredDesc->ddlXns(), FALSE);

      if (naTable->isPartitionV2Table())
      {
        const NAPartitionArray & partitionArray = naTable->getPartitionArray();
        for (Lng32 i = 0; i < partitionArray.entries(); i++)
        {
          if (partitionArray[i]->hasSubPartition())
          {
            const NAPartitionArray *subpArray = partitionArray[i]->getSubPartitions();
            for (Lng32 j = 0; j < subpArray->entries(); j++)
            {
              CorrName pcn((*subpArray)[j]->getPartitionEntityName(),
                           STMTHEAP,
                           schemaNamePart,
                           catalogNamePart);

              ActiveSchemaDB()->getNATableDB()->removeNATable
              (pcn,
               ComQiScope::REMOVE_FROM_ALL_USERS, COM_BASE_TABLE_OBJECT,
               alterStoredDesc->ddlXns(), FALSE);
            }
          }
          else
          {
            CorrName pcn(partitionArray[i]->getPartitionEntityName(),
                         STMTHEAP,
                         schemaNamePart,
                         catalogNamePart);

            ActiveSchemaDB()->getNATableDB()->removeNATable
            (pcn,
             ComQiScope::REMOVE_FROM_ALL_USERS, COM_BASE_TABLE_OBJECT,
             alterStoredDesc->ddlXns(), FALSE);
          }
        }
      }

      if (naTable->incrBackupEnabled())
      {
        cliRC = updateSeabaseXDCDDLTable(&cliInterface,
                                         catalogNamePart.data(), schemaNamePart.data(),
                                         objectNamePart.data(), COM_BASE_TABLE_OBJECT_LIT);
        if (cliRC < 0)
        {
          processReturn();
          return;
        }
      }
    }

  return;
}

// ----------------------------------------------------------------------------
// alterSeabaseTableHBaseOptions
//
//    ALTER TABLE <table> ALTER HBASE_OPTIONS (<options>)
//
// This statement is not run in a transaction by default.
// It can be run in a user transaction if user specifies "begin"
// ----------------------------------------------------------------------------
void CmpSeabaseDDL::alterSeabaseTableHBaseOptions(
                                       StmtDDLAlterTableHBaseOptions * hbaseOptionsNode,
                                       NAString &currCatName, NAString &currSchName)
{
  Lng32 retcode = 0;
  Lng32 cliRC = 0;

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, 
                               CmpCommon::context()->sqlSession()->getParentQid());

  NAString tabName = hbaseOptionsNode->getTableName();
  NAString catalogNamePart, schemaNamePart, objectNamePart;
  NAString extTableName, extNameForHbase;
  NATable * naTable = NULL;
  CorrName cn;

  retcode = lockObjectDDL(hbaseOptionsNode);
  if (retcode < 0) {
    processReturn();
    return;
  }

  retcode = 
    setupAndErrorChecks(tabName, 
                        hbaseOptionsNode->getOrigTableNameAsQualifiedName(),
                        currCatName, currSchName,
                        catalogNamePart, schemaNamePart, objectNamePart,
                        extTableName, extNameForHbase, cn,
                        &naTable, 
                        FALSE, FALSE,
                        &cliInterface,
                        COM_BASE_TABLE_OBJECT,
                        SQLOperation::ALTER_TABLE);
  if (retcode < 0)
    {
      processReturn();
      return;
    }

  CmpCommon::diags()->clear();

  // Make sure user has the privilege to perform the ALTER

  if (!isDDLOperationAuthorized(SQLOperation::ALTER_TABLE,
                                naTable->getOwner(),naTable->getSchemaOwner(),
                                (int64_t)naTable->objectUid().get_value(), 
                                COM_BASE_TABLE_OBJECT, naTable->getPrivInfo()))
    {
      *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);

      processReturn ();
      return;
    }

  CmpCommon::diags()->clear();

  // Get the object UID so we can update the metadata

  Int64 objUID = getObjectUID(&cliInterface,
                              catalogNamePart.data(), schemaNamePart.data(), 
                              objectNamePart.data(),
                              COM_BASE_TABLE_OBJECT_LIT);
  if (objUID < 0)
    {
      processReturn();
      return;
    }

  // Table exists, update the epoch value to lock out other access

  retcode = ddlSetObjectEpoch(naTable, STMTHEAP);
  if (retcode < 0)
    {
      processReturn();
      return;
    }

  // update HBase options in the metadata

  ElemDDLHbaseOptions * edhbo = hbaseOptionsNode->getHBaseOptions();
  short result = updateHbaseOptionsInMetadata(&cliInterface,objUID,edhbo);
  
  if (result < 0)
    {
      if (!xnInProgress(&cliInterface)) {
        ddlResetObjectEpochs(TRUE/*doAbort*/, TRUE /*clearList*/);

        // Release all DDL object locks
        LOGTRACE(CAT_SQL_LOCK, "Release all DDL object locks");
        CmpCommon::context()->releaseAllDDLObjectLocks();
      }
        
      processReturn();
      return;
    }

  ExpHbaseInterface * ehi = allocEHI(naTable->storageType());
  if (ehi == NULL)
    {
      if (!xnInProgress(&cliInterface)) {
        ddlResetObjectEpochs(TRUE/*doAbort*/, TRUE /*clearList*/);

        // Release all DDL object locks
        LOGTRACE(CAT_SQL_LOCK, "Release all DDL object locks");
        CmpCommon::context()->releaseAllDDLObjectLocks();
      }
        
      processReturn();
      return;
    }

  // tell HBase to change the options
  HbaseStr hbaseTable;
  hbaseTable.val = (char*)extNameForHbase.data();
  hbaseTable.len = extNameForHbase.length();
  result = alterHbaseTable(ehi,
                           &hbaseTable,
                           naTable->allColFams(),
                           &(edhbo->getHbaseOptions()),
                           hbaseOptionsNode->ddlXns());
  if (result < 0)
    {
      if (!xnInProgress(&cliInterface)) {
        ddlResetObjectEpochs(TRUE/*doAbort*/, TRUE /*clearList*/);

        // Release all DDL object locks
        LOGTRACE(CAT_SQL_LOCK, "Release all DDL object locks");
        CmpCommon::context()->releaseAllDDLObjectLocks();
      }
        
      processReturn();
      deallocEHI(ehi);

      return;
    }   

  if (naTable->incrBackupEnabled())
  {
    result = updateSeabaseXDCDDLTable(&cliInterface,
                                     catalogNamePart.data(), schemaNamePart.data(),
                                     objectNamePart.data(), COM_BASE_TABLE_OBJECT_LIT);
    if (result < 0)
    {
    if (!xnInProgress(&cliInterface)) {
        ddlResetObjectEpochs(TRUE/*doAbort*/, TRUE /*clearList*/);

        // Release all DDL object locks
        LOGTRACE(CAT_SQL_LOCK, "Release all DDL object locks");
        CmpCommon::context()->releaseAllDDLObjectLocks();
      }

      deallocEHI(ehi);
      processReturn();
      return;
    }
  }

  cliRC = updateObjectRedefTime(&cliInterface,
                                catalogNamePart, schemaNamePart, objectNamePart,
                                COM_BASE_TABLE_OBJECT_LIT, -1, objUID,
                                naTable->isStoredDesc(), 
                                naTable->storedStatsAvail());
  if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

      if (!xnInProgress(&cliInterface)) {
        ddlResetObjectEpochs(TRUE/*doAbort*/, TRUE /*clearList*/);

        // Release all DDL object locks
        LOGTRACE(CAT_SQL_LOCK, "Release all DDL object locks");
        CmpCommon::context()->releaseAllDDLObjectLocks();
      }
        
      deallocEHI(ehi);
      processReturn();
      return;
    }

  if (naTable->isStoredDesc() && (! Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)))
    {
      insertIntoSharedCacheList(catalogNamePart, schemaNamePart, objectNamePart,
                                COM_BASE_TABLE_OBJECT, SharedCacheDDLInfo::DDL_UPDATE);
      if (!xnInProgress(&cliInterface))
         finalizeSharedCache();
    }

  // invalidate cached NATable info on this table for all users

  ActiveSchemaDB()->getNATableDB()->removeNATable
    (cn,
     ComQiScope::REMOVE_FROM_ALL_USERS, COM_BASE_TABLE_OBJECT,
     hbaseOptionsNode->ddlXns(), FALSE);

  // Release DDL lock
  if (!xnInProgress(&cliInterface)) {
    ddlResetObjectEpochs(FALSE /*doAbort*/, TRUE /*clearList*/);

    // Release all DDL object locks
    LOGTRACE(CAT_SQL_LOCK, "Release all DDL object locks");
    CmpCommon::context()->releaseAllDDLObjectLocks();
  }
    
  deallocEHI(ehi);

  return;
}

// ----------------------------------------------------------------------------
// alterSeabaseTableAttribute
//
//    ALTER TABLE <table> ATTRIBUTE <attributes>
//
// This statement runs in a transaction
// ----------------------------------------------------------------------------
void CmpSeabaseDDL::alterSeabaseTableAttribute(
     StmtDDLAlterTableAttribute * alterTableNode,
     NAString &currCatName, NAString &currSchName)
{
  Lng32 retcode = 0;
  Lng32 cliRC = 0;

  NABoolean isAlteringReadOnlyAttr = FALSE;

  ComObjectName tableName(alterTableNode->getTableName());
  ComAnsiNamePart currCatAnsiName(currCatName);
  ComAnsiNamePart currSchAnsiName(currSchName);
  tableName.applyDefaults(currCatAnsiName, currSchAnsiName);
  const NAString catalogNamePart = tableName.getCatalogNamePartAsAnsiString();
  const NAString schemaNamePart = tableName.getSchemaNamePartAsAnsiString(TRUE);
  const NAString objectNamePart = tableName.getObjectNamePartAsAnsiString(TRUE);
  const NAString extTableName = tableName.getExternalName(TRUE);

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, 
  CmpCommon::context()->sqlSession()->getParentQid());
  
  if ((isSeabaseReservedSchema(tableName)) &&
      (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)))
    {
      *CmpCommon::diags() << DgSqlCode(-CAT_CREATE_TABLE_NOT_ALLOWED_IN_SMD)
                          << DgTableName(extTableName);

      processReturn();

      return;
    }
  
  if (CmpCommon::context()->sqlSession()->volatileSchemaInUse())
    {
      QualifiedName *qn =
        CmpCommon::context()->sqlSession()->
        updateVolatileQualifiedName
        (alterTableNode->getTableNameAsQualifiedName().getObjectName());
      
      if (qn == NULL)
        {
          *CmpCommon::diags()
            << DgSqlCode(-1427);
          
          processReturn();
          
          return;
        }
      
      ComObjectName volTabName (qn->getQualifiedNameAsAnsiString());
      volTabName.applyDefaults(currCatAnsiName, currSchAnsiName);
      
      NAString vtCatNamePart = volTabName.getCatalogNamePartAsAnsiString();
      NAString vtSchNamePart = volTabName.getSchemaNamePartAsAnsiString(TRUE);
      NAString vtObjNamePart = volTabName.getObjectNamePartAsAnsiString(TRUE);
      
      retcode = existsInSeabaseMDTable(&cliInterface, 
                                       vtCatNamePart, vtSchNamePart, vtObjNamePart,
                                       COM_BASE_TABLE_OBJECT);
      
      if (retcode < 0)
        {
          processReturn();
          
          return;
        }
      
      if (retcode == 1)
        {
          // table found in volatile schema. cannot rename it.
          *CmpCommon::diags()
            << DgSqlCode(-1427)
            << DgString0("Reason: Operation not allowed on volatile tables.");
          
          processReturn();
          return;
        }
    }

  retcode = lockObjectDDL(alterTableNode);
  if (retcode < 0) {
    processReturn();
    return;
  }

  retcode = existsInSeabaseMDTable(&cliInterface, 
                                   catalogNamePart, schemaNamePart, objectNamePart,
                                   COM_BASE_TABLE_OBJECT,
                                   (Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL) 
                                    ? FALSE : TRUE),
                                   TRUE, TRUE);
  if (retcode < 0)
    {
      processReturn();

      return;
    }

  BindWA bindWA(ActiveSchemaDB(), CmpCommon::context(), TRUE/*inDDL*/);
  
  CorrName cn(objectNamePart,
              STMTHEAP,
              schemaNamePart,
              catalogNamePart);
  
  NATable *naTable = bindWA.getNATable(cn); 
  if (naTable == NULL || bindWA.errStatus())
    {
      CmpCommon::diags()->clear();
      
      *CmpCommon::diags() << DgSqlCode(-CAT_OBJECT_DOES_NOT_EXIST_IN_TRAFODION)
                          << DgString0(extTableName);
  
      processReturn();
      
      return;
    }
 
  // Table exists, update the epoch value to lock out other user access
  retcode = ddlSetObjectEpoch(naTable, STMTHEAP);
  if (retcode < 0)
    {
      processReturn();
      return;
    }

  // Make sure user has the privilege to perform the rename
  if (!isDDLOperationAuthorized(SQLOperation::ALTER_TABLE,
                                naTable->getOwner(),naTable->getSchemaOwner(),
                                (int64_t)naTable->objectUid().get_value(), 
                                COM_BASE_TABLE_OBJECT, naTable->getPrivInfo()))
  {
     *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);

     processReturn ();

     return;
  }

  CmpCommon::diags()->clear();
  
  // cannot alter attributes of a view
  if (naTable->getViewText())
    {
      *CmpCommon::diags()
        << DgSqlCode(-1427)
        << DgString0("Reason: Operation not allowed on a view.");
      
      processReturn();
      
      return;
    }

  Int64 objUID = getObjectUID(&cliInterface,
                              catalogNamePart.data(), schemaNamePart.data(), 
                              objectNamePart.data(),
                              COM_BASE_TABLE_OBJECT_LIT);
  if (objUID < 0)
    {

      processReturn();

      return;
    }

  ParDDLFileAttrsAlterTable &fileAttrs = alterTableNode->getFileAttributes();
  if ((NOT fileAttrs.isXnReplSpecified()) &&
      (NOT fileAttrs.isIncrBackupSpecified()) &&
      (NOT fileAttrs.isReadOnlySpecified()))
    {
      *CmpCommon::diags()
        << DgSqlCode(-1425)
        << DgString0(extTableName)
        << DgString1("Reason: Only replication or incremental backup attribute can be altered.");
      
      processReturn();
      
      return;
    }

  if (fileAttrs.isReadOnlySpecified())
    isAlteringReadOnlyAttr = TRUE;
    
  if (isAlteringReadOnlyAttr && !naTable->isSeabaseTable()) {
    *CmpCommon::diags() << DgSqlCode(-3242)
                        << DgString0("Can not alter attribute on not a trafodion seabase table");
    processReturn();
    return;
  }

  Int64 objDataUID = naTable->objDataUID().castToInt64();

  //////////////////Should move to setupAndErrChecks/////////////
  // /* process hiatus */
  // incr table alter to reg table -- call hiatus.
  // incr table alter to incr table -- no need of hiatus.
  // reg table alter to incr table -- call hiatus with create snapshot flag.
  // reg table alter to reg table -- no need of hiatus.
  if (naTable && fileAttrs.isIncrBackupSpecified())
    {
      hiatusObjectName_ = genHBaseObjName
        (catalogNamePart.data(), schemaNamePart.data(), objectNamePart.data(),
         naTable->getNamespace(), objDataUID);

      //incr table alter to reg table -- call hiatus
      if(naTable->incrBackupEnabled() && (!fileAttrs.incrBackupEnabled()))
        {
          if (setHiatus(hiatusObjectName_, FALSE))
            {
              // setHiatus has set the diags area
              processReturn();
              
              return;
            }
        }
      
      //reg table alter to incr table -- call hiatus with create snapshot flag.
      if((!naTable->incrBackupEnabled()) && fileAttrs.incrBackupEnabled())
        {
          if (setHiatus(hiatusObjectName_, FALSE, TRUE))
            {
              // setHiatus has set the diags area
              processReturn();
              
              return;
            }
        }
    }

  // update flags with new repl options in TABLES metadata.
  char queryBuf[1000];

  if (fileAttrs.isXnReplSpecified())
    {
      // get current table flags
      str_sprintf(queryBuf, "select flags from %s.\"%s\".%s where table_uid = %ld",
                  getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_TABLES,
                  objUID);
      Int64 flags = 0;
      Lng32 flagsLen = sizeof(Int64);
      Int64 rowsAffected = 0;
      cliRC = cliInterface.executeImmediateCEFC
        (queryBuf, NULL, 0, (char*)&flags, &flagsLen, &rowsAffected);
      if (cliRC < 0)
        {
          cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
          processReturn();
          return;
        }
      
      if (cliRC == 100) // did not find the row
        {
          *CmpCommon::diags() << DgSqlCode(-1389) << DgString0(extTableName);
          processReturn();
          return;
        }
      
      // update flags with new repl options in TABLES metadata.
      Int64 newFlags = flags;
      
      // clear replication bits
      CmpSeabaseDDL::resetMDflags(newFlags, MD_TABLES_REPL_SYNC_FLG);
      CmpSeabaseDDL::resetMDflags(newFlags, MD_TABLES_REPL_ASYNC_FLG);
      
      Int64 replFlags = 0;
      if (fileAttrs.xnRepl() == COM_REPL_SYNC)
        CmpSeabaseDDL::setMDflags(newFlags, MD_TABLES_REPL_SYNC_FLG);
      else if (fileAttrs.xnRepl() == COM_REPL_ASYNC)
        {
          *CmpCommon::diags()
            << DgSqlCode(-1425)
            << DgTableName(extTableName)
            << DgString0("Reason: Asynchronous replication not yet supported.");
          
          processReturn();
          
          return;
          
          CmpSeabaseDDL::setMDflags(newFlags, MD_TABLES_REPL_ASYNC_FLG);
        }
      
      str_sprintf(queryBuf, "update %s.\"%s\".%s set flags = %ld where table_uid = %ld ",
                  getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_TABLES,
                  newFlags, objUID);
      
      cliRC = cliInterface.executeImmediate(queryBuf);
      if (cliRC < 0)
        {
          cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
          processReturn();
          return;
        }

      // if this table has an identity column, update the repl attrs for the
      // internal seq gen object
      Int32 idenColPos = naTable->getNAColumnArray().getIdentityColumnPosition();
      if (idenColPos >= 0)
        {
          NAColumn *nac = naTable->getNAColumnArray().getColumn(idenColPos);
          NAString seqName;
          SequenceGeneratorAttributes::genSequenceName
            (catalogNamePart, schemaNamePart, objectNamePart, 
             nac->getColName(),
             seqName);
            Int64 seqUID = getObjectUID
              (&cliInterface,
               catalogNamePart.data(), schemaNamePart.data(), seqName.data(),
               COM_SEQUENCE_GENERATOR_OBJECT_LIT);

            Int64 seqFlags = 0;
            if (fileAttrs.xnRepl() == COM_REPL_SYNC)
              {
                CmpSeabaseDDL::setMDflags(seqFlags, MD_SEQGEN_REPL_SYNC_FLG);
                seqFlags = ~seqFlags;
              }

            str_sprintf(queryBuf, "update %s.\"%s\".%s set flags = case when %ld != 0 then bitor(flags, %ld) else bitand(flags, bitnot(%ld)) end where seq_type = '%s' and seq_uid = (select object_uid from %s.\"%s\".\"%s\" where catalog_name = '%s' and schema_name = '%s' and object_name = '%s' and object_type = '%s') ",
                        getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_SEQ_GEN,
                        seqFlags, seqFlags, seqFlags,
                        COM_INTERNAL_SG_LIT,
                        getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS,
                        catalogNamePart.data(), schemaNamePart.data(), seqName.data(),
                        COM_SEQUENCE_GENERATOR_OBJECT_LIT);            
            cliRC = cliInterface.executeImmediate(queryBuf);
            if (cliRC < 0)
              {
                cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
                processReturn();
                return;
              }
        }
    }

  if (fileAttrs.isIncrBackupSpecified() ||
      isAlteringReadOnlyAttr)
    {
      // get current table flags
      str_sprintf(queryBuf, "select flags from %s.\"%s\".%s where object_uid = %ld",
                  getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS,
                  objUID);
      Int64 flags = 0;
      Lng32 flagsLen = sizeof(Int64);
      Int64 rowsAffected = 0;
      cliRC = cliInterface.executeImmediateCEFC
        (queryBuf, NULL, 0, (char*)&flags, &flagsLen, &rowsAffected);
      if (cliRC < 0)
        {
          cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
          processReturn();
          return;
        }
      
      if (cliRC == 100) // did not find the row
        {
          *CmpCommon::diags() << DgSqlCode(-1389) << DgString0(extTableName);
          processReturn();
          return;
        }
      
      // update flags with new repl options in OBJECTS metadata.
      Int64 newFlags = flags;
      
      if (fileAttrs.isIncrBackupSpecified())
      {
        // clear incremental backup bits
        CmpSeabaseDDL::resetMDflags(newFlags, MD_OBJECTS_INCR_BACKUP_ENABLED);
      
        if (fileAttrs.incrBackupEnabled())
          CmpSeabaseDDL::setMDflags(newFlags, MD_OBJECTS_INCR_BACKUP_ENABLED);
      }
     
      //  1 -> alter to read only
      // -1 -> alter to not rad only
      int alterReadonlyOP = 0;
      if (isAlteringReadOnlyAttr)
      {
        if (CmpCommon::getDefault(TRAF_ENABLE_DATA_LOAD_IN_SHARED_CACHE) == DF_OFF) {
          *CmpCommon::diags() << DgSqlCode(-3242) << DgString0("The data caching function is disabled");
          processReturn();
          return;
        }
        NAString type;
        if ((!naTable->isSQLMXAlignedTable() && (type = "HBASE FORMAT TABLE")) ||
            (naTable->isVolatileTable() && (type = "VOLATILE TABLE")) ||
            (naTable->isTrafExternalTable() && (type = "EXTERNAL TABLE")) ||
            (naTable->isHbaseMapTable() && (type = "HBASE MAP TABLE")) ||
            (naTable->hasLobColumn() && (type = "TABLE WITH LOB COLUMNS")) ||
            ((naTable->hasSaltedColumn() || naTable->hasTrafReplicaColumn()) && (type = "PARTITION TABLE")))
        {
          *CmpCommon::diags() << DgSqlCode(-3242) << DgString0("Unsupported table types: " + type);
          processReturn();
          return;
        }
        if (fileAttrs.readOnlyEnabled()) {
          CmpSeabaseDDL::setMDflags(newFlags, MD_OBJECTS_READ_ONLY_ENABLED);
          alterReadonlyOP = 1;
        }
        else {
          CmpSeabaseDDL::resetMDflags(newFlags, MD_OBJECTS_READ_ONLY_ENABLED);
          alterReadonlyOP = -1;
        }
      }

      str_sprintf(queryBuf, "update %s.\"%s\".%s set flags = %ld where object_uid = %ld ",
                  getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS,
                  newFlags, objUID);
      
      cliRC = cliInterface.executeImmediate(queryBuf);
      if (cliRC < 0)
        {
          cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
          processReturn();
          return;
        }

        if (alterReadonlyOP != 0)
          insertIntoSharedCacheList(catalogNamePart, schemaNamePart, objectNamePart,
                                    COM_BASE_TABLE_OBJECT,
                                    (alterReadonlyOP == 1 ? SharedCacheDDLInfo::DDL_UPDATE : SharedCacheDDLInfo::DDL_DELETE),
                                    SharedCacheDDLInfo::SHARED_DATA_CACHE);

      // update secondary indexes
      retcode = alterSeabaseIndexAttributes(&cliInterface, fileAttrs, naTable, alterReadonlyOP); 
      if (retcode < 0)
        {
           processReturn();
           return;
        }

    } // incr backup specified

  if (naTable->incrBackupEnabled() || fileAttrs.isIncrBackupSpecified())
  {
    retcode = updateSeabaseXDCDDLTable(&cliInterface,
                                     catalogNamePart.data(), schemaNamePart.data(),
                                     objectNamePart.data(), COM_BASE_TABLE_OBJECT_LIT);
    if (retcode < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      processReturn();
      return;
    }
  }

  if (updateObjectRedefTime(&cliInterface, 
                            catalogNamePart, schemaNamePart, objectNamePart,
                            COM_BASE_TABLE_OBJECT_LIT, -1, objUID,
                            naTable->isStoredDesc(), naTable->storedStatsAvail()))
    {
      processReturn();

      return;
    }
  
  if (naTable->isStoredDesc() && (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)))
    {
      insertIntoSharedCacheList(catalogNamePart, schemaNamePart, objectNamePart,
                                COM_BASE_TABLE_OBJECT, SharedCacheDDLInfo::DDL_UPDATE);
    }

  //update index's stored desc
  if (naTable->hasSecondaryIndexes())
  {
    const NAFileSetList &naFsList = naTable->getIndexList();
    for (Lng32 i = 0; i < naFsList.entries(); i++)
    {
      const NAFileSet * naFS = naFsList[i];
      if (naFS->getKeytag() == 0)
        continue;

      Int64 indexUID = naFS->getIndexUID();
      const QualifiedName &indexName = naFS->getFileSetName();
      retcode = updateObjectRedefTime(&cliInterface, 
                            indexName.getCatalogName().data(),
                            indexName.getSchemaName().data(),
                            indexName.getObjectName().data(),
                            COM_INDEX_OBJECT_LIT, -1, indexUID,
                            naTable->isStoredDesc(),
                            FALSE);
      if (fileAttrs.isReadOnlySpecified())
      {
        CorrName indexCn = CorrName(indexName.getObjectName().data(),
                                    STMTHEAP, indexName.getSchemaName().data(),
                                    indexName.getCatalogName().data());
        ActiveSchemaDB()->getNATableDB()->removeNATable(indexCn,
          ComQiScope::REMOVE_FROM_ALL_USERS, COM_INDEX_OBJECT,
          alterTableNode->ddlXns(), FALSE, indexUID);
      }
    }
  }

  ActiveSchemaDB()->getNATableDB()->removeNATable(cn,
    ComQiScope::REMOVE_FROM_ALL_USERS, COM_BASE_TABLE_OBJECT,
    alterTableNode->ddlXns(), FALSE);

  return;
}

short CmpSeabaseDDL::createSeabaseTableLike2(
     CorrName &cn,
     const NAString &likeTableName,
     NABoolean withPartns,
     NABoolean withoutSalt,
     NABoolean withoutDivision,
     NABoolean withoutRowFormat)
{
  Lng32 retcode = 0;

  char * buf = NULL;
  ULng32 buflen = 0;
  retcode = CmpDescribeSeabaseTable(cn, 3/*createlike*/, buf, buflen, STMTHEAP, FALSE,
                                    NULL, NULL,
                                    withPartns, withoutSalt, withoutDivision,
                                    withoutRowFormat,
                                    FALSE, // include LOB columns (if any)
                                    FALSE, // same namespace as source
                                    FALSE,
                                    FALSE,
                                    UINT_MAX,
                                    TRUE);
  if (retcode)
    return -1;

  NAString query = "create table ";
  query += likeTableName;
  query += " ";

  NABoolean done = FALSE;
  Lng32 curPos = 0;
  while (NOT done)
    {
      short len = *(short*)&buf[curPos];
      NAString frag(&buf[curPos+sizeof(short)],
                    len - ((buf[curPos+len-1]== '\n') ? 1 : 0));

      query += frag;
      curPos += ((((len+sizeof(short))-1)/8)+1)*8;

      if (curPos >= buflen)
        done = TRUE;
    }

  query += ";";

  // send any user CQDs down 
  Lng32 retCode = sendAllControls(FALSE, FALSE, TRUE);

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, 
  CmpCommon::context()->sqlSession()->getParentQid());

  Lng32 cliRC = 0;
  cliRC = cliInterface.executeImmediate((char*)query.data());
  if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

      return -1;
    }

  return 0;
}

short CmpSeabaseDDL::addIBAttrForTable(
     const NATable * naTable, 
     ExeCliInterface * cliInterface)
{
  Lng32 retcode = 0;
  Lng32 cliRC = 0;

  NAString tempObjName(naTable->getTableName().getObjectName());
  QualifiedName qn(tempObjName,
                   naTable->getTableName().getSchemaName(),
                   naTable->getTableName().getCatalogName());
  NAString tempTable = qn.getQualifiedNameAsAnsiString();

  cliRC = cliInterface->holdAndSetCQD("DONOT_WRITE_XDC_DDL", "ON");
  char buf[2000];
  str_sprintf(buf, "alter table %s attributes incremental backup" , tempTable.data());
  cliRC = cliInterface->executeImmediate(buf);
  if (cliRC < 0)
    {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      processReturn();

      return -1;
    }  
  cliInterface->restoreCQD("DONOT_WRITE_XDC_DDL");
  return retcode;
}

NABoolean CmpSeabaseDDL::isTableHasIBAttr(
    const NATable * naTable,
    ExpHbaseInterface * ehi )
{
  //check the Binlog meta to get current IB attribute
  return naTable->incrBackupEnabled();
}

short CmpSeabaseDDL::dropIBAttrForTable(
     const NATable * naTable, 
     ExeCliInterface * cliInterface)
{
  Lng32 retcode = 0;
  Lng32 cliRC = 0;
  ULng32 savedParserFlags;


  NAString tempObjName(naTable->getTableName().getObjectName());
  QualifiedName qn(tempObjName,
                   naTable->getTableName().getSchemaName(),
                   naTable->getTableName().getCatalogName());
  NAString tempTable = qn.getQualifiedNameAsAnsiString();

  char buf[2000];
  cliRC = cliInterface->holdAndSetCQD("DONOT_WRITE_XDC_DDL", "ON");

  str_sprintf(buf, "alter table %s attributes no incremental backup" , tempTable.data());
  cliRC = cliInterface->executeImmediate(buf);

  if (cliRC < 0)
    {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      processReturn();

      return -1;
    }  

  str_sprintf(buf, "alter trafodion metadata shared cache for  table %s delete " , tempTable.data());
  cliInterface->executeImmediate(buf);

  cliInterface->restoreCQD("DONOT_WRITE_XDC_DDL");

  return retcode;
}

short CmpSeabaseDDL::cloneHbaseTable(
     const NAString &srcTable, const NAString &clonedTable,
     ExpHbaseInterface * inEHI)
{
  Lng32 retcode = 0;

  HbaseStr hbaseTable;
  hbaseTable.val = (char*)srcTable.data();
  hbaseTable.len = srcTable.length();

  HbaseStr clonedHbaseTable;
  clonedHbaseTable.val = (char*)clonedTable.data();
  clonedHbaseTable.len = clonedTable.length();

  ExpHbaseInterface * ehi = (inEHI ? inEHI : allocEHI(COM_STORAGE_HBASE));

  if (ehi == NULL) {
     processReturn();
     return -1;
  }

  // copy hbaseTable as clonedHbaseTable
  if (retcode = ehi->copy(hbaseTable, clonedHbaseTable, TRUE))
    {
      *CmpCommon::diags()
        << DgSqlCode(-8448)
        << DgString0((char*)"ExpHbaseInterface::copy()")
        << DgString1(getHbaseErrStr(-retcode))
        << DgInt0(-retcode)
        << DgString2((char*)GetCliGlobals()->getJniErrorStr());
      
      if (! inEHI)
        deallocEHI(ehi); 
      
      processReturn();
      
      return -1;
    }

  if (! inEHI)
    deallocEHI(ehi); 
 
  return 0;
}

short CmpSeabaseDDL::cloneSeabaseTable(
     const NAString &srcTableNameStr,
     Int64 srcTableUID,
     const NAString &clonedTableNameStr,
     const NATable * naTable,
     ExpHbaseInterface * inEHI,
     ExeCliInterface * cliInterface,
     NABoolean withCreate)
{
  Lng32 cliRC = 0;
  Lng32 retcode = 0;

  ComObjectName srcTableName(srcTableNameStr, COM_TABLE_NAME);
  const NAString srcCatNamePart = srcTableName.getCatalogNamePartAsAnsiString();
  const NAString srcSchNamePart = srcTableName.getSchemaNamePartAsAnsiString(TRUE);
  const NAString srcObjNamePart = srcTableName.getObjectNamePartAsAnsiString(TRUE);
  CorrName srcCN(srcObjNamePart, STMTHEAP, srcSchNamePart, srcCatNamePart);

  ComObjectName clonedTableName(clonedTableNameStr, COM_TABLE_NAME);
  const NAString clonedCatNamePart = clonedTableName.getCatalogNamePartAsAnsiString();
  const NAString clonedSchNamePart = clonedTableName.getSchemaNamePartAsAnsiString(TRUE);
  const NAString clonedObjNamePart = clonedTableName.getObjectNamePartAsAnsiString(TRUE);

  char buf[2000];
  if (withCreate)
    {
      retcode = createSeabaseTableLike2(srcCN, clonedTableNameStr,
                                        FALSE, FALSE, TRUE, FALSE);
      if (retcode)
        return -1;

      Int64 clonedTableUID = 
        getObjectUID
        (cliInterface,
         clonedCatNamePart.data(), 
         clonedSchNamePart.data(), 
         clonedObjNamePart.data(),
         COM_BASE_TABLE_OBJECT_LIT);
      
      // if there are added or altered columns in the source table, then cloned
      // table metadata need to reflect that.
      // Update metadata and set the cloned column class to be the same as source.
      str_sprintf(buf, "merge into %s.\"%s\".%s using (select column_name, column_class from %s.\"%s\".%s where object_uid = %ld) x on (object_uid = %ld and column_name = x.column_name) when matched then update set column_class = x.column_class;",
                  getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_COLUMNS,
                  getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_COLUMNS,
                  srcTableUID,
                  clonedTableUID);
      cliRC = cliInterface->executeImmediate(buf);
      if (cliRC < 0)
        {
          cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
          processReturn();
          
          return -1;
        }
    }

  if (NOT withCreate)
    {
      NABoolean xnWasStartedHere = FALSE;
      if (gEnableRowLevelLock &&
          beginXnIfNotInProgress(cliInterface, xnWasStartedHere,
                                 FALSE /*clearDDLList*/, FALSE /*clearObjectLocks*/))
        {
          return -1;
        }

      Int64 objDataUID;
      Int64 clonedTableUID = 
        getObjectUID
        (cliInterface,
         clonedCatNamePart.data(), 
         clonedSchNamePart.data(), 
         clonedObjNamePart.data(),
         COM_BASE_TABLE_OBJECT_LIT,
         &objDataUID);
        
      // truncate cloned(tgt) table before upsert
      if (truncateHbaseTable(clonedCatNamePart, 
                             clonedSchNamePart, 
                             clonedObjNamePart,
                             ((NATable*)naTable)->getNamespace(),
                             (naTable->hasSaltedColumn() || naTable->hasTrafReplicaColumn()), 
                             (naTable->xnRepl() == COM_REPL_SYNC),
                             naTable->incrBackupEnabled(),
                             objDataUID,
                             inEHI))
        {
          if (gEnableRowLevelLock)
            {
              endXnIfStartedHere(cliInterface, xnWasStartedHere, -1,
                                 FALSE /*modifyEpoch*/, FALSE /*clearObjectLocks*/);
            }
          return -1;
        }
      if (gEnableRowLevelLock &&
          endXnIfStartedHere(cliInterface, xnWasStartedHere, 0,
                             FALSE /*modifyEpoch*/, FALSE /*clearObjectLocks*/))
        {
          return -1;
        }
      

    }

  NAString quotedSrcCatName;
  ToQuotedString(quotedSrcCatName, 
                 NAString(srcCN.getQualifiedNameObj().getCatalogName()), FALSE);
  NAString quotedSrcSchName;
  ToQuotedString(quotedSrcSchName, 
                 NAString(srcCN.getQualifiedNameObj().getSchemaName()), FALSE);
  NAString quotedSrcObjName;
  ToQuotedString(quotedSrcObjName, 
                 NAString(srcCN.getQualifiedNameObj().getObjectName()), FALSE);

  if (naTable->hasSecondaryIndexes()) // user indexes
    {
      cliRC = cliInterface->holdAndSetCQD("hide_indexes", "ALL");
      if (cliRC < 0)
        {
          cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
          return -1;
        }
    }

  cliRC = cliInterface->holdAndSetCQD("attempt_esp_parallelism", "ON");
  if (cliRC < 0)
    {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      if (naTable->hasSecondaryIndexes()) // user indexes
        cliInterface->restoreCQD("hide_indexes");

      return -1;
    }

  cliRC = cliInterface->holdAndSetCQD("OVERRIDE_SYSKEY", "ON");
  if (cliRC < 0)
    {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      if (naTable->hasSecondaryIndexes()) // user indexes
        cliInterface->restoreCQD("hide_indexes");
      cliInterface->restoreCQD("attempt_esp_parallelism");

      return -1;
    }

  str_sprintf(buf, "upsert using load into %s select * from %s.\"%s\".\"%s\"",
              clonedTableNameStr.data(), 
              quotedSrcCatName.data(),
              quotedSrcSchName.data(), 
              quotedSrcObjName.data());

  cliRC = cliInterface->executeImmediate(buf);
  if (cliRC < 0)
    {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      if (naTable->hasSecondaryIndexes()) // user indexes
        cliInterface->restoreCQD("hide_indexes");
      cliInterface->restoreCQD("attempt_esp_parallelism");
      cliInterface->restoreCQD("OVERRIDE_SYSKEY");

      processReturn();

      return -1;
    }

  if (naTable->hasSecondaryIndexes()) // user indexes
    cliInterface->restoreCQD("hide_indexes");
  cliInterface->restoreCQD("attempt_esp_parallelism");
  cliInterface->restoreCQD("OVERRIDE_SYSKEY");

  return 0;
}

// ----------------------------------------------------------------------------
// alterSeabaseTableHDFSCache
//
// Note sure about syntax or transaction attributes
// TBD - add shared cache updates && DDL locks
// ----------------------------------------------------------------------------
void CmpSeabaseDDL::alterSeabaseTableHDFSCache(StmtDDLAlterTableHDFSCache * alterTableHdfsCache,
                                               NAString &currCatName, NAString &currSchName)
{
    Lng32 cliRC = 0;
    Lng32 retcode = 0;

    const NAString &tabName = alterTableHdfsCache->getTableName();

    ComObjectName tableName(tabName, COM_TABLE_NAME);
    ComAnsiNamePart currCatAnsiName(currCatName);
    ComAnsiNamePart currSchAnsiName(currSchName);
    tableName.applyDefaults(currCatAnsiName, currSchAnsiName);

    const NAString catalogNamePart = tableName.getCatalogNamePartAsAnsiString();
    const NAString schemaNamePart = tableName.getSchemaNamePartAsAnsiString(TRUE);
    const NAString objectNamePart = tableName.getObjectNamePartAsAnsiString(TRUE);
    const NAString extTableName = tableName.getExternalName(TRUE);

    ExeCliInterface cliInterface(STMTHEAP, 0, NULL, 
                               CmpCommon::context()->sqlSession()->getParentQid());

    if ((isSeabaseReservedSchema(tableName)) &&
      (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)))
    {
       *CmpCommon::diags() << DgSqlCode(-CAT_CANNOT_ALTER_DEFINITION_METADATA_SCHEMA);
       processReturn();
       return;
    }

    retcode = lockObjectDDL(alterTableHdfsCache);
    if (retcode < 0) {
      processReturn();
      return;
    }

    retcode = existsInSeabaseMDTable(&cliInterface, 
                                   catalogNamePart, schemaNamePart, objectNamePart,
                                   COM_BASE_TABLE_OBJECT,
                                   (Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL) 
                                    ? FALSE : TRUE),
                                   TRUE, TRUE);
    if (retcode < 0)
    {
      processReturn();
      return;
    }

    ActiveSchemaDB()->getNATableDB()->useCache();

    BindWA bindWA(ActiveSchemaDB(), CmpCommon::context(), TRUE/*inDDL*/);
    CorrName cn(tableName.getObjectNamePart().getInternalName(),
                STMTHEAP,
                tableName.getSchemaNamePart().getInternalName(),
                tableName.getCatalogNamePart().getInternalName());

    NATable *naTable = bindWA.getNATable(cn); 
    if (naTable == NULL || bindWA.errStatus())
    {
        CmpCommon::diags()->clear();
        *CmpCommon::diags()
          << DgSqlCode(-4082)
          << DgTableName(cn.getExposedNameAsAnsiString());

        processReturn();
        return;
    }

  // Table exists, update the epoch value to lock out other user access
  retcode = ddlSetObjectEpoch(naTable, STMTHEAP);
  if (retcode < 0)
    {
      processReturn();
      return;
    }

  Int64 objDataUID = naTable->objDataUID().castToInt64();
    const NAString extNameForHbase = genHBaseObjName
      (catalogNamePart, schemaNamePart, objectNamePart, 
       naTable->getNamespace(), objDataUID);
    
    ExpHbaseInterface * ehi = allocEHI(naTable->storageType());
    if (ehi == NULL)
      {
        processReturn();
        return;
      }

    // Make sure user has the privilege to perform the alter table hdfs cache
    if (!isDDLOperationAuthorized(SQLOperation::ALTER_TABLE,
                                  naTable->getOwner(),naTable->getSchemaOwner(),
                                  (int64_t)naTable->objectUid().get_value(), 
                                  COM_BASE_TABLE_OBJECT, naTable->getPrivInfo()))
    {
       *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);
       processReturn ();
       return;
    }

    //does pool exist ?
    Lng32 retCode = 0;
    TextVec tableList;
    tableList.push_back(extNameForHbase.data());
    if(alterTableHdfsCache->isAddToCache())
    {
       retCode = ehi->addTablesToHDFSCache(tableList ,alterTableHdfsCache->poolName());
    }
    else
    {
       retCode = ehi->removeTablesFromHDFSCache(tableList ,alterTableHdfsCache->poolName());
    }

    if (retCode == HBC_ERROR_POOL_NOT_EXIST_EXCEPTION)
    {
        *CmpCommon::diags() << DgSqlCode(-4081)
                                               << DgString0(alterTableHdfsCache->poolName());
        processReturn();
        return;
    }

    label_return:
       processReturn();

    return;
    //////////////////////////////////////////////////////////////////
}

short CmpSeabaseDDL::cloneAndTruncateTable(
     const NATable * naTable, // IN: source table
     NAString &tempTable, // OUT: temp table
     ExpHbaseInterface * ehi,
     ExeCliInterface * cliInterface)
{
  Lng32 cliRC = 0;
  Lng32 cliRC2 = 0;
  NABoolean identityGenAlways = FALSE;
  char buf[4000];
  short retcode;

  const NAString &catalogNamePart = naTable->getTableName().getCatalogName();
  const NAString &schemaNamePart = naTable->getTableName().getSchemaName();
  const NAString &objectNamePart = naTable->getTableName().getObjectName();

  ComUID comUID;
  comUID.make_UID();
  Int64 objUID = comUID.get_value();

  char objUIDbuf[100];

  NAString tempObjName(naTable->getTableName().getObjectName());
  tempObjName += "_";
  tempObjName += str_ltoa(objUID, objUIDbuf);
  QualifiedName qn(tempObjName,
                   naTable->getTableName().getSchemaName(),
                   naTable->getTableName().getCatalogName());
  tempTable = qn.getQualifiedNameAsAnsiString();

  // identity 'generated always' columns do not permit inserting user specified
  // values. Override it since we want to move original values to tgt.
  const NAColumnArray &naColArr = naTable->getNAColumnArray();
  for (Int32 c = 0; c < naColArr.entries(); c++)
    {
      const NAColumn * nac = naColArr[c];
      if (nac->isIdentityColumnAlways())
        {
          identityGenAlways = TRUE;
          break;
        }
    } // for

  NABoolean xnWasStartedHere = FALSE;

  if (identityGenAlways)
    {
      cliRC = cliInterface->holdAndSetCQD("override_generated_identity_values", "ON");
      if (cliRC < 0)
        {
          cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
          goto label_restore;
        }
    }

  // clone source naTable to target tempTable
  if (cloneSeabaseTable(naTable->getTableName().getQualifiedNameAsAnsiString(),
                        naTable->objectUid().castToInt64(),
                        tempTable, 
                        naTable,
                        ehi, cliInterface, TRUE))
    {
      cliRC = -1;
      goto label_drop;
    }

  if (gEnableRowLevelLock &&
      beginXnIfNotInProgress(cliInterface, xnWasStartedHere,
                             FALSE /*clearDDLList*/, FALSE /*clearObjectLocks*/))
  {
    cliRC = -1;
    goto label_drop;
  }

  // truncate source naTable
  retcode = truncateHbaseTable(catalogNamePart, schemaNamePart, objectNamePart,
                         ((NATable*)naTable)->getNamespace(),
                         naTable->hasSaltedColumn() || naTable->hasTrafReplicaColumn(),
                         (naTable->xnRepl() == COM_REPL_SYNC),
                         naTable->incrBackupEnabled(),
                         naTable->objDataUID().castToInt64(),
                         ehi);
  if (retcode)
    {
      cliRC = -1;
      if (retcode == -2)
      {
        goto label_drop;    
      }
      else
      {
        goto label_restore;
      }
    }



  cliRC = 0;
  goto label_return;

label_restore:
  if (cloneSeabaseTable(tempTable, -1,
                        naTable->getTableName().getQualifiedNameAsAnsiString(),
                        naTable,
                        ehi, cliInterface, FALSE))
    {
      cliRC = -1;
      goto label_drop;
    }
  if (gEnableRowLevelLock) {
    endXnIfStartedHere(cliInterface, xnWasStartedHere, -1,
                       FALSE /*modifyEpoch*/, FALSE /*clearObjectLocks*/);
  }
 
label_drop:  
  if (gEnableRowLevelLock) {
    endXnIfStartedHere(cliInterface, xnWasStartedHere, -1,
                       FALSE /*modifyEpoch*/, FALSE /*clearObjectLocks*/);
  }
  str_sprintf(buf, "drop table %s", tempTable.data());
  cliRC2 = cliInterface->executeImmediate(buf);

label_return:  
  if (gEnableRowLevelLock) {
    endXnIfStartedHere(cliInterface, xnWasStartedHere, 0,
                       FALSE /*modifyEpoch*/, FALSE /*clearObjectLocks*/);
  }
  if (identityGenAlways)
    cliInterface->restoreCQD("override_generated_identity_values");

  if (cliRC < 0)
    tempTable.clear();

  return (cliRC < 0 ? -1 : 0); 
}

// ----------------------------------------------------------------------------
// alterMountPartition
//
//    ALTER TABLE <table_name> MOUNT PARTITION 
//
// This statement runs in a transaction
// This statement may recreate the table, so it employs a read and read/write
// DDL lock.
// ----------------------------------------------------------------------------
void CmpSeabaseDDL::alterTableMountPartition(StmtDDLAlterTableMountPartition* alterMountPartition,
  NAString& currCatName, NAString& currSchName)
{
  Lng32 cliRC = 0;
  Lng32 retcode = 0;
  BindWA bindWA(ActiveSchemaDB(), CmpCommon::context(), TRUE /*inDDL*/);
  Parser parser(bindWA.currentCmpContext());
  NAString catalogNameBase;
  NAString schemaNameBase;
  NAString objectNameBase;
  NAString catalogNamePart;
  NAString schemaNamePart;
  NAString objectNamePart;
  NAString extTableName;
  NAString extNameForHbase;
  NAString binlogTableName;
  NATable* naTableBase = NULL;
  NATable* naTablePart = NULL;
  CorrName cn;
  CorrName cnPart;
  NAString tabName = alterMountPartition->getTableName();
  ComObjectName tableName(tabName, COM_TABLE_NAME);
  ComAnsiNamePart currCatAnsiName(currCatName);
  ComAnsiNamePart currSchAnsiName(currSchName);
  tableName.applyDefaults(currCatAnsiName, currSchAnsiName);
  ElemDDLPartitionV2* tgtPartition = alterMountPartition->getTargetPartition();
  NAString tgtPartitionName = alterMountPartition->getTargetPartitionName();
  CMPASSERT(tgtPartition);
  ElemDDLPartitionV2Array* partitionV2Array = new STMTHEAP ElemDDLPartitionV2Array(STMTHEAP);
  partitionV2Array->insert(tgtPartition);
  ElemDDLPartitionV2* lastPartition;
  NAPartition* lastNAPartition;
  NAString lastPartName;
  NAString lastPartExpr;
  ItemExpr* lastPartValue = NULL;
  NAString err;

  NAString tgtPartEntityName("");
  tgtPartEntityName = tgtPartition->getPartitionName();
  char sqlBuf[2048];
  NAString colName;
  NAString unparsed;
  NAString unparsedLast;
  NABoolean xnWasStartedHere = FALSE;
  Int64 btUid;
  Int64 partUid;
  Int64 tgtPartUID = 0;
  ComTdbVirtTablePartInfo* partInfoArray;
  TrafDesc* pColDescs = NULL;
  TrafDesc* curDesc = NULL;
  int pColLength = 0;
  QualifiedName qn;
  Int64 rowsAffected = 0;
  Int32 count = 0;
  Lng32 value_len = sizeof(Int32);

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL,
    CmpCommon::context()->sqlSession()->getParentQid());

  retcode = lockObjectDDL(alterMountPartition);
  if (retcode < 0)
    return;

  retcode = setupAndErrorChecks(tgtPartEntityName,
    qn,
    currCatName, currSchName,
    catalogNamePart, schemaNamePart, objectNamePart,
    extTableName, extNameForHbase, cnPart,
    &naTablePart,
    FALSE, TRUE,
    &cliInterface,
    COM_BASE_TABLE_OBJECT,
    SQLOperation::ALTER_TABLE,
    FALSE,
    TRUE/*process hiatus*/);
  if (retcode < 0)
    return;

  retcode = lockObjectDDL(naTablePart);
  if (retcode < 0)
    return;

  retcode = setupAndErrorChecks(tabName,
    alterMountPartition->getOrigTableNameAsQualifiedName(),
    currCatName, currSchName,
    catalogNameBase, schemaNameBase, objectNameBase,
    extTableName, extNameForHbase, cn,
    &naTableBase,
    FALSE, TRUE,
    &cliInterface,
    COM_BASE_TABLE_OBJECT,
    SQLOperation::ALTER_TABLE,
    FALSE,
    TRUE/*process hiatus*/);
  if (retcode < 0)
    return;

  if (NOT naTableBase->isPartitionV2Table())
  {
    // Object tableName is not partition table.
    *CmpCommon::diags() << DgSqlCode(-8304) << DgString0(tabName);
    return;
  }

  //check constraints
  if(naTablePart->getCheckConstraints().entries()>0
    || naTablePart->getRefConstraints().entries() > 0
    || ((naTablePart->getUniqueConstraints().entries() > 0) && (naTablePart->getClusteringIndex()->hasSyskey()))
    || ((naTablePart->getUniqueConstraints().entries() > 1) && (NOT naTablePart->getClusteringIndex()->hasSyskey())))
  {
    // can not mount table with constaints
    *CmpCommon::diags() << DgSqlCode(-8641) << DgString0(tgtPartitionName);
    return;
  }

  btUid = naTableBase->objectUid().castToInt64();
  partUid = naTablePart->objectUid().castToInt64();
  UInt32 op_base = 0;
  UInt32 op_part = 0;
  UpdateObjRefTimeParam updObjRefParamBase(-1, btUid, naTableBase->isStoredDesc(), naTableBase->storedStatsAvail());
  UpdateObjRefTimeParam updObjRefParamPart(-1, partUid, naTablePart->isStoredDesc(), naTablePart->storedStatsAvail());
  RemoveNATableParam removeNATableParam(ComQiScope::REMOVE_FROM_ALL_USERS, alterMountPartition->ddlXns(), FALSE);

  if (isExistsPartitionName(naTableBase, tgtPartitionName))
  {
    *CmpCommon::diags() << DgSqlCode(-8307)
      << DgString0(tgtPartitionName)
      << DgString1("that of any other partition of the object");
    goto label_return;
  }

  //only support range partition for now
  if (naTableBase->partitionType() != COM_RANGE_PARTITION)
  {
    // An internal executor error occurred.
    *CmpCommon::diags() << DgSqlCode(-8001);
    goto label_return;
  }

  // check if ddls of two tables match
  if (NOT compareDDLs(&cliInterface, naTableBase, naTablePart, FALSE, FALSE, FALSE))
  {
    *CmpCommon::diags() << DgSqlCode(-8306) << DgString0("DDLs of two table must be the same");
    goto label_return;
  }

  //check if the expression is valid
  for (int i = 0; i < naTableBase->getPartitionColCount(); i++)
  {
    TrafDesc* temp = TrafAllocateDDLdesc(DESC_KEYS_TYPE, STMTHEAP);
    temp->keysDesc()->tablecolnumber = naTableBase->getPartitionColIdxArray()[i];
    temp->keysDesc()->keyseqnumber = i;

    pColLength += naTableBase->getNAColumnArray()[naTableBase->getPartitionColIdxArray()[i]]
      ->getType()->getEncodedKeyLength();

    if (pColDescs == NULL)
    {
      pColDescs = temp;
      curDesc = temp;
    }
    else
    {
      curDesc->next = temp;
      curDesc = temp;
    }

  }
  retcode = validatePartitionByExpr(partitionV2Array,
    naTableBase->getColumnsDesc(),
    pColDescs,
    naTableBase->getPartitionColCount(),
    pColLength);
  if (retcode)
    goto label_return;

  //new partition bound must be higher than that of the last partition
  lastNAPartition = naTableBase->getNAPartitionArray()[naTableBase->FisrtLevelPartitionCount() - 1];
  lastPartName = lastNAPartition->getPartitionName();
  for (int i = lastNAPartition->getBoundaryValueList().entries() - 1; i >= 0; i--)
  {
    if (lastNAPartition->getBoundaryValueList()[i]->isMaxValue())
    {
      err = "Partition " + lastPartName + " has higher boundary";
      *CmpCommon::diags() << DgSqlCode(-1129) << DgString0(err);
      goto label_return;
    }
    if (i != lastNAPartition->getBoundaryValueList().entries() - 1)
      lastPartExpr += ", ";
    lastPartExpr += lastNAPartition->getBoundaryValueList()[i]->highValue;
  }
  lastPartValue = parser.getItemExprTree(lastPartExpr.data(), lastPartExpr.length());
  lastPartValue->bindNode(&bindWA);
  lastPartition = new (PARSERHEAP()) ElemDDLPartitionV2(lastPartName, lastPartValue);
  lastPartition->buildPartitionValueArray();
  partitionV2Array->insertAt(0, lastPartition);
  retcode = validatePartitionRange(partitionV2Array, naTableBase->getPartitionColCount());
  if (retcode)
    goto label_return;

  //check finished, work begin
  if (beginXnIfNotInProgress(&cliInterface, xnWasStartedHere,
    TRUE /*clearDDLList*/, FALSE /*clearObjectLocks*/))
    goto label_return;

  saveAllFlags();
  setAllFlags();

  //check if the data is in range
  if (alterMountPartition->getValidation())
  {
    lastPartValue->unparse(unparsedLast);
    tgtPartition->getPartitionValue()->unparse(unparsed);
    naTableBase->getParitionColNameAsString(colName, FALSE);
    if (unparsed == "MAXVALUE")
      str_sprintf(sqlBuf, "select count(*) from %s where (%s) < (%s);",
        tgtPartEntityName.data(),  colName.data(), unparsedLast.data());
    else
      str_sprintf(sqlBuf, "select count(*) from %s where (%s) >= (%s) or (%s) < (%s);",
        tgtPartEntityName.data(), colName.data(), unparsed.data(), colName.data(), unparsedLast.data());
    cliRC = cliInterface.executeImmediateCEFC(sqlBuf, NULL, 0, (char*)&count, &value_len, &rowsAffected);
    if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      goto label_rollback;
    }
    if (count > 0)
    {
      *CmpCommon::diags() << DgSqlCode(-8306) << DgString0("please check if the data is in range");
      goto label_rollback;
    }
  }

  //do the work, mount partition table
  cliRC = updateObjectFlags(&cliInterface, naTablePart->objectUid().castToInt64(), 
    MD_PARTITION_V2, FALSE);
  if (cliRC < 0)
    goto label_rollback;

  cliRC = addSeabaseMDPartition(&cliInterface, tgtPartition,
    catalogNameBase.data(), schemaNameBase.data(),
    objectNameBase.data(), btUid, naTableBase->FisrtLevelPartitionCount(), partUid, tgtPartitionName);
  if (cliRC < 0)
    goto label_rollback;

  setCacheAndStoredDescOp(op_part, UPDATE_REDEF_TIME);
  setCacheAndStoredDescOp(op_part, REMOVE_NATABLE);
  if (naTablePart->incrBackupEnabled())
    setCacheAndStoredDescOp(op_part, UPDATE_XDC_DDL_TABLE);
  if (naTablePart->isStoredDesc())
    setCacheAndStoredDescOp(op_part, UPDATE_SHARED_CACHE);

  cliRC = updateCachesAndStoredDesc(&cliInterface, catalogNamePart, schemaNamePart, objectNamePart,
    COM_BASE_TABLE_OBJECT, op_part, &removeNATableParam, &updObjRefParamPart, SharedCacheDDLInfo::DDL_UPDATE);
  if (cliRC < 0)
  {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_rollback;
  }

  setCacheAndStoredDescOp(op_base, UPDATE_REDEF_TIME);
  setCacheAndStoredDescOp(op_base, REMOVE_NATABLE);
  if (naTableBase->incrBackupEnabled())
    setCacheAndStoredDescOp(op_base, UPDATE_XDC_DDL_TABLE);
  if (naTableBase->isStoredDesc())
    setCacheAndStoredDescOp(op_base, UPDATE_SHARED_CACHE);

  cliRC = updateCachesAndStoredDesc(&cliInterface, catalogNameBase, schemaNameBase, objectNameBase,
    COM_BASE_TABLE_OBJECT, op_base, &removeNATableParam, &updObjRefParamBase, SharedCacheDDLInfo::DDL_UPDATE);
  if (cliRC < 0)
  {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_rollback;
  }

  endXnIfStartedHere(&cliInterface, xnWasStartedHere, 0,
    FALSE/*modifyEpochs*/, FALSE /*clearObjectLocks*/);

  goto label_return;

label_rollback:

  // rollback transaction
  endXnIfStartedHere(&cliInterface, xnWasStartedHere, -1,
    FALSE/*modifyEpochs*/, FALSE /*clearObjectLocks*/);

label_return:
  processReturn();
  return;

}


// ----------------------------------------------------------------------------
// alterUnmoutPartition
//
//    ALTER TABLE <table_name> UNMOUNT PARTITION 
//
// This statement runs in a transaction
// This statement may recreate the table, so it employs a read and read/write
// DDL lock.
// ----------------------------------------------------------------------------

Lng32 CmpSeabaseDDL::unmountSeabasePartitionTable(ExeCliInterface* cliInterface, NAPartition* partInfo,
  NAString& currCatName, NAString& currSchName, NAString& nameSpace, ExpHbaseInterface* ehi, NABoolean ddlXns)
{
  int cliRC;
  Int64 uuid;
  char sqlBuf[2048];
  UInt32 op = 0;

  BindWA bindWA(ActiveSchemaDB(), CmpCommon::context(), TRUE/*inDDL*/);
  CorrName cn(partInfo->getPartitionEntityName(), STMTHEAP, currSchName, currCatName);
  NATable* naTable = bindWA.getNATable(cn);
  if (naTable == NULL)
  {
    *CmpCommon::diags() << DgSqlCode(-4402)
      << DgString0(partInfo->getPartitionName());
    return -1;
  }

  uuid = naTable->objectUid().castToInt64();
  UpdateObjRefTimeParam updObjRefParam(-1, uuid, naTable->isStoredDesc(), naTable->storedStatsAvail());
  RemoveNATableParam removeNATableParam(ComQiScope::REMOVE_FROM_ALL_USERS, ddlXns, FALSE);

  cliRC = lockObjectDDL(naTable);
  if (cliRC < 0)
    goto label_error;

  

  cliRC = updateSeabasePartitionbtMDDataPosition(cliInterface, partInfo->getPartitionUID());
  if (cliRC < 0)
    goto label_error;

  cliRC = deleteSeabasePartitionbtMDData(cliInterface, partInfo->getPartitionUID());
  if (cliRC < 0)
    goto label_error;

  cliRC = updateObjectFlags(cliInterface, partInfo->getPartitionUID(), MD_PARTITION_V2, TRUE);
  if (cliRC < 0)
    goto label_error;

  setCacheAndStoredDescOp(op, UPDATE_REDEF_TIME);
  setCacheAndStoredDescOp(op, REMOVE_NATABLE);
  if (naTable->incrBackupEnabled())
    setCacheAndStoredDescOp(op, UPDATE_XDC_DDL_TABLE);
  if (naTable->isStoredDesc())
    setCacheAndStoredDescOp(op, UPDATE_SHARED_CACHE);

  cliRC = updateCachesAndStoredDesc(cliInterface, currCatName, currSchName, partInfo->getPartitionEntityName(),
    COM_BASE_TABLE_OBJECT, op, &removeNATableParam, &updObjRefParam, SharedCacheDDLInfo::DDL_UPDATE);
  if (cliRC < 0)
  {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_error;
  }

  return 0;
label_error:
  *CmpCommon::diags() << DgSqlCode(-4402)
    << DgString0(partInfo->getPartitionName());
  return -1;
}

void CmpSeabaseDDL::alterTableUnmountPartition(StmtDDLAlterTableUnmountPartition* alterUnmountPartition,
  NAString& currCatName, NAString& currSchName)
{
  Lng32 cliRC = 0;
  Lng32 retcode = 0;
  NABoolean xnWasStartedHere = FALSE;
  Int64 btUid;
  UInt32 op = 0;
  SQL_QIKEY* qiKeys;

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL,
    CmpCommon::context()->sqlSession()->getParentQid());

  NAString tabName = alterUnmountPartition->getTableName();
  NAString objectName = alterUnmountPartition->getOrigTableNameAsQualifiedName().getObjectName();
  //NABoolean isSubpartition = alterUnmountPartition->isSubpartition();
  NAString catalogNamePart, schemaNamePart, objectNamePart;
  NAString extTableName, extNameForHbase, binlogTableName;
  NATable* naTable = NULL;
  CorrName cn;
  NAString nameSpace;
  ExpHbaseInterface* ehi;

  retcode = lockObjectDDL(alterUnmountPartition);
  if (retcode < 0)
    return;

  retcode =
    setupAndErrorChecks(tabName,
      alterUnmountPartition->getOrigTableNameAsQualifiedName(),
      currCatName, currSchName,
      catalogNamePart, schemaNamePart, objectNamePart,
      extTableName, extNameForHbase, cn,
      &naTable,
      FALSE, TRUE,
      &cliInterface,
      COM_BASE_TABLE_OBJECT,
      SQLOperation::ALTER_TABLE,
      FALSE,
      TRUE/*process hiatus*/);
  if (retcode < 0)
    return;


  if (!naTable->isPartitionV2Table())
  {
    // Object tableName is not partition table.
    *CmpCommon::diags() << DgSqlCode(-8304) << DgString0(objectName);
    return;
  }

  btUid = naTable->objectUid().castToInt64();
  UpdateObjRefTimeParam updObjRefParam(-1, btUid, naTable->isStoredDesc(), naTable->storedStatsAvail());
  RemoveNATableParam removeNATableParam(ComQiScope::REMOVE_FROM_ALL_USERS, alterUnmountPartition->ddlXns(), FALSE);

  BindWA bindWA(ActiveSchemaDB(), CmpCommon::context(), TRUE/*inDDL*/);
  TableDesc* tableDesc = NULL;
  tableDesc = bindWA.createTableDesc(naTable, cn, FALSE, NULL);
  if (!tableDesc)
  {
    // An internal executor error occurred.
    *CmpCommon::diags() << DgSqlCode(-8001);
    return;
  }

  // duplicate check
  ElemDDLPartitionNameAndForValuesArray& partArray = alterUnmountPartition->getPartitionNameAndForValuesArray();
  std::vector<NAString> unmountPartEntityNameVec;
  std::map<NAString, NAString> partEntityNameMap;
  NAString dupilcatePartName;
  Lng32 ret = checkPartition(&partArray, tableDesc, bindWA, objectName, dupilcatePartName,
    &unmountPartEntityNameVec, &partEntityNameMap);
  if (ret == -1)
  {
    // An internal executor error occurred.
    *CmpCommon::diags() << DgSqlCode(-8001);
    return;
  }
  else if (ret == 1)
  {
    // The DDL request has duplicate references to partition name.
    *CmpCommon::diags() << DgSqlCode(-8305) << DgString0(dupilcatePartName);
    return;
  }
  else if (ret == 2)
  {
    *CmpCommon::diags() << DgSqlCode(-8306) << DgString0("The partition number is invalid or out-of-range");
    return;
  }
  else if (ret == 3)
  {
    // The specified partition partName does not exist.
    *CmpCommon::diags() << DgSqlCode(-1097) << DgString0(dupilcatePartName);
    return;
  }

  Int32 firstLevelPartCnt = naTable->FisrtLevelPartitionCount();
  const NAPartitionArray& partitionArray = naTable->getPartitionArray();
  NAString partName;
  NAString partEntityName;
  std::vector<NAPartition*> unmountNAPartitionVec;

  // record all partEntityName and corresponding NAPartition.
    // <firstpartEntityName, firstNAPartition>
  std::map<NAString, NAPartition*> allNAPartitionMap;
  std::map<NAString, NAPartition*>::iterator partInfoIt;

  for (Lng32 i = 0; i < firstLevelPartCnt; ++i)
  {
    partEntityName = partitionArray[i]->getPartitionEntityName();
    allNAPartitionMap[partEntityName] = partitionArray[i];
  }

  for (Lng32 i = 0; i < partArray.entries(); ++i)
  {
    ElemDDLPartitionNameAndForValues* part = partArray[i];
    if (part->isPartitionForValues())
    {
      ItemExprList valList(part->getPartitionForValues(), bindWA.wHeap());
      LIST(NAString)* partEntityNames = tableDesc->getMatchedPartInfo(&bindWA, valList);
      if (!partEntityNames || partEntityNames->entries() == 0)
      {
        *CmpCommon::diags() << DgSqlCode(-8306)
          << DgString0("The partition number is invalid or out-of-range");
        processReturn();
        return;
      }
      partEntityName = (*partEntityNames)[0];
    }
    else
    {
      partEntityName = "";
      for (Lng32 i = 0; i < firstLevelPartCnt; ++i)
      {
        if (part->getPartitionName() == partitionArray[i]->getPartitionName())
        {
          partEntityName = partitionArray[i]->getPartitionEntityName();
          break;
        }
      }
    }

    partInfoIt = allNAPartitionMap.find(partEntityName);
    if (partInfoIt == allNAPartitionMap.end() && !part->isPartitionForValues())
    {
      // The specified partition partName does not exist.
      *CmpCommon::diags() << DgSqlCode(-1097)
        << DgString0(part->getPartitionName());
      processReturn();
      return;
    }

    unmountNAPartitionVec.push_back(partInfoIt->second);
  }

  if (firstLevelPartCnt == 1)
  {
    // Dropping the only partition of an object is not allowed.
    *CmpCommon::diags() << DgSqlCode(-8306) << DgString0("Unmounting the only partition of an object is not allowed");
    return;
  }

  if (firstLevelPartCnt - unmountNAPartitionVec.size() < 1)
  {
    // Operation is not allowed because no partition exists after drop.
    *CmpCommon::diags() << DgSqlCode(-8306) << DgString0("No partition exists after unmount");
    return;
  }


  //check finished, work begin
  if (beginXnIfNotInProgress(&cliInterface, xnWasStartedHere,
    TRUE /*clearDDLList*/, FALSE /*clearObjectLocks*/))
    goto label_return;


  saveAllFlags();
  setAllFlags();

  // unmount partition
  nameSpace = naTable->getNamespace();
  ehi = allocEHI(naTable->storageType());
  for (Lng32 i = 0; i < unmountNAPartitionVec.size(); ++i)
  {
    cliRC = unmountSeabasePartitionTable(&cliInterface, unmountNAPartitionVec[i], currCatName, currSchName,
      nameSpace, ehi, alterUnmountPartition->ddlXns());
    if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      goto label_rollback;
    }
  }

  setCacheAndStoredDescOp(op, UPDATE_REDEF_TIME);
  setCacheAndStoredDescOp(op, REMOVE_NATABLE);
  if (naTable->incrBackupEnabled())
    setCacheAndStoredDescOp(op, UPDATE_XDC_DDL_TABLE);
  if (naTable->isStoredDesc())
    setCacheAndStoredDescOp(op, UPDATE_SHARED_CACHE);

  qiKeys = new (STMTHEAP) SQL_QIKEY[firstLevelPartCnt];
  for (Lng32 i = 0; i < firstLevelPartCnt; ++i)
  {
    qiKeys[i].ddlObjectUID = partitionArray[i]->getPartitionUID();
    qiKeys[i].operation[0] = 'O';
    qiKeys[i].operation[1] = 'R';
  }
  cliRC = SQL_EXEC_SetSecInvalidKeys(firstLevelPartCnt, qiKeys);
  if (cliRC < 0)
  {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_rollback;
  }

  cliRC = updateCachesAndStoredDesc(&cliInterface, catalogNamePart, schemaNamePart, objectNamePart,
    COM_BASE_TABLE_OBJECT, op, &removeNATableParam, &updObjRefParam, SharedCacheDDLInfo::DDL_UPDATE);
  if (cliRC < 0)
  {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_rollback;
  }

  endXnIfStartedHere(&cliInterface, xnWasStartedHere, 0,
    FALSE/*modifyEpochs*/, FALSE /*clearObjectLocks*/);

  goto label_return;

label_rollback:

  // rollback transaction
  endXnIfStartedHere(&cliInterface, xnWasStartedHere, -1,
    FALSE/*modifyEpochs*/, FALSE /*clearObjectLocks*/);

label_return:
  return;
}



// ----------------------------------------------------------------------------
// alterAddPartition
//
//    ALTER TABLE <table_name> ADD PARTITION 
//
// This statement runs in a transaction
// This statement may recreate the table, so it employs a read and read/write
// DDL lock.
// ----------------------------------------------------------------------------
void CmpSeabaseDDL::alterTableAddPartition(StmtDDLAlterTableAddPartition* alterAddPartition,
  NAString& currCatName, NAString& currSchName)
{
  Lng32 cliRC = 0;
  Lng32 retcode = 0;
  NABoolean xnWasStartedHere = FALSE;
  BindWA bindWA(ActiveSchemaDB(), CmpCommon::context(), TRUE /*inDDL*/);
  Parser parser(bindWA.currentCmpContext());

  NAString tabName = alterAddPartition->getTableName();
  ComObjectName tableName(tabName, COM_TABLE_NAME);
  ComAnsiNamePart currCatAnsiName(currCatName);
  ComAnsiNamePart currSchAnsiName(currSchName);
  tableName.applyDefaults(currCatAnsiName, currSchAnsiName);

  NAString partitionNames("");
  NAList<NAString> partitionNameList(NULL);
  ElemDDLPartitionV2Array* partitionV2Array = new STMTHEAP ElemDDLPartitionV2Array(STMTHEAP);
  if (alterAddPartition->isAddSinglePartition()) {
    ElemDDLPartitionV2* tgtPartition = alterAddPartition->getTargetPartition();
    CMPASSERT(tgtPartition);
    NAString partitionName(tgtPartition->getPartitionName());
    partitionNames += partitionName;
    partitionNameList.insert(partitionName);
    partitionV2Array->insert(tgtPartition);
  } else {
   ElemDDLPartitionList *tgtPartitions = alterAddPartition->getTargetPartitions();
   CMPASSERT(tgtPartitions);
   for (int i = 0; i < tgtPartitions->entries(); i++) {
     ElemDDLPartitionV2* tgtPartition = (*tgtPartitions)[i]->castToElemDDLPartitionV2();
     NAString partitionName(tgtPartition->getPartitionName());
     partitionNames += partitionName;
     if (i < tgtPartitions->entries() - 1) {
         partitionNames += ',';
     }
     partitionNameList.insert(partitionName);
     partitionV2Array->insert(tgtPartition);
   }
  }

  NAString catalogNamePart;
  NAString schemaNamePart;
  NAString objectNamePart;
  NATable* naTable = NULL;

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL,
    CmpCommon::context()->sqlSession()->getParentQid());

  retcode = lockObjectDDL(alterAddPartition);
  if (retcode < 0)
    return;
  
  NAString extTableName;
  NAString extNameForHbase;
  CorrName cn;
  retcode = setupAndErrorChecks(tabName,
    alterAddPartition->getOrigTableNameAsQualifiedName(),
    currCatName, currSchName,
    catalogNamePart, schemaNamePart, objectNamePart,
    extTableName, extNameForHbase, cn,
    &naTable,
    FALSE, TRUE,
    &cliInterface,
    COM_BASE_TABLE_OBJECT,
    SQLOperation::ALTER_TABLE,
    FALSE,
    TRUE/*process hiatus*/);
  if (retcode < 0)
    return;

  if (NOT naTable->isPartitionV2Table())
  {
    // Object tableName is not partition table.
    *CmpCommon::diags() << DgSqlCode(-8304) << DgString0(tabName);
    return;
  }

  Int64 btUid = naTable->objectUid().castToInt64();
  if (isExistsPartitionName(naTable, partitionNameList))
  {
    *CmpCommon::diags() << DgSqlCode(-8307)
      << DgString0(partitionNames)
      << DgString1("that of any other partition of the object");
    return;
  }

  //only support range partition for now
  if (naTable->partitionType() != COM_RANGE_PARTITION)
  {
    // An internal executor error occurred.
    *CmpCommon::diags() << DgSqlCode(-8001);
    return;
  }

  //check if the expression is valid
  TrafDesc* pColDescs = NULL;
  TrafDesc* curDesc = NULL;
  int pColLength = 0;
  for (int i = 0; i < naTable->getPartitionColCount(); i++)
  {
    TrafDesc* temp = TrafAllocateDDLdesc(DESC_KEYS_TYPE, STMTHEAP);
    temp->keysDesc()->tablecolnumber = naTable->getPartitionColIdxArray()[i];
    temp->keysDesc()->keyseqnumber = i;

    pColLength += naTable->getNAColumnArray()[naTable->getPartitionColIdxArray()[i]]
      ->getType()->getEncodedKeyLength();

    if (pColDescs == NULL)
    {
      pColDescs = temp;
      curDesc = temp;
    }
    else
    {
      curDesc->next = temp;
      curDesc = temp;
    }
  }
  NAHashDictionary<NAString, Int32> *partitionNameMap
      = new(STMTHEAP) NAHashDictionary<NAString, Int32>(NAString::hash, 101, TRUE, STMTHEAP);
  retcode = validatePartitionByExpr(partitionV2Array, naTable->getColumnsDesc(),
                                    pColDescs, naTable->getPartitionColCount(),
                                    pColLength, partitionNameMap);
  if (retcode)
    return;


  //new partition bound must be higher than that of the last partition
  NAString err("");
  Int32 firstLevelPartitionCnt = naTable->FisrtLevelPartitionCount();
  NAPartition* lastNAPartition = naTable->getNAPartitionArray()[firstLevelPartitionCnt - 1];
  NAString lastPartitionName = lastNAPartition->getPartitionName();
  NAString lastPartitionExpr;
  for (int i = lastNAPartition->getBoundaryValueList().entries() - 1; i >= 0; i--)
  {
    lastPartitionExpr += lastNAPartition->getBoundaryValueList()[i]->highValue;
    if (i != 0)
      lastPartitionExpr += ",";
  }
  ItemExpr* lastPartitionValue = parser.getItemExprTree(lastPartitionExpr.data(),
                                                        lastPartitionExpr.length());
  ElemDDLPartitionV2* lastPartition = new (PARSERHEAP())
      ElemDDLPartitionV2(lastPartitionName, lastPartitionValue);
  retcode = lastPartition->replaceWithConstValue();
  if (retcode)
      return;
  if (lastPartition->buildPartitionValueArray(false) < 0)
    return;

  partitionV2Array->insertAt(0, lastPartition);
  retcode = validatePartitionRange(partitionV2Array, naTable->getPartitionColCount());
  if (retcode)
    return;

  //check finished, work begin
  UInt32 op = 0;
  UpdateObjRefTimeParam updObjRefParam(-1, btUid, naTable->isStoredDesc(), naTable->storedStatsAvail());
  RemoveNATableParam removeNATableParam(ComQiScope::REMOVE_FROM_ALL_USERS, alterAddPartition->ddlXns(), FALSE);

  if (beginXnIfNotInProgress(&cliInterface, xnWasStartedHere,
    TRUE /*clearDDLList*/, FALSE /*clearObjectLocks*/))
    goto label_return;

  saveAllFlags();
  setAllFlags();
  
  for (int i = 1; i < partitionV2Array->entries(); i++)
  {
    ElemDDLPartitionV2* tgtPartition = partitionV2Array->at(i);
    NAString partitionName(tgtPartition->getPartitionName());
    
    //do the work, create partition table
    retcode = createSeabasePartitionTable(&cliInterface, partitionName,
                                          catalogNamePart, schemaNamePart, objectNamePart);
    if (retcode < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      goto label_rollback;
    }

    cliRC = addSeabaseMDPartition(&cliInterface, tgtPartition,
                                  catalogNamePart.data(), schemaNamePart.data(),
                                  objectNamePart.data(), btUid, firstLevelPartitionCnt);
    firstLevelPartitionCnt++;
    if (cliRC < 0)
        goto label_rollback;
  }

  setCacheAndStoredDescOp(op, UPDATE_REDEF_TIME);
  setCacheAndStoredDescOp(op, REMOVE_NATABLE);
  if (naTable->incrBackupEnabled())
    setCacheAndStoredDescOp(op, UPDATE_XDC_DDL_TABLE);
  if (naTable->isStoredDesc())
    setCacheAndStoredDescOp(op, UPDATE_SHARED_CACHE);
  
  cliRC = updateCachesAndStoredDesc(&cliInterface, catalogNamePart, schemaNamePart, objectNamePart,
                                    COM_BASE_TABLE_OBJECT, op, &removeNATableParam, &updObjRefParam,
                                    SharedCacheDDLInfo::DDL_UPDATE);
  if (cliRC < 0)
  {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_rollback;
  }

  endXnIfStartedHere(&cliInterface, xnWasStartedHere, 0,
    FALSE/*modifyEpochs*/, FALSE /*clearObjectLocks*/);
  goto label_return;

label_rollback:

  // rollback transaction
  endXnIfStartedHere(&cliInterface, xnWasStartedHere, -1,
    FALSE/*modifyEpochs*/, FALSE /*clearObjectLocks*/);

label_return:
  return;
}

void CmpSeabaseDDL::alterTableMergePartition(
                                  StmtDDLAlterTableMergePartition * mergePartition,
                                  NAString & currCatName,
                                  NAString & currSchName)
{

  Lng32 cliRC = 0;
  Lng32 retcode = 0;
  Int64 defTime = NA_JulianTimestamp();
  Int64 flags = 0;
  NAString tabName = mergePartition->getTableName();
  NAString catalogNamePart, schemaNamePart, objectNamePart;
  NAString extTableName, extNameForHbase, binlogTableName;
  NATable * naTable = NULL;
  CorrName cn;
  Lng32 mergedNum = 0;
  char dummyChar[6] = "empty";

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL,
                               CmpCommon::context()->sqlSession()->getParentQid());

  retcode = lockObjectDDL(mergePartition);
  if (retcode < 0) {
    processReturn();
    return;
  }

  retcode =
    setupAndErrorChecks(tabName,
                        mergePartition->getOrigTableNameAsQualifiedName(),
                        currCatName, currSchName,
                        catalogNamePart, schemaNamePart, objectNamePart,
                        extTableName, extNameForHbase, cn,
                        &naTable,
                        FALSE, TRUE,
                        &cliInterface,
                        COM_BASE_TABLE_OBJECT,
                        SQLOperation::ALTER_TABLE,
                        FALSE,
                        TRUE/*process hiatus*/);
  if (retcode < 0)
    {
      processReturn();
      return;
    }

  if (NOT naTable->isPartitionV2Table())
  {
    *CmpCommon::diags() << DgSqlCode(-8304) << DgString0(tabName);
    processReturn();
    return;
  }
  if (naTable->subPartitionType() != NO_PARTITION)
  {
    *CmpCommon::diags() << DgSqlCode(-8306) << DgString0("sub-partition is not support yet for merge partition");
    processReturn();
    return;
  }

  binlogTableName = schemaNamePart + "." + objectNamePart;

  Int64 baseObjUID = naTable->objectUid().castToInt64();

  saveAllFlags();
  setAllFlags();

  char sqlBuf[2048];
  char tgtPartTableName[256];

  const char *tgtPartValueStr = NULL;
  Int64 tgtPartUID = 0;

  NATable *naTgtPart = NULL;

  NAString *newClonePart = new (STMTHEAP) NAString("");
  NABoolean newPartCreated = FALSE;
  NABoolean newCloneCreated = FALSE;

  Int32 isCommit = 0; //0 is commit, -1 is rollback

  NAPartition * tgtPartInfo = mergePartition->getTgtPart();
  NAList<NAPartition *> sortedSrcPart = mergePartition->getSortedSrcPart();

  int newTgtPartTmpPosition = sortedSrcPart[0]->getPartPosition();
  int tgtPartPosition = -1;
  if (tgtPartInfo != NULL)
  {
    // if target is one of merging partition
    for (int idx = 0; idx < sortedSrcPart.entries(); idx++)
    {
      if (tgtPartInfo->getPartPosition() == sortedSrcPart[idx]->getPartPosition())
      {
        tgtPartPosition = tgtPartInfo->getPartPosition();
      }
    }

    // The existing tgt partition should be one of merging partitions,
    // or the partition beside the merging range
    if (tgtPartPosition < 0 &&
        tgtPartInfo->getPartPosition() != sortedSrcPart[0]->getPartPosition() - 1 &&
        tgtPartInfo->getPartPosition() !=
                  sortedSrcPart[sortedSrcPart.entries()-1]->getPartPosition() + 1)
    {
      *CmpCommon::diags() << DgSqlCode(-1271);
      processReturn();
      return;
    }
  }

  // src partition and tgt partition is the same, finish
  if (sortedSrcPart.entries() == 1 &&
      tgtPartInfo != NULL &&
      tgtPartInfo->getPartPosition() == sortedSrcPart[0]->getPartPosition())
  {
    return;
  }

  // get max value for tgt partition
  NAString lowValuetext = "", highValueText = "";
  NAPartition * maxValPart = NULL;
  if (tgtPartInfo != NULL &&
      tgtPartInfo->getPartPosition() > sortedSrcPart[sortedSrcPart.entries()-1]->getPartPosition())
  {
    maxValPart = tgtPartInfo;
  }
  else
  {
    maxValPart = sortedSrcPart[sortedSrcPart.entries()-1];
  }
  maxValPart->boundaryValueToString(lowValuetext, highValueText);
  tgtPartValueStr = highValueText.data();

  NABoolean xnWasStartedHere = FALSE;
  if (beginXnIfNotInProgress(&cliInterface, xnWasStartedHere,
                             TRUE /*clearDDLList*/, FALSE /*clearObjectLocks*/))
  {
    processReturn();
    return;
  }

  NAString tgtPartName = mergePartition->getTargetPartition();
  getPartitionTableName(tgtPartTableName, sizeof(tgtPartTableName),
                        objectNamePart, tgtPartName);

  BindWA bindWA(ActiveSchemaDB(), CmpCommon::context(), TRUE);
  CorrName tgtCN(tgtPartTableName, STMTHEAP, schemaNamePart, catalogNamePart);

  RemoveNATableParam removeNATableParam(ComQiScope::REMOVE_FROM_ALL_USERS,
                                        mergePartition->ddlXns(), FALSE);

  // do merge 01, prepare target partition
  if (tgtPartInfo == NULL)
  {
    // create tgt partition table if not exist
    retcode = createSeabasePartitionTable(&cliInterface, tgtPartName,
                                catalogNamePart, schemaNamePart, objectNamePart);
    if (retcode < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      goto label_rollback;
    }

    naTgtPart = ActiveSchemaDB()->getNATableDB()->get(tgtCN, &bindWA, NULL, FALSE);
    tgtPartUID = naTgtPart->objectUid().get_value();

    // tmp append the parition info for meta range check of new partition
    snprintf(sqlBuf, sizeof(sqlBuf), "upsert into %s.\"%s\".%s values (%ld, '%s', '%s', %ld, '%s', %d, "
                  " cast(? as char(%ld bytes) character set utf8 not null), '%s', "
                  " '%s', '%s', %ld, %ld, '%s', '%s', '%s', '%s');",
                  getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_PARTITIONS,
                  baseObjUID, tgtPartName.data(), tgtPartTableName, tgtPartUID,
                  COM_PRIMARY_PARTITION_LIT, newTgtPartTmpPosition,
                  strlen(tgtPartValueStr), COM_YES_LIT, COM_NO_LIT, COM_NO_LIT,
                  defTime, flags, dummyChar, dummyChar, dummyChar, dummyChar);
    cliRC = cliInterface.executeImmediateCEFC(sqlBuf,
                                       CONST_CAST(char*, tgtPartValueStr), str_len(tgtPartValueStr),
                                       NULL, NULL, NULL);
    if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      goto label_rollback;
    }

    newPartCreated = TRUE;
  }
  else
  {
    char addStr[100];
    str_itoa((Int64)newClonePart, addStr);
    *newClonePart = catalogNamePart;
    *newClonePart += ".";
    *newClonePart += schemaNamePart;
    *newClonePart += ".";
    *newClonePart += "CLONE_";
    *newClonePart += tgtPartTableName;
    *newClonePart += "_";
    *newClonePart += addStr;

    naTgtPart = ActiveSchemaDB()->getNATableDB()->get(tgtCN, &bindWA, NULL, FALSE);
    tgtPartUID = naTgtPart->objectUid().get_value();

    // clone target partition naTable to target newClonePart for target partition restore

    // cloneSeabaseTable() is use CQD OVERRIDE_SYSKEY, may cause NATable inconsistent
    // do clone manually
    cliRC = createSeabaseTableLike2(tgtCN, *newClonePart,
                                        FALSE, FALSE, TRUE, FALSE);
    if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      goto label_rollback;
    }

    if (naTgtPart->hasSecondaryIndexes()) // user indexe
        cliRC = cliInterface.holdAndSetCQD("hide_indexes", "ALL");
    cliRC = cliInterface.holdAndSetCQD("attempt_esp_parallelism", "ON");

    str_sprintf(sqlBuf, "upsert using load into %s select * from %s.\"%s\".\"%s\"",
                newClonePart->data(),
                catalogNamePart.data(), schemaNamePart.data(), tgtPartTableName);
    cliRC = cliInterface.executeImmediate(sqlBuf);
    if (cliRC < 0)
      {
        cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
        processReturn();

        goto label_rollback;
      }

    if (naTgtPart->hasSecondaryIndexes()) // user indexes
      cliInterface.restoreCQD("hide_indexes");
    cliInterface.restoreCQD("attempt_esp_parallelism");

    // create snapshot for existing partition
    newCloneCreated = TRUE;
  }

  // lock target partition for later QIK
  retcode = lockObjectDDL(naTgtPart);
  if (retcode < 0) {
    goto label_rollback;
  }

  // do merge 02, disable global index, and disable local index if tgt partition exists

  // todo : disable global index

  // disable local and common indexes of existing target partition
  if (naTgtPart != NULL && naTgtPart->hasSecondaryIndexes())
  {
    snprintf(sqlBuf, sizeof(sqlBuf), "alter table %s.%s.%s disable all indexes;",
           catalogNamePart.data(), schemaNamePart.data(), tgtPartTableName);
    cliRC = cliInterface.executeImmediate(sqlBuf);
    if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      goto label_rollback;
    }
  }

  // do merge 03, load data into new tgt partition table, and drop src partition table
  cliInterface.holdAndSetCQD("TRAF_UPSERT_THROUGH_PARTITIONS", "ON");
  for (int idx = 0; idx < sortedSrcPart.entries(); idx++)
  {
    NAPartition *srcPartInfo = sortedSrcPart[idx];
    if (srcPartInfo->getPartPosition() != tgtPartPosition)
    {
      snprintf(sqlBuf, sizeof(sqlBuf), "upsert using load into %s.%s.%s select * from %s.%s.%s;",
               catalogNamePart.data(), schemaNamePart.data(), tgtPartTableName,
               catalogNamePart.data(), schemaNamePart.data(), srcPartInfo->getPartitionEntityName());
      cliRC = cliInterface.executeImmediate(sqlBuf);
      if (cliRC < 0)
      {
        cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
        goto label_rollback;
      }

      cliRC = dropSeabasePartitionTable(&cliInterface,
                                        catalogNamePart,
                                        schemaNamePart,
                                        srcPartInfo->getPartitionEntityName());
      if (cliRC < 0)
      {
        cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
        goto label_rollback;
      }

      mergedNum++;
    }
  }
  cliInterface.restoreCQD("TRAF_UPSERT_THROUGH_PARTITIONS");

  // do merge 04, change MD data

  // update/insert MD of target partition
  if (naTable->partitionType() == RANGE_PARTITION)
  {
    if (newPartCreated)
    {
      snprintf(sqlBuf, sizeof(sqlBuf), "update %s.\"%s\".%s "
                    " set PARTITION_ORDINAL_POSITION=PARTITION_ORDINAL_POSITION+1 "
                    " where PARTITION_ORDINAL_POSITION>=%d and PARENT_UID=%ld;",
                    getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_PARTITIONS,
                    sortedSrcPart[0]->getPartPosition(), baseObjUID);
      cliRC = cliInterface.executeImmediate(sqlBuf);
      if (cliRC < 0)
      {
        cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
        goto label_rollback;
      }

      // update position of tgt partition
      snprintf(sqlBuf, sizeof(sqlBuf), "upsert into %s.\"%s\".%s values (%ld, '%s', '%s', %ld, '%s', %d, "
                  " cast(? as char(%ld bytes) character set utf8 not null), '%s', "
                  " '%s', '%s', %ld, %ld, '%s', '%s', '%s', '%s');",
                  getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_PARTITIONS,
                  baseObjUID, tgtPartName.data(), tgtPartTableName, tgtPartUID,
                  COM_PRIMARY_PARTITION_LIT, newTgtPartTmpPosition,
                  strlen(tgtPartValueStr), COM_YES_LIT, COM_NO_LIT, COM_NO_LIT,
                  defTime, flags, dummyChar, dummyChar, dummyChar, dummyChar);
      cliRC = cliInterface.executeImmediateCEFC(sqlBuf,
                                       CONST_CAST(char*, tgtPartValueStr), str_len(tgtPartValueStr),
                                       NULL, NULL, NULL);
      if (cliRC < 0)
      {
        cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
        goto label_rollback;
      }
    }
    else
    {
      snprintf(sqlBuf, sizeof(sqlBuf),
                  "update %s.\"%s\".%s set "
                  " PARTITION_EXPRESSION=cast(? as char(%d bytes) character set utf8 not null) "
                  " where PARENT_UID=%ld and PARTITION_NAME='%s';",
                  getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_PARTITIONS,
                  str_len(tgtPartValueStr), baseObjUID, tgtPartName.data());
      cliRC = cliInterface.executeImmediateCEFC(sqlBuf,
                                       CONST_CAST(char*, tgtPartValueStr), str_len(tgtPartValueStr),
                                       NULL, NULL, NULL);
      if (cliRC < 0)
      {
        cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
        goto label_rollback;
      }
    }
  }

  // do merge 05, enable global and local index

  // todo global index

  // local and common index for existing partition
  if (naTgtPart != NULL && naTgtPart->hasSecondaryIndexes())
  {
    const NAFileSetList &naTgtFsList = naTgtPart->getIndexList();

    cliInterface.holdAndSetCQD("ALLOW_TRUNCATE_IN_USER_XN", "ON");
    for (Lng32 i = 0; i < naTgtFsList.entries(); i++)
    {
      const NAFileSet * naFS = naTgtFsList[i];

      // skip clustering index
      if (naFS->getKeytag() == 0)
        continue;

      snprintf(sqlBuf, sizeof(sqlBuf), "POPULATE INDEX %s ON %s.%s.%s;",
               naFS->getFileSetName().getObjectName().data(),
               catalogNamePart.data(), schemaNamePart.data(), tgtPartTableName);
      cliRC = cliInterface.executeImmediate(sqlBuf);
      if (cliRC < 0)
      {
        cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
        goto label_rollback;
      }
    }
    cliInterface.restoreCQD("ALLOW_TRUNCATE_IN_USER_XN");
  }
  else if (newPartCreated && naTable->hasSecondaryIndexes() &&
           // && naTable has local index
           naTgtPart != NULL && NOT naTgtPart->hasSecondaryIndexes())
  {
    // create local index for new created partition without index
    const NAFileSetList &naFsList = naTable->getIndexList();
    for (Lng32 i = 0; i < naFsList.entries(); i++)
    {
      NAString idxKeys("");
      const NAFileSet * naFS = naFsList[i];

      // skip clustering index
      if (naFS->getKeytag() == 0)
        continue;

      const NAColumnArray &naIndexColArr = naFS->getAllColumns();
      for (Lng32 j = 0; j < naIndexColArr.entries(); j++)
      {
        NAColumn * col = naIndexColArr.getColumn(j);
        if (idxKeys.length() != 0)
        {
          idxKeys += ", ";
        }
        idxKeys += col->getColName();
        idxKeys += naIndexColArr.isAscending(j) ? " ASC" : " DESC";
      }

      snprintf(sqlBuf, sizeof(sqlBuf), "create index INDEX_%s_%s on "
                                       " %s.%s.%s(%s);",
           naFS->getFileSetName().getObjectName().data(), tgtPartTableName,
           catalogNamePart.data(), schemaNamePart.data(), tgtPartTableName,
           idxKeys.data());
      cliRC = cliInterface.executeImmediate(sqlBuf);
      if (cliRC < 0)
      {
        cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
        goto label_rollback;
      }
    }
  }


  // update natable cache and stats for partition table
  cliRC = updateCachesAndStoredDescByNATable(&cliInterface,
                          catalogNamePart, schemaNamePart, tgtPartTableName,
                          COM_BASE_TABLE_OBJECT, UPDATE_REDEF_TIME | REMOVE_NATABLE,
                          naTgtPart, &removeNATableParam,
                          SharedCacheDDLInfo::DDL_UPDATE);
  if (cliRC < 0)
  {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_rollback;
  }
  cliRC = updateCachesAndStoredDescByNATable(&cliInterface,
                          catalogNamePart, schemaNamePart, objectNamePart,
                          COM_BASE_TABLE_OBJECT, UPDATE_REDEF_TIME | REMOVE_NATABLE,
                          naTable, &removeNATableParam,
                          SharedCacheDDLInfo::DDL_UPDATE);
  if (cliRC < 0)
  {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_rollback;
  }

  isCommit = 0;

  goto label_return;

label_rollback:

  // restore existing partition from clone
  if (newCloneCreated)
  {
    // truncate existing partition first
    cliInterface.holdAndSetCQD("ALLOW_TRUNCATE_IN_USER_XN", "ON");
    snprintf(sqlBuf, sizeof(sqlBuf), "truncate %s.%s.%s;",
             catalogNamePart.data(), schemaNamePart.data(), tgtPartTableName);
    cliRC = cliInterface.executeImmediate(sqlBuf);
    if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    }

    // restore existing partition data

    // disable all indexes, or upsert using load will go in transaction
    snprintf(sqlBuf, sizeof(sqlBuf), "alter table %s.%s.%s disable all indexes;",
           catalogNamePart.data(), schemaNamePart.data(), tgtPartTableName);
    cliRC = cliInterface.executeImmediate(sqlBuf);
    if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    }

    // load data
    cliInterface.holdAndSetCQD("attempt_esp_parallelism", "ON");
    snprintf(sqlBuf, sizeof(sqlBuf), "upsert using load into %s.%s.%s select * from %s;",
             catalogNamePart.data(), schemaNamePart.data(), tgtPartTableName,
             newClonePart->data());
    cliRC = cliInterface.executeImmediate(sqlBuf);
    if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    }

    // populate all indexes, what about old disabled index ?
    const NAFileSetList &naTgtFsList = naTgtPart->getIndexList();
    for (Lng32 i = 0; i < naTgtFsList.entries(); i++)
    {
      const NAFileSet * naFS = naTgtFsList[i];

      // skip clustering index
      if (naFS->getKeytag() == 0)
        continue;

      snprintf(sqlBuf, sizeof(sqlBuf), "POPULATE INDEX %s ON %s.%s.%s;",
               naFS->getFileSetName().getObjectName().data(),
               catalogNamePart.data(), schemaNamePart.data(), tgtPartTableName);
      cliRC = cliInterface.executeImmediate(sqlBuf);
      if (cliRC < 0)
      {
        cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      }
    }
    cliInterface.restoreCQD("attempt_esp_parallelism");
    cliInterface.restoreCQD("ALLOW_TRUNCATE_IN_USER_XN");
  }

  isCommit = -1;

label_return:

  // only drop clone tables when commit
  if (newCloneCreated && isCommit == 0)
  {
    str_sprintf(sqlBuf, "drop table %s", newClonePart->data());
    cliInterface.executeImmediate(sqlBuf);
  }

  endXnIfStartedHere(&cliInterface, xnWasStartedHere, isCommit,
                     FALSE/*modifyEpochs*/, FALSE /*clearObjectLocks*/);

  processReturn();
  return ;
}

void CmpSeabaseDDL::alterTableExchangePartition(
                                  StmtDDLAlterTableExchangePartition * exchangePartition,
                                  NAString & currCatName,
                                  NAString & currSchName)
{
  Lng32 cliRC = 0;
  Lng32 retcode = 0;
  Int64 defTime = NA_JulianTimestamp();
  Int64 flags = 0;
  // variables for partition table
  NAString tabName = exchangePartition->getTableName();
  NAString catalogNamePart, schemaNamePart, objectNamePart;
  NAString extTableName, extNameForHbase, binlogTableName;
  NATable * naTable = NULL;
  CorrName cn;
  // variables for exchange table
  NAString exchgTabName = exchangePartition->getExchangeTableName()
                                          .getQualifiedNameAsAnsiString();
  NAString exCatalogNamePart, exSchemaNamePart, exObjectNamePart;
  NAString exExtTableName, exExtNameForHbase, exBinlogTableName;
  NATable * exchgNaTable = NULL;
  CorrName exchgCN;

  char dummyChar[6] = "empty";

  char sqlBuf[2048];

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL,
                               CmpCommon::context()->sqlSession()->getParentQid());

  retcode = lockObjectDDL(exchangePartition);
  if (retcode < 0) {
    processReturn();
    return;
  }

  retcode =
    setupAndErrorChecks(tabName,
                        exchangePartition->getOrigTableNameAsQualifiedName(),
                        currCatName, currSchName,
                        catalogNamePart, schemaNamePart, objectNamePart,
                        extTableName, extNameForHbase, cn,
                        &naTable,
                        FALSE, TRUE,
                        &cliInterface,
                        COM_BASE_TABLE_OBJECT,
                        SQLOperation::ALTER_TABLE,
                        FALSE,
                        TRUE/*process hiatus*/);
  if (retcode < 0)
    {
      processReturn();
      return;
    }

  if (NOT naTable->isPartitionV2Table())
  {
    // Object tableName is not partition table.
    *CmpCommon::diags() << DgSqlCode(-8304) << DgString0(tabName);
    processReturn();
    return;
  }


  retcode =
    setupAndErrorChecks(exchgTabName,
                        exchangePartition->getExchangeTableName(),
                        currCatName, currSchName,
                        exCatalogNamePart, exSchemaNamePart, exObjectNamePart,
                        exExtTableName, exExtNameForHbase, exchgCN,
                        &exchgNaTable,
                        FALSE, TRUE,
                        &cliInterface,
                        COM_BASE_TABLE_OBJECT,
                        SQLOperation::ALTER_TABLE,
                        FALSE,
                        TRUE/*process hiatus*/);
  if (retcode < 0)
    {
      processReturn();
      return;
    }

  if (exchgNaTable->isPartitionV2Table())
  {
    *CmpCommon::diags() << DgSqlCode(-8306) << DgString0("ALTER TABLE EXCHANGE requires a non-partitioned, non-clustered table");
    processReturn();
    return;
  }

  if (exchangePartition->getIsSubPartition())
  {
    *CmpCommon::diags() << DgSqlCode(-8306) << DgString0("sub-partition is not support yet for exchange partition");
    processReturn();
    return;
  }

  retcode = lockObjectDDL(exchgNaTable);
  if (retcode < 0) {
    processReturn();
    return;
  }

  NATable * naPartTable = NULL;
  NAPartition * naPartition = exchangePartition->getNAPartition();
  ComASSERT(naPartition);

  NABoolean isCommit = -1;
  NABoolean newCloneCreated = FALSE;
  NAString *newClonePart = new (STMTHEAP) NAString("");
  NAString *newCloneTable = new (STMTHEAP) NAString("");
  Int64 exchgTableUID = exchgNaTable->objectUid().get_value();

  NAString partEntityName = naPartition->getPartitionEntityName();

  CorrName exchgPartCN(partEntityName, STMTHEAP, schemaNamePart, catalogNamePart);
  CorrName exchgTableCN(exObjectNamePart, STMTHEAP, exSchemaNamePart, exCatalogNamePart);

  NABoolean doValidate = TRUE;
  Int64 count =0;
  Lng32 count_len = sizeof(Int64);
  Int64 rowsAffected = 0;

  NAString partCols("");
  NAString partValLow("");
  NAString partValHigh("");

  const Int32 * partColPosition = naTable->getPartitionColIdxArray();
  Int32 partColCount = naTable->getPartitionColCount();

  BindWA bindWA(ActiveSchemaDB(), CmpCommon::context());
  naPartTable = ActiveSchemaDB()->getNATableDB()->get(exchgPartCN, &bindWA, NULL, FALSE);
  CMPASSERT(naPartTable);

  retcode = lockObjectDDL(naPartTable);
  if (retcode < 0) {
    processReturn();
    return;
  }

  // start transaction
  NABoolean xnWasStartedHere = FALSE;
  if (beginXnIfNotInProgress(&cliInterface, xnWasStartedHere,
                             TRUE /*clearDDLList*/, FALSE /*clearObjectLocks*/))
  {
    processReturn();
    return;
  }

  RemoveNATableParam removeNATableParam(ComQiScope::REMOVE_FROM_ALL_USERS,
                                        exchangePartition->ddlXns(), FALSE);

  // do check DDL between partition and exchange table
  if (!USE_UUID_AS_HBASE_TABLENAME)
  {
    // check 01, whether ddl of two table matches
    if (NOT compareDDLs(&cliInterface, naTable, exchgNaTable, TRUE, TRUE, TRUE))
    {
      *CmpCommon::diags() << DgSqlCode(-8306) << DgString0("DDLs of two table must be the same");
      goto label_rollback;
    }
  }
  else
  {
    // check 01, whether ddl of two table matches
    if (NOT compareDDLs(&cliInterface, naTable, exchgNaTable, FALSE, FALSE, FALSE))
    {
      *CmpCommon::diags() << DgSqlCode(-8306) << DgString0("DDLs of two table must be the same");
      goto label_rollback;
    }
  }

  // do validate data of common table
  for (Int32 i = 0; i < partColCount; i++)
  {
    if (i > 0)
    {
      partCols += ", ";
    }
    partCols += exchgNaTable->getNAColumnArray()[partColPosition[i]]->getColName();
  }

  naPartition->boundaryValueToString(partValLow, partValHigh);

  if (partValHigh == "MAXVALUE")
  {
    if (partValLow.length() > 0)
    {
      str_sprintf(sqlBuf, "select count(1) from %s.%s.%s where (%s) < (%s);",
                  exCatalogNamePart.data(), exSchemaNamePart.data(), exObjectNamePart.data(),
                  partCols.data(), partValLow.data());
    }
    else
    {
      // no need to do data validation since there is only one partition of MAXVALUE
      doValidate = FALSE;
    }
  }
  else
  {
    if (partValLow.length() > 0)
    {
      str_sprintf(sqlBuf, "select count(1) from %s.%s.%s where (%s) < (%s) or (%s) >= (%s);",
                  exCatalogNamePart.data(), exSchemaNamePart.data(), exObjectNamePart.data(),
                  partCols.data(), partValLow.data(),
                  partCols.data(), partValHigh.data());
    }
    else
    {
      str_sprintf(sqlBuf, "select count(1) from %s.%s.%s where (%s) >= (%s);",
                  exCatalogNamePart.data(), exSchemaNamePart.data(), exObjectNamePart.data(),
                  partCols.data(), partValHigh.data());
    }
  }
  if (doValidate)
  {
    cliRC = cliInterface.executeImmediateCEFC(sqlBuf, NULL, 0, (char*)&count, &count_len, &rowsAffected);
    if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      goto label_rollback;
    }
    if (count > 0)
    {
      *CmpCommon::diags() << DgSqlCode(-8306) << DgString0("data in exchange table is out of range");
      goto label_rollback;
    }
  }

  // disable all indexes
  if (naPartTable->hasSecondaryIndexes())
  {
    snprintf(sqlBuf, sizeof(sqlBuf), "alter table %s.%s.%s disable all indexes;",
           catalogNamePart.data(), schemaNamePart.data(), partEntityName.data());
    cliRC = cliInterface.executeImmediate(sqlBuf);
    if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      goto label_rollback;
    }
  }
  if (exchgNaTable->hasSecondaryIndexes())
  {
    snprintf(sqlBuf, sizeof(sqlBuf), "alter table %s.%s.%s disable all indexes;",
           exCatalogNamePart.data(), exSchemaNamePart.data(), exObjectNamePart.data());
    cliRC = cliInterface.executeImmediate(sqlBuf);
    if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      goto label_rollback;
    }
  }

  if (!USE_UUID_AS_HBASE_TABLENAME)
  {
    // exchange 01, create clones of both exchanging table and exchanging partition
    {
      char addStr[100];
      str_itoa((Int64)newClonePart, addStr);
      *newClonePart = catalogNamePart;
      *newClonePart += ".";
      *newClonePart += schemaNamePart;
      *newClonePart += ".";
      *newClonePart += "CLONE_";
      *newClonePart += naPartition->getPartitionEntityName();
      *newClonePart += "_";
      *newClonePart += addStr;

      str_itoa((Int64)newCloneTable, addStr);
      *newCloneTable = exCatalogNamePart;
      *newCloneTable += ".";
      *newCloneTable += exSchemaNamePart;
      *newCloneTable += ".";
      *newCloneTable += "CLONE_";
      *newCloneTable += exObjectNamePart;
      *newCloneTable += "_";
      *newCloneTable += addStr;

      // cloneSeabaseTable() is use CQD OVERRIDE_SYSKEY, may cause NATable inconsistent
      // do clone manually
      cliRC = createSeabaseTableLike2(exchgPartCN, *newClonePart,
                                          FALSE, FALSE, TRUE, FALSE);
      if (cliRC < 0)
      {
        cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
        goto label_rollback;
      }

      cliRC = createSeabaseTableLike2(exchgTableCN, *newCloneTable,
                                          FALSE, FALSE, TRUE, FALSE);
      if (cliRC < 0)
      {
        cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
        goto label_rollback;
      }

      cliInterface.holdAndSetCQD("hide_indexes", "ALL");
      cliInterface.holdAndSetCQD("attempt_esp_parallelism", "ON");
      str_sprintf(sqlBuf, "upsert using load into %s select * from %s.\"%s\".\"%s\"",
                  newClonePart->data(),
                  catalogNamePart.data(), schemaNamePart.data(), partEntityName.data());
      cliRC = cliInterface.executeImmediate(sqlBuf);
      if (cliRC < 0)
      {
        cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
        processReturn();

        goto label_rollback;
      }

      str_sprintf(sqlBuf, "upsert using load into %s select * from %s.\"%s\".\"%s\"",
                  newCloneTable->data(),
                  exCatalogNamePart.data(), exSchemaNamePart.data(), exObjectNamePart.data());
      cliRC = cliInterface.executeImmediate(sqlBuf);
      if (cliRC < 0)
      {
        cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
        processReturn();

        goto label_rollback;
      }

      cliInterface.restoreCQD("hide_indexes");
      cliInterface.restoreCQD("attempt_esp_parallelism");

      newCloneCreated = TRUE;
    }

    // exchange 02, exchange data
    {
      cliInterface.holdAndSetCQD("ALLOW_TRUNCATE_IN_USER_XN", "ON");

      // truncate exchanging partition and table
      snprintf(sqlBuf, sizeof(sqlBuf), "truncate %s.%s.%s;",
               catalogNamePart.data(), schemaNamePart.data(), partEntityName.data());
      cliRC = cliInterface.executeImmediate(sqlBuf);
      if (cliRC < 0)
      {
        cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
        goto label_rollback;
      }

      snprintf(sqlBuf, sizeof(sqlBuf), "truncate %s.%s.%s;",
               exCatalogNamePart.data(), exSchemaNamePart.data(), exObjectNamePart.data());
      cliRC = cliInterface.executeImmediate(sqlBuf);
      if (cliRC < 0)
      {
        cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
        goto label_rollback;
      }

      cliInterface.restoreCQD("ALLOW_TRUNCATE_IN_USER_XN");

      // upsert data from clone table
      cliRC = cliInterface.holdAndSetCQD("attempt_esp_parallelism", "ON");

      snprintf(sqlBuf, sizeof(sqlBuf), "upsert using load into %s.%s.%s select * from %s;",
               catalogNamePart.data(), schemaNamePart.data(), partEntityName.data(),
               newCloneTable->data());
      cliRC = cliInterface.executeImmediate(sqlBuf);
      if (cliRC < 0)
      {
        cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
        goto label_rollback;
      }

      snprintf(sqlBuf, sizeof(sqlBuf), "upsert using load into %s.%s.%s select * from %s;",
               exCatalogNamePart.data(), exSchemaNamePart.data(), exObjectNamePart.data(),
               newClonePart->data());
      cliRC = cliInterface.executeImmediate(sqlBuf);
      if (cliRC < 0)
      {
        cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
        goto label_rollback;
      }

      cliInterface.restoreCQD("attempt_esp_parallelism");
    }

  }
  else
  {
    Int64 uidPartition = naPartTable->objDataUID().castToInt64();;
    Int64 uidExchgTable = exchgNaTable->objDataUID().castToInt64();
    Int64 rowsAffected = 0;

    str_sprintf(sqlBuf, "update %s.\"%s\".%s set DATA_OBJECT_UID=%ld "
                          " where CATALOG_NAME='%s' and SCHEMA_NAME='%s' and OBJECT_NAME='%s' "
                          " and OBJECT_TYPE='BT'; ",
                getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS, uidExchgTable,
                catalogNamePart.data(), schemaNamePart.data(), partEntityName.data());
    cliRC = cliInterface.executeImmediate(sqlBuf);
    if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      goto label_rollback;
    }

    str_sprintf(sqlBuf, "update %s.\"%s\".%s set DATA_OBJECT_UID=%ld "
                          " where CATALOG_NAME='%s' and SCHEMA_NAME='%s' and OBJECT_NAME='%s' "
                          " and OBJECT_TYPE='BT'; ",
                getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS, uidPartition,
                exCatalogNamePart.data(), exSchemaNamePart.data(), exObjectNamePart.data());
    cliRC = cliInterface.executeImmediate(sqlBuf);
    if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      goto label_rollback;
    }


    ActiveSchemaDB()->getNATableDB()->remove(naPartTable->getKey());
    ActiveSchemaDB()->getNATableDB()->remove(exchgNaTable->getKey());

  }

  cliInterface.holdAndSetCQD("ALLOW_TRUNCATE_IN_USER_XN", "ON");
  cliInterface.holdAndSetCQD("attempt_esp_parallelism", "ON");
  if (naPartTable->hasSecondaryIndexes())
  {
    const NAFileSetList &naTgtFsList = naPartTable->getIndexList();
    for (Lng32 i = 0; i < naTgtFsList.entries(); i++)
    {
      const NAFileSet * naFS = naTgtFsList[i];

      // skip clustering index
      if (naFS->getKeytag() == 0)
        continue;

      // truncate index, since "POPULATE INDEX PURGEDATA" is not working here
      cliRC = purgedataObjectAfterError(cliInterface,
                          catalogNamePart, schemaNamePart,
                          naFS->getFileSetName().getObjectName(),
                          COM_INDEX_OBJECT, FALSE);
      if (cliRC < 0)
        goto label_return;

      snprintf(sqlBuf, sizeof(sqlBuf), "POPULATE INDEX %s ON %s.%s.%s;",
               naFS->getFileSetName().getObjectName().data(),
              catalogNamePart.data(), schemaNamePart.data(), partEntityName.data());
      cliRC = cliInterface.executeImmediate(sqlBuf);
      if (cliRC < 0)
      {
        cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
        goto label_rollback;
      }
    }
  }
  if (exchgNaTable->hasSecondaryIndexes())
  {
    const NAFileSetList &exchgFsList = exchgNaTable->getIndexList();
    for (Lng32 i = 0; i < exchgFsList.entries(); i++)
    {
      const NAFileSet * naFS = exchgFsList[i];

      // skip clustering index
      if (naFS->getKeytag() == 0)
        continue;

      // truncate index, since "POPULATE INDEX PURGEDATA" is not working here
      cliRC = purgedataObjectAfterError(cliInterface,
                          exCatalogNamePart, exSchemaNamePart,
                          naFS->getFileSetName().getObjectName(),
                          COM_INDEX_OBJECT, FALSE);
      if (cliRC < 0)
        goto label_return;

      snprintf(sqlBuf, sizeof(sqlBuf), "POPULATE INDEX %s ON %s.%s.%s;",
               naFS->getFileSetName().getObjectName().data(),
               exCatalogNamePart.data(), exSchemaNamePart.data(), exObjectNamePart.data());
      cliRC = cliInterface.executeImmediate(sqlBuf);
      if (cliRC < 0)
      {
        cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
        goto label_rollback;
      }
    }
  }
  cliInterface.restoreCQD("ALLOW_TRUNCATE_IN_USER_XN");
  cliInterface.restoreCQD("attempt_esp_parallelism");

  // update natable cache and stats for exchanging partition
  cliRC = updateCachesAndStoredDescByNATable(&cliInterface,
                          catalogNamePart, schemaNamePart, partEntityName,
                          COM_BASE_TABLE_OBJECT, UPDATE_REDEF_TIME | REMOVE_NATABLE,
                          naPartTable, &removeNATableParam,
                          SharedCacheDDLInfo::DDL_UPDATE);
  if (cliRC < 0)
  {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_rollback;
  }

  // update natable cache and stats for partition table
  cliRC = updateCachesAndStoredDescByNATable(&cliInterface,
                          catalogNamePart, schemaNamePart, objectNamePart,
                          COM_BASE_TABLE_OBJECT, UPDATE_REDEF_TIME | REMOVE_NATABLE,
                          naTable, &removeNATableParam,
                          SharedCacheDDLInfo::DDL_UPDATE);
  if (cliRC < 0)
  {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_rollback;
  }

  // update natable cache and stats for exchanging table
  cliRC = updateCachesAndStoredDescByNATable(&cliInterface,
                          exCatalogNamePart, exSchemaNamePart, exObjectNamePart,
                          COM_BASE_TABLE_OBJECT, UPDATE_REDEF_TIME | REMOVE_NATABLE,
                          exchgNaTable, &removeNATableParam,
                          SharedCacheDDLInfo::DDL_UPDATE);
  if (cliRC < 0)
  {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_rollback;
  }


  isCommit = 0;

  goto label_return;

label_rollback:

  // restore exchanging partition and table
  if (newCloneCreated)
  {
    cliInterface.holdAndSetCQD("ALLOW_TRUNCATE_IN_USER_XN", "ON");

    // truncate exchanging partition and table
    snprintf(sqlBuf, sizeof(sqlBuf), "truncate %s.%s.%s;",
             catalogNamePart.data(), schemaNamePart.data(), partEntityName.data());
    cliRC = cliInterface.executeImmediate(sqlBuf);
    if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    }

    snprintf(sqlBuf, sizeof(sqlBuf), "truncate %s.%s.%s;",
             exCatalogNamePart.data(), exSchemaNamePart.data(), exObjectNamePart.data());
    cliRC = cliInterface.executeImmediate(sqlBuf);
    if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    }

    cliInterface.restoreCQD("ALLOW_TRUNCATE_IN_USER_XN");

    cliInterface.holdAndSetCQD("attempt_esp_parallelism", "ON");

    snprintf(sqlBuf, sizeof(sqlBuf), "upsert using load into %s.%s.%s select * from %s;",
             catalogNamePart.data(), schemaNamePart.data(), partEntityName.data(),
             newClonePart->data());
    cliRC = cliInterface.executeImmediate(sqlBuf);
    if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    }

    snprintf(sqlBuf, sizeof(sqlBuf), "upsert using load into %s.%s.%s select * from %s;",
             exCatalogNamePart.data(), exSchemaNamePart.data(), exObjectNamePart.data(),
             newCloneTable->data());
    cliRC = cliInterface.executeImmediate(sqlBuf);
    if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    }

    cliInterface.restoreCQD("attempt_esp_parallelism");
  }

  isCommit = -1;

label_return:

  // only drop clone tables when commit
  if (newCloneCreated && isCommit == 0)
  {
    str_sprintf(sqlBuf, "drop table %s", newClonePart->data());
    cliRC = cliInterface.executeImmediate(sqlBuf);
    if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    }

    str_sprintf(sqlBuf, "drop table %s", newCloneTable->data());
    cliRC = cliInterface.executeImmediate(sqlBuf);
    if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    }
  }

  endXnIfStartedHere(&cliInterface, xnWasStartedHere, isCommit,
                     FALSE/*modifyEpochs*/, FALSE /*clearObjectLocks*/);

  processReturn();
  return ;
}

// ----------------------------------------------------------------------------
// alterSeabaseTableAddColumn
//
//    ALTER TABLE <table> ADD COLUMN <name> <description>
//
// This statement runs in a transaction
// This statement may recreate the table, so it employs a read and read/write
// DDL lock.
// ----------------------------------------------------------------------------
void CmpSeabaseDDL::alterSeabaseTableAddColumn(
                                               StmtDDLAlterTableAddColumn * alterAddColNode,
                                               NAString &currCatName, NAString &currSchName)
{
  Lng32 cliRC = 0;
  Lng32 retcode = 0;

  NAString tabName = alterAddColNode->getTableName();
  ComObjectName tableName(tabName, COM_TABLE_NAME);
  ComAnsiNamePart currCatAnsiName(currCatName);
  ComAnsiNamePart currSchAnsiName(currSchName);
  NAArray<HbaseStr> *defStringArray = NULL;
  HbaseStr *oldCols = NULL;
  HbaseStr *oldKeys = NULL;
  NAString colColsString;
  NAString addColString;
  char fullColBuf[1024*20]; //some table may have 1024 columns
  char fullKeyBuf[128*20];
  char addColBuf[128];
  NAString fullColBufString;
  NAString fullKeyBufString;
  Int64 ddlStartTs = 0;


  tableName.applyDefaults(currCatAnsiName, currSchAnsiName);

  memset(fullColBuf, 0, 1024*20);
  memset(addColBuf, 0, 128);

  NABoolean hbmapExtTable = FALSE;
  if (((tableName.getCatalogNamePartAsAnsiString() == HBASE_SYSTEM_CATALOG) &&
       (tableName.getSchemaNamePartAsAnsiString(TRUE) == HBASE_MAP_SCHEMA)) ||
      (alterAddColNode->isExternal()))
    {
      hbmapExtTable = TRUE;
    }

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, 
                               CmpCommon::context()->sqlSession()->getParentQid());

  NAString sampleTableName;
  Int64 sampleTableUid = 0;
  NAString catalogNamePart;
  NAString schemaNamePart;
  NAString objectNamePart;
  NAString extTableName;
  NAString extNameForHbase;
  NAString binlogTableName;
  NATable * naTable = NULL;
  CorrName cn;

  retcode = lockObjectDDL(alterAddColNode);
  if (retcode < 0) {
    processReturn();
    return;
  }

  retcode = 
    setupAndErrorChecks(tabName, 
                        alterAddColNode->getOrigTableNameAsQualifiedName(),
                        currCatName, currSchName,
                        catalogNamePart, schemaNamePart, objectNamePart,
                        extTableName, extNameForHbase, cn,
                        &naTable, 
                        FALSE, TRUE,
                        &cliInterface,
                        COM_BASE_TABLE_OBJECT,
                        SQLOperation::ALTER_TABLE,
                        hbmapExtTable,
                        TRUE/*process hiatus*/);
  if (retcode < 0)
    {
      processReturn();
      return;
    }

  //after lock we can get the ddlStartTs
  struct timeval tv;
  gettimeofday(&tv, NULL);
  ddlStartTs = tv.tv_sec * 1000 + tv.tv_usec / 1000;

  binlogTableName = schemaNamePart + "." + objectNamePart;

  ExpHbaseInterface * ehi = allocEHI(naTable->storageType());
  if (ehi == NULL)
    {
      processReturn();
      
      return;
    }

  // add X lock
  if (lockRequired(ehi, naTable, catalogNamePart, schemaNamePart, objectNamePart, HBaseLockMode::LOCK_X, FALSE/* useHbaseXN */, FALSE/* replSync */, FALSE/* incrementalBackup */, FALSE/* asyncOperation */, TRUE/* noConflictCheck */, TRUE/* registerRegion */))
  {
    deallocEHI(ehi); 
    processReturn();
    
    return;
  }
  
  const NAColumnArray &nacolArr = naTable->getNAColumnArray();

  ElemDDLColDefArray ColDefArray = alterAddColNode->getColDefArray();
  ElemDDLColDef *pColDef = ColDefArray[0];

  // Do not allow to using a NOT NULL constraint without a default
  // clause.  Do not allow DEFAULT NULL together with NOT NULL.
  if (pColDef->getIsConstraintNotNullSpecified())
    {
      if (pColDef->getDefaultClauseStatus() != ElemDDLColDef::DEFAULT_CLAUSE_SPEC)
        {
          *CmpCommon::diags() << DgSqlCode(-CAT_DEFAULT_REQUIRED);
          deallocEHI(ehi);
          processReturn();

          return;
        }
      ConstValue *pDefVal = (ConstValue *)pColDef->getDefaultValueExpr();

      if ((pDefVal) &&
          (pDefVal->origOpType() != ITM_CURRENT_USER) &&
          (pDefVal->origOpType() != ITM_CURRENT_TIMESTAMP) &&
          (pDefVal->origOpType() != ITM_UNIX_TIMESTAMP) &&
          (pDefVal->origOpType() != ITM_UNIQUE_ID) &&
          (pDefVal->origOpType() != ITM_CAST) &&
          (pDefVal->origOpType() != ITM_CURRENT_CATALOG) &&
          (pDefVal->origOpType() != ITM_CURRENT_SCHEMA))
        {
          if (pDefVal->isNull()) 
            {
              *CmpCommon::diags() << DgSqlCode(-CAT_CANNOT_BE_DEFAULT_NULL_AND_NOT_NULL);
              deallocEHI(ehi);
              processReturn();

              return;
            }
        }
    }
  
  //Do not allow NO DEFAULT
  if (pColDef->getDefaultClauseStatus() == ElemDDLColDef::NO_DEFAULT_CLAUSE_SPEC)
    {
      *CmpCommon::diags() << DgSqlCode(-CAT_DEFAULT_REQUIRED);
      deallocEHI(ehi);
      processReturn();

      return;
    }

  if (pColDef->getSGOptions())
    {
      *CmpCommon::diags() << DgSqlCode(-1514);
      deallocEHI(ehi);
      processReturn();

      return;
    }

  NAString colFamily;
  NAString colName;
  Lng32 datatype, length, precision, scale, dt_start, dt_end, nullable, upshifted;
  ComColumnClass colClass;
  ComColumnDefaultClass defaultClass;
  NAString charset, defVal;
  NAString heading;
  ULng32 hbaseColFlags;
  Int64 colFlags;
  ComLobsStorageType lobStorage;
  NAString compDefnStr;

  // if hbase map format, turn off global serialization default to off.
  NABoolean hbaseSerialization = FALSE;
  NAString hbVal;
  if (naTable->isHbaseMapTable())
    {
      if (CmpCommon::getDefault(HBASE_SERIALIZATION) == DF_ON)
        {
          NAString value("OFF");
          hbVal = "ON";
          ActiveSchemaDB()->getDefaults().validateAndInsert(
               "hbase_serialization", value, FALSE);
          hbaseSerialization = TRUE;
        }
    }

  retcode = getColInfo(pColDef,
                       FALSE, // not a metadata, histogram or repository column 
                       colFamily,
                       colName, 
                       naTable->isSQLMXAlignedTable(),
                       datatype, length, precision, scale, dt_start, dt_end, upshifted, nullable,
                       charset, colClass, defaultClass, defVal, heading, lobStorage, 
                       compDefnStr,
                       hbaseColFlags, colFlags);

  //generate new columns def string for binlog reader
  char binlogTableDefString[128];
  memset(binlogTableDefString, 0, 128);

  const char* ansitype = getAnsiTypeStrFromFSType(datatype);
  //trim ansitype
  NAString tmpansitype(ansitype);
  NAString stripedstr = tmpansitype.strip(NAString::trailing, ' ');
  sprintf(binlogTableDefString,"%d-%d-%d-%s-%d-%d-%s-%s-%d-%d-%d-%s;" , datatype, nullable, length, "A", precision, scale ,charset.data(),stripedstr.data(),dt_start,dt_end,(int)nacolArr.entries(),colName.strip(NAString::trailing,' ').data());

  // the space of query buff must big enough 
  char query[4000 + defVal.length()];
  if (hbaseSerialization)
    {
      ActiveSchemaDB()->getDefaults().validateAndInsert
        ("hbase_serialization", hbVal, FALSE);
    }
  if (retcode)
    return;

  if ((CmpCommon::getDefault(TRAF_ALLOW_RESERVED_COLNAMES) == DF_OFF) &&
      (ComTrafReservedColName(colName)))
    {
      *CmpCommon::diags() << DgSqlCode(-CAT_RESERVED_COLUMN_NAME)
                          << DgString0(colName);
      
      deallocEHI(ehi);
      processReturn();
      return;
    }

  if ((naTable->isHbaseMapTable()) &&
      ((! nullable) ||
       (defaultClass != COM_NULL_DEFAULT)))
    {
      *CmpCommon::diags() << DgSqlCode(-3242)
                          << DgString0("Added column of an HBase mapped table must be nullable with default value of NULL.");
                  
      deallocEHI(ehi);
      processReturn();
      return; 
    }

  if (colFamily.isNull())
    {
      colFamily = naTable->defaultUserColFam();
    }

  NABoolean addFam = FALSE;
  NAString trafColFam;

  if ((colFamily == SEABASE_DEFAULT_COL_FAMILY) ||
      (naTable->isHbaseMapTable()))
    trafColFam = colFamily;
  else
    {
      CollIndex idx = naTable->allColFams().index(colFamily);
      if (idx == NULL_COLL_INDEX) // doesnt exist, add it
        {
          idx = naTable->allColFams().entries();
          addFam = TRUE;
        }
      
      genTrafColFam(idx, trafColFam);
    }

  const NAColumn * nacol = nacolArr.getColumn(colName);
  if (nacol)
    {
      // column exists. Error or return, depending on 'if not exists' option.
      if (NOT alterAddColNode->addIfNotExists())
        {
          *CmpCommon::diags() << DgSqlCode(-CAT_DUPLICATE_COLUMNS)
                              << DgColumnName(colName);
        }
      deallocEHI(ehi);
      processReturn();

      return;
    }

  // Get a read only DDL lock on table
  if (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL))
    {
      retcode = ddlSetObjectEpoch(naTable, STMTHEAP, true /*readsAllowed*/);
      if (retcode < 0)
        {
          processReturn();
          return;
        }
    }

  NAString tableNamespace = naTable->getNamespace();
  char *col_name = new(STMTHEAP) char[colName.length() + 1];
  strcpy(col_name, (char*)colName.data());


  ULng32 maxColQual = nacolArr.getMaxTrafHbaseColQualifier();

  NAString quotedHeading;
  if (NOT heading.isNull())
    {
      ToQuotedString(quotedHeading, heading, FALSE);
    }
  
  NAString quotedDefVal;
  if (NOT defVal.isNull())
    {
      ToQuotedString(quotedDefVal, defVal, FALSE);
    }

  Int64 objUID = naTable->objectUid().castToInt64();
  Int32 newColNum = naTable->getColumnCount();
  for (Int32 cc = nacolArr.entries()-1; cc >= 0; cc--)
    {
      const NAColumn *nac = nacolArr[cc];

      if ((NOT naTable->isSQLMXAlignedTable()) &&
          (nac->isComputedColumn()))
        {
          str_sprintf(query, "update %s.\"%s\".%s set column_number = column_number + 1 where object_uid = %ld and column_number = %d",
                      getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_COLUMNS,
                      objUID,
                      nac->getPosition());
          
          cliRC = cliInterface.executeImmediate(query);
          if (cliRC < 0)
            {
              cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
              
              return;
            }

          str_sprintf(query, "update %s.\"%s\".%s set column_number = column_number + 1 where object_uid = %ld and column_number = %d",
                      getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_KEYS,
                      objUID,
                      nac->getPosition());
          
          cliRC = cliInterface.executeImmediate(query);
          if (cliRC < 0)
            {
              cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
              
              return;
            }

          str_sprintf(query, "update %s.\"%s\".%s set sub_id = sub_id + 1 where text_uid = %ld and text_type = %d and sub_id = %d",
                      getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_TEXT,
                      objUID,
                      COM_COMPUTED_COL_TEXT,
                      nac->getPosition());
          
          cliRC = cliInterface.executeImmediate(query);
          if (cliRC < 0)
            {
              cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
              
              return;
            }

          // keys for indexes refer to base table column number.
          // modify it so they now refer to new column numbers.
          if (naTable->hasSecondaryIndexes())
            {
              const NAFileSetList &naFsList = naTable->getIndexList();
              
              for (Lng32 i = 0; i < naFsList.entries(); i++)
                {
                  const NAFileSet * naFS = naFsList[i];
                  
                  // skip clustering index
                  if (naFS->getKeytag() == 0)
                    continue;
                  
                  const QualifiedName &indexName = naFS->getFileSetName();
                  
                  str_sprintf(query, "update %s.\"%s\".%s set column_number = column_number + 1  where column_number = %d and object_uid = (select object_uid from %s.\"%s\".%s where catalog_name = '%s' and schema_name = '%s' and object_name = '%s' and object_type = 'IX') ",
                              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_KEYS,
                              nac->getPosition(),
                              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS,
                              indexName.getCatalogName().data(),
                              indexName.getSchemaName().data(),
                              indexName.getObjectName().data());
                  cliRC = cliInterface.executeImmediate(query);
                  if (cliRC < 0)
                    {
                      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
                      
                      goto label_return;
                    }
                  
                } // for
            } // secondary indexes present
          
          newColNum--;
        }
    }

  str_sprintf(query, "insert into %s.\"%s\".%s values (%ld, '%s', %d, '" COM_ADDED_USER_COLUMN_LIT"', %d, '%s', %d, %d, %d, %d, %d, '%s', %d, %d, '%s', %d, '%s', '%s', '%s', '%u', '%s', '%s', %ld )",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_COLUMNS,
              objUID,
              col_name,
              newColNum,
              datatype,
              getAnsiTypeStrFromFSType(datatype),
              length,
              precision,
              scale,
              dt_start,
              dt_end,
              (upshifted ? "Y" : "N"),
              hbaseColFlags, 
              nullable,
              (char*)charset.data(),
              (Lng32)defaultClass,
              (quotedDefVal.isNull() ? " " : quotedDefVal.data()),//column DEFAULT_VALUE can't be NULL,so we can't use ''(null) here
              (quotedHeading.isNull() ? " " : quotedHeading.data()),//column COLUMN_HEADING can't be NULL,so we can't use ''(null) here
              trafColFam.data(),
              maxColQual+1,
              COM_UNKNOWN_PARAM_DIRECTION_LIT,
              "N",
              colFlags);
  
  cliRC = cliInterface.executeImmediate(query);
  if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      
      processReturn();
      
      return;
    }

  // if column family of added col doesnt exist in the table, add it
  if (addFam)
    {
      NAString currColFams;
      if (getTextFromMD(&cliInterface, objUID, COM_HBASE_COL_FAMILY_TEXT, 
                        0, currColFams))
        {
          deallocEHI(ehi); 
          processReturn();
          return;
        }

      Lng32 cliRC = deleteFromTextTable(&cliInterface, objUID, 
                                        COM_HBASE_COL_FAMILY_TEXT, 0);
      if (cliRC < 0)
        {
          deallocEHI(ehi); 
          processReturn();
          return;
        }

      NAString allColFams = currColFams + " " + colFamily;

      cliRC = updateTextTable(&cliInterface, objUID, 
                              COM_HBASE_COL_FAMILY_TEXT, 0,
                              allColFams);
      if (cliRC < 0)
        {
          *CmpCommon::diags()
            << DgSqlCode(-CAT_UNABLE_TO_CREATE_OBJECT)
            << DgTableName(extTableName);
          
          deallocEHI(ehi); 
          processReturn();
          return;
        }

      HbaseCreateOption hbco("NAME", trafColFam.data()); 
      NAList<HbaseCreateOption*> hbcol(STMTHEAP);
      hbcol.insert(&hbco);
      ElemDDLHbaseOptions edhbo(&hbcol, STMTHEAP);

      NAList<NAString> nal(STMTHEAP);
      nal.insert(trafColFam);

      HbaseStr hbaseTable;
      hbaseTable.val = (char*)extNameForHbase.data();
      hbaseTable.len = extNameForHbase.length();
      cliRC = alterHbaseTable(ehi,
                              &hbaseTable,
                              nal,
                              &(edhbo.getHbaseOptions()),
                              alterAddColNode->ddlXns());
      if (cliRC < 0)
        {
          deallocEHI(ehi);
          processReturn();
          return;
        }   
      
    }

  // Store string format of composite column definition in TEXT table.
  // definition of composite columns
  // Format:  numCols  colNumber  defnSize defnStr...
  //          8 bytes  8 bytes    8 bytes  defnSize bytes
  //                   Repeat for numCols entries
  if ((colFlags & SEABASE_COLUMN_IS_COMPOSITE) &&
      (! compDefnStr.isNull()))
    {
      Lng32 numCompCols = 0;
      NAString textTableCompDefnStr;
      if (naTable->hasCompositeColumns())
        {
          cliRC = getTextFromMD(&cliInterface,
                                naTable->objectUid().castToInt64(),
                                COM_COMPOSITE_DEFN_TEXT,
                                0,
                                textTableCompDefnStr);
          if (cliRC < 0)
            {
              cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
              deallocEHI(ehi); 
              processReturn();
              
              return;
            }  

          const char * compEntry = NULL;
          if (! textTableCompDefnStr.isNull())
            {
              compEntry = textTableCompDefnStr.data();
              numCompCols = str_atoi(compEntry, 8);
            }
        }

      char buf[100];
      numCompCols++;
      snprintf(buf, sizeof(buf), "%08d%08d",
               newColNum, (int)compDefnStr.length());
      textTableCompDefnStr += buf;
      textTableCompDefnStr += compDefnStr;
      
      snprintf(buf, sizeof(buf), "%08d", numCompCols);

      // replace the first 8 bytes with current number of comp cols
      textTableCompDefnStr.replace(0, 8, buf);

      cliRC = updateTextTable(&cliInterface,
                              naTable->objectUid().castToInt64(),
                              COM_COMPOSITE_DEFN_TEXT,
                              0,
                              textTableCompDefnStr,
                              NULL, -1,
                              TRUE/*delete existing row*/);
      if (cliRC < 0)
        {
          cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
          deallocEHI(ehi); 
          processReturn();
          
          return;
        }
    } // composite

  if (naTable->isStoredDesc())
    {
      int32_t diagsMark = CmpCommon::diags()->mark();
      str_sprintf(query, "alter trafodion metadata shared cache for table %s delete internal",
                  naTable->getTableName().getQualifiedNameAsAnsiString().data());

      NAString msg(query);
      msg.prepend("Removing object for add column: ");
      QRLogger::log(CAT_SQL_EXE, LL_WARN, "%s", msg.data());

      cliRC = cliInterface.executeImmediate(query);

      // If unable to remove from shared cache, it may not exist, continue 
      // If the operation is later rolled back, this table will not be added
      // back to shared cache - TBD
      CmpCommon::diags()->rewind(diagsMark);
    }

  if (naTable->incrBackupEnabled())
  {
    cliRC = updateSeabaseXDCDDLTable(&cliInterface,
                                     catalogNamePart.data(), schemaNamePart.data(),
                                     objectNamePart.data(), COM_BASE_TABLE_OBJECT_LIT);
    if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      deallocEHI(ehi);
      processReturn();
      return;
    }
  }

  //update the table column definition string in binlog
  if (!ComIsTrafodionReservedSchemaName(schemaNamePart) && naTable->isSeabaseTable() == TRUE) {
    //first , get the current string
    retcode = ehi->getTableDefForBinlog(binlogTableName, &defStringArray);
    if (retcode < 0 )
    {
        *CmpCommon::diags() << DgSqlCode(-3242)
                            << DgString0("Cannot getTableDefforBinlog.");
        processReturn();
        return;
    }

    oldCols = &defStringArray->at(0);
    oldKeys = &defStringArray->at(1);
    snprintf(fullColBuf, oldCols->len, "%s", oldCols->val);
    snprintf(fullKeyBuf, oldKeys->len, "%s", oldKeys->val);
    strcat(fullColBuf, binlogTableDefString);
    fullColBufString = fullColBuf;
    fullKeyBufString = fullKeyBuf;
    //update the meta for binlog_reader
    retcode = ehi->updateTableDefForBinlog(binlogTableName, fullColBufString , fullKeyBufString, ddlStartTs);
  }

  ActiveSchemaDB()->getNATableDB()->removeNATable
    (cn,
     ComQiScope::REMOVE_FROM_ALL_USERS, COM_BASE_TABLE_OBJECT,
     alterAddColNode->ddlXns(), FALSE);

  if (alterAddColNode->getAddConstraintPK())
    {
      // if table already has a primary key, return error.
      if ((naTable->getClusteringIndex()) && 
          (NOT naTable->getClusteringIndex()->hasOnlySyskey()))
        {
          *CmpCommon::diags()
            << DgSqlCode(-1256)
            << DgString0(extTableName);
          
          processReturn();
          
          return;
        }
    }
  
  if ((alterAddColNode->getAddConstraintPK()) OR
      (alterAddColNode->getAddConstraintCheckArray().entries() NEQ 0) OR
      (alterAddColNode->getAddConstraintUniqueArray().entries() NEQ 0) OR
      (alterAddColNode->getAddConstraintRIArray().entries() NEQ 0))
    {
      if (naTable->isHbaseMapTable())
      {
        *CmpCommon::diags() << DgSqlCode(-3242)
                            << DgString0("Cannot specify unique, referential or check constraint for an HBase mapped table.");
        
        processReturn();
        
        return;
      }
      NABoolean needRestoreIBAttr = naTable->incrBackupEnabled();
      if(needRestoreIBAttr == true) {
        naTable->setIncrBackupEnabled(FALSE);
        dropIBAttrForTable(naTable , &cliInterface);
      } 
      // find out the next unnamed constraint num
      Int32 constrNum = getNextUnnamedConstrNum(naTable);
      addConstraints(tableName, currCatAnsiName, currSchAnsiName,
                     alterAddColNode,
                     alterAddColNode->getAddConstraintPK(),
                     alterAddColNode->getAddConstraintUniqueArray(),
                     alterAddColNode->getAddConstraintRIArray(),
                     alterAddColNode->getAddConstraintCheckArray(),
                     constrNum, false, naTable->isPartitionV2Table());
        if(needRestoreIBAttr == true)  {
          naTable->setIncrBackupEnabled(TRUE);
          addIBAttrForTable(naTable, &cliInterface);
        }
      if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_))
        return;
    }

  // if sample table exist, add the same col in it.
  if(isSampleExist(catalogNamePart,
                   schemaNamePart,
                   objUID,
                   sampleTableName,
                   sampleTableUid))
  {
    char dbuf[sampleTableName.length() + pColDef->getColDefAsText().length() + 100];
    str_sprintf(dbuf, "alter table %s add column %s",
                      sampleTableName.data(),
                      pColDef->getColDefAsText().data());
    cliRC = cliInterface.executeImmediate(dbuf);
    if (cliRC < 0)
    {
      return;
    }
  }

  //if partition table, add the same col on every partition
  if (naTable->isPartitionV2Table())
  {
    NAString partitionTableName;
    NAPartitionArray pa = naTable->getNAPartitionArray();
    for(int i = 0; i < pa.entries(); i++)
    {
      partitionTableName = pa[i]->getPartitionEntityName();
      char buf[partitionTableName.length() + pColDef->getColDefAsText().length() + 100];
      str_sprintf(buf, "alter table %s add column %s",
                       partitionTableName.data(),
                       pColDef->getColDefAsText().data());
      cliRC = cliInterface.executeImmediate(buf);
      if (cliRC < 0)
      {
        return;
      }
    }
  }

  // Upgrade DDL lock to read/write
  if (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL))
    {
      retcode = ddlSetObjectEpoch(naTable, STMTHEAP, false /*readsAllowed*/, true /*continue*/);
      if (retcode < 0)
        {
          ddlResetObjectEpochs(TRUE, TRUE);

          // Release all DDL object locks
          LOGTRACE(CAT_SQL_LOCK, "Release all DDL object locks");
          CmpCommon::context()->releaseAllDDLObjectLocks();
          return;
        }
    }

  if (updateObjectRedefTime(&cliInterface, 
                            catalogNamePart, schemaNamePart, objectNamePart,
                            COM_BASE_TABLE_OBJECT_LIT, -1, objUID,
                            naTable->isStoredDesc(), naTable->storedStatsAvail()))
    {
      processReturn();

      return;
    }

  if (naTable->isStoredDesc() && (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)))
    {
      insertIntoSharedCacheList(catalogNamePart, schemaNamePart, objectNamePart,
                                COM_BASE_TABLE_OBJECT, SharedCacheDDLInfo::DDL_INSERT);
    }

 label_return:
  processReturn();

  return;
}

short CmpSeabaseDDL::dropSample(const NAString &catalogNamePart,
                                const NAString &schemaNamePart,
                                const Int64 tableUID,
                                const NAString &sampleTableName)
{
  char dbuf[4000];
  Lng32 cliRC = 0;
  ExeCliInterface cliInterface(STMTHEAP, 0, NULL,
                               CmpCommon::context()->sqlSession()->getParentQid());

  str_sprintf(dbuf, "drop table %s", sampleTableName.data());
  cliRC = cliInterface.executeImmediate(dbuf);
  if (cliRC < 0)
  {
    cliRC = -1;
  }

  str_sprintf(dbuf, "delete from %s.\"%s\".%s where table_uid = %ld",
              catalogNamePart.data(),
              schemaNamePart.data(),
              HBASE_PERS_SAMP_NAME,
              tableUID);
  cliRC = cliInterface.executeImmediate(dbuf);
  if (cliRC < 0)
  {
    cliRC = -1;
  }

  return cliRC;
}

NABoolean CmpSeabaseDDL::isSampleExist(const NAString &catName,
                                       const NAString &schName,
                                       const Int64 tableUID,
                                       NAString &sampleTableName,
                                       Int64 &sampleTableUid)
{
  char buf[catName.length() + schName.length() + 250];
  Lng32 cliRC = 0;
  Queue *triggerss = NULL;
  NAString matchClause;
  matchClause += "TRAF_SAMPLE%";
  sprintf(buf,"%ld",tableUID);
  matchClause += buf;
  matchClause += "%";

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL,
                               CmpCommon::context()->sqlSession()->getParentQid());

  str_sprintf(buf, "select OBJECT_NAME, OBJECT_UID from "
                   " %s.\"%s\".%s where OBJECT_NAME like '%s'",
              catName.data(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS, matchClause.data());

  cliRC = cliInterface.fetchAllRows(triggerss, buf, 0, FALSE, FALSE, TRUE);

  if (cliRC < 0)
  {
    return FALSE;
  }
  else if ((triggerss->numEntries()) > 0) // if we got a sample table name back
  {
    OutputInfo *trigger = (OutputInfo *)triggerss->getNext();
    char *objname = trigger->get(0);
    Int64 sampleUid = *(Int64*)trigger->get(1);
    sampleTableName = objname;
    sampleTableUid = sampleUid;
    return TRUE;
  }
  return FALSE;
}

short CmpSeabaseDDL::updateMDforDropCol(ExeCliInterface &cliInterface,
                                        const NATable * naTable,
                                        Lng32 dropColNum,
                                        const NAType *type)
{
  Lng32 cliRC = 0;

  Int64 objUID = naTable->objectUid().castToInt64();

  char buf[4000];
  str_sprintf(buf, "delete from %s.\"%s\".%s where object_uid = %ld and column_number = %d",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_COLUMNS,
              objUID,
              dropColNum);
  
  cliRC = cliInterface.executeImmediate(buf);
  if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      return -1;
    }
  
  str_sprintf(buf, "update %s.\"%s\".%s set column_number = column_number - 1 where object_uid = %ld and column_number >= %d",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_COLUMNS,
              objUID,
              dropColNum);
  
  cliRC = cliInterface.executeImmediate(buf);
  if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      return -1;
    }
  
  str_sprintf(buf, "update %s.\"%s\".%s set column_number = column_number - 1 where object_uid = %ld and column_number >= %d",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_KEYS,
              objUID,
              dropColNum);
  
  cliRC = cliInterface.executeImmediate(buf);
  if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      return -1;
    }

  //delete comment in TEXT table for column
  str_sprintf(buf, "delete from %s.\"%s\".%s where text_uid = %ld and text_type = %d and sub_id = %d ",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_TEXT,
              objUID,
              COM_COLUMN_COMMENT_TEXT,
              dropColNum);
  cliRC = cliInterface.executeImmediate(buf);
  if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      return -1;
    }

  str_sprintf(buf, "update %s.\"%s\".%s set sub_id = sub_id - 1 where text_uid = %ld and ( text_type = %d or text_type = %d ) and sub_id > %d",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_TEXT,
              objUID,
              COM_COMPUTED_COL_TEXT,
              COM_COLUMN_COMMENT_TEXT,
              dropColNum);
  
  cliRC = cliInterface.executeImmediate(buf);
  if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      return -1;
    }
  
  // keys for pkey constraint refer to base table column number.
  // modify it so they now refer to new column numbers.
  str_sprintf(buf, "update %s.\"%s\".%s K set column_number = column_number - 1  where K.column_number >= %d and K.object_uid = (select C.constraint_uid from %s.\"%s\".%s C where C.table_uid = %ld and C.constraint_type = 'P')",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_KEYS,
              dropColNum,
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_TABLE_CONSTRAINTS,
              objUID);
  cliRC = cliInterface.executeImmediate(buf);
  if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      return -1;
    }
  
  // keys for indexes refer to base table column number.
  // modify it so they now refer to new column numbers.
  if (naTable->hasSecondaryIndexes())
    {
      const NAFileSetList &naFsList = naTable->getIndexList();
      
      for (Lng32 i = 0; i < naFsList.entries(); i++)
        {
          const NAFileSet * naFS = naFsList[i];
          
          // skip clustering index
          if (naFS->getKeytag() == 0)
            continue;
          
          const QualifiedName &indexName = naFS->getFileSetName();
          
          str_sprintf(buf, "update %s.\"%s\".%s set column_number = column_number - 1  where column_number >=  %d and object_uid = (select object_uid from %s.\"%s\".%s where catalog_name = '%s' and schema_name = '%s' and object_name = '%s' and object_type = 'IX') ",
                      getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_KEYS,
                      dropColNum,
                      getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS,
                      indexName.getCatalogName().data(),
                      indexName.getSchemaName().data(),
                      indexName.getObjectName().data());
          cliRC = cliInterface.executeImmediate(buf);
          if (cliRC < 0)
            {
              cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
              return -1;
            }
          
        } // for
    } // secondary indexes present

  // if the column being dropped is a composite column, remove it from
  // the text table comp col defn str.
  if (type->isComposite())
    {
      char buf[100];
      Int32 numCompCols = 0;
      const NAColumnArray &nacolArr = naTable->getNAColumnArray();
      NAString textTableCompDefnStr;
      for (Int32 i = 0; i < nacolArr.entries(); i++)
        {
          const NAColumn *nacol = nacolArr[i];

          if ((nacol->getType()->isComposite()) &&
              (nacol->getPosition() != dropColNum))
            {
              const CompositeType *compType = (CompositeType*)nacol->getType();
              const NAString &compDefnStr = compType->getCompDefnStr();
              if (! compDefnStr.isNull())
                {
                  numCompCols++;
                  snprintf(buf, sizeof(buf), "%08d%08d",
                           (nacol->getPosition() >= dropColNum 
                            ? nacol->getPosition() - 1 : nacol->getPosition()),
                           (int)compDefnStr.length());
                  textTableCompDefnStr += buf;              
                  textTableCompDefnStr += compDefnStr;
                }
            } // if
        } // for

      cliRC = 0;
      if (numCompCols > 0)
        {
          snprintf(buf, sizeof(buf), "%08d", numCompCols);
          textTableCompDefnStr.prepend(buf);

          cliRC = updateTextTable(&cliInterface,
                                  objUID,
                                  COM_COMPOSITE_DEFN_TEXT,
                                  0,
                                  textTableCompDefnStr,
                                  NULL, -1,
                                  TRUE/*delete existing row*/);
 
        }
      else
        {
          cliRC = deleteFromTextTable(&cliInterface,
                                      objUID,
                                      COM_COMPOSITE_DEFN_TEXT,
                                      0);
        }
      if (cliRC < 0)
        {
          cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
          
          return -1;
        }      
      
    } // dropped col is composite

  // if table has V2 lob columns, update them in LOB_COLUMNS table
  if ((naTable->hasLobColumn()) && (naTable->lobV2()))
    {
      // if the col to be dropped is a lob col, delete it
      if (type->isLob())
        {
          str_sprintf(buf, "delete from %s.\"%s\".%s where table_uid = %ld and lobnum = %d",
                      getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_LOB_COLUMNS,
                      objUID,
                      dropColNum);  

          cliRC = cliInterface.executeImmediate(buf);
          if (cliRC < 0)
            {
              cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
              return -1;
            }                
        }
      
      // adjust lobnum in LOB_COLUMNS table.
      str_sprintf(buf, "update %s.\"%s\".%s set lobnum = lobnum - 1 where table_uid = %ld and lobnum >= %d",
                  getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_LOB_COLUMNS,
                  objUID,
                  dropColNum);
      
      cliRC = cliInterface.executeImmediate(buf);
      if (cliRC < 0)
        {
          cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
          return -1;
        }
      
    } // has lob columns

  return 0;
}

///////////////////////////////////////////////////////////////////////
//
// An aligned table contains all columns in one hbase cell.
// To drop a column, we need to read each row, create a
// new row with the removed column and insert into the original table.
//
// Steps to drop a column from an aligned table:
//
// -- make a copy of the source aligned table using hbase copy
// -- truncate the source table
// -- Update metadata and remove the dropped column.
// -- bulk load data from copied table into the source table
// -- drop the copied temp table
//
// If an error happens after the source table has been truncated, then
// it will be restored from the copied table.
//
///////////////////////////////////////////////////////////////////////
short CmpSeabaseDDL::alignedFormatTableDropColumn
(
 const NAString &catalogNamePart,
 const NAString &schemaNamePart,
 const NAString &objectNamePart,
 const NATable * naTable,
 const NAString &dropColName,
 ElemDDLColDef *pColDef,
 NABoolean ddlXns,
 NAList<NAString> &viewNameList,
 NAList<NAString> &viewDefnList)
{
  Lng32 cliRC = 0;
  Lng32 cliRC2 = 0;

  const NAFileSet * naf = naTable->getClusteringIndex();
  
  CorrName cn(objectNamePart, STMTHEAP, schemaNamePart, catalogNamePart);

  ExpHbaseInterface * ehi = allocEHI(naTable->storageType());
  if (ehi == NULL)
     return -1;
  ExeCliInterface cliInterface
    (STMTHEAP, 0, NULL, 
     CmpCommon::context()->sqlSession()->getParentQid());

  const NAColumnArray &naColArr = naTable->getNAColumnArray();
  const NAColumn * altNaCol = naColArr.getColumn(dropColName);
  Lng32 dropColNum = altNaCol->getPosition();

  NAString upsertBuf;
  NAString tgtCols;
  NAString srcCols;

  NABoolean xnWasStartedHere = FALSE;

  char buf[4000];
  
  // Remove object from shared cache before making changes.
  // TDB - instead of removing, disable it
  if (naTable->isStoredDesc())
    {
      int32_t diagsMark = CmpCommon::diags()->mark();
      deleteFromSharedCache (&cliInterface, 
      // Remove entry from shared cache

                             naTable->getTableName().getQualifiedNameAsAnsiString(), 
                             COM_BASE_TABLE_OBJECT,
                             "drop column");
      
      // If unable to remove from shared cache, it may not exist, continue 
      // If the operation is later rolled back, this table will not be added
      // back to shared cache - TBD
      CmpCommon::diags()->rewind(diagsMark);
    }

  NAString clonedTable;

  // save data by cloning as a temp table and truncate source table
  cliRC = cloneAndTruncateTable(naTable, clonedTable, ehi, &cliInterface);
  if (cliRC < 0)
    {
      goto label_drop; // diags already populated by called method
    }

  if (beginXnIfNotInProgress(&cliInterface, xnWasStartedHere,
                             FALSE /*clearDDLList*/, FALSE /*clearObjectLocks*/))
    {
      cliRC = -1;
      goto label_restore;
    }

  // add X lock
  if (lockRequired(naTable, catalogNamePart, schemaNamePart, objectNamePart, HBaseLockMode::LOCK_X, FALSE/* useHbaseXN */, FALSE/* replSync */, FALSE/* incrementalBackup */, FALSE/* asyncOperation */, TRUE/* noConflictCheck */, TRUE/* registerRegion */))
  {
    cliRC = -1;
    goto label_restore;
  }

  // remove hist stats for the column being dropped.
  // Dont to it for lob cols as upd stats is not support on them.
  if (NOT altNaCol->getType()->isLob())
    {
      str_sprintf(buf, "update statistics for table %s on \"%s\" clear",
                  naTable->getTableName().getQualifiedNameAsAnsiString().data(),
                  dropColName.data());
      cliRC = cliInterface.executeImmediate(buf);
      if (cliRC < 0)
        {
          cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
          goto label_restore;
        }
    }

  // update sb_histogram table and adjust column number for columns
  // that come after the dropped column. If it is the last column,
  // skip this step.
  if ((dropColNum != -1) && (dropColNum < (naColArr.entries() - 1)))
    {
      str_sprintf(buf, "update %s.\"%s\".%s set column_number = column_number - 1 where table_uid = %ld and column_number >= %d",
                  naTable->getTableName().getCatalogName().data(),
                  naTable->getTableName().getSchemaName().data(),
                  HBASE_HIST_NAME,
                  naTable->objectUid().castToInt64(),
                  dropColNum);
      
      cliRC = cliInterface.executeImmediate(buf);
      if (cliRC < 0)
        {
          cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
          goto label_restore;
        }
    }

  if (updateMDforDropCol(cliInterface, naTable, dropColNum, altNaCol->getType()))
    {
      cliRC = -1;
      goto label_restore;
    }

  ActiveSchemaDB()->getNATableDB()->removeNATable
    (cn,
     ComQiScope::REMOVE_FROM_ALL_USERS, 
     COM_BASE_TABLE_OBJECT, ddlXns, FALSE);

  for (Int32 c = 0; c < naColArr.entries(); c++)
    {
      const NAColumn * nac = naColArr[c];
      if (nac->getColName() == dropColName)
        continue;

      if (nac->isComputedColumn())
        continue;

      // for mantis 21128
      //if (nac->isSystemColumn())
      //  continue;

      tgtCols += "\"" + nac->getColName() + "\"";
      tgtCols += ",";
    } // for

  tgtCols = tgtCols.strip(NAString::trailing, ',');

  if (tgtCols.isNull())
    {
      *CmpCommon::diags() << DgSqlCode(-1424)
                          << DgColumnName(dropColName);

      cliRC = -1;
      goto label_restore;
    }

  if (naTable->hasSecondaryIndexes()) // user indexes
    {
      cliRC = cliInterface.holdAndSetCQD("hide_indexes", "ALL");
      if (cliRC < 0)
        {
          cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
          goto label_restore;
        }
    }

  cliRC = cliInterface.holdAndSetCQD("override_generated_identity_values", "ON");
  if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      goto label_restore;
    }

  cliRC = cliInterface.holdAndSetCQD("attempt_esp_parallelism", "ON");
  if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      goto label_restore;
    }

  upsertBuf = "upsert using load into ";
  upsertBuf += naTable->getTableName().getQualifiedNameAsAnsiString();
  upsertBuf += NAString("(") + tgtCols + NAString(") select ");
  upsertBuf += tgtCols + NAString(" from ") + clonedTable;

  cliRC = cliInterface.executeImmediate(upsertBuf.data());
  if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      goto label_restore;
    }

  if ((cliRC = recreateUsingViews(&cliInterface, viewNameList, viewDefnList,
                                  ddlXns)) < 0)
    {
      NAString reason = "Error occurred while recreating views due to dependency on older column definition. Drop dependent views before doing the drop.";
      *CmpCommon::diags() << DgSqlCode(-1404)
                          << DgColumnName(dropColName)
                          << DgString0("dropped")
                          << DgString1(reason);
      goto label_restore;
    }

  endXnIfStartedHere(&cliInterface, xnWasStartedHere, 0,
                     FALSE /*modifyEpochs*/, FALSE /*clearObjectLocks*/);

  goto label_drop;

label_restore:
  endXnIfStartedHere(&cliInterface, xnWasStartedHere, -1,
                     FALSE /*modifyEpochs*/, FALSE /*clearObjectLocks*/);

  ActiveSchemaDB()->getNATableDB()->removeNATable
    (cn,
     ComQiScope::REMOVE_FROM_ALL_USERS, 
     COM_BASE_TABLE_OBJECT, FALSE, FALSE);
  
  if ((cliRC < 0) &&
      (NOT clonedTable.isNull()) &&
      (cloneSeabaseTable(clonedTable, -1,
                         naTable->getTableName().getQualifiedNameAsAnsiString(),
                         naTable,
                         ehi, &cliInterface, FALSE)))
    {
      cliRC = -1;
      goto label_drop;
    }
 
label_drop:  
  if (NOT clonedTable.isNull())
    {
      str_sprintf(buf, "drop table %s", clonedTable.data());
      cliRC2 = cliInterface.executeImmediate(buf);
    }
  cliInterface.restoreCQD("override_generated_identity_values");
  
  cliInterface.restoreCQD("hide_indexes");

  cliInterface.restoreCQD("attempt_esp_parallelism");

  ActiveSchemaDB()->getNATableDB()->removeNATable
    (cn,
     ComQiScope::REMOVE_FROM_ALL_USERS, 
     COM_BASE_TABLE_OBJECT, ddlXns, FALSE);

  deallocEHI(ehi); 
  
  return (cliRC < 0 ? -1 : 0);  
}

short CmpSeabaseDDL::hbaseFormatTableDropColumn(
     ExpHbaseInterface *ehi,
     const NAString &catalogNamePart,
     const NAString &schemaNamePart,
     const NAString &objectNamePart,
     const NATable * naTable,
     const NAString &dropColName,
     const NAColumn * nacol,
     NABoolean ddlXns,
     NAList<NAString> &viewNameList,
     NAList<NAString> &viewDefnList)
{
  Lng32 cliRC = 0;

  Int64 objDataUID = (naTable ? naTable->objDataUID().castToInt64() : 0);

  const NAString extNameForHbase = genHBaseObjName
    (catalogNamePart, schemaNamePart, objectNamePart, 
     (naTable ? ((NATable *)naTable)->getNamespace() : NAString("")), objDataUID);

  ExeCliInterface cliInterface(
       STMTHEAP, 0, NULL, 
       CmpCommon::context()->sqlSession()->getParentQid());

  Lng32 dropColNum = nacol->getPosition();

  const NAColumnArray &naColArr = naTable->getNAColumnArray();

  char buf[4000];

  NAString objectName;

  NABoolean xnWasStartedHere = FALSE;
  if (beginXnIfNotInProgress(&cliInterface, xnWasStartedHere,
                             FALSE /*clearDDLList*/, FALSE /*clearObjectLocks*/))
    {
      cliRC = -1;
      goto label_error;
    }
  
  // Map hbase map table to external name
  if (ComIsHBaseMappedIntFormat(catalogNamePart, schemaNamePart))
    {
      NAString newCatName;
      NAString newSchName;
      ComConvertHBaseMappedIntToExt(catalogNamePart, schemaNamePart,
                                    newCatName, newSchName);
      objectName =  newCatName + ".\"" + newSchName + "\".\"" + objectNamePart + "\"";
    }
  else
    objectName = naTable->getTableName().getQualifiedNameAsAnsiString();

  // remove hist stats for the column being dropped
  str_sprintf(buf, "update statistics for table %s on \"%s\" clear",
              //naTable->getTableName().getQualifiedNameAsAnsiString().data(),
              objectName.data(),
              dropColName.data());
  cliRC = cliInterface.executeImmediate(buf);
  if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      goto label_error;
    }

  // update sb_histogram table and adjust column number for columns
  // that come after the dropped column. If it is the last column,
  // skip this step.
  if ((dropColNum != -1) && (dropColNum < (naColArr.entries() - 1)))
    {
      str_sprintf(buf, "update %s.\"%s\".%s set column_number = column_number - 1 where table_uid = %ld and column_number >= %d",
                  naTable->getTableName().getCatalogName().data(),
                  naTable->getTableName().getSchemaName().data(),
                  HBASE_HIST_NAME,
                  naTable->objectUid().castToInt64(),
                  dropColNum);
      
      cliRC = cliInterface.executeImmediate(buf);
      if (cliRC < 0)
        {
          cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
          goto label_error;
        }
    }

  if (updateMDforDropCol(cliInterface, naTable, dropColNum, nacol->getType()))
    {
      cliRC = -1;
      goto label_error;
    }
  
  // remove column from all rows of the base table
  HbaseStr hbaseTable;
  hbaseTable.val = (char*)extNameForHbase.data();
  hbaseTable.len = extNameForHbase.length();
  
  {
    NAString column(nacol->getHbaseColFam(), heap_);
    column.append(":");
   
    if (naTable->isHbaseMapTable())
      {
        column.append(dropColName);
      }
    else
      {
        char * colQualPtr = (char*)nacol->getHbaseColQual().data();
        Lng32 colQualLen = nacol->getHbaseColQual().length();
        Int64 colQval = str_atoi(colQualPtr, colQualLen);
        if (colQval <= UCHAR_MAX)
          {
            unsigned char c = (unsigned char)colQval;
            column.append((char*)&c, 1);
          }
        else if (colQval <= USHRT_MAX)
          {
            unsigned short s = (unsigned short)colQval;
            column.append((char*)&s, 2);
          }
        else if (colQval <= ULONG_MAX)
          {
            Lng32 l = (Lng32)colQval;
            column.append((char*)&l, 4);
          }
        else
          column.append((char*)&colQval, 8);
      }
        
    HbaseStr colNameStr;
    char * col = (char *) heap_->allocateMemory(column.length() + 1, FALSE);
    if (col)
      {
        memcpy(col, column.data(), column.length());
        col[column.length()] = 0;
        colNameStr.val = col;
        colNameStr.len = column.length();
      }
    else
      {
        cliRC = -EXE_NO_MEM_TO_EXEC;
        *CmpCommon::diags() << DgSqlCode(-EXE_NO_MEM_TO_EXEC);  // error -8571
        
        goto label_error;
      }
    
    cliRC = ehi->deleteColumns(hbaseTable, colNameStr);
    if (cliRC < 0)
      {
        *CmpCommon::diags() << DgSqlCode(-8448)
                            << DgString0((char*)"ExpHbaseInterface::deleteColumns()")
                            << DgString1(getHbaseErrStr(-cliRC))
                            << DgInt0(-cliRC)
                            << DgString2((char*)GetCliGlobals()->getJniErrorStr());
        
        goto label_error;
      }
  }
  
  if ((cliRC = recreateUsingViews(&cliInterface, viewNameList, viewDefnList,
                                  ddlXns)) < 0)
    {
      NAString reason = "Error occurred while recreating views due to dependency on older column definition. Drop dependent views before doing the alter.";
      *CmpCommon::diags() << DgSqlCode(-1404)
                          << DgColumnName(dropColName)
                          << DgString0("dropped")
                          << DgString1(reason);
      goto label_error;
    }

  endXnIfStartedHere(&cliInterface, xnWasStartedHere, 0,
                     FALSE /*modifyEpochs*/, FALSE /*clearObjectLocks*/);
  
  return 0;

 label_error:
  endXnIfStartedHere(&cliInterface, xnWasStartedHere, -1,
                     FALSE /*modifyEpochs*/, FALSE /*clearObjectLocks*/);

  return -1;
}

static void binlogRemoveColumnString(int colNum, NAString oldString, NAString & newString)
{
  char newBuf[50* 1000];
  memset(newBuf, 0 , 50*1000);
  int pos = 0, idx = 0;
  int notcopy = 0;

  for( int i = 0 ; i < oldString.length() ; i++ ) {
    if(oldString[i] == ';' ){
      pos++;
      if(pos == colNum) //this is the part to remove
        notcopy = 1;
      else
        notcopy = 0;
    }
    if(notcopy == 0)
    {
      newBuf[idx] = oldString[i];
      idx++;
    }
  }
  newString = newBuf;
}

// ----------------------------------------------------------------------------
// alterSeabaseTableDropColumn
//
//    ALTER TABLE <table> DROP COLUMN <col>
//
// This statement is not run in a transaction by default.
// It cannot be run in a user transaction
// This statement may recreate the table, so it employs a read and read/write
// DDL lock.
// ----------------------------------------------------------------------------
void CmpSeabaseDDL::alterSeabaseTableDropColumn(
                                                StmtDDLAlterTableDropColumn * alterDropColNode,
                                                NAString &currCatName, NAString &currSchName)
{
  Lng32 cliRC = 0;
  Lng32 retcode = 0;

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, 
                               CmpCommon::context()->sqlSession()->getParentQid());

  NAString tabName = alterDropColNode->getTableName();
  NAString catalogNamePart, schemaNamePart, objectNamePart;
  NAString extTableName, extNameForHbase, binlogTableName;
  NAString sampleTableName;
  Int64 sampleTableUid = 0;
  NATable * naTable = NULL;
  CorrName cn;
  NAString nochangString;
  NAString oldTableDefString, fullTableDefString, fullKeyDefString;
  NAString fullColBufString, fullKeyBufString;
  NAArray<HbaseStr> *defStringArray = NULL;
  HbaseStr *oldCols = NULL;
  HbaseStr *oldKeys = NULL;
  char fullColBuf[1024*20];
  char fullKeyBuf[128*20];

  NABoolean needRestoreIBAttr = FALSE;

  Int64 ddlStartTs = 0;

  memset(fullColBuf, 0 , 1024*20 );
  memset(fullKeyBuf, 0 , 128*20 );

  retcode = lockObjectDDL(alterDropColNode);
  if (retcode < 0) {
    processReturn();
    return;
  }

  retcode = 
    setupAndErrorChecks(tabName, 
                        alterDropColNode->getOrigTableNameAsQualifiedName(),
                        currCatName, currSchName,
                        catalogNamePart, schemaNamePart, objectNamePart,
                        extTableName, extNameForHbase, cn,
                        &naTable, 
                        FALSE, TRUE,
                        &cliInterface,
                        COM_BASE_TABLE_OBJECT,
                        SQLOperation::ALTER_TABLE,
                        FALSE,
                        TRUE/*process hiatus*/);
  if (retcode < 0)
    {
      processReturn();
      return;
    }

  struct timeval tv;
  gettimeofday(&tv, NULL);
  ddlStartTs = tv.tv_sec * 1000 + tv.tv_usec / 1000;
  binlogTableName = schemaNamePart + "." + objectNamePart;

  const NAColumnArray &nacolArr = naTable->getNAColumnArray();
  const NAString &colName = alterDropColNode->getColName();

  const NAColumn * nacol = nacolArr.getColumn(colName);

  if (! nacol)
    {
      // column doesn't exist. Error or return, depending on 'if exists' option.
      if (NOT alterDropColNode->dropIfExists())
        {
          *CmpCommon::diags() << DgSqlCode(-CAT_COLUMN_DOES_NOT_EXIST_ERROR)
                              << DgColumnName(colName);
        }

      processReturn();

      return;
    }

  // If column is a V1 LOB column, return error
  Int32 datatype = nacol->getType()->getFSDatatype();
  if (((datatype == REC_BLOB) || (datatype == REC_CLOB)) &&
      (NOT naTable->lobV2()))
    {
      *CmpCommon::diags() << DgSqlCode(-CAT_LOB_COLUMN_ALTER)
                          << DgColumnName(colName);
      processReturn();
      return;
    }
  
  const NAFileSet * naFS = naTable->getClusteringIndex();
  const NAColumnArray &naKeyColArr = naFS->getIndexKeyColumns();
  if (naKeyColArr.getColumn(colName))
    {
      // key column cannot be dropped
      *CmpCommon::diags() << DgSqlCode(-1420)
                          << DgColumnName(colName);

      processReturn();

      return;
    }

  if ((naTable->getUniqueConstraints().entries() > 0) || // unique constraints
      (naTable->getRefConstraints().entries() > 0) ||  // ref constraints
      (naTable->getCheckConstraints().entries() > 0) ) //check constraints
    {
      // check if there are any user defined non-pkey unique constraints
      NABoolean userDefinedUniqueConstr = FALSE;
      if (naTable->getUniqueConstraints().entries() > 0)
        {
          const AbstractRIConstraintList &uniqueList = 
            naTable->getUniqueConstraints();
          
          for (Int32 i = 0; i < uniqueList.entries(); i++)
            {
              AbstractRIConstraint *ariConstr = uniqueList[i];
              if (ariConstr->getOperatorType() != ITM_UNIQUE_CONSTRAINT)
                continue;
              
              UniqueConstraint * uniqConstr = (UniqueConstraint*)ariConstr;
              if (uniqConstr->isPrimaryKeyConstraint())
                continue;

              userDefinedUniqueConstr = TRUE;
            }
        }

      NABoolean userDefinedRefConstr = FALSE;
      if (naTable->getRefConstraints().entries() > 0)
      {
        const AbstractRIConstraintList& ariList = naTable->getRefConstraints();
        for (Int32 i = 0; i < ariList.entries(); i++)
        {
          AbstractRIConstraint* ariConstr = ariList[i];
          if (ariConstr->getOperatorType() != ITM_REF_CONSTRAINT)
            continue;

          RefConstraint* refConstr = (RefConstraint*)ariConstr;
          LIST(Lng32) myCols(NULL);
          refConstr->getMyKeyColumns(myCols);
          for (CollIndex currentCol = 0; currentCol < myCols.entries(); currentCol++)
          {
            if (nacol->getPosition() == myCols[currentCol])
            {
              userDefinedRefConstr = TRUE;
              break;
            }
          }
          if (userDefinedRefConstr)
            break;
        }
      }
      
      if ((userDefinedUniqueConstr) ||
          (userDefinedRefConstr) ||
          (naTable->getCheckConstraints().entries() > 0))
        {
          *CmpCommon::diags() << DgSqlCode(-1425)
                              << DgTableName(extTableName)
                              << DgString0("Reason: Cannot drop a column on a table with check, unique or referential constraints. Drop those constraints before dropping the column and then recreate them afterwards. Use SHOWDDL to find out the unique and referential constraints.");

          processReturn();
          return;
        }
    }
  
  if (naTable->hasSecondaryIndexes())
    {
      const NAFileSetList &naFsList = naTable->getIndexList();

      for (Lng32 i = 0; i < naFsList.entries(); i++)
        {
          naFS = naFsList[i];
          
          // skip clustering index
          if (naFS->getKeytag() == 0)
            continue;

          const NAColumnArray &naIndexColArr = naFS->getAllColumns();
          if (naIndexColArr.getColumn(colName))
            {
              // secondary index column cannot be dropped
              *CmpCommon::diags() << DgSqlCode(-1421)
                                  << DgColumnName(colName)
                                  << DgTableName(naFS->getExtFileSetName());

              processReturn();

              return;
            }
        } // for
    } // secondary indexes present

  if ((naTable->getClusteringIndex()->hasSyskey()) &&
      (nacolArr.entries() == 2))
    {
      // this table has one SYSKEY column and one other column.
      // Dropping that column will leave the table with no user column.
      // Return an error.
      *CmpCommon::diags() << DgSqlCode(-1424)
                          << DgColumnName(colName);

      processReturn();
      return;
    }

  Int64 objUID = naTable->objectUid().castToInt64();

  if (CmpCommon::getDefault(MODE_COMPATIBLE_1) != DF_ON)
    {
      Queue * usingViewsQueue = NULL;
      cliRC = getUsingViews(&cliInterface, objUID, colName, usingViewsQueue);
      if (cliRC < 0)
        {
          processReturn();
          return;
        }
      if (usingViewsQueue->numEntries() != 0)
        {
          NAString viewNames("");
          usingViewsQueue->position();
          for (int idx = 0; idx < usingViewsQueue->numEntries(); idx++)
            {
              OutputInfo * vi = (OutputInfo*)usingViewsQueue->getNext(); 
              char * viewName = vi->get(0);
              viewNames += viewName;
              if (idx != usingViewsQueue->numEntries() -1)
                viewNames += ',';
            }
          NAString reason("Reason: ");
          if (usingViewsQueue->numEntries() == 1)
            {
              reason += "View ";
              reason += viewNames;
              reason += " depends on it. Drop dependent view before doing the drop.";
            }
          else
            {
              reason += "Views ";
              reason += viewNames;
              reason += " depend on it. Drop dependent views before doing the drop.";
            }
          *CmpCommon::diags() << DgSqlCode(-1404)
                              << DgColumnName(colName)
                              << DgString0("dropped")
                              << DgString1(reason);
          return;
        }
    }

  // this operation cannot be done if a xn is already in progress.
  if (xnInProgress(&cliInterface) || (!GetCliGlobals()->currContext()->getTransaction()->autoCommit()))
    {
      *CmpCommon::diags() << DgSqlCode(-20125)
                          << DgString0("This ALTER");
      
      processReturn();
      return;
    }

  NABoolean xnWasStartedHere = FALSE;
  ExpHbaseInterface * ehi = NULL;

  // There is no transaction active.  saveAnDropUsingViews run in its
  // own transaction and should be ended before returning.  This
  // request does not touch the base table.
  NAList<NAString> viewNameList(STMTHEAP);
  NAList<NAString> viewDefnList(STMTHEAP);

  if (CmpCommon::getDefault(MODE_COMPATIBLE_1) != DF_ON)
    {
      if (saveAndDropUsingViews(objUID, &cliInterface, viewNameList, viewDefnList))
        {
          NAString reason = "Error occurred while saving views.";
          *CmpCommon::diags() << DgSqlCode(-1404)
                              << DgColumnName(colName)
                              << DgString0("dropped")
                              << DgString1(reason);

          processReturn();

          return;
        }
    }

  // Table exists, update the epoch value to lock out other user access
  if (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL))
    {
      retcode = ddlSetObjectEpoch(naTable, STMTHEAP);
      if (retcode < 0)
        {
          processReturn();
          return;
        }
    }

  needRestoreIBAttr = naTable->incrBackupEnabled();
  if(needRestoreIBAttr == true && NOT naTable->isPartitionV2Table()) {
     naTable->setIncrBackupEnabled(FALSE);
     dropIBAttrForTable(naTable , &cliInterface);
  }

  CmpCommon::context()->getCliContext()->execDDLOptions() = TRUE;
  Lng32 colNumber = nacol->getPosition();
  char *col = NULL;
  //if partition table skip this step
  if (NOT naTable->isPartitionV2Table())
  {
    if (naTable->isSQLMXAlignedTable())
      {
        if (alignedFormatTableDropColumn
            (
             catalogNamePart, schemaNamePart, objectNamePart,
             naTable,
             alterDropColNode->getColName(),
             NULL, alterDropColNode->ddlXns(),
             viewNameList, viewDefnList))
          {
            cliRC = -1;
            goto label_error;
          }
       }
    else
      {
        ehi = allocEHI(naTable->storageType());
        if (hbaseFormatTableDropColumn
            (
                 ehi,
                 catalogNamePart, schemaNamePart, objectNamePart,
                 naTable,
                 alterDropColNode->getColName(),
                 nacol, alterDropColNode->ddlXns(),
                 viewNameList, viewDefnList))
          {
            cliRC = -1;
            goto label_error;
          }
      } // hbase format table
  }

  //restore the flag before using it
  if(needRestoreIBAttr == true && NOT naTable->isPartitionV2Table())  {
    naTable->setIncrBackupEnabled(TRUE);
    addIBAttrForTable(naTable, &cliInterface);
  }

  if (naTable->incrBackupEnabled())
  {
    cliRC = updateSeabaseXDCDDLTable(&cliInterface,
                                     catalogNamePart.data(), schemaNamePart.data(),
                                     objectNamePart.data(), COM_BASE_TABLE_OBJECT_LIT);
    if (cliRC < 0)
    {
      cliRC = -1;
      goto label_error;
    }
  }
  //remove string at colNumber
  //first , get the current string
  if (!ComIsTrafodionReservedSchemaName(schemaNamePart) && naTable->isSeabaseTable() == TRUE) {
    if(ehi == NULL)
      ehi = allocEHI(naTable->storageType());
    retcode = ehi->getTableDefForBinlog(binlogTableName, &defStringArray);
    if (retcode < 0 )
    {
        *CmpCommon::diags() << DgSqlCode(-3242)
                            << DgString0("Cannot getTableDefforBinlog.");
        processReturn();
        return;
    }

    oldCols = &defStringArray->at(0);
    oldKeys = &defStringArray->at(1);
    snprintf(fullColBuf, oldCols->len, "%s", oldCols->val);
    snprintf(fullKeyBuf, oldKeys->len, "%s", oldKeys->val);
    fullTableDefString = fullColBuf;
    fullKeyBufString = fullKeyBuf;

    binlogRemoveColumnString( colNumber, fullTableDefString, fullColBufString);

    retcode = ehi->updateTableDefForBinlog(binlogTableName, fullColBufString , fullKeyBufString, ddlStartTs);
    if (retcode < 0 )
    {
        *CmpCommon::diags() << DgSqlCode(-3242)
                            << DgString0("Cannot updateTableDefForBinlog.");
        processReturn();
        return;
    }
  }
  // if sample table exist, drop it and clean the sample infomation in SB_PERSISTENT_SAMPLES
  // after you drop a column, and send a warn
  if(isSampleExist(catalogNamePart,
                   schemaNamePart,
                   objUID,
                   sampleTableName,
                   sampleTableUid))
  {
     cliRC = dropSample(catalogNamePart,
                        schemaNamePart,
                        objUID,
                        sampleTableName);
     if (cliRC < 0)
     {
        goto label_error;
     }
    // Warning: recreate sample table
    if(CmpCommon::getDefault(SHOWWARN_OPT) == DF_ON)
      *CmpCommon::diags() << DgSqlCode(1718);
  }

  //if partition table, drop the same col on every partition
  if (naTable->isPartitionV2Table())
  {
    //cannot drop partitioning column
    NAString pColName;
    naTable->getParitionColNameAsString(pColName, FALSE);
    if (pColName.contains(alterDropColNode->getColName().data()))
    {
      *CmpCommon::diags() << DgSqlCode(-1425)
                          << DgTableName(extTableName)
                          << DgString0("Reason: cannot drop partitioning column");
      processReturn();
      return;
    }

    CmpSeabaseDDL cmpSBD(STMTHEAP);
    if (cmpSBD.switchCompiler())
    {
      goto label_error;
    }

    NAString partitionTableName;
    NAPartitionArray pa = naTable->getNAPartitionArray();
    for(int i = 0; i < pa.entries(); i++)
    {
      partitionTableName = pa[i]->getPartitionEntityName();
      char buf[partitionTableName.length() + alterDropColNode->getColName().length() + 100];
      str_sprintf(buf, "alter table %s drop column %s", partitionTableName.data(), alterDropColNode->getColName().data());
      cliRC = cliInterface.executeImmediate(buf);
      if (cliRC < 0)
      {
        cmpSBD.switchBackCompiler();
        goto label_error;
      }
    }
    cmpSBD.switchBackCompiler();
    //partition table add lock again
    retcode = lockObjectDDL(naTable);
    if (retcode < 0)
    {
      processReturn();
      return;
    }
  }

  if (beginXnIfNotInProgress(&cliInterface, xnWasStartedHere,
                             FALSE /*clearDDLList*/, FALSE /*clearObjectLocks*/))
    {
      goto label_error;
    }

  //if partition table, drop base table md
  if (naTable->isPartitionV2Table())
  {
    const NAColumnArray &naColArr = naTable->getNAColumnArray();
    const NAColumn * altNaCol = naColArr.getColumn(alterDropColNode->getColName());
    Lng32 dropColNum = altNaCol->getPosition();
    if (updateMDforDropCol(cliInterface, naTable, dropColNum, altNaCol->getType()))
    {
      goto label_error;
    }
  }

  cliRC = updateObjectRedefTime(&cliInterface,
                                catalogNamePart, schemaNamePart, objectNamePart,
                                COM_BASE_TABLE_OBJECT_LIT, -1, objUID,
                                naTable->isStoredDesc(), naTable->storedStatsAvail());
  if (cliRC < 0)
    {
      goto label_error;
    }

  if (naTable->isStoredDesc() && (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)))
    {
      insertIntoSharedCacheList(catalogNamePart, schemaNamePart, objectNamePart,
                                COM_BASE_TABLE_OBJECT, SharedCacheDDLInfo::DDL_INSERT);
      if (!xnInProgress(&cliInterface))
        finalizeSharedCache();
    }

 label_return:
  CmpCommon::context()->getCliContext()->execDDLOptions() = FALSE;
  endXnIfStartedHere(&cliInterface, xnWasStartedHere, cliRC,
                     FALSE /*modifyEpochs*/, FALSE /*clearObjectLocks*/);

  ActiveSchemaDB()->getNATableDB()->removeNATable
    (cn,
     ComQiScope::REMOVE_FROM_ALL_USERS, COM_BASE_TABLE_OBJECT,
     alterDropColNode->ddlXns(), FALSE);

  if (cliRC < 0)
    ddlResetObjectEpochs(TRUE/*doAbort*/, TRUE /*clearList*/);
  else
    ddlResetObjectEpochs(FALSE/*doAbort*/, TRUE /*clearList*/);

  // Release all DDL object locks
  LOGTRACE(CAT_SQL_LOCK, "Release all DDL object locks");
  CmpCommon::context()->releaseAllDDLObjectLocks();

  return;

 label_error:
  CmpCommon::context()->getCliContext()->execDDLOptions() = FALSE;
  endXnIfStartedHere(&cliInterface, xnWasStartedHere, cliRC,
                     FALSE /*modifyEpochs*/, FALSE /*clearObjectLocks*/);

  if ((cliRC = recreateUsingViews(&cliInterface, viewNameList, viewDefnList,
                                  alterDropColNode->ddlXns())) < 0)
    {
      NAString reason = "Error occurred while recreating views due to dependency on older column definition. Drop dependent views before doing the drop.";
      *CmpCommon::diags() << DgSqlCode(-1404)
                          << DgColumnName(colName)
                          << DgString0("dropped")
                          << DgString1(reason);
    }

  deallocEHI(ehi); 
  heap_->deallocateMemory(col);

  ActiveSchemaDB()->getNATableDB()->removeNATable
    (cn,
     ComQiScope::REMOVE_FROM_ALL_USERS, COM_BASE_TABLE_OBJECT,
     alterDropColNode->ddlXns(), FALSE);

  ddlResetObjectEpochs(TRUE/*doAbort*/, TRUE /*clearList*/);
  processReturn();

  // Release all DDL object locks
  LOGTRACE(CAT_SQL_LOCK, "Release all DDL object locks");
  CmpCommon::context()->releaseAllDDLObjectLocks();

  return;
}

// ----------------------------------------------------------------------------
// alterSeabaseTableDropPartition
//
//    ALTER TABLE <table> DROP PARTITION
//
// This statement runs in a transaction
// This statement may recreate the table, so it employs a read and read/write
// DDL lock.
// ----------------------------------------------------------------------------

void CmpSeabaseDDL::alterSeabaseTableDropPartition(
                                                   StmtDDLAlterTableDropPartition * alterDropPartition,
                                                   NAString &currCatName, NAString &currSchName)
{
  Lng32 cliRC = 0;
  Lng32 retcode = 0;
  
  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, 
                               CmpCommon::context()->sqlSession()->getParentQid());
  
  NAString tabName = alterDropPartition->getTableName();
  NAString objectName = alterDropPartition->getOrigTableNameAsQualifiedName().getObjectName();
  NABoolean isSubpartition = alterDropPartition->isSubpartition();
  NAString catalogNamePart, schemaNamePart, objectNamePart;
  NAString extTableName, extNameForHbase, binlogTableName;
  NATable * naTable = NULL;
  CorrName cn;
  Int64 btUid = 0;
  UInt32 op = 0;

  retcode = lockObjectDDL(alterDropPartition);
  if (retcode < 0) {
    processReturn();
    return;
  }

  retcode = 
    setupAndErrorChecks(tabName, 
                        alterDropPartition->getOrigTableNameAsQualifiedName(),
                        currCatName, currSchName,
                        catalogNamePart, schemaNamePart, objectNamePart,
                        extTableName, extNameForHbase, cn,
                        &naTable, 
                        FALSE, TRUE,
                        &cliInterface,
                        COM_BASE_TABLE_OBJECT,
                        SQLOperation::ALTER_TABLE,
                        FALSE,
                        TRUE/*process hiatus*/);
  if (retcode < 0)
    {
      processReturn();
      return;
    }

  if (!naTable->isPartitionV2Table())
  {
    // Object tableName is not partition table.
    *CmpCommon::diags() << DgSqlCode(-8304)
                        << DgString0(objectName);

    processReturn();
    return;
  }

  btUid = naTable->objectUid().castToInt64();
  UpdateObjRefTimeParam updObjRefParam(-1, btUid, naTable->isStoredDesc(), naTable->storedStatsAvail());
  RemoveNATableParam removeNATableParam(ComQiScope::REMOVE_FROM_ALL_USERS, alterDropPartition->ddlXns(), FALSE);

  setCacheAndStoredDescOp(op, UPDATE_REDEF_TIME);
  setCacheAndStoredDescOp(op, REMOVE_NATABLE);
  if (naTable->incrBackupEnabled())
    setCacheAndStoredDescOp(op, UPDATE_XDC_DDL_TABLE);
  if (naTable->isStoredDesc())
    setCacheAndStoredDescOp(op, UPDATE_SHARED_CACHE);

  BindWA bindWA(ActiveSchemaDB(), CmpCommon::context(), TRUE/*inDDL*/);
  TableDesc *tableDesc = NULL;
  tableDesc = bindWA.createTableDesc(naTable, cn, FALSE, NULL);
  if (!tableDesc)
  {
    // An internal executor error occurred.
    *CmpCommon::diags() << DgSqlCode(-8001);
    processReturn();
    return;
  }

  ElemDDLPartitionNameAndForValuesArray &partArray = alterDropPartition->getPartitionNameAndForValuesArray();

  // partition entity name
  NAString partEntityName;
  std::vector<NAString> dropPartEntityNameVec;
  // <partEntityName, partName>
  std::map<NAString, NAString> partEntityNameMap;
  NAString invalidPartName;
  Lng32 ret = checkPartition(&partArray, tableDesc, bindWA, objectName, invalidPartName,
                                      &dropPartEntityNameVec, &partEntityNameMap);
  if (ret == -1)
  {
    // An internal executor error occurred.
    *CmpCommon::diags() << DgSqlCode(-8001);
    processReturn();
    return;
  }
  else if (ret == 1)
  {
    // The DDL request has duplicate references to partition name.
    *CmpCommon::diags() << DgSqlCode(-8305)
                        << DgString0(invalidPartName);
    processReturn();
    return;
  }
  else if (ret == 2)
  {
    *CmpCommon::diags() << DgSqlCode(-8306)
                        << DgString0("The partition number is invalid or out-of-range");
    processReturn();
    return;
  }
  else if (ret == 3)
  {
    // The specified partition <partName> does not exist.
    NAString errMsg = "The specified partition ";
    errMsg += invalidPartName;
    errMsg += " does not exist";

    *CmpCommon::diags() << DgSqlCode(-8306) << DgString0(errMsg);
    processReturn();
    return;
  }

  Int32 firstLevelPartCnt = naTable->FisrtLevelPartitionCount();
  const NAPartitionArray& partitionArray = naTable->getPartitionArray();

  // all NAPartition which will be dropped
  std::vector<NAPartition *> dropNAPartitionVec;

  // record partEntityName and the next corresponding NAPartition
  // <partEntityName, nextNAPartition>
  std::map<NAString, NAPartition *> nextNAPartitionMap;

  if (!isSubpartition)
  {
    // record all partEntityName and corresponding NAPartition.
    // <firstpartEntityName, firstNAPartition>
    std::map<NAString, NAPartition *> allNAPartitionMap;
    std::map<NAString, NAPartition *>::iterator partInfoIt;

    for (Lng32 i = 0; i < firstLevelPartCnt; ++i)
    {
      partEntityName = partitionArray[i]->getPartitionEntityName();
      allNAPartitionMap[partEntityName] = partitionArray[i];

      if (firstLevelPartCnt - i == 1)
        nextNAPartitionMap[partEntityName] = NULL;
      else
        nextNAPartitionMap[partEntityName] = partitionArray[i + 1];
    }

    for (Lng32 i = 0; i < dropPartEntityNameVec.size(); ++i)
    {
      partInfoIt = allNAPartitionMap.find(dropPartEntityNameVec[i]);
      if (partInfoIt == allNAPartitionMap.end())
      {
        // The specified partition <partName> does not exist.
        NAString errMsg = "The specified partition ";
        errMsg += partEntityNameMap[dropPartEntityNameVec[i]];
        errMsg += " does not exist";

        *CmpCommon::diags() << DgSqlCode(-8306)
                            << DgString0(errMsg);
        processReturn();
        return;
      }

      dropNAPartitionVec.push_back(partInfoIt->second);
    }

    if (firstLevelPartCnt == 1)
    {
      // Dropping the only partition of an object is not allowed.
      *CmpCommon::diags() << DgSqlCode(-8306)
                          << DgString0("Dropping the only partition of an object is not allowed");
      processReturn();
      return;
    }

    if (firstLevelPartCnt - dropNAPartitionVec.size() < 1)
    {
      // Operation is not allowed because no partition exists after drop.
      *CmpCommon::diags() << DgSqlCode(-8306)
                          << DgString0("No partition exists after drop");
      processReturn();
      return;
    }
  }
  else // subpartition
  {
    // record all subpartEntityName and corresponding firstNAPartition subNAPartition.
    // <subpartEntityName, <firstNAPartition, subNAPartition> >
    std::map<NAString, std::pair<NAPartition *, NAPartition *> > allSubPartitionMap;
    std::map<NAString, std::pair<NAPartition *, NAPartition *> >::iterator subpartInfoIt;

    // record the count of subpart of firstpart after dropped
    // <firstpartEntityName, subpartCount>
    std::map<NAString, Lng32> subpartCntPerFirstpartMap;

    for (Lng32 i = 0; i < firstLevelPartCnt; ++i)
    {
      const NAPartitionArray *subPartInfo = partitionArray[i]->getSubPartitions();
      for (Lng32 j = 0; j < subPartInfo->entries(); ++j)
      {
        partEntityName = (*subPartInfo)[j]->getPartitionEntityName();
        allSubPartitionMap[partEntityName] = std::pair<NAPartition *, NAPartition *>(partitionArray[i], (*subPartInfo)[j]);

        if (subPartInfo->entries() - j == 1)
          nextNAPartitionMap[partEntityName] = NULL;
        else
          nextNAPartitionMap[partEntityName] = (*subPartInfo)[j + 1];
      }
    }

    for (Lng32 i = 0; i < dropPartEntityNameVec.size(); ++i)
    {
      subpartInfoIt = allSubPartitionMap.find(dropPartEntityNameVec[i]);
      if (subpartInfoIt == allSubPartitionMap.end())
      {
        // The specified partition <partName> does not exist.
        NAString errMsg = "The specified partition ";
        errMsg += partEntityNameMap[dropPartEntityNameVec[i]];
        errMsg += " does not exist";

        *CmpCommon::diags() << DgSqlCode(-8036)
                            << DgString0(errMsg);
        processReturn();
        return;
      }

      NAPartition * firstpartInfo = subpartInfoIt->second.first;
      NAPartition * subpartInfo = subpartInfoIt->second.second;
      if (firstpartInfo->getSubPartitionCount() == 1)
      {
        // Dropping the only partition of an object is not allowed.
        *CmpCommon::diags() << DgSqlCode(-8306)
                            << DgString0("Dropping the only partition of an object is not allowed");
        processReturn();
        return;
      }

      dropNAPartitionVec.push_back(subpartInfo);
      if (subpartCntPerFirstpartMap.find(firstpartInfo->getPartitionEntityName()) == subpartCntPerFirstpartMap.end())
        subpartCntPerFirstpartMap[firstpartInfo->getPartitionEntityName()] = firstpartInfo->getSubPartitionCount() - 1;
      else
        subpartCntPerFirstpartMap[firstpartInfo->getPartitionEntityName()]--;

    }

    for (std::map<NAString, Lng32>::iterator it = subpartCntPerFirstpartMap.begin();
         it != subpartCntPerFirstpartMap.end(); it++)
    {
      if (it->second < 1)
      {
        // Operation is not allowed because no partition exists after drop.
        *CmpCommon::diags() << DgSqlCode(-8306)
                            << DgString0("No partition exists after drop");
        processReturn();
        return;
      }
    }
  }

  //check finished, work begin
  NABoolean xnWasStartedHere = FALSE;
  if (beginXnIfNotInProgress(&cliInterface, xnWasStartedHere,
                             TRUE /*clearDDLList*/, FALSE /*clearObjectLocks*/))
  {
    processReturn();
    return;
  }

  // drop partition
  for (Lng32 i = 0; i < dropNAPartitionVec.size(); ++i)
  {
    cliRC = dropSeabasePartitionTable(&cliInterface, alterDropPartition, 
                                      dropNAPartitionVec[i], catalogNamePart, schemaNamePart);
    if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      goto label_rollback;
    }

    std::map<NAString, NAPartition *>::iterator it = nextNAPartitionMap.find(dropNAPartitionVec[i]->getPartitionEntityName());
    if (it != nextNAPartitionMap.end())
    {
      NAPartition * nextNAPart = it->second;
      if (nextNAPart)
      {
        NAString nextObjName = nextNAPart->getPartitionEntityName();
        if (std::find(dropPartEntityNameVec.begin(), dropPartEntityNameVec.end(), nextObjName) != dropPartEntityNameVec.end())
          continue;

        QualifiedName qn(NAString(nextObjName), schemaNamePart, catalogNamePart);
        retcode = lockObjectDDL(qn, COM_BASE_TABLE_OBJECT,
                                naTable->isVolatileTable(),
                                naTable->isTrafExternalTable());
        if (retcode < 0)
          {
            processReturn();
            goto label_rollback;
          }

        CorrName nextCN(nextObjName, STMTHEAP, schemaNamePart, catalogNamePart);
        NATable *nextNATable = bindWA.getNATable(nextCN);
        UpdateObjRefTimeParam nextUpdObjRefParam(-1, nextNATable->objectUid().castToInt64(),
                                 nextNATable->isStoredDesc(), nextNATable->storedStatsAvail());
        RemoveNATableParam nextRemoveNATableParam(ComQiScope::REMOVE_FROM_ALL_USERS,
                                 alterDropPartition->ddlXns(), FALSE);

        cliRC = updateCachesAndStoredDesc(&cliInterface, catalogNamePart,
                                 schemaNamePart, nextObjName, COM_BASE_TABLE_OBJECT,
                                 op, &nextRemoveNATableParam, &nextUpdObjRefParam,
                                 SharedCacheDDLInfo::DDL_UPDATE);
        if (cliRC < 0)
        {
          cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
          goto label_rollback;
        }
       }
     }
  }

  cliRC = updateCachesAndStoredDesc(&cliInterface, catalogNamePart, schemaNamePart, objectNamePart,
                COM_BASE_TABLE_OBJECT, op, &removeNATableParam, &updObjRefParam, SharedCacheDDLInfo::DDL_UPDATE);
  if (cliRC < 0)
  {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_rollback;
  }

  endXnIfStartedHere(&cliInterface, xnWasStartedHere, 0,
                 FALSE/*modifyEpochs*/, FALSE /*clearObjectLocks*/);
  return;

label_rollback:

  endXnIfStartedHere(&cliInterface, xnWasStartedHere, -1,
                   FALSE/*modifyEpochs*/, FALSE /*clearObjectLocks*/);

  processReturn();
  return ;
}

// ----------------------------------------------------------------------------
// alterSeabaseTableAlterIdentityColumn
//
//    ALTER TABLE <table> ALTER COLUMN <col> SET <sg-options>
//
// This statement runs in a transaction
// ----------------------------------------------------------------------------
void CmpSeabaseDDL::alterSeabaseTableAlterIdentityColumn(
                                                         StmtDDLAlterTableAlterColumnSetSGOption * alterIdentityColNode,
                                                         NAString &currCatName, NAString &currSchName)
{
  Lng32 cliRC = 0;
  Lng32 retcode = 0;

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, 
                               CmpCommon::context()->sqlSession()->getParentQid());

  NAString tabName = alterIdentityColNode->getTableName();
  NAString catalogNamePart, schemaNamePart, objectNamePart;
  NAString extTableName, extNameForHbase;
  NATable * naTable = NULL;
  CorrName cn;

  retcode = lockObjectDDL(alterIdentityColNode);
  if (retcode < 0) {
    processReturn();
    return;
  }

  retcode = 
    setupAndErrorChecks(tabName, 
                        alterIdentityColNode->getOrigTableNameAsQualifiedName(),
                        currCatName, currSchName,
                        catalogNamePart, schemaNamePart, objectNamePart,
                        extTableName, extNameForHbase, cn,
                        &naTable, 
                        FALSE, FALSE,
                        &cliInterface,
                        COM_BASE_TABLE_OBJECT,
                        SQLOperation::ALTER_TABLE);
  if (retcode < 0)
    {
      processReturn();
      return;
    }

  const NAColumnArray &nacolArr = naTable->getNAColumnArray();
  const NAString &colName = alterIdentityColNode->getColumnName();

  const NAColumn * nacol = nacolArr.getColumn(colName);
  if (! nacol)
    {
      *CmpCommon::diags() << DgSqlCode(-CAT_COLUMN_DOES_NOT_EXIST_ERROR)
                          << DgColumnName(colName);

      processReturn();

      return;
    }

  if (! nacol->isIdentityColumn())
    {
      *CmpCommon::diags() << DgSqlCode(-1590)
                          << DgColumnName(colName);

      processReturn();

      return;
    }

  // Table exists, update the epoch value to lock out other user access
  if (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL))
    {
      retcode = ddlSetObjectEpoch(naTable, STMTHEAP);
      if (retcode < 0)
        {
          processReturn();
          return;
        }
    }

  NAString seqName;
  SequenceGeneratorAttributes::genSequenceName
    (catalogNamePart, schemaNamePart, objectNamePart, 
     alterIdentityColNode->getColumnName(),
     seqName);
  
  ElemDDLSGOptions * sgo = alterIdentityColNode->getSGOptions();
  NAString options;
  if (sgo)
    {
      char tmpBuf[1000];
      if (sgo->isIncrementSpecified())
        {
          str_sprintf(tmpBuf, " increment by %ld", sgo->getIncrement());
          options += tmpBuf;
        }
      
      if (sgo->isMaxValueSpecified())
        {
          if (sgo->isNoMaxValue())
            strcpy(tmpBuf, " no maxvalue ");
          else
            str_sprintf(tmpBuf, " maxvalue %ld", sgo->getMaxValue());
          options += tmpBuf;
        }
      
      if (sgo->isMinValueSpecified())
        {
          if (sgo->isNoMinValue())
            strcpy(tmpBuf, " no maxvalue ");
          else
            str_sprintf(tmpBuf, " minvalue %ld", sgo->getMinValue());
          options += tmpBuf;
        }
      
      if (sgo->isStartValueSpecified())
        {
          str_sprintf(tmpBuf, " start with %ld", sgo->getStartValue());
          options += tmpBuf;
        }
      
      if (sgo->isRestartValueSpecified())
        {
          str_sprintf(tmpBuf, " restart with %ld", sgo->getStartValue());
          options += tmpBuf;
        }

      if (sgo->isCacheSpecified())
        {
          if (sgo->isNoCache())
            str_sprintf(tmpBuf, " no cache ");
          else
            str_sprintf(tmpBuf, " cache %ld ", sgo->getCache());
          options += tmpBuf;
        }
      
      if (sgo->isCycleSpecified())
        {
          if (sgo->isNoCycle())
            str_sprintf(tmpBuf, " no cycle ");
          else
            str_sprintf(tmpBuf, " cycle ");
          options += tmpBuf;
        }

      if (sgo->isResetSpecified())
        {
          str_sprintf(tmpBuf, " reset ");
          options += tmpBuf;
        }

      char buf[4000];
      str_sprintf(buf, "alter internal sequence %s.\"%s\".\"%s\" %s",
                  catalogNamePart.data(), schemaNamePart.data(), seqName.data(),
                  options.data());
      
      cliRC = cliInterface.executeImmediate(buf);
      if (cliRC < 0)
        {
          cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
          
          processReturn();
          
          return;
        }
    }

  if (naTable->incrBackupEnabled())
  {
    cliRC = updateSeabaseXDCDDLTable(&cliInterface, catalogNamePart.data(),
                                     schemaNamePart.data(), objectNamePart.data(),
                                     COM_BASE_TABLE_OBJECT_LIT); 
    if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

      processReturn();
      
      return;
    }
  }

  cliRC = updateObjectRedefTime(&cliInterface,
                                catalogNamePart, schemaNamePart, objectNamePart,
                                COM_BASE_TABLE_OBJECT_LIT, -1,
                                naTable->objectUid().castToInt64(),
                                naTable->isStoredDesc(), naTable->storedStatsAvail());
  if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

      processReturn();
      
      return;
    }

  if (naTable->isStoredDesc() && (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)))
    {
      insertIntoSharedCacheList(catalogNamePart, schemaNamePart, objectNamePart,
                                COM_BASE_TABLE_OBJECT, SharedCacheDDLInfo::DDL_UPDATE);
    }

  ActiveSchemaDB()->getNATableDB()->removeNATable
    (cn,
     ComQiScope::REMOVE_FROM_ALL_USERS, COM_BASE_TABLE_OBJECT,
     alterIdentityColNode->ddlXns(), FALSE);

  CorrName seqn(seqName, STMTHEAP, schemaNamePart, catalogNamePart);
  seqn.setSpecialType(ExtendedQualName::SG_TABLE);
  ActiveSchemaDB()->getNATableDB()->removeNATable
    (seqn, 
     ComQiScope::REMOVE_FROM_ALL_USERS, COM_SEQUENCE_GENERATOR_OBJECT,
     alterIdentityColNode->ddlXns(), FALSE);

  return;
}

short CmpSeabaseDDL::saveAndDropUsingViews(Int64 objUID,
                                           ExeCliInterface *cliInterface,
                                           NAList<NAString> &viewNameList,
                                           NAList<NAString> &viewDefnList)
{
  Lng32 cliRC = 0;

  NAString catName, schName, objName;
  cliRC = getObjectName(cliInterface, objUID,
                        catName, schName, objName);
  if (cliRC < 0)
    {
      processReturn();
      
      return -1;
    }

  Queue * usingViewsQueue = NULL;
  cliRC = getAllUsingViews(cliInterface, 
                           catName, schName, objName, 
                           usingViewsQueue);
  if (cliRC < 0)
    {
      processReturn();
      
      return -1;
    }

  if (usingViewsQueue->numEntries() == 0)
    return 0;

  NABoolean xnWasStartedHere = FALSE;
  if (beginXnIfNotInProgress(cliInterface, xnWasStartedHere,
                             TRUE /*clearDDLList*/, FALSE /*clearObjectLocks*/))
    return -1;
  
  // find out any views on this table.
  // save their definition and drop them.
  // they will be recreated before return.
  usingViewsQueue->position();
  for (int idx = 0; idx < usingViewsQueue->numEntries(); idx++)
    {
      OutputInfo * vi = (OutputInfo*)usingViewsQueue->getNext(); 
      char * viewName = vi->get(0);
      
      viewNameList.insert(viewName);
      
      ComObjectName viewCO(viewName, COM_TABLE_NAME);
      
      const NAString catName = viewCO.getCatalogNamePartAsAnsiString();
      const NAString schName = viewCO.getSchemaNamePartAsAnsiString(TRUE);
      const NAString objName = viewCO.getObjectNamePartAsAnsiString(TRUE);
      
      Int64 viewUID = getObjectUID(cliInterface,
                                   catName.data(), schName.data(), objName.data(), 
                                   COM_VIEW_OBJECT_LIT);
      if (viewUID < 0 )
        {
          endXnIfStartedHere(cliInterface, xnWasStartedHere, -1,
                             FALSE/*modifyEpochs*/, FALSE /*clearObjectLocks*/);
          
          return -1;
        }
      
      NAString viewText;
      if (getTextFromMD(cliInterface, viewUID, COM_VIEW_TEXT, 0, viewText))
        {
          endXnIfStartedHere(cliInterface, xnWasStartedHere, -1,
                             FALSE/*modifyEpochs*/, FALSE /*clearObjectLocks*/);
          
          return -1;
        }
      
      viewDefnList.insert(viewText);
    }

  // drop the views.
  // usingViewsQueue contain them in ascending order of their create
  // time. Drop them from last to first.
  for (int idx = usingViewsQueue->numEntries()-1; idx >= 0; idx--)
    {
      OutputInfo * vi = (OutputInfo*)usingViewsQueue->get(idx);
      char * viewName = vi->get(0);

      if (dropOneTableorView(*cliInterface,viewName,COM_VIEW_OBJECT,false))
        {
          endXnIfStartedHere(cliInterface, xnWasStartedHere, -1,
                             FALSE/*modifyEpochs*/, FALSE /*clearObjectLocks*/);
          
          processReturn();
          
          return -1;
        }
    }

  endXnIfStartedHere(cliInterface, xnWasStartedHere, 0,
                     FALSE/*modifyEpochs*/, FALSE /*clearObjectLocks*/);
   
  return 0;
}

short CmpSeabaseDDL::recreateUsingViews(ExeCliInterface *cliInterface,
                                        NAList<NAString> &viewNameList,
                                        NAList<NAString> &viewDefnList,
                                        NABoolean ddlXns)
{
  Lng32 cliRC = 0;

  if (viewDefnList.entries() == 0)
    return 0;

  NABoolean xnWasStartedHere = FALSE;
  if (beginXnIfNotInProgress(cliInterface, xnWasStartedHere,
                             TRUE /* clearDDLList */, FALSE /*clearObjectLocks*/))
    return -1;

  for (Lng32 i = 0; i < viewDefnList.entries(); i++)
    {
      cliRC = cliInterface->executeImmediate(viewDefnList[i]);
      if (cliRC < 0)
        {
          cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
          
          cliRC = -1;
          goto label_return;
        }
    }

  cliRC = 0;
  
label_return:
 
  for (Lng32 i = 0; i < viewDefnList.entries(); i++)
    {
      ComObjectName tableName(viewNameList[i], COM_TABLE_NAME);
      const NAString catalogNamePart = 
        tableName.getCatalogNamePartAsAnsiString();
      const NAString schemaNamePart = 
        tableName.getSchemaNamePartAsAnsiString(TRUE);
      const NAString objectNamePart = 
        tableName.getObjectNamePartAsAnsiString(TRUE);

      CorrName cn(objectNamePart, STMTHEAP, schemaNamePart, catalogNamePart);
      ActiveSchemaDB()->getNATableDB()->removeNATable
        (cn,
         ComQiScope::REMOVE_MINE_ONLY, COM_VIEW_OBJECT,
         ddlXns, FALSE);
    }

  endXnIfStartedHere(cliInterface, xnWasStartedHere, cliRC,
                     TRUE /* modifyEpochs */, FALSE /*clearObjectLocks*/);
 
  return cliRC;
}

///////////////////////////////////////////////////////////////////////
//
// An aligned table constains all columns in one hbase cell.
// To alter a column, we need to read each row, create a
// new row with the altered column and insert into the original table.
//
// Validation that altered col datatype is compatible with the
// original datatype has already been done before this method
// is called.
//
// Steps to alter a column from an aligned table:
//
// -- make a copy of the source aligned table using hbase copy
// -- truncate the source table
// -- Update metadata column definition with the new definition
// -- bulk load data from copied table into the source table
// -- recreate views, if existed, based on the new definition
// -- drop the copied temp table
//
// If an error happens after the source table has been truncated, then
// it will be restored from the copied table.
//
///////////////////////////////////////////////////////////////////////
short CmpSeabaseDDL::alignedFormatTableAlterColumnAttr
(
 const NAString &catalogNamePart,
 const NAString &schemaNamePart,
 const NAString &objectNamePart,
 const NATable * naTable,
 const NAString &altColName,
 ElemDDLColDef *pColDef,
 NABoolean ddlXns,
 NAList<NAString> &viewNameList,
 NAList<NAString> &viewDefnList)
{
  Lng32 cliRC = 0;
  Lng32 cliRC2 = 0;

  const NAFileSet * naf = naTable->getClusteringIndex();
  
  CorrName cn(objectNamePart, STMTHEAP, schemaNamePart, catalogNamePart);

  ExpHbaseInterface * ehi = allocEHI(naTable->storageType());
  if (ehi == NULL) {
     processReturn();
     return -1;
  }

  ExeCliInterface cliInterface
    (STMTHEAP, 0, NULL, 
     CmpCommon::context()->sqlSession()->getParentQid());

  const NAColumnArray &naColArr = naTable->getNAColumnArray();
  const NAColumn * altNaCol = naColArr.getColumn(altColName);
  Lng32 altColNum = altNaCol->getPosition();

  NAString colFamily;
  NAString colName;
  Lng32 datatype, length, precision, scale, dt_start, dt_end, 
    nullable, upshifted;
  ComColumnClass colClass;
  ComColumnDefaultClass defaultClass;
  NAString charset, defVal;
  NAString heading;
  ULng32 hbaseColFlags;
  Int64 colFlags;
  ComLobsStorageType lobStorage;
  NAString compDefnStr;
  NAString quotedDefVal;
  NAArray<HbaseStr> *defStringArray = NULL;
  HbaseStr *oldCols = NULL;
  HbaseStr *oldKeys = NULL;
  NAString colColsString;
  NAString addColString;
  char fullColBuf[1024*20]; //some table may have 1024 columns
  char fullKeyBuf[128*20];
  char addColBuf[128];
  NAString colBufNAString;
  NAString fullColBufString;
  NAString fullKeyBufString;
  NAString binlogTableName;
  Lng32 retcode = 0;
  NAString tmpansitype;
  NAString stripedstr ; 
  char* ansitype;
  NAList<NAString> fields(STMTHEAP);
  NAList<NAString> thisfields(STMTHEAP);


  NABoolean xnWasStartedHere = FALSE;
  char buf[4000 + 1024/*max size of default*/];

  // Remove object from shared cache before making changes
  // TDB - instead of removing, disable it
  if (naTable->isStoredDesc())
    {
      // TDB - replace code with call to deleteFromSharedCache
      int32_t diagsMark = CmpCommon::diags()->mark();
      NAString alterStmt ("alter trafodion metadata shared cache for table ");
      alterStmt += naTable->getTableName().getQualifiedNameAsAnsiString().data();
      alterStmt += " delete internal";

      NAString msg(alterStmt);
      msg.prepend("Removing object for alter column: ");
      QRLogger::log(CAT_SQL_EXE, LL_WARN, "%s", msg.data());

      cliRC = cliInterface.executeImmediate(alterStmt.data());

      // If unable to remove from shared cache, it may not exist, continue 
      // If the operation is later rolled back, this table will not be added
      // back to shared cache - TBD
      CmpCommon::diags()->rewind(diagsMark);
    }

  // save data by cloning as a temp table and truncate source table
  NAString clonedTable;
  cliRC = cloneAndTruncateTable(naTable, clonedTable, ehi, &cliInterface);

  if (cliRC < 0)
    {
      goto label_drop; // diags already populated by called method
    }

  if (beginXnIfNotInProgress(&cliInterface, xnWasStartedHere,
                             FALSE /*clearDDLList*/, FALSE /*clearObjectLocks*/))
    {
      cliRC = -1;
      goto label_restore;
    }

  // add X lock
  if (lockRequired(naTable, catalogNamePart, schemaNamePart, objectNamePart, HBaseLockMode::LOCK_X, FALSE/* useHbaseXN */, FALSE/* replSync */, FALSE/* incrementalBackup */, FALSE/* asyncOperation */, TRUE/* noConflictCheck */, TRUE/* registerRegion */))
  {
    cliRC = -1;
    goto label_restore;
  }
  
  if (getColInfo(pColDef,
                 FALSE, // not a metadata, histogram or repository column
                 colFamily,
                 colName, 
                 naTable->isSQLMXAlignedTable(),
                 datatype, length, precision, scale, dt_start, dt_end, 
                 upshifted, nullable,
                 charset, colClass, defaultClass, defVal, heading, lobStorage,
                 compDefnStr,
                 hbaseColFlags, colFlags))
    {
      cliRC = -1;
      processReturn();
      
      goto label_restore;
    }
  buf[4000 + defVal.length()];
  
  if (NOT defVal.isNull())
    {
      ToQuotedString(quotedDefVal, defVal, FALSE);
    }
  //generate new columns def string for binlog reader
  char binlogTableDefString[128];
  memset(binlogTableDefString, 0, 128);

  ansitype = (char*)getAnsiTypeStrFromFSType(datatype);
  tmpansitype = ansitype;
  stripedstr = tmpansitype.strip(NAString::trailing, ' ');
  //trim ansitype
  tmpansitype = ansitype;
  stripedstr = tmpansitype.strip(NAString::trailing, ' ');

  //update the table column definition string in binlog
  binlogTableName = schemaNamePart + "." + objectNamePart;
  if (!ComIsTrafodionReservedSchemaName(schemaNamePart) && naTable->isSeabaseTable() == TRUE) {
    //first , get the current string
    retcode = ehi->getTableDefForBinlog(binlogTableName, &defStringArray);

    oldCols = &defStringArray->at(0);
    oldKeys = &defStringArray->at(1);
    snprintf(fullColBuf, oldCols->len, "%s", oldCols->val);
    snprintf(fullKeyBuf, oldKeys->len, "%s", oldKeys->val);
    //replace the col def at altColNum
    colBufNAString = fullColBuf;
    colBufNAString.split(';',fields); 

    for(int i =0; i < fields.entries(); i++)
    {
      if(i == altColNum)
      {
        //check old colClass first
        //fields[i] is old string
        fields[i].split('-',thisfields);
        if(thisfields[3] == "A")
          sprintf(binlogTableDefString,"%d-%d-%d-%s-%d-%d-%s-%s-%d-%d-%d-%s" , datatype, nullable, length, "A" , precision, scale ,charset.data(),stripedstr.data(),dt_start,dt_end,altColNum,colName.strip(NAString::trailing,' ').data());
        else
          sprintf(binlogTableDefString,"%d-%d-%d-%s-%d-%d-%s-%s-%d-%d-%d-%s" , datatype, nullable, length, "U" , precision, scale ,charset.data(),stripedstr.data(),dt_start,dt_end,altColNum,colName.strip(NAString::trailing,' ').data());
        fullColBufString += binlogTableDefString;
      }
      else
      {
        fullColBufString += fields[i];
      }
      if(i<fields.entries()-1) {
        fullColBufString = fullColBufString + ";";
      }
      
    }
    fullKeyBufString = fullKeyBuf;
    //update the meta for binlog_reader
    retcode = ehi->updateTableDefForBinlog(binlogTableName, fullColBufString , fullKeyBufString, 0);
  }   

  // remove hist stats for the column being altered
  str_sprintf(buf, "update statistics for table %s on \"%s\" clear",
              naTable->getTableName().getQualifiedNameAsAnsiString().data(),
              altColName.data());
  cliRC = cliInterface.executeImmediate(buf);
  if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      goto label_restore;
    }

  str_sprintf(buf, "update %s.\"%s\".%s set (column_class, fs_data_type, sql_data_type, column_size, column_precision, column_scale, datetime_start_field, datetime_end_field, is_upshifted, nullable, character_set, default_class, default_value) = (case when column_class in ('" COM_ADDED_USER_COLUMN_LIT"', '" COM_ADDED_ALTERED_USER_COLUMN_LIT"') then '" COM_ADDED_ALTERED_USER_COLUMN_LIT"' else '" COM_ALTERED_USER_COLUMN_LIT"' end, %d, '%s', %d, %d, %d, %d, %d, '%s', %d, '%s', %d, '%s') where object_uid = %ld and column_number = %d",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_COLUMNS,
              datatype,
              getAnsiTypeStrFromFSType(datatype),
              length,
              precision,
              scale,
              dt_start,
              dt_end,
              (upshifted ? "Y" : "N"),
              nullable,
              (char*)charset.data(),
              (Lng32)defaultClass,
              (quotedDefVal.isNull() ? " " : quotedDefVal.data()),//column DEFAULT_VALUE can't be NULL,so we can't use ''(null) here
              naTable->objectUid().castToInt64(),
              altColNum);
  
  cliRC = cliInterface.executeImmediate(buf);
  if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      goto label_restore;
    }
 
  ActiveSchemaDB()->getNATableDB()->removeNATable
    (cn,
     ComQiScope::REMOVE_FROM_ALL_USERS, 
     COM_BASE_TABLE_OBJECT, ddlXns, FALSE);

  if (naTable->hasSecondaryIndexes()) // user indexes
    {
      cliRC = cliInterface.holdAndSetCQD("hide_indexes", "ALL");
      if (cliRC < 0)
        {
          cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
          goto label_restore;
        }
    }

  cliRC = cliInterface.holdAndSetCQD("attempt_esp_parallelism", "ON");
  if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      goto label_restore;
    }

  cliRC = cliInterface.holdAndSetCQD("OVERRIDE_SYSKEY", "ON");
  if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      goto label_restore;
    }

  cliRC = cliInterface.holdAndSetCQD("OVERRIDE_GENERATED_IDENTITY_VALUES", "ON");
  if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      goto label_restore;
    }

  str_sprintf(buf, "upsert using load into %s select * from %s",
              naTable->getTableName().getQualifiedNameAsAnsiString().data(),
              clonedTable.data());
  cliRC = cliInterface.executeImmediate(buf);

  if (naTable->hasSecondaryIndexes()) // user indexes
    cliInterface.restoreCQD("hide_indexes");
  cliInterface.restoreCQD("OVERRIDE_SYSKEY");
  cliInterface.restoreCQD("attempt_esp_parallelism");

  if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

      NAString reason;
      reason = "Old data could not be updated using the altered column definition.";
      
      // column cannot be altered
      *CmpCommon::diags() << DgSqlCode(-1404)
                          << DgColumnName(altColName)
                          << DgString0("altered")
                          << DgString1(reason);

      goto label_restore;
    }

  if ((cliRC = recreateUsingViews(&cliInterface, viewNameList, viewDefnList,
                                  ddlXns)) < 0)
    {
      NAString reason = "Error occurred while recreating views due to dependency on older column definition. Drop dependent views before doing the alter.";
      *CmpCommon::diags() << DgSqlCode(-1404)
                          << DgColumnName(altColName)
                          << DgString0("altered")
                          << DgString1(reason);
      goto label_restore;
    }

  endXnIfStartedHere(&cliInterface, xnWasStartedHere, 0,
                     FALSE /*modifyEpochs*/, FALSE /*clearObjectLocks*/);
  
  goto label_drop;

label_restore:
  CmpCommon::context()->executeTestPoint(TESTPOINT_1);

  endXnIfStartedHere(&cliInterface, xnWasStartedHere, -1,
                     FALSE /*modifyEpochs*/, FALSE /*clearObjectLocks*/);

  if (naTable->hasSecondaryIndexes()) // user indexes
    cliInterface.restoreCQD("hide_indexes");
  cliInterface.restoreCQD("attempt_esp_parallelism");

  ActiveSchemaDB()->getNATableDB()->removeNATable
    (cn,
     ComQiScope::REMOVE_FROM_ALL_USERS, 
     COM_BASE_TABLE_OBJECT, FALSE, FALSE);
  
  if ((cliRC < 0) &&
      (NOT clonedTable.isNull()) &&
      (cloneSeabaseTable(clonedTable, -1,
                         naTable->getTableName().getQualifiedNameAsAnsiString(),
                         naTable,
                         ehi, &cliInterface, FALSE)))
    {
      cliRC = -1;
      goto label_drop;
    }
 
label_drop:  
  if (NOT clonedTable.isNull())
    {
      str_sprintf(buf, "drop table %s", clonedTable.data());
      cliRC2 = cliInterface.executeImmediate(buf);
    }

  cliInterface.restoreCQD("override_generated_identity_values");
  
  cliInterface.restoreCQD("hide_indexes");

  ActiveSchemaDB()->getNATableDB()->removeNATable
    (cn,
     ComQiScope::REMOVE_FROM_ALL_USERS, 
     COM_BASE_TABLE_OBJECT, ddlXns, FALSE);

  deallocEHI(ehi); 
  
  return (cliRC < 0 ? -1 : 0);
}

/////////////////////////////////////////////////////////////////////
// this method is called if alter could be done by metadata changes
// only and without affecting table data.
/////////////////////////////////////////////////////////////////////
short CmpSeabaseDDL::mdOnlyAlterColumnAttr(
     const NAString &catalogNamePart, const NAString &schemaNamePart,
     const NAString &objectNamePart,
     const NATable * naTable, const NAColumn * naCol, NAType * newType,
     StmtDDLAlterTableAlterColumnDatatype * alterColNode,
     NAList<NAString> &viewNameList,
     NAList<NAString> &viewDefnList,
     NABoolean isAlterForSample,
     Int64 sampleTableUid)
{
  Lng32 cliRC = 0;

  ExeCliInterface cliInterface
    (STMTHEAP, 0, NULL, 
     CmpCommon::context()->sqlSession()->getParentQid());
  
  Int64 objUID = 0;

  //figure out the position of altered column
  Lng32 altColNum = naCol->getPosition();
  NAString colName = naCol->getColName();

  NAString colColsString;
  NAString addColString;
  char fullColBuf[1024*20]; //some table may have 1024 columns
  char fullKeyBuf[128*20];
  char addColBuf[128];
  NAString colBufNAString;
  NAString fullColBufString;
  NAString fullKeyBufString;
  NAString binlogTableName;
  Lng32 retcode = 0;
  NAString tmpansitype;
  NAString stripedstr ;
  char* ansitype;
  Lng32 colNumber = 0;

  NAArray<HbaseStr> *defStringArray = NULL;
  HbaseStr *oldCols = NULL;
  HbaseStr *oldKeys = NULL;
  Int64 colFlags = 0;

  NAList<NAString> fields(STMTHEAP);
  NAList<NAString> thisfields(STMTHEAP);
  char binlogTableDefString[128];
  memset(binlogTableDefString, 0, 128);
  ExpHbaseInterface * ehi = NULL;
  NABoolean xnWasStartedHere = FALSE;
  TrafDesc *cd = NULL;

    NABoolean alignedFormat;
    Lng32 datatype, length, precision, scale, dtStart, dtEnd, upshifted, nullable;
    ULng32 hbaseColFlags;
    NAString charset;

  //update binlog meta
  ehi = allocEHI(naTable->storageType());
  if (ehi == NULL) {
    goto label_error;
  }
  binlogTableName = schemaNamePart + "." + objectNamePart;

  if (!ComIsTrafodionReservedSchemaName(schemaNamePart) && naTable->isSeabaseTable() == TRUE) {
    //first , get the current string
    retcode = ehi->getTableDefForBinlog(binlogTableName, &defStringArray);
    oldCols = &defStringArray->at(0);
    oldKeys = &defStringArray->at(1);
    snprintf(fullColBuf, oldCols->len, "%s", oldCols->val);
    snprintf(fullKeyBuf, oldKeys->len, "%s", oldKeys->val);
    //replace the col def at altColNum
    colBufNAString = fullColBuf;
    colBufNAString.split(';',fields);
    CharInfo::Collation collationSequence = CharInfo::DefaultCollation;
    getTypeInfo(newType, alignedFormat, -1,
                   datatype, length, precision, scale, dtStart, dtEnd, upshifted, nullable,
                   charset, collationSequence, hbaseColFlags);

    for(int i =0; i < fields.entries(); i++)
    {
      if(i == altColNum)
      {
        //check old colClass first
        //fields[i] is old string
        fields[i].split('-',thisfields);
        if(thisfields[3] == "A")
          sprintf(binlogTableDefString,"%d-%d-%d-%s-%d-%d-%s-%s-%d-%d-%d-%s;" , datatype, nullable, length, "A", precision, scale ,charset.data(),stripedstr.data(),dtStart,dtEnd,altColNum,colName.strip(NAString::trailing,' ').data());
        else
          sprintf(binlogTableDefString,"%d-%d-%d-%s-%d-%d-%s-%s-%d-%d-%d-%s" , datatype, nullable, length, "U" , precision, scale ,charset.data(),stripedstr.data(),dtStart,dtEnd,altColNum,colName.strip(NAString::trailing,' ').data());
        fullColBufString += binlogTableDefString;
      }
      else
      {
        fullColBufString += fields[i];
      }
      if(i<fields.entries()-1) {
        fullColBufString = fullColBufString + ";";
      }

    }
    fullKeyBufString = fullKeyBuf;
    //update the meta for binlog_reader
    retcode = ehi->updateTableDefForBinlog(binlogTableName, fullColBufString , fullKeyBufString, 0);

  }

  
  if (isAlterForSample)
    objUID = sampleTableUid;
  else
    objUID = naTable->objectUid().castToInt64();
  
  colNumber = naCol->getPosition();

  cd = (TrafDesc*)naTable->getColumnsDesc();

  // find the columns_desc for this key column
  while (cd->columnsDesc()->colnumber != colNumber &&
         cd->next)
    cd = cd->next;

  colFlags = cd->columnsDesc()->getColFlags();
  if (newType->getTypeQualifier() == NA_CHARACTER_TYPE)
  {
    if (((CharType*)newType)->isVarchar2())
      colFlags |= SEABASE_COLUMN_IS_TYPE_VARCHAR2;
    else
      colFlags &= ~SEABASE_COLUMN_IS_TYPE_VARCHAR2;
  }

  if (beginXnIfNotInProgress(&cliInterface, xnWasStartedHere,
                             FALSE /*clearDDLList*/, FALSE /*clearObjectLocks*/))
    return -1;

  char buf[4000];
  str_sprintf(buf, "update %s.\"%s\".%s set column_size = %d, column_precision = %d, "
              "column_class = case  when column_class in ('" COM_ADDED_USER_COLUMN_LIT"', '" COM_ADDED_ALTERED_USER_COLUMN_LIT"') then '" COM_ADDED_ALTERED_USER_COLUMN_LIT"' else '" COM_ALTERED_USER_COLUMN_LIT"' end , flags = %ld where object_uid = %ld and column_number = %d",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_COLUMNS,
              newType->getNominalSize(),
              newType->getPrecisionOrMaxNumChars(),
              colFlags,
              objUID,
              colNumber);
  
  cliRC = cliInterface.executeImmediate(buf);
  if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      
      goto label_error;
    }
  
  if (NOT isAlterForSample)
  {
    cliRC = recreateUsingViews(&cliInterface, viewNameList, viewDefnList, 
                               alterColNode->ddlXns());
    if (cliRC < 0)
    {
      NAString reason = "Error occurred while recreating views due to dependency on older column definition. Drop dependent views before doing the alter.";
      *CmpCommon::diags() << DgSqlCode(-1404)
                          << DgColumnName(naCol->getColName().data())
                          << DgString0("altered")
                          << DgString1(reason);
      
      goto label_error;
    }
  }
  
  endXnIfStartedHere(&cliInterface, xnWasStartedHere, cliRC,
                     FALSE /*modifyEpochs*/, FALSE /*clearObjectLocks*/);

  return 0;

 label_error:
  endXnIfStartedHere(&cliInterface, xnWasStartedHere, cliRC,
                     FALSE /*modifyEpochs*/, FALSE /*clearObjectLocks*/);

  return -1;
}

///////////////////////////////////////////////////////////////////////
//
// Steps to alter a column from an hbase format table:
//
// Validation that altered col datatype is compatible with the
// original datatype has already been done before this method
// is called.
//
// -- add a temp column based on the altered datatype
// -- update temp col with data from the original col 
// -- update metadata column definition with the new col definition
// -- update original col with data from temp col
// -- recreate views, if existed, based on the new definition
// -- drop the temp col. Dependent views will be recreated during drop.
//
// If an error happens after the source table has been truncated, then
// it will be restored from the copied table.
//
///////////////////////////////////////////////////////////////////////
short CmpSeabaseDDL::hbaseFormatTableAlterColumnAttr(
     const NAString &catalogNamePart, const NAString &schemaNamePart,
     const NAString &objectNamePart,
     const NATable * naTable, const NAColumn * naCol, NAType * newType,
     StmtDDLAlterTableAlterColumnDatatype * alterColNode)
{
  ExeCliInterface cliInterface
    (STMTHEAP, 0, NULL, 
     CmpCommon::context()->sqlSession()->getParentQid());

  CorrName cn(objectNamePart, STMTHEAP, schemaNamePart,catalogNamePart);

  Lng32 cliRC = 0;
  Lng32 retcode = 0;

  ComUID comUID;
  comUID.make_UID();
  Int64 objUID = comUID.get_value();
  
  char objUIDbuf[100];

  NAString tempCol(naCol->getColName());
  tempCol += "_";
  tempCol += str_ltoa(objUID, objUIDbuf);

  char dispBuf[1000];
  Lng32 ii = 0;
  NABoolean identityCol;
  ElemDDLColDef *pColDef = alterColNode->getColToAlter()->castToElemDDLColDef();
  NAColumn *nac = NULL;
  if (getNAColumnFromColDef(pColDef, nac))
    return -1;

  dispBuf[0] = 0;
  if (cmpDisplayColumn(nac, (char*)tempCol.data(), newType, 3, NULL, dispBuf, 
                       ii, FALSE, identityCol, 
                       FALSE, FALSE, UINT_MAX, NULL))
    return -1;
  
  Int64 tableUID = naTable->objectUid().castToInt64();
  const NAColumnArray &nacolArr = naTable->getNAColumnArray();
  const NAString &altColName = naCol->getColName();
  const NAColumn * altNaCol = nacolArr.getColumn(altColName);
  Lng32 altColNum = altNaCol->getPosition();

  char buf[4000];
  str_sprintf(buf, "alter table %s add column %s",
              naTable->getTableName().getQualifiedNameAsAnsiString().data(), 
              dispBuf);
  cliRC = cliInterface.executeImmediate(buf);
  if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

      processReturn();
      return -1;
    }

  ActiveSchemaDB()->getNATableDB()->removeNATable
    (cn,
     ComQiScope::REMOVE_FROM_ALL_USERS,
     COM_BASE_TABLE_OBJECT,
     alterColNode->ddlXns(), FALSE);

  str_sprintf(buf, "update %s set %s = %s",
              naTable->getTableName().getQualifiedNameAsAnsiString().data(), 
              tempCol.data(),
              naCol->getColName().data());
  cliRC = cliInterface.executeImmediate(buf);
  if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

      goto label_error1;
    }
   
  str_sprintf(buf, "delete from %s.\"%s\".%s where object_uid = %ld and column_number = %d",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_COLUMNS,
              tableUID,
              altColNum);
  
  cliRC = cliInterface.executeImmediate(buf);
  if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      goto label_error1;
    }
  
  str_sprintf(buf, "insert into %s.\"%s\".%s select object_uid, '%s', %d, '" COM_ALTERED_USER_COLUMN_LIT"', fs_data_type, sql_data_type, column_size, column_precision, column_scale, datetime_start_field, datetime_end_field, is_upshifted, column_flags, nullable, character_set, default_class, default_value, column_heading, '%s', '%s', direction, is_optional, flags from %s.\"%s\".%s where object_uid = %ld and column_number = (select column_number from %s.\"%s\".%s where object_uid = %ld and column_name = '%s')",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_COLUMNS,
              naCol->getColName().data(),              
              altColNum,
              altNaCol->getHbaseColFam().data(),
              altNaCol->getHbaseColQual().data(),
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_COLUMNS,
              tableUID,
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_COLUMNS,
              tableUID,
              tempCol.data());
  
  cliRC = cliInterface.executeImmediate(buf);
  if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      goto label_error1;
    }

  ActiveSchemaDB()->getNATableDB()->removeNATable
    (cn,
     ComQiScope::REMOVE_FROM_ALL_USERS,
     COM_BASE_TABLE_OBJECT,
     alterColNode->ddlXns(), FALSE);

  str_sprintf(buf, "update %s set %s = %s",
              naTable->getTableName().getQualifiedNameAsAnsiString().data(), 
              naCol->getColName().data(),
              tempCol.data());
  cliRC = cliInterface.executeImmediate(buf);
  if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

      NAString reason;
      reason = "Old data could not be updated into the new column definition.";
      
      // column cannot be altered
      *CmpCommon::diags() << DgSqlCode(-1404)
                          << DgColumnName(naCol->getColName())
                          << DgString0("altered")
                          << DgString1(reason);

      processReturn();
      goto label_error1;
    }
   
  str_sprintf(buf, "alter table %s drop column %s",
              naTable->getTableName().getQualifiedNameAsAnsiString().data(), 
              tempCol.data());
  cliRC = cliInterface.executeImmediate(buf);
  if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      
      processReturn();
      return -1;
    }

  return 0;

 label_error1:
  str_sprintf(buf, "alter table %s drop column %s",
              naTable->getTableName().getQualifiedNameAsAnsiString().data(), 
              tempCol.data());
  cliRC = cliInterface.executeImmediate(buf);
  if (cliRC < 0)
    {
      processReturn();
      return -1;
    } 
 
  return -1;
}

// ----------------------------------------------------------------------------
// alterSeabaseTableAlterColumnDatatype
//
//    ALTER TABLE <table> ALTER COLUMN <col> SET <sg-options>
//
// This statement is not run in a transaction by default.
// It cannot be run in a user transaction
// This statement may recreate the table, so it employs a read and read/write
// DDL lock.
// ----------------------------------------------------------------------------
void CmpSeabaseDDL::alterSeabaseTableAlterColumnDatatype(
     StmtDDLAlterTableAlterColumnDatatype * alterColNode,
     NAString &currCatName, NAString &currSchName, NABoolean isMaintenanceWindowOFF)
{
  Lng32 cliRC = 0;
  Lng32 retcode = 0;
  DDLScopeInterface ddlScope(CmpCommon::context(),CmpCommon::diags()); // to manage DDL lock release

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, 
                               CmpCommon::context()->sqlSession()->getParentQid());

  NAString tabName = alterColNode->getTableName();
  NAString catalogNamePart, schemaNamePart, objectNamePart;
  NAString extTableName, extNameForHbase;
  NAString sampleTableName;
  Int64 sampleTableUid = 0;
  NATable * naTable = NULL;
  CorrName cn;

  retcode = lockObjectDDL(alterColNode);
  if (retcode < 0) {
    processReturn();
    return;
  }

  retcode = 
    setupAndErrorChecks(tabName, 
                        alterColNode->getOrigTableNameAsQualifiedName(),
                        currCatName, currSchName,
                        catalogNamePart, schemaNamePart, objectNamePart,
                        extTableName, extNameForHbase, cn,
                        &naTable, 
                        FALSE, FALSE,
                        &cliInterface,
                        COM_BASE_TABLE_OBJECT,
                        SQLOperation::ALTER_TABLE,
                        FALSE,
                        TRUE/*process hiatus*/);
  if (retcode < 0)
    {
      processReturn();

      return;
    }

  ElemDDLColDef *pColDef = alterColNode->getColToAlter()->castToElemDDLColDef();
  
  const NAColumnArray &nacolArr = naTable->getNAColumnArray();
  const NAString &colName = pColDef->getColumnName();

  const NAColumn * nacol = nacolArr.getColumn(colName);
  if (! nacol)
    {
      // column doesnt exist. Error.
      *CmpCommon::diags() << DgSqlCode(-CAT_COLUMN_DOES_NOT_EXIST_ERROR)
                          << DgColumnName(colName);

      processReturn();

      return;
    }

  if (nacol->isIdentityColumn())
    {
	  *CmpCommon::diags() << DgSqlCode(-CAT_IDENTITY_COLUMN_DATATYPE_ALTER)
                          << DgColumnName(colName);
      processReturn();
      return;
    }
  const NAType * currType = nacol->getType();
  NAType * newType = pColDef->getColumnDataType();

  // Undeclared attributes inherit attributes from columns in the original table
  if (NOT pColDef->getIsConstraintNotNullSpecified())
    newType->setNullable(*currType);

  // if column charset is not specified, use the orignal charset as default charset
  if (NOT pColDef->isColCharsetSpecified() && (DFS2REC::isSQLVarChar(currType->getFSDatatype())) && (DFS2REC::isSQLVarChar(newType->getFSDatatype())))
  {
    NABoolean isNullable = FALSE;
    if (pColDef->getIsConstraintNotNullSpecified())
      isNullable = newType->supportsSQLnull();
    else
      isNullable = currType->supportsSQLnull();

    if (currType->getCharSet() != newType->getCharSet())
    {
      Lng32 Prec = ((CharType*)newType)->getStrCharLimit();
      Lng32 len = ((CharType*)newType)->getStrCharLimit() * CharInfo::maxBytesPerChar(currType->getCharSet());
      newType = new (STMTHEAP)
                          SQLVarChar( STMTHEAP,
                                      CharLenInfo(Prec, len),
                                      isNullable,
                                      ((CharType*)currType)->isUpshifted(),
                                      ((CharType*)currType)->isCaseinsensitive(),
                                      ((CharType*)currType)->getCharSet(),
                                      ((CharType*)currType)->getCollation(),
                                      ((CharType*)currType)->getCoercibility()
                                      );
                                    
      pColDef->setColumnDataType(newType);
    }
  }

  // If column is a LOB column , error
  if ((currType->getFSDatatype() == REC_BLOB) || (currType->getFSDatatype() == REC_CLOB))
    {
      *CmpCommon::diags() << DgSqlCode(-CAT_LOB_COLUMN_ALTER)
                          << DgColumnName(colName);
      processReturn();
      return;
     }

  if ((newType->getTypeQualifier() == NA_CHARACTER_TYPE) &&
      (newType->getNominalSize() > CmpCommon::getDefaultNumeric(TRAF_MAX_CHARACTER_COL_LENGTH)))
  {
    *CmpCommon::diags() << DgSqlCode(-4247)
                        << DgInt0(newType->getNominalSize())
                        << DgInt1(CmpCommon::getDefaultNumeric(TRAF_MAX_CHARACTER_COL_LENGTH))
                        << DgColumnName(ToAnsiIdentifier(colName));
    processReturn();
    return;
  }

  const NAFileSet * naFS = naTable->getClusteringIndex();
  const NAColumnArray &naKeyColArr = naFS->getIndexKeyColumns();
  if (naKeyColArr.getColumn(colName))
    {
      // key column cannot be altered
      *CmpCommon::diags() << DgSqlCode(-1420)
                          << DgColumnName(colName);

      processReturn();

      return;
    }
  
  if (naTable->hasSecondaryIndexes())
    {
      const NAFileSetList &naFsList = naTable->getIndexList();

      for (Lng32 i = 0; i < naFsList.entries(); i++)
        {
          naFS = naFsList[i];
          
          // skip clustering index
          if (naFS->getKeytag() == 0)
            continue;

          const NAColumnArray &naIndexColArr = naFS->getAllColumns();
          if (naIndexColArr.getColumn(colName))
            {
              // secondary index column cannot be altered
              *CmpCommon::diags() << DgSqlCode(-1421)
                                  << DgColumnName(colName)
                                  << DgTableName(naFS->getExtFileSetName());

              processReturn();

              return;
            }
        } // for
    } // secondary indexes present

  if ((NOT currType->isCompatible(*newType)) &&
      (NOT ((currType->getTypeQualifier() == NA_CHARACTER_TYPE) &&
            (newType->getTypeQualifier() == NA_CHARACTER_TYPE))))
    {
      NAString reason = "Old and New datatypes must be compatible.";

      // column cannot be altered
      *CmpCommon::diags() << DgSqlCode(-1404)
                          << DgColumnName(colName)
                          << DgString0("altered")
                          << DgString1(reason);
      
      processReturn();
      
      return;
    }
  NABoolean xnWasStartedHere = FALSE;
  // Column that can be altered by updating metadata only
  // must meet these conditions:
  //   -- old and new column datatype must be VARCHAR
  //   -- old and new datatype must have the same nullable attr (if default clause is not used, this sholud not check)
  //   -- new col length must be greater than or equal to old length
  //   -- old and new character sets must be the same
  //   -- old and new upshifted attr must be the same
  //   -- default clause must not be specified
  NABoolean mdAlterOnly = FALSE;
  if ((DFS2REC::isSQLVarChar(currType->getFSDatatype())) &&
      (DFS2REC::isSQLVarChar(newType->getFSDatatype())) &&
      (currType->getFSDatatype() == newType->getFSDatatype()) &&
      (currType->supportsSQLnull() == newType->supportsSQLnull()) &&
      (currType->getNominalSize() <= newType->getNominalSize()) &&
      (((CharType*)currType)->getCharSet() == ((CharType*)newType)->getCharSet()) &&
      (((CharType*)currType)->isUpshifted() == ((CharType*)newType)->isUpshifted()))
  {
    if (NOT pColDef->isColDefaultSpecified())
      mdAlterOnly = TRUE;
    else if (pColDef->isColDefaultSpecified())
    {
      NAString colFamily;
      NAString colName;
      Lng32 datatype, length, precision, scale, dt_start, dt_end, nullable, upshifted;
      ComColumnClass colClass;
      ComColumnDefaultClass defaultClass;
      NAString charset, defVal;
      NAString heading;
      ULng32 hbaseColFlags;
      Int64 colFlags;
      ComLobsStorageType lobStorage;
      NAString compDefnStr;
      if (getColInfo(pColDef,
                     FALSE,
                     colFamily,
                     colName,
                     naTable->isSQLMXAlignedTable(),
                     datatype, length, precision, scale, dt_start, dt_end, 
                     upshifted, nullable,
                     charset, colClass, defaultClass, defVal, heading, 
                     lobStorage, compDefnStr,
                     hbaseColFlags, colFlags))
      {
        processReturn();
        return;
      }

      NAString quotedDefVal;
      if (nacol->getDefaultClass() == defaultClass)
      {
        if (defaultClass == COM_NO_DEFAULT)
          mdAlterOnly = TRUE;
        else if (NOT defVal.isNull())
        {
          ToQuotedString(quotedDefVal, defVal, FALSE);
          if ((strncmp(defVal.data(), nacol->getDefaultValue(), defVal.length())==0))
            mdAlterOnly = TRUE;
        }
        else if (defVal.isNull() && (strcasecmp(nacol->getDefaultValue(), "NULL") == 0))
          mdAlterOnly = TRUE;
      }
    }
  }

  if (isMaintenanceWindowOFF) {
     if (! mdAlterOnly) {
        NAString tmp = "ALTER TABLE ";
        tmp += tabName;
        tmp += " ALTER COLUMN ";
        tmp += colName;
        
        *CmpCommon::diags() << DgSqlCode(-CLI_CANNOT_EXECUTE_DDL)
                          << DgString0(tmp.data());
        processReturn();
     }
  }

  if ((NOT mdAlterOnly) &&
      (CmpCommon::getDefault(TRAF_ALTER_COL_ATTRS) == DF_OFF))
    {
      NAString reason;
      if (NOT ((DFS2REC::isSQLVarChar(currType->getFSDatatype())) &&
               (DFS2REC::isSQLVarChar(newType->getFSDatatype()))))
        reason = "Old and New datatypes must be VARCHAR.";
      else if (currType->getFSDatatype() != newType->getFSDatatype())
        reason = "Old and New datatypes must be the same.";
      else if (((CharType*)currType)->getCharSet() != ((CharType*)newType)->getCharSet())
        reason = "Old and New character sets must be the same.";
      else if (currType->getNominalSize() > newType->getNominalSize())
        reason = "New length must be greater than or equal to old length.";
      else if (currType->supportsSQLnull() != newType->supportsSQLnull())
        reason = "Old and New nullability must be the same.";

      // column cannot be altered
      *CmpCommon::diags() << DgSqlCode(-1404)
                          << DgColumnName(colName)
                          << DgString0("altered")
                          << DgString1(reason);

      processReturn();
      
      return;
    }

  // this operation cannot be done if a xn is already in progress.
  if ((NOT mdAlterOnly) && (xnInProgress(&cliInterface) || (!GetCliGlobals()->currContext()->getTransaction()->autoCommit())))
    {
      *CmpCommon::diags() << DgSqlCode(-20125)
                          << DgString0("This ALTER");

      processReturn();
      return;
    }

  Int64 objUID = naTable->objectUid().castToInt64();

  NABoolean needRestoreIBAttr = naTable->incrBackupEnabled();
  if(needRestoreIBAttr == true) {
     naTable->setIncrBackupEnabled(FALSE);
     dropIBAttrForTable(naTable , &cliInterface);
  }

  // if there are views on the table, save the definition and drop
  // the views.
  // At the end of alter, views will be recreated. If an error happens
  // during view recreation, alter will fail.
  NAList<NAString> viewNameList(STMTHEAP);
  NAList<NAString> viewDefnList(STMTHEAP);
  if (saveAndDropUsingViews(objUID, &cliInterface, viewNameList, viewDefnList))
     {
       //restore the flag before using it
       if(needRestoreIBAttr == true)  {
         naTable->setIncrBackupEnabled(TRUE);
         addIBAttrForTable(naTable, &cliInterface);
         needRestoreIBAttr = false;
      }
      NAString reason = "Error occurred while saving views.";
      *CmpCommon::diags() << DgSqlCode(-1404)
                          << DgColumnName(colName)
                          << DgString0("altered")
                          << DgString1(reason);

      processReturn();
       
      return;
    }

  // Table exists, update the epoch value to lock out other user access
  if (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL))
    {
      retcode = ddlSetObjectEpoch(naTable, STMTHEAP);
      if (retcode < 0)
        {
         //restore the flag before using it
         if(needRestoreIBAttr == true)  {
           naTable->setIncrBackupEnabled(TRUE);
           addIBAttrForTable(naTable, &cliInterface);
           needRestoreIBAttr = false;
          }
          processReturn();
          return;
        }
    }

  CmpCommon::context()->getCliContext()->execDDLOptions() = TRUE;
  if (mdAlterOnly)
    {
      if (mdOnlyAlterColumnAttr
          (catalogNamePart, schemaNamePart, objectNamePart,
           naTable, nacol, newType, alterColNode,
           viewNameList, viewDefnList))
        {
          cliRC = -1;
          
          goto label_error;
        }
    }
  else if (naTable->isSQLMXAlignedTable())
    {
      ElemDDLColDef *pColDef = 
        alterColNode->getColToAlter()->castToElemDDLColDef();
      
      if (alignedFormatTableAlterColumnAttr
          (catalogNamePart, schemaNamePart, objectNamePart,
           naTable,
           colName,
           pColDef,
           alterColNode->ddlXns(),
           viewNameList, viewDefnList))
        {
          cliRC = -1;
          goto label_error;
        }
    }
  else if (hbaseFormatTableAlterColumnAttr
           (catalogNamePart, schemaNamePart, objectNamePart,
            naTable, nacol, newType, alterColNode))
    {
      cliRC = -1;
      
      goto label_error;
    }

  // if sample table exist, drop it and clean the sample infomation in SB_PERSISTENT_SAMPLES
  // after you alter a column data type, and send a warn
  if(isSampleExist(catalogNamePart,
                   schemaNamePart,
                   objUID,
                   sampleTableName,
                   sampleTableUid))
  {
    if (mdAlterOnly)
    {
      if (mdOnlyAlterColumnAttr
          (catalogNamePart, schemaNamePart, sampleTableName,
           naTable, nacol, newType, alterColNode,
           viewNameList, viewDefnList, TRUE, sampleTableUid))
        {
          cliRC = -1;
          goto label_error;
        }
        CorrName sampleCn = CorrName(sampleTableName, STMTHEAP, schemaNamePart, catalogNamePart);
        ActiveSchemaDB()->getNATableDB()->removeNATable(sampleCn,
              ComQiScope::REMOVE_FROM_ALL_USERS, COM_BASE_TABLE_OBJECT,
              alterColNode->ddlXns(), FALSE);
    }
    else
    {
      cliRC = dropSample(catalogNamePart,
                         schemaNamePart,
                         objUID,
                         sampleTableName);
      if (cliRC < 0)
      {
        goto label_error;
      }
      // Warning: recreate sample table
      if(CmpCommon::getDefault(SHOWWARN_OPT) == DF_ON)
        *CmpCommon::diags() << DgSqlCode(1718);
    }
  }

  if (beginXnIfNotInProgress(&cliInterface, xnWasStartedHere,
                             FALSE /*clearDDLList*/, FALSE /*clearObjectLocks*/))
    {
      goto label_error;
    }
  //restore the flag before using it
  if(needRestoreIBAttr == true)  {
    naTable->setIncrBackupEnabled(TRUE);
    addIBAttrForTable(naTable, &cliInterface);
    needRestoreIBAttr = false;
  }
  if (naTable->incrBackupEnabled())
  {
    cliRC = updateSeabaseXDCDDLTable(&cliInterface,
                                     catalogNamePart.data(), schemaNamePart.data(),
                                     objectNamePart.data(), COM_BASE_TABLE_OBJECT_LIT);
    if (cliRC < 0)
    {
      goto label_error;
    }
  }

  cliRC = updateObjectRedefTime(&cliInterface,
                                catalogNamePart, schemaNamePart, objectNamePart,
                                COM_BASE_TABLE_OBJECT_LIT, -1, objUID,
                                naTable->isStoredDesc(), naTable->storedStatsAvail());
  if (cliRC < 0)
    {
      goto label_error;
    }
  
  if (naTable->isStoredDesc() && (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)))
    {
      insertIntoSharedCacheList(catalogNamePart, schemaNamePart, objectNamePart,
                                COM_BASE_TABLE_OBJECT, SharedCacheDDLInfo::DDL_INSERT);
      if (!xnInProgress(&cliInterface))
         finalizeSharedCache();
    }

label_return:
  //restore the flag before using it
  if(needRestoreIBAttr == true)  {
    naTable->setIncrBackupEnabled(TRUE);
    addIBAttrForTable(naTable, &cliInterface);
    needRestoreIBAttr = false;
  }
  CmpCommon::context()->getCliContext()->execDDLOptions() = FALSE;
  endXnIfStartedHere(&cliInterface, xnWasStartedHere, cliRC,
                     FALSE /*modifyEpochs*/, FALSE /*clearObjectLocks*/);

  ActiveSchemaDB()->getNATableDB()->removeNATable
    (cn,
     ComQiScope::REMOVE_FROM_ALL_USERS, COM_BASE_TABLE_OBJECT,
     alterColNode->ddlXns(), FALSE);

  if (!xnInProgress(&cliInterface))
    {
      ddlScope.allClear(); // success; epoch will be incremented on exit
    }

  processReturn();
  
  return;

label_error:
  //restore the flag before using it
  if(needRestoreIBAttr == true)  {
    naTable->setIncrBackupEnabled(TRUE);
    addIBAttrForTable(naTable, &cliInterface);
    needRestoreIBAttr = false;
  }
  CmpCommon::context()->getCliContext()->execDDLOptions() = FALSE;
  endXnIfStartedHere(&cliInterface, xnWasStartedHere, cliRC,
                     FALSE/*modifyEpochs*/, FALSE /*clearObjectLocks*/);

  if ((cliRC = recreateUsingViews(&cliInterface, viewNameList, viewDefnList,
                                  alterColNode->ddlXns())) < 0)
    {
      NAString reason = "Error occurred while recreating views due to dependency on older column definition. Drop dependent views before doing the alter.";
      *CmpCommon::diags() << DgSqlCode(-1404)
                          << DgColumnName(colName)
                          << DgString0("altered")
                          << DgString1(reason);
    }

  ActiveSchemaDB()->getNATableDB()->removeNATable
    (cn,
     ComQiScope::REMOVE_FROM_ALL_USERS, COM_BASE_TABLE_OBJECT,
     alterColNode->ddlXns(), FALSE);

  if (!xnInProgress(&cliInterface)) {
      ddlResetObjectEpochs(TRUE/*doAbort*/, TRUE /*clearList*/);

      // Release all DDL object locks
      LOGTRACE(CAT_SQL_LOCK, "Release all DDL object locks");
      CmpCommon::context()->releaseAllDDLObjectLocks();
  }
  processReturn();

  return;
}

Lng32 CmpSeabaseDDL::updatePKeysTable(ExeCliInterface *cliInterface,
                                     const char* renamedColName,
                                     Int64 objUID,
                                     const char* colName,
                                     NABoolean isIndexColumn)
{
  char buf[1500];
  if (isIndexColumn)
    str_sprintf(buf, "update %s.\"%s\".%s set column_name = '%s' || '@' where object_uid = %ld and column_name = '%s' || '@'",
                     getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_KEYS,
                     renamedColName, objUID, colName);
  else
    str_sprintf(buf, "update %s.\"%s\".%s set column_name = '%s' where object_uid = %ld and column_name = '%s'",
                     getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_KEYS,
                     renamedColName, objUID, colName);
  Lng32 cliRC = cliInterface->executeImmediate(buf);
  if (cliRC < 0)
  {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  return 0;
}

/////////////////////////////////////////////////////////////////////
// this method renames an existing column to the specified new name.
//
// A column cannot be renamed if it is a system, salt, division by,
// replica, computed or a lob column.
// 
// If any index exists on the renamed column, then the index column
// is also renamed.
//
// If views exist on the table, they are dropped and recreated after
// rename. If recreation runs into an error, then alter fails.
//
// This statement runs in a transaction
//
///////////////////////////////////////////////////////////////////
void CmpSeabaseDDL::alterSeabaseTableAlterColumnRename(
     StmtDDLAlterTableAlterColumnRename * alterColNode,
     NAString &currCatName, NAString &currSchName)
{
  Lng32 cliRC = 0;
  Lng32 retcode = 0;

  const NAString &tabName = alterColNode->getTableName();

  ComObjectName tableName(tabName, COM_TABLE_NAME);
  ComAnsiNamePart currCatAnsiName(currCatName);
  ComAnsiNamePart currSchAnsiName(currSchName);
  tableName.applyDefaults(currCatAnsiName, currSchAnsiName);

  const NAString catalogNamePart = tableName.getCatalogNamePartAsAnsiString();
  const NAString schemaNamePart = tableName.getSchemaNamePartAsAnsiString(TRUE);
  const NAString objectNamePart = tableName.getObjectNamePartAsAnsiString(TRUE);
  const NAString extTableName = tableName.getExternalName(TRUE);
  NAString sampleTableName;
  Int64 sampleTableUid = 0;

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, 
  CmpCommon::context()->sqlSession()->getParentQid());

  if ((isSeabaseReservedSchema(tableName)) &&
      (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)))
    {
      *CmpCommon::diags() << DgSqlCode(-CAT_CANNOT_ALTER_DEFINITION_METADATA_SCHEMA);
      processReturn();
      return;
    }

  retcode = lockObjectDDL(alterColNode);
  if (retcode < 0) {
    processReturn();
    return;
  }

  retcode = existsInSeabaseMDTable(&cliInterface, 
                                   catalogNamePart, schemaNamePart, objectNamePart,
                                   COM_BASE_TABLE_OBJECT,
                                   (Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL) 
                                    ? FALSE : TRUE),
                                   TRUE, TRUE);
  if (retcode < 0)
    {
      processReturn();

      return;
    }

  if (retcode == 0) // does not exist
    {
      *CmpCommon::diags() << DgSqlCode(-CAT_CANNOT_ALTER_WRONG_TYPE)
                          << DgString0(extTableName);

      processReturn();

      return;
    }

  ActiveSchemaDB()->getNATableDB()->useCache();

  BindWA bindWA(ActiveSchemaDB(), CmpCommon::context(), TRUE/*inDDL*/);
  CorrName cn(tableName.getObjectNamePart().getInternalName(),
              STMTHEAP,
              tableName.getSchemaNamePart().getInternalName(),
              tableName.getCatalogNamePart().getInternalName());

  NATable *naTable = bindWA.getNATable(cn); 
  if (naTable == NULL || bindWA.errStatus())
    {
      *CmpCommon::diags()
        << DgSqlCode(-4082)
        << DgTableName(cn.getExposedNameAsAnsiString());
    
      processReturn();

      return;
    }

  if (naTable->isHbaseCellTable() || naTable->isHbaseRowTable())
    {
      // not supported
      *CmpCommon::diags() << DgSqlCode(-3242)
                          << DgString0("This feature is not available for an HBase ROW or CELL format table.");
      
      processReturn();
      
      return;          
    }
  
  //////////////////Should move to setupAndErrChecks/////////////
  // /* process hiatus */
  /*
  if (naTable && naTable->incrBackupEnabled())
    {
      hiatusObjectName_ = genHBaseObjName
        (tableName.getCatalogNamePart().getInternalName(),
         tableName.getSchemaNamePart().getInternalName(),
         tableName.getObjectNamePart().getInternalName(),
         naTable->getNamespace());
      
      if (setHiatus(hiatusObjectName_, FALSE))
        {
          // setHiatus has set the diags area
          processReturn();
          
          return;
        }
    }
  */

  // Make sure user has the privilege to perform the alter column
  if (!isDDLOperationAuthorized(SQLOperation::ALTER_TABLE,
                                naTable->getOwner(),naTable->getSchemaOwner(),
                                (int64_t)naTable->objectUid().get_value(), 
                                COM_BASE_TABLE_OBJECT, naTable->getPrivInfo()))
  {
     *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);

     processReturn ();

     return;
  }

  // return an error if trying to alter a column from a volatile table
  if (naTable->isVolatileTable())
    {
      *CmpCommon::diags() << DgSqlCode(-CAT_REGULAR_OPERATION_ON_VOLATILE_OBJECT);
     
      processReturn ();

      return;
    }

  // add X lock
  if (lockRequired(naTable, catalogNamePart, schemaNamePart, objectNamePart, HBaseLockMode::LOCK_X, FALSE/* useHbaseXN */, FALSE/* replSync */, FALSE/* incrementalBackup */, FALSE/* asyncOperation */, TRUE/* noConflictCheck */, TRUE/* registerRegion */))
  {
    processReturn();
    
    return;
  }
  
  const NAColumnArray &nacolArr = naTable->getNAColumnArray();
  const NAString &colName = alterColNode->getColumnName();
  const NAString &renamedColName = alterColNode->getRenamedColumnName();

  const NAColumn * nacol = nacolArr.getColumn(colName);
  if (! nacol)
    {
      // column doesnt exist. Error.
      *CmpCommon::diags() << DgSqlCode(-CAT_COLUMN_DOES_NOT_EXIST_ERROR)
                          << DgColumnName(colName);

      processReturn();

      return;
    }

  if ((CmpCommon::getDefault(TRAF_ALLOW_RESERVED_COLNAMES) == DF_OFF) &&
      (ComTrafReservedColName(renamedColName)))
    {
      NAString reason = "Renamed column " + renamedColName + " is reserved for internal system usage.";
      *CmpCommon::diags() << DgSqlCode(-1404)
                          << DgColumnName(colName)
                          << DgString0("altered")
                          << DgString1(reason);

      processReturn();

      return;
    }

  if (nacol->isComputedColumn() || nacol->isSystemColumn())
    {
      NAString reason = "Cannot rename system or computed column.";
      *CmpCommon::diags() << DgSqlCode(-1404)
                          << DgColumnName(colName)
                          << DgString0("altered")
                          << DgString1(reason);

      processReturn();

      return;
    }

  const NAColumn * renNacol = nacolArr.getColumn(renamedColName);
  if (renNacol)
    {
      // column already exist. Error.
      NAString reason = "Renamed column " + renamedColName + " already exist in the table.";
      *CmpCommon::diags() << DgSqlCode(-1404)
                          << DgColumnName(colName)
                          << DgString0("altered")
                          << DgString1(reason);

      processReturn();

      return;
    }

  const NAType * currType = nacol->getType();

  // If column is a LOB column , error
  if ((currType->getFSDatatype() == REC_BLOB) || (currType->getFSDatatype() == REC_CLOB))
    {
      *CmpCommon::diags() << DgSqlCode(-CAT_LOB_COLUMN_ALTER)
                          << DgColumnName(colName);
      processReturn();
      return;
     }

  // Remove entry from shared cache (if enabled)
  // This is needed so following CLI command won't get the wrong natable
  if (naTable->isStoredDesc())
    {
      int32_t diagsMark = CmpCommon::diags()->mark();
      NAString cliQuery = "alter trafodion metadata shared cache for table ";
      cliQuery += extTableName;
      cliQuery += " delete internal;";

      NAString msg(cliQuery);
      msg.prepend("Removing object for column rename: ");
      QRLogger::log(CAT_SQL_EXE, LL_WARN, "%s", msg.data());

      cliRC = cliInterface.executeImmediate((char*)cliQuery.data());
      // If unable to remove frome shared cache, it may not exist, continue 

      CmpCommon::diags()->rewind(diagsMark);
    }

  const NAFileSet * naFS = naTable->getClusteringIndex();
  const NAColumnArray &naKeyColArr = naFS->getIndexKeyColumns();
  NABoolean isPkeyCol = FALSE;
  if (naKeyColArr.getColumn(colName))
    {
      isPkeyCol = TRUE;
    }

  Int64 objUID = naTable->objectUid().castToInt64();
  
  NAList<NAString> viewNameList(STMTHEAP);
  NAList<NAString> viewDefnList(STMTHEAP);
  if (saveAndDropUsingViews(objUID, &cliInterface, viewNameList, viewDefnList))
    {
      NAString reason = "Error occurred while saving dependent views.";
      *CmpCommon::diags() << DgSqlCode(-1404)
                          << DgColumnName(colName)
                          << DgString0("altered")
                          << DgString1(reason);
      
      processReturn();
       
      return;
    }

  // Table exists, update the epoch value to lock out other user access
  if (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL))
    {
      retcode = ddlSetObjectEpoch(naTable, STMTHEAP);
      if (retcode < 0)
        {
          processReturn();
          return;
        }
    }


  Lng32 colNumber = nacol->getPosition();
  
  char buf[4000];
  str_sprintf(buf, "update %s.\"%s\".%s set column_name = '%s', "
              " column_class = case when column_class in ('" COM_ADDED_USER_COLUMN_LIT"', '" COM_ADDED_ALTERED_USER_COLUMN_LIT"') then '" COM_ADDED_ALTERED_USER_COLUMN_LIT"' else '" COM_ALTERED_USER_COLUMN_LIT"' end where object_uid = %ld and column_number = %d",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_COLUMNS,
              renamedColName.data(),
              objUID,
              colNumber);
  
  cliRC = cliInterface.executeImmediate(buf);
  if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      
      processReturn();
      return;
    }

  if (isPkeyCol)
    {
      Lng32 divColPos = -1;
      if ((naTable->hasDivisioningColumn(&divColPos)) || 
          naTable->hasTrafReplicaColumn())
        {
          NAString reason ;
          if (naTable->hasTrafReplicaColumn()) 
            reason = "Not supported with REPLICATE IN clause.";
          else
            reason = "Not supported with DIVISION BY clause.";

          *CmpCommon::diags() << DgSqlCode(-1404)
                              << DgColumnName(colName)
                              << DgString0("altered")
                              << DgString1(reason);
          
          processReturn();
          
          return;
        }

      Lng32 saltColPos = -1;
      if ((naTable->hasSaltedColumn(&saltColPos)))
        {
          NAString saltText;
          cliRC = getTextFromMD(&cliInterface, objUID, COM_COMPUTED_COL_TEXT,
                                saltColPos, saltText);
          if (cliRC < 0)
            {
              processReturn();
              return;
            }   
          
          // replace col reference with renamed col
          NAString quotedColName = "\"" + colName + "\"";
          NAString renamedQuotedColName = "\"" + renamedColName + "\"";
          saltText = replaceAll(saltText, quotedColName, renamedQuotedColName);
          cliRC = updateTextTable(&cliInterface, objUID, COM_COMPUTED_COL_TEXT,
                                  saltColPos, saltText, NULL, -1, TRUE);
          if (cliRC < 0)
            {
              processReturn();
              return;
            }   
        } // saltCol

      //update KEYS for baseTable
      cliRC = updatePKeysTable(&cliInterface, renamedColName.data(),
                               objUID, colName.data());
      if (cliRC < 0)
      {
        processReturn();
        return;
      }

      //update KEYS for internal pk
      NAString pkName;
      Int64 pkUid;
      //for store by (c1,c2), primary key are (c1,c2,syskey)
      //but it will not be written into "_MD_".constraints
      //as PK
      cliRC = getPKeyInfoForTable(catalogNamePart.data(),
                                  schemaNamePart.data(),
                                  objectNamePart.data(),
                                  &cliInterface,
                                  pkName,
                                  pkUid,
                                  FALSE);
      if (cliRC < 0)
      {
        processReturn();
        return;
      }
     
      if (pkUid > 0)
      {
        cliRC = updatePKeysTable(&cliInterface, renamedColName.data(),
                                 pkUid, colName.data());
        if (cliRC < 0)
        {
          processReturn();
          return;
        }
      }
    } // pkeyCol being renamed

  if (naTable->hasSecondaryIndexes())
    {
      const NAFileSetList &naFsList = naTable->getIndexList();

      for (Lng32 i = 0; i < naFsList.entries(); i++)
        {
          naFS = naFsList[i];
          
          // skip clustering index
          if (naFS->getKeytag() == 0)
            continue;

          str_sprintf(buf, "update %s.\"%s\".%s set column_name = '%s' || '@' where object_uid = %ld and column_name = '%s' || '@'",
                      getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_COLUMNS,
                      renamedColName.data(),
                      naFS->getIndexUID(),
                      colName.data());
  
          cliRC = cliInterface.executeImmediate(buf);
          if (cliRC < 0)
            {
              cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
              
              processReturn();
              return;
            }

         str_sprintf(buf, "update %s.\"%s\".%s set column_name = '%s' where object_uid = %ld and column_name = '%s'",
                      getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_COLUMNS,
                      renamedColName.data(),
                      naFS->getIndexUID(),
                      colName.data());
  
          cliRC = cliInterface.executeImmediate(buf);
          if (cliRC < 0)
            {
              cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
              
              processReturn();
              return;
            }

        //update KEYS
        if (isPkeyCol)
        {
          cliRC = updatePKeysTable(&cliInterface, renamedColName.data(),
                                   naFS->getIndexUID(), colName.data(),
                                   TRUE /*index column*/);
          if (cliRC < 0)
            {
              processReturn();
              return;
            }

          cliRC = updatePKeysTable(&cliInterface, renamedColName.data(),
                                   naFS->getIndexUID(), colName.data());
          if (cliRC < 0)
            {
              processReturn();
              return;
            }
          } // if (isPkeyCol)
        } // for
    } // secondary indexes present

  cliRC = recreateUsingViews(&cliInterface, viewNameList, viewDefnList,
                             alterColNode->ddlXns());
  if (cliRC < 0)
    {
      NAString reason = "Error occurred while recreating views due to dependency on older column definition. Drop dependent views before doing the alter.";
      *CmpCommon::diags() << DgSqlCode(-1404)
                          << DgColumnName(colName)
                          << DgString0("altered")
                          << DgString1(reason);

      processReturn();
      
      return;
    }

  // rename sample table after you rename the base table.
  if(isSampleExist(catalogNamePart,
                   schemaNamePart,
                   objUID,
                   sampleTableName,
                   sampleTableUid))
  {
    char dbuf[sampleTableName.length() + colName.length() + renamedColName.length() + 100];
    str_sprintf(dbuf, "alter table %s alter column %s rename to %s",
                      sampleTableName.data(),
                      colName.data(),
                      renamedColName.data());
    cliRC = cliInterface.executeImmediate(dbuf);
    if (cliRC < 0)
    {
      return;
    }
  }

  if (naTable->incrBackupEnabled())
  {
    cliRC = updateSeabaseXDCDDLTable(&cliInterface,
                                     catalogNamePart.data(), schemaNamePart.data(),
                                     objectNamePart.data(), COM_BASE_TABLE_OBJECT_LIT);
    if (cliRC < 0)
    {
      processReturn();
      return;
    }
  }

  cliRC = updateObjectRedefTime(&cliInterface,
                                catalogNamePart, schemaNamePart, objectNamePart,
                                COM_BASE_TABLE_OBJECT_LIT, -1, objUID,
                                naTable->isStoredDesc(), naTable->storedStatsAvail());
  if (cliRC < 0)
    {
      return;
    }

  if (naTable->isStoredDesc() && (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)))
    {
      insertIntoSharedCacheList(catalogNamePart, schemaNamePart, objectNamePart,
                                COM_BASE_TABLE_OBJECT, SharedCacheDDLInfo::DDL_INSERT);

      // Shouldn't the xn always be in progress?
      if (!xnInProgress(&cliInterface))
         finalizeSharedCache();
    }

  ActiveSchemaDB()->getNATableDB()->removeNATable
    (cn,
     ComQiScope::REMOVE_FROM_ALL_USERS,
     COM_BASE_TABLE_OBJECT,
     alterColNode->ddlXns(), FALSE);
  
  processReturn();
  
  return;
}

// ----------------------------------------------------------------------------
// alterSeabaseTableAddPKeyConstraint
//    ALTER TABLE <table> ADD CONSTRAINT <name> PRIMARY KEY (<cols>)
//
// Creates a PK constraint (empty tables only) or
// Creates a unique constraint (TRAF_ALTER_ADD_PKEY_AS_UNIQUE_CONSTRAINT is ON)
//
// This statement is not run in a transaction by default.
// It can be run in a user transaction
// ----------------------------------------------------------------------------
void CmpSeabaseDDL::alterSeabaseTableAddPKeyConstraint(
                                                       StmtDDLAddConstraint * alterAddConstraint,
                                                       NAString &currCatName, NAString &currSchName)
{
  Lng32 cliRC = 0;
  Lng32 cliRC2 = 0;
  Lng32 retcode = 0;

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, 
                               CmpCommon::context()->sqlSession()->getParentQid());

  ExpHbaseInterface * ehi = allocEHI(COM_STORAGE_HBASE);
  if (ehi == NULL)
    {
      processReturn();
      return;
    }

  NAString tabName = alterAddConstraint->getTableName();
  NAString catalogNamePart, schemaNamePart, objectNamePart;
  NAString extTableName, extNameForHbase;
  NATable * naTable = NULL;
  CorrName cn;

  retcode = lockObjectDDL(alterAddConstraint);
  if (retcode < 0) {
    processReturn();
    return;
  }

  retcode = 
    setupAndErrorChecks(tabName, 
                        alterAddConstraint->getOrigTableNameAsQualifiedName(),
                        currCatName, currSchName,
                        catalogNamePart, schemaNamePart, objectNamePart,
                        extTableName, extNameForHbase, cn,
                        &naTable, 
                        FALSE, FALSE,
                        &cliInterface,
                        COM_BASE_TABLE_OBJECT,
                        SQLOperation::ALTER_TABLE);
  if (retcode < 0)
    {
      processReturn();

      return;
    }

  // if table already has a clustering or primary key, return error.
  if ((naTable->getClusteringIndex()) && 
      (NOT naTable->getClusteringIndex()->hasOnlySyskey()))
    {
      *CmpCommon::diags()
        << DgSqlCode(-1256)
        << DgString0(extTableName);
      
      processReturn();
      
      return;
    }

  ElemDDLColRefArray &keyColumnArray = alterAddConstraint->getConstraint()->castToElemDDLConstraintPK()->getKeyColumnArray();

  NAList<NAString> keyColList(HEAP, keyColumnArray.entries());
  NAString pkeyColsStr;
  if (alterAddConstraint->getConstraint()->castToElemDDLConstraintPK()->isNullableSpecified())
    pkeyColsStr += " NULLABLE ";
  pkeyColsStr += "(";
  for (Int32 j = 0; j < keyColumnArray.entries(); j++)
    {
      const NAString &colName = keyColumnArray[j]->getColumnName();
      keyColList.insert(colName);

      pkeyColsStr += ToAnsiIdentifier(colName);
      if (keyColumnArray[j]->getColumnOrdering() == COM_DESCENDING_ORDER)
        pkeyColsStr += " DESC";
      else
        pkeyColsStr += " ASC";

      if (j < (keyColumnArray.entries() - 1))
        pkeyColsStr += ", ";
      
    }
  pkeyColsStr += ")";

  if (constraintErrorChecks(&cliInterface,
                            alterAddConstraint->castToStmtDDLAddConstraintPK(),
                            naTable,
                            COM_UNIQUE_CONSTRAINT, //TRUE, 
                            keyColList))
    {
      return;
    }

  // update primary key constraint info
  NAString uniqueStr;
  if (genUniqueName(alterAddConstraint, NULL, uniqueStr))
    {
      return;
    }

  // find out if this table has dependent objects (views, user indexes,
  // unique constraints, referential constraints)
  NABoolean dependentObjects = FALSE;
  Queue * usingViewsQueue = NULL;
  cliRC = getUsingViews(&cliInterface, naTable->objectUid().castToInt64(), 
                        usingViewsQueue);
  if (cliRC < 0)
    {
      processReturn();
      
      return;
    }
  
  if ((naTable->hasSecondaryIndexes()) || // user indexes
      (naTable->getUniqueConstraints().entries() > 0) || // unique constraints
      (naTable->getRefConstraints().entries() > 0) || // ref constraints
      (usingViewsQueue->entries() > 0) ||
      (naTable->hasTrigger()))  // exists trigger
    {
      dependentObjects = TRUE;
    }

  // create a true primary key by drop/create/populate of table.
  NABoolean isEmpty = FALSE;

  HbaseStr hbaseTable;
  hbaseTable.val = (char*)extNameForHbase.data();
  hbaseTable.len = extNameForHbase.length();
  
  retcode = ehi->isEmpty(hbaseTable);
  if (retcode < 0)
    {
      *CmpCommon::diags()
        << DgSqlCode(-8448)
        << DgString0((char*)"ExpHbaseInterface::isEmpty()")
        << DgString1(getHbaseErrStr(-retcode))
        << DgInt0(-retcode)
        << DgString2((char*)GetCliGlobals()->getJniErrorStr());
      
      if (!xnInProgress (&cliInterface)) {
        ddlResetObjectEpochs(TRUE/*doAbort*/, TRUE /*clearList*/);

        // Release all DDL object locks
        LOGTRACE(CAT_SQL_LOCK, "Release all DDL object locks");
        CmpCommon::context()->releaseAllDDLObjectLocks();
      }

      deallocEHI(ehi);
      
      processReturn();
      
      return;
    }

  isEmpty = (retcode == 1);

  NABoolean addPkeyAsUniqConstr = 
    CmpCommon::getDefault(TRAF_ALTER_ADD_PKEY_AS_UNIQUE_CONSTRAINT) == DF_ON;

  // currently if a table has lob cols, pkey is created as
  // a unique constr irrespective of whether the table is empty or not. 
  // This is needed as there is some cache related issue that causes
  // incorrect behavior if it is re-created as a true pkey.
  // Once the underlying issue is fixed, we can revert this.
  if (naTable->lobV2())
    addPkeyAsUniqConstr = TRUE;

  // Table exists, update the epoch value to lock out other user access
  if (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL))
    {
      retcode = ddlSetObjectEpoch(naTable, STMTHEAP);
      if (retcode < 0)
        {
          processReturn();
          return;
        }
    }

  NABoolean needRestoreIBAttr = naTable->incrBackupEnabled();
  if(needRestoreIBAttr == true) {
     naTable->setIncrBackupEnabled(FALSE);
     dropIBAttrForTable(naTable , &cliInterface);
  }

  // If cqd is set to create pkey as a unique constraint, then do that.
  // otherwise if table has dependent objects, return error.
  // Users need to drop them before adding primary key
  if (addPkeyAsUniqConstr)
    {
      // Remove entry from shared cache
      if (naTable->isStoredDesc())
        {
          int32_t diagsMark = CmpCommon::diags()->mark();
          deleteFromSharedCache (&cliInterface,
                                 naTable->getTableName().getQualifiedNameAsAnsiString(),
                                 COM_BASE_TABLE_OBJECT,
                                 "add primary key column as unique constraint");

          // If unable to remove from shared cache, it may not exist, continue 
          // If the operation is later rolled back, this table will not be added
          // back to shared cache - TBD
          CmpCommon::diags()->rewind(diagsMark);
        }

      // either dependent objects or cqd set to create unique constraint.
      // cannot create clustered primary key constraint.
      // create a unique constraint instead.
      NAString cliQuery;
      cliQuery = "alter table " + extTableName + " add constraint " + uniqueStr
        + " unique " + pkeyColsStr + ";";
      cliRC = cliInterface.executeImmediate((char*)cliQuery.data());
      if (cliRC < 0)
        {
          cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
        }
      
      if (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL))
        {
          // Update shared cache
          if (naTable->isStoredDesc())
            {
              insertIntoSharedCacheList(catalogNamePart, schemaNamePart, objectNamePart,
                                        COM_BASE_TABLE_OBJECT, SharedCacheDDLInfo::DDL_UPDATE);
              if (!xnInProgress (&cliInterface))
                  finalizeSharedCache();
            }

          // remove NATable for this table and send QI keys
          ActiveSchemaDB()->getNATableDB()->removeNATable
            (cn,
             ComQiScope::REMOVE_FROM_ALL_USERS, 
             COM_BASE_TABLE_OBJECT,
             alterAddConstraint->ddlXns(), FALSE);
      
          // if no transaction - release DDL lock
          if (!xnInProgress (&cliInterface)) {
            ddlResetObjectEpochs(FALSE/*doAbort*/, TRUE /*clearList*/);

            // Release all DDL object locks
            LOGTRACE(CAT_SQL_LOCK, "Release all DDL object locks");
            CmpCommon::context()->releaseAllDDLObjectLocks();
          }
        }
      //restore the flag before return 
      if(needRestoreIBAttr == true)  {
       naTable->setIncrBackupEnabled(TRUE);
       addIBAttrForTable(naTable, &cliInterface);
       needRestoreIBAttr = false;
      }
      return;
    }
  else if (dependentObjects)
    {
      // error
      *CmpCommon::diags() << DgSqlCode(-3242) 
                          << DgString0("Cannot alter/add primary key constraint on a table with dependencies. Drop all dependent objects (views, indexes, unique, trigger and referential constraints) on the specified table and recreate them after adding the primary key.");

      if (!xnInProgress (&cliInterface)) {
        ddlResetObjectEpochs(TRUE/*doAbort*/, TRUE /*clearList*/);

        // Release all DDL object locks
        LOGTRACE(CAT_SQL_LOCK, "Release all DDL object locks");
        CmpCommon::context()->releaseAllDDLObjectLocks();
      }
      //restore the flag before return 
      if(needRestoreIBAttr == true)  {
       naTable->setIncrBackupEnabled(TRUE);
       addIBAttrForTable(naTable, &cliInterface);
       needRestoreIBAttr = false;
      }
      return;
    }
  
  Int64 tableUID = 
    getObjectUID(&cliInterface,
                 catalogNamePart.data(), schemaNamePart.data(), objectNamePart.data(),
                 COM_BASE_TABLE_OBJECT_LIT);

  NAString clonedTable;
  if (NOT isEmpty) // non-empty table
    {
      // clone as a temp table and truncate source table
      CmpCommon::context()->getCliContext()->execDDLOptions() = TRUE;
      cliRC = cloneAndTruncateTable(naTable, clonedTable, ehi, &cliInterface);
      CmpCommon::context()->getCliContext()->execDDLOptions() = FALSE;
      if (cliRC < 0)
        {
          if (!xnInProgress (&cliInterface))
            {
              ddlResetObjectEpochs(TRUE/*doAbort*/, TRUE /*clearList*/);
              CmpCommon::context()->ddlObjsList().clear();

              // Release all DDL object locks
              LOGTRACE(CAT_SQL_LOCK, "Release all DDL object locks");
              CmpCommon::context()->releaseAllDDLObjectLocks();
            }
          //restore the flag before return 
          if(needRestoreIBAttr == true)  {
           naTable->setIncrBackupEnabled(TRUE);
           addIBAttrForTable(naTable, &cliInterface);
           needRestoreIBAttr = false;
          }
          return; // diags already populated by called method
        }
    }

  // Drop and recreate it with the new primary key.
  NAString pkeyName;
  if (NOT alterAddConstraint->getConstraintName().isNull())
    {
      pkeyName = alterAddConstraint->getConstraintName();
    }

  char * buf = NULL;
  ULng32 buflen = 0;
  retcode = CmpDescribeSeabaseTable(cn, 3/*createlike*/, buf, buflen, STMTHEAP, FALSE,
                                    (pkeyName.isNull() ? NULL : pkeyName.data()),
                                    pkeyColsStr.data(), TRUE);
  if (retcode)
    {
      if (!xnInProgress (&cliInterface)) {
        ddlResetObjectEpochs(TRUE/*doAbort*/, TRUE /*clearList*/);

        // Release all DDL object locks
        LOGTRACE(CAT_SQL_LOCK, "Release all DDL object locks");
        CmpCommon::context()->releaseAllDDLObjectLocks();
      }
      //restore the flag before return 
      if(needRestoreIBAttr == true)  {
       naTable->setIncrBackupEnabled(TRUE);
       addIBAttrForTable(naTable, &cliInterface);
       needRestoreIBAttr = false;
      }
      return;
    }

  NABoolean done = FALSE;
  Lng32 curPos = 0;
  
  NAString cliQuery;

  char cqdbuf[200];
  NABoolean xnWasStartedHere = FALSE;
  
  str_sprintf(cqdbuf, "cqd traf_hbase_drop_create '0';");
  cliRC = cliInterface.executeImmediate(cqdbuf);
  if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      goto label_return;
    }

  if (beginXnIfNotInProgress(&cliInterface, xnWasStartedHere,
                             FALSE /*clearDDLList*/, FALSE /*clearObjectLocks*/))
    {
      goto label_return;
    }

  // Remove entry from shared cache (if enabled)
  // This is needed so following CLI command won't get the wrong natable
  if (naTable->isStoredDesc())
    {
      int32_t diagsMark = CmpCommon::diags()->mark();
      cliQuery = "alter trafodion metadata shared cache for table ";
      cliQuery += extTableName;
      cliQuery += " delete internal;";

      NAString msg(cliQuery);
      msg.prepend("Removing object for add PK constraint: ");
      QRLogger::log(CAT_SQL_EXE, LL_WARN, "%s", msg.data());

      cliRC = cliInterface.executeImmediate((char*)cliQuery.data());
      // If unable to remove frome shared cache, it may not exist, continue 

      CmpCommon::diags()->rewind(diagsMark);
    }

  // drop this table from metadata.
  cliQuery = "drop table ";
  cliQuery += extTableName;
  cliQuery += " no check;";
  cliRC = cliInterface.executeImmediate((char*)cliQuery.data());
  if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

      goto label_return;
    }

  str_sprintf(cqdbuf, "cqd traf_create_table_with_uid '%ld';",
              tableUID);
  cliRC = cliInterface.executeImmediate(cqdbuf);
  if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      
      goto label_return;
    }

  // and recreate it with the new primary key.
  cliQuery = "create table ";
  cliQuery += extTableName;
  cliQuery += " ";

  while (NOT done)
    {
      short len = *(short*)&buf[curPos];
      NAString frag(&buf[curPos+sizeof(short)],
                    len - ((buf[curPos+len-1]== '\n') ? 1 : 0));

      cliQuery += frag;
      curPos += ((((len+sizeof(short))-1)/8)+1)*8;

      if (curPos >= buflen)
        done = TRUE;
    }

  cliRC = cliInterface.executeImmediate((char*)cliQuery.data());
  if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

      goto label_return;
    }

  str_sprintf(cqdbuf, "cqd traf_create_table_with_uid '' ;");
  cliInterface.executeImmediate(cqdbuf);

  str_sprintf(cqdbuf, "cqd traf_hbase_drop_create '1';");
  cliInterface.executeImmediate(cqdbuf);

  if (NOT isEmpty) // non-empty table
    {
      // remove NATable so current definition could be loaded
      ActiveSchemaDB()->getNATableDB()->removeNATable
        (cn,
         ComQiScope::REMOVE_FROM_ALL_USERS, 
         COM_BASE_TABLE_OBJECT, 
         alterAddConstraint->ddlXns(), FALSE);

      // copy tempTable data into newly created table
      str_sprintf(buf, "insert with no rollback into %s select * from %s",
                  naTable->getTableName().getQualifiedNameAsAnsiString().data(),
                  clonedTable.data());
      cliRC = cliInterface.executeImmediate(buf);
      if (cliRC < 0)
        {
          cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

          goto label_restore;
        }
    }

  //restore the flag before using it
  if(needRestoreIBAttr == true)  {
    naTable->setIncrBackupEnabled(TRUE);
    addIBAttrForTable(naTable, &cliInterface);
    needRestoreIBAttr = false;
  }

  if (naTable->incrBackupEnabled())
  {
    cliRC = updateSeabaseXDCDDLTable(&cliInterface,
                                     catalogNamePart.data(), schemaNamePart.data(),
                                     objectNamePart.data(), COM_BASE_TABLE_OBJECT_LIT);
    if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      goto label_restore;
    }
  }

  if (updateObjectRedefTime(&cliInterface,
                            catalogNamePart, schemaNamePart, objectNamePart,
                            COM_BASE_TABLE_OBJECT_LIT, -1, tableUID,
                            naTable->isStoredDesc(), naTable->storedStatsAvail()))
    {
      processReturn();

      goto label_return;
    }

  if (naTable->isStoredDesc() && (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)))
    {
      insertIntoSharedCacheList(catalogNamePart, schemaNamePart, objectNamePart,
                                COM_BASE_TABLE_OBJECT, SharedCacheDDLInfo::DDL_INSERT);
    }

  endXnIfStartedHere(&cliInterface, xnWasStartedHere, 0,
                     FALSE /*modifyEpochs*/, FALSE /*clearObjectLocks*/);

  if (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL))
    {
      // remove NATable for this table
      ActiveSchemaDB()->getNATableDB()->removeNATable
        (cn,
         ComQiScope::REMOVE_FROM_ALL_USERS, 
         COM_BASE_TABLE_OBJECT,
         alterAddConstraint->ddlXns(), FALSE);
    }

  if (NOT clonedTable.isNull())
    {
      str_sprintf(buf, "drop table %s", clonedTable.data());
      cliRC = cliInterface.executeImmediate(buf);
      if (cliRC < 0)
        {
          cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
          goto label_restore;
        }
    }

  cliRC = 0;
  // normal return

label_restore:
  endXnIfStartedHere(&cliInterface, xnWasStartedHere, -1,
                     FALSE /*modifyEpoch*/, FALSE /*clearObjectLocks*/);
  
  ActiveSchemaDB()->getNATableDB()->removeNATable
    (cn,
     ComQiScope::REMOVE_FROM_ALL_USERS, 
     COM_BASE_TABLE_OBJECT, FALSE, FALSE);

  CmpCommon::context()->getCliContext()->execDDLOptions() = TRUE;
  if ((cliRC < 0) && 
      (NOT clonedTable.isNull()) &&
      (cloneSeabaseTable(clonedTable, -1,
                         naTable->getTableName().getQualifiedNameAsAnsiString(),
                         naTable,
                         ehi, &cliInterface, FALSE)))
    {
      cliRC = -1;
      goto label_drop;
    }
  
label_drop:
  CmpCommon::context()->getCliContext()->execDDLOptions() = FALSE;
  endXnIfStartedHere(&cliInterface, xnWasStartedHere, -1,
                     FALSE /*modifyEpochs*/, FALSE /*clearObjectLocks*/);
 
  if ((cliRC < 0) && 
      (NOT clonedTable.isNull()))
    {
      str_sprintf(buf, "drop table %s", clonedTable.data());
      cliRC2 = cliInterface.executeImmediate(buf);
    }

  deallocEHI(ehi); 

label_return:
  //restore the flag before return 
  if(needRestoreIBAttr == true)  {
    naTable->setIncrBackupEnabled(TRUE);
    addIBAttrForTable(naTable, &cliInterface);
  }
  endXnIfStartedHere(&cliInterface, xnWasStartedHere, -1,
                     FALSE/*modifyepochs*/, FALSE /*clearObjectLocks*/);


  str_sprintf(cqdbuf, "cqd traf_create_table_with_uid '' ;");
  cliInterface.executeImmediate(cqdbuf);
  
  str_sprintf(cqdbuf, "cqd traf_hbase_drop_create '1';");
  cliInterface.executeImmediate(cqdbuf);

  NABoolean doAbort = (cliRC < 0) ? TRUE : FALSE;
  ddlResetObjectEpochs(doAbort/*doAbort*/, TRUE /*clearList*/);

  // Release all DDL object locks
  LOGTRACE(CAT_SQL_LOCK, "Release all DDL object locks");
  CmpCommon::context()->releaseAllDDLObjectLocks();
  return;
}

// ----------------------------------------------------------------------------
// alterSeabaseTableAddUniqueConstraint
//    ALTER TABLE <table> ADD CONSTRAINT <name> UNIQUE (<cols>)
//
// This statement runs in a transaction
// This statement may recreate the table, so it employs a read and read/write
// DDL lock.
// ----------------------------------------------------------------------------
void CmpSeabaseDDL::alterSeabaseTableAddUniqueConstraint(
                                                StmtDDLAddConstraint * alterAddConstraint,
                                                NAString &currCatName, NAString &currSchName)
{
  Lng32 cliRC = 0;
  Lng32 retcode = 0;

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, 
                               CmpCommon::context()->sqlSession()->getParentQid());

  NAString tabName = alterAddConstraint->getTableName();
  NAString catalogNamePart, schemaNamePart, objectNamePart;
  NAString extTableName, extNameForHbase;
  NATable * naTable = NULL;
  CorrName cn;

  retcode = lockObjectDDL(alterAddConstraint);
  if (retcode < 0) {
    processReturn();
    return;
  }

  retcode = 
    setupAndErrorChecks(tabName, 
                        alterAddConstraint->getOrigTableNameAsQualifiedName(),
                        currCatName, currSchName,
                        catalogNamePart, schemaNamePart, objectNamePart,
                        extTableName, extNameForHbase, cn,
                        &naTable, 
                        FALSE, FALSE,
                        &cliInterface,
                        COM_BASE_TABLE_OBJECT,
                        SQLOperation::ALTER_TABLE);
  if (retcode < 0)
    {
      processReturn();
      return;
    }

  //if partition table lock every partition
  if (naTable->isPartitionV2Table())
  {
    retcode = lockObjectDDL(naTable, catalogNamePart, schemaNamePart, TRUE);
    if (retcode < 0)
    {
      processReturn();
      return;
    }
  }

  // add X lock
  if (lockRequired(naTable, catalogNamePart, schemaNamePart, objectNamePart, HBaseLockMode::LOCK_X, FALSE/* useHbaseXN */, FALSE/* replSync */, FALSE/* incrementalBackup */, FALSE/* asyncOperation */, TRUE/* noConflictCheck */, TRUE/* registerRegion */))
  {
    processReturn();
    
    return;
  }
  
  ElemDDLColRefArray &keyColumnArray = alterAddConstraint->getConstraint()->castToElemDDLConstraintUnique()->getKeyColumnArray();

  NAList<NAString> keyColList(HEAP, keyColumnArray.entries());
  NAList<NAString> keyColOrderList(HEAP, keyColumnArray.entries());
  for (Int32 j = 0; j < keyColumnArray.entries(); j++)
    {
      const NAString &colName = keyColumnArray[j]->getColumnName();
      keyColList.insert(colName);

      if (keyColumnArray[j]->getColumnOrdering() == COM_DESCENDING_ORDER)
        keyColOrderList.insert("DESC");
      else
        keyColOrderList.insert("ASC");
      
    }

  if (constraintErrorChecks(
                            &cliInterface,
                            alterAddConstraint->castToStmtDDLAddConstraintUnique(),
                            naTable,
                            COM_UNIQUE_CONSTRAINT, 
                            keyColList))
    {
      return;
    }

  // create unnamed constraints in deterministic format if table was 
  // created for xDC replication. 
  Int32 constrNum = ((naTable->xnRepl() != COM_REPL_NONE || naTable->incrBackupEnabled()) ? 
                     getNextUnnamedConstrNum(naTable) : 0);
  // update unique key constraint info
  NAString uniqueStr;
  if (genUniqueName(alterAddConstraint, &constrNum, uniqueStr))
    {
      return;
    }
  
  // Get a read only DDL lock on table
  if (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL))
    {
      retcode = ddlSetObjectEpoch(naTable, STMTHEAP, true /*readsAllowed*/);
      if (retcode < 0)
        {
          processReturn();
          return;
        }
    }

  Int64 tableUID = 
    getObjectUID(&cliInterface,
                 catalogNamePart.data(), schemaNamePart.data(), objectNamePart.data(),
                 COM_BASE_TABLE_OBJECT_LIT);

  ComUID comUID;
  comUID.make_UID();
  Int64 uniqueUID = comUID.get_value();

  if (updateConstraintMD(keyColList, keyColOrderList, uniqueStr, tableUID, uniqueUID, 
                         naTable, COM_UNIQUE_CONSTRAINT, TRUE, &cliInterface))
    {
      *CmpCommon::diags()
        << DgSqlCode(-1043)
        << DgTableName(uniqueStr);

      return;
    }
  //before doing internal DDL , turn off IB attribute to not record in binlog
  NABoolean needRestoreIBAttr = naTable->incrBackupEnabled();
  if(needRestoreIBAttr == true && NOT naTable->isPartitionV2Table()) {
     naTable->setIncrBackupEnabled(FALSE);
     dropIBAttrForTable(naTable , &cliInterface);
  }

  NAList<NAString> emptyKeyColList(STMTHEAP);
  if (updateIndexInfo(keyColList,
                      keyColOrderList,
                      emptyKeyColList,
                      uniqueStr,
                      uniqueUID,
                      catalogNamePart, schemaNamePart, objectNamePart,
                      naTable,
                      TRUE,
                      (CmpCommon::getDefault(TRAF_NO_CONSTR_VALIDATION) == DF_ON),
                      TRUE,
                      FALSE,
                      &cliInterface))
    {
      *CmpCommon::diags()
        << DgSqlCode(-1029)
        << DgTableName(uniqueStr);
      //restore the flag before using it
      if(needRestoreIBAttr == true)  {
        naTable->setIncrBackupEnabled(TRUE);
        addIBAttrForTable(naTable, &cliInterface);
      }
      return;
    }

  //restore the flag before using it
  if(needRestoreIBAttr == true && NOT naTable->isPartitionV2Table())  {
    naTable->setIncrBackupEnabled(TRUE);
    addIBAttrForTable(naTable, &cliInterface);
    needRestoreIBAttr = false;
  }

  // Upgrade DDL lock to read/write
  if (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL))
    {
      retcode = ddlSetObjectEpoch(naTable, STMTHEAP, false /*readsAllowed*/, true /*continue*/);
      if (retcode < 0)
        {
          processReturn();
          return;
        }
    }

  if (naTable->incrBackupEnabled())
  {
    retcode = updateSeabaseXDCDDLTable(&cliInterface,
                                       catalogNamePart.data(), schemaNamePart.data(),
                                       objectNamePart.data(), COM_BASE_TABLE_OBJECT_LIT);
    if (retcode < 0)
    {
      processReturn();
      return;
    }
  }

  if (updateObjectRedefTime(&cliInterface,
                            catalogNamePart, schemaNamePart, objectNamePart,
                            COM_BASE_TABLE_OBJECT_LIT, -1, tableUID,
                            naTable->isStoredDesc(), naTable->storedStatsAvail()))
    {
      processReturn();

      return;
    }

  if (naTable->isStoredDesc() && (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)))
    {
      insertIntoSharedCacheList(catalogNamePart, schemaNamePart, objectNamePart,
                                COM_BASE_TABLE_OBJECT, SharedCacheDDLInfo::DDL_UPDATE);
    }

  // remove NATable for this table
  ActiveSchemaDB()->getNATableDB()->removeNATable
    (cn,
     ComQiScope::REMOVE_FROM_ALL_USERS, 
     COM_BASE_TABLE_OBJECT,
     alterAddConstraint->ddlXns(), FALSE);

  if (naTable->isPartitionV2Table())
  {
    RemoveNATableParam removeNATableParam(ComQiScope::REMOVE_FROM_ALL_USERS,
                                          alterAddConstraint->ddlXns(), FALSE);
    UInt32 operation = 0;
    if (naTable->isStoredDesc())
    {
      setCacheAndStoredDescOp(operation, UPDATE_REDEF_TIME);
      setCacheAndStoredDescOp(operation, UPDATE_SHARED_CACHE);
    }
    setCacheAndStoredDescOp(operation, REMOVE_NATABLE);

    const NAPartitionArray &partArray = naTable->getNAPartitionArray();
    for (int i = 0; i < partArray.entries(); i++)
    {
      if (partArray[i]->hasSubPartition())
      {
        const NAPartitionArray *subPartAry = partArray[i]->getSubPartitions();
        for (int j = 0; j < subPartAry->entries(); j++)
        {
          UpdateObjRefTimeParam updObjRefParam(-1, (*subPartAry)[j]->getPartitionUID(),
                                               naTable->isStoredDesc(), naTable->storedStatsAvail());
          cliRC = updateCachesAndStoredDesc(&cliInterface, catalogNamePart,
                                            schemaNamePart, NAString((*subPartAry)[j]->getPartitionEntityName()),
                                            COM_BASE_TABLE_OBJECT, operation, &removeNATableParam,
                                            &updObjRefParam, SharedCacheDDLInfo::DDL_UPDATE);
          if (cliRC < 0)
          {
            processReturn();
            return;
          }
        }
      }
      else
      {
        UpdateObjRefTimeParam updObjRefParam(-1, partArray[i]->getPartitionUID(),
                                             naTable->isStoredDesc(), naTable->storedStatsAvail());
        cliRC = updateCachesAndStoredDesc(&cliInterface, catalogNamePart,
                                          schemaNamePart, NAString(partArray[i]->getPartitionEntityName()),
                                          COM_BASE_TABLE_OBJECT, operation, &removeNATableParam,
                                          &updObjRefParam, SharedCacheDDLInfo::DDL_UPDATE);
        if (cliRC < 0)
        {
          processReturn();
          return;
        }
      }
    }
  }

  return;
}

// returns 1 if referenced table refdTable has a dependency on the 
// original referencing table origRingTable.
// return 0, if it does not.
// return -1, if error.
short CmpSeabaseDDL::isCircularDependent(CorrName &ringTable,
                                         CorrName &refdTable,
                                         CorrName &origRingTable,
                                         BindWA *bindWA)
{
  // get natable for the referenced table.
  NATable *naTable = bindWA->getNATable(refdTable); 
  if (naTable == NULL || bindWA->errStatus())
    {
      *CmpCommon::diags() << DgSqlCode(-CAT_OBJECT_DOES_NOT_EXIST_IN_TRAFODION)
                          << DgString0(naTable ? naTable->getTableName().getQualifiedNameAsString() : "");
      
      processReturn();
      
      return -1;
    }
  
  // find all the tables the refdTable depends on.
  const AbstractRIConstraintList &refList = naTable->getRefConstraints();
  
  for (Int32 i = 0; i < refList.entries(); i++)
    {
      AbstractRIConstraint *ariConstr = refList[i];
      
      if (ariConstr->getOperatorType() != ITM_REF_CONSTRAINT)
        continue;

      RefConstraint * refConstr = (RefConstraint*)ariConstr;
      if (refConstr->selfRef())
        continue;

      CorrName cn(refConstr->getUniqueConstraintReferencedByMe().getTableName());
      if (cn == origRingTable)
        {
          return 1; // dependency exists
        }
      short rc = isCircularDependent(cn, cn, 
                                     origRingTable, bindWA);
      if (rc)
        return rc;

    } // for

  return 0;
}

// ----------------------------------------------------------------------------
// alterSeabaseTableAddRIConstraint
//    ALTER TABLE <table> ADD CONSTRAINT <name> FORIEGN KEY (<cols>)
//        REFERENCES <table1> (<cols>)
//
// This statement runs in a transaction
// This statement may recreate the table, so it employs a read and read/write
// DDL lock.
// ----------------------------------------------------------------------------
void CmpSeabaseDDL::alterSeabaseTableAddRIConstraint(
                                                StmtDDLAddConstraint * alterAddConstraint,
                                                NAString &currCatName, NAString &currSchName)
{
  Lng32 cliRC = 0;
  Lng32 retcode = 0;

  const NAString &tabName = alterAddConstraint->getTableName();

  ComObjectName referencingTableName(tabName, COM_TABLE_NAME);
  ComAnsiNamePart currCatAnsiName(currCatName);
  ComAnsiNamePart currSchAnsiName(currSchName);
  referencingTableName.applyDefaults(currCatAnsiName, currSchAnsiName);

  const NAString catalogNamePart = referencingTableName.getCatalogNamePartAsAnsiString();
  const NAString schemaNamePart = referencingTableName.getSchemaNamePartAsAnsiString(TRUE);
  const NAString objectNamePart = referencingTableName.getObjectNamePartAsAnsiString(TRUE);
  const NAString extTableName = referencingTableName.getExternalName(TRUE);

  if ((isSeabaseReservedSchema(referencingTableName)) &&
      (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)))
    {
      *CmpCommon::diags() << DgSqlCode(-CAT_CANNOT_ALTER_DEFINITION_METADATA_SCHEMA);
      processReturn();
      return;
    }

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, 
  CmpCommon::context()->sqlSession()->getParentQid());

  retcode = lockObjectDDL(alterAddConstraint);
  if (retcode < 0) {
    processReturn();
    return;
  }

  retcode = existsInSeabaseMDTable(&cliInterface, 
                                   catalogNamePart, schemaNamePart, objectNamePart,
                                   COM_BASE_TABLE_OBJECT,
                                   (Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL) 
                                    ? FALSE : TRUE),
                                   TRUE, TRUE);
  if (retcode < 0)
    {
      processReturn();

      return;
    }

  BindWA bindWA(ActiveSchemaDB(), CmpCommon::context(), TRUE/*inDDL*/);
  CorrName cn(referencingTableName.getObjectNamePart().getInternalName(),
              STMTHEAP,
              referencingTableName.getSchemaNamePart().getInternalName(),
              referencingTableName.getCatalogNamePart().getInternalName());

  NATable *ringNaTable = bindWA.getNATable(cn); 
  if (ringNaTable == NULL || bindWA.errStatus())
    {
      *CmpCommon::diags()
        << DgSqlCode(-4082)
        << DgTableName(cn.getExposedNameAsAnsiString());

      processReturn();
      
      return;
    }

  // Make sure user has the privilege to perform the add RI constraint
  if (!isDDLOperationAuthorized(SQLOperation::ALTER_TABLE,
                                ringNaTable->getOwner(),ringNaTable->getSchemaOwner(),
                                (int64_t)ringNaTable->objectUid().get_value(), 
                                COM_BASE_TABLE_OBJECT, ringNaTable->getPrivInfo()))
  {
     *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);

     processReturn ();

     return;
  }

  if (ringNaTable->isHbaseMapTable())
    {
      // not supported
      *CmpCommon::diags() << DgSqlCode(-3242)
                          << DgString0("This alter option cannot be used for an HBase mapped table.");

      processReturn();
      
      return;
    }

  const ElemDDLConstraintRI *constraintNode = 
    alterAddConstraint->getConstraint()->castToElemDDLConstraintRI();
  ComObjectName referencedTableName( constraintNode->getReferencedTableName()
                                     , COM_TABLE_NAME);
  referencedTableName.applyDefaults(currCatAnsiName, currSchAnsiName);

  if ((isSeabaseReservedSchema(referencedTableName)) &&
      (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)))
    {
      *CmpCommon::diags() << DgSqlCode(-CAT_CANNOT_ALTER_DEFINITION_METADATA_SCHEMA);
      processReturn();
      return;
    }

  retcode = lockObjectDDL(referencedTableName,
			  COM_BASE_TABLE_OBJECT,
			  alterAddConstraint->isVolatile(),
			  alterAddConstraint->isExternal());
  if (retcode < 0) {
    processReturn();
    return;
  }

  retcode = existsInSeabaseMDTable(&cliInterface, 
                                   referencedTableName.getCatalogNamePart().getInternalName(),
                                   referencedTableName.getSchemaNamePart().getInternalName(),
                                   referencedTableName.getObjectNamePart().getInternalName(),
                                   COM_BASE_TABLE_OBJECT,
                                   TRUE);
  if (retcode < 0)
    {
      processReturn();

      return;
    }

  CorrName cn2(referencedTableName.getObjectNamePart().getInternalName(),
              STMTHEAP,
              referencedTableName.getSchemaNamePart().getInternalName(),
              referencedTableName.getCatalogNamePart().getInternalName());

  NATable *refdNaTable = bindWA.getNATable(cn2); 
  if (refdNaTable == NULL || bindWA.errStatus())
    {
      *CmpCommon::diags()
        << DgSqlCode(-4082)
        << DgTableName(cn2.getExposedNameAsAnsiString());

      processReturn();
      
      return;
    }

  if (refdNaTable->getViewText())
    {
     *CmpCommon::diags()
	<< DgSqlCode(-1127)
	<< DgTableName(cn2.getExposedNameAsAnsiString());

      processReturn();
      
      return;
    }

  if (refdNaTable->isHbaseMapTable())
    {
      // not supported
      *CmpCommon::diags() << DgSqlCode(-3242)
                          << DgString0("This alter option cannot be used for an HBase mapped table.");

      processReturn();
      
      return;
    }

  // If the referenced and referencing tables are the same, 
  // reject the request.  At this time, we do not allow self
  // referencing constraints.
  if ((CmpCommon::getDefault(TRAF_ALLOW_SELF_REF_CONSTR) == DF_OFF) &&
      (referencingTableName == referencedTableName))
    {
      *CmpCommon::diags() << DgSqlCode(-CAT_SELF_REFERENCING_CONSTRAINT);

      processReturn();
      
      return;
    }

  // User must have REFERENCES privilege on the referenced table 
  // First check for REFERENCES at the object level (column checks happen
  // later)
  NABoolean noObjPriv = FALSE;
  if (isAuthorizationEnabled())
    {
      PrivMgrUserPrivs* privs = refdNaTable->getPrivInfo();
      if (privs == NULL)
        {
          *CmpCommon::diags() << DgSqlCode(-CAT_UNABLE_TO_RETRIEVE_PRIVS);
          processReturn();
          return;
        }

      if (!ComUser::isRootUserID() && !privs->hasReferencePriv())
        noObjPriv = TRUE;
    }
    

  ElemDDLColNameArray &ringCols = alterAddConstraint->getConstraint()->castToElemDDLConstraintRI()->getReferencingColumns();

  NAList<NAString> ringKeyColList(HEAP, ringCols.entries());
  NAList<NAString> ringKeyColOrderList(HEAP, ringCols.entries());
  NAString ringColListForValidation;
  NAString ringNullList;
  for (Int32 j = 0; j < ringCols.entries(); j++)
    {
      const NAString &colName = ringCols[j]->getColumnName();
      ringKeyColList.insert(colName);
      ringKeyColOrderList.insert("ASC");

      ringColListForValidation += "\"";
      ringColListForValidation += colName;
      ringColListForValidation += "\"";
      if (j < (ringCols.entries() - 1))
        ringColListForValidation += ", ";

      ringNullList += "and ";
      ringNullList += "\"";
      ringNullList += colName;
      ringNullList += "\"";
      ringNullList += " is not null ";
    }

  if (constraintErrorChecks(&cliInterface,
                            alterAddConstraint->castToStmtDDLAddConstraintRI(),
                            ringNaTable,
                            COM_FOREIGN_KEY_CONSTRAINT, //FALSE, // referencing constr
                            ringKeyColList))
    {
      return;
    }

 const NAString &addConstrName = alterAddConstraint->
    getConstraintNameAsQualifiedName().getQualifiedNameAsAnsiString();

  // Compare the referenced column list against the primary and unique
  // constraint defined for the referenced table.  The referenced 
  // column list must match one of these constraints.  Note that if there
  // was no referenced column list specified, the primary key is used
  // and a match has automatically been found.

  const ElemDDLColNameArray &referencedColNode = 
    constraintNode->getReferencedColumns();

  NAList<NAString> refdKeyColList(HEAP, referencedColNode.entries());
  NAString refdColListForValidation;
  for (Int32 j = 0; j < referencedColNode.entries(); j++)
    {
      const NAString &colName = referencedColNode[j]->getColumnName();
      refdKeyColList.insert(colName);

      refdColListForValidation += "\"";
      refdColListForValidation += colName;
      refdColListForValidation += "\"";
      if (j < (referencedColNode.entries() - 1))
        refdColListForValidation += ", ";
    }

  if (referencedColNode.entries() == 0)
    {
      NAFileSet * naf = refdNaTable->getClusteringIndex();
      for (Lng32 i = 0; i < naf->getIndexKeyColumns().entries(); i++)
        {
          NAColumn * nac = naf->getIndexKeyColumns()[i];

          if (nac->isComputedColumnAlways() &&
              nac->isSystemColumn())
            // always computed system columns in the key are redundant,
            // don't include them (also don't include them in the DDL)
            continue;

          const NAString &colName = nac->getColName();
          refdKeyColList.insert(colName);

          refdColListForValidation += "\"";
          refdColListForValidation += nac->getColName();
          refdColListForValidation += "\"";
          if (i < (naf->getIndexKeyColumns().entries() - 1))
            refdColListForValidation += ", ";
        }
    }

  if (ringKeyColList.entries() != refdKeyColList.entries())
    {
      *CmpCommon::diags()
        << DgSqlCode(-1046)
        << DgConstraintName(addConstrName);
      
      processReturn();
      
      return;
    }

  const NAColumnArray &ringNACarr = ringNaTable->getNAColumnArray();
  const NAColumnArray &refdNACarr = refdNaTable->getNAColumnArray();
  for (Int32 i = 0; i < ringKeyColList.entries(); i++)
    {
      const NAString &ringColName = ringKeyColList[i];
      const NAString &refdColName = refdKeyColList[i];

      const NAColumn * ringNAC = ringNACarr.getColumn(ringColName);
      const NAColumn * refdNAC = refdNACarr.getColumn(refdColName);

      if (! refdNAC)
        {
          *CmpCommon::diags() << DgSqlCode(-1009)
                              << DgColumnName(refdColName);
          processReturn();
          return;
        }

      if (NOT (ringNAC->getType()->equalIgnoreNull(*refdNAC->getType())))
        {
          *CmpCommon::diags()
            << DgSqlCode(-1046)
            << DgConstraintName(addConstrName);
      
          processReturn();
          
          return;
        }

       // If the user/role does not have REFERENCES privilege at the object 
       // level, check to see if the user/role has been granted the privilege 
       // on all affected columns
       if (noObjPriv)
         {
           PrivMgrUserPrivs* privs = refdNaTable->getPrivInfo();
           if (!privs->hasColReferencePriv(refdNAC->getPosition()))
             {
                *CmpCommon::diags() << DgSqlCode(-4481)
                            << DgString0("REFERENCES")
                            << DgString1(referencedTableName.getObjectNamePart().getExternalName().data());

                 processReturn();
                 return;
             }
          }
    }

  // method getCorrespondingConstraint expects an empty input list if there are no
  // user specified columns. Clear the refdKeyColList before calling it.
  if (referencedColNode.entries() == 0)
    {
      refdKeyColList.clear();
    }

  NAString constrName;
  NABoolean isPkey = FALSE;
  NAList<int> reorderList(HEAP);
  // Find a uniqueness constraint on the referenced table that matches
  // the referenced column list (not necessarily in the original order
  // of columns).  Also find out how to reorder the column lists to
  // match the found uniqueness constraint.  This is the order in
  // which we'll add the columns to the metadata (KEYS table).  Note
  // that SHOWDDL may therefore show the foreign key columns in a
  // different order. This is a limitation of the current way we
  // store RI constraints in the metadata.
  if (NOT refdNaTable->getCorrespondingConstraint(refdKeyColList,
                                                  TRUE, // unique constraint
                                                  &constrName,
                                                  &isPkey,
                                                  &reorderList))
    {
      *CmpCommon::diags() << DgSqlCode(-CAT_REFERENCED_CONSTRAINT_DOES_NOT_EXIST)
                          << DgConstraintName(addConstrName);
      
      return;
    }

  if (reorderList.entries() > 0)
    {
      CollIndex numEntries = ringKeyColList.entries();

      CMPASSERT(ringKeyColOrderList.entries() == numEntries &&
                refdKeyColList.entries() == numEntries &&
                reorderList.entries() == numEntries);

      // re-order referencing and referenced key column lists to match
      // the order of the uniqueness constraint in the referenced table
      NAArray<NAString> ringTempKeyColArray(HEAP, numEntries);
      NAArray<NAString> ringTempKeyColOrderArray(HEAP, numEntries);
      NAArray<NAString> refdTempKeyColArray(HEAP, numEntries);

      // copy the lists into temp arrays in the correct order
      for (CollIndex i=0; i<numEntries; i++)
        {
          CollIndex newEntry = static_cast<CollIndex>(reorderList[i]);

          ringTempKeyColArray.insertAt(newEntry, ringKeyColList[i]);
          ringTempKeyColOrderArray.insertAt(newEntry, ringKeyColOrderList[i]);
          refdTempKeyColArray.insertAt(newEntry, refdKeyColList[i]);
        }

      // copy back into the lists (this will assert if we have any holes in the array)
      for (CollIndex j=0; j<numEntries; j++)
        {
          ringKeyColList[j]      = ringTempKeyColArray[j];
          ringKeyColOrderList[j] = ringTempKeyColOrderArray[j];
          refdKeyColList[j]      = refdTempKeyColArray[j];
        }
    } // reorder the lists if needed

  // check for circular RI dependencies.
  // check if referenced table cn2 refers back to the referencing table cn.
  retcode = isCircularDependent(cn, cn2, cn, &bindWA);
  if (retcode == 1) // dependency exists
    {
      *CmpCommon::diags() << DgSqlCode(-CAT_RI_CIRCULAR_DEPENDENCY)
                          << DgConstraintName(addConstrName)
                          << DgTableName(cn.getExposedNameAsAnsiString());
      
      return;
    }
  else if (retcode < 0)
    {
      // error. Diags area has been populated
      return; 
    }

  // Get a read only DDL lock on table
  if (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL))
    {
      retcode = ddlSetObjectEpoch(ringNaTable, STMTHEAP, true/*readsAllowed*/);
      if (retcode < 0)
        {
          processReturn();
          return;
        }

      // If self referencing constraint, don't get DDL lock again
      if (referencingTableName.getExternalName() != referencedTableName.getExternalName())
        {
          retcode = ddlSetObjectEpoch(refdNaTable, STMTHEAP, true/*readsAllowed*/);
          if (retcode < 0)
            {
              processReturn();
              return;
            }
        }
    }

  if (gEnableRowLevelLock) {
      lockRequired(ringNaTable, catalogNamePart.data(), schemaNamePart.data(), objectNamePart.data(), HBaseLockMode::LOCK_X, FALSE, FALSE, FALSE, FALSE, TRUE, TRUE);
  }

  if ((CmpCommon::getDefault(TRAF_NO_CONSTR_VALIDATION) == DF_OFF) &&
      (constraintNode->isEnforced()))
    {
      // validate data for RI constraint.
      // generate a "select" statement to validate the constraint.  For example:
      // SELECT count(*) FROM T1 
      //   WHERE NOT ((T1C1,T1C2) IN (SELECT T2C1,T2C2 FROM T2))
      //   OR T1C1 IS NULL OR T1C2 IS NULL;
      // This statement returns > 0 if there exist data violating the constraint.
      
      char * validQry =
        new(STMTHEAP) char[ringColListForValidation.length() +
                           refdColListForValidation.length() +
                           ringNullList.length() +
                           2000];
      const char* sql;
      if (gEnableRowLevelLock) {
        sql = "select count(*) from \"%s\".\"%s\".\"%s\" where not ((%s) in (select %s from \"%s\".\"%s\".\"%s\" for repeatable read access)) %s;";
      }
      else {
        sql = "select count(*) from \"%s\".\"%s\".\"%s\" where not ((%s) in (select %s from \"%s\".\"%s\".\"%s\")) %s;";
      }
      str_sprintf(validQry, sql,
                  referencingTableName.getCatalogNamePart().getInternalName().data(),
                  referencingTableName.getSchemaNamePart().getInternalName().data(),
                  referencingTableName.getObjectNamePart().getInternalName().data(),
                  ringColListForValidation.data(),
                  refdColListForValidation.data(),
                  referencedTableName.getCatalogNamePart().getInternalName().data(),
                  referencedTableName.getSchemaNamePart().getInternalName().data(),
                  referencedTableName.getObjectNamePart().getInternalName().data(),
                  ringNullList.data());

      Lng32 len = 0;
      Int64 rowCount = 0;
      cliRC = cliInterface.executeImmediate(validQry, (char*)&rowCount, &len, FALSE);
      if (cliRC < 0)
        {
          cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
          return;
        }
      
      if (rowCount > 0)
        {
          *CmpCommon::diags() << DgSqlCode(-1143)
                              << DgConstraintName(addConstrName)
                              << DgTableName(referencingTableName.getObjectNamePart().getInternalName().data())
                              << DgString0(referencedTableName.getObjectNamePart().getInternalName().data()) 
                              << DgString1(validQry);
          
          return;
        }
    }

  NABoolean needRestoreIBAttr = ringNaTable->incrBackupEnabled();
  if(needRestoreIBAttr == true) {
     ringNaTable->setIncrBackupEnabled(FALSE);
     dropIBAttrForTable(ringNaTable , &cliInterface);
  }

  // TDB: Add for error injection to make sure DDL locks are reset if the
  // underlying transaction is aborted.
  // Example query: begin; create table t2 (a int, b int references t1;
  //GetCliGlobals()->currContext()->getTransaction()->rollbackStatement();

  ComObjectName refdConstrName(constrName);
  refdConstrName.applyDefaults(currCatAnsiName, currSchAnsiName);

  const NAString refdCatName = refdConstrName.getCatalogNamePartAsAnsiString();
  const NAString refdSchName = refdConstrName.getSchemaNamePartAsAnsiString(TRUE);
  const NAString refdObjName = refdConstrName.getObjectNamePartAsAnsiString(TRUE);

  Int64 refdConstrUID = 
    getObjectUID(&cliInterface,
                 refdCatName.data(), refdSchName.data(), refdObjName.data(),
                 (isPkey ?  COM_PRIMARY_KEY_CONSTRAINT_OBJECT_LIT :
                  COM_UNIQUE_CONSTRAINT_OBJECT_LIT));
  
  // create unnamed constraints in deterministic format if table was 
  // created for xDC replication. 
  Int32 constrNum = ((ringNaTable->xnRepl() != COM_REPL_NONE || ringNaTable->incrBackupEnabled() ) ? 
                     getNextUnnamedConstrNum(ringNaTable) : 0);

  NAString uniqueStr;
  if (genUniqueName(alterAddConstraint, &constrNum, uniqueStr))
    {
      return;
    }
  
  Int64 tableUID = 
    getObjectUID(&cliInterface,
                 catalogNamePart.data(), schemaNamePart.data(), objectNamePart.data(),
                 COM_BASE_TABLE_OBJECT_LIT);

  ComUID comUID;
  comUID.make_UID();
  Int64 ringConstrUID = comUID.get_value();

  if (updateConstraintMD(ringKeyColList, ringKeyColOrderList, uniqueStr, tableUID, ringConstrUID, 
                         ringNaTable, COM_FOREIGN_KEY_CONSTRAINT, 
                         constraintNode->isEnforced(), &cliInterface))
    {
      *CmpCommon::diags()
        << DgSqlCode(-1029)
        << DgTableName(uniqueStr);

      return;
    }

  if (updateRIConstraintMD(ringConstrUID, refdConstrUID,
                           &cliInterface))
    {
      *CmpCommon::diags()
        << DgSqlCode(-1029)
        << DgTableName(uniqueStr);

      return;
    }


  if (updateIndexInfo(ringKeyColList,
                      ringKeyColOrderList,
                      refdKeyColList,
                      uniqueStr,
                      ringConstrUID,
                      catalogNamePart, schemaNamePart, objectNamePart,
                      ringNaTable,
                      FALSE,
                      (CmpCommon::getDefault(TRAF_NO_CONSTR_VALIDATION) == DF_ON),
                      constraintNode->isEnforced(),
                      TRUE, // because of the way the data is recorded in the
                            // metadata, the indexes of referencing and referenced
                            // tables need to have their columns in the same
                            // sequence (differences in ASC/DESC are ok)
                      &cliInterface))
    {
      *CmpCommon::diags()
        << DgSqlCode(-1029)
        << DgTableName(uniqueStr);

      //restore the flag before using it
      if(needRestoreIBAttr == true)  {
        ringNaTable->setIncrBackupEnabled(TRUE);
        addIBAttrForTable(ringNaTable, &cliInterface);
      }
      return;
    }

  //restore the flag before using it
  if(needRestoreIBAttr == true)  {
    ringNaTable->setIncrBackupEnabled(TRUE);
    addIBAttrForTable(ringNaTable, &cliInterface);
    needRestoreIBAttr = false;
  }

  if (NOT constraintNode->isEnforced())
    {
      *CmpCommon::diags()
        << DgSqlCode(1313)
        << DgString0(addConstrName);
    }

  // Upgrade DDL lock to read/write
  if (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL))
    {
      retcode = ddlSetObjectEpoch(ringNaTable, STMTHEAP, false/*readsAllowed*/, true/*continue*/);
      if (retcode < 0)
        {
          processReturn();
          return;
        }

      // If self referencing constraint, don't upgrade DDL lock
      if (referencingTableName.getExternalName() != referencedTableName.getExternalName())
        {
          retcode = ddlSetObjectEpoch(refdNaTable, STMTHEAP, false/*readsAllowed*/, true/*continue*/);
          if (retcode < 0)
            {
              processReturn();
              return;
            }
        }
    }


  if (ringNaTable->incrBackupEnabled())
  {
    retcode = updateSeabaseXDCDDLTable(&cliInterface,
                                       catalogNamePart.data(), schemaNamePart.data(),
                                       objectNamePart.data(), COM_BASE_TABLE_OBJECT_LIT);
    if (retcode < 0)
    {
      processReturn();
      return;
    }
  }

  if (updateObjectRedefTime(&cliInterface,
                            catalogNamePart, schemaNamePart, objectNamePart,
                            COM_BASE_TABLE_OBJECT_LIT, -1, tableUID,
                            ringNaTable->isStoredDesc(), ringNaTable->storedStatsAvail()))
    {
      processReturn();

      return;
    }

  if (ringNaTable->isStoredDesc()&& (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)))
    {
      insertIntoSharedCacheList(catalogNamePart, schemaNamePart, objectNamePart,
                                COM_BASE_TABLE_OBJECT, SharedCacheDDLInfo::DDL_UPDATE);
    }

  if (refdNaTable->isStoredDesc()&& (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)))
    {
      insertIntoSharedCacheList(referencedTableName.getCatalogNamePart().getInternalName(),
                                referencedTableName.getSchemaNamePart().getInternalName(),
                                referencedTableName.getObjectNamePart().getInternalName(),
                                COM_BASE_TABLE_OBJECT, SharedCacheDDLInfo::DDL_UPDATE);
    }

  if (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL))
    {
      // remove NATable for this table
      ActiveSchemaDB()->getNATableDB()->removeNATable
        (cn,
         ComQiScope::REMOVE_FROM_ALL_USERS, 
         COM_BASE_TABLE_OBJECT,
         alterAddConstraint->ddlXns(), FALSE);
    }
  
  // remove natable for the table being referenced
  ActiveSchemaDB()->getNATableDB()->removeNATable
    (cn2,
     ComQiScope::REMOVE_FROM_ALL_USERS, 
     COM_BASE_TABLE_OBJECT,
     alterAddConstraint->ddlXns(), FALSE);

  // regenerate and store packed descriptor in metadata for referenced table.
  if (updateObjectRedefTime
      (&cliInterface,
       referencedTableName.getCatalogNamePart().getInternalName(),
       referencedTableName.getSchemaNamePart().getInternalName(),
       referencedTableName.getObjectNamePart().getInternalName(),
       COM_BASE_TABLE_OBJECT_LIT, -1,
       refdNaTable->objectUid().castToInt64(),
       refdNaTable->isStoredDesc(), refdNaTable->storedStatsAvail()))
    {
      processReturn();

      return;
    }

  return;
}

short CmpSeabaseDDL::getCheckConstraintText(StmtDDLAddConstraintCheck *addCheckNode,
                                            NAString &qualifiedText,
                                            NABoolean useNoneExpandedName)
{
  //  ComString             qualifiedText;
  const ParNameLocList &nameLocList = addCheckNode->getNameLocList();
  const ParNameLoc     *pNameLoc    = NULL;
  const char           *pInputStr   = nameLocList.getInputStringPtr();

  StringPos inputStrPos = addCheckNode->getStartPosition();
  //  CharInfo::CharSet mapCS = (CharInfo::CharSet) SqlParser_ISO_MAPPING;

  for (size_t x = 0; x < nameLocList.entries(); x++)
  {
    pNameLoc = &nameLocList[x];
    const NAString &nameExpanded = useNoneExpandedName ?
                                   pNameLoc->getNoneExpandedName(FALSE/*no assert*/) :
                                   pNameLoc->getExpandedName(FALSE/*no assert*/);
    size_t nameAsIs = 0;
    size_t nameLenInBytes = 0;

    //
    // When the character set of the input string is a variable-length/width
    // multi-byte characters set, the value returned by getNameLength()
    // may not be numerically equal to the number of bytes in the original
    // input string that we need to skip.  So, we get the character
    // conversion routines to tell us how many bytes we need to skip.
    //
    enum cnv_charset eCnvCS = convertCharsetEnum(nameLocList.getInputStringCharSet());

    const char *str_to_test = (const char *) &pInputStr[pNameLoc->getNamePosition()];
    const int max_bytes2cnv = addCheckNode->getEndPosition()
      - pNameLoc->getNamePosition() + 1;
    const char *tmp_out_bufr = new (STMTHEAP) char[max_bytes2cnv * 4 + 10 /* Ensure big enough! */ ];
    char * p1stUnstranslatedChar = NULL;
    
    int cnvErrStatus = LocaleToUTF16(
                                     cnv_version1          // in  - const enum cnv_version version
                                     , str_to_test           // in  - const char *in_bufr
                                     , max_bytes2cnv         // in  - const int in_len
                                     , tmp_out_bufr          // out - const char *out_bufr
                                     , max_bytes2cnv * 4     // in  - const int out_len
                                     , eCnvCS                // in  - enum cnv_charset charset
                                     , p1stUnstranslatedChar // out - char * & first_untranslated_char
                                     , NULL                  // out - unsigned int *output_data_len_p
                                     , 0                     // in  - const int cnv_flags
                                     , (int)FALSE            // in  - const int addNullAtEnd_flag
                                     , NULL                  // out - unsigned int * translated_char_cnt_p
                                     , pNameLoc->getNameLength() // in - unsigned int max_chars_to_convert
                                     );
    // NOTE: No errors should be possible -- string has been converted before.
    
    NADELETEBASIC (tmp_out_bufr, STMTHEAP);
    nameLenInBytes = p1stUnstranslatedChar - str_to_test;
    
    // If name not expanded, then use the original name as is
    if (nameExpanded.isNull())
      nameAsIs = nameLenInBytes;
    
    // Copy from (last position in) input string up to current name
    qualifiedText += ComString(&pInputStr[inputStrPos],
                               pNameLoc->getNamePosition() - inputStrPos +
                               nameAsIs);
    
    if (NOT nameAsIs) // original name to be replaced with expanded
      {
        size_t namePos = pNameLoc->getNamePosition();
      size_t nameLen = pNameLoc->getNameLength();
      // Solution 10-080506-3000
      // For description and explanation of the fix, please read the
      // comments in method CatExecCreateView::buildViewText() in
      // module CatExecCreateView.cpp
      // Example: CREATE TABLE T ("c1" INT NOT NULL PRIMARY KEY,
      //          C2 INT CHECK (C2 BETWEEN 0 AND"c1")) NO PARTITION;
      if ( pInputStr[namePos] EQU '"'
           AND nameExpanded.data()[0] NEQ '"'
           AND namePos > 1
           AND ( pInputStr[namePos - 1] EQU '_' OR
                 isAlNumIsoMapCS((unsigned char)pInputStr[namePos - 1]) )
         )
      {
        // insert a blank separator to avoid syntax error
        // WITHOUT FIX - Example:
        // ... ALTER TABLE CAT.SCH.T ADD CONSTRAINT CAT.SCH.T_788388997_8627
        // CHECK (CAT.SCH.T.C2 BETWEEN 0 ANDCAT.SCH.T."c1") DROPPABLE ;
        // ...                           ^^^^^^
        qualifiedText += " "; // the FIX
        // WITH FIX - Example:
        // ... ALTER TABLE CAT.SCH.T ADD CONSTRAINT CAT.SCH.T_788388997_8627
        // CHECK (CAT.SCH.T.C2 BETWEEN 0 AND CAT.SCH.T."c1") DROPPABLE ;
        // ...                             ^^^
      }

      qualifiedText += nameExpanded;

      // Problem reported in solution 10-080506-3000
      // Example: CREATE TABLE T (C1 INT NOT NULL PRIMARY KEY,
      //          C2 INT CHECK ("C2"IN(1,2,3))) NO PARTITION;
      if ( pInputStr[namePos + nameLen - 1] EQU '"'
           AND nameExpanded.data()[nameExpanded.length() - 1] NEQ '"'
           AND pInputStr[namePos + nameLen] NEQ '\0'
           AND ( pInputStr[namePos + nameLen] EQU '_' OR
                 isAlNumIsoMapCS((unsigned char)pInputStr[namePos + nameLen]) )
         )
      {
        // insert a blank separator to avoid syntax error
        // WITHOUT FIX - Example:
        // ... ALTER TABLE CAT.SCH.T ADD CONSTRAINT CAT.SCH.T_654532688_9627
        // CHECK (CAT.SCH.T.C2IN (1, 2, 3)) DROPPABLE ;
        // ...              ^^^^
        qualifiedText += " "; // the FIX
        // WITH FIX - Example:
        // ... ALTER TABLE CAT.SCH.T ADD CONSTRAINT CAT.SCH.T_654532688_9627
        // CHECK (CAT.SCH.T.C2 IN (1, 2, 3)) DROPPABLE ;
        // ...               ^^^
      }
    } // if (NOT nameAsIs)

    inputStrPos = pNameLoc->getNamePosition() + nameLenInBytes;

  } // for

  //  CAT_ASSERT(addCheckNode->getEndPosition() >= inputStrPos);
  qualifiedText += ComString(&pInputStr[inputStrPos],
                             addCheckNode->getEndPosition() - inputStrPos + 1);
  
  PrettifySqlText(qualifiedText, NULL);

  return 0;
}

// nonstatic method, calling two member functions
short CmpSeabaseDDL::getTextFromMD(
                                   ExeCliInterface * cliInterface,
                                   Int64 textUID,
                                   ComTextType textType,
                                   Lng32 textSubID,
                                   NAString &outText,
                                   NABoolean binaryData)
{
  short retcode = getTextFromMD(getSystemCatalog(),
                                cliInterface,
                                textUID,
                                textType,
                                textSubID,
                                outText,
                                binaryData);

  if (retcode)
    processReturn();

  return retcode;
}

// static version of this method
short CmpSeabaseDDL::getTextFromMD(const char * catalogName,
                                   ExeCliInterface * cliInterface,
                                   Int64 textUID,
                                   ComTextType textType,
                                   Lng32 textSubID,
                                   NAString &outText,
                                   NABoolean binaryData)
{
  Lng32 cliRC;

  char query[1000];

  str_sprintf(query, "select octet_length(text), text from %s.\"%s\".%s where text_uid = %ld and text_type = %d and sub_id = %d order by seq_num",
              catalogName, SEABASE_MD_SCHEMA, SEABASE_TEXT,
              textUID, static_cast<int>(textType), textSubID);
  
  Queue * textQueue = NULL;
  //M-20032
  if (textType == COM_STORED_DESC_TEXT)
    {
      NAString dLockName(GENERATE_STORED_STATS_DLOCK_KEY);
      NAString objectID = Int64ToNAString(textUID);
      dLockName.append(objectID);
      int dLockTimeout = 60000;//60 seconds
      IndependentLockController dlock(dLockName.data(), dLockTimeout);
      if (dlock.lockHeld())
        {
          cliRC = cliInterface->fetchAllRows(textQueue, query, 0, FALSE, FALSE, TRUE);
          if (cliRC < 0)
            {
              cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
              return -1;
            }
        }
      else
        {
          *CmpCommon::diags() << DgSqlCode(-EXE_OEC_UNABLE_TO_GET_DIST_LOCK)
                              << DgString0(dLockName.data());
          return -1;
        }
    }
  else
    {
      cliRC = cliInterface->fetchAllRows(textQueue, query, 0, FALSE, FALSE, TRUE);
      if (cliRC < 0)
        {
          cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());

          return -1;
        }
    }
  

  // glue text together
  NAString binaryText;
  for (Lng32 idx = 0; idx < textQueue->numEntries(); idx++)
    {
      OutputInfo * vi = (OutputInfo*)textQueue->getNext(); 
    
      Lng32 len = *(Lng32*)vi->get(0);

      char * text = (char*)vi->get(1);

      if (binaryData)
        binaryText.append(text, len);
      else
        outText.append(text, len);
    }

  // if binary data, decode it and then return
  if (binaryData)
    {
      Lng32 decodedMaxLen = str_decoded_len(binaryText.length());
      
      char * decodedData = new(STMTHEAP) char[decodedMaxLen];
      Lng32 decodedLen =
        str_decode(decodedData, decodedMaxLen,
                   binaryText.data(), binaryText.length());
      if (decodedLen < 0)
        return -1;

      outText.append(decodedData, decodedLen);
      if (decodedData)
        NADELETEBASIC(decodedData, STMTHEAP);
    }

  return 0;
}

short CmpSeabaseDDL::getPartIndexName(ExeCliInterface * cliInterface,
                                   const NAString &schemaName,
                                   const NAString &index_name,
                                   const NAString &partitionName,
                                   NAString &partIndexName)
{
  Lng32 cliRC;
  NAString sqlBuff;
  Queue * objectsInfo = NULL;

  sqlBuff.format("select OBJECT_NAME from %s.\"%s\".%s where object_uid = (                       "
                 "  select PARTITION_UID from %s.\"%s\".%s where PARENT_UID = (                   "
                 "    select BASE_TABLE_UID from %s.\"%s\".%s i join %s.\"%s\".%s o               "
                 "      on i.index_uid =o.object_uid                                              "
                 "    where schema_name = '%s' and object_name = '%s' and object_type='IX'        "
                 "  ) and PARTITION_NAME = '%s'                                                   "
                 " );",
                  TRAFODION_SYSTEM_CATALOG, SEABASE_MD_SCHEMA, SEABASE_OBJECTS,
                  TRAFODION_SYSTEM_CATALOG, SEABASE_MD_SCHEMA, SEABASE_PARTITIONS,
                  TRAFODION_SYSTEM_CATALOG, SEABASE_MD_SCHEMA, SEABASE_INDEXES,
                  TRAFODION_SYSTEM_CATALOG, SEABASE_MD_SCHEMA, SEABASE_OBJECTS,
                  schemaName.data(),
                  index_name.data(),
                  partitionName.data());

  cliRC = cliInterface->fetchAllRows(objectsInfo, sqlBuff.data(), 0, FALSE, FALSE, TRUE);
  if (cliRC < 0)
  {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  if (objectsInfo && objectsInfo->numEntries() > 0)
  {
    objectsInfo->position();
    OutputInfo * vi = (OutputInfo*)objectsInfo->getNext();
    char * partTablename = (char*)vi->get(0);
    char partitionIndexName[256];
    CmpSeabaseDDL::getPartitionIndexName(partitionIndexName, 256, index_name, NAString(partTablename));
    partIndexName = NAString(partitionIndexName);
    return 0;
  }
  else
  {
    *CmpCommon::diags() << DgSqlCode(-3242) << DgString0("The specified index or partition does not exist or does not match");
    return -1;
  }
  return -1;
}

// ----------------------------------------------------------------------------
// alterSeabaseTableAddCheckConstraint
//    ALTER TABLE <table> ADD CONSTRAINT <name> CHECK (<constraint>)
//
// This statement runs in a transaction
// This statement may recreate the table, so it employs a read and read/write
// DDL lock.
// ----------------------------------------------------------------------------
void CmpSeabaseDDL::alterSeabaseTableAddCheckConstraint(
                                                StmtDDLAddConstraint * alterAddConstraint,
                                                NAString &currCatName, NAString &currSchName)
{
  StmtDDLAddConstraintCheck *alterAddCheckNode = alterAddConstraint
    ->castToStmtDDLAddConstraintCheck();

  Lng32 cliRC = 0;
  Lng32 retcode = 0;

 ExeCliInterface cliInterface(STMTHEAP, 0, NULL, 
                               CmpCommon::context()->sqlSession()->getParentQid());

  NAString tabName = alterAddConstraint->getTableName();
  NAString catalogNamePart, schemaNamePart, objectNamePart;
  NAString extTableName, extNameForHbase;
  NATable * naTable = NULL;
  CorrName cn;

  retcode = lockObjectDDL(alterAddConstraint);
  if (retcode < 0) {
    processReturn();
    return;
  }

  retcode = 
    setupAndErrorChecks(tabName, 
                        alterAddConstraint->getOrigTableNameAsQualifiedName(),
                        currCatName, currSchName,
                        catalogNamePart, schemaNamePart, objectNamePart,
                        extTableName, extNameForHbase, cn,
                        &naTable, 
                        FALSE, FALSE,
                        &cliInterface,
                        COM_BASE_TABLE_OBJECT,
                        SQLOperation::ALTER_TABLE);
  if (retcode < 0)
    {
      processReturn();
      return;
    }

  //add ddl lock for partition entity table
  retcode = lockObjectDDL(naTable, catalogNamePart, schemaNamePart, naTable->isPartitionV2Table());
  if (retcode < 0)
  {
    processReturn();
    return;
  }

  // add X lock
  if (lockRequired(naTable, catalogNamePart, schemaNamePart, objectNamePart, HBaseLockMode::LOCK_X, FALSE/* useHbaseXN */, FALSE/* replSync */, FALSE/* incrementalBackup */, FALSE/* asyncOperation */, TRUE/* noConflictCheck */, TRUE/* registerRegion */))
  {
    processReturn();
    
    return;
  }
  
  const ParCheckConstraintColUsageList &colList = 
    alterAddCheckNode->getColumnUsageList();
  for (CollIndex cols = 0; cols < colList.entries(); cols++)
    {
      const ParCheckConstraintColUsage &ckColUsg = colList[cols];
      const ComString &colName = ckColUsg.getColumnName();
      if ((colName EQU "SYSKEY") &&
          (naTable->getClusteringIndex()->hasSyskey()))
        {
          *CmpCommon::diags() << DgSqlCode(-CAT_SYSKEY_COL_NOT_ALLOWED_IN_CK_CNSTRNT)
                              << DgColumnName( "SYSKEY")
                              << DgTableName(extTableName);
          
          processReturn();

          return;
        }
    }

  NAList<NAString> keyColList(STMTHEAP);
  if (constraintErrorChecks(&cliInterface,
                            alterAddConstraint->castToStmtDDLAddConstraintCheck(),
                            naTable,
                            COM_CHECK_CONSTRAINT, 
                            keyColList))
    {
      return;
    }

  // update check constraint info
  NAString uniqueStr;
  if (genUniqueName(alterAddConstraint, NULL, uniqueStr))
    {
      return;
    }
  
  // get check text
  NAString checkConstrText;
  if (getCheckConstraintText(alterAddCheckNode, checkConstrText, naTable->isPartitionV2Table()))
    {
      return;
    }

  // Get a read only DDL lock on table
  if (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL))
    {
      retcode = ddlSetObjectEpoch(naTable, STMTHEAP, true /*readsAllowed*/);
      if (retcode < 0)
        {
          processReturn();
          return;
        }
    }

  if (CmpCommon::getDefault(TRAF_NO_CONSTR_VALIDATION) == DF_OFF)
    {
      // validate data for check constraint.
      // generate a "select" statement to validate the constraint.  For example:
      // SELECT count(*) FROM T1 where not checkConstrText;
      // This statement returns > 0 if there exist data violating the constraint.
      char * validQry = new(STMTHEAP) char[checkConstrText.length() + 2000];
      
      str_sprintf(validQry, "select count(*) from \"%s\".\"%s\".\"%s\" where not %s",
                  catalogNamePart.data(), 
                  schemaNamePart.data(), 
                  objectNamePart.data(),
                  checkConstrText.data());
      
      Lng32 len = 0;
      Int64 rowCount = 0;
      cliRC = cliInterface.executeImmediate(validQry, (char*)&rowCount, &len, FALSE);
      if (cliRC < 0)
        {
          cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
          return;
        }
      
      if (rowCount > 0)
        {
          *CmpCommon::diags() << DgSqlCode(-1083)
                              << DgConstraintName(uniqueStr);
          return;
        }
    }
      
  Int64 tableUID = 
    getObjectUID(&cliInterface,
                 catalogNamePart.data(), schemaNamePart.data(), objectNamePart.data(),
                 COM_BASE_TABLE_OBJECT_LIT);

  ComUID comUID;
  comUID.make_UID();
  Int64 checkUID = comUID.get_value();

  // Upgrade DDL lock to read/write
  if (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL))
    {
      retcode = ddlSetObjectEpoch(naTable, STMTHEAP, false /*readsAllowed*/, true/*continue(*/);
      if (retcode < 0)
        {
          processReturn();
          return;
        }
    }

  NAList<NAString> emptyList(STMTHEAP);
  if (updateConstraintMD(keyColList, emptyList, uniqueStr, tableUID, checkUID, 
                         naTable, COM_CHECK_CONSTRAINT, TRUE, &cliInterface))
    {
      *CmpCommon::diags()
        << DgSqlCode(-1029)
        << DgTableName(uniqueStr);
      
      return;
    }

  if (updateTextTable(&cliInterface, checkUID, COM_CHECK_CONSTR_TEXT, 0,
                      checkConstrText))
    {
      processReturn();
      return;
    }

  if (naTable->incrBackupEnabled())
  {
    retcode = updateSeabaseXDCDDLTable(&cliInterface,
                                       catalogNamePart.data(), schemaNamePart.data(),
                                       objectNamePart.data(), COM_BASE_TABLE_OBJECT_LIT);
    if (retcode < 0)
    {
      processReturn();
      return;
    }
  }

  if (updateObjectRedefTime(&cliInterface,
                            catalogNamePart, schemaNamePart, objectNamePart,
                            COM_BASE_TABLE_OBJECT_LIT, -1, tableUID,
                            naTable->isStoredDesc(), naTable->storedStatsAvail()))
    {
      processReturn();

      return;
    }

  if (naTable->isStoredDesc()&&(!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)))
    {
      insertIntoSharedCacheList(catalogNamePart, schemaNamePart, objectNamePart,
                                COM_BASE_TABLE_OBJECT, SharedCacheDDLInfo::DDL_UPDATE);
    }

  if (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL))
    {
      // remove NATable for this table
      ActiveSchemaDB()->getNATableDB()->removeNATable
        (cn,
         ComQiScope::REMOVE_FROM_ALL_USERS, 
         COM_BASE_TABLE_OBJECT,
         alterAddConstraint->ddlXns(), FALSE);
    }

  if (naTable->isPartitionV2Table())
  {
    RemoveNATableParam removeNATableParam(ComQiScope::REMOVE_FROM_ALL_USERS,
                                          alterAddConstraint->ddlXns(), FALSE);
    UInt32 operation = 0;
    if (naTable->isStoredDesc())
    {
      setCacheAndStoredDescOp(operation, UPDATE_REDEF_TIME);
      setCacheAndStoredDescOp(operation, UPDATE_SHARED_CACHE);
    }
    setCacheAndStoredDescOp(operation, REMOVE_NATABLE);

    const NAPartitionArray &partArray = naTable->getNAPartitionArray();
    for (int i = 0; i < partArray.entries(); i++)
    {
      if (partArray[i]->hasSubPartition())
      {
        const NAPartitionArray *subPartAry = partArray[i]->getSubPartitions();
        for (int j = 0; j < subPartAry->entries(); j++)
        {
          UpdateObjRefTimeParam updObjRefParam(-1, (*subPartAry)[j]->getPartitionUID(),
                                naTable->isStoredDesc(), naTable->storedStatsAvail());
          cliRC = updateCachesAndStoredDesc(&cliInterface, catalogNamePart,
                                schemaNamePart, NAString((*subPartAry)[j]->getPartitionEntityName()),
                                COM_BASE_TABLE_OBJECT, operation, &removeNATableParam,
                                &updObjRefParam, SharedCacheDDLInfo::DDL_UPDATE);
          if (cliRC < 0)
          {
            processReturn();
            return;
          }
        }
      }
      else
      {
        UpdateObjRefTimeParam updObjRefParam(-1, partArray[i]->getPartitionUID(),
                              naTable->isStoredDesc(), naTable->storedStatsAvail());
        cliRC = updateCachesAndStoredDesc(&cliInterface, catalogNamePart,
                              schemaNamePart, NAString(partArray[i]->getPartitionEntityName()),
                              COM_BASE_TABLE_OBJECT, operation, &removeNATableParam,
                              &updObjRefParam, SharedCacheDDLInfo::DDL_UPDATE);
        if (cliRC < 0)
        {
          processReturn();
          return;
        }
      }
    }
  }

  return;
}

// ----------------------------------------------------------------------------
// alterSeabaseTableAddDropConstraint
//    ALTER TABLE <table> DROP CONSTRAINT <name>
//
// This statement runs in a transaction
// ----------------------------------------------------------------------------
void CmpSeabaseDDL::alterSeabaseTableDropConstraint(
                                                StmtDDLDropConstraint * alterDropConstraint,
                                                NAString &currCatName, NAString &currSchName)
{
  Lng32 cliRC = 0;
  Lng32 retcode = 0;

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, 
                               CmpCommon::context()->sqlSession()->getParentQid());

  NAString tabName = alterDropConstraint->getTableName();
  NAString catalogNamePart, schemaNamePart, objectNamePart;
  NAString extTableName, extNameForHbase;
  NATable * naTable = NULL;
  CorrName cn;

  retcode = lockObjectDDL(alterDropConstraint);
  if (retcode < 0) {
    processReturn();
    return;
  }

  retcode = 
    setupAndErrorChecks(tabName, 
                        alterDropConstraint->getOrigTableNameAsQualifiedName(),
                        currCatName, currSchName,
                        catalogNamePart, schemaNamePart, objectNamePart,
                        extTableName, extNameForHbase, cn,
                        &naTable, 
                        FALSE, FALSE,
                        &cliInterface,
                        COM_BASE_TABLE_OBJECT,
                        SQLOperation::ALTER_TABLE);
  if (retcode < 0)
    {
      processReturn();
      return;
    }

  //add ddl lock for partition entity table
  retcode = lockObjectDDL(naTable, catalogNamePart, schemaNamePart, naTable->isPartitionV2Table());
  if (retcode < 0)
  {
    processReturn();
    return;
  }

  // add X lock
  if (lockRequired(naTable, catalogNamePart, schemaNamePart, objectNamePart, HBaseLockMode::LOCK_X, FALSE/* useHbaseXN */, FALSE/* replSync */, FALSE/* incrementalBackup */, FALSE/* asyncOperation */, TRUE/* noConflictCheck */, TRUE/* registerRegion */))
  {
    processReturn();
    
    return;
  }
  
  // Table exists, update the epoch value to lock out other user access
  if (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL))
    {
      retcode = ddlSetObjectEpoch(naTable, STMTHEAP);
      if (retcode < 0)
        {
          processReturn();
          return;
        }
    }

  const NAString &dropConstrName = alterDropConstraint->
    getConstraintNameAsQualifiedName().getQualifiedNameAsAnsiString();
  const NAString &constrCatName = alterDropConstraint->
    getConstraintNameAsQualifiedName().getCatalogName();
  const NAString &constrSchName = alterDropConstraint->
    getConstraintNameAsQualifiedName().getSchemaName();
  const NAString &constrObjName = alterDropConstraint->
    getConstraintNameAsQualifiedName().getObjectName();

  char outObjType[10];
  Int64 constrUID = getObjectUID(&cliInterface,
                                 constrCatName.data(), constrSchName.data(), constrObjName.data(),
                                 NULL,NULL,
                                 "object_type = '" COM_PRIMARY_KEY_CONSTRAINT_OBJECT_LIT"' or object_type = '" COM_UNIQUE_CONSTRAINT_OBJECT_LIT"' or object_type = '" COM_REFERENTIAL_CONSTRAINT_OBJECT_LIT"' or object_type = '" COM_CHECK_CONSTRAINT_OBJECT_LIT"' ",
                                 outObjType);
  if (constrUID < 0)
    {
      *CmpCommon::diags()
        << DgSqlCode(-1005)
        << DgConstraintName(dropConstrName);

      processReturn();

      return;
    }

  NABoolean isUniqConstr = 
    ((strcmp(outObjType, COM_UNIQUE_CONSTRAINT_OBJECT_LIT) == 0) ||
     (strcmp(outObjType, COM_PRIMARY_KEY_CONSTRAINT_OBJECT_LIT) == 0));
  NABoolean isRefConstr = 
    (strcmp(outObjType, COM_REFERENTIAL_CONSTRAINT_OBJECT_LIT) == 0);
  NABoolean isPkeyConstr = 
    (strcmp(outObjType, COM_PRIMARY_KEY_CONSTRAINT_OBJECT_LIT) == 0);
  NABoolean isCheckConstr = 
    (strcmp(outObjType, COM_CHECK_CONSTRAINT_OBJECT_LIT) == 0);

  // Remove object from shared cache before making changes.
  if (naTable->isStoredDesc())
    {
      int32_t diagsMark = CmpCommon::diags()->mark();
      NAString alterStmt("alter trafodion metadata shared cache for table ");
      alterStmt += naTable->getTableName().getQualifiedNameAsAnsiString().data();
      alterStmt += " delete internal";

      NAString msg(alterStmt);
      msg.prepend("Removing object for drop constraint: ");
      QRLogger::log(CAT_SQL_EXE, LL_WARN, "%s", msg.data());

      cliRC = cliInterface.executeImmediate(alterStmt.data());

      // If unable to remove from shared cache, it may not exist, continue 
      // If the operation is later rolled back, this table will not be added
      // back to shared cache - TBD
      CmpCommon::diags()->rewind(diagsMark);
    }


  NABoolean constrFound = FALSE;
  if (isUniqConstr)
    {
      constrFound = FALSE;
      const AbstractRIConstraintList &ariList = naTable->getUniqueConstraints();
      for (Int32 i = 0; i < ariList.entries(); i++)
        {
          AbstractRIConstraint *ariConstr = ariList[i];
          UniqueConstraint * uniqueConstr = (UniqueConstraint*)ariList[i];
          
          const NAString &tableConstrName = 
            uniqueConstr->getConstraintName().getQualifiedNameAsAnsiString();
          
          if (dropConstrName == tableConstrName)
            {
              constrFound = TRUE;
              if (uniqueConstr->hasRefConstraintsReferencingMe())
                {
                  *CmpCommon::diags()
                    << DgSqlCode(-1050);
                  
                  processReturn();
                  return;
                }
            }
        } // for

      if (NOT constrFound)
        {
          *CmpCommon::diags() << DgSqlCode(-1052);
          
          processReturn();
          
          return;
        }

      if (isPkeyConstr)
        {
          *CmpCommon::diags() << DgSqlCode(-1255)
                              << DgString0(dropConstrName)
                              << DgString1(extTableName);
          
          processReturn();
          
          return;
        }
    }
  
  NATable *otherNaTable = NULL;
  Int64 otherConstrUID = 0;
  if (isRefConstr)
    {
      constrFound = FALSE;

      RefConstraint * refConstr = NULL;
      
      const AbstractRIConstraintList &ariList = naTable->getRefConstraints();
      for (Int32 i = 0; i < ariList.entries(); i++)
        {
          AbstractRIConstraint *ariConstr = ariList[i];
          
          const NAString &tableConstrName = 
            ariConstr->getConstraintName().getQualifiedNameAsAnsiString();
          
          if (dropConstrName == tableConstrName)
            {
              constrFound = TRUE;
              refConstr = (RefConstraint*)ariConstr;
            }
        } // for
 
      if (NOT constrFound)
        {
          *CmpCommon::diags() << DgSqlCode(-1052);
          
          processReturn();
          
          return;
        }

      CorrName otherCN(refConstr->getUniqueConstraintReferencedByMe().getTableName());
      BindWA bindWA(ActiveSchemaDB(), CmpCommon::context(), TRUE/*inDDL*/);

      retcode = lockObjectDDL(otherCN.getQualifiedNameObj(),
			      COM_BASE_TABLE_OBJECT,
			      alterDropConstraint->isVolatile(),
			      alterDropConstraint->isExternal());
      if (retcode < 0) {
	processReturn();
	return;
      }

      otherNaTable = bindWA.getNATable(otherCN);
      if (otherNaTable == NULL || bindWA.errStatus())
        {
          processReturn();
          
          return;
        }

      AbstractRIConstraint * otherConstr = 
        refConstr->findConstraint(&bindWA, refConstr->getUniqueConstraintReferencedByMe());

       if (otherConstr == NULL)
         {
           NAString msg ("Unable to get referenced unique constraint for table: ");
           msg += otherNaTable->getTableName().getQualifiedNameAsAnsiString();
           SEABASEDDL_INTERNAL_ERROR(msg.data());
           processReturn();
           return;
         }
 
       // Update the epoch value of the referenced table to lock out other user access
       NABoolean selfRef = (otherCN.getExposedNameAsAnsiString() == extTableName);
       if (!selfRef && !Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL))
         {
           if (ddlSetObjectEpoch(otherNaTable, STMTHEAP, TRUE /*allow reads*/) < 0)
             {
               processReturn();
               return;
             } 
         }

      const NAString& otherCatName = 
        otherConstr->getConstraintName().getCatalogName();
      
      const NAString& otherSchName = 
        otherConstr->getConstraintName().getSchemaName();
      
      const NAString& otherConstrName = 
        otherConstr->getConstraintName().getObjectName();
      
      otherConstrUID = getObjectUID(&cliInterface,
                                    otherCatName.data(), otherSchName.data(), otherConstrName.data(),
                                    COM_UNIQUE_CONSTRAINT_OBJECT_LIT );
      if (otherConstrUID < 0)
        {
          CmpCommon::diags()->clear();
          otherConstrUID = getObjectUID(&cliInterface,
                                        otherCatName.data(), otherSchName.data(), otherConstrName.data(),
                                        COM_PRIMARY_KEY_CONSTRAINT_OBJECT_LIT );
          if (otherConstrUID < 0)
            {
              processReturn();
              
              return;
            }
        }
    }

  NABoolean indexFound = FALSE;
  Lng32 isExplicit = 0;
  Lng32 keytag = 0;
  if ((isUniqConstr || isRefConstr) && (NOT isPkeyConstr))
    {
      // find the index that corresponds to this constraint
      char query[1000];
      
      // the cardinality hint should force a nested join with
      // TABLE_CONSTRAINTS as the outer and INDEXES as the inner
      str_sprintf(query, "select I.keytag, I.is_explicit from %s.\"%s\".%s T, %s.\"%s\".%s I /*+ cardinality 1e9 */ where T.table_uid = %ld and T.constraint_uid = %ld and T.table_uid = I.base_table_uid and T.index_uid = I.index_uid ",
                  getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_TABLE_CONSTRAINTS,
                  getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_INDEXES,
                  naTable->objectUid().castToInt64(),
                  constrUID);
      
      Queue * indexQueue = NULL;
      ExeCliInterface cliInterface(STMTHEAP, 0, NULL, 
                                   CmpCommon::context()->sqlSession()->getParentQid());

      cliRC = cliInterface.fetchAllRows(indexQueue, query, 0, FALSE, FALSE, TRUE);
      if (cliRC < 0)
        {
          cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
          
          processReturn();
          
          return;
        }

      if (indexQueue->numEntries() > 1)
        {
          *CmpCommon::diags()
            << DgSqlCode(-1005)
            << DgConstraintName(dropConstrName);
          
          processReturn();
          
          return;
        }

      if (indexQueue->numEntries() ==1)
        {
          indexFound = TRUE;
          indexQueue->position();
      
          OutputInfo * oi = (OutputInfo*)indexQueue->getCurr(); 
          keytag = *(Lng32*)oi->get(0);
          isExplicit = *(Lng32*)oi->get(1);
        }
    }

  if (deleteConstraintInfoFromSeabaseMDTables(&cliInterface,
                                              naTable->objectUid().castToInt64(),
                                              (otherNaTable ? otherNaTable->objectUid().castToInt64() : 0),
                                              constrUID,
                                              otherConstrUID, 
                                              constrCatName,
                                              constrSchName,
                                              constrObjName,
                                              (isPkeyConstr ? COM_PRIMARY_KEY_CONSTRAINT_OBJECT :
                                               (isUniqConstr ? COM_UNIQUE_CONSTRAINT_OBJECT :
                                                (isRefConstr ? COM_REFERENTIAL_CONSTRAINT_OBJECT :
                                                 COM_CHECK_CONSTRAINT_OBJECT)))))
    {
      processReturn();
      
      return;
    }
  NABoolean needRestoreIBAttr = naTable->incrBackupEnabled();
  if(needRestoreIBAttr == true) {
     naTable->setIncrBackupEnabled(FALSE);
     dropIBAttrForTable(naTable , &cliInterface);
  }
                                              
  // if the index corresponding to this constraint is an implicit index and 'no check'
  // option is not specified, drop it.
  if (((indexFound) && (NOT isExplicit) && (keytag != 0)) &&
      (alterDropConstraint->getDropBehavior() != COM_NO_CHECK_DROP_BEHAVIOR))
    {
      char buf[4000];

      str_sprintf(buf, "drop index \"%s\".\"%s\".\"%s\" no check",
                  constrCatName.data(), constrSchName.data(), constrObjName.data());

      cliRC = cliInterface.executeImmediate(buf);
      
      if (cliRC < 0)
        {
          if(needRestoreIBAttr == true)  {
            naTable->setIncrBackupEnabled(TRUE);
            addIBAttrForTable(naTable, &cliInterface);
            needRestoreIBAttr = false;	
          }
          cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
          return;
        }

      // remove index for this constraint
      CorrName idxName(constrObjName, STMTHEAP, constrSchName, constrCatName);
      ActiveSchemaDB()->getNATableDB()->removeNATable
        (idxName,
         ComQiScope::REMOVE_FROM_ALL_USERS, COM_INDEX_OBJECT,
         alterDropConstraint->ddlXns(), FALSE);
      ActiveSchemaDB()->getNATableDB()->removeNATable
        (idxName,
         ComQiScope::REMOVE_MINE_ONLY, COM_INDEX_OBJECT,
         alterDropConstraint->ddlXns(), FALSE);

      insertIntoSharedCacheList(constrCatName, constrSchName, constrObjName,
                                COM_INDEX_OBJECT, SharedCacheDDLInfo::DDL_DELETE, SharedCacheDDLInfo::SHARED_DATA_CACHE);
    }
  //restore the flag before using it
  if(needRestoreIBAttr == true)  {
    naTable->setIncrBackupEnabled(TRUE);
    addIBAttrForTable(naTable, &cliInterface);
    needRestoreIBAttr = false;	
  }
  if (naTable->incrBackupEnabled())
  {
    cliRC = updateSeabaseXDCDDLTable(&cliInterface,
                                       catalogNamePart.data(), schemaNamePart.data(),
                                       objectNamePart.data(), COM_BASE_TABLE_OBJECT_LIT);
    if (cliRC < 0)
    {
      processReturn();
      return;
    }
  }

  Int64 tableUID = naTable->objectUid().castToInt64();
  if (updateObjectRedefTime(&cliInterface,
                            catalogNamePart, schemaNamePart, objectNamePart,
                            COM_BASE_TABLE_OBJECT_LIT, -1, tableUID,
                            naTable->isStoredDesc(), naTable->storedStatsAvail()))
    {
      processReturn();

      return;
    }


  if (naTable->isStoredDesc() && (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)))
    {
      insertIntoSharedCacheList(catalogNamePart, schemaNamePart, objectNamePart,
                                COM_BASE_TABLE_OBJECT, SharedCacheDDLInfo::DDL_UPDATE);
    }

  // remove NATable for this table
  ActiveSchemaDB()->getNATableDB()->removeNATable
    (cn,
     ComQiScope::REMOVE_FROM_ALL_USERS, COM_BASE_TABLE_OBJECT,
     alterDropConstraint->ddlXns(), FALSE);

  if (naTable->isPartitionV2Table())
  {
    RemoveNATableParam removeNATableParam(ComQiScope::REMOVE_FROM_ALL_USERS,
                                          alterDropConstraint->ddlXns(), FALSE);
    UInt32 operation = 0;
    if (naTable->isStoredDesc())
    {
      setCacheAndStoredDescOp(operation, UPDATE_REDEF_TIME);
      setCacheAndStoredDescOp(operation, UPDATE_SHARED_CACHE);
    }
    setCacheAndStoredDescOp(operation, REMOVE_NATABLE);

    const NAPartitionArray &partArray = naTable->getNAPartitionArray();
    for (int i = 0; i < partArray.entries(); i++)
    {
      if (partArray[i]->hasSubPartition())
      {
        const NAPartitionArray *subPartAry = partArray[i]->getSubPartitions();
        for (int j = 0; j < subPartAry->entries(); j++)
        {
          UpdateObjRefTimeParam updObjRefParam(-1, (*subPartAry)[j]->getPartitionUID(),
                                naTable->isStoredDesc(), naTable->storedStatsAvail());
          cliRC = updateCachesAndStoredDesc(&cliInterface, catalogNamePart,
                                schemaNamePart, NAString((*subPartAry)[j]->getPartitionEntityName()),
                                COM_BASE_TABLE_OBJECT, operation, &removeNATableParam,
                                &updObjRefParam, SharedCacheDDLInfo::DDL_UPDATE);
          if (cliRC < 0)
          {
            processReturn();
            return;
          }
        }
      }
      else
      {
        UpdateObjRefTimeParam updObjRefParam(-1, partArray[i]->getPartitionUID(),
                              naTable->isStoredDesc(), naTable->storedStatsAvail());
        cliRC = updateCachesAndStoredDesc(&cliInterface, catalogNamePart,
                              schemaNamePart, NAString(partArray[i]->getPartitionEntityName()),
                              COM_BASE_TABLE_OBJECT, operation, &removeNATableParam,
                              &updObjRefParam, SharedCacheDDLInfo::DDL_UPDATE);
        if (cliRC < 0)
        {
          processReturn();
          return;
        }
      }
    }
  }

  if (isRefConstr && otherNaTable)
    {
      CorrName otherCn(
           otherNaTable->getExtendedQualName().getQualifiedNameObj(), STMTHEAP);
      
      if (updateObjectRedefTime
          (&cliInterface,
           otherCn.getQualifiedNameObj().getCatalogName(),
           otherCn.getQualifiedNameObj().getSchemaName(),
           otherCn.getQualifiedNameObj().getObjectName(),
           COM_BASE_TABLE_OBJECT_LIT, -1, 
           otherNaTable->objectUid().castToInt64(),
           otherNaTable->isStoredDesc(), otherNaTable->storedStatsAvail()))
        {
          processReturn();
          
          return;
        }
      
      if (naTable->isStoredDesc() && (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)))
        {
          insertIntoSharedCacheList(otherCn.getQualifiedNameObj().getCatalogName(), 
                                    otherCn.getQualifiedNameObj().getSchemaName(), 
                                    otherCn.getQualifiedNameObj().getObjectName(),
                                    COM_BASE_TABLE_OBJECT, SharedCacheDDLInfo::DDL_UPDATE);
        }

    ActiveSchemaDB()->getNATableDB()->removeNATable
      (otherCn,
       ComQiScope::REMOVE_FROM_ALL_USERS, COM_BASE_TABLE_OBJECT,
       alterDropConstraint->ddlXns(), FALSE);
  }
  return;
}


void CmpSeabaseDDL::seabaseGrantRevoke(
                                      StmtDDLNode * stmtDDLNode,
                                      NABoolean isGrant,
                                      NAString &currCatName, 
                                      NAString &currSchName,
                                      NABoolean internalCall)
{
  Lng32 retcode = 0;

  if (!isAuthorizationEnabled())
  {
    *CmpCommon::diags() << DgSqlCode(-CAT_AUTHORIZATION_NOT_ENABLED);
    return;
  }


  StmtDDLGrant * grantNode = NULL;
  StmtDDLRevoke * revokeNode = NULL;
  NAString tabName;
  NAString origTabName;
  ComAnsiNameSpace nameSpace;  

  NAString grantedByName;
  NABoolean isGrantedBySpecified = FALSE;
  if (isGrant)
    {
      grantNode = stmtDDLNode->castToStmtDDLGrant();
      tabName = grantNode->getTableName();
      origTabName = grantNode->getOrigObjectName();
      nameSpace = grantNode->getGrantNameAsQualifiedName().getObjectNameSpace();
      isGrantedBySpecified = grantNode->isByGrantorOptionSpecified();
      grantedByName = 
       isGrantedBySpecified ? grantNode->getByGrantor()->getAuthorizationIdentifier(): "";
    }
  else
    {
      revokeNode = stmtDDLNode->castToStmtDDLRevoke();
      tabName = revokeNode->getTableName();
      origTabName = revokeNode->getOrigObjectName();
      nameSpace = revokeNode->getRevokeNameAsQualifiedName().getObjectNameSpace();
      isGrantedBySpecified = revokeNode->isByGrantorOptionSpecified();
      grantedByName = 
       isGrantedBySpecified ? revokeNode->getByGrantor()->getAuthorizationIdentifier(): "";
    }

  ComObjectName origTableName(origTabName, COM_TABLE_NAME);

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, 
  CmpCommon::context()->sqlSession()->getParentQid());

  BindWA bindWA(ActiveSchemaDB(), CmpCommon::context(), TRUE/*inDDL*/);

  CorrName cn(origTableName.getObjectNamePart().getInternalName(),
              STMTHEAP,
              origTableName.getSchemaNamePart().getInternalName(),
              origTableName.getCatalogNamePart().getInternalName());
              
  // set up common information for all grantees
  ComObjectType objectType = COM_BASE_TABLE_OBJECT;
  switch (nameSpace)
  {
    case COM_LIBRARY_NAME:
      objectType = COM_LIBRARY_OBJECT;
      break;
    case COM_UDF_NAME:
    case COM_UDR_NAME:
      objectType = COM_USER_DEFINED_ROUTINE_OBJECT;
      break;
    case COM_SEQUENCE_GENERATOR_NAME:
      objectType = COM_SEQUENCE_GENERATOR_OBJECT;
      break;
    default:
      objectType = COM_BASE_TABLE_OBJECT;
  }      

  // get the objectUID and objectOwner
  Int64 objectUID = 0;
  Int32 objectOwnerID = 0;
  Int32 schemaOwnerID = 0;
  Int64 objectFlags =  0 ;
  Int64 objDataUID = 0;
  NATable *naTable = NULL;

  ComObjectName tableName(tabName, COM_TABLE_NAME);
  if (objectType == COM_BASE_TABLE_OBJECT)
    {
      naTable = bindWA.getNATable(cn);
      if (naTable == NULL || bindWA.errStatus())
        {
          *CmpCommon::diags()
            << DgSqlCode(-4082)
            << DgTableName(cn.getExposedNameAsAnsiString());

          processReturn();
          return;
        }
      objectUID = (int64_t)naTable->objectUid().get_value();
      objectOwnerID = (int32_t)naTable->getOwner();
      schemaOwnerID = naTable->getSchemaOwner();
      objectType = naTable->getObjectType();
      if (naTable->isView())
        objectType = COM_VIEW_OBJECT;

      NAString tns = naTable->getTableName().getQualifiedNameAsAnsiString();
      tableName = ComObjectName(tns);
    }

  ComAnsiNamePart currCatAnsiName(currCatName);
  ComAnsiNamePart currSchAnsiName(currSchName);
  tableName.applyDefaults(currCatAnsiName, currSchAnsiName);

  const NAString catalogNamePart = tableName.getCatalogNamePartAsAnsiString();
  const NAString schemaNamePart = tableName.getSchemaNamePartAsAnsiString(TRUE);
  const NAString objectNamePart = tableName.getObjectNamePartAsAnsiString(TRUE);
  const NAString extTableName = tableName.getExternalName(TRUE);

  // If Hive table and using Sentry perform an error, must do grants and 
  // revoke through native Hive interface.
  if ((catalogNamePart == HIVE_SYSTEM_CATALOG) && NATable::usingSentry()) 
  {
    *CmpCommon::diags() << DgSqlCode(-CAT_SENTRY_NOT_SUPPORTED)
                        << DgString0(isGrant ? "GRANT" : "REVOKE");
    return;
  }

  ElemDDLGranteeArray & pGranteeArray = 
    (isGrant ? grantNode->getGranteeArray() : revokeNode->getGranteeArray());

  ElemDDLPrivActArray & privActsArray =
    (isGrant ? grantNode->getPrivilegeActionArray() :
     revokeNode->getPrivilegeActionArray());

  NABoolean   allPrivs =
    (isGrant ? grantNode->isAllPrivilegesSpecified() : 
     revokeNode->isAllPrivilegesSpecified());

  NABoolean   isWGOSpecified =
    (isGrant ? grantNode->isWithGrantOptionSpecified() : 
     revokeNode->isGrantOptionForSpecified());

  std::vector<PrivType> objectPrivs;
  std::vector<ColPrivSpec> colPrivs;

  if (allPrivs)
    objectPrivs.push_back(ALL_PRIVS);
  else
    if (!checkSpecifiedPrivs(privActsArray,extTableName.data(),objectType,
                             naTable,objectPrivs,colPrivs))
      {
        processReturn();
        return;
      }

  // If column privs specified for non SELECT ops for Hive/HBase native tables, 
  // return an error
  if (naTable && 
      (naTable->getTableName().isHive() || naTable->getTableName().isHbase()) &&
      (colPrivs.size() > 0))
    {
      if (hasValue(colPrivs, INSERT_PRIV) ||
          hasValue(colPrivs, UPDATE_PRIV) ||
          hasValue(colPrivs, REFERENCES_PRIV))
      {
         NAString text1("INSERT, UPDATE, REFERENCES");
         NAString text2("Hive columns on");
         *CmpCommon::diags() << DgSqlCode(-CAT_INVALID_PRIV_FOR_OBJECT)
                             << DgString0(text1.data())
                             << DgString1(text2.data())
                             << DgTableName(extTableName);
         processReturn();
         return;
      }
    }

  // If the object is a metadata table or a privilege manager table, don't 
  // allow the privilege to be grantable.
  NABoolean isMDTable = (isSeabaseMD(tableName) || 
                         isSeabasePrivMgrMD(tableName));

  if (isMDTable && isWGOSpecified)
    {
      *CmpCommon::diags() << DgSqlCode(-CAT_WGO_NOT_ALLOWED);

      processReturn();
      return;
    }

  // Grants/revokes of the select privilege on metadata tables are allowed
  // Grants/revokes of other relevant privileges are allowed if parser flag
  //   INTERNAL_QUERY_FROM_EXEUTIL is set
  // Revoke:  allow ALL and ALL_DML to be specified
  if (isMDTable && !Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL) &&
      !isMDGrantRevokeOK(objectPrivs,colPrivs,isGrant))
    {
      *CmpCommon::diags() << DgSqlCode(-CAT_SMD_PRIVS_CANNOT_BE_CHANGED);

      processReturn();
      return;
    }

  // Hive tables must be registered in traf metadata
  if (objectUID == 0 && naTable && 
      naTable->isHiveTable() &&
      CmpCommon::getDefault(HIVE_NO_REGISTER_OBJECTS) == DF_OFF)
    {
      // Register this hive table in traf metadata
      // Privilege checks performed by register code
      char query[(ComMAX_ANSI_IDENTIFIER_EXTERNAL_LEN*4) + 100];
      snprintf(query, sizeof(query),
               "register internal hive %s if not exists %s.\"%s\".\"%s\"",
               (naTable->isView() ? "view" : "table"),
               catalogNamePart.data(),
               schemaNamePart.data(),
               objectNamePart.data());
      Lng32 retcode = cliInterface.executeImmediate(query);
      if (retcode < 0)
        {
          cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
          return;
        }

      // reload NATable to get registered objectUID
      naTable = bindWA.getNATable(cn);
      if (naTable == NULL)
        {
          SEABASEDDL_INTERNAL_ERROR("Bad NATable pointer in seabaseGrantRevoke");
          return;
        }

      objectUID = (int64_t)naTable->objectUid().get_value();
      objectOwnerID = (int32_t)naTable->getOwner();
      schemaOwnerID = naTable->getSchemaOwner();
      objectType = naTable->getObjectType();
      if (naTable->isView())
        objectType = COM_VIEW_OBJECT;
    }

  // HBase tables must be registered in traf metadata
  if (objectUID == 0 &&
      naTable && 
      ((naTable->isHbaseCellTable()) || (naTable->isHbaseRowTable())))
    {
      // For native hbase tables, grantor must be DB__ROOT or belong
      // to one of the admin roles:  DB__ROOTROLE, DB__HBASEROLE
      // In hive, you must be an admin, DB__ROOTROLE and DB__HIVEROLE
      // is the equivalent of an admin.
      if (!ComUser::isRootUserID() &&
          !ComUser::currentUserHasRole(ROOT_ROLE_ID) &&
          !ComUser::currentUserHasRole(HBASE_ROLE_ID)) 
        {
          *CmpCommon::diags() << DgSqlCode (-CAT_NOT_AUTHORIZED);
          processReturn();
          return;
        }

      // register this hive table in traf metadata
      char query[(ComMAX_ANSI_IDENTIFIER_EXTERNAL_LEN*4) + 100];
      snprintf(query, sizeof(query),
               "register internal hbase table if not exists %s.\"%s\".\"%s\"",
               catalogNamePart.data(),
               schemaNamePart.data(),
               objectNamePart.data());
       Lng32 retcode = cliInterface.executeImmediate(query);
      if (retcode < 0)
        {
          cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
          return;
        }

      // reload NATable to get registered objectUID
      naTable = bindWA.getNATable(cn);
      if (naTable == NULL)
        {
          SEABASEDDL_INTERNAL_ERROR("Bad NATable pointer in seabaseGrantRevoke");
          return;
        }

      objectUID = (int64_t)naTable->objectUid().get_value();
      objectOwnerID = (int32_t)naTable->getOwner();
      schemaOwnerID = naTable->getSchemaOwner();
      objectType = naTable->getObjectType();
    }

  // for metadata tables, the objectUID is not initialized in the NATable
  // structure
  if (objectUID == 0)
    {
      ExeCliInterface cliInterface(STMTHEAP, 0, NULL, 
                                   CmpCommon::context()->sqlSession()->getParentQid());
      objectUID = getObjectInfo(&cliInterface,
                               catalogNamePart.data(), schemaNamePart.data(),
                               objectNamePart.data(), objectType,
                               objectOwnerID,schemaOwnerID,objectFlags,objDataUID);

      if (objectUID == -1 || objectOwnerID == 0)
        {
          if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_) == 0)
            SEABASEDDL_INTERNAL_ERROR("getting object UID and object owner for grant/revoke request");
            processReturn();
            return;
          }
    }

  // Privilege manager commands run in the metadata context so cqd's set by the
  // user are not propagated.  Determine CQD values before changing contexts. 
  bool useAnsiPrivs = (CmpCommon::getDefault(CAT_ANSI_PRIVS_FOR_TENANT) == DF_ON);
  
  // Prepare to call privilege manager
  NAString MDLoc;
  CONCAT_CATSCH(MDLoc, getSystemCatalog(), SEABASE_MD_SCHEMA);
  NAString privMgrMDLoc;
  CONCAT_CATSCH(privMgrMDLoc, getSystemCatalog(), SEABASE_PRIVMGR_SCHEMA);

  PrivMgrCommands command(std::string(MDLoc.data()), 
                          std::string(privMgrMDLoc.data()), 
                          CmpCommon::diags());

  if (useAnsiPrivs)
    command.setTenantPrivChecks(false);


  // Determine effective grantor ID and grantor name based on GRANTED BY clause
  // current user, and object owner
  //
  // NOTE: If the user can grant privilege based on a role, we may want the 
  // effective grantor to be the role instead of the current user.
  Int32 effectiveGrantorID;
  std::string effectiveGrantorName;
  PrivStatus result = command.getGrantorDetailsForObject( 
     isGrantedBySpecified,
     std::string(grantedByName.data()),
     objectOwnerID,
     effectiveGrantorID,
     effectiveGrantorName);

  if (result != STATUS_GOOD)
    {
      if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_) == 0)
        SEABASEDDL_INTERNAL_ERROR("getting grantor ID and grantor name");
      processReturn();
      return;
    }

  std::string objectName (extTableName.data());

  // Map hbase map table to external name
  if (ComIsHBaseMappedIntFormat(catalogNamePart, schemaNamePart))
  {
    NAString newCatName;
    NAString newSchName;
    ComConvertHBaseMappedIntToExt(catalogNamePart, schemaNamePart,
                                  newCatName, newSchName);
    objectName = newCatName.data() + std::string(".\"");
    objectName += newSchName.data() + std::string("\".");
    objectName += tableName.getObjectNamePart().getExternalName();
  }
  else
    objectName = extTableName.data();

  // For now, only support one grantee per request
  // TBD:  support multiple grantees - a testing effort?
  if (pGranteeArray.entries() > 1)
    {
      *CmpCommon::diags() << DgSqlCode (-CAT_ONLY_ONE_GRANTEE_ALLOWED);
      processReturn();
      return;
    }

#if 0
  // Table exists, update the epoch value to lock out other user access
  if (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL) && objectType == COM_BASE_TABLE_OBJECT)
    {
      retcode = ddlSetObjectEpoch(naTable, STMTHEAP);
      if (retcode < 0)
        {
          processReturn();
          return;
        }
    }
#endif

  for (CollIndex j = 0; j < pGranteeArray.entries(); j++)
    {
      NAString authName(pGranteeArray[j]->getAuthorizationIdentifier());
      Int32 grantee;
      if (pGranteeArray[j]->isPublic())
        {
          // don't allow WGO for public auth ID
          if (isWGOSpecified)
            {
              *CmpCommon::diags() << DgSqlCode(-CAT_WGO_NOT_ALLOWED);
              processReturn();
              return;
            }

          grantee = PUBLIC_USER;
          authName = PUBLIC_AUTH_NAME;
        }
      else
        {
          // only allow grants to users, roles, and public
          // If authName not found in metadata, ComDiags is populated with 
          // error 8732: <authName> is not a registered user or role
          Int16 retcode = ComUser::getAuthIDFromAuthName(authName.data(), grantee,
                                                         CmpCommon::diags());
          if (retcode != 0)
            {           
              processReturn();
              return;
            }
          if (CmpSeabaseDDLauth::isTenantID(grantee) ||
              CmpSeabaseDDLauth::isUserGroupID(grantee))
            {
              *CmpCommon::diags() << DgSqlCode(-CAT_IS_NOT_CORRECT_AUTHID)
                                  << DgString0(authName.data())
                                  << DgString1("user or role");
              processReturn();
              return;
            }


          // Don't allow WGO for roles
          if (CmpSeabaseDDLauth::isRoleID(grantee) && isWGOSpecified &&
              CmpCommon::getDefault(ALLOW_WGO_FOR_ROLES) == DF_OFF)
            {
              // If grantee is system role, allow grant
              Int32 numberRoles = sizeof(systemRoles)/sizeof(SystemAuthsStruct);
              NABoolean isSystemRole = FALSE;
              for (Int32 i = 0; i < numberRoles; i++)
                {
                  const SystemAuthsStruct &roleDefinition = systemRoles[i];
                  NAString systemRole = roleDefinition.authName;
                  if (systemRole == authName)
                    {
                      isSystemRole = TRUE;
                      break;
                    }
                }
              if (!isSystemRole)
                {
                  *CmpCommon::diags() << DgSqlCode(-CAT_WGO_NOT_ALLOWED);
                  processReturn();
                  return;
                }
            }
        }

      std::string granteeName (authName.data());
      if (isGrant)
      {
        PrivStatus result = command.grantObjectPrivilege(objectUID, 
                                                         objectName, 
                                                         objectType, 
                                                         grantee, 
                                                         granteeName, 
                                                         effectiveGrantorID, 
                                                         effectiveGrantorName, 
                                                         objectPrivs,
                                                         colPrivs,
                                                         allPrivs,
                                                         isWGOSpecified); 
      }
      else
      {
        PrivStatus result = command.revokeObjectPrivilege(objectUID, 
                                                          objectName, 
                                                          objectType, 
                                                          grantee, 
                                                          granteeName, 
                                                          effectiveGrantorID, 
                                                          effectiveGrantorName, 
                                                          objectPrivs, 
                                                          colPrivs,
                                                          allPrivs, 
                                                          isWGOSpecified);
 
      }
  }

  if (result == STATUS_ERROR)
    return;

  if (isHbase(tableName))
    hbaseGrantRevoke(stmtDDLNode, isGrant, currCatName, currSchName);

  //update xdc ddl
  updateSeabaseXDCDDLTable(&cliInterface, TRAFODION_SYSCAT_LIT,
                                          XDC_AUTH_SCHEMA, XDC_AUTH_TABLE,
                                          COM_XDC_AUTH_OBJ_TYPE_LIT);

  // Adjust the stored descriptor
  char objectTypeLit[3] = {0};
  strncpy(objectTypeLit,PrivMgr::ObjectEnumToLit(objectType),2);


  if (updateObjectRedefTime(&cliInterface,
                            catalogNamePart, schemaNamePart, objectNamePart,
                            objectTypeLit, -1, objectUID,
                            (naTable ? naTable->isStoredDesc() : FALSE),
                            (naTable ? naTable->storedStatsAvail() : FALSE)) > 0)
       {
          processReturn();
          return;
        }

  if (naTable && naTable->isStoredDesc() && (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)))
    {
      insertIntoSharedCacheList(catalogNamePart, schemaNamePart, objectNamePart,
                                COM_BASE_TABLE_OBJECT, SharedCacheDDLInfo::DDL_UPDATE);
    }

  return;
}

void CmpSeabaseDDL::hbaseGrantRevoke(
     StmtDDLNode * stmtDDLNode,
     NABoolean isGrant,
     NAString &currCatName, NAString &currSchName)
{
  Lng32 cliRC = 0;
  Lng32 retcode = 0;

  StmtDDLGrant * grantNode = NULL;
  StmtDDLRevoke * revokeNode = NULL;
  NAString tabName;

  if (isGrant)
    {
      grantNode = stmtDDLNode->castToStmtDDLGrant();
      tabName = grantNode->getTableName();
    }
  else
    {
      revokeNode = stmtDDLNode->castToStmtDDLRevoke();
      tabName = revokeNode->getTableName();
    }

  ComObjectName tableName(tabName);
  ComAnsiNamePart currCatAnsiName(currCatName);
  ComAnsiNamePart currSchAnsiName(currSchName);
  tableName.applyDefaults(currCatAnsiName, currSchAnsiName);

  const NAString catalogNamePart = tableName.getCatalogNamePartAsAnsiString();
  const NAString schemaNamePart = tableName.getSchemaNamePartAsAnsiString(TRUE);
  const NAString objectNamePart = tableName.getObjectNamePartAsAnsiString(TRUE);
  const NAString extTableName = tableName.getExternalName(TRUE);

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, 
  CmpCommon::context()->sqlSession()->getParentQid());

  if (isSeabaseReservedSchema(tableName))
    {
      *CmpCommon::diags() << DgSqlCode(-1118)
                          << DgTableName(extTableName);

      processReturn();
      return;
    }
  //TBD: Selva
  ExpHbaseInterface * ehi = allocEHI(COM_STORAGE_HBASE);
  if (ehi == NULL)
    {
      processReturn();
      return;
    }

  BindWA bindWA(ActiveSchemaDB(), CmpCommon::context(), TRUE/*inDDL*/);
  CorrName cn(tableName.getObjectNamePart().getInternalName(),
              STMTHEAP,
              tableName.getSchemaNamePart().getInternalName(),
              tableName.getCatalogNamePart().getInternalName());

  NATable *naTable = bindWA.getNATable(cn);
  if (naTable == NULL || bindWA.errStatus())
    {
      *CmpCommon::diags()
        << DgSqlCode(-4082)
        << DgTableName(cn.getExposedNameAsAnsiString());

      deallocEHI(ehi);

      processReturn();

      return;
    }
  Int64 objDataUID = naTable->objDataUID().castToInt64();

  const NAString extNameForHbase = genHBaseObjName
    (catalogNamePart, schemaNamePart, objectNamePart, naTable->getNamespace(), objDataUID);

  ElemDDLGranteeArray & pGranteeArray =
    (isGrant ? grantNode->getGranteeArray() : revokeNode->getGranteeArray());

  ElemDDLPrivActArray & pPrivActsArray =
    (isGrant ? grantNode->getPrivilegeActionArray() :
     revokeNode->getPrivilegeActionArray());

  NABoolean   allPrivs =
    (isGrant ? grantNode->isAllPrivilegesSpecified() :
     revokeNode->isAllPrivilegesSpecified());


  TextVec userPermissions;
  if (allPrivs)
    {
      userPermissions.push_back("READ");
      userPermissions.push_back("WRITE");
      userPermissions.push_back("CREATE");
    }
  else
    {
      for (Lng32 i = 0; i < pPrivActsArray.entries(); i++)
        {
          switch (pPrivActsArray[i]->getOperatorType() )
            {
            case ELM_PRIV_ACT_SELECT_ELEM:
              {
                userPermissions.push_back("READ");
                break;
              }

            case ELM_PRIV_ACT_INSERT_ELEM:
            case ELM_PRIV_ACT_DELETE_ELEM:
            case ELM_PRIV_ACT_UPDATE_ELEM:
              {
                userPermissions.push_back("WRITE");
                break;
              }

            case ELM_PRIV_ACT_CREATE_ELEM:
              {
                userPermissions.push_back("CREATE");
                break;
              }

            default:
              {
                NAString privType = "UNKNOWN";

                *CmpCommon::diags() << DgSqlCode(-CAT_INVALID_PRIV_FOR_OBJECT)
                                    <<  DgString0(privType)
                                    << DgString1(extTableName);

                deallocEHI(ehi);

                processReturn();

                return;
              }
            }  // end switch
        } // for
    }
  for (CollIndex j = 0; j < pGranteeArray.entries(); j++)
    {
      NAString authName(pGranteeArray[j]->getAuthorizationIdentifier());

      if (isGrant)
        retcode = ehi->grant(authName.data(), extNameForHbase.data(), userPermissions);
      else
        retcode = ehi->revoke(authName.data(), extNameForHbase.data(), userPermissions);

      if (retcode < 0)
        {
          *CmpCommon::diags() << DgSqlCode(-8448)
                              << (isGrant ? DgString0((char*)"ExpHbaseInterface::grant()") :
                                  DgString0((char*)"ExpHbaseInterface::revoke()"))
                              << DgString1(getHbaseErrStr(-retcode))
                              << DgInt0(-retcode)
                              << DgString2((char*)GetCliGlobals()->getJniErrorStr());

          deallocEHI(ehi);

          processReturn();

          return;
        }
    }

  retcode = ehi->close();
  if (retcode < 0)
    {
      *CmpCommon::diags() << DgSqlCode(-8448)
                          << DgString0((char*)"ExpHbaseInterface::close()")
                          << DgString1(getHbaseErrStr(-retcode))
                          << DgInt0(-retcode)
                          << DgString2((char*)GetCliGlobals()->getJniErrorStr());

      deallocEHI(ehi);

      processReturn();

      return;
    }

  deallocEHI(ehi);

  processReturn();

  return;
}

void CmpSeabaseDDL::createNativeHbaseTable(
                                       ExeCliInterface *cliInterface,
                                       StmtDDLCreateHbaseTable * createTableNode,
                                       NAString &currCatName, NAString &currSchName)
{
  Lng32 retcode = 0;
  Lng32 cliRC = 0;

  // Verify that user has privilege to create HBase tables - must be DB__ROOT 
  // or granted the DB__HBASEROLE
  if (isAuthorizationEnabled() && 
      !ComUser::isRootUserID() && 
      !ComUser::currentUserHasRole(ROOT_ROLE_ID) &&
      !ComUser::currentUserHasRole(HBASE_ROLE_ID))
    {
      *CmpCommon::diags() << DgSqlCode (-CAT_NOT_AUTHORIZED);
      processReturn();
      return;
    }

  ComObjectName tableName(createTableNode->getTableName());
  const NAString catalogNamePart = tableName.getCatalogNamePartAsAnsiString();
  const NAString schemaNamePart = tableName.getSchemaNamePartAsAnsiString(TRUE);
  const NAString objectNamePart = tableName.getObjectNamePartAsAnsiString(TRUE);
  
  // Verify that user has privilege to create HBase tables - must be DB__ROOT 
  // or granted the DB__HBASEROLE
  if (isAuthorizationEnabled() && 
      !ComUser::isRootUserID() && 
      !ComUser::currentUserHasRole(HBASE_ROLE_ID))
    {
      *CmpCommon::diags() << DgSqlCode (-CAT_NOT_AUTHORIZED);
      processReturn();
      return;
    }

  ExpHbaseInterface * ehi = allocEHI(COM_STORAGE_HBASE);
  if (ehi == NULL)
    {
      processReturn();
      return;
    }

  // If table already exists, return
  retcode =  existsInHbase(objectNamePart, ehi);
  if (retcode)
    {
      *CmpCommon::diags() << DgSqlCode(CAT_TABLE_ALREADY_EXISTS)
                          << DgTableName(objectNamePart.data());
      deallocEHI(ehi);
      processReturn();
      return;
    }
 
  std::vector<NAString> colFamVec;
  for (Lng32 i = 0; i < createTableNode->csl()->entries(); i++)
    {
      const NAString * nas = (NAString*)(*createTableNode->csl())[i];

      colFamVec.push_back(nas->data());
    }

  NAList<HbaseCreateOption*> hbaseCreateOptions(STMTHEAP);
  NAString hco;
  retcode = setupHbaseOptions(createTableNode->getHbaseOptionsClause(), 
                              0, objectNamePart,
                              hbaseCreateOptions, hco);
  if (retcode)
    {
      deallocEHI(ehi);
      processReturn();

      return;
    }
  
  HbaseStr hbaseTable;
  hbaseTable.val = (char*)objectNamePart.data();
  hbaseTable.len = objectNamePart.length();

  if (createHbaseTable(ehi, &hbaseTable, colFamVec, 
                       &hbaseCreateOptions) == -1)
    {
      deallocEHI(ehi); 
      
      processReturn();
      
      return;
    }

  // Register the table
  char query[(ComMAX_ANSI_IDENTIFIER_EXTERNAL_LEN) + 100];
  snprintf(query, sizeof(query),
           "register internal hbase table if not exists \"%s\"",
           objectNamePart.data());
   cliRC = cliInterface->executeImmediate(query);
   if (cliRC < 0)
    {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      return;
    }
}

void CmpSeabaseDDL::dropNativeHbaseTable(
                                       ExeCliInterface *cliInterface,
                                       StmtDDLDropHbaseTable * dropTableNode,
                                       NAString &currCatName, NAString &currSchName)
{
  Lng32 retcode = 0;
  Lng32 cliRC = 0;

  // Verify that user has privilege to drop HBase tables - must be DB__ROOT 
  // or granted the DB__HBASEROLE
  if (isAuthorizationEnabled() && 
      !ComUser::isRootUserID() &&
      !ComUser::currentUserHasRole(ROOT_ROLE_ID) &&
      !ComUser::currentUserHasRole(HBASE_ROLE_ID))
    {
      *CmpCommon::diags() << DgSqlCode (-CAT_NOT_AUTHORIZED);
      processReturn();
      return;
    }

  ComObjectName tableName(dropTableNode->getTableName());
  const NAString catalogNamePart = tableName.getCatalogNamePartAsAnsiString();
  const NAString schemaNamePart = tableName.getSchemaNamePartAsAnsiString(TRUE);
  const NAString objectNamePart = tableName.getObjectNamePartAsAnsiString(TRUE);

  // Verify that user has privilege to drop HBase tables - must be DB__ROOT 
  // or granted the DB__HBASEROLE
  if (isAuthorizationEnabled() && 
      !ComUser::isRootUserID() &&
      !ComUser::currentUserHasRole(HBASE_ROLE_ID))
    {
      *CmpCommon::diags() << DgSqlCode (-CAT_NOT_AUTHORIZED);
      processReturn();
      return;
    }

  ExpHbaseInterface * ehi = allocEHI(COM_STORAGE_HBASE);
  if (ehi == NULL)
    {
      processReturn();
      return;
    }

  // If table does not exist, return
  retcode =  existsInHbase(objectNamePart, ehi);
  if (retcode == 0)
    {
      *CmpCommon::diags() << DgSqlCode(CAT_TABLE_DOES_NOT_EXIST_ERROR)
                          << DgTableName(objectNamePart.data());
      deallocEHI(ehi);
      processReturn();
      return;
    }
 
  // Load definitions into cache
  BindWA bindWA(ActiveSchemaDB(),CmpCommon::context(),FALSE/*inDDL*/);
  CorrName cnCell(objectNamePart,STMTHEAP, HBASE_CELL_SCHEMA, HBASE_SYSTEM_CATALOG);
  NATable *naCellTable = bindWA.getNATableInternal(cnCell);
  CorrName cnRow(objectNamePart,STMTHEAP, HBASE_ROW_SCHEMA, HBASE_SYSTEM_CATALOG);
  NATable *naRowTable = bindWA.getNATableInternal(cnRow);

  // unregister tables 
  char query[(ComMAX_ANSI_IDENTIFIER_EXTERNAL_LEN*4) + 100];
  snprintf(query, sizeof(query), 
           "unregister hbase table %s", tableName.getObjectNamePart().getExternalName().data());
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0 && cliRC != -CAT_REG_UNREG_OBJECTS && cliRC != -3251)
    {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      deallocEHI(ehi);
      processReturn();
      return;
    }

  // Drop external mapping table
  snprintf(query, sizeof(query),
           "drop external table if exists %s ", 
           tableName.getObjectNamePart().getExternalName().data());
  NAString mapTable; 
  NAString nameSpace;
  const char * colonPos = strchr(objectNamePart, ':');
  if (colonPos)
    {
      nameSpace = NAString(objectNamePart.data(), (colonPos-objectNamePart));
      mapTable =
        NAString(&objectNamePart.data()[colonPos - objectNamePart + 1]);
    }
  else
    mapTable = objectNamePart;

  snprintf(query, sizeof(query),
           "drop external table if exists %s ", mapTable.data());
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0)
    {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      deallocEHI(ehi);
      processReturn();
      return;
    }

  // Remove cell and row tables from cache.
  if (naCellTable)
    {
      ActiveSchemaDB()->getNATableDB()->removeNATable
        (cnCell,
         ComQiScope::REMOVE_FROM_ALL_USERS,
         COM_BASE_TABLE_OBJECT,
         dropTableNode->ddlXns(), FALSE);
    }

  if (naRowTable)
    {
      ActiveSchemaDB()->getNATableDB()->removeNATable
        (cnRow,
         ComQiScope::REMOVE_FROM_ALL_USERS,
         COM_BASE_TABLE_OBJECT,
         dropTableNode->ddlXns(), FALSE);
    }

  // Remove table from HBase
  HbaseStr hbaseTable;
  hbaseTable.val = (char*)objectNamePart.data();
  hbaseTable.len = objectNamePart.length();
  retcode = dropHbaseTable(ehi, &hbaseTable, FALSE, FALSE);
  if (retcode < 0)
    {
      deallocEHI(ehi); 
      processReturn();
      return;
    }
}

short CmpSeabaseDDL::registerNativeTable
(
     const NAString &catalogNamePart,
     const NAString &schemaNamePart,
     const NAString &objectNamePart,
     Int32 objOwnerId,
     Int32 schemaOwnerId,
     ExeCliInterface &cliInterface,
     NABoolean isRegister,
     NABoolean isInternal
 )
{
  Lng32 retcode = 0;

  ComObjectType objType = COM_BASE_TABLE_OBJECT;

  Int64 flags = 0;
  if (isRegister && isInternal)
    flags = MD_OBJECTS_INTERNAL_REGISTER;
  
  Int64 objUID = -1;
  retcode =
    updateSeabaseMDObjectsTable
    (&cliInterface,
     catalogNamePart.data(),
     schemaNamePart.data(),
     objectNamePart.data(),
     objType,
     NULL,
     objOwnerId, schemaOwnerId,
     flags, objUID);
  if (retcode < 0)
    return -1;

  // Grant owner privileges
  if (isAuthorizationEnabled())
    {
      NAString fullName (catalogNamePart);
      fullName += ".";
      fullName += schemaNamePart;
      fullName += ".";
      fullName += objectNamePart;
      if (!insertPrivMgrInfo(objUID,
                             fullName,
                             objType,
                             objOwnerId,
                             schemaOwnerId,
                             ComUser::getCurrentUser()))
        {
          *CmpCommon::diags()
            << DgSqlCode(-CAT_UNABLE_TO_GRANT_PRIVILEGES)
            << DgTableName(objectNamePart);
          return -1;
        }
      
    }
 
  return 0;
}

short CmpSeabaseDDL::unregisterNativeTable
(
     const NAString &catalogNamePart,
     const NAString &schemaNamePart,
     const NAString &objectNamePart,
     ExeCliInterface &cliInterface,
     ComObjectType objType
 )
{
  short retcode = 0;

  Int64 objUID = getObjectUID(&cliInterface,
                              catalogNamePart.data(), 
                              schemaNamePart.data(), 
                              objectNamePart.data(),
                              comObjectTypeLit(objType));
  
  // Revoke owner privileges
  if (isAuthorizationEnabled())
    {
      NAString fullName (catalogNamePart);
      fullName += ".";
      fullName += schemaNamePart;
      fullName += ".";
      fullName += objectNamePart;
      if (!deletePrivMgrInfo(fullName,
                             objUID,
                             objType))
        {
          *CmpCommon::diags()
            << DgSqlCode(-CAT_PRIVILEGE_NOT_REVOKED)
            << DgTableName(objectNamePart);
          return -1;
        }
    }
  
  // delete hist stats, if HIST tables exist
  retcode = existsInSeabaseMDTable
    (&cliInterface, 
     HIVE_STATS_CATALOG, HIVE_STATS_SCHEMA_NO_QUOTES, HBASE_HIST_NAME,
     COM_BASE_TABLE_OBJECT);
  if (retcode < 0)
    return -1;
  
  if (retcode == 1) // exists
    {
      if (dropSeabaseStats(&cliInterface,
                           HIVE_STATS_CATALOG,
                           HIVE_STATS_SCHEMA_NO_QUOTES,
                           objUID))
        {
          return -1;
        }
    }
  
  // drop from metadata
  retcode =
    deleteFromSeabaseMDObjectsTable
    (&cliInterface,
     catalogNamePart.data(),
     schemaNamePart.data(),
     objectNamePart.data(),
     objType
     );
  
  return 0;
}

short CmpSeabaseDDL::unregisterHiveSchema
(
     const NAString &catalogNamePart,
     const NAString &schemaNamePart,
     ExeCliInterface &cliInterface,
     NABoolean cascade
 )
{
  Lng32 cliRC = 0;
  short retcode = 0;

  if (cascade)
    {
      //  unregister all objects in this schema
      Queue * objectsInfo = NULL;
      char query[2000];
      str_sprintf(query, "select object_type, object_name "
                  "from %s.\"%s\".%s "
                  "where catalog_name = '%s' and schema_name = '%s'"
                  "order by 1 for read committed access",
                  getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS,
                  catalogNamePart.data(), schemaNamePart.data());
      cliRC = cliInterface.fetchAllRows(objectsInfo, query, 0, FALSE, FALSE, TRUE);
      if (cliRC < 0)
        {
          cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
          return -1;
        }
      
      objectsInfo->position();
      for (Lng32 i = 0; i < objectsInfo->numEntries(); i++)
        {
          OutputInfo * oi = (OutputInfo*)objectsInfo->getNext(); 
          
          char * objType = (char*)oi->get(0);

          char * objName = (char*)oi->get(1);
          
          if (strcmp(objType, COM_BASE_TABLE_OBJECT_LIT) == 0)
            {
              retcode = unregisterNativeTable(
                   catalogNamePart, schemaNamePart, objName,
                   cliInterface,
                   COM_BASE_TABLE_OBJECT);
            }

          
          if (retcode < 0)
            {
              return -1;
            }
        } // for
    }

  retcode = unregisterNativeTable(
       catalogNamePart, schemaNamePart, SEABASE_SCHEMA_OBJECTNAME,
       cliInterface,
       COM_SHARED_SCHEMA_OBJECT);

  return retcode;
}

void CmpSeabaseDDL::regOrUnregNativeObject(
     StmtDDLRegOrUnregObject * regOrUnregObject,
     NAString &currCatName, NAString &currSchName)
{
  Lng32 retcode = 0;

  char errReason[400];
  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, 
                               CmpCommon::context()->sqlSession()->getParentQid());

  NAString catalogNamePart = regOrUnregObject->getObjNameAsQualifiedName().
    getCatalogName();
  NAString schemaNamePart = regOrUnregObject->getObjNameAsQualifiedName().
    getSchemaName();
  NAString objectNamePart = regOrUnregObject->getObjNameAsQualifiedName().
    getObjectName();
  ComObjectName tableName;
  NAString tabName;
  NAString extTableName;

  NABoolean isHive  = (catalogNamePart.index(HIVE_SYSTEM_CATALOG, 0, NAString::ignoreCase) == 0);

  NABoolean isHBase = (catalogNamePart.index(HBASE_SYSTEM_CATALOG, 0, NAString::ignoreCase) == 0);

  if (NOT (isHive || isHBase))
    {
      *CmpCommon::diags() << DgSqlCode(-3242) << 
        DgString0("Register/Unregister statement must specify a hive or hbase object.");
      
      processReturn();
      return;
    }

  if (isHive)
    {
      if (NOT (schemaNamePart == HIVE_SYSTEM_SCHEMA))
        schemaNamePart.toUpper();
      objectNamePart.toUpper();
    }

  // make sure that underlying hive/hbase object exists
  BindWA bindWA(ActiveSchemaDB(), CmpCommon::context(), FALSE/*inDDL*/);
  CorrName cn(objectNamePart, STMTHEAP, 
              ((isHBase && (schemaNamePart == HBASE_SYSTEM_SCHEMA))
               ? HBASE_CELL_SCHEMA : schemaNamePart),
              catalogNamePart);
  if (isHive && regOrUnregObject->objType() == COM_SHARED_SCHEMA_OBJECT)
    cn.setSpecialType(ExtendedQualName::SCHEMA_TABLE);

  NATable * naTable = bindWA.getNATable(cn);
  if (((naTable == NULL) || (bindWA.errStatus())) &&
      ((regOrUnregObject->isRegister()) || // register
       (NOT regOrUnregObject->cleanup()))) // unreg and not cleanup
    {
      CmpCommon::diags()->clear();

      *CmpCommon::diags() << DgSqlCode(-3251)
                          << (regOrUnregObject->isRegister() ? DgString0("REGISTER") :
                              DgString0("UNREGISTER"))
                          << DgString1(NAString(" Reason: Specified object ") +
                                       (regOrUnregObject->objType() == COM_SHARED_SCHEMA_OBJECT ?
                                        schemaNamePart : objectNamePart) +
                                       NAString(" does not exist."));
      
      processReturn();
      
      return;
    }
  
  // ignore errors for 'unregister cleanup'
  CmpCommon::diags()->clear();

  if (naTable)
    {
      if (regOrUnregObject->isRegister() && 
          (naTable->isRegistered())) // already registered
        {
          if (NOT regOrUnregObject->registeredOption())
            {
              str_sprintf(errReason, " Reason: %s has already been registered.",
                          (regOrUnregObject->objType() == COM_SHARED_SCHEMA_OBJECT ?
                           schemaNamePart.data() :
                           regOrUnregObject->getObjNameAsQualifiedName().
                           getQualifiedNameAsString().data()));
              *CmpCommon::diags() << DgSqlCode(-3251)
                                  << DgString0("REGISTER")
                                  << DgString1(errReason);
            }
          
          processReturn();
          
          return;
        }
      else if ((NOT regOrUnregObject->isRegister()) && // unregister
               (NOT naTable->isRegistered())) // not registered
        {
          if (NOT regOrUnregObject->registeredOption())
            {
              str_sprintf(errReason, " Reason: %s has not been registered.",
                          (regOrUnregObject->objType() == COM_SHARED_SCHEMA_OBJECT ?
                           schemaNamePart.data() :
                           regOrUnregObject->getObjNameAsQualifiedName().
                           getQualifiedNameAsString().data()));
              *CmpCommon::diags() << DgSqlCode(-3251)
                                  << DgString0("UNREGISTER")
                                  << DgString1(errReason);
            }
          
          processReturn();
          
          return;
        }
    }

  // Verify user can perform request
  if (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL) &&
      !ComUser::isRootUserID() &&
      !ComUser::currentUserHasRole(ROOT_ROLE_ID) &&
      ((isHive && !ComUser::currentUserHasRole(HIVE_ROLE_ID)) ||
       (isHBase && !ComUser::currentUserHasRole(HBASE_ROLE_ID))))
    {
      *CmpCommon::diags() << DgSqlCode (-CAT_NOT_AUTHORIZED);
      processReturn();
      return;
    }

  Int32 objOwnerId = (isHive ? HIVE_ROLE_ID : HBASE_ROLE_ID);
  Int32 schemaOwnerId = (isHive ? HIVE_ROLE_ID : HBASE_ROLE_ID);
  Int64 objUID = -1;
  Int64 flags = 0;
  if ((regOrUnregObject->isRegister()) &&
      (regOrUnregObject->objType() == COM_SHARED_SCHEMA_OBJECT))
    {
      retcode =
        updateSeabaseMDObjectsTable
        (&cliInterface,
         catalogNamePart.data(),
         schemaNamePart.data(),
         SEABASE_SCHEMA_OBJECTNAME,
         regOrUnregObject->objType(),
         NULL,
         objOwnerId, schemaOwnerId,
         flags, objUID);
    }
  else if (regOrUnregObject->isRegister())
    {
      if (((regOrUnregObject->objType() == COM_BASE_TABLE_OBJECT) &&
           (naTable && naTable->isView())) ||
          ((regOrUnregObject->objType() == COM_VIEW_OBJECT) &&
           (naTable && (! naTable->isView()))))
        {
          // underlying object is a view but registered object type specified
          // in the register statement is a table, or
          // underlying object is a table but registered object type specified
          // in the register statement is a view
          str_sprintf(errReason, " Reason: Mismatch between specified(%s) and underlying(%s) type for %s.",
                      (regOrUnregObject->objType() == COM_BASE_TABLE_OBJECT ? "TABLE" : "VIEW"),
                      (naTable->isView() ? "VIEW" : "TABLE"),
                      regOrUnregObject->getObjNameAsQualifiedName().
                      getQualifiedNameAsString().data());

          *CmpCommon::diags() << DgSqlCode(-3251)
                              << DgString0("REGISTER")
                              << DgString1(errReason);
          
          processReturn();
          
          return;
        }

      if (regOrUnregObject->objType() == COM_BASE_TABLE_OBJECT)
        {
          if (schemaNamePart == HBASE_SYSTEM_SCHEMA)
            {
              // register CELL and ROW formats of HBase table
              retcode = registerNativeTable(
                   catalogNamePart, HBASE_CELL_SCHEMA, objectNamePart,
                   objOwnerId, schemaOwnerId,
                   cliInterface, 
                   regOrUnregObject->isRegister(), 
                   regOrUnregObject->isInternal());

              retcode = registerNativeTable(
                   catalogNamePart, HBASE_ROW_SCHEMA, objectNamePart,
                   objOwnerId, schemaOwnerId,
                   cliInterface, 
                   regOrUnregObject->isRegister(), 
                   regOrUnregObject->isInternal());
            }
          else
            {
              retcode = registerNativeTable(
                   catalogNamePart, schemaNamePart, objectNamePart,
                   objOwnerId, schemaOwnerId,
                   cliInterface, 
                   regOrUnregObject->isRegister(), 
                   regOrUnregObject->isInternal());
            }
        }


      if (retcode < 0)
        return;
    }
  else // unregister
    {
      if ((regOrUnregObject->objType() == COM_BASE_TABLE_OBJECT) ||
          (regOrUnregObject->objType() == COM_SHARED_SCHEMA_OBJECT))
        {
          if (schemaNamePart == HBASE_SYSTEM_SCHEMA)
            {
              // unregister CELL and ROW formats of HBase table
              retcode = unregisterNativeTable(
                   catalogNamePart, HBASE_CELL_SCHEMA, objectNamePart,
                   cliInterface);

              retcode = unregisterNativeTable(
                   catalogNamePart, HBASE_ROW_SCHEMA, objectNamePart,
                   cliInterface);
            }
          else if ((regOrUnregObject->objType() == COM_SHARED_SCHEMA_OBJECT) &&
                   (catalogNamePart.index(HIVE_SYSTEM_CATALOG, 0, NAString::ignoreCase) == 0))
            {
              retcode = unregisterHiveSchema(
                   catalogNamePart, schemaNamePart,
                   cliInterface,
                   regOrUnregObject->cascade());
            }
          else
            {
              retcode = unregisterNativeTable(
                   catalogNamePart, schemaNamePart, objectNamePart,
                   cliInterface,
                   (regOrUnregObject->objType() == COM_SHARED_SCHEMA_OBJECT ?
                    COM_SHARED_SCHEMA_OBJECT : COM_BASE_TABLE_OBJECT));
            }
        }

    } // unregister
  if (retcode < 0)
    return;

  ActiveSchemaDB()->getNATableDB()->removeNATable
    (cn,
     ComQiScope::REMOVE_FROM_ALL_USERS, regOrUnregObject->objType(),
     FALSE, FALSE);
  if (isHBase)
    {
      CorrName cn(objectNamePart, STMTHEAP, 
                  HBASE_ROW_SCHEMA,
                  catalogNamePart);

      ActiveSchemaDB()->getNATableDB()->removeNATable
        (cn,
         ComQiScope::REMOVE_FROM_ALL_USERS, regOrUnregObject->objType(),
         FALSE, FALSE);
    }
  
  return;
}



/////////////////////////////////////////////////////////////////////////
// This method generates and returns tableInfo struct for internal special
// tables (like metadata, histograms). These tables have hardcoded definitions
// but need objectUID to be returned. ObjectUID is stored in metadata
// and is read from there.
// This is done only if we are not in bootstrap mode, for example, when initializing
// metadata. At that time, there is no metadata available so it cannot be read
// to return objectUID.
// A NULL tableInfo is returned if in bootstrap mode.
//
// RETURN: -1, if error. 0, if all ok.
//////////////////////////////////////////////////////////////////////////
short CmpSeabaseDDL::getSpecialTableInfo
(
 NAMemory * heap,
 const NAString &catName, 
 const NAString &schName, 
 const NAString &objName,
 const NAString &extTableName,
 const ComObjectType  &objType,
 ComTdbVirtTableTableInfo* &tableInfo)
{
  Lng32 cliRC = 0;
  NABoolean switched = FALSE;

  Int32 objectOwner = NA_UserIdDefault;
  Int32 schemaOwner = NA_UserIdDefault;
  Int64 objUID = 0;

  // It's okay if the schema UID for special tables is 0
  Int64 schemaUID = 0;
  Int64 objectFlags =  0 ;
  Int64 objDataUID = 0;

  NABoolean createTableInfo = FALSE;
  NABoolean isUninit = FALSE;
  if (CmpCommon::context()->isUninitializedSeabase())
    {
      isUninit = TRUE;
      createTableInfo = TRUE;
    }

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, 
                               CmpCommon::context()->sqlSession()->getParentQid());

  NABoolean getUID = TRUE;
  if (isUninit)
    getUID = FALSE;
  else if ((CmpCommon::context()->isMxcmp()) &&
           (ComIsTrafodionReservedSchemaName(schName)))
    getUID = FALSE;
  else if (CmpSeabaseDDL::getBootstrapMode())
    getUID = FALSE;
  if (getUID)
    {
      if (switchCompiler(CmpContextInfo::CMPCONTEXT_TYPE_META))
        return -1;

      CmpSeabaseDDL::setBootstrapMode(TRUE);
      objUID = getObjectInfo(&cliInterface, 
                             catName.data(), schName.data(), objName.data(), 
                             objType, objectOwner, schemaOwner,objectFlags,objDataUID);
      CmpSeabaseDDL::setBootstrapMode(FALSE);
      if (objUID <= 0)
        {
          CmpCommon::diags()->clear();
          goto label_error_return;
        }

      switchBackCompiler();

      createTableInfo = TRUE;
    }

  if (createTableInfo)
    {
      if (tableInfo == NULL)
        {
          tableInfo = new(heap) ComTdbVirtTableTableInfo[1];
          tableInfo->tableName = new(heap) char[extTableName.length() + 1];
          strcpy((char*)tableInfo->tableName, (char*)extTableName.data());
          tableInfo->createTime = 0;
          tableInfo->redefTime = 0;
          tableInfo->objUID = objUID;
          tableInfo->objDataUID = objDataUID;
          tableInfo->schemaUID = schemaUID;
          tableInfo->isAudited = 1;
          tableInfo->validDef = 1;
          tableInfo->objOwnerID = objectOwner;
          tableInfo->schemaOwnerID = schemaOwner;
          tableInfo->hbaseCreateOptions = NULL;
          tableInfo->numSaltPartns = 0;
          tableInfo->numInitialSaltRegions = 1;
          tableInfo->hbaseSplitClause = NULL;
          tableInfo->numTrafReplicas = 0;
          tableInfo->objectFlags = objectFlags;
          tableInfo->tablesFlags = 0;
          tableInfo->rowFormat = COM_UNKNOWN_FORMAT_TYPE;
          tableInfo->xnRepl = COM_REPL_NONE;
          
          tableInfo->tableNamespace = NULL;
        }
      else if ((tableInfo->objUID == 0) && (getUID))
        {
          tableInfo->objUID = objUID;
          tableInfo->objDataUID = objDataUID;
          tableInfo->schemaUID = schemaUID;
        }

      // get namespace of this table, if it was created in a non-default ns.
      NAString tableNamespace;
      if (! tableInfo->tableNamespace)
        {
          if (NOT ComIsTrafodionReservedSchemaName(schName))
            {
              if (objUID != 0)
                {
                  if (switchCompiler(CmpContextInfo::CMPCONTEXT_TYPE_META))
                    return -1;
                  
                  if (getTextFromMD(&cliInterface, objUID, COM_OBJECT_NAMESPACE, 0,
                                    tableNamespace))
                    {
                      goto label_error_return;
                    }
                  
                  switchBackCompiler();
                }
            }
          else // reserved schema name
            {
              if (CmpCommon::context()->useReservedNamespace())
                {
                  tableNamespace = ComGetReservedNamespace(schName);
                }
            }
        }

      if (NOT tableNamespace.isNull())
        {
          tableInfo->tableNamespace = 
            new(heap) char[tableNamespace.length()+1];
          strcpy((char *)tableInfo->tableNamespace, tableNamespace.data());
        }
    }

  return 0;

 label_error_return:

  switchBackCompiler();

  return -1;
}

TrafDesc * CmpSeabaseDDL::getSeabaseMDTableDesc(
                                                   const NAString &catName, 
                                                   const NAString &schName, 
                                                   const NAString &objName,
                                                   const ComObjectType objType)
{
  Lng32 cliRC = 0;

  TrafDesc * tableDesc = NULL;
  NAString schNameL = "\"";
  schNameL += schName;
  schNameL += "\"";

  ComObjectName coName(catName, schNameL, objName);
  NAString extTableName = coName.getExternalName(TRUE);

  ComTdbVirtTableTableInfo * tableInfo = NULL;
  Lng32 colInfoSize = 0;
  const ComTdbVirtTableColumnInfo * colInfo = NULL;
  Lng32 keyInfoSize = 0;
  const ComTdbVirtTableKeyInfo * keyInfo = NULL;
  Lng32 uniqueInfoSize = 0;
  ComTdbVirtTableConstraintInfo * constrInfo = NULL;

  Lng32 indexInfoSize = 0;
  const ComTdbVirtTableIndexInfo * indexInfo = NULL;
  NAString nameSpace;
  if (NOT getMDtableInfo(coName,
                         tableInfo,
                         colInfoSize, colInfo,
                         keyInfoSize, keyInfo,
                         indexInfoSize, indexInfo,
                         objType, nameSpace))
    return NULL;

  // Setup the primary key information as a unique constraint
  uniqueInfoSize = 1;
  constrInfo = new(STMTHEAP) ComTdbVirtTableConstraintInfo[uniqueInfoSize];
  constrInfo->baseTableName = (char*)extTableName.data();

  // The primary key constraint name is the name of the object appended
  // with "_PK";
  NAString constrName = extTableName;
  constrName += "_PK";
  constrInfo->constrName = (char*)constrName.data();

  constrInfo->constrType = 3; // pkey_constr

  constrInfo->colCount = keyInfoSize;
  constrInfo->keyInfoArray = (ComTdbVirtTableKeyInfo *)keyInfo;

  constrInfo->numRingConstr = 0;
  constrInfo->ringConstrArray = NULL;
  constrInfo->numRefdConstr = 0;
  constrInfo->refdConstrArray = NULL;

  constrInfo->checkConstrLen = 0;
  constrInfo->checkConstrText = NULL;

  tableDesc =
    Generator::createVirtualTableDesc
    ((char*)extTableName.data(),
     NULL, // let it decide what heap to use
     colInfoSize,
     (ComTdbVirtTableColumnInfo*)colInfo,
     keyInfoSize,
     (ComTdbVirtTableKeyInfo*)keyInfo,
     uniqueInfoSize, constrInfo,
     indexInfoSize, 
     (ComTdbVirtTableIndexInfo *)indexInfo,
     0, NULL,
     tableInfo,
     NULL /*seqinfo*/, NULL /*statsinfo*/, NULL /*endk*/, 
     FALSE /*gendesc*/, NULL /*desclen*/, FALSE /*ss*/, NULL /*priv*/,
     (nameSpace.length() > 0) ? nameSpace.data() : NULL);

  return tableDesc;

}

TrafDesc * CmpSeabaseDDL::getSeabaseHistTableDesc(const NAString &catName, 
                                                  const NAString &schName, 
                                                  const NAString &objName)
{
  Lng32 cliRC = 0;

  TrafDesc * tableDesc = NULL;
  NAString schNameL = "\"";
  schNameL += schName;
  schNameL += "\"";  // transforms internal format schName to external format

  NAString objNameL = "\"";
  objNameL += objName;
  objNameL += "\"";

  ComObjectName coName(catName, schNameL, objNameL);
  NAString extTableName = coName.getExternalName(TRUE);

  Lng32 numCols = 0;
  ComTdbVirtTableColumnInfo * colInfo = NULL;
  Lng32 numKeys;
  ComTdbVirtTableKeyInfo * keyInfo;
  ComTdbVirtTableIndexInfo * indexInfo;
  ComTdbVirtTableTableInfo * tableInfo = NULL;

  Parser parser(CmpCommon::context());

  ComTdbVirtTableConstraintInfo * constrInfo =
    new(STMTHEAP) ComTdbVirtTableConstraintInfo[1];

  NAString constrName;
  NAString nameSpace;
  if (ComIsTrafodionBackupSchemaName(schName))
    nameSpace = TRAF_RESERVED_NAMESPACE3;
  else if (ComIsTrafodionReservedSchemaName(schName))
    nameSpace = TRAF_RESERVED_NAMESPACE2;
  
  const QString * ddl = NULL;
  Lng32 sizeOfDDL = 0;
  if (objName == HBASE_HIST_NAME)
    {
      ddl = seabaseHistogramsDDL;
      sizeOfDDL = sizeof(seabaseHistogramsDDL);
      constrName = HBASE_HIST_PK;
    }
  else if (objName == HBASE_HISTINT_NAME)
    {
      ddl = seabaseHistogramIntervalsDDL;
      sizeOfDDL = sizeof(seabaseHistogramIntervalsDDL);
      constrName = HBASE_HISTINT_PK;
    }
  else if (objName == HBASE_PERS_SAMP_NAME)
    {
      ddl = seabasePersistentSamplesDDL;
      sizeOfDDL = sizeof(seabasePersistentSamplesDDL);
      constrName = HBASE_PERS_SAMP_PK;
    }
  else
    return NULL;
  
  if (processDDLandCreateDescs(parser,
                               ddl, sizeOfDDL,
                               catName, schName, 
                               nameSpace,
                               FALSE,
                               0, NULL, 0, NULL,
                               numCols, colInfo,
                               numKeys, keyInfo,
                               tableInfo,
                               indexInfo))
    return NULL;

  ComObjectName coConstrName(catName, schNameL, constrName);
  NAString * extConstrName = 
    new(STMTHEAP) NAString(coConstrName.getExternalName(TRUE));
  
  constrInfo->baseTableName = (char*)extTableName.data();
  constrInfo->constrName = (char*)extConstrName->data();
  constrInfo->constrType = 3; // pkey_constr

  constrInfo->colCount = numKeys;
  constrInfo->keyInfoArray = keyInfo;

  constrInfo->numRingConstr = 0; 
  constrInfo->ringConstrArray = NULL;
  constrInfo->numRefdConstr = 0;
  constrInfo->refdConstrArray = NULL;
  
  constrInfo->checkConstrLen = 0;
  constrInfo->checkConstrText = NULL;

  ULng32 savedParserFlags = Get_SqlParser_Flags (0xFFFFFFFF);
  Set_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL);

  if (getSpecialTableInfo(STMTHEAP, catName, schName, objName,
                          extTableName, COM_BASE_TABLE_OBJECT, tableInfo))
  {
    Assign_SqlParser_Flags (savedParserFlags);
    return NULL;
  }
  Assign_SqlParser_Flags (savedParserFlags);
  tableDesc =
    Generator::createVirtualTableDesc
    ((char*)extTableName.data(),
     NULL, // let it decide what heap to use
     numCols,
     colInfo,
     numKeys,
     keyInfo,
     1, constrInfo,
     0, NULL,
     0, NULL,
     tableInfo);

  return tableDesc;
}

TrafDesc * CmpSeabaseDDL::getTrafBackupMDTableDesc(const NAString &catName, 
                                                   const NAString &schName, 
                                                   const NAString &objName)
{
  Lng32 cliRC = 0;

  TrafDesc * tableDesc = NULL;
  NAString schNameL = "\"";
  schNameL += schName;
  schNameL += "\"";  // transforms internal format schName to external format

  NAString objNameL = "\"";
  objNameL += objName;
  objNameL += "\"";

  ComObjectName coName(catName, schNameL, objNameL);
  NAString extTableName = coName.getExternalName(TRUE);

  Lng32 numCols = 0;
  ComTdbVirtTableColumnInfo * colInfo = NULL;
  Lng32 numKeys;
  ComTdbVirtTableKeyInfo * keyInfo;
  ComTdbVirtTableIndexInfo * indexInfo;
  ComTdbVirtTableTableInfo * tableInfo = NULL;

  Parser parser(CmpCommon::context());

  size_t numMDTables = sizeof(allMDtablesInfo) / sizeof(MDTableInfo);
  NABoolean found = FALSE;
  size_t i = 0;
  for (i = 0; ((NOT found) && (i < numMDTables)); i++)
    {
      const MDTableInfo &mdti = allMDtablesInfo[i];
      
      if (objName != mdti.newName)
        continue;

      if (processDDLandCreateDescs(parser,
                                   mdti.newDDL, mdti.sizeOfnewDDL,
                                   catName, schName, 
                                   NAString(TRAF_RESERVED_NAMESPACE3),
                                   FALSE,
                                   0, NULL, 0, NULL,
                                   numCols, colInfo,
                                   numKeys, keyInfo,
                                   tableInfo,
                                   indexInfo))
        return NULL;

      found = TRUE;
    }

  if (NOT found)
    {
      size_t numMDTables = sizeof(privMgrTables)/sizeof(PrivMgrTableStruct);

      for (size_t i = 0; ((NOT found) && (i < numMDTables)); i++)
        {
          const PrivMgrTableStruct &tableDefinition = privMgrTables[i];
          
          if (objName != tableDefinition.tableName)
            continue;

          // Set up create table ddl
          NAString tableDDL("CREATE TABLE ");
          NAString privMgrMDLoc;
          CONCAT_CATSCH(privMgrMDLoc, getSystemCatalog(), schName);
          
          tableDDL += privMgrMDLoc.data() + NAString('.') + tableDefinition.tableName; 
          tableDDL += tableDefinition.tableDDL->str;
          
          QString ddlString; 
          ddlString.str = tableDDL.data();
          
          if (processDDLandCreateDescs(parser,
                                       &ddlString, sizeof(QString),
                                       catName, schName,
                                       NAString(TRAF_RESERVED_NAMESPACE3),
                                       FALSE,
                                       0, NULL, 0, NULL,
                                       numCols, colInfo,
                                       numKeys, keyInfo,
                                       tableInfo,
                                       indexInfo))
            return NULL;
          found = TRUE;
        } // for
    } // if

  if (NOT found)
    return NULL;

  // The primary key constraint name is the name of the object appended
  // with "_PK";
  ComTdbVirtTableConstraintInfo * constrInfo = NULL;

  // Setup the primary key information as a unique constraint
  Lng32 uniqueInfoSize = 1;
  constrInfo = new(STMTHEAP) ComTdbVirtTableConstraintInfo[uniqueInfoSize];
  constrInfo->baseTableName = (char*)extTableName.data();

  NAString constrName = extTableName;
  constrName += "_PK";
  constrInfo->constrName = (char*)constrName.data();

  constrInfo->constrType = 3; // pkey_constr

  constrInfo->colCount = numKeys; //keyInfoSize;
  constrInfo->keyInfoArray = (ComTdbVirtTableKeyInfo *)keyInfo;

  constrInfo->numRingConstr = 0;
  constrInfo->ringConstrArray = NULL;
  constrInfo->numRefdConstr = 0;
  constrInfo->refdConstrArray = NULL;

  constrInfo->checkConstrLen = 0;
  constrInfo->checkConstrText = NULL;

  if (getSpecialTableInfo(STMTHEAP, catName, schName, objName,
                          extTableName, COM_BASE_TABLE_OBJECT, tableInfo))
    return NULL;

  tableDesc =
    Generator::createVirtualTableDesc
    ((char*)extTableName.data(),
     NULL, // let it decide what heap to use
     numCols,
     colInfo,
     numKeys,
     keyInfo,
     1, constrInfo,
     0, NULL,
     0, NULL,
     tableInfo);

  return tableDesc;
}

Lng32 CmpSeabaseDDL::getTableDefForBinlogOp(ElemDDLColDefArray* colArray,
                                            ElemDDLColRefArray* keyArray,
                                            NAString& cols,
                                            NAString& keys,
                                            NABoolean hasPk)
{
  for (size_t index = 0; index < colArray->entries(); index++)
  {
    char buf[2048];
    memset(buf, 0, 2048);

    ElemDDLColDef *colNode = (*colArray)[index];
      NAString colFamily;
      NAString colName;
      Lng32 datatype, length, precision, scale, dt_start, dt_end, nullable, upshifted;
      ComColumnClass colClass;
      ComColumnDefaultClass defaultClass;
      NAString charset, defVal;
      NAString heading;
      ULng32 hbaseColFlags;
      Int64 colFlags;
      ComLobsStorageType lobStorage;
      NAString compDefnStr;
      if (getColInfo(colNode,
                     FALSE,
                     colFamily,
                     colName,
                     TRUE, //TODO, now I assume it is always algined format for async XDC
                     datatype, length, precision, scale, dt_start, dt_end, 
                     upshifted, nullable,
                     charset, colClass, defaultClass, defVal, heading, 
                     lobStorage, compDefnStr,
                     hbaseColFlags, colFlags))
        return -1;
           //    type null len sys p s charset  
      const char *colClassLit = NULL;
        switch (colClass)
        {
        case COM_UNKNOWN_CLASS:
          colClassLit = COM_UNKNOWN_CLASS_LIT;
          break;
        case COM_SYSTEM_COLUMN:
          colClassLit = COM_SYSTEM_COLUMN_LIT;
          break;
        case COM_USER_COLUMN:
          colClassLit = COM_USER_COLUMN_LIT;
          break;
        case COM_ADDED_USER_COLUMN:
          colClassLit = COM_ADDED_USER_COLUMN_LIT;
          break;
        case COM_MV_SYSTEM_ADDED_COLUMN:
          colClassLit = COM_MV_SYSTEM_ADDED_COLUMN_LIT;
          break;
        } 
      const char* ansitype = getAnsiTypeStrFromFSType(datatype);
      //trim ansitype
      NAString tmpansitype(ansitype);
      NAString stripedstr = tmpansitype.strip(NAString::trailing, ' ');
      sprintf(buf,"%d-%d-%d-%s-%d-%d-%s-%s-%d-%d-%d-%s;" , datatype, nullable, length, colClassLit, precision, scale ,charset.data(),stripedstr.data(),dt_start,dt_end,(int)index,colName.strip(NAString::trailing, ' ').data());
      cols += buf;
  } 

    NAList<NAString> sortList(STMTHEAP, colArray->entries());
    for ( size_t ii = 0; ii < colArray->entries() ; ii++)
    {
      sortList.insert("");
    }
    for ( size_t index = 0; index < keyArray->entries(); index++)
    {
      char buf[2048];
      memset(buf, 0, 2048);
      NAString nas((*keyArray)[index]->getColumnName());
      int tableColNum = (Lng32)
         colArray->getColumnIndex((*keyArray)[index]->getColumnName()) ;

      ElemDDLColDef *colDef = (*colArray)[tableColNum];
      NAType *colType =  colDef->getColumnDataType();
      Lng32 keyLen = colType->getEncodedKeyLength();

      NAString colFamily;
      NAString colName;
      Lng32 datatype, length, precision, scale, dt_start, dt_end, nullable, upshifted;
      ComColumnClass colClass;
      ComColumnDefaultClass defaultClass;
      NAString charset, defVal;
      NAString heading;
      ULng32 hbaseColFlags;
      Int64 colFlags;
      ComLobsStorageType lobStorage;
      NAString compDefnStr;
      ComColumnOrdering keyorder = (*keyArray)[index]->getColumnOrdering();
      if (getColInfo(colDef,
                     FALSE,
                     colFamily,
                     colName,
                     TRUE, //TODO, now I assume it is always algined format for async XDC
                     datatype, length, precision, scale, dt_start, dt_end,
                     upshifted, nullable,
                     charset, colClass, defaultClass, defVal, heading,
                     lobStorage, compDefnStr,
                     hbaseColFlags, colFlags))
        return -1;
        //    type null len sys p s charset  
        const char *colClassLit = NULL;
        switch (colClass)
        {
        case COM_UNKNOWN_CLASS:
          colClassLit = COM_UNKNOWN_CLASS_LIT;
          break;
        case COM_SYSTEM_COLUMN:
          colClassLit = COM_SYSTEM_COLUMN_LIT;
          break;
        case COM_USER_COLUMN:
          colClassLit = COM_USER_COLUMN_LIT;
          break;
        case COM_ADDED_USER_COLUMN:
          colClassLit = COM_ADDED_USER_COLUMN_LIT;
          break;
        case COM_MV_SYSTEM_ADDED_COLUMN:
          colClassLit = COM_MV_SYSTEM_ADDED_COLUMN_LIT;
          break;
        }
      const char* ansitype = getAnsiTypeStrFromFSType(datatype);
      //trim ansitype
      NAString tmpansitype(ansitype);
      NAString stripedstr = tmpansitype.strip(NAString::trailing, ' '); 
      sprintf(buf,"%d-%d-%d-%s-%d-%d-%s-%s-%d-%d-%d-%s-%d;" , datatype, nullable, length, colClassLit, precision, scale ,charset.data(),stripedstr.data(),dt_start,dt_end,tableColNum,colName.strip(NAString::trailing, ' ').data(),keyorder == COM_ASCENDING_ORDER ? 0:1);
      keys += buf;
      //sortList.insertAt(tableColNum,buf);
    }
/*
    //go through sortList , append to keys
    for(CollIndex i = 0; (i < (CollIndex) sortList.entries());i++)
    {
      if(sortList[i] != "") {
        keys += sortList[i];
      }
    }
*/
  return 0;
}

Lng32 CmpSeabaseDDL::getSeabaseColumnInfo(ExeCliInterface *cliInterface,
                                          Int64 objUID,
                                          const NAString &catName,
                                          const NAString &schName,
                                          const NAString &objName,
                                          char *direction,
                                          NABoolean *isTableSalted,
                                          Int16 *numTrafReplicas,
                                          Lng32 *identityColPos,
                                          Lng32 *partialRowSize,
                                          Lng32 *numCols,
                                          ComTdbVirtTableColumnInfo **outColInfoArray)
{
  char query[3000];
  Lng32 cliRC;

  if (identityColPos)
    *identityColPos = -1;

  Queue * tableColInfo = NULL;
  str_sprintf(query, "select column_name, column_number, column_class, "
    "fs_data_type, column_size, column_precision, column_scale, "
    "datetime_start_field, datetime_end_field, trim(is_upshifted), column_flags, "
    "nullable, trim(character_set), default_class, default_value, "
    "trim(column_heading), hbase_col_family, hbase_col_qualifier, direction, "
    "is_optional, flags  from %s.\"%s\".%s "
    "where object_uid = %ld and direction in (%s)"
    "order by 2 ",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_COLUMNS,
              objUID,
              direction);
  cliRC = cliInterface->fetchAllRows(tableColInfo, query, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0)
  {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      return -1;
  }

  *numCols = tableColInfo->numEntries();
  ComTdbVirtTableColumnInfo *colInfoArray = 
    new(STMTHEAP) ComTdbVirtTableColumnInfo[*numCols];
  NABoolean tableIsSalted = FALSE;
  Int16 tableTrafReplicas = 0;
  if (partialRowSize)
    *partialRowSize = 0;
  tableColInfo->position();

  NABoolean compositeTypeExists = FALSE;
  for (Lng32 idx = 0; idx < *numCols; idx++)
  {
      OutputInfo * oi = (OutputInfo*)tableColInfo->getNext(); 
      ComTdbVirtTableColumnInfo &colInfo = colInfoArray[idx];

      char * data = NULL;
      Lng32 len = 0;

      // get the column name
      oi->get(0, data, len);
      colInfo.colName = new(STMTHEAP) char[len + 1];
      strcpy((char*)colInfo.colName, data);
      
      colInfo.colNumber = *(Lng32*)oi->get(1);

      NAString colClass((char*)oi->get(2));
      colClass = colClass.strip(NAString::trailing, ' ');
      if (colClass == COM_USER_COLUMN_LIT)
        colInfo.columnClass = COM_USER_COLUMN;
      else if (colClass == COM_SYSTEM_COLUMN_LIT)
        colInfo.columnClass = COM_SYSTEM_COLUMN;
      else if (colClass == COM_ADDED_USER_COLUMN_LIT)
        colInfo.columnClass = COM_ADDED_USER_COLUMN;
      else if (colClass == COM_ADDED_ALTERED_USER_COLUMN_LIT)
        colInfo.columnClass = COM_ADDED_ALTERED_USER_COLUMN;
      else if (colClass == COM_MV_SYSTEM_ADDED_COLUMN_LIT)
        colInfo.columnClass = COM_MV_SYSTEM_ADDED_COLUMN;
      else if (colClass == COM_ALTERED_USER_COLUMN_LIT)
        colInfo.columnClass = COM_ALTERED_USER_COLUMN;
      else
        colInfo.columnClass = COM_UNKNOWN_CLASS;

      colInfo.datatype = *(Lng32*)oi->get(3);
      
      colInfo.length = *(Lng32*)oi->get(4);

      colInfo.precision = *(Lng32*)oi->get(5);

      colInfo.scale = *(Lng32*)oi->get(6);

      colInfo.dtStart = *(Lng32 *)oi->get(7);

      colInfo.dtEnd = *(Lng32 *)oi->get(8);

      if (strcmp((char*)oi->get(9), "Y") == 0)
        colInfo.upshifted = -1;
      else
        colInfo.upshifted = 0;

      colInfo.hbaseColFlags = *(ULng32 *)oi->get(10);

      colInfo.nullable = *(Lng32 *)oi->get(11);

      colInfo.charset = 
        (SQLCHARSET_CODE)CharInfo::getCharSetEnum((char*)oi->get(12));

      colInfo.defaultClass = (ComColumnDefaultClass)*(Lng32 *)oi->get(13);

      NAString tempDefVal;
      NAString initDefVal;
      data = NULL;
      if (colInfo.defaultClass == COM_USER_DEFINED_DEFAULT ||
          colInfo.defaultClass == COM_ALWAYS_COMPUTE_COMPUTED_COLUMN_DEFAULT ||
          colInfo.defaultClass == COM_ALWAYS_DEFAULT_COMPUTED_COLUMN_DEFAULT)
        {
          oi->get(14, data, len);
          if (colInfo.defaultClass != COM_USER_DEFINED_DEFAULT)
            {
              // get computed column definition from text table, but note
              // that for older tables the definition may be stored in
              // COLUMNS.DEFAULT_VALUE instead (that's returned in "data")
              cliRC = getTextFromMD(cliInterface,
                                    objUID,
                                    COM_COMPUTED_COL_TEXT,
                                    colInfo.colNumber,
                                    tempDefVal);
              if (cliRC < 0)
                {
                  cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
                  return -1;
                }
              if (strcmp(colInfo.colName,
                         ElemDDLSaltOptionsClause::getSaltSysColName()) == 0)
                tableIsSalted = TRUE;
            }
            if (strcmp(colInfo.colName,
                         ElemDDLReplicateClause::getReplicaSysColName()) == 0)
              tableTrafReplicas = str_atoi(data,len);
        }
      else if (colInfo.defaultClass == COM_FUNCTION_DEFINED_DEFAULT)
        {
          oi->get(14, data, len);
          tempDefVal =  data ;
        }
      else if (colInfo.defaultClass == COM_NULL_DEFAULT)
        {
          tempDefVal = "NULL";
        }
      else if (colInfo.defaultClass == COM_USER_FUNCTION_DEFAULT)
        {
          tempDefVal = "USER";
        }
      else if (colInfo.defaultClass == COM_CURRENT_DEFAULT)
        {
          tempDefVal = "CURRENT_TIMESTAMP";
          // get default value
          oi->get(14, data, len);
          initDefVal = data;
        }
      else if (colInfo.defaultClass == COM_CURRENT_UT_DEFAULT)
        {
          tempDefVal = "UNIX_TIMESTAMP()";
        }
      else if (colInfo.defaultClass == COM_UUID_DEFAULT)
        {
          tempDefVal = "UUID()";
        }
      else if (colInfo.defaultClass == COM_SYS_GUID_FUNCTION_DEFAULT)
        {
          tempDefVal = "SYS_GUID()";
        }
      else if ((colInfo.defaultClass == COM_IDENTITY_GENERATED_BY_DEFAULT) ||
               (colInfo.defaultClass == COM_IDENTITY_GENERATED_ALWAYS))
        {
          NAString  userFunc("SEQNUM(");

          NAString seqName;
          SequenceGeneratorAttributes::genSequenceName
            (catName, schName, objName, colInfo.colName,
             seqName);

          NAString fullyQSeq = catName + "." + "\"" + schName + "\"" + "." + "\"" + seqName + "\"";

          tempDefVal = userFunc + fullyQSeq + ")";

          if (identityColPos)
            *identityColPos = idx;
        }
      else if (colInfo.defaultClass == COM_CATALOG_FUNCTION_DEFAULT)
        {
          tempDefVal = "CURRENT_CATALOG";
          oi->get(14, data, len);
          initDefVal = data;
        }
      else if (colInfo.defaultClass == COM_SCHEMA_FUNCTION_DEFAULT)
        {
          tempDefVal = "CURRENT_SCHEMA";
          oi->get(14, data, len);
          initDefVal = data;
        }

      if (! tempDefVal.isNull())
        {
          data = (char*)tempDefVal.data();
          len = tempDefVal.length();
        }

      if (colInfo.defaultClass != COM_NO_DEFAULT)
        {
          colInfo.defVal = new(STMTHEAP) char[len + 2];
          str_cpy_all((char*)colInfo.defVal, data, len);
          char * c = (char*)colInfo.defVal;
          c[len] = 0;
          c[len+1] = 0;

          if (colInfo.defaultClass == COM_CURRENT_DEFAULT ||
               colInfo.defaultClass == COM_CATALOG_FUNCTION_DEFAULT ||
               colInfo.defaultClass == COM_SCHEMA_FUNCTION_DEFAULT)
            {
              int initLen = initDefVal.length();
              colInfo.initDefVal = new(STMTHEAP) char[initLen + 2];
              str_cpy_all((char*)colInfo.initDefVal, initDefVal.data(), initLen);
              char * ch = (char*)colInfo.initDefVal;
              ch[initLen] = 0;
              ch[initLen+1] = 0;
            }
        }
      else
        colInfo.defVal = NULL;

      oi->get(15, data, len);
      if (len > 0)
        {
          colInfo.colHeading = new(STMTHEAP) char[len + 1];
          strcpy((char*)colInfo.colHeading, data);
        }
      else
        colInfo.colHeading = NULL;

      oi->get(16, data, len);
      colInfo.hbaseColFam = new(STMTHEAP) char[len + 1];
      strcpy((char*)colInfo.hbaseColFam, data);

      oi->get(17, data, len);
      colInfo.hbaseColQual = new(STMTHEAP) char[len + 1];
      strcpy((char*)colInfo.hbaseColQual, data);

      strcpy(colInfo.paramDirection, (char*)oi->get(18));
      if (*((char*)oi->get(19)) == 'Y')
         colInfo.isOptional = 1;
      else
         colInfo.isOptional = 0;

      colInfo.colFlags = *(Int64 *)oi->get(20);

      // temporary code, until we have updated flags to have the salt
      // flag set for all tables, even those created before end of November
      // 2014, when the flag was added during Trafodion R1.0 development
      if (colInfo.defaultClass == COM_ALWAYS_COMPUTE_COMPUTED_COLUMN_DEFAULT &&
          strcmp(colInfo.colName,
                 ElemDDLSaltOptionsClause::getSaltSysColName()) == 0)
        colInfo.colFlags |=  SEABASE_COLUMN_IS_SALT;

      if (colInfo.colFlags & SEABASE_COLUMN_IS_COMPOSITE)
        compositeTypeExists = TRUE;
      if (colInfo.colFlags & SEABASE_COLUMN_IS_REPLICA)
      {
        if (tableTrafReplicas < 1)
          CMPASSERT(0);
      }

      // rowsize uses the same algorithm as 
      // method NATable::computeHBaseRowSizeFromMetaData
      if (partialRowSize)
        *partialRowSize += 
          strlen(colInfo.hbaseColFam) + 
          strlen(colInfo.hbaseColQual) + 
          colInfo.length;

   } // for

  if (compositeTypeExists)
    {
      NAString compTypeStr;
      Lng32 numCompFields = 0;
      const char * compEntry = NULL;

      // definition of composite columns
      // Format:  numCols  colNumber  defnSize defnStr...
      //          8 bytes  8 bytes    8 bytes  defnSize bytes
      //                   Repeat for numCols entries
      cliRC = getTextFromMD(cliInterface,
                            objUID,
                            COM_COMPOSITE_DEFN_TEXT,
                            0,
                            compTypeStr);
      if (cliRC < 0)
        {
          cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
          return -1;
        }

      // first 8 bytes contain number of struct entries
      if (! compTypeStr.isNull())
        {
          compEntry = compTypeStr.data();
          numCompFields = str_atoi(compEntry, 8);
        }

      for (Lng32 idx = 0; idx < *numCols; idx++)
        {
          ComTdbVirtTableColumnInfo &colInfo = colInfoArray[idx];

          // if column has a composite datatype (array or struct), read info
          // about it from the TEXT table.
          if (colInfo.colFlags & SEABASE_COLUMN_IS_COMPOSITE)
            {
              NABoolean found = FALSE;
              compEntry = (numCompFields > 0 ? compTypeStr.data() + 8 : NULL);
              for (int i = 0; ((NOT found) && (i < numCompFields)); i++)
                {
                  Lng32 colNumber = str_atoi(compEntry, 8);
                  compEntry += 8;
                  Lng32 defnSize = str_atoi(compEntry, 8);
                  compEntry += 8;
                  if (colNumber == colInfo.colNumber)
                    {
                      char * comp_str = new(STMTHEAP) char[defnSize +1];
                      str_cpy_all(comp_str, (char*)compEntry, defnSize);
                      comp_str[defnSize] = 0;
                      colInfo.compDefnStr = comp_str;
                      found = TRUE;
                    }
                  compEntry += defnSize;
                } // for composite fields
              
            } // if composite type
        } // for columns

    } // if compositeTypeExists

   if (isTableSalted != NULL)
     *isTableSalted = tableIsSalted;
   if (numTrafReplicas != NULL)
     *numTrafReplicas = tableTrafReplicas;
   *outColInfoArray = colInfoArray;
   return *numCols;
}

ComTdbVirtTableSequenceInfo * CmpSeabaseDDL::getSeabaseSequenceInfo(
 const NAString &catName, 
 const NAString &schName, 
 const NAString &seqName,
 NAString &extSeqName,
 Int32 & objectOwner,
 Int32 & schemaOwner,
 Int64 & seqUID)
{
  Lng32 retcode = 0;
  Lng32 cliRC = 0;

  NAString schNameL = "\"";
  schNameL += schName;
  schNameL += "\"";

  NAString seqNameL = "\"";
  seqNameL += seqName;
  seqNameL += "\"";
  ComObjectName coName(catName, schNameL, seqNameL);
  extSeqName = coName.getExternalName(TRUE);

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, 
  CmpCommon::context()->sqlSession()->getParentQid());

  objectOwner = NA_UserIdDefault;
  seqUID = -1;
  schemaOwner = NA_UserIdDefault;
  Int64 objectFlags =  0 ;
  Int64 objDataUID = 0;
  seqUID = getObjectInfo(&cliInterface,
                         catName.data(), schName.data(), seqName.data(),
                         COM_SEQUENCE_GENERATOR_OBJECT,  
                         objectOwner,schemaOwner,objectFlags,objDataUID,TRUE/*report error*/);
  if (seqUID == -1 || objectOwner == 0)
  {
    // There may not be an error in the diags area, if not, add an error
    if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_) == 0)
      SEABASEDDL_INTERNAL_ERROR("getting object UID and owners for get sequence command");
    return NULL;
  }

 char buf[4000];

 str_sprintf(buf, "select fs_data_type, start_value, increment, max_value, min_value, cycle_option, cache_size, next_value, seq_type, redef_ts, flags from %s.\"%s\".%s  where seq_uid = %ld",
             getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_SEQ_GEN,
             seqUID);

  Queue * seqQueue = NULL;
  cliRC = cliInterface.fetchAllRows(seqQueue, buf, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      return NULL;
    }
 
  if ((seqQueue->numEntries() == 0) ||
      (seqQueue->numEntries() > 1))
    {
      *CmpCommon::diags() << DgSqlCode(-4082)
                          << DgTableName(extSeqName);
      
      return NULL;
    }

  ComTdbVirtTableSequenceInfo *seqInfo = 
    new (STMTHEAP) ComTdbVirtTableSequenceInfo();

  seqQueue->position();
  OutputInfo * vi = (OutputInfo*)seqQueue->getNext(); 
  ComSequenceGeneratorType sgType;
  if(memcmp(vi->get(8),"E",1) == 0)
    sgType = COM_EXTERNAL_SG;
  else if (memcmp(vi->get(8),"I",1) == 0)
    sgType = COM_INTERNAL_SG;
  else  if(memcmp(vi->get(8),"C",1) == 0)
    sgType = COM_INTERNAL_COMPUTED_SG;
  else  if(memcmp(vi->get(8),"S",1) == 0)
    sgType = COM_SYSTEM_SG;
  else
    sgType = COM_UNKNOWN_SG;
    
  
  seqInfo->datatype = *(Lng32*)vi->get(0);
  seqInfo->startValue = *(Int64*)vi->get(1);
  seqInfo->increment = *(Int64*)vi->get(2);
  seqInfo->maxValue = *(Int64*)vi->get(3);
  seqInfo->minValue = *(Int64*)vi->get(4);
  seqInfo->cycleOption = (memcmp(vi->get(5), COM_YES_LIT, 1) == 0 ? 1 : 0);
  seqInfo->cache  = *(Int64*)vi->get(6);
  seqInfo->nextValue  = *(Int64*)vi->get(7);
  seqInfo->seqType = sgType;
  seqInfo->seqUID = seqUID;
  seqInfo->redefTime = *(Int64*)vi->get(9);
  seqInfo->flags = *(Int64*)vi->get(10);

  return seqInfo;
}

// ----------------------------------------------------------------------------
// method:  getSeabaseSchemaDesc
//
// returns a TrafDesc based on a schema object.
// ----------------------------------------------------------------------------
TrafDesc * CmpSeabaseDDL::getSeabaseSchemaDesc(
  const NAString &catName,
  const NAString &schName,
  const Int32 ctlFlags,
  Int32 &packedDescLen)
{
  TrafDesc * tableDesc = NULL;

  Lng32 retcode = 0;
  Lng32 cliRC = 0;

  ComObjectName objectName(catName, schName, SEABASE_SCHEMA_OBJECTNAME,
                           COM_UNKNOWN_NAME, ComAnsiNamePart::INTERNAL_FORMAT);
  NAString extSchName = objectName.getExternalName(TRUE);

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL,
     CmpCommon::context()->sqlSession()->getParentQid());

  Int32 objectOwner = NA_UserIdDefault;
  Int32 schemaOwner = NA_UserIdDefault;
  Int64 schemaUID = -1;
  Int64 objectFlags = 0;
  Int64 objDataUID = 0;
  NABoolean isPrivateSchema = TRUE;

  if (switchCompiler(CmpContextInfo::CMPCONTEXT_TYPE_META))
    return NULL;

  schemaUID = getObjectInfo(&cliInterface,
                         catName.data(), schName.data(), SEABASE_SCHEMA_OBJECTNAME,
                         COM_PRIVATE_SCHEMA_OBJECT,
                         objectOwner,schemaOwner,objectFlags,objDataUID,FALSE/*no error*/);


  if (schemaUID == -1 || objectOwner == 0)
  {
    // This may be a shared schema, check again.
    schemaUID = getObjectInfo(&cliInterface,
                              catName.data(), schName.data(), SEABASE_SCHEMA_OBJECTNAME,
                              COM_SHARED_SCHEMA_OBJECT,
                              objectOwner,schemaOwner,objectFlags,objDataUID,FALSE/*no error*/);

    // If neither, the return NULL
    if (schemaUID == -1 || objectOwner == NA_UserIdDefault)
    {
      switchBackCompiler();

      *CmpCommon::diags() << DgSqlCode(-CAT_SCHEMA_DOES_NOT_EXIST_ERROR)
                          << DgString0(catName.data())
                          << DgString1(schName.data());
      return NULL;
    }
    isPrivateSchema = FALSE;
  }
      

  if ((ctlFlags & READ_OBJECT_DESC) && // read stored descriptor
      ((objectFlags & MD_OBJECTS_STORED_DESC) != 0) && // stored desc available
      ((objectFlags & MD_OBJECTS_DISABLE_STORED_DESC) == 0)) // not disabled
    {
      // if good stored desc was retrieved, return it.
      // Otherwise, continue and generate descriptor the old fashioned way.
      retcode =
        checkAndGetStoredObjectDesc(
             &cliInterface, schemaUID, &tableDesc, FALSE,
             ActiveSchemaDB()->getNATableDB()->getHeap());
      if (retcode == 0)
        {
          switchBackCompiler();
          CmpCommon::diags()->clear();
          if (! tableDesc)
            return NULL;

          QRLogger::log(CAT_SQL_EXE, LL_DEBUG, 
                        "Schema definition for %s.%s retrieved from SQL descriptor store (TEXT)", 
                       catName.data(), schName.data());
          return tableDesc;
        }

      // clear diags and continue
      CmpCommon::diags()->negateAllErrors();
    }

  // get namespace of this schema, if it was created in a non-default ns.
  NAString schemaNamespace;
  if ( getTextFromMD(&cliInterface, schemaUID, COM_OBJECT_NAMESPACE, 0,
                          schemaNamespace) )
    {
      switchBackCompiler();
      processReturn();
      return NULL;
    }
     
  ComObjectType objectType = isPrivateSchema ? COM_PRIVATE_SCHEMA_OBJECT : 
                                               COM_SHARED_SCHEMA_OBJECT;
  ComTdbVirtTablePrivInfo * privInfo =
    getSeabasePrivInfo(&cliInterface, catName, schName, schemaUID, 
                       objectType, schemaOwner);

  switchBackCompiler();

  ComTdbVirtTableTableInfo * tableInfo =
    new(STMTHEAP) ComTdbVirtTableTableInfo[1];
  tableInfo->tableName = extSchName.data();
  tableInfo->createTime = 0;
  tableInfo->redefTime = 0;
  tableInfo->objUID = schemaUID;
  tableInfo->objDataUID  = objDataUID ;
  tableInfo->schemaUID = schemaUID;
  tableInfo->isAudited = 0;
  tableInfo->validDef = 1;
  tableInfo->objOwnerID = objectOwner;
  tableInfo->schemaOwnerID = schemaOwner;
  tableInfo->hbaseCreateOptions = NULL;
  tableInfo->objectFlags = objectFlags;
  tableInfo->tablesFlags = 0;
  tableInfo->tableNamespace = 
    (NOT schemaNamespace.isNull() ? schemaNamespace.data() : NULL);

  tableDesc =
    Generator::createVirtualTableDesc
    ((char*)extSchName.data(),
     NULL, // let it decide what heap to use
     0, NULL, // colInfo
     0, NULL, // keyInfo
     0, NULL, // constrInfo
     0, NULL, //indexInfo
     0, NULL, // viewInfo
     tableInfo,
     NULL, // seqInfo
     NULL, // statsInfo
     NULL, // endKeyArray, 
     ((ctlFlags & GEN_PACKED_DESC) != 0),
     &packedDescLen,
     !isPrivateSchema, 
     privInfo);

  QRLogger::log(CAT_SQL_EXE, LL_DEBUG, 
                "Schema definition for %s.%s generated from SQL metadata", 
               catName.data(), schName.data());
  return tableDesc;
}

// ****************************************************************************
// Method: getSeabasePrivInfo
//
// This method retrieves the list of privilege descriptors for each user that
// has been granted an object or column level privilege on the object.
// ****************************************************************************
ComTdbVirtTablePrivInfo * CmpSeabaseDDL::getSeabasePrivInfo(
  ExeCliInterface *cliInterface,
  const NAString &catName,
  const NAString &schName,
  const Int64 objUID,
  const ComObjectType objType,
  const Int32 schemaOwner)
{
  if (!isAuthorizationEnabled())
    return NULL;

  // Get the schema UID
  Int64 schUID = 0;
  char schObjTypeLit [3];
  if (objType == COM_PRIVATE_SCHEMA_OBJECT || objType == COM_SHARED_SCHEMA_OBJECT)
    schUID = objUID; 
  else
      schUID = 
        getObjectUID(cliInterface,
                     catName.data(), schName.data(), SEABASE_SCHEMA_OBJECTNAME,
                     NULL, NULL, "object_type in ('SS', 'PS')", schObjTypeLit);

  if (schUID == -1)
    schUID = 0;

  ComObjectType schObjType = PrivMgr::ObjectLitToEnum(schObjTypeLit);

  // Prepare to call privilege manager
  NAString MDLoc;
  CONCAT_CATSCH(MDLoc, getSystemCatalog(), SEABASE_MD_SCHEMA);
  NAString privMgrMDLoc;
  CONCAT_CATSCH(privMgrMDLoc, getSystemCatalog(), SEABASE_PRIVMGR_SCHEMA);

  PrivStatus privStatus = STATUS_GOOD;
  ComTdbVirtTablePrivInfo *privInfo = new (heap_) ComTdbVirtTablePrivInfo();
  privInfo->privmgr_desc_list = new (STMTHEAP) PrivMgrDescList(STMTHEAP);

  // Summarize privileges for object
  PrivMgrCommands command(std::string(MDLoc.data()),
                          std::string(privMgrMDLoc.data()),
                          CmpCommon::diags());
  if (command.getPrivileges(objUID, schemaOwner, schUID, schObjType, objType, 
                            *privInfo->privmgr_desc_list) != STATUS_GOOD)
    {
      *CmpCommon::diags() << DgSqlCode(-CAT_UNABLE_TO_RETRIEVE_PRIVS);
      return NULL;
    }

#ifndef NDEBUG
  if (getenv("NATABLE_DEBUG"))
    {
      cout << "Retrieved privs from metadata, " << "entries: " << privInfo->privmgr_desc_list->entries() << endl;
      privInfo->privmgr_desc_list->print();
    }
#endif

  return privInfo;
}


TrafDesc * CmpSeabaseDDL::getSeabaseLibraryDesc(
   const NAString &catName, 
   const NAString &schName, 
   const NAString &libraryName)
   
{

  TrafDesc * tableDesc = NULL;

  NAString extLibName;
  Int32 objectOwner = 0;
  Int32 schemaOwner = 0;
  Int64 objectFlags =  0 ;
  Int64 objDataUID = 0;
  
  
  
  char query[4000];
  char buf[4000];

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, 
  CmpCommon::context()->sqlSession()->getParentQid());

   if (switchCompiler(CmpContextInfo::CMPCONTEXT_TYPE_META))
     return NULL;
  Int64 libUID = getObjectInfo(&cliInterface, 
                               catName.data(), schName.data(),
                               libraryName.data(),
                               COM_LIBRARY_OBJECT,
                               objectOwner, schemaOwner,objectFlags,objDataUID);
  if (libUID == -1)
    {
      switchBackCompiler();
      return NULL;
    }
     
  str_sprintf(buf, "SELECT library_filename, version "
              "FROM %s.\"%s\".%s "
              "WHERE library_uid = %ld ",
              getSystemCatalog(),SEABASE_MD_SCHEMA,SEABASE_LIBRARIES,libUID);

  Int32 cliRC = cliInterface.fetchRowsPrologue(buf, TRUE/*no exec*/);
  if (cliRC < 0)
  {
     cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
     switchBackCompiler();
     return NULL;
  }

  cliRC = cliInterface.clearExecFetchClose(NULL, 0);
  if (cliRC < 0)
  {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
     switchBackCompiler();
     return NULL;
  }
  if (cliRC == 100) // did not find the row
  {
     cliInterface.clearGlobalDiags();

     *CmpCommon::diags() << DgSqlCode(-CAT_OBJECT_DOES_NOT_EXIST_IN_TRAFODION)
                         << DgString0(libraryName);
     switchBackCompiler();
     return NULL;
  }

  switchBackCompiler();
  char * ptr = NULL;
  Lng32 len = 0;

  ComTdbVirtTableLibraryInfo *libraryInfo = new (STMTHEAP) ComTdbVirtTableLibraryInfo();
  
  if (libraryInfo == NULL)
     return NULL;

  libraryInfo->library_name = libraryName.data();
  cliInterface.getPtrAndLen(1, ptr, len);
  libraryInfo->library_filename = new (STMTHEAP) char[len + 1];    
  str_cpy_and_null((char *)libraryInfo->library_filename, ptr, len, '\0', ' ', TRUE);
  cliInterface.getPtrAndLen(2, ptr, len);
  libraryInfo->library_version = *(Int32 *)ptr;
  libraryInfo->object_owner_id = objectOwner;
  libraryInfo->schema_owner_id = schemaOwner;
  libraryInfo->library_UID = libUID;
  
  TrafDesc *library_desc = Generator::createVirtualLibraryDesc(
            libraryName.data(),
            libraryInfo, NULL);

  processReturn();
  return library_desc;
  
}

TrafDesc * CmpSeabaseDDL::getSeabaseSequenceDesc(const NAString &catName, 
                                                 const NAString &schName, 
                                                 const NAString &seqName)
{
  TrafDesc * tableDesc = NULL;

  NAString extSeqName;
  Int32 objectOwner = 0;
  Int32 schemaOwner = 0;
  Int64 seqUID = -1;
  ComTdbVirtTableSequenceInfo * seqInfo =
    getSeabaseSequenceInfo(catName, schName, seqName, extSeqName, 
                           objectOwner, schemaOwner, seqUID);

  if (! seqInfo)
    {
      return NULL;
    }
  
  ExeCliInterface cliInterface(STMTHEAP, 0, NULL,
  CmpCommon::context()->sqlSession()->getParentQid());

  ComTdbVirtTablePrivInfo * privInfo = 
    getSeabasePrivInfo(&cliInterface, catName, schName, seqUID, 
                       COM_SEQUENCE_GENERATOR_OBJECT, schemaOwner);

  ComTdbVirtTableTableInfo * tableInfo =
    new(STMTHEAP) ComTdbVirtTableTableInfo[1];
  tableInfo->tableName = extSeqName.data();
  tableInfo->createTime = 0;
  tableInfo->redefTime = 0;
  tableInfo->objUID = seqUID;
  tableInfo->objDataUID  = 0;
  tableInfo->schemaUID = 0; // revoke usage at the schema level?
  tableInfo->isAudited = 0;
  tableInfo->validDef = 1;
  tableInfo->objOwnerID = objectOwner;
  tableInfo->schemaOwnerID = schemaOwner;
  tableInfo->hbaseCreateOptions = NULL;
  tableInfo->objectFlags = 0;
  tableInfo->tablesFlags = 0;

  tableDesc =
    Generator::createVirtualTableDesc
    ((char*)extSeqName.data(),
     NULL, // let it decide what heap to use
     0, NULL, // colInfo
     0, NULL, // keyInfo
     0, NULL, // constrInfo
     0, NULL, //indexInfo
     0, NULL, // viewInfo
     tableInfo,
     seqInfo,
	 NULL, // statsInfo
     NULL, // endKeyArray, 
     FALSE, NULL, FALSE, // genPackedDesc, packedDescLen, isSharedSchema
     privInfo);
  
  return tableDesc;
}

//After some complicated region rebalance operations
//like split/move region,we sometimes get empty end key
//of regions.That will cause Compiler Internal Error.
//So we should check table regions here.
//The region of table that is NOT:
//1. The only region in the table
//2. The last region in a table
//The region's End Key should not be empty.
//If we get empty End key here, since 
//one region's End Key equals next region's Start Key,
//we can use next region's Start Key instead.
void lcl_checkEndKeys(NAArray<HbaseStr>* endKeyArray, ExpHbaseInterface* ehi, const NAString& extNameForHbase)
{
  if (!endKeyArray || !ehi)
    return;
  Int32 entries = endKeyArray->entries();
  //A region with an empty end key is the last region in a table,
  //so we don't need to check the last region's End Key.
  NAArray<HbaseStr>* startKeyArray  = NULL;
  for (Int32 i = 0; i<entries-1; ++i)
    {
      Int32 len = endKeyArray->at(i).len;
      if (0 == len)//empty End Key
        {
           //use next region's Start Key instead.
           Int32 startIdx = i+1;
           if (!startKeyArray)
             startKeyArray  = ehi->getRegionBeginKeys(extNameForHbase);
           if (startKeyArray)
             {
               Int32 srclen = startKeyArray->at(startIdx).len;
               if (srclen)
                 {
                   if (endKeyArray->at(i).val)
                     NADELETEBASIC(endKeyArray->at(i).val, ehi->getHeap());
                   endKeyArray->at(i).val = new (ehi->getHeap()) char[srclen];
                   memcpy(endKeyArray->at(i).val, startKeyArray->at(startIdx).val, srclen);
                 }
             }
           break;
        }
    }
}

short CmpSeabaseDDL::genHbaseRegionDescs(TrafDesc * desc,
                                         const NAString &catName, 
                                         const NAString &schName, 
                                         const NAString &objName,
                                         const NAString &nameSpace)
{
  if (! desc)
    return -1;

  ExpHbaseInterface* ehi = allocEHI(COM_STORAGE_HBASE);
  if (ehi == NULL) 
    return -1;

  NAString extNameForHbase;
  if ((catName != HBASE_SYSTEM_CATALOG) ||
     ((schName != HBASE_ROW_SCHEMA) && (schName != HBASE_CELL_SCHEMA)))
    extNameForHbase = genHBaseObjName(catName, schName, objName, nameSpace);
  else
    // for HBASE._ROW_.objName or HBASE._CELL_.objName, 
    // just pass objName
    extNameForHbase = objName;

  NAArray<HbaseStr>* endKeyArray  = 
    ehi->getRegionEndKeys(extNameForHbase);
  
  lcl_checkEndKeys(endKeyArray, ehi, extNameForHbase);

  TrafDesc * regionKeyDesc = 
    Generator::assembleDescs(endKeyArray, NULL);
  
  deallocEHI(ehi);

  TrafTableDesc* tDesc = desc->tableDesc();
  if (tDesc)
    tDesc->hbase_regionkey_desc = regionKeyDesc;
  else {
    TrafIndexesDesc* iDesc = desc->indexesDesc();
  if (iDesc)
    iDesc->hbase_regionkey_desc = regionKeyDesc;
  else
    return -1;
  }
  
  return 0;
}

// histogram and histint queries processing must remain in sync with
// queries defined in ustat/hs_read.cpp, method HSCursorGetHistQueries
short CmpSeabaseDDL::generateStoredStats(
     Int64 tableUID,
     Int64 objDataUID,
     const NAString &catName, 
     const NAString &schName, 
     const NAString &objName,
     const NAString &nameSpace,
     Int32 ctlFlags,
     Int32 partialRowSize,
     Int32 numCols,
     ExpHbaseInterface * ehi,
     ComTdbVirtTableStatsInfo* &statsInfo)
{
  Lng32 retcode;

  NABoolean statsExist = false;
  NAString histogramTableName;
  NAString histintsTableName;
  
  if (! statsInfo)
    statsInfo = new(STMTHEAP) ComTdbVirtTableStatsInfo[1];
  statsInfo->rowcount = -1;
  statsInfo->hbtBlockSize = -1;
  statsInfo->hbtIndexLevels = -1;
  statsInfo->numHistograms = 0;
  statsInfo->numHistIntervals = 0;
  statsInfo->histogramInfo = NULL;
  statsInfo->histintInfo = NULL;

  // generate and store estimated stats 
  const NAString extNameForHbase = genHBaseObjName
    (catName, schName, objName, nameSpace, objDataUID);

  HbaseStr fqTblName;
  fqTblName.val = (char *)extNameForHbase.data();
  fqTblName.len = extNameForHbase.length();
  
  if (ctlFlags & FLUSH_DATA)
    {
      NAString tempSnapTbl(extNameForHbase);
      tempSnapTbl += "_GS_SNAP";
      
      HbaseStr newHbaseTable;
      newHbaseTable.val = (char*)tempSnapTbl.data();
      newHbaseTable.len = tempSnapTbl.length();
      
      retcode = ehi->copy(fqTblName, newHbaseTable, TRUE);
      if (retcode < 0)
        {
          *CmpCommon::diags() << DgSqlCode(-8448)
                              << DgString0((char*)"ExpHbaseInterface::copy()")
                              << DgString1(getHbaseErrStr(-retcode))
                              << DgInt0(-retcode)
                              << DgString2((char*)GetCliGlobals()->getJniErrorStr());
          
          return -1;
        }
      
      retcode = ehi->drop(newHbaseTable, FALSE, TRUE);
      if (retcode < 0)
        {
          *CmpCommon::diags() << DgSqlCode(-8448)
                              << DgString0((char*)"ExpHbaseInterface::drop()")
                              << DgString1(getHbaseErrStr(-retcode))
                              << DgInt0(-retcode)
                              << DgString2((char*)GetCliGlobals()->getJniErrorStr());
          
          return -1;
        }
    }
  
  Int32 hbtIndexLevels = 
    (ActiveSchemaDB()->getDefaults()).getAsLong(HBASE_INDEX_LEVEL);
  Int32 hbtBlockSize = 
    (ActiveSchemaDB()->getDefaults()).getAsLong(HBASE_BLOCK_SIZE);
  
  // call getHbaseTableInfo if index level is set to 0
  if (hbtIndexLevels == 0)
    {
      retcode = ehi->getHbaseTableInfo(fqTblName,
                                       hbtIndexLevels,
                                       hbtBlockSize);
    }
  
  statsInfo->hbtBlockSize = hbtBlockSize;
  statsInfo->hbtIndexLevels = hbtIndexLevels;



  NAString histogramQuery;
  NAString histintsQuery;

  Lng32 cliRC = 0;
  ExeCliInterface cliInterface
    (STMTHEAP, 0, NULL, 
     CmpCommon::context()->sqlSession()->getParentQid());

  Queue * histogramQueue = NULL;
  cliRC = cliInterface.fetchAllRows(histogramQueue, histogramQuery.data(),
                                    0, FALSE, FALSE, TRUE);

  if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

      processReturn();

      return -1;
    }

   // Histogram query
  /* select histogram_id, column_number, colcount, interval_count, rowcount, total_uec, juliantimestamp(stats_time), low_value, high_value, juliantimestamp(read_time), read_count, sample_Secs, col_secs, sample_percent, cv, reason, v1, v2, TRANSLATE(V5 USING UCS2TOUTF8) from %s where table_uid = %ld and reason != ' ' %s order by table_uid, histogram_id, col_position */
 
  ComTdbVirtTableHistogramInfo * histogramInfoArray = NULL;
  histogramInfoArray = 
    new(STMTHEAP) ComTdbVirtTableHistogramInfo[histogramQueue->numEntries()];
  for (Lng32 idx = 0; idx < histogramQueue->numEntries(); idx++)
    {
      OutputInfo * vi = (OutputInfo*)histogramQueue->getNext(); 

      TrafHistogramDesc &hdesc = histogramInfoArray[idx].histogramDesc_;
      hdesc.nodetype = DESC_HISTOGRAM_TYPE;
      hdesc.version = TrafDesc::CURR_VERSION;
      hdesc.histogram_id = *(Int64*)vi->get(0);
      hdesc.column_number = *(Int32*)vi->get(1);
      hdesc.colcount = *(Int32 *)vi->get(2);
      hdesc.interval_count = *(Int16*)vi->get(3);
      hdesc.rowcount = *(Int64*)vi->get(4);
      hdesc.total_uec = *(Int64*)vi->get(5);
      hdesc.stats_time = *(Int64*)vi->get(6);

      if (idx == 0)
        statsInfo->rowcount = hdesc.rowcount;

      char * ptr = NULL;
      Lng32 len = 0;
      vi->get(7, ptr, len);
      hdesc.low_value = new(STMTHEAP) char[sizeof(len) + len];
      *(Lng32*)hdesc.low_value = len;
      str_cpy_all(&hdesc.low_value[sizeof(len)], ptr, len);

      vi->get(8, ptr, len);
      hdesc.high_value = new(STMTHEAP) char[sizeof(len) + len];
      *(Lng32*)hdesc.high_value = len;
      str_cpy_all(&hdesc.high_value[sizeof(len)], ptr, len);

      hdesc.read_time = *(Int64*)vi->get(9);
      hdesc.read_count = *(Int16*)vi->get(10);
      hdesc.sample_secs = *(Int64*)vi->get(11);
      hdesc.col_secs = *(Int64*)vi->get(12);
      hdesc.sample_percent = *(Int16*)vi->get(13);
      hdesc.cv = *(Float64*)vi->get(14);
      hdesc.reason = *(char*)vi->get(15);
      hdesc.v1 = *(Int64*)vi->get(16);
      hdesc.v2 = *(Int64*)vi->get(17);

      // expression text using v5 column
      vi->get(18, ptr, len);
      hdesc.v5 = new(STMTHEAP) char[sizeof(Int16) + len];
      *(Int16*)hdesc.v5 = (Int16)len;
      str_cpy_all(&hdesc.v5[sizeof(Int16)], ptr, len);
      
    }

  statsExist = (histogramQueue->numEntries() > 0 ) ? true : false;
  Queue * histintsQueue = NULL;
  ComTdbVirtTableHistintInfo * histintsInfoArray = NULL;

  if (statsExist)
  {
  //read from SB_HISTOGRAM_INTERVALS if statistisc exists
  cliRC = cliInterface.fetchAllRows(histintsQueue, histintsQuery.data(),
                                    0, FALSE, FALSE, TRUE);

  if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

      processReturn();

      return -1;
    }

  // histints query
  /*
    select histogram_id, interval_number, interval_rowcount, interval_uec, interval_boundary, cast(std_dev_of_freq as double precision), v1, v2, v5 from %s where table_uid = %ld order by table_uid, histogram_id, interval_number;
  */
  histintsInfoArray = 
    new(STMTHEAP) ComTdbVirtTableHistintInfo[histintsQueue->numEntries()];
  for (Lng32 idx = 0; idx < histintsQueue->numEntries(); idx++)
    {
      OutputInfo * vi = (OutputInfo*)histintsQueue->getNext(); 

      TrafHistIntervalDesc &hdesc = histintsInfoArray[idx].histintDesc_;
      hdesc.nodetype = DESC_HIST_INTERVAL_TYPE;
      hdesc.version = TrafDesc::CURR_VERSION;
      hdesc.histogram_id = *(Int64*)vi->get(0);
      hdesc.interval_number = *(Int16*)vi->get(1);
      hdesc.interval_rowcount = *(Int64*)vi->get(2);
      hdesc.interval_uec = *(Int64*)vi->get(3);

      char * ptr = NULL;
      Lng32 len = 0;
      vi->get(4, ptr, len);
      hdesc.interval_boundary = new(STMTHEAP) char[sizeof(len) + len];
      *(Lng32*)hdesc.interval_boundary = len;
      str_cpy_all(&hdesc.interval_boundary[sizeof(len)], ptr, len);

      hdesc.std_dev_of_freq = *(Float64*)vi->get(5);
      hdesc.v1 = *(Int64*)vi->get(6);
      hdesc.v2 = *(Int64*)vi->get(7);
      
      vi->get(8, ptr, len);
      hdesc.v5 = new(STMTHEAP) char[sizeof(len) + len];
      *(Lng32*)hdesc.v5 = len;
      str_cpy_all(&hdesc.v5[sizeof(len)], ptr, len);
    }
  } //if (statsExist)

  if (statsExist)
  {
    statsInfo->numHistograms = histogramQueue->numEntries();
    statsInfo->numHistIntervals = histintsQueue->numEntries();
    statsInfo->histogramInfo = histogramInfoArray;
    statsInfo->histintInfo = histintsInfoArray;
  }
  else
  {
    statsInfo->numHistograms = 0;
    statsInfo->numHistIntervals = 0;
    statsInfo->histogramInfo = NULL;
    statsInfo->histintInfo = NULL;
  }

  if (!statsExist)
  {
    // if statistics does not exist, estimate row count
    Int64 estRowCount;
    Int32 breadCrumb = 0;
    NABoolean useCoprocessor = 
              (CmpCommon::getDefault(HBASE_ESTIMATE_ROW_COUNT_VIA_COPROCESSOR) == DF_ON);
    retcode = ehi->estimateRowCount(fqTblName,
                                  partialRowSize,
                                  numCols,
                                  5000, // retry limit of 5 seconds (5000 milliseconds)
                                  useCoprocessor,
                                  estRowCount /* out */,
                                  breadCrumb /* out*/);
    // TODO: check for error on return
 
    statsInfo->rowcount = estRowCount;

  }
  return 0;
}

Lng32 CmpSeabaseDDL::getIndexInfo(ExeCliInterface &cliInterface,
                                      NABoolean includeInvalidDefs,
                                      Int64 objUID,
                                      Queue* &indexQueue)
{
  Lng32 cliRC = 0;
  char query[4000];

  str_sprintf(query, "select O.catalog_name, O.schema_name, O.object_name, I.keytag, I.is_unique, I.is_explicit, I.key_colcount, I.nonkey_colcount, T.num_salt_partns, T.row_format, I.index_uid, I.FLAGS from %s.\"%s\".%s I, %s.\"%s\".%s O ,  %s.\"%s\".%s T where I.base_table_uid = %ld and I.index_uid = O.object_uid %s and I.index_uid = T.table_uid order by 1,2,3",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_INDEXES,
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS,
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_TABLES,
              objUID,
              (includeInvalidDefs ? " " : " and O.valid_def = 'Y' "));

  //Turn off CQDs MERGE_JOINS and HASH_JOINS to avoid a full table scan of
  //SEABASE_OBJECTS table. Full table scan of SEABASE_OBJECTS table causes
  //simultaneous DDL operations to run into conflict.
  //Make sure to restore the CQDs after this query including error paths.
  cliInterface.holdAndSetCQDs("MERGE_JOINS, HASH_JOINS",
                              "MERGE_JOINS 'OFF', HASH_JOINS 'OFF'");

  cliRC = cliInterface.fetchAllRows(indexQueue, query, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0)
  {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  //restore CQDs.
  cliInterface.restoreCQDs("MERGE_JOINS, HASH_JOINS");
  if (cliRC < 0)
    return -1;

  return 0;
}

TrafDesc * CmpSeabaseDDL::getSeabaseUserTableDesc(const NAString &catName, 
                                                  const NAString &schName, 
                                                  const NAString &objName,
                                                  const ComObjectType objType,
                                                  NABoolean includeInvalidDefs,
                                                  short includeBRLockedTables,
                                                  Int32 ctlFlags,
                                                  Int32 &packedDescLen,
                                                  Int32 cntxtType)
{
  Lng32 retcode = 0;
  Lng32 cliRC = 0;

  ComDiagsArea &diagsArea0  = GetCliGlobals()->currContext()->diags();

  char query[4000];
  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, 
  CmpCommon::context()->sqlSession()->getParentQid());
  
  TrafDesc * tableDesc = NULL;

  if (objType == COM_PRIVATE_SCHEMA_OBJECT || objType == COM_SHARED_SCHEMA_OBJECT)
    {
      tableDesc = getSeabaseSchemaDesc(catName, schName, ctlFlags, packedDescLen);
      return tableDesc;
    }

  Int32 objectOwner =  0 ;
  Int32 schemaOwner =  0 ;
  Int64 objUID      = -1 ;
  Int64 objDataUID  = -1 ;
  Int64 objectFlags =  0 ;
  Int64 schUID      = -1;
  Int64 schObjFlags = 0;

  NAString extName(catName + "." + schName + "." + objName);

  //  Int64 t1 = NA_JulianTimestamp();

  //
  // For performance reasons, whenever possible, we want to issue only one
  // "select" to the OBJECTS metadata table to determine both the existence
  // of the specified table and the objUID for the table.  Since it is more
  // likely that a user query refers to tables (directly or indirectly) that
  // are already in existence, this optimization can save the cost of the
  // existence check for all such user objects.  In the less likely case that
  // an object does not exist we must drop back and re-issue the metadata
  // query for the existence check in order to ensure we get the proper error
  // reported.
  //
  NABoolean checkForValidDef = TRUE;
  if ((includeInvalidDefs) ||
      (Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)) ||
      (objType == COM_INDEX_OBJECT))
    checkForValidDef = FALSE;
  
  if ( objType == COM_UNKNOWN_OBJECT ) // Must have objType
    return NULL;

  objUID = getObjectInfo(&cliInterface,
                         catName.data(), schName.data(), objName.data(),
                         objType, objectOwner, schemaOwner, objectFlags, objDataUID,
                         FALSE /*no error now */,
                         checkForValidDef);

  if ((objUID < 0) && (objType == COM_BASE_TABLE_OBJECT))
    {
      // object type passed in was for a table. Could not find it but
      // this could be a view. Look for that.s
      CmpCommon::diags()->clear();
      objUID = getObjectInfo(&cliInterface,
                             catName.data(), schName.data(), objName.data(), 
                             COM_VIEW_OBJECT,  
                             objectOwner, schemaOwner, objectFlags, objDataUID,
                             FALSE/*no error now*/);
    }
  
  if (objUID < 0)
    {
      NABoolean validDef = FALSE;
      cliRC = getObjectValidDef(&cliInterface, 
                                catName.data(), schName.data(), objName.data(),
                                objType, validDef);
      if ((cliRC == 1) && (NOT validDef)) // found and not valid
        {
          CmpCommon::diags()->clear();
          *CmpCommon::diags() << DgSqlCode(-4254)
                              << DgString0(extName);
        }
          
      processReturn();
      return NULL;
    }

  NABoolean isPartitionTableV2 = false;
  if (objType == COM_BASE_TABLE_OBJECT &&
      (objectFlags & MD_PARTITION_TABLE_V2) != 0)
    isPartitionTableV2 = true;

  NABoolean isPartitionBaseTable = false;
  if (objType == COM_BASE_TABLE_OBJECT &&
      (objectFlags & MD_PARTITION_V2) != 0)
    isPartitionBaseTable = true;
   
  schUID = getObjectUID(&cliInterface,
                        catName.data(), schName.data(), SEABASE_SCHEMA_OBJECTNAME,
                        NULL, NULL, "object_type in ('SS', 'PS')", NULL, FALSE, FALSE,
                        &schObjFlags);

  // if a backup or restore oper is in progress on this table or on the schema, return error.
  if (((schObjFlags & MD_OBJECTS_BR_IN_PROGRESS) != 0) ||
      ((objectFlags & MD_OBJECTS_BR_IN_PROGRESS) != 0))
    {
      NAString brTag;
      if ((schObjFlags & MD_OBJECTS_BR_IN_PROGRESS) != 0) // BR on schema in progress
        {
          if (getTextFromMD(&cliInterface, schUID, COM_LOCKED_OBJECT_BR_TAG, 0,
                            brTag))
            {
              processReturn();
              return NULL;
            }
        }
      else
        {
          if (getTextFromMD(&cliInterface, objUID, COM_LOCKED_OBJECT_BR_TAG, 0,
                            brTag))
            {
              processReturn();
              return NULL;
            }
        }
        
      // if table locked by Backup operation is to be included, dont return
      // an error.
      if (NOT ((includeBRLockedTables == 1) &&
               (NAString(brTag).contains("(BACKUP)"))))
        {
          *CmpCommon::diags() << DgSqlCode(-4264)
                              << DgString0(extName)
                              << DgString1(brTag);
          
          return NULL;
        }
    }

  //  Int64 t2 = NA_JulianTimestamp();

  if ((ctlFlags & READ_OBJECT_DESC) && // read stored descriptor
      ((objectFlags & MD_OBJECTS_STORED_DESC) != 0) && // stored desc available
      ((objectFlags & MD_OBJECTS_DISABLE_STORED_DESC) == 0)) // not disabled
    {

      TrafDesc * desc = NULL;

      if ((CmpCommon::getDefault(TRAF_ENABLE_METADATA_LOAD_IN_SHARED_CACHE) == DF_ON) &&
           objType == COM_BASE_TABLE_OBJECT)

        QRLogger::log(CAT_SQL_EXE, LL_INFO,
                      "Object definition for %s.%s.%s not found in shared cache, checking TEXT table" ,
                      catName.data(), schName.data(), objName.data());

      // if good stored desc was retrieved, return it.
      // Otherwise, continue and generate descriptor the old fashioned way.
      retcode = 
        checkAndGetStoredObjectDesc(
             &cliInterface, objUID, &desc, FALSE,
             ActiveSchemaDB()->getNATableDB()->getHeap());
      if (retcode == 0)
        {
          CmpCommon::diags()->clear();
          if (! desc)
            return NULL;

          QRLogger::log(CAT_SQL_EXE, LL_DEBUG, 
                        "Object definition for %s.%s retrieved from SQL descriptor store (TEXT)", 
                       catName.data(), schName.data());
          return desc;
        }

      // clear diags and continue
      CmpCommon::diags()->negateAllErrors();
    }

  str_sprintf(query, "select is_audited, num_salt_partns, row_format, flags from %s.\"%s\".%s where table_uid = %ld ",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_TABLES,
              objUID);
  
  Queue * tableAttrQueue = NULL;
  cliRC = cliInterface.fetchAllRows(tableAttrQueue, query, 0, FALSE, FALSE, TRUE);

  if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

      processReturn();

      return NULL;
    }

  Int64 tablesFlags = 0;
  NABoolean isAudited = TRUE;
  Lng32 numSaltPartns = 0;
  Int32 numInitialSaltRegions = -1;
  NABoolean alignedFormat = FALSE;
  ComReplType xnRepl = COM_REPL_NONE;
  ComStorageType storageType = COM_STORAGE_HBASE;
  NABoolean hbaseStrDataFormat = FALSE;
  NAString *  hbaseCreateOptions = new(STMTHEAP) NAString();
  NAString *  hbaseSplitClause = new(STMTHEAP) NAString();
  NAString colFamStr;
  NAString tableNamespace;
  Int32 numLOBdatafiles = -1;
  if (cliRC == 0) // read some rows
    {
      if (tableAttrQueue->entries() != 1) // only one row should be returned
        {
          processReturn();
          return NULL;                     
        }
      
      tableAttrQueue->position();
      OutputInfo * vi = (OutputInfo*)tableAttrQueue->getNext();
      
      char * audit = vi->get(0);
      isAudited =  (memcmp(audit, COM_YES_LIT, 1) == 0);
      
      numSaltPartns = *(Lng32*)vi->get(1);

      char * format = vi->get(2);
      alignedFormat = (memcmp(format, COM_ALIGNED_FORMAT_LIT, 2) == 0);
      hbaseStrDataFormat = (memcmp(format, COM_HBASE_STR_FORMAT_LIT, 2) == 0);
      
      tablesFlags = *(Int64*)vi->get(3);
      if (CmpSeabaseDDL::isMDflagsSet(tablesFlags, MD_TABLES_REPL_SYNC_FLG))
        xnRepl = COM_REPL_SYNC;
      else if (CmpSeabaseDDL::isMDflagsSet(tablesFlags, MD_TABLES_REPL_ASYNC_FLG))
        xnRepl = COM_REPL_ASYNC;
      
      if (CmpSeabaseDDL::isMDflagsSet(tablesFlags, MD_TABLES_STORAGE_MONARCH_FLG))
        storageType = COM_STORAGE_MONARCH;
      else
      if (CmpSeabaseDDL::isMDflagsSet(tablesFlags, MD_TABLES_STORAGE_BIGTABLE_FLG))
        storageType = COM_STORAGE_BIGTABLE;
      else
        storageType = COM_STORAGE_HBASE;

      if (getTextFromMD(&cliInterface, objUID, COM_HBASE_OPTIONS_TEXT, 0,
                        *hbaseCreateOptions))
        {
          processReturn();
          return NULL;
        }

      if (getTextFromMD(&cliInterface, objUID, COM_HBASE_COL_FAMILY_TEXT, 0,
                        colFamStr))
        {
          processReturn();
          return NULL;
        }
      if (getTextFromMD(&cliInterface, objUID, COM_HBASE_SPLIT_TEXT, 0,
                        *hbaseSplitClause))
        {
          processReturn();
          return NULL;
        }
      if (!hbaseSplitClause->isNull())
        {
          int num = 0;

          if (sscanf(hbaseSplitClause->data(), "IN %d REGIONS", &num) == 1)
            {
              numInitialSaltRegions = num;
              *hbaseSplitClause = "";  // this should only contain SPLIT BY, if anything
            }
        }
      else if (numSaltPartns > 1)
        numInitialSaltRegions = numSaltPartns;

      // get namespace of this table, if it was created in a non-default ns.
      if (getTextFromMD(&cliInterface, objUID, COM_OBJECT_NAMESPACE, 0,
                        tableNamespace))
        {
          processReturn();
          return NULL;
        }
     }

  Lng32 numCols = 0;
  ComTdbVirtTableColumnInfo * colInfoArray = NULL;

  NABoolean tableIsSalted = FALSE;
  Int16 numTrafReplicas = 0;
  char direction[20];
  str_sprintf(direction, "'%s'", COM_UNKNOWN_PARAM_DIRECTION_LIT);

  Lng32 identityColPos = -1;

  // partial rowsize uses the same computational algorithm as 
  // described in method NATable::computeHBaseRowSizeFromMetaData
  Lng32 partialRowSize = 0;

  //  Int64 t3 = NA_JulianTimestamp();
  
  if (getSeabaseColumnInfo(&cliInterface,
                           objUID,
                           catName, schName, objName,
                           (char *)direction,
                           &tableIsSalted,
                           &numTrafReplicas,
                           &identityColPos,
                           &partialRowSize,
                           &numCols,
                           &colInfoArray) <= 0)
    {
      processReturn();
      return NULL;                     
    } 


  if (objType == COM_INDEX_OBJECT)
    {
      str_sprintf(query, "select k.column_name, c.column_number, k.keyseq_number, ordering, cast(0 as int not null)  from %s.\"%s\".%s k, %s.\"%s\".%s c where k.column_name = c.column_name and k.object_uid = c.object_uid and k.object_uid = %ld and k.nonkeycol = 0 order by keyseq_number",
                  getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_KEYS,
                  getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_COLUMNS,
                  objUID);
    }
  else
    {
      str_sprintf(query, "select column_name, column_number, keyseq_number, ordering, cast(0 as int not null)  from %s.\"%s\".%s where object_uid = %ld and nonkeycol = 0 order by keyseq_number",
                  getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_KEYS,
                  objUID);
    }

  Queue * tableKeyInfo = NULL;
  cliRC = cliInterface.fetchAllRows(tableKeyInfo, query, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

      processReturn();

      return NULL;
    }

  ComTdbVirtTableKeyInfo * keyInfoArray = NULL;
  
  if (tableKeyInfo->numEntries() > 0)
    {
      keyInfoArray = 
        new(STMTHEAP) ComTdbVirtTableKeyInfo[tableKeyInfo->numEntries()];
    }

  Lng32 keyLen = 0;
  tableKeyInfo->position();
  for (int idx = 0; idx < tableKeyInfo->numEntries(); idx++)
    {
      OutputInfo * vi = (OutputInfo*)tableKeyInfo->getNext(); 

      populateKeyInfo(keyInfoArray[idx], vi);

      keyLen = colInfoArray[keyInfoArray[idx].tableColNum].length;
      partialRowSize += keyLen;
    }

  //get partionv2 info
  ComTdbVirtTablePartitionV2Info *partitionV2Info = NULL;
  if (isPartitionTableV2)
  {
    if (getSeabasePartitionV2Info(&cliInterface,
                                  objUID,
                                  &partitionV2Info))
    {
      processReturn();
      return NULL;
    }
  }
  else if (isPartitionBaseTable)
  {
    if (getSeabasePartitionBoundaryInfo(&cliInterface,
                                        objUID,
                                        &partitionV2Info))
    {
      processReturn();
      return NULL;
    }
  }

  // get replication type for the base table of this index.
  // That will be the replication type for this index as well.
  Int64 baseTableUID = 0;
  if (objType == COM_INDEX_OBJECT)
    {
      str_sprintf(query, "select t.flags, t.table_uid from %s.\"%s\".%s I, %s.\"%s\".%s T where I.index_uid = %ld and I.base_table_uid = T.table_uid",
                  getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_INDEXES,
                  getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_TABLES,
                  objUID);

      Int64 outVals[2];
      Int64 flags = 0;
      Lng32 outValsLen = 2 * sizeof(Int64);
      Int64 rowsAffected = 0;
      cliRC = cliInterface.executeImmediateCEFC
        (query, NULL, 0, (char*)&outVals, &outValsLen, &rowsAffected);
      if (cliRC < 0)
        {
          cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
          processReturn();
          return NULL;
        }
      
      flags = outVals[0];
      baseTableUID = outVals[1];

      if (CmpSeabaseDDL::isMDflagsSet(flags, MD_TABLES_REPL_SYNC_FLG))
        xnRepl = COM_REPL_SYNC;
      else if (CmpSeabaseDDL::isMDflagsSet(flags, MD_TABLES_REPL_ASYNC_FLG))
        xnRepl = COM_REPL_ASYNC;

      if (CmpSeabaseDDL::isMDflagsSet(flags, MD_TABLES_STORAGE_MONARCH_FLG))
        storageType = COM_STORAGE_MONARCH;
      else
      if (CmpSeabaseDDL::isMDflagsSet(flags, MD_TABLES_STORAGE_BIGTABLE_FLG))
        storageType = COM_STORAGE_BIGTABLE;
      else
        storageType = COM_STORAGE_HBASE;
    }

  Queue * indexInfoQueue = NULL;
  if (getIndexInfo(cliInterface, includeInvalidDefs, objUID, indexInfoQueue) < 0)
  {
    return NULL;
  }

  /*begin get global indexes info add by kai.deng*/
  if (isPartitionBaseTable && partitionV2Info)
  {
    Int64 pUid = partitionV2Info->baseTableUid;
    Queue * globalIndexInfoQueue = NULL;
    //get global index info
    if (getIndexInfo(cliInterface, includeInvalidDefs, pUid, globalIndexInfoQueue) < 0)
    {
      return NULL;
    }

    globalIndexInfoQueue->position();
    for(int p = 0; p < globalIndexInfoQueue->numEntries(); p++)
    {
      OutputInfo * vi = (OutputInfo*)globalIndexInfoQueue->getNext();
      if (*(Int64*)vi->get(11) & MD_IS_GLOBAL_PARTITION_FLG)
      {
        //global index add to indexInfoQueue
        indexInfoQueue->insert(vi);
      }
    }
  }
  /*end get global indexes info add by kai.deng*/

  ComTdbVirtTableIndexInfo * indexInfoArray = NULL;
  if (indexInfoQueue->numEntries() > 0)
    {
      indexInfoArray = 
        new(STMTHEAP) ComTdbVirtTableIndexInfo[indexInfoQueue->numEntries()];
    }

  NAString qCatName = "\"";
  qCatName += catName;
  qCatName += "\"";
  NAString qSchName = "\"";
  qSchName += schName;
  qSchName += "\"";
  NAString qObjName = "\"";
  qObjName += objName;
  qObjName += "\"";

  ComObjectName coName(qCatName, qSchName, qObjName);
  NAString * extTableName =
    new(STMTHEAP) NAString(coName.getExternalName(TRUE));

  indexInfoQueue->position();
  for (int idx = 0; idx < indexInfoQueue->numEntries(); idx++)
    {
      OutputInfo * vi = (OutputInfo*)indexInfoQueue->getNext(); 
      
      char * idxCatName = (char*)vi->get(0);
      char * idxSchName = (char*)vi->get(1);
      char * idxObjName = (char*)vi->get(2);
      Lng32 keyTag = *(Lng32*)vi->get(3);
      Lng32 isUnique = *(Lng32*)vi->get(4);
      Lng32 isExplicit = *(Lng32*)vi->get(5);
      Lng32 keyColCount = *(Lng32*)vi->get(6);
      Lng32 nonKeyColCount = *(Lng32*)vi->get(7);
      Lng32 idxNumSaltPartns = *(Lng32*)vi->get(8);
      char * format = vi->get(9);
      Int64 indexUID = *(Int64*)vi->get(10);
      Int64 indexFlags = *(Int64*)vi->get(11);
      // get the ngram index flag
      str_sprintf(query, "select case when X.text is not null then cast(trim(X.text) as int) else 0 end from %s.\"%s\".%s X where X.TEXT_uid = %ld AND X.TEXT_TYPE=%d AND X.SUB_ID=0 for read committed access",
                  getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_TEXT,
                  indexUID,
                  COM_OBJECT_NGRAM_TEXT);
      Queue * ngramFlagQueue = NULL;
      cliRC = cliInterface.fetchAllRows(ngramFlagQueue, query, 0, FALSE, FALSE, TRUE);
      if (cliRC < 0)
        {
          cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
          processReturn();
          return NULL;
        }
      Lng32 isNgram = 0;
      if (ngramFlagQueue->numEntries() == 1)
        {
          OutputInfo * ngramFlag = (OutputInfo*)ngramFlagQueue->getNext();
          isNgram = *(Lng32*)ngramFlag->get(0);
        }

      ComRowFormat idxRowFormat;

      if (memcmp(format, COM_ALIGNED_FORMAT_LIT, 2) == 0)
         idxRowFormat = COM_ALIGNED_FORMAT_TYPE;
      else
      if (memcmp(format, COM_HBASE_FORMAT_LIT, 2) == 0)
         idxRowFormat = COM_HBASE_FORMAT_TYPE;
      else
         idxRowFormat = COM_UNKNOWN_FORMAT_TYPE;

      Int64 idxUID = getObjectUID(&cliInterface,
                                  idxCatName, idxSchName, idxObjName,
                                  COM_INDEX_OBJECT_LIT);
      if (idxUID < 0)
        {

          processReturn();

          return NULL;
        }

      NAString * idxHbaseCreateOptions = new(STMTHEAP) NAString();
      if (getTextFromMD(&cliInterface, idxUID, COM_HBASE_OPTIONS_TEXT, 0,
                        *idxHbaseCreateOptions))
        {
          processReturn();
          
          return NULL;
        }

      indexInfoArray[idx].baseTableName = (char*)extTableName->data();

      NAString qIdxCatName = "\"";
      qIdxCatName += idxCatName;
      qIdxCatName += "\"";
      NAString qIdxSchName = "\"";
      qIdxSchName += idxSchName;
      qIdxSchName += "\"";
      NAString qIdxObjName = "\"";
      qIdxObjName += idxObjName;
      qIdxObjName += "\"";

      ComObjectName coIdxName(qIdxCatName, qIdxSchName, qIdxObjName);
      
      NAString * extIndexName = 
        new(STMTHEAP) NAString(coIdxName.getExternalName(TRUE));

      indexInfoArray[idx].indexName = (char*)extIndexName->data();
      indexInfoArray[idx].indexUID = indexUID;
      indexInfoArray[idx].indexFlags = indexFlags;
      indexInfoArray[idx].keytag = keyTag;
      indexInfoArray[idx].isUnique = isUnique;
      indexInfoArray[idx].isNgram = isNgram;
      indexInfoArray[idx].isExplicit = isExplicit;
      indexInfoArray[idx].keyColCount = keyColCount;
      indexInfoArray[idx].nonKeyColCount = nonKeyColCount;
      indexInfoArray[idx].hbaseCreateOptions = 
        (idxHbaseCreateOptions->isNull() ? NULL : idxHbaseCreateOptions->data());
      indexInfoArray[idx].numSaltPartns = idxNumSaltPartns;
      indexInfoArray[idx].numTrafReplicas = numTrafReplicas;
      indexInfoArray[idx].rowFormat = idxRowFormat;
      Queue * keyInfoQueue = NULL;
      str_sprintf(query, "select column_name, column_number, keyseq_number, ordering, nonkeycol  from %s.\"%s\".%s where object_uid = %ld order by keyseq_number",
                  getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_KEYS,
                  idxUID);
      cliRC = cliInterface.initializeInfoList(keyInfoQueue, TRUE);
      if (cliRC < 0)
        {
          cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

          processReturn();

          return NULL;
        }
      
      cliRC = cliInterface.fetchAllRows(keyInfoQueue, query);
      if (cliRC < 0)
        {
          cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

          processReturn();

          return NULL;
        }

      if (keyInfoQueue->numEntries() == 0)
        {
          *CmpCommon::diags() << DgSqlCode(-4400);

          processReturn();

          return NULL;
        }

      ComTdbVirtTableKeyInfo * keyInfoArray = 
        new(STMTHEAP) ComTdbVirtTableKeyInfo[keyColCount];

      ComTdbVirtTableKeyInfo * nonKeyInfoArray = NULL;
      if (nonKeyColCount > 0)
        {
          nonKeyInfoArray = 
            new(STMTHEAP) ComTdbVirtTableKeyInfo[nonKeyColCount];
        }

      keyInfoQueue->position();
      Lng32 jk = 0;
      Lng32 jnk = 0;
      for (Lng32 j = 0; j < keyInfoQueue->numEntries(); j++)
        {
          OutputInfo * vi = (OutputInfo*)keyInfoQueue->getNext(); 
          
          Lng32 nonKeyCol = *(Lng32*)vi->get(4);
          if (nonKeyCol == 0)
            {
              populateKeyInfo(keyInfoArray[jk], vi, TRUE);
              jk++;
            }
          else
            {
              if (nonKeyInfoArray)
                {
                  populateKeyInfo(nonKeyInfoArray[jnk], vi, TRUE);
                  jnk++;
                }
            }
        }

      indexInfoArray[idx].keyInfoArray = keyInfoArray;

      indexInfoArray[idx].nonKeyInfoArray = nonKeyInfoArray;
    } // for

  // get constraint info
  if (isPartitionBaseTable && partitionV2Info)
  {
    Int64 consObjUid = partitionV2Info->baseTableUid;
    str_sprintf(query, "select O.object_name, C.constraint_type, C.col_count, C.constraint_uid, C.enforced, C.flags from %s.\"%s\".%s O <<+cardinality 1e3>>, %s.\"%s\".%s C where O.catalog_name = '%s' and O.schema_name = '%s' and O.object_uid = C.constraint_uid and (C.table_uid = %ld and C.constraint_type <> '%s' or C.table_uid = %ld and C.constraint_type = '%s') order by 1",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS,
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_TABLE_CONSTRAINTS,
              catName.data(), schName.data(), partitionV2Info->baseTableUid,
              COM_PRIMARY_KEY_CONSTRAINT_LIT, objUID, COM_PRIMARY_KEY_CONSTRAINT_LIT);
  }
  else
    str_sprintf(query, "select O.object_name, C.constraint_type, C.col_count, C.constraint_uid, C.enforced, C.flags from %s.\"%s\".%s O <<+cardinality 1e3>>, %s.\"%s\".%s C where O.catalog_name = '%s' and O.schema_name = '%s' and C.table_uid = %ld and O.object_uid = C.constraint_uid order by 1",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS,
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_TABLE_CONSTRAINTS,
              catName.data(), schName.data(), objUID);
              
  Queue * constrInfoQueue = NULL;
  cliRC = cliInterface.fetchAllRows(constrInfoQueue, query, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

      processReturn();

      return NULL;
    }

  //  Int64 t6 = NA_JulianTimestamp();

  ComTdbVirtTableConstraintInfo * constrInfoArray = NULL;
  if (constrInfoQueue->numEntries() > 0)
    {
      constrInfoArray =
        new(STMTHEAP) ComTdbVirtTableConstraintInfo[constrInfoQueue->numEntries()];
    }

  NAString tableCatName = "\"";
  tableCatName += catName;
  tableCatName += "\"";
  NAString tableSchName = "\"";
  tableSchName += schName;
  tableSchName += "\"";
  NAString tableObjName = "\"";
  tableObjName += objName;
  tableObjName += "\"";

  ComObjectName coTableName(tableCatName, tableSchName, tableObjName);
  extTableName =
    new(STMTHEAP) NAString(coTableName.getExternalName(TRUE));

  NABoolean pkeyNotSerialized = FALSE;
  constrInfoQueue->position();
  for (int idx = 0; idx < constrInfoQueue->numEntries(); idx++)
    {
      OutputInfo * vi = (OutputInfo*)constrInfoQueue->getNext(); 
      
      char * constrName = (char*)vi->get(0);
      char * constrType = (char*)vi->get(1);
      Lng32 colCount = *(Lng32*)vi->get(2);
      Int64 constrUID = *(Int64*)vi->get(3);
      char * enforced = (char*)vi->get(4);
      Int64 flags = *(Int64*)vi->get(5);
      constrInfoArray[idx].baseTableName = (char*)extTableName->data();

      NAString cnNas = "\"";
      cnNas += constrName;
      cnNas += "\"";
      ComObjectName coConstrName(tableCatName, tableSchName, cnNas);
      NAString * extConstrName = 
        new(STMTHEAP) NAString(coConstrName.getExternalName(TRUE));

      constrInfoArray[idx].constrName = (char*)extConstrName->data();
      constrInfoArray[idx].colCount = colCount;

      if (strcmp(constrType, COM_UNIQUE_CONSTRAINT_LIT) == 0)
        constrInfoArray[idx].constrType = 0; // unique_constr
      else if (strcmp(constrType, COM_FOREIGN_KEY_CONSTRAINT_LIT) == 0)
        constrInfoArray[idx].constrType = 1; // ref_constr
     else if (strcmp(constrType, COM_CHECK_CONSTRAINT_LIT) == 0)
        constrInfoArray[idx].constrType = 2; // check_constr
     else if (strcmp(constrType, COM_PRIMARY_KEY_CONSTRAINT_LIT) == 0)
        constrInfoArray[idx].constrType = 3; // pkey_constr

      if ((constrInfoArray[idx].constrType == 3) && // pkey. TBD: Add Enum
          (CmpSeabaseDDL::isMDflagsSet(flags, CmpSeabaseDDL::MD_TABLE_CONSTRAINTS_PKEY_NOT_SERIALIZED_FLG)))
        constrInfoArray[idx].notSerialized = 1;
      else
        constrInfoArray[idx].notSerialized = 0;

      if (strcmp(enforced, COM_YES_LIT) == 0)
        constrInfoArray[idx].isEnforced = 1;
      else
        constrInfoArray[idx].isEnforced = 0;

      Queue * keyInfoQueue = NULL;
      str_sprintf(query, "select column_name, column_number, keyseq_number, ordering , cast(0 as int not null) from %s.\"%s\".%s where object_uid = %ld order by keyseq_number",
                  getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_KEYS,
                  constrUID);
      cliRC = cliInterface.initializeInfoList(keyInfoQueue, TRUE);
      if (cliRC < 0)
        {
          cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

          processReturn();

          return NULL;
        }
      
      cliRC = cliInterface.fetchAllRows(keyInfoQueue, query);
      if (cliRC < 0)
        {
          cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

          processReturn();

          return NULL;
        }
      
      ComTdbVirtTableKeyInfo * keyInfoArray = NULL;
      if (colCount > 0)
        {
          keyInfoArray = 
            new(STMTHEAP) ComTdbVirtTableKeyInfo[colCount];
          
          keyInfoQueue->position();
          Lng32 jk = 0;
          for (Lng32 j = 0; j < keyInfoQueue->numEntries(); j++)
            {
              OutputInfo * vi = (OutputInfo*)keyInfoQueue->getNext(); 
              
              populateKeyInfo(keyInfoArray[jk], vi, TRUE);
              jk++;
            }
        }

      constrInfoArray[idx].keyInfoArray = keyInfoArray;
      constrInfoArray[idx].numRingConstr = 0;
      constrInfoArray[idx].ringConstrArray = NULL;
      constrInfoArray[idx].numRefdConstr = 0;
      constrInfoArray[idx].refdConstrArray = NULL;

      constrInfoArray[idx].checkConstrLen = 0;
      constrInfoArray[idx].checkConstrText = NULL;

      // attach all the referencing constraints
      if ((strcmp(constrType, COM_UNIQUE_CONSTRAINT_LIT) == 0) ||
          (strcmp(constrType, COM_PRIMARY_KEY_CONSTRAINT_LIT) == 0))
        {
          // force the query plan; without this we tend to do full scans of
          // TABLE_CONSTRAINTS which reduces DDL concurrency
          str_sprintf(query,"control query shape sort(nested_join(nested_join(nested_join(scan('U'),scan('O')), scan('T','TRAFODION.\"_MD_\".TABLE_CONSTRAINTS_IDX')),cut))");
          cliRC = cliInterface.setCQS(query);
          if (cliRC < 0)
            {
              cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());             
              processReturn();
              return NULL;
            }

          str_sprintf(query, "select trim(O.catalog_name || '.' || '\"' || O.schema_name || '\"' || '.' || '\"' || O.object_name || '\"' ) constr_name, trim(O2.catalog_name || '.' || '\"' || O2.schema_name || '\"' || '.' || '\"' || O2.object_name || '\"' ) table_name from %s.\"%s\".%s U, %s.\"%s\".%s O, %s.\"%s\".%s O2, %s.\"%s\".%s T where  O.object_uid = U.foreign_constraint_uid and O2.object_uid = T.table_uid and T.constraint_uid = U.foreign_constraint_uid and U.unique_constraint_uid = %ld order by 2, 1",
                      getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_UNIQUE_REF_CONSTR_USAGE,
                      getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS,
                      getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS,
                      getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_TABLE_CONSTRAINTS,
                      constrUID
                      );

          Queue * ringInfoQueue = NULL;
          cliRC = cliInterface.fetchAllRows(ringInfoQueue, query, 0, FALSE, FALSE, TRUE);
          if (cliRC < 0)
            {
              cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
              
              processReturn();

              cliInterface.resetCQS(CmpCommon::diags());
              
              return NULL;
            }
      
          cliInterface.resetCQS(CmpCommon::diags());

          ComTdbVirtTableRefConstraints * ringInfoArray = NULL;
          if (ringInfoQueue->numEntries() > 0)
            {
              ringInfoArray = 
                new(STMTHEAP) ComTdbVirtTableRefConstraints[ringInfoQueue->numEntries()];
            }
          
          ringInfoQueue->position();
          for (Lng32 i = 0; i < ringInfoQueue->numEntries(); i++)
            {
              OutputInfo * vi = (OutputInfo*)ringInfoQueue->getNext(); 
              
              ringInfoArray[i].constrName = (char*)vi->get(0);
              ringInfoArray[i].baseTableName = (char*)vi->get(1);
            }

          constrInfoArray[idx].numRingConstr = ringInfoQueue->numEntries();
          constrInfoArray[idx].ringConstrArray = ringInfoArray;
        }

      // attach all the referencing constraints
      if (strcmp(constrType, COM_FOREIGN_KEY_CONSTRAINT_LIT) == 0)
        {
          str_sprintf(query, "select trim(O.catalog_name || '.' || '\"' || O.schema_name || '\"' || '.' || '\"' || O.object_name || '\"' ) constr_name, trim(O2.catalog_name || '.' || '\"' || O2.schema_name || '\"' || '.' || '\"' || O2.object_name || '\"' ) table_name from %s.\"%s\".%s R, %s.\"%s\".%s O, %s.\"%s\".%s O2, %s.\"%s\".%s T where  O.object_uid = R.unique_constraint_uid and O2.object_uid = T.table_uid and T.constraint_uid = R.unique_constraint_uid and R.ref_constraint_uid = %ld order by 2,1",
                      getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_REF_CONSTRAINTS,
                      getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS,
                      getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS,
                      getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_TABLE_CONSTRAINTS,
                      constrUID
                      );

          Queue * refdInfoQueue = NULL;
          cliRC = cliInterface.fetchAllRows(refdInfoQueue, query, 0, FALSE, FALSE, TRUE);
          if (cliRC < 0)
            {
              cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
              
              processReturn();
              
              return NULL;
            }

          ComTdbVirtTableRefConstraints * refdInfoArray = NULL;
          if (refdInfoQueue->numEntries() > 0)
            {
              refdInfoArray = 
                new(STMTHEAP) ComTdbVirtTableRefConstraints[refdInfoQueue->numEntries()];
            }
          
          // There should always be a referenced table for a foreign key constraint
          if (refdInfoQueue->numEntries() == 0)
            {
              NAString msg("Metadata mismatch, cannot find referenced table for constraint ");
              msg += extConstrName->data();
              msg += " on table ";
              msg += extTableName->data();
              SEABASEDDL_INTERNAL_ERROR(msg.data());
              processReturn();
              return NULL;
            }

          refdInfoQueue->position();
          for (Lng32 i = 0; i < refdInfoQueue->numEntries(); i++)
            {
              OutputInfo * vi = (OutputInfo*)refdInfoQueue->getNext(); 
              
              refdInfoArray[i].constrName = (char*)vi->get(0);
              refdInfoArray[i].baseTableName = (char*)vi->get(1);
            }

          constrInfoArray[idx].numRefdConstr = refdInfoQueue->numEntries();
          constrInfoArray[idx].refdConstrArray = refdInfoArray;

        }

     if (strcmp(constrType, COM_CHECK_CONSTRAINT_LIT) == 0)
       {
         NAString constrText;
         if (getTextFromMD(&cliInterface, constrUID, COM_CHECK_CONSTR_TEXT, 0,
                           constrText))
           {
              processReturn();
              
              return NULL;
           }

         char * ct = new(STMTHEAP) char[constrText.length()+1];
         memcpy(ct, constrText.data(), constrText.length());
         ct[constrText.length()] = 0;

         constrInfoArray[idx].checkConstrLen = constrText.length();
         constrInfoArray[idx].checkConstrText = ct;
       }
    } // for

  //  Int64 t7 = NA_JulianTimestamp();

  str_sprintf(query, "select check_option, is_updatable, is_insertable from %s.\"%s\".%s where view_uid = %ld ",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_VIEWS,
              objUID);
  
  Queue * viewInfoQueue = NULL;
  cliRC = cliInterface.fetchAllRows(viewInfoQueue, query, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

      processReturn();

      return NULL;
    }

  ComTdbVirtTableViewInfo * viewInfoArray = NULL;
  if (viewInfoQueue->numEntries() > 0)
    {
      // must have only one entry
      if (viewInfoQueue->numEntries() > 1)
        {
          processReturn();
          
          return NULL;
        }

      viewInfoArray = new(STMTHEAP) ComTdbVirtTableViewInfo[1];

      viewInfoQueue->position();
      
      OutputInfo * vi = (OutputInfo*)viewInfoQueue->getNext(); 
      
      char * checkOption = (char*)vi->get(0);
      Lng32 isUpdatable = *(Lng32*)vi->get(1);
      Lng32 isInsertable = *(Lng32*)vi->get(2);
      
      viewInfoArray[0].viewName = (char*)extTableName->data();
      
       if (NAString(checkOption) != COM_NONE_CHECK_OPTION_LIT)
        {
          viewInfoArray[0].viewCheckText = new(STMTHEAP) char[strlen(checkOption) + 1];
          strcpy(viewInfoArray[0].viewCheckText, checkOption);
        }
      else
        viewInfoArray[0].viewCheckText = NULL;
      viewInfoArray[0].isUpdatable = isUpdatable;
      viewInfoArray[0].isInsertable = isInsertable;

      // get view text from TEXT table
      NAString viewText;
      if (getTextFromMD(&cliInterface, objUID, COM_VIEW_TEXT, 0, viewText))
        {
          processReturn();
          
          return NULL;
        }

      viewInfoArray[0].viewText = new(STMTHEAP) char[viewText.length() + 1];
      strcpy(viewInfoArray[0].viewText, viewText.data());

      // get view col usages from TEXT table
      NAString viewColUsages;
      if (getTextFromMD(&cliInterface, objUID, COM_VIEW_REF_COLS_TEXT, 0, viewColUsages))
        {
          processReturn();
          
          return NULL;
        }

      viewInfoArray[0].viewColUsages = new(STMTHEAP) char[viewColUsages.length() + 1];
      strcpy(viewInfoArray[0].viewColUsages, viewColUsages.data());
    }

  ComTdbVirtTableSequenceInfo * seqInfo = NULL;
  if (identityColPos >= 0)
    {
      NAString seqName;
      SequenceGeneratorAttributes::genSequenceName
        (catName, schName, objName, colInfoArray[identityColPos].colName,
         seqName);
      
      NAString extSeqName;
      Int32 objectOwner;
      Int64 seqUID;
      seqInfo = getSeabaseSequenceInfo(catName, schName, seqName,
                                       extSeqName, objectOwner, schemaOwner, seqUID);
    }

  ComTdbVirtTablePrivInfo * privInfo = 
    getSeabasePrivInfo(&cliInterface, catName, schName, objUID, objType, schemaOwner);

  ComTdbVirtTableTableInfo * tableInfo = new(STMTHEAP) ComTdbVirtTableTableInfo[1];
  tableInfo->tableName = extTableName->data();
  tableInfo->createTime = 0;
  tableInfo->redefTime = 0;
  tableInfo->objUID = objUID;
  tableInfo->objDataUID = objDataUID;
  tableInfo->schemaUID = schUID;
  tableInfo->isAudited = (isAudited ? -1 : 0);
  tableInfo->validDef = 1;
  tableInfo->objOwnerID = objectOwner;
  tableInfo->schemaOwnerID = schemaOwner;
  tableInfo->numSaltPartns = numSaltPartns;
  tableInfo->numTrafReplicas = numTrafReplicas;
  tableInfo->numInitialSaltRegions = numInitialSaltRegions;
  tableInfo->hbaseSplitClause =
    (hbaseSplitClause->isNull() ? NULL : hbaseSplitClause->data());
  tableInfo->hbaseCreateOptions = 
    (hbaseCreateOptions->isNull() ? NULL : hbaseCreateOptions->data());
  if (alignedFormat)
    tableInfo->rowFormat = COM_ALIGNED_FORMAT_TYPE;
  else if (hbaseStrDataFormat)
    tableInfo->rowFormat = COM_HBASE_STR_FORMAT_TYPE;
  else
    tableInfo->rowFormat = COM_HBASE_FORMAT_TYPE;
  tableInfo->xnRepl = xnRepl;
  tableInfo->storageType = storageType;
  tableInfo->baseTableUID = baseTableUID;
  tableInfo->numLOBdatafiles = numLOBdatafiles;

  if (NOT colFamStr.isNull())
    {
      char colFamBuf[1000];
      char * colFamBufPtr = colFamBuf;
      strcpy(colFamBufPtr, colFamStr.data());
      strsep(&colFamBufPtr, " ");
      tableInfo->defaultColFam = new(STMTHEAP) char[strlen(colFamBuf)+1];
      strcpy((char*)tableInfo->defaultColFam, colFamBuf);
      tableInfo->allColFams = new(STMTHEAP) char[strlen(colFamBufPtr)+1];
      strcpy((char*)tableInfo->allColFams, colFamBufPtr);
    }
  else
    {
      tableInfo->defaultColFam = new(STMTHEAP) char[strlen(SEABASE_DEFAULT_COL_FAMILY)+1];
      strcpy((char*)tableInfo->defaultColFam, SEABASE_DEFAULT_COL_FAMILY);
      tableInfo->allColFams = NULL;
    }

  tableInfo->tableNamespace = NULL;
  if (NOT tableNamespace.isNull())
    {
      tableInfo->tableNamespace = new(STMTHEAP) char[tableNamespace.length()+1];
      strcpy((char *)tableInfo->tableNamespace, tableNamespace.data());
    }

  tableInfo->objectFlags = objectFlags;
  tableInfo->tablesFlags = tablesFlags;

  NABoolean isMonarchTable = (storageType == COM_STORAGE_MONARCH);
  ExpHbaseInterface* ehi = CmpSeabaseDDL::allocEHI(storageType);
  if (ehi == NULL) 
    return NULL;

  NAArray<HbaseStr>* endKeyArray = NULL;

  const NAString extNameForHbase = genHBaseObjName
    (catName, schName, objName, tableNamespace, objDataUID);

  ComTdbVirtTableStatsInfo * statsInfo = NULL;
  if (ctlFlags & GEN_STATS)
    {
      if (generateStoredStats(objUID, objDataUID,
                              catName, schName, objName, tableNamespace,
                              ctlFlags,
                              partialRowSize, numCols,
                              ehi, statsInfo))
        {
          CmpSeabaseDDL::deallocEHI(ehi);
          return NULL;
        }
    }

  //  Int64 t8 = NA_JulianTimestamp();
  tableDesc =
    Generator::createVirtualTableDesc
    (
     extTableName->data(), //objName,
     NULL, // let it decide what heap to use
     numCols,
     colInfoArray,
     tableKeyInfo->numEntries(), //keyIndex,
     keyInfoArray,
     constrInfoQueue->numEntries(),
     constrInfoArray,
     indexInfoQueue->numEntries(),
     indexInfoArray,
     viewInfoQueue->numEntries(),
     viewInfoArray,
     tableInfo,
     seqInfo,
     statsInfo,
     endKeyArray,
     ((ctlFlags & GEN_PACKED_DESC) != 0),
     &packedDescLen,
     TRUE /*user table*/,
     privInfo,
     NULL,//namesapce
     partitionV2Info);
  
  //  Int64 t9 = NA_JulianTimestamp();
  /*
  printf("t9-t8=%ld\n", t9-t8);
  printf("t8-t7=%ld\n", t8-t7);
  printf("t7-t6=%ld\n", t7-t6);
  printf("t6-t5=%ld\n", t6-t5);
  printf("t5-t4=%ld\n", t5-t4);
  printf("T35-t3=%ld\n", t35-t3);
  printf("t4-t35=%ld\n", t4-t35);
  printf("t3-t2=%ld\n", t3-t2);
  printf("t2-t1=%ld\n", t2-t1);
  */

  deleteNAArray(heap_, endKeyArray);
  NADELETEARRAY(constrInfoArray, constrInfoQueue->numEntries(), ComTdbVirtTableConstraintInfo, STMTHEAP);
  NADELETEARRAY(indexInfoArray, indexInfoQueue->numEntries(), ComTdbVirtTableIndexInfo, STMTHEAP);
  NADELETEARRAY(keyInfoArray, tableKeyInfo->numEntries(), ComTdbVirtTableKeyInfo, STMTHEAP);
NADELETEARRAY(viewInfoArray, viewInfoQueue->numEntries(), ComTdbVirtTableViewInfo, STMTHEAP);
  if ( tableDesc ) {
    // if this is base table or index and hbase object doesn't exist,
    // then this object is corrupted.
    if (!objectFlags & SEABASE_OBJECT_IS_EXTERNAL_HIVE &&
        !objectFlags & SEABASE_OBJECT_IS_EXTERNAL_HBASE)
      {
        if ((tableDesc->tableDesc()->objectType() == COM_BASE_TABLE_OBJECT) &&
            (existsInHbase(extNameForHbase, ehi) == 0))
          {
            *CmpCommon::diags() << DgSqlCode(-4254)
                                << DgString0(*extTableName);
            
            tableDesc = NULL;
            
            return NULL;
          }
      }
  }

  CmpSeabaseDDL::deallocEHI(ehi);
    
  if (! tableDesc)
    processReturn();
  
  return tableDesc;
} // getSeabaseUserTableDesc

TrafDesc * CmpSeabaseDDL::getSeabaseTableDesc(const NAString &catName, 
                                              const NAString &schName, 
                                              const NAString &objName,
                                              const ComObjectType objType,
                                              NABoolean includeInvalidDefs,
                                              short includeBRLockedTables)
{
  Lng32 retcode = 0;
  Lng32 cliRC = 0;

  if ((CmpCommon::context()->isUninitializedSeabase()) &&
      (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)))
     {
      if (CmpCommon::context()->uninitializedSeabaseErrNum() == -TRAF_HBASE_ACCESS_ERROR)
        *CmpCommon::diags() << DgSqlCode(CmpCommon::context()->uninitializedSeabaseErrNum())
                            << DgInt0(CmpCommon::context()->hbaseErrNum())
                            << DgString0(CmpCommon::context()->hbaseErrStr());
      else
        *CmpCommon::diags() << DgSqlCode(CmpCommon::context()->uninitializedSeabaseErrNum());

      return NULL;
    }

  ExpHbaseInterface *ehi = NULL;
  TrafDesc *tDesc = NULL;
  NABoolean isMDTable = (isSeabaseMD(catName, schName, objName) || 
                        isSeabasePrivMgrMD(catName, schName));
  Int32 ctlFlags = 0;
  if (isMDTable && (objName != SEABASE_SCHEMA_OBJECTNAME)) 
    {
      if (! CmpCommon::context()->getTrafMDDescsInfo())
        {
          *CmpCommon::diags() << DgSqlCode(-1428);

          return NULL;
        }

      tDesc = getSeabaseMDTableDesc(catName, schName, objName, objType);
      
      // Could not find this metadata object in the static predefined structs.
      // It could be a metadata view or other objects created in MD schema.
      // Look for it as a regular object.
    }

  else if (objName == SEABASE_SCHEMA_OBJECTNAME)
    {
      NAString schemaTabName;
      CONCAT_CATSCH(schemaTabName, catName.data(), schName.data());
      schemaTabName += ".";
      schemaTabName += objName;
      if ((CmpCommon::getDefault(TRAF_READ_OBJECT_DESC) == DF_ON) &&
          (NOT includeInvalidDefs) &&
          (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)))
        {
          ctlFlags |= READ_OBJECT_DESC;
        }

      Int32 packedDescLen = 0;
      tDesc = getSeabaseSchemaDesc(catName, schName, ctlFlags, packedDescLen);
      return tDesc;
    }

  else if ((objName == HBASE_HIST_NAME) ||
           (objName == HBASE_HISTINT_NAME) ||
           (objName == HBASE_PERS_SAMP_NAME))
    {
      NAString tabName = catName;
      tabName += ".";
      tabName += schName;
      tabName += ".";
      tabName += objName;
      // no hist tables in schemas used to store traf external hive/hbase tables
      if (ComIsTrafodionExternalSchemaName(schName))
        return NULL;
      tDesc = getSeabaseHistTableDesc(catName, schName, objName);
      return tDesc;
    }
  else if (isTrafBackupSchema(catName, schName))
    {
      tDesc = getTrafBackupMDTableDesc(catName, schName, objName);

      // Could not find this metadata object in the static predefined structs.
      // It could be a MD table created as a regular table, like LIBRARIES.
      // Look for it as a regular object.
    }

  if (! tDesc)
    {
      if ((CmpCommon::context()->isUninitializedSeabase()) &&
          (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)))
        {
          if (CmpCommon::context()->uninitializedSeabaseErrNum() == -TRAF_HBASE_ACCESS_ERROR)
            *CmpCommon::diags() << DgSqlCode(CmpCommon::context()->uninitializedSeabaseErrNum())
                                << DgInt0(CmpCommon::context()->hbaseErrNum())
                                << DgString0(CmpCommon::context()->hbaseErrStr());
          else
            *CmpCommon::diags() << DgSqlCode(CmpCommon::context()->uninitializedSeabaseErrNum());
        }
      else
        {
          if ((CmpCommon::getDefault(TRAF_READ_OBJECT_DESC) == DF_ON) &&
              (NOT includeInvalidDefs) &&
              (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)) &&
              (objType != COM_SEQUENCE_GENERATOR_OBJECT) &&
              (objType != COM_LIBRARY_OBJECT))
            {
              ctlFlags |= READ_OBJECT_DESC;
            }

          // do not sendAllControlAndFlags in switchCompiler, if it will be
          // be sent in getSeabaseUserTableDesc
          if (switchCompiler(CmpContextInfo::CMPCONTEXT_TYPE_META))
            return NULL;

	  switch (objType)
          {
            case COM_SEQUENCE_GENERATOR_OBJECT:
              tDesc = getSeabaseSequenceDesc(catName, schName, objName);
              break;
            case COM_LIBRARY_OBJECT:
              tDesc = getSeabaseLibraryDesc(catName, schName, objName);
              break;
            default:
              Int32 packedDescLen = 0;

              tDesc = getSeabaseUserTableDesc(catName, schName, objName, 
                                              objType, includeInvalidDefs,
                                              includeBRLockedTables,
                                              ctlFlags, packedDescLen,
                                              (CmpContextInfo::CMPCONTEXT_TYPE_META));
              break;
                 
	  }

          switchBackCompiler();
        }
    }

  return tDesc;
}

TrafDesc* CmpSeabaseDDL::decodeStoredDesc(NAString &gluedText)
{
  Lng32 decodedMaxLen = str_decoded_len(gluedText.length());

  // allocate descriptor in NATableDB heap so it can last across statements.
  NAHeap * heap = ActiveSchemaDB()->getNATableDB()->getHeap();
  char * packedDesc = new(heap) char[decodedMaxLen];
  Lng32 decodedLen =
    str_decode(packedDesc, decodedMaxLen,
               gluedText.data(), gluedText.length());
  
  TrafDesc * desc = (TrafDesc*)packedDesc;
  TrafDesc dummyDesc;
  desc = (TrafDesc*)desc->driveUnpack((void*)desc, &dummyDesc, NULL);

  return desc;
}

Lng32 
CmpSeabaseDDL::createNATableFromStoredDesc(NATableDB* natdb, BindWA &bindWA, PreloadInfo& loadInfo, NABoolean doNotReloadIfExists)
{
  CorrName cn(loadInfo.objName, STMTHEAP, loadInfo.schName, loadInfo.catName);
  if (loadInfo.objType == COM_INDEX_OBJECT_LIT)
    {
      cn.setSpecialType(ExtendedQualName::INDEX_TABLE);
    }

  NATable *naTable = natdb->get(&cn.getExtendedQualNameObj(), NULL, TRUE);
 
  // cqd traf_enable_metadata_load_in_cache 'ON' indicates that table
  // should not be reloaded if it exists.
  // If set to 'ALL', then it should be reloaded.
  if (naTable && doNotReloadIfExists)
  {
      // table already loaded. Return.
      return 1;
  }

#if 0
  Lng32 decodedMaxLen = str_decoded_len(gluedText.length());

  // allocate descriptor in NATableDB heap so it can last across statements.
  NAHeap * heap = ActiveSchemaDB()->getNATableDB()->getHeap();
  char * packedDesc = new(heap) char[decodedMaxLen];
  Lng32 decodedLen =
    str_decode(packedDesc, decodedMaxLen,
               gluedText.data(), gluedText.length());
  
  TrafDesc * desc = (TrafDesc*)packedDesc;
  TrafDesc dummyDesc;
  desc = (TrafDesc*)desc->driveUnpack((void*)desc, &dummyDesc, NULL);
#endif
  TrafDesc* desc = CmpSeabaseDDL::decodeStoredDesc(loadInfo.gluedText);

  if (! desc)
    {
      // error during unpack, ignore and return
      return -1;
    }
                                                        
  if (desc->nodetype  != DESC_TABLE_TYPE)
    return -3; // internal error

  naTable = bindWA.getNATable(cn, TRUE, desc);
  if (naTable == NULL || bindWA.errStatus())
    {
      // ignore error, this table cannot be loaded.
      CmpCommon::diags()->clear();

      bindWA.resetErrStatus();
      return -2;
    }

  naTable->setIsPreloaded(TRUE);

  return 0;
}

Lng32 CmpSeabaseDDL::createNATableForSystemObjects(BindWA &bindWA,
                                                   NAString &catName,
                                                   NAString &schName,
                                                   NAString &objName,
                                                   NABoolean isIndex,
                                                   NABoolean doNotReloadIfExists)
{

  CorrName cn(objName, STMTHEAP, schName, catName);
  if (isIndex)
     cn.setSpecialType(ExtendedQualName::INDEX_TABLE);

  NATableDB* ntdb = (bindWA.getSchemaDB())->getNATableDB();

  NATable *naTable = bindWA.getNATable(cn, TRUE);

  if (naTable && doNotReloadIfExists)
  {
      // table already loaded. Return.
      return 1;
  }

  if (naTable == NULL || bindWA.errStatus())
    {
      // ignore error, this table cannot be loaded.
      CmpCommon::diags()->clear();

      bindWA.resetErrStatus();
      return -2;
    }

  naTable->setIsPreloaded(TRUE);

  return 0;
}

void 
CmpSeabaseDDL::generateQueryToFetchUserTableDescriptors(
         QualifiedName &inSchName, char* buffer, Lng32 bufLen)
{
  NAString schNameStr;
  if (NOT inSchName.getSchemaName().isNull())
    {
      schNameStr = " and O.schema_name = '" + inSchName.getSchemaName() + "' ";
    }
  else
    {
      schNameStr = " and O.schema_name not like 'VOLATILE_SCHEMA%' and O.schema_name not in ('_MD_', '_REPOS_', '_HV_HIVE_', '_PRIVMGR_MD_', '_HIVESTATS_') ";
    }

  snprintf(buffer, bufLen,
           "select o.catalog_name, o.schema_name, o.object_name, o.object_type, t.seq_num, t.text from %s.\"%s\".%s O, %s.\"%s\".%s T where O.object_uid = T.text_uid %s and (O.object_type in ('BT', 'PS', 'SS')) and T.text_type = %d order by 1,2,3,5",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS,
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_TEXT,
              (schNameStr.isNull() ? " " : schNameStr.data()),
              COM_STORED_DESC_TEXT);
}

Lng32 CmpSeabaseDDL::generatePreloadInfoForUserTables(QualifiedName &inSchName,
                                                   ExeCliInterface &cliInterface,
                                                   NAList<PreloadInfo>& userTables,
                                                   NAList<PreloadInfo>& statsTables
                                                   )
{
  Lng32 cliRC = 0;
  Lng32 retcode;
  Lng32 tablesLoaded = 0;

  NAString catName;
  NAString schName;
  NAString objName;
  NAString objType;
  NAString gluedText;
 
  char query[2000];
  generateQueryToFetchUserTableDescriptors(inSchName, query, sizeof(query));

  // switch compiler and compile the metadata query in META context.
  if (switchCompiler(CmpContextInfo::CMPCONTEXT_TYPE_META))
    return -1;

  cliRC = cliInterface.fetchRowsPrologue(query);

  if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      goto error_return;
    }


  while (cliRC != 100)
    {
      cliRC = cliInterface.fetch();
      if (cliRC < 0)
        {
          cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
          goto error_return;
        }

      if (cliRC != 100)
        {
          char * ptr;
          Lng32 len;

          cliInterface.getPtrAndLen(5, ptr, len);
          Lng32 seqNum = *(Lng32*)ptr;
          if (seqNum == 0)
            {
              if ((NOT objName.isNull()) &&
                  (gluedText.length() > 0))
                {

                  // Save the info in either the user table list or stats table list
                  PreloadInfo loadInfo(catName, schName, objName, objType, gluedText);
                  if ( objName == HBASE_HIST_NAME ||
                       objName == HBASE_HISTINT_NAME ||
                       objName == HBASE_PERS_SAMP_NAME
                     )               
                     statsTables.insert(loadInfo);
                  else
                     userTables.insert(loadInfo);
                }

              cliInterface.getPtrAndLen(1, ptr, len);
              catName = NAString(ptr, len);
              
              cliInterface.getPtrAndLen(2, ptr, len);
              schName = NAString(ptr, len);
              
              cliInterface.getPtrAndLen(3, ptr, len);
              objName = NAString(ptr, len);

              cliInterface.getPtrAndLen(4, ptr, len);
              objType = NAString(ptr, len);

              gluedText = NAString(""); // clear out gluedText
            } // seq_num == 0

          cliInterface.getPtrAndLen(6, ptr, len);
          gluedText.append(ptr, len);
        }
    } // while

  if ((cliRC == 100) &&
      (NOT objName.isNull()) &&
      (gluedText.length() > 0))
    {
      cliInterface.clearGlobalDiags();

      // Save the info in either the user table list or stats table list
      PreloadInfo loadInfo(catName, schName, objName, objType, gluedText);
      if ( objName == HBASE_HIST_NAME || 
           objName == HBASE_HISTINT_NAME ||
           objName == HBASE_PERS_SAMP_NAME
         )
         statsTables.insert(loadInfo);
      else
         userTables.insert(loadInfo);

     }
  cliRC = cliInterface.fetchRowsEpilogue(0);
  if (cliRC < 0)
    {
      goto error_return;
    }

  switchBackCompiler();
  return 0;

 error_return:

  switchBackCompiler();
  return -1;

}

Lng32 CmpSeabaseDDL::loadTrafUserMetadataInCache(CmpContext* cmpContext,
                                                 NAList<PreloadInfo>& tables,
                                                 NABoolean doNotReloadIfExists
                                                 )
{
  Lng32 cliRC = 0;
  Lng32 retcode;
  Lng32 tablesLoaded = 0;

  SchemaDB* schemaDB = cmpContext->getSchemaDB();
  NATableDB* natDB = schemaDB->getNATableDB();

  // during preload, we dont want to return an error if preloaded cache
  // size increases the default cache size.
  // Save default size and set it to 0. This will cause cache size check
  // to be skipped during NATable creation.
  UInt32 savedDefaultCacheSize = natDB->defaultCacheSize();
  natDB->setDefaultCacheSize(0);
  natDB->setIsInPreloading(TRUE);
  NABoolean cachingStatus = natDB->usingCache();

  BindWA bindWA(schemaDB, cmpContext, FALSE/*inDDL*/);
 
  for (int i=0; i<tables.entries(); i++) {
     PreloadInfo& info = tables[i];

     // Need to call this method for each prelod info, since
     // during the previous call to createNATableFromStoredDesc(),
     // we may need to compile another meta-query and on exit
     // of that compilation, the caching status is unconditionally
     // cleared.
     natDB->useCache();

     retcode = createNATableFromStoredDesc(natDB, bindWA, info, doNotReloadIfExists);

     if (retcode == 0)
       tablesLoaded++;
     else if (retcode == 1) // table already loaded
       QRLogger::log(CAT_SQL_EXE, LL_WARN, "NATable for %s.%s.%s already loaded ", 
                     info.catName.data(), info.schName.data(), info.objName.data());
     else if (retcode < 0)
       QRLogger::log(CAT_SQL_EXE, LL_WARN, "Creating NATable for %s.%s.%s failed with error code %d ", 
                     info.catName.data(), info.schName.data(), info.objName.data(), retcode);
  }

  QRLogger::log(CAT_SQL_EXE, LL_INFO, "Metadata loaded for %d user tables in cache", tablesLoaded);

  natDB->setPreloadedCacheSize(natDB->currentCacheSize());
  natDB->setDefaultCacheSize(savedDefaultCacheSize);
  natDB->setIsInPreloading(FALSE);
  natDB->setUseCache(cachingStatus);

  return 0;
}

Lng32 CmpSeabaseDDL::loadTrafMDSystemTableIntoCache(
            NAString& tableName, CmpContext* context, 
            NABoolean doNotReloadIfExists)
{
  Lng32 retcode = 0;
  Lng32 tablesLoaded = 0;

  NAString catName;
  NAString schName;

  BindWA bindWA(context->getSchemaDB(), context, FALSE/*inDDL*/);

  NATableDB * natDB = context->getSchemaDB()->getNATableDB();
  UInt32 savedDefaultCacheSize = natDB->defaultCacheSize();
  natDB->setPreloadedCacheSize(0);
  natDB->setIsInPreloading(TRUE);

  NABoolean cachingStatus = natDB->usingCache();

  size_t numMDTables = sizeof(allMDtablesInfo) / sizeof(MDTableInfo);

  // Load definitions of system metadata tables
  for (size_t i = 0; i < numMDTables; i++)
    {
      const MDTableInfo &mdti = allMDtablesInfo[i];

      if ( !(mdti.newName == tableName) )
         continue;

      // preload "_MD_" table
      catName = TRAFODION_SYSTEM_CATALOG;
      schName = SEABASE_MD_SCHEMA;

      // Need to call this method for each prelod info, since
      // during the previous call to createNATableFromStoredDesc(),
      // we may need to compile another meta-query and on exit
      // of that compilation, the caching status is unconditionally
      // cleared.
      natDB->useCache();

      retcode = createNATableForSystemObjects(
           bindWA, catName, schName, tableName, mdti.isIndex, 
           doNotReloadIfExists);

      // if error, log it and continue. Load as many tables as can be loaded.
      if (retcode == 0)
        tablesLoaded++;
      else if (retcode < 0)
        QRLogger::log(CAT_SQL_EXE, LL_WARN, "Creating NATable for %s.%s.%s failed with error code %d ", 
                      catName.data(), schName.data(), tableName.data(), retcode);

      break;
    } // for

  QRLogger::log(CAT_SQL_EXE, LL_INFO, "Metadata loaded for %d system tables in cache", tablesLoaded);

  natDB->setPreloadedCacheSize(natDB->currentCacheSize());
  natDB->setDefaultCacheSize(savedDefaultCacheSize);
  natDB->setUseCache(cachingStatus);
  natDB->setIsInPreloading(FALSE);

  return 0;
}

Lng32 CmpSeabaseDDL::loadTrafSystemMetadataInCache(
            CmpContext* context, NABoolean doNotReloadIfExists)
{
  Lng32 retcode = 0;
  Lng32 tablesLoaded = 0;

  NAString catName;
  NAString schName;
  NAString objName;

  BindWA bindWA(context->getSchemaDB(), context, FALSE/*inDDL*/);

  NATableDB * natDB = context->getSchemaDB()->getNATableDB();
  UInt32 savedDefaultCacheSize = natDB->defaultCacheSize();
  natDB->setPreloadedCacheSize(0);
  natDB->setIsInPreloading(TRUE);

  NABoolean cachingStatus = natDB->usingCache();

  size_t numMDTables = sizeof(allMDtablesInfo) / sizeof(MDTableInfo);

  // Load definitions of system metadata tables
  for (size_t i = 0; i < numMDTables; i++)
    {
      const MDTableInfo &mdti = allMDtablesInfo[i];

      // preload "_MD_" table
      catName = TRAFODION_SYSTEM_CATALOG;
      schName = SEABASE_MD_SCHEMA;
      objName = mdti.newName;

      // Need to call this method for each prelod info, since
      // during the previous call to createNATableFromStoredDesc(),
      // we may need to compile another meta-query and on exit
      // of that compilation, the caching status is unconditionally
      // cleared.
      natDB->useCache();

      retcode = createNATableForSystemObjects(
           bindWA, catName, schName, objName, mdti.isIndex, 
           doNotReloadIfExists);

      // if error, log it and continue. Load as many tables as can be loaded.
      if (retcode == 0)
        tablesLoaded++;
      else if (retcode < 0)
        QRLogger::log(CAT_SQL_EXE, LL_WARN, "Creating NATable for %s.%s.%s failed with error code %d ", 
                      catName.data(), schName.data(), objName.data(), retcode);
    } // for

  // Load definitions of system priv metadata tables
  size_t numPrivTables = sizeof(privMgrTables)/sizeof(PrivMgrTableStruct);
  for (size_t i = 0; i < numPrivTables; i++)
    {
      const PrivMgrTableStruct &mdti = privMgrTables[i];

      // preload "_PRIVMGR_MD_" table
      catName = TRAFODION_SYSTEM_CATALOG;
      schName = SEABASE_PRIVMGR_SCHEMA;
      objName = mdti.tableName;

      // Need to call this method for each prelod info, since
      // during the previous call to createNATableFromStoredDesc(),
      // we may need to compile another meta-query and on exit
      // of that compilation, the caching status is unconditionally
      // cleared.
      natDB->useCache();

      retcode = createNATableForSystemObjects(
           bindWA, catName, schName, objName, mdti.isIndex,
           doNotReloadIfExists);

      if (retcode == 0)
        tablesLoaded++;
      else if (retcode < 0)
        QRLogger::log(CAT_SQL_EXE, LL_WARN, "Creating NATable for %s.%s.%s failed with error code %d ", 
                      catName.data(), schName.data(), objName.data(), retcode);
    } // for

  //  size_t numTenantTables = sizeof(allMDtenantsInfo) / sizeof(MDTableInfo);

  QRLogger::log(CAT_SQL_EXE, LL_INFO, "Metadata loaded for %d system tables in cache", tablesLoaded);

  natDB->setPreloadedCacheSize(natDB->currentCacheSize());
  natDB->setDefaultCacheSize(savedDefaultCacheSize);
  natDB->setUseCache(cachingStatus);
  natDB->setIsInPreloading(FALSE);

  return 0;
}

Lng32 CmpSeabaseDDL::loadTrafMetadataInCache(QualifiedName &inSchName)
{
  Lng32 retcode = 0;

  CmpContext* userContext = CmpCommon::context();

  // if uninitialized, dont load metadata cache
  if (userContext->isUninitializedSeabase())
    return 0;

  // If explicit load traf MD request is coming from sqlci, then load it
  // without checking for cqd. This makes it easy to try out this feature
  // without having to enable the cqd.
  // On the other hand, mxosrvr automatically issues load traf MD command 
  // at process startup time and is not an explicit command issued by user.
  // In that case, check for cqd to make sure that this feature is enabled.
  // If not enabled, dont load metadata cache
  if ((CmpCommon::getDefault(IS_SQLCI) == DF_OFF) &&
      (CmpCommon::getDefault(TRAF_ENABLE_METADATA_LOAD_IN_CACHE) == DF_OFF)) {
    QRLogger::log(CAT_SQL_EXE, LL_INFO, "%s", "TRAF_ENABLE_METADATA_LOAD_IN_CACHE is OFF");
    return 0;
  }

  NABoolean doNotReloadIfExists = 
    (((CmpCommon::getDefault(IS_SQLCI) == DF_ON) &&
     (CmpCommon::getDefault(TRAF_ENABLE_METADATA_LOAD_IN_CACHE) != DF_ALL))
        ||
     ((CmpCommon::getDefault(IS_SQLCI) == DF_OFF) &&
     (CmpCommon::getDefault(TRAF_ENABLE_METADATA_LOAD_IN_CACHE) == DF_ON)));

  NABoolean useSharedCache = 
    (CmpCommon::getDefault(TRAF_ENABLE_METADATA_LOAD_IN_SHARED_CACHE) == 
     DF_ON);

  ExeCliInterface cliInterface
    (STMTHEAP, 0, NULL, userContext->sqlSession()->getParentQid());

  SharedDescriptorCache* sharedDescCache = NULL;

#ifdef DEBUG_PRELOADING
cout << "loadTrafMetadataInCache"
    << ", current cliContext=" << static_cast<void*>(CmpCommon::context()->getCliContext())
    << ", current cmpContext: this=" << static_cast<void*>(CmpCommon::context())
    << ", class=" << CmpCommon::context()->getCIClass() 
    << ", NONE=" << CmpContextInfo::CMPCONTEXT_TYPE_NONE
    << ", META=" << CmpContextInfo::CMPCONTEXT_TYPE_META
    << ", USTATS=" << CmpContextInfo::CMPCONTEXT_TYPE_USTATS
    << endl;

if ( CmpCommon::context()->getCliContext() )
   CmpCommon::context()->getCliContext()->displayAllCmpContexts();
#endif



  // load system tables into the META instance
  if (switchCompiler(CmpContextInfo::CMPCONTEXT_TYPE_META)) 
    return -1;

  retcode = loadTrafSystemMetadataInCache(CmpCommon::context(), doNotReloadIfExists);

  switchBackCompiler();

  if (retcode < 0) return -1;

  // if useSharedCache is TRUE, then check if the shared cache exists. 
  sharedDescCache = (useSharedCache) ? SharedDescriptorCache::locate() : NULL;

  // Load user meta-data into compiler's NATableDB if 
  // the shared cache is not available or there are 
  // no entries for the schema 'inSchName' in it.
  //
  // The loading requires a read of the TEXT table.
  if (!sharedDescCache || sharedDescCache->entries(inSchName) == 0) {

     QRLogger::log(CAT_SQL_EXE, LL_INFO, "%s", "Load user metadata into cache.");

     NAList<PreloadInfo> userTables(getHeap()); 
     NAList<PreloadInfo> statsTables(getHeap()); 
     retcode = generatePreloadInfoForUserTables(inSchName,
                                                cliInterface,
                                                userTables,
                                                statsTables);
     if (retcode < 0) return -1;


     // load user tables into the current instance
     retcode = loadTrafUserMetadataInCache(userContext, userTables,
                                           doNotReloadIfExists);

     // load stats tables into META instance
     if (switchCompiler(CmpContextInfo::CMPCONTEXT_TYPE_META)) 
       return -1;

     retcode = loadTrafUserMetadataInCache(CmpCommon::context(), 
                                           statsTables,
                                           doNotReloadIfExists 
                                          );

     // switch compiler back.
     switchBackCompiler();

     if (retcode < 0) return -1;
     
  } else
     QRLogger::log(CAT_SQL_EXE, LL_INFO, "%s", "Skip loading user metadata into cache due to a shared cache.");

  return 0;
}

void fillNodeName(char* buf, Lng32 len)
{
  //Phandle wrapper in porting layer
  NAProcessHandle phandle;
  phandle.getmine();
  phandle.decompose();
  short cpu = phandle.getCpu();
  MS_Mon_Node_Info_Type nodeInfo;

  int rc = msg_mon_get_node_info_detail(cpu, &nodeInfo);
  if (rc == 0 && strlen(nodeInfo.node[0].node_name) < len)
    strcpy(buf, nodeInfo.node[0].node_name);
  else
    buf[0] = '\0';
}

#define emitErrorForSharedCacheOperation(reason, operation) \
      *CmpCommon::diags() << DgSqlCode(-CAT_SHARED_CACHE_LOAD_ERROR) \
                        << DgString0(reason) \
                        << DgString1(operation) \
                        << DgString2(CURRCONTEXT_CLUSTERINFO->getLocalNodeName())

// This define is identical to emitErrorForSharedCache except a warning is
// returned instead of an error.  However, if a terminating ";" is placed
// at the end of the diags statement, a bunch of compilation errors are 
// returned.  Not sure why so leaving the old code in place.
#define emitWarningForSharedCacheOperation(reason, operation) \
    *CmpCommon::diags() << DgSqlCode(CAT_SHARED_CACHE_LOAD_ERROR) \
                        << DgString0(reason) \
                        << DgString1(operation) \
                        << DgString2(CURRCONTEXT_CLUSTERINFO->getLocalNodeName())

#define emitWarningOrErrorForSharedCacheOperation(emitWarning, reason, operation) \
 { \
   if ( emitWarning ) \
    emitWarningForSharedCacheOperation(reason, operation); \
   else \
    emitErrorForSharedCacheOperation(reason, operation); \
 }

Lng32 
CmpSeabaseDDL::loadTrafMetadataIntoSharedCache(QualifiedName &inSchName,
                                               NABoolean loadLocalIfEmpty,
                                               CmpDDLwithStatusInfo* dws)
{
  Lng32 cliRC = 0;
  Lng32 retcode = 0;
  Lng32 tablesLoaded = 0;
  char msgBuf[200];
  NABoolean inserted = TRUE;

  const char* reasonString = NULL;

  if ( dws ) // must set to TRUE so that ExDDLwithStatusTcb::work()
             // can proceed to DONE_ step.
    dws->setDone(TRUE);

  // if uninitialized, dont load metadata cache
  if (CmpCommon::context()->isUninitializedSeabase())
    return 0;

  short semRetcode = 0;
  SharedDescriptorCache* sharedDescCache = NULL;
  NAMemory* sharedHeap = NULL;

  // Bypassing loading local (if empty), if the cache exists 
  // and is not empty.
  if ( loadLocalIfEmpty ) {
     sharedDescCache = SharedDescriptorCache::locate();
     if ( sharedDescCache && sharedDescCache->entries()>0 ) {
        emitWarningForSharedCacheOperation("The shared cache is not empty", "loading into");
        return 0;
     }
  }


  ExeCliInterface cliInterface
    (STMTHEAP, 0, NULL, 
     CmpCommon::context()->sqlSession()->getParentQid());

  NAString catName;
  NAString schName;
  NAString objName;
  NAString gluedText;
 
  // switch compiler 
  if (switchCompiler(CmpContextInfo::CMPCONTEXT_TYPE_META)) {
    emitErrorForSharedCacheOperation("Failed to switch compiler", "loading into");
    return -1;
  }

  char query[2000];

  Lng32 returnCode = 0;

  // load NATable objects for OBJECTS and TEXT NATable 
  NAString tableName = "OBJECTS";
  retcode = loadTrafMDSystemTableIntoCache(tableName, CmpCommon::context(), FALSE);

  tableName = "TEXT";
  retcode = loadTrafMDSystemTableIntoCache(tableName, CmpCommon::context(), FALSE);

  QualifiedName* key = NULL;
  NAString* value = NULL;


  generateQueryToFetchUserTableDescriptors(inSchName, query, sizeof(query));

  // Compile the metadata query in META context.
  cliRC = cliInterface.fetchRowsPrologue(query);

  // switch compiler back.
  switchBackCompiler();

  if (cliRC < 0) // check the return code from 
                 // cliInterface.fetchRowsPrologue()
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      snprintf(msgBuf, sizeof(msgBuf), 
                       "cliInterface.fetchRowsProlog() returns %d", cliRC);
      reasonString = msgBuf;
      emitErrorForSharedCacheOperation(reasonString, "loading into");

      return -1;
    }

  OBSERVE_SHARED_CACHE_LOCK;

  if ( retcode != 0 ) { // check the return code from d-lock
    snprintf(msgBuf, sizeof(msgBuf), 
                     "observing lock returns %d", retcode);
    reasonString = msgBuf;
    returnCode = -1;
    goto bailout_label;
  }

  // Destroy an existing one (if any) and make a brand new 
  // descriptor cache in the shared segment.
  sharedDescCache = SharedDescriptorCache::destroyAndMake();

  sharedHeap = NULL;

  // Error out if the attempt failed.
  if ( sharedDescCache ) {

     // Get hold the shared heap from the shared segment.
     sharedHeap = sharedDescCache->getSharedHeap();

     if ( !sharedHeap ) {
       reasonString = "Shared heap is NULL";
       returnCode = -1;
       goto bailout_label;
     }

  } else {

    reasonString = "SharedDescriptorCache::destroyAndMake() failed";
    QRLogger::log(CAT_SQL_EXE, LL_INFO, "%s", reasonString);

    returnCode = -1;
    goto bailout_label;
  }


  // Go over each table to assemble the descriptor as a binary string
  while (cliRC != 100)
    {
      cliRC = cliInterface.fetch();
      if (cliRC < 0)
        {
          cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

          snprintf(msgBuf, sizeof(msgBuf), 
                       "cliInterface.fetch() returns %d", cliRC);
          reasonString = msgBuf;
          returnCode = -1;
          goto bailout_label;
        }

      if (cliRC != 100)
        {
          char * ptr;
          Lng32 len;

          // seqNum field sorted in ascending order.
          cliInterface.getPtrAndLen(5, ptr, len);
          Lng32 seqNum = *(Lng32*)ptr;
          if (seqNum == 0)
            {
              // We must have found a record for a new table entry when
              // reach here.  We can insert the assembled descrptor string
              // into the shared cache.
              if ((NOT objName.isNull()) &&
                  (gluedText.length() > 0))
              {

               key = new (sharedHeap, FALSE /*failure is not fatal*/)
                   QualifiedName(objName, schName, catName, sharedHeap);

               value = new (sharedHeap, FALSE /*failure is not fatal*/) 
                   NAString(gluedText, sharedHeap);

               if ( sharedHeap->allocFailed() || !key || !value ) {

                  QRLogger::log(CAT_SQL_SHARED_CACHE, LL_DEBUG, "loadTrafMetadataIntoSharedCache(): allocate from shared heap failed: objName=%s, schName=%s, catName=%s, value length=%d",
                                   objName.data(),
                                   schName.data(),
                                   catName.data(),
                                   gluedText.length()
                                   );

                  returnCode = 1; // memory exhaust condition
                  goto bailout_label;
               }

               if ( sharedDescCache->insert(key, value, &inserted) ) {
 
                  if ( inserted )
                     tablesLoaded++;
                  else {
                      QRLogger::log(CAT_SQL_SHARED_CACHE, LL_DEBUG, "loadTrafMetadataIntoSharedCache(): sharedDescCache->insert() failed: objName=%s, schName=%s, catName=%s, value length=%d",
                                       objName.data(),
                                       schName.data(),
                                       catName.data(),
                                       gluedText.length()
                                    );
                      returnCode = 1; // memory exhaust condition
                      goto bailout_label;
                  }

               } else {
                  // duplicated name detected, ignore it.
               }

             }

             // grab the catalog name from the current row
             cliInterface.getPtrAndLen(1, ptr, len);
             catName = NAString(ptr, len);
              
             // grab the schema name from the current row
             cliInterface.getPtrAndLen(2, ptr, len);
             schName = NAString(ptr, len);
              
             // grab the table name from the current row
             cliInterface.getPtrAndLen(3, ptr, len);
             objName = NAString(ptr, len);

             gluedText = NAString(""); // clear out gluedText
           } // seq_num == 0

          // append the text field to gluedText.
          cliInterface.getPtrAndLen(6, ptr, len);
          gluedText.append(ptr, len);
        }
    } // while

  // Handle the last table entry read in.
  if ((cliRC == 100) &&
      (NOT objName.isNull()) &&
      (gluedText.length() > 0))
    {
      cliInterface.clearGlobalDiags();

      key = new (sharedHeap, FALSE)
                QualifiedName(objName, schName, catName, sharedHeap);

      value = 
                new (sharedHeap, FALSE) NAString(gluedText, sharedHeap);

      if ( sharedHeap->allocFailed() || !key || !value ) {
        QRLogger::log(CAT_SQL_SHARED_CACHE, LL_DEBUG, "loadTrafMetadataIntoSharedCache(): allocate from shared heap failed: objName=%s, schName=%s, catName=%s, value length=%d",
                         objName.data(),
                         schName.data(),
                         catName.data(),
                         gluedText.length()
                         );
        returnCode = 1; // memory exhaust condition
        goto bailout_label;
      }

      if ( sharedDescCache->insert(key, value, &inserted) )
         if ( inserted )
            tablesLoaded++;
         else {
            QRLogger::log(CAT_SQL_SHARED_CACHE, LL_DEBUG, "loadTrafMetadataIntoSharedCache(): sharedDescCache->insert() failed: objName=%s, schName=%s, catName=%s, value length=%d",
                              objName.data(),
                              schName.data(),
                              catName.data(),
                              gluedText.length()
                           );
            returnCode = 1; // memory exhaust condition
            goto bailout_label;
         }
      else {
         // duplicated name. ignore it
       }
               
     }

bailout_label:
  UNOBSERVE_SHARED_CACHE_LOCK;

  cliRC = cliInterface.fetchRowsEpilogue(0);
  if (cliRC < 0)
    {
      snprintf(msgBuf, sizeof(msgBuf), 
                   "cliInterface.fetch() returns %d", cliRC);
      reasonString = msgBuf;
      returnCode = -1;
    }

  switch (returnCode) {
    case 0: // normal
  
    return 0;

    case 1: // early return due to memory exhaust condition.
     returnCode = 0;
     break;

    case -1: // error
    default:
     emitErrorForSharedCacheOperation(reasonString, "loading into");
     break;
  }

  return returnCode;
}

Lng32 CmpSeabaseDDL::loadTrafDataIntoSharedCache(QualifiedName &inSchName, NABoolean loadLocalIfEmpty, CmpDDLwithStatusInfo *dws)
{
  Lng32 cliRC = 0;
  Lng32 retcode = 0;
  int returnCode = 0;
  NAString msgBuf;

  if (dws)
    dws->setDone(TRUE);

  if (CmpCommon::context()->isUninitializedSeabase())
    return 0;

  short semRetcode = 0;
  SharedTableDataCache *sharedDataCache = NULL;
  NAMemory *sharedHeap = NULL;

  if ( loadLocalIfEmpty ) {
     sharedDataCache = SharedTableDataCache::locate();
     if ( sharedDataCache && sharedDataCache->entries() > 0 ) {
        emitWarningForSharedCacheOperation("The shared cache is not empty", "loading into");
        return 0;
     }
  }

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL,
                               CmpCommon::context()->sqlSession()->getParentQid());

  ExeCliInterface cliInterface_load(STMTHEAP, 0, NULL,
                                    CmpCommon::context()->sqlSession()->getParentQid());

  NAString catName;
  NAString schName;
  NAString objName;
  NAString loadQuery;
  int tablesLoaded = 0;

  if (switchCompiler(CmpContextInfo::CMPCONTEXT_TYPE_META)) {
    emitErrorForSharedCacheOperation("Failed to switch compiler", "loading into");
    return -1;
  }



  NAString query;
  query.format("SELECT CATALOG_NAME, SCHEMA_NAME, OBJECT_NAME, OBJECT_TYPE FROM %s.\"%s\".%s WHERE BITAND(FLAGS, %d) != 0;", TRAFODION_SYSTEM_CATALOG, SEABASE_MD_SCHEMA, SEABASE_OBJECTS, MD_OBJECTS_READ_ONLY_ENABLED);

  cliRC = cliInterface.fetchRowsPrologue(query);

  OBSERVE_SHARED_DATA_CACHE_LOCK;

  if (retcode != 0)
  {
    msgBuf.format("observing lock returns %d", retcode);
    goto bailout_label;
  }

  sharedDataCache = SharedTableDataCache::destroyAndMake();

  if (sharedDataCache)
  {
    sharedHeap = sharedDataCache->getSharedHeap();

    if (!sharedHeap)
    {
      msgBuf = NAString("Shared heap is NULL");
      goto bailout_label;
    }
  }
  else
  {
    msgBuf = NAString("SharedTableDataCache::destroyAndMake() failed");
    QRLogger::log(CAT_SQL_EXE, LL_INFO, "%s", msgBuf.data());
    goto bailout_label;
  }

  while (cliRC != 100)
  {
    cliRC = cliInterface.fetch();
    if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      msgBuf.format("cliInterface.fetch() returns %d", cliRC);
      returnCode = -1;
      goto bailout_label;
    }

    if (cliRC != 100)
    {
      char *ptr;
      Lng32 len;
      int ret = 0;
      cliInterface.getPtrAndLen(1, ptr, len);
      NAString cat(ptr, len);
      cliInterface.getPtrAndLen(2, ptr, len);
      NAString sch(ptr, len);
      cliInterface.getPtrAndLen(3, ptr, len);
      NAString tab(ptr, len);

      cliInterface.getPtrAndLen(4, ptr, len);
      if (NAString(ptr, len) == "BT")
        loadQuery.format("select /*+index(%s)*/ [last 0]* from %s.\"%s\".%s fetch data", tab.data(), cat.data(), sch.data(), tab.data());
      else if (NAString(ptr, len) == "IX")
        loadQuery.format("select [last 0]* from TABLE(INDEX_TABLE %s.\"%s\".%s) fetch data", cat.data(), sch.data(), tab.data());
      else {
        msgBuf.format("unsupported objects type");
        returnCode = -1;
        goto bailout_label;
      }

      ret = cliInterface_load.executeImmediate(loadQuery.data());
      if (ret < 0)
      {
        cliInterface_load.retrieveSQLDiagnostics(CmpCommon::diags());
        msgBuf.format("An error occurred while processing object %s.%s.%s , return %d", cat.data(), sch.data(), tab.data(), ret);
        returnCode = -1;
        goto bailout_label;
      }
      tablesLoaded++;
    }
  }

bailout_label:
  UNOBSERVE_SHARED_DATA_CACHE_LOCK;
  switchBackCompiler();

  cliRC = cliInterface.fetchRowsEpilogue(0);
  if (cliRC < 0)
  {
    msgBuf.format("cliInterface.fetch() returns %d", cliRC);
    returnCode = -1;
  }

  switch (returnCode)
  {
  case 0:
    return 0;

  case 1:
    returnCode = 0;
    break;

  case -1:
  default:
    emitErrorForSharedCacheOperation(msgBuf.data(), "loading into");
    break;
  }

  return returnCode;
}

void 
CmpSeabaseDDL::generateQueryToFetchOneTableDescriptor(
         const QualifiedName &tableName, char* buffer, Lng32 bufLen)
{
  NAString tableNameStr ;

  if (NOT tableName.getCatalogName().isNull()) 
    {
      tableNameStr = " and O.catalog_name = '" + 
                        tableName.getCatalogName() + "' ";
    }
  else
     tableNameStr = "and O.catalog_name = 'TRAFODION'" + NAString("' ");

  // Strip off delimiters if specified.
  // Entries are always stored in internal format in the metadata
  if (NOT tableName.getSchemaName().isNull()) 
    {
      NAString schName(tableName.getSchemaName());
      if (schName[0] == '"');
        schName = schName.strip(NAString::both, '"');

      tableNameStr += " and O.schema_name = '" + schName + "' "; 
    }
  
  assert(NOT tableName.getObjectName().isNull());
  NAString objName(tableName.getObjectName());
  if (objName[0] == '"')
    objName = objName.strip(NAString::both, '"');

  tableNameStr += " and O.object_name = '" + objName + "' ";

  snprintf(buffer, bufLen,
           "select o.catalog_name, o.schema_name, o.object_name, o.object_type, t.seq_num, t.text from %s.\"%s\".%s O, %s.\"%s\".%s T where O.object_uid = T.text_uid %s and (O.object_type in ('BT', 'SS', 'PS')) and T.text_type = %d order by 1,2,3,5",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS,
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_TEXT,
              tableNameStr.data(),
              COM_STORED_DESC_TEXT);
}

Lng32 
CmpSeabaseDDL::updateTrafMetadataSharedCacheForTable(
            const QualifiedName& tableName,
            NABoolean insertOnly, 
            NABoolean errorAsWarning 
                                                    )
{
  Lng32 cliRC = 0;
  Lng32 retcode = 0;
  Lng32 tablesLoaded = 0;
  char msgBuf[200];
  NABoolean inserted = TRUE;

  SharedDescriptorCache* sharedDescCache = NULL;
  const char* reasonString = NULL;

  short semRetcode = 0;
  NAMemory* sharedHeap = NULL;

  ExeCliInterface cliInterface
    (STMTHEAP, 0, NULL, 
     CmpCommon::context()->sqlSession()->getParentQid());

  NAString catName;
  NAString schName;
  NAString objName;
  NAString gluedText;

  struct timeval cmp_start;
  gettimeofday(&cmp_start, 0);
  // switch compiler 
  if (switchCompiler(CmpContextInfo::CMPCONTEXT_TYPE_META)) {
      emitWarningOrErrorForSharedCacheOperation(errorAsWarning, "Failed to switching compiler", "updating");
    return -1;
  }

  char query[2000];
  generateQueryToFetchOneTableDescriptor(tableName, query, sizeof(query));

  // Compile the metadata query in META context.
  cliRC = cliInterface.fetchRowsPrologue(query);

  // switch compiler back.
  switchBackCompiler();

  // Inject an error to recreate core dump issues
  if (getenv("INJECT_DDL_SC1"))
    {
      NAString msg ("Injecting error for table ");
      msg += tableName.getQualifiedNameAsAnsiString();
      msg += " while updating shared cache";
      SEABASEDDL_INTERNAL_ERROR(msg.data());
      cliRC = -1001;
    }

  if (cliRC < 0) // check cliInterface.fetchRowsPrologue() return code
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      snprintf(msgBuf, sizeof(msgBuf), 
                       "cliInterface.fetchRowsProlog() returns %d", cliRC);
      emitWarningOrErrorForSharedCacheOperation(errorAsWarning, msgBuf, "updating");
      return -1;
    }



  Lng32 returnCode = 0;

  OBSERVE_SHARED_CACHE_LOCK;

  QualifiedName* key = NULL;
  NAString* value = NULL;

  if ( retcode != 0 ) { // unckeck OBSERVE_SHARED_CACHE_LOCK return code
    snprintf(msgBuf, sizeof(msgBuf), 
                     "observing lock returns %d", retcode);
    reasonString = msgBuf;
    returnCode = -1;
    goto bailout_label;
  }

  // Go over each entry to assemble the descriptor as a binary string
  while (cliRC != 100)
    {
      cliRC = cliInterface.fetch();
      if (cliRC < 0)
        {
          cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

          snprintf(msgBuf, sizeof(msgBuf), 
                       "The API cliInterface.fetch() returns %d", cliRC);
          reasonString = msgBuf;

          returnCode = -1;
          goto bailout_label;
        }

      if (cliRC != 100)
        {
          char * ptr;
          Lng32 len;

          // read catalog, schema and object name once
          if (objName.isNull()) {

             // grab the catalog name from the current row
             cliInterface.getPtrAndLen(1, ptr, len);
             catName = NAString(ptr, len);
              
             // grab the schema name from the current row
             cliInterface.getPtrAndLen(2, ptr, len);
             schName = NAString(ptr, len);
              
             // grab the table name from the current row
             cliInterface.getPtrAndLen(3, ptr, len);
             objName = NAString(ptr, len);

             gluedText = NAString(""); // clear out gluedText
          } // objName is null

          // append the text field to gluedText.
          cliInterface.getPtrAndLen(6, ptr, len);
          gluedText.append(ptr, len);
        }
    } // while
  struct timeval exe_stop;
  gettimeofday(&exe_stop, 0);

  // Handle the table entry read in above in pieces.
  if ((cliRC == 100) &&
      (NOT objName.isNull()) &&
      (gluedText.length() > 0))
    {
      cliInterface.clearGlobalDiags();

      sharedDescCache = SharedDescriptorCache::locate();
      if ( !sharedDescCache ) 
        {
          reasonString = "Failed to locate table descriptor shared cache";
          returnCode = -1;
          goto bailout_label;
        } 
      else 
        {
          // Get hold the shared heap from the shared segment.
          sharedHeap = sharedDescCache->getSharedHeap();

          if ( !sharedHeap ) 
            {
              reasonString = "Shared heap is NULL";
              returnCode = -1;
              goto bailout_label;
            }
        }

      // remove old entry
      NABoolean ok = sharedDescCache->remove(tableName);

      key = new (sharedHeap, FALSE) QualifiedName(objName, schName, catName, sharedHeap);
      value = new (sharedHeap, FALSE) NAString(gluedText, sharedHeap);


      if ( sharedHeap->allocFailed() || !key || !value ) {

        QRLogger::log(CAT_SQL_SHARED_CACHE, LL_DEBUG, "updateTrafMetadataSharedCacheForTable(): allocate from shared heap failed: objName=%s, schName=%s, catName=%s, value length=%d",
                     objName.data(),
                     schName.data(),
                     catName.data(),
                     gluedText.length()
                     );

        returnCode = 1;
        goto bailout_label;
      }

      if ( sharedDescCache->insert(key, value, &inserted) )
         if ( inserted )
            tablesLoaded++;
         else {
           QRLogger::log(CAT_SQL_SHARED_CACHE, LL_DEBUG, "loadTrafMetadataIntoSharedCache(): sharedDescCache->insert() failed: objName=%s, schName=%s, catName=%s, value length=%d",
                             objName.data(),
                             schName.data(),
                             catName.data(),
                             gluedText.length()
                          );
           returnCode = 1;
           goto bailout_label;
         }
      else {
         // duplicated name. ignore it.
       }
               
    }
  else 
    {
      reasonString = "Failed to read table descriptor data properly";
      returnCode = -1;
      goto bailout_label;
    }

bailout_label:
  UNOBSERVE_SHARED_CACHE_LOCK;

  cliRC = cliInterface.fetchRowsEpilogue(0);
  if (cliRC < 0)
    {
      snprintf(msgBuf, sizeof(msgBuf), 
                   "cliInterface.fetch() returns %d", cliRC);
      reasonString = msgBuf;

      returnCode = -1;
    }



  switch (returnCode) {
    case 0: // normal
  
    return 0;

    case 1: // early return due to memory exhaust.
     returnCode = 0;
     break;

    case -1: // error
    default:
     emitWarningOrErrorForSharedCacheOperation(errorAsWarning, reasonString, "updating");
     break;
  }

  return returnCode;
}

// Fill the blackBox data structure as follows.
//  #bytes  content
//    4   #entries (2)
//    4   length for 1st entry (1)
//    4   ' ' and 3 padding characters
//    4   strlen(<node_name>) + 2 + strlen(msg)
//    y   <node_name> ": " msg
//    1   for terminating null character
static Int32 packIntoBlackBox(const char* msg, char*& newBuf)
{
  Int32 bufLen = 16 + MAX_SEGMENT_NAME_LEN + 1 + strlen(msg) + 1;
  newBuf = new (STMTHEAP) char[bufLen];

  char* ptr = newBuf;

  *(Int32*)ptr= 2;
  ptr += sizeof(Int32);
  bufLen -= sizeof(Int32);

  *(Int32*)ptr= 1;
  ptr += sizeof(Int32);
  bufLen -= sizeof(Int32);

  memset(ptr, ' ', 4);
  ptr += 4;
  bufLen -= sizeof(Int32);

  // Pass over the length field for now so that the msg can
  // fill the buffer at the rigth position. 
  ptr += sizeof(Int32);
  bufLen -= sizeof(Int32);

  char myNodeName[MAX_SEGMENT_NAME_LEN+1];
  fillNodeName(myNodeName, sizeof(myNodeName));

  Int32 len = snprintf(ptr, bufLen, "%s: %s", myNodeName, msg);

  if ( len <= 0 )
    return 0;

  ptr[len] = '\0';

  // position back at the total length field in the buffer.
  ptr -= sizeof(Int32);
  *(Int32*)ptr= len;

  // need to round up 4 bytes. Please refer to method
  // CmpSeabaseDDL::populateBlackBox().
  len += ROUND4(len+1);

  return len + 4*sizeof(Int32);
}

#define checkTrafSharedCache(T, MSG, FUN)                            \
  {                                                                  \
    T *sharedCache = NULL;                                           \
    const char *reasonString = NULL;                                 \
                                                                     \
    char *blackBox = NULL;                                           \
    Int32 filledBlackBoxLen = 0;                                     \
                                                                     \
    sharedCache = T::locate();                                       \
    if (!sharedCache)                                                \
    {                                                                \
      reasonString = "table " MSG " shared cache does not exist";    \
      filledBlackBoxLen = packIntoBlackBox(reasonString, blackBox);  \
    }                                                                \
    else                                                             \
    {                                                                \
      char *summaryData = sharedCache->FUN;                          \
      if (summaryData)                                               \
        filledBlackBoxLen = packIntoBlackBox(summaryData, blackBox); \
    }                                                                \
                                                                     \
    if (!blackBox || filledBlackBoxLen == 0)                         \
    {                                                                \
      reasonString = "Failed to collect summary data";               \
      goto error_return;                                             \
    }                                                                \
                                                                     \
    dws->setBlackBox(blackBox);                                      \
    dws->setBlackBoxLen(filledBlackBoxLen);                          \
                                                                     \
    dws->setStep(-1);                                                \
    dws->setSubstep(0);                                              \
                                                                     \
    return 0;                                                        \
                                                                     \
  error_return:                                                      \
    emitErrorForSharedCacheOperation(reasonString, "check");         \
    return -1;                                                       \
  }

Lng32 CmpSeabaseDDL::checkTrafMetadataSharedCacheForTable(
    const QualifiedName &tableName,
    CmpDDLwithStatusInfo *dws)
    checkTrafSharedCache(SharedDescriptorCache, "descriptor", collectSummaryDataForTable(tableName));

Lng32 CmpSeabaseDDL::checkTrafMetadataSharedCacheForSchema(
    const QualifiedName &schemaName,
    CmpDDLwithStatusInfo *dws)
    checkTrafSharedCache(SharedDescriptorCache, "descriptor", collectSummaryDataForSchema(schemaName));

Lng32 CmpSeabaseDDL::checkTrafMetadataSharedCacheAll(CmpDDLwithStatusInfo *dws)
    checkTrafSharedCache(SharedDescriptorCache, "descriptor", collectSummaryDataForAll());

Lng32 CmpSeabaseDDL::checkTrafDataSharedCacheForTable(
    const QualifiedName &tableName,
    CmpDDLwithStatusInfo *dws)
    checkTrafSharedCache(SharedTableDataCache, "data", collectSummaryDataForTable(tableName));

Lng32 CmpSeabaseDDL::checkTrafDataSharedCacheForSchema(
    const QualifiedName &schemaName,
    CmpDDLwithStatusInfo *dws)
    checkTrafSharedCache(SharedTableDataCache, "descriptor", collectSummaryDataForSchema(schemaName));

#undef checkTrafSharedCache

Lng32 CmpSeabaseDDL::checkTrafDataSharedCacheAll(CmpDDLwithStatusInfo *dws, bool showDetails)
{
  char *blackBox = NULL;
  const char *reasonString = NULL;
  Int32 filledBlackBoxLen = 0;

  SharedTableDataCache * sharedCache = SharedTableDataCache::locate();
  if (!sharedCache) {
    reasonString = "table data shared cache does not exist";
    filledBlackBoxLen = packIntoBlackBox(reasonString, blackBox);
  }
  else
    blackBox = sharedCache->collectSummaryDataForAll(showDetails, filledBlackBoxLen);

  if (!blackBox || filledBlackBoxLen == 0)
  {
    reasonString = "Failed to collect summary data";
    goto error_return;
  }

  dws->setBlackBox(blackBox);
  dws->setBlackBoxLen(filledBlackBoxLen);

  dws->setStep(-1);
  dws->setSubstep(0);

  return 0;

error_return:
  emitErrorForSharedCacheOperation(reasonString, "check");
  return -1;
}

Lng32 CmpSeabaseDDL::updateTrafDataSharedCacheForTable(
    const vector<pair<QualifiedName,  int>> &tables,
    NABoolean insert,
    NABoolean errorAsWarning)
{
  Lng32 retcode = 0;
  Lng32 cliRC = 0;
  NAString msgBuf;
  SharedTableDataCache *sharedDataCache = NULL;

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL,
                               CmpCommon::context()->sqlSession()->getParentQid());

  OBSERVE_SHARED_DATA_CACHE_LOCK;
  if (retcode != 0)
  {
    msgBuf.format("Error in locking a name semaphore(%d)", retcode);
    goto error_return;
  }

  if (sharedDataCache = SharedTableDataCache::locate())
  {
    if (switchCompiler(CmpContextInfo::CMPCONTEXT_TYPE_META)) {
      msgBuf.format("Failed to switch compiler");
      goto error_return;
    }
    NAString upQuery;

    for (auto &&table : tables) {
      if (sharedDataCache->contains(table.first))
      {
        if (insert)
        {
          msgBuf.format("Failed to insert data shared cache for table %s, the table cache exist", table.first.getQualifiedNameAsAnsiString(FALSE, TRUE).data());
          goto error_return;
        }
        else
        {
          if (sharedDataCache->remove(table.first))
          {
            NAString tableName = table.first.getQualifiedNameAsAnsiString(FALSE, TRUE);
            if (table.second == 0)
              upQuery.format("select [last 0]* from %s<<+index %s>> fetch data", tableName.data(), tableName.data());
            else
              upQuery.format("select [last 0]* from table(index_table %s) fetch data", tableName.data());

            cliRC = cliInterface.executeImmediate(upQuery);
            if (cliRC < 0)
            {
              cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
              msgBuf.format("Error in load data into share cache %s, Check data cache and execute \"alter trafodion data shared cache for [table|index] OBJECTS delete internal\" to clear the inconsistent cache", table.first.getQualifiedNameAsAnsiString(FALSE, TRUE).data());
              goto error_return;
            }
          }
          else
          {
            msgBuf.format("Failed to update data shared cache for table %s befor load data into share cache", table.first.getQualifiedNameAsAnsiString(FALSE, TRUE).data());
            goto error_return;
          }
        }
      }
      else if (sharedDataCache->contains(table.first, true))
      {
        msgBuf.format("Failed to update data shared cache for table %s, the table cache is disabled", table.first.getQualifiedNameAsAnsiString(FALSE, TRUE).data());
        goto error_return;
      }
      else
      {
        NAString tableName = table.first.getQualifiedNameAsAnsiString(FALSE, TRUE);
        if (table.second == 0)
          upQuery.format("select [last 0]* from %s<<+index %s>> fetch data", tableName.data(), tableName.data());
        else
          upQuery.format("select [last 0]* from table(index_table %s) fetch data", tableName.data());

        cliRC = cliInterface.executeImmediate(upQuery);
        if (cliRC < 0)
        {
          cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
          msgBuf.format("Error in load data into share cache %s, Check data cache and execute \"alter trafodion data shared cache for [table|index] OBJECTS delete internal\" to clear the inconsistent cache if exist", table.first.getQualifiedNameAsAnsiString(FALSE, TRUE).data());
          goto error_return;
        }
      }
    }
    switchBackCompiler();
  }
  else
  {
    QRLogger::log(CAT_SQL_EXE, LL_INFO, "SharedTableDataCache::locate() failed");
    goto error_return;
  }
  UNOBSERVE_SHARED_DATA_CACHE_LOCK;

  return 0;

error_return:
  UNOBSERVE_SHARED_DATA_CACHE_LOCK;
  switchBackCompiler();
  emitWarningOrErrorForSharedCacheOperation(errorAsWarning, msgBuf.data(), "alter");
  return -1;
}

Lng32 
CmpSeabaseDDL::alterSharedDescCache(StmtDDLAlterSharedCache* ascNode,
                                CmpDDLwithStatusInfo* dws)
{
  char msgBuf[200];
  const char* reasonString = NULL;

  if ( dws ) // must set to TRUE so that ExDDLwithStatusTcb::work()
             // can proceed to DONE_ step.
    dws->setDone(TRUE);

  // if uninitialized, dont alter metadata cache
  if (CmpCommon::context()->isUninitializedSeabase())
    return 0;

  // if ascNode is NULL, dont alter metadata cache
  if (!ascNode)
    return 0;

  Lng32 retcode = 0;
  short semRetcode = 0;
  SharedDescriptorCache* sharedDescCache = NULL;
  NABoolean ok = FALSE;
  NAString op;



  QualifiedName name = ascNode->getQualifiedName();

  NABoolean errorAsWarning = ascNode->isInternal();
  if ( ascNode->opForTable()  || ascNode->opForIndex() )
  {
    // Handle the update and insert as two special cases here. Both lock the cache.
    if ( ascNode->doUpdate() )
       return updateTrafMetadataSharedCacheForTable(name, FALSE, errorAsWarning);

    else if ( ascNode->doInsert() )
       return updateTrafMetadataSharedCacheForTable(name, TRUE, errorAsWarning);

    else if ( ascNode->doCheck() )
       return checkTrafMetadataSharedCacheForTable(name, dws);

  } else if (ascNode->opForSchema()) {
      if ( ascNode->doUpdate() )
        return updateTrafMetadataSharedCacheForTable(name, FALSE, errorAsWarning);
      if ( ascNode->doInsert() )
        return updateTrafMetadataSharedCacheForTable(name, TRUE, errorAsWarning);
      if ( ascNode->doCheck() ) 
        return checkTrafMetadataSharedCacheForSchema(name, dws);
  } else {
    if (ascNode->checkAll())
      return checkTrafMetadataSharedCacheAll(dws);
  }

  // Handle the rest of the cases here. All require locking the cache
  OBSERVE_SHARED_CACHE_LOCK;

  if ( retcode != 0 ) {
    snprintf(msgBuf, sizeof(msgBuf), 
                     "Error in locking a name semaphore(%d)", retcode);
    reasonString = msgBuf;
    goto error_return;
  }

  sharedDescCache = SharedDescriptorCache::locate();
  if ( sharedDescCache ) {

     switch (ascNode->getSubject())
     {
       case StmtDDLAlterSharedCache::TABLE:
       case StmtDDLAlterSharedCache::INDEX:

         switch (ascNode->getOptions())
         {
          case StmtDDLAlterSharedCache::DISABLE:
            op = "disable";
            ok = sharedDescCache->disable(name);
            break;
            
          case StmtDDLAlterSharedCache::ENABLE:

            op = "enable";
            ok = sharedDescCache->enable(name);
            break;

          case StmtDDLAlterSharedCache::DELETE:

            op = "delete";
            ok = sharedDescCache->remove(name);
            break;

          default:
            snprintf(msgBuf, sizeof(msgBuf),
                     "Invalid option %s requested for table %s",
                     op.data(),
                     name.getQualifiedNameAsAnsiString(FALSE, TRUE).data());
             reasonString = msgBuf;
             goto error_return_after_release_semaphore;
         }
   
         if (!ok) {
             snprintf(msgBuf, sizeof(msgBuf), 
                              "Failed to %s the shared cache entry for table %s", 
                              op.data(),
                              name.getQualifiedNameAsAnsiString(FALSE, TRUE).data()
                              );
             reasonString = msgBuf;
             goto error_return_after_release_semaphore;
         }

         break;

     case StmtDDLAlterSharedCache::SCHEMA:

         switch (ascNode->getOptions())
         {
          case StmtDDLAlterSharedCache::DISABLE:

            op = "disable";
            ok = sharedDescCache->disableSchema(name);
            break;
            
          case StmtDDLAlterSharedCache::ENABLE:

            op = "enable";
            ok = sharedDescCache->enableSchema(name);
            break;

          case StmtDDLAlterSharedCache::DELETE:

            op = "delete";
            ok = sharedDescCache->removeSchema(name);
            break;

          default:
            snprintf(msgBuf, sizeof(msgBuf),
                     "Invalid option %s requested for schema %s",
                     op.data(),
                     name.getQualifiedNameAsAnsiString(FALSE, TRUE).data());
             reasonString = msgBuf;
             goto error_return_after_release_semaphore;
         }
   
         if (!ok) {
             snprintf(msgBuf, sizeof(msgBuf), 
                              "Failed to %s the shared cache entry for schema %s", 
                              op.data(),
                              name.getQualifiedNameAsAnsiString(FALSE, TRUE).data()
                              );
             reasonString = msgBuf;
             goto error_return_after_release_semaphore;
         }

         break;
       break;

     case StmtDDLAlterSharedCache::ALL:

       switch (ascNode->getOptions()) {

        case StmtDDLAlterSharedCache::CLEAR:
          op = "clear";
          sharedDescCache = sharedDescCache->destroyAndMake();
          ok = !!sharedDescCache;
          break;
 
        default:
          snprintf(msgBuf, sizeof(msgBuf),
                   "Invalid option %s requested",
                   op.data());
           reasonString = msgBuf;
           goto error_return_after_release_semaphore;
       }

       if (!ok) {
         snprintf(msgBuf, sizeof(msgBuf), 
                          "Failed to %s the shared cache", op.data());
         reasonString = msgBuf;
         goto error_return_after_release_semaphore;
       }

       break;
 
       default: // none of table, schema and all
          snprintf(msgBuf, sizeof(msgBuf),
                   "Invalid option %s requested",
                   op.data());
           reasonString = msgBuf;
           goto error_return_after_release_semaphore;
     } // switch on subject
  } else {

    reasonString = "SharedDescriptorCache::locate() failed";
    QRLogger::log(CAT_SQL_EXE, LL_INFO, "%s", reasonString);

    goto error_return_after_release_semaphore;
  }


  UNOBSERVE_SHARED_CACHE_LOCK;

  return 0;

error_return_after_release_semaphore:
  UNOBSERVE_SHARED_CACHE_LOCK;

error_return:
  emitWarningOrErrorForSharedCacheOperation(ascNode->isInternal(), reasonString, "alter");
  return -1;
}

Lng32 CmpSeabaseDDL::alterSharedDataCache(StmtDDLAlterSharedCache *ascNode,
                                          CmpDDLwithStatusInfo *dws)
{
  NAString msgBuf;
  const char* reasonString = NULL;
  NAString op;
  bool ok = true;
  Lng32 retcode = 0;

  if (dws)
    dws->setDone(TRUE);

  if (CmpCommon::context()->isUninitializedSeabase())
    return 0;

  if (!ascNode)
    return 0;

  NABoolean isInternalOp = ascNode->isInternal();
  SharedTableDataCache *sharedDataCache = NULL;
  QualifiedName name = ascNode->getQualifiedName();

  BindWA bindWA(ActiveSchemaDB(), CmpCommon::context(), TRUE /*inDDL*/);
  CorrName cn(name);

  NATable *naTable = NULL;
  Int64 uid = 0;
  using tables = pair<QualifiedName, int>;
  vector<tables> names;

  if (name.getObjectName() != SEABASE_SCHEMA_OBJECTNAME && !isInternalOp)
  {
    naTable = bindWA.getNATable(cn);
    if (naTable == NULL || bindWA.errStatus())
    {
      *CmpCommon::diags()
          << DgSqlCode(-4082)
          << DgTableName(name.getQualifiedNameAsAnsiString(FALSE, TRUE));
      return -1;
    }

    names.push_back(tables(name, 0));
    if (!naTable->readOnlyEnabled() && (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL))) {
      *CmpCommon::diags()
        << DgSqlCode(-3242)
        << DgString0("can not execute a alter shared command on a not READ_ONLY table");
      return -1;
    }
    if (naTable->hasSecondaryIndexes()) {
      const NAFileSetList &naFsList = naTable->getIndexList();
      for (size_t i = 0; i < naFsList.entries(); i++) {
        const NAFileSet * naFS = naFsList[i];
        if (naFS->getKeytag() == 0)
          continue;

        names.push_back( tables(naFS->getFileSetName(), 1) );
      }
    }
  } else
    names.push_back(tables(name, ascNode->opForTable() ? 0 : 1));

  if (ascNode->opForTable() || ascNode->opForIndex()) {
    if (ascNode->doInsert())
      return updateTrafDataSharedCacheForTable(names, TRUE, isInternalOp);
    else if (ascNode->doUpdate())
      return updateTrafDataSharedCacheForTable(names, FALSE, isInternalOp);
    else if (ascNode->doCheck())
      return checkTrafDataSharedCacheForTable(name, dws);
  } else if (ascNode->opForSchema()) {
    *CmpCommon::diags()
        << DgSqlCode(-1229)
        << DgString0("load data cache for schema");
    return -1;
  } else {
    if (ascNode->checkAll())
      return checkTrafDataSharedCacheAll(dws, false);
    else if (ascNode->checkAllDetails())
      return checkTrafDataSharedCacheAll(dws, true);
  }

  OBSERVE_SHARED_DATA_CACHE_LOCK
  if ( retcode != 0 ) {
    msgBuf.format("Error in locking a name semaphore(%d)", retcode);
    reasonString = msgBuf.data();
    goto error_return;
  }

  if (sharedDataCache = SharedTableDataCache::locate()) {
    switch (ascNode->getSubject()) {
      case StmtDDLAlterSharedCache::TABLE:
      case StmtDDLAlterSharedCache::INDEX:
        switch (ascNode->getOptions()) {
          case StmtDDLAlterSharedCache::DISABLE:
            op = "disable";
            for (auto &&table : names)
            {
              if (ok)
                sharedDataCache->disable(table.first);
              if (!ok)
                break;
            }
            break;
            
          case StmtDDLAlterSharedCache::ENABLE:

            op = "enable";
            for (auto &&table : names)
            {
              if (ok)
                sharedDataCache->enable(table.first);
              if (!ok)
                break;
            }
            break;

          case StmtDDLAlterSharedCache::DELETE:

            op = "delete";
            for (auto &&table : names)
            {
              if (ok)
                sharedDataCache->remove(table.first);
              if (!ok)
                break;
            }
            break;

          default:
            msgBuf.format("Invalid option %s requested for table %s", op.data(), name.getQualifiedNameAsAnsiString(FALSE, TRUE).data());
            reasonString = msgBuf.data();
            goto error_return_after_release_semaphore;
        }

        if (!ok) {
          msgBuf.format("Failed to %s the shared cache entry for table %s", op.data(), name.getQualifiedNameAsAnsiString(FALSE, TRUE).data());
          reasonString = msgBuf.data();
          goto error_return_after_release_semaphore;
        }
        break;
      case StmtDDLAlterSharedCache::ALL:
        switch (ascNode->getOptions()) {
          case StmtDDLAlterSharedCache::CLEAR:
            op = "clear";
            sharedDataCache = sharedDataCache->destroyAndMake();
            ok = !!sharedDataCache;
            break;
 
        default:
          msgBuf.format("Invalid option %s requested", op.data());
          reasonString = msgBuf.data();
          goto error_return_after_release_semaphore;
       }
       if (!ok) {
         msgBuf.format("Failed to %s the shared cache", op.data());
         reasonString = msgBuf.data();
         goto error_return_after_release_semaphore;
       }
       break;

       default:
        msgBuf.format("Invalid option %s requested", op.data());
        reasonString = msgBuf.data();
        goto error_return_after_release_semaphore;
    }
  } else {
    reasonString = "SharedDescriptorCache::locate() failed";
    QRLogger::log(CAT_SQL_EXE, LL_INFO, "%s", reasonString);
    goto error_return_after_release_semaphore;
  }

  UNOBSERVE_SHARED_DATA_CACHE_LOCK;

  return 0;

error_return_after_release_semaphore:
  UNOBSERVE_SHARED_DATA_CACHE_LOCK;

error_return:
  emitWarningOrErrorForSharedCacheOperation(ascNode->isInternal(), reasonString, "alter");
  return -1;
}

// a wrapper method to getSeabaseRoutineDescInternal so 
// CmpContext context switching can take place. 
// getSeabaseRoutineDescInternal prepares and executes
// several queries on metadata tables
TrafDesc *CmpSeabaseDDL::getSeabaseRoutineDesc(const NAString &catName,
                                      const NAString &schName,
                                      const NAString &objName)
{
   TrafDesc *result = NULL;
   NABoolean useLibBlobStore = FALSE;
   
   if (switchCompiler(CmpContextInfo::CMPCONTEXT_TYPE_META))
     return NULL;

   result = getSeabaseRoutineDescInternal(catName, schName, objName);

   switchBackCompiler();

   return result;
}


TrafDesc *CmpSeabaseDDL::getSeabaseRoutineDescInternal(const NAString &catName,
                                                       const NAString &schName,
                                                       const NAString &objName
                                                       )
{
  Lng32 retcode = 0;
  Lng32 cliRC = 0;
  
  TrafDesc *result;
  char query[4000];
  char buf[4000];

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, 
  CmpCommon::context()->sqlSession()->getParentQid());

  Int64 objectUID = 0;
  Int32 objectOwnerID = 0;
  Int32 schemaOwnerID = 0;
  Int64 objectFlags =  0 ;
  Int64 objDataUID = 0;
  ComObjectType objectType = COM_USER_DEFINED_ROUTINE_OBJECT;

  objectUID = getObjectInfo(&cliInterface,
                            catName.data(), schName.data(),
                            objName.data(), objectType,
                            objectOwnerID,schemaOwnerID,objectFlags,objDataUID);

  if (objectUID == -1 || objectOwnerID == 0)
    {
      if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_) == 0)
        SEABASEDDL_INTERNAL_ERROR("getting object UID and owners for routine desc request");
      processReturn();
      return NULL;
    }

 
    
    
  str_sprintf(buf, "select udr_type, language_type, deterministic_bool,"
                  " sql_access, call_on_null, isolate_bool, param_style,"
                  " transaction_attributes, max_results, state_area_size, external_name,"
                  " parallelism, user_version, external_security, execution_mode,"
                  " library_filename, version, signature,  catalog_name, schema_name,"
                  " object_name, redef_time,library_storage, l.library_uid"
                  " from %s.\"%s\".%s r, %s.\"%s\".%s l, %s.\"%s\".%s o "
                  " where r.udr_uid = %ld and r.library_uid = l.library_uid "
                  " and l.library_uid = o.object_uid for read committed access",
                  getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_ROUTINES,
                  getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_LIBRARIES,
                  getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS,
                  objectUID);
    

  cliRC = cliInterface.fetchRowsPrologue(buf, TRUE/*no exec*/);
  if (cliRC < 0)
  {
     cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
     return NULL;
  }

  cliRC = cliInterface.clearExecFetchClose(NULL, 0);
  if (cliRC < 0)
  {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    return NULL;
  }
  if (cliRC == 100) // did not find the row
  {
     cliInterface.clearGlobalDiags();

     *CmpCommon::diags() << DgSqlCode(-CAT_OBJECT_DOES_NOT_EXIST_IN_TRAFODION)
                        << DgString0(objName);
     return NULL;
  }

  char * ptr = NULL;
  Lng32 len = 0;

  ComTdbVirtTableRoutineInfo *routineInfo = new (STMTHEAP) ComTdbVirtTableRoutineInfo();

  routineInfo->object_uid = objectUID;
  routineInfo->object_owner_id = objectOwnerID;
  routineInfo->schema_owner_id = schemaOwnerID;

  routineInfo->routine_name = objName.data();
  cliInterface.getPtrAndLen(1, ptr, len);
  str_cpy_all(routineInfo->UDR_type, ptr, len);
  routineInfo->UDR_type[len] =  '\0';
  cliInterface.getPtrAndLen(2, ptr, len);
  str_cpy_all(routineInfo->language_type, ptr, len);
  routineInfo->language_type[len] = '\0';
  cliInterface.getPtrAndLen(3, ptr, len);
  if (*ptr == 'Y')
     routineInfo->deterministic = 1;
  else
     routineInfo->deterministic = 0;
  cliInterface.getPtrAndLen(4, ptr, len);
  str_cpy_all(routineInfo->sql_access, ptr, len);
  routineInfo->sql_access[len] = '\0';
  cliInterface.getPtrAndLen(5, ptr, len);
  if (*ptr == 'Y')
     routineInfo->call_on_null = 1;
  else
     routineInfo->call_on_null = 0;
  cliInterface.getPtrAndLen(6, ptr, len);
  if (*ptr == 'Y')
     routineInfo->isolate = 1;
  else
     routineInfo->isolate = 0;
  cliInterface.getPtrAndLen(7, ptr, len);
  str_cpy_all(routineInfo->param_style, ptr, len);
  routineInfo->param_style[len] = '\0';
  cliInterface.getPtrAndLen(8, ptr, len);
  str_cpy_all(routineInfo->transaction_attributes, ptr, len);
  routineInfo->transaction_attributes[len] = '\0';
  cliInterface.getPtrAndLen(9, ptr, len);
  routineInfo->max_results = *(Int32 *)ptr;
  cliInterface.getPtrAndLen(10, ptr, len);
  routineInfo->state_area_size = *(Int32 *)ptr;
  cliInterface.getPtrAndLen(11, ptr, len);
  routineInfo->external_name = new (STMTHEAP) char[len+1];    
  str_cpy_and_null((char *)routineInfo->external_name, ptr, len, '\0', ' ', TRUE);
  cliInterface.getPtrAndLen(12, ptr, len);
  str_cpy_all(routineInfo->parallelism, ptr, len);
  routineInfo->parallelism[len] = '\0';
  cliInterface.getPtrAndLen(13, ptr, len);
  str_cpy_all(routineInfo->user_version, ptr, len);
  routineInfo->user_version[len] = '\0';
  cliInterface.getPtrAndLen(14, ptr, len);
  str_cpy_all(routineInfo->external_security, ptr, len);
  routineInfo->external_security[len] = '\0';
  cliInterface.getPtrAndLen(15, ptr, len);
  str_cpy_all(routineInfo->execution_mode, ptr, len);
  routineInfo->execution_mode[len] = '\0';
  cliInterface.getPtrAndLen(16, ptr, len);
  routineInfo->library_filename = new (STMTHEAP) char[len+1];    
  str_cpy_and_null((char *)routineInfo->library_filename, ptr, len, '\0', ' ', TRUE);
  cliInterface.getPtrAndLen(17, ptr, len);
  routineInfo->library_version = *(Int32 *)ptr;
  cliInterface.getPtrAndLen(18, ptr, len);
  routineInfo->signature = new (STMTHEAP) char[len+1];    
  str_cpy_and_null((char *)routineInfo->signature, ptr, len, '\0', ' ', TRUE);
  // library SQL name, in three parts
  cliInterface.getPtrAndLen(19, ptr, len);
  char *libCat = new (STMTHEAP) char[len+1];    
  str_cpy_and_null(libCat, ptr, len, '\0', ' ', TRUE);
  cliInterface.getPtrAndLen(20, ptr, len);
  char *libSch = new (STMTHEAP) char[len+1];    
  str_cpy_and_null(libSch, ptr, len, '\0', ' ', TRUE);
  routineInfo->lib_sch_name = new (STMTHEAP) char[len+1];
  str_cpy_and_null(routineInfo->lib_sch_name, ptr, len, '\0', ' ', TRUE);
  cliInterface.getPtrAndLen(21, ptr, len);
  char *libObj = new (STMTHEAP) char[len+1];    
  str_cpy_and_null(libObj, ptr, len, '\0', ' ', TRUE);
  ComObjectName libSQLName(libCat, libSch, libObj,
                           COM_UNKNOWN_NAME,
                           ComAnsiNamePart::INTERNAL_FORMAT,
                           STMTHEAP);
  NAString libSQLExtName = libSQLName.getExternalName();
  routineInfo->library_sqlname = new (STMTHEAP) char[libSQLExtName.length()+1];    
  str_cpy_and_null((char *)routineInfo->library_sqlname,
                   libSQLExtName.data(),
                   libSQLExtName.length(),
                   '\0', ' ', TRUE);
  NAString naLibSch(libSch);
 
  
  
  cliInterface.getPtrAndLen(22,ptr,len);
  routineInfo->lib_redef_time = *(Int64 *)ptr;


  cliInterface.getPtrAndLen(23, ptr, len);
  routineInfo->lib_blob_handle = new (STMTHEAP) char[len+1];    
  str_cpy_and_null(routineInfo->lib_blob_handle, ptr, len, '\0', ' ', TRUE);

  cliInterface.getPtrAndLen(24,ptr,len);
  routineInfo->lib_obj_uid= *(Int64 *)ptr;
  
  
    
  if ((routineInfo->lib_blob_handle[0] == '\0')|| (naLibSch  == NAString(SEABASE_MD_SCHEMA)))
    {
      routineInfo->lib_redef_time = -1;
      routineInfo->lib_blob_handle=NULL;
      routineInfo->lib_obj_uid = 0;
    }  
  
  ComTdbVirtTableColumnInfo *paramsArray;
  Lng32 numParams;
  char direction[50];
  str_sprintf(direction, "'%s', '%s', '%s'", 
                      COM_INPUT_PARAM_LIT, COM_OUTPUT_PARAM_LIT,
                      COM_INOUT_PARAM_LIT);
  // Params
  if (getSeabaseColumnInfo(&cliInterface,
                           objectUID,
                           catName, schName, objName,
                           (char *)direction,
                           NULL,
                           NULL,
                           NULL,
                           NULL,
                           &numParams,
                           &paramsArray) < 0)
    {
      processReturn();
      return NULL;
    } 
  
  ComTdbVirtTablePrivInfo * privInfo = 
    getSeabasePrivInfo(&cliInterface, catName, schName, objectUID, 
                       objectType, schemaOwnerID);

  TrafDesc *routine_desc = NULL;
  routine_desc = Generator::createVirtualRoutineDesc(
       objName.data(),
       routineInfo,
       numParams,
       paramsArray,
       privInfo,
       NULL);

  if (routine_desc == NULL)
     processReturn();
  return routine_desc;
}

Lng32 CmpSeabaseDDL::getSeabasePartitionV2Info(ExeCliInterface *cliInterface,
                             Int64 btUid,
                             ComTdbVirtTablePartitionV2Info** outPartionV2InfoArray)
{
  char query[3000];
  Lng32 cliRC;
  Queue *partProp = NULL;

  //get partition/subpartion property
  str_sprintf(query, "select pp.partition_type, pp.partition_column_pos, pp.partition_column_num,"
                     "pp.subpartition_type, pp.subpartition_column_pos, pp.subpartition_column_num "
                     "from %s.\"%s\".%s pp where table_uid = %ld",
                     getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_TABLE_PARTITION, btUid);

  cliRC = cliInterface->fetchAllRows(partProp, query, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0)
  {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  if (partProp->numEntries() == 0) //not found
  {
    *outPartionV2InfoArray = NULL;
    return 0;
//    *CmpCommon::diags() << DgSqlCode(-1097)
  //                      << DgString0(Int64ToNAString(btUid));
  }

  ComTdbVirtTablePartitionV2Info *partitionV2Info =
               new (STMTHEAP) ComTdbVirtTablePartitionV2Info();

  char *data = NULL;
  Lng32 len = 0;

  partProp->position();
  OutputInfo * oi = (OutputInfo*)partProp->getNext();

  //uid
  partitionV2Info->baseTableUid = btUid;
  partitionV2Info->partitionType = *(Lng32*)oi->get(0);

  //partition_column
  oi->get(1, data, len);
  partitionV2Info->partitionColIdx = new(STMTHEAP) char[len + 1];
  strcpy(partitionV2Info->partitionColIdx, data);

  Lng32 colCnt = *(Lng32*)oi->get(2);
  partitionV2Info->partitionColCount = colCnt;

  //subpartition_type
  partitionV2Info->subpartitionType = *(Lng32*)oi->get(3);

  //subpartition_column
  partitionV2Info->subpartitionColCount = *(Lng32*)oi->get(5);
  if (partitionV2Info->subpartitionColCount > 0)
  { 
    oi->get(4, data, len);
    partitionV2Info->subpartitionColIdx = new(STMTHEAP) char[len + 1];
    strcpy(partitionV2Info->subpartitionColIdx, data);
  }

  //get partitions
  str_sprintf(query, "select p.partition_uid puid, p.partition_name pname, p.partition_entity_name pnname, "
              " p.partition_ordinal_position pos, p.partition_expression pe, nvl(tp.partition_uid,0) spuid, "
              " tp.partition_name spname, tp.partition_entity_name spnname, "
              " tp.partition_ordinal_position spos, tp.partition_expression spe from "
              " (select parent_uid, partition_name, partition_entity_name, partition_uid, "
              " partition_ordinal_position, partition_expression from "
              " %s.\"%s\".%s where parent_uid = %ld and partition_level  = '%s') as p left join "
              " (select parent_uid, partition_name, partition_entity_name, partition_uid, "
              " partition_ordinal_position, partition_expression from "
              " %s.\"%s\".%s where partition_level  = '%s') as tp on tp.parent_uid = p.partition_uid order by pos, spos;",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_PARTITIONS, btUid, COM_PRIMARY_PARTITION_LIT,
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_PARTITIONS, COM_SUBPARTITION_LIT);

  Queue *partitionsInfo = NULL;
  cliRC = cliInterface->fetchAllRows(partitionsInfo, query, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0)
  {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  ComTdbVirtTablePartInfo * partInfoArray = NULL;
  Lng32 partCnt = partitionsInfo->numEntries();
  assert(partCnt > 0);

  //need partition count and its subpartition count in advance
  Lng32 partPosAry[partCnt];
  Int64 subpartUidAry[partCnt];
  partitionsInfo->position();
  for (int i = 0; i < partCnt; i++)
  {
    OutputInfo * oi = (OutputInfo*)partitionsInfo->getNext();
    partPosAry[i] = *(Lng32*)oi->get(3);
    subpartUidAry[i] = *(Int64*)oi->get(5);
  }

  //first level partition count
  Lng32 firstLPartCnt = partPosAry[partCnt - 1] + 1;
  Lng32 subPartCntMap[firstLPartCnt];
  memset(subPartCntMap, 0, sizeof(Lng32) * firstLPartCnt);
  for (int i = 0; i < partCnt; i++)
  {
    if (subpartUidAry[i] != 0)// is subpartition
      subPartCntMap[partPosAry[i]]++;
  }

  partitionV2Info->stlPartitionCnt = firstLPartCnt;
  partInfoArray = new(STMTHEAP) ComTdbVirtTablePartInfo[firstLPartCnt];
  partitionsInfo->position();

  int curPartPos = 0;
  int curSubpartPos = 0;
  NABoolean start = true;
  for (int i = 0; i < partCnt; i++)
  {
    OutputInfo * oi = (OutputInfo*)partitionsInfo->getNext();
    if (start)
    {
      partInfoArray[curPartPos].parentUid = btUid;
      partInfoArray[curPartPos].partitionUid = *(Int64*)oi->get(0);

      oi->get(1, data, len);
      partInfoArray[curPartPos].partitionName = new (STMTHEAP)char[len + 1];
      strcpy(partInfoArray[curPartPos].partitionName, data);

      oi->get(2, data, len);
      partInfoArray[curPartPos].partitionEntityName = new (STMTHEAP)char[len + 1];
      strcpy(partInfoArray[curPartPos].partitionEntityName, data);

      partInfoArray[curPartPos].partPosition = curPartPos;

      oi->get(4, data, len);
      partInfoArray[curPartPos].partExpression = new (STMTHEAP)char[len + 1];
      memset(partInfoArray[curPartPos].partExpression, '\0', len+1);
      memcpy(partInfoArray[curPartPos].partExpression, data, len);

      partInfoArray[curPartPos].partExpressionLen = len;
      
      if (subPartCntMap[curPartPos] > 0)
      {
        partInfoArray[curPartPos].subPartArray = new(STMTHEAP)
                     ComTdbVirtTablePartInfo[subPartCntMap[curPartPos]];
        partInfoArray[curPartPos].hasSubPartition = TRUE;
        partInfoArray[curPartPos].subpartitionCnt = subPartCntMap[curPartPos];
      }
      start = false;
    }
    if (subpartUidAry[i] != 0) //subpartition uid not 0
    {
      curSubpartPos = *(Lng32*)oi->get(8);
      ComTdbVirtTablePartInfo &ptmp = 
          partInfoArray[curPartPos].subPartArray[curSubpartPos];
      ptmp.parentUid = *(Int64*)oi->get(0);
      ptmp.partitionUid = subpartUidAry[i];
      
      oi->get(6, data, len);
      ptmp.partitionName = new (STMTHEAP)char[len + 1];
      strcpy(ptmp.partitionName, data);

      oi->get(7, data, len);
      ptmp.partitionEntityName = new (STMTHEAP)char[len + 1];
      strcpy(ptmp.partitionEntityName, data);

      ptmp.partPosition = curSubpartPos;

      oi->get(9, data, len);
      ptmp.partExpression = new (STMTHEAP)char[len + 1];
      memset(ptmp.partExpression, '\0', len+1);
      memcpy(ptmp.partExpression, data, len);
      ptmp.partExpressionLen = len;
      ptmp.isSubPartition = TRUE;
    }

    if (i + 1 < partCnt)
    {
      if (partPosAry[i+1] != curPartPos)  //next partition
      {
        start = true;
        curPartPos = partPosAry[i+1];
      }
    }
  }

  partitionV2Info->partArray = partInfoArray;
  if (outPartionV2InfoArray)
    *outPartionV2InfoArray = partitionV2Info;

  return 0;
}

Lng32 CmpSeabaseDDL::getSeabasePartitionBoundaryInfo(ExeCliInterface *cliInterface,
                             Int64 btUid,
                             ComTdbVirtTablePartitionV2Info** outPartionV2InfoArray)
{
  char query[3000];
  char *data = NULL;
  Lng32 len = 0, cliRC = 0;

  //get partition info
  str_sprintf(query, "select part.parent_uid, part.partition_name, part.partition_entity_name, part.partition_level,"
              "part.partition_ordinal_position, part.partition_expression, nvl(prevp.parent_uid, 0), "
              "prevp.partition_name, prevp.partition_entity_name, prevp.partition_level, "
              "prevp.partition_ordinal_position, prevp.partition_expression from "
              "(select parent_uid, partition_name, partition_entity_name, partition_level, "
              "partition_ordinal_position, partition_expression "
              "from %s.\"%s\".%s where partition_uid = %ld) part left join "
              "(select parent_uid, partition_name, partition_entity_name, partition_level, "
              "partition_ordinal_position, partition_expression "
              "from %s.\"%s\".%s) prevp on prevp.parent_uid = part.parent_uid "
              "and part.partition_ordinal_position = prevp.partition_ordinal_position + 1",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_PARTITIONS, btUid,
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_PARTITIONS);

  Queue *partitionsInfo = NULL;
  cliRC = cliInterface->fetchAllRows(partitionsInfo, query, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0)
  {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  if (partitionsInfo->numEntries() == 0)
  {
    *outPartionV2InfoArray = NULL;
    return 0;
  }
  assert(partitionsInfo->numEntries() == 1);

  partitionsInfo->position();
  OutputInfo * oi = (OutputInfo*)partitionsInfo->getNext();

  Lng32 partCnt = (*(Int64*)oi->get(6) == 0) ? 1 : 2;

  ComTdbVirtTablePartInfo *partInfo = new (STMTHEAP) ComTdbVirtTablePartInfo[partCnt];

  Int32 offset;
  for (int k = 0; k < partCnt; k++)
  {
    if (partCnt == 1 ||
       (partCnt == 2 && k == 1))
      offset = 0;
    else if (partCnt == 2 && k == 0)
      offset = 6; 
    partInfo[k].parentUid = *(Int64*)oi->get(offset + 0);
    partInfo[k].partitionUid = btUid;
  
    oi->get(offset + 1, data, len);
    partInfo[k].partitionName = new (STMTHEAP)char[len + 1];
    strcpy(partInfo[k].partitionName, data);

    oi->get(offset + 2, data, len);
    partInfo[k].partitionEntityName = new (STMTHEAP)char[len + 1];
    strcpy(partInfo[k].partitionEntityName, data);

    partInfo[k].isSubPartition = strncmp((char*)oi->get(offset + 3), COM_PRIMARY_PARTITION_LIT, 1) == 0 ?
                            false : true;

    partInfo[k].partPosition = *(Lng32*)oi->get(offset + 4);

    oi->get(offset + 5, data, len);
    partInfo[k].partExpression = new (STMTHEAP)char[len + 1];
    memset(partInfo[k].partExpression, '\0', len+1);
    memcpy(partInfo[k].partExpression, data, len);

    partInfo[k].partExpressionLen = len;
  }
  
  Queue *partProp = NULL;    
  //get partition/subpartion property
  if (partInfo[0].isSubPartition)
  str_sprintf(query, "select pp.partition_type, pp.partition_column_pos, pp.partition_column_num, "
                     "pp.subpartition_type, pp.subpartition_column_pos, pp.subpartition_column_num, "
                     "pp.table_uid from %s.\"%s\".%s pp ,  %s.\"%s\".%s tp "
                     "where tp.parent_uid = pp.table_uid and tp.partition_uid = %ld",
                     getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_TABLE_PARTITION,
                     getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_PARTITIONS, partInfo[0].parentUid);
  else
  str_sprintf(query, "select pp.partition_type, pp.partition_column_pos, pp.partition_column_num, "
                     "pp.subpartition_type, pp.subpartition_column_pos, pp.subpartition_column_num, "
                     "pp.table_uid from %s.\"%s\".%s pp where table_uid = %ld",
                     getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_TABLE_PARTITION, partInfo[0].parentUid);

  cliRC = cliInterface->fetchAllRows(partProp, query, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0)
  {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  if (partProp->numEntries() == 0) //not found
  {
    *outPartionV2InfoArray = NULL;
    return 0;
  }

  ComTdbVirtTablePartitionV2Info *partitionV2Info =
               new (STMTHEAP) ComTdbVirtTablePartitionV2Info();
  partitionV2Info->stlPartitionCnt = partCnt;

  partProp->position();
  oi = (OutputInfo*)partProp->getNext();

  //uid
  partitionV2Info->baseTableUid = *(Int64*)oi->get(6);
  partitionV2Info->partitionType = *(Lng32*)oi->get(0);

  //partition_column
  oi->get(1, data, len);
  partitionV2Info->partitionColIdx = new(STMTHEAP) char[len + 1];
  strcpy(partitionV2Info->partitionColIdx, data);

  Lng32 colCnt = *(Lng32*)oi->get(2);
  partitionV2Info->partitionColCount = colCnt;

  //subpartition_type
  partitionV2Info->subpartitionType = *(Lng32*)oi->get(3);

  //subpartition_column
  partitionV2Info->subpartitionColCount = *(Lng32*)oi->get(5);
  if (partitionV2Info->subpartitionColCount > 0)
  { 
    oi->get(4, data, len);
    partitionV2Info->subpartitionColIdx = new(STMTHEAP) char[len + 1];
    strcpy(partitionV2Info->subpartitionColIdx, data);
  }

  partitionV2Info->partArray = partInfo;
  if (outPartionV2InfoArray)
    *outPartionV2InfoArray = partitionV2Info;

  return 0;
}

// true means the same; false means not the same
NABoolean CmpSeabaseDDL::compareDDLs(ExeCliInterface *cliInterface,
                                       NATable * naTabA,
                                       NATable * naTabB,
                                       NABoolean ignoreClusteringKey,
                                       NABoolean ignoreSaltUsing,
                                       NABoolean ignoreSaltNum,
                                       NABoolean ignoreV2Partition,//new range partition
                                       NABoolean ignoreIndex
                                       )
{
  NABoolean ret = TRUE;

  if (naTabA == NULL || naTabB == NULL)
    return FALSE;

  if (naTabA == naTabB ||
      naTabA->getTableName() == naTabB->getTableName()/* may happen?*/)
    return TRUE;

  // 
  if (NOT ignoreV2Partition &&
      naTabA->isPartitionV2Table() != naTabB->isPartitionV2Table())
    return FALSE;

  // check user columns and types
  const NAColumnArray &colArrayA = naTabA->getNAColumnArray();
  const NAColumnArray &colArrayB = naTabB->getNAColumnArray();

  NAColumnArray userColArrayA(STMTHEAP);
  NAColumnArray userColArrayB(STMTHEAP);

  for (CollIndex i=0; i<colArrayA.entries(); i++)
  {
    if (colArrayA[i]->isUserColumn())
    {
      userColArrayA.append(colArrayA[i]);
    }
  }
  for (CollIndex i=0; i<colArrayB.entries(); i++)
  {
    if (colArrayB[i]->isUserColumn())
    {
      userColArrayB.append(colArrayB[i]);
    }
  }

  if (userColArrayA.entries() != userColArrayB.entries())
    return FALSE;

  // assume NAColumn is sorted in NATable
  for (CollIndex i=0; i<userColArrayA.entries(); i++)
  {
    if (NOT (*userColArrayA[i]->getType() == *userColArrayB[i]->getType()) ||
        userColArrayA[i]->getPosition() != userColArrayB[i]->getPosition())
    {
      ret = FALSE;
      break;
    }
  }
  if (ret == FALSE)
    return ret;

  // ignoreClusteringKey
  if (NOT ignoreClusteringKey)
  {
    const NAColumnArray &clusterKeysA = naTabA->getClusteringIndex()->getAllColumns();
    const NAColumnArray &clusterKeysB = naTabB->getClusteringIndex()->getAllColumns();

    if (clusterKeysA.entries() == clusterKeysB.entries())
    {
      for (CollIndex i=0; i<clusterKeysA.entries(); i++)
      {
        if (NOT (*clusterKeysA[i]->getType() == *clusterKeysB[i]->getType()) ||
            clusterKeysA[i]->getPosition() != clusterKeysB[i]->getPosition())
        {
          ret = FALSE;
          break;
        }
      }
    }
    else
      ret = FALSE;

    if (ret == FALSE)
      return ret;
  }

  if (NOT ignoreSaltUsing)
  {
    // todo : check the salt keys
  }


  return ret;
}

// *****************************************************************************
// *                                                                           *
// * Function: checkSpecifiedPrivs                                             *
// *                                                                           *
// *    Processes the privilege specification and returns the lists of object  *
// * and column privileges.                                                    *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <privActsArray>                 ElemDDLPrivActArray &           In       *
// *    is a reference to the parsed list of privileges to be granted or       *
// *  revoked.                                                                 *
// *                                                                           *
// *  <externalObjectName>            const char *                    In       *
// *    is the fully qualified name of the object that privileges are being    *
// *  granted or revoked on.                                                   *
// *                                                                           *
// *  <objectType>                    ComObjectType                   In       *
// *    is the type of the object that privileges are being granted or         *
// *  revoked on.                                                              *
// *                                                                           *
// *  <naTable>                       NATable *                       In       *
// *    if the object type is a table or view, the cache for the metadata      *
// *  related to the object, otherwise NULL.                                   *
// *                                                                           *
// *  <objectPrivs>                   std::vector<PrivType> &         Out      *
// *    passes back a list of the object privileges to be granted or revoked.  *
// *                                                                           *
// *  <colPrivs>                      std::vector<ColPrivSpec> &      Out      *
// *    passes back a list of the column privileges and the specific columns   *
// *  on which the privileges are to be granted or revoked.                    *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: bool                                                             *
// *                                                                           *
// *  true: Privileges processed successfully.  Lists of object and column     *
// *        privileges were returned.                                          *
// * false: Error processing privileges. The error is in the diags area.       *
// *                                                                           *
// *****************************************************************************
static bool checkSpecifiedPrivs(
   ElemDDLPrivActArray & privActsArray,  
   const char * externalObjectName,
   ComObjectType objectType,
   NATable * naTable,
   std::vector<PrivType> & objectPrivs,
   std::vector<ColPrivSpec> & colPrivs) 

{

   for (Lng32 i = 0; i < privActsArray.entries(); i++)
   {
      PrivType privType;
      if (!privActsArray[i]->elmPrivToPrivType(privType))
      {
         *CmpCommon::diags() << DgSqlCode(-CAT_INVALID_PRIV_FOR_OBJECT)
                             << DgString0(PrivMgrUserPrivs::convertPrivTypeToLiteral(privType).c_str()) 
                             << DgString1(externalObjectName);
         return false;
      }
      
      //
      // The same privilege cannot be specified twice in one grant or revoke
      // statement.  This includes granting or revoking the same privilege at 
      // the object-level and the column-level.
      if (hasValue(objectPrivs,privType) || hasValue(colPrivs,privType))
      {
         *CmpCommon::diags() << DgSqlCode(-CAT_DUPLICATE_PRIVILEGES);
         return false;
      }
   
      if (!isValidPrivTypeForObject(objectType,privType))
      {
         *CmpCommon::diags() << DgSqlCode(-CAT_PRIVILEGE_NOT_ALLOWED_FOR_THIS_OBJECT_TYPE)
                             << DgString0(PrivMgrUserPrivs::convertPrivTypeToLiteral(privType).c_str());
         return false;
      }
      
      // For some DML privileges the user may be granting either column  
      // or object privileges.  If it is not a privilege that can be granted
      // at the column level, it is an object-level privilege.
      if (!isColumnPrivType(privType))
      {
         objectPrivs.push_back(privType);
         continue;
      }
      
      ElemDDLPrivActWithColumns * privActWithColumns = dynamic_cast<ElemDDLPrivActWithColumns *>(privActsArray[i]);
      ElemDDLColNameArray colNameArray = privActWithColumns->getColumnNameArray();
      // If no columns were specified, this is an object-level privilege.
      if (colNameArray.entries() == 0)  
      {
         objectPrivs.push_back(privType);
         continue;
      }
      
      // Column-level privileges can only be specified for tables and views.
      if (objectType != COM_BASE_TABLE_OBJECT && objectType != COM_VIEW_OBJECT)
      {
         *CmpCommon::diags() << DgSqlCode(-CAT_INCORRECT_OBJECT_TYPE)
                             << DgTableName(externalObjectName);
         return false;
      }
      
      // It's a table or view, validate the column.  Get the list of 
      // columns and verify the list contains the specified column(s).
      const NAColumnArray &nacolArr = naTable->getNAColumnArray();
      for (size_t c = 0; c < colNameArray.entries(); c++)
      {
         const NAColumn * naCol = nacolArr.getColumn(colNameArray[c]->getColumnName());
         if (naCol == NULL)
         {
            *CmpCommon::diags() << DgSqlCode(-CAT_COLUMN_DOES_NOT_EXIST_ERROR)
                                << DgColumnName(colNameArray[c]->getColumnName());
            return false;
         }
         // Specified column was found.
         ColPrivSpec colPrivEntry;
         
         colPrivEntry.privType = privType;
         colPrivEntry.columnOrdinal = naCol->getPosition();
         colPrivs.push_back(colPrivEntry);
      }
   } 
   
   return true;

}
//************************ End of checkSpecifiedPrivs **************************


// *****************************************************************************
// *                                                                           *
// * Function: hasValue                                                        *
// *                                                                           *
// *   This function determines if a ColPrivSpec vector contains a PrivType    *
// *   value.                                                                  *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <container>                  std::vector<ColPrivSpec>           In       *
// *    is the vector of ColPrivSpec values.                                   *
// *                                                                           *
// *  <value>                      PrivType                           In       *
// *    is the value to be compared against existing values in the vector.     *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: bool                                                             *
// *                                                                           *
// *  true: Vector contains the value.                                         *
// * false: Vector does not contain the value.                                 *
// *                                                                           *
// *****************************************************************************
static bool hasValue(
   const std::vector<ColPrivSpec> & container,
   PrivType value)
   
{

   for (size_t index = 0; index < container.size(); index++)
      if (container[index].privType == value)
         return true;
         
   return false;
   
}
//***************************** End of hasValue ********************************

// *****************************************************************************
// *                                                                           *
// * Function: hasValue                                                        *
// *                                                                           *
// *   This function determines if a PrivType vector contains a PrivType value.*
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <container>                  std::vector<PrivType>              In       *
// *    is the vector of 32-bit values.                                        *
// *                                                                           *
// *  <value>                      PrivType                           In       *
// *    is the value to be compared against existing values in the vector.     *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: bool                                                             *
// *                                                                           *
// *  true: Vector contains the value.                                         *
// * false: Vector does not contain the value.                                 *
// *                                                                           *
// *****************************************************************************
static bool hasValue(
   const std::vector<PrivType> & container,
   PrivType value)
   
{

   for (size_t index = 0; index < container.size(); index++)
      if (container[index] == value)
         return true;
         
   return false;
   
}
//***************************** End of hasValue ********************************


// *****************************************************************************
// *                                                                           *
// * Function: isMDGrantRevokeOK                                               *
// *                                                                           *
// *   This function determines if a grant or revoke a privilege to/from a     *
// * metadata table should be allowed.                                         *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <objectPrivs>                const std::vector<PrivType> &      In       *
// *    is a vector of object-level privileges.                                *
// *                                                                           *
// *  <colPrivs>                   const std::vector<ColPrivSpec> &   In       *
// *    is a vector of column-level privileges.                                *
// *                                                                           *
// *  <isGrant>                    bool                               In       *
// *    is a true if this is a grant operation, false if revoke.               *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: bool                                                             *
// *                                                                           *
// *  true: Grant/revoke is OK.                                                *
// * false: Grant/revoke should be rejected.                                   *
// *                                                                           *
// *****************************************************************************
static bool isMDGrantRevokeOK(
   const std::vector<PrivType> & objectPrivs,
   const std::vector<ColPrivSpec> & colPrivs,
   bool isGrant)

{

// Can only grant or revoke privileges on MD tables if only granting select,
// or only revoking all privileges.  Only valid combination is no object
// privileges and 1 or more column privileges (all SELECT), or no column
// privilege and exactly one object privilege.  In the latter case, the 
// privilege must either be SELECT, or if a REVOKE operation, either 
// ALL_PRIVS or ALL_DML.

// First check if no column privileges.

   if (colPrivs.size() == 0)
   {
      // Should never get this far with both vectors being empty, but check 
      // just in case.
      if (objectPrivs.size() == 0) 
         return false;
         
      if (objectPrivs.size() > 1)
         return false;
         
      if (objectPrivs[0] == SELECT_PRIV)
         return true;
         
      if (isGrant)
         return false;
       
      if (objectPrivs[0] == ALL_PRIVS || objectPrivs[0] == ALL_DML)
         return true;
      
      return false;
   }
   
// Have column privs
   if (objectPrivs.size() > 0)
      return false;
      
   for (size_t i = 0; i < colPrivs.size(); i++)
      if (colPrivs[i].privType != SELECT_PRIV)
         return false;
         
   return true;     

}
//************************* End of isMDGrantRevokeOK ***************************


// *****************************************************************************
// *                                                                           *
// * Function: isValidPrivTypeForObject                                        *
// *                                                                           *
// *   This function determines if a priv type is valid for an object.         *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <objectType>                 ComObjectType                      In       *
// *    is the type of the object.                                             *
// *                                                                           *
// *  <privType>                   PrivType                           In       *
// *    is the type of the privilege.                                          *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: bool                                                             *
// *                                                                           *
// *  true: Priv type is valid for object.                                     *
// * false: Priv type is not valid for object.                                 *
// *                                                                           *
// *****************************************************************************
static bool isValidPrivTypeForObject(
   ComObjectType objectType,
   PrivType privType)
   
{

   switch (objectType)
   {
      case COM_LIBRARY_OBJECT:
         return isLibraryPrivType(privType); 
      case COM_STORED_PROCEDURE_OBJECT:
      case COM_USER_DEFINED_ROUTINE_OBJECT:
         return isUDRPrivType(privType); 
      case COM_SEQUENCE_GENERATOR_OBJECT:
         return isSequenceGeneratorPrivType(privType); 
      case COM_BASE_TABLE_OBJECT:
      case COM_VIEW_OBJECT:
         return isTablePrivType(privType); 
      default:
         return false;
   }

   return false;

}
//********************* End of isValidPrivTypeForObject ************************

