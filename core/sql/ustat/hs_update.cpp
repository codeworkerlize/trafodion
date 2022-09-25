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
 * File:         hs_update.C
 * Descriptioon: Entry for UPDATE STATISTICS.
 * Created:      03/25/96
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */
#define HS_FILE "hs_update"




#include <stdlib.h>
#include "Platform.h"
#include "ComDiags.h"
#include "CmpCommon.h"
#include "cli_stdh.h"
#include "hs_globals.h"
#include "hs_parser.h"
#include "hs_cli.h"
#include "hs_auto.h"
#include "NATable.h"
#include "ComSpace.h" 
#include "CmpDescribe.h"
#include "ComSmallDefs.h"
#define   SQLPARSERGLOBALS_FLAGS				  
#include "SqlParserGlobals.h"
#include "SchemaDB.h"

THREAD_P char *hs_input = NULL;
THREAD_P HSGlobalsClass *hs_globals_y = NULL; // Declare global pointer to hs_globals.  Used by 
                                     // parser (Yacc code) only.  Assigned by ShowStats() GenerateStats()
                                     // and UpdateStats() functions.
THREAD_P Lng32 HSCliStatement::statementNum = 0;

static NABoolean lcl_TableHasStatistics(const HSTableDef& rTable, NABoolean bHive = FALSE, NABoolean bHbase = FALSE)
  {
    NAString sCatalogSchema;
    if (bHive)
      sCatalogSchema = NAString(HIVE_STATS_CATALOG)+NAString(".")+NAString(HIVE_STATS_SCHEMA);
    else if(bHbase)
      sCatalogSchema = NAString(HBASE_STATS_CATALOG)+NAString(".")+NAString(HBASE_STATS_SCHEMA);
    else
      sCatalogSchema = NAString(rTable.getCatName())+NAString(".")+NAString(rTable.getSchemaName());

    char UID[30];
    sprintf(UID,"%ld",rTable.getObjectUID());
    if (bHive||bHbase)
      {
        HSCursor cursor(STMTHEAP, "HS_CLI_CHECK_SCHEMA");
        NAString queryStr = "SELECT COUNT(*) FROM TRAFODION.\"_MD_\".OBJECTS "
                            "WHERE OBJECT_NAME='__SCHEMA__' AND SCHEMA_NAME=";
        if (bHive)
            queryStr.append("'").append(HIVE_STATS_SCHEMA_NO_QUOTES).append("'");
        else
            queryStr.append("'").append(HBASE_STATS_SCHEMA_NO_QUOTES).append("'");

        queryStr.append(" AND OBJECT_TYPE='PS' FOR READ UNCOMMITTED ACCESS");
        Lng32 retcode = cursor.prepareQuery(queryStr.data(), 0, 1);
        if ( 0 != retcode )
          return FALSE;
        SQLSTMT_ID* stmtId = cursor.getStmt();
        SQLDESC_ID* outputDesc = cursor.getOutDesc();
        retcode = cursor.open();
        if ( 0 != retcode )
          return FALSE;

        Int64 nRowCount = 0;
        retcode = SQL_EXEC_Fetch(stmtId, outputDesc, 1, 2, &nRowCount, NULL);
        if ( 0 == retcode && nRowCount > 0 )
          {
            //hive/hbase stats schema exist
            HSCursor cursor3019(STMTHEAP, "HS_CLI_CHECK_STATTABLE");
            NAString q = "SELECT COUNT(*) FROM ";
            q.append(sCatalogSchema).append(".").append("SB_HISTOGRAMS")
             .append(" WHERE TABLE_UID=").append(UID)
             .append(" FOR READ UNCOMMITTED ACCESS");
            retcode = cursor3019.prepareQuery(q.data(), 0, 1);
            if ( 0 != retcode )
              return FALSE;
            stmtId = cursor3019.getStmt();
            outputDesc = cursor3019.getOutDesc();
            retcode = cursor3019.open();
            if ( 0 != retcode )
              return FALSE;

            Int64 nRowCount3019 = 0;
            retcode = SQL_EXEC_Fetch(stmtId, outputDesc, 1, 2, &nRowCount3019, NULL);
            if ( 0 == retcode && nRowCount3019 > 0 )
              return TRUE;
          }
      }
    else
      {
        HSCursor cursor;
        NAString queryStr = "SELECT COUNT(*) FROM ";
        queryStr.append(sCatalogSchema).append(".").append("SB_HISTOGRAMS")
                .append(" WHERE TABLE_UID=").append(UID)
                .append(" FOR READ UNCOMMITTED ACCESS");
        Int64 nRowCount = 0;
        Lng32 retcode = cursor.fetchNumColumn(queryStr, NULL, &nRowCount);
        if ( 0 == retcode && nRowCount > 0 )
          return TRUE;
      }
    return FALSE;
  }

Lng32 getPartitionNameByForClause(NATable *naTable, const char * sourceTableName, const char * partitionForText, LIST(NAString) ** partitionNames)
{
  Lng32 retcode = 0;
  Parser parser(CmpCommon::context());

  NAString queryString = "SELECT * FROM ";
  queryString += sourceTableName;
  queryString += " PARTITION FOR (";
  queryString += partitionForText;
  queryString += ")";

  ComDiagsArea * diagsArea = CmpCommon::diags();
  Lng32 diagsMark = diagsArea->mark(); // we'll interpret the parser errors

  ExprNode * parseTree = (RelExpr *)parser.parseDML(queryString.data(), queryString.length(), CharInfo::UTF8);

  if (!parseTree)
    {
      if (diagsArea->contains(-UERR_SYNTAX_ERROR))
        {
          // suppress vanilla syntax error in favor of something more to the point
          diagsArea->rewind(diagsMark, TRUE);
          retcode = -UERR_EXPRESSION_SYNTAX;
          HSFuncMergeDiags(retcode, partitionForText);
          HSHandleError(retcode);
        }
      else
        {
          // keep the parser error but supplement it with an explanation
          retcode = -UERR_EXPRESSION_SYNTAX_1;
          HSFuncMergeDiags(retcode, partitionForText);
          HSHandleError(retcode);
        }
    }   

  // The parser produces a StmtQuery node
  StmtNode *stmt = parseTree->castToStatementExpr();
  CMPASSERT(stmt);

  BindWA bindWA(ActiveSchemaDB(), CmpCommon::context());
  CMPASSERT(stmt->isAQueryStatement());
  RelExpr * queryExpr = stmt->getQueryExpression();
  CMPASSERT(queryExpr);

  ItemExpr* valExpr = NULL;
  RelExpr * lf = (RelExpr*)queryExpr->getChild(0);
  CMPASSERT(lf);
  if (lf->isPartnValuesSpecified())
  {
    valExpr = lf->getSpecPartnVals();
  }

  CMPASSERT(valExpr);
  ItemExprList valList(valExpr, bindWA.wHeap());
  TableDesc * tableDesc = NULL;
  CorrName tableCorrName(naTable->getFullyQualifiedGuardianName(), (CollHeap*)0, NAString());
  tableDesc = bindWA.createTableDesc(naTable, tableCorrName, FALSE, NULL);
  if (partitionNames)
    *partitionNames = tableDesc->getMatchedPartInfo(&bindWA, valList);
  
  return retcode;
}

Lng32 GenerateStats()
  {
    Lng32 retcode = 0;

    HSLogMan *LM = HSLogMan::Instance();
    LM->Log("\nGenerateStats\n=====================================================================");
    HSGlobalsClass *hs_globals_obj= GetHSContext();

    if(!hs_globals_obj->objGenStatsSource || !hs_globals_obj->objDef)
      {
        return 0;
      }

    if(!hs_globals_obj->objGenStatsSource->getNATable()
       || !hs_globals_obj->objDef->getNATable())
          {
            return 0;
          }

    if ( hs_globals_obj->objGenStatsSource->getCatName() != hs_globals_obj->objDef->getCatName() )
      {//Not allow stats to be moved from one storage to another(e.g HIVE - TRAFODION)
        HSFuncMergeDiags(-USERR_GENERATE_STATS_TABLE_NOT_SAME);
        retcode = -1;
        HSExitIfError(retcode);
      }

    Lng32 nSrcTableSalt = -1;
    Lng32 nSrcTableSyskey = -1;
    Lng32 nTarTableSalt = -1;
    Lng32 nTarTableSyskey = -1;
    NABoolean bSrcTableHasSysKey = FALSE;
    NABoolean bTarTableHasSysKey = FALSE;
    NABoolean bSrcTableHasSalt = hs_globals_obj->objGenStatsSource->getNATable()->hasSaltedColumn(&nSrcTableSalt);
    NABoolean bTarTableHasSalt = hs_globals_obj->objDef->getNATable()->hasSaltedColumn(&nTarTableSalt);
    //check same number of columns,the source table's columns and
    //the target table's columns must be same.
    Lng32 nSrcTableCols = hs_globals_obj->objGenStatsSource->getNumCols();
    Lng32 nTarTableCols = hs_globals_obj->objDef->getNumCols();
    //find source table SYSKEY
    for ( int i = 0 ; i<nSrcTableCols; ++i )
       {
         HSColumnStruct aColSource = hs_globals_obj->objGenStatsSource->getColInfo(i);
         if (aColSource.colname && *aColSource.colname == NAString("SYSKEY"))
           {
             bSrcTableHasSysKey = TRUE;
             nSrcTableSyskey = i;
             break;
           }
       }
    //find target table SYSKEY
    for ( int i = 0 ; i<nTarTableCols; ++i )
       {
         HSColumnStruct aColTarget = hs_globals_obj->objDef->getColInfo(i);
         if (aColTarget.colname && *aColTarget.colname == NAString("SYSKEY"))
           {
             bTarTableHasSysKey = TRUE;
             nTarTableSyskey = i;
             break;
           }
       }
    Lng32 nSrcCount = nSrcTableCols;
    Lng32 nTarCount = nTarTableCols;
    if (bSrcTableHasSysKey)
      nSrcCount--;
    if (bSrcTableHasSalt)
      nSrcCount--;
    if (bTarTableHasSysKey)
      nTarCount--;
    if (bTarTableHasSalt)
      nTarCount--;

    if (nSrcCount != nTarCount)
      {
        HSFuncMergeDiags(-USERR_GENERATE_STATS_TABLE_NOT_SAME);
        retcode = -1;
        HSExitIfError(retcode);
      }

    NAString sCatName = hs_globals_obj->objGenStatsSource->getCatName();
    NAString sSourceHistograms;
    NAString sTargetHistograms;
    if(hs_globals_obj->isHiveCat(sCatName))
      {
        sSourceHistograms = NAString(HIVE_STATS_CATALOG)+NAString(".")+NAString(HIVE_STATS_SCHEMA);
      }
    else if(hs_globals_obj->isNativeHbaseCat(sCatName))
      {
        sSourceHistograms = NAString(HBASE_STATS_CATALOG)+NAString(".")+NAString(HBASE_STATS_SCHEMA);
      }
    else
      {
        sSourceHistograms = NAString(sCatName)+NAString(".")+NAString(hs_globals_obj->objGenStatsSource->getSchemaName());
      }

    sCatName = hs_globals_obj->objDef->getCatName();
    if(hs_globals_obj->isHiveCat(sCatName))
      {
        sTargetHistograms = NAString(HIVE_STATS_CATALOG)+NAString(".")+NAString(HIVE_STATS_SCHEMA);
      }
    else if(hs_globals_obj->isNativeHbaseCat(sCatName))
      {
        sTargetHistograms = NAString(HBASE_STATS_CATALOG)+NAString(".")+NAString(HBASE_STATS_SCHEMA);
      }
    else
      {
        sTargetHistograms = NAString(sCatName)+NAString(".")+NAString(hs_globals_obj->objDef->getSchemaName());
      }

    Int64 nSourceTableUID = hs_globals_obj->objGenStatsSource->getObjectUID();
    char cSourceUID[30];
    sprintf(cSourceUID,"%ld",nSourceTableUID);

    Int64 nTargetTableUID = hs_globals_obj->objDef->getObjectUID();
    char cTargetUID[30];
    sprintf(cTargetUID,"%ld",nTargetTableUID);
    //check same column type
    for(int nSrcCol = 0, nTarCol= 0 ;
        nSrcCol < nSrcTableCols && nTarCol < nTarTableCols;)
      {
        //ignore SALT
        if (bSrcTableHasSalt && nSrcCol == nSrcTableSalt)
          {
            ++nSrcCol;
            continue;
          }
        if (bTarTableHasSalt && nTarCol == nTarTableSalt)
          {
            ++nTarCol;
            continue;
          }
        HSColumnStruct aColSource = hs_globals_obj->objGenStatsSource->getColInfo(nSrcCol);
        HSColumnStruct aColTarget = hs_globals_obj->objDef->getColInfo(nTarCol);
        //ignore SYSKEY
        if (aColSource.colname && *aColSource.colname == NAString("SYSKEY"))
          {
            ++nSrcCol;
            continue;
          }
        if (aColTarget.colname && *aColTarget.colname == NAString("SYSKEY"))
          {
            ++nTarCol;
            continue;
          }
        if(aColSource.datatype != aColTarget.datatype
           || aColSource.charset != aColTarget.charset
           || aColSource.length != aColTarget.length
           || aColSource.precision != aColTarget.precision
           || aColSource.scale != aColTarget.scale)
          {
            HSFuncMergeDiags(-USERR_GENERATE_STATS_TABLE_NOT_SAME);
            retcode = -1;
            HSExitIfError(retcode);
          }
        ++nSrcCol;
        ++nTarCol;
      }

    //check TRAFODION."_HIVESTATS_" or TRAFODION."_HBASESTATS_" exists
    //SELECT COUNT(*) FROM TRAFODION."_MD_".OBJECTS
    //WHERE OBJECT_NAME='__SCHEMA__' AND
    //SCHEMA_NAME='_HIVESTATS_' AND
    //OBJECT_TYPE='PS' FOR READ UNCOMMITTED ACCESS
    if (hs_globals_obj->isHiveCat(hs_globals_obj->objGenStatsSource->getCatName())
        || hs_globals_obj->isNativeHbaseCat(hs_globals_obj->objGenStatsSource->getCatName()))
      {
        HSCursor cursor;
        NAString queryStr = "SELECT COUNT(*) FROM TRAFODION.\"_MD_\".OBJECTS "
                            "WHERE OBJECT_NAME='__SCHEMA__' AND SCHEMA_NAME=";
        if (hs_globals_obj->isHiveCat(hs_globals_obj->objGenStatsSource->getCatName()))
            queryStr.append("'").append(HIVE_STATS_SCHEMA_NO_QUOTES).append("'");
        else if(hs_globals_obj->isNativeHbaseCat(hs_globals_obj->objGenStatsSource->getCatName()))
            queryStr.append("'").append(HBASE_STATS_SCHEMA_NO_QUOTES).append("'");

        queryStr.append(" AND OBJECT_TYPE='PS' FOR READ UNCOMMITTED ACCESS");

        Int64 nRowCount = 0;
        retcode = cursor.fetchNumColumn(queryStr, NULL, &nRowCount);
        if ( 0 != retcode || 0 ==nRowCount )
          {
            HSFuncMergeDiags(-USERR_GENERATE_STATS_HIST_EMPTY, hs_globals_obj->objGenStatsSource->getAnsiName()->data());
            retcode = -1;
            HSExitIfError(retcode);
          }
      }
    //source table histogram exists
    //SELECT COUNT(*) FROM CATALOG.SCHEMA.SB_HISTOGRAMS WHERE TABLE_UID=sourcetableUID
    sCatName = hs_globals_obj->objGenStatsSource->getCatName();
    NABoolean bExist = lcl_TableHasStatistics(*hs_globals_obj->objGenStatsSource,
                                              hs_globals_obj->isHiveCat(sCatName),
                                              hs_globals_obj->isNativeHbaseCat(sCatName));
    if (!bExist)
      {
        HSFuncMergeDiags(-USERR_GENERATE_STATS_HIST_EMPTY, hs_globals_obj->objGenStatsSource->getAnsiName()->data());
        retcode = -1;
        HSExitIfError(retcode);
      }


    //target table must has no HISTOGRAMS
    //SELECT COUNT(*) FROM CATALOG.SCHEMA.SB_HISTOGRAMS WHERE TABLE_UID=targettableUID
    sCatName = hs_globals_obj->objDef->getCatName();
    bExist = lcl_TableHasStatistics(*hs_globals_obj->objDef,
                                    hs_globals_obj->isHiveCat(sCatName),
                                    hs_globals_obj->isNativeHbaseCat(sCatName));
    if ( bExist )
      {
        HSFuncMergeDiags(-USERR_GENERATE_STATS_INVALID_TABLE, hs_globals_obj->objDef->getAnsiName()->data());
        retcode = -1;
        HSExitIfError(retcode);
      }

    //save all source table's histograms in vHistid
    //remove all salt and syskey histogram
    std::vector<ULng32> vHistid;
    {
        ULng32 histid = 0;
        NAString queryStr = "SELECT DISTINCT HISTOGRAM_ID FROM ";
        queryStr.append(sSourceHistograms).append(".").append("SB_HISTOGRAMS")
                .append(" WHERE TABLE_UID=").append(cSourceUID);
        if (bSrcTableHasSalt && bSrcTableHasSysKey)
          {
            char cSalt[10];
            sprintf(cSalt,"%d",nSrcTableSalt);
            char cSyskey[10];
            sprintf(cSyskey,"%d",nSrcTableSyskey);
            queryStr.append(" AND HISTOGRAM_ID NOT IN ( SELECT DISTINCT HISTOGRAM_ID FROM ")
                    .append(sSourceHistograms).append(".").append("SB_HISTOGRAMS")
                    .append(" WHERE TABLE_UID=").append(cSourceUID)
                    .append(" AND (COLUMN_NUMBER=")
                    .append(cSalt)
                    .append(" OR COLUMN_NUMBER=")
                    .append(cSyskey)
                    .append(" ))");
          }
        else if (bSrcTableHasSalt)
          {
            char cSalt[10];
            sprintf(cSalt,"%d",nSrcTableSalt);
            queryStr.append(" AND HISTOGRAM_ID NOT IN ( SELECT DISTINCT HISTOGRAM_ID FROM ")
            .append(sSourceHistograms).append(".").append("SB_HISTOGRAMS")
            .append(" WHERE TABLE_UID=").append(cSourceUID)
            .append(" AND COLUMN_NUMBER=")
            .append(cSalt)
            .append(")");
          }
        else if (bSrcTableHasSysKey)
          {
            char cSyskey[10];
            sprintf(cSyskey,"%d",nSrcTableSyskey);
            queryStr.append(" AND HISTOGRAM_ID NOT IN ( SELECT DISTINCT HISTOGRAM_ID FROM ")
            .append(sSourceHistograms).append(".").append("SB_HISTOGRAMS")
            .append(" WHERE TABLE_UID=").append(cSourceUID)
            .append(" AND COLUMN_NUMBER=")
            .append(cSyskey)
            .append(")");
          }
        queryStr.append(" ORDER BY 1");
        HSCursor cursor(STMTHEAP,"SelectHistogram");
        retcode = cursor.prepareQuery(queryStr.data(),0,1);
        HSLogError(retcode);
        SQLSTMT_ID* ppStmtId = cursor.getStmt();
        SQLDESC_ID* ppOutputDesc = cursor.getOutDesc();
        retcode = cursor.open();
        HSLogError(retcode);
        while (retcode == 0)
          {
            retcode = SQL_EXEC_Fetch(ppStmtId,ppOutputDesc,1,2,&histid,NULL);
            if( 0 == retcode )
                vHistid.push_back(histid);
            else if (retcode != 100)
                HSLogError(retcode);
          }
        if ( 100 == retcode )
            retcode = 0;
        HSLogError(retcode);
    }

    //create schema trafodion._HIVESTATS_ if not exist.
    if (hs_globals_obj->isHiveCat(hs_globals_obj->objDef->getCatName()))
      {
        HSTranMan *TM = HSTranMan::Instance();
        TM->Begin("Create schema for hive stats.");
        NAString ddl = "CREATE SCHEMA IF NOT EXISTS ";
        ddl.append(HIVE_STATS_CATALOG).append('.').append(HIVE_STATS_SCHEMA).
            append(" AUTHORIZATION DB__ROOT");
        if (CmpCommon::context()->useReservedNamespace())
          ddl.append(" NAMESPACE '").append(ComGetReservedNamespace(HIVE_STATS_SCHEMA_NO_QUOTES)).append("'");

        retcode = HSFuncExecQuery(ddl, -UERR_INTERNAL_ERROR, NULL,
                                  "Creating schema for Hive statistics", NULL,
                                  NULL);
        HSHandleError(retcode);
        TM->Commit();
      }

    //create schema trafodion._HBASESTATS_ if not exist.
    if (hs_globals_obj->isNativeHbaseCat(hs_globals_obj->objDef->getCatName()))
      {
        HSTranMan *TM = HSTranMan::Instance();
        TM->Begin("Create schema for native hbase stats.");
        NAString ddl = "CREATE SCHEMA IF NOT EXISTS ";
        ddl.append(HBASE_STATS_CATALOG).append('.').append(HBASE_STATS_SCHEMA).
            append(" AUTHORIZATION DB__ROOT");
        if (CmpCommon::context()->useReservedNamespace())
          ddl.append(" NAMESPACE '").append(ComGetReservedNamespace(HBASE_STATS_SCHEMA_NO_QUOTES)).append("'");

        retcode = HSFuncExecQuery(ddl, -UERR_INTERNAL_ERROR, NULL,
                                  "Creating schema for native HBase statistics", NULL,
                                  NULL);
        HSHandleError(retcode);
        TM->Commit();
      }

    retcode = CreateHistTables(hs_globals_obj);
    HSHandleError(retcode);

    //do copy source table's histograms to target table.
    //UPSERT USING LOAD INTO CATALOG.SCHEMA.SB_HISTOGRAMS SELECT
    //targettableUID,newhistogramID,COL_POSITION,COLUMN_NUMBER,...
    //FROM CATALOG.SCHEMA.SB_HISTOGRAMS WHERE TABLE_UID=sourcetableUID
    //AND HISTOGRAM_ID=targetUID
    {
      NAString ddl = "UPSERT USING LOAD INTO ";
      ddl.append(sTargetHistograms).append(".").append("SB_HISTOGRAMS");
      ddl.append(" SELECT ");
      ddl.append(cTargetUID).append(",#1,");
      NAString sTmp("\"COL_POSITION\",\"COLUMN_NUMBER\"");
      if (bSrcTableHasSysKey && !bTarTableHasSysKey)
          sTmp.append("-1");
      else if (!bSrcTableHasSysKey && bTarTableHasSysKey)
          sTmp.append("+1");

      sTmp.append(",\"COLCOUNT\",\"INTERVAL_COUNT\","
                  "\"ROWCOUNT\",\"TOTAL_UEC\",\"STATS_TIME\",\"LOW_VALUE\",\"HIGH_VALUE\","
                  "\"READ_TIME\",\"READ_COUNT\",\"SAMPLE_SECS\",\"COL_SECS\",\"SAMPLE_PERCENT\","
                  "\"CV\",\"REASON\",\"V1\",\"V2\",\"V3\",\"V4\",\"V5\",\"V6\" FROM ");
      sTmp = ddl + sTmp;
      sTmp.append(sSourceHistograms).append(".").append("SB_HISTOGRAMS");
      sTmp.append(" WHERE \"TABLE_UID\"=").append(cSourceUID).append(" AND \"HISTOGRAM_ID\"= #2");
      if (bSrcTableHasSalt)
        {
          char cPos[10];
          sprintf(cPos,"%d",nSrcTableSalt);
          sTmp.append(" AND COLUMN_NUMBER <> ").append(cPos);
        }
      if (bSrcTableHasSysKey)
        {
          char cPos[10];
          sprintf(cPos,"%d",nSrcTableSyskey);
          sTmp.append(" AND COLUMN_NUMBER <> ").append(cPos);
        }
      NAString ddlInterval = "UPSERT USING LOAD INTO ";
      ddlInterval.append(sTargetHistograms).append(".").append("SB_HISTOGRAM_INTERVALS");
      ddlInterval.append(" SELECT ");
      ddlInterval.append(cTargetUID).append(",#1,");
      NAString sTmpInterval("\"INTERVAL_NUMBER\",\"INTERVAL_ROWCOUNT\",\"INTERVAL_UEC\","
                            "\"INTERVAL_BOUNDARY\",\"STD_DEV_OF_FREQ\",\"V1\",\"V2\","
                            "\"V3\",\"V4\",\"V5\",\"V6\" FROM ");
      sTmpInterval = ddlInterval + sTmpInterval;
      sTmpInterval.append(sSourceHistograms).append(".").append("SB_HISTOGRAM_INTERVALS");
      sTmpInterval.append(" WHERE \"TABLE_UID\"=").append(cSourceUID).append(" AND \"HISTOGRAM_ID\"= #2");

      //generate target table's histogramid
      ULng32 nNewHistId = (ULng32)(NA_JulianTimestamp() & ColStats::USTAT_HISTOGRAM_ID_THRESHOLD);

      HSTranMan *TM = HSTranMan::Instance();
      TM->Begin("GenerateStats for target table.");
      for(int i =0; i<vHistid.size(); ++i)
        {
          if ( 0 != i )
            {
              nNewHistId +=5;
            }

          //SB_HISTOGRAMS
          //replace #2 with source table histogramid
          char tmp[30];
          sprintf(tmp,"%d",vHistid[i]);
          NAString d = sTmp;
          size_t nIdx = d.index("#2");
          d.replace(nIdx,2,tmp);

          //SB_HISTOGRAM_INTERVALS
          //replace #2 with source table histogramid
          NAString d1 = sTmpInterval;
          nIdx = d1.index("#2");
          d1.replace(nIdx,2,tmp);

          //SB_HISTOGRAMS
          //replace #1 with new target histogramid
          sprintf(tmp,"%d",nNewHistId);
          nIdx = d.index("#1");
          d.replace(nIdx,2,tmp);

          //SB_HISTOGRAM_INTERVALS
          //replace #1 with new target histogramid
          nIdx = d1.index("#1");
          d1.replace(nIdx,2,tmp);

          retcode = HSFuncExecQuery(d,-UERR_INTERNAL_ERROR, NULL,
                                    "Genterate Stats for target table",
                                    NULL,NULL);
          HSHandleError(retcode);

          retcode = HSFuncExecQuery(d1,-UERR_INTERNAL_ERROR, NULL,
                                    "Genterate Stats for target table",
                                    NULL,NULL);
          HSHandleError(retcode);
        }

      TM->Commit();
    }

    //must invalidateStats, otherwise histograms will not worked for target table.
    if( 0 == retcode )
        CmpSeabaseDDL::invalidateStats(hs_globals_obj->objDef->getObjectUID());

    return retcode;
  }

static Int32 lcl_isChineseCharacter(NAWchar c)
  {
    return c >= 0x4e00 && c <= 0x9fbb;
  }

static Int32 lcl_isAlpha(NAWchar c)
  {
    return isAlpha8859_1(c);
  }

static Int32 lcl_isAlphaNum(NAWchar c)
  {
    return isAlNum8859_1(c) || c == L'_';
  }

//Search input string to find chinese character.
//If found, add double quotes.
static NAString lcl_processInputStr(const char *input)
  {
    NAString sResult;
    if (!input)
      return sResult;
    NAWString *pWStr = charToUnicode((Lng32)CharInfo::UTF8, input, strlen(input));
    if (!pWStr)
      return sResult;
    NAWString sWResult;
    size_t nLen = pWStr->length();
    size_t i = 0;
    NAWString sWQuote(L"\"");
    while ( i < nLen )
      {
        if ( lcl_isChineseCharacter((*pWStr)[i]))
          {
            int nForward = 0;
            //search forward to find whole identifier
            NABoolean bInquotes = FALSE;
            for (int n=i-1;n>0;--n)
              {
                if (lcl_isAlphaNum((*pWStr)[n]))
                  {
                    nForward++;
                    continue;
                  }
                if (sWResult[sWResult.length()-nForward-1] != sWQuote)
                  sWResult.replace(sWResult.length()-nForward,nForward,sWQuote.data(),1);
                else
                  bInquotes = TRUE;//already has double quotes

                for (int m = i-nForward; m <= i; ++m)
                  {
                    if (lcl_isAlpha((*pWStr)[m]) && !bInquotes)
                      sWResult.append(NAWString(na_towupper((*pWStr)[m])));
                    else
                      sWResult.append(NAWString((*pWStr)[m]));
                  }
                break;
              }
            //search backward to find whole identifier
            int nBackward = 0;
            for (int n=i+1;n<nLen;++n)
              {
                if (lcl_isAlphaNum((*pWStr)[n]) || lcl_isChineseCharacter((*pWStr)[n]))
                  {
                    nBackward++;
                    continue;
                  }
                for ( int m =1; m <= nBackward; ++m )
                {
                  if (lcl_isAlpha((*pWStr)[i+m]) && !bInquotes)
                    sWResult.append(NAWString(na_towupper((*pWStr)[i+m])));
                  else
                    sWResult.append(NAWString((*pWStr)[i+m]));
                }
                if((*pWStr)[i+nBackward+1] != sWQuote)
                  sWResult.append(sWQuote);
                break;
              }
             if ( nBackward<1 )
               nBackward =1;
             i = i + nBackward + 1;
          }
        else
          sWResult.append(NAWString((*pWStr)[i++]));
      }
    NAString *pStr = unicodeToChar(sWResult.data(),sWResult.length(),(Lng32)CharInfo::UTF8);
    sResult = pStr->data();
    delete pWStr;
    delete pStr;
    return sResult;
  }


Lng32 lcl_makeOnClause(const HSGlobalsClass* hs_globals_obj, NAString &onClause)
{
  if (!hs_globals_obj)
    return -1;

  if (hs_globals_obj->onClauseText)
    {
      onClause = *hs_globals_obj->onClauseText;
      return 0;
    }

  return 0;
}
//update statistics for partition table
//e.g.
//create table t1(a varchar(10) , b int, c int)
//partition by range (b)
//(
//  partition p1 values less than (10),
//  partition p2 values less than (20)
//);
//
//update statistics for table t1 on every column
//Will be split into the following statements to be executed separately:
//update statistics for table t1 partition(p1) on every column
//update statistics for table t1 partition(p2) on every column
Lng32 UpdateStats4PartitionTableV2()
{
  Lng32 retcode = 0;

  HSLogMan *LM = HSLogMan::Instance();
  if (LM->LogNeeded())
    LM->Log("\nUpdateStats4PartitionTableV2\n=====================================================================");
  HSGlobalsClass *hs_globals_obj= GetHSContext();

  NATable *pSourceTable = hs_globals_obj->objDef->getNATable();
  if (!pSourceTable || !pSourceTable->isPartitionV2Table())
    return 0;

  //UPDATE STATISTICS ..LIKE is not supported for partition v2 table.
  if ( hs_globals_obj->optFlags & GEN_STATS_LIKE)
    {    
      hs_globals_obj->diagsArea << DgSqlCode(-UERR_PAR_TABLE_NOT_SUPPORT1);
      return -1;
    }    

  //UPDATE STATISTICS ..if not exists is not supported for partition v2 table.
  if ( hs_globals_obj->optFlags & IF_NOT_EXISTS_OPT)
    {    
      hs_globals_obj->diagsArea << DgSqlCode(-UERR_PAR_TABLE_NOT_SUPPORT2);
      return -1;
    }    

  //IUS is not supported for partition v2 table.
  if (hs_globals_obj->optFlags & IUS_PERSIST || hs_globals_obj->optFlags & IUS_OPT)
    {
      hs_globals_obj->diagsArea << DgSqlCode(-UERR_PAR_TABLE_NOT_SUPPORT3);
      return -1;
    }


  NAString sUps = "UPDATE STATISTICS FOR TABLE %s ";
  if (hs_globals_obj->optFlags & CLEAR_OPT)
    {
      if (hs_globals_obj->groupCount == 0)
        sUps += " CLEAR";
      else
        {
          NAString onClause;
          lcl_makeOnClause(hs_globals_obj,onClause);
          sUps += onClause;
          sUps += " CLEAR";       
        }
      
    }
  else if (hs_globals_obj->StatsNeeded())
    {
      NAString sampleOption;
      if (hs_globals_obj->sampleOptionUsed)
        createSampleOption(hs_globals_obj->optFlags & SAMPLE_REQUESTED, hs_globals_obj->sampleTblPercent,
                           sampleOption, hs_globals_obj->sampleValue1, hs_globals_obj->sampleValue2);

      NAString onClause;
      lcl_makeOnClause(hs_globals_obj,onClause);
      sUps += onClause;

      if (sampleOption.length()>0)
        sUps += sampleOption.data();
      if (hs_globals_obj->optFlags & INTERVAL_OPT)
        {
          char sbuf[50];
          snprintf(sbuf, sizeof(sbuf), " GENERATE %d INTERVALS", hs_globals_obj->intCount);
          sUps += sbuf;
        }
    }

  if (hs_globals_obj->isPartitionV2TableHasForClause && hs_globals_obj->partitionV2ForClauseText)
  {
    LIST(NAString) * partitionForClauseTableName = NULL;
    retcode = getPartitionNameByForClause(pSourceTable, pSourceTable->getTableName().getObjectName().data(), hs_globals_obj->partitionV2ForClauseText->data(), &partitionForClauseTableName);
    if (retcode < 0)
      return retcode;

    if (partitionForClauseTableName)
    {
      for (int i = 0; i < (*partitionForClauseTableName).entries(); i++)
      {
        if (!hs_globals_obj->partitionV2NameText)
              hs_globals_obj->partitionV2NameText = new (STMTHEAP) NAString(STMTHEAP);
        *hs_globals_obj->partitionV2NameText = pSourceTable->getTableName().getCatalogName();
        *hs_globals_obj->partitionV2NameText += ".";
        *hs_globals_obj->partitionV2NameText += pSourceTable->getTableName().getSchemaName();
        *hs_globals_obj->partitionV2NameText += ".";
        *hs_globals_obj->partitionV2NameText += (*partitionForClauseTableName)[i];
        char query[sUps.length()+300];
        snprintf(query, sizeof(query), sUps.data(), hs_globals_obj->partitionV2NameText->data());
        if (LM->LogNeeded())
          {
             sprintf(LM->msg, "\n%s start\n",sUps.data());
             LM->Log(LM->msg); 
          }
        retcode = HSFuncExecQuery(query);
        if (LM->LogNeeded())
          {
             sprintf(LM->msg, "\n%s end. retcode=%d\n",sUps.data(),retcode);
             LM->Log(LM->msg);
          }
        HSExitIfError(retcode);
      }

    }
    NADELETEBASIC(hs_globals_obj->partitionV2ForClauseText, STMTHEAP);
    hs_globals_obj->partitionV2ForClauseText = NULL;
    hs_globals_obj->isPartitionV2TableHasForClause = FALSE;
    
  }
  else
  {
    for ( int i=0; i<pSourceTable->getNAPartitionArray().entries(); i++)
    {
      const char* pName = pSourceTable->getNAPartitionArray()[i]->getPartitionEntityName();
      if (pName)
        {
          if (!hs_globals_obj->partitionV2NameText)
            hs_globals_obj->partitionV2NameText = new (STMTHEAP) NAString(STMTHEAP);
          *hs_globals_obj->partitionV2NameText = pSourceTable->getTableName().getCatalogName();
          *hs_globals_obj->partitionV2NameText += ".";
          *hs_globals_obj->partitionV2NameText += pSourceTable->getTableName().getSchemaName();
          *hs_globals_obj->partitionV2NameText += ".";
          *hs_globals_obj->partitionV2NameText += pName;
          
          char query[sUps.length()+300];
          snprintf(query, sizeof(query), sUps.data(), hs_globals_obj->partitionV2NameText->data());

          if (LM->LogNeeded())
            {
               sprintf(LM->msg, "\n%s start\n",sUps.data());
               LM->Log(LM->msg); 
            }
          retcode = HSFuncExecQuery(query);

          if (LM->LogNeeded())
            {
               sprintf(LM->msg, "\n%s end. retcode=%d\n",sUps.data(),retcode);
               LM->Log(LM->msg);
            }
          HSExitIfError(retcode);
        }
    }
  }
  NADELETEBASIC(hs_globals_obj->partitionV2NameText, STMTHEAP);
  hs_globals_obj->partitionV2NameText = NULL;
   
  HSCleanUpLog();
  HSClearCLIDiagnostics();
  return retcode;
}
// -----------------------------------------------------------------------
// Entry for SHOWSTATS.
// -----------------------------------------------------------------------
Lng32 ShowStats(const char* input, char* &outBuf,
               ULng32 &outBufLen, CollHeap *heap)
  {
    WMSController wmsController;  // Control WMS CQD settings in secondary compiler.

    Lng32 retcode = 0;
    Space space;

    ComDiagsArea diags(STMTHEAP);
    HSGlobalsClass::schemaVersion = COM_VERS_UNKNOWN;

    ComDiagsArea *ptrDiags = CmpCommon::diags();
    if (!ptrDiags)
      ptrDiags = &diags;

    HSLogMan *LM = HSLogMan::Instance();
    LM->Log("\nSHOWSTATS\n=====================================================================");

    HSGlobalsClass hs_globals_obj(*ptrDiags);
    hs_globals_y = &hs_globals_obj;

    // preprocess the input for SQL expressions and partition tabel v2
    hs_input = hs_globals_obj.preprocessInput(const_cast<char *>(input));
    if (hs_input != input)
      {    
        LM->Log("Modified input after preprocessing:");
        LM->Log(hs_input);
      }    
    NAString sInput = lcl_processInputStr(hs_input);
    hs_input = (char *)sInput.data();

    NAString displayData("\n");
    
    // Do not show missing histogram warnings
    retcode = HSFuncExecQuery("CONTROL QUERY DEFAULT HIST_MISSING_STATS_WARNING_LEVEL '0'");
    HSExitIfError(retcode);
    // Turn off automation for internal queries
    retcode = HSFuncExecQuery("CONTROL QUERY DEFAULT USTAT_AUTOMATION_INTERVAL '0'");
    HSExitIfError(retcode);
    // Turn off quick stats
    retcode = HSFuncExecQuery("CONTROL QUERY DEFAULT HIST_ON_DEMAND_STATS_SIZE '0'");
    HSExitIfError(retcode);

    // Allow select * to return system added columns
    retcode = HSFuncExecQuery("CONTROL QUERY DEFAULT MV_ALLOW_SELECT_SYSTEM_ADDED_COLUMNS 'ON'");
    HSExitIfError(retcode);

    // Parse showstats statement.
    retcode = HSFuncParseStmt();
    HSExitIfError(retcode);

    retcode = HSCheckPartitionV2Table(hs_input);
    HSExitIfError(retcode);

    // check privileges
    NABoolean isShowStats = TRUE;
    if (!hs_globals_obj.isAuthorized(isShowStats))
      {
        HSFuncMergeDiags(-UERR_NO_PRIVILEGE, hs_globals_obj.user_table->data());
        retcode = -1;
        HSExitIfError(retcode);
      }

    // histogram versioning
    if (hs_globals_obj.tableFormat == SQLMX) 
      {
        HSGlobalsClass::schemaVersion = getTableSchemaVersion(*(hs_globals_obj.user_table));
        if (HSGlobalsClass::schemaVersion == COM_VERS_UNKNOWN)
        {
          HSFuncMergeDiags(-UERR_INTERNAL_ERROR, "GET_SCHEMA_VERSION");
          retcode = -1;
          HSExitIfError(retcode);
        }
      }
    if (LM->LogNeeded())
      {
        sprintf(LM->msg, "\nShowStats: TABLE: %s; SCHEMA VERSION: %d\n", 
                  hs_globals_obj.user_table->data(), 
                  HSGlobalsClass::schemaVersion);
        LM->Log(LM->msg);
      }


    if (hs_globals_obj.objDef->getNATable()->isPartitionV2Table())
      {
        retcode = hs_globals_obj.GetStatistics4PartitionTableV2(outBuf,outBufLen,heap);
      }
    else
      {
        retcode = hs_globals_obj.GetStatistics(displayData, space);
        space.allocateAndCopyToAlignedSpace(displayData, displayData.length(), sizeof(short));
        outBufLen = space.getAllocatedSpaceSize();
        outBuf = new (heap) char[outBufLen];
        space.makeContiguous(outBuf, outBufLen);
      }
    // Reset CQDs.
    retcode = HSFuncExecQuery("CONTROL QUERY DEFAULT HIST_MISSING_STATS_WARNING_LEVEL RESET");
    HSExitIfError(retcode);
    retcode = HSFuncExecQuery("CONTROL QUERY DEFAULT USTAT_AUTOMATION_INTERVAL RESET");
    HSExitIfError(retcode);
    retcode = HSFuncExecQuery("CONTROL QUERY DEFAULT HIST_ON_DEMAND_STATS_SIZE RESET");
    HSExitIfError(retcode);
    retcode = HSFuncExecQuery("CONTROL QUERY DEFAULT MV_ALLOW_SELECT_SYSTEM_ADDED_COLUMNS RESET");
    HSExitIfError(retcode);

    HSClearCLIDiagnostics();
    hs_globals_y = NULL;
    return retcode;
  }

// -----------------------------------------------------------------------
// Entry for UPDATE STATISTICS.
// -----------------------------------------------------------------------
Lng32 UpdateStats(char *input, NABoolean requestedByCompiler)
  {
    Lng32 retcode = 0;
    HSCliStatement::statementNum = 0;
    ComDiagsArea diags(STMTHEAP);
    HSGlobalsClass::schemaVersion = COM_VERS_UNKNOWN;
    HSGlobalsClass::autoInterval = 0;
    sortBuffer1 = NULL;
    sortBuffer2 = NULL;

    HSLogMan *LM = HSLogMan::Instance();
    if (LM->GetLogSetting() == HSLogMan::SYSTEM)
      LM->StartLog();  // start a log automatically for each UPDATE STATS command

    ComDiagsArea *ptrDiags = CmpCommon::diags();
    if (!ptrDiags)
      ptrDiags = &diags;

    LM->Log("\nUPDATE STATISTICS\n=====================================================================");
    LM->Log(input);
    LM->StartTimer("UpdateStats()");
    HSPrologEpilog pe("UpdateStats()");
    HSGlobalsClass hs_globals_obj(*ptrDiags);
    hs_globals_obj.requestedByCompiler = requestedByCompiler;
    hs_globals_y = &hs_globals_obj;
#ifdef _TEST_ALLOC_FAILURE
    HSColGroupStruct::allocCount = 1;  // start at 1 for each new statement
#endif


                                             /*==============================*/
                                             /*       PARSE STATEMENT        */
                                             /*==============================*/
    // preprocess the input for SQL expressions
    hs_input = hs_globals_obj.preprocessInput(input);
    if (hs_input != input)
      {
        LM->Log("Modified input after preprocessing:");
        LM->Log(hs_input);
      }
    NAString sInput = lcl_processInputStr(hs_input);
    hs_input = (char *)sInput.data();

    // Set up cnotrols (CQDs) for queries.  These queries will be run in a spawned MXCMP.
    LM->StartTimer("Setup CQDs prior to parsing");
    retcode = sendAllControls(FALSE,  // do not copyCQS
                              FALSE,  // do not sendAllCQDs
                              FALSE,  // do not sendUserCQDs
                              COM_VERS_COMPILER_VERSION); // versionOfCmplrRcvCntrlInfo
    HSExitIfError(retcode);
    retcode = HSFuncExecQuery("CONTROL QUERY DEFAULT QUERY_CACHE '0'");
    HSExitIfError(retcode);
    retcode = HSFuncExecQuery("CONTROL QUERY DEFAULT CACHE_HISTOGRAMS 'OFF'");
    HSExitIfError(retcode);
    retcode = HSFuncExecQuery("CONTROL QUERY DEFAULT USTAT_MODIFY_DEFAULT_UEC '0.05'");
    HSExitIfError(retcode);
    retcode = HSFuncExecQuery("CONTROL QUERY DEFAULT OUTPUT_DATE_FORMAT 'ANSI'");
    HSExitIfError(retcode);
    // Do not show missing histogram warnings
    retcode = HSFuncExecQuery("CONTROL QUERY DEFAULT HIST_MISSING_STATS_WARNING_LEVEL '0'");
    HSExitIfError(retcode);
    // Turn off automation for internal queries
    retcode = HSFuncExecQuery("CONTROL QUERY DEFAULT USTAT_AUTOMATION_INTERVAL '0'");
    HSExitIfError(retcode);
    // Allow select * to return system added columns
    retcode = HSFuncExecQuery("CONTROL QUERY DEFAULT MV_ALLOW_SELECT_SYSTEM_ADDED_COLUMNS 'ON'");
    HSExitIfError(retcode);
    // Turn off on demand stats (Quick stats) for internal queries.  
    retcode = HSFuncExecQuery("CONTROL QUERY DEFAULT HIST_ON_DEMAND_STATS_SIZE '0'");
    HSExitIfError(retcode);

    // If ISOLATION_LEVEL is read committed, pass this to other MXCMP.
    if (CmpCommon::getDefault(ISOLATION_LEVEL) == DF_READ_COMMITTED)
    retcode = HSFuncExecQuery("CONTROL QUERY DEFAULT ISOLATION_LEVEL 'READ COMMITTED'");
    HSExitIfError(retcode);

    // If ISOLATION_LEVEL for updates is read committed, pass this to other MXCMP.
    if (CmpCommon::getDefault(ISOLATION_LEVEL_FOR_UPDATES) == DF_READ_COMMITTED)
    retcode = HSFuncExecQuery("CONTROL QUERY DEFAULT ISOLATION_LEVEL_FOR_UPDATES 'READ COMMITTED'");
    HSExitIfError(retcode);

    // may need to query the non-audit sample table. If CQD is off the only
    // type of DML allowed on a non-audit table are sideTree inserts
    HSFuncExecQuery("CONTROL QUERY DEFAULT ALLOW_DML_ON_NONAUDITED_TABLE 'ON'");


    // allow select * to return system added columns
    retcode = HSFuncExecQuery("CONTROL QUERY DEFAULT MV_ALLOW_SELECT_SYSTEM_ADDED_COLUMNS 'ON'");
    HSExitIfError(retcode);
    
    // The next 2 defaults are used to enable nullable primary and store by
    // keys. We enable them even though the CQDs might be 'OFF' by default, 
    // because the user might have created the table when they were 'ON'.
    // They would only be needed if we choose to create a sample table; the
    // sample table would have to be created using the same CQDs. In the future,
    // perhaps CQDs should be captured at DDL time so that UPDATE STATISTICS
    // can make use of them when creating sample tables.
    
    // Allow nullable primary key on a sample table
    retcode = HSFuncExecQuery("CONTROL QUERY DEFAULT ALLOW_NULLABLE_UNIQUE_KEY_CONSTRAINT 'ON'");
    HSExitIfError(retcode);

     if (CmpCommon::getDefault(WMS_CHILD_QUERY_MONITORING) == DF_OFF)
       {
	 retcode = HSFuncExecQuery("CONTROL QUERY DEFAULT WMS_CHILD_QUERY_MONITORING 'OFF'");
	 HSExitIfError(retcode);
       }
     else
       {
	 retcode = HSFuncExecQuery("CONTROL QUERY DEFAULT WMS_CHILD_QUERY_MONITORING 'ON'");
	 HSExitIfError(retcode);
       }
     if (CmpCommon::getDefault(WMS_QUERY_MONITORING) == DF_OFF)
       {
	 retcode = HSFuncExecQuery("CONTROL QUERY DEFAULT WMS_QUERY_MONITORING 'OFF'");
	 HSExitIfError(retcode);
       }
     else
       {
	 retcode = HSFuncExecQuery("CONTROL QUERY DEFAULT WMS_QUERY_MONITORING 'ON'");
	 HSExitIfError(retcode);
       }

    // Set some CQDs to allow us to process TINYINT, BOOLEAN and LARGEINT UNSIGNED
    // columns correctly. Without these, when we read data into memory for internal
    // sort, there is a mismatch between how much space we think a column value will
    // take and how much the Executor expects, and we get buffer overruns. (See
    // JIRA TRAFODION-2131.) Someday all clients will support these datatypes and
    // these CQDs can go away.
    retcode = HSFuncExecQuery("CONTROL QUERY DEFAULT TRAF_TINYINT_RETURN_VALUES 'ON'");
    HSExitIfError(retcode);
    retcode = HSFuncExecQuery("CONTROL QUERY DEFAULT TRAF_BOOLEAN_IO 'ON'");
    HSExitIfError(retcode);
    retcode = HSFuncExecQuery("CONTROL QUERY DEFAULT TRAF_LARGEINT_UNSIGNED_IO 'ON'");
    HSExitIfError(retcode);

    // Set the following CQD to allow "_SALT_", "_DIV_" and similar system columns
    // in a sample table when the table is created using CREATE TABLE AS SELECT
    retcode = HSFuncExecQuery("CONTROL QUERY DEFAULT TRAF_ALLOW_RESERVED_COLNAMES 'ON'");
    HSExitIfError(retcode);

    // Set the following so we will see LOB columns as LOB columns and not as
    // varchars
    retcode = HSFuncExecQuery("CONTROL QUERY DEFAULT TRAF_BLOB_AS_VARCHAR 'OFF'");
    HSExitIfError(retcode);
    retcode = HSFuncExecQuery("CONTROL QUERY DEFAULT TRAF_CLOB_AS_VARCHAR 'OFF'");
    HSExitIfError(retcode);
    //set NESTED_JOINS 'ON' because it will be used in HSSample::drop()
    //otherwise, the sample table will not be dropped correctly when set NESTED_JOINS 'OFF' in metadata.
    retcode = HSFuncExecQuery("CONTROL QUERY DEFAULT NESTED_JOINS 'ON'");
    HSExitIfError(retcode);

    LM->StopTimer();

    LM->StartTimer("Parse statement");

    retcode = HSFuncParseStmt();
    LM->StopTimer();
    HSExitIfError(retcode);

    retcode = HSCheckPartitionV2Table(hs_input);
    HSExitIfError(retcode);

    if ( hs_globals_obj.optFlags & IF_NOT_EXISTS_OPT)
      {
        if ( !hs_globals_obj.singleGroup
            && !hs_globals_obj.multiGroup)
          {
            HSCleanUpLog();
            return 0;
          }
      }
    // during drop/alter col, stats on those cols are cleared. This is done by
    // an 'upd stats ... clear' command that is issued internally.
    // This command may run within the 'alter' transaction. It also doesnt
    // need to be priv checked as 'alter' priv has already been validated.
    NABoolean isInternalClear = FALSE;
    if ((Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)) &&
        (hs_globals_obj.optFlags & CLEAR_OPT))
      isInternalClear = TRUE;

    // Disallow UPDATE STATS in a user transaction.
    // But allow if internal upd stats with CLEAR option. This is issued
    // during alter column operations.
    HSTranMan *TM = HSTranMan::Instance();
    if (TM->InTransaction())
      {
        if (NOT isInternalClear)
          {
            HSFuncMergeDiags(-UERR_USER_TRANSACTION);
            retcode = -1;
            HSExitIfError(retcode);
          }
      }

                                             /*==============================*/
                                             /*    CHECK SCHEMA VERSION      */
                                             /*==============================*/
    // Also checked in AddTableName() during parse.  Check again, just in case we don't 
    // reach that code.  HISTOGRAM corruption can occur if this is not set.
    if (hs_globals_obj.tableFormat == SQLMX && HSGlobalsClass::schemaVersion == COM_VERS_UNKNOWN)
    {
      HSFuncMergeDiags(-UERR_INTERNAL_ERROR, "GET_SCHEMA_VERSION");
      retcode = -1;
      HSExitIfError(retcode);
    }

                                                /*==============================*/
                                                /* HANDLE OPTIONS WHICH DO NOT  */
                                                /* MODIFY HISTOGRAMS.           */
                                                /*==============================*/
    if (hs_globals_obj.optFlags & LOG_OPT)            /* log option requested   */
      {
        HSCleanUpLog();
        return 0;
      }

    if (hs_globals_obj.optFlags & GEN_STATS_LIKE)
      {
        return GenerateStats();
      }

                                                /*==============================*/
                                                /* VERIFY THAT THE REQUESTOR    */
                                                /* HAS NECESSARY PRIVILEGES.    */
                                                /*==============================*/

    NABoolean isShowStats = FALSE;
    if ((NOT isInternalClear) &&
        (!hs_globals_obj.isAuthorized(isShowStats)))
      {
        HSFuncMergeDiags(-UERR_NO_PRIVILEGE, hs_globals_obj.user_table->data());
        retcode = -1;
        HSExitIfError(retcode);
      }


     // for ustat - we selectively allow sending specific external CQDs to the
     // secondary compilers instead of allowing all external CQDS to be sent so
     // to not affect the performance of the dynamic statements executed by ustat
     NADefaults &defs = CmpCommon::context()->schemaDB_->getDefaults();
     char* allowedCqds = (char*)defs.getValue(USTAT_CQDS_ALLOWED_FOR_SPAWNED_COMPILERS);
     Int32 allowedCqdsSize = 0;
     if (allowedCqds)
     {
        allowedCqdsSize = strlen(allowedCqds);
     }

     if (allowedCqdsSize && (allowedCqdsSize < USTAT_CQDS_ALLOWED_FOR_SPAWNED_COMPILERS_MAX_SIZE))
     {
        if (LM->LogNeeded())
        {
           sprintf(LM->msg, "\nUSTAT_CQDS_ALLOWED_FOR_SPAWNED_COMPILERS is not empty, and its value is (%s)", allowedCqds);
           LM->Log(LM->msg);
        }

        char* filterString = new (STMTHEAP) char[allowedCqdsSize+1];
        // We need to make a copy of the CQD value here since strtok
        // overwrites delims with nulls in stored cqd value.
        strcpy(filterString, allowedCqds);
        char* name = strtok(filterString, ",");

        while (name)
        {
           if (LM->LogNeeded())
           {
              sprintf(LM->msg, "\n\tCQD name: (%s)", name);
              LM->Log(LM->msg);
           }

           DefaultConstants attrEnum = defs.lookupAttrName(name, -1);
           char* value = (char*)defs.getValue(attrEnum);

           if (value)
           {
             NAString quotedString;
             ToQuotedString (quotedString, value);
             char buf[strlen(name)+quotedString.length()+4+1+1+1];  // room for "CQD %s %s;" and null terminator
             sprintf(buf, "CQD %s %s;", name, quotedString.data());
             retcode = HSFuncExecQuery(buf);

             HSExitIfError(retcode);
           }

           name = strtok(NULL, ",");
        }

        NADELETEBASIC(filterString, STMTHEAP);
     }
     else // size is zero or too large
     {
        if (LM->LogNeeded())
        {
           sprintf(LM->msg, "\nUSTAT_CQDS_ALLOWED_FOR_SPAWNED_COMPILERS size of (%d) is not acceptable", allowedCqdsSize);
           LM->Log(LM->msg);
        }
     }

    // obtain a distributed lock on the table (to prevent concurrent UPDATE STATISTICS
    // operatons on the same table)
    HSLongHeldLockController distributedLock(hs_globals_obj.user_table->data(),input,STMTHEAP);
    if (!distributedLock.lockHeld())
      {
        // We could not get the lock. Unless we are told to ignore this situation,
        // report an error and exit.
        if (CmpCommon::getDefault(USTAT_REQUIRE_DISTRIBUTED_LOCK) == DF_ON)
          {
            if (distributedLock.lockMethodFailure()) // if an exception was thrown
              {
                HSFuncMergeDiags(-UERR_LOCK_METHOD_FAILURE, getSqlJniErrorStr());
                retcode = -1;
                HSExitIfError(retcode);
              }
            else // must be someone else holds the lock
              {
                HSFuncMergeDiags(-UERR_CANT_LOCK, distributedLock.lockData());
                retcode = -1;
                HSExitIfError(retcode);
              }
          }
      }

    if (hs_globals_obj.optFlags & CREATE_SAMPLE_OPT ||/* create sample requested*/
        hs_globals_obj.optFlags & REMOVE_SAMPLE_OPT)  /* delete sample requested*/
    {
      retcode =  managePersistentSamples();
      HSCleanUpLog();
      HSClearCLIDiagnostics();
      return retcode;
    }

    LM->StartTimer("Initialize environment");
    retcode = hs_globals_obj.Initialize();      /* Initialize structures.       */
    LM->StopTimer();
    HSExitIfError(retcode);

    if (hs_globals_obj.optFlags & VIEWONLY_OPT)
    {
      HSCleanUpLog();
      return 0;
    }

    if (hs_globals_obj.objDef->getNATable()->isPartitionV2Table())
      {
        return UpdateStats4PartitionTableV2();
      }

                                                /*==============================*/
                                                /*      COLLECT STATISTICS      */
                                                /*==============================*/
    // If an elapsed-time threshold for activating logging is defined for the
    // source table, set it, unless logging is already on.
    if (!LM->LogNeeded())
      {
        hs_globals_obj.setJitLogThreshold();
        if (hs_globals_obj.getJitLogThreshold() > 0)
          hs_globals_obj.setStmtStartTime(hs_getEpochTime());
      }

    if (hs_globals_obj.StatsNeeded())
      {
        ULng32 savedParserFlags = Get_SqlParser_Flags (0xFFFFFFFF);
        Set_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL);
        retcode = hs_globals_obj.CollectStatistics();
        Assign_SqlParser_Flags (savedParserFlags);
        hs_globals_obj.resetCQDs();
        HSExitIfError(retcode);
      }
    else if (hs_globals_obj.optFlags & IUS_PERSIST)
      {
        // The user asked for a persistent sample, but the table is empty
        // so we didn't create one. Tell the user that.
        HSFuncMergeDiags(UERR_WARNING_NO_SAMPLE_TABLE_CREATED);
      }

    // do not care about warning messages now  
    retcode = HSFuncExecQuery("CONTROL QUERY DEFAULT HIST_MISSING_STATS_WARNING_LEVEL RESET");
    HSExitIfError(retcode);

    retcode = HSFuncExecQuery("CONTROL QUERY DEFAULT USTAT_AUTOMATION_INTERVAL RESET");
    HSExitIfError(retcode);

    // don't need to select system added columns anymore
    retcode = HSFuncExecQuery("CONTROL QUERY DEFAULT MV_ALLOW_SELECT_SYSTEM_ADDED_COLUMNS RESET");

    // reset the no dml on non-audit table CQD
    HSFuncExecQuery("CONTROL QUERY DEFAULT ALLOW_DML_ON_NONAUDITED_TABLE RESET");

                                             /*==============================*/
                                             /*    FLUSH/WRITE STATISTICS    */
                                             /*==============================*/
    NABoolean statsWritten = FALSE;
    LM->StartTimer("Flush out stats");
    ULng32 savedParserFlags = Get_SqlParser_Flags(0xFFFFFFFF);
    Set_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL);
    retcode = hs_globals_obj.FlushStatistics(statsWritten);
    Assign_SqlParser_Flags (savedParserFlags);
    LM->StopTimer();
    HSExitIfError(retcode);

    // Clear this after flush statistics.  FILE_STATS requires this.
    retcode = HSFuncExecQuery("CONTROL QUERY DEFAULT ISOLATION_LEVEL RESET");
    HSExitIfError(retcode);

    retcode = HSFuncExecQuery("CONTROL QUERY DEFAULT ISOLATION_LEVEL_FOR_UPDATES RESET");
    HSExitIfError(retcode);

    // Reset insert/delete/update counters if NECESSARY, EVERY or EXISTING was used.
    if (statsWritten && 
        ((hs_globals_obj.optFlags & NECESSARY_OPT && !hs_globals_obj.allMissingStats) ||
         (hs_globals_obj.optFlags & EVERYCOL_OPT) || 
         (hs_globals_obj.optFlags & EXISTING_OPT) )) 
      {
        // Reset the row counts if the stats were written successfully.
        LM->Log("Resetting ins/upd/del DP2 counts.");
        hs_globals_obj.objDef->resetRowCounts();
      }


    // Reset the quick stats setting.
    retcode = HSFuncExecQuery("CONTROL QUERY DEFAULT HIST_ON_DEMAND_STATS_SIZE RESET");
    HSExitIfError(retcode);

    // generate and update stored packed stats, if enabled.
    // If error during store, ignore and continue.
    if (hs_globals_obj.isHbaseTable &&
        ((CmpCommon::getDefault(TRAF_USTAT_GENERATE_STORED_STATS) == DF_ON) ||
         (hs_globals_obj.objDef->getNATable() &&
          hs_globals_obj.objDef->getNATable()->isStoredDesc())))
      {
        ComObjectName tableName(hs_globals_obj.user_table->data());
        if (hs_globals_obj.isTrafodionCatalog
            (tableName.getCatalogNamePartAsAnsiString()))
          {
            //We do not want to cause ERROR8738 in UpdateStats,
            //so we use "generate stored stats internal" instead of "generate stored stats" here.
            NAString sps = "alter table " + *hs_globals_obj.user_table 
              + " generate stored stats internal";
            retcode = HSFuncExecQuery(sps.data());
            if (retcode)
              {
                HSClearCLIDiagnostics();
                retcode = 0;
              }
          }
      }

    // set Update Stats time on successful completion of update statistics
    TimeVal currTime;
    GETTIMEOFDAY(&currTime, 0); 
    Int64 lastUpdateTime = currTime.tv_sec;
    HistogramsCacheEntry::setUpdateStatsTime(lastUpdateTime);    // for use by the optimizer

    HSClearCLIDiagnostics();

    hs_globals_y = NULL;

    // Reset CQDs set above; ignore errors
    HSFuncExecQuery("CONTROL QUERY DEFAULT TRAF_BLOB_AS_VARCHAR RESET");
    HSFuncExecQuery("CONTROL QUERY DEFAULT TRAF_CLOB_AS_VARCHAR RESET");
    HSFuncExecQuery("CONTROL QUERY DEFAULT NESTED_JOINS RESET");
    LM->StopTimer();

    HSCleanUpLog();

    return retcode;
  }

