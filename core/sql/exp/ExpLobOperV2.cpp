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
#include <iostream>
using std::cerr;
using std::endl;

#include <fstream>
using std::ofstream;

#include "Platform.h"
#include "SQLCLIdev.h"
#include "Context.h"
#include "str.h"
#include "ExpLOBinterface.h"
#include "ExpLOBaccess.h"
#include "ExpLobOperV2.h"
#include "ExpLOB.h"
#include "ex_globals.h"
#include "Cli.h"
#include "exp_clause.h"

//static THREAD_P Int64 delCount = 0;

const char* const ExpLobOperV2::qTypeStr[] =
  {
    "LOB_DML_INSERT",
    "LOB_DML_INSERT_APPEND",
    "LOB_DML_UPDATE",
    "LOB_DML_UPDATE_APPEND",
    "LOB_DML_DELETE",
    "LOB_DML_SELECT_OPEN",
    "LOB_DML_SELECT_FETCH",
    "LOB_DML_SELECT_FETCH_AND_DONE",
    "LOB_DML_SELECT_CLOSE",
    "LOB_DML_SELECT_ALL",
    "LOB_DML_SELECT_LOBLENGTH"
  };

Lng32 ExpLobOperV2::ddlInterface
(
     /*IN*/     Int64  objectUID,
     /*IN*/     DDLArgs &args
 )
{
  Lng32 cliRC = 0;
  ExLobGlobals *exLobGlob = NULL;
  CliGlobals *cliGlobals = GetCliGlobals();
  ContextCli   & currContext = *(cliGlobals->currContext());
  ComDiagsArea & diags       = currContext.diags();

  NABoolean useLibHdfs = FALSE;

  ComDiagsArea * myDiags = NULL;
  char logBuf[4096];
  ExeCliInterface *cliInterface = NULL;
  cliInterface = new (currContext.exHeap()) 
    ExeCliInterface(currContext.exHeap(),
		    SQLCHARSETCODE_UTF8,
		    &currContext,
		    NULL);

  char lobChunksTableNameBuf[1024];
  str_sprintf(lobChunksTableNameBuf, "%s.%s%020ld", 
              args.schName, LOB_CHUNKS_V2_PREFIX, objectUID);

  strcpy(args.errorInfo, "");

  char query[4000];

  Int32 rc = 0;

  switch (args.qType)
    {
    case LOB_DDL_CREATE:
    case LOB_DDL_ALTER :
      {
        char ns[2000];
        strcpy(ns, " attribute stored descriptor, hbase format ");

        if (args.replication == COM_REPL_SYNC)
          strcat(ns, " ,synchronous replication ");
        else if (args.replication == COM_REPL_ASYNC)
          strcat(ns, " ,asynchronous replication ");

        if (args.nameSpace)
          {
            strcat(ns, " , namespace '");
            strcat(ns, args.nameSpace);
            strcat(ns, "' ");
          }

        //Initialize LOB interface 
        exLobGlob = ExpLOBoper::initLOBglobal(
             currContext.exHeap(), &currContext, useLibHdfs, FALSE, 0, FALSE);
        if (exLobGlob == NULL) 
          {
            cliRC = -1;
            ComDiagsArea * da = &diags;
            ExRaiseSqlError(currContext.exHeap(), &da, 
			    (ExeErrorCode)(EXE_ERROR_FROM_LOB_INTERFACE), NULL, &cliRC    , 
			    &rc, NULL, (char*)"ExpLOBoper::initLOBglobal",
		            getLobErrStr(rc), (char*)getSqlJniErrorStr());
            goto non_cli_error_return;
          }

        if (args.lobParallelDDL == 1)
          {
            cliRC = cliInterface->holdAndSetCQD("traf_hbase_drop_create", "2");
            if (cliRC < 0)
              {
                goto error_return;
              }
          }

        cliRC = cliInterface->holdAndSetCQD("traf_create_table_with_uid", "");
        if (cliRC < 0)
          {
            goto error_return;
          }

        str_sprintf(query, "create ghost table if not exists %s (lobuid largeint not null, lobseq largeint not null, lobnum int unsigned not null, chunknum int unsigned not null, chunklen largeint, hdfs_offset largeint, hdfs_offset_2 largeint, flags largeint, stringparam varchar(1000), data_in_hbase varbinary(%d)) salt using %d partitions on (lobuid) primary key (lobuid, lobseq, lobnum, chunknum) %s ",
		    lobChunksTableNameBuf, 
                    args.dataInHbaseColLen,
                     (args.numSaltPartns > 1 ? args.numSaltPartns : 4),
                    ns);

	// set parserflags to allow ghost table
	currContext.setSqlParserFlags(0x1);
        cliRC = cliInterface->executeImmediate(query);
	currContext.resetSqlParserFlags(0x1);

        cliInterface->restoreCQD("traf_create_table_with_uid");
        if (args.lobParallelDDL == 1)
          cliInterface->restoreCQD("traf_hbase_drop_create");

        if (cliRC < 0)
          {
            goto error_return;
          }

        if (args.numLOBdatafiles > 0)
          {
            for (Lng32 i = 0; i < *args.numLOBs; i++)
              {
                // create lob hdfs data tables
                rc = ExpLOBoper::createLOB
                  (exLobGlob, &currContext,
                   args.lobLocList[i], args.lobHdfsPort, args.lobHdfsServer,
                   objectUID, args.lobNumList[i], args.lobMaxSize,
                   ((args.lobTypList[i] == Lob_External) ? -1 : args.numLOBdatafiles));
                
                if (rc)
                  {
                    cliRC = -1;
                    ComDiagsArea * da = &diags;
                    ExRaiseSqlError(currContext.exHeap(), &da, 
                                    (ExeErrorCode)(EXE_ERROR_FROM_LOB_INTERFACE), NULL, &cliRC    , 
                                    &rc, NULL, (char*)"ExpLOBoper::createLOB",
                                    getLobErrStr(rc), (char*)getSqlJniErrorStr());
                    goto non_cli_error_return;
                  }
                
              } // for
          } // if
      }
      break;

    case LOB_DDL_DROP:
      {
	str_sprintf(query, "drop ghost table if exists %s",
		    lobChunksTableNameBuf);

        if (args.lobParallelDDL == 1)
          {
            cliRC = cliInterface->holdAndSetCQD("traf_hbase_drop_create", "2");
            if (cliRC < 0)
              {
                goto error_return;
              }
          }

	// set parserflags to allow ghost table
	currContext.setSqlParserFlags(0x1);
	cliRC = cliInterface->executeImmediate(query);
	currContext.resetSqlParserFlags(0x1);

        if (args.lobParallelDDL == 1)
          cliInterface->restoreCQD("traf_hbase_drop_create");

	if (cliRC < 0)
          {
            if (cliRC == -4254) // invalid state
              {
                // cleanup
                str_sprintf(query, "cleanup table %s",
		    lobChunksTableNameBuf);
                
                currContext.setSqlParserFlags(0x1);
                cliRC = cliInterface->executeImmediate(query);
                currContext.resetSqlParserFlags(0x1);
              }

            if (cliRC < 0)
              {
                strcpy(args.errorInfo, "Error while dropping chunks table ");
                strcat(args.errorInfo, lobChunksTableNameBuf);
                
                goto error_return;
              }
          } // cliRC < -
	
        exLobGlob = ExpLOBoper::initLOBglobal(
             currContext.exHeap(), &currContext, useLibHdfs, FALSE, 0, FALSE);
        if (exLobGlob == NULL) 
          {
            cliRC = -1;
            ComDiagsArea * da = &diags;
            ExRaiseSqlError(currContext.exHeap(), &da, 
			    (ExeErrorCode)(EXE_ERROR_FROM_LOB_INTERFACE), NULL, &cliRC    , 
			    &rc, NULL, (char*)"ExpLOBoper::initLOBglobal ",
		            getLobErrStr(rc), (char*)getSqlJniErrorStr());
            goto non_cli_error_return;
	      
          }

        if (args.numLOBdatafiles > 0)
          {
            for (Lng32 i = 0; i < *args.numLOBs; i++)
              {
                // drop lob hdfs data tables
                rc = ExpLOBoper::dropLOB
                  (exLobGlob,&currContext,
                   args.lobLocList[i], args.lobHdfsPort, args.lobHdfsServer,
                   objectUID, args.lobNumList[i],
                   args.numLOBdatafiles);
                if (rc && (rc != -LOB_DATA_FILE_DELETE_ERROR))
                  {
                    cliRC = -1;
                    ComDiagsArea * da = &diags;
                    ExRaiseSqlError(currContext.exHeap(), &da, 
                                    (ExeErrorCode)(EXE_ERROR_FROM_LOB_INTERFACE), NULL, &cliRC    , 
                                    &rc, NULL, (char*)"ExpLOBoper::dropLOB ",
                                    getLobErrStr(rc), (char*)getSqlJniErrorStr());
                    goto non_cli_error_return;
                  }
              }//for
          } // if
      }
      break;

    case LOB_DDL_PURGEDATA:
      {
	str_sprintf(query, "truncate table %s",
		    lobChunksTableNameBuf);

	// set parserflags to allow ghost table
	currContext.setSqlParserFlags(0x1);
	cliRC = cliInterface->executeImmediate(query);
	currContext.resetSqlParserFlags(0x1);
	if (cliRC < 0)
           goto error_return;
       
        exLobGlob = ExpLOBoper::initLOBglobal(
             currContext.exHeap(), &currContext, useLibHdfs, FALSE, 0, FALSE);
        if (exLobGlob == NULL) 
          {
            cliRC = -1;
            ComDiagsArea * da = &diags;
            ExRaiseSqlError(currContext.exHeap(), &da, 
			    (ExeErrorCode)(EXE_ERROR_FROM_LOB_INTERFACE), NULL, &cliRC    , 
			    &rc, NULL, (char*)"ExpLOBoper::initLOBglobal ",
		            getLobErrStr(rc), (char*)getSqlJniErrorStr());
            goto non_cli_error_return;
          }

        if (args.numLOBdatafiles > 0)
          {
            for (Lng32 i = 0; i < *args.numLOBs; i++)
              {
                rc = ExpLOBoper::purgedataLOB
                  (exLobGlob,
                   args.lobLocList[i],
                   objectUID, args.lobNumList[i],
                   args.numLOBdatafiles);
                
                if (rc && (rc != -LOB_DATA_FILE_DELETE_ERROR))
                  {
                    cliRC = -1;
                    ComDiagsArea * da = &diags;
                    ExRaiseSqlError(currContext.exHeap(), &da, 
                                    (ExeErrorCode)(EXE_ERROR_FROM_LOB_INTERFACE), NULL, &cliRC    , 
                                    &rc, NULL, (char*)"ExpLOBoper::purgedataLOB ",
                                    getLobErrStr(rc), (char*)getSqlJniErrorStr());
                    goto non_cli_error_return;
                  }
              }//for
          } // if
      }
      break;

    case LOB_DDL_CLEANUP:
      {
	str_sprintf(query, "cleanup table %s",
		    lobChunksTableNameBuf);

	// set parserflags to allow ghost table
	currContext.setSqlParserFlags(0x1);
	cliRC = cliInterface->executeImmediate(query);
	currContext.resetSqlParserFlags(0x1);
	if (cliRC < 0)
           goto error_return;

	//Initialize LOB interface 
        exLobGlob = ExpLOBoper::initLOBglobal(
             currContext.exHeap(), &currContext, useLibHdfs, FALSE, 0, FALSE);
        if (exLobGlob == NULL) 
          {
            cliRC = -1;
            ComDiagsArea * da = &diags;
            ExRaiseSqlError(currContext.exHeap(), &da, 
			    (ExeErrorCode)(EXE_ERROR_FROM_LOB_INTERFACE), NULL, &cliRC    , 
                            &rc, NULL, (char*)"ExpLOBoper::initLOBglobal",
		            getLobErrStr(rc), (char*)getSqlJniErrorStr());
            goto non_cli_error_return;	      
          }

	for (Lng32 i = 0; i < *args.numLOBs; i++)
	  {
	    Lng32 rc = ExpLOBoper::dropLOB
	      (exLobGlob,&currContext,
	       args.lobLocList[i], args.lobHdfsPort, args.lobHdfsServer,
	       objectUID, args.lobNumList[i],
               args.numLOBdatafiles);
	    
	    if (rc && rc != -LOB_DATA_FILE_DELETE_ERROR)
	      {
		cliRC = -1;
		ComDiagsArea * da = &diags;
		ExRaiseSqlError(currContext.exHeap(), &da, 
			    (ExeErrorCode)(EXE_ERROR_FROM_LOB_INTERFACE), NULL, &cliRC    , 
			    &rc, NULL, (char*)"ExpLOBoper::dropLOB",
		            getLobErrStr(rc), (char*)getSqlJniErrorStr());
		goto non_cli_error_return;
	      }
	  } // for
      }
      break;

    case LOB_DDL_LOBMD_SELECT:
       {
         str_sprintf(query, "select lobnum, storagetype, location, column_name, inlined_data_maxlen, hbase_data_maxlen, lob_chunk_maxlen, num_lob_files from %s.\"%s\".%s where table_uid = %ld order by lobnum for read uncommitted access",
                     TRAFODION_SYSTEM_CATALOG, SEABASE_MD_SCHEMA, SEABASE_LOB_COLUMNS,
                     objectUID);
         
         cliRC = cliInterface->fetchRowsPrologue(query);
         if (cliRC < 0)
           goto error_return;
         
	cliRC = 0;
	Lng32 j = 0;
	*args.numLOBs = 0;
	while (cliRC != 100)
	  {
	    cliRC = cliInterface->fetch();
	    if (cliRC < 0)
	      {
		cliInterface->fetchRowsEpilogue(0);
		
		goto error_return;
	      }
	    
	    if (cliRC != 100)
	      {
		char * ptr;
		Lng32 len;
		
		short lobNum;
		cliInterface->getPtrAndLen(1, ptr, len);
		str_cpy_all((char*)&lobNum, ptr, len);
		args.lobNumList[j] = lobNum;

		short stType;
		cliInterface->getPtrAndLen(2, ptr, len);
		str_cpy_all((char*)&stType, ptr, len);
		args.lobTypList[j] = stType;

		cliInterface->getPtrAndLen(3, ptr, len);
		str_cpy_and_null(args.lobLocList[j], ptr, len, '\0', ' ', TRUE);
	
                cliInterface->getPtrAndLen(4, ptr, len);
		str_cpy_and_null(args.lobColNameList[j], ptr, len, '\0', ' ', TRUE);

		Int32 inlineMaxLen;
		cliInterface->getPtrAndLen(5, ptr, len);
		str_cpy_all((char*)&inlineMaxLen, ptr, len);
		args.lobInlinedDataMaxLenList[j] = inlineMaxLen;

		Int64 hbaseMaxLen;
		cliInterface->getPtrAndLen(6, ptr, len);
		str_cpy_all((char*)&hbaseMaxLen, ptr, len);
		args.lobHbaseDataMaxLenList[j] = hbaseMaxLen;

                Int32 lobChunkMaxLen;
		cliInterface->getPtrAndLen(7, ptr, len);
		str_cpy_all((char*)&lobChunkMaxLen, ptr, len);
		args.lobChunkMaxLen = lobChunkMaxLen;

                Int32 numLobFiles;
		cliInterface->getPtrAndLen(8, ptr, len);
		str_cpy_all((char*)&numLobFiles, ptr, len);
		args.numLOBdatafiles = numLobFiles;
                
		j++;
	      }
	    else
	      {
		Lng32 ccliRC = cliInterface->fetchRowsEpilogue(0);
		if (ccliRC < 0)
		  {
		    cliRC = ccliRC;
		    goto error_return;
		  }
	      }
	  } // while

	if (j > 0)
	  cliRC = 0;
	else
	  cliRC = 100;
	
        *args.numLOBs = j;
      }
      break;
    } // switch

 error_return:
  if (cliRC < 0)
    {
      cliInterface->allocAndRetrieveSQLDiagnostics(myDiags);
      diags.mergeAfter(*myDiags);
      myDiags->decrRefCount();
    }
 non_cli_error_return:
  if (exLobGlob != NULL)
     ExpLOBoper::deleteLOBglobal(exLobGlob, currContext.exHeap());

  delete cliInterface;
  if (cliRC < 0)
     return cliRC;
  else if (cliRC == 100)
    return 100;
  else
    return 0;
}

Lng32 ExpLobOperV2::restoreCQDs(Lng32 cliRC)
{
  cliInterface6_->restoreCQD("attempt_esp_parallelism");
  cliInterface6_->restoreCQD("traf_no_dtm_xn");
  cliInterface6_->restoreCQD("hbase_check_and_updel_opt");
  cliInterface6_->restoreCQD("auto_adjust_cacherows");
  cliInterface6_->restoreCQD("hbase_cache_blocks");
  cliInterface6_->restoreCQD("upd_ordered");
  cliInterface6_->executeImmediate("control query shape cut");

  return cliRC;
}

Lng32 ExpLobOperV2::insertChunkInHbase(char *lobChunksTableName,
                                       Int64 *lobUID, Int32 lobNum,
                                       Int64 inDataLen, char *inDataPtr,
                                       Int32 startChunkNum, 
                                       Int32 &endChunkNum,
                                       Int64 &insertedDataLen,
                                       Int64 hdfsOffset,
                                       DMLOptions &options,
                                       NABoolean forUpdate,
                                       ContextCli &currContext)
{
  ComDiagsArea & diags       = currContext.diags();

  Lng32 cliRC = 0;

  char queryIns[4000];
  char queryUpd[4000];

  Int64 transID = getTransactionIDFromContext();

  NABoolean nonXnOper = FALSE;
  NABoolean autoCommit = FALSE;
  NABoolean evalUsingSingletonInsert = FALSE;

  // if this query ie being run for the first time, prepare it.
  NABoolean wasPrep = FALSE;
  if (NOT cliInterface2_->isAllocated())
    {
      if (transID <= 0)
        nonXnOper = TRUE;

      if (GetCliGlobals()->currContext()->getTransaction()->autoCommit())
        autoCommit = TRUE;

      if (nonXnOper || autoCommit || options.lobXnOper())
        evalUsingSingletonInsert = TRUE;

      cliRC = cliInterface6_->holdAndSetCQDs("attempt_esp_parallelism ", "attempt_esp_parallelism 'OFF' ");

      cliRC = cliInterface6_->executeImmediate("control query shape cut");
      if (cliRC < 0)
        return restoreCQDs(cliRC);
      
      if (evalUsingSingletonInsert)
        {
          if (nonXnOper)
            {
              cliRC = cliInterface6_->holdAndSetCQD("traf_no_dtm_xn", "ON");
              if (cliRC < 0)
                return restoreCQDs(cliRC);
            }

          str_sprintf(queryIns, "insert into table(ghost table %s) values (cast(? as largeint not null), cast(? as largeint not null), cast(? as int unsigned not null), cast(? as int unsigned not null), cast(? as largeint), cast(? as largeint), -1, 0, '%s', cast(? as varbinary(%d)))",
                      lobChunksTableName,
                      "",
                      options.lobDataInHbaseColLen);
        }
      else
        {
          str_sprintf(queryIns, "select \"_SALT_\" from (insert into table(ghost table %s) (lobuid, lobseq, lobnum, chunknum, chunklen, hdfs_offset, hdfs_offset_2, flags, stringparam) values (cast(? as largeint not null), cast(? as largeint not null), cast(? as int unsigned not null), cast(? as int unsigned not null), cast(? as largeint), cast(? as largeint), -1, 0, '%s')) x",
                      lobChunksTableName,
                      "");
        }

      // set parserflags to allow ghost table
      currContext.setSqlParserFlags(0x1);
      cliRC = cliInterface2_->executeImmediatePrepare(queryIns);
      currContext.resetSqlParserFlags(0x1);
      if (cliRC < 0)
        return restoreCQDs(cliRC);

      if (NOT evalUsingSingletonInsert)
        {
          if (NOT forUpdate)
            {
              cliRC = cliInterface6_->holdAndSetCQD("traf_no_dtm_xn", "ON");
              if (cliRC < 0)
                return restoreCQDs(cliRC);
            }

          cliRC = cliInterface6_->holdAndSetCQD("hbase_check_and_updel_opt", "ON");
          if (cliRC < 0)
            return restoreCQDs(cliRC);
          
          str_sprintf(queryUpd, "upsert into table(ghost table %s) (\"_SALT_\", lobuid, lobseq, lobnum, chunknum, data_in_hbase) values (cast(? as int unsigned not null), cast(? as largeint not null), cast(? as largeint not null), cast(? as int unsigned not null), cast(? as int unsigned not null), cast(? as varbinary(%d)) )",
                      lobChunksTableName,
                      options.lobDataInHbaseColLen);
          
          // set parserflags to allow ghost table
          currContext.setSqlParserFlags(0x1);
          cliRC = cliInterface3_->executeImmediatePrepare(queryUpd);
          currContext.resetSqlParserFlags(0x1);
          if (cliRC < 0)
            return restoreCQDs(cliRC);
        }

      restoreCQDs(0);

      wasPrep = TRUE;
    }

  if (NOT cliInterface3_->isAllocated())
    evalUsingSingletonInsert = TRUE;
  
  Lng32 dummy;

  // location of params to be passed to insert statement
  Int32 insLobUIDOffset = 0;
  Int32 insLobSeqOffset = 0;
  Int32 insLobNumOffset = 0;
  Lng32 insChunkNumOffset = 0;
  Int32 insChunkLenOffset = 0;
  Int32 insChunkLenIndOffset = 0;
  Int32 insHdfsOffsetOffset = 0;
  Int32 insHdfsOffsetIndOffset = 0;
  Lng32 insVcIndLen = 0;
  Lng32 insVarOffset = 0;
  Lng32 insIndOffset = 0;

  cliInterface2_->getAttributes(1, TRUE,
                                dummy, dummy, dummy, NULL, &insLobUIDOffset);
  cliInterface2_->getAttributes(2, TRUE,
                                dummy, dummy, dummy, NULL, &insLobSeqOffset);
  cliInterface2_->getAttributes(3, TRUE,
                                dummy, dummy, dummy, NULL, &insLobNumOffset);
  cliInterface2_->getAttributes(4, TRUE,
                                dummy, dummy, dummy, NULL, &insChunkNumOffset);
  cliInterface2_->getAttributes(5, TRUE,
                                dummy, dummy, dummy, &insChunkLenIndOffset, &insChunkLenOffset);
  // offset to where hdfsOffset will be added in the input row.
  cliInterface2_->getAttributes(6, TRUE,
                                dummy, dummy, dummy, &insHdfsOffsetIndOffset, &insHdfsOffsetOffset);

  if (evalUsingSingletonInsert)
    cliInterface2_->getAttributes(7, TRUE,
                                  dummy, dummy, insVcIndLen, &insIndOffset, &insVarOffset);

  Lng32 updVcIndLen = 0;
  Lng32 updVarOffset = 0;
  Lng32 updIndOffset = 0;
  Int32 updSaltOffset = 0;
  Int32 updLobUIDOffset = 0;
  Int32 updLobSeqOffset = 0;
  Int32 updLobNumOffset = 0;
  Lng32 updChunkNumOffset = 0;
  Int32 updChunkLenOffset = 0;
  Int32 updHdfsOffsetOffset = 0;

  if (NOT evalUsingSingletonInsert) 
    {
      // location of params to be passed to update statement
      cliInterface3_->getAttributes(1, TRUE,
                                    dummy, dummy, dummy, NULL, &updSaltOffset);
      cliInterface3_->getAttributes(2, TRUE,
                                    dummy, dummy, dummy, NULL, &updLobUIDOffset);
      cliInterface3_->getAttributes(3, TRUE,
                                    dummy, dummy, dummy, NULL, &updLobSeqOffset);
      cliInterface3_->getAttributes(4, TRUE,
                                    dummy, dummy, dummy, NULL, &updLobNumOffset);
      cliInterface3_->getAttributes(5, TRUE,
                                    dummy, dummy, dummy, NULL, &updChunkNumOffset);
      cliInterface3_->getAttributes(6, TRUE,
                                    dummy, dummy, updVcIndLen, &updIndOffset, &updVarOffset);
    }

  // find out how many chunks need to be inserted
  Int64 dataLenToInsert = 0;
  Int32 numChunks = 0;
  if ((inDataLen == 0) && (hdfsOffset == -1))// empty blob/clob
    {
      dataLenToInsert = 0;
      numChunks = 0;
    }
  else if (hdfsOffset > -1)
    {
      // error. Must have input data len
      assert(inDataLen > 0);

      numChunks = 1;
      dataLenToInsert = inDataLen;
    }
  else
    {
      dataLenToInsert = MINOF(inDataLen, options.lobHbaseDataMaxLen);
      numChunks = (dataLenToInsert <= 0 ? 0 : ((dataLenToInsert-1) / options.lobDataInHbaseColLen) + 1);
    }

  Int16 nullV = 0;
  char *dataPtr = inDataPtr;
  Int32 chunkNum = startChunkNum;
  insertedDataLen = 0;
  for (chunkNum = startChunkNum; chunkNum < (startChunkNum+numChunks); chunkNum++)
    {
      Int64 chunkLen = 0;
      if (hdfsOffset == -1)
        chunkLen = MINOF(dataLenToInsert, options.lobDataInHbaseColLen);
      else 
        chunkLen = dataLenToInsert;

      memcpy(&cliInterface2_->inputBuf()[insLobUIDOffset], (char*)&lobUID[0], sizeof(Int64));
      memcpy(&cliInterface2_->inputBuf()[insLobSeqOffset], (char*)&lobUID[1], sizeof(Int64));
      memcpy(&cliInterface2_->inputBuf()[insLobNumOffset], (char*)&lobNum, sizeof(Int32));
      memcpy(&cliInterface2_->inputBuf()[insChunkNumOffset], (char*)&chunkNum, sizeof(Int32));
      memcpy(&cliInterface2_->inputBuf()[insChunkLenIndOffset], (char*)&nullV, sizeof(nullV));
      memcpy(&cliInterface2_->inputBuf()[insChunkLenOffset], (char*)&chunkLen, sizeof(Int64));
      memcpy(&cliInterface2_->inputBuf()[insHdfsOffsetIndOffset], (char*)&nullV, sizeof(nullV));
      memcpy(&cliInterface2_->inputBuf()[insHdfsOffsetOffset], (char*)&hdfsOffset, sizeof(Int64));

      if (hdfsOffset >= 0) // hdfsdata
        chunkLen = 0; // no data to insert in hbase table

      if (evalUsingSingletonInsert)
        {
          Int16 nullV = 0;
          memcpy(&cliInterface2_->inputBuf()[insIndOffset], (char*)&nullV, sizeof(nullV));
          if (insVcIndLen == sizeof(short))
            {
              Int16 len = chunkLen;
              memcpy(&cliInterface2_->inputBuf()[insVarOffset], (char*)&len, sizeof(len));
            }
          else
            {
              Int32 len = chunkLen;
              memcpy(&cliInterface2_->inputBuf()[insVarOffset], (char*)&len, sizeof(len));
            }
          
          memcpy(&cliInterface2_->inputBuf()[insVarOffset+insVcIndLen], dataPtr, chunkLen);
        }

      Int32 insSaltValue = 0;
      Lng32 insSaltValueLen = 0;
      cliRC = cliInterface2_->clearExecFetchCloseOpt(NULL, 0, (char*)&insSaltValue, &insSaltValueLen);
      if (cliRC < 0)
        {
          char errorBuf[200];
          str_sprintf(errorBuf, "insertChunkInHbase::clearExecFetchClose insert returned error %d. %s numChunks = %d, startChunkNum = %d, chunkNum = %d ",
                      cliRC, (wasPrep ? "Stmt was prepared here. " : "Stmt was already prepared. "),
                      numChunks, startChunkNum, chunkNum);
          ComDiagsArea * da = &diags;
          ExRaiseSqlError(currContext.exHeap(), &da,
                          (ExeErrorCode)(8146), NULL, NULL, NULL, NULL,
                          errorBuf);
          return cliRC;
        }

      if (NOT evalUsingSingletonInsert)
        {
          // now update with data. This is done without dtm xns.
          memcpy(&cliInterface3_->inputBuf()[updSaltOffset], (char*)&insSaltValue, sizeof(Int32));
          memcpy(&cliInterface3_->inputBuf()[updLobUIDOffset], (char*)&lobUID[0], sizeof(Int64));
          memcpy(&cliInterface3_->inputBuf()[updLobSeqOffset], (char*)&lobUID[1], sizeof(Int64));
          memcpy(&cliInterface3_->inputBuf()[updLobNumOffset], (char*)&lobNum, sizeof(Int32));
          memcpy(&cliInterface3_->inputBuf()[updChunkNumOffset], (char*)&chunkNum, sizeof(Int32));

          Int16 nullV = 0;
          memcpy(&cliInterface3_->inputBuf()[updIndOffset], (char*)&nullV, sizeof(nullV));
          if (updVcIndLen == sizeof(short))
            {
              Int16 len = chunkLen;
              memcpy(&cliInterface3_->inputBuf()[updVarOffset], (char*)&len, sizeof(len));
            }
          else
            {
              Int32 len = chunkLen;
              memcpy(&cliInterface3_->inputBuf()[updVarOffset], (char*)&len, sizeof(len));
            }
          
          memcpy(&cliInterface3_->inputBuf()[updVarOffset+updVcIndLen], dataPtr, chunkLen);
          
          cliRC = cliInterface3_->clearExecFetchCloseOpt(NULL, 0);
          if (cliRC < 0)
            {
              char errorBuf[200];
              str_sprintf(errorBuf, "insertChunkInHbase::clearExecFetchClose for update returned error %d. ",
                          cliRC);
              ComDiagsArea * da = &diags;
              ExRaiseSqlError(currContext.exHeap(), &da,
                              (ExeErrorCode)(8146), NULL, NULL, NULL, NULL,
                              errorBuf);
              return cliRC;
            }
        }

      insertedDataLen += chunkLen;
      dataLenToInsert -= chunkLen;
      dataPtr += chunkLen;
    } // for

  endChunkNum = startChunkNum + numChunks - 1;

  // clear diags area to remove the internal lob chunks table
  // inserted row count.
  cliInterface2_->clearGlobalDiags();

  return 0;
}

Lng32 ExpLobOperV2::initLobPtr(ExLobGlobals *lobGlobs,
                               char *lobStorageLocation,
                               char *lobFileName,
                               ExLobV2* &lobPtr)
{
  Ex_Lob_Error err = LOB_OPER_OK;

  lobPtr = new (lobGlobs->getHeap())ExLobV2(lobGlobs->getHeap(), 
                                            NULL); //hdfsAccessStats);
  if (lobPtr == NULL) 
    return -1;
  
  err = lobPtr->initialize(lobFileName,
                           lobStorageLocation, Lob_Outline, //storage, 
                           lobStorageLocation,
                           lobGlobs);
  if (err != LOB_OPER_OK)
    return -1;

  return 0;
}

Lng32 ExpLobOperV2::getLobAccessPtr(ExLobGlobals *lobGlobs,
                                    char *lobStorageLocation,
                                    char *lobFileName,
                                    ExLobV2* &lobPtr)
{
  Ex_Lob_Error err = LOB_OPER_OK;

  lobMap_tV2 *lobMap = lobGlobs->getLobMapV2();
  lobMap_itV2 it = lobMap->find(string(lobFileName));
  
  lobPtr = NULL;
  if (it == lobMap->end()) // not found
    {
      if (initLobPtr(lobGlobs, lobStorageLocation, lobFileName, lobPtr))
        return -1;

      lobMap->insert(pair<string, ExLobV2*>(string(lobFileName), lobPtr));
    }
  else
    {
      lobPtr = it->second;
    }  

  return 0;
}

Lng32 ExpLobOperV2::insertDataInHDFS(ExLobGlobals *lobGlobs,
                                     Int64 inDataLen, char *inDataPtr,
                                     DMLOptions &options,
                                     Int64 objectUID,
                                     Int32 lobNum, Int64 *lobUID,
                                     char *lobHdfsStorageLocation,
                                     Int64 &hdfsOffset)
{
  // get name of lob data file
  char lobFileNameBuf[LOB_NAME_LEN];
  char *lobFileName = NULL;
  lobFileName = ExpLOBoper::ExpGetLOBname(objectUID, lobNum, 
                                          lobUID[0], options.numLOBdatafiles,
                                          lobFileNameBuf, LOB_NAME_LEN);    
  if (lobFileName == NULL)
    return -1;

  ExLobV2 *lobPtr = NULL;
  if (getLobAccessPtr(lobGlobs, lobHdfsStorageLocation, lobFileName, lobPtr) != 0)
    return -1;

  Int64 operLen = 0;
  Ex_Lob_Error err = 
    lobPtr->writeData(hdfsOffset, inDataPtr, inDataLen, operLen);
  
  if (err != LOB_OPER_OK)
    return -err;
  
  return 0;
}

Lng32 ExpLobOperV2::readDataFromHDFS(ExLobGlobals *lobGlobs,
                                     Int64 outDataLen, char *outDataPtr,
                                     DMLOptions &options,
                                     Int64 objectUID,
                                     Int32 lobNum, Int64 *lobUID,
                                     char *lobHdfsStorageLocation,
                                     Int64 hdfsOffset,
                                     Int64 &dataLenReturned)
{
  // get name of lob data file
  char lobFileNameBuf[LOB_NAME_LEN];
  char *lobFileName = NULL;
  lobFileName = ExpLOBoper::ExpGetLOBname(objectUID, lobNum, 
                                          lobUID[0], options.numLOBdatafiles,
                                          lobFileNameBuf, LOB_NAME_LEN);    
  if (lobFileName == NULL)
    return -1;

  ExLobV2 *lobPtr = NULL;
  if (getLobAccessPtr(lobGlobs, lobHdfsStorageLocation, lobFileName, lobPtr) != 0)
    return -1;

  Ex_Lob_Error err;
  err = lobPtr->readData(hdfsOffset, outDataPtr, outDataLen, dataLenReturned);
  if (err != LOB_OPER_OK)
    {
      return -err;
    }

  if (outDataLen != dataLenReturned)
    return -1;

  return 0;
}

Lng32 ExpLobOperV2::insertData(ExLobGlobals *lobGlobs,
                               Int64 objectUID, 
                               char *inLobHandle, Int32 inLobHandleLen,
                               char *outLobHandle, Int32 *outLobHandleLen,
                               char *lobChunksTableName,
                               Int64 *lobUID, Int32 lobNum,
                               Int64 inDataLen, char *inDataPtr,
                               Int32 startChunkNum, 
                               ExpLobOperV2::DMLOptions &options,
                               char *lobHdfsStorageLocation,
                               NABoolean addToHdfsOnly,
                               NABoolean forUpdate,
                               ContextCli &currContext)
{
  Lng32 cliRC = 0;

  Int32 endChunkNum = startChunkNum;
  Int64 insertedDataLen = 0;

  Int64 hdfsOffset = -1;

  Int64 remainingDataLen = inDataLen;

  Int32 nextChunkNum = endChunkNum;

  // first insert data in hbase table
  if ((NOT addToHdfsOnly) || (startChunkNum <= 1))
    {
      // copy inhandle to outhandle and append inlined data.
      Int32 inlineDataLen = MINOF(options.lobInlinedDataMaxLen, inDataLen);
      if (outLobHandle && outLobHandleLen &&
          (*outLobHandleLen >= (inLobHandleLen + inlineDataLen)))
        {
          str_cpy_all(outLobHandle, inLobHandle, inLobHandleLen);
          ExpLOBoper::updLOBhandleInlineData(outLobHandle, inlineDataLen,
                                             inDataPtr);

          *outLobHandleLen = inLobHandleLen + inlineDataLen;

          insertedDataLen  += inlineDataLen;
          remainingDataLen -= insertedDataLen;
        }

      if ((remainingDataLen > 0) && (options.lobHbaseDataMaxLen > 0))
        {
          Int64 hbaseInsertedDataLen = 0;
          cliRC = insertChunkInHbase(lobChunksTableName, lobUID, lobNum,
                                     remainingDataLen, 
                                     &inDataPtr[insertedDataLen],
                                     startChunkNum, endChunkNum, 
                                     hbaseInsertedDataLen,
                                     hdfsOffset,
                                     options,
                                     forUpdate,
                                     currContext);
          if (cliRC < 0)
            return cliRC;

          nextChunkNum = endChunkNum + 1;
          insertedDataLen += hbaseInsertedDataLen;
          remainingDataLen -= hbaseInsertedDataLen;
        }
    }

  // insert remaining data in hdfs. Get back offset.
  if (remainingDataLen > 0)
    {
      cliRC = insertDataInHDFS(lobGlobs,
                               remainingDataLen,
                               &inDataPtr[insertedDataLen],
                               options,
                               objectUID,
                               lobNum, lobUID,
                               lobHdfsStorageLocation,
                               hdfsOffset);
      if (cliRC < 0)
        return cliRC;
    }

  if (hdfsOffset > -1)
    {
      // insert hbase chunk with hdfsoffset 
      cliRC = insertChunkInHbase(lobChunksTableName, lobUID, lobNum,
                                 remainingDataLen, NULL, 
                                 nextChunkNum, endChunkNum,
                                 insertedDataLen,
                                 hdfsOffset,
                                 options, 
                                 forUpdate,
                                 currContext);
      if (cliRC < 0)
        return cliRC;
    }

  return cliRC;
}

ExpLobOperV2::ExpLobOperV2() :
       savedDataBuf_(NULL),
       savedDataBufLen_(0),
       savedDataPtr_(NULL),
       savedDataLen_(0),
       inlineDataLen_(0),
       flags_(0)
{
  CliGlobals *cliGlobals = GetCliGlobals();
  ContextCli   & currContext = *(cliGlobals->currContext());

  cliInterface1_ = NULL;
  cliInterface2_ = NULL;
  cliInterface3_ = NULL;
  cliInterface4_ = NULL;
  cliInterface5_ = NULL;
  cliInterface6_ = NULL;
  cliInterface7_ = NULL;

  setDataInHBase(TRUE);
}

ExpLobOperV2::~ExpLobOperV2()
{
  if (cliInterface1_)
    delete cliInterface1_;

  if (cliInterface2_)
    delete cliInterface2_;

  if (cliInterface3_)
    delete cliInterface3_;

  if (cliInterface4_)
    delete cliInterface4_;

  if (cliInterface5_)
    delete cliInterface5_;

  if (cliInterface6_)
    delete cliInterface6_;

  if (cliInterface7_)
    delete cliInterface7_;
}

Lng32 ExpLobOperV2::dmlInterface
(
     /*IN*/     ExLobGlobals *lobGlobs,
     /*IN*/     char * inLobHandle,
     /*IN*/     Lng32  inLobHandleLen,
     /*IN*/     DMLQueryType qType,
     /*IN*/     DMLOptions &options,
     /*IN*/     char *lobHdfsStorageLocation,
     /*IN*/     CollHeap *heap,
     /*OUT*/    char *outLobHandle, // if passed in, returns modified handle
                                    // and inlined data
     /*INOUT*/  Lng32 *outLobHandleLen, // if passed in, is length of outHandle
                                        // buffer. 
                                        // Returns: length of modified handle
     /*INOUT*/  Int64 * dataOffset, /* IN: for insert, OUT: for select */
     /*INOUT*/  Int64 * dataLenPtr, /* length of data.
                                      IN: for insert, out: for select */
     /*INOUT*/  ExeCliInterface* *inCliInterface
 )
{
  CliGlobals *cliGlobals = GetCliGlobals();
  ContextCli   & currContext = *(cliGlobals->currContext());
  ComDiagsArea & diags       = currContext.diags();

  ComDiagsArea * myDiags = NULL;

  if (! cliInterface1_)
    {
      cliInterface1_ = new (heap) 
	ExeCliInterface(heap, SQLCHARSETCODE_UTF8, &currContext, NULL);
    }

  if (! cliInterface2_)
    {
      cliInterface2_ = new (heap) 
	ExeCliInterface(heap, SQLCHARSETCODE_UTF8, &currContext, NULL);
    }

  if (! cliInterface3_)
    {
      cliInterface3_ = new (heap) 
	ExeCliInterface(heap, SQLCHARSETCODE_UTF8, &currContext, NULL);
    }

  if (! cliInterface4_)
    {
      cliInterface4_ = new (heap) 
	ExeCliInterface(heap, SQLCHARSETCODE_UTF8, &currContext, NULL);
    }

  if (! cliInterface5_)
    {

      cliInterface5_ = new (heap) 
	ExeCliInterface(heap, SQLCHARSETCODE_UTF8, &currContext, NULL);
    }

  if (! cliInterface6_)
    {
      cliInterface6_ = new (heap) 
	ExeCliInterface(heap, SQLCHARSETCODE_UTF8, &currContext, NULL);
    }

  if (! cliInterface7_)
    {
      cliInterface7_ = new (heap) 
	ExeCliInterface(heap, SQLCHARSETCODE_UTF8, &currContext, NULL);
    }

  Lng32 lobNum = 0;
  Int64 objectUID = 0;
  Int64 lobUID[2] = {0, 0};
  short schNameLen = 0;
  char schName[512];
  char logBuf[4096];
  Int64 inputValues[5];
  Lng32 inputValuesLen = 0;
  Int64 rowsAffected = 0;
  Int32 inlineDataLen = 0;
  char *inlineDataPtr = NULL;
  char * dataPtr = (dataOffset ? (char*)dataOffset : NULL);
  Int64 inDataLen = dataLenPtr ? *dataLenPtr : 0;

  char lobChunksTableNameBuf[1024];
  if (inLobHandle)
    {
      ExpLOBoper::extractFromLOBhandleV2(NULL, NULL, &lobNum, &objectUID,  
                                         &inlineDataLen, NULL, 
                                         &lobUID[0], &lobUID[1],
                                         inLobHandle);
    }
  
  if (options.chunkTableSchNameLen > 0)
    {
      schNameLen = options.chunkTableSchNameLen;
      strcpy(schName, options.chunkTableSch);
    }
  
  if ((! inLobHandle) || (lobUID[0] <= 0))
    {
      // error condition
      diags << DgSqlCode(-8145) 
            << DgString0("Invalid LOB handle.");
      return -1;
    }
  
  str_sprintf(lobChunksTableNameBuf, "%s.%s%020ld", 
              schName, LOB_CHUNKS_V2_PREFIX, objectUID);

  char query[4000];

  Lng32 cliRC = 0;
  Lng32 tempCliRC = 0;

  ExeCliInterface *errCliInterface = NULL;
  switch (qType)
    {
    case LOB_DML_INSERT:
    case LOB_DML_UPDATE:
      {
        //        assert(dataLenPtr);
        if (inDataLen > 0)
          assert(dataOffset);

        Int32 chunkNum = 1; // inserting first chunk
        Int64 hdfsOffset = -1;

        cliRC = insertData(lobGlobs,
                           objectUID,
                           inLobHandle, inLobHandleLen,
                           outLobHandle, outLobHandleLen,
                           lobChunksTableNameBuf, lobUID, lobNum,
                           inDataLen, dataPtr, chunkNum,
                           options, lobHdfsStorageLocation,
                           FALSE,
                           (qType == LOB_DML_UPDATE),
                           currContext);
        
        if (cliRC < 0)
          goto error_return;

	cliRC = 0;
      }
      break;

    case LOB_DML_INSERT_APPEND:
    case LOB_DML_UPDATE_APPEND:
      {
        assert(dataLenPtr);
        if (inDataLen > 0)
          assert(dataOffset);

        // find out max chunknum.
        if (NOT cliInterface5_->isAllocated())
          {
            cliRC = cliInterface6_->executeImmediate("control query shape cut");
            if (cliRC < 0)
              {
                errCliInterface = cliInterface6_;
                goto error_return;
              }

            // if this query ie being run for the first time, prepare it.
            str_sprintf(query, "select cast(case when max(chunknum) is null then 0 else max(chunknum) end as int not null), cast(case when max(hdfs_offset) is null then -1 else max(hdfs_offset) end as largeint not null) from table(ghost table %s) where lobuid = cast(? as largeint not null) and lobseq = cast(? as largeint not null) and lobnum = cast(? as int not null)",
                        lobChunksTableNameBuf);
            
            // set parserflags to allow ghost table
            currContext.setSqlParserFlags(0x1);
            cliRC = cliInterface5_->executeImmediatePrepare(query);
            currContext.resetSqlParserFlags(0x1);
            if (cliRC < 0)
              {
                errCliInterface = cliInterface5_;
                goto error_return;
              }
          }

        Lng32 dummy;
        Int32 lobUIDOffset = 0;
        Int32 lobSeqOffset = 0;
        Int32 lobNumOffset = 0;
        cliInterface5_->getAttributes(1, TRUE,
                                      dummy, dummy, dummy, NULL, &lobUIDOffset);
        memcpy(&cliInterface5_->inputBuf()[lobUIDOffset], (char*)&lobUID[0], sizeof(Int64));

        cliInterface5_->getAttributes(2, TRUE,
                                      dummy, dummy, dummy, NULL, &lobSeqOffset);
        memcpy(&cliInterface5_->inputBuf()[lobSeqOffset], (char*)&lobUID[1], sizeof(Int64));

        cliInterface5_->getAttributes(3, TRUE,
                                      dummy, dummy, dummy, NULL, &lobNumOffset);
        memcpy(&cliInterface5_->inputBuf()[lobNumOffset], (char*)&lobNum, sizeof(Int32));

        char outputBuf[sizeof(Int32) + sizeof(Int64)];
        Lng32 len = 0;
        cliRC = cliInterface5_->clearExecFetchClose
          (NULL, 0, 
           outputBuf, &len);
        if (cliRC < 0)
          {
            errCliInterface = cliInterface5_;
            goto error_return; 
          }

        Int32 chunkNum   = *(Int32*)outputBuf;
        Int64 hdfsOffset = *(Int64*)&outputBuf[sizeof(Int32)];

        chunkNum++; // next chunk

        cliRC = insertData(lobGlobs, objectUID,
                           inLobHandle, inLobHandleLen,
                           outLobHandle, outLobHandleLen,
                           lobChunksTableNameBuf, lobUID, lobNum,
                           inDataLen, dataPtr, chunkNum,
                           options, lobHdfsStorageLocation,
                           (hdfsOffset >= 0 ? TRUE : FALSE),
                           (qType == LOB_DML_UPDATE_APPEND),
                           currContext);
        if (cliRC < 0)
          goto error_return;

	cliRC = 0;
      }
      break;

    case LOB_DML_DELETE:
      {
	// delete from lob descriptor handle table
        if (NOT cliInterface4_->isAllocated())
          {
            Int64 transID = getTransactionIDFromContext();
            NABoolean nonXnOper = FALSE;
            if ((transID <= 0) || 
                (NOT options.lobXnOper()))
              nonXnOper = TRUE;
            
            if (nonXnOper)
              {
                cliRC = cliInterface6_->holdAndSetCQD("traf_no_dtm_xn", "ON");
                if (cliRC < 0)
                  return restoreCQDs(cliRC);
              }

            cliRC = cliInterface6_->holdAndSetCQDs("attempt_esp_parallelism, auto_adjust_cacherows, hbase_cache_blocks", "attempt_esp_parallelism 'OFF', auto_adjust_cacherows 'OFF', hbase_cache_blocks 'ON' ");
            if (cliRC < 0)
              {
                restoreCQDs(cliRC);
                errCliInterface = cliInterface6_;
                goto error_return;
              }

            cliRC = cliInterface6_->executeImmediate("control query shape cut");
            if (cliRC < 0)
              {
                restoreCQDs(cliRC);
                errCliInterface = cliInterface6_;
                goto error_return;
              }

            str_sprintf(query, "delete from table(ghost table %s) where lobuid = cast(? as largeint not null) and lobseq = cast(? as largeint not null) and lobnum = cast(? as int not null) ",
                        lobChunksTableNameBuf);

            // set parserflags to allow ghost table
            currContext.setSqlParserFlags(0x1);
            cliRC = cliInterface4_->executeImmediatePrepare(query);
            currContext.resetSqlParserFlags(0x1);
            if (cliRC < 0)
              {
                restoreCQDs(cliRC);
                errCliInterface = cliInterface4_;
                goto error_return;
              }

            restoreCQDs(0);
          }
        
        Int64 start = NA_JulianTimestamp();
        //cout << "Start " << start << " , lobnum = " << lobNum << endl;

        Lng32 dummy;
        Int32 lobUIDOffset = 0;
        Int32 lobSeqOffset = 0;
        Int32 lobNumOffset = 0;
        cliInterface4_->getAttributes(1, TRUE,
                                      dummy, dummy, dummy, NULL, &lobUIDOffset);
        memcpy(&cliInterface4_->inputBuf()[lobUIDOffset], (char*)&lobUID[0], sizeof(Int64));         
        cliInterface4_->getAttributes(2, TRUE,
                                      dummy, dummy, dummy, NULL, &lobSeqOffset);
        memcpy(&cliInterface4_->inputBuf()[lobSeqOffset], (char*)&lobUID[1], sizeof(Int64));         
        cliInterface4_->getAttributes(3, TRUE,
                                      dummy, dummy, dummy, NULL, &lobNumOffset);
        memcpy(&cliInterface4_->inputBuf()[lobNumOffset], (char*)&lobNum, sizeof(Int32));
        
        cliRC = cliInterface4_->clearExecFetchCloseOpt(NULL, 0);
        if (cliRC < 0)
          {
            errCliInterface = cliInterface4_;
            goto error_return;
          }


        /*
        if (getenv("TRACE_LOB_DELETE"))
          {
            Int64 end = NA_JulianTimestamp();

            char startStr[11];
            char endStr[11];
            convertJulianTimestamp(start, startStr);
            convertJulianTimestamp(end, endStr);
            char startBuf[100];
            sprintf(startBuf, "Start %04d:%02d:%02d %02d:%02d:%02d.%04d",
                    *(short*)&startStr[0], (Int32)startStr[2], (Int32)startStr[3],
                    (Int32)startStr[4], (Int32)startStr[6], (Int32)startStr[6], 
                    *(Int32*)&startStr[7]);

            char endBuf[100];
            sprintf(endBuf, "End %d:%02d:%02d %02d:%02d:%02d.%d",
                    *(short*)&endStr[0], (Int32)endStr[2], (Int32)endStr[3],
                    (Int32)endStr[4], (Int32)endStr[5], (Int32)endStr[6], 
                    *(Int32*)&endStr[7]);
            
            cout << startBuf << endl;
            cout << endBuf << endl;
            cout << "ET " << (end - start) << endl;
          }
        */

        cliInterface4_->clearGlobalDiags();
      }
      break;

    case LOB_DML_SELECT_OPEN:
      {
        if (inCliInterface && (! *inCliInterface))
          {
            *inCliInterface = new (heap) 
              ExeCliInterface(heap, SQLCHARSETCODE_UTF8, &currContext, NULL);
          }

        ExeCliInterface *cliInterface1 = 
          (inCliInterface ? *inCliInterface : cliInterface1_);

        inlineDataLen_ = ExpLOBoper::lobHandleGetInlineDataLength(inLobHandle);
        if ((inDataLen >= 0) && (inlineDataLen_ >= inDataLen))
          break;

        if (NOT cliInterface1->isAllocated())
          {
            cliRC = cliInterface6_->holdAndSetCQDs("attempt_esp_parallelism, auto_adjust_cacherows, hbase_cache_blocks", " attempt_esp_parallelism 'OFF', auto_adjust_cacherows 'OFF', hbase_cache_blocks 'ON' ");
            if (cliRC < 0)
              {
                restoreCQDs(cliRC);
                errCliInterface = cliInterface6_;
                goto error_return;
              }
            
            cliRC = cliInterface6_->executeImmediate("control query shape cut");
            if (cliRC < 0)
              {
                restoreCQDs(cliRC);
                errCliInterface = cliInterface6_;
                goto error_return;
              }

            // if (inDataLen <= 0)
              str_sprintf(query, "select chunknum, chunklen, hdfs_offset, data_in_hbase from table(ghost table %s) where lobuid = cast(? as largeint not null) and lobseq = cast(? as largeint not null) and lobnum = cast(? as int not null) order by chunknum",
                          lobChunksTableNameBuf);
            /* else
              str_sprintf(query, "select chunknum, chunklen, hdfs_offset, left(data_in_hbase, %ld) from table(ghost table %s) where lobuid = cast(? as largeint not null) and lobseq = cast(? as largeint not null) and lobnum = cast(? as int not null) order by chunknum",
                          inDataLen,
                          lobChunksTableNameBuf); */

            // set parserflags to allow ghost table
            currContext.setSqlParserFlags(0x1);
            cliRC = cliInterface1->fetchRowsPrologue(query, TRUE/*noexec*/);
            currContext.resetSqlParserFlags(0x1);
            if (cliRC < 0)
              {
                restoreCQDs(cliRC);
                errCliInterface = cliInterface1;
                goto error_return;
              }

            restoreCQDs(0);
          }

        Lng32 dummy;
        Int32 lobUIDOffset = 0;
        Int32 lobSeqOffset = 0;
        Int32 lobNumOffset = 0;
        cliInterface1->getAttributes(1, TRUE,
                                      dummy, dummy, dummy, NULL, &lobUIDOffset);
        memcpy(&cliInterface1->inputBuf()[lobUIDOffset], (char*)&lobUID[0], sizeof(Int64));        
        cliInterface1->getAttributes(2, TRUE,
                                      dummy, dummy, dummy, NULL, &lobSeqOffset);
        memcpy(&cliInterface1->inputBuf()[lobSeqOffset], (char*)&lobUID[1], sizeof(Int64));        
        cliInterface1->getAttributes(3, TRUE,
                                      dummy, dummy, dummy, NULL, &lobNumOffset);
        memcpy(&cliInterface1->inputBuf()[lobNumOffset], (char*)&lobNum, sizeof(Int32));        
        cliRC = cliInterface1->exec();
        if (cliRC < 0)
          {
            errCliInterface = cliInterface1;
            goto error_return;
          }

      }
      break;

    case LOB_DML_SELECT_FETCH:
    case LOB_DML_SELECT_FETCH_AND_DONE:
      {
        assert(dataOffset && dataLenPtr);

        ExeCliInterface *cliInterface1 = 
          (inCliInterface ? *inCliInterface : cliInterface1_);

        *dataLenPtr = 0;
        char *currDataPtr = (char*)(*dataOffset);
        
        cliRC = 0;
        if (inlineDataLen_ > 0)
          {
            inlineDataPtr = ExpLOBoper::lobHandleGetInlineDataPosition(inLobHandle);

            Int64 dataToMove = MINOF(inlineDataLen_, inDataLen);
            str_cpy_all(currDataPtr, inlineDataPtr, dataToMove);

            *dataLenPtr += dataToMove;
            currDataPtr += dataToMove;

            inlineDataLen_ = 0;
          }
        else if ((savedDataPtr_) && (savedDataLen_ > 0) && 
                 (qType == LOB_DML_SELECT_FETCH))
          {
            Int64 dataToMove = MINOF(savedDataLen_, inDataLen);
            if (dataInHBase()) // data from hbase
              str_cpy_all(currDataPtr, savedDataPtr_, dataToMove);
            else
              {
                // read data from hdfs
                Int64 dataLenReturned = 0;
                cliRC = readDataFromHDFS(lobGlobs, dataToMove, currDataPtr,
                                         options, objectUID,
                                         lobNum, lobUID,
                                         lobHdfsStorageLocation,
                                         (Int64)savedDataPtr_, dataLenReturned);
              }

            savedDataPtr_ += dataToMove;
            savedDataLen_ -= dataToMove;
            if (savedDataLen_ <= 0)
              savedDataPtr_ = NULL;

            *dataLenPtr += dataToMove;
          }

        NABoolean done = FALSE;
        if (cliRC < 0)
          done = TRUE;

        for (; NOT done; )
          {
            if (*dataLenPtr >= inDataLen)
              {
                done = TRUE;
                continue;
              }

            cliRC = cliInterface1->fetch();
            if (cliRC < 0)
              {
                done = TRUE;
                continue;
              }
 
            if (cliRC == 100) // all done
              {
                if (*dataLenPtr > 0)
                  cliRC = 0;

                done = TRUE;
                continue;
              }

            char * ptr;
            Lng32 len;
            
            Lng32 chunkNum = 0;
            cliInterface1->getPtrAndLen(1, ptr, len);	    
            chunkNum = *(Int32*)ptr;
            
            Lng32 chunkLen = 0;
            cliInterface1->getPtrAndLen(2, ptr, len);	    
            chunkLen = *(Int64*)ptr;

            Int64 dataToMove = MINOF(chunkLen, (inDataLen - *dataLenPtr));
            *dataLenPtr += dataToMove;

            Int64 hdfsOffset = -1;
            cliInterface1->getPtrAndLen(3, ptr, len);
            hdfsOffset = *(Int64*)ptr;

            if (hdfsOffset < 0)
              setDataInHBase(TRUE);
            else
              setDataInHBase(FALSE);

            if (dataInHBase()) // data in hbase
              {
                cliInterface1->getPtrAndLen(4, ptr, len);	    
                str_cpy_all(currDataPtr, ptr, dataToMove);
                
                currDataPtr += dataToMove;
                
                if (chunkLen > dataToMove)
                  {
                    Int64 remainingChunkLen = chunkLen - dataToMove;
                    if ((qType == LOB_DML_SELECT_FETCH) && 
                        (remainingChunkLen > 0))
                      {
                        // save remaining chunk which will be returned on 
                        // the next fetch
                        if ((savedDataBuf_) && 
                            (savedDataBufLen_ < remainingChunkLen))
                          {
                            NADELETEBASIC(savedDataBuf_, heap);
                            savedDataBuf_ = NULL;
                          }
                        
                        if (!savedDataBuf_)
                          {
                            savedDataBufLen_ = remainingChunkLen;
                            savedDataBuf_ = new(heap) char[savedDataBufLen_];
                          }
                                
                        str_cpy_all(savedDataBuf_, &ptr[dataToMove],
                                    savedDataBufLen_);
                        savedDataPtr_ = savedDataBuf_;
                        savedDataLen_ = savedDataBufLen_;
                      }

                    done = TRUE;
                  }
              } // data in hbase
            else
              {
                // get data from hdfs
                Int64 dataLenReturned = 0;
                cliRC = readDataFromHDFS(lobGlobs, dataToMove, currDataPtr,
                                         options, objectUID,
                                         lobNum, lobUID,
                                         lobHdfsStorageLocation,
                                         hdfsOffset, dataLenReturned);

                if (cliRC < 0)
                  {
                    done = TRUE;
                    continue;
                  }

                if (*dataLenPtr >= inDataLen)
                  {
                    Int64 remainingChunkLen = chunkLen - dataToMove;
                    savedDataPtr_ = (char*)(hdfsOffset + dataToMove);
                    savedDataLen_ = remainingChunkLen;

                    done = TRUE;
                    continue;
                  }

                currDataPtr += dataToMove;
              }
          } // for
        
        if (cliRC < 0)
          {
            if (savedDataBuf_)
              NADELETEBASIC(savedDataBuf_, heap);
            savedDataBuf_ = NULL;

            cliInterface1->fetchRowsEpilogue(0);

            errCliInterface = cliInterface1;
            goto error_return;
          }
       }
      break;

    case LOB_DML_SELECT_CLOSE:
      {
        ExeCliInterface *cliInterface1 = 
          (inCliInterface ? *inCliInterface : cliInterface1_);
        
        if (savedDataBuf_)
          NADELETEBASIC(savedDataBuf_, heap);
        savedDataBuf_ = NULL;
        savedDataBufLen_ = 0;
        savedDataPtr_ = 0;
        savedDataLen_ = 0;

        if (cliInterface1->isAllocated())
          {
            cliRC = cliInterface1->close(TRUE);
            if (cliRC < 0)
              {
                errCliInterface = cliInterface1;
                goto error_return;
              }
          }

	cliRC = 100;
	
        cliInterface1->clearGlobalDiags();
      }
      break;

   case LOB_DML_SELECT_ALL:
      {
        assert(dataOffset && dataLenPtr);

        cliRC = dmlInterface(lobGlobs,
                             inLobHandle, inLobHandleLen,
                             LOB_DML_SELECT_OPEN, options, 
                             lobHdfsStorageLocation, heap,
                             NULL, NULL,
                             NULL, dataLenPtr);
	if (cliRC < 0)
          goto error_return2;

        cliRC = dmlInterface(lobGlobs,
                             inLobHandle, inLobHandleLen,
                             LOB_DML_SELECT_FETCH_AND_DONE, options, 
                             lobHdfsStorageLocation, heap,
                             NULL, NULL,
                             dataOffset, dataLenPtr);
	if (cliRC < 0)
          goto error_return2;

        cliRC = dmlInterface(lobGlobs,
                             inLobHandle, inLobHandleLen,
                             LOB_DML_SELECT_CLOSE, options, 
                             lobHdfsStorageLocation, heap,
                             NULL, NULL,
                             NULL, NULL);
	if (cliRC < 0)
          goto error_return2;
      }
      break;

    case LOB_DML_SELECT_LOBLENGTH:
      {
        assert(dataLenPtr);

        if (inCliInterface && (! *inCliInterface))
          {
            *inCliInterface = new (heap) 
              ExeCliInterface(heap, SQLCHARSETCODE_UTF8, &currContext, NULL);
          }

        ExeCliInterface *cliInterface7 = 
          (inCliInterface ? *inCliInterface : cliInterface7_);
        if (NOT cliInterface7->isAllocated())
          {
            cliRC = cliInterface6_->executeImmediate("control query shape cut");
            if (cliRC < 0)
              {
                errCliInterface = cliInterface6_;
                goto error_return;
              }

            //aggregate on chunklen for this lob.
            str_sprintf(query, "select cast(case when sum(chunklen) is null then 0 else sum(chunklen) end as largeint not null) from table(ghost table %s) where lobuid = cast(? as largeint not null) and lobseq = cast(? as largeint not null) and lobnum = cast(? as int not null) for read committed access",
                        lobChunksTableNameBuf);
            
            // set parserflags to allow ghost table
            currContext.setSqlParserFlags(0x1);
            cliRC = cliInterface7->fetchRowsPrologue(query, TRUE/*noexec*/);
            currContext.resetSqlParserFlags(0x1);
            if (cliRC < 0)
              {
                errCliInterface = cliInterface7;
                goto error_return;
              }
          }

        Lng32 dummy;
        Int32 lobUIDOffset = 0;
        Int32 lobSeqOffset = 0;
        Int32 lobNumOffset = 0;
        cliInterface7->getAttributes(1, TRUE,
                                     dummy, dummy, dummy, NULL, &lobUIDOffset);
        memcpy(&cliInterface7->inputBuf()[lobUIDOffset], (char*)&lobUID[0], sizeof(Int64));        
        cliInterface7->getAttributes(2, TRUE,
                                     dummy, dummy, dummy, NULL, &lobSeqOffset);
        memcpy(&cliInterface7->inputBuf()[lobSeqOffset], (char*)&lobUID[1], sizeof(Int64));        
        cliInterface7->getAttributes(3, TRUE,
                                     dummy, dummy, dummy, NULL, &lobNumOffset);
        memcpy(&cliInterface7->inputBuf()[lobNumOffset], (char*)&lobNum, sizeof(Int32));

        Int64 loblen = 0;
        Lng32 len = 0;
        cliRC = cliInterface7->clearExecFetchClose
          (NULL, 0, 
           (char *)&loblen, &len);
        if (cliRC < 0)
          {
            errCliInterface = cliInterface7;
            goto error_return;
          }

        // return loblen in dataLen parameter
        *dataLenPtr = inlineDataLen + loblen;
      }
      break;

    } // switch 

 error_return:
  if ((cliRC < 0) && (errCliInterface))
    {
      errCliInterface->allocAndRetrieveSQLDiagnostics(myDiags);
      diags.mergeAfter(*myDiags);
      myDiags->decrRefCount();

      if (inCliInterface && (! *inCliInterface))
        {
          delete *inCliInterface;
          *inCliInterface = NULL;
        }

      errCliInterface->dealloc();

      return cliRC;
    }

 error_return2:
  if (cliRC < 0)
    return cliRC;
  else if (cliRC == 100)
    return 100;
  else
    return 0;
}

/////////////////////////////////////////////////////////////////////////
// ExLobV2
/////////////////////////////////////////////////////////////////////////
ExLobV2::ExLobV2(NAHeap * heap, ExHdfsScanStats *hdfsAccessStats)
       : ExLob(heap, hdfsAccessStats)
{
}

ExLobV2::~ExLobV2()
{
}

Ex_Lob_Error ExLobV2::initialize(const char *lobFile,
                                 char *lobStorageLocation,
                                 ComLobsStorageType storage, 
                                 char *lobLocation,
                                 ExLobGlobals *lobGlobals)
{
  int openFlags;
  struct timespec startTime;
  struct timespec endTime;
  Int64 secs, nsecs, totalnsecs;
 
  hdfsWriteLockTimeoutInSecs_ = lobGlobals->hdfsWriteLockTimeout();
  if (lobStorageLocation) 
    {
      if (lobStorageLocation_.isNull()) 
	{
          lobStorageLocation_ = lobStorageLocation;
	}

      if (lobFile)
        {
          lobDataFile_ = lobStorageLocation; 
          lobDataFile_ += "/";
          lobDataFile_ += lobFile;
        }      
    } 
  else 
    { 
      if (lobFile)
        lobDataFile_ = lobFile;
    }

  if (storage_ != Lob_Invalid_Storage) 
    {
      return LOB_INIT_ERROR;
    } else 
    {
      storage_ = storage;
    }

  clock_gettime(CLOCK_MONOTONIC, &startTime);
  lobGlobalHeap_ = lobGlobals->getHeap();    
  HDFS_Client_RetCode hdfsClientRetcode;

  hdfsClient_ = HdfsClient::newInstance(lobGlobalHeap_, stats_, hdfsClientRetcode);
  fs_ = NULL;
  if (hdfsClient_ == NULL)
    return LOB_HDFS_CONNECT_ERROR;

  clock_gettime(CLOCK_MONOTONIC, &endTime);

  secs = endTime.tv_sec - startTime.tv_sec;
  nsecs = endTime.tv_nsec - startTime.tv_nsec;
  if (nsecs < 0) 
    {
      secs--;
      nsecs += NUM_NSECS_IN_SEC;
    }
  totalnsecs = (secs * NUM_NSECS_IN_SEC) + nsecs;
   
  hdfsClientRetcode = hdfsClient_->hdfsOpen(lobDataFile_.data(), FALSE);
  if (hdfsClientRetcode != HDFS_CLIENT_OK)
    return LOB_DATA_FILE_OPEN_ERROR;
  fdData_ = NULL;
  
  return LOB_OPER_OK;
}

Ex_Lob_Error ExLobV2::writeData(Int64 &offset, char *data, Int32 size, 
                                Int64 &operLen)
{ 
  HDFS_Client_RetCode hdfsClientRetcode = 
    HDFS_CLIENT_ERROR_HDFS_WRITE_IMMEDIATE_EXCEPTION_RETRY;
  
  Int32 numRetries = 1;
  Int32 maxRetries = 3;
  Int64 delay = 100;
  operLen = 0;
  while ((hdfsClientRetcode == HDFS_CLIENT_ERROR_HDFS_WRITE_IMMEDIATE_EXCEPTION_RETRY) &&
         (numRetries <= maxRetries))
    {
      offset = hdfsClient_->hdfsWriteImmediate(
           data, size, hdfsClientRetcode, TRUE, useHdfsWriteLock_, 
           hdfsWriteLockTimeoutInSecs_ * 1000); 
      if (hdfsClientRetcode == HDFS_CLIENT_OK)
        break;

      DELAY(delay);
      numRetries++;
      delay = delay * 2;
    } // while

  if (hdfsClientRetcode == HDFS_CLIENT_ERROR_HDFS_WRITE_IMMEDIATE_EXCEPTION_RETRY)
    {
      //Return a retryable error
      return LOB_DATA_WRITE_ERROR_RETRY;
    }
  else if (hdfsClientRetcode != HDFS_CLIENT_OK)
    return LOB_DATA_WRITE_ERROR;      
  
  operLen = size;
  return LOB_OPER_OK;      
}

// read 'tgtSize' bytes of data from 'offset' into 'tgt'. 
// Return length read into 'operLen'
Ex_Lob_Error ExLobV2::readData(Int64 offset, char *tgt, Int32 tgtSize, 
                               Int64 &operLen)
{ 
  HDFS_Client_RetCode hdfsClientRetcode = HDFS_CLIENT_OK;
  
  operLen = hdfsClient_->hdfsRead(
       offset, tgt, tgtSize, hdfsClientRetcode);
  
  if (hdfsClientRetcode != HDFS_CLIENT_OK)
    return LOB_DATA_READ_ERROR;      
  
  return LOB_OPER_OK;      
}

