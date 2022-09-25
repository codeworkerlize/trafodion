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

#include "ExpParquetInterface.h"
#include "ex_ex.h"
#include "HBaseClient_JNI.h"

ExpParquetInterface* ExpParquetInterface::newInstance(CollHeap* heap,
                                                      const char* server,
                                                      const Lng32 port)
{
  return new (heap) ExpParquetInterface(heap, server, port);
}

ExpParquetInterface::ExpParquetInterface(CollHeap * heap,
                                         const char * server,
                                         const Lng32 port) :
     ExpExtStorageInterface(heap, server, port, PARQUET_),
     pfr_(NULL)
{
  pfr_ = new(heap) ParquetFileReader((NAHeap*)heap);
  if (pfr_->init() != JOI_OK) 
    ex_assert(false, "ParquetFileReader::init method failed"); 
}

ExpParquetInterface::~ExpParquetInterface()
{
  // close. Ignore errors.
  scanClose();
  NADELETE(pfr_, ParquetFileReader, heap_);
}

Lng32 ExpParquetInterface::init()
{
  pfr_->init();

  return 0;
}

Lng32 ExpParquetInterface::open(ExHdfsScanStats *hdfsStats, char * tableName,
                                char * fileName,
                                const int expectedRowSize,
                                const int maxRowsToFill,
                                const int maxAllocationInMB,
                                const int maxVectorBatchSize,
                                const Int64 startRowNum, 
                                const Int64 stopRowNum,
                                Lng32 numCols,
                                Lng32 * whichCols,
                                TextVec *ppiVec,
                                TextVec *ppiAllCols,
                                TextVec *colNameVec,
                                TextVec *colTypeVec,
                                char * parqSchStr,
                                Lng32 flags)
{
  PFR_RetCode rc = 
       pfr_->open(tableName,
                  fileName,
                  expectedRowSize,
                  maxRowsToFill,
                  maxAllocationInMB,
                  startRowNum, stopRowNum, 
                  numCols, whichCols,
                  ppiVec,
                  ppiAllCols,
                  colNameVec,
                  colTypeVec,
                  parqSchStr,
                  flags);
  if (rc != PFR_OK)
    return -rc;

  return 0;
}

Lng32 ExpParquetInterface::scanOpen(
     ExHdfsScanStats *hdfsStats,
     char * tableName,
     char * fileName,
     const int expectedRowSize,
     const int maxRowsToFill,
     const int maxAllocationInMB,
     const int maxVectorBatchSize,
     const Int64 startRowNum, 
     const Int64 stopRowNum,
     Lng32 numCols,
     Lng32 * whichCols,
     TextVec *ppiVec,
     TextVec *ppiAllCols,
     TextVec *colNameVec,
     TextVec *colTypeVec,
     std::vector<UInt64>* expectedRowsVec,
     char * parqSchStr,
     Lng32 flags,
     Int32 numThreads)
{
  Lng32 rc0 = open(hdfsStats, tableName, fileName,
                   expectedRowSize,
                   maxRowsToFill, maxAllocationInMB, 
                   maxVectorBatchSize,
                   startRowNum, stopRowNum,
                   numCols, whichCols, ppiVec, ppiAllCols,
                   colNameVec, colTypeVec, parqSchStr, flags);
  if (rc0)
    {
      if (rc0 == -PFR_ERROR_FILE_NOT_FOUND_EXCEPTION)
        {
          startRowNum_ = -1;
          return 0;
        }
      
      return rc0;
    }

  startRowNum_ = startRowNum;
  stopRowNum_ = -1;
  currRowNum_ = 0;

  return 0;
}

Lng32 ExpParquetInterface::scanFetch(
                     char** row, Int64 &rowLen, Int64 &rowNum,
                                 Lng32 &numCols,
				 ExHdfsScanStats *hss
				 )
{ 
  if ((startRowNum_ == -1) ||
      ((stopRowNum_ != -1) && (currRowNum_ > stopRowNum_)))
    return 100;
  
  PFR_RetCode rc = pfr_->fetchNextRow(row, rowLen, rowNum, numCols, hss);
  if (rc == PFR_NOMORE)
    return 100;
  
  if (rc != PFR_OK)
    return -rc;
  
  if ((rowLen < 0) || (numCols < 0))
    return -PFR_UNKNOWN_ERROR;

  currRowNum_++;
  
  return 0;
}

Lng32 ExpParquetInterface::close()
{
  PFR_RetCode rc = pfr_->close();
  if (rc != PFR_OK)
    return -rc;
 
  return 0;
}

Lng32 ExpParquetInterface::scanClose()
{
  return close();
}

Lng32 ExpParquetInterface::getColStats(Lng32 colNum, char * colName, Lng32 colType,
                                       NAArray<HbaseStr> **colStats)
{
  NAArray<HbaseStr> *retColStats = NULL;

  retColStats = pfr_->getColStats((NAHeap *)heap_, colNum, colName, colType);
  if (retColStats == NULL)
     return -PFR_UNKNOWN_ERROR;
  else
     *colStats = retColStats; 
  return 0;
}

Lng32 ExpParquetInterface::getFileStats
(char * rootDir, NAArray<HbaseStr> ** retFileStats)
{
  NAArray<HbaseStr>* fileStats = NULL;

  fileStats = pfr_->getFileStats((NAHeap *)heap_, rootDir);
  if (! fileStats)
    return -PFR_UNKNOWN_ERROR;
  else
    *retFileStats = fileStats;

  return 0;
}

Lng32 ExpParquetInterface::getFileAndBlockAttributes
(char * rootDir, NAArray<HbaseStr> ** retFileStats)
{
  NAArray<HbaseStr>* fileStats = NULL;

  fileStats = pfr_->getFileAndBlockAttributes((NAHeap *)heap_, rootDir);
  if (! fileStats)
    return -PFR_UNKNOWN_ERROR;
  else
    *retFileStats = fileStats;

  return 0;
}

Lng32 ExpParquetInterface::getFileSchema(char * tableName,
                                         char * rootDir, 
                                         TextVec *colNameVec,
                                         TextVec *colTypeVec,
                                         Lng32 flags,
                                         char* &readSchema, char* &writeSchema)
{
  PFR_RetCode rc = PFR_OK;
  rc = pfr_->getFileSchema((NAHeap *)heap_, tableName, rootDir, 
                           colNameVec, colTypeVec, flags,
                           readSchema, writeSchema);
  if (rc != PFR_OK)
    return -PFR_UNKNOWN_ERROR;

  return 0;
}

char * ExpParquetInterface::getErrorText(Lng32 errEnum)
{
  return pfr_->getErrorText((PFR_RetCode)errEnum);
}

Lng32 ExpParquetInterface::getSplitUnitInfo(char* path,
                                     LIST(Int64)& numOfRows,
                                     LIST(Int64)& offsets,
                                     LIST(Int64)& totalBytes)
{
  NAArray<HbaseStr> * retFileStats = NULL;
  Lng32 ret = getFileStats(path, &retFileStats);

  if ( ret == 0 && retFileStats != NULL ) {
    for (Int32 i=0; i<retFileStats->entries(); i++) {
      const HbaseStr& str = (*retFileStats)[i];

      // Format of str:
      //
      // path | file name | 
      // block name | file num | block_num
      // compressed size | total size | rowCount | startingPos
      //
      // Example:
      //
      // "/user/trafodion/hive/tpcds/store_sales_parquet | 000000_0
      // | null | 1 |
      // 81141726 | 81141726 | 1440185 | 4 |
      // \303\377\177",
      //
      char* delim = (char *)"|";
      char* tok = strtok(str.val, delim); //path
      tok = strtok(NULL, delim); // file name
      tok = strtok(NULL, delim); // block name
      tok = strtok(NULL, delim); // file num
      tok = strtok(NULL, delim); // block num
      tok = strtok(NULL, delim); // compressed size

      char* totalSize = strtok(NULL, delim); // total size
      totalBytes.insertAt(i, atol(totalSize));

      char* rowCount = strtok(NULL, delim); // row count
      numOfRows.insertAt(i, atol(rowCount));

      char* startingPos = strtok(NULL, delim); // starting pos
      offsets.insertAt(i, atol(startingPos));
    }
  } 

  if ( retFileStats )
     deleteNAArray(heap_, retFileStats);

  return ret;
}

Lng32 ExpParquetInterface::initStrawScan(char* webServers, char* queryId, Lng32 explainNodeId, bool isFactTable, NAText* entries, CollIndex entriesLength)
{
  TSU_RetCode rc = pfr_->initStrawScan(webServers, queryId, explainNodeId, isFactTable, entries, entriesLength);
  if (rc != TSU_OK)
    return -rc;
  return 0;
}
Lng32 ExpParquetInterface::getNextRangeNumStrawScan(char* webServers,
                                 char* queryId,
		  	  	  	  	  	     Lng32 explainNodeId,
		  	  	  	  	  	  	 Lng32 sequenceNb,
		  	  	  	  	  	     ULng32 executionCount,
								 Lng32 espNb,
								 Lng32 nodeId,
								 bool isFactTable,
								 Lng32& nextRangeNum)
{
  TSU_RetCode rc = pfr_->getNextRangeNumStrawScan(webServers,
                                                  queryId,
                                                  explainNodeId,
                                                  sequenceNb,
                                                  executionCount,
                                                  espNb,
                                                  nodeId,
                                                  isFactTable,
                                                  nextRangeNum);
  if (rc != TSU_OK)
    return -rc;
  return 0;
}
Lng32 ExpParquetInterface::freeStrawScan(char* queryId, Lng32 explainNodeId )
{
  TSU_RetCode rc = pfr_->freeStrawScan(queryId, explainNodeId );
  if (rc != TSU_OK)
    return -rc;
  return 0;
}

