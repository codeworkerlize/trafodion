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

#include "ExpAvroInterface.h"
#include "ex_ex.h"

ExpAvroInterface* ExpAvroInterface::newInstance(CollHeap* heap,
                                                      const char* server,
                                                      const Lng32 port)
{
  return new (heap) ExpAvroInterface(heap, server, port);
}

ExpAvroInterface::ExpAvroInterface(CollHeap * heap,
                                         const char * server,
                                         const Lng32 port) :
     ExpExtStorageInterface(heap, server, port, AVRO_),
     pfr_(NULL)
{
  pfr_ = new(heap) AvroFileReader((NAHeap*)heap);
  if (pfr_->init() != JOI_OK) 
    ex_assert(false, "AvroFileReader::init method failed"); 
}

ExpAvroInterface::~ExpAvroInterface()
{
  // close. Ignore errors.
  scanClose();
  NADELETE(pfr_, AvroFileReader, heap_);
}

Lng32 ExpAvroInterface::init()
{
  pfr_->init();

  return 0;
}

Lng32 ExpAvroInterface::open(ExHdfsScanStats *hdfsStats, char * tableName,
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
  AFR_RetCode rc = 
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
                  flags);
  if (rc != AFR_OK)
    return -rc;

  return 0;
}

Lng32 ExpAvroInterface::scanOpen(
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
      if (rc0 == -AFR_ERROR_FILE_NOT_FOUND_EXCEPTION)
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

Lng32 ExpAvroInterface::scanFetch(char** row, Int64 &rowLen, Int64 &rowNum,
                                 Lng32 &numCols,
				 ExHdfsScanStats *hss
				 )
{ 
  if ((startRowNum_ == -1) ||
      ((stopRowNum_ != -1) && (currRowNum_ > stopRowNum_)))
    return 100;
  
  AFR_RetCode rc = pfr_->fetchNextRow(row, rowLen, rowNum, numCols, hss);
  if (rc == AFR_NOMORE)
    return 100;
  
  if (rc != AFR_OK)
    return -rc;
  
  if ((rowLen < 0) || (numCols < 0))
    return -AFR_UNKNOWN_ERROR;

  currRowNum_++;
  
  return 0;
}

Lng32 ExpAvroInterface::close()
{
  AFR_RetCode rc = pfr_->close();
  if (rc != AFR_OK)
    return -rc;
 
  return 0;
}

Lng32 ExpAvroInterface::scanClose()
{
  return close();
}

Lng32 ExpAvroInterface::getColStats(Lng32 colNum, char * colName, Lng32 colType,
                                       NAArray<HbaseStr> **colStats)
{
  NAArray<HbaseStr> *retColStats = NULL;

  retColStats = pfr_->getColStats((NAHeap *)heap_, colNum, colName, colType);
  if (retColStats == NULL)
     return -AFR_UNKNOWN_ERROR;
  else
     *colStats = retColStats; 
  return 0;
}

Lng32 ExpAvroInterface::getFileStats
(char * rootDir, NAArray<HbaseStr> ** retFileStats)
{
  NAArray<HbaseStr>* fileStats = NULL;

  fileStats = pfr_->getFileStats((NAHeap *)heap_, rootDir);
  if (! fileStats)
    return -AFR_UNKNOWN_ERROR;
  else
    *retFileStats = fileStats;

  return 0;
}

Lng32 ExpAvroInterface::getFileSchema(char * tableName,
                                      char * rootDir, 
                                      TextVec *colNameVec,
                                      TextVec *colTypeVec,
                                      Lng32 flags,
                                      char* &readSchema, char* &writeSchema)
{
  AFR_RetCode rc = AFR_OK;
  rc = pfr_->getFileSchema((NAHeap *)heap_, tableName, rootDir, 
                           colNameVec, colTypeVec, flags,
                           readSchema, writeSchema);
  if (rc != AFR_OK)
    return -AFR_UNKNOWN_ERROR;

  return 0;
}

char * ExpAvroInterface::getErrorText(Lng32 errEnum)
{
  return pfr_->getErrorText((AFR_RetCode)errEnum);
}

Lng32 ExpAvroInterface::initStrawScan(char* webServers, char* queryId, Lng32 explainNodeId, bool isFactTable, NAText* entries, CollIndex entriesLength)
{
  TSU_RetCode rc = pfr_->initStrawScan(webServers, queryId, explainNodeId, isFactTable, entries, entriesLength);
  if (rc != TSU_OK)
    return -rc;
  return 0;
}
Lng32 ExpAvroInterface::getNextRangeNumStrawScan(char* webServers,
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
  if (rc !=TSU_OK)
    return -rc;
  return 0;
}
Lng32 ExpAvroInterface::freeStrawScan(char* queryId, Lng32 explainNodeId )
{
  TSU_RetCode rc = pfr_->freeStrawScan(queryId, explainNodeId );
  if (rc != TSU_OK)
    return -rc;
  return 0;
}

