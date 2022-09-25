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

#include "ExpORCinterface.h"
#include "ex_ex.h"

///////////////////////////////////////////////////////////
// class ExpExtStorageInterface
///////////////////////////////////////////////////////////
ExpExtStorageInterface::ExpExtStorageInterface(CollHeap* heap, 
                                                       const char* server, 
                                                       const Lng32 port,
                                                       StorageType st) :
     st_(st)
{
  heap_ = heap;

  if (server)
    strcpy(server_, server);
  else
    server_[0] = 0;

  port_ = port;
  isClosed_ = TRUE;
}

ExpExtStorageInterface::~ExpExtStorageInterface()
{}

////////////////////////////////////////////////////////////
// class ExpORCinterface
///////////////////////////////////////////////////////////
ExpORCinterface::ExpORCinterface(CollHeap * heap,
                                 const char * server,
                                 const Lng32 port, NABoolean vectoredRead) :
     ExpExtStorageInterface(heap, server, port, ORC_),
     ofr_(NULL)
   , ofvr_(NULL)
{
  if (vectoredRead) {
     ofvr_ = new(heap) OrcFileVectorReader((NAHeap*)heap);
     if (ofvr_->init() != JOI_OK) 
        ex_assert(false, "OrcFileReader::init method failed"); 
     return;
  } 
  ofr_ = new(heap) OrcFileReader((NAHeap*)heap);
  if (ofr_->init() != JOI_OK) 
    ex_assert(false, "OrcFileReader::init method failed"); 
}

ExpORCinterface* ExpORCinterface::newInstance(CollHeap* heap,
                                              const char* server,
                                              const Lng32 port, 
                                              NABoolean orcVectorizedScan)
{
  return new (heap) ExpORCinterface(heap, server, port, orcVectorizedScan);
}

ExpORCinterface::~ExpORCinterface()
{
  // close. Ignore errors.
  scanClose();
  if (ofr_ != NULL)  {
     NADELETE(ofr_, OrcFileReader, heap_);
     ofr_ = NULL;
  }
  if (ofvr_ != NULL)  {
     NADELETE(ofvr_, OrcFileVectorReader, heap_);
     ofvr_ = NULL;
  }
}

Lng32 ExpORCinterface::init()
{
  if (ofr_ != NULL)
     ofr_->init();
  if (ofvr_ != NULL)
     ofvr_->init();
  return 0;
}

Lng32 ExpORCinterface::open(ExHdfsScanStats *hdfsStats, char * tableName,
                            char * orcFileName,
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
  if (ofvr_ != NULL) {
     OFVR_RetCode rc = ofvr_->open(hdfsStats, tableName,
                                   orcFileName,
                                   maxVectorBatchSize,
                                   startRowNum, stopRowNum, 
                                   numCols, whichCols,
                                   ppiVec,
                                   ppiAllCols);
     if (rc != OFVR_OK)
        return -rc;
     isClosed_ = FALSE;
     return 0;
  }
  OFR_RetCode rc = ofr_->open(tableName,
                              orcFileName,
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
  if (rc != OFR_OK)
    return -rc;
  isClosed_ = FALSE;
  return 0;
}

Lng32 ExpORCinterface::scanOpen(
     ExHdfsScanStats *hdfsStats,
     char * tableName,
     char * orcFileName,
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
  Lng32 rc0 = open(hdfsStats, tableName, orcFileName, expectedRowSize,
                   maxRowsToFill, maxAllocationInMB, 
                   maxVectorBatchSize,
                   startRowNum, stopRowNum,
                   numCols, whichCols, ppiVec, ppiAllCols,
                   colNameVec, colTypeVec, parqSchStr, flags);
  if (rc0)
    {
      if (rc0 == -OFR_ERROR_FILE_NOT_FOUND_EXCEPTION)
        {
          startRowNum_ = -1;
          return 0;
        }
      
      return rc0;
    }

  // Since the actual value of startRowNum (aka offset) and stopRowNum(aka length)
  // have been used during the open call above, we do not need to use these two
  // variables here.
  startRowNum_ = 0; // startRowNum;
  currRowNum_  = startRowNum_;
  stopRowNum_  = -1; // stopRowNum;

  return 0;
}

Lng32 ExpORCinterface::close()
{
  if (isClosed_)
     return 0;
  if (ofvr_ != NULL) {
     OFVR_RetCode rc = ofvr_->close();
     if (rc != OFVR_OK)
        return -rc;
     isClosed_ = TRUE;
     return 0;
  }
  OFR_RetCode rc = ofr_->close();
  if (rc != OFR_OK)
    return -rc;
  isClosed_ = TRUE; 
  return 0;
}

Lng32 ExpORCinterface::scanClose()
{
  return close();
}

Lng32 ExpORCinterface::getColStats(Lng32 colNum, char * colName, Lng32 colType,
                                   NAArray<HbaseStr> **colStats)
{
  NAArray<HbaseStr> *retColStats = NULL;

  retColStats = ofr_->getColStats((NAHeap *)heap_, colNum, colName, colType);
  if (retColStats == NULL)
     return -OFR_UNKNOWN_ERROR;
  else
     *colStats = retColStats; 
  return 0;
}

Lng32 ExpORCinterface::getSumStringLengths(Int64 & result)
{
  return ofr_->getSumStringLengths(result);
}

Lng32  ExpORCinterface::getFileSchema(char * tableName,
                                      char * rootDir, 
                                      TextVec *colNameVec,
                                      TextVec *colTypeVec,
                                      Lng32 flags,
                                      char* &readSchema, char* &writeSchema)
{
  return -OFR_UNKNOWN_ERROR;
}

char * ExpORCinterface::getErrorText(Lng32 errEnum)
{
  if (ofvr_ != NULL)
     return ofvr_->getErrorText((OFVR_RetCode)errEnum);
  return ofr_->getErrorText((OFR_RetCode)errEnum);
}

Lng32 ExpORCinterface::getSplitUnitInfo(LIST(Int64)& numOfRows,
                                     LIST(Int64)& offsets,
                                     LIST(Int64)& totalBytes)
{
  OFR_RetCode rc = ofr_->getStripeInfo(numOfRows, offsets, totalBytes);
  if (rc != OFR_OK)
    return -rc;

  return 0;
}

Lng32 ExpORCinterface::initStrawScan(char* webServers, char* queryId, Lng32 explainNodeId, bool isFactTable, NAText* entries, CollIndex entriesLength)
{
  TSU_RetCode rc ;
  rc = ofvr_->initStrawScan(webServers, queryId, explainNodeId, isFactTable, entries, entriesLength);
  if (rc != TSU_OK)
    return -rc;
  return 0;
}

Lng32 ExpORCinterface::getNextRangeNumStrawScan(char* webServers,
                                 char* queryId,
		  	  	  	  	  	     Lng32 explainNodeId,
		  	  	  	  	  	  	 Lng32 sequenceNb,
		  	  	  	  	  	     ULng32 executionCount,
								 Lng32 espNb,
								 Lng32 nodeId,
								 bool isFactTable,
								 Lng32& nextRangeNum)
{
  TSU_RetCode rc;
  rc  = ofvr_->getNextRangeNumStrawScan(webServers,
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

Lng32 ExpORCinterface::freeStrawScan(char* queryId, Lng32 explainNodeId )
{
  TSU_RetCode rc;
  rc = ofvr_->freeStrawScan(queryId, explainNodeId );
  if (rc != TSU_OK)
    return -rc;
  return 0;
}

Lng32 ExpORCinterface::nextRow()
{
   Lng32 retcode;

   OFVR_RetCode rc = ofvr_->nextRow();
   if (rc == OFVR_NOMORE)
      retcode = 100;
   else if (rc == OFVR_OK)
      retcode = 0;
   else
      retcode = -rc;
   return retcode;
}

Lng32 ExpORCinterface::getBytesCol(int colNo, BYTE *colVal, Lng32 &colValLen, short &nullVal)
{
   Lng32 retcode;
 
   OFVR_RetCode rc = ofvr_->getBytesCol(colNo, colVal, colValLen, nullVal);
   if (rc == OFVR_OK)
      retcode = 0;
   else
      retcode = -rc;
   return retcode;
}

Lng32 ExpORCinterface::getNumericCol(int colNo, Int16 dataType, BYTE *colVal, Lng32 &colValLen, short &nullVal)
{
   Lng32 retcode;
 
   OFVR_RetCode rc = ofvr_->getNumericCol(colNo, dataType, colVal, colValLen, nullVal);
   if (rc == OFVR_OK)
      retcode = 0;
   else
      retcode = -rc;
   return retcode;
}

Lng32 ExpORCinterface::getDecimalCol(int colNo, Int16 dataType, BYTE *colVal, Lng32 &colValLen, short &nullVal,
                    char *tempStrVal, Lng32 &tempStrValLen, bool &convFromStr)
{
   Lng32 retcode;
 
   OFVR_RetCode rc = ofvr_->getDecimalCol(colNo, dataType, colVal, colValLen, nullVal, tempStrVal, tempStrValLen, convFromStr);
   if (rc == OFVR_OK)
      retcode = 0;
   else
      retcode = -rc;
   return retcode;
}

Lng32 ExpORCinterface::getDoubleOrFloatCol(int colNo, Int16 dataType, BYTE *colVal, Lng32 &colValLen, short &nullVal)
{
   Lng32 retcode;
 
   OFVR_RetCode rc = ofvr_->getDoubleOrFloatCol(colNo, dataType, colVal, colValLen, nullVal);
   if (rc == OFVR_OK)
      retcode = 0;
   else
      retcode = -rc;
   return retcode;
}

Lng32 ExpORCinterface::getDateOrTimestampCol(int colNo, Int16 datetimeCode, BYTE *colVal, Lng32 &colValLen, short &nullVal)
{
   Lng32 retcode;
 
   OFVR_RetCode rc = ofvr_->getDateOrTimestampCol(colNo, datetimeCode, colVal, colValLen, nullVal);
   if (rc == OFVR_OK)
      retcode = 0;
   else
      retcode = -rc;
   return retcode;
}

