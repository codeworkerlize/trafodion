/* -*-C++-*-
****************************************************************************
*
* File:             ExpAvroInterface.h
* Description:  Interface to Avro world
* Created:       
* Language:      C++
*
*
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
*
****************************************************************************
*/

#ifndef EXP_AVRO_INTERFACE_H
#define EXP_AVRO_INTERFACE_H

#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/time.h>
#include <iostream>

#include "Platform.h"
#include "Collections.h"
#include "NABasicObject.h"

#include "ExpORCinterface.h"
#include "AvroFileReader.h"

class ExpAvroInterface : public ExpExtStorageInterface
{
 public:
  static ExpAvroInterface* newInstance(CollHeap* heap, 
                                          const char* server = NULL, 
                                          const Lng32 port = -1);
  
  ExpAvroInterface(CollHeap* heap, 
                      const char* server, 
                      const Lng32 port);
                    
  ~ExpAvroInterface();
  
  Lng32 init();
  
  //////////////////////////////////////////////////////////////////
  // tableName:  name of avro table
  // filePath:   location and name of avro file
  // expectedRowSize: expected size of rows read from avro
  // maxRowsToFill: maximum number of rows to read per block fetch
  // maxAllocationInMB: max allocation in MB of buffer to read rows
  // startRowNum: first rownum to be returned. 
  // stopRowNum:  last rownum to be returned
  //    Rownums start at 1 and stop at N. If N is -1, 
  //   then all rows are to be returned.
  // 
  // numCols   : Number of columns to be returned 
  //                         set it to -1 to get all the columns
  //
  // whichCol            : array containing the column numbers to be returned
  //                        (Column numbers are zero based)
  //
  // ppiBuflen:   length of buffer containing PPI (pred pushdown info)
  // ppiBuf:      buffer containing PPI
  // Format of data in ppiBuf:
  //   <numElems><type><nameLen><name><numOpers><opValLen><opVal>... 
  //    4-bytes    4B     4B      nlB     4B         4B      ovl B
  //////////////////////////////////////////////////////////////////

  Lng32 scanOpen(
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
       Lng32 *whichCols,
       TextVec *ppiVec,
       TextVec *ppiAllCols,
       TextVec *colNameVec,
       TextVec *colTypeVec,
       std::vector<UInt64>* expectedRowsVec,
       char * parqSchStr = NULL,
       Lng32 flags = 0,
       Int32 numThreads = 1
       );
  
  // avroRow:   pointer to buffer where Avro will return the row.
  //                Buffer is allocated by caller.
  //                Row format: 4-bytes len followed by len bytes data for each col returned.
  //                                   Len of zero indicates NULL value.
  // expectedRowSize: expected size of rows read from avro
  // maxRowsToFill: maximum number of rows to read per block fetch
  // maxAllocationInMB: max allocation in MB of buffer to read rows
  // rowLen:   On input, length of allocated avroRow.
  //                On output, length of row returned
  // rowNum:  rownum of returned row. Must be >= startRowNum and <= stopRowNum.
  // numCols: number of columns that are part of this row.
 //
  // Return Code:
  //                          0, if a row is returned
  //                         -ve num, if error
  //                          100: EOD
  //
  Lng32 scanFetch(char** row, 
		  Int64 &rowLen, 
		  Int64 &rowNum, 
		  Lng32 &numCols,
		  ExHdfsScanStats *pv_hss
		  );

  Lng32 scanClose();

  Lng32 open(ExHdfsScanStats *hdfsStats, char * tableName,
             char * fileName,
             const int expectedRowSize,
             const int maxRowsToFill,
             const int maxAllocationInMB,
             const int maxVectorBatchSize = 0,
             const Int64 startRowNum = 0, 
             const Int64 stopRowNum = LLONG_MAX,
             Lng32 numCols = 0,
             Lng32 * whichCols = NULL,
             TextVec *ppiVec = NULL,
             TextVec *ppiAllCols = NULL,
             TextVec *colNameVec = NULL,
             TextVec *colTypeVec = NULL,
             char * parqSchStr = NULL,
             Lng32 flags = 0);

  Lng32 close();

  char * getErrorText(Lng32 errEnum);

  Lng32 getColStats(Lng32 colNum, char * colName, Lng32 colType,
                    NAArray<HbaseStr> **colStats);

  Lng32 getFileStats(char * rootDir, NAArray<HbaseStr> ** retFileStats);

  Lng32 getFileSchema(char * tableName,
                      char * rootDir, 
                      TextVec *colNameVec,
                      TextVec *colTypeVec,
                      Lng32 flags,
                      char* &readSchema, char* &writeSchema);

  // send list of stripes to littleJetty for a given queryId
  Lng32 initStrawScan(char* webServers, char* queryId, Lng32 explainNodeId, bool isFactTable, NAText* entries, CollIndex entriesLength);
  // get next RangeNb for a given queryId+ExecutionCount
  Lng32 getNextRangeNumStrawScan(char* webServers,
                                     char* queryId,
    		  	  	  	  	  	     Lng32 explainNodeId,
    		  	  	  	  	  	  	 Lng32 sequenceNb,
    		  	  	  	  	  	     ULng32 executionCount,
    								 Lng32 espNb,
    								 Lng32 nodeId,
    								 bool isFactTable,
									 Lng32& nextRangeNum);
  //clean up resources on littleJetty side associated with queryId/fragmentId
  Lng32 freeStrawScan(char* queryId, Lng32 explainNodeId );

protected:
    
private:
  AvroFileReader * pfr_;
 };

#endif
