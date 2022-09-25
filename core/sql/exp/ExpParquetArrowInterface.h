/* -*-C++-*-
****************************************************************************
*
* File:             ExpParquetArrowInterface.h
* Description:  Interface to Parquet world
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

#ifndef EXP_PARQUET_ARROW_INTERFACE_H
#define EXP_PARQUET_ARROW_INTERFACE_H

#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/time.h>
#include <iostream>


#include "Platform.h"
#include "Collections.h"
#include "NABasicObject.h"

#include "ExpParquetInterface.h"

#include <fstream>
#include <list>
#include <memory>
#include <sys/time.h>
#include "metadata.h"
#include "arrow/reader.h"
#include "arrow/table.h"
#include "arrow/io/arrow_hdfs.h"

#include <hdfs.h>
#include "arrow/io/file.h"
#include "arrow/util/bit-util.h"


class Attributes;

class ResultVisitor
{
public:
   ResultVisitor() : pos_(-1) {};
   virtual ~ResultVisitor() {};

   virtual void advance(int64_t newPos) { pos_ = newPos; }
   virtual Lng32 convertToTrafodion(char* target, Int64& len, std::string& coName) = 0;

   virtual Lng32 getBytesCol(int colNo, BYTE *colVal, Lng32 &colValLen, 
                     short &nullVal) = 0;
   virtual Lng32 getNumericCol(int colNo, Int16 dataType, BYTE *colVal, 
                       Lng32 &colValLen, short &nullVal) = 0;
   virtual Lng32 getDecimalCol(int colNo, Int16 dataType, BYTE *colVal, 
                       Lng32 &colValLen, short &nullVal,
                       char *tempStrVal, Lng32 &tempStrValLen, 
                       bool &convFromStr) = 0;
   virtual Lng32 getDoubleOrFloatCol(int colNo, Int16 dataType, BYTE *colVal, 
                       Lng32 &colValLen, short &nullVal) = 0;
   virtual Lng32 getDateOrTimestampCol(int colNo, Int16 datetimeCode, 
                        BYTE *colVal, Lng32 &colValLen, short &nullVal) = 0;

protected:
   inline bool processNull(bool isNull, int32_t& colValLen, short int& nullVal)
   {
     if ( isNull ) {
        colValLen = -1; nullVal = -1;   
     } else {
        nullVal = 0;   
     }
     return isNull;
   }


   Lng32 getBytesColImp(std::shared_ptr<arrow::Array> chk,
                        BYTE *colVal, Lng32 &colValLen);
   Lng32 getNumericColImp(std::shared_ptr<arrow::Array> chk,
                       Int16 dataType, BYTE *colVal, Lng32 &colValLen);
   Lng32 getDecimalColImp(std::shared_ptr<arrow::Array> chk, 
                       Int16 dataType, BYTE *colVal,
                       Lng32 &colValLen,
                       char *tempStrVal, Lng32 &tempStrValLen, 
                       bool &convFromStr);
   Lng32 getDoubleOrFloatColImp(std::shared_ptr<arrow::Array> chk,
                       Int16 dataType, BYTE *colVal, Lng32 &colValLen);
   Lng32 getDateOrTimestampColImp(std::shared_ptr<arrow::Array> chk,
                       Int16 datetimeCode, 
                       BYTE *colVal, Lng32 &colValLen);


   virtual Lng32 doConversionToTrafodion(
        std::shared_ptr<arrow::Array> chk,
        std::shared_ptr<arrow::DataType> type,
        const std::string& name,
        char* target, Int64& len);

protected:
   Lng32 getTimestampColImp(std::shared_ptr<arrow::Array> chk,
                            BYTE *colVal, Lng32 &colValLen);

protected:
   Int64 pos_;      // current position within the current chunk.
};

class TableVisitor : public ResultVisitor
{
public:
   TableVisitor(std::shared_ptr<arrow::Table> table) :
         table_(table) {};
   ~TableVisitor() {};

   Lng32 convertToTrafodion(char* target, Int64& len, std::string& coName);

   Lng32 getBytesCol(int colNo, BYTE *colVal, Lng32 &colValLen, 
                     short &nullVal);
   Lng32 getNumericCol(int colNo, Int16 dataType, BYTE *colVal, 
                       Lng32 &colValLen, short &nullVal);
   Lng32 getDecimalCol(int colNo, Int16 dataType, BYTE *colVal, 
                       Lng32 &colValLen, short &nullVal,
                       char *tempStrVal, Lng32 &tempStrValLen, 
                       bool &convFromStr);
   Lng32 getDoubleOrFloatCol(int colNo, Int16 dataType, BYTE *colVal, 
                       Lng32 &colValLen, short &nullVal);
   Lng32 getDateOrTimestampCol(int colNo, Int16 datetimeCode, 
                        BYTE *colVal, Lng32 &colValLen, short &nullVal);

protected:
   std::shared_ptr<arrow::Table> table_;
};

class RecordBatchVisitor : public ResultVisitor
{
public:
   RecordBatchVisitor(std::shared_ptr<arrow::RecordBatch> batch) :
         batch_(batch) {};
   ~RecordBatchVisitor() {};

   Lng32 convertToTrafodion(char* target, Int64& len, std::string& coName);

   Lng32 getBytesCol(int colNo, BYTE *colVal, Lng32 &colValLen, 
                     short &nullVal);
   Lng32 getNumericCol(int colNo, Int16 dataType, BYTE *colVal, 
                       Lng32 &colValLen, short &nullVal);
   Lng32 getDecimalCol(int colNo, Int16 dataType, BYTE *colVal, 
                       Lng32 &colValLen, short &nullVal,
                       char *tempStrVal, Lng32 &tempStrValLen, 
                       bool &convFromStr);
   Lng32 getDoubleOrFloatCol(int colNo, Int16 dataType, BYTE *colVal, 
                       Lng32 &colValLen, short &nullVal);
   Lng32 getDateOrTimestampCol(int colNo, Int16 datetimeCode, 
                        BYTE *colVal, Lng32 &colValLen, short &nullVal);

protected:
   std::shared_ptr<arrow::RecordBatch> batch_;
};

enum FilterType { PAGE, VALUE };

typedef std::vector<bool> maskVecType;

class ExpParquetArrowInterface : public ExpParquetInterface 
{
 public:
  static ExpParquetArrowInterface* newInstance(CollHeap* heap, 
                                         NABoolean fullScan = FALSE);
  
  ExpParquetArrowInterface(CollHeap* heap); 
                    
  virtual ~ExpParquetArrowInterface();
  
  virtual Lng32 init() { return 0; };
  
  //////////////////////////////////////////////////////////////////
  // tableName:  name of parquet table
  // fileName:   location and name of parquet file
  // expectedRowSize: expected size of rows read from parquet
  // maxRowsToFill: maximum number of rows to read per block fetch
  // maxAllocationInMB: max allocation in MB of buffer to read rows
  // startRowNum: first rownum to be returned. 
  // stopRowNum:  last rownum to be returned
  //    Rownums start at 1 and stop at N. If N is -1, 
  //   then all rows are to be returned.
  // 
  // numCols   : Number of columns to be returned 
  //             set it to -1 to get all the columns
  //
  // whichCol  : Array containing the column numbers to be returned
  //            (the column numbers are 1-based)
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
       Int32 numThreads = 1);
  
  // row:      pointer to buffer where Parquet will return the row.
  //           The buffer is allocated and maintained by this class.
  //           Row format: 4-bytes len followed by len bytes data 
  //                       for each col returned.
  //                       Len of zero indicates NULL value.
  // rowLen:   On input, length of allocated parquetRow.
  //           On output, length of row returned
  // rowNum:  rownum of returned row. Must be >= startRowNum 
  //          and <= stopRowNum.
  // numCols: number of columns that are part of this row.
 //
  // Return Code: 0, if a row is returned
  //              <0 if error
  //             100: EOD
  //
  Lng32 scanFetch(
                  char** row,
		  Int64 &rowLen, 
		  Int64 &rowNum, 
		  Lng32 &numCols,
		  ExHdfsScanStats *pv_hss
		 );

  Lng32 getBytesCol(int colNo, BYTE *colVal, Lng32 &colValLen, 
                    short &nullVal);
  Lng32 getNumericCol(int colNo, Int16 dataType, BYTE *colVal, 
                      Lng32 &colValLen, short &nullVal);
  Lng32 getDecimalCol(int colNo, Int16 dataType, BYTE *colVal, 
                      Lng32 &colValLen, short &nullVal,
                      char *tempStrVal, Lng32 &tempStrValLen, 
                      bool &convFromStr);
  Lng32 getDoubleOrFloatCol(int colNo, Int16 dataType, BYTE *colVal, 
                      Lng32 &colValLen, short &nullVal);
  Lng32 getDateOrTimestampCol(int colNo, Int16 datetimeCode, 
                       BYTE *colVal, Lng32 &colValLen, short &nullVal);

  virtual Lng32 scanClose();

  char * getErrorText(Lng32 errEnum);

  virtual NABoolean noMoreData() 
    { return ( remainingRowsToRead_  == 0 ); } 

  std::shared_ptr<arrow::io::HadoopFileSystem> getHdfsClient() 
     { return hdfsClient_; }

  void setHdfsClient(std::shared_ptr<arrow::io::HadoopFileSystem> x)
     { hdfsClient_ = x; }

  void updateStatsForScanFilters();

protected:
  virtual Lng32 prefetchIfNeeded() = 0;
  virtual int32_t numIOs() = 0;
  virtual void resetNumIOs() = 0;

  Lng32 setupFilters( const char* tablename, const char* filename,
                     TextVec* ppiVec, TextVec *colNameVec, 
                     TextVec *colTypeVec,
                     std::vector<UInt64>* expectedRowsVec
                    );

  void* buildFilter(
            OrcSearchArgVec& preds, maskVecType& masks, 
            const EncodedHiveType& colType, 
            FilterType ftype, Lng32& errorCode);

  virtual void computeRowGroups(const Int64 startOffset, const Int64 length);

  Int32 findIndexForNarrowestColumn(TextVec *colTypeVec);

  void setColNameForError(const char* name, int32_t len);
  void setColNameForError(const std::string& name);

  virtual void releaseResources();
  virtual void releaseResourcesForCurrentBatch();
    
  // A CQD controlled switch, default ON, good for debugging.
  NABoolean applyRowGroupFilters() const { return filterRowgroups_; }
  NABoolean applyPageFilters() const { return filterPages_; }

  Lng32 nextRow() { return advanceToNextRow(); }

  Lng32 advanceToNextRow(); // called before fetching the next row
  void doneNextRow(Lng32 totalRowLen);  // called after fetching the next row

protected:
  std::shared_ptr<parquet::FileMetaData> metadata_;
  std::unique_ptr<parquet::arrow::FileReader> reader_;
  std::shared_ptr<arrow::io::HadoopFileSystem> hdfsClient_;

  // All columns to fetch
  std::vector<int> columnsToReturn_;


     
  // A set of filters to be applied on some columns. The index in
  // the vector is by all columns in the table, not by the columns 
  // in the set to be returned.
  std::vector<Filters> filters_;

  // A visitor to traverse, row-wise, the values to be returned.
  ResultVisitor* visitor_; 

  ::arrow::Status arrowStatus_;

  Lng32 rowBufferLen_;
  char* rowBuffer_;

  char errorMsgBuffer_[1024];

  uint8_t* mergedSelectedBits_;

  arrow::internal::BitmapReader selectedBitsReader_;

  // total rows to available to read (after filtering)
  Int64 totalRowsToRead_;  

  // remaining rows to read (after filtering)
  Int64 remainingRowsToRead_;  

  // total rows available to read (before filtering)
  Int64 totalRowsToAccess_;  

  // The name of the current table and file to be scanned.
  std::string  tableName_;
  std::string fileName_;

  // for error reporting
  std::string colName_;

  // whether to skip column chunk stats check, see PARQUET-686
  NABoolean skipCCStatsCheck_;

  // whether to apply row group filters
  NABoolean filterRowgroups_;

  // whether to apply page filters
  NABoolean filterPages_;

  ExHdfsScanStats *hss_;

  std::vector<int> rowGroups_; // row groups to fetch

};

class ExpParquetArrowInterfaceTable : public ExpParquetArrowInterface
{
public:
  ExpParquetArrowInterfaceTable(CollHeap* heap)
  : ExpParquetArrowInterface(heap), 
    resultSet_(nullptr),
    numReadCalls_(0)
  {}
   
 ~ExpParquetArrowInterfaceTable() {};

 Lng32 scanClose();

protected:
 Lng32 prefetchIfNeeded();
 int32_t numIOs() { return numReadCalls_; }
 void resetNumIOs() { numReadCalls_ = 0; };
 void releaseResources();

protected:
  // The result set for a single file read operation
  std::shared_ptr<arrow::Table> resultSet_; 

  int32_t numReadCalls_;
};

class ExpParquetArrowInterfaceBatch : public ExpParquetArrowInterface
{
public:
  ExpParquetArrowInterfaceBatch (CollHeap* heap)
  : ExpParquetArrowInterface(heap),
    batch_(nullptr),
    batchReader_(nullptr),
    numBatchesRead_(0)
  {}
   
 ~ExpParquetArrowInterfaceBatch() {};

 Lng32 scanClose();

 NABoolean noMoreData();

protected:
 Lng32 prefetchIfNeeded();
 Lng32 readNextBatch();

 int32_t numIOs() { return numBatchesRead_; }
 void resetNumIOs() { numBatchesRead_ = 0; };

 void releaseResources();
 void releaseResourcesForCurrentBatch();

protected:

  // The result set for a rowGroup set read operation
  std::shared_ptr<arrow::RecordBatch> batch_;
  std::shared_ptr<::arrow::RecordBatchReader> batchReader_;

  int32_t numBatchesRead_;
};

#endif
