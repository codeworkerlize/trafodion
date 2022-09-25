// **********************************************************************
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
// **********************************************************************
#ifndef COM_EXT_STORAGE_ACCESS_H
#define COM_EXT_STORAGE_ACCESS_H

#include "ComTdbHdfsScan.h"

class ComTdbExtPPI : public NAVersionedObject
{
public:
  ComTdbExtPPI(ExtPushdownOperatorType type, char * colName,
               Lng32 operAttrIndex,
	       NAString colType, 
               Int32 items,
               char* filterName,
               Int16 filterId,
               UInt64 rc,
               UInt64 uec,
               UInt64 rowsToSurvive
               )
       :  NAVersionedObject(-1),
          type_((short)type),
          colName_(colName),
          operAttrIndex_(operAttrIndex),
          colType_(colType),
          items_(items),
          isaNullTerm_(FALSE),
          filterName_(filterName),
          filterId_(filterId),
          rowCount_(rc),
          uec_(uec),
          rowsToSurvive_(rowsToSurvive)
  {}
  
  virtual unsigned char getClassVersionID()
  {
    return 1;
  }
  
  virtual void populateImageVersionIDArray()
  {
    setImageVersionID(0,getClassVersionID());
  }
  
  virtual short getClassSize() { return (short)sizeof(ComTdbExtPPI); }
  
  virtual Long pack(void * space);
  
  virtual Lng32 unpack(void * base, void * reallocator);
  
  ExtPushdownOperatorType type() { return (ExtPushdownOperatorType)type_; }
  char * colName() { return colName_; }
  NAString colType() { return colType_; }
  Lng32 operAttrIndex() { return operAttrIndex_; }

  char * filterName() { return filterName_; }
  Int16 getFilterId() { return filterId_; }
  UInt64 rowCount() { return rowCount_; }
  UInt64 uec() { return uec_; }

  UInt64 rowsToSurvive() { return rowsToSurvive_; }

  NABoolean isaNullTerm() { return isaNullTerm_; } ;
  void setIsaNullTerm(NABoolean x) { isaNullTerm_ = x; };

  void display(ostream&);

  Lng32 type_;
  Lng32 operAttrIndex_;
  NABasicPtr colName_;
  NAString colType_;
  Int32 items_;
  NABoolean isaNullTerm_; // indicator if this PPI contains a NULL
  NABasicPtr filterName_;
  Int16 filterId_;
  UInt64 rowCount_;
  UInt64 uec_;
  UInt64 rowsToSurvive_;
};

// ComTdbExtStorageAccess
class ComTdbExtStorageAccess : public ComTdbHdfsScan
{
  friend class ExHdfsScanTcb;
  friend class ExHdfsScanPrivateState;
  friend class ExExtStorageScanTcb;

public:

  // Constructor
  ComTdbExtStorageAccess(); // dummy constructor. Used by 'unpack' routines.
  
  ComTdbExtStorageAccess
  (
       char * tableName,
       short type,

       ex_expr * select_pred,
       ex_expr * move_expr,
       ex_expr * convert_expr,
       ex_expr * move_convert_expr,
       ex_expr * part_elim_expr,

       UInt16 convertSkipListSize,
       Int16 * convertSkipList,
       char * hdfsHostName,
       tPort  hdfsPort, 

       Queue * hdfsFileInfoList,
       Queue * hdfsFileRangeBeginList,
       Queue * hdfsFileRangeNumList,
       Queue * hdfsColInfoList,

       char recordDelimiter,
       char columnDelimiter,

       Int64 hdfsBufSize,
       UInt32 rangeTailIOSize,

       Int32 numPartCols,
       Int64 hdfsSqlMaxRecLen,
       Int64 outputRowLength,
       Int64 asciiRowLen,
       Int64 moveColsRowLen,
       Int32 partColsRowLength,
       Int32 virtColsRowLength,

       const unsigned short tuppIndex,
       const unsigned short asciiTuppIndex,
       const unsigned short workAtpIndex,
       const unsigned short moveColsTuppIndex,
       const unsigned short partColsTuppIndex,
       const unsigned short virtColsTuppIndex,

       ex_cri_desc * work_cri_desc,
       ex_cri_desc * given_cri_desc,
       ex_cri_desc * returned_cri_desc,
       queue_index down,
       queue_index up,
       Cardinality estimatedRowCount,
       Int32  numBuffers,
       UInt32  bufferSize,

       char * errCountTable = NULL,
       char * loggingLocation = NULL,
       char * errCountId = NULL,
       
       char * hdfsRoorDir = NULL,
       Int64 modTSforDir = -1,
       Lng32  numOfPartCols = -1,
       Queue * hdfsDirsToCheck = NULL,

       Queue * colNameList = NULL,
       Queue * colTypeList = NULL,

       char * parqSchStr = NULL
   );
  
  ~ComTdbExtStorageAccess();

private:
};

// ComTdbExtStorageScan
class ComTdbExtStorageScan : public ComTdbExtStorageAccess
{
  friend class ExHdfsScanTcb;
  friend class ExHdfsScanPrivateState;
  friend class ExExtStorageScanTcb;
  friend class ExpParquetArrowInterface;
  friend class Queue;

public:

  // Constructor
  ComTdbExtStorageScan(); // dummy constructor. Used by 'unpack' routines.
  
  ComTdbExtStorageScan
  (
       char * tableName,
       short type,
       ex_expr * select_pred,
       ex_expr * move_expr,
       ex_expr * convert_expr,
       ex_expr * move_convert_expr,
       ex_expr * part_elim_expr,
       ex_expr * extOperExpr,
       UInt16 convertSkipListSize,
       Int16 * convertSkipList,
       char * hdfsHostName,
       tPort  hdfsPort, 
       Queue * hdfsFileInfoList,
       Queue * hdfsFileRangeBeginList,
       Queue * hdfsFileRangeNumList,
       Queue * hdfsColInfoList,
       Queue * extAllColInfoList,
       char recordDelimiter,
       char columnDelimiter,
       Int64 hdfsBufSize,
       Int32 extMaxRowsToFill,
       Int32 extMaxAllocationInMB,
       UInt32 rangeTailIOSize,
       Int32 numPartCols,
       Queue * tdbListOfExtPPI,
       Int64 hdfsSqlMaxRecLen,
       Int64 outputRowLength,
       Int64 asciiRowLen,
       Int64 moveColsRowLen,
       Int32 partColsRowLength,
       Int32 virtColsRowLength,
       Int64 extOperLength,
       const unsigned short tuppIndex,
       const unsigned short asciiTuppIndex,
       const unsigned short workAtpIndex,
       const unsigned short moveColsTuppIndex,
       const unsigned short partColsTuppIndex,
       const unsigned short virtColsTuppIndex,
       const unsigned short extOperTuppIndex,
       ex_cri_desc * work_cri_desc,
       ex_cri_desc * given_cri_desc,
       ex_cri_desc * returned_cri_desc,
       queue_index down,
       queue_index up,
       Cardinality estimatedRowCount,
       Int32  numBuffers,
       UInt32  bufferSize,
       char * errCountTable = NULL,
       char * loggingLocation = NULL,
       char * errCountId = NULL,

       char * hdfsRoorDir = NULL,
       Int64 modTSforDir = -1,
       Lng32  numOfPartCols = -1,
       Queue * hdfsDirsToCheck = NULL,

       Queue * colNameList = NULL,
       Queue * colTypeList = NULL,

       NABoolean removeMinMaxFromScanFilter = FALSE,
       float maxSelectivity = 0.0,
       Int32 maxFilters = 0,
       NABoolean useArrowReader = FALSE,
       NABoolean parquetFullScan = FALSE,
       Int32 parquetScanParallelism = 1,
       NABoolean skipColumnChunkStatscheck = FALSE,
       NABoolean filterRowgroups = TRUE,
       NABoolean filterPages = TRUE,
       NABoolean directCopy = TRUE,
       char * parqSchStr = NULL
   );
  
  ~ComTdbExtStorageScan();

  Long pack(void *);

  Lng32 unpack(void *, void * reallocator);
 
  virtual const char *getNodeName() const { return "EX_EXT_STORAGE_SCAN"; };

  virtual Int32 numExpressions() const 
  { return ComTdbExtStorageAccess::numExpressions() + 1; }

  virtual ex_expr* getExpressionNode(Int32 pos) {
    if (pos < ComTdbExtStorageAccess::numExpressions())
      return ComTdbExtStorageAccess::getExpressionNode(pos);
    else
      return extOperExpr_;
  }
  
  virtual const char * getExpressionName(Int32 pos) const {
    if (pos < ComTdbExtStorageAccess::numExpressions())
      return ComTdbExtStorageAccess::getExpressionName(pos);
    else
      return "extOperExpr_";
  }

  virtual void displayContents(Space *space,ULng32 flag);

  Queue* listOfExtPPI() const { return listOfExtPPI_; }
  Queue* extAllColInfoList() { return extAllColInfoList_; }

  void setOrcVectorizedScan(NABoolean v)
  {(v ? flags_ |= ORC_VECTORIZED_SCAN : flags_ &= ~ORC_VECTORIZED_SCAN); };
  NABoolean orcVectorizedScan() { return (flags_ & ORC_VECTORIZED_SCAN) != 0; };

  void setParquetLegacyTS(NABoolean v)
  {(v ? flags_ |= PARQUET_LEGACY_TS : flags_ &= ~PARQUET_LEGACY_TS); };
  NABoolean parquetLegacyTS() { return (flags_ & PARQUET_LEGACY_TS) != 0; };

  void setParquetSignedMinMax(NABoolean v)
  {(v ? flags_ |= PARQUET_SIGNED_MIN_MAX : flags_ &= ~PARQUET_SIGNED_MIN_MAX); };
  NABoolean parquetSignedMinMax() { return (flags_ & PARQUET_SIGNED_MIN_MAX) != 0; };

  NABoolean removeMinMaxFromScanFilter() 
        { return (flags_ & REMOVE_MIN_MAX ) != 0; };

  void setRemoveMinMaxFromScanFilter(NABoolean v) 
    {(v ? flags_ |= REMOVE_MIN_MAX : flags_ &= ~REMOVE_MIN_MAX); };

  NABoolean skipColumChunkStatsCheck() 
        { return (flags_ & PARQUET_CPP_SKIP_COLUMN_STATS_CHECK) != 0; };

  void setSkipColumnChunkStatsCheck(NABoolean v) 
    {(v ?  
       flags_ |= PARQUET_CPP_SKIP_COLUMN_STATS_CHECK : 
       flags_ &= ~PARQUET_CPP_SKIP_COLUMN_STATS_CHECK); };

  NABoolean filterRowgroups() 
        { return (flags_ & PARQUET_CPP_FILTER_ROWGROUPS) != 0; };

  void setFilterRowgroups(NABoolean v) 
    {(v ? flags_ |= PARQUET_CPP_FILTER_ROWGROUPS: flags_ &= ~PARQUET_CPP_FILTER_ROWGROUPS); };

  NABoolean filterPages() 
        { return (flags_ & PARQUET_CPP_FILTER_PAGES) != 0; };

  void setFilterPages(NABoolean v) 
    {(v ? flags_ |= PARQUET_CPP_FILTER_PAGES: flags_ &= ~PARQUET_CPP_FILTER_PAGES); };

  NABoolean directCopy() 
        { return (flags_ & PARQUET_CPP_DIRECT_COPY) != 0; };

  void setDirectCopy(NABoolean v) 
    {(v ? flags_ |= PARQUET_CPP_DIRECT_COPY: flags_ &= ~PARQUET_CPP_DIRECT_COPY); };

  float maxSelectivity() { return maxSelectivity_; }
  Int32 maxFilters() { return maxFilters_; } 

  void setVectorBatchSize(Int32 batchSize)
  { vectorBatchSize_ = batchSize; }

  Int32 getVectorBatchSize()
  { return vectorBatchSize_; }

  Int32 parquetScanParallelism() { return parquetScanParallelism_; } 

private:
  enum
    {
      ORC_VECTORIZED_SCAN = 0x0001,
      PARQUET_LEGACY_TS   = 0x0002,
      PARQUET_SIGNED_MIN_MAX = 0x0004,
      REMOVE_MIN_MAX      = 0x0008,
      PARQUET_CPP_SKIP_COLUMN_STATS_CHECK = 0x0010,
      PARQUET_CPP_FILTER_ROWGROUPS = 0x0020,
      PARQUET_CPP_FILTER_PAGES = 0x0040,
      PARQUET_CPP_DIRECT_COPY = 0x0080
    };

  QueuePtr listOfExtPPI_;

  ExExprPtr extOperExpr_; 

  QueuePtr extAllColInfoList_;

  UInt16 extOperTuppIndex_;  

  UInt16 flags_;

  Int32 extOperLength_;

  Int32 extMaxRowsToFill_;
  Int32 extMaxAllocationInMB_;

  float maxSelectivity_; 
  Int32 maxFilters_;  

  Int32 vectorBatchSize_;

  Int32 parquetScanParallelism_;  
};

// ComTdbExtStorageFastAggr 
class ComTdbExtStorageFastAggr : public ComTdbExtStorageScan
{
  friend class ExExtStorageFastAggrTcb;

public:
  enum ExtStorageAggrType
    {
      UNKNOWN_       = 0,
      COUNT_         = 1,
      COUNT_NONULL_  = 2,
      MIN_           = 3,
      MAX_           = 4,
      SUM_           = 5,
      ORC_NV_LOWER_BOUND_ = 6,
      ORC_NV_UPPER_BOUND_ = 7
    };

  // Constructor
  ComTdbExtStorageFastAggr(); // dummy constructor. Used by 'unpack' routines.
  
  ComTdbExtStorageFastAggr(
       char * tableName,
       short type,
       Queue * hdfsFileInfoList,
       Queue * hdfsFileRangeBeginList,
       Queue * hdfsFileRangeNumList,
       Queue * listOfAggrTypes,
       Queue * listOfAggrColInfo,
       ex_expr * aggr_expr,
       Int32 finalAggrRowLen,
       const unsigned short finalAggrTuppIndex,
       Int32 extAggrRowLen,
       const unsigned short extAggrTuppIndex,  
       ex_expr * having_expr,
       ex_expr * proj_expr,
       Int64 projRowLen,
       const unsigned short projTuppIndex,
       const unsigned short returnedTuppIndex,
       ex_cri_desc * work_cri_desc,
       ex_cri_desc * given_cri_desc,
       ex_cri_desc * returned_cri_desc,
       queue_index down,
       queue_index up,
       Int32  numBuffers,
       UInt32  bufferSize,
       Int32 numPartCols
                           );
  
  ~ComTdbExtStorageFastAggr();

  Queue* getAggrTypeList() {return aggrTypeList_;}

  virtual short getClassSize() { return (short)sizeof(ComTdbExtStorageFastAggr); }  

  virtual const char *getNodeName() const { return "EX_EXT_STORE_FAST_AGGR"; };

  virtual Int32 numExpressions() const 
  { return (ComTdbHdfsScan::numExpressions() + 1); }

  virtual ex_expr* getExpressionNode(Int32 pos) {
    if (pos < ComTdbHdfsScan::numExpressions())
      return ComTdbHdfsScan::getExpressionNode(pos);

    if (pos ==  ComTdbHdfsScan::numExpressions())
      return aggrExpr_;

    return NULL;
  }
  
  virtual const char * getExpressionName(Int32 pos) const {
    if (pos < ComTdbHdfsScan::numExpressions())
      return ComTdbHdfsScan::getExpressionName(pos);

    if (pos ==  ComTdbHdfsScan::numExpressions())
      return "aggrExpr_";

    return NULL;
  }

  virtual void displayContents(Space *space,ULng32 flag);
 
  Long pack(void *);

  Lng32 unpack(void *, void * reallocator);

  AggrExpr * aggrExpr() { return (AggrExpr*)((ex_expr*)aggrExpr_); }

private:
  QueuePtr aggrTypeList_;   

  ExExprPtr aggrExpr_;

  // max length of ext storage aggr row.
  Int32 extAggrRowLength_;
  Int32 finalAggrRowLength_;

  UInt16 extAggrTuppIndex_;

  char filler_[6];
};

#endif

