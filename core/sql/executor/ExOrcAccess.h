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
#ifndef EX_EXT_STORAGE_ACCESS_H
#define EX_EXT_STORAGE_ACCESS_H

//
// Task Definition Block
//
#include "ComTdbOrcAccess.h"
#include "ExHdfsScan.h"
#include "Timer.h"

// -----------------------------------------------------------------------
// Classes defined in this file
// -----------------------------------------------------------------------
class ExExtStorageScanTdb;
class ExExtStorageScanTcb;
#ifdef NEED_PSTATE
class ExBlockingHdfsScanPrivateState;
#endif

// -----------------------------------------------------------------------
// Classes referenced in this file
// -----------------------------------------------------------------------
class ex_tcb;
class SequenceFileReader;
class ExpExtStorageInterface;

// -----------------------------------------------------------------------
// ExExtStorageScanTdb
// -----------------------------------------------------------------------
class ExExtStorageScanTdb : public ComTdbExtStorageScan
{
  friend class Queue;

public:

  // ---------------------------------------------------------------------
  // Constructor is only called to instantiate an object used for
  // retrieval of the virtual table function pointer of the class while
  // unpacking. An empty constructor is enough.
  // ---------------------------------------------------------------------
  ExExtStorageScanTdb()
  {}

  virtual ~ExExtStorageScanTdb()
  {}

  // ---------------------------------------------------------------------
  // Build a TCB for this TDB. Redefined in the Executor project.
  // ---------------------------------------------------------------------
  virtual ex_tcb *build(ex_globals *globals);

private:
  // ---------------------------------------------------------------------
  // !!!!!!! IMPORTANT -- NO DATA MEMBERS ALLOWED IN EXECUTOR TDB !!!!!!!!
  // *********************************************************************
  // The Executor TDB's are only used for the sole purpose of providing a
  // way to supplement the Compiler TDB's (in comexe) with methods whose
  // implementation depends on Executor objects. This is done so as to
  // decouple the Compiler from linking in Executor objects unnecessarily.
  //
  // When a Compiler generated TDB arrives at the Executor, the same data
  // image is "cast" as an Executor TDB after unpacking. Therefore, it is
  // a requirement that a Compiler TDB has the same object layout as its
  // corresponding Executor TDB. As a result of this, all Executor TDB's
  // must have absolutely NO data members, but only member functions. So,
  // if you reach here with an intention to add data members to a TDB, ask
  // yourself two questions:
  //
  // 1. Are those data members Compiler-generated?
  //    If yes, put them in the appropriate ComTdb subclass instead.
  //    If no, they should probably belong to someplace else (like TCB).
  // 
  // 2. Are the classes those data members belong defined in the executor
  //    project?
  //    If your answer to both questions is yes, you might need to move
  //    the classes to the comexe project.
  // ---------------------------------------------------------------------
};

class ExExtStorageScanTcb  : public ExHdfsScanTcb
{
  friend class Queue;
  friend class ExExtStorageFastAggrTcb;

public:
  ExExtStorageScanTcb( const ComTdbExtStorageScan &tdb,
                 ex_globals *glob );

  ~ExExtStorageScanTcb();

  virtual Int32 fixup();

  virtual ExWorkProcRetcode work(); 

  static short createColInfoVecs
  (Queue *colNameList,
   Queue *colTypeList,
   TextVec &colNameVec,
   TextVec &colTypeVec);

  inline ExExtStorageScanTdb &extScanTdb() const
  { return (ExExtStorageScanTdb &) tdb; }

  inline ex_expr *extOperExpr() const 
    { return extScanTdb().extOperExpr_; }


protected:
  enum {
    NOT_STARTED
  , CHECK_FOR_DATA_MOD
  , CHECK_FOR_DATA_MOD_AND_DONE
  , SETUP_EXT_PPI
  , SETUP_EXT_COL_INFO
  , INIT_EXT_CURSOR
  , OPEN_EXT_CURSOR
  , GET_EXT_ROW
  , PROCESS_EXT_ROW
  , CLOSE_EXT_CURSOR
  , CLOSE_EXT_CURSOR_AND_DONE
  , RETURN_ROW
  , CLOSE_FILE
  , ERROR_CLOSE_FILE
  , COLLECT_STATS
  , HANDLE_ERROR
  , DONE
  , APPLY_SELECT_PRED
  , GET_NEXT_ROW_IN_VECTOR
  , PROCESS_NEXT_ROW_IN_VECTOR 
  , CLOSE_EXT_CURSOR_AND_ERROR
  } step_;

protected:
  void processBloomFilterData(ComTdbExtPPI * ppi, // in
                              char* data, // in
                              Lng32 dataLen, // in
                              std::vector<filterRecord>& filterVec // in/out
                              );

  void postProcessBloomFilters(std::vector<filterRecord>& filterVec);

  /////////////////////////////////////////////////////
  // Private methods.
  /////////////////////////////////////////////////////
 private:
  short extractAndTransformExtSourceToSqlRow(
                                             char * extRow,
                                             Int64 extRowLen,
                                             Lng32 numExtCols,
                                             ComDiagsArea* &diagsArea);
  Lng32 createSQRowFromVectors(ComDiagsArea *&diagsArea, Lng32& totalRowLen); 
  int getAndInitNextSelectedRangeUsingStrawScan();

  /////////////////////////////////////////////////////
  // Private data.
  /////////////////////////////////////////////////////

  ExpExtStorageInterface * exti_;

  Int64 extStartRowNum_;
  Int64 extNumRows_;
  Int64 extStopRowNum_;

  // Number of columns to be returned. -1, for all cols.
  Lng32 numCols_;
  // array of col numbers to be returned. (zero based)
  Lng32 *whichCols_;

  // returned row from ext scanFetch
  char * extRow_;
  Int64 extRowLen_;
  Int64 extRowNum_;
  Lng32 numExtCols_;

  char * extOperRow_;
  char * extPPIBuf_;
  Lng32 extPPIBuflen_;

  TextVec extPPIvec_;

  TextVec colNameVec_;
  TextVec colTypeVec_;

  std::vector<UInt64> expectedRowsVec_;

#ifdef NEED_INSTRUMENT_EXTSCAN
  Timer workTimer_;
  Int64 finalReturns_;
  Int64 poolBlocked_;
  Int64 isFull_;
  Int64 done_;
  Int64 handle_errors_;
  Int64 rows_;
  Int64 totalRows_;
  // total time spent to open, fetch and close from Parquet/ORC reader,
  // including the time that the reader evaluates push-down predicates,
  // excluding the time that the executor evaluates the executor 
  // predicate.
  Int64 totalScanTime_;
  Int64 totalScanOpenTime_;
  Int64 totalScanFetchTime_;
  Int64 totalScanCloseTime_;
  Int64 scanFetchCalls_;
#endif
};

// -----------------------------------------------------------------------
// ExExtStorageFastAggrTdb
// -----------------------------------------------------------------------
class ExExtStorageFastAggrTdb : public ComTdbExtStorageFastAggr
{
public:

  // ---------------------------------------------------------------------
  // Constructor is only called to instantiate an object used for
  // retrieval of the virtual table function pointer of the class while
  // unpacking. An empty constructor is enough.
  // ---------------------------------------------------------------------
  ExExtStorageFastAggrTdb()
  {}

  virtual ~ExExtStorageFastAggrTdb()
  {}

  // ---------------------------------------------------------------------
  // Build a TCB for this TDB. Redefined in the Executor project.
  // ---------------------------------------------------------------------
  virtual ex_tcb *build(ex_globals *globals);

private:
  // ---------------------------------------------------------------------
  // !!!!!!! IMPORTANT -- NO DATA MEMBERS ALLOWED IN EXECUTOR TDB !!!!!!!!
  // *********************************************************************
  // The Executor TDB's are only used for the sole purpose of providing a
  // way to supplement the Compiler TDB's (in comexe) with methods whose
  // implementation depends on Executor objects. This is done so as to
  // decouple the Compiler from linking in Executor objects unnecessarily.
  //
  // When a Compiler generated TDB arrives at the Executor, the same data
  // image is "cast" as an Executor TDB after unpacking. Therefore, it is
  // a requirement that a Compiler TDB has the same object layout as its
  // corresponding Executor TDB. As a result of this, all Executor TDB's
  // must have absolutely NO data members, but only member functions. So,
  // if you reach here with an intention to add data members to a TDB, ask
  // yourself two questions:
  //
  // 1. Are those data members Compiler-generated?
  //    If yes, put them in the appropriate ComTdb subclass instead.
  //    If no, they should probably belong to someplace else (like TCB).
  // 
  // 2. Are the classes those data members belong defined in the executor
  //    project?
  //    If your answer to both questions is yes, you might need to move
  //    the classes to the comexe project.
  // ---------------------------------------------------------------------
};

// class ExExtStorageFastAggrTcb
class ExExtStorageFastAggrTcb  : public ExExtStorageScanTcb
{
public:
  ExExtStorageFastAggrTcb( const ComTdbExtStorageFastAggr &tdb,
                    ex_globals *glob );
  
  ~ExExtStorageFastAggrTcb();

  virtual ExWorkProcRetcode work(); 

  inline ExExtStorageFastAggrTdb &extAggrTdb() const
  { return (ExExtStorageFastAggrTdb &) tdb; }

  AggrExpr *aggrExpr() const 
  { return extAggrTdb().aggrExpr(); }

   
protected:
  enum {
    NOT_STARTED
    , EXT_AGGR_INIT
    , EXT_AGGR_OPEN
    , EXT_AGGR_NEXT
    , EXT_AGGR_CLOSE
    , EXT_AGGR_EVAL
    , EXT_AGGR_COUNT
    , EXT_AGGR_COUNT_NONULL
    , EXT_AGGR_MIN
    , EXT_AGGR_MAX
    , EXT_AGGR_SUM
    , ORC_AGGR_NV_LOWER_BOUND
    , ORC_AGGR_NV_UPPER_BOUND
    , EXT_AGGR_HAVING_PRED
    , EXT_AGGR_PROJECT
    , EXT_AGGR_RETURN
    , CLOSE_FILE
    , ERROR_CLOSE_FILE
    , COLLECT_STATS
    , HANDLE_ERROR
    , DONE
  } step_;

  /////////////////////////////////////////////////////
  // Private data.
  /////////////////////////////////////////////////////
 private:
  Int64 rowCount_;
  
  char * outputRow_;

  // row where aggregate values retrieved from ext will be moved into.
  char * extAggrRow_;

  // row where aggregate value from all stripes will be computed.
  char * finalAggrRow_;

  Lng32 aggrNum_;

  ComTdbExtStorageFastAggr::ExtStorageAggrType aggrType_;
  Lng32  colNum_;
  char * colName_;
  Lng32  colType_;

  NAArray<HbaseStr> *colStats_;
};

#endif
