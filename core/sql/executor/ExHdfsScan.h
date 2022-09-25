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
#ifndef EX_HDFS_SCAN_H
#define EX_HDFS_SCAN_H

//
// Task Definition Block
//
#include "ComTdbHdfsScan.h"
#include "ExSimpleSqlBuffer.h"
#include "ExStats.h"
#include "sql_buffer.h"
#include "ex_queue.h"
#include <time.h>
#include "ExHbaseAccess.h"
#include "ExpHbaseInterface.h"
#include "ExpCompressionWA.h"

#define HIVE_MODE_DOSFORMAT    1
#define HIVE_MODE_CONV_ERROR_TO_NULL 2

// -----------------------------------------------------------------------
// Classes defined in this file
// -----------------------------------------------------------------------
class ExHdfsScanTdb;
class ExHdfsScanTcb;
class HdfsScan;
class HdfsClient;

// -----------------------------------------------------------------------
// Classes referenced in this file
// -----------------------------------------------------------------------
class ex_tcb;
class SequenceFileReader;

// -----------------------------------------------------------------------
// ExHdfsScanTdb
// -----------------------------------------------------------------------
class ExHdfsScanTdb : public ComTdbHdfsScan
{
public:

  // ---------------------------------------------------------------------
  // Constructor is only called to instantiate an object used for
  // retrieval of the virtual table function pointer of the class while
  // unpacking. An empty constructor is enough.
  // ---------------------------------------------------------------------
  ExHdfsScanTdb()
  {}

  virtual ~ExHdfsScanTdb()
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

class ExHdfsAccessTcb  : public ex_tcb
{
  friend class ExHdfsScanPrivateState;

public:
  ExHdfsAccessTcb( const ComTdbHdfsScan &tdb,
                   ex_globals *glob );
  
  ~ExHdfsAccessTcb();

  virtual void freeResources();
  virtual void registerSubtasks();  // register work procedures with scheduler

  ex_queue_pair getParentQueue() const { return qparent_;}
  virtual Int32 numChildren() const { return 0; }
  virtual const ex_tcb* getChild(Int32 /*pos*/) const { return NULL; }
  virtual NABoolean needStatsEntry();

  // For dynamic queue resizing.
  virtual ex_tcb_private_state * allocatePstates(
       Lng32 &numElems,      // inout, desired/actual elements
       Lng32 &pstateLength); // out, length of one element
  virtual ExOperStats * doAllocateStatsEntry(
       CollHeap *heap,
       ComTdb *tdb);

protected:
  /////////////////////////////////////////////////////
  // Protected methods.
  /////////////////////////////////////////////////////
  short moveRowToUpQueue(const char * row, Lng32 len, 
                         Lng32 tuppIndex,
                         short * rc, NABoolean isVarchar);
  short handleError(short &rc, ComDiagsArea * inDiagsArea);
  short handleDone(ExWorkProcRetcode &rc, Int64 rowsAffected = 0);

  short setupError(Lng32 exeError, Lng32 retcode, 
                   const char * str, const char * str2, const char * str3);

  /////////////////////////////////////////////////////
  // Private data.
  /////////////////////////////////////////////////////

  ex_queue_pair  qparent_;
  Int64 matches_;
  Int64 matchBrkPoint_;
  atp_struct * workAtp_;
 
  //ex_tcb defined pool_, use it 
  //sql_buffer_pool * pool_;            // row images after selection pred,

  char *loggingFileName_;
  NABoolean LoggingFileCreated_ ;

  ExpHbaseInterface * ehi_;

  Lng32 myInstNum_;
  ExHdfsScanStats *hdfsStats_;
  
};

/*
   USE_LIBHDFS_SCAN - OFF enables hdfs access via java classes 
      org.trafodion.sql.HdfsScan and org.trafodion.sql.HdfsClient
   Steps involved:
   1. Create a new HdfsScan object and set the scan ranges of the fragment instance in it
      The scan range involves the following and it is determined either at runtime or compile time
         a) filename
         b) offset
         c) len
      Java layer always reads more than the len by rangeTailIOSize_ to accommdate the record split 
   2. Two ByteBuffer objects are also passsed to HdfsScan object. These ByteBuffers are backed up by
      2 native buffers where the data is fetched. The buffer has a head room of size rangeTailIOSize_ and the 
      data is always read after the head room. 
   3. HdfsScan returns an int array containing bytesRead, bufNo, rangeNo, isEOF and schedules either
      the remaining bytes to be read or the next range using ByteBuffers alternatively.
   4. HdfsScan returns null array when there is no more data to be read.
   5. When the data is processed in one ByteBuffer in the native thread, the data is fetched into the other ByteBuffer by
      another Java thread.
   6. Native layer after processing all the rows in one ByteBuffer, moves the last incomplete row to head room of the
      other ByteBuffer. Then it requests to check if the read is complete. The native layer processes the buffer starting
      from the copied partial row.
*/

class ExHdfsScanTcb  : public ExHdfsAccessTcb
{
  friend class ExHdfsScanPrivateState;

public:
  enum
/*
   USE_LIBHDFS_SCAN - OFF enables hdfs access via java classes 
      org.trafodion.sql.HdfsScan and org.trafodion.sql.HdfsClient
   Steps involved:
   1. Create a new HdfsScan object and set the scan ranges of the fragment instance in it
      The scan range involves the following and it is determined either at runtime or compile time
         a) filename
         b) offset
         c) len
      Java layer always reads more than the len by rangeTailIOSize_ to accommdate the record split 
   2. Two ByteBuffer objects are also passsed to HdfsScan object. These ByteBuffers are backed up by
      2 native buffers where the data is fetched. The buffer has a head room of size rangeTailIOSize_ and the 
      data is always read after the head room. 
   3. HdfsScan returns an int array containing bytesRead, bufNo, rangeNo, isEOF and schedules either
      the remaining bytes to be read or the next range using ByteBuffers alternatively.
   4. HdfsScan returns null array when there is no more data to be read.
   5. When the data is processed in one ByteBuffer in the native thread, the data is fetched into the other ByteBuffer by
      another Java thread.
   6. Native layer after processing all the rows in one ByteBuffer, moves the last incomplete row to head room of the
      other ByteBuffer. Then it requests to check if the read is complete. The native layer processes the buffer starting
      from the copied incomplete row.
*/

  {
    BYTES_COMPLETED,
    BUF_NO,
    RANGE_NO,
    IS_EOF
  } retArrayIndices_;

  struct HDFS_SCAN_BUF
  {
     BYTE *headRoom_;
     BYTE *buf_;
  };
  ExHdfsScanTcb( const ComTdbHdfsScan &tdb,
                 ex_globals *glob );

  ~ExHdfsScanTcb();

  virtual void freeResources();


  virtual ExWorkProcRetcode work(); 
  
  // The static work procs for scheduler. 
  static ExWorkProcRetcode sWorkUp(ex_tcb *tcb) 
      { return ((ExHdfsScanTcb *) tcb)->work(); }

  ExHdfsScanStats *getHfdsScanStats()
  {
    if (getStatsEntry())
      return getStatsEntry()->castToExHdfsScanStats();
    else
      return NULL;
  }

protected:
  enum {
    NOT_STARTED
  , ASSIGN_RANGES_AT_RUNTIME
  , GET_HDFS_DATA
  , PROCESS_HDFS_ROW
  , RETURN_ROW
  , HANDLE_ERROR
  , HANDLE_EXCEPTION
  , DONE
  , HANDLE_ERROR_AND_DONE
  , SETUP_HDFS_SCAN
  , TRAF_HDFS_READ
  , COPY_TAIL_TO_HEAD
  , STOP_HDFS_SCAN
  } step_,nextStep_;

  /////////////////////////////////////////////////////
  // Protected methods.
  /////////////////////////////////////////////////////

  inline ExHdfsScanTdb &hdfsScanTdb() const
    { return (ExHdfsScanTdb &) tdb; }

  inline ex_expr *convertExpr() const 
    { return hdfsScanTdb().convertExpr_; }

  inline ex_expr *selectPred() const 
    { return hdfsScanTdb().selectPred_; }

  inline ex_expr *moveExpr() const 
    { return hdfsScanTdb().moveExpr_; }

  inline ex_expr *moveColsConvertExpr() const 
    { return hdfsScanTdb().moveColsConvertExpr_; }

  inline ex_expr *partElimExpr() const 
    { return hdfsScanTdb().partElimExpr_; }

  inline bool isSequenceFile() const
  {return hdfsScanTdb().isSequenceFile(); }

  inline bool strawScanMode() const
  {return hdfsScanTdb().strawScanMode(); }


  // returns ptr to start of next row, if any. Returning NULL 
  // indicates that no complete row was found. Beware of boundary 
  // conditions. Could be an incomplete row in a buffer returned by
  // hdfsRead. Or it could be the eof (in which case there is a good
  // row still waiting to be processed).
  char * extractAndTransformAsciiSourceToSqlRow(int &err,
						ComDiagsArea * &diagsArea, int mode);

  void initPartAndVirtColData(HdfsFileInfo* hdfo, Lng32 rangeNum, bool prefetch);
  HdfsFileInfo *getRange(Int32 r) { return getHdfsFileInfoListAsArray().at(r); } 

  int getAndInitNextSelectedRange(bool prefetch, bool selectRangesInAdvance);
  void computeRangesAtRuntime();
  void deallocateRuntimeRanges();
  void handleException(NAHeap *heap,
                          char *loggingDdata,
                          Lng32 loggingDataLen,
                          ComCondition *errorCond);

  // Get the array representation of the HdfsFileInfoList to aid in
  // o(1) access of an entry given an index.
  HdfsFileInfoArray& getHdfsFileInfoListAsArray() {return hdfsFileInfoListAsArray_;}

  /////////////////////////////////////////////////////
  // Private data.
  /////////////////////////////////////////////////////

  Int64 bytesLeft_;
  char * hdfsScanBuffer_;
  char * hdfsBufNextRow_;           // Pointer to next row

  char * debugPrevRow_;             // Pointer to help with debugging.
  Int64 debugtrailingPrevRead_;
  char *debugPenultimatePrevRow_;

  ExpCompressionWA * compressionWA_; // compression work area. Has information
                                     // about compression type, buffer for
                                     // compression scratch space and virtual
                                     // methods to initialize and decompress
                                     // each of the supported compression types.
                                     // is NULL for uncompressed files
  Int16 currCompressionTypeIx_;      // current index into table of compression
                                     // methods in the TDB or -1
  
  ExSimpleSQLBuffer *hdfsSqlBuffer_;  // this buffer for one row, converted
                                      // from ascii to SQL for select pred.
  tupp hdfsSqlTupp_;                  // tupp for this one row.
  char *hdfsSqlData_;                 // this one row. 

  ExSimpleSQLBuffer *moveExprColsBuffer_; // this buffer for one row, converted
                                      // from ascii to SQL for move expr only.
  tupp moveExprColsTupp_;             // tupp for this one row.
  char *moveExprColsData_;                 // this one row. 

  // this is where delimited columns that are read from hdfs rows will be moved to.
  // They will become source for convertExpr which will convert
  // them to sql row that will be returned by the scan operator.
  ExSimpleSQLBuffer *hdfsAsciiSourceBuffer_;
  tupp hdfsAsciiSourceTupp_;
  char * hdfsAsciiSourceData_;

  ExSimpleSQLBuffer *partColsBuffer_;
  tupp partColTupp_;                  // tupp for partition columns, if any
  char *partColData_;                 // pointer to data for partition columns

  ExSimpleSQLBuffer *virtColsBuffer_;
  tupp virtColTupp_;                  // tupp for virtual columns, if needed
  struct ComTdbHdfsVirtCols *virtColData_; // pointer to data for virtual columns



  Int64 requestTag_;
  Int64 hdfsScanBufMaxSize_;
  Int64 bytesRead_;
  Int64 uncompressedBytesRead_;
  Int64 trailingPrevRead_;     // for trailing bytes at end of buffer.
  Int64 numBytesProcessedInRange_;  // count bytes of complete records.
  bool  firstBufOfFile_;
  HdfsFileInfo *hdfo_;
  Int64 hdfsOffset_;
  Lng32 beginRangeNum_;
  Lng32 numRanges_;
  Lng32 currRangeNum_;
  Lng32 nextDelimRangeNum_;
  Lng32 preOpenedRangeNum_;    // pre-opened range
  Lng32 leftOpenRangeNum_;     // file left open, next range has same file
  char *endOfRequestedRange_ ; // helps rows span ranges.
  char * hdfsFileName_;
  SequenceFileReader* sequenceFileReader_;
  Int64 stopOffset_;
  bool  seqScanAgain_;
  char cursorId_[8];

  char *loggingFileName_;
  NABoolean loggingFileCreated_ ;
  char * hdfsLoggingRow_;
  char * hdfsLoggingRowEnd_;
  tupp_descriptor * defragTd_;

  NABoolean exception_;
  ComCondition * lastErrorCnd_;
  NABoolean checkRangeDelimiter_;

  ComDiagsArea * loggingErrorDiags_;

  // this array is populated from the info list stored as Queue.
  HdfsFileInfoArray hdfsFileInfoListAsArray_;

  //5 attributes bellow used by StrawScan feature
  char* queryId_;
  Lng32 explainNodeId_;
  ULng32 executionNb_;
  Lng32 nodeNumber_;
  Lng32 sequenceNb_;

  HdfsFileInfoArray instanceHdfsFileInfoArray_;

  HdfsClient *logFileHdfsClient_;
  HdfsClient *hdfsClient_;
  HdfsScan *hdfsScan_;
  BYTE *hdfsScanBufBacking_[2];
  HDFS_SCAN_BUF hdfsScanBuf_[2];
  int retArray_[4];
  BYTE *bufBegin_;
  BYTE *bufEnd_;
  BYTE *bufLogicalEnd_;
  long currRangeBytesRead_;
  int headRoomCopied_;
  int headRoom_;
  int prevRangeNum_;
  int extraBytesRead_;
  NABoolean recordSkip_;
  int skipHeaderCount_;
  char *errBuf_;
  int numFiles_;
};

#define RANGE_DELIMITER '\002'

inline char *hdfs_strchr(char *s, int c, const char *end, NABoolean checkRangeDelimiter, int mode , int *changedLen)
{
  char *curr = (char *)s;
  int count=0;
  //changedLen is lenght of \r which removed by this function
  *changedLen = 0;
  if( (mode & HIVE_MODE_DOSFORMAT ) == 0)
  {
   while (curr < end) {
    if (*curr == c)
    {
       return curr;
    }
    if (checkRangeDelimiter &&*curr == RANGE_DELIMITER)
       return NULL;
    curr++;
   }
  }
  else
  {
   while (curr < end) {
     if (*curr == c)
     {
         if(count>0 && c == '\n')
         {
           if(s[count-1] == '\r') 
             *changedLen = 1;
         }
         return curr - *changedLen;
      }
      if (checkRangeDelimiter &&*curr == RANGE_DELIMITER)
         return NULL;
    curr++;
    count++;
   }
  }
  return NULL;
}


inline char *hdfs_strchr(char *s, int rd, int cd, const char *end, NABoolean checkRangeDelimiter, NABoolean *rdSeen, int mode, int* changedLen)
{
  char *curr = (char *)s;
  int count = 0;
  //changedLen is lenght of \r which removed by this function
  *changedLen = 0;
  if( (mode & HIVE_MODE_DOSFORMAT)>0 )  //check outside the while loop to make it faster
  {
    while (curr < end) {
      if (*curr == rd) {
         if(count>0 && rd == '\n')
         {
             if(s[count-1] == '\r') 
               *changedLen = 1;
         }
         *rdSeen = TRUE;
         return curr - *changedLen;
      }
      else
      if (*curr == cd) {
         *rdSeen = FALSE;
         return curr;
      }
      else
      if (checkRangeDelimiter && *curr == RANGE_DELIMITER) {
         *rdSeen = TRUE;
         return NULL;
      }
      curr++;
      count++;
    }
  }
  else
  {
    while (curr < end) {
      if (*curr == rd) {
         *rdSeen = TRUE;
         return curr;
      }
      else
      if (*curr == cd) {
         *rdSeen = FALSE;
         return curr;
      }
      else
      if (checkRangeDelimiter && *curr == RANGE_DELIMITER) {
         *rdSeen = TRUE;
         return NULL;
      }
      curr++;
    }
  }
  *rdSeen = FALSE;
  return NULL;
}

#endif
