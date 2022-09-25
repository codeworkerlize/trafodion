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

#include "Platform.h"

#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/time.h>
#include <poll.h>

#include <iostream>

#include "ex_stdh.h"
#include "ComTdb.h"
#include "ex_tcb.h"
#include "ExHdfsScan.h"
#include "ex_exe_stmt_globals.h"
#include "ExpLOBinterface.h"
#include "SequenceFileReader.h" 
#include "Hbase_types.h"
#include "stringBuf.h"
#include "NLSConversion.h"
#include "Context.h"

#include "ComSmallDefs.h"
#include "ttime.h"
#include "HdfsClient_JNI.h"
#include "ExpLOB.h"

ex_tcb * ExHdfsScanTdb::build(ex_globals * glob)
{
  ExExeStmtGlobals * exe_glob = glob->castToExExeStmtGlobals();
  
  ex_assert(exe_glob,"This operator cannot be in DP2");

  ExHdfsScanTcb *tcb = NULL;
  
  if ((isTextFile()) || (isSequenceFile()))
    {
      tcb = new(exe_glob->getSpace()) 
        ExHdfsScanTcb(
                      *this,
                      exe_glob);
    }

  ex_assert(tcb, "Error building ExHdfsScanTcb.");

  return (tcb);
}

////////////////////////////////////////////////////////////////
// Constructor and initialization.
////////////////////////////////////////////////////////////////

ExHdfsAccessTcb::ExHdfsAccessTcb(
          const ComTdbHdfsScan &hdfsScanTdb, 
          ex_globals * glob ) :
  ex_tcb( hdfsScanTdb, 1, glob)
  , workAtp_(NULL)
  , matches_(0)
  , matchBrkPoint_(0)
  , loggingFileName_(NULL)
  , hdfsStats_(NULL)
{
  Space * space = (glob ? glob->getSpace() : 0);
  CollHeap * heap = (glob ? glob->getDefaultHeap() : 0);

  pool_ = new(space) 
        sql_buffer_pool(hdfsScanTdb.numBuffers_,
        hdfsScanTdb.bufferSize_,
        space,
        ((ExHdfsScanTdb &)hdfsScanTdb).denseBuffers() ? 
        SqlBufferBase::DENSE_ : SqlBufferBase::NORMAL_);

  pool_->setStaticMode(TRUE);
        
  // Allocate the queue to communicate with parent
  allocateParentQueues(qparent_);

  workAtp_ = allocateAtp(hdfsScanTdb.workCriDesc_, space);

  /*
  // Register subtasks with the scheduler
  registerSubtasks();
  registerResizeSubtasks();
  */

  Lng32 fileNum = getGlobals()->castToExExeStmtGlobals()->getMyInstanceNumber();
  ExHbaseAccessTcb::buildLoggingFileName((NAHeap *)getHeap(), ((ExHdfsScanTdb &)hdfsScanTdb).getLoggingLocation(),
                     ((ExHdfsScanTdb &)hdfsScanTdb).tableName(),
                     "hive_scan_err",
                     fileNum,
                     loggingFileName_);
  LoggingFileCreated_ = FALSE;

  ehi_ = ExpHbaseInterface::newInstance(glob->getDefaultHeap(),
                                        (char*)"",  //Later replace with server cqd
                                        (char*)"", ////Later replace with port cqd
                                        COM_STORAGE_UNKNOWN,
                                        FALSE);
}
    
ExHdfsAccessTcb::~ExHdfsAccessTcb()
{
  freeResources();
}

void ExHdfsAccessTcb::freeResources()
{
  if (loggingFileName_ != NULL) {
     NADELETEBASIC(loggingFileName_, getHeap());
     loggingFileName_ = NULL;
  }
  if (workAtp_)
  {
    workAtp_->release();
    deallocateAtp(workAtp_, getSpace());
    workAtp_ = NULL;
  }
  if (pool_)
  {
    delete pool_;
    pool_ = NULL;
  }
  if (qparent_.up)
  {
    delete qparent_.up;
    qparent_.up = NULL;
  }
  if (qparent_.down)
  {
    delete qparent_.down;
    qparent_.down = NULL;
  }

}

NABoolean ExHdfsAccessTcb::needStatsEntry()
{
  // stats are collected for ALL and OPERATOR options.
  if ((getGlobals()->getStatsArea()->getCollectStatsType() == 
       ComTdb::ALL_STATS) ||
      (getGlobals()->getStatsArea()->getCollectStatsType() == 
      ComTdb::OPERATOR_STATS))
    return TRUE;
  else if ( getGlobals()->getStatsArea()->getCollectStatsType() == ComTdb::PERTABLE_STATS)
    return TRUE;
  else
    return FALSE;
}

ExOperStats * ExHdfsAccessTcb::doAllocateStatsEntry(CollHeap *heap,
                                                        ComTdb *tdb)
{
  ExEspStmtGlobals *espGlobals = getGlobals()->castToExExeStmtGlobals()->castToExEspStmtGlobals();
  StmtStats *ss; 
  if (espGlobals != NULL)
     ss = espGlobals->getStmtStats();
  else
     ss = getGlobals()->castToExExeStmtGlobals()->castToExMasterStmtGlobals()->getStatement()->getStmtStats(); 
  
  ExHdfsScanStats *hdfsScanStats = new(heap) ExHdfsScanStats(heap,
				   this,
				   tdb);
  if (ss != NULL) 
     hdfsScanStats->setQueryId(ss->getQueryId(), ss->getQueryIdLen());
  hdfsStats_ = hdfsScanStats;
  return hdfsStats_;
}

void ExHdfsAccessTcb::registerSubtasks()
{
  ExScheduler *sched = getGlobals()->getScheduler();
  sched->registerInsertSubtask(sWork,   this, qparent_.down,"PD");
  sched->registerUnblockSubtask(sWork,    this, qparent_.up,  "PU");
  sched->registerCancelSubtask(sWork,     this, qparent_.down,"CN");
  if (getPool())
  {
    getPool()->setStaticMode(FALSE);
    getPool()->setDynamicUnlimited(TRUE);
  }
  
}



ex_tcb_private_state *ExHdfsAccessTcb::allocatePstates(
     Lng32 &numElems,      // inout, desired/actual elements
     Lng32 &pstateLength)  // out, length of one element
{
  PstateAllocator<ex_tcb_private_state> pa;
  return pa.allocatePstates(this, numElems, pstateLength);
}

short ExHdfsAccessTcb::setupError(Lng32 exeError, Lng32 retcode, 
                                const char * str, const char * str2, const char * str3)
{
  // Make sure retcode is positive.
  if (retcode < 0)
    retcode = -retcode;
    
  ex_queue_entry *pentry_down = qparent_.down->getHeadEntry();
  
  Lng32 intParam1 = retcode;
  Lng32 intParam2 = 0;
  ComDiagsArea * diagsArea = NULL;
  ExRaiseSqlError(getHeap(), &diagsArea, 
                  (ExeErrorCode)(exeError), NULL, &intParam1, 
                  &intParam2, NULL, 
                  (str ? (char*)str : (char*)" "),
                  (str2 ? (char*)str2 : (char*)" "),
                  (str3 ? (char*)str3 : (char*)" "));
                  
  pentry_down->setDiagsArea(diagsArea);
  return -1;
}

short ExHdfsAccessTcb::moveRowToUpQueue(const char * row, Lng32 len, 
                                        Lng32 tuppIndex,
                                        short * rc, NABoolean isVarchar)
{
  if (qparent_.up->isFull())
    {
      if (rc)
	*rc = WORK_OK;
      return -1;
    }

  Lng32 length;
  if (len <= 0)
    length = strlen(row);
  else
    length = len;

  tupp p;
  if (pool_->get_free_tuple(p, (Lng32)
			    ((isVarchar ? SQL_VARCHAR_HDR_SIZE : 0)
			     + length)))
    {
      if (rc)
	*rc = WORK_POOL_BLOCKED;
      return -1;
    }
  char * dp = p.getDataPointer();
  if (isVarchar)
    {
      *(short*)dp = (short)length;
      str_cpy_all(&dp[SQL_VARCHAR_HDR_SIZE], row, length);
    }
  else
    {
      str_cpy_all(dp, row, length);
    }

  ex_queue_entry * pentry_down = qparent_.down->getHeadEntry();
  ex_queue_entry * up_entry = qparent_.up->getTailEntry();
  
  up_entry->copyAtp(pentry_down);
  //  up_entry->getAtp()->getTupp((Lng32)hdfsScanTdb().tuppIndex_) = p;
  up_entry->getAtp()->getTupp(tuppIndex) = p;

  up_entry->upState.parentIndex = 
    pentry_down->downState.parentIndex;
  
  up_entry->upState.setMatchNo(++matches_);
  up_entry->upState.status = ex_queue::Q_OK_MMORE;

  // insert into parent
  qparent_.up->insert();

  return 0;
}

short ExHdfsAccessTcb::handleError(short &rc,
                                   ComDiagsArea *inDiagsArea)
{
  if (ex_tcb::handleError(&qparent_, inDiagsArea))
    {
      rc =  WORK_OK;
      return -1;
    }

  return 0;
}

short ExHdfsAccessTcb::handleDone(ExWorkProcRetcode &rc, Int64 rowsAffected)
{
  if (qparent_.up->isFull())
    {
      rc = WORK_OK;
      return -1;
    }

  ex_queue_entry *pentry_down = qparent_.down->getHeadEntry();
  ex_queue_entry *up_entry = qparent_.up->getTailEntry();
  up_entry->copyAtp(pentry_down);
  up_entry->upState.parentIndex =
    pentry_down->downState.parentIndex;
  up_entry->upState.downIndex = qparent_.down->getHeadIndex();
  up_entry->upState.status = ex_queue::Q_NO_DATA;
  up_entry->upState.setMatchNo(matches_);
  if (rowsAffected > 0)
    {
      ExMasterStmtGlobals *g = getGlobals()->
        castToExExeStmtGlobals()->castToExMasterStmtGlobals();
      if (g)
        {
          g->setRowsAffected(g->getRowsAffected() + rowsAffected);
        }
      else
        {
          ComDiagsArea * diagsArea = up_entry->getDiagsArea();
          if (!diagsArea)
            {
              diagsArea = 
                ComDiagsArea::allocate(getGlobals()->getDefaultHeap());
              up_entry->setDiagsArea(diagsArea);
            }
          diagsArea->addRowCount(rowsAffected);
        }
    }
  qparent_.up->insert();
  
  qparent_.down->removeHead();

  return 0;
}

////////////////////////////////////////////////////////////////
// Constructor and initialization.
////////////////////////////////////////////////////////////////

ExHdfsScanTcb::ExHdfsScanTcb(
          const ComTdbHdfsScan &hdfsScanTdb, 
          ex_globals * glob ) :
  ExHdfsAccessTcb( hdfsScanTdb, glob)
  , bytesLeft_(0)
  , hdfsScanBuffer_(NULL)
  , hdfsBufNextRow_(NULL)
  , compressionWA_(NULL)
  , currCompressionTypeIx_(-1)
  , hdfsLoggingRow_(NULL)
  , hdfsLoggingRowEnd_(NULL)
  , debugPrevRow_(NULL)
  , hdfsSqlBuffer_(NULL)
  , hdfsSqlData_(NULL)
  , step_(NOT_STARTED)
  , endOfRequestedRange_(NULL)
  , sequenceFileReader_(NULL)
  , seqScanAgain_(false)
  , hdfo_(NULL)
  , numBytesProcessedInRange_(0)
  , exception_(FALSE)
  , checkRangeDelimiter_(FALSE)
  , nextDelimRangeNum_(-1)
  , preOpenedRangeNum_(-1)
  , leftOpenRangeNum_(-1)
  , loggingErrorDiags_(NULL)
  , loggingFileName_(NULL)
  , logFileHdfsClient_(NULL)
  , hdfsClient_(NULL)
  , hdfsScan_(NULL)
  , hdfsFileInfoListAsArray_(glob->getDefaultHeap(), hdfsScanTdb.getHdfsFileInfoList()->numEntries())
  , instanceHdfsFileInfoArray_(glob->getDefaultHeap(), hdfsScanTdb.getHdfsFileInfoList()->numEntries())
  , numFiles_(0)
{
  Space * space = (glob ? glob->getSpace() : 0);
  CollHeap * heap = (glob ? glob->getDefaultHeap() : 0);
  hdfsScanBufMaxSize_ = hdfsScanTdb.hdfsBufSize_;
  headRoom_ = (Int32)hdfsScanTdb.rangeTailIOSize_;

  hdfsScanBufBacking_[0] = new (heap) BYTE[hdfsScanBufMaxSize_ + 2 * (headRoom_)];
  hdfsScanBufBacking_[1] = new (heap) BYTE[hdfsScanBufMaxSize_ + 2 * (headRoom_)];
  for (int i=0; i < 2; i++) {
     BYTE *hdfsScanBufBacking = hdfsScanBufBacking_[i];
     hdfsScanBuf_[i].headRoom_ = hdfsScanBufBacking;
     hdfsScanBuf_[i].buf_ = hdfsScanBufBacking + headRoom_;
  }
  bufBegin_ = NULL;
  bufEnd_ = NULL;
  bufLogicalEnd_ = NULL;
  headRoomCopied_ = 0;
  prevRangeNum_ = -1;
  currRangeBytesRead_ = 0;
  recordSkip_ = FALSE;
  skipHeaderCount_ = 0;
  moveExprColsBuffer_ = new(space) ExSimpleSQLBuffer( 1, // one row 
						      (Int32)hdfsScanTdb.moveExprColsRowLength_,
						      space);
  short error = moveExprColsBuffer_->getFreeTuple(moveExprColsTupp_);
  ex_assert((error == 0), "get_free_tuple cannot hold a row.");
  moveExprColsData_ = moveExprColsTupp_.getDataPointer();


  hdfsSqlBuffer_ = new(space) ExSimpleSQLBuffer( 1, // one row 
						 (Int32)hdfsScanTdb.hdfsSqlMaxRecLen_,
						 space);
  error = hdfsSqlBuffer_->getFreeTuple(hdfsSqlTupp_);
  ex_assert((error == 0), "get_free_tuple cannot hold a row.");
  hdfsSqlData_ = hdfsSqlTupp_.getDataPointer();

  hdfsAsciiSourceBuffer_ = new(space) ExSimpleSQLBuffer( 1, // one row 
							 (Int32)hdfsScanTdb.asciiRowLen_ * 2, // just in case
							 space);
  error = hdfsAsciiSourceBuffer_->getFreeTuple(hdfsAsciiSourceTupp_);
  ex_assert((error == 0), "get_free_tuple cannot hold a row.");
  hdfsAsciiSourceData_ = hdfsAsciiSourceTupp_.getDataPointer();

  if (hdfsScanTdb.partColsRowLength_ > 0)
    {
      partColsBuffer_ = new(space) ExSimpleSQLBuffer( 1, // one row 
                                                      hdfsScanTdb.partColsRowLength_,
                                                      space);
      error = partColsBuffer_->getFreeTuple(partColTupp_);
      ex_assert((error == 0), "get_free_tuple cannot hold a row.");
      partColData_ = partColTupp_.getDataPointer();
    }
  else
    {
      partColsBuffer_ = NULL;
      partColData_ = NULL;
    }

  if (hdfsScanTdb.virtColsRowLength_ > 0)
    {
      virtColsBuffer_ = new(space) ExSimpleSQLBuffer( 1, // one row 
                                                      hdfsScanTdb.virtColsRowLength_,
                                                      space);
      error = virtColsBuffer_->getFreeTuple(virtColTupp_);
      ex_assert((error == 0), "get_free_tuple cannot hold a part col row.");
      ex_assert(hdfsScanTdb.virtColsRowLength_ == sizeof(struct ComTdbHdfsVirtCols),
                "Inconsistency in virt col length");
      virtColData_ = reinterpret_cast<struct ComTdbHdfsVirtCols*>(
           virtColTupp_.getDataPointer());
    }
  else
    {
      virtColsBuffer_ = NULL;
      virtColData_ = NULL;
    }

  defragTd_ = NULL;
  // removing the cast produce a compile error
  if (((ExHdfsScanTdb &)hdfsScanTdb).useCifDefrag())
  {
    defragTd_ = pool_->addDefragTuppDescriptor(hdfsScanTdb.outputRowLength_);
  }
  // Allocate the queue to communicate with parent
  allocateParentQueues(qparent_);

  workAtp_ = allocateAtp(hdfsScanTdb.workCriDesc_, space);

  // fixup expressions
  if (selectPred())
    selectPred()->fixup(0, getExpressionMode(), this,  space, heap, FALSE, glob);
  if (moveExpr())
    moveExpr()->fixup(0, getExpressionMode(), this,  space, heap, FALSE, glob);
  if (convertExpr())
    convertExpr()->fixup(0, getExpressionMode(), this,  space, heap, FALSE, glob);
  if (moveColsConvertExpr())
    moveColsConvertExpr()->fixup(0, getExpressionMode(), this,  space, heap, FALSE, glob);
  if (partElimExpr())
    partElimExpr()->fixup(0, getExpressionMode(), this,  space, heap, FALSE, glob);

  registerSubtasks();
  registerResizeSubtasks();


  Lng32 fileNum = getGlobals()->castToExExeStmtGlobals()->getMyInstanceNumber();
  ExHbaseAccessTcb::buildLoggingFileName((NAHeap *)getHeap(), ((ExHdfsScanTdb &)hdfsScanTdb).getLoggingLocation(),
                     ((ExHdfsScanTdb &)hdfsScanTdb).tableName(),
                     "hive_scan_err",
                     fileNum,
                     loggingFileName_);
  loggingFileCreated_ = FALSE;
  //shoud be move to work method
  int jniDebugPort = 0;
  int jniDebugTimeout = 0;
  ehi_ = ExpHbaseInterface::newInstance(glob->getDefaultHeap(),
                                        (char*)"",  //Later replace with server cqd
                                        (char*)"",
                                        COM_STORAGE_UNKNOWN,
                                        FALSE);
  ex_assert(ehi_ != NULL, "Internal error: ehi_ is null in ExHdfsScan");
  HDFS_Client_RetCode hdfsClientRetcode;
  hdfsClient_ = HdfsClient::newInstance((NAHeap *)getHeap(), NULL, hdfsClientRetcode);
  ex_assert(hdfsClientRetcode == HDFS_CLIENT_OK, "Internal error: HdfsClient::newInstance returned an error"); 
  // Populate the hdfsInfo list into an array to gain o(1) lookup access
  Queue* hdfsInfoList = hdfsScanTdb.getHdfsFileInfoList();
  if ( hdfsInfoList && hdfsInfoList->numEntries() > 0 )
  {
     hdfsInfoList->position();
     int i = 0;
     HdfsFileInfo* fInfo = NULL;
     while ((fInfo = (HdfsFileInfo*)hdfsInfoList->getNext()) != NULL)
        {
          hdfsFileInfoListAsArray_.insertAt(i, fInfo);
          i++;
        }
  }
  if (hdfsScanTdb.strawScanMode()){
    ExEspStmtGlobals *espGlobals = getGlobals()->castToExExeStmtGlobals()->castToExEspStmtGlobals();
    queryId_ = new(heap) char[espGlobals->getQueryIdLen()+1];
    memcpy(queryId_,espGlobals->getQueryId(),espGlobals->getQueryIdLen());
    queryId_[espGlobals->getQueryIdLen()]='\0';
    //fragmentId_= espGlobals->getMyFragId();
    explainNodeId_= hdfsScanTdb.getExplainNodeId();
    nodeNumber_ = espGlobals->myNodeNumber();
    QRLogger::log(CAT_SQL_HDFS_SCAN,
                  LL_DEBUG,
                  "ExHdfsScanTcb::constructor called. QueryId=%s , explainNodeId=%d, nodeNumber=%d",
                  queryId_,explainNodeId_,nodeNumber_);
  }else
    queryId_ = NULL;
}
    
ExHdfsScanTcb::~ExHdfsScanTcb()
{
  freeResources();
}

void ExHdfsScanTcb::freeResources()
{
  if (hdfsScan_ != NULL)
     hdfsScan_->stop();
  if (loggingFileName_ != NULL) {
     NADELETEBASIC(loggingFileName_, getHeap());
     loggingFileName_ = NULL;
  }
  if (queryId_ != NULL) {
     NADELETEBASIC(queryId_, getHeap());
     queryId_ = NULL;
  }
  if (hdfsScanBuffer_)
  {
    NADELETEBASIC(hdfsScanBuffer_, getHeap());
    hdfsScanBuffer_ = NULL;
  }
  if (hdfsAsciiSourceBuffer_)
  {
    NADELETEBASIC(hdfsAsciiSourceBuffer_, getSpace());
    hdfsAsciiSourceBuffer_ = NULL;
  }
  if(sequenceFileReader_)
  {
    NADELETE(sequenceFileReader_,SequenceFileReader, getHeap());
    sequenceFileReader_ = NULL;
  }
 
  // hdfsSqlTupp_.release() ; // ??? 
  if (hdfsSqlBuffer_)
  {
    delete hdfsSqlBuffer_;
    hdfsSqlBuffer_ = NULL;
  }
  if (compressionWA_)
  {
    delete compressionWA_;
    compressionWA_ = NULL;
    currCompressionTypeIx_ = -1;
  }
  if (moveExprColsBuffer_)
  {
    delete moveExprColsBuffer_;
    moveExprColsBuffer_ = NULL;
  }
  if (partColsBuffer_)
  {
    delete partColsBuffer_;
    partColsBuffer_ = NULL;
    partColData_ = NULL;
  }
  if (virtColsBuffer_)
  {
    delete virtColsBuffer_;
    virtColsBuffer_ = NULL;
    virtColData_ = NULL;
  }
  deallocateRuntimeRanges();

  if (hdfsClient_ != NULL) 
     NADELETE(hdfsClient_, HdfsClient, getHeap());
  if (logFileHdfsClient_ != NULL) 
     NADELETE(logFileHdfsClient_, HdfsClient, getHeap());
  if (hdfsScan_ != NULL) 
     NADELETE(hdfsScan_, HdfsScan, getHeap());
}

void brkpoint()
{}

ExWorkProcRetcode ExHdfsScanTcb::work()
{
  Lng32 retcode = 0;
  SFR_RetCode sfrRetCode = SFR_OK;
  char *errorDesc = NULL;
  char cursorId[8];
  HdfsFileInfo *hdfo = NULL;
  Lng32 openType = 0;
  int changedLen = 0;

  ContextCli *currContext = getGlobals()->castToExExeStmtGlobals()->getCliGlobals()->currContext();
  Int32 hdfsErrorDetail = 0;//this is errno returned form underlying hdfsOpenFile call.
  HDFS_Scan_RetCode hdfsScanRetCode;

  while (!qparent_.down->isEmpty())
    {
      ex_queue_entry *pentry_down = qparent_.down->getHeadEntry();
      if (pentry_down->downState.request == ex_queue::GET_NOMORE && step_ != DONE) 
      {
         step_ = STOP_HDFS_SCAN;
      }
      switch (step_)
	{
	case NOT_STARTED:
	  {
	    matches_ = 0;
	    beginRangeNum_ = -1;
	    numRanges_ = -1;
	    hdfsOffset_ = 0;
            checkRangeDelimiter_ = FALSE;
	    myInstNum_ = getGlobals()->getMyInstanceNumber();
	    hdfsScanBufMaxSize_ = hdfsScanTdb().hdfsBufSize_;
            nextDelimRangeNum_   =
            preOpenedRangeNum_ =
            leftOpenRangeNum_  = -1;

	    if (hdfsScanTdb().getAssignRangesAtRuntime())
              {
                step_ = ASSIGN_RANGES_AT_RUNTIME;
                break;
              }
	    else {
               step_ = SETUP_HDFS_SCAN; 
	    }

            // if my inst num is outside the range of entries, then nothing
            // need to be retrieved. We are done.
            if (myInstNum_ >= hdfsScanTdb().getHdfsFileRangeBeginList()->entries())
              {
                step_ = DONE;
                break;
              }

	    beginRangeNum_ =  
	      *(Lng32*)hdfsScanTdb().getHdfsFileRangeBeginList()->get(myInstNum_);

	    numRanges_ =  
	      *(Lng32*)hdfsScanTdb().getHdfsFileRangeNumList()->get(myInstNum_);

	    currRangeNum_ = beginRangeNum_;
            if (numRanges_ <= 0)
               step_ = DONE;
	  }
	  break;

        case ASSIGN_RANGES_AT_RUNTIME:
          computeRangesAtRuntime();
          currRangeNum_ = beginRangeNum_;
          if (numRanges_ > 0) {
            step_ = SETUP_HDFS_SCAN; 
          }
          else
            step_ = DONE;
          break;
        case SETUP_HDFS_SCAN:
          {   
             if (hdfsScan_ != NULL)
                NADELETE(hdfsScan_, HdfsScan, getHeap());
             getAndInitNextSelectedRange(FALSE, TRUE); 
             if (instanceHdfsFileInfoArray_.entries() == 0) {
                step_ = DONE;
                break;
             } 
             hdfsScan_ = HdfsScan::newInstance((NAHeap *)getHeap(), hdfsScanBuf_, hdfsScanBufMaxSize_, 
                            hdfsScanTdb().hdfsIoByteArraySizeInKB_, 
                            &instanceHdfsFileInfoArray_, 0, instanceHdfsFileInfoArray_.entries(), hdfsScanTdb().rangeTailIOSize_, 
                            isSequenceFile(), hdfsScanTdb().recordDelimiter_, 
                            hdfsStats_, hdfsScanRetCode);
             if (hdfsScanRetCode != HDFS_SCAN_OK) {
                setupError(EXE_ERROR_HDFS_SCAN, hdfsScanRetCode, "SETUP_HDFS_SCAN", 
                              GetCliGlobals()->getJniErrorStr(), NULL);              
                step_ = HANDLE_ERROR_AND_DONE;
                break;
             } 
             bufBegin_ = NULL;
             bufEnd_ = NULL;
             bufLogicalEnd_ = NULL;
             headRoomCopied_ = 0;
             prevRangeNum_ = -1;                         
             currRangeBytesRead_ = 0;                   
             recordSkip_ = FALSE;
             skipHeaderCount_ = 0;
             extraBytesRead_ = 0;
             step_ = TRAF_HDFS_READ;
          } 
          break;
        case TRAF_HDFS_READ:
          {
             hdfsScanRetCode = hdfsScan_->trafHdfsRead(retArray_, sizeof(retArray_)/sizeof(int));
             if (hdfsScanRetCode == HDFS_SCAN_EOR) {
                step_ = DONE;
                break;
             }
             else if (hdfsScanRetCode != HDFS_SCAN_OK) {
                setupError(EXE_ERROR_HDFS_SCAN, hdfsScanRetCode, "SETUP_HDFS_SCAN", 
                              GetCliGlobals()->getJniErrorStr(), NULL);              
                step_ = HANDLE_ERROR_AND_DONE;
                break;
             } 
             if (retArray_[BYTES_COMPLETED] == 0) {
                ex_assert(headRoomCopied_ == 0, "Internal Error in HdfsScan");
                step_ = TRAF_HDFS_READ;
                break;  
             }
             hdfo = instanceHdfsFileInfoArray_.at(retArray_[RANGE_NO]);
             bufEnd_ = hdfsScanBuf_[retArray_[BUF_NO]].buf_ + retArray_[BYTES_COMPLETED];
	     if ((retArray_[IS_EOF] == 1) && (*(bufEnd_-1))!= hdfsScanTdb().recordDelimiter_) {
	       *bufEnd_ = hdfsScanTdb().recordDelimiter_;
	       bufEnd_++ ;
	       retArray_[BYTES_COMPLETED]++;
             }
             if (retArray_[RANGE_NO] != prevRangeNum_) {  
                initPartAndVirtColData(hdfo, retArray_[RANGE_NO], FALSE);
                currRangeBytesRead_ = retArray_[BYTES_COMPLETED];
                bufBegin_ = hdfsScanBuf_[retArray_[BUF_NO]].buf_;
                if (hdfo->getStartOffset() == 0) {
                   recordSkip_ = FALSE;
		   skipHeaderCount_ = hdfsScanTdb().getSkipHeaderCount();
		}
                else
                   recordSkip_ = TRUE; 
             } else {
                // Throw away the rest of the data when done with the current range
                if (currRangeBytesRead_ > hdfo->getBytesToRead()) {
                   step_ = TRAF_HDFS_READ;
                   break;
                }
                currRangeBytesRead_ += retArray_[BYTES_COMPLETED];
                bufBegin_ = hdfsScanBuf_[retArray_[BUF_NO]].buf_ - headRoomCopied_;
                recordSkip_ = FALSE;
             }
             if (currRangeBytesRead_ > hdfo->getBytesToRead())
                extraBytesRead_ = currRangeBytesRead_ - hdfo->getBytesToRead(); 
             else
                extraBytesRead_ = 0;
             ex_assert(extraBytesRead_ >= 0, "Negative number of extraBytesRead");
             // headRoom_ is the number of extra bytes to be read (rangeTailIOSize)
             // If the whole range fits in one buffer, it is needed to process rows till EOF for the last range alone.
             if (numFiles_ <= 1) {
                if (retArray_[IS_EOF] && extraBytesRead_ < headRoom_ && (retArray_[RANGE_NO] == (hdfsFileInfoListAsArray_.entries()-1)))
                   extraBytesRead_ = 0;
             }
             else {
                // If EOF is reached while reading the range and the extraBytes read
               // is less than headRoom_ then process all the data till EOF 
               if (retArray_[IS_EOF] && extraBytesRead_ < headRoom_ )  
                  extraBytesRead_ = 0;
             }

             bufLogicalEnd_ = hdfsScanBuf_[retArray_[BUF_NO]].buf_ + retArray_[BYTES_COMPLETED] - extraBytesRead_;
             prevRangeNum_ = retArray_[RANGE_NO];
             headRoomCopied_ = 0;
             hdfsBufNextRow_ = (char *)bufBegin_;
             char * skipEnd_ = (char *)bufEnd_;
             if (recordSkip_ || (skipHeaderCount_ > 0)) {
	       if (recordSkip_) // record skip and skipHeader cannot both be true
		 skipHeaderCount_ = 1 ; // recordskip is to skip a partial row for an intermediate range
               else
                 skipEnd_ = (char *)bufLogicalEnd_; // do not look past logical end for header

               while((skipHeaderCount_ > 0)&&(hdfsBufNextRow_ != NULL)) {
		 hdfsBufNextRow_ = hdfs_strchr(hdfsBufNextRow_,
					       hdfsScanTdb().recordDelimiter_, 
					       skipEnd_,
					       checkRangeDelimiter_, 
					       hdfsScanTdb().getHiveScanMode(), &changedLen);
		 if (hdfsBufNextRow_) {
		   hdfsBufNextRow_ += 1 + changedLen; 
                   skipHeaderCount_--;
                 }
               }
	       if (hdfsBufNextRow_ == NULL) {
                 if (recordSkip_) {
                   setupError(8446, 0, "No record delimiter found in buffer from hdfsRead", 
                              NULL, NULL);              
                   step_ = HANDLE_ERROR_AND_DONE;
                 }
                 else {
                   if (extraBytesRead_ == 0) {
                     if (retArray_[IS_EOF])
                       skipHeaderCount_ = 0;
                     step_ = TRAF_HDFS_READ ; 
                   }
                   else {
                     char errBuf[strlen(hdfo->fileName()) + 200];
                     snprintf(errBuf, sizeof(errBuf),"skip %d lines of header specified. %d lines remain to be found in the first range. File: %s",
                              hdfsScanTdb().getSkipHeaderCount(), 
                              skipHeaderCount_, hdfo->fileName());
                      setupError(8446, 0, errBuf, NULL, NULL);              
                      step_ = HANDLE_ERROR_AND_DONE;
                   }
                 }
                 break;
               }
             }
             QRLogger::log(CAT_SQL_EXE, LL_DEBUG, "FileName %s Offset %ld BytesToRead %ld BytesRead %ld RangeNo %d IsEOF %d BufBegin: 0x%lx BufEnd: 0x%lx BufLogicalEnd: 0x%lx  headRoom %d  extraBytes %d recordSkip %d  ", 
                    hdfo->fileName(), hdfo->getStartOffset(), hdfo->bytesToRead_, retArray_[BYTES_COMPLETED], retArray_[RANGE_NO], retArray_[IS_EOF], bufBegin_ , bufEnd_, bufLogicalEnd_, headRoom_, extraBytesRead_, recordSkip_);
             // If the first record starts after the logical end, this record should have been processed by other ESPs
             if ((BYTE *)hdfsBufNextRow_ > bufLogicalEnd_) {
                headRoomCopied_ = 0;
                hdfsBufNextRow_ = NULL;
             }
             step_ = PROCESS_HDFS_ROW;
          }
          break;
        case COPY_TAIL_TO_HEAD:
          {
             BYTE *headRoomStartAddr;
             headRoomCopied_ = bufLogicalEnd_ - (BYTE *)hdfsBufNextRow_;

             // make sure the tail is not unexpectedly long (otherwise we might
             // overrun the beginning of our buffer)
             if (headRoomCopied_ > headRoom_)
               {
                 ComDiagsArea * diagsArea = NULL;
                 ExRaiseSqlError(getHeap(), &diagsArea, 
                                 EXE_HIVE_ROW_TOO_LONG, 
                                 NULL, NULL, NULL, NULL, 
                                 NULL, NULL);
                 pentry_down->setDiagsArea(diagsArea);
                 step_ = HANDLE_ERROR;
                 break;
               }

             if (retArray_[BUF_NO] == 0)
                headRoomStartAddr = hdfsScanBuf_[1].buf_ - headRoomCopied_;
             else
                headRoomStartAddr = hdfsScanBuf_[0].buf_ - headRoomCopied_;
             memcpy(headRoomStartAddr, hdfsBufNextRow_, headRoomCopied_);
             step_ = TRAF_HDFS_READ;  
          }
          break;
        case STOP_HDFS_SCAN:
          {
            hdfsScanRetCode = (hdfsScan_ ? hdfsScan_->stop() : HDFS_SCAN_OK);
             if (hdfsScanRetCode != HDFS_SCAN_OK) {
                setupError(EXE_ERROR_HDFS_SCAN, hdfsScanRetCode, "HdfsScan::stop", 
                              GetCliGlobals()->getJniErrorStr(), NULL);              
                step_ = HANDLE_ERROR_AND_DONE;
             }    
             step_ = DONE;
          }
          break;

	case PROCESS_HDFS_ROW:
	{
          if (hdfsBufNextRow_ == NULL) {
             step_ = TRAF_HDFS_READ;
             break;
          } 
	  exception_ = FALSE;
	  nextStep_ = NOT_STARTED;
	  debugPenultimatePrevRow_ = debugPrevRow_;
	  debugPrevRow_ = hdfsBufNextRow_;

	  int formattedRowLength = 0;
	  ComDiagsArea *transformDiags = NULL;
	  int err = 0;
	  char *startOfNextRow =
	      extractAndTransformAsciiSourceToSqlRow(err, transformDiags, hdfsScanTdb().getHiveScanMode());
          QRLogger::log(CAT_SQL_EXE, LL_DEBUG, "HdfsBufRow 0x%lx StartOfNextRow 0x%lx RowLength %ld ", hdfsBufNextRow_, startOfNextRow, 
                                    startOfNextRow-hdfsBufNextRow_);
	  bool rowWillBeSelected = true;
	  lastErrorCnd_ = NULL;
	  if(err)
	  {
	    if (hdfsScanTdb().continueOnError())
	    {
	      Lng32 errorCount = workAtp_->getDiagsArea()->getNumber(DgSqlCode::ERROR_);
              if (errorCount>0)
	        lastErrorCnd_ = workAtp_->getDiagsArea()->getErrorEntry(errorCount);
	      exception_ = TRUE;
	      rowWillBeSelected = false;
	    }
	    else
	    {
	      if (transformDiags)
	        pentry_down->setDiagsArea(transformDiags);
	      step_ = HANDLE_ERROR_AND_DONE;
	      break;
	    }
	  }

	  if (startOfNextRow == NULL)
	  {
            if (retArray_[IS_EOF]) { 
               headRoomCopied_ = 0; 
               step_ = TRAF_HDFS_READ;
            }
            else
               step_ = COPY_TAIL_TO_HEAD;
            // Looks like we can break always
	    if (!exception_)
	       break;
	  }
	  else
	  {
	     Int64 thisRowLen = startOfNextRow - hdfsBufNextRow_;
             if (virtColData_)
             {
                virtColData_->block_offset_inside_file += thisRowLen;
                virtColData_->row_number_in_range++;
             }
             if ((BYTE *)startOfNextRow > bufLogicalEnd_) {
                headRoomCopied_ = 0;
                hdfsBufNextRow_ = NULL;
             } else 
	       hdfsBufNextRow_ = startOfNextRow;
	  }
           
	  if (exception_)
	  {
	    nextStep_ = step_;
	    step_ = HANDLE_EXCEPTION;
	    break;
	  }
	  if (hdfsStats_)
	    hdfsStats_->incAccessedRows();

	  workAtp_->getTupp(hdfsScanTdb().workAtpIndex_) =
	      hdfsSqlTupp_;

	  if ((rowWillBeSelected) && (selectPred()))
	  {
	    ex_expr::exp_return_type evalRetCode =
	        selectPred()->eval(pentry_down->getAtp(), workAtp_);
	    if (evalRetCode == ex_expr::EXPR_FALSE)
	      rowWillBeSelected = false;
	    else if (evalRetCode == ex_expr::EXPR_ERROR)
	    {
	      if (hdfsScanTdb().continueOnError())
	      {
                if (pentry_down->getDiagsArea() || workAtp_->getDiagsArea())
                {
                  Lng32 errorCount = 0;
                  if (pentry_down->getDiagsArea())
                  {
                    errorCount = pentry_down->getDiagsArea()->getNumber(DgSqlCode::ERROR_);
                    if (errorCount > 0)
                      lastErrorCnd_ = pentry_down->getDiagsArea()->getErrorEntry(errorCount);
                  }
                  else
                  {
                    errorCount = workAtp_->getDiagsArea()->getNumber(DgSqlCode::ERROR_);
                    if (errorCount > 0)
                      lastErrorCnd_ = workAtp_->getDiagsArea()->getErrorEntry(errorCount);
                  }
                }
                 exception_ = TRUE;
                 nextStep_ = step_;
                 step_ = HANDLE_EXCEPTION;

	        rowWillBeSelected = false;

	        break;
	      }
	      step_ = HANDLE_ERROR_AND_DONE;
	      break;
	    }
	    else
	      ex_assert(evalRetCode == ex_expr::EXPR_TRUE,
	          "invalid return code from expr eval");
	  }

	  if (rowWillBeSelected)
	  {
	    if (moveColsConvertExpr())
	    {
	      ex_expr::exp_return_type evalRetCode =
	          moveColsConvertExpr()->eval(workAtp_, workAtp_);
	      if (evalRetCode == ex_expr::EXPR_ERROR)
	      {
	        if (hdfsScanTdb().continueOnError())
	        {
                  if ( workAtp_->getDiagsArea())
                  {
                    Lng32 errorCount = 0;
                    errorCount = workAtp_->getDiagsArea()->getNumber(DgSqlCode::ERROR_);
                    if (errorCount > 0)
                      lastErrorCnd_ = workAtp_->getDiagsArea()->getErrorEntry(errorCount);
                  }
                   exception_ = TRUE;
                   nextStep_ = step_;
                   step_ = HANDLE_EXCEPTION;
	          break;
	        }
	        step_ = HANDLE_ERROR_AND_DONE;
	        break;
	      }
	    }
	    if (hdfsStats_)
	      hdfsStats_->incUsedRows();

            step_ = RETURN_ROW;
	    break;
	  }

	  break;
	}
	case RETURN_ROW:
	{
	  if (qparent_.up->isFull())
	    return WORK_OK;

	  lastErrorCnd_  = NULL;

	  ex_queue_entry *up_entry = qparent_.up->getTailEntry();
	  queue_index saveParentIndex = up_entry->upState.parentIndex;
	  queue_index saveDownIndex  = up_entry->upState.downIndex;

	  up_entry->copyAtp(pentry_down);

	  up_entry->upState.parentIndex =
	      pentry_down->downState.parentIndex;
	  up_entry->upState.downIndex = qparent_.down->getHeadIndex();
	  up_entry->upState.status = ex_queue::Q_OK_MMORE;

	  if (moveExpr())
	  {
	    UInt32 maxRowLen = hdfsScanTdb().outputRowLength_;
	    UInt32 rowLen = maxRowLen;

	    if (hdfsScanTdb().useCifDefrag() &&
	        !pool_->currentBufferHasEnoughSpace((Lng32)hdfsScanTdb().outputRowLength_))
	    {
	      up_entry->getTupp(hdfsScanTdb().tuppIndex_) = defragTd_;
	      defragTd_->setReferenceCount(1);
	      ex_expr::exp_return_type evalRetCode =
	          moveExpr()->eval(up_entry->getAtp(), workAtp_,0,-1,&rowLen);
	      if (evalRetCode ==  ex_expr::EXPR_ERROR)
	      {
                if (hdfsScanTdb().continueOnError())
                {
                  if ((pentry_down->downState.request == ex_queue::GET_N) &&
                      (pentry_down->downState.requestValue == matches_)) {
                     step_ = DONE;
                  }
                  else
                    step_ = PROCESS_HDFS_ROW;

                  up_entry->upState.parentIndex =saveParentIndex  ;
                  up_entry->upState.downIndex = saveDownIndex  ;
                  if (up_entry->getDiagsArea() || workAtp_->getDiagsArea())
                  {
                    Lng32 errorCount = 0;
                    if (up_entry->getDiagsArea())
                    {
                      errorCount = up_entry->getDiagsArea()->getNumber(DgSqlCode::ERROR_);
                      if (errorCount > 0)
                        lastErrorCnd_ = up_entry->getDiagsArea()->getErrorEntry(errorCount);
                    }
                    else
                    {
                      errorCount = workAtp_->getDiagsArea()->getNumber(DgSqlCode::ERROR_);
                      if (errorCount > 0)
                        lastErrorCnd_ = workAtp_->getDiagsArea()->getErrorEntry(errorCount);
                    }
                  }
                   exception_ = TRUE;
                   nextStep_ = step_;
                   step_ = HANDLE_EXCEPTION;
                  break;
                }
	        else
	        {
	          // Get diags from up_entry onto pentry_down, which
	          // is where the HANDLE_ERROR step expects it.
	          ComDiagsArea *diagsArea = pentry_down->getDiagsArea();
	          if (diagsArea == NULL)
	          {
	            diagsArea =
	                ComDiagsArea::allocate(getGlobals()->getDefaultHeap());
	            pentry_down->setDiagsArea (diagsArea);
	          }
	          pentry_down->getDiagsArea()->
	              mergeAfter(*up_entry->getDiagsArea());
	          up_entry->setDiagsArea(NULL);
	          step_ = HANDLE_ERROR_AND_DONE;
	          break;
	        }
	        if (pool_->get_free_tuple(
	            up_entry->getTupp(hdfsScanTdb().tuppIndex_),
	            rowLen))
	          return WORK_POOL_BLOCKED;
	        str_cpy_all(up_entry->getTupp(hdfsScanTdb().tuppIndex_).getDataPointer(),
	            defragTd_->getTupleAddress(),
	            rowLen);
	      }
	    }
	    else
	    {
	      if (pool_->get_free_tuple(
	          up_entry->getTupp(hdfsScanTdb().tuppIndex_),
	          (Lng32)hdfsScanTdb().outputRowLength_))
	        return WORK_POOL_BLOCKED;
	      ex_expr::exp_return_type evalRetCode =
	          moveExpr()->eval(up_entry->getAtp(), workAtp_,0,-1,&rowLen);
	      if (evalRetCode ==  ex_expr::EXPR_ERROR)
	      {
	        if (hdfsScanTdb().continueOnError())
	        {
	          if ((pentry_down->downState.request == ex_queue::GET_N) &&
	              (pentry_down->downState.requestValue == matches_)) {
                     step_ = DONE;
                  }
	          else
	            step_ = PROCESS_HDFS_ROW;

                  if (up_entry->getDiagsArea() || workAtp_->getDiagsArea())
                  {
                    Lng32 errorCount = 0;
                    if (up_entry->getDiagsArea())
                    {
                      errorCount = up_entry->getDiagsArea()->getNumber(DgSqlCode::ERROR_);
                      if (errorCount > 0)
                        lastErrorCnd_ = up_entry->getDiagsArea()->getErrorEntry(errorCount);
                    }
                    else
                    {
                      errorCount = workAtp_->getDiagsArea()->getNumber(DgSqlCode::ERROR_);
                      if (errorCount > 0)
                        lastErrorCnd_ = workAtp_->getDiagsArea()->getErrorEntry(errorCount);
                    }
                  }
                  up_entry->upState.parentIndex =saveParentIndex  ;
                  up_entry->upState.downIndex = saveDownIndex  ;
                  exception_ = TRUE;
                  nextStep_ = step_;
                  step_ = HANDLE_EXCEPTION;
	          break;
	        }
	        else
	        {
	          // Get diags from up_entry onto pentry_down, which
	          // is where the HANDLE_ERROR step expects it.
	          ComDiagsArea *diagsArea = pentry_down->getDiagsArea();
	          if (diagsArea == NULL)
	          {
	            diagsArea =
	                ComDiagsArea::allocate(getGlobals()->getDefaultHeap());
	            pentry_down->setDiagsArea (diagsArea);
	          }
	          pentry_down->getDiagsArea()->
	              mergeAfter(*up_entry->getDiagsArea());
	          up_entry->setDiagsArea(NULL);
	          step_ = HANDLE_ERROR;
	          break;
	        }
	      }
	      if (hdfsScanTdb().useCif() && rowLen != maxRowLen)
	      {
	        pool_->resizeLastTuple(rowLen,
	            up_entry->getTupp(hdfsScanTdb().tuppIndex_).getDataPointer());
	      }
	    }
          }
	  up_entry->upState.setMatchNo(++matches_);
	  if (matches_ == matchBrkPoint_)
	    brkpoint();
	  qparent_.up->insert();

	  // use ExOperStats now, to cover OPERATOR stats as well as
	  // ALL stats.
	  if (getStatsEntry())
	    getStatsEntry()->incActualRowsReturned();

	  workAtp_->setDiagsArea(NULL);    // get rid of warnings.
          // Mantis-622, enabling cancel
          if (((pentry_down->downState.request == ex_queue::GET_N) &&
               (pentry_down->downState.requestValue == matches_)) ||
              (pentry_down->downState.request == ex_queue::GET_NOMORE)) {
              step_ = DONE;
          }
          else
	     step_ = PROCESS_HDFS_ROW;
	  break;
	}
	case HANDLE_EXCEPTION:
	{
	  step_ = nextStep_;
	  exception_ = FALSE;

	  Int64 exceptionCount = 0;
          ExHbaseAccessTcb::incrErrorCount( ehi_,exceptionCount, 
                 hdfsScanTdb().getErrCountTable(),hdfsScanTdb().getErrCountRowId()); 
	  if (hdfsScanTdb().getMaxErrorRows() > 0)
	  {
	    if (exceptionCount >  hdfsScanTdb().getMaxErrorRows())
	    {
	      if (pentry_down->getDiagsArea())
	        pentry_down->getDiagsArea()->clear();
	      if (workAtp_->getDiagsArea())
	        workAtp_->getDiagsArea()->clear();

	      ComDiagsArea *da = workAtp_->getDiagsArea();
	      if(!da)
	      {
	        da = ComDiagsArea::allocate(getHeap());
	        workAtp_->setDiagsArea(da);
	      }
              ExRaiseSqlError(getHeap(), &da,
                (ExeErrorCode)(EXE_MAX_ERROR_ROWS_EXCEEDED));
	      step_ = HANDLE_ERROR_AND_DONE;
	      break;
	    }
	  }
          if (hdfsScanTdb().getLogErrorRows())
          {
            int loggingRowLen =  hdfsLoggingRowEnd_ - hdfsLoggingRow_ +1;
            handleException((NAHeap *)getHeap(), hdfsLoggingRow_,
                       loggingRowLen, lastErrorCnd_ );

          }

          if (pentry_down->getDiagsArea())
            pentry_down->getDiagsArea()->clear();
          if (workAtp_->getDiagsArea())
            workAtp_->getDiagsArea()->clear();
	}
	break;

	case HANDLE_ERROR:
	case HANDLE_ERROR_AND_DONE:
	  {
	    if (qparent_.up->isFull())
	      return WORK_OK;
	    ex_queue_entry *up_entry = qparent_.up->getTailEntry();
	    up_entry->copyAtp(pentry_down);
	    up_entry->upState.parentIndex =
	      pentry_down->downState.parentIndex;
	    up_entry->upState.downIndex = qparent_.down->getHeadIndex();
	    if (workAtp_->getDiagsArea())
	      {
		ComDiagsArea *diagsArea = up_entry->getDiagsArea();
		
		if (diagsArea == NULL)
		  {
		    diagsArea = 
		      ComDiagsArea::allocate(getGlobals()->getDefaultHeap());

		    up_entry->setDiagsArea (diagsArea);
		  }

		up_entry->getDiagsArea()->mergeAfter(*workAtp_->getDiagsArea());
		workAtp_->setDiagsArea(NULL);
	      }
	    up_entry->upState.status = ex_queue::Q_SQLERROR;
	    qparent_.up->insert();
          
            step_ = DONE;
	    break;
	  }

	case DONE:
	  {
	    if (qparent_.up->isFull())
	      return WORK_OK;
            if (logFileHdfsClient_ != NULL)
               retcode = logFileHdfsClient_->hdfsClose();
	    ex_queue_entry *up_entry = qparent_.up->getTailEntry();
	    up_entry->copyAtp(pentry_down);
	    up_entry->upState.parentIndex =
	      pentry_down->downState.parentIndex;
	    up_entry->upState.downIndex = qparent_.down->getHeadIndex();
	    up_entry->upState.status = ex_queue::Q_NO_DATA;
	    up_entry->upState.setMatchNo(matches_);
            if (loggingErrorDiags_ != NULL)
            {
               ComDiagsArea * diagsArea = up_entry->getDiagsArea();
               if (!diagsArea)
               {
                  diagsArea =
                   ComDiagsArea::allocate(getGlobals()->getDefaultHeap());
                  up_entry->setDiagsArea(diagsArea);
               }
               diagsArea->mergeAfter(*loggingErrorDiags_);
               loggingErrorDiags_->clear();
            }
	    qparent_.up->insert();
	    
	    qparent_.down->removeHead();
	    step_ = NOT_STARTED;
	    break;
	  }
	  
	default: 
	  {
	    break;
	  }
	  
	} // switch
    } // while
  
  return WORK_OK;
}

char * ExHdfsScanTcb::extractAndTransformAsciiSourceToSqlRow(int &err,
							     ComDiagsArea* &diagsArea, int mode)
{
  err = 0;
  char *sourceData = hdfsBufNextRow_;
  char *sourceRowEnd = NULL; 
  char *sourceColEnd = NULL;
  int changedLen = 0;
  NABoolean isTrailingMissingColumn = FALSE;
  ExpTupleDesc * asciiSourceTD =
     hdfsScanTdb().workCriDesc_->getTupleDescriptor(hdfsScanTdb().asciiTuppIndex_);

  ExpTupleDesc * origSourceTD = 
    hdfsScanTdb().workCriDesc_->getTupleDescriptor(hdfsScanTdb().origTuppIndex_);
  
  const char cd = hdfsScanTdb().columnDelimiter_;
  const char rd = hdfsScanTdb().recordDelimiter_;
  const char *sourceDataEnd;
  const char *endOfRequestedRange;
  sourceDataEnd = (const char *)bufEnd_;
  endOfRequestedRange = NULL;
  hdfsLoggingRow_ = hdfsBufNextRow_;
  if (asciiSourceTD->numAttrs() == 0)
  {
     sourceRowEnd = hdfs_strchr(sourceData, rd, sourceDataEnd, checkRangeDelimiter_, mode, &changedLen);
     hdfsLoggingRowEnd_  = sourceRowEnd + changedLen;
     
     if (sourceRowEnd == NULL) {
        return NULL; 
     }
     // no columns need to be converted. For e.g. count(*) with no predicate
     return sourceRowEnd+1;
  }

  Lng32 neededColIndex = 0;
  Attributes * attr = NULL;
  Attributes * tgtAttr = NULL;
  NABoolean rdSeen = FALSE;

  for (Lng32 i = 0; i <  hdfsScanTdb().convertSkipListSize_; i++)
    {
      // all remainin columns wil be skip columns, don't bother
      // finding their column delimiters
      if (neededColIndex == asciiSourceTD->numAttrs())
        continue;

      tgtAttr = NULL;
      if (hdfsScanTdb().convertSkipList_[i] > 0)
      {
        attr = asciiSourceTD->getAttr(neededColIndex);
        tgtAttr = origSourceTD->getAttr(neededColIndex);
        neededColIndex++;
      }
      else
        {
          attr = NULL;
        }

      if (!isTrailingMissingColumn) {
         sourceColEnd = hdfs_strchr(sourceData, rd, cd, sourceDataEnd, checkRangeDelimiter_, &rdSeen,mode, &changedLen);
         if (sourceColEnd == NULL) {
	   if (rdSeen || (sourceRowEnd == NULL))
               return NULL;
	   else
               return sourceRowEnd+1;
         }
         Int32 len = 0;
	 len = (Int64)sourceColEnd - (Int64)sourceData;
         if (rdSeen) {
            sourceRowEnd = sourceColEnd + changedLen; 
            hdfsLoggingRowEnd_  = sourceRowEnd;
            if ((endOfRequestedRange) && 
                   (sourceRowEnd >= endOfRequestedRange)) {
               checkRangeDelimiter_ = TRUE;
               *(sourceRowEnd +1)= RANGE_DELIMITER;
            }
            if (i != hdfsScanTdb().convertSkipListSize_ - 1)
               isTrailingMissingColumn = TRUE;
         }

         if (attr) // this is a needed column. We need to convert
         {
           if (attr->getVCIndicatorLength() == sizeof(short))
             *(short*)&hdfsAsciiSourceData_[attr->getVCLenIndOffset()] 
               = (short)len;
           else
             *(Int32*)&hdfsAsciiSourceData_[attr->getVCLenIndOffset()] 
               = len;

            if (attr->getNullFlag())
            {
              *(short *)&hdfsAsciiSourceData_[attr->getNullIndOffset()] = 0;
              if (hdfsScanTdb().getNullFormat()) // null format specified by user
                {
                  if (((len == 0) && (strlen(hdfsScanTdb().getNullFormat()) == 0)) ||
                      ((len > 0) && (memcmp(sourceData, hdfsScanTdb().getNullFormat(), len) == 0)))
                    {
                       *(short *)&hdfsAsciiSourceData_[attr->getNullIndOffset()] = -1;
                    }
                } // if
              else // null format not specified by user
                {
                  // Use default null format.
                  // for non-varchar, length of zero indicates a null value.
                  // For all datatypes, HIVE_DEFAULT_NULL_STRING('\N') indicates a null value.
                  if (((len == 0) && (tgtAttr && (NOT DFS2REC::isSQLVarChar(tgtAttr->getDatatype())))) ||
                      ((len == strlen(HIVE_DEFAULT_NULL_STRING)) && (memcmp(sourceData, HIVE_DEFAULT_NULL_STRING, len) == 0)))
                    {
                      *(short *)&hdfsAsciiSourceData_[attr->getNullIndOffset()] = -1;
                    }
                } // else
            } // if nullable attr

            if (len > 0)
            {
              // move address of data into the source operand.
              // convertExpr will dereference this addr and get to the actual
              // data.
              *(Int64*)&hdfsAsciiSourceData_[attr->getOffset()] =
                (Int64)sourceData;
            }
            else
            {
              *(Int64*)&hdfsAsciiSourceData_[attr->getOffset()] =
                (Int64)0;

            }
          } // if(attr)
	} // if (!trailingMissingColumn)
      else
	{
	  //  A delimiter was found, but not enough columns.
	  //  Treat the rest of the columns as NULL.
          if (attr && attr->getNullFlag())
            *(short *)&hdfsAsciiSourceData_[attr->getNullIndOffset()] = -1;
	}
      sourceData = sourceColEnd + 1 ;
    }
  // It is possible that the above loop came out before
  // rowDelimiter is encountered
  // So try to find the record delimiter
  if (sourceRowEnd == NULL) {
     sourceRowEnd = hdfs_strchr(sourceData, rd, sourceDataEnd, checkRangeDelimiter_,mode, &changedLen);
     if (sourceRowEnd) {
        hdfsLoggingRowEnd_  = sourceRowEnd + changedLen; //changedLen is when hdfs_strchr move the return pointer to remove the extra \r
        if ((endOfRequestedRange) &&
              (sourceRowEnd >= endOfRequestedRange )) {
           checkRangeDelimiter_ = TRUE;
          *(sourceRowEnd +1)= RANGE_DELIMITER;
        }
     }
  }

  workAtp_->getTupp(hdfsScanTdb().workAtpIndex_) = hdfsSqlTupp_;
  workAtp_->getTupp(hdfsScanTdb().asciiTuppIndex_) = hdfsAsciiSourceTupp_;
  workAtp_->getTupp(hdfsScanTdb().moveExprColsTuppIndex_) = moveExprColsTupp_;

  if (convertExpr())
  {
    ex_expr::exp_return_type evalRetCode =
      convertExpr()->eval(workAtp_, workAtp_);
    if (evalRetCode == ex_expr::EXPR_ERROR)
      err = -1;
    else
      err = 0;
  }
  if (sourceRowEnd)
     return sourceRowEnd+1;
  return NULL;
}

void ExHdfsScanTcb::initPartAndVirtColData(HdfsFileInfo* hdfo,
                                           Lng32 rangeNum,
                                           bool prefetch)
{
  if (partColData_)
    {
      ex_assert(hdfo->getPartColValues(),
                "Missing part col values");
      memcpy(partColData_,
             hdfo->getPartColValues(),
             hdfsScanTdb().partColsRowLength_);
      workAtp_->getTupp(hdfsScanTdb().partColsTuppIndex_) = partColTupp_;
    }
  if (virtColData_)
    {
      int fileNameLen = strlen(hdfo->fileName());
      str_cpy(virtColData_->input_file_name,
              hdfo->fileName(),
              sizeof(virtColData_->input_file_name));
      // truncate the file name (should not happen)
      if (fileNameLen > sizeof(virtColData_->input_file_name))
        fileNameLen = sizeof(virtColData_->input_file_name);
      virtColData_->input_file_name_len = (UInt16) fileNameLen;
      virtColData_->input_range_number = rangeNum;
      if (!prefetch)
        {
          virtColData_->block_offset_inside_file = hdfo->getStartOffset();
          virtColData_->row_number_in_range = -1;
        }
      workAtp_->getTupp(hdfsScanTdb().virtColsTuppIndex_) = virtColTupp_;
    }
}

int ExHdfsScanTcb::getAndInitNextSelectedRange(bool prefetch, bool selectRangesInAdvance)
{
  HdfsFileInfo* hdfo = NULL;
  Lng32 rangeNum = currRangeNum_;
  bool rangeIsSelected = false;
  bool restorePartAndVirtCols = false;
  int i = 0;

  if (prefetch)
    rangeNum++;


  QRLogger::log(CAT_SQL_HDFS_SCAN,
                LL_DEBUG,
                "ExHdfsScanTcb::getAndInitNextSelectedRange() range list entries=%d, start range=%d, end range=%d.",
                getHdfsFileInfoListAsArray().entries(),
                rangeNum, beginRangeNum_ + numRanges_
                );
  
  while (rangeNum < (beginRangeNum_ + numRanges_))
    {
      hdfo = getRange(rangeNum);
      rangeIsSelected = FALSE;

      // eliminate empty ranges (e.g. sqoop-generated files)
      if (hdfo->getBytesToRead() > 0)
        {
          // initialize partition and virtual column with the
          // values for this range and see whether it qualifies
          if (!prefetch || partElimExpr())
            {
              initPartAndVirtColData(hdfo, rangeNum, prefetch);
              restorePartAndVirtCols = prefetch;
            }

          if (partElimExpr())
            {
              // Try to eliminate the entire range based on the
              // partition elimination expression. Note that we call
              // it partition elimination expression, but we will
              // actually call it for each range, and it can
              // reference the INPUT__RANGE__NUMBER virtual column,
              // which is specific to the range.


              // the min/max 
              // qparent_.down->getHeadEntry()->getAtp()->displayMinMaxTupp("PEelimExpr: argument", 6);

              // The current partition x to be determined if minmax(x) is TRUE
              // workAtp_->dump("partition value stored in workAtp", 
              //                  hdfsScanTdb().partColsTuppIndex_);

              ex_expr::exp_return_type evalRetCode =
                partElimExpr()->eval(qparent_.down->getHeadEntry()->getAtp(),
                                     workAtp_);
              
              if (evalRetCode == ex_expr::EXPR_TRUE)
                rangeIsSelected = true;
            }
          else
            rangeIsSelected = true;
        }

      if (!selectRangesInAdvance) {
         if (rangeIsSelected)
            break;
         else
            rangeNum++;
      }
      else {
         rangeNum++;
         if (rangeIsSelected)
            instanceHdfsFileInfoArray_.insertAt(i++, hdfo);
      }
    }

    if (rangeIsSelected && !prefetch && !selectRangesInAdvance)
      {
        // prepare the TDB to scan this new, selected, range
        currRangeNum_ = rangeNum;
        hdfo_         = hdfo;
        hdfsFileName_ = hdfo_->fileName();
        hdfsOffset_   = hdfo_->getStartOffset();
        bytesLeft_    = hdfo_->getBytesToRead();
        // part and virt columns have already been set above
        //cout << "File name in the selected range=" << hdfsFileName_ << endl;
      }

    if (restorePartAndVirtCols || selectRangesInAdvance)
      {
        // Put back the original partition and virtual columns
        initPartAndVirtColData(
             getRange(currRangeNum_),
             currRangeNum_,
             prefetch);
      }

#if 0
     fstream& out1 = getPrintHandle();
     out1  << "Selected: hdfsFileName_=" << hdfsFileName_
          << ", hdfsOffset_=" << hdfsOffset_ 
          << ", bytesLeft_(bytesToRead)=" << bytesLeft_
          << endl;
     out1.close();
#endif

    if (rangeIsSelected) {
      return rangeNum;
    } else {
      return -1;
    }
}

void ExHdfsScanTcb::computeRangesAtRuntime()
{
  Int64 totalSize = 0;
  Int64 myShare = 0;
  Int64 runningSum = 0;
  Int64 myStartPositionInBytes = 0;
  Int64 myEndPositionInBytes = 0;
  Int64 firstFileStartingOffset = 0;
  Int64 lastFileBytesToRead = -1;
  Int32 numParallelInstances = MAXOF(getGlobals()->getNumOfInstances(),1);

  HDFS_FileInfo *fileInfos;
  HDFS_Client_RetCode hdfsClientRetcode;

  hdfsClientRetcode = hdfsClient_->hdfsListDirectory(hdfsScanTdb().hdfsRootDir_, &fileInfos, &numFiles_); 
  ex_assert(hdfsClientRetcode == HDFS_CLIENT_OK, "Internal error:hdfsClient->hdfsListDirectory returned an error")

  deallocateRuntimeRanges();

  // in a first round, count the total number of bytes
  for (int f=0; f<numFiles_; f++)
    {
      ex_assert(fileInfos[f].mKind == HDFS_FILE_KIND,
                "subdirectories not supported with runtime HDFS ranges");
      totalSize += (Int64) fileInfos[f].mSize;
    }

  // compute my share, in bytes
  // (the last of the ESPs may read a bit more)
  myShare = totalSize / numParallelInstances;
  myStartPositionInBytes = myInstNum_ * myShare;
  beginRangeNum_ = -1;
  numRanges_ = 0;

  if (totalSize > 0)
    {
      if (myInstNum_ < numParallelInstances-1)
        // read "myShare" bytes
        myEndPositionInBytes = myStartPositionInBytes + myShare;
      else
        // the last ESP reads whatever is remaining
        myEndPositionInBytes = totalSize;

      // second round, find out the range of files I need to read
      for (int g=0;
           g < numFiles_ && runningSum < myEndPositionInBytes;
           g++)
        {
          Int64 prevSum = runningSum;

          runningSum += (Int64) fileInfos[g].mSize;

          if (runningSum >= myStartPositionInBytes)
            {
              if (beginRangeNum_ < 0)
                {
                  // I have reached the first file that I need to read
                  beginRangeNum_ = g;
                  firstFileStartingOffset =
                    myStartPositionInBytes - prevSum;
                }

              numRanges_++;

              if (runningSum > myEndPositionInBytes)
                // I don't need to read all the way to the end of this file
                lastFileBytesToRead = myEndPositionInBytes - prevSum;

            } // file is at or beyond my starting file
        } // loop over files, determining ranges
    } // total size > 0
  else
    beginRangeNum_ = 0;

  // third round, populate the ranges that this ESP needs to read
  for (int h=beginRangeNum_; h<beginRangeNum_+numRanges_; h++)
    {
      HdfsFileInfo *e = new(getHeap()) HdfsFileInfo;
      const char *fileName = fileInfos[h].mName;
      Int32 fileNameLen = strlen(fileName) + 1;

      e->entryNum_ = h;
      e->flags_    = 0;
      e->fileName_ = new(getHeap()) char[fileNameLen];
      str_cpy_all(e->fileName_, fileName, fileNameLen);
      if (h == beginRangeNum_ &&
          firstFileStartingOffset > 0)
        {
          e->startOffset_ = firstFileStartingOffset;
          e->setFileIsSplitBegin(TRUE);
        }
      else
        e->startOffset_ = 0;

      
      if (h == beginRangeNum_+numRanges_-1 && lastFileBytesToRead > 0)
        {
          e->bytesToRead_ = lastFileBytesToRead;
          e->setFileIsSplitEnd(TRUE);
        }
      else
        e->bytesToRead_ = (Int64) fileInfos[h].mSize;
      e->compressionTypeIx_ = 0;
      // Runtime ranges requires the file to be uncompressed currently
      e->compressionMethod_ = ComCompressionInfo::UNCOMPRESSED;

      hdfsFileInfoListAsArray_.insertAt(h, e);
    }

  // All entries have compression type index 0, and the
  // compressionWA_ is NULL, meaning uncompressed files.
  // There is no list of ComCompressionInfo objects.
  currCompressionTypeIx_ = 0;
}

void ExHdfsScanTcb::deallocateRuntimeRanges()
{
  if (hdfsScanTdb().getAssignRangesAtRuntime() &&
      instanceHdfsFileInfoArray_.entries() > 0)
   {
      for (int i=0; i < instanceHdfsFileInfoArray_.getUsedLength(); i++)
        if (instanceHdfsFileInfoArray_.used(i))
          {
            NADELETEBASIC (instanceHdfsFileInfoArray_[i]->fileName_.getPointer(), getHeap());
            NADELETEBASIC (instanceHdfsFileInfoArray_[i], getHeap());
          }
      instanceHdfsFileInfoArray_.clear();
    }
}

void ExHdfsScanTcb::handleException(NAHeap *heap,
                                    char *logErrorRow,
                                    Lng32 logErrorRowLen,
                                    ComCondition *errorCond)
{
  Lng32 errorMsgLen = 0;
  charBuf *cBuf = NULL;
  char *errorMsg;
  HDFS_Client_RetCode hdfsClientRetcode;

  if (loggingErrorDiags_ != NULL)
     return;

  if (!loggingFileCreated_) {
     logFileHdfsClient_ = HdfsClient::newInstance((NAHeap *)getHeap(), NULL, hdfsClientRetcode);
     if (hdfsClientRetcode == HDFS_CLIENT_OK)
        hdfsClientRetcode = logFileHdfsClient_->hdfsCreate(loggingFileName_, TRUE, FALSE, FALSE);
     if (hdfsClientRetcode == HDFS_CLIENT_OK)
        loggingFileCreated_ = TRUE;
     else 
        goto logErrorReturn;
  }
  logFileHdfsClient_->hdfsWrite(logErrorRow, logErrorRowLen, hdfsClientRetcode);
  if (hdfsClientRetcode != HDFS_CLIENT_OK) 
     goto logErrorReturn;
  if (errorCond != NULL) {
     errorMsgLen = errorCond->getMessageLength();
     const NAWcharBuf wBuf((NAWchar*)errorCond->getMessageText(), errorMsgLen, heap);
     cBuf = unicodeToISO88591(wBuf, heap, cBuf);
     errorMsg = (char *)cBuf->data();
     errorMsgLen = cBuf -> getStrLen();
     errorMsg[errorMsgLen]='\n';
     errorMsgLen++;
  }
  else {
     errorMsg = (char *)"[UNKNOWN EXCEPTION]\n";
     errorMsgLen = strlen(errorMsg);
  }
  logFileHdfsClient_->hdfsWrite(errorMsg, errorMsgLen, hdfsClientRetcode);
logErrorReturn:
  if (hdfsClientRetcode != HDFS_CLIENT_OK) {
     loggingErrorDiags_ = ComDiagsArea::allocate(heap);
     *loggingErrorDiags_ << DgSqlCode(EXE_ERROR_WHILE_LOGGING)
                 << DgString0(loggingFileName_)
                 << DgString1((char *)GetCliGlobals()->getJniErrorStr());
  }
}


