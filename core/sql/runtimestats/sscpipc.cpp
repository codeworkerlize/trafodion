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
// File:         sscpipc.cpp
// Description:  Class declaration for SSCP IPC infrastructure
//
// Created:      5/02/2006
**********************************************************************/

#include "common/Platform.h"
#include "executor/ex_stdh.h"
#include "sscpipc.h"
#include "sqlmxevents/logmxevent.h"
#include "ExCextdecs.h"
#include <sys/ipc.h>
#include <sys/shm.h>
#include <semaphore.h>
#include "nsk/nskport.h"
#include "seabed/ms.h"
#include "seabed/fs.h"
#include "common/NAStdlib.h"
#include "zsysc.h"
#include "executor/ExStats.h"
#include "runtimestats/rts_msg.h"
#include "porting/PortProcessCalls.h"
#include "comexe/ComTdb.h"
#include "common/ComSqlId.h"
#include "common/ComDistribution.h"

SscpGlobals::SscpGlobals(NAHeap *sscpheap, StatsGlobals *statsGlobals)
  : heap_(sscpheap),
    statsGlobals_(statsGlobals)
  , doLogCancelKillServers_(false)
{
  int error;
  Int32 myCpu;
  char programDir[100];
  short processType;
  char myNodeName[MAX_SEGMENT_NAME_LEN+1];
  Lng32 myNodeNumber;
  short myNodeNameLen = MAX_SEGMENT_NAME_LEN;
  Int64 myStartTime;
  short pri;
  char myProcessNameString[PROCESSNAME_STRING_LEN];

  error = statsGlobals_->openStatsSemaphore(semId_);
  ex_assert(error == 0, "BINSEM_OPEN returned an error");


  error = ComRtGetProgramInfo(programDir, 100, processType,
    myCpu, myPin_,
    myNodeNumber, myNodeName, myNodeNameLen, myStartTime, myProcessNameString);
  ex_assert(error == 0,"Error in ComRtGetProgramInfo");

  pri = 0;
  error = statsGlobals_->getStatsSemaphore(semId_, myPin_);
  // ProcessHandle wrapper in porting layer library
  NAProcessHandle sscpPhandle;
  error = sscpPhandle.getmine(statsGlobals->getSscpProcHandle());

  statsGlobals_->setSscpPid(myPin_);
  statsGlobals_->setSscpPriority(pri);
  statsGlobals_->setSscpTimestamp(myStartTime);
  statsGlobals_->setSscpProcSemId(semId_);
  statsGlobals->setSscpInitialized(TRUE);
  NAHeap *statsHeap = (NAHeap *)statsGlobals_->getStatsHeap()->
        allocateHeapMemory(sizeof *statsHeap, FALSE);

  // The following assertion may be hit if the RTS shared memory
  // segment is full.  
  ex_assert(statsHeap, "allocateHeapMemory returned NULL.");

  // This next allocation, a placement "new" will not fail.
  statsHeap = new (statsHeap, statsGlobals_->getStatsHeap())
       NAHeap("Process Stats Heap", statsGlobals_->getStatsHeap(),
               8192,
               0);
  bool reincarnated = statsGlobals_->addProcess(myPin_, statsHeap);
  if (reincarnated)
     statsGlobals_->logProcessDeath(myCpu, myPin_, "Process reincarnated before RIP");

  statsGlobals_->releaseStatsSemaphore(semId_, myPin_);
  CliGlobals *cliGlobals = GetCliGlobals();
  cliGlobals->setSemId(semId_);
  cliGlobals->setStatsHeap(statsHeap);
  char defineName[24+1];
  short zeroMeansNo;
  str_cpy_all (defineName, "=_MX_RTS_LOG_KILL_SERVER", 24);
  if (((error = getDefineNumericValue(defineName, &zeroMeansNo)) == 0) &&
      (zeroMeansNo != 0))
    doLogCancelKillServers_ = true;
}

SscpGlobals::~SscpGlobals()
{
  sem_close((sem_t *)semId_);
}

void SscpGuaReceiveControlConnection::actOnSystemMessage(
       short                  messageNum,
       IpcMessageBufferPtr    sysMsg,
       IpcMessageObjSize      sysMsgLen,
       short                  clientFileNumber,
       const GuaProcessHandle &clientPhandle,
       GuaConnectionToClient  *connection)
{
  CliGlobals *cliGlobals = GetCliGlobals();
  switch (messageNum)
    {
    case ZSYS_VAL_SMSG_OPEN:
      {
        SscpNewIncomingConnectionStream *newStream = new(getEnv()->getHeap())
          SscpNewIncomingConnectionStream((NAHeap *)getEnv()->getHeap(),
              getEnv(),getSscpGlobals());

        ex_assert(connection != NULL,"Must create connection for open sys msg");
        newStream->addRecipient(connection);
        newStream->receive(FALSE);
        initialized_ = TRUE;
      }
      break;
    case ZSYS_VAL_SMSG_CLOSE:
      break;
    case ZSYS_VAL_SMSG_CPUDOWN:
    case ZSYS_VAL_SMSG_REMOTECPUDOWN:
    case ZSYS_VAL_SMSG_NODEDOWN:
      // Somebody closed us or went down. Do a search thru all
      // downloaded fragment entries and check whether their
      // client is still using them. The IPC layer will wake
      // up the scheduler so the actual release can take place.
      sscpGlobals_->releaseOrphanEntries();
      break;
    case XZSYS_VAL_SMSG_SHUTDOWN:
      sem_unlink(getRmsSemName());
      // Mark the shared memory segment for desctruction
      shmctl(cliGlobals->getSharedMemId(), IPC_RMID, NULL);
      NAExit(0);
      break;
    default:
      // do nothing for all other kinds of system messages
      break;
    } // switch


}

SscpNewIncomingConnectionStream::SscpNewIncomingConnectionStream(NAHeap *heap, IpcEnvironment *env,
                                SscpGlobals *sscpGlobals) :
                IpcMessageStream(env,
                   IPC_MSG_SSCP_REPLY,
		   CurrSscpReplyMessageVersion,
#ifndef USE_SB_NEW_RI
		   RTS_STATS_MSG_BUF_SIZE,
#else
		   env->getGuaMaxMsgIOSize(),
#endif
		   TRUE)
{
  heap_ = heap;
  sscpGlobals_ = sscpGlobals;
  ipcEnv_ = env;
  bytesReplied_ = 0;
}


void SscpNewIncomingConnectionStream::actOnSend(IpcConnection *connection)
{
}

void SscpNewIncomingConnectionStream::actOnSendAllComplete()
{
  clearAllObjects();
  receive(FALSE);
}

void SscpNewIncomingConnectionStream::actOnReceive(IpcConnection *connection)
{
  if (connection->getErrorInfo() != 0)
    return;

  sscpGlobals_->incSscpReqMsg(getBytesReceived());
  bytesReplied_ = 0;

  switch(getNextObjType())
  {
  case RTS_MSG_STATS_REQ:
    processStatsReq(connection);
    break;
  case RTS_MSG_CPU_STATS_REQ:
    processCpuStatsReq(connection);
    break;
  case CANCEL_QUERY_KILL_SERVERS_REQ:
    processKillServersReq();
    break;
  case SUSPEND_QUERY_REQ:
    suspendActivateSchedulers();
    break;
  case SECURITY_INVALID_KEY_REQ:
    processSecInvReq();
    break;
  case SNAPSHOT_LOCK_REQ:
    processSnapshotLockReq();
    break;
  case SNAPSHOT_UNLOCK_REQ:
    processSnapshotUnLockReq();
    break;

  case OBJECT_EPOCH_CHANGE_REQ:
    processObjectEpochChangeReq();
    break;
  case OBJECT_EPOCH_STATS_REQ:
    processObjectEpochStatsReq();
    break;
  case OBJECT_LOCK_REQ:
    processObjectLockReq();
    break;
  case OBJECT_LOCK_STATS_REQ:
    processObjectLockStatsReq();
    break;
  default:
    ex_assert(FALSE,"Invalid request for first client message");
  }
  sscpGlobals_->incSscpReplyMsg(bytesReplied_);
}

void SscpNewIncomingConnectionStream::actOnReceiveAllComplete()
{
  if (getState() == ERROR_STATE)
    addToCompletedList();
}
void SscpNewIncomingConnectionStream::processStatsReq(IpcConnection *connection)
{
  IpcMessageObjVersion msgVer;
  msgVer = getNextObjVersion();

  if (msgVer > currRtsStatsReqVersionNumber)
    // Send Error
    ;
  RtsStatsReq *request = new (getHeap())
      RtsStatsReq(INVALID_RTS_HANDLE, getHeap());

  *this >> *request;

  if (moreObjects())
  {
    RtsMessageObjType objType =
      (RtsMessageObjType) getNextObjType();

    switch (objType)
    {
    case RTS_QUERY_ID:
      {
        RtsQueryId *queryId = new (getHeap())
          RtsQueryId(getHeap());

        // Get the query Id from IPC
        *this >> *queryId;

        char *qid = queryId->getQid();
        short reqType = queryId->getStatsReqType();
        RtsStatsReply *reply = new (getHeap())
          RtsStatsReply(request->getHandle(), getHeap());

        SscpGlobals *sscpGlobals = getSscpGlobals();
        StatsGlobals *statsGlobals = sscpGlobals->getStatsGlobals();
        clearAllObjects();
        setType(IPC_MSG_SSCP_REPLY);
        setVersion(CurrSscpReplyMessageVersion);
        int error = statsGlobals->getStatsSemaphore(sscpGlobals->getSemId(),
                sscpGlobals->myPin());
        SyncHashQueue *stmtStatsList = statsGlobals->getStmtStatsList();
        StmtStats *stmtStats = statsGlobals->getStmtStats(qid, str_len(qid));
        ExStatisticsArea *stats;
        ExStatisticsArea *mergedStats = NULL;
        // stats Vptr is not belonging to this process
        // So avoid calling any virtual functions of this class or any embedded classes.
        // We create a mergeStats instance and create a
        // Operator stats entry for each stats entry and then call merge
        // This avoid calling any virtual function in the merge function
        // Also, for some reason, packing any object to the stream is allowed only when the
        // reference count is 1. It looks like some of the StatisticsArea has reference count
        // more than 1.
        while (stmtStats != NULL)
        {
          stats = stmtStats->getStatsArea();
          ComTdb::CollectStatsType statsType;
          if (stats != NULL && ((statsType = stats->getCollectStatsType()) == ComTdb::ACCUMULATED_STATS
                  || statsType == ComTdb::PERTABLE_STATS
                  || statsType == ComTdb::OPERATOR_STATS))
          {
            if (mergedStats == NULL)
            {
              switch (queryId->getStatsMergeType())
              {
              case SQLCLI_ACCUMULATED_STATS:
                 mergedStats = new (getHeap())
                    ExStatisticsArea(getHeap(), 0, ComTdb::ACCUMULATED_STATS,
                        stats->getOrigCollectStatsType());
                break;
              case SQLCLI_PERTABLE_STATS:
                 mergedStats = new (getHeap())
                    ExStatisticsArea(getHeap(), 0, ComTdb::PERTABLE_STATS,
                        stats->getOrigCollectStatsType());
                break;
              case SQLCLI_PROGRESS_STATS:
                 mergedStats = new (getHeap())
                    ExStatisticsArea(getHeap(), 0, ComTdb::PROGRESS_STATS,
                        stats->getOrigCollectStatsType());
                break;
              default:
                 if (reqType == SQLCLI_STATS_REQ_QID_DETAIL)
                    mergedStats = new (getHeap())
                       ExStatisticsArea(getHeap(), 0, ComTdb::QID_DETAIL_STATS,
                        ComTdb::QID_DETAIL_STATS);
                 else
                    mergedStats = new (getHeap())
                       ExStatisticsArea(getHeap(), 0, stats->getCollectStatsType(),
                        stats->getOrigCollectStatsType());
              }
              mergedStats->setDetailLevel(queryId->getDetailLevel());
            }
            if (reqType == SQLCLI_STATS_REQ_QID_DETAIL)
            {
              mergedStats->appendCpuStats(stats, TRUE);
              if (stats->getMasterStats() != NULL)
              {
                ExMasterStats *masterStats = new (getHeap()) ExMasterStats((NAHeap *)getHeap());
                masterStats->copyContents(stats->getMasterStats());
                mergedStats->setMasterStats(masterStats);
              }
            }
            else
            {
              mergedStats->merge(stats, queryId->getStatsMergeType());
              reply->incNumSqlProcs();
            }
          }
          do
          {
            stmtStats = (StmtStats *)stmtStatsList->getNext();
          } while (stmtStats != NULL && str_cmp(qid, stmtStats->getQueryId(), stmtStats->getQueryIdLen()) != 0);
        }

        statsGlobals->releaseStatsSemaphore(sscpGlobals->getSemId(), sscpGlobals->myPin());
#ifdef _DEBUG_RTS
        cerr << "Merged Stats " << mergedStats << " \n";
#endif
        if (mergedStats != NULL)
          reply->incNumCpus();
        *this << *reply;
        if (mergedStats != NULL)
          *this << *mergedStats;
        send(FALSE, -1, &bytesReplied_);
#ifdef _DEBUG_RTS
        cerr << "After send \n";
#endif
	NADELETE(mergedStats, ExStatisticsArea, getHeap());
	reply->decrRefCount();
        queryId->decrRefCount();
        request->decrRefCount();
      }
      break;
    default:
      break;
    }
  }
}

void SscpNewIncomingConnectionStream::processCpuStatsReq(IpcConnection *connection)
{
  Int64 currTimestamp;
  struct timespec currTimespec;
  size_t memThreshold;

  IpcMessageObjVersion msgVer;
  msgVer = getNextObjVersion();

  if (msgVer > currRtsStatsReqVersionNumber)
    // Send Error
    ;
  RtsCpuStatsReq *request = new (getHeap())
      RtsCpuStatsReq(INVALID_RTS_HANDLE, getHeap());

  *this >> *request;
  RtsStatsReply *reply = new (getHeap())
    RtsStatsReply(request->getHandle(), getHeap());

  SscpGlobals *sscpGlobals = getSscpGlobals();
  StatsGlobals *statsGlobals = sscpGlobals->getStatsGlobals();
  clearAllObjects();
  setType(IPC_MSG_SSCP_REPLY);
  setVersion(CurrSscpReplyMessageVersion);
  *this << *reply;
  ExStatisticsArea *stats;
  ExStatisticsArea *mergedStats = NULL;
  StmtStats *stmtStats;
  ExMasterStats *masterStats;
  short reqType = request->getReqType();
  short noOfQueries = request->getNoOfQueries();
  short subReqType = request->getSubReqType();
  Lng32 filter = request->getFilter();
  switch (reqType)
  {
  case SQLCLI_STATS_REQ_CPU_OFFENDER:
    mergedStats = new (getHeap())
      ExStatisticsArea(getHeap(), 0, ComTdb::CPU_OFFENDER_STATS, ComTdb::CPU_OFFENDER_STATS);
    break;
  case SQLCLI_STATS_REQ_SE_OFFENDER:
    mergedStats = new (getHeap())
      ExStatisticsArea(getHeap(), 0, ComTdb::SE_OFFENDER_STATS, ComTdb::SE_OFFENDER_STATS);
    mergedStats->setSubReqType(subReqType);
    break;
  case SQLCLI_STATS_REQ_ET_OFFENDER:
    mergedStats = new (getHeap())
      ExStatisticsArea(getHeap(), 0, ComTdb::ET_OFFENDER_STATS, ComTdb::ET_OFFENDER_STATS);
    break;
  case SQLCLI_STATS_REQ_RMS_INFO:
    mergedStats = new (getHeap())
      ExStatisticsArea(getHeap(), 0, ComTdb::RMS_INFO_STATS, ComTdb::RMS_INFO_STATS);
    mergedStats->setSubReqType(subReqType);
    break;
  case SQLCLI_STATS_REQ_MEM_OFFENDER:
    mergedStats = new (getHeap())
           ExStatisticsArea(getHeap(), 0, ComTdb::MEM_OFFENDER_STATS,
                     ComTdb::MEM_OFFENDER_STATS);
    mergedStats->setSubReqType(subReqType);
    break;
  default:
    ex_assert(0, "Unsupported Request Type");
  }
  mergedStats->setDetailLevel(request->getNoOfQueries());
  if (reqType != SQLCLI_STATS_REQ_RMS_INFO &&
          reqType != SQLCLI_STATS_REQ_MEM_OFFENDER)
  {
    int error = statsGlobals->getStatsSemaphore(sscpGlobals->getSemId(),
            sscpGlobals->myPin());
    SyncHashQueue *stmtStatsList = statsGlobals->getStmtStatsList();
    stmtStatsList->position();
    if (reqType == SQLCLI_STATS_REQ_ET_OFFENDER)
    {
       currTimestamp = NA_JulianTimestamp();
       while ((stmtStats = (StmtStats *)stmtStatsList->getNext()) != NULL)
       {
          stats = stmtStats->getStatsArea();
          masterStats = stmtStats->getMasterStats();
          if (masterStats != NULL)
             mergedStats->appendCpuStats(masterStats, FALSE,
                subReqType, filter, currTimestamp);
       }
    }
    else if (reqType == SQLCLI_STATS_REQ_SE_OFFENDER)
    {
       clock_gettime(CLOCK_MONOTONIC, &currTimespec);
       while ((stmtStats = (StmtStats *)stmtStatsList->getNext()) != NULL)
       {
          stats = stmtStats->getStatsArea();
          if (stats != NULL)
             mergedStats->appendCpuStats(stats, FALSE, filter, currTimespec); 
       }
    }
    else
    {
       currTimestamp = -1;
       while ((stmtStats = (StmtStats *)stmtStatsList->getNext()) != NULL)
       {
          stats = stmtStats->getStatsArea();
          if (stats != NULL)
             mergedStats->appendCpuStats(stats, FALSE);
       }
    }
    statsGlobals->releaseStatsSemaphore(sscpGlobals->getSemId(), sscpGlobals->myPin());
  }
  if (reqType == SQLCLI_STATS_REQ_RMS_INFO)
  {
    ExRMSStats *rmsStats = statsGlobals->getRMSStats(getHeap());
    mergedStats->insert(rmsStats);
    if (request->getNoOfQueries() == RtsCpuStatsReq::INIT_RMS_STATS_)
        statsGlobals->getRMSStats()->reset();
  }
  if (reqType == SQLCLI_STATS_REQ_MEM_OFFENDER)
  {
    statsGlobals->getMemOffender(mergedStats, filter);
  }
  if (mergedStats->numEntries() > 0)
    *this << *mergedStats;
  send(FALSE, -1, &bytesReplied_);
  NADELETE(mergedStats, ExStatisticsArea, getHeap());
  reply->decrRefCount();
  request->decrRefCount();
}

int reportStops(int alreadyStoppedCnt, int stoppedCnt)
{
  return alreadyStoppedCnt + stoppedCnt;
}

void SscpNewIncomingConnectionStream::processKillServersReq()
{
  int alreadyStoppedCnt = 0;
  int stoppedCnt = 0;

  // On SQ, stop catcher does not run for processes stopped by another
  // process, so the original loop below will not advance beyond the first
  // ESP.  So we will keep a list of already stopped ESPs and skip these.
  // This is an N-squared algorithm, but it is mitigated in two ways:
  // 1.) N is number of ESPs local to this MXSSCP, problably never more than
  // ten or so;
  // 2.) After first process is stopped, the others will most likely not
  // need to be, so  the list of already stopped ESPs will be only one or two.

  HashQueue alreadyStopped( getHeap() );

  IpcMessageObjVersion msgVer = getNextObjVersion();

  ex_assert(msgVer <= currRtsStatsReqVersionNumber, "Up-rev message received.");

  CancelQueryKillServersRequest *request = new (getHeap())
    CancelQueryKillServersRequest(INVALID_RTS_HANDLE, getHeap());

  *this >> *request;

  ex_assert(moreObjects(), "CancelQueryKillServersRequest all by itself.");

  RtsMessageObjType objType = (RtsMessageObjType) getNextObjType();

  ex_assert(objType == RTS_QUERY_ID,
            "CancelQueryKillServersRequest came with unknown msg obj.");

  RtsQueryId *queryId = new (getHeap()) RtsQueryId(getHeap());

  *this >> *queryId;
  char *qid = queryId->getQueryId();
  Lng32 qidLen = queryId->getQueryIdLen();

  SscpGlobals *sscpGlobals = getSscpGlobals();
  StatsGlobals *statsGlobals = sscpGlobals->getStatsGlobals();
  clearAllObjects();
  setType(IPC_MSG_SSCP_REPLY);
  setVersion(CurrSscpReplyMessageVersion);

  int error = statsGlobals->getStatsSemaphore(sscpGlobals->getSemId(),
                  sscpGlobals->myPin());

  SyncHashQueue *stmtStatsList = statsGlobals->getStmtStatsList();
  stmtStatsList->position(qid, qidLen);

  StmtStats *kqStmtStats = NULL;

  while (NULL != (kqStmtStats = (StmtStats *)stmtStatsList->getNext()))
  {
    if (str_cmp(kqStmtStats->getQueryId(), qid, qidLen) != 0)
    {
      // This stmtStats is on the HashQueue collision chain, but
      // it is for a different query id.  Keep looking.
      continue;
    }

    ExOperStats *rootStats = NULL;
    if (kqStmtStats->getStatsArea())
      rootStats = kqStmtStats->getStatsArea()->getRootStats();

    ExFragRootOperStats *rootOperStats = NULL;
    if ( rootStats &&
        (rootStats->statType() == ExOperStats::ROOT_OPER_STATS))
      rootOperStats = (ExFragRootOperStats *) rootStats;

    ExMeasStats *measStats = NULL;
    if ( rootStats &&
        (rootStats->statType() == ExOperStats::MEAS_STATS))
      measStats = (ExMeasStats *) rootStats;

    if (!rootOperStats && !measStats)
    {
      // Could be operator stats or other?
      continue;
    }

    // Make sure the ESP is still working on the query.  ESPs update
    // their exectionCount_ when they *finish*, so this is a good
    // test.

    if ( rootOperStats  &&
        (request->getExecutionCount() != rootOperStats->getExecutionCount()))
      continue;

    if ( measStats  &&
        (request->getExecutionCount() != measStats->getExecutionCount()))
      continue;

      const SB_Phandle_Type *statsPhandle = rootOperStats ?
                                  rootOperStats->getPhandle() :
                                  measStats->getPhandle();

    GuaProcessHandle gph;
    mem_cpy_all(&gph.phandle_, statsPhandle, sizeof(gph.phandle_));

    // Don't stop the query's master executor here.
    if (request->getMasterPhandle() == gph)
      continue;

    //Phandle wrapper in porting layer
    NAProcessHandle phandle((SB_Phandle_Type *)&gph.phandle_);

    int guaRetcode = phandle.decompose();

    if (!guaRetcode)
    {
      char *phandleString = phandle.getPhandleString();
      short phandleStrLen =  phandle.getPhandleStringLen();

      alreadyStopped.position(phandleString, phandleStrLen);
      char *alreadyStoppedEsp = NULL;
      bool wasAlreadyStopped = false;
      while ( NULL != (alreadyStoppedEsp = (char *) alreadyStopped.getNext()))
      {
        if (0 == memcmp(alreadyStoppedEsp, phandleString, phandleStrLen))
        {
          wasAlreadyStopped = true;
          alreadyStoppedCnt++;
          break;
        }
      }

      if (wasAlreadyStopped)
        continue;
      else
        alreadyStopped.insert(phandleString, phandleStrLen, phandleString);
    }

    // Okay, here goes...
    stoppedCnt++;
    statsGlobals->releaseStatsSemaphore(sscpGlobals->getSemId(),
                                      sscpGlobals->myPin());
    gph.dumpAndStop(request->getMakeSaveabend(),
                    true);                    // doStop

    // wait 100 milliseconds.  This will increase the chances that the
    // internal cancel (and error handling for SQLCODE 2034) will clean up
    // the query and thereby minimize the # of ESPs that must be killed.
    DELAY(10);

    // Reacquire the sema4.  And reposition into the HashQueue.
    error = statsGlobals->getStatsSemaphore(sscpGlobals->getSemId(),
                    sscpGlobals->myPin());

    stmtStatsList->position(qid, qidLen);
  }

  statsGlobals->releaseStatsSemaphore(sscpGlobals->getSemId(),
                                    sscpGlobals->myPin());

  if (sscpGlobals->shouldLogCancelKillServers() ||
      request->getCancelLogging())
  {
    char msg[120 + // the constant text
             ComSqlId::MAX_QUERY_ID_LEN
            ];

    str_sprintf(msg,
      "Escalation of cancel of query %s caused %d ESP server "
      "process(es) to be stopped and %d to be dumped.",
       queryId->getQueryId(), stoppedCnt,
       (request->getMakeSaveabend() ? stoppedCnt : 0) );

    SQLMXLoggingArea::logExecRtInfo(__FILE__, __LINE__, msg, 0);
  }

  RtsHandle rtsHandle = (RtsHandle) this;
  CancelQueryKillServersReply *reply = new(getHeap())
        CancelQueryKillServersReply( rtsHandle , getHeap());
  *this << *reply;

  send(FALSE, -1, &bytesReplied_);

  queryId->decrRefCount();
  reply->decrRefCount();
  request->decrRefCount();
  reportStops(alreadyStoppedCnt, stoppedCnt);
}

void SscpNewIncomingConnectionStream::suspendActivateSchedulers()
{
  int espFragCnt = 0;
  IpcMessageObjVersion msgVer = getNextObjVersion();

  ex_assert(msgVer <= currRtsStatsReqVersionNumber, "Up-rev message received.");

  SuspendActivateServersRequest *request = new (getHeap())
    SuspendActivateServersRequest(INVALID_RTS_HANDLE, getHeap());

  *this >> *request;

  ex_assert(moreObjects(), "SuspendActivateServersRequest all by itself.");

  RtsMessageObjType objType = (RtsMessageObjType) getNextObjType();

  ex_assert(objType == RTS_QUERY_ID,
            "SuspendActivateServersRequest came with unknown msg obj.");

  RtsQueryId *queryId = new (getHeap()) RtsQueryId(getHeap());

  *this >> *queryId;
  char *qid = queryId->getQueryId();
  Lng32 qidLen = queryId->getQueryIdLen();

  SscpGlobals *sscpGlobals = getSscpGlobals();
  StatsGlobals *statsGlobals = sscpGlobals->getStatsGlobals();
  clearAllObjects();
  setType(IPC_MSG_SSCP_REPLY);
  setVersion(CurrSscpReplyMessageVersion);

  int error = statsGlobals->getStatsSemaphore(sscpGlobals->getSemId(),
                  sscpGlobals->myPin());

  SyncHashQueue *stmtStatsList = statsGlobals->getStmtStatsList();
  stmtStatsList->position(qid, qidLen);

  StmtStats *kqStmtStats = NULL;

  while (NULL != (kqStmtStats = (StmtStats *)stmtStatsList->getNext()))
  {
    if (str_cmp(kqStmtStats->getQueryId(), qid, qidLen) != 0)
    {
      // This stmtStats is on the HashQueue collision chain, but
      // it is for a different query id.  Keep looking.
      continue;
    }

    ExOperStats *rootStats = NULL;
    if (kqStmtStats->getStatsArea())
      rootStats = kqStmtStats->getStatsArea()->getRootStats();

    ExFragRootOperStats *rootOperStats = NULL;
    if ( rootStats &&
        (rootStats->statType() == ExOperStats::ROOT_OPER_STATS))
      rootOperStats = (ExFragRootOperStats *) rootStats;

    ExMeasStats *measStats = NULL;
    if ( rootStats &&
        (rootStats->statType() == ExOperStats::MEAS_STATS))
      measStats = (ExMeasStats *) rootStats;

    // Logic in ex_root_tcb::register query ensures that the
    // registered query has root_oper or meas stats.
    ex_assert(rootOperStats || measStats,
              "suspending/activating unregistered query.")

    if (rootOperStats)
      rootOperStats->setFragSuspended(request->isRequestToSuspend());
    else if (measStats)
      measStats->setFragSuspended(request->isRequestToSuspend());
   espFragCnt++;
  }

  statsGlobals->releaseStatsSemaphore(sscpGlobals->getSemId(),
                                    sscpGlobals->myPin());

  if (request->getSuspendLogging())
  {
    char msg[80 +
           ComSqlId::MAX_QUERY_ID_LEN];

    str_sprintf(msg,
       "MXSSCP has %s %d fragment(s) for query %s.",
       ( request->isRequestToSuspend() ? "suspended" : "reactivated" ),
        espFragCnt, qid);
    SQLMXLoggingArea::logExecRtInfo(__FILE__, __LINE__, msg, 0);
  }

  RtsHandle rtsHandle = (RtsHandle) this;
  CancelQueryKillServersReply *reply = new(getHeap())
        CancelQueryKillServersReply( rtsHandle , getHeap());
  *this << *reply;

  send(FALSE, -1, &bytesReplied_);

  queryId->decrRefCount();
  reply->decrRefCount();
  request->decrRefCount();
}

static bool revokeTimerInitialized = false;
static bool revokeTimer = false;

void SscpNewIncomingConnectionStream::processSecInvReq()
{
  IpcMessageObjVersion msgVer = getNextObjVersion();

  ex_assert(msgVer <= currRtsStatsReqVersionNumber, "Up-rev message received.");

  SecInvalidKeyRequest *request = new(getHeap())
    SecInvalidKeyRequest(getHeap());

  *this >> *request;

  ex_assert( !moreObjects(), "unknown object follows SecInvalidKeyRequest.");

  Int32 numSiks = request->getNumSiks();
  if (numSiks)
  {
    if (!revokeTimerInitialized)
    {
      revokeTimerInitialized = true;
      char *r = getenv("RMS_REVOKE_TIMER");
      if (r && *r != '0')
        revokeTimer = true;
    }
    SscpGlobals *sscpGlobals = getSscpGlobals();
    StatsGlobals *statsGlobals = sscpGlobals->getStatsGlobals();
    int error = statsGlobals->getStatsSemaphore(sscpGlobals->getSemId(),
                  sscpGlobals->myPin());
    ExTimeStats timer;
    if (revokeTimer)
      timer.start();

    SyncHashQueue *stmtStatsList = statsGlobals->getStmtStatsList();
    StmtStats *kqStmtStats = NULL;
    stmtStatsList->position();
    // Look at each StmtStats
    while (NULL != (kqStmtStats = (StmtStats *)stmtStatsList->getNext()))
    {
      ExMasterStats *masterStats = kqStmtStats->getMasterStats();
      if (masterStats)
      {
        bool keysAreInvalid = false;
        bool histAreInvalid = false;
        // for each new invalidation key
        for (Int32 i = 0; i < numSiks && !keysAreInvalid; i++)
        {
          ComQIActionType siKeyType =
            ComQIActionTypeLiteralToEnum(request->getSik()[i].operation);
          if (siKeyType == COM_QI_OBJECT_REDEF)
          {
            // The statement may be in the progress of compiling,
            // set validDDL false.
            if (masterStats->getNumObjUIDs() == 0) {
              keysAreInvalid = true;
              masterStats->setValidDDL(false);
            }

            // compare the new DDL invalidation key to each key in the
            // master stats.
            for (Int32 m = 0; m < masterStats->getNumObjUIDs()
                              && !keysAreInvalid; m++)            
            {
              if (masterStats->getObjUIDs()[m] ==
                  request->getSik()[i].ddlObjectUID)
              {
                keysAreInvalid = true;
                masterStats->setValidDDL(false);
              }
            }
          }

          // If a role is revoked from a user or a role is
          // revoked from a group, do checks next time query is
          // executed
          else if (siKeyType == COM_QI_USER_GRANT_ROLE ||
                   siKeyType == COM_QI_GROUP_GRANT_ROLE)
          {
             keysAreInvalid = true;
             masterStats->setValidPrivs(false);
          }

          else if (siKeyType != COM_QI_STATS_UPDATED)
          {
            // compare the new REVOKE invalidation key to each key in the 
            // master stats.
            for (Int32 m = 0; m < masterStats->getNumSIKeys() 
                              && !keysAreInvalid; m++)
            {
              if (!memcmp(&masterStats->getSIKeys()[m], 
                          &request->getSik()[i], sizeof(SQL_QIKEY)))
              {
                keysAreInvalid = true;
                masterStats->setValidPrivs(false);
              } 
            }  // for each key in master stats.
          }  // revoke
          else//siKeyType == COM_QI_STATS_UPDATED
          {
            for (Int32 m = 0; m < masterStats->getNumObjUIDs()
                              && !histAreInvalid; m++)
            {
              if (masterStats->getObjUIDs()[m] ==
                  request->getSik()[i].ddlObjectUID)
              {
                histAreInvalid = true;
                masterStats->setValidHistogram(false);
              }
            }
          }
        }  // for each new invalidation key
      }  // if masterstats
    }
    statsGlobals->mergeNewSikeys(numSiks, request->getSik());

    statsGlobals->releaseStatsSemaphore(sscpGlobals->getSemId(),
                                    sscpGlobals->myPin());
    if (revokeTimer)
    {
      timer.stop();
      Int64 microSeconds = timer.getTime();
      char msg[256];
      str_sprintf(msg,
          "MXSSCP has processed %d security invalidation "
          "keys in %d milliseconds.",
          numSiks, (Int32)microSeconds / 1000);

      SQLMXLoggingArea::logExecRtInfo(__FILE__, __LINE__, msg, 0);
    }

  }

  clearAllObjects();
  setType(IPC_MSG_SSCP_REPLY);
  setVersion(CurrSscpReplyMessageVersion);

  RmsGenericReply *reply = new(getHeap())
    RmsGenericReply(getHeap());

  *this << *reply;

  send(FALSE, -1, &bytesReplied_);
  reply->decrRefCount();
  request->decrRefCount();
}

void SscpNewIncomingConnectionStream::processSnapshotLockReq()
{
  IpcMessageObjVersion msgVer = getNextObjVersion();

  ex_assert(msgVer <= CurrSnapshotLockVersionNumber, "Up-rev message received.");

  SnapshotLockRequest *request = new(getHeap())
    SnapshotLockRequest(getHeap());

  *this >> *request;

  ex_assert( !moreObjects(), "unknown object follows SnapshotLockRequest.");

  SscpGlobals *sscpGlobals = getSscpGlobals();
  StatsGlobals *statsGlobals = sscpGlobals->getStatsGlobals();
  short error = statsGlobals->getStatsSemaphore(sscpGlobals->getSemId(),
                  sscpGlobals->myPin());
  
  statsGlobals->setSnapshotInProgress();
    
  statsGlobals->releaseStatsSemaphore(sscpGlobals->getSemId(),
                                    sscpGlobals->myPin());
  clearAllObjects();
  setType(IPC_MSG_SSCP_REPLY);
  setVersion(CurrSscpReplyMessageVersion);

  RmsGenericReply *reply = new(getHeap())
    RmsGenericReply(getHeap());

  *this << *reply;

  send(FALSE, -1, &bytesReplied_);
  reply->decrRefCount();
  request->decrRefCount();
}

void SscpNewIncomingConnectionStream::processSnapshotUnLockReq()
{
  IpcMessageObjVersion msgVer = getNextObjVersion();

  ex_assert(msgVer <= CurrSnapshotUnLockVersionNumber, "Up-rev message received.");

  SnapshotUnLockRequest *request = new(getHeap())
    SnapshotUnLockRequest(getHeap());

  *this >> *request;

  ex_assert( !moreObjects(), "unknown object follows SnapshotUnLockRequest.");
  SscpGlobals *sscpGlobals = getSscpGlobals();
  StatsGlobals *statsGlobals = sscpGlobals->getStatsGlobals();
  int error = statsGlobals->getStatsSemaphore(sscpGlobals->getSemId(),
                  sscpGlobals->myPin());

  
  statsGlobals->resetSnapshotInProgress();
    
  statsGlobals->releaseStatsSemaphore(sscpGlobals->getSemId(),
                                    sscpGlobals->myPin());
  clearAllObjects();
  setType(IPC_MSG_SSCP_REPLY);
  setVersion(CurrSscpReplyMessageVersion);

  RmsGenericReply *reply = new(getHeap())
    RmsGenericReply(getHeap());

  *this << *reply;

  send(FALSE, -1, &bytesReplied_);
  reply->decrRefCount();
  request->decrRefCount();
}



void SscpNewIncomingConnectionStream::processObjectEpochChangeReq()
{
  IpcMessageObjVersion msgVer = getNextObjVersion();
  ex_assert(msgVer <= currRtsStatsReqVersionNumber, "Up-rev message received.");

  ObjectEpochChangeRequest *request = new(getHeap())
    ObjectEpochChangeRequest(getHeap());
  *this >> *request;

  ex_assert( !moreObjects(), "unknown object follows ObjectEpochChangeRequest.");

  SscpGlobals *sscpGlobals = getSscpGlobals();
  StatsGlobals *statsGlobals = sscpGlobals->getStatsGlobals();
  ObjectEpochCache *objectEpochCache = statsGlobals->getObjectEpochCache();
  ObjectEpochChangeReply::Result result = ObjectEpochChangeReply::SUCCESS;  // assume success
  UInt32 replyEpoch = request->newEpoch();
  UInt32 replyFlags = request->newFlags();
  ObjectEpochCacheKey key(request->objectName(), request->key(), COM_BASE_TABLE_OBJECT); // TODO: get object type from request
  ObjectEpochCacheEntry *oecEntry = NULL;
  short retcode = objectEpochCache->getEpochEntry(oecEntry /* out */,key);
  if ((retcode == ObjectEpochCache::OEC_ERROR_OK) || 
      (retcode == ObjectEpochCache::OEC_ERROR_ENTRY_NOT_EXISTS))
    {
      switch (request->operation())
        {
          case ObjectEpochChangeRequest::CREATE_OBJECT_EPOCH_ENTRY:
            {
              if (oecEntry) // if entry already exists
                {
                  // if the expected epoch and flags match the entry, we'll
                  // count that as success (this might happen in a node-up
                  // situation where we get a request from a node that has
                  // just come up)
                  replyEpoch = oecEntry->epoch();
                  replyFlags = oecEntry->flags();
                  if ((request->expectedEpoch() != replyEpoch) ||
                      (request->expectedFlags() != replyFlags))
                    result = ObjectEpochChangeReply::UNEXPECTED_VALUES_FOUND;
                }
              else
                {
                  // TODO: add redef timestamp and object name when method supports it
                  retcode = objectEpochCache->createEpochEntry(oecEntry /* out */,
                         key,request->newEpoch(),request->newFlags());
                }
              break;
            }  
          case ObjectEpochChangeRequest::START_OR_CONTINUE_DDL_OPERATION:
            {
              if (oecEntry)
                {
                  replyEpoch = oecEntry->epoch();
                  replyFlags = oecEntry->flags();
                  if ((request->expectedEpoch() == replyEpoch) &&
                      (request->expectedFlags() == replyFlags))
                    oecEntry->update(request->newEpoch(),request->newFlags());
                  else
                    result = ObjectEpochChangeReply::UNEXPECTED_VALUES_FOUND;              
                }
              else
                {
                  // this might happen in a node-up situation where this is
                  // the new node and we got a request from an existing node;
                  // just create the entry in this case
                  // TODO: add redef timestamp and object name when method supports it
                  retcode = objectEpochCache->createEpochEntry(oecEntry /* out */,
                         key,request->newEpoch(),request->newFlags());
                }
              break;
            }
          case ObjectEpochChangeRequest::COMPLETE_DDL_OPERATION:
            {
              if (oecEntry)
                {
                  replyEpoch = oecEntry->epoch();
                  replyFlags = oecEntry->flags();
                  // For DDL END operation, only need to make sure we
                  // are in DDL operation.
                  if ((request->expectedEpoch() == replyEpoch) &&
                      (replyFlags | ObjectEpochChangeRequest::DDL_IN_PROGRESS))
                    oecEntry->update(request->newEpoch(),request->newFlags());
                  else
                    result = ObjectEpochChangeReply::UNEXPECTED_VALUES_FOUND;
                }
              else
                {
                  // this might happen in a node-up situation where this is
                  // the new node and we got a request from an existing node;
                  // just create the entry in this case
                  // TODO: add redef timestamp and object name when method supports it
                  retcode = objectEpochCache->createEpochEntry(oecEntry /* out */,
                         key,request->newEpoch(),request->newFlags());
                }
              break;
            }
          case ObjectEpochChangeRequest::ABORT_DDL_OPERATION:
            {
              if (oecEntry)
                {
                  replyEpoch = oecEntry->epoch();
                  replyFlags = oecEntry->flags();
                  if ((request->expectedEpoch() == replyEpoch)
                      //&& (request->expectedEpoch() & ObjectEpochChangeRequest::DDL_IN_PROGRESS)
                     )
                    oecEntry->update(request->newEpoch(),request->newFlags());
                  else
                    result = ObjectEpochChangeReply::UNEXPECTED_VALUES_FOUND;              
                }
              else
                {
                  // TODO: add redef timestamp and object name when method supports it
                  retcode = objectEpochCache->createEpochEntry(oecEntry /* out */,
                         key,request->newEpoch(),request->newFlags());
                }
              break;
            }
          case ObjectEpochChangeRequest::FORCE:
            {
              if (oecEntry)
                oecEntry->update(request->newEpoch(),request->newFlags());
              else
                {
                  // this might happen in a node-up situation where this is
                  // the new node and we got a request from an existing node;
                  // just create the entry in this case
                  // TODO: add redef timestamp and object name when method supports it
                  retcode = objectEpochCache->createEpochEntry(oecEntry /* out */,
                         key,request->newEpoch(),request->newFlags());
                }
              break;
            }
          default:
            {
              result = ObjectEpochChangeReply::INTERNAL_ERROR;
              break;
            }
        }  // end switch
    }
 
  if (retcode == ObjectEpochCache::OEC_ERROR_NO_MEMORY)
    {
      result = ObjectEpochChangeReply::CACHE_FULL;
    }
  else if (retcode)  // if there was some other error accessing the ObjectEpochCache
    {
      result = ObjectEpochChangeReply::INTERNAL_ERROR;
      replyEpoch = retcode;  // stash the retcode for debugging purposes
    }
  
  clearAllObjects();
  setType(IPC_MSG_SSCP_REPLY);
  setVersion(CurrSscpReplyMessageVersion);

  ObjectEpochChangeReply *reply = new(getHeap())
    ObjectEpochChangeReply(result,
                           replyEpoch,
                           replyFlags,
                           getHeap());

  *this << *reply;

  send(FALSE);
  reply->decrRefCount();
  request->decrRefCount();
}

void SscpNewIncomingConnectionStream::processObjectEpochStatsReq()
{
  IpcMessageObjVersion msgVer = getNextObjVersion();
  ex_assert(msgVer <= CurrObjectEpochStatsRequestVersionNumber,
            "Up-rev message received.");

  ObjectEpochStatsRequest *request = new(getHeap())
    ObjectEpochStatsRequest(getHeap());
  *this >> *request;
  ex_assert(!moreObjects(),
            "unknown object follows ObjectEpochStatsRequest.");

  QRDEBUG("Processing object epoch stats request");

  ObjectEpochStatsReply *reply = new(getHeap())
    ObjectEpochStatsReply(getHeap());

  clearAllObjects();
  setType(IPC_MSG_SSCP_REPLY);
  setVersion(CurrSscpReplyMessageVersion);
  *this << *reply;

  SscpGlobals *sscpGlobals = getSscpGlobals();
  StatsGlobals *statsGlobals = sscpGlobals->getStatsGlobals();
  ex_assert(statsGlobals,
            "StatsGlobals not properly initialized");
  ObjectEpochCache *epochCache = statsGlobals->getObjectEpochCache();
  ex_assert(epochCache,
            "ObjectEpochCache not properly initialized");
  ExStatisticsArea *statsArea = new (getHeap())
    ExStatisticsArea(getHeap(), 0,
                     ComTdb::OBJECT_EPOCH_STATS,
                     ComTdb::OBJECT_EPOCH_STATS);
  epochCache->appendToStats(statsArea,
                            request->getObjectName(),
                            request->getLocked());
  *this << *statsArea;
  QRDEBUG("Sending object epoch stats reply");
  send(FALSE);
  NADELETE(statsArea, ExStatisticsArea, getHeap());
  reply->decrRefCount();
  request->decrRefCount();
}

void SscpNewIncomingConnectionStream::processObjectLockReq()
{
  IpcMessageObjVersion msgVer = getNextObjVersion();
  ex_assert(msgVer <= CurrObjectLockRequestVersionNumber,
            "Up-rev message received.");

  ObjectLockRequest *request = new(getHeap())
    ObjectLockRequest(getHeap());
  *this >> *request;
  ex_assert(!moreObjects(),
            "unknown object follows ObjectLockRequest.");

  const char *objectName = request->getObjectName();
  ComObjectType objectType = request->getObjectType();
  ObjectLockRequest::OpType opType = request->getOpType();
  Int32 lockNid = request->lockNid();
  Int32 lockPid = request->lockPid();

  ex_assert(objectName != NULL,
            "NULL objectName in ObjectLockRequest.");

  LOGDEBUG(CAT_SQL_LOCK, "SSCP Processing object %s request",
           ObjectLockRequest::opTypeLit(opType));
  LOGDEBUG(CAT_SQL_LOCK, "  object name: %s", objectName);
  LOGDEBUG(CAT_SQL_LOCK, "  object type: %s", comObjectTypeName(objectType));
  LOGDEBUG(CAT_SQL_LOCK, "  node id: %d", lockNid);
  LOGDEBUG(CAT_SQL_LOCK, "  process id: %d", lockPid);

  ObjectLockReply *reply = new(getHeap())
    ObjectLockReply(getHeap());

  LockHolder holder(lockNid, lockPid);
  if (opType == ObjectLockRequest::LOCK) {
    ObjectLockPointer lock(objectName, objectType);
    LockHolder conflictHolder;
    ObjectLockController::Result result = lock->lockDDL(holder, conflictHolder);
    if (result == ObjectLockController::Result::LOCK_OK) {
      LOGDEBUG(CAT_SQL_LOCK,
               "DDL lock for %s %s succeeded",
               comObjectTypeName(objectType), objectName);
    } else if (result == ObjectLockController::Result::LOCK_CONFLICT_DML) {
      LOGWARN(CAT_SQL_LOCK,
              "DDL lock for %s %s conflict with DML operations on NID %d PID %d",
              comObjectTypeName(objectType), objectName,
              conflictHolder.getNid(), conflictHolder.getPid());
    } else if (result == ObjectLockController::Result::LOCK_CONFLICT_DDL) {
      LOGWARN(CAT_SQL_LOCK,
              "DDL lock for %s %s conflict with DDL operations on NID %d PID %d",
              comObjectTypeName(objectType), objectName,
              conflictHolder.getNid(), conflictHolder.getPid());
    } else if (result == ObjectLockController::Result::LOCK_OUT_OF_ENTRY) {
      LOGWARN(CAT_SQL_LOCK,
              "DDL lock for %s %s failed due to out of lock entry",
              comObjectTypeName(objectType), objectName);
    } else {
      LOGERROR(CAT_SQL_LOCK,
               "DDL lock for %s %s failed with invalid lock result: %d",
               comObjectTypeName(objectType), objectName, result);
      ex_assert(0, "Invalid lock result");
    }
    reply->setConflictNid(conflictHolder.getNid());
    reply->setConflictPid(conflictHolder.getPid());
    reply->setLockState(result);
  } else if (opType == ObjectLockRequest::UNLOCK) {
    ObjectLockPointer lock(objectName, objectType);
    lock->unlockDDL(holder);
    reply->setLockState(ObjectLockReply::LOCK_OK);
  } else if (opType == ObjectLockRequest::UNLOCKALL) {
    ObjectLockController::releaseDDLLocksHeldBy(holder);
    reply->setLockState(ObjectLockReply::LOCK_OK);
  } else if (opType == ObjectLockRequest::CLEANUP) {
    ObjectLockController::releaseLocksHeldBy(holder);
    reply->setLockState(ObjectLockReply::LOCK_OK);
  } else {
    LOGERROR(CAT_SQL_LOCK,
             "Unsupported object lock operation: %d",
             opType);
    ex_assert(0, "Unsupported object lock operation");
  }

  clearAllObjects();
  setType(IPC_MSG_SSCP_REPLY);
  setVersion(CurrSscpReplyMessageVersion);
  *this << *reply;

  LOGDEBUG(CAT_SQL_LOCK,
           "Sending object %s reply state: %s",
           ObjectLockRequest::opTypeLit(opType),
           reply->getLockStateLit());
  send(FALSE);
  reply->decrRefCount();
  request->decrRefCount();
}

void SscpNewIncomingConnectionStream::processObjectLockStatsReq()
{
  IpcMessageObjVersion msgVer = getNextObjVersion();
  ex_assert(msgVer <= CurrObjectLockStatsRequestVersionNumber,
            "Up-rev message received.");

  ObjectLockStatsRequest *request = new(getHeap())
    ObjectLockStatsRequest(getHeap());
  *this >> *request;
  ex_assert(!moreObjects(),
            "unknown object follows ObjectLockStatsRequest.");

  LOGDEBUG(CAT_SQL_LOCK,
           "Processing object lock stats request of %s %s",
           comObjectTypeName(request->getObjectType()),
           request->getObjectName() ? request->getObjectName() : "ALL");

  ObjectLockStatsReply *reply = new(getHeap())
    ObjectLockStatsReply(getHeap());

  clearAllObjects();
  setType(IPC_MSG_SSCP_REPLY);
  setVersion(CurrSscpReplyMessageVersion);
  *this << *reply;

  ExStatisticsArea *statsArea = new (getHeap())
    ExStatisticsArea(getHeap(), 0,
                     ComTdb::OBJECT_LOCK_STATS,
                     ComTdb::OBJECT_LOCK_STATS);
  ObjectLockController::appendToStats(statsArea,
                                      request->getObjectName(),
                                      request->getObjectType());
  *this << *statsArea;
  LOGDEBUG(CAT_SQL_LOCK, "Sending object lock stats reply");
  send(FALSE);
  NADELETE(statsArea, ExStatisticsArea, getHeap());
  reply->decrRefCount();
  request->decrRefCount();
}
