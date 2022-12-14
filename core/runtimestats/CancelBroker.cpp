/**********************************************************************

// File:         CancelBroker.cpp
// Description:  Implementation of methods used in the Cancel Broker
//               role of SSMP.
//
// Created:      Oct 5, 2009
**********************************************************************/

#include "CancelBroker.h"

#include "cli/Statement.h"
#include "common/ComSqlId.h"
#include "executor/ExStats.h"
#include "executor/ex_stdh.h"

#include "porting/PortProcessCalls.h"
#include "runtimestats/SqlStats.h"
#include "seabed/fs.h"
#include "seabed/ms.h"
#include "sqlmxevents/logmxevent.h"

///////////////////////////////////////////////////////////////////////
// Methods for class ActiveQueryStream
///////////////////////////////////////////////////////////////////////

ActiveQueryStream::ActiveQueryStream(IpcEnvironment *env, SsmpGlobals *ssmpG, ActiveQueryEntry *myAq)
    : IpcMessageStream(env, CANCEL_QUERY_STARTED_REPLY, CurrSsmpReplyMessageVersion, 0, TRUE),
      ssmpGlobals_(ssmpG),
      myAq_(myAq) {}

void ActiveQueryStream::actOnSend(IpcConnection *connection) {
  // check for OS errors
  if (connection->getErrorInfo() == 0) ssmpGlobals_->incSsmpReplyMsg(connection->getLastSentMsg()->getMessageLength());
}

///////////////////////////////////////////////////////////////////////
// Methods for ActiveQueryEntry
///////////////////////////////////////////////////////////////////////

ActiveQueryEntry::ActiveQueryEntry(char *qid, int qidLen, long startTime, GuaProcessHandle master, short masterFileNum,
                                   int executionCount, IpcEnvironment *ipcEnv, SsmpGlobals *ssmpG)
    : queryStartTime_(startTime),
      master_(master),
      masterFileNum_(masterFileNum),
      executionCount_(executionCount),
      replyStartedStream_(NULL),
      qidLen_(qidLen) {
  qid_ = new (collHeap()) char[qidLen + 1];
  str_cpy_all(qid_, qid, qidLen);
  qid_[qidLen] = '\0';

  replyStartedStream_ = new ((NAHeap *)ipcEnv->getHeap()) ActiveQueryStream(ipcEnv, ssmpG, this);
}

ActiveQueryEntry::~ActiveQueryEntry() {
  if (qid_) {
    NADELETEBASIC(qid_, collHeap());
    qid_ = NULL;
  }
  releaseStream();
}

void ActiveQueryEntry::replyToQueryStarted(NAHeap *ipcHeap, NextActionForSubject nxtA, bool cancelLogging) {
  replyStartedStream_->clearAllObjects();

  RtsHandle rtsHandle = (RtsHandle)this;
  QueryStartedReply *qsReply = new (ipcHeap) QueryStartedReply(rtsHandle, ipcHeap, cancelLogging);

  switch (nxtA) {
    case CB_COMPLETE:
      qsReply->nextActionIsComplete();
      break;
    case CB_CANCEL:
      qsReply->nextActionIsCancel();
      break;
    case CB_DONT_CARE:
      // Client is gone, it will never get this reply.  Purpose of the reply
      // is to allow IPC cleanup.
      break;
    default:
      ex_assert(0, "NextActionForSubject has no case.");
      break;
  }

  *replyStartedStream_ << *qsReply;
  qsReply->decrRefCount();
  replyStartedStream_->send(FALSE);

  if (cancelLogging) {
    char pname[40];
    pname[0] = '\0';
    int pnameLen = getMasterPhandle().toAscii(pname, sizeof(pname));
    pname[pnameLen] = '\0';

    char msg[90 +  // the constant text
             ComSqlId::MAX_QUERY_ID_LEN];

    str_sprintf(msg,
                "Early reply to query started message from %s "
                "to attempt graceful cancel of query %s.",
                pname, getQid());

    SQLMXLoggingArea::logExecRtInfo(__FILE__, __LINE__, msg, 0);
  }
}

void ActiveQueryEntry::releaseStream() {
  if (replyStartedStream_) {
    replyStartedStream_->addToCompletedList();
    replyStartedStream_ = NULL;
  }
}

///////////////////////////////////////////////////////////////////////
// Methods for ActiveQueryMgr.
///////////////////////////////////////////////////////////////////////

ActiveQueryEntry *ActiveQueryMgr::getActiveQuery(char *qid, int qidLen) {
  ActiveQueryEntry *aq = NULL;

  activeQueries_.position(qid, (int)qidLen);
  aq = (ActiveQueryEntry *)activeQueries_.getNext();
  while (aq != NULL) {
    if (str_cmp(aq->getQid(), qid, (int)qidLen) == 0) return aq;
    aq = (ActiveQueryEntry *)activeQueries_.getNext();
  }
  return NULL;
}

void ActiveQueryMgr::addActiveQuery(char *qid, int qidLen, long startTime, GuaProcessHandle masterPhandle,
                                    int executionCount, SsmpNewIncomingConnectionStream *cStream, IpcConnection *conn) {
  GuaConnectionToClient *gctc = conn->castToGuaConnectionToClient();
  ex_assert(gctc,
            "Need a GuaConnectionToClient so that "
            "system messages can be properly handled.");
  short masterFileNum = gctc->getFileNumForLogging();

  ActiveQueryEntry *aq = new (heap_) ActiveQueryEntry(qid, qidLen, startTime, masterPhandle, masterFileNum,
                                                      executionCount, ipcEnv_, cStream->getSsmpGlobals());

  cStream->giveMessageTo(*aq->getReplyStartedStream(), conn);

  activeQueries_.insert(qid, (int)qidLen, aq);
}

void ActiveQueryMgr::rmActiveQuery(char *qid, int qidLen, NAHeap *ipcHeap, NextActionForSubject nxtA,
                                   bool cancelLogging) {
  ActiveQueryEntry *aq = getActiveQuery(qid, qidLen);
  char msg[300];
  if (aq) {
    aq->replyToQueryStarted(ipcHeap, nxtA, cancelLogging);
    activeQueries_.remove(qid, qidLen, aq);
    delete aq;
  } else {
    if (nxtA == CB_COMPLETE) {
      strcpy(msg, "Query Id ");
      strncat(msg, qid, qidLen);
      strcat(msg, " is not an active, response to query started message not sent");
      SQLMXLoggingArea::logExecRtInfo(__FILE__, __LINE__, msg, 0);
    }
  }
}

void ActiveQueryMgr::clientIsGone(const GuaProcessHandle &c, short fileNum) {
  ActiveQueryEntry *aq = NULL;

  activeQueries_.position();
  while (NULL != (aq = (ActiveQueryEntry *)activeQueries_.getNext())) {
    if ((aq->getMasterPhandle() == c) && (aq->getMasterFileNum() == fileNum)) {
      aq->replyToQueryStarted((NAHeap *)ipcEnv_->getHeap(), CB_DONT_CARE, false);
      activeQueries_.remove(aq->getQid(), aq->getQidLen(), aq);
      delete aq;
    }
  }
}

///////////////////////////////////////////////////////////////////////
// Methods for ActiveQueryEntry
///////////////////////////////////////////////////////////////////////

PendingQueryEntry::PendingQueryEntry(char *qid, int qidLen, int executionCount, GuaProcessHandle master,
                                     short masterFileNum, long escalateTime1, long escalateTime2,
                                     bool cancelEscalationSaveabend, bool cancelLogging)
    : qidLen_(qidLen),
      executionCount_(executionCount),
      master_(master),
      masterFileNum_(masterFileNum),
      escalateTime1_(escalateTime1),
      escalateTime2_(escalateTime2),
      cancelEscalationSaveabend_(cancelEscalationSaveabend),
      cancelLogging_(cancelLogging),
      haveEscalated1_(false) {
  qid_ = new (collHeap()) char[qidLen + 1];
  str_cpy_all(qid_, qid, qidLen);
  qid_[qidLen] = '\0';
}

PendingQueryEntry::~PendingQueryEntry() {
  if (qid_) {
    NADELETEBASIC(qid_, collHeap());
    qid_ = NULL;
  }
}

///////////////////////////////////////////////////////////////////////
// Methods for PendingQueryMgr.
///////////////////////////////////////////////////////////////////////
PendingQueryMgr::PendingQueryMgr(SsmpGlobals *ssmpGlobals, NAHeap *heap)
    : pendingQueries_(heap), ssmpGlobals_(ssmpGlobals) {}

void PendingQueryMgr::addPendingQuery(ActiveQueryEntry *aq, int ceFirstInterval, int ceSecondInterval,
                                      NABoolean ceSaveabend, NABoolean cancelLogging) {
  char *qid = aq->getQid();
  int qidLen = aq->getQidLen();
  long timeNow = JULIANTIMESTAMP();

  long ceTime1Tmp64 = ceFirstInterval;
  long ceTime1 = (ceTime1Tmp64 == 0) ? 0 : timeNow + (ceTime1Tmp64 * 1000 * 1000);

  long ceTime2Tmp64 = ceSecondInterval;
  long ceTime2 = (ceTime2Tmp64 == 0) ? 0 : timeNow + (ceTime2Tmp64 * 1000 * 1000);

  PendingQueryEntry *pq = new (ssmpGlobals_->getHeap())
      PendingQueryEntry(qid, qidLen, aq->getExecutionCount(), aq->getMasterPhandle(), aq->getMasterFileNum(), ceTime1,
                        ceTime2, (bool)ceSaveabend, (bool)cancelLogging);

  pendingQueries_.insert(qid, (int)qidLen, pq);
}

void PendingQueryMgr::removePendingQuery(PendingQueryEntry *pq) {
  pendingQueries_.remove();
  delete pq;
}

void PendingQueryMgr::clientIsGone(const GuaProcessHandle &c, short fileNum) {
  PendingQueryEntry *pq = NULL;

  pendingQueries_.position();
  while (NULL != (pq = (PendingQueryEntry *)pendingQueries_.getNext())) {
    if ((pq->getMasterPhandle() == c) && (pq->getMasterFileNum() == fileNum)) removePendingQuery(pq);
  }
}

void PendingQueryMgr::killPendingCanceled() {
  long timeNow = JULIANTIMESTAMP();
  short savedPriority, savedStopMode;

  StatsGlobals *statsGlobals = ssmpGlobals_->getStatsGlobals();
  PendingQueryEntry *pq = NULL;

  bool haveSema4 = false;
  pendingQueries_.position();
  while (NULL != (pq = (PendingQueryEntry *)pendingQueries_.getNext())) {
    if (!haveSema4) {
      int error = statsGlobals->getStatsSemaphore(ssmpGlobals_->getSemId(), ssmpGlobals_->myPin());
      haveSema4 = true;
    }

    StmtStats *pqStmtStats = statsGlobals->getMasterStmtStats(pq->getQid(), pq->getQidLen(), RtsQueryId::ANY_QUERY_);

    // See if query is removed from shared segment.  If so, no escalation.
    if (pqStmtStats == NULL) {
      removePendingQuery(pq);
      continue;
    }

    // Will need a ExOperStats and either ExRootOperStats or ExMeasStats
    // for the tests below.
    ExOperStats *rootStats = NULL;
    if (pqStmtStats->getStatsArea()) rootStats = pqStmtStats->getStatsArea()->getRootStats();

    ExFragRootOperStats *rootOperStats = NULL;
    if (rootStats && (rootStats->statType() == ExOperStats::ROOT_OPER_STATS))
      rootOperStats = (ExFragRootOperStats *)rootStats;

    ExMeasStats *measStats = NULL;
    if (rootStats && (rootStats->statType() == ExOperStats::MEAS_STATS)) measStats = (ExMeasStats *)rootStats;

    // Test ExRootOperStats and if execution count is different, disregard.
    if (rootOperStats && (pq->getExecutionCount() != rootOperStats->getExecutionCount())) {
      removePendingQuery(pq);
      continue;
    }

    // Test ExMeasStats and if execution count is different, disregard.
    if (measStats && (pq->getExecutionCount() != measStats->getExecutionCount())) {
      removePendingQuery(pq);
      continue;
    }

    // See if query is finished. If so, no escalation.
    ExMasterStats *masterStats = pqStmtStats->getMasterStats();
    if ((masterStats == NULL) || ((masterStats->getExeStartTime() == -1) ||  // not started
                                  (masterStats->getExeEndTime() != -1))      // is finished
    ) {
      removePendingQuery(pq);
      continue;
    }

    // If time to escalate #1, then do so.
    if ((pq->getEscalateTime1() != 0) && (timeNow > pq->getEscalateTime1()) && !pq->getHaveEscalated1()) {
      pq->setHaveEscalated1();

      ex_assert(haveSema4, "Internal error in PendingQueryMgr::killPendingCanceled()");

      // Release semaphore before messaging.
      statsGlobals->releaseStatsSemaphore(ssmpGlobals_->getSemId(), ssmpGlobals_->myPin());
      haveSema4 = false;

      askSscpsToStopServers(pq);

      // See if this query had only one escalation level.  If so,
      // we are finished.
      if (pq->getEscalateTime2() == 0) removePendingQuery(pq);

      // Evaluate escalation #2 for this on the next call to this method.
      // We want to give the escalation #1 a chance to work, even if the
      // two escalation intervals are the same.
      continue;
    }

    // If time to escalate #2, then do so.
    if (pq->getEscalateTime2() != 0 && timeNow > pq->getEscalateTime2() && (rootOperStats || measStats)) {
      // Stop master here.

      const SB_Phandle_Type *statsPhandle = rootOperStats ? rootOperStats->getPhandle() : measStats->getPhandle();
      if (statsPhandle) {
        GuaProcessHandle gph;
        mem_cpy_all(&gph.phandle_, statsPhandle, sizeof(gph.phandle_));
        short makeSavebend = (short)pq->getCancelEscalationSaveabend();

        ex_assert(haveSema4, "Internal error in PendingQueryMgr::killPendingCanceled()");

        // Release semaphore.
        statsGlobals->releaseStatsSemaphore(ssmpGlobals_->getSemId(), ssmpGlobals_->myPin());
        haveSema4 = false;

        gph.dumpAndStop(makeSavebend,  // do make a core-file.
                        true);         // doStop == true.

        if (pq->getCancelLogging()) {
          char pname[40];
          pname[0] = '\0';
          int pnameLen = gph.toAscii(pname, sizeof(pname));
          pname[pnameLen] = '\0';

          char msg[80 +                             // the constant text
                   ComSqlId::MAX_QUERY_ID_LEN + 40  // the process name (e.g., $Z000R0U:40143296)
          ];

          str_sprintf(msg, "Escalation of cancel of query %s caused process %s to be stopped.", pq->getQid(), pname);

          SQLMXLoggingArea::logExecRtInfo(__FILE__, __LINE__, msg, 0);
        }
      }

      removePendingQuery(pq);
    }  // end if escalation #2
  }    // end while (pq = getNext())

  if (haveSema4) {
    statsGlobals->releaseStatsSemaphore(ssmpGlobals_->getSemId(), ssmpGlobals_->myPin());
    haveSema4 = false;
  }
}

void PendingQueryMgr::askSscpsToStopServers(PendingQueryEntry *pq) {
  CollHeap *ipcHeap = ssmpGlobals_->getIpcEnv()->getHeap();

  ssmpGlobals_->allocateServers();

  SscpClientMsgStream *sscpMsgStream =
      new (ipcHeap) SscpClientMsgStream((NAHeap *)ipcHeap, ssmpGlobals_->getIpcEnv(), ssmpGlobals_, NULL);

  sscpMsgStream->setUsedToSendCbMsgs();

  ssmpGlobals_->addRecipients(sscpMsgStream);

  sscpMsgStream->clearAllObjects();

  GuaProcessHandle master(pq->getMasterPhandle());

  CancelQueryKillServersRequest *request =
      new (ipcHeap) CancelQueryKillServersRequest((RtsHandle)sscpMsgStream, ipcHeap, pq->getExecutionCount(), &master,
                                                  pq->getCancelEscalationSaveabend(), pq->getCancelLogging());

  *sscpMsgStream << *request;

  RtsQueryId *rtsQueryId = new (ipcHeap) RtsQueryId(ipcHeap, pq->getQid(), pq->getQidLen());

  *sscpMsgStream << *rtsQueryId;

  // Send the Message to all
  // Do not do this ? ssmpGlobals_->addPendingSscpMessage(sscpMsgStream);
  sscpMsgStream->send(FALSE);

  if (pq->getCancelLogging()) {
    char msg[140 +  // the constant text
             ComSqlId::MAX_QUERY_ID_LEN];

    str_sprintf(msg,
                "To attempt first level escalation of cancel for "
                "query %s, messages have been sent to mxsscp "
                "processes to request that ESPs be stopped.",
                pq->getQid());

    SQLMXLoggingArea::logExecRtInfo(__FILE__, __LINE__, msg, 0);
  }

  request->decrRefCount();
  rtsQueryId->decrRefCount();
}
