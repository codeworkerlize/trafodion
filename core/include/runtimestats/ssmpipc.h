/**********************************************************************

// File:         ssmpIpc.h
// Description:  Class declaration for SSCP IPC infrastructure
//
// Created:      5/08/2006
**********************************************************************/
#ifndef _SSMPIPC_H_
#define _SSMPIPC_H_

#include "CancelBroker.h"
#include "runtimestats/SqlStats.h"
#include "comexe/ComQueue.h"
#include "common/Collections.h"
#include "common/Ipc.h"
#include "common/Platform.h"
#include "runtimestats/rts_msg.h"

class StatsGlobals;
class StmtStats;
class HashQueue;
class SscpClientMsgStream;
class SsmpNewIncomingConnectionStream;
class ActiveQueryMgr;
class PendingQueryMgr;
class ExStatisticsArea;

typedef struct ServerId {
  char nodeName_[MAX_SEGMENT_NAME_LEN + 1];
  short cpuNum_;
} ServerId;

/////////////////////////////////////////////////////////////
// class ExSsmpManager
/////////////////////////////////////////////////////////////
class ExSsmpManager {
 public:
  ExSsmpManager(IpcEnvironment *env);
  ~ExSsmpManager();
  IpcServer *getSsmpServer(NAHeap *heap, char *nodeName, short cpuNum, ComDiagsArea *&diagsArea);
  IpcEnvironment *getIpcEnvironment() { return env_; }
  void removeSsmpServer(char *nodeName, short cpuNum);
  void cleanupDeletedSsmpServers();
  IpcServerClass *getServerClass() { return ssmpServerClass_; }

 private:
  IpcEnvironment *env_;
  IpcServerClass *ssmpServerClass_;
  HashQueue *ssmps_;
  NAList<IpcServer *> *deletedSsmps_;  // list of ssmp servers to be deleted
};                                     // ExSsmpManager

enum SuspendOrActivate { SUSPEND, ACTIVATE };

/////////////////////////////////////////////////////////////
// class SsmpGlobals
/////////////////////////////////////////////////////////////
class SsmpGlobals {
 public:
  SsmpGlobals(NAHeap *ssmpheap, IpcEnvironment *ipcEnv, StatsGlobals *statsGlobals);
  ~SsmpGlobals();
  NAHeap *getHeap() { return heap_; }
  StatsGlobals *getStatsGlobals() { return statsGlobals_; }
  void releaseOrphanEntries() {}
  int allocateServers();
  IpcServer *allocateServer(char *nodeName, short nodeNameLen, short cpuNum);
  void allocateServerOnNextRequest(char *nodeName, short nodeNameLen, short cpuNum);
  int deAllocateServer(char *nodeName, short nodeNameLen, short cpuNum);
  void work();
  long getStatsCollectionInterval() { return statsCollectionInterval_; }
  long getStatsMergeTimeout() { return statsTimeout_; }
  Long &getSemId() { return semId_; }
  IpcEnvironment *getIpcEnv() { return ipcEnv_; }
  IpcSetOfConnections getRecipients() { return recipients_; }
  void addRecipients(SscpClientMsgStream *msgStream);
  NAHeap *getStatsHeap() { return statsHeap_; }
  int myCpu() { return myCpu_; }
  pid_t myPin() { return myPin_; }
  NABoolean getForceMerge() { return forceMerge_; }
  int getNumDeallocatedServers() { return deallocatedSscps_->numEntries(); }
  inline NABoolean doingGC() { return doingGC_; }
  inline void setDoingGC(NABoolean value) { doingGC_ = value; }
  inline int getNumPendingSscpMessages() { return pendingSscpMessages_->numEntries(); }
  inline void finishPendingSscpMessages();
  inline void addPendingSscpMessage(SscpClientMsgStream *sscpClientMsgStream) {
    pendingSscpMessages_->insert(sscpClientMsgStream, sizeof(sscpClientMsgStream));
  }
  void removePendingSscpMessage(SscpClientMsgStream *sscpClientMsgStream);
  inline short getStoreSqlLen() { return storeSqlSrcLen_; }
  inline short getLdoneRetryTimes() { return ldoneRetryTimes_; }
  inline short getNumAllocatedServers() { return (short)sscps_->numEntries(); }
  inline void incSsmpReqMsg(long msgBytes) { statsGlobals_->incSsmpReqMsg(msgBytes); }
  inline void incSsmpReplyMsg(long msgBytes) { statsGlobals_->incSsmpReplyMsg(msgBytes); }
  void insertDeallocatedSscp(char *nodeName, short cpuNum);
  bool cancelQueryTree(char *queryId, int queryIdLen, CancelQueryRequest *request, ComDiagsArea **diags);
  bool cancelQuery(char *queryId, int queryIdLen, CancelQueryRequest *request, ComDiagsArea **diags);
  inline ActiveQueryMgr &getActiveQueryMgr() { return activeQueryMgr_; }
  inline PendingQueryMgr &getPendingQueryMgr() { return pendingQueryMgr_; }
  void cleanupDeletedSscpServers();
  bool getQidFromPid(int pid,         // IN
                     int minimumAge,  // IN
                     char *queryId,   // OUT
                     int &queryIdLen  // OUT
  );
  bool activateFromQid(char *queryId, int qidLen,
                       SuspendOrActivate sOrA,  // Param is placeholder.
                                                // Someday may handle cancel.
                       ComDiagsArea *&diags, bool suspendLogging);
  void suspendOrActivate(char *queryId, int qidLen, SuspendOrActivate sOrA, bool suspendLogging);
  int stopMasterProcess(char *queryId, int queryIdLen);

 private:
  NAHeap *heap_;  // pointer to heap for process duration storage
  StatsGlobals *statsGlobals_;
  IpcEnvironment *ipcEnv_;
  IpcServerClass *sscpServerClass_;
  HashQueue *sscps_;
  NAList<IpcServer *> *deletedSscps_;  // list of sscp servers to be deleted
  long statsCollectionInterval_;
  long statsTimeout_;
  IpcSetOfConnections recipients_;
  Long semId_;
  int myCpu_;
  pid_t myPin_;
  NAHeap *statsHeap_;  // Heap to store merged stats
  Queue *deallocatedSscps_;
  NABoolean forceMerge_;
  NABoolean doingGC_;
  Queue *pendingSscpMessages_;
  short storeSqlSrcLen_;
  ActiveQueryMgr activeQueryMgr_;
  PendingQueryMgr pendingQueryMgr_;
  short ldoneRetryTimes_;
};  // SsmpGlobals

class SsmpGuaReceiveControlConnection : public GuaReceiveControlConnection {
 public:
  SsmpGuaReceiveControlConnection(IpcEnvironment *env, SsmpGlobals *ssmpGlobals, short receiveDepth = 256)
      : GuaReceiveControlConnection(env, receiveDepth) {
    ssmpGlobals_ = ssmpGlobals;
  }

  virtual void actOnSystemMessage(short messageNum, IpcMessageBufferPtr sysMsg, IpcMessageObjSize sysMsgLen,
                                  short clientFileNumber, const GuaProcessHandle &clientPhandle,
                                  GuaConnectionToClient *connection);
  SsmpGlobals *getSsmpGlobals() { return ssmpGlobals_; }

 private:
  SsmpGlobals *ssmpGlobals_;
};  // SsmpGuaReceiveControlConnection

// -----------------------------------------------------------------------
// An object that holds a new connection, created by a Guardian open
// system message, until the first application message comes in
// -----------------------------------------------------------------------

class SsmpNewIncomingConnectionStream : public IpcMessageStream {
 public:
  SsmpNewIncomingConnectionStream(NAHeap *heap, IpcEnvironment *ipcEnv, SsmpGlobals *ssmpGlobals)
      : IpcMessageStream(ipcEnv, IPC_MSG_SSMP_REPLY, CurrSsmpReplyMessageVersion,
#ifndef USE_SB_NEW_RI
                         RTS_STATS_MSG_BUF_SIZE,
#else
                         ipcEnv->getGuaMaxMsgIOSize(),
#endif
                         TRUE),
        sscpDiagsArea_(NULL) {
    ipcEnv_ = ipcEnv;
    ssmpGlobals_ = ssmpGlobals;
    heap_ = heap;
    handle_ = INVALID_RTS_HANDLE;
    wmsProcess_ = FALSE;
    bytesReplied_ = 0;
  }

  ~SsmpNewIncomingConnectionStream();
  virtual void actOnSend(IpcConnection *connection);
  virtual void actOnSendAllComplete();
  virtual void actOnReceive(IpcConnection *connection);
  virtual void actOnReceiveAllComplete();
  SsmpGlobals *getSsmpGlobals() { return ssmpGlobals_; }
  NAHeap *getHeap() { return heap_; }
  IpcEnvironment *getIpcEnv() { return ipcEnv_; }

  void actOnStatsReq(IpcConnection *connection);
  void actOnCpuStatsReq(IpcConnection *connection);
  void actOnExplainReq(IpcConnection *connection);
  void actOnQueryStartedReq(IpcConnection *connection);
  void actOnQueryFinishedReq(IpcConnection *connection);
  void actOnCancelQueryReq(IpcConnection *connection);
  void actOnSuspendQueryReq(IpcConnection *connection);
  void actOnActivateQueryReq(IpcConnection *connection);
  void actOnSecInvalidKeyReq(IpcConnection *connection);
  void actOnSnapshotLockReq(IpcConnection *connection);
  void actOnSnapshotUnLockReq(IpcConnection *connection);
  void actOnObjectEpochChangeReq(IpcConnection *connection);
  void actOnObjectEpochStatsReq(IpcConnection *connection);
  void actOnObjectLockReq(IpcConnection *connection);
  void actOnObjectLockStatsReq(IpcConnection *connection);
  void actOnQryInvalidStatsReq(IpcConnection *connection);
  void getProcessStats(short reqType, short subReqType, pid_t pid);

  void getMergedStats(RtsStatsReq *request, RtsQueryId *queryId, StmtStats *stmtStats, short reqType,
                      UInt16 statsMergeType);
  void sendMergedStats(ExStatisticsArea *mergedStats, short numErrors, short reqType, StmtStats *stmtStats,
                       NABoolean updateMergeStats);
  void sendMergedEpochStats(ExStatisticsArea *mergedStats);
  void sendMergedLockStats(ExStatisticsArea *mergedStats);
  void sscpIpcError(IpcConnection *conn);
  ComDiagsArea *getSscpDiagsArea() { return sscpDiagsArea_; }
  void clearSscpDiagsArea() {
    sscpDiagsArea_->decrRefCount();
    sscpDiagsArea_ = NULL;
  }

  inline RtsHandle getHandle() { return handle_; }
  inline void setHandle(const RtsHandle h) { handle_ = h; }
  inline NABoolean isWmsProcess() { return wmsProcess_; }
  inline void setWmsProcess(NABoolean flag) { wmsProcess_ = flag; }

 private:
  NAHeap *heap_;
  IpcEnvironment *ipcEnv_;
  SsmpGlobals *ssmpGlobals_;
  ComDiagsArea *sscpDiagsArea_;
  RtsHandle handle_;
  NABoolean wmsProcess_;
  IpcMessageObjSize bytesReplied_;

};  // SsmpNewIncomingConnectionStream

// -----------------------------------------------------------------------
// The message stream used by the send top node to exchange data with
// the send bottom node via an Ipc connection
// -----------------------------------------------------------------------

class SscpClientMsgStream : public IpcMessageStream {
 public:
  // constructor
  SscpClientMsgStream(NAHeap *heap, IpcEnvironment *ipcEnv, SsmpGlobals *ssmpGlobals,
                      SsmpNewIncomingConnectionStream *ssmpStream)
      : IpcMessageStream(ipcEnv, IPC_MSG_SSCP_REQUEST, CurrSscpRequestMessageVersion,
#ifndef USE_SB_NEW_RI
                         RTS_STATS_MSG_BUF_SIZE,
#else
                         ipcEnv->getGuaMaxMsgIOSize(),
#endif
                         TRUE),  // Share the objects,
        heap_(heap) {
    ssmpGlobals_ = ssmpGlobals;
    mergedStats_ = NULL;
    mergedEpochStats_ = NULL;
    mergeStartTime_ = 0;
    numOfClientRequestsSent_ = 0;
    numOfErrorRequests_ = 0;
    replySent_ = FALSE;
    ssmpStream_ = ssmpStream;
    numSqlProcs_ = 0;
    numCpus_ = 0;
    stmtStats_ = NULL;
    detailLevel_ = 0;
    completionProcessing_ = STATS;
    subReqType_ = -1;

    oecrResult_ = ObjectEpochChangeReply::SUCCESS;
    oecrMaxExpectedEpochFound_ = 0;
    oecrMaxExpectedFlagsFound_ = 0;

    objectLockState_ = ObjectLockReply::LOCK_UNKNOWN;
    mergedLockStats_ = NULL;
  }
  ~SscpClientMsgStream();
  // method called upon send complete
  virtual void actOnSendAllComplete();

  // method called upon receive complete
  virtual void actOnReceive(IpcConnection *connection);
  virtual void actOnReceiveAllComplete();

  void actOnStatsReply(IpcConnection *connection);
  void actOnObjectEpochStatsReply();
  void actOnObjectLockReply();
  void actOnObjectLockStatsReply();
  void delinkConnection(IpcConnection *conn);
  NAHeap *getHeap() { return heap_; }
  inline long getMergeStartTime() { return mergeStartTime_; }
  inline void setMergeStartTime(long startTime) { mergeStartTime_ = startTime; }
  ExStatisticsArea *getMergedStats() { return mergedStats_; }
  void incNumOfClientRequestsSent() { numOfClientRequestsSent_++; }
  int getNumOfClientRequestsPending() { return numOfClientRequestsSent_; }
  int getNumOfErrorRequests() { return numOfErrorRequests_; }
  SsmpGlobals *getSsmpGlobals() { return ssmpGlobals_; }
  NABoolean isReplySent() { return replySent_; }
  SsmpNewIncomingConnectionStream *getSsmpStream() { return ssmpStream_; }
  void setReplySent() {
    replySent_ = TRUE;
    mergedStats_ = NULL;
  }
  void sendMergedStats();
  void sendMergedEpochStats();
  void sendMergedLockStats();
  inline short getReqType() { return reqType_; }
  inline void setReqType(short reqType) { reqType_ = reqType; }
  inline void incNumSqlProcs(short i = 1) { numSqlProcs_ += i; }
  inline void incNumCpus(short i = 1) { numCpus_ += i; }
  inline short getNumSqlProcs() { return numSqlProcs_; }
  inline short getNumCpus() { return numCpus_; }
  inline StmtStats *getStmtStats() { return stmtStats_; }
  inline void setStmtStats(StmtStats *stmtStats) { stmtStats_ = stmtStats; }
  inline void setDetailLevel(short level) { detailLevel_ = level; }
  inline short getDetailLevel() { return detailLevel_; }
  inline void setUsedToSendCbMsgs() { completionProcessing_ = CB; }
  inline void setUsedToSendSikMsgs() { completionProcessing_ = SIK; }
  inline void setUsedToSendSlMsgs() { completionProcessing_ = SL; }
  inline void setUsedToSendSulMsgs() { completionProcessing_ = SUL; }
  inline void setUsedToSendLLMsgs() { completionProcessing_ = LL; }
  inline void setUsedToSendOecMsgs() { completionProcessing_ = OEC; }
  inline void setUsedToSendOesMsgs() { completionProcessing_ = OES; }
  inline void setUsedToSendOlMsgs() { completionProcessing_ = OL; }
  inline void setUsedToSendOlsMsgs() { completionProcessing_ = OLS; }
  void replySik();
  void replySL();
  void replyLL();
  void replyOEC();
  void replyOL();
  inline short getSubReqType() { return subReqType_; }
  inline void setSubReqType(short subReqType) { subReqType_ = subReqType; }
  inline ObjectLockReply::LockState getObjectLockState() const { return objectLockState_; }
  inline void setObjectLockState(ObjectLockReply::LockState state) { objectLockState_ = state; }
  inline int getObjectLockConflictNid() const { return objectLockConflictNid_; }
  inline void setObjectLockConflictNid(int nid) { objectLockConflictNid_ = nid; }
  inline int getObjectLockConflictPid() const { return objectLockConflictPid_; }
  inline void setObjectLockConflictPid(int pid) { objectLockConflictPid_ = pid; }

 private:
  NAHeap *heap_;
  ExStatisticsArea *mergedStats_;
  long mergeStartTime_;
  int numOfClientRequestsSent_;
  SsmpGlobals *ssmpGlobals_;
  int numOfErrorRequests_;
  NABoolean replySent_;
  SsmpNewIncomingConnectionStream *ssmpStream_;
  short reqType_;
  short numSqlProcs_;
  short numCpus_;
  StmtStats *stmtStats_;
  enum { STATS, CB, SIK, SL, SUL, LL, OEC, OES, OL, OLS } completionProcessing_;
  short detailLevel_;
  short subReqType_;

  // information accumulated from ObjectEpochChangeReplys
  ObjectEpochChangeReply::Result oecrResult_;
  UInt32 oecrMaxExpectedEpochFound_;
  UInt32 oecrMaxExpectedFlagsFound_;

  // Merged stats for ObjectEpochStatsReply, maybe we can reuse the
  // `mergedStats_` member, but I am not sure, so add a new member to
  // avoid possible impact on other statistics functions.
  ExStatisticsArea *mergedEpochStats_;

  // Object lock result state
  ObjectLockReply::LockState objectLockState_;
  int objectLockConflictNid_;
  int objectLockConflictPid_;
  // Merged stats for ObjectLockStatsReply
  ExStatisticsArea *mergedLockStats_;
};

// -----------------------------------------------------------------------
// The message stream used by the collector in ExStatsTcb or ExExplainTcb
// via an Ipc connection
// -----------------------------------------------------------------------

class SsmpClientMsgStream : public IpcMessageStream {
 public:
  // constructor
  SsmpClientMsgStream(NAHeap *heap, ExSsmpManager *ssmpManager, ComDiagsArea *diagsForClient = NULL)

      : IpcMessageStream(ssmpManager->getIpcEnvironment(), IPC_MSG_SSMP_REQUEST, CurrSsmpRequestMessageVersion,
#ifndef USE_SB_NEW_RI
                         RTS_STATS_MSG_BUF_SIZE,
#else
                         ssmpManager->getIpcEnvironment()->getGuaMaxMsgIOSize(),
#endif
                         TRUE),
        heap_(heap),
        ssmpManager_(ssmpManager),
        diagsForClient_(diagsForClient) {
    stats_ = NULL;
    replyRecvd_ = FALSE;
    rtsQueryId_ = NULL;
    numSscpReqFailed_ = 0;
    explainFrag_ = NULL;

    oecrResult_ = 0;
    oecrMaxExpectedEpochFound_ = 0;
    oecrMaxExpectedFlagsFound_ = 0;

    objectLockState_ = ObjectLockReply::LOCK_UNKNOWN;
  }

  // method called upon send complete
  virtual void actOnSend(IpcConnection *connection);
  virtual void actOnSendAllComplete();

  // method called upon receive complete
  virtual void actOnReceive(IpcConnection *connection);
  virtual void actOnReceiveAllComplete();

  virtual void delinkConnection(IpcConnection *);

  void actOnStatsReply(IpcConnection *connection);
  void actOnExplainReply(IpcConnection *connection);
  void actOnObjectEpochChangeReply();
  void actOnObjectEpochStatsReply();
  void actOnObjectLockReply();
  void actOnObjectLockStatsReply();
  void actOnGenericReply();
  void actOnQryInvalidStatsReply();
  ExStatisticsArea *getStats() { return stats_; };
  NAHeap *getHeap() { return heap_; }
  NABoolean isReplyReceived() { return replyRecvd_; }
  RtsQueryId *getRtsQueryId() { return rtsQueryId_; }
  short getNumSscpReqFailed() { return numSscpReqFailed_; }
  RtsExplainFrag *getExplainFrag() { return explainFrag_; }
  int getOecrResult() { return oecrResult_; }
  UInt32 getOecrMaxExpectedEpochFound() { return oecrMaxExpectedEpochFound_; }
  UInt32 getOecrMaxExpectedFlagsFound() { return oecrMaxExpectedFlagsFound_; }
  ObjectLockReply::LockState getObjectLockState() const { return objectLockState_; }
  void setObjectLockState(ObjectLockReply::LockState state) { objectLockState_ = state; }
  inline int getObjectLockConflictNid() const { return objectLockConflictNid_; }
  inline void setObjectLockConflictNid(int nid) { objectLockConflictNid_ = nid; }
  inline int getObjectLockConflictPid() const { return objectLockConflictPid_; }
  inline void setObjectLockConflictPid(int pid) { objectLockConflictPid_ = pid; }

 private:
  NAHeap *heap_;
  ExStatisticsArea *stats_;
  ExSsmpManager *ssmpManager_;
  ComDiagsArea *diagsForClient_;  // non-null if client wants ipc diags.
  NABoolean replyRecvd_;
  RtsQueryId *rtsQueryId_;
  short numSscpReqFailed_;
  RtsExplainFrag *explainFrag_;

  // information saved from ObjectEpochChangeReply
  int oecrResult_;
  UInt32 oecrMaxExpectedEpochFound_;
  UInt32 oecrMaxExpectedFlagsFound_;

  // Object lock reply state
  ObjectLockReply::LockState objectLockState_;
  int objectLockConflictNid_;
  int objectLockConflictPid_;
};

#endif  // _SSMPIPC_H_
