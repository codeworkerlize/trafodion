/**********************************************************************

// File:         sscpIpc.h
// Description:  Class declaration for SSCP IPC infrastructure
//
// Created:      5/01/2006
**********************************************************************/
#ifndef _SSCPIPC_H_
#define _SSCPIPC_H_
#include "SqlStats.h"
#include "common/Ipc.h"

class StatsGlobals;
class ExStatisticsArea;
class RtsQueryId;
/////////////////////////////////////////////////////////////
// class SscpGlobals
/////////////////////////////////////////////////////////////

class SscpGlobals {
 public:
  SscpGlobals(NAHeap *sscpheap, StatsGlobals *statsGlobals);
  ~SscpGlobals();
  NAHeap *getHeap() { return heap_; }
  StatsGlobals *getStatsGlobals() { return statsGlobals_; }
  void releaseOrphanEntries() {}
  Long &getSemId() { return semId_; }
  pid_t myPin() { return myPin_; }
  inline void incSscpReqMsg(long msgBytes) { statsGlobals_->incSscpReqMsg(msgBytes); }
  inline void incSscpReplyMsg(long msgBytes) { statsGlobals_->incSscpReplyMsg(msgBytes); }
  bool shouldLogCancelKillServers() { return doLogCancelKillServers_; }

 private:
  NAHeap *heap_;  // pointer to heap for process duration storage
  StatsGlobals *statsGlobals_;
  Long semId_;
  pid_t myPin_;
  bool doLogCancelKillServers_;
};  // SSCPGlobals

class SscpGuaReceiveControlConnection : public GuaReceiveControlConnection {
 public:
  SscpGuaReceiveControlConnection(IpcEnvironment *env, SscpGlobals *sscpGlobals, short receiveDepth = 256)
      : GuaReceiveControlConnection(env, receiveDepth) {
    sscpGlobals_ = sscpGlobals;
  }

  virtual void actOnSystemMessage(short messageNum, IpcMessageBufferPtr sysMsg, IpcMessageObjSize sysMsgLen,
                                  short clientFileNumber, const GuaProcessHandle &clientPhandle,
                                  GuaConnectionToClient *connection);
  SscpGlobals *getSscpGlobals() { return sscpGlobals_; }

 private:
  SscpGlobals *sscpGlobals_;
};  // SscpGuaReceiveControlConnection
// -----------------------------------------------------------------------
// An object that holds a new connection, created by a Guardian open
// system message, until the first application message comes in
// -----------------------------------------------------------------------

class SscpNewIncomingConnectionStream : public IpcMessageStream {
 public:
  SscpNewIncomingConnectionStream(NAHeap *heap, IpcEnvironment *env, SscpGlobals *sscpGlobals);

  virtual void actOnSend(IpcConnection *connection);
  virtual void actOnSendAllComplete();
  virtual void actOnReceive(IpcConnection *connection);
  virtual void actOnReceiveAllComplete();
  NAHeap *getHeap() { return heap_; }
  SscpGlobals *getSscpGlobals() { return sscpGlobals_; }
  void processStatsReq(IpcConnection *connection);
  void processCpuStatsReq(IpcConnection *connection);
  void processKillServersReq();
  void suspendActivateSchedulers();
  void processSecInvReq();
  void processSnapshotLockReq();
  void processSnapshotUnLockReq();
  void processObjectEpochChangeReq();
  void processObjectEpochStatsReq();
  void processObjectLockReq();
  void processObjectLockStatsReq();

 private:
  SscpGlobals *sscpGlobals_;
  IpcEnvironment *ipcEnv_;
  NAHeap *heap_;
  IpcMessageObjSize bytesReplied_;
};  // SscpNewIncomingConnectionStream

#endif  // _SSCPIPC_H_
