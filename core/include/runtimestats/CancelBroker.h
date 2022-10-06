/**********************************************************************

// File:         CancelBroker.h
// Description:  Class declaration for Cancel Broker role of SSMP.
//
// Created:      Aug 17, 2009
**********************************************************************/

#ifndef _CANCELBROKER_H_
#define _CANCELBROKER_H_
#include "comexe/ComQueue.h"
#include "common/Int64.h"
#include "common/Ipc.h"
#include "runtimestats/ssmpipc.h"

// -----------------------------------------------------------------------
// Classes defined in this file
// -----------------------------------------------------------------------
class ActiveQueryMgr;
class ActiveQueryEntry;
class ActiveQueryStream;
class PendingQueryMgr;
class PendingQueryEntry;

// -----------------------------------------------------------------------
// Classes referenced in this file
// -----------------------------------------------------------------------
class SsmpNewIncomingConnectionStream;
class SsmpGlobals;

// -----------------------------------------------------------------------
// API for some methods.
// -----------------------------------------------------------------------

enum NextActionForSubject { CB_COMPLETE, CB_CANCEL, CB_DONT_CARE };

/////////////////////////////////////////////////////////////
// class ActiveQueryStream
//
// Since the Start Query message is not replied to until the Finished
// Query or Cancel Query message arrives, an ActiveQueryEntry needs
// its own message stream to allow the SsmpNewIncomingConnectionStream
// to continue receiving and replying to unrelated messages during
// this interval.  The method IpcMessageStream::giveMessageTo is used
// to transfer the message from SsmpNewIncomingConnectionStream to
// the ActiveQueryStream.  The model here is similar to that which
// is used by ex_split_bottom_tcb's SplitBottomSavedMessage.
/////////////////////////////////////////////////////////////

class ActiveQueryStream : public IpcMessageStream {
 public:
  ActiveQueryStream(IpcEnvironment *env, SsmpGlobals *ssmpG, ActiveQueryEntry *myAq);

  virtual void actOnSend(IpcConnection *connection);

 private:
  ActiveQueryEntry *myAq_;
  SsmpGlobals *ssmpGlobals_;
};

/////////////////////////////////////////////////////////////
// class ActiveQueryEntry
/////////////////////////////////////////////////////////////

class ActiveQueryEntry : public NABasicObject {
 public:
  ActiveQueryEntry(char *qid, int qidLen, long startTime, GuaProcessHandle master, short masterFileNum,
                   int executionCount, IpcEnvironment *ipcEnv, SsmpGlobals *ssmpG);

  ~ActiveQueryEntry();

  void replyToQueryStarted(NAHeap *ipcHeap, NextActionForSubject nxtA, bool cancelLogging);

  char *getQid() const { return qid_; }

  int getQidLen() const { return qidLen_; }

  long getQueryStartTime() const { return queryStartTime_; }

  GuaProcessHandle getMasterPhandle() const { return master_; }

  short getMasterFileNum() const { return masterFileNum_; }

  ActiveQueryStream *getReplyStartedStream() { return replyStartedStream_; }

  int getExecutionCount() const { return executionCount_; }

  void releaseStream();

 private:
  char *qid_;
  int qidLen_;
  ActiveQueryStream *replyStartedStream_;
  long queryStartTime_;
  GuaProcessHandle master_;
  short masterFileNum_;
  int executionCount_;
};  // ActiveQueryEntry

/////////////////////////////////////////////////////////////
// class ActiveQueryMgr
/////////////////////////////////////////////////////////////

class ActiveQueryMgr : public NABasicObject {
 public:
  ActiveQueryMgr(IpcEnvironment *ipcEnv, NAHeap *heap) : activeQueries_(heap), heap_(heap), ipcEnv_(ipcEnv) {}

  ~ActiveQueryMgr(){};

  // Called from actOnCancelQueryReq for the Cancel Query msg.
  ActiveQueryEntry *getActiveQuery(char *qid, int qidLen);

  // Called from actOnQueryStartedReq for the Begin Query msg
  void addActiveQuery(char *qid, int qidLen, long startTime, GuaProcessHandle masterPhandle, int executionCount,
                      SsmpNewIncomingConnectionStream *cStream, IpcConnection *conn);

  // Called from actOnQueryFinishedReq for the Finished Query msg.  This call
  // will send a reply to the QueryStarted message.  The caller is responsible
  // for replying to the FinishedQuery msg.
  void rmActiveQuery(char *qid, int qidLen, NAHeap *ipcHeap, NextActionForSubject nxtA, bool cancelLogging);

  // Called from actOnSystemMessage for the close message, if the master
  // process ends before the Finished Query message is sent.
  void clientIsGone(const GuaProcessHandle &client, short fnum);

 private:
  NAHeap *heap_;

  IpcEnvironment *ipcEnv_;

  HashQueue activeQueries_;
};  // ActiveQueryMgr

/////////////////////////////////////////////////////////////
// class PendingQueryEntry
/////////////////////////////////////////////////////////////

class PendingQueryEntry : public NABasicObject {
 public:
  PendingQueryEntry(char *qid, int qidLen, int executionCount, GuaProcessHandle master, short masterFileNum,
                    long escalateTime1, long escalateTime2, bool cancelEscalationSaveabend, bool cancelLogging);

  ~PendingQueryEntry();

  char *getQid() const { return qid_; }

  int getQidLen() const { return qidLen_; }

  int getExecutionCount() const { return executionCount_; }

  long getEscalateTime1() const { return escalateTime1_; }

  long getEscalateTime2() const { return escalateTime2_; }

  bool getCancelEscalationSaveabend() const { return cancelEscalationSaveabend_; }

  GuaProcessHandle getMasterPhandle() const { return master_; }

  short getMasterFileNum() const { return masterFileNum_; }

  bool getHaveEscalated1() const { return haveEscalated1_; }

  void setHaveEscalated1() { haveEscalated1_ = true; }

  bool getCancelLogging() const { return cancelLogging_; }

 private:
  char *qid_;
  int qidLen_;
  int executionCount_;
  long escalateTime1_;
  long escalateTime2_;
  bool cancelEscalationSaveabend_;
  bool haveEscalated1_;
  GuaProcessHandle master_;
  short masterFileNum_;
  bool cancelLogging_;
};  // PendingQueryEntry

class PendingQueryMgr : public NABasicObject {
 public:
  PendingQueryMgr(SsmpGlobals *ssmpGlobals, NAHeap *heap);

  ~PendingQueryMgr(){};

  // Called from actOnCancelRequest
  void addPendingQuery(ActiveQueryEntry *aq, int ceFirstInterval, int ceSecondInterval, NABoolean ceSaveabend,
                       NABoolean cancelLogging);

  // Called from SsmpGlobals::work every time it awakes.
  void killPendingCanceled();

  // Called from actOnSystemMessage for the close message, if the master
  // process ends before the Finished Query message is sent.
  void clientIsGone(const GuaProcessHandle &client, short fnum);

 private:
  void removePendingQuery(PendingQueryEntry *pq);

  // Called from this->killPendingCanceled(), to stop server
  // processes of one pending query.
  void askSscpsToStopServers(PendingQueryEntry *pq);

  SsmpGlobals *ssmpGlobals_;

  HashQueue pendingQueries_;
};  // PendingQueryMgr

#endif  // _CANCELBROKER_H_
