
#ifndef EX_FRAG_RT_H
#define EX_FRAG_RT_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ex_frag_rt.h
 * Description:  Run time fragment management in the master executor
 *
 * Created:      1/22/96
 * Language:     C++
 *
 *
 *****************************************************************************
 */

#include "common/ComExeTrace.h"
#include "common/Ipc.h"
#include "common/NABitVector.h"
#include "executor/ExScheduler.h"
#include "executor/ex_frag_inst.h"

// -----------------------------------------------------------------------
// Contents of this file
// -----------------------------------------------------------------------

class ExRtFragTable;
class ExRtFragTableEntry;
class ExRtFragInstance;
class ExMasterEspMessage;
class ExEspDbEntry;
class ExEspCacheKey;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
class ex_root_tcb;
class ExMasterStmtGlobals;
class ExOperStats;
class ContextCli;
class NAWNodeSet;

// reply buffer length for control messages
#define CONTROL_MSG_REPLY_BUFFER_LENGTH 400

// -----------------------------------------------------------------------
// The run-time fragment directory (contains all the allocated ESPs and
// handles of the downloaded fragment instances for a statement and
// resides in the master executor). It is allocated once in the global
// Space object.
// -----------------------------------------------------------------------
class ExRtFragTable : public NABasicObject {
  friend class ExRtFragTableEntry;
  friend class ExRtFragInstance;
  friend class ExMasterEspMessage;

 public:
  // state of the fragment directory (all downloaded instances together)
  enum FragDirState {
    NOT_ASSIGNED,  // no ESPs have been assigned
    NO_ESPS_USED,  // this statement doesn't use ESPs (may have DP2 frags)
    ASSIGNED,      // ESPs are assigned
    READY,         // all fragments are fixed up and ready
    ERROR,         // an error occurred, statement can not continue
  };

  ExRtFragTable(ExMasterStmtGlobals *glob, ExFragDir *fragDir, char *generatedObject);
  ~ExRtFragTable();

  // get the state
  inline enum FragDirState getState() const { return state_; }

  // root node indicates that it is processing the next exec() request
  // or that it is done with the current request
  // NOTE: with dynamic load balancing in progress the root node
  // can't process more than one request at a time
  NABoolean setActiveState();
  void setInactiveState();

  // assign ESPs for all entries after talking to the resource governor.
  // numOfEspsStarted: number of new esps started by this operator.

  // VO, Genesis solution 10-051125-2802:
  // Added ComVersion_NodeInfo parameters. See ex_root.cpp for explanation.
  void assignEsps(NABoolean checkResourceGovernor, UInt32 &numOfTotalEspsUsed, UInt32 &numOfEspsStarted,
                  NABoolean checkDuplicateEsps = false);

  // unfix, and release ESPs when errors occur during assignEsps()
  void releaseAssignedEsps();

  // download fragments unless already done and fix them up
  void downloadAndFixup();

  // restore priority for all esps after fixup is completed
  short restoreEspPriority();

  // assign initial (or permanent) partition ranges to the ESPs
  // or assign additional ranges during dynamic load balancing
  // (may subcontract this task out to another process),
  // send transaction down to ESPs, if needed
  void assignPartRangesAndTA(NABoolean initial);

  // stop using the transaction and quiesce those ESPs that need
  // a transaction, complete all transaction-bearing I/Os.
  // If savepoints were being used, tell esps to commit or abort them.
  void releaseTransaction(NABoolean allWorkRequests, NABoolean alwaysSendReleaseMsg, NABoolean commitSavepoint,
                          NABoolean rollbackSavepoint, long savepointId);

  // tell whether there are outstanding transaction requests
  inline NABoolean hasOutstandingWorkRequests() const { return numWorkMsgesOut_ > 0; }

  // tell how many outstanding transaction requests
  inline int numOutstandingWorkRequests() const { return numWorkMsgesOut_; }

  // tell whether there are outstanding transaction requests
  inline NABoolean hasOutstandingTransactionalMsges() const { return numTransactionalMsgesOut_ > 0; }

  // tell how many outstanding transaction requests
  inline int numOutstandingTransactionalMsges() const { return numTransactionalMsgesOut_; }

  // tell whether there are outstanding load/fixup requests
  inline NABoolean hasOutstandingLoadFixupMsges() const { return numLoadFixupMsgesOut_ > 0; }

  // tell whether there are outstanding release esp requests
  inline NABoolean hasOutstandingReleaseEspMsges() const { return numReleaseEspMsgesOut_ > 0; }

  // tell how many outstanding release esp requests
  inline int numOutstandingReleaseEspMsges() const { return numReleaseEspMsgesOut_; }

  // start working again after a call to releaseTransaction()
  void continueWithTransaction();

  // close cursors, unfix, and release ESPs
  void releaseEsps(NABoolean closeAllOpens);

  // get the process id of a fragment instance
  const IpcProcessId &getInstanceProcessId(ExFragId fragId, CollIndex espNum) const;

  // how many instances are there for a given fragment id
  int getNumOfInstances(ExFragId fragId) const;

  // get the control connection for a given instance
  IpcConnection *getControlConnection(ExFragId fragId, CollIndex espNum) const;
  // get the fragment handle for a given instance
  int getFragmentHandle(ExFragId fragId, CollIndex espNum) const;

  // check whether a given process id is this process or whether
  // it identifies another process
  NABoolean isLocal(const IpcProcessId &procId) const;

  // return the fragment key for the root fragment of the statement
  // (can be easily converted into fragment keys for all other fragments)
  const ExFragKey &getMasterFragKey() const { return masterFragmentInstanceKey_; }

  // add sent service requests and remove finished ones
  void addRequestToBeSent(ExMasterEspMessage *m);
  NABoolean removeCompletedRequest(ExMasterEspMessage *m);

  // try to complete outstanding service requests
  ExWorkProcRetcode workOnRequests();

  // make the scheduler event known to the run-time frag table
  void setSchedulerEvent(ExSubtask *st) { schedulerEvent_ = st; }

  // manage GUI display (debugging only)
  inline void displayGuiForESPs(NABoolean doit = TRUE) { displayInGui_ = doit; }
  inline NABoolean isGuiDisplayActive() const { return displayInGui_; }

  // access executor statement globals
  inline ExMasterStmtGlobals *getGlobals() const { return glob_; }

  // get the available nodes for this statement/tenant
  const NAWNodeSet *getAvailableNodes() const;

  // used by Statement::releaseTransaction after fatal error.
  void abandonPendingIOs();

  // used by runtime statistics.
  short countSQLNodes(short masterNode);

  // SeaMonster: Print routing information
  void dumpSMRouteTable();

  void reportESPLimitViolation(int totalESPLimit, int numOfNodes, int numESPsNeeded, NABoolean singleStmtExceededLimit);

  // for debugging
  void print();

#ifdef IPC_INTEGRITY_CHECKING
  void checkIntegrity(void);
  void checkLocalIntegrity(void);
#endif

 private:
  // state of a downloaded fragment instance
  enum FragInstanceState {
    UNASSIGNED,
    ESP_ASSIGNED,
    DOWNLOADING,
    DOWNLOADED,
    FIXING_UP,
    FIXED_UP,
    LOST_CONNECTION
    // - comment out states that are not used
    // RELEASING
    // BROKEN
  };

  // the state
  enum FragDirState state_;

  // identification of the master fragment instance
  ExFragKey masterFragmentInstanceKey_;

  // all instances of all fragments
  LIST(ExRtFragTableEntry *) fragmentEntries_;

  // a pointer back to the executor globals
  ExMasterStmtGlobals *glob_;

  // a pointer to the generated fragment directory containing the number
  // of compiled fragments, the compiled object, and the recommended resource
  // usage
  ExFragDir *fragDir_;

  // the character buffer of the generated object from the module
  char *generatedObject_;

  // set if the frag dir performs dynamic load balancing for ESPs
  NABoolean dynamicLoadBalancing_;

  // indicate whether and how many queue entries of the root's down
  // queue to its child are currently active (in most cases we have
  // to restrict this to at most 1)
  int numRootRequests_;

  // set to TRUE if we need to complete all I/Os that are associated with
  // a transaction, so the application can commit or REPLY.
  NABoolean quiesce_;

  // count how many fragment instances still have a fixup message outstanding
  int numLoadFixupMsgesOut_;

  // count how many work messages are outstanding
  int numWorkMsgesOut_;

  // count how many transactional messages are outstanding
  // (can't commit a transaction while having such messages outstanding)
  int numTransactionalMsgesOut_;

  // count how many release esp messages are outstanding
  int numReleaseEspMsgesOut_;

  // A list of outstanding service messages, to be completed.
  LIST(ExMasterEspMessage *) outstandingServiceRequests_;

  // A link to the scheduler to indicate when more work is to be done
  ExSubtask *schedulerEvent_;

  // for debugging: start GUI display for ESPs
  NABoolean displayInGui_;

  // nodes available for multi-tenancy or Adaptive Segmentation, but
  // only if different from the default nodes stored in ContextCli and
  // only if needed by this query
  NAWNodeSet *availableNodes_;

  // private methods

  // add load and fixup requests to a message
  void addLoadRequestToMessage(ExMasterEspMessage *msg, ExFragId fragId, NABoolean addHeader = TRUE,
                               NABoolean compressFrag = FALSE);
  void addFixupRequestToMessage(ExMasterEspMessage *msg, ExFragId fragId, IpcPriority fixupPriority,
                                IpcPriority executePriority, int maxPollingInterval, int persistentOpens,
                                NABoolean espCloseErrorLogging, int espFreeMemTimeout);
  void addReleaseRequestToMessage(ExMasterEspMessage *msg, ExFragId fragId, int idleTimeout,
                                  NABoolean releaseAll = FALSE, NABoolean closeAllOpens = FALSE);

  // find a particular instance, given its control connection
  ExRtFragInstance *findInstance(ExFragId fragId, IpcConnection *connection) const;

  int getStopIdleEspsTimeout();
  int getEspIdleTimeout();
  int getEspInactiveTimeout();
};

// -----------------------------------------------------------------------
// all instances of one fragment (allocate on "Space")
// no public access, only friend is ExRtFragTable
// -----------------------------------------------------------------------

class ExRtFragTableEntry {
  friend class ExRtFragTable;
  friend class ExMasterEspMessage;

 private:
  // fragment id, this is the index of this entry
  ExFragId id_;

  // how many ESPs are needed (ex_frag_dir has compiler suggestion, this
  // is the REAL number)
  int numEsps_;

  // TRUE if we assign partitions to ESPs each time the root query is
  // executed, FALSE if we have a static assignments of partition input
  // values to ESPs (in which case #ESPs >= #part input values).
  NABoolean dynamicLoadBalancing_;

  // an array with an entry for each assigned ESP / fragment instance
  ARRAY(ExRtFragInstance *) assignedEsps_;

  // the partition descriptor, generating partition input values
  ExPartInputDataDesc *partDesc_;

  // used for dynamic load balancing, those partition input values that
  // are already assigned to an ESP
  NABitVector assignedPartInputValues_;

  // constructor and destructors
  ExRtFragTableEntry(CollHeap *heap);
  inline ~ExRtFragTableEntry() { release(); }
  void release();
};

// -----------------------------------------------------------------------
// one instance of one fragment (allocated on a heap)
// no public access, only friends are ExRtFragTable and ExRtFragTableEntry
// -----------------------------------------------------------------------

class ExRtFragInstance {
  friend class ExRtFragTable;
  friend class ExRtFragTableEntry;
  friend class ExMasterEspMessage;
  friend class ExEspManager;

 private:
  // desired properties of the ESP
  const char *clusterName_;
  IpcCpuNum cpuNum_;
  short memoryQuota_;

  // actual state of the assigned ESP (if any)
  ExRtFragTable::FragInstanceState state_;

  // the EspDbEntry: is a shared structure that can be pointed to by
  // multiple fragments or even multiple statements in this process
  ExEspDbEntry *usedEsp_;

  // the (shortcut) fragment handle assigned by the ESP, once the
  // ESP has told us this info in its reply to a load message
  int fragmentHandle_;

  // the heap from which "this" is allocated
  CollHeap *whereIComeFrom_;

  // set to the number of outstanding control messages (work + data)
  int numControlMessages_;

  // set if the fragment instance got partition input values and has
  // not yet replied to the PIV message or if PIVs are assigned statically
  NABoolean partInputDataAssigned_;

  // indicates whether we have sent a work request (with or without
  // transaction) to this instance
  NABoolean workMessageSent_;
  // and the pointer to the message stream
  ExMasterEspMessage *workMsg_;

  // indicates whether we are in the process of releasing the work request
  NABoolean releasingWorkMsg_;

  // indicates whether the esp should do the real work. This flag is
  // used to implment the "partial ESP" feathere where some of the ESPs
  // are not required to work, and some are.
  NABoolean needToWork_;

  // memory allocation/deallocation
  ExRtFragInstance(CollHeap *heap);
  ~ExRtFragInstance();
  void release();
  static void *operator new(size_t size);  // illegal form, will abort
  static void *operator new(size_t size, CollHeap *heap);
  static void operator delete(void *ptr);  // also illegal
  void deleteMe();
  inline ExMasterEspMessage *getWorkMsg() { return workMsg_; }
};

// -----------------------------------------------------------------------
// Request message used by the master executor to set ESPs up for
// query processing. This is an object that gets created and filled with
// a request. Once the request is sent, it continues on its own,
// completing the I/Os and receiving replies.
// -----------------------------------------------------------------------

class ExMasterEspMessage : public IpcMessageStream {
  friend class ExRtFragTable;

 public:
  ExMasterEspMessage(IpcEnvironment *env, ExRtFragTable *rtFragTable);
  virtual ~ExMasterEspMessage();

  inline void markAsDownloadRequest() { downloadRequest_ = TRUE; }
  inline void markAsFixupRequest() { fixupRequest_ = TRUE; }
  inline void markAsAssignPartRequest() { assignPartRequest_ = TRUE; }
  inline void markAsWorkTransRequest() { workTransRequest_ = TRUE; }
  inline void markAsReleaseTransRequest() { releaseTransRequest_ = TRUE; }
  inline void markAsReleaseRequest() { releaseRequest_ = TRUE; }
  inline void markAsTransactionalRequest() { transactionalRequest_ = TRUE; }

  virtual void actOnSend(IpcConnection *connection);
  virtual void actOnSendAllComplete();
  virtual void actOnReceive(IpcConnection *connection);
  virtual void actOnReceiveAllComplete();
  virtual ExMasterEspMessage *castToExMasterEspMessage(void);
  void incReqMsg(long msgBytes);

 private:
  // a helper method that does book keepings on rtFragTable_
  void decrFragTableCounters();

  // a helper method that cleans up frag instance states upon connection error
  void actOnErrorConnection(IpcConnection *connection);

  // a pointer back to the run-time fragment table, so we can update its
  // state as we receive replies
  ExRtFragTable *rtFragTable_;

  // indicators on the request, this helps the callbacks to set the
  // states of the run-time fragment table to appropriate values
  NABoolean downloadRequest_;
  NABoolean fixupRequest_;
  NABoolean assignPartRequest_;
  NABoolean workTransRequest_;
  NABoolean releaseTransRequest_;
  NABoolean releaseRequest_;
  NABoolean transactionalRequest_;
};

// ESP state name strings, must match with the enums below
static const char *EspStateEnumName[] = {
    "INITIAL      ",  // A pseudo state, just to get 0 out of the way
    "CREATING     ",  // Create started
    "CREATED_USE  ",  // Created and get assigned first time
    "USED_IDLING  ",  // Done using, start idling
    "IDLE_REUSE   ",  // Reusing an idle one
    "IDLE_TIMEDOUT",  // A short lived state
    "IDLE_DELETE  ",  // Stop idle esps from ODBC
    "DELETED      "   // Gone detected via server died test
};

// -----------------------------------------------------------------------
// Per-process information needed by an ESP manager. The ESP manager
// may or may not be identical to the master executor process.
// -----------------------------------------------------------------------

class ExEspManager {
 public:
  ExEspManager(IpcEnvironment *env, ContextCli *contextCli);
  ~ExEspManager();

  // VO, Genesis solution 10-051125-2802:
  // Added ComVersion_NodeInfo parameters. See ex_root.cpp for explanation.
  ExEspDbEntry *shareEsp(ComDiagsArea **diags, LIST(ExEspDbEntry *) & alreadyAssignedEsps, CollHeap *statementHeap,
                         Statement *statement, const char *clusterName,
                         NABoolean &startedANewEsp,  // returns TRUE, if a new esp was started
                         IpcCpuNum cpuNum, short memoryQuota, int userID, int tenantId,
                         const NAWNodeSet *availableNodes,
                         NABoolean verifyESP,   // need to verify that prior ESP is alive ?
                         NABoolean *verifyCPU,  // input: need to verify each CPU
                                                // output: if create ESP failed -- return TRUE
                         IpcPriority priority, int espLevel, int idleTimeout, int assignTimeWindow,
                         IpcGuardianServer **creatingEsp, NABoolean soloFragment, Int16 esp_multi_fragment,
                         Int16 esp_num_fragments, bool esp_multi_threaded);

  void releaseEsp(ExEspDbEntry *esp, NABoolean verifyEsp, NABoolean badEsp);

  // change ESP priorities. Some ESPs could have idle timedout
  // and could return error 11 (not find). If the error should not
  // be reported to caller, set ignoreNotFound to true
  short changePriorities(IpcPriority priority, NABoolean isDelta, bool ignoreNotFound = false);

  // kill/delete all free esps in cache
  int endSession(ContextCli *context);
  // kill/delete all idle esps in cache
  void stopIdleEsps(ContextCli *context, NABoolean ignoreTimeout = FALSE);

  IpcCpuNum getMaxCpuNum() { return maxCpuNum_; }

  int getNumOfEsps() { return numOfESPs_; }
  int printTrace(int lineno, char *buf);
  static int getALine(void *mine, int lineno, char *buf) { return ((ExEspManager *)mine)->printTrace(lineno, buf); };

  // estimate the ESP count, to avoid allocating a large number of
  // ESPs just to find out that we exceeded the limit
  NABoolean checkESPLimitPerNodeEst(ExFragDir *fragDir, int totalESPLimit, int &numESPsNeeded);

 private:
  ExEspDbEntry *getEspFromCache(LIST(ExEspDbEntry *) & alreadyAssignedEsps,  // multi fragment esp
                                CollHeap *statementHeap, Statement *statement, const char *clusterName,
                                IpcCpuNum cpuNum, short memoryQuota, int user_id, int tenantId, NABoolean verifyESP,
                                int espLevel, int idleTimeout, int assignTimeWindow, int nowaitDepth,
                                NABoolean &espServerError, NABoolean soloFragment, Int16 esp_multi_fragment,
                                Int16 esp_num_fragments, bool esp_multi_threaded);

  IpcCpuNum getRoundRobinCPU(const NAWNodeSet *availableNodes);

  // ESP state tracing >>
  enum EspStateEnum {
    INITIAL,        // A pseudo state, just to get 0 out of the way
    CREATING,       // Create started
    CREATED_USE,    // Created and get assigned first time
    USED_IDLING,    // Done using, start idling
    IDLE_REUSE,     // Reusing an idle one
    IDLE_TIMEDOUT,  // A short lived state
    IDLE_DELETE,    // Stop idle esps from ODBC
    DELETED         // Gone detected via server died test
  };
  struct EspDbEntryTrace {
    ExEspDbEntry *espEntry_;
    int espState_;  // value of enum EspStateEnum
  };
#ifdef _DEBUG
#define NUM_ESP_STATE_TRACE_ENTRIES 32
#else
#define NUM_ESP_STATE_TRACE_ENTRIES 512
#endif
#define MAX_NUM_ESP_STATE_TRACE_ENTRIES 2049
  void addToTrace(ExEspDbEntry *e, int espState) {
    if (++lastEspTraceIndex_ >= maxEspTraceIndex_) lastEspTraceIndex_ = 0;
    espTraceArea_[lastEspTraceIndex_].espEntry_ = e;
    espTraceArea_[lastEspTraceIndex_].espState_ = espState;
  }
  // ESP state tracing <<

  IpcEnvironment *env_;
  ContextCli *context_;
  IpcServerClass *espServerClass_;
  // esp cache: a hash dictionary
  NAHashDictionary<ExEspCacheKey, NAList<ExEspDbEntry *> > *espCache_;
  int numOfESPs_;  // number of ESPs in espCache_

  // round-robin CPU assignment for those ESPs that don't specify one
  IpcCpuNum roundRobinPosition_;
  IpcCpuNum maxCpuNum_;  // max number of CPU
  // ESP state tracing
  EspDbEntryTrace *espTraceArea_;  // Array of EspDbEntryTrace
  UInt32 lastEspTraceIndex_;       // points to last used entry
  UInt32 maxEspTraceIndex_;        // max index can be used
  void *traceRef_;                 // my trace reference in global trace depot
};
// -----------------------------------------------------------------------
// An entry of the global list of ESPs assigned to this process
// $$$$ this may change later
// -----------------------------------------------------------------------
class ExEspDbEntry : public NABasicObject {
  friend class ExEspManager;

 public:
  void release();
  inline ExEspCacheKey *getKey() const { return key_; }
  inline IpcServer *getIpcServer() const { return server_; }
  inline void clearIdleTimestamp() { idleTimestamp_ = 0; }
  void setIdleTimestamp();
  inline bool inUse() { return inUse_; }

 private:
  // private methods

  ExEspDbEntry(CollHeap *heap, IpcServer *server, const char *clusterName, IpcCpuNum cpuNum, int espLevel, int userId,
               int tenantId, bool multiThreaded);
  ~ExEspDbEntry();

  void deleteMe();

  // data members
  ExEspCacheKey *key_;  // hash key for esp cache
  IpcServer *server_;   // the IPC layer object for the server process
  int espLevel_;
  long idleTimestamp_;
  bool inUse_;
  short totalMemoryQuota_;
  int usageCount_;        // how many fragment instances use this process - multi-fragment
  Statement *statement_;  // Allow multiple fragments for just this statement
  bool soloFragment_;
  int tenantId_;
  bool multiThreaded_;
};

// -----------------------------------------------------------------------
// Hash key for esp cache
// -----------------------------------------------------------------------
class ExEspCacheKey : public NABasicObject {
  friend class ExEspManager;

 public:
  ExEspCacheKey(const char *segment, IpcCpuNum cpu, int userId, CollHeap *heap = NULL);
  ~ExEspCacheKey();

  inline NABoolean operator==(const ExEspCacheKey &key) {
    return (!str_cmp_ne(segment_, key.segment_) && cpu_ == key.cpu_ && userId_ == key.userId_);
  }

  int hash() const;

  // useful for supplying hash function to template hash collection creators
  static int hash(const ExEspCacheKey &);

 private:
  CollHeap *heap_;
  char *segment_;
  IpcCpuNum cpu_;
  int userId_;
};

#endif /* ex_frag_rt_h */
