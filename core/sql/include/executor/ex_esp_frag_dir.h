
#ifndef EX_ESP_FRAG_DIR_H
#define EX_ESP_FRAG_DIR_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ex_esp_frag_dir.h
 * Description:  Fragment instance directory in the ESP
 *
 *
 * Created:      1/22/96
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

// -----------------------------------------------------------------------

#include "Ex_esp_msg.h"
#include "cli/Globals.h"
#include "pthread.h"

// -----------------------------------------------------------------------
// Contents of this file
// -----------------------------------------------------------------------

class ExEspFragInstanceDir;
class ExEspInstanceThread;
// -----------------------------------------------------------------------
// Forward references
// -----------------------------------------------------------------------

class ex_split_bottom_tdb;
class ex_split_bottom_tcb;
class ExEspStmtGlobals;
class StatsGlobals;

// -----------------------------------------------------------------------
// Fragment instance directory in the ESP (multiple instances of multiple
// fragments from multiple owners)
// -----------------------------------------------------------------------

const int NumFiTraceElements = 32;

// state name strings, must match with the enums below
static const char *FragmentInstanceStateName[] = {
    "UNUSED         ", "DOWNLOADED     ", "UNPACKED       ", "BUILT          ", "FIXED_UP       ", "READY_INACTIVE ",
    "ACTIVE         ", "WAIT_TO_RELEASE", "RELEASING_WORK ", "RELEASING      ", "GOING_FATAL    ", "BROKEN         "};

class ExEspFragInstanceDir {
  friend class ExEspControlMessage;
  friend class ExEspInstanceThread;

 public:
  enum FragmentInstanceState {
    UNUSED,           // unused entry, no ExEspFragInstance is allocated
    DOWNLOADED,       // fragment downloaded, globals but no tcb allocated
    UNPACKED,         // pointers relocated
    BUILT,            // tcb is now allocated (built)
    FIXED_UP,         // fixup method is now called, files are open
    READY_INACTIVE,   // ready to work but there is currently no work
    ACTIVE,           // tcb has current requests and its transid is active
    WAIT_TO_RELEASE,  // waiting to get replies from msgs to producer ESPs
    RELEASING_WORK,   // need to release work requests, tcb may be active
    RELEASING,        // release request for this instance has been received
    GOING_FATAL,      // frag's scheduler has received WORK_BAD_ERROR
    BROKEN            // error state, don't use statement

    // Possible state transitions (all others should be considered illegal):
    //
    // UNUSED -> DOWNLOADED             load message received, instance is
    //                                  initialized
    // DOWNLOADED -> UNPACKED           TDB tree got unpacked from load message
    // UNPACKED -> BUILT                TCB tree got built after unpack
    // BUILT -> FIXED_UP                first fixup message received
    // READY_INACTIVE -> FIXED_UP       second or later fixup message received
    //
    // FIXED_UP -> READY_INACTIVE       first send top has opened send bottom
    //
    // READY_INACTIVE -> ACTIVE         send bottom receives first request
    //
    // ACTIVE ->WAIT_TO_RELEASE         Master sends release message. Frag
    //                                  dir now switches among frag instances
    //                                  while waiting for replies to msgs
    //                                  to producer ESPs.
    // ACTIVE -> RELEASING_WORK         last send bottom replies to last
    //                                  request
    // WAIT_TO_RELEASE -> RELEASING_WORK all messages from producer ESPs
    //                                  replied
    // RELEASING_WORK -> ACTIVE         work message is released and
    //                                  there are still active requests
    // RELEASING_WORK -> READY_INACTIVE work message is released and
    //                                  there are no more requests
    //
    // any state -> GOING_FATAL         bad things happen
    //
    // (DOWNLOADED, UNPACKED, BUILT,
    //  FIXED_UP, READY_INACTIVE,
    //  RELEASING, GOING_FATAL,
    //  BROKEN) -> RELEASING            release message received
    //
    // RELEASING -> UNUSED              release message processed
    //

  };

  ExEspFragInstanceDir(CliGlobals *cliGlobals, NAHeap *heap, StatsGlobals *statsGlobals, IpcThreadInfo *mainThreadInfo);
  ~ExEspFragInstanceDir();

  inline CollIndex getNumEntries() const { return instances_.entries(); }
  inline int getNumMasters() const { return numMasters_; }
  inline IpcEnvironment *getEnvironment() const { return cliGlobals_->getEnvironment(); }
  inline CliGlobals *getCliGlobals() const { return cliGlobals_; }

  // convert key into handle and vice versa
  int findHandle(const ExFragKey &key) const;
  inline const ExFragKey &findKey(int handle) const { return instances_[handle]->key_; }

  // get the top level tcb for an entry
  inline ex_split_bottom_tcb *getTcb(int h) const { return instances_[h]->localRootTcb_; }
  inline ExEspStmtGlobals *getGlobals(int h) const { return instances_[h]->globals_; }

  ex_split_bottom_tcb *getExtractTop(const char *securityKey);

  int addEntry(ExMsgFragment *msgFragment, IpcConnection *connection, ComDiagsArea &da);
  void fixupEntry(int handle, int numOfParentInstances, ComDiagsArea &da);

  // keep track of the activities that go on in the send bottom nodes,
  // record open operation and arrival and finishing of requests
  void openedSendBottom(int handle);
  void startedSendBottomRequest(int handle);
  void finishedSendBottomRequest(int handle);
  void startedSendBottomCancel(int handle);
  void finishedSendBottomCancel(int handle);
  void startedLateCancelRequest(int handle);
  void finishedLateCancelRequest(int handle);
  int numLateCancelRequests(int handle);

  //  Do the ACTIVE->RELEASING_WORK transition
  void finishedRequest(int handle, NABoolean testAllQueues = FALSE);

  // Do the ACTIVE->WAIT_TO_RELEASE transition
  void hasReleaseRequest(int handle);

  // release the entry and all its dependent entries
  void releaseEntry(int handle);

  // release the entries that lost their parent
  void releaseOrphanEntries();

  // mark in the entry that it no longer has a transid
  void hasTransidReleaseRequest(int handle);

  inline int getNumActiveInstances() { return numActiveInstances_; }
  enum FragmentInstanceState getEntryState(int handle) const { return instances_[handle]->fiState_; }
  ExMsgFragment *getFragment(int handle) const { return instances_[handle]->msgFragment_; }
  ex_split_bottom_tcb *getTopTcb(int handle) const { return instances_[handle]->localRootTcb_; }

  int getNumSendBottomRequests(int handle) { return instances_[handle]->numSendBottomRequests_; }
  int getNumSendBottomCancels(int handle) { return instances_[handle]->numSendBottomCancels_; }

  NABoolean multiThreadedEsp() { return mainThreadInfo_ != NULL; }
  void wakeMainThread() {
    if (mainThreadInfo_) mainThreadInfo_->resume();
  }

  ExEspInstanceThread *getEspInstanceThread(int handle) { return instances_[handle]->espInstanceThread_; }

  // work on all active fragment instances
  void work(long prevWaitTime);

  class ExEspFragInstance {
   public:
    ExFragKey key_;                        // pid,stmthandle,fragid triplet
    int handle_;                           // index of the owning array
    FragmentInstanceState fiState_;        // state of this entry
    ExFragDir::ExFragEntryType fragType_;  // ESP or DP2 fragment
    ExFragKey parentKey_;                  // parent fragment id
    IpcConnection *controlConn_;           // control connection used
    int topNodeOffset_;                  // offset of top tcb in frag
    ExMsgFragment *msgFragment_;           // downloaded fragment buffer
    ex_split_bottom_tdb *localRootTdb_;    // root of this fragment
    ex_split_bottom_tcb *localRootTcb_;    // root of this fragment
    ExEspStmtGlobals *globals_;            // global statement vars
    int numSendBottomRequests_;          // # client work requests
    int numSendBottomCancels_;           // # client cancel requests
    int numLateCancelRequests_;          // # late cancels below
    NABoolean displayInGui_;               // enable executor GUI
    unsigned short mxvOfOriginator_;       // plan version of the plan fragment
    unsigned short planVersion_;           // plan version of the plan fragment
    char *queryId_;
    int queryIdLen_;
    ExEspInstanceThread *espInstanceThread_;
  };

  // Check the plan version of the sent fragment
  int checkPlanVersion(const ExEspFragInstance *entry, ComDiagsArea &da);

  StatsGlobals *getStatsGlobals() { return statsGlobals_; }
  NAHeap *getStatsHeap() { return statsHeap_; }
  pid_t getPid() { return pid_; };
  pid_t getTid() { return tid_; };
  int getCpu() { return cpu_; }
  Long &getSemId() { return semId_; };
  NAHeap *getLocalStatsHeap();

  // A function to establish database user identity for this
  // process. On Linux, ContextCli data members will be updated. On
  // other platforms this call is a no-op.
  void setDatabaseUserID(int userID, const char *userName);

  NABoolean getUserIDEstablished() const { return userIDEstablished_; }

  // For fragment instance trace
  struct FiStateTrace {
    int fragId_;
    FragmentInstanceState fiState_;
    int lineNum_;
  };

  int printALiner(int lineno, char *buf);
  static int getALine(void *mine, int lineno, char *buf) {
    return ((ExEspFragInstanceDir *)mine)->printALiner(lineno, buf);
  }

  void initFiStateTrace();
  FragmentInstanceState setFiState(int fragId, FragmentInstanceState newState, int linenum);

  // for testing and debugging
  void rescheduleAll();  // reschedule all tasks in all instances

  NABoolean allInstanceAreFree();

 private:
  // how many instances need to have their scheduler called?
  std::atomic<int> numActiveInstances_;

  // no instance has a number >= to this
  CollIndex highWaterMark_;

  // an array with pointers to all downloaded fragment instances
  // protect with mutex when referencing in callbacks or when
  // inserting or removint
  ARRAY(ExEspFragInstance *) instances_;

  // how many master ESPs are we talking to?
  int numMasters_;

  CliGlobals *cliGlobals_;

  NAHeap *heap_;

  StatsGlobals *statsGlobals_;
  NAHeap *statsHeap_;
  pid_t pid_;
  pid_t tid_;
  int cpu_;
  Long semId_;
  NAHeap *localStatsHeap_;

  // A boolean to indicate whether this process has already updated
  // the ContextCli user identity. Only meaningful on Linux.
  NABoolean userIDEstablished_;

  IpcThreadInfo *mainThreadInfo_;
  mutable NAMutex mutex_;
  // Fragment instance trace
  FiStateTrace fiStateTrace_[NumFiTraceElements];
  int fiTidx_;
  void *traceRef_;

  // private methods
  ExEspFragInstance *findEntry(const ExFragKey &key);
  void destroyEntry(int handle);

  void traceIdleMemoryUsage();
};

// -----------------------------------------------------------------------
// Message stream to receive requests through the control connection
// (such as load, fixup, ...)
// -----------------------------------------------------------------------

class ExEspControlMessage : public IpcMessageStream {
 public:
  ExEspControlMessage(ExEspFragInstanceDir *fragInstanceDir, IpcEnvironment *ipcEnvironment, CollHeap *heap);
  virtual ~ExEspControlMessage();

  // work on received messages
  NABoolean hasRequest() { return hasRequest_; }
  void workOnReceivedMessage();

 private:
  // private data members

  // fragment instance directory, to find instances referred to in requests
  ExEspFragInstanceDir *fragInstanceDir_;

  CollHeap *heap_;

  // set by the callback, indicates that we should call workOnReceivedMessage()
  NABoolean hasRequest_;
  IpcConnection *connectionForRequest_;

  // store info about the received request (key, handle)
  ExFragKey currKey_;
  int currHandle_;

  // private methods

  // callback functions
  virtual void actOnSend(IpcConnection *connection);
  virtual void actOnSendAllComplete();
  virtual void actOnReceive(IpcConnection *connection);

  // act on individual requests
  void actOnLoadFragmentReq(IpcConnection *connection, ComDiagsArea &da);
  void actOnFixupFragmentReq(ComDiagsArea &da);
  void actOnReleaseFragmentReq(ComDiagsArea &da);
  void actOnReqForSplitBottom(IpcConnection *connection);
  void incReplyMsg(long msgBytes);
};

class ExEspInstanceThread : public IpcThreadInfo {
  friend class ExEspFragInstanceDir;

 public:
  ExEspInstanceThread(NAHeap *heap, ExEspFragInstanceDir *instanceDir,
                      ExEspFragInstanceDir::ExEspFragInstance *instance, CollIndex instanceIndex, long prevWaitTime);
  virtual ~ExEspInstanceThread();
  pid_t start();
  void stop();
  void run();
  void resume();

 private:
  NAHeap *heap_;
  pthread_t threadId_;
  pthread_attr_t thread_attr_;
  ExEspFragInstanceDir *instanceDir_;
  ExEspFragInstanceDir::ExEspFragInstance *instance_;
  CollIndex instanceIndex_;
  long prevWaitTime_;
  NABoolean suspended_;
};

static void *espThreadStart(void *espThreadInstance);

#endif /* EX_ESP_FRAG_DIR_H */
