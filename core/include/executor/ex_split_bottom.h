
#ifndef EX_SPLIT_BOTTOM_H
#define EX_SPLIT_BOTTOM_H

#include "common/ExCollections.h"
#include "executor/ex_frag_inst.h"
#include "executor/ex_tcb.h"
#include "comexe/ComTdbSplitBottom.h"

class ex_split_bottom_tcb;
class SplitBottomRequestMessage;
class SplitBottomSavedMessage;

class ex_send_bottom_tdb;
class ex_send_bottom_tcb;
class ExEspStmtGlobals;
class StmtStats;

class ExExeStmtGlobals;
class ExEspFragInstanceDir;

class ex_split_bottom_tdb : public ComTdbSplitBottom {
 public:
  // ---------------------------------------------------------------------
  // Constructor is only called to instantiate an object used for
  // retrieval of the virtual table function pointer of the class while
  // unpacking. An empty constructor is enough.
  // ---------------------------------------------------------------------
  ex_split_bottom_tdb() {}

  virtual ~ex_split_bottom_tdb() {}

  // ---------------------------------------------------------------------
  // Build a TCB for this TDB. Redefined in the Executor project.
  // ---------------------------------------------------------------------
  virtual ex_tcb *build(ex_globals *globals);

  ex_split_bottom_tcb *buildESPTcbTree(ExExeStmtGlobals *glob, ExEspFragInstanceDir *espInstanceDir,
                                       const ExFragKey &myKey, const ExFragKey &parentKey, int myHandle,
                                       int numOfParentInstances);
};

class ex_split_bottom_tcb : public ex_tcb {
  friend class SplitBottomRequestMessage;

 public:
  // Constructor
  ex_split_bottom_tcb(const ex_split_bottom_tdb &split_bottom_tdb, const ex_tcb *child_tcb, ExExeStmtGlobals *glob,
                      ExEspFragInstanceDir *espInstanceDir, const ExFragKey &myKey, const ExFragKey &parentKey,
                      int myHandle, int numOfParentInstances);

  ~ex_split_bottom_tcb();

  inline const ex_split_bottom_tdb &splitBottomTdb() const { return (const ex_split_bottom_tdb &)tdb; }

  void freeResources();  // free resources
  void registerSubtasks();
  void tickleScheduler() { ioHandler_->schedule(); }
  virtual NABoolean needStatsEntry();
  virtual ExOperStats *doAllocateStatsEntry(CollHeap *heap, ComTdb *tdb);

  // Stub to processCancel() used by scheduler.
  static ExWorkProcRetcode sCancel(ex_tcb *tcb) { return ((ex_split_bottom_tcb *)tcb)->processCancel(); }

  // For late cancel.
  ExWorkProcRetcode processInactiveSendTopsBelow(NABoolean &foundOne);

  virtual ex_queue_pair getParentQueue() const;  // there isn't a single one

  ex_queue_pair getSendQueue(CollIndex i) const;
  inline ex_send_bottom_tcb *getSendNode(CollIndex i) const { return sendNodes_[i - firstParentNum_]; }

  ex_send_bottom_tcb *getConsumerSendBottom() const;

  inline ex_expr *partFunction() const { return splitBottomTdb().partFunction_; }
  inline NABoolean partFuncUsesNarrow() const { return (NABoolean)splitBottomTdb().partFuncUsesNarrow_; }
  inline SplitBottomRequestMessage *getMessageStream() { return newMessage_; }
  NABoolean reportErrorToMaster();
  inline NABoolean hasTransaction() const { return workMessageSaved_; }
  void releaseWorkRequest();

  ExWorkProcRetcode work();

  virtual int numChildren() const;
  virtual const ex_tcb *getChild(int pos) const;

  void testAllQueues();

  int getMyFragInstanceHandle() const { return myHandle_; }

  // A mutator method to let this operator know which send bottom is connected
  // to a consumer ESP co-located on this ESP's CPU.  Also, sets the
  // uniformDistributionType_.
  void setLocalSendBottom(CollIndex s);

  // Enforce query CPU limit.
  virtual void cpuLimitExceeded();

  NABoolean needToWaitForMinMaxInputs();
  void cleanupForMinMax();

 private:
  enum SplitBottomWorkState {
    IDLE,
    WAIT_FOR_MINMAX_INPUT,
    WORK_ON_REQUEST,
    WAIT_TO_REPLY,
    WAIT_FOR_MORE_REQUESTORS,
    WAIT_FOR_PARTITION_INPUT_DATA,
    WAIT_FOR_LATE_CANCELS,
    LIMITS_EXCEEDED_ERROR,
    CLEANUP
  };

  void setWorkState(SplitBottomWorkState);
  const char *getWorkState(SplitBottomWorkState);

  void enableHook();

  const ex_tcb *tcbChild_;
  ex_queue_pair qChild_;
  ExEspFragInstanceDir *espInstanceDir_;
  int myHandle_;
  int numOfParentInstances_;
  ARRAY(ex_send_bottom_tcb *) sendNodes_;
  ARRAY(ex_queue *) sendNodesUpQ_;
  ARRAY(ex_queue *) sendNodesDownQ_;
  atp_struct *workAtp_;
  tupp_descriptor partNumTupp_;  // target of part # expr.
  struct {
    int calculatedPartNum_;  // target of part # expr.
    char pad[4];                // alignment
    long partHash_;            // Used for skew buster only,
                                // this is the intermediate
                                // result of the application
                                // of the hash function to the
                                // of the hash function to the
                                // of the hash function to the
                                // partitioning key by the
                                // partitioning key by the
                                // partitioning key by the
                                // partitioning function.
  } partNumInfo_;
  tupp_descriptor convErrTupp_;        // tupp for Narrow flag
  int conversionErrorFlg_;           // side-effected by Narrow
  tupp_descriptor partInputDataTupp_;  // part input data
  SplitBottomWorkState workState_;
  ExSubtask *ioHandler_;  // for part input message
  ExEspStmtGlobals *glob_;

  // this data member indicates the only or the first requestor that we
  // are working for (valid if state is not IDLE).
  CollIndex currRequestor_;

  // This data member points to one send node that has a full up queue.
  // It avoids checking for all nodes repeatedly. We can't proceed until
  // this one node allows us to send more rows. Valid only for state
  // WAIT_TO_REPLY.
  CollIndex fullUpQueue_;

  // This data member points to one send node from which we haven't received
  // a request yet. We cannot proceed until we get a request from that
  // one node. Valid only for state WAIT_FOR_MORE_REQUESTORS.
  CollIndex emptyDownQueue_;

  // State of affairs re partition input data and work requests. We can
  // only do work while we have a work request from the master executor.
  // The work request may or may not have a transaction attached.
  // If <staticInputData_> is set to FALSE, the node can only work while
  // it has an active input data request. Otherwise, the partition input
  // data is assumed to be static. Work requests are also received
  // through the newMessage_ stream. Long-lasting messages are saved
  // away so the incoming message stream is free for other requests.
  // The boolean flags indicate whether we have saved messages and can
  // work on them.
  NABoolean staticInputData_;
  NABoolean workMessageSaved_;
  NABoolean inputDataMessageSaved_;
  NABoolean releaseMessageSaved_;
  NABoolean haveTransaction_;
  NABoolean calcPartNumIsValid_;
  NABoolean broadcastThisRow_;
  SplitBottomRequestMessage *newMessage_;
  SplitBottomSavedMessage *savedDataMessage_;
  SplitBottomSavedMessage *savedWorkMessage_;
  SplitBottomSavedMessage *savedReleaseMessage_;

  // for canceling.
  int numCanceledPartitions_;

  queue_index bugCatcher3041_;  // bugzilla 3041.

  // Skew buster members for repartitioning skewed data.
  // Only used if we as combining requests.

  enum { CoLocated_ = 0, RoundRobin_ = 1 } uniformDistributionType_;

  // Supports CoLocated uniform distribution.
  CollIndex localSendBotttom_;

  // When there is no colocated ESP, uniform distrbution of a skewed value
  // will send to consumers using a round robin.
  CollIndex roundRobinRecipient_;

  // Hash keys of skewed values have been supplied by the optimizer to
  // the generator and are stored in the TDB .  When the TCB is constructed,
  // these keys will be organized into a hash table.  Then, when we
  // repartition results for the consumer ESPs, each partitioning key
  // will be used to probe this hash table to determine if it is possibly
  // a skewed value.
  //
  // The value 1009 is chosen for the number of collision chain headers
  // because it is prime, and close to 1000.
  //
  // Linkage is done with array indexes (not memory pointers), to allow
  // the links to be noncontiguous to the hash keys, which avoids
  // duplicating the potentially large (10K entries) array of hash
  // keys.  So next we define a value indicating no link (analogous to
  // null ptr value, if memory pointers were used).

  // There is a bug in MS Visual C++ that raises a compiler error
  // if we do in-place initialization of static const integral member data
  // (see KB Article ID 241569).  So the workaround is to use an enum:

  enum { numSkewHdrs_ = 1009, noLink_ = -1 };

  // Hash table header. The values stored are array indexes into
  // skewInfo_.skewHashValues_ and the skewLinks_.
  int skewHdrs_[numSkewHdrs_];

  // Collisions for the hash table of skewed keys.
  int *skewLinks_;

  // For table N-M way repartition validation
  int firstParentNum_;  // index of the first ESP

  NABoolean broadcastOneRow_;

  NABoolean *minMaxInputsReceived_;  // A Boolean array to record which parents
                                     // have sent us the min/max inputs already.

  int numOfMinMaxInputsReceived_;  // number of minmax inputs receivered so far,
                                     // which is the number of TRUE elements in
                                     // which is the number of TRUE elements in
                                     // which is the number of TRUE elements in
                                     // minMaxInputsReceived_[].

  int currentMinMaxCheckIndex_;  // The current ith parent SEND_BOTTOM that
                                   // we want to check.

  tupp_descriptor minMaxDataTupp_;  // to accumulate the computed min/max
                                    // values computed so far.

  // An expression used to compute Min and Max values of one or more
  // of the join values coming from the inner side.  Used when hashj
  // is configured to use the min/max optimization.  If present, the
  // expression is evaluated at the same time as the rightHashExpr.
  ex_expr *minMaxExpr_;

  // An expression used to move the final min/max values from minMaxDataTupp_ to
  // down queue.
  ex_expr *minMaxMoveOutExpr_;

  // private methods
 private:
  inline AggrExpr *minMaxExpr() const { return splitBottomTdb().minMaxExpr(); }

  ExWorkProcRetcode workOnWaitMinMaxInputs();
  ExWorkProcRetcode workUp();
  ExWorkProcRetcode processCancel();
  ExWorkProcRetcode workCancelBeforeSent();
  void replyToMessage(IpcMessageStream *sm);
  void acceptNewPartInputValues(NABoolean moreWork);

  // Table N-M way repartition mapping, valid only for hash2 partitions:
  //
  //                     ------------------------------------
  //  numOfTopParts (m)  |    |    |    |    |    |    |    |
  //                     ------------------------------------
  //  feeds                 |  /  \ /  \  /  /  \  /  \  /
  //                     ------------------------------------
  //  numOfBotParts (n)  |      |      |      |      |      |
  //                     ------------------------------------
  //
  // For EPS i where i in [0, n), the number of target ESPs can be found
  // by this function:

  inline int numOfTargetESPs(int numOfTopPs, int numOfBottomPs, int myIndex) {
    ex_assert(myIndex < numOfBottomPs, "Invalid N-M repartition mapping at bottom!");
    return ((myIndex * numOfTopPs + numOfTopPs - 1) / numOfBottomPs - (myIndex * numOfTopPs) / numOfBottomPs + 1);
  }

  // To find the index of my first parent ESP:
  inline int myFirstTargetESP(int numOfTopPs, int numOfBottomPs, int myIndex) {
    return ((myIndex * numOfTopPs) / numOfBottomPs);
  }
};

// -----------------------------------------------------------------------
// A message stream that is used to receive partition input data and
// to signal back that the node is done with the data.
// -----------------------------------------------------------------------
class SplitBottomRequestMessage : public IpcMessageStream {
 public:
  SplitBottomRequestMessage(IpcEnvironment *env, ex_split_bottom_tcb *splitBottom, ExEspInstanceThread *threadInfo);

  virtual void actOnSend(IpcConnection *connection);
  virtual void actOnReceive(IpcConnection *connection);

  // work on received messages
  NABoolean hasRequest() { return hasRequest_; }
  void workOnReceivedMessage();

 private:
  ex_split_bottom_tcb *splitBottom_;
  NABoolean hasRequest_;
  IpcConnection *connectionForRequest_;
};

// -----------------------------------------------------------------------
// Split bottom nodes hold on to certain requests, such as dynamically
// assigned partition input values and transaction work requests. While
// they hold on to those requests they must still be able to receive
// a transaction release request. In order to do this, an extra message
// stream is allocated to hold the message, which frees up the main
// SplitBottomRequestMessage object for further receive operations.
// SplitBottomSavedMessage objects, like SplitBottomRequestMessage
// objects, never directly have their receive() method called directly,
// they receive their messages from other message streams instead.
// However, SplitBottomSavedMessage::send() is called once the split
// bottom node decides to reply to the saved message. The callbacks
// of this node do nothing, all work is done in
// SplitBottomRequestMessage:actOnReceive().
// -----------------------------------------------------------------------

class SplitBottomSavedMessage : public IpcMessageStream {
 public:
  SplitBottomSavedMessage(IpcEnvironment *env, ExEspInstanceThread *threadInfo);

  virtual void actOnSend(IpcConnection *connection);
  virtual void actOnReceive(IpcConnection *connection);
};

#endif /* EX_SPLIT_BOTTOM_H */
