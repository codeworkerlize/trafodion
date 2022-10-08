
#ifndef ExSequence_h
#define ExSequence_h

/* -*-C++-*-
******************************************************************************
*
* File:         ExSequence.h
* Description:  Class declarations for ExSequence. ExSequence performs
*               sequence functions, like running sums, averages, etc.
* Created:
* Language:     C++
*
*
*
*
******************************************************************************
*/

// External forward declarations
//
class ExSimpleSQLBuffer;

// Task Definition Block
//
#include "executor/cluster.h"
#include "comexe/ComTdbSequence.h"
#include "common/NAMemory.h"
#include "exp/ExpError.h"
#include "export/NABasicObject.h"

// -----------------------------------------------------------------------
// Classes defined in this file
// -----------------------------------------------------------------------
class ExSequenceTdb;
class ExSequenceTcb;
class ExSequencePrivateState;

// -----------------------------------------------------------------------
// Classes referenced in this file
// -----------------------------------------------------------------------
class ex_tcb;
class ex_tdb;
//

// -----------------------------------------------------------------------
// ExSequenceTdb
// -----------------------------------------------------------------------
class ExSequenceTdb : public ComTdbSequence {
 public:
  // ---------------------------------------------------------------------
  // Constructor is only called to instantiate an object used for
  // retrieval of the virtual table function pointer of the class while
  // unpacking. An empty constructor is enough.
  // ---------------------------------------------------------------------
  ExSequenceTdb(){};

  virtual ~ExSequenceTdb(){};

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
  //    If yes, put them in the ComTdbSequence instead.
  //    If no, they should probably belong to someplace else (like TCB).
  //
  // 2. Are the classes those data members belong defined in the executor
  //    project?
  //    If your answer to both questions is yes, you might need to move
  //    the classes to the comexe project.
  // ---------------------------------------------------------------------
};

//
// Task control block
//
class ExSequenceTcb : public ex_tcb {
  friend class ExSequenceTdb;
  friend class ExSequencePrivateState;

 public:
  enum RequestState {
    ExSeq_EMPTY,
    ExSeq_WORKING_READ,
    ExSeq_WORKING_RETURN,
    ExSeq_CANCELLED,
    ExSeq_ERROR,
    ExSeq_DONE,
    ExSeq_OVERFLOW_WRITE,
    ExSeq_OVERFLOW_READ,
    ExSeq_END_OF_PARTITION
  };

  ExSequenceTcb(const ExSequenceTdb &seq_tdb, const ex_tcb &child_tcb, ex_globals *glob);
  ~ExSequenceTcb();

  virtual void registerSubtasks();
  void freeResources();
  short work();

  inline ExSequenceTdb &myTdb() const;

  ex_queue_pair getParentQueue() const { return (qparent_); }

  inline ex_expr *sequenceExpr() const;
  inline ex_expr *returnExpr() const;
  inline ex_expr *postPred() const;
  inline ex_expr *cancelExpr() const;
  inline ex_expr *checkPartitionChangeExpr() const;

  inline ex_expr *moveExpr() const;

  friend char *GetHistoryRow(void *data, int n, NABoolean leading, int winSize, int &);
  friend char *GetHistoryRowOLAP(void *data, int n, NABoolean leading, int winSize, int &);
  friend char *GetHistoryRowFollowingOLAP(void *data, int n, NABoolean leading, int winSize, int &);

  // inline char * GetFirstHistoryRowPtr();
  //
  NABoolean advanceHistoryRow(NABoolean checkMemoryPressure = FALSE);
  // void unAdvanceHistoryRow();
  inline NABoolean isHistoryFull() const;
  inline NABoolean isHistoryEmpty() const;  // if the history buffer is empty (i.e. histRowsToReturn_ == 0)
  inline NABoolean canReturnRows() const;
  inline int numFollowingRows() const;  // number of rows following the current row
  void advanceReturnHistoryRow();
  // inline char * getCurrentRetHistRowPtr()
  inline void updateHistRowsToReturn();
  void initializeHistory();
  void createCluster();

  NABoolean removeOLAPBuffer();
  NABoolean shrinkOLAPBufferList();

  inline NABoolean isOverflowStarted();
  inline NABoolean isUnboundedFollowing();

  inline NABoolean canAllocateOLAPBuffer();
  NABoolean addNewOLAPBuffer(NABoolean checkMemoryPressure = TRUE);

  void updateDiagsArea(ex_queue_entry *centry);
  void updateDiagsArea(ExeErrorCode rc_);
  void updateDiagsArea(ComDiagsArea *da);

  NABoolean getPartitionEnd() const { return partitionEnd_; }
  void setPartitionEnd(NABoolean v) { partitionEnd_ = v; }
  inline int recLen();

  virtual int numChildren() const;
  virtual const ex_tcb *getChild(int pos) const;
  virtual ex_tcb_private_state *allocatePstates(int &numElems,       // inout, desired/actual elements
                                                int &pstateLength);  // out, length of one element
 private:
  const ex_tcb *childTcb_;

  ex_queue_pair qparent_;
  ex_queue_pair qchild_;

  atp_struct *workAtp_;
  ExSubtask *ioEventHandler_;
  HashBuffer *firstOLAPBuffer_;
  HashBuffer *lastOLAPBuffer_;

  HashBuffer *currentOLAPBuffer_;
  HashBuffer *currentRetOLAPBuffer_;

  int currentHistRowInOLAPBuffer_;
  int currentRetHistRowInOLAPBuffer_;

  char *currentHistRowPtr_;
  char *currentRetHistRowPtr_;
  char *lastRow_;

  int minFollowing_;
  int numberHistoryRows_;
  int maxNumberHistoryRows_;
  int histRowsToReturn_;

  NABoolean partitionEnd_;
  NABoolean unboundedFollowing_;

  int allocRowLength_;  // allocated size of a row (original rounded up to 8)
  int maxRowsInOLAPBuffer_;
  int olapBufferSize_;

  int maxNumberOfOLAPBuffers_;
  int numberOfOLAPBuffers_;
  int minNumberOfOLAPBuffers_;

  ClusterDB *clusterDb_;  // used to call ::enoughMemory(), etc.
  Cluster *cluster_;      // used for overflow calls: flush(), read(), etc
  NABoolean OLAPBuffersFlushed_;
  NABoolean memoryPressureDetected_;
  HashBuffer *firstOLAPBufferFromOF_;
  int numberOfOLAPBuffersFromOF_;
  ExeErrorCode rc_;

  int numberOfWinOLAPBuffers_;
  int maxNumberOfRowsReturnedBeforeReadOF_;
  int numberOfRowsReturnedBeforeReadOF_;

  NABoolean overflowEnabled_;

  queue_index processedInputs_;

  CollHeap *heap_;
};  // class ExSequenceTcb

// ExSequenceTcb inline functions
//
inline ex_expr *ExSequenceTcb::sequenceExpr() const { return myTdb().sequenceExpr_; };

inline ex_expr *ExSequenceTcb::returnExpr() const { return myTdb().returnExpr_; };

inline ex_expr *ExSequenceTcb::postPred() const { return myTdb().postPred_; };

inline ex_expr *ExSequenceTcb::cancelExpr() const { return myTdb().cancelExpr_; };

inline ex_expr *ExSequenceTcb::checkPartitionChangeExpr() const { return myTdb().checkPartitionChangeExpr_; };

inline NABoolean ExSequenceTcb::isHistoryFull() const { return (histRowsToReturn_ == maxNumberHistoryRows_); };

inline NABoolean ExSequenceTcb::isHistoryEmpty() const { return (histRowsToReturn_ == 0); };

inline NABoolean ExSequenceTcb::canReturnRows() const { return (numFollowingRows() >= minFollowing_); };

inline int ExSequenceTcb::numFollowingRows() const { return histRowsToReturn_ - 1; };

inline void ExSequenceTcb::updateHistRowsToReturn() { histRowsToReturn_--; }

inline NABoolean ExSequenceTcb::isOverflowStarted() { return cluster_ && cluster_->getState() == Cluster::FLUSHED; }

inline NABoolean ExSequenceTcb::canAllocateOLAPBuffer() { return (numberOfOLAPBuffers_ < maxNumberOfOLAPBuffers_); }

inline NABoolean ExSequenceTcb::isUnboundedFollowing() { return unboundedFollowing_; }
inline int ExSequenceTcb::recLen() {
  // return myTdb().recLen_;
  return allocRowLength_;
};

inline int ExSequenceTcb::numChildren() const { return 1; }

inline const ex_tcb *ExSequenceTcb::getChild(int pos) const {
  ex_assert((pos >= 0), "");
  if (pos == 0)
    return childTcb_;
  else
    return NULL;
}

inline ExSequenceTdb &ExSequenceTcb::myTdb() const { return (ExSequenceTdb &)tdb; };

// class ExSequencePrivateState
//
class ExSequencePrivateState : public ex_tcb_private_state {
  friend class ExSequenceTcb;

 public:
  ExSequencePrivateState();
  ex_tcb_private_state *allocate_new(const ex_tcb *tcb);
  ~ExSequencePrivateState();

 private:
  ExSequenceTcb::RequestState step_;
  queue_index index_;
  long matchCount_;
};  // class ExSequencePrivateState

#endif
