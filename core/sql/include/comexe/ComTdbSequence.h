
#ifndef ComTdbExSequence_h
#define ComTdbExSequence_h

#include "comexe/ComTdb.h"
#include "comexe/ComPackDefs.h"

// Task Definition Block
//
class ComTdbSequence : public ComTdb {
  friend class ExSequenceTcb;
  friend class ExSequencePrivateState;

 public:
  ComTdbSequence();

  ComTdbSequence(ex_expr *sequenceExpr, ex_expr *returnExpr, ex_expr *postPred, ex_expr *cancelExpr, int minFollowing,
                 int reclen, const unsigned short tupp_index, ComTdb *child_tdb, ex_cri_desc *given_cri_desc,
                 ex_cri_desc *returned_cri_desc, queue_index down, queue_index up, int num_buffers,
                 ULng32 buffer_size,      // for SQL buffer (with results)
                 int OLAP_buffer_size,  // for OLAP buffer
                 // olap_number_of_buffers is for testing purposes, can be removed later
                 int max_number_of_OLAP_buffers,  // number of olap buffers
                 int maxHistoryRows, NABoolean unboundedFollowing, NABoolean logDiagnostics,
                 NABoolean possibleMultipleCalls, short scratchThresholdPct, unsigned short memUsagePercent,
                 short pressureThreshold, int maxRowsInOLAPBuffer, int minNumberOfOLAPBuffers,
                 int numberOfWinOLAPBuffers, NABoolean noOverflow, ex_expr *partExpr);

  ~ComTdbSequence();

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(1, getClassVersionID());
    ComTdb::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }

  Long pack(void *);
  int unpack(void *, void *reallocator);

  void display() const;

  // ---------------------------------------------------------------------
  // Used by the internal SHOWPLAN command to get attributes of a TDB.
  // ---------------------------------------------------------------------
  virtual void displayContents(Space *space, ULng32 flag);

  inline ComTdb *getChildTdb();

  int orderedQueueProtocol() const;

  virtual const ComTdb *getChild(int pos) const;
  virtual int numChildren() const { return 1; }
  virtual const char *getNodeName() const { return "EX_SEQUENCE_FUNCTION"; };
  virtual int numExpressions() const { return 5; }
  virtual ex_expr *getExpressionNode(int pos) {
    if (pos == 0)
      return sequenceExpr_;
    else if (pos == 1)
      return returnExpr_;
    else if (pos == 2)
      return postPred_;
    else if (pos == 3)
      return cancelExpr_;
    else if (pos == 4)
      return checkPartitionChangeExpr_;
    else
      return NULL;
  }

  virtual const char *getExpressionName(int pos) const {
    if (pos == 0)
      return "SequenceExpr_";
    else if (pos == 1)
      return "returnExpr_";
    else if (pos == 2)
      return "postPred_";
    else if (pos == 3)
      return "cancelExpr_";
    else if (pos == 4)
      return "checkPartitionChangeExpr_";
    else
      return NULL;
  }

  int getRecLength() const { return recLen_; };

  NABoolean isUnboundedFollowing() const { return (OLAPFlags_ & UNBOUNDED_FOLLOWING) != 0; }

  NABoolean isNoOverflow() const { return (OLAPFlags_ & NO_OVERFLOW); }
  UInt16 forceOverflowEvery() const { return forceOverflowEvery_; }
  void setForceOverflowEvery(UInt16 times) {
    // don't force when "no overflow" is enforced
    forceOverflowEvery_ = (OLAPFlags_ & NO_OVERFLOW) ? 0 : times;
  }
  NABoolean logDiagnostics() const { return (OLAPFlags_ & LOG_DIAGNOSTICS) != 0; }
  // Is this Sequence Operator under the right child of a TSJ ?
  NABoolean isPossibleMultipleCalls() const { return (OLAPFlags_ & POSSIBLE_MULTIPLE_CALLS) != 0; }
  ULng32 memoryQuotaMB() const { return (ULng32)memoryQuotaMB_; }
  void setMemoryQuotaMB(UInt16 v) { memoryQuotaMB_ = v; }

  int getOLAPBufferSize() const { return OLAPBufferSize_; }

  int getMaxRowsInOLAPBuffer() const { return maxRowsInOLAPBuffer_; }
  int getMinNumberOfOLAPBuffers() const { return minNumberOfOLAPBuffers_; }
  int getMaxNumberOfOLAPBuffers() const { return maxNumberOfOLAPBuffers_; }

  int getNumberOfWinOLAPBuffers() const { return numberOfWinOLAPBuffers_; }

  int getMinFollowing() const { return minFollowing_; }

  int scratchIOVectorSize() { return (int)scratchIOVectorSize_; }
  void setScratchIOVectorSize(Int16 v) { scratchIOVectorSize_ = v; }

  void setBmoMinMemBeforePressureCheck(UInt16 m) { bmoMinMemBeforePressureCheck_ = m; }
  UInt16 getBmoMinMemBeforePressureCheck() { return bmoMinMemBeforePressureCheck_; }

  void setBMOMaxMemThresholdMB(UInt16 m) { bmoMaxMemThresholdMB_ = m; }
  UInt16 getBMOMaxMemThresholdMB() { return bmoMaxMemThresholdMB_; }

 protected:
  enum olap_flags {
    UNBOUNDED_FOLLOWING = 0x0001,
    NO_OVERFLOW = 0x0008,
    LOG_DIAGNOSTICS = 0x0040,
    POSSIBLE_MULTIPLE_CALLS = 0x200,
  };

  ExExprPtr sequenceExpr_;               //  00-07
  ExExprPtr postPred_;                   //  08-15
  ExExprPtr cancelExpr_;                 //  16-23
  ComTdbPtr tdbChild_;                   //  24-31
  int recLen_;                         //  32-35
  int maxHistoryRows_;                 //  36-39  //may need to rename to minFixedHistoryRows_???
  const UInt16 tuppIndex_;               //  40-41
  char filler_[2];                       //  42-43
  int minFollowing_;                   //  44-47
  ExExprPtr returnExpr_;                 //  48-55
  ExExprPtr checkPartitionChangeExpr_;   //  56-63
  int OLAPBufferSize_;                 //  64-67
  int maxNumberOfOLAPBuffers_;         //  68-71
  int maxRowsInOLAPBuffer_;            //  72-75
  int minNumberOfOLAPBuffers_;         //  76-79
  int numberOfWinOLAPBuffers_;         //  80-83
  UInt16 OLAPFlags_;                     //  84-85
  UInt16 memoryQuotaMB_;                 //  86-87
  UInt16 scratchThresholdPct_;           //  88-89
  UInt16 forceOverflowEvery_;            //  90-91
  UInt16 memUsagePercent_;               //  92-93
  Int16 pressureThreshold_;              //  94-95
  Int16 scratchIOVectorSize_;            //  96-97
  UInt16 bmoMinMemBeforePressureCheck_;  //  98-99
  UInt16 bmoMaxMemThresholdMB_;          // 100-101

  // ---------------------------------------------------------------------
  // Filler for potential future extensions without changing class size.
  // When a new member is added, size of this filler should be reduced so
  // that the size of the object remains the same (and is modulo 8).
  // ---------------------------------------------------------------------
  char fillers_[2];  // 102-103
};

inline ComTdb *ComTdbSequence::getChildTdb() { return tdbChild_; };

/*****************************************************************************
  Description : Return ComTdb* depending on the position argument.
                  Position 0 means the left most child.
  Comments    :
  History     : Yeogirl Yun                                      8/22/95
                 Initial Revision.
*****************************************************************************/
inline const ComTdb *ComTdbSequence::getChild(int pos) const {
  if (pos == 0)
    return tdbChild_;
  else
    return NULL;
}

#endif
