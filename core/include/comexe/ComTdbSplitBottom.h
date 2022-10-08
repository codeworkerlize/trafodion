

#ifndef COM_SPLIT_BOTTOM_H
#define COM_SPLIT_BOTTOM_H

#include "comexe/ComExtractInfo.h"
#include "comexe/ComTdb.h"
#include "export/NAVersionedObject.h"

class ComTdbSplitBottom;
class ComTdbSendBottom;
class ComExtractProducerInfo;

////////////////////////////////////////////////////////////////////////////
// Class SplitBottomSkewInfo -- helps split bottom by encapsulating the
// skew buster information, including hash values
// of the partitioning keys containing values with very high occurence
// frequency, either in the data stream that this split bottom is
// producing, or in the relation to which this split bottom's data will be
// joined.
////////////////////////////////////////////////////////////////////////////

class SplitBottomSkewInfo : public NAVersionedObject {
  friend class ComTdbSplitBottom;

 public:
  SplitBottomSkewInfo() : NAVersionedObject(-1), numSkewHashValues_(0), skewHashValues_(NULL) {}

  SplitBottomSkewInfo(int numSkewHashValues, long *skewHashValues)
      : numSkewHashValues_(numSkewHashValues), skewHashValues_(skewHashValues) {}

  const long *getSkewHashValues(void) { return skewHashValues_; };

  const int getNumSkewHashValues(void) { return numSkewHashValues_; };

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() { setImageVersionID(0, getClassVersionID()); }

  virtual short getClassSize() { return (short)sizeof(SplitBottomSkewInfo); }

  Long pack(void *);
  int unpack(void *base, void *reallocator);

 private:
  int numSkewHashValues_;                 // 00-03
  char fillersSplitBottomSkewInfo_[4];    // 04-07
  Int64Ptr skewHashValues_;               // 08-15
  char fillersSplitBottomSkewInfo2_[48];  // 16-63
};

// ---------------------------------------------------------------------
// Template instantiation to produce a 64-bit pointer emulator class
// for SplitBottomSkewInfo
// ---------------------------------------------------------------------
typedef NAVersionedObjectPtrTempl<SplitBottomSkewInfo> SplitBottomSkewInfoPtr;

typedef NAVersionedObjectPtrTempl<ComTdbSendBottom> ComTdbSendBottomPtr;

typedef NAVersionedObjectPtrTempl<ComExtractProducerInfo> ComExtractProducerInfoPtr;

////////////////////////////////////////////////////////////////////////////
// Task Definition Block for split bottom node
////////////////////////////////////////////////////////////////////////////
class ComTdbSplitBottom : public ComTdb {
  friend class ex_split_bottom_tcb;

 public:
  // Constructors
  ComTdbSplitBottom() : ComTdb(ex_SPLIT_BOTTOM, "FAKE") {}

  // real constructor, used by generator
  ComTdbSplitBottom(ComTdb *child, ComTdbSendBottom *sendTdb, ex_expr *partFunction, int partNoATPIndex,
                    int partFunctionUsesNarrow, int conversionErrorFlagATPIndex, int partInputATPIndex,
                    int partInputDataLen, Cardinality estimatedRowCount, ex_cri_desc *givenCriDesc,
                    ex_cri_desc *returnedCriDesc, ex_cri_desc *workCriDesc, NABoolean combineRequests, int topNumESPs,
                    int topNumParts, int bottomNumESPs, int bottomNumParts, SplitBottomSkewInfo *skewInfo,
                    short minMaxValsWorkAtpIndex, int minMaxRowLength, int minValStartOffset, ex_expr *minMaxExpr,
                    ex_expr *minMaxMoveOutExpr);

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(1, getClassVersionID());
    ComTdb::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(ComTdbSplitBottom); }

  int orderedQueueProtocol() const;

  inline int getPartInputDataLen() const { return partInputDataLen_; }

  Long pack(void *);
  int unpack(void *, void *reallocator);

  void display() const;

  ComTdbSendBottom *getSendBottomTdb() const { return sendTdb_; }

  inline int getPlanVersion() const { return planVersion_; }

  virtual void setPlanVersion(UInt32 value) { planVersion_ = value; }

  NABoolean useSkewBuster() const { return (splitBottomFlags_ & SKEWBUSTER) != 0; }

  void setUseSkewBuster(NABoolean v) { (v ? splitBottomFlags_ |= SKEWBUSTER : splitBottomFlags_ &= ~SKEWBUSTER); }

  NABoolean doBroadcastSkew() const { return (splitBottomFlags_ & SKEW_BROADCAST) != 0; }

  void setBroadcastSkew(NABoolean v) {
    (v ? splitBottomFlags_ |= SKEW_BROADCAST : splitBottomFlags_ &= ~SKEW_BROADCAST);
  }

  NABoolean getBroadcastOneRow() const { return (splitBottomFlags_ & ONE_ROW_BROADCAST) != 0; }

  void setBroadcastOneRow(NABoolean v) {
    (v ? splitBottomFlags_ |= ONE_ROW_BROADCAST : splitBottomFlags_ &= ~ONE_ROW_BROADCAST);
  }

  NABoolean forceSkewRoundRobin() const { return (splitBottomFlags_ & SKEW_FORCE_RR) != 0; }

  void setForceSkewRoundRobin(NABoolean v) {
    (v ? splitBottomFlags_ |= SKEW_FORCE_RR : splitBottomFlags_ &= ~SKEW_FORCE_RR);
  }

  // For SeaMonster
  // * The "exchange uses SM" flag means this exchange uses SeaMonster
  // * The "query uses SM" flag means SeaMonster is used somewhere in
  //   the query but not necessarily in this fragment
  NABoolean getExchangeUsesSM() const { return (splitBottomFlags_ & SPLB_EXCH_USES_SM) ? TRUE : FALSE; }
  void setExchangeUsesSM() { splitBottomFlags_ |= SPLB_EXCH_USES_SM; }

  NABoolean getQueryUsesSM() const { return (splitBottomFlags_ & SPLB_QUERY_USES_SM) ? TRUE : FALSE; }
  void setQueryUsesSM() { splitBottomFlags_ |= SPLB_QUERY_USES_SM; }

  int getInitialRoundRobin() const { return initialRoundRobin_; }

  void setInitialRoundRobin(int ir) { initialRoundRobin_ = ir; }

  int getFinalRoundRobin() const { return finalRoundRobin_; }

  void setFinalRoundRobin(int fr) { finalRoundRobin_ = fr; }

  // These functions allow split bottom tcb initialization methods to read
  // the hash values for the skewed partitioning key values.
  SplitBottomSkewInfo *getSkewInfo() { return skewInfo_; }
  const int getNumSkewValues(void) { return getSkewInfo()->numSkewHashValues_; }
  // those 3 lines won't be covered, code nowhere used
  long const *hashValueArray(void) { return getSkewInfo()->skewHashValues_; }

  // for GUI
  virtual const ComTdb *getChild(int pos) const;
  virtual int numChildren() const;
  virtual const char *getNodeName() const { return "EX_SPLIT_BOTTOM"; };
  virtual int numExpressions() const;
  virtual ex_expr *getExpressionNode(int pos);
  virtual const char *getExpressionName(int pos) const;

  // for showplan
  virtual void displayContents(Space *space, int flag);

  // For parallel extract
  NABoolean getExtractProducerFlag() const { return (splitBottomFlags_ & EXTRACT_PRODUCER) ? TRUE : FALSE; }
  void setExtractProducerFlag() { splitBottomFlags_ |= EXTRACT_PRODUCER; }

  const char *getExtractSecurityKey() const {
    return (extractProducerInfo_ ? extractProducerInfo_->getSecurityKey() : NULL);
  }

  void setExtractProducerInfo(ComExtractProducerInfo *e) { extractProducerInfo_ = e; }

  NABoolean isMWayRepartition() const { return (splitBottomFlags_ & MWAY_REPARTITION) ? TRUE : FALSE; }
  void setMWayRepartitionFlag() { splitBottomFlags_ |= MWAY_REPARTITION; }

  NABoolean isAnESPAccess() const { return (splitBottomFlags_ & ESP_ACCESS) ? TRUE : FALSE; }
  void setIsAnESPAccess() { splitBottomFlags_ |= ESP_ACCESS; }

  NABoolean getQueryLimitDebug() const { return (splitBottomFlags_ & QUERY_LIMIT_DEBUG) ? TRUE : FALSE; }
  void setQueryLimitDebug() { splitBottomFlags_ |= QUERY_LIMIT_DEBUG; }

  NABoolean useHdfsWriteLock() const { return ((splitBottomFlags_ & USE_HDFS_WRITE_LOCK) != 0); };

  void setUseHdfsWriteLock(NABoolean v) {
    (v ? splitBottomFlags_ |= USE_HDFS_WRITE_LOCK : splitBottomFlags_ &= ~USE_HDFS_WRITE_LOCK);
  }

  enum { NO_ABEND = 0, SIGNED_OVERFLOW, ASSERT, INVALID_MEMORY, SLEEP180, INTERNAL_ERROR, TEST_LOG4CXX };

  int getAbendType(void) const { return abendType_; }

  void setAbendType(int a) { abendType_ = a; }

  void setCpuLimit(long cpuLimit) { cpuLimit_ = cpuLimit; }

  void setCpuLimitCheckFreq(int f) { cpuLimitCheckFreq_ = f; }

  AggrExpr *minMaxExpr() const { return (AggrExpr *)((ex_expr *)minMaxExpr_); }

  Int16 hdfsWriteLockTimeout() { return hdfsWriteLockTimeout_; }
  void setHdfsWriteLockTimeout(Int16 hdfsWriteLockTimeout) { hdfsWriteLockTimeout_ = hdfsWriteLockTimeout; }

 protected:
  enum split_bottom_flags {
    SKEWBUSTER = 0x0001,
    SKEW_BROADCAST = 0x0002,
    SKEW_FORCE_RR = 0x0004,
    EXTRACT_PRODUCER = 0x0008,
    MWAY_REPARTITION = 0x0010,
    ESP_ACCESS = 0x0020,
    QUERY_LIMIT_DEBUG = 0x0040,
    ONE_ROW_BROADCAST = 0x0080,
    SPLB_QUERY_USES_SM = 0x0100,
    SPLB_EXCH_USES_SM = 0x0200,
    USE_HDFS_WRITE_LOCK = 0x0400
  };

  // the combination of a split top / split bottom node maps bottomNumParts_
  // partitions from bottomNumESPs_ processes into topNumParts_ partitions
  // in topNumESPs_ processes.
  int topNumESPs_;      // 00-03
  int topNumParts_;     // 04-07
  int bottomNumESPs_;   // 08-11
  int bottomNumParts_;  // 12-15

  // an expression used to determine to which output partition to send
  // a value that came from our child's up queue (if partFunction_ is not
  // existent then the value is sent to all output partitions that generated
  // the corresponding input queue entry)
  ExExprPtr partFunction_;  // 16-23
  int partNoATPIndex_;      // 24-27

  // does the partitioning function possibly fail with a data conversion
  // error?
  int partFuncUsesNarrow_;  // boolean value; 28-31
  int convErrorATPIndex_;   // 32-35

  // info on where the partition input tupp is stored in the work atp
  int partInputATPIndex_;  // 36-39

  // record descriptions of local ATPs
  ExCriDescPtr workCriDesc_;  // 40-47

  // the partition input values are values sent by some process, identifying
  // the partition that this ESP is working on (this may be a partition number
  // or a begin/end key pair); the partition input values are passed on
  // to our child as an input value
  int partInputDataLen_;  // 48-51

  // The split bottom node can treat each request coming in as a separate
  // job and execute it. Most of the time, however, all requesters send a
  // request synchronously and the split bottom node is supposed to process
  // all those (identical) requests as a single unit.
  int combineRequests_;  // 52-55

  ComTdbPtr child_;  // 56-63

  ComTdbSendBottomPtr sendTdb_;  // 64-71

  UInt16 splitBottomFlags_;  // 72-73

  Int16 initialRoundRobin_;  // 74-75

  UInt32 planVersion_;  // 76-79

  SplitBottomSkewInfoPtr skewInfo_;  // 80-87

  ComExtractProducerInfoPtr extractProducerInfo_;  // 88-95

  int abendType_;  // 96-99

  UInt16 finalRoundRobin_;   // 100-101
  Int16 cpuLimitCheckFreq_;  // 102-103
  long cpuLimit_;            // 104-111

  // Added for support of the MIN/MAX optimization
  // for type-1 HJ.
  // The Min and Max values are computed during
  // readInnerChild phase in HJ, re-computed in the
  // split bottom operator, and passed to the outer
  // scan node before starting the read.
  //
  // The atp index in the workAtp where the minmax
  // tuple resides
  Int16 minMaxValsWorkAtpIndex_;  // 112-113

  // The length of the min max tuple.
  UInt32 minMaxRowLength_;  // 114-117

  // The expression to compute the min max values
  ExExprPtr minMaxExpr_;  // 118-125

  // The expression to move the final min max values
  // out to the down queue
  ExExprPtr minMaxMoveOutExpr_;  // 126-133

  UInt32 minValStartOffset_;          // 134-137
  short hdfsWriteLockTimeout_;        // 138-139
  char fillersComTdbSplitBottom_[6];  // 140-145
};

#endif /* EX_SPLIT_BOTTOM_H */
