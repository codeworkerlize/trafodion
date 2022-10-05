

#ifndef COM_SEND_BOTTOM_H
#define COM_SEND_BOTTOM_H

#include "comexe/ComTdb.h"

#include "executor/ex_send_bottom.h"

////////////////////////////////////////////////////////////////////////////
// forward declarations
////////////////////////////////////////////////////////////////////////////
// class ExSendBottomMessageStream;
class ExExeStmtGlobals;
class ExEspFragInstanceDir;
class ExFragKey;

////////////////////////////////////////////////////////////////////////////
// Task Definition Block for send top node
////////////////////////////////////////////////////////////////////////////
class ComTdbSendBottom : public ComTdb {
  friend class ex_send_bottom_tcb;

 public:
  // Constructors
  ComTdbSendBottom() : ComTdb(ex_SEND_BOTTOM, "FAKE") {}
  ComTdbSendBottom(ex_expr *moveOutputValues, queue_index toSplit, queue_index fromSplit, ex_cri_desc *downCriDesc,
                   ex_cri_desc *upCriDesc, ex_cri_desc *workCriDesc, int moveExprTuppIndex, int downRecordLength,
                   int upRecordLength, int requestBufferSize, int numRequestBuffers, int replyBufferSize,
                   int numReplyBuffers, Cardinality estNumRowsRequested, Cardinality estNumRowsReplied);

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(1, getClassVersionID());
    ComTdb::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(ComTdbSendBottom); }

  int orderedQueueProtocol() const;

  Long pack(void *);
  int unpack(void *, void *reallocator);

  void display() const;

  int getDownRecordLength() const { return downRecordLength_; }

  int getUpRecordLength() const { return upRecordLength_; }
  int getRequestBufferSize() const { return requestBufferSize_; }
  int getNumRequestBuffers() const { return numRequestBuffers_; }
  int getReplyBufferSize() const { return replyBufferSize_; }
  int getNumReplyBuffers() const { return numReplyBuffers_; }

  // for GUI
  virtual const ComTdb *getChild(int pos) const;
  virtual int numChildren() const;
  virtual const char *getNodeName() const { return "EX_SEND_BOTTOM"; };
  virtual int numExpressions() const;
  virtual ex_expr *getExpressionNode(int pos);
  virtual const char *getExpressionName(int pos) const;

  virtual ex_send_bottom_tcb *buildInstance(ExExeStmtGlobals *glob, ExEspFragInstanceDir *espInstanceDir,
                                            const ExFragKey &myKey, const ExFragKey &parentKey, int myHandle,
                                            int parentInstanceNum, NABoolean isLocal) {
    return NULL;
  }
  // its executor twin is used, ignore

  // For SHOWPLAN
  virtual void displayContents(Space *space, ULng32 flag);

  // For parallel extract
  NABoolean getExtractProducerFlag() const { return (sendBottomFlags_ & EXTRACT_PRODUCER) ? TRUE : FALSE; }
  void setExtractProducerFlag() { sendBottomFlags_ |= EXTRACT_PRODUCER; }

  // For SeaMonster
  NABoolean getExchangeUsesSM() const { return (sendBottomFlags_ & SNDB_EXCH_USES_SM) ? TRUE : FALSE; }
  void setExchangeUsesSM() { sendBottomFlags_ |= SNDB_EXCH_USES_SM; }

  int getSMTag() const { return smTag_; }
  void setSMTag(int tag) { smTag_ = tag; }

  NABoolean considerBufferDefrag() const { return (sendBottomFlags_ & CONSIDER_BUFFER_DEFRAG) ? TRUE : FALSE; }
  void setConsiderBufferDefrag(NABoolean v) {
    (v ? sendBottomFlags_ |= CONSIDER_BUFFER_DEFRAG : sendBottomFlags_ &= ~CONSIDER_BUFFER_DEFRAG);
  }

 protected:
  enum send_bottom_flags {
    NO_FLAGS = 0x0000,
    EXTRACT_PRODUCER = 0x0001,
    CONSIDER_BUFFER_DEFRAG = 0x0002,
    SNDB_EXCH_USES_SM = 0x0004
  };

  ExCriDescPtr workCriDesc_;  // 00-07

  // move child's outputs to a contiguous buffer to be sent back to parent
  ExExprPtr moveOutputValues_;  // 08-15

  UInt32 sendBottomFlags_;  // 16-19

  // copied input row's pos. in workAtp
  int moveExprTuppIndex_;  // 20-23

  // guidelines for the executor's runtime parameters

  // length of input or output row
  int downRecordLength_;  // 24-27
  int upRecordLength_;    // 28-31

  // recommended size of each buffer
  int requestBufferSize_;  // 32-35

  // total no of buffers to allocate
  int numRequestBuffers_;  // 36-39

  // same for receive buffers
  int replyBufferSize_;  // 40-43

  int numReplyBuffers_;  // 44-47

 private:
  // compiler estimate for # input rows
  Float32 p_estNumRowsRequested_;  // 48-51

  // est. # returned rows per input row
  Float32 p_estNumRowsReplied_;  // 52-55

 protected:
  int smTag_;                       // 56-59
  char fillersComTdbSendBottom_[36];  // 60-95
};

// ---------------------------------------------------------------------
// Template instantiation to produce a 64-bit pointer emulator class
// for ComTdbSendBottom
// ---------------------------------------------------------------------
typedef NAVersionedObjectPtrTempl<ComTdbSendBottom> ComTdbSendBottomPtr;

#endif /* EX_SEND_BOTTOM_H */
