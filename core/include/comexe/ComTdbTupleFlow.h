
/* -*-C++-*-
****************************************************************************
*
* File:         ComTdbTupleFlow.h
* Description:
*
* Created:      5/6/98
* Language:     C++
*
*
*
*
****************************************************************************
*/

#ifndef COM_TUPLE_FLOW_H
#define COM_TUPLE_FLOW_H

#include "comexe/ComTdb.h"

class ComTdbTupleFlow : public ComTdb {
  friend class ExTupleFlowTcb;
  friend class ExTupleFlowPrivateState;

  enum TFlowFlags {
    VSBB_INSERT = 0x0001,
    ROWSET_ITERATOR = 0x0002,
    USER_SIDETREE_INSERT = 0x0004,
    SEND_EOD_TO_TGT = 0x0008
  };

 protected:
  ComTdbPtr tdbSrc_;  // 00-07
  ComTdbPtr tdbTgt_;  // 08-15

  // if present, then used to compute the 'row' that
  // has to be sent to right (target) child.
  ExExprPtr tgtExpr_;  // 16-23

  ExCriDescPtr workCriDesc_;  // 24-31

  UInt32 flags_;  // 32-35

  char fillersComTdbTupleFlow_[36];  // 36-71

  inline NABoolean vsbbInsertOn() {
    if (flags_ & VSBB_INSERT)
      return TRUE;
    else
      return FALSE;
  }

  inline NABoolean isRowsetIterator() {
    if (flags_ & ROWSET_ITERATOR)
      return TRUE;
    else
      return FALSE;
  }

 public:
  ComTdbTupleFlow() : ComTdb(ComTdb::ex_TUPLE_FLOW, eye_TUPLE_FLOW) {}

  ComTdbTupleFlow(ComTdb *tdb_src, ComTdb *tdb_tgt, ex_cri_desc *given_cri_desc, ex_cri_desc *returned_cri_desc,
                  ex_expr *tgt_expr, ex_cri_desc *work_cri_desc, queue_index down, queue_index up,
                  Cardinality estimatedRowCount, int num_buffers, int buffer_size, NABoolean vsbbInsert,
                  NABoolean rowsetIterator, NABoolean tolerateNonFatalError);

  virtual ~ComTdbTupleFlow();

  NABoolean userSidetreeInsert() { return ((flags_ & USER_SIDETREE_INSERT) != 0); }
  void setUserSidetreeInsert(NABoolean v) { (v ? flags_ |= USER_SIDETREE_INSERT : flags_ &= ~USER_SIDETREE_INSERT); }

  NABoolean sendEODtoTgt() { return ((flags_ & SEND_EOD_TO_TGT) != 0); }
  void setSendEODtoTgt(NABoolean v) { (v ? flags_ |= SEND_EOD_TO_TGT : flags_ &= ~SEND_EOD_TO_TGT); }

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(1, getClassVersionID());
    ComTdb::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(ComTdbTupleFlow); }
  Long pack(void *);
  int unpack(void *, void *reallocator);

  void display() const {};

  int orderedQueueProtocol() const {
    return -1;  // return true
  };

  inline void setSrcTdb(ComTdb *src) { tdbSrc_ = src; };
  inline void setTgtTdb(ComTdb *tgt) { tdbTgt_ = tgt; };

  virtual const ComTdb *getChild(int pos) const;
  virtual int numChildren() const { return 2; }
  virtual const char *getNodeName() const { return "EX_TUPLE_FLOW"; };
  virtual int numExpressions() const { return 1; }
  virtual ex_expr *getExpressionNode(int pos);
  virtual const char *getExpressionName(int pos) const;

  // ---------------------------------------------------------------------
  // Used by the internal SHOWPLAN command to get attributes of a TDB.
  // ---------------------------------------------------------------------
  virtual void displayContents(Space *space, int flag);
};

#endif
