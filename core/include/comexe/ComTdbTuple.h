
/* -*-C++-*-
****************************************************************************
*
* File:         ComTdbTuple.h
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

#ifndef COM_TUPLE_H
#define COM_TUPLE_H

#include "comexe/ComQueue.h"
#include "comexe/ComTdb.h"

class ComTdbTuple : public ComTdb {
  friend class ExTupleTcb;
  friend class ExTupleLeafTcb;
  friend class ExTupleNonLeafTcb;
  friend class ExTuplePrivateState;

 public:
  enum TupleTdbType { LEAF_, NON_LEAF_ };

  ComTdbTuple();

  ComTdbTuple(TupleTdbType ttt, Queue *tupleExprList, const int tupleLen, const unsigned short tuppIndex,
              ex_cri_desc *givenCriDesc, ex_cri_desc *returnedCriDesc, queue_index down, queue_index up,
              Cardinality estimatedRowCount, int numBuffers, int bufferSize, ex_expr *predExpr = NULL);

  ~ComTdbTuple();

  // only derived class is used, see GenRelMisc.cpp
  int orderedQueueProtocol() const { return -1; };

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(1, getClassVersionID());
    ComTdb::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(ComTdbTuple); }

  virtual Long pack(void *);
  virtual int unpack(void *, void *reallocator);

  void display() const;

  TupleTdbType getTupleType() { return (TupleTdbType)ttt_; }

  virtual const ComTdb *getChild(int pos) const { return NULL; }

  virtual int numChildren() const { return 0; }
  virtual const char *getNodeName() const { return NULL; };
  virtual int numExpressions() const;
  virtual ex_expr *getExpressionNode(int pos);
  virtual const char *getExpressionName(int pos) const;

  // end of excluding the base tuple operator class from coverage checking
 protected:
  QueuePtr tupleExprList_;      // 00-07
  UInt32 tupleLen_;             // 08-11
  UInt32 flags_;                // 12-15
  UInt16 tuppIndex_;            // 16-17
  Int16 ttt_;                   // 18-19
  char filler0ComTdbTuple_[4];  // 20-23  unused

  // expression to evaluate the predicate on the fetched row
  ExExprPtr predExpr_;           // 24-31
  char fillersComTdbTuple_[40];  // 32-71  unused
};

////////////////////////////////////////////
// class ComTdbTupleLeaf
////////////////////////////////////////////
class ComTdbTupleLeaf : public ComTdbTuple {
  friend class ExTupleLeafTcb;
  friend class ExTupleLeafPrivateState;

 public:
  ComTdbTupleLeaf(){};

  ComTdbTupleLeaf(Queue *tupleExprList, const int tupleLen, const unsigned short tuppIndex, ex_expr *pred_expr,
                  ex_cri_desc *givenCriDesc, ex_cri_desc *returnedCriDesc, queue_index down, queue_index up,
                  Cardinality estimatedRowCount, int numBuffers, int bufferSize);

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ComTdbTuple::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(ComTdbTupleLeaf); }

  virtual int numChildren() const { return 0; }
  virtual const char *getNodeName() const { return "EX_TUPLE_LEAF_TDB"; }

 protected:
  char fillersComTdbTupleLeaf_[40];  // 0-39  unused
};

// This non-leaf tuple operator was not used so far on SQ, see GenRelMisc.cpp
////////////////////////////////////////////////
// class ComTdbTupleNonLeaf
////////////////////////////////////////////////
class ComTdbTupleNonLeaf : public ComTdbTuple {
  friend class ExTupleNonLeafTcb;
  friend class ExTupleNonLeafPrivateState;

 public:
  ComTdbTupleNonLeaf(){};

  ComTdbTupleNonLeaf(Queue *tupleExprList, ComTdb *childTdb, const int tupleLen, const unsigned short tuppIndex,
                     ex_cri_desc *givenCriDesc, ex_cri_desc *returnedCriDesc, queue_index down, queue_index up,
                     Cardinality estimatedRowCount, int numBuffers, int bufferSize);

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ComTdbTuple::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(ComTdbTupleNonLeaf); }

  virtual Long pack(void *);
  virtual int unpack(void *, void *reallocator);

  virtual int numChildren() const { return 1; }
  virtual const ComTdb *getChild(int pos) const {
    if (pos == 0)
      return tdbChild_;
    else
      return NULL;
  }

  virtual const char *getNodeName() const { return "EX_TUPLE_NON_LEAF_TDB"; };

 protected:
  ComTdbPtr tdbChild_;             // 00-07
  char fillersComTdbNonLeaf_[40];  // 08-47  unused
};

// end of excluding non-leaf operator from coverage checking

#endif
