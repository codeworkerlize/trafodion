
/* -*-C++-*-
****************************************************************************
*
* File:         ComTdbPackRows.h
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

#ifndef COMTDBPACKROWS_H
#define COMTDBPACKROWS_H

// External forward declarations
//
#include "comexe/ComTdb.h"

class ComTdbPackRows : public ComTdb {
  friend class ExPackRowsTcb;
  friend class ExPackRowsPrivateState;

 public:
  // Default Constructor.
  ComTdbPackRows();

  // Copy constructor.
  ComTdbPackRows(const ComTdbPackRows *packTdb);

  // Constructor used by the PhyPack::codeGen() in GenRelMisc.cpp.
  ComTdbPackRows(ComTdb *childTdb, ex_expr *packExpr, ex_expr *predExpr, const unsigned short packTuppIndex,
                 int packTuppLen, ex_cri_desc *givenCriDesc, ex_cri_desc *returnedCriDesc, queue_index fromParent,
                 queue_index toParent);

  // Pack the pack TDB for transmission from one process to another.
  Long pack(void *);

  // Unpack the pack TDB after receiving it from another process.
  int unpack(void *, void *reallocator);

  // Don't know what it is for ??
  void display() const;

  // Return a pointer to the child TBD of this Pack TDB.
  inline ComTdb *getChildTdb() { return childTdb_; }

  // We are observing order queue protocol.
  int orderedQueueProtocol() const;

  // Return a pointer to the specifed (by position) child TDB.
  virtual const ComTdb *getChild(int pos) const {
    if (pos == 0) return childTdb_;
    return NULL;
  }

  // Return the number of children for this node.
  virtual int numChildren() const { return 1; }

  // Return the number of expression this node has.
  virtual int numExpressions() const { return 2; }

  // Return the expression by position.
  virtual ex_expr *getExpressionNode(int pos) {
    switch (pos) {
      case 0:
        return packExpr_;
      case 1:
        return predExpr_;
      default:
        break;
    }
    return NULL;
  }

  // Return the name of an expression by position.
  virtual const char *getExpressionName(int pos) const {
    switch (pos) {
      case 0:
        return "packExpr";
      case 1:
        return "predExpr";
      default:
        break;
    }
    return NULL;
  }

  virtual const char *getNodeName() const { return "EX_PACK"; }

 protected:
  // The child of this Pack TDB.
  ComTdbPtr childTdb_;  // 00-07

  // The expression to do the real packing.
  ExExprPtr packExpr_;  // 08-15

  // The selection predicate.
  ExExprPtr predExpr_;  // 16-23

  // The Cri Descriptor given to this node by its parent.
  ExCriDescPtr givenCriDesc_;  // 24-31

  // The Cri Descriptor return to the parent node.
  ExCriDescPtr returnedCriDesc_;  // 32-39

  // The length of the packed record.
  int packTuppLen_;  // 40-43

  // The length of the down queue used to communicate with the parent.
  UInt32 fromParent_;  // 44-47

  // The length of the up queue used to communicate with the parent.
  UInt32 toParent_;  // 48-51

  // The size of pool buffers to allocate.
  UInt32 bufferSize_;  // 52-55

  // The number of buffers to allocate for the pool.
  UInt16 noOfBuffers_;  // 56-57

  // The index of the generated tupp in the ATP.
  UInt16 packTuppIndex_;  // 58-59

  char fillersComTdbPackRows_[36];  // 60-95
};

#endif
