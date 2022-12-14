
/* -*-C++-*-
****************************************************************************
*
* File:         ComTdbTuple.cpp
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

#include "comexe/ComTdbTuple.h"

#include "comexe/ComQueue.h"
#include "comexe/ComTdbCommon.h"
#include "common/str.h"

///////////////////////////////////////////////////////////////////////////////
//
//  TDB procedures
////////////////////////////////////////////////////////////////////////

ComTdbTuple::ComTdbTuple() : ComTdb(ComTdb::ex_TUPLE, eye_TUPLE), tuppIndex_(0), tupleLen_(0) {}

ComTdbTuple::ComTdbTuple(TupleTdbType ttt, Queue *tupleExprList, const int tupleLen, const unsigned short tuppIndex,
                         ex_cri_desc *givenCriDesc, ex_cri_desc *returnedCriDesc, queue_index down, queue_index up,
                         Cardinality estimatedRowCount, int numBuffers, int bufferSize, ex_expr *predExpr)
    : ComTdb(ComTdb::ex_TUPLE, eye_TUPLE, estimatedRowCount, givenCriDesc, returnedCriDesc, down, up, numBuffers,
             bufferSize),
      ttt_(ttt),
      tupleExprList_(tupleExprList),
      tupleLen_(tupleLen),
      tuppIndex_(tuppIndex),
      flags_(0),
      predExpr_(predExpr) {
  switch (ttt) {
    case LEAF_:
      setNodeType(ComTdb::ex_LEAF_TUPLE);
      break;
    case NON_LEAF_:
      setNodeType(ComTdb::ex_NON_LEAF_TUPLE);
      break;
  }
}

ComTdbTuple::~ComTdbTuple() {}

void ComTdbTuple::display() const {};

Long ComTdbTuple::pack(void *space) {
  PackQueueOfNAVersionedObjects(tupleExprList_, space, ex_expr);
  predExpr_.pack(space);

  return ComTdb::pack(space);
}

int ComTdbTuple::unpack(void *base, void *reallocator) {
  UnpackQueueOfNAVersionedObjects(tupleExprList_, base, ex_expr, reallocator);
  if (predExpr_.unpack(base, reallocator)) return -1;

  return ComTdb::unpack(base, reallocator);
}

int ComTdbTuple::numExpressions() const { return tupleExprList_->numEntries() + 1; }

ex_expr *ComTdbTuple::getExpressionNode(int pos) {
  if (pos == 0)
    return predExpr_;
  else
    return (ex_expr *)(tupleExprList_->get(pos - 1));
}

const char *ComTdbTuple::getExpressionName(int pos) const {
  switch (pos) {
    case 0:
      return "predExpr_";
    case 1:
      return "tupleExprList_[0]";
    case 2:
      return "tupleExprList_[1]";
    case 3:
      return "tupleExprList_[2]";
    default:
      return "tupleExprList_[n]";
  }
}

///////////////////////////////////////
// class ExTupleLeafTdb
///////////////////////////////////////
ComTdbTupleLeaf::ComTdbTupleLeaf(Queue *tupleExprList, const int tupleLen, const unsigned short tuppIndex,
                                 ex_expr *predExpr, ex_cri_desc *givenCriDesc, ex_cri_desc *returnedCriDesc,
                                 queue_index down, queue_index up, Cardinality estimatedRowCount, int numBuffers,
                                 int bufferSize)
    : ComTdbTuple(LEAF_, tupleExprList, tupleLen, tuppIndex, givenCriDesc, returnedCriDesc, down, up, estimatedRowCount,
                  numBuffers, bufferSize, predExpr) {}

// This non-leaf tuple operator was not used so far on SQ, see GenRelMisc.cpp

///////////////////////////////////////
// class ExTupleNonLeafTdb
///////////////////////////////////////
ComTdbTupleNonLeaf::ComTdbTupleNonLeaf(Queue *tupleExprList, ComTdb *tdbChild, const int tupleLen,
                                       const unsigned short tuppIndex, ex_cri_desc *givenCriDesc,
                                       ex_cri_desc *returnedCriDesc, queue_index down, queue_index up,
                                       Cardinality estimatedRowCount, int numBuffers, int bufferSize)
    : ComTdbTuple(NON_LEAF_, tupleExprList, tupleLen, tuppIndex, givenCriDesc, returnedCriDesc, down, up,
                  estimatedRowCount, numBuffers, bufferSize),
      tdbChild_(tdbChild) {}

Long ComTdbTupleNonLeaf::pack(void *space) {
  tdbChild_.pack(space);
  return ComTdbTuple::pack(space);
}

int ComTdbTupleNonLeaf::unpack(void *base, void *reallocator) {
  if (tdbChild_.unpack(base, reallocator)) return -1;
  return ComTdbTuple::unpack(base, reallocator);
}

// end of excluding non-leaf operator from coverage checking
