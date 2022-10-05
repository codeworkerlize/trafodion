

#include "comexe/ComTdbExplain.h"
#include "comexe/ComTdbCommon.h"
#include "comexe/ExplainTuple.h"

// Default Constructor.  This is used by the ComTdb::fixupVTblPtr()
// routine which fixes up the virtual function table pointer after
// a node has been unpacked.
ComTdbExplain::ComTdbExplain()
    : ComTdb(ComTdb::ex_EXPLAIN, eye_EXPLAIN),
      tuppIndex_(0)  // Must be initialized since it
                     // is declared const.
{}

// Constructor used by the generator ExplainFunc::codegen to
// generate the explain TDB.
//
// Arguements:
//
//   criDescParentDown - pointer to the criDesc for the parent down queue.
//   A descriptor of the request recieved from the parent.
//
//   criDescParentUp - pointer to the criDesc for the parent up queue.
//   A descriptor of the replies given to the parent.
//
//   queueSizeDown - Size of the down queue. Must be a power of two.
//
//   queueSizeUp - Size of the up queue. Must be a power of two.
//
//   tuppIndex - Index into the ATP of the parent queue which will
//   contain the explainTuple.
//
//   scanPred - An expression to be evaluated on each explainTuple.
//   If not TRUE, the tuple will not be given to the parent, otherwise
//   it will.
//
//   criDescParams - A pointer to the criDesc for the paramsAtp.
//   A description of the paramsAtp which contains all the tuple pointers
//   of the request and the empty paramsTuple in the last entry.
//   Evaluation of the paramsExpr will populate the paramsTuple with
//   the values of the explain function parameters.
//
//   tupleLength - length of the paramsTuple to be allocated.
//
//   paramsExpr - An expression to be evaluated to calculate the values
//   of the parameters to the explain function.  The values will be
//   placed in the paramsTuple.

ComTdbExplain::ComTdbExplain(ex_cri_desc *criDescParentDown, ex_cri_desc *criDescParentUp, queue_index queueSizeDown,
                             queue_index queueSizeUp, const unsigned short tuppIndex, ex_expr *scanPred,
                             ex_cri_desc *criDescParams, int tupleLength, ex_expr *paramsExpr, int numBuffers,
                             int bufferSize)
    : ComTdb(ComTdb::ex_EXPLAIN, eye_EXPLAIN, (Cardinality)0.0, criDescParentDown, criDescParentUp, queueSizeDown,
             queueSizeUp, numBuffers, bufferSize),
      tuppIndex_(tuppIndex),
      scanPred_(scanPred),
      paramsExpr_(paramsExpr),
      criDescParams_(criDescParams),
      tupleLength_(tupleLength) {}

// Used by GUI, does nothing now.
void ComTdbExplain::display() const {}

// The explain TDB has no children
int ComTdbExplain::numChildren() const { return (0); }

// Return the number of expressions held by the explain TDB (2)
// They are enumerated as: 0 - scanPred, 1 - paramsExpr
int ComTdbExplain::numExpressions() const { return (2); }

// Return the expression names of the explain TDB based on some
// enumeration. 0 - scanPred, 1 - paramsExpr
const char *ComTdbExplain::getExpressionName(int expNum) const {
  switch (expNum) {
    case 0:
      return "Scan Pred";
    case 1:
      return "Params Expr";
    default:
      return 0;
  }
}

// Return the expressions of the explain TDB based on some
// enumeration. 0 - scanPred, 1 - paramsExpr
ex_expr *ComTdbExplain::getExpressionNode(int expNum) {
  switch (expNum) {
    case 0:
      return scanPred_;
    case 1:
      return paramsExpr_;
    default:
      return 0;
  }
}

// Pack the explainTdb: Convert all pointers to offsets relative
// to the space object.
Long ComTdbExplain::pack(void *space) {
  scanPred_.pack(space);
  criDescParams_.pack(space);
  paramsExpr_.pack(space);
  return ComTdb::pack(space);
}

// Unpack the explainTdb.: Convert all offsets relative to base
// to pointers
int ComTdbExplain::unpack(void *base, void *reallocator) {
  if (scanPred_.unpack(base, reallocator)) return -1;
  if (criDescParams_.unpack(base, reallocator)) return -1;
  if (paramsExpr_.unpack(base, reallocator)) return -1;
  return ComTdb::unpack(base, reallocator);
}

void ComTdbExplain::displayContents(Space *space, int flag) {
  ComTdb::displayContents(space, flag & 0xFFFFFFFE);

  if (flag & 0x00000008) {
    char buf[1000];

    str_sprintf(buf, "\nFor ComTdbExplan :");
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    str_sprintf(buf, "tupleLength_ = %d, tuppIndex_ = %d", tupleLength_, tuppIndex_);
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    str_sprintf(buf, "offsetStmtPattern = %d, vcLenOffsetStmtPattern = %d, lengthStmtPattern = %d",
                getOffsetStmtPattern(), getVCIndOffsetStmtPattern(), getLengthStmtPattern());
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
  }

  if (flag & 0x00000001) {
    displayExpression(space, flag);
    displayChildren(space, flag);
  }
}
