
/* -*-C++-*-
****************************************************************************
*
* File:         ComTdbSplitTop.cpp
* Description:  Split top tdb (for parallel execution)
*
* Created:      5/6/98
* Language:     C++
*
*
*
*
****************************************************************************
*/

#include "comexe/ComTdbSplitTop.h"

#include "comexe/ComTdbCommon.h"
#include "comexe/PartInputDataDesc.h"

// -----------------------------------------------------------------------
// Methods for class ComTdbSplitTop
// -----------------------------------------------------------------------

ComTdbSplitTop::ComTdbSplitTop(ComTdb *child, ex_expr *childInputPartFunction, int inputPartAtpIndex,
                               ex_expr *mergeKeyExpr, int mergeKeyAtpIndex, int mergeKeyLength,
                               ExPartInputDataDesc *partInputDataDesc, int partInputDataAtpIndex, int paPartNoAtpIndex,
                               ex_cri_desc *criDescDown, ex_cri_desc *criDescUp, ex_cri_desc *downCriDesc,
                               ex_cri_desc *workCriDesc, NABoolean bufferedInserts, queue_index fromParent,
                               queue_index toParent, Cardinality estimatedRowCount, int bottomNumParts,
                               int streamTimeout, int sidNumBuffers, int sidBufferSize)
    : ComTdb(ex_SPLIT_TOP, eye_SPLIT_TOP, estimatedRowCount, criDescDown, criDescUp, fromParent, toParent,
             sidNumBuffers, sidBufferSize)

{
  child_ = child;
  childInputPartFunction_ = childInputPartFunction;
  inputPartAtpIndex_ = inputPartAtpIndex;
  mergeKeyExpr_ = mergeKeyExpr;
  mergeKeyAtpIndex_ = mergeKeyAtpIndex;
  mergeKeyLength_ = mergeKeyLength;
  partInputDataDesc_ = partInputDataDesc;
  partInputDataAtpIndex_ = partInputDataAtpIndex;
  paPartNoAtpIndex_ = paPartNoAtpIndex;
  downCriDesc_ = downCriDesc;
  workCriDesc_ = workCriDesc;
  bottomNumParts_ = bottomNumParts;

  splitTopFlags_ = 0x0;

  if (bufferedInserts) splitTopFlags_ |= BUFFERED_INSERTS;

  streamTimeout_ = streamTimeout;
}

int ComTdbSplitTop::orderedQueueProtocol() const {
  return TRUE;
}  // these 3 lines won't be covered, obsolete but not in the list yet

void ComTdbSplitTop::display() const {}  // these 3 lines won't be covered, used by Windows GUI only

const ComTdb *ComTdbSplitTop::getChild(int pos) const { return child_; }

int ComTdbSplitTop::numChildren() const { return 1; }

int ComTdbSplitTop::numExpressions() const { return 2; }

ex_expr *ComTdbSplitTop::getExpressionNode(int pos) {
  if (pos == 0)
    return childInputPartFunction_;
  else if (pos == 1)
    return mergeKeyExpr_;
  else
    return NULL;
}

const char *ComTdbSplitTop::getExpressionName(int pos) const {
  if (pos == 0)
    return "childInputPartFunction_";
  else if (pos == 1)
    return "mergeKeyExpr_";
  else
    return NULL;
}

Long ComTdbSplitTop::pack(void *space) {
  child_.pack(space);
  mergeKeyExpr_.pack(space);
  childInputPartFunction_.pack(space);
  partInputDataDesc_.pack(space);
  downCriDesc_.pack(space);
  workCriDesc_.pack(space);
  extractProducerInfo_.pack(space);
  return ComTdb::pack(space);
}

int ComTdbSplitTop::unpack(void *base, void *reallocator) {
  if (child_.unpack(base, reallocator)) return -1;
  if (childInputPartFunction_.unpack(base, reallocator)) return -1;
  if (mergeKeyExpr_.unpack(base, reallocator)) return -1;
  if (partInputDataDesc_.unpack(base, reallocator)) return -1;
  if (downCriDesc_.unpack(base, reallocator)) return -1;
  if (workCriDesc_.unpack(base, reallocator)) return -1;
  if (extractProducerInfo_.unpack(base, reallocator)) return -1;
  return ComTdb::unpack(base, reallocator);
}

void ComTdbSplitTop::displayContents(Space *space, int flag) {
  ComTdb::displayContents(space, flag & 0xFFFFFFFE);

  if (flag & 0x00000008) {
    char buf[256];

    str_sprintf(buf, "\nFor ComTdbSplitTop :\nFlags = %x ", flags_);
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    str_sprintf(buf, "splitTopFlags_ = %x", (int)splitTopFlags_);
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    str_sprintf(buf, "bottomNumParts_ = %d", (int)bottomNumParts_);
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    if (splitTopFlags_ & EXTRACT_PRODUCER) {
      str_sprintf(buf, "This is a parallel extract producer");
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

      const char *key = getExtractSecurityKey();
      str_sprintf(buf, "Security key = %s", (key ? key : "(NULL)"));
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    }

    str_sprintf(buf, "Exchange uses SeaMonster: %s", getExchangeUsesSM() ? "yes" : "no");
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
  }

  if (flag & 0x00000001) {
    displayExpression(space, flag);
    displayChildren(space, flag);
  }
}
