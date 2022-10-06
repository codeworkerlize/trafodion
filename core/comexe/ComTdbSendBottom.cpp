
/* -*-C++-*-
****************************************************************************
*
* File:         ComTdbSendBottom.cpp
* Description:  Send bottom node (server part of a point to point connection)
*
* Created:      5/6/98
* Language:     C++
*
*
*
*
****************************************************************************
*/

// -----------------------------------------------------------------------

#include "comexe/ComTdbSendBottom.h"

#include "comexe/ComTdbCommon.h"

// -----------------------------------------------------------------------
// Methods for class ComTdbSendBottom
// -----------------------------------------------------------------------

ComTdbSendBottom::ComTdbSendBottom(ex_expr *moveOutputValues, queue_index downSize, queue_index upSize,
                                   ex_cri_desc *criDescDown, ex_cri_desc *criDescUp, ex_cri_desc *workCriDesc,
                                   int moveExprTuppIndex, int downRecordLength, int upRecordLength,
                                   int requestBufferSize, int numRequestBuffers, int replyBufferSize,
                                   int numReplyBuffers, Cardinality estNumRowsRequested, Cardinality estNumRowsReplied)
    : ComTdb(ex_SEND_BOTTOM, eye_SEND_BOTTOM, estNumRowsReplied, criDescDown, criDescUp, downSize, upSize) {
  moveOutputValues_ = moveOutputValues;
  workCriDesc_ = workCriDesc;
  moveExprTuppIndex_ = moveExprTuppIndex;
  downRecordLength_ = downRecordLength;
  upRecordLength_ = upRecordLength;
  requestBufferSize_ = requestBufferSize;
  numRequestBuffers_ = numRequestBuffers;
  replyBufferSize_ = replyBufferSize;
  numReplyBuffers_ = numReplyBuffers;
  p_estNumRowsRequested_ = estNumRowsRequested;
  p_estNumRowsReplied_ = estNumRowsReplied;
  sendBottomFlags_ = 0;
  smTag_ = 0;
}

int ComTdbSendBottom::orderedQueueProtocol() const { return TRUE; }  // these 3 lines won't be covered, obsolete

void ComTdbSendBottom::display() const {}  // ignore these lines, used by Windows GUI only

const ComTdb *ComTdbSendBottom::getChild(int pos) const {
  return NULL;
}  // these lines won't be covered, it's never called as it has no child opr

int ComTdbSendBottom::numChildren() const { return 0; }

int ComTdbSendBottom::numExpressions() const { return 1; }

ex_expr *ComTdbSendBottom::getExpressionNode(int pos) {
  if (pos == 0) return moveOutputValues_;
  return NULL;
}

const char *ComTdbSendBottom::getExpressionName(int pos) const {
  if (pos == 0) return "moveOutputValues_";
  return NULL;
}

Long ComTdbSendBottom::pack(void *space) {
  moveOutputValues_.pack(space);
  workCriDesc_.pack(space);
  return ComTdb::pack(space);
}

int ComTdbSendBottom::unpack(void *base, void *reallocator) {
  if (moveOutputValues_.unpack(base, reallocator)) return -1;
  if (workCriDesc_.unpack(base, reallocator)) return -1;
  return ComTdb::unpack(base, reallocator);
}

void ComTdbSendBottom::displayContents(Space *space, int flag) {
  ComTdb::displayContents(space, flag & 0xFFFFFFFE);

  if (flag & 0x00000008) {
    char buf[256];

    str_sprintf(buf, "\nFor ComTdbSendBottom :");
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    str_sprintf(buf, "sendBottomFlags_ = %x", (int)sendBottomFlags_);
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    str_sprintf(buf, "downRecordLength_ = %d, upRecordLength_ = %d", downRecordLength_, upRecordLength_);

    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    str_sprintf(buf, "requestBufferSize_ = %d, replyBufferSize_ = %d", requestBufferSize_, replyBufferSize_);
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    str_sprintf(buf, "numRequestBuffers_ = %d, numReplyBuffers_ = %d", numRequestBuffers_, numReplyBuffers_);
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    str_sprintf(buf, "Exchange uses SeaMonster: %s", getExchangeUsesSM() ? "yes" : "no");
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
  }

  if (flag & 0x00000001) {
    displayExpression(space, flag);
    displayChildren(space, flag);
  }
}
