
/* -*-C++-*-
****************************************************************************
*
* File:         ComTdbSendTop.cpp
* Description:  Send top node (client part of client-server connection)
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

#include "comexe/ComTdbSendTop.h"

#include "comexe/ComTdbCommon.h"
#include "executor/sql_buffer.h"
#include "executor/sql_buffer_size.h"  // for SqlBuffer::neededSize().

// -----------------------------------------------------------------------
// Methods for class ComTdbSendTop
// -----------------------------------------------------------------------

ComTdbSendTop::ComTdbSendTop(ExFragId childFragId, ex_expr *moveInputValues, ex_cri_desc *criDescDown,
                             ex_cri_desc *criDescUp, ex_cri_desc *downRecordCriDesc, ex_cri_desc *upRecordCriDesc,
                             ex_cri_desc *workCriDesc, int moveExprTuppIndex, queue_index fromParent,
                             queue_index toParent, int downRecordLength, int upRecordLength, int sendBufferSize,
                             int numSendBuffers, int recvBufferSize, int numRecvBuffers, Cardinality estNumRowsSent,
                             Cardinality estNumRowsRecvd, NABoolean logDiagnostics)
    : ComTdb(ex_SEND_TOP, eye_SEND_TOP, estNumRowsRecvd, criDescDown, criDescUp, fromParent, toParent) {
  childFragId_ = childFragId;
  moveInputValues_ = moveInputValues;
  downRecordCriDesc_ = downRecordCriDesc;
  upRecordCriDesc_ = upRecordCriDesc;
  workCriDesc_ = workCriDesc;
  moveExprTuppIndex_ = moveExprTuppIndex;
  downRecordLength_ = downRecordLength;

  upRecordLength_ = upRecordLength;
  sendBufferSize_ = sendBufferSize;
  numSendBuffers_ = numSendBuffers;
  recvBufferSize_ = recvBufferSize;
  numRecvBuffers_ = numRecvBuffers;
  p_estNumRowsSent_ = estNumRowsSent;
  p_estNumRowsRecvd_ = estNumRowsRecvd;
  sendTopFlags_ = logDiagnostics ? LOG_DIAGNOSTICS : NO_FLAGS;
  smTag_ = 0;
}

int ComTdbSendTop::orderedQueueProtocol() const {
  return TRUE;
}  // these lines won't be covered, obsolete but not in the list yet

int ComTdbSendTop::minSendBufferSize(int downRecLen, int numRecs) {
  // start with the regular size it would take to pack the records
  // into an SqlBuffer
  int recSpace = SqlBufferNeededSize(numRecs, downRecLen, SqlBufferHeader::DENSE_);

  // now add the needed space for the ExpControlInfo struct that goes
  // along with each record
  int delta = SqlBufferNeededSize(2, sizeof(ControlInfo), SqlBufferHeader::DENSE_) -
              SqlBufferNeededSize(1, sizeof(ControlInfo), SqlBufferHeader::DENSE_);

  return recSpace + numRecs * delta;
}

int ComTdbSendTop::minReceiveBufferSize(int upRecLen, int numRecs) {
  // right now send and receive buffers work the same
  return minSendBufferSize(upRecLen, numRecs);
}

void ComTdbSendTop::display() const {}  // these 3 lines won't be covered, used by Windows GUI only

const ComTdb *ComTdbSendTop::getChild(int /*pos*/) const {
  return NULL;
}  // these 4 lines won't be covered, used by Windows GUI only

const ComTdb *ComTdbSendTop::getChildForGUI(int /*pos*/, int base, void *frag_dir) const {
  ExFragDir *fragDir = (ExFragDir *)frag_dir;

  return (ComTdb *)((long)base + fragDir->getGlobalOffset(childFragId_) + fragDir->getTopNodeOffset(childFragId_));
}

int ComTdbSendTop::numChildren() const { return 1; }

int ComTdbSendTop::numExpressions() const { return 1; }

ex_expr *ComTdbSendTop::getExpressionNode(int pos) {
  if (pos == 0)
    return moveInputValues_;
  else
    return NULL;
}

const char *ComTdbSendTop::getExpressionName(int pos) const {
  if (pos == 0)
    return "moveInputValues_";
  else
    return NULL;
}

Long ComTdbSendTop::pack(void *space) {
  moveInputValues_.pack(space);
  downRecordCriDesc_.pack(space);
  upRecordCriDesc_.pack(space);
  workCriDesc_.pack(space);
  extractConsumerInfo_.pack(space);
  return ComTdb::pack(space);
}

int ComTdbSendTop::unpack(void *base, void *reallocator) {
  if (moveInputValues_.unpack(base, reallocator)) return -1;
  if (downRecordCriDesc_.unpack(base, reallocator)) return -1;
  if (upRecordCriDesc_.unpack(base, reallocator)) return -1;
  if (workCriDesc_.unpack(base, reallocator)) return -1;
  if (extractConsumerInfo_.unpack(base, reallocator)) return -1;
  return ComTdb::unpack(base, reallocator);
}

void ComTdbSendTop::displayContents(Space *space, int flag) {
  ComTdb::displayContents(space, flag & 0xFFFFFFFE);

  if (flag & 0x00000008) {
    char buf[256];

    str_sprintf(buf, "\nFor ComTdbSendTop :");
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    str_sprintf(buf, "childFragId_ = %d, sendTopFlags_ = %x", (int)childFragId_, (int)sendTopFlags_);
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    str_sprintf(buf, "downRecordLength_ = %d, upRecordLength_ = %d", downRecordLength_, upRecordLength_);
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    str_sprintf(buf, "sendBufferSize_ = %d, recvBufferSize_ = %d", sendBufferSize_, recvBufferSize_);
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    if (sendTopFlags_ & EXTRACT_CONSUMER) {
      str_sprintf(buf, "This is a parallel extract consumer");
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

      const char *esp = getExtractEsp();
      str_sprintf(buf, "Extract ESP = %s", (esp ? esp : "(NULL)"));
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
