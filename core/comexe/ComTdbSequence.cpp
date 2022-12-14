
/* -*-C++-*-
******************************************************************************
*
* File:         $File$
* RCS:          $Id$
* Description:
* Created:
* Language:     C++
* Status:       $State$
*
*
*
*
******************************************************************************
*/

#include "comexe/ComTdbSequence.h"

#include "comexe/ComTdbCommon.h"

//////////////////////////////////////////////////////////////////////////////////
//
//  TDB procedures
//
//////////////////////////////////////////////////////////////////////////////////

// Constructor
ComTdbSequence::ComTdbSequence() : ComTdb(ComTdb::ex_SEQUENCE_FUNCTION, eye_SEQUENCE_FUNCTION), tuppIndex_(0) {}

ComTdbSequence::ComTdbSequence(ex_expr *sequenceExpr, ex_expr *returnExpr, ex_expr *postPred, ex_expr *cancelExpr,
                               int minFollowing, int reclen, const unsigned short tupp_index, ComTdb *child_tdb,
                               ex_cri_desc *given_cri_desc, ex_cri_desc *returned_cri_desc, queue_index down,
                               queue_index up, int num_buffers, int buffer_size, int OLAP_buffer_size,
                               int max_number_of_OLAP_buffers, int maxHistoryRows, NABoolean unboundedFollowing,
                               NABoolean logDiagnostics, NABoolean possibleMultipleCalls, short scratchThresholdPct,
                               unsigned short memUsagePercent, short pressureThreshold, int maxRowsInOLAPBuffer,
                               int minNumberOfOLAPBuffers, int numberOfWinOLAPBuffers, NABoolean noOverflow,
                               ex_expr *partExpr)
    : ComTdb(ComTdb::ex_SEQUENCE_FUNCTION, eye_SEQUENCE_FUNCTION, (Cardinality)0.0, given_cri_desc, returned_cri_desc,
             down, up, num_buffers, buffer_size),
      sequenceExpr_(sequenceExpr),
      postPred_(postPred),
      cancelExpr_(cancelExpr),
      minFollowing_(minFollowing),
      tdbChild_(child_tdb),
      recLen_(reclen),
      maxHistoryRows_(maxHistoryRows),
      tuppIndex_(tupp_index),
      returnExpr_(returnExpr),
      checkPartitionChangeExpr_(partExpr),
      OLAPBufferSize_(OLAP_buffer_size),
      maxNumberOfOLAPBuffers_(max_number_of_OLAP_buffers),
      maxRowsInOLAPBuffer_(maxRowsInOLAPBuffer),
      minNumberOfOLAPBuffers_(minNumberOfOLAPBuffers),
      numberOfWinOLAPBuffers_(numberOfWinOLAPBuffers),
      scratchThresholdPct_(scratchThresholdPct),
      memUsagePercent_(memUsagePercent),
      pressureThreshold_(pressureThreshold),
      bmoMinMemBeforePressureCheck_(0),
      bmoMaxMemThresholdMB_(0),
      OLAPFlags_(0) {
  if (noOverflow) {
    OLAPFlags_ |= NO_OVERFLOW;
  }
  if (unboundedFollowing) {
    OLAPFlags_ |= UNBOUNDED_FOLLOWING;
  }
  if (logDiagnostics) {
    OLAPFlags_ |= LOG_DIAGNOSTICS;
  }
  if (possibleMultipleCalls) {
    OLAPFlags_ |= POSSIBLE_MULTIPLE_CALLS;
  }
}

ComTdbSequence::~ComTdbSequence() {}

void ComTdbSequence::display() const {};

void ComTdbSequence::displayContents(Space *space, int flag) {
  ComTdb::displayContents(space, flag & 0xFFFFFFFE);

  if (flag & 0x00000008) {
    char buf[100];
    str_sprintf(buf, "\nFor ComTdbSequence :");
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    str_sprintf(buf, "recLen_ = %d, maxHistoryRows_ = %d, OLAPFlags_ = %x %s", recLen_, maxHistoryRows_, OLAPFlags_,
                isUnboundedFollowing() ? ", UNBOUNDED_FOLLOWING" : "");
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    str_sprintf(buf, "minFollowing_ = %d, OLAPBufferSize_ = %d, maxNumberOfOLAPBuffers_ = %d", minFollowing_,
                OLAPBufferSize_, maxNumberOfOLAPBuffers_);
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    str_sprintf(buf, "maxRowsInOLAPBuffer_ = %d, minNumberOfOLAPBuffers_ = %d", maxRowsInOLAPBuffer_,
                minNumberOfOLAPBuffers_);
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    str_sprintf(buf, "numberOfWinOLAPBuffers_ = %d, %s memoryQuotaMB = %d", numberOfWinOLAPBuffers_,
                logDiagnostics() ? "LOG_DIAGNOSTICS," : "", memoryQuotaMB());
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    str_sprintf(buf, "%s memUsagePercent = %d, pressureThreshold = %d", isNoOverflow() ? "NO_OVERFLOW," : "",
                memUsagePercent_, pressureThreshold_);
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
  }

  if (flag & 0x00000001) {
    displayExpression(space, flag);
    displayChildren(space, flag);
  }
}

int ComTdbSequence::orderedQueueProtocol() const { return -1; }

Long ComTdbSequence::pack(void *space) {
  tdbChild_.pack(space);
  sequenceExpr_.pack(space);
  postPred_.pack(space);
  cancelExpr_.pack(space);
  returnExpr_.pack(space);
  checkPartitionChangeExpr_.pack(space);
  return ComTdb::pack(space);
}

int ComTdbSequence::unpack(void *base, void *reallocator) {
  if (tdbChild_.unpack(base, reallocator)) return -1;
  if (sequenceExpr_.unpack(base, reallocator)) return -1;
  if (postPred_.unpack(base, reallocator)) return -1;
  if (cancelExpr_.unpack(base, reallocator)) return -1;
  if (returnExpr_.unpack(base, reallocator)) return -1;
  if (checkPartitionChangeExpr_.unpack(base, reallocator)) return -1;
  return ComTdb::unpack(base, reallocator);
}
