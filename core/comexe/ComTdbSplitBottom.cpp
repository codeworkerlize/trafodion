
/* -*-C++-*-
****************************************************************************
*
* File:         ComTdbSplitBottom.cpp
* Description:  Split bottom tdb (for parallel execution)
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

#include "comexe/ComTdbSplitBottom.h"

#include "comexe/ComTdbCommon.h"
#include "comexe/ComTdbSendBottom.h"
//#include  "ex_esp_frag_dir.h"

// -----------------------------------------------------------------------
// Methods for class ComTdbSplitBottom
// -----------------------------------------------------------------------
ComTdbSplitBottom::ComTdbSplitBottom(ComTdb *child, ComTdbSendBottom *sendTdb, ex_expr *partFunction,
                                     int partNoATPIndex, int partFunctionUsesNarrow, int conversionErrorFlagATPIndex,
                                     int partInputATPIndex, int partInputDataLen, Cardinality estimatedRowCount,
                                     ex_cri_desc *criDescDown, ex_cri_desc *criDescUp, ex_cri_desc *workCriDesc,
                                     NABoolean combineRequests, int topNumESPs, int topNumParts, int bottomNumESPs,
                                     int bottomNumParts, SplitBottomSkewInfo *skewInfo, short minMaxValsWorkAtpIndex,
                                     int minMaxRowLength, int minValStartOffset, ex_expr *minMaxExpr,
                                     ex_expr *minMaxMoveOutExpr)
    : ComTdb(ex_SPLIT_BOTTOM, eye_SPLIT_BOTTOM, estimatedRowCount, criDescDown, criDescUp) {
  child_ = child;
  sendTdb_ = sendTdb;
  partFunction_ = partFunction;
  partFuncUsesNarrow_ = partFunctionUsesNarrow;
  convErrorATPIndex_ = conversionErrorFlagATPIndex;
  partNoATPIndex_ = partNoATPIndex;
  partInputATPIndex_ = partInputATPIndex;
  partInputDataLen_ = partInputDataLen;
  workCriDesc_ = workCriDesc;
  combineRequests_ = combineRequests;
  topNumESPs_ = topNumESPs;
  topNumParts_ = topNumParts;
  bottomNumESPs_ = bottomNumESPs;
  bottomNumParts_ = bottomNumParts;
  splitBottomFlags_ = 0;
  skewInfo_ = skewInfo;
  finalRoundRobin_ = (short)topNumESPs_ - 1;
  initialRoundRobin_ = 0;
  cpuLimit_ = 0;
  cpuLimitCheckFreq_ = 32;
  minMaxValsWorkAtpIndex_ = minMaxValsWorkAtpIndex;
  minMaxRowLength_ = minMaxRowLength;
  minValStartOffset_ = minValStartOffset;
  minMaxExpr_ = minMaxExpr;
  minMaxMoveOutExpr_ = minMaxMoveOutExpr;
  //  setPlanVersion(ComVersion_GetCurrentPlanVersion());
}

int ComTdbSplitBottom::orderedQueueProtocol() const {
  return -1;
}  // these lines won't be covered, obsolete but not in the list yet

void ComTdbSplitBottom::display() const {}  // these lines won't be covered, used by Windows GUI only

const ComTdb *ComTdbSplitBottom::getChild(int pos) const {
  if (pos == 0)
    return child_;
  else if (pos == 1)
    return (ComTdb *)sendTdb_;
  else
    return NULL;
}

int ComTdbSplitBottom::numChildren() const { return 2; }

int ComTdbSplitBottom::numExpressions() const {
  return 3;  // partFunction_,  minMaxExpr_ and minMaxMoveOutExpr_
}

ex_expr *ComTdbSplitBottom::getExpressionNode(int pos) {
  if (pos == 0)
    return partFunction_;
  else if (pos == 1)
    return minMaxExpr_;
  else if (pos == 2)
    return minMaxMoveOutExpr_;
  else
    return NULL;
}

const char *ComTdbSplitBottom::getExpressionName(int pos) const {
  if (pos == 0)
    return "partFunction_";
  else if (pos == 1)
    return "minMaxExpr_";
  else if (pos == 2)
    return "minMaxMoveOutExpr_";
  else
    return NULL;
}

Long ComTdbSplitBottom::pack(void *space) {
  child_.pack(space);
  sendTdb_.pack(space);
  partFunction_.pack(space);
  workCriDesc_.pack(space);
  skewInfo_.pack(space);
  extractProducerInfo_.pack(space);
  minMaxExpr_.pack(space);
  minMaxMoveOutExpr_.pack(space);
  return ComTdb::pack(space);
}

int ComTdbSplitBottom::unpack(void *base, void *reallocator) {
  if (child_.unpack(base, reallocator)) return -1;
  if (sendTdb_.unpack(base, reallocator)) return -1;
  if (partFunction_.unpack(base, reallocator)) return -1;
  if (workCriDesc_.unpack(base, reallocator)) return -1;
  if (skewInfo_.unpack(base, reallocator)) return -1;
  if (extractProducerInfo_.unpack(base, reallocator)) return -1;
  if (minMaxExpr_.unpack(base, reallocator)) return -1;
  if (minMaxMoveOutExpr_.unpack(base, reallocator)) return -1;
  return ComTdb::unpack(base, reallocator);
}

void ComTdbSplitBottom::displayContents(Space *space, int flag) {
  ComTdb::displayContents(space, flag & 0xFFFFFFFE);

  if (flag & 0x00000008) {
    char buf[200];

    str_sprintf(buf, "\nFor ComTdbSplitBottom :");
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    str_sprintf(buf, "splitBottomFlags_ = %x", (int)splitBottomFlags_);
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    str_sprintf(buf, "topNumESPs = %d, bottomNumESPs = %d", topNumESPs_, bottomNumESPs_);
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    str_sprintf(buf, "partInputDataLen = %d, partInputATPIndex = %d, combineRequests = %d", partInputDataLen_,
                partInputATPIndex_, combineRequests_);
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    if (partFunction_) {
      str_sprintf(buf, "partNoATPIndex = %d, partFuncUsesNarrow = %d, convErrorATPIndex = %d", partNoATPIndex_,
                  partFuncUsesNarrow_, convErrorATPIndex_);
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    }

    if (minMaxExpr_) {
      str_sprintf(buf, "minMaxValsWorkAtpIndex_= %d, minMaxRowLength_= %d, minValStartOffset_= %d",
                  minMaxValsWorkAtpIndex_, minMaxRowLength_, minValStartOffset_);
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    }

    if (splitBottomFlags_ & SKEWBUSTER) {
      if (splitBottomFlags_ & SKEW_BROADCAST)
        str_sprintf(buf, "Skewbuster Broadcast is used.");
      else
        str_sprintf(buf,
                    "Skewbuster Uniform Distribution is used, "
                    "starting with consumer number %d, ending with consumer number %d.",
                    initialRoundRobin_, finalRoundRobin_);
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
      str_sprintf(buf, "Number of skewed partitioning keys = %d.", getNumSkewValues());
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

      str_sprintf(buf, "Hash keys of skewed values:");
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

      // Do 3 int64s per line until the final 1 or 2.

      int numTriads = skewInfo_->getNumSkewHashValues() / 3;
      int numLeftovers = skewInfo_->getNumSkewHashValues() % 3;
      int i = 0;

      while (numTriads > 0) {
        str_sprintf(buf, "%23ld %23ld %23ld", skewInfo_->getSkewHashValues()[i], skewInfo_->getSkewHashValues()[i + 1],
                    skewInfo_->getSkewHashValues()[i + 2]);
        space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
        i += 3;
        numTriads -= 1;
      }

      // Now do the final line: 1 or 2 values.
      if (numLeftovers == 2) {
        str_sprintf(buf, "%23ld %23ld", skewInfo_->getSkewHashValues()[i], skewInfo_->getSkewHashValues()[i + 1]);
        space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
      } else if (numLeftovers == 1) {
        str_sprintf(buf, "%23ld", skewInfo_->getSkewHashValues()[i]);
        space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
      }
    }

    if (splitBottomFlags_ & EXTRACT_PRODUCER) {
      str_sprintf(buf, "This is a parallel extract producer");
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

      const char *key = getExtractSecurityKey();
      str_sprintf(buf, "Security key = %s", (key ? key : "(NULL)"));
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    }

    str_sprintf(buf, "Query uses SeaMonster: %s", getQueryUsesSM() ? "yes" : "no");
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    str_sprintf(buf, "Exchange uses SeaMonster: %s", getExchangeUsesSM() ? "yes" : "no");
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
  }

  if (flag & 0x00000001) {
    displayExpression(space, flag);
    displayChildren(space, flag);
  }
}

// -----------------------------------------------------------------------
// Methods for class SplitBottomSkewInfo
// -----------------------------------------------------------------------

Long SplitBottomSkewInfo::pack(void *space) {
  if (skewHashValues_.pack(space)) return -1;
  return NAVersionedObject::pack(space);
}

int SplitBottomSkewInfo::unpack(void *base, void *reallocator) {
  if (skewHashValues_.unpack(base)) return -1;
  return NAVersionedObject::unpack(base, reallocator);
}
