
#include "comexe/ComTdbHashj.h"

#include "comexe/ComTdbCommon.h"
#include "comexe/HashBufferHeader.h"
#include "exp/ExpSqlTupp.h"  // for sizeof(tupp_descriptor)

///////////////////////////////////////////////////////////////////////////////
//
//  TDB procedures
//
///////////////////////////////////////////////////////////////////////////////

// Constructor
ComTdbHashj::ComTdbHashj() : ComTdb(ComTdb::ex_HASHJ, eye_HASHJ){};

ComTdbHashj::ComTdbHashj(
    ComTdb *leftChildTdb, ComTdb *rightChildTdb, ex_cri_desc *criDescDown, ex_cri_desc *criDescUp,
    ex_expr *rightHashExpr, ex_expr *rightMoveInExpr, ex_expr *rightMoveOutExpr, ex_expr *rightSearchExpr,
    ex_expr *leftHashExpr, ex_expr *leftMoveExpr, ex_expr *leftMoveInExpr, ex_expr *leftMoveOutExpr,
    ex_expr *probeSearchExpr1, ex_expr *probeSearchExpr2, ex_expr *leftJoinExpr,
    // nullInstForLeftJoinExpr(i.e)instantiates the right row.
    ex_expr *nullInstForLeftJoinExpr, ex_expr *beforeJoinPred1, ex_expr *beforeJoinPred2, ex_expr *afterJoinPred1,
    ex_expr *afterJoinPred2, ex_expr *afterJoinPred3, ex_expr *afterJoinPred4, ex_expr *afterJoinPred5,
    ex_expr *checkInputPred, ex_expr *moveInputExpr, int inputValuesLen, short prevInputTuppIndex, int rightRowLength,
    int extRightRowLength, int leftRowLength, int extLeftRowLength, int instRowForLeftJoinLength,
    ex_cri_desc *workCriDesc, short leftRowAtpIndex, short extLeftRowAtpIndex, short rightRowAtpIndex,
    short extRightRowAtpIndex1, short extRightRowAtpIndex2, short hashValueAtpIndex, short instRowForLeftJoinAtpIndex,
    short returnedLeftRowAtpIndex, short returnedRightRowAtpIndex, short returnedInstRowForLeftJoinAtpIndex,
    unsigned short memUsagePercent, short pressureThreshold, short scratchThresholdPct, queue_index down,
    queue_index up, int isSemiJoin, int isLeftJoin, int isAntiSemiJoin, int isUniqueHashJoin, int isNoOverflow,
    int isReuse, int numBuffers, int bufferSize, int hashBufferSize, Cardinality estimatedRowCount,
    Cardinality innerExpectedRows, Cardinality outerExpectedRows, int isRightJoin, ex_expr *rightJoinExpr,
    // nullInstForRightJoinExpr(i.e)instantiates the
    // left row.
    ex_expr *nullInstForRightJoinExpr, short instRowForRightJoinAtpIndex, short returnedInstRowForRightJoinAtpIndex,
    int instRowForRightJoinLength, unsigned short minBuffersToFlush, int numInBatch,
    // unsigned short minBuffersToFlush,
    ex_expr *checkInnerNullExpr, ex_expr *checkOuterNullExpr, short hjGrowthPercent, short minMaxValsAtpIndex,
    int minMaxRowLength, ex_expr *minMaxExpr, ex_cri_desc *leftDownCriDesc, ex_expr *rangeSpecPackingExpr,
    int rangeSpecSanityCheckCycle)
    : ComTdb(ComTdb::ex_HASHJ, eye_HASHJ, estimatedRowCount, criDescDown, criDescUp, down, up, numBuffers, bufferSize),
      leftChildTdb_(leftChildTdb),
      rightChildTdb_(rightChildTdb),
      rightHashExpr_(rightHashExpr),
      rightMoveInExpr_(rightMoveInExpr),
      rightMoveOutExpr_(rightMoveOutExpr),
      rightSearchExpr_(rightSearchExpr),
      leftHashExpr_(leftHashExpr),
      leftMoveExpr_(leftMoveExpr),
      leftMoveInExpr_(leftMoveInExpr),
      leftMoveOutExpr_(leftMoveOutExpr),
      probeSearchExpr1_(probeSearchExpr1),
      probeSearchExpr2_(probeSearchExpr2),
      leftJoinExpr_(leftJoinExpr),
      nullInstForLeftJoinExpr_(nullInstForLeftJoinExpr),
      beforeJoinPred1_(beforeJoinPred1),
      beforeJoinPred2_(beforeJoinPred2),
      afterJoinPred1_(afterJoinPred1),
      afterJoinPred2_(afterJoinPred2),
      afterJoinPred3_(afterJoinPred3),
      afterJoinPred4_(afterJoinPred4),
      checkInputPred_(checkInputPred),
      moveInputExpr_(moveInputExpr),
      inputValuesLen_(inputValuesLen),
      prevInputTuppIndex_(prevInputTuppIndex),
      rightRowLength_(rightRowLength),
      extRightRowLength_(extRightRowLength),
      leftRowLength_(leftRowLength),
      extLeftRowLength_(extLeftRowLength),
      instRowForLeftJoinLength_(instRowForLeftJoinLength),
      workCriDesc_(workCriDesc),
      leftRowAtpIndex_(leftRowAtpIndex),
      extLeftRowAtpIndex_(extLeftRowAtpIndex),
      rightRowAtpIndex_(rightRowAtpIndex),
      extRightRowAtpIndex1_(extRightRowAtpIndex1),
      extRightRowAtpIndex2_(extRightRowAtpIndex2),
      hashValueAtpIndex_(hashValueAtpIndex),
      instRowForLeftJoinAtpIndex_(instRowForLeftJoinAtpIndex),
      returnedLeftRowAtpIndex_(returnedLeftRowAtpIndex),
      returnedRightRowAtpIndex_(returnedRightRowAtpIndex),
      returnedInstRowForLeftJoinAtpIndex_(returnedInstRowForLeftJoinAtpIndex),
      hashBufferSize_(hashBufferSize),
      p_innerExpectedRows_(innerExpectedRows),
      p_outerExpectedRows_(outerExpectedRows),
      memUsagePercent_(memUsagePercent),
      pressureThreshold_(pressureThreshold),
      scratchThresholdPct_(scratchThresholdPct),
      memoryQuotaMB_(0),
      numClusters_(0),
      rightJoinExpr_(rightJoinExpr),
      nullInstForRightJoinExpr_(nullInstForRightJoinExpr),
      instRowForRightJoinAtpIndex_(instRowForRightJoinAtpIndex),
      returnedInstRowForRightJoinAtpIndex_(returnedInstRowForRightJoinAtpIndex),
      instRowForRightJoinLength_(instRowForRightJoinLength),
      minBuffersToFlush_(minBuffersToFlush),
      numInBatch_(numInBatch),
      // minBuffersToFlush_(minBuffersToFlush),
      checkInnerNullExpr_(checkInnerNullExpr),
      checkOuterNullExpr_(checkOuterNullExpr),
      afterJoinPred5_(afterJoinPred5),
      hjMemEstInKBPerNode_(0),
      bmoCitizenshipFactor_(0),
      pMemoryContingencyMB_(0),
      bmoMinMemBeforePressureCheck_(0),
      bmoMaxMemThresholdMB_(0),
      hjGrowthPercent_(hjGrowthPercent),
      minMaxValsAtpIndex_(minMaxValsAtpIndex),
      minMaxRowLength_(minMaxRowLength),
      minMaxExpr_(minMaxExpr),
      leftDownCriDesc_(leftDownCriDesc),
      hjFlags2_(0),
      rangeSpecPackingExpr_(rangeSpecPackingExpr),
      rangeSpecSanityCheckCycle_(rangeSpecSanityCheckCycle) {
  // For now
  hjFlags_ = 0;
  if (isSemiJoin) hjFlags_ |= ComTdbHashj::SEMI_JOIN;

  if (isLeftJoin) hjFlags_ |= ComTdbHashj::LEFT_JOIN;

  if (isRightJoin) hjFlags_ |= ComTdbHashj::RIGHT_JOIN;

  if (isAntiSemiJoin) hjFlags_ |= ComTdbHashj::ANTI_SEMI_JOIN;

  if (isUniqueHashJoin) hjFlags_ |= ComTdbHashj::UNIQUE_HASH_JOIN;

  if (isNoOverflow) hjFlags_ |= ComTdbHashj::NO_OVERFLOW;

  if (isReuse) hjFlags_ |= ComTdbHashj::REUSE;

  // for the estimates, we get at least one result row. Otherwise
  // the calculations for #buckets/#clusters might fail
  if (p_innerExpectedRows_ < (Cardinality)1.0) p_innerExpectedRows_ = (Cardinality)1.0;

  // also, make sure that we don't run into overflow problems.
  // innerExpectedRows_ is an estimate. Therfore it is ok to limit it

  // the c89 doesn't handle the direct comparison of float and
  // UINT_MAX correctly. For now we use 4294967295.0!!!!!!!
  if (p_innerExpectedRows_ > (Cardinality)4294967295.0) p_innerExpectedRows_ = (Cardinality)4294967295.0;
};

void ComTdbHashj::display() const {};

Long ComTdbHashj::pack(void *space) {
  leftChildTdb_.pack(space);
  rightChildTdb_.pack(space);
  rightHashExpr_.pack(space);
  rightMoveInExpr_.pack(space);
  rightMoveOutExpr_.pack(space);
  rightSearchExpr_.pack(space);
  leftHashExpr_.pack(space);
  leftMoveExpr_.pack(space);
  leftMoveInExpr_.pack(space);
  leftMoveOutExpr_.pack(space);
  probeSearchExpr1_.pack(space);
  probeSearchExpr2_.pack(space);
  leftJoinExpr_.pack(space);
  nullInstForLeftJoinExpr_.pack(space);
  rightJoinExpr_.pack(space);
  nullInstForRightJoinExpr_.pack(space);
  beforeJoinPred1_.pack(space);
  beforeJoinPred2_.pack(space);
  afterJoinPred1_.pack(space);
  afterJoinPred2_.pack(space);
  afterJoinPred3_.pack(space);
  afterJoinPred4_.pack(space);
  checkInputPred_.pack(space);
  moveInputExpr_.pack(space);
  workCriDesc_.pack(space);
  checkInnerNullExpr_.pack(space);
  checkOuterNullExpr_.pack(space);
  afterJoinPred5_.pack(space);
  minMaxExpr_.pack(space);
  leftDownCriDesc_.pack(space);
  rangeSpecPackingExpr_.pack(space);
  return ComTdb::pack(space);
}

int ComTdbHashj::unpack(void *base, void *reallocator) {
  if (leftChildTdb_.unpack(base, reallocator)) return -1;
  if (rightChildTdb_.unpack(base, reallocator)) return -1;
  if (rightHashExpr_.unpack(base, reallocator)) return -1;
  if (rightMoveInExpr_.unpack(base, reallocator)) return -1;
  if (rightMoveOutExpr_.unpack(base, reallocator)) return -1;
  if (rightSearchExpr_.unpack(base, reallocator)) return -1;
  if (leftHashExpr_.unpack(base, reallocator)) return -1;
  if (leftMoveExpr_.unpack(base, reallocator)) return -1;
  if (leftMoveInExpr_.unpack(base, reallocator)) return -1;
  if (leftMoveOutExpr_.unpack(base, reallocator)) return -1;
  if (probeSearchExpr1_.unpack(base, reallocator)) return -1;
  if (probeSearchExpr2_.unpack(base, reallocator)) return -1;
  if (leftJoinExpr_.unpack(base, reallocator)) return -1;
  if (nullInstForLeftJoinExpr_.unpack(base, reallocator)) return -1;
  if (rightJoinExpr_.unpack(base, reallocator)) return -1;
  if (nullInstForRightJoinExpr_.unpack(base, reallocator)) return -1;
  if (beforeJoinPred1_.unpack(base, reallocator)) return -1;
  if (beforeJoinPred2_.unpack(base, reallocator)) return -1;
  if (afterJoinPred1_.unpack(base, reallocator)) return -1;
  if (afterJoinPred2_.unpack(base, reallocator)) return -1;
  if (afterJoinPred3_.unpack(base, reallocator)) return -1;
  if (afterJoinPred4_.unpack(base, reallocator)) return -1;
  if (checkInputPred_.unpack(base, reallocator)) return -1;
  if (moveInputExpr_.unpack(base, reallocator)) return -1;
  if (workCriDesc_.unpack(base, reallocator)) return -1;
  if (checkInnerNullExpr_.unpack(base, reallocator)) return -1;
  if (checkOuterNullExpr_.unpack(base, reallocator)) return -1;
  if (afterJoinPred5_.unpack(base, reallocator)) return -1;
  if (minMaxExpr_.unpack(base, reallocator)) return -1;
  if (leftDownCriDesc_.unpack(base, reallocator)) return -1;
  if (rangeSpecPackingExpr_.unpack(base, reallocator)) return -1;
  return ComTdb::unpack(base, reallocator);
}

void ComTdbHashj::displayContents(Space *space, int flag) {
  ComTdb::displayContents(space, flag & 0xFFFFFFFE);

  if (flag & 0x00000008) {
    char buf[100];
    str_sprintf(buf, "\nFor ComTdbHashj :");
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    str_sprintf(buf, "hjFlags = %x, isSemiJoin = %d, isLeftJoin = %d, isRightJoin = %d", hjFlags_, isSemiJoin(),
                isLeftJoin(), isRightJoin());
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    str_sprintf(buf,
                "isAntiSemiJoin = %d, isUniqueHashJoin = %d, "
                "isNoOverflow = %d, isReuse = %d",
                isAntiSemiJoin(), isUniqueHashJoin(), isNoOverflow(), isReuse());
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    if (forceOverflowEvery()) {
      str_sprintf(buf, "forceOverflowEvery = %d ", forceOverflowEvery());
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    }
    if (forceClusterSplitAfterMB() || forceHashLoopAfterNumBuffers()) {
      str_sprintf(buf, "forceClusterSplitAfterMB = %d, forceHashLoopAfterNumBuffers = %d ", forceClusterSplitAfterMB(),
                  forceHashLoopAfterNumBuffers());
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    }

    str_sprintf(buf, "bufferedWrites = %d, logDiagnostics = %d, hashBufferSize = %d", bufferedWrites(),
                logDiagnostics(), hashBufferSize_);
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    str_sprintf(buf, "memoryQuotaMB = %d, numClusters = %d", memoryQuotaMB(), numClusters());
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    str_sprintf(buf, "isReturnRightOrdered = %d, isPossibleMultipleCalls = %d ", isReturnRightOrdered(),
                isPossibleMultipleCalls());
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    str_sprintf(buf, "All or Nothing = %d, delayLeftRequest = %d ", isAntiSemiJoin() && !rightSearchExpr_,
                delayLeftRequest());
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    str_sprintf(buf, "leftRowLength = %d, rightRowLength = %d, instRowForLeftJoinLength = %d ", leftRowLength_,
                rightRowLength_, instRowForLeftJoinLength_);
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    str_sprintf(buf, "extLeftRowLength = %d, extRightRowLength = %d", extLeftRowLength_, extRightRowLength_);
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    str_sprintf(buf, "minMaxValsAtpIndex = %d, minMaxRowLength = %d", minMaxValsAtpIndex_, minMaxRowLength_);
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    str_sprintf(buf, "outerExpectedRows = %f, innerExpectedRows = %f", outerExpectedRows(), innerExpectedRows());
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    str_sprintf(buf, "memUsagePercent = %d, pressureThreshold = %d", memUsagePercent_, pressureThreshold_);
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    str_sprintf(buf, "hjMemEstInKbPerNode = %f, estimateErrorPenalty = %d ", hjMemEstInKBPerNode_, hjGrowthPercent_);
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    str_sprintf(buf, "bmoCitizenshipFactor = %f, PhyMemoryContingencyMB = %d ", bmoCitizenshipFactor_,
                pMemoryContingencyMB_);
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
  }

  if (flag & 0x00000001) {
    displayExpression(space, flag);
    displayChildren(space, flag);
  }
}
