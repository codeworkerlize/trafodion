

#include "comexe/ComTdbConnectBy.h"
#include "comexe/ComTdbCommon.h"
#include "comexe/ComQueue.h"
#include "common/str.h"

ComTdbConnectBy::ComTdbConnectBy() : ComTdb(ComTdb::ex_CONNECT_BY, eye_CONNECT_BY) {}

ComTdbConnectBy::ComTdbConnectBy(ex_cri_desc *workCriDesc, ex_cri_desc *givenCriDesc, ex_cri_desc *returnedCriDesc,
                                 queue_index down, queue_index up, Lng32 numBuffers, ULng32 bufferSize,
                                 ComTdb *s_child_tdb, ComTdb *c_child_tdb, UInt32 outputRowLen,
                                 UInt32 pseudoOutputRowLen, ex_expr *leftMoveExpr, ex_expr *rightMoveExpr,
                                 short returnRowAtpIndex, short fixedPseudoColRowAtpIndex, ex_expr *priorPredExpr,
                                 short priorPredAtpIndex, UInt32 priorPredHostVarLen, ex_expr *priorValMoveExpr1,
                                 ex_expr *priorValMoveExpr2, short priorValsValsDownAtpIndex,
                                 ex_cri_desc *rightDownCriDesc, ex_expr *condExpr, short pathExprAtpIndex,
                                 ex_expr *leftPathExpr, ex_expr *rightPathExpr, short pathPseudoColRowAtpIndex,
                                 UInt32 pathOutputRowLen, UInt32 pathItemLength, ex_expr *priorCondExpr)
    : ComTdb(ComTdb::ex_CONNECT_BY, eye_CONNECT_BY, 0, givenCriDesc, returnedCriDesc, down, up, numBuffers, bufferSize),
      workCriDesc_(workCriDesc),
      tdbSChild_(s_child_tdb),
      tdbCChild_(c_child_tdb),
      outputRowLen_(outputRowLen),
      leftMoveExpr_(leftMoveExpr),
      rightMoveExpr_(rightMoveExpr),
      pseudoOutputRowLen_(pseudoOutputRowLen),
      returnRowAtpIndex_(returnRowAtpIndex),
      fixedPseudoColRowAtpIndex_(fixedPseudoColRowAtpIndex),
      priorPredExpr_(priorPredExpr),
      priorPredAtpIndex_(priorPredAtpIndex),
      priorPredHostVarLen_(priorPredHostVarLen),
      priorValMoveExpr1_(priorValMoveExpr1),
      priorValMoveExpr2_(priorValMoveExpr2),
      priorValsValsDownAtpIndex_(priorValsValsDownAtpIndex),
      rightDownCriDesc_(rightDownCriDesc),
      condExpr_(condExpr),
      priorCondExpr_(priorCondExpr),
      pathExprAtpIndex_(pathExprAtpIndex),
      leftPathExpr_(leftPathExpr),
      rightPathExpr_(rightPathExpr),
      pathPseudoColRowAtpIndex_(pathPseudoColRowAtpIndex),
      pathOutputRowLen_(pathOutputRowLen),
      pathItemLength_(pathItemLength) {
  noPrior_ = FALSE;
  nocycle_ = FALSE;
  useCache_ = FALSE;
}

ComTdbConnectBy::~ComTdbConnectBy() {}

void ComTdbConnectBy::display() const {};

Long ComTdbConnectBy::pack(void *space) {
  tdbSChild_.pack(space);
  tdbCChild_.pack(space);
  workCriDesc_.pack(space);
  leftMoveExpr_.pack(space);
  rightMoveExpr_.pack(space);
  priorPredExpr_.pack(space);
  priorValMoveExpr1_.pack(space);
  priorValMoveExpr2_.pack(space);
  rightDownCriDesc_.pack(space);
  condExpr_.pack(space);
  priorCondExpr_.pack(space);
  leftPathExpr_.pack(space);
  rightPathExpr_.pack(space);
  return ComTdb::pack(space);
}

Lng32 ComTdbConnectBy::unpack(void *base, void *reallocator) {
  if (tdbSChild_.unpack(base, reallocator)) return -1;
  if (tdbCChild_.unpack(base, reallocator)) return -1;
  if (workCriDesc_.unpack(base, reallocator)) return -1;
  if (leftMoveExpr_.unpack(base, reallocator)) return -1;
  if (rightMoveExpr_.unpack(base, reallocator)) return -1;
  if (priorPredExpr_.unpack(base, reallocator)) return -1;
  if (priorValMoveExpr1_.unpack(base, reallocator)) return -1;
  if (priorValMoveExpr2_.unpack(base, reallocator)) return -1;
  if (rightDownCriDesc_.unpack(base, reallocator)) return -1;
  if (condExpr_.unpack(base, reallocator)) return -1;
  if (priorCondExpr_.unpack(base, reallocator)) return -1;
  if (leftPathExpr_.unpack(base, reallocator)) return -1;
  if (rightPathExpr_.unpack(base, reallocator)) return -1;

  return ComTdb::unpack(base, reallocator);
}

void ComTdbConnectBy::displayContents(Space *space, ULng32 flag) {
  ComTdb::displayContents(space, flag & 0xFFFFFFFE);

  if (flag & 0x00000008) {
    char buf[100];
    str_sprintf(buf, "\nFor ComTdbConnectBy :");
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
  }

  if (flag & 0x00000001) {
    displayExpression(space, flag);
    displayChildren(space, flag);
  }
}
const char *ComTdbConnectBy::getExpressionName(Int32 pos) const {
  switch (pos) {
    case 0:
      return "leftDataMoveExpr_";
    case 1:
      return "rightDataMoveExpr_";
    case 2:
      return "priorValueMoveExpr_";
    case 3:
      return "priorValueMoveExpr2_";
    case 4:
      return "conditionExpr_";
    case 5:
      return "leftPathMoveExpr_";
    case 6:
      return "rightPathMoveExpr_";
    case 7:
      return "priorCondition_";
    default:
      return "Unknown";
  }
}

ex_expr *ComTdbConnectBy::getExpressionNode(Int32 pos) {
  switch (pos) {
    case 0:
      return leftMoveExpr_;
    case 1:
      return rightMoveExpr_;
    case 2:
      return priorValMoveExpr1_;
    case 3:
      return priorValMoveExpr2_;
    case 4:
      return condExpr_;
    case 5:
      return leftPathExpr_;
    case 6:
      return rightPathExpr_;
    case 7:
      return priorCondExpr_;
    default:
      return NULL;
  }
}
