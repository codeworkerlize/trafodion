
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         exp_dp2_expr.cpp
 * Description:  Expressions that are evaluated by DP2 outside of the
 *               EID code. These expressions have to be completely
 *               self-sufficient.
 * Created:      7/10/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "exp/exp_dp2_expr.h"

#include "common/Platform.h"
#include "exp/ExpAtp.h"
#include "exp/ExpSqlTupp.h"
#include "exp/exp_stdh.h"

// -----------------------------------------------------------------------
// This static method computes the space to be reserved at compile-time
// following the object image of ExpDP2Expr for the workAtp generated at
// run-time. It reserves twice the amount of space necessary due to
// versioning concern.
// -----------------------------------------------------------------------
int ExpDP2Expr::spaceNeededForWorkAtp() {
  return ((criDesc_ != (ExCriDescPtr)NULL)
              ? 2 * (sizeof(atp_struct) + criDesc_->noTuples() * (sizeof(tupp) + sizeof(tupp_descriptor)))
              : 0);
}

ExpDP2Expr::ExpDP2Expr(ex_expr_base *expr, ex_cri_desc *work_cri_desc, Space *space, short allocateAtpAtRunTime)
    : expr_(expr), criDesc_(work_cri_desc), NAVersionedObject(-1), pCodeMode_(0) {
  if (allocateAtpAtRunTime) {
    // workAtp_ only constructed at run time. At compile-time, we only reserve
    // space for it.
    workAtpSpace_ = spaceNeededForWorkAtp();
    workAtp_ = (atp_struct *)space->allocateAlignedSpace(workAtpSpace_);
  } else {
    workAtpSpace_ = -1;
    workAtp_ = allocateAtp(work_cri_desc, space);

    // allocate tuple descriptors for all the tupps (except constant and temp)
    // in workAtp_.
    for (int i = 2; i < work_cri_desc->noTuples(); i++) {
      workAtp_->getTupp(i) = (tupp_descriptor *)(new (space) tupp_descriptor);
    }
  }

  // allocate space for temps, if needed.
  if ((expr) && (expr->getTempsLength() > 0) && (!expr->getTempsArea())) {
    ((ex_expr *)expr)->setTempsArea(new (space) char[expr->getTempsLength()]);
  }
}

ExpDP2Expr::~ExpDP2Expr() {}

Long ExpDP2Expr::pack(void *space) {
  expr_.pack(space);
  criDesc_.pack(space);
  workAtp_.packShallow(space);
  return NAVersionedObject::pack(space);
}

int ExpDP2Expr::unpack(void *base, void *reallocator) {
  if (expr_.unpack(base, reallocator)) return -1;
  if (criDesc_.unpack(base, reallocator)) return -1;
  if (workAtp_.unpackShallow(base)) return -1;
  char *buf = NULL;
  createWorkAtp(buf, FALSE);
  return NAVersionedObject::unpack(base, reallocator);
}

void ExpDP2Expr::createWorkAtp(char *&inbuf, NABoolean createTempTupp) {
  if (criDesc_ == (ExCriDescPtr)NULL) return;

  // if inbuf passed in, create work atp in it.
  // Otherwise, use the buffer in workAtp_.
  char *buf = (inbuf ? inbuf : (char *)(workAtp_.getPointer()));
  atp_struct *workAtp = createAtpInBuffer(criDesc_, buf);
  int start = (createTempTupp ? 1 : 2);
  for (int i = start; i < criDesc_->noTuples(); i++) {
    tupp_descriptor *td = (tupp_descriptor *)buf;
    td->init();
    workAtp->getTupp(i) = td;
    buf += sizeof(tupp_descriptor);
  }
}

ExpDP2KeyEncodeExpr::ExpDP2KeyEncodeExpr(ex_expr_base *expr, ex_cri_desc *work_cri_desc, Space *space,
                                         short allocateAtpAtRunTime)
    : ExpDP2Expr(expr, work_cri_desc, space, allocateAtpAtRunTime), flags_(0) {
  memset(fillersExpDP2KEExpr_, 0, FILLERS_EXP_DP2_KE_EXPR_SIZE);
}
