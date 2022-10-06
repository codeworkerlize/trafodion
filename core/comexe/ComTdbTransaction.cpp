
/* -*-C++-*-
****************************************************************************
*
* File:         ComTdbTransaction.cpp
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

// -----------------------------------------------------------------------

#include "comexe/ComTdbTransaction.h"
#include "comexe/ComTdbCommon.h"

/////////////////////////////////////////////////////////////////
// class ComTdbTransaction, ExTransTcb, ExTransPrivateState
/////////////////////////////////////////////////////////////////
ComTdbTransaction::ComTdbTransaction(TransStmtType trans_type, TransMode *trans_mode, ex_expr *diag_area_size_expr,
                                     ex_cri_desc *work_cri_desc, ex_cri_desc *given_cri_desc,
                                     ex_cri_desc *returned_cri_desc, queue_index down, queue_index up,
                                     int num_buffers, int buffer_size)
    : ComTdb(ComTdb::ex_TRANSACTION, eye_TRANSACTION, (Cardinality)0.0, given_cri_desc, returned_cri_desc, down, up,
             num_buffers, buffer_size),
      transType_(trans_type),
      transMode_(trans_mode),
      diagAreaSizeExpr_(diag_area_size_expr),
      workCriDesc_(work_cri_desc),
      hasSavepointName_(0),
      flags_(0) {}

Long ComTdbTransaction::pack(void *space) {
  transMode_.pack(space);
  diagAreaSizeExpr_.pack(space);
  workCriDesc_.pack(space);
  return ComTdb::pack(space);
}

int ComTdbTransaction::unpack(void *base, void *reallocator) {
  if (transMode_.unpack(base, reallocator)) return -1;
  if (diagAreaSizeExpr_.unpack(base, reallocator)) return -1;
  if (workCriDesc_.unpack(base, reallocator)) return -1;
  return ComTdb::unpack(base, reallocator);
}
