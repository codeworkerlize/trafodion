
/* -*-C++-*-
****************************************************************************
*
* File:         ComTdbTimeout.cpp
* Description:
*
* Created:      12/27/1999
* Language:     C++
*
*
*
****************************************************************************
*/

// -----------------------------------------------------------------------

#include "comexe/ComTdbTimeout.h"

#include "comexe/ComTdbCommon.h"

/////////////////////////////////////////////////////////////////
// class ComTdbTimeout
/////////////////////////////////////////////////////////////////
ComTdbTimeout::ComTdbTimeout(ex_expr *timeout_value_expr, ex_cri_desc *work_cri_desc, ex_cri_desc *given_cri_desc,
                             ex_cri_desc *returned_cri_desc, queue_index down, queue_index up, int num_buffers,
                             int buffer_size)
    : ComTdb(ComTdb::ex_SET_TIMEOUT, eye_SET_TIMEOUT, (Cardinality)0.0, given_cri_desc, returned_cri_desc, down, up,
             num_buffers, buffer_size),
      timeoutValueExpr_(timeout_value_expr),
      workCriDesc_(work_cri_desc),
      flags_(0) {}

Long ComTdbTimeout::pack(void *space) {
  timeoutValueExpr_.pack(space);
  workCriDesc_.pack(space);
  return ComTdb::pack(space);
}

int ComTdbTimeout::unpack(void *base, void *reallocator) {
  if (timeoutValueExpr_.unpack(base, reallocator)) return -1;
  if (workCriDesc_.unpack(base, reallocator)) return -1;
  return ComTdb::unpack(base, reallocator);
}
