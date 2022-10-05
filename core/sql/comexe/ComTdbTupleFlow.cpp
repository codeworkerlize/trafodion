
/* -*-C++-*-
****************************************************************************
*
* File:         ComTdbTupleFlow.cpp
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

#include "comexe/ComTdbTupleFlow.h"
#include "comexe/ComTdbCommon.h"

/////////////////////////////////////////////////////////////////////////
//
//  TDB procedures
//
/////////////////////////////////////////////////////////////////////////
ComTdbTupleFlow::ComTdbTupleFlow(ComTdb *tdb_src, ComTdb *tdb_tgt, ex_cri_desc *given_cri_desc,
                                 ex_cri_desc *returned_cri_desc, ex_expr *tgt_expr, ex_cri_desc *work_cri_desc,
                                 queue_index down, queue_index up, Cardinality estimatedRowCount, int num_buffers,
                                 int buffer_size, NABoolean vsbbInsert, NABoolean rowsetIterator,
                                 NABoolean tolerateNonFatalError)
    : ComTdb(ComTdb::ex_TUPLE_FLOW, eye_TUPLE_FLOW, estimatedRowCount, given_cri_desc, returned_cri_desc, down, up,
             num_buffers, buffer_size),
      tdbSrc_(tdb_src),
      tdbTgt_(tdb_tgt),
      tgtExpr_(tgt_expr),
      workCriDesc_(work_cri_desc),
      flags_(0) {
  if (vsbbInsert == TRUE) flags_ |= VSBB_INSERT;
  if (rowsetIterator == TRUE) flags_ |= ROWSET_ITERATOR;
  if (tolerateNonFatalError == TRUE) setTolerateNonFatalError(TRUE);
}

// destructor
ComTdbTupleFlow::~ComTdbTupleFlow() {}

Long ComTdbTupleFlow::pack(void *space) {
  tdbSrc_.pack(space);
  tdbTgt_.pack(space);
  workCriDesc_.pack(space);
  tgtExpr_.pack(space);
  return ComTdb::pack(space);
}

int ComTdbTupleFlow::unpack(void *base, void *reallocator) {
  if (tdbSrc_.unpack(base, reallocator)) return -1;
  if (tdbTgt_.unpack(base, reallocator)) return -1;
  if (workCriDesc_.unpack(base, reallocator)) return -1;
  if (tgtExpr_.unpack(base, reallocator)) return -1;
  return ComTdb::unpack(base, reallocator);
}

const ComTdb *ComTdbTupleFlow::getChild(int pos) const {
  if (pos == 0)
    return tdbSrc_;
  else if (pos == 1)
    return tdbTgt_;
  else
    return NULL;
}

ex_expr *ComTdbTupleFlow::getExpressionNode(int pos) {
  if (pos == 0)
    return tgtExpr_;
  else
    return NULL;
}

const char *ComTdbTupleFlow::getExpressionName(int pos) const {
  if (pos == 0)
    return "tgtExpr_";
  else
    return NULL;
}

void ComTdbTupleFlow::displayContents(Space *space, int flag) {
  ComTdb::displayContents(space, flag & 0xFFFFFFFE);

  if (flag & 0x00000008) {
    char buf[100];

    int lFlags = flags_ % 65536;
    int hFlags = (flags_ - lFlags) / 65536;
    str_sprintf(buf, "\nFor ComTdbTupleFlow :\nFlags = %x%x ", hFlags, lFlags);
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
  }

  if (flag & 0x00000001) {
    displayExpression(space, flag);
    displayChildren(space, flag);
  }
}
