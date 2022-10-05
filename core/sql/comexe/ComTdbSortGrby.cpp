
/* -*-C++-*-
****************************************************************************
*
* File:         ComTdbSortGrby.cpp
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

#include "comexe/ComTdbSortGrby.h"
#include "comexe/ComTdbCommon.h"

//////////////////////////////////////////////////////////////////////////////////
//
//  TDB procedures
//
//////////////////////////////////////////////////////////////////////////////////

// Constructor
ComTdbSortGrby::ComTdbSortGrby() : ComTdb(ComTdb::ex_SORT_GRBY, eye_SORT_GRBY), tuppIndex_(0) {}

ComTdbSortGrby::ComTdbSortGrby(ex_expr *aggr_expr, ex_expr *grby_expr, ex_expr *move_expr, ex_expr *having_expr,
                               int reclen, const unsigned short tupp_index, ComTdb *child_tdb,
                               ex_cri_desc *given_cri_desc, ex_cri_desc *returned_cri_desc, queue_index down,
                               queue_index up, Cardinality estimatedRowCount, int num_buffers, int buffer_size,
                               NABoolean tolerateNonFatalError)

    : ComTdb(ComTdb::ex_SORT_GRBY, eye_SORT_GRBY, estimatedRowCount, given_cri_desc, returned_cri_desc, down, up,
             num_buffers, buffer_size),
      recLen_(reclen),
      tuppIndex_(tupp_index),
      aggrExpr_(aggr_expr),
      grbyExpr_(grby_expr),
      moveExpr_(move_expr),
      havingExpr_(having_expr),
      flags_(0),
      tdbChild_(child_tdb),
      numRollupGroups_(-1) {
  if (tolerateNonFatalError) setTolerateNonFatalError(TRUE);
}

ComTdbSortGrby::~ComTdbSortGrby() {}

void ComTdbSortGrby::display() const {};

int ComTdbSortGrby::orderedQueueProtocol() const { return -1; }

Long ComTdbSortGrby::pack(void *space) {
  tdbChild_.pack(space);
  aggrExpr_.pack(space);
  grbyExpr_.pack(space);
  moveExpr_.pack(space);
  havingExpr_.pack(space);
  return ComTdb::pack(space);
}

int ComTdbSortGrby::unpack(void *base, void *reallocator) {
  if (tdbChild_.unpack(base, reallocator)) return -1;
  if (aggrExpr_.unpack(base, reallocator)) return -1;
  if (grbyExpr_.unpack(base, reallocator)) return -1;
  if (moveExpr_.unpack(base, reallocator)) return -1;
  if (havingExpr_.unpack(base, reallocator)) return -1;
  return ComTdb::unpack(base, reallocator);
}
void ComTdbSortGrby::displayContents(Space *space, int flag) {
  ComTdb::displayContents(space, flag & 0xFFFFFFFE);

  if (flag & 0x00000008) {
    char buf[100];
    str_sprintf(buf, "\nFor ComTdbSortGrby :\nFlags = %x, recLen = %d ", flags_, recLen_);
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
  }

  if (flag & 0x00000001) {
    displayExpression(space, flag);
    displayChildren(space, flag);
  }
}
