
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

#include "comexe/ComTdbSample.h"

#include "comexe/ComTdbCommon.h"

//////////////////////////////////////////////////////////////////////////////////
//
//  TDB procedures
//
//////////////////////////////////////////////////////////////////////////////////

// Constructor
ComTdbSample::ComTdbSample() : ComTdb(ComTdb::ex_SAMPLE, eye_SAMPLE) {}

ComTdbSample::ComTdbSample(ex_expr *initExpr, ex_expr *balanceExpr, int returnFactorOffset, ex_expr *postPred,
                           ComTdb *child_tdb, ex_cri_desc *given_cri_desc, ex_cri_desc *returned_cri_desc,
                           queue_index down, queue_index up)

    : ComTdb(ComTdb::ex_SAMPLE, eye_SAMPLE, (Cardinality)0.0, given_cri_desc, returned_cri_desc, down, up, 0, 0),
      initExpr_(initExpr),
      balanceExpr_(balanceExpr),
      returnFactorOffset_(returnFactorOffset),
      postPred_(postPred),
      tdbChild_(child_tdb) {}

ComTdbSample::~ComTdbSample() {}

void ComTdbSample::display() const {};

int ComTdbSample::orderedQueueProtocol() const { return -1; }

Long ComTdbSample::pack(void *space) {
  tdbChild_.pack(space);
  initExpr_.pack(space);
  balanceExpr_.pack(space);
  postPred_.pack(space);

  return ComTdb::pack(space);
}

int ComTdbSample::unpack(void *base, void *reallocator) {
  if (tdbChild_.unpack(base, reallocator)) return -1;
  if (initExpr_.unpack(base, reallocator)) return -1;
  if (balanceExpr_.unpack(base, reallocator)) return -1;
  if (postPred_.unpack(base, reallocator)) return -1;

  return ComTdb::unpack(base, reallocator);
}
