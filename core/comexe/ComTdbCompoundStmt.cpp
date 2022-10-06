
/* -*-C++-*-
******************************************************************************
*
* File:         ComTdbCompoundStmt.cpp
* Description:  3GL compound statement (CS) operator.
*
* Created:      4/1/98
* Language:     C++
*
*
*
******************************************************************************
*/

#include "comexe/ComTdbCompoundStmt.h"

#include "comexe/ComTdb.h"
#include "comexe/ComTdbCommon.h"

//////////////////////////////////////////////////////////////////////////////
//
// CompoundStmt TDB methods.
//
//////////////////////////////////////////////////////////////////////////////

ComTdbCompoundStmt::ComTdbCompoundStmt() : ComTdb(ComTdb::ex_COMPOUND_STMT, eye_CS) {}

ComTdbCompoundStmt::ComTdbCompoundStmt(ComTdb *left, ComTdb *right, ex_cri_desc *given, ex_cri_desc *returned,
                                       queue_index down, queue_index up, int numBuffers, int bufferSize,
                                       NABoolean rowsFromLeft, NABoolean rowsFromRight, NABoolean afterUpdate)
    : ComTdb(ComTdb::ex_COMPOUND_STMT, eye_CS, (Cardinality)0.0, given, returned, down, up, numBuffers, bufferSize),
      tdbLeft_(left),
      tdbRight_(right),
      flags_(0) {
  if (rowsFromLeft) flags_ |= ComTdbCompoundStmt::ROWS_FROM_LEFT;
  if (rowsFromRight) flags_ |= ComTdbCompoundStmt::ROWS_FROM_RIGHT;
  if (afterUpdate) flags_ |= ComTdbCompoundStmt::AFTER_UPDATE;
}

Long ComTdbCompoundStmt::pack(void *space) {
  tdbLeft_.pack(space);
  tdbRight_.pack(space);

  return ComTdb::pack(space);

}  // ComTdbCompoundStmt::pack

int ComTdbCompoundStmt::unpack(void *base, void *reallocator) {
  if (tdbLeft_.unpack(base, reallocator)) return -1;
  if (tdbRight_.unpack(base, reallocator)) return -1;

  return ComTdb::unpack(base, reallocator);

}  // ComTdbCompoundStmt::unpack

// exclude from code coverage analysis sind it is used only by GUI
inline const ComTdb *ComTdbCompoundStmt::getChild(int pos) const {
  if (pos == 0)
    return tdbLeft_;
  else if (pos == 1)
    return tdbRight_;
  else
    return NULL;

}  // ComTdbCompoundStmt::getChild
