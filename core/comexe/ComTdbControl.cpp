
/* -*-C++-*-
****************************************************************************
*
* File:         ComTdbControl.cpp
* Description:  This is just a template for Executor TDB classes derived
*               from subclasses of ComTdb.
*
* Created:      5/6/98
* Language:     C++
*
*
*
*
****************************************************************************
*/

#include "comexe/ComTdbCommon.h"
#include "comexe/ComTdbControl.h"

/////////////////////////////////////////////////////
// class ExControlTdb
/////////////////////////////////////////////////////
ComTdbControl::ComTdbControl(ControlQueryType cqt, int reset, char *sqlText, Int16 sqlTextCharSet, char *value1,
                             char *value2, char *value3, ex_cri_desc *given_cri_desc, ex_cri_desc *returned_cri_desc,
                             queue_index down, queue_index up, int num_buffers, int buffer_size)
    : ComTdb(ComTdb::ex_CONTROL_QUERY, eye_CONTROL_QUERY, (Cardinality)0.0, given_cri_desc, returned_cri_desc, down, up,
             num_buffers, buffer_size),
      cqt_(cqt),
      reset_(reset),
      sqlText_(sqlText),
      sqlTextCharSet_(sqlTextCharSet),
      value1_(value1),
      value2_(value2),
      value3_(value3),
      actionType_(NONE_),
      nonResettable_(FALSE),
      flags_(0) {}

Long ComTdbControl::pack(void *space) {
  sqlText_.pack(space);
  value1_.pack(space);
  value2_.pack(space);
  value3_.pack(space);
  return ComTdb::pack(space);
}

int ComTdbControl::unpack(void *base, void *reallocator) {
  if (sqlText_.unpack(base)) return -1;
  if (value1_.unpack(base)) return -1;
  if (value2_.unpack(base)) return -1;
  if (value3_.unpack(base)) return -1;
  return ComTdb::unpack(base, reallocator);
}
