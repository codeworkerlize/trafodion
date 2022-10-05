
/* -*-C++-*-
****************************************************************************
*
* File:         ComTdbDp2Oper.h
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

#ifndef COM_DP2_OPER_H
#define COM_DP2_OPER_H

#include "common/Platform.h"
#include "comexe/ComTdb.h"

///////////////////////////////////////////////////////
// class ComTdbDp2Oper
///////////////////////////////////////////////////////
class ComTdbDp2Oper : public ComTdb {
 public:
  enum SqlTableType { KEY_SEQ_, KEY_SEQ_WITH_SYSKEY_, ENTRY_SEQ_, RELATIVE_, NOOP_ };
};

#endif
