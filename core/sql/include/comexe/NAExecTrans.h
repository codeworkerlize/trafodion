/* -*-C++-*-
****************************************************************************
*
* File:         NAExecTrans.h
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

#ifndef NAEXECTRANS_H
#define NAEXECTRANS_H

#include "common/BaseTypes.h"
#include "common/Int64.h"
#include "dtm/tm.h"

// -----------------------------------------------------------------------
// some transaction related routines, called by both executor and arkcmp
// -----------------------------------------------------------------------

//   return TRUE, if there is a transaction, transId will hold transaction Id
//   return FALSE, if there is no transaction
// Other values for command are not supported at this time.
NABoolean NAExecTrans(long *transId = NULL);  // IN/OUT
#endif
