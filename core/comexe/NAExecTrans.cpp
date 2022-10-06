/* -*-C++-*-
****************************************************************************
*
* File:         NAExecTrans.cpp
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

#include "comexe/NAExecTrans.h"
#include "cli/sqlcli.h"
#include "cli/sql_id.h"

//   return TRUE, if there is a transaction, transId will hold
//                transaction ID if passed in.
//   return FALSE, if there is no transaction
NABoolean NAExecTrans(long *transId) {
  short retcode = 0;

  long l_transid;
  retcode = GETTRANSID((short *)&l_transid);
  if (transId) *transId = l_transid;

  return (retcode == 0);
}
