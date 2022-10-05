#ifndef STMTCOMPILATIONMODE_C
#define STMTCOMPILATIONMODE_C
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtCompilationMode.C
 * Description:  class for parse node representing Static / Dynamic SQL
 *               Static allow host variable , but not param. Dynamic allow
 *               param, but not host variable.
 * Created:      03/28/96
 *
 *

 *
 *
 *****************************************************************************
 */

#include "parser/StmtCompilationMode.h"

// Init the static data members.
THREAD_P WhoAmI IdentifyMyself::myName_ = I_AM_UNKNOWN;

#endif  // STMTCOMPILATIONMODE_C
