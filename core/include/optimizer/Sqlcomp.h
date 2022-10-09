#ifndef SQLCOMP_HDR
#define SQLCOMP_HDR
/* -*-C++-*-
******************************************************************************
*
* File:         Sqlcomp.h
* Description:  A header file that includes all data structures needed by
*               the optimizer files. This file is included in all .C files
*               of the optimizer (or at least in most of them), allowing
*               the use of precompiled header files and speeding up
*               compilation
* Created:      7/29/94
* Language:     C++
*
*

*
*
******************************************************************************
*/

// -----------------------------------------------------------------------

#include "common/CmpCommon.h"
#include "common/ComOptIncludes.h"
#include "optimizer/opt_error.h"
#include "optimizer/SchemaDB.h"

#endif  // SQLCOMP_HDR
