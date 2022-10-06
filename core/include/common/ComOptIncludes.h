#ifndef COMINCLUDES_HDR
#define COMINCLUDES_HDR
/* -*-C++-*-
******************************************************************************
*
* File:         ComIncludes.h
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

#include <string.h>
#include <ctype.h>
#include "optimizer/RelExpr.h"
#include "optimizer/ItemExpr.h"
#include "optimizer/ItemExprList.h"
#include "common/CharType.h"
#include "common/CompositeType.h"
#include "common/DatetimeType.h"
#include "common/IntervalType.h"
#include "common/MiscType.h"
#include "common/NumericType.h"
#endif  // COMINCLUDES_HDR
