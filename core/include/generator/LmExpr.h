
#ifndef LM_EXPR_H
#define LM_EXPR_H
/* -*-C++-*-
******************************************************************************
*
* File:         LmExpr.h
* Description:  Code to create ItemExpr trees that convert UDR parameters
*               to/from formats acceptable as input to the Language Manager
*
* Created:      10/28/2000
* Language:     C++
*
*
*
******************************************************************************
*/

/*
    This file defines global functions used during UDR codegen to
    manage expressions that convert SQL values to/from formats
    acceptable to the Language Manager (LM). See comments in
    LmExpr.cpp for details.
*/

#include "common/ComSmallDefs.h"
#include "common/NABoolean.h"

//
// Forward class references
//
class NAType;
class ItemExpr;
class CmpContext;

enum LmExprResult { LmExprOK, LmExprError };

NABoolean LmTypeSupportsPrecision(const NAType &t);
NABoolean LmTypeSupportsScale(const NAType &t);
NABoolean LmTypeIsString(const NAType &t, ComRoutineLanguage language, ComRoutineParamStyle style,
                         NABoolean isResultSet);
NABoolean LmTypeIsObject(const NAType &t);

LmExprResult CreateLmInputExpr(const NAType &formalType, ItemExpr &actualValue, ComRoutineLanguage language,
                               ComRoutineParamStyle style, CmpContext *cmpContext, ItemExpr *&newExpr);

LmExprResult CreateLmOutputExpr(const NAType &formalType, ComRoutineLanguage language, ComRoutineParamStyle style,
                                CmpContext *cmpContext, ItemExpr *&newSourceExpr, ItemExpr *&newTargetExpr,
                                NABoolean isResultSet);

#endif  // LM_EXPR_H
