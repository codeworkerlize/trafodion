
/* -*-C++-*-
****************************************************************************
*
* File:         ExpError.h (previously part of /executor/ex_error.h)
* Description:
*
* Created:      5/6/98
* Language:     C++
*
*
*
****************************************************************************
*/

#ifndef EXP_ERROR_H
#define EXP_ERROR_H

#include <stdio.h>

class ComDiagsArea;
class NAMemory;
class ex_clause;

#include "common/OperTypeEnum.h"
#include "exp/ExpErrorEnums.h"
#include "export/NAVersionedObject.h"

// -----------------------------------------------------------------------
// List of all errors generated in the SQL executor code
// -----------------------------------------------------------------------

// NOTE: All the enum values are moved to "ExpErrorEnums.h"

class ComCondition;

#define MAX_OFFENDING_SOURCE_DATA_DISPLAY_LEN 256

// This version of ExRaiseSqlError is used by the expressions code.  In
// addition to having slightly different parameters, it differs from the
// above version in that it puts an error condition in the supplied
// ComDiagsArea rather than in a copy.

ComDiagsArea *ExAddCondition(NAMemory *heap, ComDiagsArea **diagsArea, int err, ComCondition **cond = NULL,
                             int *intParam1 = NULL, int *intParam2 = NULL, int *intParam3 = NULL,
                             const char *stringParam1 = NULL, const char *stringParam2 = NULL,
                             const char *stringParam3 = NULL);

ComDiagsArea *ExRaiseSqlError(NAMemory *heap, ComDiagsArea **diagsArea, ExeErrorCode err, ComCondition **cond = NULL,
                              int *intParam1 = NULL, int *intParam2 = NULL, int *intParam3 = NULL,
                              const char *stringParam1 = NULL, const char *stringParam2 = NULL,
                              const char *stringParam3 = NULL);

ComDiagsArea *ExRaiseSqlError(NAMemory *heap, ComDiagsArea **diagsArea, int err, int *intParam1 = NULL,
                              int *intParam2 = NULL, int *intParam3 = NULL, const char *stringParam1 = NULL,
                              const char *stringParam2 = NULL, const char *stringParam3 = NULL);

ComDiagsArea *ExRaiseSqlWarning(NAMemory *heap, ComDiagsArea **diagsArea, ExeErrorCode err, ComCondition **cond = NULL);
ComDiagsArea *ExRaiseSqlWarning(NAMemory *heap, ComDiagsArea **diagsArea, ExeErrorCode err, ComCondition **cond,
                                int *intParam1 = NULL, int *intParam2 = NULL, int *intParam3 = NULL,
                                const char *stringParam1 = NULL, const char *stringParam2 = NULL,
                                const char *stringParam3 = NULL);
ComDiagsArea *ExRaiseFunctionSqlError(NAMemory *heap, ComDiagsArea **diagsArea, ExeErrorCode err,
                                      NABoolean derivedFunction = FALSE,
                                      OperatorTypeEnum origOperType = ITM_FIRST_ITEM_OP, ComCondition **cond = NULL);
ComDiagsArea *ExRaiseDetailSqlError(CollHeap *heap, ComDiagsArea **diagsArea, ExeErrorCode err, int pciInst, char *op1,
                                    char *op2 = NULL, char *op3 = NULL);

ComDiagsArea *ExRaiseDetailSqlError(CollHeap *heap, ComDiagsArea **diagsArea, ExeErrorCode err, ex_clause *clause,
                                    char *op_data[]);

ComDiagsArea *ExRaiseDetailSqlError(CollHeap *heap, ComDiagsArea **diagsArea, ExeErrorCode err, char *src,
                                    int srcLength, Int16 srcType, int srcScale, Int16 tgtType, UInt32 flags,
                                    int tgtLength = -1, int tgtScale = -1, int tgtPrecision = 0, int srcPrecision = -1,
                                    char *sourceValue = NULL);
char *stringToHex(char *out, int outLen, char *in, int inLen);

#endif /* EXP_ERROR_H */
