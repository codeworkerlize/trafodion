#ifndef EX_ERROR_H
#define EX_ERROR_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ex_error.h
 * Description:  Executor error codes
 *
 *
 * Created:      2/16/96
 * Language:     C++
 *
 *

 *
 *
 *****************************************************************************
 */

#include "exp/ExpError.h"  // contains enum ExeErrorCode
#include "export/ComDiags.h"

class ComCondition;
class ex_globals;
class ex_queue_entry;
struct ex_queue_pair;

// -----------------------------------------------------------------------
// Some convenient ways to raise errors
// -----------------------------------------------------------------------

// If request "req" has an attached diagnostics area, then it is copied,
// and the indicated condition is added to it.  If "req" does not have an
// attached diagnostics area, then an empty one is allocated in heap "heap",
// and the indicated condition added to it.  In either case, a pointer to
// the new condition is returned through parameter "cond".
ComDiagsArea *ExRaiseSqlError(CollHeap *heap, ex_queue_entry *req, ExeErrorCode code, int *intParam1 = NULL,
                              char *stringParam1 = NULL, ComCondition **cond = NULL);

ComDiagsArea *ExRaiseSqlWarning(CollHeap *heap, ex_queue_entry *req, ExeErrorCode code, int *intParam1 = NULL,
                                char *stringParam1 = NULL, ComCondition **cond = NULL);

void ExHandleArkcmpErrors(ex_queue_pair &qparent, ex_queue_entry *down_entry, int matchNo, ex_globals *globals,
                          ComDiagsArea *da, ExeErrorCode err = EXE_INTERNAL_ERROR);

void ExHandleErrors(ex_queue_pair &qparent, ex_queue_entry *down_entry, int matchNo, ex_globals *globals,
                    ComDiagsArea *da, ExeErrorCode err = EXE_INTERNAL_ERROR, int *intParam1 = NULL,
                    const char *stringParam1 = NULL, int *nskErr = NULL, const char *stringParam2 = NULL);

#endif /* EX_ERROR_H */
