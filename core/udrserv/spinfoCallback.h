
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         spinfocallback.h
 * Description:  The SPInfo APIs provided for LmRoutines to call back
 *               SPInfo methods.
 *
 * Created:
 * Language:     C++
 *
 *****************************************************************************
 */

#include "common/Platform.h"
#include "sqludr/sqludr.h"

void SpInfoGetNextRow(char *rowData,               // OUT
                      int tableIndex,              // IN
                      SQLUDR_Q_STATE *queue_state  // OUT
);

int SpInfoEmitRow(char *rowData,               // IN
                  int tableIndex,              // IN
                  SQLUDR_Q_STATE *queue_state  // IN/OUT
);

void SpInfoEmitRowCpp(char *rowData,               // IN
                      int tableIndex,              // IN
                      SQLUDR_Q_STATE *queue_state  // IN/OUT
);
