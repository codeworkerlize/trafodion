
#ifndef EXPOVFLPTAL_H
#define EXPOVFLPTAL_H

/* -*-C++-*-
 *****************************************************************************
 *
 * File:         <exp_ovfl_ptal.h>
 * Description:
 *
 *
 * Created:      9/28/1999
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "common/Int64.h"

long EXP_FIXED_OV_ADD(long op1, long op2, short *ov);
long EXP_FIXED_OV_SUB(long op1, long op2, short *ov);
long EXP_FIXED_OV_MUL(long op1, long op2, short *ov);
long EXP_FIXED_OV_DIV(long op1, long op2, short *ov);

long EXP_FIXED_OV_ADD_SETMAX(long op1, long op2);
short EXP_SHORT_OV_ADD_SETMAX(short op1, short op2);

#endif
