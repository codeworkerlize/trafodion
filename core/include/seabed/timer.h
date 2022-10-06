//------------------------------------------------------------------
//

//
// Timer module
//
#ifndef __SB_TIMER_H_
#define __SB_TIMER_H_

#include <unistd.h>  // pid_t

#include "cc.h"
#include "int/diag.h"
#include "int/exp.h"

typedef void (*Timer_Cb_Type)(int tleid, int toval, short parm1, long parm2);
SB_Export int timer_cancel(short tag) SB_DIAG_UNUSED;
SB_Export int timer_register() SB_DIAG_UNUSED;
SB_Export int timer_start_cb(int toval, short parm1, long parm2, short *tleid, Timer_Cb_Type callback) SB_DIAG_UNUSED;
SB_Export _xcc_status XCANCELTIMEOUT(short tag) SB_DIAG_UNUSED;
SB_Export _xcc_status XSIGNALTIMEOUT(int toval, short parm1, long parm2, short *tleid, pid_t tid = 0) SB_DIAG_UNUSED;

#endif  // !__SB_TIMER_H_
