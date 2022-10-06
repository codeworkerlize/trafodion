//------------------------------------------------------------------
//


//
// Process-Control module
//
#ifndef __SB_PCTL_H_
#define __SB_PCTL_H_

#include "int/diag.h"
#include "int/exp.h"
#include "int/types.h"

#include "pctlcom.h"

#ifndef USE_EVENT_REG
//#define USE_EVENT_REG*/
#endif

//
// Call this to enable external wakeups
//
SB_Export void proc_enable_external_wakeups();

#ifdef USE_EVENT_REG
//
// Call this to de-register a thread with an event.
//
SB_Export int proc_event_deregister(short event) SB_DIAG_UNUSED;

//
// Call this to disable abort (TODO: temporary)
//
SB_Export void proc_event_disable_abort();

//
// Call this to register a thread with an event.
//
SB_Export int proc_event_register(short event) SB_DIAG_UNUSED;
#endif

//
// Call this to register group/pin for thread
//
SB_Export void proc_register_group_pin(int group, int pin);

//
// Call this to set process-based completions
//
SB_Export int proc_set_process_completion() SB_DIAG_UNUSED;

//
// emulation functions
//
SB_Export void XAWAKE(int pin, short event);
SB_Export void XAWAKE_A06(int pin, short event, short fun);
SB_Export short XPROCESS_AWAKE_(int pid, short event) SB_DIAG_UNUSED;
// XPROCESS_GETPAIRINFO_() - see pctlcom.h
SB_Export short XPROCESSHANDLE_COMPARE_(SB_Phandle_Type *processhandle1,
                                        SB_Phandle_Type *processhandle2) SB_DIAG_UNUSED;
// XPROCESSHANDLE_DECOMPSE_() - see pctlcom.h
SB_Export short XPROCESSHANDLE_GETMINE_(SB_Phandle_Type *processhandle) SB_DIAG_UNUSED;
SB_Export short XPROCESSHANDLE_NULLIT_(SB_Phandle_Type *processhandle) SB_DIAG_UNUSED;
//
// Current emulates NSK, but will change to disallow time = 0.
//
SB_Export short XWAIT(short mask, int time) SB_DIAG_UNUSED;
//
// Emulates NSK
//
SB_Export short XWAIT0(short mask, int time) SB_DIAG_UNUSED;
//
// Emulates NSK EXCEPT that time = 0 not allowed
//
SB_Export short XWAITNO0(short mask, int time) SB_DIAG_UNUSED;

#endif  // !__SB_PCTL_H_
