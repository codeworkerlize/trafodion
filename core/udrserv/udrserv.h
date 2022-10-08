
#ifndef _UDRSERV_H_
#define _UDRSERV_H_

/* -*-C++-*-
******************************************************************************
*
* File:         udrserv.h
* Description:  Definitions for mainline code.
*
* Created:      4/10/2001
* Language:     C++
*
*
*
*
******************************************************************************
*/

#define NA_COMPILE_INSTANTIATE

#include <limits.h>
#include <stdio.h>
#include <stdlib.h>

#include <iostream>

#include "LmError.h"
#include "executor/UdrExeIpc.h"
#include "common/Int64.h"
#include "common/NumericType.h"
#include "common/copyright.h"
#include "export/ComDiags.h"
#include "spinfo.h"
#include "sqlmxevents/logmxevent.h"
#include "udrdefs.h"
#include "udrglobals.h"
#include "udrutil.h"

extern UdrGlobals *UDR_GLOBALS;  // UDR globals area

jmp_buf UdrHeapLongJmpTgt;  // udrHeap memory failure
jmp_buf IpcHeapLongJmpTgt;  // ipcHeap memory failure

#define UDRSERV_ERROR_PREFIX "*** ERROR[] "

#endif  // _UDRSERV_H_
