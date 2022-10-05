
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
#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include "common/Int64.h"

#include "spinfo.h"
#include "common/copyright.h"
#include "udrutil.h"
#include "LmError.h"
#include "common/NumericType.h"
#include "udrdefs.h"
#include "udrglobals.h"

#include "sqlmxevents/logmxevent.h"

#include "UdrExeIpc.h"

#include "export/ComDiags.h"

extern UdrGlobals *UDR_GLOBALS;  // UDR globals area

jmp_buf UdrHeapLongJmpTgt;  // udrHeap memory failure
jmp_buf IpcHeapLongJmpTgt;  // ipcHeap memory failure

#define UDRSERV_ERROR_PREFIX "*** ERROR[] "

#endif  // _UDRSERV_H_
