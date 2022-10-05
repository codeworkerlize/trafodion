
/* -*-C++-*-
****************************************************************************
*
* File:         ExpDll.cpp
* Description:  "main()" for tdm_sqlexp.dll
*
* Created:      5/6/98
* Language:     C++
*
*
*
****************************************************************************
*/

#include "common/Platform.h"

#include <setjmp.h>

THREAD_P jmp_buf ExportJmpBuf;
