
/* -*-C++-*-
****************************************************************************
*
* File:         ExportDll.cpp
* RCS:          $Id: ExportDll.cpp,v 1.1 1998/06/29 03:50:39  Exp $
* Description:  "main()" for tdm_sqlexport.dll
*
* Created:      5/6/98
* Modified:     $ $Date: 1998/06/29 03:50:39 $ (GMT)
* Language:     C++
* Status:       $State: Exp $
*
*
*
****************************************************************************
*/

// Buf is defined here and exported. Its value is initialized by
// the topmost executable linking this DLL. (e.g. arkcmp.cpp/sqlci.cpp...)
//

#include "common/Platform.h"
#include <setjmp.h>

THREAD_P jmp_buf ExportJmpBuf;
