
#ifndef _UDREXTRN_H_
#define _UDREXTRN_H_

/* -*-C++-*-
*****************************************************************************
*
* File:         udrextrn.h
* Description:
* Created:      01/01/2001
* Language:     C++
*
*
*****************************************************************************
*/

#include "LmError.h"
#include "LmLangManagerJava.h"
#include "UdrExeIpc.h"
#include "cli/SQLCLIdev.h"
#include "cli/sqlcli.h"
#include "export/ComDiags.h"
#include "spinfo.h"
#include "udrglobals.h"

class UdrServerReplyStream;
class UdrServerDataStream;
class UdrGlobals;

extern void sendControlReply(UdrGlobals *UdrGlob, UdrServerReplyStream &msgStream, SPInfo *sp);
extern void sendDataReply(UdrGlobals *UdrGlob, UdrServerDataStream &msgStream, SPInfo *sp);

extern void controlErrorReply(UdrGlobals *UdrGlob, UdrServerReplyStream &msgStream, int errortype, int error,
                              const char *charErrorInfo);
extern void dataErrorReply(UdrGlobals *UdrGlob, UdrServerDataStream &msgStream, int errortype, int error,
                           const char *charErrorInfo = NULL, ComDiagsArea *diags = NULL);

#endif  // _UDREXTRN_H_
