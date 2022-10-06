
#ifndef GETERRORMESSAGE_H
#define GETERRORMESSAGE_H

/* -*-C++-*-
 *****************************************************************************
 *
 * File:         GetErrorMessage.h
 * RCS:          $Id: GetErrorMessage.h,v 1.12 1998/07/20 07:26:40  Exp $
 * Description:
 *
 * Created:      4/5/96
 * Modified:     $ $Date: 1998/07/20 07:26:40 $ (GMT)
 * Language:     C++
 * Status:       $State: Exp $
 *
 *
 *****************************************************************************
 */

#include "common/NABoolean.h"
#include "common/NAWinNT.h"

enum MsgTextType {
  ERROR_TEXT = 1,
  CAUSE_TEXT,
  EFFECT_TEXT,
  RECOVERY_TEXT,
  SQL_STATE,
  HELP_ID,
  EMS_SEVERITY,
  EMS_EVENT_TARGET,
  EMS_EXPERIENCE_LEVEL
};

enum ErrorType { MAIN_ERROR = 0, SUBERROR_HBASE, SUBERROR_TM };

#define SQLERRORS_MSGFILE_VERSION_INFO 10
#define SQLERRORS_MSGFILE_NOT_FOUND    16000
#define SQLERRORS_MSG_NOT_FOUND        16001

#ifdef __cplusplus
extern "C" {
#endif

void GetErrorMessageRebindMessageFile();
char *GetPastHeaderOfErrorMessage(char *text);

const char *GetErrorMessageFileName();

short GetErrorMessageRC(int num, NAWchar *msgBuf, int bufSize);

short GetErrorMessage(ErrorType errType, int error_code, NAWchar *&return_text, MsgTextType M_type = ERROR_TEXT,
                      NAWchar *alternate_return_text = NULL, int recurse_level = 0, NABoolean prefixAdded = TRUE);

void ErrorMessageOverflowCheckW(NAWchar *buf, size_t maxsiz);

#ifdef __cplusplus
}
#endif

#endif /* GETERRORMESSAGE_H */
