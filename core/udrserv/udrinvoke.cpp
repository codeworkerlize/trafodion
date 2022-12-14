
/* -*-C++-*-
*****************************************************************************
*
* File:         udrinvoke.cpp
* Description:  This is the module that processes UDR Invoke messages.
*               The tasks for this process are to :
*               . Extract SP descriptive attributes from message
*               . Extract SP parameter attributes from message
*               . Build SPInfo data structure and populate with attributes
*               . Return SP Id (creation timestamp) to client
*               . Handle errors returned from LM.
*               . Deal with resource allocation problems of SPInfo
*                 data structures
*
* Created:      01/01/2001
* Language:     C++
*
*
*****************************************************************************
*/

#include "executor/UdrExeIpc.h"
#include "UdrStreams.h"
#include "executor/sql_buffer.h"
#include "udrdecs.h"
#include "udrdefs.h"
#include "udrextrn.h"
#include "udrutil.h"

NABoolean allocateReplyRow(UdrGlobals *UdrGlob,
                           SqlBuffer &replyBuffer,        // [IN]  A reply buffer
                           queue_index parentIndex,       // [IN]  Identifies the request queue entry
                           int replyRowLen,               // [IN]  Length of reply row
                           char *&newReplyRow,            // [OUT] The allocated reply row
                           ControlInfo *&newControlInfo,  // [OUT] The allocated ControlInfo entry
                           ex_queue::up_status upStatus   // [IN]  Q_OK_MMORE, Q_NO_DATA, Q_SQLERROR
) {
  const char *moduleName = "allocateReplyRow";

  doMessageBox(UdrGlob, TRACE_SHOW_DIALOGS, UdrGlob->showInvoke_, moduleName);

  NABoolean result = FALSE;
  SqlBufferBase::moveStatus status;
  up_state upState;
  upState.parentIndex = parentIndex;
  upState.downIndex = 0;
  upState.setMatchNo(0);

  upState.status = upStatus;

  tupp_descriptor *tdesc = NULL, **tuppDesc;
  ControlInfo **ctrlInfo;
  NABoolean moveCtrlInfo, moveDataInfo;

  switch (upStatus) {
    case ex_queue::Q_OK_MMORE: {
      ctrlInfo = &newControlInfo;
      moveCtrlInfo = TRUE;
      moveDataInfo = TRUE;
      tuppDesc = &tdesc;
      break;
    }

    case ex_queue::Q_SQLERROR: {
      ctrlInfo = &newControlInfo;
      moveCtrlInfo = TRUE;
      moveDataInfo = FALSE;
      tuppDesc = NULL;
      break;
    }

    case ex_queue::Q_NO_DATA: {
      ctrlInfo = NULL;
      moveCtrlInfo = TRUE;
      moveDataInfo = FALSE;
      tuppDesc = NULL;
      break;
    }

    default: {
      UDR_ASSERT(FALSE, "Unknown ex_queue::up_status value.");
      return FALSE;
    }
  }

  status = replyBuffer.moveInSendOrReplyData(FALSE,                // [IN] sending? (vs. replying)
                                             moveCtrlInfo,         // [IN] force move of ControlInfo?
                                             moveDataInfo,         // [IN] move data?
                                             (void *)&upState,     // [IN] queue state
                                             sizeof(ControlInfo),  // [IN] length of ControlInfo
                                             ctrlInfo,             // [OUT] new ControlInfo
                                             replyRowLen,          // [IN] data row length
                                             tuppDesc,             // [OUT] new data tupp_desc
                                             NULL,                 // [IN] diags area
                                             0                     // [OUT] new diags tupp_desc
  );

  if (status == SqlBufferBase::MOVE_SUCCESS) {
    if (upStatus == ex_queue::Q_OK_MMORE) {
      newReplyRow = tdesc->getTupleAddress();
      memset(newReplyRow, 0, replyRowLen);
    }
    result = TRUE;
  } else {
    result = FALSE;
  }
  return result;
}

NABoolean allocateErrorRow(UdrGlobals *UdrGlob, SqlBuffer &replyBuffer, queue_index parentIndex,
                           NABoolean setDiagsFlag) {
  char *dummyData = NULL;
  ControlInfo *ci = NULL;

  NABoolean ok = allocateReplyRow(UdrGlob, replyBuffer, parentIndex, 0, dummyData, ci, ex_queue::Q_SQLERROR);

  if (ok && ci && setDiagsFlag) ci->setIsExtDiagsAreaPresent(TRUE);

  return ok;
}

NABoolean allocateEODRow(UdrGlobals *UdrGlob, SqlBuffer &replyBuffer, queue_index parentIndex) {
  char *dummyData = NULL;
  ControlInfo *ci = NULL;

  NABoolean ok = allocateReplyRow(UdrGlob, replyBuffer, parentIndex, 0, dummyData, ci, ex_queue::Q_NO_DATA);

  return ok;
}

NABoolean allocateReplyRowAndEOD(UdrGlobals *UdrGlob, SqlBuffer &replyBuffer, queue_index parentIndex, char *&replyData,
                                 int rowLen, ControlInfo *&newControlInfo) {
  const char *moduleName = "allocateReplyRowAndEOD";

  doMessageBox(UdrGlob, TRACE_SHOW_DIALOGS, UdrGlob->showInvoke_, moduleName);

  NABoolean ok =
      allocateReplyRow(UdrGlob, replyBuffer, parentIndex, rowLen, replyData, newControlInfo, ex_queue::Q_OK_MMORE);

  if (ok) ok = allocateEODRow(UdrGlob, replyBuffer, parentIndex);

  return ok;
}

NABoolean allocateErrorRowAndEOD(UdrGlobals *UdrGlob, SqlBuffer &replyBuffer, queue_index parentIndex,
                                 NABoolean setDiagsFlag) {
  NABoolean ok = allocateErrorRow(UdrGlob, replyBuffer, parentIndex, setDiagsFlag);

  if (ok) ok = allocateEODRow(UdrGlob, replyBuffer, parentIndex);

  return ok;
}

void backoutTupps(SqlBuffer &b, int numTuppsBefore) {
  while (b.getTotalTuppDescs() > numTuppsBefore) {
    b.remove_tuple_desc();
  }
}

NABoolean convertReplyRowToErrorRow(SqlBuffer *sqlBuf, int numTuppsBefore, queue_index requestQueueIndex,
                                    UdrServerDataStream &msgStream, UdrGlobals *UdrGlob) {
  // Remove tupps after numTuppsBefore
  backoutTupps(*sqlBuf, numTuppsBefore);

  // Add Error and EOD row
  NABoolean ok = allocateErrorRowAndEOD(UdrGlob, *sqlBuf, requestQueueIndex, TRUE);
  if (!ok) {
    // failed to allocate Error Row and EOD
    // Backout all the reply rows and return UdrErrorReply
    backoutTupps(*sqlBuf, 0);
    dataErrorReply(UdrGlob, msgStream, UDR_ERR_MESSAGE_PROCESSING, INVOKE_ERR_NO_ERROR_ROW, NULL);
  }

  return ok;
}
