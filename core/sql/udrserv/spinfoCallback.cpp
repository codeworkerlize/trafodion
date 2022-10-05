
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         spinfocallback.cpp
 * Description:  The SPInfo APIs provided for LmRoutines to call back
 *               SPInfo methods.
 *
 * Created:
 * Language:     C++
 *
 *****************************************************************************
 */

#include "common/Platform.h"  // 64-BIT
#include "spinfoCallback.h"
#include "spinfo.h"
#include "udrextrn.h"
#include "exp/exp_expr.h"
#include "executor/sql_buffer.h"
#include "ex_queue.h"
#include "udrutil.h"
#include "LmRoutineCppObj.h"
// include the file generated by this command and checked into git:
// javah -d $TRAF_HOME/../sql/langman org.trafodion.sql.udr.UDR
#include "org_trafodion_sql_udr_UDR.h"

extern UdrGlobals *UDR_GLOBALS;
extern int PerformWaitedReplyToClient(UdrGlobals *UdrGlob, UdrServerDataStream &msgStream);
extern void doMessageBox(UdrGlobals *UdrGlob, int trLevel, NABoolean moduleType, const char *moduleName);

extern NABoolean allocateReplyRow(UdrGlobals *UdrGlob,
                                  SqlBuffer &replyBuffer,        // [IN]  A reply buffer
                                  queue_index parentIndex,       // [IN]  Identifies the request queue entry
                                  int replyRowLen,             // [IN]  Length of reply row
                                  char *&newReplyRow,            // [OUT] The allocated reply row
                                  ControlInfo *&newControlInfo,  // [OUT] The allocated ControlInfo entry
                                  ex_queue::up_status upStatus   // [IN]  Q_OK_MMORE, Q_NO_DATA, Q_SQLERROR
);
extern NABoolean allocateEODRow(UdrGlobals *UdrGlob, SqlBuffer &replyBuffer, queue_index parentIndex);

int sendReqBufferWaitedReply(UdrGlobals *udrGlobals, SPInfo *sp, int tableIndex) {
  // get datastream
  UdrServerDataStream *dataStream = sp->getDataStream();

  // construct a reply with flags indicating more data required
  // for this table index.
  UdrDataBuffer *reply = new (*dataStream, sp->getReplyBufferSize())
      UdrDataBuffer(sp->getReplyBufferSize(), UdrDataBuffer::UDR_DATA_OUT, NULL);

  // Sql buffer is initilized when created on IPC stream. no need to init again
  // because the buffer state will change from INUSE to EMPTY state. This will
  // cause IPC to not even send the buffer as part of reply.

  // Indicate the table index so that client can send a buffer
  // that is applicable to this table.
  reply->setTableIndex(tableIndex);

  // indicate that server expects a buffer from client.
  reply->setSendMoreData(TRUE);

  // set SPInfo state to INVOKED_GETROWS for error checking purposes.
  // Also helps debugging.
  if (sp->getSPInfoState() != SPInfo::INVOKED) {
    return SQLUDR_ERROR;
  }
  sp->setSPInfoState(SPInfo::INVOKED_GETROWS);

  // now wait for IO completion. Once IO completes
  // necessary updates to spInfo are already performed.
  int result = PerformWaitedReplyToClient(udrGlobals, *dataStream);

  // reset SpInfo state back to INVOKED state.
  if (sp->getSPInfoState() != SPInfo::INVOKED_GETROWS) {
    return SQLUDR_ERROR;
  }
  sp->setSPInfoState(SPInfo::INVOKED);

  return result;
}

void SpInfoGetNextRow(char *rowData, int tableIndex, SQLUDR_Q_STATE *queue_state) {
  UdrGlobals *udrGlobals = UDR_GLOBALS;

  const char *moduleName = "SPInfo::SpInfoGetNextRow";

  doMessageBox(udrGlobals, TRACE_SHOW_DIALOGS, udrGlobals->showInvoke_, moduleName);

  // Assumption here that there is always one SP when table mapping
  // UDR is used.
  SPInfo *sp = udrGlobals->getCurrSP();
  if (!sp) {
    *queue_state = SQLUDR_Q_CANCEL;
    return;
  }

  // Access  SQL buffer that corresponds to table index
  SqlBuffer *getSqlBuf = sp->getReqSqlBuffer(tableIndex);
  if (getSqlBuf == NULL) {
    // Perform a waited reply to client requesting for more data,
    int status = sendReqBufferWaitedReply(udrGlobals, sp, tableIndex);
    if (status == SQLUDR_ERROR) {
      *queue_state = SQLUDR_Q_CANCEL;
      return;
    }

    // expect sql buffer to be populated in spinfo now. So get it.
    getSqlBuf = sp->getReqSqlBuffer(tableIndex);

    if (getSqlBuf == NULL) {
      // Something is wrong this time.
      *queue_state = SQLUDR_Q_CANCEL;
      return;
    }
  }

  down_state downState;
  tupp requestRow;
  NABoolean endOfData = getSqlBuf->moveOutSendOrReplyData(TRUE,        // [IN] sending? (vs. replying)
                                                          &downState,  // [OUT] queue state
                                                          requestRow,  // [OUT] new data tupp_descriptor
                                                          NULL,        // [OUT] new ControlInfo area
                                                          NULL,        // [OUT] new diags tupp_descriptor
                                                          NULL);       // [OUT] new stats area

  if (endOfData)  // indicates no more tupples in the buffer.
  {
    // Check if the client already indicated that the current buffer
    // was the last buffer. If it is last buffer, return.
    if (sp->isLastReqSqlBuffer(tableIndex)) {
      *queue_state = SQLUDR_Q_EOD;
      return;
    }

    // As a safety measure, memset the sql buffer since we expect new
    // data in sql buffer after performing waited send below.
    memset(getSqlBuf, '\0', sp->getRequestBufferSize());

    // Perform a waited reply to client requesting for more data,
    int status = sendReqBufferWaitedReply(udrGlobals, sp, tableIndex);
    if (status == SQLUDR_ERROR) {
      *queue_state = SQLUDR_Q_CANCEL;
      return;
    }

    // Now get the new buffer.
    getSqlBuf = sp->getReqSqlBuffer(tableIndex);
    if (getSqlBuf == NULL) {
      *queue_state = SQLUDR_Q_CANCEL;
      return;
    }

    // Extract the row again.
    endOfData = getSqlBuf->moveOutSendOrReplyData(TRUE,        // [IN] sending? (vs. replying)
                                                  &downState,  // [OUT] queue state
                                                  requestRow,  // [OUT] new data tupp_descriptor
                                                  NULL,        // [OUT] new ControlInfo area
                                                  NULL,        // [OUT] new diags tupp_descriptor
                                                  NULL);

    if (endOfData) {
      // It is possible for client to return a empty sql buffer and
      // and indicate that it is the last buffer. In this case, return
      // EOD. before that check for any last set of rows if any.
      if (sp->isLastReqSqlBuffer(tableIndex)) {
        *queue_state = SQLUDR_Q_EOD;
        return;
      }

      // we just got the buffer, it cannot be empty unless
      // it is last buffer indication.
      *queue_state = SQLUDR_Q_CANCEL;
      return;
    }
  }

  memset(rowData, '\0', sp->getInputRowLength(tableIndex));
  memcpy(rowData, requestRow.getDataPointer(), sp->getInputRowLength(tableIndex));

  *queue_state = SQLUDR_Q_MORE;
  return;
}

int sendEmitWaitedReply(UdrGlobals *udrGlobals, SPInfo *sp, SqlBuffer *emitSqlBuffer, NABoolean includesEOD) {
  int result = 0;
  const char *moduleName = "sendEmitWaitedReply";

  NABoolean traceInvokeDataAreas = false;
  if (udrGlobals->verbose_ && udrGlobals->showInvoke_ && udrGlobals->traceLevel_ >= TRACE_DATA_AREAS)
    traceInvokeDataAreas = true;

  if (traceInvokeDataAreas) {
    ServerDebug("");
    ServerDebug("[UdrServ (%s)] EMIT SQL Buffer", moduleName);

    displaySqlBuffer(emitSqlBuffer, sp->getReplyBufferSize());
  }

  // get datastream
  UdrServerDataStream *dataStream = sp->getDataStream();

  // construct a reply with flags indicating continue request
  // needed.
  UdrDataBuffer *reply = new (*dataStream, sp->getReplyBufferSize())
      UdrDataBuffer(sp->getReplyBufferSize(), UdrDataBuffer::UDR_DATA_OUT, NULL);

  // Before copying the sqlBuffer from current heap to the one in IPC stream,
  // pack the buffer.
  emitSqlBuffer->drivePack();

  // memcpy the emitSqlBuffer into reply sqlBuffer that just got created
  // inside UdrDataBuffer. We could avoid this memcpy. Not sure how? PV
  memcpy(reply->getSqlBuffer(), emitSqlBuffer, sp->getReplyBufferSize());

  if (includesEOD) reply->setLastBuffer(TRUE);

  // indicate to client to send a continue request, we don't seem to
  // need more input data right now
  reply->setSendMoreData(FALSE);

  // set SPInfo state to INVOKED_EMITROWS for error checking purposes.
  // Also helps debugging.
  if (sp->getSPInfoState() != SPInfo::INVOKED) {
    return SQLUDR_ERROR;
  }
  sp->setSPInfoState(SPInfo::INVOKED_EMITROWS);

  if (includesEOD) {
    // return EOD to client and return without waiting
    sendDataReply(udrGlobals, *dataStream, sp);

    sp->setSPInfoState(SPInfo::LOADED);
  } else {
    // Return data back to client and wait for a new message
    // from the client. Restore context of this invocation
    // once we have a new request.
    result = PerformWaitedReplyToClient(udrGlobals, *dataStream);

    // reset SpInfo state back to INVOKED state.
    if (sp->getSPInfoState() != SPInfo::INVOKED_EMITROWS) {
      return SQLUDR_ERROR;
    }
    sp->setSPInfoState(SPInfo::INVOKED);
  }

  return result;
}

// This method is used for the older TMUDF C interface. It does not
// include an EOD entry in the SqlBuffer. We send a the buffer from
// the UDF without an EOD and ask for a continue message. The caller
// of the language manger "invokeRoutine()" method then sends the
// EOD in a separate reply.
int SpInfoEmitRow(char *rowData, int tableIndex, SQLUDR_Q_STATE *queue_state) {
  UdrGlobals *udrGlobals = UDR_GLOBALS;

  const char *moduleName = "SPInfo::SpInfoEmitRow";

  doMessageBox(udrGlobals, TRACE_SHOW_DIALOGS, udrGlobals->showInvoke_, moduleName);

  // Assumption here that there is always one SP when table mapping
  // UDR is used.
  SPInfo *sp = udrGlobals->getCurrSP();
  if (!sp) {
    *queue_state = SQLUDR_Q_CANCEL;
    return SQLUDR_ERROR;
  }

  // Access emit SQL buffer that corresponds to table index
  SqlBuffer *emitSqlBuffer = sp->getEmitSqlBuffer(tableIndex);

  // if SQLUDR_Q_EOD is received as the very first emit, there
  // is nothing much to do. We could avoid allocating emitSqlbuffer.
  if ((emitSqlBuffer == NULL) && (*queue_state == SQLUDR_Q_EOD)) return SQLUDR_SUCCESS;

  // If SQLUDR_Q_EOD is received and emitSqlBuffer is partially filled,
  // then send the emitSqlBUffer to client in awaited call. Note that
  // there is no need to insert a Q_NO_DATA tupple in the buffer.
  // Q_NO_DATA tupple is set in the final reply from workTM().
  if ((emitSqlBuffer != NULL) && (*queue_state == SQLUDR_Q_EOD) && (emitSqlBuffer->getTotalTuppDescs() > 0)) {
    int status = sendEmitWaitedReply(udrGlobals, sp, emitSqlBuffer, FALSE);

    if (status == SQLUDR_ERROR) {
      *queue_state = SQLUDR_Q_CANCEL;
      return SQLUDR_ERROR;
    }

    return SQLUDR_SUCCESS;
  }

  if (*queue_state == SQLUDR_Q_EOD) {
    return SQLUDR_SUCCESS;
  }

  // If we reach here means, atleast one row is available to be emitted.
  if (emitSqlBuffer == NULL) {
    // allocate one and set it back in SPInfo.
    NAHeap *udrHeap = udrGlobals->getUdrHeap();
    emitSqlBuffer = (SqlBuffer *)udrHeap->allocateMemory(sp->getReplyBufferSize());

    if (emitSqlBuffer == NULL) {
      *queue_state = SQLUDR_Q_CANCEL;
      return SQLUDR_ERROR;
    }

    emitSqlBuffer->driveInit(sp->getReplyBufferSize(), FALSE, SqlBuffer::NORMAL_);
    emitSqlBuffer->bufferInUse();
    sp->setEmitSqlBuffer(emitSqlBuffer, tableIndex);
  }

  // Allocate a row inside replyBuffer.
  char *replyData = NULL;
  ControlInfo *replyControlInfo = NULL;
  NABoolean replyRowAllocated = allocateReplyRow(udrGlobals,
                                                 *emitSqlBuffer,         // [IN]  A reply buffer
                                                 sp->getParentIndex(),   // [IN]  Identifies the request queue entry
                                                 sp->getReplyRowSize(),  // [IN]  Length of reply row
                                                 replyData,              // [OUT] The allocated reply row
                                                 replyControlInfo,       // [OUT] The allocated ControlInfo entry
                                                 ex_queue::Q_OK_MMORE    // [IN]  Q_OK_MMORE, Q_NO_DATA, Q_SQLERROR
  );

  if (!replyRowAllocated) {
    // Since buffer is full send this buffer off to client and then continue.
    int status = sendEmitWaitedReply(udrGlobals, sp, emitSqlBuffer, FALSE);

    if (status == SQLUDR_ERROR) {
      *queue_state = SQLUDR_Q_CANCEL;
      return SQLUDR_ERROR;
    }

    // Now that we got continue message back from client, lets continue
    // filling up emitSqlbuffer again.
    emitSqlBuffer->driveInit(sp->getReplyBufferSize(), FALSE, SqlBuffer::NORMAL_);

    replyRowAllocated = allocateReplyRow(udrGlobals,
                                         *emitSqlBuffer,         // [IN]  A reply buffer
                                         sp->getParentIndex(),   // [IN]  Identifies the request queue entry
                                         sp->getReplyRowSize(),  // [IN]  Length of reply row
                                         replyData,              // [OUT] The allocated reply row
                                         replyControlInfo,       // [OUT] The allocated ControlInfo entry
                                         ex_queue::Q_OK_MMORE    // [IN]  Q_OK_MMORE, Q_NO_DATA, Q_SQLERROR
    );

    if (!replyRowAllocated) {
      *queue_state = SQLUDR_Q_CANCEL;
      return SQLUDR_ERROR;
    }
  }
  memcpy(replyData, rowData, sp->getReplyRowSize());

  /*
  This piece of code emits every token, for testing purposes.
  // Since buffer is full send this buffer off to client and then continue.
  int status = sendEmitWaitedReply(udrGlobals, sp, emitSqlBuffer);

  if(status == SQLUDR_ERROR)
  {
    *queue_state = SQLUDR_Q_CANCEL;
    return SQLUDR_ERROR;
  }

  // Now that we got continue message back from client, lets continue
  // filling up emitSqlbuffer again.
  emitSqlBuffer->driveInit(sp->getReplyBufferSize(), FALSE, SqlBuffer::NORMAL_);
  */
  return SQLUDR_SUCCESS;
}

// This method is used for the TMUDF C++ and Java interfaces. It can also be
// used to send an EOD row, which is included in the last reply buffer of
// regular results. The EOD call is done by the caller of the UDF code,
// not the UDF code itself.
void SpInfoEmitRowCpp(char *rowData, int tableIndex, SQLUDR_Q_STATE *queue_state) {
  UdrGlobals *udrGlobals = UDR_GLOBALS;

  const char *moduleName = "SPInfo::SpInfoEmitRowCpp";

  doMessageBox(udrGlobals, TRACE_SHOW_DIALOGS, udrGlobals->showInvoke_, moduleName);

  // Assumption here that there is always one SP when table mapping
  // UDR is used.
  SPInfo *sp = udrGlobals->getCurrSP();
  if (!sp) throw tmudr::UDRException(38900, "Missing SPInfo for this UDF");

  LmRoutine *routine = static_cast<LmRoutine *>(sp->getLMHandle());

  // Access emit SQL buffer that corresponds to table index
  SqlBuffer *emitSqlBuffer = sp->getEmitSqlBuffer(tableIndex);

  // Allocate a row inside replyBuffer.
  char *replyData = NULL;
  ControlInfo *replyControlInfo = NULL;
  NABoolean replyRowAllocated = FALSE;
  int numRetries = 0;

  while (!replyRowAllocated) {
    if (emitSqlBuffer == NULL) {
      // allocate one and set it back in SPInfo.
      NAHeap *udrHeap = udrGlobals->getUdrHeap();
      emitSqlBuffer = (SqlBuffer *)udrHeap->allocateMemory(sp->getReplyBufferSize());

      if (emitSqlBuffer == NULL) throw tmudr::UDRException(38900, "Unable to allocate an emitRow SqlBuffer");

      emitSqlBuffer->driveInit(sp->getReplyBufferSize(), FALSE, SqlBuffer::NORMAL_);
      emitSqlBuffer->bufferInUse();
      sp->setEmitSqlBuffer(emitSqlBuffer, tableIndex);
    }

    if (*queue_state != SQLUDR_Q_EOD)
      replyRowAllocated = allocateReplyRow(udrGlobals,
                                           *emitSqlBuffer,         // [IN]  A reply buffer
                                           sp->getParentIndex(),   // [IN]  Identifies the request queue entry
                                           sp->getReplyRowSize(),  // [IN]  Length of reply row
                                           replyData,              // [OUT] The allocated reply row
                                           replyControlInfo,       // [OUT] The allocated ControlInfo entry
                                           ex_queue::Q_OK_MMORE);  // [IN]  Q_OK_MMORE, Q_NO_DATA, Q_SQLERROR
    else
      replyRowAllocated = allocateEODRow(udrGlobals, *emitSqlBuffer, sp->getParentIndex());

    if (!replyRowAllocated)
      if (numRetries++ < 1) {
        // Since buffer is full send this buffer off to client and then continue.
        int status = sendEmitWaitedReply(udrGlobals, sp, emitSqlBuffer, FALSE);

        if (status == SQLUDR_ERROR) throw tmudr::UDRException(38900, "Error in sending result buffer from TMUDF");

        // Now that we got continue message back from client, lets continue
        // filling up emitSqlbuffer again.
        emitSqlBuffer->driveInit(sp->getReplyBufferSize(), FALSE, SqlBuffer::NORMAL_);
      } else
        throw tmudr::UDRException(38900, "Unable to allocate %d bytes in result SqlBuffer of size %d",
                                  (int)sp->getReplyBufferSize(), (int)sp->getReplyBufferSize());
  }

  if (routine && routine->getLanguage() == COM_LANGUAGE_CPP) {
    LmRoutineCppObj *cppRoutine = static_cast<LmRoutineCppObj *>(routine);

    if (cppRoutine->getInvocationInfo()->getDebugFlags() & tmudr::UDRInvocationInfo::VALIDATE_WALLS)
      cppRoutine->validateWalls();
  }

  if (replyData)
    memcpy(replyData, rowData, sp->getReplyRowSize());
  else if (*queue_state == SQLUDR_Q_EOD) {
    // if the UDR signals EOD then send the partially filled buffer up
    int status = sendEmitWaitedReply(udrGlobals, sp, emitSqlBuffer, TRUE);

    if (status == SQLUDR_ERROR) throw tmudr::UDRException(38900, "Error in sending buffer with EOD from TMUDF");
  }
}

// C native methods called from Java TMUDFs via JNI
extern "C" {

JNIEXPORT void JNICALL Java_org_trafodion_sql_udr_UDR_SpInfoGetNextRowJava(JNIEnv *jni, jobject, jbyteArray rowData,
                                                                           jint tableIndex, jobject queueState) {
  LmLanguageManagerJava *javaLm = UDR_GLOBALS->getJavaLM();
  SQLUDR_Q_STATE queueStateLocal = SQLUDR_Q_MORE;
  jbyte *rowElems = jni->GetByteArrayElements(rowData, NULL);

  SpInfoGetNextRow(reinterpret_cast<char *>(rowElems), (int)tableIndex, &queueStateLocal);
  // free the JNI row buffer and copy back the data read
  jni->ReleaseByteArrayElements(rowData, rowElems, 0);

  // pass the queue state back to the Java caller
  jni->SetIntField(queueState, static_cast<jfieldID>(javaLm->getUdrQueueStateField()),
                   static_cast<jint>(queueStateLocal));
}

JNIEXPORT void JNICALL Java_org_trafodion_sql_udr_UDR_SpInfoEmitRowJava(JNIEnv *jni, jobject, jbyteArray rowData,
                                                                        jint tableIndex, jobject queueState) {
  SQLUDR_Q_STATE queueStateLocal = SQLUDR_Q_MORE;
  jbyte *rowElems = jni->GetByteArrayElements(rowData, NULL);

  SpInfoEmitRowCpp(reinterpret_cast<char *>(rowElems), (int)tableIndex, &queueStateLocal);
  // free the JNI row buffer without copying back any data
  jni->ReleaseByteArrayElements(rowData, rowElems, JNI_ABORT);
}

}  // end extern "C"
