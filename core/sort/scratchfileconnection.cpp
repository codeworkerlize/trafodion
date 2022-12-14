
/******************************************************************************
*
* File:         ScratchFileConnection.h
* RCS:          $Id: ScratchFileConnection.h,v 1.3.16.3 1998/07/08 21:47:20 KS
diqui Exp
$
*
* Description:  This class is used to define all scratchfile related
*               scheduler events
* Created:          04/01/02
* Modified:     $Author:
* Language:     C++
* Status:       $State: Exp $
*
*
*
*
******************************************************************************
*/

#define AEVENT 1

#include "ScratchFileConnection.h"

#include "executor/ex_ex.h"

ScratchFileConnection::ScratchFileConnection(int index, ScratchFile *sf, ExSubtask *eventHandler, IpcEnvironment *env,
                                             ex_tcb *tcb, char *eye)
    : IpcConnection(env, IpcProcessId(), eye) {
  scratchFile_ = sf;
  fileIndex_ = index;
  eventHandler_ = eventHandler;
  ioOutstanding_ = FALSE;
  callingTcb_ = tcb;
  isWriteIO_ = TRUE;
}

ScratchFileConnection::~ScratchFileConnection() {
  if (ioOutstanding_) ioOutstanding_ = FALSE;
  setState(INITIAL);
}

void ScratchFileConnection::ioStarted() {
  setState(RECEIVING);
  ioOutstanding_ = TRUE;
}

void ScratchFileConnection::ioStopped() {
  setState(INITIAL);
  if (ioOutstanding_) {
    ioOutstanding_ = FALSE;
    isWriteIO_ = FALSE;
    getEnvironment()->getAllConnections()->bumpCompletionCount();
  }
}

WaitReturnStatus ScratchFileConnection::wait(IpcTimeout timeout, UInt32 *eventConsumed, IpcAwaitiox *ipcAwaitiox) {
  NABoolean ipcAwaitioxCompleted = ipcAwaitiox != NULL;
  if (ipcAwaitioxCompleted) ipcAwaitioxCompleted = ipcAwaitiox->getCompleted();
  ex_assert((ipcAwaitioxCompleted == FALSE),
            "Internal error: IPC AWAITIOX has not been implemented for scratch files");  // Reminder
  //   printf("Calling ScratchFileConnectionWait ..\n");
  RESULT error = SCRATCH_SUCCESS;
  ex_assert(timeout != IpcInfiniteTimeout, "Cannot wait forever on an Sort request.");

  error = scratchFile_->checkScratchIO(fileIndex_, timeout);  // Check if Sort IO completed

  if (error == IO_NOT_COMPLETE)  // Awaitiox timedout try later
  {
    return WAIT_OK;  // Let the scheduler know that nothing completed yet
  }

  if (eventConsumed != NULL) *eventConsumed |= AEVENT;

  if ((error == SCRATCH_SUCCESS) || (error == IO_COMPLETE))  // The IO completed
  {
    eventHandler_->scheduleAndNoteCompletion();  // wake up the scheduler
    return WAIT_OK;
  }
  if (error) {
    // checkScratchIO returned an error. Save it in the scratch file
    // so Sort can process this error when it's work method is scheduled
    // again.

    scratchFile_->setPreviousError(fileIndex_, error);
    eventHandler_->scheduleAndNoteCompletion();
  }
  return WAIT_OK;
}

void ScratchFileConnection::send(IpcMessageBuffer *) { ex_assert(0, "Should never call SqlTableConnection::send()"); }

void ScratchFileConnection::receive(IpcMessageStreamBase *) {
  ex_assert(0, "Should never call SqlTableConnection::receive()");
}

int ScratchFileConnection::numQueuedSendMessages() {
  ex_assert(0, "Should never call SqlTableConnection::numQueuedSendMessages()");
  return 0;
}

int ScratchFileConnection::numQueuedReceiveMessages() {
  ex_assert(0, "Should never call SqlTableConnection::numQueuedReceiveMessages()");
  return 0;
}

void ScratchFileConnection::populateDiagsArea(ComDiagsArea *&, CollHeap *) {
  ex_assert(0, "Should never call SqlTableConnection::populateDiagsArea()");
}
