
#ifndef SCRATCHFILECONN_H
#define SCRATCHFILECONN_H
/******************************************************************************
 *
 * File:         ScratchFileConnection.h
 *
 * Description:  This class is used to define all scratchfile related
 *               scheduler events
 * Created:      04/01/02
 * Language:     C++
 * Status:       $State: Exp $
 *
 *
 *
 *
 ******************************************************************************
 */

#include "ScratchFile.h"
#include "common/Ipc.h"
class ex_queue;
#include "executor/ExScheduler.h"
class ScratchFile;

class ScratchFileConnection : public IpcConnection {
 public:
  ScratchFileConnection(int index, ScratchFile *sf, ExSubtask *eventHandler, IpcEnvironment *env, ex_tcb *tcb,
                        char *eye = (char *)eye_SCRATCH_FILE_CONNECTION);
  ~ScratchFileConnection();
  void ioStarted();
  void ioStopped();
  WaitReturnStatus wait(IpcTimeout timeout, UInt32 *eventConsumed = NULL, IpcAwaitiox *ipcAwaitiox = NULL);
  void ioError();
  short isIoOutstanding() { return ioOutstanding_; }
  NABoolean isWriteIO() { return isWriteIO_; }
  void setWriteIO(NABoolean isWrite) { isWriteIO_ = isWrite; }
  // The following methods are pure virtual methods in the base class
  // and therefore need to be redefined. They will abort the program
  // if called, though. Use only the methods above this comment.
  void send(IpcMessageBuffer *);
  void receive(IpcMessageStreamBase *);
  int numQueuedSendMessages();
  int numQueuedReceiveMessages();
  void populateDiagsArea(ComDiagsArea *&, CollHeap *);

 private:
  // pointer back to the ScratchFile object
  ScratchFile *scratchFile_;
  ExSubtask *eventHandler_;
  ex_tcb *callingTcb_;
  short ioOutstanding_;
  NABoolean isWriteIO_;  // Indicates if read or write IO
  int fileIndex_;      // Corresponds to a perticular open on the scratch file among multiple opens.
};
#endif
