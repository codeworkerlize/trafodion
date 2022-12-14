

#ifndef CMPCONNECTION__H
#define CMPCONNECTION__H

#include "arkcmp/CmpContext.h"
#include "common/CmpCommon.h"
#include "common/Int64.h"
#include "common/NAIpc.h"
#include "sqlmsg/ErrorMessage.h"

class CmpStatementISP;

#define ARKCMP_ERROR_PREFIX "*** ERROR "

enum ArkcmpErrorSeverity { NOMEM_SEV, ERROR_SEV, WARNING_SEV, INFO_SEV };

void ArkcmpDelayExit();
void ArkcmpErrorMessageBox(const char *msg, ArkcmpErrorSeverity sev = ERROR_SEV, NABoolean doExit = TRUE,
                           NABoolean doDelay = TRUE, NABoolean doCerr = TRUE);
inline void ArkcmpFatalError(const char *msg, ArkcmpErrorSeverity sev = ERROR_SEV) {
  NADumpDiags(cerr, CmpCommon::diags(), TRUE /*newline*/);
  ArkcmpErrorMessageBox(msg, sev);
}

// -----------------------------------------------------------------------
// Message stream to receive requests through executor
// -----------------------------------------------------------------------

class ExCmpMessage : public IpcMessageStream {
 public:
  ExCmpMessage(IpcEnvironment *);

  void setCmpContext(CmpContext *context);

  NABoolean hasError() { return (getState() == IpcMessageStream::ERROR_STATE); }

  virtual void actOnReceive(IpcConnection *connection);
  virtual void actOnSend(IpcConnection *connection);

  NABoolean end() { return endOfConnection_; }
  void setEnd(NABoolean t = TRUE) { endOfConnection_ = t; }

  virtual ~ExCmpMessage();

 private:
  ExCmpMessage(const ExCmpMessage &);
  ExCmpMessage &operator=(const ExCmpMessage &);

  // retrieve the CmpStatementISP from current CmpContext that was created
  // for processing the ISP request with id.
  CmpStatementISP *getISPStatement(long id);

  NABoolean endOfConnection_;
  CmpContext *cmpContext_;

};  // end of ExCmpMessage

// -----------------------------------------------------------------------
// CmpIpcEnvironment is to set up the IPC environment for arkcmp process
// -----------------------------------------------------------------------

class CmpIpcEnvironment : public IpcEnvironment {
 public:
  CmpIpcEnvironment() : IpcEnvironment() {}

  void initControl(IpcServerAllocationMethod, int sockArg = 0, int portArg = 0);

  ~CmpIpcEnvironment() {}

 private:
  CmpIpcEnvironment(const CmpIpcEnvironment &);
  CmpIpcEnvironment &operator=(const CmpIpcEnvironment &);

};  // end of CmpIpcEnvironment;

// -----------------------------------------------------------------------
// ARKCMP control connection, handles system messages
// -----------------------------------------------------------------------
#include "common/zsysc.h"

class CmpGuaControlConnection : public GuaReceiveControlConnection {
 public:
  CmpGuaControlConnection(IpcEnvironment *env, short receiveDepth = 2);

  virtual ~CmpGuaControlConnection();

 private:
  CmpGuaControlConnection(const CmpGuaControlConnection &);
  CmpGuaControlConnection &operator=(const CmpGuaControlConnection &);

};  // end of CmpGuaControlConnection

#if (defined(_DEBUG) || defined(NSK_MEMDEBUG))
#include <new>

// This NewHandler class is to setup the error handling routines
// (by calling set_new_handler) for running out of virtual memory
// errors.
// The error handling for out of memory cases might need to be
// platform dependent. Because the components arkcmp links in
// might be using system new operators ( instead of from NAMemory )
// or malloc. The error handlings have to conform to the way system
// handling errors for operator new and malloc. While on NT,
// set_new_handler and set_new_mode are used.
// They should be used as stacks of new handlers, NewHandler is a
// class to push/pop the new_handlers for for certain code segment.
// It is now used in arkcmp ( 12/17/97 ) main program only
// 1. In the beginning of main program, the new_handler will simply
//    terminate the program.
// 2. After the IPC is setup, the new_handler will send the appropriate
//    messages back to executor and exit the program.

class NewHandler {
 public:
  // The ctor takes the new_handler
  NewHandler();
  // The dtor reset the new_handler and new_mode back to the previous
  // values.
  ~NewHandler();

 private:
  NewHandler(const NewHandler &);
  const NewHandler &operator=(const NewHandler &);
};  // end of NewHandler

inline NewHandler::NewHandler() {}

inline NewHandler::~NewHandler() {}

#include "sqlcomp/NewDel.h"
typedef void (*PNH)();

class NewHandler_NSK : public NewHandler {
 public:
  // The ctor takes the new_handler and the new_mode and set them
  // as the system new_handler and new_mode.
  // a non 0 newMode will be treated as 1 when calling set_new_mode
  NewHandler_NSK(PNH handler);
  // The dtor reset the new_handler and new_mode back to the previous
  // values.
  ~NewHandler_NSK();

 private:
  PNH prevPNH_;  // previous PNH

  NewHandler_NSK(const NewHandler_NSK &);
  const NewHandler_NSK &operator=(const NewHandler_NSK &);
};  // end of NewHandler

inline NewHandler_NSK::NewHandler_NSK(PNH handler) { prevPNH_ = CmpSetNewHandler(handler); }

inline NewHandler_NSK::~NewHandler_NSK() { CmpSetNewHandler(prevPNH_); }
#endif
#endif
