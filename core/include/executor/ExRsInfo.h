/******************************************************************************
*
* File:         ExRsInfo.h
* Description:  A container class for Statment globals Result Set info
*
* Created:      October 2005
* Language:     C++
*

*
******************************************************************************
*/
#ifndef EX_RS_INFO_H
#define EX_RS_INFO_H

#include "common/Collections.h"
#include "common/ComRtUtils.h"
#include "common/ComSmallDefs.h"
#include "common/Ipc.h"
#include "common/NABoolean.h"
#include "common/Platform.h"
#include "export/NABasicObject.h"

// Contents of this file
class RsInfo;
class ExRsInfo;

// Forward declarations
class NAMemory;
class ExUdrServer;
class Statement;
class ExExeStmtGlobals;
class UdrClientControlStream;

// Some data members such as the NAArray come from outside this
// DLL. The Windows compiler generates a warning about them requiring
// a DLL interface in order to be used by ExRsInfo clients. We
// will suppress such warnings.

//------------------------------------------------------------------------
// class RsInfo
//
// This class is a container for Result Set info
// and is instantiated by ExRsInfo
//
//------------------------------------------------------------------------
class RsInfo : public NABasicObject {
  friend class ExRsInfo;

 private:
  RsInfo();
  virtual ~RsInfo();

  // Accessors
  Statement *getStatement() const { return statement_; }
  NABoolean getOpenAttempted() const { return openAttempted_; }
  NABoolean getCloseAttempted() const { return closeAttempted_; }
  NABoolean isPrepared() const { return prepared_; }
  char *getProxySyntax() const { return proxySyntax_; }

  // Mutators
  void setStatement(Statement *statement) { statement_ = statement; }
  void setOpenAttempted(NABoolean flag) { openAttempted_ = flag; }
  void setCloseAttempted(NABoolean flag) { closeAttempted_ = flag; }
  void setPrepared(NABoolean flag) { prepared_ = flag; }
  void setProxySyntax(char *proxySyntax) { proxySyntax_ = proxySyntax; }

  // Class data members
  Statement *statement_;
  NABoolean openAttempted_;
  NABoolean closeAttempted_;
  NABoolean prepared_;
  char *proxySyntax_;
};  // RsInfo

//------------------------------------------------------------------------
// class ExRsInfo
//
// This class is a wrapper around a collection of RsInfo objects.
//
//------------------------------------------------------------------------
class ExRsInfo : public NABasicObject {
 public:
  ExRsInfo();
  virtual ~ExRsInfo();

  void populate(int index, const char *proxySyntax);
  void bind(int index, Statement *statement);
  void unbind(Statement *statement);
  void setOpenAttempted(int index);
  void setCloseAttempted(int index);
  void setPrepared(int index);
  void reset();
  NABoolean statementExists(int index) const;
  NABoolean openAttempted(int index) const;
  NABoolean closeAttempted(int index) const;
  NABoolean isPrepared(int index) const;
  NABoolean getRsInfo(int position,               // IN
                      Statement *&statement,      // OUT
                      const char *&proxySyntax,   // OUT
                      NABoolean &openAttempted,   // OUT
                      NABoolean &closeAttempted,  // OUT
                      NABoolean &isPrepared)      // OUT
      const;
  int getIndex(Statement *statement) const;

  ExUdrServer *getUdrServer() const { return udrServer_; }
  const IpcProcessId &getIpcProcessId() const { return ipcProcessId_; }
  long getUdrHandle() const { return udrHandle_; }
  void setUdrServer(ExUdrServer *udrServer) { udrServer_ = udrServer; }
  void setIpcProcessId(const IpcProcessId &ipcProcessId);
  void setUdrHandle(long udrHandle) { udrHandle_ = udrHandle; }

  int getNumReturnedByLastCall() const { return numReturnedByLastCall_; }
  int getNumClosedSinceLastCall() const { return numClosedSinceLastCall_; }

  NABoolean allResultsAreClosed() const { return (numClosedSinceLastCall_ >= numReturnedByLastCall_ ? TRUE : FALSE); }

  // Methods to get and set the maximum number of RS entries this
  // object can hold.
  //
  // If the user of this object calls setNumEntries(N) to set the max,
  // the object may actually raise the limit internally and
  // getNumEntries() may return a value higher than N.
  //
  // Users of this class should not rely on this "non-exact"
  // behavior. It is a side-effect of using NAArray in the
  // implementation of this class, and may change in the future.
  void setNumEntries(ComUInt32 maxEntries);
  const int getNumEntries() const {
    int n = rsInfoArray_.getSize();
    // We return (n-1) to account for the never-used entry in slot 0
    return (n > 0 ? n - 1 : 0);
  }

  // Methods to bring the UDR server into and out of a transaction for
  // a single CALL operation that can potentially return result
  // sets. To bring the server into a transaction we will send an
  // ENTER TX request. To temporarily bring the server out we send a
  // SUSPEND TX request. To bring the server fully out we send an EXIT
  // TX request. The stmtGlobals object passed in will have its diags
  // area populated if IPC errors are encountered.
  void enterUdrTx(ExExeStmtGlobals &stmtGlobals);
  void suspendUdrTx(ExExeStmtGlobals &stmtGlobals);
  void exitUdrTx(ExExeStmtGlobals &stmtGlobals);

  UdrClientControlStream *getEnterTxStream() const { return enterTxStream_; }
  UdrClientControlStream *getExitOrSuspendStream() const { return exitOrSuspendStream_; }

  // Message streams for ENTER, EXIT, and SUSPEND TX messages will use
  // this method to inform the ExRsInfo that the message interaction
  // has completed.
  void reportCompletion(UdrClientControlStream *);

 private:
  enum TxMsgType { ENTER, SUSPEND, EXIT };

  enum TxState { IDLE, ACTIVE, SUSPENDED };

  static const char *getTxMsgTypeString(TxMsgType t) {
    switch (t) {
      case ENTER:
        return "ENTER";
      case SUSPEND:
        return "SUSPEND";
      case EXIT:
        return "EXIT";
      default:
        return ComRtGetUnknownString((int)t);
    }
  }

  static const char *getTxStateString(TxState s) {
    switch (s) {
      case IDLE:
        return "IDLE";
      case ACTIVE:
        return "ACTIVE";
      case SUSPENDED:
        return "SUSPENDED";
      default:
        return ComRtGetUnknownString((int)s);
    }
  }

  // A private method to do IPC for TX requests
  void sendTxMessage(ExExeStmtGlobals &, TxMsgType);

  ARRAY(RsInfo *) rsInfoArray_;
  ExUdrServer *udrServer_;
  IpcProcessId ipcProcessId_;
  long udrHandle_;
  TxState txState_;

  // When an ENTER TX message is sent we need to keep a pointer to the
  // stream. This allows the ENTER TX to later be abandoned if the
  // EXIT TX or SUSPEND TX cannot be successfully sent.
  UdrClientControlStream *enterTxStream_;

  // We also need a pointer to the EXIT TX or SUSPEND TX stream. This
  // allows the ExRsInfo destructor to inform the stream when the
  // ExRsInfo instance is going away.
  UdrClientControlStream *exitOrSuspendStream_;

  int numReturnedByLastCall_;
  int numClosedSinceLastCall_;

#ifdef _DEBUG
 public:
  void ExRsPrintf(const char *formatString, ...) const;
  NABoolean debugEnabled() const { return debug_; }

 private:
  NABoolean debug_;
  NABoolean txDebug_;
  void displayList() const;
#endif
};  // class ExRsInfo

#endif  // EX_RS_INFO_H
