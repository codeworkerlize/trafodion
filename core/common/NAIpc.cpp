/* -*-C++-*-
 *****************************************************************************
 *
 * File:         NAIpc.C
 * Description:  Interprocess communication among SQL/ARK processes. Defines
 *               servers, requestors, and messages.
 *
 * Created:      10/17/95
 * Language:     C++
 *
 *

 *
 *
 *****************************************************************************
 */

// -----------------------------------------------------------------------
#include "common/NAIpc.h"

#include "common/Platform.h"

// -----------------------------------------------------------------------
// A method that implements a global variable for the IPC environment
// -----------------------------------------------------------------------
IpcEnvironment *GetIpcEnv() {
  // a static (global) pointer to the environment that IPC routines need
  static IpcEnvironment *NAIpcEnvironment = NULL;

  if (NAIpcEnvironment == NULL) {
    NAIpcEnvironment = new IpcEnvironment();
  }

  return NAIpcEnvironment;
}

// -----------------------------------------------------------------------
// How many requestors do we have (if we are a server process)
// -----------------------------------------------------------------------

int GetNumRequestors() {
  IpcEnvironment *e = GetIpcEnv();
  IpcControlConnection *cc = e->getControlConnection();

  if (cc == NULL)
    return 0;
  else
    return cc->getNumRequestors();
}

// -----------------------------------------------------------------------
// methods for class NASingleServer
// -----------------------------------------------------------------------

NASingleServer::NASingleServer(ComDiagsArea **diags, CollHeap *diagsHeap, IpcServerType serverType, const char *node,
                               IpcServerAllocationMethod allocationMethod) {
  NAString serverName;

  sc_ = new IpcServerClass(GetIpcEnv(), serverType, allocationMethod);
  s_ = sc_->allocateServerProcess(diags, diagsHeap, node);
}

// -----------------------------------------------------------------------
// methods for class NAMessage
// -----------------------------------------------------------------------

NAMessage::NAMessage(IpcNetworkDomain domain) : IpcMessageStream(GetIpcEnv(), IPC_MSG_INVALID, 100, 0, TRUE) {
  // the message will be using the control connection of the server
  // check whether the control connection exists already

  IpcControlConnection *cc = GetIpcEnv()->getControlConnection();
  if (cc == NULL) {
    switch (domain) {
      case IPC_DOM_GUA_PHANDLE:

        // open $RECEIVE
        cc = new (GetIpcEnv()) GuaReceiveControlConnection(GetIpcEnv(), 5);

        // wait for the open message of the client
        while (cc->getConnection() == NULL) cc->castToGuaReceiveControlConnection()->wait(IpcInfiniteTimeout);
        break;

      case IPC_DOM_INTERNET:

        // open a connection on the stdin/stdout socket
        cc = new (GetIpcEnv()) SockControlConnection(GetIpcEnv());
        break;

      default:
        ABORT("Invalid domain specified in NAMessage::NAMessage()");
    }
    GetIpcEnv()->setControlConnection(cc);
  }

  // can't have requestors from two different domains in the same process
  // (note that it is possible to talk to servers in different domains)
  assert(domain == cc->getDomain());

  // associate the control connection with the message stream
  addRecipient(cc->getConnection());
}

NAMessage::NAMessage(NASingleServer *destination) : IpcMessageStream(GetIpcEnv(), IPC_MSG_INVALID, 100, 0, TRUE) {
  // add a single recipient to the message
  addRecipient(destination->s_->getControlConnection());
}

void NAMessage::clear() {
  // clear all objects, don't reset the communications partners, since
  // they are specified in the constructor, so we can only delete them
  // by deleting the object itself
  clearAllObjects();
}

void NAMessage::send(NABoolean wait) { IpcMessageStream::send(wait); }

void NAMessage::receive(NABoolean wait) { IpcMessageStream::receive(wait); }
