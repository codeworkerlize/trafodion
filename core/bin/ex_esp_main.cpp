
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ex_esp_main.cpp
 * Description:  ESP main program and related methods
 *
 *
 * Created:      1/22/96
 * Language:     C++
 *
 *
 *
 *****************************************************************************
 */

#include "cli/ExSqlComp.h"
#include "cli/Globals.h"
#include "comexe/ComTdb.h"
#include "common/ComSqlId.h"
#include "common/ComUser.h"
#include "common/Int64.h"
#include "common/NAExit.h"
#include "common/Platform.h"
#include "common/cextdecs.h"
#include "executor/ex_esp_frag_dir.h"
#include "executor/ex_exe_stmt_globals.h"
#include "executor/ex_send_bottom.h"
#include "executor/ex_split_bottom.h"
#include "executor/ex_stdh.h"
#include "executor/ex_tcb.h"
#include "exp/ExpError.h"
#include "porting/PortProcessCalls.h"
#include "runtimestats/SqlStats.h"

#define psecure_h_including_section
#define psecure_h_security_psb_get_
#include <errno.h>
#include <pthread.h>


static pthread_t gv_main_thread_id;

#include "seabed/fs.h"
#include "seabed/ms.h"
extern void my_mpi_fclose();
#include "common/SCMVersHelp.h"
DEFINE_DOVERS(tdm_arkesp)

#include "cli/Context.h"
#include "common/NAStdlib.h"
#include "parser/StmtCompilationMode.h"


// -----------------------------------------------------------------------
// ESP control connection, handle system messages
// -----------------------------------------------------------------------

#include <sys/syscall.h>

#include "common/zsysc.h"

#include "qmscommon/QRLogger.h"


class EspGuaControlConnection : public GuaReceiveControlConnection {
 public:
  EspGuaControlConnection(IpcEnvironment *env, ExEspFragInstanceDir *espFragInstanceDir, short receiveDepth = 4000,
                          IpcThreadInfo *threadInfo = NULL, GuaReceiveFastStart *guaReceiveFastStart = NULL)
      : GuaReceiveControlConnection(env, receiveDepth, eye_ESP_GUA_CONTROL_CONNECTION, threadInfo,
                                    guaReceiveFastStart) {
    espFragInstanceDir_ = espFragInstanceDir;
  }

  virtual void actOnSystemMessage(short messageNum, IpcMessageBufferPtr sysMsg, IpcMessageObjSize sysMsgLen,
                                  short clientFileNumber, const GuaProcessHandle &clientPhandle,
                                  GuaConnectionToClient *connection);

 private:
  ExEspFragInstanceDir *espFragInstanceDir_;

  virtual NABoolean fakeErrorFromNSK(short errorFromNSK, GuaProcessHandle *clientPhandle);

  NABoolean getErrorDefine(char *defineName, ExFragId &targetFragId, short &targetCpu, int &targetSegment);

  // Cannot do in-place initialization of static const integral
  // member data in Visual C++.  See MS Knowledge base
  // article 241569.  No need to workaround this, because
  // we only need these members in non-WINNT builds.
};

// -----------------------------------------------------------------------
// An object that holds a new connection, created by a Guardian open
// system message, until the first application message comes in
// -----------------------------------------------------------------------

class EspNewIncomingConnectionStream : public IpcMessageStream {
 public:
  EspNewIncomingConnectionStream(IpcEnvironment *env, ExEspFragInstanceDir *espFragInstanceDir_,
                                 IpcThreadInfo *threadInfo);
  virtual ~EspNewIncomingConnectionStream();
  virtual void actOnSend(IpcConnection *connection);
  virtual void actOnReceive(IpcConnection *connection);

 private:
  ExEspFragInstanceDir *espFragInstanceDir_;
};

// forward declaration
void DoEspStartup(int argc, char **argv, IpcEnvironment &env, ExEspFragInstanceDir &fragInstanceDir,
                  IpcThreadInfo *threadInfo, GuaReceiveFastStart *guaReceiveFastStart);

int runESP(int argc, char **argv, GuaReceiveFastStart *guaReceiveFastStart = NULL);

typedef void *stopCatchHandle;
typedef void *stopCatchContext;
typedef void (*stopCatchFunction)(stopCatchContext);
extern "C" _priv _resident stopCatchHandle STOP_CATCH_REGISTER_(stopCatchFunction, stopCatchContext);

_priv _resident void stopCatcher(stopCatchContext scContext);

// -----------------------------------------------------------------------
// -----------  ESP main program for NT or NSK with C runtime ------------
// -----------------------------------------------------------------------

int main(int argc, char **argv) {
  dovers(argc, argv);

  try {
    file_init(&argc, &argv);
  } catch (SB_Fatal_Excep &e) {
    exit(1);
  }
  try {
    file_mon_process_startup(true);

    // Initialize log4cxx
    QRLogger::initLog4cplus(QRLogger::QRL_ESP);
  } catch (SB_Fatal_Excep &e) {
    SQLMXLoggingArea::logExecRtInfo(__FILE__, __LINE__, e.what(), 0);
    exit(1);
  }

  atexit(my_mpi_fclose);

  IdentifyMyself::SetMyName(I_AM_ESP);

  msg_debug_hook("arkesp", "esp.hook");

  // Leave this commented out unless you need to debug the argument
  // cracking code below and can't rely on the -debug option.  This
  // allows the esp to put up a dialog box and then you can manually
  // force the esp into debug.
  if (getenv("SQL_MSGBOX_PROCESS") != NULL) {
    MessageBox(NULL, "Server: Process Launched", "tdm_arkesp", MB_OK | MB_ICONINFORMATION);
  };

  NABoolean fastStart = TRUE;
  int currArg = 1;
  while (currArg < argc && fastStart == TRUE) {
    if (strcmp("-noespfaststart", argv[currArg]) == 0) fastStart = FALSE;
    currArg++;
  }
  short retCode;
  if (fastStart) {
    GuaReceiveFastStart *guaReceiveFastStart = new GuaReceiveFastStart();
    retCode = runESP(argc, argv, guaReceiveFastStart);
  } else
    retCode = runESP(argc, argv);
  ENDTRANSACTION();

  return retCode;
}

GuaReceiveFastStart::GuaReceiveFastStart() {
  _bcc_status status;
  int bufferAddr;
  readUpdate_ = FALSE;
  awaitiox_ = FALSE;
  replyx_ = FALSE;
  awaitioxError_ = 0;
  fileGetReceiveInfo_ = FALSE;
  zsys_ddl_smsg_open_reply_def openReply;
  // openError_ -- not altered
  openError_ = BFILE_OPEN_((char *)"$RECEIVE", 8, &receiveFile_, 0, 0, 1, 4000, 0);
  // open_ -- not altered
  open_ = TRUE;
  if (openError_ == 0) {
    status = BSETMODE(receiveFile_, 74, -1);

    if (_status_ne(status)) {
      // this is bad
      ABORT("Internal error on setmode($receive)");
    }
    // readUpdateStatus_ -- not altered
    readUpdateStatus_ = BREADUPDATEX(receiveFile_, (char *)&readBuffer_[0], 80);
    // readUpdate_ -- altered
    readUpdate_ = TRUE;
    // bufferData_ -- altered
    bufferData_ = NULL;
    if (_status_eq(readUpdateStatus_))  // Did not get an error on READUPDATEX
    {
      // awaitioxStatus_ -- not altered
      awaitioxStatus_ =
          BAWAITIOX(&receiveFile_, (void **)&bufferAddr, &awaitioxCountTransferred_, (SB_Tag_Type *)&ioTag_,
                    100 * 60 * 10);  // 10 minutes
      // fileGetInfoError_ -- not altered
      fileGetInfoError_ = BFILE_GETINFO_(receiveFile_, &awaitioxError_);
      // awaitiox_ -- altered
      awaitiox_ = TRUE;
      if (fileGetInfoError_ == 0 && awaitioxError_ == 6) {
        fileGetReceiveInfoError_ = BFILE_GETRECEIVEINFO_((FS_Receiveinfo_Type *)&receiveInfo_);

        // fileGetReceiveInfo_ -- altered
        fileGetReceiveInfo_ = TRUE;
        if (fileGetReceiveInfoError_ == 0) {
          openReply.z_msgnumber = ZSYS_VAL_SMSG_OPEN;
          openReply.z_openid = 0;  // GuaReceiveControlConnection id_ must be zero
          // replyxstatus_ -- not altered
          replyxstatus_ =
              BREPLYX((IpcMessageBufferPtr)&openReply, 4, &replyxCountWritten_, receiveInfo_.replyTag_, GuaOK);
          // replyx_ == altered
          replyx_ = TRUE;
        }
      }
    }
  }
}

// -----------------------------------------------------------------------
// Waiter thread
//
// This thread has only one task to do, and that is to wait on all
// connections. It is only used in a multi-threaded IPC environment.
// The reason is as follows:
// - In multi-threaded IPC, each IpcMessageStreamBase or derived object
//   is associated with a thread.
// - When a send or receive on such a message stream completes, the
//   IpcThreadInfo object gets notified and sets a condition variable,
//   waking up the thread, if needed.
// - Threads that own message streams can wait on IPC connections with
//   a zero timeout, but they must wait on their condition variable and
//   not on connection(s) if they want to use an infinite timeout.
// - The main thread owns plenty of message streams, so it needs to wait
//   on its condition variable when it is busy and we are in a multi-
//   threaded IPC environment.
// - Therefore, this additional waiter thread takes on the role of
//   waiting on all IPC connections. This waiter thread owns no message
//   streams and therefore does not need to wait on a condition variable
//   (it does not even have one).
// -----------------------------------------------------------------------

static void *waiterThreadStart(void *arg) {
  IpcAllConnections *allc = ((IpcEnvironment *)arg)->getAllConnections();
  NABoolean timeout;
  long prevWaitTime = 0;

  while (1)
    allc->waitOnAll(IpcInfiniteTimeout,
                    TRUE,  // called by ESP main
                    &timeout, &prevWaitTime);

  // never reaches here
  return NULL;
}

// -----------------------------------------------------------------------
// Startup handling of ESP
// -----------------------------------------------------------------------

int runESP(int argc, char **argv, GuaReceiveFastStart *guaReceiveFastStart) {
  // initialize ESP global data
  StatsGlobals *statsGlobals;

  XCONTROLMESSAGESYSTEM(XCTLMSGSYS_SETRECVLIMIT, XMAX_SETTABLE_RECVLIMIT_H);
  CliGlobals *cliGlobals = NULL;
  cliGlobals = CliGlobals::createCliGlobals(TRUE);  // TRUE indicates a non-master process (WAIT on LREC)
  if (cliGlobals == NULL)                           // Sanity check
    NAExit(1);                                      // Abend
  int shmid;
  statsGlobals = shareStatsSegmentWithRetry(shmid);
  if (statsGlobals == NULL) {
    exit(0);
  }
  cliGlobals->setSharedMemId(shmid);
  // int numCliCalls = cliGlobals->incrNumOfCliCalls();
  cliGlobals->setIsESPProcess(TRUE);
  NAHeap *espExecutorHeap = cliGlobals->getExecutorMemory();
  // must create default context after set IpcEnvironment in CliGlobals first
  // because context's ExSqlComp object needs IpcEnvironment
  cliGlobals->initiateDefaultContext();
  NAHeap *espIpcHeap = cliGlobals->getIpcHeap();
  // Check if it is multiThreaded ESP server
  int currArg = 1;
  NABoolean multiThreaded = FALSE;
  while (currArg < argc) {
    if (strcmp("-multiThread", argv[currArg]) == 0) {
      multiThreaded = TRUE;
      break;
    }
    currArg++;
  }
  IpcEnvironment *ipcEnvPtr = cliGlobals->getEnvironment();
  IpcThreadInfo mainThreadInfo("ESP main thread", 0, GETTID());

  if (multiThreaded) {
    ipcEnvPtr->setMultiThreaded();
    espIpcHeap->setThreadSafe();
    espExecutorHeap->setThreadSafe();
    ipcEnvPtr->addThreadInfo(&mainThreadInfo);
  }

  // After CLI globals are initialized but before we begin ESP message
  // processing, have the CLI context set its user identity based on
  // the OS user identity.
  ContextCli *context = cliGlobals->currContext();
  ex_assert(context, "Invalid context pointer");
  context->initializeUserInfoFromOS();

  ExEspFragInstanceDir espFragInstanceDir(cliGlobals, espExecutorHeap, (StatsGlobals *)statsGlobals,
                                          (multiThreaded ? &mainThreadInfo : NULL));

  ExEspControlMessage espIpcControlMessage(&espFragInstanceDir, ipcEnvPtr, espIpcHeap);

  // handle startup (command line args, control connection)
  DoEspStartup(argc, argv, *ipcEnvPtr, espFragInstanceDir, (multiThreaded ? &mainThreadInfo : NULL),
               guaReceiveFastStart);
  // the control message stream talks through the control connection
  espIpcControlMessage.addRecipient(ipcEnvPtr->getControlConnection()->getConnection());

  // start the first receive operation
  espIpcControlMessage.receive(FALSE);

  NABoolean timeout;
  long prevWaitTime = 0;
  pthread_t waiterThreadId;

  if (multiThreaded) {
    // start a waiter thread
    int retcode;
    pthread_attr_t waiterThreadAttr;

    retcode = pthread_attr_init(&waiterThreadAttr);
    if (retcode != 0) {
      char errorBuf[100];

      snprintf(errorBuf, sizeof(errorBuf), "Error %d in pthread_attr_init of waiter thread", retcode);
      ex_assert(retcode == 0, errorBuf);
    }

    retcode = pthread_create(&waiterThreadId, &waiterThreadAttr, waiterThreadStart, (void *)ipcEnvPtr);
    if (retcode != 0) {
      char errorBuf[100];

      snprintf(errorBuf, sizeof(errorBuf), "Error %d in pthread_create for waiter thread", retcode);
      ex_assert(retcode == 0, errorBuf);
    }
  }

  // while there are requesters
  while (espFragInstanceDir.getNumMasters() > 0) {
    if (multiThreaded)
      // work on requests from the master executor
      while (espIpcControlMessage.hasRequest()) espIpcControlMessage.workOnReceivedMessage();

    // -----------------------------------------------------------------
    // The ESPs most important line of code: DO THE WORK
    // -----------------------------------------------------------------

    espFragInstanceDir.work(prevWaitTime);

    // -----------------------------------------------------------------
    // After we have done work, it's necessary to wait for some I/O
    // (the frag instance dir work procedure works until it is blocked).
    // -----------------------------------------------------------------

    timeout = FALSE;
    if (multiThreaded) {
      // work on requests from the master executor
      while (espIpcControlMessage.hasRequest()) espIpcControlMessage.workOnReceivedMessage();

      // For debugging: Reschedule all tasks regularly, to see
      // whether a deadlock is due to tasks that have work to
      // do but are not scheduled to run. Don't use this line
      // in normal production mode, it is for debugging only!
      // espFragInstanceDir.rescheduleAll();

      // waitOnAll() is called in a separate waiter thread, wait
      // here for completed IPC messages for this thread
      mainThreadInfo.wait();
    } else
      ipcEnvPtr->getAllConnections()->waitOnAll(30000 /*5mins*/, TRUE, &timeout,
                                                &prevWaitTime);  // TRUE means: Called by ESP main

    if (timeout && NOT espFragInstanceDir.allInstanceAreFree()) {
      QRLogger::log(CAT_SQL_ESP, LL_WARN, "Active ESP wait message timeout, timeout=%d, time=%ld.", timeout,
                    prevWaitTime);
    }
  }

  // nobody wants us anymore, right now that means that we stop
  return 0;
}

void DoEspStartup(int argc, char **argv, IpcEnvironment &env, ExEspFragInstanceDir &fragInstanceDir,
                  IpcThreadInfo *threadInfo, GuaReceiveFastStart *guaReceiveFastStart) {
  // make the compiler happy by using fragInstanceDir for something
  if (fragInstanceDir.getNumEntries() < 0) {
  }

  // interpret command line arguments
  IpcServerAllocationMethod allocMethod = IPC_ALLOC_DONT_CARE;

  int currArg = 1;

  int socketArg = 0;
  int portArg = 0;

  while (currArg < argc) {
    if (strcmp("-fork", argv[currArg]) == 0) {
      allocMethod = IPC_POSIX_FORK_EXEC;
    } else if (strcmp("-service", argv[currArg]) == 0) {
      // /etc/inetd.conf should be configured such that the "-service"
      // command line option is given
      allocMethod = IPC_INETD;
    } else if (strcmp("-guardian", argv[currArg]) == 0) {
      allocMethod = IPC_LAUNCH_GUARDIAN_PROCESS;
    } else if (strcmp("-noespfaststart", argv[currArg]) == 0)
      ;
    else if (strcmp("-debug", argv[currArg]) == 0) {
      NADebug();
    }
    currArg++;
  }

  // create control connection (open $RECEIVE in Tandemese)
  switch (allocMethod) {
    case IPC_LAUNCH_GUARDIAN_PROCESS:
    case IPC_SPAWN_OSS_PROCESS: {
      // open $RECEIVE with a receive depth of 4000

      GuaReceiveControlConnection *cc =

          new (&env) EspGuaControlConnection(&env, &fragInstanceDir, 4000, threadInfo, guaReceiveFastStart);
      env.setControlConnection(cc);

      // wait for the first open message to come in
      cc->waitForMaster();
      // set initial timeout in case the master never send first plan message
      env.setIdleTimestamp();
    } break;

    default:
      // bad command line arguments again
      NAExit(-1);
  }
}

void EspGuaControlConnection::actOnSystemMessage(short messageNum, IpcMessageBufferPtr sysMsg,
                                                 IpcMessageObjSize sysMsgLen, short clientFileNumber,
                                                 const GuaProcessHandle &clientPhandle,
                                                 GuaConnectionToClient *connection) {
  switch (messageNum) {
    case ZSYS_VAL_SMSG_OPEN:
      if (initialized_) {
        // This an OPEN message for a connection that isn't the
        // initial control connection. Create a new message stream and
        // attach it to the newly created connection.
        EspNewIncomingConnectionStream *newStream =
            new (getEnv()->getHeap()) EspNewIncomingConnectionStream(getEnv(), espFragInstanceDir_,
                                                                     // make sure to associate the
                                                                     // thread that owns the control
                                                                     // connection, not the current
                                                                     // thread
                                                                     threadInfo_);

        ex_assert(connection != NULL, "Must create connection for open sys msg");
        newStream->addRecipient(connection);
        newStream->receive(FALSE);

        // now abandon the new object, it will find its way to the right
        // send bottom TCB on its own
        // (a memory leak would result if the client would open our $RECEIVE
        // w/o sending corresponding ESP level open requests)
      }
      break;

    case ZSYS_VAL_SMSG_CPUDOWN:
    case ZSYS_VAL_SMSG_REMOTECPUDOWN:
    case ZSYS_VAL_SMSG_CLOSE:
    case ZSYS_VAL_SMSG_NODEDOWN:
      // Somebody closed us or went down. Was it master executor?
      // Note that GuaReceiveControlConnection::getConnection returns
      // the master executor connection.
      if (getConnection() == connection) {
        // Master is gone, stop this process and let the OS cleanup.
        if (getEnv()->getLogEspGotCloseMsg()) {
          /*
          Coverage notes: to test this code in a dev regression requires
          changing $TRAF_VAR/ms.env, so I made a manual test on
          May 11, 2012 to verify this code.
          */
          char myName[20];
          memset(myName, '\0', sizeof(myName));
          getEnv()->getMyOwnProcessId(IPC_DOM_GUA_PHANDLE).toAscii(myName, sizeof(myName));
          char buf[500];
          char *sysMsgName = NULL;
          switch (messageNum) {
            case ZSYS_VAL_SMSG_CPUDOWN:
              sysMsgName = (char *)"CPUDOWN";
              break;
            case ZSYS_VAL_SMSG_REMOTECPUDOWN:
              sysMsgName = (char *)"REMOTECPUDOWN";
              break;
            case ZSYS_VAL_SMSG_CLOSE:
              sysMsgName = (char *)"CLOSE";
              break;
            case ZSYS_VAL_SMSG_NODEDOWN:
              sysMsgName = (char *)"NODEDOWN";
              break;
          }
          str_sprintf(buf, "System %s message causes %s to exit.", sysMsgName, myName);
          SQLMXLoggingArea::logExecRtInfo(__FILE__, __LINE__, buf, 0);
        }
        getEnv()->stopIpcEnvironment();
      }
      // Otherwise, do a search thru all
      // downloaded fragment entries and check whether their
      // client is still using them. The IPC layer will wake
      // up the scheduler so the actual release can take place.
      espFragInstanceDir_->releaseOrphanEntries();
      break;

    default:
      // do nothing for all other kinds of system messages
      break;
  }  // switch

  // The parent class already handles the job of closing all connections
  // who lost their client process by failed processes, failed CPUs and
  // failed systems or networks. Check here that we die if all our
  // requestors go away, but don't die if the first system message is
  // something other than an OPEN message.
  if (getNumRequestors() == 0 AND initialized_) {
    // ABORT("Lost connection to client");
    // losing the client is not a reason to panic, the client may
    // have voluntarily decided to exit without freeing its resources
    NAExit(0);
  } else if (NOT initialized_ AND getNumRequestors() > 0) {
    // the first requestor came in
    initialized_ = TRUE;
  }
}

/////////////////////////////////////////////////////////////////////////////
//
// The next two methods support error injection.  This is controlled by
// setting either one or two defines.
//
// Any character on "fragment" part of the client side is ignored.
//
// Important limitation: this feature will not be able to selectively support
// more than one statement per ESP.

NABoolean EspGuaControlConnection::getErrorDefine(char *defineName, ExFragId &targetFragId, short &targetCpu,
                                                  int &targetSegment) {
  NABoolean fakeOpenDefineIsSet = FALSE;
  return fakeOpenDefineIsSet;
}

NABoolean EspGuaControlConnection::fakeErrorFromNSK(short errorFromNSK, GuaProcessHandle *clientPhandle) {
  NABoolean retcode = FALSE;
  // tbd - could we use getEnv here on Windows?
  return retcode;
}

// -----------------------------------------------------------------------
// methods for class EspNewIncomingConnectionStream
// -----------------------------------------------------------------------

EspNewIncomingConnectionStream::EspNewIncomingConnectionStream(IpcEnvironment *ipcEnvironment,
                                                               ExEspFragInstanceDir *espFragInstanceDir,
                                                               IpcThreadInfo *threadInfo)
    : IpcMessageStream(ipcEnvironment, IPC_MSG_SQLESP_SERVER_INCOMING, CurrEspReplyMessageVersion, 0, TRUE,
                       threadInfo) {
  espFragInstanceDir_ = espFragInstanceDir;
}

EspNewIncomingConnectionStream::~EspNewIncomingConnectionStream() {
  // nothing to do
}

void EspNewIncomingConnectionStream::actOnSend(IpcConnection *) {
  // typically the stream will never send but we use it for a send
  // when we reject extract consumer opens.
}

void EspNewIncomingConnectionStream::actOnReceive(IpcConnection *connection) {
  // check for OS errors
  if (getState() == ERROR_STATE) {
    ex_assert(FALSE, "Error while receiving first message from client");
  }

  // check for protocol errors
  bool willPassTheAssertion =
      (getType() == IPC_MSG_SQLESP_DATA_REQUEST OR getType() == IPC_MSG_SQLESP_CANCEL_REQUEST) AND getVersion() ==
      CurrEspRequestMessageVersion AND moreObjects();
  if (!willPassTheAssertion) {
    char *doCatchBugCRx = getenv("ESP_BUGCATCHER_CR_NONUMBER");
    if (!doCatchBugCRx || *doCatchBugCRx != '0') {
      connection->dumpAndStopOtherEnd(true, false);
      environment_->getControlConnection()->castToGuaReceiveControlConnection()->getConnection()->dumpAndStopOtherEnd(
          true, false);
    }
  }

  ex_assert((getType() == IPC_MSG_SQLESP_DATA_REQUEST OR getType() == IPC_MSG_SQLESP_CANCEL_REQUEST)
                    AND getVersion() == CurrEspRequestMessageVersion AND moreObjects(),
            "Invalid first message from client");

  // take a look at the type of the first object in the message
  IpcMessageObjType nextObjType = getNextObjType();
  switch (nextObjType) {
    case ESP_OPEN_HDR:
    case ESP_LATE_CANCEL_HDR: {
      ExFragKey key;
      int remoteInstNum;
      NABoolean isParallelExtract = false;

      // peek at the message header to see for whom it is
      if (nextObjType == ESP_OPEN_HDR) {
        ExEspOpenReqHeader reqHdr((NAMemory *)NULL);

        *this >> reqHdr;
        key = reqHdr.key_;
        remoteInstNum = reqHdr.myInstanceNum_;
        if (reqHdr.getOpenType() == ExEspOpenReqHeader::PARALLEL_EXTRACT) {
          isParallelExtract = true;
        }
      } else {
        // note that the late cancel request may or may not
        // arrive as the first request (only in the former case
        // will we reach here)
        ExEspLateCancelReqHeader reqHdr((NAMemory *)NULL);

        *this >> reqHdr;
        key = reqHdr.key_;
        remoteInstNum = reqHdr.myInstanceNum_;
      }

      if (!isParallelExtract) {
        int handle = espFragInstanceDir_->findHandle(key);

        if (handle != NullFragInstanceHandle) {
          // the send bottom node # myInstanceNum of this downloaded fragment
          // is the true recipient of this open request
          ex_split_bottom_tcb *receivingTcb = espFragInstanceDir_->getTopTcb(handle);
          ex_send_bottom_tcb *receivingSendTcb = receivingTcb->getSendNode(remoteInstNum);

          // Check the connection for a co-located client, and if so,
          // tell the split bottom, because it may prefer this send
          // bottom when using skew buster uniform distribution.
          if (espFragInstanceDir_->getEnvironment()
                  ->getMyOwnProcessId(IPC_DOM_GUA_PHANDLE)
                  .match(connection->getOtherEnd().getNodeName(), connection->getOtherEnd().getCpuNum()))
            receivingTcb->setLocalSendBottom(remoteInstNum);

          // Portability note for the code above: we pass IPC_DOM_GUA_PHANDLE
          // for IpcEnvironment::getMyOwnProcessId, even though that method
          // can be called with the default param (IpcNetworkDomain
          // IPC_DOM_INVALID).  In fact it would  probably be better
          // to call the object without specifying the IpcNetworkDomain so
          // that it can decide for itself what domain it is using.
          // But there is a problem with the Windows implementation
          // of IpcEnvironment::getMyOwnProcessId, it seems to assume
          // that its domain is IPC_DOM_INTERNET and so this will
          // cause the botch of an assertion that its control connection
          // (which is type EspGuaControlConnection) can be cast to a
          // SockControlConnection.  When this problem is fixed, the
          // IPC_DOM_GUA_PHANDLE param above can be removed.  Also,
          // when this code is ported to run it a domain other than
          // "guardian", it will be necessary to fix this and to
          // fix IpcEnvironment::getMyOwnProcessId to work properly on
          // windows.

          receivingSendTcb->setClient(connection);
          receivingSendTcb->routeMsg(*this);
        } else {
          connection->dumpAndStopOtherEnd(true, false);
          ex_assert(FALSE, "entry not found, set diagnostics area and reply");
        }

      }  // normal case, not parallel extract

      else {
        // The OPEN request is from a parallel extract consumer. The
        // incoming request contains a user ID which we will compare
        // against the current user ID for this ESP.

        // NOTE: The user ID for the extract security check is
        // currently sent and compared as a C string. On Linux it is
        // possible to send and compare integers which would lead to
        // simpler code. The code to send/compare strings is still
        // used because it works on all platforms.

        char errorStr[150];

        // check if next msg is of securityInfo type.
        ex_assert(moreObjects(), "expected object not received");
        ex_assert(getNextObjType() == ESP_SECURITY_INFO, "received message for unknown message type");

        // unpack security info
        ExMsgSecurityInfo secInfo(environment_->getHeap());
        *this >> secInfo;

        // Get the auth ID of this ESP in text form and compare it
        // to the auth ID that arrived in the message. Skip this
        // step in the debug build if an environment variable is
        // set.
        NABoolean doAuthIdCheck = TRUE;
        int status = 0;
#ifdef _DEBUG
        const char *envvar = getenv("NO_EXTRACT_AUTHID_CHECK");
        if (envvar && envvar[0]) doAuthIdCheck = FALSE;
#endif
        if (doAuthIdCheck) {
          // Get user ID from ExMsgSecurityInfo -> (secUserID)
          // the user ID is the integer value made into a string
          // Convert it back into its integer value
          short userIDLen = (short)str_len(secInfo.getAuthID());
          int secUserID = str_atoi(secInfo.getAuthID(), userIDLen);

          // Get the current user ID
          int curUserID = ComUser::getSessionUser();

          // Report an error if the user ID is not valid
          if (curUserID == NA_UserIdDefault || secUserID == NA_UserIdDefault) {
            str_cpy_c(errorStr,
                      "Producer ESP could not authenticate the consumer, "
                      "no valid current user.");
            status = -1;
          }

          // Make sure user id passed in ExMsgSecurityInfo matches
          // the user id associated with the current session

#if defined(_DEBUG)
          NABoolean doDebug = (getenv("DBUSER_DEBUG") ? TRUE : FALSE);
          if (doDebug)
            printf(
                "[DBUSER:%d] ESP extract user ID: "
                "local [%d], msg [%d]\n",
                (int)getpid(), curUserID, secUserID);
#endif

          // Compare user ID, Report an error, if comparison fails
          if (curUserID != secUserID) {
            str_cpy_c(errorStr,
                      "Producer ESP could not authenticate the consumer, "
                      "user named passed in ExMsgSecurityInfo is not the "
                      "current user");
            status = -1;
          }

        }  // if (doAuthIdCheck)

        // get the split bottom TCB that matches the securityKey
        ex_split_bottom_tcb *receivingTcb = NULL;
        if (status == 0) {
          receivingTcb = espFragInstanceDir_->getExtractTop(secInfo.getSecurityKey());
          if (receivingTcb == NULL) {
            str_cpy_c(errorStr, "Producer ESP could not locate extract node");
            status = -1;
          }
        }

        // get the sendBottom TCB if not already connected to a client
        ex_send_bottom_tcb *receivingSendTcb = NULL;
        if (status == 0) {
          receivingSendTcb = receivingTcb->getConsumerSendBottom();
          if (receivingSendTcb == NULL) {
            str_cpy_c(errorStr, "Producer ESP already connected to a client");
            status = -1;
          }
        }

        // send the error message to the consumer
        if (status != 0) {
          clearAllObjects();
          setType(IPC_MSG_SQLESP_DATA_REPLY);

          NAMemory *heap = environment_->getHeap();

          IpcMessageObj *baseObj = new (heap) IpcMessageObj(IPC_SQL_DIAG_AREA, CurrEspReplyMessageVersion);
          *this << *baseObj;

          // prepare proper error message
          char phandle[100];
          MyGuaProcessHandle myHandle;
          myHandle.toAscii(phandle, 100);

          ComDiagsArea *diags = ComDiagsArea::allocate(heap);
          *diags << DgSqlCode(-EXE_PARALLEL_EXTRACT_OPEN_ERROR) << DgString0(phandle) << DgString1(errorStr);
          *this << *diags;

          diags->decrRefCount();

          send(TRUE /* TRUE indicates waited */);
        }

        // if everything okay, then make the connection
        if (status == 0) {
          receivingSendTcb->setClient(connection);
          receivingSendTcb->routeMsg(*this);
        }
      }  // parallel extract case
    }    // open or cancel header
    break;

    default:
      ex_assert(FALSE, "Invalid request for first client message");
  }  // end switch

  // self-destruct, the new connection is now handled by someone else
  addToCompletedList();
}
