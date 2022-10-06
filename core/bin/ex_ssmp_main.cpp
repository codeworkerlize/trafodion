
/* -*-C++-*-
*****************************************************************************
*
* File:         ex_ssmp_main.cpp
* Description:  This is the main program for SSMP. SQL stats merge process.
*
* Created:      05/08/2006
* Language:     C++
*
*****************************************************************************
*/
#include "common/Platform.h"
#ifdef _DEBUG
#include <fstream>
#include <iostream>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#endif
#include <errno.h>
#include "ExCextdecs.h"
#include "ex_ex.h"
#include "common/Ipc.h"
#include "cli/Globals.h"
#include "runtimestats/SqlStats.h"

#include "runtimestats/ssmpipc.h"
#include "runtimestats/rts_msg.h"
#include "porting/PortProcessCalls.h"
#include "seabed/ms.h"
#include "seabed/fs.h"
#include "seabed/pctl.h"
extern void my_mpi_fclose();
#include "common/SCMVersHelp.h"
DEFINE_DOVERS(mxssmp)

void runServer(int argc, char **argv);

void processAccumulatedStatsReq(SsmpNewIncomingConnectionStream *ssmpMsgStream, SsmpGlobals *ssmpGlobals);

int main(int argc, char **argv) {
  dovers(argc, argv);
  msg_debug_hook("mxssmp", "mxssmp.hook");

  try {
    file_init(&argc, &argv);
    file_mon_process_startup(true);
  } catch (SB_Fatal_Excep &e) {
    SQLMXLoggingArea::logExecRtInfo(__FILE__, __LINE__, e.what(), 0);
    exit(1);
  }

  atexit(my_mpi_fclose);
  // setup log4cxx, need to be done here so initLog4cxx can have access to
  // process information since it is needed to compose the log name
  // the log4cxx log name for this ssmp process  will be
  // based on this process' node number as suffix sscp_<nid>.log
  QRLogger::instance().setModule(QRLogger::QRL_SSMP);
  QRLogger::instance().initLog4cplus("log4cplus.trafodion.ssmp.config");

  // Synchronize C and C++ output streams
  ios::sync_with_stdio();
#ifdef _DEBUG
  // Redirect stdout and stderr to files named in environment
  // variables
  const char *stdOutFile = getenv("SQ_SSCP_STDOUT");
  const char *stdErrFile = getenv("SQ_SSCP_STDERR");
  int fdOut = -1;
  int fdErr = -1;

  if (stdOutFile && stdOutFile[0]) {
    fdOut = open(stdOutFile, O_WRONLY | O_APPEND | O_CREAT | O_SYNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (fdOut >= 0) {
      fprintf(stderr, "[Redirecting MXUDR stdout to %s]\n", stdOutFile);
      fflush(stderr);
      dup2(fdOut, fileno(stdout));
    } else {
      fprintf(stderr, "*** WARNING: could not open %s for redirection: %s.\n", stdOutFile, strerror(errno));
    }
  }

  if (stdErrFile && stdErrFile[0]) {
    fdErr = open(stdErrFile, O_WRONLY | O_APPEND | O_CREAT | O_SYNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (fdErr >= 0) {
      fprintf(stderr, "[Redirecting MXUDR stderr to %s]\n", stdErrFile);
      fflush(stderr);
      dup2(fdErr, fileno(stderr));
    } else {
      fprintf(stderr, "*** WARNING: could not open %s for redirection: %s.\n", stdErrFile, strerror(errno));
    }
  }

  runServer(argc, argv);

  if (fdOut >= 0) {
    close(fdOut);
  }
  if (fdErr >= 0) {
    close(fdErr);
  }
#else
  runServer(argc, argv);
#endif
  return 0;
}

void runServer(int argc, char **argv) {
  int shmId;
  StatsGlobals *statsGlobals = (StatsGlobals *)shareStatsSegment(shmId);
  int r = 0;
  while (statsGlobals == NULL && ++r < 10) {  // try 9 more times if the shared segement is not available
    DELAY(100);                               // delay for 1 sec.
    statsGlobals = (StatsGlobals *)shareStatsSegment(shmId);
  }
  if (statsGlobals == NULL) {
    char tmbuf[64];
    time_t now;
    struct tm *nowtm;
    now = time(NULL);
    nowtm = localtime(&now);
    strftime(tmbuf, sizeof tmbuf, "%Y-%m-%d %H:%M:%S ", nowtm);

    cout << tmbuf << "SSCP didn't create/initialize the RMS shared segment"
         << ", SSMP exited\n";
    NAExit(0);
  }

  CliGlobals *cliGlobals = CliGlobals::createCliGlobals(FALSE);
  statsGlobals->setSsmpPid(cliGlobals->myPin());
  statsGlobals->setSsmpTimestamp(cliGlobals->myStartTime());
  short error = statsGlobals->openStatsSemaphore(statsGlobals->getSsmpProcSemId());
  ex_assert(error == 0, "Error in opening the semaphore");

  cliGlobals->setStatsGlobals(statsGlobals);
  cliGlobals->setSharedMemId(shmId);

  // Handle possibility that the previous instance of MXSSMP died
  // while holding the stats semaphore.  This code has been covered in
  // a manual unit test, but it is not possible to cover this easily in
  // an automated test.
  if (statsGlobals->getSemPid() != -1) {
    NAProcessHandle prevSsmpPhandle((SB_Phandle_Type *)statsGlobals->getSsmpProcHandle());
    prevSsmpPhandle.decompose();
    if (statsGlobals->getSemPid() == prevSsmpPhandle.getPin()) {
      NAProcessHandle myPhandle;
      myPhandle.getmine();
      myPhandle.decompose();
      int error = statsGlobals->releaseAndGetStatsSemaphore(statsGlobals->getSsmpProcSemId(), (pid_t)myPhandle.getPin(),
                                                            (pid_t)prevSsmpPhandle.getPin());
      ex_assert(error == 0, "releaseAndGetStatsSemaphore() returned error");

      statsGlobals->releaseStatsSemaphore(statsGlobals->getSsmpProcSemId(), (pid_t)myPhandle.getPin());
    }
  }

  XPROCESSHANDLE_GETMINE_(statsGlobals->getSsmpProcHandle());

  NAHeap *ssmpHeap = cliGlobals->getExecutorMemory();

  IpcEnvironment *ssmpIpcEnv =
      new (ssmpHeap) IpcEnvironment(ssmpHeap, cliGlobals->getEventConsumed(), FALSE, IPC_SQLSSMP_SERVER, FALSE, TRUE);

  SsmpGlobals *ssmpGlobals = new (ssmpHeap) SsmpGlobals(ssmpHeap, ssmpIpcEnv, statsGlobals);

  // Currently open $RECEIVE with 2048
  SsmpGuaReceiveControlConnection *cc = new (ssmpHeap) SsmpGuaReceiveControlConnection(ssmpIpcEnv, ssmpGlobals, 2048);

  ssmpIpcEnv->setControlConnection(cc);

  while (TRUE) {
    ssmpGlobals->work();
  }
}
