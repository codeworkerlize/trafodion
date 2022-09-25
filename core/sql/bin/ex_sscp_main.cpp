/**********************************************************************
// @@@ START COPYRIGHT @@@
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// @@@ END COPYRIGHT @@@
**********************************************************************/
/* -*-C++-*-
*****************************************************************************
*
* File:         ex_sscp_main.cpp
* Description:  This is the main program for SSCP. SQL stats control process The process
*				does the following:
*				- Creates the shared segment
*               . Handle messages from SSMP
*
* Created:      04/17/2006
* Language:     C++
*
*****************************************************************************
*/
#include "Platform.h"
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
#include "Ipc.h"
#include "Globals.h"
#include "SqlStats.h"
#include "memorymonitor.h"
#include "sscpipc.h"
#include "rts_msg.h"
#include "ex_stdh.h"
#include "ExStats.h"
#include "PortProcessCalls.h"
#include <sys/ipc.h>
#include <sys/shm.h>
#include "seabed/ms.h"
#include "seabed/fs.h"
extern void my_mpi_fclose();
#include "SCMVersHelp.h"
DEFINE_DOVERS(mxsscp)
void  runServer(Int32 argc, char **argv);


Int32 main(Int32 argc, char **argv)
{
  dovers(argc, argv);
  msg_debug_hook("mxsscp", "mxsscp.hook");
  try {
    file_init_attach(&argc, &argv, TRUE, (char *)"");
  }
  catch (SB_Fatal_Excep &e) {
    SQLMXLoggingArea::logExecRtInfo(__FILE__, __LINE__, e.what(), 0);
    exit(1);
  }

  try {
    file_mon_process_startup(true);
  }
  catch (SB_Fatal_Excep &e) {
    SQLMXLoggingArea::logExecRtInfo(__FILE__, __LINE__, e.what(), 0);
    exit(1);
  }

  atexit(my_mpi_fclose);
  // setup log4cxx, need to be done here so initLog4cxx can have access to
  // process information since it is needed to compose the log name
  // the log4cxx log name for this ssmp process  will be
  // based on this process' node number sscp_<nid>.log
  QRLogger::instance().setModule(QRLogger::QRL_SSCP);
  QRLogger::instance().initLog4cplus("log4cplus.trafodion.sscp.config");

  // Synchronize C and C++ output streams
  ios::sync_with_stdio();

#ifdef _DEBUG
  // Redirect stdout and stderr to files named in environment
  // variables
  const char *stdOutFile = getenv("SQ_SSCP_STDOUT");
  const char *stdErrFile = getenv("SQ_SSCP_STDERR");
  Int32 fdOut = -1;
  Int32 fdErr = -1;

  if (stdOutFile && stdOutFile[0])
  {
    fdOut = open(stdOutFile,
                 O_WRONLY | O_APPEND | O_CREAT | O_SYNC,
                 S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (fdOut >= 0)
    {
      fprintf(stderr, "[Redirecting MXSSCP stdout to %s]\n", stdOutFile);
      fflush(stderr);
      dup2(fdOut, fileno(stdout));
    }
    else
    {
      fprintf(stderr, "*** WARNING: could not open %s for redirection: %s.\n",
              stdOutFile, strerror(errno));
    }
  }

  if (stdErrFile && stdErrFile[0])
  {
    fdErr = open(stdErrFile,
                 O_WRONLY | O_APPEND | O_CREAT | O_SYNC,
                 S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (fdErr >= 0)
    {
      fprintf(stderr, "[Redirecting MXUDR stderr to %s]\n", stdErrFile);
      fflush(stderr);
      dup2(fdErr, fileno(stderr));
    }
    else
    {
      fprintf(stderr, "*** WARNING: could not open %s for redirection: %s.\n",
              stdErrFile, strerror(errno));
    }
  }
  runServer(argc, argv);

  if (fdOut >= 0)
  {
    close(fdOut);
  }
  if (fdErr >= 0)
  {
    close(fdErr);
  }
#else
  runServer(argc, argv);
#endif
  return 0;
}

void runServer(Int32 argc, char **argv)
{
  Int32 shmid;
  StatsGlobals *statsGlobals = NULL;
  void *statsGlobalsAddr;
  NABoolean createStatsGlobals = FALSE;
  CliGlobals *cliGlobals = CliGlobals::createCliGlobals(FALSE);
  char tmbuf[64];
  time_t now;
  struct tm *nowtm;

  long maxSegSize = STATS_MAX_SEG_SIZE;
  char *envSegSize = getenv("RMS_SHARED_SEG_SIZE_MB");
  if (envSegSize)
  {
    maxSegSize = (long) str_atoi(envSegSize, str_len(envSegSize));
    if (maxSegSize < 32)
      maxSegSize = 32;
    maxSegSize *= 1024 * 1024;
  }
  long maxStatsSegSize = maxSegSize;
#ifdef USE_RMS_SHARED_SEGMENT_FOR_SHARED_CACHE
  // The maxSegSize now holds accumulated size for all segments
  // (stats, shared cache, and maybe more to come).
  // The shared cache area is to be located after the RMS area.
  long maxMetaCacheSegSize = SHARED_CACHE_SEG_SIZE;
  envSegSize = getenv("SHARED_CACHE_SEG_SIZE");
  if (envSegSize)
  {
    maxMetaCacheSegSize = (long) str_atoi(envSegSize, str_len(envSegSize));
    maxMetaCacheSegSize *= 1024 * 1024;
  }
  maxSegSize += maxMetaCacheSegSize;
#endif
  long enableHugePages = 0;
  int shmFlag = RMS_SHMFLAGS;
  char *envShmHugePages = getenv("SQ_RMS_ENABLE_HUGEPAGES");
  if (envShmHugePages != NULL)
  {
     enableHugePages = (long) str_atoi(envShmHugePages,
                         str_len(envShmHugePages));
     if (enableHugePages > 0)
       shmFlag =  shmFlag | SHM_HUGETLB;
  }
  now = time(NULL);
  nowtm = localtime(&now);
  strftime(tmbuf, sizeof tmbuf, "%Y-%m-%d %H:%M:%S ", nowtm);

  if ((shmid = shmget((key_t)getStatsSegmentId(),
                         0,  // size doesn't matter unless we are creating.
                         shmFlag)) == -1)
  {
    if (errno == ENOENT)
    {
      // Normal case, segment does not exist yet. Try to create.
      bool didCreate = true;
      if ((shmid = shmget((key_t)getStatsSegmentId(),
                                maxSegSize,
                                shmFlag | IPC_CREAT)) == -1)
      {
        if (enableHugePages > 0)
        {
          enableHugePages = 0;
          // try again withouf hugepages
          shmFlag =  shmFlag & ~SHM_HUGETLB;
          if ((shmid = shmget((key_t)getStatsSegmentId(),
                                    maxSegSize,
                                    shmFlag | IPC_CREAT)) == -1)
            didCreate = false;
        }
        else 
          didCreate = false;
      }

      if (didCreate)
      {
        cout << tmbuf 
             << " RMS Shared segment id = " 
             << shmid << ", key = " 
             << (key_t)getStatsSegmentId() ;
        if (enableHugePages > 0)
          cout << ", created with huge pages support." << endl;
        else
          cout << ", created without huge pages support." << endl;

        createStatsGlobals = TRUE;
      }
      else
      {
        cout << tmbuf
             << " Shmget failed, key = "
             << getStatsSegmentId()
             <<", Error code: "
             << errno
             << " ("
             << strerror(errno)
             << ")" << endl;
        exit(errno);
      }
    } // if ENOENT (i.e., attempting creation.)
  }
  else
  {
     cout << tmbuf << " RMS Shared segment exists, attaching to it, shmid="<< shmid << ", key=" << (key_t)getStatsSegmentId() << "\n";
  }
  if ((statsGlobalsAddr = shmat(shmid, getRmsSharedMemoryAddr(), SHM_RND))
		== (void *)-1)
  {
    cout << tmbuf << "Shmat failed, shmid=" <<shmid << ", key=" << (key_t) getStatsSegmentId() << ", Error code : "  << errno << "(" << strerror(errno) << ")\n";
    exit(errno);
  }
  gRmsSharedMemoryAddr_ = statsGlobalsAddr;
  char *statsGlobalsStartAddr = (char *)statsGlobalsAddr;
  if (createStatsGlobals)
  {
     short envType = StatsGlobals::RTS_GLOBAL_ENV;
     statsGlobals = new (statsGlobalsStartAddr)
             StatsGlobals((void *)statsGlobalsAddr, envType, maxStatsSegSize);
     cliGlobals->setSharedMemId(shmid);
     // We really should not squirrel the statsGlobals pointer away like
     // this until the StatsGloblas is initialized, but
     // statsGlobals->init() needs it ......
     cliGlobals->setStatsGlobals(statsGlobals);
     statsGlobals->init();
  }
  else
  {
    statsGlobals = (StatsGlobals *)statsGlobalsAddr;
    cliGlobals->setSharedMemId(shmid);
    cliGlobals->setStatsGlobals(statsGlobals);
  }
  // call getenv once per process
  Int32 rmsDeadLockSecs;
  char *envRmsDeadLockSecs = getenv("RMS_DEADLOCK_TOLERATE_SECS");
  if (envRmsDeadLockSecs) {
     rmsDeadLockSecs = atoi(envRmsDeadLockSecs);
     if (rmsDeadLockSecs < 1)
        rmsDeadLockSecs = 1;
     statsGlobals->setRmsDeadLockSecs(rmsDeadLockSecs);
  }
  char *envDumpRmsDeadLock = getenv("RMS_DEADLOCK_PROCESS_DUMP");
  if ((envDumpRmsDeadLock != NULL) &&
      (strcmp(envDumpRmsDeadLock, "N") == 0))
     statsGlobals->setDumpRmsDeadLockProcess(FALSE);
  else
     statsGlobals->setDumpRmsDeadLockProcess(TRUE);
        
  unsigned short dmlLockArraySize;
  char *envDMLLockArraySize = getenv("DML_LOCK_ARRAY_SIZE");
  if (envDMLLockArraySize) {
    dmlLockArraySize = atoi(envDMLLockArraySize);
    if (dmlLockArraySize < MIN_LOCK_ARRAY_SIZE) {
      dmlLockArraySize = MIN_LOCK_ARRAY_SIZE;
    } else if (dmlLockArraySize > MAX_LOCK_ARRAY_SIZE) {
      dmlLockArraySize = MAX_LOCK_ARRAY_SIZE;
    }
    statsGlobals->setDMLLockArraySize(dmlLockArraySize);
  }
  unsigned short ddlLockArraySize;
  char *envDDLLockArraySize = getenv("DDL_LOCK_ARRAY_SIZE");
  if (envDDLLockArraySize) {
    ddlLockArraySize = atoi(envDDLLockArraySize);
    if (ddlLockArraySize < MIN_LOCK_ARRAY_SIZE) {
      ddlLockArraySize = MIN_LOCK_ARRAY_SIZE;
    } else if (ddlLockArraySize > MAX_LOCK_ARRAY_SIZE) {
      ddlLockArraySize = MAX_LOCK_ARRAY_SIZE;
    }
    statsGlobals->setDDLLockArraySize(ddlLockArraySize);
  }

  XPROCESSHANDLE_GETMINE_(statsGlobals->getSscpProcHandle());
  NAHeap *sscpHeap = cliGlobals->getExecutorMemory();
  IpcEnvironment  *sscpIpcEnv = new (sscpHeap) IpcEnvironment(sscpHeap, cliGlobals->getEventConsumed(),
      FALSE, IPC_SQLSSCP_SERVER, FALSE, TRUE);

  SscpGlobals *sscpGlobals = NULL;

  sscpGlobals = new (sscpHeap) SscpGlobals(sscpHeap, statsGlobals);

  // Initialize ObjectCache after the semaphore is setup 
  // RMS Shared segment can be turned off completely.
  // However, epochCache is not made optional though it can be turned off. 
  // Hence, ObjectEpochCache is obtained here just to initialize it 
  // in RMS Heap
  ObjectEpochCache *epochCache = statsGlobals->getObjectEpochCache();

  // Currently open $RECEIVE with 256
  SscpGuaReceiveControlConnection *cc =
	 new(sscpHeap) SscpGuaReceiveControlConnection(sscpIpcEnv,
					  sscpGlobals,
                                           256);
  sscpIpcEnv->setControlConnection(cc);
  while (TRUE)
  {
     while (cc->getConnection() == NULL)
      cc->wait(IpcInfiniteTimeout);

#ifdef _DEBUG_RTS
    cerr << "No. of Requesters-1 "  << cc->getNumRequestors() << " \n";
#endif
    while (cc->getNumRequestors() > 0)
    {
      sscpIpcEnv->getAllConnections()->waitOnAll(IpcInfiniteTimeout);
    } // Inner while
  }
}  // runServer




