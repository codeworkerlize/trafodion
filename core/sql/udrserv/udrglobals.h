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
#ifndef _UDRGLOBALS_H_
#define _UDRGLOBALS_H_

/* -*-C++-*-
******************************************************************************
*
* File:         udrglobals.h
* Description:  Class declaration for UDR globals
*
* Created:      4/10/2001
* Language:     C++
*
*
*
*
******************************************************************************
*/

#include <time.h>
#include "common/ComSmallDefs.h"
#include "common/ComSqlId.h"
#include "UdrExeIpc.h"
#include "UdrStreams.h"
#include "cli/sqlcli.h"
#include "common/charinfo.h"

#define MAXSERVERNAME 12

// Defines for Max lengths for current user name and current role names stored in
// UdrGlobals and used in filling up the UDRINFO struct to be passed to UDF.
// On NT, role name is same as user name.  Length consists of 30 bytes for domain
// name, 1 for the backslash character and 20 for the user name.
// On NSK, user name is big enough to accommodate LDAP name (128 bytes). Role
// name is mapped to a Guardian user id of the form GROUP.USER (8 + 1 + 8 bytes)
#define MAX_USER_NAME_LENGTH ComSqlId::MAX_LDAP_USER_NAME_LEN
#define MAX_ROLE_NAME_LENGTH ComSqlId::MAX_GUARDIAN_USER_ALIAS_LEN

//
// Forward declarations
//
#include "LmError.h"
#include "common/ComCGroup.h"
class LmLanguageManager;
class LmLanguageManagerJava;
class LmJavaOptions;
class LmLanguageManagerC;
class NAHeap;
class SPList;
class SPInfo;

/////////////////////////////////////////////////////////////
// class UdrGlobals
/////////////////////////////////////////////////////////////
class UdrGlobals {
 public:
  UdrGlobals(NAHeap *udrheap = NULL, NAHeap *ipcheap = NULL);

  NAHeap *getUdrHeap() const { return udrHeap_; }
  NAHeap *getIpcHeap() const { return ipcHeap_; }

  IpcEnvironment *getIpcEnvironment() const { return ipcEnv_; }

  NAList<UdrServerReplyStream *> &getReplyStreams() { return replyStreams_; }
  void addReplyStream(UdrServerReplyStream *stream) { replyStreams_.insert(stream); }

  GuaReceiveControlConnection *getControlConnection() const { return ctrlConn_; }

  SPList *getSPList() { return spList_; }

  void resetAllStats();
  void displayStats(ostream &out, int indent);

  NABoolean getCommandLineMode() const { return commandLineMode_; }
  void setCommandLineMode(NABoolean b) { commandLineMode_ = b; }

  LmLanguageManagerJava *getJavaLM() const { return javaLanguageManager_; }

  LmLanguageManagerJava *getOrCreateJavaLM(LmResult &result,    // OUT
                                           ComDiagsArea *diags  // OUT
  );

  LmLanguageManagerC *getCLM() const { return cLanguageManager_; }

  LmLanguageManagerC *getOrCreateCLM(LmResult &result,    // OUT
                                     ComDiagsArea *diags  // OUT
  );
  void destroyCLM();

  LmLanguageManager *getOrCreateLM(LmResult &result, ComRoutineLanguage language, ComDiagsArea *diags);

  LmLanguageManager *getLM(ComRoutineLanguage language) const;

  // In some scenarios it is necessary to detach from the JVM and shut
  // it down. These functions provide the interface. destroyJavaLM()
  // only shuts down the LM but leaves the JVM
  // alive. destroyJavaLMAndJVM() does that too, but also brings down
  // the JVM.
  void destroyJavaLM();
  void destroyJavaLMAndJVM();

  LmJavaOptions *getJavaOptions();

  CharInfo::CharSet getIsoMapping() { return isoMapping_; }

  SPInfo *getCurrSP() { return currSP_; }
  void setCurrSP(SPInfo *sp) { currSP_ = sp; }

  const char *getCurrentUserName() const { return currentUserName_; }
  const char *getSessionUserName() const { return sessionUserName_; }
  const char *getCurrentRoleName() const { return currentRoleName_; }

  // return a non-const reference of the cgroup
  ComCGroup &getCurrCGroup() { return currCGroup_; }

  // global variables...

  int cliSqlViolation_;    // True - SPJ with NO SQL attribute issued SQL call
  int cliXactViolation_;   // True - SPJ issued a BEGIN, COMMIT, or ABORT
  int cliSqlError_;        // True - SQL Error occurred while in SPJ
  int cliXactWasAborted_;  // True - Transaction abort occurred while in SPJ

  IpcMessageObjSize currentMsgSize_;  // Size of current request messaqe

  NABoolean exitPrintPid_;  // True - Print NT Pid on End message at shutdown

  ComUInt16 gNumOpeners_;  // For initial release either 0 or 1.
                           // Future: Number of Openers when multithreading

  GuaProcessHandle gOpenerPhandle_[UDRMAXOPENERS_V100];
  // The NSK process handle of each opener.
  // See /executor/udrexeipc.h for maximum

#ifdef UDR_MULTIPLE_CONTEXTS
  SQLCTX_HANDLE initCliContextHandle_;  // There must be an initial CLI context
                                        // or CLI calls will return an error.
#endif                                  // UDR_MULTIPLE_CONTEXTS

  NABoolean logFileProvided_;  // True - Trace Facility log file has been
                               // provided by user
  time_t ltime_;               // Used to provide Udr Server start and
                               // stop time in logging.

  long nextUniqueIdentifier_;  // To generate process-wide unique numbers

  int objectCount_;  // Used during message processing to identify
                       // number of objects in a message

#ifdef UDR_MULTIPLE_CONTEXTS
  SQLCTX_HANDLE prevCliContextHandle_;
  // Used to preserve context during CLI calls
#endif  // UDR_MULTIPLE_CONTEXTS

  int replyCount_;    // reply count since start of Server
  int requestCount_;  // request count since start of Server

  char serverName_[MAXSERVERNAME];  // Value of 'Udr Server' for
                                    // logging messages

  NABoolean showInvoke_;  // True - activate logging for invoke message
  NABoolean showLoad_;    // True - activate logging for load message
  NABoolean showMain_;    // True - activate logging for main message
                          //  processing loop
  NABoolean showSPInfo_;  // True - activate logging for SPInfo class
  NABoolean showUnload_;  // True - activate logging for unload message

  int traceLevel_;  // Trace Facility - level of trace.
                      // See udrdefs.h for values

  // Logging for Result Set related messages
  NABoolean showRSLoad_;      // True - activate logging for RS load msg
  NABoolean showRSFetch_;     // True - activate logging for RS invoke msg
  NABoolean showRSContinue_;  // True - activate logging for RS continue msg
  NABoolean showRSClose_;     // True - activate logging for RS close msg
  NABoolean showRSUnload_;    // True - activate logging for RS unload msg

  // Flag to activate detailed trace reporting and logging
  NABoolean verbose_;

  // Statistics for all UDRs
  long numReqUDR_;
  long numErrUDR_;
  long numReqSP_;
  long numErrSP_;
  long numReqDataSP_;
  long numErrDataSP_;

  long numReqLoadSP_;
  long numReqInvokeSP_;
  long numReqContinueSP_;
  long numReqUnloadSP_;
  long numErrLoadSP_;
  long numErrInvokeSP_;
  long numErrContinueSP_;
  long numErrUnloadSP_;

  long numErrLMCall_;
  long numErrCLICall_;
  long numTotalSPs_;
  long numCurrSPs_;
  long numTotalRSets_;
  long numCurrRSets_;

  // Statistics for result sets
  long numReqRSLoad_;
  long numReqRSFetch_;
  long numReqRSContinue_;
  long numReqRSClose_;
  long numReqRSUnload_;
  long numErrRSLoad_;
  long numErrRSFetch_;
  long numErrRSContinue_;
  long numErrRSClose_;
  long numErrRSUnload_;

 private:
  NAHeap *udrHeap_;  // pointer to heap for process duration storage
  NAHeap *ipcHeap_;  // pointer to heap for message duration storage

  IpcEnvironment *ipcEnv_;                 // IpcEnvironment of this Server process
  GuaReceiveControlConnection *ctrlConn_;  // Initial conn from client

  SPList *spList_;  // List of SPInfo's

  // List of streams that have work to do
  NAList<UdrServerReplyStream *> replyStreams_;

  LmLanguageManagerJava *javaLanguageManager_;
  LmJavaOptions *javaOptions_;

  LmLanguageManagerC *cLanguageManager_;

  // commandLineMode_ will be TRUE if a command-line operation is
  // being performed, as opposed to this instance running inside an
  // MXUDR server.
  NABoolean commandLineMode_;

  // =_SQL_MX_ISO_MAPPING DEFINE value
  CharInfo::CharSet isoMapping_;

  // currently active SPInfo
  SPInfo *currSP_;

  // current user name to be passed to udrinfo struct
  char currentUserName_[MAX_USER_NAME_LENGTH + 1];

  // session user name to be passed to udrinfo struct
  char sessionUserName_[MAX_USER_NAME_LENGTH + 1];

  // session user name to be passed to udrinfo struct
  char currentRoleName_[MAX_ROLE_NAME_LENGTH + 1];

  // Linux cgroup of this process
  ComCGroup currCGroup_;

};  // UdrGlobals

#endif  // _UDRGLOBALS_H_
