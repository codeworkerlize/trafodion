//**********************************************************************
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
//**********************************************************************/
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         Context.cpp
 * Description:  Procedures to add/update/delete/manipulate executor context.
 *
 * Created:      7/10/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "common/Platform.h"
#include "cli_stdh.h"
#include "common/NAStdlib.h"
#include "stdio.h"
#include "comexe/ComQueue.h"
#include "cli/ExSqlComp.h"
#include "executor/ex_transaction.h"
#include "common/ComRtUtils.h"
#include "executor/ExStats.h"
#include "cli/sql_id.h"
#include "ex_control.h"
#include "ExControlArea.h"
#include "ex_root.h"
#include "executor/ExExeUtil.h"
#include "ex_frag_rt.h"
#include "executor/ExExeUtil.h"
#include "common/ComExeTrace.h"
#include "exp/exp_clause_derived.h"
#include "common/ComUser.h"
#include "sqlcomp/CmpSeabaseDDLauth.h"
#include "sqlcomp/CmpSeabaseTenant.h"
#include "parser/StmtCompilationMode.h"
#include "ExCextdecs.h"
#include "export/ComMemoryDiags.h"  // ComMemoryDiags::DumpMemoryInfo()
#include "common/DllExportDefines.h"
#include "common/NAWNodeSet.h"

#include <fstream>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <algorithm>

#include "ComplexObject.h"
#include "CliMsgObj.h"
#include "UdrExeIpc.h"
#include "common/ComTransInfo.h"
#include "ExUdrServer.h"
#include "common/ComSqlId.h"
#include "common/NAUserId.h"

#include "common/stringBuf.h"
#include "common/NLSConversion.h"

#include <sys/types.h>
#include <arpa/inet.h>
#include <pwd.h>

#include "common/CmpCommon.h"
#include "arkcmp_proc.h"

#include "ExRsInfo.h"
#include "../../dbsecurity/auth/inc/dbUserAuth.h"
#include "common/ComDistribution.h"
#include "LmRoutine.h"

#include "optimizer/TriggerDB.h"
#include "seabed/sys.h"

// Printf-style tracing macros for the debug build. The macros are
// no-ops in the release build.
#ifdef _DEBUG
#include <stdarg.h>
#define StmtListDebug0(s, f)                StmtListPrintf((s), (f))
#define StmtListDebug1(s, f, a)             StmtListPrintf((s), (f), (a))
#define StmtListDebug2(s, f, a, b)          StmtListPrintf((s), (f), (a), (b))
#define StmtListDebug3(s, f, a, b, c)       StmtListPrintf((s), (f), (a), (b), (c))
#define StmtListDebug4(s, f, a, b, c, d)    StmtListPrintf((s), (f), (a), (b), (c), (d))
#define StmtListDebug5(s, f, a, b, c, d, e) StmtListPrintf((s), (f), (a), (b), (c), (d), (e))
#else
#define StmtListDebug0(s, f)
#define StmtListDebug1(s, f, a)
#define StmtListDebug2(s, f, a, b)
#define StmtListDebug3(s, f, a, b, c)
#define StmtListDebug4(s, f, a, b, c, d)
#define StmtListDebug5(s, f, a, b, c, d, e)
#endif

#include "dtm/tm.h"

#include "CmpContext.h"

static void doInitForFristContext() {
  // do somethine when first of cmpContext is Initialization
  ComAuthenticationType authType = CmpCommon::loadAuthenticationType();
  if (authType != COM_LDAP_AUTH && NOT CmpCommon::isUninitializedSeabase()) {
    ComPwdPolicySyntax syntax;
    bool skipEnvVar = ("ON" == CmpCommon::getDefaultString(TRAF_IGNORE_EXTERNAL_PASSWORD_POLICY_SYNTAX));
    CmpCommon::loadPasswordCheckSyntax(syntax, skipEnvVar);
    CmpCommon::setPasswordCheckSyntax(syntax);
  }
  CmpCommon::setAuthenticationType(authType);
}

// A helper class, used for validDDLCheckList_ in ContextCli
class CliDDLValidator : public ExGod {
 public:
  CliDDLValidator(NAHeap *heap, const char *objectName, UInt32 expectedEpoch, UInt32 expectedFlags,
                  ObjectEpochCacheEntry *oecEntry)
      : heap_(heap), objectName_(NULL), ddlValidator_(expectedEpoch, expectedFlags) {
    objectName_ = new (heap) char[strlen(objectName) + 1];
    strcpy(objectName_, objectName);
    ddlValidator_.setValidationInfo(oecEntry);
  }

  virtual ~CliDDLValidator() {
    NADELETEBASIC(objectName_, heap_);
    objectName_ = NULL;
  }

  const char *objectName() { return objectName_; }

  bool isDDLValid() { return ddlValidator_.isDDLValid(); }

 private:
  NAHeap *heap_;

  // the memory for objectName_ is owned by this object
  char *objectName_;
  ExDDLValidator ddlValidator_;
};

ContextCli::ContextCli(CliGlobals *cliGlobals, NABoolean isDefaultContext)
    // the context heap's parent is the basic executor memory
    : cliGlobals_(cliGlobals),

      exHeap_((char *)"Heap in ContextCli", cliGlobals->getExecutorMemory(),
              ((!cliGlobals->isESPProcess()) ? 524288 : 32768) /* block size */, 0 /* upperLimit */),
      diagsArea_(&exHeap_),
      timeouts_(NULL),
      timeoutChangeCounter_(0),
      validAuthID_(TRUE),
      externalUsername_(NULL),
      sqlParserFlags_(0),
      closeStatementList_(NULL),
      nextReclaimStatement_(NULL),
      closeStmtListSize_(0),
      stmtReclaimCount_(0),
      currSequence_(2),
      prevFixupSequence_(0),
      catmanInfo_(NULL),
      flags_(0),
      ssmpManager_(NULL),
      udrServerList_(NULL),
      udrRuntimeOptions_(NULL),
      udrRuntimeOptionDelimiters_(NULL),
      udrErrorChecksEnabled_(FALSE),
      sessionDefaults_(NULL),
      bdrConfigInfoList_(NULL),
      //    aqrInfo_(NULL),
      userSpecifiedSessionName_(NULL),
      sessionID_(NULL),
      sessionInUse_(FALSE),
      mxcmpSessionInUse_(FALSE),
      volatileSchemaCreated_(FALSE),
      prevStmtStats_(NULL),
      triggerDB_(NULL),
      hdfsHandleList_(NULL),
      seqGen_(NULL),
      dropInProgress_(FALSE),
      isEmbeddedArkcmpInitialized_(FALSE),
      embeddedArkcmpContext_(NULL),
      prevCmpContext_(NULL),
      ddlStmtsExecuted_(FALSE),
      execDDLOptions_(FALSE),
      numCliCalls_(0),
      jniErrorStr_(&exHeap_),
      DLockClientJNI_(NULL),
      tenantHelperJNI_(NULL),
      arkcmpArray_(&exHeap_),
      cmpContextInfo_(&exHeap_),
      cmpContextInUse_(&exHeap_),
      arkcmpInitFailed_(&exHeap_),
      trustedRoutines_(&exHeap_),
      tenantCGroup_(&exHeap_),
      numRoles_(0),
      roleIDs_(NULL),
      roleLoopLevel_(0),
      rewindRoleLevels_(FALSE),
      granteeIDs_(NULL),
      unusedBMOsMemoryQuota_(0),
      availableNodes_(NULL),
      slaName_(NULL),
      clientConnected_(FALSE),
      isTdmArkcmp_(UNKNOWN),
      profileName_(NULL),
      cliInterfaceTrigger_(NULL),
      skipCheckValidPrivs_(false),
      clientInfo_(NULL),
      ddlStmtsInSPExecuted_(FALSE) {
  triggerDB_ = new (exHeap()) TriggerDB(exHeap());
  tid_ = GETTID();
  cliSemaphore_ = new (&exHeap_) CLISemaphore();
  ipcHeap_ = new (cliGlobals_->getProcessIpcHeap()) NAHeap("IPC Context Heap", cliGlobals_->getProcessIpcHeap(),
                                                           (cliGlobals->isESPProcess() ? (2048 * 1024) : (512 * 1024)));
  if (!cliGlobals->isESPProcess())
    env_ = new (ipcHeap_) IpcEnvironment(ipcHeap_, &eventConsumed_, breakEnabled_);
  else
    env_ = new (ipcHeap_) IpcEnvironment(ipcHeap_, &eventConsumed_, FALSE, IPC_SQLESP_SERVER, TRUE);
  env_->setCliGlobals(cliGlobals_);
  exeTraceInfo_ = new (&exHeap_) ExeTraceInfo();
  // esp register IPC data trace at runESP()
  env_->registTraceInfo(exeTraceInfo_);
  // We are not worrying about context handle wrap around for now.
  // contextHandle_ is a long (32 bits) that allows us to create 4
  // billion contexts without having a conflict.
  contextHandle_ = cliGlobals_->getNextUniqueContextHandle();

  // For now, in open source, set the user to the root  user (DB__ROOT)
  databaseUserID_ = ComUser::getRootUserID();
  databaseUserName_ = new (exCollHeap()) char[MAX_DBUSERNAME_LEN + 1];
  strcpy(databaseUserName_, ComUser::getRootUserName());
  sessionUserID_ = databaseUserID_;

  tenantName_ = new (exCollHeap()) char[MAX_DBUSERNAME_LEN + 1];
  if (msg_license_multitenancy_enabled()) {
    strcpy(tenantName_, DB__SYSTEMTENANT);  // use ESGYNDB as the default tenant
    tenantID_ = SYSTEM_TENANT_ID;           // use ESGYNDB as the default tenant
  } else {
    strcpy(tenantName_, "");
    tenantID_ = 0;
  }

  slaName_ = new (exCollHeap()) char[MAX_SLA_NAME_LEN + 1];
  memset(slaName_, 0, MAX_SLA_NAME_LEN + 1);
  strcpy(slaName_, "defaultSLA");

  profileName_ = new (exCollHeap()) char[MAX_PROFILE_NAME_LEN + 1];
  memset(profileName_, 0, MAX_PROFILE_NAME_LEN + 1);
  strcpy(profileName_, "defaultProfile");

  tenantDefaultSchema_ = new (exCollHeap()) char[COL_MAX_SCHEMA_LEN + 1];
  strcpy(tenantDefaultSchema_, "");

  hostName_ = new (exCollHeap()) char[MAX_CLIENT_HOSTNAME_LEN + 1];
  strcpy(hostName_, NA_CLIENT_HOSTNAME);

  osUserName_ = new (exCollHeap()) char[MAX_CLIENT_OSNAME_LEN + 1];
  strcpy(osUserName_, NA_CLIENT_OSUSERNAME);

  ipAddress_ = new (exCollHeap()) char[INET6_ADDRSTRLEN];
  strcpy(ipAddress_, NA_CLIENT_IP_ADDRESS);

  // moduleList_ is a HashQueue with a hashtable of size 53
  // the other have default size of the hash table
  moduleList_ = new (exCollHeap()) HashQueue(exCollHeap(), 53);
  statementList_ = new (exCollHeap()) HashQueue(exCollHeap());
  descriptorList_ = new (exCollHeap()) HashQueue(exCollHeap());
  cursorList_ = new (exCollHeap()) HashQueue(exCollHeap());
  // openStatementList_ is short (TRUE?!). Use only a small
  // hash table
  openStatementList_ = new (exCollHeap()) HashQueue(exCollHeap(), 23);
  statementWithEspsList_ = new (exCollHeap()) Queue(exCollHeap());
  validDDLCheckList_ = new (exCollHeap()) Queue(exCollHeap());
  // nonAuditedLockedTableList_ is short. Use only a small
  // hash table
  nonAuditedLockedTableList_ = new (exCollHeap()) HashQueue(exCollHeap(), 23);

  trafSElist_ = new (exCollHeap()) HashQueue(exCollHeap());

  authID_ = 0;
  authToken_ = 0;
  authIDType_ = SQLAUTHID_TYPE_INVALID;

  arkcmpArray_.insertAt(
      0, new (exCollHeap()) ExSqlComp(0, exCollHeap(), cliGlobals, 0, COM_VERS_COMPILER_VERSION, NULL, env_));
  arkcmpInitFailed_.insertAt(0, arkcmpIS_OK_);
  versionOfMxcmp_ = COM_VERS_COMPILER_VERSION;
  mxcmpNodeName_ = NULL;
  indexIntoCompilerArray_ = 0;
  transaction_ = 0;
  externalUsername_ = new (exCollHeap()) char[ComSqlId::MAX_LDAP_USER_NAME_LEN + 1];
  memset(externalUsername_, 0, ComSqlId::MAX_LDAP_USER_NAME_LEN + 1);
  strcpy(externalUsername_, "NOT AVAILABLE");

  userNameChanged_ = FALSE;

  userSpecifiedSessionName_ = new (exCollHeap()) char[ComSqlId::MAX_SESSION_NAME_LEN + 1];
  setUserSpecifiedSessionName((char *)"");
  lastDescriptorHandle_ = 0;
  lastStatementHandle_ = 0;
  transaction_ = new (exCollHeap()) ExTransaction(cliGlobals, exCollHeap());

  controlArea_ = new (exCollHeap()) ExControlArea(this, exCollHeap());

  stats_ = NULL;

  defineContext_ = getCurrentDefineContext();

  catmanInfo_ = NULL;

  //  aqrInfo_ = new(exCollHeap()) AQRInfo(exCollHeap());

  volTabList_ = NULL;

  beginSession(NULL);
  mergedStats_ = NULL;

  char *envVar = getenv("RECLAIM_FREE_MEMORY_RATIO");
  if (envVar)
    stmtReclaimMinPctFree_ = atol(envVar);
  else
    stmtReclaimMinPctFree_ = 25;
  envVar = getenv("ESP_RELEASE_TIMEOUT");
  if (envVar)
    espReleaseTimeout_ = atol(envVar);
  else
    espReleaseTimeout_ = 5;

  processStats_ = cliGlobals_->getExProcessStats();
  // if we are in ESP, this espManager_ has to be NULL, see "else" part
  if (cliGlobals_->isESPProcess())
    espManager_ = NULL;
  else
    espManager_ = new (ipcHeap_) ExEspManager(env_, this);

  udrServerManager_ = new (ipcHeap_) ExUdrServerManager(env_);

  ssmpManager_ = new (ipcHeap_) ExSsmpManager(env_);

  seqGen_ = new (exCollHeap()) SequenceValueGenerator(exCollHeap());

  hdfsHandleList_ = new (exCollHeap()) HashQueue(
      exCollHeap(), 50);  // The hfsHandleList_ represents a list of distict hdfs Handles with unique hdfs port numbers
                          // and server names. Assume not more than 50 hdfsServers could be connected in the Trafodion
                          // setup.  These will get initialized the first time access is made to a particular hdfs
                          // server. This list gets cleaned up when the thread exits.

  // if this is the default context, initialize global static array elements
  // of nadefaults. See sqlcomp/nadefaults.cpp for details.
  if (isDefaultContext) initGlobalNADefaultsEntries();
}

ContextCli::~ContextCli() { NADELETE(cliSemaphore_, CLISemaphore, &exHeap_); }

void ContextCli::deleteMe() {
  ComDiagsArea *diags = NULL;

  if (cliInterfaceTrigger_ != NULL) {
    NADELETE(cliInterfaceTrigger_, ExeCliInterface, exHeap());
    cliInterfaceTrigger_ = NULL;
  }

  if (volatileSchemaCreated_) {
    // drop volatile schema, if one exists
    short rc = ExExeUtilCleanupVolatileTablesTcb::dropVolatileSchema(this, NULL, exCollHeap(), diags);
    if (rc < 0 && diags != NULL && diags->getNumber(DgSqlCode::ERROR_) > 0) {
      ComCondition *condition = diags->getErrorEntry(1);
      logAnMXEventForError(*condition, GetCliGlobals()->getEMSEventExperienceLevel());
    }
    SQL_EXEC_ClearDiagnostics(NULL);
    volatileSchemaCreated_ = FALSE;
  }

  SQL_EXEC_ClearDiagnostics(NULL);

  delete moduleList_;
  statementList_->position();
  delete statementList_;
  delete descriptorList_;
  delete cursorList_;
  delete openStatementList_;
  // cannot use 'delete statementWithEspsList_' since it is not an NABasicObject.
  statementWithEspsList_->cleanup();
  exCollHeap()->deallocateMemory((void *)statementWithEspsList_);
  // similarly with validDDLCheckList_
  doValidDDLChecks(FALSE /* just clean up */);
  exCollHeap()->deallocateMemory((void *)validDDLCheckList_);

  moduleList_ = 0;
  statementList_ = 0;
  descriptorList_ = 0;
  cursorList_ = 0;
  openStatementList_ = 0;
  statementWithEspsList_ = 0;
  validDDLCheckList_ = NULL;

  delete transaction_;
  transaction_ = 0;

  stats_ = NULL;
  authIDType_ = SQLAUTHID_TYPE_INVALID;

  if (authID_) {
    exHeap()->deallocateMemory(authID_);
    authID_ = 0;
  }

  if (authToken_) {
    exHeap()->deallocateMemory(authToken_);
    authToken_ = 0;
  }
  // Release all ExUdrServer references
  releaseUdrServers();

  // delete all trusted routines in this context
  CollIndex maxTrustedRoutineIx = trustedRoutines_.getUsedLength();

  for (CollIndex rx = 0; rx < maxTrustedRoutineIx; rx++)
    if (trustedRoutines_.used(rx)) putTrustedRoutine(rx);
  trustedRoutines_.clear();

  closeStatementList_ = NULL;
  nextReclaimStatement_ = NULL;
  closeStmtListSize_ = 0;
  stmtReclaimCount_ = 0;
  currSequence_ = 0;
  prevFixupSequence_ = 0;

  clearTimeoutData();

  // delete all CmpContexts that are pointed at from
  // the corresponding CmpContextInfo objects
  for (short i = 0; i < cmpContextInfo_.entries(); i++) {
    CmpContextInfo *cci = cmpContextInfo_[i];
    NAHeap *heap;
    CmpContext *cmpContext = cci->getCmpContext();
    heap = cmpContext->heap();
    // Set the current cmp Context as the one that is being deleted
    // because CmpContext destructor expects it that way
    cmpCurrentContext = cmpContext;
    // Only need to run this once
    if (i == 0) {
      LOGTRACE(CAT_SQL_LOCK, "Release all DML/DDL object locks");
      cmpContext->releaseAllObjectLocks();
    }
    NADELETE(cmpContext, CmpContext, heap);
    NADELETE(heap, NAHeap, &exHeap_);
    NADELETE(cci, CmpContextInfo, &exHeap_);
    cmpCurrentContext = NULL;
  }

  // Deallocate the space allocated for all pointing
  // CmpContextInfo objects.
  cmpContextInfo_.deallocate();

  // CLI context should only be destructed outside recursive CmpContext calls
  ex_assert(cmpContextInUse_.entries() == 0, "Should not be inside recursive compilation");

  for (short i = 0; i < arkcmpArray_.entries(); i++) delete arkcmpArray_[i];

  arkcmpArray_.clear();

  arkcmpInitFailed_.clear();

  if (espManager_ != NULL) NADELETE(espManager_, ExEspManager, ipcHeap_);
  if (udrServerManager_ != NULL) NADELETE(udrServerManager_, ExUdrServerManager, ipcHeap_);
  if (ssmpManager_ != NULL) NADELETE(ssmpManager_, ExSsmpManager, ipcHeap_);

  if (exeTraceInfo_ != NULL) {
    delete exeTraceInfo_;
    exeTraceInfo_ = NULL;
  }
  NADELETE(env_, IpcEnvironment, ipcHeap_);
  NAHeap *parentHeap = cliGlobals_->getProcessIpcHeap();
  NADELETE(ipcHeap_, NAHeap, parentHeap);
  DistributedLock_JNI::deleteInstance();
  disconnectHdfsConnections();
  delete hdfsHandleList_;
  hdfsHandleList_ = NULL;

  // TODO: delete for databaseUserName_ is missing; should we add it?
  // TODO: do a delete for tenantName_ as well?
}

int ContextCli::initializeSessionDefaults() {
  int rc = 0;

  /* session attributes */
  sessionDefaults_ = new (exCollHeap()) SessionDefaults(exCollHeap());

  // read defaults table, if in master exe process
  if (NOT cliGlobals_->isESPProcess()) {
    // should allow reading of global session defaults here
  } else {
    // ESP process. Set isoMapping using the define that was propagated
    // from master exe.
    char *imn = ComRtGetIsoMappingName();
    sessionDefaults_->setIsoMappingName(ComRtGetIsoMappingName(), (int)strlen(ComRtGetIsoMappingName()));
  }

  return 0;
}

/* return -1, if module has already been added. 0, if not */
short ContextCli::moduleAdded(const SQLMODULE_ID *module_id) {
  Module *module;
  int module_nm_len = getModNameLen(module_id);

  moduleList()->position(module_id->module_name, module_nm_len);
  while (module = (Module *)moduleList()->getNext()) {
    if ((module_nm_len == module->getModuleNameLen()) &&
        (str_cmp(module_id->module_name, module->getModuleName(), module_nm_len) == 0))
      return -1;  // found
  }
  // not found
  return 0;
}

// common for NT and NSK
static int allocAndReadPos(ModuleOSFile &file, char *&buf, int pos, int len, NAMemory *heap,
                             ComDiagsArea &diagsArea, const char *mname) {
  // allocate memory and read the requested section into it
  buf = (char *)(heap->allocateMemory((size_t)len));
  short countRead;
  int retcode = file.readpos(buf, pos, len, countRead);

  if (retcode) diagsArea << DgSqlCode(-CLI_MODULEFILE_CORRUPTED) << DgString0(mname);
  return retcode;
}

RETCODE ContextCli::allocateDesc(SQLDESC_ID *descriptor_id, int max_entries) {
  const char *hashData;
  ULng32 hashDataLength;

  if (descriptor_id->name_mode != desc_handle) {
    /* find if this descriptor exists. Return error, if so */
    if (getDescriptor(descriptor_id)) {
      diagsArea_ << DgSqlCode(-CLI_DUPLICATE_DESC);
      return ERROR;
    }
  }

  Descriptor *descriptor = new (exCollHeap()) Descriptor(descriptor_id, max_entries, this);

  if (descriptor_id->name_mode == desc_handle) {
    descriptor_id->handle = descriptor->getDescHandle();
    hashData = (char *)&descriptor_id->handle;
    hashDataLength = sizeof(descriptor_id->handle);
  } else {
    hashData = descriptor->getDescName()->identifier;
    hashDataLength = getIdLen(descriptor->getDescName());
  }

  descriptorList()->insert(hashData, hashDataLength, (void *)descriptor);
  return SUCCESS;
}

RETCODE ContextCli::deallocDesc(SQLDESC_ID *desc_id, NABoolean deallocStaticDesc) {
  Descriptor *desc = getDescriptor(desc_id);
  /* desc must exist and must have been allocated by call to AllocDesc*/
  if (!desc) {
    diagsArea_ << DgSqlCode(-CLI_DESC_NOT_EXISTS);
    return ERROR;
  } else if ((!desc->dynAllocated()) && (NOT deallocStaticDesc)) {
    diagsArea_ << DgSqlCode(-CLI_NOT_DYNAMIC_DESC) << DgString0("deallocate");
    return ERROR;
  }

  // remove this descriptor from the descriptor list
  descriptorList()->remove(desc);

  // delete the descriptor itself
  delete desc;

  return SUCCESS;
}

/* returns the statement, if it exists. NULL(0), otherwise */
Descriptor *ContextCli::getDescriptor(SQLDESC_ID *descriptor_id) {
  if (!descriptor_id) return 0;

  // NOTE: we don't bounds check the SQLDESC_ID that is passed in
  // although it may come from the user. However, we don't modify
  // any memory in it and don't return any information if the
  // descriptor id should point into PRIV memory.

  const SQLMODULE_ID *module1 = descriptor_id->module;

  SQLDESC_ID *desc1 = 0;
  const SQLMODULE_ID *module2 = 0;
  SQLDESC_ID *desc2 = 0;

  const char *hashData = NULL;
  ULng32 hashDataLength = 0;

  //
  // First, get the descriptor name into descName1...
  // Get it *outside* of the loop, since it's invariant.
  //
  switch (descriptor_id->name_mode) {
    case desc_handle:
      // no need for a name here!
      hashData = (char *)&(descriptor_id->handle);
      hashDataLength = sizeof(descriptor_id->handle);
      break;

    case desc_name:
      desc1 = descriptor_id;
      hashData = desc1->identifier;
      // hashDataLength = strlen(hashData);
      hashDataLength = getIdLen(desc1);
      break;

    case desc_via_desc: {
      // descriptr_id->identifier is the name of the descriptor
      // containing the actual name of this statement/cursor

      // decode the value in the descriptor into the statement name
      //  NOTE: the returned value is dynamically allocated from
      //  exHeap() and will need to be deleted before returning.

      desc1 = (SQLDESC_ID *)Descriptor::GetNameViaDesc(descriptor_id, this, *exHeap());
      if (desc1) {
        hashData = desc1->identifier;
        hashDataLength = getIdLen(desc1);
      } else
        return 0;

    }

    break;

    default:
      // must be an error
      return 0;
      break;
  }

  Descriptor *descriptor;
  descriptorList()->position(hashData, hashDataLength);
  while (descriptor = (Descriptor *)descriptorList()->getNext()) {
    switch (descriptor_id->name_mode) {
      case desc_handle:
        if (descriptor_id->handle == descriptor->getDescHandle()) {
          return descriptor;
        }
        break;

      case desc_name:
      case desc_via_desc:

        module2 = descriptor->getModuleId();
        desc2 = descriptor->getDescName();

        if (isEqualByName(module1, module2) && isEqualByName(desc1, desc2)) {
          if ((descriptor_id->name_mode == desc_via_desc) && desc1) {
            // fix memory leak (genesis case 10-981230-3244)
            // cause by not freeing desc1.
            if (desc1->identifier) {
              exHeap()->deallocateMemory((char *)desc1->identifier);
              setNameForId(desc1, 0, 0);
            }
            exHeap()->deallocateMemory(desc1);
            desc1 = 0;
          }
          return descriptor;
        }
        break;

      default:
        // error
        break;
    }
  }

  // fix memory leak (genesis case 10-981230-3244) caused by not
  // freeing the desc1 returned by Descriptor::GetNameViaDesc(desc).
  if ((descriptor_id->name_mode == desc_via_desc) && desc1) {
    exHeap()->deallocateMemory(desc1);
  }

  return 0;
}

/* returns the statement, if it exists. NULL(0), otherwise */
Statement *ContextCli::getNextStatement(SQLSTMT_ID *statement_id, NABoolean position, NABoolean advance) {
  if ((!statement_id) || (!statement_id->module)) return 0;

  Statement *statement;

  HashQueue *stmtList = statementList();

  if (position) stmtList->position();

  const SQLMODULE_ID *module1 = statement_id->module;
  const SQLMODULE_ID *module2 = 0;

  while (statement = (Statement *)stmtList->getCurr()) {
    switch (statement_id->name_mode) {
      case stmt_name:
      case cursor_name: {
        module2 = statement->getModuleId();

        if (isEqualByName(module1, module2)) {
          if (advance) stmtList->advance();

          return statement;
        }

      } break;

      default:
        return 0;
    }

    stmtList->advance();
  }

  return 0;
}

/* returns the statement, if it exists. NULL(0), otherwise */
Statement *ContextCli::getStatement(SQLSTMT_ID *statement_id, HashQueue *stmtList) {
  if (!statement_id || !(statement_id->module)) return 0;

  // NOTE: we don't bounds check the SQLSTMT_ID that is passed in
  // although it may come from the user. However, we don't modify
  // any memory in it and don't return any information if the
  // descriptor id should point into PRIV memory.

  if (ComMemoryDiags::DumpMemoryInfo()) {
    dumpStatementAndGlobalInfo(ComMemoryDiags::DumpMemoryInfo());
    delete ComMemoryDiags::DumpMemoryInfo();
    ComMemoryDiags::DumpMemoryInfo() = NULL;  // doit once per statement only
  }
  const SQLMODULE_ID *module1 = statement_id->module;
  SQLSTMT_ID *stmt1 = 0;
  const SQLMODULE_ID *module2 = 0;
  SQLSTMT_ID *stmt2 = 0;

  // the following two variables are used to hash into
  // the statmentlist
  const char *hashData = NULL;
  ULng32 hashDataLength = 0;

  //
  // First, get the statement name.
  // Get it *outside* of the loop, since it's invariant.
  //
  switch (statement_id->name_mode) {
    case stmt_handle:
      // no need for a name here!
      hashData = (char *)&(statement_id->handle);
      hashDataLength = sizeof(statement_id->handle);
      break;

    case stmt_name:
    case cursor_name:
      stmt1 = statement_id;

      hashData = stmt1->identifier;
      hashDataLength = getIdLen(stmt1);
      break;

    case stmt_via_desc:
    case curs_via_desc: {
      // statement_id->identifier is the name of the descriptor
      // containing the actual name of this statement/cursor
      SQLDESC_ID desc_id;
      init_SQLCLI_OBJ_ID(&desc_id);

      desc_id.name_mode = desc_via_desc;
      desc_id.identifier = statement_id->identifier;
      desc_id.identifier_len = getIdLen(statement_id);
      desc_id.module = statement_id->module;

      // decode the value in the descriptor into the statement name
      //  the returned value is dynamically allocated, it will need
      //  to be deleted before returning.
      stmt1 = Descriptor::GetNameViaDesc(&desc_id, this, *exHeap());
      if (stmt1) {
        hashData = stmt1->identifier;
        hashDataLength = getIdLen(stmt1);
      } else
        return 0;

    } break;

    default:
      // must be an error
      break;
  }

  // if the input param stmtList is NULL, we do the lookup from
  // statementList() or cursorList(). Otherwise we lookup stmtList.
  if (!stmtList)
    if ((statement_id->name_mode == cursor_name) || (statement_id->name_mode == curs_via_desc))
      stmtList = cursorList();
    else
      stmtList = statementList();

  Statement *statement;

  stmtList->position(hashData, hashDataLength);
  while (statement = (Statement *)stmtList->getNext()) {
    switch (statement_id->name_mode) {
      case stmt_handle:
        if (statement_id->handle == statement->getStmtHandle()) {
          return statement;
        }
        break;

      case stmt_name:
      case cursor_name:
        if (statement_id->name_mode == cursor_name)
          stmt2 = statement->getCursorName();
        else
          stmt2 = statement->getStmtId();

        if (stmt2 != NULL && (stmt2->name_mode != stmt_handle)) {
          module2 = statement->getModuleId();
          if (isEqualByName(module1, module2) && isEqualByName(stmt1, stmt2)) {
            return statement;
          }
        }
        break;

      case stmt_via_desc:
      case curs_via_desc:

        if (statement_id->name_mode == curs_via_desc)
          stmt2 = statement->getCursorName();
        else
          stmt2 = statement->getStmtId();

        if (stmt2 != NULL && (stmt2->name_mode != stmt_handle)) {
          module2 = statement->getModuleId();
          if (isEqualByName(module1, module2) && isEqualByName(stmt1, stmt2)) {
            if (stmt1) {
              if (stmt1->identifier) {
                exHeap()->deallocateMemory((char *)stmt1->identifier);
                setNameForId(stmt1, 0, 0);
              }
              // fix memory leak (genesis case 10-981230-3244)
              // caused by not freeing stmt1.
              exHeap()->deallocateMemory(stmt1);
            }
            return statement;
          }
        }
        break;

      default:
        break;
    }
  }

  // fix memory leak (genesis case 10-981230-3244) caused by not
  // freeing the stmt1 returned by Descriptor::GetNameViaDesc(desc).
  if (((statement_id->name_mode == stmt_via_desc) || (statement_id->name_mode == curs_via_desc)) && stmt1) {
    if (stmt1->identifier) {
      exHeap()->deallocateMemory((char *)stmt1->identifier);
      setNameForId(stmt1, 0, 0);
    }
    exHeap()->deallocateMemory(stmt1);
  }

  return 0;
}

RETCODE ContextCli::allocateStmt(SQLSTMT_ID *statement_id, Statement::StatementType stmt_type,
                                 SQLSTMT_ID *cloned_from_stmt_id) {
  const char *hashData = NULL;
  ULng32 hashDataLength = 0;

  if (statement_id->name_mode != stmt_handle) {
    /* find if this statement exists. Return error, if so */
    if (getStatement(statement_id)) {
      diagsArea_ << DgSqlCode(-CLI_DUPLICATE_STMT);
      return ERROR;
    }
  }

  // when 'cloning' a statement, we are in the following situation
  //  a) we are doing an ALLOCATE CURSOR - extended dynamic SQL
  //  b) we are binding to that statement - right now!
  //  c) that statement must already exist and be in a prepared state...
  //
  // there is another way to 'bind' a statement and a cursor
  // besides cloning.  By 'bind'ing a cursor to a statement, I mean that
  // cursor has to acquire a root_tdb from a prepared statement, and then
  // has to build its own tcb tree from the tdb tree.
  //
  // in the ordinary dynamic SQL case - DECLARE CURSOR C1 FOR S1;
  // S1 will not have been prepared initially.  Binding will not
  // take place until the cursor is OPENed.
  //
  // See Statement::execute() and for details of this type of
  // binding.
  //
  Statement *cloned_from_stmt = 0;

  if (cloned_from_stmt_id) {
    cloned_from_stmt = getStatement(cloned_from_stmt_id);
    /* statement we're cloning from must exists. Return error, if not */
    if (!cloned_from_stmt) {
      diagsArea_ << DgSqlCode(-CLI_STMT_NOT_EXISTS);
      return ERROR;
    }
  }

  Statement *statement = new (exCollHeap()) Statement(statement_id, cliGlobals_, stmt_type, NULL, NULL);

  if (cloned_from_stmt) statement->bindTo(cloned_from_stmt);

  if (statement_id->name_mode == stmt_handle) {
    statement_id->handle = statement->getStmtHandle();
    hashData = (char *)&(statement_id->handle);
    hashDataLength = sizeof(statement_id->handle);
  } else {
    hashData = statement->getIdentifier();
    hashDataLength = getIdLen(statement->getStmtId());
  }

  StmtListDebug1(statement, "Adding %p to statementList_", statement);

  semaphoreLock();
  if (hashData) statementList()->insert(hashData, hashDataLength, (void *)statement);
  if (processStats_ != NULL) processStats_->incStmtCount(stmt_type);
  semaphoreRelease();

  return SUCCESS;
}

RETCODE ContextCli::deallocStmt(SQLSTMT_ID *statement_id, NABoolean deallocStaticStmt) {
  // for now, dump memory info before deleting the statement
#if defined(_DEBUG)
  char *filename = getenv("MEMDUMP");
  if (filename) {
    // open output file sytream.
    ofstream outstream(filename);
    cliGlobals_->getExecutorMemory()->dump(&outstream, 0);
    // also dump the memory map
    outstream.flush();
    outstream.close();
  };
#endif

  Statement *stmt = getStatement(statement_id);
  // Check if the name modes match
  /* stmt must exist and must have been allocated by call to AllocStmt*/
  if (!stmt) {
    diagsArea_ << DgSqlCode(-CLI_STMT_NOT_EXISTS);
    return ERROR;
  } else if ((!stmt->allocated()) && (NOT deallocStaticStmt)) {
    diagsArea_ << DgSqlCode(-CLI_NOT_DYNAMIC_STMT) << DgString0("deallocate");
    return ERROR;
  } else if (((stmt->getStmtId())->name_mode == cursor_name) && (NOT deallocStaticStmt)) {
    // This is really a cloned statment entry
    // Don't deallocate this since there is a cursor entry
    // in the cursor list
    diagsArea_ << DgSqlCode(-CLI_STMT_NOT_EXISTS);
    return ERROR;
  }

  // if this is part of an auto query retry and esp need to be cleaned up,
  // set that indication in root tcb.
  if (aqrInfo() && aqrInfo()->espCleanup()) {
    stmt->getGlobals()->setVerifyESP();
    if (stmt->getRootTcb()) stmt->getRootTcb()->setFatalError();
  }

  if (stmt->stmtInfo() != NULL && stmt->stmtInfo()->statement() == stmt) {
    if (statement_id->name_mode != desc_handle && (StatementInfo *)statement_id->handle == stmt->stmtInfo())
      statement_id->handle = 0;
    exHeap_.deallocateMemory((void *)stmt->stmtInfo());
  }

  if ((stmt->getStmtStats()) && (stmt->getStmtStats()->getMasterStats())) {
    stmt->getStmtStats()->getMasterStats()->setFreeupStartTime(NA_JulianTimestamp());
  }

  // Remove statement from statementList
  // Update stmtGlobals transid with current transid.
  // The stmtGlobals transid is used in sending release msg to remote esps.
  // This function can be called by CliGlobals::closeAllOpenCursors
  // when ExTransaction::inheritTransaction detects the context transid
  // is not the same as the current transid.  The context transid can be
  // obsolete if the transaction has been committed/aborted by the user.
  long transid;
  if (!getTransaction()->getCurrentXnId(&transid))
    stmt->getGlobals()->castToExExeStmtGlobals()->getTransid() = transid;
  else
    stmt->getGlobals()->castToExExeStmtGlobals()->getTransid() = (long)-1;

  StmtListDebug1(stmt, "Removing %p from statementList_", stmt);

  semaphoreLock();
  statementList()->remove(stmt);
  if (processStats_ != NULL) processStats_->decStmtCount(stmt->getStatementType());
  semaphoreRelease();

  // the statement might have a cursorName. If it has, it is also in the
  // cursorList_. Remove it from this list;
  removeCursor(stmt->getCursorName(), stmt->getModuleId());

  // the statement might be on the openStatementList. Remove it.
  removeFromOpenStatementList(statement_id);
  removeFromCloseStatementList(stmt, FALSE);

  // remove any cloned statements.
  Queue *clonedStatements = stmt->getClonedStatements();
  clonedStatements->position();
  Statement *clone = (Statement *)clonedStatements->getNext();
  for (; clone != NULL; clone = (Statement *)clonedStatements->getNext()) {
    RETCODE cloneRetcode = cleanupChildStmt(clone);
    if (cloneRetcode == ERROR) return ERROR;
  }

  ExRsInfo *rsInfo = stmt->getResultSetInfo();
  if (rsInfo) {
    ULng32 numChildren = rsInfo->getNumEntries();
    for (ULng32 i = 1; i <= numChildren; i++) {
      Statement *rsChild = NULL;
      const char *proxySyntax;
      NABoolean open;
      NABoolean closed;
      NABoolean prepared;
      NABoolean found = rsInfo->getRsInfo(i, rsChild, proxySyntax, open, closed, prepared);
      if (found && rsChild) {
        RETCODE proxyRetcode = cleanupChildStmt(rsChild);
        if (proxyRetcode == ERROR) return ERROR;
      }
    }
  }

  // if deallocStaticStmt is TRUE, then remove the default descriptors
  // from the descriptor list. The actual descriptor will be destructed
  // when the stmt is deleted.
  if (deallocStaticStmt) {
    Descriptor *desc = stmt->getDefaultDesc(SQLWHAT_INPUT_DESC);
    if (desc) {
      desc = getDescriptor(desc->getDescName());
      descriptorList()->remove(desc);
    }

    desc = stmt->getDefaultDesc(SQLWHAT_OUTPUT_DESC);
    if (desc) {
      desc = getDescriptor(desc->getDescName());
      descriptorList()->remove(desc);
    }
  }

  if ((stmt->getStmtStats()) && (stmt->getStmtStats()->getMasterStats())) {
    stmt->getStmtStats()->getMasterStats()->setFreeupEndTime(NA_JulianTimestamp());
  }

  // delete the statement itself. This will delete child statements
  // such as clones or stored procedure result sets too.
  delete stmt;

  // close all open cursors referencing this statement...
  // Not sure why this needs to be called
  closeAllCursorsFor(statement_id);

  return SUCCESS;
}

short ContextCli::commitTransaction() {
  releaseAllTransactionalRequests();

  // now do the actual commit
  return transaction_->commitTransaction();
}

short ContextCli::releaseAllTransactionalRequests() {
  // for all statements in the context that need a transaction,
  // make sure they have finished their outstanding I/Os
  Statement *statement;
  short result = SUCCESS;

  openStatementList()->position();  // to head of list
  statement = (Statement *)openStatementList()->getNext();
  while (statement) {
    if (statement->transactionReqd() || statement->getState() == Statement::PREPARE_) {
      if (statement->releaseTransaction()) result = ERROR;
    }

    statement = (Statement *)openStatementList()->getNext();
  }
  return result;
}

void ContextCli::clearAllTransactionInfoInClosedStmt(long transid) {
  // for all closed statements in the context, clear transaction Id
  // kept in the statement global

  Statement *statement;

  statementList()->position();
  while (statement = (Statement *)statementList()->getNext()) {
    if (statement->getState() == Statement::CLOSE_) {
      if (statement->getGlobals()->castToExExeStmtGlobals()->getTransid() == transid)
        statement->getGlobals()->castToExExeStmtGlobals()->getTransid() = (long)-1;
    }
  }
}

void ContextCli::closeAllCursors(enum CloseCursorType type, enum closeTransactionType transType,
                                 const long executorXnId, NABoolean inRollback) {
  // for all statements in the context close all of them that are open;
  // only the cursor(s) should be open at this point.

  // VO, Dec. 2005: if the executorXnId parameter is non-zero, we use that as the current
  // transid instead of asking TMF. This allows us to use the CLOSE_CURR_XN functionality
  // in cases where TMF cannot or will not return the current txn id.
  // Currently used by ExTransaction::validateTransaction (executor/ex_transaction.cpp).

  Statement *statement;
  Statement **openStmtArray = NULL;
  int i;
  int noOfStmts;
  long currentXnId = executorXnId;

  if ((transType == CLOSE_CURR_XN) && (transaction_->xnInProgress()) && (executorXnId == 0)) {
    short retcode = transaction_->getCurrentXnId((long *)&currentXnId);
    // Use the transaction id from context if the curret
    // transaction is already closed.
    if (retcode == 78 || retcode == 75) currentXnId = transaction_->getExeXnId();
  }

  // Get the open Statements in a array
  // This will avoid repositioning the hashqueue to the begining of openStatementList
  // when the cursor is closed and also avoid issuing the releaseTransaction
  // repeatedly when the hashqueue is repositioned
  noOfStmts = openStatementList_->numEntries();
  if (noOfStmts > 0) {
    openStmtArray = new (exHeap()) Statement *[noOfStmts];
    openStatementList_->position();
    for (i = 0; (statement = (Statement *)openStatementList_->getNext()) != NULL && i < noOfStmts; i++) {
      openStmtArray[i] = statement;
    }
    noOfStmts = i;
  }
  NABoolean gotStatusTrans = FALSE;
  NABoolean isTransActive = FALSE;
  short rc;
  short status;
  for (i = 0; isTransActive == FALSE && i < noOfStmts; i++) {
    statement = openStmtArray[i];
    Statement::State st = statement->getState();

    switch (st) {
      case Statement::OPEN_:
      case Statement::FETCH_:
      case Statement::EOF_:
      case Statement::RELEASE_TRANS_: {
        NABoolean closeStmt = FALSE;
        NABoolean releaseStmt = FALSE;

        switch (transType) {
          case CLOSE_CURR_XN:
            if (currentXnId == statement->getGlobals()->castToExExeStmtGlobals()->getTransid() &&
                statement->transactionReqd()) {
              checkIfNeedToClose(statement, type, closeStmt, releaseStmt);
            }
            break;
          case CLOSE_ENDED_XN:
            // Close statement if Statement TCBREF matches with context TCBREF
            // or if Statement TCBREF is -1 (for browse access)
            // There is a still problem that we could end up closing
            // browse access cursors that belong to a different transaction
            // in the context or browse access cursors that are never opened in a
            // transaction. STATUSTRANSACTION slows down the response time
            // and hence avoid calling it more than necessary.
            // If the transaction is active(not aborted or committed), end the loop
            currentXnId = statement->getGlobals()->castToExExeStmtGlobals()->getTransid();
            if (currentXnId == transaction_->getExeXnId() || currentXnId == -1) {
              if (!gotStatusTrans) {
                rc = STATUSTRANSACTION(&status, transaction_->getTransid());
                if (rc == 0 && status != 3 && status != 4 && status != 5) {
                  isTransActive = TRUE;
                  break;
                }
                gotStatusTrans = TRUE;
              }
              checkIfNeedToClose(statement, type, closeStmt, releaseStmt);
            }
            break;
          case CLOSE_ALL_XN:
            checkIfNeedToClose(statement, type, closeStmt, releaseStmt);
            break;
        }
        if (closeStmt) {
          // only close those statements which were constructed in this
          // scope.
          if (statement->getCliLevel() == getNumOfCliCalls()) {
            statement->close(diagsArea_, inRollback);
          }
          // STATUSTRANSACTION slows down the response time
          // Browse access cursor that are started under a transaction
          // will have the transid set in its statement globals. These browse
          // access cursors will be closed and the others (ones that aren't)
          // started under transaction will not be closed
          if (currentXnId == transaction_->getExeXnId()) {
          }  // TMM: Do Nothing.
        } else if (releaseStmt)
          statement->releaseTransaction(TRUE, TRUE, !closeStmt);
      } break;
      default:
        break;
    }
  }
  if (openStmtArray) {
    NADELETEBASIC(openStmtArray, exHeap());
  }
}

void ContextCli::checkIfNeedToClose(Statement *stmt, enum CloseCursorType type, NABoolean &closeStmt,
                                    NABoolean &releaseStmt) {
  closeStmt = FALSE;
  releaseStmt = FALSE;

  if (stmt->isAnsiHoldable()) {
    if (type == CLOSE_ALL_INCLUDING_ANSI_HOLDABLE || type == CLOSE_ALL_INCLUDING_ANSI_PUBSUB_HOLDABLE ||
        type == CLOSE_ALL_INCLUDING_ANSI_PUBSUB_HOLDABLE_WHEN_CQD)
      closeStmt = TRUE;
    else
      releaseStmt = TRUE;
  } else if (stmt->isPubsubHoldable()) {
    if (type == CLOSE_ALL_INCLUDING_PUBSUB_HOLDABLE || type == CLOSE_ALL_INCLUDING_ANSI_PUBSUB_HOLDABLE)
      closeStmt = TRUE;
    else if (type == CLOSE_ALL_INCLUDING_ANSI_PUBSUB_HOLDABLE_WHEN_CQD &&
             stmt->getRootTdb()->getPsholdCloseOnRollback())
      closeStmt = TRUE;
    else
      releaseStmt = TRUE;
  } else
    closeStmt = TRUE;
  return;
}

void ContextCli::closeAllCursorsFor(SQLSTMT_ID *statement_id) {
  // for all statements in the context, close all of them for the
  // given statement id...

  Statement *statement;

  statementList()->position();
  while (statement = (Statement *)statementList()->getNext()) {
    if (statement->getCursorName() && statement->getIdentifier() && statement_id->identifier &&
        !strcmp(statement->getIdentifier(), statement_id->identifier) &&
        ((statement_id->module && statement_id->module->module_name && statement->getModuleId()->module_name &&
          isEqualByName(statement_id->module, statement->getModuleId())) ||
         ((statement_id->module == NULL) && (statement->getModuleId()->module_name == NULL)))) {
      statement->close(diagsArea_);
    }
  }
}

#pragma page "ContextCli::setAuthID"
// *****************************************************************************
// *                                                                           *
// * Function: ContextCli::setAuthID                                           *
// *                                                                           *
// *    Authenticates a user on a Trafodion database instance.                 *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <usersInfo>                     const USERS_INFO &              In       *
// *    structure containing metadata details for connecting user.             *
// *                                                                           *
// *  <authToken>                     const char *                    In       *
// *    is the token generated for this user session.                          *
// *                                                                           *
// *  <authTokenLen>                  int                           In       *
// *    is the length of the token in bytes.                                   *
// *                                                                           *
// *  <slaName>                       const char *                    In       *
// *    sla name assigned to the connection                                    *
// *                                                                           *
// *  <profileName>                   const char *                    In       *
// *    profile name assigned to the connection                                *
// *                                                                           *
// *  <resetAttributes>               int                           In       *
// *    if TRUE, then reset all attributes and cached information              *
// *                                                                           *
// *****************************************************************************

int ContextCli::setAuthID(const USERS_INFO &usersInfo, const char *authToken, int authTokenLen, const char *slaName,
                            const char *profileName, int resetAttributes) {
  validAuthID_ = FALSE;  // Is TRUE only if a valid user found,set within completeSetAuthId()

  // Note authID being NULL is only to support sqlci processes and embedded apps
  if (usersInfo.extUsername == NULL) {
    const char *rootUserName = ComUser::getRootUserName();
    USERS_INFO defaultUsersInfo;
    defaultUsersInfo.effectiveUserID = ComUser::getRootUserID();
    defaultUsersInfo.sessionUserID = defaultUsersInfo.effectiveUserID;
    defaultUsersInfo.extUsername = new char[strlen(rootUserName) + 1];
    strcpy(defaultUsersInfo.extUsername, rootUserName);
    defaultUsersInfo.dbUsername = new char[strlen(rootUserName) + 1];
    strcpy(defaultUsersInfo.dbUsername, rootUserName);
    defaultUsersInfo.tenantName = new char[strlen(DB__SYSTEMTENANT) + 1];
    strcpy(defaultUsersInfo.tenantName, DB__SYSTEMTENANT);
    defaultUsersInfo.tenantID = SYSTEM_TENANT_ID;
    defaultUsersInfo.client_host_name = new char[strlen(NA_CLIENT_HOSTNAME) + 1];
    strcpy(defaultUsersInfo.client_host_name, NA_CLIENT_HOSTNAME);
    defaultUsersInfo.client_user_name = new char[strlen(NA_CLIENT_OSUSERNAME) + 1];
    strcpy(defaultUsersInfo.client_user_name, NA_CLIENT_OSUSERNAME);
    defaultUsersInfo.client_addr = new char[strlen(NA_CLIENT_IP_ADDRESS) + 1];
    strcpy(defaultUsersInfo.client_addr, NA_CLIENT_IP_ADDRESS);

    if (externalUsername_) exHeap()->deallocateMemory(externalUsername_);
    externalUsername_ = (char *)exHeap()->allocateMemory(strlen(rootUserName) + 1);
    strcpy(externalUsername_, rootUserName);

    if (completeSetAuthID(defaultUsersInfo, NULL, 0, NULL, NULL, false, false, true, false, 0)) {
      setSqlParserFlags(0x400000);
      return ERROR;
    }
    // allow setOnce cqds.
    setSqlParserFlags(0x400000);
    return SUCCESS;
  }

  // Mainline of function, we have a real username.
  bool resetCQDs = true;

  bool eraseCQDs = false;
  bool dropVolatileSchema = false;

  // Get the external user name
  if (externalUsername_) exHeap()->deallocateMemory(externalUsername_);

  externalUsername_ = (char *)exHeap()->allocateMemory(strlen(usersInfo.extUsername) + 1);

  strcpy(externalUsername_, usersInfo.extUsername);

  if ((usersInfo.effectiveUserID != databaseUserID_) || (usersInfo.sessionUserID != sessionUserID_) ||
      (msg_license_multitenancy_enabled() && usersInfo.tenantID != tenantID_) || resetAttributes) {
    // user id has changed.
    // Drop any volatile schema created in this context
    // before switching the user.
    dropSession(TRUE);  // dropSession() clears the this->sqlParserFlags_

    // close all tables that were opened by the previous user
    closeAllTables();
    userNameChanged_ = TRUE;
    eraseCQDs = true;
  } else
    userNameChanged_ = FALSE;

  if (completeSetAuthID(usersInfo, authToken, authTokenLen, slaName, profileName, eraseCQDs, true, dropVolatileSchema,
                        resetCQDs, resetAttributes)) {
    setSqlParserFlags(0x400000);
    return ERROR;
  }

  // allow setOnce cqds.
  setSqlParserFlags(0x400000);
  return SUCCESS;
}
//*********************** End of ContextCli::setAuthID *************************

#pragma page "ContextCli::completeSetAuthID"
// *****************************************************************************
// *                                                                           *
// * Function: ContextCli::completeSetAuthID                                   *
// *                                                                           *
// *    Sets fields in CliContext related to authentication and takes actions  *
// * when user ID has changed.                                                 *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <usersInfo>                     const USERS_INFO &              In       *
// *    structure containing metadata details for connecting user.             *
// *                                                                           *
// *  <authToken>                     const char *                    In       *
// *    is the credentials used for authenticating this user, specifically     *
// *  a valid token key.                                                       *
// *                                                                           *
// *  <authTokenLen>                  int                           In       *
// *    is the credentials used for authenticating this user, specifically     *
// *  a valid token key.                                                       *
// *                                                                           *
// *  <slaName>                       const char *                    In       *
// *    sla name assigned to the connection                                    *
// *                                                                           *
// *  <profileName>                   const char *                    In       *
// *    profile name assigned to the connection                                *
// *                                                                           *
// *  <eraseCQDs>                     bool                            In       *
// *    if true, all existing CQDs are erased.                                 *
// *                                                                           *
// *  <releaseUDRServers>             bool                            In       *
// *    if true, any existing UDR servers are released.                        *
// *                                                                           *
// *  <dropVolatileSchema>            bool                            In       *
// *    if true, any existing volatile schemas are dropped.                    *
// *                                                                           *
// *  <resetCQDs>                     bool                            In       *
// *    if true, all existing CQDs are reset.                                  *
// *
// * <slaName>                      const char *                      In       *
//   service level name stored in zookeeper and passed in during session start
// * <profileName>                  const char *                      In       *
//   profile name stored in zookeeper and passed in during session start       *
//                                                                             *
// *****************************************************************************
int ContextCli::completeSetAuthID(const USERS_INFO &usersInfo, const char *authToken, int authTokenLen,
                                    const char *slaName, const char *profileName, bool eraseCQDs,
                                    bool releaseUDRServers, bool dropVolatileSchema, bool resetCQDs,
                                    int resetAttributes) {
  bool mtEnabled = msg_license_multitenancy_enabled();
  bool updateCredentials = false;
  if ((usersInfo.effectiveUserID != databaseUserID_) || (usersInfo.sessionUserID != sessionUserID_) ||
      (mtEnabled && usersInfo.tenantID != tenantID_))
    updateCredentials = true;

  if (dropVolatileSchema || updateCredentials || resetAttributes) {
    // user id has changed.
    // Drop any volatile schema created in this context.
    // Also kill and restart child mxcmp and query cache
    if (updateCredentials || resetAttributes)
      dropSession(TRUE);  // remove query cache
    else
      dropSession();
  }

  if (releaseUDRServers) {
    // Release all ExUdrServer references
    releaseUdrServers();
  }

  if (resetCQDs) {
    ExeCliInterface *cliInterface = new (exHeap()) ExeCliInterface(exHeap(), SQLCHARSETCODE_UTF8, this, NULL);
    ComDiagsArea *tmpDiagsArea = &diagsArea_;
    cliInterface->executeImmediate((char *)"control query default * reset;", NULL, NULL, TRUE, NULL, 0, &tmpDiagsArea);
  }

  // set the tenant-specific values
  if (mtEnabled) {
    if ((usersInfo.tenantID != tenantID_) || resetAttributes) {
      if (retrieveAndSetTenantInfo(usersInfo.tenantID)) return ERROR;
    }
  }

  // Recreate MXCMP if previously connected and currently connected user id's
  // are different.
  if (updateCredentials) killAndRecreateMxcmp();

  if (eraseCQDs) {
    if (controlArea_) NADELETE(controlArea_, ExControlArea, exCollHeap());
    controlArea_ = NULL;
    // create a fresh one
    controlArea_ = new (exCollHeap()) ExControlArea(this, exCollHeap());
  }

  // reset rolelist in anticipation of the new user
  if (updateCredentials) resetRoleList();

  authIDType_ = SQLAUTHID_TYPE_ASCII_USERNAME;
  if (authID_) exHeap()->deallocateMemory(authID_);
  authID_ = (char *)exHeap()->allocateMemory(strlen(usersInfo.extUsername) + 1);
  strcpy(authID_, usersInfo.extUsername);
  // save userid in context.
  databaseUserID_ = usersInfo.effectiveUserID;

  // save sessionid in context.
  sessionUserID_ = usersInfo.sessionUserID;
  if (databaseUserName_) {
    strncpy(databaseUserName_, usersInfo.dbUsername, MAX_DBUSERNAME_LEN);
    databaseUserName_[MAX_DBUSERNAME_LEN] = 0;
  }

  if (slaName) {
    memset(slaName_, 0, MAX_SLA_NAME_LEN + 1);
    strcpy(slaName_, slaName);
  }

  if (profileName) {
    memset(profileName_, 0, MAX_PROFILE_NAME_LEN + 1);
    strcpy(profileName_, profileName);
  }

  if (authToken_) {
    exHeap()->deallocateMemory(authToken_);
    authToken_ = 0;
  }

  if (authToken) {
    authToken_ = (char *)exHeap()->allocateMemory(authTokenLen + 1);
    strcpy(authToken_, authToken);
  }

  // save Hostname, OS user name and ip address of client in context
  if (hostName_ && usersInfo.client_host_name) {
    strncpy(hostName_, usersInfo.client_host_name, MAX_CLIENT_HOSTNAME_LEN);
    hostName_[MAX_CLIENT_HOSTNAME_LEN] = 0;
  }

  if (osUserName_ && usersInfo.client_user_name) {
    strncpy(osUserName_, usersInfo.client_user_name, MAX_CLIENT_OSNAME_LEN);
    osUserName_[MAX_CLIENT_OSNAME_LEN] = 0;
  }

  if (ipAddress_ && usersInfo.client_addr) {
    strncpy(ipAddress_, usersInfo.client_addr, INET6_ADDRSTRLEN - 1);
    osUserName_[INET6_ADDRSTRLEN] = 0;
  }

  // reset caches based on the new user
  if (updateCredentials || resetAttributes) {
    CmpContextInfo *cmpCntxtInfo;
    for (int i = 0; i < cmpContextInfo_.entries(); i++) {
      cmpCntxtInfo = cmpContextInfo_[i];
      cmpCntxtInfo->getCmpContext()->resetAllCaches(FALSE);
    }
    NAString msg("Credentials updated and ");
    if (CmpCommon::getDefault(TRAF_ENABLE_METADATA_LOAD_IN_CACHE) == DF_OFF)
      msg += "caches cleared.";
    else
      msg += "caches reset.";
    QRLogger::log(CAT_SQL_EXE, LL_INFO, "%s", msg.data());
  } else
    QRLogger::log(CAT_SQL_EXE, LL_INFO, "User credentials did not change, caches retained.");

  validAuthID_ = TRUE;

  // Setup current user role list
  int numRoles = 0;
  int *cachedRoleIDs = NULL;
  int *cachedGranteeIDs = NULL;
  if (getRoleList(numRoles, cachedRoleIDs, cachedGranteeIDs) != SUCCESS) {
    diagsArea_ << DgSqlCode(1057);  // CAT_UNABLE_TO_RETRIEVE_ROLES
    return WARNING;
  }

  return SUCCESS;
}
//******************* End of ContextCli::completeSetAuthID *********************

int ContextCli::boundsCheckMemory(void *startAddress, ULng32 length) {
  int retcode = 0;

  if (cliGlobals_->boundsCheck(startAddress, length, retcode)) diagsArea_ << DgSqlCode(retcode);
  return retcode;
}

void ContextCli::removeCursor(SQLSTMT_ID *cursorName, const SQLMODULE_ID *moduleId) {
  if (!cursorName) return;

  Statement *statement;
  cursorList()->position(cursorName->identifier, getIdLen(cursorName));

  while (statement = (Statement *)cursorList()->getNext()) {
    if (isEqualByName(cursorName, statement->getCursorName()) && isEqualByName(statement->getModuleId(), moduleId)) {
      StmtListDebug1(statement, "Removing %p from cursorList_", statement);
      cursorList()->remove(statement);
      return;
    }
  }
}

void ContextCli::addToOpenStatementList(SQLSTMT_ID *statement_id, Statement *statement) {
  const char *hashData = NULL;
  ULng32 hashDataLength = 0;

  if (statement_id->name_mode == stmt_handle) {
    hashData = (char *)&(statement_id->handle);
    hashDataLength = sizeof(statement_id->handle);
  } else {
    hashData = statement->getIdentifier();
    // hashDataLength = strlen(hashData);
    hashDataLength = getIdLen(statement->getStmtId());
  }

  StmtListDebug1(statement, "Adding %p to openStatementList_", statement);
  openStatementList()->insert(hashData, hashDataLength, (void *)statement);
  if (processStats_ != NULL) processStats_->incOpenStmtCount();
  removeFromCloseStatementList(statement, TRUE /*forOpen*/);
}

void ContextCli::removeFromOpenStatementList(SQLSTMT_ID *statement_id) {
  Statement *statement = getStatement(statement_id, openStatementList());
  if (statement) {
    // Make sure we complete all outstanding nowaited transactional
    // ESP messages to the statement. This is so we need only check
    // the open statements for such messages at commit time.
    if (statement->transactionReqd()) statement->releaseTransaction(TRUE);

    StmtListDebug1(statement, "Removing %p from openStatementList_", statement);
    openStatementList()->remove(statement);
    if (processStats_ != NULL) processStats_->decOpenStmtCount();
    addToCloseStatementList(statement);
  }
}

// Add to closeStatementList_ and reclaim other statements if possible.
// [EL]
void ContextCli::addToCloseStatementList(Statement *statement) {
  // Get the Heap usage at the Statement Level
  size_t lastBlockSize;
  size_t freeSize;
  size_t totalSize;
  NAHeap *stmtHeap = statement->stmtHeap();
  NABoolean crowded = stmtHeap->getUsage(&lastBlockSize, &freeSize, &totalSize);

  if ((((statementList_->numEntries() < 50) && (!crowded || freeSize > totalSize / (100 / stmtReclaimMinPctFree_))) &&
       closeStatementList_ == NULL) ||
      statement->closeSequence() > 0)
    return;

  // Add to the head of closeStatementList.
  statement->nextCloseStatement() = closeStatementList_;
  statement->prevCloseStatement() = NULL;
  incCloseStmtListSize();

  // Update the head of the list.
  if (closeStatementList_ != NULL) closeStatementList_->prevCloseStatement() = statement;
  closeStatementList_ = statement;

  StmtListDebug1(statement, "Stmt %p is now at the head of closeStatementList_", statement);

  if (nextReclaimStatement_ == NULL) nextReclaimStatement_ = statement;

  // Rewind the sequence counter to avoid overflow.
  if (currSequence_ == INT_MAX) {
    // Re-number closeSequence numbers.
    int i = 2;
    Statement *st2, *st = nextReclaimStatement_;
    if (st != NULL)
      st2 = st->nextCloseStatement();
    else
      st2 = closeStatementList_;

    while (st2 != NULL) {  // Set to 1 for statements older than nextReclaimStatement_.
      st2->closeSequence() = 1;
      st2 = st2->nextCloseStatement();
    }
    while (st != NULL) {  // Set to consecutive numbers 3, 4, ...
      st->closeSequence() = ++i;
      st = st->prevCloseStatement();
    }
    // Set to the largest closeSequence.
    currSequence_ = i;
  }

  // Set the closeSequence_ number.
  if (statement->closeSequence() < 0)
    // This is the first time close of the dynamic statement.
    statement->closeSequence() = currSequence_;
  else
    statement->closeSequence() = ++currSequence_;
}

// True if OK to redrive.  False if no more statements to reclaim.
NABoolean ContextCli::reclaimStatements() {
  NABoolean someStmtReclaimed = FALSE;

  if (nextReclaimStatement_ == NULL) return FALSE;

  long reclaimTotalMemorySize = getSessionDefaults()->getReclaimTotalMemorySize();
  if (reclaimTotalMemorySize < 0) return FALSE;

#if defined(_DEBUG)
  char *reclaimAfter = NULL;
  int reclaimIf = (reclaimAfter == NULL) ? 0 : atol(reclaimAfter);

  if (reclaimIf > 0 && reclaimIf < closeStmtListSize_) {
    someStmtReclaimed = reclaimStatementSpace();
    return someStmtReclaimed;
  }
#endif
  // Get the Heap usage at the Context Level
  size_t lastBlockSize;
  size_t freeSize;
  size_t totalSize;
  NAHeap *heap = exHeap();  // Get the context Heap
  NABoolean crowded = heap->getUsage(&lastBlockSize, &freeSize, &totalSize);
  int reclaimFreeMemoryRatio;
  double freeRatio;
  NABoolean retcode;
  if (((long)totalSize) > reclaimTotalMemorySize) {
    reclaimFreeMemoryRatio = getSessionDefaults()->getReclaimFreeMemoryRatio();
    retcode = TRUE;
    freeRatio = (double)(freeSize)*100 / (double)totalSize;
    while (retcode && ((long)totalSize) > reclaimTotalMemorySize && freeRatio < reclaimFreeMemoryRatio) {
      retcode = reclaimStatementSpace();
      if (retcode) someStmtReclaimed = TRUE;
      crowded = heap->getUsage(&lastBlockSize, &freeSize, &totalSize);
      freeRatio = (double)(freeSize)*100 / (double)totalSize;
    }
  } else
      // We have observed that we are reclaiming more aggressively than
      // needed due to the conditions below. Hence, it is now controlled
      // via a variable now
      if (getenv("SQL_RECLAIM_AGGRESSIVELY") != NULL) {
    // Reclaim space from one other statement under certain conditions.
    int cutoff = MINOF(closeStmtListSize_ * 8, 800);
    // Reclaim if
    //  heap is crowded or
    //  less than 25% of the heap is free and
    //  nextReclaimStatement_ is closed earlier than cutoff
    //  or the recent fixup of a closed statement occurred earlier than
    //  the past 50 sequences.
    //  This is to prevent excessive fixup due to statement reclaim.
    if (crowded || freeSize <= (totalSize / (100 / stmtReclaimMinPctFree_))) {
      if (((currSequence_ - nextReclaimStatement_->closeSequence()) >= cutoff) ||
          ((currSequence_ - prevFixupSequence_) >= 50)) {
        retcode = reclaimStatementSpace();
        if (retcode) someStmtReclaimed = TRUE;
      }
    }
  }
  if (someStmtReclaimed) {
    logReclaimEvent((int)freeSize, (int)totalSize);
  }
  return someStmtReclaimed;
}

// True if OK to redrive.  False if no more statements to reclaim.
NABoolean ContextCli::reclaimStatementSpace() {
  while (nextReclaimStatement_ != NULL && (!nextReclaimStatement_->isReclaimable()))
    nextReclaimStatement_ = nextReclaimStatement_->prevCloseStatement();
  if (nextReclaimStatement_ == NULL) return FALSE;
  nextReclaimStatement_->releaseSpace();
  nextReclaimStatement_->closeSequence() -= 1;
  assert(nextReclaimStatement_->closeSequence() > 0);
  incStmtReclaimCount();
  nextReclaimStatement_ = nextReclaimStatement_->prevCloseStatement();
  return TRUE;
}

// Called at open or dealloc.
// [EL]
void ContextCli::removeFromCloseStatementList(Statement *statement, NABoolean forOpen) {
  if (statement->closeSequence() <= 0) return;

  // Relink the neighbors.
  if (statement->prevCloseStatement() != NULL)
    statement->prevCloseStatement()->nextCloseStatement() = statement->nextCloseStatement();
  if (statement->nextCloseStatement() != NULL)
    statement->nextCloseStatement()->prevCloseStatement() = statement->prevCloseStatement();

  // Decrement counters for closeStmtList_.
  decCloseStmtListSize();

  if (nextReclaimStatement_ == NULL ||
      statement->closeSequence() < nextReclaimStatement_->closeSequence()) {  // Indicates space has been reclaimed.
    // Then also decrement stmtReclaimCount_.
    decStmtReclaimCount();
    if (forOpen)  // fixup has occurred; record the current sequence.
      prevFixupSequence_ = currSequence_;
  }

  // Move forward the head of closeStatementList_.
  if (statement == closeStatementList_) closeStatementList_ = statement->nextCloseStatement();
  // Move backward nextReclaimedStatement_.
  if (statement == nextReclaimStatement_) nextReclaimStatement_ = statement->prevCloseStatement();

  // Clear related fields.
  statement->prevCloseStatement() = NULL;
  statement->nextCloseStatement() = NULL;
  statement->closeSequence() = 0;

  StmtListDebug1(statement, "Stmt %p has been removed from closeStatementList_", statement);
}

#if defined(_DEBUG)

void ContextCli::dumpStatementInfo() {
  // open output "stmtInfo" file.
  ofstream outstream("stmtInfo");

  Statement *statement;
  statementList()->position();
  while (statement = (Statement *)statementList()->getNext()) {
    statement->dump(&outstream);
  };
  outstream.flush();
  outstream.close();
};
#endif

void ContextCli::dumpStatementAndGlobalInfo(ostream *outstream) {
  NAHeap *globalExecutorHeap = exHeap_.getParent();
  globalExecutorHeap->dump(outstream, 0);
  // exHeap_.dump(outstream, 0);

  Statement *statement;
  statementList()->position();

  while (statement = (Statement *)statementList()->getNext()) {
    statement->dump(outstream);
  };
};

unsigned short ContextCli::getCurrentDefineContext() {
  unsigned short context_id = 0;

  return context_id;
}

NABoolean ContextCli::checkAndSetCurrentDefineContext() {
  unsigned short context_id = getCurrentDefineContext();
  if (defineContext() != context_id) {
    defineContext() = context_id;
    return TRUE;
  } else
    return FALSE;
}

TimeoutData *ContextCli::getTimeouts(NABoolean allocate) {
  if (NULL == timeouts_ && allocate) {
    timeouts_ = new (&exHeap_) TimeoutData(&exHeap_);
  }
  return timeouts_;
}

void ContextCli::clearTimeoutData()  // deallocate the TimeoutData
{
  if (timeouts_) {
    delete timeouts_;
    timeouts_ = NULL;
  }
}

void ContextCli::incrementTimeoutChangeCounter() { timeoutChangeCounter_++; }

UInt32 ContextCli::getTimeoutChangeCounter() { return timeoutChangeCounter_; }

void ContextCli::reset(void *contextMsg) {
  closeAllStatementsAndCursors();
  /*  retrieveContextInfo(contextMsg); */
}

void ContextCli::closeAllStatementsAndCursors() {
  // for all statements in the context close all of them that are open;
  Statement *statement;

  openStatementList()->position();
  while ((statement = (Statement *)openStatementList()->getNext())) {
    statement->close(diagsArea_);

    // statement->close() sets the state to close. As a side effect
    // the statement is removed from the openStatementList. Here
    // is the catch: Removing an entry from a hashQueue invalidates
    // the current_ in the HashQueue. Thus, we have to re-position

    openStatementList()->position();
  }
}

void ContextCli::buildRuntimeOptions(NAString &result, const char *options, const char *delimiters) {
  if (str_cmp_ne(options, "OFF") != 0 && str_cmp_ne(options, "ANYTHING") != 0) {
    result.append(options);
  }

  result.append(delimiters);
  result.append("-Dsqlmx.udr.host=");
  result.append(hostName_);

  result.append(delimiters);
  result.append("-Dsqlmx.udr.os_user=");
  result.append(osUserName_);

  result.append(delimiters);
  result.append("-Dsqlmx.udr.ip_address=");
  result.append(ipAddress_);

  result.append(delimiters);
  result.append("-Dsqlmx.udr.sessionid=");
  result.append(sessionID_);
}

// Method to acquire a reference to a UDR server associated with a
// given set of JVM startup options
ExUdrServer *ContextCli::acquireUdrServer(const char *runtimeOptions, const char *optionDelimiters,
                                          IpcThreadInfo *threadInfo, NABoolean dedicated) {
  ExUdrServer *udrServer = NULL;
  CliGlobals *cliGlobals = getCliGlobals();
  ExUdrServerManager *udrManager = cliGlobals->getUdrServerManager();

  if (udrRuntimeOptions_) {
    runtimeOptions = udrRuntimeOptions_;
    optionDelimiters = (udrRuntimeOptionDelimiters_ ? udrRuntimeOptionDelimiters_ : " ");
  }

  NAString tmp;

  buildRuntimeOptions(tmp, runtimeOptions, optionDelimiters);
  runtimeOptions = tmp.data();

  // First see if there is a matching server already servicing this
  // context
  udrServer = findUdrServer(runtimeOptions, optionDelimiters, threadInfo, dedicated);

  if (udrServer == NULL) {
    // Ask the UDR server manager for a matching server
    udrServer = udrManager->acquireUdrServer(databaseUserID_, runtimeOptions, optionDelimiters, authID_, authToken_,
                                             threadInfo, dedicated);

    ex_assert(udrServer, "Unexpected error while acquiring a UDR server");

    if (udrServerList_ == NULL) {
      // The UDR server list will typically be short so we can use a
      // small hash table size to save memory
      udrServerList_ = new (exCollHeap()) HashQueue(exCollHeap(), 23);
    }

    const char *hashData = runtimeOptions;
    ULng32 hashDataLength = str_len(runtimeOptions);
    udrServerList_->insert(hashData, hashDataLength, (void *)udrServer);

    if (dedicated) {
      udrServer->setDedicated(TRUE);

      // Note that the match logic used above in finding or acquiring a server
      // always looks for a server with reference count 0 to get a dedicated
      // server.
      ex_assert(udrServer->getRefCount() == 1, "Dedicated server request failed with reference count > 1.");
    }
  }

  if (dedicated) {
    udrServer->setDedicated(TRUE);

    // Note that the match logic used above in finding or acquiring a server
    // always looks for a server with reference count 0 to get a dedicated
    // server. Set the reference count to 1 now. Inserting a server flag
    // as dedicated prevents any other client( whether scalar or tmudf) from
    // sharing the server.
    udrServer->setRefCount(1);
  }

  return udrServer;
}

// When a context is going away or is changing its user identity, we
// need to release all references on ExUdrServer instances by calling
// the ExUdrServerManager interface. Note that the ExUdrServerManager
// may not stop a UDR server process when the reference is
// released. The server may be servicing other contexts or the
// ExUdrServerManager may have its own internal reasons for retaining
// the server.
void ContextCli::releaseUdrServers() {
  if (cliInterfaceTrigger_ != NULL) {
    NADELETE(cliInterfaceTrigger_, ExeCliInterface, exHeap());
    cliInterfaceTrigger_ = NULL;
  }

  if (udrServerList_) {
    ExUdrServerManager *manager = getCliGlobals()->getUdrServerManager();
    ExUdrServer *s;

    udrServerList_->position();
    while ((s = (ExUdrServer *)udrServerList_->getNext()) != NULL) {
      manager->releaseUdrServer(s);
    }

    delete udrServerList_;
    udrServerList_ = NULL;
  }
}

// We support a method to dynamically set UDR runtime options
// (currently the only supported options are JVM startup
// options). Once UDR options have been set, those options take
// precedence over options that were compiled into a plan.
int ContextCli::setUdrRuntimeOptions(const char *options, ULng32 optionsLen, const char *delimiters,
                                       ULng32 delimsLen) {
  // A NULL or empty option string tells us to clear the current UDR
  // options
  if (options == NULL || optionsLen == 0) {
    if (udrRuntimeOptions_) {
      exHeap()->deallocateMemory(udrRuntimeOptions_);
    }
    udrRuntimeOptions_ = NULL;

    if (udrRuntimeOptionDelimiters_) {
      exHeap()->deallocateMemory(udrRuntimeOptionDelimiters_);
    }
    udrRuntimeOptionDelimiters_ = NULL;

    return SUCCESS;
  }

  // A NULL or empty delimiter string gets changed to the default
  // single space delimiter string here
  if (delimiters == NULL || delimsLen == 0) {
    delimiters = " ";
    delimsLen = 1;
  }

  char *newOptions = (char *)exHeap()->allocateMemory(optionsLen + 1);
  char *newDelims = (char *)exHeap()->allocateMemory(delimsLen + 1);

  str_cpy_all(newOptions, options, optionsLen);
  newOptions[optionsLen] = 0;

  str_cpy_all(newDelims, delimiters, delimsLen);
  newDelims[delimsLen] = 0;

  if (udrRuntimeOptions_) {
    exHeap()->deallocateMemory(udrRuntimeOptions_);
  }
  udrRuntimeOptions_ = newOptions;

  if (udrRuntimeOptionDelimiters_) {
    exHeap()->deallocateMemory(udrRuntimeOptionDelimiters_);
  }
  udrRuntimeOptionDelimiters_ = newDelims;

  return SUCCESS;
}

// A lookup function to see if a UDR server with a given set of
// runtime options is already servicing this context
ExUdrServer *ContextCli::findUdrServer(const char *runtimeOptions, const char *optionDelimiters,
                                       IpcThreadInfo *threadInfo, NABoolean dedicated) {
  if (udrServerList_) {
    const char *hashData = runtimeOptions;
    ULng32 hashDataLength = str_len(runtimeOptions);
    ExUdrServer *s;

    udrServerList_->position(hashData, hashDataLength);
    while ((s = (ExUdrServer *)udrServerList_->getNext()) != NULL) {
      // We are looking for servers that have been acquired from server manager
      // as dedicated. We don't want to mix shared and dedicated servers.
      if (dedicated != s->isDedicated()) {
        continue;
      }

      // If dedicated server is being requested, make sure it is not being used within
      // contextCli scope by others.
      if (dedicated && s->inUse()) {
        continue;
      }

      // Also make sure other runtimeOptions match.
      if (s->match(databaseUserID_, runtimeOptions, optionDelimiters, threadInfo)) {
        return s;
      }
    }
  }

  return NULL;
}
void ContextCli::logReclaimEvent(int freeSize, int totalSize) {
  int totalContexts = 0;
  int totalStatements = 0;
  ContextCli *context;

  if (!cliGlobals_->logReclaimEventDone()) {
    cliSemaphore_->get();
    totalContexts = cliGlobals_->getContextList()->numEntries();
    cliGlobals_->getContextList()->position();
    while ((context = (ContextCli *)cliGlobals_->getContextList()->getNext()) != NULL)
      if (context->statementList()) totalStatements += context->statementList()->numEntries();
    cliSemaphore_->release();
    SQLMXLoggingArea::logCliReclaimSpaceEvent(freeSize, totalSize, totalContexts, totalStatements);
    cliGlobals_->setLogReclaimEventDone(TRUE);
  }
}
// The following new function sets the version of the compiler to be
// used by the context. If the compiler had already been started, it
// uses the one in the context. If not, it starts one and  returns the
// index into the compiler array for the compiler that has been
// started.

int ContextCli::setOrStartCompiler(short mxcmpVersionToUse, char *remoteNodeName, short &indexIntoCompilerArray) {
  short found = FALSE;
  short i = 0;
  int retcode = SUCCESS;

  if (mxcmpVersionToUse == COM_VERS_R2_FCS || mxcmpVersionToUse == COM_VERS_R2_1)
    // Use R2.2 compiler for R2.0 and R2.1 nodes
    mxcmpVersionToUse = COM_VERS_R2_2;

  for (i = 0; i < getNumArkcmps(); i++) {
    if (remoteNodeName && (str_cmp_ne(cliGlobals_->getArkcmp(i)->getNodeName(), remoteNodeName) == 0)) {
      setVersionOfMxcmp(mxcmpVersionToUse);
      setMxcmpNodeName(remoteNodeName);
      setIndexToCompilerArray(i);
      found = TRUE;
      break;
    } else if ((cliGlobals_->getArkcmp(i))->getVersion() == mxcmpVersionToUse) {
      setVersionOfMxcmp(mxcmpVersionToUse);
      setMxcmpNodeName(NULL);
      setIndexToCompilerArray(i);
      found = TRUE;
      break;
    }
  }
  if (!found)

  {
    // Create a new ExSqlComp and insert into the arrary
    // of Sqlcomps in the context.

    setArkcmp(new (exCollHeap()) ExSqlComp(0, exCollHeap(), cliGlobals_, 0, mxcmpVersionToUse, remoteNodeName, env_));

    setArkcmpFailMode(ContextCli::arkcmpIS_OK_);
    short numArkCmps = getNumArkcmps();
    setIndexToCompilerArray((short)(getNumArkcmps() - 1));
    setVersionOfMxcmp(mxcmpVersionToUse);
    setMxcmpNodeName(remoteNodeName);
    i = (short)(getNumArkcmps() - 1);
  }
  indexIntoCompilerArray = i;
  return retcode;
}

void ContextCli::addToStatementWithEspsList(Statement *statement) { statementWithEspsList_->insert((void *)statement); }

void ContextCli::removeFromStatementWithEspsList(Statement *statement) {
  statementWithEspsList_->remove((void *)statement);
}

bool ContextCli::unassignEsps(bool force) {
  bool didUnassign = false;
  int espAssignDepth = getSessionDefaults()->getEspAssignDepth();
  Statement *lastClosedStatement = NULL;

  if (force || ((espAssignDepth != -1) && (statementWithEspsList_->numEntries() >= espAssignDepth))) {
    statementWithEspsList_->position();
    Statement *lastStatement = (Statement *)statementWithEspsList_->getNext();
    while (lastStatement) {
      if ((lastStatement->getState() == Statement::CLOSE_) && (lastStatement->isReclaimable()))
        lastClosedStatement = lastStatement;
      lastStatement = (Statement *)statementWithEspsList_->getNext();
    }
  }
  if (lastClosedStatement) {
    lastClosedStatement->releaseSpace();
    // Need to do same things that reclaimStatementSpace would do
    incStmtReclaimCount();
    if (lastClosedStatement == nextReclaimStatement_) {
      nextReclaimStatement_->closeSequence() -= 1;
      nextReclaimStatement_ = nextReclaimStatement_->prevCloseStatement();
    }
    statementWithEspsList_->remove((void *)lastClosedStatement);
    didUnassign = true;
  }
  return didUnassign;
}

void ContextCli::addToValidDDLCheckList(SqlTableOpenInfoPtr *stoiList, int tableCount) {
  // Find out if this is a tdm_arkcmp process (unfortunately, the
  // CmpCommon::context() object is created later than ContextCli,
  // so we can't do this calculation in the ContextCli constructor)
  if (isTdmArkcmp_ == UNKNOWN) {
    if (CmpCommon::context() && CmpCommon::context()->isMxcmp())
      isTdmArkcmp_ = IS_TDMARKCMP;
    else
      isTdmArkcmp_ = IS_NOT_TDMARKCMP;
  }

  // We only want to check statements in the top-level ContextCli object.
  // Recursed ContextCli objects are used for DDL and utility operations
  // and we don't want to bother checking for DDL changes on metadata or
  // utility-related tables. Too, commit time processing only looks at
  // the top-level ContextCli object so anything we stash here for lower
  // level objects will not get checked.
  //
  // The top-level ContextCli object always has getNumOfCliCalls equal to 1.
  // This isn't sufficient by itself, though, as sometimes recursion goes
  // off-process. When that happens, there is another ContextCli object
  // with getNumCliCalls equal to 1 in the tdm_arkcmp process. So we need
  // to exclude that as well. (Note that if we ever start transactions in
  // the tdm_arkcmp process that do writes and are independent of the
  // transaction in the master executor process, we'd need to revise this
  // test.)
  if ((getNumOfCliCalls() == 1) &&         // if top level ContextCli object
      (isTdmArkcmp_ == IS_NOT_TDMARKCMP))  // and we haven't recursed off-process
  {
    StatsGlobals *statsGlobals = cliGlobals_->getStatsGlobals();
    ObjectEpochCache *objectEpochCache = statsGlobals->getObjectEpochCache();
    for (int i = 0; i < tableCount; i++) {
      SqlTableOpenInfo *stoi = stoiList[i];

      if (stoi->ddlValidationRequired())  // if there is a write to this object
      {
        // check to see if the object is already on the list
        const char *objectName = stoi->ansiName();
        CliDDLValidator *ddlValidator = NULL;

        validDDLCheckList_->position();
        while (ddlValidator = (CliDDLValidator *)validDDLCheckList_->getCurr()) {
          if (strcmp(objectName, ddlValidator->objectName()) == 0) break;
          validDDLCheckList_->advance();
        }

        if (!ddlValidator)  // if not already on the list
        {
          // obtain the ObjectEpochCacheEntry for this object
          ObjectEpochCacheEntryName oecName(&exHeap_, objectName, COM_BASE_TABLE_OBJECT, 0);
          ObjectEpochCacheEntry *oecEntry = NULL;
          ObjectEpochCacheKey key(objectName, oecName.getHash(), COM_BASE_TABLE_OBJECT);

          short retcode = objectEpochCache->getEpochEntry(oecEntry /* out */, key);
          ex_assert(oecEntry, "Unable to retrieve entry epoch");

          // TODO: what to do if there is an error? oecEntry will be NULL in that case

          // create a CliDDLValidator object and put it on the list
          ddlValidator = new (&exHeap_)
              CliDDLValidator(&exHeap_, objectName, stoi->getExpectedEpoch(), stoi->getExpectedFlags(), oecEntry);
          validDDLCheckList_->insert(ddlValidator);

#if 0
          // Useful debugging code
          cout << "In addToValidDDLCheckList: DDL validator for object " << objectName << " added, retcode = " << retcode << endl;
#endif
        }
      }
    }
  }
}

bool ContextCli::doValidDDLChecks(NABoolean inRollback) {
  bool rc = true;  // assume all is well
  CliDDLValidator *ddlValidator = NULL;
  while (ddlValidator = (CliDDLValidator *)validDDLCheckList_->getHead()) {
    bool validDDL = true;
    validDDLCheckList_->removeHead();
    if (!inRollback) {
      if (!ddlValidator->isDDLValid()) {
        // the DDL has changed for some object associated with
        // these stats, so raise an error
        rc = false;
        validDDL = false;
      }
    }

#if 0
    // Useful debug code
    cout << "In doValidDDLChecks: object " << ddlValidator->objectName() << " processed." << endl;
    cout << "  inRollback = " << inRollback << ", isDDLValid() returned " << validDDL << "." << endl;
#endif

    delete ddlValidator;
  }
  return rc;
}

short ContextCli::ddlResetObjectEpochs(ComDiagsArea *diags) {
  return CmpSeabaseDDL::ddlResetObjectEpochs(TRUE /*do abort*/, TRUE /*clear list*/, diags);
}

void ContextCli::addToCursorList(Statement &s) {
  SQLSTMT_ID *cursorName = s.getCursorName();
  const char *id = cursorName->identifier;
  int idLen = getIdLen(cursorName);

  StmtListDebug1(&s, "Adding %p to cursorList_", &s);
  cursorList_->insert(id, idLen, &s);
}

// Remove a child statement such as a clone or a stored procedure
// result set proxy from the context's statement lists. Do not call
// the child's destructor (that job is left to the parent
// statement's destructor).
RETCODE ContextCli::cleanupChildStmt(Statement *child) {
  StmtListDebug1(child, "[BEGIN cleanupChildStmt] child %p", child);

  if (!child) return SUCCESS;

  // One goal in this method is to remove child from the
  // statementList() list. Instead of using a global search in
  // statementList() to locate and remove child, we build a hash key
  // first and do a hash-based lookup. The hash key is build from
  // components of child's SQLSTMT_ID structure.

  SQLSTMT_ID *statement_id = child->getStmtId();
  Statement *lookupResult = getStatement(statement_id, statementList());

  StmtListDebug1(child, "  lookupResult is %p", lookupResult);

  if (!lookupResult) {
    StmtListDebug0(child, "  *** lookupResult is NULL, returning ERROR");
    StmtListDebug1(child, "[END cleanupChildStmt] child %p", child);
    diagsArea_ << DgSqlCode(-CLI_STMT_NOT_EXISTS);
    return ERROR;
  }

  StmtListDebug1(lookupResult, "  Removing %p from statementList_", lookupResult);
  semaphoreLock();
  statementList()->remove(lookupResult);
  if (processStats_ != NULL) processStats_->decStmtCount(lookupResult->getStatementType());
  semaphoreRelease();

  if (child->getStatementType() != Statement::STATIC_STMT) removeCursor(child->getCursorName(), child->getModuleId());

  removeFromCloseStatementList(child, FALSE);

  StmtListDebug1(child, "[END cleanupChildStmt] child %p", child);
  return SUCCESS;

}  // ContextCLI::cleanupChildStmt()

#ifdef _DEBUG
void ContextCli::StmtListPrintf(const Statement *s, const char *formatString, ...) const {
  if (s) {
    if (s->stmtDebugEnabled() || s->stmtListDebugEnabled()) {
      FILE *f = stdout;
      va_list args;
      va_start(args, formatString);
      fprintf(f, "[STMTLIST] ");
      vfprintf(f, formatString, args);
      fprintf(f, "\n");
      fflush(f);
    }
  }
}
#endif
void ContextCli::genSessionId() {
  getCliGlobals()->genSessionUniqueNumber();
  char si[ComSqlId::MAX_SESSION_ID_LEN];

  // On Linux, database user names can potentially be too long to fit
  // into the session ID.
  //
  // If the user ID is >= 0: The user identifier inside the session ID
  // will be "U<user-id>".
  //
  // If the user ID is less than zero: The identifier will be
  // "U_NEG_<absolute value of user-id>".
  //
  //  The U_NEG_ prefix is used, rather than putting a minus sign into
  // the string, so that the generated string is a valid regular
  // identifier.

  // The userID type is unsigned. To test for negative numbers we
  // have to first cast the value to a signed data type.
  int localUserID = (int)databaseUserID_;

  char userName[32];
  if (localUserID >= 0)
    sprintf(userName, "U%d", (int)localUserID);
  else
    sprintf(userName, "U_NEG_%d", (int)-localUserID);
  int userNameLen = strlen(userName);

  char tenantIdString[32];
  if (tenantID_ >= 0)
    sprintf(tenantIdString, "T%d", (int)tenantID_);
  else
    sprintf(tenantIdString, "T_NEG_%d", (int)-tenantID_);

  int tenantIdLen = strlen(tenantIdString);

  // generate a unique session ID and set it in context.
  int sessionIdLen;
  ComSqlId::createSqlSessionId(si, ComSqlId::MAX_SESSION_ID_LEN, sessionIdLen, getCliGlobals()->myNodeNumber(),
                               getCliGlobals()->myCpu(), getCliGlobals()->myPin(), getCliGlobals()->myStartTime(),
                               (int)getCliGlobals()->getSessionUniqueNumber(), userNameLen, userName, tenantIdLen,
                               tenantIdString, (int)strlen(userSpecifiedSessionName_), userSpecifiedSessionName_);

  setSessionId(si);
}

void ContextCli::startUdrServer() {
  if (CmpCommon::getDefault(TRAF_START_UDR) == DF_ON && cliGlobals_->isMasterProcess()) {
    ExeCliInterface cliInterface(exHeap(), 0, this);
    char sql[] = "EXECUTE 'BEGIN NULL; END;'";
    int cliRC = cliInterface.executeImmediate(sql);
  }
}

void ContextCli::createMxcmpSession() {
  if ((mxcmpSessionInUse_) && (NOT userNameChanged_)) {
    startUdrServer();
    return;
  }

  // Steps to perform
  // 1. Send the user details to mxcmp
  // 2. Send CQD's  to mxcmp
  // 3. Set the mxcmpSessionInUse_ flag to TRUE

  // Send the user details (auth state, userID, and username
  CmpContext *cmpCntxt = CmpCommon::context();
  ex_assert(cmpCntxt, "No compiler context exists");
  NABoolean authOn = cmpCntxt->isAuthorizationEnabled();
  NAText serializedNodes;

  if (availableNodes_) availableNodes_->serialize(serializedNodes);

  // The message contains the following:
  //   (the fields are delimited by commas)
  //     authorization state (0 - off, 1 - on)
  //     integer user ID
  //     database user name
  //     integer tenant ID
  //     tenant name
  //     tenant nodes
  //     tenant default schema
  // See CmpStatement::process (CmpMessageDatabaseUser) for more details

  char userMessage[2 * MAX_AUTHID_AS_STRING_LEN + 2 + 2 * MAX_USERNAME_LEN + 2 + serializedNodes.size() + 12 +
                   COL_MAX_SCHEMA_LEN + 1];
  int bytesWritten = snprintf(userMessage, sizeof(userMessage), "%d,%d,%s,%d,%s,%s,%s", authOn, databaseUserID_,
                              databaseUserName_, tenantID_, tenantName_, serializedNodes.c_str(), tenantDefaultSchema_);
  ex_assert(bytesWritten > 0 && bytesWritten < sizeof(userMessage), "message buffer to compiler is too small");

  char *pMessage = (char *)&userMessage;

  int cmpStatus = 2;  // assume failure
  if (getSessionDefaults()->callEmbeddedArkcmp() && isEmbeddedArkcmpInitialized() && CmpCommon::context() &&
      (CmpCommon::context()->getRecursionLevel() == 0)) {
    char *dummyReply = NULL;
    ULng32 dummyLen;
    ComDiagsArea *diagsArea = NULL;
    cmpStatus = CmpCommon::context()->compileDirect(pMessage, (ULng32)sizeof(userMessage), &exHeap_,
                                                    SQLCHARSETCODE_UTF8, EXSQLCOMP::DATABASE_USER, dummyReply, dummyLen,
                                                    getSqlParserFlags(), NULL, 0, diagsArea);
    if (cmpStatus != 0) {
      char emsText[120];
      str_sprintf(emsText, "Set DATABASE_USER in embedded arkcmp failed, return code %d", cmpStatus);
      SQLMXLoggingArea::logExecRtInfo(__FILE__, __LINE__, emsText, 0);
    }

    if (dummyReply != NULL) {
      exHeap_.deallocateMemory((void *)dummyReply);
      dummyReply = NULL;
    }
    if (diagsArea != NULL) {
      diagsArea->decrRefCount();
      diagsArea = NULL;
    }
  }

  // if there is an error using embedded compiler or we are already in the
  // embedded compiler, send this to a regular compiler .
  if (!getSessionDefaults()->callEmbeddedArkcmp() ||
      (getSessionDefaults()->callEmbeddedArkcmp() && CmpCommon::context() &&
       (cmpStatus != 0 || (CmpCommon::context()->getRecursionLevel() > 0) || getArkcmp()->getServer()))) {
    short indexIntoCompilerArray = getIndexToCompilerArray();
    ExSqlComp *exSqlComp = cliGlobals_->getArkcmp(indexIntoCompilerArray);
    ex_assert(exSqlComp, "CliGlobals::getArkcmp() returned NULL");
    exSqlComp->sendRequest(EXSQLCOMP::DATABASE_USER, (const char *)pMessage, (ULng32)strlen(pMessage));
  }

  // Send one of two CQDs to the compiler
  //
  // If mxcmpSessionInUse_ is FALSE:
  //   CQD SESSION_ID '<ID>'
  //     where <ID> is getSessionId()
  //
  // Otherwise:
  //   CQD SESSION_USERNAME '<name>'
  //     where <name> is databaseUserName_

  const char *cqdName = "";
  const char *cqdValue = "";

  if (NOT mxcmpSessionInUse_) {
    cqdName = "SESSION_ID";
    cqdValue = getSessionId();
  } else {
    cqdName = "SESSION_USERNAME";
    cqdValue = databaseUserName_;
  }

  UInt32 numBytes =
      strlen("CONTROL QUERY DEFAULT ") + strlen(cqdName) + strlen(" '") + strlen(cqdValue) + strlen("';") + 1;

  char *sendCQD = new (exHeap()) char[numBytes];

  strcpy(sendCQD, "CONTROL QUERY DEFAULT ");
  strcat(sendCQD, cqdName);
  strcat(sendCQD, " '");
  strcat(sendCQD, cqdValue);
  strcat(sendCQD, "';");

  ExeCliInterface cliInterface(exHeap(), 0, this);
  int cliRC = cliInterface.executeImmediate(sendCQD);
  NADELETEBASIC(sendCQD, exHeap());

  // Send the LDAP user name
  char *userName = new (exHeap()) char[ComSqlId::MAX_LDAP_USER_NAME_LEN + 1];
  memset(userName, 0, ComSqlId::MAX_LDAP_USER_NAME_LEN + 1);
  if (externalUsername_)
    strcpy(userName, externalUsername_);
  else if (databaseUserName_)
    strcpy(userName, databaseUserName_);

  if (userName) {
    char *sendCQD1 = new (exHeap()) char[strlen("CONTROL QUERY DEFAULT ") + strlen("LDAP_USERNAME ") + strlen("'") +
                                         strlen(userName) + strlen("';") + 1];
    strcpy(sendCQD1, "CONTROL QUERY DEFAULT ");
    strcat(sendCQD1, "LDAP_USERNAME '");
    strcat(sendCQD1, userName);
    strcat(sendCQD1, "';");

    cliRC = cliInterface.executeImmediate(sendCQD1);
    NADELETEBASIC(sendCQD1, exHeap());
    NADELETEBASIC(userName, exHeap());
  }

  // load querycache when first connect to mxosrvr. NOT sqlci
  // execute any statement, first time when look up for cachekey, querycache will be loaded
  if (authOn && GetCliGlobals()->isMasterProcess() && NOT GetCliGlobals()->isSqlciProcess() &&
      (CmpCommon::getDefault(GENERATE_USER_QUERYCACHE) == DF_ON ||
       CmpCommon::getDefault(GENERATE_USER_QUERYCACHE) == DF_PRELOAD)) {
    try {
      cliRC = cliInterface.executeImmediate("select 1 from dual;");
    } catch (...) {
    }
  }
  // TODO: send Tenant name also?

  // Set the "mxcmp in use" flag
  mxcmpSessionInUse_ = TRUE;
  startUdrServer();
}

// ----------------------------------------------------------------------------
// Method:  updateMxcmpSession
//
// Updates security attributes in child arkcmp
//
// Returns: 0 = succeeded; -1 = failed
// ----------------------------------------------------------------------------
int ContextCli::updateMxcmpSession() {
  // If no child arkcmp, just return
  if (getArkcmp()->getServer() == NULL) return 0;

  // Send changed user information to arkcmp process
  CmpContext *cmpCntxt = CmpCommon::context();
  ex_assert(cmpCntxt, "No compiler context exists");
  NABoolean authOn = cmpCntxt->isAuthorizationEnabled();
  NAText serializedNodes;

  if (availableNodes_) availableNodes_->serialize(serializedNodes);

  // The message contains the following:
  //   (the fields are delimited by commas)
  //     authorization state (0 - off, 1 - on)
  //     integer user ID
  //     database user name
  //     integer tenant ID
  //     tenant name
  //     tenant nodes
  //     tenant default schema
  // See CmpStatement::process (CmpMessageDatabaseUser) for more details

  char userMessage[2 * MAX_AUTHID_AS_STRING_LEN + 2 + 2 * MAX_USERNAME_LEN + 2 + serializedNodes.size() + 12 +
                   COL_MAX_SCHEMA_LEN + 1];
  int bytesWritten = snprintf(userMessage, sizeof(userMessage), "%d,%d,%s,%d,%s,%s,%s", authOn, databaseUserID_,
                              databaseUserName_, tenantID_, tenantName_, serializedNodes.c_str(), tenantDefaultSchema_);
  ex_assert(bytesWritten > 0 && bytesWritten < sizeof(userMessage), "message buffer to compiler is too small");

  char *pMessage = (char *)&userMessage;

  // Send message to child arkcmp, if one exists
  ExSqlComp::ReturnStatus cmpStatus;
  ExSqlComp *exSqlComp = getArkcmp();
  ex_assert(exSqlComp, "CliGlobals::getArkcmp() returned NULL");
  cmpStatus = exSqlComp->sendRequest(EXSQLCOMP::DATABASE_USER, (const char *)pMessage, (ULng32)strlen(pMessage));
  if (cmpStatus == ExSqlComp::ERROR) return -1;

  return 0;
}

void ContextCli::beginSession(char *userSpecifiedSessionName) {
  if (userSpecifiedSessionName)
    strcpy(userSpecifiedSessionName_, userSpecifiedSessionName);
  else
    strcpy(userSpecifiedSessionName_, "");

  genSessionId();

  initVolTabList();

  // save the current session default settings.
  // We restore these settings at session end or if user issues a
  // SSD reset stmt.
  if (sessionDefaults_) sessionDefaults_->saveSessionDefaults();

  if (aqrInfo()) aqrInfo()->saveAQRErrors();

  sessionInUse_ = TRUE;
  if (getSessionDefaults()) getSessionDefaults()->beginSession();

  setInMemoryObjectDefn(FALSE);
  clientConnected_ = TRUE;
}

void ContextCli::endMxcmpSession(NABoolean cleanupEsps, NABoolean clearCmpCache) {
  int flags = 0;
  char *dummyReply = NULL;
  ULng32 dummyLength;

  if (cleanupEsps) flags |= CmpMessageEndSession::CLEANUP_ESPS;

  flags |= CmpMessageEndSession::RESET_ATTRS;

  if (clearCmpCache) flags |= CmpMessageEndSession::CLEAR_CACHE;

  int cmpStatus = 2;  // assume failure
  if (getSessionDefaults()->callEmbeddedArkcmp() && isEmbeddedArkcmpInitialized() && CmpCommon::context() &&
      (CmpCommon::context()->getRecursionLevel() == 0)) {
    char *dummyReply = NULL;
    ULng32 dummyLen;
    ComDiagsArea *diagsArea = NULL;
    cmpStatus = CmpCommon::context()->compileDirect((char *)&flags, (ULng32)sizeof(int), &exHeap_,
                                                    SQLCHARSETCODE_UTF8, EXSQLCOMP::END_SESSION, dummyReply, dummyLen,
                                                    getSqlParserFlags(), NULL, 0, diagsArea);
    if (cmpStatus != 0) {
      char emsText[120];
      str_sprintf(emsText, "Set END_SESSION in embedded arkcmp failed, return code %d", cmpStatus);
      SQLMXLoggingArea::logExecRtInfo(__FILE__, __LINE__, emsText, 0);
    }

    if (dummyReply != NULL) {
      exHeap_.deallocateMemory((void *)dummyReply);
      dummyReply = NULL;
    }
    if (diagsArea != NULL) {
      diagsArea->decrRefCount();
      diagsArea = NULL;
    }
  }

  // if there is an error using embedded compiler or we are already in the
  // embedded compiler, send this to a regular compiler .
  if (!getSessionDefaults()->callEmbeddedArkcmp() ||
      (getSessionDefaults()->callEmbeddedArkcmp() && CmpCommon::context() &&
       (cmpStatus != 0 || (CmpCommon::context()->getRecursionLevel() > 0) || getArkcmp()->getServer()))) {
    // send request to mxcmp so it can cleanup esps started by it
    // and reset nonResetable attributes.
    // Ignore errors.
    if (getNumArkcmps() > 0) {
      short indexIntoCompilerArray = getIndexToCompilerArray();
      ExSqlComp::ReturnStatus status = cliGlobals_->getArkcmp(indexIntoCompilerArray)
                                           ->sendRequest(EXSQLCOMP::END_SESSION, (char *)&flags, sizeof(int));

      if (status == ExSqlComp::SUCCESS) {
        status = cliGlobals_->getArkcmp(indexIntoCompilerArray)->getReply(dummyReply, dummyLength);
        cliGlobals_->getArkcmp(indexIntoCompilerArray)->getHeap()->deallocateMemory((void *)dummyReply);
      }
    }
  }  // end if (getSessionDefaults()->callEmbeddedArkcmp() && ...
}

void ContextCli::resetAttributes() {
  // reset all parserflags
  sqlParserFlags_ = 0;

  // reset attributes which are set for the session based on
  // the caller. These do not persist across multiple session.
  // For example,  scripts, or defaults.
  if (sessionDefaults_) sessionDefaults_->resetSessionOnlyAttributes();
}

int ContextCli::reduceEsps() {
  int countBefore = cliGlobals_->getEspManager()->getNumOfEsps();
  // make assigned ESPs to be idle.
  while (unassignEsps(true))
    ;
  // get rid of idle ESPs.
  cliGlobals_->getEspManager()->endSession(this);
  return countBefore - cliGlobals_->getEspManager()->getNumOfEsps();
}

void ContextCli::endSession(NABoolean cleanupEsps, NABoolean cleanupEspsOnly, NABoolean cleanupOpens,
                            NABoolean cleanupCmpContext) {
  short rc = 0;
  if (NOT cleanupEspsOnly) {
    rc = ExExeUtilCleanupVolatileTablesTcb::dropVolatileTables(this, exHeap());
    SQL_EXEC_ClearDiagnostics(NULL);
  }

  if (clientInfo_ != NULL) {
    NADELETEBASIC(clientInfo_, exCollHeap());
    clientInfo_ = NULL;
  }

  if (cliInterfaceTrigger_ != NULL) {
    NADELETE(cliInterfaceTrigger_, ExeCliInterface, exHeap());
    cliInterfaceTrigger_ = NULL;
  }
  // normally when user disconnects, by default odbc calls cli end session
  // without the cleanup esp parameter. so master will only kill idle esps.
  // however, user can set two parameters at data source level:
  //
  //   - number of disconnects (e.g. 10)
  //   - session cleanup time (e.g. 30 minutes)
  //
  // so after user disconnects 10 times or disconnects after session alive
  // for 30 minutes, odbc will call cli end session with the cleanup esp
  // parameter. the result is master will kill all esps in cache that
  // currently are not being used.
  //
  // SET SESSION DEFAULT SQL_SESSION 'END:CLEANUP_ESPS';
  // if cleanupEsps flag is set from above session default, then kill all esps
  // in esp cache that are not currently being used. otherwise, kill esps that
  // have been idle longer than session default ESP_STOP_IDLE_TIMEOUT.
  if (cleanupEsps)
    cliGlobals_->getEspManager()->endSession(this);
  else
    cliGlobals_->getEspManager()->stopIdleEsps(this);

  if (cleanupOpens) {
    closeAllTables();
  }

  // restore all SSD settings to their initial value
  if (sessionDefaults_) sessionDefaults_->restoreSessionDefaults();

  // restore all entries for aqr that were set in this session
  // to their initial value.
  if (aqrInfo()) {
    if ((aqrInfo()->restoreAQRErrors() == FALSE) && sessionInUse_) {
      diagsArea_ << DgSqlCode(-CLI_INTERNAL_ERROR);
      diagsArea_ << DgString0(":Saved AQR error map is empty");
    }
  }
  if (NOT cleanupEspsOnly) {
    strcpy(userSpecifiedSessionName_, "");

    sessionInUse_ = FALSE;
  }
  clientConnected_ = FALSE;
  killAndRecreateMxcmp();
  if (rc < 0) {
    // an error was returned during drop of tables in the volatile schema.
    // Drop the schema and mark it as obsolete.
    dropSession();
  } else {
    endMxcmpSession(cleanupEsps);
    resetAttributes();
  }

  // With ldap user identities, user identity token changes everytime a user
  // logs on. So UDR Server needs to get token everytime an mxcs connection
  // is reassigned which is not being done currently.
  // Instead, since UDR Server always gets the token duing startup, it
  // is being killed when the client logs off and restared when a client
  // logs on and assigned the same connection.
  releaseUdrServers();
  cliGlobals_->setLogReclaimEventDone(FALSE);
  // Reset the stats area to ensure that the reference count in conext_->prevStmtStats_ is
  // decremented so that it can be freed up when GC happens in ssmp
  setStatsArea(NULL, FALSE, FALSE, TRUE);

  if (cleanupCmpContext) {
    for (short i = 0; i < cmpContextInfo_.entries(); i++) {
      CmpContextInfo *cci = cmpContextInfo_[i];
      CmpContext *cmpContext = cci->getCmpContext();
      NAHeap *heap = cmpContext->heap();

      cmpCurrentContext = cmpContext;
      if (i == 0) {
        cmpContext->releaseAllObjectLocks();
      }
      NADELETE(cmpContext, CmpContext, heap);
      NADELETE(heap, NAHeap, &exHeap_);
      NADELETE(cci, CmpContextInfo, &exHeap_);
      cmpCurrentContext = NULL;
    }
    cmpContextInfo_.clear();
    isEmbeddedArkcmpInitialized_ = false;
    embeddedArkcmpContext_ = NULL;
    mxcmpSessionInUse_ = false;
    NADefaults::setReadFromDefaultsTable(FALSE);
  }
}

void ContextCli::dropSession(NABoolean clearCmpCache) {
  short rc = 0;
  ComDiagsArea *diags = NULL;
  if (volatileSchemaCreated_) {
    rc = ExExeUtilCleanupVolatileTablesTcb::dropVolatileSchema(this, NULL, exHeap(), diags);
    if (rc < 0 && diags != NULL && diags->getNumber(DgSqlCode::ERROR_) > 0) {
      ComCondition *condition = diags->getErrorEntry(1);
      logAnMXEventForError(*condition, GetCliGlobals()->getEMSEventExperienceLevel());
    }
    volatileSchemaCreated_ = FALSE;
    SQL_EXEC_ClearDiagnostics(NULL);
  }

  if (mxcmpSessionInUse_) {
    // now send NULL session id to mxcmp
    char *sendCQD = new (exHeap()) char[strlen("CONTROL QUERY DEFAULT SESSION_ID '';") + 1];
    strcpy(sendCQD, "CONTROL QUERY DEFAULT SESSION_ID '';");

    ExeCliInterface cliInterface(exHeap(), 0, this);
    int cliRC = cliInterface.executeImmediate(sendCQD);
    NADELETEBASIC(sendCQD, exHeap());

    endMxcmpSession(TRUE, clearCmpCache);
  }

  resetAttributes();

  mxcmpSessionInUse_ = FALSE;

  sessionInUse_ = FALSE;

  // Reset the stats area to ensure that the reference count in
  // prevStmtStats_ is decremented so that it can be freed up when
  // GC happens in mxssmp
  setStatsArea(NULL, FALSE, FALSE, TRUE);
  DistributedLock_JNI::deleteInstance();
  disconnectHdfsConnections();
}

void ContextCli::resetVolatileSchemaState() {
  ComDiagsArea *savedDiagsArea = NULL;
  if (diagsArea_.getNumber() > 0) {
    savedDiagsArea = ComDiagsArea::allocate(exHeap());
    savedDiagsArea->mergeAfter(diagsArea_);
  }

  // reset volatile schema usage in mxcmp
  char *sendCQD = new (exHeap()) char[strlen("CONTROL QUERY DEFAULT VOLATILE_SCHEMA_IN_USE 'FALSE';") + 1];
  strcpy(sendCQD, "CONTROL QUERY DEFAULT VOLATILE_SCHEMA_IN_USE 'FALSE';");

  ExeCliInterface cliInterface(exHeap());
  int cliRC = cliInterface.executeImmediate(sendCQD);
  NADELETEBASIC(sendCQD, exHeap());

  if (savedDiagsArea) {
    diagsArea_.mergeAfter(*savedDiagsArea);
  }

  if (savedDiagsArea) {
    savedDiagsArea->clear();
    savedDiagsArea->deAllocate();
  }
}

void ContextCli::setSessionId(char *si) {
  if (sessionID_) exHeap()->deallocateMemory(sessionID_);

  if (si) {
    sessionID_ = (char *)exHeap()->allocateMemory(strlen(si) + 1);
    strcpy(sessionID_, si);
  } else
    sessionID_ = NULL;
}

void ContextCli::initVolTabList() {
  if (volTabList_) delete volTabList_;

  volTabList_ = new (exCollHeap()) HashQueue(exCollHeap());
}

void ContextCli::resetVolTabList() {
  if (volTabList_) delete volTabList_;

  volTabList_ = NULL;
}

void ContextCli::closeAllTables() {}

// this method is used to send a transaction operation specific message
// to arkcmp by executor.
// Based on that, arkcmp takes certain actions.
// Called from executor/ex_transaction.cpp.
// Note: most of the code in this method has been moved from ex_transaction.cpp
ExSqlComp::ReturnStatus ContextCli::sendXnMsgToArkcmp(char *data, int dataSize, int xnMsgType,
                                                      ComDiagsArea *&diagsArea) {
  // send the set trans request to arkcmp so compiler can
  // set this trans mode in its memory. This is done so any
  // statement compiled after this could be compiled with the
  // current trans mode.
  ExSqlComp *cmp = NULL;
  ExSqlComp::ReturnStatus cmpStatus;
  // the dummyReply is moved up because otherwise the
  // compiler would complain about
  // initialization of variables afer goto statements.

  char *dummyReply = NULL;
  ULng32 dummyLength;
  ContextCli *currCtxt = this;
  int cmpRet = 0;

  // If use embedded compiler, send the settings to it
  if (currCtxt->getSessionDefaults()->callEmbeddedArkcmp() && currCtxt->isEmbeddedArkcmpInitialized() &&
      (CmpCommon::context()) && (CmpCommon::context()->getRecursionLevel() == 0)) {
    NAHeap *arkcmpHeap = currCtxt->exHeap();

    cmpRet = CmpCommon::context()->compileDirect(data, dataSize, arkcmpHeap, SQLCHARSETCODE_UTF8,
                                                 CmpMessageObj::MessageTypeEnum(xnMsgType), dummyReply, dummyLength,
                                                 currCtxt->getSqlParserFlags(), NULL, 0, diagsArea);
    if (cmpRet != 0) {
      char emsText[120];
      str_sprintf(emsText, "Set transaction mode to embedded arkcmp failed, return code %d", cmpRet);
      SQLMXLoggingArea::logExecRtInfo(__FILE__, __LINE__, emsText, 0);
      diagsArea = CmpCommon::diags();
    }

    if (dummyReply != NULL) {
      arkcmpHeap->deallocateMemory((void *)dummyReply);
      dummyReply = NULL;
    }
  }

  if (!currCtxt->getSessionDefaults()->callEmbeddedArkcmp() ||
      (currCtxt->getSessionDefaults()->callEmbeddedArkcmp() &&
       ((cmpRet != 0) || (CmpCommon::context()->getRecursionLevel() > 0) || currCtxt->getArkcmp()->getServer()))) {
    for (short i = 0; i < currCtxt->getNumArkcmps(); i++) {
      cmp = currCtxt->getArkcmp(i);
      cmpStatus = cmp->sendRequest(CmpMessageObj::MessageTypeEnum(xnMsgType), data, dataSize, TRUE, NULL,
                                   SQLCHARSETCODE_UTF8, TRUE /*resend, if needed*/
      );

      if (cmpStatus != ExSqlComp::SUCCESS) {
        diagsArea = cmp->getDiags();
        // If its an error don't proceed further.
        if (cmpStatus == ExSqlComp::ERROR) return cmpStatus;
      }

      cmpStatus = cmp->getReply(dummyReply, dummyLength);
      cmp->getHeap()->deallocateMemory((void *)dummyReply);
      if (cmpStatus != ExSqlComp::SUCCESS) {
        diagsArea = cmp->getDiags();
        // Don't proceed if its an error.
        if (cmpStatus == ExSqlComp::ERROR) return cmpStatus;
      }

      if (cmp->status() != ExSqlComp::FETCHED) diagsArea = cmp->getDiags();
    }  // for
  }

  return ExSqlComp::SUCCESS;
}

int ContextCli::setSecInvalidKeys(
    /* IN */ int numSiKeys,
    /* IN */ SQL_QIKEY siKeys[]) {
  CliGlobals *cliGlobals = getCliGlobals();

  if (cliGlobals->getStatsGlobals() == NULL) {
    (diagsArea_) << DgSqlCode(-EXE_RTS_NOT_STARTED);
    return diagsArea_.mainSQLCODE();
  }

  for (int i = 0; i < numSiKeys; i++) {
    // Initialize the filler so that functions like memcmp can be
    // used.
    memset(siKeys[i].filler, 0, sizeof(siKeys[i].filler));
    // Bad operation values cause problems down-stream, so catch
    // them here by letting ComQIActionTypeLiteralToEnum assert.
    ComQIActionType siKeyType = ComQIActionTypeLiteralToEnum(siKeys[i].operation);

    if (CmpCommon::getDefault(GENERATE_USER_QUERYCACHE) == DF_ON ||
        CmpCommon::getDefault(GENERATE_USER_QUERYCACHE) == DF_PRELOAD) {
      // delete querycache for _MD_.TEXT table
      if (siKeyType == COM_QI_OBJECT_REDEF || siKeyType == COM_QI_STATS_UPDATED) {
        long objUID = siKeys[i].ddlObjectUID;
        char buf[512];

        if (siKeyType == COM_QI_OBJECT_REDEF)
          str_sprintf(buf,
                      "delete/*+TRAF_NO_DTM_XN('ON')*/ from  %s.\"%s\".%s where \
SUB_ID in (select SUB_ID  from %s.\"%s\".%s where TEXT_UID=%ld and text_type=%d and \
(flags = 0 or flags = 3)) and TEXT_TYPE = %d",
                      CmpSeabaseDDL::getSystemCatalogStatic().data(), SEABASE_MD_SCHEMA, SEABASE_TEXT,
                      CmpSeabaseDDL::getSystemCatalogStatic().data(), SEABASE_MD_SCHEMA, SEABASE_TEXT, objUID,
                      COM_USER_QUERYCACHE_TEXT, COM_USER_QUERYCACHE_TEXT);
        else
          str_sprintf(buf,
                      "update/*+TRAF_NO_DTM_XN('ON')*/using upsert  %s.\"%s\".%s set FLAGS = 1 where \
SUB_ID in (select SUB_ID  from %s.\"%s\".%s where TEXT_UID=%ld and text_type=%d and flags = 0) and \
TEXT_TYPE = %d and FLAGS = 0",
                      CmpSeabaseDDL::getSystemCatalogStatic().data(), SEABASE_MD_SCHEMA, SEABASE_TEXT,
                      CmpSeabaseDDL::getSystemCatalogStatic().data(), SEABASE_MD_SCHEMA, SEABASE_TEXT, objUID,
                      COM_USER_QUERYCACHE_TEXT, COM_USER_QUERYCACHE_TEXT);

        CmpSeabaseDDL cmpSBD(STMTHEAP);
        if (cmpSBD.switchCompiler(CmpContextInfo::CMPCONTEXT_TYPE_META)) {
          diagsArea_ << DgSqlCode(-4400);
          return diagsArea_.mainSQLCODE();
        }
        ExeCliInterface cliInterface(STMTHEAP);
        int cliRC = cliInterface.executeImmediate(buf);
        if (cliRC < 0) {
          diagsArea_ << DgSqlCode(-1160) << DgString0("failed to disable querycache in TEXT table.");
          cmpSBD.switchBackCompiler();
          return diagsArea_.mainSQLCODE();
        }
        QRLogger::log(CAT_SQL_EXE, LL_INFO, "invalidate querycache in HDFS, objUID:%ld ", objUID);
        cmpSBD.switchBackCompiler();
      }
    }
  }

  ComDiagsArea *tempDiagsArea = &diagsArea_;
  tempDiagsArea->clear();

  IpcServer *ssmpServer =
      ssmpManager_->getSsmpServer(exHeap(), cliGlobals->myNodeName(), cliGlobals->myCpu(), tempDiagsArea);
  if (ssmpServer == NULL) return diagsArea_.mainSQLCODE();

  SsmpClientMsgStream *ssmpMsgStream = new (cliGlobals->getIpcHeap())
      SsmpClientMsgStream((NAHeap *)cliGlobals->getIpcHeap(), ssmpManager_, tempDiagsArea);

  ssmpMsgStream->addRecipient(ssmpServer->getControlConnection());

  SecInvalidKeyRequest *sikMsg =
      new (cliGlobals->getIpcHeap()) SecInvalidKeyRequest(cliGlobals->getIpcHeap(), numSiKeys, siKeys);

  *ssmpMsgStream << *sikMsg;

  // Call send with in waited mode with no timeout.
  // ssmpMsgStream->send();

  // Send message in nowaited mode. Check for reply in a loop.
  // If no reply for 60 secs, return an error

  // 60 secs = 6000 centisecs
  int RtsTimeout = 6000;

  ssmpMsgStream->send(FALSE, -1);
  long startTime = NA_JulianTimestamp();
  long currTime;
  long elapsedTime;
  IpcTimeout timeout = (IpcTimeout)RtsTimeout;
  while (timeout > 0 && ssmpMsgStream->hasIOPending()) {
    ssmpMsgStream->waitOnMsgStream(timeout);
    currTime = NA_JulianTimestamp();
    elapsedTime = (long)(currTime - startTime) / 10000;
    timeout = (IpcTimeout)(RtsTimeout - elapsedTime);
  }

  // Break received - set diags area
  if (ssmpMsgStream->getState() == IpcMessageStream::BREAK_RECEIVED) {
    (diagsArea_) << DgSqlCode(-EXE_CANCELED);
  }

  // if IO pending, return error.
  else if (!ssmpMsgStream->isReplyReceived()) {
    (diagsArea_) << DgSqlCode(-8146) << DgString0("QI request to SSMP timed out after one minute.");
  }

  // I/O is now complete.
  sikMsg->decrRefCount();

  cliGlobals->getEnvironment()->deleteCompletedMessages();
  ssmpManager_->cleanupDeletedSsmpServers();
  return diagsArea_.mainSQLCODE();
}

int ContextCli::setObjectEpochEntry(
    /* IN */ int operation,
    /* IN */ int objectNameLength,
    /* IN */ const char *objectName,
    /* IN */ long redefTime,
    /* IN */ long key,
    /* IN */ UInt32 expectedEpoch,
    /* IN */ UInt32 expectedFlags,
    /* IN */ UInt32 newEpoch,
    /* IN */ UInt32 newFlags,
    /* OUT */ UInt32 *maxExpectedEpochFound,
    /* OUT */ UInt32 *maxExpectedFlagsFound) {
  CliGlobals *cliGlobals = getCliGlobals();
  if (cliGlobals->getStatsGlobals() == NULL) {
    (diagsArea_) << DgSqlCode(-EXE_RTS_NOT_STARTED);
    return diagsArea_.mainSQLCODE();
  }

  ComDiagsArea *tempDiagsArea = &diagsArea_;

  tempDiagsArea->clear();

  IpcServer *ssmpServer =
      ssmpManager_->getSsmpServer(exHeap(), cliGlobals->myNodeName(), cliGlobals->myCpu(), tempDiagsArea);
  if (ssmpServer == NULL) return diagsArea_.mainSQLCODE();

  SsmpClientMsgStream *ssmpMsgStream = new (cliGlobals->getIpcHeap())
      SsmpClientMsgStream((NAHeap *)cliGlobals->getIpcHeap(), ssmpManager_, tempDiagsArea);

  ssmpMsgStream->addRecipient(ssmpServer->getControlConnection());

  ObjectEpochChangeRequest::Operation op =
      (ObjectEpochChangeRequest::Operation)operation;  // TODO: could validate here and return error if bad

  ObjectEpochChangeRequest *oecMsg = new (cliGlobals->getIpcHeap())
      ObjectEpochChangeRequest(cliGlobals->getIpcHeap(), op, objectNameLength, objectName, redefTime, key,
                               expectedEpoch, expectedFlags, newEpoch, newFlags);

  *ssmpMsgStream << *oecMsg;

  // Send message in nowaited mode. Check for reply in a loop.
  // If no reply for 60 secs, return an error

  // 60 secs = 6000 centisecs
  int RtsTimeout = 6000;

  ssmpMsgStream->send(FALSE, -1);
  long startTime = NA_JulianTimestamp();
  long currTime;
  long elapsedTime;
  IpcTimeout timeout = (IpcTimeout)RtsTimeout;
  while (timeout > 0 && ssmpMsgStream->hasIOPending()) {
    ssmpMsgStream->waitOnMsgStream(timeout);
    currTime = NA_JulianTimestamp();
    elapsedTime = (long)(currTime - startTime) / 10000;
    timeout = (IpcTimeout)(RtsTimeout - elapsedTime);
  }

  // Break received - set diags area
  if (ssmpMsgStream->getState() == IpcMessageStream::BREAK_RECEIVED) {
    (diagsArea_) << DgSqlCode(-EXE_CANCELED);
  }

  // if IO pending, return error.
  else if (!ssmpMsgStream->isReplyReceived()) {
    (diagsArea_) << DgSqlCode(-8146) << DgString0("Object epoch change request to SSMP timed out after one minute.");
  }

  else {
    *maxExpectedEpochFound = ssmpMsgStream->getOecrMaxExpectedEpochFound();  // from reply message
    *maxExpectedFlagsFound = ssmpMsgStream->getOecrMaxExpectedFlagsFound();  // from reply message
    if (ssmpMsgStream->getOecrResult() == ObjectEpochChangeReply::UNEXPECTED_VALUES_FOUND) {
      (diagsArea_) << DgSqlCode(-EXE_OEC_UNEXPECTED_STATE) << DgString0(objectName) << DgInt0(*maxExpectedEpochFound)
                   << DgInt1(*maxExpectedFlagsFound) << DgInt2(expectedEpoch) << DgInt3(expectedFlags);
    } else if (ssmpMsgStream->getOecrResult() == ObjectEpochChangeReply::INTERNAL_ERROR) {
      (diagsArea_) << DgSqlCode(-EXE_OEC_INTERNAL_ERROR) << DgString0(objectName)
                   << DgInt0(*maxExpectedEpochFound);  // the internal error code was stashed here
    } else if (ssmpMsgStream->getOecrResult() == ObjectEpochChangeReply::CACHE_FULL) {
      // the ObjectEpochCache is full; behavior depends on CQD TRAF_OBJECT_EPOCH_CACHE_FULL_BEHAVIOR:
      // 0 - raise an error
      // 1 - raise just a warning
      // 2 - no diagnostic at all, but log it
      switch (CmpCommon::getDefaultLong(TRAF_OBJECT_EPOCH_CACHE_FULL_BEHAVIOR)) {
        case 0: {
          // error 8151
          (diagsArea_) << DgSqlCode(-EXE_OEC_CACHE_FULL) << DgString0(objectName);
          break;
        }
        case 1: {
          // warning 8151
          (diagsArea_) << DgSqlCode(EXE_OEC_CACHE_FULL) << DgString0(objectName);
          break;
        }
        default: {
          // log it and ignore it
          QRLogger::log(CAT_SQL_EXE, LL_INFO, "%s %s", "Object Epoch Cache full when creating entry for ", objectName);
          break;
        }
      }
    }
  }

  // I/O is now complete.
  oecMsg->decrRefCount();

  cliGlobals->getEnvironment()->deleteCompletedMessages();
  ssmpManager_->cleanupDeletedSsmpServers();
  return diagsArea_.mainSQLCODE();
}

int ContextCli::checkLobLock(char *inLobLockId, NABoolean *found) {
  int retcode = 0;
  *found = FALSE;
  CliGlobals *cliGlobals = getCliGlobals();
  StatsGlobals *statsGlobals = GetCliGlobals()->getStatsGlobals();
  if (cliGlobals->getStatsGlobals() == NULL) {
    (diagsArea_) << DgSqlCode(-EXE_RTS_NOT_STARTED);
    return diagsArea_.mainSQLCODE();
  }
  statsGlobals->checkLobLock(cliGlobals, inLobLockId);
  if (inLobLockId != NULL) *found = TRUE;
  return retcode;
}

int ContextCli::performObjectLockRequest(const char *objectName, ComObjectType objectType,
                                           ObjectLockRequest::OpType opType, int nid, int pid, int maxRetries,
                                           int delay) {
  int retries = 0;
  int retcode = 0;
  bool conflictLock = false;
  int conflictNid = -1;
  int conflictPid = -1;
  bool retryAfterClean = false;
  ComDiagsArea *tempDiagsArea = ComDiagsArea::allocate(STMTHEAP);
  while (retries < maxRetries || retryAfterClean) {
    conflictLock = false;
    conflictNid = -1;
    conflictPid = -1;
    tempDiagsArea->clear();
    retcode = performObjectLockRequest(objectName, objectType, opType, nid, pid, conflictLock, conflictNid, conflictPid,
                                       tempDiagsArea);
    if (retcode >= 0) {
      // Should have no errors, merge in possible warnings
      diagsArea_.mergeAfter(*tempDiagsArea);
      tempDiagsArea->clear();
      tempDiagsArea->deAllocate();
      return retcode;
    }

    retries++;
    if (retries < maxRetries) {
      LOGTRACE(CAT_SQL_LOCK, "DDL %s request failed, will attempt retry %d after %d ms",
               ObjectLockRequest::opTypeLit(opType), retries, delay);
      usleep(delay * 1000);
    } else if (conflictLock) {
      LOGDEBUG(CAT_SQL_LOCK, "Try to check if the lock holder node %d process %d is still alive", conflictNid,
               conflictPid);
      LockHolder conflictHolder(conflictNid, conflictPid);
      retryAfterClean = cleanupStaleLockHolder(conflictHolder);
      if (retryAfterClean) {
        LOGWARN(CAT_SQL_LOCK, "Try again after cleaned stale lock holder node %d process %d", conflictNid, conflictPid);
      }
    }
  }

  // Request failed, merge in the diags and free the temp diags
  diagsArea_.mergeAfter(*tempDiagsArea);
  tempDiagsArea->clear();
  tempDiagsArea->deAllocate();
  return retcode;
}

int ContextCli::performObjectLockRequest(const char *objectName, ComObjectType objectType,
                                           ObjectLockRequest::OpType opType, int nid, int pid, bool &conflictLock,
                                           int &conflictNid, int &conflictPid, ComDiagsArea *tempDiagsArea) {
  int retcode = 0;

  conflictLock = false;
  LOGTRACE(CAT_SQL_LOCK, "ContextCli::performObjectLockRequest entry");
  if (CmpCommon::getDefault(TRAF_OBJECT_LOCK) != DF_ON) {
    LOGTRACE(CAT_SQL_LOCK, "ContextCli::performObjectLockRequest exit");
    return 0;
  }

  CliGlobals *cliGlobals = getCliGlobals();
  if (!cliGlobals->isMasterProcess()) {
    LOGTRACE(CAT_SQL_LOCK, "ContextCli::performObjectLockRequest exit");
    return 0;
  }

  int maxRetries = CmpCommon::getDefaultLong(TRAF_OBJECT_LOCK_DDL_RETRY_LIMIT);
  int delay = CmpCommon::getDefaultLong(TRAF_OBJECT_LOCK_DDL_RETRY_DELAY);

  LOGINFO(CAT_SQL_LOCK, "Perform DDL %s request of %s %s for node %d process %d", ObjectLockRequest::opTypeLit(opType),
          comObjectTypeName(objectType), objectName, nid, pid);

  if (cliGlobals->getStatsGlobals() == NULL) {
    (*tempDiagsArea) << DgSqlCode(-EXE_RTS_NOT_STARTED);
    LOGTRACE(CAT_SQL_LOCK, "ContextCli::performObjectLockRequest exit");
    return tempDiagsArea->mainSQLCODE();
  }

  IpcServer *ssmpServer =
      ssmpManager_->getSsmpServer(exHeap(), cliGlobals->myNodeName(), cliGlobals->myCpu(), tempDiagsArea);
  if (ssmpServer == NULL) {
    LOGTRACE(CAT_SQL_LOCK, "ContextCli::performObjectLockRequest exit");
    return tempDiagsArea->mainSQLCODE();
  }

  SsmpClientMsgStream *ssmpMsgStream = new (cliGlobals->getIpcHeap())
      SsmpClientMsgStream((NAHeap *)cliGlobals->getIpcHeap(), ssmpManager_, tempDiagsArea);

  ssmpMsgStream->addRecipient(ssmpServer->getControlConnection());

  NAHeap *ipcHeap = cliGlobals->getIpcHeap();
  ObjectLockRequest *request = new (ipcHeap)
      ObjectLockRequest(ipcHeap, objectName, strlen(objectName), objectType, opType, nid, pid, maxRetries, delay);

  *ssmpMsgStream << *request;

  // Send message in nowaited mode. Check for reply in a loop.
  // If no reply for 60 secs, return an error

  // 60 secs = 6000 centisecs
  int RtsTimeout = 6000;

  LOGDEBUG(CAT_SQL_LOCK, "Send DDL %s request to SSMP", ObjectLockRequest::opTypeLit(opType));
  ssmpMsgStream->send(FALSE, -1);
  long startTime = NA_JulianTimestamp();
  long currTime;
  long elapsedTime;
  IpcTimeout timeout = (IpcTimeout)RtsTimeout;
  while (timeout > 0 && ssmpMsgStream->hasIOPending()) {
    ssmpMsgStream->waitOnMsgStream(timeout);
    currTime = NA_JulianTimestamp();
    elapsedTime = (long)(currTime - startTime) / 10000;
    timeout = (IpcTimeout)(RtsTimeout - elapsedTime);
  }

  // Break received - set diags area
  if (ssmpMsgStream->getState() == IpcMessageStream::BREAK_RECEIVED) {
    LOGDEBUG(CAT_SQL_LOCK, "DDL %s request to SSMP cancled", ObjectLockRequest::opTypeLit(opType));
    (*tempDiagsArea) << DgSqlCode(-EXE_CANCELED);
  } else if (!ssmpMsgStream->isReplyReceived()) {
    // if IO pending, return error.
    LOGDEBUG(CAT_SQL_LOCK, "DDL %s Request to SSMP timed out after one minute.", ObjectLockRequest::opTypeLit(opType));
    (*tempDiagsArea) << DgSqlCode(-EXE_STMT_RUNTIME_ERROR)
                     << DgString0("DDL LOCK/UNLOCK request to SSMP timed out after one minute.");
  } else {
    ObjectLockReply::LockState state = ssmpMsgStream->getObjectLockState();
    conflictNid = ssmpMsgStream->getObjectLockConflictNid();
    conflictPid = ssmpMsgStream->getObjectLockConflictPid();
    LOGDEBUG(CAT_SQL_LOCK, "DDL %s request to SSMP returned %s", ObjectLockRequest::opTypeLit(opType),
             ObjectLockReply::lockStateLit(state));
    if (state == ObjectLockReply::LOCK_OK) {
      // Do a quick local verification
      if (opType == ObjectLockRequest::LOCK) {
        ObjectLockPointer lock(objectName, objectType);
        ex_assert(lock->ddlLockedByMe(), "DDL LOCK does not work properly");
      } else if (opType == ObjectLockRequest::UNLOCK) {
        ObjectLockPointer lock(objectName, objectType);
        ex_assert(!lock->ddlLockedByMe(), "DDL UNLOCK does not work propertly");
      } else if (opType == ObjectLockRequest::UNLOCKALL) {
        // TODO: quick verification for UNLOCKALL
      }
    } else if (state == ObjectLockReply::LOCK_UNKNOWN) {
      LOGWARN(CAT_SQL_LOCK, "DDL %s request for '%s' failed due to internal error",
              ObjectLockRequest::opTypeLit(opType), objectName);
      (*tempDiagsArea) << DgSqlCode(-EXE_STMT_RUNTIME_ERROR)
                       << DgString0("DDL LOCK/UNLOCK request to SSMP failed due to internal error");
    } else if (state == ObjectLockReply::LOCK_CONFLICT_DML) {
      conflictLock = true;
      LOGWARN(CAT_SQL_LOCK, "DDL %s request for '%s' failed due to concurrent DML operation on node %d process %d",
              ObjectLockRequest::opTypeLit(opType), objectName, conflictNid, conflictPid);
      (*tempDiagsArea) << DgSqlCode(-EXE_OL_UNABLE_TO_GET_DDL_LOCK) << DgString0(objectName) << DgString1("DML")
                       << DgInt0(conflictNid) << DgInt1(conflictPid);
    } else if (state == ObjectLockReply::LOCK_CONFLICT_DDL) {
      conflictLock = true;
      LOGWARN(CAT_SQL_LOCK, "DDL %s request for '%s' failed due to concurrent DDL operation on node %d process %d",
              ObjectLockRequest::opTypeLit(opType), objectName, conflictNid, conflictPid);
      (*tempDiagsArea) << DgSqlCode(-EXE_OL_UNABLE_TO_GET_DDL_LOCK) << DgString0(objectName) << DgString1("DDL")
                       << DgInt0(conflictNid) << DgInt1(conflictPid);
    } else if (state == ObjectLockReply::LOCK_OUT_OF_ENTRY) {
      LOGERROR(CAT_SQL_LOCK, "DDL lock for %s %s failed due to out of lock entry", comObjectTypeName(objectType),
               objectName);
      (*tempDiagsArea) << DgSqlCode(-EXE_OL_OUT_OF_LOCK_ENTRY) << DgString0(objectName) << DgString1("DDL");
    } else {
      LOGERROR(CAT_SQL_LOCK, "DDL %s request for '%s' failed with invalid lock state: %d",
               ObjectLockRequest::opTypeLit(opType), objectName, state);
      ex_assert(0, "Invalid lock state");
    }
  }

  request->decrRefCount();
  cliGlobals->getEnvironment()->deleteCompletedMessages();
  ssmpManager_->cleanupDeletedSsmpServers();

  LOGTRACE(CAT_SQL_LOCK, "ContextCli::performObjectLockRequest exit");
  return tempDiagsArea->mainSQLCODE();
}

int ContextCli::lockObjectDDL(const char *objectName, ComObjectType objectType) {
  if (CmpCommon::getDefault(TRAF_OBJECT_LOCK) != DF_ON) {
    return 0;
  }

  CliGlobals *cliGlobals = getCliGlobals();
  int nid = cliGlobals->myAncestorNid();
  int pid = cliGlobals->myAncestorPid();

  int maxRetries = CmpCommon::getDefaultLong(TRAF_OBJECT_LOCK_DDL_RETRY_LIMIT);
  int delay = CmpCommon::getDefaultLong(TRAF_OBJECT_LOCK_DDL_RETRY_DELAY);
  int dLockTimeout = CmpCommon::getDefaultLong(TRAF_OBJECT_LOCK_DDL_DIST_LOCK_TIMEOUT);

  ObjectKey key(objectName, objectType);
  LOGDEBUG(CAT_SQL_LOCK, "Acquire distributed lock for key %s, timeout %d", key.keyName(), dLockTimeout);
  WaitedLockController dlock(key.keyName(), dLockTimeout);
  if (!dlock.lockHeld()) {
    LOGERROR(CAT_SQL_LOCK, "Unable to get object distributed lock for %s", objectName);
    diagsArea_ << DgSqlCode(-EXE_OL_UNABLE_TO_GET_DIST_LOCK) << DgString0(key.keyName());
    return -EXE_OL_UNABLE_TO_GET_DIST_LOCK;
  }

  ObjectLockPointer lock(objectName, objectType);
  if (lock->ddlLockedByMe()) {
    LOGDEBUG(CAT_SQL_LOCK, "Already holding the DDL lock for %s %s", comObjectTypeName(objectType), objectName);
    return 0;
  }

  int retcode = performObjectLockRequest(objectName, objectType, ObjectLockRequest::LOCK, nid, pid, maxRetries, delay);
  if (retcode < 0) {
    // LOCK failed cluster wide, but some nodes may have been locked,
    // need to perforam an UNLOCK to cleanup partial DDL locks.
    LOGDEBUG(CAT_SQL_LOCK, "DDL lock for %s %s failed, try clean up partial DDL locks", comObjectTypeName(objectType),
             objectName);
    if (unlockObjectDDL(objectName, objectType, nid, pid) < 0) {
      LOGERROR(CAT_SQL_LOCK, "Cleanup partial DDL locks failed for %s %s", comObjectTypeName(objectType), objectName);
      diagsArea_ << DgSqlCode(-EXE_OL_CLEANUP_PARTIAL_LOCK) << DgString0(objectName);
    }
  } else {
    StatsGlobals *statsGlobals = cliGlobals->getStatsGlobals();
    ex_assert(statsGlobals, "StatsGlobals not properly initialized");
    RTSSemaphoreController sem;
    ProcessStats *ps = statsGlobals->checkProcess(pid);
    if (ps != NULL) {
      ps->setHoldingDDLLocks(true);
    } else {
      LOGWARN(CAT_SQL_LOCK, "ProcessStats for %d is missing", pid);
      diagsArea_ << DgSqlCode(EXE_STMT_RUNTIME_ERROR)  // a warning
                 << DgString0("ProcessStats is missing");
    }
  }
  return retcode;
}

int ContextCli::unlockObjectDDL(const char *objectName, ComObjectType objectType, int nid, int pid, bool all) {
  if (CmpCommon::getDefault(TRAF_OBJECT_LOCK) != DF_ON) {
    return 0;
  }

  int retcode = 0;
  int maxRetries = 3;
  int delay = 1000;  // in milliseconds
  ObjectLockRequest::OpType opType = all ? ObjectLockRequest::UNLOCKALL : ObjectLockRequest::UNLOCK;

  retcode = performObjectLockRequest(objectName, objectType, opType, nid, pid, maxRetries, delay);
  if (retcode < 0) {
    if (all) {
      LOGERROR(CAT_SQL_LOCK, "Unlock all DDL object lock failed");
      diagsArea_ << DgSqlCode(-EXE_OL_UNABLE_TO_RELEASE_DDL_LOCK) << DgString0("all objects");
    } else {
      LOGERROR(CAT_SQL_LOCK, "Unlock DDL object lock failed for %s %s", comObjectTypeName(objectType), objectName);
      diagsArea_ << DgSqlCode(-EXE_OL_UNABLE_TO_RELEASE_DDL_LOCK) << DgString0(objectName);
    }
  }
  return retcode;
}

int ContextCli::unlockObjectDDL(const char *objectName, ComObjectType objectType) {
  if (CmpCommon::getDefault(TRAF_OBJECT_LOCK) != DF_ON) {
    return 0;
  }

  CliGlobals *cliGlobals = getCliGlobals();
  int nid = cliGlobals->myAncestorNid();
  int pid = cliGlobals->myAncestorPid();
  return unlockObjectDDL(objectName, objectType, nid, pid);
}

bool ContextCli::cleanupStaleLockHolder(const LockHolder &holder) {
  char processName[MS_MON_MAX_PROCESS_NAME + 1];
  int nid = holder.getNid();
  pid_t pid = holder.getPid();
  bool force = CmpCommon::getDefault(TRAF_OBJECT_LOCK_FORCE) == DF_ON;

  LOGDEBUG(CAT_SQL_LOCK, "Check status of the lock holder node %d process %d", nid, pid);
  int rc = msg_mon_get_process_name(nid, pid, processName);
  if (rc != XZFIL_ERR_NOSUCHDEV) {
    LOGDEBUG(CAT_SQL_LOCK, "The lock holder is a running process %d on node %d", pid, nid);
    if (!force) {
      return false;
    }
  }

  LOGWARN(CAT_SQL_LOCK, "%s stale locks held by node %d process %d", force ? "Force clean" : "Clean", nid, pid);
  // Holder process not exist, cleanup all lock held by the process
  int maxRetries = 3;
  int delay = 1000;  // in milliseconds
  rc = performObjectLockRequest("ALL", COM_BASE_TABLE_OBJECT, ObjectLockRequest::CLEANUP, nid, pid, maxRetries, delay);
  if (rc < 0) {
    LOGERROR(CAT_SQL_LOCK, "Failed to cleanup all locks held by %d %d", nid, pid);
    return false;
  }
  return true;
}

int ContextCli::lockObjectDML(const char *objectName, ComObjectType objectType) {
  if (CmpCommon::getDefault(TRAF_OBJECT_LOCK) != DF_ON) {
    return 0;
  }

  LOGTRACE(CAT_SQL_LOCK, "ContextCli::lockObjectDML entry");
  LOGTRACE(CAT_SQL_LOCK, "  Object name: %s, type: %d", objectName, objectType);

  LOGDEBUG(CAT_SQL_LOCK, "DML lock for %s", objectName);
  ObjectLockPointer lock(objectName, objectType);
  int retries = 0;
  int maxRetries = CmpCommon::getDefaultLong(TRAF_OBJECT_LOCK_DML_RETRY_LIMIT);
  int delay = CmpCommon::getDefaultLong(TRAF_OBJECT_LOCK_DML_RETRY_DELAY);
  LockHolder conflictHolder;
  ObjectLockReply::LockState state = ObjectLockReply::LOCK_UNKNOWN;
  while (retries < maxRetries) {
    state = lock->lockDML(conflictHolder);
    if (state == ObjectLockReply::LOCK_OK) {
      break;
    }
    retries++;
    if (retries >= maxRetries) {
      break;
    }
    if (state == ObjectLockReply::LOCK_CONFLICT_DDL) {
      LOGTRACE(CAT_SQL_LOCK, "Conflict with DDL operations on node %d process %d, will attemp retry %d after %d ms",
               conflictHolder.getNid(), conflictHolder.getPid(), retries, delay);
    } else if (state == ObjectLockReply::LOCK_OUT_OF_ENTRY) {
      LOGTRACE(CAT_SQL_LOCK, "Out of lock entry, will attemp retry %d after %d ms", retries, delay);
    } else {
      LOGERROR(CAT_SQL_LOCK, "DML lock failed for %s %s with invalid state %d", comObjectTypeName(objectType),
               objectName, state);
      ex_assert(0, "Invalid lock state");
    }
    usleep(delay * 1000);
  }
  if (retries < maxRetries) {
    LOGDEBUG(CAT_SQL_LOCK, "DML lock for %s succeeded after %d retries", objectName, retries);
    LOGTRACE(CAT_SQL_LOCK, "ContextCli::lockObjectDML exit");
    return 0;
  }

  if (state == ObjectLockReply::LOCK_CONFLICT_DDL) {
    bool cleaned = cleanupStaleLockHolder(conflictHolder);
    if (cleaned) {
      state = lock->lockDML(conflictHolder);
      if (state == ObjectLockReply::LOCK_OK) {
        LOGDEBUG(CAT_SQL_LOCK, "DML lock for %s succeeded after clean stale lock", objectName);
        LOGTRACE(CAT_SQL_LOCK, "ContextCli::lockObjectDML exit");
        return 0;
      }
    }
  }

  int retcode = -EXE_OL_UNABLE_TO_GET_DML_LOCK;
  if (state == ObjectLockReply::LOCK_CONFLICT_DDL) {
    LOGERROR(CAT_SQL_LOCK, "DML lock failed for %s %s, conflict with DDL operations on node %d process %d",
             comObjectTypeName(objectType), objectName, conflictHolder.getNid(), conflictHolder.getPid());
    diagsArea_ << DgSqlCode(-EXE_OL_UNABLE_TO_GET_DML_LOCK) << DgString0(objectName) << DgInt0(conflictHolder.getNid())
               << DgInt1(conflictHolder.getPid());
  } else if (state == ObjectLockReply::LOCK_OUT_OF_ENTRY) {
    LOGERROR(CAT_SQL_LOCK, "DML lock failed for %s %s, out of lock entry", comObjectTypeName(objectType), objectName);
    diagsArea_ << DgSqlCode(-EXE_OL_OUT_OF_LOCK_ENTRY) << DgString0(objectName) << DgString1("DML");
    retcode = -EXE_OL_OUT_OF_LOCK_ENTRY;
  } else {
    LOGERROR(CAT_SQL_LOCK, "DML lock failed for %s %s with invalid state %d", comObjectTypeName(objectType), objectName,
             state);
    ex_assert(0, "Invalid lock state");
  }
  LOGTRACE(CAT_SQL_LOCK, "ContextCli::lockObjectDML exit");
  return retcode;
}

int ContextCli::releaseAllDDLObjectLocks() {
  if (CmpCommon::getDefault(TRAF_OBJECT_LOCK) != DF_ON) {
    return 0;
  }

  CliGlobals *cliGlobals = getCliGlobals();
  ex_assert(cliGlobals, "CliGlobals not properly initialized");
  if (!cliGlobals->isMasterProcess()) {
    return 0;
  }
  StatsGlobals *statsGlobals = cliGlobals->getStatsGlobals();
  ex_assert(statsGlobals, "StatsGlobals not properly initialized");
  int nid = cliGlobals->myNodeNumber();
  int pid = cliGlobals->myPin();
  ProcessStats *ps = statsGlobals->checkProcess(pid);
  ex_assert(ps, "ProcessStats not properly initialized");
  if (!ps->holdingDDLLocks()) {
    return 0;
  }
  int retcode = unlockObjectDDL("ALL",  // A dummy name, should not be used
                                  COM_BASE_TABLE_OBJECT, nid, pid, true /* all */);
  ps->setHoldingDDLLocks(false);
  return retcode;
}

int ContextCli::releaseAllDMLObjectLocks() {
  if (CmpCommon::getDefault(TRAF_OBJECT_LOCK) != DF_ON) {
    return 0;
  }
  CliGlobals *cliGlobals = getCliGlobals();
  ex_assert(cliGlobals, "CliGlobals not properly initialized");
  if (!cliGlobals->isMasterProcess()) {
    return 0;
  }
  StatsGlobals *statsGlobals = cliGlobals->getStatsGlobals();
  ex_assert(statsGlobals, "StatsGlobals not properly initialized");
  pid_t pid = cliGlobals->myPin();
  ObjectLockArray *dmlLockArray = statsGlobals->dmlLockArray(pid);
  unsigned short entries = dmlLockArray->entries();
  if (entries > 0) {
    dmlLockArray->removeAll();
    LOGDEBUG(CAT_SQL_LOCK, "Released %d DML lock entries", entries);
  }
  return 0;
}

ExStatisticsArea *ContextCli::getObjectEpochStats(const char *objectName, int objectNameLen, short cpu, bool locked,
                                                  short &retryAttempts) {
  ExStatisticsArea *stats = NULL;
  ComDiagsArea *tempDiagsArea = &diagsArea_;
  CliGlobals *cliGlobals = getCliGlobals();
  ExSsmpManager *ssmpManager = cliGlobals->getSsmpManager();
  IpcServer *ssmpServer =
      ssmpManager->getSsmpServer(exHeap(), NULL /* nodeName */, (cpu < 0 ? cliGlobals->myCpu() : cpu), tempDiagsArea);
  if (ssmpServer == NULL) return NULL;  // diags are in diagsArea_

  // Create the SsmpClientMsgStream on the IpcHeap, since we don't dispose of it immediately.
  // We just add it to the list of completed messages in the IpcEnv, and it is disposed of later.
  // If we create it on the ExStatsTcb's heap, that heap gets deallocated when the statement is
  // finished, and we can corrupt some other statement's heap later on when we deallocate this stream.
  SsmpClientMsgStream *ssmpMsgStream =
      new (cliGlobals->getIpcHeap()) SsmpClientMsgStream((NAHeap *)cliGlobals->getIpcHeap(), ssmpManager);

  ssmpMsgStream->addRecipient(ssmpServer->getControlConnection());

  NAHeap *ipcHeap = cliGlobals->getIpcHeap();

  ObjectEpochStatsRequest *request =
      new (ipcHeap) ObjectEpochStatsRequest(ipcHeap, objectName, objectNameLen, cpu, locked);
  *ssmpMsgStream << *request;

  // Send message in nowaited mode. Check for reply in a loop.
  // If no reply for 60 secs, return an error

  // 60 secs = 6000 centisecs
  int RtsTimeout = 6000;

  QRDEBUG("Sending ObjectEpochStatsRequest to SSMP");
  ssmpMsgStream->send(FALSE, -1);
  long startTime = NA_JulianTimestamp();
  long currTime;
  long elapsedTime;
  IpcTimeout timeout = (IpcTimeout)RtsTimeout;
  while (timeout > 0 && ssmpMsgStream->hasIOPending()) {
    ssmpMsgStream->waitOnMsgStream(timeout);
    currTime = NA_JulianTimestamp();
    elapsedTime = (long)(currTime - startTime) / 10000;
    timeout = (IpcTimeout)(RtsTimeout - elapsedTime);
  }

  // Callbacks would have placed broken connections into
  // ExSsmpManager::deletedSsmps_.  Delete them now.
  ssmpManager->cleanupDeletedSsmpServers();
  if (ssmpMsgStream->getState() == IpcMessageStream::ERROR_STATE && retryAttempts < 3) {
    QRWARN(
        "Error state returned for ObjectEpochStatsRequest,"
        " retryAttempts: %d",
        retryAttempts);
    request->decrRefCount();
    DELAY(100);
    retryAttempts++;
    return getObjectEpochStats(objectName, objectNameLen, cpu, locked, retryAttempts);
  }

  if (ssmpMsgStream->getState() == IpcMessageStream::BREAK_RECEIVED) {
    // Break received - set diags area
    (diagsArea_) << DgSqlCode(-EXE_CANCELED);
    return NULL;
  }
  if (!ssmpMsgStream->isReplyReceived()) {
    (diagsArea_) << DgSqlCode(-EXE_RTS_TIMED_OUT) << DgString0("GET OBJECT EPOCH CACHE") << DgInt0(RtsTimeout / 100);
    return NULL;
  }
  QRDEBUG("Received reply for ObjectEpochStatsRequest from SSMP");
  request->decrRefCount();
  stats = ssmpMsgStream->getStats();
  if (stats == NULL) {
    QRDEBUG("No stats returned for ObjectEpochStats");
  }
  if (ssmpMsgStream->getNumSscpReqFailed() > 0) {
    (diagsArea_) << DgSqlCode(EXE_RTS_REQ_PARTIALY_SATISFIED) << DgInt0(ssmpMsgStream->getNumSscpReqFailed());
    if (stats != NULL && stats->getMasterStats() != NULL)
      stats->getMasterStats()->setStatsErrorCode(EXE_RTS_REQ_PARTIALY_SATISFIED);
  }
  return stats;
}

ExStatisticsArea *ContextCli::getObjectLockStats(const char *objectName, int objectNameLen, short cpu,
                                                 short &retryAttempts) {
  ExStatisticsArea *stats = NULL;
  ComDiagsArea *tempDiagsArea = &diagsArea_;
  CliGlobals *cliGlobals = getCliGlobals();
  ExSsmpManager *ssmpManager = cliGlobals->getSsmpManager();
  IpcServer *ssmpServer =
      ssmpManager->getSsmpServer(exHeap(), NULL /* nodeName */, (cpu < 0 ? cliGlobals->myCpu() : cpu), tempDiagsArea);
  if (ssmpServer == NULL) return NULL;  // diags are in diagsArea_

  // Create the SsmpClientMsgStream on the IpcHeap, since we don't dispose of it immediately.
  // We just add it to the list of completed messages in the IpcEnv, and it is disposed of later.
  // If we create it on the ExStatsTcb's heap, that heap gets deallocated when the statement is
  // finished, and we can corrupt some other statement's heap later on when we deallocate this stream.
  SsmpClientMsgStream *ssmpMsgStream =
      new (cliGlobals->getIpcHeap()) SsmpClientMsgStream((NAHeap *)cliGlobals->getIpcHeap(), ssmpManager);

  ssmpMsgStream->addRecipient(ssmpServer->getControlConnection());

  NAHeap *ipcHeap = cliGlobals->getIpcHeap();

  ObjectLockStatsRequest *request = new (ipcHeap) ObjectLockStatsRequest(ipcHeap, objectName, objectNameLen,
                                                                         COM_BASE_TABLE_OBJECT,  // TODO
                                                                         cpu);
  *ssmpMsgStream << *request;

  // Send message in nowaited mode. Check for reply in a loop.
  // If no reply for 60 secs, return an error

  // 60 secs = 6000 centisecs
  int RtsTimeout = 6000;

  LOGDEBUG(CAT_SQL_LOCK, "Sending ObjectLockStatsRequest to SSMP");
  ssmpMsgStream->send(FALSE, -1);
  long startTime = NA_JulianTimestamp();
  long currTime;
  long elapsedTime;
  IpcTimeout timeout = (IpcTimeout)RtsTimeout;
  while (timeout > 0 && ssmpMsgStream->hasIOPending()) {
    ssmpMsgStream->waitOnMsgStream(timeout);
    currTime = NA_JulianTimestamp();
    elapsedTime = (long)(currTime - startTime) / 10000;
    timeout = (IpcTimeout)(RtsTimeout - elapsedTime);
  }

  // Callbacks would have placed broken connections into
  // ExSsmpManager::deletedSsmps_.  Delete them now.
  ssmpManager->cleanupDeletedSsmpServers();
  if (ssmpMsgStream->getState() == IpcMessageStream::ERROR_STATE && retryAttempts < 3) {
    LOGWARN(CAT_SQL_LOCK, "Error state returned for ObjectLockStatsRequest, retryAttempts: %d", retryAttempts);
    request->decrRefCount();
    DELAY(100);
    retryAttempts++;
    return getObjectLockStats(objectName, objectNameLen, cpu, retryAttempts);
  }

  if (ssmpMsgStream->getState() == IpcMessageStream::BREAK_RECEIVED) {
    // Break received - set diags area
    (diagsArea_) << DgSqlCode(-EXE_CANCELED);
    return NULL;
  }
  if (!ssmpMsgStream->isReplyReceived()) {
    (diagsArea_) << DgSqlCode(-EXE_RTS_TIMED_OUT) << DgString0("GET OBJECT LOCK STATS") << DgInt0(RtsTimeout / 100);
    return NULL;
  }
  QRDEBUG("Received reply for ObjectLockStatsRequest from SSMP");
  request->decrRefCount();
  stats = ssmpMsgStream->getStats();
  if (stats == NULL) {
    QRDEBUG("No stats returned for ObjectLockStats");
  }
  if (ssmpMsgStream->getNumSscpReqFailed() > 0) {
    (diagsArea_) << DgSqlCode(EXE_RTS_REQ_PARTIALY_SATISFIED) << DgInt0(ssmpMsgStream->getNumSscpReqFailed());
    if (stats != NULL && stats->getMasterStats() != NULL)
      stats->getMasterStats()->setStatsErrorCode(EXE_RTS_REQ_PARTIALY_SATISFIED);
  }
  return stats;
}

ExStatisticsArea *ContextCli::getMergedStats(
    /* IN */ short statsReqType,
    /* IN */ char *statsReqStr,
    /* IN */ int statsReqStrLen,
    /* IN */ short activeQueryNum,
    /* IN */ short statsMergeType, short &retryAttempts) {
  ExStatisticsArea *stats = NULL;
  ExStatisticsArea *statsTmp = NULL;
  short tmpStatsMergeType;

  CliGlobals *cliGlobals = getCliGlobals();
  if (statsMergeType == SQLCLI_DEFAULT_STATS) {
    if (sessionDefaults_ != NULL)
      tmpStatsMergeType = (short)sessionDefaults_->getStatisticsViewType();
    else
      tmpStatsMergeType = SQLCLI_SAME_STATS;
  } else
    tmpStatsMergeType = statsMergeType;
  NABoolean deleteStats = FALSE;
  if (statsReqType == SQLCLI_STATS_REQ_STMT || statsReqType == SQLCLI_STATS_REQ_QID_CURRENT) {
    if (statsReqType == SQLCLI_STATS_REQ_STMT) {
      SQLMODULE_ID module;
      SQLSTMT_ID stmt_id;
      init_SQLMODULE_ID(&module);
      init_SQLCLI_OBJ_ID(&stmt_id, SQLCLI_CURRENT_VERSION, stmt_name, &module, statsReqStr, NULL,
                         SQLCHARSETSTRING_ISO88591, statsReqStrLen);
      Statement *stmt = getStatement(&stmt_id);
      ExMasterStats *masterStats, *tmpMasterStats;

      if (stmt != NULL) {
        statsTmp = stmt->getStatsArea();
        if (statsTmp == NULL && stmt->getStmtStats() != NULL &&
            (masterStats = stmt->getStmtStats()->getMasterStats()) != NULL) {
          tmpStatsMergeType = masterStats->compilerStatsInfo().collectStatsType();
          statsTmp = new (exCollHeap())
              ExStatisticsArea((NAMemory *)exCollHeap(), 0, (ComTdb::CollectStatsType)tmpStatsMergeType,
                               (ComTdb::CollectStatsType)tmpStatsMergeType);
          tmpMasterStats = new (exHeap()) ExMasterStats(exHeap());
          tmpMasterStats->copyContents(masterStats);
          statsTmp->setMasterStats(tmpMasterStats);
          deleteStats = TRUE;
        }
      }
    } else if (statsReqType == SQLCLI_STATS_REQ_QID_CURRENT)
      statsTmp = getStats();
    if (statsTmp != NULL) {
      if (statsTmp->getCollectStatsType() == (ComTdb::CollectStatsType)SQLCLI_ALL_STATS)
        tmpStatsMergeType = SQLCLI_SAME_STATS;
      if (tmpStatsMergeType == SQLCLI_SAME_STATS || (tmpStatsMergeType == statsTmp->getCollectStatsType())) {
        if (deleteStats)
          setDeleteStats(TRUE);
        else
          setDeleteStats(FALSE);
        stats = statsTmp;
      } else {
        stats = new (exCollHeap())
            ExStatisticsArea((NAMemory *)exCollHeap(), 0, (ComTdb::CollectStatsType)tmpStatsMergeType,
                             statsTmp->getOrigCollectStatsType());
        StatsGlobals *statsGlobals = cliGlobals_->getStatsGlobals();
        if (statsGlobals != NULL) {
          int error = statsGlobals->getStatsSemaphore(cliGlobals_->getSemId(), cliGlobals_->myPin());
          stats->merge(statsTmp, tmpStatsMergeType);
          setDeleteStats(TRUE);
          statsGlobals->releaseStatsSemaphore(cliGlobals_->getSemId(), cliGlobals_->myPin());
        } else {
          stats->merge(statsTmp, tmpStatsMergeType);
          setDeleteStats(TRUE);
        }
      }
      return stats;
    } else
      return NULL;
  }
  short cpu = -1;
  pid_t pid = (pid_t)-1;
  long timeStamp = -1;
  int queryNumber = -1;
  char nodeName[MAX_SEGMENT_NAME_LEN + 1];
  if (cliGlobals->getStatsGlobals() == NULL) {
    (diagsArea_) << DgSqlCode(-EXE_RTS_NOT_STARTED);
    return NULL;
  }
  switch (statsReqType) {
    case SQLCLI_STATS_REQ_QID:
      if (statsReqStr == NULL) {
        (diagsArea_) << DgSqlCode(-EXE_RTS_INVALID_QID) << DgString0("NULL");
        return NULL;
      }
      if (getMasterCpu(statsReqStr, statsReqStrLen, nodeName, MAX_SEGMENT_NAME_LEN + 1, cpu) == -1) {
        (diagsArea_) << DgSqlCode(-EXE_RTS_INVALID_QID) << DgString0(statsReqStr);
        return NULL;
      }
      break;
    case SQLCLI_STATS_REQ_CPU:
    case SQLCLI_STATS_REQ_PID:
    case SQLCLI_STATS_REQ_RMS_INFO:
    case SQLCLI_STATS_REQ_PROCESS_INFO:
    case SQLCLI_STATS_REQ_QID_INTERNAL:
      if (parse_statsReq(statsReqType, statsReqStr, statsReqStrLen, nodeName, cpu, pid, timeStamp, queryNumber) == -1) {
        (diagsArea_) << DgSqlCode(-EXE_RTS_INVALID_CPU_PID);
        return NULL;
      }
      break;
    default:
      (diagsArea_) << DgSqlCode(-EXE_RTS_INVALID_QID) << DgString0(statsReqStr);
      return NULL;
  }
  ComDiagsArea *tempDiagsArea = &diagsArea_;
  ExSsmpManager *ssmpManager = cliGlobals->getSsmpManager();
  IpcServer *ssmpServer =
      ssmpManager->getSsmpServer(exHeap(), nodeName, (cpu == -1 ? cliGlobals->myCpu() : cpu), tempDiagsArea);
  if (ssmpServer == NULL) return NULL;  // diags are in diagsArea_

  // Create the SsmpClientMsgStream on the IpcHeap, since we don't dispose of it immediately.
  // We just add it to the list of completed messages in the IpcEnv, and it is disposed of later.
  // If we create it on the ExStatsTcb's heap, that heap gets deallocated when the statement is
  // finished, and we can corrupt some other statement's heap later on when we deallocate this stream.
  SsmpClientMsgStream *ssmpMsgStream =
      new (cliGlobals->getIpcHeap()) SsmpClientMsgStream((NAHeap *)cliGlobals->getIpcHeap(), ssmpManager);

  ssmpMsgStream->addRecipient(ssmpServer->getControlConnection());
  RtsHandle rtsHandle = (RtsHandle)this;
  SessionDefaults *sd = getSessionDefaults();
  int RtsTimeout;
  // Retrieve the Rts collection interval and active queries. If they are valid, calculate the timeout
  // and send to the SSMP process.
  NABoolean wmsProcess;
  if (sd) {
    RtsTimeout = sd->getRtsTimeout();
    wmsProcess = sd->getWmsProcess();
  } else {
    RtsTimeout = 0;
    wmsProcess = FALSE;
  }
  RtsCpuStatsReq *cpuStatsReq = NULL;
  RtsStatsReq *statsReq = NULL;
  RtsQueryId *rtsQueryId = NULL;
  setDeleteStats(TRUE);
  if (statsReqType == SQLCLI_STATS_REQ_RMS_INFO) {
    cpuStatsReq = new (cliGlobals->getIpcHeap())
        RtsCpuStatsReq(rtsHandle, cliGlobals->getIpcHeap(), nodeName, cpu, activeQueryNum, statsReqType);
    *ssmpMsgStream << *cpuStatsReq;
  } else {
    statsReq = new (cliGlobals->getIpcHeap()) RtsStatsReq(rtsHandle, cliGlobals->getIpcHeap(), wmsProcess);
    *ssmpMsgStream << *statsReq;
    switch (statsReqType) {
      case SQLCLI_STATS_REQ_QID:
        rtsQueryId = new (cliGlobals->getIpcHeap()) RtsQueryId(cliGlobals->getIpcHeap(), statsReqStr, statsReqStrLen,
                                                               (UInt16)tmpStatsMergeType, activeQueryNum);
        break;
      case SQLCLI_STATS_REQ_CPU:
        rtsQueryId = new (cliGlobals->getIpcHeap())
            RtsQueryId(cliGlobals->getIpcHeap(), nodeName, cpu, (UInt16)tmpStatsMergeType, activeQueryNum);
        break;
      case SQLCLI_STATS_REQ_PID:
      case SQLCLI_STATS_REQ_PROCESS_INFO:
        rtsQueryId = new (cliGlobals->getIpcHeap()) RtsQueryId(cliGlobals->getIpcHeap(), nodeName, cpu, pid,
                                                               (UInt16)tmpStatsMergeType, activeQueryNum, statsReqType);
        break;
      case SQLCLI_STATS_REQ_QID_INTERNAL:
        rtsQueryId = new (cliGlobals->getIpcHeap()) RtsQueryId(cliGlobals->getIpcHeap(), nodeName, cpu, pid, timeStamp,
                                                               queryNumber, (UInt16)tmpStatsMergeType, activeQueryNum);
        break;
      default:
        rtsQueryId = NULL;
        break;
        break;
    }
    if (NULL != rtsQueryId) *ssmpMsgStream << *rtsQueryId;
  }
  if (RtsTimeout != 0) {
    // We have a valid value for the timeout, so we use it by converting it to centiseconds.
    RtsTimeout = RtsTimeout * 100;
  } else
    // Use the default value of 4 seconds, or 400 centiseconds.
    RtsTimeout = 400;
  // Send the message
  ssmpMsgStream->send(FALSE, -1);
  long startTime = NA_JulianTimestamp();
  long currTime;
  long elapsedTime;
  IpcTimeout timeout = (IpcTimeout)RtsTimeout;
  while (timeout > 0 && ssmpMsgStream->hasIOPending()) {
    ssmpMsgStream->waitOnMsgStream(timeout);
    currTime = NA_JulianTimestamp();
    elapsedTime = (long)(currTime - startTime) / 10000;
    timeout = (IpcTimeout)(RtsTimeout - elapsedTime);
  }
  // Callbacks would have placed broken connections into
  // ExSsmpManager::deletedSsmps_.  Delete them now.
  ssmpManager->cleanupDeletedSsmpServers();
  if (ssmpMsgStream->getState() == IpcMessageStream::ERROR_STATE && retryAttempts < 3) {
    if (statsReqType != SQLCLI_STATS_REQ_RMS_INFO) {
      rtsQueryId->decrRefCount();
      statsReq->decrRefCount();
    } else
      cpuStatsReq->decrRefCount();
    DELAY(100);
    retryAttempts++;
    stats = getMergedStats(statsReqType, statsReqStr, statsReqStrLen, activeQueryNum, statsMergeType, retryAttempts);
    return stats;
  }
  if (ssmpMsgStream->getState() == IpcMessageStream::BREAK_RECEIVED) {
    // Break received - set diags area
    (diagsArea_) << DgSqlCode(-EXE_CANCELED);
    return NULL;
  }
  if (!ssmpMsgStream->isReplyReceived()) {
    char lcStatsReqStr[ComSqlId::MAX_QUERY_ID_LEN + 1];
    int len;
    if (statsReqStrLen > ComSqlId::MAX_QUERY_ID_LEN)
      len = ComSqlId::MAX_QUERY_ID_LEN;
    else
      len = statsReqStrLen;
    str_cpy_all(lcStatsReqStr, statsReqStr, len);
    lcStatsReqStr[len] = '\0';
    (diagsArea_) << DgSqlCode(-EXE_RTS_TIMED_OUT) << DgString0(lcStatsReqStr) << DgInt0(RtsTimeout / 100);
    return NULL;
  }
  if (ssmpMsgStream->getRtsQueryId() != NULL) {
    rtsQueryId->decrRefCount();
    statsReq->decrRefCount();
    retryAttempts = 0;
    stats = getMergedStats(SQLCLI_STATS_REQ_QID, ssmpMsgStream->getRtsQueryId()->getQueryId(),
                           ssmpMsgStream->getRtsQueryId()->getQueryIdLen(), 1, statsMergeType, retryAttempts);
    ssmpMsgStream->getRtsQueryId()->decrRefCount();
    return stats;
  }
  stats = ssmpMsgStream->getStats();
  if (stats != NULL && stats->getMasterStats() != NULL) {
    if (tmpStatsMergeType == SQLCLIDEV_SAME_STATS)
      stats->getMasterStats()->setCollectStatsType(stats->getCollectStatsType());
    else
      stats->getMasterStats()->setCollectStatsType((ComTdb::CollectStatsType)tmpStatsMergeType);
  }
  if (statsReqType != SQLCLI_STATS_REQ_RMS_INFO) {
    rtsQueryId->decrRefCount();
    statsReq->decrRefCount();
  } else
    cpuStatsReq->decrRefCount();
  if (ssmpMsgStream->getNumSscpReqFailed() > 0) {
    (diagsArea_) << DgSqlCode(EXE_RTS_REQ_PARTIALY_SATISFIED) << DgInt0(ssmpMsgStream->getNumSscpReqFailed());
    if (stats != NULL && stats->getMasterStats() != NULL)
      stats->getMasterStats()->setStatsErrorCode(EXE_RTS_REQ_PARTIALY_SATISFIED);
  }
  return stats;
}

int ContextCli::GetStatistics2(
    /* IN */ short statsReqType,
    /* IN */ char *statsReqStr,
    /* IN */ int statsReqStrLen,
    /* IN */ short activeQueryNum,
    /* IN */ short statsMergeType,
    /* OUT */ short *statsCollectType,
    /* IN/OUT */ SQLSTATS_DESC sqlstats_desc[],
    /* IN */ int max_stats_desc,
    /* OUT */ int *no_returned_stats_desc) {
  int retcode;
  short retryAttempts = 0;

  if (mergedStats_ != NULL && deleteStats()) {
    NADELETE(mergedStats_, ExStatisticsArea, mergedStats_->getHeap());
    setDeleteStats(FALSE);
    mergedStats_ = NULL;
  }

  mergedStats_ =
      getMergedStats(statsReqType, statsReqStr, statsReqStrLen, activeQueryNum, statsMergeType, retryAttempts);

  cliGlobals_->getEnvironment()->deleteCompletedMessages();

  if (mergedStats_ == NULL) {
    if (diagsArea_.getNumber(DgSqlCode::ERROR_) > 0)
      return diagsArea_.mainSQLCODE();
    else if (statsReqType == SQLCLI_STATS_REQ_QID) {
      (diagsArea_) << DgSqlCode(-EXE_RTS_QID_NOT_FOUND) << DgString0(statsReqStr);
      return diagsArea_.mainSQLCODE();
    } else {
      if (no_returned_stats_desc != NULL) *no_returned_stats_desc = 0;
      return 0;
    }
  }
  if (statsCollectType != NULL) *statsCollectType = mergedStats_->getOrigCollectStatsType();
  retcode = mergedStats_->getStatsDesc(statsCollectType, sqlstats_desc, max_stats_desc, no_returned_stats_desc);
  if (retcode != 0) (diagsArea_) << DgSqlCode(retcode);
  return retcode;
}

void ContextCli::setStatsArea(ExStatisticsArea *stats, NABoolean isStatsCopy, NABoolean inSharedSegment,
                              NABoolean getSemaphore) {
  int error;
  StatsGlobals *statsGlobals = cliGlobals_->getStatsGlobals();

  // Delete Stats only when it is different from the incomng stats
  if (statsCopy() && stats_ != NULL && stats != stats_) {
    if (statsInSharedSegment() && getSemaphore) {
      if (statsGlobals != NULL) {
        error = statsGlobals->getStatsSemaphore(cliGlobals_->getSemId(), cliGlobals_->myPin());
      }
      NADELETE(stats_, ExStatisticsArea, stats_->getHeap());
      if (statsGlobals != NULL) statsGlobals->releaseStatsSemaphore(cliGlobals_->getSemId(), cliGlobals_->myPin());
    } else
      NADELETE(stats_, ExStatisticsArea, stats_->getHeap());
  }
  stats_ = stats;
  setStatsCopy(isStatsCopy);
  setStatsInSharedSegment(inSharedSegment);
  if (prevStmtStats_ != NULL) {
    if (prevStmtStats_->canbeGCed() && (!prevStmtStats_->isWMSMonitoredCliQuery())) {
      if (statsGlobals != NULL) {
        if (getSemaphore) {
          error = statsGlobals->getStatsSemaphore(cliGlobals_->getSemId(), cliGlobals_->myPin());
        }
        prevStmtStats_->setStmtStatsUsed(FALSE);
        statsGlobals->removeQuery(cliGlobals_->myPin(), prevStmtStats_);
        if (getSemaphore) statsGlobals->releaseStatsSemaphore(cliGlobals_->getSemId(), cliGlobals_->myPin());
      }
    } else
      prevStmtStats_->setStmtStatsUsed(FALSE);
    prevStmtStats_ = NULL;
  }
}

int ContextCli::holdAndSetCQD(const char *defaultName, const char *defaultValue) {
  ExeCliInterface cliInterface(exHeap(), 0, this);

  return ExExeUtilTcb::holdAndSetCQD(defaultName, defaultValue, &cliInterface);
}

int ContextCli::restoreCQD(const char *defaultName) {
  ExeCliInterface cliInterface(exHeap(), 0, this);

  return ExExeUtilTcb::restoreCQD(defaultName, &cliInterface, &this->diags());
}

int ContextCli::setCS(const char *csName, char *csValue) {
  ExeCliInterface cliInterface(exHeap(), 0, this);

  return ExExeUtilTcb::setCS(csName, csValue, &cliInterface);
}

int ContextCli::resetCS(const char *csName) {
  ExeCliInterface cliInterface(exHeap(), 0, this);

  return ExExeUtilTcb::resetCS(csName, &cliInterface, &this->diags());
}

int parse_statsReq(short statsReqType, char *statsReqStr, int statsReqStrLen, char *nodeName, short &cpu,
                     pid_t &pid, long &timeStamp, int &queryNumber) {
  char tempStr[ComSqlId::MAX_QUERY_ID_LEN + 1];
  char *ptr;
  long tempNum;
  char *cpuTempPtr;
  char *pinTempPtr;
  char *timeTempPtr = (char *)NULL;
  char *queryNumTempPtr = (char *)NULL;
  short pHandle[10];
  int error;
  int tempCpu;
  pid_t tempPid;
  long tempTime = -1;
  int tempQnum = -1;
  short len;
  int cpuMinRange;
  CliGlobals *cliGlobals;

  if (statsReqStr == NULL || nodeName == NULL || statsReqStrLen > ComSqlId::MAX_QUERY_ID_LEN) return -1;
  cpu = 0;
  pid = 0;
  switch (statsReqType) {
    case SQLCLI_STATS_REQ_QID:
      if (str_cmp(statsReqStr, "MXID", 4) != 0) return -1;
      break;
    case SQLCLI_STATS_REQ_CPU:
    case SQLCLI_STATS_REQ_RMS_INFO:
      str_cpy_all(tempStr, statsReqStr, statsReqStrLen);
      tempStr[statsReqStrLen] = '\0';
      if ((ptr = str_chr(tempStr, '.')) != NULL) {
        *ptr++ = '\0';
        str_cpy_all(nodeName, tempStr, str_len(tempStr));
        nodeName[str_len(tempStr)] = '\0';
      } else {
        nodeName[0] = '\0';
        ptr = tempStr;
      }
      tempNum = str_atoi(ptr, str_len(ptr));
      if (statsReqType == SQLCLI_STATS_REQ_CPU)
        cpuMinRange = 0;
      else
        cpuMinRange = -1;
      if (tempNum < cpuMinRange) return -1;
      cpu = (short)tempNum;
      pid = -1;
      break;
    case SQLCLI_STATS_REQ_PID:
    case SQLCLI_STATS_REQ_PROCESS_INFO:
      if (strncasecmp(statsReqStr, "CURRENT", 7) == 0) {
        cliGlobals = GetCliGlobals();
        pid = cliGlobals->myPin();
        cpu = cliGlobals->myCpu();
        break;
      }
      str_cpy_all(tempStr, statsReqStr, statsReqStrLen);
      tempStr[statsReqStrLen] = '\0';
      if ((ptr = str_chr(tempStr, '.')) != NULL) {
        *ptr++ = '\0';
        str_cpy_all(nodeName, tempStr, str_len(tempStr));
        nodeName[str_len(tempStr)] = '\0';
      } else {
        nodeName[0] = '\0';
        ptr = tempStr;
      }
      if (*ptr != '$') {
        cpuTempPtr = ptr;
        if ((ptr = str_chr(cpuTempPtr, ',')) != NULL) {
          *ptr++ = '\0';
          pinTempPtr = ptr;
        } else
          return -1;
        tempNum = str_atoi(cpuTempPtr, str_len(cpuTempPtr));
        if (tempNum < 0) return -1;
        cpu = (short)tempNum;
        tempNum = str_atoi(pinTempPtr, str_len(pinTempPtr));
        pid = (pid_t)tempNum;
      } else {
        if ((error = msg_mon_get_process_info(ptr, &tempCpu, &tempPid)) != XZFIL_ERR_OK) return -1;
        cpu = tempCpu;
        pid = tempPid;
      }
      break;
    case SQLCLI_STATS_REQ_QID_INTERNAL:
      str_cpy_all(tempStr, statsReqStr, statsReqStrLen);
      tempStr[statsReqStrLen] = '\0';
      nodeName[0] = '\0';
      ptr = tempStr;
      cpuTempPtr = ptr;
      if ((ptr = str_chr(cpuTempPtr, ',')) != NULL) {
        *ptr++ = '\0';
        pinTempPtr = ptr;
      } else
        return -1;
      tempCpu = str_atoi(cpuTempPtr, str_len(cpuTempPtr));
      if (tempCpu < 0) return -1;
      cpu = tempCpu;
      if ((ptr = str_chr(pinTempPtr, ',')) != NULL) {
        *ptr++ = '\0';
        timeTempPtr = ptr;
      } else
        return -1;
      tempPid = str_atoi(pinTempPtr, str_len(pinTempPtr));
      if (tempPid < 0) return -1;
      pid = (pid_t)tempPid;
      if ((ptr = str_chr(timeTempPtr, ',')) != NULL) {
        *ptr++ = '\0';
        queryNumTempPtr = ptr;
      } else
        return -1;
      tempTime = str_atoi(timeTempPtr, str_len(timeTempPtr));
      if (tempTime < 0) return -1;
      timeStamp = tempTime;
      tempQnum = str_atoi(queryNumTempPtr, str_len(queryNumTempPtr));
      if (tempQnum < 0) return -1;
      queryNumber = tempQnum;
      break;
    default:
      return -1;
  }
  return 0;
}

void ContextCli::killIdleMxcmp() {
  long currentTimestamp;
  int compilerIdleTimeout;
  long recentIpcTimestamp;

  if (arkcmpArray_.entries() == 0) return;
  if (arkcmpArray_[0]->getServer() == NULL) return;
  compilerIdleTimeout = getSessionDefaults()->getCompilerIdleTimeout();
  if (compilerIdleTimeout == 0) return;
  currentTimestamp = NA_JulianTimestamp();
  recentIpcTimestamp = arkcmpArray_[0]->getRecentIpcTimestamp();
  if (recentIpcTimestamp != -1 && (((currentTimestamp - recentIpcTimestamp) / 1000000) >= compilerIdleTimeout))
    killAndRecreateMxcmp();
}

void ContextCli::killAndRecreateMxcmp() {
  for (unsigned short i = 0; i < arkcmpArray_.entries(); i++) delete arkcmpArray_[i];

  arkcmpArray_.clear();
  arkcmpInitFailed_.clear();

  arkcmpArray_.insertAt(
      0, new (exCollHeap()) ExSqlComp(0, exCollHeap(), cliGlobals_, 0, COM_VERS_COMPILER_VERSION, NULL, env_));
  arkcmpInitFailed_.insertAt(0, arkcmpIS_OK_);
}

// Initialize the database user ID from the OS user ID. If a row in
// the AUTHS table contains the OS user ID in the EXTERNAL_USER_NAME
// column, we switch to the user associated with that row. Otherwise
// this method has no side effects. Errors and warnings are written
// into diagsArea_.
void ContextCli::initializeUserInfoFromOS() {}

#pragma page "ContextCli::authQuery"
// *****************************************************************************
// *                                                                           *
// * Function: ContextCli::authQuery                                           *
// *                                                                           *
// *    Queries the AUTHS table for the specified authorization name or ID.    *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <queryType>                     AuthQueryType                   In       *
// *    is the type of query to perform.                                       *
// *                                                                           *
// *  <authName>                      const char *                    In       *
// *    is the name of the authorization entry to retrieve.  Optional field    *
// *  only required for queries that lookup by name, otherwise NULL.           *
// *                                                                           *
// *  <authID>                        int                             In       *
// *    is the name of the authorization entry to retrieve.  Optional field    *
// *  only supplied for queries that lookup by numeric ID.                     *
// *                                                                           *
// *  <authNameFromTable>             char *                          Out      *
// *    passes back the name of the authorization entry.                       *
// *                                                                           *
// *  <authNameMaxLen>                int                             In       *
// *    is the maximum number of characters that can be written to             *
// *  authNameFromTable.                                                       *
// *                                                                           *
// *  <authIDFromTable>              int                              Out      *
// *    passes back the numeric ID of the authorization entry.                 *
// *                                                                           *
// *
// * <tenantInfo>                   TenantInfo*                       Out      *
//                                                                             *
// *****************************************************************************
// *                                                                           *
// *  Returns: RETCODE                                                         *
// *                                                                           *
// *  SUCCESS: auth ID/name found, auth name/ID returned                       *
// *  ERROR: auth ID/name not found                                            *
// *                                                                           *
// *****************************************************************************

RETCODE ContextCli::authQuery(AuthQueryType queryType, const char *authName, int authID, char *authNameFromTable,
                              int authNameMaxLen, int &authIDFromTable, TenantInfo *tenantInfo)

{
  // We may need to perform a transactional lookup into metadata.
  // The following steps will be taken to manage the
  // transaction. The same steps are used in the Statement class when
  // we read the authorization information
  //
  //  1. Disable autocommit
  //  2. Note whether a transaction is already in progress
  //  3. Do the work
  //  4. Commit the transaction if a new one was started
  //  5. Enable autocommit if it was disabled

  // If we hit errors and need to inject the user name in error
  // messages, but the caller passed in a user ID not a name, we will
  // use the string form of the user ID.

  const char *nameForDiags = authName;
  char localNameBuf[32];
  char isValidFromUsersTable[3];

  if (queryType == USERS_QUERY_BY_USER_ID || queryType == ROLE_QUERY_BY_ROLE_ID ||
      queryType == ROLES_QUERY_BY_AUTH_ID || queryType == TENANT_BY_USER_ID) {
    sprintf(localNameBuf, "Authorization ID %d", (int)authID);
    nameForDiags = localNameBuf;
  }

  //  1. Disable autocommit
  NABoolean autoCommitDisabled = FALSE;

  if (transaction_->autoCommit()) {
    autoCommitDisabled = TRUE;
    transaction_->disableAutoCommit();
  }

  //  2. Note whether a transaction is already in progress
  NABoolean txWasInProgress = transaction_->xnInProgress();

  //  3. Do the work
  CmpSeabaseDDLauth::AuthStatus authStatus = CmpSeabaseDDLauth::STATUS_GOOD;
  RETCODE result = SUCCESS;
  CmpSeabaseDDLuser userInfo;
  CmpSeabaseDDLauth authInfo;
  CmpSeabaseDDLrole roleInfo;
  CmpSeabaseDDLgroup groupInfo;
  CmpSeabaseDDLtenant tenantAuthInfo;
  CmpSeabaseDDLauth *authInfoPtr = NULL;

  // Calls to authQuery operations return error details in CmpCommon::diags.
  // However ContextCli uses the diagsArea_ to report issues.  So, we copy the
  // diags returned from CmpCommon::diags into the diagsArea_ and then remove
  // errors from CmpCommon::diags generated during the authQuery request.
  // This avoids potentially reporting the same error twice.
  // For example:
  //     call to getAuthNameFromAuthID initiates from DDL (CmpCommon::diags)
  //     getAuthNameFromAuthID calls authQuery which uses diagsArea_
  //     authQuery gets information from the DDL layer (CmpCommon::diags)
  //       and merges any errors into diagsArea_
  // If authQuery did not clear out CmpCommon::diags, the error would be
  // reported twice. Once from the authQuery call and once when the DDL request
  // merges the results from diagsArea_.

  // The CmpCommon::diags() may not be allocated yet.
  int diagsMark(-1);
  NABoolean diagsExist = (CmpCommon::diags() != NULL);
  if (diagsExist) diagsMark = CmpCommon::diags()->mark();

  switch (queryType) {
    case USERS_QUERY_BY_USER_NAME: {
      authInfoPtr = &userInfo;
      authStatus = userInfo.getUserDetails(authName, false);
    } break;

    case USERS_QUERY_BY_EXTERNAL_NAME: {
      authInfoPtr = &userInfo;
      authStatus = userInfo.getUserDetails(authName, true);
    } break;

    case USERS_QUERY_BY_USER_ID: {
      authInfoPtr = &userInfo;
      authStatus = userInfo.getUserDetails(authID);
    } break;

    case AUTH_QUERY_BY_NAME: {
      authInfoPtr = &authInfo;
      authStatus = authInfo.getAuthDetails(authName, false);
    } break;

    case ROLE_QUERY_BY_ROLE_ID: {
      authInfoPtr = &roleInfo;
      authStatus = roleInfo.getAuthDetails(authID);
    } break;

    case ROLES_QUERY_BY_AUTH_ID: {
      authInfoPtr = &authInfo;
      std::vector<int32_t> roleIDs;
      std::vector<int32_t> granteeIDs;
      authStatus = authInfo.getRoleIDs(authID, roleIDs, granteeIDs);
      ex_assert((roleIDs.size() == granteeIDs.size()), "mismatch between roleIDs and granteeIDs");
      numRoles_ = roleIDs.size() + 1;  // extra for public role
      roleIDs_ = new (&exHeap_) int[numRoles_];
      granteeIDs_ = new (&exHeap_) int[numRoles_];

      for (size_t i = 0; i < roleIDs.size(); i++) {
        roleIDs_[i] = roleIDs[i];
        granteeIDs_[i] = granteeIDs[i];
      }

      // Add the public user to the last entry
      int lastEntry = numRoles_ - 1;
      roleIDs_[lastEntry] = PUBLIC_USER;
      granteeIDs_[lastEntry] = databaseUserID_;
    } break;

    case TENANT_BY_NAME: {
      authInfoPtr = &tenantAuthInfo;
      bool temp;
      if (tenantInfo) authStatus = tenantAuthInfo.getTenantDetails(authName, *tenantInfo);
    } break;

    case TENANT_BY_USER_ID: {
      authInfoPtr = &tenantAuthInfo;
      if (tenantInfo) authStatus = tenantAuthInfo.getTenantDetails(authID, *tenantInfo);
    } break;

    case QUERY_FOR_EXTERNAL_NAME: {
      authInfoPtr = &authInfo;
      authStatus = authInfo.getAuthDetails(authID);
    } break;

    case GROUP_QUERY_BY_GROUP_ID: {
      // same as user
      authInfoPtr = &groupInfo;
      authStatus = groupInfo.getAuthDetails(authID);
    }

    default: {
      ex_assert(0, "Invalid query type");
    } break;
  }

  if (authStatus == CmpSeabaseDDLauth::STATUS_GOOD) {
    // TODO: Check for valid auth
    authIDFromTable = authInfoPtr->getAuthID();
    if (queryType == QUERY_FOR_EXTERNAL_NAME)
      result = storeName(authInfoPtr->getAuthExtName().data(), authNameFromTable, authNameMaxLen);
    else
      result = storeName(authInfoPtr->getAuthDbName().data(), authNameFromTable, authNameMaxLen);

    if (result == ERROR) diagsArea_ << DgSqlCode(-CLI_USERNAME_BUFFER_TOO_SMALL);
  }

  //  4. Commit the transaction if a new one was started
  if (!txWasInProgress && transaction_->xnInProgress()) transaction_->commitTransaction();

  //  5. Enable autocommit if it was disabled
  if (autoCommitDisabled) transaction_->enableAutoCommit();

  // Errors to consider:
  // * an unexpected error (authStatus == CmpSeabaseDDLauth::STATUS_ERROR)
  // * the row does not exist (authStatus == CmpSeabaseDDLauth::STATUS_NOTFOUND)
  // * row exists but is marked invalid
  // Errors are returned in the CmpCommon::diags area
  switch (authStatus) {
    case CmpSeabaseDDLauth::STATUS_ERROR: {
      result = ERROR;

      // Fix core dump occurring during regression run - doesn't fix the cause
      // of the problem but prevents the core.
      if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_) == 0) {
        diagsArea_ << DgSqlCode(-CLI_INTERNAL_ERROR)
                   << DgString0("Error returned but diags is not updated (ContextCLI::authQuery)");
      } else {
        diagsArea_.mergeAfter(*CmpCommon::diags());
        int primaryError = CmpCommon::diags()->getErrorEntry(1)->getSQLCODE();
        diagsArea_ << DgSqlCode(-CLI_PROBLEM_READING_USERS) << DgString0(nameForDiags) << DgInt1(primaryError);
      }
      break;
    }
    case CmpSeabaseDDLauth::STATUS_NOTFOUND: {
      result = ERROR;
      ExeErrorCode sqlcode = CLI_USER_NOT_REGISTERED;
      if ((queryType == TENANT_BY_NAME) || (queryType == TENANT_BY_USER_ID)) sqlcode = CLI_TENANT_NOT_REGISTERED;
      diagsArea_.clear();
      diagsArea_ << DgSqlCode(-sqlcode) << DgString0(nameForDiags);
      break;
    }
    case CmpSeabaseDDLauth::STATUS_WARNING: {
      // If warnings were generated, do not propagate them to the caller
      diagsArea_.clear();
      break;
    }
    case CmpSeabaseDDLauth::STATUS_GOOD:
      // TODO: Check for invalid user?
      break;
    case CmpSeabaseDDLauth::STATUS_UNKNOWN:
    default: {
      diagsArea_ << DgSqlCode(-CLI_INTERNAL_ERROR) << DgString0("Unknown auth status in ContextCLI::authQuery");
      diagsArea_.mergeAfter(*CmpCommon::diags());
    }
  }

  // At this time all errors should have been merged into the Context's diagsArea_
  // Remove errors generated by authQuery but retain existing diags.
  if (diagsExist)
    CmpCommon::diags()->rewind(diagsMark);
  else {
    // If diags did not exist but now exist, just clear.  There aren't any
    // diags to retain
    if (CmpCommon::diags()) CmpCommon::diags()->clear();
  }

  return result;
}
//*********************** End of ContextCli::authQuery *************************

// Public method to update the databaseUserID_ and databaseUserName_
// members and at the same time. It also calls dropSession() to create a
// session boundary.
void ContextCli::setDatabaseUser(const int &uid, const char *uname) {
  // Since this was moved from private to public, do some sanity checks
  // to make sure the passed in parameters are valid.  We don't want to
  // read any metadata since this could get into an infinte loop.
  ex_assert((uid >= MIN_USERID && uid <= MAX_USERID), "Invalid userID was specified");
  ex_assert((uname != NULL && strlen(uname) > 0), "No username was specified");

  // If the passed in credentials match what is stored, nothing needs
  // to be done.
  if (databaseUserID_ == uid && strlen(uname) == strlen(databaseUserName_) &&
      str_cmp(databaseUserName_, uname, strlen(uname)) == 0)
    return;

  // Is this the place to drop any compiler contexts?
  dropSession();

  // Save changed user credentials
  databaseUserID_ = uid;
  strncpy(databaseUserName_, uname, MAX_DBUSERNAME_LEN);
  databaseUserName_[MAX_DBUSERNAME_LEN] = 0;
}

// Public method to establish a new user identity. userID will be
// verified against metadata in the AUTHS table. If a matching row is
// not found an error code will be returned and diagsArea_ populated.
RETCODE ContextCli::setDatabaseUserByID(int userID) {
  char username[MAX_USERNAME_LEN + 1];
  int userIDFromMetadata;

  // See if the USERS row exists
  RETCODE result = authQuery(USERS_QUERY_BY_USER_ID,
                             NULL,      // IN user name (ignored)
                             userID,    // IN user ID
                             username,  // OUT
                             sizeof(username), userIDFromMetadata);

  // Update the instance if the USERS lookup was successful
  if (result != ERROR) setDatabaseUser(userIDFromMetadata, username);

  return result;
}

// Public method to establish a new user identity. userName will be
// verified against metadata in the AUTHS table. If a matching row is
// not found an error code will be returned and diagsArea_ populated.
RETCODE ContextCli::setDatabaseUserByName(const char *userName) {
  char usersNameFromUsersTable[MAX_USERNAME_LEN + 1];
  int userIDFromUsersTable;

  RETCODE result = authQuery(USERS_QUERY_BY_USER_NAME,
                             userName,                 // IN user name
                             0,                        // IN user ID (ignored)
                             usersNameFromUsersTable,  // OUT
                             sizeof(usersNameFromUsersTable), userIDFromUsersTable);

  // Update the instance if the lookup was successful
  if (result != ERROR) setDatabaseUser(userIDFromUsersTable, usersNameFromUsersTable);

  return result;
}

// -----------------------------------------------------------------------------
// Method:  getUserAttrs
//
// Gets user details for connection logic
// Returns:
//   usersInfo - defined in auth.h (DBSecurity) containing information needed by
//     connection logic
//   authDetails - defined in sqlcli.h contaning information needed in addition
//     to usersInfo required to authenticate a user
//
// Connection logic is performed in multiple steps:
//    Authenticate the user (uses usersInfo & authDetails)
//    Store user details in various mxosrvr caches (uses usersInfo)
//
// Errors:
//    -1 - error occurred
//     0 - successful (user and/or tenant may not exist)
// -----------------------------------------------------------------------------
int ContextCli::getUserAttrs(const char *username, const char *tenantname, USERS_INFO *usersInfo,
                               struct SQLSEC_AuthDetails *authDetails) {
  ex_assert((username), "Invalid username was specified");
  ex_assert((tenantname), "Invalid tenant name was specified");
  ex_assert((usersInfo), "Invalid connection attributes was specified");
  ex_assert((authDetails), "Invalid user attributes was specified");

  // record status of any active transaction
  // All the calls in this method should just be selects so
  // a transaction should not be started.  But to be similar to
  // other calls made in this file, will add transaction support.
  NABoolean autoCommitDisabled = FALSE;
  if (transaction_->autoCommit()) {
    autoCommitDisabled = TRUE;
    transaction_->disableAutoCommit();
  }
  NABoolean txWasInProgress = transaction_->xnInProgress();

  bool enabled = msg_license_multitenancy_enabled();
  int retcode = 0;

  usersInfo->reset();
  authDetails->reset();

  authDetails->num_tenants = 1;
  if (CmpCommon::getDefault(TRAF_TENANT_GROUP_CHECK) == DF_ON)
    authDetails->tenant_group_check = true;
  else
    authDetails->tenant_group_check = false;

  // get user info
  CmpSeabaseDDLauth authInfo;
  retcode = addUserInfo(username, usersInfo, authDetails, authInfo);

  // get tenant info
  if (retcode >= 0 && enabled) retcode = addTenantInfo(tenantname, usersInfo, authDetails, authInfo);
  // get group info from local table
  if (CmpCommon::getAuthenticationType() != COM_LDAP_AUTH && retcode >= 0 && enabled) {
    std::vector<int32_t> groupIDs;
    usersInfo->groupList.clear();
    // Read metadata about the user
    ULng32 flagBits = getSqlParserFlags();
    setSqlParserFlags(0x20000);
    if (CmpSeabaseDDLauth::STATUS_GOOD == authInfo.getUserGroups(groupIDs)) {
      std::vector<std::string> tmp;
      authInfo.getAuthDbNameByAuthIDs(groupIDs, tmp);
      if (!tmp.empty()) {
        usersInfo->groupList.insert(tmp.begin(), tmp.end());
      }
    }
    assignSqlParserFlags(flagBits);
  }

  // Calls to get authentication attrs return error details in CmpCommon::diags.
  // At this time all errors should have been merged into the Context's diagsArea_
  // Clear out the CmpCommon diags to avoid subsequent failures.
  CmpCommon::diags()->clear();

  // If successful, commit transaction
  if (!txWasInProgress && transaction_->xnInProgress()) {
    if (retcode >= 0)
      transaction_->commitTransaction();
    else
      transaction_->rollbackTransaction();
  }
  if (autoCommitDisabled) transaction_->enableAutoCommit();

  return retcode;
}

// -----------------------------------------------------------------------------
// Method:  addUserInfo
//
// Add data to usersInfo and authsDetails that pertain the users.
// It also returns authInfo that contains user details read from the metadata
//
// Returns:
//    -1 - unexpected error
//     0 - successful
// -----------------------------------------------------------------------------
int ContextCli::addUserInfo(const char *username, USERS_INFO *usersInfo, struct SQLSEC_AuthDetails *authDetails,
                              CmpSeabaseDDLauth &authInfo) {
  ComAuthenticationType authType = CmpCommon::getAuthenticationType();
  /*
  //this core was delete on R2.8
  //In the original design, it was designed to speed up authentication
  //because the database did not need to manage passwords
  //This is very insecure, so delete it
  if (strcmp(username, externalUsername_) == 0)
  {
    int authID = databaseUserID_;
    usersInfo->dbUsername = new char[strlen(databaseUserName_) + 1];
    strcpy (usersInfo->dbUsername, databaseUserName_);
    usersInfo->effectiveUserID = databaseUserID_;
    usersInfo->sessionUserID = sessionUserID_;
    usersInfo->redefTime = 12345;
    usersInfo->configNumber = 0;

    authDetails->auth_id = authID;
    authDetails->auth_is_valid = true;
    return 0;
  }
  */

  // Read metadata about the user
  ULng32 flagBits = getSqlParserFlags();
  CmpSeabaseDDLauth::AuthStatus authStatus;
  setSqlParserFlags(0x20000);
  authStatus = authInfo.getAuthDetails(username, true);
  assignSqlParserFlags(flagBits);

  // User is registered
  if (authStatus == CmpSeabaseDDLauth::STATUS_GOOD || authStatus == CmpSeabaseDDLauth::STATUS_WARNING) {
    usersInfo->dbUsername = new char[authInfo.getAuthDbName().length() + 1];
    strcpy(usersInfo->dbUsername, authInfo.getAuthDbName().data());
    usersInfo->dbUserPassword = new char[authInfo.getAuthPassword().length() + 1];
    strcpy(usersInfo->dbUserPassword, authInfo.getAuthPassword().data());
    usersInfo->effectiveUserID = authInfo.getAuthID();
    usersInfo->sessionUserID = authInfo.getAuthID();
    usersInfo->redefTime = authInfo.getAuthRedefTime();
    usersInfo->passwdModTime = authInfo.getPasswdModTime();
    usersInfo->configNumber = authInfo.getAuthConfig();

    authDetails->auth_id = authInfo.getAuthID();
    authDetails->auth_is_valid = authInfo.isAuthValid();

    return 0;
  }

  // User is not not registered
  if (COM_LDAP_AUTH == authType && CmpCommon::getDefault(TRAF_AUTO_REGISTER_USER) == DF_ON) {
    return 0;
  }
  diagsArea_ << DgSqlCode(-1076) << DgString0(username);
  return -1;
}

// -----------------------------------------------------------------------------
// Method:  addTenantInfo
//
// Add data to usersInfo and authsDetails that pertain the specified tenant.
//
// Returns:
//    -1 - unexpected error
//     0 - successful
// -----------------------------------------------------------------------------
int ContextCli::addTenantInfo(const char *tenantname, USERS_INFO *usersInfo, struct SQLSEC_AuthDetails *authDetails,
                                CmpSeabaseDDLauth &authInfo) {
  CmpSeabaseDDLauth::AuthStatus authStatus = CmpSeabaseDDLauth::STATUS_GOOD;
  ULng32 flagBits = getSqlParserFlags();
  Int16 retcode = 0;

  NAString tname(tenantname);
  if (tname == NAString(DB__SYSTEMTENANT) || tname.isNull()) {
    // no "initialize authorization" no check
    if (!(CmpCommon::context()->isAuthorizationEnabled() && CmpCommon::context()->isAuthorizationReady())) {
      authDetails->num_tenants = 1;
      goto GET_TENANT_INFO;
    }
    // special users can connect, assume numTenants = 1
    if ((authDetails->auth_id == ROOT_USER_ID) || (authDetails->auth_id == ADMIN_USER_ID))
      authDetails->num_tenants = 1;
    else {
      // If user granted root or services role they can connect,
      // assume numTenants = 1

      // Since user details in the context have not been setup yet, can't use
      // ComUser::currentUserHasRole that checks the cached role list.
      // Instead, read metadata
      int servicesID = NA_UserIdDefault;
      setSqlParserFlags(0x20000);
      retcode = ComUser::getAuthIDFromAuthName(DB__SERVICESROLE, servicesID, NULL);
      assignSqlParserFlags(flagBits);

      if (retcode < 0) {
        // unexpected error occurred, DB__SERVICESROLE should exist
        diagsArea_.mergeAfter(*CmpCommon::diags());
        return -1;
      }

      std::vector<int32_t> roleIDs;
      std::vector<int32_t> granteeIDs;
      setSqlParserFlags(0x20000);
      authStatus = authInfo.getRoleIDs(authInfo.getAuthID(), roleIDs, granteeIDs);
      assignSqlParserFlags(flagBits);

      if (authStatus == CmpSeabaseDDLauth::STATUS_ERROR) {
        diagsArea_.mergeAfter(*CmpCommon::diags());
        return -1;
      }

      // TBD, should we also allow anyone granted ADMIN_ROLE_ID access?
      if (std::find(roleIDs.begin(), roleIDs.end(), servicesID) != roleIDs.end() ||
          std::find(roleIDs.begin(), roleIDs.end(), ROOT_ROLE_ID) != roleIDs.end())
        authDetails->num_tenants = 1;

      // Get the total number of tenants currently defined
      else {
        setSqlParserFlags(0x20000);
        authDetails->num_tenants = authInfo.getNumberTenants();
        assignSqlParserFlags(flagBits);

        // Unexpected error, diags area setup
        if (authDetails->num_tenants == -1) {
          diagsArea_.mergeAfter(*CmpCommon::diags());
          return -1;
        }
      }
    }

  GET_TENANT_INFO:
    // If tenant name not specified, set it to the default
    if (tname.isNull() && authDetails->num_tenants == 1) tname = DB__SYSTEMTENANT;
  }

  // read tenant details
  CmpSeabaseDDLtenant tenant;
  TenantInfo tenantInfo(&exHeap_);
  setSqlParserFlags(0x20000);
  authStatus = tenant.getTenantDetails(tname.data(), tenantInfo);
  assignSqlParserFlags(flagBits);

  if (authStatus == CmpSeabaseDDLauth::STATUS_ERROR) {
    diagsArea_.mergeAfter(*CmpCommon::diags());
    return -1;
  }

  // Not found and if there is only 1 tenant, assume ESGYNDB
  if (authStatus == CmpSeabaseDDLauth::STATUS_NOTFOUND) {
    if (authDetails->num_tenants == 1) {
      usersInfo->tenantID = 1500000;
      usersInfo->tenantName = new char[strlen(DB__SYSTEMTENANT) + 1];
      strcpy(usersInfo->tenantName, DB__SYSTEMTENANT);
      usersInfo->tenantRedefTime = 0;
      usersInfo->tenantAffinity = -1;
      usersInfo->tenantSize = -1;
      usersInfo->tenantClusterSize = -1;
      usersInfo->defaultSchema = NULL;
      authDetails->tenant_id = 1500000;
      authDetails->tenant_is_valid = 1;
    } else
      authDetails->tenant_is_valid = false;

    return 0;
  }

  // Tenant exists
  usersInfo->tenantID = tenant.getAuthID();
  usersInfo->tenantName = new char[tname.length() + 1];
  strcpy(usersInfo->tenantName, tname.data());
  usersInfo->tenantRedefTime = tenant.getAuthRedefTime();
  usersInfo->tenantAffinity = tenantInfo.getAffinity();
  usersInfo->tenantSize = tenantInfo.getTenantSize();
  usersInfo->tenantClusterSize = tenantInfo.getClusterSize();
  if (tenantInfo.getDefaultSchemaUID() == NA_UserIdDefault)
    usersInfo->defaultSchema = NULL;
  else {
    NAString defaultSchemaStr = tenantInfo.getDefaultSchemaName();
    if (defaultSchemaStr.length() == 0)
      usersInfo->defaultSchema = NULL;
    else {
      usersInfo->defaultSchema = new char[defaultSchemaStr.length() + 1];
      strcpy(usersInfo->defaultSchema, defaultSchemaStr.data());
    }
  }

  authDetails->tenant_id = tenant.getAuthID();
  authDetails->tenant_is_valid = tenant.isAuthValid();

  if (tenant.isAuthValid()) {
    setSqlParserFlags(0x20000);
    TenantGroupInfoList *inGroupList = tenant.getTenantGroupList(tenantInfo, exHeap());
    assignSqlParserFlags(flagBits);
    if (inGroupList) {
      authDetails->num_groups = inGroupList->entries();
      authDetails->group_list = new SQLSEC_GroupInfo[authDetails->num_groups];
      for (int i = 0; i < inGroupList->entries(); i++) {
        TenantGroupInfo *inGroup = (*inGroupList)[i];
        char *outGroupName = new char[inGroup->getGroupName().length() + 1];
        strcpy(outGroupName, inGroup->getGroupName().data());
        authDetails->group_list[i].group_name = outGroupName;
        authDetails->group_list[i].group_id = inGroup->getGroupID();
        authDetails->group_list[i].group_config_num = inGroup->getConfig();
      }
    }

    // Unable to process group list, error
    else {
      diagsArea_.mergeAfter(*CmpCommon::diags());
      return -1;
    }
  }
  return 0;
}

// -----------------------------------------------------------------------------
// Register a user through a special CLI request
//
// Returns:
//   usersInfo - connection details
//   authDetails - authentication details
//
// Errors:
//    -1 = unexpected error (ComDiags is set up)
//     n = authID for new user
// -----------------------------------------------------------------------------
int ContextCli::registerUser(const char *username, const char *config, USERS_INFO *usersInfo,
                               struct SQLSEC_AuthDetails *authDetails) {
  ex_assert((username), "Invalid username was specified");
  ex_assert((usersInfo), "Invalid connection info was specified");
  ex_assert((authDetails), "Invalid user attributes was specified");

  //  1. Disable autocommit
  NABoolean autoCommitDisabled = FALSE;
  if (transaction_->autoCommit()) {
    autoCommitDisabled = TRUE;
    transaction_->disableAutoCommit();
  }

  //  2. Note whether a transaction is already in progress
  NABoolean txWasInProgress = transaction_->xnInProgress();

  //  3. Perform the request
  CmpSeabaseDDLuser userInfo;
  NAString dbUserName(username);
  NAString authPassword(AUTH_DEFAULT_WORD);
  ULng32 flagBits = getSqlParserFlags();
  setSqlParserFlags(0x20000);

  int retcode = userInfo.registerUserInternal(dbUserName, dbUserName, config, true, ROOT_USER_ID, authPassword);
  assignSqlParserFlags(flagBits);
  if (retcode == 0) {
    usersInfo->dbUsername = new char[userInfo.getAuthDbName().length() + 1];
    strcpy(usersInfo->dbUsername, userInfo.getAuthDbName().data());
    usersInfo->effectiveUserID = userInfo.getAuthID();
    usersInfo->sessionUserID = userInfo.getAuthID();
    usersInfo->redefTime = userInfo.getAuthRedefTime();
    usersInfo->configNumber = userInfo.getAuthConfig();

    authDetails->auth_id = userInfo.getAuthID();
    authDetails->auth_is_valid = userInfo.isAuthValid();
  } else
    diagsArea_.mergeAfter(*CmpCommon::diags());

  // Calls to get auto register users return error details in CmpCommon::diags.
  // At this time errors should have been merged into the Context's diagsArea_.
  // Clear out the CmpCommon diags to avoid subsequent failures.
  CmpCommon::diags()->clear();

  //  4. Commit the transaction if a new one was started
  if (!txWasInProgress && transaction_->xnInProgress()) {
    if (retcode < 0)
      transaction_->rollbackTransaction();
    else
      transaction_->commitTransaction();
  }

  //  5. Enable autocommit if it was disabled
  if (autoCommitDisabled) transaction_->enableAutoCommit();

  //  6. Return result
  return retcode;
}

int ContextCli::getAuthErrPwdCnt(int userid, Int16 &errcnt) {
  //  1. Disable autocommit
  NABoolean autoCommitDisabled = FALSE;
  if (transaction_->autoCommit()) {
    autoCommitDisabled = TRUE;
    transaction_->disableAutoCommit();
  }

  //  2. Note whether a transaction is already in progress
  NABoolean txWasInProgress = transaction_->xnInProgress();

  //  3. Perform the request
  CmpSeabaseDDLauth authInfo;
  ULng32 flagBits = getSqlParserFlags();
  setSqlParserFlags(0x20000);
  int retcode = authInfo.getErrPwdCnt(userid, errcnt);
  assignSqlParserFlags(flagBits);
  if (retcode != 0) {
    diagsArea_.mergeAfter(*CmpCommon::diags());
  }

  //  4. Commit the transaction if a new one was started
  if (!txWasInProgress && transaction_->xnInProgress()) {
    if (retcode < 0)
      transaction_->rollbackTransaction();
    else
      transaction_->commitTransaction();
  }

  //  5. Enable autocommit if it was disabled
  if (autoCommitDisabled) transaction_->enableAutoCommit();

  //  6. Return result
  return retcode;
}

int ContextCli::updateAuthErrPwdCnt(int userid, Int16 errcnt, bool reset) {
  //  1. Disable autocommit
  NABoolean autoCommitDisabled = FALSE;
  if (transaction_->autoCommit()) {
    autoCommitDisabled = TRUE;
    transaction_->disableAutoCommit();
  }

  //  2. Note whether a transaction is already in progress
  NABoolean txWasInProgress = transaction_->xnInProgress();

  //  3. Perform the request
  CmpSeabaseDDLuser userInfo;
  ULng32 flagBits = getSqlParserFlags();
  setSqlParserFlags(0x20000);
  int retcode = userInfo.updateErrPwdCnt(userid, errcnt, reset);
  assignSqlParserFlags(flagBits);
  if (retcode != 0) {
    diagsArea_.mergeAfter(*CmpCommon::diags());
  }

  //  4. Commit the transaction if a new one was started
  if (!txWasInProgress && transaction_->xnInProgress()) {
    if (retcode < 0)
      transaction_->rollbackTransaction();
    else
      transaction_->commitTransaction();
  }

  //  5. Enable autocommit if it was disabled
  if (autoCommitDisabled) transaction_->enableAutoCommit();

  //  6. Return result
  return retcode;
}

// Public method to update the context with the tenant details
// and members and at the same time.
// ----- deprecated, remove in a future commit ------
void ContextCli::setTenantInfoInContext(int tenantID, const char *tenantName, int affinity, int nodeSize,
                                        int clusterSize, const char *tenantDefaultSchema) {
  NAASNodes asNodes(nodeSize, clusterSize, affinity);
  NAText serializedNodes;

  asNodes.serialize(serializedNodes);

  setTenantInfoInContext(tenantID, tenantName, serializedNodes.c_str(), tenantDefaultSchema);
}

// Public method to update the context with the tenant details
// and members and at the same time.
void ContextCli::setTenantInfoInContext(int tenantID, const char *tenantName, const char *serializedAvailableNodes,
                                        const char *tenantDefaultSchema) {
  // Do some sanity checks
  // to make sure the passed in parameters are valid.  We don't want to
  // read any metadata since this could get into an infinte loop.
  ex_assert((tenantID >= MIN_TENANTID && tenantID <= MAX_TENANTID), "Invalid tenantID was specified");
  ex_assert((tenantName != NULL && strlen(tenantName) > 0), "No tenantName was specified");

  int numErrors = 0;

  if (!(CURRCONTEXT_CLUSTERINFO->hasVirtualNodes())) {
    // assign this process to the cgroup for this tenant
    // (cgroup and tenant have the same name, by convention)
    tenantCGroup_.setOrAlterCGroup(tenantName, diagsArea_);
    numErrors = (diagsArea_).getNumber(DgSqlCode::ERROR_);
  }

  if (numErrors == 0) {
    // alter these values only if the cgroup setting did not return an error
    setTenantNodes(serializedAvailableNodes);
    if (tenantDefaultSchema != NULL) strcpy(tenantDefaultSchema_, tenantDefaultSchema);
    // Save  tenant credentials
    tenantID_ = tenantID;
    if (tenantName_) {
      strncpy(tenantName_, tenantName, MAX_DBUSERNAME_LEN);
      tenantName_[MAX_DBUSERNAME_LEN] = 0;
    }
  }
}

// Public method to establish a new tenant identity. tenantID will be
// verified against metadata in the AUTHS table. If a matching row is
// not found an error code will be returned and diagsArea_ populated.
RETCODE ContextCli::retrieveAndSetTenantInfo(int tenantID) {
  char tenantName[MAX_USERNAME_LEN + 1];
  int tenantIDFromMetadata;
  TenantInfo tenantInfo(&exHeap_);
  ULng32 flagBits = getSqlParserFlags();
  setSqlParserFlags(0x20000);

  // See if the USERS row exists
  RETCODE result = authQuery(TENANT_BY_USER_ID,
                             NULL,        // IN user name (ignored)
                             tenantID,    // IN user ID
                             tenantName,  // OUT
                             sizeof(tenantName), tenantIDFromMetadata,
                             &tenantInfo);  // OUT
  assignSqlParserFlags(flagBits);

  // Update the instance if the USERS lookup was successful
  NAString defaultSchema = tenantInfo.getDefaultSchemaName();

  if (result != ERROR) {
    NAText serializedTenant;

    tenantInfo.getTenantNodes()->serialize(serializedTenant);
    setTenantInfoInContext(tenantID, tenantName, serializedTenant.c_str(), defaultSchema);
  }
  if ((diagsArea_).getNumber(DgSqlCode::ERROR_) > 0) return ERROR;

  tenantInfo.updateDefaultSchema(defaultSchema);

  return result;
}

// Public method to establish a new tenant identity. tenantName will be
// verified against metadata in the AUTHS table. If a matching row is
// not found an error code will be returned and diagsArea_ populated.
// Used only by sqlci to support sqlci -t <tenant name>
RETCODE ContextCli::setTenantByName(const char *tenantName) {
  char tenantNameFromUsersTable[MAX_USERNAME_LEN + 1];
  int tenantIDFromUsersTable;
  TenantInfo tenantInfo(&exHeap_);
  ULng32 flagBits = getSqlParserFlags();
  setSqlParserFlags(0x20000);

  RETCODE result = authQuery(TENANT_BY_NAME,
                             tenantName,                // IN tenant name
                             0,                         // IN user ID (ignored)
                             tenantNameFromUsersTable,  // OUT
                             sizeof(tenantNameFromUsersTable), tenantIDFromUsersTable,
                             &tenantInfo);  // OUT
  assignSqlParserFlags(flagBits);

  // Update the instance if the lookup was successful
  if (result != ERROR) {
    NAText serializedTenant;

    tenantInfo.getTenantNodes()->serialize(serializedTenant);
    setTenantInfoInContext(tenantInfo.getTenantID(), tenantNameFromUsersTable, serializedTenant.c_str(),
                           tenantInfo.getDefaultSchemaName());
  }

  return result;
}

void ContextCli::setTenantID(int tenantID) {
  // Do some sanity checks
  // to make sure the passed in parameters are valid.  We don't want to
  // read any metadata since this could get into an infinte loop.
  ex_assert((tenantID >= MIN_TENANTID && tenantID <= MAX_TENANTID), "Invalid tenantID was specified");
  tenantID_ = tenantID;
}

void ContextCli::setTenantName(const char *tenantName) {
  ex_assert((tenantName != NULL && strlen(tenantName) > 0), "No tenantName was specified");

  strncpy(tenantName_, tenantName, MAX_DBUSERNAME_LEN);
  tenantName_[MAX_DBUSERNAME_LEN] = 0;

  // assign this process to the cgroup for this tenant
  // (cgroup and tenant have the same name, by convention)
  tenantCGroup_.setOrAlterCGroup(tenantName_, diagsArea_);
}

void ContextCli::setTenantNodes(const char *serializedNodes) {
  NAWNodeSet *tenantNodes = NULL;

  if (serializedNodes && strlen(serializedNodes) > 0) {
    tenantNodes = NAWNodeSet::deserialize(serializedNodes, &exHeap_);

    ex_assert(tenantNodes, "received invalid serialized nodes");
  }

  if (availableNodes_) delete availableNodes_;

  availableNodes_ = tenantNodes;
}

void ContextCli::setTenantDefaultSchema(const char *schemaName) {
  strncpy(tenantDefaultSchema_, schemaName, COL_MAX_SCHEMA_LEN);
}
// ****************************************************************************
// *
// * Function: ContextCli::getRoleList
// *
// * Return the role IDs and their grantee for the current user.  The grantee
// * includes the current user and current user's groups.
// *   If the list of roles is already stored, just return this list.
// *   If the list of roles does not exist extract the roles granted to the
// *     current user and their groups from the Metadata and store in roleIDs_.
// *
// ****************************************************************************
RETCODE ContextCli::getRoleList(int &numEntries, int *&roleIDs, int *&granteeIDs) {
  // If role list has not been created, go read metadata
  // There will always be at least one role - PUBLIC in the list
  if (roleIDs_ == NULL) {
    // If authorization is not enabled, just setup the PUBLIC role
    CmpContext *cmpCntxt = CmpCommon::context();
    ex_assert(cmpCntxt, "No compiler context exists");
    if (!cmpCntxt->isAuthorizationEnabled()) {
      numRoles_ = 1;
      roleIDs_ = new (&exHeap_) int[numRoles_];
      roleIDs_[0] = PUBLIC_USER;
      granteeIDs = new (&exHeap_) int[numRoles_];
      granteeIDs[0] = databaseUserID_;
      numEntries = numRoles_;
      roleIDs = roleIDs_;
      return SUCCESS;
    }

    // For Mantis-12304, we are recursively calling getRoleList when processing
    // QI keys. If we find we are in this recursive loop, then return and do not
    // read metadata.  See case for details.
    if (roleLoopLevel_ > 5 || rewindRoleLevels_) {
      QRLogger::log(CAT_SQL_EXE, LL_INFO, "%s",
                    "Recursively calling getRoleList, "
                    " skip retrieving user's role list from metadata.");

      numEntries = 0;
      roleIDs = NULL;
      granteeIDs = NULL;

      // Set rewindRoleLevels_ to TRUE which will cause the recursive stack to rewind
      rewindRoleLevels_ = TRUE;
      return SUCCESS;
    }

    if (getenv("TEST_QI_INJECTION")) {
      // Keep injecting QI events before retrieving role details to simulate
      // the error documented in Mantis-12304.
      SQL_QIKEY siKeyList[1];
      siKeyList[0].revokeKey.subject = 669332613;
      siKeyList[0].revokeKey.object = 4092767692;
      NAString actionKey("OS");
      strncpy(siKeyList[0].operation, actionKey.data(), 2);
      SQL_EXEC_SetSecInvalidKeys(1, siKeyList);
      sleep(5);
    }

    if (roleLoopLevel_ == 0) rewindRoleLevels_ = FALSE;

    roleLoopLevel_++;
    // cout << "Increment role list level: " << roleLoopLevel_ << endl;

    // Get roles for userID
    char usersNameFromUsersTable[MAX_USERNAME_LEN + 1];
    int userIDFromUsersTable;
    RETCODE result = authQuery(ROLES_QUERY_BY_AUTH_ID,
                               NULL,  // user name
                               databaseUserID_,
                               usersNameFromUsersTable,  // OUT
                               sizeof(usersNameFromUsersTable), userIDFromUsersTable);
    roleLoopLevel_--;
    // cout << "Decrement role list level: " << roleLoopLevel_ << endl;

    if (result != SUCCESS) return result;
  }

  numEntries = numRoles_;
  roleIDs = roleIDs_;
  granteeIDs = granteeIDs_;

  return SUCCESS;
}

// ****************************************************************************
// *
// * Function: ContextCli::resetRoleList
// *
// * Deletes the current role list, a subsequent call to getRoleList reads the
// * metadata to get the list of roles associated with the current user and,
// * for the current user, stores the list in roleIDs_ member
// *
// ****************************************************************************
RETCODE ContextCli::resetRoleList() {
  if (roleIDs_) NADELETEBASIC(roleIDs_, &exHeap_);
  roleIDs_ = NULL;

  if (granteeIDs_) NADELETEBASIC(granteeIDs_, &exHeap_);
  granteeIDs_ = NULL;

  numRoles_ = 0;
  return SUCCESS;
}

// *****************************************************************************
// *                                                                           *
// * Function: ContextCli::getAuthIDFromName                                   *
// *                                                                           *
// *    Map an authentication name to a numeric ID.  If the name cannot be     *
// * mapped to a number, an error is returned.                                 *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <authName>                      const char *                    In       *
// *    is the authorization name to be mapped to a numeric ID.                *
// *                                                                           *
// *  <authID>                        int &                         Out      *
// *    passes back the numeric ID.                                            *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Returns: RETCODE                                                         *
// *                                                                           *
// *  SUCCESS: auth ID returned                                                *
// *  ERROR: auth name not found                                               *
// *                                                                           *
// *****************************************************************************

RETCODE ContextCli::getAuthIDFromName(const char *authName, int &authID)

{
  RETCODE result = SUCCESS;
  char authNameFromTable[MAX_USERNAME_LEN + 1];

  if (authName == NULL) return ERROR;

  // Cases to consider
  // * authName is the current user name
  // * SYSTEM_USER and PUBLIC_USER have special integer user IDs and
  //   are not registered in the AUTHS table
  // * other users

  if (databaseUserName_ && strcasecmp(authName, databaseUserName_) == 0) {
    authID = databaseUserID_;
    return SUCCESS;
  }

  if (strcasecmp(authName, ComUser::getPublicUserName()) == 0) {
    authID = ComUser::getPublicUserID();
    return SUCCESS;
  }

  if (strcasecmp(authName, ComUser::getSystemUserName()) == 0) {
    authID = ComUser::getSystemUserID();
    return SUCCESS;
  }

  // TODO: If list of roles granted to user is cached in context, search there first.

  return authQuery(AUTH_QUERY_BY_NAME, authName, 0, authNameFromTable, sizeof(authNameFromTable), authID);
}
//******************* End of ContextCli::getAuthIDFromName *********************

// *****************************************************************************
// *                                                                           *
// * Function: ContextCli::getAuthNameFromID                                   *
// *                                                                           *
// *    Map an integer authentication ID to a name.  If the number cannot be   *
// * mapped to a name, the numeric ID is converted to a string.                *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <authID>                        int                           In       *
// *    is the numeric ID to be mapped to a name.                              *
// *                                                                           *
// *  <authName>                      char *                          Out      *
// *    passes back the name that the numeric ID mapped to.  If the ID does    *
// *  not map to a name, the ASCII equivalent of the ID is passed back.        *
// *                                                                           *
// *  <maxBufLen>                     int                           In       *
// *    is the size of <authName>.                                             *
// *                                                                           *
// *  <requiredLen>                   int &                         Out      *
// *    is the size of the auth name in the table.  If larger than <maxBufLen> *
// *  caller needs to supply a larger buffer.                                  *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Returns: RETCODE                                                         *
// *                                                                           *
// *  SUCCESS: auth name returned                                              *
// *  ERROR: auth name too small, see <requiredLen>                            *
// *                                                                           *
// *****************************************************************************

RETCODE ContextCli::getAuthNameFromID(int authID,        // IN
                                      char *authName,      // OUT
                                      int maxBufLen,     // IN
                                      int &requiredLen)  // OUT optional

{
  RETCODE result = SUCCESS;
  char authNameFromTable[MAX_USERNAME_LEN + 1];
  int authIDFromTable;
  TenantInfo tenantInfo(&exHeap_);

  requiredLen = 0;

  // Cases to consider
  // * userID is the current user ID
  // * SYSTEM_USER and PUBLIC_USER have special integer user IDs and
  //   are not registered in the AUTHS table
  // * other users

  if (authID == (int)databaseUserID_) return storeName(databaseUserName_, authName, maxBufLen, requiredLen);

  if (authID == ComUser::getPublicUserID())
    return storeName(ComUser::getPublicUserName(), authName, maxBufLen, requiredLen);

  if (authID == ComUser::getSystemUserID())
    return storeName(ComUser::getSystemUserName(), authName, maxBufLen, requiredLen);

  //
  // For user and role (and eventually group, need to lookup in metadata.
  //

  switch (ComUser::getAuthType(authID)) {
    case ComUser::AUTH_USER:
      result = authQuery(USERS_QUERY_BY_USER_ID,
                         NULL,               // IN user name (ignored)
                         authID,             // IN user ID
                         authNameFromTable,  // OUT
                         sizeof(authNameFromTable), authIDFromTable);
      if (result == SUCCESS) return storeName(authNameFromTable, authName, maxBufLen, requiredLen);
      break;

    case ComUser::AUTH_ROLE:
      result = authQuery(ROLE_QUERY_BY_ROLE_ID,
                         NULL,               // IN user name (ignored)
                         authID,             // IN user ID
                         authNameFromTable,  // OUT
                         sizeof(authNameFromTable), authIDFromTable);
      if (result == SUCCESS) return storeName(authNameFromTable, authName, maxBufLen, requiredLen);
      break;

    case ComUser::AUTH_TENANT:
      result = authQuery(TENANT_BY_USER_ID,
                         NULL,               // IN user name (ignored)
                         authID,             // IN user ID
                         authNameFromTable,  // OUT
                         sizeof(authNameFromTable), authIDFromTable,
                         &tenantInfo);  // OUT
      if (result == SUCCESS) return storeName(authNameFromTable, authName, maxBufLen, requiredLen);
      break;

    default:;  // ACH Error?
  }
  //
  // Could not lookup auth ID.  Whether due to auth ID not defined, or SQL error,
  // or any other cause, a valid string returned, so we use numeric
  // input value and convert it to a string.  Note, we still require the caller
  // (executor) to provide a buffer with sufficient space.
  //

  sprintf(authNameFromTable, "%d", (int)authID);
  return storeName(authNameFromTable, authName, maxBufLen, requiredLen);
}
//******************* End of ContextCli::getAuthNameFromID *********************

const char *ContextCli::getExternalUserName() {
  if (externalUsername_ && strcmp(externalUsername_, DEFAULT_EXTERNAL_NAME) != 0) return externalUsername_;

  // Either the external name is not setup or it is set to the default name
  // Go find the actual name for the current user.
  RETCODE result = SUCCESS;
  char usersNameFromAuths[MAX_USERNAME_LEN + 1];
  int userIDFromAuths;

  result = authQuery(QUERY_FOR_EXTERNAL_NAME, NULL, databaseUserID_,
                     usersNameFromAuths,  // OUT
                     sizeof(usersNameFromAuths), userIDFromAuths);

  // if error, just return what exists in context
  if (result == ERROR) {
    if (externalUsername_ == NULL) {
      externalUsername_ = (char *)exHeap()->allocateMemory(strlen(DEFAULT_EXTERNAL_NAME) + 1);
      strcpy(externalUsername_, DEFAULT_EXTERNAL_NAME);
      return externalUsername_;
    }
  }

  // set up correct external name
  if (externalUsername_) exHeap()->deallocateMemory(externalUsername_);
  externalUsername_ = (char *)exHeap()->allocateMemory(strlen(usersNameFromAuths) + 1);
  strcpy(externalUsername_, usersNameFromAuths);

  return externalUsername_;
}

// Public method only meant to be called in ESPs. All we do is call
// the private method to update user ID data members. On platforms
// other than Linux the method is a no-op.
void ContextCli::setDatabaseUserInESP(const int &uid, const char *uname, bool closeAllOpens) {
  // check if we need to close all opens to avoid share opens for different
  // users
  if (closeAllOpens) {
    if (databaseUserID_ != uid || !strncmp(databaseUserName_, uname, MAX_DBUSERNAME_LEN)) {
      setDatabaseUser(uid, uname);
    }
    // else, user hasn't changed
    // TODO: Consider whether tenant ID test should be added here as well
  } else
    setDatabaseUser(uid, uname);
}

void ContextCli::setAuthStateInCmpContexts(NABoolean authEnabled, NABoolean authReady) {
  // change authorizationEnabled and authorizationReady state
  // in all the compiler contexts
  CmpContextInfo *cmpCntxtInfo;
  CmpContext *cmpCntxt;
  short i;
  for (i = 0; i < cmpContextInfo_.entries(); i++) {
    cmpCntxtInfo = cmpContextInfo_[i];
    cmpCntxt = cmpCntxtInfo->getCmpContext();
    cmpCntxt->setIsAuthorizationEnabled(authEnabled);
    cmpCntxt->setIsAuthorizationReady(authReady);
  }
}

void ContextCli::getAuthState(int &authenticationType, bool &authorizationEnabled, bool &authorizationReady,
                              bool &auditingEnabled) {
  // Check authorization state
  CmpContext *cmpCntxt = CmpCommon::context();
  ex_assert(cmpCntxt, "No compiler context exists");
  authorizationEnabled = cmpCntxt->isAuthorizationEnabled();
  authorizationReady = cmpCntxt->isAuthorizationReady();

  // Check for authentication type
  authenticationType = (int)GetCliGlobals()->getAuthenticationType();

  // set auditingState to FALSE
  // TDB - add auditing support
  auditingEnabled = false;
}

void ContextCli::setUdrXactAborted(long currTransId, NABoolean b) {
  if (udrXactId_ != long(-1) && currTransId == udrXactId_) udrXactAborted_ = b;
}

void ContextCli::clearUdrErrorFlags() {
  udrAccessModeViolation_ = FALSE;
  udrXactViolation_ = FALSE;
  udrXactAborted_ = FALSE;
  udrXactId_ = getTransaction()->getTransid();
}

NABoolean ContextCli::sqlAccessAllowed() const {
  if (udrErrorChecksEnabled_ && (udrSqlAccessMode_ == (int)COM_NO_SQL)) return FALSE;
  return TRUE;
}

//********************** Switch CmpContext ************************************
int ContextCli::switchToCmpContext(int cmpCntxtType) {
  if (cmpCntxtType < 0 || cmpCntxtType >= CmpContextInfo::CMPCONTEXT_TYPE_LAST) return -2;

  const char *cmpCntxtName = CmpContextInfo::getCmpContextClassName(cmpCntxtType);
  bool isFirstContext = false;

  // find CmpContext_ with the same type to use
  CmpContextInfo *cmpCntxtInfo = NULL;
  CmpContext *cmpCntxt = NULL;
  short i;
  for (i = 0; i < cmpContextInfo_.entries(); i++) {
    cmpCntxtInfo = cmpContextInfo_[i];

    if (cmpCntxtInfo->isSameClass(cmpCntxtName)) {
      cmpCntxt = cmpCntxtInfo->getCmpContext();
      if (cmpCntxt->getRecursionLevel() > 0) continue;
      // found a CmpContext instance to switch to
      cmpCntxtInfo->incrUseCount();
      break;
    }
  }

  if (i == cmpContextInfo_.entries()) {
    // find none to use, create new CmpContext instance
    CmpContext *savedCntxt = cmpCurrentContext;
    int rc = 0;
    if (rc = arkcmp_main_entry()) {
      cmpCurrentContext = savedCntxt;
      if (rc == 2)
        return -2;  // error during NADefaults creation
      else
        return -1;  // failed to create new CmpContext instance
    }

    cmpCntxt = CmpCommon::context();
    cmpCntxt->setCIClass((CmpContextInfo::CmpContextClassType)cmpCntxtType);
    cmpCntxt->setCIindex(i);

    cmpCntxtInfo = new (exCollHeap()) CmpContextInfo(cmpCntxt, cmpCntxtName);
    cmpCntxtInfo->incrUseCount();
    cmpContextInfo_.insert(i, cmpCntxtInfo);
    isFirstContext = (0 == i);
  }

  // save current and switch to the qualified CmpContext
  if (embeddedArkcmpContext_) cmpContextInUse_.insert(embeddedArkcmpContext_);
  embeddedArkcmpContext_ = cmpCntxt;
  cmpCurrentContext = cmpCntxt;  // restore the thread global
  if (isFirstContext) {
    // frist context
    doInitForFristContext();
  }

  return 0;  // success
}

int ContextCli::switchToCmpContext(void *cmpCntxt) {
  if (!cmpCntxt) return -1;  // invalid input

  // check if given CmpContext is known
  CmpContextInfo *cmpCntxtInfo;
  bool isFirstContext = false;
  short i;
  for (i = 0; i < cmpContextInfo_.entries(); i++) {
    cmpCntxtInfo = cmpContextInfo_[i];
    if (cmpCntxtInfo->getCmpContext() == (CmpContext *)cmpCntxt) {
      // make sure the context is unused before switch
      assert(cmpCntxtInfo->getUseCount() == 0);
      cmpCntxtInfo->incrUseCount();
      break;
    }
  }

  // add the given CmpContext if not know
  if (i == cmpContextInfo_.entries()) {
    cmpCntxtInfo = new (exCollHeap()) CmpContextInfo((CmpContext *)cmpCntxt, "NONE");
    cmpCntxtInfo->incrUseCount();
    cmpContextInfo_.insert(i, cmpCntxtInfo);
    isFirstContext = (0 == i);
  }

  // save current and switch to given CmpContext
  if (embeddedArkcmpContext_) cmpContextInUse_.insert(embeddedArkcmpContext_);
  embeddedArkcmpContext_ = (CmpContext *)cmpCntxt;
  cmpCurrentContext = (CmpContext *)cmpCntxt;  // restore the thread global
  if (isFirstContext) {
    // frist context
    doInitForFristContext();
  }

  return (cmpCntxtInfo->getUseCount() == 1 ? 0 : 1);  // success
}

void ContextCli::copyDiagsAreaToPrevCmpContext() {
  ex_assert(cmpContextInUse_.entries(), "Invalid use of switch back call");

  CmpContext *curr = embeddedArkcmpContext_;

  if (cmpContextInUse_.getLast(prevCmpContext_) == FALSE) return;
  if (curr->diags()->getNumber() > 0) prevCmpContext_->diags()->mergeAfter(*curr->diags());
}

int ContextCli::switchBackCmpContext(void) {
  if (prevCmpContext_ == NULL) {
    ex_assert(cmpContextInUse_.entries(), "Invalid use of switch back call");
    if (cmpContextInUse_.getLast(prevCmpContext_) == FALSE) return -1;
  }
  // switch back
  CmpContext *curr = embeddedArkcmpContext_;

  embeddedArkcmpContext_ = prevCmpContext_;
  cmpCurrentContext = prevCmpContext_;  // restore the thread global

  // book keeping
  CmpContextInfo *cmpCntxtInfo;
  for (short i = 0; i < cmpContextInfo_.entries(); i++) {
    cmpCntxtInfo = cmpContextInfo_[i];
    if (cmpCntxtInfo->getCmpContext() == curr) {
      cmpCntxtInfo->decrUseCount();
      break;
    }
  }

  // switch back to a previous context
  cmpCurrentContext->switchBackContext();

  deinitializeArkcmp();
  prevCmpContext_ = NULL;

  return 0;  // success
}

void ContextCli::initGlobalNADefaultsEntries() { CmpContext::initGlobalNADefaultsEntries(); }

// *****************************************************************************
// *                                                                           *
// * Function: ContextCli::storeName                                           *
// *                                                                           *
// *    Stores a name in a buffer.  If the name is too large, an error is      *
// * returned.                                                                 *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <src>                           const char *                    In       *
// *    is the name to be stored.                                              *
// *                                                                           *
// *  <dest>                          char *                          Out      *
// *    is the buffer to store the name into.                                  *
// *                                                                           *
// *  <maxLength>                     int                             In       *
// *    is the maximum number of bytes (including trailing null) that can      *
// *    be stored into <dest>.                                                 *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Returns: RETCODE                                                         *
// *                                                                           *
// *  SUCCESS: destination returned                                            *
// *  ERROR: destination too small                                             *
// *                                                                           *
// *****************************************************************************
RETCODE ContextCli::storeName(const char *src, char *dest, int maxLength)

{
  int actualLength;

  return storeName(src, dest, maxLength, actualLength);
}
//*********************** End of ContextCli::storeName *************************

// *****************************************************************************
// *                                                                           *
// * Function: ContextCli::storeName                                           *
// *                                                                           *
// *    Stores a name in a buffer.  If the name is too large, an error is      *
// * returned along with the size of the name.                                 *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <src>                           const char *                    In       *
// *    is the name to be stored.                                              *
// *                                                                           *
// *  <dest>                          char *                          Out      *
// *    is the buffer to store the name into.                                  *
// *                                                                           *
// *  <maxLength>                     int                             In       *
// *    is the maximum number of bytes (including trailing null) that can      *
// *    be stored into <dest>.                                                 *
// *                                                                           *
// *  <actualLength>                  int &                           In       *
// *    is the number of bytes (not including the trailing null) in <src>,     *
// *    and <also dest>, if the store operation is successful.                 *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Returns: RETCODE                                                         *
// *                                                                           *
// *  SUCCESS: destination returned                                            *
// *  ERROR: destination too small, see <actualLength>                         *
// *                                                                           *
// *****************************************************************************
RETCODE ContextCli::storeName(const char *src, char *dest, int maxLength, int &actualLength)

{
  actualLength = strlen(src);
  // Go ahead and return a partial name if buffer is not big enough
  if (actualLength >= maxLength) {
    memcpy(dest, src, maxLength - 1);
    dest[maxLength] = 0;
    diagsArea_ << DgSqlCode(-CLI_USERNAME_BUFFER_TOO_SMALL);
    return ERROR;
  }

  memcpy(dest, src, actualLength);
  dest[actualLength] = 0;
  return SUCCESS;
}
//*********************** End of ContextCli::storeName *************************

CollIndex ContextCli::addTrustedRoutine(LmRoutine *r) {
  // insert routine into a free array element and return its index
  CollIndex result = trustedRoutines_.freePos();

  ex_assert(r != NULL, "Trying to insert a NULL routine into CliContext");
  trustedRoutines_.insertAt(result, r);

  return result;
}

LmRoutine *ContextCli::findTrustedRoutine(CollIndex ix) {
  if (trustedRoutines_.used(ix))
    return trustedRoutines_[ix];
  else {
    diags() << DgSqlCode(-CLI_ROUTINE_INVALID);
    return NULL;
  }
}

void ContextCli::putTrustedRoutine(CollIndex ix) {
  // free element ix of the Routine array and delete the object
  ex_assert(trustedRoutines_[ix] != NULL, "Trying to delete a non-existent routine handle");

  LmResult res = LM_ERR;
  LmLanguageManager *lm = cliGlobals_->getLanguageManager(trustedRoutines_[ix]->getLanguage());

  if (lm) res = lm->putRoutine(trustedRoutines_[ix]);

  if (res != LM_OK) diags() << DgSqlCode(-CLI_ROUTINE_DEALLOC_ERROR);
  trustedRoutines_.remove(ix);
}

// This method looks for an hdfsServer connection with the given hdfsServer
// name and port. If the conection does not exist, a new connection is made and
// cached in the hdfsHandleList_ hashqueue for later use. The hdfsHandleList_
// gets cleaned up when the thread exits.
hdfsFS ContextCli::getHdfsServerConnection(char *hdfs_server, int port, hdfsConnectStruct **entryPtr) {
  if (hdfs_server == NULL)  // guard against NULL and use default value.
  {
    // TODO: illegal assignment
    hdfs_server = (char *)"default";
    port = 0;
  }
  if (hdfsHandleList_) {
    // Look for the entry on the list
    hdfsHandleList_->position(hdfs_server, strlen(hdfs_server));
    hdfsConnectStruct *hdfsConnectEntry = NULL;
    while (hdfsConnectEntry = (hdfsConnectStruct *)(hdfsHandleList_->getNext())) {
      if ((strcmp(hdfsConnectEntry->hdfsServer_, hdfs_server) == 0) && (hdfsConnectEntry->hdfsPort_ == port)) {
        if (entryPtr) *entryPtr = hdfsConnectEntry;

        return (hdfsConnectEntry->hdfsHandle_);
      }
    }
  }

  // If not found create a new one and add to list.
  hdfsFS newFS = NULL;
  if (strcmp(hdfs_server, "__local_fs__") == 0) {
    // NULL for the host means to connect to the LocalFileSystem
    // (this would be done when the location URI is: 'file:/')
    newFS = hdfsConnect(NULL, port);
  } else {
    newFS = hdfsConnect(hdfs_server, port);
  }

  hdfsConnectStruct *hdfsConnectEntry = new (exCollHeap()) hdfsConnectStruct;
  memset(hdfsConnectEntry, 0, sizeof(hdfsConnectStruct));
  hdfsConnectEntry->hdfsHandle_ = newFS;
  hdfsConnectEntry->hdfsPort_ = port;
  str_cpy_all(hdfsConnectEntry->hdfsServer_, hdfs_server, str_len(hdfs_server));
  hdfsHandleList_->insert(hdfs_server, strlen(hdfs_server), hdfsConnectEntry);

  if (entryPtr) *entryPtr = hdfsConnectEntry;

  return newFS;
}

void ContextCli::disconnectHdfsConnections() {
  if (hdfsHandleList_) {
    hdfsConnectStruct *entry = NULL;
    while (entry = (hdfsConnectStruct *)hdfsHandleList_->getNext()) {
      hdfsDisconnect(entry->hdfsHandle_);
    }
  }
}

int ContextCli::loadTrafMetadataInCache() {
  ComDiagsArea *diagsArea = &diagsArea_;
  ExeCliInterface cliInterface(exHeap(), 0, this);
  int retcode =
      cliInterface.executeImmediate((char *)"load trafodion metadata in cache;", NULL, NULL, TRUE, NULL, 0, &diagsArea);
  if (retcode != 0 && retcode != 100) {
    QRLogger::log(CAT_SQL_EXE, LL_WARN, "%s %d", "Load trafodion metadata in cache failed with error code", retcode);
    QRLogger::logDiags(&diagsArea_, CAT_SQL_EXE);
    diagsArea_.clear();
  } else
    QRLogger::log(CAT_SQL_EXE, LL_INFO, "%s", "Load trafodion metadata in cache completed successfully");
  return 0;
}

int ContextCli::loadTrafMetadataIntoSharedCache() {
  ComDiagsArea *diagsArea = &diagsArea_;
  ExeCliInterface cliInterface(exHeap(), 0, this);
  int retcode = cliInterface.executeImmediate((char *)"load trafodion metadata into shared cache;", NULL, NULL, TRUE,
                                                NULL, 0, &diagsArea);
  if (retcode == 0 || retcode == 100) {
    QRLogger::log(CAT_SQL_EXE, LL_INFO, "%s", "Load trafodion metadata into shared cache completed successfully");
  } else if (retcode > 0) {
    QRLogger::log(CAT_SQL_EXE, LL_INFO, "%s %d",
                  "Load trafodion metadata into shared cache completed successfully with warning code", retcode);
  } else {
    QRLogger::log(CAT_SQL_EXE, LL_WARN, "%s %d", "Load trafodion metadata into shared cache failed with error code",
                  retcode);
    QRLogger::logDiags(&diagsArea_, CAT_SQL_EXE);
    diagsArea_.clear();
  }
  return 0;
}

int ContextCli::loadTrafDataIntoSharedCache() {
  ComDiagsArea *diagsArea = &diagsArea_;
  ExeCliInterface cliInterface(exHeap(), 0, this);
  int retcode = cliInterface.executeImmediate((char *)"load trafodion data into shared cache;", NULL, NULL, TRUE,
                                                NULL, 0, &diagsArea);
  if (retcode == 0 || retcode == 100) {
    QRLogger::log(CAT_SQL_EXE, LL_INFO, "%s", "Load trafodion data into shared cache completed successfully");
  } else if (retcode > 0) {
    QRLogger::log(CAT_SQL_EXE, LL_INFO, "%s %d",
                  "Load trafodion data into shared cache completed successfully with warning code", retcode);
  } else {
    QRLogger::log(CAT_SQL_EXE, LL_WARN, "%s %d", "Load trafodion data into shared cache failed with error code",
                  retcode);
    QRLogger::logDiags(&diagsArea_, CAT_SQL_EXE);
    diagsArea_.clear();
  }
  return 0;
}

void ContextCli::collectSpecialRTStats() {
  // collect actual data stats and save them in hdfs. The file name is
  // the query hash.
  const char *getRTStatsQuery = "get statistics for qid current default, options 'AS'";

  ExeCliInterface *cliInterface = new (exHeap()) ExeCliInterface(exHeap(), SQLCHARSETCODE_UTF8, this, NULL);

  int retcode = cliInterface->executeImmediate((char *)getRTStatsQuery,
                                                 NULL,   // outputBuf
                                                 NULL,   // outputBufLen
                                                 TRUE,   // null terminated
                                                 NULL,   // long* rowsAffected
                                                 FALSE,  // monitorThis
                                                 NULL);  // ComDiagsArea * globalDiags

  delete cliInterface;
}

NABoolean ContextCli::initEmbeddedArkcmp(NABoolean executeInESP) {
  if (!isEmbeddedArkcmpInitialized()) {
    int embeddedArkcmpSetup;
    // embeddedArkcmpSetup = arkcmp_main_entry();
    embeddedArkcmpSetup = switchToCmpContext((int)0);
    if (embeddedArkcmpSetup == 0) {
      setEmbeddedArkcmpIsInitialized(TRUE);
    } else if (embeddedArkcmpSetup == -2) {
      return FALSE;
    } else {
      setEmbeddedArkcmpIsInitialized(FALSE);
      setEmbeddedArkcmpContext(NULL);
    }
  }
  // Set the Global CmpContext from the one saved in the CLI context
  // for proper operation
  if (isEmbeddedArkcmpInitialized() && getEmbeddedArkcmpContext()) {
    cmpCurrentContext = getEmbeddedArkcmpContext();
    cmpCurrentContext->setExecuteInESP(executeInESP);

    cmpCurrentContext->setCliContext(this);
  }

  return TRUE;
}

void ContextCli::displayAllCmpContextInfo() {
  cout << "cmpContextInfo_ is as follows." << endl;
  for (int i = 0; i < cmpContextInfo_.entries(); i++) {
    CmpContextInfo *cmpCntxtInfo = cmpContextInfo_[i];
    cout << " CmpContext[" << i << "]=" << static_cast<void *>(cmpCntxtInfo->getCmpContext()) << "  "
         << cmpCntxtInfo->getName() << "  " << cmpCntxtInfo->getCmpContext()->getCIindex() << endl;
  }
}

void ContextCli::displayAllCmpContextInUse() {
  cout << "cmpContextInUse_ is as follows." << endl;
  for (int i = 0; i < cmpContextInUse_.entries(); i++) {
    CmpContext *cmpContext = cmpContextInUse_[i];
    cout << " CmpContext[" << i << "]=" << static_cast<void *>(cmpContext) << endl;
  }
}

void ContextCli::setClientInfo(const char *value, int length, const char *characterset) {
  if (clientInfo_ != NULL) NADELETEBASIC(clientInfo_, exCollHeap());
  clientInfo_ = new (exCollHeap()) char[length + 1];
  strncpy(clientInfo_, value, length);
  clientInfo_[length] = '\0';
}

int ContextCli::prepareCallTrigger() {
  if (cliInterfaceTrigger_ != NULL) {
    return 0;
  }

  char query[1000];
  str_sprintf(query, "call %s.\"%s\".%s(?,?,?,?,?,?,?,?,?,?,?,?,?)", TRAFODION_SYSTEM_CATALOG, SEABASE_MD_SCHEMA,
              SEABASE_SPSQL_CALL_SPJ_TRIGGER);

  cliInterfaceTrigger_ = new (exHeap()) ExeCliInterface(exHeap());
  return cliInterfaceTrigger_->executeImmediatePrepare(query);
}

int ContextCli::executeCallTrigger(char *tblName, int isBefore, int opType, char *oldSKVBuffer, char *oldBufSize,
                                     char *oldRowIDs, char *oldRowIDsLen, int newRowIDLen, char *newRowIDs,
                                     int newRowIDsLen, char *newRows, int newRowsLen, char *curExecSql) {
  setSkipCheckValidPrivs(true);
  setTriggerParam(1, tblName);
  setTriggerParam(2, isBefore);
  setTriggerParam(3, opType);
  setTriggerParam(4, oldSKVBuffer);
  setTriggerParam(5, oldBufSize);
  setTriggerParam(6, oldRowIDs);
  setTriggerParam(7, oldRowIDsLen);
  setTriggerParam(8, newRowIDLen);
  setTriggerParam(9, newRowIDs);
  setTriggerParam(10, newRowIDsLen);
  setTriggerParam(11, newRows);
  setTriggerParam(12, newRowsLen);
  setTriggerParam(13, curExecSql);

  int retCode = 0;
  retCode = cliInterfaceTrigger_->clearExecFetchClose(NULL, NULL, NULL, NULL);
  setSkipCheckValidPrivs(false);
  return retCode;
}
int ContextCli::getGracePwdCnt(int userid, Int16 &graceCounter) {
  //  1. Disable autocommit
  NABoolean autoCommitDisabled = FALSE;
  if (transaction_->autoCommit()) {
    autoCommitDisabled = TRUE;
    transaction_->disableAutoCommit();
  }

  //  2. Note whether a transaction is already in progress
  NABoolean txWasInProgress = transaction_->xnInProgress();

  //  3. Perform the request
  CmpSeabaseDDLauth authInfo;
  ULng32 flagBits = getSqlParserFlags();
  setSqlParserFlags(0x20000);
  int retcode = authInfo.getGracePwdCnt(userid, graceCounter);
  assignSqlParserFlags(flagBits);
  if (retcode != 0) {
    diagsArea_.mergeAfter(*CmpCommon::diags());
  }

  //  4. Commit the transaction if a new one was started
  if (!txWasInProgress && transaction_->xnInProgress()) {
    if (retcode < 0)
      transaction_->rollbackTransaction();
    else
      transaction_->commitTransaction();
  }

  //  5. Enable autocommit if it was disabled
  if (autoCommitDisabled) transaction_->enableAutoCommit();

  //  6. Return result
  return retcode;
}

int ContextCli::updateGracePwdCnt(int userid, Int16 graceCounter) {
  //  1. Disable autocommit
  NABoolean autoCommitDisabled = FALSE;
  if (transaction_->autoCommit()) {
    autoCommitDisabled = TRUE;
    transaction_->disableAutoCommit();
  }

  //  2. Note whether a transaction is already in progress
  NABoolean txWasInProgress = transaction_->xnInProgress();

  //  3. Perform the request
  CmpSeabaseDDLuser userInfo;
  ULng32 flagBits = getSqlParserFlags();
  setSqlParserFlags(0x20000);
  int retcode = userInfo.updateGracePwdCnt(userid, graceCounter);
  assignSqlParserFlags(flagBits);
  if (retcode != 0) {
    diagsArea_.mergeAfter(*CmpCommon::diags());
  }

  //  4. Commit the transaction if a new one was started
  if (!txWasInProgress && transaction_->xnInProgress()) {
    if (retcode < 0)
      transaction_->rollbackTransaction();
    else
      transaction_->commitTransaction();
  }

  //  5. Enable autocommit if it was disabled
  if (autoCommitDisabled) transaction_->enableAutoCommit();

  //  6. Return result
  return retcode;
}
