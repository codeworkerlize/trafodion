

/**********************************************************************/
#ifndef CONTEXT_H
#define CONTEXT_H

/* -*-C++-*-
 *****************************************************************************
 *
 * File:         Context.h
 * Description:  CLI context. A CLI context is an entire execution environment
 *               for an SQL client (including a user id, transactions,
 *               prepared statements, cursor states, etc). Right now
 *               (December 1999) we only support a single context.
 *
 * Created:      7/10/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "cli/CliSemaphore.h"
#include "cli/CliDefs.h"
#include "cli/ExSqlComp.h"
#include "cli/Globals.h"
#include "cli/Module.h"
#include "cli/SessionDefaults.h"
#include "cli/Statement.h"
#include "cli/sqlcli.h"
#include "common/CmpCommon.h"
#include "common/ComCGroup.h"
#include "common/NAUserId.h"
#include "common/Platform.h"
#include "executor/DistributedLock_JNI.h"
#include "executor/ExStats.h"
#include "executor/ex_god.h"
#include "executor/ex_transaction.h"
#include "executor/timeout_data.h"
#include "exp/ExpSeqGen.h"
#include "exp/hdfs.h"
#include "export/ComDiags.h"
#include "runtimestats/ssmpipc.h"

class CliGlobals;
class HashQueue;
class Queue;
class ExSqlComp;
class ExStatisticsArea;
class ExControlArea;
class ExUdrServer;
class UdrContextMsg;
class SequenceValueGenerator;
class LmRoutine;
class HBaseClient_JNI;
class TenantHelper_JNI;
class TenantHelper_JNI;
class IpcThreadInfo;
class NAWNodeSet;
class DistributedLock_JNI;

struct hdfsConnectStruct;

class CliDDLValidator;

class ContextCli : public ExGod {
 public:
  enum ArkcmpFailMode { arkcmpIS_OK_ = FALSE /*no failure*/, arkcmpWARN_, arkcmpERROR_ };

  enum CloseCursorType {
    CLOSE_ALL,                                         // close all non-holdable cursors
    CLOSE_ALL_INCLUDING_ANSI_PUBSUB_HOLDABLE,          // close all cursors including pubsub and ansi holdable cursors
    CLOSE_ALL_INCLUDING_ANSI_HOLDABLE,                 // close all non-holdable cursors and ansi holdable cursors
    CLOSE_ALL_INCLUDING_PUBSUB_HOLDABLE,               // close all non-holdable cursors and pubsub holdable cursors
    CLOSE_ALL_INCLUDING_ANSI_PUBSUB_HOLDABLE_WHEN_CQD  // close all incl AHC holdable cursors and pubsub holdable
                                                       // cursors when CQD PSHOLD_CLOSE_ON_ROLLBACK
  };

  enum closeTransactionType {
    CLOSE_ALL_XN,   // close cursors belonging to any transaction
    CLOSE_CURR_XN,  // close only those that are part of current transaction
    CLOSE_ENDED_XN  // close those cursors whose
                    // associated xns are no longer active
  };

  SQLCTX_HANDLE getContextHandle() { return contextHandle_; }

  // Accessor functions to retrieve the current user name and ID
  const char *getDatabaseUserName() { return databaseUserName_; }
  const char *getTenantName() { return tenantName_; }
  ComCGroup &getTenantCGroup() { return tenantCGroup_; }
  int *getDatabaseUserID() { return &databaseUserID_; }
  int *getSessionUserID() { return &sessionUserID_; }
  int getTenantID() { return tenantID_; }  // TODO: why do the other getters return a pointer?
  NAWNodeSet *getAvailableNodes() { return availableNodes_; }
  const char *getTenantDefaultSchema() { return tenantDefaultSchema_; }
  const char *getClientHostName() { return hostName_; }
  const char *getClientOsName() { return osUserName_; }
  const char *getClientIpAddr() { return ipAddress_; }
  // Functions to switch user identity. The specified user must exist
  // as a valid user in the USERS metadata table. Otherwise an error
  // code is returned and error conditions will be written to the
  // context's diagnostics area. These functions are only supported on
  // Linux.
  RETCODE setDatabaseUserByID(int userID);
  RETCODE setDatabaseUserByName(const char *userName);

  void setDatabaseUser(const int &uid,      // IN
                       const char *uname);  // IN

  int getUserAttrs(const char *username, const char *tenant_name, USERS_INFO *users_info,
                   struct SQLSEC_AuthDetails *auth_details);

  int registerUser(const char *username, const char *config, USERS_INFO *users_info,
                   struct SQLSEC_AuthDetails *auth_details);

  int getAuthErrPwdCnt(int userid, Int16 &errcnt);

  int updateAuthErrPwdCnt(int userid, Int16 errcnt, bool reset);

  int getGracePwdCnt(int, Int16 &);

  int updateGracePwdCnt(int userid, Int16 errcnt = 0);

  // Functions to switch tenant identity. The specified tenant must exist
  // as a valid name in the USERS metadata table. Otherwise an error
  // code is returned and error conditions will be written to the
  // context's diagnostics area. These functions are only supported on
  // Linux.
  RETCODE retrieveAndSetTenantInfo(
      int tenantID);  // retrieve tenantInfo from the tenants table and set values in the context
  RETCODE setTenantByName(const char *tenantName);
  void setTenantNodes(const char *serializedNodes);
  void setTenantDefaultSchema(const char *tenantDefSchName);
  // Set Tenant related info in the context
  // deprecated method, will go away eventually
  void setTenantInfoInContext(int tenantId, const char *tenantName, int affinity, int tenantSize, int clusterSize,
                              const char *tenantDefaultSchema);

  // new method, to be phased in
  void setTenantInfoInContext(int tenantId, const char *tenantName, const char *serializedAvailableNodes,
                              const char *tenantDefaultSchema);
  // Set tenant name and id alone in context
  void setTenantID(int tenantID);
  void setTenantName(const char *tenantName);

  // Functions to map between Trafodion authentication IDs and names.
  RETCODE getAuthIDFromName(const char *authName, int &authID);

  RETCODE getAuthNameFromID(int authID,            // IN
                            char *authNameBuffer,  // OUT
                            int maxBufLen,         // IN
                            int &requiredLen);     // OUT optional

  // Function to be used only in ESPs to establish user identity. This
  // call will update data members and will NOT verify the input
  // arguments by querying the USERS table.
  void setDatabaseUserInESP(const int &uid, const char *uname, bool closeAllOpens = false);

  const char *getExternalUserName();

  char *userSpecifiedSessionName() { return userSpecifiedSessionName_; }
  void setUserSpecifiedSessionName(const char *un) { strcpy(userSpecifiedSessionName_, un); }

  void setAuthStateInCmpContexts(NABoolean authEnabled, NABoolean authReady);

  void getAuthState(int &authenticationType, bool &authorizationEnabled, bool &authorizationReady,
                    bool &auditingEnabled);

  // functions to get and set roles for the current user
  RETCODE getRoleList(int &numEntries, int *&roleIDs, int *&granteeIDs);

  RETCODE resetRoleList();
  NABoolean roleIDsCached() { return roleIDs_ != NULL; }

  // functions to get and set roles for the current user
  RETCODE getGroupList(std::set<std::string> &groupList);

  RETCODE setGroupList(const std::set<std::string> &groupList);

  SequenceValueGenerator *&seqGen() { return seqGen_; }

  HashQueue *trafSElist() { return trafSElist_; }

  inline void incStmtReclaimCount() {
    stmtReclaimCount_++;
    if (processStats_ != NULL) processStats_->incReclaimStmtCount();
  }

  inline void decStmtReclaimCount() {
    stmtReclaimCount_--;
    if (processStats_ != NULL) processStats_->decReclaimStmtCount();
  }

  inline void incCloseStmtListSize() {
    closeStmtListSize_--;
    if (processStats_ != NULL) processStats_->incCloseStmtCount();
  }

  inline void decCloseStmtListSize() {
    closeStmtListSize_--;
    if (processStats_ != NULL) processStats_->decCloseStmtCount();
  }

  inline ExProcessStats *getExProcessStats() { return processStats_; }

  void setTenantHelper(TenantHelper_JNI *tenantHelperJNI) { tenantHelperJNI_ = tenantHelperJNI; }
  TenantHelper_JNI *getTenantHelper() { return tenantHelperJNI_; }

  DistributedLock_JNI *getDLockClient() { return DLockClientJNI_; }
  void setDLockClient(DistributedLock_JNI *DLockClientJNI) { DLockClientJNI_ = DLockClientJNI; }

  // expose cmpContextInfo_ to get HQC info of different contexts
  const NAArray<CmpContextInfo *> &getCmpContextInfo() const { return cmpContextInfo_; }

  // expose cmpContext stack to allow search
  const LIST(CmpContext *) & getCmpContextsInUse() const { return cmpContextInUse_; }

  void displayAllCmpContextInfo();
  void displayAllCmpContextInUse();

  void displayAllCmpContexts() {
    displayAllCmpContextInfo();
    displayAllCmpContextInUse();
  }

  CollIndex addTrustedRoutine(LmRoutine *r);
  LmRoutine *findTrustedRoutine(CollIndex ix);
  void putTrustedRoutine(CollIndex ix);

  int loadTrafMetadataInCache();
  int loadTrafMetadataIntoSharedCache();
  int loadTrafDataIntoSharedCache();

  void initGlobalNADefaultsEntries();

  ExSqlComp::ReturnStatus sendXnMsgToArkcmp(char *data, int dataSize, int xnMsgType, ComDiagsArea *&diagsArea);

  inline NABoolean grabMemoryQuotaIfAvailable(int size) {
    if (unusedBMOsMemoryQuota_ < size) return FALSE;
    unusedBMOsMemoryQuota_ -= size;
    return TRUE;
  }

  inline void resetMemoryQuota() { unusedBMOsMemoryQuota_ = 0; }

  inline int unusedMemoryQuota() { return unusedBMOsMemoryQuota_; }

  inline void yieldMemoryQuota(int size) { unusedBMOsMemoryQuota_ += size; }

  void collectSpecialRTStats();

  char *getSlaName() { return slaName_; }
  char *getProfileName() { return profileName_; }

  inline ExeCliInterface *getCliInterfaceTrigger() { return cliInterfaceTrigger_; }
  int prepareCallTrigger();
  inline int setTriggerParam(short entry, char *ptr, int len) {
    return cliInterfaceTrigger_->setInputValue(entry, ptr, len);
  }
  inline int setTriggerParam(short entry, char *ptr) { return cliInterfaceTrigger_->setInputValue(entry, ptr); }
  inline int setTriggerParam(short entry, int val) { return cliInterfaceTrigger_->setInputValue(entry, val); }
  int executeCallTrigger(char *tblName, int isBefore, int opType, char *oldSKVBuffer, char *oldBufSize, char *oldRowIDs,
                         char *oldRowIDsLen, int newRowIDLen, char *newRowIDs, int newRowIDsLen, char *newRows,
                         int newRowsLen, char *curExecSql);

 private:
  // The heap where executor 'stuff' will be allocated from.
  // exHeap_ must be first so that it's dtor is called last (Gil)
  NAHeap exHeap_;

  SQLCTX_HANDLE contextHandle_;

  CliGlobals *cliGlobals_;

  SessionDefaults *sessionDefaults_;

  HashQueue *bdrConfigInfoList_;

  // this class defined in file SessionDefaults.h
  //  AQRInfo * aqrInfo_;

  char *authID_;
  char *authToken_;
  NABoolean validAuthID_;
  SQLAUTHID_TYPE authIDType_;

  // External name, max 128 characters
  char *externalUsername_;

  // Database user name. On Linux the values come from the USER_NAME
  // column in the USERS table. Max 128 characters.
  char *databaseUserName_;

  // Tenant name. There is a many-to-many relationship between
  // database user names and tenant names.
  char *tenantName_;

  // Hostname, OS user name and ip address of client
  char *hostName_;
  char *osUserName_;
  char *ipAddress_;

  // The associated cgroup for the tenant
  ComCGroup tenantCGroup_;

  // List of active roles and their grantees for the databaseUser
  int numRoles_;
  int *roleIDs_;
  int *granteeIDs_;
  int roleLoopLevel_;
  NABoolean rewindRoleLevels_;

  NABoolean userNameChanged_;

  // queues(lists) to keep track of loaded modules, statements
  // and descriptors.
  HashQueue *moduleList_;
  HashQueue *statementList_;
  HashQueue *descriptorList_;
  HashQueue *cursorList_;
  // this is a subset of statementList_. It contains all statements in
  // state OPEN_ or EOF_
  HashQueue *openStatementList_;

  // list of non-audited tbat have been locked.
  HashQueue *nonAuditedLockedTableList_;

  // List of closed statements with ESPs assigned
  Queue *statementWithEspsList_;

  // List of CliDDLValidator objects that require an isDDLValid check
  // at commit time (to avoid inconsistencies due to concurrent DDL)
  Queue *validDDLCheckList_;

  // remember the last assigned descriptor handle
  ULong lastDescriptorHandle_;
  ULong lastStatementHandle_;

  int databaseUserID_;
  // With SPJ Definer Rights, we need to support different values for
  // SESSION_USER and CURRENT_USER/USER. SESSION_USER is the user that is
  // logged on to the current SQL session. CURRENT_USER is the one with
  // whose privileges a SQL statement is executed, With Definer Rights SPJ,
  // the CURRENT_USER is the owner of the SPJ while SESSION_USER is the
  // user who invoked the SPJ.
  int sessionUserID_;

  int tenantID_;
  NAWNodeSet *availableNodes_;
  char *slaName_;
  char *profileName_;
  char *tenantDefaultSchema_;
  // keeps track of current define context.
  unsigned short defineContext_;

  // flag and pointer to the embedded arkcmp context
  NABoolean isEmbeddedArkcmpInitialized_;
  CmpContext *embeddedArkcmpContext_;

  CmpContext *prevCmpContext_;

  // pointer to the array of server  versions used to communicate with ARKCMP.
  ARRAY(ExSqlComp *) arkcmpArray_;

  // Arkcmp context (CmpContext) switching related info
  // known CmpContext instance set to be used for different catagory
  ARRAY(CmpContextInfo *) cmpContextInfo_;
  // stack of CmpContexts in use except current one
  LIST(CmpContext *) cmpContextInUse_;

  // Array of failure modes for each arkcmp
  ARRAY(ArkcmpFailMode) arkcmpInitFailed_;
  short versionOfMxcmp_;
  char *mxcmpNodeName_;
  short indexIntoCompilerArray_;
  // the transaction object
  ExTransaction *transaction_;

  // A diagnostics area for this context
  ComDiagsArea diagsArea_;

  // Points to the statistics area of current statement
  // being executed. Used to return stat info for the
  // 'last' statement that was executed.
  ExStatisticsArea *stats_;

  // this area contains the defaults and other controls
  // specified by user using CONTROL QUERY statements.
  ExControlArea *controlArea_;

  // used by asynchronous CLI cancel feature to
  // synchronize the main thread and the cancel thread.
  CLISemaphore cancelSemaphore_;

  // parser flags to be sent to mxcmp
  int sqlParserFlags_;

  // Hold all the dynamicly set (in this process) timeout data
  TimeoutData *timeouts_;  // it is NULL if not used

  // logical timestamp; increment each time SET TIMEOUT executes
  // (We ignore counter overlap; this can only affect a statement that
  //  remained fixed up after 2^32 times of timeout-settings .... :-)
  UInt32 timeoutChangeCounter_;

  // A list of closed statements maintained in LRU order.
  // Used to determine aging statements for space deallocation.
  // Rules: (when there are enough allocated statements).
  // - A statement is added to the head of the list at close() time.
  // - A statement is removed from the list at open() and
  //   deallocate() time.
  // - Each statement on the list has a unique sequence > 0.
  //   A zero or negative sequence indicates the statement is not on the list.
  // - The sequence numbers indicate the sequence of closes of
  //   those statements.
  //
  // closeStatementList points to the most recently closed static statement,
  // which has the largest close sequence number.
  // nextReclaimStatement points to the statement most eligible for
  // space reclaim.  nextReclaimStatement will move toward the head of
  // the list as statements are reclaimed one by one.
  //
  Statement *closeStatementList_;
  Statement *nextReclaimStatement_;

  // Statistics data.
  // stmtReclaimCount_ is the number of statements on the closeStmtList_
  // for which space reclaim has been done.

  int closeStmtListSize_;
  int stmtReclaimCount_;
  int stmtReclaimMinPctFree_;  // Reclaim space when free is <= percent of total
  IpcTimeout espReleaseTimeout_;

  // currSequence_ is the sequence of the latest close of a statement.
  // prevFixupSequence_ is the sequence of the latest fixup.
  int currSequence_;
  int prevFixupSequence_;

  // a pointer containing information that catman
  // wants to persist across multiple calls to catman interface
  // from executor.
  // Its contents are unknown to cli or executor. It is allocated
  // and maintained by catman code.
  // Passed to CatManAnsiNameToGuardianName.
  void *catmanInfo_;

  // get prepare trigger
  ExeCliInterface *cliInterfaceTrigger_;
  bool skipCheckValidPrivs_;

  enum Flags {
    // If the stmt for which stats were collected is deallocated,
    // then a copy of it ExStatisticsArea is made and pointed to
    // by the stats_ field. This flag, if set, indicates that.
    // If this flag is not set, then stats_ points to the stats
    // area of the corresponding stmt.
    STATS_COPY_ = 0x0001,
    DELETE_MERGED_STATS_ = 0x0002,
    STATS_IN_SHARED_SEGMENT_ = 0x0004,
    // an inMemory object definition was created during this session.
    // Kill mxcmp at the end of this session.
    IN_MEMORY_OBJECT_DEFN = 0x0008
  };

  int flags_;

  // List of UDR servers servicing this context. Each list element is
  // an ExUdrServer pointer. There may be duplicates in the list. Each
  // element represents a reference on an ExUdrServer instance. A
  // context may own multiple references on a given ExUdrServer, one
  // for each fixed up CALL statement that has been executed within
  // the context. When a context is destroyed all ExUdrServer
  // references must be released by calling the ExUdrServerManager
  // interface.
  HashQueue *udrServerList_;

  // Strings to store UDR runtime options that have been set
  // dynamically by the calling application. These strings can only be
  // set via an internal CLI function and catman is the only caller of
  // that function.
  char *udrRuntimeOptions_;
  char *udrRuntimeOptionDelimiters_;
  Statement *pendingNoWaitPrepare_;
  ExProcessStats *processStats_;

  char *clientInfo_;

  inline HashQueue *moduleList() { return moduleList_; };
  inline HashQueue *statementList() { return statementList_; };
  inline HashQueue *descriptorList() { return descriptorList_; };
  inline HashQueue *cursorList() { return cursorList_; };
  inline HashQueue *openStatementList() { return openStatementList_; };

  // Used to reclaim space from a list of closed statements.
  void addToCloseStatementList(Statement *statement);

  // SPJ add on
  void issueCtrlStmts(IpcConstMessageBufferPtr ctrlStmtBuffer, int length);
  void internalPrepareExecuteFetchStmt(char *sqlText);
  UdrContextMsg *manufactureUdrContextMsg();

  // Locate a matching UDR server within this context
  ExUdrServer *findUdrServer(const char *runtimeOptions, const char *optionDelimiters, IpcThreadInfo *threadInfo,
                             NABoolean dedicated);

  // Release all ExUdrServer references currently held
  void releaseUdrServers();
  void startUdrServer();

  // Remove a child statement such as a clone or a stored procedure
  // result set proxy from the context's statement lists. Do not call
  // the child's destructor (that job is left to the parent
  // statement's destructor).
  RETCODE cleanupChildStmt(Statement *child);

  char *userSpecifiedSessionName_;

  // created whenever user begins a session.
  // Should never be NULL as we are always in a session.
  char *sessionID_;

  // list of volatile tables created in a session.
  HashQueue *volTabList_;

  HashQueue *hdfsHandleList_;
  SequenceValueGenerator *seqGen_;

  NABoolean sessionInUse_;
  NABoolean mxcmpSessionInUse_;
  NABoolean volatileSchemaCreated_;
  ExStatisticsArea *mergedStats_;
  StmtStats *prevStmtStats_;

  TriggerDB *triggerDB_;

  CLISemaphore *cliSemaphore_;
  int numCliCalls_;
  IpcEnvironment *env_;
  NAHeap *ipcHeap_;
  UInt32 eventConsumed_;
  NABoolean breakEnabled_;
  // ESP manager for the context
  ExEspManager *espManager_;
  ExeTraceInfo *exeTraceInfo_;
  // UDR server manager for this process
  ExUdrServerManager *udrServerManager_;
  // trusted routines in use locally in this context,
  // without going through udrserv
  ARRAY(LmRoutine *) trustedRoutines_;
  // SSMP manager for this context.
  ExSsmpManager *ssmpManager_;
  NABoolean dropInProgress_;

  // set to true if ddl stmts are issued.
  // Reset at begin and commit Xn. Used to do NATable invalidation
  // for ddl stmts issued within a transaction.
  NABoolean ddlStmtsExecuted_;
  NABoolean execDDLOptions_;

  // set to true if ddl stmts IN SAVEPOINT are issued.
  // Reset at begin and commit Xn. Used to do NATable invalidation
  // for ddl stmts issued within a transaction.
  NABoolean ddlStmtsInSPExecuted_;

  //
  //
  //  Fields to enforce SQL semantics inside the UDR server. These
  //  fields are checked by the UDR server after a user-defined routine
  //  returns control, to see if that routine violated any SQL
  //  semantics for UDRs. Some examples of violations include a NO SQL
  //  routine attempting to access SQL, and a routine attempting to
  //  issue SQL transaction statements such as BEGIN WORK.
  //  If the udrErrorChecksEnabled_ flag is FALSE then none of this UDR
  //  error checking is performed. The only SQL application that
  //  enables the UDR error checking is the UDR server.
  //
  NABoolean udrErrorChecksEnabled_;
  int udrSqlAccessMode_;
  NABoolean udrAccessModeViolation_;
  NABoolean udrXactViolation_;
  NABoolean udrXactAborted_;
  long udrXactId_;  // transid that UDR Server is interested to know
                    // if aborted
  NAString jniErrorStr_;
  DistributedLock_JNI *DLockClientJNI_;
  TenantHelper_JNI *tenantHelperJNI_;

  // this points to data used by trafSE (traf storage engine) that is context specific.
  // It points to a list of 'black box' of data allocated by user and is returned
  // back whenever this context is in use.
  HashQueue *trafSElist_;
  // memory quota allocation given back by BMOs to be used by other BMOs
  int unusedBMOsMemoryQuota_;
  pid_t tid_;
  NABoolean clientConnected_;

  // locally cached variable that tells us whether this is a tdm_arkcmp
  // process
  enum TdmArkcmp { UNKNOWN, IS_TDMARKCMP, IS_NOT_TDMARKCMP };

  TdmArkcmp isTdmArkcmp_;

  NABoolean deleteStats() { return (flags_ & DELETE_MERGED_STATS_) != 0; }

  void setDeleteStats(NABoolean v) { (v ? flags_ |= DELETE_MERGED_STATS_ : flags_ &= ~DELETE_MERGED_STATS_); }

  // An enumeration for different types of lookups into the AUTHS table
  //   USER BY USER NAME       -- Find the row matching a database user name
  //   USER BY USER ID         -- Find the row matching a database user ID
  //   USER BY EXTERNAL NAME   -- Find the row matching an external user name
  //   ROLE BY ROLE ID	       -- Find the row matching a database role ID
  //   AUTH BY NAME            -- Find the row matching the database name
  //   ROLES_QUERY_BY_AUTH_ID  -- Find the roles for a given auth ID
  //   TENANT_BY_NAME          -- Find the row matching a tenant name
  //   TENANT_BY_USER_ID       -- Find the row matching a tenant ID
  //   QUERY_FOR_EXTERNAL_NAME -- Find the row matching a tenant ID
  //   GROUP_QUERY_BY_GROUP_ID -  Find the group matching a group ID
  enum AuthQueryType {
    USERS_QUERY_BY_USER_NAME = 2,
    USERS_QUERY_BY_USER_ID,
    USERS_QUERY_BY_EXTERNAL_NAME,
    ROLE_QUERY_BY_ROLE_ID,
    AUTH_QUERY_BY_NAME,
    ROLES_QUERY_BY_AUTH_ID,
    TENANT_BY_NAME,
    TENANT_BY_USER_ID,
    QUERY_FOR_EXTERNAL_NAME,
    GROUP_QUERY_BY_GROUP_ID
  };

  // Private method to perform single-row lookups into the AUTHS
  // table or TENANTS table.
  // If a matching row is found, column values are returned in
  //
  // For query type USERS_QUERY_BY_USER_ID, the authID argument must
  // be provided.
  //
  // For query types USERS_QUERY_BY_USER_NAME and
  // USERS_QUERY_BY_EXTERNAL_NAME, the authName argument must be
  // provided.
  // For query types TENANT_BY_* , there is aan output param containing
  // tenant related info
  RETCODE authQuery(AuthQueryType queryType, const char *authName, int authID, char *authNameFromTable,
                    int authNameMaxLen, int &authIDFromTable,
                    TenantInfo *tenantInfo = NULL);  // tenantInfo needs to be allocated by the caller

  RETCODE storeName(const char *src, char *dest, int maxLength);

  RETCODE storeName(const char *src, char *dest, int maxLength, int &actualLength);

  int addUserInfo(const char *username, USERS_INFO *usersInfo, struct SQLSEC_AuthDetails *authDetails,
                  CmpSeabaseDDLauth &authInfo);

  int addTenantInfo(const char *tenant_name, USERS_INFO *users_info, struct SQLSEC_AuthDetails *authDetails,
                    CmpSeabaseDDLauth &authInfo);

 public:
  ContextCli(CliGlobals *cliGlobals, NABoolean isDefaultContext = FALSE);
  ~ContextCli();
  void deleteMe();
  inline TriggerDB *getTriggerDB() { return triggerDB_; }
  inline UInt32 *getEventConsumed() { return &eventConsumed_; }
  inline IpcEnvironment *getEnvironment() { return env_; }
  inline ExEspManager *getEspManager() { return espManager_; }
  inline ExSsmpManager *getSsmpManager() { return ssmpManager_; }
  inline ExeTraceInfo *getExeTraceInfo() { return exeTraceInfo_; }
  NAHeap *getIpcHeap() { return ipcHeap_; }
  inline ExUdrServerManager *getUdrServerManager() { return udrServerManager_; }
  //
  // Accessor and mutator functions for UDR error checking. Notes on
  // UDR error checking appear below with the data member
  // declarations.
  //
  NABoolean getUdrErrorChecksEnabled() { return udrErrorChecksEnabled_; }
  int getUdrSQLAccessMode() { return udrSqlAccessMode_; }
  NABoolean getUdrAccessModeViolation() { return udrAccessModeViolation_; }
  NABoolean getUdrXactViolation() { return udrXactViolation_; }
  NABoolean getUdrXactAborted() { return udrXactAborted_; }

  void setUdrErrorChecksEnabled(NABoolean b) { udrErrorChecksEnabled_ = b; }
  void setUdrSQLAccessMode(int mode) { udrSqlAccessMode_ = mode; }
  void setUdrAccessModeViolation(NABoolean b) { udrAccessModeViolation_ = b; }
  void setUdrXactViolation(NABoolean b) { udrXactViolation_ = b; }
  void setUdrXactAborted(long currTransId, NABoolean b);
  //
  // A few useful wrapper functions to ease management of the UDR
  // error checking fields
  //
  void clearUdrErrorFlags();

  NABoolean sqlAccessAllowed() const;

  void getUdrErrorFlags(NABoolean &sqlViolation, NABoolean &xactViolation, NABoolean &xactAborted) const {
    sqlViolation = udrAccessModeViolation_;
    xactViolation = udrXactViolation_;
    xactAborted = udrXactAborted_;
  }

  inline CLISemaphore *getSemaphore() { return cliSemaphore_; }
  inline int incrNumOfCliCalls() { return ++numCliCalls_; }
  inline int decrNumOfCliCalls() {
    if (numCliCalls_ > 0)
      return --numCliCalls_;
    else
      return numCliCalls_;
  }
  inline int getNumOfCliCalls() { return numCliCalls_; }
  int initializeSessionDefaults();
  int initializeExeBDRConfigInfo(char *mgbltyCatName, ComDiagsArea *diagsArea);

  short moduleAdded(const SQLMODULE_ID *module_name);

  RETCODE allocateDesc(SQLDESC_ID *desc_id, int max_entries);
  RETCODE deallocDesc(SQLDESC_ID *desc_id, NABoolean deallocStaticDesc);
  Descriptor *getDescriptor(SQLDESC_ID *desc_id);

  RETCODE allocateStmt(SQLSTMT_ID *stmt_id, Statement::StatementType stmt_type = Statement::DYNAMIC_STMT,
                       SQLSTMT_ID *cloned_stmt_id = NULL);

  RETCODE deallocStmt(SQLSTMT_ID *stmt_id, NABoolean deallocStaticStmt);

  Statement *getStatement(SQLSTMT_ID *statement_id, HashQueue *stmtList = NULL);

  Statement *getNextStatement(SQLSTMT_ID *statement_id, NABoolean position, NABoolean advance);

  // Upgrade both argument types for multi charset module names
  void removeCursor(SQLSTMT_ID *cursorName, const SQLMODULE_ID *moduleName);

  void addToCursorList(Statement &s);

  void addToOpenStatementList(SQLSTMT_ID *statement_id, Statement *statement);
  void removeFromOpenStatementList(SQLSTMT_ID *statement_id);

  short commitTransaction();
  short releaseAllTransactionalRequests();

  void closeAllCursors(enum CloseCursorType, enum closeTransactionType transType, const long executorXnId = 0,
                       NABoolean inRollback = FALSE);
  void checkIfNeedToClose(Statement *stmt, enum CloseCursorType type, NABoolean &closeStmt, NABoolean &releaseStmt);
  void clearAllTransactionInfoInClosedStmt(long transid);
  void closeAllCursorsFor(SQLSTMT_ID *statement_id);

  inline NAHeap *exHeap() { return &exHeap_; }
  inline CollHeap *exCollHeap() { return &exHeap_; }
  inline HashQueue *getStatementList() const;
  inline HashQueue *getCursorList() const;
  inline HashQueue *getOpenStatementList() const;
  inline HashQueue *getNonAuditedLockedTableList() { return nonAuditedLockedTableList_; }

  inline ULong getNextDescriptorHandle() { return lastDescriptorHandle_++; }
  inline ULong getNextStatementHandle() { return ++lastStatementHandle_; }

#ifdef _DEBUG
  // dumps a list of all statements with info about allocated memory
  // for debugging purposes only
  void dumpStatementInfo();
#endif

  void dumpStatementAndGlobalInfo(ostream *outstream);

  // Returns a reference to the ComDiagsArea, diagsArea_, of this context.
  ComDiagsArea &diags() { return diagsArea_; }

  ComDiagsArea *getDiagsArea() { return &diagsArea_; }

  ExStatisticsArea *getStats() { return stats_; }

  void setStatsArea(ExStatisticsArea *stats, NABoolean statsCopy = FALSE, NABoolean inSharedSegment = TRUE,
                    NABoolean getSemaphore = TRUE);

  ExControlArea *getControlArea() { return controlArea_; }

  // return the TimeoutData field of the context (if it is NULL --allocate it)
  // (If allocate == FALSE, just return it as is, even if it is NULL)
  TimeoutData *getTimeouts(NABoolean allocate = TRUE);

  void clearTimeoutData();  // deallocate the TimeoutData

  // make these functions non-inline on NT and inline on NSK.
  // These methods are called from code in executor directory
  // which creates a problem when linking for arkesp since arkesp
  // does not link in with cli code.
  // By making them non-inline on nt, a stub could be added.
  // This problem does not show up on nsk since inline functions
  // are really inline out there.
  void incrementTimeoutChangeCounter();
  UInt32 getTimeoutChangeCounter();

  void *&catmanInfo() { return catmanInfo_; }

  ////////////////////////////////////////////////////
  // Used to communicate with the embedded ARKCMP.
  ////////////////////////////////////////////////////

  NABoolean isEmbeddedArkcmpInitialized() { return isEmbeddedArkcmpInitialized_; }
  void setEmbeddedArkcmpIsInitialized(NABoolean val) { isEmbeddedArkcmpInitialized_ = val; }
  CmpContext *getEmbeddedArkcmpContext() { return embeddedArkcmpContext_; }
  void setEmbeddedArkcmpContext(CmpContext *cntx) { embeddedArkcmpContext_ = cntx; }

  NABoolean initEmbeddedArkcmp(NABoolean executeInESP = FALSE);

  ////////////////////////////////////////////////////
  // Used to communicate with the ARKCMP process.
  ////////////////////////////////////////////////////

  void setArkcmp(ExSqlComp *es) {
    short index = arkcmpArray_.entries();
    arkcmpArray_.insertAt(index, es);
  }
  void setArkcmpFailMode(ContextCli::ArkcmpFailMode afm) {
    short index = arkcmpInitFailed_.entries();
    arkcmpInitFailed_.insertAt(index, afm);
  }
  ExSqlComp *getArkcmp(short index = 0) { return arkcmpArray_[index]; }
  ArkcmpFailMode &arkcmpInitFailed(short index = 0) { return (ContextCli::ArkcmpFailMode &)arkcmpInitFailed_[index]; }

  short getNumArkcmps() { return arkcmpArray_.entries(); }
  int setOrStartCompiler(short mxcmpVersionToUse, char *remoteNodeName, short &indexIntoCompilerArray);

  inline short getVersionOfCompiler() { return versionOfMxcmp_; }
  inline void setVersionOfMxcmp(short version) { versionOfMxcmp_ = version; }
  inline void setIndexToCompilerArray(short index) { indexIntoCompilerArray_ = index; }
  inline short getIndexToCompilerArray() { return indexIntoCompilerArray_; }
  inline const char *getMxcmpNodeName() { return mxcmpNodeName_; }
  inline void setMxcmpNodeName(char *mxcmpNodeName) { mxcmpNodeName_ = mxcmpNodeName; }

  inline NABoolean sendSettingsToCompiler(const short index) const
  // Send controls if current compiler is the default compiler, or if caller asks
  // for the current compiler
  {
    return ((indexIntoCompilerArray_ == 0) || (indexIntoCompilerArray_ == index));
  };

  // Methods to switch to different CmpContext
  int switchToCmpContext(int cmpCntxtType);
  int switchToCmpContext(void *cmpCntxt);
  int switchBackCmpContext(void);
  void copyDiagsAreaToPrevCmpContext();
  NABoolean isDropInProgress() { return dropInProgress_; }
  void setDropInProgress() { dropInProgress_ = TRUE; };

  void buildRuntimeOptions(NAString &result, const char *options, const char *delimiters);

  // Method to acquire a reference to a UDR server associated with
  // a given set of JVM startup options
  ExUdrServer *acquireUdrServer(const char *runtimeOptions, const char *optionDelimiters, IpcThreadInfo *threadInfo,
                                NABoolean dedicated = FALSE);

  // Here are methods to manage the UDR runtime option strings that
  // can be set dynamically by the calling application. These strings
  // can only be set via an internal CLI function and catman is the
  // only caller of that function.
  const char *getUdrRuntimeOptions() const { return udrRuntimeOptions_; }
  const char *getUdrRuntimeOptionDelimiters() const { return udrRuntimeOptionDelimiters_; }
  int setUdrRuntimeOptions(const char *options, int optionsLen, const char *delimiters, int delimsLen);

  ExTransaction *getTransaction() { return transaction_; }

  void setClientInfo(const char *value, int length, const char *characterset);

  char *getClientInfo() { return clientInfo_; }

  NABoolean &ddlStmtsExecuted() { return ddlStmtsExecuted_; }
  NABoolean &ddlStmtsInSPExecuted() { return ddlStmtsInSPExecuted_; }
  NABoolean &execDDLOptions() { return execDDLOptions_; }

  int setAuthID(const USERS_INFO &usersInfo, const char *authToken, int authTokenLen, const char *slaName,
                const char *profileName, int resetAttributes);

  int completeSetAuthID(const USERS_INFO &usersInfo, const char *authToken, int authTokenLen, const char *slaName,
                        const char *profileName, bool eraseCQDs, bool releaseUDRServers, bool dropVolatileSchema,
                        bool resetCQDs, int resetAttributes);

  char *getAuthID() {
    if (authIDType_ == SQLAUTHID_TYPE_ASCII_USERROLE || authIDType_ == SQLAUTHID_TYPE_ASCII_USERNAME)
      return authID_;
    else
      return NULL;
  }

  int boundsCheckMemory(void *startAddress, int length);

  inline SQLCTX_HANDLE getContextHandle() const { return contextHandle_; }

  void reset(void *contextMsg);

  void closeAllStatementsAndCursors();  // Close all statements

  void retrieveContextInfo(void *contextMsg);

  inline int getSqlParserFlags() const { return sqlParserFlags_; }
  inline void setSqlParserFlags(int flagbits) { sqlParserFlags_ |= flagbits; }
  inline void resetSqlParserFlags(int flagbits) { sqlParserFlags_ &= ~flagbits; }
  inline void assignSqlParserFlags(int flagbits) { sqlParserFlags_ = flagbits; }

  inline void semaphoreLock() { cancelSemaphore_.get(); }
  inline void semaphoreRelease() { cancelSemaphore_.release(); }

  void removeFromCloseStatementList(Statement *statement, NABoolean forOpen);

  // Reclaim statements if it is available
  NABoolean reclaimStatements();

  // Dealocate statement space based on least recently used rule.
  NABoolean reclaimStatementSpace();

  // returns the current DEFINE context.
  unsigned short getCurrentDefineContext();

  unsigned short &defineContext() { return defineContext_; };

  // returns TRUE, if define context has changed.
  // Sets the current define context.
  NABoolean checkAndSetCurrentDefineContext();

  inline CliGlobals *getCliGlobals(void) { return cliGlobals_; };

  NABoolean statsCopy() { return (flags_ & STATS_COPY_) != 0; }
  void setStatsCopy(NABoolean v) { (v ? flags_ |= STATS_COPY_ : flags_ &= ~STATS_COPY_); }

  NABoolean statsInSharedSegment() { return (flags_ & STATS_IN_SHARED_SEGMENT_) != 0; }
  void setStatsInSharedSegment(NABoolean v) {
    (v ? flags_ |= STATS_IN_SHARED_SEGMENT_ : flags_ &= ~STATS_IN_SHARED_SEGMENT_);
  }

  inline Statement *pendingNoWaitPrepare() const { return pendingNoWaitPrepare_; }
  NABoolean inMemoryObjectDefn() { return (flags_ & IN_MEMORY_OBJECT_DEFN) != 0; }
  void setInMemoryObjectDefn(NABoolean v) { (v ? flags_ |= IN_MEMORY_OBJECT_DEFN : flags_ &= ~IN_MEMORY_OBJECT_DEFN); }

  inline void setPendingNoWaitPrepare(Statement *statement) { pendingNoWaitPrepare_ = statement; };

  inline void resetPendingNoWaitPrepare() { pendingNoWaitPrepare_ = NULL; };
  void logReclaimEvent(int freeSize, int totalSize);

  SessionDefaults *getSessionDefaults() { return sessionDefaults_; }
  HashQueue *getBDRConfigInfoList() { return bdrConfigInfoList_; }
  void resetBDRConfigInfoList();

  AQRInfo *aqrInfo() { return (sessionDefaults_ ? sessionDefaults_->aqrInfo() : NULL); }

  void addToStatementWithEspsList(Statement *statement);
  void removeFromStatementWithEspsList(Statement *);
  bool unassignEsps(bool force = false);

  void addToValidDDLCheckList(SqlTableOpenInfoPtr *stoiList, int tableCount);
  bool doValidDDLChecks(NABoolean inRollback);
  short ddlResetObjectEpochs(ComDiagsArea *diags);

  void beginSession(char *userSpecifiedSessionName);
  int reduceEsps();
  void endSession(NABoolean cleanupEsps = FALSE, NABoolean cleanupEspsOnly = FALSE, NABoolean cleanupOpens = FALSE,
                  NABoolean cleanupCmpContext = FALSE);
  void dropSession(NABoolean clearCmpCache = FALSE);
  void endMxcmpSession(NABoolean cleanupEsps, NABoolean clearCmpCache = FALSE);
  void resetAttributes();

  void createMxcmpSession();
  int updateMxcmpSession();

  void resetVolatileSchemaState();

  void closeAllTables();

  char *getSessionId() { return sessionID_; }
  void setSessionId(char *si);
  void genSessionId();

  NABoolean sessionInUse() { return sessionInUse_; }
  NABoolean mxcmpSessionInUse() { return mxcmpSessionInUse_; }

  NABoolean volatileSchemaCreated() { return volatileSchemaCreated_; }
  void setVolatileSchemaCreated(NABoolean v) { volatileSchemaCreated_ = v; }

  HashQueue *getVolTabList() { return volTabList_; }
  void initVolTabList();
  void resetVolTabList();

  void killIdleMxcmp();
  void killAndRecreateMxcmp();

  ExStatisticsArea *getObjectEpochStats(const char *objectName, int objectNameLen, short cpu, bool locked,
                                        short &retryAttempts);

  ExStatisticsArea *getObjectLockStats(const char *objectName, int objectNameLen, short cpu, short &retryAttempts);

  int performObjectLockRequest(const char *objectName, ComObjectType objectType, ObjectLockRequest::OpType opType,
                               int nid, int pid, int maxRetries, int delay);

  int performObjectLockRequest(const char *objectName, ComObjectType objectType, ObjectLockRequest::OpType opType,
                               int nid, int pid, bool &conflictLock, int &conflictNid, int &conflictPid,
                               ComDiagsArea *tempDiagsArea);

  int lockObjectDDL(const char *objectName, ComObjectType objectType);

  int unlockObjectDDL(const char *objectName, ComObjectType objectType);

  int unlockObjectDDL(const char *objectName, ComObjectType objectType, int nid, int pid, bool all = false);

  bool cleanupStaleLockHolder(const LockHolder &ddlHolder);

  int lockObjectDML(const char *objectName, ComObjectType objectType);

  int releaseAllDDLObjectLocks();
  int releaseAllDMLObjectLocks();

#ifdef _DEBUG
 public:
  void StmtListPrintf(const Statement *s, const char *formatString, ...) const;
#endif
  ExStatisticsArea *getMergedStats() { return mergedStats_; }

  void setMergedStats(ExStatisticsArea *stats) { mergedStats_ = stats; }
  ExStatisticsArea *getMergedStats(
      /* IN */ short statsReqType,
      /* IN */ char *statsReqStr,
      /* IN */ int statsReqStrLen,
      /* IN */ short activeQueryNum,
      /* IN */ short statsMergeType,
      /*IN/out */ short &retryAttempts);

  int GetStatistics2(
      /* IN */ short statsReqType,
      /* IN */ char *statsReqStr,
      /* IN */ int statsReqStrLen,
      /* IN */ short activeQueryNum,
      /* IN */ short statsMergeType,
      /* OUT */ short *statsCollectType,
      /* IN/OUT */ SQLSTATS_DESC sqlstats_desc[],
      /* IN */ int max_stats_desc,
      /* OUT */ int *no_returned_stats_desc);
  StmtStats *getPrevStmtStats() { return prevStmtStats_; }

  void setPrevStmtStats(StmtStats *ss) { prevStmtStats_ = ss; }

  int setSecInvalidKeys(
      /* IN */ int numSiKeys,
      /* IN */ SQL_QIKEY siKeys[]);

  int setObjectEpochEntry(
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
      /* OUT */ UInt32 *maxExpectedFlagsFound);

  int checkLobLock(char *inLobLockId, NABoolean *found);

  int holdAndSetCQD(const char *defaultName, const char *defaultValue);
  int restoreCQD(const char *defaultName);

  int setCS(const char *csName, char *csValue);
  int resetCS(const char *csName);
  inline IpcTimeout getEspReleaseTimeout() const { return espReleaseTimeout_; }

  // Private method to initialize the context's user identity based on
  // OS user identity. Should be called before any query processing
  // begins. This step requires a fully initialized CliGlobals so
  // should not be called before CliGlobals initialization completes.
  void initializeUserInfoFromOS();
  hdfsFS getHdfsServerConnection(char *hdfsServer, int port, hdfsConnectStruct **entryPtr = NULL);
  void disconnectHdfsConnections();

  void setSkipCheckValidPrivs(bool v) { skipCheckValidPrivs_ = v; }
  bool getSkipCheckValidPrivs() { return skipCheckValidPrivs_; }
};  // class ContextCli

/* ContextTidMap - Maps contextCli to  a thread id
 * 1. Only one mapping entry is possible for a given thread id.
 * 2. A thread can work on only one ContextCli object at a time.
 * 3. Multiple threads in the same process can work on the same ContextCli.
 * `
 */

class ContextTidMap {
 public:
  ContextTidMap(pid_t tid, ContextCli *context) {
    tid_ = tid;
    context_ = context;
  }

 public:
  pid_t tid_;
  ContextCli *context_;
};

inline HashQueue *ContextCli::getStatementList() const { return statementList_; }

inline HashQueue *ContextCli::getCursorList() const { return cursorList_; }

inline HashQueue *ContextCli::getOpenStatementList() const { return openStatementList_; }

struct hdfsConnectStruct {
  hdfsFS hdfsHandle_;
  int hdfsPort_;
  char hdfsServer_[256];  // max length determined by dfs.namenode.fs-limits.max-component-length(255)
};

#endif

int parse_statsReq(short statsReqType, char *statsReqPtr, int statsReqStrLen, char *nodeName, short &cpu, pid_t &pid,
                   long &timeStamp, int &queryNumber);
