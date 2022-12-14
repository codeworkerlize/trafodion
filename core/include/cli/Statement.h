
#ifndef STATEMENT_H
#define STATEMENT_H

/* -*-C++-*-
 *****************************************************************************
 *
 * File:         Statement.h
 * Description:
 *
 *
 * Created:      7/10/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

// -----------------------------------------------------------------------

#include <sys/types.h>
//#include <sys/time.h>
//#include <fstream>
#include "cli/Descriptor.h"
#include "cli/Module.h"
#include "cli/SQLCLIdev.h"
#include "comexe/ComQueue.h"
#include "comexe/SqlTableOpenInfo.h"  // triggers
#include "common/ComSmallDefs.h"
#include "common/DLock.h"
#include "common/NAMemory.h"
#include "export/ComDiags.h"

class ex_root_tdb;
class ex_root_tcb;
class ExMasterStmtGlobals;
class ExStatisticsArea;
class LateNameInfo;
class LateNameInfoList;
class SqlTableOpenInfo;
class QuerySimilarityInfo;
class SimilarityInfo;
class TriggerStatusWA;
class CliGlobals;
class ContextCli;
class FileNumberManager;
class UdrSecurityInfo;
class StmtStats;
class ExRsInfo;
class AQRStatementInfo;
class StrTarget;

class Statement;

////////////////////////////////////////////////////////////////////////
// this class stores statement information that is needed on each cli
// call. Things like the internal module, Statement, input and output
// descriptor, etc. The 'handle' field of STATEMENT_ID class points to
// this. Used only if the name_mode is not 'handle'. Also, used for
// embedded, static, preprocessor generated SQL calls.
/////////////////////////////////////////////////////////////////////////
class StatementInfo : public ExGod {
 public:
  StatementInfo();
  ~StatementInfo();
  Statement *&statement() { return statement_; }
  Descriptor *&inputDesc() { return inputDesc_; }
  Descriptor *&outputDesc() { return outputDesc_; }
  int hashValue() { return hashValue_; }

  NABoolean moduleAdded() { return (flags_ & MODULE_ADDED) != 0; };
  void setModuleAdded(NABoolean v) { (v ? flags_ |= MODULE_ADDED : flags_ &= ~MODULE_ADDED); };

 private:
  enum { MODULE_ADDED = 0x0001 };

  Statement *statement_;
  Descriptor *inputDesc_;
  Descriptor *outputDesc_;
  int hashValue_;
  int flags_;
};

class Statement : public ExGod {
 public:
#define RTMD_HEAP_STR    "RTMD heap"
#define RTMD_BLOCKSIZE   2048
#define RTMD_UPPER_LIMIT 0

  enum StatementType { STATIC_STMT, DYNAMIC_STMT };

  enum State {
    INITIAL_,
    OPEN_,
    EOF_,
    CLOSE_,
    DEALLOCATED_,
    FETCH_,
    CLOSE_TABLES_,
    PREPARE_,
    PROCESS_ENDED_,
    RELEASE_TRANS_,
    SUSPENDED_,     // A pseudo-state used by RMS.
    STMT_EXECUTE_,  // A pseduo-state to denote query started executing
    STMT_FIXUP_,    // A pseduo-state to denote query has entered into fixup state in RMS
  };

  enum {
    FIXUP_BIT = 0x0001,            // indicates if the plan has been fixed up.
    RESECURE_NEEDED_BIT = 0x0002,  // indicates that plan needs to be resecured
    CLONED_BIT = 0x0004,           // indicates that this statement is a 'clone' of
                                   // another statement, root_tdb is shared with
                                   // that original statement and should not be
                                   // deleted by the 'clone'

    // if this was an updatable/deletable cursor
    // then this bit is reset at runtime when a row is fetched.
    // It is set, if that row is later deleted using delete...where
    // current of statement. Used to return an error if a deleted
    // row is deleted or updated again without another fetch.
    DELETED_CURSOR_BIT = 0x0008,

    // this is set the first time a statement is compiled or if a
    // statement is re-compiled at runtime.for input_desc
    COMPUTE_INPUTDESC_BULKMOVE_INFO = 0x0010,

    // For certain embedded static queries that are executed by calling
    // the Cli method, ClearExecFetchClose, a fast search for the statement
    // is done. The statement_id passed in by application that does not
    // use a handle, is made to point to this Statement. This makes it
    // easier and faster to search for that statement. Also, since these
    // statements are one-shot stmts (completed within one cli call), we
    // do not add them to open statement list.
    OLT_OPT = 0x0020,

    // set the first time a timestamp mismatch is detected. Used to
    // detect the case if a statement, after recompile and refixup
    // due to a timestamp mismatch, returns another timestamp mismatch.
    // This is treated as an error case.
    TS_MISMATCHED = 0x0040,

    // set at runtime after resolving the names for the first time.
    FIRST_RESOLVE_DONE = 0x0080,

    // for static statements, set if recomp warnings are to be returned.
    RECOMP_WARN = 0x0100,

    // if autocommit is on, and a transaction is started for this
    // statement, then this flag is set.
    // The transaction is only committed at the end of a statement
    // execution if this flag is set for that statement.
    AUTOCOMMIT_XN = 0x0200,

    // set, if odbc_process was on at static compilation time.
    // Used to send this info to mxcmp at auto recompilation time.
    ODBC_PROCESS = 0x0800,

    // this is set the first time a statement is compiled or if a
    // statement is re-compiled at runtime.for OUTPUT_DESC
    COMPUTE_OUTPUTDESC_BULKMOVE_INFO = 0x1000,

    // This is set when a module is added to indicate that it is a system
    // module statement. This is passsed to CmpCompileInfo to indicate to the
    // compiler that we are compiling a system module statement.
    SYSTEM_MODULE_STMT = 0x2000,

    // this statement need to be reprepared before executing it.
    // See method SQLCLI_RetryValidateDescs for details.
    AQR_REPREPARE_NEEDED = 0x4000,

    // if autocommit savepoint is on, and a savepoint is started for this
    // statement, then this flag is set.
    // The savepoint is only committed at the end of a statement
    // execution if this flag is set for that statement.
    AUTOCOMMIT_SAVEPOINT = 0x8000,

    // Synchronous statement for binglog
    SYNC_QUERY_FOR_BINGLOG = 0x10000,
  };

  enum RmsLimitLevel { NO_LIMIT = 0, LIMIT_DETAILED_STATISTICS = 1, LIMIT_RMS_USAGE_THOROUGHLY = 2 };

  enum AtomicityType { UNSPECIFIED_ = 0, ATOMIC_ = 1, NOT_ATOMIC_ = 2 };

  enum ExecState {
    INITIAL_STATE_,
    DO_SIM_CHECK_,
    CHECK_DYNAMIC_SETTINGS_,
    VALIDATE_SECURITY_,
    FIXUP_,
    EXECUTE_,
    ERROR_,
    ERROR_RETURN_,
    FIXUP_DONE_,
    FIXUP_DONE_START_XN_,
    RE_EXECUTE_
  };

  static const char *stmtState(State state);

  bool isOpen();

 private:
  StatementType stmt_type;
  State stmt_state;

  SQLSTMT_ID *statement_id;

  SQLSTMT_ID *cursor_name_;

  char *source_str;
  int source_length;  // octet length of source_str
  int charset_;

  // name of cat.sch when this query was initially compiled.
  // Used at recomp time.
  char *schemaName_;
  int schemaNameLength_;

  ex_root_tdb *root_tdb;
  ex_root_tcb *root_tcb;

  int root_tdb_size;

  CliGlobals *cliGlobals_;
  ContextCli *context_;
  Module *module_;

  ExMasterStmtGlobals *statementGlobals_;

  Descriptor *default_input_desc;

  Descriptor *default_output_desc;

  int flags_;

  Queue *clonedStatements;  // List of statements cloned from this statement.
                            // Will be empty for a clone.

  Statement *clonedFrom_;  // Valid only for a clone. Statement from which
                           // this statement was cloned.

  Statement *parentCall_;  // Valid only for a stored procedure result
                           // set. The CALL statement that produced
                           // this result set.

  Space *spaceObject_;
  Space &space_;
  NAHeap heap_;

  // This contains a space object which might be used during unpacking to
  // allocate additional space for potential upgrading of objects in the
  // plan. This space is derived from heap_, and therefore goes away with
  // it when the statement is destroyed.
  //
  Space *unpackSpace_;

  // valid if this is an 'update where current of cursor' query.
  // Points to the referenced cursor statement.
  Statement *currentOfCursorStatement_;

  LateNameInfoList *lnil_;
  char *inputData_;
  int inputDatalen_;

  ExecState state_;

  // keeps track of current define context.
  unsigned short defineContext_;

  // keeps track of current envvars context.
  long envvarsContext_;

  // Double linked list of close statements eligible for space reclaim.
  Statement *prevCloseStatement_;
  Statement *nextCloseStatement_;

  // Sequence number assigned at close.
  // Used to measure the age of the statement after close.
  int closeSequence_;

  SQLATTRHOLDABLE_INTERNAL_TYPE holdable_;

  // holds max length of input parameter arrays for dynamic statements, prior to compilation.
  // all input parameters are arrays of this maximum length.
  // set only by ODBC through CLI call as of Release 1.5.
  int inputArrayMaxsize_;

  // Used to denote that a rowset insert statement is NOT ATOMIC by ODBC
  // can take three values UNSPECIFIED_, ATOMIC_, or NOT_ATOMIC_
  AtomicityType rowsetAtomicity_;

  // Number of nonfatal errors that will be tolerated by a NOT ATOMIC rowset insert.
  // used by ODBC
  int notAtomicFailureLimit_;

  // statement index in a module, to identify Measure statement counters.
  int statementIndex_;

  // The following supports implicit transactions which are started and
  // committed in the scope of Statement::execute when the statement would
  // otherwise run without a transaction.  Such transactions are needed to
  // support recursive calls into the CLI to do CatMapAnsiNameToGuardianName,
  // Catalog Visibility checks, RTMD fetches, etc.
  // What this variable means is that if there is any active transaction,
  // then that transaction one of these implcit transactions and
  // was started by the Statement class.
  NABoolean anyTransWasStartedByMe_;

  // The following supports temporarily clearing the autocommit setting if
  // needed, so that recursive alls into the CLI will not commit the
  // transaction when they finish.  These recursive calls are the ones that
  // support CatMapAnsiNameToGuardianName, Catalog Visibility checks,
  // RTMD fetches, etc.
  NABoolean autoCommitCleared_;
  // The following supports temporarily clearing the Tmode setting if
  // needed, so that recursive calls into the CLI will not overwrite the
  // transaction when they finish.  These recursive calls are the ones that
  // support CatMapAnsiNameToGuardianName, Catalog Visibility checks,
  // RTMD fetches, etc.
  Int16 savedRoVal_;
  Int16 savedRbVal_;
  int savedAiVal_;

  // Support for UDR security/runtime re-compilation
  LIST(UdrSecurityInfo *) * udrSecurity_;

  // the following fields support no-wait operations
  NABoolean noWaitOpEnabled_;  // just a cache; same info is in FileNumber object
  NABoolean noWaitOpPending_;
  NABoolean noWaitOpIncomplete_;
  NABoolean standaloneStatement_;  // this statement is part of an ExecDirect ststemant . It is not an explicitly
                                   // prepared user statement. This is used during AQR
  NABoolean wmsMonitorQuery_;
  int tasks_;
  int fileNumber_;

  // the following fields are used to hold the defaults related information
  // which is used at auto recomp time. They are currently no looked at
  // by cli. This info is created during static compilation and stored
  // in rtdu(see cli/rtdu.h). It is then shipped back to mxcmp during
  // auto recomp(see Statement::prepare method).
  int recompControlInfoLen_;
  char *recompControlInfo_;

  char *uniqueStmtId_;
  int uniqueStmtIdLen_;

  AQRStatementInfo *aqrStmtInfo_;

  StmtStats *stmtStats_;
  // this is the cli level where this statement was used.
  // For top level cli calls, it will be 1. Gets incremented for
  // recursive cli calls.
  // Initialized to cliGlobals->numCliCalls at Statement constructor time.
  // Is currently used during closeAllCursors to only close those
  // cursors/statements which were instantiated at the same level where
  // the closeAllCursors call is being issued from.
  // Prevents closing of cursors/statements in parent's (or child's scope).
  int cliLevel_;
  StatementInfo *stmtInfo_;

  // VO, Plan Versioning Support.
  // The following two fields are used to control the resetting of current compiler
  // when preparing a query. Initialise to COM_VERS_UNKNOWN at construction time.
  // The associated prepareReturn method resets the fields on exit from ::prepare,
  // based upon the retcode value.
  short versionOnEntry_;  // Version of current compiler when prepare was called
  short versionToUse_;    // Version of compiler that will do the prepare
  RETCODE prepareReturn(const RETCODE retcode);

  // VO, Plan Versioning Support.
  // The following error information is used when a plan versioning error is detected which may
  // be reported later. The error code fields are initialised to VERSION_NO_ERROR, the remaining
  // fields need no initialisation; access to them are controlled by the error code fields.
  VersionErrorCode fetchErrorCode_;
  COM_VERSION fetchPlanVersion_;
  COM_VERSION fetchSupportedVersion_;
  ComNodeName fetchNode_;

  VersionErrorCode mxcmpErrorCode_;
  COM_VERSION mxcmpStartedVersion_;

  // For a parallel extract producer query. This is the template for
  // consumer query text strings
  char *extractConsumerQueryTemplate_;
  char *parentQid_;
  char parentQidSystem_[25];
  // StatsArea to return master stats when the statement is not yet fixed up
  ExStatisticsArea *compileStatsArea_;
  long compileEndTime_;  // In case there are no statistics.

  char *childQueryId_;
  int childQueryIdLen_;
  SQL_QUERY_COST_INFO *childQueryCostInfo_;
  SQL_QUERY_COMPILER_STATS_INFO *childQueryCompStatsInfo_;
  long aqrInitialExeStartTime_;
  bool multiThreadedEsp_;
  NABoolean skipEndValidDDLCheck_;
  char utf8sql_[TRIGGER_ID_LEN];
  bool triggerExecErr_;

  // A pointer to a dlock.  When we call Statement::execute()
  // to start an execution, we create a new WaitedLockController
  // object to lock the dlock (exclusive update to all shared
  // cache instances). When we reach the end of execution in
  // Statement::fetch() (setState EOF_), we delete this dlock
  // object to unlock.
  WaitedLockController *dlockForSharedCache_;

  RmsLimitLevel rmsLimit_;

  // Private Functions
  void buildConsumerQueryTemplate();

  // returns true, if plan has been fixed up. 0, otherwise.
  inline short fixupState();
  inline void setFixupState(short state);

  // TRUE, if this statement is using a auto commit savepoint.
  NABoolean savepointUsed();

  // starts a transaction, if one has not already been started
  short beginTransaction(ComDiagsArea &diagsArea);

  // ends(commits) transaction, if one is running and auto commit is on.
  short commitTransaction(ComDiagsArea &diagsArea);

  // aborts(rollbacks) transaction, if one is running and auto commit is on.
  // If doXnRollback is passed in and is TRUE, then rollback the Xn.
  // This could happen if savepoint rollback has failed.
  short rollbackTransaction(ComDiagsArea &diagsArea, NABoolean doXnRollback = FALSE);

  short beginImplicitSavepoint();
  short commitImplicitSavepoint();
  short rollbackImplicitSavepoint();

  unsigned short &defineContext() { return defineContext_; };

  long &envvarsContext() { return envvarsContext_; };

  ex_root_tdb *assignRootTdb(ex_root_tdb *new_root_tdb);

  NABoolean implicitTransNeeded(void);
  void turnOffAutoCommit(void);
  void resetAutoCommit(void);
  void saveTmodeValues(void);
  void resetTmodeValues(void);
  void commitImplicitTransAndResetTmodes(void);

  NABoolean isExeDebug(char *src, int charset);
  int octetLen(char *s, int charset);
  int octetLenplus1(char *s, int charset);
  int sourceLenplus1();

  // For stored procedure result set proxy statements, see if a
  // prepare of proxy syntax is required and if so, do the internal
  // prepare
  RETCODE rsProxyPrepare(ExRsInfo &rsInfo,          // IN
                         int rsIndex,               // IN
                         ComDiagsArea &diagsArea);  // INOUT

  // For stored procedure result set proxy statements, return TRUE if
  // the current statement source matches the newSource input string.
  NABoolean rsProxyCompare(const char *newSource) const;

  NABoolean isUninitializedMv(const char *physicalName, const char *lastUsedAnsiName, ComDiagsArea &diagsArea);
  NABoolean doesUninitializedMvExist(char **pMvName, ComDiagsArea &diagsArea);

  void unlockDLockForSharedCache(const char *msg = "");
  NABoolean ddlValid();

 public:
  Statement(SQLSTMT_ID *statement_id, CliGlobals *cliGlobals, StatementType stmt_type = DYNAMIC_STMT,
            char *cursorName = 0, Module *module = NULL);
  ~Statement();

  RETCODE prepare(char *source, ComDiagsArea &diagsArea, char *passed_gen_code, int passed_gen_code_len,
                  int charset = SQLCHARSETCODE_ISO88591, NABoolean unpackTdbs = TRUE, int cliFlags = 0);

  RETCODE prepare2(char *source, ComDiagsArea &diagsArea, char *gen_code, int gen_code_len, int charset,
                   NABoolean unpackTdbs, int cliFlags);

  int unpackAndInit(ComDiagsArea &diagsArea, short indexIntoCompilerArray);

  RETCODE fixup(CliGlobals *cliGlobals, Descriptor *input_desc, ComDiagsArea &diagsArea, NABoolean &doSimCheck,
                NABoolean &partitionUnavailable, const NABoolean donePrepare);

  RETCODE execute(CliGlobals *cliGlobals, Descriptor *input_desc, ComDiagsArea &diagsArea, ExecState = INITIAL_STATE_,
                  NABoolean fixupOnly = FALSE, int cliFlags = 0);
  RETCODE fetch(CliGlobals *cliGlobals, Descriptor *output_desc, ComDiagsArea &diagsArea, NABoolean newOperation);

  RETCODE error(ComDiagsArea &diagsArea);

  RETCODE doOltExecute(CliGlobals *cliGlobals, Descriptor *input_desc, Descriptor *output_desc, ComDiagsArea &diagsArea,
                       NABoolean &doNormalExecute, NABoolean &reExecute);

  int cancel();  // called by the cancel thread only.
  RETCODE describe(Descriptor *desc, int what_desc, ComDiagsArea &diagsArea);

  RETCODE addDescInfoIntoStaticDesc(Descriptor *desc, int what_desc, ComDiagsArea &diagsArea);

  RETCODE getRSProxySyntax(char *proxy, int maxlength, int *spaceRequired);
  RETCODE getExtractConsumerSyntax(char *proxy, int maxlength, int *spaceRequired);
  RETCODE getProxySyntax(char *proxy, int maxlength, int *spaceRequired, const char *prefix, const char *suffix);

  RETCODE doQuerySimilarityCheck(TrafQuerySimilarityInfo *qsi, NABoolean &simCheckFailed, ComDiagsArea &diagsArea);

  RETCODE mvSimilarityCheck(char *table, int siMvBitmap, int rcbMvBitmap, NABoolean &simCheckFailed,
                            ComDiagsArea &diagsArea);

  NABoolean isIudTargetTable(char *tableName, SqlTableOpenInfoPtr *stoiList);

  RETCODE close(ComDiagsArea &diagsArea, NABoolean inRollback = FALSE);
  RETCODE bindTo(Statement *statement_id);

  RETCODE closeTables(ComDiagsArea &diagsArea);
  RETCODE reOpenTables(ComDiagsArea &diagsArea);

  RETCODE lockTables();
  void unlockAllObjects();

  RETCODE releaseTransaction(NABoolean allWorkRequests = FALSE, NABoolean alwaysSendReleaseMsg = FALSE,
                             NABoolean statementRemainsOpen = FALSE  // this param for holdable cursor.
  );

  void releaseEsps(NABoolean closeAllOpens);

  // When a stmt is deallocated, opens on tables are closed. If
  // the opens are to be reused, then only shared opens are closed.
  // There are some cases where we want to close all opens even
  // if opens are being reused.
  // If closeAllOpens is TRUE, then do the real close of the table.
  RETCODE dealloc(NABoolean closeAllOpens = FALSE);

  // Helper functions called by dealloc() and releaseSpace()
  RETCODE releaseTcbs(NABoolean closeAllOpens);
  void releaseStats();
  NABoolean updateInProgress();

  // reads trigger status from rfork and updates trigger status vector in TCB.
  RETCODE getTriggersStatus(SqlTableOpenInfoPtr *stoiList, ComDiagsArea &diagsArea);
  inline void *getStmtHandle();

  inline Module *getModule() { return module_; }

  inline int getStatementIndex() { return statementIndex_; };
  inline void setStatementIndex(int i) { statementIndex_ = i; };

  SQLSTMT_ID *getStmtId() { return (SQLSTMT_ID *)statement_id; };
  const SQLMODULE_ID *getModuleId() { return statement_id->module; };

  inline const char *getIdentifier();
  // inline char *getModuleName();

  inline ContextCli *getContext() { return context_; };

  inline ex_root_tdb *getRootTdb() const { return root_tdb; }
  inline ex_root_tcb *getRootTcb() const { return root_tcb; }

  NABoolean doOltQueryOpt();

  Space *getUnpackSpace() { return unpackSpace_; }
  void setUnpackSpace(Space *sp) { unpackSpace_ = sp; }

  // void setCursorName(char * cn, long cn_len = strlen(cn));
  void setCursorName(const char *cn);
  inline SQLSTMT_ID *getCursorName();

  inline short allocated();
  short transactionReqd();

  ExMasterStmtGlobals *getGlobals() { return statementGlobals_; }

  long getRowsAffected();
  NABoolean noRowsAffected(ComDiagsArea &diags);

  inline void addDefaultDesc(Descriptor *desc, int what_desc);
  inline Descriptor *getDefaultDesc(int what_desc);

  inline State getState() const { return stmt_state; }
  inline ExecState getExecState() const { return state_; }
  inline NABoolean isStandaloneQ() { return standaloneStatement_; }
  inline void setStandaloneQ(NABoolean b) { standaloneStatement_ = b; }
  inline NABoolean wmsMonitorQuery() { return wmsMonitorQuery_; }
  inline void setWMSMonitorQuery(NABoolean b) { wmsMonitorQuery_ = b; }
  inline bool isMultiThreadedEsp() { return multiThreadedEsp_ != FALSE; }
  // set the state of a statement. As a side effect the statement
  // is added or removed from the current context's openStatementList.
  // Thus, never set the state directly. Always use this method.
  // The only "exception" is the initialization list of the constructor
  // of the statement.
  void setState(State state);

  inline StatementType getStatementType();
  int getQueryType();

  void copyGenCode(char *gen_code, int gen_code_len, NABoolean unpackTDBs = TRUE);

  void copyInSourceStr(char *in_source_str_, int in_source_length_, int charset = SQLCHARSETCODE_ISO88591);

  void copyOutSourceStr(char *out_source_str_, int &out_source_length_);

  void copySchemaName(char *schemaName, int schemaNameLength);

  void copyRecompControlInfo(char *basePtr, char *controlInfo, int controlInfoLength);

  Statement *getCurrentOfCursorStatement(char *cursorName);

  Statement *currentOfCursorStatement() { return currentOfCursorStatement_; };
  short handleUpdDelCurrentOf(ComDiagsArea &diags);

  int recompControlInfoLen() { return recompControlInfoLen_; };
  char *recompControlInfo() { return recompControlInfo_; };

  Queue *getClonedStatements() { return clonedStatements; };

  void setUniqueStmtId(char *id);
  char *getUniqueStmtId() { return uniqueStmtId_; }
  int getUniqueStmtIdLen() { return uniqueStmtIdLen_; }

  int setParentQid(char *queryId);
  char *getParentQid();
  void setParentQidSystem(char *parentQidSystem);
  char *getParentQidSystem();
  long getExeStartTime();
  void setExeStartTime(long exeStartTime);

  int getCliLevel() const { return cliLevel_; }

  inline short isResecureNeeded();
  inline void setResecureNeeded();
  inline void resetResecureNeeded();
  inline short isCloned();
  inline void setCloned();
  inline void resetCloned();

  inline NABoolean isDeletedCursor();
  inline void setDeletedCursor();
  inline void resetDeletedCursor();

  NABoolean isSelectInto();
  NABoolean isDeleteCurrentOf();
  NABoolean isUpdateCurrentOf();

  NABoolean computeInputDescBulkMoveInfo() { return ((flags_ & COMPUTE_INPUTDESC_BULKMOVE_INFO) != 0); }
  NABoolean computeOutputDescBulkMoveInfo() { return ((flags_ & COMPUTE_OUTPUTDESC_BULKMOVE_INFO) != 0); }
  void setComputeBulkMoveInfo(NABoolean v) {
    if (v) {
      flags_ |= COMPUTE_INPUTDESC_BULKMOVE_INFO;
      flags_ |= COMPUTE_OUTPUTDESC_BULKMOVE_INFO;
    } else {
      flags_ &= ~COMPUTE_INPUTDESC_BULKMOVE_INFO;
      flags_ &= ~COMPUTE_OUTPUTDESC_BULKMOVE_INFO;
    }
  }

  void setComputeInputDescBulkMoveInfo(NABoolean v) {
    (v ? flags_ |= COMPUTE_INPUTDESC_BULKMOVE_INFO : flags_ &= ~COMPUTE_INPUTDESC_BULKMOVE_INFO);
  };

  void setComputeOutputDescBulkMoveInfo(NABoolean v) {
    (v ? flags_ |= COMPUTE_OUTPUTDESC_BULKMOVE_INFO : flags_ &= ~COMPUTE_OUTPUTDESC_BULKMOVE_INFO);
  };

  NABoolean oltOpt() { return ((flags_ & OLT_OPT) != 0); }
  void setOltOpt(NABoolean v);

  StatementInfo *stmtInfo() { return stmtInfo_; }
  void setStmtInfo(StatementInfo *stmtInfo) { stmtInfo_ = stmtInfo; }

  NABoolean tsMismatched() { return ((flags_ & TS_MISMATCHED) != 0); }
  void setTsMismatched(NABoolean v) { v ? flags_ |= TS_MISMATCHED : flags_ &= ~TS_MISMATCHED; }

  NABoolean firstResolveDone() { return (flags_ & FIRST_RESOLVE_DONE) != 0; };
  void setFirstResolveDone(short v) { (v ? flags_ |= FIRST_RESOLVE_DONE : flags_ &= ~FIRST_RESOLVE_DONE); };

  NABoolean recompWarn() { return (flags_ & RECOMP_WARN) != 0; };
  void setRecompWarn(short v) { (v ? flags_ |= RECOMP_WARN : flags_ &= ~RECOMP_WARN); };

  NABoolean autocommitXn() { return (flags_ & AUTOCOMMIT_XN) != 0; };
  void setAutocommitXn(short v) { (v ? flags_ |= AUTOCOMMIT_XN : flags_ &= ~AUTOCOMMIT_XN); };

  NABoolean autocommitSavepoint() { return (flags_ & AUTOCOMMIT_SAVEPOINT) != 0; };
  void setAutocommitSavepoint(short v) { (v ? flags_ |= AUTOCOMMIT_SAVEPOINT : flags_ &= ~AUTOCOMMIT_SAVEPOINT); };

  NABoolean syncQueryForBinglog() { return (flags_ & SYNC_QUERY_FOR_BINGLOG) != 0; };
  void setSyncQueryForBinglog(short v) { (v ? flags_ |= SYNC_QUERY_FOR_BINGLOG : flags_ &= ~SYNC_QUERY_FOR_BINGLOG); };

  NABoolean odbcProcess() { return (flags_ & ODBC_PROCESS) != 0; };
  void setOdbcProcess(short v) { (v ? flags_ |= ODBC_PROCESS : flags_ &= ~ODBC_PROCESS); };

  NABoolean systemModuleStmt() { return (flags_ & SYSTEM_MODULE_STMT) != 0; };
  void setSystemModuleStmt(short v) { (v ? flags_ |= SYSTEM_MODULE_STMT : flags_ &= ~SYSTEM_MODULE_STMT); };

  NABoolean aqrReprepareNeeded() { return (flags_ & AQR_REPREPARE_NEEDED) != 0; };
  void setAqrReprepareNeeded(NABoolean v) { (v ? flags_ |= AQR_REPREPARE_NEEDED : flags_ &= ~AQR_REPREPARE_NEEDED); };

  NABoolean returnRecompWarn();

  void dump(ostream *outstream);

  // QSTUFF
  void setHoldable(SQLATTRHOLDABLE_INTERNAL_TYPE h) { holdable_ = h; }
  RETCODE setHoldable(ComDiagsArea &diagsArea, NABoolean h);
  SQLATTRHOLDABLE_INTERNAL_TYPE getHoldable() { return holdable_; }
  RETCODE setPubsubHoldable(ComDiagsArea &diagsArea, NABoolean h);
  inline NABoolean isPubsubHoldable() { return holdable_ == SQLCLIDEV_PUBSUB_HOLDABLE; }
  NABoolean isEmbeddedUpdateOrDelete(void);
  NABoolean isStreamScan(void);
  // QSTUFF

  RETCODE setAnsiHoldable(ComDiagsArea &diagsArea, NABoolean h);
  inline NABoolean isAnsiHoldable() { return holdable_ == SQLCLIDEV_ANSI_HOLDABLE; }

  RETCODE setInputArrayMaxsize(ComDiagsArea &diagsArea, const int inpArrSize);
  inline int getInputArrayMaxsize() const { return inputArrayMaxsize_; }

  RETCODE setRowsetAtomicity(ComDiagsArea &diagsArea, const AtomicityType atomicity);
  inline AtomicityType getRowsetAtomicity() const { return (AtomicityType)rowsetAtomicity_; }

  RETCODE setNotAtomicFailureLimit(ComDiagsArea &diagsArea, const int limit);
  inline int getNotAtomicFailureLimit() const { return notAtomicFailureLimit_; }

  inline int getRootTdbSize() { return root_tdb_size; }

  inline int getSrcStrSize() { return source_length; }

  inline char *getSrcStr() { return source_str; }

  inline int getCharSet() { return charset_; }

  inline RmsLimitLevel getRmsLimitLevel() const { return (RmsLimitLevel)rmsLimit_; }

  void setRmsLimitLevel(const RmsLimitLevel limit) { rmsLimit_ = limit; }

  ExStatisticsArea *getStatsArea();
  ExStatisticsArea *getOrigStatsArea();
  // A method return the masterStats when the statement is not yet fixed up
  ExStatisticsArea *getCompileStatsArea();

  void setCompileEndTime(long julianTime) { compileEndTime_ = julianTime; };
  long getCompileEndTime() const { return compileEndTime_; };

  inline Statement *&prevCloseStatement() { return prevCloseStatement_; }
  inline Statement *&nextCloseStatement() { return nextCloseStatement_; }
  inline int &closeSequence() { return closeSequence_; }
  int releaseSpace();
  NABoolean isReclaimable();

  // Wait for completion of UDR requests associated with this
  // statement. If allRequests is FALSE then only wait for
  // transactional requests.
  RETCODE completeUdrRequests(NABoolean allRequests) const;

  NABoolean containsUdrInteractions() const;

  void setParentCall(Statement *statement) { parentCall_ = statement; }
  Statement *getParentCall() const { return parentCall_; }
  ExRsInfo *getResultSetInfo() const;
  ExRsInfo *getOrCreateResultSetInfo();

  // the following methods support no-wait operations on a
  // Statement

  NABoolean mightHaveWorkToDo(void);
  inline NABoolean noWaitOpEnabled(void) { return noWaitOpEnabled_; };
  inline void setNoWaitOpEnabled(void) { noWaitOpEnabled_ = TRUE; };
  inline void resetNoWaitOpEnabled(void) { noWaitOpEnabled_ = FALSE; };
  inline void setNoWaitOpEnableStatus(NABoolean status) { noWaitOpEnabled_ = status; };
  inline NABoolean noWaitOpPending(void) { return noWaitOpPending_; };
  void setNoWaitOpPending(void);
  void resetNoWaitOpPending(void);
  // BM Gil Someone needs to use the following method
  inline NABoolean noWaitOpIncomplete(void) { return noWaitOpIncomplete_; };
  inline void setNoWaitOpIncomplete(void) { noWaitOpIncomplete_ = TRUE; };
  inline int getNowaitTag(void) { return statement_id->tag; };
  inline void setNowaitTag(int tag) { statement_id->tag = tag; };
  inline int getFileNumber(void) { return fileNumber_; };

  inline RETCODE setFileNumber(int fileNumber) {
    RETCODE rc = SUCCESS;  // assume success

    if (fileNumber_ == -1)
      fileNumber_ = fileNumber;
    else
      rc = ERROR;

    return rc;
  };

  inline RETCODE resetFileNumber(void) {
    RETCODE rc = SUCCESS;

    if (fileNumber_ != -1)
      fileNumber_ = -1;
    else
      rc = ERROR;

    return rc;
  };
  inline int getStmtTasks(void) { return tasks_; };
  inline void setStmtTasks(int tasks) { tasks_ = tasks; };

  inline NAHeap *stmtHeap() { return &heap_; };

  void updateTModeValues();

  inline StmtStats *getStmtStats() { return stmtStats_; }
  void setStmtStats(NABoolean autoRetry);
  NABoolean isExcludedFromRMS();

  // Plan versioning stuff
  void issuePlanVersioningWarnings(ComDiagsArea &diagsArea);

  // For returning statement attributes related to parallel extract
  int getConsumerQueryLen(int index);
  void getConsumerQuery(int index, char *buf, int buflen);
  int getConsumerCpu(int index);
  int initStrTarget(SQLDESC_ID *sql_source, ContextCli &currContext, ComDiagsArea &diags, StrTarget &strTarget);
  // auto query retry
  AQRStatementInfo *aqrStmtInfo() { return aqrStmtInfo_; };
  void setAqrStmtInfo(AQRStatementInfo *v) { aqrStmtInfo_ = v; }
  NABoolean updateChildQid();
  void updateStatsAreaInContext();
  int setChildQueryInfo(ComDiagsArea *diagsArea, char *uniqueQueryId, int uniqueQueryIdLen,
                        SQL_QUERY_COST_INFO *query_cost_info, SQL_QUERY_COMPILER_STATS_INFO *comp_stats_info);
  int getChildQueryInfo(ComDiagsArea &diagsArea, char *uniqueQueryId, int uniqueQueryIdMaxLen, int *uniqueQueryIdLen,
                        SQL_QUERY_COST_INFO *query_cost_info, SQL_QUERY_COMPILER_STATS_INFO *comp_stats_info);
  // return TRUE if query is prefixed by display,
  // e.g. display select ...
  NABoolean isDISPLAY();
  char *getUtf8Sql(NABoolean withNoReplicate = FALSE);
  void setTriggerExecErr(bool v) { triggerExecErr_ = v; }
  bool getTriggerExecErr() { return triggerExecErr_; }
  NABoolean isInternalTransactionSql();

#ifdef _DEBUG
 public:
  void StmtPrintf(const char *formatString, ...) const;
  NABoolean stmtDebugEnabled() const { return stmtDebug_; }
  NABoolean stmtListDebugEnabled() const { return stmtListDebug_; }

 private:
  NABoolean stmtDebug_;
  NABoolean stmtListDebug_;
#endif

};  // class Statement

inline short Statement::isCloned() { return (short)(flags_ & CLONED_BIT); }

inline void Statement::setCloned() { flags_ |= CLONED_BIT; }

inline void Statement::resetCloned() { flags_ &= ~CLONED_BIT; }

inline NABoolean Statement::isDeletedCursor() { return ((flags_ & DELETED_CURSOR_BIT) != 0); }

inline void Statement::setDeletedCursor() { flags_ |= DELETED_CURSOR_BIT; }

inline void Statement::resetDeletedCursor() { flags_ &= ~DELETED_CURSOR_BIT; }

inline short Statement::isResecureNeeded() { return (short)(flags_ & RESECURE_NEEDED_BIT); }

inline void Statement::setResecureNeeded() { flags_ |= RESECURE_NEEDED_BIT; }

inline void Statement::resetResecureNeeded() { flags_ &= ~RESECURE_NEEDED_BIT; }

inline short Statement::fixupState() { return (short)(flags_ & FIXUP_BIT); }

inline void Statement::setFixupState(short state) {
  if (state)
    flags_ |= FIXUP_BIT;
  else
    flags_ &= ~FIXUP_BIT;
}

inline void *Statement::getStmtHandle() { return statement_id->handle; }

inline const char *Statement::getIdentifier() { return statement_id->identifier; }

/*
inline long Statement::getIdentifierLen()
{
  return GET_SQLCLI_OBJ_NAME_LEN_PTR(statement_id->identifier);
}
*/

/*
inline char * Statement::getModuleName()
{
  return statement_id->module->module_name;
}
*/

/*
inline long Statement::getModuleNameLen()
{
  return GET_SQL_MODULE_NAME_LEN_PTR(statement_id->module);
}
*/

/* return -1, if statement was allocated by a call to AllocStmt(). */
/* AllocStmt() is called for extended dyn statements or for CLI    */
/* users passing no name.                                          */
inline short Statement::allocated() {
  if (stmt_type == DYNAMIC_STMT)
    return -1;
  else
    return 0;
}

inline SQLSTMT_ID *Statement::getCursorName() { return cursor_name_; }

inline void Statement::addDefaultDesc(Descriptor *desc, int what_desc) {
  if (what_desc == SQLWHAT_INPUT_DESC)
    default_input_desc = desc;
  else if (what_desc == SQLWHAT_OUTPUT_DESC)
    default_output_desc = desc;
}

inline Descriptor *Statement::getDefaultDesc(int what_desc) {
  if (what_desc == SQLWHAT_INPUT_DESC)
    return default_input_desc;
  else if (what_desc == SQLWHAT_OUTPUT_DESC)
    return default_output_desc;
  else
    return 0;
}

inline Statement::StatementType Statement::getStatementType() { return stmt_type; }
short convertTableName(char *tgt, char *src);

//
// Class to store last known surrogate file security timestamp
// and permission check information.
// Used by implementation of CALL <udr>
//
class UdrSecurityInfo : public NABasicObject {
 public:
  UdrSecurityInfo() : previousSecurityTS_(0), previouslyChecked_(FALSE), previousResult_(ERROR) {}

  // Accessors
  const char *getUdrName() { return udrName_; }

  long getPreviousSecurityTS() const { return previousSecurityTS_; }

  NABoolean isPreviouslyChecked() const { return previouslyChecked_; }

  RETCODE getPreviousResult() { return previousResult_; }

  // Mutators
  void setUdrName(char *udrName) { udrName_ = udrName; }

  void setPreviousSecurityTS(long secTime) { previousSecurityTS_ = secTime; }

  void setPreviouslyChecked(NABoolean checked) { previouslyChecked_ = checked; }

  void setPreviousResult(RETCODE result) { previousResult_ = result; }

 private:
  char *udrName_;
  long previousSecurityTS_;
  NABoolean previouslyChecked_;
  RETCODE previousResult_;
};
#endif
