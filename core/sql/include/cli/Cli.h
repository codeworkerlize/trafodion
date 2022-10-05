
#ifndef CLI_H
#define CLI_H

/* -*-C++-*-
******************************************************************************
*
* File:         Cli.h
* Description:  Separation of Cli.cpp into a stub and client routine.
*               Originally done to help with NT work on single-threading
*               access to the CLI routines.  Will help to segregate
*               other work like DLL/Library work etc.
*
* Created:      7/19/97
* Language:     C++
*
*
*
******************************************************************************
*/

#include <stdarg.h>
#include "common/Platform.h"
#include "cli/SQLCLIdev.h"
#include "export/ComDiags.h"
#include "executor/DistributedLock_JNI.h"
class Statement;
class CliGlobals;
class ContextCli;
class Descriptor;
class DistributedLock_JNI;

// these enums are used in method, PerformTasks.
enum CliTasks {
  CLI_PT_CLEAR_DIAGS = 0x0001,
  CLI_PT_EXEC = 0x0002,
  CLI_PT_FETCH = 0x0004,
  CLI_PT_CLOSE = 0x0008,
  CLI_PT_GET_INPUT_DESC = 0x0010,
  CLI_PT_GET_OUTPUT_DESC = 0x0020,
  CLI_PT_SPECIAL_END_PROCESS = 0x0040,
  CLI_PT_CLOSE_ON_ERROR = 0x0080,
  CLI_PT_PROLOGUE = 0x0100,
  CLI_PT_EPILOGUE = 0x0200,
  CLI_PT_OPT_STMT_INFO = 0x0400,
  CLI_PT_CLEAREXECFETCHCLOSE = 0x0800,
  CLI_PT_SET_AQR_INFO = 0x1000
};

// -----------------------------------------------------------------------
// Internal CLI layer, common to NT and NSK platforms. On NT, this is
// similar to the external layer, except that no try/catch block is
// activated and that calls are not protected by a semaphore. On NSK,
// the executor segment is revealed at this time and the switch to
// PRIV has occurred. The internal layer has an additional argument:
// the CLI Globals.
//
// The main reason for giving these functions extern "C" linkage is
// function redirection. This makes the function names easier to
// handle since they don't get mangled.
// -----------------------------------------------------------------------

struct DiagsConditionItem {
  int item_id;
  char *var_ptr;
  int length;
  int output_entry;
  int condition_index;

  // When MESSAGE_OCTET_LENGTH is retrieved from the diags area,
  // its value will be dependent on the CHARACTER SET defined
  // on the host variable to which MESSAGE_TEXT is assigned.
  // This new data member "charset" is used to remember the
  // CHARACTER SET defined on the host variable.
  int charset;
};

extern "C" {

int SQLCLI_AllocDesc(/*IN*/ CliGlobals *cliGlobals,
                       /*INOUT*/ SQLDESC_ID *desc_id,

                       /*IN OPTIONAL*/ SQLDESC_ID *input_descriptor);

int SQLCLI_AllocStmt(/*IN*/ CliGlobals *cliGlobals,
                       /*INOUT*/ SQLSTMT_ID *new_statement_id,
                       /*IN OPTIONAL*/ SQLSTMT_ID *cloned_statement);

int SQLCLI_AllocStmtForRS(/*IN*/ CliGlobals *cliGlobals,
                            /*IN*/ SQLSTMT_ID *callStmtId,
                            /*IN*/ int resultSetIndex,
                            /*INOUT*/ SQLSTMT_ID *resultSetStmtId);

int SQLCLI_AssocFileNumber(/*IN*/ CliGlobals *cliGlobals,
                             /*IN*/ SQLSTMT_ID *statement_id,
                             /*IN*/ short file_number);

int SQLCLI_GetDiskMaxSize(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN*/ char *volname,
    /*OUT*/ long *totalCapacity,
    /*OUT*/ long *totalFreespace);
int SQLCLI_GetListOfDisks(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN/OUT*/ char *diskBuffer,
    /* OUT */ int *numTSEs,
    /* OUT */ int *maxTSELength,
    /* IN/OUT */ int *diskBufferLength);
int SQLCLI_BreakEnabled(/*IN*/ CliGlobals *cliGlobals,
                          /*IN*/ UInt32 enabled);

int SQLCLI_SPBreakRecvd(/*IN*/ CliGlobals *cliGlobals,
                          /*OUT*/ UInt32 *breakRecvd);

int SQLCLI_ClearDiagnostics(/*IN*/ CliGlobals *cliGlobals,
                              /*IN OPTIONAL*/ SQLSTMT_ID *statement_id);

int SQLCLI_CloseStmt(/*IN*/ CliGlobals *cliGlobals,
                       /*IN*/ SQLSTMT_ID *statement_id);

int SQLCLI_CreateContext(/*IN*/ CliGlobals *cliGlobals,
                           /*OUT*/ SQLCTX_HANDLE *context_handle,
                           /*IN*/ char *sqlAuthId,
                           /*IN*/ int /* for future use */);

int SQLCLI_CurrentContext(/*IN*/ CliGlobals *cliGlobals,
                            /*OUT*/ SQLCTX_HANDLE *context_handle);

int SQLCLI_DropModule(/*IN*/ CliGlobals *cliGlobals,
                        /*IN*/ const SQLMODULE_ID *module_name);

int SQLCLI_DeleteContext(/*IN*/ CliGlobals *cliGlobals,
                           /*IN*/ SQLCTX_HANDLE context_handle);

int SQLCLI_ResetContext(/*IN*/ CliGlobals *cliGlobals,
                          /*IN*/ SQLCTX_HANDLE context_handle,
                          /*IN*/ void *contextMsg);

int SQLCLI_GetUdrErrorFlags_Internal(/*IN*/ CliGlobals *cliGlobals,
                                       /*OUT*/ int *udrErrorFlags);

int SQLCLI_SetUdrAttributes_Internal(/*IN*/ CliGlobals *cliGlobals,
                                       /*IN*/ int sqlAccessMode,
                                       /*IN*/ int /* for future use */);

int SQLCLI_ResetUdrErrorFlags_Internal(/*IN*/ CliGlobals *cliGlobals);

int SQLCLI_SetUdrRuntimeOptions_Internal(/*IN*/ CliGlobals *cliGlobals,
                                           /*IN*/ const char *options,
                                           /*IN*/ int optionsLen,
                                           /*IN*/ const char *delimiters,
                                           /*IN*/ int delimsLen);

int SQLCLI_DeallocDesc(/*IN*/ CliGlobals *cliGlobals,
                         /*IN*/ SQLDESC_ID *desc_id);

int SQLCLI_DeallocStmt(/*IN*/ CliGlobals *cliGlobals,
                         /*IN*/ SQLSTMT_ID *statement_id);

int SQLCLI_DefineDesc(/*IN*/ CliGlobals *cliGlobals,
                        /*IN*/ SQLSTMT_ID *statement_id,
                        /* (SQLWHAT_DESC) *IN*/ int what_descriptor,
                        /*IN*/ SQLDESC_ID *sql_descriptor);

int SQLCLI_DescribeStmt(/*IN*/ CliGlobals *cliGlobals,
                          /*IN*/ SQLSTMT_ID *statement_id,
                          /*IN OPTIONAL*/ SQLDESC_ID *input_descriptor,
                          /*IN OPTIONAL*/ SQLDESC_ID *output_descriptor);

int SQLCLI_DisassocFileNumber(/*IN*/ CliGlobals *cliGlobals,
                                /*IN*/ SQLSTMT_ID *statement_id);

int SQLCLI_DropContext(/*IN*/ CliGlobals *cliGlobals,
                         /*IN*/ SQLCTX_HANDLE context_handle);

int SQLCLI_Exec(/*IN*/ CliGlobals *cliGlobals,
                  /*IN*/ SQLSTMT_ID *statement_id,
                  /*IN OPTIONAL*/ SQLDESC_ID *input_descriptor,
                  /*IN*/ int num_ptr_pairs,
                  /*IN*/ int num_ap,
                  /*IN*/ va_list ap,
                  /*IN*/ SQLCLI_PTR_PAIRS ptr_pairs[]);

int SQLCLI_ExecClose(/*IN*/ CliGlobals *cliGlobals,
                       /*IN*/ SQLSTMT_ID *statement_id,
                       /*IN OPTIONAL*/ SQLDESC_ID *input_descriptor,
                       /*IN*/ int num_ptr_pairs,
                       /*IN*/ int num_ap,
                       /*IN*/ va_list ap,
                       /*IN*/ SQLCLI_PTR_PAIRS ptr_pairs[]);

int SQLCLI_ExecDirect(/*IN*/ CliGlobals *cliGlobals,
                        /*IN*/ SQLSTMT_ID *statement_id,
                        /*IN*/ SQLDESC_ID *sql_source,
                        /*IN OPTIONAL*/ SQLDESC_ID *input_descriptor,
                        /*IN*/ int num_ptr_pairs,
                        /*IN*/ int num_ap,
                        /*IN*/ va_list ap,
                        /*IN*/ SQLCLI_PTR_PAIRS ptr_pairs[]);
int SQLCLI_ExecDirect2(/*IN*/ CliGlobals *cliGlobals,
                         /*IN*/ SQLSTMT_ID *statement_id,
                         /*IN*/ SQLDESC_ID *sql_source,
                         /*IN*/ int prepFlags,
                         /*IN  OPTIONAL*/ SQLDESC_ID *input_descriptor,
                         /*IN*/ int num_ptr_pairs,
                         /*IN*/ int num_ap,
                         /*IN*/ va_list ap,
                         /*IN*/ SQLCLI_PTR_PAIRS ptr_pairs[]);
int SQLCLI_ExecDirectDealloc(/*IN*/ CliGlobals *cliGlobals,
                               /*IN*/ SQLSTMT_ID *statement_id,
                               /*IN*/ SQLDESC_ID *sql_source,
                               /*IN OPTIONAL*/ SQLDESC_ID *input_descriptor,
                               /*IN*/ int num_ptr_pairs,
                               /*IN*/ int num_ap,
                               /*IN*/ va_list ap,
                               /*IN*/ SQLCLI_PTR_PAIRS ptr_pairs[]);

int SQLCLI_ExecFetch(/*IN*/ CliGlobals *cliGlobals,
                       /*IN*/ SQLSTMT_ID *statement_id,
                       /*IN OPTIONAL*/ SQLDESC_ID *input_descriptor,
                       /*IN*/ int num_ptr_pairs,
                       /*IN*/ int num_ap,
                       /*IN*/ va_list ap,
                       /*IN*/ SQLCLI_PTR_PAIRS ptr_pairs[]);

int SQLCLI_ClearExecFetchClose(/*IN*/ CliGlobals *cliGlobals,
                                 /*IN*/ SQLSTMT_ID *statement_id,
                                 /*IN OPTIONAL*/ SQLDESC_ID *input_descriptor,
                                 /*IN OPTIONAL*/ SQLDESC_ID *output_descriptor,
                                 /*IN*/ int num_input_ptr_pairs,
                                 /*IN*/ int num_output_ptr_pairs,
                                 /*IN*/ int num_total_ptr_pairs,
                                 /*IN*/ int num_ap,
                                 /*IN*/ va_list ap,
                                 /*IN*/ SQLCLI_PTR_PAIRS input_ptr_pairs[],
                                 /*IN*/ SQLCLI_PTR_PAIRS output_ptr_pairs[]);

int SQLCLI_Fetch(/*IN*/ CliGlobals *cliGlobals,
                   /*IN*/ SQLSTMT_ID *statement_id,
                   /*IN OPTIONAL*/ SQLDESC_ID *output_descriptor,
                   /*IN*/ int num_ptr_pairs,
                   /*IN*/ int num_ap,
                   /*IN*/ va_list ap,
                   /*IN*/ SQLCLI_PTR_PAIRS ptr_pairs[]);

int SQLCLI_FetchClose(/*IN*/ CliGlobals *cliGlobals,
                        /*IN*/ SQLSTMT_ID *statement_id,
                        /*IN OPTIONAL*/ SQLDESC_ID *output_descriptor,
                        /*IN*/ int num_ptr_pairs,
                        /*IN*/ int num_ap,
                        /*IN*/ va_list ap,
                        /*IN*/ SQLCLI_PTR_PAIRS ptr_pairs[]);

int SQLCLI_FetchMultiple(/*IN*/ CliGlobals *cliGlobals,
                           /*IN*/ SQLSTMT_ID *statement_id,
                           /*IN  OPTIONAL*/ SQLDESC_ID *output_descriptor,
                           /*IN*/ int rowset_size,
                           /*IN*/ int *rowset_status_ptr,
                           /*OUT*/ int *rowset_nfetched,
                           /*IN*/ int num_quadruple_fields,
                           /*IN*/ int num_ap,
                           /*IN*/ va_list ap,
                           /*IN*/ SQLCLI_QUAD_FIELDS quad_fields[]);

int SQLCLI_Cancel(/*IN*/ CliGlobals *cliGlobals,
                    /*IN OPTIONAL*/ SQLSTMT_ID *statement_id);

int SQLCLI_CancelOperation(/*IN*/ CliGlobals *cliGlobals,
                             /*IN*/ long transid);

int SQLCLI_GetDescEntryCount(/*IN*/ CliGlobals *cliGlobals,
                               /*IN*/ SQLDESC_ID *sql_descriptor,
                               /*IN*/ SQLDESC_ID *output_descriptor);

int SQLCLI_GetDescItem(/*IN*/ CliGlobals *cliGlobals,
                         /*IN*/ SQLDESC_ID *sql_descriptor,
                         /*IN*/ int entry,
                         /* (SQLDESC_ITEM_ID) *IN*/ int what_to_get,
                         /*OUT OPTIONAL*/ void *numeric_value,
                         /*OUT OPTIONAL*/ char *string_value,
                         /*IN OPTIONAL*/ int max_string_len,
                         /*OUT OPTIONAL*/ int *len_of_item,
                         /*IN OPTIONAL*/ int start_from_offset);

int SQLCLI_GetDescItems(/*IN*/ CliGlobals *cliGlobals,
                          /*IN*/ SQLDESC_ID *sql_descriptor,
                          /*IN*/ SQLDESC_ITEM desc_items[],
                          /*IN*/ SQLDESC_ID *value_num_descriptor,
                          /*IN*/ SQLDESC_ID *output_descriptor);

int SQLCLI_GetDescItems2(/*IN*/ CliGlobals *cliGlobals,
                           /*IN*/ SQLDESC_ID *desc_id,
                           /*IN*/ int no_of_desc_items,
                           /*IN*/ SQLDESC_ITEM desc_items[]);

int SQLCLI_GetDiagnosticsStmtInfo(/*IN*/ CliGlobals *cliGlobals,
                                    /*IN*/ int *stmt_info_items,
                                    /*IN*/ SQLDESC_ID *output_descriptor);

int SQLCLI_GetDiagnosticsStmtInfo2(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN OPTIONAL*/ SQLSTMT_ID *statement_id,
    /*IN* (SQLDIAG_STMT_INFO_ITEM_ID) */ int what_to_get,
    /*OUT OPTIONAL*/ void *numeric_value,
    /*OUT OPTIONAL*/ char *string_value,
    /*IN OPTIONAL*/ int max_string_len,
    /*OUT OPTIONAL*/ int *len_of_item);

int SQLCLI_GetDiagnosticsCondInfo(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN*/ SQLDIAG_COND_INFO_ITEM *cond_info_items,
    /*IN*/ SQLDESC_ID *cond_num_descriptor,
    /*IN*/ SQLDESC_ID *output_descriptor,
    /*OUT*/ IpcMessageBufferPtr message_buffer_ptr,
    /*IN*/ IpcMessageObjSize message_obj_size,
    /*OUT*/ IpcMessageObjSize *message_obj_size_needed,
    /*OUT*/ IpcMessageObjType *message_obj_type,
    /*OUT*/ IpcMessageObjVersion *message_obj_version,
    /*IN*/ int condition_item_count,
    /*OUT*/ int *condition_item_count_needed,
    /*OUT*/ DiagsConditionItem *condition_item_array,
    /*OUT*/ SQLMXLoggingArea::ExperienceLevel *emsEventEL);

int SQLCLI_GetPackedDiagnostics(
    /*IN*/ CliGlobals *cliGlobals,
    /*OUT*/ IpcMessageBufferPtr message_buffer_ptr,
    /*IN*/ IpcMessageObjSize message_obj_size,
    /*OUT*/ IpcMessageObjSize *message_obj_size_needed,
    /*OUT*/ IpcMessageObjType *message_obj_type,
    /*OUT*/ IpcMessageObjVersion *message_obj_version);

int SQLCLI_GetSQLCODE(/*IN*/ CliGlobals *cliGlobals,
                        /*OUT*/ int *theSQLCODE);

int SQLCLI_GetDiagnosticsCondInfo2(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN* (SQLDIAG_COND_INFO_ITEM_ID) */ int what_to_get,
    /*IN*/ int conditionNum,
    /*OUT OPTIONAL*/ int *numeric_value,
    /*OUT OPTIONAL*/ char *string_value,
    /*IN OPTIONAL */ int max_string_len,
    /*OUT OPTIONAL*/ int *len_of_item);

int SQLCLI_GetDiagnosticsCondInfo3(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN*/ int no_of_condition_items,
    /*IN*/ SQLDIAG_COND_INFO_ITEM_VALUE diag_cond_info_item_values[],
    /*OUT*/ IpcMessageObjSize *message_obj_size_needed);

int SQLCLI_GetDiagnosticsArea(
    /*IN*/ CliGlobals *cliGlobals,
    /*OUT*/ IpcMessageBufferPtr message_buffer_ptr,
    /*IN*/ IpcMessageObjSize message_obj_size,
    /*OUT*/ IpcMessageObjType *message_obj_type,
    /*OUT*/ IpcMessageObjVersion *message_obj_version);

int SQLCLI_GetMainSQLSTATE(/*IN*/ CliGlobals *cliGlobals,
                             /*IN*/ SQLSTMT_ID *stmtId,
                             /*IN*/ int sqlcode,
                             /*OUT*/ char *sqlstate /* assumed to be char[6] */);

int SQLCLI_GetSQLSTATE(/*IN*/ CliGlobals *cliGlobals,
                         /*OUT*/ char *theSQLSTATE);

int SQLCLI_GetCSQLSTATE(/*IN*/ CliGlobals *cliGlobals,
                          /*OUT*/ char *theSQLSTATE,
                          /*IN*/ int theSQLCODE);

int SQLCLI_GetCobolSQLSTATE(/*IN*/ CliGlobals *cliGlobals,
                              /*OUT*/ char *theSQLSTATE,
                              /*IN*/ int theSQLCODE);

// For internal use only -- do not document!
int SQLCLI_GetRootTdb_Internal(/*IN*/ CliGlobals *cliGlobals,
                                 /*INOUT*/ char *roottdb_ptr,
                                 /*IN*/ int roottdb_len,
                                 /*INOUT*/ char *srcstr_ptr,
                                 /*IN*/ int srcstr_len,
                                 /*IN*/ SQLSTMT_ID *statement_id);

int SQLCLI_GetRootTdbSize_Internal(/*IN*/ CliGlobals *cliGlobals,
                                     /*INOUT*/ int *roottdb_size,
                                     /*INOUT*/ int *srcstr_size,
                                     /*IN*/ SQLSTMT_ID *statement_id);

int SQLCLI_GetCollectStatsType_Internal(
    /*IN*/ CliGlobals *cliGlobals,
    /*OUT*/ int *collectStatsType,
    /*IN*/ SQLSTMT_ID *statement_id);

int SQLCLI_GetTotalTcbSpace(/*IN*/ CliGlobals *cliGlobals,
                              /*IN*/ char *comTdb,
                              /*IN*/ char *otherInfo);

int SQLCLI_GetMPCatalog(/*IN*/ CliGlobals *cliGlobals,
                          /*IN*/ char *ANSINameObj,
                          /*OUT*/ char *MPObjName,
                          /*IN*/ int MPObjNameMaxLen,
                          /*OUT*/ int *MPObjNameLen,
                          /*OUT*/ char *MPCatalogName,
                          /*IN*/ int MPCatalogNameMaxLen,
                          /*OUT*/ int *MPCatalogNameLen);

int SQLCLI_GetPfsSize(/*IN*/ CliGlobals *cliGlobals,
                        /*OUT*/ int *pfsSize,
                        /*OUT*/ int *pfsCurUse,
                        /*OUT*/ int *pfsMaxUse);
int SQLCLI_CleanUpPfsResources(/*IN*/ CliGlobals *cliGlobals);

int SQLCLI_PerformTasks(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN*/ int tasks,
    /*IN*/ SQLSTMT_ID *statement_id,
    /*IN  OPTIONAL*/ SQLDESC_ID *input_descriptor,
    /*IN  OPTIONAL*/ SQLDESC_ID *output_descriptor,
    /*IN*/ int num_input_ptr_pairs,
    /*IN*/ int num_output_ptr_pairs,
    /*IN*/ int num_ap,
    /*IN*/ va_list ap,
    /*IN*/ SQLCLI_PTR_PAIRS input_ptr_pairs[],
    /*IN*/ SQLCLI_PTR_PAIRS output_ptr_pairs[]);

int SQLCLI_Prepare(/*IN*/ CliGlobals *cliGlobals,
                     /*IN*/ SQLSTMT_ID *statement_id,
                     /*IN*/ SQLDESC_ID *sql_source);

int SQLCLI_Prepare2(/*IN*/ CliGlobals *cliGlobals,
                      /*IN*/ SQLSTMT_ID *statement_id,
                      /*IN*/ SQLDESC_ID *sql_source,
                      /*INOUT*/ char *gencode_ptr,
                      /*IN*/ int gencode_len,
                      /*INOUT*/ int *ret_gencode_len,
                      /*INOUT*/ SQL_QUERY_COST_INFO *query_cost_info,
                      /*INOUT*/ SQL_QUERY_COMPILER_STATS_INFO *comp_stats_info,
                      /*INOUT*/ char *uniqueStmtId,
                      /*INOUT*/ int *uniqueStmtIdLen,
                      /*IN*/ int flags);

int SQLCLI_GetExplainData(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN*/ SQLSTMT_ID *statement_id,
    /*INOUT*/ char *explain_ptr,
    /*IN*/ int explain_len,
    /*INOUT*/ int *ret_explain_len);

int SQLCLI_StoreExplainData(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN*/ long *exec_start_utc_ts,
    /*IN*/ char *query_id,
    /*INOUT*/ char *explain_ptr,
    /*IN*/ int explain_len);

int SQLCLI_ResDescName(/*IN*/ CliGlobals *cliGlobals,
                         /*INOUT*/ SQLDESC_ID *statement_id,
                         /*IN OPTIONAL*/ SQLSTMT_ID *from_statement,
                         /* (SQLWHAT_DESC) *IN OPTIONAL*/ int what_desc);

int SQLCLI_ResStmtName(/*IN*/ CliGlobals *cliGlobals,
                         /*INOUT*/ SQLSTMT_ID *statement_id);

int SQLCLI_SetCursorName(/*IN*/ CliGlobals *cliGlobals,
                           /*IN*/ SQLSTMT_ID *statement_id,
                           /*IN*/ SQLSTMT_ID *cursor_name);

int SQLCLI_SetStmtAttr(/*IN*/ CliGlobals *cliGlobals,
                         /*IN*/ SQLSTMT_ID *statement_id,
                         /*IN* SQLATTR_TYPE */ int attrName,
                         /*IN OPTIONAL*/ int numeric_value,
                         /*IN OPTIONAL*/ char *string_value);

int SQLCLI_GetSessionAttr(/*IN*/ CliGlobals *cliGlobals,
                            /*IN SESSIONATTR_TYPE  */ int attrName,
                            /*OUT OPTIONAL*/ int *numeric_value,
                            /*OUT OPTIONAL*/ char *string_value,
                            /*IN OPTIONAL*/ int max_string_len,
                            /*OUT OPTIONAL*/ int *len_of_item);

int SQLCLI_SetSessionAttr(/*IN*/ CliGlobals *cliGlobals,
                            /*IN SESSIONATTR_TYPE*/ int attrName,
                            /*IN OPTIONAL*/ int numeric_value,
                            /*IN OPTIONAL*/ const char *string_value);

int SQLCLI_GetAuthID(CliGlobals *cliGlobals, const char *authName, int &authID);

int SQLCLI_GetAuthName(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN*/ int auth_id,
    /*OUT*/ char *string_value,
    /*IN*/ int max_string_len,
    /*OUT */ int &len_of_item);

int SQLCLI_GetDatabaseUserName(/*IN*/ CliGlobals *cliGlobals,
                                 /*IN*/ int user_id,
                                 /*OUT*/ char *string_value,
                                 /*IN*/ int max_string_len,
                                 /*OUT OPTIONAL*/ int *len_of_item);

int SQLCLI_GetDatabaseUserID(/*IN*/ CliGlobals *cliGlobals,
                               /*IN*/ char *string_value,
                               /*OUT*/ int *numeric_value);

int SQLCLI_GetAuthState(
    /*IN*/ CliGlobals *cliGlobals,
    /*OUT*/ int &authenticationType,
    /*OUT*/ bool &authorizationEnabled,
    /*OUT*/ bool &authorizationReady,
    /*OUT*/ bool &auditingEnabled);

int SQLCLI_GetRoleList(CliGlobals *cliGlobals, int &numEntries, int *&roleIDs, int *&granteeIDs);

int SQLCLI_ResetRoleList(
    /*IN*/ CliGlobals *cliGlobals);

int SQLCLI_GetUserAttrs(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN*/ const char *username,
    /*IN*/ const char *tenant_name,
    /*OUT*/ USERS_INFO *users_info,
    /*OUT*/ struct SQLSEC_AuthDetails *auth_details);

int SQLCLI_RegisterUser(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN*/ const char *username,
    /*IN*/ const char *config,
    /*OUT*/ USERS_INFO *usersInfo,
    /*OUT*/ struct SQLSEC_AuthDetails *auth_details);

int SQLCLI_GetAuthErrPwdCnt(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN*/ int userid,
    /*OUT*/ Int16 &errcnt);

int SQLCLI_UpdateAuthErrPwdCnt(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN*/ int userid,
    /*IN*/ Int16 errcnt,
    /*IN*/ bool reset);

int SQLCLI_GetAuthGracePwdCnt(
    /*IN*/ CliGlobals *,
    /*IN*/ int,
    /*OUT*/ Int16 &);

int SQLCLI_UpdateAuthGracePwdCnt(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN*/ int userid,
    /*IN*/ Int16 graceCnt = 0);

int SQLCLI_GetUniqueQueryIdAttrs(/*IN*/ CliGlobals *cliGlobals,
                                   /*IN*/ char *queryId,
                                   /*IN*/ int queryIdLen,
                                   /*IN*/ int no_of_attrs,
                                   /*INOUT*/ UNIQUEQUERYID_ATTR queryid_attrs[]);

int SQLCLI_GetStmtAttr(/*IN*/ CliGlobals *cliGlobals,
                         /*IN*/ SQLSTMT_ID *statement_id,
                         /*IN SQLATTR_TYPE  */ int attrName,
                         /*OUT OPTIONAL*/ int *numeric_value,
                         /*OUT OPTIONAL*/ char *string_value,
                         /*IN OPTIONAL*/ int max_string_len,
                         /*OUT OPTIONAL*/ int *len_of_item);

int SQLCLI_GetStmtAttrs(/*IN*/ CliGlobals *cliGlobals,
                          /*IN*/ SQLSTMT_ID *statement_id,
                          /*IN*/ int number_of_attrs,
                          /*INOUT*/ SQLSTMT_ATTR attrs[],
                          /*OUT OPTIONAL*/ int *num_returned);

int SQLCLI_SetDescEntryCount(/*IN*/ CliGlobals *cliGlobals,
                               /*IN*/ SQLDESC_ID *sql_descriptor,
                               /*IN*/ SQLDESC_ID *input_descriptor);

int SQLCLI_SetDescItem(/*IN*/ CliGlobals *cliGlobals,
                         /*IN*/ SQLDESC_ID *sql_descriptor,
                         /*IN*/ int entry,
                         /* (SQLDESC_ITEM_ID) *IN*/ int what_to_set,
                         /*IN OPTIONAL*/ Long numeric_value,
                         /*IN OPTIONAL*/ char *string_value);

int SQLCLI_SetDescItems(/*IN*/ CliGlobals *cliGlobals,
                          /*IN*/ SQLDESC_ID *sql_descriptor,
                          /*IN*/ SQLDESC_ITEM desc_items[],
                          /*IN*/ SQLDESC_ID *value_num_descriptor,
                          /*IN*/ SQLDESC_ID *input_descriptor);

int SQLCLI_SetDescItems2(/*IN*/ CliGlobals *cliGlobals,
                           /*IN*/ SQLDESC_ID *sql_descriptor,
                           /*IN*/ int no_of_desc_items,
                           /*IN*/ SQLDESC_ITEM desc_items[]);

int SQLCLI_SetDescPointers(/*IN*/ CliGlobals *cliGlobals,
                             /*IN*/ SQLDESC_ID *sql_descriptor,
                             /*IN*/ int starting_entry,
                             /*IN*/ int num_ptr_pairs,
                             /*IN*/ int num_ap,
                             /*IN*/ va_list ap,
                             /*IN*/ SQLCLI_PTR_PAIRS ptr_pairs[]);

int SQLCLI_SetRowsetDescPointers(CliGlobals *cliGlobals, SQLDESC_ID *desc_id, int rowset_size,
                                   int *rowset_status_ptr, int starting_entry, int num_quadruple_fields,
                                   int num_ap, va_list ap, SQLCLI_QUAD_FIELDS quad_fields[]);

int SQLCLI_GetRowsetNumprocessed(CliGlobals *cliGlobals, SQLDESC_ID *desc_id, int &rowset_nprocessed);

int SQLCLI_SwitchContext(/*IN*/ CliGlobals *cliGlobals,
                           /*IN*/ SQLCTX_HANDLE ctxt_handle,
                           /*OUT OPTIONAL*/ SQLCTX_HANDLE *prev_ctxt_handle,
                           /*IN */ int allowSwitchBackToDefault);

int SQLCLI_Xact(/*IN*/ CliGlobals *cliGlobals,
                  /*IN* (SQLTRANS_COMMAND) */ int command,
                  /*OUT OPTIONAL*/ SQLDESC_ID *transid_descriptor);

int SQLCLI_SetAuthID(CliGlobals *cliGlobals,      /*IN*/
                       const USERS_INFO &usersInfo, /*IN*/
                       const char *authToken,       /*IN*/
                       int authTokenLen,          /*IN*/
                       const char *slaName,         /*IN*/
                       const char *profileName,     /*IN*/
                       int resetAttributes);      /*IN*/

/* temporary functions -- for use by sqlcat simulator only */

int SQLCLI_AllocDescInt(/*IN*/ CliGlobals *cliGlobals,
                          /*INOUT*/ SQLDESC_ID *desc_id,
                          /*IN OPTIONAL*/ int max_entries);

int SQLCLI_GetDescEntryCountInt(/*IN*/ CliGlobals *cliGlobals,
                                  /*IN*/ SQLDESC_ID *sql_descriptor,
                                  /*OUT*/ int *num_entries);

int SQLCLI_SetDescEntryCountInt(/*IN*/ CliGlobals *cliGlobals,
                                  /*IN*/ SQLDESC_ID *sql_descriptor,
                                  /*IN*/ int num_entries);

int SQLCLI_MergeDiagnostics(/*IN*/ CliGlobals *cliGlobals,
                              /*INOUT*/ ComDiagsArea &newDiags);

int SQLCLI_GetRowsAffected(/*IN*/ CliGlobals *cliGlobals,
                             /*?*/ SQLSTMT_ID *statement_id,
                             /*?*/ long &rowsAffected);

// Function to get the length of the desc_items array
// returns the length if no error occurs, if error_occurred
// is 1 on return then return value indicates error
int SQLCLI_GetDescItemsEntryCount(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN*/ SQLDESC_ID *desc_id,
    /*IN*/ SQLDESC_ITEM desc_items[],
    /*IN*/ SQLDESC_ID *value_num_descriptor,
    /*IN*/ SQLDESC_ID *output_descriptor,
    /*Out*/ int &error_occurred);

int SQLCLI_SetParserFlagsForExSqlComp_Internal(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN*/ int flagbits);

int SQLCLI_ResetParserFlagsForExSqlComp_Internal(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN*/ int flagbits);

int SQLCLI_AssignParserFlagsForExSqlComp_Internal(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN*/ int flagbits);

int SQLCLI_GetParserFlagsForExSqlComp_Internal(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN*/ int &flagbits);

int SQLCLI_OutputValueIntoNumericHostvar(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN*/ SQLDESC_ID *output_descriptor,
    /*IN*/ int desc_entry,
    /*IN*/ int value);

int SQLCLI_SetEnviron_Internal(/*IN*/ CliGlobals *cliGlobals, char **envvars, int propagate);

int SQLCLI_SetCompilerVersion_Internal(CliGlobals *cliGlobals, short mxcmpVersion, char *nodeName);

int SQLCLI_GetCompilerVersion_Internal(CliGlobals *cliGlobals, short &mxcmpVersion, char *nodeName);

short SQLCLI_GETANSINAME(CliGlobals *cliGlobals, const char *guardianName, int guardianNameLen, char *ansiName,
                         int ansiNameMaxLength, int *returnedAnsiNameLength, char *nameSpace, char *partitionName,
                         int partitionNameMaxLength, int *returnedPartitionNameLength);

int SQLCLI_DecodeAndFormatKey(CliGlobals *cliGlobals,
                                void *RCB_Pointer_Addr,     // in
                                void *KeyAddr,              // in
                                int KeyLength,            // in
                                void *DecodedKeyBufAddr,    // in/out: where decoded key to be returned
                                void *FormattedKeyBufAddr,  // in/out: formatted key to be returned
                                int FormattedKeyBufLen,   // in
                                int *NeededKeyBufLen);    // out: required buffer size to be returned

int SQLCLI_GetPartitionKeyFromRow(CliGlobals *cliGlobals, void *RCB_Pointer_Addr, void *Row_Addr, int Row_Length,
                                    void *KeyAddr, int KeyLength);

int SQLCLI_SetErrorCodeInRTS(CliGlobals *cliGlobals, SQLSTMT_ID *statement_id, int sqlErrorCode);

int SQLCLI_SetSecInvalidKeys(CliGlobals *cliGlobals,
                               /* IN */ int numSiKeys,
                               /* IN */ SQL_QIKEY siKeys[]);

int SQLCLI_GetSecInvalidKeys(CliGlobals *cliGlobals,
                               /* IN */ long prevTimestamp,
                               /* IN/OUT */ SQL_QIKEY siKeys[],
                               /* IN */ int maxNumSiKeys,
                               /* IN/OUT */ int *returnedNumSiKeys,
                               /* IN/OUT */ long *maxTimestamp);

int SQLCLI_SetObjectEpochEntry(CliGlobals *cliGlobals,
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

int SQLCLI_LockObjectDDL(CliGlobals *cliGlobals,
                           /* IN */ const char *objectName,
                           /* IN */ short objectType);

int SQLCLI_UnLockObjectDDL(CliGlobals *cliGlobals,
                             /* IN */ const char *objectName,
                             /* IN */ short objectType);

int SQLCLI_LockObjectDML(CliGlobals *cliGlobals,
                           /* IN */ const char *objectName,
                           /* IN */ short objectType);

int SQLCLI_ReleaseAllDDLObjectLocks(CliGlobals *cliGlobals);
int SQLCLI_ReleaseAllDMLObjectLocks(CliGlobals *cliGlobals);

int SQLCLI_CheckLobLock(CliGlobals *cliGlobals,
                          /* IN */ char *lobLockId,
                          /*OUT */ NABoolean *found);
int SQLCLI_GetStatistics2(CliGlobals *cliGlobals,
                            /* IN */ short statsReqType,
                            /* IN */ char *statsReqStr,
                            /* IN */ int statsReqStrLen,
                            /* IN */ short activeQueryNum,
                            /* IN */ short statsMergeType,
                            /* OUT */ short *statsCollectType,
                            /* IN/OUT */ SQLSTATS_DESC sqlstats_desc[],
                            /* IN */ int max_stats_desc,
                            /* OUT */ int *no_returned_stats_desc);

int SQLCLI_GetStatisticsItems(CliGlobals *cliGlobals,
                                /* IN */ short statsReqType,
                                /* IN */ char *queryId,
                                /* IN */ int queryIdLen,
                                /* IN */ int no_of_stats_items,
                                /* IN/OUT */ SQLSTATS_ITEM sqlstats_items[]);

int SQLCLI_ProcessRetryQuery(CliGlobals *cliGlobals, SQLSTMT_ID *statement_id, int sqlcode, short afterPrepare,
                               short afterExec, short afterFetch, short afterCEFC);

int SQLCLI_LocaleToUTF8(CliGlobals *cliGlobals, int conv_charset, void *Input_Buffer_Addr,
                          int Input_Buffer_Length, void *Output_Buffer_Addr, int Output_Buffer_Length,
                          void **First_Untranslated_Char_Addr, int *Output_Data_Length, int add_null_at_end_Flag,
                          int *num_translated_char);

int SQLCLI_LocaleToUTF16(CliGlobals *cliGlobals, int conv_charset, void *Input_Buffer_Addr,
                           int Input_Buffer_Length, void *Output_Buffer_Addr, int Output_Buffer_Length,
                           void **First_Untranslated_Char_Addr, int *Output_Data_Length, int conv_flags,
                           int add_null_at_end_Flag, int *num_translated_char);

int SQLCLI_UTF8ToLocale(CliGlobals *cliGlobals, int conv_charset, void *Input_Buffer_Addr,
                          int Input_Buffer_Length, void *Output_Buffer_Addr, int Output_Buffer_Length,
                          void **First_Untranslated_Char_Addr, int *Output_Data_Length, int add_null_at_end_Flag,
                          int allow_invalids, int *num_translated_char, void *substitution_char_addr);

int SQLCLI_UTF16ToLocale(CliGlobals *cliGlobals, int conv_charset, void *Input_Buffer_Addr,
                           int Input_Buffer_Length, void *Output_Buffer_Addr, int Output_Buffer_Length,
                           void **First_Untranslated_Char_Addr, int *Output_Data_Length, int conv_flags,
                           int add_null_at_end_Flag, int allow_invalids, int *num_translated_char,
                           void *substitution_char_addr);

int SQLCLI_RegisterQuery(CliGlobals *cliGlobals, SQLQUERY_ID *queryId, int fragId, int tdbId, int explainTdbId,
                           short collectStatsType, int instNum, int tdbType, char *tdbName, int tdbNameLen);

int SQLCLI_DeregisterQuery(CliGlobals *cliGlobals, SQLQUERY_ID *queryId, int fragId);
// This method returns the pointer to the CLI ExStatistics area.
// The returned pointer is a read only pointer, its contents cannot be
// modified by the caller.
int SQLCLI_GetStatisticsArea_Internal(
    /* IN */ CliGlobals *cliGlobals,
    /* IN */ short statsReqType,
    /* IN */ char *statsReqStr,
    /* IN */ int statsReqStrLen,
    /* IN */ short activeQueryNum,
    /* IN */ short statsMergeType,
    /*INOUT*/ const ExStatisticsArea *&exStatsArea);

int SQLCLI_GetObjectEpochStats_Internal(
    /* IN */ CliGlobals *cliGlobals,
    /* IN */ const char *objectName,
    /* IN */ int objectNameLen,
    /* IN */ short cpu,
    /* IN */ bool locked,
    /*INOUT*/ ExStatisticsArea *&exStatsArea);

int SQLCLI_GetObjectLockStats_Internal(
    /* IN */ CliGlobals *cliGlobals,
    /* IN */ const char *objectName,
    /* IN */ int objectNameLen,
    /* IN */ short cpu,
    /*INOUT*/ ExStatisticsArea *&exStatsArea);

int SQLCLI_GetChildQueryInfo(CliGlobals *cliGlobals, SQLSTMT_ID *statement_id, char *uniqueQueryId,
                               int uniqueQueryIdMaxLen, int *uniqueQueryIdLen, SQL_QUERY_COST_INFO *query_cost_info,
                               SQL_QUERY_COMPILER_STATS_INFO *comp_stats_info);

int SQLCLI_SWITCH_TO_COMPILER_TYPE(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN*/ int cmpCntxtType);

int SQLCLI_SWITCH_TO_COMPILER(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN*/ void *cmpCntxt);

int SQLCLI_SWITCH_BACK_COMPILER(
    /*IN*/ CliGlobals *cliGlobals);

int SQLCLI_SEcliInterface(
    CliGlobals *cliGlobals,

    SECliQueryType qType,

    void **cliInterface, /* IN: if passed in and not null, use it.
                                                    OUT: if returned, save it and pass it back in */

    const char *inStrParam1 = NULL, const char *inStrParam2 = NULL, int inIntParam1 = -1, int inIntParam2 = -1,

    char **outStrParam1 = NULL, char **outStrParam2 = NULL, int *outIntParam1 = NULL

);

int SQLCLI_SeqGenCliInterface(CliGlobals *cliGlobals,

                                void **cliInterface, /* IN: if passed in and not null, use it.
                                                        OUT: if returned, save it and pass it back in */

                                void *seqGenAttrs);

int SQLCLI_OrderSeqXDCCliInterface(CliGlobals *cliGlobals,
                                     void **cliInterface, /* IN: if passed in and not null, use it.
                                                              OUT: if returned, save it and pass it back in */
                                     void *seqGenAttrs, long endValue);

int SQLCLI_GetRoutine(
    /* IN */ CliGlobals *cliGlobals,
    /* IN */ const char *serializedInvocationInfo,
    /* IN */ int invocationInfoLen,
    /* IN */ const char *serializedPlanInfo,
    /* IN */ int planInfoLen,
    /* IN */ int language,
    /* IN */ int paramStyle,
    /* IN */ const char *externalName,
    /* IN */ const char *containerName,
    /* IN */ const char *externalPath,
    /* IN */ const char *librarySqlName,
    /* OUT */ int *handle);

int SQLCLI_InvokeRoutine(
    /* IN */ CliGlobals *cliGlobals,
    /* IN */ int handle,
    /* IN */ int phaseEnumAsInt,
    /* IN */ const char *serializedInvocationInfo,
    /* IN */ int invocationInfoLen,
    /* OUT */ int *invocationInfoLenOut,
    /* IN */ const char *serializedPlanInfo,
    /* IN */ int planInfoLen,
    /* IN */ int planNum,
    /* OUT */ int *planInfoLenOut,
    /* IN */ char *inputRow,
    /* IN */ int inputRowLen,
    /* OUT */ char *outputRow,
    /* IN */ int outputRowLen);

int SQLCLI_GetRoutineInvocationInfo(
    /* IN */ CliGlobals *cliGlobals,
    /* IN */ int handle,
    /* IN/OUT */ char *serializedInvocationInfo,
    /* IN */ int invocationInfoMaxLen,
    /* OUT */ int *invocationInfoLenOut,
    /* IN/OUT */ char *serializedPlanInfo,
    /* IN */ int planInfoMaxLen,
    /* IN */ int planNum,
    /* OUT */ int *planInfoLenOut);

int SQLCLI_PutRoutine(
    /* IN */ CliGlobals *cliGlobals,
    /* IN */ int handle);

int SQLCLI_LoadTrafMetadataInCache(
    /* IN */ CliGlobals *cliGlobals);

int SQLCLI_LoadTrafMetadataIntoSharedCache(
    /* IN */ CliGlobals *cliGlobals);

int SQLCLI_LoadTrafDataIntoSharedCache(
    /* IN */ CliGlobals *cliGlobals);

int SQLCLI_GetTransactionId(
    /*IN*/ CliGlobals *cliGlobals,
    /*OUT*/ long *trans_id);

int SQLCLI_GetStatementHeapSize(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN*/ SQLSTMT_ID *statement_id,
    /*OUT*/ long *heapSize);
}  // end extern "C"

class StrTarget {
 private:
  CollHeap *heap_;
  char *str_;
  int externalCharset_;     // these are really enum CharInfo::CharSet,
  int internalCharset_;     // defined in common/charinfo.h
  int cnvExternalCharset_;  // these are really enum cnv_charset,
  int cnvInternalCharset_;  // defined in common/csconvert.h
 public:
  StrTarget();
  StrTarget(Descriptor *desc, int entry);
  ~StrTarget() {
    if (str_) heap_->deallocateMemory(str_);
  }
  void init(Descriptor *desc, int entry);
  void init(char *source, int sourceLen, int sourceType, int externalCharset, int internalCharset,
            CollHeap *heap, ComDiagsArea *&diagsArea);
  char *getStr() { return str_; }
  int getIntCharSet() { return internalCharset_; }
};

#ifdef ENABLE_VA_LIST
#define VA_LIST_NULL va_list()
#else
#define VA_LIST_NULL 0
#endif

#endif /* CLI_H */
