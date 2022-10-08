/* -*-C++-*-

 *****************************************************************************
 *
 * File:         Cli.C
 * Description:  CLI procedures body.
 *
 * Created:      7/10/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "cli/Cli.h"

#include <stdarg.h>
#include <time.h>

#include "ComRegAPI.h"
#include "ExExplain.h"
#include "executor/ExRsInfo.h"
#include "LmLangManager.h"
#include "LmLangManagerC.h"
#include "LmLangManagerJava.h"
#include "LmRoutine.h"
#include "arkcmp/CmpContext.h"
#include "cli/ExSqlComp.h"
#include "cli/sql_id.h"
#include "cli/cli_stdh.h"
#include "comexe/CmpMessage.h"
#include "comexe/ComTdb.h"
#include "comexe/ComTdbSplitTop.h"
#include "comexe/ExplainTuple.h"
#include "common/ComCextdecs.h"
#include "common/ComRtUtils.h"
#include "common/ComSqlId.h"
#include "common/NAString.h"
#include "common/NLSConversion.h"
#include "common/Platform.h"
#include "common/csconvert.h"
#include "common/feerrors.h"
#include "dtm/tm.h"
#include "executor/ex_exe_stmt_globals.h"
#include "executor/ex_frag_rt.h"
#include "executor/ex_root.h"
#include "executor/ExExeUtil.h"
#include "executor/ExStats.h"
#include "executor/ex_transaction.h"
#include "exp/ExpLOBinterface.h"
#include "exp/exp_clause_derived.h"
#include "exp/exp_expr.h"
#include "exp/exp_stdh.h"
#include "exp/exp_function.h"
#include "seabed/ms.h"
#include "sqlcomp/CmpDDLCatErrorCodes.h"
#include "sqlmxevents/logmxevent.h"

#define DISPLAY_DONE_WARNING 1032
extern int getTotalTcbSpace(char *tdb, char *otherInfo, char *parentMemory);

StrTarget::StrTarget()
    : heap_(NULL),
      str_(NULL),
      externalCharset_(CharInfo::UnknownCharSet),
      internalCharset_(CharInfo::UnknownCharSet),
      cnvExternalCharset_(cnv_UnknownCharSet),
      cnvInternalCharset_(cnv_UnknownCharSet) {}
StrTarget::StrTarget(Descriptor *desc, int entry)
    : heap_(NULL),
      str_(NULL),
      externalCharset_(CharInfo::UnknownCharSet),
      internalCharset_(CharInfo::UnknownCharSet),
      cnvExternalCharset_(cnv_UnknownCharSet),
      cnvInternalCharset_(cnv_UnknownCharSet) {
  init(desc, entry);
}
void StrTarget::init(Descriptor *desc, int entry) {
  int sourceLen, sourceType, externalCharset, internalCharset;
  CollHeap *heap = desc->getContext()->exCollHeap();
  ComDiagsArea *diagsArea = desc->getContext()->getDiagsArea();
  char *source = desc->getVarData(entry);
  desc->getDescItem(entry, SQLDESC_TYPE_FS, &sourceType, 0, 0, 0, 0);
  desc->getDescItem(entry, SQLDESC_OCTET_LENGTH, &sourceLen, 0, 0, 0, 0);
  desc->getDescItem(1, SQLDESC_CHAR_SET, &externalCharset, 0, 0, 0, 0);

  // If ISO_MAPPING is equal to the charset in the descriptor,
  // then process as usual.  Otherwise, convert
  // the string from the input charset to ISO_MAPPING (on NSK platform)
  // -- On SeaQuest platform, always convert to UTF8 (or keep as is if
  // already in UTF8 or UCS2/UTF16).
  if (externalCharset == CharInfo::UCS2)
    internalCharset = CharInfo::UCS2;
  else
    internalCharset = CharInfo::UTF8;

  init(source, sourceLen, sourceType, externalCharset, internalCharset, heap, diagsArea);
}

void StrTarget::init(char *source, int sourceLen, int sourceType, int externalCharset, int internalCharset,
                     CollHeap *heap, ComDiagsArea *&diagsArea) {
  externalCharset_ = externalCharset;
  internalCharset_ = internalCharset;
  heap_ = heap;

  cnvExternalCharset_ = convertCharsetEnum(externalCharset_);
  cnvInternalCharset_ = convertCharsetEnum(internalCharset_);

  if (sourceType < REC_MIN_CHARACTER || sourceType > REC_MAX_CHARACTER) return;

  // How many bytes will we need after conversion?
  // Add one char length for the NULL terminator
  int intStrLen = CharInfo::getMaxConvertedLenInBytes((CharInfo::CharSet)externalCharset_, sourceLen,
                                                      (CharInfo::CharSet)internalCharset_) +
                  CharInfo::minBytesPerChar((CharInfo::CharSet)internalCharset_);
  str_ = (char *)heap_->allocateMemory(intStrLen);

  short retCode = convDoIt(source, sourceLen, (short)sourceType, 0, externalCharset_, str_, intStrLen,
                           CharInfo::getFSTypeANSIChar((CharInfo::CharSet)internalCharset_), 0, internalCharset_, NULL,
                           0, heap_, &diagsArea);
  if (retCode != ex_expr::EXPR_OK) {
    heap_->deallocateMemory(str_);
    str_ = NULL;
  }
}

static inline NABoolean isERROR(int retcode) { return (retcode < 0); }

static inline NABoolean isWARNING(int retcode) {
  // 100 is EOF, not WARNING
  return ((retcode > 0) && (retcode != SQL_EOF));
}

static inline NABoolean isSUCCESS(int retcode) { return (retcode == 0); }

static inline NABoolean isEOF(int retcode) { return (retcode == 100); }

static int SQLCLI_ReturnCode(ContextCli *context, int retcode) {
  return (context != NULL && isERROR(retcode) ? context->diags().mainSQLCODE() : retcode);
}

// Move this method to Statement class. TBD.
static int SQLCLI_Prepare_Setup_Pre(ContextCli &currContext, Statement *stmt, int flags) {
  long compStartTime = NA_JulianTimestamp();
  StmtStats *stmtStats = stmt->getStmtStats();
  ExMasterStats *masterStats;

  if (stmtStats != NULL) {
    masterStats = stmtStats->getMasterStats();
    if (masterStats != NULL) {
      masterStats->setElapsedStartTime(compStartTime);
      masterStats->setCompStartTime(compStartTime);
      masterStats->setSlaName(currContext.getSlaName());
      masterStats->setProfileName(currContext.getProfileName());

      if (flags & PREPARE_STANDALONE_QUERY)  // standalone query
        masterStats->setIsPrepAndExec(TRUE);
      else {
        masterStats->setIsPrepAndExec(FALSE);
        masterStats->setIsPrepare(TRUE);
      }
    }
  }

  return 0;
}

static int SQLCLI_Prepare_Setup_Post(ContextCli &currContext, Statement *stmt, int flags) {
  StmtStats *stmtStats = stmt->getStmtStats();
  long compEndTime = stmt->getCompileEndTime();
  ExMasterStats *masterStats;
  ex_root_tdb *rootTdb = NULL;
  if (stmtStats != NULL) {
    if ((masterStats = stmtStats->getMasterStats()) != NULL) {
      masterStats->setCompEndTime(compEndTime);
      rootTdb = (stmt ? stmt->getRootTdb() : NULL);
      if (rootTdb) {
        stmtStats->getMasterStats()->setInvalidationKeys(currContext.getCliGlobals(), rootTdb->getSikInfo(),
                                                         rootTdb->getNumObjectUIDs(), rootTdb->getObjectUIDs());
      }

      if (rootTdb != NULL) masterStats->setQueryHash(rootTdb->getQueryHash());

      if (rootTdb != NULL && rootTdb->getCollectStatsType() != ComTdb::NO_STATS) {
        masterStats->setCollectStatsType(rootTdb->getCollectStatsType());
        if (rootTdb->getQueryCostInfo()) masterStats->queryCostInfo() = *(rootTdb->getQueryCostInfo());
        if (rootTdb->getCompilerStatsInfo()) masterStats->compilerStatsInfo() = *(rootTdb->getCompilerStatsInfo());

        if (rootTdb->qCacheInfoIsClass() && rootTdb->qcInfo()) {
          masterStats->setCompilerCacheHit(rootTdb->qcInfo()->cacheWasHit());
        }
        masterStats->setQueryType((Int16)rootTdb->getQueryType(), rootTdb->getSubqueryType());
        masterStats->exePriority() = (short)currContext.getCliGlobals()->myPriority();

        if (currContext.getSessionDefaults()->getEspPriority() > 0)
          masterStats->espPriority() = (short)currContext.getSessionDefaults()->getEspPriority();
        else if (currContext.getSessionDefaults()->getEspPriorityDelta() != 0)
          masterStats->espPriority() = (short)(stmtStats->getMasterStats()->exePriority() +
                                               currContext.getSessionDefaults()->getEspPriorityDelta());
        else
          masterStats->espPriority() = masterStats->exePriority();

        if (currContext.getSessionDefaults()->getMxcmpPriority() > 0)
          masterStats->cmpPriority() = (short)currContext.getSessionDefaults()->getMxcmpPriority();
        else if (currContext.getSessionDefaults()->getMxcmpPriorityDelta() != 0)
          masterStats->cmpPriority() =
              (short)(masterStats->exePriority() + currContext.getSessionDefaults()->getMxcmpPriorityDelta());
        else
          masterStats->cmpPriority() = masterStats->exePriority();

        masterStats->fixupPriority() = (short)currContext.getSessionDefaults()->getEspFixupPriorityDelta();

        // detect if this query is sequential at top. If
        // the root doesn't have an ESP exchange as its child,
        // then it is sequential at top. A FirstN operator could
        // also be between root and ESP exchange, that is
        // not considered sequential.
        UInt32 numOfRootEsps = 0;
        const ComTdb *st = NULL;
        if (((rootTdb->getChild(0)->getNodeType() == ComTdb::ex_FIRST_N) &&
             ((st = rootTdb->getChild(0)->getChild(0))->getNodeType() == ComTdb::ex_SPLIT_TOP) &&
             (rootTdb->getChild(0)->getChild(0)->getChild(0)->getNodeType() == ComTdb::ex_SEND_TOP)) ||
            (((st = rootTdb->getChild(0))->getNodeType() == ComTdb::ex_SPLIT_TOP) &&
             (rootTdb->getChild(0)->getChild(0)->getNodeType() == ComTdb::ex_SEND_TOP))) {
          numOfRootEsps = ((ComTdbSplitTop *)st)->getBottomNumParts();
        }

        masterStats->numOfRootEsps() = numOfRootEsps;
        if (rootTdb->transactionReqd()) masterStats->setXnReqd(TRUE);

        masterStats->setElapsedEndTime(compEndTime);
        ExStatisticsArea *newStats = new (currContext.exHeap())
            ExStatisticsArea(currContext.exHeap(), 0, rootTdb->getCollectStatsType(), rootTdb->getCollectStatsType());
        newStats->setStatsEnabled(TRUE);
        ExMasterStats *ems = new (currContext.exHeap()) ExMasterStats(currContext.exHeap());
        ems->copyContents(masterStats);
        ems->setCollectStatsType(rootTdb->getCollectStatsType());
        newStats->setMasterStats(ems);
        currContext.setStatsArea(newStats, TRUE, FALSE);
      } else {
        masterStats->setCollectStatsType(ComTdb::NO_STATS);
        if (rootTdb != NULL) {
          if (rootTdb->getCompilerStatsInfo()) masterStats->compilerStatsInfo() = *(rootTdb->getCompilerStatsInfo());
          masterStats->setQueryType((Int16)rootTdb->getQueryType(), rootTdb->getSubqueryType());
        }
      }
    }
  }
  return 0;
}

//
// A helper function to check whether SQL access is currently allowed.
// The UDR server is the only SQL application that disables SQL access
// from time to time. This is done in order to enforce a policy where
// a UDR registered with the NO SQL attribute is not allowed to access
// SQL/MX.
//
static int CheckNOSQLAccessMode(CliGlobals &cliGlobals) {
  // rare error condition
  if (!cliGlobals.sqlAccessAllowed()) {
    cliGlobals.setUdrAccessModeViolation(TRUE);
    ContextCli *context = cliGlobals.currContext();
    if (context) {
      ComDiagsArea &diags = context->diags();
      diags << DgSqlCode(-CLI_NO_SQL_ACCESS_MODE_VIOLATION);
    }
    return -CLI_NO_SQL_ACCESS_MODE_VIOLATION;
  }
  return SUCCESS;
}

static int CliPrologue(CliGlobals *cliGlobals, const SQLMODULE_ID *module) {
  int retcode;

  ContextCli *context = cliGlobals->currContext();
  ComDiagsArea &diags = context->diags();

  // If this is not a recursive CLI call, inherit the current
  // process transid into this context. (We don't want to
  // inherit the current process transid if this is a recursive
  // CLI call. A recursive CLI call should always be done in
  // the transid of its calling context, and the process transid
  // might be different. This can happen, for example, if we are
  // in a recursive CLI call underneath FILE_COMPLETE_.)
  int numOfCliCalls = context->getNumOfCliCalls();
  if ((numOfCliCalls == 1) || ((numOfCliCalls == 2) && (cliGlobals->isESPProcess()))) {
    ExTransaction *exTransaction = context->getTransaction();
    if (exTransaction != NULL) {
      retcode = exTransaction->inheritTransaction();
      if (isERROR(retcode)) {
        diags << DgSqlCode(-CLI_BEGIN_TRANSACTION_ERROR);
        return ERROR;
      }
    }
  }

  if ((context->getVersionOfCompiler() != COM_VERS_COMPILER_VERSION) && (numOfCliCalls > 1)) {
    short index = 0;
    cliGlobals->setSavedVersionOfCompiler(context->getVersionOfCompiler());
    context->setOrStartCompiler(COM_VERS_COMPILER_VERSION, NULL, index);
  }

  // Initialize session defaults.
  // if session defaults have not been read from the default table,
  // read them now.
  if (context->getSessionDefaults() == NULL) {
    retcode = context->initializeSessionDefaults();
    if (isERROR(retcode)) return SQLCLI_ReturnCode(cliGlobals->currContext(), retcode);
  }

  return SUCCESS;
}

static int CliEpilogue(CliGlobals *cliGlobals, SQLSTMT_ID *currentSqlStmt, int inRetcode = 0) {
  ContextCli *context = cliGlobals->currContext();

  // Validate the current process transid only if this is not
  // a recursive CLI call. (It is legitimate, for example,
  // for the process to not have a transid, or to have a
  // different transid than the current context, if we are
  // in a recursive CLI call inside FILE_COMPLETE_.)

  int numOfCliCalls = context->getNumOfCliCalls();
  if (numOfCliCalls == 1) {
    short retcode = context->getTransaction()->validateTransaction();

    if (isERROR(retcode)) {
      ComDiagsArea &diags = context->diags();
      return diags.mainSQLCODE();
    }
  }
#if 0
  if ((cliGlobals->getSavedVersionOfCompiler() != context->getVersionOfCompiler()) && (numOfCliCalls > 1 ))
    {
      short index;
      cliGlobals->setSavedVersionOfCompiler(context->getVersionOfCompiler());
      context->setOrStartCompiler(cliGlobals->getSavedVersionOfCompiler(),NULL, index);
    }
#endif
  if (inRetcode == SQL_EOF) return SQL_EOF;

  if (isERROR(inRetcode)) {
    // if priority of master was changed for execution,
    // switch it back to its original priority.
    // Do this only for root cli level.
    if ((numOfCliCalls == 1) && (cliGlobals->priorityChanged())) {
      ComRtSetProcessPriority(cliGlobals->myPriority(), FALSE);
      cliGlobals->setPriorityChanged(FALSE);
    }

    return SQLCLI_ReturnCode(cliGlobals->currContext(), inRetcode);
  }

  // if there's stuff in diags, it's a warning, and we need to
  // pass the warning code out
  if (context->diags().getNumber()) return context->diags().mainSQLCODE();

  return SQLCLI_ReturnCode(context, inRetcode);
}

// Set the pointers for the host variable and its indicator host variable in
// pairs. Note, we are setting only the addresses for the host variables in
// the first row of the Rowset. The other are implicitely set by the binding
// style.
int local_SetDescPointers(/*IN*/ Descriptor *desc,
                          /*IN*/ int starting_entry,
                          /*IN*/ int num_ptr_pairs,
                          /*IN*/ int num_ap,
                          /*IN*/ va_list *ap,
                          /*IN*/ SQLCLI_PTR_PAIRS ptr_pairs[]) {
  ContextCli *context = desc->getContext();

  if ((!desc) || (num_ptr_pairs < 0) || (starting_entry <= 0) ||
      ((num_ptr_pairs + starting_entry - 1) > desc->getUsedEntryCount())) {
    ComDiagsArea &diags = context->diags();

    diags << DgSqlCode(-CLI_INTERNAL_ERROR);
    return SQLCLI_ReturnCode(context, -CLI_INTERNAL_ERROR);
  }

  for (int entry = starting_entry, i = 0; entry < starting_entry + num_ptr_pairs; entry++, i++) {
    // need to re-order this to avoid possible problems with not setting
    // the data area (i.e. VAR_PTR) last.
    char *vp, *ip;
    if (num_ap) {
      vp = va_arg(*ap, char *);
      ip = va_arg(*ap, char *);
    } else {
      vp = (char *)ptr_pairs[i].var_ptr;
      ip = ptr_pairs[i].ind_ptr == (void *)-1 ? 0 : (char *)ptr_pairs[i].ind_ptr;
    }

    if (ip && (context->boundsCheckMemory(ip, desc->getIndLength(entry))) ||
        (context->boundsCheckMemory(vp, desc->getVarDataLength(entry))))
      return SQLCLI_ReturnCode(context, -CLI_USER_MEMORY_IN_EXECUTOR_SEGMENT);

    desc->setDescItem(entry, SQLDESC_IND_PTR, (Long)ip, 0);
    desc->setDescItem(entry, SQLDESC_VAR_PTR, (Long)vp, 0);
  }

  return SUCCESS;
}

// Some CLI calls deal with numeric host variables without there being
// an input/output expression. So far, the CLI assumed those variables
// to be "long" C variables. The methods here are a slight improvement
// but no perfect solution. They handle a few more cases.

// Retrieve information about the pointers and lengths of a numeric host var
// for the first row of the RowSet. To get the addresses for other row sets,
// the member function, getDescPointers should be used.

static int getNumericHostVarInfo(Descriptor *desc, int desc_entry,
                                 // output parameters
                                 void *&hv_ptr, int &hv_length, int &hv_type, void *&ind_ptr, int &ind_length,
                                 int &ind_type) {
  int retcode = desc->getDescItem(desc_entry, SQLDESC_VAR_PTR, (int *)&hv_ptr, NULL, 0, NULL, 0);
  if (retcode != 0) return SQLCLI_ReturnCode(desc->getContext(), retcode);

  retcode = desc->getDescItem(desc_entry, SQLDESC_LENGTH, (int *)&hv_length, NULL, 0, NULL, 0);
  if (retcode != 0) return SQLCLI_ReturnCode(desc->getContext(), retcode);

  retcode = desc->getDescItem(desc_entry, SQLDESC_TYPE_FS, (int *)&hv_type, NULL, 0, NULL, 0);
  if (retcode != 0) return SQLCLI_ReturnCode(desc->getContext(), retcode);

  retcode = desc->getDescItem(desc_entry, SQLDESC_IND_PTR, (int *)&ind_ptr, NULL, 0, NULL, 0);
  if (retcode != 0) return SQLCLI_ReturnCode(desc->getContext(), retcode);

  // the host variable must have a scale of 0
  int hv_scale;
  retcode = desc->getDescItem(desc_entry, SQLDESC_SCALE, (int *)&hv_scale, NULL, 0, NULL, 0);
  if (retcode != 0) return SQLCLI_ReturnCode(desc->getContext(), retcode);
  // $$$$ Enable this after scale got initialized
  // if (hv_scale != 0)
  //   return -1;

  if (ind_ptr) {
    retcode = desc->getDescItem(desc_entry, SQLDESC_IND_TYPE, (int *)&ind_type, NULL, 0, NULL, 0);
    if (retcode != 0) return SQLCLI_ReturnCode(desc->getContext(), retcode);

    switch (ind_type) {
      case REC_BIN16_SIGNED:
      case REC_BIN16_UNSIGNED:
        ind_length = 2;
        break;
      case REC_BIN32_SIGNED:
      case REC_BIN32_UNSIGNED:
        ind_length = 4;
        break;
      case REC_BIN64_SIGNED:
      case REC_BIN64_UNSIGNED:
        ind_length = 8;
        break;
      default:
        retcode = -1;
    }
  }
  return SQLCLI_ReturnCode(desc->getContext(), retcode);

  /* RS -- to be tested,debugged and re-enabled
  long retcode;
  short nullable;

  retcode = desc->getDescItemMainVarInfo(desc_entry,
                                         nullable,
                                         hv_type,
                                         hv_length,
                                         (void *)&hv_ptr,
                                         ind_type,
                                         ind_length,
                                         (void *)&ind_ptr);
  return SQLCLI_ReturnCode(desc->getContext(),retcode);
  */
}

static int InputValueFromNumericHostvar(Descriptor *desc, int desc_entry, int &value) {
  void *hvPtr;
  int hvLength;
  int hvType;
  void *indPtr;
  int indLength;
  int indType;
  ex_expr::exp_return_type expRetcode;
  CollHeap *heap;
  ComDiagsArea *diagsArea;

  heap = desc->getContext()->exCollHeap();
  diagsArea = desc->getContext()->getDiagsArea();

  int retcode = getNumericHostVarInfo(desc, desc_entry, hvPtr, hvLength, hvType, indPtr, indLength, indType);
  if (retcode) return SQLCLI_ReturnCode(desc->getContext(), retcode);

  expRetcode = convDoIt((char *)hvPtr, hvLength, (short)hvType, 0, 0, (char *)&value, (int)sizeof(value),
                        REC_BIN32_SIGNED, 0, 0, NULL, 0, heap, &diagsArea);
  if (expRetcode != ex_expr::EXPR_OK) return ERROR;
  if (indPtr) {
    // values coming in should not be NULL, just make
    // sure nobody passes in a NULL
    int userIndicatorVal;

    expRetcode = convDoIt((char *)indPtr, indLength, (short)indType, 0, 0, (char *)&userIndicatorVal,
                          (int)sizeof(userIndicatorVal), REC_BIN32_SIGNED, 0, 0, NULL, 0, heap, &diagsArea);

    if (expRetcode != ex_expr::EXPR_OK) return ERROR;
    if (userIndicatorVal) return ERROR;  // user specified NULL or junk
  }

  return (SUCCESS);
}

static int OutputValueIntoNumericHostvar(Descriptor *desc, int desc_entry, int value) {
  void *hvPtr;
  int hvLength;
  int hvType;
  void *indPtr;
  int indLength;
  int indType;
  ex_expr::exp_return_type expRetcode;
  CollHeap *heap;
  ComDiagsArea *diagsArea;
  ContextCli *context;

  context = desc->getContext();
  heap = context->exCollHeap();
  diagsArea = context->getDiagsArea();

  int retcode = getNumericHostVarInfo(desc, desc_entry, hvPtr, hvLength, hvType, indPtr, indLength, indType);
  if (retcode) return ERROR;

  if (hvType >= REC_MIN_NUMERIC && hvType <= REC_MAX_NUMERIC) {
    expRetcode = convDoIt((char *)&value, (int)sizeof(value), REC_BIN32_SIGNED, 0, 0, (char *)hvPtr, hvLength,
                          (short)hvType, 0, 0, NULL, 0, heap, &diagsArea);

    if (expRetcode != ex_expr::EXPR_OK) return ERROR;
  } else {
    ComDiagsArea &diags = context->diags();

    diags << DgSqlCode(-3164);
    return ERROR;
  }
  if (indPtr) {
    // move a zero into the indicator host variable
    int nonNull = 0;
    expRetcode = convDoIt((char *)&nonNull, (int)sizeof(nonNull), REC_BIN32_SIGNED, 0, 0, (char *)indPtr, indLength,
                          (short)indType, 0, 0, NULL, 0, heap, &diagsArea);
    if (expRetcode != ex_expr::EXPR_OK) return ERROR;
  }

  return SUCCESS;
}

int SQLCLI_GetDiskMaxSize(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN*/ char *volname,
    /*OUT*/ long *totalCapacity,
    /*OUT*/ long *totalFreespace) {
  int retcode = 0;
  // create initial context, if first call, don't add module because
  // that would cause infinite recursion
  retcode = CliPrologue(cliGlobals, NULL);
  if (isERROR(retcode)) return retcode;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();
  return retcode;
}

int SQLCLI_GetListOfDisks(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN/OUT*/ char *diskBuf,
    /* OUT */ int *numTSEs,
    /*OUT */ int *maxTSELength,
    /*IN/OUT */ int *diskBufLen) {
  int retcode = 0;
  int numPrimaries = 0;
  MS_Mon_Process_Info_Type *tseInfo = NULL;
  // create initial context, if first call, don't add module because
  // that would cause infinite recursion
  retcode = CliPrologue(cliGlobals, NULL);
  if (isERROR(retcode)) return retcode;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();
  NAHeap *heap = cliGlobals->currContext()->exHeap();
  *maxTSELength = MS_MON_MAX_PROCESS_NAME;

  retcode = msg_mon_get_process_info_type(MS_ProcessType_TSE, numTSEs, 0, NULL);
  if (retcode) {
    diags << DgSqlCode(-EXE_ERROR_FROM_FS2) << DgNskCode(retcode) << DgString0("msg_mon_get_process_info_type");
    retcode = -EXE_ERROR_FROM_FS2;
  }
  tseInfo = new (heap) MS_Mon_Process_Info_Type[*numTSEs];
  retcode = msg_mon_get_process_info_type(MS_ProcessType_TSE, numTSEs, *numTSEs, tseInfo);

  if (retcode) {
    diags << DgSqlCode(-EXE_ERROR_FROM_FS2) << DgNskCode(retcode) << DgString0("msg_mon_get_process_info_type");
    retcode = -EXE_ERROR_FROM_FS2;
  }

  if (*diskBufLen < *numTSEs * MS_MON_MAX_PROCESS_NAME) {
    diags << DgSqlCode(-CLI_BUFFER_TOO_SMALL);
    retcode = -CLI_BUFFER_TOO_SMALL;
    *diskBufLen = (*numTSEs * MS_MON_MAX_PROCESS_NAME) + MS_MON_MAX_PROCESS_NAME;
  } else {
    // Fill in the TSE disk name information in the output param.
    str_pad(diskBuf, *numTSEs * MS_MON_MAX_PROCESS_NAME, ' ');
    for (short j = 0, i = 0; i < *numTSEs; i++) {
      if (!tseInfo[i].backup) {
        str_cpy((char *)&diskBuf[j], tseInfo[i].process_name, MS_MON_MAX_PROCESS_NAME, '\0');
        numPrimaries++;
        j += MS_MON_MAX_PROCESS_NAME;
      }
    }
    *numTSEs = numPrimaries;
  }

  if (isERROR(retcode)) return SQLCLI_ReturnCode(&currContext, retcode);

  return retcode;
}

int SQLCLI_AllocDesc(/*IN*/ CliGlobals *cliGlobals,
                     /*INOUT*/ SQLDESC_ID *desc_id,
                     /*IN  OPTIONAL*/ SQLDESC_ID *input_descriptor) {
  int retcode;

  // create initial context, if first call, and add module, if any.
  retcode = CliPrologue(cliGlobals, desc_id->module);
  if (isERROR(retcode)) return retcode;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  int maxEntries = 0L;

  if (input_descriptor) {
    Descriptor *input_desc = currContext.getDescriptor(input_descriptor);

    /* descriptor must exist */
    if (!input_desc) {
      diags << DgSqlCode(-CLI_DESC_NOT_EXISTS);
      return SQLCLI_ReturnCode(&currContext, -CLI_DESC_NOT_EXISTS);
    }

    retcode = InputValueFromNumericHostvar(input_desc, 1, maxEntries);
    if (isERROR(retcode)) return SQLCLI_ReturnCode(&currContext, retcode);
  } else {
    // no input descriptor was passed in. Allocate the
    // max entries allowed.
    maxEntries = 500;
  }
  if (maxEntries < 1) {
    diags << DgSqlCode(-CLI_DATA_OUTOFRANGE) << DgInt0(maxEntries);

    return SQLCLI_ReturnCode(&currContext, -CLI_DATA_OUTOFRANGE);
  }

  /* allocate a new descriptor in the current context.         */
  /* return the descriptor handle in desc_id, if name mode     */
  /* is "desc_handle"                                          */
  retcode = currContext.allocateDesc(desc_id, maxEntries);
  if (isERROR(retcode)) return SQLCLI_ReturnCode(&currContext, retcode);

  return CliEpilogue(cliGlobals, NULL);
}

#define MULTIPLE_CURSORS_PER_STATEMENT 1
#define KLUGE_CURSOR                   1

#if defined(MULTIPLE_CURSORS_PER_STATEMENT) && defined(KLUGE_CURSOR)
#include "comexe/ComQueue.h"
#endif

int SQLCLI_AllocStmt(/*IN*/ CliGlobals *cliGlobals,
                     /*INOUT*/ SQLSTMT_ID *statement_id,
                     /*IN  OPTIONAL*/ SQLSTMT_ID *cloned_statement) {
  int retcode;

  // create initial context, if first call, and add module, if any.
  retcode = CliPrologue(cliGlobals, statement_id->module);
  if (isERROR(retcode)) return retcode;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  if (!cloned_statement) {
    /* allocate a new statement entry in the current context.    */
    /* return the statement handle in statement_id, if name mode */
    /* is "stmt_handle"                                          */

    retcode = currContext.allocateStmt(statement_id, Statement::DYNAMIC_STMT);
    if (isERROR(retcode)) return SQLCLI_ReturnCode(&currContext, retcode);
  } else {
    if (
        // cloned statement must be a statement, not a cursor
        // so it's an error if it's not stmt_name or stmt_handle...
        ((cloned_statement->name_mode != stmt_name) && (cloned_statement->name_mode != stmt_handle) &&
         (cloned_statement->name_mode != stmt_via_desc)) ||
        // new statement, must be a cursor or a stmt handle
        (statement_id->name_mode == stmt_name) || (statement_id->name_mode == stmt_via_desc)) {
      diags << DgSqlCode(-CLI_INTERNAL_ERROR);
      return SQLCLI_ReturnCode(cliGlobals->currContext(), -CLI_INTERNAL_ERROR);
    }

#if defined(MULTIPLE_CURSORS_PER_STATEMENT)
    retcode = currContext.allocateStmt(statement_id, Statement::DYNAMIC_STMT, cloned_statement);
#else
    // kluge!!! this avoids the problem that we don't know how to
    //          allocate cursors which are separate from statements
    //          For beta, the limitation is 1 cursor per statement
    SQLDESC_ID desc;
    desc.module = statement_id->module;
    desc.identifier = statement_id->identifier;
    desc.name_mode = desc_name;

    retcode = SQLCLI_SetCursorName(cliGlobals, cloned_statement, &desc);
#endif
    if (isERROR(retcode)) return SQLCLI_ReturnCode(cliGlobals->currContext(), retcode);
  }

  return CliEpilogue(cliGlobals, NULL);
}

int SQLCLI_AllocStmtForRS(/*IN*/ CliGlobals *cliGlobals,
                          /*IN*/ SQLSTMT_ID *callStmtId,
                          /*IN*/ int resultSetIndex,
                          /*INOUT*/ SQLSTMT_ID *resultSetStmtId) {
  int retcode;

  // create initial context, if first call, and add module, if any.
  retcode = CliPrologue(cliGlobals, callStmtId->module);
  if (isERROR(retcode)) return retcode;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  // The CALL statement must exist
  Statement *callStmt = currContext.getStatement(callStmtId);
  if (!callStmt) {
    diags << DgSqlCode(-CLI_STMT_NOT_EXISTS);
    return SQLCLI_ReturnCode(&currContext, -CLI_STMT_NOT_EXISTS);
  }

  // The parent statement must be a CALL. This can be determine
  // from the root TDB's query type

  NABoolean parentIsCall = FALSE;
  ex_root_tdb *parentRoot = callStmt->getRootTdb();
  if (parentRoot) {
    int queryType = parentRoot->getQueryType();
    if (queryType == SQL_CALL_NO_RESULT_SETS || queryType == SQL_CALL_WITH_RESULT_SETS) parentIsCall = TRUE;
  }

  if (!parentIsCall) {
    diags << DgSqlCode(-EXE_UDR_RS_ALLOC_STMT_NOT_CALL);
    return SQLCLI_ReturnCode(&currContext, -EXE_UDR_RS_ALLOC_STMT_NOT_CALL);
  }

  // The RS index must not be out of range
  int maxRS = 0;
  if (parentRoot) maxRS = parentRoot->getMaxResultSets();
  if (resultSetIndex < 1 || resultSetIndex > maxRS) {
    diags << DgSqlCode(-EXE_UDR_RS_ALLOC_INVALID_INDEX) << DgInt0(resultSetIndex) << DgInt1(maxRS);
    return SQLCLI_ReturnCode(&currContext, -EXE_UDR_RS_ALLOC_INVALID_INDEX);
  }

  // A result set must not already exist at the specified position
  ExRsInfo *rsInfo = callStmt->getResultSetInfo();
  if (rsInfo && rsInfo->statementExists(resultSetIndex)) {
    diags << DgSqlCode(-EXE_UDR_RS_ALLOC_ALREADY_EXISTS) << DgInt0(resultSetIndex) << DgInt1(resultSetIndex);
    return SQLCLI_ReturnCode(&currContext, -EXE_UDR_RS_ALLOC_ALREADY_EXISTS);
  }

  // The RS statement must not exist
  Statement *rs = currContext.getStatement(resultSetStmtId);
  if (rs) {
    diags << DgSqlCode(-CLI_DUPLICATE_STMT);
    return SQLCLI_ReturnCode(&currContext, -CLI_DUPLICATE_STMT);
  }

  // Create the new statement
  retcode = SQLCLI_AllocStmt(cliGlobals, resultSetStmtId, NULL);
  if (isERROR(retcode)) return SQLCLI_ReturnCode(&currContext, retcode);

  // Locate the new statement
  rs = currContext.getStatement(resultSetStmtId);
  if (!rs) {
    diags << DgSqlCode(-CLI_STMT_NOT_EXISTS);
    return SQLCLI_ReturnCode(&currContext, -CLI_STMT_NOT_EXISTS);
  }

  // Bind the new statement to its parent CALL statement
  rsInfo = callStmt->getOrCreateResultSetInfo();
  rsInfo->setNumEntries(maxRS);
  rsInfo->bind(resultSetIndex, rs);
  rs->setParentCall(callStmt);

  return CliEpilogue(cliGlobals, resultSetStmtId);

}  // SQLCLI_AllocStmtForRS

int SQLCLI_AssocFileNumber(/*IN*/ CliGlobals *cliGlobals,
                           /*IN*/ SQLSTMT_ID *statement_id,
                           /*IN*/ short file_num) {
  return 0;
}
int SQLCLI_BreakEnabled(/*IN*/ CliGlobals *cliGlobals,
                        /*IN*/ UInt32 enabled) {
  if (!cliGlobals) {
    return -CLI_NO_CURRENT_CONTEXT;
  }

  cliGlobals->setBreakEnabled(enabled);
  return 0;
}
int SQLCLI_SPBreakRecvd(/*IN*/ CliGlobals *cliGlobals,
                        /*OUT*/ UInt32 *breakRecvd) {
  if (!cliGlobals) {
    return -CLI_NO_CURRENT_CONTEXT;
  }

  *breakRecvd = cliGlobals->SPBreakReceived();
  cliGlobals->setSPBreakReceived(FALSE);
  return 0;
}

int SQLCLI_CreateContext(/*IN*/ CliGlobals *cliGlobals,
                         /*OUT*/ SQLCTX_HANDLE *contextHandle,
                         /*IN*/ char *sqlAuthId,
                         /*IN*/ int mustBeZero /* for future use */) {
  int retcode;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();
  if (mustBeZero != 0) {
    //
    // We constrain "for future use" parameters so that only a
    // particular value (in this case 0) is accepted. Without this
    // restriction things might break should we suddenly give the
    // parameters some semantics.
    //
    diags << DgSqlCode(-CLI_RESERVED_ARGUMENT) << DgString0("2") << DgString1("SQL_EXEC_CreateContext")
          << DgString2("0");
    return SQLCLI_ReturnCode(&currContext, -CLI_RESERVED_ARGUMENT);
  }
  ContextCli *newContext = NULL;
  retcode = cliGlobals->createContext(newContext);
  if (!newContext) {
    diags << DgSqlCode(-CLI_INTERNAL_ERROR);
    return SQLCLI_ReturnCode(&currContext, -CLI_INTERNAL_ERROR);
  }

  if (contextHandle) *contextHandle = newContext->getContextHandle();

  return SUCCESS;
}

int SQLCLI_CurrentContext(/*IN*/ CliGlobals *cliGlobals,
                          /*OUT*/ SQLCTX_HANDLE *contextHandle) {
  if (!cliGlobals) {
    return -CLI_NO_CURRENT_CONTEXT;
  }

  ContextCli *currentContext = cliGlobals->currContext();

  if (contextHandle) *contextHandle = currentContext->getContextHandle();

  return SUCCESS;
}

int SQLCLI_DeleteContext(/*IN*/ CliGlobals *cliGlobals,
                         /*IN*/ SQLCTX_HANDLE context_handle) {
  if (!cliGlobals) {
    return -CLI_NO_CURRENT_CONTEXT;
  }

  return SQLCLI_DropContext(cliGlobals, context_handle);
}

int SQLCLI_DropModule(/*IN*/ CliGlobals *cliGlobals,
                      /*IN*/ const SQLMODULE_ID *module_id) {
  return SUCCESS;
}

int SQLCLI_ResetContext(/*IN*/ CliGlobals *cliGlobals,
                        /*IN*/ SQLCTX_HANDLE context_handle,
                        /*IN*/ void *contextMsg) {
  int retcode;

  ContextCli *context = cliGlobals->getContext(context_handle);
  ContextCli *defaultContext = cliGlobals->getDefaultContext();
  ComDiagsArea &diags = defaultContext->diags();
  if (context == NULL) {
    diags << DgSqlCode(-CLI_CONTEXT_NOT_FOUND);
    return SQLCLI_ReturnCode(defaultContext, -CLI_CONTEXT_NOT_FOUND);
  }
  CLISemaphore *tmpSemaphore = context->getSemaphore();
  tmpSemaphore->get();
  try {
    retcode = cliGlobals->resetContext(context, contextMsg);
  } catch (...) {
    retcode = -CLI_INTERNAL_ERROR;
    tmpSemaphore->release();
  }
  tmpSemaphore->release();
  return SQLCLI_ReturnCode(context, retcode);
}

int SQLCLI_GetUdrErrorFlags_Internal(/*IN*/ CliGlobals *cliGlobals,
                                     /*OUT*/ int *udrErrorFlags) {
  if (!cliGlobals) {
    return -CLI_NO_CURRENT_CONTEXT;
  }

  if (!cliGlobals->getUdrErrorChecksEnabled()) {
    return -CLI_NOT_CHECK_VIOLATION;
  }

  NABoolean sqlViolation = FALSE;
  NABoolean xactViolation = FALSE;
  NABoolean xactWasAborted = FALSE;

  cliGlobals->getUdrErrorFlags(sqlViolation, xactViolation, xactWasAborted);

  *udrErrorFlags = 0;

  if (sqlViolation) {
    *udrErrorFlags |= SQLUDR_SQL_VIOL;
  }
  if (xactViolation) {
    *udrErrorFlags |= SQLUDR_XACT_VIOL;
  }
  if (xactWasAborted) {
    *udrErrorFlags |= SQLUDR_XACT_ABORT;
  }

  return SUCCESS;
}

int SQLCLI_SetUdrAttributes_Internal(/*IN*/ CliGlobals *cliGlobals,
                                     /*IN*/ int sqlAccessMode,
                                     /*IN*/ int mustBeZero /* for future use */) {
  if (!cliGlobals) {
    return -CLI_NO_CURRENT_CONTEXT;
  }

  if (mustBeZero != 0) {
    //
    // We constrain "for future use" parameters so that only a
    // particular value (in this case 0) is accepted. Without this
    // restriction things might break should we suddenly give the
    // parameters some semantics.
    //
    ContextCli &currContext = *(cliGlobals->currContext());
    ComDiagsArea &diags = currContext.diags();
    diags << DgSqlCode(-CLI_RESERVED_ARGUMENT) << DgString0("2") << DgString1("SQL_EXEC_SetUdrAttributes_Internal")
          << DgString2("0");
    return SQLCLI_ReturnCode(&currContext, -CLI_RESERVED_ARGUMENT);
  }

  cliGlobals->setUdrErrorChecksEnabled(TRUE);
  cliGlobals->setUdrSQLAccessMode((enum ComRoutineSQLAccess)sqlAccessMode);

  return SUCCESS;
}

int SQLCLI_ResetUdrErrorFlags_Internal(/*IN*/ CliGlobals *cliGlobals) {
  if (!cliGlobals) {
    return -CLI_NO_CURRENT_CONTEXT;
  }

  cliGlobals->clearUdrErrorFlags();

  return SUCCESS;
}
// A CLI wrapper around a ContextCli method to dynamically set JVM
// startup options in the UDR server
int SQLCLI_SetUdrRuntimeOptions_Internal(/*IN*/ CliGlobals *cliGlobals,
                                         /*IN*/ const char *options,
                                         /*IN*/ int optionsLen,
                                         /*IN*/ const char *delimiters,
                                         /*IN*/ int delimsLen) {
  if (!cliGlobals) {
    return -CLI_NO_CURRENT_CONTEXT;
  }

  ContextCli &currContext = *(cliGlobals->currContext());
  int retcode = currContext.setUdrRuntimeOptions(options, optionsLen, delimiters, delimsLen);

  return SQLCLI_ReturnCode(&currContext, retcode);
}

int SQLCLI_DeallocDesc(/*IN*/ CliGlobals *cliGlobals,
                       /*IN*/ SQLDESC_ID *desc_id) {
  int retcode;
  if (!cliGlobals) {
    return -CLI_NO_CURRENT_CONTEXT;
  }

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  retcode = currContext.deallocDesc(desc_id, FALSE);

  return SQLCLI_ReturnCode(&currContext, retcode);
}

int SQLCLI_DeallocStmt(/*IN*/ CliGlobals *cliGlobals,
                       /*IN*/ SQLSTMT_ID *statement_id) {
  int retcode;

  if (!cliGlobals) {
    return (-CLI_NO_CURRENT_CONTEXT);
  }

  retcode = CheckNOSQLAccessMode(*cliGlobals);
  if (isERROR(retcode)) {
    return retcode;
  }

  retcode = CliPrologue(cliGlobals, NULL);
  if (isERROR(retcode)) return retcode;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  retcode = currContext.deallocStmt(statement_id, FALSE);

  return SQLCLI_ReturnCode(&currContext, retcode);
}
int SQLCLI_DefineDesc(/*IN*/ CliGlobals *cliGlobals,
                      /*IN*/ SQLSTMT_ID *statement_id,
                      /* (SQLWHAT_DESC) *IN*/ int what_descriptor,
                      /*IN*/ SQLDESC_ID *desc_id) {
  int retcode;

  // create initial context, if first call, and add module, if any.
  retcode = CliPrologue(cliGlobals, statement_id->module);
  if (isERROR(retcode)) return retcode;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  Statement *stmt = currContext.getStatement(statement_id);

  /* stmt must exist */
  if (!stmt) {
    diags << DgSqlCode(-CLI_STMT_NOT_EXISTS);
    return SQLCLI_ReturnCode(&currContext, -CLI_STMT_NOT_EXISTS);
  }

  Descriptor *desc = currContext.getDescriptor(desc_id);

  /* descriptor must exist */
  if (!desc) {
    diags << DgSqlCode(-CLI_DESC_NOT_EXISTS);
    return SQLCLI_ReturnCode(&currContext, -CLI_DESC_NOT_EXISTS);
  }

  stmt->addDefaultDesc(desc, what_descriptor);

  return SUCCESS;
}
int SQLCLI_DescribeStmt(/*IN*/ CliGlobals *cliGlobals,
                        /*IN*/ SQLSTMT_ID *statement_id,
                        /*IN  OPTIONAL*/ SQLDESC_ID *input_descriptor,
                        /*IN  OPTIONAL*/ SQLDESC_ID *output_descriptor) {
  if (!cliGlobals) {
    return -CLI_NO_CURRENT_CONTEXT;
  }

  int retcode;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  /* describe the statement */
  Statement *stmt = currContext.getStatement(statement_id);

  /* stmt must exist */
  if (!stmt) {
    diags << DgSqlCode(-CLI_STMT_NOT_EXISTS);
    return SQLCLI_ReturnCode(&currContext, -CLI_STMT_NOT_EXISTS);
  }

  /* stmt should be a dynamic statement and must not be in INITIAL state */
  /* if it is in INITIAL state then the statement did not get prepared. */
  /* If it is in PREPARE state, then it is a nowaited prepare which is
     still being prepared. */

  if ((stmt->getState() == Statement::INITIAL_) || (stmt->getState() == Statement::PREPARE_)) {
    // One special case for CALL statements. A stored procedure result
    // set proxy statement can be described without first being
    // prepared by the CLI caller. The prepare gets done internally
    // inside Statement::describe(). So for this case we do not want
    // to return here with an error just because the statement is not
    // yet prepared.
    NABoolean reportAnError = TRUE;
    if (stmt->getState() == Statement::INITIAL_ && stmt->getParentCall()) reportAnError = FALSE;

    if (reportAnError) {
      diags << DgSqlCode(-CLI_STMT_NOT_PREPARED);
      return SQLCLI_ReturnCode(&currContext, -CLI_STMT_NOT_PREPARED);
    }
  }

  if (input_descriptor) {
    Descriptor *input_desc = currContext.getDescriptor(input_descriptor);
    if (!input_desc) {
      diags << DgSqlCode(-CLI_DESC_NOT_EXISTS);
      return SQLCLI_ReturnCode(&currContext, -CLI_DESC_NOT_EXISTS);
    }

    retcode = stmt->describe(input_desc, SQLWHAT_INPUT_DESC, diags);
    if (isERROR(retcode)) return SQLCLI_ReturnCode(&currContext, retcode);
  }

  if (output_descriptor) {
    Descriptor *output_desc = currContext.getDescriptor(output_descriptor);

    if (!output_desc) {
      diags << DgSqlCode(-CLI_DESC_NOT_EXISTS);
      return SQLCLI_ReturnCode(&currContext, -CLI_DESC_NOT_EXISTS);
    }

    retcode = stmt->describe(output_desc, SQLWHAT_OUTPUT_DESC, diags);
    if (isERROR(retcode)) return SQLCLI_ReturnCode(&currContext, retcode);
  }

  return CliEpilogue(cliGlobals, statement_id);
}
int SQLCLI_DisassocFileNumber(/*IN*/ CliGlobals *cliGlobals,
                              /*IN*/ SQLSTMT_ID *statement_id) {
  return 0;
}

int SQLCLI_DropContext(/*IN*/ CliGlobals *cliGlobals,
                       /*IN*/ SQLCTX_HANDLE context_handle) {
  ContextCli *defaultContext = cliGlobals->getDefaultContext();
  ComDiagsArea &diags = defaultContext->diags();

  ContextCli *droppedContext = cliGlobals->getContext(context_handle, TRUE);

  if (context_handle == cliGlobals->getDefaultContext()->getContextHandle()) {
    diags << DgSqlCode(-CLI_DEFAULT_CONTEXT_NOT_ALLOWED);
    return SQLCLI_ReturnCode(defaultContext, -CLI_DEFAULT_CONTEXT_NOT_ALLOWED);
  }

  if (droppedContext == NULL) {
    diags << DgSqlCode(-CLI_CONTEXT_NOT_FOUND);
    return SQLCLI_ReturnCode(defaultContext, -CLI_CONTEXT_NOT_FOUND);
  }
  int retcode;

  try {
    retcode = cliGlobals->dropContext(droppedContext);
    if (retcode == -1) {
      // $$$ probably should be a different error message
      diags << DgSqlCode(-CLI_NO_CURRENT_CONTEXT);
    }
  } catch (...) {
    return -CLI_INTERNAL_ERROR;
  }
  return SQLCLI_ReturnCode(defaultContext, retcode);
}

int SQLCLI_SetRowsetDescPointers(CliGlobals *cliGlobals, SQLDESC_ID *desc_id, int rowset_size, int *rowset_status_ptr,
                                 int starting_entry, int num_quadruple_fields, int num_ap, va_list ap,
                                 SQLCLI_QUAD_FIELDS quad_fields[]) {
  if (!desc_id) {
    return -CLI_INTERNAL_ERROR;
  }

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();
  Descriptor *desc = currContext.getDescriptor(desc_id);

  /* descriptor must exist */
  if (!desc) {
    diags << DgSqlCode(-CLI_DESC_NOT_EXISTS);
    return SQLCLI_ReturnCode(&currContext, -CLI_DESC_NOT_EXISTS);
  }

  desc->setDescItem(0, SQLDESC_ROWSET_SIZE, rowset_size, 0);
  desc->setDescItem(0, SQLDESC_ROWSET_STATUS_PTR, (Long)rowset_status_ptr, 0);

  if (num_quadruple_fields > 0) {
    for (int entry = starting_entry, i = 0; entry < starting_entry + num_quadruple_fields; entry++, i++) {
      if (desc->getUsedEntryCount() < entry) {
        diags << DgSqlCode(-CLI_INVALID_DESC_ENTRY);
        return SQLCLI_ReturnCode(&currContext, -CLI_INVALID_DESC_ENTRY);
      }

      // need to re-order this to avoid possible problems with not setting
      // the data area (i.e. VAR_PTR) last.
      char *var_layout, *var_ptr, *ind_layout, *ind_ptr;
      if (num_ap) {
        var_layout = va_arg(ap, char *);
        var_ptr = va_arg(ap, char *);
        ind_layout = va_arg(ap, char *);
        ind_ptr = va_arg(ap, char *);
      } else {
        var_layout = (char *)((long)quad_fields[i].var_layout);
        var_ptr = (char *)quad_fields[i].var_ptr;
        ind_layout = (char *)((long)quad_fields[i].ind_layout);
        ind_ptr = quad_fields[i].ind_ptr == (void *)-1 ? 0 : (char *)quad_fields[i].ind_ptr;
      }
      // setDescItems requires setting the rowset size and layout size items
      // before setting the pointers. Do not change the following order.
      desc->setDescItem(entry, SQLDESC_ROWSET_VAR_LAYOUT_SIZE, (Long)var_layout, 0);
      desc->setDescItem(entry, SQLDESC_ROWSET_IND_LAYOUT_SIZE, (Long)ind_layout, 0);
      desc->setDescItem(entry, SQLDESC_IND_PTR, (Long)ind_ptr, 0);
      desc->setDescItem(entry, SQLDESC_VAR_PTR, (Long)var_ptr, 0);
    }
    va_end(ap);
  }

  return SUCCESS;
}
int SQLCLI_GetRowsetNumprocessed(CliGlobals *cliGlobals, SQLDESC_ID *desc_id, int &rowset_nprocessed) {
  int retcode = SUCCESS;

  // create initial context, if first call, and add module, if any.
  retcode = CliPrologue(cliGlobals, desc_id->module);
  if (isERROR(retcode)) return retcode;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  Descriptor *desc = currContext.getDescriptor(desc_id);

  /* descriptor must exist */
  if (!desc) {
    diags << DgSqlCode(-CLI_DESC_NOT_EXISTS);
    return SQLCLI_ReturnCode(&currContext, -CLI_DESC_NOT_EXISTS);
  }

  retcode = desc->getDescItem(1, SQLDESC_ROWSET_NUM_PROCESSED, (int *)&rowset_nprocessed, NULL, 0, NULL, 0);

  retcode = CliEpilogue(cliGlobals, NULL);
  return SQLCLI_ReturnCode(&currContext, retcode);
}
static NABoolean SQLCLI_GetRetryInfo(CliGlobals *cliGlobals, int retcode, AQRStatementInfo *aqrSI, short afterPrepare,
                                     short afterExec, short afterFetch, short afterCEFC, int &retries, int &delay,
                                     int &type, int &numCQDs, char *&cqdStr, int &cmpInfo, ComCondition *&cc) {
  retries = -1;
  delay = -1;
  type = -1;
  cmpInfo = 0;
  cc = NULL;
  int intAQR;
  ContextCli *context = cliGlobals->currContext();
  AQRInfo *aqr = context->aqrInfo();
  NABoolean doAqr = FALSE;  // auto query retry
  if ((aqr) && (aqrSI) && (aqrSI->getRetryStatementId()) && (aqrSI->getRetrySqlSource()) && (isERROR(retcode)) &&
      (retcode != -EXE_CANCELED) && (context->diags().getNumber(DgSqlCode::ERROR_) > 0) &&
      (context->getNumOfCliCalls() == 1)) {
    type = AQRInfo::RETRY_NONE;

    for (int i = 1; ((NOT doAqr) && (i <= context->diags().getNumber(DgSqlCode::ERROR_))); i++) {
      ComCondition *lcc = context->diags().getErrorEntry(i);
      aqr->getAQREntry(lcc->getSQLCODE(), lcc->getNskCode(), retries, delay, type, numCQDs, cqdStr, cmpInfo, intAQR);
      if ((type >= AQRInfo::RETRY_MIN_VALID_TYPE) && (type <= AQRInfo::RETRY_MAX_VALID_TYPE)) {
        cc = lcc;

        doAqr = TRUE;
      }  // if
    }    // for

    if ((type != AQRInfo::RETRY_NONE) && ((afterFetch) && (NOT aqrSI->retryFirstFetch())) &&
        !(afterExec && aqrSI->retryAfterExec()))

      doAqr = FALSE;
  }  // if

  return doAqr;
}

static int SQLCLI_RetryDeallocStmt(
    /*IN*/ CliGlobals *cliGlobals, AQRStatementInfo *aqrSI) {
  int retcode = 0;

  ContextCli *currContext = cliGlobals->currContext();

  ComDiagsArea &diags = currContext->diags();
  diags.clear();

  SQLDESC_ID *temp_input_desc_id = NULL;
  SQLDESC_ID *temp_output_desc_id = NULL;
  if (!aqrSI->getRetryInputDesc()) {
    SQLMODULE_ID *module = new (currContext->exHeap()) SQLMODULE_ID;
    init_SQLMODULE_ID(module);

    temp_input_desc_id = new (currContext->exHeap()) SQLDESC_ID;
    init_SQLDESC_ID(temp_input_desc_id, SQLCLI_CURRENT_VERSION, desc_handle, module);

    retcode = SQLCLI_AllocDescInt(cliGlobals, temp_input_desc_id, 1);
    if (isERROR(retcode)) return retcode;
  }

  if (!aqrSI->getRetryOutputDesc()) {
    SQLMODULE_ID *module = new (currContext->exHeap()) SQLMODULE_ID;
    init_SQLMODULE_ID(module);

    temp_output_desc_id = new (currContext->exHeap()) SQLDESC_ID;
    init_SQLDESC_ID(temp_output_desc_id, SQLCLI_CURRENT_VERSION, desc_handle, module);

    retcode = SQLCLI_AllocDescInt(cliGlobals, temp_output_desc_id, 1);
    if (isERROR(retcode)) return retcode;
  }

  retcode = SQLCLI_DescribeStmt(cliGlobals, aqrSI->getRetryStatementId(), temp_input_desc_id, temp_output_desc_id);
  if (isERROR(retcode) && (retcode != -CLI_STMT_NOT_PREPARED)) return retcode;
  if (retcode == -CLI_STMT_NOT_PREPARED) diags.clear();

  aqrSI->setRetryTempInputDesc(temp_input_desc_id);
  aqrSI->setRetryTempOutputDesc(temp_output_desc_id);

  retcode = SQLCLI_DeallocStmt(cliGlobals, aqrSI->getRetryStatementId());

  if (isERROR(retcode)) return retcode;

  return retcode;
}

// If a query was explicitly prepared and it gets a timestamp mismatch
// or a lost open error during its fixup or execute phase, then AQR
// is not done. This is needed to handle the case where the table shape
// or structure got changed by a ddl operation after the stmt was
// explicitly prepared. And that ddl operation caused the ts mismatch.
// Since this was an explicitly prepared stmt, it cannot be automatically
// re-described during the execute phase. Executing a statement that was
// internally prepared during AQR but not re-described can cause
// inconsistency between the structures/data that is returned or input
// and the structure that the caller had set up to handle during
// describe input/output phase of the initial prepare.
// Users need to explicitly reprepare this statement.
// Add an error indicating this condition and return the original
// error back to user.
static int SQLCLI_RetryValidateDescs(/*IN*/ CliGlobals *cliGlobals,
                                     /*IN*/ SQLSTMT_ID *statement_id,
                                     /*IN  OPTIONAL*/ SQLDESC_ID *curr_input_desc_id,
                                     /*IN  OPTIONAL*/ SQLDESC_ID *curr_output_desc_id) {
  int retcode = 0;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  Statement *stmt = currContext.getStatement(statement_id);
  if ((stmt == NULL) || (stmt->isStandaloneQ())) return retcode;

  SQLMODULE_ID module;
  init_SQLMODULE_ID(&module);

  SQLDESC_ID new_input_desc_id;
  SQLDESC_ID new_output_desc_id;

  init_SQLDESC_ID(&new_input_desc_id, SQLCLI_CURRENT_VERSION, desc_handle, &module);
  init_SQLDESC_ID(&new_output_desc_id, SQLCLI_CURRENT_VERSION, desc_handle, &module);

  retcode = SQLCLI_AllocDescInt(cliGlobals, &new_input_desc_id, 1);
  if (isERROR(retcode)) return retcode;

  retcode = SQLCLI_AllocDescInt(cliGlobals, &new_output_desc_id, 1);
  if (isERROR(retcode)) return retcode;

  Descriptor *currInputDesc = currContext.getDescriptor(curr_input_desc_id);
  if (!currInputDesc) {
    diags << DgSqlCode(-CLI_DESC_NOT_EXISTS);
    return -CLI_DESC_NOT_EXISTS;
  }

  Descriptor *currOutputDesc = currContext.getDescriptor(curr_output_desc_id);
  if (!currOutputDesc) {
    diags << DgSqlCode(-CLI_DESC_NOT_EXISTS);
    return -CLI_DESC_NOT_EXISTS;
  }

  Descriptor *newInputDesc = currContext.getDescriptor(&new_input_desc_id);
  if (!newInputDesc) {
    diags << DgSqlCode(-CLI_DESC_NOT_EXISTS);
    return -CLI_DESC_NOT_EXISTS;
  }
  Descriptor *newOutputDesc = currContext.getDescriptor(&new_output_desc_id);
  if (!newOutputDesc) {
    diags << DgSqlCode(-CLI_DESC_NOT_EXISTS);
    return -CLI_DESC_NOT_EXISTS;
  }
  // Before describing , save the flags from the old descriptors that the user
  // may have set using a setDescItem call
  newOutputDesc->setDescFlags(currOutputDesc->getDescFlags());
  newInputDesc->setDescFlags(currInputDesc->getDescFlags());
  retcode = SQLCLI_DescribeStmt(cliGlobals, statement_id, &new_input_desc_id, &new_output_desc_id);
  if (isERROR(retcode)) return retcode;
  newInputDesc = currContext.getDescriptor(&new_input_desc_id);
  if (!newInputDesc) {
    diags << DgSqlCode(-CLI_DESC_NOT_EXISTS);
    return -CLI_DESC_NOT_EXISTS;
  }
  newOutputDesc = currContext.getDescriptor(&new_output_desc_id);
  if (!newOutputDesc) {
    diags << DgSqlCode(-CLI_DESC_NOT_EXISTS);
    return -CLI_DESC_NOT_EXISTS;
  }
  if (NOT(*currInputDesc == *newInputDesc)) {
    retcode = -EXE_USER_PREPARE_NEEDED;
    goto retError;
  }

  if (NOT(*currOutputDesc == *newOutputDesc)) {
    retcode = -EXE_USER_PREPARE_NEEDED;
    goto retError;
  }

  // deallocate new input/output descriptor
  SQLCLI_DeallocDesc(cliGlobals, &new_input_desc_id);
  SQLCLI_DeallocDesc(cliGlobals, &new_output_desc_id);
  return retcode;

retError:
  SQLCLI_DeallocDesc(cliGlobals, &new_input_desc_id);
  SQLCLI_DeallocDesc(cliGlobals, &new_output_desc_id);

  diags << DgSqlCode(retcode);

  if (stmt) stmt->setAqrReprepareNeeded(TRUE);

  return retcode;
}

static int SQLCLI_RetryQuery(
    /*IN*/ CliGlobals *cliGlobals, AQRStatementInfo *aqrSI, int flags, short afterPrepare, short afterExec,
    short afterFetch, short afterCEFC, StmtStats *savedStmtStats) {
  int retcode = 0;

  ContextCli *currContext = cliGlobals->currContext();
  ComDiagsArea &diags = currContext->diags();

  SQL_QUERY_COST_INFO queryCostInfo;
  SQL_QUERY_COMPILER_STATS_INFO queryCompStatsInfo;
  // allocate a new statement which will be used for the retry

  retcode = SQLCLI_AllocStmt(cliGlobals, aqrSI->getRetryStatementId(), NULL);

  // Copy the saved off  statement attributes into the newly created stmt
  Statement *newStmt = currContext->getStatement(aqrSI->getRetryStatementId());
  if (newStmt) {
    aqrSI->copyAttributesToStmt(newStmt);
  }

  if (isERROR(retcode)) {
    if (savedStmtStats) savedStmtStats->setAqrInProgress(FALSE);
    return retcode;
  }

  // In case of fsth path CLI calls, we need to clear out the statement handle  // that may point to a StatementInfo()
  // struct that no longer is valid.
  if (aqrSI->getRetryStatementId()->name_mode == stmt_name) aqrSI->getRetryStatementId()->handle = NULL;
  // pass in the original query id when retrying this query.
  char *uniqueQueryId = aqrSI->getAQRStatementAttributes()->uniqueStmtId_;
  int uniqueQueryIdLen = aqrSI->getAQRStatementAttributes()->uniqueStmtIdLen_;
  // prepare the statement that has been already allocated earlier
  retcode = SQLCLI_Prepare2(cliGlobals, aqrSI->getRetryStatementId(), aqrSI->getRetrySqlSource(), NULL, 0, NULL,
                            &queryCostInfo, &queryCompStatsInfo, uniqueQueryId, &uniqueQueryIdLen, flags);

  if (isERROR(retcode)) {
    if (savedStmtStats) savedStmtStats->setAqrInProgress(FALSE);
    return retcode;
  }

  if (afterPrepare) {
    if (savedStmtStats) savedStmtStats->setAqrInProgress(FALSE);
    return 0;
  }

  // before executing this statement,
  // validate that the new prepare's input/output descriptors are the same
  // as the original input/output descriptors.
  retcode = SQLCLI_RetryValidateDescs(
      cliGlobals, aqrSI->getRetryStatementId(),
      (aqrSI->getRetryInputDesc() ? aqrSI->getRetryInputDesc() : aqrSI->getRetryTempInputDesc()),
      (aqrSI->getRetryOutputDesc() ? aqrSI->getRetryOutputDesc() : aqrSI->getRetryTempOutputDesc()));
  if (isERROR(retcode)) return retcode;

  SQLCLI_PTR_PAIRS ptr_pairs[1];

  if (afterCEFC) {
    // MXCS may pass a nam_mode of stmt_handle for this particular call.
    // So handle tht case also. Reset the statement handle to be NULL.
    // The StmtInfo() class
    // would need to be reinitialized with this newly created statement
    if (aqrSI->getRetryStatementId()->name_mode != desc_handle) aqrSI->getRetryStatementId()->handle = NULL;
    SQLDESC_ID *retryOutputDesc = aqrSI->getRetryOutputDesc();
    Statement *n_stmt = currContext->getStatement(aqrSI->getRetryStatementId());
    if (n_stmt && n_stmt->getRootTdb() && (n_stmt->getRootTdb()->getQueryType() == SQL_SELECT_UNIQUE)) {
      retryOutputDesc = aqrSI->getRetryOutputDesc() ? aqrSI->getRetryOutputDesc() : aqrSI->getRetryTempOutputDesc();
    }
    retcode = SQLCLI_ClearExecFetchClose(cliGlobals, aqrSI->getRetryStatementId(), aqrSI->getRetryInputDesc(),
                                         retryOutputDesc, 0, 0, 0, 0, VA_LIST_NULL, 0, 0);
    if (isERROR(retcode)) return retcode;
  } else {
    retcode = SQLCLI_Exec(cliGlobals, aqrSI->getRetryStatementId(), aqrSI->getRetryInputDesc(), 0, 0, VA_LIST_NULL,
                          ptr_pairs);
    if (isERROR(retcode)) return retcode;

    if (afterExec && !afterFetch) return 0;

    retcode = SQLCLI_Fetch(cliGlobals, aqrSI->getRetryStatementId(), aqrSI->getRetryOutputDesc(), 0, 0, VA_LIST_NULL,
                           ptr_pairs);
    if (isERROR(retcode)) return retcode;
  }
  // If this is a retry of an ExecFetch ,also close the statement because
  // SQCLI_ExecFetch also  internally closes the statement. So we have to do
  // the same when going through AQR
  if (afterExec && afterFetch) {
    retcode = SQLCLI_CloseStmt(cliGlobals, aqrSI->getRetryStatementId());
    if (isERROR(retcode)) return retcode;
  }

  return retcode;
}

// When previous attempt of a failed NO ROLLBACK did change
// the target table in ways that it could not undo, then
// do not allow retry, except in case of non-ACID DELETE which
// is configured to continue using AQR.
bool preventAqrWnr(ContextCli *currContext, Statement *stmt, bool &addWarning8737) {
  bool preventAqr = true;
  addWarning8737 = false;

  // Get statistics from RMS shared segment via ssmp
  // to guard against scenarios like 2034, where the stats
  // would not flow back to the master due to broken data
  // channels.
  const ExStatisticsArea *constStatsArea = NULL;
  int cliRc = SQL_EXEC_GetStatisticsArea_Internal(SQLCLI_STATS_REQ_QID, stmt->getUniqueStmtId(),
                                                  stmt->getUniqueStmtIdLen(), -1, SQLCLI_SAME_STATS, constStatsArea);

  ExStatisticsArea *statsArea = (ExStatisticsArea *)constStatsArea;

  bool targetIsChanged = false;

  if ((cliRc < 0) || (NULL == statsArea))
    targetIsChanged = true;  // Cannot ascertain, so assume worst
  else if ((NULL == statsArea->getMasterStats()) || (statsArea->getMasterStats()->getStatsErrorCode() != 0))
    targetIsChanged = true;  // partial success, e.g., node down.
  else {
    if (statsArea->anyHaveSentMsgIUD()) targetIsChanged = true;
  }

  if (stmt->getGlobals() && stmt->getGlobals()->getAqrWnrInsertCleanedup()) targetIsChanged = false;

  if (targetIsChanged) {
    // for deletes, if AQR_WNR_DELETE_NO_ROWCOUNT, we may
    // retry even if target has changed.
    ex_root_tdb *rootTdb = stmt->getRootTdb();
    if (((rootTdb->getQueryType() == ComTdbRoot::SQL_DELETE_UNIQUE) ||
         (rootTdb->getQueryType() == ComTdbRoot::SQL_DELETE_NON_UNIQUE)) &&
        rootTdb->aqrWnrDeleteContinue()) {
      preventAqr = false;
      addWarning8737 = true;
    }
  } else
    preventAqr = false;

  return preventAqr;
}
int SQLCLI_ProcessRetryQuery(
    /*IN*/ CliGlobals *cliGlobals, SQLSTMT_ID *statement_id, int sqlcode, short afterPrepare, short afterExec,
    short afterFetch, short afterCEFC) {
  int retcode = sqlcode;
  if (NOT isERROR(retcode)) return retcode;

  ContextCli *currContext = cliGlobals->currContext();
  ComDiagsArea &diags = currContext->diags();
  AQRInfo *aqr = currContext->aqrInfo();
  ExTransaction *exTransaction = currContext->getTransaction();
  bool addWarning8737 = false;
  bool checkAqrWnr = false;

  Statement *stmt = currContext->getStatement(statement_id);
  if ((stmt == NULL) || (stmt->getStatementType() == Statement::STATIC_STMT) || (stmt->getStmtId() == NULL) ||
      (stmt->getSrcStrSize() <= 0))
    return retcode;

  ex_root_tdb *rootTdb = stmt->getRootTdb();
  if (rootTdb) {
    // for shared cache related query, do not AQR
    if (rootTdb->useDlockForSharedCache()) return retcode;
    // Client statement pooling is turned on, make the client to re-prepare
    // for CLI_DDL_REDEFINED
    int clientMaxStatementPooling = rootTdb->getClientMaxStatementPooling();
    if (clientMaxStatementPooling > 0 &&
        ((!stmt->isStandaloneQ()) && (sqlcode == -CLI_DDL_REDEFINED || sqlcode == -EXE_USER_PREPARE_NEEDED ||
                                      sqlcode == -CLI_STMT_CONFLICTING_CONCURRENT_DDL)))
      return retcode;
    // Of course, there are many other checks to see if AQR can be
    // attempted. This next check considers the SQLCODE and attributes
    // of the statement which were set by the code generator.
    if (NOT rootTdb->aqrEnabledForSqlcode(sqlcode)) return retcode;

    // WITH NO ROLLBACK that has sent msgs to IUD cannot be
    // retried
    if (!rootTdb->isPacked() && (rootTdb->getTransMode()->anyNoRollback() ||
                                 (exTransaction && exTransaction->getTransMode()->anyNoRollback()))) {
      checkAqrWnr = true;
      if (preventAqrWnr(currContext, stmt, addWarning8737)) return retcode;
    }

    if ((rootTdb->updDelInsertQuery()) || (rootTdb->ddlQuery())) {
      if (rootTdb->transactionReqd() && (NOT exTransaction->xnInProgress())) {
        if (exTransaction->autoCommit()) {
          if ((aqr->abortedTransWasImplicit() == FALSE) && (diags.getNumber(DgSqlCode::ERROR_) > 0) &&
              diags.containsError(-CLI_VALIDATE_TRANSACTION_ERROR))
            return retcode;

          // if aqr was enabled but cant be done at runtime, return.
          if (aqr->noAQR()) return retcode;
        } else {
          if (sqlcode != -CLI_HISTOGRAM_UPDATE)
            // Part of the fix for 10-100604-0887.  See statement.cpp
            // for the other part and for more commentary.
            return retcode;
        }
      }
    }

    // if this is a udr/call statement and not a standalone statement, return an error
    // The caller will need to reprepare the query so we have an updated tdb to execute.
    // If it's a standalone statement, the statement will get reprepared and executed.
    if ((rootTdb->getUdrCount() > 0) && !stmt->isStandaloneQ()) {
      return retcode;
    }
  } else if (!rootTdb && afterPrepare) {
    if ((!stmt->isStandaloneQ()) && (sqlcode == -CLI_DDL_REDEFINED || sqlcode == -EXE_USER_PREPARE_NEEDED))
      return retcode;
    // rootTdb is null. Query failed during compile.
    // if AQR is off, return error.
    if (currContext->getSessionDefaults()->getAqrType() == 0) return retcode;
  }

  AQRStatementInfo *aqrSI = NULL;
  char *stmtId = NULL;
  char *id = NULL;
  char *charset = NULL;
  SQLSTMT_ID sql_stmt;
  SQLDESC_ID sql_src;
  SQLMODULE_ID sql_module;
  if (stmt) {
    aqrSI = new (currContext->exHeap()) AQRStatementInfo(currContext->exHeap());

    id = new (currContext->exHeap()) char[stmt->getStmtId()->identifier_len + 1];
    str_cpy_all(id, stmt->getStmtId()->identifier, stmt->getStmtId()->identifier_len);
    id[stmt->getStmtId()->identifier_len] = 0;

    init_SQLMODULE_ID(&sql_module);
    init_SQLSTMT_ID(&sql_stmt, SQLCLI_CURRENT_VERSION, (SQLOBJ_ID_NAME_MODE)stmt->getStmtId()->name_mode, &sql_module,
                    id, stmt->getStmtId()->handle, stmt->getStmtId()->charset, stmt->getStmtId()->identifier_len);
    aqrSI->setRetryStatementId(&sql_stmt);

    charset = NULL;
    charset = new (currContext->exHeap()) char[strlen(SQLCHARSETSTRING_UTF8) + 1];
    strcpy(charset, SQLCHARSETSTRING_UTF8);

    init_SQLDESC_ID(&sql_src, SQLCLI_CURRENT_VERSION, string_data, NULL, NULL, 0, charset);
    sql_src.identifier_len = stmt->getSrcStrSize();
    sql_src.identifier = new (currContext->exHeap()) char[sql_src.identifier_len + 2];
    stmt->copyOutSourceStr((char *)sql_src.identifier, sql_src.identifier_len);

    // int isoMapping = SQLCHARSETCODE_ISO_MAPPING; //;
    // if (isoMapping != 0)
    // retcode = SQL_EXEC_SetDescItem(&sql_src, 1, SQLDESC_CHAR_SET,
    //			       isoMapping,0);

    aqrSI->setRetrySqlSource(&sql_src);

    aqrSI->setRetryInputDesc(stmt->aqrStmtInfo()->getRetryInputDesc());
    aqrSI->setRetryOutputDesc(stmt->aqrStmtInfo()->getRetryOutputDesc());
    aqrSI->setRetryPrepareFlags(stmt->aqrStmtInfo()->getRetryPrepareFlags());
    aqrSI->setRetryFirstFetch(stmt->aqrStmtInfo()->retryFirstFetch());
    aqrSI->setRetryAfterExec(stmt->aqrStmtInfo()->retryAfterExec());

    // Save of any statement attributes or other info from the old
    // statement object into the AQR statement attribute struct
    if (stmt) {
      aqrSI->saveAttributesFromStmt(stmt);
    }

    aqrSI->saveStatementHandle(stmt->getStmtId());
    aqr->setAqrStmtInfo(aqrSI);

  } else
    return retcode;

  NABoolean done = FALSE;
  int numRetries = 0;
  int retries = 0;
  int delay = 0;
  int type = 0;
  int numCQDs = 0;
  char *cqdStr = NULL;
  int cmpInfo = 0;
  ComCondition *cc = NULL;
  ComCondition *errCond = NULL;
  int aqrDelay = 0;
  int aqrCmpInfo = 0;

  while (NOT done) {
    if (NOT SQLCLI_GetRetryInfo(cliGlobals, retcode, aqrSI, afterPrepare, afterExec, afterFetch, afterCEFC, retries,
                                delay, type, numCQDs, cqdStr, cmpInfo, cc))
      done = TRUE;
    else {
      // need to check again if aqr is enabled on root because
      // the previous retry may have disabled AQR
      stmt = currContext->getStatement(statement_id);
      if (stmt && stmt->getRootTdb()) {
        if (NOT stmt->getRootTdb()->aqrEnabledForSqlcode(sqlcode))
          done = TRUE;
        else if (checkAqrWnr && preventAqrWnr(currContext, stmt, addWarning8737))
          done = TRUE;
      }
      if (!done) {
        if (numRetries == 0) {
          // first retry.
          // Save the error condition which caused this retry.
          if (cc) {
            errCond = ComCondition::allocate(currContext->exHeap());
            *errCond = *cc;
          }

          // set cqds before reprepare
          if (numCQDs > 0) {
            // set the desired cqds before recompiling the query.
            retcode = aqr->setCQDs(numCQDs, cqdStr, currContext);
            if (retcode < 0) {
              return retcode;
            }
          }

          if (cmpInfo == 1) {
            // send relevant information to compiler through a
            // control session statement.
            retcode = aqr->setCompilerInfo(stmt->getUniqueStmtId(), errCond, currContext);
            if (retcode < 0) {
              return retcode;
            }
          }
          aqrDelay = delay;
          aqrCmpInfo = cmpInfo;
        }

        if (numRetries < retries) {
          if (type == AQRInfo::RETRY_WITH_ESP_CLEANUP) aqr->setEspCleanup(TRUE);

          // Before deallocating the statement, set an indication in the
          // master stats for this query id to indicate that AQR is being
          // done

          // Get the statement again in case if was reallocated in the
          // previous loop.
          stmt = currContext->getStatement(statement_id);
          if (stmt && stmt->getStmtStats()) {
            stmt->getStmtStats()->setAqrInProgress(TRUE);
            ExMasterStats *ms = (stmt->getStmtStats())->getMasterStats();
            if (ms) {
              ms->setAqrLastErrorCode(sqlcode);
              ms->setNumAqrRetries(numRetries + 1);
              ms->setAqrDelayBeforeRetry(delay);
            }
          }
          if (afterPrepare && ((type == AQRInfo::RETRY_MXCMP_KILL))) {
            // Kill the current mxcmp. A new one will get started
            // when  the query is prepared
            ExeCliInterface *cliInterface = new (currContext->exHeap())
                ExeCliInterface(currContext->exHeap(), SQLCHARSETCODE_UTF8, currContext, NULL);
            ComDiagsArea *tmpDiagsArea = &diags;
            retcode =
                cliInterface->executeImmediate((char *)"SELECT TESTEXIT;", NULL, NULL, TRUE, NULL, 0, &tmpDiagsArea);
            // ignore errors from this call.

          } else
              // only kill mxcmp if the retry error happened suring a prepare
              // otherwise return it to the user.
              if (!afterPrepare && ((type == AQRInfo::RETRY_MXCMP_KILL))) {
            if (stmt->getStmtStats()) stmt->getStmtStats()->setAqrInProgress(FALSE);
            return retcode;
          }

          // deallocate the statement before the delay.

          StmtStats *saveStmtStat = NULL;
          if (stmt) saveStmtStat = stmt->getStmtStats();

          // if the original statement_id had its 'handle' field pointed to
          // StatementInfo struct to do stmt lookup optimization(see method
          // SQLCLI_PerformTasks for details), reset that field.
          // The original statement now deallocated and
          // any reference to it(StatementInfo struct points to it) is invalid.
          if (((statement_id->name_mode != desc_handle) && (afterCEFC)) || (statement_id->name_mode == stmt_name))
            statement_id->handle = NULL;

          retcode = SQLCLI_RetryDeallocStmt(cliGlobals, aqrSI);

          if (isERROR(retcode)) {
            if (saveStmtStat) {
              saveStmtStat->setAqrInProgress(FALSE);
              ExMasterStats *ms = saveStmtStat->getMasterStats();
              if (ms) {
                ms->setAqrLastErrorCode(0);
                ms->setNumAqrRetries(0);
                ms->setAqrDelayBeforeRetry(0);
              }
            }
          }
          // delay for the specified 'delay'
          if (NOT isERROR(retcode) && (delay >= 0)) {
            Sleep(delay * 1000);
          }

          numRetries++;

          if (NOT isERROR(retcode)) {
            int flags = aqrSI->getRetryPrepareFlags();
            flags |= PREPARE_AUTO_QUERY_RETRY;

            if (type == AQRInfo::RETRY_WITH_DECACHE)
              flags |= PREPARE_WITH_DECACHE;
            else if (type == AQRInfo::RETRY_SENTRY_PRIV_CHECK)
              flags |= (PREPARE_SENTRY_PRIV_RECHECK | PREPARE_WITH_DECACHE);

            // log an ems event, unless disabled or if regressions
            // are running.
            char *sqlmxRegr = NULL;
            sqlmxRegr = getenv("SQLMX_REGRESS");
            if ((currContext->getSessionDefaults()->getAqrEmsEvent()) && (sqlmxRegr == NULL)) {
              int sqlcode = 0;
              int nskcode = 0;
              char *stringParam1 = NULL;
              int intParam1 = ComDiags_UnInitialized_Int;
              char emsText[300];
              if (errCond) {
                sqlcode = errCond->getSQLCODE();
                nskcode = errCond->getNskCode();
                if (errCond->getOptionalStringCharSet(0) == CharInfo::ISO88591)
                  stringParam1 = (char *)errCond->getOptionalString(0);
                intParam1 = errCond->getOptionalInteger(0);

                str_sprintf(emsText,
                            "AutoQueryRetry will be attempted for Sqlcode=%d, Nskcode=%d, StringParam=%s, IntParam=%d",
                            sqlcode, nskcode, (stringParam1 ? stringParam1 : "NULL"),
                            (intParam1 != ComDiags_UnInitialized_Int ? intParam1 : 0));
              } else {
                str_sprintf(emsText, "AutoQueryRetry will be attempted");
              }

              SQLMXLoggingArea::logExecRtInfo(NULL, 0, emsText, 0);
            }
            QRWARN("AutoQueryRetry will be attempted for Sqlcode=%d, retry count=%d", sqlcode, numRetries);

            if (sqlcode == -CLI_HISTOGRAM_UPDATE) flags |= PREPARE_RECOMP_USE_NATABLE_CACHE;

            retcode = SQLCLI_RetryQuery(cliGlobals, aqrSI, flags, afterPrepare, afterExec, afterFetch, afterCEFC,
                                        saveStmtStat);
          }

          if (type == AQRInfo::RETRY_WITH_ESP_CLEANUP) aqr->setEspCleanup(FALSE);
        } else {
          done = TRUE;
        }
      }
    }
  }

  if (numRetries > 0) {
    if (numCQDs > 0) {
      // reset the desired cqds after recompiling the query.
      int rc = aqr->resetCQDs(numCQDs, cqdStr, currContext);

      if (rc < 0) return rc;
    }

    if (aqrCmpInfo == 1) {
      // reset the control session stmt.
      int rc = aqr->resetCompilerInfo(stmt->getUniqueStmtId(), errCond, currContext);
      if (rc < 0) return rc;
    }

    // if recomp warnings are to be returned, then add a warning.
    // Also return the error which was previously returned to the caller
    // causing the retry.
    if (currContext->getSessionDefaults()->aqrWarnings() != 0) {
      NABoolean add100 = FALSE;
      if ((NOT isERROR(retcode)) && (diags.getNumber(DgSqlCode::ERROR_) == 0) && (diags.mainSQLCODE() == SQL_EOF)) {
        diags.removeFinalCondition100();
        add100 = TRUE;
      }

      diags << DgSqlCode(EXE_RECOMPILE_AUTO_QUERY_RETRY)  // a warning only
            << DgInt0(numRetries) << DgInt1(aqrDelay) << DgString0("")
            << (errCond ? DgString1("See next entry for the error that caused this retry.") : DgString1(""));

      if (errCond) {
        ComCondition *newCC = diags.makeNewCondition();
        *newCC = *errCond;
        if (isERROR(newCC->getSQLCODE())) newCC->setSQLCODE(-newCC->getSQLCODE());
        diags.acceptNewCondition();
      }

      if (NOT isERROR(retcode) && addWarning8737) diags << DgSqlCode(CLI_INVALID_ROWS_AFFECTED);

      if (add100) {
        retcode = diags.mainSQLCODE();
        diags << DgSqlCode(SQL_EOF);
      }

      if (isSUCCESS(retcode))
        retcode = EXE_RECOMPILE_AUTO_QUERY_RETRY;
      else if (isEOF(retcode)) {
        // if (aqrSI->getRetryOutputDesc() == NULL)
        // retcode = EXE_RECOMPILE_AUTO_QUERY_RETRY;
      }
    }
  }

  if ((isERROR(retcode)) && (numRetries > 0)) {
    // close this stmt. Ignore errors, the stmt may already be closed.
    // Rewind diags area to get rid of any errors from SQLCLI_CloseStmt.
    int oldDiagsAreaMark = diags.mark();
    int rc = SQLCLI_CloseStmt(cliGlobals, aqrSI->getRetryStatementId());
    diags.rewind(oldDiagsAreaMark, TRUE);
  }

  // Retry done. Clear up any retry related information
  // before returning.
  if (charset) currContext->exHeap()->deallocateMemory((void *)charset);
  if (sql_src.identifier) currContext->exHeap()->deallocateMemory((void *)sql_src.identifier);
  if (stmtId) NADELETEBASIC(stmtId, currContext->exHeap());
  if (id) NADELETEBASIC(id, currContext->exHeap());

  Statement *newStmt = currContext->getStatement(aqrSI->getRetryStatementId());
  StmtStats *stmtStats = (newStmt ? newStmt->getStmtStats() : NULL);
  if (stmtStats) stmtStats->setAqrInProgress(FALSE);

  aqr->clearRetryInfo();

  if (aqrSI) {
    if (aqrSI->getRetryTempInputDesc()) {
      SQLCLI_DeallocDesc(cliGlobals, aqrSI->getRetryTempInputDesc());
      NADELETEBASIC(aqrSI->getRetryTempInputDesc()->module, currContext->exHeap());
      NADELETEBASIC(aqrSI->getRetryTempInputDesc(), currContext->exHeap());
      aqrSI->setRetryTempInputDesc(NULL);
    }

    if (aqrSI->getRetryTempOutputDesc()) {
      SQLCLI_DeallocDesc(cliGlobals, aqrSI->getRetryTempOutputDesc());
      NADELETEBASIC(aqrSI->getRetryTempOutputDesc()->module, currContext->exHeap());
      NADELETEBASIC(aqrSI->getRetryTempOutputDesc(), currContext->exHeap());
      aqrSI->setRetryTempOutputDesc(NULL);
    }

    // aqrSI->clearRetryInfo();
    NADELETE(aqrSI, AQRStatementInfo, currContext->exHeap());
    aqrSI = NULL;
  }

  if (errCond) errCond->deAllocate();

  return retcode;
}

////////////////////////////////////////////////////////////////////////
// This method is called by various Exec, Fetch and Close methods.
// It performs the tasks (see enum CliTasks) that are asked for.
//
////////////////////////////////////////////////////////////////////////
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
    /*IN*/ SQLCLI_PTR_PAIRS output_ptr_pairs[]) {
  int retcode = SUCCESS;

  if (!statement_id) return -CLI_STMT_NOT_EXISTS;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  StatementInfo *stmtInfo = NULL;
  StmtStats *stmtStats = NULL;
  if ((tasks & CLI_PT_OPT_STMT_INFO) && (statement_id->name_mode != desc_handle)) {
    stmtInfo = (StatementInfo *)(statement_id->handle);
    if (stmtInfo == NULL) {
      // when do we deallocate this heap? Or do we?
      CollHeap *heap = cliGlobals->currContext()->exCollHeap();
      stmtInfo = new (heap) StatementInfo();
      statement_id->handle = stmtInfo;
    }
  }

  // create initial context, if first call, and add module, if any.
  if (tasks & CLI_PT_PROLOGUE) {
    retcode = CliPrologue(cliGlobals, ((stmtInfo && stmtInfo->moduleAdded()) ? NULL : statement_id->module));
    if (isERROR(retcode)) return retcode;

    if (stmtInfo) stmtInfo->setModuleAdded(TRUE);
  } else {
    // module must have been added
    if ((statement_id->module) && (statement_id->module->module_name) &&
        (!currContext.moduleAdded(statement_id->module))) {
      diags << DgSqlCode(-CLI_MODULE_NOT_ADDED);
      return SQLCLI_ReturnCode(&currContext, -CLI_MODULE_NOT_ADDED);
    }
  }

  if (tasks & CLI_PT_CLEAR_DIAGS) {
    // Don't clear diags if this is a recursive call.  Otherwise,
    // warning conditions that have acumulated will be lost.
    if (currContext.getNumOfCliCalls() == 1) diags.clear();
  }

  /* stmt must exist */
  Statement *stmt = NULL;
  if ((stmtInfo) && (stmtInfo->statement()))
    stmt = stmtInfo->statement();
  else {
    stmt = currContext.getStatement(statement_id);
    if (!stmt) {
      diags << DgSqlCode(-CLI_STMT_NOT_EXISTS);
      return SQLCLI_ReturnCode(&currContext, -CLI_STMT_NOT_EXISTS);
    }
    if (stmtInfo) {
      stmtInfo->statement() = stmt;
      stmt->setStmtInfo(stmtInfo);
      if (tasks & CLI_PT_CLEAREXECFETCHCLOSE) stmt->setOltOpt(TRUE);
    }
  }
  stmt->getGlobals()->setNoNewRequest(FALSE);

  Descriptor *input_desc = NULL;
  Descriptor *output_desc = NULL;

  va_list cpy;

  if (num_input_ptr_pairs || num_output_ptr_pairs) va_copy(cpy, ap);

  if (tasks & CLI_PT_GET_INPUT_DESC) {
    NABoolean setPtrs = FALSE;
    if ((stmtInfo) && (stmtInfo->inputDesc())) {
      input_desc = stmtInfo->inputDesc();
      setPtrs = TRUE;
      // If this call is made from multiple callers and the stack address
      // of input pointers are not the same, then this optimization
      // incorrectly uses the previously set pointers.
      // It needs to be enhanced so it either validates that the stack
      // is the same as the previous call or that the pointers are
      // not stack addresses.
      // Removing this optimization for now.
      setPtrs = TRUE;
    } else {
      if (!input_descriptor)
        input_desc = stmt->getDefaultDesc(SQLWHAT_INPUT_DESC);
      else
        input_desc = currContext.getDescriptor(input_descriptor);

      if (stmtInfo) stmtInfo->inputDesc() = input_desc;

      setPtrs = TRUE;
    }

    if ((setPtrs) && (num_input_ptr_pairs > 0)) {
      /* descriptor must exist */
      if (!input_desc) {
        diags << DgSqlCode(-CLI_DESC_NOT_EXISTS);
        return SQLCLI_ReturnCode(&currContext, -CLI_DESC_NOT_EXISTS);
      }
      retcode = local_SetDescPointers(input_desc, 1, num_input_ptr_pairs, num_ap, &cpy, input_ptr_pairs);
      if (isERROR(retcode)) return SQLCLI_ReturnCode(&currContext, retcode);
    }
  }

  if (tasks & CLI_PT_GET_OUTPUT_DESC) {
    NABoolean setPtrs = FALSE;
    if ((stmtInfo) && (stmtInfo->outputDesc())) {
      output_desc = stmtInfo->outputDesc();
      setPtrs = TRUE;
      // If this call is made from multiple callers and the stack address
      // of output pointers are not the same, then this optimization
      // incorrectly uses the previously set pointers.
      // It needs to be enhanced so it either validates that the stack
      // is the same as the previous call or that the pointers are
      // not stack addresses.
      // Removing this optimization for now.
      setPtrs = TRUE;
    } else {
      if (!output_descriptor)
        output_desc = stmt->getDefaultDesc(SQLWHAT_OUTPUT_DESC);
      else
        output_desc = currContext.getDescriptor(output_descriptor);

      if (stmtInfo) stmtInfo->outputDesc() = output_desc;

      setPtrs = TRUE;
    }

    if ((setPtrs) && (num_output_ptr_pairs > 0)) {
      // if we do have something to set... the output descriptor
      // had better be available by the time we call local_SetDescPointers
      //
      if (!output_desc) {
        diags << DgSqlCode(-CLI_DESC_NOT_EXISTS);
        return SQLCLI_ReturnCode(&currContext, -CLI_DESC_NOT_EXISTS);
      }

      retcode = local_SetDescPointers(output_desc, 1, num_output_ptr_pairs, num_ap, &cpy, output_ptr_pairs);
      if (isERROR(retcode)) return SQLCLI_ReturnCode(&currContext, retcode);
    }
  }

  va_end(ap);
  stmtStats = stmt->getStmtStats();
  // Execute the statement.
  if (tasks & CLI_PT_EXEC) {
    if (tasks & CLI_PT_SET_AQR_INFO) {
      if (stmt->aqrStmtInfo()) {
        stmt->aqrStmtInfo()->setRetryInputDesc(input_descriptor);
        stmt->aqrStmtInfo()->setRetryOutputDesc(output_descriptor);
      }
    }

    // if priority of master was changed for execution and didn't get
    // changed back to its original priority at the end of that stmt,
    // switch it back to its original priority now.
    // Do this only for root cli level.
    if ((currContext.getNumOfCliCalls() == 1) && (cliGlobals->priorityChanged())) {
      ComRtSetProcessPriority(cliGlobals->myPriority(), FALSE);
      cliGlobals->setPriorityChanged(FALSE);
    }

    int retcode;
    retcode = CheckNOSQLAccessMode(*cliGlobals);
    if (isERROR(retcode)) {
      return retcode;
    }

    if ((stmt->getRootTdb()) && (((ComTdb *)stmt->getRootTdb())->getCollectStats())) {
      stmt->getGlobals()->setStatsEnabled(TRUE);
#ifdef _DEBUG
      if (getenv("DISABLE_STATS")) stmt->getGlobals()->setStatsEnabled(FALSE);
#endif
    }

    if (!stmt->noWaitOpPending()) stmt->getGlobals()->clearCancelState();

    // Check to see if we need to revalidate any Apache Sentry
    // privilege checks
    ex_root_tdb *rootTdb = stmt->getRootTdb();
    if (rootTdb) {
      long sentryAuthExpirationTimeStamp = rootTdb->sentryAuthExpirationTimeStamp();
      if ((sentryAuthExpirationTimeStamp) && (sentryAuthExpirationTimeStamp < NA_JulianTimestamp())) {
        retcode = -EXE_APACHE_SENTRY_PRIV_EXPIRED;
        diags << DgSqlCode(retcode);
        return CliEpilogue(cliGlobals, statement_id, retcode);
      }
    }

    // if OLT optimization is being done, call our special
    // execute method(doOltExecute).
    // Only done for embedded, waited requests...these requests
    // have their version set to VERSION_1.
    NABoolean doNormalExecute = FALSE;
    NABoolean reExecute = FALSE;

    if (stmtInfo && stmt->oltOpt() &&
        (statement_id->version == SQLCLI_STATEMENT_VERSION_1 ||
         (statement_id->version == SQLCLI_STATEMENT_VERSION_2))) {
      retcode = stmt->doOltExecute(cliGlobals, input_desc, output_desc, diags, doNormalExecute, reExecute);

      // redrive this query using normal execution, if doNormalExecute
      // is TRUE. This could happen for certain cases, like timestamp
      // mismatch or blown away opens. These cases are not handled
      // by fastpath OLT execution.
      if (NOT doNormalExecute) {
        if (stmt->getState() != Statement::CLOSE_) stmt->close(diags);
        return CliEpilogue(cliGlobals, statement_id, retcode);
      }
    }

    if (input_desc) {
      if (stmt->computeInputDescBulkMoveInfo()) {
        input_desc->reComputeBulkMoveInfo();
        stmt->setComputeInputDescBulkMoveInfo(FALSE);
      } else if (NOT input_desc->bulkMoveDisabled()) {
        if (input_desc->bulkMoveStmt() != stmt) input_desc->reComputeBulkMoveInfo();
      }
    }

    Statement::ExecState execute_state = Statement::INITIAL_STATE_;
    if (reExecute) execute_state = Statement::RE_EXECUTE_;

    retcode = stmt->execute(cliGlobals, input_desc, diags, execute_state);
    if (isERROR(retcode)) {
      if (stmt->aqrStmtInfo()) stmt->aqrStmtInfo()->setRetryAfterExec(TRUE);

      // If statement is in a closeable state, then
      // close on error but return the retcode from execute.
      if ((tasks & CLI_PT_CLOSE_ON_ERROR) &&
          (stmt->getState() == Statement::OPEN_ || stmt->getState() == Statement::FETCH_ ||
           stmt->getState() == Statement::EOF_ || stmt->getState() == Statement::RELEASE_TRANS_ ||
           stmt->getState() == Statement::PREPARE_))
        stmt->close(diags);
      return CliEpilogue(cliGlobals, statement_id, retcode);
    }

    // if bulk move was done, remember the statement it was done for.
    if ((input_desc) && (NOT input_desc->bulkMoveDisabled())) {
      // if (getenv("BULKMOVEWARN"))
      //   diags << DgSqlCode(EXE_ERROR_NOT_IN_USE_8350);

      input_desc->bulkMoveStmt() = stmt;
    }
  }

  // do a fetch to retrieve one row or EOD.
  if (tasks & CLI_PT_FETCH) {
    if (tasks & CLI_PT_SET_AQR_INFO) {
      if (stmt->aqrStmtInfo()) {
        stmt->aqrStmtInfo()->setRetryOutputDesc(output_descriptor);
      }
    }

    // In case of multiple cursors fetching into the same descriptor,
    // the bulkMove flag in the descriptor applies to the statement/cursor
    // it was computed for. Validate that the current statement is the
    // one bulk move was done for. If not, reset bulk move flag so
    // bulk move compatibility checks could be done again in
    // outputValues.

    // disable rowwise rowset fetch for ExeUtil operator.
    // This is done so status messages could be returned to caller as
    // soon as they are sent up by executor. With RWRS, returned msgs
    // will be blocked until the output rowset is filled up.
    if (output_desc) {
      if (stmt->computeOutputDescBulkMoveInfo()) {
        output_desc->reComputeBulkMoveInfo();
        stmt->setComputeOutputDescBulkMoveInfo(FALSE);

        if ((currContext.getSessionDefaults()->getCliBulkMove() == FALSE) ||
            ((stmt->getRootTdb()) &&
             ((stmt->getRootTdb()->getQueryType() == ComTdbRoot::SQL_EXE_UTIL) ||
              (stmt->getRootTdb()->getQueryType() == ComTdbRoot::SQL_DDL_WITH_STATUS)) &&
             (NOT stmt->getRootTdb()->exeUtilRwrs()))) {
          output_desc->setBulkMoveDisabled(TRUE);
          // mantis-9992 by haiyan
          output_desc->setRowwiseRowsetDisabled(FALSE);
        }
      } else if (NOT output_desc->bulkMoveDisabled()) {
        if ((currContext.getSessionDefaults()->getCliBulkMove() == FALSE) ||
            ((stmt->getRootTdb()) &&
             ((stmt->getRootTdb()->getQueryType() == ComTdbRoot::SQL_EXE_UTIL) ||
              (stmt->getRootTdb()->getQueryType() == ComTdbRoot::SQL_DDL_WITH_STATUS)) &&
             (NOT stmt->getRootTdb()->exeUtilRwrs()))) {
          output_desc->setBulkMoveDisabled(TRUE);
          // mantis-9992 by haiyan
          output_desc->setRowwiseRowsetDisabled(FALSE);
        } else if (output_desc->bulkMoveStmt() != stmt) {
          output_desc->reComputeBulkMoveInfo();
          output_desc->bulkMoveStmt() = stmt;
        }
      }
    }

    // if this statement is already in FETCH_ state
    if ((stmt->getState() == Statement::INITIAL_) || (stmt->getState() == Statement::OPEN_)) {
      if (stmt->aqrStmtInfo()) stmt->aqrStmtInfo()->setRetryFirstFetch(TRUE);
    } else if (stmt->aqrStmtInfo())
      stmt->aqrStmtInfo()->setRetryFirstFetch(FALSE);

    retcode = stmt->fetch(cliGlobals, output_desc, diags, TRUE);

    if (isERROR(retcode)) {
      return CliEpilogue(cliGlobals, statement_id, retcode);
    } else if (retcode == WARNING) {
      int mainSQLcode = diags.mainSQLCODE();
    }

    // if select into query, then make sure that atmost one
    // row is returned by executor. More than one would
    // result in an error.
    if ((stmt->isSelectInto()) && (retcode == SUCCESS)) {
      // BertBert VV
      if (stmt->isEmbeddedUpdateOrDelete() || stmt->isStreamScan()) {
        // For streams and destructive selects, we don't want the above behavior,
        //  instead, we should just return the first row.
      }
      // BertBert ^^
      else {
        // select into and a row was returned.
        // See if we can get one more row.
        // Do not send in an output desc. We want
        // to return the first row to application.
        // This is being consistent with SQL/MP behavior.
        retcode = stmt->fetch(cliGlobals, 0 /*no output desc*/, diags, TRUE);
        if (retcode == SUCCESS) {
          diags << DgSqlCode(-CLI_SELECT_INTO_ERROR);
          stmt->close(diags);
          return CliEpilogue(cliGlobals, statement_id, -CLI_SELECT_INTO_ERROR);
        }

        if (retcode == SQL_EOF) {
          // remove warning 100 from diags.
          diags.removeFinalCondition100();

          retcode = SUCCESS;
        }
      }
    }

    if ((output_desc == NULL) && (retcode == SQL_EOF)) {
      // remove warning 100 from diags.
      diags.removeFinalCondition100();

      retcode = SUCCESS;
    }
  }

  // Close the statement.
  if (tasks & CLI_PT_CLOSE) {
    ex_root_tdb *rootTdb = stmt->getRootTdb();
    int tretcode = stmt->close(diags);

    if (isERROR(tretcode)) {
      return CliEpilogue(cliGlobals, statement_id, tretcode);
    }

    // Collect useful RT stats if needed.
    if (rootTdb && rootTdb->isAutoCollectRTStats()) currContext.collectSpecialRTStats();

    // if priority of master was changed for execution,
    // switch it back to its original priority.
    // Do this only for root cli level.
    if ((currContext.getNumOfCliCalls() == 1) && (cliGlobals->priorityChanged())) {
      ComRtSetProcessPriority(cliGlobals->myPriority(), FALSE);
      cliGlobals->setPriorityChanged(FALSE);
    }
  }

  if (tasks & CLI_PT_SPECIAL_END_PROCESS) {
    if (stmt->handleUpdDelCurrentOf(diags) == SQL_EOF) retcode = SQL_EOF;
  }

  if (tasks & CLI_PT_EPILOGUE)
    return CliEpilogue(cliGlobals, statement_id, retcode);
  else
    return SQLCLI_ReturnCode(&currContext, retcode);
}

int SQLCLI_CloseStmt(/*IN*/ CliGlobals *cliGlobals,
                     /*IN*/ SQLSTMT_ID *statement_id) {
  int tasks = CLI_PT_PROLOGUE | CLI_PT_CLOSE | CLI_PT_EPILOGUE |
              // stmt lookup opt also done for version_2 queries
              // with stmt_name name mode. Currently only jdbc
              // could make use of this optimization.
              (((statement_id->version == SQLCLI_STATEMENT_VERSION_2) && (statement_id->name_mode == stmt_name))
                   ? CLI_PT_OPT_STMT_INFO
                   : 0);

  return SQLCLI_PerformTasks(cliGlobals, tasks, statement_id, NULL, NULL, 0, 0, 0, VA_LIST_NULL, 0, 0);
}

int SQLCLI_Exec(/*IN*/ CliGlobals *cliGlobals,
                /*IN*/ SQLSTMT_ID *statement_id,
                /*IN  OPTIONAL*/ SQLDESC_ID *input_descriptor,
                /*IN*/ int num_ptr_pairs,
                /*IN*/ int num_ap,
                /*IN*/ va_list ap,
                /*IN*/ SQLCLI_PTR_PAIRS ptr_pairs[]) {
  int tasks = CLI_PT_PROLOGUE | CLI_PT_GET_INPUT_DESC | CLI_PT_EXEC | CLI_PT_SET_AQR_INFO | CLI_PT_EPILOGUE |
              // stmt lookup opt also done for version_2 queries
              // with stmt_name name mode. Currently only jdbc
              // could make use of this optimization.
              (((statement_id->version == SQLCLI_STATEMENT_VERSION_2) && (statement_id->name_mode == stmt_name))
                   ? CLI_PT_OPT_STMT_INFO
                   : 0);
  return SQLCLI_PerformTasks(cliGlobals, tasks, statement_id, input_descriptor, NULL, num_ptr_pairs, 0, num_ap, ap,
                             ptr_pairs, 0);
}

int SQLCLI_ExecClose(/*IN*/ CliGlobals *cliGlobals,
                     /*IN*/ SQLSTMT_ID *statement_id,
                     /*IN  OPTIONAL*/ SQLDESC_ID *input_descriptor,
                     /*IN*/ int num_ptr_pairs,
                     /*IN*/ int num_ap,
                     /*IN*/ va_list ap,
                     /*IN*/ SQLCLI_PTR_PAIRS ptr_pairs[]) {
  int tasks = CLI_PT_PROLOGUE | CLI_PT_GET_INPUT_DESC | CLI_PT_EXEC | CLI_PT_CLOSE_ON_ERROR | CLI_PT_EPILOGUE;
  return SQLCLI_PerformTasks(cliGlobals, tasks, statement_id, input_descriptor, NULL, num_ptr_pairs, 0, num_ap, ap,
                             ptr_pairs, 0);
}

/////////////////////////////////////////////////////////////////
// if sql_source->name_mode is string_data, then
// sql_source->identifier points to the sql_source string and
// identifier_len is the length of that string.
/////////////////////////////////////////////////////////////////
int SQLCLI_ExecDirect(/*IN*/ CliGlobals *cliGlobals,
                      /*IN*/ SQLSTMT_ID *statement_id,
                      /*IN*/ SQLDESC_ID *sql_source,
                      /*IN  OPTIONAL*/ SQLDESC_ID *input_descriptor,
                      /*IN*/ int num_ptr_pairs,
                      /*IN*/ int num_ap,
                      /*IN*/ va_list ap,
                      /*IN*/ SQLCLI_PTR_PAIRS ptr_pairs[]) {
  int retcode;

  // create initial context, if first call, and add module, if any.
  retcode = CliPrologue(cliGlobals, statement_id->module);
  if (isERROR(retcode)) return retcode;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  /* prepare the statement */

  Statement *stmt = currContext.getStatement(statement_id);
  /* stmt must exist */
  if (!stmt) {
    diags << DgSqlCode(-CLI_STMT_NOT_EXISTS);
    return SQLCLI_ReturnCode(&currContext, -CLI_STMT_NOT_EXISTS);
  }
  stmt->getGlobals()->clearCancelState();

  StrTarget strTarget;
  retcode = stmt->initStrTarget(sql_source, currContext, diags, strTarget);
  if (isERROR(retcode)) return SQLCLI_ReturnCode(&currContext, retcode);
  // CLI callers are not allowed to request PREPARE or EXEC DIRECT
  // operations on stored procedure result sets.
  if (stmt->getParentCall()) {
    diags << DgSqlCode(-EXE_UDR_RS_PREPARE_NOT_ALLOWED);
    return SQLCLI_ReturnCode(&currContext, -EXE_UDR_RS_PREPARE_NOT_ALLOWED);
  }

  // For ExecDirect, MXOSRVR calls SQL_EXEC_SetStmtAttr(NULL) to set the unique id
  // before calling SQL_EXEC_EXECDirect. So, we need to use them
  if (stmt->getUniqueStmtId() == NULL) stmt->setUniqueStmtId(NULL);
  // Set the StmtStats in the shared segment
  stmt->setStmtStats(FALSE);
  SQLCLI_Prepare_Setup_Pre(currContext, stmt, 1);
  UInt32 flags;
  SessionDefaults *sd = currContext.getSessionDefaults();
  if (sd != NULL && sd->getCallEmbeddedArkcmp())
    flags = PREPARE_USE_EMBEDDED_ARKCMP;
  else
    flags = 0;

  retcode = stmt->prepare(strTarget.getStr(), diags, NULL, 0L, strTarget.getIntCharSet(), TRUE, flags);

  SQLCLI_Prepare_Setup_Post(currContext, stmt, 1);

  if (isERROR(retcode)) return SQLCLI_ReturnCode(&currContext, retcode);

  stmt->issuePlanVersioningWarnings(diags);

  int tasks =
      CLI_PT_GET_INPUT_DESC | CLI_PT_EXEC | CLI_PT_FETCH | CLI_PT_CLOSE | CLI_PT_SPECIAL_END_PROCESS | CLI_PT_EPILOGUE;
  return SQLCLI_PerformTasks(cliGlobals, tasks, statement_id, input_descriptor, NULL, num_ptr_pairs, 0, num_ap, ap,
                             ptr_pairs, 0);
}
/////////////////////////////////////////////////////////////////
// if sql_source->name_mode is string_data, then
// sql_source->identifier points to the sql_source string and
// identifier_len is the length of that string.
/////////////////////////////////////////////////////////////////
int SQLCLI_ExecDirect2(/*IN*/ CliGlobals *cliGlobals,
                       /*IN*/ SQLSTMT_ID *statement_id,
                       /*IN*/ SQLDESC_ID *sql_source,
                       /*IN*/ int prepFlags,
                       /*IN  OPTIONAL*/ SQLDESC_ID *input_descriptor,
                       /*IN*/ int num_ptr_pairs,
                       /*IN*/ int num_ap,
                       /*IN*/ va_list ap,
                       /*IN*/ SQLCLI_PTR_PAIRS ptr_pairs[]) {
  int retcode;

  // create initial context, if first call, and add module, if any.
  retcode = CliPrologue(cliGlobals, statement_id->module);
  if (isERROR(retcode)) return retcode;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  /* prepare the statement */

  Statement *stmt = currContext.getStatement(statement_id);
  /* stmt must exist */
  if (!stmt) {
    diags << DgSqlCode(-CLI_STMT_NOT_EXISTS);
    return SQLCLI_ReturnCode(&currContext, -CLI_STMT_NOT_EXISTS);
  }
  stmt->getGlobals()->clearCancelState();

  StrTarget strTarget;
  retcode = stmt->initStrTarget(sql_source, currContext, diags, strTarget);
  if (isERROR(retcode)) return SQLCLI_ReturnCode(&currContext, retcode);
  // CLI callers are not allowed to request PREPARE or EXEC DIRECT
  // operations on stored procedure result sets.
  if (stmt->getParentCall()) {
    diags << DgSqlCode(-EXE_UDR_RS_PREPARE_NOT_ALLOWED);
    return SQLCLI_ReturnCode(&currContext, -EXE_UDR_RS_PREPARE_NOT_ALLOWED);
  }

  // For ExecDirect, MXOSRVR calls SQL_EXEC_SetStmtAttr(NULL) to set the unique id
  // before calling SQL_EXEC_EXECDirect. So, we need to use them
  if (stmt->getUniqueStmtId() == NULL) stmt->setUniqueStmtId(NULL);
  // Set the StmtStats in the shared segment
  stmt->setStmtStats(FALSE);
  SQLCLI_Prepare_Setup_Pre(currContext, stmt, 1);
  UInt32 tmpFlags;
  SessionDefaults *sd = currContext.getSessionDefaults();
  if (sd != NULL && sd->getCallEmbeddedArkcmp())
    tmpFlags = prepFlags | PREPARE_USE_EMBEDDED_ARKCMP;
  else
    tmpFlags = prepFlags;
  if (sql_source)

    retcode = stmt->prepare(strTarget.getStr(), diags, NULL, 0L, strTarget.getIntCharSet(), TRUE, tmpFlags);

  SQLCLI_Prepare_Setup_Post(currContext, stmt, 1);

  if (isERROR(retcode)) return SQLCLI_ReturnCode(&currContext, retcode);

  stmt->issuePlanVersioningWarnings(diags);

  int tasks =
      CLI_PT_GET_INPUT_DESC | CLI_PT_EXEC | CLI_PT_FETCH | CLI_PT_CLOSE | CLI_PT_SPECIAL_END_PROCESS | CLI_PT_EPILOGUE;
  return SQLCLI_PerformTasks(cliGlobals, tasks, statement_id, input_descriptor, NULL, num_ptr_pairs, 0, num_ap, ap,
                             ptr_pairs, 0);
}

int SQLCLI_ExecDirectDealloc(/*IN*/ CliGlobals *cliGlobals,
                             /*IN*/ SQLSTMT_ID *statement_id,
                             /*IN*/ SQLDESC_ID *sql_source,
                             /*IN  OPTIONAL*/ SQLDESC_ID *input_descriptor,
                             /*IN*/ int num_ptr_pairs,
                             /*IN*/ int num_ap,
                             /*IN*/ va_list ap,
                             /*IN*/ SQLCLI_PTR_PAIRS ptr_pairs[]) {
  int retCode1 =
      SQLCLI_ExecDirect(cliGlobals, statement_id, sql_source, input_descriptor, num_ptr_pairs, num_ap, ap, ptr_pairs);

  int retCode2 = SQLCLI_DeallocStmt(cliGlobals, statement_id);

  if (isERROR(retCode1)) return (retCode1);

  if (isERROR(retCode2)) return (retCode2);

  if (isWARNING(retCode1) || isEOF(retCode1)) return (retCode1);

  return retCode2;
}

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
                               /*IN*/ SQLCLI_PTR_PAIRS output_ptr_pairs[]) {
  int tasks = CLI_PT_PROLOGUE | CLI_PT_CLEAR_DIAGS | CLI_PT_GET_INPUT_DESC | CLI_PT_GET_OUTPUT_DESC | CLI_PT_EXEC |
              CLI_PT_FETCH | CLI_PT_CLOSE | CLI_PT_SET_AQR_INFO | CLI_PT_SPECIAL_END_PROCESS | CLI_PT_EPILOGUE |
              CLI_PT_CLEAREXECFETCHCLOSE | ((statement_id->name_mode == stmt_name) ? CLI_PT_OPT_STMT_INFO : 0);
  return SQLCLI_PerformTasks(cliGlobals, tasks, statement_id, input_descriptor, output_descriptor, num_input_ptr_pairs,
                             num_output_ptr_pairs, num_ap, ap, input_ptr_pairs, output_ptr_pairs);
}

int SQLCLI_ExecFetch(/*IN*/ CliGlobals *cliGlobals,
                     /*IN*/ SQLSTMT_ID *statement_id,
                     /*IN  OPTIONAL*/ SQLDESC_ID *input_descriptor,
                     /*IN*/ int num_ptr_pairs,
                     /*IN*/ int num_ap,
                     /*IN*/ va_list ap,
                     /*IN*/ SQLCLI_PTR_PAIRS ptr_pairs[]) {
  int tasks = CLI_PT_PROLOGUE | CLI_PT_GET_INPUT_DESC | CLI_PT_SET_AQR_INFO | CLI_PT_EXEC | CLI_PT_FETCH |
              CLI_PT_CLOSE | CLI_PT_SPECIAL_END_PROCESS | CLI_PT_EPILOGUE |
              // stmt lookup opt also done for version_2 queries
              // with stmt_name name mode. Currently only jdbc
              // could make use of this optimization.
              (((statement_id->version == SQLCLI_STATEMENT_VERSION_2) && (statement_id->name_mode == stmt_name))
                   ? CLI_PT_OPT_STMT_INFO
                   : 0);
  return SQLCLI_PerformTasks(cliGlobals, tasks, statement_id, input_descriptor, NULL, num_ptr_pairs, 0, num_ap, ap,
                             ptr_pairs, 0);
}

int SQLCLI_Fetch(/*IN*/ CliGlobals *cliGlobals,
                 /*IN*/ SQLSTMT_ID *statement_id,
                 /*IN  OPTIONAL*/ SQLDESC_ID *output_descriptor,
                 /*IN*/ int num_ptr_pairs,
                 /*IN*/ int num_ap,
                 /*IN*/ va_list ap,
                 /*IN*/ SQLCLI_PTR_PAIRS ptr_pairs[]) {
  int tasks = CLI_PT_PROLOGUE | CLI_PT_GET_OUTPUT_DESC | CLI_PT_FETCH | CLI_PT_SET_AQR_INFO | CLI_PT_EPILOGUE |
              // stmt lookup opt also done for version_2 queries
              // with stmt_name name mode. Currently only jdbc
              // could make use of this optimization.
              (((statement_id->version == SQLCLI_STATEMENT_VERSION_2) && (statement_id->name_mode == stmt_name))
                   ? CLI_PT_OPT_STMT_INFO
                   : 0);
  return SQLCLI_PerformTasks(cliGlobals, tasks, statement_id, NULL, output_descriptor, 0, num_ptr_pairs, num_ap, ap, 0,
                             ptr_pairs);
}

int SQLCLI_FetchClose(/*IN*/ CliGlobals *cliGlobals,
                      /*IN*/ SQLSTMT_ID *statement_id,
                      /*IN  OPTIONAL*/ SQLDESC_ID *output_descriptor,
                      /*IN*/ int num_ptr_pairs,
                      /*IN*/ int num_ap,
                      /*IN*/ va_list ap,
                      /*IN*/ SQLCLI_PTR_PAIRS ptr_pairs[]) {
  int tasks = CLI_PT_GET_OUTPUT_DESC | CLI_PT_FETCH | CLI_PT_CLOSE | CLI_PT_EPILOGUE;
  return SQLCLI_PerformTasks(cliGlobals, tasks, statement_id, NULL, output_descriptor, 0, num_ptr_pairs, num_ap, ap, 0,
                             ptr_pairs);
}

int SQLCLI_FetchMultiple(/*IN*/ CliGlobals *cliGlobals,
                         /*IN*/ SQLSTMT_ID *statement_id,
                         /*IN  OPTIONAL*/ SQLDESC_ID *output_descriptor,
                         /*IN*/ int rowset_size,
                         /*IN*/ int *rowset_status_ptr,
                         /*OUT*/ int *rowset_nfetched,
                         /*IN*/ int num_quadruple_fields,
                         /*IN*/ int num_ap,
                         /*IN*/ va_list ap,
                         /*IN*/ SQLCLI_QUAD_FIELDS quad_fields[]) {
  if (!cliGlobals) return -CLI_NO_CURRENT_CONTEXT;

  int retcode = SQLCLI_SetRowsetDescPointers(cliGlobals, output_descriptor, rowset_size, rowset_status_ptr, 1,
                                             num_quadruple_fields, num_ap, ap, quad_fields);

  if (isERROR(retcode)) return SQLCLI_ReturnCode(cliGlobals->currContext(), retcode);

  retcode = SQLCLI_Fetch(cliGlobals, statement_id, output_descriptor, 0, 0, VA_LIST_NULL, 0);

  if (isERROR(retcode)) return SQLCLI_ReturnCode(cliGlobals->currContext(), retcode);

  int tempRowsetNfetched;
  retcode = SQLCLI_GetRowsetNumprocessed(cliGlobals, output_descriptor, tempRowsetNfetched);
  *rowset_nfetched = tempRowsetNfetched;

  return retcode;
}

/////////////////////////////////////////////////////////////////////

int SQLCLI_CancelOperation(/*IN*/ CliGlobals *cliGlobals,
                           /*IN*/ long transid) {
  if (!cliGlobals) return -CLI_NO_CURRENT_CONTEXT;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  currContext.semaphoreLock();
  HashQueue stmtList(TRUE, *(currContext.getStatementList()));
  Statement *stmt;
  int retcode;
  int error;

  retcode = 0;
  error = -CLI_CANCEL_REJECTED;
  NABoolean anyOne = FALSE;
  stmtList.position();
  HBaseClient_JNI *hbaseClient = cliGlobals->getHBaseClient();
  if (hbaseClient != NULL && currContext.getTransaction() != NULL) {
    if (transid == currContext.getTransaction()->getTransid())
      hbaseClient->cancelOperation(currContext.getTransaction()->getTransid());
    else
      QRWARN(
          "TransID unmatched, do nothing. Transid from mxosrvr = %ld, "
          "current transidi = %ld",
          transid, currContext.getTransaction()->getTransid());
  }

  currContext.semaphoreRelease();

  return retcode;
}

int SQLCLI_Cancel(/*IN*/ CliGlobals *cliGlobals,
                  /*IN OPTIONAL*/ SQLSTMT_ID *statement_id) {
  if (!cliGlobals) return -CLI_NO_CURRENT_CONTEXT;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  // Enter a critical section for this async cancel request.
  ///////////////////////////////////////////////////////////
  currContext.semaphoreLock();
  HashQueue stmtList(TRUE, *(currContext.getStatementList()));
  // Copy to make getStatement thread-safe.
  Statement *stmt;
  int retcode;
  int error;

  if (statement_id) {
    stmt = currContext.getStatement(statement_id, &stmtList);
    if (!stmt) {
      currContext.semaphoreRelease();
      return -CLI_STMT_NOT_EXISTS;
    }
    retcode = stmt->cancel();
  } else {  // Cancel all statements on the ContextCli::statementList_.
    retcode = 0;
    error = -CLI_CANCEL_REJECTED;
    NABoolean anyOne = FALSE;

    stmtList.position();
    while (stmt = (Statement *)(stmtList.getNext())) {  // Report the least serious error.
                                                        // One success -> success.
                                                        // Otherwise -> -CLI_CANCEL_REJECTED.
      retcode = stmt->cancel();
      if (!retcode) {
        anyOne = TRUE;
        continue;
      }
    }
    if (anyOne) retcode = 0;
    if (retcode) retcode = error;
  }

  currContext.semaphoreRelease();
  ///////////////////////////////////////////////////////////
  // Exit the critical section.

  return retcode;
}
int SQLCLI_GetDescEntryCount(/*IN*/ CliGlobals *cliGlobals,

                             /*IN*/ SQLDESC_ID *desc_id,
                             /*IN*/ SQLDESC_ID *output_descriptor) {
  if (!cliGlobals) {
    return -CLI_NO_CURRENT_CONTEXT;
  }

  if (!desc_id || !output_descriptor) {
    return -CLI_INTERNAL_ERROR;
  }

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  Descriptor *desc = currContext.getDescriptor(desc_id);

  /* descriptor must exist */
  if (!desc) {
    diags << DgSqlCode(-CLI_DESC_NOT_EXISTS);
    return SQLCLI_ReturnCode(&currContext, -CLI_DESC_NOT_EXISTS);
  }
  Descriptor *output_desc = currContext.getDescriptor(output_descriptor);

  /* descriptor must exist */
  if (!output_desc) {
    diags << DgSqlCode(-CLI_DESC_NOT_EXISTS);
    return SQLCLI_ReturnCode(&currContext, -CLI_DESC_NOT_EXISTS);
  }

  int retcode = OutputValueIntoNumericHostvar(output_desc, 1, desc->getUsedEntryCount());

  if (isERROR(retcode)) return SQLCLI_ReturnCode(&currContext, retcode);

  return SUCCESS;
}

int SQLCLI_GetDescItems(/*IN*/ CliGlobals *cliGlobals,

                        /*IN*/ SQLDESC_ID *desc_id,
                        /*IN*/ SQLDESC_ITEM desc_items[],
                        /*IN*/ SQLDESC_ID *value_num_descriptor,
                        /*IN*/ SQLDESC_ID *output_descriptor) {
#if 0
  // sanity check
  if (!desc_id || !value_num_descriptor || !output_descriptor)
    return -CLI_INTERNAL_ERROR;
#endif

  int retcode;

  CollHeap *heap;
  ComDiagsArea *diagsArea;

  heap = cliGlobals->currContext()->exCollHeap();
  diagsArea = cliGlobals->currContext()->getDiagsArea();

  // create initial context, if first call, and add module, if any.
  retcode = CliPrologue(cliGlobals, desc_id->module);

  if (isERROR(retcode)) return -CLI_INTERNAL_ERROR;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  Descriptor *desc = currContext.getDescriptor(desc_id);

  /* descriptor must exist */
  if (!desc) {
    diags << DgSqlCode(-CLI_DESC_NOT_EXISTS);
    return SQLCLI_ReturnCode(&currContext, -CLI_DESC_NOT_EXISTS);
  }

  Descriptor *value_num_desc = currContext.getDescriptor(value_num_descriptor);

  /* descriptor must exist */
  if (!value_num_desc) {
    diags << DgSqlCode(-CLI_DESC_NOT_EXISTS);
    return SQLCLI_ReturnCode(&currContext, -CLI_DESC_NOT_EXISTS);
  }

  Descriptor *output_desc = currContext.getDescriptor(output_descriptor);

  /* descriptor must exist */
  if (!output_desc) {
    diags << DgSqlCode(-CLI_DESC_NOT_EXISTS);
    return SQLCLI_ReturnCode(&currContext, -CLI_DESC_NOT_EXISTS);
  }

  int outputUsedEntryCount = output_desc->getUsedEntryCount();

  for (int i = 0; i < outputUsedEntryCount; i++) {
    //
    // Get the entry number of the descriptor entry whose item is to be
    // retrieved.
    //
    int entry;
    retcode = InputValueFromNumericHostvar(value_num_desc, desc_items[i].value_num_desc_entry, entry);

    if (isERROR(retcode)) return SQLCLI_ReturnCode(&currContext, retcode);

    // check that the item entry is < the max num of occurrences specified
    // when the SQL descriptor area was allocated (must also be > 0)
    if ((entry < 1) || (entry > desc->getUsedEntryCount())) {
      diags << DgSqlCode(-CLI_ITEM_NUM_OUT_OF_RANGE) << DgInt0(desc->getMaxEntryCount());

      return SQLCLI_ReturnCode(&currContext, -CLI_ITEM_NUM_OUT_OF_RANGE);
    }

    int targetType, targetLen, targetPrecision, targetScale;
    int sourceLen, sourcePrecision, sourceScale;
    short sourceType;
    char *source;
    int temp;
    Long tempPtr;

    // set source, source type, source length, source precision
    // and source scale according to the desciptor item being choosen.
    // get the value of the source into temp if the data type is long
    // else get the address of descItem for char data types by calling
    // Descriptor::getDescItemPtr()
    switch (desc_items[i].item_id) {
      case SQLDESC_TYPE:
      case SQLDESC_DATETIME_CODE:
      case SQLDESC_TYPE_FS:
      case SQLDESC_LENGTH:
      case SQLDESC_OCTET_LENGTH:
      case SQLDESC_PRECISION:
      case SQLDESC_SCALE:
      case SQLDESC_INT_LEAD_PREC:
      case SQLDESC_NULLABLE:

      // charset and coll_seq are stored as long.
      case SQLDESC_CHAR_SET:
      case SQLDESC_COLLATION:

      case SQLDESC_UNNAMED:
      case SQLDESC_IND_TYPE:
      case SQLDESC_IND_LENGTH:

        // case SQLDESC_VAR_PTR:
        // case SQLDESC_IND_PTR:
      case SQLDESC_RET_LEN:
      case SQLDESC_RET_OCTET_LEN:
      case SQLDESC_IND_DATA:
      case SQLDESC_ROWSET_VAR_LAYOUT_SIZE:
      case SQLDESC_ROWSET_IND_LAYOUT_SIZE:
      case SQLDESC_ROWSET_SIZE:

        // case SQLDESC_ROWSET_STATUS_PTR:

      case SQLDESC_ROWSET_NUM_PROCESSED:
      case SQLDESC_ROWSET_HANDLE:
      case SQLDESC_PARAMETER_MODE:
      case SQLDESC_ORDINAL_POSITION:
      case SQLDESC_PARAMETER_INDEX:
      case SQLDESC_DATA_OFFSET:
      case SQLDESC_NULL_IND_OFFSET:
      case SQLDESC_ALIGNED_LENGTH: {
        retcode = desc->getDescItem(entry, desc_items[i].item_id, &temp, NULL, 0, NULL, 0);
        if (retcode != 0) return SQLCLI_ReturnCode(&currContext, retcode);

        source = (char *)&temp;
        sourceLen = sizeof(int);
        sourceType = REC_BIN32_SIGNED;
        sourcePrecision = 0;
        sourceScale = 0;
      } break;

      case SQLDESC_VAR_PTR:
      case SQLDESC_IND_PTR:
      case SQLDESC_ROWSET_STATUS_PTR:
      case SQLDESC_ROWWISE_ROWSET_PTR: {
        retcode = desc->getDescItem(entry, desc_items[i].item_id, &tempPtr, NULL, 0, NULL, 0);
        if (retcode != 0) return SQLCLI_ReturnCode(&currContext, retcode);

        source = (char *)&tempPtr;
        sourceLen = sizeof(Long);
        sourceType = REC_BIN64_SIGNED;
        sourcePrecision = 0;
        sourceScale = 0;
      } break;

      case SQLDESC_VAR_DATA:
      case SQLDESC_CHAR_SET_NAM:
      case SQLDESC_NAME:
      case SQLDESC_HEADING:
      case SQLDESC_TABLE_NAME:
      case SQLDESC_SCHEMA_NAME:
      case SQLDESC_CATALOG_NAME:

        // R2
        // SQLDESC_CHAR_SET_NAM, SQLDESC_CHAR_SET_CAT,
        // SQLDESC_CHAR_SET_SCH, SQLDESC_COLL_NAM,
        // SQLDESC_COLL_CAT and SQLDESC_COLL_SCH are exposed as
        // character strings.

      case SQLDESC_CHAR_SET_CAT:
      case SQLDESC_CHAR_SET_SCH:
      case SQLDESC_COLL_NAM:
      case SQLDESC_COLL_CAT:
      case SQLDESC_COLL_SCH:

      {
        retcode = desc->getDescItemPtr(entry, desc_items[i].item_id, &source, &sourceLen);
        if (retcode != 0) return SQLCLI_ReturnCode(&currContext, retcode);

        if (desc_items[i].item_id != SQLDESC_VAR_DATA) {
          sourceType = REC_BYTE_F_ASCII;
          sourcePrecision = 0;
          sourceScale = 0;
        } else {
          int sType;
          desc->getDescItem(entry, SQLDESC_TYPE_FS, &sType, NULL, 0, NULL, 0);
          sourceType = (short)sType;

          if ((sourceType >= REC_MIN_INTERVAL) && (sourceType <= REC_MAX_INTERVAL)) {
            desc->getDescItem(entry, SQLDESC_INT_LEAD_PREC, &sourcePrecision, 0, 0, 0, 0);
            desc->getDescItem(entry, SQLDESC_PRECISION, &sourceScale, 0, 0, 0, 0);
          } else if (sourceType == REC_DATETIME) {
            desc->getDescItem(entry, SQLDESC_DATETIME_CODE, &sourcePrecision, 0, 0, 0, 0);
            desc->getDescItem(entry, SQLDESC_PRECISION, &sourceScale, 0, 0, 0, 0);
          } else {
            desc->getDescItem(entry, SQLDESC_PRECISION, &sourcePrecision, 0, 0, 0, 0);
            desc->getDescItem(entry, SQLDESC_SCALE, &sourceScale, 0, 0, 0, 0);
          };
        }
      } break;

      default:
        diags << DgSqlCode(-CLI_INVALID_DESC_INFO_REQUEST);
        return SQLCLI_ReturnCode(&currContext, -CLI_INVALID_DESC_INFO_REQUEST);

        break;
    }

    //
    // Get the address of the target where the item's value is to be stored.
    //
    char *pTarget = 0;
    output_desc->getDescItem(i + 1, SQLDESC_VAR_PTR,
                             (int *)&pTarget,  // $$$$ hjz: don't assume this is not null hostvar
                             NULL, 0, NULL, 0);

    if (!pTarget) return -CLI_INTERNAL_ERROR;

    output_desc->getDescItem(i + 1, SQLDESC_TYPE_FS, &targetType, NULL, 0, NULL, 0);

    output_desc->getDescItem(i + 1, SQLDESC_OCTET_LENGTH, &targetLen, NULL, 0, NULL, 0);

    if ((targetType >= REC_MIN_INTERVAL) && (targetType <= REC_MAX_INTERVAL)) {
      output_desc->getDescItem(i + 1, SQLDESC_INT_LEAD_PREC, &targetPrecision, 0, 0, 0, 0);
      output_desc->getDescItem(i + 1, SQLDESC_PRECISION, &targetScale, 0, 0, 0, 0);
    } else if (targetType == REC_DATETIME) {
      output_desc->getDescItem(i + 1, SQLDESC_DATETIME_CODE, &targetPrecision, 0, 0, 0, 0);
      output_desc->getDescItem(i + 1, SQLDESC_PRECISION, &targetScale, 0, 0, 0, 0);
    } else {
      output_desc->getDescItem(i + 1, SQLDESC_PRECISION, &targetPrecision, 0, 0, 0, 0);
      output_desc->getDescItem(i + 1, SQLDESC_SCALE, &targetScale, 0, 0, 0, 0);
    };

    char *targetVCLen = NULL;
    short targetVCLenSize = 0;

    if (DFS2REC::isSQLVarChar(targetType)) {
      targetVCLen = pTarget;
      targetVCLenSize = SQL_VARCHAR_HDR_SIZE;  // user host vars have VCLen 2
      pTarget = &pTarget[targetVCLenSize];
    } else
      targetVCLenSize = 0;

    // Check for ansi compatibility of source and target types
    if (NOT(InputOutputExpr::areCompatible(sourceType, (short)targetType))) {
      diags << DgSqlCode(-EXE_CONVERT_NOT_SUPPORTED);
      return SQLCLI_ReturnCode(&currContext, -EXE_CONVERT_NOT_SUPPORTED);
    }

    // CharSet Phase I work.
    // To check the LHS and RHS character sets for the assignment
    // If the character set for the host var is UCS2,
    // then any conversion is legitimate;
    // If the character set for the host var is not UCS2,
    // then the charsets must be the same.
    //
    // The check is performed for VARIABLE_DATA and VARIABLE_POINTER.
    if (DFS2REC::isAnyCharacter(sourceType) && DFS2REC::isAnyCharacter(targetType)) {
      int csSource, csTarget;
      retcode = output_desc->getDescItem(i + 1, SQLDESC_CHAR_SET, &csTarget, NULL, 0, NULL, 0);
      if (retcode != 0) return SQLCLI_ReturnCode(&currContext, retcode);

      if ((desc_items[i].item_id == SQLDESC_VAR_DATA || desc_items[i].item_id == SQLDESC_VAR_PTR)) {
        retcode = desc->getDescItem(entry, SQLDESC_CHAR_SET, &csSource, NULL, 0, NULL, 0);
        if (retcode != 0) return SQLCLI_ReturnCode(&currContext, retcode);

        if (NOT CharInfo::isAssignmentCompatible((CharInfo::CharSet)csTarget, (CharInfo::CharSet)csSource)) {
          // Error 8896: The character set $0~string0 of a host variable does not match $1~string1 of the corresponding
          // descriptor item (entry $2~Int0).
          diags << DgSqlCode(-CLI_CHARSET_MISMATCH) << DgString0(CharInfo::getCharSetName((CharInfo::CharSet)csTarget))
                << DgString1(CharInfo::getCharSetName((CharInfo::CharSet)csSource)) << DgInt0(entry);
          return SQLCLI_ReturnCode(&currContext, -CLI_CHARSET_MISMATCH);
        }
      }
    }

    if (source) {
      // if the target is REC_DECIMAL_LS and the source is NOT
      // REC_DECIMAL_LSE, convert the source to REC_DECIMAL_LSE first.
      char *intermediate = NULL;
      if ((targetType == REC_DECIMAL_LS) && (sourceType != REC_DECIMAL_LSE)) {
        intermediate = new (heap) char[targetLen - 1];
        if (convDoIt(source, sourceLen, (short)sourceType, sourcePrecision, sourceScale, intermediate, targetLen - 1,
                     REC_DECIMAL_LSE, targetPrecision, targetScale, targetVCLen, targetVCLenSize, heap,
                     &diagsArea) != ex_expr::EXPR_OK) {
          NADELETEBASIC(intermediate, heap);
          retcode = ERROR;
        }
        if (isERROR(retcode)) return SQLCLI_ReturnCode(&currContext, retcode);
        source = intermediate;
        sourceLen = targetLen - 1;
        sourceType = REC_DECIMAL_LSE;
      }

      if ((sourceType == REC_NCHAR_F_UNICODE || sourceType == REC_NCHAR_V_UNICODE)) {
        // Charset can be retrieved as a long INTERNALLY
        // using programatic interface by calling getDescItem(),
        // and can only be retrieved as a character string EXTERNALLY
        // using "get descriptor" syntax.
        int char_set;

        output_desc->getDescItem(entry, SQLDESC_CHAR_SET, &char_set, NULL, 0, NULL, 0);

        if (char_set == CharInfo::SJIS) {
          switch (targetType) {
            case REC_BYTE_F_ASCII:
              targetType = REC_MBYTE_F_SJIS;
              break;

            case REC_BYTE_V_ANSI:
            case REC_BYTE_V_ASCII_LONG:
            case REC_BYTE_V_ASCII:
              targetType = REC_MBYTE_V_SJIS;
              break;

            default:
              break;
          }
        }
      }

      // get the size of memory to be bounds checked
      int memorySize = targetLen;

      // if varchar, add header size
      if (DFS2REC::isAnyVarChar(targetType)) {
        if (DFS2REC::isSQLVarChar(targetType))
          memorySize += SQL_VARCHAR_HDR_SIZE;  // count prefix
        else if (targetType == REC_BYTE_V_ANSI_DOUBLE)
          memorySize += SQL_DBCHAR_SIZE;  // double-byte nul terminator
        else
          memorySize += 1;  // single-byte nul terminator
      }

      if ((desc_items[i].item_id == SQLDESC_VAR_DATA) &&
          ((sourceType == REC_DATETIME) || ((sourceType >= REC_MIN_INTERVAL) && (sourceType <= REC_MAX_INTERVAL))) &&
          (sourceType == targetType)) {
        // this is a datetime or interval conversion. The fetched VAR_DATA
        // value was saved as an ascii string.
        sourceType = REC_BYTE_F_ASCII;
        sourceScale = 0;
        sourcePrecision = 0;
        targetType = REC_BYTE_F_ASCII;
        targetScale = 0;
        targetPrecision = 0;
      }

      // bounds check memory
      if (currContext.boundsCheckMemory(pTarget, memorySize))
        return SQLCLI_ReturnCode(&currContext, -CLI_USER_MEMORY_IN_EXECUTOR_SEGMENT);

      retcode =
          convDoIt(source, sourceLen, (short)sourceType, sourcePrecision, sourceScale, pTarget, targetLen,
                   (short)targetType, targetPrecision, targetScale, targetVCLen, targetVCLenSize, heap, &diagsArea);

      if (retcode != ex_expr::EXPR_OK) retcode = ERROR;

      if (retcode == ex_expr::EXPR_OK && targetType >= REC_MIN_NUMERIC && targetType <= REC_MAX_NUMERIC &&
          ::scaleDoIt(pTarget, targetLen, targetType, sourceScale, targetScale, sourceType, heap) != ex_expr::EXPR_OK) {
        retcode = ERROR;
      }

      NADELETEBASIC(intermediate, heap);
      if (isERROR(retcode)) return SQLCLI_ReturnCode(&currContext, retcode);
    }
  }

  return SUCCESS;
}

int SQLCLI_GetDescItems2(/*IN*/ CliGlobals *cliGlobals,
                         /*IN*/ SQLDESC_ID *desc_id,
                         /*IN*/ int no_of_desc_items,
                         /*IN*/ SQLDESC_ITEM desc_items[]) {
  int retcode;

  // create initial context, if first call, and add module, if any.
  retcode = CliPrologue(cliGlobals, desc_id->module);
  if (isERROR(retcode)) return retcode;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  Descriptor *desc = currContext.getDescriptor(desc_id);

  /* descriptor must exist */
  if (!desc) {
    diags << DgSqlCode(-CLI_DESC_NOT_EXISTS);
    return SQLCLI_ReturnCode(&currContext, -CLI_DESC_NOT_EXISTS);
  }

  for (int i = 0; i < no_of_desc_items; i++) {
    //
    // Get the entry number of the descriptor entry whose item is to be
    // retrieved.
    //
    int entry = desc_items[i].entry;

    // check that the item entry is < the max num of occurrences specified
    // when the SQL descriptor area was allocated (must also be > 0)
    if ((entry < 1) || (entry > desc->getUsedEntryCount())) {
      diags << DgSqlCode(-CLI_ITEM_NUM_OUT_OF_RANGE) << DgInt0(desc->getMaxEntryCount());

      return SQLCLI_ReturnCode(&currContext, -CLI_ITEM_NUM_OUT_OF_RANGE);
    }

    int sourceLen = 0;
    char *source = NULL;

    int tmp32BitVal;

    switch (desc_items[i].item_id) {
      case SQLDESC_TYPE:
      case SQLDESC_DATETIME_CODE:
      case SQLDESC_TYPE_FS:
      case SQLDESC_LENGTH:
      case SQLDESC_OCTET_LENGTH:
      case SQLDESC_PRECISION:
      case SQLDESC_SCALE:
      case SQLDESC_INT_LEAD_PREC:
      case SQLDESC_NULLABLE:
      case SQLDESC_CHAR_SET:
      case SQLDESC_UNNAMED:
      case SQLDESC_IND_TYPE:
      case SQLDESC_IND_LENGTH:

        // case SQLDESC_VAR_PTR:
        // case SQLDESC_IND_PTR:

      case SQLDESC_RET_LEN:
      case SQLDESC_RET_OCTET_LEN:
      case SQLDESC_IND_DATA:
      case SQLDESC_ROWSET_VAR_LAYOUT_SIZE:
      case SQLDESC_ROWSET_IND_LAYOUT_SIZE:
      case SQLDESC_ROWSET_SIZE:

        // case SQLDESC_ROWSET_STATUS_PTR:
      case SQLDESC_ROWSET_NUM_PROCESSED:
      case SQLDESC_ROWSET_HANDLE:
      case SQLDESC_PARAMETER_MODE:
      case SQLDESC_ORDINAL_POSITION:
      case SQLDESC_PARAMETER_INDEX:
      case SQLDESC_VC_IND_LENGTH: {
        retcode = desc->getDescItem(entry, desc_items[i].item_id, &tmp32BitVal, NULL, 0, NULL, 0);
        if (retcode != 0)
          return SQLCLI_ReturnCode(&currContext, retcode);
        else
          desc_items[i].num_val_or_len = tmp32BitVal;
      } break;
      case SQLDESC_VAR_PTR:
      case SQLDESC_IND_PTR:
      case SQLDESC_ROWSET_STATUS_PTR:
      case SQLDESC_ROWWISE_ROWSET_PTR: {
        retcode = desc->getDescItem(entry, desc_items[i].item_id, &desc_items[i].num_val_or_len, NULL, 0, NULL, 0);

        if (retcode != 0) return SQLCLI_ReturnCode(&currContext, retcode);
      }

      break;

      case SQLDESC_VAR_DATA:
      case SQLDESC_NAME:
      case SQLDESC_HEADING:
      case SQLDESC_TABLE_NAME:
      case SQLDESC_SCHEMA_NAME:
      case SQLDESC_CATALOG_NAME:
      case SQLDESC_CHAR_SET_CAT:
      case SQLDESC_CHAR_SET_SCH:
      case SQLDESC_CHAR_SET_NAM:
      case SQLDESC_COLL_CAT:
      case SQLDESC_COLL_SCH:
      case SQLDESC_COLL_NAM:
      case SQLDESC_TEXT_FORMAT: {
        retcode = desc->getDescItemPtr(entry, desc_items[i].item_id, &source, &sourceLen);
        if (retcode != 0) return SQLCLI_ReturnCode(&currContext, retcode);

        if ((sourceLen > desc_items[i].num_val_or_len) || ((sourceLen > 0) && (desc_items[i].string_val == NULL))) {
          diags << DgSqlCode(-EXE_STRING_OVERFLOW);
          return SQLCLI_ReturnCode(&currContext, -EXE_STRING_OVERFLOW);
        }

        if (sourceLen > 0) {
          str_cpy_all(desc_items[i].string_val, source, sourceLen);
        }

        desc_items[i].num_val_or_len = sourceLen;
      } break;

      default: {
        diags << DgSqlCode(-CLI_INTERNAL_ERROR);
        return SQLCLI_ReturnCode(&currContext, -CLI_INTERNAL_ERROR);
      } break;

    }  // switch

  }  // for

  return SUCCESS;
}

int SQLCLI_ClearDiagnostics(/*IN*/ CliGlobals *cliGlobals,
                            /*IN OPTIONAL*/ SQLSTMT_ID *statement_id) {
  // create initial context, if first call, and add module, if any.
  int retcode;
  if (statement_id == NULL)
    retcode = CliPrologue(cliGlobals, 0);
  else
    retcode = CliPrologue(cliGlobals, statement_id->module);

  if (isERROR(retcode)) return retcode;

  cliGlobals->currContext()->diags().clear();

  return SUCCESS;
}

static int getStmtInfo(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN*/ ComDiagsArea &diags,
    /*IN*/ Statement *stmt,
    /*IN* (SQLDIAG_STMT_INFO_ITEM_ID) */ int what_to_get,
    /*OUT OPTIONAL*/ void *numeric_value,
    /*OUT OPTIONAL*/ char *string_value,
    /*IN OPTIONAL*/ int max_string_len,
    /*OUT OPTIONAL*/ int *len_of_item) {
  switch (what_to_get) {
    case SQLDIAG_NUMBER:
      *(int *)numeric_value = diags.getNumber();  // actually returns long
      break;

    case SQLDIAG_MORE: {
      if (string_value && max_string_len > 0) {
        *string_value = diags.areMore() ? 'Y' : 'N';
        if (len_of_item) *len_of_item = 1;
      }
    } break;

      // A ComDiagsArea doesn't, itself, currently know the
      // difference between a command and dynamic sql function.
    case SQLDIAG_COMMAND_FUNC:
    case SQLDIAG_DYNAMIC_FUNC:
      *(int *)numeric_value = diags.getFunction();  // returns FunctionEnum type
      break;

    case SQLDIAG_ROW_COUNT: {
      long rowCount;
      if (stmt)
        rowCount = stmt->getRowsAffected();  // returns long
      else
        rowCount = diags.getRowCount();  // returns ComDiagBigInt
      if (rowCount > LONG_MAX) return EXE_NUMERIC_OVERFLOW;
      *(long *)numeric_value = rowCount;
      break;
    }

    case SQLDIAG_ROWSET_ROW_COUNT: {
      if (!(diags.hasValidRowsetRowCountArray())) return -EXE_ROWSET_ROW_COUNT_ARRAY_NOT_AVAILABLE;
      if (max_string_len < diags.numEntriesInRowsetRowCountArray()) return -EXE_ROWSET_ROW_COUNT_ARRAY_WRONG_SIZE;

      long rowCount;
      for (int i = 0; i < diags.numEntriesInRowsetRowCountArray(); i++) {
        rowCount = diags.getValueFromRowsetRowCountArray(i);
        assert(rowCount != -1);  // accessing a row count array element that was never set.
        if (rowCount > INT_MAX) return EXE_NUMERIC_OVERFLOW;
        ((int *)numeric_value)[i] = (int)rowCount;
      }
      if (len_of_item) *len_of_item = diags.numEntriesInRowsetRowCountArray();
    } break;

    case SQLDIAG_AVERAGE_STREAM_WAIT:
      *(ComDiagBigInt *)numeric_value = diags.getAvgStreamWaitTime();  // returns ComDiagBigInt
      break;
    // The SQL/MP extension statement info items that are supported.
    case SQLDIAG_COST: {
      double temp_double = diags.getCost();
      if (((double)INT_MAX) <= temp_double) {
        *(int *)numeric_value = INT_MAX;
      } else {
        *(int *)numeric_value = (int)temp_double;
      };
    } break;

    // The SQL/MP extension statement info items aren't currently unsupported.
    // (but should be supported soon! ;-)
    case SQLDIAG_FIRST_FSCODE:
    case SQLDIAG_LAST_FSCODE:
    case SQLDIAG_LAST_SYSKEY:
      *(int *)numeric_value = 0;
      break;

    default:
      return -CLI_INTERNAL_ERROR;
      break;
  }

  return SUCCESS;
}

int SQLCLI_GetDiagnosticsStmtInfo(/*IN*/ CliGlobals *cliGlobals,
                                  /*IN*/ int *stmt_info_items,
                                  /*IN*/ SQLDESC_ID *output_descriptor) {
  // test to see if our inputs are even valid
  if (!stmt_info_items) {
    return -CLI_INTERNAL_ERROR;
  }

  if (!output_descriptor) {
    return -CLI_DESC_NOT_EXISTS;
  }

  // go ahead...
  // create initial context, if first call, and add module, if any.
  int retcode = CliPrologue(cliGlobals, output_descriptor->module);

  if (isERROR(retcode)) return retcode;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  Descriptor *output_desc = currContext.getDescriptor(output_descriptor);

  if (!output_desc) {
    return -CLI_DESC_NOT_EXISTS;
  }

  int outputUsedEntryCount = output_desc->getUsedEntryCount();
  for (int i = 0; i < outputUsedEntryCount; i++) {
    if ((stmt_info_items[i] == SQLDIAG_ROW_COUNT) || (stmt_info_items[i] == SQLDIAG_NUMBER) ||
        (stmt_info_items[i] == SQLDIAG_ROWSET_ROW_COUNT)) {
      char *target;
      int targetLen;
      int targetType;
      int targetPrecision;
      int targetScale;
      int targetRowsetSize = 1;  // initialized to 1 for ROW_COUNT and NUMBER
      int targetRowsetVarLayoutSize;

      retcode = output_desc->getDescItem(i + 1, SQLDESC_VAR_PTR, (int *)&target, NULL, 0, NULL, 0);
      if (isERROR(retcode)) break;

      retcode = output_desc->getDescItem(i + 1, SQLDESC_TYPE_FS, &targetType, NULL, 0, NULL, 0);
      if (isERROR(retcode)) break;

      retcode = output_desc->getDescItem(i + 1, SQLDESC_LENGTH, &targetLen, NULL, 0, NULL, 0);
      if (isERROR(retcode)) break;

      retcode = output_desc->getDescItem(i + 1, SQLDESC_PRECISION, &targetPrecision, NULL, 0, NULL, 0);
      if (isERROR(retcode)) break;

      retcode = output_desc->getDescItem(i + 1, SQLDESC_SCALE, &targetScale, NULL, 0, NULL, 0);
      if (isERROR(retcode)) break;

      if ((targetType >= REC_MIN_CHARACTER) && (targetType <= REC_MAX_CHARACTER)) {
        int temp_char_set;
        output_desc->getDescItem(i + 1, SQLDESC_CHAR_SET, &temp_char_set, 0, 0, 0, 0);
        if (CharInfo::is_NCHAR_MP((CharInfo::CharSet)temp_char_set)) {
          retcode = -EXE_CONVERT_NOT_SUPPORTED;
          break;
        }
      }

      long value;
      if (stmt_info_items[i] == SQLDIAG_ROW_COUNT)
        value = diags.getRowCount();
      else if (stmt_info_items[i] == SQLDIAG_NUMBER) {
        int number;
        retcode = getStmtInfo(cliGlobals, diags, NULL, stmt_info_items[i], &number, NULL, 0, NULL);
        if (isERROR(retcode)) break;

        value = number;
      } else  // SQLDIAG_ROWSET_ROW_COUNT
      {
        // retrieve user provided rowset size into targetRowsetSize
        retcode = output_desc->getDescItem(0, SQLDESC_ROWSET_SIZE, &targetRowsetSize, NULL, 0, NULL, 0);
        if (isERROR(retcode)) break;

        retcode = output_desc->getDescItem(i + 1, SQLDESC_ROWSET_VAR_LAYOUT_SIZE, &targetRowsetVarLayoutSize, NULL, 0,
                                           NULL, 0);
        if (isERROR(retcode)) break;

        if (!(diags.hasValidRowsetRowCountArray())) {
          retcode = -EXE_ROWSET_ROW_COUNT_ARRAY_NOT_AVAILABLE;
          break;
        }
        if (targetRowsetSize < diags.numEntriesInRowsetRowCountArray()) {
          retcode = -EXE_ROWSET_ROW_COUNT_ARRAY_WRONG_SIZE;
          break;
        } else {
          // reset targetRowsetSize to length of run-time rowset size
          // i.e. the number of entries in rowCountArray, if this is smaller
          // than user provided value.
          targetRowsetSize = diags.numEntriesInRowsetRowCountArray();
        }
      }
      CollHeap *heap = output_desc->getContext()->exCollHeap();
      ComDiagsArea *diagsArea = output_desc->getContext()->getDiagsArea();
      int oldDiagsAreaMark = diagsArea->mark();
      int oldLastError = diagsArea->getNumber(DgSqlCode::ERROR_);

      // we will go through this loop only once for NUMBER and ROW_COUNT
      for (int j = 0; j < targetRowsetSize; j++) {
        if (stmt_info_items[i] == SQLDIAG_ROWSET_ROW_COUNT) {
          value = diags.getValueFromRowsetRowCountArray(j);
          // increment target to point to the next element in the output rowset
          if (j > 0) target += targetRowsetVarLayoutSize;
        }

        ex_expr::exp_return_type convDoItRetcode =
            convDoIt((char *)&value, SQL_LARGE_SIZE, (short)REC_BIN64_SIGNED,
                     0,  // sourcePrecision
                     0,  // sourceScale
                     target, targetLen, (short)targetType, targetPrecision, targetScale,
                     NULL,  // varCharLen
                     0,     // varCharLenSize,
                     heap, &diagsArea);

        if (convDoItRetcode != ex_expr::EXPR_OK) retcode = diagsArea->getErrorEntry(oldLastError + 1)->getSQLCODE();

        diagsArea->rewind(oldDiagsAreaMark, TRUE);

        if (isERROR(retcode)) break;
      }
      if (isERROR(retcode)) break;
    } else if (stmt_info_items[i] == SQLDIAG_MORE) {  // only string member in stmt info
      char *more_entry = 0;
      int targetLen;

      retcode = output_desc->getDescItem(i + 1, SQLDESC_VAR_PTR, (int *)&more_entry, NULL, 0, NULL, 0);
      if (isERROR(retcode)) break;

      retcode = output_desc->getDescItem(i + 1, SQLDESC_LENGTH, &targetLen, NULL, 0, NULL, 0);
      if (isERROR(retcode)) break;

      retcode = getStmtInfo(cliGlobals, diags, NULL, stmt_info_items[i], NULL, more_entry, targetLen, NULL);

      if (isERROR(retcode)) break;

    } else {
      //
      // Get the entry number of the descriptor entry whose item is to be set.
      //
      int *entry = 0;

      // guess we need to get the datatype of the descriptor item
      // and then decide what kind of variable to use...
      // Right now, we assume that it is 'long' numeric type.
      retcode = output_desc->getDescItem(i + 1, SQLDESC_VAR_PTR, (int *)&entry, NULL, 0, NULL, 0);
      if (isERROR(retcode)) break;

      retcode = getStmtInfo(cliGlobals, diags, NULL, stmt_info_items[i], entry, NULL, 0, NULL);

      if (isERROR(retcode)) break;
    }
  }

  if (isERROR(retcode))
    return retcode;
  else
    return SUCCESS;
}

int SQLCLI_GetDiagnosticsStmtInfo2(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN OPTIONAL*/ SQLSTMT_ID *statement_id,
    /*IN* (SQLDIAG_STMT_INFO_ITEM_ID) */ int what_to_get,
    /*OUT OPTIONAL*/ void *numeric_value,
    /*OUT OPTIONAL*/ char *string_value,
    /*IN OPTIONAL*/ int max_string_len,
    /*OUT OPTIONAL*/ int *len_of_item) {
  // go ahead...
  // create initial context, if first call, and add module, if any.
  int retcode = CliPrologue(cliGlobals, statement_id ? statement_id->module : NULL);

  if (isERROR(retcode)) return retcode;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  Statement *stmt = NULL;
  if (statement_id) {
    stmt = currContext.getStatement(statement_id);

    // stmt must exist.
    // Only rowsAffected supported for now, if statement_id is passed in.

    if ((!stmt) || ((what_to_get != SQLDIAG_NUMBER) && (what_to_get != SQLDIAG_ROW_COUNT))) {
      return -CLI_STMT_NOT_EXISTS;
    }
  }

  retcode = getStmtInfo(cliGlobals, diags, stmt, what_to_get, numeric_value, string_value, max_string_len, len_of_item);

  return SQLCLI_ReturnCode(&currContext, retcode);
}

static void copyResultString(char *dest, const char *src, int destLen, int *copyLen = NULL) {
  if (!dest) return;

  if (copyLen) *copyLen = 0;
  if (src) {
    int len = MINOF(str_len(src) + 1, destLen);
    str_cpy_all(dest, src, len);
    if (copyLen) *copyLen = len;
  } else
    *dest = '\0';
}

int SQLCLI_GetDiagnosticsCondInfo(/*IN*/ CliGlobals *cliGlobals,
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
                                  /*OUT*/ SQLMXLoggingArea::ExperienceLevel *emsEventEL) {
  int i;
  // test to see if our inputs are even valid
  if (!cond_info_items) {
    return -CLI_INTERNAL_ERROR;
  }

  if (!cond_num_descriptor || !output_descriptor) {
    return -CLI_DESC_NOT_EXISTS;
  }

  // go ahead...
  // create initial context, if first call, and add module, if any.
  int retcode = CliPrologue(cliGlobals, output_descriptor->module);
  if (isERROR(retcode)) return retcode;
  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();
  // *emsEventEL is assigned the system experience level value
  // passed to the cli through the plan

  if (emsEventEL != NULL) *emsEventEL = cliGlobals->getEMSEventExperienceLevel();
#ifdef BOUNDS_CHECK_IN_CLI

#endif

  if (currContext.boundsCheckMemory(message_buffer_ptr, message_obj_size) ||
      currContext.boundsCheckMemory(message_obj_size_needed, sizeof(IpcMessageObjSize)) ||
      currContext.boundsCheckMemory(message_obj_type, sizeof(IpcMessageObjType)) ||
      currContext.boundsCheckMemory(message_obj_version, sizeof(IpcMessageObjVersion)) ||
      currContext.boundsCheckMemory(condition_item_array, condition_item_count * sizeof(DiagsConditionItem)) ||
      currContext.boundsCheckMemory(condition_item_count_needed, sizeof(int)))
    return SQLCLI_ReturnCode(&currContext, -CLI_USER_MEMORY_IN_EXECUTOR_SEGMENT);

  Descriptor *condnum_desc = currContext.getDescriptor(cond_num_descriptor);

  if (!condnum_desc) {
    return -CLI_DESC_NOT_EXISTS;
  }

  Descriptor *output_desc = currContext.getDescriptor(output_descriptor);

  if (!output_desc) {
    return -CLI_DESC_NOT_EXISTS;
  }

  int outputUsedEntryCount = output_desc->getUsedEntryCount();

  // the condition number is contained in a single descriptor entry
  if (condnum_desc->getUsedEntryCount() < 1) {
    return -CLI_INTERNAL_ERROR;
  }

  *message_obj_size_needed = diags.packedLength();

  *condition_item_count_needed = 0;
  for (i = 0; i < outputUsedEntryCount; i++) {
    switch (cond_info_items[i].item_id) {
      case SQLDIAG_RET_SQLSTATE:  /* (string ) returned SQLSTATE */
      case SQLDIAG_CLASS_ORIG:    /* (string ) class origin, e.g. ISO 9075 */
      case SQLDIAG_SUBCLASS_ORIG: /* (string ) subclass origin, e.g. ISO 9075 */
      case SQLDIAG_MSG_TEXT:      /* (string ) message text */
      case SQLDIAG_MSG_LEN:       /* (numeric) message length in characters */
      case SQLDIAG_MSG_OCTET_LEN: /* (numeric) message length in bytes */
        *condition_item_count_needed += 1;
    }
    if (cond_info_items[i].cond_number_desc_entry < 1 ||
        (condnum_desc->getUsedEntryCount() > 1 &&
         cond_info_items[i].cond_number_desc_entry > condnum_desc->getUsedEntryCount()))
      return -CLI_INTERNAL_ERROR;
  }
  if (*condition_item_count_needed > 0 &&
      (condition_item_count < *condition_item_count_needed || message_obj_size < *message_obj_size_needed))
    return 0;  // Caller has to call again with larger array or buffer
  if (*condition_item_count_needed > 0) {
    *condition_item_count_needed = 0;
    if (diags.getNumber(DgSqlCode::ERROR_) > SHRT_MAX) return -CLI_INTERNAL_ERROR;
    *message_obj_size_needed = diags.packObjIntoMessage(message_buffer_ptr);
    *message_obj_type = diags.getType();
    *message_obj_version = diags.getVersion();
  }

  CharInfo::CharSet messageCharSet = CharInfo::UnknownCharSet;
  int msgOctLenInd = -1;

  for (i = 0; i < outputUsedEntryCount; i++) {
    //
    // Get the entry number of the descriptor entry whose item is to be set.
    //
    int *entry = 0;
    retcode = output_desc->getDescItem(i + 1, SQLDESC_VAR_PTR,
                                       (int *)&entry,  // $$$$ hjz: don't assume this is a long hostvar
                                       NULL, 0, NULL, 0);
    if (isERROR(retcode)) return SQLCLI_ReturnCode(&currContext, retcode);

    if (!entry) return -CLI_INTERNAL_ERROR;

    int theLength = 0L;
    retcode = output_desc->getDescItem(i + 1, SQLDESC_LENGTH, &theLength, NULL, 0, NULL, 0);
    if (isERROR(retcode)) return SQLCLI_ReturnCode(&currContext, retcode);

    int userIndex = 0L;
    retcode = InputValueFromNumericHostvar(condnum_desc,
                                           condnum_desc->getUsedEntryCount() > 1
                                               ? cond_info_items[i].cond_number_desc_entry
                                               : 1,  // use 1 for compatibility (used to be this way)
                                           userIndex);
    if (isERROR(retcode)) return SQLCLI_ReturnCode(&currContext, retcode);

    if (userIndex < 1 || userIndex > diags.getNumber()) {
      return -CLI_ITEM_NUM_OUT_OF_RANGE;  // index out of range
    }

    ComCondition &condition = diags[userIndex];

    switch (cond_info_items[i].item_id) {
      case SQLDIAG_SQLCODE: /* (numeric) the SQLCODE */
        retcode = OutputValueIntoNumericHostvar(output_desc, i + 1, condition.getSQLCODE());
        break;

      case SQLDIAG_COND_NUMBER: /* (numeric) condition number */
        retcode = OutputValueIntoNumericHostvar(output_desc, i + 1, condition.getConditionNumber());
        break;

      case SQLDIAG_NSK_CODE: /* (numeric) NSK/FS/DP2 FE error number */
        retcode = OutputValueIntoNumericHostvar(output_desc, i + 1, condition.getNskCode());
        break;

      case SQLDIAG_MSG_TEXT: /* (string ) message text */
        condition.incrEMSEventVisits();
      case SQLDIAG_CLASS_ORIG:    /* (string ) class origin, e.g. ISO 9075 */
      case SQLDIAG_SUBCLASS_ORIG: /* (string ) subclass origin, e.g. ISO 9075 */
      case SQLDIAG_MSG_LEN:       /* (numeric) message length in characters */
      case SQLDIAG_MSG_OCTET_LEN: /* (numeric) message length in bytes */
                                  // UR2: should get the charset specified in the output_desc to
                                  // condition_item_array so that the right charset can be used to
                                  // translate the MSG_TEXT and length.
        condition_item_array[*condition_item_count_needed].item_id = cond_info_items[i].item_id;
        condition_item_array[*condition_item_count_needed].var_ptr = (char *)entry;
        condition_item_array[(*condition_item_count_needed)].length = theLength;
        condition_item_array[(*condition_item_count_needed)].output_entry = i;
        condition_item_array[(*condition_item_count_needed)].condition_index = userIndex;

        // The charset for the hostvar that the error message is assigned to
        // has to be retrived. Use the internal fast SQLDESC_CHAR_SET version.
        if (cond_info_items[i].item_id == SQLDIAG_MSG_TEXT) {
          int charset;
          retcode = output_desc->getDescItem(i + 1, SQLDESC_CHAR_SET, &charset, NULL, 0, NULL, 0);
          if (retcode != 0) return SQLCLI_ReturnCode(&currContext, retcode);
          if (charset == (int)CharInfo::ISO88591)  // do not know where to set this field
            charset = CharInfo::UTF8;              // so set it here for now.

          messageCharSet = (CharInfo::CharSet)charset;

          // Only ISO88591 and UCS2 can be used to retrieve the diagnostic msg.
          if (CharInfo::isMsgCharSetSupported(messageCharSet)) {
            condition_item_array[(*condition_item_count_needed)].charset = messageCharSet;
          } else {
            return SQLCLI_ReturnCode(&currContext, -CLI_MSG_CHAR_SET_NOT_SUPPORTED);
          }
        } else if (cond_info_items[i].item_id == SQLDIAG_MSG_OCTET_LEN) {
          // remember the index for SQLDIAG_MSG_OCTET_LEN into
          // the condition_item_array.
          msgOctLenInd = *condition_item_count_needed;
        }
        ++(*condition_item_count_needed);

        break;

      case SQLDIAG_RET_SQLSTATE: /* (string ) returned SQLSTATE */
        if (condition.getCustomSQLState() == NULL) {
          condition_item_array[*condition_item_count_needed].item_id = cond_info_items[i].item_id;
          condition_item_array[*condition_item_count_needed].var_ptr = (char *)entry;
          condition_item_array[(*condition_item_count_needed)].length = theLength;
          condition_item_array[(*condition_item_count_needed)].output_entry = i;
          condition_item_array[(*condition_item_count_needed)++].condition_index = userIndex;
        } else {
          copyResultString((char *)entry, condition.getCustomSQLState(), theLength);
        }
        break;

      case SQLDIAG_SERVER_NAME: /* (string ) SQL server name */
        copyResultString((char *)entry, condition.getServerName(), theLength);
        break;

      case SQLDIAG_CONNECT_NAME: /* (string ) connection name */
        copyResultString((char *)entry, condition.getConnectionName(), theLength);
        break;

      case SQLDIAG_CONSTR_CAT: /* (string ) constraint catalog name */
        copyResultString((char *)entry, condition.getConstraintCatalog(), theLength);
        break;

      case SQLDIAG_CONSTR_SCHEMA: /* (string ) constraint schema name */
        copyResultString((char *)entry, condition.getConstraintSchema(), theLength);
        break;

      case SQLDIAG_CONSTR_NAME: /* (string ) constraint name */
        copyResultString((char *)entry, condition.getConstraintName(), theLength);
        break;

      case SQLDIAG_TRIGGER_CAT: /* (string ) trigger catalog name */
        copyResultString((char *)entry, condition.getTriggerCatalog(), theLength);
        break;

      case SQLDIAG_TRIGGER_SCHEMA: /* (string ) trigger schema name */
        copyResultString((char *)entry, condition.getTriggerSchema(), theLength);
        break;

      case SQLDIAG_TRIGGER_NAME: /* (string ) trigger name */
        copyResultString((char *)entry, condition.getTriggerName(), theLength);
        break;

      case SQLDIAG_CATALOG_NAME: /* (string ) catalog name */
        copyResultString((char *)entry, condition.getCatalogName(), theLength);
        break;

      case SQLDIAG_SCHEMA_NAME: /* (string ) schema name */
        copyResultString((char *)entry, condition.getSchemaName(), theLength);
        break;

      case SQLDIAG_TABLE_NAME: /* (string ) table name */
        copyResultString((char *)entry, condition.getTableName(), theLength);
        break;

      case SQLDIAG_COLUMN_NAME: /* (string ) column name */
        copyResultString((char *)entry, condition.getColumnName(), theLength);
        break;

      // CursorName is  not implemented (co-opted for SqlID)
      case SQLDIAG_CURSOR_NAME: /* (string ) cursor name */
        copyResultString((char *)entry, "", theLength);
        break;

        /* ODBC -- not implemented currently */
      case SQLDIAG_COLUMN_NUMBER: /* (numeric) column number     */
      case SQLDIAG_NATIVE:        /* (numeric) native error code */
        break;

      case SQLDIAG_ROW_NUMBER: /* (numeric) row number of input rowset that caused the error  */
        retcode = OutputValueIntoNumericHostvar(output_desc, i + 1, condition.getRowNumber());
        break;

        /* SQL/MP -- not implemented yet (except for SQLCODE) */
      case SQLDIAG_SOURCE_FILE:  /* (string ) source file name            */
      case SQLDIAG_LINE_NUMBER:  /* (numeric) source code line number     */
      case SQLDIAG_SUBSYSTEM_ID: /* (string ) component that issued error */
        break;

      default:
        // do nothing -- no way to report error, so, okay.
        break;
    }
  }

  // SQLDIAG_MSG_OCT_LEN depends on the message char set.
  // remember the message character set for the entry into
  // the condition item array for SQLDIAG_MSG_OCT_LEN.
  if (msgOctLenInd != -1) {
    condition_item_array[msgOctLenInd].charset = messageCharSet;
  }

  return retcode;
}
int SQLCLI_GetPackedDiagnostics(
    /*IN*/ CliGlobals *cliGlobals,
    /*OUT*/ IpcMessageBufferPtr message_buffer_ptr,
    /*IN*/ IpcMessageObjSize message_obj_size,
    /*OUT*/ IpcMessageObjSize *message_obj_size_needed,
    /*OUT*/ IpcMessageObjType *message_obj_type,
    /*OUT*/ IpcMessageObjVersion *message_obj_version) {
  // create initial context, if first call, and add module, if any.
  int retcode = CliPrologue(cliGlobals, NULL);
  if (isERROR(retcode)) return retcode;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  *message_obj_size_needed = diags.packedLength();

  if (message_obj_size < *message_obj_size_needed) return 0;  // Caller has to call again with larger array or buffer

  *message_obj_size_needed = diags.packObjIntoMessage(message_buffer_ptr);
  *message_obj_type = diags.getType();
  *message_obj_version = diags.getVersion();

  return retcode;
}
int GetDiagnosticsCondInfoSwitchStatement(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN* (SQLDIAG_COND_INFO_ITEM_ID) */ int what_to_get,
    /*IN*/ int conditionNum,
    /*OUT OPTIONAL*/ int *numeric_value,
    /*OUT OPTIONAL*/ char *string_value,
    /*IN OPTIONAL */ int max_string_len,
    /*OUT OPTIONAL*/ int *len_of_item) {
  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();
  ComCondition &condition = diags[conditionNum];
  // LPWSTR p = NULL;

  switch (what_to_get) {
    case SQLDIAG_SQLCODE: /* (numeric) the SQLCODE */
      *numeric_value = condition.getSQLCODE();
      break;
    case SQLDIAG_COND_NUMBER: /* (numeric) condition number */
      *numeric_value = condition.getConditionNumber();
      break;

    case SQLDIAG_NSK_CODE: /* (numeric) NSK/FS/DP2 FE error number */
      *numeric_value = condition.getNskCode();
      break;

    case SQLDIAG_SERVER_NAME: /* (string ) SQL server name */
      copyResultString(string_value, condition.getServerName(), max_string_len, len_of_item);
      break;

    case SQLDIAG_CONNECT_NAME: /* (string ) connection name */
      copyResultString(string_value, condition.getConnectionName(), max_string_len, len_of_item);
      break;

    case SQLDIAG_CONSTR_CAT: /* (string ) constraint catalog name */
      copyResultString(string_value, condition.getConstraintCatalog(), max_string_len, len_of_item);
      break;

    case SQLDIAG_CONSTR_SCHEMA: /* (string ) constraint schema name */
      copyResultString(string_value, condition.getConstraintSchema(), max_string_len, len_of_item);
      break;

    case SQLDIAG_CONSTR_NAME: /* (string ) constraint name */
      copyResultString(string_value, condition.getConstraintName(), max_string_len, len_of_item);
      break;

    case SQLDIAG_TRIGGER_CAT: /* (string ) trigger catalog name */
      copyResultString(string_value, condition.getTriggerCatalog(), max_string_len, len_of_item);
      break;

    case SQLDIAG_TRIGGER_SCHEMA: /* (string ) trigger schema name */
      copyResultString(string_value, condition.getTriggerSchema(), max_string_len, len_of_item);
      break;

    case SQLDIAG_TRIGGER_NAME: /* (string ) trigger name */
      copyResultString(string_value, condition.getTriggerName(), max_string_len, len_of_item);
      break;

    case SQLDIAG_CATALOG_NAME: /* (string ) catalog name */
      copyResultString(string_value, condition.getCatalogName(), max_string_len, len_of_item);
      break;

    case SQLDIAG_SCHEMA_NAME: /* (string ) schema name */
      copyResultString(string_value, condition.getSchemaName(), max_string_len, len_of_item);
      break;

    case SQLDIAG_TABLE_NAME: /* (string ) table name */
      copyResultString(string_value, condition.getTableName(), max_string_len, len_of_item);
      break;

    case SQLDIAG_COLUMN_NAME: /* (string ) column name */
      copyResultString(string_value, condition.getColumnName(), max_string_len, len_of_item);
      break;

    // CursorName is  not implemented (co-opted for SqlID)
    case SQLDIAG_CURSOR_NAME: /* (string ) cursor name */
      copyResultString(string_value, "", max_string_len, len_of_item);
      break;

    case SQLDIAG_CLASS_ORIG:    /* (string ) class origin, e.g. ISO 9075 */
    case SQLDIAG_SUBCLASS_ORIG: /* (string ) subclass origin, e.g. ISO 9075*/
    case SQLDIAG_RET_SQLSTATE:  /* (string ) returned SQLSTATE */
    case SQLDIAG_MSG_TEXT:      /* (string ) message text */
    case SQLDIAG_MSG_LEN:       /* (numeric) message length in characters */
    case SQLDIAG_MSG_OCTET_LEN: /* (numeric) message length in bytes */

    {
      // for these cases, call GetDiagnosticsCondInfo().
      // TBD.
    } break;

      /* ODBC -- not implemented currently */
    case SQLDIAG_COLUMN_NUMBER: /* (numeric) column number     */
    case SQLDIAG_NATIVE:        /* (numeric) native error code */
      break;

    case SQLDIAG_ROW_NUMBER: /* (numeric) row number of input rowset that caused the error  */
      *numeric_value = condition.getRowNumber();
      break;

      /* SQL/MP -- not implemented yet (except for SQLCODE) */
    case SQLDIAG_SOURCE_FILE:  /* (string ) source file name            */
    case SQLDIAG_LINE_NUMBER:  /* (numeric) source code line number     */
    case SQLDIAG_SUBSYSTEM_ID: /* (string ) component that issued error */
      break;

    default:
      // do nothing -- no way to report error, so, okay.
      break;
  }
  return 0;
}

int SQLCLI_GetDiagnosticsCondInfo2(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN* (SQLDIAG_COND_INFO_ITEM_ID) */ int what_to_get,
    /*IN*/ int conditionNum,
    /*OUT OPTIONAL*/ int *numeric_value,
    /*OUT OPTIONAL*/ char *string_value,
    /*IN OPTIONAL */ int max_string_len,
    /*OUT OPTIONAL*/ int *len_of_item) {
  // go ahead...
  // create initial context, if first call, and add module, if any.
  int retcode = CliPrologue(cliGlobals, NULL);
  if (isERROR(retcode)) return retcode;

  retcode = GetDiagnosticsCondInfoSwitchStatement(cliGlobals, what_to_get, conditionNum, numeric_value, string_value,
                                                  max_string_len, len_of_item);

  return retcode;
}

int SQLCLI_GetDiagnosticsCondInfo3(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN*/ int no_of_condition_items,
    /*IN*/ SQLDIAG_COND_INFO_ITEM_VALUE diag_cond_info_item_values[],
    /*OUT*/ IpcMessageObjSize *message_obj_size_needed) {
  int retcode;
  SQLDIAG_COND_INFO_ITEM_VALUE diagItemValues;
  int max_string_len;
  int what_to_get;
  NABoolean executePartiallyInNonPriv = FALSE;

  // go ahead...
  // create initial context, if first call, and add module, if any.
  retcode = CliPrologue(cliGlobals, NULL);
  if (isERROR(retcode)) return retcode;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  for (int i = 0; i < no_of_condition_items; i++) {
    diagItemValues = diag_cond_info_item_values[i];
    if (diagItemValues.string_val)
      max_string_len = *(diagItemValues.num_val_or_len);
    else
      max_string_len = 0;
    what_to_get = diagItemValues.item_id_and_cond_number.item_id;
    ComCondition &condition = diags[diagItemValues.item_id_and_cond_number.cond_number_desc_entry];
    switch (what_to_get) {
      case SQLDIAG_MSG_TEXT: /* (string ) message text */
        condition.incrEMSEventVisits();
      case SQLDIAG_RET_SQLSTATE:  /* (string ) returned SQLSTATE */
      case SQLDIAG_CLASS_ORIG:    /* (string ) class origin, e.g. ISO 9075 */
      case SQLDIAG_SUBCLASS_ORIG: /* (string ) subclass origin, e.g. ISO 9075 */
      case SQLDIAG_MSG_LEN:       /* (numeric) message length in characters */
      case SQLDIAG_MSG_OCTET_LEN: /* (numeric) message length in bytes */
        executePartiallyInNonPriv = TRUE;
    }

    retcode = GetDiagnosticsCondInfoSwitchStatement(
        cliGlobals, what_to_get, diagItemValues.item_id_and_cond_number.cond_number_desc_entry,
        diagItemValues.num_val_or_len, diagItemValues.string_val, max_string_len, diagItemValues.num_val_or_len);

    if (retcode < 0) return retcode;
  }

  *message_obj_size_needed = executePartiallyInNonPriv ? diags.packedLength() : 0;

  return retcode;
}

int SQLCLI_GetDiagnosticsArea(/*IN*/ CliGlobals *cliGlobals,
                              /*OUT*/ IpcMessageBufferPtr message_buffer_ptr,
                              /*IN*/ IpcMessageObjSize message_obj_size,
                              /*OUT*/ IpcMessageObjType *message_obj_type,
                              /*OUT*/ IpcMessageObjVersion *message_obj_version) {
  // go ahead...
  // create initial context, if first call, and add module, if any.
  int retcode = CliPrologue(cliGlobals, NULL);
  if (isERROR(retcode)) return retcode;
  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();
  int oldDiagsAreaMark = diags.mark();

  if (currContext.boundsCheckMemory(message_buffer_ptr, message_obj_size) ||
      currContext.boundsCheckMemory(message_obj_type, sizeof(IpcMessageObjType)) ||
      currContext.boundsCheckMemory(message_obj_version, sizeof(IpcMessageObjVersion))) {
    diags.rewind(oldDiagsAreaMark, TRUE);
    return -CLI_USER_MEMORY_IN_EXECUTOR_SEGMENT;
  }

  if (diags.getNumber(DgSqlCode::ERROR_) > SHRT_MAX) return -CLI_INTERNAL_ERROR;
  if (diags.packedLength() > message_obj_size) return -CLI_INTERNAL_ERROR;

  // It is the caller's responsibility to pass in buffer of appropriate size.
  // This method assumes that the buffer is large enough to hold the diagsArea,
  // if not CLI_INTERNAL_ERROR is returned.
  diags.packObjIntoMessage(message_buffer_ptr);
  *message_obj_type = diags.getType();
  *message_obj_version = diags.getVersion();
  return 0;
}

int SQLCLI_GetSQLCODE(/*IN*/ CliGlobals *cliGlobals,
                      /*OUT*/ int *SQLCODE) {
  if (!cliGlobals) return -CLI_NO_CURRENT_CONTEXT;

  *SQLCODE = cliGlobals->currContext()->diags().mainSQLCODE();

  return SUCCESS;  // this call always succeeds, no need to check errors!
}
int SQLCLI_GetMainSQLSTATE(/*IN*/ CliGlobals *cliGlobals,
                           /*IN*/ SQLSTMT_ID *statement_id,
                           /*IN*/ int sqlcode,
                           /*OUT*/ char *sqlstate /* assumed to be char[6] */) {
  return -1;
}

/////////////////////////////////////////////////////////////////
// if sql_source->name_mode is string_data, then
// sql_source->identifier points to the sql_source string and
// identifier_len is the length of that string.
/////////////////////////////////////////////////////////////////
int SQLCLI_Prepare(/*IN*/ CliGlobals *cliGlobals,
                   /*IN*/ SQLSTMT_ID *statement_id,
                   /*IN*/ SQLDESC_ID *sql_source) {
  int retcode;

  // create initial context, if first call, and add module, if any.
  retcode = CliPrologue(cliGlobals, statement_id->module);
  if (isERROR(retcode)) return retcode;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  /* prepare the statement */
  Statement *stmt = currContext.getStatement(statement_id);

  /* stmt must exist */
  if (!stmt) {
    diags << DgSqlCode(-CLI_STMT_NOT_EXISTS);
    return SQLCLI_ReturnCode(&currContext, -CLI_STMT_NOT_EXISTS);
  }

  StrTarget strTarget;
  retcode = stmt->initStrTarget(sql_source, currContext, diags, strTarget);
  if (isERROR(retcode)) return SQLCLI_ReturnCode(&currContext, retcode);

  // CLI callers do not prepare stored procedure result set
  // statements. The prepare is done internally on-demand when the
  // statement is described or executed.
  if (stmt->getParentCall()) {
    diags << DgSqlCode(-EXE_UDR_RS_PREPARE_NOT_ALLOWED);
    return SQLCLI_ReturnCode(&currContext, -EXE_UDR_RS_PREPARE_NOT_ALLOWED);
  }

  // Set the uniqueStmtId in Statement
  stmt->setUniqueStmtId(NULL);
  // Set the StmtStats in the shared segment
  stmt->setStmtStats(FALSE);

  SQLCLI_Prepare_Setup_Pre(currContext, stmt, 0);
  UInt32 flags;
  SessionDefaults *sd = currContext.getSessionDefaults();
  if (sd != NULL && sd->getCallEmbeddedArkcmp())
    flags = PREPARE_USE_EMBEDDED_ARKCMP;
  else
    flags = 0;
  retcode = stmt->prepare(strTarget.getStr(), diags, NULL, 0L, strTarget.getIntCharSet(), TRUE, flags);

  SQLCLI_Prepare_Setup_Post(currContext, stmt, 1);

  if (isERROR(retcode)) return SQLCLI_ReturnCode(&currContext, retcode);
  stmt->issuePlanVersioningWarnings(diags);
  return CliEpilogue(cliGlobals, statement_id);
}

int SQLCLI_Prepare2(/*IN*/ CliGlobals *cliGlobals,
                    /*IN*/ SQLSTMT_ID *statement_id,
                    /*IN*/ SQLDESC_ID *sql_source,
                    /*INOUT*/ char *gencode_ptr,
                    /*IN*/ int gencode_len,
                    /*INOUT*/ int *ret_gencode_len,
                    /*INOUT*/ SQL_QUERY_COST_INFO *query_cost_info,
                    /*INOUT*/ SQL_QUERY_COMPILER_STATS_INFO *query_comp_stats_info,
                    /*INOUT*/ char *uniqueStmtId,
                    /*INOUT*/ int *uniqueStmtIdLen,
                    /*IN*/ int flags) {
  int retcode = 0;

  // create initial context, if first call, and add module, if any.
  retcode = CliPrologue(cliGlobals, statement_id->module);
  if (isERROR(retcode)) {
    return retcode;
  }

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  AQRInfo *aqr = currContext.aqrInfo();

  /* prepare the statement */
  Statement *stmt = currContext.getStatement(statement_id);

  /* stmt must exist */
  if (!stmt) {
    diags << DgSqlCode(-CLI_STMT_NOT_EXISTS);
    return SQLCLI_ReturnCode(&currContext, -CLI_STMT_NOT_EXISTS);
  }

  NABoolean retrieveGenCode = FALSE;
  NABoolean reTry = ((flags & PREPARE_AUTO_QUERY_RETRY) != 0);
  UInt32 tmpFlags;
  SessionDefaults *sd = currContext.getSessionDefaults();
  if (sd != NULL && sd->getCallEmbeddedArkcmp())
    tmpFlags = flags | PREPARE_USE_EMBEDDED_ARKCMP;
  else
    tmpFlags = flags;
  if (sql_source) {
    StrTarget strTarget;
    retcode = stmt->initStrTarget(sql_source, currContext, diags, strTarget);
    if (isERROR(retcode)) {
      return SQLCLI_ReturnCode(&currContext, retcode);
    }

    NABoolean standaloneQ = tmpFlags & PREPARE_STANDALONE_QUERY;

    if (reTry) {
      if ((uniqueStmtId == NULL) || (uniqueStmtIdLen == NULL) || (*uniqueStmtIdLen <= 0)) {
        // Error
        diags << DgSqlCode(-CLI_STMT_NOT_EXISTS);
        return SQLCLI_ReturnCode(&currContext, -CLI_STMT_NOT_EXISTS);
      }

      stmt->setUniqueStmtId(uniqueStmtId);
    }

    // if uniqueStmt id buffer passed in contains a valid query id,
    // use that as the query id and it should be same as the one
    // that already exists
    else if ((uniqueStmtId) && (uniqueStmtIdLen) && (*uniqueStmtIdLen >= (int)strlen(COM_SESSION_ID_PREFIX)) &&
             (str_cmp(uniqueStmtId, COM_SESSION_ID_PREFIX, strlen(COM_SESSION_ID_PREFIX)) == 0)) {
      if (stmt->getUniqueStmtId() == NULL || (*uniqueStmtIdLen) != stmt->getUniqueStmtIdLen() ||
          str_cmp(stmt->getUniqueStmtId(), uniqueStmtId, stmt->getUniqueStmtIdLen()) != 0) {
        diags << DgSqlCode(-CLI_QID_NOT_MATCHING);
        return SQLCLI_ReturnCode(&currContext, -CLI_QID_NOT_MATCHING);
      }
    } else
      // generate a new query id.
      stmt->setUniqueStmtId(NULL);

    // Set the StmtStats in the shared segment
    stmt->setStmtStats(reTry);
    SQLCLI_Prepare_Setup_Pre(currContext, stmt, tmpFlags);
    if (!reTry) {
      // new prepare. Clear any previously set retry condition.
      aqr->clearRetryInfo();
      stmt->aqrStmtInfo()->clearRetryInfo();
      stmt->aqrStmtInfo()->setRetryPrepareFlags(tmpFlags);
    }

    if (ret_gencode_len == NULL) {
      // assign passed in source str and gen code to 'this'
      retcode = stmt->prepare(strTarget.getStr(), diags, gencode_ptr, gencode_len, strTarget.getIntCharSet(),
                              TRUE /* unpack tdbs */, tmpFlags);
      retrieveGenCode = FALSE;
    } else {
      // prepare this statement.
      retcode =
          stmt->prepare(strTarget.getStr(), diags, NULL, 0L, strTarget.getIntCharSet(), FALSE, /* do not unpack tdbs */
                        tmpFlags);
      retrieveGenCode = TRUE;
    }
  } else
    retrieveGenCode = TRUE;

  if (uniqueStmtId) {
    if ((uniqueStmtIdLen) && (*uniqueStmtIdLen >= stmt->getUniqueStmtIdLen())) {
      str_cpy_all(uniqueStmtId, stmt->getUniqueStmtId(), stmt->getUniqueStmtIdLen());
      *uniqueStmtIdLen = stmt->getUniqueStmtIdLen();
    }
  }

  if (isERROR(retcode)) {
    return SQLCLI_ReturnCode(&currContext, retcode);
  }

  if (query_cost_info) {
    query_cost_info->cpuTime = 0;
    query_cost_info->ioTime = 0;
    query_cost_info->msgTime = 0;
    query_cost_info->idleTime = 0;
    query_cost_info->totalTime = 0;
    query_cost_info->cardinality = 0;
    query_cost_info->estimatedTotalMem = 0;
    query_cost_info->resourceUsage = 0;
    query_cost_info->maxCpuUsage = 0;

    if (stmt->getRootTdb()) {
      if (stmt->getRootTdb()->getQueryCostInfo()) {
        stmt->getRootTdb()->getQueryCostInfo()->translateToExternalFormat(query_cost_info);
      }

      else
        query_cost_info->totalTime = stmt->getRootTdb()->getCost();
    }
  }

  if (query_comp_stats_info) {
    if (stmt->getRootTdb()) {
      CompilerStatsInfo *cmpStatsInfo = stmt->getRootTdb()->getCompilerStatsInfo();

      if (cmpStatsInfo) {
        short xnNeeded = (stmt->transactionReqd() ? 1 : 0);
        cmpStatsInfo->translateToExternalFormat(query_comp_stats_info, xnNeeded);
        // populate shmSize and leftShmSize
        StatsGlobals *statsGlobal = cliGlobals->getStatsGlobals();
        if (statsGlobal && statsGlobal->getStatsHeap()) {
          query_comp_stats_info->shmSize = statsGlobal->getStatsHeap()->getTotalSize();
          query_comp_stats_info->leftShmSize =
              statsGlobal->getStatsHeap()->getTotalSize() - statsGlobal->getStatsHeap()->getAllocSize();
        } else {  // should not happen
          query_comp_stats_info->shmSize = 0;
          query_comp_stats_info->leftShmSize = 0;
        }
      }

      // CompilationStatsData.
      CompilationStatsData *cmpData = stmt->getRootTdb()->getCompilationStatsData();

      SQL_COMPILATION_STATS_DATA *query_cmp_data = &query_comp_stats_info->compilationStats;

      if (cmpData) {
        StmtStats *stmtStats = stmt->getStmtStats();
        ExMasterStats *masterStats;
        long cmpStartTime = -1;
        long cmpEndTime = NA_JulianTimestamp();
        if (stmtStats != NULL && (masterStats = stmtStats->getMasterStats()) != NULL)
          cmpStartTime = masterStats->getCompStartTime();

        cmpData->translateToExternalFormat(query_cmp_data, cmpStartTime, cmpEndTime);
        stmt->setCompileEndTime(cmpEndTime);
      }
    }
  }

  // A cursor on an embedded INSERT is not supported.
  if (stmt->getRootTdb() && stmt->getRootTdb()->isEmbeddedInsert()) {
    if (statement_id->identifier && strstr((char *)statement_id->identifier, "SQLCI_PREPSTMT_FOR_CURSOR_") != NULL) {
      diags << DgSqlCode(-3409);
      return SQLCLI_ReturnCode(&currContext, -3409);
    }
  }

  if (retrieveGenCode) {
    if (ret_gencode_len) *ret_gencode_len = stmt->getRootTdbSize();

    if ((gencode_ptr == NULL) || (gencode_len < stmt->getRootTdbSize())) {
      diags << DgSqlCode(-CLI_GENCODE_BUFFER_TOO_SMALL);
      return SQLCLI_ReturnCode(&currContext, -CLI_GENCODE_BUFFER_TOO_SMALL);
    }

    memcpy(gencode_ptr, stmt->getRootTdb(), stmt->getRootTdbSize());

    int unpackRetcode = stmt->unpackAndInit(diags, -1);
    if (isERROR(unpackRetcode) && !isERROR(retcode)) retcode = unpackRetcode;

    // root tdb is not unpacked in some cases.
    // See Statement::unpackAndInit for details.
    // For those cases, do not do stats post prepare setup as that will
    // try to access fields in root tdb.
    if (NOT stmt->getRootTdb()->isFromShowplan()) SQLCLI_Prepare_Setup_Post(currContext, stmt, tmpFlags);
  } else
    SQLCLI_Prepare_Setup_Post(currContext, stmt, tmpFlags);

  if (cliGlobals->currContext()->diags().getNumber()) {
    return cliGlobals->currContext()->diags().mainSQLCODE();
  } else {
    return retcode;
  }
}

int SQLCLI_GetExplainData(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN*/ SQLSTMT_ID *statement_id,
    /*INOUT*/ char *explain_ptr,
    /*IN*/ int explain_buf_len,
    /*INOUT*/ int *ret_explain_len) {
  int retcode = 0;

  // create initial context, if first call, and add module, if any.
  retcode = CliPrologue(cliGlobals, statement_id->module);
  if (isERROR(retcode)) {
    return retcode;
  }

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  /* get the statement from context */
  Statement *stmt = currContext.getStatement(statement_id);

  /* stmt must exist */
  if ((!stmt) || (!stmt->getRootTdb()) || (!ret_explain_len)) {
    diags << DgSqlCode(-CLI_STMT_NOT_EXISTS);
    return SQLCLI_ReturnCode(&currContext, -CLI_STMT_NOT_EXISTS);
  }

  retcode = ExExplainTcb::getExplainData(stmt->getRootTdb(), explain_ptr, explain_buf_len, ret_explain_len, &diags,
                                         currContext.exCollHeap());

  if (diags.getNumber()) {
    return diags.mainSQLCODE();
  } else if (retcode < 0) {
    diags << DgSqlCode(retcode);
    return retcode;
  }

  return retcode;
}

int SQLCLI_StoreExplainData(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN*/ long *exec_start_utc_ts,
    /*IN*/ char *query_id,
    /*INOUT*/ char *explain_ptr,
    /*IN*/ int explain_buf_len) {
  int retcode = 0;

  // create initial context, if first call, and add module, if any.
  retcode = CliPrologue(cliGlobals, NULL);
  if (isERROR(retcode)) {
    return retcode;
  }

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  if ((!explain_ptr) || (explain_buf_len <= 0)) {
    diags << DgSqlCode(-CLI_GENCODE_BUFFER_TOO_SMALL);
    return SQLCLI_ReturnCode(&currContext, -CLI_GENCODE_BUFFER_TOO_SMALL);
  }

  retcode = ExExplainTcb::storeExplainInRepos(cliGlobals, exec_start_utc_ts, query_id, strlen(query_id), explain_ptr,
                                              explain_buf_len);
  if (retcode < 0) {
    if (diags.getNumber()) {
      return diags.mainSQLCODE();
    } else {
      diags << DgSqlCode(retcode);
    }

    return retcode;
  }

  return retcode;
}

int SQLCLI_ResDescName(/*IN*/ CliGlobals *cliGlobals,
                       /*INOUT*/ SQLDESC_ID *descriptor_id,
                       /*IN  OPTIONAL*/ SQLSTMT_ID *from_statement,
                       /* (SQLWHAT_DESC) *IN  OPTIONAL*/ int what_desc) {
  int retcode;

  // create initial context, if first call, and add module, if any.
  retcode = CliPrologue(cliGlobals, descriptor_id->module);
  if (isERROR(retcode)) return retcode;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  Descriptor *desc = 0;

  if (from_statement) {
    Statement *stmt = currContext.getStatement(from_statement);

    /* stmt must exist */
    if (!stmt) {
      diags << DgSqlCode(-CLI_STMT_NOT_EXISTS);
      return SQLCLI_ReturnCode(&currContext, -CLI_STMT_NOT_EXISTS);
    }

    desc = stmt->getDefaultDesc(what_desc);
  } else {
    desc = currContext.getDescriptor(descriptor_id);
  }

  /* desc must exist */
  if (!desc) {
    diags << DgSqlCode(-CLI_DESC_NOT_EXISTS);
    return SQLCLI_ReturnCode(&currContext, -CLI_DESC_NOT_EXISTS);
  }

  if (descriptor_id->name_mode == desc_handle) {
    descriptor_id->handle = desc->getDescHandle();
  } else if (descriptor_id->name_mode == desc_name) {
    if ((!descriptor_id->identifier) || (!desc->getDescName()) || (!desc->getDescName()->identifier)) {
      diags << DgSqlCode(-CLI_INVALID_DESC_INFO_REQUEST);
      return SQLCLI_ReturnCode(&currContext, -CLI_INVALID_DESC_INFO_REQUEST);
    }

    // On input, descriptor_id->identifier_len contains the max length of
    // the buffer where identifier name will be copied.
    // On output, it contains the actual length of the identifier copied.
    // Returned identifier is NOT null terminated as this method could
    // be called from both C and Cobol.
    int lenToCopy = MINOF(descriptor_id->identifier_len, desc->getDescName()->identifier_len);
    str_cpy_all((char *)descriptor_id->identifier, desc->getDescName()->identifier, lenToCopy);
    descriptor_id->identifier_len = lenToCopy;
  }

  return SUCCESS;
}

int SQLCLI_ResStmtName(/*IN*/ CliGlobals *cliGlobals,
                       /*INOUT*/ SQLSTMT_ID *statement_id) {
  int retcode;

  // create initial context, if first call, and add module, if any.
  retcode = CliPrologue(cliGlobals, statement_id->module);
  if (isERROR(retcode)) return retcode;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  /* get the statement */
  Statement *stmt = currContext.getStatement(statement_id);

  /* stmt must exist */
  if (!stmt) {
    diags << DgSqlCode(-CLI_STMT_NOT_EXISTS);
    return SQLCLI_ReturnCode(&currContext, -CLI_STMT_NOT_EXISTS);
  }

  statement_id->name_mode = stmt_handle;
  statement_id->handle = stmt->getStmtHandle();

  return SUCCESS;
}
// statement_id must point to an existing statement
// The Descriptor Id  cursor_name must be of name_mode
// cursor_name or cursor_via_desc   **exception ODBC/JDBC

int SQLCLI_SetCursorName(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN*/ SQLSTMT_ID *statement_id,
    /*IN*/ SQLSTMT_ID *cursor_id)  // must of name_mode cursor_name or cursor_via_desc   **exception ODBC/JDBC
{
  int retcode;
  SQLDESC_ID *curs_id;

  NAHeap &heap = *(cliGlobals->currContext()->exHeap());

  // create initial context, if first call, and add module, if any.
  retcode = CliPrologue(cliGlobals, statement_id->module);
  if (isERROR(retcode)) return retcode;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  Statement *stmt = currContext.getStatement(statement_id);

  /* stmt must exist */
  if (!stmt) {
    diags << DgSqlCode(-CLI_STMT_NOT_EXISTS);
    return SQLCLI_ReturnCode(&currContext, -CLI_STMT_NOT_EXISTS);
  } else if (!stmt->allocated()) {
    diags << DgSqlCode(-CLI_NOT_DYNAMIC_STMT) << DgString0("set cursor name for");

    return SQLCLI_ReturnCode(&currContext, -CLI_NOT_DYNAMIC_STMT);
  }

  // For the sake of ODBC/JDBC, we will leave this check in here
  // since they do not pass a STMT_ID for cursor_name but a DESC_ID
  // with name mode desc_handle
  if (cursor_id->name_mode == desc_handle) {
    Descriptor *desc = currContext.getDescriptor(cursor_id);
    /* descriptor must exist */
    if (!desc) {
      diags << DgSqlCode(-CLI_DESC_NOT_EXISTS);
      return SQLCLI_ReturnCode(&currContext, -CLI_DESC_NOT_EXISTS);
    }
    stmt->setCursorName(desc->getVarData(1));

    return SUCCESS;
  }

  switch (cursor_id->name_mode) {
    case cursor_name:
      // Simply set the cursor name as it is passed. Do not parse it
      // Assumption is that the caller is passing it in  valid ansi format
      stmt->setCursorName(cursor_id->identifier);
      break;
    case curs_via_desc:
      // This is the extended dynamic case. In this case the CLI will
      // convert/check the input to valid ansi format
      curs_id = Descriptor::GetNameViaDesc((SQLDESC_ID *)cursor_id, &currContext, heap);
      if (curs_id) stmt->setCursorName(curs_id->identifier);
      break;
    default:
      diags << DgSqlCode(-CLI_INVALID_ATTR_VALUE);
      return SQLCLI_ReturnCode(&currContext, -CLI_INVALID_ATTR_VALUE);
      break;
  }
  return SUCCESS;
}

int SQLCLI_SetStmtAttr(/*IN*/ CliGlobals *cliGlobals,
                       /*IN*/ SQLSTMT_ID *statement_id,
                       /*IN SQLATTR_TYPE */ int attrName,
                       /*IN OPTIONAL*/ int numeric_value,
                       /*IN OPTIONAL*/ char *string_value) {
  int retcode = SUCCESS;

  // create initial context, if first call, and add module, if any.
  retcode = CliPrologue(cliGlobals, statement_id->module);
  if (isERROR(retcode)) return retcode;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  Statement *stmt = currContext.getStatement(statement_id);

  /* stmt must exist */
  if (!stmt) {
    diags << DgSqlCode(-CLI_STMT_NOT_EXISTS);
    return SQLCLI_ReturnCode(&currContext, -CLI_STMT_NOT_EXISTS);
  }

  if (attrName == SQL_ATTR_CURSOR_HOLDABLE) {
    switch (numeric_value) {
      case SQL_NONHOLDABLE:
        retcode = stmt->setHoldable(diags, FALSE);
        break;
      case SQL_HOLDABLE:
        retcode = stmt->setHoldable(diags, TRUE);
        break;
      default:
        diags << DgSqlCode(-CLI_INVALID_ATTR_VALUE);
        return SQLCLI_ReturnCode(&currContext, -CLI_INVALID_ATTR_VALUE);
    }
  } else if (attrName == SQL_ATTR_INPUT_ARRAY_MAXSIZE) {
    retcode = stmt->setInputArrayMaxsize(diags, numeric_value);
  } else if (attrName == SQL_ATTR_ROWSET_ATOMICITY) {
    switch (numeric_value) {
      case SQL_NOT_SPECIFIED:
        retcode = stmt->setRowsetAtomicity(diags, Statement::UNSPECIFIED_);
        break;
      case SQL_ATOMIC:
        retcode = stmt->setRowsetAtomicity(diags, Statement::ATOMIC_);
        break;
      case SQL_NOT_ATOMIC:
        retcode = stmt->setRowsetAtomicity(diags, Statement::NOT_ATOMIC_);
        break;
      default:
        diags << DgSqlCode(-CLI_INVALID_ATTR_VALUE);
        return SQLCLI_ReturnCode(&currContext, -CLI_INVALID_ATTR_VALUE);
    }
    // If stmt is in INITIAL state then the statement did not get prepared.
    // Or if it is in PREPARE state, then it is a nowaited prepare which is
    //  still being prepared.

    if ((stmt->getState() != Statement::INITIAL_) && (stmt->getState() != Statement::PREPARE_)) {
      diags << DgSqlCode(CLI_STMT_NEEDS_PREPARE);
      return SQLCLI_ReturnCode(&currContext, CLI_STMT_NEEDS_PREPARE);
    }
  } else if (attrName == SQL_ATTR_NOT_ATOMIC_FAILURE_LIMIT) {
    if ((numeric_value >= 30) || (numeric_value == ComCondition::NO_LIMIT_ON_ERROR_CONDITIONS)) {
      retcode = stmt->setNotAtomicFailureLimit(diags, numeric_value);
    } else {
      diags << DgSqlCode(-CLI_INVALID_ATTR_VALUE);
      return SQLCLI_ReturnCode(&currContext, -CLI_INVALID_ATTR_VALUE);
    }
  } else if (attrName == SQL_ATTR_UNIQUE_STMT_ID) {
    stmt->setUniqueStmtId(string_value);
  } else if (attrName == SQL_ATTR_COPY_STMT_ID_TO_DIAGS) {
    diags.setAllSqlID(stmt->getUniqueStmtId());
  } else if (attrName == SQL_ATTR_PARENT_QID) {
    retcode = stmt->setParentQid(string_value);
    if (retcode != 0) {
      diags << DgSqlCode(retcode);
    }
  } else if (attrName == SQL_ATTR_SYNC_QUERY_FOR_BINGLOG) {
    if (numeric_value != 0) stmt->setSyncQueryForBinglog(TRUE);
  } else {
    diags << DgSqlCode(-CLI_INVALID_ATTR_NAME);
    return SQLCLI_ReturnCode(&currContext, -CLI_INVALID_ATTR_NAME);
  }

  if (retcode) return diags.mainSQLCODE();

  return retcode;
}

int SQLCLI_GetSessionAttr(/*IN*/ CliGlobals *cliGlobals,
                          /*IN SESSIONATTR_TYPE */ int attrName,
                          /*OUT OPTIONAL*/ int *numeric_value,
                          /*OUT OPTIONAL*/ char *string_value,
                          /*IN OPTIONAL*/ int max_string_len,
                          /*OUT OPTIONAL*/ int *len_of_item) {
  int retcode = SUCCESS;

  // create initial context, if first call, and add module, if any.
  retcode = CliPrologue(cliGlobals, NULL);
  if (isERROR(retcode)) return retcode;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();
  NABoolean bufferNullOrTooSmall = FALSE;

  switch (attrName) {
    case SESSION_ATTR_ID: {
      const char *sessionID = currContext.getSessionId();
      int bytesNeeded = (sessionID ? strlen(sessionID) + 1 : 0);
      if (sessionID && string_value && max_string_len >= bytesNeeded)
        strcpy(string_value, sessionID);
      else
        bufferNullOrTooSmall = TRUE;
      if (len_of_item) *len_of_item = bytesNeeded;
    } break;

    case SESSION_PARENT_QID: {
      SessionDefaults *sd = currContext.getSessionDefaults();
      const char *queryID = (sd ? sd->getParentQid() : NULL);
      int bytesNeeded = (queryID ? strlen(queryID) + 1 : 0);
      if (queryID && string_value && max_string_len >= bytesNeeded)
        strcpy(string_value, queryID);
      else
        bufferNullOrTooSmall = TRUE;
      if (len_of_item) *len_of_item = bytesNeeded;

      if (bufferNullOrTooSmall) {
        // Prior to Seaquest we were returning CLI_BUFFER_TOO_SMALL
        // (which is specific to query IDs) when the query ID target
        // buffer is null or too small. We will continue to return
        // that code in case any CLI callers expect it. For other
        // attributes we return a more general error saying the
        // "session attribute" is null or too small.
        diags << DgSqlCode(-CLI_BUFFER_TOO_SMALL);
        return SQLCLI_ReturnCode(&currContext, -CLI_BUFFER_TOO_SMALL);
      }
    } break;

    case SESSION_DATABASE_USER_NAME: {
      const char *authID = currContext.getDatabaseUserName();
      int bytesNeeded = (authID ? strlen(authID) + 1 : 0);
      if (authID && string_value && max_string_len >= bytesNeeded)
        strcpy(string_value, authID);
      else
        bufferNullOrTooSmall = TRUE;
      if (len_of_item) *len_of_item = bytesNeeded;
    } break;

    case SESSION_DATABASE_USER_ID: {
      int *userID = currContext.getDatabaseUserID();
      ex_assert(userID, "Context user ID should never be NULL");
      if (numeric_value)
        *numeric_value = *userID;
      else
        bufferNullOrTooSmall = TRUE;
    } break;

    case SESSION_SESSION_USER_ID: {
      int *sessionUserID = currContext.getSessionUserID();
      ex_assert(sessionUserID, "Context session user ID should never be NULL");
      if (numeric_value)
        *numeric_value = *sessionUserID;
      else
        bufferNullOrTooSmall = TRUE;
    } break;

    case SESSION_EXTERNAL_USER_NAME: {
      const char *externalUsername = currContext.getExternalUserName();
      int bytesNeeded = (externalUsername ? strlen(externalUsername) + 1 : 0);
      if (externalUsername && string_value && max_string_len >= bytesNeeded)
        strcpy(string_value, externalUsername);
      else
        bufferNullOrTooSmall = TRUE;
      if (len_of_item) *len_of_item = bytesNeeded;
    } break;

    case SESSION_TENANT_NAME: {
      const char *tenantName = currContext.getTenantName();
      int bytesNeeded = (tenantName ? strlen(tenantName) + 1 : 0);
      if (tenantName && string_value && max_string_len >= bytesNeeded)
        strcpy(string_value, tenantName);
      else
        bufferNullOrTooSmall = TRUE;
      if (len_of_item) *len_of_item = bytesNeeded;
    } break;

    case SESSION_TENANT_ID: {
      if (numeric_value)
        *numeric_value = currContext.getTenantID();
      else
        bufferNullOrTooSmall = TRUE;
    } break;

    default: {
      diags << DgSqlCode(-CLI_INVALID_ATTR_NAME);
      return SQLCLI_ReturnCode(&currContext, -CLI_INVALID_ATTR_NAME);
    } break;

  }  // switch (attrName)

  if (bufferNullOrTooSmall) {
    diags << DgSqlCode(-CLI_SESSION_ATTR_BUFFER_TOO_SMALL);
    return SQLCLI_ReturnCode(&currContext, -CLI_SESSION_ATTR_BUFFER_TOO_SMALL);
  }

  return SUCCESS;
}

int SQLCLI_GetAuthID(CliGlobals *cliGlobals, const char *authName, int &authID)

{
  int retcode = 0;

  // create initial context, if first call, and add module, if any.
  retcode = CliPrologue(cliGlobals, NULL);
  if (isERROR(retcode)) return retcode;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  retcode = currContext.getAuthIDFromName(authName, authID);

  return CliEpilogue(cliGlobals, NULL, retcode);
}

int SQLCLI_GetAuthName(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN*/ int auth_id,
    /*OUT*/ char *string_value,
    /*IN*/ int max_string_len,
    /*OUT */ int &len_of_item) {
  int retcode = 0;

  // create initial context, if first call, and add module, if any.
  retcode = CliPrologue(cliGlobals, NULL);
  if (isERROR(retcode)) return retcode;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  retcode = currContext.getAuthNameFromID(auth_id, string_value, max_string_len, len_of_item);

  return CliEpilogue(cliGlobals, NULL, retcode);
}

int SQLCLI_GetDatabaseUserName(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN*/ int user_id,
    /*OUT*/ char *string_value,
    /*IN*/ int max_string_len,
    /*OUT OPTIONAL*/ int *len_of_item) {
  int retcode = 0;

  // create initial context, if first call, and add module, if any.
  retcode = CliPrologue(cliGlobals, NULL);
  if (isERROR(retcode)) return retcode;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  retcode = currContext.getAuthNameFromID(user_id, string_value, max_string_len, *len_of_item);

  return CliEpilogue(cliGlobals, NULL, retcode);
}

int SQLCLI_GetDatabaseUserID(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN*/ char *string_value,
    /*OUT*/ int *numeric_value) {
  int retcode = 0;

  // create initial context, if first call, and add module, if any.
  retcode = CliPrologue(cliGlobals, NULL);
  if (isERROR(retcode)) return retcode;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  retcode = currContext.getAuthIDFromName(string_value, *numeric_value);

  return CliEpilogue(cliGlobals, NULL, retcode);
}

int SQLCLI_GetAuthState(
    /*IN*/ CliGlobals *cliGlobals,
    /*OUT*/ int &authenticationType,
    /*OUT*/ bool &authorizationEnabled,
    /*OUT*/ bool &authorizationReady,
    /*OUT*/ bool &auditingEnabled) {
  int retcode = 0;

  // create initial context, if first call, and add module, if any.
  retcode = CliPrologue(cliGlobals, NULL);
  if (isERROR(retcode)) return retcode;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  currContext.getAuthState(authenticationType, authorizationEnabled, authorizationReady, auditingEnabled);

  return CliEpilogue(cliGlobals, NULL, retcode);
}

int SQLCLI_GetRoleList(CliGlobals *cliGlobals, int &numEntries, int *&roleIDs, int *&granteeIDs)

{
  int retcode = 0;

  // create initial context, if first call, and add module, if any.
  retcode = CliPrologue(cliGlobals, NULL);
  if (isERROR(retcode)) return retcode;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  retcode = currContext.getRoleList(numEntries, roleIDs, granteeIDs);

  return CliEpilogue(cliGlobals, NULL, retcode);
}

int SQLCLI_ResetRoleList(CliGlobals *cliGlobals)

{
  int retcode = 0;

  // create initial context, if first call, and add module, if any.
  retcode = CliPrologue(cliGlobals, NULL);
  if (isERROR(retcode)) return retcode;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  retcode = currContext.resetRoleList();

  return CliEpilogue(cliGlobals, NULL, retcode);
}

int SQLCLI_GetUserAttrs(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN*/ const char *username,
    /*IN*/ const char *tenant_name,
    /*OUT*/ USERS_INFO *users_info,
    /*OUT*/ struct SQLSEC_AuthDetails *auth_details) {
  int retcode = 0;

  // create initial context, if first call, and add module, if any.
  retcode = CliPrologue(cliGlobals, NULL);
  if (isERROR(retcode)) return retcode;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  currContext.getUserAttrs(username, tenant_name, users_info, auth_details);

  return CliEpilogue(cliGlobals, NULL, retcode);
}

int SQLCLI_RegisterUser(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN*/ const char *username,
    /*IN*/ const char *config,
    /*OUT*/ USERS_INFO *users_info,
    /*OUT*/ struct SQLSEC_AuthDetails *auth_details) {
  int retcode = 0;

  // create initial context, if first call, and add module, if any.
  retcode = CliPrologue(cliGlobals, NULL);
  if (isERROR(retcode)) return retcode;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  retcode = currContext.registerUser(username, config, users_info, auth_details);

  return CliEpilogue(cliGlobals, NULL, retcode);
}

int SQLCLI_GetAuthErrPwdCnt(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN*/ int userid,
    /*OUT*/ Int16 &errcnt) {
  int retcode = 0;

  // create initial context, if first call, and add module, if any.
  retcode = CliPrologue(cliGlobals, NULL);
  if (isERROR(retcode)) return retcode;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  retcode = currContext.getAuthErrPwdCnt(userid, errcnt);

  return CliEpilogue(cliGlobals, NULL, retcode);
}

int SQLCLI_UpdateAuthErrPwdCnt(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN*/ int userid,
    /*IN*/ Int16 errcnt,
    /*IN*/ bool reset) {
  int retcode = 0;

  // create initial context, if first call, and add module, if any.
  retcode = CliPrologue(cliGlobals, NULL);
  if (isERROR(retcode)) return retcode;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  retcode = currContext.updateAuthErrPwdCnt(userid, errcnt, reset);

  return CliEpilogue(cliGlobals, NULL, retcode);
}

int SQLCLI_SetSessionAttr(/*IN*/ CliGlobals *cliGlobals,
                          /*IN SESSIONATTR_TYPE*/ int attrName,
                          /*IN OPTIONAL*/ int numeric_value,
                          /*IN OPTIONAL*/ const char *string_value) {
  int retcode = SUCCESS;

  // create initial context, if first call, and add module, if any.
  retcode = CliPrologue(cliGlobals, NULL);
  if (isERROR(retcode)) return retcode;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  switch (attrName) {
    case SESSION_DATABASE_USER_ID: {
      retcode = currContext.setDatabaseUserByID(numeric_value);
    } break;

    case SESSION_DATABASE_USER_NAME: {
      retcode = currContext.setDatabaseUserByName(string_value);
    } break;

    case SESSION_DATABASE_USER: {
      // The call to setDatabaseUser set values in the Context
      // it does not return any errors
      currContext.setDatabaseUser(numeric_value, string_value);
    } break;

    case SESSION_TENANT_ID: {
      currContext.setTenantID(numeric_value);
    } break;

    case SESSION_TENANT_NAME: {
      currContext.setTenantName(string_value);
    } break;

    case SESSION_TENANT_BY_NAME: {
      retcode = currContext.setTenantByName(string_value);  // This does a lookup of
      // the tenants table and sets values in the context.
    } break;
    case SESSION_TENANT_NODES: {
      currContext.setTenantNodes(string_value);
    } break;
    case SESSION_TENANT_DEFAULT_SCHEMA: {
      currContext.setTenantDefaultSchema(string_value);
    } break;
    // LCOV_EXCL_START
    case SESSION_PARENT_QID: {
      SessionDefaults *sd = currContext.getSessionDefaults();
      if (sd && string_value) sd->setParentQid(string_value, strlen(string_value));
    } break;
    default: {
      // Other attributes can be supported in time
      diags << DgSqlCode(-CLI_INVALID_ATTR_NAME);
      return SQLCLI_ReturnCode(&currContext, -CLI_INVALID_ATTR_NAME);
    } break;
  }

  return CliEpilogue(cliGlobals, NULL);
}

int SQLCLI_GetUniqueQueryIdAttrs(/*IN*/ CliGlobals *cliGlobals,
                                 /*IN*/ char *queryId,
                                 /*IN*/ int queryIdLen,
                                 /*IN*/ int no_of_attrs,
                                 /*INOUT*/ UNIQUEQUERYID_ATTR queryid_attrs[]) {
  int retcode = SUCCESS;

  // create initial context, if first call, and add module, if any.
  retcode = CliPrologue(cliGlobals, NULL);
  if (isERROR(retcode)) return retcode;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  /* stmt must exist */
  if (!queryId) {
    diags << DgSqlCode(-CLI_STMT_NOT_EXISTS);
    return SQLCLI_ReturnCode(&currContext, -CLI_STMT_NOT_EXISTS);
  }

  for (int i = 0; i < no_of_attrs && !retcode; i++) {
    long temp_nvol = (long)queryid_attrs[i].num_val_or_len;
    retcode = ComSqlId::getSqlQueryIdAttr(queryid_attrs[i].attr_type, queryId, queryIdLen, temp_nvol,
                                          queryid_attrs[i].string_val);
    queryid_attrs[i].num_val_or_len = temp_nvol;
  }

  return retcode;
}

// A helper function to copy one statement attribute into the target
// buffer
int CopyOneStmtAttr(/*IN*/ Statement &stmt,
                    /*IN*/ ContextCli &context,
                    /*OUT*/ ComDiagsArea &diags,
                    /*IN*/ int attrName,
                    /*IN*/ int index,
                    /*OUT*/ int *numeric_value,
                    /*OUT*/ char *string_value,
                    /*IN*/ int max_string_len,
                    /*OUT*/ int *len_of_item) {
  int retcode = SUCCESS;

  if (attrName == SQL_ATTR_CURSOR_HOLDABLE) {
    if (stmt.isAnsiHoldable() || stmt.isPubsubHoldable()) {
      *numeric_value = SQL_HOLDABLE;
    } else {
      *numeric_value = SQL_NONHOLDABLE;
    }
  } else if (attrName == SQL_ATTR_INPUT_ARRAY_MAXSIZE) {
    *numeric_value = (int)stmt.getInputArrayMaxsize();
  } else if (attrName == SQL_ATTR_QUERY_TYPE) {
    if (!stmt.getRootTdb()) {
      // for display query e.g. display select ...
      // just return SUCCESS to suppress error message
      if (stmt.isDISPLAY()) {
        return SQLCLI_ReturnCode(&context, DISPLAY_DONE_WARNING);
      } else {
        diags << DgSqlCode(-CLI_STMT_NOT_EXISTS);
        return SQLCLI_ReturnCode(&context, -CLI_STMT_NOT_EXISTS);
      }
    }
    *numeric_value = stmt.getRootTdb()->getQueryType();
  } else if (attrName == SQL_ATTR_SUBQUERY_TYPE) {
    if (!stmt.getRootTdb()) {
      // for display query e.g. display select ...
      // just return SUCCESS to suppress error message
      if (stmt.isDISPLAY()) {
        return SQLCLI_ReturnCode(&context, DISPLAY_DONE_WARNING);
      } else {
        diags << DgSqlCode(-CLI_STMT_NOT_EXISTS);
        return SQLCLI_ReturnCode(&context, -CLI_STMT_NOT_EXISTS);
      }
    }
    *numeric_value = stmt.getRootTdb()->getSubqueryType();
  } else if (attrName == SQL_ATTR_MAX_RESULT_SETS) {
    if (!stmt.getRootTdb()) {
      diags << DgSqlCode(-CLI_STMT_NOT_EXISTS);
      return SQLCLI_ReturnCode(&context, -CLI_STMT_NOT_EXISTS);
    }
    *numeric_value = stmt.getRootTdb()->getMaxResultSets();
  } else if (attrName == SQL_ATTR_XN_NEEDED) {
    *numeric_value = (stmt.getRootTdb()->transactionReqd() ? 1 : 0);
  } else if (attrName == SQL_ATTR_UNIQUE_STMT_ID) {
    int actual_len = stmt.getUniqueStmtIdLen();

    if (len_of_item) *len_of_item = actual_len;

    if (stmt.getUniqueStmtId()) {
      if ((max_string_len) && (actual_len > max_string_len)) {
        diags << DgSqlCode(-CLI_BUFFER_TOO_SMALL);
        return SQLCLI_ReturnCode(&context, -CLI_BUFFER_TOO_SMALL);
      }

      strncpy(string_value, stmt.getUniqueStmtId(), stmt.getUniqueStmtIdLen());
    }
  } else if (attrName == SQL_ATTR_RS_PROXY_SYNTAX) {
    if (!stmt.getRootTdb()) {
      diags << DgSqlCode(-CLI_STMT_NOT_EXISTS);
      return SQLCLI_ReturnCode(&context, -CLI_STMT_NOT_EXISTS);
    }

    stmt.getRSProxySyntax(string_value, max_string_len, len_of_item);

    if (!string_value || (len_of_item && (max_string_len < *len_of_item))) {
      diags << DgSqlCode(-CLI_RS_PROXY_BUFFER_SMALL_OR_NULL);
      return SQLCLI_ReturnCode(&context, -CLI_RS_PROXY_BUFFER_SMALL_OR_NULL);
    }
  } else if (attrName == SQL_ATTR_CONSUMER_QUERY_TEXT) {
    int actual_len = stmt.getConsumerQueryLen(index - 1);
    if (len_of_item) *len_of_item = actual_len;
    if (actual_len > 0) {
      if (actual_len > max_string_len) {
        diags << DgSqlCode(-CLI_CONSUMER_QUERY_BUF_TOO_SMALL) << DgInt0(index);
        return SQLCLI_ReturnCode(&context, -CLI_CONSUMER_QUERY_BUF_TOO_SMALL);
      }
      stmt.getConsumerQuery(index - 1, string_value, max_string_len);
    }
  } else if (attrName == SQL_ATTR_CONSUMER_CPU) {
    *numeric_value = stmt.getConsumerCpu(index - 1);
  } else if (attrName == SQL_ATTR_PARENT_QID) {
    int actual_len;
    const char *parentQid = stmt.getParentQid();
    if (parentQid)
      actual_len = str_len(parentQid);
    else
      actual_len = 0;
    if (len_of_item) *len_of_item = actual_len;
    if (parentQid) {
      if ((max_string_len) && (actual_len > max_string_len)) {
        diags << DgSqlCode(-CLI_BUFFER_TOO_SMALL);
        return SQLCLI_ReturnCode(&context, -CLI_BUFFER_TOO_SMALL);
      }
      strncpy(string_value, parentQid, actual_len);
    }

  } else if (attrName == SQL_ATTR_CURSOR_UPDATABLE) {
    if (stmt.getRootTdb()) {
      *numeric_value = stmt.getRootTdb()->getCursorType();
    } else {
      diags << DgSqlCode(-CLI_STMT_NOT_EXISTS);
      return SQLCLI_ReturnCode(&context, -CLI_STMT_NOT_EXISTS);
    }

  } else {
    diags << DgSqlCode(-CLI_INVALID_ATTR_NAME);
    return SQLCLI_ReturnCode(&context, -CLI_INVALID_ATTR_NAME);
  }

  return retcode;
}

int SQLCLI_GetStmtAttr(/*IN*/ CliGlobals *cliGlobals,
                       /*IN*/ SQLSTMT_ID *statement_id,
                       /*IN SQLATTR_TYPE */ int attrName,
                       /*OUT OPTIONAL*/ int *numeric_value,
                       /*OUT OPTIONAL*/ char *string_value,
                       /*IN OPTIONAL*/ int max_string_len,
                       /*OUT OPTIONAL*/ int *len_of_item) {
  int retcode = SUCCESS;

  // create initial context, if first call, and add module, if any.
  retcode = CliPrologue(cliGlobals, statement_id->module);
  if (isERROR(retcode)) return retcode;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  Statement *stmt = currContext.getStatement(statement_id);

  /* stmt must exist */
  if (!stmt) {
    diags << DgSqlCode(-CLI_STMT_NOT_EXISTS);
    return SQLCLI_ReturnCode(&currContext, -CLI_STMT_NOT_EXISTS);
  }

  retcode =
      CopyOneStmtAttr(*stmt, currContext, diags, attrName, 0, numeric_value, string_value, max_string_len, len_of_item);
  return retcode;
}

int SQLCLI_GetStmtAttrs(/*IN*/ CliGlobals *cliGlobals,
                        /*IN*/ SQLSTMT_ID *statement_id,
                        /*IN*/ int number_of_attrs,
                        /*INOUT*/ SQLSTMT_ATTR attrs[],
                        /*OUT OPTIONAL*/ int *num_returned) {
  int retcode = SUCCESS;
  *num_returned = 0;

  // create initial context, if first call, and add module, if any.
  retcode = CliPrologue(cliGlobals, statement_id->module);
  if (isERROR(retcode)) return retcode;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  Statement *stmt = currContext.getStatement(statement_id);

  /* stmt must exist */
  if (!stmt) {
    diags << DgSqlCode(-CLI_STMT_NOT_EXISTS);
    return SQLCLI_ReturnCode(&currContext, -CLI_STMT_NOT_EXISTS);
  }

  for (int i = 0; i < number_of_attrs && retcode >= 0; i++) {
    const SQLSTMT_ATTR &attr = attrs[i];
    retcode = CopyOneStmtAttr(*stmt, currContext, diags, attr.attr_type, attr.index, attr.numeric_value,
                              attr.string_value, attr.max_string_len, attr.len_of_item);
    if (retcode >= 0) (*num_returned)++;
  }

  return retcode;
}
int SQLCLI_SetDescEntryCount(/*IN*/ CliGlobals *cliGlobals,
                             /*IN*/ SQLDESC_ID *desc_id,
                             /*IN*/ SQLDESC_ID *input_descriptor) {
  int retcode;

  if (!desc_id) return -CLI_INTERNAL_ERROR;

  // create initial context, if first call, and add module, if any.
  retcode = CliPrologue(cliGlobals, desc_id->module);
  if (isERROR(retcode)) return retcode;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  Descriptor *desc = currContext.getDescriptor(desc_id);

  /* descriptor must exist */
  if (!desc) {
    diags << DgSqlCode(-CLI_DESC_NOT_EXISTS);
    return SQLCLI_ReturnCode(&currContext, -CLI_DESC_NOT_EXISTS);
  }

  Descriptor *input_desc = currContext.getDescriptor(input_descriptor);

  /* descriptor must exist */
  if (!input_desc) {
    diags << DgSqlCode(-CLI_DESC_NOT_EXISTS);
    return SQLCLI_ReturnCode(&currContext, -CLI_DESC_NOT_EXISTS);
  }

  // JORGE: Why are we doing it this way. It may be better if we call
  //        getUsedEntryCount() on the input descriptor. ??

  int num_entries = 0L;
  retcode = InputValueFromNumericHostvar(input_desc, 1, num_entries);
  if (isERROR(retcode)) return SQLCLI_ReturnCode(&currContext, retcode);

  desc->setUsedEntryCount(num_entries);

  // Now allocate the space for num_entries worth of descriptors.
  desc->addEntry(num_entries);

  return SUCCESS;
}
int SQLCLI_SetDescItems(/*IN*/ CliGlobals *cliGlobals,
                        /*IN*/ SQLDESC_ID *desc_id,
                        /*IN*/ SQLDESC_ITEM desc_items[],
                        /*IN*/ SQLDESC_ID *value_num_descriptor,
                        /*IN*/ SQLDESC_ID *input_descriptor) {
  int retcode;
  int first_set = SQLDESC_ITEM_ORDER_COUNT, desc_item_entry[SQLDESC_ITEM_ORDER_COUNT];
  const int not_set = -1;

  // create initial context, if first call, and add module, if any.
  retcode = CliPrologue(cliGlobals, desc_id->module);
  if (isERROR(retcode)) return retcode;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  Descriptor *desc = currContext.getDescriptor(desc_id);

  /* descriptor must exist */
  if (!desc) {
    diags << DgSqlCode(-CLI_DESC_NOT_EXISTS);
    return SQLCLI_ReturnCode(&currContext, -CLI_DESC_NOT_EXISTS);
  }

  Descriptor *value_num_desc = currContext.getDescriptor(value_num_descriptor);

  /* descriptor must exist */
  if (!value_num_desc) {
    diags << DgSqlCode(-CLI_DESC_NOT_EXISTS);
    return SQLCLI_ReturnCode(&currContext, -CLI_DESC_NOT_EXISTS);
  }

  Descriptor *input_desc = currContext.getDescriptor(input_descriptor);

  /* descriptor must exist */
  if (!input_desc) {
    diags << DgSqlCode(-CLI_DESC_NOT_EXISTS);
    return SQLCLI_ReturnCode(&currContext, -CLI_DESC_NOT_EXISTS);
  }

  int inputUsedEntryCount = input_desc->getUsedEntryCount();

  int i = 0;
  for (; i < SQLDESC_ITEM_ORDER_COUNT; i++) desc_item_entry[i] = not_set;
  for (i = 0; i < inputUsedEntryCount; i++)
    if (desc_items[i].item_id <= SQLDESC_ITEM_MAX && SQLDESC_ITEM_ORDER[desc_items[i].item_id - 1] >= 0 &&
        desc_item_entry[SQLDESC_ITEM_ORDER[desc_items[i].item_id - 1]] == not_set) {
      desc_item_entry[SQLDESC_ITEM_ORDER[desc_items[i].item_id - 1]] = i;
      if (SQLDESC_ITEM_ORDER[desc_items[i].item_id - 1] < first_set)
        first_set = SQLDESC_ITEM_ORDER[desc_items[i].item_id - 1];
    } else {
      diags << DgSqlCode(-CLI_INVALID_DESC_ENTRY);
      return diags.mainSQLCODE();
    }

  for (int j = first_set, count = 0; count < inputUsedEntryCount; j++) {
    if (desc_item_entry[j] != not_set) {
      count += 1;
      i = desc_item_entry[j];
      //
      // Get the entry number of the descriptor entry whose item is to be set.
      //
      int entry = 0L;
      retcode = InputValueFromNumericHostvar(value_num_desc, desc_items[i].value_num_desc_entry, entry);

      if (isERROR(retcode)) return SQLCLI_ReturnCode(&currContext, retcode);
      if (entry > desc->getMaxEntryCount()) {
        diags << DgSqlCode(-CLI_INVALID_DESC_ENTRY);
        return diags.mainSQLCODE();
      }

      if (entry > desc->getUsedEntryCount()) {
        retcode = desc->addEntry(entry);

        if (isERROR(retcode)) {
          diags << DgSqlCode(-CLI_INTERNAL_ERROR);
          return SQLCLI_ReturnCode(&currContext, -CLI_INTERNAL_ERROR);
        }
      }
      //
      // Get the address of the source where the item's value is stored.
      //
      char *var_ptr = 0;
      retcode = input_desc->getDescItem(i + 1, SQLDESC_VAR_PTR, (int *)&var_ptr, NULL, 0, NULL, 0);
      if (isERROR(retcode)) return SQLCLI_ReturnCode(&currContext, retcode);

      if (!var_ptr) return -CLI_INTERNAL_ERROR;

      // if item to be set is vardata, get input data length.
      // This is needed so that we can make a copy of input data.
      // Do the same for any attribute that need copying of data,
      // like SQLDESC_NAME, etc.
      int numericInput;
      numericInput = *(int *)var_ptr;

      if ((desc_items[i].item_id == SQLDESC_VAR_DATA) || (desc_items[i].item_id == SQLDESC_NAME) ||
          (desc_items[i].item_id == SQLDESC_HEADING)) {
        int length;
        retcode = input_desc->getDescItem(i + 1, SQLDESC_LENGTH, &length, 0, 0, 0, 0);
        if (isERROR(retcode)) return SQLCLI_ReturnCode(&currContext, retcode);

        numericInput = length;
      }

      // get size of memory to be bounds checked
      int memorySize = input_desc->getVarDataLength(i + 1);

      // if setting indicator variable get length of indicator variable
      if ((desc_items[i].item_id == SQLDESC_IND_PTR) || (desc_items[i].item_id == SQLDESC_IND_DATA)) {
        // since indicator, get length of indicator
        memorySize = input_desc->getIndLength(i + 1);
      }

      if (currContext.boundsCheckMemory((void *)var_ptr, memorySize))
        return SQLCLI_ReturnCode(&currContext, -CLI_USER_MEMORY_IN_EXECUTOR_SEGMENT);
      //
      // Set the item in the descriptor.
      //
      retcode = desc->setDescItem(entry, desc_items[i].item_id, numericInput, var_ptr, input_desc, i);
      if (isERROR(retcode)) return SQLCLI_ReturnCode(&currContext, retcode);
    }
  }
  return SUCCESS;
}

int SQLCLI_SetDescItems2(/*IN*/ CliGlobals *cliGlobals,
                         /*IN*/ SQLDESC_ID *desc_id,
                         /*IN*/ int no_of_desc_items,
                         /*IN*/ SQLDESC_ITEM desc_items[]) {
  int retcode;

  // create initial context, if first call, and add module, if any.
  retcode = CliPrologue(cliGlobals, desc_id->module);
  if (isERROR(retcode)) return retcode;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  Descriptor *desc = currContext.getDescriptor(desc_id);

  /* descriptor must exist */
  if (!desc) {
    diags << DgSqlCode(-CLI_DESC_NOT_EXISTS);
    return SQLCLI_ReturnCode(&currContext, -CLI_DESC_NOT_EXISTS);
  }

  for (int i = 0; i < no_of_desc_items; i++) {
    //
    // Get the entry number of the descriptor entry whose item is to be
    // set.
    //
    int entry = desc_items[i].entry;

    if (entry > desc->getUsedEntryCount()) {
      retcode = desc->addEntry(entry);
      if (isERROR(retcode)) {
        diags << DgSqlCode(-CLI_INTERNAL_ERROR);
        return SQLCLI_ReturnCode(&currContext, -CLI_INTERNAL_ERROR);
      }
    }

    // if item to be set is vardata, get input data length.
    // This is needed so that we can make a copy of input data.
    // Do the same for any attribute that need copying of data,
    // like SQLDESC_NAME, etc.
    if ((desc_items[i].item_id == SQLDESC_VAR_DATA) || (desc_items[i].item_id == SQLDESC_CHAR_SET_NAM) ||
        (desc_items[i].item_id == SQLDESC_NAME) || (desc_items[i].item_id == SQLDESC_HEADING)) {
      if (currContext.boundsCheckMemory((void *)desc_items[i].string_val, desc_items[i].num_val_or_len))
        return SQLCLI_ReturnCode(&currContext, -CLI_USER_MEMORY_IN_EXECUTOR_SEGMENT);
    }

    //
    // Set the item in the descriptor.
    //
    retcode = desc->setDescItem(desc_items[i].entry, desc_items[i].item_id, desc_items[i].num_val_or_len,
                                desc_items[i].string_val);
    if (isERROR(retcode)) return SQLCLI_ReturnCode(&currContext, retcode);
  }

  return SUCCESS;
}

int SQLCLI_SetDescPointers(/*IN*/ CliGlobals *cliGlobals,
                           /*IN*/ SQLDESC_ID *desc_id,
                           /*IN*/ int starting_entry,
                           /*IN*/ int num_ptr_pairs,
                           /*IN*/ int num_ap,
                           /*IN*/ va_list ap,
                           /*IN*/ SQLCLI_PTR_PAIRS ptr_pairs[]) {
  int retcode = SUCCESS;

  // create initial context, if first call, and add module, if any.
  retcode = CliPrologue(cliGlobals, desc_id->module);
  if (isERROR(retcode)) return retcode;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  Descriptor *desc = currContext.getDescriptor(desc_id);

  /* descriptor must exist */
  if (!desc) {
    diags << DgSqlCode(-CLI_DESC_NOT_EXISTS);
    return SQLCLI_ReturnCode(&currContext, -CLI_DESC_NOT_EXISTS);
  }

  va_list cpy;
  va_copy(cpy, ap);
  va_end(ap);

  retcode = local_SetDescPointers(desc, starting_entry, num_ptr_pairs, num_ap, &cpy, ptr_pairs);
  va_end(cpy);

  return SQLCLI_ReturnCode(&currContext, retcode);
}

int SQLCLI_SwitchContext(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN*/ SQLCTX_HANDLE context_handle,
    /*OUT OPTIONAL*/ SQLCTX_HANDLE *prev_context_handle,
    /*IN */ int allowSwitchBackToDefault) {
  int retcode = SUCCESS;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  if (!allowSwitchBackToDefault && (context_handle == cliGlobals->getDefaultContext()->getContextHandle())) {
    diags << DgSqlCode(-CLI_DEFAULT_CONTEXT_NOT_ALLOWED);
    return SQLCLI_ReturnCode(&currContext, -CLI_DEFAULT_CONTEXT_NOT_ALLOWED);
  }

  // find the new context
  ContextCli *newContext = cliGlobals->getContext(context_handle);
  if (!newContext) {
    diags << DgSqlCode(-CLI_CONTEXT_NOT_FOUND);
    return SQLCLI_ReturnCode(&currContext, -CLI_CONTEXT_NOT_FOUND);
  }
  try {
    if (prev_context_handle) *prev_context_handle = currContext.getContextHandle();

    // The CLI sometimes starts transactions during recursive CLI
    // calls (e.g. metadata lookup or resource fork reads during
    // fixup). Such transactions should have been committed before
    // the top-level CLI call returned. Make sure this is true
    // before allowing a switch context; if it is not true, data
    // integrity could be compromised if another context inherits
    // such a transaction.
    ExTransaction *exTransaction = currContext.getTransaction();
    if (exTransaction != NULL && exTransaction->exeStartedXnDuringRecursiveCLICall()) {
      // abort the transaction so this problem doesn't persist
      exTransaction->rollbackStatement();

      // report the error
      diags << DgSqlCode(-CLI_INTERR_ON_CONTEXT_SWITCH);
      return SQLCLI_ReturnCode(&currContext, -CLI_INTERR_ON_CONTEXT_SWITCH);
    }

    // Recursive calls to the CLI assume that the context is not
    // changed. Therefore, prevent attempts within the CLI to
    // switch contexts.
    if (currContext.getNumOfCliCalls() > 1) {
      // Just raise a vanilla internal error; this should never
      // happen.
      // diags << DgSqlCode(-CLI_INTERNAL_ERROR);
      // tmpSemaphore->release();
      // return SQLCLI_ReturnCode(&currContext,-CLI_INTERNAL_ERROR);
    }

    // Do not call CliPrologue which will switch transaction.
    // Wait till next CLI call to do the switching.

    retcode = cliGlobals->switchContext(newContext);
    if (retcode != 0) {
      diags << DgSqlCode(-20567) << DgInt0(retcode);
      return SQLCLI_ReturnCode(&currContext, -20567);
    }
  } catch (...) {
    return -CLI_INTERNAL_ERROR;
  }
  return retcode;
}

int SQLCLI_Xact(/*IN*/ CliGlobals *cliGlobals,
                /* IN (SQLTRANS_COMMAND) */ int command,
                /* OUT OPTIONAL */ SQLDESC_ID *transid_descriptor) {
  int retcode;

  // create initial context, if first call, and add module, if any.
  retcode = CliPrologue(cliGlobals, 0);

  if (isERROR(retcode)) return retcode;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  switch (command) {
    case SQLTRANS_STATUS: {
      // returns ERROR, if no transaction has been started.
      // Otherwise returns SUCCESS and (optionally) returns
      // the transaction id.
      if (!currContext.getTransaction()->xnInProgress()) {
        // this is used internally for checking, diags area is not set
        return (-CLI_TRANSACTION_NOT_STARTED);
      }
    } break;

    case SQLTRANS_QUIESCE: {
      if (currContext.getTransaction()->xnInProgress()) {
        // go through each context that has matched transid, and
        // release all transactional messages to ESPs,MXCMPs,MXUDR,and DP2.
        ContextCli *cntxt;
        cliGlobals->getContextList()->position();
        while (cntxt = (ContextCli *)cliGlobals->getContextList()->getNext()) {
          if (cntxt->getTransaction()->xnInProgress() &&
              (currContext.getTransaction()->getExeXnId() == cntxt->getTransaction()->getExeXnId()))
            cntxt->releaseAllTransactionalRequests();
        }
      }
    } break;
    case SQLTRANS_ROLLBACK_IMPLICIT_XN: {
      if ((currContext.getTransaction()->xnInProgress()) && (currContext.getTransaction()->exeStartedXn()) &&
          (currContext.getTransaction()->autoCommit())) {
        retcode = currContext.getTransaction()->rollbackTransactionWaited();
        if (retcode) {
          diags.mergeAfter(*(currContext.getTransaction()->getDiagsArea()));
          return -1;
        }
      }
    } break;

    case SQLTRANS_COMMIT: {
      if ((currContext.getTransaction()->xnInProgress()) && (currContext.getTransaction()->exeStartedXn())) {
        retcode = currContext.getTransaction()->commitTransaction();

        if (retcode && currContext.getTransaction()->getDiagsArea()) {
          diags.mergeAfter(*(currContext.getTransaction()->getDiagsArea()));
          return -1;
        }
      }
    } break;

    case SQLTRANS_ROLLBACK: {
      if ((currContext.getTransaction()->xnInProgress()) && (currContext.getTransaction()->exeStartedXn())) {
        retcode = currContext.getTransaction()->rollbackTransaction();
        if (retcode) {
          diags.mergeAfter(*(currContext.getTransaction()->getDiagsArea()));
          return -1;
        }
      }
    } break;

    case SQLTRANS_BEGIN: {
      if (!(currContext.getTransaction()->xnInProgress())) {
        retcode = currContext.getTransaction()->beginTransaction();
        if (retcode) {
          diags.mergeAfter(*(currContext.getTransaction()->getDiagsArea()));
          return -1;
        }
      }
    } break;

    case SQLTRANS_INHERIT: {
      currContext.getTransaction()->resetXnState();
      currContext.getTransaction()->inheritTransaction();
    } break;

    case SQLTRANS_SUSPEND: {
      currContext.getTransaction()->suspendTransaction();
    } break;

    case SQLTRANS_RESUME: {
      currContext.getTransaction()->resumeTransaction();
    } break;

    default:
      diags << DgSqlCode(-CLI_INVALID_SQLTRANS_COMMAND);
      return SQLCLI_ReturnCode(&currContext, diags.mainSQLCODE());

  }  // switch command

  // return transid, if transid_descriptor is provided.
  if (transid_descriptor) {
    Descriptor *transid_desc = currContext.getDescriptor(transid_descriptor);

    // descriptor must exist
    if (!transid_desc) {
      diags << DgSqlCode(-CLI_DESC_NOT_EXISTS);
      return SQLCLI_ReturnCode(&currContext, -CLI_DESC_NOT_EXISTS);
    }

    // return the transaction identifier in the
    // descriptor provided. Note that for commit/rollback
    // requests, this will return a -1 in the transid desc.
    long *pTransid = 0;
    retcode = transid_desc->getDescItem(1, SQLDESC_VAR_PTR, (int *)&pTransid, NULL, 0, NULL, 0);
    if (isERROR(retcode)) return SQLCLI_ReturnCode(&currContext, retcode);

    if (!pTransid) return -CLI_INTERNAL_ERROR;

    if (currContext.boundsCheckMemory(pTransid, sizeof(long))) {
      return SQLCLI_ReturnCode(&currContext, -CLI_USER_MEMORY_IN_EXECUTOR_SEGMENT);
    } else {
      *pTransid = currContext.getTransaction()->getExeXnId();
    }
  }

  return SUCCESS;
}

#pragma page "SQLCLI_SetAuthID"
// *****************************************************************************
// *                                                                           *
// * Function: SQLCLI_SetAuthID                                                *
// *                                                                           *
// *    Sets user authentication values in CLI context.                        *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <cliGlobals>                    CliGlobals *                    In       *
// *    is a pointer to CLI globals.                                           *
// *                                                                           *
// *  <usersInfo>                     USERS_INFO &                    In       *
// *    is a description of user information needed by connection logic        *
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
int SQLCLI_SetAuthID(CliGlobals *cliGlobals, const USERS_INFO &usersInfo, /*IN*/
                     const char *authToken, int authTokenLen, const char *slaName, const char *profileName,
                     int resetAttributes) {
  // create initial context, if first call, and add module, if any.
  int retcode = CliPrologue(cliGlobals, 0);

  if (isERROR(retcode)) return retcode;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();
  // ACH can this be removed?  Seems to be an artifact of NSK priv boundary check
  if (currContext.boundsCheckMemory((void *)usersInfo.extUsername, str_len(usersInfo.extUsername)))
    return SQLCLI_ReturnCode(&currContext, -CLI_USER_MEMORY_IN_EXECUTOR_SEGMENT);

  // This method is a no-op on platforms other than Linux. Only on
  // Linux do we need to update the ContextCli instance.
  retcode = currContext.setAuthID(usersInfo, authToken, authTokenLen, slaName, profileName, resetAttributes);

  return SQLCLI_ReturnCode(&currContext, retcode);
}
//************************** End of SQLCLI_SetAuthID ***************************

/* temporary functions -- for use by sqlcat simulator only */

int SQLCLI_AllocDescInt(/*IN*/ CliGlobals *cliGlobals,
                        /*INOUT*/ SQLDESC_ID *desc_id,
                        /*IN*/ int max_entries) {
  int retcode;

  // create initial context, if first call, and add module, if any.
  retcode = CliPrologue(cliGlobals, desc_id->module);
  if (isERROR(retcode)) return retcode;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  if (max_entries < 0) {
    diags << DgSqlCode(-CLI_DATA_OUTOFRANGE) << DgInt0(max_entries);
    return diags.mainSQLCODE();
  }

  /* allocate a new descriptor in the current context.         */
  /* return the descriptor handle in desc_id, if name mode     */
  /* is "desc_handle"                                          */
  retcode = currContext.allocateDesc(desc_id, max_entries);

  return SQLCLI_ReturnCode(&currContext, retcode);
}

int SQLCLI_GetDescEntryCountInt(/*IN*/ CliGlobals *cliGlobals,
                                /*IN*/ SQLDESC_ID *desc_id,
                                /*OUT*/ int *num_entries) {
  if (!cliGlobals || !num_entries) {
    return (-CLI_NO_CURRENT_CONTEXT);
  }

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  Descriptor *desc = currContext.getDescriptor(desc_id);

  /* descriptor must exist */
  if (!desc) {
    diags << DgSqlCode(-CLI_DESC_NOT_EXISTS);
    return SQLCLI_ReturnCode(&currContext, -CLI_DESC_NOT_EXISTS);
  }

  if (currContext.boundsCheckMemory(num_entries, sizeof(num_entries)))
    return SQLCLI_ReturnCode(&currContext, -CLI_USER_MEMORY_IN_EXECUTOR_SEGMENT);

  *num_entries = desc->getUsedEntryCount();

  return SUCCESS;
}

int SQLCLI_GetDescItem(/*IN*/ CliGlobals *cliGlobals,
                       /*IN*/ SQLDESC_ID *desc_id,
                       /*IN*/ int entry,
                       /* (SQLDESC_ITEM_ID) *IN*/ int what_to_get,
                       /*OUT OPTIONAL*/ void *numeric_value,
                       /*OUT OPTIONAL*/ char *string_value,
                       /*IN  OPTIONAL*/ int max_string_len,
                       /*OUT OPTIONAL*/ int *len_of_item,
                       /*IN  OPTIONAL*/ int start_from_offset) {
  int retcode;

  // create initial context, if first call, and add module, if any.
  retcode = CliPrologue(cliGlobals, desc_id->module);
  if (isERROR(retcode)) return retcode;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  Descriptor *desc = currContext.getDescriptor(desc_id);

  /* descriptor must exist */
  if (!desc) {
    diags << DgSqlCode(-CLI_DESC_NOT_EXISTS);
    return SQLCLI_ReturnCode(&currContext, -CLI_DESC_NOT_EXISTS);
  }

  if (numeric_value) {
    if (currContext.boundsCheckMemory((void *)numeric_value, 4))
      return SQLCLI_ReturnCode(&currContext, -CLI_USER_MEMORY_IN_EXECUTOR_SEGMENT);
  }

  if (string_value) {
    if (currContext.boundsCheckMemory((void *)string_value, max_string_len))
      return SQLCLI_ReturnCode(&currContext, -CLI_USER_MEMORY_IN_EXECUTOR_SEGMENT);
  }
  retcode = desc->getDescItem(entry, what_to_get, numeric_value, string_value, max_string_len, len_of_item,
                              start_from_offset);

  return SQLCLI_ReturnCode(&currContext, retcode);
}

int SQLCLI_SetDescEntryCountInt(/*IN*/ CliGlobals *cliGlobals,
                                /*IN*/ SQLDESC_ID *desc_id,
                                /*IN*/ int num_entries) {
  int retcode;

  // create initial context, if first call, and add module, if any.
  retcode = CliPrologue(cliGlobals, desc_id->module);
  if (isERROR(retcode)) return retcode;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  Descriptor *desc = currContext.getDescriptor(desc_id);

  /* descriptor must exist */
  if (!desc) {
    diags << DgSqlCode(-CLI_DESC_NOT_EXISTS);
    return SQLCLI_ReturnCode(&currContext, -CLI_DESC_NOT_EXISTS);
  }

  desc->setUsedEntryCount(num_entries);

  return SUCCESS;
}

int SQLCLI_SetDescItem(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN*/ SQLDESC_ID *desc_id,
    /*IN*/ int entry,
    /* (SQLDESC_ITEM_ID) *IN*/ int what_to_set,
    /*IN  OPTIONAL*/ Long numeric_value,
    /*IN  OPTIONAL*/ char *string_value) {
  int retcode;

  // create initial context, if first call, and add module, if any.
  retcode = CliPrologue(cliGlobals, desc_id->module);
  if (isERROR(retcode)) return retcode;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  Descriptor *desc = currContext.getDescriptor(desc_id);

  /* descriptor must exist */
  if (!desc) {
    diags << DgSqlCode(-CLI_DESC_NOT_EXISTS);
    return SQLCLI_ReturnCode(&currContext, -CLI_DESC_NOT_EXISTS);
  }

  if (entry > desc->getMaxEntryCount()) {
    diags << DgSqlCode(-CLI_INVALID_DESC_ENTRY);
    return diags.mainSQLCODE();
  }

  if (entry > desc->getUsedEntryCount()) {
    retcode = desc->addEntry(entry);
    if (isERROR(retcode)) {
      diags << DgSqlCode(-CLI_INTERNAL_ERROR);
      return SQLCLI_ReturnCode(&currContext, -CLI_INTERNAL_ERROR);
    }
  }

  int length = 0;

  if (what_to_set == SQLDESC_VAR_PTR) {
    length = desc->getVarDataLength(entry);
    if (currContext.boundsCheckMemory((void *)numeric_value, length))
      return SQLCLI_ReturnCode(&currContext, -CLI_USER_MEMORY_IN_EXECUTOR_SEGMENT);
  }
  if (what_to_set == SQLDESC_IND_PTR) {
    length = desc->getIndLength(entry);
    if (currContext.boundsCheckMemory((void *)numeric_value, length))
      return SQLCLI_ReturnCode(&currContext, -CLI_USER_MEMORY_IN_EXECUTOR_SEGMENT);
  }

  if (string_value) {
    // get length from varchar
    length = desc->getVarDataLength(entry);
    if (currContext.boundsCheckMemory(string_value, length))
      return SQLCLI_ReturnCode(&currContext, -CLI_USER_MEMORY_IN_EXECUTOR_SEGMENT);
  }

  retcode = desc->setDescItem(entry, what_to_set, numeric_value, string_value);
  if (isERROR(retcode)) return SQLCLI_ReturnCode(&currContext, retcode);

  return SUCCESS;
}

// -----------------------------------------------------------------------
// NOTE: this procedure will go away, please use GetDiagnosticsStmtInfo()
// instead!!!
// -----------------------------------------------------------------------
int SQLCLI_GetRowsAffected(/*IN*/ CliGlobals *cliGlobals,
                           /*IN*/ SQLSTMT_ID *statement_id, long &rowsAffected) {
  if (!cliGlobals) return -CLI_NO_CURRENT_CONTEXT;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  Statement *stmt = currContext.getStatement(statement_id);

  if (!stmt) {
    diags << DgSqlCode(-CLI_STMT_NOT_EXISTS);
    return SQLCLI_ReturnCode(&currContext, -CLI_STMT_NOT_EXISTS);
  }

  if (currContext.boundsCheckMemory(&rowsAffected, sizeof(rowsAffected)))
    return SQLCLI_ReturnCode(&currContext, -CLI_USER_MEMORY_IN_EXECUTOR_SEGMENT);

  rowsAffected = stmt->getRowsAffected();

  return SUCCESS;
}
int SQLCLI_MergeDiagnostics(/*IN*/ CliGlobals *cliGlobals,
                            /*INOUT*/ ComDiagsArea &newDiags) {
  if (&newDiags == NULL) return ERROR;

  if (!cliGlobals) return -CLI_NO_CURRENT_CONTEXT;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  newDiags.mergeAfter(cliGlobals->currContext()->diags());
  return SUCCESS;
}

// For internal use only -- do not document!
static int GetStatement_Internal(/*IN*/ CliGlobals *cliGlobals,
                                 /*INOUT*/ Statement **statement_ptr,
                                 /*IN*/ SQLSTMT_ID *statement_id) {
  int retcode;
  Long stmtHandle = (Long)statement_id->handle;

  NABoolean getnext = ((((statement_id->name_mode != stmt_handle) && ((stmtHandle & 0x0001) != 0))) ? TRUE : FALSE);
  NABoolean position = (((stmtHandle & 0x0002) != 0) ? TRUE : FALSE);
  NABoolean advance = (((stmtHandle & 0x0004) != 0) ? TRUE : FALSE);

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  if (statement_id->name_mode != stmt_handle) {
    NABoolean sequentialAdd = (((stmtHandle & 0x0008) != 0) ? TRUE : FALSE);

    currContext.getStatementList()->setSequentialAdd(sequentialAdd);
  }

  Statement *stmt = NULL;

  if (getnext)
    stmt = currContext.getNextStatement(statement_id, position, advance);
  else
    stmt = currContext.getStatement(statement_id);

  /* stmt must exist */
  if (!stmt) {
    return SQL_EOF;
  }

  *statement_ptr = stmt;

  return SUCCESS;
}

// Function to get the length of the desc_items array
// returns the length if no error occurs, if error_occurred
// is 1 on return then return value indicates error
int GetDescItemsEntryCount(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN*/ SQLDESC_ID *desc_id,
    /*IN*/ SQLDESC_ITEM desc_items[],
    /*IN*/ SQLDESC_ID *value_num_descriptor,
    /*IN*/ SQLDESC_ID *descriptor,
    /*Out*/ int &error_occurred) {
  error_occurred = 0;
#if 0
  // sanity check
  if (!desc_id || !value_num_descriptor || !output_descriptor){
     error_occurred = 1;
     return -CLI_INTERNAL_ERROR;
  }
#endif

  int retcode;

  // create initial context, if first call, and add module, if any.
  retcode = CliPrologue(cliGlobals, desc_id->module);

  if (isERROR(retcode)) {
    error_occurred = 1;
    return -CLI_INTERNAL_ERROR;
  }

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  Descriptor *desc = currContext.getDescriptor(descriptor);

  /* descriptor must exist */
  if (!desc) {
    error_occurred = 1;
    diags << DgSqlCode(-CLI_DESC_NOT_EXISTS);
    return SQLCLI_ReturnCode(&currContext, -CLI_DESC_NOT_EXISTS);
  }

  int UsedEntryCount = desc->getUsedEntryCount();
  return UsedEntryCount;
}

// For internal use only -- do not document!
int SQLCLI_GetRootTdb_Internal(/*IN*/ CliGlobals *cliGlobals,
                               /*INOUT*/ char *roottdb_ptr,
                               /*IN*/ int roottdb_len,
                               /*INOUT*/ char *srcstr_ptr,
                               /*IN*/ int srcstr_len,
                               /*IN*/ SQLSTMT_ID *statement_id) {
  int retcode;

  Statement *statement;

  retcode = GetStatement_Internal(cliGlobals, &statement, statement_id);

  if (retcode) return retcode;

  if (!statement)  // statement must exists
    return -1;

  ex_root_tdb *exRootTdb = statement->getRootTdb();
  int rootTdbSize = statement->getRootTdbSize();
  ex_assert(rootTdbSize <= roottdb_len, "Insufficient space for root TDB");
  memcpy(roottdb_ptr, exRootTdb, rootTdbSize);

  if (srcstr_ptr) {
    char *srcStr = statement->getSrcStr();
    int srcStrSize = statement->getSrcStrSize();
    ex_assert(srcStrSize <= srcstr_len, "Insufficient space for source");
    memcpy(srcstr_ptr, srcStr, srcStrSize);
  }

  return retcode;
}

// For internal use only -- do not document!
int SQLCLI_GetRootTdbSize_Internal(/*IN*/ CliGlobals *cliGlobals,
                                   /*INOUT*/ int *roottdb_size,
                                   /*INOUT*/ int *srcstr_size,
                                   /*IN*/ SQLSTMT_ID *statement_id) {
  int retcode;
  Statement *statement;

  retcode = GetStatement_Internal(cliGlobals, &statement, statement_id);

  if (retcode) return retcode;

  if (!statement)  // statement must exists
    return -1;

  *roottdb_size = statement->getRootTdbSize();
  *srcstr_size = statement->getSrcStrSize();

  return retcode;
}

// For internal use only -- do not document!
int SQLCLI_GetCollectStatsType_Internal(/*IN*/ CliGlobals *cliGlobals,
                                        /*OUT*/ int *collectStatsType,
                                        /*IN*/ SQLSTMT_ID *statement_id) {
  int retcode = 0;
  Statement *statement;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  if (statement_id) {
    statement = currContext.getStatement(statement_id);

    if ((!statement) || (!statement->getRootTdb()))
      *collectStatsType = (int)ComTdb::NO_STATS;
    else
      *collectStatsType = (int)((ComTdb *)statement->getRootTdb())->getCollectStatsType();
  } else {
    if (currContext.getStats())
      *collectStatsType = (int)currContext.getStats()->getCollectStatsType();
    else
      *collectStatsType = (int)ComTdb::NO_STATS;
  }

  return retcode;
}

// For internal use only -- do not document!
int SQLCLI_GetTotalTcbSpace(/*IN*/ CliGlobals *cliGlobals,
                            /*IN*/ char *tdb,
                            /*IN*/ char *otherInfo) {
  int retcode;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  retcode = getTotalTcbSpace(tdb, otherInfo, (char *)cliGlobals->getExecutorMemory());

  return retcode;
}

int SQLCLI_SetParserFlagsForExSqlComp_Internal(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN*/ int flagbits) {
  if (cliGlobals->currContext()) cliGlobals->currContext()->setSqlParserFlags(flagbits);
  return 0;
}

int SQLCLI_ResetParserFlagsForExSqlComp_Internal(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN*/ int flagbits) {
  if (cliGlobals->currContext()) cliGlobals->currContext()->resetSqlParserFlags(flagbits);
  return 0;
}

int SQLCLI_AssignParserFlagsForExSqlComp_Internal(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN*/ int flagbits) {
  if (cliGlobals->currContext()) cliGlobals->currContext()->assignSqlParserFlags(flagbits);
  return 0;
}

int SQLCLI_GetParserFlagsForExSqlComp_Internal(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN*/ int &flagbits) {
  flagbits = 0;
  if (cliGlobals->currContext()) flagbits = cliGlobals->currContext()->getSqlParserFlags();
  return 0;
}

int SQLCLI_OutputValueIntoNumericHostvar(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN*/ SQLDESC_ID *output_descriptor,
    /*IN*/ int desc_entry,
    /*IN*/ int value) {
  // go ahead...
  // create initial context, if first call, and add module, if any.
  int retcode = CliPrologue(cliGlobals, output_descriptor->module);
  if (isERROR(retcode)) return retcode;
  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  Descriptor *output_desc = currContext.getDescriptor(output_descriptor);

  if (!output_desc) {
    return -CLI_DESC_NOT_EXISTS;
  }

  retcode = OutputValueIntoNumericHostvar(output_desc, desc_entry, value);

  return retcode;
}

int SQLCLI_SetEnviron_Internal(/*IN*/ CliGlobals *cliGlobals, char **envvars, int propagate) {
  int retcode = 0;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  retcode = cliGlobals->setEnvVars(envvars);
  if (retcode) return SQLCLI_ReturnCode(&currContext, retcode);

  return 0;
}

int SQLCLI_SetErrorCodeInRTS(CliGlobals *cliGlobals, SQLSTMT_ID *statement_id, int sqlErrorCode) {
  int retcode = 0;

  // create initial context, if first call, and add module, if any.
  retcode = CliPrologue(cliGlobals, statement_id->module);
  if (isERROR(retcode)) return retcode;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();
  /* prepare the statement */
  Statement *stmt = currContext.getStatement(statement_id);

  /* stmt must exist */
  if (stmt) {
    StmtStats *stmtStats = stmt->getStmtStats();
    if (stmtStats != NULL && stmtStats->getMasterStats() != NULL) {
      if (diags.mainSQLCODE() < 0) {
        long exeEndTime = NA_JulianTimestamp();
        stmtStats->getMasterStats()->setExeEndTime(exeEndTime);
        stmtStats->getMasterStats()->setElapsedEndTime(exeEndTime);
      }
      stmtStats->getMasterStats()->setSqlErrorCode(diags.mainSQLCODE() != 0 ? diags.mainSQLCODE() : sqlErrorCode);
    }
  } else
    retcode = -CLI_STMT_NOT_EXISTS;
  return retcode;
}

int SQLCLI_LocaleToUTF8(CliGlobals *cliGlobals, int conv_charset, void *Input_Buffer_Addr, int Input_Buffer_Length,
                        void *Output_Buffer_Addr, int Output_Buffer_Length, void **First_Untranslated_Char_Addr,
                        int *Output_Data_Length, int add_null_at_end_Flag, int *num_translated_char) {
  short error = 0;

  ContextCli &currContext = *(cliGlobals->currContext());
  char *&firstUntranslatedChar = (char *&)First_Untranslated_Char_Addr;

  // Bounds check the input buffer
  if (currContext.boundsCheckMemory((void *)Input_Buffer_Addr, Input_Buffer_Length)) return FEBOUNDSERR;

  // Bounds check the output buffer
  if (currContext.boundsCheckMemory((void *)Output_Buffer_Addr, Output_Buffer_Length)) return FEBOUNDSERR;

  error = LocaleToUTF8(cnv_version1, (char *)Input_Buffer_Addr, Input_Buffer_Length, (char *)Output_Buffer_Addr,
                       Output_Buffer_Length, (cnv_charset)conv_charset, firstUntranslatedChar,
                       (UInt32 *)Output_Data_Length, add_null_at_end_Flag, (UInt32 *)num_translated_char);
  return error;
}

int SQLCLI_LocaleToUTF16(CliGlobals *cliGlobals, int conv_charset, void *Input_Buffer_Addr, int Input_Buffer_Length,
                         void *Output_Buffer_Addr, int Output_Buffer_Length, void **First_Untranslated_Char_Addr,
                         int *Output_Data_Length, int conv_flags, int add_null_at_end_Flag, int *num_translated_char) {
  short error = 0;

  ContextCli &currContext = *(cliGlobals->currContext());
  char *&firstUntranslatedChar = (char *&)First_Untranslated_Char_Addr;

  // Bounds check the input buffer
  if (currContext.boundsCheckMemory((void *)Input_Buffer_Addr, Input_Buffer_Length)) return FEBOUNDSERR;

  // Bounds check the output buffer
  if (currContext.boundsCheckMemory((void *)Output_Buffer_Addr, Output_Buffer_Length)) return FEBOUNDSERR;

  error = LocaleToUTF16(cnv_version1, (char *)Input_Buffer_Addr, Input_Buffer_Length, (char *)Output_Buffer_Addr,
                        Output_Buffer_Length, (cnv_charset)conv_charset, firstUntranslatedChar,
                        (UInt32 *)Output_Data_Length, conv_flags, add_null_at_end_Flag, (UInt32 *)num_translated_char);

  return error;
}

int SQLCLI_UTF8ToLocale(CliGlobals *cliGlobals, int conv_charset, void *Input_Buffer_Addr, int Input_Buffer_Length,
                        void *Output_Buffer_Addr, int Output_Buffer_Length, void **First_Untranslated_Char_Addr,
                        int *Output_Data_Length, int add_null_at_end_Flag, int allow_invalids, int *num_translated_char,
                        void *substitution_char_addr) {
  short error = 0;

  ContextCli &currContext = *(cliGlobals->currContext());
  char *&firstUntranslatedChar = (char *&)First_Untranslated_Char_Addr;
  const char *substitutionChar = (const char *)substitution_char_addr;

  // Bounds check the input buffer
  if (currContext.boundsCheckMemory((void *)Input_Buffer_Addr, Input_Buffer_Length)) return FEBOUNDSERR;

  // Bounds check the output buffer
  if (currContext.boundsCheckMemory((void *)Output_Buffer_Addr, Output_Buffer_Length)) return FEBOUNDSERR;

  error =
      UTF8ToLocale(cnv_version1, (char *)Input_Buffer_Addr, Input_Buffer_Length, (char *)Output_Buffer_Addr,
                   Output_Buffer_Length, (cnv_charset)conv_charset, firstUntranslatedChar, (UInt32 *)Output_Data_Length,
                   add_null_at_end_Flag, allow_invalids, (UInt32 *)num_translated_char, substitutionChar);

  return error;
}

int SQLCLI_UTF16ToLocale(CliGlobals *cliGlobals, int conv_charset, void *Input_Buffer_Addr, int Input_Buffer_Length,
                         void *Output_Buffer_Addr, int Output_Buffer_Length, void **First_Untranslated_Char_Addr,
                         int *Output_Data_Length, int conv_flags, int add_null_at_end_Flag, int allow_invalids,
                         int *num_translated_char, void *substitution_char_addr) {
  short error = 0;

  ContextCli &currContext = *(cliGlobals->currContext());
  char *&firstUntranslatedChar = (char *&)First_Untranslated_Char_Addr;
  const char *substitutionChar = (const char *)substitution_char_addr;

  // Bounds check the input buffer
  if (currContext.boundsCheckMemory((void *)Input_Buffer_Addr, Input_Buffer_Length)) return FEBOUNDSERR;

  // Bounds check the output buffer
  if (currContext.boundsCheckMemory((void *)Output_Buffer_Addr, Output_Buffer_Length)) return FEBOUNDSERR;

  error = UTF16ToLocale(cnv_version1, (char *)Input_Buffer_Addr, Input_Buffer_Length, (char *)Output_Buffer_Addr,
                        Output_Buffer_Length, (cnv_charset)conv_charset, firstUntranslatedChar,
                        (UInt32 *)Output_Data_Length, conv_flags, add_null_at_end_Flag, allow_invalids,
                        (UInt32 *)num_translated_char, substitutionChar);

  return error;
}

int SQLCLI_SetSecInvalidKeys(CliGlobals *cliGlobals,
                             /* IN */ int numSiKeys,
                             /* IN */ SQL_QIKEY siKeys[]) {
  return cliGlobals->currContext()->setSecInvalidKeys(numSiKeys, siKeys);
}

int SQLCLI_GetSecInvalidKeys(CliGlobals *cliGlobals,
                             /* IN */ long prevTimestamp,
                             /* IN/OUT */ SQL_QIKEY siKeys[],
                             /* IN */ int maxNumSiKeys,
                             /* IN/OUT */ int *returnedNumSiKeys,
                             /* IN/OUT */ long *maxTimestamp) {
  int retcode = 0;
  *returnedNumSiKeys = 0;

  StatsGlobals *statsGlobals = cliGlobals->getStatsGlobals();
  if (statsGlobals) {
    long newestSik = statsGlobals->getNewestRevokeTimestamp();
    long newMaxTimestamp = NA_JulianTimestamp();

    if (prevTimestamp < newestSik)
      retcode = cliGlobals->getStatsGlobals()->getSecInvalidKeys(cliGlobals, prevTimestamp, siKeys, maxNumSiKeys,
                                                                 returnedNumSiKeys);
    if (retcode == 0) *maxTimestamp = newMaxTimestamp;
  } else {
    // This could happens as the instance starts -- some compilers could
    // call this CLI before the RMS shared segment is ready.  It
    // definitely happens when the init_sql createsysmdf step is
    // executed, because at that time the compiler is compiling
    // without having the instance up.  This is too early for a
    // revoke, so we handle the scenario by returning success, but
    // leave the maxTimestamp to the same value as the prevTimestamp,
    // so that the compiler will continue asking for SQL_QIKEYs each
    // time it compiles a query, until the RMS shared segment is
    // ready.
    *maxTimestamp = prevTimestamp;
  }

  return retcode;
}

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
                               /* OUT */ UInt32 *maxExpectedFlagsFound) {
  return cliGlobals->currContext()->setObjectEpochEntry(operation, objectNameLength, objectName, redefTime, key,
                                                        expectedEpoch, expectedFlags, newEpoch, newFlags,
                                                        maxExpectedEpochFound, maxExpectedFlagsFound);
}

int SQLCLI_LockObjectDDL(CliGlobals *cliGlobals,
                         /* IN */ const char *objectName,
                         /* IN */ short objectType) {
  return cliGlobals->currContext()->lockObjectDDL(objectName, (ComObjectType)objectType);
}

int SQLCLI_UnLockObjectDDL(CliGlobals *cliGlobals,
                           /* IN */ const char *objectName,
                           /* IN */ short objectType) {
  return cliGlobals->currContext()->unlockObjectDDL(objectName, (ComObjectType)objectType);
}

int SQLCLI_LockObjectDML(CliGlobals *cliGlobals,
                         /* IN */ const char *objectName,
                         /* IN */ short objectType) {
  return cliGlobals->currContext()->lockObjectDML(objectName, (ComObjectType)objectType);
}

int SQLCLI_ReleaseAllDDLObjectLocks(CliGlobals *cliGlobals) {
  return cliGlobals->currContext()->releaseAllDDLObjectLocks();
}

int SQLCLI_ReleaseAllDMLObjectLocks(CliGlobals *cliGlobals) {
  return cliGlobals->currContext()->releaseAllDMLObjectLocks();
}

int SQLCLI_CheckLobLock(CliGlobals *cliGlobals,
                        /* IN */ char *lobLockId,
                        /*OUT */ NABoolean *found) {
  int retcode = 0;
  retcode = cliGlobals->currContext()->checkLobLock(lobLockId, found);
  return retcode;
}
int SQLCLI_GetStatistics2(CliGlobals *cliGlobals,
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

  ContextCli *currContext = cliGlobals->currContext();
  retcode = currContext->GetStatistics2(statsReqType, statsReqStr, statsReqStrLen, activeQueryNum, statsMergeType,
                                        statsCollectType, sqlstats_desc, max_stats_desc, no_returned_stats_desc);

  return retcode;
}

int SQLCLI_GetStatisticsItems(CliGlobals *cliGlobals,
                              /* IN */ short statsReqType,
                              /* IN */ char *queryId,
                              /* IN */ int queryIdLen,
                              /* IN */ int no_of_stats_items,
                              /* IN/OUT */ SQLSTATS_ITEM sqlStats_items[]) {
  int retcode;

  ContextCli *currContext = cliGlobals->currContext();
  ComDiagsArea &diags = currContext->diags();

  ExStatisticsArea *mergedStats = currContext->getMergedStats();
  ExMasterStats *masterStats;
  if (mergedStats == NULL) {
    diags << DgSqlCode(-CLI_MERGED_STATS_NOT_AVAILABLE);
    return SQLCLI_ReturnCode(currContext, -CLI_MERGED_STATS_NOT_AVAILABLE);
  }
  if (statsReqType == SQLCLI_STATS_REQ_QID) {
    masterStats = mergedStats->getMasterStats();
    if (masterStats != NULL && queryId != NULL) {
      if (queryIdLen != masterStats->getQueryIdLen() ||
          str_cmp(masterStats->getQueryId(), queryId, masterStats->getQueryIdLen()) != 0) {
        diags << DgSqlCode(-CLI_QID_NOT_MATCHING);
        return SQLCLI_ReturnCode(currContext, -CLI_QID_NOT_MATCHING);
      }
    }
  }
  retcode = mergedStats->getStatsItems(no_of_stats_items, sqlStats_items);
  return retcode;
}
int SQLCLI_RegisterQuery(CliGlobals *cliGlobals, SQLQUERY_ID *queryId, int fragId, int tdbId, int explainTdbId,
                         short collectStatsType, int instNum, int tdbType, char *tdbName, int tdbNameLen) {
  ContextCli *currContext = cliGlobals->currContext();
  ComDiagsArea &diags = currContext->diags();
  int retcode = SUCCESS;
  StatsGlobals *statsGlobals = cliGlobals->getStatsGlobals();
  if (statsGlobals == NULL) return retcode;
  int error;
  error = statsGlobals->getStatsSemaphore(cliGlobals->getSemId(), cliGlobals->myPin());
  retcode = statsGlobals->registerQuery(diags, cliGlobals->myPin(), queryId, fragId, tdbId, explainTdbId,
                                        collectStatsType, instNum, (ComTdb::ex_node_type)tdbType, tdbName, tdbNameLen);
  statsGlobals->releaseStatsSemaphore(cliGlobals->getSemId(), cliGlobals->myPin());
  return retcode;
}

int SQLCLI_DeregisterQuery(CliGlobals *cliGlobals, SQLQUERY_ID *queryId, int fragId) {
  ContextCli *currContext = cliGlobals->currContext();
  ComDiagsArea &diags = currContext->diags();
  int retcode = SUCCESS;
  StatsGlobals *statsGlobals = cliGlobals->getStatsGlobals();
  if (statsGlobals == NULL) return retcode;
  int error;
  error = statsGlobals->getStatsSemaphore(cliGlobals->getSemId(), cliGlobals->myPin());
  retcode = statsGlobals->deregisterQuery(diags, cliGlobals->myPin(), queryId, fragId);
  statsGlobals->releaseStatsSemaphore(cliGlobals->getSemId(), cliGlobals->myPin());
  return retcode;
}

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
    /*INOUT*/ const ExStatisticsArea *&exStatsArea) {
  ContextCli *currContext = cliGlobals->currContext();
  ComDiagsArea &diagsArea = currContext->diags();
  int retcode = SUCCESS;

  short retryAttempts = 0;

  exStatsArea = currContext->getMergedStats(statsReqType, statsReqStr, statsReqStrLen, activeQueryNum, statsMergeType,
                                            retryAttempts);

  if (exStatsArea == NULL) {
    if (diagsArea.getNumber(DgSqlCode::ERROR_) > 0)
      return diagsArea.mainSQLCODE();
    else if (statsReqType == SQLCLI_STATS_REQ_QID) {
      (diagsArea) << DgSqlCode(-EXE_RTS_QID_NOT_FOUND) << DgString0(statsReqStr);
      return diagsArea.mainSQLCODE();
    } else {
      (diagsArea) << DgSqlCode(-CLI_MERGED_STATS_NOT_AVAILABLE);
      return -CLI_MERGED_STATS_NOT_AVAILABLE;
    }
  } else {
    currContext->setMergedStats((ExStatisticsArea *)exStatsArea);
    // temporary assert to see if I can catch this statsArea in
    // the shared segment.
    long statsAddr = (long)exStatsArea;
    if ((statsAddr > 0x10000000) && (statsAddr < 0x14000000))
      ex_assert(0, "ExStatisticsArea in shared seg, need a sema4.");
  }

  return retcode;
}

int SQLCLI_GetObjectEpochStats_Internal(
    /* IN */ CliGlobals *cliGlobals,
    /* IN */ const char *objectName,
    /* IN */ int objectNameLen,
    /* IN */ short cpu,
    /* IN */ bool locked,
    /*INOUT*/ ExStatisticsArea *&exStatsArea) {
  ContextCli *currContext = cliGlobals->currContext();
  ComDiagsArea &diagsArea = currContext->diags();
  int retcode = SUCCESS;

  short retryAttempts = 0;

  exStatsArea = currContext->getObjectEpochStats(objectName, objectNameLen, cpu, locked, retryAttempts);

  if (exStatsArea == NULL) {
    if (diagsArea.getNumber(DgSqlCode::ERROR_) > 0) return diagsArea.mainSQLCODE();

    (diagsArea) << DgSqlCode(-CLI_MERGED_STATS_NOT_AVAILABLE);
    return -CLI_MERGED_STATS_NOT_AVAILABLE;
  }

  return retcode;
}

int SQLCLI_GetObjectLockStats_Internal(
    /* IN */ CliGlobals *cliGlobals,
    /* IN */ const char *objectName,
    /* IN */ int objectNameLen,
    /* IN */ short cpu,
    /*INOUT*/ ExStatisticsArea *&exStatsArea) {
  ContextCli *currContext = cliGlobals->currContext();
  ComDiagsArea &diagsArea = currContext->diags();
  int retcode = SUCCESS;

  short retryAttempts = 0;

  exStatsArea = currContext->getObjectLockStats(objectName, objectNameLen, cpu, retryAttempts);

  if (exStatsArea == NULL) {
    if (diagsArea.getNumber(DgSqlCode::ERROR_) > 0) return diagsArea.mainSQLCODE();

    (diagsArea) << DgSqlCode(-CLI_MERGED_STATS_NOT_AVAILABLE);
    return -CLI_MERGED_STATS_NOT_AVAILABLE;
  }

  return retcode;
}

int SQLCLI_GetChildQueryInfo(CliGlobals *cliGlobals, SQLSTMT_ID *statement_id, char *uniqueQueryId,
                             int uniqueQueryIdMaxLen, int *uniqueQueryIdLen, SQL_QUERY_COST_INFO *query_cost_info,
                             SQL_QUERY_COMPILER_STATS_INFO *comp_stats_info) {
  int retcode = 0;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();
  /* prepare the statement */
  Statement *stmt = currContext.getStatement(statement_id);
  if (!stmt) {
    diags << DgSqlCode(-CLI_STMT_NOT_EXISTS);
    return SQLCLI_ReturnCode(&currContext, -CLI_STMT_NOT_EXISTS);
  }
  retcode = stmt->getChildQueryInfo(diags, uniqueQueryId, uniqueQueryIdMaxLen, uniqueQueryIdLen, query_cost_info,
                                    comp_stats_info);

  return retcode;
}

int SQLCLI_SWITCH_TO_COMPILER_TYPE(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN*/ int cmpClassType) {
  ContextCli *currContext = GetCliGlobals()->currContext();

  int retCode = currContext->switchToCmpContext(cmpClassType);

  if (retCode == 0) return retCode;  // success

  ComDiagsArea &diagsArea = currContext->diags();
  if (retCode == -1)
    diagsArea << DgSqlCode(2032) << DgString0(CmpContextInfo::getCmpContextClassName(cmpClassType));
  else
    diagsArea << DgSqlCode(2032) << DgString0("(invalid type)");  // give warning

  return retCode;
}

/*
  int SQLCLI_SWITCH_TO_COMPILER(CliGlobals * cliGlobals,
                                  void * cmpCntxt)
          - switch to given CmpContext instance
            if the given instance is not known, add it to the CmpContext
            list with class name "NONE" (default)
          - return value
              0 success
              -1 given instance is null
              1 given instance is in use but switched anyway
*/
int SQLCLI_SWITCH_TO_COMPILER(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN*/ void *cmpCntxt) {
  ContextCli *currContext = cliGlobals->currContext();

  int retCode = currContext->switchToCmpContext(cmpCntxt);

  if (retCode >= 0) return retCode;  // success

  ComDiagsArea &diagsArea = currContext->diags();
  diagsArea << DgSqlCode(2032) << DgString0("invalid compiler pointer");  // give warning

  return retCode;
}

/*
  int SQLCLI_SWITCH_BACK_COMPILER(CliGlobals *cliGlobals)
          - switch back to previous used CmpContext instance
          - return value
              0 success
              -1 no previous CmpContext to switch back
*/
int SQLCLI_SWITCH_BACK_COMPILER(
    /*IN*/ CliGlobals *cliGlobals) {
  ContextCli *currContext = cliGlobals->currContext();

  return (currContext->switchBackCmpContext());
}

int SQLCLI_SEcliInterface(CliGlobals *cliGlobals, SECliQueryType qType,

                          void **inCliInterface,

                          const char *inStrParam1, const char *inStrParam2, int inIntParam1, int inIntParam2,

                          char **outStrParam1, char **outStrParam2, int *outIntParam1) {
  int rc = 0;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  ExeCliInterface *cliInterface = NULL;
  if (inCliInterface && (*inCliInterface)) {
    cliInterface = (ExeCliInterface *)(*inCliInterface);
  } else {
    cliInterface =
        new (currContext.exHeap()) ExeCliInterface(currContext.exHeap(), SQLCHARSETCODE_UTF8, &currContext, NULL);

    cliInterface->setNotExeUtilInternalQuery(TRUE);

    if (inCliInterface) *inCliInterface = cliInterface;
  }

  switch (qType) {
    case SE_CLI_CREATE_CONTEXT: {
      rc = cliInterface->createContext((char *)inStrParam1);
    } break;

    case SE_CLI_SWITCH_CONTEXT: {
      rc = cliInterface->switchContext((char *)inStrParam1);
    } break;

    case SE_CLI_CURRENT_CONTEXT: {
      rc = cliInterface->currentContext((char *)inStrParam1);
    } break;

    case SE_CLI_DROP_CONTEXT: {
      rc = cliInterface->deleteContext((char *)inStrParam1);
    } break;

    case SE_CLI_CLEAR_DIAGS: {
      cliInterface->clearGlobalDiags();
      rc = 0;
    } break;

    case SE_CLI_EXEC_IMMED: {
      rc = cliInterface->executeImmediate((char *)inStrParam1);
    } break;

    case SE_CLI_EXEC_IMMED_PREP: {
      rc = cliInterface->executeImmediatePrepare((char *)inStrParam1, NULL, NULL, NULL, FALSE, (char *)inStrParam2);
    } break;

    case SE_CLI_EXEC_IMMED_CEFC: {
      rc = cliInterface->executeImmediateCEFC((char *)inStrParam1, NULL, 0, (char *)inStrParam2, NULL);
    } break;

    case SE_CLI_FETCH_ROWS_PROLOGUE: {
      rc = cliInterface->fetchRowsPrologue((char *)inStrParam1, FALSE, FALSE, (char *)inStrParam2);
    } break;

    case SE_CLI_EXEC: {
      rc = cliInterface->exec((char *)inStrParam1, inIntParam1);
    } break;

    case SE_CLI_FETCH: {
      rc = cliInterface->fetch();
    } break;

    case SE_CLI_CLOSE: {
      rc = cliInterface->close();
    } break;

    case SE_CLI_CEFC: {
      rc = cliInterface->clearExecFetchClose((char *)inStrParam1, inIntParam1);
    } break;

    case SE_CLI_BEGIN_XN: {
      rc = cliInterface->beginXn();
    } break;

    case SE_CLI_COMMIT_XN: {
      rc = cliInterface->commitXn();
    } break;

    case SE_CLI_ROLLBACK_XN: {
      rc = cliInterface->rollbackXn();
    } break;

    case SE_CLI_STATUS_XN: {
      rc = cliInterface->statusXn();
    } break;

    case SE_CLI_GET_DATA_OFFSETS: {
      rc = cliInterface->getDataOffsets(inIntParam1, inIntParam2, (int *)inStrParam1, (int *)inStrParam2);
    } break;

    case SE_CLI_GET_PTR_AND_LEN: {
      char *ptr = NULL;
      int len = 0;
      short *ind = NULL;
      rc = cliInterface->getPtrAndLen(inIntParam1, ptr, len, &ind);
      if (outStrParam1) *outStrParam1 = ptr;
      if (outStrParam2) *outStrParam2 = (char *)ind;
      if (outIntParam1) *outIntParam1 = len;
    } break;

    case SE_CLI_GET_IO_LEN: {
      if (inIntParam1 == 0) {
        *outIntParam1 = cliInterface->inputDatalen();
      }

      if (inIntParam1 == 1) {
        *outIntParam1 = cliInterface->outputDatalen();
      }

      rc = 0;
    } break;

    case SE_CLI_GET_STMT_ATTR: {
      rc = cliInterface->getStmtAttr((char *)inStrParam1, (int)inIntParam1, (int *)outIntParam1, *outStrParam1);
    } break;

    case SE_CLI_DEALLOC: {
      rc = cliInterface->dealloc();
    } break;

    case SE_CLI_TRAFQ_INSERT: {
      currContext.trafSElist()->insert(inStrParam1, inIntParam1, (void *)inStrParam2);
    } break;

    case SE_CLI_TRAFQ_GET: {
      currContext.trafSElist()->position(inStrParam1, inIntParam1);
      *outStrParam1 = (char *)currContext.trafSElist()->getNext();
    } break;

  }  // switch

  return rc;
}

///////////////////////////////////////////////////////////////////////
// Current design to update sequence metadata:
//
// If there is no Xn running:
//    A transaction is started, update is done within it, and then that Xn is committed.
//    If there is a conflict during commit, then update is retried.
//
// If there is a user Xn running:
//    Seq Gen table is not updated as part of user xn. It is run in non-transactional
//    mode using hbase transactions.
//    To avoid concurrent updates to seq gen table,
//    Sequence gen table col upd_ts is updated with a generated unique value.
//    Table is then updated with new nextValue and current value is retrieved.
//    At the end, upd_ts value is retrieved and compared to the generated unique
//    value. If they are different, update is retried.
//
//    seq gen update is retried max 10 times.
//    An error is returned if we dont get consistent result after that.
//
//
// Future design:
//
//    When support for local transactions and repeatable reads are in,
//    then the select...update will be done in one local transaction.
//    That will guarantee that the select and update are done in the same transaction.
//
//    If a transaction is already running when this update is to be done, then that Xn
//    will be suspended and a new local transaction wil be started.
//    This local Xn will be committed after update and the
//    original transaction will be resumed(joined back).
//
////////////////////////////////////////////////////////////////////////////

#define SEQ_GEN_NUM_PREPS      6
#define SEQ_UPD_TS_QRY_IDX     0
#define SEQ_SEL_TS_QRY_IDX     1
#define SEQ_PROCESS_QRY_IDX    2
#define SEQ_CQD_IDX            3
#define SEQ_PROCESS_QRY_IDX_DL 4
#define SEQ_UPSERT_XDC_SEQ_IDX 5

static int SeqGenCliInterfacePrepQry(const char *qryStr, int qryIdx, const char *qryName,
                                     ExeCliInterface **cliInterfaceArr, SequenceGeneratorAttributes *sga,
                                     ContextCli &currContext, ComDiagsArea &diags, NAHeap *exHeap) {
  int cliRC = 0;

  char query[2000];
  char stmtName[200];

  long rowsAffected = 0;

  ComDiagsArea *myDiags = NULL;

  ExeCliInterface *cliInterface = NULL;
  ExeCliInterface *cqdCliInterface = NULL;

  NABoolean doPrep = FALSE;
  if (!cliInterfaceArr[qryIdx]) {
    if (qryIdx != SEQ_UPSERT_XDC_SEQ_IDX)
      str_sprintf(query, qryStr, TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_SEQ_GEN,
                  sga->getSGObjectUID().get_value());
    else
      str_sprintf(query, qryStr, TRAFODION_SYSCAT_LIT, SEABASE_XDC_MD_SCHEMA, XDC_SEQUENCE_TABLE, TRAFODION_SYSCAT_LIT,
                  SEABASE_MD_SCHEMA, SEABASE_OBJECTS, sga->getSGObjectUID().get_value());

    cliInterfaceArr[qryIdx] =
        new (currContext.exHeap()) ExeCliInterface(currContext.exHeap(), SQLCHARSETCODE_UTF8, &currContext, NULL);

    if (!cliInterfaceArr[SEQ_CQD_IDX])
      cliInterfaceArr[SEQ_CQD_IDX] =
          new (currContext.exHeap()) ExeCliInterface(currContext.exHeap(), SQLCHARSETCODE_UTF8, &currContext, NULL);

    str_sprintf(stmtName, "%s_%ld", qryName, sga->getSGObjectUID().get_value());

    doPrep = TRUE;
  }

  cliInterface = cliInterfaceArr[qryIdx];
  cqdCliInterface = cliInterfaceArr[SEQ_CQD_IDX];

  if (doPrep) {
    if (qryIdx != SEQ_UPSERT_XDC_SEQ_IDX) {
      if (sga->getSGUseDtmImpl())
        cliRC = cqdCliInterface->holdAndSetCQDs("limit_max_numeric_precision, traf_md_replication",
                                                "limit_max_numeric_precision 'ON', traf_md_replication 'ON'");
      else
        cliRC = cqdCliInterface->holdAndSetCQDs("limit_max_numeric_precision, traf_no_dtm_xn",
                                                "limit_max_numeric_precision 'ON', traf_no_dtm_xn 'ON'");
      if (cliRC < 0) {
        if (myDiags == NULL) myDiags = ComDiagsArea::allocate(exHeap);
        cqdCliInterface->retrieveSQLDiagnostics(myDiags);
        diags.mergeAfter(*myDiags);
        myDiags->decrRefCount();
        return cliRC;
      }
    }

    cliRC = cliInterface->executeImmediatePrepare(query, NULL, 0, &rowsAffected, FALSE, stmtName);
    if (cliRC < 0) {
      if (myDiags == NULL) myDiags = ComDiagsArea::allocate(exHeap);
      cliInterface->retrieveSQLDiagnostics(myDiags);
      diags.mergeAfter(*myDiags);
      myDiags->decrRefCount();
      cqdCliInterface->restoreCQDs("limit_max_numeric_precision, traf_no_dtm_xn");
      return cliRC;
    }
    if (qryIdx != SEQ_UPSERT_XDC_SEQ_IDX) {
      if (sga->getSGUseDtmImpl())
        cqdCliInterface->restoreCQDs("limit_max_numeric_precision, traf_md_replication");
      else
        cqdCliInterface->restoreCQDs("limit_max_numeric_precision, traf_no_dtm_xn");
    }
  }

  return 0;
}

static int SeqGenCliInterfaceProcessXDC(ExeCliInterface **cliInterfaceArr, SequenceGeneratorAttributes *sga,
                                        ContextCli &currContext, ComDiagsArea &diags, NAHeap *exHeap,
                                        NABoolean doPrepare, NABoolean doExecute, long nextValue = 0 /*input*/) {
  if (!(sga->getSGAsyncRepl())) return 0;
  int cliRC = 0;
  if (doPrepare) {
    if (!cliInterfaceArr[SEQ_UPSERT_XDC_SEQ_IDX]) {
      cliRC = SeqGenCliInterfacePrepQry(
          "upsert into %s.\"%s\".%s select CATALOG_NAME, SCHEMA_NAME, OBJECT_NAME, cast(? as largeint not null) from "
          "%s.\"%s\".%s O where O.object_uid = %ld;",
          SEQ_UPSERT_XDC_SEQ_IDX, "SEQ_UPSERT_XDC_SEQ_IDX", cliInterfaceArr, sga, currContext, diags, exHeap);
      if (cliRC < 0) return cliRC;
    }
  }
  if (doExecute) {
    assert(cliInterfaceArr[SEQ_UPSERT_XDC_SEQ_IDX] != NULL);
    ExeCliInterface *cliInterface = cliInterfaceArr[SEQ_UPSERT_XDC_SEQ_IDX];
    ComDiagsArea *myDiags = NULL;

    long inputValues[1];
    int inputValuesLen = 0;

    inputValues[0] = nextValue;
    inputValuesLen = sizeof(nextValue);
    cliRC = cliInterface->clearExecFetchCloseOpt((char *)inputValues, inputValuesLen, NULL, NULL, NULL);
    if (cliRC < 0) {
      if (myDiags == NULL) myDiags = ComDiagsArea::allocate(exHeap);
      cliInterface->retrieveSQLDiagnostics(myDiags);
      diags.mergeAfter(*myDiags);
      myDiags->decrRefCount();
    }
  }
  return cliRC;
}

static int SeqGenCliInterfaceUpdAndValidate(ExeCliInterface **cliInterfaceArr, SequenceGeneratorAttributes *sga,
                                            NABoolean recycleQry, ContextCli &currContext, ComDiagsArea &diags,
                                            NAHeap *exHeap, long &nextValue, long &endValue) {
  int cliRC;

  long outputValues[5];
  long inputValues[5];

  int outputValuesLen = 0;
  int inputValuesLen = 0;

  long rowsAffected = 0;

  ExeCliInterface *cliInterface = NULL;

  ComDiagsArea *myDiags = NULL;

  char queryBuf[2000];

  long updUID = 0;

  long transID = getTransactionIDFromContext();

  if (!cliInterfaceArr[SEQ_UPD_TS_QRY_IDX]) {
    cliRC = SeqGenCliInterfacePrepQry(
        "update %s.\"%s\".%s set upd_ts = cast(? as largeint not null) where seq_uid = %ld", SEQ_UPD_TS_QRY_IDX,
        "SEQ_UPD_TS_QRY_IDX", cliInterfaceArr, sga, currContext, diags, exHeap);
    if (cliRC < 0) return cliRC;
  }

  cliInterface = cliInterfaceArr[SEQ_UPD_TS_QRY_IDX];

  // generated a unique id and update it. If this value changes later, then
  // this operation will be retried.
  ComUID comUID;
  comUID.make_UID();
  updUID = comUID.get_value();
  inputValues[0] = updUID;
  inputValuesLen = sizeof(updUID);
  cliRC = cliInterface->clearExecFetchCloseOpt((char *)inputValues, inputValuesLen, NULL, NULL, &rowsAffected);
  if (cliRC < 0) {
    if (myDiags == NULL) myDiags = ComDiagsArea::allocate(exHeap);
    cliInterface->retrieveSQLDiagnostics(myDiags);
    diags.mergeAfter(*myDiags);
    myDiags->decrRefCount();
    return cliRC;
  }

  // if not found, aqr
  if (rowsAffected == 0) {
    ComDiagsArea *da = &diags;
    NAString uid = Int64ToNAString(sga->getSGObjectUID().get_value());
    ExRaiseSqlError(exHeap, &da, (ExeErrorCode)(-1582), NULL, NULL, NULL, uid.data());

    return -1582;
  }

  if (!cliInterfaceArr[SEQ_PROCESS_QRY_IDX]) {
    cliRC = SeqGenCliInterfacePrepQry(
        "select  case when cast(? as largeint not null) = 1 then t.minVal else t.nextValue end, t.redefTS from (update "
        "%s.\"%s\".%s set next_value = (case when cast(? as largeint not null) = 1 then min_value + cast(? as largeint "
        "not null) else (case when next_value + cast(? as largeint not null) > max_value then max_value+1 else "
        "next_value + cast(? as largeint not null) end) end), num_calls = num_calls + 1 where seq_uid = %ld return "
        "\"OLD\".min_value, \"OLD\".next_value, \"OLD\".redef_ts) t(minVal, nextValue, redefTS);",
        SEQ_PROCESS_QRY_IDX, "SEQ_PROCESS_QRY_IDX", cliInterfaceArr, sga, currContext, diags, exHeap);
    if (cliRC < 0) return cliRC;
  }

  cliInterface = cliInterfaceArr[SEQ_PROCESS_QRY_IDX];

  // execute using optimized path
  long delta = sga->getSGIncrement() * (sga->getSGCache() > 0 ? sga->getSGCache() : 1);
  inputValues[0] = (recycleQry ? 1 : 0);
  inputValues[1] = (recycleQry ? 1 : 0);
  inputValues[2] = delta;
  inputValues[3] = delta;
  inputValues[4] = delta;
  inputValuesLen = 5 * sizeof(long);

  cliRC = cliInterface->clearExecFetchCloseOpt((char *)inputValues, inputValuesLen, (char *)outputValues,
                                               &outputValuesLen, &rowsAffected);
  if (cliRC < 0) {
    if (myDiags == NULL) myDiags = ComDiagsArea::allocate(exHeap);
    cliInterface->retrieveSQLDiagnostics(myDiags);
    diags.mergeAfter(*myDiags);
    myDiags->decrRefCount();
    if (diags.mainSQLCODE() == -EXE_NUMERIC_OVERFLOW) {
      cliRC = -EXE_SG_MAXVALUE_EXCEEDED;
    }

    return cliRC;
  }

  // must find this uid in seq generator. if not found, aqr.
  if (rowsAffected == 0) {
    ComDiagsArea *da = &diags;
    NAString uid = Int64ToNAString(sga->getSGObjectUID().get_value());
    ExRaiseSqlError(exHeap, &da, (ExeErrorCode)(-1582), NULL, NULL, NULL, uid.data());

    return -1582;
  }

  long startValue = outputValues[0];
  long redefTS = outputValues[1];

  // if timestamp mismatch, aqr
  if (redefTS != sga->getSGRedefTime()) {
    ComDiagsArea *da = &diags;
    ExRaiseSqlError(exHeap, &da, (ExeErrorCode)(-1584), NULL, NULL, NULL, Int64ToNAString(redefTS).data(),
                    Int64ToNAString(sga->getSGRedefTime()).data());

    return -1584;
  }

  // this query retrieves upd_ts after this seqgen has been updated.
  // it is used to validate that it didn't get updated by someone else
  // after my update.
  if (!cliInterfaceArr[SEQ_SEL_TS_QRY_IDX]) {
    cliRC = SeqGenCliInterfacePrepQry("select upd_ts from %s.\"%s\".%s where seq_uid = %ld", SEQ_SEL_TS_QRY_IDX,
                                      "SEQ_SEL_TS_QRY_IDX", cliInterfaceArr, sga, currContext, diags, exHeap);
    if (cliRC < 0) return cliRC;
  }

  cliInterface = cliInterfaceArr[SEQ_SEL_TS_QRY_IDX];
  cliRC = cliInterface->clearExecFetchCloseOpt(NULL, 0, (char *)outputValues, &outputValuesLen, &rowsAffected);
  if (cliRC < 0) {
    if (myDiags == NULL) myDiags = ComDiagsArea::allocate(exHeap);
    cliInterface->retrieveSQLDiagnostics(myDiags);
    diags.mergeAfter(*myDiags);
    myDiags->decrRefCount();
    return cliRC;
  }

  // must find this uid in seq generator. if not found, is an error.
  if (rowsAffected == 0) {
    ComDiagsArea *da = &diags;
    NAString uid = Int64ToNAString(sga->getSGObjectUID().get_value());
    ExRaiseSqlError(exHeap, &da, (ExeErrorCode)(-1582), NULL, NULL, NULL, uid.data());

    return -1582;
  }

  // if update time different than my upd time, retry.
  if (outputValues[0] != updUID) {
    return -FAIL_TO_ACQUIRE_LOCK;
  }

  nextValue = startValue;
  endValue = nextValue + sga->getSGIncrement() * (sga->getSGCache() - 1);

  if (endValue > sga->getSGMaxValue()) endValue = sga->getSGMaxValue();

  return 0;
}

static int SeqGenCliInterfaceUpdAndValidateUseDlock(ExeCliInterface **cliInterfaceArr, SequenceGeneratorAttributes *sga,
                                                    NABoolean recycleQry, ContextCli &currContext, ComDiagsArea &diags,
                                                    NAHeap *exHeap, long &nextValue, long &endValue) {
  int cliRC;

  long outputValues[5];
  long inputValues[6];

  int outputValuesLen = 0;
  int inputValuesLen = 0;

  long rowsAffected = 0;

  ExeCliInterface *cliInterface = NULL;

  ComDiagsArea *myDiags = NULL;

  char queryBuf[2000];

  long updUID = 0;

  if (!cliInterfaceArr[SEQ_PROCESS_QRY_IDX_DL]) {
    cliRC = SeqGenCliInterfacePrepQry(
        "select  case when cast(? as largeint not null) = 1 then t.minVal else t.nextValue end, t.redefTS from (update "
        "%s.\"%s\".%s set next_value = (case when cast(? as largeint not null) = 1 then min_value + cast(? as largeint "
        "not null) else (case when next_value + cast(? as largeint not null) > max_value then max_value+1 else "
        "next_value + cast(? as largeint not null) end) end), num_calls = num_calls + 1, upd_ts = cast(? as largeint "
        "not null) where seq_uid = %ld return \"OLD\".min_value, \"OLD\".next_value, \"OLD\".redef_ts) t(minVal, "
        "nextValue, redefTS);",
        SEQ_PROCESS_QRY_IDX_DL, "SEQ_PROCESS_QRY_IDX_DL", cliInterfaceArr, sga, currContext, diags, exHeap);
    if (cliRC < 0) return cliRC;
  }

  // prepare xdc statement
  cliRC = SeqGenCliInterfaceProcessXDC(cliInterfaceArr, sga, currContext, diags, exHeap, true /*doPrepare*/,
                                       false /*doExecute*/);
  if (cliRC < 0) return cliRC;
  cliInterface = cliInterfaceArr[SEQ_PROCESS_QRY_IDX_DL];

  // execute using optimized path
  long delta = sga->getSGIncrement() * (sga->getSGCache() > 0 ? sga->getSGCache() : 1);
  inputValues[0] = (recycleQry ? 1 : 0);
  inputValues[1] = (recycleQry ? 1 : 0);
  inputValues[2] = delta;
  inputValues[3] = delta;
  inputValues[4] = delta;
  inputValuesLen = 5 * sizeof(long);

  // generated a unique id as upd_ts
  ComUID comUID;
  comUID.make_UID();
  updUID = comUID.get_value();
  inputValues[5] = updUID;
  inputValuesLen += sizeof(updUID);

  DistributedLock_JNI *lock = DistributedLock_JNI::getInstance();
  CMPASSERT(lock != NULL);

  long uid = sga->getSGObjectUID().get_value();
  string lockName = "SeqUid" + to_string((long long)uid);
  if (lock->lock(lockName.data(), 0) != DL_OK)  // lockName depends on its uid
    return -FAIL_TO_ACQUIRE_LOCK;

  cliRC = cliInterface->clearExecFetchCloseOpt((char *)inputValues, inputValuesLen, (char *)outputValues,
                                               &outputValuesLen, &rowsAffected);
  if (cliRC < 0) {
    if (myDiags == NULL) myDiags = ComDiagsArea::allocate(exHeap);
    cliInterface->retrieveSQLDiagnostics(myDiags);
    diags.mergeAfter(*myDiags);
    myDiags->decrRefCount();
    if (diags.mainSQLCODE() == -EXE_NUMERIC_OVERFLOW) {
      cliRC = -EXE_SG_MAXVALUE_EXCEEDED;
    }
    lock->unlock();
    return cliRC;
  }

  // must find this uid in seq generator. if not found, aqr.
  if (rowsAffected == 0) {
    ComDiagsArea *da = &diags;
    NAString uid = Int64ToNAString(sga->getSGObjectUID().get_value());
    ExRaiseSqlError(exHeap, &da, (ExeErrorCode)(-1582), NULL, NULL, NULL, uid.data());

    lock->unlock();
    return -1582;
  }

  long startValue = outputValues[0];
  long redefTS = outputValues[1];

  // if timestamp mismatch, aqr
  if (redefTS != sga->getSGRedefTime()) {
    ComDiagsArea *da = &diags;
    ExRaiseSqlError(exHeap, &da, (ExeErrorCode)(-1584), NULL, NULL, NULL, Int64ToNAString(redefTS).data(),
                    Int64ToNAString(sga->getSGRedefTime()).data());

    lock->unlock();
    return -1584;
  }

  nextValue = startValue;
  endValue = nextValue + sga->getSGIncrement() * (sga->getSGCache() - 1);

  if (endValue > sga->getSGMaxValue()) endValue = sga->getSGMaxValue();

  long valueForXdc = endValue < sga->getSGMaxValue() ? endValue + 1 : endValue;
  cliRC = SeqGenCliInterfaceProcessXDC(cliInterfaceArr, sga, currContext, diags, exHeap, false /*doPrepare*/,
                                       true /*doExecute*/, valueForXdc);
  lock->unlock();
  return cliRC;
}

static void SeqGenUseDtmHandleError(int cliRC, ExeCliInterface *cliInterface, DistributedLock_JNI *lock, NAHeap *exHeap,
                                    ComDiagsArea &diags) {
  if (cliRC >= 0) return;

  ComDiagsArea *myDiags = ComDiagsArea::allocate(exHeap);
  cliInterface->retrieveSQLDiagnostics(myDiags);
  diags.mergeAfter(*myDiags);
  myDiags->decrRefCount();
  lock->unlock();
  cliInterface->restoreXnState0();
}

static int SeqGenCliInterfaceUpdUseDtm(ExeCliInterface **cliInterfaceArr, SequenceGeneratorAttributes *sga,
                                       NABoolean recycleQry, ContextCli &currContext, ComDiagsArea &diags,
                                       NAHeap *exHeap, long &nextValue, long &endValue) {
  int cliRC;

  long outputValues[5];
  long inputValues[6];

  int outputValuesLen = 0;
  int inputValuesLen = 0;

  long rowsAffected = 0;

  ExeCliInterface *cliInterface = NULL;

  ComDiagsArea *myDiags = NULL;

  char queryBuf[2000];

  long updUID = 0;

  if (!cliInterfaceArr[SEQ_PROCESS_QRY_IDX_DL]) {
    cliRC = SeqGenCliInterfacePrepQry(
        "select  case when cast(? as largeint not null) = 1 then t.minVal else t.nextValue end, t.redefTS from (update "
        "%s.\"%s\".%s set next_value = (case when cast(? as largeint not null) = 1 then min_value + cast(? as largeint "
        "not null) else (case when next_value + cast(? as largeint not null) > max_value then max_value+1 else "
        "next_value + cast(? as largeint not null) end) end), num_calls = num_calls + 1, upd_ts = cast(? as largeint "
        "not null) where seq_uid = %ld return \"OLD\".min_value, \"OLD\".next_value, \"OLD\".redef_ts) t(minVal, "
        "nextValue, redefTS);",
        SEQ_PROCESS_QRY_IDX_DL, "SEQ_PROCESS_QRY_IDX_DL", cliInterfaceArr, sga, currContext, diags, exHeap);
    if (cliRC < 0) return cliRC;
  }

  cliRC = SeqGenCliInterfaceProcessXDC(cliInterfaceArr, sga, currContext, diags, exHeap, true /*doPrepare*/,
                                       false /*doExecute*/);
  if (cliRC < 0) return cliRC;
  cliInterface = cliInterfaceArr[SEQ_PROCESS_QRY_IDX_DL];

  // execute using optimized path
  long delta = sga->getSGIncrement() * (sga->getSGCache() > 0 ? sga->getSGCache() : 1);
  inputValues[0] = (recycleQry ? 1 : 0);
  inputValues[1] = (recycleQry ? 1 : 0);
  inputValues[2] = delta;
  inputValues[3] = delta;
  inputValues[4] = delta;
  inputValuesLen = 5 * sizeof(long);

  // generated a unique id as upd_ts
  ComUID comUID;
  comUID.make_UID();
  updUID = comUID.get_value();
  inputValues[5] = updUID;
  inputValuesLen += sizeof(updUID);

  DistributedLock_JNI *lock = DistributedLock_JNI::getInstance();
  CMPASSERT(lock != NULL);

  long uid = sga->getSGObjectUID().get_value();
  string lockName = "SeqUid" + to_string((long long)uid);
  if (lock->lock(lockName.data(), 0) != DL_OK)  // lockName depends on its uid
    return -FAIL_TO_ACQUIRE_LOCK;

  NABoolean xnWasSuspendedHere = FALSE;
  long transID = getTransactionIDFromContext();

  if (transID > 0)  // a transaction is in progress. Suspend it.
  {
    cliRC = cliInterface->suspendXn();
    if (cliRC < 0) {
      SeqGenUseDtmHandleError(cliRC, cliInterface, lock, exHeap, diags);
      return cliRC;
    }

    xnWasSuspendedHere = TRUE;
  }

  // begin a new transaction
  if (NOT xnWasSuspendedHere) cliInterface->saveXnState0();

  cliRC = cliInterface->beginXn();
  if (cliRC < 0) {
    SeqGenUseDtmHandleError(cliRC, cliInterface, lock, exHeap, diags);
    if (xnWasSuspendedHere) cliInterface->resumeXn();

    return cliRC;
  }

  GetCliGlobals()->currContext()->getTransaction()->implicitXn() = TRUE;

  cliRC = cliInterface->clearExecFetchCloseOpt((char *)inputValues, inputValuesLen, (char *)outputValues,
                                               &outputValuesLen, &rowsAffected);
  if (cliRC < 0) {
    SeqGenUseDtmHandleError(cliRC, cliInterface, lock, exHeap, diags);

    if (diags.mainSQLCODE() == -EXE_NUMERIC_OVERFLOW) {
      cliRC = -EXE_SG_MAXVALUE_EXCEEDED;
    }

    cliInterface->rollbackXn();

    if (xnWasSuspendedHere)
      cliInterface->resumeXn();
    else
      cliInterface->restoreXnState0();

    return cliRC;
  }

  cliRC = cliInterface->commitXn();
  if (cliRC < 0) {
    SeqGenUseDtmHandleError(cliRC, cliInterface, lock, exHeap, diags);

    if (xnWasSuspendedHere)
      cliInterface->resumeXn();
    else
      cliInterface->restoreXnState0();

    cliRC = diags.mainSQLCODE();
    return cliRC;
  }

  if (xnWasSuspendedHere) {
    cliRC = cliInterface->resumeXn();
    if (cliRC < 0) {
      SeqGenUseDtmHandleError(cliRC, cliInterface, lock, exHeap, diags);

      return cliRC;
    }
  }

  if (NOT xnWasSuspendedHere) cliInterface->restoreXnState0();
  xnWasSuspendedHere = FALSE;

  // must find this uid in seq generator. if not found, aqr.
  if (rowsAffected == 0) {
    ComDiagsArea *da = &diags;
    NAString uid = Int64ToNAString(sga->getSGObjectUID().get_value());
    ExRaiseSqlError(exHeap, &da, (ExeErrorCode)(-1582), NULL, NULL, NULL, uid.data());

    lock->unlock();
    return -1582;
  }

  long startValue = outputValues[0];
  long redefTS = outputValues[1];

  // if timestamp mismatch, aqr
  if (redefTS != sga->getSGRedefTime()) {
    ComDiagsArea *da = &diags;
    ExRaiseSqlError(exHeap, &da, (ExeErrorCode)(-1584), NULL, NULL, NULL, Int64ToNAString(redefTS).data(),
                    Int64ToNAString(sga->getSGRedefTime()).data());

    lock->unlock();
    return -1584;
  }

  nextValue = startValue;
  endValue = nextValue + sga->getSGIncrement() * (sga->getSGCache() - 1);

  if (endValue > sga->getSGMaxValue()) endValue = sga->getSGMaxValue();

  cliRC = SeqGenCliInterfaceProcessXDC(cliInterfaceArr, sga, currContext, diags, exHeap, false /*doPrepare*/,
                                       true /*doExecute*/, endValue);
  lock->unlock();
  return cliRC;
}

static int SeqGenCliInterfaceUpdAndValidateMulti(ExeCliInterface **cliInterfaceArr, SequenceGeneratorAttributes *sga,
                                                 NABoolean recycleQry, ContextCli &currContext, ComDiagsArea &diags,
                                                 NAHeap *exHeap, long &nextValue, long &endValue) {
  int cliRC = 0;
  int retCliRC = 0;
  ComDiagsArea *myDiags = NULL;

  if (!cliInterfaceArr[SEQ_CQD_IDX])
    cliInterfaceArr[SEQ_CQD_IDX] =
        new (currContext.exHeap()) ExeCliInterface(currContext.exHeap(), SQLCHARSETCODE_UTF8, &currContext, NULL);
  ExeCliInterface *cqdCliInterface = cliInterfaceArr[SEQ_CQD_IDX];

  // dont run SEQ_GEN update queries as DTM transactional queries.
  // This is needed as seq_gen update may be happening within a user
  // transaction, or within an internal implicit transaction,
  // or within a region transaction query.
  // In all these cases, we dont want the enclosing transaction to impact
  // the seq_gen table update, for ex, if it is aborted.

  int numTries = 0, maxRetryNum = sga->getSGRetryNum();
  NABoolean isOk = FALSE;
  NABoolean useDlockImpl = sga->getSGUseDlockImpl();
  NABoolean useDtmImpl = sga->getSGUseDtmImpl();
  if (useDlockImpl && useDtmImpl) useDtmImpl = FALSE;
  while ((NOT isOk) && (numTries < maxRetryNum)) {
    if (useDtmImpl) {
      cliRC = SeqGenCliInterfaceUpdUseDtm(cliInterfaceArr, sga, recycleQry, currContext, diags, exHeap, nextValue,
                                          endValue);
    } else if (!useDlockImpl)
      cliRC = SeqGenCliInterfaceUpdAndValidate(cliInterfaceArr, sga, recycleQry, currContext, diags, exHeap, nextValue,
                                               endValue);
    else
      cliRC = SeqGenCliInterfaceUpdAndValidateUseDlock(cliInterfaceArr, sga, recycleQry, currContext, diags, exHeap,
                                                       nextValue, endValue);

    if (cliRC == 0) {
      isOk = TRUE;
      break;
    } else if (cliRC != -FAIL_TO_ACQUIRE_LOCK) {
      retCliRC = cliRC;
      goto label_return;
    }

    numTries++;
    int delayTime = 100 + numTries * 25 + rand() % 10;
    if (delayTime < 1000)  // MAX is 1 second
      DELAY(delayTime);
    else
      DELAY(900 + rand() % 100);
  }

  // could not update it after 10 tries. Return error.
  if (NOT isOk) {
    ComDiagsArea *da = &diags;
    ExRaiseSqlError(exHeap, &da, (ExeErrorCode)(1583));

    retCliRC = -1583;
    goto label_return;
  }

label_return:
  if (retCliRC == 0) cqdCliInterface->clearGlobalDiags();
  return retCliRC;
}

int SQLCLI_SeqGenCliInterface(CliGlobals *cliGlobals, void **inCliInterfaceArr, void *seqGenAttrs) {
  int rc = 0;
  int cliRC = 0;

  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();

  ExeCliInterface **cliInterfaceArr = NULL;
  if (inCliInterfaceArr && (*inCliInterfaceArr)) {
    cliInterfaceArr = (ExeCliInterface **)(*inCliInterfaceArr);
  } else {
    cliInterfaceArr = new (currContext.exHeap()) ExeCliInterface *[SEQ_GEN_NUM_PREPS];

    for (int i = 0; i < SEQ_GEN_NUM_PREPS; i++) {
      cliInterfaceArr[i] = NULL;
    }

    if (inCliInterfaceArr) *inCliInterfaceArr = cliInterfaceArr;
  }

  SequenceGeneratorAttributes *sga = (SequenceGeneratorAttributes *)seqGenAttrs;

  long nextValue = 0;
  long endValue = 0;

  cliRC = SeqGenCliInterfaceUpdAndValidateMulti(cliInterfaceArr, sga, FALSE, currContext, diags, currContext.exHeap(),
                                                nextValue, endValue);
  if (cliRC < 0) {
    return cliRC;
  }

  if ((sga->getSGCycleOption()) && (nextValue > sga->getSGMaxValue())) {
    cliRC = SeqGenCliInterfaceUpdAndValidateMulti(cliInterfaceArr, sga, TRUE, currContext, diags, currContext.exHeap(),
                                                  nextValue, endValue);
    if (cliRC < 0) {
      return cliRC;
    }
  }
  sga->setSGNextValue(nextValue);
  sga->setSGEndValue(endValue);

  return 0;
}

int SQLCLI_OrderSeqXDCCliInterface(CliGlobals *cliGlobals, void **inCliInterfaceArr, void *seqGenAttrs, long endValue) {
  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();
  int cliRC = 0;

  ExeCliInterface **cliInterfaceArr = NULL;
  if (inCliInterfaceArr && (*inCliInterfaceArr)) {
    cliInterfaceArr = (ExeCliInterface **)(*inCliInterfaceArr);
  } else {
    cliInterfaceArr = new (currContext.exHeap()) ExeCliInterface *[SEQ_GEN_NUM_PREPS];
    for (int i = 0; i < SEQ_GEN_NUM_PREPS; i++) {
      cliInterfaceArr[i] = NULL;
    }
    if (inCliInterfaceArr) *inCliInterfaceArr = cliInterfaceArr;
  }

  SequenceGeneratorAttributes *sga = (SequenceGeneratorAttributes *)seqGenAttrs;
  cliRC = SeqGenCliInterfaceProcessXDC(cliInterfaceArr, sga, currContext, diags, currContext.exHeap(),
                                       true /*doPrepare*/, true /*doExecute*/, endValue);
  return cliRC;
}

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
    /* OUT */ int *handle) {
  int cliRC = 0;
  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();
  LmLanguageManager *lm = cliGlobals->getLanguageManager((ComRoutineLanguage)language);
  LmRoutine *createdRoutine = NULL;
  LmResult lmres = LM_ERR;

  *handle = NullCliRoutineHandle;

  if (lm)
    lmres = lm->getObjRoutine(serializedInvocationInfo, invocationInfoLen, serializedPlanInfo, planInfoLen,
                              (ComRoutineLanguage)language, (ComRoutineParamStyle)paramStyle, externalName,
                              containerName, externalPath, librarySqlName, &createdRoutine, &diags);

  if (lmres == LM_OK && createdRoutine != NULL) {
    // At the moment we don't set any callback pointers for getRow/emitRow,
    // so the object we are getting can't be used to call the runtime interface,
    // only compile-time calls will work.
    // createdRoutine->setFunctionPtrs(TBD, TBD);

    // assign a CLI handle to the routine and remember it in the context
    *handle = (int)currContext.addTrustedRoutine(createdRoutine);
  } else {
    cliRC = diags.mainSQLCODE();
    if (lmres == LM_OK && cliRC >= 0 && invocationInfoLen == 0)
      // The DDL code calls getRoutine to validate the routine
      // entry point. It does not specify an InvocationInfo and
      // therefore no routine is created, due to insufficient
      // info. Indicate this through this special warning code.
      // Note: No diags area is set, since this is an expected
      // code.
      cliRC = LME_ROUTINE_VALIDATED;
    else
      ex_assert(cliRC < 0, "no diags from getObjRoutine");
  }

  return cliRC;
}

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
    /* IN */ int outputRowLen) {
  int cliRC = 0;
  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();
  LmResult res = LM_ERR;
  LmRoutine *lmRoutine = cliGlobals->currContext()->findTrustedRoutine(handle);

  if (lmRoutine)
    res = lmRoutine->invokeRoutineMethod(static_cast<tmudr::UDRInvocationInfo::CallPhase>(phaseEnumAsInt),
                                         serializedInvocationInfo, invocationInfoLen, invocationInfoLenOut,
                                         serializedPlanInfo, planInfoLen, planNum, planInfoLenOut, inputRow,
                                         inputRowLen, outputRow, outputRowLen, &diags);

  if (res != LM_OK) {
    cliRC = diags.mainSQLCODE();

    if (cliRC >= 0) {
      // make sure the diags area indicates an error
      cliRC = -CLI_ROUTINE_INVOCATION_ERROR;
      diags << DgSqlCode(cliRC);
    }
  }

  return cliRC;
}

int SQLCLI_GetRoutineInvocationInfo(
    /* IN */ CliGlobals *cliGlobals,
    /* IN */ int handle,
    /* IN/OUT */ char *serializedInvocationInfo,
    /* IN */ int invocationInfoMaxLen,
    /* OUT */ int *invocationInfoLenOut,
    /* IN/OUT */ char *serializedPlanInfo,
    /* IN */ int planInfoMaxLen,
    /* IN */ int planNum,
    /* OUT */ int *planInfoLenOut) {
  int cliRC = 0;
  ContextCli &currContext = *(cliGlobals->currContext());
  ComDiagsArea &diags = currContext.diags();
  LmResult res = LM_ERR;
  LmRoutine *lmRoutine = cliGlobals->currContext()->findTrustedRoutine(handle);

  if (lmRoutine)
    res = lmRoutine->getRoutineInvocationInfo(serializedInvocationInfo, invocationInfoMaxLen, invocationInfoLenOut,
                                              serializedPlanInfo, planInfoMaxLen, planNum, planInfoLenOut, &diags);

  if (res != LM_OK) {
    cliRC = diags.mainSQLCODE();

    if (cliRC >= 0) {
      cliRC = -CLI_ROUTINE_INVOCATION_INFO_ERROR;
      diags << DgSqlCode(cliRC);
    }
  }

  return cliRC;
}

int SQLCLI_PutRoutine(
    /* IN */ CliGlobals *cliGlobals,
    /* IN */ int handle) {
  if (handle != NullCliRoutineHandle) cliGlobals->currContext()->putTrustedRoutine(handle);

  return 0;
}

int SQLCLI_LoadTrafMetadataInCache(
    /* IN */ CliGlobals *cliGlobals) {
  cliGlobals->currContext()->loadTrafMetadataInCache();

  return 0;
}

int SQLCLI_LoadTrafMetadataIntoSharedCache(
    /* IN */ CliGlobals *cliGlobals) {
  cliGlobals->currContext()->loadTrafMetadataIntoSharedCache();

  return 0;
}

int SQLCLI_LoadTrafDataIntoSharedCache(
    /* IN */ CliGlobals *cliGlobals) {
  cliGlobals->currContext()->loadTrafDataIntoSharedCache();

  return 0;
}

int SQLCLI_GetTransactionId(
    /*IN*/ CliGlobals *cliGlobals,
    /*OUT*/ long *trans_id) {
  assert(trans_id != NULL);
  if (!cliGlobals) return -CLI_NO_CURRENT_CONTEXT;

  ContextCli *currentContext = cliGlobals->currContext();
  if (currentContext && currentContext->getTransaction())
    *trans_id = currentContext->getTransaction()->getTransid();
  else
    *trans_id = -1;

  return SUCCESS;
}

int SQLCLI_GetStatementHeapSize(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN*/ SQLSTMT_ID *statement_id,
    /*OUT*/ long *heapSize) {
  if (statement_id == NULL || heapSize == NULL) return SUCCESS;

  *heapSize = 0;
  StatementInfo *stmtInfo = (StatementInfo *)(statement_id->handle);
  if (stmtInfo && stmtInfo->statement()) {
    NAHeap *heap = stmtInfo->statement()->stmtHeap();
    if (heap) *heapSize = heap->getTotalSize();
  }
  if (heapSize == 0) {
    ContextCli &currContext = *(cliGlobals->currContext());
    Statement *stmt = currContext.getStatement(statement_id);
    if (stmt != NULL) {
      NAHeap *heap = stmt->stmtHeap();
      if (heap) *heapSize = heap->getTotalSize();
    }
  }
  return SUCCESS;
}

int SQLCLI_GetAuthGracePwdCnt(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN*/ int userid,
    /*OUT*/ Int16 &graceCnt) {
  int retcode = 0;

  // create initial context, if first call, and add module, if any.
  retcode = CliPrologue(cliGlobals, NULL);
  if (isERROR(retcode)) return retcode;

  ContextCli &currContext = *(cliGlobals->currContext());

  retcode = currContext.getGracePwdCnt(userid, graceCnt);

  return CliEpilogue(cliGlobals, NULL, retcode);
}

int SQLCLI_UpdateAuthGracePwdCnt(
    /*IN*/ CliGlobals *cliGlobals,
    /*IN*/ int userid,
    /*IN*/ Int16 graceCnt) {
  int retcode = 0;

  // create initial context, if first call, and add module, if any.
  retcode = CliPrologue(cliGlobals, NULL);
  if (isERROR(retcode)) return retcode;

  ContextCli &currContext = *(cliGlobals->currContext());

  retcode = currContext.updateGracePwdCnt(userid, graceCnt);

  return CliEpilogue(cliGlobals, NULL, retcode);
}
