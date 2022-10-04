

#include "sqlcomp/CompilerSwitchDDL.h"

CompilerSwitchDDL::CompilerSwitchDDL() : cmpSwitched_(FALSE), savedCmpParserFlags_(0), savedCliParserFlags_(0) {}

short CompilerSwitchDDL::switchCompiler(Int32 cntxtType) {
  cmpSwitched_ = FALSE;
  CmpContext *currContext = CmpCommon::context();

  // we should switch to another CI only if we are in an embedded CI
  if (IdentifyMyself::GetMyName() == I_AM_EMBEDDED_SQL_COMPILER) {
    if (SQL_EXEC_SWITCH_TO_COMPILER_TYPE(cntxtType)) {
      // failed to switch/create compiler context.
      return -1;
    }

    cmpSwitched_ = TRUE;
  }

  if (sendAllControlsAndFlags(currContext, cntxtType)) {
    switchBackCompiler();
    return -1;
  }

  return 0;
}

short CompilerSwitchDDL::switchBackCompiler() {
  if (cmpSwitched_) {
    GetCliGlobals()->currContext()->copyDiagsAreaToPrevCmpContext();
    CmpCommon::diags()->clear();
  }
  restoreAllControlsAndFlags();

  if (cmpSwitched_) {
    // Clear the diagnostics area of the current CmpContext
    CmpCommon::diags()->clear();
    // switch back to the original commpiler, ignore return error
    SQL_EXEC_SWITCH_BACK_COMPILER();

    cmpSwitched_ = FALSE;
  }

  return 0;
}

void CompilerSwitchDDL::saveAllFlags() {
  // save the current parserflags setting from this compiler and executor context
  savedCmpParserFlags_ = Get_SqlParser_Flags(0xFFFFFFFF);
  SQL_EXEC_GetParserFlagsForExSqlComp_Internal(savedCliParserFlags_);
}

void CompilerSwitchDDL::setAllFlags() {
  SQL_EXEC_SetParserFlagsForExSqlComp_Internal(INTERNAL_QUERY_FROM_EXEUTIL);

  Set_SqlParser_Flags(ALLOW_VOLATILE_SCHEMA_IN_TABLE_NAME);

  SQL_EXEC_SetParserFlagsForExSqlComp_Internal(ALLOW_VOLATILE_SCHEMA_IN_TABLE_NAME);
}

void CompilerSwitchDDL::restoreAllFlags() {
  // Restore parser flags settings of cmp and exe context to what
  // they originally were
  Set_SqlParser_Flags(savedCmpParserFlags_);
  SQL_EXEC_AssignParserFlagsForExSqlComp_Internal(savedCliParserFlags_);

  return;
}

short CompilerSwitchDDL::sendAllControlsAndFlags(CmpContext *prevContext, Int32 cntxtType) {
  Int32 cliRC;
  CmpContext *cmpctxt = CmpCommon::context();

  // already sent, just return
  if (cmpctxt->isSACDone()) {
    saveAllFlags();
    setAllFlags();

    return 0;
  }

  NAString cqdsHold(" control query defaults ");
  NAString cqdsSet(" control query defaults ");

  cqdsHold.append(" volatile_schema_in_use ");
  cqdsSet.append(" volatile_schema_in_use 'OFF' ");

  cqdsHold.append(" , hbase_filter_preds ");
  cqdsSet.append(" , hbase_filter_preds 'OFF' ");

  // We have to turn NJ on for meta query compilation.
  cqdsHold.append(" , nested_joins ");
  cqdsSet.append(" , nested_joins 'ON' ");

  // turn off esp parallelism until optimizer fixes esp plan issue pbm.
  cqdsHold.append(" , attempt_esp_parallelism ");
  cqdsSet.append(" , attempt_esp_parallelism 'OFF' ");

  // this cqd causes problems when internal indexes are created.
  // disable it here for ddl operations.
  // Not sure if this cqd is used anywhere or is needed.
  // Maybe we should remove it.
  cqdsHold.append(" , hide_indexes ");
  cqdsSet.append(" , hide_indexes 'NONE' ");

  cqdsHold.append(" , traf_no_dtm_xn ");
  cqdsSet.append(" , traf_no_dtm_xn 'OFF' ");

  cqdsHold.append(" , hbase_rowset_vsbb_opt ");
  cqdsSet.append(" , hbase_rowset_vsbb_opt 'OFF' ");

  NABoolean hqcLeakTest = FALSE;
#ifndef NDEBUG
  if (getenv("HQC_LEAK_TEST")) hqcLeakTest = TRUE;
#endif

  if (NOT hqcLeakTest) {
    // there is a memory leak that shows up if HQC is turned on for internal
    // MD queries. Until the leak is diagnosed and fixed, turn off HQC.

    // maybe we have fixed it in m15995
    // cqdsHold.append(" , hybrid_query_cache ");
    // cqdsSet.append (" , hybrid_query_cache 'OFF' ");
  }

  cqdsHold.append(" HOLD ; ");
  cqdsSet.append(" ; ");

  saveAllFlags();
  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, cmpctxt->sqlSession()->getParentQid());

  if (cmpctxt->getCIClass() != CmpContextInfo::CMPCONTEXT_TYPE_META) {
    cliRC = cliInterface.executeImmediate("control query shape hold;");
    if (cliRC < 0) {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      return -1;
    }

    cliRC = cliInterface.executeImmediate(cqdsHold.data());
    if (cliRC < 0) {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      return -1;
    }
  }

  cliRC = cliInterface.executeImmediate(cqdsSet.data());
  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  cmpctxt->setSACDone(TRUE);

  setAllFlags();

  return 0;
}

void CompilerSwitchDDL::restoreAllControlsAndFlags() {
  Lng32 cliRC = 0;
  CmpContext *cmpctxt = CmpCommon::context();

  if (cmpctxt->getCIClass() != CmpContextInfo::CMPCONTEXT_TYPE_META) {
    ExeCliInterface cliInterface(STMTHEAP);

    cliRC = cliInterface.executeImmediate("control query shape restore;");

    NAString cqdsRestore(" control query defaults ");

    cqdsRestore.append("   volatile_schema_in_use ");
    cqdsRestore.append(" , hbase_filter_preds ");
    cqdsRestore.append(" , nested_joins ");
    cqdsRestore.append(" , attempt_esp_parallelism ");
    cqdsRestore.append(" , hide_indexes ");
    cqdsRestore.append(" , traf_no_dtm_xn ");
    cqdsRestore.append(" , hbase_rowset_vsbb_opt ");
    // cqdsRestore.append(" , hybrid_query_cache ");

    cqdsRestore.append(" RESTORE ; ");

    cliRC = cliInterface.executeImmediate(cqdsRestore.data());
    if (cliRC < 0) {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      return;
    }

    cmpctxt->setSACDone(FALSE);
  }

  restoreAllFlags();

  return;
}
