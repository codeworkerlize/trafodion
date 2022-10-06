/* -*-C++-*-
**********************************************************************
*
* File:         LmRoutine.cpp
* Description:  LmRoutine class
* Created:      09/02/2009
* Language:     C++
*

**********************************************************************/

#include "LmRoutine.h"

#include "LmLangManager.h"
#include "common/ComObjectName.h"
#include "langman/LmCommon.h"

LmRoutine::LmRoutine(LmHandle container, LmHandle routine, const char *sqlName, const char *externalName,
                     const char *librarySqlName, ComUInt32 numParam, ComUInt32 maxResultSets,
                     ComRoutineLanguage language, ComRoutineParamStyle paramStyle,
                     ComRoutineTransactionAttributes transactionAttrs, ComRoutineSQLAccess sqlAccessMode,
                     ComRoutineExternalSecurity externalSecurity, int routineOwnerId, const char *parentQid,
                     const char *clientInfo, int inputParamRowLen, int outputRowLen, const char *currentUserName,
                     const char *sessionUserName, LmParameter *lmParams, LmLanguageManager *lm)
    : container_(container),
      routine_(routine),
      externalName_(NULL),
      numSqlParam_(numParam),
      maxResultSets_(maxResultSets),
      lm_(lm),
      sqlName_(NULL),
      librarySqlName_(NULL),
      numParamsInSig_(0),
      resultSetList_(collHeap()),
      parentQid_(NULL),
      clientInfo_(NULL),
      language_(language),
      paramStyle_(paramStyle),
      transactionAttrs_(transactionAttrs),
      sqlAccessMode_(sqlAccessMode),
      externalSecurity_(externalSecurity),
      routineOwnerId_(routineOwnerId),
      inputParamRowLen_(inputParamRowLen),
      outputRowLen_(outputRowLen),
      lmParams_(lmParams),
      udrCatalog_(NULL),
      udrSchema_(NULL) {
  // Make copies of some of the strings passed in
  if (externalName) externalName_ = copy_and_pad(externalName, strlen(externalName), 8);
  if (sqlName) sqlName_ = copy_and_pad(sqlName, strlen(sqlName), 8);
  if (librarySqlName) librarySqlName_ = copy_and_pad(librarySqlName, strlen(librarySqlName), 8);
  if (parentQid) parentQid_ = copy_and_pad(parentQid, strlen(parentQid), 8);
  if (clientInfo) clientInfo_ = copy_and_pad(clientInfo, strlen(clientInfo), 8);

  // Use a ComObjectName object to parse the SQL name and store copies
  // of the catalog and schema names
  NAString sqlNameStr(sqlName);
  ComObjectName objName(sqlNameStr);
  const NAString &cat = objName.getCatalogNamePartAsAnsiString();
  const NAString &sch = objName.getSchemaNamePartAsAnsiString();
  if (!cat.isNull()) udrCatalog_ = copy_string(collHeap(), cat.data());
  if (!sch.isNull()) udrSchema_ = copy_string(collHeap(), sch.data());
}

LmRoutine::~LmRoutine() {
  if (externalName_) free(externalName_);
  if (sqlName_) free(sqlName_);
  if (librarySqlName_) free(librarySqlName_);
  if (parentQid_) free((char *)parentQid_);
  if (clientInfo_) free((char *)clientInfo_);

  if (udrCatalog_) NADELETEBASIC(udrCatalog_, collHeap());
  if (udrSchema_) NADELETEBASIC(udrSchema_, collHeap());
}

LmResult LmRoutine::invokeRoutineMethod(
    /* IN */ tmudr::UDRInvocationInfo::CallPhase phase,
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
    /* IN */ int outputRowLen,
    /* IN/OUT */ ComDiagsArea *da) {
  LM_ASSERT(0);  // should not call this method on the base class
  return LM_ERR;
}

LmResult LmRoutine::setRuntimeInfo(const char *parentQid, int totalNumInstances, int instanceNum, ComDiagsArea *da) {
  LM_ASSERT(0);  // should not call this method on the base class
  return LM_ERR;
}

LmResult LmRoutine::getRoutineInvocationInfo(
    /* IN/OUT */ char *serializedInvocationInfo,
    /* IN */ int invocationInfoMaxLen,
    /* OUT */ int *invocationInfoLenOut,
    /* IN/OUT */ char *serializedPlanInfo,
    /* IN */ int planInfoMaxLen,
    /* IN */ int planNum,
    /* OUT */ int *planInfoLenOut,
    /* IN/OUT */ ComDiagsArea *da) {
  LM_ASSERT(0);  // should not call this method on the base class
  return LM_ERR;
}

LmResult LmRoutine::setFunctionPtrs(SQLUDR_GetNextRow getNextRowPtr, SQLUDR_EmitRow emitRowPtr, ComDiagsArea *da) {
  LM_ASSERT(0);  // should not call this method on the base class
  return LM_ERR;
}

const char *LmRoutine::getNameForDiags() { return (lm_->getCommandLineMode() ? externalName_ : sqlName_); }
