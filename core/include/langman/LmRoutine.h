#ifndef LMROUTINE_H
#define LMROUTINE_H
/* -*-C++-*-
**********************************************************************
*
* File:         LmRoutine.h.h
* Description:
*
* Created:      08/22/2003
* Language:     C++
*

**********************************************************************/

// Some data members such as the NAList come from outside this
// DLL. The Windows compiler generates a warning about them requiring
// a DLL interface in order to be used by LmRoutine clients. We
// will suppress such warnings.

#include "LmAssert.h"
#include "LmContManager.h"
#include "LmResultSet.h"
#include "common/ComSmallDefs.h"
#include "langman/LmCommon.h"

// Some data members such as the NAList come from outside this
// DLL. The Windows compiler generates a warning about them requiring
// a DLL interface in order to be used by LmRoutine clients. We
// will suppress such warnings.

//////////////////////////////////////////////////////////////////////
//
// Forward References
//
//////////////////////////////////////////////////////////////////////
class LmContainer;
class LmLanguageManager;
class LmParameter;

//////////////////////////////////////////////////////////////////////
//
// LmRoutine
//
// The LmRoutine class represents a handle to an external routine,
// being return by the LM to a client so the client can execute the
// the routine using the invokeRoutine service. Internally the class
// records invocation state related to the external routine as determined
// by a specific LM, possibly via derivation.
//
//////////////////////////////////////////////////////////////////////
class SQLLM_LIB_FUNC LmRoutine : public NABasicObject {
 public:
  // Returns the number of result sets that were returned by the
  // routine
  ComUInt32 getNumResultSets() const { return resultSetList_.entries(); }

  // Gets the LmResultSet object from the resultSetList_ given a
  // index as the input. Asserts on an invalid index.
  LmResultSet *getLmResultSet(ComUInt32 index) const {
    LM_ASSERT(index < resultSetList_.entries());
    return resultSetList_[index];
  }

  ComRoutineTransactionAttributes getTransactionAttrs() const { return transactionAttrs_; }
  ComRoutineSQLAccess getSqlAccessMode() const { return sqlAccessMode_; }

  const char *getSqlName() const { return sqlName_; }

  const char *getLibrarySqlName() const { return librarySqlName_; }

  // Deletes the given LmResultSet object.
  // Any error during the close will be reported in the
  // diagsArea, if available. Asserts on an invalid pointer.
  virtual void cleanupLmResultSet(LmResultSet *resultSet, ComDiagsArea *diagsArea = NULL) = 0;

  // Deletes the LmResultSet object at a given index.
  // Any error during the close will be reported in the
  // diagsArea, if available. Asserts on an invalid index.
  virtual void cleanupLmResultSet(ComUInt32 index, ComDiagsArea *diagsArea = NULL) = 0;

  // Deletes all the LmResultSet objects from the resultSetList_
  // and empties the list. Any error during the cleanup process
  // will be reported in the diagsArea, if available.
  virtual void cleanupResultSets(ComDiagsArea *diagsArea = NULL) = 0;

  virtual const char *getParentQid() { return parentQid_; }

  virtual const char *getClientInfo() { return clientInfo_; }

  virtual LmResult setRuntimeInfo(const char *parentQid, int numTotalInstances, int myInstanceNum, ComDiagsArea *da);

  // Main routine invocation method
  virtual LmResult invokeRoutine(void *inputRow, void *outputRow, ComDiagsArea *da) = 0;
  virtual LmResult invokeRoutineMethod(
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
      /* IN/OUT */ ComDiagsArea *da);
  virtual LmResult getRoutineInvocationInfo(
      /* IN/OUT */ char *serializedInvocationInfo,
      /* IN */ int invocationInfoMaxLen,
      /* OUT */ int *invocationInfoLenOut,
      /* IN/OUT */ char *serializedPlanInfo,
      /* IN */ int planInfoMaxLen,
      /* IN */ int planNum,
      /* OUT */ int *planInfoLenOut,
      /* IN/OUT */ ComDiagsArea *da);

  virtual LmResult setFunctionPtrs(SQLUDR_GetNextRow getNextRowPtr, SQLUDR_EmitRow emitRowPtr, ComDiagsArea *da);

  ComRoutineLanguage getLanguage() const { return language_; }
  ComRoutineParamStyle getParamStyle() const { return paramStyle_; }

  ComRoutineExternalSecurity getExternalSecurity() const { return externalSecurity_; }
  int getRoutineOwnerId() const { return routineOwnerId_; }

  LmParameter *getLmParams() const { return lmParams_; }

  int getInputParamRowLen() const { return inputParamRowLen_; }
  int getOutputRowLen() const { return outputRowLen_; }

  // Returns the routine name to use for error reporting.
  // For command-line operations the sqlName_ is not available
  // and so we'll use the externalName_ instead.
  const char *getNameForDiags();

  virtual ~LmRoutine();

  virtual LmLanguageManager *getLM() { return lm_; }

  LmContainer *container() { return (LmContainer *)container_; }

  LmHandle getContainerHandle() { return ((LmContainer *)container_)->getHandle(); }

  // make a final call to the UDF or do the equivalent, if needed
  virtual LmResult handleFinalCall(ComDiagsArea *diagsArea = NULL) = 0;

 protected:
  LmRoutine(LmHandle container, LmHandle routine, const char *sqlName, const char *externalName,
            const char *librarySqlName, ComUInt32 numParam, ComUInt32 maxResultSets, ComRoutineLanguage language,
            ComRoutineParamStyle paramStyle, ComRoutineTransactionAttributes transactionAttrs,
            ComRoutineSQLAccess sqlAccessMode, ComRoutineExternalSecurity externalSecurity, int routineOwnerId,
            const char *parentQid, const char *clientInfo, int inputParamRowLen, int outputRowLen,
            const char *currentUserName, const char *sessionUserName, LmParameter *lmParams, LmLanguageManager *lm);

  LmLanguageManager *lm_;     // Owning LM
  LmHandle container_;        // Handle to the external routine's container.
  LmHandle routine_;          // Handle to the external routine.
  ComUInt32 numSqlParam_;     // Number of parameters.
  ComUInt32 numParamsInSig_;  // Number of params in the method signature
  ComUInt32 maxResultSets_;   // Maximum number of resultsets that
                              // this routine can return

  NAList<LmResultSet *> resultSetList_;  // LmResultSet objects. Only
                                         // Non-Null, Not-Closed &
                                         // Non-Duplicate result sets
                                         // are stored.

  ComRoutineTransactionAttributes transactionAttrs_;  // check if transaction is required

  ComRoutineSQLAccess sqlAccessMode_;  // level of SQL access that is
                                       // allowed within external routine

  char *externalName_;     // Name of the C/Java function or method
  char *sqlName_;          // Routine's three part ANSI name
  char *librarySqlName_;   // Routine Library's three part ANSI name
  const char *parentQid_;  // Query ID of the parent SQL statement
  const char *clientInfo_;

  ComRoutineLanguage language_;
  ComRoutineParamStyle paramStyle_;

  ComRoutineExternalSecurity externalSecurity_;
  int routineOwnerId_;

  int inputParamRowLen_;
  int outputRowLen_;

  const char *udrCatalog_;  // Default catalog value
  const char *udrSchema_;   // Default schema value

  LmParameter *lmParams_;

 private:
  // Do not implement a default constructor
  LmRoutine();

};  // class LmRoutine

#endif
