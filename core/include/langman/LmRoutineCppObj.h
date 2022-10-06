#ifndef LMROUTINECPPOBJ_H
#define LMROUTINECPPOBJ_H
/* -*-C++-*-
**********************************************************************
*
* File:         LmRoutineCppObj.h
* Description:  LmRoutine for C++ routines
* Created:      01/22/2015
* Language:     C++
*

**********************************************************************/

#include "LmRoutineC.h"
#include "sqludr/sqludr.h"

class SqlBuffer;

//////////////////////////////////////////////////////////////////////
//
// LmRoutineCppObj
//
// The LmRoutineCppObj is a concrete class used to maintain state
// for, and the invocation of, a C++ routine that uses the object
// interface (class TMUDRInterface).
//
//////////////////////////////////////////////////////////////////////
class SQLLM_LIB_FUNC LmRoutineCppObj : public LmRoutine {
  friend class LmLanguageManagerC;

 public:
  tmudr::UDRInvocationInfo *getInvocationInfo() { return invocationInfo_; }
  virtual const char *getParentQid();
  virtual LmResult setRuntimeInfo(const char *parentQid, int totalNumInstances, int myInstanceNum, ComDiagsArea *da);
  virtual LmResult invokeRoutine(void *inputRow, void *outputRow, ComDiagsArea *da);
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
  LmResult validateWall(char *userBuf, int userBufLen, ComDiagsArea *da, const char *bufferName);
  LmResult validateWalls(ComDiagsArea *da = NULL);

  // The following pure virtual methods must be implemented even
  // though we do not currently support result sets for C++ routines
  void cleanupLmResultSet(LmResultSet *resultSet, ComDiagsArea *diagsArea = NULL) {}
  void cleanupLmResultSet(ComUInt32 index, ComDiagsArea *diagsArea = NULL) {}
  void cleanupResultSets(ComDiagsArea *diagsArea = NULL) {}

  virtual LmResult handleFinalCall(ComDiagsArea *diagsArea = NULL);

 protected:
  LmRoutineCppObj(tmudr::UDRInvocationInfo *invocationInfo, tmudr::UDRPlanInfo *planInfo, tmudr::UDR *interfaceObj,
                  const char *sqlName, const char *externalName, const char *librarySqlName, ComUInt32 maxResultSets,
                  ComRoutineTransactionAttributes transactionAttrs, ComRoutineSQLAccess sqlAccessMode,
                  ComRoutineExternalSecurity externalSecurity, int routineOwnerId, LmLanguageManagerC *lm,
                  LmContainer *container, ComDiagsArea *diagsArea);

  virtual ~LmRoutineCppObj();
  LmResult dealloc(ComDiagsArea *diagsArea);

 private:
  void setUpWall(char *userBuf, int userBufLen);

  tmudr::UDRInvocationInfo *invocationInfo_;
  NAArray<tmudr::UDRPlanInfo *> planInfos_;
  tmudr::UDR *interfaceObj_;

  // number, lengths and pointers to row buffers
  char *paramRow_;  // of length inputParamRowLen_ (stored in base class)
  int numInputTables_;
  int *inputRowLengths_;
  char **inputRows_;
  char *outputRow_;  // of length outputRowLen_ (stored in base class)
};                   // class LmRoutineCppObj

#endif
