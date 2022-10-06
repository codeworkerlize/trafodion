#ifndef LMROUTINEJAVAOBJ_H
#define LMROUTINEJAVAOBJ_H
/* -*-C++-*-
**********************************************************************
*
* File:         LmRoutineJavaObj.h
* Description:
*
* Created:      04/30/2015
* Language:     C++
*

**********************************************************************/

#include "LmLangManagerJava.h"
#include "LmRoutineJava.h"
#include "sqludr/sqludr.h"

//////////////////////////////////////////////////////////////////////
//
// LmRoutineJava
//
// The LmRoutineJavaObj is a concrete class used to maintain state for,
// and the invocation of, a fixed set of methods for objects
// of a class derived from tmudr::UDR.
//////////////////////////////////////////////////////////////////////
class LmRoutineJavaObj : public LmRoutineJava {
  friend class LmLanguageManagerJava;
  friend class LmJavaExceptionReporter;

 public:
  virtual LmResult setRuntimeInfo(const char *parentQid, int totalNumInstances, int myInstanceNum, ComDiagsArea *da);

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
  void setJNIFunctionPtrs(void *jniGetNextRowPtr, void *jniEmitRowPtr);
  void setRowLengths(int inputRowLen, int outputRowLen);

 protected:
  LmRoutineJavaObj(const char *sqlName, const char *externalName, const char *librarySqlName,
                   ComRoutineTransactionAttributes transactionAttrs, ComRoutineSQLAccess sqlAccessMode,
                   ComRoutineExternalSecurity externalSecurity, int routineOwnerId,
                   const char *serializedInvocationInfo, int invocationInfoLen, const char *serializedPlanInfo,
                   int planInfoLen, LmLanguageManagerJava *lm, jobject udrObject, LmContainer *container,
                   ComDiagsArea *diagsArea);

  ~LmRoutineJavaObj();

  // check whether the constructor successfully created the Java objects
  virtual ComBoolean isValid() const;

  // global reference to UDR object, of the user-defined class
  // that implements the UDR
  jobject udrObjectG_;

  // global reference to the LmUDRObjMethodInvoke object that
  // is used to unpack/pack information and invoke routines, this
  // is the one we actually call through JNI
  jobject udrInvokeObjectG_;

  // local reference to a byte array with the last returned
  // serialized UDRInvocationInfo and UDRPlanInfo objects and
  // their lengths
  jbyteArray returnedIIL_;
  jbyteArray returnedPIL_;
  jsize iiLen_;
  jsize piLen_;

  // lengths of input and output rows, these are stored here
  // to avoid unpacking the UDRInvocationInfo object on the
  // C++ side
  int inputRowLen_;
  int outputRowLen_;

  // function pointers for JNI and to emit result rows and EOD
  SQLUDR_EmitRow emitRowPtr_;
  void *jniGetNextRowPtr_;
  void *jniEmitRowPtr_;
};  // class LmRoutineJavaObj

#endif
