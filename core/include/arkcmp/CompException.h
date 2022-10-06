
#ifndef _CMP_EXCEPTION_H_
#define _CMP_EXCEPTION_H_
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         CompException.h
 * Description:  Compiler Exception handling class
 *
 * Created:      9/26/2003
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************/
#include "common/NABoolean.h"
#include "export/ExceptionCallBack.h"
#include "export/NABasicObject.h"
#include "export/NAStringDef.h"
#include "sqlmxevents/logmxevent.h"

// BaseException should not be instantiated directly
class BaseException : public NABasicObject {
 public:
  UInt32 getLineNum();
  const char *getFileName();
  virtual void throwException() = 0;

 protected:
  void doFFDCDebugger(char *msg);
  BaseException(const char *fileName, UInt32 lineNum);
  BaseException();

 private:
  void hookDebugger();
  void hookDebuggerHelper();
  char fileName_[128];
  UInt32 lineNum_;
};

// UserException is a user error such as those detected by GenExit() callers
// like Generator::verifyUpdatableTransMode(), ItmBalance::preCodeGen(),
// and ExpGenerator::addDevaultValues()
class UserException : public BaseException {
 public:
  UserException(const char *fileName, UInt32 lineNum);
  virtual void throwException();
};

class DDLException : public BaseException {
 public:
  DDLException(int sqlcode, const char *fileName, UInt32 lineNum);
  virtual void throwException();
  int getSqlcode(void) { return sqlcode_; }

 private:
  int sqlcode_;
};

// FatalException is unrecoverable, give up the compilation if one is thrown
class FatalException : public BaseException {
 public:
  FatalException(const char *msg, const char *fileName, UInt32 lineNum, const char *stackTrace = NULL);
  const char *getMsg();
  const char *getStackTrace();
  virtual void throwException();

 private:
  char msg_[EXCEPTION_MSG_SIZE + 1];
  char stackTrace_[STACK_TRACE_SIZE + 1];
};

// CmpInternalException is a replacement for EH_INTRNAL_EXCEPTION
class CmpInternalException : public BaseException {
 public:
  CmpInternalException(const char *msg, const char *fileName, UInt32 lineNum);
  const char *getMsg();
  virtual void throwException();

 private:
  char msg_[EXCEPTION_MSG_SIZE];
};

// AssertException is thrown from an Assertion in the compiler.
// Depending on the compilation environment, it may be recoverable.
class AssertException : public BaseException {
 public:
  AssertException(const char *condition, const char *fileName, UInt32 lineNum, const char *stackTrace = NULL);
  // copy contructor
  AssertException(AssertException &e);
  const char *getCondition();
  const char *getStackTrace();
  virtual void throwException();

 private:
  char condition_[EXCEPTION_CONDITION_SIZE + 1];
  char stackTrace_[STACK_TRACE_SIZE + 1];
};

class OsimLogException : public BaseException {
 public:
  OsimLogException(const char *errMsg, const char *srcFileName, UInt32 srcLineNum);
  const char *getErrMessage() { return errMsg_.data(); }
  virtual void throwException();

 private:
  NAString errMsg_;
};

class PassOneAssertFatalException : public FatalException {
 public:
  PassOneAssertFatalException(const char *condition, const char *fileName, UInt32 lineNum);
  virtual void throwException();

 private:
  char condition_[EXCEPTION_CONDITION_SIZE];
};

class PassOneNoPlanFatalException : public FatalException {
 public:
  PassOneNoPlanFatalException(const char *fileName, UInt32 lineNum);
  virtual void throwException();
};

class PassOneSkippedPassTwoNoPlanFatalException : public FatalException {
 public:
  PassOneSkippedPassTwoNoPlanFatalException(const char *fileName, UInt32 lineNum);
  virtual void throwException();
};

class OptAssertException : public AssertException {
 public:
  OptAssertException(AssertException &e, int taskCount);
  virtual void throwException();

 private:
  int taskCount_;
};

class CmpExceptionCallBack : public ExceptionCallBack {
 public:
  void throwFatalException(const char *msg, const char *file, UInt32 line, const char *stackTrace = NULL);
  void throwAssertException(const char *cond, const char *file, UInt32 line, const char *stackTrace = NULL);
};

class CmpExceptionEnv {
 private:
  static CmpExceptionCallBack eCallBack_;

 public:
  static void registerCallBack();
  static void unRegisterCallBack();
};

class CompCCAssert : public NABasicObject {
 public:
  static NABoolean getUseCCMPAssert() { return useCCMPAssert_; };
  static void setUseCCMPAssert(NABoolean useAssert) { useCCMPAssert_ = useAssert; };

 private:
  CompCCAssert(){};
  ~CompCCAssert(){};
  static THREAD_P NABoolean useCCMPAssert_;
};

extern void CmpCCAssert(char *, char *, int);
// The following CCMPASSERT is for supporting an assert mechanism
// This is to let QA catch some assertions during their testing, but
// also we do not want the customer to be effected.
//
#define CCMPASSERT(x)                                                                              \
  {                                                                                                \
    if (CompCCAssert::getUseCCMPAssert() && !(x)) CmpAssertInternal("" #x "", __FILE__, __LINE__); \
  }

#endif
