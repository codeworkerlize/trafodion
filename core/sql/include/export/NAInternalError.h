
#ifndef _NA_INTERNAL_ERROR_H_
#define _NA_INTERNAL_ERROR_H_
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         NAInternalError.h
 * Description:  Encapsulate the exception call back handler
 *               used in NAAssert and NAAbort functions
 *
 * Created:      2/9/2003
 * Language:     C++
 *
 *****************************************************************************
 */
class ExceptionCallBack;

class NAInternalError {
 private:
  static ExceptionCallBack *pExceptionCallBack_;

 public:
  static ExceptionCallBack *getExceptionCallBack();
  static void registerExceptionCallBack(ExceptionCallBack *p);
  static void unRegisterExceptionCallBack();
  static void throwFatalException(const char *msg, const char *fileName, UInt32 lineNum);
  static void throwAssertException(const char *cond, const char *fileName, UInt32 lineNum);
};

#endif
