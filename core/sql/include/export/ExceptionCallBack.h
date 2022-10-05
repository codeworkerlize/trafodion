
#ifndef _EXCEPTION_CALL_BACK_H_
#define _EXCEPTION_CALL_BACK_H_
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ExceptionCallBack.cpp
 * Description:  Call back super class for NAAssert and NAAbort
 *               to throw an Exception object in the compiler context
 *
 * Created:      2/9/2003
 * Language:     C++
 *
 *****************************************************************************
 */
class ExceptionCallBack {
 public:
  virtual void throwAssertException(const char *condition, const char *fileName, UInt32 lineNum,
                                    const char *stackTrace = NULL) = 0;
  virtual void throwFatalException(const char *msg, const char *fileName, UInt32 lineNum,
                                   const char *stackTrace = NULL) = 0;
};

#endif
