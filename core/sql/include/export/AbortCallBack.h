
#ifndef _ABORT_CALL_BACK_H_
#define _ABORT_CALL_BACK_H_
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         AbortCallBack.h
 * Description:  abort call back functions
 *
 *
 * Created:      7/08/02
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */
#include "common/Platform.h"  // 64-BIT

class AbortCallBack {
 public:
  virtual void doCallBack(const char *msg, const char *file, UInt32 line) = 0;
};
void registerAbortCallBack(AbortCallBack *pACB);
#endif
