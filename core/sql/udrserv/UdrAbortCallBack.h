
#ifndef _UDR_ABORT_CALL_BACK_H_
#define _UDR_ABORT_CALL_BACK_H_
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         UdrAbortCallBack.h
 * Description:  abort call back functions from the UDR server
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
#include "AbortCallBack.h"
class UdrAbortCallBack : public AbortCallBack {
 public:
  virtual void doCallBack(const char *msg, const char *file, UInt32 line);
};
#endif
