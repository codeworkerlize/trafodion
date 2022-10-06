
#ifndef _CMP_EH_CALL_BACK_H_
#define _CMP_EH_CALL_BACK_H_
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         CmpEHCallBack.cpp
 * Description:  Compiler Call back functions for exception handling
 *
 *
 * Created:      5/4/02
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */
#include "EHCallBack.h"
#include "common/Platform.h"

class CmpEHCallBack : public EHCallBack {
 public:
  virtual void doFFDC();
  virtual void dumpDiags();
};

void makeTFDSCall(const char *msg, const char *file, UInt32 line);

void printSignalHandlers();

#endif
