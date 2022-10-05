
#ifndef _EH_CALL_BACK_H_
#define _EH_CALL_BACK_H_
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         EHCallBack.cpp
 * Description:  Call back functions for exception handling
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
class EHCallBack {
 public:
  virtual void doFFDC() = 0;
  virtual void dumpDiags() = 0;
};

#endif
