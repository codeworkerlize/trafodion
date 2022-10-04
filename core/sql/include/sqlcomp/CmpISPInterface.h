
#ifndef CMPISPINTERFACE_H
#define CMPISPINTERFACE_H

/* -*-C++-*-
******************************************************************************
*
* File:         CmpISPInterface.h
* Description:  Item expression base class ItemExpr
* Created:      3/26/2014
* Language:     C++
*
*
******************************************************************************/

#include "common/NABoolean.h"
#include "arkcmp/CmpISPStd.h"

class CmpISPInterface {
 public:
  CmpISPInterface();
  void InitISPFuncs();
  virtual ~CmpISPInterface();

 private:
  NABoolean initCalled_;
  SP_DLL_HANDLE handle_;
  CmpISPInterface(const CmpISPInterface &);
  const CmpISPInterface &operator=(const CmpISPInterface &);
};

extern CmpISPInterface cmpISPInterface;

#endif
