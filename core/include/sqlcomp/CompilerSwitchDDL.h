

#ifndef _COMPILER_SWITCH_DDL_H_
#define _COMPILER_SWITCH_DDL_H_

#include "common/Platform.h"
#include "cli/Context.h"
#include "arkcmp/CmpContext.h"

// *****************************************************************************
// *
// * File:         CompilerSwitchDDL.h
// * Description:  the class responsible for compiler instance
// *               switching on behave of DDL operations.
// *
// * Contents:
// *
// *****************************************************************************

class CompilerSwitchDDL {
 public:
  CompilerSwitchDDL();
  ~CompilerSwitchDDL(){};

  short switchCompiler(int cntxtType = CmpContextInfo::CMPCONTEXT_TYPE_META);

  short switchBackCompiler();

 protected:
  void setAllFlags();
  void saveAllFlags();
  void restoreAllFlags();
  short sendAllControlsAndFlags(CmpContext *prevContext, int cntxtType);
  void restoreAllControlsAndFlags();

 protected:
  NABoolean cmpSwitched_;
  int savedCmpParserFlags_;
  int savedCliParserFlags_;
};

#endif
