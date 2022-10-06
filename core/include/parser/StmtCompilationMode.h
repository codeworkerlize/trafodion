#ifndef STMTCOMPILATIONMODE_H
#define STMTCOMPILATIONMODE_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtCompilationMode.h
 * Description:  class for parse node representing Static / Dynamic SQL
 *               Static allow host variable , but not param. Dynamic allow
 *               param, but not host variable.
 *
 * Created:      03/28/96
 * Language:     C++
 *

 *
 *****************************************************************************
 */

#include "export/NABasicObject.h"

enum WhoAmI {
  I_AM_UNKNOWN,
  I_AM_C_PREPROCESSOR,
  I_AM_COBOL_PREPROCESSOR,
  I_AM_ESP,
  I_AM_SQL_COMPILER,
  I_AM_EMBEDDED_SQL_COMPILER
};

struct IdentifyMyself {
 public:
  static WhoAmI GetMyName() { return myName_; }
  static void SetMyName(WhoAmI myname) { myName_ = myname; }

  static NABoolean IsPreprocessor() {
    return (GetMyName() == I_AM_COBOL_PREPROCESSOR || GetMyName() == I_AM_C_PREPROCESSOR);
  };

 private:
  static THREAD_P WhoAmI myName_;
};

#endif  // STMTCOMPILATIONMODE_H
