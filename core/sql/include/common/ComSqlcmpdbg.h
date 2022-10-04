
#ifndef COMSQLCMPDBG_H
#define COMSQLCMPDBG_H


#include "common/Platform.h"

class ExScheduler;
class ExSubtask;

class Sqlcmpdbg {
  // This class exists merely to give a nice naming scope for this enum
 public:
  enum CompilationPhase {
    AFTER_PARSING,
    AFTER_BINDING,
    AFTER_TRANSFORMATION,
    AFTER_NORMALIZATION,
    AFTER_SEMANTIC_QUERY_OPTIMIZATION,
    DURING_MVQR,
    AFTER_ANALYZE,
    AFTER_OPT1,
    AFTER_OPT2,
    AFTER_PRECODEGEN,
    AFTER_CODEGEN,
    AFTER_TDBGEN,
    DURING_EXECUTION,
    DURING_MEMOIZATION,
    FROM_MSDEV
  };
};

struct SqlcmpdbgExpFuncs {
  void (*fpDisplayQueryTree)(Sqlcmpdbg::CompilationPhase, void *, void *);
  void (*fpSqldbgSetCmpPointers)(void *, void *, void *, void *, void *);
  void (*fpDoMemoStep)(Int32, Int32, Int32, void *, void *, void *);
  void (*fpDisplayTDBTree)(Sqlcmpdbg::CompilationPhase, void *, void *);
  int (*fpExecutionDisplayIsEnabled)(void);
  void (*fpSqldbgSetExePointers)(void *, void *, void *);
  void (*fpDisplayExecution)(ExSubtask **, ExScheduler *);
  void (*fpCleanUp)(void);
};

typedef SqlcmpdbgExpFuncs *(*fpGetSqlcmpdbgExpFuncs)();

#endif
