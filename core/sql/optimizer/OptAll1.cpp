

#include "common/Platform.h"

#define SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#define SQLPARSERGLOBALS_FLAGS
#define SQLPARSERGLOBALS_NADEFAULTS

// files below are commented out and are in optall.cpp


#include "CacheWA.cpp"

#include "IndexDesc.cpp"

#include "OptItemExpr.cpp"
#include "OptLogRelExpr.cpp"
#include "OptPhysRelExpr.cpp"
#include "PackedColDesc.cpp"
#include "PartFunc.cpp"
#include "PartKeyDist.cpp"
#include "PartReq.cpp"
#include "PhyProp.cpp"

#include "Rel3GL.cpp"
#include "RelCache.cpp"
#include "RelDCL.cpp"
//#include "RelExeUtil.cpp"
#include "RelExpr.cpp"
#include "RelPackedRows.cpp"
// Moved to OptAll2.cpp #include "RelRoutine.cpp"
#include "RelSample.cpp"
#include "RelSequence.cpp"
#include "RelStoredProc.cpp"
#include "ReqGen.cpp"
#include "RETDesc.cpp"
// Moved to OptAll2.cpp #include "RoutineDesc.cpp"
#include "Rule.cpp"
#include "ScanOptimizer.cpp"
#include "SchemaDB.cpp"
#include "ScmCostMethod.cpp"
#include "SearchKey.cpp"
#include "SimpleScanOptimizer.cpp"
#include "Stats.cpp"
#include "SynthType.cpp"
#include "TableDesc.cpp"
#include "TableNameMap.cpp"
#include "tasks.cpp"
// need reordering
#include "AccessSets.cpp"
// Temporary changes
//   OptAll1.cpp is getting too large.
//   c89 with optimize=0 option hits a limit.
//   Temporarily move a few files from OptAll1.cpp
//   to w:/nskgmake/optimizer/Makefile for the
//   yosrelease build to succeed.  The Optimizer
//   group will provide a more permanent solution.
#if 0  // temporary
#include "TriggerDB.cpp"
#include "optimizer/Triggers.h"
#include "OptTrigger.cpp"
#include "InliningInfo.cpp"
#endif  // 0 - temporary
