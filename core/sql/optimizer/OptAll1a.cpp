

#include "common/Platform.h"

#define SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#define SQLPARSERGLOBALS_FLAGS
#define SQLPARSERGLOBALS_NADEFAULTS


#include "RelExpr.cpp"
#include "RelPackedRows.cpp"
#include "RelSample.cpp"
#include "RelSequence.cpp"
#include "RelStoredProc.cpp"
#include "ReqGen.cpp"
#include "RETDesc.cpp"
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
#include "TriggerDB.cpp"
#include "optimizer/Triggers.h"
#include "OptTrigger.cpp"
#include "InliningInfo.cpp"
