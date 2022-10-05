

#include "common/Platform.h"

#define SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#define SQLPARSERGLOBALS_FLAGS
#define SQLPARSERGLOBALS_NADEFAULTS

#include "Analyzer.cpp"
#include "AppliedStatMan.cpp"
#include "BindItemExpr.cpp"
#include "BindRelExpr.cpp"
#include "BindRI.cpp"
#include "BindWA.cpp"
#include "BinderUtils.cpp"
#include "CacheWA.cpp"
#include "CascadesBasic.cpp"
#include "ChangesTable.cpp"
#include "ColStatDesc.cpp"
#include "ColumnDesc.cpp"
#include "ControlDB.cpp"
#include "Cost.cpp"
#include "costmethod.cpp"
#include "CostScalar.cpp"
#include "DomainDesc.cpp"
#include "EncodedValue.cpp"
#include "EstLogProp.cpp"
#include "GroupAttr.cpp"
#include "ImplRule.cpp"
#include "IndexDesc.cpp"
#include "ItemCache.cpp"
#include "ItemExpr.cpp"
#include "ItemExprList.cpp"
#include "ItemSample.cpp"
#include "ItmBitMuxFunction.cpp"
#include "ItmFlowControlFunction.cpp"
#include "LargeScopeRules.cpp"

#include "OptHints.cpp"
