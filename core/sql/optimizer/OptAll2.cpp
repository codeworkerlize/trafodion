

#include "common/Platform.h"

#define SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#define SQLPARSERGLOBALS_FLAGS
#define SQLPARSERGLOBALS_NADEFAULTS


#include "MJVIndexBuilder.cpp"
#include "MultiJoin.cpp"
#include "mdam.cpp"
#include "memo.cpp"
#include "QRDescGenerator.cpp"
#include "OptRange.cpp"
#include "QRDescriptorExtentions.cpp"
#include "RelRoutine.cpp"
#include "RoutineDesc.cpp"

#include "RelExeUtil.cpp"

// files below are commented out and are in optall1.cpp

#include "NAClusterInfo.cpp"
#include "NAColumn.cpp"
#include "NAFileSet.cpp"
#include "NARoutine.cpp"
#include "NATable.cpp"
#include "NodeMap.cpp"
#include "NormItemExpr.cpp"
#include "NormRelExpr.cpp"
#include "NormWA.cpp"

// files below used to be in OptAll1.cpp

#include "ObjectNames.cpp"

