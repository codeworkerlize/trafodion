

#include "common/Platform.h"

#define SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#define SQLPARSERGLOBALS_FLAGS
#define SQLPARSERGLOBALS_NADEFAULTS

// This file includes some files that either benefit by being compiled as
// one unit on Windows and Linux or that are required to be compiled as
// one unit.

#include "SimpleScanOptimizer.cpp"
#include "ScmCostMethod.cpp"
