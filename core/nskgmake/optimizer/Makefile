#######################################################################
# @@@ START COPYRIGHT @@@
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# @@@ END COPYRIGHT @@@
#######################################################################

INCLUDE_DIRS += sqludr
# Define the correct compilation and linking flags depending on whether
# this is a debug or release build.
ifeq ($(FLAVOR),debug)
   # for coverage checking support
   ifeq ($(SQ_COVERAGE_OPTIMIZER),1)
      CXXFLAGS += --coverage
   endif
endif

#GUI Debugger
# Like in sqlcomp, enable this unconditionally, to be able to display in release mode
# if the necessary DLL is present
CXXFLAGS += -DNA_DEBUG_GUI

SYS_LIBS += -L$(LIBROOT)

ifdef BUILD_ORC_READER
SYS_LIBS += -lorc 
endif

SRCPATH += common ../sqf/monitor/linux

CPPSRC := \
	AccessSets.cpp \
	Analyzer.cpp \
	AppliedStatMan.cpp \
	BindItemExpr.cpp \
	BindRelExpr.cpp \
	BindRI.cpp \
	BindWA.cpp \
	BinderUtils.cpp \
	CacheWA.cpp \
	CascadesBasic.cpp \
	ChangesTable.cpp \
	CmpMemoryMonitor.cpp \
	ColStatDesc.cpp \
	ColumnDesc.cpp \
	CmpProcess.cpp \
	CompilerTracking.cpp \
	CompilationStats.cpp \
	ControlDB.cpp \
	Cost.cpp \
	costmethod.cpp \
	CostScalar.cpp \
	DomainDesc.cpp \
	EncodedKeyValue.cpp \
	EncodedValue.cpp \
	EstLogProp.cpp \
	GroupAttr.cpp \
	ImplRule.cpp \
	IndexDesc.cpp \
	Inlining.cpp \
	InliningInfo.cpp \
	ItemCache.cpp \
        ItemComposite.cpp \
	ItemExpr.cpp \
	ItemExprList.cpp \
	ItemSample.cpp \
	ItmBitMuxFunction.cpp \
	ItmFlowControlFunction.cpp \
	LargeScopeRules.cpp \
	MJVIndexBuilder.cpp \
	MultiJoin.cpp \
	mdam.cpp \
	memo.cpp \
        NAPartition.cpp \
	NAColumn.cpp \
	NAFileSet.cpp \
	NARoutine.cpp \
	NATable.cpp \
	NodeMap.cpp \
	NormItemExpr.cpp \
	NormRelExpr.cpp \
	NormWA.cpp \
	ObjectNames.cpp \
	opt.cpp \
	OptHints.cpp \
	OptimizerSimulator.cpp \
	OptItemExpr.cpp \
	OptLogRelExpr.cpp \
	OptPhysRelExpr.cpp \
	OptRange.cpp \
	OptTrigger.cpp \
	PackedColDesc.cpp \
	PartFunc.cpp \
	PartKeyDist.cpp \
	PartReq.cpp \
	PhyProp.cpp \
	QRDescGenerator.cpp \
	QRDescriptorExtentions.cpp \
	Rel3GL.cpp \
	RelCache.cpp \
	RelDCL.cpp \
	RelExeUtil.cpp \
	RelExpr.cpp \
	RelPackedRows.cpp \
	RelRoutine.cpp \
	RelSample.cpp \
	RelSequence.cpp \
	RelStoredProc.cpp \
	ReqGen.cpp \
	RETDesc.cpp \
	RoutineDesc.cpp \
	Rule.cpp \
	ScanOptimizer.cpp \
	SchemaDB.cpp \
	ScmCostMethod.cpp \
	SearchKey.cpp \
	SimpleScanOptimizer.cpp \
	Stats.cpp \
	SynthType.cpp \
	TableDesc.cpp \
	TableNameMap.cpp \
	tasks.cpp \
	TransRule.cpp \
	UdfDllInteraction.cpp \
	ValueDesc.cpp \
	TriggerDB.cpp \
	Triggers.cpp \
	VEGTable.cpp \
	HbaseSearchSpec.cpp \
	vers_liboptimizer.cpp \
	orcPushdownPredInfo.cpp \
	licensecommon.cpp


# include the following line to trace expression evaluation, see exp_expr.h and 
# exp_eval.cpp
ifeq ($(TRACE_EXPR_EVAL),1)
   DEFS += -DTRACE_EXPR_EVAL
endif

