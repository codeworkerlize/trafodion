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

CPPSRC := GenBitMuxFunction.cpp \
	Generator.cpp \
	GenExpGenerator.cpp \
	GenExplain.cpp \
	GenFlowControlFunction.cpp \
	GenItemComposite.cpp \
	GenItemExpr.cpp \
	GenItemFunc.cpp \
	GenItemSampling.cpp \
	GenKey.cpp \
	GenMapTable.cpp \
	GenMdamPred.cpp \
	GenPartFunc.cpp \
	GenPreCode.cpp \
	GenProbeCache.cpp \
	GenRel3GL.cpp \
	GenRelDCL.cpp \
	GenRelEnforcer.cpp \
	GenRelExeUtil.cpp \
	GenRelExpr.cpp \
	GenRelGrby.cpp \
	GenRelJoin.cpp \
	GenRelMisc.cpp \
	GenRelPackedRows.cpp \
	GenRelSample.cpp \
	GenRelScan.cpp \
	GenRelSequence.cpp \
	GenRelSet.cpp \
	GenRelUpdate.cpp \
	GenResources.cpp \
	GenSequenceFunction.cpp \
	GenShape.cpp \
	GenStoredProc.cpp \
	GenUdr.cpp \
        GenFastTransport.cpp \
        CmUtil.cpp \
	LmExpr.cpp \
	LmGenUtil.cpp \
	vers_libgenerator.cpp

DEFS := -DNDEBUG

SRCPATH := generator langman smdio

# include the following line to trace expression evaluation, see exp_expr.h and 
# exp_eval.cpp
ifeq ($(TRACE_EXPR_EVAL),1)
   DEFS += -DTRACE_EXPR_EVAL
endif

