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
#
# do not remove this line since we have to un-define CCSRC 
# which holds a list of cc files to build the parquet reader.
# The line below was copied from orc/Makefile because orc is not being built
CCSRC :=

CPPSRC := CmpMessage.cpp \
	ComASNodes.cpp \
	ComExtractInfo.cpp \
	ComKeyMDAM.cpp \
	ComKeyRange.cpp \
	ComKeySingleSubset.cpp \
	ComQueue.cpp \
	ComResourceInfo.cpp \
	ComTdb.cpp \
	ComTdbCompoundStmt.cpp \
	ComTdbControl.cpp \
	ComTdbDDL.cpp \
	ComTdbExeUtil.cpp \
	ComTdbExplain.cpp \
	ComTdbFirstN.cpp \
	ComTdbConnectBy.cpp \
	ComTdbConnectByTempTable.cpp \
	ComTdbHashGrby.cpp \
	ComTdbHashj.cpp \
	ComTdbHbaseAccess.cpp \
	ComTdbMj.cpp \
        ComTdbQI.cpp \
	ComTdbOnlj.cpp \
	ComTdbPackRows.cpp \
	ComTdbProbeCache.cpp \
	ComTdbRoot.cpp \
	ComTdbSample.cpp \
	ComTdbSendBottom.cpp \
	ComTdbSendTop.cpp \
	ComTdbSequence.cpp \
	ComTdbSort.cpp \
	ComTdbSortGrby.cpp \
	ComTdbSplitBottom.cpp \
	ComTdbSplitTop.cpp \
	ComTdbStats.cpp \
	ComTdbStoredProc.cpp \
	ComTdbTimeout.cpp \
	ComTdbTransaction.cpp \
	ComTdbTranspose.cpp \
	ComTdbTuple.cpp \
	ComTdbTupleFlow.cpp \
	ComTdbUdr.cpp \
	ComTdbUnion.cpp \
	ComTdbUnPackRows.cpp \
	ExplainTuple.cpp \
	FragDir.cpp \
	FragmentDir.cpp \
	LateBindInfo.cpp \
	NAExecTrans.cpp \
	PartInputDataDesc.cpp \
	UdrFormalParamInfo.cpp \
	udrtabledescinfo.cpp  \
	ComTrace.cpp \
	ComTdbCancel.cpp \
	ComCompressionInfo.cpp 

CPPSRC += vers_libcomexe.cpp

# include the following line to trace expression evaluation, see exp_expr.h and 
# exp_eval.cpp
ifeq ($(TRACE_EXPR_EVAL),1)
   DEFS += -DTRACE_EXPR_EVAL
endif
