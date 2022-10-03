
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
