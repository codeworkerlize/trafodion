
/* -*-C++-*-
****************************************************************************
*
* File:         ComTdbExe.cpp
* Description:  Implementation for those methods of ComTDB which have to
*               be implemented in the executor project because of their
*               dependencies on other executor objects.
*
* Created:      5/6/98
* Language:     C++
*
****************************************************************************
*/

#include "ExCancel.h"
#include "ExCompoundStmt.h"
#include "ExConnectBy.h"
#include "ExConnectByTempTable.h"
#include "executor/ExExplain.h"
#include "ExFirstN.h"
#include "executor/ExHbaseAccess.h"
#include "ExPack.h"
#include "ExPackedRows.h"
#include "ExProbeCache.h"
#include "ExSample.h"
#include "ExSequence.h"
#include "ExSimpleSample.h"
#include "ExTranspose.h"
#include "ExUdr.h"
#include "comexe/ComPackDefs.h"
#include "comexe/ComTdb.h"
#include "comexe/PartInputDataDesc.h"
#include "common/ExCollections.h"
#include "executor/ex_control.h"
#include "executor/ex_ddl.h"
#include "executor/ex_exe_stmt_globals.h"
#include "executor/ex_hash_grby.h"
#include "executor/ex_hashj.h"
#include "executor/ex_mj.h"
#include "executor/ex_onlj.h"
#include "executor/ex_root.h"
#include "executor/ex_send_bottom.h"
#include "executor/ex_send_top.h"
#include "ex_sort.h"
#include "executor/ex_sort_grby.h"
#include "executor/ex_split_bottom.h"
#include "executor/ex_split_top.h"
#include "executor/ex_stored_proc.h"
#include "ex_timeout.h"
#include "executor/ex_tuple.h"
#include "ex_tuple_flow.h"
#include "executor/ex_union.h"
#include "executor/ExExeUtil.h"
#include "executor/ExStats.h"
#include "executor/ex_stdh.h"
#include "executor/ex_tcb.h"
#include "executor/ex_transaction.h"

// -----------------------------------------------------------------------
// When called within tdm_sqlcli.dll, fixupVTblPtr() translates to a call
// to fix up the TDB's to the Executor version. This function is also
// defined in Generator.cpp. That code, however, fixes up the TDB's to
// the Compiler version.
// -----------------------------------------------------------------------
void ComTdb::fixupVTblPtr() { fixupVTblPtrExe(); }

char *ComTdb::findVTblPtr(short classID) { return findVTblPtrExe(classID); }

// -----------------------------------------------------------------------
// This method fixes up a TDB object which is retrieved from disk or
// received from another process to the Executor version of the TDB
// for its node type. There is a similar method called fixupVTblPtrCom()
// implemented in the comexe project which fixes up a TDB object to the
// Compiler version of the TDB.
// -----------------------------------------------------------------------
void ComTdb::fixupVTblPtrExe() {
  ex_assert(0, "fixupVTblPtrExe() shouldn't be called");  // method retired
}

// -----------------------------------------------------------------------
// This method returns the virtual function table pointer for an object
// with the given class ID as a "executor TDB" (the one with a build()
// method defined). There is a similar method called findVTblPtrCom()
// implemented in the comexe project (in ComTdb.cpp) which returns the
// pointer for an "compiler TDB".
// -----------------------------------------------------------------------
char *ComTdb::findVTblPtrExe(short classID) {
  char *vtblptr = NULL;
  switch (classID) {
    case ex_HASH_GRBY: {
      GetVTblPtr(vtblptr, ex_hash_grby_tdb);
      break;
    }

    case ex_SORT_GRBY: {
      GetVTblPtr(vtblptr, ex_sort_grby_tdb);
      break;
    }
    case ex_FIRST_N: {
      GetVTblPtr(vtblptr, ExFirstNTdb);
      break;
    }

    case ex_TRANSPOSE: {
      GetVTblPtr(vtblptr, ExTransposeTdb);
      break;
    }

    case ex_UNPACKROWS: {
      GetVTblPtr(vtblptr, ExUnPackRowsTdb);
      break;
    }

    case ex_PACKROWS: {
      GetVTblPtr(vtblptr, ExPackRowsTdb);
      break;
    }

    case ex_SAMPLE: {
      GetVTblPtr(vtblptr, ExSampleTdb);
      break;
    }

#if 0
// unused feature, done as part of SQ SQL code cleanup effort
    case ex_SIMPLE_SAMPLE:
    {
      GetVTblPtr(vtblptr,ExSimpleSampleTdb);
      break;
    }
#endif  // if 0

    case ex_LEAF_TUPLE: {
      GetVTblPtr(vtblptr, ExTupleLeafTdb);
      break;
    }

    case ex_COMPOUND_STMT: {
      GetVTblPtr(vtblptr, ExCatpoundStmtTdb);
      break;
    }

    case ex_NON_LEAF_TUPLE: {
      GetVTblPtr(vtblptr, ExTupleNonLeafTdb);
      break;
    }

    case ex_CONTROL_QUERY: {
      GetVTblPtr(vtblptr, ExControlTdb);
      break;
    }

    case ex_ROOT: {
      GetVTblPtr(vtblptr, ex_root_tdb);
      break;
    }

    case ex_ONLJ: {
      GetVTblPtr(vtblptr, ExOnljTdb);
      break;
    }

    case ex_HASHJ: {
      GetVTblPtr(vtblptr, ex_hashj_tdb);
      break;
    }

    case ex_MJ: {
      GetVTblPtr(vtblptr, ex_mj_tdb);
      break;
    }

    case ex_UNION: {
      GetVTblPtr(vtblptr, ex_union_tdb);
      break;
    }

    case ex_UDR: {
      GetVTblPtr(vtblptr, ExUdrTdb);
      break;
    }
    case ex_EXPLAIN: {
      GetVTblPtr(vtblptr, ExExplainTdb);
      break;
    }

    case ex_SEQUENCE_FUNCTION: {
      GetVTblPtr(vtblptr, ExSequenceTdb);
      break;
    }

    case ex_SORT: {
      GetVTblPtr(vtblptr, ExSortTdb);
      break;
    }

    case ex_SPLIT_TOP: {
      GetVTblPtr(vtblptr, ex_split_top_tdb);
      break;
    }

    case ex_SPLIT_BOTTOM: {
      GetVTblPtr(vtblptr, ex_split_bottom_tdb);
      break;
    }

    case ex_SEND_TOP: {
      GetVTblPtr(vtblptr, ex_send_top_tdb);
      break;
    }

    case ex_SEND_BOTTOM: {
      GetVTblPtr(vtblptr, ex_send_bottom_tdb);
      break;
    }

    case ex_STATS: {
      GetVTblPtr(vtblptr, ExStatsTdb);
      break;
    }

    case ex_STORED_PROC: {
      GetVTblPtr(vtblptr, ExStoredProcTdb);
      break;
    }

    case ex_TUPLE_FLOW: {
      GetVTblPtr(vtblptr, ExTupleFlowTdb);
      break;
    }

    case ex_SET_TIMEOUT: {
      GetVTblPtr(vtblptr, ExTimeoutTdb);
      break;
    }

    case ex_TRANSACTION: {
      GetVTblPtr(vtblptr, ExTransTdb);
      break;
    }

    case ex_DDL: {
      GetVTblPtr(vtblptr, ExDDLTdb);
      break;
    }

    case ex_DDL_WITH_STATUS: {
      GetVTblPtr(vtblptr, ExDDLwithStatusTdb);
      break;
    }

    case ex_DESCRIBE: {
      GetVTblPtr(vtblptr, ExDescribeTdb);
      break;
    }

    case ex_EXE_UTIL: {
      GetVTblPtr(vtblptr, ExExeUtilTdb);
      break;
    }

    case ex_PROCESS_VOLATILE_TABLE: {
      GetVTblPtr(vtblptr, ExProcessVolatileTableTdb);
      break;
    }

    case ex_LOAD_VOLATILE_TABLE: {
      GetVTblPtr(vtblptr, ExExeUtilLoadVolatileTableTdb);

      break;
    }

    case ex_CLEANUP_VOLATILE_TABLES: {
      GetVTblPtr(vtblptr, ExExeUtilCleanupVolatileTablesTdb);

      break;
    }

    case ex_GET_VOLATILE_INFO: {
      GetVTblPtr(vtblptr, ExExeUtilGetVolatileInfoTdb);

      break;
    }

    case ex_PROCESS_INMEMORY_TABLE: {
      GetVTblPtr(vtblptr, ExProcessInMemoryTableTdb);
      break;
    }

    case ex_CREATE_TABLE_AS: {
      GetVTblPtr(vtblptr, ExExeUtilCreateTableAsTdb);
      break;
    }

    case ex_GET_STATISTICS: {
      GetVTblPtr(vtblptr, ExExeUtilGetStatisticsTdb);

      break;
    }

    case ex_GET_METADATA_INFO: {
      GetVTblPtr(vtblptr, ExExeUtilGetMetadataInfoTdb);

      break;
    }

    case ex_GET_UID: {
      GetVTblPtr(vtblptr, ExExeUtilGetUIDTdb);

      break;
    }

    case ex_GET_QID: {
      GetVTblPtr(vtblptr, ExExeUtilGetQIDTdb);

      break;
    }

    case ex_POP_IN_MEM_STATS: {
      GetVTblPtr(vtblptr, ExExeUtilPopulateInMemStatsTdb);

      break;
    }

    case ex_DISPLAY_EXPLAIN: {
      GetVTblPtr(vtblptr, ExExeUtilDisplayExplainTdb);
      break;
    }

    case ex_DISPLAY_EXPLAIN_COMPLEX: {
      GetVTblPtr(vtblptr, ExExeUtilDisplayExplainComplexTdb);
      break;
    }

    case ex_PROBE_CACHE: {
      GetVTblPtr(vtblptr, ExProbeCacheTdb);

      break;
    }

    case ex_LONG_RUNNING: {
      GetVTblPtr(vtblptr, ExExeUtilLongRunningTdb);
      break;
    }

      // Suspend uses ExCancelTdb

    case ex_SHOW_SET: {
      GetVTblPtr(vtblptr, ExExeUtilShowSetTdb);

      break;
    }

    case ex_AQR: {
      GetVTblPtr(vtblptr, ExExeUtilAQRTdb);

      break;
    }

    case ex_GET_ERROR_INFO: {
      GetVTblPtr(vtblptr, ExExeUtilGetErrorInfoTdb);

      break;
    }
    case ex_PROCESS_STATISTICS: {
      GetVTblPtr(vtblptr, ExExeUtilGetProcessStatisticsTdb);
      break;
    }
    case ex_ARQ_WNR_INSERT: {
      GetVTblPtr(vtblptr, ExExeUtilAqrWnrInsertTdb);
      break;
    }

    case ex_HBASE_ACCESS: {
      GetVTblPtr(vtblptr, ExHbaseAccessTdb);

      break;
    }

    case ex_HBASE_COPROC_AGGR: {
      GetVTblPtr(vtblptr, ExHbaseCoProcAggrTdb);

      break;
    }

    case ex_HBASE_LOAD: {
      GetVTblPtr(vtblptr, ExExeUtilHBaseBulkLoadTdb);

      break;
    }

    case ex_CANCEL: {
      GetVTblPtr(vtblptr, ExCancelTdb);

      break;
    }

    case ex_REGION_STATS: {
      GetVTblPtr(vtblptr, ExExeUtilRegionStatsTdb);

      break;
    }

    case ex_CONNECT_BY: {
      GetVTblPtr(vtblptr, ExConnectByTdb);

      break;
    }

    case ex_CONNECT_BY_TEMP_TABLE: {
      GetVTblPtr(vtblptr, ExConnectByTempTableTdb);

      break;
    }

    case ex_COMPOSITE_UNNEST: {
      GetVTblPtr(vtblptr, ExExeUtilCompositeUnnestTdb);

      break;
    }

    case ex_GET_OBJECT_EPOCH_STATS: {
      GetVTblPtr(vtblptr, ExExeUtilGetObjectEpochStatsTdb);

      break;
    }

    case ex_GET_OBJECT_LOCK_STATS: {
      GetVTblPtr(vtblptr, ExExeUtilGetObjectLockStatsTdb);

      break;
    }

    case ex_QUERY_INVALIDATION: {
      GetVTblPtr(vtblptr, ExQryInvalidStatsTdb);
      break;
    }

    case ex_SNAPSHOT_UPDATE_DELETE: {
      GetVTblPtr(vtblptr, ExExeUtilUpdataDeleteTdb);
      break;
    }

    default:
      ex_assert(0, "findVTblPtrExe(): Cannot find entry of this ClassId");
      break;
  }
  return vtblptr;
}

void fixupExeVtblPtr(ComTdb *tdb) {
  for (int i = 0; i < tdb->numChildren(); i++) {
    fixupExeVtblPtr((ComTdb *)(tdb->getChild(i)));
  }

  char *vtblPtr = NULL;
  vtblPtr = tdb->findVTblPtrExe(tdb->getClassID());
  tdb->setVTblPtr(vtblPtr);
}

void fixupComVtblPtr(ComTdb *tdb) {
  for (int i = 0; i < tdb->numChildren(); i++) {
    fixupComVtblPtr((ComTdb *)(tdb->getChild(i)));
  }

  char *vtblPtr = NULL;
  vtblPtr = tdb->findVTblPtrCom(tdb->getClassID());
  tdb->setVTblPtr(vtblPtr);
}

void resetBufSize(ex_tcb *tcb, int &tcbSpaceNeeded, int &poolSpaceNeeded) {
  for (int i = 0; i < tcb->numChildren(); i++) {
    resetBufSize((ex_tcb *)(tcb->getChild(i)), tcbSpaceNeeded, poolSpaceNeeded);
  }

  if (tcb->getPool()) {
    int numBuffs = -1;
    UInt32 staticPoolSpaceSize = 0;
    UInt32 dynPoolSpaceSize = 0;

    // compute total pool space needed(static + dynamic) at runtime
    tcb->computeNeededPoolInfo(numBuffs, staticPoolSpaceSize, dynPoolSpaceSize);

    // size of pool space that was allocated during the build
    // phase. This value is included in tcbSpaceNeeded.
    // compute tcb space needed by subtracting staticPoolSize
    tcbSpaceNeeded -= staticPoolSpaceSize;

    poolSpaceNeeded += dynPoolSpaceSize;

    if ((tcb->resizePoolInfo()) && (numBuffs >= 0))
      ((ComTdb *)(tcb->getTdb()))->resetBufInfo(numBuffs, (staticPoolSpaceSize + dynPoolSpaceSize));
  }
}

int getTotalTcbSpace(char *inTdb, char *otherInfo, char *parentMemory) {
  ComTdb *tdb = (ComTdb *)inTdb;

  Space space(Space::EXECUTOR_SPACE);
  space.setParent((NAHeap *)parentMemory);
  //  space.setType(Space::SYSTEM_SPACE);
  //  space.setType(Space::EXECUTOR_SPACE);

  //  NAHeap heap("temp heap", NAMemory::DERIVED_FROM_SYS_HEAP);
  // NAHeap heap("temp heap", NAMemory::EXECUTOR_MEMORY);
  NAHeap heap("temp heap", (NAHeap *)parentMemory, 32000 /*blocksize*/);
  ex_globals *g = NULL;

  switch (tdb->getNodeType()) {
      // This method is called from an internal CLI call and the only palce
      // where this internal CLI call is make is from generator for Generator.cpp
      // for EID Root tdb and so this code is not called for other tdb's
    case ComTdb::ex_ROOT:
      g = new (&heap) ExMasterStmtGlobals(10, NULL, NULL, 0, &space, &heap);
      break;

    case ComTdb::ex_SPLIT_TOP:
      g = new (&heap) ExEspStmtGlobals(10, NULL, 0, &space, &heap, NULL, NullFragInstanceHandle, 0, FALSE);
      break;
    case ComTdb::ex_EXPLAIN:
      break;

    default:
      break;
  }

  g->setComputeSpace(TRUE);

  fixupExeVtblPtr(tdb);
  ex_tcb *tcb = tdb->build(g);
  fixupComVtblPtr(tdb);

  int totalSpaceNeeded = 0;
  int tcbSpaceNeeded = 0;
  int poolSpaceNeeded = 0;
  tcbSpaceNeeded = space.getAllocatedSpaceSize();

  resetBufSize(tcb, tcbSpaceNeeded, poolSpaceNeeded);

  // add a 25% fudge factor to TCB space
  tcbSpaceNeeded = (tcbSpaceNeeded * 125) / 100;

  totalSpaceNeeded = tcbSpaceNeeded + poolSpaceNeeded;

  return totalSpaceNeeded;
}
