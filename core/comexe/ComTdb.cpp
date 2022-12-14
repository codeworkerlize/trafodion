

#include "common/Platform.h"

// -----------------------------------------------------------------------
// TBM after completion of decoupling work.
// -----------------------------------------------------------------------
#include "comexe/ComPackDefs.h"
#include "comexe/ComTdb.h"
#include "exp/exp_clause_derived.h"
#include "exp/exp_stdh.h"
#include "float.h"

// -----------------------------------------------------------------------
// Inclusion of all subclasses header needed for fixupVTblPtrCom().
// -----------------------------------------------------------------------
#include "comexe/ComTdbAll.h"

// This method is called only if params parameter is sent to ComTdb constructor
// and no where in the code the ComTdb constructor is being called with
// params paramter.  It was tested by removing the the param parameter
// from the cosntructor and compiling.
void ComTdbParams::getValues(Cardinality &estimatedRowCount, ExCriDescPtr &criDown, ExCriDescPtr &criUp,
                             queue_index &sizeDown, queue_index &sizeUp, int &numBuffers, UInt32 &bufferSize,
                             int &firstNRows) {
  estimatedRowCount = estimatedRowCount_;
  criDown = criDown_;
  criUp = criUp_;
  sizeDown = sizeDown_;
  sizeUp = sizeUp_;
  numBuffers = numBuffers_;
  firstNRows = firstNRows_;
}

// -----------------------------------------------------------------------
// TDB constructor & Destructor
// -----------------------------------------------------------------------
ComTdb::ComTdb(ex_node_type type, const char *eye, Cardinality estRowsUsed, ex_cri_desc *criDown, ex_cri_desc *criUp,
               queue_index sizeDown, queue_index sizeUp, int numBuffers, UInt32 bufferSize, int uniqueId,
               int initialQueueSizeDown, int initialQueueSizeUp, short queueResizeLimit, short queueResizeFactor,
               ComTdbParams *params)
    : criDescDown_(criDown),
      criDescUp_(criUp),
      queueSizeDown_(sizeDown),
      queueSizeUp_(sizeUp),
      numBuffers_(numBuffers),
      bufferSize_(bufferSize),
      expressionMode_(ex_expr::PCODE_NONE),
      flags_(0),
      tdbId_(uniqueId),
      initialQueueSizeDown_(initialQueueSizeDown),
      initialQueueSizeUp_(initialQueueSizeUp),
      queueResizeLimit_(queueResizeLimit),
      queueResizeFactor_(queueResizeFactor),
      NAVersionedObject(type),
      firstNRows_(-1),
      overflowMode_(OFM_DISK),
      recordLength_(0) {
  // ---------------------------------------------------------------------
  // Copy the eye catcher
  // ---------------------------------------------------------------------
  str_cpy_all((char *)&eyeCatcher_, eye, 4);

  // This constructor is never called with params parameter
  if (params) {
    params->getValues(estRowsUsed, criDescDown_, criDescUp_, queueSizeDown_, queueSizeUp_, numBuffers_, bufferSize_,
                      firstNRows_);
  }

  collectStatsType_ = NO_STATS;

  estRowsUsed_ = estRowsUsed;

  // any float field in child TDB is to be interpreted as IEEE float.
  flags_ |= FLOAT_FIELDS_ARE_IEEE;
}

ComTdb::ComTdb() : NAVersionedObject(-1), overflowMode_(OFM_DISK), recordLength_(0) {}

ComTdb::~ComTdb() {
  // ---------------------------------------------------------------------
  // Change the eye catcher
  // ---------------------------------------------------------------------
  str_cpy_all((char *)&eyeCatcher_, eye_FREE, 4);
}

Long ComTdb::pack(void *space) {
  criDescDown_.pack(space);
  criDescUp_.pack(space);
  parentTdb_.pack(space);

  return NAVersionedObject::pack(space);
}

int ComTdb::unpack(void *base, void *reallocator) {
  if (criDescDown_.unpack(base, reallocator)) return -1;
  if (criDescUp_.unpack(base, reallocator)) return -1;
  if (parentTdb_.unpack(base, reallocator)) return -1;
  return NAVersionedObject::unpack(base, reallocator);
}

// -----------------------------------------------------------------------
// Used by the internal SHOWPLAN command to get attributes of a TDB in a
// string.
// -----------------------------------------------------------------------
void ComTdb::displayContents(Space *space, int flag) {
  char buf[100];
  str_sprintf(buf, "Contents of %s [%d]:", getNodeName(), getExplainNodeId());
  int j = str_len(buf);
  space->allocateAndCopyToAlignedSpace(buf, j, sizeof(short));
  for (int k = 0; k < j; k++) buf[k] = '-';
  buf[j] = '\n';
  buf[j + 1] = 0;
  space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

  if (flag & 0x00000008) {
    str_sprintf(buf, "For ComTdb :");
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    str_sprintf(buf, "Class Version = %d, Class Size = %d", getClassVersionID(), getClassSize());
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    str_sprintf(buf, "InitialQueueSizeDown = %d, InitialQueueSizeUp = %d", getInitialQueueSizeDown(),
                getInitialQueueSizeUp());
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    str_sprintf(buf, "queueResizeLimit = %d, queueResizeFactor = %d", getQueueResizeLimit(), getQueueResizeFactor());
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    str_sprintf(buf, "queueSizeDown = %d, queueSizeUp = %d, numBuffers = %d, bufferSize = %d", getMaxQueueSizeDown(),
                getMaxQueueSizeUp(), numBuffers_, bufferSize_);
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    str_sprintf(buf, "estimatedRowUsed = %0.0f, estimatedRowsAccessed = %0.0f, expressionMode = %d", estRowsUsed_,
                estRowsAccessed_, expressionMode_);
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    str_sprintf(buf, "Flag = %#x", flags_);
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    if (criDescDown_ && criDescUp_) {
      str_sprintf(buf, "criDescDown_->noTuples() = %d, criDescUp_->noTuples() = %d", criDescDown_->noTuples(),
                  criDescUp_->noTuples());
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    }

    if (firstNRows() >= 0) {
      str_sprintf(buf, "Request Type: GET_N (%d) ", firstNRows());
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    }
  }

  if (flag & 0x00000001) {
    displayExpression(space, flag);
    displayChildren(space, flag);
  }
}

void ComTdb::displayExpression(Space *space, int flag) {
  char buf[100];

  str_sprintf(buf, "\n# of Expressions = %d\n", numExpressions());
  space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

  if (flag & 0x00000006) {
    for (int i = 0; i < numExpressions(); i++) {
      if (getExpressionNode(i))
        getExpressionNode(i)->displayContents(space, expressionMode_, (char *)getExpressionName(i), flag);
      else {
        str_sprintf(buf, "Expression: %s is NULL\n", (char *)getExpressionName(i));
        space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
      }
    }
  } else {
    for (int i = 0; i < numExpressions(); i++) {
      if (getExpressionNode(i)) {
        str_sprintf(buf, "Expression: %s is not NULL", (char *)getExpressionName(i));
        space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
      } else {
        str_sprintf(buf, "Expression: %s is NULL", (char *)getExpressionName(i));
        space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
      }
    }
  }
}

void ComTdb::displayChildren(Space *space, int flag) {
  for (int i = 0; i < numChildren(); i++) {
    // currTdb->getChildForGUI(i)->displayContents(space);
    if (getChild(i)) ((ComTdb *)getChild(i))->displayContents(space, flag);
  }
}

// -----------------------------------------------------------------------
// This method fixes up a TDB object which is retrieved from disk or
// received from another process to the Compiler-aware version of the TDB
// for its node type. There is a similar method called fixupVTblPtrExe()
// implemented in the executor project which fixes up a TDB object to the
// Executor version of the TDB.
// -----------------------------------------------------------------------
void ComTdb::fixupVTblPtrCom() {}

// -----------------------------------------------------------------------
// This method returns the virtual function table pointer for an object
// with the given class ID as a "compiler TDB" (the one without a build()
// method defined). There is a similar method called findVTblPtrExe()
// implemented in the executor project (in ExComTdb.cpp) which returns
// the pointer for an "executor TDB".
// -----------------------------------------------------------------------
char *ComTdb::findVTblPtrCom(short classID) {
  char *vtblptr = NULL;
  switch (classID) {
    case ex_FIRST_N: {
      GetVTblPtr(vtblptr, ComTdbFirstN);
      break;
    }

    case ex_HASH_GRBY: {
      GetVTblPtr(vtblptr, ComTdbHashGrby);
      break;
    }

    case ex_SORT_GRBY: {
      GetVTblPtr(vtblptr, ComTdbSortGrby);
      break;
    }

    case ex_TRANSPOSE: {
      GetVTblPtr(vtblptr, ComTdbTranspose);
      break;
    }

    case ex_UNPACKROWS: {
      GetVTblPtr(vtblptr, ComTdbUnPackRows);
      break;
    }

    case ex_PACKROWS: {
      GetVTblPtr(vtblptr, ComTdbPackRows);
      break;
    }

    case ex_SAMPLE: {
      GetVTblPtr(vtblptr, ComTdbSample);
      break;
    }

    case ex_LEAF_TUPLE: {
      GetVTblPtr(vtblptr, ComTdbTupleLeaf);
      break;
    }

    case ex_NON_LEAF_TUPLE: {
      GetVTblPtr(vtblptr, ComTdbTupleNonLeaf);
      break;
    }

    case ex_COMPOUND_STMT: {
      GetVTblPtr(vtblptr, ComTdbCompoundStmt);
      break;
    }

    case ex_CONNECT_BY: {
      GetVTblPtr(vtblptr, ComTdbConnectBy);
      break;
    }

    case ex_CONNECT_BY_TEMP_TABLE: {
      GetVTblPtr(vtblptr, ComTdbConnectByTempTable);
      break;
    }

    case ex_TUPLE: {
      GetVTblPtr(vtblptr, ComTdbTuple);
      break;
    }

    case ex_SEQUENCE_FUNCTION: {
      GetVTblPtr(vtblptr, ComTdbSequence);
      break;
    }

    case ex_CONTROL_QUERY: {
      GetVTblPtr(vtblptr, ComTdbControl);
      break;
    }

    case ex_ROOT: {
      GetVTblPtr(vtblptr, ComTdbRoot);
      break;
    }

    case ex_ONLJ: {
      GetVTblPtr(vtblptr, ComTdbOnlj);
      break;
    }

    case ex_HASHJ: {
      GetVTblPtr(vtblptr, ComTdbHashj);
      break;
    }

    case ex_MJ: {
      GetVTblPtr(vtblptr, ComTdbMj);
      break;
    }

    case ex_UNION: {
      GetVTblPtr(vtblptr, ComTdbUnion);
      break;
    }

    case ex_EXPLAIN: {
      GetVTblPtr(vtblptr, ComTdbExplain);
      break;
    }

#if 0
// unused feature, done as part of SQ SQL code cleanup effort
    case ex_SEQ:
    {
      GetVTblPtr(vtblptr,ComTdbSeq);
      break;
    }
#endif  // if 0

    case ex_SORT: {
      GetVTblPtr(vtblptr, ComTdbSort);
      break;
    }

    case ex_SPLIT_TOP: {
      GetVTblPtr(vtblptr, ComTdbSplitTop);
      break;
    }

    case ex_SPLIT_BOTTOM: {
      GetVTblPtr(vtblptr, ComTdbSplitBottom);
      break;
    }

    case ex_SEND_TOP: {
      GetVTblPtr(vtblptr, ComTdbSendTop);
      break;
    }

    case ex_SEND_BOTTOM: {
      GetVTblPtr(vtblptr, ComTdbSendBottom);
      break;
    }

    case ex_STATS: {
      GetVTblPtr(vtblptr, ComTdbStats);
      break;
    }

    case ex_STORED_PROC: {
      GetVTblPtr(vtblptr, ComTdbStoredProc);
      break;
    }

    case ex_TUPLE_FLOW: {
      GetVTblPtr(vtblptr, ComTdbTupleFlow);
      break;
    }

    case ex_TRANSACTION: {
      GetVTblPtr(vtblptr, ComTdbTransaction);
      break;
    }

    case ex_DDL: {
      GetVTblPtr(vtblptr, ComTdbDDL);
      break;
    }

    case ex_DDL_WITH_STATUS: {
      GetVTblPtr(vtblptr, ComTdbDDLwithStatus);
      break;
    }

    case ex_DESCRIBE: {
      GetVTblPtr(vtblptr, ComTdbDescribe);
      break;
    }

    case ex_EXE_UTIL: {
      GetVTblPtr(vtblptr, ComTdbExeUtil);
      break;
    }

    case ex_MAINTAIN_OBJECT: {
      GetVTblPtr(vtblptr, ComTdbExeUtilMaintainObject);
      break;
    }

    case ex_LONG_RUNNING: {
      GetVTblPtr(vtblptr, ComTdbExeUtilLongRunning);
      break;
    }

    case ex_DISPLAY_EXPLAIN: {
      GetVTblPtr(vtblptr, ComTdbExeUtilDisplayExplain);
      break;
    }

    case ex_DISPLAY_EXPLAIN_COMPLEX: {
      GetVTblPtr(vtblptr, ComTdbExeUtilDisplayExplainComplex);
      break;
    }

    case ex_LOAD_VOLATILE_TABLE: {
      GetVTblPtr(vtblptr, ComTdbExeUtilLoadVolatileTable);
      break;
    }

    case ex_PROCESS_VOLATILE_TABLE: {
      GetVTblPtr(vtblptr, ComTdbProcessVolatileTable);
      break;
    }

    case ex_CLEANUP_VOLATILE_TABLES: {
      GetVTblPtr(vtblptr, ComTdbExeUtilCleanupVolatileTables);
      break;
    }

    case ex_GET_VOLATILE_INFO: {
      GetVTblPtr(vtblptr, ComTdbExeUtilGetVolatileInfo);
      break;
    }

    case ex_PROCESS_INMEMORY_TABLE: {
      GetVTblPtr(vtblptr, ComTdbProcessInMemoryTable);
      break;
    }

    case ex_CREATE_TABLE_AS: {
      GetVTblPtr(vtblptr, ComTdbExeUtilCreateTableAs);
      break;
    }

    case ex_GET_STATISTICS: {
      GetVTblPtr(vtblptr, ComTdbExeUtilGetStatistics);
      break;
    }

    case ex_GET_METADATA_INFO: {
      GetVTblPtr(vtblptr, ComTdbExeUtilGetMetadataInfo);
      break;
    }

    case ex_GET_UID: {
      GetVTblPtr(vtblptr, ComTdbExeUtilGetUID);
      break;
    }

    case ex_GET_QID: {
      GetVTblPtr(vtblptr, ComTdbExeUtilGetQID);
      break;
    }

    case ex_POP_IN_MEM_STATS: {
      GetVTblPtr(vtblptr, ComTdbExeUtilPopulateInMemStats);
      break;
    }

    case ex_SET_TIMEOUT: {
      GetVTblPtr(vtblptr, ComTdbTimeout);
      break;
    }

    case ex_UDR: {
      GetVTblPtr(vtblptr, ComTdbUdr);
      break;
    }

    case ex_PROBE_CACHE: {
      GetVTblPtr(vtblptr, ComTdbProbeCache);
      break;
    }

    case ex_CANCEL: {
      GetVTblPtr(vtblptr, ComTdbCancel);
      break;
    }

    case ex_SHOW_SET: {
      GetVTblPtr(vtblptr, ComTdbExeUtilShowSet);
      break;
    }

    case ex_AQR: {
      GetVTblPtr(vtblptr, ComTdbExeUtilAQR);
      break;
    }

    case ex_GET_ERROR_INFO: {
      GetVTblPtr(vtblptr, ComTdbExeUtilGetErrorInfo);
      break;
    }

    case ex_HBASE_ACCESS: {
      GetVTblPtr(vtblptr, ComTdbHbaseAccess);
    } break;

    case ex_HBASE_COPROC_AGGR: {
      GetVTblPtr(vtblptr, ComTdbHbaseCoProcAggr);
    } break;

    case ex_ARQ_WNR_INSERT: {
      GetVTblPtr(vtblptr, ComTdbExeUtilAqrWnrInsert);
      break;
    }

    case ex_COMPOSITE_UNNEST: {
      GetVTblPtr(vtblptr, ComTdbExeUtilCompositeUnnest);
      break;
    }
    case ex_GET_OBJECT_EPOCH_STATS: {
      GetVTblPtr(vtblptr, ComTdbExeUtilGetObjectEpochStats);
      break;
    }
    case ex_GET_OBJECT_LOCK_STATS: {
      GetVTblPtr(vtblptr, ComTdbExeUtilGetObjectLockStats);
      break;
    }
    case ex_QUERY_INVALIDATION: {
      GetVTblPtr(vtblptr, ComTdbQryInvalid);
      break;
    }

    default:
      break;
  }
  return vtblptr;
}
