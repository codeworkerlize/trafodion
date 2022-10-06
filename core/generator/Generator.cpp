
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         Generator.cpp
 * Description:  Methods which are used by the high level generator.
 *
 * Created:      4/15/95
 * Language:     C++
 *
 *
 *****************************************************************************
 */
#include "generator/Generator.h"

#include <stddef.h>
#include <stdlib.h>
#include <sys/time.h>

#include "CmUtil.h"
#include "CmpSqlSession.h"
#include "GenExpGenerator.h"
#include "arkcmp/CmpContext.h"
#include "arkcmp/CmpStatement.h"
#include "comexe/ComTdb.h"
#include "comexe/ComTrace.h"
#include "comexe/ExplainTuple.h"
#include "comexe/LateBindInfo.h"
#include "common/ComCextdecs.h"
#include "common/ComDistribution.h"
#include "common/ComOptIncludes.h"
#include "common/ComSqlId.h"
#include "common/ComSysUtils.h"
#include "common/ComTransInfo.h"
#include "common/dfs2rec.h"
#include "exp/exp_tuple_desc.h"
#include "exp_function.h"
#include "optimizer/BindWA.h"
#include "optimizer/ControlDB.h"
#include "optimizer/GroupAttr.h"
#include "optimizer/RelExeUtil.h"
#include "optimizer/RelMisc.h"
#include "optimizer/SchemaDB.h"
#include "sqlcat/TrafDDLdesc.h"
#include "sqlmxevents/logmxevent.h"

// #include "PCodeExprCache.h"
#include "cli/Context.h"
#include "comexe/ComASNodes.h"
#include "common/NAWNodeSet.h"
#include "executor/HBaseClient_JNI.h"
#include "sqlcat/TrafDDLdesc.h"

#define SQLPARSERGLOBALS_FLAGS
#include "parser/SqlParserGlobals.h"  // Parser Flags
#include "sqlcomp/CmpSeabaseDDLmd.h"

// -----------------------------------------------------------------------
// When called within arkcmp.exe, fixupVTblPtr() translates to a call
// to fix up the TDB's to the Compiler version. This function is also
// defined in ExComTdb.cpp. That code, however, fixes up the TDB's to
// the Executor version.
// -----------------------------------------------------------------------

// To prevent redefinition problem on platforms where the executor is (still)
// directly linked to arkcmp.exe.

///////////////////////////////////////////////////
// class Generator
//////////////////////////////////////////////////
Generator::Generator(CmpContext *currentCmpContext, NAHeap *workingHeap)
    : currentCmpContext_(currentCmpContext),
      wHeap_(workingHeap),
      objectUids_(wHeap(), 1),
      objectNames_(wHeap(), 0),
      snapshotScanTmpLocation_(NULL),
      baseFileDescs_(wHeap()),
      baseStoiList_(wHeap()),
      numOfVpsPerBase_(wHeap()),
      vpFileDescs_(wHeap()),
      lateNameInfoList_(wHeap()),
      genOperSimInfoList_(wHeap()),
      stoiList_(wHeap()),
      insertNodesList_(wHeap()),
      avgVarCharSizeList_(wHeap()),
      trafSimTableInfoList_(wHeap()),
      startMinMaxIndex_(0),
      endMinMaxIndex_(0),
      numTrafReplicas_(0)
      //,minMaxRowLength_(0)
      ,
      bmoQuotaMap_(wHeap()),
      callers_(wHeap()),
      innerChildUecs_(wHeap()),
      innerIsBroadcastPartitioned_(wHeap()),
      rowsProducedByHJ_(wHeap()),
      outerRowsToSurvive_(wHeap()) {
  // nothing generated yet.
  genObj = 0;
  genObjLength = 0;

  // no up or down descriptors.
  up_cri_desc = 0;
  down_cri_desc = 0;

  // fragment directory and resource entries for ESPs
  fragmentDir_ = new (wHeap()) FragmentDir(wHeap());

  firstMapTable_ = NULL;
  lastMapTable_ = NULL;

  tableId_ = 0;
  tempTableId_ = 0;
  tdbId_ = 0;
  pertableStatsTdbId_ = 0;

  bindWA = 0;

  flags_ = 0;
  flags2_ = 0;

  imUpdateRel_ = NULL;
  imUpdateTdb_ = NULL;

  updateCurrentOfRel_ = NULL;

  explainIsDisabled_ = FALSE;

  affinityValueUsed_ = 0;

  dynQueueSizeValuesAreValid_ = FALSE;

  orderRequired_ = FALSE;

  foundAnUpdate_ = FALSE;

  nonCacheableMVQRplan_ = FALSE;

  nonCacheableCSEPlan_ = FALSE;

  updateWithinCS_ = FALSE;

  isInternalRefreshStatement_ = FALSE;

  savedGenLeanExpr_ = FALSE;

  lruOperation_ = FALSE;

  queryUsesSM_ = FALSE;

  genSMTag_ = 0;

  tempSpace_ = NULL;

  numBMOs_ = 0;

  totalBMOsMemoryPerNode_ = 0;

  BMOsMemoryLimitPerNode_ = 0;

  totalNumBMOs_ = 0;

  numESPs_ = 1;

  totalNumESPs_ = 0;

  espLevel_ = 0;

  halloweenProtection_ = NOT_SELF_REF;
  collectRtsStats_ = TRUE;

  makeOnljRightQueuesBig_ = FALSE;
  makeOnljLeftQueuesBig_ = FALSE;
  onljLeftUpQueue_ = 0;
  onljLeftDownQueue_ = 0;
  onljRightSideUpQueue_ = 0;
  onljRightSideDownQueue_ = 0;

  // Used to specify queue size to use on RHS of flow or nested join.
  // Initialized to 0 (meaning don't use).  Set to a value by SplitTop
  // when on RHS of flow and there is a large degree of fanout. In this
  // situation, large queues are required above the split top (between
  // Split Top and Flow/NestedJoin).  Also used to set size of queues
  // below a SplitTop on RHS, but these do not need to be as large.
  largeQueueSize_ = 0;

  totalEstimatedMemory_ = 0.0;
  operEstimatedMemory_ = 0;

  maxCpuUsage_ = 0;

  // Exploded format is the default data format.
  // This can be changed via the CQD COMPRESSED_INTERNAL_FORMAT 'ON'
  setExplodedInternalFormat();
  overflowMode_ = ComTdb::OFM_DISK;

  // By default, set the query to be reclaimed
  setCantReclaimQuery(FALSE);

  avgVarCharSizeList_.clear();

  tupleFlowLeftChildAttrs_ = NULL;
  // avgVarCharSizeValList_.clear();
  initNNodes();

  initNbStrawScans();

  currentEspFragmentPCG_ = NULL;
  planExpirationTimestamp_ = -1;

  // Initialize the NExDbgInfoObj_ object within the Generator object.
  //
  NExDbgInfoObj_.setNExDbgLvl(getDefaultAsLong(PCODE_NE_DBG_LEVEL));
  NExDbgInfoObj_.setNExStmtSrc(NULL);
  NExDbgInfoObj_.setNExStmtPrinted(FALSE);

  NExLogPathNam_[0] = '\0';

  // Initialize other member variables.
  //
  computeStats_ = FALSE;
  explainInRms_ = TRUE;
  topNRows_ = 0;

  setMinmaxOptWithRangeOfValues(CURRSTMT_OPTDEFAULTS->rangeOptimizedScan());
}

void Generator::initTdbFields(ComTdb *tdb) {
  if (!dynQueueSizeValuesAreValid_) {
    // get the values from the default table if this is the first time
    // we are calling this method
    NADefaults &def = ActiveSchemaDB()->getDefaults();

    initialQueueSizeDown_ = (int)def.getAsULong(DYN_QUEUE_RESIZE_INIT_DOWN);
    initialQueueSizeUp_ = (int)def.getAsULong(DYN_QUEUE_RESIZE_INIT_UP);
    initialPaQueueSizeDown_ = (int)def.getAsULong(DYN_PA_QUEUE_RESIZE_INIT_DOWN);
    initialPaQueueSizeUp_ = (int)def.getAsULong(DYN_PA_QUEUE_RESIZE_INIT_UP);
    queueResizeLimit_ = (short)def.getAsULong(DYN_QUEUE_RESIZE_LIMIT);
    queueResizeFactor_ = (short)def.getAsULong(DYN_QUEUE_RESIZE_FACTOR);
    makeOnljLeftQueuesBig_ = (def.getToken(GEN_ONLJ_SET_QUEUE_LEFT) == DF_ON);
    onljLeftUpQueue_ = (int)def.getAsULong(GEN_ONLJ_LEFT_CHILD_QUEUE_UP);
    onljLeftDownQueue_ = (int)def.getAsULong(GEN_ONLJ_LEFT_CHILD_QUEUE_DOWN);
    makeOnljRightQueuesBig_ = (def.getToken(GEN_ONLJ_SET_QUEUE_RIGHT) == DF_ON);
    onljRightSideUpQueue_ = (int)def.getAsULong(GEN_ONLJ_RIGHT_SIDE_QUEUE_UP);
    onljRightSideDownQueue_ = (int)def.getAsULong(GEN_ONLJ_RIGHT_SIDE_QUEUE_DOWN);

    dynQueueSizeValuesAreValid_ = TRUE;
  }

  if (ActiveSchemaDB()->getDefaults().getToken(DYN_QUEUE_RESIZE_OVERRIDE) == DF_ON) {
    tdb->setQueueResizeParams(tdb->getMaxQueueSizeDown(), tdb->getMaxQueueSizeUp(), queueResizeLimit_,
                              queueResizeFactor_);
  }
  // Typically the sequence operaotr may have to deal with a large numer of rows when
  // it's part of the IM tree that performs elimination of dups.
  if ((tdb->getNodeType() == ComTdb::ex_SEQUENCE_FUNCTION) && isEffTreeUpsert()) {
    tdb->setQueueResizeParams(tdb->getMaxQueueSizeDown(), tdb->getMaxQueueSizeUp(), queueResizeLimit_,
                              queueResizeFactor_);
  }
  // Make the size of the upQ of ONLJ the same as that of the upQ
  // of the right child.
  if ((tdb->getNodeType() == ComTdb::ex_ONLJ || getRightSideOfOnlj()) && makeOnljRightQueuesBig_) {
    tdb->setQueueResizeParams(onljRightSideDownQueue_, onljRightSideUpQueue_, queueResizeLimit_, queueResizeFactor_);
  } else {
    tdb->setQueueResizeParams(initialQueueSizeDown_, initialQueueSizeUp_, queueResizeLimit_, queueResizeFactor_);
  }

  // If large queue sizes are specified, then adjust the up and down
  // queue sizes.  This is used when a SplitTop appears on the RHS of
  // a Flow/NestedJoin.  Above the SplitTop the queue sizes may be
  // very large (e.g. 128K), below the split top they will be modestly
  // large (e.g. 2048)
  if (largeQueueSize_ > 0 && tdb->getInitialQueueSizeDown() < largeQueueSize_ &&
      tdb->getNodeType() != ComTdb::ex_ROOT && (ActiveSchemaDB()->getDefaults().getToken(USE_LARGE_QUEUES) == DF_ON)) {
    tdb->setQueueResizeParams(largeQueueSize_, largeQueueSize_, queueResizeLimit_, queueResizeFactor_);
  }

  tdb->setTdbId(getAndIncTdbId());

  tdb->setPlanVersion(ComVersion_GetCurrentPlanVersion());

  if (computeStats()) {
    tdb->setCollectStats(computeStats());
    tdb->setCollectStatsType(collectStatsType());
  }

  compilerStatsInfo().totalOps()++;
}

RelExpr *Generator::preGenCode(RelExpr *expr_node) {
  // initialize flags
  //  flags_ = 0;
  //  flags2_ = 0;

  foundAnUpdate_ = FALSE;

  // create an expression generator.
  exp_generator = new (wHeap()) ExpGenerator(this);

  //  exp_generator->setNoTempSpace(TRUE);

  // later get it from CmpContext after parser sets it there.
  const NAString *val = ActiveControlDB()->getControlSessionValue("SHOWPLAN");
  if ((val) && (*val == "ON")) exp_generator->setShowplan(1);

  // the following is to support transaction handling for
  // CatAPIRequest.  Not to start transaction if TRANSACTION OFF.
  const NAString *tval = ActiveControlDB()->getControlSessionValue("TRANSACTION");
  if ((tval) && (*tval == "OFF")) exp_generator->setNoTransaction(1);

  if (CmpCommon::context()->GetMode() == STMT_STATIC)
    staticCompMode_ = TRUE;
  else
    staticCompMode_ = FALSE;

  // remember whether expression has order by clause
  if (NOT((RelRoot *)expr_node)->reqdOrder().isEmpty()) orderRequired_ = TRUE;

  NAString tmp;

  computeStats_ = FALSE;

  // check whether rms reached limit
  NABoolean genStats = true;
  StatsGlobals *statsGlobals = GetCliGlobals()->getStatsGlobals();
  ContextCli *curCtx = GetCliGlobals()->currContext();
  if ((statsGlobals && statsGlobals->calLimitLevel()) || (curCtx && (curCtx->getSqlParserFlags() & 0x20000) != 0))
    genStats = false;
  CmpCommon::getDefault(DETAILED_STATISTICS, tmp, -1);
  if ((tmp != "OFF") && (!Get_SqlParser_Flags(DISABLE_RUNTIME_STATS)) && genStats)

  {
    computeStats_ = TRUE;
    if ((tmp == "ALL") || (tmp == "ON"))
      collectStatsType_ = ComTdb::ALL_STATS;
    else if (tmp == "ACCUMULATED")
      collectStatsType_ = ComTdb::ACCUMULATED_STATS;
    else if (tmp == "PERTABLE")
      collectStatsType_ = ComTdb::PERTABLE_STATS;
    else if (tmp == "OPERATOR")
      collectStatsType_ = ComTdb::OPERATOR_STATS;
    else
      computeStats_ = FALSE;
  } else
    explainInRms_ = FALSE;

  if (CmpCommon::getDefault(COMP_BOOL_156) == DF_ON && genStats)
    collectRtsStats_ = TRUE;
  else
    collectRtsStats_ = FALSE;

  if (CmpCommon::getDefault(COMP_BOOL_166) == DF_OFF)
    r251HalloweenPrecode_ = true;
  else
    r251HalloweenPrecode_ = false;

  precodeHalloweenLHSofTSJ_ = false;
  precodeRHSofNJ_ = false;
  unblockedHalloweenScans_ = 0;
  halloweenSortForced_ = false;
  halloweenESPonLHS_ = false;

  if (CmpCommon::getDefault(DefaultConstants::PARQUET_IN_SUPPORT) == DF_OFF)
    setParquetInSupport(FALSE);
  else
    setParquetInSupport(TRUE);

  CmpCommon::getDefault(OVERFLOW_MODE, tmp, -1);
  if (tmp == "SSD")
    overflowMode_ = ComTdb::OFM_SSD;
  else if (tmp == "MMAP")
    overflowMode_ = ComTdb::OFM_MMAP;
  else
    overflowMode_ = ComTdb::OFM_DISK;
  // turn computeStats off if this is a SELECT from statistics virtual
  // table stmt, a control stmt or a SPJ result sets proxy statement.
  if (expr_node->child(0) &&
      ((expr_node->child(0)->getOperatorType() == REL_STATISTICS) ||
       (expr_node->child(0)->getOperatorType() == REL_QUERYINVALIDATION) ||
       (expr_node->child(0)->getOperatorType() == REL_CONTROL_QUERY_SHAPE) ||
       (expr_node->child(0)->getOperatorType() == REL_CONTROL_QUERY_DEFAULT) ||
       (expr_node->child(0)->getOperatorType() == REL_CONTROL_TABLE) ||
       (expr_node->child(0)->getOperatorType() == REL_CONTROL_SESSION) ||
       (expr_node->child(0)->getOperatorType() == REL_SET_SESSION_DEFAULT) ||
       (expr_node->child(0)->getOperatorType() == REL_TRANSACTION) ||
       (expr_node->child(0)->getOperatorType() == REL_DESCRIBE) ||
       (expr_node->child(0)->getOperatorType() == REL_LOCK) || (expr_node->child(0)->getOperatorType() == REL_UNLOCK) ||
       (expr_node->child(0)->getOperatorType() == REL_SET_TIMEOUT) ||
       (expr_node->child(0)->getOperatorType() == REL_CONTROL_RUNNING_QUERY) ||
       (expr_node->child(0)->getOperatorType() == REL_SP_PROXY))) {
    computeStats_ = FALSE;
    explainInRms_ = FALSE;
  }
  if (expr_node->child(0) &&
      ((expr_node->child(0)->getOperatorType() == REL_DDL) || (expr_node->child(0)->getOperatorType() == REL_CALLSP) ||
       (expr_node->child(0)->getOperatorType() == REL_EXE_UTIL)))
    explainInRms_ = FALSE;
  if (expr_node->child(0) &&
      (expr_node->child(0)->getOperatorType() == REL_EXE_UTIL &&
       ((((ExeUtilExpr *)expr_node->child(0)->castToRelExpr())->getExeUtilType() == ExeUtilExpr::GET_STATISTICS_) ||
        (((ExeUtilExpr *)expr_node->child(0)->castToRelExpr())->getExeUtilType() == ExeUtilExpr::DISPLAY_EXPLAIN_)))) {
    computeStats_ = FALSE;
    explainInRms_ = FALSE;
  }

#ifdef _DEBUG
  if (getenv("NO_DETAILED_STATS")) {
    computeStats_ = FALSE;
    explainInRms_ = FALSE;
  }
#endif

  setUpdatableSelect(((RelRoot *)expr_node)->updatableSelect());
  // RelRoot::updatableSelect() is not accurate,
  // also use lock mode to detect select-for-update for now
  if (NOT updatableSelect() && gEnableRowLevelLock &&
      ((RelRoot *)expr_node)->accessOptions().accessType() == TransMode::ACCESS_TYPE_NOT_SPECIFIED_ &&
      ((RelRoot *)expr_node)->accessOptions().lockMode() == LockMode::PROTECTED_) {
    setUpdatableSelect(TRUE);
  }

  // see if aqr could be done
  NABoolean aqr = FALSE;
  // Only dynamic queries from odbc/jdbc, trafci or mxci will enable AQR.
  if ((staticCompMode_ == FALSE) &&
      ((CmpCommon::getDefault(IS_SQLCI) == DF_ON) || (CmpCommon::getDefault(IS_TRAFCI) == DF_ON) ||
       (CmpCommon::getDefault(ODBC_PROCESS) == DF_ON))) {
    // Users can enable aqr by setting AUTO_QUERY_RETRY to ON.
    if (CmpCommon::getDefault(AUTO_QUERY_RETRY) == DF_ON) {
      aqr = TRUE;
    } else if (CmpCommon::getDefault(AUTO_QUERY_RETRY) == DF_SYSTEM) {
      if (Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)) {
        // if internal query from executor for explain, enable aqr.
        const NAString *val = ActiveControlDB()->getControlSessionValue("EXPLAIN");
        if (((val) && (*val == "ON")) || (exp_generator->getShowplan())) {
          aqr = TRUE;
        } else if (Get_SqlParser_Flags(ALLOW_SPECIALTABLETYPE)) {
          // special/ghost tables are accessed internally.
          // They need to be cached.
          aqr = TRUE;
        } else {
          aqr = FALSE;
        }
      } else {
        aqr = TRUE;
      }
    }
  }
  setAqrEnabled(aqr);

  // pre code gen.
  ValueIdSet pulledInputs;
  return expr_node->preCodeGen(this, expr_node->getGroupAttr()->getCharacteristicInputs(), pulledInputs);
}

void Generator::genCode(const char *source, RelExpr *expr_node) {
  // Set the plan Ident. to be a time stamp.
  // This is used by EXPLAIN as the planID.

  planId_ = NA_JulianTimestamp();

  explainNodeId_ = 0;

  explainTuple_ = NULL;

  stmtSource_ = source;

  NExDbgInfoObj_.setNExStmtSrc((char *)source);

  explainFragDirIndex_ = NULL_COLL_INDEX;

  explainIsDisabled_ = 0;
  if (CmpCommon::getDefault(GENERATE_EXPLAIN) == DF_OFF) disableExplain();

  if (expr_node->child(0) && ((expr_node->child(0)->getOperatorType() == REL_CONTROL_QUERY_SHAPE) ||
                              (expr_node->child(0)->getOperatorType() == REL_CONTROL_QUERY_DEFAULT) ||
                              (expr_node->child(0)->getOperatorType() == REL_CONTROL_TABLE) ||
                              (expr_node->child(0)->getOperatorType() == REL_CONTROL_SESSION) ||
                              (expr_node->child(0)->getOperatorType() == REL_SET_SESSION_DEFAULT)))
    disableExplain();

  foundAnUpdate_ = FALSE;

  if ((expr_node->getOperatorType() == REL_ROOT) && (((RelRoot *)expr_node)->isTrueRoot()) &&
      (((RelRoot *)expr_node)->hasCompositeExpr()))
    setHasCompositeExpr(TRUE);

  // walk through the tree of RelExpr and ItemExpr objects, generating
  // ComTdb, ex_expr and their relatives
  expr_node->codeGen(this);

  // pack each fragment independently; packing converts pointers to offsets
  // relative to the start of the fragment
  for (CollIndex i = 0; i < fragmentDir_->entries(); i++) {
    Space *fragSpace = fragmentDir_->getSpace(i);
    char *fragTopNode = fragmentDir_->getTopNode(i);

    switch (fragmentDir_->getType(i)) {
      case FragmentDir::MASTER:
        ComTdbPtr((ComTdb *)fragTopNode).pack(fragSpace);
        break;

      case FragmentDir::DP2:
        GenAssert(0, "DP2 fragments not supported");
        break;

      case FragmentDir::ESP:
        ComTdbPtr((ComTdb *)fragTopNode).pack(fragSpace);
        break;

      case FragmentDir::EXPLAIN:
        ExplainDescPtr((ExplainDesc *)fragTopNode).pack(fragSpace);
        break;
    }
  }
}

Generator::~Generator() {
  // cleanup
  if (fragmentDir_) NADELETE(fragmentDir_, FragmentDir, wHeap());
}

// moves the generated code into out_buf. If the generated code
// is allocated from a list of buffers, then each of the buffer is
// moved contiguously to out_buf. The caller MUST have allocated
// sufficient space in out_buf to contain the generated code.
char *Generator::getFinalObj(char *out_buf, int out_buflen) {
  if (out_buflen < (int)getFinalObjLength()) return NULL;

  // copy the objects of all spaces into one big buffer
  int outputLengthSoFar = 0;
  for (CollIndex i = 0; i < fragmentDir_->entries(); i++) {
    // copy the next space into the buffer
    if (fragmentDir_->getSpace(i)->makeContiguous(&out_buf[outputLengthSoFar], out_buflen - outputLengthSoFar) == 0)
      return NULL;
    outputLengthSoFar += fragmentDir_->getFragmentLength(i);
  }
  return out_buf;
}

void Generator::doRuntimeSpaceComputation(char *root_tdb, char *fragTopNode, int &tcbSize) {
  tcbSize = 0;
  // compute space.
  tcbSize = SQL_EXEC_GetTotalTcbSpace(root_tdb, fragTopNode);
}

void Generator::setTransactionFlag(NABoolean transIsNeeded, NABoolean isNeededForAllFragments) {
  if (transIsNeeded)  // if transaction is needed
  {
    // remember it for the entire statement...
    flags_ |= TRANSACTION_FLAG;

    if (fragmentDir_->entries() > 0) {
      // ...and also for the current fragment...
      fragmentDir_->setNeedsTransaction(fragmentDir_->getCurrentId(), TRUE);
      // ...and for the root fragment
      fragmentDir_->setNeedsTransaction(0, TRUE);

      if (isNeededForAllFragments) fragmentDir_->setAllEspFragmentsNeedTransaction();
    }
  }
}

void Generator::resetTransactionFlag() { flags_ &= ~TRANSACTION_FLAG; }

TransMode *Generator::getTransMode() { return CmpCommon::transMode(); }

// Verify that the current transaction mode is suitable for
// update, delete, insert, or ddl operation.
// Clone of code in GenericUpdate::bindNode(),
// which cannot (?) detect all cases of transgression (at least the DDL ones).
//
void Generator::verifyUpdatableTransMode(StmtLevelAccessOptions *sAxOpt, TransMode *tm,
                                         TransMode::IsolationLevel *ilForUpd) {
  int sqlcodeA = 0, sqlcodeB = 0;

  //  if (getTransMode()->isolationLevel() == TransMode::READ_UNCOMMITTED_)
  //    sqlcodeA = -3140;
  // if (getTransMode()->accessMode()     != TransMode::READ_WRITE_)
  //   sqlcodeB = -3141;

  TransMode::IsolationLevel il;
  ActiveSchemaDB()->getDefaults().getIsolationLevel(il, CmpCommon::getDefault(ISOLATION_LEVEL_FOR_UPDATES));
  verifyUpdatableTrans(sAxOpt, tm, il, sqlcodeA, sqlcodeB);
  if (ilForUpd) *ilForUpd = il;

  if (sqlcodeA || sqlcodeB) {
    // 3140 The isolation level cannot be READ UNCOMMITTED.
    // 3141 The transaction access mode must be READ WRITE.
    if (sqlcodeA) *CmpCommon::diags() << DgSqlCode(sqlcodeA);
    if (sqlcodeB) *CmpCommon::diags() << DgSqlCode(sqlcodeB);
    GenExit();
  }
  setNeedsReadWriteTransaction(TRUE);
}

CollIndex Generator::addFileDesc(const IndexDesc *desc, SqlTableOpenInfo *stoi) {
  CollIndex index = baseFileDescs_.entries();
  baseFileDescs_.insert(desc);
  baseStoiList_.insert(stoi);
  numOfVpsPerBase_.insert(0);  // no vertical partitions for this entry
  return index;
}

CollIndex Generator::addVpFileDesc(const IndexDesc *vpDesc, SqlTableOpenInfo *stoi, CollIndex &vpIndex) {
  const TableDesc *baseDesc = vpDesc->getPrimaryTableDesc();
  CollIndex i = 0;
  for (i = 0; i < baseFileDescs_.entries(); i++) {
    if (numOfVpsPerBase_[i] && baseDesc == baseFileDescs_[i]->getPrimaryTableDesc()) break;
  }
  if (i == baseFileDescs_.entries()) {  // no base entry found, add new base file descriptor
    i = addFileDesc(vpDesc, stoi);
  }
  vpIndex = numOfVpsPerBase_[i];  // vp descriptor index
  numOfVpsPerBase_[i]++;
  vpFileDescs_.insert(vpDesc);  // add to list of all vp descriptors
  return i;
}

NAHeap *Generator::wHeap() {
  if (wHeap_) return wHeap_;

  return (currentCmpContext_) ? currentCmpContext_->statementHeap() : 0;
}

void Generator::setGenObj(const RelExpr *node, ComTdb *genObj_) {
  genObj = genObj_;

  // if my child needs to return first N rows, then get that number from
  // my child and set it in my tdb. At runtime, my tcb's work method will
  // ask my child for N rows with a GET_N request.
  //  if (node && node->child(0) && genObj)
  //    genObj->firstNRows() = (int)((RelExpr *)node)->child(0)->getFirstNRows();
};

ComTdbRoot *Generator::getTopRoot() {
  for (CollIndex i = 0; i < fragmentDir_->entries(); i++) {
    if (fragmentDir_->getType(i) == FragmentDir::MASTER) {
      ComTdb *root = (ComTdb *)(fragmentDir_->getTopNode(i));

      GenAssert(root->getNodeType() == ComTdb::ex_ROOT, "Bad Top Root");
      return (ComTdbRoot *)root;
    }
  }
  return NULL;
}

const Space *Generator::getTopSpace() const {
  for (CollIndex i = 0; i < fragmentDir_->entries(); i++) {
    if (fragmentDir_->getType(i) == FragmentDir::MASTER) {
      return fragmentDir_->getSpace(i);
    }
  }
  return NULL;
}

//
// Handle user specified ESP remapping case.
//
// Return false if the specification is not correct. For spec, refer to
// COMP_STRING_2 in the comment section for method remapESPAllocationAS().
//
static NABoolean remapESPAllocationViaUserInputs(FragmentDir *fragDir, const char *espOrder, CollHeap *heap) {
  CollIndex i;

  // if CycleSegs TRUE, will cause each ESP layer to start with the next
  // CPU in the list.
  //
  NABoolean cycleSegs = (ActiveSchemaDB()->getDefaults()).getAsLong(AS_CYCLIC_ESP_PLACEMENT);
  int numCPUs = CURRCONTEXT_CLUSTERINFO->getTotalNumberOfCPUs();

  int *utilcpus = new (heap) int[numCPUs];
  int *utilsegs = new (heap) int[numCPUs];

  // Parse the espOrderString is specified.
  //

  // Indicates if the espOrderString is properly specified.
  //
  NABoolean espOrderOK = FALSE;
  if (espOrder && *espOrder) {
    espOrderOK = TRUE;
    const char *espOrderp = espOrder;

    for (i = 0; i < (CollIndex)numCPUs && espOrderOK && *espOrderp; i++) {
      int seg = 0;
      int cpu = 0;
      int state = 0;

      if (*espOrderp >= '0' && *espOrderp <= '9') {
        state++;

        seg = atoi(espOrderp);
        while (*espOrderp >= '0' && *espOrderp <= '9') espOrderp++;
      }

      if (*espOrderp == ':') {
        espOrderp++;
        state++;
      }

      if (*espOrderp >= '0' && *espOrderp <= '9') {
        state++;

        cpu = atoi(espOrderp);
        while (*espOrderp >= '0' && *espOrderp <= '9') espOrderp++;
      }

      if (*espOrderp == ',') espOrderp++;

      if (state == 3) {
        utilcpus[i] = cpu;
        utilsegs[i] = seg;

      } else {
        espOrderOK = FALSE;
      }
    }
  }

  int numEntries = i;

  if (!espOrderOK) {
    return FALSE;

  } else {
    // Remap Each ESP fragment.
    //

    int nextCPUToUse = 0;

    for (i = 0; i < fragDir->entries(); i++) {
      if (fragDir->getPartitioningFunction(i) != NULL && fragDir->getType(i) == FragmentDir::ESP) {
        // Get the node map for this ESP fragment.
        //
        NodeMap *nodeMap = (NodeMap *)fragDir->getPartitioningFunction(i)->getNodeMap();

        // Copy the existing node map for this ESP fragment.
        // Need to make a copy because this node map could be
        // shared with other node maps.
        //
        nodeMap = nodeMap->copy(heap);

        // Reset for each ESP layer, unless cycleSegs was specified.
        //
        if (!cycleSegs) nextCPUToUse = 0;

        // Remap each entry in the node map for this fragment.
        //
        for (CollIndex j = 0; j < nodeMap->getNumEntries(); j++) {
          // The index into the CPU and Segment maps.  This
          // cpuNumber is the number relative to the whole
          // system (all segments)
          //
          int cpuNumber = nextCPUToUse++;

          // Wrap around if at end of list.
          //
          if (nextCPUToUse == numEntries) {
            nextCPUToUse = 0;
          }

          // Get the cpu based on the CPU map.
          // This cpu is the cpu number for a specific segment.
          //
          int cpu = (int)utilcpus[cpuNumber];
          int seg = (int)utilsegs[cpuNumber];

          // Set the cpu and segment for this node map entry.
          //
          nodeMap->setNodeNumber(j, cpu);
          nodeMap->setClusterNumber(j, seg);
        }

        // After remapping the node map (copy), make it the
        // node map for this ESP fragment.
        //
        PartitioningFunction *partFunc = (PartitioningFunction *)(fragDir->getPartitioningFunction(i));
        partFunc->replaceNodeMap(nodeMap);
      }  // end of ESP fragment processing
    }    // end of FOR loop on all fragments
  }
  return TRUE;
}

void Generator::remapESPAllocation() {
  if (!fragmentDir_ || !fragmentDir_->containsESPLayer()) return;

  const NAWNodeSet *tenantNodes = CmpCommon::context()->getAvailableNodes();
  const NAASNodes *asNodes = tenantNodes->castToNAASNodes();
  int defaultAffinity = ActiveSchemaDB()->getDefaults().getAsLong(AS_AFFINITY_VALUE);

  if (  // we are using a non-AS tenant
      asNodes == NULL ||
      // or we are using a tenant that can use all nodes and AS is off
      (asNodes->canUseAllNodes(CURRCONTEXT_CLUSTERINFO) && defaultAffinity == -2))
    remapESPAllocationRandomly();
  else {
    // If set, defines a new ordering of the CPUs.
    //
    const char *espOrder = ActiveSchemaDB()->getDefaults().getValue(COMP_STRING_2);

    if (espOrder && *espOrder) {
      GenAssert(0, "ESP remapping via COMP_STRING_2 currently not supported");
      if (remapESPAllocationViaUserInputs(fragmentDir_, espOrder, wHeap())) {
        return;
      }
    }
    // use Adaptive Segmentation
    remapESPAllocationAS(asNodes);
  }

  compilerStatsInfo().affinityNumber() = getAffinityValueUsed();
}

void Generator::computeAvailableNodes(NAWNodeSet *&availableNodes, const NAWNodeSet *availableNodesFromCLI,
                                      const NAWNodeSet *availableNodesFromOSIM, int defaultAffinity, CollHeap *heap) {
  // At the end of this call, availableNodes will point to an object
  // that is allocated on <heap> and fits one of these descriptions:
  // - a) an NAASNodes object representing the entire cluster when not
  //      using a tenant
  // - b) a copy of the tenant nodes object stored in the CLI (or OSIM),
  //      with an updated affinity, based on defaultAffinity
  //      when using an AS tenant and when defaultAffinity is not -2
  // - c) otherwise, a copy of the tenant nodes object stored in the CLI
  //      (or OSIM)
  //
  // AS_AFFINITY_VALUE CQD (defaultAffinity):
  // For non-tenant connections:    Specifies the affinity
  // For AS tenant connections:     Use the specified affinity to balance
  //                                different connections within the tenant.
  // For non-AS tenant connections: defaultAffinity has no effect.
  //
  // See method Generator::remapESPAllocationAS for a description
  // of the allowed values for the AS_AFFINITY_VALUE CQD.

  const NAWNodeSet *tenantNodes = availableNodesFromOSIM;
  const NAASNodes *asNodes = NULL;

  if (!tenantNodes) tenantNodes = availableNodesFromCLI;

  if (defaultAffinity < -4 || defaultAffinity == -1) {
    // invalid affinity value in CQD
    *CmpCommon::diags() << DgSqlCode(-7034) << DgInt0(defaultAffinity);
    return;
  }

  if (tenantNodes) asNodes = tenantNodes->castToNAASNodes();

  DCMPASSERT(availableNodes == NULL);

  // if AS is off (default affinity -2) and we can't use all nodes
  // then change that to affinity -4, since this situation requires AS
  if (defaultAffinity == -2 && asNodes && !asNodes->canUseAllNodes(CURRCONTEXT_CLUSTERINFO)) defaultAffinity = -4;

  if (tenantNodes == NULL ||                       // a)
      (asNodes != NULL && defaultAffinity != -2))  // b)
  {
    // a) or b)

    // the new affinity we want to use, based on the other two
    // (defaultAffinity and tenant affinity)
    int newAffinity = defaultAffinity;

    // Handle negative affinity values -2, -3 and -4. See
    // comment in Generator::remapESPAllocationAS()
    if (defaultAffinity == -2)
      newAffinity = 0;  // doesn't really matter, AS is off
    else if (defaultAffinity == -3) {
      // default affinity -3: A random value based on the session id
      const char *sessionId = ActiveSchemaDB()->getDefaults().getValue(SESSION_ID);

      int length = strlen(sessionId);

      length = (length > 43 ? 43 : length);

      UInt32 affinity32 = ExHDPHash::hash(sessionId, ExHDPHash::NO_FLAGS, length);

      // make it non-negative
      affinity32 &= 0x7FFFFFFF;
      newAffinity = (int)affinity32;
      GenAssert(tenantNodes == NULL || tenantNodes->canUseAllNodes(CURRCONTEXT_CLUSTERINFO),
                "default affinity -3 is not supported in a multi-tenant environment");
    } else if (defaultAffinity == -4) {
      // affinity -4: choose affinity such that node of master
      //              executor is a part of the segment
      const char *sessionId = ActiveSchemaDB()->getDefaults().getValue(SESSION_ID);
      int length = strlen(sessionId);
      long cpu_l = -99;

      ComSqlId::getSqlSessionIdAttr(ComSqlId::SQLQUERYID_CPUNUM, sessionId, length, cpu_l, NULL);

      // Use the CPU/node number of the session. MXOAS balances the
      // connections among the nodes and it also makes sure that
      // a connection for a tenant is running on one of the allowed
      // nodes for this tenant. Note that AS uses "dense" node ids,
      // so we need to map the physical node id to a dense id.
      newAffinity = CURRCONTEXT_CLUSTERINFO->mapPhysicalToLogicalNodeId((int)cpu_l);
      DCMPASSERT(newAffinity >= 0);
      if (newAffinity < 0)
        // this should be very unlikely, an invalid node number in
        // the session id, pick a more or less random affinity
        newAffinity = 3;
    }

    // now create a new object, based on the cases a) and b) described above
    if (defaultAffinity == -2 || asNodes == NULL) {
      // a) no tenant or AS tenant covering all nodes
      int numUnits = CURRCONTEXT_CLUSTERINFO->numberOfTenantUnitsInTheCluster();
      int numNodes = CURRCONTEXT_CLUSTERINFO->getTotalNumberOfCPUs();

      availableNodes = new (heap) NAASNodes(numUnits, numNodes, newAffinity);
    } else {
      // b) AS tenant not covering all nodes, AS is ON via CQD

      // Make newAffinity compatible with the tenant, such that
      // we will choose the same nodes, but with a random starting
      // point
      newAffinity = asNodes->randomizeAffinity(newAffinity);

      availableNodes = new (heap) NAASNodes(asNodes->getTotalWeight(), asNodes->getClusterSize(), newAffinity);
    }
  }  // a) or b)
  else {
    // c) non-AS tenant or AS tenant and AS CQD is off
    availableNodes = tenantNodes->copy(heap);
  }
}

// remapESPAllocationAS: Called by RelRoot::codeGen()

// Re-assign each ESP to a CPU for adaptive segmentation based on the
// affinity value.
//
// To disable ESP remapping, set the CQD AS_AFFINITY_VALUE to '-2' (default)
// Settings for AS_AFFINITY_VALUE:
//
//   -4 - Use session based remapping (use an affinity value based on
//        the location of the master executor).
//   -3 - Use session based remapping (use an affinity value based on
//        a hash of the session ID)
//        (not supported in multi-tenant environments)
//   -2 - Disable ESP remapping
//        (behaves like -4 in multi-tenant environments)
//   -1 - No longer supported (was random affinity for every statement)
//        (will result in an error)
//   non-negative integer: remap ESPs based on given affinity value.
//
// Note that affinity values -3 and -4 are translated into a non-negative
// affinity value in method Generator::computeTenantNodes().
//
// See also class NAASNodes in file common/NAWNodeSet.h for more details.
//
// Elasticity:
//
// When removing nodes dynamically from a cluster, "holes" in the
// node ids can be created. Such holes cause a potential problem for
// the algorithms used with adaptive segmentation. We therefore apply
// the algorithms to a virtual list of nodes numbered 0 ... n-1 and
// then map those ids to the actual node ids, using the array stored
// in NAClusterInfo::getCPUArray. This means that when a node gets
// removed, node assignment for tenants may shift.
//
// Other settings:
//
// AS_CYCLIC_ESP_PLACEMENT - Use a different affinity value for each
// n ESP fragments of the query. The default is 1 - place each fragment
// on a different segment. A value of 0 turns this feature off. Note that
// fragments with BMOs count as 2, so setting this value to 4 would place
// 2 BMO or 4 non-BMO fragments into the same adaptive segment.
//
// AS_FRAGMENT_SIZE_THRESHOLD - ESP layers that are smaller than the
// value of this CQD will be placed in a round-robin fashion by the
// executor at runtime.
//
// AS_SHIFT_FULL_FRAGMENTS - Default OFF (FALSE).  If TRUE (ON),
// then for ESP layers that use all CPUs/nodes, shift the ESP mapping
// between fragments. For example, a node map that uses all 32 nodes
// of a cluster (0-31) could be remapped to something like: (3-31, 0-2).
// This could help if we expect one of the partitions to have skew.
//
// Experimental Settings:
//
// COMP_STRING_2 - remapString - not supported at this time!!
// Default empty (do not use remap string).  If set, must be of the form:
//
//   "<seg_number>:<cpu_number>[,<seg_number>:<cpu_number>]..."
//
// and must contain numCPUs entries. Any deviation from this form will
// cause it to be ignored.  If set properly, the string specifies a
// different ordering of the CPUs for the purposes of remapping.  For
// instance if the remapString were set to:
// "1:0,1:8,1:1,1:9,1:2,1:10,1:3,1:11,...", then segment 1, CPU 0
// would be treated as CPU 0, segment 1, CPU 8 would be treated as CPU
// 1 and so on.  With this control, it is possible to map each
// adaptive segment to any subset of CPUs.
//
void Generator::remapESPAllocationAS(const NAASNodes *asNodes) {
  DCMPASSERT(asNodes);
  int numTenantNodes = asNodes->getNumNodes();
  const NAArray<CollIndex> &cpuArray(CURRCONTEXT_CLUSTERINFO->getCPUArray());
  int numCPUs = cpuArray.entries();
  int clusterSize = asNodes->getClusterSize();

  // The affinity value to use for this query and for each fragment
  //
  int queryAffinity = asNodes->getAffinity();
  int fragmentAffinity = queryAffinity;

  // affinities < 0 should have been handled by now
  GenAssert(queryAffinity >= 0, "calling AS with affinity < 0");

  // if CycleSegs TRUE, will cause ESP layers after layersInCycle to use the
  // next affinity_value.
  //
  int layersInCycle = ((ActiveSchemaDB()->getDefaults()).getAsLong(AS_CYCLIC_ESP_PLACEMENT));

  NABoolean cycleSegs = (layersInCycle > 0);
  int espLayersInCurrentCycle = 0;

  // check whether we should do a circular shift for fragments that
  // touch all the nodes
  //
  NABoolean shiftFullFragments =
      (CmpCommon::getDefault(AS_SHIFT_FULL_FRAGMENTS) == DF_ON && asNodes->canUseAllNodes(CURRCONTEXT_CLUSTERINFO));

  // The fragment size threshold specifies the ESP layer size below
  // which a random affinity value is used.
  //
  int asThreshold = ActiveSchemaDB()->getDefaults().getAsLong(AS_FRAGMENT_SIZE_THRESHOLD);

  // Save the affinity value in the Generator so it can be used by Explain.
  //
  setAffinityValueUsed(queryAffinity);

  // Remap Each ESP fragment.
  //
  for (CollIndex i = 0; i < fragmentDir_->entries(); i++) {
    if (fragmentDir_->getPartitioningFunction(i) != NULL && fragmentDir_->getType(i) == FragmentDir::ESP) {
      // Get the node map for this ESP fragment.
      //
      NodeMap *nodeMap = (NodeMap *)fragmentDir_->getPartitioningFunction(i)->getNodeMap();

      // If this node map is qualified for remapping ...
      //
      NAText output;
      if (!nodeMap->usesLocality() && nodeMap->getNumEntries() >= asThreshold) {
#ifdef _DEBUG
        if ((CmpCommon::getDefault(NSK_DBG) == DF_ON) && (CmpCommon::getDefault(NSK_DBG_GENERIC) == DF_ON)) {
          OptDebug *optDbg = CmpCommon::context()->getOptDbg();
          optDbg->stream() << "Remapping NodeMap::" << endl
                           << "fragment number = " << i << ", entries = " << nodeMap->getNumEntries()
                           << ", query affinity = " << queryAffinity << ", fragment affinity = " << fragmentAffinity
                           << endl;
        }
#endif

        int dop = nodeMap->getNumEntries();

        // The optimizer should use DoPs that are factors or
        // multiples of asNodes, but in some cases it computes
        // the DoP from the size of tables.  This code should
        // tolerate any DoP.
        int segmentSize = asNodes->makeSizeFeasible(dop, numTenantNodes, 1);

        // don't let the tenant out of its subset of nodes
        if (segmentSize > numTenantNodes && numTenantNodes < clusterSize) segmentSize = numTenantNodes;

        // Make an NAASNodes object for the tenant with the
        // given affinity and size, and the DoP of this fragment
        NAASNodes fragmentNodes(segmentSize, clusterSize, fragmentAffinity);
        int nodeNumber = -1;

        NABoolean shiftNodes = (shiftFullFragments && dop >= numTenantNodes);
        int shift = (shiftNodes ? i : 0);

        // Copy the existing node map for this ESP fragment.
        // Need to make a copy because this node map could be
        // shared with other node maps.
        //
        nodeMap = nodeMap->copy(wHeap());

        // Remap each entry in the node map for this fragment.
        //
        for (CollIndex j = 0; j < dop; j++) {
          int physNodeNumber;

          nodeNumber = fragmentNodes.getNodeNumber(j + shift);

          // one more, final, mapping, accounts for potential
          // holes in the list of node ids for this cluster
          physNodeNumber = cpuArray[nodeNumber % numCPUs];

          // this is where the actual remapping happens
          // ------------------------------------------
          nodeMap->setNodeNumber(j, physNodeNumber);

#ifdef _DEBUG
          if ((CmpCommon::getDefault(NSK_DBG) == DF_ON) && (CmpCommon::getDefault(NSK_DBG_GENERIC) == DF_ON)) {
            OptDebug *optDbg = CmpCommon::context()->getOptDbg();
            optDbg->stream() << " entry = " << j << ", nodeNumber = " << nodeNumber
                             << ", physNodeNumber = " << physNodeNumber << endl;
          }
#endif
        }  // loop over node map entries

        // After remapping the node map (copy), make it the
        // node map for this ESP fragment.
        //
        PartitioningFunction *partFunc = (PartitioningFunction *)(fragmentDir_->getPartitioningFunction(i));
        partFunc->replaceNodeMap(nodeMap);

        espLayersInCurrentCycle++;

        // Count an ESP fragment with BMOs twice. This is an
        // approximation to the real logic used in run-time to
        // pack ESPs with BMO operators into ESP proceses.
        if (fragmentDir_->getNumBMOs(i) > 0) espLayersInCurrentCycle++;

        // If cycleSegs is specified, use a different affinity
        // value for layersInCycle # of ESP layers.
        //
        if (cycleSegs && (espLayersInCurrentCycle >= layersInCycle)) {
          // set the affinity to the next allowed value
          // (mod numTenantNodes), so that the allocated
          // segments look something like this
          // (may also wrap-around, of course):
          // +--+--+--+-- fragment 0, dop 4, affinity 3
          // +--+--+--+-- fragment 1, dop 4, affinity 3
          //              ---- shift affinity to 4 ----
          // -+--+--+--+- fragment 2, dop 4, affinity 4
          // +---+---+--- fragment 3, dop 3, affinity 4
          fragmentAffinity = (fragmentAffinity + fragmentNodes.getNodeDistanceInSegment()) % clusterSize;
          espLayersInCurrentCycle = 0;
        }  // cycle segs
        fragmentNodes.serialize(output);
      }  // fragment qualifies for remapping
      QRLogger::log("SQL", LL_INFO, "remapESPAllocationAS(). fragmentNodes(%s). NodeMap(%s).", output.data(),
                    nodeMap->getText().data());
    }  // ESP fragment
  }    // loop over fragments of the statement
}  // remapESPAllocationAS()

// map ESPs randomly
void Generator::remapESPAllocationRandomly() {
  NAClusterInfo *nac = CmpCommon::context()->getClusterInfo();
  int numCPUs = nac->getTotalNumberOfCPUs();
  for (int i = 0; i < fragmentDir_->entries(); i++) {
    if (fragmentDir_->getPartitioningFunction(i) != NULL && fragmentDir_->getType(i) == FragmentDir::ESP) {
      // Get the node map for this ESP fragment.
      NodeMap *nodeMap = (NodeMap *)fragmentDir_->getPartitioningFunction(i)->getNodeMap();

      NABoolean allowRandomMapping = fragmentDir_->getPartitioningFunction(i)->allowRandomMapping();

      for (CollIndex j = 0; j < nodeMap->getNumEntries(); j++) {
        if (!allowRandomMapping) {
          // Assign the node number to j, limited by the total
          // number of CPUs. The limit is needed to take care of
          // situations where the number of available CPUs is faked.
          nodeMap->setNodeNumber(j, MINOF(j, numCPUs - 1));
        } else
            // if ESP-RegionServer colocation logic is off, then assign any node
            if (CmpCommon::getDefault(TRAF_ALLOW_ESP_COLOCATION) == DF_OFF)
          nodeMap->setNodeNumber(j, IPC_CPU_DONT_CARE);

        nodeMap->setClusterNumber(j, 0);
      }

      // After remapping the node map (copy), make it the
      // node map for this ESP fragment.
      PartitioningFunction *partFunc = (PartitioningFunction *)(fragmentDir_->getPartitioningFunction(i));
      partFunc->replaceNodeMap(nodeMap);
      QRLogger::log("SQL", LL_INFO, "remapESPAllocationRandomly(numCPUs : %d). NodeMap%s", numCPUs,
                    nodeMap->getText().data());
    }
  }
}

int Generator::getRecordLength(ComTdbVirtTableIndexInfo *indexInfo, ComTdbVirtTableColumnInfo *columnInfoArray) {
  int recLen = 0;

  if ((!indexInfo) && (!columnInfoArray)) return recLen;

  int keyCount = indexInfo->keyColCount;
  const ComTdbVirtTableKeyInfo *keyInfoArray = indexInfo->keyInfoArray;

  if (!keyInfoArray) return recLen;

  for (Int16 keyNum = 0; keyNum < keyCount; keyNum++) {
    const ComTdbVirtTableKeyInfo &keyInfo = keyInfoArray[keyNum];

    const ComTdbVirtTableColumnInfo &colInfo = columnInfoArray[keyInfo.tableColNum];
    recLen += colInfo.length;
  }

  if (indexInfo->nonKeyInfoArray) {
    keyCount = indexInfo->nonKeyColCount;
    keyInfoArray = indexInfo->nonKeyInfoArray;

    for (Int16 keyNum = 0; keyNum < keyCount; keyNum++) {
      const ComTdbVirtTableKeyInfo &keyInfo = keyInfoArray[keyNum];

      const ComTdbVirtTableColumnInfo &colInfo = columnInfoArray[keyInfo.tableColNum];
      recLen += colInfo.length;
    }
  }

  return recLen;
}

TrafDesc *Generator::createColDescs(const char *tableName, ComTdbVirtTableColumnInfo *columnInfo, Int16 numCols,
                                    UInt32 &offset, NAMemory *space) {
  if (!columnInfo) return NULL;

  TrafDesc *first_col_desc = NULL;
  TrafDesc *prev_desc = NULL;
  for (Int16 colNum = 0; colNum < numCols; colNum++) {
    ComTdbVirtTableColumnInfo *info = columnInfo + colNum;

    UInt32 colOffset = ExpTupleDesc::sqlarkExplodedOffsets(offset, info->length, (Int16)info->datatype, info->nullable);

    int i = colNum;               // Don't want colNum altered by the call
    int tmpOffset = (int)offset;  // Ignore returned offset
    SQLCHARSET_CODE info_charset = info->charset;
    if (info_charset == SQLCHARSETCODE_UNKNOWN &&
        (info->datatype == REC_NCHAR_V_UNICODE || info->datatype == REC_NCHAR_F_UNICODE ||
         info->datatype == REC_NCHAR_V_ANSI_UNICODE))
      info_charset = SQLCHARSETCODE_UCS2;

    char *colname = new GENHEAP(space) char[strlen(info->colName) + 1];
    strcpy(colname, info->colName);

    TrafDesc *col_desc =
        TrafMakeColumnDesc(tableName,
                           colname,  // info->colName,
                           i, info->datatype, info->length, tmpOffset, info->nullable, info_charset, space);

    // Virtual tables use SQLARK_EXPLODED_FORMAT in which numeric column
    // values are aligned.  Ignore TrafMakeColumnDesc's
    // offset calculation which doesn't reflect column value alignment.
    offset = colOffset + info->length;

    // EXPLAIN__ table uses 22-bit precision REAL values
    if (info->datatype == REC_FLOAT32) col_desc->columnsDesc()->precision = 22;

    col_desc->columnsDesc()->precision = info->precision;
    if (DFS2REC::isInterval(info->datatype)) col_desc->columnsDesc()->intervalleadingprec = info->precision;

    col_desc->columnsDesc()->scale = info->scale;
    if ((DFS2REC::isInterval(info->datatype)) || (DFS2REC::isDateTime(info->datatype)))
      col_desc->columnsDesc()->datetimefractprec = info->scale;

    col_desc->columnsDesc()->datetimestart = (rec_datetime_field)info->dtStart;
    col_desc->columnsDesc()->datetimeend = (rec_datetime_field)info->dtEnd;

    col_desc->columnsDesc()->setUpshifted(info->upshifted);
    col_desc->columnsDesc()->setCaseInsensitive(FALSE);

    char pt[350];
    NAType::convertTypeToText(pt,  // OUT
                              col_desc->columnsDesc()->datatype, col_desc->columnsDesc()->length,
                              col_desc->columnsDesc()->precision, col_desc->columnsDesc()->scale,
                              col_desc->columnsDesc()->datetimeStart(), col_desc->columnsDesc()->datetimeEnd(),
                              col_desc->columnsDesc()->datetimefractprec, col_desc->columnsDesc()->intervalleadingprec,
                              col_desc->columnsDesc()->isUpshifted(), col_desc->columnsDesc()->isCaseInsensitive(),
                              (CharInfo::CharSet)col_desc->columnsDesc()->character_set,
                              (CharInfo::Collation)col_desc->columnsDesc()->collation_sequence, NULL);
    col_desc->columnsDesc()->pictureText = new GENHEAP(space) char[strlen(pt) + 1];
    strcpy(col_desc->columnsDesc()->pictureText, pt);
    col_desc->columnsDesc()->setDefaultClass(info->defaultClass);
    if ((info->defaultClass == COM_NO_DEFAULT) || (info->defVal == NULL))
      col_desc->columnsDesc()->defaultvalue = NULL;
    else {
      col_desc->columnsDesc()->defaultvalue = new GENHEAP(space) char[strlen(info->defVal) + 1];
      strcpy(col_desc->columnsDesc()->defaultvalue, (char *)info->defVal);
    }

    if ((info->defaultClass == COM_CURRENT_DEFAULT || info->defaultClass == COM_CATALOG_FUNCTION_DEFAULT ||
         info->defaultClass == COM_SCHEMA_FUNCTION_DEFAULT) &&
        (info->initDefVal != NULL) && (strlen(info->initDefVal) > 0)) {
      col_desc->columnsDesc()->initDefaultValue = new GENHEAP(space) char[strlen(info->initDefVal) + 1];
      strcpy(col_desc->columnsDesc()->initDefaultValue, (char *)info->initDefVal);
    } else
      col_desc->columnsDesc()->initDefaultValue = NULL;

    col_desc->columnsDesc()->colclass = *(char *)COM_USER_COLUMN_LIT;
    col_desc->columnsDesc()->setAdded(FALSE);
    if (info->columnClass == COM_SYSTEM_COLUMN)
      col_desc->columnsDesc()->colclass = *(char *)COM_SYSTEM_COLUMN_LIT;
    else if (info->columnClass == COM_ADDED_USER_COLUMN) {
      col_desc->columnsDesc()->colclass = *(char *)COM_ADDED_USER_COLUMN_LIT;
      col_desc->columnsDesc()->setAdded(TRUE);
    } else if (info->columnClass == COM_ADDED_ALTERED_USER_COLUMN) {
      col_desc->columnsDesc()->colclass = *(char *)COM_ADDED_ALTERED_USER_COLUMN_LIT;
      col_desc->columnsDesc()->setAdded(TRUE);
    } else if (info->columnClass == COM_ALTERED_USER_COLUMN) {
      col_desc->columnsDesc()->colclass = *(char *)COM_ALTERED_USER_COLUMN_LIT;
    }

    if (info->colHeading) {
      col_desc->columnsDesc()->heading = new GENHEAP(space) char[strlen(info->colHeading) + 1];
      strcpy(col_desc->columnsDesc()->heading, info->colHeading);
    } else
      col_desc->columnsDesc()->heading = NULL;

    if (info->hbaseColFam) {
      col_desc->columnsDesc()->hbaseColFam = new GENHEAP(space) char[strlen(info->hbaseColFam) + 1];
      strcpy(col_desc->columnsDesc()->hbaseColFam, (char *)info->hbaseColFam);
    } else
      col_desc->columnsDesc()->hbaseColFam = NULL;

    if (info->hbaseColQual) {
      col_desc->columnsDesc()->hbaseColQual = new GENHEAP(space) char[strlen(info->hbaseColQual) + 1];
      strcpy(col_desc->columnsDesc()->hbaseColQual, (char *)info->hbaseColQual);
    } else
      col_desc->columnsDesc()->hbaseColQual = NULL;

    col_desc->columnsDesc()->hbaseColFlags = info->hbaseColFlags;

    col_desc->columnsDesc()->setParamDirection(CmGetComDirectionAsComParamDirection(info->paramDirection));
    col_desc->columnsDesc()->setOptional(info->isOptional);
    col_desc->columnsDesc()->colFlags = info->colFlags;

    if (info->colFlags & SEABASE_COLUMN_IS_COMPOSITE) {
      if (info->compDefnStr) {
        col_desc->columnsDesc()->compDefnStr = new GENHEAP(space) char[strlen(info->compDefnStr) + 1];
        strcpy(col_desc->columnsDesc()->compDefnStr, (char *)info->compDefnStr);
      }
    }

    if (!first_col_desc)
      first_col_desc = col_desc;
    else
      prev_desc->next = col_desc;

    prev_desc = col_desc;
  }  // for

  return first_col_desc;
}

static void initKeyDescStruct(TrafKeysDesc *tgt, const ComTdbVirtTableKeyInfo *src, NAMemory *space) {
  if (src->colName) {
    tgt->keyname = new GENHEAP(space) char[strlen(src->colName) + 1];
    strcpy(tgt->keyname, src->colName);
  } else
    tgt->keyname = NULL;

  tgt->keyseqnumber = src->keySeqNum;
  tgt->tablecolnumber = src->tableColNum;
  tgt->setDescending(src->ordering ? TRUE : FALSE);

  if (src->nonKeyCol == 2) tgt->setAddnlCol(TRUE);

  if (src->hbaseColFam) {
    tgt->hbaseColFam = new GENHEAP(space) char[strlen(src->hbaseColFam) + 1];
    strcpy(tgt->hbaseColFam, src->hbaseColFam);
  } else
    tgt->hbaseColFam = NULL;

  if (src->hbaseColQual) {
    tgt->hbaseColQual = new GENHEAP(space) char[strlen(src->hbaseColQual) + 1];
    strcpy(tgt->hbaseColQual, src->hbaseColQual);
  } else
    tgt->hbaseColQual = NULL;
}

TrafDesc *Generator::createKeyDescs(int numKeys, const ComTdbVirtTableKeyInfo *keyInfo, NAMemory *space) {
  TrafDesc *first_key_desc = NULL;

  if (keyInfo == NULL) return NULL;

  // create key descs
  TrafDesc *prev_desc = NULL;
  for (int keyNum = 0; keyNum < numKeys; keyNum++) {
    TrafDesc *key_desc = TrafAllocateDDLdesc(DESC_KEYS_TYPE, space);
    if (prev_desc)
      prev_desc->next = key_desc;
    else
      first_key_desc = key_desc;

    prev_desc = key_desc;

    initKeyDescStruct(key_desc->keysDesc(), &keyInfo[keyNum], space);
  }

  return first_key_desc;
}

TrafDesc *Generator::createConstrKeyColsDescs(int numKeys, ComTdbVirtTableKeyInfo *keyInfo,
                                              ComTdbVirtTableColumnInfo *columnInfo, NAMemory *space) {
  TrafDesc *first_key_desc = NULL;

  if (keyInfo == NULL) return NULL;

  // create key descs
  TrafDesc *prev_desc = NULL;
  for (int keyNum = 0; keyNum < numKeys; keyNum++) {
    TrafDesc *key_desc = TrafAllocateDDLdesc(DESC_CONSTRNT_KEY_COLS_TYPE, space);
    if (prev_desc)
      prev_desc->next = key_desc;
    else
      first_key_desc = key_desc;

    prev_desc = key_desc;

    ComTdbVirtTableKeyInfo *src = &keyInfo[keyNum];
    TrafConstrntKeyColsDesc *tgt = key_desc->constrntKeyColsDesc();
    if (src->colName) {
      tgt->colname = new GENHEAP(space) char[strlen(src->colName) + 1];
      strcpy(tgt->colname, src->colName);
    } else
      tgt->colname = NULL;

    tgt->position = src->tableColNum;
    ComTdbVirtTableColumnInfo *info = columnInfo + src->tableColNum;
    if (info->columnClass == COM_SYSTEM_COLUMN) tgt->setSystemKey(TRUE);
  }

  return first_key_desc;
}

// ****************************************************************************
// This method creates a set of trafodion descriptors (TrafDesc) based on
// ComTdbVirtTablePrivInfo
//
// see ComTdb.h for a description of the ComTdbVirtTablePrivInfo
// see TrafDDLdesc.h for a description of TrafDesc for the priv_desc
// ****************************************************************************
TrafDesc *Generator::createPrivDescs(const ComTdbVirtTablePrivInfo *privInfo, NAMemory *space) {
  // When authorization is enabled, each object must have at least one grantee
  // - the system grant to the object owner
  PrivMgrDescList *privGrantees = privInfo[0].privmgr_desc_list;
  DCMPASSERT(privGrantees->entries() > 0);

  TrafDesc *priv_desc = TrafAllocateDDLdesc(DESC_PRIV_TYPE, space);
  TrafDesc *first_grantee_desc = NULL;
  TrafDesc *prev_grantee_desc = NULL;

  // generate a TrafPrivGranteeDesc for each grantee and
  // attach to the privileges descriptor (priv_desc)
  for (int i = 0; i < privGrantees->entries(); i++) {
    PrivMgrDesc *granteeDesc = (*privGrantees)[i];
    TrafDesc *curr_grantee_desc = TrafAllocateDDLdesc(DESC_PRIV_GRANTEE_TYPE, space);
    if (!first_grantee_desc) first_grantee_desc = curr_grantee_desc;

    curr_grantee_desc->privGranteeDesc()->grantee = granteeDesc->getGrantee();
    curr_grantee_desc->privGranteeDesc()->schemaUID = granteeDesc->getSchemaUID();

    // generate a TrafPrivBitmap for the schema level privs and
    // attach it to the privilege grantee descriptor (curr_grantee_desc)
    PrivMgrCoreDesc schDesc = granteeDesc->getSchemaPrivs();

    TrafDesc *sch_bitmap_desc = TrafAllocateDDLdesc(DESC_PRIV_BITMAP_TYPE, space);
    sch_bitmap_desc->privBitmapDesc()->columnOrdinal = -1;
    sch_bitmap_desc->privBitmapDesc()->privBitmap = schDesc.getPrivBitmap().to_ulong();
    sch_bitmap_desc->privBitmapDesc()->privWGOBitmap = schDesc.getWgoBitmap().to_ulong();
    curr_grantee_desc->privGranteeDesc()->schemaBitmap = sch_bitmap_desc;

    // generate a TrafPrivBitmap for the object level privs and
    // attach it to the privilege grantee descriptor (curr_grantee_desc)
    // if this is a schema, then get object privileges from schema privileges
    PrivMgrCoreDesc objDesc =
        (granteeDesc->isSchemaObject()) ? granteeDesc->getSchemaPrivs() : granteeDesc->getTablePrivs();

    TrafDesc *obj_bitmap_desc = TrafAllocateDDLdesc(DESC_PRIV_BITMAP_TYPE, space);
    obj_bitmap_desc->privBitmapDesc()->columnOrdinal = -1;
    obj_bitmap_desc->privBitmapDesc()->privBitmap = objDesc.getPrivBitmap().to_ulong();
    obj_bitmap_desc->privBitmapDesc()->privWGOBitmap = objDesc.getWgoBitmap().to_ulong();
    curr_grantee_desc->privGranteeDesc()->objectBitmap = obj_bitmap_desc;

    // generate a list of TrafPrivBitmapDesc, one for each column and
    // attach it to the TrafPrivGranteeDesc
    size_t numCols = granteeDesc->getColumnPrivs().entries();
    if (numCols > 0) {
      TrafDesc *first_col_desc = NULL;
      TrafDesc *prev_col_desc = NULL;
      for (int j = 0; j < numCols; j++) {
        const PrivMgrCoreDesc colBitmap = granteeDesc->getColumnPrivs()[j];
        TrafDesc *curr_col_desc = TrafAllocateDDLdesc(DESC_PRIV_BITMAP_TYPE, space);
        if (!first_col_desc) first_col_desc = curr_col_desc;

        curr_col_desc->privBitmapDesc()->columnOrdinal = colBitmap.getColumnOrdinal();
        curr_col_desc->privBitmapDesc()->privBitmap = colBitmap.getPrivBitmap().to_ulong();
        curr_col_desc->privBitmapDesc()->privWGOBitmap = colBitmap.getWgoBitmap().to_ulong();

        if (prev_col_desc) prev_col_desc->next = curr_col_desc;
        prev_col_desc = curr_col_desc;
      }
      curr_grantee_desc->privGranteeDesc()->columnBitmaps = first_col_desc;
    } else
      curr_grantee_desc->privGranteeDesc()->columnBitmaps = NULL;

    if (prev_grantee_desc) prev_grantee_desc->next = curr_grantee_desc;
    prev_grantee_desc = curr_grantee_desc;
  }
  priv_desc->privDesc()->privGrantees = first_grantee_desc;
  return priv_desc;
}

// this method is used to create both referencing and referenced constraint structs.
TrafDesc *Generator::createRefConstrDescStructs(int numConstrs, ComTdbVirtTableRefConstraints *refConstrs,
                                                NAMemory *space) {
  TrafDesc *first_constr_desc = NULL;

  if ((numConstrs == 0) || (refConstrs == NULL)) return NULL;

  // create constr descs
  TrafDesc *prev_desc = NULL;
  for (int constrNum = 0; constrNum < numConstrs; constrNum++) {
    TrafDesc *constr_desc = TrafAllocateDDLdesc(DESC_REF_CONSTRNTS_TYPE, space);
    if (prev_desc)
      prev_desc->next = constr_desc;
    else
      first_constr_desc = constr_desc;

    prev_desc = constr_desc;

    ComTdbVirtTableRefConstraints *src = &refConstrs[constrNum];
    TrafRefConstrntsDesc *tgt = constr_desc->refConstrntsDesc();
    if (src->constrName) {
      tgt->constrntname = new GENHEAP(space) char[strlen(src->constrName) + 1];
      strcpy(tgt->constrntname, src->constrName);
    } else
      tgt->constrntname = NULL;

    if (src->baseTableName) {
      tgt->tablename = new GENHEAP(space) char[strlen(src->baseTableName) + 1];
      strcpy(tgt->tablename, src->baseTableName);
    } else
      tgt->tablename = NULL;
  }

  return first_constr_desc;
}

static int createDescStructs(char *tableName, int numCols, ComTdbVirtTableColumnInfo *columnInfo, int numKeys,
                             ComTdbVirtTableKeyInfo *keyInfo, TrafDesc *&colDescs, TrafDesc *&keyDescs,
                             NAMemory *space) {
  colDescs = NULL;
  keyDescs = NULL;
  UInt32 reclen = 0;

  // create column descs
  colDescs = Generator::createColDescs(tableName, columnInfo, (Int16)numCols, reclen, space);

  keyDescs = Generator::createKeyDescs(numKeys, keyInfo, space);

  return (int)reclen;
}

static void populateRegionDescForEndKey(char *buf, int len, struct TrafDesc *target) {
  target->hbaseRegionDesc()->beginKey = NULL;
  target->hbaseRegionDesc()->beginKeyLen = 0;
  target->hbaseRegionDesc()->endKey = buf;
  target->hbaseRegionDesc()->endKeyLen = len;
}

static void populateRegionDescAsRANGE(char *buf, int len, struct TrafDesc *target) {
  target->nodetype = DESC_HBASE_RANGE_REGION_TYPE;
  populateRegionDescForEndKey(buf, len, target);
}

//
// Produce a list of TrafDesc objects. In each object, the body_struct
// field points at hbaseRegion_desc. The order of the keyinfo, obtained from
// org.apache.hadoop.hbase.client.HTable.getEndKey(), is preserved.
//
TrafDesc *Generator::assembleDescs(NAArray<HbaseStr> *keyArray, NAMemory *space) {
  if (keyArray == NULL) return NULL;

  TrafDesc *result = NULL;
  int entries = keyArray->entries();
  int len = 0;
  char *buf = NULL;

  for (int i = entries - 1; i >= 0; i--) {
    len = keyArray->at(i).len;
    if (len > 0) {
      buf = new GENHEAP(space) char[len];
      memcpy(buf, keyArray->at(i).val, len);
    } else
      buf = NULL;

    TrafDesc *wrapper = TrafAllocateDDLdesc(DESC_HBASE_RANGE_REGION_TYPE, space);

    populateRegionDescAsRANGE(buf, len, wrapper);

    wrapper->next = result;
    result = wrapper;
  }

  return result;
}

TrafDesc *Generator::createVirtualTableDesc(
    const char *inTableName, NAMemory *heap, int numCols, ComTdbVirtTableColumnInfo *columnInfo, int numKeys,
    ComTdbVirtTableKeyInfo *keyInfo, int numConstrs, ComTdbVirtTableConstraintInfo *constrInfo, int numIndexes,
    ComTdbVirtTableIndexInfo *indexInfo, int numViews, ComTdbVirtTableViewInfo *viewInfo,
    ComTdbVirtTableTableInfo *tableInfo, ComTdbVirtTableSequenceInfo *seqInfo, ComTdbVirtTableStatsInfo *statsInfo,
    NAArray<HbaseStr> *endKeyArray, NABoolean genPackedDesc, int *packedDescLen, NABoolean isSharedSchema,
    ComTdbVirtTablePrivInfo *privInfo, const char *nameSpace, ComTdbVirtTablePartitionV2Info *partitionV2Info) {
  // If genPackedDesc is set, then use Space class to allocate descriptors and
  // returned contiguous packed copy of it.
  // This packed copy will be stored in metadata.

  // If heap is set (and genPackedDesc is not set), use the heap passed to
  // us by our caller. For example, we might be called at NATableDB::get time,
  // to create descriptors that are going to live in the NATable cache or on
  // the statement heap rather than in a Generator space.

  // There is some danger in this mixed use of the "space" variable as a
  // base class pointer. The NAMemory and Space classes avoid using virtual
  // functions, so we can't count on polymorphism to pick the right
  // implementation of a method on "space". Rather, the NAMemory methods
  // will be used unless we explicitly override them. Fortunately, in
  // almost all of this method, and the methods it calls, the only use of
  // "space" is as an operand to the GENHEAP macro, which casts it as an
  // NAMemory * anyway, for use by operator new. That works for both classes.
  // There is one place in this method where we are concerned with contiguous
  // placement of objects. There, we have to cast the "space" variable to
  // class Space to get its methods. (Sorry about the variable naming; it
  // would have been a lot more changes to rename the "space" variable.)

  Space lSpace(ComSpace::GENERATOR_SPACE);
  NAMemory *space = NULL;
  if (genPackedDesc)
    space = &lSpace;
  else if (heap)
    space = heap;

  const char *tableName = (tableInfo ? tableInfo->tableName : inTableName);
  TrafDesc *table_desc = TrafAllocateDDLdesc(DESC_TABLE_TYPE, space);
  table_desc->tableDesc()->tablename = new GENHEAP(space) char[strlen(tableName) + 1];
  strcpy(table_desc->tableDesc()->tablename, tableName);

  table_desc->tableDesc()->tableDescFlags = 0;
  table_desc->tableDesc()->catUID = 0;
  table_desc->tableDesc()->schemaUID = 0;
  if (tableInfo) {
    table_desc->tableDesc()->createTime = tableInfo->createTime;
    table_desc->tableDesc()->redefTime = tableInfo->redefTime;
    table_desc->tableDesc()->objectUID = tableInfo->objUID;
    table_desc->tableDesc()->objDataUID = tableInfo->objDataUID;
    table_desc->tableDesc()->schemaUID = tableInfo->schemaUID;

    table_desc->tableDesc()->baseTableUID = tableInfo->baseTableUID;
  } else {
    table_desc->tableDesc()->createTime = 0;
    table_desc->tableDesc()->redefTime = 0;

    ComUID comUID;
    comUID.make_UID();
    long objUID = comUID.get_value();

    table_desc->tableDesc()->objectUID = objUID;
    table_desc->tableDesc()->objDataUID = objUID;
  }

  if (isSharedSchema)
    table_desc->tableDesc()->setSystemTableCode(FALSE);
  else
    table_desc->tableDesc()->setSystemTableCode(TRUE);

  table_desc->tableDesc()->setStorageType(COM_STORAGE_HBASE);
  if (tableInfo) {
    table_desc->tableDesc()->setRowFormat(tableInfo->rowFormat);

    if (tableInfo->xnRepl == COM_REPL_SYNC)
      table_desc->tableDesc()->setXnReplSync(TRUE);
    else if (tableInfo->xnRepl == COM_REPL_ASYNC)
      table_desc->tableDesc()->setXnReplAsync(TRUE);

    table_desc->tableDesc()->setStorageType(tableInfo->storageType);
  }

  if (CmpCommon::context()->sqlSession()->validateVolatileName(tableName))
    table_desc->tableDesc()->setVolatileTable(TRUE);
  else
    table_desc->tableDesc()->setVolatileTable(FALSE);

  // view, the view object
  if (numViews > 0) table_desc->tableDesc()->setObjectType(COM_VIEW_OBJECT);

  // if have sequence info, then sequence object
  else if ((seqInfo) && (!columnInfo))
    table_desc->tableDesc()->setObjectType(COM_SEQUENCE_GENERATOR_OBJECT);

  // If not as sequence and no columns defined, then a schema object
  else if ((!seqInfo) && (!columnInfo))
    if (isSharedSchema)
      table_desc->tableDesc()->setObjectType(COM_SHARED_SCHEMA_OBJECT);
    else
      table_desc->tableDesc()->setObjectType(COM_PRIVATE_SCHEMA_OBJECT);

  // It is a table object
  else
    table_desc->tableDesc()->setObjectType(COM_BASE_TABLE_OBJECT);

  table_desc->tableDesc()->owner = (tableInfo ? tableInfo->objOwnerID : SUPER_USER);
  table_desc->tableDesc()->schemaOwner = (tableInfo ? tableInfo->schemaOwnerID : SUPER_USER);

  if (tableInfo && tableInfo->defaultColFam) {
    table_desc->tableDesc()->default_col_fam = new GENHEAP(space) char[strlen(tableInfo->defaultColFam) + 1];
    strcpy(table_desc->tableDesc()->default_col_fam, tableInfo->defaultColFam);
  }

  if (tableInfo && tableInfo->allColFams) {
    table_desc->tableDesc()->all_col_fams = new GENHEAP(space) char[strlen(tableInfo->allColFams) + 1];
    strcpy(table_desc->tableDesc()->all_col_fams, tableInfo->allColFams);
  }

  if (tableInfo && tableInfo->tableNamespace) {
    table_desc->tableDesc()->tableNamespace = new GENHEAP(space) char[strlen(tableInfo->tableNamespace) + 1];
    strcpy(table_desc->tableDesc()->tableNamespace, tableInfo->tableNamespace);
  } else if (nameSpace) {
    table_desc->tableDesc()->tableNamespace = new GENHEAP(space) char[strlen(nameSpace) + 1];
    strcpy(table_desc->tableDesc()->tableNamespace, nameSpace);
  }

  table_desc->tableDesc()->objectFlags = (tableInfo ? tableInfo->objectFlags : 0);
  table_desc->tableDesc()->tablesFlags = (tableInfo ? tableInfo->tablesFlags : 0);

  table_desc->tableDesc()->numLOBdatafiles_ = -1;
  if (tableInfo) table_desc->tableDesc()->numLOBdatafiles_ = tableInfo->numLOBdatafiles;

  TrafDesc *files_desc = TrafAllocateDDLdesc(DESC_FILES_TYPE, space);
  files_desc->filesDesc()->setAudited(tableInfo ? tableInfo->isAudited : -1);

  table_desc->tableDesc()->files_desc = files_desc;

  TrafDesc *cols_descs = NULL;
  TrafDesc *keys_descs = NULL;
  table_desc->tableDesc()->colcount = numCols;
  table_desc->tableDesc()->record_length = createDescStructs(table_desc->tableDesc()->tablename, numCols, columnInfo,
                                                             numKeys, keyInfo, cols_descs, keys_descs, space);

  TrafDesc *first_constr_desc = NULL;
  if (numConstrs > 0) {
    TrafDesc *prev_desc = NULL;
    for (int i = 0; i < numConstrs; i++) {
      TrafDesc *curr_constr_desc = TrafAllocateDDLdesc(DESC_CONSTRNTS_TYPE, space);

      if (!first_constr_desc) first_constr_desc = curr_constr_desc;

      curr_constr_desc->constrntsDesc()->tablename = new GENHEAP(space) char[strlen(constrInfo[i].baseTableName) + 1];
      strcpy(curr_constr_desc->constrntsDesc()->tablename, constrInfo[i].baseTableName);

      curr_constr_desc->constrntsDesc()->constrntname = new GENHEAP(space) char[strlen(constrInfo[i].constrName) + 1];
      strcpy(curr_constr_desc->constrntsDesc()->constrntname, constrInfo[i].constrName);

      curr_constr_desc->constrntsDesc()->check_constrnts_desc = NULL;
      curr_constr_desc->constrntsDesc()->setEnforced(constrInfo[i].isEnforced);
      curr_constr_desc->constrntsDesc()->setNotSerialized(constrInfo[i].notSerialized);

      switch (constrInfo[i].constrType) {
        case 0:  // unique_constr
          curr_constr_desc->constrntsDesc()->type = UNIQUE_CONSTRAINT;
          break;

        case 1:  // ref_constr
          curr_constr_desc->constrntsDesc()->type = REF_CONSTRAINT;
          break;

        case 2:  // check_constr
          curr_constr_desc->constrntsDesc()->type = CHECK_CONSTRAINT;
          break;

        case 3:  // pkey_constr
          curr_constr_desc->constrntsDesc()->type = PRIMARY_KEY_CONSTRAINT;
          break;

      }  // switch

      curr_constr_desc->constrntsDesc()->colcount = constrInfo[i].colCount;

      curr_constr_desc->constrntsDesc()->constr_key_cols_desc =
          Generator::createConstrKeyColsDescs(constrInfo[i].colCount, constrInfo[i].keyInfoArray, columnInfo, space);

      if (constrInfo[i].ringConstrArray) {
        curr_constr_desc->constrntsDesc()->referencing_constrnts_desc =
            Generator::createRefConstrDescStructs(constrInfo[i].numRingConstr, constrInfo[i].ringConstrArray, space);
      }

      if (constrInfo[i].refdConstrArray) {
        curr_constr_desc->constrntsDesc()->referenced_constrnts_desc =
            Generator::createRefConstrDescStructs(constrInfo[i].numRefdConstr, constrInfo[i].refdConstrArray, space);
      }

      if ((constrInfo[i].constrType == 2) &&  // check constr
          (constrInfo[i].checkConstrLen > 0)) {
        TrafDesc *check_constr_desc = TrafAllocateDDLdesc(DESC_CHECK_CONSTRNTS_TYPE, space);

        check_constr_desc->checkConstrntsDesc()->constrnt_text =
            new GENHEAP(space) char[constrInfo[i].checkConstrLen + 1];
        memcpy(check_constr_desc->checkConstrntsDesc()->constrnt_text, constrInfo[i].checkConstrText,
               constrInfo[i].checkConstrLen);
        check_constr_desc->checkConstrntsDesc()->constrnt_text[constrInfo[i].checkConstrLen] = 0;

        curr_constr_desc->constrntsDesc()->check_constrnts_desc = check_constr_desc;
      }

      if (prev_desc) prev_desc->next = curr_constr_desc;

      prev_desc = curr_constr_desc;
    }  // for
  }

  TrafDesc *index_desc = TrafAllocateDDLdesc(DESC_INDEXES_TYPE, space);
  index_desc->indexesDesc()->tablename = table_desc->tableDesc()->tablename;
  index_desc->indexesDesc()->indexname = table_desc->tableDesc()->tablename;
  index_desc->indexesDesc()->keytag = 0;  // primary index
  index_desc->indexesDesc()->indexUID = 0;
  index_desc->indexesDesc()->record_length = table_desc->tableDesc()->record_length;
  index_desc->indexesDesc()->colcount = table_desc->tableDesc()->colcount;
  index_desc->indexesDesc()->blocksize = 32 * 1024;
  index_desc->indexesDesc()->setVolatile(table_desc->tableDesc()->isVolatileTable());
  index_desc->indexesDesc()->hbaseCreateOptions = NULL;
  index_desc->indexesDesc()->numSaltPartns = 0;
  index_desc->indexesDesc()->numInitialSaltRegions = -1;
  index_desc->indexesDesc()->hbaseSplitClause = NULL;
  index_desc->indexesDesc()->setRowFormat(table_desc->tableDesc()->rowFormat());

  if (tableInfo) {
    index_desc->indexesDesc()->indexUID = tableInfo->objUID;

    index_desc->indexesDesc()->numSaltPartns = tableInfo->numSaltPartns;

    index_desc->indexesDesc()->numInitialSaltRegions = tableInfo->numInitialSaltRegions;
    if (tableInfo->hbaseSplitClause) {
      index_desc->indexesDesc()->hbaseSplitClause = new HEAP char[strlen(tableInfo->hbaseSplitClause) + 1];
      strcpy(index_desc->indexesDesc()->hbaseSplitClause, tableInfo->hbaseSplitClause);
    }

    if (tableInfo->hbaseCreateOptions) {
      index_desc->indexesDesc()->hbaseCreateOptions =
          new GENHEAP(space) char[strlen(tableInfo->hbaseCreateOptions) + 1];
      strcpy(index_desc->indexesDesc()->hbaseCreateOptions, tableInfo->hbaseCreateOptions);
    }
  }

  if (numIndexes > 0) {
    TrafDesc *prev_desc = index_desc;
    for (int i = 0; i < numIndexes; i++) {
      TrafDesc *curr_index_desc = TrafAllocateDDLdesc(DESC_INDEXES_TYPE, space);

      prev_desc->next = curr_index_desc;

      curr_index_desc->indexesDesc()->tablename = new GENHEAP(space) char[strlen(indexInfo[i].baseTableName) + 1];
      strcpy(curr_index_desc->indexesDesc()->tablename, indexInfo[i].baseTableName);

      curr_index_desc->indexesDesc()->indexname = new GENHEAP(space) char[strlen(indexInfo[i].indexName) + 1];
      strcpy(curr_index_desc->indexesDesc()->indexname, indexInfo[i].indexName);

      curr_index_desc->indexesDesc()->indexUID = indexInfo[i].indexUID;
      curr_index_desc->indexesDesc()->keytag = indexInfo[i].keytag;
      curr_index_desc->indexesDesc()->setUnique(indexInfo[i].isUnique);
      // set ngram flag
      curr_index_desc->indexesDesc()->setNgram(indexInfo[i].isNgram);

      if (CmpSeabaseDDL::isMDflagsSet(indexInfo[i].indexFlags, MD_IS_LOCAL_BASE_PARTITION_FLG))
        curr_index_desc->indexesDesc()->setPartLocalBaseIndex(TRUE);

      if (CmpSeabaseDDL::isMDflagsSet(indexInfo[i].indexFlags, MD_IS_LOCAL_PARTITION_FLG))
        curr_index_desc->indexesDesc()->setPartLocalIndex(TRUE);

      if (CmpSeabaseDDL::isMDflagsSet(indexInfo[i].indexFlags, MD_IS_GLOBAL_PARTITION_FLG))
        curr_index_desc->indexesDesc()->setPartGlobalIndex(TRUE);

      curr_index_desc->indexesDesc()->setExplicit(indexInfo[i].isExplicit);
      curr_index_desc->indexesDesc()->record_length = getRecordLength(&indexInfo[i], columnInfo);
      curr_index_desc->indexesDesc()->colcount = indexInfo[i].keyColCount + indexInfo[i].nonKeyColCount;
      curr_index_desc->indexesDesc()->blocksize = 32 * 1024;

      curr_index_desc->indexesDesc()->keys_desc =
          Generator::createKeyDescs(indexInfo[i].keyColCount, indexInfo[i].keyInfoArray, space);

      curr_index_desc->indexesDesc()->non_keys_desc =
          Generator::createKeyDescs(indexInfo[i].nonKeyColCount, indexInfo[i].nonKeyInfoArray, space);

      if (CmpCommon::context()->sqlSession()->validateVolatileName(indexInfo[i].indexName))
        curr_index_desc->indexesDesc()->setVolatile(TRUE);
      else
        curr_index_desc->indexesDesc()->setVolatile(FALSE);
      curr_index_desc->indexesDesc()->hbaseCreateOptions = NULL;
      curr_index_desc->indexesDesc()->numSaltPartns = indexInfo[i].numSaltPartns;
      if (curr_index_desc->indexesDesc()->numSaltPartns > 0) {
        // that the index is salted like the base table
        TrafDesc *ci_files_desc = TrafAllocateDDLdesc(DESC_FILES_TYPE, space);
        ci_files_desc->filesDesc()->setAudited(TRUE);  // audited table
        curr_index_desc->indexesDesc()->files_desc = ci_files_desc;
      }
      curr_index_desc->indexesDesc()->setRowFormat(indexInfo[i].rowFormat);
      if (indexInfo[i].hbaseCreateOptions) {
        curr_index_desc->indexesDesc()->hbaseCreateOptions =
            new GENHEAP(space) char[strlen(indexInfo[i].hbaseCreateOptions) + 1];
        strcpy(curr_index_desc->indexesDesc()->hbaseCreateOptions, indexInfo[i].hbaseCreateOptions);
      }

      prev_desc = curr_index_desc;
    }
  }

  TrafDesc *view_desc = NULL;
  if (numViews > 0) {
    view_desc = TrafAllocateDDLdesc(DESC_VIEW_TYPE, space);

    view_desc->viewDesc()->viewname = new GENHEAP(space) char[strlen(viewInfo[0].viewName) + 1];
    strcpy(view_desc->viewDesc()->viewname, viewInfo[0].viewName);

    view_desc->viewDesc()->viewfilename = view_desc->viewDesc()->viewname;
    view_desc->viewDesc()->viewtext = new GENHEAP(space) char[strlen(viewInfo[0].viewText) + 1];
    strcpy(view_desc->viewDesc()->viewtext, viewInfo[0].viewText);

    view_desc->viewDesc()->viewtextcharset = (CharInfo::CharSet)SQLCHARSETCODE_UTF8;

    if (viewInfo[0].viewCheckText) {
      view_desc->viewDesc()->viewchecktext = new GENHEAP(space) char[strlen(viewInfo[0].viewCheckText) + 1];
      strcpy(view_desc->viewDesc()->viewchecktext, viewInfo[0].viewCheckText);
    } else
      view_desc->viewDesc()->viewchecktext = NULL;

    if (viewInfo[0].viewColUsages) {
      view_desc->viewDesc()->viewcolusages = new GENHEAP(space) char[strlen(viewInfo[0].viewColUsages) + 1];
      strcpy(view_desc->viewDesc()->viewcolusages, viewInfo[0].viewColUsages);
    } else
      view_desc->viewDesc()->viewcolusages = NULL;

    view_desc->viewDesc()->setUpdatable(viewInfo[0].isUpdatable);
    view_desc->viewDesc()->setInsertable(viewInfo[0].isInsertable);
  }

  TrafDesc *seq_desc = NULL;
  if (seqInfo) {
    seq_desc = TrafAllocateDDLdesc(DESC_SEQUENCE_GENERATOR_TYPE, space);

    seq_desc->sequenceGeneratorDesc()->setSgType((ComSequenceGeneratorType)seqInfo->seqType);

    seq_desc->sequenceGeneratorDesc()->fsDataType = (ComFSDataType)seqInfo->datatype;
    seq_desc->sequenceGeneratorDesc()->startValue = seqInfo->startValue;
    seq_desc->sequenceGeneratorDesc()->increment = seqInfo->increment;

    seq_desc->sequenceGeneratorDesc()->maxValue = seqInfo->maxValue;
    seq_desc->sequenceGeneratorDesc()->minValue = seqInfo->minValue;
    seq_desc->sequenceGeneratorDesc()->cycleOption = (seqInfo->cycleOption ? TRUE : FALSE);
    seq_desc->sequenceGeneratorDesc()->cache = seqInfo->cache;
    seq_desc->sequenceGeneratorDesc()->objectUID = seqInfo->seqUID;

    seq_desc->sequenceGeneratorDesc()->nextValue = seqInfo->nextValue;
    seq_desc->sequenceGeneratorDesc()->redefTime = seqInfo->redefTime;
    if (CmpSeabaseDDL::isMDflagsSet(seqInfo->flags, MD_SEQGEN_ORDER_FLG))
      seq_desc->sequenceGeneratorDesc()->seqOrder = true;

    if (CmpSeabaseDDL::isMDflagsSet(seqInfo->flags, MD_SEQGEN_REPL_SYNC_FLG))
      seq_desc->sequenceGeneratorDesc()->setXnReplSync(TRUE);
    else if (CmpSeabaseDDL::isMDflagsSet(seqInfo->flags, MD_SEQGEN_REPL_ASYNC_FLG))
      seq_desc->sequenceGeneratorDesc()->setXnReplAsync(TRUE);
  }

  // partition table
  TrafDesc *partitionv2_desc = NULL;
  if (partitionV2Info) {
    NABoolean isPartitionBaseTable = false;
    if ((tableInfo->objectFlags & MD_PARTITION_V2) != 0) isPartitionBaseTable = true;
    partitionv2_desc = TrafAllocateDDLdesc(DESC_PARTITIONV2_TYPE, space);
    partitionv2_desc->partitionV2Desc()->baseTableUid = partitionV2Info->baseTableUid;
    partitionv2_desc->partitionV2Desc()->partitionType = partitionV2Info->partitionType;
    partitionv2_desc->partitionV2Desc()->partitionColIdx =
        new GENHEAP(space) char[strlen(partitionV2Info->partitionColIdx) + 1];

    strcpy(partitionv2_desc->partitionV2Desc()->partitionColIdx, partitionV2Info->partitionColIdx);
    partitionv2_desc->partitionV2Desc()->partitionColCount = partitionV2Info->partitionColCount;
    partitionv2_desc->partitionV2Desc()->subpartitionType = partitionV2Info->subpartitionType;
    if (partitionV2Info->subpartitionColCount > 0) {
      partitionv2_desc->partitionV2Desc()->subpartitionColIdx =
          new GENHEAP(space) char[strlen(partitionV2Info->subpartitionColIdx) + 1];
      strcpy(partitionv2_desc->partitionV2Desc()->subpartitionColIdx, partitionV2Info->subpartitionColIdx);
    }
    partitionv2_desc->partitionV2Desc()->subpartitionColCount = partitionV2Info->subpartitionColCount;
    if (isPartitionBaseTable)
      partitionv2_desc->partitionV2Desc()->stlPartitionCnt = 1;
    else
      partitionv2_desc->partitionV2Desc()->stlPartitionCnt = partitionV2Info->stlPartitionCnt;
    partitionv2_desc->partitionV2Desc()->partitionInterval = NULL;
    partitionv2_desc->partitionV2Desc()->subpartitionInterval = NULL;
    partitionv2_desc->partitionV2Desc()->partitionAutolist = 0;
    partitionv2_desc->partitionV2Desc()->subpartitionAutolist = 0;
    partitionv2_desc->partitionV2Desc()->flags = partitionV2Info->flags;

    TrafDesc *firstl_prev_desc = NULL;
    ComTdbVirtTablePartInfo *partInfo = partitionV2Info->partArray;
    int startwith = 0;
    if (isPartitionBaseTable && partitionV2Info->stlPartitionCnt == 2) startwith = 1;
    for (int i = startwith; i < partitionV2Info->stlPartitionCnt; i++) {
      TrafDesc *st_curr_desc = TrafAllocateDDLdesc(DESC_PART_TYPE, space);
      TrafPartDesc *pdesc = st_curr_desc->partDesc();
      pdesc->parentUid = partInfo[i].parentUid;
      pdesc->partitionUid = partInfo[i].partitionUid;
      pdesc->partitionName = new GENHEAP(space) char[strlen(partInfo[i].partitionName) + 1];
      strcpy(pdesc->partitionName, partInfo[i].partitionName);
      pdesc->partitionEntityName = new GENHEAP(space) char[strlen(partInfo[i].partitionEntityName) + 1];
      strcpy(pdesc->partitionEntityName, partInfo[i].partitionEntityName);
      pdesc->isSubPartition = partInfo[i].isSubPartition;
      pdesc->hasSubPartition = partInfo[i].hasSubPartition;
      pdesc->partPosition = partInfo[i].partPosition;
      pdesc->partitionValueExpr = new GENHEAP(space) char[partInfo[i].partExpressionLen + 1];
      memset(pdesc->partitionValueExpr, '\0', partInfo[i].partExpressionLen + 1);
      memcpy(pdesc->partitionValueExpr, partInfo[i].partExpression, partInfo[i].partExpressionLen);
      pdesc->partitionValueExprLen = partInfo[i].partExpressionLen;
      if (i == 0)  // first partition
      {
        pdesc->prevPartitionValueExpr = NULL;
        pdesc->prevPartitionValueExprLen = 0;
      } else {
        pdesc->prevPartitionValueExpr = new GENHEAP(space) char[partInfo[i - 1].partExpressionLen + 1];
        memset(pdesc->prevPartitionValueExpr, '\0', partInfo[i - 1].partExpressionLen + 1);
        memcpy(pdesc->prevPartitionValueExpr, partInfo[i - 1].partExpression, partInfo[i - 1].partExpressionLen);
        pdesc->prevPartitionValueExprLen = partInfo[i - 1].partExpressionLen;
      }
      pdesc->isValid = partInfo[i].isValid;
      pdesc->isReadonly = partInfo[i].isReadonly;
      pdesc->isInMemory = partInfo[i].isInMemory;
      pdesc->defTime = partInfo[i].defTime;
      pdesc->flags = partInfo[i].flags;

      pdesc->subpartitionCnt = partInfo[i].subpartitionCnt;
      if (partInfo[i].subpartitionCnt > 0) {
        TrafDesc *sub_prev_desc = NULL;
        ComTdbVirtTablePartInfo *subpartInfo = partInfo[i].subPartArray;
        for (int j = 0; j < partInfo[i].subpartitionCnt; j++) {
          TrafDesc *sub_curr_desc = TrafAllocateDDLdesc(DESC_PART_TYPE, space);
          TrafPartDesc *subdesc = sub_curr_desc->partDesc();
          subdesc->parentUid = pdesc->partitionUid;
          subdesc->partitionUid = subpartInfo[j].partitionUid;
          subdesc->partitionName = new GENHEAP(space) char[strlen(subpartInfo[j].partitionName) + 1];
          strcpy(subdesc->partitionName, subpartInfo[j].partitionName);
          subdesc->partitionEntityName = new GENHEAP(space) char[strlen(subpartInfo[j].partitionEntityName) + 1];
          strcpy(subdesc->partitionEntityName, subpartInfo[j].partitionEntityName);
          subdesc->isSubPartition = subpartInfo[j].isSubPartition;
          subdesc->hasSubPartition = subpartInfo[j].hasSubPartition;
          subdesc->partPosition = subpartInfo[j].partPosition;
          subdesc->partitionValueExpr = new GENHEAP(space) char[subpartInfo[j].partExpressionLen + 1];
          memset(subdesc->partitionValueExpr, '\0', subpartInfo[j].partExpressionLen + 1);
          memcpy(subdesc->partitionValueExpr, subpartInfo[j].partExpression, subpartInfo[j].partExpressionLen);
          subdesc->partitionValueExprLen = subpartInfo[j].partExpressionLen;

          if (j == 0)  // first subpartition
          {
            subdesc->prevPartitionValueExpr = NULL;
            subdesc->prevPartitionValueExprLen = 0;
          } else {
            subdesc->prevPartitionValueExpr = new GENHEAP(space) char[subpartInfo[j - 1].partExpressionLen + 1];
            memset(subdesc->prevPartitionValueExpr, '\0', subpartInfo[j - 1].partExpressionLen + 1);
            memcpy(subdesc->prevPartitionValueExpr, subpartInfo[j - 1].partExpression,
                   subpartInfo[j - 1].partExpressionLen);
            subdesc->prevPartitionValueExprLen = subpartInfo[j - 1].partExpressionLen;
          }

          subdesc->isValid = subpartInfo[j].isValid;
          subdesc->isReadonly = subpartInfo[j].isReadonly;
          subdesc->isInMemory = subpartInfo[j].isInMemory;
          subdesc->defTime = subpartInfo[j].defTime;
          subdesc->flags = subpartInfo[j].flags;
          subdesc->subpartitionCnt = 0;

          if (!sub_prev_desc)
            pdesc->subpart_desc = sub_curr_desc;
          else
            sub_prev_desc->next = sub_curr_desc;
          sub_prev_desc = sub_curr_desc;
        }
      }
      if (!firstl_prev_desc)
        partitionv2_desc->partitionV2Desc()->part_desc = st_curr_desc;
      else
        firstl_prev_desc->next = st_curr_desc;
      firstl_prev_desc = st_curr_desc;
    }
  }

  // Setup the privilege descriptors for objects including views, tables,
  // libraries, udrs, sequences, and constraints.
  TrafDesc *priv_desc = NULL;
  if (privInfo) priv_desc = createPrivDescs(privInfo, space);

  TrafDesc *i_files_desc = TrafAllocateDDLdesc(DESC_FILES_TYPE, space);
  i_files_desc->filesDesc()->setAudited(TRUE);  // audited table
  index_desc->indexesDesc()->files_desc = i_files_desc;

  index_desc->indexesDesc()->keys_desc = keys_descs;
  table_desc->tableDesc()->columns_desc = cols_descs;
  table_desc->tableDesc()->indexes_desc = index_desc;
  table_desc->tableDesc()->views_desc = view_desc;
  table_desc->tableDesc()->constrnts_desc = first_constr_desc;
  table_desc->tableDesc()->constr_count = numConstrs;
  table_desc->tableDesc()->sequence_generator_desc = seq_desc;
  table_desc->tableDesc()->priv_desc = priv_desc;
  table_desc->tableDesc()->partitionv2_desc = partitionv2_desc;

  if (statsInfo) {
    TrafDesc *stats_desc = NULL;

    stats_desc = TrafAllocateDDLdesc(DESC_TABLE_STATS_TYPE, space);
    stats_desc->tableStatsDesc()->setFastStats(TRUE);
    stats_desc->tableStatsDesc()->rowcount = statsInfo->rowcount;
    stats_desc->tableStatsDesc()->hbtBlockSize = statsInfo->hbtBlockSize;
    stats_desc->tableStatsDesc()->hbtIndexLevels = statsInfo->hbtIndexLevels;
    stats_desc->tableStatsDesc()->numHistograms = statsInfo->numHistograms;
    stats_desc->tableStatsDesc()->numHistIntervals = statsInfo->numHistIntervals;

    if (statsInfo->histogramInfo) {
      TrafDesc *prev_desc = NULL;
      ComTdbVirtTableHistogramInfo *histInfo = statsInfo->histogramInfo;
      for (int i = 0; i < statsInfo->numHistograms; i++) {
        TrafHistogramDesc *curr_desc = (TrafHistogramDesc *)TrafAllocateDDLdesc(DESC_HISTOGRAM_TYPE, space);

        // copy info from histInfo[i].histogramDesc_ to curr_desc
        curr_desc->copyFrom(&histInfo[i].histogramDesc_, space);

        if (!prev_desc) {
          stats_desc->tableStatsDesc()->histograms_desc = curr_desc;
        } else
          prev_desc->next = curr_desc;

        prev_desc = curr_desc;
      }  // for

      if (statsInfo->histintInfo) {
        prev_desc = NULL;
        ComTdbVirtTableHistintInfo *histInfo = statsInfo->histintInfo;

        for (int i = 0; i < statsInfo->numHistIntervals; i++) {
          TrafHistIntervalDesc *curr_desc = (TrafHistIntervalDesc *)TrafAllocateDDLdesc(DESC_HIST_INTERVAL_TYPE, space);

          // copy info from histInfo[i].histintDesc_ to curr_desc
          curr_desc->copyFrom(&histInfo[i].histintDesc_, space);

          if (!prev_desc) {
            stats_desc->tableStatsDesc()->hist_interval_desc = curr_desc;
          } else
            prev_desc->next = curr_desc;

          prev_desc = curr_desc;
        }  // for
      }    // if histint
    }      // if histogram

    table_desc->tableDesc()->table_stats_desc = stats_desc;
  }

  if (endKeyArray) {
    // create a list of region descriptors
    table_desc->tableDesc()->hbase_regionkey_desc = assembleDescs(endKeyArray, space);
  }

  if (genPackedDesc && space) {
    if (!space->isComSpace())  // to insure cast (Space *) is safe
    {
      table_desc = NULL;
      return table_desc;
    }

    Space *trueSpace = (Space *)space;  // space really is a Space

    // pack generated desc and move it to a contiguous buffer before return.
    DescStructPtr((TrafDesc *)table_desc).pack(trueSpace);
    int allocSize = trueSpace->getAllocatedSpaceSize();
    char *contigTableDesc = new HEAP char[allocSize];

    if (!trueSpace->makeContiguous(contigTableDesc, allocSize)) {
      table_desc = NULL;
      return table_desc;
    }

    table_desc = (TrafDesc *)contigTableDesc;

    if (packedDescLen) *packedDescLen = allocSize;
  }

  return table_desc;
}

TrafDesc *Generator::createVirtualLibraryDesc(const char *libraryName, ComTdbVirtTableLibraryInfo *libraryInfo,
                                              Space *space) {
  TrafDesc *library_desc = TrafAllocateDDLdesc(DESC_LIBRARY_TYPE, space);
  library_desc->libraryDesc()->libraryName = new GENHEAP(space) char[strlen(libraryName) + 1];
  strcpy(library_desc->libraryDesc()->libraryName, libraryName);
  library_desc->libraryDesc()->libraryFilename = new GENHEAP(space) char[strlen(libraryInfo->library_filename) + 1];
  strcpy(library_desc->libraryDesc()->libraryFilename, libraryInfo->library_filename);
  library_desc->libraryDesc()->libraryVersion = libraryInfo->library_version;
  library_desc->libraryDesc()->libraryUID = libraryInfo->library_UID;
  library_desc->libraryDesc()->libraryOwnerID = libraryInfo->object_owner_id;
  library_desc->libraryDesc()->librarySchemaOwnerID = libraryInfo->schema_owner_id;

  return library_desc;
}

TrafDesc *Generator::createVirtualRoutineDesc(const char *routineName, ComTdbVirtTableRoutineInfo *routineInfo,
                                              int numParams, ComTdbVirtTableColumnInfo *paramsArray,
                                              ComTdbVirtTablePrivInfo *privInfo, Space *space) {
  TrafDesc *routine_desc = TrafAllocateDDLdesc(DESC_ROUTINE_TYPE, space);
  routine_desc->routineDesc()->objectUID = routineInfo->object_uid;
  routine_desc->routineDesc()->routineName = new GENHEAP(space) char[strlen(routineName) + 1];
  strcpy(routine_desc->routineDesc()->routineName, routineName);
  routine_desc->routineDesc()->externalName = new GENHEAP(space) char[strlen(routineInfo->external_name) + 1];
  strcpy(routine_desc->routineDesc()->externalName, routineInfo->external_name);
  routine_desc->routineDesc()->librarySqlName = NULL;
  routine_desc->routineDesc()->libraryFileName = new GENHEAP(space) char[strlen(routineInfo->library_filename) + 1];
  strcpy(routine_desc->routineDesc()->libraryFileName, routineInfo->library_filename);
  routine_desc->routineDesc()->signature = new GENHEAP(space) char[strlen(routineInfo->signature) + 1];
  strcpy(routine_desc->routineDesc()->signature, routineInfo->signature);
  routine_desc->routineDesc()->librarySqlName = new GENHEAP(space) char[strlen(routineInfo->library_sqlname) + 1];
  strcpy(routine_desc->routineDesc()->librarySqlName, routineInfo->library_sqlname);
  routine_desc->routineDesc()->libRedefTime = routineInfo->lib_redef_time;
  routine_desc->routineDesc()->libBlobHandle = routineInfo->lib_blob_handle;

  routine_desc->routineDesc()->libVersion = routineInfo->library_version;
  routine_desc->routineDesc()->libObjUID = routineInfo->lib_obj_uid;
  // routine_desc->routineDesc()->libSchName = new GENHEAP(space) char[strlen(routineInfo->lib_sch_name)+1];
  // strcpy(routine_desc->routineDesc()->libSchName ,routineInfo->lib_sch_name);
  routine_desc->routineDesc()->libSchName = routineInfo->lib_sch_name;
  routine_desc->routineDesc()->language = CmGetComRoutineLanguageAsRoutineLanguage(routineInfo->language_type);
  routine_desc->routineDesc()->UDRType = CmGetComRoutineTypeAsRoutineType(routineInfo->UDR_type);
  routine_desc->routineDesc()->sqlAccess = CmGetComRoutineSQLAccessAsRoutineSQLAccess(routineInfo->sql_access);
  routine_desc->routineDesc()->transactionAttributes =
      CmGetComRoutineTransactionAttributesAsRoutineTransactionAttributes(routineInfo->transaction_attributes);
  routine_desc->routineDesc()->maxResults = routineInfo->max_results;
  routine_desc->routineDesc()->paramStyle = CmGetComRoutineParamStyleAsRoutineParamStyle(routineInfo->param_style);
  routine_desc->routineDesc()->isDeterministic = routineInfo->deterministic;
  routine_desc->routineDesc()->isCallOnNull = routineInfo->call_on_null;
  routine_desc->routineDesc()->isIsolate = routineInfo->isolate;
  routine_desc->routineDesc()->externalSecurity =
      CmGetRoutineExternalSecurityAsComRoutineExternalSecurity(routineInfo->external_security);
  routine_desc->routineDesc()->executionMode =
      CmGetRoutineExecutionModeAsComRoutineExecutionMode(routineInfo->execution_mode);
  routine_desc->routineDesc()->stateAreaSize = routineInfo->state_area_size;
  routine_desc->routineDesc()->parallelism = CmGetRoutineParallelismAsComRoutineParallelism(routineInfo->parallelism);
  UInt32 reclen;
  routine_desc->routineDesc()->paramsCount = numParams;
  routine_desc->routineDesc()->params =
      Generator::createColDescs(routineName, paramsArray, (Int16)numParams, reclen, space);
  routine_desc->routineDesc()->owner = routineInfo->object_owner_id;
  routine_desc->routineDesc()->schemaOwner = routineInfo->schema_owner_id;

  // Setup the privilege descriptors for routines.
  TrafDesc *priv_desc = NULL;
  if (privInfo) priv_desc = createPrivDescs(privInfo, space);
  routine_desc->routineDesc()->priv_desc = priv_desc;

  return routine_desc;
}

short Generator::genAndEvalExpr(CmpContext *cmpContext, char *exprStr, int numChildren, ItemExpr *childNode0,
                                ItemExpr *childNode1, ComDiagsArea *diagsArea) {
  short rc = 0;

  Parser parser(cmpContext);
  BindWA bindWA(ActiveSchemaDB(), cmpContext);

  ItemExpr *parseTree = NULL;
  parseTree = parser.getItemExprTree(exprStr, strlen(exprStr), CharInfo::ISO88591, numChildren, childNode0, childNode1);

  if (!parseTree) return -1;

  parseTree = parseTree->bindNode(&bindWA);
  if (bindWA.errStatus()) return -1;

  const NAType &resultType = parseTree->getValueId().getType();

  // result may contain ExpTupleDesc::LONG_FORMAT info
  int castValBufLen = resultType.getNominalSize() + 8;
  char *castValBuf = new (CmpCommon::statementHeap()) char[castValBufLen];
  int outValLen = 0;
  int outValOffset = 0;
  rc = ValueIdList::evaluateTree(parseTree, castValBuf, castValBufLen, &outValLen, &outValOffset, diagsArea);

  if (rc) return -1;

  return 0;
}

PhysicalProperty *Generator::genPartitionedPhysProperty(const IndexDesc *clusIndex) {
  PlanExecutionEnum plenum = EXECUTE_IN_ESP;

  PartitioningFunction *myPartFunc = NULL;
  if ((clusIndex->getPartitioningFunction()) &&
      (clusIndex->getPartitioningFunction()->isAHash2PartitioningFunction())) {
    int forcedEsps = 0;
    if (CmpCommon::getDefault(PARALLEL_NUM_ESPS, 0) != DF_SYSTEM)
      forcedEsps = ActiveSchemaDB()->getDefaults().getAsLong(PARALLEL_NUM_ESPS);
    else
      forcedEsps = 32;  // this seems to be an optimum number
    // forcedEsps = rpp->getCountOfAvailableCPUs();

    const Hash2PartitioningFunction *h2pf = clusIndex->getPartitioningFunction()->castToHash2PartitioningFunction();
    int numPartns = h2pf->getCountOfPartitions();
    forcedEsps = numPartns;
    if ((forcedEsps <= numPartns) && ((numPartns % forcedEsps) == 0)) {
      NodeMap *myNodeMap =
          new (CmpCommon::statementHeap()) NodeMap(CmpCommon::statementHeap(), forcedEsps, NodeMapEntry::ACTIVE);

      CollIndex entryNum = 0;
      int currNodeNum = -1;
      int i = 0;
      while (i < forcedEsps) {
        if (entryNum == h2pf->getNodeMap()->getNumEntries()) {
          entryNum = 0;
          currNodeNum = -1;
          continue;
        }

        const NodeMapEntry *entry = h2pf->getNodeMap()->getNodeMapEntry(entryNum);

        if (entry->getNodeNumber() == currNodeNum) {
          entryNum++;
          continue;
        }

        myNodeMap->setNodeMapEntry(i, *entry);
        currNodeNum = entry->getNodeNumber();

        entryNum++;
        i++;
      }

      myPartFunc = new (CmpCommon::statementHeap())
          Hash2PartitioningFunction(h2pf->getPartitioningKey(), h2pf->getKeyColumnList(), forcedEsps, myNodeMap);
      myPartFunc->createPartitioningKeyPredicates();
    }
  }

  if (myPartFunc == NULL) myPartFunc = clusIndex->getPartitioningFunction();

  if (!myPartFunc) {
    //----------------------------------------------------------
    // Create a node map with a single, active, wild-card entry.
    //----------------------------------------------------------
    NodeMap *myNodeMap = new (CmpCommon::statementHeap()) NodeMap(CmpCommon::statementHeap(), 1, NodeMapEntry::ACTIVE);

    //------------------------------------------------------------
    // The table is not partitioned. No need to start ESPs.
    // Synthesize a partitioning function with a single partition.
    //------------------------------------------------------------
    myPartFunc = new (CmpCommon::statementHeap()) SinglePartitionPartitioningFunction(myNodeMap);
    plenum = EXECUTE_IN_MASTER;
  }

  PhysicalProperty *sppForMe =
      new (CmpCommon::statementHeap()) PhysicalProperty(myPartFunc, plenum, SOURCE_VIRTUAL_TABLE);

  return sppForMe;
}

/////////////////////////////////////////////////////////////////
//
// This next method helps with the fix for Soln 10-071204-9253.
// Sometimes, SORT is used as a blocking operator to make a self-ref
// update safe from the Halloween problem.  But if the TSJforWrite and
// SORT are executed in parallel and the scan of the self-ref table is
// accessed from the same ESP as the SORT, the resulting plan
// will not be safe, because the scans can finish asynchronously
// which will allow some SORT to return rows before other scans
// have finished.  To prevent this, Sort::preCodeGen will call
// this method to insert an ESP Exchange below the SORT, so that
// none of the SORT instances will begin returning rows until
// all of the scans have finished.  There is also code in the
// preCodeGen methods of NestedJoin, Exchange, and PartitionAccess
// to help detect the need for Sort::preCodeGen to call this method.
//
// The method is also used to add an ESP Exchange on top of a
// SequenceGenerator operator.  In this case the Exchange is actually
// treated as an ESP Access operator.
/////////////////////////////////////////////////////////////////

RelExpr *Generator::insertEspExchange(RelExpr *oper, const PhysicalProperty *unPreCodeGendPP) {
  GroupAttributes *ga = oper->getGroupAttr();

  // Gather some information about partitioning to allow an
  // assertion to check the assumption that this is safe to do.

  PartitioningFunction *pf = unPreCodeGendPP->getPartitioningFunction();

  const ValueIdSet &partKeys = pf->getPartitioningKey();

  ValueId pkey;
  partKeys.getFirst(pkey);

  NABoolean isRandomRepart = ((pf->isAHashPartitioningFunction() || pf->isATableHashPartitioningFunction()) &&
                              (partKeys.entries() == 1) && (pkey.getItemExpr()->getOperatorType() == ITM_RANDOMNUM));

  if (isRandomRepart == FALSE) {
    ValueIdSet newInputs;
    ValueIdSet referencedInputs;
    ValueIdSet coveredSubExpr;
    ValueIdSet uncoveredExpr;
    NABoolean isCovered = partKeys.isCovered(newInputs, *ga, referencedInputs, coveredSubExpr, uncoveredExpr);
    //      if (isCovered == FALSE)
    // GenAssert(0, "Bad assumptions in Generator::insertEspExchange.")
  }

  Exchange *exch = new (CmpCommon::statementHeap()) Exchange(oper);

  exch->setPhysicalProperty(unPreCodeGendPP);
  exch->setGroupAttr(oper->getGroupAttr());
  exch->setEstRowsUsed(oper->getEstRowsUsed());
  exch->setMaxCardEst(oper->getMaxCardEst());
  exch->setInputCardinality(exch->getInputCardinality());
  exch->setOperatorCost(0);
  exch->setRollUpCost(exch->getRollUpCost());

  // Don't let Exchange::preCodeGen eliminate this Exchange.
  exch->doSkipRedundancyCheck();

  exch->setUpMessageBufferLength(ActiveSchemaDB()->getDefaults().getAsULong(UNOPTIMIZED_ESP_BUFFER_SIZE_UP) / 1024);
  exch->setDownMessageBufferLength(ActiveSchemaDB()->getDefaults().getAsULong(UNOPTIMIZED_ESP_BUFFER_SIZE_DOWN) / 1024);

  return exch;
}

/////////////////////////////////////////////////////////////////
// Methods to manipulate the map tables
/////////////////////////////////////////////////////////////////
// appends 'map_table' to the end of the
// list of map tables.
// If no map table is passed in, allocates a new map table
// and appends it.
// Returns pointer to the maptable being added.
MapTable *Generator::appendAtEnd(MapTable *map_table) {
  MapTable *mt = (map_table ? map_table : (new (wHeap()) MapTable()));

  if (!mt) return NULL;

  if (!firstMapTable_) {
    firstMapTable_ = mt;
    lastMapTable_ = mt;
  } else {
    mt->prev() = lastMapTable_;
    lastMapTable_->next() = mt;
    lastMapTable_ = mt;

    while (lastMapTable_->next()) lastMapTable_ = lastMapTable_->next();
  }

  return mt;
}

// searches for value_id in the list of map tables.
// If mapTable is input, starts the search from there.
// Returns MapInfo, if found.
// Raises assertion, if not found.
MapInfo *Generator::getMapInfo(const ValueId &value_id, MapTable *mapTable) {
  MapInfo *mi = getMapInfoAsIs(value_id, mapTable);
  if (mi) return mi;

  // value not found. Assert.
  NAString unparsed(wHeap());
  value_id.getItemExpr()->unparse(unparsed);
  char errmsg[200];

  if (mapTable) {
    sprintf(errmsg, "\nValueId %d (%.100s...) not found in MapTable %p", (CollIndex)value_id, unparsed.data(),
            mapTable);
  } else {
    sprintf(errmsg, "\nValueId %d (%.100s...) not found in all chained MapTables", (CollIndex)value_id,
            unparsed.data());
  }

  displayMapTables(INT32_MAX, errmsg);

  *CmpCommon::diags() << DgSqlCode(-7007) << DgString0(" ");
  GenExit();

  return NULL;
}

MapTable *Generator::getMapTable(ValueId id) {
  MapTable *x = lastMapTable_;

  while (x) {
    if (x->getTotalVids() > 0) {
      MapInfo *mi = x->getMapInfoFromThis(id);
      if (mi) return x;
    }

    x = x->prev();
  }

  return NULL;
}

// searches for value_id in the list of map tables.
// If mapTable is input, starts the search from there.
// Returns MapInfo, if found.
// Returns NULL, if not found.
MapInfo *Generator::getMapInfoAsIs(const ValueId &value_id, MapTable *mapTable) {
  // first look for this value_id in the last map table. There
  // is a good chance it will be there.
  MapInfo *mi = ((getLastMapTable()->getTotalVids() > 0) ? getLastMapTable()->getMapInfoFromThis(value_id) : NULL);
  if (mi) return mi;

  // now search all the map tables.
  // Do not look in the last map table as we have already searched it.

  if ((!mapTable) && (getLastMapTable() == getMapTable())) return NULL;
  MapTable *mt = (mapTable ? mapTable : getLastMapTable()->prev());
  while (mt) {
    if (mt->getTotalVids() > 0) {
      mi = mt->getMapInfoFromThis(value_id);
      if (mi) return mi;
    }

    if (mt != (mapTable ? mapTable : getMapTable()))
      mt = mt->prev();
    else
      break;
  }

  return NULL;
}

// gets MapInfo from mapTable.
// Raises assertion, if not found.
MapInfo *Generator::getMapInfoFromThis(MapTable *mapTable, const ValueId &value_id) {
  return mapTable->getMapInfoFromThis(value_id);
}

// adds to the last maptable, if value doesn't exist.
// Returns the MapInfo, if that value exists.
MapInfo *Generator::addMapInfo(const ValueId &value_id, Attributes *attr) {
  MapInfo *map_info;

  // return the map information, if already been added to the map table.
  if (map_info = getMapInfoAsIs(value_id)) return map_info;

  return getLastMapTable()->addMapInfoToThis(value_id, attr);
}

// adds to input mapTable. Does NOT check if the value exists.
// Caller should have checked for that.
// Returns the MapInfo for the added value.
MapInfo *Generator::addMapInfoToThis(MapTable *mapTable, const ValueId &value_id, Attributes *attr) {
  return mapTable->addMapInfoToThis(value_id, attr);
}

// deletes ALL maptables starting at 'next' of inMapTable.
// If inMapTable is NULL, removes all map tables in generator.
// Makes inMapTable the last map table.
void Generator::removeAll(MapTable *inMapTable) {
  MapTable *moreToDelete = (inMapTable ? inMapTable->next() : firstMapTable_);
  MapTable *me;

  while (moreToDelete) {
    me = moreToDelete;
    moreToDelete = moreToDelete->next();

    me->next() = NULL;  // no dangling pointer
    delete me;
  }

  if (inMapTable) {
    inMapTable->next() = NULL;
    lastMapTable_ = inMapTable;
  } else {
    firstMapTable_ = NULL;
    lastMapTable_ = NULL;
  }
}

// removes the last map table in the list.
void Generator::removeLast() {
  if (!lastMapTable_) return;

  MapTable *newLastMapTable = lastMapTable_->prev();
  delete lastMapTable_;
  lastMapTable_ = newLastMapTable;

  if (lastMapTable_) lastMapTable_->next() = NULL;
}

// unlinks the next mapTable in the list and returns it.
// Makes mapTable the last map table.
// Does not delete the next map table.
void Generator::unlinkNext(MapTable *mapTable) {
  if (mapTable == NULL) return;

  MapTable *mt = mapTable->next();
  mapTable->next() = NULL;
  lastMapTable_ = mapTable;

  // return mt;
}

// unlinks the last mapTable in the list and returns it.
MapTable *Generator::unlinkLast() {
  if (!lastMapTable_) return NULL;

  MapTable *lastMapTable = lastMapTable_;
  lastMapTable_ = lastMapTable_->prev();
  lastMapTable_->next() = NULL;

  return lastMapTable;
}

void Generator::displayMapTables(int depth, const char *msg) {
  if (msg) cout << msg << ". ";

  cout << "All mapTables of up to a depth of " << depth << "." << endl;
  MapTable *x = lastMapTable_;

  int i = 0;
  char buf[100];

  while (x && i < depth) {
    if (x == lastMapTable_)  // is the last one?
      sprintf(buf, "last->  ");
    else if (x == getMapTable())  // is the first one?
      sprintf(buf, "first-> ");
    else
      sprintf(buf, "        ");

    sprintf(buf + strlen(buf), "mapTable[%d]: %p, ", i++, x);

    x->display(buf);

    buf[0] = 0;

    x = x->prev();
  }
  cout << endl;
}

void Generator::reportMappingInfo(ostream &out, const ValueIdList &list, const char *title) {
  if (title) cout << title << endl;

  for (int i = 0; i < list.entries(); i++) {
    ValueId id = list[i];

    MapTable *mapt = getMapTable(id);

    out << "vid=" << id << ", ";
    id.getItemExpr()->display(out);

    out << "mapTable=" << static_cast<void *>(mapt) << endl;

    Attributes *attr = getMapInfo(id)->getAttr();

    out << "attr=";
    if (attr)
      attr->displayContents(out);
    else
      out << "NULL";

    out << endl;
  }
}

// unlinks the this mapTable in the list, and whatever follows
void Generator::unlinkMe(MapTable *mapTable) {
  if (mapTable == NULL) return;

  MapTable *mt = mapTable->prev();
  if (mt != NULL) {
    mapTable->prev() = NULL;
    mt->next() = NULL;
    lastMapTable_ = mt;
  } else if (mapTable == firstMapTable_) {
    lastMapTable_ = NULL;
    firstMapTable_ = NULL;
  }
  // else do nothing, we are not on a chain.

  // return mt;
}

void Generator::setMapTable(MapTable *map_table_) {
  firstMapTable_ = map_table_;
  lastMapTable_ = firstMapTable_;

  if (lastMapTable_)
    while (lastMapTable_->next()) lastMapTable_ = lastMapTable_->next();
}

const NAString Generator::genGetNameAsAnsiNAString(const QualifiedName &qual) {
  return qual.getQualifiedNameAsAnsiString();
}

const NAString Generator::genGetNameAsAnsiNAString(const CorrName &corr) {
  return genGetNameAsAnsiNAString(corr.getQualifiedNameObj());
}

const NAString Generator::genGetNameAsNAString(const QualifiedName &qual) { return qual.getQualifiedNameAsString(); }

const NAString Generator::genGetNameAsNAString(const CorrName &corr) {
  // This warning is wrong, or at least misleading.  True, at this call we
  // are ignoring (losing) host-variable name, but at a later point in
  // CodeGen the proper hv linkage is set up for correct Late Name Resolution.
  //
  //   HostVar *proto = corr.getPrototype();
  //   if (proto)
  //     cerr << "*** WARNING[] Ignoring variable " << proto->getName()
  //         << ", using prototype value "
  //         << GenGetQualifiedName(corr.getQualifiedNameObj()) << endl;

  return genGetNameAsNAString(corr.getQualifiedNameObj());
}

// returns attributes corresponding to itemexpr ie.
// If clause has already been generated for this ie, then returns
// attribute from operand 0 (result) of clause.
// If not, searches the map table and returns it from there.
Attributes *Generator::getAttr(ItemExpr *ie) {
  if ((getExpGenerator()->clauseLinked()) && (ie->getClause()))
    return ie->getClause()->getOperand(0);
  else
    return getMapInfo(ie->getValueId())->getAttr();
}

void Generator::addTrafSimTableInfo(TrafSimilarityTableInfo *newST) {
  for (CollIndex i = 0; i < getTrafSimTableInfoList().entries(); i++) {
    TrafSimilarityTableInfo *ti = (TrafSimilarityTableInfo *)getTrafSimTableInfoList()[i];
    if (*ti == *newST) {
      // value exists, do not add.
      return;
    }
  }

  getTrafSimTableInfoList().insert(newST);
}

// Helper method used by caching operators to ensure a statement
// execution count is included in their characteristic input.
// The "getOrAdd" semantic is to create the ValueId if it doesn't
// exist, otherwise return a preexisting one.
ValueId Generator::getOrAddStatementExecutionCount() {
  ValueId execCount;

  for (ValueId x = internalInputs_.init(); internalInputs_.next(x); internalInputs_.advance(x)) {
    if (x.getItemExpr()->getOperatorType() == ITM_EXEC_COUNT) {
      execCount = x;
      break;
    }
  }

  if (execCount == NULL_VALUE_ID) {
    // nobody has asked for an execution count before, create
    // a new item expression and add it to the internal input
    // values that are to be generated by the root node
    ItemExpr *ec = new (wHeap()) StatementExecutionCount();
    ec->bindNode(getBindWA());
    execCount = ec->getValueId();
    internalInputs_ += execCount;
  }

  return execCount;
}

bool Generator::setPrecodeHalloweenLHSofTSJ(bool newVal) {
  bool oldVal = precodeHalloweenLHSofTSJ_;
  precodeHalloweenLHSofTSJ_ = newVal;
  return oldVal;
}

bool Generator::setPrecodeRHSofNJ(bool newVal) {
  bool oldVal = precodeRHSofNJ_;
  precodeRHSofNJ_ = newVal;
  return oldVal;
}

// Called by the HJ for one of the min/max expressions to be
// evaluated there.
void Generator::addMinMaxVals(const ValueId &outerVal, const ValueId &innerVal, const ValueId &minValScan,
                              const ValueId &maxValScan, const ValueId &bitMapScan, CostScalar child1Uec,
                              NABoolean isNonPartitioned, NABoolean child1IsBroadcastPartitioned, RelExpr *caller,
                              CostScalar rowsProducedByHJ) {
  CollIndex ix = outerMinMaxKeys_.entries();

  outerMinMaxKeys_.insert(outerVal);
  innerMinMaxKeys_.insert(innerVal);
  minVals_.insert(minValScan);
  maxVals_.insert(maxValScan);
  rangeOfValuesVals_.insert(bitMapScan);

  innerChildUecs_.insert(child1Uec);

  innerIsBroadcastPartitioned_.insert(child1IsBroadcastPartitioned);

  callers_.insert(caller);

  rowsProducedByHJ_.insert(rowsProducedByHJ);

  outerRowsToSurvive_.insert(CostScalar(0.0));

  // values start out in the enabled, unused state
  minMaxEnabled_ += ix;

  if (isNonPartitioned) minMaxNonPartitioned_ += ix;

  DCMPASSERT(outerMinMaxKeys_.entries() == ++ix && innerMinMaxKeys_.entries() == ix && minVals_.entries() == ix &&
             maxVals_.entries() == ix && callers_.entries() == ix && innerChildUecs_.entries() == ix &&
             rowsProducedByHJ_.entries() == ix);
}

void Generator::saveAndMapMinMaxKeys(ValueIdMap &map, ValueIdList &savedValues /*OUT*/) {
  // save the old values for later use with restoreMinMaxVals
  savedValues = outerMinMaxKeys_;
  outerMinMaxKeys_.clear();
  map.mapValueIdListDown(savedValues, outerMinMaxKeys_);
  DCMPASSERT(outerMinMaxKeys_.entries() == innerMinMaxKeys_.entries());
}

void Generator::restoreMinMaxKeys(const ValueIdList &savedValues /*IN*/) {
  // copy the saved values back, leave additional values
  // generated by the child tree in place
  for (CollIndex i = 0; i < savedValues.entries(); i++) outerMinMaxKeys_[i] = savedValues[i];
  DCMPASSERT(outerMinMaxKeys_.entries() == innerMinMaxKeys_.entries());
}

void Generator::setPlanExpirationTimestamp(long t) {
  // if t == -1 that has no effect (initial default is -1)
  // Otherwise, use the smaller of planExpirationTimestamp_ and t
  if (t >= 0 && planExpirationTimestamp_ > t) planExpirationTimestamp_ = t;
}

////////////////////////////////////////////////////////////////////
// the next comment lines are historical and are not relevant
// any more.
////////////////////////////////////////////////////////////////////
// ##
// For three-part ANSI names (truly qualified names),
// these functions should return NAString (non-ref)
// which is concatenation of
//   getCatalogName() + getSchemaName + getObjectName()
// with a fixed number of bytes of length preceding each field
// -- or some other encoding of a three-part name such that the
// three parts are clearly delineated/unambiguous
// (parsing by dots is no good, since delimited names can have dots).
// -- or perhaps we need to return three separate strings?
// ## But for now, this will do since ANSI names aren't in the 1996 EAP.
//
// ##
// Callers of these functions need to save this corr-to-hostvar linkage
// somewhere (callers passing in non-Ansi-name indexdesc's and fileset names
// need to save an additional item of context to indicate what kind of
// path or role it is -- i.e. Ansi tablename, raw physical partition name of
// a table, Ansi or physical name of an index, etc.).
//
// Upon finding a :hv, Executor must do:
// If hostvar
//   Get the user input value from the variable
// Else (envvar)
//   Do a getenv on the name
//   If no current value and no saved value, error
//   If no current value, then local :hv = saved value, else :hv = current
// Parse value in :hv, resulting in QualifiedName object
// Do a Catalog Manager lookup of this QName (Executor<->CatMan messages)
// Traverse the NATable (or whatever) catalog info tree to find the correct
// physical name for the role/path/context to be opened.
// Open the file.
//
// Eventually (not in first release), we need to save Similarity Check
// info of the prototype table, so that at run-time,
// after the catalog info fetch, and probably also after the open,
// the Similarity Check(s) must be done.
//
// Caching can be done to create a fastpath from var name straight to
// previously-opened file (or at least part of the way, to prev-fetched
// catalog info, if this is a different role).  Worth doing if it cuts
// out the overhead of repeated catalog reads.
//
// Parsing: can reuse/clone some of the code from CorrName::applyPrototype
// and applyDefaults -- for name-part defaulting, it makes sense to me to
// determine which defaults to use the same way as dynamic SQL compilations:
// look first if any dynamic Ansi defaults (SET SCHEMA),
// next at static Tandem extension defaults (DECLARE SCHEMA),
// finally at the (always present) schema and catalog of the module
// (compilation unit) this query is part of.
//
// I assume we can't have all the heavy machinery of the entire Parser
// to be linked into the Executor for just this.  But we do want something
// robust enough to handle identifiers (both regular and delimited)
// and names (one, two, and three-part) in all their glory and idiosyncrasy
// (character sets, valid initial characters, treatment of blank space, etc).
// I think we should move the ShortStringSequence and xxxNameFromStrings and
// transformIdentifier code out of SqlParser.y -- make a suitable .h file
// for public interface -- write a teeny parser just for this problem,
// based on that abstracted code -- and have Executor call that.
// ##
//

const NAString GenGetQualifiedName(const CorrName &corr, NABoolean formatForDisplay, NABoolean asAnsiString) {
  if (asAnsiString)
    return corr.getQualifiedNameObj().getQualifiedNameAsAnsiString(formatForDisplay);
  else
    return corr.getQualifiedNameObj().getQualifiedNameAsString(formatForDisplay);
}

void GeneratorAssert(const char *file, int line, const char *assertionMsg, const char *errorMessage) {
  NAString msg;
  if (errorMessage)
    msg += NAString("Error Message: ") + errorMessage;
  else if (assertionMsg)
    msg += NAString("Assertion Failure: ") + assertionMsg;

  SQLMXLoggingArea::logSQLMXAssertionFailureEvent(file, line, msg);

  *CmpCommon::diags() << DgSqlCode(-7000) << DgString0(file) << DgInt0(line) << DgString1(msg);

  GeneratorExit(file, line);
}

void GeneratorExit(const char *file, int line) { UserException(file, line).throwException(); }
/*****************************
 Determine the tuple data format to use based on some heuristics.
 and whether we want to resize or not
 this functions determines the tuple format that we need to for a rel expression
 based on a value Id list which constitutes the row.
 the paramaters and explanatios are as folows:
 const ValueIdList & valIdList, --> this the row we are exemining
 RelExpr * relExpr, --> rel expression that is trying to determine the tuple format
 NABoolean & resizeCifRecord,  --> should we resize the cif record or not -- if exploded format
                                   this boolean ill be set to FALSE
                                     //otherwiase it is TRUE if there varchars FALSE if not
 RelExpr::CifUseOptions bmo_cif, bmo (relexpr's) CIF seeting on , off or system
 NABoolean bmo_affinity,-->if TRUE then the rel expr will use the same tuple format as query one
 UInt32 & alignedLength, --> length of the row in aligned format
 UInt32 & explodedLength, --> length of the row in exploded format
 UInt32 & alignedVarCharSize, --> length of the vachar fields
 UInt32 & alignedHeaderSize, --> length of the header
 double & avgVarCharUsage) --> combined average varchar usgae of all the varchar feilds based
                              on stats info if available

*/
ExpTupleDesc::TupleDataFormat Generator::determineInternalFormat(const ValueIdList &valIdList, RelExpr *relExpr,
                                                                 NABoolean &resizeCifRecord,
                                                                 RelExpr::CifUseOptions bmo_cif, NABoolean bmo_affinity,
                                                                 UInt32 &alignedLength, UInt32 &explodedLength,
                                                                 UInt32 &alignedVarCharSize, UInt32 &alignedHeaderSize,
                                                                 double &avgVarCharUsage, UInt32 prefixLength)

{
  if (bmo_cif == RelExpr::CIF_OFF) {
    resizeCifRecord = FALSE;
    return ExpTupleDesc::SQLARK_EXPLODED_FORMAT;
  }

  resizeCifRecord = valIdList.hasVarChars();

  if (bmo_cif == RelExpr::CIF_ON) {
    return ExpTupleDesc::SQLMX_ALIGNED_FORMAT;
  }

  DCMPASSERT(bmo_cif == RelExpr::CIF_SYSTEM);

  // when bmo affinity is true ==> use the same tuple format as the main one
  // valid when bmo cif is sett o system only
  if (bmo_affinity == TRUE) {
    if (getInternalFormat() == ExpTupleDesc::SQLMX_ALIGNED_FORMAT) {
      return ExpTupleDesc::SQLMX_ALIGNED_FORMAT;
    } else {
      DCMPASSERT(getInternalFormat() == ExpTupleDesc::SQLARK_EXPLODED_FORMAT);
      resizeCifRecord = FALSE;
      return ExpTupleDesc::SQLARK_EXPLODED_FORMAT;
    }
  }

  // The aligned and exploded row length and aligned row length and  varchar size
  // are computed first --
  alignedHeaderSize = 0;
  alignedVarCharSize = 0;
  explodedLength = 0;
  alignedLength = 0;

  ExpGenerator *exp_gen = getExpGenerator();

  // compute the aligned row  length, varchar size and header size
  exp_gen->computeTupleSize(valIdList, ExpTupleDesc::SQLMX_ALIGNED_FORMAT, alignedLength, 0, &alignedVarCharSize,
                            &alignedHeaderSize);

  alignedLength += prefixLength;

  DCMPASSERT(resizeCifRecord == (alignedVarCharSize > 0));

  // compute the exploded format row size also
  exp_gen->computeTupleSize(valIdList, ExpTupleDesc::SQLARK_EXPLODED_FORMAT, explodedLength, 0);
  explodedLength += prefixLength;

  // use exploded format if row size is less than a predefined Min row size....
  // this parameter is set to a low number like 8 or less
  // the idea is that is a row is very shor than using CIF may not be a good idea because
  // of the aligned format header
  if (alignedLength < CmpCommon::getDefaultNumeric(COMPRESSED_INTERNAL_FORMAT_MIN_ROW_SIZE) ||
      explodedLength < CmpCommon::getDefaultNumeric(COMPRESSED_INTERNAL_FORMAT_MIN_ROW_SIZE)) {
    resizeCifRecord = FALSE;
    return ExpTupleDesc::SQLARK_EXPLODED_FORMAT;
  }

  double cifRowSizeAdj = CmpCommon::getDefaultNumeric(COMPRESSED_INTERNAL_FORMAT_ROW_SIZE_ADJ);
  if (resizeCifRecord == FALSE) {
    if (alignedLength < explodedLength * cifRowSizeAdj) {
      return ExpTupleDesc::SQLMX_ALIGNED_FORMAT;
    } else {
      // resizeCifRecord is false
      return ExpTupleDesc::SQLARK_EXPLODED_FORMAT;
    }
  }

  double cumulAvgVarCharSize = 0;
  UInt32 CumulTotVarCharSize = 0;
  avgVarCharUsage = 1;

  // compute the average varchar size usage for the whole row based on individuka fileds average
  // varchar usages
  // resizeCifRecord is TRUE

  CollIndex numEntries = valIdList.entries();

  for (CollIndex i = 0; i < numEntries; i++) {
    if (valIdList.at(i).getType().getVarLenHdrSize() > 0) {
      double avgVarCharSize = 0;
      ValueId vid = valIdList.at(i);

      if (!findCifAvgVarCharSizeToCache(vid, avgVarCharSize))  // do we need the cache???
      {
        avgVarCharSize = relExpr->getGroupAttr()->getAverageVarcharSize(valIdList.at(i));
        addCifAvgVarCharSizeToCache(vid, avgVarCharSize);
      }

      if (avgVarCharSize > 0) {
        cumulAvgVarCharSize += avgVarCharSize;
      } else {
        cumulAvgVarCharSize += valIdList.at(i).getType().getTotalSize() * avgVarCharUsage;
      }
      CumulTotVarCharSize += valIdList.at(i).getType().getTotalSize();
    }
  }

  if (CumulTotVarCharSize > 0) avgVarCharUsage = cumulAvgVarCharSize / CumulTotVarCharSize;

  UInt32 alignedNonVarSize = alignedLength - alignedVarCharSize;

  // if cumulltative var char size > header size and ...  use aligned format
  if (alignedVarCharSize > alignedHeaderSize &&
      (alignedNonVarSize + avgVarCharUsage * alignedVarCharSize < explodedLength * cifRowSizeAdj)) {
    return ExpTupleDesc::SQLMX_ALIGNED_FORMAT;
  }

  resizeCifRecord = FALSE;
  return ExpTupleDesc::SQLARK_EXPLODED_FORMAT;
}

ExpTupleDesc::TupleDataFormat Generator::determineInternalFormat(const ValueIdList &valIdList, RelExpr *relExpr,
                                                                 NABoolean &resizeCifRecord,
                                                                 RelExpr::CifUseOptions bmo_cif, NABoolean bmo_affinity,
                                                                 NABoolean &considerDefrag, UInt32 prefixLength)

{
  considerDefrag = FALSE;
  resizeCifRecord = FALSE;
  if (bmo_cif == RelExpr::CIF_OFF) {
    return ExpTupleDesc::SQLARK_EXPLODED_FORMAT;
  }

  UInt32 alignedHeaderSize = 0;
  UInt32 alignedVarCharSize = 0;
  UInt32 explodedLength = 0;
  UInt32 alignedLength = 0;
  double avgVarCharUsage = 1;
  ExpTupleDesc::TupleDataFormat tf =
      determineInternalFormat(valIdList, relExpr, resizeCifRecord, bmo_cif, bmo_affinity, alignedLength, explodedLength,
                              alignedVarCharSize, alignedHeaderSize, avgVarCharUsage, prefixLength);

  if (relExpr) {
    considerDefrag = considerDefragmentation(valIdList, relExpr->getGroupAttr(), resizeCifRecord, prefixLength);
  }
  return tf;
}

NABoolean Generator::considerDefragmentation(const ValueIdList &valIdList, GroupAttributes *gattr,
                                             NABoolean resizeCifRecord, UInt32 prefixLength)

{
  if (resizeCifRecord && CmpCommon::getDefaultNumeric(COMPRESSED_INTERNAL_FORMAT_DEFRAG_RATIO) > 1) return TRUE;

  if (!resizeCifRecord || !gattr) return FALSE;

  NABoolean considerDefrag = FALSE;

  // determine if we wantto defragment the buffers or not using the average row size stats
  UInt32 maxRowSize = 0;
  double avgRowSize = gattr->getAverageVarcharSize(valIdList, maxRowSize);
  if (maxRowSize > 0 && (prefixLength + avgRowSize) / (prefixLength + maxRowSize) <
                            CmpCommon::getDefaultNumeric(COMPRESSED_INTERNAL_FORMAT_DEFRAG_RATIO)) {
    considerDefrag = TRUE;
  }

  return considerDefrag;
}

void Generator::setHBaseNumCacheRows(double estRowsAccessed, ComTdbHbaseAccess::HbasePerfAttributes *hbpa,
                                     int hbaseRowSize, Float32 samplePercent, NABoolean isNeedPushDownLimit) {
  // compute the number of rows accessed per scan node instance and use it
  // to set HBase scan cache size (in units of number of rows). This cache
  // is in the HBase client, i.e. in the java side of
  // master executor or esp process. Using this cache avoids RPC calls to the
  // region server for each row, however setting the value too high consumes
  // memory and can lead to timeout errors as the RPC call that gets a
  // big chunk of rows can take longer to complete.
  CollIndex myId = getFragmentDir()->getCurrentId();
  int numProcesses = getFragmentDir()->getNumESPs(myId);
  NABoolean bParallel = (numProcesses > 0);
  int cacheMin = CmpCommon::getDefaultNumeric(HBASE_NUM_CACHE_ROWS_MIN);
  int cacheMax = CmpCommon::getDefaultNumeric(HBASE_NUM_CACHE_ROWS_MAX);
  if (numProcesses == 0) numProcesses++;
  UInt32 rowsAccessedPerProcess = ceil(estRowsAccessed / numProcesses);
  int cacheRows;
  if (rowsAccessedPerProcess < cacheMin)
    cacheRows = cacheMin;
  else if (rowsAccessedPerProcess < cacheMax)
    cacheRows = rowsAccessedPerProcess;
  else
    cacheRows = cacheMax;

  // Reduce the scanner cache if necessary based on the sampling rate (usually
  // only for Update Stats) so that it will return to the client once for every
  // USTAT_HBASE_SAMPLE_RETURN_INTERVAL rows on average. This avoids long stays
  // in the region server (and a possible OutOfOrderScannerNextException), where
  // a random row filter is used for sampling.
  if (cacheRows > cacheMin && samplePercent > 0.0) {
    int sampleReturnInterval = ActiveSchemaDB()->getDefaults().getAsULong(USTAT_HBASE_SAMPLE_RETURN_INTERVAL);
    int newScanCacheSize = (int)(sampleReturnInterval * samplePercent);
    if (newScanCacheSize < cacheRows) {
      if (newScanCacheSize >= cacheMin)
        cacheRows = newScanCacheSize;
      else
        cacheRows = cacheMin;
    }
  }

  // Limit the scanner cache size to a fixed number if we are dealing with
  // very wide rows eg rows with varchar(16MB)
  int maxRowSizeInCache = CmpCommon::getDefaultNumeric(TRAF_MAX_ROWSIZE_IN_CACHE) * 1024 * 1024;
  if (hbaseRowSize > maxRowSizeInCache) cacheRows = 2;
  if (CmpCommon::getDefault(AUTO_ADJUST_CACHEROWS) == DF_ON) {
    // check whether total size > JVM_MAX_HEAP_SIZE_MB or ESP_JVM_MAX_HEAP_SIZE_MB,
    // and limit the scanner cache size to a fixed number.
    long nTotalSize = cacheRows * hbaseRowSize;
    char *jvm_env = getenv("JVM_MAX_HEAP_SIZE_MB");
    int nJvmHeapSize = 0;
    int nEspJvmHeapSize = 0;
    if (jvm_env) nJvmHeapSize = atoi(jvm_env);
    if (bParallel) {
      jvm_env = getenv("ESP_JVM_MAX_HEAP_SIZE_MB");
      if (jvm_env) nEspJvmHeapSize = atoi(jvm_env);
    }
    long nJVMHeapSize = nJvmHeapSize;
    if (nEspJvmHeapSize != 0) nJVMHeapSize = min(nJvmHeapSize, nEspJvmHeapSize);
    if (nJVMHeapSize > 0) {
      int percentCanBeUsed = (nJVMHeapSize <= 512) ? 80 : 90;
      int nNeedMemroyInMB = (nTotalSize >> 20);
      if (nNeedMemroyInMB > nJVMHeapSize * percentCanBeUsed / 100) {
        *CmpCommon::diags() << DgSqlCode(4499) << DgInt0(cacheRows) << DgInt1(2)
                            << DgInt2((nNeedMemroyInMB * 100) / percentCanBeUsed);
        cacheRows = 2;
      }
    }
  }

  if (isNeedPushDownLimit && (topNRows_ > 0) && (!orderRequired_) && (topNRows_ < cacheRows) &&
      (topNRows_ <= ActiveSchemaDB()->getDefaults().getAsULong(GEN_SORT_TOPN_THRESHOLD)))
    cacheRows = topNRows_;

  hbpa->setNumCacheRows(cacheRows);
}

void Generator::setHBaseCacheBlocks(int hbaseRowSize, double estRowsAccessed,
                                    ComTdbHbaseAccess::HbasePerfAttributes *hbpa) {
  if (CmpCommon::getDefault(HBASE_CACHE_BLOCKS) == DF_ON)
    hbpa->setCacheBlocks(TRUE);
  else if (CmpCommon::getDefault(HBASE_CACHE_BLOCKS) == DF_SYSTEM) {
    float frac = ActiveSchemaDB()->getHbaseBlockCacheFrac();
    float regionServerCacheMemInMB = frac * getDefaultAsLong(HBASE_REGION_SERVER_MAX_HEAP_SIZE);
    float memNeededForScanInMB = (hbaseRowSize * estRowsAccessed) / (1024 * 1024);
    if (regionServerCacheMemInMB > memNeededForScanInMB) hbpa->setCacheBlocks(TRUE);
  }
}

void Generator::setHBaseSmallScanner(int hbaseRowSize, double estRowsAccessed, int hbaseBlockSize,
                                     ComTdbHbaseAccess::HbasePerfAttributes *hbpa) {
  if (CmpCommon::getDefault(HBASE_SMALL_SCANNER) == DF_SYSTEM) {
    if (((hbaseRowSize * estRowsAccessed) < hbaseBlockSize) &&
        (estRowsAccessed > 0))  // added estRowsAccessed > 0 because MDAM costing is not populating this field correctly
      hbpa->setUseSmallScanner(TRUE);
    hbpa->setUseSmallScannerForProbes(TRUE);
  } else if (CmpCommon::getDefault(HBASE_SMALL_SCANNER) == DF_ON) {
    hbpa->setUseSmallScanner(TRUE);
    hbpa->setUseSmallScannerForProbes(TRUE);
  }
  hbpa->setMaxNumRowsPerHbaseBlock(hbaseBlockSize / hbaseRowSize);
}

void Generator::setHBaseParallelScanner(ComTdbHbaseAccess::HbasePerfAttributes *hbpa) {
  hbpa->setDopParallelScanner(CmpCommon::getDefaultNumeric(HBASE_DOP_PARALLEL_SCANNER));
}

double Generator::getEstMemPerNode(NAString *key, int &numStreams) {
  OperBMOQuota *operBMOQuota = bmoQuotaMap_.get(key);
  if (operBMOQuota != NULL) {
    numStreams = operBMOQuota->getNumStreams();
    return operBMOQuota->getEstMemPerNode();
  } else {
    numStreams = 0;
    return 0;
  }
}

double Generator::getEstMemForTdb(NAString *key) {
  OperBMOQuota *operBMOQuota = bmoQuotaMap_.get(key);
  if (operBMOQuota != NULL)
    return operBMOQuota->getEstMemForTdb();
  else
    return 0;
}

double Generator::getEstMemPerInst(NAString *key) {
  OperBMOQuota *operBMOQuota = bmoQuotaMap_.get(key);
  if (operBMOQuota != NULL)
    return operBMOQuota->getEstMemPerInst();
  else
    return 0;
}

void Generator::finetuneBMOEstimates() {
  if (bmoQuotaMap_.entries() == 1) return;
  double bmoMemoryLimitPerNode = ActiveSchemaDB()->getDefaults().getAsDouble(BMO_MEMORY_LIMIT_PER_NODE_IN_MB);
  if (bmoMemoryLimitPerNode == 0) return;
  NAHashDictionaryIterator<NAString, OperBMOQuota> iter(bmoQuotaMap_);

  double capMemoryRatio = ActiveSchemaDB()->getDefaults().getAsDouble(BMO_MEMORY_ESTIMATE_RATIO_CAP);
  double bmoMemoryEstOutlier = ActiveSchemaDB()->getDefaults().getAsDouble(BMO_MEMORY_ESTIMATE_OUTLIER_FACTOR) *
                               bmoMemoryLimitPerNode * 1024 * 1024;

  double totalEstMemPerNode = totalBMOsMemoryPerNode_.value();
  double bmoMemoryRatio;
  double calcTotalEstMemPerNode = 0;
  double calcOperEstMemPerNode;

  NAString *key;
  OperBMOQuota *operBMOQuota;
  // Find the outliers and set it to the tolerable value first
  iter.reset();
  iter.getNext(key, operBMOQuota);
  while (key) {
    calcOperEstMemPerNode = operBMOQuota->getEstMemPerNode();
    if (calcOperEstMemPerNode > bmoMemoryEstOutlier) {
      operBMOQuota->setEstMemPerNode(bmoMemoryEstOutlier);
      calcTotalEstMemPerNode += bmoMemoryEstOutlier;
    } else
      calcTotalEstMemPerNode += calcOperEstMemPerNode;
    iter.getNext(key, operBMOQuota);
  }
  totalBMOsMemoryPerNode_ = calcTotalEstMemPerNode;

  // Then check for the CAP to adjust it again
  totalEstMemPerNode = totalBMOsMemoryPerNode_.value();
  calcTotalEstMemPerNode = 0;
  iter.reset();
  iter.getNext(key, operBMOQuota);
  while (key) {
    calcOperEstMemPerNode = operBMOQuota->getEstMemPerNode();
    bmoMemoryRatio = calcOperEstMemPerNode / totalEstMemPerNode;
    if (capMemoryRatio > 0 && capMemoryRatio <= 1 && bmoMemoryRatio > capMemoryRatio) {
      bmoMemoryRatio = capMemoryRatio;
      calcOperEstMemPerNode = bmoMemoryRatio * calcOperEstMemPerNode;
      operBMOQuota->setEstMemPerNode(calcOperEstMemPerNode);
      calcTotalEstMemPerNode += calcOperEstMemPerNode;
    } else
      calcTotalEstMemPerNode += calcOperEstMemPerNode;
    iter.getNext(key, operBMOQuota);
  }
  totalBMOsMemoryPerNode_ = calcTotalEstMemPerNode;
}

CostScalar Generator::findRowsToSurvive(const ValueId &outerColValId) {
  CollIndex idx = outerMinMaxKeys_.index(outerColValId);
  for (CollIndex i = 0; i < outerMinMaxKeys_.entries(); i++) {
    ValueIdSet vidSet(outerMinMaxKeys_[i]);

    ValueIdSet result;
    vidSet.findAllReferencedBaseCols(result);

    ItemExpr *ie = outerColValId.getItemExpr();

    ValueId id;
    switch (ie->getOperatorType()) {
      case ITM_INDEXCOLUMN: {
        IndexColumn *ixCol = (IndexColumn *)(ie);
        id = ixCol->getDefinition();
      } break;

      case ITM_BASECOLUMN:
        id = outerColValId;
        break;

      default:
        continue;
    }

    if (result.contains(id)) return outerRowsToSurvive_[i];
  }

  return 0.0;
}
