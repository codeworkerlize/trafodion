
#ifndef GENERATOR_H
#define GENERATOR_H

/* -*-C++-*-
 *****************************************************************************
 *
 * File:         Generator.h
 * Description:  This class defines the high level generator.
 *
 * Created:      4/15/95
 * Language:     C++
 *
 *
 *****************************************************************************
 */

// -----------------------------------------------------------------------

#include "GenMapTable.h"
#include "comexe/ComTdb.h"
#include "comexe/ComTdbHbaseAccess.h"
#include "comexe/ComTdbStats.h"
#include "comexe/FragmentDir.h"
#include "comexe/SqlTableOpenInfo.h"
#include "common/ComSpace.h"
#include "common/ComTransInfo.h"
#include "common/Int64.h"
#include "common/str.h"
#include "exp/exp_clause.h"
#include "export/NABasicObject.h"
#include "optimizer/OptUtilIncludes.h"

class ExprNode;
class ExpGenerator;
void GeneratorAssert(const char *f, int l, const char *a, const char *m);
void GeneratorExit(const char *file, int line);
class ex_cri_desc;
class ExplainTuple;
class TransMode;
class TableDesc;
class IndexDesc;
class CmpContext;
class ComTdb;
class ComTdbDp2Oper;
class ComTdbRoot;
class LateNameInfo;
class LateNameInfoArray;
class GenOperSimilarityInfo;
class ComTdb;
class Attributes;
class DP2Insert;
class TrafSimilarityTableInfo;
class OperBMOQuota;

// this define is used to raise assertion in generator.
// Calls GeneratorAssert which does a longjmp out of the calling scope.
#define GenAssert(p, msg)                                  \
  if (!(p)) {                                              \
    if (msg)                                               \
      GeneratorAssert(__FILE__, __LINE__, NULL, msg);      \
    else                                                   \
      GeneratorAssert(__FILE__, __LINE__, "" #p "", NULL); \
  };

#define GenExit() \
  { GeneratorExit(__FILE__, __LINE__); };

struct CifAvgVarCharSizeCache {
  ValueId vid;
  double avgSize;
};

class XBMOQuotaMap : public NAKeyLookup<NAString, OperBMOQuota> {
 public:
  XBMOQuotaMap(CollHeap *heap) : NAKeyLookup<NAString, OperBMOQuota>(10, NAKeyLookupEnums::KEY_INSIDE_VALUE, heap) {}
};

//////////////////////////////////////////////////////////////////////////
// class Generator
//////////////////////////////////////////////////////////////////////////
class Generator : public NABasicObject {
  enum {
    TRANSACTION_FLAG = 0x0001,             // transaction is needed somewhere
    VSBB_INSERT = 0x0002,                  // stmt uses VSBB insert
    UPD_ERROR_INTERNAL_ON_ERROR = 0x0004,  // return error on error even
                                           // if it leaves inconsistent
                                           // database. Used for internal
                                           // commands, like ddl, refresh.
    UPD_ERROR_ON_ERROR = 0x0008,           // return error on error, if possible.
                                           // See ComDiags flag noRollbackOnError.
    UPD_ABORT_ON_ERROR = 0x0010,           // rollback at runtime on an error
                                           // during an IUD query.
    UPD_PARTIAL_ON_ERROR = 0x0020,
    SAVEPOINT_ENABLED = 0x0040,

    // this is an updatable select query
    UPDATABLE_SELECT = 0x0080,

    // Processing a nested firstn sample balance expression.
    // (see ItmBalance::preCodeGen() in GenItemSampling.cpp)
    IN_NESTED_FIRSTN_EXPR = 0x0100,

    // For create index operations, even if the index is non-audited (which
    // it is during the load phase), the transaction id needs to be passed
    // to all ESPs used for loading the index. This is to ensure that
    // DP2 does not return error 73 during the open of the index at
    // load time, due to an in-progress DDL operation, namely the create
    // index itself. (See GenRelUpdate.cpp, DP2Insert::codeGen for
    // details on how this flag is used).
    PASS_TRANSACTION_IF_EXISTS = 0x0200,

    // if set, generate 'lean' expr.
    // A lean expr is generated to reduce the size of expr.
    // It only contains pcode and no clauses.
    // If pcode could not be generated or a clause_eval pcode
    // is generated, then 'lean' expr is not generated.
    GEN_LEAN_EXPR = 0x0400,

    DO_EID_SPACE_USAGE_OPT = 0x0800,

    // this flag is used to indicate to the Root TDB that this is a Non-atomic statement
    // where nonfatal errors are to be tolerated.
    TOLERATE_NONFATAL_ERROR = 0x2000,

    // this flag is used to indicate to Tuple Flow's right child tree
    // that is a Non-atomic statement. It is turned off as soon as the
    // right child is codegenned, while the previous flag is not turned off
    // and is valid for the whole tree. The intention is to turn on the tolerate
    // nonfatalerror only for PA nodes that are in the right child tree of TF (flow node)
    TOLERATE_NONFATAL_ERROR_IN_TF_RC = 0x4000,

    // if NOT set, generate PCode that checks value range for moving
    // (assign) from float64 to float64 as there could be cases
    // such as from application that values are not within the
    // IEEE float64 range. Otherwise, we just do simple assignment
    // in PCode evaluation
    GEN_NO_FLOAT_VALIDATE_PCODE = 0x8000,

    // this flag indicates that we are operating on the right side
    // of a Tuple Flow node.  The operators being generated
    // on the right side expect to receive lots of input.

    RIGHT_SIDE_OF_TUPLE_FLOW = 0x10000,

    // skip partitions at runtime, if unavailable
    SKIP_UNAVAILABLE_PARTITION = 0x20000,

    // mts statement, either (a) select [last 1] ... insert .. select...
    // or (b) select [last 1] ... delete ...
    EMBEDDED_IUD_WITH_LAST1 = 0x40000,

    // Rowset update or delete. We need to compute rowsaffected
    // for each rowset element
    COMPUTE_ROWSET_ROWSAFFECTED = 0x80000,

    // Statement performs some kind of write operation
    // say with a GU (for Insert.Update,Delete) or DDL node
    NEEDS_READ_WRITE_TRANSACTION = 0x100000,

    // IUD Statement uses WITH NO ROLLBACK syntax
    WITH_NO_ROLLBACK_USED = 0x00200000,

    // Embedded insert statement
    EMBEDDED_INSERT = 0x00400000,

    // Embedded insert / select in the tree
    EMBEDDED_INSERT_SELECT = 0x00800000,

    USER_SIDETREE_INSERT = 0x1000000,
    // Precodegen tree walk is below an ESP exchange --
    // solution 10-071204-9253.
    PRECODEGEN_PARALLEL = 0x2000000,

    // Halloween precodegen needs to check for unsychronized
    // sort -- solution 10-071204-9253.
    CHECK_UNSYNC_SORT = 0x4000000,

    // Halloween nested join needs to insert an ESP exchange
    // to prevent unsychonized sort operators --
    // solution 10-071204-9253.
    UNSYNC_SORT_FOUND = 0x8000000,

    // this flag indicates that we are operating on the right side
    // of a Nested Join node.  The operators being generated
    // on the right side expect to receive lots of input.

    RIGHT_SIDE_OF_ONLJ = 0x10000000,

    // Does any scan use SERIALIZABLE access?
    ANY_SERIALIZABLE_SCAN = 0x20000000,

    // if AQR(auto query retry) is enabled and could be done at runtime
    AQR_ENABLED = 0x40000000,

    // an inMemory object definition was used in this query
    IN_MEMORY_OBJECT_DEFN = 0x80000000,

    // If the statement can be reclaimed
    CANT_RECLAIM_QUERY = 0x1000
  };

  /* All enum values for the flags_ have been used. Use this enum with
     the flags2_ variable */
  enum {
    DP2_XNS_ENABLED = 0x00000001  // could use dp2 transactions at runtime
    ,
    UNUSED_1 = 0x00000002  // available for reuse

    // if set, then sidetreeinsert operator need to be added.
    ,
    ENABLE_TRANSFORM_TO_STI = 0x00000004,
    IS_FAST_EXTRACT = 0x00000008  // UNLOAD query

    // if hdfs is being accessed at runtime.
    ,
    HDFS_ACCESS = 0x00000020

    ,
    MAY_NOT_CANCEL = 0x00000040,
    PA_OF_GENERIC_UPDATE = 0x00000080  // partn access child is IUD
    ,
    AQR_WNR_INSERT_EMPTY = 0x00000100

    // if trafodion/hbase IUD operation is using RI inlining
    ,
    RI_INLINING_FOR_TRAF_IUD = 0x00000200

    // If Hive tables are accessed at runtime

    ,
    HIVE_READ_ACCESS = 0x00000400,
    HIVE_WRITE_ACCESS = 0x00000800,
    CONTAINS_FAST_EXTRACT = 0x00001000,
    AUTO_COLLECT_RTSTATS = 0x00002000,
    MINMAX_WITH_RANGE_OF_VALUES = 0x00004000,
    PARQUET_IN_SUPPORT = 0x00008000,
    EFF_TREE_UPSERT = 0x00010000,
    PARQUET_USE_CPP_READER = 0x00020000

    ,
    HAS_COMPOSITE_EXPR = 0x00040000,
    BLOOMFILTER_USE_NATIVE_BYTEORDER = 0x00080000

    ,
    USE_REGION_XN = 0x00100000,
    SKIP_WRITE_MUTATION_FOR_IB = 0x00200000
  };

  NAHeap *wHeap_;  // The heap to allocate space from.
                   // If NULL, allocate from stmtheap
                   // from currentCmpContext_.
                   // Must be declared up in the
                   // data member list as wHeap_
                   // must be initialized first
                   // in Generator::Generator()

  // Each operator node receives some tupps in its input atp and
  // returns back some tupps in its output atp. See Executor Programmers
  // Guide or .h files in executor directory for details on tupp and atps.
  // The description of these tupps is defined by a cri (composite row
  // instance) by the class ex_cri_desc.
  // Each operator node receives its input cri descriptor in down_cri_desc
  // and returns the output cri desc in up_cri_desc.
  ex_cri_desc *down_cri_desc;
  ex_cri_desc *up_cri_desc;

  // points to the TDB of the last generated code (set for each node)
  ComTdb *genObj;
  int genObjLength;

  // Fragment directory. When the plan uses multiple Executor Server
  // Processes (ESPs), this directory contains information about which
  // part of the plan is executed in which set of ESPs.
  FragmentDir *fragmentDir_;

  // The map table. See GenMaptable.h.
  MapTable *firstMapTable_;
  MapTable *lastMapTable_;

  // The expression generator. Used to generate expressions.
  // See ExpGenerator.h.
  ExpGenerator *exp_generator;

  // Identifier specifying a table (base table or index).
  // Starts at 0 and increments for each table used in the query.
  int tableId_;

  // Identifier specifying a temp table (hash, btree or queue).
  // Starts at 0 and increments for each temp used in the query.
  int tempTableId_;

  // unique id for TDBs, incremented for each new TDB
  // Starts at 0 (for root) and increments for each TDB used in the query.
  int tdbId_;

  // Work area of the binder. Passed to binder when binding/type-propagating
  // trees created at generation time.
  BindWA *bindWA;

  int flags_;

  int flags2_;

  long planId_;        // timestamp used by explain
  int explainNodeId_;  // current number for EXPLAIN node

  // Pointer to a DP2 table update (as oppossed to index update) operator.
  // Both pointers are set by the update codeGen and then
  // grabbed and reset by partition access codeGen so that partition access
  // can get a handle on any update children operators for producing
  // index maintenance nodes. void* are used to keep generator isolated
  // from relational expression and tdb classes.
  //
  void *imUpdateRel_;
  void *imUpdateTdb_;

  // Placeholder for update node to be used by the RelRoot node in the
  // case of UPDATE CURRENT OF during codeGen.
  void *updateCurrentOfRel_;

  CollIndex explainFragDirIndex_;  // Index of the Frag used to store
                                   // explain Info.  Set on first call
                                   // to populate

  ExplainTuple *explainTuple_;  // The Value of the last explain Tuple generated.
                                // Used as a return value from codeGen.
  const char *stmtSource_;

  CmpContext *currentCmpContext_;  // the current CmpContext

  int explainIsDisabled_;  // if greater than zero, calls to
                           // addExplainInfo are disabled
  int affinityValueUsed_;  // The Affinity value used for ESP remapping.
                           // This is the value after interpreting
                           // special values (-1, -3, -4)

  // Set to TRUE, if runtime stats are to be gathered for this
  // statement. statsType_ contains info about what kind of stats are
  // to be collected.
  NABoolean computeStats_;
  // TRUE means explain fragment will be stored in RMS shared segment
  NABoolean explainInRms_;
  ComTdb::CollectStatsType collectStatsType_;

  // ---------------------------------------------------------------------
  // This tdb id is used to consolidate information on a per table basis
  // similar to the way it is done in sqlmp. Valid when PERTABLE_STATS are
  // being collected. All tdbs that are working for the same table
  // (like, PA, DP2 oper) get the same tdb id at code generation time.
  // ---------------------------------------------------------------------
  int pertableStatsTdbId_;

  // Set to TRUE if this is static compilation.
  NABoolean staticCompMode_;

  // For late-name resolution, need the input expression in order to
  // determine the position of the table name host variable within
  // the given input variable list. This index is set at
  // compile time, so that no additional processing is required
  // at run-time to determine where the input host variable is.
  // Thus, the input expression is used by PartitionAccess::codeGen
  // to compute the index of the tablename host variable, if any,
  // in the input variable list.
  void *inputExpr_;

  // The following lists are used by PartitionAccess::codeGen() to determine
  // what base files and cooresponding VPs are accessed in DP2.
  LIST(const IndexDesc *) baseFileDescs_;        // base file descriptors
  LIST(const SqlTableOpenInfo *) baseStoiList_;  // base file open infos
  LIST(CollIndex) numOfVpsPerBase_;              // number of VPs per base file
  LIST(const IndexDesc *) vpFileDescs_;          // VP descriptors for all files

  // List of information for each table that appears in the query.
  // Used at runtime to do late name resolution.
  LIST(const LateNameInfo *) lateNameInfoList_;

  // List of info for each operator that accesses a DP2 table in a query.
  // Used to generate similarity info list in Root::codeGen.
  LIST(const GenOperSimilarityInfo *) genOperSimInfoList_;

  LIST(const SqlTableOpenInfo *) stoiList_;

  LIST(const TrafSimilarityTableInfo *) trafSimTableInfoList_;

  // Some nodes in the tree may require the root node to compute
  // special internal input values, such as an execution count
  // or the current transaction id. The CURRENT_TIMESTAMP function
  // would be another example, but it is currently handled before
  // code generation and not stored in this list.
  ValueIdSet internalInputs_;

  ValueIdList prefixSortKey_;

  OltOptInfo oltOptInfo_;

  // SQLCOMP defaults for dynamic queue resizing
  int initialQueueSizeDown_;
  int initialQueueSizeUp_;
  int initialPaQueueSizeDown_;
  int initialPaQueueSizeUp_;
  short queueResizeLimit_;
  short queueResizeFactor_;
  NABoolean dynQueueSizeValuesAreValid_;

  // MV
  NABoolean isInternalRefreshStatement_;

  NABoolean nonCacheableMVQRplan_;
  NABoolean nonCacheableCSEPlan_;

  // is set when relroot has an order by requirement
  NABoolean orderRequired_;

  // flag to indicate update/delete/insert operations are performed
  // on either side of compound statement operator to detect whether the
  // left child or the right child of CS returns rows
  // This is reset in PhysCompoundStmt::codeGen() before calling codeGen()
  // on the left child and before calling codeGen() on the right child
  NABoolean foundAnUpdate_;

  // flag to indicate whether any update/delete/insert operations are
  // performed before each select statement within the compound statement
  // This flag is never reset and is set to FALSE only in the beginning
  NABoolean updateWithinCS_;

  NABoolean savedGenLeanExpr_;

  ComSpace *tempSpace_;

  // indicates to the split-top that this is a LRU query. This flag
  // is set during RelRoot::codeGen(); that is where we know if this
  // is a LRU query or not.
  NABoolean lruOperation_;

  // Flag to indicate if SeaMonster is used anywhere in this query
  NABoolean queryUsesSM_;

  // Increasing counter for SeaMonster tags
  int genSMTag_;

  // temporary value holder (during pre code gen) for #BMOs in this fragment
  unsigned short numBMOs_;

  CostScalar totalBMOsMemoryPerNode_;  // accumulated BMO memory, per Node

  CostScalar nBMOsMemoryPerNode_;  // accumulated nBMO memory, per Node

  // BMO memory limit per Node
  CostScalar BMOsMemoryLimitPerNode_;

  // Total number of BMOs in the query
  unsigned short totalNumBMOs_;

  // temporary value holder that indicates number of esp instances. This value
  // is accessed by child sort operator during code gen. Value set in exchange
  // operator which is the parent operator. Default is 1.
  int numESPs_;

  // total number of ESPs for the query.
  int totalNumESPs_;

  // level of esp layer relative to root node. First esp layer is 1.
  int espLevel_;

  // Member to hold if RTS stats like (process busy time in master, ESPs and to get
  // Memory usage if COMP_BOOL_156 is ON. Default is COMP_BOOL_156 is ON
  NABoolean collectRtsStats_;

  CompilerStatsInfo compilerStatsInfo_;

  // Helpers for the special ONLJ queue sizing defaults.
  NABoolean makeOnljLeftQueuesBig_;
  int onljLeftUpQueue_;
  int onljLeftDownQueue_;
  NABoolean makeOnljRightQueuesBig_;
  int onljRightSideUpQueue_;
  int onljRightSideDownQueue_;
  ComTdb::OverflowModeType overflowMode_;

  // Used to specify queue size to use on RHS of flow or nested join.
  // Initialized to 0 (meaning don't use).  Set to a value by SplitTop
  // when on RHS of flow and there is a large degree of fanout. In this
  // situation, large queues are required above the split top (between
  // Split Top and Flow/NestedJoin).  Also used to set size of queues
  // below a SplitTop on RHS, but these do not need to be as large.
  int largeQueueSize_;

  // For self-referencing queries.  At the beginning of preCodeGen, if protection
  // from Halloween is needed, we will set the type to DP2Locks (in RelRoot::
  // codeGen). When the preCodeGen method is called for the NestJoin which
  // implements the TSJForWrite, if NJ's left child is a sort, the DP2Locks
  // value will be changed.  It is changed to either FORCED_SORT, if the
  // optimizer inserted the sort specifically to protect agains Halloween, or
  // else it is changed to PASSIVE if a sort was produced by the optimizer
  // for any other reason.  The distinction, FORCED_SORT vs PASSIVE is used
  // only in the EXPLAIN.

  // CIF -

  GroupAttributes *tupleFlowLeftChildAttrs_;

  // pre-code-gen phase esp fragment currently being worked on
  pcgEspFragment *currentEspFragmentPCG_;

  // recompile plan after this time
  // -1: Never recompile (the default)
  //  0: Always recompile
  // >0: Recompile after this timestamp
  long planExpirationTimestamp_;

  NASet<long> objectUids_;

  NASet<NAString> objectNames_;
  char *snapshotScanTmpLocation_;

  // Help define a range in the minVals_ and maxVals_ list
  // to help the exchange node of child 0 of a HJ to
  // obtain the min and max host vars. The range is set
  // by the HJ in question.
  CollIndex startMinMaxIndex_;
  CollIndex endMinMaxIndex_;

  Int16 numTrafReplicas_;

 public:
  enum HalloweenProtectionType { DP2LOCKS, FORCED_SORT, PASSIVE, NOT_SELF_REF };

 private:
  HalloweenProtectionType halloweenProtection_;

  // For self-referencing queries, while the plan allows Halloween protection
  // by using DP2Locks, collect all the insert nodes during pre-codegen.
  LIST(DP2Insert *) insertNodesList_;

  // Start of flags to fix 10-100310-8659 >>>

  // By default, in R2.5.1, we do not apply the fix for solution
  // 10-100310-8659.  This was a management decision to limit risk.
  // In Seaquest the fix is turned on by default.  The toggle to turn it
  // off and on is COMP_BOOL_166.  When this CQD is ON the fix is off.
  // It would be a good idea to simplify the code on Seaquest and remove
  // the old way that preCodeGen turns off DP2_LOCKS for incompatible plans.
  // When that code change is made, one can follow this r251HalloweenPrecode_
  // usage to get guidance.  At that point, r251HalloweenPrecode_ can be
  // be removed.
  bool r251HalloweenPrecode_;

  // Two flags to help give context to preCodeGen methods, when handling
  // Halloween inserts.
  // On the left hand side of TSJ for Write.
  bool precodeHalloweenLHSofTSJ_;

  // On RHS of any nested join.
  bool precodeRHSofNJ_;

  // Count the # of scans below the TSJ-for-write that access
  // the IUD target table without an intervening blocking operator
  // combination.
  int unblockedHalloweenScans_;

  // If Halloween protection is achieved with a blocking sort, is this
  // considered FORCED or PASSIVE?
  bool halloweenSortForced_;

  // At or below an ESP on LHS of halloween TSJ for Write;
  bool halloweenESPonLHS_;

  // <<< end of flags to fix 10-100310-8659

  // total estimated memory used by BMOs and certain other operators in bytes
  double totalEstimatedMemory_;

  // estimated memory for an individual operator. Used by Explain
  // set to 0 after Explain has been called so that next operator
  // can used this field. In KB and on a per Node basis.
  int operEstimatedMemory_;

  Int16 maxCpuUsage_;

  // Data format to use - SQLARK_EXPLODED_FORMAT or the more compressed
  // SQLMX_ALIGNED_FORMAT
  Int16 internalDataFormat_;

  // Added for support of the HashJoin min/max optimization.

  // Candidate values for columns of the right (inner) table for which
  // min/max values could be computed.  Values are added to this list
  // by HashJoin::preCodeGen() in the pre-order phase of the tree
  // walk. (before calling codegen on the children.)  These values are
  // available for scans on the left hand (outer) side of the join
  // that added them. If the join predicate is a VEGPredicate, both
  // inner and outer key point to the VEGRef.
  ValueIdList innerMinMaxKeys_;
  ValueIdList outerMinMaxKeys_;

  // These are the min values corresponding to the outerMinMaxKeys_ above.
  // These are system generated hostvars at this point.
  ValueIdList minVals_;

  // These are the max values corresponding to the outerMinMaxKeys_ above.
  // These are system generated hostvars at this point.
  ValueIdList maxVals_;

  // These are the range of values corresponding to the outerMinMaxKeys_ above.
  // These are system generated hostvars at this point.
  ValueIdList rangeOfValuesVals_;

  // The UECs of the inner child arriving at the HJ, which initiates the
  // min/max.
  NAList<CostScalar> innerChildUecs_;

  // Whether the inner child is broadcast partitioned
  NAList<NABoolean> innerIsBroadcastPartitioned_;

  // The callers (HJs) that call addMinMaxVals()
  NAList<RelExpr *> callers_;

  // The row count produced by HJ on the join column
  NAList<CostScalar> rowsProducedByHJ_;

  // The expected number of rows to survive the filtering
  NAList<CostScalar> outerRowsToSurvive_;

  // the min/max values above have three boolean indicators associated
  // with them:
  // enabled: Is this value available for use by scan nodes right now?
  //          A value may be disabled because we are above the node that
  //          produces it or for other reasons.
  // non-partitioned: This means that we have only a single hash table
  //          or a replicated hash table in the hash join(s). It means
  //          that we can use the min/max values in other plan fragments.
  // used:    This indicates that one or more scan nodes actually
  //          use a value, so the hash join that entered this value
  //          must actually produce it at runtime
  NABitVector minMaxEnabled_;
  NABitVector minMaxNonPartitioned_;
  NABitVector minMaxUsed_;

  int nCIFNodes_;

  // used to disable strawscan if there is too many strawscan scans.
  int nbStrawscanCandidates_;

  // used if an ExeUtilWnrInsert node is added.
  // see RelRoot::preCodeGen where this is added.
  CorrName utilInsertTable_;

  // Used by PCODE Native Expressions
  NExDbgInfo NExDbgInfoObj_;  // Only 1 needed, so we put it in Generator object
  char NExLogPathNam_[1024];  // Only 1 needed, so we put it in Generator object

  // the signature hash of the query text
  UInt64 queryHash_;

  LIST(CifAvgVarCharSizeCache) avgVarCharSizeList_;

  UInt32 topNRows_;
  // LIST(double) avgVarCharSizeValList_;
  void addCifAvgVarCharSizeToCache(ValueId vid, double size) {
    struct CifAvgVarCharSizeCache s;
    s.vid = vid;
    s.avgSize = size;

    avgVarCharSizeList_.insert(s);
  };

  NABoolean findCifAvgVarCharSizeToCache(ValueId vid, double &v) {
    for (CollIndex i = 0; i < avgVarCharSizeList_.entries(); i++) {
      if (avgVarCharSizeList_.at(i).vid == vid) {
        v = avgVarCharSizeList_.at(i).avgSize;
        return TRUE;
      }
    }
    return FALSE;
  };

  XBMOQuotaMap bmoQuotaMap_;

 public:
  enum cri_desc_type { UP, DOWN };

  Generator(CmpContext *cmpCurrentContext = 0, NAHeap *workHeap = NULL);
  ~Generator();

  // See above...
  //
  void *&imUpdateRel() { return (imUpdateRel_); }
  void *&imUpdateTdb() { return (imUpdateTdb_); }

  void *&updateCurrentOfRel() { return updateCurrentOfRel_; }

  // Accessor and mutators for base/VP file descriptor lists.
  //
  LIST(const IndexDesc *) & getBaseFileDescs() { return baseFileDescs_; }
  LIST(const SqlTableOpenInfo *) & getBaseStoiList() { return baseStoiList_; }
  LIST(CollIndex) & getNumOfVpsPerBase() { return numOfVpsPerBase_; }
  LIST(const IndexDesc *) & getVpFileDescs() { return vpFileDescs_; }
  CollIndex addFileDesc(const IndexDesc *desc, SqlTableOpenInfo *stoi);
  CollIndex addVpFileDesc(const IndexDesc *vpDesc, SqlTableOpenInfo *stoi, CollIndex &vpIndex);
  void resetFileDescInfo() {
    baseFileDescs_.clear();
    baseStoiList_.clear();
    numOfVpsPerBase_.clear();
    vpFileDescs_.clear();
  }
  // Accessor and mutators for data members related to late table
  // name resolution.
  //
  LIST(const LateNameInfo *) & getLateNameInfoList() { return lateNameInfoList_; }

  void addLateNameInfo(const LateNameInfo *lni) { lateNameInfoList_.insert(lni); }

  void resetLateNameInfoList() { lateNameInfoList_.clear(); }

  void addTrafSimTableInfo(TrafSimilarityTableInfo *ti);

  // Accessor and mutators for data members related to similarity check.
  LIST(const GenOperSimilarityInfo *) & genOperSimInfoList() { return genOperSimInfoList_; }

  // Accessor and mutators for data members related to traf similarity check.
  LIST(const TrafSimilarityTableInfo *) & getTrafSimTableInfoList() { return trafSimTableInfoList_; }

  LIST(const SqlTableOpenInfo *) & getSqlTableOpenInfoList() { return stoiList_; }

  void addSqlTableOpenInfo(const SqlTableOpenInfo *lni) { stoiList_.insert(lni); }

  void resetSqlTableOpenInfoList() { stoiList_.clear(); }

  // Initialize the common data members of a TDB from the defaults
  // table and with counters kept in the Generator object
  void initTdbFields(ComTdb *tdb);

  // Code to get and set the input expression used by the PartitionAccess
  // class to build late name resolution information.
  inline void *getInputExpr() { return inputExpr_; }
  inline void setInputExpr(void *inputExpr) { inputExpr_ = inputExpr; }

  // called to preCodeGen the expr_node. Pre-codegen modifies
  // the input tree to make it suitable for the generator.
  // May return an ExprNode different than the input.
  // Returns NULL if an error occurs.
  RelExpr *preGenCode(RelExpr *table_expr);

  // called to generate code the expr_node. Call to getGenObj() must
  // be made after this to get the generated code.
  void genCode(const char *source, RelExpr *expr_node);

  // Space for the generated code is not allocated from system heap.
  // It is allocated from space managed by the generator. This is
  // needed because the generated code space for the master executor
  // and for each ESP has to be contiguous
  // so that it could be written out to disk or sent back to executor
  // after code generation as a stream of bytes. It is also needed
  // to convert pointers to offsets and vice-versa during packing
  // and unpacking phase. For packing/unpacking see ex_pack.C.
  // 'space' points to this generator managed space.
  // The 'new' method has also been overloaded to take 'space'
  // as a parameter and allocate space from there. See class ExGod
  // in ex_god.C.
  inline ComSpace *getSpace() {
    if (tempSpace_)
      return tempSpace_;
    else
      return fragmentDir_->getCurrentSpace();
  }

  ComSpace *getTempSpace() { return tempSpace_; }

  void setTempSpace(ComSpace *space, NABoolean delPrevTempSpace) {
    if ((tempSpace_) && (delPrevTempSpace)) delete tempSpace_;

    tempSpace_ = space;
  }

  // returns the last generated code. Returns NULL, if no code was generated.
  inline ComTdb *getGenObj() { return genObj; };
  void setGenObj(const RelExpr *node, ComTdb *genObj_);

  // moves all of the generated code into out_buf. If the generated code
  // is allocated from a list of buffers, then each of the buffer is
  // moved contiguously to out_buf. The caller MUST have allocated
  // sufficient space in out_buf to contain the generated code.
  // Returns out_buf as the return parameter. Returns NULL, in case
  // of error.
  char *getFinalObj(char *out_buf, int out_buflen);

  inline int getFinalObjLength() { return fragmentDir_->getTotalLength(); };

  //  void doRuntimeSpaceComputation();
  void doRuntimeSpaceComputation(char *root_tdb, char *fragTopNode, int &tcbSize);

  inline ex_cri_desc *getCriDesc(cri_desc_type type) {
    if (type == UP)
      return up_cri_desc;
    else
      return down_cri_desc;
  };
  inline void setCriDesc(ex_cri_desc *cri_desc_, cri_desc_type type) {
    if (type == UP)
      up_cri_desc = cri_desc_;
    else
      down_cri_desc = cri_desc_;
  };

  static void computeAvailableNodes(NAWNodeSet *&availableNodes, const NAWNodeSet *availableNodesFromCLI,
                                    const NAWNodeSet *availableNodesFromOSIM, int defaultAffinity, CollHeap *heap);
  void remapESPAllocation();
  void remapESPAllocationAS(const NAASNodes *asNodes);
  void remapESPAllocationRandomly();

  ExpTupleDesc::TupleDataFormat determineInternalFormat(const ValueIdList &valIdList, RelExpr *relExpr,
                                                        NABoolean &resizeCifRecord, RelExpr::CifUseOptions cif,
                                                        NABoolean bmo_affinity, NABoolean &considerBufferDefrag,
                                                        UInt32 prefixLength = 0);

  ExpTupleDesc::TupleDataFormat determineInternalFormat(const ValueIdList &valIdList, RelExpr *relExpr,
                                                        NABoolean &resizeCifRecord, RelExpr::CifUseOptions cif,
                                                        NABoolean bmo_affinity, UInt32 &alignedLength,
                                                        UInt32 &explodedLength, UInt32 &alignedVarCharSize,
                                                        UInt32 &alignedHeaderSize, double &avgVarCharUsage,
                                                        UInt32 prefixLength = 0);

  NABoolean considerDefragmentation(const ValueIdList &valIdList, GroupAttributes *gattr, NABoolean resizeCifRecord,
                                    UInt32 prefixLength = 0);

  void incNCIFNodes() { nCIFNodes_++; }
  void decNCIFNodes() { nCIFNodes_--; }
  void initNNodes() { nCIFNodes_ = 0; }
  int getNCIFNodes() { return nCIFNodes_; }

  void incNbStrawScans() { nbStrawscanCandidates_++; }
  void initNbStrawScans() { nbStrawscanCandidates_ = 0; }
  int getNbStrawScans() { return nbStrawscanCandidates_; }

  GroupAttributes *getTupleFlowLeftChildAttrs() const { return tupleFlowLeftChildAttrs_; }

  void setTupleFlowLeftChildAttrs(GroupAttributes *gatt) { tupleFlowLeftChildAttrs_ = gatt; }
  //////////////////////////////////////////////////////
  // methods to manipulate the map tables
  //////////////////////////////////////////////////////

  // returns the map table containing id. Null otherwise.
  MapTable *getMapTable(ValueId id);

  // returns the first & last map tables
  MapTable *getMapTable() { return firstMapTable_; };
  MapTable *getLastMapTable() { return lastMapTable_; };

  // appends 'map_table' to the end of the
  // list of map tables.
  // If no map table is passed in, allocates a new map table
  // and appends it.
  // Returns pointer to the maptable being added.
  MapTable *appendAtEnd(MapTable *map_table = NULL);

  // searches for value_id in the list of map tables.
  // If mapTable is input, starts the search from there.
  // Returns MapInfo, if found.
  // Raises assertion, if not found.
  MapInfo *getMapInfo(const ValueId &value_id, MapTable *mapTable = NULL);

  // searches for value_id in the list of map tables.
  // If mapTable is input, starts the search from there.
  // Returns MapInfo, if found.
  // Returns NULL, if not found.
  MapInfo *getMapInfoAsIs(const ValueId &value_id, MapTable *mapTable = NULL);

  // adds to the last maptable, if value doesn't exist.
  // Returns the MapInfo, if that value exists.
  MapInfo *addMapInfo(const ValueId &value_id, Attributes *attr);

  // adds to input mapTable. Does NOT check if the value exists.
  // Caller should have checked for that.
  // Returns the MapInfo for the added value.
  MapInfo *addMapInfoToThis(MapTable *mapTable, const ValueId &value_id, Attributes *attr);

  // gets MapInfo from mapTable.
  // Raises assertion, if not found.
  MapInfo *getMapInfoFromThis(MapTable *mapTable, const ValueId &value_id);

  // deletes ALL maptables starting at 'next' of inMapTable.
  // If no mapTable has been passed in, deletes all.
  // Makes inMapTable the last map table.
  void removeAll(MapTable *inMapTable = NULL);

  // removes the last map table in the list.
  void removeLast();

  // unlinks the next mapTable in the list.
  void unlinkNext(MapTable *mapTable);

  // unlinks the this mapTable in the list, and whatever follows
  void unlinkMe(MapTable *mapTable);

  // unlinks the last mapTable in the list and returns it.
  MapTable *unlinkLast();

  void setMapTable(MapTable *map_table_);

  void displayMapTables(int depth = 5, const char *msg = NULL);

  void reportMappingInfo(ostream &out, const ValueIdList &list, const char *title);

  // returns attributes corresponding to itemexpr ie.
  // If clause has already been generated for this ie, then returns
  // attribute from operand 0 (result) of clause.
  // If not, searches the map table and returns it from there.
  Attributes *getAttr(ItemExpr *ie);

  //////////////////////////////////////////////////////////////////

  inline ExpGenerator *getExpGenerator() { return exp_generator; };
  inline void setExpGenerator(ExpGenerator *exp_gen_) { exp_generator = exp_gen_; };

  // accessor functions to fragment directory and resource table
  inline FragmentDir *getFragmentDir() { return fragmentDir_; }

  // accessor functions to fragment directory and resource table
  inline FragmentDir *removeFragmentDir() {
    FragmentDir *fd = fragmentDir_;
    fragmentDir_ = NULL;
    return fd;
  }

  inline BindWA *getBindWA() { return bindWA; };
  inline void setBindWA(BindWA *bindWA_) { bindWA = bindWA_; };

  // returns current base table/index id and increments it.
  inline int getTableId() { return tableId_++; }

  // returns current temp table id and increments it.
  inline int getTempTableId() { return tempTableId_++; }

  // returns current TDB id and increments it.
  inline int getTdbId() { return tdbId_; }

  inline int getAndIncTdbId() { return tdbId_++; }
  // returns current TDB id and increments it.
  inline int getPertableStatsTdbId() { return pertableStatsTdbId_; }
  inline void setPertableStatsTdbId(int tdbId) { pertableStatsTdbId_ = tdbId; }

  const NAString genGetNameAsAnsiNAString(const QualifiedName &);
  const NAString genGetNameAsAnsiNAString(const CorrName &);
  const NAString genGetNameAsNAString(const QualifiedName &);
  const NAString genGetNameAsNAString(const CorrName &);

  // is transaction needed?
  inline int isTransactionNeeded() { return (flags_ & TRANSACTION_FLAG); }

  // If transaction is needed to execute this query, remember it.
  // The second argument is used when we know that the ESP fragments will
  // need a transaction (typically because the label of a table is
  // locked during DDL and we want to do DML inside the same transaction).
  void setTransactionFlag(NABoolean transIsNeeded, NABoolean isNeededForAllFragments = FALSE);

  void resetTransactionFlag();

  // For create index operations, even if the index is non-audited (which
  // it is during the load phase), the transaction id needs to be passed
  // to all ESPs used for loading the index. This is to ensure that
  // DP2 does not return error 73 during the open of the index at
  // load time, due to an in-progress DDL operation, namely the create
  // index itself.
  void setPassTransactionIfOneExists(NABoolean value) {
    if (value == TRUE)
      flags_ |= PASS_TRANSACTION_IF_EXISTS;
    else
      flags_ &= ~PASS_TRANSACTION_IF_EXISTS;
  }

  NABoolean passTransactionIfOneExists() {
    if (flags_ & PASS_TRANSACTION_IF_EXISTS)
      return TRUE;
    else
      return FALSE;
  }

  void setVSBBInsert(NABoolean vi) {
    if (vi == TRUE)
      flags_ |= VSBB_INSERT;
    else
      flags_ &= ~VSBB_INSERT;
  }

  NABoolean getVSBBInsert() {
    if (flags_ & VSBB_INSERT)
      return TRUE;
    else
      return FALSE;
  }

  void setUserSidetreeInsert(NABoolean vi) {
    if (vi == TRUE)
      flags_ |= USER_SIDETREE_INSERT;
    else
      flags_ &= ~USER_SIDETREE_INSERT;
  }

  NABoolean getUserSidetreeInsert() {
    if (flags_ & USER_SIDETREE_INSERT)
      return TRUE;
    else
      return FALSE;
  }

  NABoolean updErrorOnError() { return (flags_ & UPD_ERROR_ON_ERROR) != 0; };
  void setUpdErrorOnError(NABoolean v) { (v ? flags_ |= UPD_ERROR_ON_ERROR : flags_ &= ~UPD_ERROR_ON_ERROR); }

  NABoolean updErrorInternalOnError() { return (flags_ & UPD_ERROR_INTERNAL_ON_ERROR) != 0; };
  void setUpdErrorInternalOnError(NABoolean v) {
    (v ? flags_ |= UPD_ERROR_INTERNAL_ON_ERROR : flags_ &= ~UPD_ERROR_INTERNAL_ON_ERROR);
  }

  NABoolean updAbortOnError() { return (flags_ & UPD_ABORT_ON_ERROR) != 0; };
  void setUpdAbortOnError(NABoolean v) { (v ? flags_ |= UPD_ABORT_ON_ERROR : flags_ &= ~UPD_ABORT_ON_ERROR); }

  NABoolean updPartialOnError() { return (flags_ & UPD_PARTIAL_ON_ERROR) != 0; };
  void setUpdPartialOnError(NABoolean v) { (v ? flags_ |= UPD_PARTIAL_ON_ERROR : flags_ &= ~UPD_PARTIAL_ON_ERROR); }

  NABoolean savepointEnabled() { return (flags_ & SAVEPOINT_ENABLED) != 0; };
  void setSavepointEnabled(NABoolean v) { (v ? flags_ |= SAVEPOINT_ENABLED : flags_ &= ~SAVEPOINT_ENABLED); }

  NABoolean updatableSelect() { return (flags_ & UPDATABLE_SELECT) != 0; };
  void setUpdatableSelect(NABoolean v) { (v ? flags_ |= UPDATABLE_SELECT : flags_ &= ~UPDATABLE_SELECT); }

  NABoolean inNestedFIRSTNExpr() { return (flags_ & IN_NESTED_FIRSTN_EXPR) != 0; };
  void setInNestedFIRSTNExpr(NABoolean v) { (v ? flags_ |= IN_NESTED_FIRSTN_EXPR : flags_ &= ~IN_NESTED_FIRSTN_EXPR); }

  NABoolean genLeanExpr() { return (flags_ & GEN_LEAN_EXPR) != 0; };
  void setGenLeanExpr(NABoolean v) { (v ? flags_ |= GEN_LEAN_EXPR : flags_ &= ~GEN_LEAN_EXPR); }

  void setAndSaveGenLeanExpr(NABoolean v) {
    savedGenLeanExpr_ = genLeanExpr();
    setGenLeanExpr(v);
  }

  void restoreGenLeanExpr() { setGenLeanExpr(savedGenLeanExpr_); }

  void setLRUOperation(NABoolean v) { lruOperation_ = v; }

  NABoolean isLRUOperation() { return lruOperation_; }

  // For SeaMonster
  void setQueryUsesSM(NABoolean v) { queryUsesSM_ = v; }
  NABoolean getQueryUsesSM() const { return queryUsesSM_; }

  // Methods to retrieve and increment the current SeaMonster tag
  int getSMTag() const { return genSMTag_; }
  int getNextSMTag() { return ++genSMTag_; }

  NABoolean doEidSpaceUsageOpt() { return (flags_ & DO_EID_SPACE_USAGE_OPT) != 0; };
  void setDoEidSpaceUsageOpt(NABoolean v) {
    (v ? flags_ |= DO_EID_SPACE_USAGE_OPT : flags_ &= ~DO_EID_SPACE_USAGE_OPT);
  }

  NABoolean getTolerateNonFatalError() { return (flags_ & TOLERATE_NONFATAL_ERROR) != 0; };
  void setTolerateNonFatalError(NABoolean v) {
    (v ? flags_ |= TOLERATE_NONFATAL_ERROR : flags_ &= ~TOLERATE_NONFATAL_ERROR);
  }

  NABoolean getTolerateNonFatalErrorInFlowRightChild() { return (flags_ & TOLERATE_NONFATAL_ERROR_IN_TF_RC) != 0; };
  void setTolerateNonFatalErrorInFlowRightChild(NABoolean v) {
    (v ? flags_ |= TOLERATE_NONFATAL_ERROR_IN_TF_RC : flags_ &= ~TOLERATE_NONFATAL_ERROR_IN_TF_RC);
  }

  NABoolean genNoFloatValidatePCode() { return (flags_ & GEN_NO_FLOAT_VALIDATE_PCODE) != 0; };
  void setGenNoFloatValidatePCode(NABoolean v) {
    (v ? flags_ |= GEN_NO_FLOAT_VALIDATE_PCODE : flags_ &= ~GEN_NO_FLOAT_VALIDATE_PCODE);
  }

  NABoolean getRightSideOfTupleFlow() const { return (flags_ & RIGHT_SIDE_OF_TUPLE_FLOW) != 0; };

  void setRightSideOfTupleFlow(NABoolean v) {
    (v ? flags_ |= RIGHT_SIDE_OF_TUPLE_FLOW : flags_ &= ~RIGHT_SIDE_OF_TUPLE_FLOW);
  }

  NABoolean getRightSideOfOnlj() const { return (flags_ & RIGHT_SIDE_OF_ONLJ) != 0; };

  void setRightSideOfOnlj(NABoolean v) { (v ? flags_ |= RIGHT_SIDE_OF_ONLJ : flags_ &= ~RIGHT_SIDE_OF_ONLJ); }

  NABoolean getRightSideOfFlow() const { return (getRightSideOfTupleFlow() || getRightSideOfOnlj()); }

  NABoolean skipUnavailablePartition() { return (flags_ & SKIP_UNAVAILABLE_PARTITION) != 0; }
  void setSkipUnavailablePartition(NABoolean v) {
    (v ? flags_ |= SKIP_UNAVAILABLE_PARTITION : flags_ &= ~SKIP_UNAVAILABLE_PARTITION);
  }

  NABoolean embeddedIUDWithLast1() { return (flags_ & EMBEDDED_IUD_WITH_LAST1) != 0; }
  void setEmbeddedIUDWithLast1(NABoolean v) {
    (v ? flags_ |= EMBEDDED_IUD_WITH_LAST1 : flags_ &= ~EMBEDDED_IUD_WITH_LAST1);
  }

  NABoolean embeddedInsert() { return (flags_ & EMBEDDED_INSERT) != 0; }
  void setEmbeddedInsert(NABoolean v) { (v ? flags_ |= EMBEDDED_INSERT : flags_ &= ~EMBEDDED_INSERT); }

  NABoolean embeddedInsertSelect() { return (flags_ & EMBEDDED_INSERT_SELECT) != 0; }
  void setEmbeddedInsertSelect(NABoolean v) {
    (v ? flags_ |= EMBEDDED_INSERT_SELECT : flags_ &= ~EMBEDDED_INSERT_SELECT);
  }
  NABoolean computeRowsetRowsAffected() { return (flags_ & COMPUTE_ROWSET_ROWSAFFECTED) != 0; }
  void setComputeRowsetRowsAffected(NABoolean v) {
    (v ? flags_ |= COMPUTE_ROWSET_ROWSAFFECTED : flags_ &= ~COMPUTE_ROWSET_ROWSAFFECTED);
  }

  NABoolean needsReadWriteTransaction() { return (flags_ & NEEDS_READ_WRITE_TRANSACTION) != 0; }
  void setNeedsReadWriteTransaction(NABoolean v) {
    (v ? flags_ |= NEEDS_READ_WRITE_TRANSACTION : flags_ &= ~NEEDS_READ_WRITE_TRANSACTION);
  }

  NABoolean withNoRollbackUsed() { return (flags_ & WITH_NO_ROLLBACK_USED) != 0; }
  void setWithNoRollbackUsed(NABoolean v) { (v ? flags_ |= WITH_NO_ROLLBACK_USED : flags_ &= ~WITH_NO_ROLLBACK_USED); }

  NABoolean preCodeGenParallelOperator() { return (flags_ & PRECODEGEN_PARALLEL) != 0; }
  void setPreCodeGenParallelOperator(NABoolean v) {
    (v ? flags_ |= PRECODEGEN_PARALLEL : flags_ &= ~PRECODEGEN_PARALLEL);
  }

  NABoolean checkUnsyncdSort() { return (flags_ & CHECK_UNSYNC_SORT) != 0; }
  void setCheckUnsyncdSort(NABoolean v) { (v ? flags_ |= CHECK_UNSYNC_SORT : flags_ &= ~CHECK_UNSYNC_SORT); }

  NABoolean unsyncdSortFound() { return (flags_ & UNSYNC_SORT_FOUND) != 0; }
  void setUnsyncdSortFound(NABoolean v) { (v ? flags_ |= UNSYNC_SORT_FOUND : flags_ &= ~UNSYNC_SORT_FOUND); }

  NABoolean anySerialiableScan() { return (flags_ & ANY_SERIALIZABLE_SCAN) != 0; }
  void setAnySerialiableScan(NABoolean v) { (v ? flags_ |= ANY_SERIALIZABLE_SCAN : flags_ &= ~ANY_SERIALIZABLE_SCAN); }

  NABoolean aqrEnabled() { return (flags_ & AQR_ENABLED) != 0; }
  void setAqrEnabled(NABoolean v) { (v ? flags_ |= AQR_ENABLED : flags_ &= ~AQR_ENABLED); }

  NABoolean inMemoryObjectDefn() { return (flags_ & IN_MEMORY_OBJECT_DEFN) != 0; }
  void setInMemoryObjectDefn(NABoolean v) { (v ? flags_ |= IN_MEMORY_OBJECT_DEFN : flags_ &= ~IN_MEMORY_OBJECT_DEFN); }

  NABoolean cantReclaimQuery() { return (flags_ & CANT_RECLAIM_QUERY) != 0; }
  void setCantReclaimQuery(NABoolean v) { (v ? flags_ |= CANT_RECLAIM_QUERY : flags_ &= ~CANT_RECLAIM_QUERY); }

  NABoolean dp2XnsEnabled() { return (flags2_ & DP2_XNS_ENABLED) != 0; }
  void setDp2XnsEnabled(NABoolean v) { (v ? flags2_ |= DP2_XNS_ENABLED : flags2_ &= ~DP2_XNS_ENABLED); }

  NABoolean getAutoCollectRTStats() { return (flags2_ & AUTO_COLLECT_RTSTATS) != 0; };
  void setAutoCollectRTStats(NABoolean v) { (v ? flags2_ |= AUTO_COLLECT_RTSTATS : flags2_ &= ~AUTO_COLLECT_RTSTATS); }

  /*  NABoolean noTransformToSTI()
    { return (flags2_ & NO_TRANSFORM_TO_STI) != 0; }
  void setNoTransformToSTI(NABoolean v)
    { (v ? flags2_ |= NO_TRANSFORM_TO_STI : flags2_ &= ~NO_TRANSFORM_TO_STI); }
  */

  NABoolean enableTransformToSTI() { return (flags2_ & ENABLE_TRANSFORM_TO_STI) != 0; }
  void setEnableTransformToSTI(NABoolean v) {
    (v ? flags2_ |= ENABLE_TRANSFORM_TO_STI : flags2_ &= ~ENABLE_TRANSFORM_TO_STI);
  }

  NABoolean parquetInSupport() { return (flags2_ & PARQUET_IN_SUPPORT) != 0; }
  void setParquetInSupport(NABoolean v) { (v ? flags2_ |= PARQUET_IN_SUPPORT : flags2_ &= ~PARQUET_IN_SUPPORT); }

  NABoolean parquetUseCppReader() { return (flags2_ & PARQUET_USE_CPP_READER) != 0; }
  void setParquetUseCppReader(NABoolean v) {
    (v ? flags2_ |= PARQUET_USE_CPP_READER : flags2_ &= ~PARQUET_USE_CPP_READER);
  }

  NABoolean bloomFilterUseNativeByteOrder() { return (flags2_ & BLOOMFILTER_USE_NATIVE_BYTEORDER) != 0; }
  void setBloomFilterUseNativeByteOrder(NABoolean v) {
    (v ? flags2_ |= BLOOMFILTER_USE_NATIVE_BYTEORDER : flags2_ &= ~BLOOMFILTER_USE_NATIVE_BYTEORDER);
  }

  NABoolean hdfsAccess() { return (flags2_ & HDFS_ACCESS) != 0; }
  void setHdfsAccess(NABoolean v) { (v ? flags2_ |= HDFS_ACCESS : flags2_ &= ~HDFS_ACCESS); }

  NABoolean hiveReadAccess() { return (flags2_ & HIVE_READ_ACCESS) != 0; }
  void setHiveReadAccess(NABoolean v) { (v ? flags2_ |= HIVE_READ_ACCESS : flags2_ &= ~HIVE_READ_ACCESS); }

  NABoolean hiveWriteAccess() { return (flags2_ & HIVE_WRITE_ACCESS) != 0; }
  void setHiveWriteAccess(NABoolean v) { (v ? flags2_ |= HIVE_WRITE_ACCESS : flags2_ &= ~HIVE_WRITE_ACCESS); }

  NABoolean hiveAccess() { return hiveReadAccess() || hiveWriteAccess(); }

  NABoolean mayNotCancel() { return (flags2_ & MAY_NOT_CANCEL) != 0; }
  void setMayNotCancel(NABoolean v) { (v ? flags2_ |= MAY_NOT_CANCEL : flags2_ &= ~MAY_NOT_CANCEL); }

  NABoolean isPartnAccessChildIUD() { return (flags2_ & PA_OF_GENERIC_UPDATE) != 0; }
  void setPartnAccessChildIUD() { flags2_ |= PA_OF_GENERIC_UPDATE; }
  void clearPartnAccessChildIUD() { flags2_ &= ~PA_OF_GENERIC_UPDATE; }

  NABoolean isAqrWnrInsert() { return (flags2_ & AQR_WNR_INSERT_EMPTY) != 0; }
  void setAqrWnrInsert(NABoolean v) { v ? flags2_ |= AQR_WNR_INSERT_EMPTY : flags2_ &= ~AQR_WNR_INSERT_EMPTY; }

  NABoolean isRIinliningForTrafIUD() { return (flags2_ & RI_INLINING_FOR_TRAF_IUD) != 0; }
  void setRIinliningForTrafIUD(NABoolean v) {
    v ? flags2_ |= RI_INLINING_FOR_TRAF_IUD : flags2_ &= ~RI_INLINING_FOR_TRAF_IUD;
  }
  NABoolean isMinmaxOptWithRangeOfValues() { return (flags2_ & MINMAX_WITH_RANGE_OF_VALUES) != 0; }
  void setMinmaxOptWithRangeOfValues(NABoolean v) {
    v ? flags2_ |= MINMAX_WITH_RANGE_OF_VALUES : flags2_ &= ~MINMAX_WITH_RANGE_OF_VALUES;
  }

  NABoolean isEffTreeUpsert() { return (flags2_ & EFF_TREE_UPSERT) != 0; }

  void setEffTreeUpsert(NABoolean v) { v ? flags2_ |= EFF_TREE_UPSERT : flags2_ &= ~EFF_TREE_UPSERT; }

  NABoolean hasCompositeExpr() { return (flags2_ & HAS_COMPOSITE_EXPR) != 0; }
  void setHasCompositeExpr(NABoolean v) { v ? flags2_ |= HAS_COMPOSITE_EXPR : flags2_ &= ~HAS_COMPOSITE_EXPR; }

  NABoolean useRegionXN() { return (flags2_ & USE_REGION_XN) != 0; }
  void setUseRegionXN(NABoolean v) { v ? flags2_ |= USE_REGION_XN : flags2_ &= ~USE_REGION_XN; }

  NABoolean skipWriteMutation() { return (flags2_ & SKIP_WRITE_MUTATION_FOR_IB) != 0; }
  void setSkipWriteMutation(NABoolean v) {
    v ? flags2_ |= SKIP_WRITE_MUTATION_FOR_IB : flags2_ &= ~SKIP_WRITE_MUTATION_FOR_IB;
  }

  inline long getPlanId();
  inline int getExplainNodeId() const;
  inline int getNextExplainNodeId();
  inline const char *getStmtSource();
  inline CollIndex getExplainFragDirIndex() const;
  inline void setExplainFragDirIndex(CollIndex index);
  inline void setExplainTuple(ExplainTuple *explainTuple);
  inline ExplainTuple *getExplainTuple();

  TransMode *getTransMode();

  void nonvirtual_method_placeholder_TO_BE_REUSED__();

  // verify that the current transaction mode is suitable for
  // update, delete or insert operation. Should be SERIALIZABLE
  // and READ_WRITE.
  void verifyUpdatableTransMode(StmtLevelAccessOptions *sAxOpt, TransMode *tm, TransMode::IsolationLevel *ilForUp);

  // retrieve the current CmpContext
  inline CmpContext *currentCmpContext() { return currentCmpContext_; }

  // retrieve the current working heap, expected to be cleaned after
  // each statement compilation
  // It is not an inline function to avoid the change in CmpContext
  // trigger a recompilation. Should be implemented as inline in the
  // future.
  NAHeap *wHeap();

  inline void enableExplain() { explainIsDisabled_--; }

  inline void disableExplain() { explainIsDisabled_++; }

  inline int explainDisabled() const { return (explainIsDisabled_ > 0); }

  inline int getAffinityValueUsed() const { return affinityValueUsed_; };
  inline void setAffinityValueUsed(int affVal) { affinityValueUsed_ = affVal; };

  inline NABoolean computeStats() const { return computeStats_; }
  inline void setComputeStats(NABoolean v) { computeStats_ = v; }

  // If oltMsgOpt or OltEidOpt or OltEidLeanOpt is set
  // change the default collection type to PERTABLE_STATS
  // It may not be possible to ship operator stats info
  // from DP2 to PA node when the above olt options are set
  ComTdb::CollectStatsType collectStatsType() {
    if (computeStats_) {
      if (collectStatsType_ == ComTdb::OPERATOR_STATS && oltOptInfo_.disableOperStats())
        return ComTdb::PERTABLE_STATS;
      else
        return collectStatsType_;
    } else
      return ComTdb::NO_STATS;
  }

  NABoolean explainInRms() {
    if (collectStatsType() == ComTdb::PERTABLE_STATS) explainInRms_ = FALSE;
    return explainInRms_;
  }

  inline NABoolean collectRtsStats() const { return collectRtsStats_; }

  NABoolean isStaticCompMode() const { return staticCompMode_; }
  NABoolean isDynamicCompMode() const { return !staticCompMode_; }

  inline const ValueIdSet &getInternalInputs() const { return internalInputs_; }
  inline void addInternalInput(ValueId vid) { internalInputs_ += vid; }

  void setInternalRefreshStatement() { isInternalRefreshStatement_ = TRUE; }
  NABoolean isInternalRefreshStatement() const { return isInternalRefreshStatement_; }

  OltOptInfo *oltOptInfo() { return &oltOptInfo_; }

  NABoolean orderRequired() { return orderRequired_; }

  NABoolean foundAnUpdate() const { return foundAnUpdate_; }

  void setFoundAnUpdate(NABoolean u) { foundAnUpdate_ = u; }

  NABoolean isNonCacheablePlan() const { return nonCacheableMVQRplan_ || nonCacheableCSEPlan_; }
  NABoolean isNonCacheableMVQRplan() const { return nonCacheableMVQRplan_; }
  NABoolean isNonCacheableCSEPlan() const { return nonCacheableCSEPlan_; }

  void setNonCacheableMVQRplan(NABoolean n) { nonCacheableMVQRplan_ = n; }
  void setNonCacheableCSEPlan(NABoolean n) { nonCacheableCSEPlan_ = n; }

  NABoolean updateWithinCS() const { return updateWithinCS_; }

  void setUpdateWithinCS(NABoolean u) { updateWithinCS_ = u; }
  ComTdbRoot *getTopRoot();
  const Space *getTopSpace() const;

  static int getRecordLength(ComTdbVirtTableIndexInfo *indexInfo, ComTdbVirtTableColumnInfo *columnInfoArray);

  static TrafDesc *createColDescs(const char *tableName, ComTdbVirtTableColumnInfo *columnInfo, Int16 numCols,
                                  UInt32 &offset, NAMemory *space);

  static TrafDesc *createKeyDescs(int numKeys, const ComTdbVirtTableKeyInfo *keyInfo, NAMemory *space);

  static TrafDesc *createConstrKeyColsDescs(int numKeys, ComTdbVirtTableKeyInfo *keyInfo,
                                            ComTdbVirtTableColumnInfo *colInfo, NAMemory *space);

  static TrafDesc *createRefConstrDescStructs(int numConstrs, ComTdbVirtTableRefConstraints *refConstrs,
                                              NAMemory *space);

  static TrafDesc *createPrivDescs(const ComTdbVirtTablePrivInfo *privs, NAMemory *space);

  static TrafDesc *createVirtualTableDesc(
      const char *tableName,
      NAMemory *heap,  // in case caller wants a particular heap; if NULL is passed, we decide
      int numCols, ComTdbVirtTableColumnInfo *columnInfo, int numKeys, ComTdbVirtTableKeyInfo *keyInfo,
      int numConstrs = 0, ComTdbVirtTableConstraintInfo *constrInfo = NULL, int numIndexes = 0,
      ComTdbVirtTableIndexInfo *indexInfo = NULL, int numViews = 0, ComTdbVirtTableViewInfo *viewInfo = NULL,
      ComTdbVirtTableTableInfo *tableInfo = NULL, ComTdbVirtTableSequenceInfo *seqInfo = NULL,
      ComTdbVirtTableStatsInfo *statsInfo = NULL, NAArray<HbaseStr> *endKeyArray = NULL,
      NABoolean genPackedDesc = FALSE, int *packedDescLen = NULL, NABoolean isSharedSchema = FALSE,
      ComTdbVirtTablePrivInfo *privInfo = NULL, const char *nameSpace = NULL,
      ComTdbVirtTablePartitionV2Info *partitionV2Info = NULL);

  static TrafDesc *assembleDescs(NAArray<HbaseStr> *keyArray, NAMemory *space);

  static TrafDesc *createVirtualRoutineDesc(const char *routineName, ComTdbVirtTableRoutineInfo *routineInfo,
                                            int numParams, ComTdbVirtTableColumnInfo *paramsArray,
                                            ComTdbVirtTablePrivInfo *privInfo, Space *space);
  static TrafDesc *createVirtualLibraryDesc(const char *libraryName, ComTdbVirtTableLibraryInfo *libraryInfo,
                                            Space *space);
  static short genAndEvalExpr(CmpContext *cmpContext, char *exprStr, int numChildren, ItemExpr *childNode0,
                              ItemExpr *childNode1, ComDiagsArea *diagsArea);

  inline void incrBMOsMemory(CostScalar x) { totalBMOsMemoryPerNode_ += x; }

  inline CostScalar getTotalBMOsMemoryPerNode() { return totalBMOsMemoryPerNode_; }
  inline void incrNumBMOs() {
    incrNumBMOsPerFrag(1);
    totalNumBMOs_++;
  }

  inline void incrNumBMOsPerFrag(UInt32 x) { numBMOs_ += x; }

  inline unsigned short replaceNumBMOs(unsigned short newVal) {
    unsigned short retVal = numBMOs_;
    numBMOs_ = newVal;
    return retVal;
  }

  inline CostScalar getTotalNBMOsMemoryPerNode() { return nBMOsMemoryPerNode_; }
  inline void incrNBMOsMemoryPerNode(CostScalar x) { nBMOsMemoryPerNode_ += x; }

  inline void setBMOsMemoryLimitPerNode(CostScalar x) { BMOsMemoryLimitPerNode_ = x; }

  inline CostScalar getBMOsMemoryLimitPerNode() { return BMOsMemoryLimitPerNode_; }

  inline unsigned short getTotalNumBMOs() { return totalNumBMOs_; }

  void incTotalESPs() { totalNumESPs_++; }
  int getTotalESPs(void) { return totalNumESPs_; }

  void setNumESPs(int numEsps) { numESPs_ = numEsps; }
  int getNumESPs(void) { return numESPs_; }

  int getEspLevel() { return espLevel_; }
  void incrEspLevel() { espLevel_++; }
  void decrEspLevel() { espLevel_--; }

  void setHalloweenProtection(HalloweenProtectionType h) { halloweenProtection_ = h; }

  HalloweenProtectionType getHalloweenProtection() { return halloweenProtection_; }

  LIST(DP2Insert *) & getInsertNodesList() { return insertNodesList_; }

  // These next 5 methods are to use to toggle the internal data format
  // between SQLARK_EXPLODED_FORMAT and SQLMX_ALIGNED_FORMAT.
  void setCompressedInternalFormat() { internalDataFormat_ = (Int16)ExpTupleDesc::SQLMX_ALIGNED_FORMAT; }

  void setExplodedInternalFormat() { internalDataFormat_ = (Int16)ExpTupleDesc::SQLARK_EXPLODED_FORMAT; }

  void setInternalFormat() {
    if (CmpCommon::getDefault(COMPRESSED_INTERNAL_FORMAT) == DF_ON)
      setCompressedInternalFormat();
    else
      setExplodedInternalFormat();
  }

  ExpTupleDesc::TupleDataFormat getInternalFormat() { return (ExpTupleDesc::TupleDataFormat)internalDataFormat_; }

  NABoolean isCompressedInternalFormat() { return (internalDataFormat_ == ExpTupleDesc::SQLMX_ALIGNED_FORMAT); }

  //
  // Return the ExpTupleDesc enum value that corresponds to the row
  // format in the NATable object passed in.
  ExpTupleDesc::TupleDataFormat getTableDataFormat(const NATable *tbl, const IndexDesc *indexDesc = NULL) {
    ExpTupleDesc::TupleDataFormat tdf = ExpTupleDesc::UNINITIALIZED_FORMAT;
    NABoolean isAlignedFormat = tbl->isAlignedFormat(indexDesc);
    if (isAlignedFormat)
      tdf = ExpTupleDesc::SQLMX_ALIGNED_FORMAT;
    else
      tdf = ExpTupleDesc::SQLMX_FORMAT;

    return tdf;
  }

  // For an embedded insert select statement the originating
  ExpTupleDesc::TupleDataFormat getDataFormat(const NATable *tbl, ExpTupleDesc::TupleDataFormat tdf) {
    if (embeddedInsertSelect())
      return ExpTupleDesc::SQLMX_ALIGNED_FORMAT;
    else
      return tdf;
  }

  CompilerStatsInfo &compilerStatsInfo() { return compilerStatsInfo_; }

  // Helper method used by caching operators to ensure a statement
  // execution count is included in their characteristic input.
  // The "getOrAdd" semantic is to create the ValueId if it doesn't
  // exist, otherwise return a preexisting one.
  ValueId getOrAddStatementExecutionCount();

  PhysicalProperty *genPartitionedPhysProperty(const IndexDesc *clusIndex);

  // Part of the fix for Soln 10-071204-9253 -- see comments in
  // Generator.cpp.
  RelExpr *insertEspExchange(RelExpr *oper, const PhysicalProperty *unPreCodeGendPP);

  // Helpers for the special ONLJ queue sizing defaults.
  NABoolean const getMakeOnljLeftQueuesBig() { return makeOnljLeftQueuesBig_; }
  void setMakeOnljLeftQueuesBig(NABoolean x) { makeOnljLeftQueuesBig_ = x; }
  int const getOnljLeftUpQueue() { return onljLeftUpQueue_; }
  int const getOnljLeftDownQueue() { return onljLeftDownQueue_; }

  int getLargeQueueSize(void) const { return largeQueueSize_; }
  void setLargeQueueSize(int size) { largeQueueSize_ = size; }

  // these are used in partial sort so that a split-top node knows
  // to maintain required order sought by a sort operator above
  void setPrefixSortKey(ValueIdList &prefixSort) { prefixSortKey_ = prefixSort; }
  ValueIdList &getPrefixSortKey() { return prefixSortKey_; }
  void clearPrefixSortKey() { prefixSortKey_.clear(); }

  inline double getTotalEstimatedMemory() { return totalEstimatedMemory_; }
  inline void addToTotalEstimatedMemory(double val) { totalEstimatedMemory_ += val; }

  inline short getMaxCpuUsage() { return maxCpuUsage_; }
  inline void setMaxCpuUsage(short val) { maxCpuUsage_ = val; }

  inline ComTdb::OverflowModeType getOverflowMode() { return overflowMode_; }

  // Each of these two mutators return the old value
  // so that the caller can restore it when finished
  // with its method (i.e., the new value applies to
  // the operator's child(ren).
  bool setPrecodeHalloweenLHSofTSJ(bool newVal);
  bool setPrecodeRHSofNJ(bool newVal);

  bool getPrecodeHalloweenLHSofTSJ() const { return precodeHalloweenLHSofTSJ_; };
  bool getPrecodeRHSofNJ() const { return precodeRHSofNJ_; };
  bool getR251HalloweenPrecode() const { return r251HalloweenPrecode_; }

  void incUnblockedHalloweenScans() { unblockedHalloweenScans_++; };
  void setUnblockedHalloweenScans(int u) { unblockedHalloweenScans_ = u; };
  int getUnblockedHalloweenScans() const { return unblockedHalloweenScans_; };

  void setHalloweenSortForced() { halloweenSortForced_ = true; };
  bool getHalloweenSortForced() const { return halloweenSortForced_; }

  void setHalloweenESPonLHS(bool h) { halloweenESPonLHS_ = h; }
  bool getHalloweenESPonLHS() const { return halloweenESPonLHS_; }

  // Accessors to lists used by the HashJoin min/max optimizations.

  const ValueIdList &getMinVals() const { return minVals_; }
  const ValueIdList &getMaxVals() const { return maxVals_; }
  const ValueIdList &getRangeOfValueVals() const { return rangeOfValuesVals_; }

  CollIndex findIndexOfMinValForScan(ValueId x) const { return minVals_.index(x); }

  ValueId getMinVal(CollIndex i) const { return minVals_[i]; }
  ValueId getMaxVal(CollIndex i) const { return maxVals_[i]; }
  ValueId getRangeOfValueVal(CollIndex i) const { return rangeOfValuesVals_[i]; }

  const CostScalar getChild1Uec(CollIndex i) const { return innerChildUecs_[i]; }
  const CostScalar getRowsProducedByHJ(CollIndex i) const { return rowsProducedByHJ_[i]; }

  const NABoolean isChild1Broadcasting(CollIndex i) const { return innerIsBroadcastPartitioned_[i]; }

  ValueId getMinMaxKey(CollIndex i) const { return outerMinMaxKeys_[i]; }
  ValueId getHJInnerMinMaxKey(CollIndex i) const { return innerMinMaxKeys_[i]; }
  const NABitVector &getEnabledMinMaxKeys() const { return minMaxEnabled_; }

  RelExpr *getCaller(CollIndex i) const { return callers_[i]; }

  CollIndex getNumMinMaxVals() const { return outerMinMaxKeys_.entries(); }

  // hash joins create, enable/disable and check for usage
  void addMinMaxVals(const ValueId &outerVal, const ValueId &innerVal, const ValueId &minVal, const ValueId &maxVal,
                     const ValueId &rangeOfValuesVal, CostScalar innerChildUec, NABoolean isNonPartitioned,
                     NABoolean child1IsBroadcastPartitioned, RelExpr *, /* the expression (HJ) makes the call*/
                     CostScalar rowsProducedByHJ);
  void saveAndMapMinMaxKeys(ValueIdMap &map, ValueIdList &savedValues /*OUT*/);
  void restoreMinMaxKeys(const ValueIdList &savedValues /*IN*/);
  void disableMinMaxKey(CollIndex i) { minMaxEnabled_ -= i; }

  // scans (and possibly other nodes) make use of the keys
  void setMinMaxKeyToUsed(CollIndex i) { minMaxUsed_ += i; }
  void setMinMaxKeyToNotUsed(CollIndex i) { minMaxUsed_ -= i; }
  NABoolean isMinMaxKeyUsed(CollIndex i) const { return minMaxUsed_.contains(i); }

  CollIndex getStartMinMaxIndex() const { return startMinMaxIndex_; }
  void setStartMinMaxIndex(CollIndex i) { startMinMaxIndex_ = i; }
  CollIndex getEndMinMaxIndex() const { return endMinMaxIndex_; }
  void setEndMinMaxIndex(CollIndex i) { endMinMaxIndex_ = i; }

  Int16 getNumTrafReplicas() const { return numTrafReplicas_; }
  void setNumTrafReplicas(Int16 n) { numTrafReplicas_ = n; }

  void setOuterRowsToSurvive(CollIndex i, CostScalar &rc) { outerRowsToSurvive_[i] = rc; }

  CostScalar findRowsToSurvive(const ValueId &outerColValId);

  void setCurrentEspFragmentPCG(pcgEspFragment *pcgEsp) { currentEspFragmentPCG_ = pcgEsp; };

  pcgEspFragment *getCurrentEspFragmentPCG() { return currentEspFragmentPCG_; };

  CorrName &utilInsertTable() { return utilInsertTable_; }

  NExDbgInfo *getNExDbgInfoAddr() { return &(NExDbgInfoObj_); }
  void setPlanExpirationTimestamp(long t);
  long getPlanExpirationTimestamp() { return planExpirationTimestamp_; }

  ItemExpr *addCompDecodeForDerialization(ItemExpr *ie, NABoolean isAlignedFormat);

  void setHBaseNumCacheRows(double rowsAccessed, ComTdbHbaseAccess::HbasePerfAttributes *hbpa, int hbaseRowSize,
                            Float32 samplePercent = 0.0, NABoolean isNeedPushDownLimit = FALSE);
  void setHBaseCacheBlocks(int hbaseRowSize, double rowsAccessed, ComTdbHbaseAccess::HbasePerfAttributes *hbpa);
  void setHBaseSmallScanner(int hbaseRowSize, double rowsAccessed, int hbaseBlockSize,
                            ComTdbHbaseAccess::HbasePerfAttributes *hbpa);
  void setHBaseParallelScanner(ComTdbHbaseAccess::HbasePerfAttributes *hbpa);

  NASet<long> &objectUids() { return objectUids_; }

  NASet<NAString> &objectNames() { return objectNames_; }

  char *getSnapshotScanTmpLocation() {
    if (snapshotScanTmpLocation_ == NULL) {
      NAString tmpLoc = NAString(ActiveSchemaDB()->getDefaults().getValue(TRAF_TABLE_SNAPSHOT_SCAN_TMP_LOCATION));
      CMPASSERT(tmpLoc[tmpLoc.length() - 1] == '/');
      char str[30];
      time_t t;
      time(&t);
      struct tm *curgmtime = gmtime(&t);
      strftime(str, 30, "%Y%m%d%H%M%S", curgmtime);
      char str2[60];
      srand(getpid());
      sprintf(str2, "%s_%d", str, rand() % 1000);
      tmpLoc.append(str);
      tmpLoc.append("/");
      snapshotScanTmpLocation_ = new (wHeap()) char[tmpLoc.length() + 1];
      strcpy(snapshotScanTmpLocation_, tmpLoc.data());
    }
    return snapshotScanTmpLocation_;
  }

  void setQueryHash(UInt64 x) { queryHash_ = x; }
  UInt64 getQueryHash() { return queryHash_; }
  inline void setTopNRows(int topNRows) { topNRows_ = topNRows; }
  inline int getTopNRows() { return topNRows_; }
  inline XBMOQuotaMap *getBMOQuotaMap() { return &bmoQuotaMap_; }
  double getEstMemPerNode(NAString *key, int &numStreams);
  double getEstMemForTdb(NAString *key);
  double getEstMemPerInst(NAString *key);
  void finetuneBMOEstimates();
};  // class Generator

class GenOperSimilarityInfo : public NABasicObject {
 public:
  GenOperSimilarityInfo(const IndexDesc *indexDesc, const OperatorTypeEnum op)
      : indexDesc_(indexDesc), op_(op), flags_(0) {
    constraintsTrigPresent_ = FALSE;
  }

  const IndexDesc *getIndexDesc() { return indexDesc_; };
  const OperatorTypeEnum getOper() { return op_; };
  NABoolean &constraintsTrigPresent() { return constraintsTrigPresent_; };

  NABoolean noPartitionSimCheck() { return ((flags_ & NO_PARTITION_SIM_CHECK) != 0); };
  void setNoPartitionSimCheck(NABoolean v) {
    (v ? flags_ |= NO_PARTITION_SIM_CHECK : flags_ &= ~NO_PARTITION_SIM_CHECK);
  };

 private:
  enum Flags {
    // if set, indicates that partition sim check always passes even if
    // the num of partitions are different. Used in cases where we know
    // that the change in num of partns will not change the plan, for ex,
    // if OLT opt is being used in which case only one partn will be
    // accessed at runtime.
    NO_PARTITION_SIM_CHECK = 0x0001
  };

  const OperatorTypeEnum op_;
  const IndexDesc *indexDesc_;
  NABoolean constraintsTrigPresent_;

  UInt32 flags_;
};

class OperBMOQuota : public NABasicObject {
 public:
  OperBMOQuota(NAString *operAddr, int numStreams, CostScalar estMemPerNode, CostScalar estMemPerInst,
               CostScalar estRowsUsed, CostScalar maxCard)
      : operAddr_(operAddr),
        numStreams_(numStreams),
        estMemPerNode_(estMemPerNode),
        estMemPerInst_(estMemPerInst),
        estRowsUsed_(estRowsUsed),
        maxCard_(maxCard),
        ignoreEstimate_(FALSE),
        origEstMemPerNode_(estMemPerNode) {
    // weight_ = (estRowsUsed_ / maxCard_).value();
    weight_ = 0;
  }
  const NAString *getKey() const { return operAddr_; }
  inline int getNumStreams() { return numStreams_; }
  inline double getEstMemPerNode() { return estMemPerNode_.value(); }
  inline double getEstMemPerInst() { return estMemPerInst_.value(); }
  inline double getEstMemForTdb() { return estMemPerInst_.value() * numStreams_; }
  inline void setIgnoreEstimate() { ignoreEstimate_ = TRUE; }
  NABoolean operator==(const OperBMOQuota &other) const { return this == &other; }
  inline void setEstMemPerNode(double estMemPerNode) { estMemPerNode_ = estMemPerNode; }

 private:
  const NAString *operAddr_;
  int numStreams_;
  CostScalar estMemPerNode_;
  CostScalar estMemPerInst_;
  CostScalar estRowsUsed_;
  CostScalar maxCard_;
  CostScalar origEstMemPerNode_;
  double weight_;
  NABoolean ignoreEstimate_;
};

// Get table and index filename
extern const NAString GenGetQualifiedName(const CorrName &, NABoolean formatForDisplay = FALSE,
                                          NABoolean asAnsiString = FALSE);

inline long Generator::getPlanId() { return planId_; }

inline int Generator::getExplainNodeId() const { return explainNodeId_; }

inline int Generator::getNextExplainNodeId() { return ++explainNodeId_; }

inline const char *Generator::getStmtSource() { return stmtSource_; }

inline CollIndex Generator::getExplainFragDirIndex() const { return explainFragDirIndex_; }

inline void Generator::setExplainFragDirIndex(CollIndex index) { explainFragDirIndex_ = index; }

inline void Generator::setExplainTuple(ExplainTuple *explainTuple) { explainTuple_ = explainTuple; }

inline ExplainTuple *Generator::getExplainTuple() { return explainTuple_; }

#endif
