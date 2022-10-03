

#ifndef COMTDB_H
#define COMTDB_H

#include "common/Int64.h"              // for Int64
#include "export/NAVersionedObject.h"  // for NAVersionedObject
#include "common/BaseTypes.h"          // for Cardinality
#include "exp/ExpCriDesc.h"
#include "exp/exp_expr.h"  // subclasses of TDB contain expressions
#include "cli/sqlcli.h"
#include "common/ComSmallDefs.h"
#include "sqlcomp/PrivMgrDesc.h"  // Privilege descriptors
#include "sqlcat/TrafDDLdesc.h"
#include "exp/ExpLOBenums.h"

// -----------------------------------------------------------------------
// Classes defined in this file
// -----------------------------------------------------------------------
class ComTdb;

// -----------------------------------------------------------------------
// Classes referenced in this file
// -----------------------------------------------------------------------
class ex_tcb;
class ex_expr;
class ex_globals;

struct ex_eye_catcher {
  char name_for_sun_compiler[4];
};

#include "comexe/QueueIndex.h"

// -----------------------------------------------------------------------
// copy only 4 chars and not the '\0'
// -----------------------------------------------------------------------
#define eye_CS                    "CS  "
#define eye_CONTROL_QUERY         "CQRY"
#define eye_CONNECT_BY            "CTBY"
#define eye_CONNECT_BY_TEMP_TABLE "CBTT"
#define eye_DDL                   "DDL "
#define eye_DESCRIBE              "DESC"
#define eye_FASTSORT              "FSRT"
#define eye_FIRST_N               "FSTN"
#define eye_FREE                  "FREE"
#define eye_ROOT                  "ROOT"
#define eye_LOCK                  "LOCK"
#define eye_MATER                 "MATR"
#define eye_ONLJ                  "ONLJ"
#define eye_UNLJ                  "UNLJ"
#define eye_MJ                    "MJ  "
#define eye_SCAN                  "SCAN"
#define eye_HASHJ                 "HSHJ"
#define eye_HASH_GRBY             "HGBY"
#define eye_INSERT                "INS "
#define eye_PACKROWS              "PACK"
#define eye_PROBE_CACHE           "PRBC"
#define eye_RECURSE               "RCRS"
#define eye_SAMPLE                "SAMP"
#define eye_SEQUENCE_FUNCTION     "SEQF"
#define eye_SET_TIMEOUT           "TOUT"
#define eye_SORT                  "SORT"
#define eye_SORT_GRBY             "SGBY"
#define eye_STATS                 "STAT"
#define eye_TRANSACTION           "TRAN"
#define eye_TRANSPOSE             "TRSP"
#define eye_UNION                 "UN  "
#define eye_UNPACKROWS            "UNPK"
#define eye_UPDATE                "UPD "
#define eye_TUPLE                 "TUPL"
#define eye_TUPLE_FLOW            "TFLO"
#define eye_SPLIT_TOP             "SPLT"
#define eye_SPLIT_BOTTOM          "SPLB"
#define eye_STORED_PROC           "STPR"
#define eye_EID_ROOT              "EIDR"
#define eye_PARTN_ACCESS          "PA  "
#define eye_SEND_TOP              "SNDT"
#define eye_SEND_BOTTOM           "SNDB"
#define eye_EXPLAIN               "XPLN"
#define eye_IM_DATA               "IMDT"
#define eye_VPJOIN                "VPJN"
#define eye_UDR                   "UDR "
#define eye_CANCEL                "CNCL"
#define eye_FAST_EXTRACT          "EXTR"
#define eye_HDFS_SCAN             "HDFS"
#define eye_EXT_STORAGE_SCAN      "EXTS"
#define eye_HIVE_MD               "HVMD"
#define eye_HBASE_ACCESS          "HBSA"
#define eye_HBASE_COPROC_AGGR     "HBCA"
#define eye_AQR_WNR_INS           "UAWI"
#define eye_EXT_STORAGE_AGGR      "EXTA"
#define eye_QUERY_INVALIDATION    "QI  "

// -----------------------------------------------------------------------
// Template instantiation to produce a 64-bit pointer emulator class
// for ComTdb
// -----------------------------------------------------------------------
typedef NAVersionedObjectPtrTempl<ComTdb> ComTdbPtr;

// -----------------------------------------------------------------------
// Template instantiation to produce a 64-bit pointer emulator class
// for ComTdbPtr
// -----------------------------------------------------------------------
typedef NAVersionedObjectPtrArrayTempl<ComTdbPtr> ComTdbPtrPtr;

class ComTdbParams {
 public:
  ComTdbParams(Cardinality estimatedRowCount, ex_cri_desc *criDown, ex_cri_desc *criUp, queue_index sizeDown,
               queue_index sizeUp, Int32 numBuffers, UInt32 bufferSize, Int32 firstNRows)
      : estimatedRowCount_(estimatedRowCount),
        criDown_(criDown),
        criUp_(criUp),
        sizeDown_(sizeDown),
        sizeUp_(sizeUp),
        numBuffers_(numBuffers),
        bufferSize_(bufferSize),
        firstNRows_(firstNRows){};

  void getValues(Cardinality &estimatedRowCount, ExCriDescPtr &criDown, ExCriDescPtr &criUp, queue_index &sizeDown,
                 queue_index &sizeUp, Int32 &numBuffers, UInt32 &bufferSize, Int32 &firstNRows);

 private:
  Cardinality estimatedRowCount_;
  ex_cri_desc *criDown_;
  ex_cri_desc *criUp_;
  queue_index sizeDown_;
  queue_index sizeUp_;
  Int32 numBuffers_;
  UInt32 bufferSize_;
  Int32 firstNRows_;
  char filler_[40];
};

// -----------------------------------------------------------------------
// ComTdb
// -----------------------------------------------------------------------
class ComTdb : public NAVersionedObject {
  // ---------------------------------------------------------------------
  // Allow access to protected part of tdb from tcb
  // ---------------------------------------------------------------------
  friend class ex_tcb;

 public:
  // ---------------------------------------------------------------------
  // ex_node_type is used in both TCBs and TDBs.
  // Don't ever change the assigned numeric values of a node type when
  // a new one is added. Always add new types as the end of the list.
  // ---------------------------------------------------------------------
  enum ex_node_type {
    ex_TDB = -1,  // Object of this base class ComTdb.

    ex_CONTROL_QUERY = 1,  // control query statements

    ex_DDL = 3,       // process DDL statements
    ex_DESCRIBE = 4,  // describe DDL statements
    ex_STATS = 5,     // return statistics

    ex_ROOT = 10,  // communicates with CLI
    ex_ONLJ = 11,  // vanilla NLJ -- Records returned in order
    ex_MJ = 12,    // merge join
    ex_FASTSORT = 13,
    ex_HASHJ = 14,  // hash join

    ex_HASH_GRBY = 50,  // hash groupby
    ex_LOCK = 51,       // Lock/Unlock Table/View

    ex_SORT = 55,         // sort
    ex_SORT_GRBY = 56,    // sort groupby
    ex_STORED_PROC = 57,  // stored procedure
    ex_TRANSACTION = 58,  // transaction
    ex_TRANSPOSE = 59,    // transpose

    ex_UNION = 70,  // merge union

    ex_TUPLE = 74,  // Base class for all tuple operators
    ex_LEAF_TUPLE = 75,
    ex_NON_LEAF_TUPLE = 76,
    ex_FIRST_N = 77,

    ex_TUPLE_FLOW = 80,    // moves rows from left child to right
    ex_SPLIT_TOP = 81,     // top node in a connection master-ESP/ESP-ESP
    ex_SPLIT_BOTTOM = 82,  // bottom node in such a connection
    ex_PARTN_ACCESS = 83,
    ex_SEND_TOP = 84,     // top node in a connection master-ESP/ESP-ESP
    ex_SEND_BOTTOM = 85,  // bottom node in such a connection
    ex_EXPLAIN = 86,      // explain the plan generated
    ex_VPJOIN = 87,       // join scans of vertical partition together
    ex_UNPACKROWS = 88,   // unpack rows retrieved from a packed table
    ex_PACKROWS = 89,     // pack rows to insert into a packed table

    ex_SAMPLE = 91,
    ex_SEQUENCE_FUNCTION = 92,
    ex_COMPOUND_STMT = 93,

    ex_SET_TIMEOUT = 94,
    ex_UDR = 100,       // User-defined routines
    ex_EXE_UTIL = 101,  // general exe util tdb, used to implement
                        // a variety of requests. See ComTdbExeUtil.
    ex_REORG = 102,
    ex_DISPLAY_EXPLAIN = 103,
    ex_MAINTAIN_OBJECT = 104,
    ex_PROCESS_VOLATILE_TABLE = 107,
    ex_LOAD_VOLATILE_TABLE = 108,
    ex_CLEANUP_VOLATILE_TABLES = 109,
    ex_GET_VOLATILE_INFO = 110,
    ex_CREATE_TABLE_AS = 111,
    ex_GET_STATISTICS = 113,
    ex_PROBE_CACHE = 114,
    ex_LONG_RUNNING = 116,
    ex_GET_METADATA_INFO = 117,
    ex_GET_VERSION_INFO = 118,
    ex_SUSPEND_ACTIVATE = 121,
    ex_DISK_LABEL_STATISTICS = 122,
    ex_REGION_STATS = 123,
    ex_GET_FORMATTED_DISK_STATS = 124,
    ex_SHOW_SET = 125,
    ex_AQR = 126,
    ex_CANCEL = 127,
    ex_DISPLAY_EXPLAIN_COMPLEX = 128,
    ex_PROCESS_INMEMORY_TABLE = 129,
    ex_GET_UID = 130,
    ex_POP_IN_MEM_STATS = 131,
    ex_REPLICATOR = 133,
    ex_GET_ERROR_INFO = 136,
    ex_GET_REORG_STATISTICS = 140,
    ex_HBASE_ACCESS = 143,
    ex_HBASE_COPROC_AGGR = 144,
    ex_PROCESS_STATISTICS = 145,
    ex_ARQ_WNR_INSERT = 146,
    ex_METADATA_UPGRADE = 147,
    ex_HBASE_LOAD = 148,
    ex_DDL_WITH_STATUS = 152,
    ex_GET_QID = 153,
    ex_BACKUP_RESTORE = 154,
    ex_CONNECT_BY = 161,
    ex_COMPOSITE_UNNEST = 162,
    ex_CONNECT_BY_TEMP_TABLE = 163,
    ex_GET_OBJECT_EPOCH_STATS = 164,
    ex_GET_OBJECT_LOCK_STATS = 165,
    ex_QUERY_INVALIDATION = 166,
    ex_SNAPSHOT_UPDATE_DELETE = 167,
    ex_LAST = 9999  // not used
  };

  // ---------------------------------------------------------------------
  // Constructor/Destructor.
  // If params is not NULL, then initialize variable from ComTdbParams
  // object passed in.
  // ---------------------------------------------------------------------
  ComTdb(ex_node_type type, const char *eye, Cardinality estRowsUsed = 0.0, ex_cri_desc *criDown = NULL,
         ex_cri_desc *criUp = NULL, queue_index sizeDown = 0, queue_index sizeUp = 0, Int32 numBuffers = 0,
         UInt32 bufferSize = 0, Lng32 uniqueId = 0, ULng32 initialQueueSizeDown = 4, ULng32 initialQueueSizeUp = 4,
         short queueResizeLimit = 9, short queueResizeFactor = 4, ComTdbParams *params = NULL);

  // ---------------------------------------------------------------------
  // Dummy constructor used in findVTblPtr()
  // ---------------------------------------------------------------------
  ComTdb();

  virtual ~ComTdb();

  // ---------------------------------------------------------------------
  // Pack a TDB before storing it on disk or transporting it to another
  // process. Unpack a TDB after it's retrieved from disk or receive from
  // another process.
  // ---------------------------------------------------------------------
  virtual Long pack(void *space);
  virtual Lng32 unpack(void *base, void *reallocator);

  // ---------------------------------------------------------------------
  // Fix up the virtual function table pointer of a TDB retrieved from
  // disk or in a message according to classID_ field. fixupVTblPtrCom()
  // fixes up the result to a Compiler version of TDB. fixupVTblPtrExe()
  // fixes up the result to an Executor version of TDB which redefines
  // project.
  // ---------------------------------------------------------------------
  void fixupVTblPtr();
  void fixupVTblPtrCom();
  void fixupVTblPtrExe();

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual char *findVTblPtr(short classID);
  virtual unsigned char getClassVersionID() { return 1; }
  virtual void populateImageVersionIDArray() { setImageVersionID(0, getClassVersionID()); }
  virtual short getClassSize() { return (short)sizeof(ComTdb); }

  // ---------------------------------------------------------------------
  // There are two versions of findVTblPtr(classID). The version which is
  // linked into the executor (or tdm_sqlcli.dll) calls findVTblPtrExe()
  // which the one linked into the compiler (or tdm_arkcmp.exe) calls
  // findVTblPtrCom(). The former returns the VtblPtr of an executor TDB
  // which has the build() method defined, while the latter returns the
  // one of a "compiler" TDB with no build() method.
  // ---------------------------------------------------------------------
  char *findVTblPtrExe(short classID);
  char *findVTblPtrCom(short classID);

  // ---------------------------------------------------------------------
  // Whether the TDB supports out-of-order replies to its requests.
  // ---------------------------------------------------------------------
  virtual Int32 orderedQueueProtocol() const { return 1; }

  // ---------------------------------------------------------------------
  // Used by the internal SHOWPLAN command to get attributes of a TDB.
  // ---------------------------------------------------------------------
  virtual void displayContents(Space *space, ULng32 flag);
  virtual void displayExpression(Space *space, ULng32 flag);
  virtual void displayChildren(Space *space, ULng32 flag);

  // ---------------------------------------------------------------------
  // Accessors/Mutators.
  // ---------------------------------------------------------------------
  inline ex_node_type getNodeType() const { return (ComTdb::ex_node_type)getClassID(); }
  inline void setNodeType(ex_node_type t) { setClassID(t); }
  inline void setEyeCatcher(const char *eye) { str_cpy_all((char *)&eyeCatcher_, eye, 4); }

  inline ComTdb *getParentTdb() { return parentTdb_; }
  inline void setParentTdb(ComTdb *tdb) { parentTdb_ = tdb; }
  inline void setTdbId(Lng32 uniqueId) { tdbId_ = uniqueId; }
  inline Lng32 getTdbId() const { return tdbId_; }
  inline Cardinality getEstRowsAccessed() const { return estRowsAccessed_; }

  inline void setEstRowsAccessed(Cardinality estRowsAccessed) { estRowsAccessed_ = estRowsAccessed; }

  inline Cardinality getEstRowsUsed() const { return estRowsUsed_; }

  inline void setEstRowsUsed(Cardinality estRowsUsed) { estRowsUsed_ = estRowsUsed; }

  inline ULng32 getMaxQueueSizeDown() const { return queueSizeDown_; }
  inline ULng32 getMaxQueueSizeUp() const { return queueSizeUp_; }
  inline void setMaxQueueSizeUp(ULng32 qSize) { queueSizeUp_ = qSize; }
  inline ULng32 getInitialQueueSizeDown() const { return initialQueueSizeDown_; }
  inline ULng32 getInitialQueueSizeUp() const { return initialQueueSizeUp_; }
  inline short getQueueResizeLimit() const { return queueResizeLimit_; }
  inline short getQueueResizeFactor() const { return queueResizeFactor_; }
  inline void setQueueResizeParams(ULng32 initialQueueSizeDown, ULng32 initialQueueSizeUp, short queueResizeLimit,
                                   short queueResizeFactor) {
    initialQueueSizeDown_ = initialQueueSizeDown;
    initialQueueSizeUp_ = initialQueueSizeUp;
    queueResizeLimit_ = queueResizeLimit;
    queueResizeFactor_ = queueResizeFactor;
  }

  // this method should be defined in any TDB which store the plan
  // version.
  virtual void setPlanVersion(UInt32 /*pv*/) {
    // should never reach here.
  }

  // ---------------------------------------------------------------------
  // numChildren() -- Returns the number of children for this TDB
  // ---------------------------------------------------------------------
  virtual Int32 numChildren() const { return -1; }

  // ---------------------------------------------------------------------
  // getNodeName() -- Returns the name of this TDB.
  // ---------------------------------------------------------------------
  virtual const char *getNodeName() const { return "ComTDB"; }

  // ---------------------------------------------------------------------
  // getChild(int child) -- Returns a pointer to the nth child TDB of this
  // TDB. This function cannot be used to get children which reside in
  // other processes -- see getChildForGUI.
  // ---------------------------------------------------------------------
  virtual const ComTdb *getChild(Int32) const { return NULL; }

  // ---------------------------------------------------------------------
  // getChildForGUI() takes as additional arguments the fragment directory
  // and the base pointer. The method is used by the GUI to display a
  // complete TDB tree, including the parts which will be shipped to a
  // different process (to DP2 or ESP). It is only redefined by operators
  // which will sit on process boundaries. Since the child TDB of such
  // operators is located in another fragment, the fragment directory has
  // to be looked up. See ComTdbPartnAccess.h_tdb::getChildForGUI() for an
  // example of how this could be done.
  // ---------------------------------------------------------------------
  virtual const ComTdb *getChildForGUI(Int32 pos, Lng32 /*base*/, void * /*frag_dir*/) const { return getChild(pos); }

  // ---------------------------------------------------------------------
  // numExpressions() -- Returns the number of expression for this TDB.
  // ---------------------------------------------------------------------
  virtual Int32 numExpressions() const { return 0; }

  // ---------------------------------------------------------------------
  // GetExpressionName(int) -- Returns a string identifying the nth
  // expression for this TDB.
  // ---------------------------------------------------------------------
  virtual const char *getExpressionName(Int32) const { return "none"; }

  // ---------------------------------------------------------------------
  // GetExpressionNode(int) -- Returns the nth expression for this TDB.
  // ---------------------------------------------------------------------
  virtual ex_expr *getExpressionNode(Int32) { return 0; }

  // ---------------------------------------------------------------------
  // overloaded to return TRUE for root operators (master executor root,
  // EID root, ESP root)
  // ---------------------------------------------------------------------
  virtual NABoolean isRoot() const { return FALSE; }

  // ---------------------------------------------------------------------
  // Build a TCB for this TDB. Redefined in the Executor project.
  // ---------------------------------------------------------------------
  virtual ex_tcb *build(ex_globals *globals) { return NULL; }

  Lng32 getBufferSize() const { return bufferSize_; }
  Lng32 getNumBuffers() const { return numBuffers_; }

  void resetBufInfo(Int32 numBuffers, UInt32 bufferSize) {
    numBuffers_ = numBuffers;
    bufferSize_ = bufferSize;
  }

  Int32 &firstNRows() { return firstNRows_; }

  // ---------------------------------------------------------------------
  // do we collect statistics? This indicator is only set for root tdbs.
  // (root, eid-root and split-bottom (esp-root).
  // The root nodes allocate a stats area if the indicator is true. All
  // other tcbs allocate a stats entry if the stats area is present
  // ---------------------------------------------------------------------
  NABoolean getCollectStats() { return (flags_ & COLLECT_STATS) != 0; }
  inline void setCollectStats(NABoolean v) { (v ? flags_ |= COLLECT_STATS : flags_ &= ~COLLECT_STATS); }

  NABoolean denseBuffers() { return (flags_ & DENSE_BUFFERS) != 0; }
  inline void setDenseBuffers(NABoolean v) { (v ? flags_ |= DENSE_BUFFERS : flags_ &= ~DENSE_BUFFERS); }

  short doOltQueryOpt() const { return flags_ & DO_OLT_QUERY_OPT; }

  void setDoOltQueryOpt(NABoolean v) { (v ? flags_ |= DO_OLT_QUERY_OPT : flags_ &= ~DO_OLT_QUERY_OPT); };

  short doOltQueryOptLean() const { return flags_ & DO_OLT_QUERY_OPT_LEAN; }

  void setDoOltQueryOptLean(NABoolean v) { (v ? flags_ |= DO_OLT_QUERY_OPT_LEAN : flags_ &= ~DO_OLT_QUERY_OPT_LEAN); };

  NABoolean getCollectRtsStats() { return (flags_ & COLLECT_RTS_STATS) != 0; }
  inline void setCollectRtsStats(NABoolean v) { (v ? flags_ |= COLLECT_RTS_STATS : flags_ &= ~COLLECT_RTS_STATS); }

  short floatFieldsAreIEEE() const { return flags_ & FLOAT_FIELDS_ARE_IEEE; }

  short isNonFatalErrorTolerated() const { return flags_ & TOLERATE_NONFATAL_ERROR; }

  void setTolerateNonFatalError(NABoolean v) {
    v ? flags_ |= TOLERATE_NONFATAL_ERROR : flags_ &= ~TOLERATE_NONFATAL_ERROR;
  }

  short isCIFON() const { return flags_ & CIFON; }

  void setCIFON(NABoolean v) { v ? flags_ |= CIFON : flags_ &= ~CIFON; }

  short parquetInSupport() const { return flags_ & PARQUET_IN_SUPPORT; }
  void setParquetInSupport(NABoolean v) { v ? flags_ |= PARQUET_IN_SUPPORT : flags_ &= ~PARQUET_IN_SUPPORT; }
  NABoolean useLibHdfs() const { return ((flags_ & USE_LIBHDFS) > 0); }

  void setUseLibHdfs(NABoolean v) { v ? flags_ |= USE_LIBHDFS : flags_ &= ~USE_LIBHDFS; }

  enum CollectStatsType {
    NO_STATS = SQLCLI_NO_STATS,
    ACCUMULATED_STATS = SQLCLI_ACCUMULATED_STATS,    // collect accumulated stats.
    PERTABLE_STATS = SQLCLI_PERTABLE_STATS,          // collect per table stats that
    ALL_STATS = SQLCLI_ALL_STATS,                    // collect all stats about all exe operators
    OPERATOR_STATS = SQLCLI_OPERATOR_STATS,          // collect all stats but merge at operator(tdb)
                                                     // granularity. Used to return data at user operator
                                                     // level.
    CPU_OFFENDER_STATS = SQLCLI_CPU_OFFENDER_STATS,  // Collection of ROOT_OPER_STATS of
                                                     // all currently running queries in a cpu
    QID_DETAIL_STATS = SQLCLI_QID_DETAIL_STATS,      // Collection of ROOT_OPER_STATS for
                                                     // a given QID
    SE_OFFENDER_STATS = SQLCLI_SE_OFFENDER_STATS,
    RMS_INFO_STATS = SQLCLI_RMS_INFO_STATS,
    SAME_STATS = SQLCLI_SAME_STATS,
    PROGRESS_STATS = SQLCLI_PROGRESS_STATS,  // Petable Stats + BMO stats for display via GET STATISTICS command
    ET_OFFENDER_STATS = SQLCLI_ET_OFFENDER_STATS,
    MEM_OFFENDER_STATS = SQLCLI_MEM_OFFENDER_STATS,
    OBJECT_EPOCH_STATS = SQLCLI_OBJECT_EPOCH_STATS,
    OBJECT_LOCK_STATS = SQLCLI_OBJECT_LOCK_STATS,
    QUERY_INVALIDATION_STATS = SQLCLI_QUERY_INVALIDATION_STATS
  };

  CollectStatsType getCollectStatsType() { return (CollectStatsType)collectStatsType_; };
  void setCollectStatsType(CollectStatsType s) { collectStatsType_ = s; };

  Lng32 getExplainNodeId() const { return (Lng32)explainNodeId_; }
  void setExplainNodeId(Lng32 id) { explainNodeId_ = (Int32)id; }

  UInt16 getPertableStatsTdbId() const { return pertableStatsTdbId_; }
  void setPertableStatsTdbId(UInt16 id) { pertableStatsTdbId_ = id; }

  Float32 getFloatValue(char *v) const {
    Float32 f;
    str_cpy_all((char *)&f, v, sizeof(Float32));

    return f;
  }

  Float64 getDoubleValue(char *v) const {
    Float64 f;
    str_cpy_all((char *)&f, v, sizeof(Float64));

    return f;
  }

  enum OverflowModeType {
    OFM_DISK = SQLCLI_OFM_DISK_TYPE,
    OFM_SSD = SQLCLI_OFM_SSD_TYPE,
    OFM_MMAP = SQLCLI_OFM_MMAP_TYPE
  };

  OverflowModeType getOverFlowMode() { return (OverflowModeType)overflowMode_; }

  void setOverflowMode(OverflowModeType ofm) { overflowMode_ = (Int16)ofm; }

  ex_cri_desc *getCriDescUp() { return criDescUp_; };
  void setCriDescUp(ex_cri_desc *cri) { criDescUp_ = cri; };

  ex_cri_desc *getCriDescDown() { return criDescDown_; };

  // Help function on output record length
  UInt32 getRecordLength() { return recordLength_; }
  void setRecordLength(UInt32 x) { recordLength_ = x; }
  virtual Float32 getEstimatedMemoryUsage() { return 0; }
  enum Type {
    TABLE_INFO,
    COLUMN_INFO,
    KEY_INFO,
    INDEX_INFO,
    REF_CONSTRAINTS_INFO,
    CONSTRAINTS_INFO,
    VIEW_INFO,
    ROUTINE_INFO,
    SEQUENCE_INFO
  };

 private:
  enum FlagsType {
    COLLECT_STATS = 0x0001,

    DENSE_BUFFERS = 0x0002,

    DO_OLT_QUERY_OPT = 0x0004,

    // this bit, if set, indicates that the fields defined as 'Float'
    // in TDBs are to be interpreted as IEEE floats.
    // It is to distinguish them from that field containing tandem
    // float values for pre R2 plans.
    // Starting R2 and beyond, this flag bit will always be set to 1
    // since all floats are now IEEE floats.
    FLOAT_FIELDS_ARE_IEEE = 0x0008,

    // olt query opt part 2 (leaner olt opt).
    DO_OLT_QUERY_OPT_LEAN = 0x0010,
    COLLECT_RTS_STATS = 0x0020,

    // flag to teach operators to ignore NF(NAR) errors
    TOLERATE_NONFATAL_ERROR = 0x0040,

    // flag for CIF
    CIFON = 0x0080,

    PARQUET_IN_SUPPORT = 0x0200,
    //
    USE_LIBHDFS = 0x0400

  };

 protected:
  // ---------------------------------------------------------------------
  // Identification information
  // ---------------------------------------------------------------------
  Int32 objectId_;             // 00-03
  ex_eye_catcher eyeCatcher_;  // 04-07

  // ---------------------------------------------------------------------
  // Queue sizes for parent down and up queues
  // ---------------------------------------------------------------------
  UInt32 queueSizeDown_;  // 08-11
  UInt32 queueSizeUp_;    // 12-15

  // ---------------------------------------------------------------------
  // Number and size of buffers to be allocated
  // ---------------------------------------------------------------------
  Int32 numBuffers_;   // 16-19
  UInt32 bufferSize_;  // 20-23

  // ---------------------------------------------------------------------
  // CRI descriptors of requests down from and up to parent
  // ---------------------------------------------------------------------
  ExCriDescPtr criDescDown_;  // 24-31
  ExCriDescPtr criDescUp_;    // 32-39

  // ---------------------------------------------------------------------
  // Parent TDB of this TDB.
  // ---------------------------------------------------------------------
  ComTdbPtr parentTdb_;      // 40-47
  Float32 estRowsAccessed_;  // 48-51
  char filler_[4];           // 52-55

 private:
  // ---------------------------------------------------------------------
  // Compiler estimate of number of rows
  // ---------------------------------------------------------------------
  Float32 estRowsUsed_;  // 56-59

 protected:
  // ---------------------------------------------------------------------
  // Signals the expression mode to be applied at fixup time for PCODE
  // generation.
  // ---------------------------------------------------------------------
  UInt16 expressionMode_;  // 60-61
  UInt16 flags_;           // 62-63

  // Maximum number of rows to be returned.
  // A GET_N request is sent to child at runtime.
  // Executor stops processing (cancel) after
  // returning these many rows. If set to -1,
  // then all rows are to be returned.
  Int32 firstNRows_;  // 64-67

  // id of this TDB for statistics display
  Int32 tdbId_;  // 68-71

  // ---------------------------------------------------------------------
  // Initial values for dynamically resized queues and buffer pools
  // the other set of values is either the max size (for dynamic
  // resizing) or the fixed size (without dynamic resizing). These
  // values give the initial size, the number of empty/full plus
  // full/empty transitions until a resize happens, and the factor
  // by which the queue is resized.
  // ---------------------------------------------------------------------
  UInt32 initialQueueSizeDown_;  // 72-75
  UInt32 initialQueueSizeUp_;    // 76-79
  Int16 queueResizeLimit_;       // 80-81
  Int16 queueResizeFactor_;      // 82-83

  // ---------------------------------------------------------------------
  // if COLLECT_STATS is set, this fields contains details about what
  // kind of statistics are to be collected.
  // ---------------------------------------------------------------------
  UInt16 collectStatsType_;  // 84-85

  // ---------------------------------------------------------------------
  // This tdb id is used to consolidate information on a per table basis
  // similar to the way it is done in sqlmp. Valid when PERTABLE_STATS are
  // being collected. All tdbs that are working for the same table
  // (like, PA, DP2 oper) get the same tdb id at code generation time.
  // ---------------------------------------------------------------------
  UInt16 pertableStatsTdbId_;  // 86-87

  // link to the EXPLAIN node id of the corresponding node in
  // the EXPLAIN stored procedure (note that multiple TDBs can
  // map to one EXPLAIN node, like in PA or ESP exchange nodes).
  Int32 explainNodeId_;  // 88-91

  Int16 overflowMode_;  // 92-93

  UInt32 recordLength_;  // 94-97
  // ---------------------------------------------------------------------
  // Fillers for potential future extensions without changing class size.
  // When a new member is added, size of this filler should be reduced so
  // that the size of the object remains the same.
  // ---------------------------------------------------------------------
  char fillersComTdb_[34];  // 94-127
};

class ComTdbVirtTableBase {
 public:
  ComTdbVirtTableBase() {}

  virtual void init() {
    // skip virtual table pointer stored as first eight bytes (64 bit pointer)
    char *vtblPtr = (char *)this;
    char *dataMem = vtblPtr + sizeof(vtblPtr);
    memset(dataMem, '\0', (size() - sizeof(vtblPtr)));
  }
  virtual Int32 size() { return 0; }
};

class ComTdbVirtTableTableInfo : public ComTdbVirtTableBase {
 public:
  ComTdbVirtTableTableInfo() : ComTdbVirtTableBase() {
    init();
    numInitialSaltRegions = -1;
    numTrafReplicas = 0;
  }

  virtual Int32 size() { return sizeof(ComTdbVirtTableTableInfo); };

  const char *tableName;
  Int64 createTime;
  Int64 redefTime;
  Int64 objUID;
  Int64 objDataUID;
  Lng32 isAudited;
  Lng32 validDef;  // 0, invalid. 1, valid. 2, disabled.
  Int32 objOwnerID;
  Int32 schemaOwnerID;
  const char *hbaseCreateOptions;
  Lng32 numSaltPartns;          // num of salted partitions this table was created with.
  Lng32 numInitialSaltRegions;  // initial number of regions for salted table
  Int16 numTrafReplicas;
  const char *hbaseSplitClause;  // SPLIT BY clause as string

  ComRowFormat rowFormat;

  ComReplType xnRepl;
  ComStorageType storageType;

  const char *defaultColFam;
  const char *allColFams;

  const char *tableNamespace;

  Int64 baseTableUID;

  // if this table has LOB columns which are created with multiple datafiles
  // for each column, this field represents the number of hdfs datafiles.
  // See CmpSeabaseDDL::createSeabaseTable2 where datafiles are created.
  Int32 numLOBdatafiles;

  Int64 schemaUID;

  Int64 objectFlags;  // flags from OBJECTS table
  Int64 tablesFlags;  // flags from TABLES table
};

class ComTdbVirtTableColumnInfo : public ComTdbVirtTableBase {
 public:
  ComTdbVirtTableColumnInfo(const char *cName, Lng32 cNum, ComColumnClass cc, Lng32 dt, Lng32 l, Lng32 n,
                            SQLCHARSET_CODE cs, Lng32 p, Lng32 s, Lng32 dtS, Lng32 dtE, Lng32 u, const char *ch,
                            ULng32 flags, ComColumnDefaultClass dc, const char *defVal, const char *hcf,
                            const char *hcq, const char *pd, Lng32 io)
      : ComTdbVirtTableBase(),
        colName(cName),
        colNumber(cNum),
        columnClass(cc),
        datatype(dt),
        length(l),
        nullable(n),
        charset(cs),
        precision(p),
        scale(s),
        dtStart(dtS),
        dtEnd(dtE),
        upshifted(u),
        colHeading(ch),
        hbaseColFlags(flags),
        defaultClass(dc),
        hbaseColFam(hcf),
        hbaseColQual(hcq),
        isOptional(io),
        colFlags(0),
        compDefnStr(NULL) {
    strcpy(paramDirection, pd);
  };

  ComTdbVirtTableColumnInfo() : ComTdbVirtTableBase() { init(); }

  Int32 size() { return sizeof(ComTdbVirtTableColumnInfo); }

  const char *colName;
  Lng32 colNumber;
  ComColumnClass columnClass;
  Lng32 datatype;
  Lng32 length;
  Lng32 nullable;
  SQLCHARSET_CODE charset;
  Lng32 precision;  // if interval, this is leading precision
  Lng32 scale;      // if dt/int, this is fractional precision
  Lng32 dtStart;    // 1:year, 2:month, 3:day, 4:hour, 5:min, 6:sec, 0:n/a
  Lng32 dtEnd;      // 1:year, 2:month, 3:day, 4:hour, 5:min, 6:sec, 0:n/a
  Lng32 upshifted;  // 0, regular. 1, UPSHIFT char datatype
  const char *colHeading;
  ULng32 hbaseColFlags;
  ComColumnDefaultClass defaultClass;
  const char *defVal;
  const char *initDefVal;
  const char *hbaseColFam;
  const char *hbaseColQual;
  char paramDirection[4];
  Lng32 isOptional;  // 0, regular (does not apply OR FALSE). 1, Param is optional

  // definition of composite column
  const char *compDefnStr;

  Int64 colFlags;
};

class ComTdbVirtTableHistogramInfo : public ComTdbVirtTableBase {
 public:
  ComTdbVirtTableHistogramInfo() : ComTdbVirtTableBase() { init(); }

  virtual Int32 size() { return sizeof(ComTdbVirtTableHistogramInfo); }

  TrafHistogramDesc histogramDesc_;
};

class ComTdbVirtTableHistintInfo : public ComTdbVirtTableBase {
 public:
  ComTdbVirtTableHistintInfo() : ComTdbVirtTableBase() { init(); }

  virtual Int32 size() { return sizeof(ComTdbVirtTableHistintInfo); }

  TrafHistIntervalDesc histintDesc_;
};

class ComTdbVirtTableKeyInfo : public ComTdbVirtTableBase {
 public:
  ComTdbVirtTableKeyInfo(const char *cn, Lng32 ksn, Lng32 tcn, Lng32 o, Lng32 nkc, const char *hcf, const char *hcq)
      : ComTdbVirtTableBase(),
        colName(cn),
        keySeqNum(ksn),
        tableColNum(tcn),
        ordering(o),
        nonKeyCol(nkc),
        hbaseColFam(hcf),
        hbaseColQual(hcq) {}

  ComTdbVirtTableKeyInfo() : ComTdbVirtTableBase() { init(); }

  virtual Int32 size() { return sizeof(ComTdbVirtTableKeyInfo); }

  const char *colName;
  Lng32 keySeqNum;
  Lng32 tableColNum;

  enum { ASCENDING_ORDERING = 0, DESCENDING_ORDERING = 1 };
  Lng32 ordering;  // 0 means ascending, 1 means descending
                   // (see, for example, CmpSeabaseDDL::buildKeyInfoArray)

  Lng32 nonKeyCol;  // if 1, this is a base table pkey col for unique indexes

  const char *hbaseColFam;
  const char *hbaseColQual;
};

class ComTdbVirtTableIndexInfo : public ComTdbVirtTableBase {
 public:
  ComTdbVirtTableIndexInfo() : ComTdbVirtTableBase() {
    init();
    numTrafReplicas = 0;
  }

  virtual Int32 size() { return sizeof(ComTdbVirtTableIndexInfo); }

  const char *baseTableName;
  const char *indexName;
  Int64 indexFlags;  // flags from INDEXES table
  Int64 indexUID;
  Lng32 keytag;
  Lng32 isUnique;
  // ngram flag
  Lng32 isNgram;
  Lng32 isExplicit;
  Lng32 keyColCount;
  Lng32 nonKeyColCount;
  const char *hbaseCreateOptions;
  Lng32 numSaltPartns;  // num of salted partitions this index was created with.
  Int16 numTrafReplicas;
  ComRowFormat rowFormat;
  const ComTdbVirtTableKeyInfo *keyInfoArray;
  const ComTdbVirtTableKeyInfo *nonKeyInfoArray;
};

// referencing and referenced constraints
class ComTdbVirtTableRefConstraints : public ComTdbVirtTableBase {
 public:
  ComTdbVirtTableRefConstraints() : ComTdbVirtTableBase() { init(); }

  virtual Int32 size() { return sizeof(ComTdbVirtTableRefConstraints); }

  char *constrName;
  char *baseTableName;
};

class ComTdbVirtTableConstraintInfo : public ComTdbVirtTableBase {
 public:
  ComTdbVirtTableConstraintInfo() : ComTdbVirtTableBase() { init(); }

  virtual Int32 size() { return sizeof(ComTdbVirtTableConstraintInfo); }

  char *baseTableName;
  char *constrName;
  Lng32 constrType;  // unique constr = 0, ref constr = 1, check constr = 2, pkey constr = 3
  Lng32 colCount;
  Lng32 isEnforced;

  ComTdbVirtTableKeyInfo *keyInfoArray;

  // referencing constraints
  Lng32 numRingConstr;
  ComTdbVirtTableRefConstraints *ringConstrArray;

  // referenced constraints
  Lng32 numRefdConstr;
  ComTdbVirtTableRefConstraints *refdConstrArray;

  // if constrType == 2
  Lng32 checkConstrLen;
  char *checkConstrText;

  Lng32 notSerialized;
};

class ComTdbVirtTableViewInfo : ComTdbVirtTableBase {
 public:
  ComTdbVirtTableViewInfo() : ComTdbVirtTableBase() { init(); }

  virtual Int32 size() { return sizeof(ComTdbVirtTableViewInfo); }

  char *viewName;
  char *viewText;
  char *viewCheckText;
  char *viewColUsages;
  Lng32 isUpdatable;
  Lng32 isInsertable;
};

class ComTdbVirtTableRoutineInfo : public ComTdbVirtTableBase {
 public:
  ComTdbVirtTableRoutineInfo(const char *rn, const char *ut, const char *lt, Int16 d, const char *sa, Int16 con,
                             Int16 i, const char *ps, const char *ta, Int32 mr, Int32 sas, const char *en,
                             const char *p, const char *uv, const char *es, const char *em, const char *lf, Int32 lv,
                             const char *s, const char *ls, Int64 luid)
      : ComTdbVirtTableBase(),
        routine_name(rn),
        deterministic(d),
        call_on_null(con),
        isolate(i),
        max_results(mr),
        state_area_size(sas),
        external_name(en),
        library_filename(lf),
        library_version(lv),
        signature(s),
        library_sqlname(ls),
        lib_obj_uid(luid) {
    strcpy(UDR_type, ut);
    strcpy(language_type, lt);
    strcpy(sql_access, sa);
    strcpy(param_style, ps);
    strcpy(transaction_attributes, ta);
    strcpy(parallelism, p);
    strcpy(user_version, uv);
    strcpy(external_security, es);
    strcpy(execution_mode, em);
  }

  ComTdbVirtTableRoutineInfo() : ComTdbVirtTableBase() { init(); }

  virtual Int32 size() { return sizeof(ComTdbVirtTableRoutineInfo); }

  const char *routine_name;
  char UDR_type[3];
  char language_type[3];
  Int16 deterministic;
  char sql_access[3];
  Int16 call_on_null;
  Int16 isolate;
  char param_style[3];
  char transaction_attributes[3];
  Int32 max_results;
  Int32 state_area_size;
  const char *external_name;
  char parallelism[3];
  char user_version[32];
  char external_security[3];
  char execution_mode[3];
  const char *library_filename;
  Int32 library_version;
  const char *signature;
  const char *library_sqlname;
  Int64 object_uid;
  Int32 object_owner_id;
  Int32 schema_owner_id;
  Int64 lib_redef_time;
  char *lib_blob_handle;
  char *lib_sch_name;
  Int64 lib_obj_uid;
};

class ComTdbVirtTableSequenceInfo : public ComTdbVirtTableBase {
 public:
  ComTdbVirtTableSequenceInfo() : ComTdbVirtTableBase() { init(); }

  virtual Int32 size() { return sizeof(ComTdbVirtTableSequenceInfo); }

  Lng32 seqType;  // ComSequenceGeneratorType
  Lng32 datatype;
  Int64 startValue;
  Int64 increment;
  Int64 maxValue;
  Int64 minValue;
  Lng32 cycleOption;
  Int64 cache;
  Int64 seqUID;
  Int64 nextValue;
  Int64 redefTime;
  Int64 flags;  // flags from SEQ_GEN table
};

// This class describes schema, object and column privileges and if they are
// grantable (WGO) for an object. Privileges are stored as a vector of
// PrivMgrDesc's, one per distinct grantee.
//
//    PrivMgrDesc:
//      grantee - Int32
//      schemaUID -  identifies the schema
//      schemaPrivs - PrivMgrCoreDesc
//      objectPrivs - PrivMgrCoreDesc
//      columnPrivs - list of PrivMgrCoreDesc
//    PrivMgrCoreDesc:
//      bitmap of granted privileges
//      bitmap of associated WGO (with grant option)
//      column ordinal (number) set to -1 for schema and object privs
class ComTdbVirtTablePrivInfo : public ComTdbVirtTableBase {
 public:
  ComTdbVirtTablePrivInfo() : ComTdbVirtTableBase() { init(); }

  virtual Int32 size() { return sizeof(ComTdbVirtTablePrivInfo); }

  PrivMgrDescList *privmgr_desc_list;
};

class ComTdbVirtTableLibraryInfo : public ComTdbVirtTableBase {
 public:
  ComTdbVirtTableLibraryInfo() : ComTdbVirtTableBase() { init(); }

  virtual Int32 size() { return sizeof(ComTdbVirtTableLibraryInfo); }

  const char *library_name;
  Int64 library_UID;
  Int32 library_version;
  const char *library_filename;
  Int32 object_owner_id;
  Int32 schema_owner_id;
};

class ComTdbVirtTableStatsInfo : public ComTdbVirtTableBase {
 public:
  ComTdbVirtTableStatsInfo() : ComTdbVirtTableBase() { init(); }

  virtual Int32 size() { return sizeof(ComTdbVirtTableStatsInfo); };

  Float64 rowcount;
  Int32 hbtBlockSize;
  Int32 hbtIndexLevels;
  Int32 numHistograms;     // num of histograms_descs
  Int32 numHistIntervals;  // num of hist_interval_descs

  ComTdbVirtTableHistogramInfo *histogramInfo;
  ComTdbVirtTableHistintInfo *histintInfo;
};

class ComTdbVirtIndexCommentInfo : public ComTdbVirtTableBase {
 public:
  ComTdbVirtIndexCommentInfo() : ComTdbVirtTableBase() { init(); }

  virtual Int32 size() { return sizeof(ComTdbVirtIndexCommentInfo); }

  const char *indexFullName;
  const char *indexComment;
};

class ComTdbVirtColumnCommentInfo : public ComTdbVirtTableBase {
 public:
  ComTdbVirtColumnCommentInfo() : ComTdbVirtTableBase() { init(); }

  virtual Int32 size() { return sizeof(ComTdbVirtColumnCommentInfo); }

  const char *columnName;
  const char *columnComment;
};

class ComTdbVirtObjCommentInfo : public ComTdbVirtTableBase {
 public:
  ComTdbVirtObjCommentInfo() : ComTdbVirtTableBase() {
    init();

    objectUid = 0;
    objectComment = NULL;
    numColumnComment = 0;
    columnCommentArray = NULL;
    numIndexComment = 0;
    indexCommentArray = NULL;
  }

  virtual Int32 size() { return sizeof(ComTdbVirtObjCommentInfo); }

  Int64 objectUid;
  const char *objectComment;

  Int32 numColumnComment;
  ComTdbVirtColumnCommentInfo *columnCommentArray;

  Int32 numIndexComment;
  ComTdbVirtIndexCommentInfo *indexCommentArray;
};

class ComTdbVirtTablePartInfo : public ComTdbVirtTableBase {
 public:
  ComTdbVirtTablePartInfo() : ComTdbVirtTableBase() {
    init();

    parentUid = 0;
    partitionUid = 0;
    partitionName = NULL;
    partitionEntityName = NULL;
    isSubPartition = FALSE;
    hasSubPartition = FALSE;
    partPosition = 0;
    partExpression = NULL;
    partExpressionLen = 0;
    isValid = TRUE;
    isReadonly = FALSE;
    isInMemory = FALSE;
    defTime = 0;
    flags = 0;
    subpartitionCnt = 0;
    subPartArray = NULL;
  }

  Int32 size() { return sizeof(ComTdbVirtTablePartInfo); }

  Int64 parentUid;
  Int64 partitionUid;

  char *partitionName;
  char *partitionEntityName;
  NABoolean isSubPartition;
  NABoolean hasSubPartition;
  Lng32 partPosition;
  char *partExpression;
  Lng32 partExpressionLen;

  // currently not used
  NABoolean isValid;
  NABoolean isReadonly;
  NABoolean isInMemory;
  Int64 defTime;
  Int64 flags;

  Lng32 subpartitionCnt;

  ComTdbVirtTablePartInfo *subPartArray;
};

class ComTdbVirtTablePartitionV2Info : public ComTdbVirtTableBase {
 public:
  ComTdbVirtTablePartitionV2Info() : ComTdbVirtTableBase() {
    init();

    baseTableUid = 0;
    partitionType = 0;
    partitionColIdx = NULL;
    partitionColCount = 0;
    subpartitionType = 0;
    subpartitionColIdx = NULL;
    subpartitionColCount = 0;
    partitionInterval = NULL;
    subpartitionInterval = NULL;
    partitionAutolist = 0;
    subpartitionAutolist = 0;
    partArray = NULL;
    stlPartitionCnt = 0;
    flags = 0;
  }

  Int32 size() { return sizeof(ComTdbVirtTablePartitionV2Info); }

  Int64 baseTableUid;

  // partition
  Lng32 partitionType;
  char *partitionColIdx;
  Lng32 partitionColCount;

  // subpartition
  Lng32 subpartitionType;
  char *subpartitionColIdx;
  Lng32 subpartitionColCount;

  // interval and autolist, currently not used
  char *partitionInterval;
  char *subpartitionInterval;
  Lng32 partitionAutolist;
  Lng32 subpartitionAutolist;

  Lng32 stlPartitionCnt;

  // specific partition/subpartition info
  ComTdbVirtTablePartInfo *partArray;

  Int64 flags;
};

#endif /* COMTDB_H */
