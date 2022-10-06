

#ifndef COMTDB_H
#define COMTDB_H

#include "cli/sqlcli.h"
#include "common/BaseTypes.h"  // for Cardinality
#include "common/ComSmallDefs.h"
#include "common/Int64.h"  // for long
#include "exp/ExpCriDesc.h"
#include "exp/ExpLOBenums.h"
#include "exp/exp_expr.h"              // subclasses of TDB contain expressions
#include "export/NAVersionedObject.h"  // for NAVersionedObject
#include "sqlcat/TrafDDLdesc.h"
#include "sqlcomp/PrivMgrDesc.h"  // Privilege descriptors

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
               queue_index sizeUp, int numBuffers, UInt32 bufferSize, int firstNRows)
      : estimatedRowCount_(estimatedRowCount),
        criDown_(criDown),
        criUp_(criUp),
        sizeDown_(sizeDown),
        sizeUp_(sizeUp),
        numBuffers_(numBuffers),
        bufferSize_(bufferSize),
        firstNRows_(firstNRows){};

  void getValues(Cardinality &estimatedRowCount, ExCriDescPtr &criDown, ExCriDescPtr &criUp, queue_index &sizeDown,
                 queue_index &sizeUp, int &numBuffers, UInt32 &bufferSize, int &firstNRows);

 private:
  Cardinality estimatedRowCount_;
  ex_cri_desc *criDown_;
  ex_cri_desc *criUp_;
  queue_index sizeDown_;
  queue_index sizeUp_;
  int numBuffers_;
  UInt32 bufferSize_;
  int firstNRows_;
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
         ex_cri_desc *criUp = NULL, queue_index sizeDown = 0, queue_index sizeUp = 0, int numBuffers = 0,
         UInt32 bufferSize = 0, int uniqueId = 0, int initialQueueSizeDown = 4, int initialQueueSizeUp = 4,
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
  virtual int unpack(void *base, void *reallocator);

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
  virtual int orderedQueueProtocol() const { return 1; }

  // ---------------------------------------------------------------------
  // Used by the internal SHOWPLAN command to get attributes of a TDB.
  // ---------------------------------------------------------------------
  virtual void displayContents(Space *space, int flag);
  virtual void displayExpression(Space *space, int flag);
  virtual void displayChildren(Space *space, int flag);

  // ---------------------------------------------------------------------
  // Accessors/Mutators.
  // ---------------------------------------------------------------------
  inline ex_node_type getNodeType() const { return (ComTdb::ex_node_type)getClassID(); }
  inline void setNodeType(ex_node_type t) { setClassID(t); }
  inline void setEyeCatcher(const char *eye) { str_cpy_all((char *)&eyeCatcher_, eye, 4); }

  inline ComTdb *getParentTdb() { return parentTdb_; }
  inline void setParentTdb(ComTdb *tdb) { parentTdb_ = tdb; }
  inline void setTdbId(int uniqueId) { tdbId_ = uniqueId; }
  inline int getTdbId() const { return tdbId_; }
  inline Cardinality getEstRowsAccessed() const { return estRowsAccessed_; }

  inline void setEstRowsAccessed(Cardinality estRowsAccessed) { estRowsAccessed_ = estRowsAccessed; }

  inline Cardinality getEstRowsUsed() const { return estRowsUsed_; }

  inline void setEstRowsUsed(Cardinality estRowsUsed) { estRowsUsed_ = estRowsUsed; }

  inline int getMaxQueueSizeDown() const { return queueSizeDown_; }
  inline int getMaxQueueSizeUp() const { return queueSizeUp_; }
  inline void setMaxQueueSizeUp(int qSize) { queueSizeUp_ = qSize; }
  inline int getInitialQueueSizeDown() const { return initialQueueSizeDown_; }
  inline int getInitialQueueSizeUp() const { return initialQueueSizeUp_; }
  inline short getQueueResizeLimit() const { return queueResizeLimit_; }
  inline short getQueueResizeFactor() const { return queueResizeFactor_; }
  inline void setQueueResizeParams(int initialQueueSizeDown, int initialQueueSizeUp, short queueResizeLimit,
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
  virtual int numChildren() const { return -1; }

  // ---------------------------------------------------------------------
  // getNodeName() -- Returns the name of this TDB.
  // ---------------------------------------------------------------------
  virtual const char *getNodeName() const { return "ComTDB"; }

  // ---------------------------------------------------------------------
  // getChild(int child) -- Returns a pointer to the nth child TDB of this
  // TDB. This function cannot be used to get children which reside in
  // other processes -- see getChildForGUI.
  // ---------------------------------------------------------------------
  virtual const ComTdb *getChild(int) const { return NULL; }

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
  virtual const ComTdb *getChildForGUI(int pos, int /*base*/, void * /*frag_dir*/) const { return getChild(pos); }

  // ---------------------------------------------------------------------
  // numExpressions() -- Returns the number of expression for this TDB.
  // ---------------------------------------------------------------------
  virtual int numExpressions() const { return 0; }

  // ---------------------------------------------------------------------
  // GetExpressionName(int) -- Returns a string identifying the nth
  // expression for this TDB.
  // ---------------------------------------------------------------------
  virtual const char *getExpressionName(int) const { return "none"; }

  // ---------------------------------------------------------------------
  // GetExpressionNode(int) -- Returns the nth expression for this TDB.
  // ---------------------------------------------------------------------
  virtual ex_expr *getExpressionNode(int) { return 0; }

  // ---------------------------------------------------------------------
  // overloaded to return TRUE for root operators (master executor root,
  // EID root, ESP root)
  // ---------------------------------------------------------------------
  virtual NABoolean isRoot() const { return FALSE; }

  // ---------------------------------------------------------------------
  // Build a TCB for this TDB. Redefined in the Executor project.
  // ---------------------------------------------------------------------
  virtual ex_tcb *build(ex_globals *globals) { return NULL; }

  int getBufferSize() const { return bufferSize_; }
  int getNumBuffers() const { return numBuffers_; }

  void resetBufInfo(int numBuffers, UInt32 bufferSize) {
    numBuffers_ = numBuffers;
    bufferSize_ = bufferSize;
  }

  int &firstNRows() { return firstNRows_; }

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

  int getExplainNodeId() const { return (int)explainNodeId_; }
  void setExplainNodeId(int id) { explainNodeId_ = (int)id; }

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
  int objectId_;               // 00-03
  ex_eye_catcher eyeCatcher_;  // 04-07

  // ---------------------------------------------------------------------
  // Queue sizes for parent down and up queues
  // ---------------------------------------------------------------------
  UInt32 queueSizeDown_;  // 08-11
  UInt32 queueSizeUp_;    // 12-15

  // ---------------------------------------------------------------------
  // Number and size of buffers to be allocated
  // ---------------------------------------------------------------------
  int numBuffers_;     // 16-19
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
  int firstNRows_;  // 64-67

  // id of this TDB for statistics display
  int tdbId_;  // 68-71

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
  int explainNodeId_;  // 88-91

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
  virtual int size() { return 0; }
};

class ComTdbVirtTableTableInfo : public ComTdbVirtTableBase {
 public:
  ComTdbVirtTableTableInfo() : ComTdbVirtTableBase() {
    init();
    numInitialSaltRegions = -1;
    numTrafReplicas = 0;
  }

  virtual int size() { return sizeof(ComTdbVirtTableTableInfo); };

  const char *tableName;
  long createTime;
  long redefTime;
  long objUID;
  long objDataUID;
  int isAudited;
  int validDef;  // 0, invalid. 1, valid. 2, disabled.
  int objOwnerID;
  int schemaOwnerID;
  const char *hbaseCreateOptions;
  int numSaltPartns;          // num of salted partitions this table was created with.
  int numInitialSaltRegions;  // initial number of regions for salted table
  Int16 numTrafReplicas;
  const char *hbaseSplitClause;  // SPLIT BY clause as string

  ComRowFormat rowFormat;

  ComReplType xnRepl;
  ComStorageType storageType;

  const char *defaultColFam;
  const char *allColFams;

  const char *tableNamespace;

  long baseTableUID;

  // if this table has LOB columns which are created with multiple datafiles
  // for each column, this field represents the number of hdfs datafiles.
  // See CmpSeabaseDDL::createSeabaseTable2 where datafiles are created.
  int numLOBdatafiles;

  long schemaUID;

  long objectFlags;  // flags from OBJECTS table
  long tablesFlags;  // flags from TABLES table
};

class ComTdbVirtTableColumnInfo : public ComTdbVirtTableBase {
 public:
  ComTdbVirtTableColumnInfo(const char *cName, int cNum, ComColumnClass cc, int dt, int l, int n, SQLCHARSET_CODE cs,
                            int p, int s, int dtS, int dtE, int u, const char *ch, int flags, ComColumnDefaultClass dc,
                            const char *defVal, const char *hcf, const char *hcq, const char *pd, int io)
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

  int size() { return sizeof(ComTdbVirtTableColumnInfo); }

  const char *colName;
  int colNumber;
  ComColumnClass columnClass;
  int datatype;
  int length;
  int nullable;
  SQLCHARSET_CODE charset;
  int precision;  // if interval, this is leading precision
  int scale;      // if dt/int, this is fractional precision
  int dtStart;    // 1:year, 2:month, 3:day, 4:hour, 5:min, 6:sec, 0:n/a
  int dtEnd;      // 1:year, 2:month, 3:day, 4:hour, 5:min, 6:sec, 0:n/a
  int upshifted;  // 0, regular. 1, UPSHIFT char datatype
  const char *colHeading;
  int hbaseColFlags;
  ComColumnDefaultClass defaultClass;
  const char *defVal;
  const char *initDefVal;
  const char *hbaseColFam;
  const char *hbaseColQual;
  char paramDirection[4];
  int isOptional;  // 0, regular (does not apply OR FALSE). 1, Param is optional

  // definition of composite column
  const char *compDefnStr;

  long colFlags;
};

class ComTdbVirtTableHistogramInfo : public ComTdbVirtTableBase {
 public:
  ComTdbVirtTableHistogramInfo() : ComTdbVirtTableBase() { init(); }

  virtual int size() { return sizeof(ComTdbVirtTableHistogramInfo); }

  TrafHistogramDesc histogramDesc_;
};

class ComTdbVirtTableHistintInfo : public ComTdbVirtTableBase {
 public:
  ComTdbVirtTableHistintInfo() : ComTdbVirtTableBase() { init(); }

  virtual int size() { return sizeof(ComTdbVirtTableHistintInfo); }

  TrafHistIntervalDesc histintDesc_;
};

class ComTdbVirtTableKeyInfo : public ComTdbVirtTableBase {
 public:
  ComTdbVirtTableKeyInfo(const char *cn, int ksn, int tcn, int o, int nkc, const char *hcf, const char *hcq)
      : ComTdbVirtTableBase(),
        colName(cn),
        keySeqNum(ksn),
        tableColNum(tcn),
        ordering(o),
        nonKeyCol(nkc),
        hbaseColFam(hcf),
        hbaseColQual(hcq) {}

  ComTdbVirtTableKeyInfo() : ComTdbVirtTableBase() { init(); }

  virtual int size() { return sizeof(ComTdbVirtTableKeyInfo); }

  const char *colName;
  int keySeqNum;
  int tableColNum;

  enum { ASCENDING_ORDERING = 0, DESCENDING_ORDERING = 1 };
  int ordering;  // 0 means ascending, 1 means descending
                 // (see, for example, CmpSeabaseDDL::buildKeyInfoArray)

  int nonKeyCol;  // if 1, this is a base table pkey col for unique indexes

  const char *hbaseColFam;
  const char *hbaseColQual;
};

class ComTdbVirtTableIndexInfo : public ComTdbVirtTableBase {
 public:
  ComTdbVirtTableIndexInfo() : ComTdbVirtTableBase() {
    init();
    numTrafReplicas = 0;
  }

  virtual int size() { return sizeof(ComTdbVirtTableIndexInfo); }

  const char *baseTableName;
  const char *indexName;
  long indexFlags;  // flags from INDEXES table
  long indexUID;
  int keytag;
  int isUnique;
  // ngram flag
  int isNgram;
  int isExplicit;
  int keyColCount;
  int nonKeyColCount;
  const char *hbaseCreateOptions;
  int numSaltPartns;  // num of salted partitions this index was created with.
  Int16 numTrafReplicas;
  ComRowFormat rowFormat;
  const ComTdbVirtTableKeyInfo *keyInfoArray;
  const ComTdbVirtTableKeyInfo *nonKeyInfoArray;
};

// referencing and referenced constraints
class ComTdbVirtTableRefConstraints : public ComTdbVirtTableBase {
 public:
  ComTdbVirtTableRefConstraints() : ComTdbVirtTableBase() { init(); }

  virtual int size() { return sizeof(ComTdbVirtTableRefConstraints); }

  char *constrName;
  char *baseTableName;
};

class ComTdbVirtTableConstraintInfo : public ComTdbVirtTableBase {
 public:
  ComTdbVirtTableConstraintInfo() : ComTdbVirtTableBase() { init(); }

  virtual int size() { return sizeof(ComTdbVirtTableConstraintInfo); }

  char *baseTableName;
  char *constrName;
  int constrType;  // unique constr = 0, ref constr = 1, check constr = 2, pkey constr = 3
  int colCount;
  int isEnforced;

  ComTdbVirtTableKeyInfo *keyInfoArray;

  // referencing constraints
  int numRingConstr;
  ComTdbVirtTableRefConstraints *ringConstrArray;

  // referenced constraints
  int numRefdConstr;
  ComTdbVirtTableRefConstraints *refdConstrArray;

  // if constrType == 2
  int checkConstrLen;
  char *checkConstrText;

  int notSerialized;
};

class ComTdbVirtTableViewInfo : ComTdbVirtTableBase {
 public:
  ComTdbVirtTableViewInfo() : ComTdbVirtTableBase() { init(); }

  virtual int size() { return sizeof(ComTdbVirtTableViewInfo); }

  char *viewName;
  char *viewText;
  char *viewCheckText;
  char *viewColUsages;
  int isUpdatable;
  int isInsertable;
};

class ComTdbVirtTableRoutineInfo : public ComTdbVirtTableBase {
 public:
  ComTdbVirtTableRoutineInfo(const char *rn, const char *ut, const char *lt, Int16 d, const char *sa, Int16 con,
                             Int16 i, const char *ps, const char *ta, int mr, int sas, const char *en, const char *p,
                             const char *uv, const char *es, const char *em, const char *lf, int lv, const char *s,
                             const char *ls, long luid)
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

  virtual int size() { return sizeof(ComTdbVirtTableRoutineInfo); }

  const char *routine_name;
  char UDR_type[3];
  char language_type[3];
  Int16 deterministic;
  char sql_access[3];
  Int16 call_on_null;
  Int16 isolate;
  char param_style[3];
  char transaction_attributes[3];
  int max_results;
  int state_area_size;
  const char *external_name;
  char parallelism[3];
  char user_version[32];
  char external_security[3];
  char execution_mode[3];
  const char *library_filename;
  int library_version;
  const char *signature;
  const char *library_sqlname;
  long object_uid;
  int object_owner_id;
  int schema_owner_id;
  long lib_redef_time;
  char *lib_blob_handle;
  char *lib_sch_name;
  long lib_obj_uid;
};

class ComTdbVirtTableSequenceInfo : public ComTdbVirtTableBase {
 public:
  ComTdbVirtTableSequenceInfo() : ComTdbVirtTableBase() { init(); }

  virtual int size() { return sizeof(ComTdbVirtTableSequenceInfo); }

  int seqType;  // ComSequenceGeneratorType
  int datatype;
  long startValue;
  long increment;
  long maxValue;
  long minValue;
  int cycleOption;
  long cache;
  long seqUID;
  long nextValue;
  long redefTime;
  long flags;  // flags from SEQ_GEN table
};

// This class describes schema, object and column privileges and if they are
// grantable (WGO) for an object. Privileges are stored as a vector of
// PrivMgrDesc's, one per distinct grantee.
//
//    PrivMgrDesc:
//      grantee - int
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

  virtual int size() { return sizeof(ComTdbVirtTablePrivInfo); }

  PrivMgrDescList *privmgr_desc_list;
};

class ComTdbVirtTableLibraryInfo : public ComTdbVirtTableBase {
 public:
  ComTdbVirtTableLibraryInfo() : ComTdbVirtTableBase() { init(); }

  virtual int size() { return sizeof(ComTdbVirtTableLibraryInfo); }

  const char *library_name;
  long library_UID;
  int library_version;
  const char *library_filename;
  int object_owner_id;
  int schema_owner_id;
};

class ComTdbVirtTableStatsInfo : public ComTdbVirtTableBase {
 public:
  ComTdbVirtTableStatsInfo() : ComTdbVirtTableBase() { init(); }

  virtual int size() { return sizeof(ComTdbVirtTableStatsInfo); };

  Float64 rowcount;
  int hbtBlockSize;
  int hbtIndexLevels;
  int numHistograms;     // num of histograms_descs
  int numHistIntervals;  // num of hist_interval_descs

  ComTdbVirtTableHistogramInfo *histogramInfo;
  ComTdbVirtTableHistintInfo *histintInfo;
};

class ComTdbVirtIndexCommentInfo : public ComTdbVirtTableBase {
 public:
  ComTdbVirtIndexCommentInfo() : ComTdbVirtTableBase() { init(); }

  virtual int size() { return sizeof(ComTdbVirtIndexCommentInfo); }

  const char *indexFullName;
  const char *indexComment;
};

class ComTdbVirtColumnCommentInfo : public ComTdbVirtTableBase {
 public:
  ComTdbVirtColumnCommentInfo() : ComTdbVirtTableBase() { init(); }

  virtual int size() { return sizeof(ComTdbVirtColumnCommentInfo); }

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

  virtual int size() { return sizeof(ComTdbVirtObjCommentInfo); }

  long objectUid;
  const char *objectComment;

  int numColumnComment;
  ComTdbVirtColumnCommentInfo *columnCommentArray;

  int numIndexComment;
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

  int size() { return sizeof(ComTdbVirtTablePartInfo); }

  long parentUid;
  long partitionUid;

  char *partitionName;
  char *partitionEntityName;
  NABoolean isSubPartition;
  NABoolean hasSubPartition;
  int partPosition;
  char *partExpression;
  int partExpressionLen;

  // currently not used
  NABoolean isValid;
  NABoolean isReadonly;
  NABoolean isInMemory;
  long defTime;
  long flags;

  int subpartitionCnt;

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

  int size() { return sizeof(ComTdbVirtTablePartitionV2Info); }

  long baseTableUid;

  // partition
  int partitionType;
  char *partitionColIdx;
  int partitionColCount;

  // subpartition
  int subpartitionType;
  char *subpartitionColIdx;
  int subpartitionColCount;

  // interval and autolist, currently not used
  char *partitionInterval;
  char *subpartitionInterval;
  int partitionAutolist;
  int subpartitionAutolist;

  int stlPartitionCnt;

  // specific partition/subpartition info
  ComTdbVirtTablePartInfo *partArray;

  long flags;
};

#endif /* COMTDB_H */
