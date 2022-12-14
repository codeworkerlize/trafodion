
#ifndef __EX_STATS_H__
#define __EX_STATS_H__

#include <sys/times.h>

#include "cli/Globals.h"
#include "cli/SQLCLIdev.h"
#include "cli/Statement.h"
#include "comexe/ComTdb.h"
#include "comexe/ComTdbStats.h"
#include "comexe/ComTdbUdr.h"
#include "common/ComCextdecs.h"
#include "common/ComGuardianFileNameParts.h"
#include "common/ComRtUtils.h"
#include "common/Int64.h"
#include "executor/ExExeUtil.h"
#include "executor/ExScheduler.h"
#include "executor/ex_stdh.h"
#include "executor/ex_tcb.h"
#include "exp/ExpLOBstats.h"
#include "runtimestats/SqlStats.h"
#include "runtimestats/ssmpipc.h"
#include "seabed/fs.h"

class ExStatisticsArea;
class ExOperStats;
class ExStatsCounter;
class ExClusterStats;
class ExTimeStats;
class ExFragRootOperStats;
class ExPartitionAccessStats;
class ExHashGroupByStats;
class ExHashJoinStats;
class ExESPStats;
class ExSplitTopStats;
class ExMeasBaseStats;
class ExMeasStats;
class ExSortStats;
class ExUDRStats;
class ExProbeCacheStats;
class ExMasterStats;
class ExRMSStats;
class ExBMOStats;
class ExUDRBaseStats;
class ExFastExtractStats;
class ExStorageEngineStats;
class ExObjectEpochStats;
class ExObjectLockStats;
class ExQryInvalidStats;
class ExQryInvalidStatsTcb;

typedef ExStorageEngineStats ExHbaseAccessStats;
typedef ExStorageEngineStats ExHdfsScanStats;

//////////////////////////////////////////////////////////////////
// forward classes
//////////////////////////////////////////////////////////////////
class Queue;
class HashQueue;
class NAMemory;
class ComTdb;
class IDInfo;
class ex_tcb;
class SsmpClientMsgStream;

#ifdef max
#undef max
#endif

#ifdef min
#undef min
#endif

#ifdef sum
#undef sum
#endif

#define _STATS_RTS_VERSION_R25_1 6
#define _STATS_RTS_VERSION_R25   5
#define _STATS_RTS_VERSION_R23_1 4
#define _STATS_RTS_VERSION_R23   3
#define _STATS_RTS_VERSION_R22   2
#define _STATS_RTS_VERSION       1
#define _STATS_PRE_RTS_VERSION   0

#define _UNINITIALIZED_TDB_ID 9999999
#define _NO_TDB_NAME          "NO_TDB"

const int StatsCurrVersion = _STATS_RTS_VERSION_R25_1;

//////////////////////////////////////////////////////////////////////
// this class is used to provide some utility methods (like, pack
// and unpack) that are used by all derived classes.
// Any utility virtual or non-virtual methods could be added here.
// DO NOT ADD ANY FIELDS TO THIS CLASS.
// We don't want to increase the size of any derived class.
//////////////////////////////////////////////////////////////////////
class ExStatsBase {
 public:
  virtual UInt32 pack(char *buffer) { return 0; };

  virtual void unpack(const char *&buffer){};

  UInt32 alignedPack(char *buffer);

  void alignedUnpack(const char *&buffer);

  void alignSizeForNextObj(UInt32 &size);

  void alignBufferForNextObj(const char *&buffer);
};

class ExStatsBaseNew : public ExStatsBase {
 public:
  virtual UInt32 pack(char *buffer) { return 0; };

  virtual void unpack(const char *&buffer){};

  NAMemory *getHeap() { return heap_; }
  ExStatsBaseNew(NAHeap *heap) { heap_ = heap; }

  ExStatsBaseNew() { heap_ = NULL; }

 protected:
  NAMemory *heap_;
};

//////////////////////////////////////////////////////////////////
// class ExStatsCounter
//
// a generic counter used in ExOperStats (and deerived classes).
// ExStatsCounter provides cnt, min, max, avg, sum, and var
//////////////////////////////////////////////////////////////////
class ExStatsCounter {
 public:
  ExStatsCounter();

  ~ExStatsCounter(){};

  ExStatsCounter &operator=(const ExStatsCounter &other);

  void addEntry(long value);

  int entryCnt() const { return entryCnt_; };

  long min() const { return (entryCnt_ ? min_ : 0); }

  long max() const { return (entryCnt_ ? max_ : 0); }

  long sum() const { return sum_; };

  float sum2() const { return sum2_; };

  void merge(ExStatsCounter *other);

  void init();

  //////////////////////////////////////////////////////////////////
  // the following methods are only used to finalize statistics
  // (display them). They are never used in DP2.
  //////////////////////////////////////////////////////////////////
  float mean();

  float variance();

 private:
  int entryCnt_;
  long min_;
  long max_;
  long sum_;    // sum of all values
  float sum2_;  // sum of the square of all values
};

//////////////////////////////////////////////////////////////////
// class ExClusterStats
//
// Statistics for operators which use clusters (HJ, HGB,
// materialize)
//////////////////////////////////////////////////////////////////
class ExClusterStats : public ExStatsBase {
 public:
  ExClusterStats();
  ExClusterStats(NABoolean isInner, int bucketCnt, long actRows, long totalSize, ExStatsCounter hashChains,
                 long writeIOCnt, long readIOCnt);

  ~ExClusterStats(){};

  ExClusterStats &operator=(const ExClusterStats &other);

  void getVariableStatsInfo(char *dataBuffer, char *datalen, int maxLen);

  void getVariableStatsInfoAggr(char *dataBuffer, char *datalen, int maxLen);

  //////////////////////////////////////////////////////////////////
  // accessors, mutators
  //////////////////////////////////////////////////////////////////
  // Returning const char *
  inline const char *getInner() const { return ((isInner_) ? "inner" : "outer"); };

  inline int getBucketCnt() const { return bucketCnt_; };

  inline long getActRows() const { return actRows_; };

  inline long getTotalSize() const { return totalSize_; };

  inline ExStatsCounter getHashChains() const { return hashChains_; };

  inline long getWriteIOCnt() const { return writeIOCnt_; };

  inline long getReadIOCnt() const { return readIOCnt_; };

  inline void setNext(ExClusterStats *i) { next_ = i; };

  inline ExClusterStats *getNext() const { return next_; };

  UInt32 packedLength();

  //////////////////////////////////////////////////////////////////
  // packs 'this' into a message.
  //////////////////////////////////////////////////////////////////
  UInt32 pack(char *buffer);

  void unpack(const char *&buffer);

 private:
  int version_;
  NABoolean isInner_;          // inner or outer Cluster
  int bucketCnt_;              // number of buckets in this cluster
  long actRows_;               // for this cluster
  long totalSize_;             // total size in bytes
  ExStatsCounter hashChains_;  // stats about the hash table
  long writeIOCnt_;            // write operations to scratch file
  long readIOCnt_;             // read operations from scratch file
  ExClusterStats *next_;
};

//////////////////////////////////////////////////////////////////
// class ExTimeStats
//
// ExTimeStats collects time information. Real-time, process or
// thread CPU time.
//////////////////////////////////////////////////////////////////
class ExTimeStats : public ExStatsBase {
  friend class ExOperStats;
  friend class ExHashJoinStats;

 public:
  ExTimeStats(clockid_t clk_id = CLOCK_THREAD_CPUTIME_ID);

  ~ExTimeStats() {}

  inline ExTimeStats &operator+(const ExTimeStats &other) {
    sumTime_ += other.sumTime_;

    return *this;
  }

  inline void incTime(long time) { sumTime_ += time; }

  // start an active period (record the starting time)
  void start();

  // stop an active period and record the spent time
  // returns increment Time
  long stop();

  inline void reset();

  inline long getTime() const { return sumTime_; }

  UInt32 packedLength();

  UInt32 pack(char *buffer);

  void unpack(const char *&buffer);
  void setVersion(int ver) { version_ = ver; }

  int filterForSEstats(struct timespec currTimespec);

 private:
  int version_;
  // aggregated times (microseconds) over multiple start/stop operations
  // This is computed for the process, but the merge of DP2 stats will
  // add to this also.
  long sumTime_;

  // start/stop indicator and timestamps of the latest start() operation
  NABoolean isStarted_;
  timespec startTime_;
  clockid_t clockid_;
};

//////////////////////////////////////////////////////////////////
// struct ExOperStatsId
//
// Data members that identify an ExOperStats uniquely within
// a given statement
//////////////////////////////////////////////////////////////////
struct ExOperStatsId {
  ExFragId fragId_;
  int tdbId_;
  int instNum_;
  int subInstNum_;

  // some methods that make use of this struct like a basic data type easier
  // fragID_ is a type ExFragId.  Set to 0, rather than a negative value
  ExOperStatsId() {
    fragId_ = 0;
    tdbId_ = -1;
    instNum_ = -1;
    subInstNum_ = -1;
  }
  ExOperStatsId(const ExOperStatsId &other)
      : fragId_(other.fragId_), tdbId_(other.tdbId_), instNum_(other.instNum_), subInstNum_(other.subInstNum_) {}

  /*
  operator ==(const ExOperStatsId &other) const
  { return (fragId_ == other.fragId_ && tdbId_ == other.tdbId_ &&
            instNum_ == other.instNum_ && subInstNum_ == other.subInstNum_); }
  */
  NABoolean compare(const ExOperStatsId &other, ComTdb::CollectStatsType cst) const;
};

// Define the number of retcodes here instead of taking the enum
// value WORK_LAST_RETCODE from file ExScheduler.h. The reason is
// that otherwise we couldn't change WORK_LAST_RETCODE without
// versioning implications, and that would be worse than the
// hard-coded literal here.
#define STATS_WORK_LAST_RETCODE      6
#define STATS_WORK_LAST_RETCODE_PREV 4

///////////////////////////////////////////
// class ExeSEStats
///////////////////////////////////////////
class ExeSEStats : public ExStatsBase {
 public:
  ExeSEStats() : accessedRows_(0), usedRows_(0), numIOCalls_(0), numIOBytes_(0), maxIOTime_(0), filteredRows_(0){};

  inline long getAccessedRows() const { return accessedRows_; }
  inline void setAccessedRows(long cnt) { accessedRows_ = cnt; }
  inline void incAccessedRows(long i = 1) { accessedRows_ += i; }

  inline long getUsedRows() const { return usedRows_; }
  inline void setUsedRows(long cnt) { usedRows_ = cnt; }
  inline void incUsedRows(long i = 1) { usedRows_ += +i; }

  inline long getNumIOCalls() const { return numIOCalls_; }
  inline void setNumIOCalls(long cnt) { numIOCalls_ = cnt; }
  inline void incNumIOCalls(long i = 1) { numIOCalls_ += +i; }

  inline long getNumIOBytes() const { return numIOBytes_; }
  inline void setNumIOBytes(long cnt) { numIOBytes_ = cnt; }
  inline void incNumIOBytes(long i = 1) { numIOBytes_ += +i; }

  inline long getMaxIOTime() const { return maxIOTime_; }
  inline void setMaxIOTime(long cnt) { maxIOTime_ = cnt; }
  inline void incMaxIOTime(long i = 1) { maxIOTime_ += +i; }

  inline long getFilteredRows() const { return filteredRows_; }
  inline void setFilteredRows(long cnt) { filteredRows_ = cnt; }
  inline void incFilteredRows(long i) { filteredRows_ += i; };

  void init(NABoolean resetDop) {
    accessedRows_ = 0;
    usedRows_ = 0;
    numIOCalls_ = 0;
    numIOBytes_ = 0;
    maxIOTime_ = 0;
    filteredRows_ = 0;
  }

  UInt32 packedLength();

  UInt32 pack(char *buffer);

  void merge(ExeSEStats *other);
  void merge(ExStorageEngineStats *other);

  void unpack(const char *&buffer);

  void copyContents(ExeSEStats *other);

 private:
  long accessedRows_;
  long usedRows_;
  long numIOCalls_;
  long numIOBytes_;
  long maxIOTime_;
  long filteredRows_;
};

/////////////////////////////////////////////////////////////////////
// This class keeps track of the statistics related to the
// messages that are sent/received between exe/esp or fs/dp2.
// Used by ExPartitionAccessStats and ExEspStats.
// It contains:
//  -- compile time size estimate of sent/recvd buffers
//  -- number of buffers that are sent/recvd
//  -- Total number of bytes that are sent/recvd. This number
//     could be less than the estimated size if dense buffers are
//     used.
//  -- number of statistics info related bytes that are recvd
//     (this number is included in Total number of bytes sent/recvd
//  -- sent/recvd counters (See ExStatsCounter)
/////////////////////////////////////////////////////////////////////
class ExBufferStats : public ExStatsBase {
 public:
  ExBufferStats() : sendBufferSize_(0), recdBufferSize_(0), totalSentBytes_(0), totalRecdBytes_(0), statsBytes_(0){};

  int &sendBufferSize() { return sendBufferSize_; }

  int &recdBufferSize() { return recdBufferSize_; }

  long &totalSentBytes() { return totalSentBytes_; }

  long &totalRecdBytes() { return totalRecdBytes_; }

  long &statsBytes() { return statsBytes_; }

  ExStatsCounter &sentBuffers() { return sentBuffers_; }

  ExStatsCounter &recdBuffers() { return recdBuffers_; }

  void init(NABoolean resetDop) {
    totalSentBytes_ = 0;
    totalRecdBytes_ = 0;
    statsBytes_ = 0;

    sentBuffers_.init();
    recdBuffers_.init();
  }

  UInt32 packedLength();

  UInt32 pack(char *buffer);

  //////////////////////////////////////////////////////////////////
  // merge two ExBufferStats of the same type. Merging accumulated
  // the counters of other into this.
  //////////////////////////////////////////////////////////////////
  void merge(ExBufferStats *other);

  void unpack(const char *&buffer);

  void copyContents(ExBufferStats *other);

 private:
  int sendBufferSize_;
  int recdBufferSize_;
  long totalSentBytes_;
  long totalRecdBytes_;

  // number of bytes out of the totalRecdBytes used for statistics data.
  long statsBytes_;

  ExStatsCounter sentBuffers_;  // counts buffers sent and the
                                // used data bytes in these buffers
  ExStatsCounter recdBuffers_;  // counts buffers received and the
                                // used data bytes in these buffers
};

//////////////////////////////////////////////////////////////////
// class ExOperStats
//
// ExOperStats collects basic statistics for each tcb. Classes
// derived from ExOperStats collect more operator specific
// statistics
//////////////////////////////////////////////////////////////////
class ExOperStats : public ExStatsBaseNew {
  friend class ExMasterStats;

 public:
  //////////////////////////////////////////////////////////////////
  // StatType enumerates the existing variants of ExOperStats. If
  // you want to add  a new type, add it here, too.
  //////////////////////////////////////////////////////////////////
  enum StatType {
    EX_OPER_STATS = SQLSTATS_DESC_OPER_STATS,
    ROOT_OPER_STATS = SQLSTATS_DESC_ROOT_OPER_STATS,
    PARTITION_ACCESS_STATS = SQLSTATS_DESC_PARTITION_ACCESS_STATS,
    GROUP_BY_STATS = SQLSTATS_DESC_GROUP_BY_STATS,
    HASH_JOIN_STATS = SQLSTATS_DESC_HASH_JOIN_STATS,
    PROBE_CACHE_STATS = SQLSTATS_DESC_PROBE_CACHE_STATS,
    ESP_STATS = SQLSTATS_DESC_ESP_STATS,
    SPLIT_TOP_STATS = SQLSTATS_DESC_SPLIT_TOP_STATS,
    MEAS_STATS = SQLSTATS_DESC_MEAS_STATS,
    SORT_STATS = SQLSTATS_DESC_SORT_STATS,
    UDR_STATS = SQLSTATS_DESC_UDR_STATS,
    NO_OP = SQLSTATS_DESC_NO_OP,
    MASTER_STATS = SQLSTATS_DESC_MASTER_STATS,
    RMS_STATS = SQLSTATS_DESC_RMS_STATS,
    BMO_STATS = SQLSTATS_DESC_BMO_STATS,
    UDR_BASE_STATS = SQLSTATS_DESC_UDR_BASE_STATS,
    REPLICATE_STATS = SQLSTATS_DESC_REPLICATE_STATS,
    REPLICATOR_STATS = SQLSTATS_DESC_REPLICATOR_STATS,
    FAST_EXTRACT_STATS = SQLSTATS_DESC_FAST_EXTRACT_STATS,
    REORG_STATS = SQLSTATS_DESC_REORG_STATS,
    SE_STATS = SQLSTATS_DESC_SE_STATS,
    HDFSSCAN_STATS = SQLSTATS_DESC_SE_STATS,
    HBASE_ACCESS_STATS = SQLSTATS_DESC_SE_STATS,
    PROCESS_STATS = SQLSTATS_DESC_PROCESS_STATS,
    OBJECT_EPOCH_STATS = SQLSTATS_DESC_OBJECT_EPOCH_STATS,
    OBJECT_LOCK_STATS = SQLSTATS_DESC_OBJECT_LOCK_STATS,
    QUERY_INVALIDATION_STATS = SQLSTATS_DESC_QUERY_INVALIDATION_STATS,
  };

  //////////////////////////////////////////////////////////////////
  // constructor, destructor
  // Keep a heap pointer for dynamic allocations.
  //////////////////////////////////////////////////////////////////
  ExOperStats(NAMemory *heap, StatType statType, ex_tcb *tcb, const ComTdb *tdb);

  // second constructor is used only when unpacking objects from a message
  ExOperStats(NAMemory *heap, StatType statType = EX_OPER_STATS);

  ExOperStats(NAMemory *heap, StatType statType, ComTdb::CollectStatsType collectStatsType, ExFragId fragId, int tdbId,
              int explainTdbId, int instNum, ComTdb::ex_node_type tdbType, char *tdbName, int tdbNameLen);
  ExOperStats();

  ~ExOperStats();

  //////////////////////////////////////////////////////////////////
  // Accessors, mutators
  //////////////////////////////////////////////////////////////////

  inline StatType statType() const { return statType_; }

  inline short dop() const { return dop_; }

  inline short subReqType() { return subReqType_; }

  inline void setSubReqType(short subReqType) { subReqType_ = subReqType; }

  inline void restoreDop() {
    if (statsInTcb()) dop_ = savedDop_;
  }

  inline void incDop() {
    dop_++;
    if (statsInTcb()) savedDop_++;
  }

  inline int getExplainNodeId() const { return explainNodeId_; }

  inline queue_index getDownQueueSize() const { return allStats.downQueueSize_; }

  inline void setDownQueueSize(queue_index size) { allStats.downQueueSize_ = size; }

  inline queue_index getUpQueueSize() const { return allStats.upQueueSize_; }

  inline void setUpQueueSize(queue_index size) { allStats.upQueueSize_ = size; }

  inline ExStatsCounter &getDownQueueStats() { return allStats.downQueueStats_; }

  inline ExStatsCounter &getUpQueueStats() { return allStats.upQueueStats_; }

  inline int getParentTdbId() const { return (int)parentTdbId_; }

  inline int getLeftChildTdbId() const { return (int)leftChildTdbId_; }

  inline int getRightChildTdbId() const { return (int)rightChildTdbId_; }

  inline int getNTProcessId() const { return (int)allStats.ntProcessId_; }

  inline void setParentTdbId(int id) { parentTdbId_ = id; }

  inline void setLeftChildTdbId(int id) { leftChildTdbId_ = id; }

  inline void setRightChildTdbId(int id) { rightChildTdbId_ = id; }

  inline void setPertableStatsId(int id) { pertableStatsId_ = id; }

  inline int getPertableStatsId() { return pertableStatsId_; }

  inline const ExOperStatsId *getId() const { return &id_; }

  inline int getFragId() const { return (int)id_.fragId_; }

  inline int getTdbId() const { return id_.tdbId_; }

  inline int getInstNum() const { return id_.instNum_; }

  inline int getSubInstNum() const { return id_.subInstNum_; }

  inline void setSubInstNum(int n) { id_.subInstNum_ = n; }

  inline char *getTdbName() { return tdbName_; }

  inline ComTdb::ex_node_type getTdbType() const { return tdbType_; }

  inline ComTdb::CollectStatsType getCollectStatsType() const { return (ComTdb::CollectStatsType)collectStatsType_; }

  inline void setCollectStatsType(ComTdb::CollectStatsType s) { collectStatsType_ = s; }

  virtual const char *getStatTypeAsString() const { return "EX_OPER_STATS"; };

  inline long getActualRowsReturned() const { return actualRowsReturned_; }

  inline void setActualRowsReturned(long cnt) { actualRowsReturned_ = cnt; }

  inline void incActualRowsReturned(long i = 1) { actualRowsReturned_ = actualRowsReturned_ + i; }

  inline long getEstimatedRowsReturned() const { return u.estRowsReturned_; }

  inline void setEstimatedRowsReturned(long cnt) { u.estRowsReturned_ = cnt; }

  inline Float32 getEstRowsUsed() const { return u.est.estRowsUsed_; }

  inline void setEstRowsUsed(Float32 cnt) { u.est.estRowsUsed_ = cnt; }

  inline Float32 getEstRowsAccessed() const { return u.est.estRowsAccessed_; }

  inline void setEstRowsAccessed(Float32 cnt) { u.est.estRowsAccessed_ = cnt; }

  inline long getNumberCalls() const { return numberCalls_; }

  inline void setNumberCalls(long cnt) { numberCalls_ = cnt; }

  inline void incNumberCalls() { numberCalls_++; }

  //////////////////////////////////////////////////////////////////
  // reset all counters in the stats. Init affects only counters
  // and does NOT reset the tdbName and similar data members.
  //////////////////////////////////////////////////////////////////
  virtual void init(NABoolean resetDop);

  virtual void done();

  virtual void addMessage(int x);
  virtual void addMessage(const char *);
  virtual void addMessage(const char *, int);

  //////////////////////////////////////////////////////////////////
  // subTaskReturn is used by the scheduler to set the return code
  // of a tcb
  //////////////////////////////////////////////////////////////////
  void subTaskReturn(ExWorkProcRetcode rc);

  //////////////////////////////////////////////////////////////////
  // calculate the packed length. This is usually just
  // sizeof(*this). If the object has dynamically allocated data
  // members (strings, arrays, other obejcts) packedLength() has to
  // be adjusted accordingly
  //////////////////////////////////////////////////////////////////
  virtual UInt32 packedLength();

  //////////////////////////////////////////////////////////////////
  // packs 'this' into a message. Converts pointers to offsets.
  //////////////////////////////////////////////////////////////////
  virtual UInt32 pack(char *buffer);

  virtual void unpack(const char *&buffer);

  //////////////////////////////////////////////////////////////////
  // merge two ExOperStats of the same type. Merging accumulated
  // the counters of other into this.
  //////////////////////////////////////////////////////////////////
  void merge(ExOperStats *other);

  //////////////////////////////////////////////////////////////////
  // copies the content of other to this. copyContent does NOT
  // merge counters.
  //////////////////////////////////////////////////////////////////
  void copyContents(ExOperStats *other);

  //////////////////////////////////////////////////////////////////
  // allocates a new ExOperStats object on heap and copies this
  // to the new object.
  //////////////////////////////////////////////////////////////////
  virtual ExOperStats *copyOper(NAMemory *heap);

  /////////////////////////////////////////////////////////////////
  // cast to more specific ExStatsEntry classes.
  //////////////////////////////////////////////////////////////////
  virtual ExMeasStats *castToExMeasStats();

  virtual ExMeasBaseStats *castToExMeasBaseStats();

  virtual ExFragRootOperStats *castToExFragRootOperStats();

  virtual ExPartitionAccessStats *castToExPartitionAccessStats();

  virtual ExProbeCacheStats *castToExProbeCacheStats();

  virtual ExFastExtractStats *castToExFastExtractStats();

  virtual ExHdfsScanStats *castToExHdfsScanStats();

  virtual ExHbaseAccessStats *castToExHbaseAccessStats();

  virtual ExHashGroupByStats *castToExHashGroupByStats();

  virtual ExHashJoinStats *castToExHashJoinStats();

  virtual ExESPStats *castToExESPStats();
  virtual ExSplitTopStats *castToExSplitTopStats();

  virtual ExSortStats *castToExSortStats();

  virtual ExeSEStats *castToExeSEStats() { return NULL; }

  virtual ExUDRStats *castToExUDRStats();

  virtual ExMasterStats *castToExMasterStats();

  virtual ExBMOStats *castToExBMOStats();
  virtual ExUDRBaseStats *castToExUDRBaseStats();
  virtual ExObjectEpochStats *castToExObjectEpochStats() { return NULL; }
  virtual ExObjectLockStats *castToExObjectLockStats() { return NULL; }

  virtual ExQryInvalidStats *castToExQryInvalidStats() { return NULL; }
  ExTimeStats *getTimer() { return &operTimer_; }

  inline void incCpuTime(long cpuTime){};

  NABoolean operator==(ExOperStats *other);

  long getHashData(UInt16 statsMergeType = SQLCLI_SAME_STATS);

  //////////////////////////////////////////////////////////////////
  // format stats for display
  //////////////////////////////////////////////////////////////////

  // return 3 characteristics counters for this operator and a short text
  // identification what the counter means
  virtual const char *getNumValTxt(int i) const;

  virtual long getNumVal(int i) const;

  virtual const char *getTextVal();

  // this method returns the variable part of stats related information.
  // This info is delimited by tokens. Each Stats class redefines this
  // method.
  // It returns data in dataBuffer provided by caller and the length
  // of data(2 bytes short) in location pointed to by datalen.
  // This method is called by ExStatsTcb::work().
  virtual void getVariableStatsInfo(char *dataBuffer, char *datalen, int maxLen);
  virtual int getStatsItem(SQLSTATS_ITEM *sqlStats_item);

  void setCpuStatsHistory() { return; }

  void setVersion(short version) { version_ = version; }

  short getVersion() { return version_; }

  NABoolean statsInDp2() { return (flags_ & STATS_IN_DP2) != 0; }

  void setStatsInDp2(NABoolean v) { (v ? flags_ |= STATS_IN_DP2 : flags_ &= ~STATS_IN_DP2); }

  NABoolean statsInEsp() { return (flags_ & STATS_IN_ESP) != 0; }

  void setStatsInEsp(NABoolean v) { (v ? flags_ |= STATS_IN_ESP : flags_ &= ~STATS_IN_ESP); }

  NABoolean statsInTcb() { return (flags_ & STATS_IN_TCB) != 0; }

  void setStatsInTcb(NABoolean v) { (v ? flags_ |= STATS_IN_TCB : flags_ &= ~STATS_IN_TCB); }

  NABoolean hasSentMsgIUD() { return (flags_ & MSG_SENT_IUD) != 0; }

  void setHasSentMsgIUD() { flags_ |= MSG_SENT_IUD; }

  void clearHasSentMsgIUD() { flags_ &= ~MSG_SENT_IUD; }

  void initTdbForRootOper();

  void setQueryId(char *queryId, int queryIdLen) {}

  UInt32 getRecordLength() { return recordLength_; }

 private:
  enum Flags { STATS_IN_DP2 = 0x0001, STATS_IN_ESP = 0x0002, STATS_IN_TCB = 0x0004, MSG_SENT_IUD = 0x0008 };

  short version_;
  short subReqType_;

  StatType statType_;
  // Using the implicit gap in the structure to accomodate the new field
  short dop_;
  short savedDop_;
  // this ID is assigned at runtime. It is used to identify
  // the stats area. It never changes once it is assigned, except
  // that in some cases the caller alters the subInstNum_ immediately
  // after the constructor call.
  struct ExOperStatsId id_;
  int parentTdbId_;
  int leftChildTdbId_;
  int rightChildTdbId_;
  int explainNodeId_;
  int pertableStatsId_;

  char tdbName_[MAX_TDB_NAME_LEN + 1];
  ComTdb::ex_node_type tdbType_;

  UInt16 collectStatsType_;
  UInt16 flags_;

  ExTimeStats operTimer_;

  union {
    // optimizer estimate of rows returned by this operator
    long estRowsReturned_;

    struct {
      Float32 estRowsAccessed_;
      Float32 estRowsUsed_;
    } est;
  } u;

  // actual rows returned by this operator at runtime
  long actualRowsReturned_;
  // Number of times this operator was called
  long numberCalls_;

  char processNameString_[40];  // PROCESSNAME_STRING_LEN in ComRtUtils.h

  UInt32 recordLength_;  // record length

  struct {
    int ntProcessId_;
    queue_index downQueueSize_;
    queue_index upQueueSize_;
    ExStatsCounter downQueueStats_;
    ExStatsCounter upQueueStats_;

    // no filler is strictly needed here, but we pack and send this
    // filler in messages, which may come in handy some day
    long fillerForFutureUse_;

    // counters for the different return codes returned by the work
    // procedure of a tcb. These counters are incremented by the
    // scheduler via subTaskReturn()
    int retCodes_[STATS_WORK_LAST_RETCODE + 2];
  } allStats;
};

class ExBMOStats : public ExOperStats {
  friend class ExMeasStats;
  friend class ExFragRootOperStats;

 public:
  ExBMOStats(NAMemory *heap);
  ExBMOStats(NAMemory *heap, StatType statType);
  ExBMOStats(NAMemory *heap, StatType statType, ex_tcb *tcb, const ComTdb *tdb);
  ExBMOStats(NAMemory *heap, ex_tcb *tcb, const ComTdb *tdb);
  void init(NABoolean resetDop);
  UInt32 packedLength();
  UInt32 pack(char *buffer);
  void unpack(const char *&buffer);
  void deleteMe() {}
  ExOperStats *copyOper(NAMemory *heap);
  void copyContents(ExBMOStats *other);
  virtual const char *getNumValTxt(int i) const;
  virtual long getNumVal(int i) const;
  void getVariableStatsInfo(char *dataBuffer, char *dataLen, int maxLen);
  ExBMOStats *castToExBMOStats();
  void merge(ExBMOStats *other);
  int getStatsItem(SQLSTATS_ITEM *sqlStats_item);
  inline void setScratchBufferBlockSize(int size) { scratchBufferBlockSize_ = size >> 10; }
  inline void incSratchFileCount() { scratchFileCount_++; }
  inline void incScratchBufferBlockRead(int count = 1) { scratchBufferBlockRead_ += count; }
  inline void incScratchBufferBlockWritten(int count = 1) { scratchBufferBlockWritten_ += count; }
  inline void incScratchReadCount(int count = 1) { scratchReadCount_ += count; }
  inline void incScratchWriteCount(int count = 1) { scratchWriteCount_ += count; }
  inline void incScratchIOMaxTime(long v) { scratchIOMaxTime_ += v; }
  inline void setSpaceBufferSize(int size) { spaceBufferSize_ = size >> 10; }
  inline void setSpaceBufferCount(int count) { spaceBufferCount_ = count; }
  inline void updateBMOHeapUsage(NAHeap *heap) {
    bmoHeapAlloc_ = (int)(heap->getTotalSize() >> 10);
    bmoHeapUsage_ = (int)(heap->getAllocSize() >> 10);
    bmoHeapWM_ = (int)(heap->getHighWaterMark() >> 10);
    if (bmoHeapAlloc_ > bmoHeapWM_) bmoHeapWM_ = bmoHeapAlloc_;
  }
  inline void setScratchOverflowMode(Int16 overflowMode) { scratchOverflowMode_ = overflowMode; }
  inline void setTopN(int size) { topN_ = size; }
  inline long getScratchReadCount(void) { return scratchReadCount_; }
  static const char *getScratchOverflowMode(Int16 overflowMode);
  ExTimeStats &getScratchIOTimer() { return timer_; }
  inline void setScratchIOSize(long size) { scratchIOSize_ = size; }
  const char *getBmoPhaseStr();
  inline void setBmoPhase(Int16 phase) { phase_ = phase; }
  inline Int16 getBmoPhase() { return phase_; }
  void resetInterimRowCount() { interimRowCount_ = 0; }
  void incInterimRowCount() { interimRowCount_++; }

  ScanFilterStatsList *getScanFilterStats() { return &scanFilterStats_; }

 private:
  ExTimeStats timer_;
  int bmoHeapAlloc_;
  int bmoHeapUsage_;
  int bmoHeapWM_;
  int spaceBufferSize_;
  int spaceBufferCount_;
  int scratchFileCount_;
  int scratchBufferBlockSize_;
  int scratchBufferBlockRead_;
  int scratchBufferBlockWritten_;
  int scratchIOSize_;
  long scratchReadCount_;
  long scratchWriteCount_;
  long scratchIOMaxTime_;
  Int16 scratchOverflowMode_;  // 0 - disk 1 - SSD
  int topN_;                   // TOPN value
  Float32 estMemoryUsage_;
  long interimRowCount_;
  Int16 phase_;

  ScanFilterStatsList scanFilterStats_;
};

/////////////////////////////////////////////////////////////////
// class ExFragRootOperStats
/////////////////////////////////////////////////////////////////
class ExFragRootOperStats : public ExOperStats {
  friend class ExMeasStats;

 public:
  ExFragRootOperStats(NAMemory *heap, ex_tcb *tcb, const ComTdb *tdb);

  ExFragRootOperStats(NAMemory *heap);

  ExFragRootOperStats(NAMemory *heap, ComTdb::CollectStatsType collectStatsType, ExFragId fragId, int tdbId,
                      int explainNodeId, int instNum, ComTdb::ex_node_type tdbType, char *tdbName, int tdbNameLen);

  ~ExFragRootOperStats();

  void init(NABoolean resetDop);

  UInt32 packedLength();

  //////////////////////////////////////////////////////////////////
  // packs 'this' into a message. Converts pointers to offsets.
  //////////////////////////////////////////////////////////////////
  UInt32 pack(char *buffer);

  void unpack(const char *&buffer);

  void copyContents(ExFragRootOperStats *other);

  void merge(ExOperStats *other);
  void merge(ExFragRootOperStats *other);
  void merge(ExUDRBaseStats *other);
  void merge(ExBMOStats *other);

  ExOperStats *copyOper(NAMemory *heap);
  /////////////////////////////////////////////////////////////////
  // accessors, mutators
  /////////////////////////////////////////////////////////////////
  inline const SB_Phandle_Type *getPhandle() const { return (const SB_Phandle_Type *)&phandle_; }

  inline SB_Phandle_Type *getPhandlePtr() { return &phandle_; }

  bool isFragSuspended() const { return isFragSuspended_; }

  void setFragSuspended(bool s) { isFragSuspended_ = s; }

  inline long getCpuTime() const { return cpuTime_; }

  inline long getLocalCpuTime() const { return localCpuTime_; }

  inline void incCpuTime(long cpuTime) {
    cpuTime_ += cpuTime;
    localCpuTime_ += cpuTime;
  }

  inline void setCpuTime(long cpuTime) { cpuTime_ = cpuTime; }

  inline long getMaxSpaceUsage() const { return spaceUsage_; }

  inline long getMaxHeapUsage() const { return heapUsage_; }
  inline int getStmtIndex() const { return stmtIndex_; }

  inline void setStmtIndex(int i) { stmtIndex_ = i; }

  inline long getTimestamp() const { return timestamp_; }

  inline void setTimestamp(long t) { timestamp_ = t; }

  inline void updateSpaceUsage(Space *space, CollHeap *heap) {
    spaceUsage_ = (int)(space->getAllocSize() >> 10);
    spaceAlloc_ = (int)(space->getTotalSize() >> 10);
    heapUsage_ = (int)(heap->getAllocSize() >> 10);
    heapAlloc_ = (int)(heap->getTotalSize() >> 10);
    heapWM_ = (int)(heap->getHighWaterMark() >> 10);
  }

  ExFragRootOperStats *castToExFragRootOperStats();

  virtual const char *getNumValTxt(int i) const;

  virtual long getNumVal(int i) const;

  virtual const char *getTextVal();

  virtual void getVariableStatsInfo(char *dataBuffer, char *datalen, int maxLen);
  void initHistory();

  int getExecutionCount() const { return executionCount_; }

  inline int getNewprocess() { return newprocess_; }
  inline void setNewprocess(int n) { newprocess_ = n; }
  inline void incNewprocess(int n = 1) { newprocess_ = newprocess_ + n; }

  inline long getNewprocessTime() { return newprocessTime_; }
  inline void setNewprocessTime(long t) { newprocessTime_ = t; }
  inline void incNewprocessTime(long t) { newprocessTime_ = newprocessTime_ + t; }

  inline long getEspCpuTime() const { return espCpuTime_; }

  int getStatsItem(SQLSTATS_ITEM *sqlStats_item);
  void setCpuStatsHistory() { histCpuTime_ = cpuTime_; }

  NABoolean filterForCpuStats();
  void setQueryId(char *queryId, int queryIdLen) {
    queryId_ = queryId;
    queryIdLen_ = queryIdLen;
  }
  char *getQueryId() { return queryId_; }
  int getQueryIdLen() { return queryIdLen_; }
  inline void incReqMsg(long msgBytes) {
    reqMsgCnt_++;
    reqMsgBytes_ += msgBytes;
  }
  inline void incReplyMsg(long msgBytes) {
    replyMsgCnt_++;
    replyMsgBytes_ += msgBytes;
  }

  inline long getPagesInUse() { return pagesInUse_; }
  inline void setPagesInUse(long pagesInUse) { pagesInUse_ = pagesInUse; }
  inline void incWaitTime(long incWaitTime) {
    waitTime_ += incWaitTime;
    maxWaitTime_ = waitTime_;
  }
  inline long getAvgWaitTime() {
    if (dop() == 0)
      return -1;
    else
      return waitTime_ / dop();
  }
  inline long getMaxWaitTime() { return maxWaitTime_; }

  NABoolean hdfsAccess() { return (flags_ & HDFS_ACCESS) != 0; }
  void setHdfsAccess(NABoolean v) { (v ? flags_ |= HDFS_ACCESS : flags_ &= ~HDFS_ACCESS); }

  ScanFilterStatsList *getScanFilterStats() { return &scanFilterStats_; }

 private:
  enum Flags { HDFS_ACCESS = 0x0001 };

  // some heap statistics for the entire fragment instance
  int spaceUsage_;
  int spaceAlloc_;
  int heapUsage_;
  int heapAlloc_;
  int heapWM_;
  long cpuTime_;
  Int16 scratchOverflowMode_;
  int newprocess_;
  long newprocessTime_;
  int espSpaceUsage_;
  int espSpaceAlloc_;
  int espHeapUsage_;
  int espHeapAlloc_;
  int espHeapWM_;
  long espCpuTime_;
  long histCpuTime_;
  long reqMsgCnt_;
  long reqMsgBytes_;
  long replyMsgCnt_;
  long replyMsgBytes_;
  long pagesInUse_;
  // Helps with cancel escalation.  Local only.  Do not merge.
  int executionCount_;
  int stmtIndex_;   // Statement index used by Measure
  long timestamp_;  // timestamp indicating when the statement executed
                    // (master executor only)
  char *queryId_;
  int queryIdLen_;
  int scratchFileCount_;
  int spaceBufferSize_;
  long spaceBufferCount_;
  int scratchIOSize_;
  long scratchReadCount_;
  long scratchWriteCount_;
  long interimRowCount_;
  long scratchIOMaxTime_;
  long udrCpuTime_;
  int topN_;
  // process id of this fragment instance (to correlate it with MEASURE data)
  // Also used by logic on runtimestats/CancelBroker.cpp
  SB_Phandle_Type phandle_;
  // This is aggregated only for the process.  It is never merged into or
  // from.
  long localCpuTime_;

  // Set to true and reset to false by the MXSSCP process under the
  // stats global semaphore.  Read by master and ESP EXE without the
  // semaphore.
  bool isFragSuspended_;
  long maxWaitTime_;
  long waitTime_;
  long diffCpuTime_;

  int flags_;

  ScanFilterStatsList scanFilterStats_;
};

/////////////////////////////////////////////////////////////////
// class ExPartitionAccessStats
/////////////////////////////////////////////////////////////////
class ExPartitionAccessStats : public ExOperStats {
 public:
  ExPartitionAccessStats(NAMemory *heap, ex_tcb *tcb, ComTdb *tdb, int bufferSize);

  ExPartitionAccessStats(NAMemory *heap);

  ~ExPartitionAccessStats();

  void init(NABoolean resetDop);

  void copyContents(ExPartitionAccessStats *other);

  void merge(ExPartitionAccessStats *other);

  ExOperStats *copyOper(NAMemory *heap);

  UInt32 packedLength();

  //////////////////////////////////////////////////////////////////
  // packs 'this' into a message. Converts pointers to offsets.
  //////////////////////////////////////////////////////////////////
  UInt32 pack(char *buffer);

  void unpack(const char *&buffer);

  /////////////////////////////////////////////////////////////////
  // accessors, mutators
  /////////////////////////////////////////////////////////////////
  inline char *ansiName() const { return ansiName_; }

  inline char *fileName() const { return fileName_; }

  ExBufferStats *bufferStats() { return &bufferStats_; }

  ExPartitionAccessStats *castToExPartitionAccessStats();

  virtual const char *getNumValTxt(int i) const;

  virtual const char *getTextVal();

  virtual long getNumVal(int i) const;
  ExeSEStats *castToExeSEStats() { return exeSEStats(); }

  ExeSEStats *exeSEStats() { return &seStats_; }

  inline int getOpens() { return opens_; }
  inline void setOpens(int o) { opens_ = o; }
  inline void incOpens(int o = 1) { opens_ += o; }

  inline long getOpenTime() { return openTime_; }
  inline void setOpenTime(long t) { openTime_ = t; }
  inline void incOpenTime(long t) { openTime_ += t; }

  int getStatsItem(SQLSTATS_ITEM *sqlStats_item);

  virtual void getVariableStatsInfo(char *dataBuffer, char *datalen, int maxLen);

 private:
  ExeSEStats seStats_;
  ExBufferStats bufferStats_;

  char *ansiName_;
  char *fileName_;
  int opens_;
  long openTime_;
};

/////////////////////////////////////////////////////////////////
// class ExProbeCacheStats
/////////////////////////////////////////////////////////////////
class ExProbeCacheStats : public ExOperStats {
 public:
  ExProbeCacheStats(NAMemory *heap, ex_tcb *tcb, ComTdb *tdb, int bufferSize, int numCacheEntries);

  ExProbeCacheStats(NAMemory *heap);

  ~ExProbeCacheStats(){};

  void init(NABoolean resetDop);

  void merge(ExProbeCacheStats *other);

  void copyContents(ExProbeCacheStats *other);

  ExOperStats *copyOper(NAMemory *heap);

  UInt32 packedLength();

  //////////////////////////////////////////////////////////////////
  // packs 'this' into a message. Converts pointers to offsets.
  //////////////////////////////////////////////////////////////////
  UInt32 pack(char *buffer);

  void unpack(const char *&buffer);

  /////////////////////////////////////////////////////////////////
  // accessors, mutators
  /////////////////////////////////////////////////////////////////
  inline void incHit() { ++cacheHits_; }

  inline void incMiss() { ++cacheMisses_; }

  inline void incCanceledHit() { ++canceledHits_; }

  inline void incCanceledMiss() { ++canceledMisses_; }

  inline void incCanceledNotStarted() { ++canceledNotStarted_; }

  inline void updateLongChain(UInt32 clen) {
    if (clen > longestChain_) longestChain_ = clen;
  }
  inline void updateUseCount(UInt32 uc) {
    if (uc > highestUseCount_) highestUseCount_ = uc;
  }
  inline void newChain() {
    if (++numChains_ > maxNumChains_) maxNumChains_ = numChains_;
  }

  inline void freeChain() { --numChains_; }

  int longestChain() const { return longestChain_; }

  int highestUseCount() const { return highestUseCount_; }

  int maxNumChains() const { return maxNumChains_; }

  ExProbeCacheStats *castToExProbeCacheStats();

  virtual const char *getNumValTxt(int i) const;

  virtual long getNumVal(int i) const;

  virtual void getVariableStatsInfo(char *dataBuffer, char *datalen, int maxLen);

 private:
  int cacheHits_;
  int cacheMisses_;
  int canceledHits_;
  int canceledMisses_;
  int canceledNotStarted_;
  int longestChain_;
  int numChains_;  // supports maxNumChains_.
  int maxNumChains_;
  int highestUseCount_;
  int bufferSize_;       // static for now.
  int numCacheEntries_;  // static for now.
};

//////////////////////////////////////////////////////////////////
// class ExHashGroupByStats
//////////////////////////////////////////////////////////////////
class ExHashGroupByStats : public ExBMOStats {
 public:
  ExHashGroupByStats(NAMemory *heap, ex_tcb *tcb, const ComTdb *tdb);

  ExHashGroupByStats(NAMemory *heap);

  ~ExHashGroupByStats(){};

  void init(NABoolean resetDop);

  void copyContents(ExHashGroupByStats *other);

  void merge(ExHashGroupByStats *other);

  UInt32 packedLength();

  //////////////////////////////////////////////////////////////////
  // packs 'this' into a message. Converts pointers to offsets.
  //////////////////////////////////////////////////////////////////
  UInt32 pack(char *buffer);

  void unpack(const char *&buffer);

  ExOperStats *copyOper(NAMemory *heap);

  ExHashGroupByStats *castToExHashGroupByStats();

  ExBMOStats *castToExBMOStats();

  void incPartialGroupsReturned() { partialGroups_++; };

  virtual const char *getNumValTxt(int i) const;

  virtual long getNumVal(int i) const;

  virtual void getVariableStatsInfo(char *dataBuffer, char *datalen, int maxLen);

  // just add the pointer as last element to cluster stats list
  void addClusterStats(ExClusterStats *clusterStats);

  // make a copy of the clusterstats and add them to the list
  void addClusterStats(ExClusterStats clusterStats);

  // delete all the clusterstats
  void deleteClusterStats();

  inline void updMemorySize(long memSize) {
    if (memSize_ < memSize) memSize_ = memSize;
  }

  inline void updIoSize(long newSize) { ioSize_ = newSize; }

 private:
  long partialGroups_;
  long memSize_;  // max amount of memory used (bytes)
  long ioSize_;   // number of bytes transferred from and to disk
  int clusterCnt_;
  ExClusterStats *clusterStats_;
  ExClusterStats *lastStat_;
};

//////////////////////////////////////////////////////////////////
// class ExHashJoinStats
//////////////////////////////////////////////////////////////////
class ExHashJoinStats : public ExBMOStats {
 public:
  ExHashJoinStats(NAMemory *heap, ex_tcb *tcb, const ComTdb *tdb);

  ExHashJoinStats(NAMemory *heap);

  ~ExHashJoinStats(){};

  void init(NABoolean resetDop);

  void copyContents(ExHashJoinStats *other);

  void merge(ExHashJoinStats *other);

  UInt32 packedLength();

  //////////////////////////////////////////////////////////////////
  // packs 'this' into a message. Converts pointers to offsets.
  //////////////////////////////////////////////////////////////////
  UInt32 pack(char *buffer);

  void unpack(const char *&buffer);

  ExOperStats *copyOper(NAMemory *heap);

  ExHashJoinStats *castToExHashJoinStats();

  ExBMOStats *castToExBMOStats();

  virtual const char *getNumValTxt(int i) const;

  virtual long getNumVal(int i) const;

  virtual void getVariableStatsInfo(char *dataBuffer, char *datalen, int maxLen);

  // just add the pointer as last element to cluster stats list
  void addClusterStats(ExClusterStats *clusterStats);

  // make a copy of the clusterstats and add them to the list
  void addClusterStats(ExClusterStats clusterStats);

  // delete all the clusterstats
  void deleteClusterStats();

  // set the phase of the HJ so that we charge the right time statistics
  // note that we only can increae the phase number (makes sense, right?).
  inline void incPhase() { phase_++; };

  inline void incEmptyChains(int i = 1) { emptyChains_ += i; };

  inline int getEmptyChains() const { return emptyChains_; };

  inline void updMemorySize(long memSize) {
    if (memSize_ < memSize) memSize_ = memSize;
  }

  inline void updIoSize(long newSize) { ioSize_ = newSize; }

  inline void incrClusterSplits() { clusterSplits_++; }
  inline void incrHashLoops() { hashLoops_++; }

 private:
  ExTimeStats phaseTimes_[3];
  short phase_;   // indicates which phase to charge with times
  long memSize_;  // max. amount of memory bytes used
  long ioSize_;   // number of bytes transferred from and to disk
  int emptyChains_;
  int clusterCnt_;
  int clusterSplits_;  // how many times clusters were split
  int hashLoops_;      // how many hash loops were performed
  ExClusterStats *clusterStats_;
  ExClusterStats *lastStat_;
};

//////////////////////////////////////////////////////////////////
// class ExESPStats
//////////////////////////////////////////////////////////////////
class ExESPStats : public ExOperStats {
 public:
  ExESPStats(NAMemory *heap, int sendBufferSize, int recBufferSize, int subInstNum, ex_tcb *tcb, const ComTdb *tdb);

  ExESPStats(NAMemory *heap);

  ~ExESPStats(){};

  void init(NABoolean resetDop);

  void copyContents(ExESPStats *other);

  void merge(ExESPStats *other);

  UInt32 packedLength();

  //////////////////////////////////////////////////////////////////
  // packs 'this' into a message. Converts pointers to offsets.
  //////////////////////////////////////////////////////////////////
  UInt32 pack(char *buffer);

  void unpack(const char *&buffer);

  ExOperStats *copyOper(NAMemory *heap);

  inline long getSendTopStatID() const { return sendTopStatID_; }

  inline void setSendTopStatID(long id) { sendTopStatID_ = id; }

  ExBufferStats *bufferStats() { return &bufferStats_; }

  ExESPStats *castToExESPStats();

  virtual const char *getNumValTxt(int i) const;

  virtual long getNumVal(int i) const;

  virtual void getVariableStatsInfo(char *dataBuffer, char *datalen, int maxLen);

 private:
  ExBufferStats bufferStats_;

  long sendTopStatID_;  // used for send bottoms to make
                        // the connection to the corresponding
                        // send tops
};

//////////////////////////////////////////////////////////////////
// class ExSplitTopStats
//////////////////////////////////////////////////////////////////
class ExSplitTopStats : public ExOperStats {
 public:
  ExSplitTopStats(NAMemory *heap, ex_tcb *tcb, const ComTdb *tdb);

  ExSplitTopStats(NAMemory *heap);

  ~ExSplitTopStats(){};

  void init(NABoolean resetDop);

  void merge(ExSplitTopStats *other);

  UInt32 packedLength();

  //////////////////////////////////////////////////////////////////
  // packs 'this' into a message. Converts pointers to offsets.
  //////////////////////////////////////////////////////////////////
  UInt32 pack(char *buffer);

  void unpack(const char *&buffer);

  ExOperStats *copyOper(NAMemory *heap);

  void copyContents(ExSplitTopStats *other);

  inline int getMaxChildren() const { return maxChildren_; }

  inline int getActChildren() const { return actChildren_; }

  ExSplitTopStats *castToExSplitTopStats();

  virtual const char *getNumValTxt(int i) const;

  virtual long getNumVal(int i) const;

  virtual void getVariableStatsInfo(char *dataBuffer, char *datalen, int maxLen);

  NABoolean isPapa() { return (flags_ & IS_PAPA) != 0; }

 private:
  enum {
    // PAPA or EspExchange
    IS_PAPA = 0x0001
  };

  // this is the maximum number of children(SendTop or PA) that
  // this operator will communicate with.
  int maxChildren_;

  // this is the actual number of children(SendTop or PA) that this
  // operator communicated with during query execution.
  int actChildren_;

  int flags_;
};

//////////////////////////////////////////////////////////////////
// class ExSortStats
//////////////////////////////////////////////////////////////////
class ExSortStats : public ExBMOStats {
 public:
  ExSortStats(NAMemory *heap, ex_tcb *tcb, const ComTdb *tdb);

  ExSortStats(NAMemory *heap);

  ~ExSortStats(){};

  void init(NABoolean resetDop);

  void copyContents(ExSortStats *other);

  void merge(ExSortStats *other);

  UInt32 packedLength();

  //////////////////////////////////////////////////////////////////
  // packs 'this' into a message. Converts pointers to offsets.
  //////////////////////////////////////////////////////////////////
  UInt32 pack(char *buffer);

  void unpack(const char *&buffer);

  ExOperStats *copyOper(NAMemory *heap);

  ExSortStats *castToExSortStats();

  ExBMOStats *castToExBMOStats();

  virtual const char *getNumValTxt(int i) const;

  virtual long getNumVal(int i) const;

  virtual void getVariableStatsInfo(char *dataBuffer, char *datalen, int maxLen);

  UInt32 &runSize() { return runSize_; }
  UInt32 &numRuns() { return numRuns_; }
  UInt32 &numCompares() { return numCompares_; }
  UInt32 &numDupRecs() { return numDupRecs_; }

  UInt32 &scrBlockSize() { return scrBlockSize_; }
  UInt32 &scrNumBlocks() { return scrNumBlocks_; }
  UInt32 &scrNumWrites() { return scrNumWrites_; }
  UInt32 &scrNumReads() { return scrNumReads_; }
  UInt32 &scrNumAwaitio() { return scrNumAwaitio_; }

 private:
  // for explanation of these sort stats, see sort/Statistics.h
  UInt32 runSize_;
  UInt32 numRuns_;
  UInt32 numCompares_;
  UInt32 numDupRecs_;

  UInt32 scrBlockSize_;
  UInt32 scrNumBlocks_;
  UInt32 scrNumWrites_;
  UInt32 scrNumReads_;
  UInt32 scrNumAwaitio_;
};

/////////////////////////////////////////////////////////////////
//// class ExStorageEngineStats
///////////////////////////////////////////////////////////////////

const int FILTERINFO_LENGTH_TERSE = 300;
const int FILTERINFO_LENGTH_DETAIL = (FILTERINFO_LENGTH_TERSE + 300);

class ExStorageEngineStats : public ExOperStats {
 public:
  ExStorageEngineStats(NAMemory *heap, ex_tcb *tcb,
                       ComTdb *tdb);  // tbd - other, specific params?

  ExStorageEngineStats(NAMemory *heap);

  ~ExStorageEngineStats();

  void init(NABoolean resetDop);

  void merge(ExStorageEngineStats *other);

  void copyContents(ExStorageEngineStats *other);

  ExOperStats *copyOper(NAMemory *heap);

  UInt32 packedLength();

  //////////////////////////////////////////////////////////////////
  //// packs 'this' into a message. Converts pointers to offsets.
  ////////////////////////////////////////////////////////////////////
  UInt32 pack(char *buffer);

  void unpack(const char *&buffer);

  /////////////////////////////////////////////////////////////////
  //// accessors, mutators
  ///////////////////////////////////////////////////////////////////
  char *tableName() const { return tableName_; }

  ExTimeStats &getHdfsTimer() { return timer_; }
  ExTimeStats &getHbaseTimer() { return timer_; }

  inline void incBytesRead(long bytesRead) { numBytesRead_ += bytesRead; }

  inline void incAccessedRows() { ++accessedRows_; }
  inline void incAccessedRows(long v) { accessedRows_ += v; }

  inline void incUsedRows() { ++usedRows_; }
  inline void incUsedRows(long v) { usedRows_ += v; }

  void incFilteredRows() { incFilteredRows(1); }
  void incFilteredRows(long v) { filteredRows_ += v; };

  inline void incMaxHdfsIOTime(long v) { maxIOTime_ += v; }
  inline void incMaxHbaseIOTime(long v) { maxIOTime_ += v; }

  long numBytesRead() const { return numBytesRead_; }
  long rowsAccessed() const { return accessedRows_; }
  long rowsUsed() const { return usedRows_; }
  long filteredRows() const { return filteredRows_; }

  NABoolean filterForSEstats(struct timespec currTimespec, int filter);

  long maxHdfsIOTime() const { return maxIOTime_; }
  long maxHbaseIOTime() const { return maxIOTime_; }

  ExHdfsScanStats *castToExHdfsScanStats();

  ExHbaseAccessStats *castToExHbaseAccessStats();

  virtual const char *getNumValTxt(int i) const;

  virtual long getNumVal(int i) const;

  virtual void getVariableStatsInfo(char *dataBuffer, char *datalen, int maxLen);

  int getStatsItem(SQLSTATS_ITEM *sqlStats_item);

  void setQueryId(char *queryId, int queryIdLen) {
    queryId_ = queryId;
    queryIdLen_ = queryIdLen;
  }
  char *getQueryId() { return queryId_; }
  int getQueryIdLen() { return queryIdLen_; }

  inline void incHdfsCalls(long i = 1) { numIOCalls_ += i; }
  inline void incHbaseCalls(long i = 1) { numIOCalls_ += i; }

  long hdfsCalls() const { return numIOCalls_; }
  long hbaseCalls() const { return numIOCalls_; }

  void addScanFilterStats(ScanFilterStats &stats, ScanFilterStats::MergeSemantics);

  // protected:
  ScanFilterStatsList *getScanFilterStats() { return &scanFilterStats_; }

 private:
  ExTimeStats timer_;
  char *tableName_;
  long numBytesRead_;
  long accessedRows_;
  long usedRows_;
  long numIOCalls_;
  long maxIOTime_;
  long filteredRows_;
  char *queryId_;
  int queryIdLen_;
  int blockTime_;

  ScanFilterStatsList scanFilterStats_;
};

/////////////////////////////////////////////////////////////////
// class ExMeasBaseStats
/////////////////////////////////////////////////////////////////
class ExMeasBaseStats : public ExOperStats {
 public:
  ExMeasBaseStats(NAMemory *heap, StatType statType, ex_tcb *tcb, const ComTdb *tdb);

  ExMeasBaseStats(NAMemory *heap, StatType statType);

  ~ExMeasBaseStats(){};

  virtual void init(NABoolean resetDop);

  virtual UInt32 packedLength();

  //////////////////////////////////////////////////////////////////
  // packs 'this' into a message. Converts pointers to offsets.
  //////////////////////////////////////////////////////////////////
  virtual UInt32 pack(char *buffer);

  virtual void unpack(const char *&buffer);

  void merge(ExMeasBaseStats *other);

  void copyContents(ExMeasBaseStats *other);

  virtual ExeSEStats *castToExeSEStats() { return exeSEStats(); }

  ExeSEStats *exeSEStats() { return &seStats_; }

  virtual void getVariableStatsInfo(char *dataBuffer, char *datalen, int maxLen);

  virtual const char *getNumValTxt(int i) const;

  virtual long getNumVal(int i) const;

  void setVersion(int version);
  /*
  inline int getOpens()          { return opens_; }
  inline void setOpens(int o)    { opens_ = o; }
  inline void incOpens(int o = 1)    { opens_ = opens_ + o; }

  inline long getOpenTime()          { return openTime_; }
  inline void setOpenTime(long t)    { openTime_ = t; }
  inline void incOpenTime(long t)    { openTime_ = openTime_ + t; }
  */
  ExMeasBaseStats *castToExMeasBaseStats();

 protected:
  /*
    FsDp2MsgsStats fsDp2MsgsStats_;
    ExeDp2Stats exeDp2Stats_;

    UInt16 filler1_;
    int opens_;
    long openTime_;
  */
  ExeSEStats seStats_;
};

/////////////////////////////////////////////////////////////////
// class ExMeasStats
/////////////////////////////////////////////////////////////////
class ExMeasStats : public ExMeasBaseStats {
 public:
  ExMeasStats(NAMemory *heap, ex_tcb *tcb, const ComTdb *tdb);

  ExMeasStats(NAMemory *heap);

  ~ExMeasStats();

  ExMeasStats *castToExMeasStats();

  void init(NABoolean resetDop);

  UInt32 packedLength();

  //////////////////////////////////////////////////////////////////
  // packs 'this' into a message. Converts pointers to offsets.
  //////////////////////////////////////////////////////////////////
  UInt32 pack(char *buffer);

  void unpack(const char *&buffer);

  void merge(ExOperStats *other);
  void merge(ExMeasStats *other);
  void merge(ExFragRootOperStats *other);
  void merge(ExUDRBaseStats *other);
  void merge(ExBMOStats *other);
  void merge(ExStorageEngineStats *other);

  ExOperStats *copyOper(NAMemory *heap);

  void copyContents(ExMeasStats *other);

  virtual void getVariableStatsInfo(char *dataBuffer, char *datalen, int maxLen);
  void updateSpaceUsage(Space *space, CollHeap *heap);

  inline int getNewprocess() { return newprocess_; }
  inline void setNewprocess(int n) { newprocess_ = n; }
  inline void incNewprocess(int n = 1) { newprocess_ = newprocess_ + n; }

  inline long getNewprocessTime() { return newprocessTime_; }
  inline void setNewprocessTime(long t) { newprocessTime_ = t; }
  inline void incNewprocessTime(long t) { newprocessTime_ = newprocessTime_ + t; }
  inline int getTimeouts() { return timeouts_; }
  inline void setTimeouts(int t) { timeouts_ = t; }
  virtual const char *getNumValTxt(int i) const;

  inline int getNumSorts() { return numSorts_; }
  inline void setNumSorts(int n) { numSorts_ = n; }
  inline void incNumSorts(int n = 1) { numSorts_ = numSorts_ + n; }

  inline long getSortElapsedTime() { return sortElapsedTime_; }
  inline void setSortElapsedTime(long t) { sortElapsedTime_ = t; }
  inline void incSortElapsedTime(long t) { sortElapsedTime_ = sortElapsedTime_ + t; }

  virtual long getNumVal(int i) const;

  void initHistory();

  bool isFragSuspended() const { return isFragSuspended_; }

  void setFragSuspended(bool s) { isFragSuspended_ = s; }

  inline long getCpuTime() const { return cpuTime_; }

  inline long getLocalCpuTime() const { return localCpuTime_; }

  inline void incCpuTime(long cpuTime) {
    cpuTime_ += cpuTime;
    localCpuTime_ += cpuTime;
  }

  inline void setCpuTime(long cpuTime) { cpuTime_ = cpuTime; }

  int getStatsItem(SQLSTATS_ITEM *sqlStats_item);
  void setCpuStatsHistory() { histCpuTime_ = cpuTime_; }
  NABoolean filterForCpuStats();
  void setQueryId(char *queryId, int queryIdLen) {
    queryId_ = queryId;
    queryIdLen_ = queryIdLen;
  }
  char *getQueryId() { return queryId_; }
  int getQueryIdLen() { return queryIdLen_; }
  inline void incReqMsg(long msgBytes) {
    reqMsgCnt_++;
    reqMsgBytes_ += msgBytes;
  }
  inline void incReplyMsg(long msgBytes) {
    replyMsgCnt_++;
    replyMsgBytes_ += msgBytes;
  }
  int getExecutionCount() const { return executionCount_; }

  inline const SB_Phandle_Type *getPhandle() const { return (const SB_Phandle_Type *)&phandle_; }

 private:
  int newprocess_;
  long newprocessTime_;
  int timeouts_;
  int numSorts_;
  long sortElapsedTime_;
  // some heap statistics for the entire fragment instance
  int spaceUsage_;
  int spaceAlloc_;
  int heapUsage_;
  int heapAlloc_;
  int heapWM_;
  long cpuTime_;
  int espSpaceUsage_;
  int espSpaceAlloc_;
  int espHeapUsage_;
  int espHeapAlloc_;
  int espHeapWM_;
  long espCpuTime_;
  long histCpuTime_;
  char *queryId_;
  int queryIdLen_;
  long reqMsgCnt_;
  long reqMsgBytes_;
  long replyMsgCnt_;
  long replyMsgBytes_;
  // Helps with cancel escalation.  Local only.  Do not merge.
  int executionCount_;
  // Used by logic on runtimestats/CancelBroker.cpp (cancel escalation).
  // Local copy, do not merge.
  SB_Phandle_Type phandle_;
  // Set to true and reset to false by the MXSSCP process under the
  // stats global semaphore.  Read by master and ESP EXE without the
  // semaphore.
  bool isFragSuspended_;

  long localCpuTime_;
  Int16 scratchOverflowMode_;
  int scratchFileCount_;
  int spaceBufferSize_;
  long spaceBufferCount_;
  long scratchReadCount_;
  long scratchWriteCount_;
  long udrCpuTime_;
  int topN_;
  int scratchIOSize_;
  long interimRowCount_;
  long scratchIOMaxTime_;
};

class ExUDRBaseStats : public ExOperStats {
  friend class ExMeasStats;
  friend class ExFragRootOperStats;

 public:
  ExUDRBaseStats(NAMemory *heap);
  ExUDRBaseStats(NAMemory *heap, StatType statType);
  ExUDRBaseStats(NAMemory *heap, StatType statType, ex_tcb *tcb, const ComTdb *tdb);
  ExUDRBaseStats(NAMemory *heap, ex_tcb *tcb, const ComTdb *tdb);
  void init(NABoolean resetDop);
  UInt32 packedLength();
  UInt32 pack(char *buffer);
  void unpack(const char *&buffer);
  void deleteMe() {}
  ExOperStats *copyOper(NAMemory *heap);
  void copyContents(ExUDRBaseStats *other);
  void getVariableStatsInfo(char *dataBuffer, char *dataLen, int maxLen);
  ExUDRBaseStats *castToExUDRBaseStats();
  void merge(ExUDRBaseStats *other);
  int getStatsItem(SQLSTATS_ITEM *sqlStats_item);
  void setUDRServerId(const char *serverId, int maxlen);
  inline const char *getUDRServerId() const { return UDRServerId_; }
  void incReplyMsg(long msgBytes) {
    replyMsgCnt_++;
    replyMsgBytes_ += msgBytes;
    recentReplyTS_ = NA_JulianTimestamp();
  }

  void incReqMsg(long msgBytes) {
    reqMsgCnt_++;
    reqMsgBytes_ += msgBytes;
    recentReqTS_ = NA_JulianTimestamp();
  }

 private:
  long reqMsgCnt_;
  long reqMsgBytes_;
  long replyMsgCnt_;
  long replyMsgBytes_;
  long udrCpuTime_;
  char UDRServerId_[40];  // PROCESSNAME_STRING_LEN in ComRtUtils.h
  long recentReqTS_;
  long recentReplyTS_;
};

//////////////////////////////////////////////////////////////////
// class ExUDRStats
//////////////////////////////////////////////////////////////////

class ExUDRStats : public ExUDRBaseStats {
 public:
  ExUDRStats(NAMemory *heap, int sendBufferSize, int recBufferSize, const ComTdb *tdb, ex_tcb *tcb);

  ExUDRStats(NAMemory *heap);

  ~ExUDRStats();

  void init(NABoolean resetDop);

  void copyContents(ExUDRStats *other);

  void merge(ExUDRStats *other);

  ExOperStats *copyOper(NAMemory *heap);

  UInt32 packedLength();

  //////////////////////////////////////////////////////////////////
  // packs 'this' into a message. Converts pointers to offsets.
  //////////////////////////////////////////////////////////////////

  UInt32 pack(char *buffer);

  void unpack(const char *&buffer);

  /////////////////////////////////////////////////////////////////
  // accessors, mutators
  /////////////////////////////////////////////////////////////////
  ExBufferStats *bufferStats() { return &bufferStats_; }

  inline ExStatsCounter &getSentControlBuffers() { return sentControlBuffers_; }

  inline ExStatsCounter &getSentContinueBuffers() { return sentContinueBuffers_; }

  inline void setUDRServerInit(long i = 0) { UDRServerInit_ = i; }

  inline void setUDRServerStart(long s = 0) { UDRServerStart_ = s; }

  inline const char *getUDRName() const { return UDRName_; }

  void setUDRName(const char *udrName, int maxlen);

  ExUDRStats *castToExUDRStats();

  virtual const char *getNumValTxt(int i) const;

  virtual long getNumVal(int i) const;

  virtual const char *getTextVal();

  virtual void getVariableStatsInfo(char *dataBuffer, char *datalen, int maxLen);

 private:
  ExBufferStats bufferStats_;           // request and reply buffer statistics
  ExStatsCounter sentControlBuffers_;   // counts control msg buffers and bytes
  ExStatsCounter sentContinueBuffers_;  // counts continue msg buffers and bytes

  long UDRServerInit_;   // UDR Server initialization time
  long UDRServerStart_;  // UDR Server successfull start time
  char *UDRName_;        // UDR name registered in CREATE
};

///////////////////////////////////////////////////////////////////
// class ExStatisticsArea
//
// ExStatisticsArea is a list (or more precisely a hash queue) of
// ExOperStats
///////////////////////////////////////////////////////////////////

class ExStatisticsArea : public IpcMessageObj {
 public:
  ExStatisticsArea(NAMemory *heap = NULL, int sendBottomNum = 0, ComTdb::CollectStatsType cst = ComTdb::ALL_STATS,
                   ComTdb::CollectStatsType origCst = ComTdb::NO_STATS);
  ~ExStatisticsArea();

  void removeEntries();

  //////////////////////////////////////////////////////////////////
  // Accessors, mutators
  //////////////////////////////////////////////////////////////////
  inline void setHeap(NAMemory *heap) { heap_ = heap; }

  void initEntries();

  void restoreDop();

  int numEntries();

  inline NAMemory *getHeap() { return heap_; }

  inline NABoolean sendStats() {
    sendBottomCnt_++;
    return (sendBottomCnt_ == sendBottomNum_);
  }

  void allocDynamicStmtCntrs(const char *stmtName);

  //////////////////////////////////////////////////////////////////
  // Merges statistics from 'stats' to the the corresponding
  // entry in the entries_. Searches for matching statID.
  // Returns TRUE, if matching entry found. FALSE, if not found.
  //////////////////////////////////////////////////////////////////
  NABoolean merge(ExOperStats *stats, UInt16 statsMergeType = SQLCLIDEV_SAME_STATS);

  //////////////////////////////////////////////////////////////////
  // merges all entries of statArea. If entries in statArea
  // are not present in 'this', appends the new entries.
  //////////////////////////////////////////////////////////////////
  NABoolean merge(ExStatisticsArea *otherStatArea, UInt16 statsMergeType = SQLCLIDEV_SAME_STATS);

  //////////////////////////////////////////////////////////////////
  // inserts a new entry, stats, to 'this'
  //////////////////////////////////////////////////////////////////
  NABoolean insert(ExOperStats *stats);

  //////////////////////////////////////////////////////////////////
  // positions to the head of ExOperStats list
  //////////////////////////////////////////////////////////////////
  void position();

  //////////////////////////////////////////////////////////////////
  // positions to the entry with the hash data passed in.
  //////////////////////////////////////////////////////////////////
  void position(char *hashData, int hashDatalen);

  //////////////////////////////////////////////////////////////////
  // get the next entry
  //////////////////////////////////////////////////////////////////
  ExOperStats *getNext();

  ExOperStats *get(const ExOperStatsId &id);

  // gets the next 'type' of stat entry with 'operType'
  ExOperStats *get(ExOperStats::StatType type, ComTdb::ex_node_type tdbType);

  // gets the next 'type' of stat entry with 'tdbId'
  ExOperStats *get(ExOperStats::StatType type, int tdbId);

  // gets the next stat entry with 'tdbId'
  ExOperStats *get(int tdbId);

  IpcMessageObjSize packedLength();

  //////////////////////////////////////////////////////////////////
  // packs 'this' into a message. Converts pointers to offsets.
  //////////////////////////////////////////////////////////////////
  IpcMessageObjSize packObjIntoMessage(IpcMessageBufferPtr buffer);

  IpcMessageObjSize packSmallObjIntoMessage(IpcMessageBufferPtr buffer);

  void unpackObj(IpcMessageObjType objType, IpcMessageObjVersion objVersion, NABoolean sameEndianness,
                 IpcMessageObjSize objSize, IpcConstMessageBufferPtr buffer);

  void unpackObjFromEid(IpcConstMessageBufferPtr buffer, ExOperStats *parentStatsEntry = NULL, int parentTdb = -1);

  void unpackSmallObjFromEid(IpcConstMessageBufferPtr buffer, int version = _STATS_PRE_RTS_VERSION);

  long getExplainPlanId() const { return explainPlanId_; }

  void setExplainPlanId(long pid) { explainPlanId_ = pid; }

  ComTdb::CollectStatsType getCollectStatsType() { return (ComTdb::CollectStatsType)collectStatsType_; };
  void setCollectStatsType(ComTdb::CollectStatsType s) { collectStatsType_ = s; };

  ComTdb::CollectStatsType getOrigCollectStatsType() { return (ComTdb::CollectStatsType)origCollectStatsType_; };

  //////////////////////////////////////////////////////////////////
  // scan all ExOperStats entries and update stmtCntrs.
  //////////////////////////////////////////////////////////////////

  int updateStmtCntrs(ExMeasStmtCntrs *stmtCntrs, int statementCount, char *moduleName, int moduleNameLen);

  //////////////////////////////////////////////////////////////////
  // get Opens counter from  ExMeasStats entry.
  //////////////////////////////////////////////////////////////////
  int getMeasOpensCntr();
  long getMeasOpenTimeCntr();

  //////////////////////////////////////////////////////////////////
  // get Newprocess counter from  ExMeasStats entry.
  //////////////////////////////////////////////////////////////////
  int getMeasNewprocessCntr();
  long getMeasNewprocessTimeCntr();

  NABoolean statsInDp2() { return (detailedFlags_.smallFlags_ & STATS_IN_DP2) != 0; }

  void setStatsInDp2(NABoolean v) {
    (v ? detailedFlags_.smallFlags_ |= STATS_IN_DP2 : detailedFlags_.smallFlags_ &= ~STATS_IN_DP2);
  }

  NABoolean statsInEsp() { return (detailedFlags_.smallFlags_ & STATS_IN_ESP) != 0; }

  void setStatsInEsp(NABoolean v) {
    (v ? detailedFlags_.smallFlags_ |= STATS_IN_ESP : detailedFlags_.smallFlags_ &= ~STATS_IN_ESP);
  }

  NABoolean smallStatsObj() { return (detailedFlags_.smallFlags_ & SMALL_STATS_OBJ) != 0; }

  void setSmallStatsObj(NABoolean v) {
    (v ? detailedFlags_.smallFlags_ |= SMALL_STATS_OBJ : detailedFlags_.smallFlags_ &= ~SMALL_STATS_OBJ);
  }

  NABoolean statsEnabled() { return (detailedFlags_.otherFlags_ & STATS_ENABLED) != 0; }

  void setStatsEnabled(NABoolean v) {
    (v ? detailedFlags_.otherFlags_ |= STATS_ENABLED : detailedFlags_.otherFlags_ &= ~STATS_ENABLED);
  }

  NABoolean rtsStatsCollectEnabled() { return (detailedFlags_.otherFlags_ & RTS_STATS_COLLECT_ENABLED) != 0; }

  void setRtsStatsCollectEnabled(NABoolean v) {
    (v ? detailedFlags_.otherFlags_ |= RTS_STATS_COLLECT_ENABLED
       : detailedFlags_.otherFlags_ &= ~RTS_STATS_COLLECT_ENABLED);
  }

  void setDonotUpdateCounters(NABoolean v) {
    (v ? detailedFlags_.smallFlags_ |= DONT_UPDATE_COUNTERS : detailedFlags_.smallFlags_ &= ~DONT_UPDATE_COUNTERS);
  }
  NABoolean donotUpdateCounters() { return (detailedFlags_.smallFlags_ & DONT_UPDATE_COUNTERS) != 0; }
  void setMasterStats(ExMasterStats *masterStats);

  ExMasterStats *getMasterStats() { return masterStats_; }

  void fixup(ExStatisticsArea *other);
  void updateSpaceUsage(Space *space, CollHeap *heap);
  void initHistoryEntries();

  ExOperStats *getRootStats() { return rootStats_; };
  void setRootStats(ExOperStats *root) { rootStats_ = root; }
  int getStatsItems(int no_of_stats_items, SQLSTATS_ITEM sqlStats_items[]);
  int getStatsDesc(short *statsCollectType,
                   /* IN/OUT */ SQLSTATS_DESC sqlstats_desc[],
                   /* IN */ int max_stats_desc,
                   /* OUT */ int *no_returned_stats_desc);
  static const char *getStatsTypeText(short statsType);
  void setCpuStatsHistory();
  NABoolean appendCpuStats(ExStatisticsArea *other, NABoolean appendAlways = FALSE);
  NABoolean appendCpuStats(ExMasterStats *masterStats, NABoolean appendAlways, short subReqType, int etTimeFilter,
                           long currTimestamp);  // Return TRUE if the stats is appended
  NABoolean appendCpuStats(ExOperStats *stat, NABoolean appendAlways, int filter,
                           struct timespec currTimespec);  // Return TRUE if the stats is appended
  NABoolean appendCpuStats(ExStatisticsArea *stats, NABoolean appendAlways, int filter,
                           struct timespec currTimespec);  // Return TRUE if the stats is appended
  void incReqMsg(long msgBytes);
  void incReplyMsg(long msgBytes);
  void setDetailLevel(short level) { detailLevel_ = level; }
  void setSubReqType(short subReqType) { subReqType_ = subReqType; }
  short getSubReqType() { return subReqType_; }
  void setQueryId(char *queryId, int queryIdLen);
  long getHashData(ExOperStats::StatType type, int tdbId);

  NABoolean anyHaveSentMsgIUD();

 private:
  void unpackThisClass(const char *&buffer, ExOperStats *parentStatsEntry = NULL, int parentTdb = -1);

  enum SmallFlags {
    STATS_IN_DP2 = 0x01,
    SMALL_STATS_OBJ = 0x02,
    STATS_IN_ESP = 0x04,
    DONT_UPDATE_COUNTERS = 0x08  // Don't update counters in the scheduler once the stats is shipped
  };

  enum OtherFlags {
    // This flag is set by executor before returning statistics to cli,
    // if stats area was allocated AND stats collection was enabled
    // (that is, statistics were actually collected).
    // Statistics collection could be enabled or disabled by application.
    // Currently, stats collection is disabled if measure stats was chosen
    // at compile time but measure was not up or stmt counters were not
    // being collected at runtime.
    STATS_ENABLED = 0x0001,
    RTS_STATS_COLLECT_ENABLED = 0x0002
  };

  IDInfo *IDLookup(HashQueue *hq, long id);

  void preProcessStats();

  NAMemory *heap_;
  HashQueue *entries_;

  // some info that relates to the entire statement
  // (used only in the master fragment)
  long explainPlanId_;

  // the following 2 members are only meaningful in an ESP.
  // The split-bottom sets the sendBottomNum_ member. Whenever
  // a send-bottom sees an EOF, it increments the sendBottomCnt_.
  // Statistics are only sent by the "last" send-bottom
  // (sendBottomCnt == sendBottomNum)
  int sendBottomNum_;
  int sendBottomCnt_;

  // ---------------------------------------------------------------------
  // this fields contains details about what
  // kind of statistics are to be collected.
  // ---------------------------------------------------------------------
  UInt16 collectStatsType_;
  UInt16 origCollectStatsType_;
  struct DetailedFlags {
    unsigned char smallFlags_;
    unsigned char filler;
    UInt16 otherFlags_;
  };
  union {
    UInt32 flags_;
    DetailedFlags detailedFlags_;
  };

  ExMasterStats *masterStats_;
  ExOperStats *rootStats_;  // ExOperStats entry that keeps track of memory usage
  short detailLevel_;       // Detail Level for SQLCLI_QID_DETAIL_STATS
  short subReqType_;
  char filler_[16];
};

//////////////////////////////////////////////////////////////////
// the following class is used to:
// 1 - map statIDs to more redeable ids
// 2 - build the tree of TCBs for detailed statistics display
// IDInfo is used only during the preparation of the stats for
// display
//////////////////////////////////////////////////////////////////
class IDInfo : public NABasicObject {
 public:
  IDInfo(long id, ExOperStats *stat);

  long newId_;
  ExOperStats *stat_;
  // count the number of child TCBs processed during the tree building
  int childrenProcessed_;
};

// -----------------------------------------------------------------------
// ExStatsTdb
// -----------------------------------------------------------------------
class ExStatsTdb : public ComTdbStats {
 public:
  // ---------------------------------------------------------------------
  // Constructor is only called to instantiate an object used for
  // retrieval of the virtual table function pointer of the class while
  // unpacking. An empty constructor is enough.
  // ---------------------------------------------------------------------
  ExStatsTdb() {}

  virtual ~ExStatsTdb() {}

  // ---------------------------------------------------------------------
  // Build a TCB for this TDB. Redefined in the Executor project.
  // ---------------------------------------------------------------------
  virtual ex_tcb *build(ex_globals *globals);

 private:
  // ---------------------------------------------------------------------
  // !!!!!!! IMPORTANT -- NO DATA MEMBERS ALLOWED IN EXECUTOR TDB !!!!!!!!
  // *********************************************************************
  // The Executor TDB's are only used for the sole purpose of providing a
  // way to supplement the Compiler TDB's (in comexe) with methods whose
  // implementation depends on Executor objects. This is done so as to
  // decouple the Compiler from linking in Executor objects unnecessarily.
  //
  // When a Compiler generated TDB arrives at the Executor, the same data
  // image is "cast" as an Executor TDB after unpacking. Therefore, it is
  // a requirement that a Compiler TDB has the same object layout as its
  // corresponding Executor TDB. As a result of this, all Executor TDB's
  // must have absolutely NO data members, but only member functions. So,
  // if you reach here with an intention to add data members to a TDB, ask
  // yourself two questions:
  //
  // 1. Are those data members Compiler-generated?
  //    If yes, put them in the ComTdbStats instead.
  //    If no, they should probably belong to someplace else (like TCB).
  //
  // 2. Are the classes those data members belong defined in the executor
  //    project?
  //    If your answer to both questions is yes, you might need to move
  //    the classes to the comexe project.
  // ---------------------------------------------------------------------
};

class ExStatsTcb : public ex_tcb {
 public:
  // Constructor called during build phase (ExStatsTdb::build()).
  ExStatsTcb(const ExStatsTdb &statsTdb, ex_globals *glob);

  // Default destructor
  ~ExStatsTcb();

  // Free resources (Don't know exactly what this should do)
  void freeResources();

  // The work procedure for ExStatsTcb.
  // For details see ExStats.cpp
  short work();

  // The queue pair used to communicate with the parent TCB
  ex_queue_pair getParentQueue() const { return qparent_; };

  // A virtual function used by the GUI.  Will always return 0 for
  // ExStatsTcb
  virtual int numChildren() const { return 0; };
  virtual const ex_tcb *getChild(int pos) const { return NULL; };
  ExStatisticsArea *sendToSsmp();
  int parse_stmt_name(char *string, int len);
  ComDiagsArea *getDiagsArea() { return diagsArea_; }

  int str_parse_stmt_name(char *string, int len, char *nodeName, short *cpu, pid_t *pid, long *timeStamp,
                          int *queryNumber, short *qidOffset, short *qidLen, short *activeQueryNum,
                          UInt16 *statsMergeType, short *detailLevel, short *subReqType, int *filterTimeInSecs);
  enum StatsStep {
    INITIAL_,
    GET_NEXT_STATS_ENTRY_,
    APPLY_SCAN_EXPR_,
    PROJECT_,
    ERROR_,
    DONE_,
    SEND_TO_SSMP_,
    GET_MASTER_STATS_ENTRY_,
  };
  void getColumnValues(ExOperStats *stat);

 private:
  NABoolean deleteStats() { return (flags_ & STATS_DELETE_IN_TCB_) != 0; }

  void setDeleteStats(NABoolean v) { (v ? flags_ |= STATS_DELETE_IN_TCB_ : flags_ &= ~STATS_DELETE_IN_TCB_); }

  enum Flags {
    STATS_DELETE_IN_TCB_ = 0x0001,
  };
  // A reference to the corresponding TDB (ExStatsTdb)
  ComTdbStats &statsTdb() const { return (ComTdbStats &)tdb; };

  // Queues used to communicate with the parent TCB.
  ex_queue_pair qparent_;

  // this is where the stats row will be created.
  tupp tuppData_;
  char *data_;

  atp_struct *workAtp_;

  ExStatisticsArea *stats_;

  char *inputModName_;
  char *inputStmtName_;

  char *qid_;
  pid_t pid_;
  short cpu_;
  long timeStamp_;
  int queryNumber_;
  char nodeName_[MAX_SEGMENT_NAME_LEN + 1];
  short reqType_;
  short retryAttempts_;
  ComDiagsArea *diagsArea_;
  short activeQueryNum_;
  UInt16 statsMergeType_;
  int flags_;
  short detailLevel_;
  char *stmtName_;
  short subReqType_;
  int filter_;
};

class ExStatsPrivateState : public ex_tcb_private_state {
  friend class ExStatsTcb;

 public:
  ExStatsPrivateState();
  ~ExStatsPrivateState();
  ex_tcb_private_state *allocate_new(const ex_tcb *tcb);

 private:
  ExStatsTcb::StatsStep step_;

  long matchCount_;  // number of rows returned for this parent row
};

//////////////////////////////////////////////////////////////////
// class ExMasterStats
//////////////////////////////////////////////////////////////////
class ExMasterStats : public ExOperStats {
 public:
  ExMasterStats(NAHeap *heap);
  ExMasterStats(NAHeap *heap, char *sourceStr, int storedSqlTextLen, int originalSqlTextLen, char *queryId,
                int queryIdLen);
  ExMasterStats();
  ~ExMasterStats();
  UInt32 packedLength();
  //////////////////////////////////////////////////////////////////
  // packs 'this' into a message.
  //////////////////////////////////////////////////////////////////
  UInt32 pack(char *buffer);

  void unpack(const char *&buffer);

  void deleteMe();

  ExOperStats *copyOper(NAMemory *heap);

  void copyContents(ExMasterStats *other);

  void getVariableStatsInfo(char *dataBuffer, char *dataLen, int maxLen);

  ExMasterStats *castToExMasterStats();

  void setElapsedStartTime(long elapsedStartTime) {
    elapsedStartTime_ = elapsedStartTime;
    firstRowReturnTime_ = -1;
    elapsedEndTime_ = -1;
  }

  void setElapsedEndTime(long elapsedEndTime) { elapsedEndTime_ = elapsedEndTime; }

  void setCompStartTime(long compStartTime) {
    compStartTime_ = compStartTime;
    compEndTime_ = -1;
  }

  void setCompEndTime(long compEndTime) { compEndTime_ = compEndTime; }

  void setExeStartTime(long exeStartTime) {
    exeStartTime_ = exeStartTime;
    exeEndTime_ = -1;
    canceledTime_ = -1;
    querySuspendedTime_ = -1;
  }

  void setExeEndTime(long exeEndTime) {
    exeEndTime_ = exeEndTime;
    exeTimes_.addEntry(exeEndTime_ - exeStartTime_);
  }

  void setCanceledTime(long canceledTime) { canceledTime_ = canceledTime; }

  void setFixupStartTime(long fixupStartTime) {
    fixupStartTime_ = fixupStartTime;
    fixupEndTime_ = -1;
  }

  void setFixupEndTime(long fixupEndTime) { fixupEndTime_ = fixupEndTime; }

  void setFreeupStartTime(long freeupStartTime) {
    freeupStartTime_ = freeupStartTime;
    freeupEndTime_ = -1;
  }

  void setFreeupEndTime(long freeupEndTime) { freeupEndTime_ = freeupEndTime; }

  void setReturnedRowsIOTime(long rrIOtime) { returnedRowsIOTime_ = rrIOtime; }

  void setStmtState(short state) { stmtState_ = state; }

  char *getQueryId() { return queryId_; }
  int getQueryIdLen() { return queryIdLen_; }

  long getCompStartTime() { return compStartTime_; }

  long getCompEndTime() { return compEndTime_; }

  long getElapsedStartTime() { return elapsedStartTime_; }

  long getElapsedEndTime() { return elapsedEndTime_; }

  long getFirstRowReturnTime() { return firstRowReturnTime_; }

  long getExeStartTime() { return exeStartTime_; }

  long getExeEndTime() { return exeEndTime_; }

  long getCanceledTime() { return canceledTime_; }

  void setRowsAffected(long rowsAffected) { rowsAffected_ = rowsAffected; }

  void setSqlErrorCode(int sqlErrorCode) { sqlErrorCode_ = sqlErrorCode; }

  int getSqlErrorCode() { return sqlErrorCode_; }

  void setStatsErrorCode(int errorCode) { statsErrorCode_ = errorCode; }

  int getStatsErrorCode() { return statsErrorCode_; }

  long getFixupStartTime() { return fixupStartTime_; }

  long getFixupEndTime() { return fixupEndTime_; }

  long getFreeupStartTime() { return freeupStartTime_; }

  long getFreeupEndTime() { return freeupEndTime_; }

  long getReturnedRowsIOTime() { return returnedRowsIOTime_; }

  long getRowsAffected() { return rowsAffected_; }

  int &numOfTotalEspsUsed() { return numOfTotalEspsUsed_; }
  int &numOfNewEspsStarted() { return numOfNewEspsStarted_; }
  int &numOfRootEsps() { return numOfRootEsps_; }
  short &exePriority() { return exePriority_; }
  short &espPriority() { return espPriority_; }
  short &cmpPriority() { return cmpPriority_; }
  short &dp2Priority() { return dp2Priority_; }
  short &fixupPriority() { return fixupPriority_; }
  void incNumEspsInUse() { numOfTotalEspsUsed_++; }
  inline void setNumCpus(short i) { numCpus_ = i; }
  inline short getNumSqlProcs() { return numOfTotalEspsUsed_ + 1; }

  inline short getNumCpus() { return numCpus_; }

  inline void setAqrLastErrorCode(int ec) { aqrLastErrorCode_ = ec; }
  inline int getAqrLastErrorCode() { return aqrLastErrorCode_; }

  inline void setNumAqrRetries(int numRetries) { numAqrRetries_ = numRetries; }
  inline int getNumAqrRetries() { return numAqrRetries_; }

  inline void setAqrDelayBeforeRetry(int d) { delayBeforeAqrRetry_ = d; }
  inline int getAqrDelayBeforeRetry() { return delayBeforeAqrRetry_; }

  short getState() { return stmtState_; }

  void fixup(ExMasterStats *other);

  void setEndTimes(NABoolean updateExeEndTime);

  QueryCostInfo &queryCostInfo() { return queryCostInfo_; }
  void setQueryType(Int16 queryType, Int16 subqueryType) {
    queryType_ = queryType;
    subqueryType_ = subqueryType;
  }
  Int16 getQueryType() { return queryType_; }

  CompilerStatsInfo &compilerStatsInfo() { return compilerStatsInfo_; }

  NABoolean compilerCacheHit() { return (masterFlags_ & COMPILER_CACHE_HIT) != 0; }
  void setCompilerCacheHit(NABoolean v) {
    (v ? masterFlags_ |= COMPILER_CACHE_HIT : masterFlags_ &= ~COMPILER_CACHE_HIT);
  }

  NABoolean executorCacheHit() { return (masterFlags_ & EXECUTOR_CACHE_HIT) != 0; }
  void setExecutorCacheHit(NABoolean v) {
    (v ? masterFlags_ |= EXECUTOR_CACHE_HIT : masterFlags_ &= ~EXECUTOR_CACHE_HIT);
  }

  NABoolean isPrepare() { return (masterFlags_ & IS_PREPARE) != 0; }
  void setIsPrepare(NABoolean v) { (v ? masterFlags_ |= IS_PREPARE : masterFlags_ &= ~IS_PREPARE); }

  NABoolean isPrepAndExec() { return (masterFlags_ & IS_PREP_AND_EXEC) != 0; }
  void setIsPrepAndExec(NABoolean v) { (v ? masterFlags_ |= IS_PREP_AND_EXEC : masterFlags_ &= ~IS_PREP_AND_EXEC); }

  NABoolean xnReqd() { return (masterFlags_ & XN_REQD) != 0; }
  void setXnReqd(NABoolean v) { (v ? masterFlags_ |= XN_REQD : masterFlags_ &= ~XN_REQD); }

  void setSuspendMayHaveAuditPinned(bool v) { suspendMayHaveAuditPinned_ = v; }

  bool getSuspendMayHaveAuditPinned() { return suspendMayHaveAuditPinned_; }

  void setSuspendMayHoldLock(bool v) { suspendMayHoldLock_ = v; }

  bool getSuspendMayHoldLock() { return suspendMayHoldLock_; }

  bool isReadyToSuspend() const { return (readyToSuspend_ == READY); }
  void setReadyToSuspend() { readyToSuspend_ = READY; }
  void setNotReadyToSuspend() { readyToSuspend_ = NOT_READY; }

  void init(NABoolean resetDop);
  void reuse();
  void initBeforeExecute(long currentTimestamp);
  void resetAqrInfo();

  int getStatsItem(SQLSTATS_ITEM *sqlStats_item);
  void setParentQid(char *queryId, int queryIdLen);
  char *getParentQid() { return parentQid_; }
  int getParentQidLen() { return parentQidLen_; }
  void setParentQidSystem(char *parentQidSystem, int len);
  char *getParentQidSystem() { return parentQidSystem_; }
  long getTransId() { return transId_; }
  void setTransId(long transId) { transId_ = transId; }
  void setChildQid(char *queryId, int queryIdLen);
  char *getChildQid() { return childQid_; }
  int getChildQidLen() { return childQidLen_; }
  int getReclaimSpaceCount() { return reclaimSpaceCount_; }
  void incReclaimSpaceCount() { reclaimSpaceCount_++; }
  NABoolean filterForCpuStats(short subReqType, long currTimestamp, int etTimeInSecs);
  long getRowsReturned() const { return rowsReturned_; }
  void setRowsReturned(long cnt) { rowsReturned_ = cnt; }
  void incRowsReturned(long i = 1) {
    rowsReturned_ += i;
    if (firstRowReturnTime_ == -1) firstRowReturnTime_ = NA_JulianTimestamp();
  }

  bool isQuerySuspended() const { return isQuerySuspended_; }
  void setQuerySuspended(bool s) {
    isQuerySuspended_ = s;
    if (isQuerySuspended_) querySuspendedTime_ = NA_JulianTimestamp();
  }
  long getQuerySuspendedTime() const { return querySuspendedTime_; }
  char *getCancelComment() const { return cancelComment_; }

  // setCancelComment is called by Control Broker mxssmp under semaphore.
  void setCancelComment(const char *c);

  void setIsBlocking();
  void setNotBlocking();
  int timeSinceBlocking(int seconds);
  int timeSinceUnblocking(int seconds);

  void setValidPrivs(bool v) { validPrivs_ = v; }
  bool getValidPrivs() { return validPrivs_; }
  // Security Invalidation Keys -- no need to pack or unpack.
  int getNumSIKeys() const { return numSIKeys_; }
  SQL_QIKEY *getSIKeys() const { return sIKeys_; }
  void setInvalidationKeys(CliGlobals *cliGlobals, SecurityInvKeyInfo *sikInfo, int numObjUIDs, const long *objectUIDs);
  void setValidDDL(bool v) { validDDL_ = v; }
  bool getValidDDL() { return validDDL_; }
  void setValidHistogram(bool v) { validHistogram_ = v; }
  bool getValidHistogram() { return validHistogram_; }
  int getNumObjUIDs() const { return numObjUIDs_; }
  long *getObjUIDs() const { return objUIDs_; }

  char *getSourceString() { return sourceStr_; }
  int getOriginalSqlTextLen() { return originalSqlTextLen_; }

  long getQueryHash() { return queryHash_; }
  void setQueryHash(long x) { queryHash_ = x; }
  void setSlaName(char *slaName) {
    if (slaName != NULL)
      strcpy(slaName_, slaName);
    else
      slaName_[0] = '\0';
  }
  void setProfileName(char *profileName) {
    if (profileName != NULL)
      strcpy(profileName_, profileName);
    else
      profileName_[0] = '\0';
  }

 private:
  enum Flags {
    COMPILER_CACHE_HIT = 0x0001,
    EXECUTOR_CACHE_HIT = 0x0002,
    IS_PREPARE = 0x0004,
    IS_PREP_AND_EXEC = 0x0008,
    XN_REQD = 0x0010
  };

  char *sourceStr_;
  int storedSqlTextLen_;
  int originalSqlTextLen_;
  char *queryId_;
  int queryIdLen_;
  long elapsedStartTime_;
  long elapsedEndTime_;
  long firstRowReturnTime_;
  long compStartTime_;
  long compEndTime_;
  long exeStartTime_;
  long exeEndTime_;
  long canceledTime_;
  long fixupStartTime_;
  long fixupEndTime_;
  long freeupStartTime_;
  long freeupEndTime_;
  long returnedRowsIOTime_;
  long rowsAffected_;
  int sqlErrorCode_;
  int statsErrorCode_;

  short stmtState_;
  UInt16 masterFlags_;

  short numCpus_;

  QueryCostInfo queryCostInfo_;

  int numOfTotalEspsUsed_;
  int numOfNewEspsStarted_;
  int numOfRootEsps_;

  short exePriority_;
  short espPriority_;
  short cmpPriority_;
  short dp2Priority_;
  short fixupPriority_;
  short queryType_;
  short subqueryType_;

  CompilerStatsInfo compilerStatsInfo_;
  char *parentQid_;
  int parentQidLen_;
  char parentQidSystem_[24];
  long transId_;
  long rowsReturned_;
  int aqrLastErrorCode_;
  int numAqrRetries_;
  int delayBeforeAqrRetry_;
  char *childQid_;
  int childQidLen_;
  int reclaimSpaceCount_;
  bool isQuerySuspended_;
  long querySuspendedTime_;
  int cancelCommentLen_;
  char *cancelComment_;
  // These helpers for suspend/resume are written in the master of the
  // subject query and they are read by ssmpipc.cpp.
  bool suspendMayHaveAuditPinned_;
  bool suspendMayHoldLock_;
  enum { READY = 1, NOT_READY = 2 } readyToSuspend_;
  struct timeval blockOrUnblockSince_;
  bool isBlocking_;
  int lastActivity_;
  // query priv invalidation
  bool validPrivs_;
  // preallocate sIKeys_ while we have the semaphore.  Can add more later.
  enum {
    PreAllocatedSikKeys =
#ifdef _DEBUG
        1
#else
        30
#endif
    ,
    PreAllocatedObjUIDs =
#ifdef _DEBUG
        1
#else
        20
#endif
  };
  int numSIKeys_;
  SQL_QIKEY *sIKeys_;
  SQL_QIKEY preallocdSiKeys_[PreAllocatedSikKeys];
  bool validDDL_;
  bool validHistogram_;
  int numObjUIDs_;
  long *objUIDs_;
  long preallocdObjUIDs_[PreAllocatedObjUIDs];
  long queryHash_;
  ExStatsCounter exeTimes_;
  char slaName_[MAX_SLA_NAME_LEN + 1];
  char profileName_[MAX_PROFILE_NAME_LEN + 1];
};

class ExRMSStats : public ExOperStats {
 public:
  ExRMSStats(NAHeap *heap);
  UInt32 packedLength();
  UInt32 pack(char *buffer);
  void unpack(const char *&buffer);
  void deleteMe() {}
  ExOperStats *copyOper(NAMemory *heap);
  void copyContents(ExRMSStats *other);
  void getVariableStatsInfo(char *dataBuffer, char *dataLen, int maxLen);
  ExRMSStats *castToExRMSStats();
  void merge(ExRMSStats *other);
  inline void setRmsVersion(int version) { rmsVersion_ = version; }
  void setNodeName(char *nodeName);
  inline pid_t getSsmpPid() { return ssmpPid_; }
  inline pid_t getSscpPid() { return sscpPid_; }
  inline long getSsmpTimestamp() { return ssmpTimestamp_; }
  inline void setCpu(short cpu) { cpu_ = cpu; }
  inline void setSscpPid(pid_t pid) { sscpPid_ = pid; }
  inline void setSsmpPid(pid_t pid) { ssmpPid_ = pid; }
  inline void setSscpPriority(short pri) { sscpPriority_ = pri; }
  inline void setSsmpPriority(short pri) { ssmpPriority_ = pri; }
  inline void setStoreSqlSrcLen(short srcLen) { storeSqlSrcLen_ = srcLen; }
  inline void setRmsEnvType(short envType) { rmsEnvType_ = envType; }
  inline void setGlobalStatsHeapAlloc(long size) { currGlobalStatsHeapAlloc_ = size; }
  inline void setGlobalStatsHeapUsed(long size) { currGlobalStatsHeapUsage_ = size; }
  inline void setStatsHeapWaterMark(long size) { globalStatsHeapWatermark_ = size; }
  inline void setNoOfStmtStats(int noOfStmts) { currNoOfStmtStats_ = noOfStmts; }
  inline void incProcessStatsHeaps() { currNoOfProcessStatsHeap_++; }
  inline void decProcessStatsHeaps() { currNoOfProcessStatsHeap_--; }
  inline void incProcessRegd() { currNoOfRegProcesses_++; }
  inline void decProcessRegd() { currNoOfRegProcesses_--; }
  inline int getProcessRegd() { return currNoOfProcessStatsHeap_; }
  inline void setSemPid(pid_t pid) { semPid_ = pid; }
  inline void setSscpOpens(short numSscps) { sscpOpens_ = numSscps; }
  inline void setSscpDeletedOpens(short numSscps) { sscpDeletedOpens_ = numSscps; }
  inline void setSscpTimestamp(long timestamp) { sscpTimestamp_ = timestamp; }
  inline void setSsmpTimestamp(long timestamp) { ssmpTimestamp_ = timestamp; }
  inline void setRMSStatsResetTimestamp(long timestamp) { rmsStatsResetTimestamp_ = timestamp; }
  inline void incStmtStatsGCed(short inc) {
    stmtStatsGCed_ = inc;
    totalStmtStatsGCed_ += inc;
  }
  inline long getLastGCTime() { return lastGCTime_; }
  inline void setLastGCTime(long gcTime) { lastGCTime_ = gcTime; }
  inline void incSsmpReqMsg(long msgBytes) {
    ssmpReqMsgCnt_++;
    ssmpReqMsgBytes_ += msgBytes;
  }
  inline void incSsmpReplyMsg(long msgBytes) {
    ssmpReplyMsgCnt_++;
    ssmpReplyMsgBytes_ += msgBytes;
  }
  inline void incSscpReqMsg(long msgBytes) {
    sscpReqMsgCnt_++;
    sscpReqMsgBytes_ += msgBytes;
  }
  inline void incSscpReplyMsg(long msgBytes) {
    sscpReplyMsgCnt_++;
    sscpReplyMsgBytes_ += msgBytes;
  }
  inline void setNumQueryInvKeys(int n) { numQueryInvKeys_ = n; }
  inline void setNodesInCluster(short n) { nodesInCluster_ = n; }
  inline void setConfiguredPidMax(pid_t pid) { configuredPidMax_ = pid; }
  int getStatsItem(SQLSTATS_ITEM *sqlStats_item);
  void reset();
  inline void setEpochHeapAlloc(long size) { epochHeapAlloc_ = size; }
  inline void setEpochHeapUsed(long size) { epochHeapUsage_ = size; }
  inline void setEpochHeapWaterMark(long size) { epochHeapWatermark_ = size; }
  inline void setNoOfEpochEntries(int num) { currNoOfEpochEntries_ = num; }

 private:
  int rmsVersion_;
  char nodeName_[MAX_SEGMENT_NAME_LEN + 1];
  short cpu_;
  pid_t sscpPid_;
  short sscpPriority_;
  long sscpTimestamp_;
  pid_t ssmpPid_;
  short ssmpPriority_;
  long ssmpTimestamp_;
  short storeSqlSrcLen_;
  short rmsEnvType_;
  long currGlobalStatsHeapAlloc_;
  long currGlobalStatsHeapUsage_;
  long globalStatsHeapWatermark_;
  int currNoOfStmtStats_;
  int currNoOfRegProcesses_;
  int currNoOfProcessStatsHeap_;
  pid_t semPid_;
  short sscpOpens_;
  short sscpDeletedOpens_;
  short stmtStatsGCed_;
  long lastGCTime_;
  long totalStmtStatsGCed_;
  long ssmpReqMsgCnt_;
  long ssmpReqMsgBytes_;
  long ssmpReplyMsgCnt_;
  long ssmpReplyMsgBytes_;
  long sscpReqMsgCnt_;
  long sscpReqMsgBytes_;
  long sscpReplyMsgCnt_;
  long sscpReplyMsgBytes_;
  long rmsStatsResetTimestamp_;
  int numQueryInvKeys_;
  short nodesInCluster_;
  pid_t configuredPidMax_;
  long epochHeapAlloc_;
  long epochHeapUsage_;
  long epochHeapWatermark_;
  int currNoOfEpochEntries_;
};

//////////////////////////////////////////////////////////////////
// class ExFastExtractStats
//////////////////////////////////////////////////////////////////
class ExFastExtractStats : public ExOperStats {
 public:
  ExFastExtractStats(NAMemory *heap, ex_tcb *tcb, const ComTdb *tdb);

  ExFastExtractStats(NAMemory *heap);

  ~ExFastExtractStats(){};

  void init(NABoolean resetDop);

  void copyContents(ExFastExtractStats *other);

  void merge(ExFastExtractStats *other);

  UInt32 packedLength();

  //////////////////////////////////////////////////////////////////
  // packs 'this' into a message. Converts pointers to offsets.
  //////////////////////////////////////////////////////////////////
  UInt32 pack(char *buffer);

  void unpack(const char *&buffer);

  ExOperStats *copyOper(NAMemory *heap);

  ExFastExtractStats *castToExFastExtractStats();

  virtual const char *getNumValTxt(int i) const;

  virtual long getNumVal(int i) const;

  virtual void getVariableStatsInfo(char *dataBuffer, char *datalen, int maxLen);
  UInt32 &buffersCount() { return buffersCount_; }
  UInt32 &processedRowsCount() { return processedRowsCount_; }
  UInt32 &readyToSendBuffersCount() { return readyToSendBuffersCount_; }
  UInt32 &sentBuffersCount() { return sentBuffersCount_; }
  UInt32 &partitionsCount() { return partitionNumber_; }
  UInt32 &bufferAllocFailuresCount() { return bufferAllocFailuresCount_; }
  void setBuffersCount(UInt32 v) { buffersCount_ = v; }
  void setProcessedRowsCount(UInt32 v) { processedRowsCount_ = v; }
  UInt32 getProcessedRowsCount() { return processedRowsCount_; }
  void incProcessedRowsCount() { processedRowsCount_++; }
  void setErrorRowsCount(UInt32 v) { errorRowsCount_ = v; }
  UInt32 getErrorRowsCount() { return errorRowsCount_; }
  void incErrorRowsCount() { errorRowsCount_++; }
  void incReadyToSendBuffersCount() { readyToSendBuffersCount_++; }
  void incSentBuffersCount() { sentBuffersCount_++; }
  void incReadyToSendBytes(UInt32 v = 1) { readyToSendBytes_ += v; }
  void incSentBytes(UInt32 v = 1) { sentBytes_ += v; }

  void setPartitionNumber(UInt32 v) { partitionNumber_ = v; }
  void incBufferAllocFailuresCount() { bufferAllocFailuresCount_++; }
  void setBufferAllocFailuresCount(UInt32 v) { bufferAllocFailuresCount_ = v; }

 private:
  UInt32 buffersCount_;
  UInt32 processedRowsCount_;
  UInt32 errorRowsCount_;
  UInt32 readyToSendBuffersCount_;
  UInt32 sentBuffersCount_;
  UInt32 partitionNumber_;
  UInt32 bufferAllocFailuresCount_;
  UInt32 readyToSendBytes_;
  UInt32 sentBytes_;
};

///////////////////////////////////////////////////////////////
// class ExProcessStats
//////////////////////////////////////////////////////////////////
class ExProcessStats : public ExOperStats {
 public:
  ExProcessStats(NAMemory *heap);
  ExProcessStats(NAMemory *heap, short nid, pid_t pid);
  ~ExProcessStats() {
    if (delQid_) NADELETEBASIC(recentQid_, getHeap());
    recentQid_ = NULL;
    if (recentSourceStr_ != NULL && delStr_) NADELETEBASIC(recentSourceStr_, getHeap());
    recentSourceStr_ = NULL;
  };

  void init(NABoolean resetDop);

  void copyContents(ExProcessStats *other);

  void merge(ExProcessStats *other);

  UInt32 packedLength();
  UInt32 pack(char *buffer);

  void unpack(const char *&buffer);

  ExOperStats *copyOper(NAMemory *heap);

  ExProcessStats *castToExProcessStats();

  virtual const char *getNumValTxt(int i) const;

  virtual long getNumVal(int i) const;

  virtual void getVariableStatsInfo(char *dataBuffer, char *datalen, int maxLen);
  virtual const char *getTextVal();
  inline size_t getExeMemHighWM() { return exeMemHighWM_; }
  inline size_t getExeMemAlloc() { return exeMemAlloc_; }
  inline size_t getExeMemUsed() { return exeMemUsed_; }
  inline size_t getIpcMemHighWM() { return ipcMemHighWM_; }
  inline size_t getIpcMemAlloc() { return ipcMemAlloc_; }
  inline size_t getIpcMemUsed() { return ipcMemUsed_; }
  inline short getNid() { return nid_; }
  inline pid_t getPid() { return pid_; }
  inline int getStaticStmtCount() { return staticStmtCount_; }
  inline void incStaticStmtCount() { staticStmtCount_++; }
  inline void decStaticStmtCount() { staticStmtCount_--; }
  inline int getDynamicStmtCount() { return dynamicStmtCount_; }
  inline void incDynamicStmtCount() { dynamicStmtCount_++; }
  inline void decDynamicStmtCount() { dynamicStmtCount_--; }
  inline void incStmtCount(Statement::StatementType stmtType) {
    if (stmtType == Statement::STATIC_STMT)
      incStaticStmtCount();
    else
      incDynamicStmtCount();
  }
  inline void decStmtCount(Statement::StatementType stmtType) {
    if (stmtType == Statement::STATIC_STMT)
      decStaticStmtCount();
    else
      decDynamicStmtCount();
  }
  inline short getOpenStmtCount() { return openStmtCount_; }
  inline void incOpenStmtCount() { openStmtCount_++; }
  inline void decOpenStmtCount() { openStmtCount_--; }
  inline short getCloseStmtCount() { return closeStmtCount_; }
  inline void incCloseStmtCount() { closeStmtCount_++; }
  inline void decCloseStmtCount() {
    if (closeStmtCount_ > 0) closeStmtCount_--;
  }
  inline short getReclaimStmtCount() { return reclaimStmtCount_; }
  inline void incReclaimStmtCount() { reclaimStmtCount_++; }
  inline void decReclaimStmtCount() { reclaimStmtCount_--; }
  inline long getStartTime() { return startTime_; }
  void setStartTime(long startTime) { startTime_ = startTime; }
  inline int getPfsSize() { return pfsSize_; }
  void setPfsSize(int pfsSize) { pfsSize_ = pfsSize; }
  inline int getPfsCurUse() { return pfsCurUse_; }
  void setPfsCurUse(int pfsCurUse) { pfsCurUse_ = pfsCurUse; }
  inline int getPfsMaxUse() { return pfsMaxUse_; }
  void setPfsMaxUse(int pfsMaxUse) { pfsMaxUse_ = pfsMaxUse; }
  inline short getNumArkFsSessions() { return arkfsSessionCount_; }
  inline void incArkFsSessionCount() { arkfsSessionCount_++; }
  void decArkFsSessionCount() {
    if (arkfsSessionCount_ > 0) arkfsSessionCount_--;
  }
  inline short getNumSqlOpens() { return sqlOpenCount_; }
  inline void incNumSqlOpens() { sqlOpenCount_++; }
  void decNumSqlOpens() {
    if (sqlOpenCount_ > 0) sqlOpenCount_--;
  }
  void updateMemStats(NAHeap *exeHeap, NAHeap *ipcHeap);
  void setRecentQid(char *recentQid) {
    recentQid_ = recentQid;
    if (recentQid_ != NULL)
      qidLen_ = strlen(recentQid_);
    else
      qidLen_ = 0;
  }
  void setRecentQidToNull(char *recentQid) {
    if (recentQid_ == recentQid) {
      recentQid_ = NULL;
      delQid_ = FALSE;
      qidLen_ = 0;
    }
  }
  inline char *getRecentQid() { return recentQid_; }
  inline void incStartedEsps() { numESPsStarted_++; }
  inline int getNumESPsStarted() { return numESPsStarted_; }
  inline void incStartupCompletedEsps() { numESPsStartupCompleted_++; }
  inline int getNumESPsStartupCompleted() { return numESPsStartupCompleted_; }
  inline void incBadEsps() { numESPsBad_++; }

  inline int getQidLen() { return qidLen_; }

  void setSourceStr(char *sourcestr, int sqlLen) {
    if (sourcestr == NULL || sqlLen == 0) {
      sourcestr = NULL;
      delStr_ = FALSE;
      return;
    }

    if (recentSourceStr_ != NULL && delStr_) NADELETEBASIC(recentSourceStr_, getHeap());

    strLen_ = MINOF(sqlLen, 256);

    recentSourceStr_ = new ((NAHeap *)(getHeap())) char[strLen_ + 1];
    strncpy(recentSourceStr_, sourcestr, strLen_);
    recentSourceStr_[strLen_] = '\0';
    delStr_ = TRUE;
  }

  inline int getNumESPsBad() { return numESPsBad_; }
  inline void incDeletedEsps() { numESPsDeleted_++; }
  inline int getNumESPsDeleted() { return numESPsDeleted_; }
  inline int getNumESPsInUse() { return numESPsInUse_; }
  inline int getNumESPsFree() { return numESPsFree_; }
  inline void incNumESPsInUse(NABoolean gotFromCache) {
    numESPsInUse_++;
    if (gotFromCache) decNumESPsFree();
  }

  void decNumESPsInUse() {
    if (numESPsInUse_ > 0) numESPsInUse_--;
  }

  inline void decNumESPsFree() {
    if (numESPsFree_ > 0) numESPsFree_--;
  }
  void decNumESPs(NABoolean wasInUse, NABoolean beingDeleted) {
    if (wasInUse) {
      decNumESPsInUse();
      if (beingDeleted)
        numESPsDeleted_++;
      else
        numESPsFree_++;
    } else {
      decNumESPsFree();
      if (beingDeleted) numESPsDeleted_++;
    }
  }

 private:
  short nid_;
  pid_t pid_;
  size_t exeMemHighWM_;
  size_t exeMemAlloc_;
  size_t exeMemUsed_;
  size_t ipcMemHighWM_;
  size_t ipcMemAlloc_;
  size_t ipcMemUsed_;
  long startTime_;
  int staticStmtCount_;
  int dynamicStmtCount_;
  short openStmtCount_;
  short closeStmtCount_;
  short reclaimStmtCount_;
  int pfsSize_;
  int pfsCurUse_;
  int pfsMaxUse_;
  short sqlOpenCount_;
  short arkfsSessionCount_;
  char *recentQid_;
  int qidLen_;
  NABoolean delQid_;
  char *recentSourceStr_;
  NABoolean delStr_;
  int strLen_;
  int numESPsStarted_;
  int numESPsStartupCompleted_;
  int numESPsDeleted_;
  int numESPsBad_;
  int numESPsInUse_;
  int numESPsFree_;
};

class ExObjectEpochStats : public ExOperStats {
 public:
  ExObjectEpochStats(NAMemory *heap) : ExOperStats(heap, OBJECT_EPOCH_STATS) {}
  ExObjectEpochStats(NAMemory *heap, short cpu, const NAString &objectName, ComObjectType objectType, int hash,
                     long redefTime, UInt32 epoch, UInt32 flags)
      : ExOperStats(heap, OBJECT_EPOCH_STATS),
        cpu_(cpu),
        objectType_(objectType),
        hash_(hash),
        redefTime_(redefTime),
        epoch_(epoch),
        flags_(flags) {
    strncpy(objectName_, objectName.data(), sizeof(objectName_));
  }
  ~ExObjectEpochStats() {}

  void init(NABoolean resetDop) {
    cpu_ = -1;
    objectName_[0] = 0;
    hash_ = 0;
    objectType_ = COM_UNKNOWN_OBJECT;
    redefTime_ = 0;
    epoch_ = 0;
    flags_ = 0;
  }

  void copyContents(ExObjectEpochStats *other) {
    ExOperStats::copyContents(other);
    char *srcPtr = (char *)other + sizeof(ExOperStats);
    char *destPtr = (char *)this + sizeof(ExOperStats);
    UInt32 srcLen = sizeof(ExObjectEpochStats) - sizeof(ExOperStats);
    memcpy((void *)destPtr, (void *)srcPtr, srcLen);
  }

  UInt32 packedLength() {
    UInt32 size = ExOperStats::packedLength();
    alignSizeForNextObj(size);
    size += sizeof(ExObjectEpochStats) - sizeof(ExOperStats);
    return size;
  }
  UInt32 pack(char *buffer) {
    UInt32 packedLen;
    packedLen = ExOperStats::pack(buffer);
    alignSizeForNextObj(packedLen);
    buffer += packedLen;
    UInt32 srcLen = sizeof(ExObjectEpochStats) - sizeof(ExOperStats);
    char *srcPtr = (char *)this + sizeof(ExOperStats);
    memcpy(buffer, (void *)srcPtr, srcLen);
    return packedLen + srcLen;
  }

  void unpack(const char *&buffer) {
    UInt32 srcLen;
    ExOperStats::unpack(buffer);
    alignBufferForNextObj(buffer);
    srcLen = sizeof(ExObjectEpochStats) - sizeof(ExOperStats);
    char *targetPtr = (char *)this + sizeof(ExOperStats);
    memcpy((void *)targetPtr, buffer, srcLen);
    buffer += srcLen;
  }

  ExOperStats *copyOper(NAMemory *heap) {
    ExObjectEpochStats *stat = new (heap) ExObjectEpochStats(heap);
    stat->copyContents(this);
    return stat;
  }

  ExObjectEpochStats *castToExObjectEpochStats() { return this; }

  short getCpu() const { return cpu_; }
  void setCpu(short cpu) { cpu_ = cpu; }
  const char *getObjectName() const { return objectName_; }
  void setObjectName(const char *objectName) { strncpy(objectName_, objectName, sizeof(objectName_)); }
  ComObjectType getObjectType() const { return objectType_; }
  void setObjectType(ComObjectType objectType) { objectType_ = objectType; }
  int getHash() const { return hash_; }
  void setHash(int hash) { hash_ = hash; }
  long getRedefTime() const { return redefTime_; }
  void setRedefTime(long redefTime) { redefTime_ = redefTime; }
  UInt32 getEpoch() const { return epoch_; }
  void setEpoch(UInt32 epoch) { epoch_ = epoch; }
  UInt32 getFlags() const { return flags_; }
  void setFlags(UInt32 flags) { flags_ = flags; }
  const char *getFlagsString() const {
    const char *results[] = {"", "(DDL_IN_PROGRESS:1 | READS_DISALLOWED:0)", "(INVALID FLAGS)",
                             "(DDL_IN_PROGRESS:1 | READS_DISALLOWED:1)"};
    ex_assert(flags_ < sizeof(results), "Unkown object epoch flags");
    return results[flags_];
  }

 private:
  short cpu_;
  char objectName_[1024];  // Hope this is enough
  int hash_;
  ComObjectType objectType_;
  long redefTime_;
  UInt32 epoch_;
  UInt32 flags_;
};

class ExObjectLockStats : public ExOperStats {
 public:
  ExObjectLockStats(NAMemory *heap) : ExOperStats(heap, OBJECT_LOCK_STATS) {}
  ExObjectLockStats(NAMemory *heap, short cpu, const NAString &objectName, ComObjectType objectType, pid_t pid,
                    int holderNid, int holderPid, bool dmlLocked, bool ddlLocked)
      : ExOperStats(heap, OBJECT_LOCK_STATS),
        cpu_(cpu),
        objectType_(objectType),
        pid_(pid),
        holderNid_(holderNid),
        holderPid_(holderPid),
        dmlLocked_(dmlLocked),
        ddlLocked_(ddlLocked) {
    strncpy(objectName_, objectName.data(), sizeof(objectName_));
  }
  ~ExObjectLockStats() {}

  void init(NABoolean resetDop) {
    cpu_ = -1;
    objectName_[0] = 0;
    objectType_ = COM_UNKNOWN_OBJECT;
    pid_ = -1;
    holderNid_ = -1;
    holderPid_ = -1;
    dmlLocked_ = false;
    ddlLocked_ = false;
  }

  void copyContents(ExObjectLockStats *other) {
    ExOperStats::copyContents(other);
    char *srcPtr = (char *)other + sizeof(ExOperStats);
    char *destPtr = (char *)this + sizeof(ExOperStats);
    UInt32 srcLen = sizeof(ExObjectLockStats) - sizeof(ExOperStats);
    memcpy((void *)destPtr, (void *)srcPtr, srcLen);
  }

  UInt32 packedLength() {
    UInt32 size = ExOperStats::packedLength();
    alignSizeForNextObj(size);
    size += sizeof(ExObjectLockStats) - sizeof(ExOperStats);
    return size;
  }
  UInt32 pack(char *buffer) {
    UInt32 packedLen;
    packedLen = ExOperStats::pack(buffer);
    alignSizeForNextObj(packedLen);
    buffer += packedLen;
    UInt32 srcLen = sizeof(ExObjectLockStats) - sizeof(ExOperStats);
    char *srcPtr = (char *)this + sizeof(ExOperStats);
    memcpy(buffer, (void *)srcPtr, srcLen);
    return packedLen + srcLen;
  }

  void unpack(const char *&buffer) {
    UInt32 srcLen;
    ExOperStats::unpack(buffer);
    alignBufferForNextObj(buffer);
    srcLen = sizeof(ExObjectLockStats) - sizeof(ExOperStats);
    char *targetPtr = (char *)this + sizeof(ExOperStats);
    memcpy((void *)targetPtr, buffer, srcLen);
    buffer += srcLen;
  }

  ExOperStats *copyOper(NAMemory *heap) {
    ExObjectLockStats *stat = new (heap) ExObjectLockStats(heap);
    stat->copyContents(this);
    return stat;
  }

  ExObjectLockStats *castToExObjectLockStats() { return this; }

  short getCpu() const { return cpu_; }
  void setCpu(short cpu) { cpu_ = cpu; }
  const char *getObjectName() const { return objectName_; }
  void setObjectName(const char *objectName) { strncpy(objectName_, objectName, sizeof(objectName_)); }
  ComObjectType getObjectType() const { return objectType_; }
  void setObjectType(ComObjectType objectType) { objectType_ = objectType; }
  pid_t getPid() const { return pid_; }
  void setPid(pid_t pid) { pid_ = pid; }
  bool getDMLLocked() const { return dmlLocked_; }
  void setDMLLocked(bool dmlLocked) { dmlLocked_ = dmlLocked; }
  bool getDDLLocked() const { return ddlLocked_; }
  void setDDLLocked(bool ddlLocked) { ddlLocked_ = ddlLocked; }
  int getHolderNid() const { return holderNid_; }
  void setHolderNid(int holderNid) { holderNid_ = holderNid; }
  int getHolderPid() const { return holderPid_; }
  void setHolderPid(int holderPid) { holderPid_ = holderPid; }

  const char *getLockString() const {
    if (dmlLocked_ && ddlLocked_) {
      return "DML DDL";
    }
    if (dmlLocked_) {
      return "DML";
    }
    if (ddlLocked_) {
      return "DDL";
    }
    return "";
  }

  bool isSameLock(const ExObjectLockStats &other) {
    if (objectType_ == other.getObjectType() && dmlLocked_ == other.getDMLLocked() &&
        ddlLocked_ == other.getDDLLocked() && (strncmp(objectName_, other.getObjectName(), sizeof(objectName_)) == 0)) {
      return true;
    }
    return false;
  }

 private:
  short cpu_;
  char objectName_[1024];  // Hope this is enough
  ComObjectType objectType_;
  pid_t pid_;
  int holderNid_;
  int holderPid_;
  bool dmlLocked_;
  bool ddlLocked_;
};

class ExQryInvalidStats : public ExOperStats {
 public:
  ExQryInvalidStats(NAMemory *heap) : ExOperStats(heap, QUERY_INVALIDATION_STATS) { init(); }

  ExQryInvalidStats(NAMemory *heap, short cpu, char *op, long objUid, int subHash, int objHash, long revokeTime)
      : ExOperStats(heap, QUERY_INVALIDATION_STATS),
        cpu_(cpu),
        objectUid_(objUid),
        subjectHash_(subHash),
        objectHash_(objHash),
        revokeTime_(revokeTime) {
    memcpy(oper_, op, sizeof(oper_));
  }

  ~ExQryInvalidStats() {}

  void init() {
    cpu_ = -1;
    objectUid_ = 0;
    subjectHash_ = 0;
    objectHash_ = 0;
    revokeTime_ = 0;
    oper_[0] = '\0';
  }

  void copyContents(ExQryInvalidStats *other) {
    ExOperStats::copyContents(other);
    char *srcPtr = (char *)other + sizeof(ExOperStats);
    char *destPtr = (char *)this + sizeof(ExOperStats);
    UInt32 srcLen = sizeof(ExQryInvalidStats) - sizeof(ExOperStats);
    memcpy((void *)destPtr, (void *)srcPtr, srcLen);
  }

  UInt32 packedLength() {
    UInt32 size = ExOperStats::packedLength();
    alignSizeForNextObj(size);
    size += sizeof(ExQryInvalidStats) - sizeof(ExOperStats);
    return size;
  }
  UInt32 pack(char *buffer) {
    UInt32 packedLen;
    packedLen = ExOperStats::pack(buffer);
    alignSizeForNextObj(packedLen);
    buffer += packedLen;
    UInt32 srcLen = sizeof(ExQryInvalidStats) - sizeof(ExOperStats);
    char *srcPtr = (char *)this + sizeof(ExOperStats);
    memcpy(buffer, (void *)srcPtr, srcLen);
    return packedLen + srcLen;
  }

  void unpack(const char *&buffer) {
    UInt32 srcLen;
    ExOperStats::unpack(buffer);
    alignBufferForNextObj(buffer);
    srcLen = sizeof(ExQryInvalidStats) - sizeof(ExOperStats);
    char *targetPtr = (char *)this + sizeof(ExOperStats);
    memcpy((void *)targetPtr, buffer, srcLen);
    buffer += srcLen;
  }

  ExOperStats *copyOper(NAMemory *heap) {
    ExQryInvalidStats *stat = new (heap) ExQryInvalidStats(heap);
    stat->copyContents(this);
    return stat;
  }

  ExQryInvalidStats *castToExQryInvalidStats() { return this; }

  short getCpu() const { return cpu_; }
  void setCpu(short cpu) { cpu_ = cpu; }

  long getObjectUid() const { return objectUid_; }
  void setObjectUid(long objUid) { objectUid_ = objUid; }

  UInt32 getSubjectHash() const { return subjectHash_; }
  void setSubjectHash(UInt32 hash) { subjectHash_ = hash; }

  UInt32 getObjectHash() const { return objectHash_; }
  void setObjectHash(UInt32 hash) { objectHash_ = hash; }

  long getRevokeTime() const { return revokeTime_; }
  void setRevokeTime(long revokeTime) { revokeTime_ = revokeTime; }

  const char *getOperation() const { return oper_; }
  void setOperation(char *op) { memcpy(oper_, op, sizeof(oper_)); }

 private:
  short cpu_;
  char oper_[2];
  long objectUid_;
  UInt32 subjectHash_;
  UInt32 objectHash_;
  long revokeTime_;
};

// -----------------------------------------------------------------------
// ExQryInvalidStatsTdb
// -----------------------------------------------------------------------
class ExQryInvalidStatsTdb : public ComTdbStats {
 public:
  ExQryInvalidStatsTdb() {}

  virtual ~ExQryInvalidStatsTdb() {}

  virtual ex_tcb *build(ex_globals *globals);
};

class ExQryInvalidStatsTcb : public ex_tcb {
 public:
  ExQryInvalidStatsTcb(const ExQryInvalidStatsTdb &qiTdb, ex_globals *glob);

  ~ExQryInvalidStatsTcb();

  void freeResources();

  short work();

  ex_queue_pair getParentQueue() const { return qparent_; };

  virtual int numChildren() const { return 0; };
  virtual const ex_tcb *getChild(int pos) const { return NULL; };
  ExStatisticsArea *sendToSsmp();
  void parse_stmt_name(char *string, int len);
  ComDiagsArea *getDiagsArea() { return diagsArea_; }
  enum QIStatsStep {
    INITIAL_,
    GET_NEXT_STATS_ENTRY_,
    APPLY_SCAN_EXPR_,
    PROJECT_,
    ERROR_,
    DONE_,
    SEND_TO_SSMP_,
    GET_LOCAL_QI_STATS_ENTRY_,
  };

  void getColumnValues(ExOperStats *stat);

 private:
  NABoolean deleteStats() { return (flags_ & QI_STATS_DELETE_IN_TCB_) != 0; }

  void setDeleteStats(NABoolean v) { (v ? flags_ |= QI_STATS_DELETE_IN_TCB_ : flags_ &= ~QI_STATS_DELETE_IN_TCB_); }

  enum Flags {
    QI_STATS_DELETE_IN_TCB_ = 0x0001,
  };
  ComTdbStats &statsTdb() const { return (ComTdbStats &)tdb; };

  // Queues used to communicate with the parent TCB.
  ex_queue_pair qparent_;

  // this is where the stats row will be created.
  tupp tuppData_;
  char *data_;

  atp_struct *workAtp_;

  ExStatisticsArea *stats_;

  char *inputModName_;
  char *inputStmtName_;

  short cpu_;
  char nodeName_[MAX_SEGMENT_NAME_LEN + 1];
  short retryAttempts_;
  long flags_;
  ComDiagsArea *diagsArea_;
};

class ExQIStatsPrivateState : public ex_tcb_private_state {
  friend class ExQryInvalidStatsTcb;

 public:
  ExQIStatsPrivateState();
  ~ExQIStatsPrivateState(){};
  ex_tcb_private_state *allocate_new(const ex_tcb *tcb);

 private:
  ExQryInvalidStatsTcb::QIStatsStep step_;

  long matchCount_;  // number of rows returned for this parent row
};

#endif
