// **********************************************************************

// **********************************************************************
#ifndef COMPILATIONSTATS_H
#define COMPILATIONSTATS_H

#include "optimizer/CmpProcess.h"
#include "optimizer/NATable.h"
#include "optimizer/SchemaDB.h"
#include "optimizer/opt.h"
#include "sqlcomp/QCache.h"
/************************************************************************
class CompilationStats

Singleton class to collect information about this Query compilation
and to be logged into the repository after the compilation is complete.

************************************************************************/
class CompilationStats {
 public:
  CompilationStats();

  ~CompilationStats(){};

  void takeSnapshotOfCounters();

  // Used to count the CPU path length for each compilation phase
  enum CompilationPhase {
    CMP_PHASE_ALL = 0,
    CMP_PHASE_PARSER,
    CMP_PHASE_BINDER,
    CMP_PHASE_TRANSFORMER,
    CMP_PHASE_NORMALIZER,
    CMP_PHASE_SEMANTIC_QUERY_OPTIMIZATION,
    CMP_PHASE_ANALYZER,
    CMP_PHASE_OPTIMIZER,
    CMP_PHASE_PRECODE_GENERATOR,
    CMP_PHASE_GENERATOR,
    CMP_NUM_PHASES
  };

  long compileStartTime();
  long compileEndTime();

  // pass in a buffer of size COMPILER_ID_LEN
  void getCompilerId(char *cmpId, int len);
  //
  // metadata cache counters
  int metadataCacheHits();
  int metadataCacheLookups();
  //
  // See QCacheState enum
  int getQueryCacheState();
  //
  // histogram counters
  int histogramCacheHits();
  int histogramCacheLookups();
  //
  // statement heap
  static inline size_t stmtHeapCurrentSize() { return CmpCommon::statementHeap()->getAllocSize(); }
  //
  // context heap
  static inline size_t cxtHeapCurrentSize() { return CmpCommon::contextHeap()->getAllocSize(); }
  //
  // optimization tasks
  static inline int optimizationTasks() { return CURRSTMT_OPTDEFAULTS->getTaskCount(); }
  //
  // optimization contexts
  void incrOptContexts();

  int optimizationContexts();

  // is recompile
  void setIsRecompile();

  NABoolean isRecompile();

  // pass in a buffer of size CompilationStats::MAX_COMPILER_INFO_LEN
  void getCompileInfo(char *cmpInfo);
  int getCompileInfoLen();

  void enterCmpPhase(CompilationPhase phase);
  void exitCmpPhase(CompilationPhase phase);
  int cmpPhaseLength(CompilationPhase phase);
  //
  // maximum # of characters for compiler info
  enum { MAX_COMPILER_INFO_LEN = 4096 };

  enum QCacheState {
    QCSTATE_UNKNOWN = -1,
    QCSTATE_TEXT = 0,
    QCSTATE_TEMPLATE,
    QCSTATE_MISS_NONCACHEABLE,
    QCSTATE_MISS_CACHEABLE,
    QCSTATE_NUM_STATES
  };

  //  the valid phases only go up to one before CMP_NUM_PHASES
  inline NABoolean isValidPhase(CompilationPhase phase) { return (phase >= CMP_PHASE_ALL && phase < CMP_NUM_PHASES); }

  // valid QCacheState up to one before QCSTATE_NUM_STATUSES
  inline NABoolean isValidQCacheState(QCacheState state) {
    return (state >= QCSTATE_TEXT && state < QCSTATE_NUM_STATES);
  }

  void dumpToFile();

 private:
  // timestamp for start/end time of this compilation
  long compileStartTime_;
  long compileEndTime_;
  //
  //  Task Monitor used for CPU path length for each phase
  TaskMonitor cpuMonitor_[CMP_NUM_PHASES];
  //
  // metadata cache counters
  int mdCacheHitsBegin_;
  int mdCacheLookupsBegin_;
  //
  // histogram cache counters
  int hCacheHitsBegin_;
  int hCacheLookupsBegin_;
  //
  // optimization tasks/contexts counters
  int optContexts_;
  //
  // is this query a recompile
  NABoolean isRecompile_;
  //
  // additional compiler information
  char compileInfo_[MAX_COMPILER_INFO_LEN];
  //
  // Use snapshot of QueryCacheStats to compare
  // beginning/end of compilation
  QCacheStats qCacheStatsBegin_;
};

#endif  // COMPILATIONSTATS_H
