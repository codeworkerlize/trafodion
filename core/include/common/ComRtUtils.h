
#ifndef COMRTUTILS_H
#define COMRTUTILS_H

#include <fstream>
#include <set>
#include <string>
#include <vector>

#include "common/ComCextMisc.h"
#include "common/ComSmallDefs.h"
#include "common/Int64.h"
#include "common/NABoolean.h"
#include "common/NAMemory.h"
#include "common/NAString.h"
#include "common/Platform.h"

using namespace std;
#include "seabed/ms.h"
#ifdef min
#undef min
#endif  // min

#define MAX_SEGMENT_NAME_LEN   255
#define PROCESSNAME_STRING_LEN 40
#define PROGRAM_NAME_LEN       64
#define BDR_CLUSTER_NAME_LEN   24
#define BDR_CLUSTER_NAME_KEY   "BDR_CLUSTER"

// Keep in sync with common/ComRtUtils.cpp ComRtGetModuleFileName():
//                               0123456789*123456789*123456789*1: position0-31
#define systemModulePrefix        "HP_SYSTEM_CATALOG.SYSTEM_SCHEMA."
#define systemModulePrefixLen     32
#define systemModulePrefixODBC    "HP_SYSTEM_CATALOG.MXCS_SCHEMA."
#define systemModulePrefixLenODBC 30

// const NAString InternalModNameList;

// returns TRUE, if modName is an internal module name
NABoolean ComRtIsInternalModName(const char *modName);

// returns 'next' internal mod name.
// 'index' keeps track of the current mod name returned. It should
// be initialized to 0 on the first call to this method.
const char *ComRtGetNextInternalModName(int &index, char *modNameBuf);

NABoolean getLinuxGroups(const char *userName, std::set<std::string> &userGroups);

// -----------------------------------------------------------------------
// Class to read an oss file by oss path name or Guardian File name
// -----------------------------------------------------------------------

#include <iosfwd>
using namespace std;

class ModuleOSFile {
 public:
  ModuleOSFile();
  ~ModuleOSFile();
  int open(const char *fname);
  int openGuardianFile(const char *fname);
  int close();
  int readpos(char *buf, int pos, int len, short &countRead);

 private:
  fstream fs_;
};

// -----------------------------------------------------------------------
// All of the methods below require a buffer that holds the output
// string.  The true length of the output (string length w/o NUL
// terminator) is returned in resultLength (may be greater than
// inputBufferLength). The output string in buffer is NUL-
// terminated. The return code is either 0 or an operating system
// error or -1 if the buffer wasn't large enough. The "resultLength"
// output parameter is set if the return code is 0 or -1.
// The diagnostics area is not set by these calls.
// -----------------------------------------------------------------------
// -----------------------------------------------------------------------
// Get the directory name where NonStop SQL software resides
// (from registry on NT, $SYSTEM.SYSTEM on NSK)
// -----------------------------------------------------------------------
int ComRtGetInstallDir(char *buffer, int inputBufferLength, int *resultLength);

// -----------------------------------------------------------------------
// Convert a 3-part module name into the file name in which the module
// is stored
// -----------------------------------------------------------------------
int ComRtGetModuleFileName(const char *moduleName, const char *moduleDir, char *buffer, int inputBufferLength,
                           char *sysModuleDir,   // location of SYSTEMMODULES
                           char *userModuleDir,  // location of USERMODULES
                           int *resultLength, short &isSystemModule);

// -----------------------------------------------------------------------
// Get the cluster (EXPAND node) name (returns "NSK" on NT)
// -----------------------------------------------------------------------
int ComRtGetOSClusterName(char *buffer, int inputBufferLength, int *resultLength, short *nodeNumber = NULL);

// -----------------------------------------------------------------------
// Get the MP system catalog name.
// -----------------------------------------------------------------------
int ComRtGetMPSysCatName(char *sysCatBuffer,    /* out */
                         int inputBufferLength, /* in  */
                         char *inputSysName,    /* in must set to NULL if no name is passed */
                         int *sysCatLength,     /* out */
                         short *detailError,    /* out */
                         NAMemory *heap = 0);   /* in  */

// -----------------------------------------------------------------------
// Determine if the name is an NSK name, \sys.$vol.subvol.file, look for
// \ . $ characters in the string.
// -----------------------------------------------------------------------
NABoolean ComRtIsNSKName(char *name);

// -----------------------------------------------------------------------
//
// ComRtGetJulianFromUTC()
//
// This function converts a unix-epoch timespec, which is based on midnight
// GMT, the morning of Jan 1, 1970 to a JulianTimestamp, which is based on
// noon GMT, the day of Jan 1, 4713 B.C.  The constant 2440588 represents
// the number of whole days between these two dates.  The constant 86400 is
// the number of seconds per day.  The 43200 is number of seconds in a half
// day and is subtracted to account for the JulianDate starting at noon.
// The 1000000 constant converts seconds to microseconds, and the 1000 is
// to convert the nanosecond part of the unix timespec to microseconds. The
// JulianTimesamp returned is in microseconds so it can be used directly
// with the Guardian INTERPRETTIMESTAMP function.
inline long ComRtGetJulianFromUTC(timespec ts) {
  return (((ts.tv_sec + (2440588LL * 86400LL) - 43200LL) * 1000000LL) + (ts.tv_nsec / 1000));
}

// -----------------------------------------------------------------------
//
// ComRtGetProgramInfo()
//
// Outputs:
// 1) the pathname of the directory where the application program
//    is being run from.
//    For OSS processes, this will be the fully qualified oss directory
//      pathname.
//    For Guardian processes, pathname is not set
// 2) the process type (oss or guardian).
// 3) Other output values are: cpu, pin, nodename, nodename Len, processCreateTime
//       and processNameString in the format <\node_name>.<cpu>,<pin>
//
// // Return status:      0, if all ok. <errnum>, in case of an error.
//
// -----------------------------------------------------------------------
int ComRtGetProgramInfo(char *pathName,     /* out */
                        int pathNameMaxLen, /* in */
                        short &processType, /* out */
                        int &cpu,           /* cpu */
                        pid_t &pin,         /* pin */
                        int &nodeNumber,
                        char *nodeName,  // GuaNodeNameMaxLen+1
                        short &nodeNameLen, long &processCreateTime, char *processNameString,
                        char *parentProcessNameString = NULL, SB_Verif_Type *verifier = NULL, int *ancestorNid = NULL,
                        pid_t *ancestorPid = NULL);

// OUT: processPriority: current priority of process
int ComRtGetProcessPriority(int &processPriority /* out */);

int ComRtSetProcessPriority(int priority, NABoolean isDelta);

// OUT: pagesInUse: Pages(16k) currently in use by process
int ComRtGetProcessPagesInUse(long &pagesInUse /* out */);

// IN:  if cpu, pin and nodeName are passed in, is that to find process.
//      Otherwise, use current process
// OUT: processCreateTime: time when this process was created.
int ComRtGetProcessCreateTime(short *cpu, /* cpu */
                              pid_t *pin, /* pin */
                              short *nodeNumber, long &processCreateTime, short &errorDetail);

int ComRtGetIsoMappingEnum();
char *ComRtGetIsoMappingName();

// -----------------------------------------------------------------------
// Upshift a simple char string
// -----------------------------------------------------------------------
void ComRt_Upshift(char *buf);

const char *ComRtGetEnvValueFromEnvvars(const char **envvars, const char *envvar, int *envvarPos = NULL);

#if defined(_DEBUG)
// -----------------------------------------------------------------------
// Convenient handling of envvars: Return a value if one exists
// NB: DEBUG mode only!
// -----------------------------------------------------------------------
NABoolean ComRtGetEnvValue(const char *envvar, const char **envvarValue = NULL);

NABoolean ComRtGetEnvValue(const char *envvar, int *envvarValue);

NABoolean ComRtGetValueFromFile(const char *envvar, char *valueBuffer, const UInt32 valueBufferSizeInBytes);
#endif  // #if defined (_DEBUG) ...

// -----------------------------------------------------------------------
// Get the MX system catalog name.
// -----------------------------------------------------------------------
int ComRtGetMXSysVolName(char *sysCatBuffer,              /* out */
                         int inputBufferLength,           /* in  */
                         int *sysCatLength,               /* out */
                         const char *nodeName,            /* in */
                         NABoolean fakeReadError,         /* in */
                         NABoolean fakeCorruptAnchorError /* in */
);

// -----------------------------------------------------------------------
// Extract System MetaData Location ( VolumeName ).
// -----------------------------------------------------------------------
int extract_SMDLocation(char *buffer,       /* in */
                        int bufferLength,   /* in */
                        char *SMDLocation); /* out */

// -----------------------------------------------------------------------
// Validate MetaData Location ( VolumeName ) format.
// -----------------------------------------------------------------------
int validate_SMDLocation(char *SMDLocation); /* in */

// allocate and populate an array with entries for all the configured
// CPUs (Trafodion node ids) and return the number of CPUs. Usually,
// the array will contain node ids 0 ... n-1, but sometimes there may
// be holes in the assigned node ids, when CPUs (Linux nodes) get
// removed from the cluster.
int ComRtPopulatePhysicalCPUArray(int *&cpuArray, NAHeap *heap);

NABoolean ComRtGetCpuStatus(char *nodeName, short cpuNum);
int ComRtTransIdToText(long transId, char *buf, short len);

// A function to return the string "UNKNOWN (<val>)" which can be
// useful when displaying values from an enumeration and an unexpected
// value is encountered. The function is thread-safe. The returned
// string can be overwritten by another call to the function from the
// same thread.
const char *ComRtGetUnknownString(int val);

void genLinuxCorefile(const char *eventMsg);  // no-op except on Linux.

#ifdef _DEBUG
static THREAD_P UInt32 TraceAllocSize = 0;
void saveTrafStack(LIST(TrafAddrStack *) * la, void *addr);
bool delTrafStack(LIST(TrafAddrStack *) * la, void *addr);
void dumpTrafStack(LIST(TrafAddrStack *) * la, const char *header, bool toFile = false);
void displayCurrentStack(int depth);
void displayCurrentStack(ostream &, int depth);
#endif  // DEBUG

Int16 getBDRClusterName(char *bdrClusterName);

int get_phandle_with_retry(char *pname, SB_Phandle_Type *phandle);

fstream &getPrintHandle();
void genFullMessage(char *buf, int len, const char *className, int queryNodeId);

class RangeDate;
class RangeTime;

class EncodedHiveType {
 public:
  EncodedHiveType(const std::string &text);
  ~EncodedHiveType(){};

  int typeCode() const { return type_; }
  int length() const { return length_; }
  int precision() const { return precision_; }
  int scale() const { return scale_; }

 protected:
  int type_;
  int length_;
  int precision_;
  int scale_;
};

class filterRecord {
 public:
  filterRecord(UInt64 rc, UInt64 uec, UInt32 probes, NABoolean enabled, char *name, std::string &min, std::string &max,
               std::vector<std::string> &fs, UInt64 rowsToSurvive)
      : rowCount_(rc),
        uec_(uec),
        probes_(probes),
        enabled_(enabled),
        filterName_(name),
        minVal_(min),
        maxVal_(max),
        filter_(fs),
        rowsToSurvive_(rowsToSurvive) {}

  ~filterRecord() {}

  bool operator<(const filterRecord &rhs) const;

  const std::vector<std::string> &getFilter() const { return filter_; }
  const char *filterName() const { return filterName_; }

  Int16 getFilterId();

  void appendTerseFilterName(NAString &);

  UInt64 rowsToSelect() const;
  NABoolean isEnabled() const { return enabled_; };
  UInt64 rowCount() const { return rowCount_; };
  float selectivity() const;
  UInt64 rowsToSurvive() const { return rowsToSurvive_; };

  const std::string &getMinVal() const { return minVal_; }
  const std::string &getMaxVal() const { return maxVal_; }

  void display(ostream &, const char *msg) const;

 protected:
  UInt64 rowCount_;
  UInt64 uec_;
  UInt64 probes_;  // unique probes
  NABoolean enabled_;
  char *filterName_;
  std::vector<std::string> filter_;
  std::string minVal_;  // in format length(4 bytes) + ascii string
  std::string maxVal_;  // in format length(4 bytes) + ascii string
  UInt64 rowsToSurvive_;
};

struct packedBits {
  int filterId_ : 14;
  unsigned int flags_ : 2;
} __attribute__((packed));

class ScanFilterStats {
 protected:
  enum FilterState { FILTER_STATS_ENABLED = 0x01, FILTER_STATS_SELECTED = 0x02 };

 public:
  enum MergeSemantics { MAX, ADD, COPY };

 public:
  ScanFilterStats(Int16 id, long rows, NABoolean enabled, NABoolean selected = FALSE) : totalRowsAffected_(rows) {
    packedBits_.flags_ = 0;

    setIsEnabled(enabled);
    setIsSelected(selected);

    packedBits_.filterId_ = id;

    if (packedBits_.filterId_ < 0) packedBits_.filterId_ = -1;
  };

  ScanFilterStats(ScanFilterStats &s) : totalRowsAffected_(s.totalRowsAffected_) {
    packedBits_.filterId_ = s.packedBits_.filterId_;
    packedBits_.flags_ = s.packedBits_.flags_;
  }

  ScanFilterStats() : totalRowsAffected_(0) {
    packedBits_.filterId_ = -1;
    packedBits_.flags_ = 0;
  };

  UInt32 packedLength();
  UInt32 pack(char *&buffer);
  void unpack(const char *&buffer);

  void merge(const ScanFilterStats &, ScanFilterStats::MergeSemantics);

  Int16 getFilterId() const { return (Int16)packedBits_.filterId_; }

  void getVariableStatsInfo(char *buf, int len) const;
  int getVariableStatsInfoLen() const;

  void setIsEnabled(NABoolean x) {
    x ? packedBits_.flags_ |= FILTER_STATS_ENABLED : packedBits_.flags_ &= ~FILTER_STATS_ENABLED;
  }

  NABoolean getIsEnabled() const { return (packedBits_.flags_ & FILTER_STATS_ENABLED) != 0; }

  void setIsSelected(NABoolean x) {
    x ? packedBits_.flags_ |= FILTER_STATS_SELECTED : packedBits_.flags_ &= ~FILTER_STATS_SELECTED;
  }

  NABoolean getIsSelected() const { return (packedBits_.flags_ & FILTER_STATS_SELECTED) != 0; }

 protected:
  int getStateCode() const;

 private:
  long totalRowsAffected_;
  packedBits packedBits_;

} __attribute__((packed));

const int MAX_FILTER_STATS = 10;

class ScanFilterStatsList {
 public:
  ScanFilterStatsList() : entries_(0){};
  ScanFilterStatsList(ScanFilterStatsList &);

  ScanFilterStatsList &operator=(ScanFilterStatsList &other);

  UInt32 packedLength();
  UInt32 pack(char *&buffer);
  void unpack(const char *&buffer);

  void merge(ScanFilterStatsList &, ScanFilterStats::MergeSemantics);

  void getVariableStatsInfo(char *buf, int len, const char *msg = NULL) const;
  int getVariableStatsInfoLen(const char *msg = NULL) const;

  void dump(ostream &out, const char *msg = NULL);

  void addEntry(const ScanFilterStats &, ScanFilterStats::MergeSemantics);

  void clear() { entries_ = 0; }

  int entries() const { return (entries_ < MAX_FILTER_STATS) ? entries_ : MAX_FILTER_STATS; }

 protected:
  void merge(ScanFilterStats &source, ScanFilterStats::MergeSemantics);

 private:
  int entries_;
  ScanFilterStats scanFilterStats_[MAX_FILTER_STATS];
};

/*
void outputOrcSearchArgLessThanPred(std::string& text,
                                    const char* colName,
                                    const char* colType, int v);

void outputOrcSearchArgLessThanEqPred(std::string& text,
                                    const char* colName,
                                    const char* colType, int v);
*/

pid_t ComRtGetConfiguredPidMax();

long getCurrentTime();

// convert the 1st argument to a 11 byte string in following format.

// year: 2 bytes
// month: 1 byte
// day: 1 byte
// hour: 1 byte
// min: 1 byte
// second: 1-byte
// mllisec: integer (4-bytes)
//
int convertJulianTimestamp(long julianTimestamp, char *target);

class ClusterRole {
 public:
  enum RoleType { PRIMARY, SECONDARY };
  ClusterRole();
  ClusterRole(RoleType role);

  bool operator==(const ClusterRole &other) const;
  bool operator!=(const ClusterRole &other) const;

  static ClusterRole &get_role();

 private:
  RoleType role_;
};

#endif  // COMRTUTILS_H
