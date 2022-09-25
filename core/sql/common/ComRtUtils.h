/**********************************************************************
// @@@ START COPYRIGHT @@@
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// @@@ END COPYRIGHT @@@
**********************************************************************/
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ComRtUtils.h
 * Description:  Some common OS functions that are called by the
 *               executor (run-time) and may also b called by other components
 *
 * Created:      7/4/97
 * Language:     C++
 *
 *
 *****************************************************************************
 */
#ifndef COMRTUTILS_H
#define COMRTUTILS_H

#include "NABoolean.h"
#include "NAString.h"
#include "Platform.h"
#include "NAMemory.h"
#include "Int64.h"
#include "ComSmallDefs.h"
#include "ComCextMisc.h"

#include <fstream>
#include <string>
#include <set>
#include <vector>

using namespace std;
#include "seabed/ms.h"
#ifdef min
#undef min
#endif // min

#define MAX_SEGMENT_NAME_LEN  255
#define PROCESSNAME_STRING_LEN    40
#define PROGRAM_NAME_LEN    64
#define BDR_CLUSTER_NAME_LEN 24
#define BDR_CLUSTER_NAME_KEY "BDR_CLUSTER"


// Keep in sync with common/ComRtUtils.cpp ComRtGetModuleFileName():
//                               0123456789*123456789*123456789*1: position0-31
#define systemModulePrefix	    "HP_SYSTEM_CATALOG.SYSTEM_SCHEMA."
#define systemModulePrefixLen	32
#define systemModulePrefixODBC	"HP_SYSTEM_CATALOG.MXCS_SCHEMA."
#define systemModulePrefixLenODBC	30

//const NAString InternalModNameList;

// returns TRUE, if modName is an internal module name
NABoolean ComRtIsInternalModName(const char * modName);

// returns 'next' internal mod name.
// 'index' keeps track of the current mod name returned. It should
// be initialized to 0 on the first call to this method.
const char * ComRtGetNextInternalModName(Lng32 &index, char * modNameBuf);

NABoolean getLinuxGroups(const char *userName,
                         std::set<std::string> &userGroups);

// -----------------------------------------------------------------------
// Class to read an oss file by oss path name or Guardian File name
// -----------------------------------------------------------------------

#include <iosfwd>
using namespace std;

class ModuleOSFile
{
public:

  ModuleOSFile();
  ~ModuleOSFile();
  Int32 open(const char *fname);
  Int32 openGuardianFile (const char *fname);
  Int32 close();
  Int32 readpos(char *buf, Lng32 pos, Lng32 len, short &countRead);

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
Lng32 ComRtGetInstallDir(
     char *buffer,
     Lng32 inputBufferLength,
     Lng32 *resultLength);

// -----------------------------------------------------------------------
// Convert a 3-part module name into the file name in which the module
// is stored
// -----------------------------------------------------------------------
Lng32 ComRtGetModuleFileName(
     const char *moduleName,
     const char *moduleDir,
     char *buffer,
     Lng32 inputBufferLength,
     char * sysModuleDir,  // location of SYSTEMMODULES
     char * userModuleDir, // location of USERMODULES
     Lng32 *resultLength,
     short &isSystemModule);

// -----------------------------------------------------------------------
// Get the cluster (EXPAND node) name (returns "NSK" on NT)
// -----------------------------------------------------------------------
Lng32 ComRtGetOSClusterName(
     char *buffer,
     Lng32 inputBufferLength,
     Lng32 *resultLength,
     short * nodeNumber = NULL);

// -----------------------------------------------------------------------
// Get the MP system catalog name.
// -----------------------------------------------------------------------
Lng32 ComRtGetMPSysCatName(
     char *sysCatBuffer,    /* out */
     Lng32 inputBufferLength,/* in  */
     char *inputSysName,    /* in must set to NULL if no name is passed */
     Lng32 *sysCatLength,    /* out */
     short *detailError,    /* out */
     NAMemory *heap = 0 );  /* in  */

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
inline Int64 ComRtGetJulianFromUTC(timespec ts)
{
  return (((ts.tv_sec  + (2440588LL * 86400LL) - 43200LL) * 1000000LL)
                + (ts.tv_nsec / 1000)) ;
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
Lng32 ComRtGetProgramInfo(char * pathName,    /* out */
			 Lng32 pathNameMaxLen, /* in */
			 short  &processType, /* out */
			 Int32  &cpu, /* cpu */
			 pid_t  &pin, /* pin */
			 Lng32   &nodeNumber,
			 char * nodeName, // GuaNodeNameMaxLen+1
			 short  &nodeNameLen,
			 Int64  &processCreateTime,
			  char *processNameString,
			  char *parentProcessNameString = NULL
                         , SB_Verif_Type *verifier = NULL
                         , Int32 *ancestorNid = NULL
                         , pid_t *ancestorPid = NULL
                          );

// OUT: processPriority: current priority of process
Lng32 ComRtGetProcessPriority(Lng32  &processPriority /* out */);

Lng32 ComRtSetProcessPriority(Lng32 priority,
			     NABoolean isDelta);

// OUT: pagesInUse: Pages(16k) currently in use by process
Lng32 ComRtGetProcessPagesInUse(Int64  &pagesInUse /* out */);

// IN:  if cpu, pin and nodeName are passed in, is that to find process.
//      Otherwise, use current process
// OUT: processCreateTime: time when this process was created.
Lng32 ComRtGetProcessCreateTime(short  *cpu, /* cpu */
			       pid_t  *pin, /* pin */
			       short  *nodeNumber,
			       Int64  &processCreateTime,
			       short  &errorDetail
			       );


Lng32 ComRtGetIsoMappingEnum();
char * ComRtGetIsoMappingName();

// -----------------------------------------------------------------------
// Upshift a simple char string
// -----------------------------------------------------------------------
void ComRt_Upshift (char * buf);

const char * ComRtGetEnvValueFromEnvvars(const char ** envvars,
					 const char * envvar,
					 Lng32 * envvarPos = NULL);

#if defined (_DEBUG)
// -----------------------------------------------------------------------
// Convenient handling of envvars: Return a value if one exists
// NB: DEBUG mode only!
// -----------------------------------------------------------------------
NABoolean ComRtGetEnvValue(const char * envvar, const char ** envvarValue = NULL);

NABoolean ComRtGetEnvValue(const char * envvar, Lng32 * envvarValue);

NABoolean ComRtGetValueFromFile (const char * envvar, char * valueBuffer,
                                 const UInt32 valueBufferSizeInBytes);
#endif // #if defined (_DEBUG) ...

// -----------------------------------------------------------------------
// Get the MX system catalog name.
// -----------------------------------------------------------------------
Lng32 ComRtGetMXSysVolName(
     char *sysCatBuffer,                /* out */
     Lng32 inputBufferLength,            /* in  */
     Lng32 *sysCatLength,                /* out */
     const char *nodeName,              /* in */
     NABoolean fakeReadError,           /* in */
     NABoolean fakeCorruptAnchorError   /* in */
     );

// -----------------------------------------------------------------------
// Extract System MetaData Location ( VolumeName ).
// -----------------------------------------------------------------------
Lng32 extract_SMDLocation(
     char *buffer, /* in */
     Int32 bufferLength, /* in */
     char *SMDLocation); /* out */

// -----------------------------------------------------------------------
// Validate MetaData Location ( VolumeName ) format.
// -----------------------------------------------------------------------
Lng32 validate_SMDLocation(
      char *SMDLocation); /* in */

// allocate and populate an array with entries for all the configured
// CPUs (Trafodion node ids) and return the number of CPUs. Usually,
// the array will contain node ids 0 ... n-1, but sometimes there may
// be holes in the assigned node ids, when CPUs (Linux nodes) get
// removed from the cluster.
Int32 ComRtPopulatePhysicalCPUArray(Int32 *&cpuArray, NAHeap *heap);

NABoolean ComRtGetCpuStatus(char *nodeName, short cpuNum);
Lng32 ComRtTransIdToText(Int64 transId, char *buf, short len);

// A function to return the string "UNKNOWN (<val>)" which can be
// useful when displaying values from an enumeration and an unexpected
// value is encountered. The function is thread-safe. The returned
// string can be overwritten by another call to the function from the
// same thread.
const char *ComRtGetUnknownString(Int32 val);

void genLinuxCorefile(const char *eventMsg);   // no-op except on Linux.

#ifdef _DEBUG
static THREAD_P UInt32 TraceAllocSize = 0;
void saveTrafStack(LIST(TrafAddrStack*) *la, void *addr);
bool delTrafStack(LIST(TrafAddrStack*) *la, void *addr);
void dumpTrafStack(LIST(TrafAddrStack *) *la, const char *header, bool toFile = false);
void displayCurrentStack(Int32 depth);
void displayCurrentStack(ostream&, Int32 depth);
#endif // DEBUG


Int16 getBDRClusterName(char *bdrClusterName);

int get_phandle_with_retry(char *pname, SB_Phandle_Type *phandle);

NABoolean getHDFSDefaultName(NAString& host, Int32& port);

fstream& getPrintHandle();
void genFullMessage(char* buf, Int32 len, const char* className, Int32 queryNodeId);

class RangeDate;
class RangeTime;

class EncodedHiveType
{
public:
  EncodedHiveType(const std::string& text);
  ~EncodedHiveType() {};

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

class OrcSearchArg 
{
public: 
   enum SearchOperatorType 
   {
     UNKNOWN_OPER = ExtPushdownOperatorType::UNKNOWN_OPER, // 0
     EQUALS = ExtPushdownOperatorType::EQUALS, // 5
     LESSTHAN = ExtPushdownOperatorType::LESSTHAN, // 6
     LESSTHANEQUALS = ExtPushdownOperatorType::LESSTHANEQUALS, //7
     ISNULL = ExtPushdownOperatorType::ISNULL, //8
     BLOOMFILTER = ExtPushdownOperatorType::BLOOMFILTER, // 10

     // The following operator types are not available
     // from Hadoop Java Parquet push-down type.
     GREATERTHAN = ExtPushdownOperatorType::LAST, //== 11
     GREATERTHANEQUALS,  // == 12
     ISNOTNULL // == 13
   };

public: 
   OrcSearchArg():
      operatorType_(SearchOperatorType::UNKNOWN_OPER),
      colNameLen_(0),
      colName_(NULL),
      operandType_(0),
      operandLen_(0),
      operand_(NULL),
      colTypeIndicator_(NULL),
      filterId_(0),
      expectedRows_(0)
   {};

   OrcSearchArg(const OrcSearchArg& other):
      operatorType_(other.operatorType_),
      colNameLen_(other.colNameLen_),
      colName_(other.colName_),
      operandType_(other.operandType_),
      operandLen_(other.operandLen_),
      operand_(other.operand_),
      colTypeIndicator_(other.colTypeIndicator_),
      filterId_(other.filterId_),
      expectedRows_(other.expectedRows_)
   {};

   OrcSearchArg(const std::string&);

   ~OrcSearchArg() {};

   void appendOperator(ExtPushdownOperatorType t);
   void appendColName(const char* colName);
   void appendTypeIndicator(const char* colType);

   void appendValue(Int32 v);
   void appendValue(Int64 v);
   void appendValue(double v);
   void appendValue(const char*);
   void appendValue(const char*, Lng32 len);
   void appendValue(const wchar_t*);

   void appendValue(RangeDate*);
   void appendValue(RangeTime*);

   std::string& getText() { return text_; };

   static std::string extractValueFromPredicate(const std::string& source);

   static void outputOperatorControl(std::vector<std::string>& text, 
                                     ExtPushdownOperatorType t);

   // methods to reverse parse a text_ of encodeded search arg
   NABoolean isEqualTypePred() const; // true for EQUALS
   NABoolean isLessThanPred() const; // true for LESSTHAN 
   NABoolean isGreaterThanPred() const; // true for GREATER 
   NABoolean isLessThanEqualPred() const; // true for LESSTHANEQUALS
   NABoolean isGreaterThanEqualPred() const; // true for GREATERTHANEQUALS

   NABoolean isSimplePushdownPredicate() const; 

   NABoolean isNullPredicate() const
   { return ( operatorType_ == ExtPushdownOperatorType::ISNULL); }

   NABoolean isINPredicate() const
   { return ( operatorType_ == ExtPushdownOperatorType::IN); }

   NABoolean isBloomFilterPredicate() const
   { return ( operatorType_ == ExtPushdownOperatorType::BLOOMFILTER); }

   NABoolean isSTART_AND() const
   { return ( operatorType_ == ExtPushdownOperatorType::STARTAND); }

   NABoolean isSTART_OR() const 
   { return ( operatorType_ == ExtPushdownOperatorType::STARTOR); }

   NABoolean isSTART_NOT() const 
   { return ( operatorType_ == ExtPushdownOperatorType::STARTNOT); }

   NABoolean isEND() const
   { return ( operatorType_ == ExtPushdownOperatorType::END); }

   NABoolean matchColName(const std::string& name) const; 

   std::string operandAsStdstring() const 
       { return std::string(operand_, operandLen_); };

   const char* operand() const { return operand_; };
   Int32 operandLen() const { return operandLen_; };

   const char* colName() const { return colName_; };
   Int32 colNameLen() const { return colNameLen_; };

   Int32 getOperatorType() const { return operatorType_; }

   void reverseOperatorTypeForNot();

   bool checkOperandAsBool(NABoolean forced = FALSE) const;

   void display();

   Int32 getFilterId() const { return filterId_; }

   void setExpectedRows(UInt64 x) { expectedRows_ = x; }
   UInt64 getExpectedRows() const { return expectedRows_; }

protected:
   std::string text_;

   Int32 operatorType_;
   Int32 colNameLen_;
   const char* colName_;

   Int32 colTypeIndicator_;

   Int32 operandType_;
   Int32 operandLen_;
   const char* operand_;

   Int32 filterId_;
   std::string min_;
   std::string max_;

   UInt64 expectedRows_;
};

typedef std::vector<OrcSearchArg> OrcSearchArgVec;


class filterRecord
{
public: 
   filterRecord(UInt64 rc, UInt64 uec, UInt32 probes, NABoolean enabled,
                char* name, std::string& min, std::string& max,
                std::vector<std::string>& fs, 
                UInt64 rowsToSurvive):
      rowCount_(rc), uec_(uec), probes_(probes), enabled_(enabled), 
      filterName_(name), minVal_(min), maxVal_(max), filter_(fs),
      rowsToSurvive_(rowsToSurvive)
   {}

   ~filterRecord() {}

   bool operator<(const filterRecord& rhs) const;

   const std::vector<std::string>& getFilter() const { return filter_; }
   const char* filterName() const { return filterName_; }

   Int16 getFilterId();

   void appendTerseFilterName(NAString&);

   UInt64 rowsToSelect() const;
   NABoolean isEnabled() const { return enabled_; };
   UInt64 rowCount() const { return rowCount_; };
   float selectivity() const;
   UInt64 rowsToSurvive() const { return rowsToSurvive_; };

   const std::string& getMinVal() const { return minVal_; }
   const std::string& getMaxVal() const { return maxVal_; }

   void display(ostream&, const char* msg) const;

protected:
   UInt64 rowCount_;
   UInt64 uec_;
   UInt64 probes_; // unique probes
   NABoolean enabled_;
   char*  filterName_;
   std::vector<std::string> filter_;
   std::string minVal_; // in format length(4 bytes) + ascii string
   std::string maxVal_; // in format length(4 bytes) + ascii string
   UInt64 rowsToSurvive_;
};
  
struct packedBits {
   int filterId_ : 14;
   unsigned int flags_ : 2;
} __attribute__((packed));

class ScanFilterStats {

protected:
   enum FilterState {
     FILTER_STATS_ENABLED  = 0x01,
     FILTER_STATS_SELECTED = 0x02
   };

public:
   enum MergeSemantics { MAX, ADD, COPY };

public:
  ScanFilterStats(Int16 id, Int64 rows, 
                  NABoolean enabled, NABoolean selected=FALSE) :
    totalRowsAffected_(rows) 
   {
     packedBits_.flags_ = 0;

     setIsEnabled(enabled);
     setIsSelected(selected);

     packedBits_.filterId_ = id; 

     if ( packedBits_.filterId_ < 0 ) packedBits_.filterId_ = -1;
   };

  ScanFilterStats(ScanFilterStats& s) :
    totalRowsAffected_(s.totalRowsAffected_) 
  {
    packedBits_.filterId_ = s.packedBits_.filterId_;
    packedBits_.flags_ = s.packedBits_.flags_;
  }

  ScanFilterStats() :
    totalRowsAffected_(0) 
  {
    packedBits_.filterId_ = -1;
    packedBits_.flags_ = 0;
  };

  UInt32 packedLength();
  UInt32 pack(char * &buffer);
  void unpack(const char* &buffer);

  void merge(const ScanFilterStats&, ScanFilterStats::MergeSemantics);

  Int16 getFilterId() const { return (Int16)packedBits_.filterId_; }

  void getVariableStatsInfo(char* buf, Int32 len) const;
  Lng32 getVariableStatsInfoLen() const;

  void setIsEnabled(NABoolean x) 
   { x ? 
      packedBits_.flags_  |= FILTER_STATS_ENABLED : 
      packedBits_.flags_ &= ~FILTER_STATS_ENABLED;      
   }

  NABoolean getIsEnabled() const
   { return (packedBits_.flags_ & FILTER_STATS_ENABLED) != 0; }

  void setIsSelected(NABoolean x)
   { x ? 
      packedBits_.flags_  |= FILTER_STATS_SELECTED : 
      packedBits_.flags_ &= ~FILTER_STATS_SELECTED;
   }

  NABoolean getIsSelected() const
   { return (packedBits_.flags_ & FILTER_STATS_SELECTED) != 0; }

protected:
  Int32 getStateCode() const;

private:
  Int64 totalRowsAffected_; 
  packedBits packedBits_;

} __attribute__((packed));

const int MAX_FILTER_STATS = 10;

class ScanFilterStatsList {

public:
  ScanFilterStatsList() : entries_(0) {};
  ScanFilterStatsList(ScanFilterStatsList&);

  ScanFilterStatsList& operator=(ScanFilterStatsList& other);

  UInt32 packedLength();
  UInt32 pack(char * &buffer);
  void unpack(const char* &buffer);

  void merge(ScanFilterStatsList&, ScanFilterStats::MergeSemantics);

  void getVariableStatsInfo(char* buf, Int32 len, const char* msg = NULL) const;
  Lng32 getVariableStatsInfoLen(const char* msg = NULL) const;

  void dump(ostream& out, const char* msg = NULL);

  void addEntry(const ScanFilterStats&, ScanFilterStats::MergeSemantics);

  void clear() { entries_ = 0; }

  Int32 entries() const { 
     return ( entries_ < MAX_FILTER_STATS ) ? 
               entries_ : MAX_FILTER_STATS ; 
                  }

protected:
   void merge(ScanFilterStats& source, ScanFilterStats::MergeSemantics);

private:

  Int32 entries_;
  ScanFilterStats scanFilterStats_[MAX_FILTER_STATS];
};


/*
void outputOrcSearchArgLessThanPred(std::string& text, 
                                    const char* colName, 
                                    const char* colType, Int32 v);

void outputOrcSearchArgLessThanEqPred(std::string& text, 
                                    const char* colName, 
                                    const char* colType, Int32 v);
*/

pid_t ComRtGetConfiguredPidMax();

Int64 getCurrentTime();

// convert the 1st argument to a 11 byte string in following format.

// year: 2 bytes
// month: 1 byte 
// day: 1 byte 
// hour: 1 byte 
// min: 1 byte 
// second: 1-byte 
// mllisec: integer (4-bytes)
//
Int32 convertJulianTimestamp(Int64 julianTimestamp, char* target);

class ClusterRole {
public:
  enum RoleType {PRIMARY, SECONDARY};
  ClusterRole();
  ClusterRole(RoleType role);

  bool operator==(const ClusterRole& other) const;
  bool operator!=(const ClusterRole& other) const;

  static ClusterRole& get_role();

private:
  RoleType role_;
};


#endif // COMRTUTILS_H
