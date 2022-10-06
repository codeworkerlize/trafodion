/* -*-C++-*-
***********************************************************************
*
* File:         OptimizerSimulator.h
* Description:  This file is the header file for Optimizer Simulator
*               component (OSIM).
*
* Created:      12/2006
* Language:     C++
*
*
**********************************************************************/


#ifndef __OPTIMIZERSIMULATOR_H
#define __OPTIMIZERSIMULATOR_H

#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <fstream>
#include "common/CmpCommon.h"
#include "common/BaseTypes.h"
#include "common/CollHeap.h"
#include "qmscommon/XMLUtil.h"
#include "common/NAClusterInfo.h"

// forward declaration to allow usage of NATable *
class NATable;
class ExeCliInterface;
class CmpSeabaseDDL;
class Queue;

class OsimAllHistograms;
class OsimHistogramEntry;
class HHDFSStatsBase;

class OsimAllHistograms : public XMLElement {
 public:
  static const char elemName[];
  OsimAllHistograms(NAMemory *heap = 0) : XMLElement(NULL, heap), list_(heap) { setParent(this); }

  virtual const char *getElementName() const { return elemName; }

  virtual ElementType getElementType() const { return ElementType::ET_List; }

  NAList<OsimHistogramEntry *> &getHistograms() { return list_; }

  void addEntry(const char *fullpath, const char *username, const char *pid, const char *cat, const char *sch,
                const char *table, const char *histogram);

 protected:
  virtual void serializeBody(XMLString &xml);
  virtual void startElement(void *parser, const char *elementName, const char **atts);

 private:
  // disable copy constructor
  OsimAllHistograms(const OsimAllHistograms &);
  // disable assign operator
  OsimAllHistograms &operator=(const OsimAllHistograms &);
  NAList<OsimHistogramEntry *> list_;
};

// represent one histogram file xxx.histograms or xxx.sb_histogram_interval
class OsimHistogramEntry : public XMLElement {
  friend class OsimAllHistograms;

 public:
  static const char elemName[];
  OsimHistogramEntry(XMLElementPtr parent, NAMemory *heap)
      : XMLElement(parent, heap),
        currentTag_(heap),
        fullPath_(heap),
        userName_(heap),
        pid_(heap),
        catalog_(heap),
        schema_(heap),
        table_(heap),
        histogram_(heap) {}

  virtual const char *getElementName() const { return elemName; }
  virtual ElementType getElementType() const { return ElementType::ET_Table; }

  NAString &getFullPath() { return fullPath_; }
  NAString &getUserName() { return userName_; }
  NAString &getPID() { return pid_; }
  NAString &getCatalog() { return catalog_; }
  NAString &getSchema() { return schema_; }
  NAString &getTable() { return table_; }
  NAString &getHistogram() { return histogram_; }

 protected:
  virtual void serializeBody(XMLString &xml);
  virtual void endElement(void *parser, const char *elementName);
  virtual void charData(void *parser, const char *data, int len);
  virtual void startElement(void *parser, const char *elementName, const char **atts);

 private:
  NAString currentTag_;
  NAString fullPath_;
  NAString userName_;
  NAString pid_;
  NAString catalog_;
  NAString schema_;
  NAString table_;
  NAString histogram_;
  // disable copy constructor and assignment.
  OsimHistogramEntry(const OsimHistogramEntry &);
  OsimHistogramEntry &operator=(const OsimHistogramEntry &);
};
// for reading histogram files path
class OsimElementMapper : public XMLElementMapper {
 public:
  virtual XMLElementPtr operator()(void *parser, char *elementName, AttributeList atts);
};

class OsimNAClusterInfoLinux : public NAClusterInfoLinux {
 public:
  OsimNAClusterInfoLinux(CollHeap *heap);
  void simulateNAClusterInfo();
  void simulateNAClusterInfoLinux();
};

// This class initializes OSIM and implements capture and simulation
// OSIM is a single-threaded singleton.
class OptimizerSimulator : public NABasicObject {
 public:
  // The default OSIM mode is OFF and is set to either CAPTURE or
  // SIMULATE by an environment variable OSIM_MODE.
  enum osimMode { OFF, CAPTURE, SIMULATE, LOAD, UNLOAD };

  enum OsimLog {
    FIRST_LOG,
    ESTIMATED_ROWS = FIRST_LOG,
    NODE_AND_CLUSTER_NUMBERS,
    NACLUSTERINFO,
    MYSYSTEMNUMBER,
    VIEWSFILE,
    VIEWDDLS,
    TABLESFILE,
    CREATE_SCHEMA_DDLS,
    CREATE_TABLE_DDLS,
    SYNONYMSFILE,
    SYNONYMDDLS,
    CQD_DEFAULTSFILE,
    QUERIESFILE,
    VERSIONSFILE,
    CAPTURE_SYS_TYPE,
    HISTOGRAM_PATHS,
    HIVE_HISTOGRAM_PATHS,
    HIVE_CREATE_TABLE,
    HIVE_CREATE_EXTERNAL_TABLE,
    HIVE_TABLE_LIST,
    TENANT,
    NUM_OF_LOGS
  };

  // sysType indicates the type of system that captured the queries
  enum sysType { OSIM_UNKNOWN_SYSTYPE, OSIM_NSK, OSIM_WINNT, OSIM_LINUX };
  // Constructor
  OptimizerSimulator(CollHeap *heap);
  // Accessor methods
  osimMode getOsimMode() { return osimMode_; }

  // Accessor methods to get and set osimLogHDFSDir_.
  void setOsimLogdir(const char *localdir);
  const char *getOsimLogdir() const { return osimLogLocalDir_.isNull() ? NULL : osimLogLocalDir_.data(); }
  void initHashDictionaries();
  void createLogFilepath(OsimLog sc);
  void openWriteLogStreams(OsimLog sc);
  void initLogFilePaths();

  void capture_CQDs();
  void capture_TableOrView(NATable *naTab);

  void capturePrologue();
  void captureQueryText(const char *query);
  void captureQueryShape(const char *shape);

  void cleanup();
  void cleanupSimulator();
  void cleanupAfterStatement();

  void errorMessage(const char *msg);
  void warningMessage(const char *msg);
  void debugMessage(const char *format, ...);

  NABoolean setOsimModeAndLogDir(osimMode mode, const char *localdir);
  NABoolean readHiveStmt(ifstream &DDLFile, NAString &stmt, NAString &comment);
  NABoolean readStmt(ifstream &DDLFile, NAString &stmt, NAString &comment);
  NABoolean massageTableUID(OsimHistogramEntry *entry, NAHashDictionary<NAString, QualifiedName> *modifiedPathList,
                            NABoolean isHive);
  NABoolean isCallDisabled(int callBitPosition);

  NABoolean runningSimulation();
  NABoolean runningInCaptureMode();
  NABoolean runningInLoadMode();

  NAHashDictionary<const QualifiedName, long> *getHashDictTables() { return hashDict_Tables_; }

  NAHashDictionary<const QualifiedName, long> *getHashDictViews() { return hashDict_Views_; }

  NAHashDictionary<const QualifiedName, long> *getHashDictHiveTables() { return hashDict_HiveTables_; }

  void readSysCallLogfiles();

  void capture_getEstimatedRows(const char *tableName, double estRows);
  void readLogfile_getEstimatedRows();
  double simulate_getEstimatedRows(const char *tableName);
  void simulate_getNodeAndClusterNumbers(short &nodeNum, int &clusterNum);
  void readLogFile_getNodeAndClusterNumbers();
  void capture_getNodeAndClusterNumbers(short &nodeNum, int &clusterNum);
  void log_getNodeAndClusterNumbers();

  void readLogFile_captureSysType();
  void captureSysType();
  sysType getCaptureSysType();

  void captureTenant();
  void readLogFile_Tenant();

  void capture_MYSYSTEMNUMBER(short sysNum);
  void readLogfile_MYSYSTEMNUMBER();
  short simulate_MYSYSTEMNUMBER();

  NABoolean isClusterInfoInitialized() { return clusterInfoInitialized_; }

  void setClusterInfoInitialized(NABoolean b) { clusterInfoInitialized_ = b; }

  // File pathnames for log files that contain system call data.
  const char *getLogFilePath(OsimLog index) const { return logFilePaths_[index]; }

  void setForceLoad(NABoolean b) { forceLoad_ = b; }
  NABoolean isForceLoad() const { return forceLoad_; }

 private:
  void readAndSetCQDs();
  void enterSimulationMode();
  void buildHistogramUnload(NAString &query, const QualifiedName *name, long tableUID);
  void buildHistogramIntervalUnload(NAString &query, const QualifiedName *name, long tableUID);
  void buildHistogramCreate(NAString &query, const QualifiedName *name, NABoolean isHive);
  void buildHistogramUpsert(NAString &query, const QualifiedName *name, NABoolean isHive);
  void buildHiveHistogramIntervalCreate(NAString &query, const QualifiedName *name);
  void buildHistogramIntervalCreate(NAString &query, const QualifiedName *name, NABoolean isHive);
  void buildHistogramIntervalUpsert(NAString &query, const QualifiedName *name, NABoolean isHive);
  void dumpHistograms();
  void dumpDDLs(const QualifiedName &qualifiedName);
  void initializeCLI();
  void loadHistograms(const char *histogramPath, NABoolean isHive);
  long getTableUID(const char *catName, const char *schName, const char *objName);
  short fetchAllRowsFromMetaContext(Queue *&q, const char *query);
  short executeFromMetaContext(const char *query);
  void loadDDLs();
  void histogramHDFSToLocal();
  void removeHDFSCacheDirectory();
  void createLogDir();
  void dropObjects();
  void dumpVersions();

  // This is the directory OSIM uses to read/write log files.
  NAString osimLogLocalDir_;  // OSIM dir in local disk, used during capture and simu mode.
  NAString hiveTableStatsDir_;
  // This is the mode under which OSIM is running (default is OFF).
  // It is set by an environment variable OSIM_MODE.
  osimMode osimMode_;

  // System call names.
  static const char *logFileNames_[NUM_OF_LOGS];

  char *logFilePaths_[NUM_OF_LOGS];

  ofstream *writeLogStreams_[NUM_OF_LOGS];

  NAHashDictionary<NAString, double> *hashDict_getEstimatedRows_;
  NAHashDictionary<const QualifiedName, long> *hashDict_Views_;
  NAHashDictionary<const QualifiedName, long> *hashDict_Tables_;
  NAHashDictionary<const QualifiedName, int> *hashDict_Synonyms_;
  NAHashDictionary<const QualifiedName, long> *hashDict_HiveTables_;

  short nodeNum_;
  int clusterNum_;
  short mySystemNumber_;
  sysType captureSysType_;
  NABoolean capturedNodeAndClusterNum_;
  NABoolean capturedInitialData_;
  NABoolean hashDictionariesInitialized_;
  NABoolean clusterInfoInitialized_;
  NABoolean tablesBeforeActionInitilized_;
  NABoolean viewsBeforeActionInitilized_;
  NABoolean CLIInitialized_;
  CmpSeabaseDDL *cmpSBD_;
  ExeCliInterface *cliInterface_;
  Queue *queue_;
  // for debugging
  int sysCallsDisabled_;
  // in respond to force option of osim load,
  // e.g. osim load from '/xxx/xxx/osim-dir', force
  // if true, when loading osim tables/views/indexes
  // existing objects with same qualified name
  // will be droped first
  NABoolean forceLoad_;
  CollHeap *heap_;
};

// System call wrappers.
short OSIM_MYSYSTEMNUMBER();
void OSIM_getNodeAndClusterNumbers(short &nodeNum, int &clusterNum);
void OSIM_captureTableOrView(NATable *naTab);
void OSIM_capturePrologue();
void OSIM_captureQueryShape(const char *shape);
// errorMessage and warningMessage wrappers.
void OSIM_errorMessage(const char *msg);
void OSIM_warningMessage(const char *msg);

// Check this, the parent and all ancestor CmpContext
// to find out if one of them is running in Load mode.
NABoolean OSIM_runningLoadEmbedded();

NABoolean OSIM_runningSimulation();
NABoolean OSIM_runningLoad();
NABoolean OSIM_runningInCaptureMode();
NABoolean OSIM_ustatIsDisabled();
NABoolean OSIM_ClusterInfoInitialized();
void raiseOsimException(const char *fmt, ...);
#endif
