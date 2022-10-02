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
#ifndef _HDFSHOOK_H
#define _HDFSHOOK_H

// HDFS-level statistics for Hive tables (Hive HDFS = HHDFS)
//

// We assume a Hive table to look like this in HDFS
//
// + top-level directory for Hive table
// |
// ---> + partition directories for partitioned tables only
//      |
//      ---> + buckets (_<bucket> file suffixes) for bucketed tables only
//           |
//           ---> + files (one or more HDFS files with data)
//                |
//                ---> + blocks of the file
//                     |
//                     ---> list of hosts with replicas for a given block

#include "Collections.h"
#include "NAString.h"
#include "CmpContext.h"
#include "ComCompressionInfo.h"
#include "hdfs.h"
#include <stdio.h>
#include <jni.h>

// forward declarations
struct hive_tbl_desc;
struct hive_tblparams_desc;
class HivePartitionAndBucketKey;
class NodeMap;
class HiveNodeMapEntry;
class NodeMapIterator;
class HHDFSTableStats;
class HHDFSPartitionStats;
class OsimHHDFSStatsBase;
class OptimizerSimulator;
class ExLobGlobals;

typedef CollIndex HostId;
typedef Int64 BucketNum;
typedef Int64 BlockNum;
typedef Int64 Offset;

extern void* addFileToBucket(void *args);

class HHDFSDiags
{
public:

 HHDFSDiags() : success_(TRUE), skipCount_(0) { }
  NABoolean isSuccess() const { return success_; }
  const char *getErrLoc() const { return errLoc_; }
  const char *getErrMsg() const { return errMsg_; }
  const char *getSkippedFiles() const { return skippedFiles_; }
  const Int32  getSkipCount() const {return skipCount_; }
  void incSkipCount() {skipCount_++ ; }
  void recordError(const char *errMsg,
                   const char *errLoc = NULL);
  void moveErrorToSkipFiles(const char* fileName);
  void reset() { errLoc_ = ""; errMsg_ = ""; skippedFiles_ = "" ; 
    success_ = TRUE; skipCount_ = 0; }

private:
  NABoolean success_;
  NAString errLoc_;
  NAString errMsg_;
  NAString skippedFiles_;
  Int32 skipCount_;
};

 
class threadedOrcFileInfoDesc
{
public:
  Int32 pid;
  Int32 numFiles;
  Int32 numThreads;
  NABoolean doEstimate;
  hdfsFS fs;
  char recordTerminator;
  NABoolean isORC;
  NABoolean isParquet;
  HHDFSDiags diags;
  Int32 filesEstimated;
  Int32 filesToEstimate;
  hdfsFileInfo *fileInfos;
  HHDFSPartitionStats *partStats;
  CmpContext* context;
};

typedef NAHashDictionaryIterator<NAString, NAString> 
        statsCacheItor;

class 
FileStatsCache : public NAHashDictionary<NAString, NAString> 
{

public: 
   FileStatsCache(NAMemory* heap);
   ~FileStatsCache();

   NABoolean populated() { return populated_; };

   void populateCache(hive_tbl_desc *htd, HHDFSDiags& diags);

protected:
   NABoolean populated_;
};

class HHDFSStatsBase : public NABasicObject
{
  friend class OsimHHDFSStatsBase;
public:
  HHDFSStatsBase(HHDFSTableStats *table) : numBlocks_(0),
                                           numFiles_(0),
                                           totalRows_(0),
                                           totalStringLengths_(0),
                                           totalSize_(0),
                                           numSplitUnits_(0),
                                           modificationTS_(0),
                                           sampledBytes_(0),
                                           sampledRows_(0),
                                           numLocalBlocks_(0),
                                           table_(table) {}

  void add(const HHDFSStatsBase *o);
  void subtract(const HHDFSStatsBase *o);

  Int64 getTotalSize() const { return totalSize_; }
  Int64 getNumFiles() const { return numFiles_; }
  Int64 getNumBlocks() const { return numBlocks_; }
  Int64 getTotalRows() const { return totalRows_; }
  Int64 getTotalStringLengths() { return totalStringLengths_; }
  Int64 getNumSplitUnits() const { return numSplitUnits_; }
  Int64 getSampledBytes() const { return sampledBytes_; }
  Int64 getSampledRows() const { return sampledRows_; }
  Int64 getNumLocalBlocks() const { return numLocalBlocks_; }
  time_t getModificationTS() const { return modificationTS_; }
  Int64 getEstimatedBlockSize() const;
  Int64 getEstimatedRowCount() const;
  Int64 getEstimatedRecordLength() const;
  void display();
  void print(FILE *ofd, const char *msg);
  const HHDFSTableStats *getTable() const { return table_; }
  HHDFSTableStats *getTable() { return table_; }
  NABoolean blocksAreOnLocalCluster() const { return numLocalBlocks_ == numBlocks_; }
  
  virtual OsimHHDFSStatsBase* osimSnapShot(NAMemory * heap){ return NULL; }

protected:
  Int64 numBlocks_;
  Int64 numFiles_;
  Int64 totalRows_;  
  Int64 numSplitUnits_;  // for ORC/Parquet files
  Int64 totalStringLengths_;  
  Int64 totalSize_;
  time_t modificationTS_; // last modification time of this object (file, partition/directory, bucket or table)
  Int64 sampledBytes_;
  Int64 sampledRows_;
  Int64 numLocalBlocks_; // blocks on nodes of the Trafodion cluster
  HHDFSTableStats *table_;
};

class HHDFSFileStats : public HHDFSStatsBase
{
  friend class OsimHHDFSFileStats;
public:
  enum PrimarySplitUnit
  {
    UNKNOWN_SPLIT_UNIT_,
    SPLIT_AT_FILE_LEVEL_,
    SPLIT_AT_HDFS_BLOCK_LEVEL_,
    SPLIT_AT_ORC_STRIPE_LEVEL_,
    SPLIT_AT_PARQUET_ROWGROUP_LEVEL_
    // could later add splittable compression blocks for delimited files
  };

  static const CollIndex InvalidHostId = NULL_COLL_INDEX;

  HHDFSFileStats(NAMemory *heap,
                 HHDFSTableStats *table) :
       HHDFSStatsBase(table),
       heap_(heap),
       fileName_(heap),
       replication_(1),
       blockSize_(-1),
       blockHosts_(NULL) {}
  ~HHDFSFileStats();
  virtual void populate(hdfsFS fs,
                hdfsFileInfo *fileInfo,
                Int32& samples,
                HHDFSDiags &diags,
                NABoolean doEstimation = TRUE,
                char recordTerminator = '\n'
                );

  // populate a string.  The first field must be "FileStats".  
  //
  // Please refer to mehtod getFileAndBlockAttributes() in
  // TrafParquetFileReader.java.
  virtual void populate(char* str, const char* delim, HHDFSDiags &diags);

  const NAString & getFileName() const                   { return fileName_; }
  Int32 getReplication() const                        { return replication_; }
  Int64 getBlockSize() const                            { return blockSize_; }
  HostId getHostId(Int32 replicate, Int64 blockNum) const;
  const ComCompressionInfo &getCompressionInfo() const
                                                  { return compressionInfo_; }
  virtual void display();
  virtual void print(FILE *ofd);

                                // methods to split the file into work units
  virtual PrimarySplitUnit getSplitUnitType(Int32 balanceLevel) const;
  virtual Int32 getNumOFSplitUnits(PrimarySplitUnit su) const;
  virtual void getSplitUnit(Int32 ix, Int64 &offset, Int64 &length, PrimarySplitUnit su) const;
  virtual Int32 getHDFSBlockNumForSplitUnit(Int32 ix, PrimarySplitUnit su) const;
  virtual NABoolean splitsOfPrimaryUnitsAllowed(PrimarySplitUnit su) const;

  // Assign all blocks in this to ESPs, considering locality
  // return: total # of bytes assigned for the hive file
  virtual Int64 assignToESPs(Int64 *espDistribution,
                             NodeMap* nodeMap,
                             Int32 numSQNodes,
                             Int32 numESPs,
                             HHDFSPartitionStats *partition,
                             Int32 &nextDefaultPartNum,
                             NABoolean useLocality,
                             Int32 balanceLevel);

  // Assign all blocks in this to the entry (ESP). The ESP will access
  // all the blocks.
  virtual void assignFileToESP(HiveNodeMapEntry*& entry,
                               const HHDFSPartitionStats* p);
  
  virtual OsimHHDFSStatsBase* osimSnapShot(NAMemory * heap);

protected:
  NABoolean sampleFileUsingJNI(const char *fileName,
                                  Int32& samples,
                                  HHDFSDiags &diags,
                                  char recordTerminator);
  void fakeHostIdsOnVirtualSQNodes();

  NABoolean checkExtractedString(char* extractedStr, const char* msg, char* sourceStr, HHDFSDiags& diags);

protected:
  NAString fileName_;
  Int32 replication_;
  Int64 blockSize_;

  // list of blocks for this file
  HostId *blockHosts_;
  NAMemory *heap_;

  ComCompressionInfo compressionInfo_;
};

class HHDFSSplittableFileStats : public HHDFSFileStats
{
  friend class OsimHHDFSSplittableFileStats;
public:
  HHDFSSplittableFileStats(NAMemory *heap,
                    HHDFSTableStats *table) : 
       HHDFSFileStats(heap, table),
       numOfRows_(heap), 
       offsets_(heap), 
       totalBytes_(heap)
  {}

  ~HHDFSSplittableFileStats() {};

  Int64 setStripeOrRowgroupInfo(hdfsFileInfo *fileInfo, int numStripesOrRowgroups, 
               jlong *pOffsets, jlong *pLengths, jlong *pNumRows, int numBlocks, jobjectArray j_blockHosts, NABoolean sortHosts);
  // methods to split the file into work units
  virtual Int32 getNumOFSplitUnits(PrimarySplitUnit su) const;
  virtual void getSplitUnit(Int32 ix, Int64 &offset, Int64 &length, PrimarySplitUnit su) const;
  virtual Int32 getHDFSBlockNumForSplitUnit(Int32 ix, PrimarySplitUnit su) const;
  virtual NABoolean splitsOfPrimaryUnitsAllowed(PrimarySplitUnit su) const;

  void populate(char* str, const char* delim, HHDFSDiags &diags);

  void print(FILE *ofd, const char* msg, const char* unit_name="Unit");
  void display();

protected: 
 
  // Info about all splittable units
  LIST(Int64) numOfRows_;
  LIST(Int64) offsets_;
  LIST(Int64) totalBytes_;

};

class HHDFSORCFileStats : public HHDFSSplittableFileStats 
{
  friend class OsimHHDFSORCFileStats;
public:
  HHDFSORCFileStats(NAMemory *heap,
                    HHDFSTableStats *table) : 
       HHDFSSplittableFileStats(heap, table)
  {}

  ~HHDFSORCFileStats() {};

  void setOrcStripeInfo(hdfsFileInfo *fileInfo, int numStripesOrRowgroups, jlong *pOffsets, jlong *pLengths, jlong *pNumRows, int numBlocks, 
         jobjectArray j_blockHosts, NABoolean sortHosts);
  void populate(hdfsFS fs,
                hdfsFileInfo *fileInfo,
                Int32& samples,
                HHDFSDiags &diags,
                NABoolean doEstimation = TRUE,
                char recordTerminator = '\n'
                ); 
  //void populateWithCplus(HHDFSDiags &diags, hdfsFS fs, hdfsFileInfo *fileInfo, NABoolean readStripeInfo, NABoolean readNumRows, NABoolean needToOpenORCI);
  void populateWithJNI(HHDFSDiags &diags, NABoolean readStripeInfo, NABoolean readNumRows, NABoolean needToOpenORCI);

  virtual OsimHHDFSStatsBase* osimSnapShot(NAMemory * heap);

  static void resetTotalAccumulatedRows() 
   { 
      totalAccumulatedRows_ = 0;
      totalAccumulatedTotalSize_ = 0;
      totalAccumulatedStripes_ = 0;
      totalAccumulatedBlocks_ = 0;
   }

  virtual PrimarySplitUnit getSplitUnitType(Int32 balanceLevel) const;
  void print(FILE* ofd);

private: 
 
  static THREAD_P Int64 totalAccumulatedRows_;
  static THREAD_P Int64 totalAccumulatedTotalSize_;
  static THREAD_P Int64 totalAccumulatedStripes_;
  static THREAD_P Int64 totalAccumulatedBlocks_;
};

class HHDFSParquetFileStats : public HHDFSSplittableFileStats
{
  friend class OsimHHDFSParquetFileStats;
public:
  HHDFSParquetFileStats(NAMemory *heap,
                    HHDFSTableStats *table) : 
       HHDFSSplittableFileStats(heap, table)
  {}

  ~HHDFSParquetFileStats() {};

  void setParquetRowgroupInfo(hdfsFileInfo *fileInfo, int numRowgroups, jlong *pOffsets, jlong *pLengths, jlong *pNumRows, int numBlocks, 
         jobjectArray j_blockHosts, NABoolean sortHosts);

  void populate(hdfsFS fs,
                hdfsFileInfo *fileInfo,
                Int32& samples,
                HHDFSDiags &diags,
                NABoolean doEstimation = TRUE,
                char recordTerminator = '\n'
                ); 

  void populateWithJNI(char* path, HHDFSDiags &diags, NABoolean readRowGroupInfo, NABoolean readNumRows, NABoolean needToOpenParquetI);

  void populate(char* str, const char* delim, HHDFSDiags &diags);

  virtual OsimHHDFSStatsBase* osimSnapShot(NAMemory * heap);

  static void resetTotalAccumulatedRows() 
   { 
      totalAccumulatedRows_ = 0;
      totalAccumulatedTotalSize_ = 0;
      totalAccumulatedRowGroups_= 0;
      totalAccumulatedBlocks_= 0;
   }

  virtual PrimarySplitUnit getSplitUnitType(Int32 balanceLevel) const;
  void print(FILE* ofd);

private: 
 
  static THREAD_P Int64 totalAccumulatedRows_;
  static THREAD_P Int64 totalAccumulatedTotalSize_;
  static THREAD_P Int64 totalAccumulatedRowGroups_;
  static THREAD_P Int64 totalAccumulatedBlocks_;
};

class HHDFSBucketStats : public HHDFSStatsBase
{
  friend class OsimHHDFSBucketStats;
public:
  HHDFSBucketStats(NAMemory *heap,
                   HHDFSTableStats *table) :
       HHDFSStatsBase(table),
       heap_(heap), fileStatsList_(heap), scount_(0) { 
        pthread_mutex_init(&mutex_bs, NULL);
       }
  ~HHDFSBucketStats();
  // 
  const CollIndex entries() const         { return fileStatsList_.entries(); }
  const HHDFSFileStats * operator[](CollIndex i) const 
                                                 { return fileStatsList_[i]; }
  HHDFSFileStats * operator[](CollIndex i)       { return fileStatsList_[i]; }

  HHDFSFileStats *setStripeOrRowgroupInfo(HiveFileType fileType, hdfsFileInfo *fileInfo, int numStripesOrRowgroups, jlong *pOffsets, jlong *pLengths, jlong *pNumRows, 
         int numBlocks, jobjectArray j_blockHosts, NABoolean sortHosts);

  void addFile(hdfsFS fs, hdfsFileInfo *fileInfo, 
               HHDFSDiags &diags,
               NABoolean doEstimate = TRUE,
               char recordTerminator = '\n',
               CollIndex pos = NULL_COLL_INDEX,
               NABoolean isORC = FALSE,
               NABoolean isParquet = FALSE);
                    
  void addFile(char* str,
               HHDFSDiags &diags,
               NABoolean doEstimate = TRUE,
               char recordTerminator = '\n',
               CollIndex pos = NULL_COLL_INDEX,
               NABoolean isORC = FALSE,
               NABoolean isParquet = FALSE);
                    
  void removeAt(CollIndex i);

  void display();
  void print(FILE *ofd);

  void insertAt(Int32 pos, HHDFSFileStats* st){  fileStatsList_.insertAt(pos, st);  }
  
  virtual OsimHHDFSStatsBase* osimSnapShot(NAMemory * heap);

  pthread_mutex_t mutex_bs;

private:

  // list of files in this bucket
  LIST(HHDFSFileStats *) fileStatsList_;
  NAMemory *heap_;

  Int32 scount_; // total # of sampled files associated with this bucket
};

class HHDFSPartitionStats : public HHDFSStatsBase
{
    friend class OsimHHDFSPartitionStats;
public:
  HHDFSPartitionStats(NAMemory *heap,
                          HHDFSTableStats *table) :
       HHDFSStatsBase(table),
       heap_(heap), partitionDir_(heap),
    bucketStatsList_(heap),
    partIndex_(-1),
    defaultBucketIdx_(-1),
    recordTerminator_(0)
    {
      pthread_mutex_init(&mutex_pbs, NULL);
    }
  ~HHDFSPartitionStats();

  void incrFileInfo(int numBlocks, int numStripesOrRowgroups, long totalSize, long totalRows, long modificationTs);
  void init(int partIndex, const char *partDir, int numOfBuckets, const char *partKeyValues);
  const NAString &getDirName() const                 { return partitionDir_; }
  int getPartIndex() const                              { return partIndex_; }
  const NAString &getPartitionKeyValues() const {return partitionKeyValues_; }
  const CollIndex entries() const       { return bucketStatsList_.entries(); }
  const HHDFSBucketStats * operator[](CollIndex i) const 
           { return (bucketStatsList_.used(i) ? bucketStatsList_[i] : NULL); }

  Int32 getNumOfBuckets() const { return (defaultBucketIdx_ ? defaultBucketIdx_ : 1); }
  Int32 getLastValidBucketIndx() const               { return defaultBucketIdx_; }

  //const hdfsFileInfo * dirInfo() const {return &dirInfo_; }

  void populate(hdfsFS fs,
                const NAString &dir,
                int partIndex,
                const char *partitionKeyValues,
                Int32 numOfBuckets, 
                HHDFSDiags &diags,
                NABoolean canDoEstimation, char recordTerminator, 
                NABoolean isORC,
                NABoolean isParquet,
		NABoolean allowSubdirs,
                Int32& filesEstimated);

  void populate(const FileStatsCache& statsCach,
                const NAString &dir,
                int partIndex,
                const char *partitionKeyValues,
                Int32 numOfBuckets, 
                HHDFSDiags &diags,
                NABoolean canDoEstimation, char recordTerminator, 
                NABoolean isORC,
                NABoolean isParquet,
                Int32& filesEstimated);

   void populateOrc(const NAString &dir,
                                   int partIndex,
                                   const char *partitionKeyValues,
                                   Int32 numOfBuckets,
                                   HHDFSDiags &diags,
                                   NABoolean canDoEstimation,
                                   Int32& filesEstimated);

  HHDFSFileStats *setStripeOrRowgroupInfo(HiveFileType fileType, hdfsFileInfo *fileInfo, 
				   jint numStripesOrRowgroups, 
				   jlong *pOffsets, 
				   jlong *pLengths, 
				   jlong *pNumRows, 
				   int numBlocks, 
				   jobjectArray j_blockHosts, 
				   NABoolean sortHosts);

  void processInnerDirectory(hdfsFS fs,
			     const char* dir,
			     Int32 numOfBuckets,
			     HHDFSDiags &diags,
			     NABoolean canDoEstimation,
			     char recordTerminator, 
			     NABoolean isORC,
			     NABoolean isParquet,
			     Int32& filesEstimated) ;

  NABoolean validateAndRefresh(hdfsFS fs, HHDFSDiags &diags, NABoolean refresh, 
                               NABoolean isORC, NABoolean isParquet);
  Int32 determineBucketNum(const char *fileName);

  void display();
  void print(FILE *ofd);

  void insertAt(Int32 pos, HHDFSBucketStats* st){  bucketStatsList_.insertAt(pos, st);  }
  
  virtual OsimHHDFSStatsBase* osimSnapShot(NAMemory * heap);

  HHDFSBucketStats* determineBucket(const char *fileName);

  pthread_mutex_t mutex_pbs;

protected:

private:

  // directory of the partition
  NAString partitionDir_;
  NAString partitionKeyValues_;
  int partIndex_; // index in HDFSTableStats list

  // number of buckets (from table DDL) or 0 if partition is not bucketed
  // Note this value can never be 1. This value indicates the last
  // valid index in bucketStatsList_ below.
  Int32 defaultBucketIdx_;

  // array of buckets in this partition (index is bucket #)
  ARRAY(HHDFSBucketStats *) bucketStatsList_;

  char recordTerminator_;
  
  //hdfsFileInfo dirInfo_;

  NAMemory *heap_;
};

// HDFS file-level statistics for a Hive table. This includes
// partitioned and bucketed tables, binary and compressed tables
class HHDFSTableStats : public HHDFSStatsBase
{
  friend class HivePartitionAndBucketKey; // to be able to make a subarray of the partitions
  friend class OsimHHDFSTableStats;
  friend class OptimizerSimulator;
public:
  HHDFSTableStats(NAMemory *heap);
  ~HHDFSTableStats();

  const CollIndex entries() const          { return partitionStatsList_.entries(); }
  const HHDFSPartitionStats * operator[](CollIndex i) const 
                                                  { return partitionStatsList_[i]; }


  // Populate the HDFS statistics for this table and set diagnostics info
  // return success status: TRUE = success, FALSE = failure.
  // Use getDiags() method to get error details.
  NABoolean populate(struct hive_tbl_desc *htd);
  void getStripeOrRowgroupInfo(HiveFileType fileType, Int64 &modificationTsInMillis, Int32& filesEstimated);
  void setStripeOrRowgroupInfo(jint partIndex, const char *partDir, 
               const char *partitionKeyValues, hdfsFileInfo *fileInfo, int numStripesOrRowgroups, 
               jlong *pOffsets, jlong *pLengths, jlong *pNumRows, int numBlocks, jobjectArray j_blockHosts);

  NABoolean populateTblParams(struct hive_tblparams_desc *htp);

  // Check for HDFS file changes, return TRUE if stats are still valid
  // or could be refreshed. Diagnostics could be set if returning FALSE,
  // checking for diagnostics is optional.
  NABoolean validateAndRefresh(Int64 expirationTimestamp=-1, NABoolean refresh=TRUE);

  // Split a location into its parts.
  // If you want to set a ComDiagsArea for errors, use
  // TableDesc::splitHiveLocation
  static NABoolean splitLocation(const char *tableLocation,
                                 NAString &hdfsHost,
                                 Int32 &hdfsPort,
                                 NAString &tableDir,
                                 HHDFSDiags &diags,
                                 int hdfsPortOverride);

  // a simplified version of splitLocation() that only
  // recognizes host name and port. 
  static NABoolean splitLocation(const char *tableLocation,
                                 NAString &hdfsHost,
                                 Int32 &hdfsPort,
                                 HHDFSDiags &diags);

  void processDirectory(NABoolean batchRead,
                        const NAString &dir,
                        const char *partColValues,
                        Int32 numOfBuckets, 
                        NABoolean doEstimation,
                        char recordTerminator,
                        NABoolean isORC, 
                        NABoolean isParquet, 
                        Int32& filesEstimated);


  void setPortOverride(Int32 portOverride)         { hdfsPortOverride_ = portOverride; }

  Int32 getPortOverride() const {return hdfsPortOverride_;}

  char getRecordTerminator() const {return recordTerminator_;}
  char getFieldTerminator() const {return fieldTerminator_;}
  char *getNullFormat() const { return nullFormat_; }
  char *getSerializeLib() const { return serializeLib_; }

  Int32 getNumPartitions() const {return totalNumPartitions_;}
  Int32 getSkipHeaderCount() const ;

  Int64 getValidationTimestamp() const                 { return validationJTimestamp_; }

  // return # of buckets if all partns are consistently bucketed, 0 otherwise
  // caller has to check for same bucket cols
  Int32 getNumOfConsistentBuckets() const;

  // return average string length of character columns per row
  Lng32 getAvgStringLengthPerRow();
  
  // for the NATable cache
  void setupForStatement();
  void resetAfterStatement();

  void display();
  void print(FILE *ofd);
  // heap size used by the hive stats
  Int32 getHiveStatsUsedHeap()
   {
     return (hiveStatsSize_);
   }

  const HHDFSDiags &getDiags() const { return diags_; }
  const NABoolean hasError() const { return !diags_.isSuccess(); }

  const NABoolean isTextFile() const { return (type_ == TEXT);}
  const NABoolean isSequenceFile() const { return (type_ == SEQUENCE);}  
  const NABoolean isOrcFile() const { return (type_ == ORC);}
  const NABoolean isParquetFile() const { return (type_ == PARQUET);}
  const NABoolean isAvroFile() const { return (type_ == AVRO);}

  const NAString &tableDir() const { return tableDir_; }

  void insertAt(Int32 pos, HHDFSPartitionStats * st) {  partitionStatsList_.insertAt(pos, st);  }
  virtual OsimHHDFSStatsBase* osimSnapShot(NAMemory * heap);
  void captureHiveTableStats(const NAString &tableName, Int64 lastModificationTs, hive_tbl_desc* hvt_desc );
  static HHDFSTableStats * restoreHiveTableStats(const NAString & tableName,  Int64 lastModificationTs,  hive_tbl_desc* hvt_desc, NAMemory* heap);
  void initLOBInterface(const NAString &host, Int32 port);
  void releaseLOBInterface();

  const NAString &getCurrHdfsHost() const { return currHdfsHost_; }
  Int32 getCurrHdfsPort() const { return currHdfsPort_; }
  ExLobGlobals *getLOBGlobals() const { return lobGlob_; }
  
  const Lng32 numOfPartCols() const { return numOfPartCols_; }

  hive_tblparams_desc *hiveTblParams() const {return hiveTblParams_;}

  const char* getBinaryPartColValues(CollIndex i) const ;
  void setBinaryPartColValues(CollIndex i, char* v, Lng32 len);

  // finer-resolution timestamp for entire table
  // (can remove this once we use JNI to collect this info
  // for all HDFS files)
  Int64 getModificationTSmsec() const { return modificationTSInMillisec_; }
  void computeModificationTSmsec();
  NABoolean isS3Hosted() const {return (currHdfsHost_.index("s3a:", 0, NAString::ignoreCase) == 0) ||
		                               (currHdfsHost_.index("s3:", 0, NAString::ignoreCase) == 0);}

public:
  const char *statsLogFile_;
private:
  NABoolean connectHDFS(const NAString &host, Int32 port);
  void disconnectHDFS();

  NAString currHdfsHost_;
  Int32    currHdfsPort_;
  hdfsFS   fs_;

  // host/port used to connect to HDFS
  Int32 hdfsPortOverride_; // if > -1, use this port # instead of what's in the Hive metadata
  // HDFS directory name of the table
  NAString tableDir_;
  // indicates how many levels of partitioning directories the table has
  int numOfPartCols_;
  // total number of actual partitions, at all levels,
  // or 1 if the table is not partitioned
  int totalNumPartitions_;

  char recordTerminator_ ;
  char fieldTerminator_ ;

  char *nullFormat_;
  char *serializeLib_; // serde lib specified in Hive DDL. For example 
  //org.apache.hadoop.hive.serde2.OpenCSVSerde. To be used soon as of 3/14/2019

  Int64 validationJTimestamp_;
  // heap size used by the hive stats
  Int32 hiveStatsSize_;

  // List of partitions of this file. The reason why this is
  // an array and not a list is that we want to use an NASubarray
  // for it in class HivePartitionAndBucketKey (file SearchKey.h).
  ARRAY(HHDFSPartitionStats *) partitionStatsList_;

  // array of binary partition column values in exploded
  // format, for caching computed binary part col values
  // in HivePartitionAndBucketKey::computeActivePartitions()
  ARRAY(const char *) binaryPartColValues_;

  // These diags get reset and populated by the two top-level calls
  // for HDFS statistics: populate() and validateAndRefresh().
  // Check the diags after these two calls.

  // For validateAndRefresh() the best way to deal with any errors is
  // to treat them as an invalid cache entry (FALSE return value) and
  // retry reading all stats - it may succeed with fresh information.
  HHDFSDiags diags_;

  NAMemory *heap_;

  HiveFileType type_;

  // LOB interface for reading HDFS files
  ExLobGlobals *lobGlob_;

  hive_tblparams_desc *hiveTblParams_;

  FileStatsCache fileStatsCache_;
  
  Int64 modificationTSInMillisec_;
   
  int numOfBuckets_;
  
};

NABoolean determineObject(const char* source, NAString& path, NAString& name, NAString& type, long& modTime, HHDFSDiags& diags);

#endif
