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

#include "HDFSHook.h"
#include "hiveHook.h"
#include "CmpCommon.h"
#include "SchemaDB.h"
#include "ComCextdecs.h"
#include "ExpORCinterface.h"
#include "ExpParquetInterface.h"
#include "NodeMap.h"
#include "ExpLOBinterface.h"
#include "OptimizerSimulator.h"
#include "CompException.h"
#include "ex_ex.h"
#include "HBaseClient_JNI.h"
#include "HdfsClient_JNI.h"
#include "Globals.h"
#include "Context.h"
#include "QRLogger.h"
#include "NAString.h"

void HHDFSDiags::recordError(const char *errMsg,
                             const char *errLoc)
{
  // don't overwrite the original error
  if (success_)
    {
      success_ = FALSE;
      errMsg_ = errMsg;
      if (errLoc)
        errLoc_ = errLoc;
    }
}

void HHDFSDiags::moveErrorToSkipFiles(const char* fileName)
 {
   if(success_ || !fileName)
     return;
   if (skippedFiles_.length() != 0)
     skippedFiles_ += ", " ;
   skippedFiles_ += fileName;
   errMsg_ = "";
   errLoc_ = "";
   success_ = TRUE;
   skipCount_++ ;
 }

void HHDFSStatsBase::add(const HHDFSStatsBase *o)
{
  numBlocks_ += o->numBlocks_;
  numFiles_ += o->numFiles_; 
  totalRows_ += o->totalRows_;
  numSplitUnits_ += o->numSplitUnits_;
  totalStringLengths_ += o->totalStringLengths_; 
  totalSize_ += o->totalSize_;
  if (o->modificationTS_ > modificationTS_)
    modificationTS_ = o->modificationTS_ ;
  sampledBytes_ += o->sampledBytes_;
  sampledRows_ += o->sampledRows_;
  numLocalBlocks_ += o->numLocalBlocks_;
}

void HHDFSStatsBase::subtract(const HHDFSStatsBase *o)
{
  numBlocks_ -= o->numBlocks_;
  numFiles_ -= o->numFiles_; 
  totalRows_-= o->totalRows_;
  numSplitUnits_ -= o->numSplitUnits_;
  totalStringLengths_ -= o->totalStringLengths_;
  totalSize_ -= o->totalSize_;
  sampledBytes_ -= o->sampledBytes_;
  sampledRows_ -= o->sampledRows_;
  numLocalBlocks_ -= o->numLocalBlocks_;
}

Int64 HHDFSStatsBase::getEstimatedRowCount() const
{
   return  ( getTotalSize() / getEstimatedRecordLength() );
}

Int64 HHDFSStatsBase::getEstimatedRecordLength() const
{
  return MAXOF(sampledBytes_ / (sampledRows_ ? sampledRows_ : 1), 1);
}

Int64 HHDFSStatsBase::getEstimatedBlockSize() const
{
  return MAXOF(totalSize_ / (numBlocks_ ? numBlocks_ : 1), 32768);
}

void HHDFSStatsBase::display() { print(stdout, ""); }

void HHDFSStatsBase::print(FILE *ofd, const char *msg)
{
  fprintf(ofd,"File stats at %s level:\n", msg);
  fprintf(ofd," ++ numBlocks:    %ld\n",  numBlocks_);
  fprintf(ofd," ++ numFiles:     %ld\n",  numFiles_);
  fprintf(ofd," ++ totalRows:    %ld\n",  totalRows_);
  fprintf(ofd," ++ totalSize:    %ld\n",  totalSize_);
  fprintf(ofd," ++ sampledBytes: %ld\n",  sampledBytes_);
  fprintf(ofd," ++ sampledRows:  %ld\n",  sampledRows_);
  fprintf(ofd," ++ numSplitUnits:%ld\n",  numSplitUnits_);
  fprintf(ofd," ++ numLocalBlks: %ld\n",  numLocalBlocks_);
  fprintf(ofd," ++ totalStringLengths: %ld\n",  totalStringLengths_);
  fprintf(ofd," ++ modificationTS_: %ld\n",  modificationTS_);
}

HHDFSFileStats::~HHDFSFileStats()
{
  if (blockHosts_)
    NADELETEBASIC(blockHosts_, heap_);
}

static void sortHostArray(HostId *blockHosts,
                          Int32 numBlocks,
                          Int32 replication, 
                          const NAString &randomizer)
{
  // the hdfsGetHosts() call randomizes the hosts for 1st, 2nd and 3rd replica etc.
  // for each call, probably to get more even access patterns. This makes it hard
  // to debug the placement algorithm, since almost no 2 query plans are alike.
  // Replace the random method of hdfsGetHosts with a pseudo-random one,
  // based on the file name. With no randomization we would put a bigger load
  // on hosts with a lower id.

  // we have replication * numBlocks entries in blockHosts, with entry
  // (r * numBlocks + b) being the rth replica of block #b.

  if (replication > 1 && replication <= 10)
    {
      UInt32 rshift = (UInt32) randomizer.hash();

      for (Int32 b=0; b<numBlocks; b++)
        {
          // a sorted array of HostIds for a given block
          HostId s[10];

          // insert the first v
          s[0]=blockHosts[b];
          for (Int32 r=1; r<replication; r++)
            {
              HostId newVal = blockHosts[r*numBlocks + b];

              // replication is a small number, bubblesort of s will do...
              for (Int32 x=0; x<r; x++)
                if (newVal < s[x])
                  {
                    // shift the larger values up by 1
                    for (Int32 y=r; y>x; y--)
                      s[y] = s[y-1];
                    // then insert the new value
                    s[x] = newVal;
                    break;
                  }
                else if (x == r-1)
                  // new value is the largest, insert at end
                  s[r] = newVal;
            } // for each replica host of a block

          // now move sorted values in s back to blockHosts,
          // but shift them by rshift mod replication
          for (Int32 m=0; m<replication; m++)
            blockHosts[m*numBlocks + b] = s[((UInt32) m + rshift + (UInt32) b) % replication];

        } // for each block b
    } // replication between 2 and 10
} // sortHostArray

//Hidden file will be invalid
static NABoolean HDFSFileValidate( hdfsFileInfo *fileInfo)
{
  if (!fileInfo)
    return FALSE;

  if (fileInfo->mSize <= 0)
    return FALSE;
  
  // figure out name from file prefix 
  const char *mark = fileInfo->mName + strlen(fileInfo->mName) - 1;
  
  // search backwards for the last slash in the name or the start
  while (*mark != '/' && mark != fileInfo->mName)
    mark--;
  
  if (*mark == '/')
    mark++;
  
  if (*mark == '.' || *mark == '_')
    return FALSE;

  return TRUE;
}

static NABoolean fileNumExceedsLimit(NAString& rootDir, HHDFSDiags& diags, 
				     NAMemory* heap)
{
   Lng32 numFiles = 0;
   Lng32 rc = HdfsClient::getNumFiles((char*)rootDir.data(), FALSE, numFiles);
   if ( rc ) {
     char msg[1024];
     snprintf(msg, sizeof(msg),
	      "getNumFiles() on directory %s returned an error. Possibly trafodion id cannot access this directory", rootDir.data());
     diags.recordError(NAString(msg));
     return TRUE;
   }

   if (numFiles < 
       ActiveSchemaDB()->getDefaults().getAsLong(PARQUET_BATCH_READ_FILESTATS))
     return FALSE; // typical case. #files is small enough to allow batch read
     
   return TRUE; // more files than batch mode can handle
}

HostId HHDFSFileStats::getHostId(Int32 replicate, Int64 blockNum) const
{
  DCMPASSERT(replicate >= 0 && replicate < replication_ &&
             blockNum >= -1 && blockNum < numBlocks_);

  if (blockHosts_ && blockNum >= 0)
    return blockHosts_[replicate*numBlocks_+blockNum];
  else
    return InvalidHostId;
}

void HHDFSFileStats::populate(hdfsFS fs, hdfsFileInfo *fileInfo, 
                              Int32& samples,
                              HHDFSDiags &diags,
                              NABoolean doEstimation,
                              char recordTerminator)
{
  // copy fields from fileInfo
  fileName_       = fileInfo->mName;
  replication_    = (Int32) fileInfo->mReplication;
  totalSize_      = (Int64) fileInfo->mSize;
  blockSize_      = (Int64) fileInfo->mBlockSize;
  modificationTS_ = fileInfo->mLastMod;
  numFiles_       = 1;

  NABoolean sortHosts = (CmpCommon::getDefault(HIVE_SORT_HDFS_HOSTS) == DF_ON);

  compressionInfo_.setCompressionMethod(fileName_);
  // file should be ignored 
  if (!HDFSFileValidate(fileInfo))
    return ;

  if (doEstimation) {

    // 
    // Open the hdfs file to estimate record length. Read one block at
    // a time searching for <s> instances of record separators. Stop reading 
    // when either <s> instances have been found or a partial number of
    // instances have and we have exhausted all data content in the block.
    // We will keep reading if the current block does not contain 
    // any instance of the record separator.

    if (!sampleFileUsingJNI(fileName_, samples, diags, recordTerminator))
      {
        // without the LOB interface for compressed tables, fake the numbers
        // (200 bytes/row)
        sampledBytes_ = 2000;
        sampledRows_ = 10;
      }
  }
  if (blockSize_)
    {
      numBlocks_ = totalSize_ / blockSize_;
      if (totalSize_ % blockSize_ > 0)
        numBlocks_++; // partial block at the end
    }
  else
    {
      diags.recordError(NAString("Could not determine block size of HDFS file ") + fileInfo->mName,
                        "HHDFSFileStats::populate");
    }

  if (CURRCONTEXT_CLUSTERINFO->hasVirtualNodes())
    {
      if (CmpCommon::getDefault(HIVE_SIMULATE_REAL_NODEMAP) == DF_ON)
        fakeHostIdsOnVirtualSQNodes();
    }
  else if ( NodeMap::useLocalityForHiveScanInfo() && totalSize_ > 0 && !getTable()->isS3Hosted() && diags.isSuccess())
    {

      blockHosts_ = new(heap_) HostId[replication_*numBlocks_];

      // walk through blocks and record their locations
      NAString hostName;
      tOffset o = 0;
      Int64 blockNum;

      for (blockNum=0; blockNum < numBlocks_ && diags.isSuccess(); blockNum++)
        {
          char*** blockHostNames = hdfsGetHosts(fs,
                                                fileInfo->mName, 
                                                o,
                                                fileInfo->mBlockSize);

          o += blockSize_;

          if (blockHostNames == NULL)
            {
              diags.recordError(NAString("Could not determine host of blocks for HDFS file ") + fileInfo->mName,
                                "HHDFSFileStats::populate");
            }
          else
            {
              char **h = *blockHostNames;
              HostId hostId;
              int blockIsOnThisCluster = 0;

              for (Int32 r=0; r<replication_; r++)
                {
                  if (h[r])
                    {
                      hostName = h[r];

                      // NAClusterInfo stores only the unqualified
                      // first part of the host name, so do the same
                      // with the host name we got from HDFS
                      size_t pos = hostName.index('.');

                      if (pos != NA_NPOS)
                        hostName.remove(pos);

                      Lng32 nodeNum = CURRCONTEXT_CLUSTERINFO->mapNodeNameToNodeNum(hostName);
                      if (nodeNum >= 0)
                        {
                          hostId = nodeNum;
                          blockIsOnThisCluster = 1;
                        }
                      else
                        hostId = InvalidHostId;
                    }
                  else
                    hostId = InvalidHostId;
                  blockHosts_[r*numBlocks_+blockNum] = hostId;
                }
              if (sortHosts)
                sortHostArray(blockHosts_,
                              (Int32) numBlocks_,
                              replication_,
                              getFileName());
              numLocalBlocks_ += blockIsOnThisCluster;
            }
          hdfsFreeHosts(blockHostNames);
        }
    }
}

NABoolean HHDFSFileStats::sampleFileUsingJNI(const char *fileName,
                                           Int32& samples,
                                           HHDFSDiags &diags,
                                           char recordTerminator)
{
  // Read file directly, using libhdfs interface.
  // Note that this doesn't handle compressed files.
  // This code should probably be removed once the LOB
  // interface code is stable.

  Int64 sampleBufferSize = MINOF(blockSize_, 65536);
  sampleBufferSize = MINOF(sampleBufferSize,totalSize_/10);

  if (sampleBufferSize <= 100)
    return FALSE;
  NABoolean compressedFile = 
      (ComCompressionInfo::getCompressionMethodFromFileName(fileName) != ComCompressionInfo::UNCOMPRESSED);
  HDFS_Client_RetCode hdfsClientRetcode;
  HdfsClient *hdfsClient  = HdfsClient::newInstance((NAHeap *)heap_, NULL, hdfsClientRetcode);
  if (hdfsClientRetcode == HDFS_CLIENT_OK)
  {
     hdfsClient->hdfsOpen(fileName, compressedFile);
     if (hdfsClientRetcode == HDFS_CLIENT_OK)
     {
        tOffset offset = 0;
        tSize bufLen = sampleBufferSize;
        char* buffer = new (heap_) char[bufLen+1];

        buffer[bufLen] = 0; // extra null at the end to protect strchr()
               // to run over the buffer.

        NABoolean sampleDone = FALSE;

        Int32 totalSamples = 10;
        Int32 totalLen = 0;
        Int32 recordPrefixLen = 0;
   
        while (!sampleDone) {
   
           tSize szRead = hdfsClient->hdfsRead(offset, buffer, bufLen, hdfsClientRetcode);
           if ( szRead <= 0 )
              break;

           CMPASSERT(szRead <= bufLen);

           char* pos = NULL;
           char* start = buffer;
           for (Int32 i=0; i<totalSamples; i++ ) {
             if ( (pos=strchr(start, recordTerminator)) ) {
                totalLen += pos - start + 1 + recordPrefixLen;
                samples++;
                start = pos+1;
                if ( start > buffer + szRead ) {
                   sampleDone = TRUE;
                   break;
                }
                recordPrefixLen = 0;
             } else {
                 recordPrefixLen += szRead - (start - buffer + 1);
                 break;
             }
          }
          if ( samples > 0 )
             break;
          else
             offset += szRead;
        }
        NADELETEBASIC(buffer, heap_);
        if ( samples > 0 ) {
           sampledBytes_ += totalLen;
           sampledRows_  += samples;
        }
     
        hdfsClient->hdfsClose ();
        HdfsClient::deleteInstance(hdfsClient);
     } else {
        diags.recordError(NAString("Unable to open HDFS file ") + fileName,
                      "HHDFSFileStats::sampleFileUsingJNI");
        HdfsClient::deleteInstance(hdfsClient);
        return FALSE;
     }
  }
  else {
     diags.recordError(NAString("Unable to create HdfsClient Instance ") + fileName,
                      "HHDFSFileStats::sampleFileUsingJNI");
     return FALSE;
  }
  return TRUE;
}

void HHDFSFileStats::fakeHostIdsOnVirtualSQNodes()
{
  // to be able to test on a single node development system
  // with two virtual nodes, make a fake list of block hosts
  // and pretend that we have a replication factor of 2
  replication_ = 2;
  blockHosts_ = new(heap_) HostId[replication_*numBlocks_];

  for (Int64 blockNum=0; blockNum < numBlocks_; blockNum++)
    for (int r=0; r<replication_; r++)
      blockHosts_[r*numBlocks_+blockNum] = ((blockNum+r) % 2);

  numLocalBlocks_ = numBlocks_;
}

void HHDFSFileStats::display() { print(stdout); }

void HHDFSFileStats::print(FILE *ofd)
{
  fprintf(ofd,"-----------------------------------\n");
  fprintf(ofd,">>>> File:       %s\n", fileName_.data());
  fprintf(ofd,"  replication:   %d\n", replication_);
  fprintf(ofd,"  block size:    %ld\n", blockSize_);
  fprintf(ofd,"\n");
  fprintf(ofd,"            host for replica\n");
  fprintf(ofd,"  block #     1    2    3    4\n");
  fprintf(ofd,"  --------- ---- ---- ---- ----\n");
  for (Int32 b=0; b<numBlocks_; b++)
    fprintf(ofd,"  %9d %4d %4d %4d %4d\n",
            b,
            getHostId(0, b),
            (replication_ >= 2 ? getHostId(1, b) : -1),
            (replication_ >= 3 ? getHostId(2, b) : -1),
            (replication_ >= 4 ? getHostId(3, b) : -1));
  HHDFSStatsBase::print(ofd, "file");
}

HHDFSBucketStats::~HHDFSBucketStats()
{
  pthread_mutex_destroy(&mutex_bs);
  for (CollIndex i=0; i<fileStatsList_.entries(); i++)
    delete fileStatsList_[i];
}

HHDFSFileStats *HHDFSBucketStats::setStripeOrRowgroupInfo(HiveFileType fileType, hdfsFileInfo *fileInfo, int numStripesOrRowgroups, 
               jlong *pOffsets, jlong *pLengths, jlong *pNumRows, int numBlocks, jobjectArray j_blockHosts, NABoolean sortHosts)
{
   HHDFSSplittableFileStats *fileStats = NULL;
   if (fileType == ORC) {
      fileStats = new(heap_) HHDFSORCFileStats(heap_, getTable());
      ((HHDFSORCFileStats *)fileStats)->setOrcStripeInfo(fileInfo, numStripesOrRowgroups, pOffsets, pLengths, pNumRows, numBlocks, j_blockHosts, sortHosts);
   } else if (fileType == PARQUET) {
      fileStats = new(heap_) HHDFSParquetFileStats(heap_, getTable());
      ((HHDFSParquetFileStats *)fileStats)->setParquetRowgroupInfo(fileInfo, numStripesOrRowgroups, pOffsets, pLengths, pNumRows, numBlocks, j_blockHosts, sortHosts);
   }
   scount_++;
   fileStatsList_.insert(fileStats);
   add(fileStats);
   return fileStats;
}

void HHDFSBucketStats::addFile(hdfsFS fs, hdfsFileInfo *fileInfo, 
                               HHDFSDiags &diags,
                               NABoolean doEstimate, 
                               char recordTerminator,
                               CollIndex pos,
                               NABoolean isORC,
                               NABoolean isParquet)
{
  HHDFSFileStats *fileStats = NULL;

  // file should be ignored 
  if (!HDFSFileValidate(fileInfo))
    return ;

  if (isORC) 
    fileStats = new(heap_) HHDFSORCFileStats(heap_, getTable());
  else 
  if ( isParquet )
    fileStats = new(heap_) HHDFSParquetFileStats(heap_, getTable());
  else
    fileStats = new(heap_) HHDFSFileStats(heap_, getTable());

  // Set doEstimate to false if scount_ number of files have been 
  // read in detail (sampled).
  if ( scount_ > CmpCommon::getDefaultLong(HIVE_HDFS_STATS_MAX_SAMPLE_FILES))
    doEstimate = FALSE;

  Int32 sampledRecords = 0;

  fileStats->populate(fs, fileInfo, sampledRecords, diags,
                      doEstimate, recordTerminator);

  if (diags.isSuccess())
    {
      pthread_mutex_lock(&mutex_bs);
      if ( sampledRecords > 0 )
        scount_++;

      if (pos == NULL_COLL_INDEX)
        fileStatsList_.insert(fileStats);
      else
        fileStatsList_.insertAt(pos, fileStats);
      add(fileStats);
      pthread_mutex_unlock(&mutex_bs);
    }
  else if ((CmpCommon::getDefaultNumeric(HIVE_MAX_ERROR_FILES) > 0) && 
           (CmpCommon::getDefaultNumeric(HIVE_MAX_ERROR_FILES) > 
            diags.getSkipCount()))
    {
      // ignore error and give a warning later
      diags.moveErrorToSkipFiles(fileInfo->mName);
    }
         
         
}

void HHDFSBucketStats::addFile(char* str,
                               HHDFSDiags &diags,
                               NABoolean doEstimate, 
                               char recordTerminator,
                               CollIndex pos,
                               NABoolean isORC,
                               NABoolean isParquet)
{
  if ( !isParquet ) {
    diags.recordError("Can't handle the detected file type in HHDFSBucketStats::addFile()");
    return;
  }

  HHDFSParquetFileStats *fileStats = 
           new(heap_) HHDFSParquetFileStats(heap_, getTable());

  const char* delim = "|";

  // in Parquet batch read mode, files to be ignored are handled in the 
  // Java side
  fileStats->populate(str, delim, diags);

  if (diags.isSuccess())
    {
      if (pos == NULL_COLL_INDEX)
        fileStatsList_.insert(fileStats);
      else
        fileStatsList_.insertAt(pos, fileStats);
      add(fileStats);
    }
}

void HHDFSBucketStats::removeAt(CollIndex i)
{
  HHDFSFileStats *e = fileStatsList_[i];
  subtract(e);
  fileStatsList_.removeAt(i);
  delete e;
}

void HHDFSBucketStats::display() { print(stdout); }

void HHDFSBucketStats::print(FILE *ofd)
{
  for (CollIndex f=0; f<fileStatsList_.entries(); f++)
    fileStatsList_[f]->print(ofd);
  HHDFSStatsBase::print(ofd, "bucket");
}

OsimHHDFSStatsBase* HHDFSBucketStats::osimSnapShot(NAMemory * heap)
{
    OsimHHDFSBucketStats* stats = new(heap) OsimHHDFSBucketStats(NULL, this, heap);
    
    for(Int32 i = 0; i < fileStatsList_.getUsedLength(); i++){
            //"gaps" are not added, but record the position
            if(fileStatsList_.getUsage(i) != UNUSED_COLL_ENTRY)
                stats->addEntry(fileStatsList_[i]->osimSnapShot(heap), i);
    }
    return stats;
}


HHDFSPartitionStats::~HHDFSPartitionStats()
{
  pthread_mutex_destroy(&mutex_pbs);
  for (CollIndex b=0; b<=defaultBucketIdx_; b++)
    if (bucketStatsList_.used(b))
      delete bucketStatsList_[b];
}

void HHDFSPartitionStats::init(int partIndex, const char *partDir, int numOfBuckets, const char *partKeyValues)
{
   partIndex_ = partIndex;
   partitionDir_ = partDir; 
   defaultBucketIdx_ = (numOfBuckets >= 1) ? numOfBuckets : 0;
   if (partKeyValues != NULL)
      partitionKeyValues_ = partKeyValues;
}

HHDFSFileStats *HHDFSPartitionStats::setStripeOrRowgroupInfo(HiveFileType fileType, hdfsFileInfo *fileInfo, jint numStripesOrRowgroups, 
               jlong *pOffsets, jlong *pLengths, jlong *pNumRows, int numBlocks, jobjectArray j_blockHosts, NABoolean sortHosts)
{
   HHDFSFileStats *fileStats = NULL; 
   if (!HDFSFileValidate(fileInfo))
      return fileStats;
   if (fileInfo->mKind == kObjectKindFile) {
      // the default (unbucketed) bucket number is
      // defaultBucketIdx_
      Int32 bucketNum = determineBucketNum(fileInfo->mName);
      HHDFSBucketStats *bucketStats = NULL;
      if (! bucketStatsList_.used(bucketNum)) {
          bucketStats = new(heap_) HHDFSBucketStats(heap_, getTable());
           bucketStatsList_.insertAt(bucketNum, bucketStats);
      } else
          bucketStats = bucketStatsList_[bucketNum];
      fileStats = bucketStats->setStripeOrRowgroupInfo(fileType, fileInfo, numStripesOrRowgroups, pOffsets, pLengths, pNumRows, numBlocks, j_blockHosts, sortHosts);
      if (fileStats != NULL)
         add(fileStats);
   }
   return fileStats;
}

void HHDFSPartitionStats::populate(hdfsFS fs,
				   const NAString &dir,
				   int partIndex,
				   const char *partitionKeyValues,
				   Int32 numOfBuckets,
				   HHDFSDiags &diags,
				   NABoolean canDoEstimation,
				   char recordTerminator, 
				   NABoolean isORC,
				   NABoolean isParquet,
				   NABoolean allowSubdirs,
				   Int32& filesEstimated)
{
  int numFiles = 0;

  // remember parameters
  partitionDir_     = dir;
  partIndex_        = partIndex;
  defaultBucketIdx_ = (numOfBuckets >= 1) ? numOfBuckets : 0;
  recordTerminator_ = recordTerminator;
  if (partitionKeyValues)
    partitionKeyValues_ = partitionKeyValues;

  // to avoid a crash, due to lacking permissions, check the directory
  // itself first
  hdfsFileInfo *dirInfo = hdfsGetPathInfo(fs, dir.data());
  
  if (!dirInfo)
    {
      diags.recordError(NAString("Could not access HDFS directory ") + dir,
                        "HHDFSPartitionStats::populate");
    }
  else
    {
      modificationTS_ = dirInfo->mLastMod;

      // list all the files in this directory, they all belong
      // to this partition and either belong to a specific bucket
      // or to the default bucket
      hdfsFileInfo *fileInfos = hdfsListDirectory(fs,
                                                  dir.data(),
                                                  &numFiles);

      Int32 maxThreadNum = CmpCommon::getDefaultLong(ORC_METADATA_READ_MAX_THREADS);
      Int32 filesToEstimate = CmpCommon::getDefaultLong(HIVE_HDFS_STATS_MAX_SAMPLE_FILES);
      Int32 threadNum;
      if (numFiles <= maxThreadNum)
        threadNum = numFiles;
      else 
        threadNum = maxThreadNum;

      threadedOrcFileInfoDesc infoDesc[threadNum];
      pthread_t tid[threadNum];

      for (Int32 i = 0; i < threadNum; ++i) {
        infoDesc[i].pid = i;
        infoDesc[i].numFiles = numFiles;
        infoDesc[i].numThreads = threadNum;
        infoDesc[i].doEstimate = canDoEstimation;
        infoDesc[i].fs = fs;
        infoDesc[i].recordTerminator = recordTerminator;
        infoDesc[i].isORC = isORC;
        infoDesc[i].isParquet = isParquet;
        infoDesc[i].filesEstimated = 0;
        infoDesc[i].filesToEstimate = filesToEstimate;
        infoDesc[i].fileInfos = fileInfos;
        infoDesc[i].partStats = this;
        infoDesc[i].context = CmpCommon::context();
      }
      if(numFiles <= CmpCommon::getDefaultLong(ORC_METADATA_READ_MAX_FILENUM) || !isORC )
      {
        NABoolean doEstimate = canDoEstimation;
 
       // populate partition stats
        for (int f=0; f<numFiles && diags.isSuccess(); f++)
        {
	  if ((fileInfos[f].mKind == kObjectKindDirectory) && allowSubdirs)
          {
	    processInnerDirectory(fs,fileInfos[f].mName, numOfBuckets, diags,
				  canDoEstimation, recordTerminator, isORC,
				  isParquet,filesEstimated) ;
	  }
          else if (fileInfos[f].mKind == kObjectKindFile)
	  {
	    // file should be ignored 
	    if (!HDFSFileValidate(&fileInfos[f]))
	      continue;
	    // the default (unbucketed) bucket number is
	    // defaultBucketIdx_
	    Int32 bucketNum = determineBucketNum(fileInfos[f].mName);
	    HHDFSBucketStats *bucketStats = NULL;
	    
	    if (! bucketStatsList_.used(bucketNum))
            {
	      bucketStats = new(heap_) HHDFSBucketStats(heap_, getTable());
	      bucketStatsList_.insertAt(bucketNum, bucketStats);
	    }
	    else
	      bucketStats = bucketStatsList_[bucketNum];
	    
	    if ( filesEstimated > filesToEstimate )
	      doEstimate = FALSE;
	    else
	      filesEstimated++;
	    
	    bucketStats->addFile(fs, &fileInfos[f], diags, doEstimate, 
				 recordTerminator, NULL_COLL_INDEX, isORC, isParquet);
	    
	  }
	}
      }
      else // do it in parallel
      {
        for (Int32 i = 0; i < threadNum; ++i) {
          int rc = pthread_create(&tid[i], NULL, addFileToBucket, &infoDesc[i]);
          if (rc) {
            diags.recordError(NAString("Could not create multi thread for reading file in ") + dir,
                                       "HHDFSPartitionStats::populate");
            return;
          }
        }
        for (Int32 i = 0; i < threadNum; ++i) {
          pthread_join(tid[i], NULL);
        }
        for (Int32 i = 0; i < threadNum; ++i) {
          if (! infoDesc[i].diags.isSuccess()) {
            diags.recordError(NAString(infoDesc[i].diags.getErrMsg()),
	                               NAString(infoDesc[i].diags.getErrLoc()));
            break;
          }
          filesEstimated += infoDesc[i].filesEstimated;
        }
      }
      hdfsFreeFileInfo(fileInfos, numFiles);
      hdfsFreeFileInfo(dirInfo,1);

      // aggregate statistics over all buckets
      for (Int32 b=0; b<=defaultBucketIdx_; b++)
        if (bucketStatsList_.used(b))
          add(bucketStatsList_[b]);
    }
}

void HHDFSPartitionStats::processInnerDirectory(hdfsFS fs,
                                       const char* dir,
                                       Int32 numOfBuckets,
                                       HHDFSDiags &diags,
                                       NABoolean canDoEstimation,
                                       char recordTerminator, 
                                       NABoolean isORC,
                                       NABoolean isParquet,
                                       Int32& filesEstimated)
{
  int numFiles = 0;

  // to avoid a crash, due to lacking permissions, check the directory
  // itself first
  hdfsFileInfo *dirInfo = hdfsGetPathInfo(fs, dir); 
  if (!dirInfo)
  {
    diags.recordError(NAString("Could not access HDFS directory ") + dir,
		      "HHDFSPartitionStats::populate");
    return ;
  }
  if (dirInfo->mLastMod > modificationTS_)
    modificationTS_ = dirInfo->mLastMod;
  
  // list all the files in this directory, they all belong
  // to this partition and either belong to a specific bucket
  // or to the default bucket
  hdfsFileInfo *fileInfos = hdfsListDirectory(fs,
					      dir,
					      &numFiles);
     
  Int32 filesToEstimate = CmpCommon::getDefaultLong(HIVE_HDFS_STATS_MAX_SAMPLE_FILES);
  
  NABoolean doEstimate = canDoEstimation;
  
  // populate partition stats
  for (int f=0; f<numFiles && diags.isSuccess(); f++)
  {

    if (fileInfos[f].mKind == kObjectKindDirectory)
    {
      processInnerDirectory(fs,fileInfos[f].mName, numOfBuckets,diags,
                            canDoEstimation, recordTerminator, isORC,
			    isParquet,filesEstimated) ;
    }
    else if (fileInfos[f].mKind == kObjectKindFile)
    {
      // file should be ignored 
      if (!HDFSFileValidate(&fileInfos[f]))
	continue;

      // the default (unbucketed) bucket number is
      // defaultBucketIdx_
      Int32 bucketNum = determineBucketNum(fileInfos[f].mName);
      HHDFSBucketStats *bucketStats = NULL;
      if (! bucketStatsList_.used(bucketNum))
      {
	bucketStats = new(heap_) HHDFSBucketStats(heap_, getTable());
	bucketStatsList_.insertAt(bucketNum, bucketStats);
      }
      else
	bucketStats = bucketStatsList_[bucketNum];
      
      if ( filesEstimated > filesToEstimate )
	doEstimate = FALSE;
      else
	filesEstimated++;
      
      bucketStats->addFile(fs, &fileInfos[f], diags, doEstimate, 
			   recordTerminator, NULL_COLL_INDEX, isORC, isParquet);

    }
  }
  
  hdfsFreeFileInfo(fileInfos, numFiles);
  hdfsFreeFileInfo(dirInfo,1);
  return ;
}

 
void* addFileToBucket(void *args)
{
  threadedOrcFileInfoDesc *infoDesc = (threadedOrcFileInfoDesc *)args;
  HHDFSPartitionStats *partStats = infoDesc->partStats;
  hdfsFileInfo *fileInfos = infoDesc->fileInfos;
  cmpCurrentContext = infoDesc->context;

  for (Int32 f = 0; f < infoDesc->numFiles && infoDesc->diags.isSuccess(); ++f)
    if (f % infoDesc->numThreads == infoDesc->pid)
      if (fileInfos[f].mKind == kObjectKindFile) {
        HHDFSBucketStats *bucketStats = partStats->determineBucket(fileInfos[f].mName);
        if (infoDesc->filesEstimated > infoDesc->filesToEstimate)
          infoDesc->doEstimate = FALSE;
        else
          infoDesc->filesEstimated++;

        bucketStats->addFile(infoDesc->fs, &fileInfos[f], infoDesc->diags,
                             infoDesc->doEstimate, infoDesc->recordTerminator,
                             NULL_COLL_INDEX, infoDesc->isORC, infoDesc->isParquet);
      }
    return NULL;
}

HHDFSBucketStats* HHDFSPartitionStats::determineBucket(const char *fileName)
{
  HHDFSBucketStats *bucketStats = NULL;
  Int32 bucketNum = determineBucketNum(fileName);

  pthread_mutex_lock(&mutex_pbs);
  if (! bucketStatsList_.used(bucketNum)) {
    bucketStats = new(heap_)HHDFSBucketStats(heap_, getTable());
    bucketStatsList_.insertAt(bucketNum, bucketStats);
  }
  else
    bucketStats = bucketStatsList_[bucketNum];
  pthread_mutex_unlock(&mutex_pbs);
	
  return bucketStats;
}

static base64Decoder base64Decoder;

NABoolean 
determineObject(const char* source, NAString& path, NAString& name, NAString& type, long& modTime, HHDFSDiags& diags)
{
   const char* delim = "|";

   // find the 1st character past the end of the 3rd '|'
   // so that we copy only the minimal # of fields needed
   // to determine the object type, name (for File), or 
   // modification time (for directory).
   char* ptr = (char*)source;
   for (Int32 i=0; i<3; i++ ) {
      ptr=strchr(ptr, delim[0]);

      if ( !ptr )
        return FALSE;

      ptr++;
   }

   NAString copy(source, ptr-source);

   char* indicator = strtok((char*)copy.data(), delim); // indicator
   if ( indicator == NULL ) 
      return FALSE;

   type = indicator;

   const char* pathStr = strtok(NULL, delim);     // path
   if ( pathStr == NULL ) 
      return FALSE;

  if ( !base64Decoder.decode(pathStr, strlen(pathStr), path) ) {
    char msg[100];
    snprintf(msg, sizeof(msg),
             "Fail to base64 decode directory path %s in HHDFSFileStats::populate()",
             pathStr);
    diags.recordError(msg);
    return FALSE;
  }

   // directory: <indicator>|<path>|<modtime>|
   if ( type == "DirectoryStats" ) {
      const char* mt = strtok(NULL, delim);     // mod time
      if ( mt == NULL ) 
         return FALSE;
      modTime = atol(mt);

      return TRUE;
   }
   else
   // file : <indicator>|<path>|<name>|...
   if ( type == "FileStats" ) {

      const char* nm = strtok(NULL, delim);     // name
      if ( nm == NULL ) 
         return FALSE;

      name = nm;
   } else
      return FALSE;

   return TRUE;
}

void HHDFSPartitionStats::populate(const FileStatsCache& statsCache,
                                   const NAString &dir,
                                   int partIndex,
                                   const char *partitionKeyValues,
                                   Int32 numOfBuckets,
                                   HHDFSDiags &diags,
                                   NABoolean canDoEstimation,
                                   char recordTerminator, 
                                   NABoolean isORC,
                                   NABoolean isParquet,
                                   Int32& filesEstimated)
{
  int numFiles = 0;

  // remember parameters
  partitionDir_     = dir;
  partIndex_        = partIndex;
  defaultBucketIdx_ = (numOfBuckets >= 1) ? numOfBuckets : 0;
  recordTerminator_ = recordTerminator;
  if (partitionKeyValues)
    partitionKeyValues_ = partitionKeyValues;


  NAString* key = NULL;
  NAString* value = NULL;

  NAHashDictionaryIterator<NAString, NAString> itor(statsCache, &dir); 

  // sample only a limited number of files
  Int32 filesToEstimate = CmpCommon::getDefaultLong(HIVE_HDFS_STATS_MAX_SAMPLE_FILES);

  // Process key 'dir', where each value is about the directory dir
  // or a data file within dir.
  for(itor.getNext(key, value); key && value; itor.getNext(key, value))
  {

      NABoolean doEstimate = canDoEstimation;

      NAString path;
      NAString name;
      NAString type;
      long modTime;
      if ( !determineObject(value->data(), path, name, type, modTime, diags) ) {
         return;
      }

      if ( type == "DirectoryStats" ) {
	if (modTime >  modificationTS_)
	  modificationTS_ = modTime;
      } else
      if ( type == "FileStats" ) {
         // the default (unbucketed) bucket number is
         // defaultBucketIdx_
         Int32 bucketNum = determineBucketNum(name.data());
         HHDFSBucketStats *bucketStats = NULL;
   
         if (! bucketStatsList_.used(bucketNum))
         {
             bucketStats = new(heap_) HHDFSBucketStats(heap_, getTable());
             bucketStatsList_.insertAt(bucketNum, bucketStats);
         } else
             bucketStats = bucketStatsList_[bucketNum];
   
         if ( filesEstimated > filesToEstimate )
            doEstimate = FALSE;
         else
            filesEstimated++;
   
         NAString copy(*value, heap_);
         bucketStats->addFile(const_cast<char*>(copy.data()), diags, doEstimate, 
                              recordTerminator, NULL_COLL_INDEX, isORC, isParquet);
      } else {
         diags.recordError("An unknown object type is detected.");
         return;
      }
  }


  // aggregate statistics over all buckets
  for (Int32 b=0; b<=defaultBucketIdx_; b++) {
    if (bucketStatsList_.used(b)) {

      add(bucketStatsList_[b]);

      //if ( bucketStatsList_[b]->getModificationTS() > modificationTS_ )
      //   modificationTS_ = bucketStatsList_[b]->getModificationTS();
    }
  }
}

NABoolean HHDFSPartitionStats::validateAndRefresh(hdfsFS fs, HHDFSDiags &diags, NABoolean refresh, 
                                                      NABoolean isORC, NABoolean isParquet)
{
  NABoolean result = TRUE;

  // assume we get the files sorted by file name
  int numFiles = 0;
  Int32 lastBucketNum = -1;
  ARRAY(Int32) fileNumInBucket(HEAP, getLastValidBucketIndx()+1);
  HHDFSBucketStats *bucketStats = NULL;

  for (CollIndex i=0; i<=getLastValidBucketIndx(); i++)
    fileNumInBucket.insertAt(i, (Int32) -1);

  // to avoid a crash, due to lacking permissions, check the directory
  // itself first
  hdfsFileInfo *dirInfo = hdfsGetPathInfo(fs, partitionDir_.data());

  if (!dirInfo)
    // don't set diags, let caller re-read the entire stats
    return FALSE;

  // list directory contents and compare with cached statistics
  hdfsFileInfo *fileInfos = hdfsListDirectory(fs,
                                              partitionDir_.data(),
                                              &numFiles);
  CMPASSERT(fileInfos || numFiles == 0);

  // populate partition stats
  for (int f=0; f<numFiles && result; f++)
  {
    // file should be ignored 
    if (!HDFSFileValidate(&fileInfos[f]))
      continue ;

    if (fileInfos[f].mKind == kObjectKindFile)
      {
        Int32 bucketNum = determineBucketNum(fileInfos[f].mName);

        if (bucketNum != lastBucketNum)
          {
            if (! bucketStatsList_.used(bucketNum))
              {
                // first file for a new bucket got added
                if (!refresh)
                  return FALSE;
                bucketStats = new(heap_) HHDFSBucketStats(heap_, getTable());
                bucketStatsList_.insertAt(bucketNum, bucketStats);
              }
            else
              bucketStats = bucketStatsList_[bucketNum];
            lastBucketNum = bucketNum;
          }

        // file stats for an existing file, or NULL
        // for a new file
        HHDFSFileStats *fileStats = NULL;
        // position in bucketStats of the file (existing or new)
        fileNumInBucket[bucketNum] = fileNumInBucket[bucketNum] + 1;

        if (fileNumInBucket[bucketNum] < bucketStats->entries())
          fileStats = (*bucketStats)[fileNumInBucket[bucketNum]];
        // else this is a new file, indicated by fileStats==NULL

        if (fileStats &&
            fileStats->getFileName() == fileInfos[f].mName)
          {
            // file still exists, check modification timestamp
            if (fileStats->getModificationTS() !=
                fileInfos[f].mLastMod ||
                fileStats->getTotalSize() !=
                (Int64) fileInfos[f].mSize)
              {
                if (refresh)
                  {
                    // redo this file, it changed
                    subtract(fileStats);
                    bucketStats->removeAt(fileNumInBucket[bucketNum]);
                    fileStats = NULL;
                  }
                else
                  result = FALSE;
              }
            // else this file is unchanged from last time
          } // file name matches
        else
          {
            if (refresh)
              {
                if (fileStats)
                  {
                    // We are looking at a file in the directory, fileInfos[f]
                    // and at a file stats entry, with names that do not match.
                    // This could be because a new file got inserted or because
                    // the file of our file stats entry got deleted or both.
                    // We can only refresh this object in the first case, if
                    // a file got deleted we will return FALSE and not refresh.

                    // check whether fileStats got deleted,
                    // search for fileStats->getFileName() in the directory
                    int f2;
                    for (f2=f+1; f2<numFiles; f2++)
                      if (fileStats->getFileName() == fileInfos[f2].mName)
                        break;

                    if (f2<numFiles)
                      {
                        // file fileInfos[f] got added, don't consume
                        // a FileStats entry, instead add it below
                        fileStats = NULL;
                      }
                    else
                      {
                        // file fileStats->getFileName() got deleted,
                        // it's gone from the HDFS directory,
                        // give up and redo the whole thing
                        result = FALSE;
                      }
                  }
                // else file was inserted (fileStats is NULL)
              }
            else
              result = FALSE;
          } // file names for HHDFSFileStats and directory don't match

        if (result && !fileStats)
          {
            // add this file
            bucketStats->addFile(fs,
                                 &fileInfos[f],
                                 diags,
                                 TRUE, // do estimate for the new file
                                 recordTerminator_,
                                 fileNumInBucket[bucketNum], 
                                 isORC,
                                 isParquet);
            if (!diags.isSuccess())
              {
                result = FALSE;
              }
            else
              add((*bucketStats)[fileNumInBucket[bucketNum]]);
          }
      } 
  } // loop over actual files in the directory

  hdfsFreeFileInfo(fileInfos, numFiles);
  hdfsFreeFileInfo(dirInfo,1);
  // check for file stats that we did not visit at the end of each bucket
  for (CollIndex i=0; i<=getLastValidBucketIndx() && result; i++)
    if (bucketStatsList_.used(i) &&
        bucketStatsList_[i]->entries() != fileNumInBucket[i] + 1)
      result = FALSE; // some files got deleted at the end

  return result;
}

Int32 HHDFSPartitionStats::determineBucketNum(const char *fileName)
{
  Int32 result = 0;
  HHDFSBucketStats *bucketStats;

  // determine bucket number (from file name for bucketed tables)
  if (defaultBucketIdx_ <= 1)
    return 0;

  // figure out name from file prefix bb..bb_*
  const char *mark = fileName + strlen(fileName) - 1;

  // search backwards for the last slash in the name or the start
  while (*mark != '/' && mark != fileName)
    mark--;

  if (*mark == '/')
    mark++;

  // go forward, expect digits, followed by an underscore
  while (*mark >= '0' && *mark <= '9' && result < defaultBucketIdx_)
    {
      result = result*10 + (*mark - '0');
      mark++;
    }

  // we should see an underscore as a separator
  if (*mark != '_' || result > defaultBucketIdx_)
    {
      // this file has no valid bucket number encoded in its name
      // use an artificial bucket number "defaultBucketIdx_" in this case
      result = defaultBucketIdx_;
    }

  return result;
}

void HHDFSPartitionStats::display() { print(stdout); }

void HHDFSPartitionStats::print(FILE *ofd)
{
  fprintf(ofd,"------------- Partition %s\n", partitionDir_.data());
  fprintf(ofd," num of buckets: %d\n", defaultBucketIdx_);
  if (partitionKeyValues_.length() > 0)
    fprintf(ofd," partition key values: %s\n", partitionKeyValues_.data());

  for (CollIndex b=0; b<=defaultBucketIdx_; b++)
    if (bucketStatsList_.used(b))
      {
        fprintf(ofd,"---- statistics for bucket %d:\n", b);
        bucketStatsList_[b]->print(ofd);
      }
  HHDFSStatsBase::print(ofd, "partition");
}

OsimHHDFSStatsBase* HHDFSPartitionStats::osimSnapShot(NAMemory * heap)
{
    OsimHHDFSPartitionStats* stats = new(heap) OsimHHDFSPartitionStats(NULL, this, heap);

    for(Int32 i = 0; i < bucketStatsList_.getUsedLength(); i++)
    {
        //"gaps" are not added, but record the position
        if(bucketStatsList_.getUsage(i) != UNUSED_COLL_ENTRY)
            stats->addEntry(bucketStatsList_[i]->osimSnapShot(heap), i);
    }
    return stats;
}

HHDFSTableStats::~HHDFSTableStats()
{
  for (int p=0; p<totalNumPartitions_; p++)
    delete partitionStatsList_[p];
                                    
  for (CollIndex i=0; i<binaryPartColValues_.entries(); i++) {
     if ( binaryPartColValues_.used(i) )
       NADELETEBASIC(binaryPartColValues_[i], heap_);
  }
}

NABoolean HHDFSTableStats::populateTblParams(struct hive_tblparams_desc *htp)
{
  hiveTblParams_ = htp;

  return TRUE;
}

Int32 HHDFSTableStats::getSkipHeaderCount() const 
{
  if (hiveTblParams_ && isTextFile())
    return hiveTblParams_->getTextSkipHeaderCount();
  else
    return 0;
}

NABoolean HHDFSTableStats::populate(struct hive_tbl_desc *htd)
{
  // here is the basic outline how this works:
  //
  // 1. Walk SD descriptors of the table, one for the table
  //    itself and one for each partition. Each one represents
  //    one HDFS directory with files for the table.
  // 2. For each list partition directory (or the directory for
  //    an unpartitioned table):
  //     3. Walk through every file. For every file:
  //         4. Determine bucket number (0 if file is not bucketed)
  //         5. Add file to its bucket
  //         6. Walk through blocks of file. For every block:
  //             7. Get host list for this block and add it
  //         9. Get file stats
  //     10. Aggregate file stats for all files and buckets
  // 11. Aggregate bucket stats for all buckets of the partition
  // 12. Aggregate partition stats for all partitions of the table

  struct hive_sd_desc *hsd = htd->getSDs();
  if (hsd == NULL)
    return TRUE; // nothing need to be done

  diags_.reset();
  tableDir_ = hsd->location_;
  numOfPartCols_ = htd->getNumOfPartCols();
  recordTerminator_ = hsd->getRecordTerminator();
  fieldTerminator_ = hsd->getFieldTerminator() ;
  nullFormat_ = hsd->getNullFormat();
  numOfBuckets_ = hsd->buckets_;
  serializeLib_ = hsd->getSerializeLib();
  NAString hdfsHost;
  Int32 hdfsPort = -1;
  NAString tableDir;
  NABoolean lobInterfaceInitialized = FALSE;


  if (hsd) {
     if (hsd->isTextFile())
        type_ = TEXT;
     else if (hsd->isSequenceFile())
        type_ = SEQUENCE;
      else if (hsd->isOrcFile()) {
        type_ = ORC;
        HHDFSORCFileStats::resetTotalAccumulatedRows();
      } else if (hsd->isParquetFile()) {
        type_ = PARQUET;
        HHDFSParquetFileStats::resetTotalAccumulatedRows();
      } else if (hsd->isAvroFile())
        type_ = AVRO;
     else
        type_ = ANY;
  }

  // batchReading of parquet footers not supported when error files are
  // skipped. This restriction can be removed by adding code in Java layer
  NABoolean batchReading = 
      ( type_ == PARQUET &&
        (CmpCommon::getDefaultNumeric(PARQUET_BATCH_READ_FILESTATS) > 0) && 
        (CmpCommon::getDefaultNumeric(HIVE_MAX_ERROR_FILES) == 0));

  // sample only a limited number of files
  Int32 filesEstimated = 0;
  Int64 modificationTsInMillis = 0;
  bool newImplementation = FALSE; 
  TaskMonitor tm;
  tm.enter();
  if ((type_ == ORC && (CmpCommon::getDefault(ORC_METADATA_READ_WITH_JAVA_THREADS) == DF_ON)) ||
      (type_ == PARQUET && (CmpCommon::getDefault(PARQUET_METADATA_READ_WITH_JAVA_THREADS) == DF_ON))) {
     newImplementation = TRUE;
     //print log for regression test
     const char *logFile = NULL;
     if (type_ == ORC)
        logFile = ActiveSchemaDB()->getDefaults().getValue(ORC_HDFS_STATS_LOG_FILE);
     else if (type_ == PARQUET)
        logFile = ActiveSchemaDB()->getDefaults().getValue(PARQUET_HDFS_STATS_LOG_FILE);

     if (logFile != NULL && strlen(logFile) != 0)
        statsLogFile_ = logFile;
     // Create Partition at index 0 (not sure why?)
     getStripeOrRowgroupInfo(type_, modificationTsInMillis, filesEstimated);
     // When the table is partitioned, insert a dummy entry
     // at index 0 to avoid core dump elsewhere.
     if (numOfPartCols_ > 0)
     {
        HHDFSPartitionStats *partStats = new(heap_) HHDFSPartitionStats(heap_, this);
        partStats->init(0, tableDir_, 0, NULL);
        partitionStatsList_.insertAt(0, partStats);
     }
     if (modificationTS_ < modificationTsInMillis)
        modificationTSInMillisec_ = htd->setRedeftime(modificationTsInMillis);
     else
        modificationTSInMillisec_ = htd->setRedeftime(modificationTS_);
  } else {
  // split table URL into host, port and filename
  if (! splitLocation(hsd->location_,
                hdfsHost,
                hdfsPort,
                tableDir,
                diags_,
                hdfsPortOverride_)) 
      return FALSE;
  if (!lobInterfaceInitialized) {
     initLOBInterface(hdfsHost,hdfsPort);
     lobInterfaceInitialized = TRUE;
  }
  if (! connectHDFS(hdfsHost, hdfsPort))  {
     releaseLOBInterface();
     return FALSE; // diags_ is set
  }
  // put back fully qualified URI
  tableDir = hsd->location_;
  computeModificationTSmsec();
  if (batchReading && fileNumExceedsLimit(tableDir, diags_, heap_))
    batchReading = FALSE;

  if (diags_.isSuccess()) {
     modificationTSInMillisec_ = htd->setRedeftime(modificationTSInMillisec_);
     while (hsd && diags_.isSuccess()) {
        tableDir = hsd->location_;
        NABoolean canDoEstimate = hsd->isTrulyText() || 
                                hsd->isOrcFile() ||
                                hsd->isParquetFile();

        if ( batchReading ) {
           if ( !fileStatsCache_.populated() ) {
              // read in all the file stats in batch and 
              // store the results in fileStatsCache_
	     fileStatsCache_.populateCache(htd, diags_);
           }
        }
        // populate the directory. It is necessary to 
        // populate even when tableDir points at the
        // parent directory of all the partitions, due
        // to the need to have such a partition stats 
        // available at partitionStatsList_[0].
        processDirectory(batchReading, tableDir,
                       hsd->partitionColValues_,
                       hsd->buckets_, 
                       canDoEstimate, 
                       hsd->getRecordTerminator(), 
                       type_==ORC, 
                       type_==PARQUET, 
                       filesEstimated);

        hsd = hsd->next_;
     }
  }
} 
   tm.exit();

   if (CmpCommon::getDefault(HIVE_REPORT_FILESTATS_READ_ET) == DF_ON)
    {
      cout << "Total ET to read file stats for " << tableDir.data();
      cout << " =" << tm.elapsed_time() << "s" << endl;
    }

  if (htd->tblParams_)
    populateTblParams(htd->tblParams_);
  if (! newImplementation) {
     disconnectHDFS();
     releaseLOBInterface();
  }
  validationJTimestamp_ = JULIANTIMESTAMP();

  return diags_.isSuccess();
}

void HHDFSTableStats::getStripeOrRowgroupInfo(HiveFileType fileType, Int64 &modificationTsInMillis, Int32& filesEstimated)
{
   HVC_RetCode hvcRetcode;

   int numFilesToSample = CmpCommon::getDefaultLong(HIVE_HDFS_STATS_MAX_SAMPLE_FILES);
   NABoolean allowSubdirs = 
     (CmpCommon::getDefault(HIVE_SUPPORTS_SUBDIRECTORIES) == DF_ON);
   HiveClient_JNI *hiveClient  = HiveClient_JNI::newInstance((NAHeap *)heap_, hvcRetcode);
   if (hvcRetcode != HVC_OK) {
      diags_.recordError(NAString("Could not get HiveClient_JNI::newInstance ") + getSqlJniErrorStr());
      return;
   }
   hvcRetcode = hiveClient->getStripeOrRowgroupInfo(fileType, tableDir_, numOfPartCols_, numFilesToSample, allowSubdirs, modificationTsInMillis, this); 
   if (hvcRetcode != HVC_OK) {
      diags_.recordError(NAString("Could not get HiveClient_JNI::getStripeOrRowgroupInfo for " + tableDir_ + " " + getSqlJniErrorStr())); 
      NADELETE(hiveClient, HiveClient_JNI, heap_);
      return;
   }
   filesEstimated = hiveClient->getNumFilesStatsRead();
   NADELETE(hiveClient, HiveClient_JNI, heap_);
   return;
}


void HHDFSTableStats::setStripeOrRowgroupInfo(jint partIndex, const char *partDir, 
               const char *partKeyValues, hdfsFileInfo *fileInfo, int numStripesOrRowgroups, 
               jlong *pOffsets, jlong *pLengths, jlong *pNumRows, int numBlocks, jobjectArray j_blockHosts)
{
   HHDFSPartitionStats *partStats; 
   HHDFSFileStats *fileStats = NULL;
 
   NABoolean sortHosts = (CmpCommon::getDefault(HIVE_SORT_HDFS_HOSTS) == DF_ON);

   if (!HDFSFileValidate(fileInfo))
      return ;
 
   if (! partitionStatsList_.used(partIndex))
   {
      partStats = new(heap_) HHDFSPartitionStats(heap_, this);
      partStats->init(partIndex, partDir, numOfBuckets_, partKeyValues);
      partitionStatsList_.insertAt(partIndex, partStats);
   } else
      partStats = partitionStatsList_[partIndex];
   fileStats = partStats->setStripeOrRowgroupInfo(type_, fileInfo, numStripesOrRowgroups, pOffsets, pLengths, pNumRows, numBlocks, j_blockHosts, sortHosts);
   if (fileStats != NULL)
      add(fileStats);
}

NABoolean HHDFSTableStats::validateAndRefresh(Int64 expirationJTimestamp, NABoolean refresh)
{
  NABoolean result = TRUE;
  // initial heap allocation size
  Int32 initialSize = heap_->getAllocSize();
  NABoolean lobInterfaceInitialized = FALSE;

  diags_.reset();

  // check if the stats needs to be fetched within a specified time interval
  // when not requested to refresh
  if (! refresh && (expirationJTimestamp == -1 ||
      (expirationJTimestamp > 0 &&
       validationJTimestamp_ < expirationJTimestamp)))
    return result; // consider the stats still valid

  // if partitions get added or deleted, that gets
  // caught in the Hive metadata, so no need to check for
  // that here
  for (int p=0; p<totalNumPartitions_ && result && diags_.isSuccess(); p++)
    {
      HHDFSPartitionStats *partStats = partitionStatsList_[p];
      NAString hdfsHost;
      Int32 hdfsPort;
      NAString partDir;

      result = splitLocation(partStats->getDirName(),
                             hdfsHost,
                             hdfsPort, 
                             partDir,
                             diags_,
                             hdfsPortOverride_);
      if (! result)
        break;

      if (!lobInterfaceInitialized){
    	  initLOBInterface(hdfsHost, hdfsPort);
    	  lobInterfaceInitialized = TRUE;
      }

      if (! connectHDFS(hdfsHost, hdfsPort))
        {
          releaseLOBInterface();
          return FALSE;
        }

      subtract(partStats);
      result = partStats->validateAndRefresh(fs_, diags_, refresh, type_==ORC, type_==PARQUET);
      if (result)
        add(partStats);
    }

  disconnectHDFS();
  releaseLOBInterface();

  validationJTimestamp_ = JULIANTIMESTAMP();
  // account for the heap used by stats. Heap released during
  // stats refresh will also be included
  hiveStatsSize_ += (heap_->getAllocSize() - initialSize);

  return result;
}

NABoolean HHDFSTableStats::splitLocation(const char *tableLocation,
                                         NAString &hdfsHost,
                                         Int32 &hdfsPort,
                                         NAString &tableDir,
                                         HHDFSDiags &diags,
                                         int hdfsPortOverride)
{
  const char *hostMark = NULL;
  const char *portMark = NULL;
  const char *dirMark  = NULL;
  const char *fileSysTypeTok = NULL;
  bool isAdl = false;
  bool isS3 = false;
  bool isS3a = false;
  bool isAlluxio = false;
  bool isLocal = false;

  // The only filesysTypes supported are hdfs: maprfs: s3a: alluxio: s3: adl:
  // One of these tokens must appear at the the start of tableLocation

  // hdfs://localhost:35000/hive/tpcds/customer
  if (fileSysTypeTok = strstr(tableLocation, "hdfs:"))
    tableLocation = fileSysTypeTok + 5; 
  // maprfs:/user/hive/warehouse/f301c7af0-2955-4b02-8df0-3ed531b9abb/select
  else if (fileSysTypeTok = strstr(tableLocation, "file:")) {
    tableLocation = fileSysTypeTok + 5; 
    isLocal = true;
  }
  else if (fileSysTypeTok = strstr(tableLocation, "maprfs:"))
    tableLocation = fileSysTypeTok + 7; 
  else if (fileSysTypeTok = strstr(tableLocation, "s3:")){
    tableLocation = fileSysTypeTok + 3;
    isS3 = true;
  }
  else if (fileSysTypeTok = strstr(tableLocation, "s3a:")){
    tableLocation = fileSysTypeTok + 4;
    isS3a = true;
  }
  else if (fileSysTypeTok = strstr(tableLocation, "alluxio:")){
    tableLocation = fileSysTypeTok + 8;
    isAlluxio = true;
  }
  else if (fileSysTypeTok = strstr(tableLocation,"adl:")){
    tableLocation = fileSysTypeTok + 4;
    isAdl = true;
  }
  else {
      diags.recordError(NAString("Expected hdfs: maprfs: s3: s3a: alluxio: adl: in the HDFS URI ") + tableLocation,
                        "HHDFSTableStats::splitLocation");
      return FALSE;
  }

  
  // The characters that  come after "//" is the hostName.
  // "//" has to be at the start of the string (after hdfs: or maprfs: or s3: or s3a: or alluxio: or adl:)
  if ((hostMark = strstr(tableLocation, "//"))&&
      (hostMark == tableLocation))
    {
      hostMark = hostMark + 2;
      
      dirMark = strchr(hostMark, '/');
      if (dirMark == NULL)
        {
          diags.recordError(NAString("Could not find slash in HDFS directory name ") + tableLocation,
                            "HHDFSTableStats::splitLocation");
          return FALSE;
        }

      // if there is a hostName there could be a hostPort too.
      // It is not not an error if there is a hostName but no hostPort
      // for example  hdfs://localhost/hive/tpcds/customer is valid
      portMark = strchr(hostMark, ':');
      if (portMark && (portMark < dirMark))
        portMark = portMark +1 ;
      else
        portMark = NULL; 
    }
  else // no host location, for example maprfs:/user/hive/warehouse/
    {
      hostMark = NULL;
      portMark = NULL;
      if (*tableLocation != '/') 
        {
          diags.recordError(NAString("Expected a maprfs:/<filename> URI: ") + tableLocation,
                            "HHDFSTableStats::splitLocation");
          return FALSE;
        }
      dirMark = tableLocation;
    }

  
  if (hostMark){
     if (isS3)
       hdfsHost = NAString(hostMark - 5, (portMark ? portMark-(hostMark-5)-1 //-5 to get back the s3:// as part of the host name so that re correct filesystem is returned for s3
                                                   : dirMark-(hostMark-5)));  
  	else if (isS3a)
       hdfsHost = NAString(hostMark - 6, (portMark ? portMark-(hostMark-6)-1 //-6 to get back the s3a:// as part of the host name so that re correct filesystem is returned for s3a
                                                   : dirMark-(hostMark-6)));
	else if (isAlluxio)
       hdfsHost = NAString(hostMark - 10, (portMark ? portMark-(hostMark-10)-1 //-10 to get back the alluxio:// as part of the host name so that re correct filesystem is returned for alluxio
                                                    : dirMark-(hostMark-10)));
	else if (isAdl)
       hdfsHost = NAString(hostMark - 6, (portMark ? portMark-(hostMark-6)-1 //-6 to get back the adl:// as part of the host name so that re correct filesystem is returned for adl
                                                   : dirMark-(hostMark-6)));
    else
       hdfsHost    = NAString(hostMark, (portMark ? portMark-hostMark-1
                                                  : dirMark-hostMark));
  }
  else {
    if (isLocal) {
      hdfsHost = NAString("__local_fs__");
    }
    else {
      hdfsHost = NAString("default");
    }
  }

  if (hdfsPortOverride > -1)
    hdfsPort    = hdfsPortOverride;
  else
    if (portMark)
      hdfsPort  = atoi(portMark);
    else
      hdfsPort  = 0;
  tableDir      = NAString(dirMark);
  return TRUE;
}

NABoolean HHDFSTableStats::splitLocation(const char *tableLocation,
                                         NAString &hdfsHost,
                                         Int32 &hdfsPort,
                                         HHDFSDiags &diags)
{
  const char *hostMark = NULL;
  const char *hostEndMark = NULL;
  const char *portMark = NULL;
  const char *fileSysTypeTok = NULL;
  bool isAdl = false;
  bool isS3 = false;
  bool isS3a = false;
  bool isAlluxio = false;
  bool isLocal = false;

  // The only six filesysTypes supported are hdfs: and maprfs: s3: s3a: and alluxio: adl:
  // One of these six tokens must appear at the the start of tableLocation

  // hdfs://localhost:35000/hive/tpcds/customer
  if (fileSysTypeTok = strstr(tableLocation, "hdfs:"))
    tableLocation = fileSysTypeTok + 5; 
  else if (fileSysTypeTok = strstr(tableLocation, "file:")) {
    tableLocation = fileSysTypeTok + 5; 
    isLocal = true;
  }
  // maprfs:/user/hive/warehouse/f301c7af0-2955-4b02-8df0-3ed531b9abb/select
  else if (fileSysTypeTok = strstr(tableLocation, "maprfs:"))
    tableLocation = fileSysTypeTok + 7;
  else if (fileSysTypeTok = strstr(tableLocation, "s3:"))
  {
    tableLocation = fileSysTypeTok + 3;
    isS3 = true;
  }
  else if (fileSysTypeTok = strstr(tableLocation, "s3a:"))
  {
    tableLocation = fileSysTypeTok + 4;
    isS3a = true;
  }
  else if (fileSysTypeTok = strstr(tableLocation, "alluxio:"))
  {
    tableLocation = fileSysTypeTok + 8;
    isAlluxio = true;
  }
  else if (fileSysTypeTok = strstr(tableLocation, "adl:"))
  {
    tableLocation = fileSysTypeTok + 4;
    isAdl = true;
  }
  else
    {
      diags.recordError(NAString("Expected hdfs: maprfs: s3: s3a: alluxio: adl: in the HDFS URI ") + tableLocation,
                        "HHDFSTableStats::splitLocation");
      return FALSE;
    }

  
  // The characters that  come after "//" is the hostName.
  // "//" has to be at the start of the string (after hdfs: or maprfs: or s3: or s3a: or alluxio: or adl:)
  if ((hostMark = strstr(tableLocation, "//"))&&
      (hostMark == tableLocation))
    {
      hostMark += 2;
      
      // if there is a hostName there could be a hostPort too.
      // It is not not an error if there is a hostName but no hostPort
      portMark = strchr(hostMark, ':');

      // Any characters prior to ':' are part of the host name.
      // for example  hdfs://localhost:1234/hive/tpcds/customer is valid
      //                              ^
      if (portMark) {
        hostEndMark = portMark-1;
        portMark++;
      } else {

         // Any characters prior to '/' are part of the host name.
         // For example hdfs://localhost/hive/tpcds/customer is valid
         //                             ^ 
         hostEndMark = strchr(hostMark, '/');
         if (hostEndMark) {
           hostEndMark--;
         } else {
           hostEndMark = hostMark + strlen(hostMark) - 1;
         }
      }
    }
  else // no host location, for example maprfs:/user/hive/warehouse/
    {
      hostMark = NULL;
      portMark = NULL;
    }

  if (hostMark && hostEndMark && hostMark <= hostEndMark )
  {
    if (isS3)
      hdfsHost = NAString(hostMark - 5, hostEndMark-(hostMark-5)+1); //add s3:// back to make sure we pick the correct FileSystem for s3
    else if (isS3a)
      hdfsHost = NAString(hostMark - 6, hostEndMark-(hostMark-6)+1); //add s3a:// back to make sure we pick the correct FileSystem for s3
    else if (isAlluxio)
      hdfsHost = NAString(hostMark - 10, hostEndMark-(hostMark-10)+1); //add alluxio:// back to make sure we pick the correct FileSystem for alluxio
    else if (isAdl)
      hdfsHost = NAString(hostMark - 6, hostEndMark-(hostMark-6)+1); //add adl:// back to make sure we pick the correct FileSystem for adl

    else
      hdfsHost = NAString(hostMark, hostEndMark-hostMark+1);
  }
  else {
    if (isLocal) {
      hdfsHost = NAString("__local_fs__");
    }
    else {
      hdfsHost = NAString("default");
    }
  }

  if (portMark)
    hdfsPort  = atoi(portMark);
  else
    hdfsPort  = 0;

  return TRUE;
}

void HHDFSTableStats::processDirectory(NABoolean batchRead,
                                       const NAString &dir,
                                       const char *partColValues,
                                       Int32 numOfBuckets, 
                                       NABoolean canDoEstimate,
                                       char recordTerminator,
                                       NABoolean isORC, 
                                       NABoolean isParquet, 
                                       Int32& filesEstimated)
{
  HHDFSPartitionStats *partStats = new(heap_)
    HHDFSPartitionStats(heap_, this);

  NABoolean allowSubdirs = 
    (CmpCommon::getDefault(HIVE_SUPPORTS_SUBDIRECTORIES) == DF_ON);
  // For partitioned tables do not look at subdirs under the rootdir
  // of whole table. If we did it would include all data files.
  allowSubdirs = allowSubdirs && 
    ((numOfPartCols() == 0) || (partitionStatsList_.entries() > 0)) ;
  
  if ( !batchRead ) {
     partStats->populate(fs_, 
                         dir, partitionStatsList_.entries(),
                         partColValues, numOfBuckets,
                         diags_, canDoEstimate, recordTerminator, 
                         isORC, isParquet, allowSubdirs,
                         filesEstimated);
  } else {
     partStats->populate(fileStatsCache_,
                         dir, partitionStatsList_.entries(),
                         partColValues, numOfBuckets,
                         diags_, canDoEstimate, recordTerminator, 
                         isORC, isParquet,
                         filesEstimated);
  }

  if (diags_.isSuccess())
    {
      partitionStatsList_.insertAt(partitionStatsList_.entries(), partStats);
      totalNumPartitions_++;
      // aggregate stats
      add(partStats);

      //if (partStats->getModificationTS() > modificationTS_)
      //  modificationTS_ = partStats->getModificationTS();
    }
}

Int32 HHDFSTableStats::getNumOfConsistentBuckets() const
{
  Int32 result = 0;

  // go through all partitions and chck whether they have
  // the same # of buckets and have no files w/o enforced bucketing
  for (Int32 i=0; i<partitionStatsList_.entries(); i++)
    {
      Int32 b = partitionStatsList_[i]->getLastValidBucketIndx();

      if (result == 0)
        result = b;
      if (b <= 1 || b != result)
        return 1; // some partition not bucketed or different from others
      if ((*partitionStatsList_[i])[b] != NULL)
        return 1; // this partition has files that are not bucketed at all
                  // and are therefore assigned to the exception bucket # b
    }
  // everything is consistent, with multiple buckets
  return result;
}

void HHDFSTableStats::setupForStatement()
{
}

void HHDFSTableStats::resetAfterStatement()
{
}

void HHDFSTableStats::display() { print(stdout); }

void HHDFSTableStats::print(FILE *ofd)
{
  fprintf(ofd,"====================================================================\n");
  fprintf(ofd,"HDFS file stats for directory %s\n", tableDir_.data());
  fprintf(ofd,"  number of part cols: %d\n", numOfPartCols_);
  fprintf(ofd,"  total number of partns: %d\n", totalNumPartitions_);
  fprintf(ofd,"  Record Terminator: %d\n", recordTerminator_);
  fprintf(ofd,"  Field Terminator: %d\n", fieldTerminator_);
  
  for (CollIndex p=0; p<partitionStatsList_.entries(); p++)
    partitionStatsList_[p]->print(ofd);
  HHDFSStatsBase::print(ofd, "table");
  fprintf(ofd,"\n");
  fprintf(ofd,"end of HDFS file stats for directory %s\n", tableDir_.data());
  fprintf(ofd,"====================================================================\n");
}

void HHDFSTableStats::initLOBInterface(const NAString &host, Int32 port)
{
  lobGlob_ = NULL;

  ExpLOBinterfaceInit
    (lobGlob_, (NAHeap *)heap_, GetCliGlobals()->currContext(), TRUE, (char *)host.data(), port);
}

void HHDFSTableStats::releaseLOBInterface()
{
  ExpLOBinterfaceCleanup
    (lobGlob_);
}

NABoolean HHDFSTableStats::connectHDFS(const NAString &host, Int32 port)
{
  NABoolean result = TRUE;

  // establish connection to HDFS . Conect to the connection cached in the context.
 
  
  fs_ = ((GetCliGlobals()->currContext())->getHdfsServerConnection((char *)host.data(),port));
     
      
      if (fs_ == NULL)
        {
          NAString errMsg("hdfsConnect to ");
          errMsg += host;
          errMsg += ":";
          errMsg += port;
          errMsg += " failed";
          diags_.recordError(errMsg, "HHDFSTableStats::connectHDFS");
          result = FALSE;
        }
      currHdfsHost_ = host;
      currHdfsPort_ = port;
      //  }
  return result;
}

void HHDFSTableStats::disconnectHDFS()
{
  // No op. The disconnect happens at the context level wehn the session 
  // is dropped or the thread exits.
}

void HHDFSTableStats::computeModificationTSmsec()
{
      HDFS_Client_RetCode rc;
      Lng32 depthLevel = (CmpCommon::getDefault(HIVE_SUPPORTS_SUBDIRECTORIES) == DF_OFF) ? numOfPartCols_ : -1 ;

      // get a millisecond-resolution timestamp via JNI
      rc = HdfsClient::getHiveTableMaxModificationTs(
               modificationTSInMillisec_,
               tableDir_.data(),
               depthLevel);
      // check for errors and timestamp mismatches
      if (rc != HDFS_CLIENT_OK || modificationTSInMillisec_ <= 0)
        {
          NAString errMsg;

          errMsg.format("Exception %s when reading msec timestamp for HDFS URL %s",
                        getSqlJniErrorStr(), 
                        tableDir_.data());
          diags_.recordError(errMsg, "HHDFSTableStats::computeModificationTSmsec");
          modificationTSInMillisec_ = -1;
        }
}

OsimHHDFSStatsBase* HHDFSTableStats::osimSnapShot(NAMemory * heap)
{
    OsimHHDFSTableStats* stats = new(heap) OsimHHDFSTableStats(NULL, this, heap);

    for(Int32 i = 0; i < partitionStatsList_.getUsedLength(); i++)
    {
        //"gaps" are not added, but record the position
        if(partitionStatsList_.getUsage(i) != UNUSED_COLL_ENTRY)
            stats->addEntry(partitionStatsList_[i]->osimSnapShot(heap), i);
    }
    return stats;
}

OsimHHDFSStatsBase* HHDFSFileStats::osimSnapShot(NAMemory * heap)
{
    OsimHHDFSFileStats* stats = new(heap) OsimHHDFSFileStats(NULL, this, heap);

    return stats;
}

// compute cache file name. include variability based not only on lastModification timestamp, but also on CQD that can influence data in hive stats
NAString ComputeCacheFilePath(Int64 lastModificationTs, const NAString & tableName, NABoolean canDoEstimate){
    NAString filePath = "/user/trafodion/.hiveStats/";
    filePath.append(tableName)
            .append('-')
            .append(std::to_string((long long unsigned int)lastModificationTs).c_str())
            .append('-')
            .append((CmpCommon::getDefault(ORC_READ_STRIPE_INFO) == DF_ON)?"STRIPE":"NOSTRIPE")
            .append((CmpCommon::getDefault(ORC_READ_NUM_ROWS) == DF_ON)?"NUMROWS":"NONUMROWS")
            .append(canDoEstimate?"ESTIMATE":"NOESTIMATE")
            .append(std::to_string((long long int)(CmpCommon::getDefaultLong(HIVE_HDFS_STATS_MAX_SAMPLE_FILES))).c_str());
    return filePath;

}

HHDFSTableStats::HHDFSTableStats(NAMemory *heap) : HHDFSStatsBase(this),
                                    currHdfsPort_(-1),
                                    fs_(NULL),
                                    hdfsPortOverride_(-1),
                                    tableDir_(heap),
                                    numOfPartCols_(0),
                                    totalNumPartitions_(0),
                                    recordTerminator_(0),
                                    fieldTerminator_(0),
                                    nullFormat_(NULL),
                                    serializeLib_(NULL),
                                    validationJTimestamp_(-1),
                                    partitionStatsList_(heap),
                                    binaryPartColValues_(heap),
                                    hiveStatsSize_(0),
                                    heap_(heap),
                                    type_(ANY),
                                    lobGlob_(NULL),
                                    hiveTblParams_(NULL),
                                    fileStatsCache_(heap),
                                    statsLogFile_(NULL),
                                    modificationTSInMillisec_(-1)
{
}


void HHDFSTableStats::captureHiveTableStats(const NAString &tableName, Int64 lastModificationTs, hive_tbl_desc* hvt_desc)
{
         XMLFormattedString * xmltext = new (heap_) XMLFormattedString(heap_);
         OsimHHDFSStatsBase* root = osimSnapShot(heap_);

          if(!root){
              diags_.recordError(NAString("root was null on captureHiveTableStats"));
              NADELETE(xmltext, XMLFormattedString, heap_);
              NADELETE(root,OsimHHDFSStatsBase, heap_);
              return;
          }
         root->toXML(*xmltext);

         struct hive_sd_desc *hsd = hvt_desc->getSDs();
         NABoolean canDoEstimate = hsd->isTrulyText() || hsd->isOrcFile();
         NAString filePath = ComputeCacheFilePath(lastModificationTs, tableName, canDoEstimate);
         hdfsFS fs = ((GetCliGlobals()->currContext())->getHdfsServerConnection(NULL,0));// hive stats are cached on default HDFS file system

         if (fs == NULL){
            diags_.recordError(NAString("fs was null on captureHiveTableStats"));
            NADELETE(xmltext, XMLFormattedString, heap_);
            NADELETE(root,OsimHHDFSStatsBase, heap_);
            return;//should not happen
         }

        if(hdfsExists(fs,filePath)){//bypass if someone is already writing this file
            hdfsFile writeFile = hdfsOpenFile(fs, filePath.data(), O_WRONLY | O_CREAT, 0, 1, 0);//force replication to 1, as this is just a cache file and will recreate on failure.
            if (!writeFile){
                diags_.recordError(NAString("could not open file for writing hiveTableStats  on captureHiveTableStats"));
                NADELETE(xmltext, XMLFormattedString, heap_);
                NADELETE(root,OsimHHDFSStatsBase, heap_);
                return; // add error reporting
            }
            xmltext->append('\n');//extra \n required by the XMLParser
            tSize num_written_bytes = hdfsWrite(fs,writeFile,(void*)xmltext->data(),strlen(xmltext->data())+1);
            hdfsFlush(fs,writeFile);
            hdfsCloseFile(fs,writeFile);
        }
         NADELETE(xmltext, XMLFormattedString, heap_);
         NADELETE(root,OsimHHDFSStatsBase, heap_);
}


HHDFSTableStats * HHDFSTableStats::restoreHiveTableStats(const NAString & tableName, Int64 lastModificationTs, hive_tbl_desc* hvt_desc, NAMemory* heap){

const unsigned int bufSize = 65536; // ran tests to get optimal buf size.8K 276ms, 16k 215ms,32k 180ms, 64k 158ms, 128k 148ms, 256k 142ms, 512k 141ms, 1M 158 ms
OsimHHDFSTableStats* hhTableStats = 0;
OsimHHDFSStatsMapper om(heap);
XMLDocument doc((NAHeap *)heap, om);
struct hive_sd_desc *hsd = hvt_desc->getSDs();
NABoolean canDoEstimate = hsd->isTrulyText() || hsd->isOrcFile();
NAString filePath = ComputeCacheFilePath(lastModificationTs, tableName, canDoEstimate);
// split table URL into host, port and filename
NAString tableDir;
NAString hdfsHost;
Int32 hdfsPort = -1;
HHDFSDiags hDiags;
 if (! splitLocation(hsd->location_, hdfsHost, hdfsPort, tableDir, hDiags, -1)){
    HHDFSTableStats* error = new(heap) HHDFSTableStats(heap);
    ((HHDFSDiags &)error->getDiags()).recordError(hDiags.getErrMsg(), hDiags.getErrLoc());
    return error;//should not happen
}
Int32 portOverride = CmpCommon::getDefaultLong(HIVE_LIB_HDFS_PORT_OVERRIDE);
if (portOverride>0)
    hdfsPort = portOverride;

hdfsFS fs = ((GetCliGlobals()->currContext())->getHdfsServerConnection(NULL,0));// hive stats are cached on default HDFS file system

if (fs == NULL){
    HHDFSTableStats* error = new(heap) HHDFSTableStats(heap);
    ((HHDFSDiags &)error->getDiags()).recordError(NAString("fs was null in restoreHiveTableStats"));
    return error;//should not happen
}
hdfsFile readFile = hdfsOpenFile(fs, filePath.data(), O_RDONLY , bufSize, 1, 0);

if (!readFile){
    //no cache hit normal path
    return NULL;
}
char * txt = new (heap) char[bufSize];
tSize t = hdfsRead(fs, readFile, (void*) txt, bufSize);
while(t>0){
    if(txt[t-1] == '\000'){ // xml file is null char terminated. so use this as end of file condition.
        hhTableStats = (OsimHHDFSTableStats *)doc.parse(txt, t-1, 1);
        break;
    }else
        hhTableStats = (OsimHHDFSTableStats *)doc.parse(txt, t, 0);
    t = hdfsRead(fs, readFile, (void*) txt, bufSize);
}
hdfsCloseFile(fs,readFile);

NADELETEBASIC(txt,heap);
HHDFSTableStats* hiveHDFSTableStats = (HHDFSTableStats *)(hhTableStats->getHHStats());
NADELETE (hhTableStats,OsimHHDFSTableStats,heap);
if(hiveHDFSTableStats == NULL){
    HHDFSTableStats* error = new(heap) HHDFSTableStats(heap);
    ((HHDFSDiags &)error->getDiags()).recordError(NAString("hiveHDFSTableStats was null in restoreHiveTableStats"));
    return error;//should not happen
}
hiveHDFSTableStats->setPortOverride(CmpCommon::getDefaultLong(HIVE_LIB_HDFS_PORT_OVERRIDE));
hiveHDFSTableStats->tableDir_ = hsd->location_;
hiveHDFSTableStats->validationJTimestamp_ = JULIANTIMESTAMP();
hiveHDFSTableStats->connectHDFS(hdfsHost, hdfsPort); //will set diag_ on failure

return hiveHDFSTableStats;
}


HHDFSFileStats::PrimarySplitUnit HHDFSFileStats::getSplitUnitType(Int32 balanceLevel) const
{
  if (!compressionInfo_.isCompressed() && balanceLevel >= 0)
    return SPLIT_AT_HDFS_BLOCK_LEVEL_;
  else
    return SPLIT_AT_FILE_LEVEL_;

  // return another value here, once splittable compression is enabled
}

Int32 HHDFSFileStats::getNumOFSplitUnits(HHDFSFileStats::PrimarySplitUnit su) const
{
  if (su == SPLIT_AT_HDFS_BLOCK_LEVEL_)
    // size in blocks, rounded up to the next full block
    return (getTotalSize() + getBlockSize() - 1) / getBlockSize();
  else
    // whole file is one unit
    return 1;
}

void HHDFSFileStats::getSplitUnit(Int32 ix, Int64 &offset, Int64 &length,
                                  HHDFSFileStats::PrimarySplitUnit su) const
{
  if (su == SPLIT_AT_HDFS_BLOCK_LEVEL_)
    {
      length = getBlockSize();
      offset = ix * getBlockSize();

      // check for a partial block at the end
      if (offset + length > getTotalSize())
        length = getTotalSize() - offset;
    }
  else
    {
      offset = 0;
      length = getTotalSize();
    }
}

Int32 HHDFSFileStats::getHDFSBlockNumForSplitUnit(
     Int32 ix,
     HHDFSFileStats::PrimarySplitUnit su) const
{
  if (su == SPLIT_AT_HDFS_BLOCK_LEVEL_)
    {
      // split unit is the same as HDFS blocks
      return ix;
    }
  else
    if (getTotalSize() > 2*getBlockSize())
      // entire file is too large to use locality
      return -1;
    else
      // first HDFS block is a significant portion of the file
      return 0;
}

NABoolean HHDFSFileStats::splitsOfPrimaryUnitsAllowed(HHDFSFileStats::PrimarySplitUnit su) const
{
  return (su == SPLIT_AT_HDFS_BLOCK_LEVEL_);
}

// Assign all blocks in this to ESPs, considering locality
Int64 HHDFSFileStats::assignToESPs(Int64 *espDistribution,
                                   NodeMap* nodeMap,
                                   Int32 numSQNodes,
                                   Int32 numESPs,
                                   HHDFSPartitionStats *partition,
                                   Int32 &nextDefaultPartNum,
                                   NABoolean useLocality,
                                   Int32 balanceLevel)
{
   Int64 totalBytesAssigned = 0;
   Int64 offset = 0;
   Int64 hdfsBlockSize = getBlockSize();

   // the number of blocks, stripes, file, etc we have
   PrimarySplitUnit su = getSplitUnitType(balanceLevel);
   Int64 numOfSplitUnits = getNumOFSplitUnits(su);

   for (Int64 b=0; b<numOfSplitUnits; b++)
     {
       // find the host for the first replica of this block,
       // the host id is also the SQ node id
       Int32 partNum = -1;
       Int64 bytesToRead = 0;
       Int32 hdfsBlockNum;

       getSplitUnit(b, offset, bytesToRead, su);
       hdfsBlockNum = getHDFSBlockNumForSplitUnit(b, su);

       if (useLocality && hdfsBlockNum >= 0)
         {
           HostId h = getHostId(0, hdfsBlockNum);

           partNum = h;
         }

       if (partNum < 0 || partNum >= numESPs /*tbd:remove*/ || partNum >= numSQNodes)
         {
           // We don't use locality or don't have ESPs covering this
           // node, assign a default partition.
           // NOTE: If we have fewer ESPs than SQ nodes we should
           // really be doing AS, using affinity - TBD later.
           partNum = nextDefaultPartNum++;
           if (nextDefaultPartNum >= numESPs)
             nextDefaultPartNum = 0;

           // we are not using locality for this block/range,
           // note that it may still be local by chance
           hdfsBlockNum = -1;
         }

       // If we have multiple ESPs per SQ node, pick the one with the
       // smallest load so far. ESPs with   ESP# % numSQNodes == n
       // will run on node n.
       Int32 startPartNum = partNum;
       Int32 currPartNum = startPartNum;

       do
         {
           if (espDistribution[currPartNum] < espDistribution[partNum])
             partNum = currPartNum;

           currPartNum += numSQNodes;
           if (currPartNum >= numESPs)
             currPartNum = startPartNum % numSQNodes;
         }
       while (currPartNum != startPartNum);

       HiveNodeMapEntry *e = (HiveNodeMapEntry*) (nodeMap->getNodeMapEntry(partNum));
       e->addScanInfo(HiveScanInfo(this, offset, bytesToRead, hdfsBlockNum, partition, NULL, FALSE));

       // do bookkeeping
       espDistribution[partNum] += bytesToRead;
       totalBytesAssigned += bytesToRead;
     }

   return totalBytesAssigned;
}

void HHDFSFileStats::assignFileToESP(HiveNodeMapEntry*& entry,
                                     const HHDFSPartitionStats* p)
{
   HiveScanInfo info(this, 0, getTotalSize(), -1, p, NULL, TRUE);
   entry->addScanInfo(info);
}

////////////////////////////
// HHDFSSplittableFileStats
////////////////////////////
void HHDFSSplittableFileStats::display() { print(stdout, "file"); }

void 
HHDFSSplittableFileStats::print(FILE *ofd, const char* msg, const char* unit_name)
{
  PrimarySplitUnit su = getSplitUnitType(3);

  fprintf(ofd, "---numOfRows_.entries(): %d---\n", numOfRows_.entries());
  fprintf(ofd, "---offsets_.entries(): %d---\n", offsets_.entries());
  fprintf(ofd, "---totalBytes_.entries(): %d---\n", totalBytes_.entries());
  fprintf(ofd, "---totalRows_: %ld---\n", totalRows_);
  // per unit info
  for(int i = 0; i < numOfRows_.entries(); i++)
  {
      Int32 b = getHDFSBlockNumForSplitUnit(i,su);

      fprintf(ofd, "---%s: %d---\n", unit_name, i);
      fprintf(ofd, "number of rows: %lu\n", numOfRows_[i]);
      fprintf(ofd, "offset: %lu\n", offsets_[i]);
      fprintf(ofd, "bytes: %lu\n", totalBytes_[i]);

      if (b >= 0 && blockHosts_)
        {
          fprintf(ofd, "block (hosts): %d (%d, %d, %d, %d)\n",
                  b,
                  getHostId(0, b),
                  (replication_ >= 2 ? getHostId(1, b) : -1),
                  (replication_ >= 3 ? getHostId(2, b) : -1),
                  (replication_ >= 4 ? getHostId(3, b) : -1));
        }
  }
  HHDFSStatsBase::print(ofd, msg);
}

Int32 HHDFSSplittableFileStats::getNumOFSplitUnits(HHDFSFileStats::PrimarySplitUnit su) const
{
  if (su != SPLIT_AT_FILE_LEVEL_ && offsets_.entries() > 0)
    return offsets_.entries();
  else
    return 1;
}

void HHDFSSplittableFileStats::getSplitUnit(Int32 ix, Int64 &offset, Int64 &length,
                                     HHDFSFileStats::PrimarySplitUnit su) const
{
  if (ix == 0 && su == SPLIT_AT_FILE_LEVEL_)
    {
      // reading entire file, no stripe infos
      offset = 0;
      length = getTotalSize();
    }
  else
    {
      // return split unit ix (also checks for parameter errors)
      offset = offsets_[ix];
      length = totalBytes_[ix];
    }
}

Int32 HHDFSSplittableFileStats::getHDFSBlockNumForSplitUnit(
     Int32 stripeNum,
     HHDFSFileStats::PrimarySplitUnit su) const
{
  if (su == SPLIT_AT_FILE_LEVEL_)
    {
      if (getTotalSize() <= 2*getBlockSize())
        return 0; // use first HDFS block to represent the file
      else
        return -1; // indicate not to attempt locality
    }

  // Try to find an HDFS block that will maximize the benefit of a
  // local scan for this stripe or row group. This method
  // differentiates 3 main cases for a stripe or row group:
  //
  //                                   n-1        n        n+1
  // HDFS blocks:                  +---------+---------+---------+
  // a) stripe is one block                  |<------->|
  // b) stripe contains block(s):          |<-------------->|
  // c) block contains stripe                  |<--->|
  // d) stripe straddles 2 blocks:       |<----->|

  Int64 offset              = offsets_[stripeNum];
  // first block with a starting offset greater or equal the offset of the split unit
  Int32 firstBndryFromLeft  = (offset + getBlockSize() - 1) / getBlockSize();
  // block that contains the byte right after the split unit
  Int32 firstBndryFromRight = (offset + totalBytes_[stripeNum]) / getBlockSize();

  if (firstBndryFromRight > firstBndryFromLeft+2)
    // b) stripe is longer than 2 HDFS blocks, don't use locality
    return -1;

  else if (firstBndryFromLeft < firstBndryFromRight)
    // a or b) stripe contains at least one complete HDFS block,
    //         return the first one of these
    return firstBndryFromLeft;

  else if (firstBndryFromLeft > firstBndryFromRight)
    // c) stripe is fully contained in one HDFS block
    return firstBndryFromRight;

  else // firstBndryFromLeft == firstBndryFromRight
    // d) return the block that contains at least half of the stripe
    //    (note that some cases of b) also get here, if a stripe
    //    ends on a block boundary)
    if ((firstBndryFromLeft*getBlockSize() - offset) >= totalBytes_[stripeNum]/2)
      return firstBndryFromLeft-1;
    else
      return firstBndryFromRight;
}

NABoolean HHDFSSplittableFileStats::splitsOfPrimaryUnitsAllowed(
     HHDFSFileStats::PrimarySplitUnit su) const
{
  // do not split these files any further than the primary
  // units returned by the methods above
  return FALSE;
}

Int64 HHDFSSplittableFileStats::setStripeOrRowgroupInfo(hdfsFileInfo *fileInfo, int numStripesOrRowgroups, 
               jlong *pOffsets, jlong *pLengths, jlong *pNumRows, int numBlocks, jobjectArray j_blockHosts, NABoolean sortHosts)
{
   fileName_       = fileInfo->mName;
   replication_    = (Int32) fileInfo->mReplication;
   totalSize_      = (Int64) fileInfo->mSize;
   blockSize_      = (Int64) fileInfo->mBlockSize;
   modificationTS_ = fileInfo->mLastMod;
   numFiles_       = 1;
   totalRows_      = 0;
   compressionInfo_.setCompressionMethod(fileName_);
   for (int i = 0; i < numStripesOrRowgroups; i++) {
      numOfRows_.insert(pNumRows[i]);
      offsets_.insert(pOffsets[i]);
      totalBytes_.insert(pLengths[i]);
      totalRows_ += pNumRows[i]; 
   }
   numBlocks_ = numBlocks;
   numLocalBlocks_ = 0;
   numSplitUnits_ = numStripesOrRowgroups;
 
   if ((NodeMap::useLocalityForHiveScanInfo() || CURRCONTEXT_CLUSTERINFO->hasVirtualNodes()) 
        && totalSize_ > 0 && (!getTable()->isS3Hosted())) {
      if (CURRCONTEXT_CLUSTERINFO->hasVirtualNodes()) {
         if (CmpCommon::getDefault(HIVE_SIMULATE_REAL_NODEMAP) == DF_ON)
             fakeHostIdsOnVirtualSQNodes();
      }
      blockHosts_ = new(heap_) HostId[replication_*numBlocks_];
      numLocalBlocks_ = HiveClient_JNI::getHostIds(numBlocks_, replication_, j_blockHosts, blockHosts_);
      if (sortHosts)
         sortHostArray(blockHosts_,
                      (Int32) numBlocks_,
                      replication_,
                     getFileName());
   }
   return totalRows_;
}

////////////////////////////
// ORC file
////////////////////////////
THREAD_P Int64 HHDFSORCFileStats::totalAccumulatedRows_ = 0;
THREAD_P Int64 HHDFSORCFileStats::totalAccumulatedTotalSize_ = 0;
THREAD_P Int64 HHDFSORCFileStats::totalAccumulatedStripes_ = 0;
THREAD_P Int64 HHDFSORCFileStats::totalAccumulatedBlocks_ = 0;

HHDFSFileStats::PrimarySplitUnit HHDFSORCFileStats::getSplitUnitType(Int32 balanceLevel) const
{
  if (offsets_.entries() > 0 && balanceLevel >= 0)
    return SPLIT_AT_ORC_STRIPE_LEVEL_;
  else
    return SPLIT_AT_FILE_LEVEL_;
}

void HHDFSORCFileStats::print(FILE *ofd)
{
  fprintf(ofd, ">>>> ORC File:    %s\n", getFileName().data());

  HHDFSSplittableFileStats::print(ofd, "file", "stripe");

  fprintf(ofd, "totalAccumulatedRows: %lu\n", totalAccumulatedRows_);
  fprintf(ofd, "totalAccumulatedTotalSize: %lu\n", totalAccumulatedTotalSize_);
  fprintf(ofd, "totalAccumulatedStripes: %lu\n", totalAccumulatedStripes_);
  fprintf(ofd, "totalAccumulatedBlocks: %lu\n", totalAccumulatedBlocks_);

}

#if 0
// Beware - Please do not delete this important code though it is ifdef-ed out until a year later from 3/8/2019
void HHDFSORCFileStats::populateWithCplus(HHDFSDiags &diags, hdfsFS fs, hdfsFileInfo *fileInfo, NABoolean readStripeInfo, NABoolean readNumRows, NABoolean needToOpenORCI)
{
           //use C++ code to decode ORC stream read from libhdfs.so
           std::unique_ptr<orc::Reader> cppReader ;
           if ( needToOpenORCI ) {
               try{
                 cppReader = orc::createReader(orc::readHDFSFile(fileInfo, fs), orc::ReaderOptions());
               }
               catch(...)
               {
                 diags.recordError(NAString("ORC C++ Reader error: fail to create."));
                 return;
               }
           }
           //NULL pointer
           if(cppReader.get() == 0)
           {
               diags.recordError(NAString("ORC C++ Reader error: null pointer."));
               return;
           }

           if(readStripeInfo) {
               for(int i = 0; i < cppReader->getNumberOfStripes(); i++) {
                   std::unique_ptr<orc::StripeInformation> stripeInfo = cppReader->getStripe(i);
                   numOfRows_.insert(stripeInfo->getNumberOfRows());
                   offsets_.insert(stripeInfo->getOffset());
                   totalBytes_.insert(stripeInfo->getLength());
               }
               numSplitUnits_ = cppReader->getNumberOfStripes();
               totalAccumulatedStripes_ += numSplitUnits_;
           } else {
               if ( totalAccumulatedTotalSize_ > 0 ) {
                   float stripesPerByteRatio = float(totalAccumulatedStripes_) / totalAccumulatedTotalSize_;
                   numSplitUnits_ = totalSize_ * stripesPerByteRatio;
               } else
                   numSplitUnits_ = 1;
           }


           if ( readNumRows || readStripeInfo ) {
                totalRows_ = cppReader->getNumberOfRows();
                totalStringLengths_ = cppReader->getSumStringLengths();
                totalAccumulatedRows_ += totalRows_;
                totalAccumulatedTotalSize_ += totalSize_;
           }
           else {
               if ( totalAccumulatedTotalSize_ > 0 ) {
                   float rowsPerByteRatio =  float(totalAccumulatedRows_) / totalAccumulatedTotalSize_;
                   totalRows_ = totalSize_ * rowsPerByteRatio;
               } else
                   sampledRows_ = 100;
           }
}
#endif


void HHDFSORCFileStats::setOrcStripeInfo(hdfsFileInfo *fileInfo, int numStripesOrRowgroups, 
               jlong *pOffsets, jlong *pLengths, jlong *pNumRows, int numBlocks, jobjectArray j_blockHosts, NABoolean sortHosts)
{
   Int64 totalRows = setStripeOrRowgroupInfo(fileInfo, numStripesOrRowgroups, pOffsets, pLengths, pNumRows, numBlocks,
                                   j_blockHosts, sortHosts);
   totalAccumulatedStripes_ += numStripesOrRowgroups;
   totalAccumulatedRows_ += totalRows;
   totalAccumulatedTotalSize_ += fileInfo->mSize;
   totalAccumulatedBlocks_ += numBlocks;
   if (getTable()->statsLogFile_ != NULL) {
      FILE *ofd = fopen(getTable()->statsLogFile_, "a");
      if (ofd) {
           print(ofd);
           fclose(ofd);
      }
   }
   return;
}

void HHDFSORCFileStats::populateWithJNI(HHDFSDiags &diags, NABoolean readStripeInfo, NABoolean readNumRows, NABoolean needToOpenORCI)
{
   ExpORCinterface* orci = NULL;
   Lng32 rc = 0;

   if ( needToOpenORCI ) {
     orci = ExpORCinterface::newInstance(heap_);
     if (orci == NULL) {
       diags.recordError(NAString("Could not allocate an object of class ExpORCInterface") +
  		       "HHDFSORCFileStats::populate");
       return;
     }

      rc = orci->open(NULL, NULL, (char*)(getFileName().data()), 0, 0, 0);
      if (rc) {
        diags.recordError(NAString("ORC interface open() failed for file ") + getFileName().data());
        return;
      }
   }

   if ( readStripeInfo ) {
      orci->getSplitUnitInfo(numOfRows_, offsets_, totalBytes_);
      numSplitUnits_ = offsets_.entries();
      totalAccumulatedStripes_ += offsets_.entries();
   } else {
      if ( totalAccumulatedTotalSize_ > 0 ) {
         float stripesPerByteRatio = float(totalAccumulatedStripes_) / totalAccumulatedTotalSize_;
         numSplitUnits_ = totalSize_ * stripesPerByteRatio;
      } else
         numSplitUnits_ = 1;
   }

   if ( readNumRows ) {
     NAArray<HbaseStr> *colStats = NULL;
     Lng32 colIndex = -1;
     rc = orci->getColStats(colIndex, NULL, HIVE_UNKNOWN_TYPE, &colStats);

     if (rc) {
       diags.recordError(NAString("ORC interface getColStats() failed"));
       orci->close(); // ignore any error here
       delete orci;
       return;
     }

      // Read the total # of rows
      Lng32 len = 0;
      HbaseStr *hbaseStr = &colStats->at(0);
      ex_assert(hbaseStr->len <= sizeof(totalRows_), "Insufficient length");
      memcpy(&totalRows_, hbaseStr->val, hbaseStr->len);

      totalAccumulatedRows_ += totalRows_;
      totalAccumulatedTotalSize_ += totalSize_;

      // get sum of the lengths of the strings
      Int64 sum = 0;
      rc = orci->getSumStringLengths(sum /* out */);
      if (rc) {
        diags.recordError(NAString("ORC interface getSumStringLengths() failed"));
        orci->close(); // ignore any error here
        delete orci;
        return;
      }
   totalStringLengths_ = sum;

   rc = orci->close();
   if (rc) {
     diags.recordError(NAString("ORC interface close() failed"));
     // fall through to delete orci and to return
   }
      deleteNAArray((NAHeap *)heap_, colStats);
      rc = orci->close();
      if (rc) {
        diags.recordError(NAString("ORC interface close() failed"));
        return;
      }

   } else {
      if ( totalAccumulatedTotalSize_ > 0 ) {
         float rowsPerByteRatio =  float(totalAccumulatedRows_) / totalAccumulatedTotalSize_;
         totalRows_ = totalSize_ * rowsPerByteRatio;
      } else
         sampledRows_ = 100;
   } 

   if ( needToOpenORCI ) {
      rc = orci->close();
      if (rc) {
        diags.recordError(NAString("ORC interface close() failed"));
        return;
      }
      delete orci;
   }
}

void HHDFSORCFileStats::populate(hdfsFS fs,
                hdfsFileInfo *fileInfo,
                Int32& samples,
                HHDFSDiags &diags,
                NABoolean doEstimation,
                char recordTerminator)
{

  // file should be ignored 
  if (!HDFSFileValidate(fileInfo))
    return ;

   // set the 5th argument (doEstimation) to FALSE to avoid estimating
   // # of records for ORC.
   HHDFSFileStats::populate(fs, fileInfo, samples, diags, FALSE, recordTerminator);

   NABoolean readStripeInfo = doEstimation || 
                             (CmpCommon::getDefault(ORC_READ_STRIPE_INFO) == DF_ON);
   NABoolean readNumRows = doEstimation || 
                           (CmpCommon::getDefault(ORC_READ_NUM_ROWS) == DF_ON);

   NABoolean needToOpenORCI = (readStripeInfo || readNumRows );
   populateWithJNI(diags, readStripeInfo, readNumRows, needToOpenORCI);

   // Set sampled # of rows to the actual # of rows.
   samples = totalRows_;
   
   //print log for regression test
   NAString logFile = 
        ActiveSchemaDB()->getDefaults().getValue(ORC_HDFS_STATS_LOG_FILE);
   if (logFile.length()){
       FILE *ofd = fopen(logFile, "a");
       if (ofd){
           print(ofd);
           fclose(ofd);
       }
   }
}

OsimHHDFSStatsBase* HHDFSORCFileStats::osimSnapShot(NAMemory * heap)
{
    OsimHHDFSORCFileStats* stats = new(heap) OsimHHDFSORCFileStats(NULL, this, heap);

    return stats;
}

Lng32 HHDFSTableStats::getAvgStringLengthPerRow()
{
   Int64 estimatedRC = getTotalRows();
   Int64 totalStringLengths = getTotalStringLengths();
   return (estimatedRC > 0) ? Lng32(totalStringLengths / estimatedRC) : 0;
}

////////////////////////////////////////////////////
// For Parquet format
////////////////////////////////////////////////////
THREAD_P Int64 HHDFSParquetFileStats::totalAccumulatedRows_ = 0;
THREAD_P Int64 HHDFSParquetFileStats::totalAccumulatedTotalSize_ = 0;
THREAD_P Int64 HHDFSParquetFileStats::totalAccumulatedRowGroups_ = 0;
THREAD_P Int64 HHDFSParquetFileStats::totalAccumulatedBlocks_ = 0;

void HHDFSParquetFileStats::setParquetRowgroupInfo(hdfsFileInfo *fileInfo, int numRowgroups, 
               jlong *pOffsets, jlong *pLengths, jlong *pNumRows, int numBlocks, jobjectArray j_blockHosts, NABoolean sortHosts)
{
   Int64 totalRows = HHDFSSplittableFileStats::setStripeOrRowgroupInfo(fileInfo, numRowgroups, pOffsets, pLengths, pNumRows, numBlocks,
                                   j_blockHosts, sortHosts);
   totalAccumulatedRowGroups_ += numRowgroups;
   totalAccumulatedRows_ += totalRows;
   totalAccumulatedTotalSize_ += fileInfo->mSize;
   totalAccumulatedBlocks_ += numBlocks;
   if (getTable()->statsLogFile_ != NULL) {
      FILE *ofd = fopen(getTable()->statsLogFile_, "a");
      if (ofd) {
           print(ofd);
           fclose(ofd);
      }
   }
   return;
}

void HHDFSParquetFileStats::print(FILE *ofd)
{
  fprintf(ofd, ">>>> Parquet File:    %s\n", getFileName().data());

  HHDFSSplittableFileStats::print(ofd, "file", "row group");

  fprintf(ofd, "totalAccumulatedRows: %lu\n", totalAccumulatedRows_);
  fprintf(ofd, "totalAccumulatedTotalSize: %lu\n", totalAccumulatedTotalSize_);
  fprintf(ofd, "totalAccumulatedRowGroups: %lu\n", totalAccumulatedRowGroups_);
  fprintf(ofd, "totalAccumulatedBlocks: %lu\n", totalAccumulatedBlocks_);

}

Int64 sum(NAList<Int64>& list)
{
   Int64 total = 0;
   for (Int32 i=0; i<list.entries(); i++ ) {
     total += list[i];
   }
   return total;
}

void HHDFSParquetFileStats::populateWithJNI(char* path, HHDFSDiags &diags, NABoolean readRowGroupInfo, NABoolean readNumRows, NABoolean needToOpenParquetI)
{
   ExpParquetInterface* parqueti = NULL;
   Lng32 rc = 0;

   if ( needToOpenParquetI ) {
                
     parqueti = ExpParquetInterface::newInstance(heap_);
     if (parqueti == NULL) {
       diags.recordError(NAString("Could not allocate an object of class ExpParquetInterface") +
  		       "HHDFSParquetFileStats::populate");
       return;
     }

      rc = parqueti->open(NULL, NULL, (char*)(getFileName().data()), 0, 0, 0);
      if (rc) {
        diags.recordError(NAString("Parquet interface open() failed for file ") + getFileName().data());
	delete parqueti;
        return;
      }
   }

   // totalSize_ is populated in HHDFSFileStats::populate(), a call 
   // made prior to the call to this method.
   totalAccumulatedTotalSize_ += totalSize_;

   NABoolean numRowsRead = FALSE;

   if ( readRowGroupInfo ) {

      parqueti->getSplitUnitInfo(path, numOfRows_, offsets_, totalBytes_);

      numSplitUnits_ = offsets_.entries();

      totalAccumulatedRowGroups_ += offsets_.entries();

      totalRows_ = sum(numOfRows_);
      totalAccumulatedRows_ += totalRows_;

      numRowsRead = TRUE;

   } else {
      if ( totalAccumulatedTotalSize_ > 0 ) {
         float rowGroupsPerByteRatio = float(totalAccumulatedRowGroups_) / totalAccumulatedTotalSize_;
         numSplitUnits_ = totalSize_ * rowGroupsPerByteRatio;
      } else
         numSplitUnits_ = 1;
   }

   if ( !numRowsRead ) {
      if ( readNumRows ) {
        NAArray<HbaseStr> *colStats = NULL;
        Lng32 colIndex = -1;
        rc = parqueti->getColStats(colIndex, (char *)"", HIVE_UNKNOWN_TYPE, &colStats);
   
        if (rc) {
          diags.recordError(NAString("Parquet interface getColStats() failed"));
          parqueti->close(); // ignore any error here
          delete parqueti;
          return;
        }
   
        // Read the total # of rows
        Lng32 len = 0;
        HbaseStr *hbaseStr = &colStats->at(0);
        ex_assert(hbaseStr->len <= sizeof(totalRows_), "Insufficient length");
        memcpy(&totalRows_, hbaseStr->val, hbaseStr->len);
   
        totalAccumulatedRows_ += totalRows_;
   
        deleteNAArray((NAHeap *)heap_, colStats);
   
      } else {
         if ( totalAccumulatedTotalSize_ > 0 ) {
            float rowsPerByteRatio =  float(totalAccumulatedRows_) / totalAccumulatedTotalSize_;
            totalRows_ = totalSize_ * rowsPerByteRatio;
         } else
            sampledRows_ = 100;
      }
   }

   if ( needToOpenParquetI ) {
      rc = parqueti->close();
      if (rc) 
        diags.recordError(NAString("Parquet interface close() failed"));
      delete parqueti;
      return ;
   }
}


void HHDFSParquetFileStats::populate(hdfsFS fs,
                hdfsFileInfo *fileInfo,
                Int32& samples,
                HHDFSDiags &diags,
                NABoolean doEstimation,
                char recordTerminator)
{
  // file should be ignored 
  if (!HDFSFileValidate(fileInfo))
    return ;

   // set the 5th argument (doEstimation) to FALSE to avoid estimating
   // # of records for Parquet.
   HHDFSFileStats::populate(fs, fileInfo, samples, diags, FALSE, recordTerminator);

   NABoolean readRowGroupInfo = doEstimation &&
                           (CmpCommon::getDefault(PARQUET_READ_ROWGROUP_INFO) == DF_ON);
   NABoolean readNumRows = doEstimation &&  
                           (CmpCommon::getDefault(PARQUET_READ_NUM_ROWS) == DF_ON);

   NABoolean needToOpenParquetI = (readRowGroupInfo || readNumRows );

   populateWithJNI(fileInfo->mName, diags, readRowGroupInfo, readNumRows, needToOpenParquetI);

   // Set sampled # of rows to the actual # of rows.
   samples = totalRows_;

   //print log for regression test
   NAString logFile = 
        ActiveSchemaDB()->getDefaults().getValue(PARQUET_HDFS_STATS_LOG_FILE);
   if (logFile.length()){
       FILE *ofd = fopen(logFile, "a");
       if (ofd){
           print(ofd);
           fclose(ofd);
       }
   }
}

OsimHHDFSStatsBase* HHDFSParquetFileStats::osimSnapShot(NAMemory * heap)
{
    OsimHHDFSParquetFileStats* stats = 
        new(heap) OsimHHDFSParquetFileStats(NULL, this, heap);
    return stats;
}

HHDFSFileStats::PrimarySplitUnit HHDFSParquetFileStats::getSplitUnitType(Int32 balanceLevel) const
{
  if (offsets_.entries() > 0 && balanceLevel >= 0)
    return SPLIT_AT_PARQUET_ROWGROUP_LEVEL_;
  else
    return SPLIT_AT_FILE_LEVEL_;
}

FileStatsCache::FileStatsCache(NAMemory* heap)
:  NAHashDictionary<NAString, NAString>(
                   hashKey, 
                   NAHashDictionary_Default_Size, 
                   FALSE, heap),
   populated_(FALSE)
{
}

FileStatsCache::~FileStatsCache() {}

void FileStatsCache::populateCache(
				   hive_tbl_desc *htd,
				   HHDFSDiags &diags
                                  )
{
   Lng32 rc = 0;
                
   ExpParquetInterface* parqueti = NULL;
   parqueti = ExpParquetInterface::newInstance(getHeap());

   if (parqueti == NULL) {
       diags.recordError(NAString("Could not allocate ExpParquetInterface in HHDFSParquetFileStats::populateCache()"));
       return;
   }

   NAArray<HbaseStr>* statsData = NULL;
   hive_sd_desc *hsd = htd->getSDs();
   const char * tableDir = hsd->location_;
   hive_sd_desc *root_hsd = hsd;
   

   rc = parqueti->getFileAndBlockAttributes((char *)tableDir, &statsData);

   if ( rc ) {
       char msg[1024];
       snprintf(msg, sizeof(msg),
                "getFileAndBlockAttributes() returns %d .Possibly a file in directory %s is not of type Parquet or trafodion id does not have read access to a file in this directory", rc, tableDir);
       diags.recordError(NAString(msg));
       delete parqueti;
       return;
   }

   const char* delim = "|";
   NABoolean allowSubdirs = 
    (CmpCommon::getDefault(HIVE_SUPPORTS_SUBDIRECTORIES) == DF_ON);
   
   for (Int32 i=0; i<statsData->entries(); i++ ) {
      HbaseStr& str = (*statsData)[i];

      NAString path;
      NAString name; // not used
      NAString type; // not used
      long modTime;  // not used
      if ( !determineObject(str.val, path, name, type, modTime, diags) ) {
	delete parqueti;
	return;
      }
      if (allowSubdirs) {
	if (htd->getNumOfPartCols() == 0) {
	  if (path.contains(tableDir))
	    path = tableDir;
	  else {
	    char msg[1024];
	    snprintf(msg, sizeof(msg),
		     "Tabledir %s is not a prefix of path %s returned by getFileAndBlockAttributes() ", tableDir, path.data());
	    diags.recordError(NAString(msg));
	    return;
	  }
	}
	else if (i > 0) { // partition of a partitioned table
	  NABoolean found = FALSE;
	  hsd = root_hsd;
	  while(hsd->next_ && !found) {
	    hsd = hsd->next_;
	    if (path.contains(hsd->location_)) {
	      path = hsd->location_;
	      found = TRUE;
	    }
	  }
	  // if there is an orphan file that is not under a valid partition dir.
	  // (for example, an unregistered partition), then we wairs no error,
	  // and simply add to it cache with invalid dir, which will cause it
	  // to be ignored.
	}
      } // end of allowSubdirs

 //cout << "indicator=" << type.data() << ", path=" << path.data() << ", value=" << str.val << endl;

      // put the file in the same bucket as its dir. 
      // key = path
      // value = the file stats string.
      NAString* ret = 
         insert(new (getHeap()) NAString(path), 
                new (getHeap()) NAString(str.val));

      if ( !ret )  {
         diags.recordError("Error to insert a stats entry during FileStatsCache::populateCache()");
	 delete parqueti;
         return;
      }
   }

   populated_ = TRUE;
   delete parqueti;
}


NABoolean HHDFSFileStats::checkExtractedString(char* extractedStr, const char* msg, char* sourceStr, HHDFSDiags& diags)
{
  if ( !extractedStr ) {
    char msg[300];
    snprintf(msg, sizeof(msg),
             "In HHDFSFileStats::populate(str, delim, diags), fail to extract %s from data string %s",
             msg, sourceStr);
    diags.recordError(msg);
    return FALSE;
  }
  return TRUE;
}

#define RETURN_IF_NOT_OK(x) \
    if (!x) { return; }

// The str will be modified on exit
void HHDFSFileStats::populate(char* str, const char* delim, HHDFSDiags &diags)
{
  char* indicator = strtok(str, delim); // indicator

  if ( strcmp(indicator, "FileStats") )
   {
       char msg[100];
       snprintf(msg, sizeof(msg),
                "An incorrect indicator string %s is detected", indicator);
       diags.recordError(NAString(msg) + "in HHDFSFileStats::populate()");
       return;
   }

  char* path = strtok(NULL, delim);          // path

  RETURN_IF_NOT_OK(checkExtractedString(path, "file path", str, diags));

  NAString decodedPath;
  if ( !base64Decoder.decode(path, strlen(path), decodedPath) ) {
    char msg[100];
    snprintf(msg, sizeof(msg),
             "Fail to base64 decode file path %s in HHDFSFileStats::populate()",
             path);
    diags.recordError(msg);
    return;
  }

  char* name = strtok(NULL, delim);          // name

  RETURN_IF_NOT_OK(checkExtractedString(name, "file name", str, diags));

  NAString decodedName;
  if ( !base64Decoder.decode(name, strlen(name), decodedName) ) {
    char msg[100];
    snprintf(msg, sizeof(msg),
             "Fail to base64 decode file name %s in HHDFSFileStats::populate()",
             name);
    diags.recordError(msg);
    return;
  }

  const NAClusterInfo *clusterInfo = CURRCONTEXT_CLUSTERINFO;
  char* length = strtok(NULL, delim);        // length
  RETURN_IF_NOT_OK(checkExtractedString(length, "file length", str, diags));

  char* accessTime = strtok(NULL, delim);    // access time
  RETURN_IF_NOT_OK(checkExtractedString(accessTime, "accessTime", str, diags));

  char* modTime = strtok(NULL, delim);       // modification time
  RETURN_IF_NOT_OK(checkExtractedString(modTime, "modTime", str, diags));

  char* blockSize = strtok(NULL, delim);     // block size
  RETURN_IF_NOT_OK(checkExtractedString(blockSize, "blockSize", str, diags));

  char* repFactor = strtok(NULL, delim);     // replication factor
  RETURN_IF_NOT_OK(checkExtractedString(repFactor, "repFactor", str, diags));

  char* blocks = strtok(NULL, delim);        // # of blocks
  RETURN_IF_NOT_OK(checkExtractedString(blocks, "blocks", str, diags));
   

   // fields for HHDFSStatsBase

  numBlocks_ = atoi(blocks);
  numFiles_ = 1;
  totalRows_ = 0;
  numSplitUnits_ = 0;
  totalStringLengths_ = 0;
  totalSize_ = atol(length);

  modificationTS_ = atol(modTime);

  sampledBytes_ = 0;
  sampledRows_ = 0;

  numLocalBlocks_ = 0; // blocks on nodes of the Trafodion cluster

   // fields for HHDFSFileStats
  fileName_ = decodedPath + "/" + decodedName;
  replication_ = atoi(repFactor);
  blockSize_ = atoi(blockSize);

  compressionInfo_.setCompressionMethod(fileName_);


  if ( (
        clusterInfo->hasVirtualNodes() ||
        NodeMap::useLocalityForHiveScanInfo() 
       )
        && !getTable()->isS3Hosted()			//S3 blocks are never on cluster, so skip collecting block locality for this case
		&&
       totalSize_ > 0 && diags.isSuccess())
    {

      NABoolean sortHosts = 
         (CmpCommon::getDefault(HIVE_SORT_HDFS_HOSTS) == DF_ON);

      blockHosts_ = new(heap_) HostId[replication_*numBlocks_];


      // Walk through a block string and record their host locations.
      // The block string is delim separated host name substrings 
      // like "adev04.esgyn.com". The total number of such substrings is
      // numBlocks_ * replicator_. The 1st replicator_ substrings are for
      // block #1; and the ith set of replicator substrings are for the ith
      // block. 
      for (Int64 blockNum=0; blockNum < numBlocks_ && diags.isSuccess(); blockNum++)
        {

          HostId hostId;
          int blockIsOnThisCluster = 0;

          for (Int32 r=0; r<replication_; r++)
            {
              char* hostName = strtok(NULL, delim);

              if (hostName)
                {
                  NAString host(hostName);

                  // NAClusterInfo stores only the unqualified
                  // first part of the host name, so do the same
                  // with the host name we got from HDFS
                  size_t pos = host.index('.');

                  if (pos != NA_NPOS)
                     host.remove(pos);

                  Lng32 nodeNum = clusterInfo->mapNodeNameToNodeNum(host);

                  if (nodeNum >= 0)
                    {
                      hostId = nodeNum;
                      blockIsOnThisCluster = 1;
                    }
                  else
                    hostId = InvalidHostId;
                }
                else
                    hostId = InvalidHostId;

                blockHosts_[r*numBlocks_+blockNum] = hostId;
            }

            if (sortHosts)
                sortHostArray(blockHosts_,
                              (Int32) numBlocks_,
                              replication_,
                              getFileName());

            numLocalBlocks_ += blockIsOnThisCluster;
        }
    }
}

// The str will be modified on exit
void HHDFSSplittableFileStats::populate(char* str, const char* delim, HHDFSDiags &diags)
{
   HHDFSFileStats::populate(str, delim, diags);

   char* numRowGroups = strtok(NULL, delim);
   if (!numRowGroups)
   {
     numSplitUnits_ = 0;
     totalStringLengths_ = 0;
     totalRows_ = 0;
     return ;
   }
   numSplitUnits_ = atol(numRowGroups);

   for (Int64 i=0; i<numSplitUnits_; i++ )
    {
      char* compressedSize = strtok(NULL, delim);         
      char* totalSize = strtok(NULL, delim);         
      char* rowCount = strtok(NULL, delim);         
      char* offset = strtok(NULL, delim);         

      totalBytes_.insert(atol(totalSize));
      numOfRows_.insert(atol(rowCount));
      offsets_.insert(atol(offset));
   }
      
   char* sumOfStringLength = strtok(NULL, delim);         
      
   if ( sumOfStringLength ) {
      totalStringLengths_ = atoll(sumOfStringLength);
   }

   totalRows_ = sum(numOfRows_);
}


void HHDFSParquetFileStats::populate(char* str, const char* delim, HHDFSDiags &diags)
{
  HHDFSSplittableFileStats::populate(str, delim, diags);

  //print log for regression test
  NAString logFile = 
    ActiveSchemaDB()->getDefaults().getValue(PARQUET_HDFS_STATS_LOG_FILE);

  if (logFile.length()){
    FILE *ofd = fopen(logFile, "a");
    if (ofd){
      print(ofd);
      fclose(ofd);
    }
  }
}

const char* HHDFSTableStats::getBinaryPartColValues(CollIndex i) const 
{ 
   if ( binaryPartColValues_.getUsage(i) != UNUSED_COLL_ENTRY )
      return binaryPartColValues_[i]; 

   return NULL;
}

void HHDFSTableStats::setBinaryPartColValues(CollIndex i, char* v, Lng32 len)
{ 
   char* buf = new (heap_) char[len];
   memcpy(buf, v, len);
   binaryPartColValues_.insertAt(i, buf);
}

