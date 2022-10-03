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
**************************************************************************
*
* File:         NATable.C
* Description:  A Non-Alcoholic table
* Created:      4/27/94
* Language:     C++
*
*
**************************************************************************
*/

#define   SQLPARSERGLOBALS_FLAGS	// must precede all #include's

#undef  _DP2NT_
#define _DP2NT_
#define __ROSETTA
#undef _DP2NT_
#undef __ROSETTA

#include "NATable.h"
#include "Sqlcomp.h"
#include "Const.h"
#include "dfs2rec.h"
#include "hs_read.h"
#include "parser.h"
#include "BindWA.h"
#include "ComAnsiNamePart.h"
#include "ItemColRef.h"
#include "ItemFunc.h"
#include "ItemOther.h"
#include "PartFunc.h"
#include "EncodedValue.h"
#include "SchemaDB.h"
#include "NAClusterInfo.h"
#include "MVInfo.h"
#include "ComMPLoc.h"
#include "NATable.h"
#include "opt.h"
#include "CmpStatement.h"
#include "ControlDB.h"
#include "ComCextdecs.h"
#include "ComSysUtils.h"
#include "ComObjectName.h"
#include "ComMisc.h"
#include "SequenceGeneratorAttributes.h"
#include "security/uid.h"

#include "ComCextdecs.h"
#include "ExpHbaseInterface.h"
#include "CmpSeabaseDDL.h"
#include "RelScan.h"
#include "exp_clause_derived.h"
#include "PrivMgrCommands.h"
#include "ComDistribution.h"
#include "ExExeUtilCli.h"
#include "CmpDescribe.h"
#include "Globals.h"
#include "ComUser.h"
#include "ComSmallDefs.h"
#include "CmpMain.h"
#include "CompException.h"
#include "TrafDDLdesc.h"
#include "CmpSeabaseDDL.h"
#include "ComEncryption.h"
#include "SharedCache.h"
#include "NamedSemaphore.h"

#define MAX_NODE_NAME 9

#include "SqlParserGlobals.h"
#include "HBaseClient_JNI.h"


//#define __ROSETTA
//#include "rosetta_ddl_include.h"


#include "SqlParserGlobals.h"
extern TrafDesc *generateSpecialDesc(const CorrName& corrName);

#include "CmpMemoryMonitor.h"

#include "OptimizerSimulator.h"

#include "SQLCLIdev.h"
#include "sql_id.h"
#include "ComRtUtils.h"
#include "Context.h"
#include "TriggerDB.h"



SQLMODULE_ID __SQL_mod_natable = {
  /* version */         SQLCLI_CURRENT_VERSION,
  /* module name */     "HP_SYSTEM_CATALOG.SYSTEM_SCHEMA.READDEF_N29_000",
  /* time stamp */      866668761818000LL,
  /* char set */        "ISO88591",
  /* name length */     47 
};

static NABoolean createNAPartitions(TrafDesc *partition_desc,
                                    Int32 partCnt,
                                    LIST(NAString) &colnameAry, 
                                    NAPartitionArray &partitions,
                                    NAMemory *heap);

// -----------------------------------------------------------------------
// skipLeadingBlanks()
// Examines the given string keyValueBuffer from 'startIndex' for
// 'length' bytes and skips any blanks that appear as a prefix of the
// first non-blank character.
// -----------------------------------------------------------------------

Int64 HistogramsCacheEntry::getLastUpdateStatsTime()
{
  return cmpCurrentContext->getLastUpdateStatsTime();
}

void HistogramsCacheEntry::setUpdateStatsTime(Int64 updateTime)
{
  cmpCurrentContext->setLastUpdateStatsTime(updateTime);
}

/*
static Int64 getCurrentTime()
{
  // GETTIMEOFDAY returns -1, in case of an error
  Int64 currentTime;
  TimeVal currTime;
  if (GETTIMEOFDAY(&currTime, 0) != -1)
    currentTime = currTime.tv_sec;
  else
    currentTime = 0;
  return currentTime;
}
*/

void HistogramsCacheEntry::updateRefreshTime()
{
  Int64 currentTime = getCurrentTime();
  this->setRefreshTime(currentTime);
}

static Lng32 skipLeadingBlanks(const char * keyValueBuffer,
			      const Lng32 startIndex,
			      const Lng32 length)
{
  Lng32 newIndex = startIndex;
  Lng32 stopIndex = newIndex + length;
  // Localize the search for blanks between the startIndex and stopIndex.
  while ((newIndex <= stopIndex) AND (keyValueBuffer[newIndex] == ' '))
    newIndex++;
  return newIndex;
} // static skipLeadingBlanks()

// -----------------------------------------------------------------------
// skipTrailingBlanks()
// Examines the given string keyValueBuffer from startIndex down through
// 0 and skips any blanks that appear as a suffix of the first non-blank
// character.
// -----------------------------------------------------------------------
static Lng32 skipTrailingBlanks(const char * keyValueBuffer,
			       const Lng32 startIndex)
{
  Lng32 newIndex = startIndex;
  while ((newIndex >= 0) AND (keyValueBuffer[newIndex] == ' '))
    newIndex--;
  return newIndex;
} // static skipTrailingBlanks

//----------------------------------------------------------------------
// qualNameHashFunc()
// calculates a hash value given a QualifiedName.Hash value is mod by
// the hashTable size in HashDictionary.
//----------------------------------------------------------------------
ULng32 qualNameHashFunc(const QualifiedName& qualName)
{
  ULng32 index = 0;
  const NAString& name = qualName.getObjectName();
  
  for(UInt32 i=0;i<name.length();i++)
    {
      index += (ULng32) (name[i]);
    }
  return index;
}

//-------------------------------------------------------------------------
//constructor() for HistogramCache
//-------------------------------------------------------------------------
HistogramCache::HistogramCache(NAMemory * heap,Lng32 initSize)
: heap_(heap),
  lastTouchTime_(getCurrentTime()),
  hits_(0),
  lookups_(0),
  memoryLimit_(33554432),
  lruQ_(heap), tfd_(NULL), mfd_(NULL), size_(0)
{
	//create the actual cache
  HashFunctionPtr hashFunc = (HashFunctionPtr)(&qualNameHashFunc);
  histogramsCache_ = new (heap_) 
    NAHashDictionary<QualifiedName,HistogramsCacheEntry>
    (hashFunc,initSize,TRUE,heap_);
}

//reset all entries to not accessedInCurrentStatement
void HistogramCache::resetAfterStatement()
{
  for (CollIndex x=lruQ_.entries(); x>0; x--)
  {
    if (lruQ_[x-1]->accessedInCurrentStatement())
      lruQ_[x-1]->resetAfterStatement();
  }
}

//-------------------------------------------------------------------------
//invalidate what is in the cache
//-------------------------------------------------------------------------
void HistogramCache::invalidateCache()
{
  while (lruQ_.entries())
  {
    HistogramsCacheEntry* entry = lruQ_[0];
    deCache(&entry);
  }
  histogramsCache_->clearAndDestroy();
  lruQ_.clear();
  lastTouchTime_ = getCurrentTime();
}

//--------------------------------------------------------------------------
// HistogramCache::getCachedHistogram()
// Looks for the histogram in the cache if it is there then makes a deep copy
// of it on the statementHeap() and returns it. If the histogram is not in
// the cache then it fetches the histogram and makes a deep copy of it on the
// context heap to store it in the hash table.
//--------------------------------------------------------------------------

void HistogramCache::getHistograms(NATable& table, 
                                   NABoolean useStoredStats)
{
  const QualifiedName& qualifiedName = table.getFullyQualifiedGuardianName();
  ExtendedQualName::SpecialTableType type = table.getTableType();
  const NAColumnArray& colArray = table.getNAColumnArray();
  StatsList& colStatsList = *(table.getColStats());
  const Int64& redefTime = table.getRedefTime();
  Int64& statsTime = const_cast<Int64&>(table.getStatsTime());
  Int64 tableUID = table.objectUid().castToInt64();
  Int64 siKeyGCinterval = CURRSTMT_OPTDEFAULTS->siKeyGCinterval();

  // Fail safe logic: Make sure that we haven't been idle longer than
  // the RMS security invalidation garbage collection logic. If we have,
  // it is possible that an invalidation key for stats has been missed.
  // So to be safe, we invalidate the whole cache.
  if (getCurrentTime() > lastTouchTime_ + siKeyGCinterval)
    invalidateCache();

  //1//
  //This 'flag' is set to NULL if FetchHistogram has to be called to
  //get the statistics in case
  //1. If a table's histograms are not in the cache
  //2. If some kind of timestamp mismatch occurs and therefore the
  //   cached histogram has to be refreshed from disk.
  //Pointer to cache entry for histograms on this table
  HistogramsCacheEntry * cachedHistograms = NULL;

  //Do we need to use the cache
  //Depends on :
  //1. If histogram caching is ON
  //2. If the table is a normal table
  if(CURRSTMT_OPTDEFAULTS->cacheHistograms() &&
    type == ExtendedQualName::NORMAL_TABLE)
  { //2//
    // Do we have cached histograms for this table
    // look up the cache and get a reference to statistics for this table
    cachedHistograms = lookUp(table);

    // (Possibly useless) sanity tests

    // Check to see if the redefinition timestamp has changed. This seems
    // to be always stubbed to zero today on Trafodion, so this check
    // seems to never fail.

    if (cachedHistograms && (cachedHistograms->getRedefTime() != redefTime))
    {
      deCache(&cachedHistograms);
    }

    // Check to see if the table's objectUID has changed (indicating that
    // it has been dropped and recreated). This test shouldn't fail, because
    // a drop should have caused the NATable object to be invalidated via
    // the query invalidation infrastructure which should have already caused
    // the histograms to be decached.

    if (cachedHistograms && (cachedHistograms->getTableUID() != tableUID))
    {
      deCache(&cachedHistograms);
    }

    //check if histogram preFetching is on
    if(CURRSTMT_OPTDEFAULTS->preFetchHistograms() && cachedHistograms)
    { //3//
      //we do need to preFetch histograms
      if(!cachedHistograms->preFetched())
      { //4//
        //preFetching is on but these histograms
        //were not preFetched so delete them and
        //re-Read them
        deCache(&cachedHistograms);
     } //4//
    } //3//
  } //2//

  if( cachedHistograms )
  {
    hits_++;
  }
  else
  {
    lookups_++;
  }

#ifndef NDEBUG
  // this code figures out if our table is a table of interest (that is, not
  // a metadata table or histograms table)
  const NAString & xSchemaName = qualifiedName.getSchemaName();
  const NAString & xObjectName = qualifiedName.getObjectName();
  bool debugStop = false;
  if ((xSchemaName != "_MD_") &&
      (xSchemaName != "_PRIVMGR_MD_") &&
      (xSchemaName != "_TENANT_MD_") &&
      (xObjectName != "SB_HISTOGRAMS") &&
      (xObjectName != "SB_HISTOGRAM_INTERVALS") &&
      (xObjectName != "SB_PERSISTENT_SAMPLES"))
    debugStop = true;  // put debug stop here to skip metadata tables
#endif  // NDEBUG

  //retrieve the statistics for the table in colStatsList
  createColStatsList(table, cachedHistograms, useStoredStats);

  //if not using histogram cache, then invalidate cache
  if(!CURRSTMT_OPTDEFAULTS->cacheHistograms())
    invalidateCache();

  lastTouchTime_ = getCurrentTime();
} //1//
  
//----------------------------------------------------------------------------
// HistogramCache::createColStatsList()
// This method actually puts the statistics for columns that require statistics
// into colStatsList.
// 1. If reRead is false meaning that the table's statistics exist in the cache,
//    then this method gets statistics from the cache and copies them into
//    colStatsList. If statistics for some columns are not found in the cache, then
//    this method calls FetchHistograms to get statistics for these columns. It
//    then puts these missing statistics into the cache, then copies the statistics
//    from the cache into colStatsList
// 2. If reRead is true meaning that we need to get statistics from disk via
//    FetchHistograms. reRead can be true for any of the following cases:
//    a. Histogram Caching is on but we updated statistics since we last read them
//       so we have deleted the old statistics and we need to read the tables
//       statistics again from disk.
// 3. If histograms are being Fetched on demand meaning that histogram caching is off,
//    then this method will fetch statistics into colStatsList using FetchHistograms.
//
// Now that we also have the option of reducing the number of intervals in histograms
// this method also factors that in.
//
// Each entry of the colArray contains information about a column that tells
// us what kind of histograms is required by that colum. The decision on what
// kind of a histograms is required for a column is base on the following factors
//
// 1. A column that is not referenced and neither is a index/primary key does
//    not need histogram
//
// 2. Column that is a index/primary key or is referenced in the query but not part
//    of a predicate or groupby or orderby clause requires compressed histogram.
//    A full histogram can be altered to make it seem like a compressed histogram.
//
// 3. Columns that are part of a predicate or are in orderby or groupby clause requires
//    full histogram are referencedForHistogram. A full histogram can only satisfy
//    the requirement for a full histogram.
//
// Just to for the sake of reitirating the main point:
// Columns that are referencedForHistogram needs full histogram
// Columns that are just referenced or is a index/primary key only requires a
// compressed histogram
//----------------------------------------------------------------------------
void HistogramCache::createColStatsList
(NATable& table, HistogramsCacheEntry* cachedHistograms, 
 NABoolean useStoredStats)
{
  StatsList& colStatsList = *(table.getColStats());

  NAColumnArray& colArray = const_cast<NAColumnArray&>
    (table.getNAColumnArray());
  const QualifiedName& qualifiedName = table.getFullyQualifiedGuardianName();
  ExtendedQualName::SpecialTableType type = table.getTableType();
  const Int64& redefTime = table.getRedefTime();
  Int64& statsTime = const_cast<Int64&>(table.getStatsTime());

  // The singleColsFound is used to prevent stats from being inserted
  // more than once in the output list.
  ColumnSet singleColsFound(STMTHEAP);

  //"lean" cachedHistograms/are in the context heap.
  //colStatsList is in the statement heap. 
  //The context heap persists for the life of this mxcmp.
  //The statement heap is deleted at end of a compilation.
  //getStatsListFromCache will expand "lean" cachedHistograms
  //into "fat" colStatsList.

  //this points to the stats list
  //that is used to fetch statistics
  //that are not in the cache
  StatsList * statsListForFetch=NULL;

  // Used to count the number of columns
  // whose histograms are in the cache.
  UInt32 coveredList = 0;

  //Do we need to use the cache
  //Depends on :
  //1. If histogram caching is ON
  //2. If the table is a normal table
  if(cachedHistograms && (CURRSTMT_OPTDEFAULTS->cacheHistograms() &&
		          type == ExtendedQualName::NORMAL_TABLE))
  {
    // getStatsListFromCache will unmark columns that have statistics 
    // in cachedHistograms. All columns whose statistics are not in
    // cachedHistogram are still marked as needing histograms. 
    // This is then passed into FetchHistograms, which will
    // return statistics for columns marked as needing histograms. 
    // colArray tells getStatsListFromCache what columns need 
    // histograms. getStatsListFromCache uses colArray to tell
    // us what columns were not found in cachedHistograms.

    // get statistics from cachedHistograms into list.
    // colArray has the columns whose histograms we need.
    coveredList = getStatsListFromCache
      (colStatsList, colArray, table.interestingExpressions(), 
       cachedHistograms, singleColsFound);
   }

  // set to TRUE if all columns in the table have default statistics
  NABoolean allFakeStats = TRUE;

  //if some of the needed statistics were not found in the cache
  //then call FetchHistograms to get those statistics
  // TODO: revise this test for expressions histograms
  if (colArray.entries() > coveredList)
  {
     //this is the stats list into which statistics will be fetched
     statsListForFetch = &colStatsList;

     if(CURRSTMT_OPTDEFAULTS->cacheHistograms() &&
        type == ExtendedQualName::NORMAL_TABLE)
     {
       //if histogram caching is on and not all histograms where found in the cache
       //then create a new stats list object to get histograms that were missing
       statsListForFetch = new(CmpCommon::statementHeap()) 
               StatsList(CmpCommon::statementHeap(),2*colArray.entries());
     }

     //set pre-fetching to false by default
     NABoolean preFetch = FALSE;

     //turn prefetching on if caching is on and
     //we want to prefetch histograms
     if(CURRSTMT_OPTDEFAULTS->cacheHistograms() &&
        CURRSTMT_OPTDEFAULTS->preFetchHistograms() &&
		   (type == ExtendedQualName::NORMAL_TABLE))
      preFetch = TRUE;

     // flag the unique columns so the uec can be set correctly
     // specially in the case of columns with fake stats
     for (CollIndex j = 0; j < colArray.entries(); j++)
     {
        NAList<NAString> keyColList(STMTHEAP, 1);
        NAColumn *col = colArray[j];
        if (!col->isUnique())
        {
           const NAString &colName = col->getColName();
           keyColList.insert(colName);
 
           // is there a unique index on this column?
           if (col->needHistogram () && 
               table.getCorrespondingIndex(keyColList,  // input columns
                                           TRUE,        // look for explicit index
                                           TRUE,        // look for unique index
                                           FALSE,        // look for primary key
                                           FALSE,       // look for any index or primary key
                                           FALSE,       // sequence of cols doesn't matter
                                           FALSE,        // exclude computed cols
                                           NULL         // index name
                                           ))
              col->setIsUnique(); 
        }
     }

     FetchHistograms(qualifiedName,
                     type,
                     (colArray),
                     table.interestingExpressions(),
                     (*statsListForFetch),
                     FALSE,
                     CmpCommon::statementHeap(),
                     statsTime,
                     allFakeStats,//set to TRUE if all columns have default stats
                     preFetch,
                     (Int64) CURRSTMT_OPTDEFAULTS->histDefaultSampleSize(),
                     (table.isORC() || table.isParquet()),
                     (useStoredStats ? (TrafDesc *)table.getStoredStatsDesc() : NULL)
                    );  
  
  }

  //check if we are using the cache
  if(CURRSTMT_OPTDEFAULTS->cacheHistograms() &&
     type == ExtendedQualName::NORMAL_TABLE)
  {
      //we are using the cache but did we already
      //have the statistics in cache
      if(cachedHistograms)
      {
        // yes some of the statistics where already in cache
        // Did we find statistics in the cache for all the columns
        // whose statistics we needed?
        if (colArray.entries() > coveredList)
        {
          // not all the required statistics were in the cache,
          // some statistics were missing from the cache entry.
          // therefore must have done a FetchHistograms to get
          // the missing histograms. Now update the cache entry
          // by adding the missing histograms that were just fetched
          ULng32 histCacheHeapSize = heap_->getAllocSize();
          cachedHistograms->addToCachedEntry(colArray,(*statsListForFetch));
          ULng32 entrySizeGrowth = (heap_->getAllocSize() - histCacheHeapSize);
          ULng32 entrySize = cachedHistograms->getSize() + entrySizeGrowth;
          cachedHistograms->setSize(entrySize);
          size_ += entrySizeGrowth;
  
          //get statistics from the cache that where missing from the 
          //cache earlier and have since been added to the cache
          coveredList = getStatsListFromCache
            (colStatsList, colArray, table.interestingExpressions(),
             cachedHistograms, singleColsFound);
        }
      }
      else
      {
        CMPASSERT(statsListForFetch);

        // used the cache but had to re-read
        // all the table's histograms from disk
  
        // put the re-read histograms into cache
        putStatsListIntoCache((*statsListForFetch), colArray, qualifiedName,
                             table.objectUid().castToInt64(),
                             statsTime, redefTime, allFakeStats);
  
        // look up the cache and get a reference to statistics for this table
        cachedHistograms = lookUp(table);


        // get statistics from the cache
        coveredList = getStatsListFromCache
          (colStatsList, colArray, table.interestingExpressions(),
           cachedHistograms, singleColsFound);
      }
  }


  if(CURRSTMT_OPTDEFAULTS->reduceBaseHistograms())
    colStatsList.reduceNumHistIntsAfterFetch(table);

    //clean up
	if(statsListForFetch != &colStatsList)
		delete statsListForFetch;

  // try to decache any old entries if we're over the memory limit
  if(CURRSTMT_OPTDEFAULTS->cacheHistograms())
  {
    enforceMemorySpaceConstraints();
  }
  traceTable(table);
}

//------------------------------------------------------------------------
//HistogramCache::getStatsListFromCache()
//gets the StatsList into list from cachedHistograms and
//returns the number of columns whose statistics were
//found in the cache. The columns whose statistics are required
//are passed in through colArray. The expressions whose statistics
//are desired are passed in through interestingExpressions.
//------------------------------------------------------------------------
Int32 HistogramCache::getStatsListFromCache
( StatsList&            list, //In \ Out
  NAColumnArray&        colArray, //In
  NAHashDictionary<NAString, NABoolean> & interestingExpressions, // In
  HistogramsCacheEntry* cachedHistograms, // In
  ColumnSet&            singleColsFound) //In \ Out
{
  // cachedHistograms points to the memory-efficient contextheap 
  // representation of table's histograms. 
  // list points to statementheap list container that caller is 
  // expecting us to fill-in with ColStats required by colArray.

  // counts columns whose histograms are in cache or not needed
  UInt32 columnsCovered = 0;

  // Collect the mc stats with this temporary list. If the 
  // mc stats objects are stored in the middle of the output 'list', 
  // IndexDescHistograms::appendHistogramForColumnPosition() will
  // abort, because "There must be a ColStatDesc for every key column!".
  StatsList mcStatsList(CmpCommon::statementHeap());

    //iterate over all the columns in the colArray
  for(UInt32 i=0;i<colArray.entries();i++)
	{
		//get a reference to the column
    NAColumn * column = colArray[i];

		//get the position of the column in the table
    CollIndex colPos = column->getPosition();

    // singleColsFound is used to prevent stats from
    // being inserted more than once in the output list.
    if (singleColsFound.contains(colPos))
    {
      columnsCovered++;
      continue;
    }

    NABoolean columnNeedsHist = column->needHistogram();
    NABoolean columnNeedsFullHist = column->needFullHistogram();

    // Did histograms for this column get added
    NABoolean colAdded = FALSE;

    if (NOT columnNeedsHist)
    {
      //if the column was marked as not needing any histogram
      //then increment columnsCovered & skip to next column, as neither 
      //single interval nor full histograms are required for this column.
      columnsCovered++;
    }
    else if (cachedHistograms->contains(colPos) AND columnNeedsHist)
		{
			//we have full histograms for this column
      columnsCovered++;
      colAdded = TRUE;

      //set flag in column not to fetch histogram
      //the histogram is already in cache
      if (NOT columnNeedsFullHist)
        column->setDontNeedHistogram();

      NABoolean copyIntervals=TRUE;
      ColStatsSharedPtr const singleColStats =
        cachedHistograms->getHistForCol(*column);
      if (NOT columnNeedsFullHist)
      {
        //full histograms are not required. get single interval histogram
        //from the full histogram and insert it into the user's statslist 
        copyIntervals=FALSE;
      }
      //since we've tested containment, we are guaranteed to get a
      //non-null histogram for column
      list.insertAt
        (list.entries(), 
         ColStats::deepCopySingleColHistFromCache
         (*singleColStats, *column, list.heap(), copyIntervals));
    }
    //Assumption: a multi-column histogram is retrieved when 
    //histograms for any of its columns are retrieved.
    if (columnNeedsHist)
    {
      // insert all multicolumns referencing column
      // use singleColsFound to avoid duplicates
      cachedHistograms->getMCStatsForColFromCacheIntoList
        (mcStatsList, *column, singleColsFound);
    }
	
    // if column was added, then add it to the duplist
    if (colAdded) singleColsFound += colPos;
  }

  // append any desired expressions histograms
  if (interestingExpressions.entries() > 0)
  {
    // for each expression histogram in cachedHistograms
    // look it up in interesting expressions
    // if found, do a list.insertAt(list.entries(), ...)
    cachedHistograms->getDesiredExpressionsStatsIntoList(list,
      interestingExpressions);
  }  

  // append the mc stats at the end of the output lit.
  for (Lng32 i=0; i<mcStatsList.entries(); i++ ) {
     list.insertAt(list.entries(), mcStatsList[i]);
  }

  return columnsCovered;
}

//this method is used to put into the cache stats lists, that
//needed to be re-read or were not there in the cache
void HistogramCache::putStatsListIntoCache(StatsList & colStatsList,
                                          const NAColumnArray& colArray,
                                          const QualifiedName & qualifiedName,
                                          Int64 tableUID,
                                          Int64 statsTime,
                                          const Int64 & redefTime,
					  NABoolean allFakeStats)
{
  ULng32 histCacheHeapSize = heap_->getAllocSize();
  // create memory efficient representation of colStatsList
  HistogramsCacheEntry * histogramsForCache = new (heap_) 
    HistogramsCacheEntry(colStatsList, qualifiedName, tableUID,
                         statsTime, redefTime, heap_);
  ULng32 cacheEntrySize = heap_->getAllocSize() - histCacheHeapSize;

  if(CmpCommon::getDefault(CACHE_HISTOGRAMS_CHECK_FOR_LEAKS) == DF_ON)
  {
    delete histogramsForCache;
    ULng32 histCacheHeapSize2 = heap_->getAllocSize();
    CMPASSERT( histCacheHeapSize == histCacheHeapSize2);
    histogramsForCache = new (heap_) 
      HistogramsCacheEntry(colStatsList, qualifiedName, tableUID, 
                           statsTime, redefTime, heap_);
    cacheEntrySize = heap_->getAllocSize() - histCacheHeapSize2;
  }
  histogramsForCache->setSize(cacheEntrySize);

  // add it to the cache 
  QualifiedName* key = const_cast<QualifiedName*>
    (histogramsForCache->getName());
  QualifiedName *name = histogramsCache_->insert(key, histogramsForCache);
  if (name)
  {
    // append it to least recently used queue
    lruQ_.insertAt(lruQ_.entries(), histogramsForCache);
  }
  size_ += cacheEntrySize;
}

// if we're above memoryLimit_, try to decache
NABoolean HistogramCache::enforceMemorySpaceConstraints()
{
  if (size_ <= memoryLimit_)
    return TRUE;

  HistogramsCacheEntry* entry = NULL;
  while (lruQ_.entries())
  {
    entry = lruQ_[0];
    if (entry->accessedInCurrentStatement())
      return FALSE;
    deCache(&entry);
    if (size_ <= memoryLimit_)
      return TRUE;
  }
  return FALSE;
}

// lookup given table's histograms.
// if found, return its HistogramsCacheEntry*.
// otherwise, return NULL.
HistogramsCacheEntry* HistogramCache::lookUp(NATable& table)
{
  const QualifiedName& tblNam = table.getFullyQualifiedGuardianName();
  HistogramsCacheEntry* hcEntry = NULL;
  if (histogramsCache_) 
  {
    // lookup given table's lean histogram cache entry
    hcEntry = histogramsCache_->getFirstValue(&tblNam);
    if (hcEntry)
    {
      // move entry to tail of least recently used queue
      lruQ_.remove(hcEntry);
      lruQ_.insertAt(lruQ_.entries(), hcEntry);
    }
  }
  return hcEntry;
}

// decache entry
void HistogramCache::deCache(HistogramsCacheEntry** entry)
{
  if (entry && (*entry))
  {
    ULng32 entrySize = (*entry)->getSize();
    histogramsCache_->remove(const_cast<QualifiedName*>((*entry)->getName()));
    lruQ_.remove(*entry);
    ULng32 heapSizeBeforeDelete = heap_->getAllocSize();
    delete (*entry);
    ULng32 memReclaimed = heapSizeBeforeDelete - heap_->getAllocSize();
    if(CmpCommon::getDefault(CACHE_HISTOGRAMS_CHECK_FOR_LEAKS) == DF_ON)
      CMPASSERT( memReclaimed >= entrySize );
    *entry = NULL;
    size_ -= entrySize;
  }
}


void HistogramCache::resizeCache(size_t limit)
{
  memoryLimit_ = limit;
  enforceMemorySpaceConstraints();
}

ULng32 HistogramCache::entries() const
{
  return histogramsCache_ ? histogramsCache_->entries() : 0;
}

void HistogramCache::display() const
{
  HistogramCache::print();
}

void 
HistogramCache::print(FILE *ofd, const char* indent, const char* title) const
{
#ifndef NDEBUG
  BUMP_INDENT(indent);
  fprintf(ofd,"%s%s\n",NEW_INDENT,title);
  fprintf(ofd,"entries: %d \n", entries());
  fprintf(ofd,"size: %d bytes\n", size_);
  for (CollIndex x=lruQ_.entries(); x>0; x--)
  {
    lruQ_[x-1]->print(ofd, indent, "HistogramCacheEntry");
  }
#endif
}

void HistogramCache::traceTable(NATable& table) const
{
  if (tfd_)
  {
    NAString tableName(table.getTableName().getQualifiedNameAsString());
    fprintf(tfd_,"table:%s\n",tableName.data());
    table.getColStats()->trace(tfd_, &table);
    fflush(tfd_);
  }
}

void HistogramCache::traceTablesFinalize() const
{
  if (tfd_)
  {
    fprintf(tfd_,"cache_size:%d\n", size_);
    fprintf(tfd_,"cache_heap_size:" PFSZ "\n", heap_->getAllocSize());
    fflush(tfd_);
  }
}

void HistogramCache::closeTraceFile() 
{
  if (tfd_) fclose(tfd_);
  tfd_ = NULL;
}

void HistogramCache::openTraceFile(const char *filename) 
{
  tfd_ = fopen(filename, "w+");
}

void HistogramCache::closeMonitorFile() 
{
  if (mfd_) fclose(mfd_);
  mfd_ = NULL;
}

void HistogramCache::openMonitorFile(const char *filename) 
{
  mfd_ = fopen(filename, "w+");
}

void HistogramCache::monitor() const
{
  // if histogram caching is off, there's nothing to monitor
  if(!OptDefaults::cacheHistograms()) return;

  if (mfd_)
  {
    for (CollIndex x=lruQ_.entries(); x>0; x--)
    {
      lruQ_[x-1]->monitor(mfd_);
    }
    if (CmpCommon::getDefault(CACHE_HISTOGRAMS_MONITOR_MEM_DETAIL) == DF_ON)
    {
      fprintf(mfd_,"cache_size:%d\n", size_);
      fprintf(mfd_,"cache_heap_size:" PFSZ "\n", heap_->getAllocSize());
    }
    fflush(mfd_);
  }
}

void HistogramCache::freeInvalidEntries(Int32 numKeys,
                                        SQL_QIKEY * qiKeyArray)
{
  // an empty set for qiCheckForInvalidObject call
  ComSecurityKeySet dummy(CmpCommon::statementHeap());
  // create an iterator that will iterate over the whole cache
  NAHashDictionaryIterator<QualifiedName, HistogramsCacheEntry> it(*histogramsCache_);
  QualifiedName * qn = NULL;
  HistogramsCacheEntry * entry = NULL;
  
  for (int i = 0; i < it.entries(); i++)
  {
    it.getNext(qn,entry);
    if (qiCheckForInvalidObject(numKeys, qiKeyArray,
                                entry->getTableUID(),
                                dummy))
      deCache(&entry);
  }

  lastTouchTime_ = getCurrentTime();
}


// constructor for memory efficient representation of colStats.
// colStats has both single-column & multi-column histograms.
HistogramsCacheEntry::HistogramsCacheEntry
(const StatsList & colStats,
 const QualifiedName & qualifiedName,
 Int64 tableUID,
 const Int64 & statsTime,
 const Int64 & redefTime,
 NAMemory * heap) 
  : full_(NULL), multiColumn_(NULL), expressions_(NULL), name_(NULL), heap_(heap)
  , refreshTime_(0), singleColumnPositions_(heap), tableUID_(tableUID)
  , accessedInCurrentStatement_(TRUE)
  , size_(0)
{
  statsTime_ = statsTime;
  updateRefreshTime();
  redefTime_ = redefTime;
  preFetched_ = CURRSTMT_OPTDEFAULTS->preFetchHistograms();
  allFakeStats_ = colStats.allFakeStats();

  // make a deep copy of the key. 
  // qualifiedName is short-lived (from stmtheap).
  // name_ is longer-lived (from contextheap).
  name_ = new(heap_) QualifiedName(qualifiedName, heap_);

  // create pointers to full single-column histograms (include fake)
  UInt32 singleColumnCount = colStats.getSingleColumnCount(); 
  if (singleColumnCount > 0)
  {
    full_ = new(heap_) NAList<ColStatsSharedPtr>(heap_, singleColumnCount);

    // fill-in pointers to deep copy of single-column histograms
    for(UInt32 i=0; i<colStats.entries();i++)
    {
      const NAColumnArray& colArray = colStats[i]->getStatColumns();
      Lng32 position = (Lng32)colArray.getColumn(Lng32(0))->getPosition();
      if ((colArray.entries() == 1) && (position >= 0))
      {
        // keep pointer to deep copy of single-column histogram
        full_->insertAt(full_->entries(),
                        ColStats::deepCopyHistIntoCache(*(colStats[i]),heap_));

        // update singleColumnPositions
        singleColumnPositions_ += 
          (Lng32)colArray.getColumn(Lng32(0))->getPosition();
      }
    }
  }

  // create pointers to expressions histograms
  UInt32 expressionCount = colStats.getExpressionsCount();
  if (expressionCount > 0)
  {
    expressions_ = new(heap_) NAList<ColStatsSharedPtr>(heap_, expressionCount);

    // fill-in pointers to deep copy of single-column histograms
    for(UInt32 i=0; i<colStats.entries();i++)
    {
      const NAColumnArray& colArray = colStats[i]->getStatColumns();
      Lng32 position = (Lng32)colArray.getColumn(Lng32(0))->getPosition();
      if ((colArray.entries() == 1) && (position < 0))
      {
        // Keep pointer to deep copy of expressions histogram.
        // Note that we call ColStats::deepCopy directly, so we can
        // tell it to copy the NAColumnArray rather than rely on
        // column positions. We need the NAColumnArray for expression
        // histograms since the column name in its only NAColumn member
        // contains the expression text.
        expressions_->insertAt(expressions_->entries(),
          ColStats::deepCopy(*(colStats[i]),heap_,FALSE /* that is, copy NAColumnArray */));
      }
    }
  }

  // create pointers to multi-column histograms
  multiColumn_ = new(heap_) MultiColumnHistogramList(heap_);

  // add deep copy of multi-column histograms (but, avoid duplicates)
  multiColumn_->addMultiColumnHistograms(colStats);
}

// insertDeepCopyIntoCache adds histograms of the sametype
// (single-column and/or multi-column) to this cache entry
void 
HistogramsCacheEntry::addToCachedEntry
(NAColumnArray & columns, StatsList & list)
{
  // update allFakeStats_
  if (allFakeStats_)
    allFakeStats_ = list.allFakeStats();

		//iterate over all the colstats in the stats list passed in
  ColumnSet singleColHistAdded(heap_);
		
  for(UInt32 j=0;j<list.entries();j++)
  {
    //get the columns for the current colstats
    NAColumnArray colList = list[j]->getStatColumns();
    
		//get the first column for the columns represented by
		//the current colstats
		NAColumn * column = colList.getColumn(Lng32(0));
    
    //column position of first column
    Lng32 currentColPosition = column->getPosition();
    
    //check if current column requires full histograms
    NABoolean requiresHistogram = column->needHistogram();
    
    //check if current colstats is a single-column histogram
    NABoolean singleColHist = (colList.entries()==1? TRUE: FALSE);
    NABoolean mcForHbasePart = list[j]->isMCforHbasePartitioning ();

    //only fullHistograms are inserted in full_.
    //We also add fake histograms to the cache.
    //This will help us not to call FetchHistograms
    //for a column that has fake statistics. 
    //Previously we did not cache statistics for
    //columns that did not have statistics in the histograms tables 
    //(FetchHistogram faked statistics for such column). 
    //Since statistics for such columns were not found in the
    //cache we had to repeatedly call FetchHistogram 
    //to get statistics for these columns
    //instead of just getting the fake statistics from the cache. 
    //FetchHistograms always return fake statistics for such columns 
    //so why not just cache them and not call FetchHistograms. 
    //When statistics are added for these columns then the timestamp
    //matching code will realize that and 
    //re-read the statistics for the table again.
    if((requiresHistogram || NOT singleColHist)|| list[j]->isFakeHistogram())
    {
        //if single column Histograms
        //if((singleColHist || mcForHbasePart) && (!singleColumnPositions_.contains(currentColPosition)))
        if((singleColHist) && (!singleColumnPositions_.contains(currentColPosition)))
			{
			  //Current colstats represent a single column histogram
			  //Insert the colstats from the stats list passed in, at the end of
			  //this objects stats list (represented by colStats_).
        full_->insertAt(full_->entries(),
                        ColStats::deepCopyHistIntoCache(*(list[j]),heap_));
        
        singleColHistAdded += currentColPosition;
      }
      else if (NOT singleColHist)
      {
        //Assumption: a multi-column histogram is retrieved when 
        //histograms for any of its columns are retrieved.
        //e.g. Table T1(a int, b int, c int)
        //histograms: {a},{b},{c},{a,b},{a,c},{b,c},{a,b,c}
        //If histograms for column a are fetched we will get 
        //histograms: {a}, {a,b}, {a,c}, {a,b,c}
        //If histograms for column b are fetched we will get
        //histograms: {b}, {a,b}, {b,c}, {a,b,c}
        //Therefore to avoid duplicated multicolumn stats being inserted
        //we pass down the list of single columns for which we have stats

			  //Current colstats represent a multicolumn histogram
        addMultiColumnHistogram(*(list[j]), &singleColumnPositions_);
      }
    }
  }
  singleColumnPositions_ += singleColHistAdded;
}

// add multi-column histogram to this cache entry
void 
HistogramsCacheEntry::addMultiColumnHistogram
(const ColStats& mcStat, ColumnSet* singleColPositions)
{
  if (!multiColumn_)
    multiColumn_ = new(heap_) MultiColumnHistogramList(heap_);

  multiColumn_->addMultiColumnHistogram(mcStat, singleColPositions);
}

const QualifiedName* 
HistogramsCacheEntry::getName() const
{
  return name_;
}

const ColStatsSharedPtr 
HistogramsCacheEntry::getStatsAt(CollIndex x) const
{
  if (!full_ OR x > full_->entries())
    return NULL;
  else
    return full_->at(x);
}

const MultiColumnHistogram*
HistogramsCacheEntry::getMultiColumnAt(CollIndex x) const
{
  if (!multiColumn_ OR x > multiColumn_->entries())
    return NULL;
  else
    return multiColumn_->at(x);
}

// return pointer to full single-column histogram identified by col
ColStatsSharedPtr const
HistogramsCacheEntry::getHistForCol (NAColumn& col) const
{
  if (!full_) return NULL;

  // search for colPos in full_
  for(UInt32 i=0; i < full_->entries(); i++)
  {
    // have we found colPos?
    if (((*full_)[i]->getStatColumnPositions().entries() == 1) AND
        (*full_)[i]->getStatColumnPositions().contains(col.getPosition()))
    {
      return (*full_)[i];
    }
  }
  return NULL;
}
  
// insert all multicolumns referencing col into list
// use singleColsFound to avoid duplicates
void
HistogramsCacheEntry::getMCStatsForColFromCacheIntoList
(StatsList& list, // out: "fat" rep of multi-column stats for col
 NAColumn&  col,  // in:  column whose multi-column stats we want
 ColumnSet& singleColsFound) // in: columns whose single-column 
  //stats have already been processed by caller.
  //Assumption: a multi-column histogram is retrieved when 
  //histograms for any of its columns are retrieved.
{
  CollIndex multiColCount = multiColumnCount();
  if (multiColCount <= 0) return; // entry has no multicolumn stats

  // search entry's multicolumn stats for col
  NAMemory* heap = list.heap();
  for(UInt32 i=0; i<multiColCount; i++)
  {
    const MultiColumnHistogram* mcHist = getMultiColumnAt(i);
    if (mcHist)
    {
      ColumnSet mcCols(mcHist->cols(), STMTHEAP);
      if (!mcCols.contains(col.getPosition())) 
        continue; // no col

      if ((mcCols.intersectSet(singleColsFound)).entries()) 
        continue; // avoid dup
 
      // create "fat" representation of multi-column histogram

      ColStatsSharedPtr mcStat;
      if (col.getNATable()->isHbaseTable() && col.isPrimaryKey()) {

        // For mcStats covering a key column of a HBASE table, 
        // create a colStat object with multi-intervals, which will
        // be useful in allowing better stats-based split.
        mcStat = new (STMTHEAP) ColStats(*(mcHist->getColStatsPtr()), 
                                         STMTHEAP, TRUE);

      } else {

        ComUID id(mcHist->id());
        CostScalar uec(mcHist->uec());
        CostScalar rows(mcHist->rows());

        mcStat = new (STMTHEAP) ColStats 
          (id, uec, rows, rows, FALSE, FALSE, NULL, FALSE,
           1.0, 1.0, 0, STMTHEAP, FALSE);
      
        // populate its NAColumnArray with mcCols
        (*mcStat).populateColumnArray(mcHist->cols(), col.getNATable());

        // set up its histogram interval
        HistogramSharedPtr histogram = new(STMTHEAP) Histogram(heap);
        HistInt loInt;
        NABoolean boundaryInclusive = TRUE;
        HistInt hiInt(1, NULL, (*mcStat).statColumns(), 
                      rows, uec, boundaryInclusive, 0);
        histogram->insert(loInt);
        histogram->insert(hiInt);
        mcStat->setHistogram(histogram);

        MCSkewedValueList * mcSkewedValueList = new (STMTHEAP) MCSkewedValueList (*(mcHist->getMCSkewedValueList()), STMTHEAP);
        mcStat->setMCSkewedValueList(*mcSkewedValueList);
      }

      // append to list the mcStat
      list.insertAt(list.entries(), mcStat);
    }
  }
}

// insert all the desired expressions histograms into list
void HistogramsCacheEntry::getDesiredExpressionsStatsIntoList(StatsList& list,
  NAHashDictionary<NAString, NABoolean> & interestingExpressions)
{
  if (expressions_)
  {
    for (CollIndex i = 0; i < expressions_->entries(); i++)
    {
      // obtain the expression text for the histogram (this is in
      // the column name of the first and only NAColumn object in
      // its NAColumnArray)
      ColStatsSharedPtr const expressionStats = expressions_->at(i);
      const NAColumnArray & colArray = expressionStats->getStatColumns();
      const NAColumn * nac = colArray[0];
      const NAString & expressionText = nac->getColName();

      // if this histogram is an interesting expression, copy it
      if (interestingExpressions.contains(&expressionText))
      {      
        list.insertAt(list.entries(), 
          ColStats::deepCopy(*expressionStats, list.heap())); 
      }
    }
  }
}

//destructor
HistogramsCacheEntry::~HistogramsCacheEntry()
{
  if(full_)
  {
    ColStatsSharedPtr colStat = NULL;
    while(full_->getFirst(colStat))
    {
      colStat->deepDeleteFromHistogramCache();

      //colStats is a shared pointer
      //and will not be deleted till
      //ref count goes to zero
      //Therefore to avoid leaks and
      //ensure colStats is deleted we
      //do the following
      ColStats * colStatPtr = colStat.get();
      colStat.reset();
      delete colStatPtr;
    }
    delete full_;
  }
  if(expressions_)
  {
    ColStatsSharedPtr colStat = NULL;
    while(expressions_->getFirst(colStat))
    {
      colStat->deepDeleteFromHistogramCache();

      //colStats is a shared pointer
      //and will not be deleted till
      //ref count goes to zero
      //Therefore to avoid leaks and
      //ensure colStats is deleted we
      //do the following
      ColStats * colStatPtr = colStat.get();
      colStat.reset();
      delete colStatPtr;
    }
    delete expressions_;
  }
  if(multiColumn_)
    delete multiColumn_;
  if(name_)
    delete name_;
  singleColumnPositions_.clear();
}

void HistogramsCacheEntry::display() const
{
  HistogramsCacheEntry::print();
}

void HistogramsCacheEntry::monitor(FILE* mfd) const
{
  NAString tableName(name_->getQualifiedNameAsString());
  fprintf(mfd,"table:%s\n",tableName.data());
  if (CmpCommon::getDefault(CACHE_HISTOGRAMS_MONITOR_HIST_DETAIL) == DF_ON)
  {
    if (full_)
    {
      for (CollIndex x=0; x<full_->entries(); x++)
      {
        full_->at(x)->trace(mfd, NULL);
      }
    }
    if (multiColumn_)
    {
      multiColumn_->print(mfd, NULL);
    }
  }
  if (CmpCommon::getDefault(CACHE_HISTOGRAMS_MONITOR_MEM_DETAIL) == DF_ON)
    fprintf(mfd,"table_size:%d\n",size_);
  fflush(mfd);
}

void HistogramsCacheEntry::print
(FILE *ofd, const char* indent, const char* title) const
{
#ifndef NDEBUG
  BUMP_INDENT(indent);
  fprintf(ofd,"%s%s\n",NEW_INDENT,title);
  name_->print(ofd);
  fprintf(ofd,"accessedInCurrentStatement_:%d ", accessedInCurrentStatement_);
  fprintf(ofd,"allFakeStats_:%d ", allFakeStats_);
  fprintf(ofd,"preFetched_:%d \n", preFetched_);
  char time[30];
  convertInt64ToAscii(redefTime_, time);
  fprintf(ofd,"redefTime_:%s ", time);
  convertInt64ToAscii(refreshTime_, time);
  fprintf(ofd,"refreshTime_:%s ", time);
  convertInt64ToAscii(statsTime_, time);
  fprintf(ofd,"statsTime_:%s ", time);
  convertInt64ToAscii(getLastUpdateStatsTime(), time);
  fprintf(ofd,"lastUpdateStatsTime:%s \n", time);
  convertInt64ToAscii(tableUID_, time);
  fprintf(ofd,"tableUID_:%s \n", time);
  fprintf(ofd,"single-column histograms:%d ", singleColumnCount());
  singleColumnPositions_.printColsFromTable(ofd,NULL);
  if (full_)
  {
    for (CollIndex x=0; x<full_->entries(); x++)
    {
      full_->at(x)->print(ofd);
    }
  }
  fprintf(ofd,"multi-column histograms:%d ", multiColumnCount());
  if (multiColumn_)
  {
    multiColumn_->print(ofd);
  }
  fprintf(ofd,"expressions histograms:%d ", expressionsCount());
  if (expressions_)
  {
    for (CollIndex x=0; x<expressions_->entries(); x++)
    {
      expressions_->at(x)->print(ofd);
    }
  }
#endif
}

// -----------------------------------------------------------------------
// getRangePartitionBoundaryValues()
// This method receives a string within which the partitioning key values
// appear in a comma-separated sequence. It returns an ItemExprList that
// contains ConstValue expressions for representing each partitioning
// key value as shown below:
//
//                                   ------      ------      ------
// "<value1>, <value2>, <value3>" => |    | ---> |    | ---> |    |
//                                   ------      ------      ------
//                                     |           |           |
//                                     v           v           v
//                                 ConstValue  ConstValue  ConstValue
//                                 (<value1>)  (<value2>)  (<value3>)
//
// -----------------------------------------------------------------------
ItemExpr* NATable::getRangePartitionBoundaryValues
                        (const char * keyValueBuffer,
                         const Lng32   keyValueBufferSize,
			 NAMemory* heap,
                         CharInfo::CharSet strCharSet)
{
  char * keyValue;             // the string for the key value
  ItemExpr * partKeyValue;     // -> dynamically allocated expression
  Lng32 length;                 // index to the next key value and its length
  Lng32 startIndex = 0;
  Lng32 stopIndex = keyValueBufferSize-1;

  startIndex = skipLeadingBlanks(keyValueBuffer, startIndex, stopIndex);
  // Skip leading '('
  NABoolean leadingParen = FALSE;
  if (keyValueBuffer[startIndex] == '(')
    {
      leadingParen = TRUE;
      startIndex++;
    }

  stopIndex = skipTrailingBlanks(&keyValueBuffer[startIndex], stopIndex);
  // Skip trailing ')' only if there was a leading paren. This
  // is the case where the value comes in as (<value>)
  if ((keyValueBuffer[stopIndex] == ')') &&
      (leadingParen == TRUE))
    stopIndex--;

  length = stopIndex - startIndex + 1;

  NAString keyValueString( &keyValueBuffer[startIndex], (size_t) length );

  // ---------------------------------------------------------------------
  // Copy the string from the keyValueBuffer into a string that
  // is terminated by a semicolon and a null.
  // ---------------------------------------------------------------------
  keyValue = new (heap) char[length + 1 /* for semicolon */ + 1 /* for eol */ ];
  // strncpy( keyValue, keyValueString.data(), (size_t) length );
  //soln:10-031112-1256
  // strncpy replaced with memcpy to handle columns of the partition?s first key value is
  // NULL character within double-quote  eg:( ?\0? ).  i.e (  "( "6666673"  , "\0" ,  8060928 )").

  memcpy(keyValue, (char *)( keyValueString.data() ), (size_t) length );
  keyValue[length] = ';';
  keyValue[length+1] = '\0';

  // ---------------------------------------------------------------------
  // Create a new ItemExprList using the parse tree generated from the
  // string of comma-separated literals.
  // ---------------------------------------------------------------------
  Parser parser(CmpCommon::context());
  //partKeyValue = parser.getItemExprTree(keyValue);
  partKeyValue = parser.getItemExprTree(keyValue,length+1,strCharSet);
  // Check to see if the key values parsed successfully.
  if(partKeyValue == NULL) {
    return NULL;
  }

  return partKeyValue->copyTree(heap);

} // static getRangePartitionBoundaryValues()

// In some cases we don't have a text representation of the start keys,
// only the encoded keys (e.g. from HBase regions start keys). In this
// case, un-encode these binary values and form ConstValues from them.
static ItemExpr * getRangePartitionBoundaryValuesFromEncodedKeys(
             const NAColumnArray & partColArray,
             const char * encodedKey,
             const Lng32  encodedKeyLen,
             NAMemory*    heap)
{
  Lng32 keyColOffset = 0;
  ItemExpr *result = NULL;
  char *actEncodedKey = (char *) encodedKey; // original key or a copy
  const char* encodedKeyP = NULL;
  char* varCharstr = NULL;
  Lng32 totalKeyLength = 0;
  Lng32 numProvidedCols = 0;
  Lng32 lenOfFullyProvidedCols = 0;
  Lng32 vcHdrExtraBytes = 0;
  Lng32 tmpCurrOffset = 0;

  // in newer HBase versions, the region start key may be shorter than an actual key
  for (CollIndex i = 0; i < partColArray.entries(); i++)
    {
      const NAType *pkType = partColArray[i]->getType();
      Lng32 colEncodedLength = pkType->getSQLnullHdrSize() + pkType->getNominalSize();

      totalKeyLength += colEncodedLength;
      if (totalKeyLength <= encodedKeyLen)
        {
          // this column is fully provided in the region start key
          numProvidedCols++;
          lenOfFullyProvidedCols = totalKeyLength;
          tmpCurrOffset = lenOfFullyProvidedCols;
        }
      else //this column is partitially covered or totally missing
        {
          if (tmpCurrOffset >= encodedKeyLen && //totally missing column
              pkType->getTypeName() == "VARCHAR") //and varchar type
            {
              vcHdrExtraBytes += pkType->getVarLenHdrSize();
              tmpCurrOffset += pkType->getVarLenHdrSize();
            }
          tmpCurrOffset += colEncodedLength;
        }
    }

  if (encodedKeyLen < totalKeyLength)
    {
      // the provided key does not cover all the key columns

      // need to extend the partial buffer, allocate a copy
      actEncodedKey = new(heap) char[totalKeyLength + vcHdrExtraBytes];
      memcpy(actEncodedKey, encodedKey, encodedKeyLen);

      // extend the remainder with zeroes, assuming that this is what
      // HBase does when deciding which region a row belongs to
      memset(&actEncodedKey[encodedKeyLen], 0, totalKeyLength-encodedKeyLen+vcHdrExtraBytes);
      Lng32 currOffset = lenOfFullyProvidedCols;

      // go through the partially or completely missing columns and make something up
      // so that we can treat the buffer as fully encoded in the final loop below
      for (CollIndex j = numProvidedCols; j < partColArray.entries(); j++)
        {
          const NAType *pkType        = partColArray[j]->getType();
          Lng32 nullHdrSize           = pkType->getSQLnullHdrSize();
          int valOffset               = currOffset + nullHdrSize;
          int valEncodedLength        = pkType->getNominalSize();
          Lng32 colEncodedLength      = pkType->getTotalSize();
          NABoolean isDescending      = (partColArray[j]->getClusteringKeyOrdering() == DESCENDING);

          NABoolean nullHdrAlreadySet = FALSE;
          NABoolean columnIsPartiallyProvided = (currOffset < encodedKeyLen);

          if (columnIsPartiallyProvided)
            {
              // This column is partially provided, try to make sure that it has a valid
              // value. Note that the buffer has a prefix of some bytes with actual key
              // values, followed by bytes that are zeroed out. 

              // the number of bytes actually provided in the key (not filled in)
              int numBytesInProvidedVal = encodedKeyLen-valOffset;

              if (nullHdrSize && numBytesInProvidedVal <= 0)
                {
                  // only the null-header or a part thereof was provided
                  CMPASSERT(nullHdrSize == sizeof(short));

                  // get the partial indicator values into a short
                  short indicatorVal = *reinterpret_cast<short *>(&actEncodedKey[currOffset]);

                  // make it either 0 or -1
                  if (indicatorVal)
                    indicatorVal = -1;

                  // put it back and let the code below know that we set it already
                  // (this is handled otherwise as a non-provided column)
                  memcpy(&actEncodedKey[currOffset], &indicatorVal, sizeof(indicatorVal));
                  nullHdrAlreadySet = TRUE;
                  columnIsPartiallyProvided = FALSE;
                }

              // Next, decide by data type whether it's ok for the type to have
              // a suffix of the buffer zeroed out (descending columns will
              // see 0xFF values, once the encoded value gets inverted). If the
              // type can't take it or we are not quite sure, we'll just discard
              // all the partial information. Note that this could potentially
              // lead to two partition boundaries with the same key, and also
              // to partition boundaries that don't reflect the actual region
              // boundaries.

              if (columnIsPartiallyProvided)
                switch (pkType->getTypeQualifier())
                  {
                  case NA_NUMERIC_TYPE:
                    {
                      NumericType *nt = (NumericType *) pkType;

                      if (!nt->isExact() || nt->isDecimal() || nt->isBigNum() ||
                          (isDescending && nt->decimalPrecision()))
                        // we may be able to improve this in the future
                        columnIsPartiallyProvided = FALSE;
                    }
                    break;

                  case NA_DATETIME_TYPE:
                    // those types should tolerate zeroing out trailing bytes, but
                    // not filling with 0xFF
                    if (isDescending)
                      columnIsPartiallyProvided = FALSE;
                    else if (numBytesInProvidedVal < 4 &&
                             static_cast<const DatetimeType *>(pkType)->getSubtype() !=
                             DatetimeType::SUBTYPE_SQLTime)
                      {
                        // avoid filling year, month, day with a zero,
                        // convert those to a 1

                        // sorry, this makes use of the knowledge that
                        // encoded date and timestamp all start with
                        // 4 bytes, 2 byte big-endian year, 1 byte
                        // month, 1 byte day

                        if (actEncodedKey[valOffset] != 0 && numBytesInProvidedVal == 1)
                          // only 1 byte of a year > 255 provided, in
                          // this case leave the second byte zero,
                          // only change the second byte to 1 if the
                          // first byte is also zero
                          numBytesInProvidedVal = 2;

                        // set bytes 1, 2, 3 (year LSB, month, day) to
                        // 1 if they are not provided and zero
                        for (int ymd=numBytesInProvidedVal; ymd<4; ymd++)
                          if (actEncodedKey[valOffset+ymd] == 0)
                            actEncodedKey[valOffset+ymd] = 1;
                      }
                    break;

                  case NA_INTERVAL_TYPE:
                    // those types should tolerate zeroing out trailing bytes, but
                    // not filling with 0xFF
                    if (isDescending)
                      columnIsPartiallyProvided = FALSE;
                    break;

                  case NA_CHARACTER_TYPE:
                    // generally, character types should also tolerate zeroing out
                    // trailing bytes, but we might need to clean up characters
                    // that got split in the middle
                    {
                      CharInfo::CharSet cs = pkType->getCharSet();

                      switch (cs)
                        {
                        case CharInfo::UCS2:
                          // For now just accept partial characters, it's probably ok
                          // since they are just used as a key. May look funny in EXPLAIN.
                          break;
                        case CharInfo::UTF8:
                          {
                            // temporarily invert the provided key so it is actual UTF8
                            if (isDescending)
                              for (int i=0; i<numBytesInProvidedVal; i++)
                                actEncodedKey[valOffset+i] = ~actEncodedKey[valOffset+i];

                            CMPASSERT(numBytesInProvidedVal > 0);

                            // remove a trailing partial character, if needed
                            int validLen = lightValidateUTF8Str(&actEncodedKey[valOffset],
                                                                numBytesInProvidedVal);

                            // replace the remainder of the buffer with UTF8 min/max chars
                            fillWithMinMaxUTF8Chars(&actEncodedKey[valOffset+validLen],
                                                    valEncodedLength - validLen,
                                                    0,
                                                    isDescending);

                            // limit to the max # of UTF-8characters, if needed
                            if (pkType->getPrecisionOrMaxNumChars() > 0)
                              {
                                // this time validate the # of chars (likely to be more,
                                // since we filled to the end with non-blanks)
                                validLen = lightValidateUTF8Str(&actEncodedKey[valOffset],
                                                                valEncodedLength,
                                                                pkType->getPrecisionOrMaxNumChars());

                                if (validLen > 0)
                                  // space after valid #chars is filled with blanks
                                  memset(&actEncodedKey[valOffset+validLen], ' ', valEncodedLength-validLen);
                                else
                                  columnIsPartiallyProvided = FALSE;
                              }

                            // undo the inversion, if needed, now for the whole key
                            if (isDescending)
                              for (int k=0; k<valEncodedLength; k++)
                                actEncodedKey[valOffset+k] = ~actEncodedKey[valOffset+k];
                          }
                          break;
                        case CharInfo::ISO88591:
                          // filling with 0x00 or oxFF should both be ok
                          break;

                        default:
                          // don't accept partial keys for other charsets
                          columnIsPartiallyProvided = FALSE;
                          break;
                        }
                    }
                    break;

                  default:
                    // don't accept partial keys for any other data types
                    columnIsPartiallyProvided = FALSE;
                    break;
                  }

              if (columnIsPartiallyProvided)
                {
                  // a CQD can suppress, give errors, warnings or enable partially provided cols
                  DefaultToken tok = CmpCommon::getDefault(HBASE_RANGE_PARTITIONING_PARTIAL_COLS);

                  switch (tok)
                    {
                    case DF_OFF:
                      // disable use of partial columns
                      // (use this as a workaround if they cause problems)
                      columnIsPartiallyProvided = FALSE;
                      break;

                    case DF_MINIMUM:
                      // give an error (again, this is probably mostly used as a
                      // workaround or to detect past problems)
                      *CmpCommon::diags() << DgSqlCode(-1212) << DgInt0(j);
                      break;

                    case DF_MEDIUM:
                      // give a warning, could be used for searching or testing
                      *CmpCommon::diags() << DgSqlCode(+1212) << DgInt0(j);
                      break;

                    case DF_ON:
                    case DF_MAXIMUM:
                    default:
                      // allow it, no warning or error
                      break;
                    }
                }

              if (columnIsPartiallyProvided)
                // from now on, treat it as if it were fully provided
                numProvidedCols++;
            }

          if (!columnIsPartiallyProvided)
            {
              // This column is not at all provided in the region start key
              // or we decided to erase the partial value.
              // Generate the min/max value for ASC/DESC key columns.
              // NOTE: This is generating un-encoded values, unlike
              //       the values we get from HBase. The next loop below
              //       will skip decoding for any values generated here.
              if (pkType->getTypeName() == "VARCHAR")
                colEncodedLength += pkType->getVarLenHdrSize();
              Lng32 remainingBufLen = colEncodedLength;

              if (nullHdrSize && !nullHdrAlreadySet)
                {
                  // generate a NULL indicator
                  // NULL (-1) for descending columns, this is the max value
                  // non-NULL (0) for ascending columns, min value is non-null
                  short indicatorVal = (isDescending ? -1 : 0);

                  CMPASSERT(nullHdrSize == sizeof(short));
                  memcpy(&actEncodedKey[currOffset], &indicatorVal, sizeof(indicatorVal));
                }

              pkType->minMaxRepresentableValue(&actEncodedKey[valOffset],
                                               &remainingBufLen,
                                               isDescending,
                                               NULL,
                                               heap);
            }

          currOffset += colEncodedLength;

        } // loop through columns not entirely provided

    } // provided encoded key length < total key length

  for (CollIndex c = 0; c < partColArray.entries(); c++)
    {
      const NAType *pkType = partColArray[c]->getType();
      Lng32 decodedValueLen = 
        pkType->getNominalSize() + pkType->getSQLnullHdrSize();
      ItemExpr *keyColVal = NULL;

      // does this column need encoding (only if it actually came
      // from an HBase split key, if we made up the value it's
      // already in the decoded format)
      if (pkType->isEncodingNeeded() && c < numProvidedCols)
        {
          encodedKeyP = &actEncodedKey[keyColOffset];

          // for varchar the decoding logic expects the length to be in the first
          // pkType->getVarLenHdrSize() chars, so add it.
          // Please see bug LP 1444134 on how to improve this in the long term.

          // Note that this is less than ideal:
          // - A VARCHAR is really encoded as a fixed char in the key, as
          //   the full length without a length field
          // - Given that an encoded key is not aligned, we should really
          //   consider it a byte string, e.g. a character type with charset
          //   ISO88591, which tolerates any bit patterns. Considering the
          //   enoded key as the same data type as the column causes all kinds
          //   of problems.
          // - The key decode function in the expressions code expects the varchar
          //   length field, even though it is not present in an actual key. So,
          //   we add it here in a separate buffer.
          // - When we generate a ConstValue to represent the decoded key, we also
          //   need to include the length field, with length = max. length

          if (pkType->getTypeName() == "VARCHAR" || pkType->getTypeName() == "CHAR")
          {
            Int32 varLenSize = pkType->getVarLenHdrSize() ;
            Int32 nullHdrSize = pkType->getSQLnullHdrSize();
            // Format of encodedKeyP :| null hdr | varchar data|
            // Format of VarcharStr : | null hdr | var len hdr | varchar data|
            varCharstr = new (heap) char[decodedValueLen + varLenSize];
            
            if (nullHdrSize > 0)
              str_cpy_all(varCharstr, encodedKeyP, nullHdrSize);

            // careful, this works on little-endian systems only!!
            str_cpy_all(varCharstr+nullHdrSize, (char*) &decodedValueLen, 
                        varLenSize);
            str_cpy_all(varCharstr+nullHdrSize+varLenSize, 
                        encodedKeyP+nullHdrSize, 
                        decodedValueLen-nullHdrSize);
            decodedValueLen += pkType->getVarLenHdrSize();
            encodedKeyP = varCharstr;
          }

          // un-encode the key value by using an expression
          NAString encConstLiteral("encoded_val");
          ConstValue *keyColEncVal =
            new (heap) ConstValue(pkType,
                                  (void *) encodedKeyP,
                                  decodedValueLen,
                                  &encConstLiteral,
                                  heap);
          CMPASSERT(keyColEncVal);

          if (keyColEncVal->isNull())
          {
            // do not call the expression evaluator if the value 
            // to be decoded is NULL.
            keyColVal = keyColEncVal ;
          }
          else 
          {
            keyColVal =
              new(heap) CompDecode(keyColEncVal,
                                   pkType,
                                   !partColArray.isAscending(c),
                                   decodedValueLen,
                                   CollationInfo::Sort,
                                   TRUE,
                                   heap);

            keyColVal->synthTypeAndValueId();

            keyColVal = keyColVal->evaluate(heap);

            if ( !keyColVal ) 
              return NULL;
          }

        } // encoded 
      else
        {
          // simply use the provided value as the binary value of a constant
          // for key generated internally, vcHdrSize should be included.
          if (pkType->getTypeName() == "VARCHAR")
            decodedValueLen += pkType->getVarLenHdrSize();
          keyColVal =
            new (heap) ConstValue(pkType,
                                  (void *) &actEncodedKey[keyColOffset],
                                  decodedValueLen,
                                  NULL,
                                  heap);
        }

      // this and the above assumes that encoded and unencoded values
      // have the same length
      keyColOffset += decodedValueLen;
      if (pkType->isEncodingNeeded() &&
          (pkType->getTypeName() == "VARCHAR" || pkType->getTypeName() == "CHAR") &&
          c < numProvidedCols)
      {
         keyColOffset -= pkType->getVarLenHdrSize();
         NADELETEBASIC (varCharstr, heap);
         varCharstr = NULL;
      }

      if (result)
        result = new(heap) ItemList(result, keyColVal);
      else
        result = keyColVal;
    }

  // make sure we consumed the entire key but no more than that
  CMPASSERT(keyColOffset == (totalKeyLength + vcHdrExtraBytes));

  if (actEncodedKey != encodedKey)
    NADELETEBASIC(actEncodedKey, heap);

  return result;
} // static getRangePartitionBoundaryValuesFromEncodedKeys()


// -----------------------------------------------------------------------
// createRangePartitionBoundaries()
// This method is used for creating a tuple, which defines the maximum
// permissible values that the partitioning key columns can contain
// within a certain partition, for range-partitioned data.
// -----------------------------------------------------------------------
NABoolean checkColumnTypeForSupportability(const NAColumnArray & partColArray, const char* key)
{
  NABoolean floatWarningIssued = FALSE;
  for (CollIndex c = 0; c < partColArray.entries(); c++) {

    const NAType *pkType = partColArray[c]->getType();

    // For the EAP release, the unsupported types are the non-standard
    // SQL/MP Datetime types.  For the FCS release the unsupported
    // types are the FRACTION only SQL/MP Datetime types.
    //
    // They are (for now) represented as CHAR types that have a
    // non-zero MP Datetime size.
    //
    NABoolean unsupportedPartnKey = FALSE;
    NABoolean unsupportedFloatDatatype = FALSE;
    if (NOT pkType->isSupportedType())
      unsupportedPartnKey = TRUE;
    else if (DFS2REC::isFloat(pkType->getFSDatatype())) {

      const NATable * naTable = partColArray[c]->getNATable();
      
      if ((CmpCommon::getDefault(MARIAQUEST_PROCESS) == DF_OFF) &&
	  (NOT naTable->isSeabaseTable()) &&
	  (NOT naTable->isHiveTable())) {
	unsupportedPartnKey = TRUE;
	unsupportedFloatDatatype = TRUE;
      }
    }

    if (unsupportedPartnKey) {
      // Get the name of the table which has the unsupported
      // partitioning key column.
      //
      const NAString &tableName =
          partColArray[c]->getNATable()->
          getTableName().getQualifiedNameAsAnsiString();

      if (unsupportedFloatDatatype)
	*CmpCommon::diags()
	  << DgSqlCode(-1120);
      else
	// ERROR 1123 Unable to process the partition key values...
	*CmpCommon::diags()
	  << DgSqlCode(-1123)
	  << DgString0(key)
	  << DgTableName(tableName);

      return FALSE;
    }
  }
      
  return TRUE;
}

// -----------------------------------------------------------------------
// createRangePartitionBoundaries()
// This method is used for creating a tuple, which defines the maximum
// permissible values that the partitioning key columns can contain
// within a certain partition, for range-partitioned data.
// -----------------------------------------------------------------------
static RangePartitionBoundaries * createRangePartitionBoundaries
                                     (TrafDesc * part_desc_list,
				      Lng32 numberOfPartitions,
			              const NAColumnArray & partColArray,
				      NAMemory* heap)
{

  // ---------------------------------------------------------------------
  // ASSUMPTION: The partitions descriptor list is a singly-linked list
  // ==========  in which the first element is the descriptor for the
  //             first partition and the last element is the descriptor
  //             for the last partition, in partitioning key sequence.
  // ---------------------------------------------------------------------
    TrafDesc * partns_desc = part_desc_list;
  CMPASSERT(partns_desc->partnsDesc()->primarypartition);


  // Check all the partitioning keys.  If any of them are not
  // supported, issue an error and return.
  //
  // Skip past the primary partition, so that a meaningful first
  // key value can be used for the error message.

  char* key = (partns_desc->next) ->partnsDesc()->firstkey;

  if ( !checkColumnTypeForSupportability(partColArray, key) )
    return NULL;
  

  // ---------------------------------------------------------------------
  // Allocate a new RangePartitionBoundaries.
  // ---------------------------------------------------------------------
  RangePartitionBoundaries * partBounds = new (heap)
    RangePartitionBoundaries
      (numberOfPartitions,
       partColArray.entries(),heap);

  // ---------------------------------------------------------------------
  // compute the length of the encoded partitioning key
  // ---------------------------------------------------------------------


  // ---------------------------------------------------------------------
  // Iterate over all the partitions and define the boundary (maximum
  // permissible key values) for each one of them.
  // The first key for the first partition cannot be specified in
  // the CREATE TABLE command. It is therefore stored as an empty
  // string in the SMD.
  // NOTE: The RangePartitionBoundaries is 0 based.
  // ---------------------------------------------------------------------
  partns_desc = partns_desc->next; // skip the primary partition
  Lng32 counter = 1;
  char* encodedKey;

  while (partns_desc AND (counter < numberOfPartitions))
    {
      encodedKey = partns_desc->partnsDesc()->encodedkey;
      size_t encodedKeyLen = partns_desc->partnsDesc()->encodedkeylen;

      if(heap != CmpCommon::statementHeap() && encodedKeyLen > 0)
      {
        //we don't know here if encodedkey is a regular char or a wchar
        //if it's a wchar then it should end with "\0\0", so add an extra
        //'\0' to the end, it wont hurt anyways. Copying encodedKeyLen+1 chars
        //will include one '\0' character and we add an extra '\0' to the end
        //to make it "\0\0".
        encodedKey = new(heap) char [encodedKeyLen+2];
        encodedKey[encodedKeyLen] = encodedKey[encodedKeyLen+1] = '\0';
        str_cpy_all(encodedKey, partns_desc->partnsDesc()->encodedkey,
                    encodedKeyLen);
      }

      ItemExpr *rangePartBoundValues = NULL;

      if (partns_desc->partnsDesc()->firstkey)
        // Extract and parse the partition boundary values, producing an
        // ItemExprList of the boundary values.
        //
        rangePartBoundValues = NATable::getRangePartitionBoundaryValues(
             partns_desc->partnsDesc()->firstkey,
             partns_desc->partnsDesc()->firstkeylen,
             heap);
      else
        rangePartBoundValues = getRangePartitionBoundaryValuesFromEncodedKeys(
             partColArray,
             encodedKey,
             encodedKeyLen,
             heap);

      // Check to see if the key values parsed successfully, also check
      // for constants mistaken as a column reference.
      if (rangePartBoundValues == NULL ||
          rangePartBoundValues->containsOpType(ITM_REFERENCE)) {

        // Get the name of the table which has the 'bad' first key
        // value.  Use the first entry in the array of partition
        // columns (partColArray) to get to the NATable object.
        //
        const NAString &tableName =
          partColArray[0]->getNATable()->
          getTableName().getQualifiedNameAsAnsiString();

        // The Parser will have already issued an error.
        // ERROR 1123 Unable to process the partition key values...
        *CmpCommon::diags()
          << DgSqlCode(-1123)
          << DgString0(partns_desc->partnsDesc()->firstkey)
          << DgTableName(tableName);
        delete partBounds;
        //coverity[leaked_storage]
        return NULL;
      }

      partBounds->defineUnboundBoundary(
           counter++,
           rangePartBoundValues,
           encodedKey);

      partns_desc = partns_desc->next;
    } // end while (partns_desc)

  // ---------------------------------------------------------------------
  // Before doing consistency check setup for the statement
  // ---------------------------------------------------------------------
  partBounds->setupForStatement(FALSE);

  // ---------------------------------------------------------------------
  // Perform a consistency check to ensure that a boundary was defined
  // for each partition.
  // ---------------------------------------------------------------------
  partBounds->checkConsistency(numberOfPartitions);

  return partBounds;

} // static createRangePartitionBoundaries()

// -----------------------------------------------------------------------
// createRangePartitioningFunction()
// This method is used for creating a rangePartitioningFunction.
// -----------------------------------------------------------------------
static PartitioningFunction * createRangePartitioningFunction
                                (TrafDesc * part_desc_list,
			         const NAColumnArray & partKeyColArray,
                                 NodeMap* nodeMap,
				 NAMemory* heap)
{
  // ---------------------------------------------------------------------
  // Compute the number of partitions.
  // ---------------------------------------------------------------------
  TrafDesc * partns_desc = part_desc_list;
  Lng32 numberOfPartitions = 0;
  while (partns_desc)
    {
      numberOfPartitions++;
      partns_desc = partns_desc->next;
    }

  // ---------------------------------------------------------------------
  // Each table has at least 1 partition
  // ---------------------------------------------------------------------
  numberOfPartitions = MAXOF(1,numberOfPartitions);

  if (numberOfPartitions == 1)
    return new (heap) SinglePartitionPartitioningFunction(nodeMap, heap);

  // ---------------------------------------------------------------------
  // Create the partitioning key ranges
  // ---------------------------------------------------------------------
  RangePartitionBoundaries *boundaries =
    createRangePartitionBoundaries(part_desc_list,
				   numberOfPartitions,
				   partKeyColArray,
				   heap);

  // Check to see if the boundaries were created successfully.  An
  // error could occur if one of the partitioning keys is an
  // unsupported type or if the table is an MP Table and the first key
  // values contain MP syntax that is not supported by MX.  For the
  // EAP release, the unsupported types are the non-standard SQL/MP
  // Datetime types.  For the FCS release the unsupported types are
  // the FRACTION only SQL/MP Datetime types. An example of a syntax
  // error is a Datetime literal which does not have the max number of
  // digits in each field. (e.g. DATETIME '1999-2-4' YEAR TO DAY)
  //
  if (boundaries == NULL) {
    // The Parser may have already issued an error.
    // ERROR 1123 Unable to process the partition key values...
    // will have been issued by createRangePartitionBoundaries.
    //
    return NULL;
  }

  return new (heap) RangePartitioningFunction(boundaries,  // memory leak??
                                              nodeMap, heap);

} // static createRangePartitioningFunction()

// -----------------------------------------------------------------------
// createRoundRobinPartitioningFunction()
// This method is used for creating a RoundRobinPartitioningFunction.
// -----------------------------------------------------------------------
static PartitioningFunction * createRoundRobinPartitioningFunction
                                (TrafDesc * part_desc_list,
                                 NodeMap* nodeMap,
				 NAMemory* heap)
{
  // ---------------------------------------------------------------------
  // Compute the number of partitions.
  // ---------------------------------------------------------------------
  TrafDesc * partns_desc = part_desc_list;
  Lng32 numberOfPartitions = 0;
  while (partns_desc)
    {
      numberOfPartitions++;
      partns_desc = partns_desc->next;
    }

  // ---------------------------------------------------------------------
  // Each table has at least 1 partition
  // ---------------------------------------------------------------------
  numberOfPartitions = MAXOF(1,numberOfPartitions);

  // For round robin partitioning, must create the partitioning function
  // even for one partition, since the SYSKEY must be generated for
  // round robin and this is trigger off the partitioning function.
  //
//  if (numberOfPartitions == 1)
//    return new (heap) SinglePartitionPartitioningFunction(nodeMap);

  return new (heap) RoundRobinPartitioningFunction(numberOfPartitions, nodeMap, heap);

} // static createRoundRobinPartitioningFunction()

// -----------------------------------------------------------------------
// createHashDistPartitioningFunction()
// This method is used for creating a HashDistPartitioningFunction.
// -----------------------------------------------------------------------
static PartitioningFunction * createHashDistPartitioningFunction
                                (TrafDesc * part_desc_list,
			         const NAColumnArray & partKeyColArray,
                                 NodeMap* nodeMap,
				 NAMemory* heap)
{
  // ---------------------------------------------------------------------
  // Compute the number of partitions.
  // ---------------------------------------------------------------------
  TrafDesc * partns_desc = part_desc_list;
  Lng32 numberOfPartitions = 0;
  while (partns_desc)
    {
      numberOfPartitions++;
      partns_desc = partns_desc->next;
    }

  // ---------------------------------------------------------------------
  // Each table has at least 1 partition
  // ---------------------------------------------------------------------
  numberOfPartitions = MAXOF(1,numberOfPartitions);

  if (numberOfPartitions == 1)
    return new (heap) SinglePartitionPartitioningFunction(nodeMap, heap);

  return new (heap) HashDistPartitioningFunction(numberOfPartitions, nodeMap, heap);

} // static createHashDistPartitioningFunction()

// -----------------------------------------------------------------------
// createHash2PartitioningFunction()
// This method is used for creating a Hash2PartitioningFunction.
// -----------------------------------------------------------------------
static PartitioningFunction * createHash2PartitioningFunction
                                (TrafDesc * part_desc_list,
                                 const NAColumnArray & partKeyColArray,
                                 NodeMap* nodeMap,
                                 NAMemory* heap)
{
  // ---------------------------------------------------------------------
  // Compute the number of partitions.
  // ---------------------------------------------------------------------
  TrafDesc * partns_desc = part_desc_list;
  Lng32 numberOfPartitions = 0;
  while (partns_desc)
    {
      numberOfPartitions++;
      partns_desc = partns_desc->next;
    }

  // ---------------------------------------------------------------------
  // Each table has at least 1 partition
  // ---------------------------------------------------------------------
  numberOfPartitions = MAXOF(1,numberOfPartitions);

  if (numberOfPartitions == 1)
    return new (heap) SinglePartitionPartitioningFunction(nodeMap, heap);

  return new (heap) Hash2PartitioningFunction(numberOfPartitions, nodeMap, heap);

} // static createHash2PartitioningFunction()


static PartitioningFunction * createHash2PartitioningFunction
                                (Int32 numberOfPartitions,
                                 const NAColumnArray & partKeyColArray,
                                 NodeMap* nodeMap,
                                 NAMemory* heap)
{
  // ---------------------------------------------------------------------
  // Each table has at least 1 partition
  // ---------------------------------------------------------------------
  if (numberOfPartitions == 1)
    return new (heap) SinglePartitionPartitioningFunction(nodeMap, heap);

  return new (heap) Hash2PartitioningFunction(numberOfPartitions, nodeMap, heap);

} // static createHash2PartitioningFunction()


static 
NodeMap* createNodeMapForHbase(TrafDesc* desc, const NATable* table,
                               int numSaltBuckets, NAMemory* heap)
{
   Int32 partns = 0;
   Int32 numRegions = 0;
   TrafDesc* hrk = desc;
 
   while ( hrk ) {
     numRegions++;
     hrk=hrk->next;
   }

   if (numSaltBuckets <= 1)
     partns = numRegions;
   else
     partns = numSaltBuckets;

   NodeMap* nodeMap = new (heap) 
       NodeMap(heap, partns, NodeMapEntry::ACTIVE, NodeMap::HBASE);

   // get nodeNames of region servers by making a JNI call
   // do it only for multiple partition table
   // TBD: co-location for tables where # of salt buckets and # regions don't match
   if (partns > 1 && (CmpCommon::getDefault(TRAF_ALLOW_ESP_COLOCATION) == DF_ON) &&
       (numSaltBuckets <= 1 || numSaltBuckets == numRegions)) {
     ARRAY(const char *) nodeNames(heap, partns);
     if (table->getRegionsNodeName(partns, nodeNames)) {
       for (Int32 p=0; p < partns; p++) {
         NAString node(nodeNames[p], heap);
         // remove anything after node name
         size_t size = node.index('.');
          if (size && size != NA_NPOS)
            node.remove(size);

         // populate NodeMape with region server node ids
         nodeMap->setNodeNumber(p, nodeMap->mapNodeNameToNodeNum(node));
       }
     }
   }

   return nodeMap;
}

static 
PartitioningFunction*
createHash2PartitioningFunctionForHBase(TrafDesc* desc,
                                        const NATable * table,
                                        int numSaltBuckets,
                                        NAMemory* heap)
{

   TrafDesc* hrk = desc;
 
   NodeMap* nodeMap = createNodeMapForHbase(desc, table, numSaltBuckets, heap);

   Int32 partns = nodeMap->getNumEntries();

   PartitioningFunction* partFunc;
   if ( partns > 1 )
     partFunc = new (heap) Hash2PartitioningFunction(partns, nodeMap, heap);
   else
     partFunc = new (heap) SinglePartitionPartitioningFunction(nodeMap, heap);

   return partFunc;
}

// -----------------------------------------------------------------------
// createRangePartitionBoundaries()
// This method is used for creating a tuple, which defines the maximum
// permissible values that the partitioning key columns can contain
// within a certain partition, for range-partitioned data.
//
// The boundary values of the range partitions are completely defined by 
// a histogram's boundary values.
//
// -----------------------------------------------------------------------
RangePartitionBoundaries * createRangePartitionBoundariesFromStats
                                      (const IndexDesc* idesc, 
                                       HistogramSharedPtr& hist,
                                       Lng32 numberOfPartitions,
                                       const NAColumnArray & partColArray,
                                       const ValueIdList& partitioningKeyColumnsOrder,
                                       const Int32 statsColsCount,
                                       NAMemory* heap)
{
  if ( (!checkColumnTypeForSupportability(partColArray, "")) ||
       (numberOfPartitions != hist->numIntervals())          ||
       (partColArray.entries() < statsColsCount)
     )
     return NULL;

  // ---------------------------------------------------------------------
  // Allocate a new RangePartitionBoundaries.
  // ---------------------------------------------------------------------
  RangePartitionBoundaries * partBounds = new (heap)
    RangePartitionBoundaries
      (numberOfPartitions,
       partColArray.entries(),heap);

  // ---------------------------------------------------------------------
  // compute the length of the encoded partitioning key
  // ---------------------------------------------------------------------

  // ---------------------------------------------------------------------
  // Iterate over all the partitions and define the boundary (maximum
  // permissible key values) for each one of them.
  // The first key for the first partition cannot be specified in
  // the CREATE TABLE command. It is therefore stored as an empty
  // string in the SMD.
  // NOTE: The RangePartitionBoundaries is 0 based.
  // ---------------------------------------------------------------------
  Lng32 counter = 1;
  ULng32 totalEncodedKeyLength = 0;

  Interval iter = hist->getFirstInterval();

  while ( iter.isValid() ) {

     totalEncodedKeyLength = 0;
  
     NAString* evInStr = NULL;

     NAColumn* ncol = partColArray[0];
     const NAType* nt = ncol->getType();

     double ev = ( !iter.isLast() ) ?  
                  iter.hiBound().getDblValue() : nt->getMaxValue();

     if ((partColArray.entries() == 1) && (statsColsCount == 1))
     {
        NABoolean bExact = FALSE;
        if (nt->getTypeQualifier() == NA_NUMERIC_TYPE && nt->getTypeName() == LiteralNumeric)
          if ((((NumericType*)nt))->isExact())
            bExact =TRUE;
        if (!bExact)
        // Convert the double into a string value of the type of 
        // the leading key column
          evInStr = nt->convertToString(ev, heap);
        else
          {
            Int64 nValue = 0;
            (((SQLNumeric*)nt))->getMaxValue(&nValue);
            evInStr = (((SQLNumeric*)nt))->convertToString(nValue, heap);
          }
     }
     else if ((partColArray.entries() > 1) && (statsColsCount == 1))
     {
        MCboundaryValueList mcEv; 
        mcEv.insert(EncodedValue(ev));
        evInStr = mcEv.convertToString(partColArray, iter.isLast());
     }
     else // partColArray.entries() > 1 && statsColsCount > 1
     {
        MCboundaryValueList mcEv = iter.hiMCBound();
        evInStr = mcEv.convertToString(partColArray, iter.isLast());
     }

     if ( !evInStr )
        return NULL;

     // Construct a boundary as ItemExprList of ConstValues 
     ItemExpr* rangePartBoundValues = NATable::getRangePartitionBoundaryValues(
          evInStr->data(), evInStr->length(), heap, CharInfo::ISO88591);

     NAString totalEncodedKeyBuf;
     ItemExpr* val = NULL;
     ItemExpr* encodeExpr = NULL ;
   
     ItemExprList* list = NULL;
     list = new (heap) ItemExprList(rangePartBoundValues, heap,ITM_ITEM_LIST,FALSE);

     for (CollIndex c = 0; c < partColArray.entries(); c++)
     {
         NAColumn* ncol = partColArray[c];
         const NAType* nt = ncol->getType();
         
         val = (ItemExpr*) (*list) [c];

         // make sure the value is the same type as the column
         val = new(heap) Cast(val, nt->newCopy(heap));

         if (nt->isEncodingNeeded())
            encodeExpr = new(heap) CompEncode(val, !(partColArray.isAscending(c)));
         else
            encodeExpr = val;

         encodeExpr->synthTypeAndValueId();
         const NAType& eeNT = encodeExpr->getValueId().getType();
         ULng32 encodedKeyLength = eeNT.getEncodedKeyLength();

         char* encodedKeyBuffer = new (heap) char[encodedKeyLength];

         Lng32 offset;
         Lng32 length;
         ValueIdList vidList;
    
         short ok = vidList.evaluateTree(encodeExpr,
                                    encodedKeyBuffer,
                                    encodedKeyLength,
                                    &length,
                                    &offset,
                                    (CmpCommon::diags()));

         totalEncodedKeyLength += encodedKeyLength;
         totalEncodedKeyBuf.append(encodedKeyBuffer, encodedKeyLength);

         if ( ok != 0 ) 
            return NULL;
     }

     char* char_totalEncodedKeyBuf =new char[totalEncodedKeyLength];
     memcpy (char_totalEncodedKeyBuf, totalEncodedKeyBuf.data(), totalEncodedKeyLength);

     if (totalEncodedKeyLength != 0)
     {
        partBounds->defineUnboundBoundary(
              counter++,
              rangePartBoundValues,
              char_totalEncodedKeyBuf);

     }

     iter.next();
  }

  // ---------------------------------------------------------------------
  // Before doing consistency check setup for the statement
  // ---------------------------------------------------------------------
  partBounds->setupForStatement(FALSE);

  // ---------------------------------------------------------------------
  // Perform a consistency check to ensure that a boundary was defined
  // for each partition.
  // ---------------------------------------------------------------------
  partBounds->checkConsistency(numberOfPartitions);

  // -----------------------------------------------------------------
  // Add the first and the last boundary (0 and numberOfPartitions)
  // at the ends that do not separate two partitions
  // -----------------------------------------------------------------
   partBounds->completePartitionBoundaries(
          partitioningKeyColumnsOrder,
          totalEncodedKeyLength);

  return partBounds;

} // createRangePartitionBoundariesFromStats()

static 
PartitioningFunction*
createRangePartitioningFunctionForSingleRegionHBase(
			                const NAColumnArray & partKeyColArray,
                                        NAMemory* heap
                                                   )
{
   NodeMap* nodeMap = NULL;

   Lng32 regionsToFake = 
      (ActiveSchemaDB()->getDefaults()).getAsLong(HBASE_USE_FAKED_REGIONS);

   if ( regionsToFake == 0 ) {

     nodeMap = new (heap) 
            NodeMap(heap, 1, NodeMapEntry::ACTIVE, NodeMap::HBASE);

     return new (heap) SinglePartitionPartitioningFunction(nodeMap, heap);
   }

   nodeMap = new (heap) 
            NodeMap(heap, regionsToFake, NodeMapEntry::ACTIVE, NodeMap::HBASE);

   //
   // Setup an array of doubles to record the next begin key value for
   // each key column. Needed when the table has a single region.
   // The number ranges is controlled by CQD HBASE_USE_FAKED_REGIONS.
   //
   // Later on, we can make smart split utilizing the stats. 
   // 
   Int32 keys = partKeyColArray.entries();

   double* firstkeys = new (heap) double[keys];
   double* steps = new (heap) double[keys];

   for ( Int32 i=0; i<keys; i++ ) {

       double min = partKeyColArray[i]->getType()->getMinValue();
       double max = partKeyColArray[i]->getType()->getMaxValue();

       firstkeys[i] = partKeyColArray[i]->getType()->getMinValue();
       steps[i] = (max - min) / regionsToFake;
   }


   struct TrafDesc* head = NULL;
   struct TrafDesc* tail = NULL;

   Int32 i=0;
   for ( i=0; i<regionsToFake; i++ ) {

     if ( tail == NULL ) {
        head = tail = new (heap) TrafPartnsDesc();

        // to satisfy createRangePartitionBoundaries() in NATable.cpp
        tail->partnsDesc()->primarypartition = 1;

     } else {
        tail->next = new (heap) TrafPartnsDesc();
        tail = tail->next;
     }
     tail->next = NULL;

     NAString firstkey('(');
     for ( Int32 i=0; i<keys; i++ ) {
         double v = firstkeys[i];
         NAString* v_str = partKeyColArray[i]->getType()->convertToString(v,heap);

        // If for some reason we can not make the conversion, we 
         // return a single-part func.
         if ( !v_str ) {
            nodeMap = new (heap)
                   NodeMap(heap, 1, NodeMapEntry::ACTIVE, NodeMap::HBASE);
            return new (heap) SinglePartitionPartitioningFunction(nodeMap, heap);
         }

         firstkey.append(*v_str);

         if ( i < keys-1 )
           firstkey.append(',');

         // Prepare for the next range
         firstkeys[i] += steps[i];
     }
     firstkey.append(')');


     Int32 len = firstkey.length();

     tail->partnsDesc()->firstkeylen = len;
     tail->partnsDesc()->firstkey = new (heap) char[len];
     memcpy(tail->partnsDesc()->firstkey, firstkey.data(), len);

     // For now, assume firstkey == encodedkey
     tail->partnsDesc()->encodedkeylen = len;
     tail->partnsDesc()->encodedkey = new (heap) char[len];
     memcpy(tail->partnsDesc()->encodedkey, firstkey.data(), len);

   }

   // 
   return createRangePartitioningFunction
                                (head,
                                 partKeyColArray,
                                 nodeMap,
                                 heap);
}

void
populatePartnDescOnEncodingKey( struct TrafDesc* prevEndKey,
                               struct TrafDesc* tail, 
                               struct TrafDesc* hrk, 
                               NAMemory* heap)
{
     if (!prevEndKey) {
       // the start key of the first partitions has all zeroes in it
       Int32 len = hrk->hbaseRegionDesc()->endKeyLen;

       tail->partnsDesc()->encodedkeylen = len;
       tail->partnsDesc()->encodedkey = new (heap) char[len];
       memset(tail->partnsDesc()->encodedkey, 0, len);
     }
     else {
       // the beginning key of this partition is the end key of
       // the previous one
       // (HBase returns end keys, we need begin keys here)
       Int32 len = prevEndKey->hbaseRegionDesc()->endKeyLen;

       // For HBase regions, we don't have the text representation
       // (value, value, ... value) of the boundary, just the encoded
       // key.
       tail->partnsDesc()->encodedkeylen = len;
       tail->partnsDesc()->encodedkey = new (heap) char[len];
       memcpy(tail->partnsDesc()->encodedkey, 
              prevEndKey->hbaseRegionDesc()->endKey, len);
     }
}

void
populatePartnDescOnFirstKey( struct TrafDesc* ,
                             struct TrafDesc* tail, 
                             struct TrafDesc* hrk,
                             NAMemory* heap)
{
   char* buf = hrk->hbaseRegionDesc()->beginKey;
   Int32 len = hrk->hbaseRegionDesc()->beginKeyLen;

   NAString firstkey('(');
   firstkey.append('\'');
   firstkey.append(buf, len);
   firstkey.append('\'');
   firstkey.append(')');

   Int32 keyLen = firstkey.length();
   tail->partnsDesc()->firstkeylen = keyLen;
   tail->partnsDesc()->firstkey = new (heap) char[keyLen];
   memcpy(tail->partnsDesc()->firstkey, firstkey.data(), keyLen);

   tail->partnsDesc()->encodedkeylen = keyLen;
   tail->partnsDesc()->encodedkey = new (heap) char[keyLen];
   memcpy(tail->partnsDesc()->encodedkey, firstkey.data(), keyLen);
}

typedef void (*populatePartnDescT)( struct TrafDesc* prevEndKey,
                                    struct TrafDesc* tail, 
                                    struct TrafDesc* hrk,
                                    NAMemory* heap);
static struct TrafDesc*
convertRangeDescToPartnsDesc(TrafDesc* desc, populatePartnDescT funcPtr, NAMemory* heap)
{
   TrafDesc* hrk = desc;
   TrafDesc* prevEndKey = NULL;
 
   struct TrafDesc* head = NULL;
   struct TrafDesc* tail = NULL;

   Int32 i=0;
   while ( hrk ) {

     struct TrafDesc *newNode = 
       TrafAllocateDDLdesc(DESC_PARTNS_TYPE, NULL);

     if ( tail == NULL ) {
        head = tail = newNode;

        // to satisfy createRangePartitionBoundaries() in NATable.cpp
        tail->partnsDesc()->primarypartition = 1;

     } else {
        tail->next = newNode;
        tail = tail->next;
     }

     (*funcPtr)(prevEndKey, tail, hrk, heap);

     prevEndKey = hrk;
     hrk     = hrk->next;
   }

   return head;
}


static 
PartitioningFunction*
createRangePartitioningFunctionForMultiRegionHBase(Int32 partns,
                                        TrafDesc* desc, 
                                        const NATable* table, 
			                const NAColumnArray & partKeyColArray,
                                        NAMemory* heap)
{
   NodeMap* nodeMap = createNodeMapForHbase(desc, table, -1, heap);

   struct TrafDesc* 
      partns_desc = ( table->isHbaseCellTable() || table->isHbaseRowTable()) ?
         convertRangeDescToPartnsDesc(desc, populatePartnDescOnFirstKey, heap)
             :
         convertRangeDescToPartnsDesc(desc, populatePartnDescOnEncodingKey, heap);


   return createRangePartitioningFunction
                                (partns_desc,
                                 partKeyColArray,
                                 nodeMap,
                                 heap);
}


Int32 findDescEntries(TrafDesc* desc)
{
   Int32 partns = 0;
   TrafDesc* hrk = desc;
   while ( hrk ) {
     partns++;
     hrk = hrk->next;
   }
   return partns;
}

//
// A single entry point to figure out range partition function for
// Hbase. 
//
static 
PartitioningFunction*
createRangePartitioningFunctionForHBase(TrafDesc* desc, 
			                const NATable* table,
                                        const NAColumnArray & partKeyColArray,
                                        NAMemory* heap)
{

  Int32 partns = 0;
  if (CmpCommon::getDefault(HBASE_RANGE_PARTITIONING) != DF_OFF)
      // First figure out # partns
      partns = findDescEntries(desc);
  else
    partns = 1;

   return (partns > 1) ?
      createRangePartitioningFunctionForMultiRegionHBase(partns,
                                    desc, table, partKeyColArray, heap)
       :
      createRangePartitioningFunctionForSingleRegionHBase(
                                    partKeyColArray, heap);
}

static PartitioningFunction * createHivePartitioningFunction
                                (Int32 numberOfPartitions,
                                 const NAColumnArray & partKeyColArray,
                                 NodeMap* nodeMap,
                                 NAMemory* heap)
{
  // ---------------------------------------------------------------------
  // Each table has at least 1 partition
  // ---------------------------------------------------------------------
  if (numberOfPartitions == 1)
    return new (heap) SinglePartitionPartitioningFunction(nodeMap, heap);

  return new (heap) HivePartitioningFunction(numberOfPartitions, nodeMap, heap);

} // static createHivePartitioningFunction()

// -----------------------------------------------------------------------
// createNodeMap()
// This method is used for creating a node map for all DP2 partitions of
// associated with this table or index.
// -----------------------------------------------------------------------
static void createNodeMap (TrafDesc* part_desc_list,
		           NodeMap*     nodeMap,
                           NAMemory*    heap,
                           char * tableName,
                           Int32 tableIdent)
{
  // ---------------------------------------------------------------------
  // Loop over all partitions creating a DP2 node map entry for each
  // partition.
  // ---------------------------------------------------------------------
  TrafDesc* partns_desc      = part_desc_list;
  CollIndex    currentPartition = 0;
  if(NOT partns_desc)
  {
    NodeMapEntry entry =
          NodeMapEntry(tableName,NULL,heap,tableIdent);
    nodeMap->setNodeMapEntry(currentPartition,entry,heap);
  }
  else{
    while (partns_desc)
    {
      NodeMapEntry entry(partns_desc->partnsDesc()->partitionname,
	partns_desc->partnsDesc()->givenname,
        heap,tableIdent);
      nodeMap->setNodeMapEntry(currentPartition,entry,heap);
      partns_desc = partns_desc->next;
      currentPartition++;
    }
  }
  // -------------------------------------------------------------------
  //  If no partitions supplied, create a single partition node map with
  // a dummy entry.
  // -------------------------------------------------------------------
  if (nodeMap->getNumEntries() == 0)
    {

      NodeMapEntry entry(NodeMapEntry::ACTIVE);
      nodeMap->setNodeMapEntry(0,entry,heap);

    }

  // -------------------------------------------------------------------
  // Set the tableIndent into the nodemap itself.
  // -------------------------------------------------------------------
  nodeMap->setTableIdent(tableIdent);

  // -----------------------------------------------------------------------
  //  See if we need to build a bogus node map with fake volume assignments.
  // This will allow us to fake costing code into believing that all
  // partitions are distributed evenly among SMP nodes in the cluster.
  // -----------------------------------------------------------------------
  if (CmpCommon::getDefault(FAKE_VOLUME_ASSIGNMENTS) == DF_ON)
    {

      // --------------------------------------------------------------------
      //  Extract number of SMP nodes in the cluster from the defaults table.
      // --------------------------------------------------------------------
      NADefaults &defs     = ActiveSchemaDB()->getDefaults();
      CollIndex  numOfSMPs =  CmpCommon::context()->getNumOfSMPs();

      if(CURRSTMT_OPTDEFAULTS->isFakeHardware())
      {
        numOfSMPs = defs.getAsLong(DEF_NUM_NODES_IN_ACTIVE_CLUSTERS);
      }


      // ------------------------------------------------------------------
      //  Determine how many node map entries will be assigned a particular
      // node, and also calculate if there are any remaining entries.
      // ------------------------------------------------------------------
      CollIndex entriesPerNode   = nodeMap->getNumEntries() / numOfSMPs;
      CollIndex entriesRemaining = nodeMap->getNumEntries() % numOfSMPs;

      // ----------------------------------------------------------------
      //  Assign each node to consecutive entries such that each node has
      // approximately the same number of entries.
      //
      //  Any extra entries get assigned evenly to the last remaining
      // nodes.  For example, if the cluster has 5 nodes and the node map
      // has 23 entries, we would assign nodes to entries as follows:
      //
      //  Entries  0 -  3 to node 0. (4 entries)
      //  Entries  4 -  7 to node 1. (4 entries)
      //  Entries  8 - 12 to node 2. (5 entries)
      //  Entries 13 - 17 to node 3. (5 entries)
      //  Entries 18 - 22 to node 4. (5 entries)
      // ----------------------------------------------------------------
      CollIndex mapIdx = 0;
      for (CollIndex nodeIdx = 0; nodeIdx < numOfSMPs; nodeIdx++)
        {
          if (nodeIdx == numOfSMPs - entriesRemaining)
            {
              entriesPerNode += 1;
            }

          for (CollIndex entryIdx = 0; entryIdx < entriesPerNode; entryIdx++)
            {
              nodeMap->setNodeNumber(mapIdx,nodeIdx);
              mapIdx += 1;
            }
        }

    }

} // static createNodeMap()

static void createNodeMap (hive_tbl_desc* hvt_desc,
		           NodeMap*     nodeMap,
                           NAMemory*    heap,
                           char * tableName,
                           Int32 tableIdent)
{

  // ---------------------------------------------------------------------
  // Loop over all hive storage (partition file ) creating a node map 
  // entry for each partition.
  // ---------------------------------------------------------------------

  CMPASSERT(nodeMap->type() == NodeMap::HIVE);
  hive_sd_desc* sd_desc = hvt_desc->getSDs();

  CollIndex    currentPartition = 0;

  //  char buf[500];
  Int32 i= 0;
  while (sd_desc)
    {
      HiveNodeMapEntry entry(NodeMapEntry::ACTIVE, heap);
      nodeMap->setNodeMapEntry(currentPartition++,entry,heap);
      sd_desc = sd_desc->next_;
    }

  // -------------------------------------------------------------------
  //  If no partitions supplied, create a single partition node map with
  // a dummy entry.
  // -------------------------------------------------------------------
  if (nodeMap->getNumEntries() == 0)
    {

      HiveNodeMapEntry entry(NodeMapEntry::ACTIVE, heap);
      nodeMap->setNodeMapEntry(0,entry,heap);

    }

  // -------------------------------------------------------------------
  // Set the tableIndent into the nodemap itself.
  // -------------------------------------------------------------------
  nodeMap->setTableIdent(tableIdent);

  // No fake volumn assignment because Hive' partitions are not hash
  // based, there is no balance of data among all partitions.
} // static createNodeMap()

//-------------------------------------------------------------------------
// This function checks if a table/index or any of its partitions are
// remote. This is required to determine the size of the EidRootBuffer
// to be sent to DP2 - Expand places limits on the size of messages
// - approx 31000 for messages to remote nodes, and 56000 for messages
// on the local node.
//-------------------------------------------------------------------------
static NABoolean checkRemote(TrafDesc* part_desc_list,
                             char * tableName)
{
    return TRUE;
}


static NAString makeTableName(const NATable *table,
			      const TrafColumnsDesc *column_desc)
{
  return NAString(
       table ?
       table->getTableName().getQualifiedNameAsAnsiString().data() : "");
}

static NAString makeColumnName(const NATable *table,
			       const TrafColumnsDesc *column_desc)
{
  NAString nam(makeTableName(table, column_desc));
  if (!nam.isNull()) nam += ".";
  nam += column_desc->colname;
  return nam;
}

// -----------------------------------------------------------------------
// Method for inserting new NAColumn entries in NATable::colArray_,
// one for each column_desc in the list supplied as input.
// -----------------------------------------------------------------------
static NABoolean createNAColumns(TrafDesc *column_desc_list	/*IN*/,
                                 NATable *table		/*IN*/,
                                 Int64 tablesFlags,     /*IN*/
                                 NAColumnArray &colArray	/*OUT*/,
                                 NAMemory *heap		/*IN*/)
{
  NAType *type;
  ColumnClass colClass;
  NABoolean hasCompositeCols = FALSE;
  while (column_desc_list)
    {
      TrafColumnsDesc * column_desc = column_desc_list->columnsDesc();
      NABoolean isMvSystemAdded = FALSE;
      NABoolean hasSystemColumnAsUserColumn = FALSE;

      if (NAColumn::createNAType(column_desc, table, tablesFlags, type, heap))
	return TRUE;

      // Get the column class.  The column will either be a system column or a
      // user column.
      //
      if (column_desc->colclass == *(char*)COM_SYSTEM_COLUMN_LIT)
        {
          if ( (CmpCommon::getDefault(OVERRIDE_SYSKEY)==DF_ON) &&
               (strcmp(column_desc->colname, "SYSKEY") == 0) &&
               (table && table->getSpecialType() != ExtendedQualName::VIRTUAL_TABLE) )
            {
	      colClass = USER_COLUMN;
	      hasSystemColumnAsUserColumn = TRUE;
            }
          else
            colClass = SYSTEM_COLUMN;
        }
      else if ((column_desc->colclass == *(char*)COM_USER_COLUMN_LIT) ||
               (column_desc->colclass == *(char*)COM_ADDED_USER_COLUMN_LIT) ||
               (column_desc->colclass == *(char*)COM_ADDED_ALTERED_USER_COLUMN_LIT) ||
               (column_desc->colclass == *(char*)COM_ALTERED_USER_COLUMN_LIT))
        colClass = USER_COLUMN;
      else if (column_desc->colclass == *(char*)COM_MV_SYSTEM_ADDED_COLUMN_LIT)
        {
          colClass = USER_COLUMN;
          isMvSystemAdded = TRUE;
        }
      else
        {
          // 4032 column is an unknown class (not sys nor user)
          *CmpCommon::diags() << DgSqlCode(-4032)
                              << DgColumnName(makeColumnName(table, column_desc))
                              << DgInt0(column_desc->colclass);
          return TRUE;					// error
        }
      
      // Create an NAColumn and insert it into the NAColumn array.
      //
      NAColumn *newColumn = NULL;
      if (column_desc->colname[0] != '\0')
        {
	  // Standard named column
          CMPASSERT(column_desc->colnumber >= 0);

         char* defaultValue = column_desc->defaultvalue;
         char* initDefaultValue = column_desc->initDefaultValue;
         char* heading = column_desc->heading;
         char* computed_column_text = column_desc->computed_column_text;
         NABoolean isSaltColumn = FALSE;
         NABoolean isDivisioningColumn = FALSE;
         NABoolean isReplicaColumn = FALSE;

         if (column_desc->defaultClass() == COM_ALWAYS_COMPUTE_COMPUTED_COLUMN_DEFAULT)
           {
             if (column_desc->colFlags & SEABASE_COLUMN_IS_SALT)
               isSaltColumn = TRUE;
             if (column_desc->colFlags & SEABASE_COLUMN_IS_DIVISION)
               isDivisioningColumn = TRUE;
             if (!computed_column_text)
               {
                 computed_column_text = defaultValue;
                 defaultValue = NULL;
               }
           }
         if ((column_desc->colFlags & SEABASE_COLUMN_IS_REPLICA)||
             ((table->getSpecialType() == ExtendedQualName::INDEX_TABLE) && 
              (strncmp(column_desc->colname, "_REPLICA_@", 10) == 0)))
           isReplicaColumn = TRUE;

         if(ActiveSchemaDB()->getNATableDB()->cachingMetaData()){
           //make copies of stuff onto the heap passed in
           if(defaultValue){
             defaultValue = (char*) new (heap) char[strlen(defaultValue)+1];
             strcpy(defaultValue, column_desc->defaultvalue);
           }
           if(initDefaultValue){
             initDefaultValue = (char*) new (heap) char[strlen(initDefaultValue)+1];
             strcpy(initDefaultValue, column_desc->initDefaultValue);
           }

           if(heading){
             Int32 headingLength = str_len(heading)+1;
             heading = new (heap) char [headingLength];
             memcpy(heading,column_desc->heading,headingLength);
           }

           if(computed_column_text){
             char * computed_column_text_temp = computed_column_text;
             Int32 cctLength = str_len(computed_column_text)+1;
             computed_column_text = new (heap) char [cctLength];
             memcpy(computed_column_text,computed_column_text_temp,cctLength);
           }
         }

         newColumn = new (heap)
		      NAColumn(column_desc->colname,
			       column_desc->colnumber,
			       type,
                               heap,
			       table,
			       colClass,
			       column_desc->defaultClass(),
			       defaultValue,
                               heading,
			       column_desc->isUpshifted(),
                               ((column_desc->colclass == *(char*)COM_ADDED_USER_COLUMN_LIT) ||
                                (column_desc->colclass == *(char*)COM_ADDED_ALTERED_USER_COLUMN_LIT)),
                               COM_UNKNOWN_DIRECTION,
                               FALSE,
                               NULL,
                               TRUE, // stored on disk
                               computed_column_text,
                               isSaltColumn,
                               isDivisioningColumn,
                               isReplicaColumn,
                               ((column_desc->colclass == *(char*)COM_ADDED_ALTERED_USER_COLUMN_LIT) ||
                                (column_desc->colclass == *(char*)COM_ALTERED_USER_COLUMN_LIT)),
                               initDefaultValue);
	}
      else
        {
          CMPASSERT(0);
	}
      
      if (isMvSystemAdded)
	newColumn->setMvSystemAddedColumn();

      if (table &&
	  ((table->isSeabaseTable()) ||
	   (table->isHbaseCellTable()) ||
	   (table->isHbaseRowTable())))
	{
	  if (column_desc->hbaseColFam)
	    newColumn->setHbaseColFam(column_desc->hbaseColFam);
	  if (column_desc->hbaseColQual)
	    newColumn->setHbaseColQual(column_desc->hbaseColQual);

	  newColumn->setHbaseColFlags(column_desc->hbaseColFlags);
	}
      
      if (table != NULL)
	{
	  if (newColumn->isAddedColumn())
	    table->setHasAddedColumn(TRUE);

	  if (newColumn->getType()->isVaryingLen())
	    table->setHasVarcharColumn(TRUE);

	  if (hasSystemColumnAsUserColumn)
	    table->setSystemColumnUsedAsUserColumn(TRUE) ;

	  if (newColumn->getType()->isLob())
	    table->setHasLobColumn(TRUE);

	  if (CmpSeabaseDDL::isEncodingNeededForSerialization(newColumn))
	    table->setHasSerializedEncodedColumn(TRUE);

          if (CmpSeabaseDDL::isSerialized(newColumn->getHbaseColFlags()))
	    table->setHasSerializedColumn(TRUE);
	}

      colArray.insert(newColumn);

      if (column_desc->compDefnStr)
        hasCompositeCols = TRUE;

      column_desc_list = column_desc_list->next;
    } // end while

  table->setHasCompositeColumns(hasCompositeCols);

  return FALSE;							// no error

} // createNAColumns()
      
NAType* getSQColTypeForHive(const char* hiveType, NABoolean isORC,
                            NAMemory* heap)
{
  return NAType::getNATypeForHive(hiveType, isORC, FALSE, heap);
}

static NABoolean addVirtualHiveColumns(NAColumnArray &colArray,
                                       NATable *table,
                                       NABoolean isView,
                                       int maxColIx,
                                       NAMemory *heap)
{
  // dont add virtual cols if this is a hive view
  if (isView)
    return FALSE;

  // Add virtual Hive columns:
  
  // INPUT__FILE__NAME            char(1024 bytes) character set utf8
  // BLOCK__OFFSET__INSIDE__FILE  largeint
  // INPUT__RANGE__NUMBER         integer
  // ROW__NUMBER__IN__RANGE       largeint
  
  for (int v=0; v<4; v++)
    {
      const char *virtColName = NULL;
      NAType* virtType = NULL;
      NAColumn::VirtColType virtColType = NAColumn::HIVE_VIRT_FILE_COL;


      switch (v)
        {
        case 0: // INPUT__FILE__NAME
           virtColName = NATable::getNameOfInputFileCol();
           // VARCHAR(1024 BYTES) CHARACTER SET UTF8
           virtType = new(heap) SQLVarChar(heap, CharLenInfo(4096, 4096),
                                           FALSE, // not NULL
                                           FALSE, // not upshifted
                                           FALSE, // case sensitive
                                           CharInfo::UTF8);
           break;
        case 1: // BLOCK__OFFSET__INSIDE__FILE
          virtColName = NATable::getNameOfBlockOffsetCol();
          virtType = new(heap) SQLLargeInt(heap, TRUE, FALSE);
          virtColType = NAColumn::HIVE_VIRT_ROW_COL;
          break;
        case 2: // INPUT__RANGE__NUMBER
          virtColName = NATable::getNameOfInputRangeCol();
          virtType = new(heap) SQLInt(heap, TRUE, FALSE);
          break;
        case 3: // ROW__NUMBER__IN__RANGE
          virtColName = NATable::getNameOfRowInRangeCol();
          virtType = new(heap) SQLLargeInt(heap, TRUE, FALSE);
          virtColType = NAColumn::HIVE_VIRT_ROW_COL;
          break;
        }
      
      NAColumn* newVirtColumn = new (heap)
        NAColumn(virtColName,
                 ++maxColIx,
                 virtType,
                 heap,
                 table,
                 SYSTEM_COLUMN);
      
      newVirtColumn->setVirtualColumnType(virtColType);
      colArray.insert(newVirtColumn);
    }

  return FALSE;
}

static NABoolean createNAColumns(struct hive_column_desc* hcolumn /*IN*/,
                                 struct hive_pkey_desc* pcolumn /*IN*/,
                                 NABoolean isView, /*IN*/
                                 NATable *table		/*IN*/,
                                 NAColumnArray &colArray	/*OUT*/,
                                 NAMemory *heap		/*IN*/)
{
  // Assume that hive_struct->conn has the right connection,
  // and tblID and sdID has be properly set.
  // In the following loop, we need to extract the column information.
  int maxColIx = -1;

   while (hcolumn) {

     NAType* natype = getSQColTypeForHive(hcolumn->type_, 
                                          table->isORC(), 
                                          heap);

      if ( !natype ) {
	*CmpCommon::diags()
	  << DgSqlCode(-1204)
	  << DgString0(hcolumn->type_);
         return TRUE;
      }

      if (natype->isComposite())
        table->setHasCompositeColumns(TRUE);

      NAString colName(hcolumn->name_);
      colName.toUpper();

      NAColumn* newColumn = new (heap)
                    NAColumn(colName.data(),
                             hcolumn->intIndex_,
                             natype,
                             heap,
                             table,
                             USER_COLUMN, // colClass,
                             COM_NULL_DEFAULT  ,//defaultClass,
                             (char*)"", // defaultValue,
                             (char*)"", // heading,
                             FALSE, // column_desc->isUpshifted(),
                             FALSE, // added column
                             COM_UNKNOWN_DIRECTION,
                             FALSE,  // isOptional
                             NULL,  // routineParamType
                             TRUE, // column_desc->stored_on_disk,
                             (char*)"" //computed_column_text
                            );

      if (table != NULL)
      {
        if (newColumn->isAddedColumn())
          table->setHasAddedColumn(TRUE);

        if (newColumn->getType()->isVaryingLen())
          table->setHasVarcharColumn(TRUE);
      }

      colArray.insert(newColumn);

      if (maxColIx < hcolumn->intIndex_)
        maxColIx = hcolumn->intIndex_;

      hcolumn= hcolumn->next_;

    }

   // Add partitioning columns at the end, if present. These
   // will be computed from the metadata, they are not present
   // in the actual file.
   int partColIx = 0;

   while (pcolumn) {

     NAType* natype = getSQColTypeForHive(pcolumn->type_, table->isORC(), heap);

      if ( !natype ) {
	*CmpCommon::diags()
	  << DgSqlCode(-1204)
	  << DgString0(pcolumn->type_);
         return TRUE;
      }

      NAString colName(pcolumn->name_);
      colName.toUpper();

      NAColumn* newColumn = new (heap)
        NAColumn(colName.data(),
                 ++maxColIx,
                 natype,
                 heap,
                 table);

      newColumn->setVirtualColumnType(NAColumn::HIVE_PART_COL);
      colArray.insert(newColumn);

      pcolumn= pcolumn->next_;

    }

   if (addVirtualHiveColumns(colArray, table, isView, maxColIx, heap))
     return TRUE;

  return FALSE;							// no error

} // createNAColumns()



NABoolean createNAKeyColumns(TrafDesc *keys_desc_list	/*IN*/,
			     NAColumnArray &colArray	/*IN*/,
			     NAColumnArray &keyColArray /*OUT*/,
			     CollHeap *heap		/*IN*/)
{
  const TrafDesc *keys_desc = keys_desc_list;

  while (keys_desc)
    {
      Int32 tablecolnumber = keys_desc->keysDesc()->tablecolnumber;

      NAColumn *indexColumn = colArray.getColumn(tablecolnumber);

      SortOrdering order = NOT_ORDERED;

      keyColArray.insert(indexColumn);
      order = keys_desc->keysDesc()->isDescending() ? DESCENDING : ASCENDING;
      keyColArray.setAscending(keyColArray.entries()-1, order == ASCENDING);

      // Remember that this columns is part of the clustering
      // key and remember its key ordering (asc or desc)
      indexColumn->setClusteringKey(order);

      keys_desc = keys_desc->next;
    } // end while (keys_desc)

  return FALSE;
}

static void createHiveKeyColumns(const NAColumnArray &colArray  /*IN*/,
                                 NAColumnArray &keyColArray     /*OUT*/,
                                 CollHeap *heap	                /*IN*/)
{
  // There are two candidate keys in the virtual columns, the real
  // columns do not have a unique key. Pick one of them, based on
  // CQD HIVE_USE_PERSISTENT_KEY:
  //
  // ON:  (INPUT__FILE__NAME, BLOCK__OFFSET__INSIDE__FILE)
  // OFF: (INPUT__RANGE__NUMBER, ROW__NUMBER__IN__RANGE)
  //
  // The persistent key is much longer, but persists across different
  // scans. The non-persistent key is shorter, but can't be used for
  // a self join.

  // If CQD HIVE_USE_SORT_COLS_IN_KEY is ON, and if the table is a
  // sorted bucketed table, then add these sort columns to the key.
  // They are not needed for uniqueness, but they could define a
  // sort order, if a few conditions are met (we currently don't
  // check those, so this function is not yet enabled).

  NAColumn *indexColumn;
  const char *colName;
  NABoolean persistentKey =
    (CmpCommon::getDefault(HIVE_USE_PERSISTENT_KEY) != DF_OFF);

  for (int i=0; i<2; i++)
    {
      if (persistentKey)
        if (i == 0)
          colName = NATable::getNameOfInputFileCol();
        else
          colName = NATable::getNameOfBlockOffsetCol();
      else
        if (i == 0)
          colName = NATable::getNameOfInputRangeCol();
        else
          colName = NATable::getNameOfRowInRangeCol();

      indexColumn = colArray.getColumn(colName);
      keyColArray.insert(indexColumn);
      indexColumn->setClusteringKey(ASCENDING);
    }
}

// ************************************************************************
// The next two methods are used for code related to indexes hiding.
// In particular, this is related to hiding remote indexes having the same
// name as the local name. Here we mark the remote indexes that have the
// same local name and in addition share the following:
//  (1) both share the same index columns
//  (2) both have the same partioning keys
//
// The method naStringHashFunc is used by the NAHashDictionary<NAString, Index>
// that maps indexname to the corresponding list of indexes having that name
//
//*************************************************************************
ULng32 naStringHashFunc(const NAString& indexName)
{
  ULng32 hash= (ULng32) NAString::hash(indexName);
  return hash;
}

//*************************************************************************
// The method processDuplicateNames() is called by createNAFileSet() for
// tables having duplicate remote indexes.
//*************************************************************************
void processDuplicateNames(NAHashDictionaryIterator<NAString, Int32> &Iter,
                           NAFileSetList & indexes,
                           char *localNodeName)

{
    return;
} // processDuplicateNames()

void NATable::statsToUse(
     TrafDesc * storedStatsDesc,
     NABoolean &useEstimatedStats, 
     NABoolean &useStoredStats)
{
  useEstimatedStats = FALSE;
  useStoredStats = FALSE;

  if ((! storedStatsDesc) ||
      (! storedStatsDesc->tableStatsDesc()) ||
      ((NOT isPreloaded()) &&
       (CmpCommon::getDefault(TRAF_USE_STORED_STATS) == DF_OFF)) ||
      ((! storedStatsDesc->tableStatsDesc()->histograms_desc) &&
       (storedStatsDesc->tableStatsDesc()->rowcount == -1)))
    return;

  if ((CmpCommon::getDefault(TRAF_USE_STORED_STATS) == DF_ALL) &&
      (storedStatsDesc->tableStatsDesc()->histograms_desc))
    {
      useStoredStats = TRUE;
      return;
    }

  if (storedStatsDesc->tableStatsDesc()->rowcount != -1)
    {
      useEstimatedStats = TRUE;
      return;
    }

  return;
}

// -----------------------------------------------------------------------
// Method for:
// -  inserting new NAFileSet entries in NATable::indexes_
//    one for each index in the list supplied as input. It also
//    returns a pointer to the NAFileSet for the clustering index
//    as well as the primary index on this NATable.
// -  inserting new NAFileSet entries in NATable::vertParts_
//    one for each vertical partition in the list supplied as input.
// -----------------------------------------------------------------------
static
NABoolean createNAFileSets(TrafDesc * table_desc       /*IN*/,
                           const NATable * table          /*IN*/,
                           const NAColumnArray & colArray /*IN*/,
                           NAFileSetList & indexes        /*OUT*/,
                           NAFileSetList & vertParts      /*OUT*/,
                           NAFileSet * & clusteringIndex  /*OUT*/,
			   LIST(CollIndex) & tableIdList  /*OUT*/,
                           NAMemory* heap,
                           BindWA * bindWA,
                           NAColumnArray &newColumns, /*OUT */
			   Int32 *maxIndexLevelsPtr = NULL)
{
  // ---------------------------------------------------------------------
  // Add index/vertical partition (VP) information; loop over all indexes/
  // VPs, but start with the clustering key, then process all others.
  // The clustering key has a keytag 0.
  // ---------------------------------------------------------------------

  TrafDesc *indexes_desc = table_desc->tableDesc()->indexes_desc;

  while (indexes_desc AND indexes_desc->indexesDesc()->keytag)
    indexes_desc = indexes_desc->next;

  // must have a clustering key if not view or is a virtual table
  CMPASSERT((indexes_desc AND !indexes_desc->indexesDesc()->keytag) OR
	    (table_desc->tableDesc()->views_desc) OR
            (((NATable*)table)->getTableType() == ExtendedQualName::VIRTUAL_TABLE));

  NABoolean isTheClusteringKey = TRUE;
  NABoolean hasRemotePartition = FALSE;
  CollIndex numClusteringKeyColumns = 0;
  NABoolean tableAlignedRowFormat = table->isSQLMXAlignedTable();
  // get hbase table index level and blocksize. costing code uses index_level
  // and block size to estimate cost. Here we make a JNI call to read index level
  // and block size. If there is a need to avoid reading from Hbase layer,
  // HBASE_INDEX_LEVEL cqd can be used to disable JNI call. User can
  // set this CQD to reflect desired index_level for his query.
  // Default value of HBASE_BLOCK_SIZE is 64KB, when not reading from Hbase layer. 
  Int32 hbtIndexLevels = 0;
  Int32 hbtBlockSize = 0;
  NABoolean res = false;
  if (table->isHbaseTable())
  {
    NABoolean doStoredStatsOpt = FALSE;
    if (table->storedStatsAvail())
      doStoredStatsOpt = TRUE;

    if (doStoredStatsOpt)
      {
        TrafTableStatsDesc * statsDesc = 
          table->getStoredStatsDesc()->tableStatsDesc();
        hbtIndexLevels = statsDesc->hbtIndexLevels;
        hbtBlockSize = statsDesc->hbtBlockSize;
      }
    else
      {
        // get default values of index_level and block size
        hbtIndexLevels = (ActiveSchemaDB()->getDefaults()).getAsLong(HBASE_INDEX_LEVEL);
        hbtBlockSize = (ActiveSchemaDB()->getDefaults()).getAsLong(HBASE_BLOCK_SIZE);
        // call getHbaseTableInfo if index level is set to 0
        if (hbtIndexLevels == 0)
          res = table->getHbaseTableInfo(hbtIndexLevels, hbtBlockSize);
      }
  }
    
  NABoolean doHash2 = 
      (CmpCommon::getDefault(HBASE_HASH2_PARTITIONING) != DF_OFF && 
       !(bindWA && bindWA->isTrafLoadPrep())); 

  // ---------------------------------------------------------------------
  // loop over all indexes/VPs defined on the base table
  // ---------------------------------------------------------------------
  while (indexes_desc)
    {
      Lng32 numberOfFiles = 1;	  	// always at least 1
      NAColumn * indexColumn;		// an index/VP key column
      NAColumn * newIndexColumn;
      NAFileSet * newIndex;		// a new file set
      //hardcoding statement heap here, previosly the following calls
      //used the heap that was passed in (which was always statement heap)
      //Now with the introduction of NATable caching we pass in the NATable
      //heap and these guys should not be created on the NATable heap, they
      //should be created on the statement heap. Only the array objects
      //will be on the statement heap whatever is in the arrays i.e. NAColumns
      //will still be where ever they were before.
      NAColumnArray allColumns(CmpCommon::statementHeap());// all columns that belong to an index
      NAColumnArray indexKeyColumns(CmpCommon::statementHeap());// the index key columns
      NAColumnArray saveNAColumns(CmpCommon::statementHeap());// save NAColums of secondary index columns
      NAColumnArray partitioningKeyColumns(CmpCommon::statementHeap());// the partitioning key columns
      NAColumnArray noHiveSortColumns(CmpCommon::statementHeap());// not needed in this method
      PartitioningFunction * partFunc = NULL;
      NABoolean isPacked = FALSE;
      TrafIndexesDesc* currIndexDesc = indexes_desc->indexesDesc();
      NABoolean indexAlignedRowFormat = (currIndexDesc->rowFormat() == COM_ALIGNED_FORMAT_TYPE);

      NABoolean isNotAvailable = FALSE;

      ItemExprList hbaseSaltColumnList(CmpCommon::statementHeap());
      Int64 numOfSaltedPartitions = 0;
      Int16 numTrafReplicas = 0;

      // ---------------------------------------------------------------------
      // loop over the clustering key columns of the index
      // ---------------------------------------------------------------------
      const TrafDesc *keys_desc = currIndexDesc->keys_desc;
      while (keys_desc)
	{
          // Add an index/VP key column.
          //
          // If this is an alternate index or a VP, the keys table actually
          // describes all columns of the index or VP. For nonunique
	  // indexes, all index columns form the key, while for unique
	  // alternate indexes the last "numClusteringKeyColumns"
	  // columns are non-key columns, they are just the clustering
	  // key columns used to find the base table record. This is
	  // true for both SQL/MP and SQL/MX tables at this time.
	  // To make these assumptions is not optimal, but the
	  // TrafDescs that are used as input are a historical
	  // leftover from SQL/MP and therefore aren't set up very
	  // well to describe index columns and index keys.  Some day
	  // we might consider a direct conversion from the MX catalog
	  // manager (SOL) objects into NATables and NAFilesets.
	  //
	  // NOTE:
	  // The last "numClusteringKeyColumns" key columns
	  // of a unique alternate index (which ARE described in the
	  // keys_desc) get deleted later.
          
	  Int32 tablecolnumber = keys_desc->keysDesc()->tablecolnumber;
          indexColumn = colArray.getColumn(tablecolnumber);

          if ((table->isHbaseTable()) &&
              ((currIndexDesc->keytag != 0) || 
                (indexAlignedRowFormat  && indexAlignedRowFormat != tableAlignedRowFormat)))
            {
              newIndexColumn = new(heap) NAColumn(*indexColumn);
              newIndexColumn->setIndexColName(keys_desc->keysDesc()->keyname);
              newIndexColumn->setHbaseColFam(keys_desc->keysDesc()->hbaseColFam);
              newIndexColumn->setHbaseColQual(keys_desc->keysDesc()->hbaseColQual);
              newIndexColumn->resetSerialization(); 
              saveNAColumns.insert(indexColumn);
              newColumns.insert(newIndexColumn);
              indexColumn = newIndexColumn;
              
            }
          SortOrdering order = NOT_ORDERED;

          // as mentioned above, for all alternate indexes we
          // assume at first that all columns are key columns
          // and we make adjustments later
          indexKeyColumns.insert(indexColumn);
          order = keys_desc->keysDesc()->isDescending() ?
            DESCENDING : ASCENDING;
          indexKeyColumns.setAscending(indexKeyColumns.entries() - 1,
                                       order == ASCENDING);
          
          if ( table->isHbaseTable() && 
               indexColumn->isSaltColumn() ) 
            {
              
              // examples of the saltClause string:
              // 1. HASH2PARTFUNC(CAST(L_ORDERKEY AS INT NOT NULL) FOR 4)
              // 2. HASH2PARTFUNC(CAST(A AS INT NOT NULL),CAST(B AS INT NOT NULL) FOR 4) 
              const char* saltClause = indexColumn->getComputedColumnExprString();
              
              Parser parser(CmpCommon::context());
              ItemExpr* saltExpr = parser.getItemExprTree(saltClause, 
                                                          strlen(saltClause), 
                                                          CharInfo::UTF8);
              
              CMPASSERT(saltExpr &&
                        saltExpr->getOperatorType() == ITM_HASH2_DISTRIB);
                
              // get the # of salted partitions from saltClause
              ItemExprList csList(CmpCommon::statementHeap());
              saltExpr->findAll(ITM_CONSTANT, csList, FALSE, FALSE);

              // get #salted partitions from last ConstValue in the list
              if ( csList.entries() > 0 ) {
                ConstValue* ct = (ConstValue*)csList[csList.entries()-1];

                if ( ct->canGetExactNumericValue() )  {
                  numOfSaltedPartitions = ct->getExactNumericValue();
                }
              }

              // collect all ColReference objects into hbaseSaltColumnList.
              saltExpr->findAll(ITM_REFERENCE, hbaseSaltColumnList, FALSE, FALSE);
            }

          if ( table->isHbaseTable() && 
               indexColumn->isReplicaColumn() && 
               ((NATable *)table)->getTableType() != ExtendedQualName::INDEX_TABLE) 
            {
              numTrafReplicas = str_atoi(indexColumn->getDefaultValue(), 
                                         str_len(indexColumn->getDefaultValue()));
            }
          
	  if (isTheClusteringKey)
            {
              // Since many columns of the base table may not be in the
              // clustering key, we'll delay setting up the list of all
              // columns in the index until later, so we can then just
              // add them all at once.
              
              // Remember that this columns is part of the clustering
              // key and remember its key ordering (asc or desc)
	      indexColumn->setClusteringKey(order);
	      numClusteringKeyColumns++;
            }
	  else
	  {
            // Since all columns in the index are guaranteed to be in
            // the key, we can set up the list of all columns in the index
            // now just by adding every key column.
	    allColumns.insert(indexColumn);
	  }

	  keys_desc = keys_desc->next;
	} // end while (keys_desc)

      // ---------------------------------------------------------------------
      // Loop over the non key columns of the index/vertical partition.
      // These columns get added to the list of all the columns for the index/
      // VP.  Their length also contributes to the total record length.
      // ---------------------------------------------------------------------
      const TrafDesc *non_keys_desc = currIndexDesc->non_keys_desc;
      while (non_keys_desc)
	{
	  Int32 tablecolnumber = non_keys_desc->keysDesc()->tablecolnumber;
          indexColumn = colArray.getColumn(tablecolnumber);

          if (non_keys_desc->keysDesc()->isAddnlCol())
            indexColumn->setAddnlColumn();

	  if ((table->isHbaseTable()) &&
	      ((currIndexDesc->keytag != 0) || 
               (indexAlignedRowFormat  && indexAlignedRowFormat != tableAlignedRowFormat)))
	    {
	      newIndexColumn = new(heap) NAColumn(*indexColumn);
	      if (non_keys_desc->keysDesc()->keyname)
		newIndexColumn->setIndexColName(non_keys_desc->keysDesc()->keyname);
	      newIndexColumn->setHbaseColFam(non_keys_desc->keysDesc()->hbaseColFam);
	      newIndexColumn->setHbaseColQual(non_keys_desc->keysDesc()->hbaseColQual);
              newIndexColumn->resetSerialization(); 
	      indexColumn = newIndexColumn;
              newColumns.insert(newIndexColumn);
	    }

	  allColumns.insert(indexColumn);
	  non_keys_desc = non_keys_desc->next;
	} // end while (non_keys_desc)

      TrafDesc *files_desc;
      NABoolean isSystemTable;
      if (isTheClusteringKey)
	{
          // We haven't set up the list of all columns in the clustering
          // index yet, so do that now. Do this by adding all
	  // the base table columns to the columns of the clustering index.
          // Don't add a column, of course, if somehow it has already
          // been added.
	  for (CollIndex bcolNo = 0; bcolNo < colArray.entries(); bcolNo++)
	    {
	      NAColumn *baseCol = colArray[bcolNo];
	      if (NOT allColumns.contains(baseCol))
		{
		  // add this base column
		  allColumns.insert(baseCol);
		}
	    } // end for

	  files_desc = table_desc->tableDesc()->files_desc;
	  isSystemTable = table_desc->tableDesc()->isSystemTableCode();

          // Record length of clustering key is the same as that of the base table record
          currIndexDesc->record_length = table_desc->tableDesc()->record_length;
	} // endif (isTheClusteringKey)
      else
	{
	  if (currIndexDesc->isUnique())
	    {
	      // As mentioned above, if this is a unique index,
	      // the last numClusteringKeyColumns are actually not
	      // part of the KEY of the index, they are just part of
	      // the index record. Since there are keys_desc entries
	      // for these columns, remove the correspoinding entries
	      // from indexKeyColumns
	      // $$$$ Commenting this out, since Catman and DP2 handle index
	      // keys differently: they always assume that all index columns
	      // are part of the key. Somehow DP2 is told which prefix length
	      // of the key is actually the unique part.
	      // $$$$ This could be enabled when key lengths and record lengths
	      // are different.
	      // for (CollIndex i = 0; i < numClusteringKeyColumns; i++)
	      //   indexKeyColumns.removeAt(indexKeyColumns.entries() - 1);
	    }

	  files_desc = currIndexDesc->files_desc;
	  isSystemTable = currIndexDesc->isSystemTableCode();

	} // endif (NOT isTheClusteringKey)

      // -------------------------------------------------------------------
      // Build the partition attributes for this table.
      //
      // Usually the partitioning key columns are the same as the
      // clustering key columns.  If no partitioning key columns have
      // been specified then the partitioning key columns will be assumed
      // to be the same as the clustering key columns.  Otherwise, they
      // could be the same but may not necessarily be the same.
      //
      // We will ASSUME here that NonStop SQL/MP or the simulator will not
      // put anything into partitioning keys desc and only SQL/MX will.  So
      // we don't have to deal with keytag columns here.
      //
      // Partitioning Keys Desc is not set and returned for traf tables.
      // -------------------------------------------------------------------
      const TrafDesc *partitioning_keys_desc = NULL;

      // the key columns that build the salt column for HBase table
      NAColumnArray hbaseSaltOnColumns(CmpCommon::statementHeap());

      if (partitioning_keys_desc)
        {
	  keys_desc = partitioning_keys_desc;
          while (keys_desc)
	    {
              Int32 tablecolnumber = keys_desc
                                   ->keysDesc()->tablecolnumber;
              indexColumn = colArray.getColumn(tablecolnumber);
              partitioningKeyColumns.insert(indexColumn);
              SortOrdering order = keys_desc
                ->keysDesc()->isDescending() ?
                DESCENDING : ASCENDING;
	      partitioningKeyColumns.setAscending
                                       (partitioningKeyColumns.entries() - 1,
				        order == ASCENDING);
              keys_desc = keys_desc->next;
            } // end while (keys_desc)
        }
      else {
        partitioningKeyColumns = indexKeyColumns;

         // compute the partition key columns for HASH2 partitioning scheme
         // for a salted HBase table. Later on, we will replace 
         // partitioningKeyColumns with the column list computed here if
         // the desired partitioning schema is HASH2.
         for (CollIndex i=0; i<hbaseSaltColumnList.entries(); i++ )
         {
            ColReference* cRef = (ColReference*)hbaseSaltColumnList[i];
            const NAString& colName = (cRef->getColRefNameObj()).getColName();
            NAColumn *col = allColumns.getColumn(colName.data()) ;
            if (col == NULL) {
                *CmpCommon::diags() << DgSqlCode(-1009)
                                    << DgColumnName(colName);
                return TRUE;
            }
            hbaseSaltOnColumns.insert(col);
         }
      }

      // Create DP2 node map for partitioning function.
      NodeMap* nodeMap = NULL; 

      //increment for each table/index to create unique identifier
      cmpCurrentContext->incrementTableIdent();
      

      // NB: Just in case, we made a call to setupClusterInfo at the
      // beginning of this function.
      TrafDesc * partns_desc;
      Int32 indexLevels = 1;
      Int32 blockSize = currIndexDesc->blocksize;
      // Create fully qualified ANSI name from indexname
      QualifiedName qualIndexName(currIndexDesc->indexname, 1, heap, 
                                  bindWA);
      if (files_desc)
      {
	if( (table->getSpecialType() != ExtendedQualName::VIRTUAL_TABLE AND
	     (NOT table->isHbaseTable()))
	    OR files_desc->filesDesc()->partns_desc )
	  {
            nodeMap = new (heap) NodeMap(heap);
            createNodeMap(files_desc->filesDesc()->partns_desc,
			  nodeMap,
			  heap,
			  table_desc->tableDesc()->tablename,
			  cmpCurrentContext->getTableIdent());
	    tableIdList.insert(CollIndex(cmpCurrentContext->getTableIdent()));
	  }
	// Check whether the index has any remote partitions.
	if (checkRemote(files_desc->filesDesc()->partns_desc,
			currIndexDesc->indexname))
	  hasRemotePartition = TRUE;
	else
	  hasRemotePartition = FALSE;

        // Sol: 10-030703-7600. Earlier we assumed that the table is
	// partitioned same as the indexes, hence we used table partitioning
	// to create partitionining function. But this is not true. Hence
	// we now use the indexes partitioning function
	switch (currIndexDesc->partitioningScheme())
        {
	case COM_ROUND_ROBIN_PARTITIONING :
	  // Round Robin partitioned table
	  partFunc = createRoundRobinPartitioningFunction(
	       files_desc->filesDesc()->partns_desc,
	       nodeMap,
	       heap);
	  break;

	case COM_HASH_V1_PARTITIONING :
	  // Hash partitioned table
	  partFunc = createHashDistPartitioningFunction(
	       files_desc->filesDesc()->partns_desc,
	       partitioningKeyColumns,
	       nodeMap,
	       heap);
	  break;

	case COM_HASH_V2_PARTITIONING :
	  // Hash partitioned table
	  partFunc = createHash2PartitioningFunction(
	       files_desc->filesDesc()->partns_desc,
	       partitioningKeyColumns,
	       nodeMap,
	       heap);

          partitioningKeyColumns = hbaseSaltOnColumns;

	  break;

	case COM_UNSPECIFIED_PARTITIONING :
	case COM_NO_PARTITIONING :
	case COM_RANGE_PARTITIONING :
	case COM_SYSTEM_PARTITIONING :
	  {
              // Do Hash2 only if the table is salted orignally.
              if ( doHash2 )
                doHash2 = (numOfSaltedPartitions > 0);

              if ( doHash2 ) 
                {
                  partFunc = createHash2PartitioningFunctionForHBase(
                       NULL, 
                       table,
                       numOfSaltedPartitions,
                       heap);
                  
                  partitioningKeyColumns = hbaseSaltOnColumns;
                }
              else
                {
                  // need hbase region descs for range partitioning for 
                  // the following cases:
                  //   -- cqd HBASE_HASH2_PARTITIONING is OFF
                  //   -- or num salted partitions is 0
                  //   -- or load command is being processed
                  // If not already present, generate it now.
                  NABoolean genHbaseRegionDesc = FALSE;
                  TrafTableDesc* tDesc = table_desc->tableDesc(); 
                  if (((! currIndexDesc->hbase_regionkey_desc) &&
                       (table->getSpecialType() != ExtendedQualName::VIRTUAL_TABLE) &&
                       (NOT table->isSeabaseMDTable()) &&
                       (NOT table->isHistogramTable())) &&
                      ((currIndexDesc->numSaltPartns == 0) ||
                       (CmpCommon::getDefault(HBASE_HASH2_PARTITIONING) == DF_OFF) ||
                       (bindWA && bindWA->isTrafLoadPrep())))
                    genHbaseRegionDesc = TRUE;
                  else
                    genHbaseRegionDesc = FALSE;
                  
                  if (genHbaseRegionDesc) 
                    {
                      CmpSeabaseDDL cmpSBD((NAHeap *)heap);
                      cmpSBD.genHbaseRegionDescs
                        (currIndexDesc,
                         qualIndexName.getCatalogName(),
                         qualIndexName.getSchemaName(),
                         qualIndexName.getObjectName(),
                         ((NATable *)table)->getNamespace());

                      if(currIndexDesc->keytag == 0)
                        tDesc->hbase_regionkey_desc = 
                          currIndexDesc->hbase_regionkey_desc;
                    }

                  if ((! tDesc->hbase_regionkey_desc) &&
                      ((table->getSpecialType() == ExtendedQualName::VIRTUAL_TABLE) ||
                       (table->isSeabaseMDTable()) ||
                       (table->isHistogramTable())))
                    {
                      partFunc = createRangePartitioningFunction(
                           files_desc->filesDesc()->partns_desc,
                           partitioningKeyColumns,
                           nodeMap,
                           heap);                      
                    }
                  else
                    {
                      partFunc = createRangePartitioningFunctionForHBase(
                           currIndexDesc->hbase_regionkey_desc,
                           table,
                           partitioningKeyColumns,
                           heap);
                    }
                } // else
	    break;
	  }
	case COM_UNKNOWN_PARTITIONING:
	  {
            *CmpCommon::diags() << DgSqlCode(-4222)
                                << DgString0("Unsupported partitioning");
	    return TRUE;
	  }
	default:
	  CMPASSERT_STRING(FALSE, "Unhandled switch statement");
        }

        // Check to see if the partitioning function was created
        // successfully.  An error could occur if one of the
        // partitioning keys is an unsupported type or if the table is
        // an MP Table and the first key values contain MP syntax that
        // is not supported by MX.  The unsupported types are the
        // FRACTION only SQL/MP Datetime types.  An example of a
        // syntax error is a Datetime literal which does not have the
        // max number of digits in each field. (e.g. DATETIME
        // '1999-2-4' YEAR TO DAY)
        //
        if (partFunc == NULL) {
          return TRUE;
        }

        // currently we save the indexLevels in the fileset. Since there
        // is a indexLevel for each file that belongs to the fileset,
        // we get the biggest of this indexLevels and save in the fileset.
        partns_desc = files_desc->filesDesc()->partns_desc;
	if(partns_desc)
	  {
	    while (partns_desc)
	      {
		if ( indexLevels < partns_desc->partnsDesc()->indexlevel)
		  indexLevels = partns_desc->partnsDesc()->indexlevel;
		partns_desc = partns_desc->next;
	      }

	  }
      }

      // add a new access path
      //
      // $$$ The estimated number of records should be available from
      // $$$ a FILES descriptor. If it is not available, it may have
      // $$$ to be computed by examining the EOFs of each individual
      // $$$ file that belongs to the file set.

    
      // This ext_indexname is expected to be set up correctly as an
      // EXTERNAL-format name (i.e., dquoted if any delimited identifiers)
      // by sqlcat/read*.cpp.  The ...AsAnsiString() is just-in-case (MP?).
      NAString extIndexName(
	   qualIndexName.getQualifiedNameAsAnsiString(),
	   CmpCommon::statementHeap());

      QualifiedName qualExtIndexName;
      if (table->getSpecialType() != ExtendedQualName::VIRTUAL_TABLE)
	qualExtIndexName = QualifiedName(extIndexName, 1, heap, bindWA);
      else
	qualExtIndexName = qualIndexName;

      // for volatile tables, set the object part as the external name.
      // cat/sch parts are internal and should not be shown.
      if (currIndexDesc->isVolatile())
	{
	  ComObjectName con(extIndexName);
	  extIndexName = con.getObjectNamePartAsAnsiString();
	}

      if (partFunc)
	numberOfFiles = partFunc->getCountOfPartitions();

      CMPASSERT(currIndexDesc->blocksize > 0);

      NAList<HbaseCreateOption*>* hbaseCreateOptions = NULL;
      if ((currIndexDesc->hbaseCreateOptions) &&
          (CmpSeabaseDDL::genHbaseCreateOptions
           (currIndexDesc->hbaseCreateOptions,
            hbaseCreateOptions,
            heap,
            NULL,
            NULL)))
        return TRUE;

      if (table->isHbaseTable())
      {
        indexLevels = hbtIndexLevels;
        blockSize = hbtBlockSize;
      }

      TrafDesc * localKeysDesc = NULL;
      if (currIndexDesc->keys_desc)
        {
          // make a local copy of keys_desc in 'heap'
          localKeysDesc = 
            TrafDesc::copyDescList(currIndexDesc->keys_desc, heap);
        }

      newIndex = new (heap)
	NAFileSet(
		  qualIndexName, // QN containing "\NSK.$VOL", FUNNYSV, FUNNYNM
		  qualExtIndexName, // :
		  extIndexName,	 // string containing Ansi name CAT.SCH."indx"
		  KEY_SEQUENCED_FILE,
		  isSystemTable,
		  numberOfFiles,
		  100,
                  currIndexDesc->record_length,
                  blockSize,
		  indexLevels,
		  allColumns,
		  indexKeyColumns,
		  partitioningKeyColumns,
                  noHiveSortColumns,
		  partFunc,
		  currIndexDesc->keytag,
                  0, 
                  files_desc ? files_desc->filesDesc()->isAudited() : 0,
                  0,
                  0,
                  COM_NO_COMPRESSION,
                  0,
                  0,
                  0,
                  isPacked,
                  hasRemotePartition,
                  ((currIndexDesc->keytag != 0) &&
                   (currIndexDesc->isUnique())),
                  currIndexDesc->isNgram(), //ngram
                  currIndexDesc->isPartLocalBaseIndex(),
                  currIndexDesc->isPartLocalIndex(),
                  currIndexDesc->isPartGlobalIndex(),
                  0,
                  0,
                  (currIndexDesc->isVolatile()),
                  (currIndexDesc->isInMemoryObject()),
                  currIndexDesc->indexUID,
                  localKeysDesc,
                  NULL, // no Hive stats
                  currIndexDesc->numSaltPartns,
                  currIndexDesc->numInitialSaltRegions,
                  numTrafReplicas,
                  hbaseCreateOptions,
                  heap);
      
      if (isNotAvailable)
         newIndex->setNotAvailable(TRUE);

       newIndex->setRowFormat(currIndexDesc->rowFormat());
       // Mark each NAColumn in the list
       indexKeyColumns.setIndexKey();
       if ((table->isHbaseTable()) && (currIndexDesc->keytag != 0))
         saveNAColumns.setIndexKey();

       if (currIndexDesc->isExplicit())
         newIndex->setIsCreatedExplicitly(TRUE);

       //if index is unique and is on one column, then mark column as unique
       if ((currIndexDesc->isUnique()) &&
           (indexKeyColumns.entries() == 1))
         indexKeyColumns[0]->setIsUnique();

       partitioningKeyColumns.setPartitioningKey();

       indexes.insert(newIndex);

       //
       // advance to the next index
       //
       if (isTheClusteringKey)
         {
           clusteringIndex = newIndex; // >>>> RETURN VALUE
           // switch to the alternate indexes by starting over again
           isTheClusteringKey = FALSE;
           indexes_desc = table_desc->tableDesc()->indexes_desc;
         }
       else
         {
           // simply advance to the next in the list
           indexes_desc = indexes_desc->next;
         }

       // skip the clustering index, if we encounter it again
       if (indexes_desc AND !indexes_desc->indexesDesc()->keytag)
         indexes_desc = indexes_desc->next;
     } // end while (indexes_desc)

  // logic related to indexes hiding
   return FALSE;
 } // static createNAFileSets()

// for Hive tables
static
NABoolean createNAFileSets(hive_tbl_desc* hvt_desc        /*IN*/,
                           TrafDesc* extTableDesc         /*IN*/,
                           const NATable * table          /*IN*/,
                           const NAColumnArray & colArray /*IN*/,
                           NAFileSetList & indexes        /*OUT*/,
                           NAFileSetList & vertParts      /*OUT*/,
                           NAFileSet * & clusteringIndex  /*OUT*/,
			   LIST(CollIndex) & tableIdList  /*OUT*/,
                           NAMemory* heap,
                           BindWA * bindWA,
			   NABoolean& isORC,
                           NABoolean& isParquet,
                           NABoolean& isAvro,
			   Int32 *maxIndexLevelsPtr = NULL)
{
  NABoolean isTheClusteringKey = TRUE;
  NABoolean isVerticalPartition;
  NABoolean hasRemotePartition = FALSE;
  CollIndex numClusteringKeyColumns = 0;

  // only one set of key columns to handle for hive

      Lng32 numberOfFiles = 1;	  	// always at least 1
      //      NAColumn * indexColumn;		// an index/VP key column
      NAFileSet * newIndex;		// a new file set

      // all columns that belong to an index
      NAColumnArray allColumns(CmpCommon::statementHeap());

      // the index key columns
      NAColumnArray indexKeyColumns(CmpCommon::statementHeap());

      // the BUCKETING columns
      NAColumnArray bucketingKeyColumns(CmpCommon::statementHeap());

      // the sort columns for bucketed tables
      NAColumnArray sortKeyColumns(CmpCommon::statementHeap());

      PartitioningFunction * partFunc = NULL;
      // is this an index or is it really a VP?

      isVerticalPartition = FALSE;
      NABoolean isPacked = FALSE;

      NABoolean isNotAvailable = FALSE;

      // ---------------------------------------------------------------------
      // loop over the clustering key columns of the index
      // ---------------------------------------------------------------------
      const hive_bkey_desc *hbk_desc = hvt_desc->getBucketingKeys();

      Int32 numBucketingColumns = 0;

      while (hbk_desc)
	{
          NAString colName(hbk_desc->name_);
          colName.toUpper();

          NAColumn* bucketingColumn = colArray.getColumn(colName);

          if ( bucketingColumn ) {
	     bucketingKeyColumns.insert(bucketingColumn);
             numBucketingColumns++;
          }

	  hbk_desc = hbk_desc->next_;
	} // end while (hvk_desc)

      const hive_skey_desc *hsk_desc = hvt_desc->getSortKeys();

      // handle sorted columns
      while (hsk_desc)
        {
          NAString colName(hsk_desc->name_);
          colName.toUpper();

          NAColumn* sortKeyColumn = colArray.getColumn(colName);

          if (sortKeyColumn)
            {
              sortKeyColumns.insert(sortKeyColumn);
              if (hsk_desc->orderInt_ == 0)
                sortKeyColumns.setAscending(sortKeyColumns.entries()-1,FALSE);
            }

          hsk_desc = hsk_desc->next_;
        }

      // Hive tables don't enforce a unique key, but we can pick virtual
      // columns to form a unique key (this can also be used internally,
      // e.g. for subquery unnesting).
      createHiveKeyColumns(colArray,
                           indexKeyColumns,
                           CmpCommon::statementHeap());

      // ---------------------------------------------------------------------
      // Loop over the non key columns. 
      // ---------------------------------------------------------------------
      for (CollIndex i=0; i<colArray.entries(); i++)
	{
	  allColumns.insert(colArray[i]);
	}

      //increment for each table/index to create unique identifier
      cmpCurrentContext->incrementTableIdent();

      // collect file stats from HDFS for the table
      const hive_sd_desc *sd_desc = hvt_desc->getSDs();

      HHDFSTableStats * hiveHDFSTableStats = NULL;

      {
{
              hiveHDFSTableStats = new(heap) HHDFSTableStats(heap);
              hiveHDFSTableStats->setPortOverride(CmpCommon::getDefaultLong(HIVE_LIB_HDFS_PORT_OVERRIDE));
              // create file-level statistics and estimate total row count and record length
              hiveHDFSTableStats->populate(hvt_desc);
          }
      }

      if (hiveHDFSTableStats->hasError())
        {
          *CmpCommon::diags() << DgSqlCode(-1200)
                              << DgString0(hiveHDFSTableStats->getDiags().getErrMsg())
                              << DgTableName(table->getTableName().getQualifiedNameAsAnsiString());
          return TRUE;
        }
      else if (hiveHDFSTableStats->getDiags().getSkipCount() > 0)
        {
          *CmpCommon::diags() << DgSqlCode(1218)
                              << DgString0(hiveHDFSTableStats->getDiags().getSkippedFiles())
                              << DgTableName(table->getTableName().getQualifiedNameAsAnsiString());
        }



      if ( (hiveHDFSTableStats->isOrcFile()) ||
           (hiveHDFSTableStats->isParquetFile()) ||
           (hiveHDFSTableStats->isAvroFile()) ) {

        if (hiveHDFSTableStats->isOrcFile() &&
            CmpCommon::getDefault(TRAF_ENABLE_ORC_FORMAT) == DF_OFF)
        {
          *CmpCommon::diags() << DgSqlCode(-3069)
                              << DgTableName(table->getTableName().getQualifiedNameAsAnsiString());
          return TRUE;

        }

        if (hiveHDFSTableStats->isParquetFile() &&
            CmpCommon::getDefault(TRAF_ENABLE_PARQUET_FORMAT) == DF_OFF)
        {
          *CmpCommon::diags() << DgSqlCode(-3069)
                              << DgTableName(table->getTableName().getQualifiedNameAsAnsiString());
          return TRUE;

        }

        if (hiveHDFSTableStats->isAvroFile() &&
            CmpCommon::getDefault(TRAF_ENABLE_AVRO_FORMAT) == DF_OFF)
        {
          *CmpCommon::diags() << DgSqlCode(-3069)
                              << DgTableName(table->getTableName().getQualifiedNameAsAnsiString());
          return TRUE;

        }


        if (hiveHDFSTableStats->isOrcFile())
          isORC = TRUE;

        if (hiveHDFSTableStats->isParquetFile())
          isParquet = TRUE;

        if (hiveHDFSTableStats->isAvroFile())
          isAvro = TRUE;

      }

#ifndef NDEBUG
      NAString logFile = 
        ActiveSchemaDB()->getDefaults().getValue(HIVE_HDFS_STATS_LOG_FILE);
      if (logFile.length())
        {
          FILE *ofd = fopen(logFile, "a");
          if (ofd)
            {
              hiveHDFSTableStats->print(ofd);
              fclose(ofd);
            }
        }
      // for release code, would need to sandbox the ability to write
      // files, e.g. to a fixed log directory
#endif

      // Create a node map for partitioning function.
      NodeMap* nodeMap = new (heap) NodeMap(heap, NodeMap::HIVE);

      createNodeMap(hvt_desc,
                  nodeMap,
                  heap,
                  (char*)(table->getTableName().getObjectName().data()),
                  cmpCurrentContext->getTableIdent());
      tableIdList.insert(CollIndex(cmpCurrentContext->getTableIdent()));

      // For the time being, set it up as Hash2 partitioned table
               
      Int32 numBuckets = (hvt_desc->getSDs() ? hvt_desc->getSDs()->buckets_
                           : 0);

      if (numBuckets>1 && bucketingKeyColumns.entries()>0) {
         if ( CmpCommon::getDefault(HIVE_USE_HASH2_AS_PARTFUNCTION) == DF_ON )
            partFunc = createHash2PartitioningFunction
                          (numBuckets, bucketingKeyColumns, nodeMap, heap);
         else
            partFunc = createHivePartitioningFunction
                          (numBuckets, bucketingKeyColumns, nodeMap, heap);
      } else
         partFunc = new (heap)
                       SinglePartitionPartitioningFunction(nodeMap, heap);

      // NB: Just in case, we made a call to setupClusterInfo at the
      // beginning of this function.
      //      desc_struct * partns_desc;
      Int32 indexLevels = 1;
      

      // add a new access path
      //
      // $$$ The estimated number of records should be available from
      // $$$ a FILES descriptor. If it is not available, it may have
      // $$$ to be computed by examining the EOFs of each individual
      // $$$ file that belongs to the file set.

      // Create fully qualified ANSI name from indexname, the PHYSICAL name.
      QualifiedName qualIndexName(
           (char*)(table->getTableName().getObjectName().data()),
           (table->getTableName().getSchemaName().isNull() ? HIVE_SYSTEM_SCHEMA
            : (char*)(table->getTableName().getSchemaName().data())),
           "", heap);

      // This ext_indexname is expected to be set up correctly as an
      // EXTERNAL-format name (i.e., dquoted if any delimited identifiers)
      // by sqlcat/read*.cpp.  The ...AsAnsiString() is just-in-case (MP?).
      NAString extIndexName(
	   qualIndexName.getQualifiedNameAsAnsiString(),
	   CmpCommon::statementHeap());

      QualifiedName qualExtIndexName = QualifiedName(extIndexName, 1, heap, bindWA);


      if (partFunc)
	numberOfFiles = partFunc->getCountOfPartitions();

      Int64 estimatedRC = 0;
      Int64 estimatedRecordLength = 0;

      Lng32 blockSize = (Lng32)hiveHDFSTableStats->getEstimatedBlockSize();

      if ( isORC || isParquet || isAvro ) {
         // We cannot use colArray.getTotalStorageSize() for estimating row length,
         // since we make worst-case assumptions about string column sizes. 
         // (Typically, we assume VARCHAR(31999), which grossly overestimates.)
         // Instead we use the total storage size for non-string columns, then
         // use the average string length as computed from the ORC file statistics.
 
         estimatedRecordLength = 
                          colArray.getTotalStorageSizeForNonChars() +
                          hiveHDFSTableStats->getAvgStringLengthPerRow();

         // The block size is purposely set to CQD NCM_SEQ_IO_WEIGHT so that the 
         // seq IO to scan ORC table is the amount of data scanned.
         blockSize = ActiveSchemaDB()->getDefaults().getAsDouble(NCM_SEQ_IO_WEIGHT);

         // use the information fetched/estimated during populate() call
         estimatedRC = hiveHDFSTableStats->getTotalRows();

      } else if ( sd_desc && (!sd_desc->isTrulyText()) ) {
         //
         // Poor man's estimation by assuming the record length in hive is the 
         // same as SQ's. We can do better once we know how the binary data is
         // stored in hdfs.
         //
         estimatedRecordLength = colArray.getTotalStorageSize();
         estimatedRC = hiveHDFSTableStats->getTotalSize() / estimatedRecordLength;
      } else {
         // use the information estimated during populate() call
         estimatedRC = hiveHDFSTableStats->getEstimatedRowCount();
         estimatedRecordLength = 
           Lng32(MINOF(hiveHDFSTableStats->getEstimatedRecordLength(),
                       hiveHDFSTableStats->getEstimatedBlockSize()-100));
      }
		  
      blockSize = MAXOF(blockSize, (Lng32)estimatedRecordLength);
      
      ((NATable*)table)-> setOriginalRowCount((double)estimatedRC);

      newIndex = new (heap)
	NAFileSet(
		  qualIndexName, // QN containing "\NSK.$VOL", FUNNYSV, FUNNYNM
		  //(indexes_desc->body.indexes_desc.isVolatile ?
		  qualExtIndexName, // :
		  //qualIndexName),
		  extIndexName,	 // string containing Ansi name CAT.SCH."indx"

                  // The real orginization is a hybrid of KEY_SEQ and HASH.
                  // Well, we just take the KEY_SEQ for now.
                  KEY_SEQUENCED_FILE,

		  FALSE, // isSystemTable
		  numberOfFiles,

                  // HIVE-TBD
		  Cardinality(estimatedRC),
                  Lng32(estimatedRecordLength),
		  blockSize,

		  indexLevels, // HIVE-TBD
		  allColumns,
		  indexKeyColumns,
		  bucketingKeyColumns,
                  sortKeyColumns,
		  partFunc,
		  0, // indexes_desc->body.indexes_desc.keytag,

		  hvt_desc->redeftime(), 
                 
		  1, // files_desc->body.files_desc.audit 
		  0, // files_desc->body.files_desc.auditcompress 
		  0, // files_desc->body.files_desc.compressed 
		  COM_NO_COMPRESSION,
		  0, // files_desc->body.files_desc.icompressed 
		  0, // files_desc->body.files_desc.buffered:
		  0, // files_desc->body.files_desc.clearOnPurge: 0,
		  isPacked,
                  hasRemotePartition,
		  0, // not a unique secondary index
		  0, // not a ngram index
                  0,  // not a local base partition index
                  0,  // not a local partition index
                  0,  // not a global partition index
                  0, // isDecoupledRangePartitioned
                  0, // file code
		  0, // not a volatile
		  0, // inMemObjectDefn
                  0,
                  NULL, // indexes_desc->body.indexes_desc.keys_desc,
                  hiveHDFSTableStats,
                  0, // saltPartns
                  0, // initial regions
                  0, // numTrafReplicas
                  NULL, //hbaseCreateOptions
                  heap);

      if (isNotAvailable)
	newIndex->setNotAvailable(TRUE);

      // Mark each NAColumn in the list
      indexKeyColumns.setIndexKey();

      bucketingKeyColumns.setPartitioningKey();


      // If it is a VP add it to the list of VPs.
      // Otherwise, add it to the list of indices.
      indexes.insert(newIndex);

       clusteringIndex = newIndex;


  return FALSE;
} // static createNAFileSets()


 // -----------------------------------------------------------------------
 // Mark columns named in PRIMARY KEY constraint (these will be different
 // from clustering key columns when the PK is droppable), for Binder error 4033.
 // -----------------------------------------------------------------------
 static void markPKCols(const TrafConstrntsDesc * constrnt /*IN*/,
                        const NAColumnArray& columnArray       /*IN*/)
 {
   TrafDesc *keycols_desc = constrnt->constr_key_cols_desc;
   while (keycols_desc)
     {
       TrafConstrntKeyColsDesc *key =
         keycols_desc->constrntKeyColsDesc();
       // Lookup by name (not position: key->position is pos *within the PK*)
       NAColumn *nacol = columnArray.getColumn(key->colname);
       if(nacol != NULL)
         {
           nacol->setPrimaryKey();
           if (((TrafConstrntsDesc*)constrnt)->notSerialized())
             nacol->setPrimaryKeyNotSerialized();
         }

       keycols_desc = keycols_desc->next;
     }
 } // static markPKCols

 // -----------------------------------------------------------------------
 // Insert constraint details into NATable constraint lists
 // -----------------------------------------------------------------------
 static NABoolean
 createConstraintInfo(const TrafDesc * table_desc        /*IN*/,
                      const QualifiedName& tableQualName    /*IN*/,
                      const NAColumnArray& columnArray      /*IN*/,
                      CheckConstraintList& checkConstraints /*OUT*/,
                      AbstractRIConstraintList& uniqueConstraints,
                      AbstractRIConstraintList& refConstraints,
                      NAMemory* heap,
                      BindWA *bindWA)
 {
   TrafDesc *constrnts_desc = table_desc->tableDesc()->constrnts_desc;

   while (constrnts_desc)
     {
       TrafConstrntsDesc *constrntHdr = constrnts_desc->constrntsDesc();

       Int32 minNameParts=3;

       QualifiedName constrntName(constrntHdr->constrntname, minNameParts, (NAMemory*)0, bindWA);

       if (constrntName.numberExpanded() == 0) {
         // There was an error parsing the name of the constraint (see
         // QualifiedName ctor).  Return TRUE indicating an error.
         //
         return TRUE;
       }

       switch (constrntHdr->type)
         {

         case PRIMARY_KEY_CONSTRAINT:
           markPKCols(constrntHdr, columnArray);

         case UNIQUE_CONSTRAINT:	  {

             UniqueConstraint *uniqueConstraint = new (heap)
               UniqueConstraint(constrntName, tableQualName, heap,
                                (constrntHdr->type == PRIMARY_KEY_CONSTRAINT));
             uniqueConstraint->setKeyColumns(constrntHdr, heap);

             uniqueConstraint->setRefConstraintsReferencingMe(constrntHdr, heap, bindWA);
             uniqueConstraints.insert(uniqueConstraint);
         }
         break;
         case REF_CONSTRAINT:
           {
             char *refConstrntName = constrntHdr->referenced_constrnts_desc->
               refConstrntsDesc()->constrntname;
             char *refTableName = constrntHdr->referenced_constrnts_desc->
               refConstrntsDesc()->tablename;

             QualifiedName refConstrnt(refConstrntName, 3, (NAMemory*)0, bindWA);
             QualifiedName refTable(refTableName, 3, (NAMemory*)0, bindWA);

             RefConstraint *refConstraint = new (heap)
               RefConstraint(constrntName, tableQualName,
                             refConstrnt, refTable, heap);

             refConstraint->setKeyColumns(constrntHdr, heap);
             refConstraint->setIsEnforced(constrntHdr->isEnforced());

             refConstraints.insert(refConstraint);
           }
         break;
         case CHECK_CONSTRAINT:
           {
             char *constrntText = constrntHdr->check_constrnts_desc->
                                  checkConstrntsDesc()->constrnt_text;
             checkConstraints.insert(new (heap)
               CheckConstraint(constrntName, constrntText, heap));
           }
           break;
         default:
           CMPASSERT(FALSE);
         }

       constrnts_desc = constrnts_desc->next;
     }

   // return FALSE, indicating no error.
   //
   return FALSE;

 } // static createConstraintInfo()

 ULng32 hashColPosList(const CollIndexSet &colSet)
 {
   return colSet.hash();
 }


 // ----------------------------------------------------------------------------
 // method: lookupObjectUidByName
 //
 // Calls DDL manager to get the object UID for the specified object
 //
 // params:
 //    qualName - name of object to lookup
 //    objectType - type of object
 //    reportError - whether to set diags area when not found
 //
 // returns:
 //   -1 -> error found trying to read metadata including object not found
 //   UID of found object
 //
 // the diags area contains details of any error detected
 //
 // ----------------------------------------------------------------------------      
 static Int64 lookupObjectUidByName( const QualifiedName& qualName
                                     , ComObjectType objectType
                                     , NABoolean reportError
                                     , Int64 *objectFlags = NULL
                                     , Int64 *createTime = NULL
                                     , Int64 *objDataUID = NULL
                                   )
 {
   ExeCliInterface cliInterface(STMTHEAP);
   Int64 objectUID = 0;

   CmpSeabaseDDL cmpSBD(STMTHEAP);
   if (cmpSBD.switchCompiler(CmpContextInfo::CMPCONTEXT_TYPE_META))
     {
       if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_) == 0)
         *CmpCommon::diags() << DgSqlCode( -4400 );

       return -1;
     }

   Int32 objectOwner, schemaOwner;
   Int64 l_objFlags = 0;
   Int64 l_createTime = -1;
   Int64 l_objDataUID = -1;
   objectUID = cmpSBD.getObjectInfo(&cliInterface,
                                    qualName.getCatalogName().data(),
                                    qualName.getSchemaName().data(),
                                    qualName.getObjectName().data(),
                                    objectType,
                                    objectOwner,
                                    schemaOwner,
                                    l_objFlags,
                                    l_objDataUID,
                                    reportError,
                                    FALSE,
                                    &l_createTime, NULL);

   if (objectFlags)
     *objectFlags = l_objFlags;

   if (createTime)
     *createTime = l_createTime;

   if (objDataUID)
    *objDataUID = l_objDataUID;

   cmpSBD.switchBackCompiler();

   return objectUID;
 }

// Object UID for hive tables. 
// 3 cases:
// case 1: external hive table has an object uid in metadata but the hive
//         table itself is not registered.
//         This is the case for external tables created prior to the
//         hive registration change.
//         Return object UID for external table.
// case 2: hive table is registered but there is no external table.
//         Return object UID for the registered table.
// case 3: UIDs for external and registered tables exist in metadata.
//         This can happen if an external table is created with the new
//         change where hive table is automatically registered before
//         creating the external table.
//         Or it can happen if an older external table is explicitly registered
//         with the new code.
//         Return the object UID that was created earlier.
// 
// case 4: For external hbase tables. Get external object uid.
// 
// Set my objectUID_ with the object uid determined based on the previous steps.
NABoolean NATable::fetchObjectUIDForNativeTable(const CorrName& corrName,
                                                NABoolean isView)
 {
   objectUID_ = 0;

   // get uid if this hive table has been registered in traf metadata.
   Int64 regCreateTime = -1;
   Int64 extCreateTime = -1;
   Int64 regObjectUID = -1;
   Int64 extObjectUID = -1;
   Int64 objDataUID = -1;
   if ((corrName.isHive()) || (corrName.isHbase()))
     {
       // first get uid for the registered table/view.
       Int64 objectFlags = 0;

       ComObjectType objType;
       if (isView)
         objType = COM_VIEW_OBJECT;
       else if (corrName.isSpecialTable() && (corrName.getSpecialType() == ExtendedQualName::SCHEMA_TABLE))
         objType = COM_SHARED_SCHEMA_OBJECT;
       else
         objType = COM_BASE_TABLE_OBJECT;
       regObjectUID = lookupObjectUidByName(corrName.getQualifiedNameObj(),
                                            objType, FALSE,
                                            &objectFlags,
                                            &regCreateTime,
                                            &objDataUID);
       
       if (NOT isView)
         {
           // then get uid for corresponding external table.
           NAString adjustedName = ComConvertNativeNameToTrafName
             (corrName.getQualifiedNameObj().getCatalogName(),
              corrName.getQualifiedNameObj().getUnqualifiedSchemaNameAsAnsiString(),
              corrName.getQualifiedNameObj().getUnqualifiedObjectNameAsAnsiString());
           QualifiedName extObjName (adjustedName, 3, STMTHEAP);
           extObjectUID = lookupObjectUidByName(extObjName, 
                                                COM_BASE_TABLE_OBJECT, FALSE,
                                                NULL, &extCreateTime, &objDataUID);
         }

       if ((regObjectUID <= 0) && (extObjectUID > 0))
         {
           // case 1
           objectUID_ = extObjectUID;
         }
       else if ((extObjectUID <= 0) && (regObjectUID > 0))
         {
           // case 2
           objectUID_ = regObjectUID;
         }
       else if ((regObjectUID > 0) && (extObjectUID > 0))
         {
           // case 3
           if (regCreateTime < extCreateTime)
             objectUID_ = regObjectUID;
           else
             objectUID_ = extObjectUID;
         }

       if (regObjectUID > 0) // hive table is registered
         {
           setIsRegistered(TRUE);
           
           if (CmpSeabaseDDL::isMDflagsSet(objectFlags, MD_OBJECTS_INTERNAL_REGISTER))
             setIsInternalRegistered(TRUE);
         }

       if (extObjectUID > 0)
         {
           setHasExternalTable(TRUE);
         }
     } // is hive
   else 
     {
       // case 4. external hbase table.
       NAString adjustedName = ComConvertNativeNameToTrafName
         (corrName.getQualifiedNameObj().getCatalogName(),
          corrName.getQualifiedNameObj().getUnqualifiedSchemaNameAsAnsiString(),
          corrName.getQualifiedNameObj().getUnqualifiedObjectNameAsAnsiString());
       QualifiedName extObjName (adjustedName, 3, STMTHEAP);
       
       objectUID_ = lookupObjectUidByName(extObjName, 
                                          COM_BASE_TABLE_OBJECT, FALSE,
                                          NULL, &extCreateTime, &objDataUID);
       if (objectUID_ > 0)
         setHasExternalTable(TRUE);
     }

    objDataUID_ = objDataUID;

   // If the objectUID is not found, then the table is not registered or 
   // externally defined in Trafodion. Set the objectUID to 0
   // If an unexpected error occurs, then return with the error
   if (objectUID_ <= 0)
     {
       if (CmpCommon::diags()->mainSQLCODE() < 0)
         return FALSE;
       else
         objectUID_ = 0;
     }
   
   return TRUE;
 }

 // -----------------------------------------------------------------------
 // NATable::NATable() constructor
 // -----------------------------------------------------------------------

 const Lng32 initHeapSize = 32 * 1024;		// ## 32K: tune this someday!

ULng32 minmaxKeyHashFunction(const minmaxKey& key)
{
   return key.hash();
}

////////////////////////////////////////////////////////////////////
short NATable::addEncryptionInfo()
{
  ComEncryption::initEncryptionInfo(
       (baseTableUID_.get_value() == 0 ? objectUID_.get_value()
        : baseTableUID_.get_value()),
       (useRowIdEncryption() ? ComEncryption::AES_256_CBC : -1),
       (useDataEncryption() ? ComEncryption::AES_256_CBC : -1),
       encryptionInfo_);

  ComEncryption::setEncryptionKeys(encryptionInfo_);
  
  ComEncryption::setInitVectors(encryptionInfo_);

  return 0;
}

void toIntArray(char *source, Int32 *target,
                char *delimeter, int maxArraySize)
{
  if (source == NULL || target == NULL || maxArraySize <= 0)
    return;

  char def_del[2] = ",";
  char *del = (delimeter == NULL) ? def_del : delimeter;

  char *token = strtok(source, del);
  int index = 0;

  while (token != NULL)
  {
    ((Int32*)target)[index] = aToInt32(token);
    index++;
    if (index >= maxArraySize)
      return;
    token = strtok(NULL, del);
   }
}

void populateColumnIdxArray(char *source, Int32** target,
                            Int32 size, NAMemory *h)
{
  char *idx = new (h) char[str_len(source) + 1];
  strcpy(idx, source);
  *target = new (h)int[size];
  toIntArray(idx, *target, NULL, size);
  if (idx)
     NADELETEBASIC(idx, h);
}
// -----------------------------------------------------------------------
// NATable::NATable() constructor
// -----------------------------------------------------------------------

//const Lng32 initHeapSize = 32 * 1024;		// ## 32K: tune this someday!

NATable::NATable(BindWA *bindWA,
                 const CorrName& corrName,
		 NAMemory *heap,
		 TrafDesc* inTableDesc,
		 UInt32 currentEpoch,
		 UInt32 currentFlags)

  //
  // The NATable heap ( i.e. heap_ ) used to come from ContextHeap
  // (i.e. heap) but it creates high memory usage/leakage in Context
  // Heap. Although the NATables are deleted at the end of each statement,
  // the heap_ is returned to heap (i.e. context heap) which caused
  // context heap containing a lot of not used chunk of memory. So it is
  // changed to be from whatever heap is passed in at the call in
  // NATableDB.getNATable.
  //
  // Now NATable objects can be cached.If an object is to be cached (persisted
  // across statements) a NATable heap is allocated for the object
  // and is passed in (this is done in NATableDB::get(CorrName& corrName...).
  // Otherwise a reference to the Statement heap is passed in. When a cached
  // object is to be deleted the object's heap is deleted which wipes out the
  // NATable object all its related stuff. NATable objects that are not cached
  // are wiped out at the end of the statement when the statement heap is deleted.
  //
  : heap_(heap),
    referenceCount_(0),
    refsIncompatibleDP2Halloween_(FALSE),
    isHalloweenTable_(FALSE),
    qualifiedName_(corrName.getExtendedQualNameObj(),heap),
    synonymReferenceName_(heap),
    fileSetName_(corrName.getQualifiedNameObj(),heap),   // for now, set equal
    clusteringIndex_(NULL),
    colcount_(0),
    colArray_(heap),
    hiveColArray_(heap),
    interestingExpressions_(hashKey, 
                   NAHashDictionary_Default_Size, 
                   TRUE /* enforce uniqueness */,heap),
    recordLength_(0),
    indexes_(heap),
    vertParts_(heap),
    colStats_(NULL),
    statsFetched_(FALSE),
    viewFileName_(NULL),
    viewText_(NULL),
    viewTextInNAWchars_(heap),
    viewTextCharSet_(CharInfo::UnknownCharSet),
    viewCheck_(NULL),
    viewColUsages_(NULL),
    hiveOriginalViewText_(NULL),
    hiveExpandedViewText_(NULL),
    flags_(IS_INSERTABLE | IS_UPDATABLE),
    flags2_(0),
    insertMode_(COM_REGULAR_TABLE_INSERT_MODE),
    isSynonymTranslationDone_(FALSE),
    checkConstraints_(heap),
    createTime_(0),
    redefTime_(0),
    statsTime_(0),
    catalogUID_(0),
    schemaUID_(0),
    objectUID_(0),
    objDataUID_(0),
    baseTableUID_(0),
    objectType_(COM_UNKNOWN_OBJECT),
    partitioningScheme_(COM_UNKNOWN_PARTITIONING),
    uniqueConstraints_(heap),
    refConstraints_(heap),
    isAnMV_(FALSE),
    isAnMVMetaData_(FALSE),
    mvsUsingMe_(heap),
    mvInfo_(NULL),
    accessedInCurrentStatement_(TRUE),
    setupForStatement_(FALSE),
    resetAfterStatement_(FALSE),
    hitCount_(0),
    replacementCounter_(2),
    sizeInCache_(0),
    recentlyUsed_(TRUE),
    tableConstructionHadWarnings_(FALSE),
    isAnMPTableWithAnsiName_(FALSE),
    isUMDTable_(FALSE),
    isSMDTable_(FALSE),
    isMVUMDTable_(FALSE),

    // For virtual tables, we set the object schema version
    // to be the current schema version
    osv_(COM_VERS_CURR_SCHEMA),
    ofv_(COM_VERS_CURR_SCHEMA),
    colsWithMissingStats_(NULL),
    originalCardinality_(-1.0),
    tableIdList_(heap),
    rcb_(NULL),
    rcbLen_(0),
    keyLength_(0),
    parentTableName_(NULL),
    sgAttributes_(NULL),
    isORC_(FALSE),
    isParquet_(FALSE),
    isAvro_(FALSE),
    isHive_(FALSE),
    isHbase_(FALSE),
    isHbaseCell_(FALSE),
    isHbaseRow_(FALSE),
    isSeabase_(FALSE),
    isSeabaseMD_(FALSE),
    isSeabasePrivSchemaTable_(FALSE),
    isUserUpdatableSeabaseMD_(FALSE),
    resetHDFSStatsAfterStmt_(FALSE),
    hiveDefaultStringLen_(0),
    hiveTableId_(-1),
    storedStatsDesc_(NULL),
    privInfo_(NULL),
    privDescs_(NULL),
    secKeySet_(heap),
    newColumns_(heap),
    xnRepl_(COM_REPL_NONE),
    storageType_(COM_STORAGE_HBASE),
    incrBackupEnabled_(FALSE),
    readOnlyEnabled_(FALSE),
    prototype_(NULL),
    columnsDesc_(NULL),
    allColFams_(heap),
    minmaxCache_(minmaxKeyHashFunction, 1, TRUE, heap),
    lobStorageLocation_(NULL),
    numLOBdatafiles_(-1),
    lobChunksTableDataInHbaseColLen_(-1),
    shallowCopied_(FALSE),
    ddlValidationRequired_(FALSE),
    expectedEpoch_(currentEpoch), 
    expectedFlags_(currentFlags),
    userBaseTable_(FALSE),
    partitionType_(NO_PARTITION),
    subpartitionType_(NO_PARTITION),
    partitionColCount_(0),
    subpartitionColCount_(0),
    partitionColIdxAry_(NULL),
    subpartitionColIdxAry_(NULL),
    stlPartitionCnt_(0),
    partArray_(heap),
    partitionInterval_(NULL),
    subpartitionInterval_(NULL),
    partitionAutolist_(0),
    subpartitionAutolist_(0),
    partitionV2Flags_(0)
{
  
  if (corrName.isSeabase() && 
      !(corrName.isSeabaseMD() || corrName.isSeabasePrivMgrMD()) &&
      CmpCommon::getDefault(TRAF_LOCK_DDL) == DF_ON)
    ddlValidationRequired_ = TRUE;

  userBaseTable_ = ComIsUserTable(corrName);

  NAString tblName = qualifiedName_.getQualifiedNameObj().getQualifiedNameAsString();
  NAString mmPhase;

  Lng32 preCreateNATableWarnings = CmpCommon::diags()->getNumber(DgSqlCode::WARNING_);

  //set heap type
  if(heap_ == CmpCommon::statementHeap()){
    heapType_ = STATEMENT;
    mmPhase = "NATable Init (Stmt) - " + tblName;
  }else if (heap_ == CmpCommon::contextHeap()){
    heapType_ = CONTEXT;
    mmPhase = "NATable Init (Cnxt) - " + tblName;
  }else {
    heapType_ = OTHER;
    mmPhase = "NATable Init (Other) - " + tblName;
  }
  

  MonitorMemoryUsage_Enter((char*)mmPhase.data(), heap_, TRUE);

  // Do a metadata read, if table descriptor has not been passed in
  //
  TrafDesc * table_desc = NULL;
  Int32 *maxIndexLevelsPtr = new (STMTHEAP) Int32;
  if (!inTableDesc)
    {
      // lookup from metadata other than HBase is not currently supported
      CMPASSERT(inTableDesc);
    }
  else
    {
      // use the input descriptor to create NATable.
      // Used if 'virtual' tables, like EXPLAIN,
      // DESCRIBE, RESOURCE_FORK, etc are to be created.
      table_desc = inTableDesc;

      // Need to initialize the maxIndexLevelsPtr field
      *maxIndexLevelsPtr = 1;
    }

  if ((corrName.isHbase()) || (corrName.isSeabase()))
    {
      setIsHbaseTable(TRUE); 
      setIsSeabaseTable(corrName.isSeabase());
      setIsHbaseCellTable(corrName.isHbaseCell());
      setIsHbaseRowTable(corrName.isHbaseRow());
      setIsSeabaseMDTable(corrName.isSeabaseMD());

  if (CmpSeabaseDDL::isMDflagsSet
      (table_desc->tableDesc()->objectFlags, MD_OBJECTS_STORED_DESC))
    setStoredDesc(TRUE);
    }

  // Check if the synonym name translation to reference object has been done.
  if (table_desc->tableDesc()->isSynonymTranslationDone())
    {
      isSynonymTranslationDone_ = TRUE;
      NAString synonymReferenceName(table_desc->tableDesc()->tablename);
      synonymReferenceName_ = synonymReferenceName;
      ComUID uid(table_desc->tableDesc()->objectUID);
      synonymReferenceObjectUid_ = uid;
    }
  // Check if it is a UMD table, or SMD table or MV related UMD object
  // and set cll correcsponding flags to indicate this.
  if (table_desc->tableDesc()->isUMDTable())
    {
      isUMDTable_ = TRUE;
    }

  if (table_desc->tableDesc()->isSystemTableCode())
    {
      isSMDTable_ = TRUE;
    }

  if (table_desc->tableDesc()->isMVMetadataObject())
    {
      isMVUMDTable_ = TRUE;
    }

  isTrigTempTable_ = (qualifiedName_.getSpecialType() == ExtendedQualName::TRIGTEMP_TABLE);

  if (table_desc->tableDesc()->isVolatileTable())
    {
      setVolatileTable( TRUE );
    }

  switch(table_desc->tableDesc()->rowFormat())
    {
    case COM_ALIGNED_FORMAT_TYPE:
      setSQLMXAlignedTable(TRUE);
      break;
    case COM_HBASE_FORMAT_TYPE:
    case COM_UNKNOWN_FORMAT_TYPE:
      break;
    case COM_HBASE_STR_FORMAT_TYPE:
      setHbaseDataFormatString(TRUE);
      break;
    }
  if (table_desc->tableDesc()->isInMemoryObject())
    {
      setInMemoryObjectDefn( TRUE );
    }

  if (table_desc->tableDesc()->isDroppable())
    {
      setDroppableTable( TRUE );
    }  

  setStorageType(table_desc->tableDesc()->storageType());
   
  if ((ComReplType)table_desc->tableDesc()->isXnReplSync())    
    setXnRepl(COM_REPL_SYNC);
  else if ((ComReplType)table_desc->tableDesc()->isXnReplAsync())
    setXnRepl(COM_REPL_ASYNC);
   
  if (CmpSeabaseDDL::isMDflagsSet
      (table_desc->tableDesc()->objectFlags, MD_OBJECTS_INCR_BACKUP_ENABLED))
    setIncrBackupEnabled(TRUE);

  if (CmpSeabaseDDL::isMDflagsSet
      (table_desc->tableDesc()->objectFlags, MD_OBJECTS_READ_ONLY_ENABLED))
    setReadOnlyEnabled(TRUE);


  if (corrName.isExternal())
    {
      setIsTrafExternalTable(TRUE);
    }

  if (CmpSeabaseDDL::isMDflagsSet
      (table_desc->tableDesc()->objectFlags, MD_OBJECTS_ENCRYPT_ROWID_FLG))
    setUseRowIdEncryption(TRUE);

  if (CmpSeabaseDDL::isMDflagsSet
      (table_desc->tableDesc()->objectFlags, MD_OBJECTS_ENCRYPT_DATA_FLG))
    setUseDataEncryption(TRUE);

  if (CmpSeabaseDDL::isMDflagsSet
      (table_desc->tableDesc()->objectFlags, MD_OBJECTS_INCLUDE_TRIGGER) &&
      isModuleOpen(LM_TRIGGER))
      {
          setHasTrigger(TRUE);
          // remove triggers from cache associated with this table
          TriggerDB::removeTriggers(getTableName(), COM_INSERT);
      }

  if (CmpSeabaseDDL::isMDflagsSet
      (table_desc->tableDesc()->objectFlags, MD_PARTITION_TABLE_V2))
    if (qualifiedName_.getSpecialType() == ExtendedQualName::INDEX_TABLE)
      setIsPartitionV2IndexTable(TRUE);
    else
      setIsPartitionV2Table(TRUE);
 
  if (CmpSeabaseDDL::isMDflagsSet
      (table_desc->tableDesc()->objectFlags, MD_PARTITION_V2))
    if (qualifiedName_.getSpecialType() == ExtendedQualName::INDEX_TABLE)
      setIsPartitionEntityIndexTable(TRUE);
    else
      setIsPartitionEntityTable(TRUE);

  if (qualifiedName_.getQualifiedNameObj().isHistograms() || 
      qualifiedName_.getQualifiedNameObj().isHistogramIntervals())
    {
      setIsHistogramTable(TRUE);
    }
 
  insertMode_ = table_desc->tableDesc()->insertMode();

  setRecordLength(table_desc->tableDesc()->record_length);
  //
  // Add timestamp information.
  //
  createTime_ = table_desc->tableDesc()->createTime;
  redefTime_  = table_desc->tableDesc()->redefTime;

  catalogUID_ = table_desc->tableDesc()->catUID;
  schemaUID_ = table_desc->tableDesc()->schemaUID;
  objectUID_ = table_desc->tableDesc()->objectUID;
  objDataUID_ = table_desc->tableDesc()->objDataUID;
  baseTableUID_ = table_desc->tableDesc()->baseTableUID;

  // Set the objectUID_ for hbase Cell and Row tables, if the table has
  // been defined in Trafodion use this value, otherwise, set to 0
  if (isHbaseCell_ || isHbaseRow_)
    {
      if ( !fetchObjectUIDForNativeTable(corrName, FALSE) )
        return;
    }

  if (table_desc->tableDesc()->owner)
    {
      Int32 userInfo (table_desc->tableDesc()->owner);
      owner_ = userInfo;
    }
  if (table_desc->tableDesc()->schemaOwner)
    {
      Int32 schemaUser(table_desc->tableDesc()->schemaOwner);
      schemaOwner_ = schemaUser;
    }

  objectType_ = table_desc->tableDesc()->objectType();
  partitioningScheme_ = table_desc->tableDesc()->partitioningScheme();

  if ((table_desc->tableDesc()->objectFlags & SEABASE_OBJECT_IS_EXTERNAL_HIVE) != 0 ||
      (table_desc->tableDesc()->objectFlags & SEABASE_OBJECT_IS_EXTERNAL_HBASE) != 0)
    {
      setIsTrafExternalTable(TRUE);
      
      if (table_desc->tableDesc()->objectFlags & SEABASE_OBJECT_IS_IMPLICIT_EXTERNAL)
        setIsImplicitTrafExternalTable(TRUE);
    }

  // Set up privs
  if ((corrName.getSpecialType() == ExtendedQualName::SG_TABLE) ||
      (corrName.getSpecialType() == ExtendedQualName::SCHEMA_TABLE) ||
      (!(corrName.isSeabaseMD() || corrName.isSpecialTable())))
    getPrivileges(table_desc->tableDesc()->priv_desc, bindWA);

  if (CmpSeabaseDDL::isMDflagsSet
      (table_desc->tableDesc()->tablesFlags, MD_TABLES_HIVE_EXT_COL_ATTRS))
    setHiveExtColAttrs(TRUE);
  if (CmpSeabaseDDL::isMDflagsSet
      (table_desc->tableDesc()->tablesFlags, MD_TABLES_HIVE_EXT_KEY_ATTRS))
    setHiveExtKeyAttrs(TRUE);
  if (table_desc->tableDesc()->default_col_fam)
    {
      defaultUserColFam_ = table_desc->tableDesc()->default_col_fam;
    }

  if (table_desc->tableDesc()->all_col_fams)
    {
      // Space delimited col families.

      string buf; // Have a buffer string
      stringstream ss(table_desc->tableDesc()->all_col_fams); // Insert the string into a stream

      while (ss >> buf)
        {
          allColFams_.insert(buf.c_str());
        }
    }
  else
    allColFams_.insert(defaultUserColFam_);

  if ((NOT defaultUserColFam_.isNull()) &&
      (defaultUserColFam_ != SEABASE_DEFAULT_COL_FAMILY))
    {
      CollIndex idx = allColFams_.index(defaultUserColFam_);
      CmpSeabaseDDL::genTrafColFam(idx, defaultTrafColFam_);
    }
  else
    defaultTrafColFam_ = SEABASE_DEFAULT_COL_FAMILY;

  if (table_desc->tableDesc()->tableNamespace)
    tableNamespace_ = table_desc->tableDesc()->tableNamespace;

  TrafDesc * files_desc = table_desc->tableDesc()->files_desc;

  // currently, packed stats are only created for regular user seabase tables.
  if ((isSeabaseTable()) &&
      (NOT isSeabaseMDTable()) &&
      (NOT isHistogramTable()) &&
      (NOT isSeabasePrivSchemaTable()) &&
      ((getSpecialType() == ExtendedQualName::NORMAL_TABLE) ||
       (getSpecialType() == ExtendedQualName::INDEX_TABLE)) &&
      (table_desc->tableDesc()->table_stats_desc) &&
      (table_desc->tableDesc()->table_stats_desc->tableStatsDesc()))
    {
      storedStatsDesc_ =
        TrafDesc::copyDescList(
             table_desc->tableDesc()->table_stats_desc->tableStatsDesc(), 
             heap);
    }

  // create column descs for seabase tables
  if ((isSeabaseTable()) &&
      (table_desc->tableDesc()->columns_desc))
    {
      // make a local copy of columns_desc in 'heap'
      columnsDesc_ = 
        TrafDesc::copyDescList(table_desc->tableDesc()->columns_desc, heap);
    }
 
  //
  // Insert a NAColumn in the colArray_ for this NATable for each
  // columns_desc from the ARK SMD. Returns TRUE if error creating NAColumns.
  //
  if (createNAColumns(table_desc->tableDesc()->columns_desc,
                      this,
                      table_desc->tableDesc()->tablesFlags,
                      colArray_ /*OUT*/,
                      heap_))
    //coverity[leaked_storage]
    return; // colcount_ == 0 indicates an error

  char  lobHdfsServer[256] ; // max length determined by dfs.namenode.fs-limits.max-component-length(255)
  memset(lobHdfsServer,0,256);
  strncpy(lobHdfsServer,CmpCommon::getDefaultString(LOB_HDFS_SERVER), sizeof(lobHdfsServer)-1);// leave a NULL terminator at the end.
  Int32 lobHdfsPort = (Lng32)CmpCommon::getDefaultNumeric(LOB_HDFS_PORT);

  setNumLOBdatafiles(-1);


  //
  // Add view information, if this is a view
  //
  TrafDesc *view_desc = table_desc->tableDesc()->views_desc;
  if (view_desc)
    {
      viewText_ = new (heap_) char[strlen(view_desc->viewDesc()->viewtext) + 2];
      strcpy(viewText_, view_desc->viewDesc()->viewtext);
      strcat(viewText_, ";");

      viewTextCharSet_ = (CharInfo::CharSet)view_desc->viewDesc()->viewtextcharset;

      viewCheck_    = NULL; //initialize
      if(view_desc->viewDesc()->viewchecktext){
        UInt32 viewCheckLength = str_len(view_desc->viewDesc()->viewchecktext)+1;
        viewCheck_ = new (heap_) char[ viewCheckLength];
        memcpy(viewCheck_, view_desc->viewDesc()->viewchecktext,
               viewCheckLength);
      }

      viewColUsages_ = NULL;
      if(view_desc->viewDesc()->viewcolusages){
        viewColUsages_ = new (heap_) NAList<ComViewColUsage *>(heap_); //initialize empty list
        char * beginStr (view_desc->viewDesc()->viewcolusages);
        char * endStr = strchr(beginStr, ';');
        while (endStr != NULL) {
          ComViewColUsage *colUsage = new (heap_) ComViewColUsage;
          NAString currentUsage(beginStr, endStr - beginStr + 1); 
          colUsage->unpackUsage (currentUsage.data());
          viewColUsages_->insert(colUsage);
          beginStr = endStr+1;
          endStr = strchr(beginStr, ';');
        }
      }
      setUpdatable(view_desc->viewDesc()->isUpdatable());
      setInsertable(view_desc->viewDesc()->isInsertable());

      //
      // The updatable flag is false for an MP view only if it is NOT a
      // protection view. Therefore updatable == FALSE iff it is a
      // shorthand view. See ReadTableDef.cpp, l. 3379.
      //

      viewFileName_ = NULL;
      CMPASSERT(view_desc->viewDesc()->viewfilename);
      UInt32 viewFileNameLength = str_len(view_desc->viewDesc()->viewfilename) + 1;
      viewFileName_ = new (heap_) char[viewFileNameLength];
      memcpy(viewFileName_, view_desc->viewDesc()->viewfilename,
             viewFileNameLength);
    }
  else
    {
      //keep track of memory used by NAFileSets
      Lng32 preCreateNAFileSetsMemSize = heap_->getAllocSize();

      //
      // Process indexes and vertical partitions for this table.
      //
      if (createNAFileSets(table_desc       /*IN*/,
                           this             /*IN*/,
                           colArray_        /*IN*/,
                           indexes_         /*OUT*/,
                           vertParts_       /*OUT*/,
                           clusteringIndex_ /*OUT*/,
			   tableIdList_     /*OUT*/,
                           heap_,
                           bindWA,
                           newColumns_,     /*OUT*/
			   maxIndexLevelsPtr)) {
        return; // colcount_ == 0 indicates an error
      }
            
      // Add constraint info.
      //
      // This call to createConstraintInfo, calls the parser on
      // the constraint name
      //
      
      NABoolean  errorOccurred =
        createConstraintInfo(table_desc        /*IN*/,
                             getTableName()    /*IN*/,
                             getNAColumnArray()/*IN (some columns updated)*/,
                             checkConstraints_ /*OUT*/,
                             uniqueConstraints_/*OUT*/,
                             refConstraints_   /*OUT*/,
                             heap_,
                             bindWA);
      
      if (errorOccurred) {
        // return before setting colcount_, indicating that there
        // was an error in constructing this NATable.
        //
        return;
      }
      
      //
      // FetchHistograms call used to be here -- moved to getStatistics().
      //
    }
  
  // change partFunc for base table if PARTITION clause has been used
  // to limit the number of partitions that will be accessed.
  if ((qualifiedName_.isPartitionNameSpecified()) ||
      (qualifiedName_.isPartitionRangeSpecified())) {
    if (filterUnusedPartitions(corrName.getPartnClause())) {
      return ;
    }
  }

  //
  // Set colcount_ after all possible errors (Binder uses nonzero colcount
  // as an indicator of valid table definition).
  //
  CMPASSERT(table_desc->tableDesc()->colcount >= 0);   // CollIndex cast ok?
  colcount_ = (CollIndex)table_desc->tableDesc()->colcount;

  // If there is a host variable associated with this table, store it
  // for use by the generator to generate late-name resolution information.
  //
  HostVar *hv = corrName.getPrototype();
  prototype_ = hv ? new (heap_) HostVar(*hv) : NULL;

  // MV
  // Initialize the MV support data members
  isAnMV_           = table_desc->tableDesc()->isMVTable();
  isAnMVMetaData_   = table_desc->tableDesc()->isMVMetadataObject();
  mvAttributeBitmap_.initBitmap(table_desc->tableDesc()->mvAttributesBitmap);

  TrafDesc *mvs_desc = NULL; // using mvs not set or returned for traf tables
  // Memory Leak
  while (mvs_desc)
    {
      TrafUsingMvDesc* mv = mvs_desc->usingMvDesc();

      UsingMvInfo *usingMv = new(heap_)
        UsingMvInfo(mv->mvName, mv->refreshType(), mv->rewriteEnabled,
                    mv->isInitialized, heap_);
      mvsUsingMe_.insert(usingMv);

      mvs_desc = mvs_desc->next;
    }

  // ++MV

  // fix the special-type for MV objects. There are case where the type is
  // set to NORMAL_TABLE although this is an MV.
  //
  // Example:
  // --------
  // in the statement "select * from MV1" mv1 will have a NORMAL_TABLE
  // special-type, while in "select * from table(mv_table MV1)" it will
  // have the MV_TABLE special-type.

  if (isAnMV_)
    {
      switch(qualifiedName_.getSpecialType())
        {
        case ExtendedQualName::GHOST_TABLE:
          qualifiedName_.setSpecialType(ExtendedQualName::GHOST_MV_TABLE);
          break;
        case ExtendedQualName::GHOST_MV_TABLE:
          // Do not change it
          break;
        default:
          qualifiedName_.setSpecialType(ExtendedQualName::MV_TABLE);
          break;
        }
    }

  // --MV

  // Initialize the sequence generator fields
  TrafDesc *sequence_desc = table_desc->tableDesc()->sequence_generator_desc;
  if (sequence_desc != NULL) {
    TrafSequenceGeneratorDesc *sg_desc = sequence_desc->sequenceGeneratorDesc();

    if (sg_desc != NULL)
      {
        sgAttributes_ = 
          new(heap_) SequenceGeneratorAttributes(
               sg_desc->startValue,
               sg_desc->increment,
               sg_desc->maxValue,
               sg_desc->minValue,
               sg_desc->sgType(),
               (ComSQLDataType)sg_desc->sqlDataType,
               (ComFSDataType)sg_desc->fsDataType,
               sg_desc->cycleOption,
               FALSE,
               sg_desc->objectUID,
               sg_desc->cache,
               sg_desc->nextValue,
               sg_desc->seqOrder,
               0,
               sg_desc->redefTime);
        if (sgAttributes_->isSystemSG())
          {
            Int64 override_seq_timeout = 0;
            //Value of -2 is used for special debugging.
            if ((override_seq_timeout = CmpCommon::getDefaultNumeric(OVERRIDE_GLOBAL_SEQ_TIMEOUT)) != -1)
              sgAttributes_->setSGTimeout(override_seq_timeout);
            else // use the value specified at create sequence time
              sgAttributes_->setSGTimeout(sg_desc->increment); // increment is overloaded to hold the timeout value in the SEQ_GEN metadata table
          }

        ComReplType sgReplType = COM_REPL_NONE;
        if (sg_desc->isXnReplSync())
          sgReplType = COM_REPL_SYNC;
        else if (sg_desc->isXnReplAsync())
          sgReplType = COM_REPL_ASYNC;
        sgAttributes_->setSGReplType(sgReplType);
        
        if ((CmpCommon::getDefault(TRAF_SEQUENCE_USE_DTM) == DF_ON) ||
            (sgAttributes_->getSGSyncRepl()))
          sgAttributes_->setSGUseDtmImpl(TRUE);
      }
  }

  //construct NAPartitions
  TrafDesc *partitionv2_desc = table_desc->tableDesc()->partitionv2_desc;
  if (partitionv2_desc)
  {
    TrafPartitionV2Desc *partv2_desc = partitionv2_desc->partitionV2Desc();
    partitionType_ = partv2_desc->partitionType;
    subpartitionType_ = partv2_desc->subpartitionType;
    partitionColCount_ = partv2_desc->partitionColCount;
    subpartitionColCount_ = partv2_desc->subpartitionColCount;
    stlPartitionCnt_ = partv2_desc->stlPartitionCnt;
    partitionInterval_ = partv2_desc->partitionInterval;
    subpartitionInterval_ = partv2_desc->subpartitionInterval;
    partitionAutolist_ = partv2_desc->partitionAutolist;
    subpartitionAutolist_ = partv2_desc->subpartitionAutolist;
    partitionV2Flags_ = partv2_desc->flags;
   
    LIST(NAString) colNameList(heap_), subColNameList(heap_);
    populateColumnIdxArray(partv2_desc->partitionColIdx, &partitionColIdxAry_,
                           partitionColCount_, heap_); 

    for (int i = 0; i < partitionColCount_; i++)
    {
      colNameList.insert(NAString(colArray_[partitionColIdxAry_[i]]->getColName()));
    }
    if (subpartitionColCount_ > 0)
    {
      populateColumnIdxArray(partv2_desc->subpartitionColIdx, &subpartitionColIdxAry_,
                             subpartitionColCount_, heap_);
      for (int i = 0; i < partitionColCount_; i++)
        subColNameList.insert(NAString(colArray_[subpartitionColIdxAry_[i]]->getColName()));
    }

    TrafPartDesc *part_desc =  partv2_desc->part_desc->partDesc();
    NABoolean rc = createNAPartitions(part_desc, stlPartitionCnt_,
                                     colNameList, partArray_, heap_);
    if (rc)
      return;

    if (subpartitionColCount_ > 0)
    {
      TrafDesc* part_desc_list = part_desc;
      for (int i = 0; i < stlPartitionCnt_; i++)
      {
       if (part_desc_list != NULL)
       {
         TrafPartDesc* pdesc = part_desc_list->partDesc();
         if (pdesc->subpartitionCnt > 0)
         {
           NAPartitionArray **subp = partArray_[i]->getSubPartitionsPtr();
           *subp = new (heap_)NAPartitionArray(heap_);
           rc = createNAPartitions(pdesc->subpart_desc,
                                   pdesc->subpartitionCnt,
                                   subColNameList, *(*subp), heap_);
           if (rc)
             return;
           
         }
       }
       part_desc_list = part_desc_list->next;
      }
    }
  }

#ifndef NDEBUG
  if (getenv("NATABLE_DEBUG"))
    {
      cout << "NATable " << (void*)this << " "
           << qualifiedName_.getQualifiedNameObj().getQualifiedNameAsAnsiString() << " "
           << (Int32)qualifiedName_.getSpecialType() << endl;
      colArray_.print();
    }
#endif
  //this guy is cacheable
  if((qualifiedName_.isCacheable())&&
     (NOT (isHbaseTable())) && 
     //this object is not on the statement heap (i.e. it is being cached)
     ((heap_ != CmpCommon::statementHeap())||
      (OSIM_runningInCaptureMode())))
    {
      char * nodeName = NULL;
      char * catStr = NULL;
      char * schemaStr = NULL;
      char * fileStr = NULL;
      short nodeNameLen = 0;
      Int32 catStrLen = 0;
      Int32 schemaStrLen = 0;
      Int32 fileStrLen = 0;
      int_32 primaryNodeNum=0;
      short error = 0;

      //clusteringIndex has physical filename that can be used to check
      //if a catalog operation has been performed on a table.
      //Views don't have clusteringIndex, so we get physical filename
      //from the viewFileName_ datamember.
      if(viewText_)
        {
          //view filename starts with node name
          //filename is in format \<node_name>.$<volume>.<subvolume>.<file>
          //catStr => <volume>
          //schemaStr => <subvolume>
          //fileStr => <file>
          nodeName = viewFileName_;
          catStr = nodeName;

          //skip over node name
          //measure node name length
          //get to begining of volume name
          //Measure length of node name
          //skip over node name i.e. \MAYA, \AZTEC, etc
          //and get to volume name
          while((nodeNameLen < 8) && (nodeName[nodeNameLen]!='.')){
            catStr++;
            nodeNameLen++;
          };

          //skip over '.' and the '$' in volume name
          catStr=&nodeName[nodeNameLen+2];
          schemaStr=catStr;

          //skip over the volume/catalog name
          //while measuring catalog name length
          while((catStrLen < 8) && (catStr[catStrLen]!='.'))
            {
              schemaStr++;
              catStrLen++;
            }

          //skip over the '.'
          schemaStr++;
          fileStr=schemaStr;

          //skip over the subvolume/schema name
          //while measuring schema name length
          while((schemaStrLen < 8) && (schemaStr[schemaStrLen]!='.'))
            {
              fileStr++;
              schemaStrLen++;
            }

          //skip over the '.'
          fileStr++;
          fileStrLen = str_len(fileStr);

          //figure out the node number for the node
          //which has the primary partition.
          primaryNodeNum=0;

          if(!OSIM_runningSimulation())        
            primaryNodeNum = CURRCONTEXT_CLUSTERINFO->mapNodeNameToNodeNum(NAString(nodeName));
        }
      else{
        //get qualified name of the clustering index which should
        //be the actual physical file name of the table
        const QualifiedName fileNameObj = getClusteringIndex()->
          getRandomPartition();
        const NAString fileName = fileNameObj.getObjectName();

        //get schemaName object
        const SchemaName schemaNameObj = fileNameObj.getSchemaName();
        const NAString schemaName = schemaNameObj.getSchemaName();

        //get catalogName object
        //this contains a string in the form \<node_name>.$volume
        const CatalogName catalogNameObj = fileNameObj.getCatalogName();
        const NAString catalogName = catalogNameObj.getCatalogName();
        nodeName = (char*) catalogName.data();
        catStr = nodeName;

        //Measure length of node name
        //skip over node name i.e. \MAYA, \AZTEC, etc
        //and get to volume name
        while((nodeNameLen < 8) && (nodeName[nodeNameLen]!='.')){
          catStr++;
          nodeNameLen++;
        };

        //get volume/catalog name
        //skip ".$"
        catStr=&nodeName[nodeNameLen+2];
        catStrLen = catalogName.length() - (nodeNameLen+2);

        //get subvolume/schema name
        schemaStr = (char *) schemaName.data();
        schemaStrLen = schemaName.length();

        //get file name
        fileStr = (char *) fileName.data();
        fileStrLen = fileName.length();

        //figure out the node number for the node
        //which has the primary partition.
        primaryNodeNum=0;

        primaryNodeNum = CURRCONTEXT_CLUSTERINFO->mapNodeNameToNodeNum(NAString(nodeName));

      }
    }
  Lng32 postCreateNATableWarnings = CmpCommon::diags()->getNumber(DgSqlCode::WARNING_);		
  if(postCreateNATableWarnings != preCreateNATableWarnings)		
    tableConstructionHadWarnings_=TRUE;
   
  // add encryption info
  if (useEncryption())
    {
      addEncryptionInfo();
    }

  initialSize_ = heap_->getAllocSize();
  MonitorMemoryUsage_Exit((char*)mmPhase.data(), heap_, NULL, TRUE);
} // NATable()


// Constructor for a Hive table
NATable::NATable(BindWA *bindWA,
                 const CorrName& corrName,
		 NAMemory *heap,
		 struct hive_tbl_desc* htbl,
                 TrafDesc* extTableDesc)
  //
  // The NATable heap ( i.e. heap_ ) used to come from ContextHeap
  // (i.e. heap) but it creates high memory usage/leakage in Context
  // Heap. Although the NATables are deleted at the end of each statement,
  // the heap_ is returned to heap (i.e. context heap) which caused
  // context heap containing a lot of not used chunk of memory. So it is
  // changed to be from whatever heap is passed in at the call in
  // NATableDB.getNATable.
  //
  // Now NATable objects can be cached.If an object is to be cached (persisted
  // across statements) a NATable heap is allocated for the object
  // and is passed in (this is done in NATableDB::get(CorrName& corrName...).
  // Otherwise a reference to the Statement heap is passed in. When a cached
  // object is to be deleted the object's heap is deleted which wipes out the
  // NATable object all its related stuff. NATable objects that are not cached
  // are wiped out at the end of the statement when the statement heap is deleted.
  //
  : heap_(heap),
    referenceCount_(0),
    refsIncompatibleDP2Halloween_(FALSE),
    isHalloweenTable_(FALSE),
    qualifiedName_(corrName.getExtendedQualNameObj(),heap),
    synonymReferenceName_(heap),
    fileSetName_(corrName.getQualifiedNameObj(),heap),   // for now, set equal
    clusteringIndex_(NULL),
    colcount_(0),
    colArray_(heap),
    hiveColArray_(heap),
    interestingExpressions_(hashKey, 
                   NAHashDictionary_Default_Size, 
                   TRUE /* enforce uniqueness */,heap),
    recordLength_(0),
    indexes_(heap),
    vertParts_(heap),
    colStats_(NULL),
    statsFetched_(FALSE),
    viewFileName_(NULL),
    viewText_(NULL),
    viewTextInNAWchars_(heap),
    viewTextCharSet_(CharInfo::UnknownCharSet),
    viewCheck_(NULL),
    viewColUsages_(NULL),
    hiveOriginalViewText_(NULL),
    hiveExpandedViewText_(NULL),
    flags_(IS_INSERTABLE | IS_UPDATABLE),
    flags2_(0),
    insertMode_(COM_REGULAR_TABLE_INSERT_MODE),
    isSynonymTranslationDone_(FALSE),
    checkConstraints_(heap),
    createTime_(htbl->creationTS_),
    redefTime_(htbl->redeftime()),
    statsTime_(0),
    catalogUID_(0),
    schemaUID_(0),
    objectUID_(0),
    objDataUID_(0),
    baseTableUID_(0),
    objectType_(COM_UNKNOWN_OBJECT),
    partitioningScheme_(COM_UNKNOWN_PARTITIONING),
    uniqueConstraints_(heap),
    refConstraints_(heap),
    isAnMV_(FALSE),
    isAnMVMetaData_(FALSE),
    mvsUsingMe_(heap),
    mvInfo_(NULL),
    accessedInCurrentStatement_(TRUE),
    setupForStatement_(FALSE),
    resetAfterStatement_(FALSE),
    hitCount_(0),
    replacementCounter_(2),
    sizeInCache_(0),
    recentlyUsed_(TRUE),
    tableConstructionHadWarnings_(FALSE),
    isAnMPTableWithAnsiName_(FALSE),
    isUMDTable_(FALSE),
    isSMDTable_(FALSE),
    isMVUMDTable_(FALSE),

    // For virtual tables, we set the object schema version
    // to be the current schema version
    osv_(COM_VERS_CURR_SCHEMA),
    ofv_(COM_VERS_CURR_SCHEMA),
    colsWithMissingStats_(NULL),
    originalCardinality_(-1.0),
    tableIdList_(heap),
    rcb_(NULL),
    rcbLen_(0),
    keyLength_(0),
    parentTableName_(NULL),
    sgAttributes_(NULL),
    isORC_(FALSE),
    isParquet_(FALSE),
    isAvro_(FALSE),
    isHive_(TRUE),
    isHbase_(FALSE),
    isHbaseCell_(FALSE),
    isHbaseRow_(FALSE),
    isSeabase_(FALSE),
    isSeabaseMD_(FALSE),
    isSeabasePrivSchemaTable_(FALSE),
    isUserUpdatableSeabaseMD_(FALSE),
    resetHDFSStatsAfterStmt_(FALSE),
    hiveDefaultStringLen_(0),
    hiveTableId_(htbl->tblID_),
    storedStatsDesc_(NULL),
    secKeySet_(heap),
    privInfo_(NULL),
    privDescs_(NULL),
    newColumns_(heap),
    columnsDesc_(NULL),
    xnRepl_(COM_REPL_NONE),
    storageType_(COM_STORAGE_HBASE),
    prototype_(NULL),
    readOnlyEnabled_(FALSE),
    allColFams_(heap),
    minmaxCache_(minmaxKeyHashFunction, 107, TRUE, heap),
    numLOBdatafiles_(-1),
    lobChunksTableDataInHbaseColLen_(-1),
    shallowCopied_(FALSE),
    ddlValidationRequired_(FALSE), // not used for Hive tables
    expectedEpoch_(0),  // not used for Hive tables
    expectedFlags_(0),   // not used for Hive tables
    userBaseTable_(FALSE), // not used for Hive tables
    partitionType_(NO_PARTITION),
    subpartitionType_(NO_PARTITION),
    partitionColCount_(0),
    subpartitionColCount_(0),
    partitionColIdxAry_(NULL),
    subpartitionColIdxAry_(NULL),
    stlPartitionCnt_(0),
    partArray_(heap),
    partitionInterval_(NULL),
    subpartitionInterval_(NULL),
    partitionAutolist_(0),
    subpartitionAutolist_(0),
    partitionV2Flags_(0)

{
	NAString tblName = qualifiedName_.getQualifiedNameObj().getQualifiedNameAsString();
	NAString mmPhase;

	Lng32 preCreateNATableWarnings = CmpCommon::diags()->getNumber(DgSqlCode::WARNING_);

	//set heap type
	if(heap_ == CmpCommon::statementHeap()){
		heapType_ = STATEMENT;
		mmPhase = "NATable Init (Stmt) - " + tblName;
	}else if (heap_ == CmpCommon::contextHeap()){
		heapType_ = CONTEXT;
		mmPhase = "NATable Init (Cnxt) - " + tblName;
	}else {
		heapType_ = OTHER;
		mmPhase = "NATable Init (Other) - " + tblName;
	}

	MonitorMemoryUsage_Enter((char*)mmPhase.data(), heap_, TRUE);


	isTrigTempTable_ = FALSE;


	insertMode_ = 
		COM_MULTISET_TABLE_INSERT_MODE; // allow dup, to check
	//ComInsertMode::COM_MULTISET_TABLE_INSERT_MODE; // allow dup, to check

	//
	// Add timestamp information.
	//

	// To get from Hive
	/*
	   createTime_ = longArrayToInt64(table_desc->tableDesc()->createtime);
	   redefTime_  = longArrayToInt64(table_desc->tableDesc()->redeftime);
	   */

	// NATable has a schemaUID column, probably should propogate it.
	// for now, set to 0.
	schemaUID_ = 0;

	// Set the objectUID_
	// If the HIVE table has been registered in Trafodion, get the objectUID
	// from Trafodion, otherwise, set it to 0.
	// TBD - does getQualifiedNameObj handle delimited names correctly?
        if ( !fetchObjectUIDForNativeTable(corrName, htbl->isView()) )
          return;

	if ( objectUID_ > 0 )
		setHasExternalTable(TRUE);

	// for HIVE objects, the schema owner and table owner is HIVE_ROLE_ID
	if (CmpCommon::context()->isAuthorizationEnabled())
	{
		owner_ = HIVE_ROLE_ID;
		schemaOwner_ = HIVE_ROLE_ID;
	}
	else
	{
		owner_ = SUPER_USER;
		schemaOwner_ = SUPER_USER;
	}

	objectType_ = htbl->isView() ? COM_VIEW_OBJECT : COM_BASE_TABLE_OBJECT;

	// to check
	partitioningScheme_ = COM_UNKNOWN_PARTITIONING;

	// to check
	rcb_ = 0;
	rcbLen_ = 0;
	keyLength_ = 0;

	//
	// Insert a NAColumn in the colArray_ for this NATable for each
	// columns_desc from the ARK SMD. Returns TRUE if error creating NAColumns.
	//

        NABoolean isORC = FALSE;
        NABoolean isParquet = FALSE;
        NABoolean isAvro = FALSE;
        if (htbl && htbl->getSDs())
          {
            isORC = htbl->getSDs()->isOrcFile();
            setIsORC(isORC);

            isParquet = htbl->getSDs()->isParquetFile();
            setIsParquet(isParquet);

            isAvro = htbl->getSDs()->isAvroFile();
            setIsAvro(isAvro);

          }
	if (createNAColumns(htbl->getColumns(),
                            htbl->getPartKey(),
                            htbl->isView(),
                            this,
                            colArray_ /*OUT*/,
                            heap_))
          return;


	//
	// Set colcount_ after all possible errors (Binder uses nonzero colcount
	// as an indicator of valid table definition).
	//

	// To set it via the new createNAColumns()
	colcount_ = colArray_.entries();

	// compute record length from colArray

	Int32 recLen = 0;
	for ( CollIndex i=0; i<colcount_; i++ ) {
		recLen += colArray_[i]->getType()->getNominalSize();
	} 

	setRecordLength(recLen);

  //
  // Add view information, if this is a native hive view
  //
  if (htbl->isView())
    {
      NAString expandedText(htbl->viewExpandedText_);

      // expanded hive view text quotes table and column names with
      // back single quote (`). It also refers to default hive schema
      // as `default`.

      // replace `default` with "HIVE"
      expandedText = replaceAll(expandedText, "`default`", HIVE_SYSTEM_SCHEMA);

      // replace back quoted string with traf ansi string.
      // This will properly double quote reserved table and column names.
      NAString hiveNormalizedViewText;
      Int32 i = 0;
      const char * ptr = expandedText.data();
      NABoolean quoteSeen = FALSE;
      Int32 quoteStartPos = 0;
      Int32  quoteEndPos = 0;
      while (i < expandedText.length())
        {
          if (ptr[i] == '`')
            {
              if (NOT quoteSeen)
                {
                  quoteSeen = TRUE;
                  quoteStartPos = i;
                }
              else
                {
                  quoteEndPos = i;

                  NAString subs(&ptr[quoteStartPos+1], (quoteEndPos-quoteStartPos-1));
                  subs.toUpper();
                  QualifiedName qn(subs);
                  hiveNormalizedViewText += qn.getUnqualifiedObjectNameAsAnsiString();
                  quoteSeen = FALSE;
                }
            }
          else if (NOT quoteSeen)
            hiveNormalizedViewText += ptr[i];

          i++;
        } // while

      // save original view text
      hiveOriginalViewText_ = new (heap_) char[strlen(htbl->viewOriginalText_) + 1];
      strcpy(hiveOriginalViewText_, htbl->viewOriginalText_);

      // save expanded view text
      hiveExpandedViewText_ = new (heap_) char[strlen(htbl->viewExpandedText_) + 1];
      strcpy(hiveExpandedViewText_, htbl->viewExpandedText_);

      NAString createViewStmt("CREATE VIEW ");

      NAString viewSchName;
      if (strcmp(htbl->schName_, HIVE_DEFAULT_SCHEMA_EXE) == 0)
        viewSchName += HIVE_SYSTEM_SCHEMA;
      else
        viewSchName += htbl->schName_;
      viewSchName.toUpper();
      NAString viewName(htbl->tblName_);
      viewName.toUpper();

      QualifiedName qn(viewName, viewSchName, HIVE_SYSTEM_CATALOG);
      NAString fqViewName = qn.getQualifiedNameAsAnsiString();

      createViewStmt += fqViewName + NAString(" AS ") +
        hiveNormalizedViewText + NAString(";");
        
      Lng32 viewTextLen = createViewStmt.length();
      viewText_ = new (heap_) char[viewTextLen+ 2];
      strcpy(viewText_, createViewStmt.data());

      viewTextCharSet_ = CharInfo::UTF8;

      viewFileName_ = NULL;
      UInt32 viewFileNameLength = str_len(htbl->tblName_) + 1;
      viewFileName_ = new (heap_) char[viewFileNameLength];
      memcpy(viewFileName_, htbl->tblName_, viewFileNameLength);

      setUpdatable(FALSE);
      setInsertable(FALSE);
    }
  else
    {
      if (htbl->isExternalTable())
        setIsHiveExternalTable(TRUE);
      else if (htbl->isManagedTable())
        setIsHiveManagedTable(TRUE);
      
      if (createNAFileSets(htbl             /*IN*/,
                           extTableDesc     /*IN*/,
                           this             /*IN*/,
                           colArray_        /*IN*/,
                           indexes_         /*OUT*/,
                           vertParts_       /*OUT*/,
                           clusteringIndex_ /*OUT*/,
                           tableIdList_     /*OUT*/,
                           heap_,
                           bindWA,
                           isORC,
                           isParquet,
                           isAvro
                           )) {
        colcount_ = 0; // indicates failure
        return;
      }
  
      // HIVE-TBD ignore constraint info creation for now
      setIsORC(isORC);
      
      getPrivileges(NULL, bindWA); 
      
      // If there is a host variable associated with this table, store it
      // for use by the generator to generate late-name resolution information.
      //
      HostVar *hv = corrName.getPrototype();
      prototype_ = hv ? new (heap_) HostVar(*hv) : NULL;
      
      // MV
      // Initialize the MV support data members
      isAnMV_           = FALSE;
      isAnMVMetaData_   = FALSE;
      
      Lng32 postCreateNATableWarnings = CmpCommon::diags()->getNumber(DgSqlCode::WARNING_);
      
      if(postCreateNATableWarnings != preCreateNATableWarnings)
        tableConstructionHadWarnings_=TRUE;
      hiveDefaultStringLen_ = CmpCommon::getDefaultLong(HIVE_MAX_STRING_LENGTH_IN_BYTES);
      
      Int32 hiveDefaultStringLenInBytes = CmpCommon::getDefaultLong(HIVE_MAX_STRING_LENGTH_IN_BYTES);
      if( hiveDefaultStringLenInBytes != 31999 ) 
        hiveDefaultStringLen_ = hiveDefaultStringLenInBytes;
      
      initialSize_ = heap_->getAllocSize();
      MonitorMemoryUsage_Exit((char*)mmPhase.data(), heap_, NULL, TRUE);
    }
} // NATable()


NABoolean NATable::doesMissingStatsWarningExist(CollIndexSet & colsSet) const
{
	return colsWithMissingStats_->contains(&colsSet);
}

NABoolean NATable::insertMissingStatsWarning(CollIndexSet colsSet) const
{
	CollIndexSet * setOfColsWithMissingStats = new (STMTHEAP) CollIndexSet (colsSet);

	Int32 someVar = 1;
	CollIndexSet * result = colsWithMissingStats_->insert(setOfColsWithMissingStats, &someVar);

	if (result == NULL)
		return FALSE;
	else
		return TRUE;
}

char* allocateAndCopyString(char* str, NAMemory* h) 
{
   char* result = NULL;
   if (str) {
      int len = strlen(str);
      result = new (h) char[len+1];
      memcpy(result, str, len);
      result[len] = NULL;
   }
   return result;
}

Int32* allocateAndCopyInt32(Int32* source, Int32 size, NAMemory* h)
{
  if (source == NULL || size == 0)
    return NULL;

  Int32* result = new(h)int[size];
  for (int i = 0; i < size; i++)
    result[i] = source[i];

  return result;
}

// copy cstr
// Unless stated otherwise in this method, data members 
// are deep copied.
NATable::NATable(const NATable &other, NAMemory *h, NATableHeapType heapType)
  : 
  initialSize_(other.initialSize_),
  sizeAfterLastStatement_(other.sizeAfterLastStatement_),
  heap_(h), //shallow copied
  heapType_(heapType),
  referenceCount_(0),
  refsIncompatibleDP2Halloween_(other.refsIncompatibleDP2Halloween_),
  isHalloweenTable_(other.isHalloweenTable_),
  flags_(other.flags_),
  flags2_(other.flags2_),
  qualifiedName_(other.qualifiedName_, h),
  synonymReferenceName_(other.synonymReferenceName_, h),
  isSynonymTranslationDone_(other.isSynonymTranslationDone_),
  synonymReferenceObjectUid_(other.synonymReferenceObjectUid_),
  fileSetName_(other.fileSetName_, h),
  prototype_( (other.prototype_)? new (h) HostVar(*other.prototype_) : NULL),
  colcount_(other.colcount_),
  colArray_(other.colArray_, h), // shallow copied
  hiveColArray_(other.hiveColArray_, h), // shallow copied
  interestingExpressions_(other.interestingExpressions_, h), // shallow copied
  recordLength_(other.recordLength_),

  clusteringIndex_(other.clusteringIndex_ ?  // some data member is shallow copied
                   new (h) NAFileSet(*other.clusteringIndex_, h) : NULL),

  indexes_(other.indexes_, h), // shallow copied
  vertParts_(other.vertParts_, h), // shallow copied

  colStats_(other.colStats_ ? new (h) StatsList(*other.colStats_, h) : NULL), // some data member is shallow copied 
  statsFetched_(other.statsFetched_),
  originalCardinality_(other.originalCardinality_),
  createTime_(other.createTime_),
  redefTime_(other.redefTime_),
  statsTime_(other.statsTime_),

  catalogUID_(other.catalogUID_),
  schemaUID_(other.schemaUID_),
  objectUID_(other.objectUID_),
  objDataUID_(other.objDataUID_),
  baseTableUID_(other.baseTableUID_),
  owner_(other.owner_),
  objectType_(other.objectType_),
  schemaOwner_(other.schemaOwner_),
  partitioningScheme_(other.partitioningScheme_),
  viewFileName_(allocateAndCopyString(other.viewFileName_, h)),
  viewText_(allocateAndCopyString(other.viewText_, h)),
  viewTextInNAWchars_(other.viewTextInNAWchars_, h),
  viewTextCharSet_(other.viewTextCharSet_),
  viewCheck_(allocateAndCopyString(other.viewCheck_, h)), 

  viewColUsages_((other.viewColUsages_) ? // shallow copied
                 new (h) NAList<ComViewColUsage*>(*other.viewColUsages_ , h) : NULL),

  xnRepl_(other.xnRepl_),
  storageType_(other.storageType_),
  incrBackupEnabled_(other.incrBackupEnabled_),
  readOnlyEnabled_(other.readOnlyEnabled_),
  hiveExpandedViewText_(allocateAndCopyString(other.hiveExpandedViewText_, h)),
  hiveOriginalViewText_(allocateAndCopyString(other.hiveOriginalViewText_, h)),

  accessedInCurrentStatement_(other.accessedInCurrentStatement_),
  setupForStatement_(other.setupForStatement_),
  resetAfterStatement_(other.resetAfterStatement_),
  tableConstructionHadWarnings_(other.tableConstructionHadWarnings_),

  isAnMPTableWithAnsiName_(other.isAnMPTableWithAnsiName_),
  isUMDTable_(other.isUMDTable_),
  isSMDTable_(other.isSMDTable_),
  isMVUMDTable_(other.isMVUMDTable_),
  isTrigTempTable_(other.isTrigTempTable_),
  isExceptionTable_(other.isExceptionTable_),
  insertMode_(other.insertMode_),

  checkConstraints_(other.checkConstraints_, h),
  uniqueConstraints_(other.uniqueConstraints_),
  refConstraints_(other.refConstraints_),
  isAnMV_(other.isAnMV_),
  isAnMVMetaData_(other.isAnMVMetaData_),

  mvsUsingMe_(other.mvsUsingMe_),

  mvInfo_((other.mvInfo_) ?
          new (h) MVInfoForDML(*other.mvInfo_, h) : NULL),

  mvAttributeBitmap_(other.mvAttributeBitmap_),

  hitCount_(other.hitCount_),
  replacementCounter_(other.replacementCounter_),
  sizeInCache_(other.sizeInCache_),
  recentlyUsed_(other.recentlyUsed_),

  osv_(other.osv_),
  ofv_(other.ofv_),

  rcb_(other.rcb_), // shared. should be a NULL

  rcbLen_(other.rcbLen_),
  keyLength_(other.keyLength_),

  parentTableName_(allocateAndCopyString(other.parentTableName_, h)),
  //TrafDesc *columnsDesc_;
  columnsDesc_(TrafDesc::copyDescList(other.columnsDesc_, h)),
  //TrafDesc *storedStatsDesc_;
  storedStatsDesc_(TrafDesc::copyDescList(other.storedStatsDesc_, h)),

  // hash table to store all the column positions for which missing
  // stats warning has been generated. We are not storing ValueIdSet
  // of the columns but the column positions. This is because for cases
  // where same base table appears in both the query and the sub-query, the 
  // warning for the same column can appears twice, since the ValueId
  // for the column will be different in the two places. We don't want 
  // that
  colsWithMissingStats_(  // shallow copied
       (other.colsWithMissingStats_) ?
              new (h) NAHashDictionary<CollIndexSet, Int32>(
                                   *other.colsWithMissingStats_,h) : NULL),
              
  tableIdList_(other.tableIdList_, h),

  sgAttributes_((other.sgAttributes_)?
              new (h) SequenceGeneratorAttributes(*other.sgAttributes_, h) : NULL),

  isHive_(other.isHive_),
  isHbase_(other.isHbase_),
  isORC_(other.isORC_),
  isParquet_(other.isParquet_),
  isAvro_(other.isAvro_),
  isHbaseCell_(other.isHbaseCell_),
  isHbaseRow_(other.isHbaseRow_),

  isSeabase_(other.isSeabase_),
  isSeabaseMD_(other.isSeabaseMD_),
  isSeabasePrivSchemaTable_(other.isSeabasePrivSchemaTable_),
  isUserUpdatableSeabaseMD_(other.isUserUpdatableSeabaseMD_),
  resetHDFSStatsAfterStmt_(FALSE),
  hiveDefaultStringLen_(other.hiveDefaultStringLen_),
  hiveTableId_(other.hiveTableId_),

  privDescs_((other.privDescs_) ? // shallow copied
             new (h) PrivMgrDescList(*other.privDescs_, h) : NULL),

  privInfo_(other.privInfo_ ? 
             new (h)PrivMgrUserPrivs(*other.privInfo_) : NULL),

  secKeySet_(other.secKeySet_, h),
  newColumns_(other.newColumns_, h), // shallow copied
  defaultUserColFam_(other.defaultUserColFam_, h),
  defaultTrafColFam_(other.defaultTrafColFam_, h),
  allColFams_(other.allColFams_, h),
  tableNamespace_(other.tableNamespace_,h),
  minmaxCache_(other.minmaxCache_, h),
  encryptionInfo_(other.encryptionInfo_),
  lobStorageLocation_(allocateAndCopyString(other.lobStorageLocation_, h)),
  numLOBdatafiles_(other.numLOBdatafiles_),
  lobChunksTableDataInHbaseColLen_(other.lobChunksTableDataInHbaseColLen_),
  shallowCopied_(TRUE),
  ddlValidationRequired_(other.ddlValidationRequired_),
  expectedEpoch_(other.expectedEpoch_),
  expectedFlags_(other.expectedFlags_),
  userBaseTable_(other.userBaseTable_),
  partitionType_(other.partitionType_),
  subpartitionType_(other.subpartitionType_),
  partitionColCount_(other.partitionColCount_),
  subpartitionColCount_(other.subpartitionColCount_),
  partitionColIdxAry_(allocateAndCopyInt32(other.partitionColIdxAry_,
                      other.partitionColCount_, h)),
  subpartitionColIdxAry_(allocateAndCopyInt32(other.subpartitionColIdxAry_,
                      other.subpartitionColCount_, h)),
  stlPartitionCnt_(other.stlPartitionCnt_),
  partArray_(h), //useless, not deepCopy
  partitionInterval_(allocateAndCopyString(other.partitionInterval_, h)),
  subpartitionInterval_(allocateAndCopyString(other.subpartitionInterval_, h)),
  partitionAutolist_(other.partitionAutolist_),
  subpartitionAutolist_(other.subpartitionAutolist_),
  partitionV2Flags_(other.partitionV2Flags_)
{
  assert(!rcb_);
} // copy cstr
  
NABoolean NATable::operator==(const NATable &other) const
{ 
   if (this == &other)
     return TRUE;

   // Check each data member
   if ( initialSize_ != other.initialSize_  ||
        sizeAfterLastStatement_ != other.sizeAfterLastStatement_ ||
        referenceCount_ != other.referenceCount_ ||
        refsIncompatibleDP2Halloween_ != other.refsIncompatibleDP2Halloween_ ||
        isHalloweenTable_ != other.isHalloweenTable_ ||
        flags_ != other.flags_ ||
        flags2_ != other.flags2_ ||
        !(qualifiedName_ == other.qualifiedName_) ||
        !(synonymReferenceName_ == other.synonymReferenceName_) ||
        isSynonymTranslationDone_ != other.isSynonymTranslationDone_ ||
        !(synonymReferenceObjectUid_ == other.synonymReferenceObjectUid_ )
      )
      return FALSE;

   if ( !(fileSetName_ == other.fileSetName_) ||
        !COMPARE_PTRS(prototype_, other.prototype_) ||
        colcount_ != other.colcount_ ||
        !(colArray_ == other.colArray_) ||
        !(hiveColArray_ == other.hiveColArray_) ||
        !(interestingExpressions_ == other.interestingExpressions_) 
      )
      return FALSE;

   if ( recordLength_ != other.recordLength_ ||
        !COMPARE_PTRS(clusteringIndex_, other.clusteringIndex_) || 
        !(indexes_ == other.indexes_) ||
        !(vertParts_ == other.vertParts_) ||
        !COMPARE_PTRS(colStats_, other.colStats_) ||
        statsFetched_ != other.statsFetched_  ||
        originalCardinality_ != other.originalCardinality_ ||
        createTime_ != other.createTime_ ||
        redefTime_ != other.redefTime_ ||
        statsTime_ != other.statsTime_ ||
        catalogUID_ != other.catalogUID_ ||
        schemaUID_ != other.schemaUID_ ||
        objectUID_ != other.objectUID_ ||
        baseTableUID_ != other.baseTableUID_ ||
        owner_ != other.owner_ ||
        objectType_ != other.objectType_ ||
        schemaOwner_ != other.schemaOwner_ ||
        partitioningScheme_ != other.partitioningScheme_ ||
        !COMPARE_CHAR_PTRS(viewFileName_, other.viewFileName_ ) 
      )
      return FALSE;

   if (
        !COMPARE_CHAR_PTRS(viewText_, other.viewText_) ||
        viewTextInNAWchars_ !=  other.viewTextInNAWchars_ ||
        viewTextCharSet_ != other.viewTextCharSet_ ||
        !COMPARE_CHAR_PTRS(viewCheck_, other.viewCheck_) ||
        !COMPARE_PTRS(viewColUsages_, other.viewColUsages_) ||
        xnRepl_ != other.xnRepl_ ||
        storageType_ != other.storageType_ ||
        incrBackupEnabled_ != other.incrBackupEnabled_ ||
        readOnlyEnabled_ != other.readOnlyEnabled_ ||
        !COMPARE_CHAR_PTRS(hiveExpandedViewText_, other.hiveExpandedViewText_) ||
        !COMPARE_CHAR_PTRS(hiveOriginalViewText_, other.hiveOriginalViewText_)
      )
      return FALSE;

   if (
        accessedInCurrentStatement_ !=  other.accessedInCurrentStatement_ ||
        setupForStatement_ !=  other.setupForStatement_ ||
        resetAfterStatement_ !=  other.resetAfterStatement_ ||
        tableConstructionHadWarnings_ !=  other.tableConstructionHadWarnings_ ||
        isAnMPTableWithAnsiName_ !=  other.isAnMPTableWithAnsiName_ ||
        isUMDTable_ !=  other.isUMDTable_ ||
        isSMDTable_ !=  other.isSMDTable_ ||
        isMVUMDTable_ !=  other.isMVUMDTable_ ||
        isTrigTempTable_ !=  other.isTrigTempTable_ ||
        isExceptionTable_ !=  other.isExceptionTable_ ||
        insertMode_ !=  other.insertMode_ ||
        !(checkConstraints_ == other.checkConstraints_) ||
        !(uniqueConstraints_ == other.uniqueConstraints_) ||
        !(refConstraints_ == other.refConstraints_) 
      )
      return FALSE;

#if 0
   // Data members about MVs are not checked until MVs are supported.
   if (
        isAnMV_ != other.isAnMV_ ||
        isAnMVMetaData_ != other.isAnMVMetaData_ ||
        !(mvsUsingMe_ == other.mvsUsingMe_)  ||
        ! COMPARE_PTRS(mvInfo_, other.mvInfo_) ||  //MVInfoForDML
        !(mvAttributeBitmap_ == other.mvAttributeBitmap_) // ComMvAttributeBitmap
      )
      return FALSE;
#endif

   if (
        hitCount_ != other.hitCount_ ||
        replacementCounter_ != other.replacementCounter_ ||
        sizeInCache_ != other.sizeInCache_ ||
        recentlyUsed_ != other.recentlyUsed_ ||
        osv_ != other.osv_ ||
        ofv_ != other.ofv_
      )
      return FALSE;
      
   if (
        !COMPARE_VOID_PTRS(rcb_, rcbLen_, other.rcb_, other.rcbLen_) ||
        keyLength_ != other.keyLength_ ||
        !COMPARE_CHAR_PTRS(parentTableName_, other.parentTableName_) ||
        !COMPARE_TRAFDESC_PTRS(columnsDesc_, other.columnsDesc_) ||
        !COMPARE_TRAFDESC_PTRS(storedStatsDesc_, other.storedStatsDesc_ ) || 
        !COMPARE_PTRS(colsWithMissingStats_, other.colsWithMissingStats_) || 
        !(tableIdList_ == other.tableIdList_ ) ||
        !COMPARE_PTRS(sgAttributes_, other.sgAttributes_) 
      )
      return FALSE;

   if (
        isHive_ != other.isHive_ ||
        isHbase_ != other.isHbase_ ||
        isORC_ != other.isORC_||
        isParquet_ != other.isParquet_ ||
        isAvro_ != other.isAvro_ ||
        isHbaseCell_ != other.isHbaseCell_ ||
        isHbaseRow_ != other.isHbaseRow_ ||
        isSeabase_ != other.isSeabase_ ||
        isSeabaseMD_ != other.isSeabaseMD_ ||
        isSeabasePrivSchemaTable_ != other.isSeabasePrivSchemaTable_ ||
        isUserUpdatableSeabaseMD_ != other.isUserUpdatableSeabaseMD_ ||
        resetHDFSStatsAfterStmt_ != other.resetHDFSStatsAfterStmt_ ||
        hiveDefaultStringLen_ != other.hiveDefaultStringLen_ ||
        hiveTableId_ != other.hiveTableId_
      )
      return FALSE;

   if (
        !COMPARE_PTRS(privDescs_, other.privDescs_) || 
        !COMPARE_PTRS(privInfo_, other.privInfo_) || //PrivMgrUserPrivs
        !(secKeySet_ == other.secKeySet_ ) ||        //ComSecurityKeySet (aka NASet())
        !(newColumns_ == other.newColumns_) || 
        (defaultUserColFam_ != other.defaultUserColFam_) || 
        (defaultTrafColFam_ != other.defaultTrafColFam_) || 
        !(allColFams_ == other.allColFams_) || 
        (tableNamespace_ != other.tableNamespace_) || 
        !(minmaxCache_ == other.minmaxCache_) || 
        !(encryptionInfo_ == other.encryptionInfo_ ) ||  
        !COMPARE_CHAR_PTRS(lobStorageLocation_, other.lobStorageLocation_) ||
        (numLOBdatafiles_ != other.numLOBdatafiles_)
      )
      return FALSE;

   return TRUE;
}

// This gets called in the Optimizer phase -- the Binder phase will already have
// marked columns that were referenced in the query, so that the ustat function
// below can decide which histograms and histints to leave in the stats list
// and which to remove.
//
StatsList *
NATable::getStatistics(NABoolean useStoredStats)
{
#ifndef NDEBUG
  // this code figures out if our table is a table of interest (that is, not
  // a metadata table or histograms table)
  const NAString & xSchemaName = qualifiedName_.getQualifiedNameObj().getSchemaName();
  const NAString & xObjectName = qualifiedName_.getQualifiedNameObj().getObjectName();
  bool debugStop = false;
  if ((xSchemaName != "_MD_") &&
      (xSchemaName != "_PRIVMGR_MD_") &&
      (xSchemaName != "_TENANT_MD_") &&
      (xObjectName != "SB_HISTOGRAMS") &&
      (xObjectName != "SB_HISTOGRAM_INTERVALS") &&
      (xObjectName != "SB_PERSISTENT_SAMPLES"))
    debugStop = true;  // put debug stop here to skip metadata tables
#endif  // NDEBUG

  if (!statsFetched_)
    {
      // mark the kind of histograms needed for this table's columns
      markColumnsForHistograms();

      NAString tblName = qualifiedName_.getQualifiedNameObj().getQualifiedNameAsString();
      NAString mmPhase = "NATable getStats - " + tblName;
      MonitorMemoryUsage_Enter((char*)mmPhase.data(), NULL, TRUE);

      //trying to get statistics for a new statement allocate colStats_
      colStats_ = new (CmpCommon::statementHeap()) StatsList(CmpCommon::statementHeap());

      // Do not create statistics for tables in schema "TRAFODION._REPOS_"
      bool isReposTable = CmpSeabaseDDL::isREPOSSchema(qualifiedName_.getQualifiedNameObj().getCatalogName(),
              qualifiedName_.getQualifiedNameObj().getSchemaName());

      bool isXdcTable = CmpSeabaseDDL::isXDCSchema(qualifiedName_.getQualifiedNameObj().getCatalogName(),
              qualifiedName_.getQualifiedNameObj().getSchemaName());

      // Do not create statistics on the fly for the following tables
      if (isAnMV() || isUMDTable() ||
          isSMDTable() || isMVUMDTable() ||
          isTrigTempTable() || isReposTable || isXdcTable)
        CURRSTMT_OPTDEFAULTS->setHistDefaultSampleSize(0);

      CURRCONTEXT_HISTCACHE->getHistograms(*this, useStoredStats);

      if ((*colStats_).entries() > 0)
        originalCardinality_ = (*colStats_)[0]->getRowcount();
      else
        originalCardinality_ = ActiveSchemaDB()->getDefaults().getAsDouble(HIST_NO_STATS_ROWCOUNT);

      // -----------------------------------------------------------------------
      // So now we have read in the contents of the HISTOGRM & HISTINTS
      // tables from the system catalog.  Before we can use them, we need
      // to massage them into a format we can use.  In particular, we need
      // to make sure that what we read in (which the user may have mucked
      // about with) matches the histogram classes' internal semantic
      // requirements.  Also, we need to generate the MultiColumnUecList.
      //  ----------------------------------------------------------------------

      // what did the user set as the max number of intervals?
      NADefaults &defs = ActiveSchemaDB()->getDefaults();
      CollIndex maxIntervalCount = defs.getAsLong(HIST_MAX_NUMBER_OF_INTERVALS);

      //-----------------------------------------------------------------------------------
      // Need to flag the MC colStatsDesc so it is only used for the range partitioning task
      // and not any cardinality calculations tasks. Flagging it also makes the logic
      // to check fo the presence for this MC easier (at the time we need to create
      // the range partitioning function)
      //-----------------------------------------------------------------------------------

      if (CmpCommon::getDefault(HBASE_RANGE_PARTITIONING_MC_SPLIT) == DF_ON && 
          !(*colStats_).allFakeStats())
        {
          CollIndex currentMaxsize = 1;
          Int32 posMCtoUse = -1;

          NAColumnArray partCols;

          if (getClusteringIndex()->getPartitioningKeyColumns().entries() > 0)
            partCols = getClusteringIndex()->getPartitioningKeyColumns();
          else
            partCols = getClusteringIndex()->getIndexKeyColumns();

          CollIndex partColNum = partCols.entries();

          // look for MC histograms that have multiple intervals and whose columns are a prefix for the
          // paritition column list. If multiple pick the one with the most matching columns
          for (Int32 i=0; i < (*colStats_).entries(); i++)
            {
              NAColumnArray statsCols = (*colStats_)[i]->getStatColumns();
              CollIndex colNum = statsCols.entries();

              CollIndex j = 0;

              NABoolean potentialMatch = TRUE;
              if ((colNum > currentMaxsize) && 
                  (!(*colStats_)[i]->isSingleIntHist()) && // no SIH -- number of histograms is large enough to do splitting
                  (colNum <= partColNum))
                {
                  while ((j < colNum) && potentialMatch)
                    {
                      j++;
                      NAColumn * col = partCols[j-1];
                      if (statsCols[j-1]->getPosition() != partCols[j-1]->getPosition())
                        {
                          potentialMatch = FALSE;
                          break;
                        }   
                    }
                }
              else
                {
                  potentialMatch = FALSE;
                }

              if (potentialMatch)
                {
                  currentMaxsize = j;
                  posMCtoUse = i;
                }

              // we got what we need, just return
              if (potentialMatch && (currentMaxsize == partColNum))
                {
                  break;
                }
            }

          if (posMCtoUse >= 0)
            {
              (*colStats_)[posMCtoUse]->setMCforHbasePartitioning (TRUE);
            }
        }

      // *************************************************************************
      // FIRST: Generate the stats necessary to later create the
      // MultiColumnUecList; then filter out the multi-column histograms
      // because later code doesn't know how to handle them
      // In the same loop, also mark another flag for originally fake histogram
      // This is to differentiate the cases when the histogram is fake because
      // it has no statistics and the case where the histogram has been termed
      // fake by the optimizer because its statistics is no longer reliable.
      // *************************************************************************
      CollIndex i ;
      for ( i = 0 ; i < (*colStats_).entries() ; /* no automatic increment */ )
        {
          // the StatsList has two lists which it uses to store the information we
          // need to fill the MultiColumnUecList with <table-col-list,uec value> pairs:
          //
          // LIST(NAColumnArray) groupUecColumns_
          // LIST(CostScalar)    groupUecValues_
          //
          // ==> insert the NAColumnArray & uec total values for each
          // entry in colStats_

          // don't bother storing multicolumnuec info for fake histograms
          // but do set the originallly fake histogram flag to TRUE
          if ( (*colStats_)[i]->isFakeHistogram() )
            (*colStats_)[i]->setOrigFakeHist(TRUE);
          else if (!(*colStats_)[i]->isExpressionHistogram())
            {
              NAColumnArray cols = (*colStats_)[i]->getStatColumns() ;
              (*colStats_).groupUecColumns_.insert(cols) ;

              CostScalar uecs = (*colStats_)[i]->getTotalUec() ;
              (*colStats_).groupUecValues_.insert(uecs) ;

              if (CmpCommon::getDefault(USTAT_COLLECT_MC_SKEW_VALUES) == DF_ON)
                {
                  MCSkewedValueList mcSkewedValueList = (*colStats_)[i]->getMCSkewedValueList() ;
                  (*colStats_).groupMCSkewedValueLists_.insert(mcSkewedValueList) ;
                }
            }

          // MCH:
          // once we've stored the column/uec information, filter out the
          // multi-column histograms, since our synthesis code doesn't
          // handle them
          if (( (*colStats_)[i]->getStatColumns().entries() != 1) &&
              (!(*colStats_)[i]->isMCforHbasePartitioning()))
            {
              (*colStats_).removeAt(i) ;
            }
          else
            {
              i++ ; // in-place removal from a list is a bother!
            }
        }

      // *************************************************************************
      // SECOND: do some fixup work to make sure the histograms maintain
      // the semantics we later expect (& enforce)
      // *************************************************************************

      // -------------------------------------------------------------------------
      // HISTINT fixup-code : char-string histograms
      // -------------------------------------------------------------------------
      // problem arises with HISTINTs that are for char* columns
      // here's what we can get:
      //
      // Rows    Uec    Value
      // ----    ---    -----
      //    0      0    "value"
      //   10      5    "value"
      //
      // this is not good!  The problem is our (lousy) encoding of
      // char strings into EncodedValue's
      //
      // After much deliberation, here's our current fix:
      //
      // Rows    Uec    Value
      // ----    ---    -----
      //    0      0    "valu" <-- reduce the min value of 1st interval
      //   10      5    "value"    by a little bit
      //
      // When we find two intervals like this where they aren't the
      // first intervals in the histogram, we simply merge them into
      // one interval (adding row/uec information) and continue; note
      // that in this case, we haven't actually lost any information;
      // we've merely made sense out of (the garbage) what we've got
      //
      // -------------------------------------------------------------------------
      // additional HISTINT fixup-code
      // -------------------------------------------------------------------------
      // 1. If there are zero or one HISTINTs, then set the HISTINTs to match
      // the max/min information contained in the COLSTATS object.
      //
      // 2. If there are any HISTINTs whose boundary values are out-of-order,
      // we abort with an an ERROR message.
      //
      // 3. If there is a NULL HISTINT at the end of the Histogram, then we
      // need to make sure there are *TWO* NULL HISTINTS, to preserve correct
      // histogram semantics for single-valued intervals.
      // -------------------------------------------------------------------------

      CollIndex j ;
      for ( i = 0 ; i < (*colStats_).entries() ; i++ )
        {
          // we only worry about histograms on char string columns
          // correction: it turns out that these semantically-deranged
          // ----------  histograms were being formed for other, non-char string
          //             columns, so we commented out the code below
          // if ( colStats_[i]->getStatColumns()[0]->getType()->getTypeQualifier() !=
          //     NA_CHARACTER_TYPE)
          //   continue ; // not a string, skip to next

          ColStatsSharedPtr stats = (*colStats_)[i] ;

          HistogramSharedPtr hist = stats->getHistogramToModify() ;
          // histograms for key columns of a table that are not
          // referenced in the query are read in with zero intervals
          // (to conserve memory); however, internal
          // histogram-semantic checking code assumes that any
          // histogram which has zero intervals is FAKE; however
          // however, MDAM will not be chosen in the case where one of
          // the histograms for a key column is FAKE.  Thus -- we will
          // avoid this entire issue by creating a single interval for
          // any Histograms that we read in that are empty.
          if ( hist->entries() < 2 )
            {
              if(stats->getMinValue() > stats->getMaxValue())
                {
                  *CmpCommon::diags() << DgSqlCode(CATALOG_HISTOGRM_HISTINTS_TABLES_CONTAIN_BAD_VALUE)
                                      << DgString0("")
                                      << DgString1(stats->getStatColumns()[0]->getFullColRefNameAsAnsiString().data() );

                  stats->createFakeHist();
                  continue;
                }

              stats->setToSingleInterval ( stats->getMinValue(),
                                           stats->getMaxValue(),
                                           stats->getRowcount(),
                                           stats->getTotalUec() ) ;
              // now we have to undo some of the automatic flag-setting
              // of ColStats::setToSingleInterval()
              stats->setMinSetByPred (FALSE) ;
              stats->setMaxSetByPred (FALSE) ;
              stats->setShapeChanged (FALSE) ;
              continue ; // skip to next ColStats
            }

          // NB: we'll handle the first Interval last
          for ( j = 1 ; j < hist->entries()-1 ; /* no automatic increment */ )
            {

              if ( (*hist)[j].getUec() == 0 || (*hist)[j].getCardinality() == 0 )
                {
                  hist->removeAt(j) ;
                  continue ; // don't increment, loop again
                }

              // intervals must be in order!
              if ( (*hist)[j].getBoundary() > (*hist)[j+1].getBoundary() )
                {
                  *CmpCommon::diags() <<
                    DgSqlCode(CATALOG_HISTINTS_TABLES_CONTAIN_BAD_VALUES)
                                      << DgInt0(j)
                                      << DgInt1(j+1)
                                      << DgString1(stats->getStatColumns()[0]->getFullColRefNameAsAnsiString().data() );

                  stats->createFakeHist();
                  break ; // skip to next ColStats
                }

              if ( (*hist)[j].getBoundary() == (*hist)[j+1].getBoundary() )
                {
                  // merge Intervals, if the two consecutive intervals have same 
                  // boundaries and these are not single valued (UEC > 1)
                  // If there are more two single valued intervals, then merge
                  // all except the last one.
                  NABoolean mergeIntervals = FALSE;

                  if (CmpCommon::getDefault(COMP_BOOL_79) == DF_ON)
                    {
                      mergeIntervals = TRUE;

                      if( (j < (hist->entries() - 2)) && ((*hist)[j+1].getUec() == 1) &&
                          ((*hist)[j+1].getBoundary() != (*hist)[j+2].getBoundary())
                          ||
                          (j == (hist->entries() - 2)) && ((*hist)[j+1].getUec() == 1) )
                        mergeIntervals = FALSE;
                    }
                  else
                    {
                      if ( (*hist)[j+1].getUec() > 1)
                        mergeIntervals = TRUE;
                    }

                  if ( mergeIntervals ) 
                    {
                      // if the intervals with same boundary are not SVI, just merge them 
                      // together.
                      // Also do the merge, if there are more than one SVIs with same 
                      // encoded interval boundary. Example, we want to avoid intervals
                      // such as
                      //   boundary   inclusive_flag  UEC
                      //   12345.00    <               1
                      //   12345.00    <               1
                      //   12345.00    <=              1
                      // These would be changed to 
                      //   12345.00    <               2
                      //   12345.00    <=              1
                      CostScalar combinedRows = (*hist)[ j ].getCardinality() +
                        (*hist)[j+1].getCardinality() ;
                      CostScalar combinedUec  = (*hist)[ j ].getUec() +
                        (*hist)[j+1].getUec() ;
                      (*hist)[j].setCardAndUec (combinedRows, combinedUec) ;
                      stats->setIsColWithBndryConflict(TRUE);
                      hist->removeAt(j+1) ;
                    }
                  else
                    {
                      // for some reason, some SVI's aren't being
                      // generated correctly!
                      (*hist)[j].setBoundIncl(FALSE) ;
                      (*hist)[j+1].setBoundIncl(TRUE) ;
                      j++;
                    }
                }
              else
                j++ ; // in-place removal from a list is a bother!
            } // loop over intervals

          // ----------------------------------------------------------------------
          // now we handle the first interval
          //
          // first, it must be in order w.r.t. the second interval!
          if ( (*hist)[0].getBoundary() > (*hist)[1].getBoundary() )
            {
              *CmpCommon::diags() <<
                DgSqlCode(CATALOG_HISTINTS_TABLES_CONTAIN_BAD_VALUES)
                                  << DgInt0(0)
                                  << DgInt1(1)
                                  << DgString1(stats->getStatColumns()[0]->getFullColRefNameAsAnsiString().data() );

              stats->createFakeHist();
              continue ; // skip to next ColStats
            }

          // second, handle the case where first and second interval are the same
          if ( hist->entries() > 1 && // avoid the exception! might just be a single NULL
               //                     // interval after the loop above
               (*hist)[0].getBoundary() == (*hist)[1].getBoundary() &&
               (*hist)[1].getUec() > 1 )
            {
              const double KLUDGE_VALUE = 0.0001 ;
              const double oldVal = (*hist)[0].getBoundary().getDblValue() ;
              const EncodedValue newVal =
                EncodedValue(oldVal - (_ABSOLUTE_VALUE_(oldVal) * KLUDGE_VALUE)) ; // kludge alert!
              //Absolute of oldval due to CR 10-010426-2457
              (*hist)[0].setBoundary( newVal ) ;
              (*hist)[0].setBoundIncl( FALSE ) ; // no longer a real boundary!
              (*colStats_)[i]->setMinValue( newVal ) ; // set aggr info also
            }
          // done with first interval
          // ----------------------------------------------------------------------

          //
          // NULL values must only be stored in single-valued intervals
          // in the histograms ; so, just in case we're only getting
          // *one* HistInt for the NULL interval, insert a 2nd one
          //
          // 0   1   2
          // |   |   |
          // |   |   |    entries() == 3
          //         NULL
          //
          // 0   1   2   3
          // |   |   |   |
          // |   |   |   |    entries() == 4
          //        new  NULL
          //        NULL
          //
          if ( hist->lastHistInt().isNull() )
            {
              CollIndex count = hist->entries() ;
              if ( !(*hist)[count-2].isNull() )
                {
                  // insert a 2nd NULL HISTINT, with boundaryIncl value FALSE
                  HistInt secondLast (hist->lastHistInt().getBoundary(), FALSE) ;
                  hist->insertAt(count-1,secondLast) ;
                  // new HISTINT by default has row/uec of 0, which is what we want
                }
            }

          //
          // Now, reduce the total number of intervals to be the number
          // that the user wants.  This is used to test the tradeoffs
          // between compile time & rowcount estimation.
          //
          (*colStats_)[i]->setMaxIntervalCount (maxIntervalCount) ;
          (*colStats_)[i]->reduceToMaxIntervalCount () ;

          if ((*colStats_)[i]->getRowcount() == (*colStats_)[i]->getTotalUec() )
            (*colStats_)[i]->setAlmostUnique(TRUE);

        } // outer for loop -- done with this COLSTATS, continue with next one
      // ***********************************************************************

      statsFetched_ = TRUE;
      MonitorMemoryUsage_Exit((char*)mmPhase.data(), NULL, NULL, TRUE);
    } // !statsFetched_

  return (colStats_);
}

StatsList *
NATable::generateFakeStats()
{
	if (colStats_ == NULL)
	{
		//trying to get statistics for a new statement allocate colStats_
		colStats_ = new (CmpCommon::statementHeap()) StatsList(CmpCommon::statementHeap());
	}

  if (colStats_->entries() > 0)
    return (colStats_);

	NAColumnArray colList = getNAColumnArray() ;
	double defaultFakeRowCount = (ActiveSchemaDB()->getDefaults()).getAsDouble(HIST_NO_STATS_ROWCOUNT);
	double defaultFakeUec = (ActiveSchemaDB()->getDefaults()).getAsDouble(HIST_NO_STATS_UEC);

  if ( isHiveTable() ) {
    defaultFakeRowCount = getOriginalRowCount().value();
  }
  else if (getOriginalRowCount().value() >= 0) {
    defaultFakeRowCount = getOriginalRowCount().value();
  }
    
  for (CollIndex i = 0; i < colList.entries(); i++ )
  {
    NAColumn * col = colList[i];

		if (col->isUnique() )
			defaultFakeUec = defaultFakeRowCount;
		else
			defaultFakeUec = MINOF(defaultFakeUec, defaultFakeRowCount);

		EncodedValue dummyVal(0.0);

		EncodedValue lowBound = dummyVal.minMaxValue(col->getType(), TRUE);
		EncodedValue highBound = dummyVal.minMaxValue(col->getType(), FALSE);

		HistogramSharedPtr emptyHist(new (HISTHEAP) Histogram(HISTHEAP));

		HistInt newFirstHistInt(lowBound, FALSE);

		HistInt newSecondHistInt(highBound, TRUE);

		newSecondHistInt.setCardAndUec(defaultFakeRowCount,
				defaultFakeUec);

		emptyHist->insert(newFirstHistInt);
		emptyHist->insert(newSecondHistInt);

		ComUID histid(NA_JulianTimestamp());
		ColStatsSharedPtr fakeColStats(
				new (HISTHEAP) ColStats(histid,
					defaultFakeUec,
					defaultFakeRowCount,
					defaultFakeRowCount,
					col->isUnique(),
					FALSE,
					emptyHist,
					FALSE,
					1.0,
					1.0,
					-1, // avg varchar size
					HISTHEAP));

		fakeColStats->setFakeHistogram(TRUE);
		fakeColStats->setOrigFakeHist(TRUE);
		fakeColStats->setMinValue(lowBound);
		fakeColStats->setMaxValue(highBound);
		fakeColStats->statColumns().insert(col);

		colStats_->insert(fakeColStats);
	}
	setStatsFetched(TRUE);
	setOriginalRowCount(defaultFakeRowCount);

  return (colStats_);
}

NABoolean NATable::rowsArePacked() const
{
	// If one fileset is packed, they all are
	return (getVerticalPartitionList().entries() &&
			getVerticalPartitionList()[0]->isPacked());
}

// MV
// Read materialized view information from the catalog manager.
MVInfoForDML *NATable::getMVInfo(BindWA *bindWA)
{
	return mvInfo_;
}

// MV
// An MV is usable unly when it is initialized and not unavailable.
// If not initialized, keep a list and report error at runtime.
NABoolean NATable::verifyMvIsInitializedAndAvailable(BindWA *bindWA) const
{
	CMPASSERT(isAnMV());
	const ComMvAttributeBitmap& bitmap = getMvAttributeBitmap();

	// First check if the table is Unavailable.
	NAString value;
	if (bitmap.getIsMvUnAvailable())
	{

		// 12312 Materialized View $0~TableName is unavailable.
		*CmpCommon::diags() << DgSqlCode(-12312)
			<< DgTableName(getTableName().getQualifiedNameAsString());
		bindWA->setErrStatus();

		return TRUE;
	}

	// if the mv is uninitialized,
	// add it to the uninitializedMvList in the BindWA
	if (bitmap.getIsMvUnInitialized())
	{

		// get physical and ansi names
		NAString fileName(
				getClusteringIndex()->getFileSetName().getQualifiedNameAsString(),
				bindWA->wHeap() );

		NAString ansiName( getTableName().getQualifiedNameAsAnsiString(),
				bindWA->wHeap() );

		// get physical and ansi name
		bindWA->addUninitializedMv(
				convertNAString( fileName, bindWA->wHeap() ),
				convertNAString( ansiName, bindWA->wHeap() ) );
	}


	return FALSE;
}

// Return value: TRUE, found an index or constr. FALSE, not found.
// explicitIndex: get explicitly created index
// uniqueIndex: TRUE, get unique index. FALSE, any index.
// 
// primaryKeyOnly: TRUE, get primary key 
// indexName: return index name, if passed in
// lookForSameSequenceOfCols: TRUE, look for an index in which the
//                            columns appear in the same sequence
//                            as in inputCols (whether they are ASC or
//                            DESC doesn't matter).
//                            FALSE, accept any index that has the
//                            same columns, in any sequence.
NABoolean NATable::getCorrespondingIndex(NAList<NAString> &inputCols,
		NABoolean lookForExplicitIndex,
		NABoolean lookForUniqueIndex,
		NABoolean lookForPrimaryKey,
		NABoolean lookForAnyIndexOrPkey,
		NABoolean lookForSameSequenceOfCols,
		NABoolean excludeAlwaysComputedSystemCols,
		NAString *indexName)
{
	NABoolean indexFound = FALSE;
	CollIndex numInputCols = inputCols.entries();

	if (numInputCols == 0)
	{
		lookForPrimaryKey = TRUE;
		lookForUniqueIndex = FALSE;
		lookForAnyIndexOrPkey = FALSE;
	}

	Lng32 numBTpkeys = getClusteringIndex()->getIndexKeyColumns().entries();

	const NAFileSetList &indexList = getIndexList();
	for (Int32 i = 0; (NOT indexFound && (i < indexList.entries())); i++)
	{
		NABoolean isPrimaryKey = FALSE;
		NABoolean isUniqueIndex = FALSE;

		const NAFileSet * naf = indexList[i];
		if (naf->getKeytag() == 0)
			isPrimaryKey = TRUE;
		else if (naf->uniqueIndex())
			isUniqueIndex = TRUE;

		if ((NOT lookForPrimaryKey) && (isPrimaryKey))
			continue;

		NABoolean found = FALSE;
		if (lookForAnyIndexOrPkey)
			found = TRUE;
		else if (lookForPrimaryKey && isPrimaryKey)
			found = TRUE;
		else if (lookForUniqueIndex && isUniqueIndex)
			found = TRUE;

		if (found)
		{
			if (lookForExplicitIndex)  // need an explicit index to match.
			{
				if ((naf->isCreatedExplicitly()) ||
						(isPrimaryKey))
					found = TRUE;
				else
					found = FALSE;
			}
		}

		if (NOT found)
			continue;

		Int32 numMatchedCols = 0;
		NABoolean allColsMatched = TRUE;

		if (numInputCols > 0)
		{
			const NAColumnArray &nacArr = naf->getIndexKeyColumns();

			Lng32 numKeyCols = naf->getCountOfColumns(
					TRUE,           // exclude non-key cols
					!isPrimaryKey,  // exclude cols other than user-specified index cols
					FALSE,          // don't exclude all system cols like SYSKEY
					excludeAlwaysComputedSystemCols);

			// compare # of columns first and disqualify the index
			// if it doesn't have the right number of columns
			if (numInputCols != numKeyCols)
				continue;

			// compare individual key columns with the provided input columns
			for (Int32 j = 0; j < nacArr.entries() && allColsMatched; j++)
			{
				NAColumn *nac = nacArr[j];

				// exclude the same types of columns that we excluded in
				// the call to naf->getCountOfColumns() above
				if (!isPrimaryKey &&
						nac->getIndexColName() == nac->getColName())
					continue;

				if (excludeAlwaysComputedSystemCols &&
						nac->isComputedColumnAlways() && nac->isSystemColumn())
					continue;

				const NAString &keyColName = nac->getColName();
				NABoolean colFound = FALSE;

				// look up the key column name in the provided input columns
				if (lookForSameSequenceOfCols)
				{
					// in this case we know exactly where to look
					colFound = (keyColName == inputCols[numMatchedCols]);
				}
				else
					for (Int32 k = 0; !colFound && k < numInputCols; k++)
					{
						if (keyColName == inputCols[k])
							colFound = TRUE;
					} // loop over provided input columns

				if (colFound)
					numMatchedCols++;
				else
					allColsMatched = FALSE;
			} // loop over key columns of the index

			if (allColsMatched)
			{
				// just checking that the above loop and
				// getCountOfColumns() don't disagree
				CMPASSERT(numMatchedCols == numKeyCols);

				indexFound = TRUE;
			}
		} // inputCols specified
		else
			indexFound = TRUE; // primary key, no input cols specified

		if (indexFound)
		{
			if (indexName)
			{
				*indexName = naf->getExtFileSetName();
			}
		}
	} // loop over indexes of the table

	return indexFound;
}

NABoolean NATable::getCorrespondingConstraint(NAList<NAString> &inputCols,
		NABoolean uniqueConstr,
		NAString *constrName,
		NABoolean * isPkey,
		NAList<int> *reorderList)
{
	NABoolean constrFound = FALSE;
	NABoolean lookForPrimaryKey = (inputCols.entries() == 0);

	const AbstractRIConstraintList &constrList = 
		(uniqueConstr ? getUniqueConstraints() : getRefConstraints());

	if (isPkey)
		*isPkey = FALSE;

	for (Int32 i = 0; (NOT constrFound && (i < constrList.entries())); i++)
	{
		AbstractRIConstraint *ariConstr = constrList[i];

		if (uniqueConstr && (ariConstr->getOperatorType() != ITM_UNIQUE_CONSTRAINT))
			continue;

		if (lookForPrimaryKey && (NOT ((UniqueConstraint*)ariConstr)->isPrimaryKeyConstraint()))
			continue;

		if ((NOT uniqueConstr) && (ariConstr->getOperatorType() != ITM_REF_CONSTRAINT))
			continue;

		if (NOT lookForPrimaryKey)
		{
			Int32 numUniqueCols = 0;
			NABoolean allColsMatched = TRUE;
			NABoolean reorderNeeded = FALSE;

			if (reorderList)
				reorderList->clear();

			for (Int32 j = 0; j < ariConstr->keyColumns().entries() && allColsMatched; j++)
			{
				// The RI constraint contains a dummy NAColumn, get to the
				// real one to test for computed columns
				NAColumn *nac = getNAColumnArray()[ariConstr->keyColumns()[j]->getPosition()];

				if (nac->isComputedColumnAlways() && nac->isSystemColumn())
					// always computed system columns in the key are redundant,
					// don't include them (also don't include them in the DDL)
					continue;

				const NAString &uniqueColName = (ariConstr->keyColumns()[j])->getColName();
				NABoolean colFound = FALSE;

				// compare the unique column name to the provided input columns
				for (Int32 k = 0; !colFound && k < inputCols.entries(); k++)
					if (uniqueColName == inputCols[k])
					{
						colFound = TRUE;
						numUniqueCols++;
						if (reorderList)
							reorderList->insert(k);
						if (j != k)
							// inputCols and key columns come in different order
							// (order/sequence of column names, ignoring ASC/DESC)
							reorderNeeded = TRUE;
					}

				if (!colFound)
					allColsMatched = FALSE;
			}

			if (inputCols.entries() == numUniqueCols && allColsMatched)
			{
				constrFound = TRUE;

				if (reorderList && !reorderNeeded)
					reorderList->clear();
			}
		}
		else
		{
			// found the primary key constraint we were looking for
			constrFound = TRUE;
		}

		if (constrFound)
		{
			if (constrName)
			{
				*constrName = ariConstr->getConstraintName().getQualifiedNameAsAnsiString();
			}

			if (isPkey)
			{
				if ((uniqueConstr) && (((UniqueConstraint*)ariConstr)->isPrimaryKeyConstraint()))
					*isPkey = TRUE;
			}
		}
		else
			if (reorderList)
				reorderList->clear();
	}

	return constrFound;
}

void NATable::removePrivInfo(void)
{
  if (privInfo_)
  {
    {
      NADELETE(privInfo_, PrivMgrUserPrivs, heap_);
      privInfo_ = NULL;
    }
  }
  secKeySet_.clear();
  secKeySet_ = NULL;
}

     
// Extract priv bitmaps and security invalidation keys for the current user
void NATable::getPrivileges(TrafDesc * priv_desc, BindWA *bindWA)
{
#ifdef DEBUG_GET_PRIVILEDGES
  cout << "On ENTRY: NATable::getPrivileges()" << endl; 
  parquet::StopWatch s1;
  s1.Start();
#endif
     
  // Return if this is a PrivMgr table to avoid an infinite loop.  Part of 
  // gathering privileges requires access to PrivMgr tables.  This access 
  // calls getPrivileges, which in turn calls getPrivileges, ad infinitum.
  // PrivMgr table privileges are checked later in RelRoot::checkPrivileges.
  if (CmpSeabaseDDL::isSeabasePrivMgrMD
       (qualifiedName_.getQualifiedNameObj().getCatalogName(),
        qualifiedName_.getQualifiedNameObj().getSchemaName()))

  {
    isSeabasePrivSchemaTable_ = TRUE;
   return;
  }

  // Most of the time, MD is read by the compiler/DDL on behalf of the user so 
  // privilege checks are skipped.  Instead of performing the I/O to get 
  // privilege information at this time, set privInfo_ to NULL and rely on 
  // RelRoot::checkPrivileges to look up privileges. checkPrivileges is 
  // optimized to lookup privileges only when needed.
  if (CmpSeabaseDDL::isSeabaseReservedSchema
       (qualifiedName_.getQualifiedNameObj().getCatalogName(),
        qualifiedName_.getQualifiedNameObj().getSchemaName()))
    return;

  Int32 currentUser (ComUser::getCurrentUser());

  // If TRAF_CLEAR_METADATA_CACHE is OFF, then when reconnecting to mxosrvrs, we
  // adjust (reset) cache entries for the new user.  In order to effectively reset
  // privileges then we cannot skip gathering privileges for DB__ROOT.
  // Set gatherPrivsForRoot to indicate this.
  NABoolean gatherPrivsForRoot = (CmpCommon::getDefault(TRAF_CLEAR_METADATA_CACHE) == DF_OFF);

  // Automatically have owner default privileges.
  if (!CmpCommon::context()->isAuthorizationEnabled() ||
      isVolatileTable() ||
      (ComUser::isRootUserID() && !gatherPrivsForRoot &&
        (!isHiveTable() && !isHbaseCellTable() &&
         !isHbaseRowTable() && !isHbaseMapTable()
        ) 
      )
    )
  {
    privInfo_ = new(heap_) PrivMgrUserPrivs;
    privInfo_->setOwnerDefaultPrivs();
    return;
  }

  if (priv_desc == NULL)
  {
    // TDB - add schema level privs to hive tables
    if (!isSeabaseTable())
    {
      ULng32 savedParserFlags = 0;
      ContextCli *ctxCli = GetCliGlobals()->currContext();
      if (ctxCli)
        savedParserFlags = ctxCli->getSqlParserFlags();
      readPrivileges();
      if (ctxCli)
        ctxCli->assignSqlParserFlags(savedParserFlags);
    }
    else
    {
      privInfo_ = NULL;
      return;
    }
  }

  else
  {

    // gather schema privileges for objects, skip if this is a schema
    NATable *schemaTable = NULL;
    if (!isSchemaObject())
    {
      CorrName cn(SEABASE_SCHEMA_OBJECTNAME,
                   STMTHEAP,
                   qualifiedName_.getQualifiedNameObj().getSchemaName(),
                   qualifiedName_.getQualifiedNameObj().getCatalogName());
      cn.setSpecialType(ExtendedQualName::SCHEMA_TABLE);

      UInt32 parserFlags;
      SQL_EXEC_GetParserFlagsForExSqlComp_Internal(parserFlags);
      SQL_EXEC_SetParserFlagsForExSqlComp_Internal(INTERNAL_QUERY_FROM_EXEUTIL);
      schemaTable = ActiveSchemaDB()->getNATableDB()->get(cn, bindWA, NULL, FALSE);
      SQL_EXEC_AssignParserFlagsForExSqlComp_Internal(parserFlags);
    }

    // get roles granted to current user 
    // use embedded compiler.
    CmpSeabaseDDL cmpSBD(STMTHEAP);
    if (cmpSBD.switchCompiler(CmpContextInfo::CMPCONTEXT_TYPE_META))
    {
      if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_) == 0)
        *CmpCommon::diags() << DgSqlCode( -4400 );

      return;
    }

    NAList <Int32> roleIDs(heap_);
    Int32 retcode = ComUser::getCurrentUserRoles(roleIDs, CmpCommon::diags());
    cmpSBD.switchBackCompiler();
    if (retcode != 0)
    {
      if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_) == 0)
        *CmpCommon::diags() << DgSqlCode(-1078) 
                            << DgString0(ComUser::getCurrentUsername());
      return;
    }

    Int32 numRoles = roleIDs.entries();

    // At this time we should have at least one entry in roleIDs (PUBLIC_USER)
    //CMPASSERT (numRoles > 0);

    // (PrivMgrUserPrivs)  privInfo_ are privs for the current user
    // (PrivMgrDescList)   privDescs_ are all privs for the object
    // (TrafPrivDesc)      priv_desc are all object privs in TrafDesc form
    //                     created by CmpSeabaseDDL::getSeabasePrivInfo
    //                     before the NATable entry is constructed
    // (ComSecurityKeySet) secKeySet_ are the qi keys for the current user

    // Convert priv_desc into a list of PrivMgrDesc (privDescs_)
    privDescs_ = new (heap_) PrivMgrDescList(heap_); //initialize empty list
    TrafDesc *priv_grantees_desc = priv_desc->privDesc()->privGrantees;
    
    // Check the first entry to verify that stored priv descriptors need 
    // to be re-generated.  Priv descriptor structure changed and stored values
    // need to be re-generated.
    if (priv_grantees_desc)
    {
      TrafDesc *schemaPrivs = priv_grantees_desc->privGranteeDesc()->schemaBitmap;
      TrafDesc *objectPrivs = priv_grantees_desc->privGranteeDesc()->objectBitmap;
      if (schemaPrivs == NULL || objectPrivs == NULL)
      {
        *CmpCommon::diags() << DgSqlCode(1126)
                            << DgString0(getTableName().getQualifiedNameAsString());
        return;
      }
    }

    while (priv_grantees_desc)
    {
      PrivMgrDesc *privs = new (heap_) PrivMgrDesc(priv_grantees_desc->privGranteeDesc()->grantee);
      privs->setSchemaUID(priv_grantees_desc->privGranteeDesc()->schemaUID);
      TrafDesc *schemaPrivs = priv_grantees_desc->privGranteeDesc()->schemaBitmap;
      PrivMgrCoreDesc schemaDesc(schemaPrivs->privBitmapDesc()->privBitmap,
                                 schemaPrivs->privBitmapDesc()->privWGOBitmap);

      TrafDesc *objectPrivs = priv_grantees_desc->privGranteeDesc()->objectBitmap;
      PrivMgrCoreDesc objectDesc(objectPrivs->privBitmapDesc()->privBitmap,
                                 objectPrivs->privBitmapDesc()->privWGOBitmap);

      TrafDesc *priv_grantee_desc = priv_grantees_desc->privGranteeDesc();
      TrafDesc *columnPrivs = priv_grantee_desc->privGranteeDesc()->columnBitmaps;
      NAList<PrivMgrCoreDesc> columnDescs(heap_);
      while (columnPrivs)
      {
        PrivMgrCoreDesc columnDesc(columnPrivs->privBitmapDesc()->privBitmap,
                                   columnPrivs->privBitmapDesc()->privWGOBitmap,
                                   columnPrivs->privBitmapDesc()->columnOrdinal);
        columnDescs.insert(columnDesc);
        columnPrivs = columnPrivs->next;
      }

      if (isSchemaObject())
      {
        // schema privs does not support object and column level
        privs->setSchemaObject(true);
        privs->setSchemaPrivs(objectDesc);
      }
      else
      {
        privs->setTablePrivs(objectDesc);
        privs->setColumnPrivs(columnDescs);
      }

      privs->setHasPublicPriv(ComUser::isPublicUserID(privs->getGrantee()));

      privDescs_->insert(privs);
      priv_grantees_desc = priv_grantees_desc->next;
    }
 
#ifndef NDEBUG
    if (getenv("NATABLE_DEBUG"))
    {
      cout << "NATable privs for: " << qualifiedName_.getQualifiedNameObj().getObjectName() << endl;
      privDescs_->print();
    }
#endif

    // Generate privInfo_ and secKeySet_ for current user from privDescs_
    privInfo_ = new(heap_) PrivMgrUserPrivs;
    privInfo_->initUserPrivs(roleIDs,
                             privDescs_,
                             currentUser,
                             objectUID_.get_value(),
                             &secKeySet_);

    if (privInfo_ == NULL)
    {
      if (!CmpCommon::diags()->containsError(-1034))
        *CmpCommon::diags() << DgSqlCode(-1034);
      return;
    }

    // If privileges for private schemas exist, add to privList and security 
    // key list.  Schema privileges are accumulated across all grantors 
    // including roles and public.  
    if (!isSchemaObject() && schemaTable && 
        schemaTable->getPrivDescs() && 
        schemaTable->getObjectType() == COM_PRIVATE_SCHEMA_OBJECT)
    {
      // attach the schema's privs to the object
      for (Int32 s = 0; s < schemaTable->getPrivDescs()->entries(); s++)
      {
        PrivMgrDesc schemaDesc = *(*schemaTable->getPrivDescs())[s];
        PrivMgrDesc *descToAdd = new (heap_) PrivMgrDesc(schemaDesc);
        privDescs_->insert(descToAdd);
      }

      privInfo_->setSchemaPrivBitmap(schemaTable->getPrivInfo()->getSchemaPrivBitmap());
      privInfo_->setSchemaGrantableBitmap(schemaTable->getPrivInfo()->getSchemaGrantableBitmap());
      if (privInfo_->getSchemaPrivBitmap().any())
      {
        // Add schema's security keys
        for (Int32 i = 0; i < schemaTable->getSecKeySet().entries(); i++)
        {
          ComSecurityKey key = schemaTable->getSecKeySet()[i];
          secKeySet_.insert(key);
        }
        privInfo_->setHasPublicPriv(schemaTable->getPrivInfo()->getHasPublicPriv());
      }
    }
  }

  if (!isSchemaObject())
  {
    // log privileges enabled for table
    Int32 len = 500;
    char msg[len];
    std::string privDetails = (privInfo_) ? privInfo_->print() : "no privileges defined";
    snprintf(msg, len, "NATable::getPrivileges (list of all privileges on object), user: %s obj %s, %s",
            ComUser::getCurrentUsername(), 
            qualifiedName_.getExtendedQualifiedNameAsString().data(),
            privDetails.c_str());
    QRLogger::log(CAT_SQL_EXE, LL_DEBUG, "%s", msg);
#ifndef NDEBUG
    static char *dbuser_debug = getenv("DBUSER_DEBUG");
    if (dbuser_debug != NULL)
    {
      printf("[DBUSER:%d] %s\n", (int) getpid(), msg);
      fflush(stdout);
    }
#endif
  }

#ifdef DEBUG_GET_PRIVILEDGES
   cout << "On Exit: NATable::getPrivileges()" 
        << " ET=" << s1.Stop() << "us"
        << endl; 
#endif
}

// Cll privilege manager to get privileges and security keys
// update privInfo_ and secKeySet_ members with values 
void NATable::readPrivileges ()
{
  // Determine if SENTRY mode is enabled
   
  // If table caching is off, clear out stored group list
  NABoolean usingSentryUserApi = FALSE;
  if (CmpCommon::getDefault(PRIV_SENTRY_USE_USERNAME_API) == DF_ON)
    usingSentryUserApi = TRUE;

  privInfo_ = new(heap_) PrivMgrUserPrivs;

  bool testError = false;
#ifndef NDEBUG
  char *tpie = getenv("TEST_PRIV_INTERFACE_ERROR");
  if (tpie && *tpie == '1')
  testError = true;
#endif

  // use embedded compiler.
  CmpSeabaseDDL cmpSBD(STMTHEAP);
  if (cmpSBD.switchCompiler(CmpContextInfo::CMPCONTEXT_TYPE_META))
  {
    if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_) == 0)
      *CmpCommon::diags() << DgSqlCode( -4400 );

    return;
  }

  NAString privMDLoc = CmpSeabaseDDL::getSystemCatalogStatic();
  privMDLoc += ".\"";
  privMDLoc += SEABASE_PRIVMGR_SCHEMA;
  privMDLoc += "\"";

  PrivMgrCommands privInterface(privMDLoc.data(), CmpCommon::diags(),PrivMgr::PRIV_INITIALIZED);

  if (testError || (STATUS_GOOD !=
    privInterface.getPrivileges((NATable *)this,
                                 ComUser::getCurrentUser(), 
                                 usingSentryUserApi,
                                 *privInfo_, &secKeySet_)))
  {
  if (testError)
#ifndef NDEBUG
    *CmpCommon::diags() << DgSqlCode(-8142) 
                        << DgString0("TEST_PRIV_INTERFACE_ERROR")  
                        << DgString1(tpie) ;
#else
  abort();
#endif
  NADELETE(privInfo_, PrivMgrUserPrivs, heap_);
  privInfo_ = NULL;

  cmpSBD.switchBackCompiler();
  return;
}

  CMPASSERT (privInfo_);

  cmpSBD.switchBackCompiler();

}

// Query the metadata to find the object uid of the table. This is used when
// the uid for a metadata table is requested, since 0 is usually stored for
// these tables.
// 
Int64 NATable::lookupObjectUid()
{
  Int64 objDataUID = 0;
  QualifiedName qualName = getExtendedQualName().getQualifiedNameObj();
  objectUID_ = lookupObjectUidByName(qualName, objectType_, FALSE, NULL, NULL, &objDataUID);
  objDataUID_ = objDataUID;

  if (objectUID_ <= 0 && CmpCommon::diags()->mainSQLCODE() >= 0)
    // object not found, no serious error
    objectUID_ = 0;

  return objectUID_.get_value();
}

bool NATable::isEnabledForDDLQI() const
{
  if (isSeabaseMD_ || isSMDTable_ || (getSpecialType() == ExtendedQualName::VIRTUAL_TABLE))
    return false;
  else if (isHiveTable() && (objectUID_.get_value() == 0))
    return false;
  else
    {
      if (objectUID_.get_value() == 0)
        {
          // Looking up object UIDs at code-gen time was shown to cause
          // more than 10% performance regression in YCSB benchmark. In
          // that investigation, we learned that metadata and histogram 
          // NATables would have no object UID at code-gen and would 
          // require the lookup.  We're pretty sure these are the only 
          // types of tables but will abend here otherwise. If this 
          // causes problems, the envvar below can be used as a 
          // temporary workaround. 
          static char *noAbendOnLp1398600 = getenv("NO_ABEND_ON_LP_1398600");
          if (!noAbendOnLp1398600 || *noAbendOnLp1398600 == '0')
            abort();
        }
      return true;
    }
}

NATable::~NATable()
{
  // remove the map entries of associated table identifers in
  // NAClusterInfo::tableToClusterMap_.
  CMPASSERT(CURRCONTEXT_CLUSTERINFO);
  NAColumn *col;
  NABoolean delHeading = ActiveSchemaDB()->getNATableDB()->cachingMetaData();
  const LIST(CollIndex) & tableIdList = getTableIdList();

  if (privInfo_)
    {
      NADELETE(privInfo_, PrivMgrUserPrivs, heap_);
      privInfo_ = NULL;
    }

  if (privDescs_ != NULL)
    {
     // privDescs_ is shallowed copied. NADELETEBASIC() deallocates the 
     // memory used by privDescs_ and does not call its dstr.
      NADELETEBASIC(privDescs_, heap_); 
      privDescs_ = NULL;
    }

  if (! isHive_) {
    if ( !shallowCopied_ ) {
       for (int i = 0 ; i < colcount_ ; i++) {
         col = (NAColumn *)colArray_[i];
         if (delHeading) {
           if (col->getDefaultValue())
             NADELETEBASIC(col->getDefaultValue(), heap_);
           if (col->getHeading())
             NADELETEBASIC(col->getHeading(), heap_);
           if (col->getComputedColumnExprString())
             NADELETEBASIC(col->getComputedColumnExprString(),heap_);
         }
         NADELETE(col->getType(), NAType, heap_);
         NADELETE(col, NAColumn, heap_);
       }
    } 
    colArray_.clear();
  }



  if (parentTableName_ != NULL)
    {
      NADELETEBASIC(parentTableName_, heap_);
      parentTableName_ = NULL;
    } 
  if (viewText_ != NULL)
    {
      NADELETEBASIC(viewText_, heap_);
      viewText_ = NULL;
    } 
  if (viewCheck_ != NULL)
    {
      NADELETEBASIC(viewCheck_, heap_);
      viewCheck_ = NULL;
    } 
  if (viewColUsages_ != NULL)
    {
      if ( !shallowCopied_ )
      {
        for(Int32 i = 0; i < viewColUsages_->entries(); i++)
        {
          ComViewColUsage *pUsage = viewColUsages_->operator[](i);
          NADELETEBASIC(pUsage, heap_);
        }
      }
      NADELETEBASIC(viewColUsages_, heap_);
      viewColUsages_ = NULL;
    } 
  if (viewFileName_ != NULL)
    {
      NADELETEBASIC(viewFileName_, heap_);
      viewFileName_ = NULL;
    } 
  if (hiveExpandedViewText_ != NULL)
    {
      NADELETEBASIC(hiveExpandedViewText_, heap_);
      hiveExpandedViewText_ = NULL;
    }
  if (hiveOriginalViewText_ != NULL)
    {
      NADELETEBASIC(hiveOriginalViewText_, heap_);
      hiveOriginalViewText_ = NULL;
    }
  if (prototype_ != NULL)
    {
      NADELETE(prototype_, HostVar, heap_);
      prototype_ = NULL;
    }
  if (sgAttributes_ != NULL)
    {
      NADELETE(sgAttributes_, SequenceGeneratorAttributes, heap_);
      sgAttributes_ = NULL;
    }
  // clusteringIndex_ is part of indexes - No need to delete clusteringIndex_
  if ( !shallowCopied_ ) {
     CollIndex entryCount = indexes_.entries();
     for (CollIndex i = 0 ; i < entryCount; i++) {
       if (indexes_[i]->getKeysDesc())
         TrafDesc::deleteDescList(indexes_[i]->getKeysDesc(), heap_);

       NADELETE(indexes_[i], NAFileSet, heap_);
     }
  }
  indexes_.clear();
  
  if ( !shallowCopied_ ) {
     CollIndex entryCount  = vertParts_.entries();
     for (CollIndex i = 0 ; i < entryCount; i++) {
       NADELETE(vertParts_[i], NAFileSet, heap_);
     }
  }

  vertParts_.clear();

  if ( !shallowCopied_ ) {
     CollIndex entryCount = newColumns_.entries();
     for (int i = 0 ; i < entryCount ; i++) {
       col = (NAColumn *)newColumns_[i];
       NADELETE(col, NAColumn, heap_);
     }
  }
  newColumns_.clear();

  if ( !shallowCopied_ ) {
     CollIndex entryCount = checkConstraints_.entries();
     for (CollIndex i = 0 ; i < entryCount; i++) {
       NADELETE(checkConstraints_[i], CheckConstraint, heap_);
     }
  }

  checkConstraints_.clear();

  if ( !shallowCopied_ ) {
     CollIndex entryCount = uniqueConstraints_.entries();
     for (CollIndex i = 0 ; i < entryCount; i++) {
       NADELETE((UniqueConstraint *)uniqueConstraints_[i], UniqueConstraint, heap_);
     }
  }
  uniqueConstraints_.clear();

  if ( !shallowCopied_ ) {
     CollIndex entryCount  = refConstraints_.entries();
     for (CollIndex i = 0 ; i < entryCount; i++) {
       NADELETE((RefConstraint *)refConstraints_[i], RefConstraint, heap_);
     }
  }
  refConstraints_.clear();

  if ( !shallowCopied_ ) {
     CollIndex entryCount  = mvsUsingMe_.entries();
     for (CollIndex i = 0 ; i < entryCount; i++) {
       NADELETE(mvsUsingMe_[i], UsingMvInfo, heap_);
     }
  }
  mvsUsingMe_.clear();
  // mvInfo_ is not used at all
  // tableIDList_ is list of ints - No need to delete the entries
  // colStats_ and colsWithMissingStats_ comes from STMTHEAP
  // secKeySet_ is the set that holds ComSecurityKeySet object itself

  if (columnsDesc_)
    TrafDesc::deleteDescList(columnsDesc_, heap_);

  if (storedStatsDesc_)
    TrafDesc::deleteDescList(storedStatsDesc_, heap_);

  if ( shallowCopied_ ) {
    minmaxCache_.detachHashBuckets();

    interestingExpressions_.detachHashBuckets();

    if ( colsWithMissingStats_ )
     colsWithMissingStats_->detachHashBuckets();
  }

  if (partitionColIdxAry_)
    NADELETEBASIC(partitionColIdxAry_, heap_);

  if (subpartitionColIdxAry_) 
    NADELETEBASIC(subpartitionColIdxAry_, heap_);

  partArray_.deepDelete();
  partArray_.clear();

  if (partitionInterval_)
    NADELETEBASIC(partitionInterval_, heap_);

  if (subpartitionInterval_)
    NADELETEBASIC(subpartitionInterval_, heap_);
}

void NATable::resetAfterStatement() // ## to be implemented?
{
  if(resetAfterStatement_)
    return;
  //It is not clear to me whether virtual tables and resource forks
  //(any "special" table type) can/should be reused.  Maybe certain
  //types can; I just have no idea right now.  But as we're not reading
  //metadata tables for them anyway, there seems little savings in
  //caching them; perhaps we should just continue to build them on the fly.
  //
  //All the real metadata in NATable members can stay as it is.
  //But there are a few pieces of for-this-query-only data:

  referenceCount_ = 0;
  refsIncompatibleDP2Halloween_ = FALSE;
  isHalloweenTable_ = FALSE;
  //And we now optimize/filter/reduce histogram statistics for each
  //individual query, so stats and adminicular structures must be reset:

  statsFetched_ = FALSE;

  //set this to NULL, the object pointed to by mvInfo_ is on the
  //statement heap, for the next statement this will be set again
  //this is set in 'MVInfoForDML *NATable::getMVInfo' which is called
  //in the binder after the construction of the NATable. Therefore
  //This will be set for every statement
  mvInfo_ = NULL;

  //delete/clearAndDestroy colStats_
  //set colStats_ pointer to NULL the object itself is deleted when
  //the statement heap is disposed at the end of a statement
  colStats_ = NULL;

  //mark table as unaccessed for following statements
  accessedInCurrentStatement_ = FALSE;

  //for (i in colArray_) colArray_[i]->setReferenced(FALSE);
  for (UInt32 i = 0; i < colArray_.entries(); i++)
    {
      //reset each NAColumn
      if(colArray_[i])
        colArray_[i]->resetAfterStatement();
    }

  // Reset the set of interesting expressions. We don't use
  // clearAndDestroy() because we want to keep the hash buckets
  // around. Also, the K and V things that this points to are
  // on the Statement Heap, so they have already been destroyed
  // by the time this method, resetAfterStatement(), has been
  // called.
  interestingExpressions_.clear();

  //reset the clustering index
  if(clusteringIndex_)
    clusteringIndex_->resetAfterStatement();

  //reset the fileset for indices
  for (UInt32 j=0; j < indexes_.entries(); j++)
    {
      //reset the fileset for each index
      if(indexes_[j])
        indexes_[j]->resetAfterStatement();
    }

  //reset the fileset for each vertical partition
  for (UInt32 k=0; k < vertParts_.entries(); k++)
    {
      //reset the fileset for each index
      if(vertParts_[k])
        vertParts_[k]->resetAfterStatement();
    }

  // reset the pointers (keyColumns_ in refConstraintsReferencingMe)
  // that are referencing the NATable of the 'other table'.
  uniqueConstraints_.resetAfterStatement();

  // reset the pointers (keyColumns_ in uniqueConstraintsReferencedByMe_)
  // that are referencing the NATable of the 'other table'.
  refConstraints_.resetAfterStatement();

  colsWithMissingStats_ = NULL;

  resetAfterStatement_ = TRUE;
  setupForStatement_ = FALSE;

  sizeAfterLastStatement_ = heap_->getAllocSize();
  return;
}

void NATable::setupForStatement()
{

  if(setupForStatement_)
    return;

  //reset the clustering index
  if(clusteringIndex_)
    clusteringIndex_->setupForStatement();

  //reset the fileset for indices
  for (UInt32 i=0; i < indexes_.entries(); i++)
    {
      //reset the fileset for each index
      if(indexes_[i])
        indexes_[i]->setupForStatement();
    }

  //reset the fileset for each vertical partition
  for (UInt32 j=0; j < vertParts_.entries(); j++)
    {
      //reset the fileset for each index
      if(vertParts_[j])
        vertParts_[j]->setupForStatement();
    }

  // We are doing this here, as we want this to be maintained on a per statement basis
  colsWithMissingStats_ = new (STMTHEAP) NAHashDictionary<CollIndexSet, Int32>
    (&(hashColPosList),107,TRUE,STMTHEAP);


  setupForStatement_ = TRUE;
  resetAfterStatement_ = FALSE;

  return;
}

void NATable::setupForStatementAfterCopyCstr()
{
  interestingExpressions_.clear();
  isHalloweenTable_ = FALSE;
  hitCount_ = 0;
}

static void formatPartitionNameString(const NAString &tbl,
                                      const NAString &pName,
                                      NAString &fmtOut)
{
  fmtOut = NAString("(TABLE ") + tbl +
    ", PARTITION " + pName + ")";
}
static void formatPartitionNumberString(const NAString &tbl,
                                        Lng32 pNumber,
                                        NAString &fmtOut)
{
  char buf[10];
  sprintf(buf, "%d", pNumber);

  fmtOut = NAString("(TABLE ") + tbl +
    ", PARTITION NUMBER " + buf + ")";
}

NABoolean NATable::filterUnusedPartitions(const PartitionClause& pClause)
{
  if (pClause.isEmpty())
    return TRUE;

  if (pClause.isPartitionV2())
    return FALSE;

  if (getViewText())
    {
      *CmpCommon::diags()
        << DgSqlCode(-1276)
        << DgString0(pClause.getPartitionName())
        << DgTableName(getTableName().getQualifiedNameAsString());
      return TRUE;
    }

  if ((pClause.partnNumSpecified() && pClause.getPartitionNumber() < 0) ||
      (pClause.partnNameSpecified() && IsNAStringSpaceOrEmpty(pClause.getPartitionName())))
    // Partion Number specified is less than zero or name specified was all blanks.
    return TRUE ;

  CMPASSERT(indexes_.entries() > 0);
  NAFileSet* baseTable = indexes_[0];
  PartitioningFunction* oldPartFunc = baseTable->getPartitioningFunction();
  CMPASSERT(oldPartFunc);
  const NodeMap* oldNodeMap = oldPartFunc->getNodeMap();
  CMPASSERT(oldNodeMap);
  const NodeMapEntry* oldNodeMapEntry = NULL;
  PartitioningFunction* newPartFunc = NULL; 
  if (pClause.partnRangeSpecified())
    {
      /*      if (NOT oldPartFunc->isAHash2PartitioningFunction())
              {
              // ERROR 1097 Unable to find specified partition...
              *CmpCommon::diags()
              << DgSqlCode(-1097)
              << DgString0("")
              << DgTableName(getTableName().getQualifiedNameAsAnsiString());
              return TRUE;
              }
      */

      NAString errorString;
      // partition range specified
      if ((pClause.getBeginPartitionNumber() == -1) ||
          ((pClause.getBeginPartitionNumber() > 0) &&
           (oldPartFunc->getCountOfPartitions() >= pClause.getBeginPartitionNumber())))
        {
          oldPartFunc->setRestrictedBeginPartNumber(
               pClause.getBeginPartitionNumber());
        }
      else
        {
          formatPartitionNumberString(
               getTableName().getQualifiedNameAsAnsiString(),
               pClause.getBeginPartitionNumber(), errorString);
        }

      if ((pClause.getEndPartitionNumber() == -1) ||
          ((pClause.getEndPartitionNumber() > 0) &&
           (oldPartFunc->getCountOfPartitions() >= pClause.getEndPartitionNumber())))
        {
          oldPartFunc->setRestrictedEndPartNumber(
               pClause.getEndPartitionNumber());
        }
      else
        {
          formatPartitionNumberString(
               getTableName().getQualifiedNameAsAnsiString(),
               pClause.getEndPartitionNumber(), errorString);
        }

      if (NOT errorString.isNull())
        {
          // ERROR 1097 Unable to find specified partition...
          *CmpCommon::diags()
            << DgSqlCode(-1097)
            << DgString0(errorString)
            << DgTableName(getTableName().getQualifiedNameAsAnsiString());
          return TRUE;
        } // Unable to find specified partition.
    } // partition range specified
  else 
    {
      // single partition specified
      if (pClause.getPartitionNumber() >= 0) // PARTITION NUMBER was specified
        { 
          if ((pClause.getPartitionNumber() > 0) &&
              (oldPartFunc->getCountOfPartitions() >= pClause.getPartitionNumber()))
            oldNodeMapEntry = oldNodeMap->getNodeMapEntry(pClause.getPartitionNumber()-1);
          else
            {
              NAString errorString;
              formatPartitionNumberString(getTableName().getQualifiedNameAsAnsiString(),
                                          pClause.getPartitionNumber(), errorString);

              // ERROR 1097 Unable to find specified partition...
              *CmpCommon::diags()
                << DgSqlCode(-1097)
                << DgString0(errorString)
                << DgTableName(getTableName().getQualifiedNameAsAnsiString());
              return TRUE;
            } // Unable to find specified partition.
        }
      else  // PARTITION NAME was specified
        {
          for (CollIndex i =0; i < oldNodeMap->getNumEntries(); i++)
            {
              oldNodeMapEntry = oldNodeMap->getNodeMapEntry(i);
              if (oldNodeMapEntry->getGivenName() == pClause.getPartitionName())
                break;
              if ( i == (oldNodeMap->getNumEntries() -1)) // match not found
                {
                  NAString errorString;
                  formatPartitionNameString(getTableName().getQualifiedNameAsAnsiString(),
                                            pClause.getPartitionName(), errorString);

                  // ERROR 1097 Unable to find specified partition...
                  *CmpCommon::diags()
                    << DgSqlCode(-1097)
                    << DgString0(errorString)
                    << DgTableName(getTableName().getQualifiedNameAsAnsiString());
                  return TRUE;
                }
            }
        }

      if (!isHbaseTable())
        {
          // Create DP2 node map for partitioning function with only the partition requested
          NodeMap* newNodeMap = new (heap_) NodeMap(heap_);
          NodeMapEntry newEntry((char *)oldNodeMapEntry->getPartitionName(),
                                (char *)oldNodeMapEntry->getGivenName(),
                                heap_,oldNodeMap->getTableIdent());
          newNodeMap->setNodeMapEntry(0,newEntry,heap_);
          newNodeMap->setTableIdent(oldNodeMap->getTableIdent());

          /*  if (oldPartFunc->getPartitioningFunctionType() == 
              PartitioningFunction::ROUND_ROBIN_PARTITIONING_FUNCTION)
              {
              // For round robin partitioning, must create the partitioning function
              // even for one partition, since the SYSKEY must be generated for
              // round robin and this is trigger off the partitioning function.
              newPartFunc = new (heap) RoundRobinPartitioningFunction(1, newNodeMap, heap_);
              }
              else */
          newPartFunc = new (heap_) SinglePartitionPartitioningFunction(newNodeMap, heap_);

          baseTable->setPartitioningFunction(newPartFunc);
          baseTable->setCountOfFiles(1);
          baseTable->setHasRemotePartitions(checkRemote(NULL,
                                                        (char *)oldNodeMapEntry->getPartitionName()));
          // for now we are not changing indexlevels_ It could potentially be larger than the 
          // number of index levels for the requested partition.
          QualifiedName physicalName(oldNodeMapEntry->getPartitionName(),
                                     1, heap_, NULL);
          baseTable->setFileSetName(physicalName);
        }
      else
        {
          // For HBase tables, we attach a predicate to select a single partition in Scan::bindNode
          oldPartFunc->setRestrictedBeginPartNumber(pClause.getPartitionNumber());
          oldPartFunc->setRestrictedEndPartNumber(pClause.getPartitionNumber());
        }

    } // single partition specified

  return FALSE;
}

const LIST(CollIndex) &
NATable::getTableIdList() const
{
  return tableIdList_;
}

void NATable::resetReferenceCount()
{ 
  referenceCount_ = 0;
  refsIncompatibleDP2Halloween_ = FALSE; 
  isHalloweenTable_ = FALSE; 
}

void NATable::decrReferenceCount()
{ 
  --referenceCount_; 
  if (referenceCount_ == 0)
    {
      refsIncompatibleDP2Halloween_ = FALSE; 
      isHalloweenTable_ = FALSE;
    }
}

CollIndex NATable::getUserColumnCount() const
{
  CollIndex result = 0;

  for (CollIndex i=0; i<colArray_.entries(); i++)
    if (colArray_[i]->isUserColumn())
      result++;

  return result;
}

void NATable::getNAColumnMap(std::map<Lng32, char *> &colMap, NABoolean lowercase)
{
  for(Int32 i=0;i<colArray_.entries();i++)
  {
    NAColumn * colEntry = colArray_[i];
    NAString colName = colEntry->getColName();
    if (lowercase)
      colName.toLower();
    char * pColName = new(CmpCommon::statementHeap()) char[colName.length() + 1];
    strcpy(pColName, colName.data());
    colMap[colEntry->getPosition()]= pColName;
  }
}
  
NATableDB::NATableDB(NAMemory *h) :
    heap_(h),
    statementTableList_(h),
    statementCachedTableList_(h),
    cachedTableList_(h),
    tablesToDeleteAfterStatement_(h),
    nonCacheableTableIdents_(h),
    nonCacheableTableNames_(h),
    metaDataCached_(FALSE),
    cacheMetaData_(FALSE),
    currentCacheSize_(0),
    preloadedCacheSize_(0),
    defaultCacheSize_(0),
    refreshCacheInThisStatement_(FALSE),
    useCache_(FALSE),
    highWatermarkCache_(0),                                      
    totalLookupsCount_(0),        
    totalCacheHits_(0),
    intervalWaterMark_(0),
    NAKeyLookup<ExtendedQualName,NATable> (NATableDB_INIT_SIZE,
                                           NAKeyLookupEnums::KEY_INSIDE_VALUE,
                                           h),
    hiveMetaDB_(NULL),
    replacementCursor_(0),
    inPreloading_(FALSE),
    objUidToName_(&ComUID::hashKey, NATableDB_INIT_SIZE, TRUE, h)
  {
  }

// NATableDB function definitions
NATable * NATableDB::get(const ExtendedQualName* key, BindWA* bindWA, NABoolean findInCacheOnly)
{
  //get the cached NATable entry
  NATable * cachedNATable =
    NAKeyLookup<ExtendedQualName,NATable>::get(key);

  //entry not found in cache
  if(!cachedNATable)
    return NULL;

  //This flag determines if a cached object should be deleted and
  //reconstructed
  NABoolean removeEntry = FALSE;

  if ( cachedNATable->isHbaseTable() ) {

    const NAFileSet* naSet = cachedNATable -> getClusteringIndex();

    if ( naSet ) {
      PartitioningFunction* pf = naSet->getPartitioningFunction();

      if ( pf ) {
        NABoolean rangeSplitSaltedTable = 
          CmpCommon::getDefault(HBASE_HASH2_PARTITIONING) == DF_OFF ||
          (bindWA && bindWA->isTrafLoadPrep());

        // if force to range partition a salted table, and the salted table is 
        // not a range, do not return the cached object.
        if ( rangeSplitSaltedTable &&
             cachedNATable->hasSaltedColumn() &&
             pf->castToHash2PartitioningFunction() ) {
          removeEntry = TRUE;
        } else 
          // if force to hash2 partition a salted table, and the cached table is 
          // not a hash2, do not return the cached object.
          if ( 
               CmpCommon::getDefault(HBASE_HASH2_PARTITIONING) != DF_OFF &&
               cachedNATable->hasSaltedColumn() &&
               pf->castToHash2PartitioningFunction() == NULL
               )
            removeEntry = TRUE;
      } 
    }
  }

  // the reload cqd will be set during aqr after compiletime and runtime
  // timestamp mismatch is detected.
  // If set, reload hive metadata.
  if ((cachedNATable->isHiveTable()) &&
      (CmpCommon::getDefault(HIVE_DATA_MOD_CHECK) == DF_ON) &&
      (CmpCommon::getDefault(TRAF_RELOAD_NATABLE_CACHE) == DF_ON))
    {
      removeEntry = TRUE;
    }

  // check if hive table is S3 hosted. Normally disable cache for S3 hosted tables as timestamp
  // based invalidation is not possible. Check CQD for forcing caching regardless of lack of timestamp based invalidation
  if ((cachedNATable->isHiveTable()) && !(cachedNATable->isView()) &&
      (cachedNATable->getClusteringIndex()->getHHDFSTableStats()->isS3Hosted()) &&
      (CmpCommon::getDefault(HIVE_NATABLE_CACHE_S3_TABLES) == DF_OFF))
    {
      removeEntry = TRUE;
    }

  //Found in cache.  If that's all the caller wanted, return now.
  if ( !removeEntry && findInCacheOnly )
    return cachedNATable;

  //if this is the first time this cache entry has been accessed
  //during the current statement
  if( !removeEntry && !cachedNATable->accessedInCurrentStatement())
    {
      //Note: cachedNATable->labelDisplayKey_ won't not be NULL
      //for NATable Objects that are in the cache. If the object
      //is not a cached object from a previous statement then we
      //will not come into this code.

      //Read label to get time of last catalog operation
      short error = 0;
      //Get redef time of table
      const Int64 tableRedefTime = cachedNATable->getRedefTime();
      //Get last catalog operation time
      Int64 labelCatalogOpTime = tableRedefTime;
      Int64 currentSchemaRedefTS = 0;
      Int64 cachedSchemaRedefTS = 0;

      if (!OSIM_runningSimulation())
        {
          if ((!cachedNATable->isHiveTable()) &&
              (!cachedNATable->isHbaseTable()))
            {
            } // non-hive table
          else if (!cachedNATable->isHbaseTable())
            {
              // oldest cache entries we will still accept
              // Values for CQD HIVE_METADATA_REFRESH_INTERVAL:
              // -1: Never invalidate any metadata
              //  0: Always check for the latest metadata in the compiler,
              //     no check in the executor
              // >0: Check in the compiler, metadata is valid n seconds
              //     (n = value of CQD). Recompile plan after n seconds.
              //     NOTE: n has to be long enough to compile the statement,
              //     values < 20 or so are impractical.
              Int64 refreshInterval = 
                (Int64) CmpCommon::getDefaultLong(HIVE_METADATA_REFRESH_INTERVAL);
              Int32 defaultStringLenInBytes = 
                CmpCommon::getDefaultLong(HIVE_MAX_STRING_LENGTH_IN_BYTES);
              Int64 expirationTimestamp = refreshInterval;
              NAString defSchema =
                ActiveSchemaDB()->getDefaults().getValue(HIVE_DEFAULT_SCHEMA);
              defSchema.toUpper();

              if (refreshInterval > 0)
                expirationTimestamp = NA_JulianTimestamp() - 1000000 * refreshInterval;

              // if default string length changed, don't reuse this entry
              if (defaultStringLenInBytes != cachedNATable->getHiveDefaultStringLen())
                removeEntry = TRUE;

              QualifiedName objName = cachedNATable->getTableName();
              NAString        sName = objName.getSchemaName();
              const NAString  tName = objName.getObjectName();

              // map the Trafodion default Hive schema (usually "HIVE")
              // to the name used in Hive (usually "default")
              if (objName.getUnqualifiedSchemaNameAsAnsiString() == defSchema)
                sName = hiveMetaDB_->getDefaultSchemaName();

              // validate Hive table timestamps to check if there is change
              // in directory timestamp
              if (hiveMetaDB_->getTableDesc(sName,
                                            tName,
                                            TRUE /*validate only*/,
                                            (CmpCommon::getDefault(TRAF_RELOAD_NATABLE_CACHE) == DF_ON),
					    (CmpCommon::getDefault(HIVE_SUPPORTS_SUBDIRECTORIES) == DF_ON),
					    TRUE) == NULL)
                removeEntry = TRUE;

              // validate HDFS stats and update them in-place, if needed
              if (!removeEntry && (cachedNATable->getClusteringIndex()))
                removeEntry = 
                  ! (cachedNATable->getClusteringIndex()->
                     getHHDFSTableStats()->validateAndRefresh(expirationTimestamp));
            }
        } // ! osim simulation

      //if time of last catalog operation and table redef times
      //don't match, then delete this cache entry since it is
      //stale.
      //if error is non-zero then we were not able to read file
      //label and therefore delete this cached entry because
      //we cannot ensure it is fresh.
      // Note that on an Apache Sentry privilege recheck, we don't
      // need to read the metadata; we just need to reread the 
      // privilege info.
      if((CmpCommon::statement() &&
         CmpCommon::statement()->recompiling() &&
         !(CmpCommon::statement()->recompUseNATableCache()) && 
         !(CmpCommon::statement()->sentryPrivRecheck())) ||
         (labelCatalogOpTime != tableRedefTime )||
         (error)||
         (currentSchemaRedefTS != cachedSchemaRedefTS) ||
         (!usingCache()) ||
         (refreshCacheInThisStatement_))
        {
          //mark this entry to be removed
          removeEntry = TRUE;
        }
    } // !cachedNATable->accessedInCurrentStatement()

  if(removeEntry)
    {
      //remove from list of cached NATables
      cachedTableList_.remove(cachedNATable);

      //remove pointer to NATable from cache
      remove(key);

      //if metadata caching is ON, then adjust cache size
      //since we are deleting a caching entry
      if(cacheMetaData_)
        currentCacheSize_ = heap_->getAllocSize();

      //insert into list of tables that will be deleted
      //at the end of the statement after the query has
      //been compiled and the plan has been sent to the
      //executor. The delete is done in method
      //NATableDB::resetAfterStatement(). This basically
      //gives a little performance saving because the delete
      //won't be part of the compile time as perceived by the
      //client of the compiler
      tablesToDeleteAfterStatement_.insert(cachedNATable);

      return NULL;
    }
  else {
    // Special tables are not added to the statement table list.
    if( (NOT cachedNATable->getExtendedQualName().isSpecialTable()) ||
        (cachedNATable->getExtendedQualName().getSpecialType() == 
         ExtendedQualName::MV_TABLE) || 
        (cachedNATable->getExtendedQualName().getSpecialType() == 
         ExtendedQualName::GHOST_MV_TABLE) ||
        (cachedNATable->getExtendedQualName().getSpecialType() ==
         ExtendedQualName::GHOST_INDEX_TABLE) ||
        (cachedNATable->getExtendedQualName().getSpecialType() ==
         ExtendedQualName::INDEX_TABLE)
        )
      statementTableList_.insert(cachedNATable);
  }

  //increment the replacement, if not already max
  if(cachedNATable)
    {
      cachedNATable->replacementCounter_+=2;

      //don't let replacementcounter go over NATABLE_MAX_REFCOUNT
      if(cachedNATable->replacementCounter_ > NATABLE_MAX_REFCOUNT)
        cachedNATable->replacementCounter_ = NATABLE_MAX_REFCOUNT;

      //Keep track of tables accessed during current statement
      if((!cachedNATable->accessedInCurrentStatement()))
        {
          cachedNATable->setAccessedInCurrentStatement();
          statementCachedTableList_.insert(cachedNATable);
        }
    }

  //return NATable from cache
  return cachedNATable;
}

// by default column histograms are marked to not be fetched, 
// i.e. needHistogram_ is initialized to DONT_NEED_HIST.
// this method will mark columns for appropriate histograms depending on
// where they have been referenced in the query
void NATable::markColumnsForHistograms()
{
  // Check if Show Query Stats command is being run
  NABoolean runningShowQueryStatsCmd = CmpCommon::context()->showQueryStats();

  // we want to get 1 key column that is not SYSKEY
  NABoolean addSingleIntHist = FALSE;
  if(colArray_.getColumn("SYSKEY"))
    addSingleIntHist = TRUE;

  // iterate over all the columns in the table
  for(UInt32 i=0;i<colArray_.entries();i++)
    {
      // get a reference to the column
      NAColumn * column = colArray_[i];

      // is column part of a key
      NABoolean isAKeyColumn = (column->isIndexKey() OR column->isPrimaryKey()
				OR column->isPartitioningKey());

      //check if this column requires histograms
      if(column->isReferencedForHistogram() ||
         (isAKeyColumn && isHbaseTable()))
        column->setNeedFullHistogram();
      else
        // if column is:
        // * a key
        // OR
        // * isReferenced but not for histogram and addSingleIntHist is true
        if (isAKeyColumn ||
            ((runningShowQueryStatsCmd || addSingleIntHist) && 
             column->isReferenced() && !column->isReferencedForHistogram()))
          {
            // if column is not a syskey
            if (addSingleIntHist && (column->getColName() != "SYSKEY"))
              addSingleIntHist = FALSE;

            column->setNeedCompressedHistogram();      
          }
        else
          if (column->getType()->getVarLenHdrSize() &&
              (CmpCommon::getDefault(COMPRESSED_INTERNAL_FORMAT) != DF_OFF ||
               CmpCommon::getDefault(COMPRESSED_INTERNAL_FORMAT_BMO) != DF_OFF ))
            {
              column->setNeedCompressedHistogram();
            }
    }
}

void NATable::addInterestingExpression(NAString & unparsedExpr)
{
  if (!interestingExpressions_.contains(&unparsedExpr))
    { 
      NAString * unparsedExprCopy = new(STMTHEAP) NAString(unparsedExpr,STMTHEAP);
      NABoolean * dummy = new(STMTHEAP) NABoolean;
      *dummy = TRUE;  // doesn't matter what value we use though; it's just a dummy
      interestingExpressions_.insert(unparsedExprCopy,dummy);
    }
}

const QualifiedName& NATable::getFullyQualifiedGuardianName()
{
  //qualified name and fileSetName are different 
  //so we use fileSetName because it will contain
  //fully qualified guardian name
  QualifiedName * fileName;

  if(qualifiedName_.getQualifiedNameObj().getQualifiedNameAsString()
     != fileSetName_.getQualifiedNameAsString())
    {
      fileName = new(CmpCommon::statementHeap()) QualifiedName
        (fileSetName_,CmpCommon::statementHeap());
    }
  else
    {
      fileName = new(CmpCommon::statementHeap()) QualifiedName
        (qualifiedName_.getQualifiedNameObj(),CmpCommon::statementHeap());
    }
  return *fileName;
}

ExtendedQualName::SpecialTableType NATable::getTableType()
{
  return qualifiedName_.getSpecialType();
}

NABoolean NATable::hasSaltedColumn(Lng32 * saltColPos) const
{
  for (CollIndex i=0; i<colArray_.entries(); i++ )
    {
      if ( colArray_[i]->isSaltColumn() ) 
        {
          if (saltColPos)
            *saltColPos = i;
          return TRUE;
        }
    }
  return FALSE;
}

NABoolean NATable::hasTrafReplicaColumn(Lng32 * repColPos) const
{
  for (CollIndex i=0; i<colArray_.entries(); i++ )
    {
      if ( colArray_[i]->isReplicaColumn() )
        {
          if (repColPos)
            *repColPos = i;
          return TRUE;
        }
    }
  return FALSE;
}

Int16 NATable::getNumTrafReplicas() const
{
  if (isView() || getSpecialType() == ExtendedQualName::INDEX_TABLE)
    return 0 ; // for updateable view we need a way to get to underlying clusteringindex
               // for index table columns default values are not currently used, so we don't have a way to get num replicas
               // this will be fixed.
  else if (getClusteringIndex())
    return getClusteringIndex()->numTrafReplicas();
  else { 
    for (CollIndex i=colArray_.entries(); i>0; i-- )
    {
      if ( colArray_[i-1]->isReplicaColumn() )
          return str_atoi(colArray_[i-1]->getDefaultValue(), // for the traf replica col the default value is the
                          str_len(colArray_[i-1]->getDefaultValue())); // number of replicas
    }
    return 0;
  }
}

/*
const NABoolean NATable::hasSaltedColumn(Lng32 * saltColPos) const
{
  return ((NATable*)this)->hasSaltedColumn(saltColPos);
}
*/

NABoolean NATable::hasDivisioningColumn(Lng32 * divColPos)
{
  for (CollIndex i=0; i<colArray_.entries(); i++ )
    {
      if ( colArray_[i]->isDivisioningColumn() ) 
        {
          if (divColPos)
            *divColPos = i;
          return TRUE;
        }
    }
  return FALSE;
}

ComBoolean NATable::usingSentry()
{
 if (msg_license_advanced_enabled() || msg_license_multitenancy_enabled())
  {
    static char * sentryEnv = getenv("SENTRY_SECURITY_FOR_HIVE");
    if (sentryEnv && strcmp(sentryEnv, "TRUE") == 0)
      return TRUE;
  }
  return FALSE;
}

NABoolean NATable::usingLinuxGroups()
{
 if (msg_license_advanced_enabled())
  {
    static char * sentryEnv = getenv("SENTRY_SECURITY_GROUP_MODE");
    if (sentryEnv && strcmp(sentryEnv, "SHELL") == 0)
      return TRUE;
  }
  return FALSE;
}

NABoolean NATable::usingLDAPGroups()
{
 if (msg_license_advanced_enabled())
  {
    static char * sentryEnv = getenv("SENTRY_SECURITY_GROUP_MODE");
    if (sentryEnv && strcmp(sentryEnv, "LDAP") == 0)
      return TRUE;
  }
  return FALSE;
}


// Get the part of the row size that is computable with info we have available
// without accessing HBase. The result is passed to estimateHBaseRowCount(), which
// completes the row size calculation with HBase info.
//
// A row stored in HBase consists of the following fields for each column:
//          -----------------------------------------------------------------------
//          | Key  |Value | Row  | Row  |Column|Column|Column| Time | Key  |Value |
//  Field   |Length|Length| Key  | Key  |Family|Family|Qualif| stamp| Type |      |
//          |      |      |Length|      |Length|      |      |      |      |      |
//          -----------------------------------------------------------------------
//  # Bytes    4      4       2             1                    8      1
//
// The field lengths calculated here are for Row Key, Column Qualif, and Value.
// The size of the Value fields are not known to HBase, which treats cols as
// untyped, so we add up their lengths here, as well as the row key lengths,
// which are readily accessible via Traf metadata. The qualifiers, which represent
// the names of individual columns, are not the Trafodion column names, but
// minimal binary values that are mapped to the actual column names.
// The fixed size fields could also be added in here, but we defer that to the Java
// side so constants of the org.apache.hadoop.hbase.KeyValue class can be used.
// The single column family used by Trafodion is also a known entity, but we
// again do it in Java using the HBase client interface as insulation against
// possible future changes.
Int32 NATable::computeHBaseRowSizeFromMetaData() const
{
  Int32 partialRowSize = 0;
  Int32 rowKeySize = 0;
  const NAColumnArray& keyCols = clusteringIndex_->getIndexKeyColumns();
  CollIndex numKeyCols = keyCols.entries();

  // For each column of the table, add the length of its value and the length of
  // its name (HBase column qualifier). If a given column is part of the primary
  // key, add the length of its value again, because it is part of the HBase row
  // key.
  for (Int32 colInx=0; colInx<colcount_; colInx++)
    {
      // Get length of the column qualifier and its data.
      NAColumn* col = colArray_[colInx];;
      Lng32 colLen = col->getType()->getNominalSize(); // data length
      Lng32 colPos = col->getPosition();  // position in table

      partialRowSize += colLen;

      // The qualifier is not the actual column name, but a binary value
      // representing the ordinal position of the col in the table.
      // Single byte is used if possible.
      partialRowSize++;
      if (colPos > 255)
        partialRowSize++;

      // Add col length again if a primary key column, because it will be part
      // of the row key.
      NABoolean found = FALSE;
      for (CollIndex keyColInx=0; keyColInx<numKeyCols && !found; keyColInx++)
        {
          if (colPos == keyCols[keyColInx]->getPosition())
            {
              rowKeySize += colLen;
              found = TRUE;
            }
        }
    }

  partialRowSize += rowKeySize;
  return partialRowSize;
}

// For an HBase table, we can estimate the number of rows by dividing the number
// of KeyValues in all HFiles of the table by the number of columns (with a few
// other considerations).
Int64 NATable::estimateHBaseRowCount(Int32 retryLimitMilliSeconds, Int32& errorCode, Int32& breadCrumb) const
{
  Int64 estRowCount = 0;
  ExpHbaseInterface* ehi = getHBaseInterface();
  errorCode = -1;
  breadCrumb = -1;
  if (ehi)
    {
      HbaseStr fqTblName;
      NAString tblName = CmpSeabaseDDL::genHBaseObjName(
           getTableName().getCatalogName(),
           getTableName().getSchemaName(),
           getTableName().getObjectName(),
           ((NATable *)this)->getNamespace(),
           objDataUID_.get_value());

      if (getTableName().isHbaseCellOrRow())
        tblName = getTableName().getObjectName();
      else if (getTableName().isHbaseMappedName())
        {
          if (NOT ((NATable*)this)->getNamespace().isNull())
            tblName = ((NATable*)this)->getNamespace() + ":" + getTableName().getObjectName();
          else
            tblName = getTableName().getObjectName();
        }
      fqTblName.len = tblName.length();
      fqTblName.val = new(STMTHEAP) char[fqTblName.len+1];
      strncpy(fqTblName.val, tblName.data(), fqTblName.len);
      fqTblName.val[fqTblName.len] = '\0';

      Int32 partialRowSize = computeHBaseRowSizeFromMetaData();
      NABoolean useCoprocessor = 
        (CmpCommon::getDefault(HBASE_ESTIMATE_ROW_COUNT_VIA_COPROCESSOR) == DF_ON);

      // Note: If in the future we support a hybrid parially aligned +
      // partially non-aligned format, the following has to be revised.
      CollIndex keyValueCountPerRow = 1;  // assume aligned format
      if (!isAlignedFormat(NULL))
        keyValueCountPerRow = colcount_;  // non-aligned; each column is a separate KV

      errorCode = ehi->estimateRowCount(fqTblName,
                                      partialRowSize,
                                      keyValueCountPerRow,
                                      retryLimitMilliSeconds,
                                      useCoprocessor,
                                      estRowCount /* out */,
                                      breadCrumb /* out */);
      NADELETEBASIC(fqTblName.val, STMTHEAP);

      // Return 100 million as the row count if an error occurred while
      // estimating. One example where this is appropriate is that we might get
      // FileNotFoundException from the Java layer if a large table is in 
      // the midst of a compaction cycle. It is better to return a large
      // number in this case than a small number, as plan quality suffers
      // more when we vastly underestimate than when we vastly overestimate.

      // The estimate could be 0 if there is less than 1MB of storage
      // dedicated to the table -- no HFiles, and < 1MB in MemStore, for which
      // size is reported only in megabytes.
      if (errorCode < 0)
        estRowCount = 100000000;

      delete ehi;
    }

  return estRowCount;
}

void NATable::getConnectParams(ComStorageType storageType, char **connectParam1, char **connectParam2) 
{
  NADefaults* defs = &ActiveSchemaDB()->getDefaults();
  switch (storageType) 
  {
     case COM_STORAGE_MONARCH:
        *connectParam1 = (char *)defs->getValue(MONARCH_LOCATOR_ADDRESS);
        *connectParam2 = (char *)defs->getValue(MONARCH_LOCATOR_PORT);
        break;
     case COM_STORAGE_BIGTABLE:
        *connectParam1 = (char *)defs->getValue(BIGTABLE_PROJECT_ID);
        *connectParam2 = (char *)defs->getValue(BIGTABLE_INSTANCE_ID);
        break;
     case COM_STORAGE_HBASE:
        *connectParam1 = (char *)defs->getValue(HBASE_SERVER);
        *connectParam2 = (char *)defs->getValue(HBASE_ZOOKEEPER_PORT);
        break;
     default:
        *connectParam1 = NULL;
        *connectParam2 = NULL;
        break;
  }
  return;
}


// Method to get hbase regions servers node names
ExpHbaseInterface* NATable::getHBaseInterface() const
{
  if (!isHbaseTable() || isSeabaseMDTable() ||
      getExtendedQualName().getQualifiedNameObj().getObjectName() == HBASE_HISTINT_NAME ||
      getExtendedQualName().getQualifiedNameObj().getObjectName() == HBASE_HIST_NAME ||
      getSpecialType() == ExtendedQualName::VIRTUAL_TABLE)
    return NULL;
  return NATable::getHBaseInterfaceRaw(storageType_);
}

ExpHbaseInterface* NATable::getHBaseInterfaceRaw(ComStorageType storageType) 
{
   char *connectParam1;
   char *connectParam2;
   NATable::getConnectParams(storageType, &connectParam1, &connectParam2);
   NABoolean replSync = FALSE;
   ExpHbaseInterface* ehi = ExpHbaseInterface::newInstance
       (STMTHEAP, connectParam1, connectParam2,
       storageType,
       replSync);

   Lng32 retcode = ehi->init(NULL);
   if (retcode < 0) {
      *CmpCommon::diags()
        << DgSqlCode(-8448)
        << DgString0((char*)"ExpHbaseInterface::init()")
        << DgString1(getHbaseErrStr(-retcode))
        << DgInt0(-retcode)
        << DgString2((char*)GetCliGlobals()->getJniErrorStr());
      delete ehi;
      return NULL;
   }

   return ehi;
}

NAArray<HbaseStr> *NATable::getRegionsBeginKey(const char* hbaseName) 
{
  ExpHbaseInterface* ehi = getHBaseInterfaceRaw(COM_STORAGE_HBASE);
  NAArray<HbaseStr> *keyArray = NULL;

  if (!ehi)
    return NULL;
  else
    {
      keyArray = ehi->getRegionBeginKeys(hbaseName);

      delete ehi;
    }
  return keyArray;
}


NABoolean  NATable::getRegionsNodeName(Int32 partns, ARRAY(const char *)& nodeNames ) const
{
  ExpHbaseInterface* ehi = getHBaseInterface();

  if (!ehi)
    return FALSE;
  else
    {
      HbaseStr fqTblName;

      CorrName corrName(getTableName());

      NAString tblName;
      if (corrName.isHbaseCell() || corrName.isHbaseRow())
        tblName = corrName.getQualifiedNameObj().getObjectName();
      else
        tblName = CmpSeabaseDDL::genHBaseObjName(
             getTableName().getCatalogName(),
             getTableName().getSchemaName(),
             getTableName().getObjectName(),
             ((NATable *)this)->getNamespace(),
             objDataUID_.get_value());
      
      fqTblName.len = tblName.length();
      fqTblName.val = new(STMTHEAP) char[fqTblName.len+1];
      strncpy(fqTblName.val, tblName.data(), fqTblName.len);
      fqTblName.val[fqTblName.len] = '\0';

      Lng32 retcode = ehi->getRegionsNodeName(fqTblName, partns, nodeNames);

      NADELETEBASIC(fqTblName.val, STMTHEAP);

      delete ehi;
      if (retcode < 0)
        return FALSE;
    }
  return TRUE;

}

// Method to get hbase table index levels and block size
NABoolean  NATable::getHbaseTableInfo(Int32& hbtIndexLevels, Int32& hbtBlockSize) const
{
  ExpHbaseInterface* ehi = getHBaseInterface();
  if (!ehi)
    return FALSE;
  else
    {
      HbaseStr fqTblName;
      NAString tblName = CmpSeabaseDDL::genHBaseObjName(
           getTableName().getCatalogName(),
           getTableName().getSchemaName(),
           getTableName().getObjectName(),
           ((NATable *)this)->getNamespace(),
           objDataUID_.get_value());
      
      fqTblName.len = tblName.length();
      fqTblName.val = new(STMTHEAP) char[fqTblName.len+1];
      strncpy(fqTblName.val, tblName.data(), fqTblName.len);
      fqTblName.val[fqTblName.len] = '\0';
      
      Lng32 retcode = ehi->getHbaseTableInfo(fqTblName,
                                             hbtIndexLevels,
                                             hbtBlockSize);
      
      NADELETEBASIC(fqTblName.val, STMTHEAP);
      delete ehi;
      if (retcode < 0)
        return FALSE;
    }
  return TRUE;
}

int NATable::getNumReplications() 
{
   int numReplications = -1;
   if (isSeabaseMDTable()) { 
      if (CmpCommon::getDefault(TRAF_METADATA_READ_REPLICA) == DF_OFF)
          return numReplications;
   }
   else {
      if (CmpCommon::getDefault(HBASE_READ_REPLICA) == DF_OFF)
          return numReplications;
   }

   NAList<HbaseCreateOption*> *hbaseOptions = hbaseCreateOptions();
   if (hbaseOptions != NULL && hbaseOptions->entries() > 0) {
      for (CollIndex i = 0; i < hbaseOptions->entries(); i++) {
         HbaseCreateOption *hbaseOption = hbaseOptions->at(i);
         if (hbaseOption->key() == "REGION_REPLICATION") {
            numReplications = atoi(hbaseOption->val().c_str());
            break;
         }
      }
   }
   return numReplications;
}

// This method is called on a hive/orc NATable.
// If that table has a corresponding external table,
// then this method moves the relevant attributes from 
// NATable of external table (etTable) to this.
// Currently, column and clustering key info is moved.
short NATable::updateExtTableAttrs(NATable *etTable)
{
  NAFileSet *fileset = this->getClusteringIndex();
  NAFileSet *etFileset = etTable->getClusteringIndex();
  Int32 numUserCols = getUserColumnCount();

  if (numUserCols != etTable->getUserColumnCount())
    return -1;

  // save the original colArray_
  for (CollIndex i = 0; i < colArray_.entries(); i++)
    {
      NAColumn * nac =
        new(heap_) NAColumn(*colArray_[i], heap_);
	                          
      hiveColArray_.insert(nac);

      // set the hive type of the external NATable to the same value
      // that was specified in the underlying hive NATable 
      if (nac->isUserColumn()) {
	NAType *lv_type = (NAType *) etTable->getNAColumnArray()[i]->getType();
        lv_type->
          setHiveType(nac->getType()->getHiveType());
      }
    }

  // save the virtual columns, those are not contained in the
  // external table
  colArray_.removeAt(0, numUserCols);
  colArray_.insertArray(etTable->getNAColumnArray(),
                        0, // insert at position 0
                        0, // stat with et column 0
                        numUserCols);

  // mark the partition columns in the external table
  NAColumnArray &myCols = fileset->allColumns_;

  for (CollIndex i=0; i<myCols.entries(); i++) {
    if (myCols[i]->isHivePartColumn())
      etFileset->markAsHivePartitioningColumn(i);
    if (i < numUserCols)
      colArray_[i]->setNATable(this) ; // NATable ptr in NAColumn got 
    // overwritten to be etTable in insertArray step above. etTable is typically
    // discarded after this step.
  }

  // assert that the general layout of the columns hasn't changed,
  // user columns first, followed by virtual columns, and the fileset
  // columns are the same as the base table columns
  CMPASSERT(myCols.entries() == colArray_.entries());
  for (CollIndex u=numUserCols; u<myCols.entries(); u++)
    CMPASSERT(myCols[u]->isHiveVirtualColumn());

  myCols.removeAt(0, numUserCols);
  myCols.insertArray(etFileset->getAllColumns(),
                     0, // insert at position 0
                     0, // stat with et column 0
                     numUserCols);

  // Note that the key cannot be changed with an external table

  /* those can't be changed with an external table, either
     fileset->partitioningKeyColumns_ = etFileset->getPartitioningKeyColumns();
     fileset->partFunc_ = etFileset->getPartitioningFunction();
     fileset->countOfFiles_ = etFileset->getCountOfFiles();
  */

  return 0;
}

void NATable::updateStoredStats(TrafDesc *tableDesc)
{
  if (storedStatsDesc_)
  {
    TrafDesc::deleteDescList(storedStatsDesc_, heap_);
    storedStatsDesc_ = NULL;
  }
  if (tableDesc &&
      tableDesc->tableDesc() &&
      tableDesc->tableDesc()->table_stats_desc &&
      tableDesc->tableDesc()->table_stats_desc->tableStatsDesc())
  {
    storedStatsDesc_ = TrafDesc::copyDescList(
             tableDesc->tableDesc()->table_stats_desc->tableStatsDesc(), 
             heap_); 
  }
}

void getDelimeterPos(char* source, const char* del, IntegerList &posAry)
{
  Int32 strLen = str_len(source);
  NABoolean inQuate = false, inBracket = false;

  for (Int32 i = 0; i < strLen; i++)
  {
    if (strncmp(source + i, "(", 1) == 0)
      inBracket = true;
    else if (inBracket && strncmp(source + i, ")", 1) == 0)
      inBracket = false;
    else if (inQuate && strncmp(source + i, "'", 1) == 0)
      inQuate = false;
    else if (strncmp(source + i, "'", 1) == 0)
      inQuate = true;
    else if (strncmp(source + i, del, 1) == 0)
    {
      if ( NOT inQuate && NOT inBracket)
      {
        posAry.insert(i);
      }
    }
  }
}

static NABoolean createNAPartitions(TrafDesc *partition_desc,
                                    Int32 partCnt,
                                    LIST(NAString) &colnameAry, 
                                    NAPartitionArray &partitions,
                                    NAMemory *heap)
{
  if (partition_desc == NULL ||
      partCnt == 0 ||
      colnameAry.entries() == 0)
    return TRUE;

  TrafDesc* part_desc_list = partition_desc;
 
  Int32 colCnt = colnameAry.entries(); 

  for (int i = 0; i < partCnt && part_desc_list != NULL; i++)
  {
    TrafPartDesc* part_desc = part_desc_list->partDesc();
    NAPartition *newPartition = NULL;
    Int32 plen = str_len(part_desc->partitionName) + 1;
    char *partName = new (heap)char[plen];
    memcpy(partName, part_desc->partitionName, plen);

    plen = str_len(part_desc->partitionEntityName) + 1;
    char* entityName = new (heap)char[plen];
    memcpy(entityName, part_desc->partitionEntityName, plen);

    newPartition = new (heap)
           NAPartition(heap,
                       part_desc->parentUid,
                       part_desc->partitionUid,
                       partName,
                       entityName,
                       part_desc->isSubPartition,
                       part_desc->hasSubPartition,
                       part_desc->partPosition,
                       part_desc->isValid,
                       part_desc->isReadonly,
                       part_desc->isInMemory,
                       part_desc->defTime,
                       part_desc->flags,
                       part_desc->subpartitionCnt);

    char *bValue[2];
    if (part_desc->prevPartitionValueExpr != NULL)
    {
      plen = str_len(part_desc->prevPartitionValueExpr) + 1;
      bValue[0] = new (heap)char[plen];
      memcpy(bValue[0], part_desc->prevPartitionValueExpr, plen);
    }
    else
    {
      bValue[0] = NULL;
    }

    plen = str_len(part_desc->partitionValueExpr) + 1;
    bValue[1] = new (heap)char[plen];
    memcpy(bValue[1], part_desc->partitionValueExpr, plen);

    LIST(BoundaryValue*)& boundValueList = newPartition->getBoundaryValueList();
    char deli[2] = ",";

    NABoolean first = true;
    for (int k = 0; k < 2; k++)
    {
      char *valueExp = bValue[k];
      if (valueExp == NULL)
        continue;

      BoundaryValue* boundValue = NULL;
      IntegerList delPos;
      getDelimeterPos(valueExp, deli, delPos);

      Int32 colPos = 0, strLen = str_len(valueExp);
      for (int p = 0; p <= delPos.entries(); p++)
      {
        Int32 tokLen = 0, offset = 0;
        if (first)
          boundValue = new (heap)BoundaryValue();
        else
          boundValue = boundValueList[colPos];

        if (p == 0)
        {
          offset == 0;
          if (delPos.entries() == 0)
            tokLen = strLen;
          else
            tokLen = delPos[p];
        }
        else if (p == delPos.entries())
        {
          tokLen = strLen - delPos[p - 1] - 1;
          offset = delPos[p - 1] + 1; 
        }
        else
        {
          tokLen = delPos[p] - delPos[p - 1] - 1;
          offset = delPos[p - 1] + 1;
        }
 
        char *value = new (heap)char[tokLen + 1];
        memset(value, '\0', tokLen + 1);
        memcpy(value, valueExp + offset, tokLen);

        if (k == 1 && NAString("MAXVALUE").compareTo(value) == 0)
        {
          newPartition->maxValueBitSet(colPos);
        }

        if (k == 0)
          boundValue->lowValue = value;
        else
          boundValue->highValue = value; 
        if (first)  
          boundValueList.insert(boundValue); 
        colPos++;
      }
      first = false;
    }

    partitions.insert(newPartition);

    part_desc_list = part_desc_list->next;

    for (int i = 0; i < 2; i++)
      if (bValue[i] != NULL)
        NADELETEBASIC(bValue[i], heap);
  }

  return FALSE; 
}

const char* NATable::partitionSchemeToString(ComPartitioningSchemeV2 ps)
{
  switch(ps) {
    case COM_NO_PARTITION :
      return "NO PARTITION";
    break;
    case COM_HASH_PARTITION :
      return "HASH";
    break;
    case COM_RANGE_PARTITION :
      return "RANGE";
    break;
    case COM_LIST_PARTITION :
      return "LIST";
    break;

    default:
      return "UNKNOWN PARTITION TYPE";
  }
}

void NATable::getParitionColNameAsString(NAString &target, NABoolean getSubParType) const
{
  Int32 *idxAry = 0;
  Int32 size = 0;
  if (getSubParType)
    idxAry = subpartitionColIdxAry_, size = subpartitionColCount_;
  else
    idxAry = partitionColIdxAry_, size = partitionColCount_;

  if (idxAry == NULL || size == 0)
    return;

  for (int i = 0; i < size; i++)
  {
    if (i == 0)
      target += ToAnsiIdentifier(colArray_[idxAry[i]]->getColName());
    else
      target += ", " + ToAnsiIdentifier(colArray_[idxAry[i]]->getColName());
  }
}

void NATable::displayPartitonV2(FILE* ofd, const char* indent,
                                const char* title,
                                CollHeap *c, char *buf)
{
  Space * space = (Space *)c;
  char mybuf[1000];
  NAString colName, subColName;
  
  getParitionColNameAsString(colName, FALSE);
  getParitionColNameAsString(subColName, TRUE);

  snprintf(mybuf, 1000, "Partition Type : %s, Partition Column : %s, "
          "Subartition Type : %s, Subpartition Column : %s, "
          "Fisrt-Level Partition Count : %d\n",
          partitionSchemeToString((ComPartitioningSchemeV2)partitionType_),
          colName.data(),
          partitionSchemeToString((ComPartitioningSchemeV2)subpartitionType_),
          subColName.data(),
          stlPartitionCnt_);
  PRINTIT(ofd, c, space, buf, mybuf);

  partArray_.print(ofd, indent, title, c, buf);
}

// get details of this NATable cache entry
void NATableDB::getEntryDetails(
     Int32 ii,                      // (IN) : NATable cache iterator entry
     NATableEntryDetails &details)  // (OUT): cache entry's details
{
  Int32      NumEnt = cachedTableList_.entries();
  if  ( ( NumEnt == 0 ) || ( NumEnt <= ii ) )
    {
      memset(&details, 0, sizeof(details));
    }
  else {
    NATable * object = cachedTableList_[ii];
    QualifiedName QNO = object->qualifiedName_.getQualifiedNameObj();

    Int32 partLen = QNO.getCatalogName().length();
    strncpy(details.catalog, (char *)(QNO.getCatalogName().data()), partLen );
    details.catalog[partLen] = '\0';

    partLen = QNO.getSchemaName().length();
    strncpy(details.schema, (char *)(QNO.getSchemaName().data()), partLen );
    details.schema[partLen] = '\0';

    partLen = QNO.getObjectName().length();
    strncpy(details.object, (char *)(QNO.getObjectName().data()), partLen );
    details.object[partLen] = '\0';

    details.hasPriv = (ComUser::isRootUserID()) ? 1 : 0;
    if (details.hasPriv == 0)
    {
      if (object->privInfo_ && object->privInfo_->hasAnyPriv())
        details.hasPriv = 1;
    }
      
    details.size = object->sizeInCache_;
  }
}


NABoolean NATableDB::isHiveTable(CorrName& corrName)
{
  return corrName.isHive();
}

NABoolean NATableDB::isSQUtiDisplayExplain(CorrName& corrName)
{
  const char* tblName = corrName.getQualifiedNameObj().getObjectName();
  if ( !strcmp(tblName, "EXE_UTIL_DISPLAY_EXPLAIN__"))
    return TRUE;

  if ( !strcmp(tblName, "EXPLAIN__"))
    return TRUE;

  if ( !strcmp(tblName, "HIVEMD__"))
    return TRUE;

  if ( !strcmp(tblName, "DESCRIBE__"))
    return TRUE;

  if ( !strcmp(tblName, "EXE_UTIL_EXPR__"))
    return TRUE;

  if ( !strcmp(tblName, "STATISTICS__"))
    return TRUE;

  return FALSE;
}

NABoolean NATableDB::isSQInternalStoredProcedure(CorrName& corrName)
{
  const char* tblName = corrName.getQualifiedNameObj().getObjectName();

  if ( !strncmp(tblName, "SPTableOutQUERYCACHEENTRIES",
                         strlen("SPTableOutQUERYCACHEENTRIES")))
    return TRUE;

  if ( !strncmp(tblName, "SPTableOutQUERYCACHEDELETE",
                         strlen("SPTableOutQUERYCACHEDELETE")))
    return TRUE;

  if ( !strncmp(tblName, "SPTableOutQUERYCACHE",
                         strlen("SPTableOutQUERYCACHE")))
    return TRUE;

  if ( !strncmp(tblName, "SPTableOutHYBRIDQUERYCACHEENTRIES",
                         strlen("SPTableOutHYBRIDQUERYCACHEENTRIES")))
    return TRUE;

  if ( !strncmp(tblName, "SPTableOutHYBRIDQUERYCACHE",
                         strlen("SPTableOutHYBRIDQUERYCACHE")))
    return TRUE;

  return FALSE;
}


NABoolean NATableDB::isSQUmdTable(CorrName& corrName)
{
  return FALSE;
}

// Helper function for NATableDB::get(CorrName & ...
//
// This function obtains the current epoch and current flags
// from the RMS segment ObjectEpochCache.
//
// Input parameters:
// corrName - the name of the object
// createIfNotExists - if TRUE, we'll create the ObjectEpochCacheEntry
//   if none currently exists
// writeReference - if TRUE, the object reference is a write reference;
//   otherwise it is a read reference
//
// Output parameters:
// Return value - 0 if success, SqlCode if failure (diags are populated)
// found - set to TRUE if an entry was found (or created)
// currentEpoch - set to current epoch if found is TRUE, set to zero if found is FALSE
// currentFlags - set to current epoch if found is TRUE, set to zero if found is FALSE
// 
Int32 accessObjectEpochCache(CorrName& corrName,
                             NABoolean createIfNotExists,
                             NABoolean writeReference,
                             NABoolean & found /* out */,
                             UInt32 & currentEpoch /* out */,
                             UInt32 & currentFlags /* out */)
{
  Int32 retcode = 0;  // assume success
  currentEpoch = 0;
  currentFlags = 0;
  found = TRUE;  // will pretend it is found if it is metadata

  if (CmpCommon::getDefault(TRAF_LOCK_DDL) == DF_OFF)
    return 0;

  if (corrName.isSeabase() && !corrName.isSeabaseMD() && !corrName.isSeabasePrivMgrMD()
      && !corrName.isVolatile())
  {
    found = FALSE;  // now be pessimistic

    // For the moment, we put COM_BASE_TABLE_OBJECT here. When we extend to UDRs we
    // may need to revise this.
    NAString externalName = corrName.getQualifiedNameAsString();
    ObjectEpochCacheEntryName oecName(STMTHEAP, 
                                      externalName,
                                      COM_BASE_TABLE_OBJECT,
                                      0);

    CliGlobals *cliGlobals = GetCliGlobals();
    StatsGlobals *statsGlobals = cliGlobals->getStatsGlobals();
    if (!statsGlobals)
    {
      // statsGlobals can be NULL if sqlci is started before sqstart completes.
      // Rather than have an annoying core, we'll return an error.
      retcode = -4401;
      *CmpCommon::diags() << DgSqlCode(-4401) << DgString0(externalName.data())
        << DgString1("RMS Segment is not yet available");
      return retcode;
    }
    ObjectEpochCache *objectEpochCache = statsGlobals->getObjectEpochCache();
    ObjectEpochCacheEntry *oecEntry = NULL;

    ObjectEpochCacheKey key(externalName, oecName.getHash(), COM_BASE_TABLE_OBJECT);

    short geeCode = objectEpochCache->getEpochEntry(oecEntry /* out */,key);
    if (geeCode == ObjectEpochCache::OEC_ERROR_OK)
    {
      found = TRUE;
      currentEpoch = oecEntry->epoch();
      currentFlags = oecEntry->flags();
    }
    else if (geeCode == ObjectEpochCache::OEC_ERROR_ENTRY_NOT_EXISTS)
    {
      if (createIfNotExists)
      {
        // No entry currently exists in the ObjectEpochCache for this name,
        // so create one
        retcode = CmpSeabaseDDL::createObjectEpochCacheEntry(&oecName);
        if (retcode)
        {
          // unexpected error; diags area has been populated
          return retcode;
        }
        else
        {
          geeCode = objectEpochCache->getEpochEntry(oecEntry /* out */,key);
          if (geeCode == ObjectEpochCache::OEC_ERROR_OK)
          {
            currentEpoch = oecEntry->epoch();
            currentFlags = oecEntry->flags();
          }
          else if ((geeCode == ObjectEpochCache::OEC_ERROR_ENTRY_NOT_EXISTS) &&
                   (CmpCommon::getDefaultLong(TRAF_OBJECT_EPOCH_CACHE_FULL_BEHAVIOR) == 2))
          {
            // When the Object Epoch Cache is full, and CQD TRAF_OBJECT_EPOCH_CACHE_FULL_BEHAVIOR
            // is set to 2 (meaning, ignore the cache full condition), then we will arrive here
            // if we were unable to create an object epoch cache entry above. We'll treat this
            // as a "found" condition but use zero epoch and flags in this case.
            found = TRUE;
            currentEpoch = 0;
            currentFlags = 0;
            retcode = 0; 
          }
          else
          {
            // Unexpected error
            char buf[100];
            sprintf(buf,"Internal error, %d returned on getEpochEntry call",(int)geeCode);
            *CmpCommon::diags() << DgSqlCode(-4401) << DgString0(externalName.data())
              << DgString1(buf);
            retcode = -4401;
          }
        }
      }
      else  // didn't find an entry, and it's not time to create one
      {
        retcode = 0;  // success, though found is FALSE
      }
    }
    else  // unexpected error
    {
      // Unexpected error
      char buf[100];
      sprintf(buf,"Internal error, %d returned on getEpochEntry call",(int)geeCode);
      *CmpCommon::diags() << DgSqlCode(-4401) << DgString0(externalName.data())
        << DgString1(buf);
      retcode = -4401;
    }

    // Check to see if a DDL operation is in flight. If it is, and we are doing 
    // a normal compile, we'll try to wait out the DDL operation. If on the
    // other hand, we are internal, then it must be us that is doing the DDL
    // operation. (Or at least likely. It's possible a development user is
    // setting the parser flags for test purposes.) If it is internal, we assume
    // we know what we are doing and allow use of the metadata as-is. If the
    // reference is a read reference, and reads are allowed, we'll just use
    // the current epoch.

    if ((retcode == 0) && (!writeReference) && 
        ((currentFlags & ObjectEpochChangeRequest::READS_DISALLOWED) == 0))
      currentFlags = 0;  // read reference + reads allowed; just zero out the flags

    if ((retcode == 0) && 
        (currentFlags & ObjectEpochChangeRequest::DDL_IN_PROGRESS) &&
        (Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL) == 0) &&
        (CmpCommon::context()->ddlObjsList().findEntry(externalName) < 0))
    {
      // A concurrent DDL operation is in progress, so the metadata is in flux
      retcode = -4401;  // assume failure
      if (!createIfNotExists)
      {
        // sleep and retry in hopes that the DDL operation will complete; do this
        // for up to TRAF_CONCURRENT_DDL_WAIT_RETRIES seconds
        int retryLimit = (ActiveSchemaDB()->getDefaults()).getAsLong(TRAF_CONCURRENT_DDL_WAIT_RETRIES);
        for (int retry = 0; (retry < retryLimit) && (retcode); retry++)
        {
          sleep(1);
          geeCode = objectEpochCache->getEpochEntry(oecEntry /* out */,key);
          if ((geeCode == ObjectEpochCache::OEC_ERROR_OK) &&
              ((oecEntry->flags() & ObjectEpochChangeRequest::DDL_IN_PROGRESS) == 0))
          {
            currentEpoch = oecEntry->epoch();
            currentFlags = oecEntry->flags();
            retcode = 0;
          }        
        }
      }
      if (retcode)
      {
        // A concurrent DDL operation is in progress; raise an error
        *CmpCommon::diags() << DgSqlCode(retcode) << DgString0(externalName.data())
          << DgString1("A concurrent DDL operation is in progress");
      }
    }
  }

  return retcode;
}

  
//#define DEBUG_NATABLEDB_GET 1

NATable * NATableDB::get(CorrName& corrName, BindWA * bindWA,
                         TrafDesc *inTableDescStruct, NABoolean writeReference){

  // get the current epoch and flags from the ObjectEpochCache (if any); do this
  // before doing an actual metadata read so we can detect later if it suddenly changed
  UInt32 currentEpoch = 0;
  UInt32 currentFlags = 0;
  NABoolean found = FALSE;
  Int32 accessRetcode = 0;

  if (CmpCommon::getDefault(TRAF_LOCK_DDL) == DF_ON)
  {
    accessRetcode = accessObjectEpochCache(corrName,false /* don't create if not found */,
      writeReference, found /* out */, currentEpoch /* out */, currentFlags /* out */);
    if (accessRetcode < 0)
    {
      Int32 ddlListEntry = CmpCommon::context()->ddlObjsList().findEntry(corrName.getQualifiedNameAsString());
      if (ddlListEntry < 0)
        {
          // Diags already set, just return null
          bindWA->setErrStatus();
          return NULL;
        }
    }
  }
  else
    found = TRUE;

  //check cache to see if a cached NATable object exists
  NATable *table = get(&corrName.getExtendedQualNameObj(), bindWA);  

#ifdef DEBUG_NATABLEDB_GET
   cout << "<<<On ENTER NATableDB::get():"
        <<  ", key=" << corrName.getQualifiedNameObj().getQualifiedNameAsAnsiString() 
        <<  ", initial lookup=" << static_cast<void*>(table)
        << endl;

    if ( CmpCommon::context()->getCliContext() )
      CmpCommon::context()->getCliContext()->displayAllCmpContexts();

    cout << ">>>" << endl << endl;

    parquet::StopWatch s1;
    s1.Start();
#endif
         
  // The caller can set the in preloading state (default to FALSE)
  // in advance. Here that state is transfer to variable inPreload.
  NABoolean inPreload = isInPreloading();

  if ( !table && !inTableDescStruct && 
       !corrName.isSeabaseMD() &&
       !corrName.isSeabasePrivMgrMD() ) {
    // check the shared cache for a trafDesc keyed on 
    // corrName.getExtendedQualNameObj()
  
    SharedDescriptorCache* sharedDescCache = 
       SharedDescriptorCache::locate();
    if (sharedDescCache &&
       (CmpCommon::getDefault(TRAF_ENABLE_METADATA_LOAD_IN_SHARED_CACHE) == DF_ON))
    {
      const ExtendedQualName& extQualName =
            corrName.getExtendedQualNameObj();

      const QualifiedName& qualName = 
            extQualName.getQualifiedNameObj();

      NAString* tableDescStr = NULL;
      int retcode = 0;
      OBSERVE_SHARED_CACHE_LOCK;
      if (retcode != 0) {
         NAString msg ("Unable to OBSERVE_SHARED_CACHE_LOCK ");
         msg += qualName.getQualifiedNameAsAnsiString();
         msg += " because failed to getSemaphore()"; 
         QRLogger::log(CAT_SQL_EXE, LL_WARN, "%s", msg.data());
         goto bailout_label;
      }

      if (CmpCommon::getDefault(TRAF_CHECK_SHARED_CACHE_LOCKED_AT_LOOKUP) == DF_ON)
      {
        // If shared cache is locked (someone else is updating the entry), skip
        // the shared cache lookup and get definition from metadata.
        // We use the DistributedLockObserver class which looks for an existing
        // shared lock entry. No distributed locks are obtained.
        Int64 retryLimit = (ActiveSchemaDB()->getDefaults()).getAsLong(TRAF_CONCURRENT_DDL_WAIT_RETRIES);
        NABoolean sharedCacheLocked = TRUE;
        for (int retry = 0; (retry < retryLimit); retry++)
        {
          DistributedLockObserver dlock(SHARED_CACHE_DLOCK_KEY,0);
          if (!dlock.lockHeld())
          {
            tableDescStr = sharedDescCache->find(qualName);
            sharedCacheLocked = FALSE;
            break;
          }
        }
       

        // Instead of returning an error if the cache is locked, we continue and
        // retrieve the table definition from metadata. If for some reason, the
        // shared cache entry becomes locked then we can continue to use the table.
        if (sharedCacheLocked)
          {
            NAString msg ("Unable to look up table ");
            msg += qualName.getQualifiedNameAsAnsiString();
            msg += " because shared cache is locked.  Checking for entry in metadata."; 
            QRLogger::log(CAT_SQL_EXE, LL_WARN, "%s", msg.data());
          }
      }
      else
        tableDescStr = sharedDescCache->find(qualName);

      if ( tableDescStr ) 
      {
        inTableDescStruct = 
           CmpSeabaseDDL::decodeStoredDesc(*tableDescStr);

        // If the table desc can be found in the shared cache,
        // we define this as an in-preload event.
        inPreload = TRUE;

#ifdef DEBUG_SHARED_CACHE_LOOKUP
       cout << "Getting table desc for table " 
            << corrName.getQualifiedNameObj().getQualifiedNameAsAnsiString()
            << " from shared cache and decode()" 
            << endl;
#endif
      }
bailout_label:
      UNOBSERVE_SHARED_CACHE_LOCK;
    }
  }

  if (table && (corrName.isHbase() || corrName.isSeabase()) && inTableDescStruct)
    {
      remove(table->getKey());
      table = NULL;
    }

  if (table && ((table->isHbaseTable() || table->isSeabaseTable()) && 
                !(table->isSeabaseMDTable())))
    {
      if (CmpCommon::getDefault(TRAF_RELOAD_NATABLE_CACHE) == DF_ON)
	{
	  remove(table->getKey());
	  table = NULL;
	}
    }

  if (table && table->isHiveTable())
    {
      if (CmpCommon::getDefault(TRAF_RELOAD_NATABLE_CACHE) == DF_ON)
	{
	  remove(table->getKey());
	  table = NULL;
	}
    }

  if (table && (corrName.isHbaseCell() || corrName.isHbaseRow()))
    {
      if (NOT HbaseAccess::validateVirtualTableDesc(table))
        {
          remove(table->getKey());
          table = NULL;
        }
      else if (table->objectUid() == 0)
        {
          remove(table->getKey());
          table = NULL;
        }
    }

  if (table && bindWA && bindWA->isTrafLoadPrep())
    {
      remove(table->getKey());
      table = NULL;
    }

  // for caching statistics
  if ((cacheMetaData_ && useCache_) && corrName.isCacheable())
  {
      //One lookup counted
      ++totalLookupsCount_;
      if (table) ++totalCacheHits_;  //Cache hit counted
  }

  NABoolean isMV = (table && table->isAnMV());
  ComObjectType objectType = COM_BASE_TABLE_OBJECT;

  if (NOT table ||
      (NOT isMV && table->getSpecialType() != corrName.getSpecialType())) {

    // in open source, only the SEABASE catalog is allowed.
    // Return an error if some other catalog is being used.
    if ((NOT corrName.isHbase()) &&
	(NOT corrName.isSeabase()) &&
	(NOT corrName.isHive()) &&
	(corrName.getSpecialType() != ExtendedQualName::VIRTUAL_TABLE))
      {
	*CmpCommon::diags()
	  << DgSqlCode(-1002)
	  << DgCatalogName(corrName.getQualifiedNameObj().getCatalogName())
	  << DgString0("");
	
	bindWA->setErrStatus();
	return NULL;
      }
    
    // If this is a 'special' table, generate a table descriptor for it.
    //
    if (NOT inTableDescStruct && corrName.isSpecialTable())
      inTableDescStruct = generateSpecialDesc(corrName);

    //Heap used by the NATable object
    NAMemory * naTableHeap = CmpCommon::statementHeap();
    size_t allocSizeBefore = 0;

    //if NATable caching is on check if this table is not already
    //in the NATable cache. If it is in the cache create this NATable
    //on the statment heap, since the cache can only store one value per
    //key, therefore all duplicates (two or more different NATable objects
    //that have the same key) are deleted at the end of the statement.
    //ALSO
    //We don't cache any special tables across statements. Please check
    //the class ExtendedQualName for method isSpecialTable to see what
    //are special tables
    if (((NOT table) && cacheMetaData_ && useCache_) &&
        corrName.isCacheable()){
      naTableHeap = getHeap();
      allocSizeBefore = naTableHeap->getAllocSize();
    }

    //if table is in cache tableInCache will be non-NULL
    //otherwise it is NULL.
    NATable * tableInCache = table;

    CmpSeabaseDDL cmpSBD((NAHeap *)CmpCommon::statementHeap());

    if ((corrName.isHbase() || corrName.isSeabase()) &&
	(!isSQUmdTable(corrName)) &&
	(!isSQUtiDisplayExplain(corrName)) &&
	(!isSQInternalStoredProcedure(corrName))
	) {
      // ------------------------------------------------------------------
      // Create an NATable object for a Trafodion/HBase table
      // ------------------------------------------------------------------
      TrafDesc *tableDesc = NULL;

      NABoolean isSeabase = FALSE;
      NABoolean isSeabaseMD = FALSE;
      NABoolean isUserUpdatableSeabaseMD = FALSE;
      NABoolean isHbaseCell = corrName.isHbaseCell();
      NABoolean isHbaseRow = corrName.isHbaseRow();
      NABoolean isHbaseMap = corrName.isHbaseMap();

      NATable* masterNATable = NULL;



      if (isHbaseCell || isHbaseRow)// explicit cell or row format specification
	{
	  const char* extHBaseName = corrName.getQualifiedNameObj().getObjectName();
	  if (cmpSBD.existsInHbase(extHBaseName, NULL) != 1)
	    {
	      *CmpCommon::diags()
		<< DgSqlCode(-1389)
		<< DgString0(corrName.getQualifiedNameObj().getObjectName());
	      
	      bindWA->setErrStatus();
	      return NULL;
	    }

          NAArray<HbaseStr> *keyArray = NATable::getRegionsBeginKey(extHBaseName);

          // create the virtual table descriptor on the same heap that
          // we are creating the NATable object on
	  tableDesc = 
	    HbaseAccess::createVirtualTableDesc
	    (corrName.getExposedNameAsAnsiString(FALSE, TRUE).data(),
	     isHbaseRow, isHbaseCell, keyArray, naTableHeap);
          deleteNAArray(STMTHEAP, keyArray);

	  isSeabase = FALSE;

	}
      else if (corrName.isSeabaseMD())
	{
          // If we are constructing an NATable for a system table
          // in ith compiler instance (i>1), try to clone it with
          // the same NATable object in the 1st compiler instance.
          //
          // Some measurement of the elapsed time taken to clone:
          //
          //  NATable for "_MD_".OBJECTS: 119us
          //  NATable for "_MD_".TEXT: 49us
          //
          // If we take the path of construction via the table 
          // descriptor, the corresponidng ET is much more. 
          //
          // 1. For "_MD_".OBJECTS:  ET = 88us + 407us = 495 us
          // 2. For "_MD_".TEXT:  ET = 52us + 284us = 356us
          //
          // For more details, please refer to Mantis 11894.
	  if (CmpCommon::context()->getCIClass() == 
                    CmpContextInfo::CMPCONTEXT_TYPE_META &&
              CmpCommon::context()->getCIindex() > 1) 
          {
             // get the master NATable for the MD table.
             masterNATable = CmpCommon::context()->
               getNATableFromFirstMetaCI(corrName.getExtendedQualNameObj()); 
          }

	  if (masterNATable) {
            // Do nothing here. No need to grab the Table Desc as 
            // the master NATable wil be cloned instead 
            // (fast code path).
          } else
	  if (corrName.isSpecialTable() && corrName.getSpecialType() == ExtendedQualName::INDEX_TABLE)
	    {
	      tableDesc = 
		cmpSBD.getSeabaseTableDesc(
					   corrName.getQualifiedNameObj().getCatalogName(),
					   corrName.getQualifiedNameObj().getSchemaName(),
					   corrName.getQualifiedNameObj().getObjectName(),
					   COM_INDEX_OBJECT);
              objectType = COM_INDEX_OBJECT;
	    }
	  else if (corrName.isSpecialTable() && corrName.getSpecialType() == ExtendedQualName::SCHEMA_TABLE)
	    {
	      tableDesc = 
		cmpSBD.getSeabaseTableDesc(
					   corrName.getQualifiedNameObj().getCatalogName(),
					   corrName.getQualifiedNameObj().getSchemaName(),
					   corrName.getQualifiedNameObj().getObjectName(),
					   COM_PRIVATE_SCHEMA_OBJECT);
              objectType = COM_PRIVATE_SCHEMA_OBJECT;
	    }
	  else
	    {
	      tableDesc = 
		cmpSBD.getSeabaseTableDesc(
					   corrName.getQualifiedNameObj().getCatalogName(),
					   corrName.getQualifiedNameObj().getSchemaName(),
					   corrName.getQualifiedNameObj().getObjectName(),
					   COM_BASE_TABLE_OBJECT,
                                           FALSE,
                                           bindWA->getBRLockedObjectsAccess());
	      if (tableDesc)
		{
		  if (cmpSBD.isUserUpdatableSeabaseMD(
						      corrName.getQualifiedNameObj().getCatalogName(),
						      corrName.getQualifiedNameObj().getSchemaName(),
						      corrName.getQualifiedNameObj().getObjectName()))
		    isUserUpdatableSeabaseMD = TRUE;
		}
	    }

	  isSeabase = TRUE;
	  isSeabaseMD = TRUE;

#ifdef DEBUG_NATABLEDB_GET
   cout << "Creating NATable with tableDesc for MD table " 
        << (corrName.getQualifiedNameObj()).getQualifiedNameAsAnsiString()
        << ", entries=" << entries()
        << ", current cliContext=" << static_cast<void*>(CmpCommon::context()->getCliContext())
        << ", current cmpContext: this=" << static_cast<void*>(CmpCommon::context())
        << ", class=" << CmpCommon::context()->getCIClass() 
        << ", NONE=" << CmpContextInfo::CMPCONTEXT_TYPE_NONE
        << ", META=" << CmpContextInfo::CMPCONTEXT_TYPE_META
        << ", USTATS=" << CmpContextInfo::CMPCONTEXT_TYPE_USTATS
        << endl;

   if ( CmpCommon::context()->getCliContext() )
     CmpCommon::context()->getCliContext()->displayAllCmpContexts();
#endif
	}
      else // if (!corrName.isSeabaseMD()), or base tables
        {
          
          isSeabase = TRUE;
          if (corrName.isSpecialTable())
          {
            switch (corrName.getSpecialType())
            {
              case ExtendedQualName::INDEX_TABLE:
              {
                objectType = COM_INDEX_OBJECT;
                break;
              }
              case ExtendedQualName::SG_TABLE:
              {
                objectType = COM_SEQUENCE_GENERATOR_OBJECT;
                isSeabase = FALSE;
                break;
              }
              case ExtendedQualName::LIBRARY_TABLE:
              {
                objectType = COM_LIBRARY_OBJECT;
                isSeabase = FALSE;
                break;
              }
              case ExtendedQualName::SCHEMA_TABLE:
              {
                objectType = COM_PRIVATE_SCHEMA_OBJECT;
                isSeabase = FALSE;
                break;
              }
              case ExtendedQualName::VIRTUAL_TABLE:
              {
                isSeabase = FALSE;
                break;
              }
              default: //TODO: No SpecialTableType for UDFs/Routines/COM_USER_DEFINED_ROUTINE_OBJECT
              {
                objectType = COM_BASE_TABLE_OBJECT;
              }
            }
          }

          if (! inTableDescStruct)
            tableDesc = cmpSBD.getSeabaseTableDesc(
                 corrName.getQualifiedNameObj().getCatalogName(),
                 corrName.getQualifiedNameObj().getSchemaName(),
                 corrName.getQualifiedNameObj().getObjectName(),
                 objectType,
                 FALSE,
                 bindWA->getBRLockedObjectsAccess());

#ifdef DEBUG_NATABLEDB_GET
   cout << "Creating NATable with tableDesc for base seabase table " 
        << (corrName.getQualifiedNameObj()).getQualifiedNameAsAnsiString()
        << ", entries=" << entries()
        << ", current cliContext=" << static_cast<void*>(CmpCommon::context()->getCliContext())
        << ", current cmpContext: this=" << static_cast<void*>(CmpCommon::context())
        << ", class=" << CmpCommon::context()->getCIClass() 
        << ", NONE=" << CmpContextInfo::CMPCONTEXT_TYPE_NONE
        << ", META=" << CmpContextInfo::CMPCONTEXT_TYPE_META
        << ", USTATS=" << CmpContextInfo::CMPCONTEXT_TYPE_USTATS
        << endl;

   if ( CmpCommon::context()->getCliContext() )
     CmpCommon::context()->getCliContext()->displayAllCmpContexts();
#endif

        }

      NABoolean inError = FALSE;

      if ( masterNATable ) {
	table = new (naTableHeap) NATable(*masterNATable, 
                                          naTableHeap, 
                                          masterNATable->getHeapType()
                                         );

#ifdef DEBUG_NATABLE_COPY_CSTR
        NABoolean ok = (*table) == *masterNATable;
        cout << "Compare equal=" << ok << endl;
#endif

        table->setupForStatementAfterCopyCstr();
      } else {

         if (inTableDescStruct)
            tableDesc = inTableDescStruct;

         if (tableDesc) {
           if (!found && (objectType == COM_BASE_TABLE_OBJECT)) { 
             // If it is a base table and the ObjectEpochCacheEntry didn't exist above,
             // create it now.
             // TODO: Think about whether to use the returned epoch value;
             // it is conceivable that the epoch changed due to a DDL operation
             // after we read the metadata but before we made this call. We
             // could instead simply use the zero values returned earlier; if
             // a DDL operation has happened or this is a node up situation, 
             // we'll end up in AQR hopefully. 
             accessRetcode = accessObjectEpochCache(corrName,TRUE /* create if not found */,
               writeReference, found /* out */, currentEpoch /* out */, currentFlags /* out */);  
             // will ignore any errors here, using zero epoch values in that case     
           }
	   table = new (naTableHeap) 
                     NATable(bindWA, corrName, naTableHeap, tableDesc, 
                             currentEpoch, currentFlags);
           if (objectType != COM_BASE_TABLE_OBJECT) {
             table->setDDLValidationRequired(FALSE); // DDL validation applies only to tables
             table->setUserBaseTable(FALSE);
           }
         }

         inError = (!tableDesc || bindWA->errStatus());
      }

      if (!table || inError)
	{
	  if (isSeabase)
	    *CmpCommon::diags()
	      << DgSqlCode(-4082)
	      << DgTableName(corrName.getExposedNameAsAnsiString());
	  else
	    *CmpCommon::diags()
	      << DgSqlCode(-1389)
	      << DgString0(corrName.getExposedNameAsAnsiString());
	  
	  bindWA->setErrStatus();
	  return NULL;
	}

      table->setIsHbaseCellTable(isHbaseCell);
      table->setIsHbaseRowTable(isHbaseRow);
      table->setIsSeabaseTable(isSeabase);
      table->setIsSeabaseMDTable(isSeabaseMD);
      table->setIsUserUpdatableSeabaseMDTable(isUserUpdatableSeabaseMD);
      if (isHbaseMap)
        {
          table->setIsHbaseMapTable(TRUE);
          table->setIsTrafExternalTable(TRUE);
        }
    }
    else if (isHiveTable(corrName) &&
             (!isSQUmdTable(corrName)) &&
             (!isSQUtiDisplayExplain(corrName)) &&
             (!corrName.isSpecialTable()) &&
             (!isSQInternalStoredProcedure(corrName))
	) {
      // ------------------------------------------------------------------
      // Create an NATable object for a Hive table
      // ------------------------------------------------------------------
      if ( hiveMetaDB_ == NULL ) {
	if (CmpCommon::getDefault(HIVE_USE_FAKE_TABLE_DESC) != DF_ON)
	  {
	    hiveMetaDB_ = new (CmpCommon::contextHeap()) HiveMetaData((NAHeap *)CmpCommon::contextHeap());
	    
	    if ( !hiveMetaDB_->init() ) {
	      *CmpCommon::diags() << DgSqlCode(-1190)
                                  << DgString0(hiveMetaDB_->getErrMethodName())
                                  << DgString1(hiveMetaDB_->getErrCodeStr())
                                  << DgString2(hiveMetaDB_->getErrDetail())
                                  << DgInt0(hiveMetaDB_->getErrCode());
	      bindWA->setErrStatus();
	      
	      NADELETEBASIC(hiveMetaDB_, CmpCommon::contextHeap());
	      hiveMetaDB_ = NULL;
	      
	      return NULL;
	    }
	  }
	else
	  hiveMetaDB_ = new (CmpCommon::contextHeap()) 
            HiveMetaData((NAHeap *)CmpCommon::contextHeap()); // fake metadata
      }
      
      // this default schema name is what the Hive default schema is called in SeaHive
       NAString defSchema = ActiveSchemaDB()->getDefaults().getValue(HIVE_DEFAULT_SCHEMA);
       defSchema.toUpper();
       struct hive_tbl_desc* htbl;
       NAString schemaNameInt = corrName.getQualifiedNameObj().getSchemaName();
       if (schemaNameInt.compareTo(defSchema, NAString::ignoreCase) == 0)
         schemaNameInt = hiveMetaDB_->getDefaultSchemaName();

       if (CmpCommon::getDefault(HIVE_USE_FAKE_TABLE_DESC) == DF_ON)
         htbl = hiveMetaDB_->getFakedTableDesc(corrName.getQualifiedNameObj().getObjectName());
       else
         {
           htbl = hiveMetaDB_->getTableDesc(
                schemaNameInt,
                corrName.getQualifiedNameObj().getObjectName(),
                FALSE,
                // reread Hive Table Desc from MD.
                (CmpCommon::getDefault(TRAF_RELOAD_NATABLE_CACHE) == DF_ON),
		(CmpCommon::getDefault(HIVE_SUPPORTS_SUBDIRECTORIES) == DF_ON),
                TRUE);
         }

       NAString extName = ComConvertNativeNameToTrafName(
            corrName.getQualifiedNameObj().getCatalogName(),
            corrName.getQualifiedNameObj().getSchemaName(),
            corrName.getQualifiedNameObj().getUnqualifiedObjectNameAsAnsiString());

       QualifiedName qn(extName, 3);

       if ( htbl )
	 {
           table = new (naTableHeap) NATable
             (bindWA, corrName, naTableHeap, htbl);

           if ((NOT table->isView()) && (table->hasCompositeColumns()))
             {
               if (CmpCommon::getDefault(HIVE_COMPOSITE_DATATYPE_SUPPORT) == DF_OFF)
                 {
                   *CmpCommon::diags() << DgSqlCode(-3242)
                                       << DgString0("Composite columns are not allowed for hive tables.");
                   bindWA->setErrStatus();
                   return NULL;   
                 }
               
               if (NOT table->isParquet())
                 {
                   *CmpCommon::diags() << DgSqlCode(-3242)
                                       << DgString0("Composite columns are not allowed in non-parquet hive tables.");
                   bindWA->setErrStatus();
                   return NULL;               
                 }
             } // composite columns

           // 'table' is the NATable for underlying hive table.
           // That table may also have an associated external table.
           // Skip processing the external table defn, if only the 
           // underlying hive table is needed. Skip processing also
           // if there were errors creating the NATable object 
           // (which is indicated by a getColumnCount() value of 0).
           if ((NOT bindWA->returnHiveTableDefn()) &&
               (table->getColumnCount() > 0) )
             {
               // if this hive/orc table has an associated external table, 
               // get table desc for it.
               TrafDesc *etDesc = cmpSBD.getSeabaseTableDesc(
                    qn.getCatalogName(),
                    qn.getSchemaName(),
                    qn.getObjectName(),
                    COM_BASE_TABLE_OBJECT,
                    TRUE);
               
               if (table && etDesc)
                 {
                   CorrName cn(qn);
                   NATable * etTable = new (STMTHEAP) NATable
                     (bindWA, cn, naTableHeap, etDesc, 0, 0);  // TODO: do we care about epochs for external tables?
		   // etTable is discarded after this if block. It is not
		   // cached.
                   
                   // if ext and hive columns dont match, return error
                   // unless it is a drop stmt.
                   if ((table->getUserColumnCount() != etTable->getUserColumnCount()) &&
                       (NOT bindWA->externalTableDrop()))
                     {
                       *CmpCommon::diags()
                         << DgSqlCode(-8437);
                       
                       bindWA->setErrStatus();
                       return NULL;
                     }
                   
                   if (etTable->hiveExtColAttrs() || etTable->hiveExtKeyAttrs())
                     {
                       // attrs were explicitly specified for this external
                       // table. Merge them with the hive table attrs.
                       short rc = table->updateExtTableAttrs(etTable);
                       if (rc)
                         {
                           bindWA->setErrStatus();
                           return NULL;
                         }
                     }
                   table->setHasHiveExtTable(TRUE);
                 } // ext table 
             } // allowExternalTables
         } // htbl
       else 
         {
            // find out if this table has an associated external table.
            TrafDesc *etDesc = cmpSBD.getSeabaseTableDesc(
                 qn.getCatalogName(),
                 qn.getSchemaName(),
                 qn.getObjectName(),
                 COM_BASE_TABLE_OBJECT);
         
           // find out if this table is registered in traf metadata
           Int64 regObjUid = 0;
           // is this a base table
           regObjUid = lookupObjectUidByName(corrName.getQualifiedNameObj(),
                                             COM_BASE_TABLE_OBJECT, FALSE);
           if (regObjUid <= 0)
             {
               // is this a view
               regObjUid = lookupObjectUidByName(corrName.getQualifiedNameObj(),
                                                 COM_VIEW_OBJECT, FALSE);
             }

            // if this is to drop an external table and underlying hive
            // tab doesn't exist, return NATable for external table.
            if ((etDesc) &&
                (bindWA->externalTableDrop()))
              {
                CorrName cn(qn);
                table = new (naTableHeap) NATable
                  (bindWA, cn, naTableHeap, etDesc, 0, 0);  // TODO: Do we care about epochs for external tables?
              }
            else
              {
                if ((hiveMetaDB_->getErrCode() == 0)||
                    (hiveMetaDB_->getErrCode() == 100))
                  {
                    if (etDesc)
                     {
                       *CmpCommon::diags()
                         << DgSqlCode(-4262)
                         << DgString0(corrName.getExposedNameAsAnsiString());
                     }
                   else if (regObjUid > 0) // is registered
                     {
                       *CmpCommon::diags()
                         << DgSqlCode(-4263)
                         << DgString0(corrName.getExposedNameAsAnsiString());
                     }
                   else
                     {
                       *CmpCommon::diags()
                         << DgSqlCode(-1388)
                         << DgString0("Object")
                         << DgString1(corrName.getExposedNameAsAnsiString());
                     }
                  }
                else
                  {
                    *CmpCommon::diags()
                      << DgSqlCode(-1192)
                      << DgString0(hiveMetaDB_->getErrMethodName())
                      << DgString1(hiveMetaDB_->getErrCodeStr())
                      << DgString2(hiveMetaDB_->getErrDetail())
                      << DgInt0(hiveMetaDB_->getErrCode());

                    hiveMetaDB_->resetErrorInfo();
                  } 

               bindWA->setErrStatus();
               return NULL;
              } // else
       }

    }
    else if (isHiveTable(corrName) &&
             corrName.isSpecialTable() &&
             (corrName.getSpecialType() == 
              ExtendedQualName::SCHEMA_TABLE))
      {
        // ------------------------------------------------------------------
        // Create an NATable object for a special Hive table
        // ------------------------------------------------------------------
        if ( hiveMetaDB_ == NULL ) 
          {
            hiveMetaDB_ = new (CmpCommon::contextHeap()) HiveMetaData((NAHeap *)CmpCommon::contextHeap());
            
            if ( !hiveMetaDB_->init() ) 
              {
                *CmpCommon::diags() << DgSqlCode(-1190)
                                    << DgString0(hiveMetaDB_->getErrMethodName())
                                    << DgString1(hiveMetaDB_->getErrCodeStr())
                                    << DgString2(hiveMetaDB_->getErrDetail())
                                    << DgInt0(hiveMetaDB_->getErrCode());
                bindWA->setErrStatus();
                
                NADELETEBASIC(hiveMetaDB_, CmpCommon::contextHeap());
                hiveMetaDB_ = NULL;
                
                return NULL;
              }
          }
      
        // this default schema name is what the Hive default schema is called in SeaHive
        NAString defSchema = ActiveSchemaDB()->getDefaults().getValue(HIVE_DEFAULT_SCHEMA);
        defSchema.toUpper();
        struct hive_tbl_desc* htbl;
        NAString schemaNameInt = corrName.getQualifiedNameObj().getSchemaName();
        if (schemaNameInt.compareTo(defSchema, NAString::ignoreCase) == 0)
          schemaNameInt = hiveMetaDB_->getDefaultSchemaName();
        // Hive stores names in lower case
        // Right now, just downshift, could check for mixed case delimited
        // identifiers at a later point, or wait until Hive supports delimited identifiers
        schemaNameInt.toLower();
        
        // check if this hive schema exists in hiveMD
        LIST(NAText*) tblNames(naTableHeap);
        HVC_RetCode rc =
          HiveClient_JNI::getAllTables((NAHeap *)naTableHeap, schemaNameInt, tblNames);
        if ((rc != HVC_OK) && (rc != HVC_DONE))
          {
            *CmpCommon::diags()
              << DgSqlCode(-1192)
              << DgString0(hiveMetaDB_->getErrMethodName())
              << DgString1(hiveMetaDB_->getErrCodeStr())
              << DgString2(hiveMetaDB_->getErrDetail())
              << DgInt0(hiveMetaDB_->getErrCode());
            
            hiveMetaDB_->resetErrorInfo();
            
            bindWA->setErrStatus();
            return NULL;
          }

        htbl = new(naTableHeap) hive_tbl_desc
          ((NAHeap *)naTableHeap, 0, 
           corrName.getQualifiedNameObj().getObjectName(),
           corrName.getQualifiedNameObj().getSchemaName(),
           NULL, NULL,
           0, NULL, NULL, NULL, NULL, NULL);
        table = new (naTableHeap) NATable
          (bindWA, corrName, naTableHeap, htbl);
        
      }
    else
      // ------------------------------------------------------------------
      // Neither Trafodion nor Hive (probably dead code below)
      // ------------------------------------------------------------------
       table = new (naTableHeap)
         NATable(bindWA, corrName, naTableHeap, inTableDescStruct, 0, 0);
    
    CMPASSERT(table);
    
    if (bindWA && bindWA->errStatus())
      return NULL;

    table->setIsPreloaded(inPreload);
    
    //if there was a problem in creating the NATable object
    if (NOT ((table->getExtendedQualName().isSpecialTable()) &&
	     ((table->getExtendedQualName().getSpecialType() == 
	      ExtendedQualName::SG_TABLE) ||
	     (table->getExtendedQualName().getSpecialType() == 
	      ExtendedQualName::SCHEMA_TABLE))) &&
	(table->getColumnCount() == 0)) {

      bindWA->setErrStatus();
      
      return NULL;
    }

    // Special tables are not added to the statement table list.
    // Index tables are added to the statement table list
    if(  (NOT table->getExtendedQualName().isSpecialTable()) ||
         (table->getExtendedQualName().getSpecialType() == 
             ExtendedQualName::INDEX_TABLE) ||
         (table->getExtendedQualName().getSpecialType() == 
             ExtendedQualName::MV_TABLE) ||
	 (table->getExtendedQualName().getSpecialType() == 
             ExtendedQualName::GHOST_MV_TABLE) ||
         (table->getExtendedQualName().getSpecialType() ==
             ExtendedQualName::GHOST_INDEX_TABLE)
         || (table->getExtendedQualName().getSpecialType() ==
             ExtendedQualName::SCHEMA_TABLE)
      )
      statementTableList_.insert(table);

    //if there was no entry in cache associated with this key then
    //insert it into cache.
    //if there is already a value associated with this in the cache
    //then don't insert into cache.
    //This might happen e.g. if we call this method twice for the same table
    //in the same statement.
    if(!tableInCache){

       //insert into cache
	insert(table);

      //if we are using the cache or in preloading,
      //and this NATable object is cacheable
      if((useCache_ || isInPreloading()) &&
         (corrName.isCacheable()))
      {
        //insert into list of all cached tables;
        cachedTableList_.insert(table);

        //insert into list of cached tables accessed
        //during this statement
        statementCachedTableList_.insert(table);

        //if metadata caching is ON then adjust the size of the cache
        //since we are adding an entry to the cache
        if(cacheMetaData_)
          {
            currentCacheSize_ = heap_->getAllocSize();
            table->sizeInCache_ = currentCacheSize_ - allocSizeBefore;
          }

	//update the high watermark for caching statistics
	if (currentCacheSize_ > highWatermarkCache_)        
	  highWatermarkCache_ = currentCacheSize_;                  
        //
        // the CompilerTrackingInfo highWaterMark gets reset on each
        // tracking interval so it is tracked independently
        if (currentCacheSize_ > intervalWaterMark_)          
          intervalWaterMark_ = currentCacheSize_;  

        //if we are caching metadata and previously the cache was
        //empty set this flag to TRUE to indicate that there is
        //something in the cache
        if(!metaDataCached_ && cacheMetaData_)
          metaDataCached_ = TRUE;

        //enforce the cache memory constraints
        if(!enforceMemorySpaceConstraints())
        {
          //was not able to get cache size below
          //max allowed cache size
          #ifndef NDEBUG
          //          CMPASSERT(FALSE);
          #endif
        }
      }
      else{
        //this has to be on the context heap since we need
        //it after the statement heap has been remove
        ExtendedQualName * nonCacheableTableName = new(CmpCommon::contextHeap())
                               ExtendedQualName(corrName.getExtendedQualNameObj(),
                                                CmpCommon::contextHeap());
        //insert into list of names of special tables
        nonCacheableTableNames_.insert(nonCacheableTableName);

        // insert into list of non cacheable table idents.  This
        // allows the idents to be removed after the statement so
        // the context heap doesn't keep growing.
        const LIST(CollIndex) & tableIdList = table->getTableIdList();
        for(CollIndex i = 0; i < tableIdList.entries(); i++)
        {
          nonCacheableTableIdents_.insert(tableIdList[i]);
        }
      }
    }
  }

  //setup this NATable object for use in current statement
  //if this object has already been setup earlier in the
  //statement then this method will just return without doing
  //anything

  if(table) {
    table->setupForStatement();
  }

#ifdef DEBUG_NATABLEDB_GET
  cout << "<<<On EXIT NATableDB::get():"
       << ", key=" << (corrName.getQualifiedNameObj()).getQualifiedNameAsAnsiString() 
       << ", ET=" << s1.Stop() << "us" << endl;
#endif

  return table;
}

TrafDesc *NATableDB::getTableDescFromCacheOrText(const ExtendedQualName& extQualName,
                                                 Int64 objectUid,
                                                 NAHeap ** descHeap)
{
  if (extQualName.getSpecialType() != ExtendedQualName::NORMAL_TABLE)
    return NULL;

  const QualifiedName& qualName = 
                       extQualName.getQualifiedNameObj();

  SharedDescriptorCache* sharedDescCache = 
            SharedDescriptorCache::locate();

  TrafDesc *tableDesc = NULL;
  if (sharedDescCache &&
      CmpCommon::getDefault(TRAF_ENABLE_METADATA_LOAD_IN_SHARED_CACHE) == DF_ON)
  {
    NAString* tableDescStr = NULL;
    int retcode = 0;
    OBSERVE_SHARED_CACHE_LOCK;
    if (retcode != 0) {
      NAString msg ("Unable to OBSERVE_SHARED_CACHE_LOCK ");
      msg += qualName.getQualifiedNameAsAnsiString();
      msg += " because failed to getSemaphore()"; 
      QRLogger::log(CAT_SQL_EXE, LL_WARN, "%s", msg.data());
      goto release_lock;
    }
    tableDescStr = sharedDescCache->find(qualName);
    if (tableDescStr)
      tableDesc = CmpSeabaseDDL::decodeStoredDesc(*tableDescStr);

release_lock:
    UNOBSERVE_SHARED_CACHE_LOCK;      
  }

  if (tableDesc)
  {
    if (descHeap)
      *descHeap = ActiveSchemaDB()->getNATableDB()->getHeap();
    return tableDesc;
  }

  //read from metadata
  CmpSeabaseDDL cmpSBD((NAHeap *)CmpCommon::statementHeap());
  if (cmpSBD.switchCompiler(CmpContextInfo::CMPCONTEXT_TYPE_META))
    return NULL;
  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, 
                  CmpCommon::context()->sqlSession()->getParentQid()); 
  short retcode =
        cmpSBD.checkAndGetStoredObjectDesc(
                   &cliInterface, objectUid,
                   &tableDesc, FALSE,
                   getHeap());
  if (tableDesc && descHeap)
    *descHeap = (getHeap()) ? (getHeap()) : STMTHEAP;
  cmpSBD.switchBackCompiler();

  if (retcode == 0)
  {
    CmpCommon::diags()->clear();
    CmpCommon::diags()->negateAllErrors();
  }

  return tableDesc;
}

void removeFromAllUsers(const QualifiedName& objName,
                        ComQiScope qiScope,
                        ComObjectType ot,
                        NASet<Int64>& objectUIDs,
                        NABoolean ddlXns, NABoolean atCommit,
                        NABoolean noCheck)
{
  // DDL xns enabled and committing, clear out other user's cache will
  // be handled in CmpSeabaseDDL::ddlInvalidateNATables.
  if (ddlXns && atCommit)
    {
      return;
    }

  // There are some scenarios where the affected object
  // does not have an NATable cache entry. Need to get one and
  // add its objectUID to the set.
  if (0 == objectUIDs.entries())
    {
      Int64 ouid = lookupObjectUidByName(
                       objName,
                       ot,
                       FALSE);
      if (ouid > 0)
        objectUIDs.insert(ouid);
    }

  Int32 numKeys = objectUIDs.entries();
  if (numKeys == 0)
    {
      return;
    }

  if ((ddlXns) &&
      (NOT atCommit))
    {
      for (CollIndex i = 0; i < numKeys; i++)
        {
          // TDB - pass redef time and expectedEpoch
          // Or have the caller preset the DDLObjInfo entry
          DDLObjInfo ddlObj;
          ddlObj.setObjName(objName.getQualifiedNameAsAnsiString());
          ddlObj.setQIScope(qiScope);
          ddlObj.setObjType(ot);
          ddlObj.setObjUID(objectUIDs[i]);
          ddlObj.setDDLOp(FALSE);
          if (CmpCommon::transMode()->savepointInProgress())
            ddlObj.setSvptId(CmpCommon::transMode()->getCurrSaveointId());

          // With Epoch support, there should always be an entry in the ddlObjsList
          // If not, should we continue.  If not continue what values to set for Epoch?

#if 0
          // TODO: replace this temporary fix
          // For the moment, just get the expectedEpoch out of the RMS ObjectEpochCache
          // directly. This will work fine so long as there is just one DDL operation
          // on the object in a given transaction. For multiple DDL operations, we will
          // need to get the expected epoch from our caller. (Other changes are needed
          // for multiple DDL operations, so this is necessary but not sufficient.)
          UInt32 currentEpoch = 0;
          ObjectEpochCacheEntryName oecName(STMTHEAP, 
                                    ddlObj.getObjName(),
                                    COM_BASE_TABLE_OBJECT,
                                    0);

          CliGlobals *cliGlobals = GetCliGlobals();
          StatsGlobals *statsGlobals = cliGlobals->getStatsGlobals();
          ObjectEpochCache *objectEpochCache = statsGlobals->getObjectEpochCache();
          ObjectEpochCacheEntry *oecEntry = NULL;

          ObjectEpochCacheKey key(ddlObj.getObjName(), oecName.getHash(),COM_BASE_TABLE_OBJECT);

          short retcode = objectEpochCache->getEpochEntry(oecEntry /* out */,key);
          if (retcode == ObjectEpochCache::OEC_ERROR_OK)
            {
              // oecEntry points to the ObjectEpochCacheEntry
              currentEpoch = oecEntry->epoch();
              ddlObj.setExpectedEpoch(currentEpoch);
            }
          // end temporary fix
#endif

          NABoolean found = FALSE;
          for (Lng32 i = 0;
               ((NOT found) && (i <  CmpCommon::context()->ddlObjsList().entries()));
               i++)
            {
              DDLObjInfo &ddlObjInList = CmpCommon::context()->ddlObjsList()[i];
              if (ddlObj.objName_ == ddlObjInList.objName_ && ddlObj.isDDLOp() == ddlObjInList.isDDLOp())
                found = TRUE;
            }

          // if in SP, insert ddlObj into ddlObjsInSPList, while
          //    rollback savepoint
          //        clean up it
          //    commit savepoint
          //        move it to ddlObjsList
          if (CmpCommon::transMode()->savepointInProgress())
            CmpCommon::context()->ddlObjsInSPList().insertEntry(ddlObj, TRUE);
          else if (NOT found)
            CmpCommon::context()->ddlObjsList().insertEntry(ddlObj);
        }
    }

  // clear out the other users' caches too.
  if (qiScope == REMOVE_FROM_ALL_USERS)
    {
      NAString tabName = objName.getQualifiedNameAsAnsiString();

      // Sanity check, when TRAF_OBJECT_LOCK is enabled, we should
      // holding the object DDL lock
      if (!noCheck
          && CmpCommon::getDefault(TRAF_OBJECT_LOCK) == DF_ON
          && !Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)
          && ot == COM_BASE_TABLE_OBJECT
          && ComIsUserTable(objName))
        {
          ObjectLockPointer lock(tabName.data(), ot);
          if (!lock->ddlLockedByMe()) {
            LOGWARN(CAT_SQL_LOCK,
                    "DDL lock released before sending invalidation message for %s",
                    tabName.data());
            *CmpCommon::diags() << DgSqlCode(EXE_OL_RELEASE_DDL_LOCK_TOO_EARLY) // Warning
                                << DgString0(tabName.data());
          }
        }

      SQL_QIKEY qiKeys[numKeys];
      for (CollIndex i = 0; i < numKeys; i++)
        {
          LOGINFO(CAT_SQL_LOCK,
                  "Adding QIKEY to invalidate %s %s (UID: %lu)",
                  comObjectTypeName(ot),
                  tabName.data(),
                  objectUIDs[i]);

          qiKeys[i].ddlObjectUID = objectUIDs[i];
          qiKeys[i].operation[0] = 'O';
          qiKeys[i].operation[1] = 'R';
        }
      long retcode = SQL_EXEC_SetSecInvalidKeys(numKeys, qiKeys);
      if (retcode < 0)
        {
          retcode = SQL_EXEC_MergeDiagnostics_Internal(*CmpCommon::diags());
        }
    }
}

void NATableDB::removeNATable(CorrName &corrName, ComQiScope qiScope,
                              ComObjectType ot, NABoolean ddlXns,
                              NABoolean atCommit, Int64 objUID,
                              NABoolean noCheck)
{
  const ExtendedQualName* toRemove = &(corrName.getExtendedQualNameObj());
  //mantis m17845
  const QualifiedName& qualName = toRemove->getQualifiedNameObj();
  NAString bachupSch = NAString(TRAF_BACKUP_MD_PREFIX);
  if (qualName.getSchemaName().index(bachupSch, bachupSch.length(),
      0, NAString::exact) == 0) //start with "_BACKUP_"
    return;
  NAHashDictionaryIterator<ExtendedQualName,NATable> iter(*this); 
  ExtendedQualName *key = NULL;
  NATable *cachedNATable = NULL;
  NABoolean found = FALSE;
  NASet<Int64> objectUIDs(CmpCommon::statementHeap(), 1);

  // iterate over all entries and remove the ones that match the name
  // ignoring any partition clauses and other additional info
  iter.getNext(key,cachedNATable);

  while(key)
    {
      if (key->getQualifiedNameObj() == toRemove->getQualifiedNameObj())
        {

          //remove from list of cached NATables
          if (cachedTableList_.remove(cachedNATable) > 0)
            {
              //if metadata caching is ON, then adjust cache size
              //since we are deleting a caching entry
              if(cacheMetaData_)
                currentCacheSize_ = heap_->getAllocSize();
              if (cachedNATable->heap_ &&
                  cachedNATable->heap_ != CmpCommon::statementHeap())
                tablesToDeleteAfterStatement_.insert(cachedNATable);
            }
          else
            {
              // this must have been a non-cacheable table
              const LIST(CollIndex) & tableIdList = cachedNATable->getTableIdList();
              for(CollIndex i = 0; i < tableIdList.entries(); i++)
                {
                  nonCacheableTableIdents_.remove(tableIdList[i]);
                }

              for (CollIndex i=0; i<nonCacheableTableNames_.entries(); i++)
                {
                  if (*(nonCacheableTableNames_[i]) == *key)
                    {
                      nonCacheableTableNames_.removeAt(i);
                      i--;
                    }
                }
            }
  
          //remove pointer to NATable from cache
          remove(key);

          found = TRUE;
          objectUIDs.insert(cachedNATable->objectUid().castToInt64());
          statementCachedTableList_.remove(cachedNATable);
          statementTableList_.remove(cachedNATable);
        }
      
      iter.getNext(key,cachedNATable);
    }

  //When a specific NATable object can not be found in the (local) cache
  //and its objectUID (as in objUID argument) is suppilied,
  //remove it for all users from all caches in the cluster anyway.
  //For example, such an NATable object can be an index that has 
  //been deleted from the system table during a DDL transaction.
  if (NOT found && 0 < objUID)
    objectUIDs.insert(objUID);

  removeFromAllUsers(corrName.getQualifiedNameObj(),
                     qiScope, ot, objectUIDs, ddlXns, atCommit, noCheck);
}

//#define DEBUG_NATABLEDB_RESET_AFTER_STMT 1

//This method is called at the end of each statement to reset statement
//specific stuff in the NATable objects in the cache.
void NATableDB::resetAfterStatement(){

#ifdef DEBUG_NATABLEDB_RESET_AFTER_STMT
   cout << "<<< On ENTRY NATableDB::resetAfterStatement()"
        << ", this=" << static_cast<void*>(this) 
        << ", entries=" << entries() << endl << endl;
   display();
   cout << ">>>" << endl;
#endif

  //Variable used for iteration in loops below
  CollIndex i = 0;

  //Variable used to point to a table's heap. Only delete the heap if it is
  // neither the context nor the statement heap (i.e., allocated from the
  // C++ system heap). The CmpContext heap is deleted in 
  // in ContextCli::deleteMe().
  // The statement heap is deleted in the destructor of class CmpStatement. 

  NAMemory * tableHeap = NULL;

  //if metadata caching (i.e. NATable caching) is not on then just
  //flush the cache. Since it might be that there are still some
  //tables in the cache.
  if (!cacheMetaData_){
    flushCache();
  }
  else{

    //if caching is ON then reset all cached NATables used during statement
    //if this was a DDL statment delete all NATables that participated in the
    //statement
    for (i=0; i < statementCachedTableList_.entries(); i++)
    {
      NATable* natable = statementCachedTableList_[i];
      if(natable)
      {
        //if the statment was a DDL statement, if so then delete
        //all the tables used in the statement, since the DDL affected
        //the tables and they should be reconstructed for whatever
        //statement follows.
        NABoolean mustRemove =  
           (natable->isAnMV())||
           (natable->isAnMVMetaData())||
           (natable->constructionHadWarnings()) ||
           (natable->getClearHDFSStatsAfterStmt());

        // If the table is not in DDL, further check if 
        // we want to cache it, or the table is a preload one.
        // If so, we do not want to remove it.
        if ( !mustRemove ) {
           mustRemove = !useCache_ && !(natable->isPreloaded());
        }

        if( mustRemove )
         {
          //remove from list of cached Tables
          cachedTableList_.remove(statementCachedTableList_[i]);
          //remove from the cache itself
          remove(statementCachedTableList_[i]->getKey());

          // Add to tablesToDeleteAfterStatement_ so that we can detect any
          // duplicated entries (pointers to NATables) in 
          // statementCachedTableList_ and in tablesToDeleteAfterStatement_ 
          // later in this method, since it is possible that some NATable 
          // object could be in both  statementCachedTableList_ and 
          // tablesToDeleteAfterStatement_.
          //
          // We are quite certain, through a static analysis, that 
          // tablesToDeleteAfterStatement_ and cachedTableList_ are mutually
          // exclusive.
          tablesToDeleteAfterStatement_.insert((statementCachedTableList_[i]));
        }
        else{
          statementCachedTableList_[i]->resetAfterStatement();
        }
      }
    }

    nonCacheableTableIdents_.clear();

    //remove references to nonCacheable tables from cache
    //and delete the name
    for(i=0; i < nonCacheableTableNames_.entries(); i++){
      remove(nonCacheableTableNames_[i]);
      delete nonCacheableTableNames_[i]; // delete the name only
    }

    //clear the list of special tables
    nonCacheableTableNames_.clear();

  }

  //delete tables that were not deleted earlier to
  //save compile-time performance. Since the heaps
  //deleted below are large 16KB+, it takes time
  //to delete them. The time to delete these heaps
  //at this point is not 'visible' in the compile-
  //time since the statement has been compiled and
  //sent to the executor.

  // Duplication (NATable pointers) detection first
  // to avoid double or even triple deletions.
  NASet<NATable*> distinctNATableSet(heap_);

  for(i=0; i < tablesToDeleteAfterStatement_.entries(); i++)
  {
    NATable* naTable = tablesToDeleteAfterStatement_[i];
    if ( naTable->getHeapType() == NATable::OTHER &&
         !(naTable->isPreloaded()) ) 
    {
      if ( !distinctNATableSet.contains(naTable) )
         distinctNATableSet.insert(naTable);
    }
  }

  // Now delete all NATables to be deleted and only once.
  for (size_t i = 0; i < distinctNATableSet.entries(); i++)
   {
     delete distinctNATableSet[i];
     currentCacheSize_ = heap_->getAllocSize();
   }

  //clear the list of tables to delete after statement
  tablesToDeleteAfterStatement_.clear();

  //clear the list of tables used in the current statement
  statementTableList_.clear();
  //clear the list of cached tables used in the current statement
  statementCachedTableList_.clear();
  //reset various statement level flags
  refreshCacheInThisStatement_=FALSE;
  useCache_=FALSE;

#ifdef DEBUG_NATABLEDB_RESET_AFTER_STMT
  cout << "<<<On EXIT NATableDB::resetAfterStatement()"
       << ", this=" << static_cast<void*>(this) 
       << ", entries=" << entries() << endl << endl;
  display();
  cout << ">>>" << endl;
#endif
}

//flush the cache if there is anything cached in it
//otherwise just destroy all the keys in the cache.
//If there is nothing cached, which could mean either
//of the following:
//1. NATable caching is off.
//2. All entries currently in cache where created on
//   the statment heap, i.e. not persistent across
//   statements.
//In such a case we don't need to delete any NATable
//objects (since they will be removed when the statement
//heap is deleted. We only need to delete the keys.
void NATableDB::flushCache()
{
  //if something is cached
  if(metaDataCached_){
    //set the flag to indicate cache is clear
    metaDataCached_ = FALSE;

    //Destroy the keys in the cache, this also
    //clears out the cache entries without deleting
    //the cached NATable
    clearAndDestroyKeysOnly();

    //delete the tables that were cached by deleting each table's
    //heap. Each cached table and all of its stuff are allocated
    //on a seperate heap (i.e. a heap per table). That seems to
    //be the safest thing to do to avoid memory leaks.
    for(CollIndex i=0; i < cachedTableList_.entries(); i++)
    {
      if(cachedTableList_[i])
      {
        delete cachedTableList_[i];
      }
    }

  }
  else{
    //no metadata cached (i.e. metadata caching is off and there
    //is no remaining metadata in the cache from when the caching
    //was on). Just clear out the cache entries, of course we need
    //to delete keys because the cache allocates keys on the context
    //heap.
    clearAndDestroyKeysOnly ();
  }

  //clear out the lists of tables in the cache
  //1. list of tables in the cache used in this statement
  //2. list of all tables in the cache
  statementCachedTableList_.clear();
  cachedTableList_.clear();

  //set cache size to 0 to indicate nothing in cache
  currentCacheSize_ = 0;
  highWatermarkCache_ = 0;   // High watermark of currentCacheSize_  
  totalLookupsCount_ = 0;    // reset NATable entries lookup counter
  totalCacheHits_ = 0;       // reset cache hit counter

  // per interval counters
  intervalWaterMark_ = 0;

  // clear out HiveMetaData object
  if (hiveMetaDB_)
  {
    NADELETE(hiveMetaDB_,HiveMetaData,CmpCommon::contextHeap());
    hiveMetaDB_ = NULL;
  }
}

//check if cache size is with maximum allowed cache size.
//if cache size is above the maximum allowed cache size,
//then remove entries in the cache based on the cache
//replacement policy to get the cache size under the maximum
//allowed cache size.
NABoolean NATableDB::enforceMemorySpaceConstraints()
{
  //check if cache size is within memory constraints
  if (maxCacheSize() == 0 || heap_->getAllocSize()  <= maxCacheSize())
    return TRUE;

  //need to get cache size under memory allowance

  //if our cursor is pointing past the end of the
  //list of cached entries, reset it to point to
  //start of the list of cached entries.
  if(replacementCursor_ >= (Int32) cachedTableList_.entries())
      replacementCursor_ = 0;

  //keep track of entry in the list of cached entries
  //where we are starting from, since we start from
  //where we left off the last time this method got
  //called.
  Int32 startingCursorPosition = replacementCursor_;
  Int32 numLoops = 0; //number of loops around the list of cached objects

  //this loop iterates over list of cached NATable objects.
  //in each iteration it decrements the replacementCounter
  //of a table.
  //if a table with a replacementCounter value of zero is
  //encountered, it is removed if it is not being used
  //in the current statement.

  //check if cache is now within memory constraints
  while (heap_->getAllocSize()  > maxCacheSize()){

    //get reference to table
    NATable * table = cachedTableList_[replacementCursor_];

    // if preloaded or metadata NATable, do not remove from cache
    if ((table) &&
        (NOT (table->isSeabaseMDTable() || table->isPreloaded())))
      {
        //check if table has a zero replacementCount
        if(!table->replacementCounter_)
          {
            //if table is not being accessed in current statement then remove it
            if(!table->accessedInCurrentStatement_)
              {
                RemoveFromNATableCache( table , replacementCursor_ );
                
                // Since the above call can reduce the length of cachedTableList_ by 1,
                // we need to make sure the start position never falls out of the valid
                // range in cachedTableList_.
                if( startingCursorPosition >= cachedTableList_.entries() )
                  startingCursorPosition = cachedTableList_.entries()-1;
              }
          }
        else{
          table->replacementCounter_--;
        }
      }

    replacementCursor_++;

    //if replacement cursor ran of the end of the list of cached tables
    //reset it to the beginig of the list
    if(replacementCursor_ >= (Int32) cachedTableList_.entries())
      replacementCursor_ = 0;

    //check if we completed one loop around all the cached entries
    //if so, increment the loop count
    if(replacementCursor_ == startingCursorPosition){
        numLoops++;
    }

    //did NATABLE_MAX_REFCOUNT loops around list of cached objects
    //still could not free up enough space
    //We check for NATABLE_MAX_REFCOUNT loops since the replacementCounter_
    //is capped at NATABLE_MAX_REFCOUNT loops.
    if(numLoops==NATABLE_MAX_REFCOUNT)
      return FALSE;
  }

  //return true indicating cache size is below maximum memory allowance.
  return TRUE;
}

//Turn metadata caching ON
void NATableDB::setCachingON()
{
  cacheMetaData_ = TRUE;
}

// Obtain a list of table identifiers for the current statement.
// Allocate the list on the heap passed.
const LIST(CollIndex) &
NATableDB::getStmtTableIdList(NAMemory *heap) const
{
  LIST(CollIndex) *list = new (heap) LIST(CollIndex)(heap);
  for(CollIndex i = 0; i < statementTableList_.entries(); i++)
  {
    NATable *table = statementTableList_[i];
    list->insert(table->getTableIdList());
  }
  return *list;
}

// function to return number of entries in cachedTableList_ LIST.
Int32 NATableDB::end()
{
   return cachedTableList_.entries() ;
}

void
NATableDB::free_entries_with_QI_key(Int32 numKeys, SQL_QIKEY* qiKeyArray)
{
  UInt32 currIndx = 0;

  // For each table in cache, see if it should be removed
  while ( currIndx < cachedTableList_.entries() )
  {
    NATable * currTable = cachedTableList_[currIndx];

    // Only need to remove seabase, hive or external Hive/hbase tables
    NABoolean toRemove = FALSE;
    if ((currTable->isSeabaseTable()) ||
        (currTable->isHiveTable()) ||
        (currTable->isSchemaObject()) ||
        (currTable->isHbaseCellTable()) ||
        (currTable->isHbaseRowTable()) ||
        (currTable->hasExternalTable()) ||
        (currTable->getTableType() == ExtendedQualName::SG_TABLE))
      toRemove = TRUE;
    if (! toRemove)
    {
      currIndx++;
      continue;
    }

    if (qiCheckForInvalidObject(numKeys, qiKeyArray,
                                currTable->objectUID_.get_value(),
                                currTable->getSecKeySet()))
    {
      if ( currTable->accessedInCurrentStatement_ )
        statementCachedTableList_.remove( currTable );
      while ( statementTableList_.remove( currTable ) ) // Remove as many times as on list!
      { ; }

      // remove triggers from cache associated with this table
      TriggerDB::removeTriggers(currTable->getTableName(), COM_INSERT);

      RemoveFromNATableCache( currTable , currIndx );
    }
    else currIndx++; //Increment if NOT found ... else currIndx already pointing at next entry!
  }
}

void
NATableDB::update_entry_stored_stats_with_QI_key(Int32 numKeys, SQL_QIKEY* qiKeyArray)
{
  UInt32 currIndx = 0;

  // For each table in cache, see if it's stored stats should be removed
  while ( currIndx < cachedTableList_.entries() )
  {
    NATable * currTable = cachedTableList_[currIndx];

    if (qiCheckForInvalidObject(numKeys, qiKeyArray,
                                currTable->objectUID_.get_value(),
                                currTable->getSecKeySet()))
    {
      const ExtendedQualName &qualName =
                             currTable->getExtendedQualName();
      NAHeap *descHeap = NULL;
      if (currTable->isStoredDesc())
      {
        TrafDesc *tableDesc = getTableDescFromCacheOrText(qualName,
                               currTable->objectUID_.get_value(),
                               &descHeap);
        if (tableDesc)
        {
          currTable->updateStoredStats(tableDesc);
          NADELETEBASIC(tableDesc, descHeap);
        }
      }
    }
    currIndx++;
  }
}

void
NATableDB::free_schema_entries()
{
  
  UInt32 currIndx = 0;


  while ( currIndx < cachedTableList_.entries() )
  {
    NATable * currTable = cachedTableList_[currIndx];

    // if this is a schema object, remove it
    if (currTable->isSchemaObject())
    {
#ifndef NDEBUG
      if (getenv("NATABLE_DEBUG"))
        cout << "Removing cached entry: " << currTable->qualifiedName_.getQualifiedNameObj().getQualifiedNameAsAnsiString() << endl; 
#endif
      if ( currTable->accessedInCurrentStatement_ )
        statementCachedTableList_.remove( currTable );
      while ( statementTableList_.remove( currTable ) ) // Remove as many times as on list!
      { ; }

      RemoveFromNATableCache( currTable , currIndx );
    }
    else currIndx++; //Increment if NOT found ... else currIndx already pointing at next entry!
  }
}

void
NATableDB::free_entries_with_schemaUID(Int32 numKeys, SQL_QIKEY* qiKeyArray)
{
  UInt32 currIndx = 0;

  // For each table in cache, see if it should be removed
  while ( currIndx < cachedTableList_.entries() )
  {
    NATable * currTable = cachedTableList_[currIndx];

    // Only need to remove seabase, hive or external Hive/hbase tables
    NABoolean toRemove = FALSE;
    if ((currTable->isSeabaseTable()) ||
        (currTable->isHiveTable()) ||
        (currTable->isSchemaObject()) ||
        (currTable->isHbaseCellTable()) ||
        (currTable->isHbaseRowTable()) ||
        (currTable->hasExternalTable()) ||
        (currTable->getTableType() == ExtendedQualName::SG_TABLE))
      toRemove = TRUE;
    if (! toRemove || (currTable->schemaUID_.castToInt64() == 0))
    {
      currIndx++;
      continue;
    }

    if (qiCheckForSchemaUID(numKeys, qiKeyArray,
                            currTable->schemaUID_.castToInt64()))
    {
#ifndef NDEBUG
      if (getenv("NATABLE_DEBUG"))
        cout << "Removing cached entry: " << currTable->qualifiedName_.getQualifiedNameObj().getQualifiedNameAsAnsiString() << endl; 
#endif
      if ( currTable->accessedInCurrentStatement_ )
        statementCachedTableList_.remove( currTable );
      while ( statementTableList_.remove( currTable ) ) // Remove as many times as on list!
      { ; }

      RemoveFromNATableCache( currTable , currIndx );
    }
    else currIndx++; //Increment if NOT found ... else currIndx already pointing at next entry!
  }
}



void
NATableDB::free_hive_tables()
{
  
  UInt32 currIndx = 0;

  while ( currIndx < cachedTableList_.entries() )
  {
    NATable * currTable = cachedTableList_[currIndx];

    // if this is a hive table, remove it
    if (currTable->isHiveTable())
    {
      if ( currTable->accessedInCurrentStatement_ )
        statementCachedTableList_.remove( currTable );
      while ( statementTableList_.remove( currTable ) ) // Remove as many times as on list!
      { ; }

      RemoveFromNATableCache( currTable , currIndx );
    }
    else currIndx++; //Increment if NOT found ... else currIndx already pointing at next entry!
  }
}

// -----------------------------------------------------------------------------
// method: reset_priv_entries()
//
// Adjust privilege bitmaps for each NATable entry to match the current user
// Called at connection time when the current user changes.
// If privilege descriptors do not exist, set priv bitmaps to NULL.  At compile 
// time privileges will be read from the metadata.
// -----------------------------------------------------------------------------
void NATableDB::reset_priv_entries()
{
  UInt32 currIndx = 0;

  Int32 len = 500;         
  char msg[len];           

  // If error getting current roles, clear the cache list and return
  NAList <Int32> roleIDs(heap_);
  Int16 retcode = ComUser::getCurrentUserRoles(roleIDs, NULL/*don't update diags*/);

  // If unable to get roles, then just clear the list
  if (!ComUser::roleIDsCached())
  {
    snprintf(msg, len, "NATableDB::reset_priv_entries, user: %s, retcode: %d,  "
                       "unable to retrieve roles assigned to user, clearing cache",
             ComUser::getCurrentUsername(),
             retcode);
    QRLogger::log(CAT_SQL_EXE, LL_INFO, "%s", msg);
    setCachingOFF();
    setCachingON();
    return;
  }

  Int32 userID = ComUser::getCurrentUser();

  while ( currIndx < cachedTableList_.entries() )
  {
    NATable * currTable = cachedTableList_[currIndx];

    currTable->removePrivInfo();
    if (currTable->getPrivDescs())
    {
      PrivMgrUserPrivs *privInfo = new (currTable->getHeap()) PrivMgrUserPrivs;
      ComSecurityKeySet secKeySet(currTable->getHeap());
      bool result = privInfo->initUserPrivs(roleIDs, currTable->getPrivDescs(), userID, 
                                            currTable->objectUid().castToInt64(), &secKeySet);           
      if (result)
      {
        currTable->setPrivInfo(privInfo);
        currTable->setSecKeySet(secKeySet);
        std::string privDetails = currTable->privInfo_->print();
        snprintf(msg, len, "NATableDB::reset_priv_entries, user: %s obj %s, %s",
                ComUser::getCurrentUsername(),
                currTable->qualifiedName_.getExtendedQualifiedNameAsString().data(),
                privDetails.c_str());
        QRLogger::log(CAT_SQL_EXE, LL_DEBUG, "%s", msg);
#ifndef NDEBUG
        if (getenv("NATABLE_DEBUG"))
          cout << msg << endl;
#endif
      }

      // result is false if unable to build the security key
      // assume no privileges, will be rechecked later.
      else
      {
        snprintf(msg, len, "NATableDB::reset_priv_entries, user: %s obj: %s, invalid security key",
                ComUser::getCurrentUsername(),
                currTable->qualifiedName_.getExtendedQualifiedNameAsString().data());
        QRLogger::log(CAT_SQL_EXE, LL_INFO, "%s", msg);

        currTable->removePrivInfo();
      }
    }
    else
    {
      snprintf(msg, len, "NATableDB::reset_priv_entries, user: %s obj: %s, privDesc is null",
               ComUser::getCurrentUsername(),
               currTable->qualifiedName_.getExtendedQualifiedNameAsString().data());
      QRLogger::log(CAT_SQL_EXE, LL_DEBUG, "%s", msg);
   }
    currIndx++;
  }
}
 
     
//
// Remove a specifed NATable entry from the NATable Cache
//
void
NATableDB::RemoveFromNATableCache( NATable * NATablep , UInt32 currIndx )
{
   NAMemory * tableHeap = NATablep->heap_;
   NABoolean InStatementHeap = (tableHeap == (NAMemory *)CmpCommon::statementHeap());

   remove(NATablep->getKey());
   
   cachedTableList_.removeAt( currIndx );
   if ( ! InStatementHeap )
      delete NATablep;
   if ( ! InStatementHeap )
      currentCacheSize_ = heap_->getAllocSize();
}

//
// Remove ALL entries from the NATable Cache that have been
// marked for removal before the next compilation.
// Remove nonCacheable entries also.
//
void
NATableDB::remove_entries_marked_for_removal()
{
   NATableDB * TableDB = ActiveSchemaDB()->getNATableDB() ;

   UInt32 currIndx = 0;
   while ( currIndx < TableDB->cachedTableList_.entries() )
   {
      NATable * NATablep = TableDB->cachedTableList_[ currIndx ] ;
      NABoolean accInCurrStmt = NATablep->accessedInCurrentStatement() ;
      if ( NATablep->isToBeRemovedFromCacheBNC() ) //to be removed by CmpMain Before Next Comp. retry?
      {
         TableDB->RemoveFromNATableCache( NATablep, currIndx );
         if ( accInCurrStmt )
         {
            TableDB->statementCachedTableList_.remove( NATablep );
         }
         while ( TableDB->statementTableList_.remove( NATablep ) ) // Remove as many times as on list!
         { ; }
      }
      else currIndx++ ; //Note: No increment if the entry was removed !
   }

   //remove the nonCacheableTableList and delete the name, 
   //this is needed to remove objects such as sequence generators which 
   //are not stored in the cached list
   for(CollIndex i=0; i < nonCacheableTableNames_.entries(); i++){
     remove(nonCacheableTableNames_[i]);
     delete nonCacheableTableNames_[i]; // delete the name only
   }

   //clear the list of special tables
   nonCacheableTableNames_.clear();
}

//
// UNMARK all entries from the NATable Cache that have been
// marked for removal before the next compilation.  We have 
// decided to leave them in the NATable cache afterall.
//
void
NATableDB::unmark_entries_marked_for_removal()
{
   NATableDB * TableDB = ActiveSchemaDB()->getNATableDB() ;

   UInt32 currIndx = 0;
   while ( currIndx < TableDB->cachedTableList_.entries() )
   {
      NATable * NATablep = TableDB->cachedTableList_[ currIndx ] ;
      if ( NATablep->isToBeRemovedFromCacheBNC() ) //to be removed by CmpMain Before Next Comp. retry?
      {
         NATablep->setRemoveFromCacheBNC(FALSE);
      }
      else currIndx++ ; //Note: No increment if the entry was removed !
   }
}

void NATableDB::getCacheStats(NATableCacheStats & stats)
{
  memset(stats.contextType, ' ', sizeof(stats.contextType));
  stats.numLookups = totalLookupsCount_;
  stats.numCacheHits = totalCacheHits_;
  stats.preloadedCacheSize = preloadedCacheSize_;
  stats.currentCacheSize = currentCacheSize_;
  stats.defaultCacheSize = defaultCacheSize_;
  stats.maxCacheSize = maxCacheSize();
  stats.highWaterMark = highWatermarkCache_;
  stats.numEntries =  cachedTableList_.entries();    
}

void NATableDB::display()
{
  LIST(NATable*) list(CmpCommon::statementHeap());
  dump(list);

  cout << "NATableDB: the following tables are cached." << endl;

  for (int i=0; i<list.entries(); i++) {
     NATable* table = list[i];
     NAString tblName = 
       table->getExtendedQualName().getQualifiedNameObj().getQualifiedNameAsString();
     cout << "table name=" << tblName.data() 
          << ", isPreloaded=" << table->isPreloaded() << endl;
  }
}

void minmaxKey::display(const char* msg)
{
   if (msg) 
     printf("%s\n", msg);
         
   printf("join column=%s\n", joinCols_.data());
   printf("local pred=%s\n",  localPreds_.data());
}

