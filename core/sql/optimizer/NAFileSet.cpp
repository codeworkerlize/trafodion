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
******************************************************************************
*
* File:         NAFileSet.C
* Description:  Implementations for the FileSet class.
* Created:      12/27/95
* Language:     C++
*
*
*
*
******************************************************************************
*/

// -----------------------------------------------------------------------

#include "optimizer/Sqlcomp.h"
#include "PartFunc.h"
#include "common/ComSysUtils.h"
#include "optimizer/NAFileSet.h"
#include "optimizer/opt.h"

#include "CliSemaphore.h"
#include "exp/ExpHbaseDefs.h"

NAFileSet::NAFileSet(const QualifiedName & fileSetName,
		     const QualifiedName & extFileSetObj,
		     const NAString & extFileSetName,
		     enum FileOrganizationEnum org,
		     NABoolean isSystemTable,
		     Lng32 numberOfFiles,
		     Cardinality estimatedNumberOfRecords,
		     Lng32 recordLength,
		     Lng32 blockSize,
		     Int32 indexLevels,
		     const NAColumnArray & allColumns,
		     const NAColumnArray & indexKeyColumns,
		     const NAColumnArray & horizontalPartKeyColumns,
		     const NAColumnArray & hiveSortKeyColumns,
		     PartitioningFunction * forHorizontalPartitioning,
		     short keytag, 
		     Int64 redefTime,
		     NABoolean audited,
		     NABoolean auditCompressed,
		     NABoolean compressed,
		     ComCompressionType dcompressed,
		     NABoolean icompressed,
		     NABoolean buffered,
		     NABoolean clearOnPurge,
		     NABoolean packedRows,
                     NABoolean hasRemotePartition,
		     NABoolean isUniqueSecondaryIndex,
		     NABoolean isNgramIndex,  // is a ngram index
                     NABoolean isPartLocalBaseIndex,
                     NABoolean isPartLocalIndex,
                     NABoolean isPartGlobalIndex,
                     NABoolean isDecoupledRangePartitioned,
                     Lng32 fileCode,
		     NABoolean isVolatile,
		     NABoolean inMemObjectDefn,
                     Int64 indexUID,
                     TrafDesc *keysDesc,
                     Lng32 numSaltPartns,
                     Lng32 numInitialSaltRegions,
                     Int16 numTrafReplicas,
                     NAList<HbaseCreateOption*>* hbaseCreateOptions,
                     CollHeap * h)
         : fileSetName_(fileSetName, h),
	   extFileSetObj_(extFileSetObj, h),
	   extFileSetName_(extFileSetName, h),
           fileOrganization_(org), 
	   isSystemTable_(isSystemTable),
           countOfFiles_(numberOfFiles),
	   estimatedNumberOfRecords_(estimatedNumberOfRecords),
	   recordLength_(recordLength),
	   blockSize_(blockSize),
	   indexLevels_(indexLevels),
           allColumns_(allColumns, h), 
           indexKeyColumns_(indexKeyColumns, h),
	   partitioningKeyColumns_(horizontalPartKeyColumns, h),
           hiveSortKeyColumns_(hiveSortKeyColumns, h),
           partFunc_(forHorizontalPartitioning),
	   keytag_(keytag),
	   redefTime_(redefTime),
	   audited_(audited),
	   auditCompressed_(auditCompressed),
	   compressed_(compressed),
	   dcompressed_(dcompressed),
	   icompressed_(icompressed),
	   buffered_(buffered),
	   clearOnPurge_(clearOnPurge),
	   packedRows_(packedRows),
           hasRemotePartition_(hasRemotePartition),
           setupForStatement_(FALSE),
           resetAfterStatement_(FALSE),
	   bitFlags_(0),
	   keyLength_(0),
	   encodedKeyLength_(0),
           thisRemoteIndexGone_(FALSE),
           isDecoupledRangePartitioned_(isDecoupledRangePartitioned),
           fileCode_(fileCode),
           indexUID_(indexUID),
           keysDesc_(keysDesc),
           numSaltPartns_(numSaltPartns),
           numInitialSaltRegions_(numInitialSaltRegions),
           numTrafReplicas_(numTrafReplicas),
           hbaseCreateOptions_(hbaseCreateOptions),
           numMaxVersions_(1),
           shallowCopied_(FALSE)
{
  setUniqueIndex(isUniqueSecondaryIndex);
  // for ngram
  setNgramIndex(isNgramIndex);

  setPartLocalBaseIndex(isPartLocalBaseIndex);
  setPartLocalIndex(isPartLocalIndex);
  setPartGlobalIndex(isPartGlobalIndex);
  
  setIsVolatile(isVolatile);
  setInMemoryObjectDefn(inMemObjectDefn);

  if (hbaseCreateOptions_)
    {
      for (Lng32 i = 0; i < hbaseCreateOptions_->entries(); i++)
        {
          HbaseCreateOption * hco = (*hbaseCreateOptions_)[i];
          
          if (hco->key() == "MAX_VERSIONS")
            {
              numMaxVersions_ = atoInt64(hco->val().data());
            }
        }
    }

  NABoolean nullablePkey = FALSE;
  for (Int32 i = 0; ((NOT nullablePkey) && (i < indexKeyColumns.entries())); i++)
    {
      if (indexKeyColumns.getColumn(i)->getType()->supportsSQLnull())
        nullablePkey = TRUE;
    }
  setNullablePkey(nullablePkey);
}

// copy constructor.
// Unless stated otherwise in this method, data members 
// are deep copied.
NAFileSet::NAFileSet(const NAFileSet& other, CollHeap * h)
         : fileSetName_(other.fileSetName_, h),
	   extFileSetName_(other.extFileSetName_, h),
	   extFileSetObj_(other.extFileSetObj_, h),
           fileOrganization_(other.fileOrganization_), 
           countOfFiles_(other.countOfFiles_),
	   estimatedNumberOfRecords_(other.estimatedNumberOfRecords_),
	   recordLength_(other.recordLength_),
	   keyLength_(other.keyLength_),
	   encodedKeyLength_(other.encodedKeyLength_),
	   lockLength_(other.lockLength_),
           fileCode_(other.fileCode_),
	   blockSize_(other.blockSize_),
           packingScheme_(other.packingScheme_),
           packingFactor_(other.packingFactor_),
	   indexLevels_(other.indexLevels_),
           allColumns_(other.allColumns_, h), //shallow copied
           indexKeyColumns_(other.indexKeyColumns_, h),//shallow copied
           indexUID_(other.indexUID_),
           keysDesc_((other.keysDesc_) ? 
                    TrafDesc::copyDescList(other.keysDesc_, h) : NULL), 
	   partitioningKeyColumns_(other.partitioningKeyColumns_, h),//shallow copied
           hiveSortKeyColumns_(other.hiveSortKeyColumns_, h),//shallow copied
           partFunc_(other.partFunc_ ?  other.partFunc_->copy(h) : NULL),



	   keytag_(other.keytag_),
	   redefTime_(other.redefTime_),
	   audited_(other.audited_),
	   auditCompressed_(other.auditCompressed_),
	   compressed_(other.compressed_),
	   dcompressed_(other.dcompressed_),
	   icompressed_(other.icompressed_),
	   buffered_(other.buffered_),
	   clearOnPurge_(other.clearOnPurge_),
	   isSystemTable_(other.isSystemTable_),
	   packedRows_(other.packedRows_),
           hasRemotePartition_(other.hasRemotePartition_),
           setupForStatement_(other.setupForStatement_),
           resetAfterStatement_(other.resetAfterStatement_),
           isDecoupledRangePartitioned_(other.isDecoupledRangePartitioned_),

	   bitFlags_(other.bitFlags_),
           thisRemoteIndexGone_(other.thisRemoteIndexGone_),


           numSaltPartns_(other.numSaltPartns_),
           numInitialSaltRegions_(other.numInitialSaltRegions_),
           numTrafReplicas_(other.numTrafReplicas_),

           numMaxVersions_(other.numMaxVersions_),
           hbaseCreateOptions_( (other.hbaseCreateOptions_) ? // shallow copied 
                   new (h) NAList<HbaseCreateOption*>(
                    *other.hbaseCreateOptions_, h) : NULL),
           rowFormat_(other.rowFormat_),
           shallowCopied_(TRUE)

{
   setUniqueIndex(other.uniqueIndex());
   setNgramIndex(other.ngramIndex());
   setPartLocalBaseIndex(other.partLocalBaseIndex());
   setPartLocalIndex(other.partLocalIndex());
   setPartGlobalIndex(other.partGlobalIndex());
   setIsVolatile(other.isVolatile());
   setInMemoryObjectDefn(other.isInMemoryObjectDefn());
   setNullablePkey(other.isNullablePkey());
}

NAFileSet::~NAFileSet()
{
  delete partFunc_;


}

NABoolean NAFileSet::operator==(const NAFileSet& other) const
{
   if ( this == &other )
     return TRUE;

   // Compare each data member.
   if ( !(fileSetName_ == other.fileSetName_) ||
	!(extFileSetName_ == other.extFileSetName_) ||
	!(extFileSetObj_ == other.extFileSetObj_) ||
        fileOrganization_ != other.fileOrganization_  ||
        countOfFiles_ != other.countOfFiles_ ||
	estimatedNumberOfRecords_ != other.estimatedNumberOfRecords_
      )
     return FALSE;

   if ( recordLength_ != other.recordLength_ ||
	   keyLength_ !=other.keyLength_ ||
	   encodedKeyLength_ !=other.encodedKeyLength_ ||
	   lockLength_ !=other.lockLength_ ||
	   fileCode_ != other.fileCode_ ||
	   blockSize_ != other.blockSize_ ||
	   packingScheme_ != other.packingScheme_ ||
	   packingFactor_ != other.packingFactor_ ||
	   indexLevels_ != other.indexLevels_ ||
           !(allColumns_ == other.allColumns_)  ||
           !(indexKeyColumns_ == other.indexKeyColumns_) ||
           indexUID_ != other.indexUID_ ||
           !COMPARE_TRAFDESC_PTRS(keysDesc_, other.keysDesc_) ||
	   !(partitioningKeyColumns_ == other.partitioningKeyColumns_) ||
           !(hiveSortKeyColumns_ == other.hiveSortKeyColumns_)
      )
     return FALSE;



    if ( keytag_ != other.keytag_ ||
	   redefTime_ != other.redefTime_ ||
	   audited_ != other.audited_ ||
	   auditCompressed_ != other.auditCompressed_ ||
	   compressed_ != other.compressed_ ||
	   dcompressed_ != other.dcompressed_ ||
	   icompressed_ != other.icompressed_ ||
	   buffered_ != other.buffered_ ||
	   clearOnPurge_ != other.clearOnPurge_ ||
	   isSystemTable_ != other.isSystemTable_ ||
	   packedRows_ != other.packedRows_ ||
           hasRemotePartition_ != other.hasRemotePartition_ ||
           setupForStatement_ != other.setupForStatement_ ||
           resetAfterStatement_ != other.resetAfterStatement_ ||
           isDecoupledRangePartitioned_ != other.isDecoupledRangePartitioned_ ||
	   bitFlags_ != other.bitFlags_ ||
           thisRemoteIndexGone_ != other.thisRemoteIndexGone_ ||


           numSaltPartns_ != other.numSaltPartns_ ||
           numInitialSaltRegions_ != other.numInitialSaltRegions_ ||
           numTrafReplicas_ != other.numTrafReplicas_ ||
           numMaxVersions_ != other.numMaxVersions_  ||
           !COMPARE_PTRS(hbaseCreateOptions_, other.hbaseCreateOptions_) ||// NAList<HbaseCreateOption*>
           rowFormat_ != other.rowFormat_
     )
     return FALSE;

   if ( uniqueIndex() != other.uniqueIndex() ||
        ngramIndex() != other.ngramIndex() ||
        isVolatile() != other.isVolatile() ||
        isInMemoryObjectDefn() != other.isInMemoryObjectDefn() ||
        isNullablePkey() != other.isNullablePkey() ||
        partLocalIndex() != other.partLocalIndex() ||
        partGlobalIndex() != other.partGlobalIndex() ||
        partLocalBaseIndex() != other.partLocalBaseIndex()
      )
     return FALSE;

   return TRUE;
}

NABoolean NAFileSet::isPartitioned() const
{ 
  return partFunc_ && partFunc_->getCountOfPartitions() > 1;
}

// returns the length of the key in bytes for this index
Lng32 NAFileSet::getKeyLength()
{
	if(keyLength_ >0) return keyLength_;

	for(CollIndex i=0;i<indexKeyColumns_.entries();i++)
	{
		keyLength_ += indexKeyColumns_[i]->getType()->getTotalSize();
	}
	return keyLength_;
}

// returns the length of the encoded key in bytes for this index
Lng32 NAFileSet::getEncodedKeyLength()
{
	if(encodedKeyLength_ >0) return encodedKeyLength_;

	for(CollIndex i=0;i<indexKeyColumns_.entries();i++)
	{
		encodedKeyLength_ += indexKeyColumns_[i]->getType()->getEncodedKeyLength();
	}
	return encodedKeyLength_;
}

Lng32 NAFileSet::getCountOfPartitions() const
{
  return partFunc_ ? partFunc_->getCountOfPartitions() : 1;
}

NABoolean NAFileSet::containsPartition(const NAString &partitionName) const
{ 
  return partFunc_ &&
         partFunc_->getNodeMap()->containsPartition(partitionName);
}

Int32 NAFileSet::numHivePartCols() const
{
  Int32 result = 0;

  for (CollIndex i=0; i<allColumns_.entries(); i++)
    if (allColumns_[i]->isHivePartColumn())
      result++;

  return result;
}

void NAFileSet::markAsHivePartitioningColumn(int colNum)
{
  // this is currently used to mark columns of external tables
  allColumns_[colNum]->setVirtualColumnType(NAColumn::HIVE_PART_COL);
}

NABoolean NAFileSet::isSyskeyLeading() const
{
  return (indexKeyColumns_.entries() > 0 &&
          indexKeyColumns_[0]->getPosition() == 0 &&
          indexKeyColumns_[0]->isSyskeyColumn());
}

Int32 NAFileSet::getSysKeyPosition() const
{
   for(CollIndex i=0;i<indexKeyColumns_.entries();i++)
   {
     if ( indexKeyColumns_[i]->isSyskeyColumn() )
         return i;
   }
   return -1;
}

NABoolean NAFileSet::hasSyskey() const
{
  // check the NAColumn class of the key column
  // ++MV - 7/3/01 bug fix

//  If the table only contains a clustering key, it is last.
//  If it contains a clustering and hash key and the hash key has columns 
//  not in the clustering key, then the system key falls between the store 
//  by columns and the hash columns.

  Int32 numKeyCols = indexKeyColumns_.entries();
  if (numKeyCols == 0)
    return TRUE;

  if (indexKeyColumns_[numKeyCols - 1]->isSyskeyColumn())
     return TRUE;

  // the array element index right before the first partition key col in the 
  // indexKeyColumns_[]
  Int32 otherSyskeyLoc = numKeyCols - partitioningKeyColumns_.entries() - 1;
  
  return ( otherSyskeyLoc >= 0 ) ? 
            indexKeyColumns_[otherSyskeyLoc]->isSyskeyColumn() 
                : FALSE;
}

NABoolean NAFileSet::hasOnlySyskey() const
{
  // Syskey is the only clustering key
  return ((indexKeyColumns_.entries() == 1) &&
          (indexKeyColumns_[0]->isSyskeyColumn()));
}

NABoolean NAFileSet::hasSingleColVarcharKey() const
{
  // clustering key is single column varchar 
  return ((indexKeyColumns_.entries() == 1) &&
          (indexKeyColumns_[0]->getType()->isVaryingLen()));
}

//cleanup after statement, so that this can be used for the next statement
void NAFileSet::resetAfterStatement()
{
  if(resetAfterStatement_)
    return;

  if(partFunc_)
    partFunc_->resetAfterStatement();




  for (UInt32 i = 0; i < allColumns_.entries(); i++)
  {
    //reset each NAColumn
    if(allColumns_[i])
      allColumns_[i]->resetAfterStatement();
  }

  for (UInt32 j = 0; j < indexKeyColumns_.entries(); j++)
  {
    //reset each NAColumn
    if(indexKeyColumns_[j])
      indexKeyColumns_[j]->resetAfterStatement();
  }

  resetAfterStatement_ = TRUE;
  setupForStatement_ = FALSE;
  return;
}

//setup before being used in a statement
void NAFileSet::setupForStatement()
{
  if(setupForStatement_)
    return;

  if(partFunc_)
    partFunc_->setupForStatement();




  setupForStatement_ = TRUE;
  resetAfterStatement_= FALSE;
}

Lng32 NAFileSet::getCountOfColumns(
     NABoolean excludeNonKeyColumns,
     NABoolean excludeNonUserSpecifiedAlternateIndexColumns,
     NABoolean excludeSystemColumns,
     NABoolean excludeAlwaysComputedSystemColumns) const
{
  Lng32 numCols = 0;
  const NAColumnArray *colArray = &allColumns_;

  if (excludeNonKeyColumns ||
      excludeNonUserSpecifiedAlternateIndexColumns)
    colArray = &indexKeyColumns_;

  for (CollIndex i=0; i < colArray->entries(); i++)
    {
      // figure out the various exclusion conditions other
      // that non-key columns
      const NAColumn *nac = (*colArray)[i];
      if ( NOT ((excludeNonUserSpecifiedAlternateIndexColumns &&
                 nac->getIndexColName() == nac->getColName())
                ||
                (excludeSystemColumns && nac->isSystemColumn())
                ||
                (excludeAlwaysComputedSystemColumns &&
                 nac->isComputedColumnAlways() &&
                 nac->isSystemColumn())))
        numCols++;
    }

  return numCols;
}

// load-time initialization
static THREAD_P RandomSequence* random_ = NULL;
static THREAD_P NABoolean seeded_ = FALSE;

// seed random number generator
static void seedIt()
{
  if (seeded_)
    return;
  // else
  Lng32 seed = CmpCommon::getDefaultLong(FLOAT_ESP_RANDOM_NUM_SEED);

  if (!random_)
     random_ = new(GetCliGlobals()->exCollHeap()) RandomSequence();

  CLISemaphore * sema = GetCliGlobals()->getSemaphore();
  sema->get();

  if (!seeded_) {
    if (seed == 0) {
      TimeVal tim;
      GETTIMEOFDAY(&tim, 0);
      seed = tim.tv_usec;
    }
    seed = seed % 65535;
    random_->initialize(seed);
    seeded_ = TRUE;
  }
  sema->release();
}

static double srandom()
{
  seedIt();
  double rv;
  CLISemaphore * sema = GetCliGlobals()->getSemaphore();
  sema->get();
  rv = random_->random();
  sema->release();
  return rv;
};

const QualifiedName& NAFileSet::getRandomPartition() const
{
  if (!partFunc_)
    return getFileSetName();

  Int32 numEntries = partFunc_->getNodeMap()->getNumEntries();
  const NodeMapEntry *nme = 
    partFunc_->getNodeMapEntry((CollIndex)floor(srandom()*numEntries));
  QualifiedName *partQName = 
    new (STMTHEAP) QualifiedName(nme->getPartitionName(), 3, STMTHEAP, NULL);
  return *partQName;
}

NAString NAFileSet::getBestPartitioningKeyColumns(char separator) const
{
   const NAColumnArray & partKeyCols = getPartitioningKeyColumns();

   if ( partKeyCols.entries() > 0 ) {
      return partKeyCols.getColumnNamesAsString(separator);
   } else {
      const NAColumnArray& allCols = getAllColumns();
      UInt32 ct = allCols.entries();
      if ( ct > 2 ) ct=2;
      return allCols.getColumnNamesAsString(separator, ct);
   }
}
