/**********************************************************************
//
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
* File:         RelFastTransport.cpp
* Description:  RelExprs related to support FastTransport (Load and Extract)
* Created:      9/29/12
* Language:     C++
*
*************************************************************************
*/

#include "CostMethod.h"
#include "AllRelExpr.h"
#include "Globals.h"
#include "HDFSHook.h"

// -----------------------------------------------------------------------
// methods for class RelFastExtract
// -----------------------------------------------------------------------

//! FastExtract::FastExtract Copy Constructor
FastExtract::FastExtract(const FastExtract & other)
: RelExpr(other)
{
  targetType_ = other.targetType_;
  targetName_ = other.targetName_;
  hdfsHostName_ = other.hdfsHostName_;
  hdfsPort_ = other.hdfsPort_;
  hiveTableDesc_ = other.hiveTableDesc_;
  hiveTableName_ = other.hiveTableName_;
  delimiter_ = other.delimiter_;
  isAppend_ = other.isAppend_;
  includeHeader_ = other.includeHeader_;
  header_ = other.header_;
  cType_ = other.cType_,
  nullString_ = other.nullString_,
  recordSeparator_ = other.recordSeparator_;
  selectList_ = other.selectList_;
  isSequenceFile_ = other.isSequenceFile_;
  overwriteHiveTable_ = other.overwriteHiveTable_;
  hiveNATable_ = other.hiveNATable_;
  isMainQueryOperator_ = other.isMainQueryOperator_;
  isPartitioned_ = FALSE;
}

//! FastExtract::~FastExtract Destructor
FastExtract::~FastExtract()
{

}

RelExpr *FastExtract::makeFastExtractTree(
     TableDesc *tableDesc,
     RelExpr *child,
     ItemExpr *orderByTree,
     NABoolean overwriteTable,
     const ItemExprList *partColValList,
     NABoolean calledFromBinder,
     NABoolean tempTableForCSE,
     BindWA *bindWA)
{
  RelExpr *result = NULL;
  const HHDFSTableStats* hTabStats = 
      tableDesc->getNATable()->getClusteringIndex()->getHHDFSTableStats();
				 
  const char * hiveTablePath;
  NAString hostName;
  Int32 hdfsPort;
  NAString tableDir;

  char fldSep[2];
  char recSep[2];
  memset(fldSep,'\0',2);
  memset(recSep,'\0',2);
  fldSep[0] = hTabStats->getFieldTerminator();
  recSep[0] = hTabStats->getRecordTerminator();

  // don't rely on timeouts to invalidate the HDFS stats for the target table,
  // make sure that we invalidate them right after compiling this statement,
  // at least for this process
  ((NATable*)(tableDesc->getNATable()))->setClearHDFSStatsAfterStmt(TRUE);

  hiveTablePath = hTabStats->tableDir();
  NABoolean splitSuccess = TableDesc::splitHiveLocation(
       hiveTablePath,
       hostName,
       hdfsPort,
       tableDir,
       CmpCommon::diags(),
       hTabStats->getPortOverride());      
 
  if (!splitSuccess) {
    *CmpCommon::diags() << DgSqlCode(-4224)
                        << DgString0(hiveTablePath);
    bindWA->setErrStatus();
    return NULL;
  }

  const NABoolean isSequenceFile = hTabStats->isSequenceFile();
  
  FastExtract * unloadRelExpr =
    new (bindWA->wHeap()) FastExtract(
         child,
         new (bindWA->wHeap()) NAString(hiveTablePath, bindWA->wHeap()),
         new (bindWA->wHeap()) NAString(hostName, bindWA->wHeap()),
         hdfsPort,
         tableDesc,
         new (bindWA->wHeap()) NAString(
              tableDesc->getCorrNameObj().getQualifiedNameObj().getObjectName(),
              bindWA->wHeap()),
         FastExtract::FILE,
         bindWA->wHeap());
  unloadRelExpr->setRecordSeparator(recSep);
  unloadRelExpr->setDelimiter(fldSep);
  unloadRelExpr->setOverwriteHiveTable(overwriteTable);
  unloadRelExpr->setSequenceFile(isSequenceFile);
  unloadRelExpr->setHiveNATable(tableDesc->getNATable());
  unloadRelExpr->setIsMainQueryOperator(calledFromBinder);

  if (orderByTree) {
    RETDesc *savedRETDesc = bindWA->getCurrentScope()->getRETDesc();

    bindWA->getCurrentScope()->context()->inOrderBy() = TRUE;
    bindWA->getCurrentScope()->setRETDesc(child->getRETDesc());
    orderByTree->convertToValueIdList(unloadRelExpr->reqdOrder(), 
                                      bindWA, ITM_ITEM_LIST);
      
    bindWA->getCurrentScope()->context()->inOrderBy() = FALSE;
    if (bindWA->errStatus()) return NULL;
    bindWA->getCurrentScope()->setRETDesc(savedRETDesc);
  }

  result = unloadRelExpr;

  if (overwriteTable)
    {
      if ((tableDesc->getNATable()->getClusteringIndex()->numHivePartCols() == 0) &&
          (partColValList)) // explicit partition list specified
        {
          *CmpCommon::diags()
            << DgSqlCode(-3242)
            << DgString0("Cannot specify drop partition list for a non-partitioned table.");

          bindWA->setErrStatus();
          return NULL;
        }

      ExeUtilHiveTruncate *trunc = new (bindWA->wHeap())
        ExeUtilHiveTruncate(tableDesc->getCorrNameObj(),
                            partColValList,
                            TRUE,
                            bindWA->wHeap());
      trunc->setNoSecurityCheck(TRUE);

      RelExpr * newRelExpr = trunc;

      if ((tableDesc->getNATable()->getClusteringIndex()->numHivePartCols() > 0) &&
          (! partColValList)) // explicit partition list not specified
        {
          // INSERT OVERWRITE TABLE is not supported for partitioned Hive tables
          // if an explicit list to drop partitions is not specified.  
          // Hive selectively overwrites partitions that
          // are touched by this statement (when using dynamic
          // partition insert, which is the equivalent of a Trafodion
          // insert into a partition table). Our design of making a
          // UNION node to clear the table at compile time won't work
          // with the need to empty partitions dynamically. Note: We
          // could potentially allow this if we had compile time
          // constants for all partitioning columns...
          *CmpCommon::diags()
            << DgSqlCode(-4223)
            << DgString0("INSERT OVERWRITE TABLE into partitioned Hive tables is");
          bindWA->setErrStatus();
          return NULL;
        }

      if (tempTableForCSE)
        {
          // This table gets created at compile time, unlike most
          // other tables. It gets dropped when the statement is
          // deallocated. Note that there are three problems:
          // a) Statement gets never executed
          // b) Process exits before deallocating the statement
          // c) Statement gets deallocated, then gets executed again
          //
          // Todo: CSE: Handle these issues.
          // Cases a) and b) are handled like volatile tables, there
          // is a cleanup mechanism.
          // Case c) gets handled by AQR.
          trunc->setDropTableOnDealloc();
        }

      if (calledFromBinder)
        //new root to prevent  error 4056 when binding
        newRelExpr = new (bindWA->wHeap()) RelRoot(newRelExpr);
      else
        // this node must be bound, even outside the binder,
        // to set some values
        newRelExpr = newRelExpr->bindNode(bindWA);

      Union *blockedUnion = new (bindWA->wHeap()) Union(newRelExpr, result);

      blockedUnion->setBlockedUnion();
      blockedUnion->setSerialUnion();
      result = blockedUnion;
    }

  return result;
}

//! FastExtract::copyTopNode method
RelExpr * FastExtract::copyTopNode(RelExpr *derivedNode,
                                CollHeap* outHeap)

{
   FastExtract *result;

   if (derivedNode == NULL)
   {
     result = new (outHeap) FastExtract(NULL, outHeap);
   }
   else
     result = (FastExtract *) derivedNode;

  result->targetType_ = targetType_;
  result->targetName_ = targetName_;
  result->hdfsHostName_ = hdfsHostName_;
  result->hdfsPort_ = hdfsPort_;
  result->hiveTableDesc_= hiveTableDesc_;
  result->hiveTableName_ = hiveTableName_;
  result->delimiter_ = delimiter_;
  result->isAppend_ = isAppend_;
  result->includeHeader_ = includeHeader_;
  result->header_ = header_;
  result->cType_ = cType_;
  result->nullString_ = nullString_;
  result->recordSeparator_ = recordSeparator_ ;
  result->selectList_ = selectList_;
  result->isSequenceFile_ = isSequenceFile_;
  result->hiveNATable_ = hiveNATable_;
  result->overwriteHiveTable_ = overwriteHiveTable_;
  result->reqdOrder_ = reqdOrder_;
  result->isMainQueryOperator_ = isMainQueryOperator_;
  result->isPartitioned_= isPartitioned_;

  return RelExpr::copyTopNode(result, outHeap);
}

//! FastExtract::getText method
const NAString FastExtract::getText() const
{

  if (isHiveInsert())
  {
    NAString op(CmpCommon::statementHeap());
    if (hiveNATable_->isORC())
      op = "orc_insert";
    else if (hiveNATable_->isParquet())
      op = "parquet_insert";
    else if (hiveNATable_->isAvro())
      op = "avro_insert";
    else
      op = "hive_insert";
    NAString tname(hiveTableName_,CmpCommon::statementHeap());
    return op + " " + tname;
  }
  else
    return "UNLOAD";
}

void FastExtract::addLocalExpr(LIST(ExprNode *) &xlist,
                            LIST(NAString) &llist) const
{ 
  if (NOT selectList_.isEmpty())
  {
    xlist.insert(selectList_.rebuildExprTree());
    llist.insert("select_list");
  }

  if (NOT partStringExpr_.isEmpty())
  {
    xlist.insert(partStringExpr_.rebuildExprTree());
    llist.insert("partition_string");
  }

  RelExpr::addLocalExpr(xlist,llist);
};

void FastExtract::transformNode(NormWA & normWARef,
    ExprGroupId & locationOfPointerToMe) 
{
  RelExpr::transformNode(normWARef, locationOfPointerToMe);
};

void FastExtract::rewriteNode(NormWA & normWARef)
{
  selectList_.normalizeNode(normWARef);
  // rewrite group attributes and selection pred
  RelExpr::rewriteNode(normWARef);

  FastExtract * fe
    = (FastExtract *)(this->castToRelExpr());
  
  if (fe->reqdOrder().normalizeNode(normWARef))
    {
    }
} ;

RelExpr * FastExtract::normalizeNode(NormWA & normWARef)
{
  return RelExpr::normalizeNode(normWARef);
};
 
void FastExtract::getPotentialOutputValues(ValueIdSet & vs) const
{
  vs.clear();
};

void FastExtract::recomputeOuterReferences()
{
  ValueIdSet outerRefs(getGroupAttr()->getCharacteristicInputs());
  ValueIdSet allMyExprs(getSelectionPred());

  allMyExprs.insertList(selectList_);
  allMyExprs.insertList(reqdOrder_);
  allMyExprs.weedOutUnreferenced(outerRefs);

  // Add references needed by children, if any
  for (Int32 i = 0; i < getArity(); i++)
    outerRefs += child(i).getPtr()->getGroupAttr()->getCharacteristicInputs();

  getGroupAttr()->setCharacteristicInputs(outerRefs);
}

void FastExtract::pullUpPreds()
{
   // A FastExtract never pulls up predicates from its children.
   for (Int32 i = 0; i < getArity(); i++) 
    child(i)->recomputeOuterReferences();
};


void FastExtract::pushdownCoveredExpr(
                                   const ValueIdSet & outputExprOnOperator,
                                   const ValueIdSet & newExternalInputs,
                                   ValueIdSet& predOnOperator,
                                   const ValueIdSet *
                                   	   nonPredNonOutputExprOnOperator,
                                   Lng32 childId)
{
  ValueIdSet exprOnParent;
  if(nonPredNonOutputExprOnOperator)
    exprOnParent = *nonPredNonOutputExprOnOperator;

    exprOnParent.insertList(getSelectList());

    RelExpr::pushdownCoveredExpr(outputExprOnOperator,
                                  newExternalInputs,
                                  predOnOperator,
                                  &exprOnParent,
                                  childId);
};

void FastExtract::synthEstLogProp(const EstLogPropSharedPtr& inputLP)
{
  // get child histograms
  EstLogPropSharedPtr childrenInputEstLogProp;
  CostScalar inputFromParentCard = inputLP->getResultCardinality();

  EstLogPropSharedPtr myEstProps(new (HISTHEAP) EstLogProp(*inputLP));
  childrenInputEstLogProp = child(0).outputLogProp(inputLP);
  CostScalar extractCard = childrenInputEstLogProp->getResultCardinality();
  myEstProps->setResultCardinality(extractCard);
  // attach histograms to group attributes
  getGroupAttr()->addInputOutputLogProp (inputLP, myEstProps);

};

short FastExtract::setOptions(NAList<UnloadOption*> *
                              fastExtractOptionList,
                              ComDiagsArea * da)
{
  if (!fastExtractOptionList)
    return 0;

  for (CollIndex i = 0; i < fastExtractOptionList->entries(); i++)
  {
    UnloadOption * feo = (*fastExtractOptionList)[i];
    switch (feo->option_)
    {
      case UnloadOption::DELIMITER_:
      {
        if (delimiter_.length() == 0)
          delimiter_ = feo->stringVal_;
        else
        {
          *da << DgSqlCode(-4376) << DgString0("DELIMITER");
          return 1;
        }
      }
      break;
      case UnloadOption::NULL_STRING_:
      {
        if (nullString_.length() == 0)
          nullString_ = feo->stringVal_;
        else
        {
          *da << DgSqlCode(-4376) << DgString0("NULL_STRING");
          return 1;
        }
        nullStringSpec_ = TRUE;
      }
      break;
      case UnloadOption::RECORD_SEP_:
      {
        if (recordSeparator_.length() == 0)
          recordSeparator_ = feo->stringVal_;
        else
        {
          *da << DgSqlCode(-4376) << DgString0("RECORD_SEPARATOR");
          return 1;
        }
      }
      break;
      case UnloadOption::APPEND_:
      {
        if (!isAppend_)
          isAppend_ = TRUE;
        else
        {
          *da << DgSqlCode(-4376) << DgString0("APPEND");
          return 1;
        }
      }
      break;
      case UnloadOption::HEADER_:
      {
        if (includeHeader_)
          includeHeader_ = FALSE;
        else
        {
          *da << DgSqlCode(-4376) << DgString0("HEADER");
          return 1;
        }
      }
      break;
      case UnloadOption::COMPRESSION_:
      {
        if (cType_ == NONE)
          cType_ = (CompressionType) feo->numericVal_;
        else
        {
          *da << DgSqlCode(-4376) << DgString0("COMPRESSION");
          return 1;
        }
      }
      break;
      default:
        return 1;
    }
  }
  return 0;

};




  ///////////////////////////////////////////////////////////

  // ---------------------------------------------------------------------
  // comparison, hash, and copy methods
  // ---------------------------------------------------------------------

HashValue FastExtract::topHash()
{
  HashValue result = RelExpr::topHash();
  result ^= getSelectList() ;
  return result;
};

NABoolean FastExtract::duplicateMatch(const RelExpr & other) const
{
  if (!RelExpr::duplicateMatch(other))
    return FALSE;

  FastExtract &o = (FastExtract &) other;

  if (NOT (getSelectList() == o.getSelectList()))
      return FALSE;

  return TRUE;
};

// -----------------------------------------------------------------------
// methods for class PhysicalFastExtract
// -----------------------------------------------------------------------


PhysicalProperty* PhysicalFastExtract::synthPhysicalProperty(const Context* myContext,
                                                             const Lng32     planNumber,
                                                             PlanWorkSpace  *pws)
{
  PartitioningFunction* myPartFunc = NULL;
  const IndexDesc* myIndexDesc = NULL;

    // simply propagate the physical property
    const PhysicalProperty * const sppOfChild =
      myContext->getPhysicalPropertyOfSolutionForChild(0);
    myPartFunc = sppOfChild->getPartitioningFunction();
    myIndexDesc = sppOfChild->getIndexDesc();
  
    PhysicalProperty * sppForMe =
    new(CmpCommon::statementHeap()) PhysicalProperty(
         myPartFunc,
         EXECUTE_IN_MASTER_AND_ESP,
         SOURCE_VIRTUAL_TABLE);

  // remove anything that's not covered by the group attributes
  sppForMe->enforceCoverageByGroupAttributes (getGroupAttr()) ;
  sppForMe->setIndexDesc(myIndexDesc);

  return sppForMe ;
};

double PhysicalFastExtract::getEstimatedRunTimeMemoryUsage(Generator *generator, ComTdb * tdb) 
{

// The executor attempts to get buffers, each of size 1 MB. This memory
// is not from the SQLMXBufferSpace though, but is got directly from TSE
// through EXECUTOR_DP2_ADD_MEMORY. We may not get 1MB of memory for each 
// buffer at runtime. This value is interpreted as a potential/likely
// maximum. totalMemory is per TSE now.
  double totalMemory = ((ActiveSchemaDB()->getDefaults()).
                        getAsULong(FAST_EXTRACT_IO_BUFFERS))*1024*1024;

  const PhysicalProperty* const phyProp = getPhysicalProperty();
  if (phyProp != NULL)
  {
    PartitioningFunction * partFunc = phyProp -> getPartitioningFunction() ;
    // totalMemory is for all TSEs at this point of time.
    totalMemory *= partFunc->getCountOfPartitions();
  }

  return totalMemory;

}

// -----------------------------------------------------------------------
// PhysicalFastExtract::costMethod()
// Obtain a pointer to a CostMethod object providing access
// to the cost estimation functions for nodes of this class.
// -----------------------------------------------------------------------
CostMethod* PhysicalFastExtract::costMethod() const
{
  /*static THREAD_P CostMethodFastExtract *m = NULL;
  if (m == NULL)
    m = new (GetCliGlobals()->exCollHeap())  CostMethodFastExtract();
  return m;*/
  return pCostMethod_;

}


