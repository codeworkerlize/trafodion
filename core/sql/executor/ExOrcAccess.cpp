// **********************************************************************
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
// **********************************************************************

#include "Platform.h"

#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/time.h>
#include <poll.h>
#include <zlib.h>
#include <iostream>
#include <algorithm>

#include "ex_stdh.h"
#include "ComTdb.h"
#include "ex_tcb.h"
#include "ExHdfsScan.h"
#include "ExOrcAccess.h"
#include "ExHbaseAccess.h"
#include "ex_exe_stmt_globals.h"
#include "ExpLOBinterface.h"
#include "Hbase_types.h"
#include "stringBuf.h"
#include "NLSConversion.h"
#include "exp_clause_derived.h"

#include  "cli_stdh.h"

#include "ExpORCinterface.h"
#include "ExpParquetInterface.h"

#ifdef BUILD_PARQUET_READER
#include "ExpParquetArrowInterface.h"
#endif

#include "ExpAvroInterface.h"
#include "ttime.h"
#include "QRLogger.h"

ex_tcb * ExExtStorageScanTdb::build(ex_globals * glob)
{
  ExExeStmtGlobals * exe_glob = glob->castToExExeStmtGlobals();
  
  ex_assert(exe_glob,"This operator cannot be in DP2");

  ExHdfsScanTcb *tcb = NULL;
  
  if (isOrcFile() || isParquetFile() || isAvroFile())
    {
      tcb = new(exe_glob->getSpace()) 
        ExExtStorageScanTcb(
             *this,
             exe_glob);
    }
     
  ex_assert(tcb, "Error building ExHdfsScanTcb.");

  return (tcb);
}


ex_tcb * ExExtStorageFastAggrTdb::build(ex_globals * glob)
{
  ExHdfsScanTcb *tcb = NULL;
  tcb = new(glob->getSpace()) 
    ExExtStorageFastAggrTcb(
                     *this,
                     glob);
  
  ex_assert(tcb, "Error building ExHdfsScanTcb.");

  return (tcb);
}

////////////////////////////////////////////////////////////////////////
// External Storage files (currently: orc, parquet and avro)
////////////////////////////////////////////////////////////////////////
ExExtStorageScanTcb::ExExtStorageScanTcb(
          const ComTdbExtStorageScan &ext_scan_tdb, 
          ex_globals * glob ) :
  ExHdfsScanTcb(ext_scan_tdb, glob),
  step_(NOT_STARTED)
#ifdef NEED_INSTRUMENT_EXTSCAN
  , finalReturns_(0)
  , poolBlocked_(0)
  , isFull_(0)
  , done_(0)
  , handle_errors_(0)
  , rows_(0)
  , totalRows_(0)
  , totalScanTime_(0) 
  , totalScanOpenTime_(0) 
  , totalScanFetchTime_(0) 
  , totalScanCloseTime_(0) 
  , scanFetchCalls_(0)
#endif
{
  Space * space = (glob ? glob->getSpace() : 0);
  CollHeap * heap = (glob ? glob->getDefaultHeap() : 0);

  exti_ = NULL;
  /*
  ExpExtStorageInterface::StorageType st = ExpORCinterface::UNKNOWN_;
  if (extScanTdb().isOrcFile())
    st = ExpExtStorageInterface::ORC_;
  else if (extScanTdb().isParquetFile())
    st = ExpExtStorageInterface::PARQUET_;
  else if (extScanTdb().isAvroFile())
    st = ExpExtStorageInterface::AVRO_;
  */

  if (extScanTdb().isOrcFile())
    exti_ = ExpORCinterface::newInstance(glob->getDefaultHeap(),
                                         (char*)extScanTdb().hostName_,
                                         extScanTdb().port_, extScanTdb().orcVectorizedScan());
  else if (extScanTdb().isParquetFile())
  {
#ifdef BUILD_PARQUET_READER
    if (extScanTdb().useArrowReader()) {

       ContextCli *currContext = GetCliGlobals()->currContext();

       hdfsConnectStruct* entryPtr = NULL;
       hdfsFS hdfs = currContext->
            getHdfsServerConnection((char*)extScanTdb().hostName_,
                                    extScanTdb().port_,
                                    &entryPtr);

       assert(entryPtr);

       std::shared_ptr<arrow::io::HadoopFileSystem> client;

       if ( entryPtr->arrowFileSystem_.use_count() == 0 ) {

          // First time use of the hdfs in the CliContext cache.
          arrow::io::HdfsConnectionConfig config(
                                     (char*)extScanTdb().hostName_,
                                     extScanTdb().port_,
                                     "trafodion"
                                     );

          // Setup hdfs in the Arrow Reader io Layer.
          arrow::io::HadoopFileSystem::Reuse(hdfs, &config, &client);

          // Need to keep a separate copy of 
          // shared_ptr<HadoopFileSystem> so that the managed 
          // HadoopFileSystem will not be deleted when 
          // object exti_ is deleted. 
          //
          // We do so by caching the shared pointer in the 
          // hdfs connection cache in cliContext.
          entryPtr->arrowFileSystem_ = client;
       } else {
          // Just reuse the existing arrow file system.
          client = entryPtr->arrowFileSystem_;
       }

       exti_ = ExpParquetArrowInterface::newInstance(
                                         glob->getDefaultHeap(),
                                         extScanTdb().parquetFullScan());

       // The client is needed by the Arrow reader to acceess
       // the parquet data files through hdfs.
       ((ExpParquetArrowInterface*)exti_)->setHdfsClient(client);


    } else
#endif
       exti_ = ExpParquetInterface::newInstance(glob->getDefaultHeap(),
                                                (char*)extScanTdb().hostName_,
                                                extScanTdb().port_);
  }
  else if (extScanTdb().isAvroFile())
    exti_ = ExpAvroInterface::newInstance(glob->getDefaultHeap(),
                                             (char*)extScanTdb().hostName_,
                                             extScanTdb().port_);
  
  numCols_ = 0;
  whichCols_ = NULL;

  if ((extScanTdb().getHdfsColInfoList()) &&
      (extScanTdb().getHdfsColInfoList()->numEntries() > 0))
    {
      numCols_ = extScanTdb().getHdfsColInfoList()->numEntries();

      whichCols_ = 
        new(glob->getDefaultHeap()) Lng32[extScanTdb().getHdfsColInfoList()->numEntries()];

      Queue *hdfsColInfoList = extScanTdb().getHdfsColInfoList();
      hdfsColInfoList->position();
      int i = 0;
      HdfsColInfo * hco = NULL;
      while ((hco = (HdfsColInfo*)hdfsColInfoList->getNext()) != NULL)
        {
          whichCols_[i] = hco->colNumber();
          i++;
        }
    }

  // fixup expressions
  if (extOperExpr())
    {
      extOperExpr()->fixup(0, getExpressionMode(), this,  
                           space, heap, FALSE, glob);
    }
  
  extOperRow_ = NULL;
  if (extScanTdb().extOperTuppIndex_ > 0)
    {
      pool_->get_free_tuple(workAtp_->getTupp
                            (extScanTdb().extOperTuppIndex_), 0);

      extOperRow_ = new(glob->getDefaultHeap()) 
        char[extScanTdb().extOperLength_];

      memset(extOperRow_, 0, extScanTdb().extOperLength_);

      workAtp_->getTupp(extScanTdb().extOperTuppIndex_)
        .setDataPointer(extOperRow_);

      workAtp_->getTupp(extScanTdb().extOperTuppIndex_)
        .setAllocatedSize(extScanTdb().extOperLength_);

    }
}

ExExtStorageScanTcb::~ExExtStorageScanTcb()
{
  if (hdfsScanTdb().strawScanMode() && getGlobals()->getMyInstanceNumber() == 0){//only ESP0 perform freeing of strawScan
    exti_->freeStrawScan(queryId_,explainNodeId_);
  }

  if (exti_ != NULL) {
    delete exti_;
  }
}

short ExExtStorageScanTcb::extractAndTransformExtSourceToSqlRow(
                                                         char * extRow,
                                                         Int64 extRowLen,
                                                         Lng32 numExtCols,
                                                         ComDiagsArea* &diagsArea)
{
  short err = 0;

  if (numExtCols == 0)
    return 0;

  if ((!extRow) || (extRowLen <= 0))
    return -1;

  char *sourceData = extRow;

  ExpTupleDesc * asciiSourceTD =
     extScanTdb().workCriDesc_->getTupleDescriptor(extScanTdb().asciiTuppIndex_);
  if (asciiSourceTD->numAttrs() == 0)
    {
      // no columns need to be converted. For e.g. count(*) with no predicate
      return 0;
    }
  
  Attributes * attr = NULL;

  Lng32 currColLen;
            
  for (Lng32 i = 0; i < asciiSourceTD->numAttrs(); i++) 
       {
         attr = asciiSourceTD->getAttr(i);
   
         currColLen = *(Lng32*)sourceData;
         sourceData += sizeof(currColLen);
   
         if (attr->getNullFlag())
           {
             if (currColLen == -1)
               *(short *)&hdfsAsciiSourceData_[attr->getNullIndOffset()] = -1;
             else
               *(short *)&hdfsAsciiSourceData_[attr->getNullIndOffset()] = 0;
           }
   
         Lng32 copyLen = currColLen;
         if (currColLen >= 0)
           {
             if (attr->getVCIndicatorLength() > 0)
               {
                 copyLen = MINOF(currColLen, attr->getLength());
   
                 str_cpy_all((char*)&hdfsAsciiSourceData_[attr->getOffset()],
                             sourceData, copyLen);
                 
                 char * vcLenLoc = 
                   &hdfsAsciiSourceData_[attr->getVCLenIndOffset()];
                 attr->setVarLength(copyLen, vcLenLoc);
               }
             else if (DFS2REC::isSQLFixedChar(attr->getDatatype()))
               {
                 byte_str_cpy((char*)&hdfsAsciiSourceData_[attr->getOffset()],
                              attr->getLength(), sourceData, copyLen, ' ');
               }
             else if (attr->isNumericWithPrecision())
               {

                 ex_expr::exp_return_type rc;
                 rc = convHiveDecimalToNumeric
                   (sourceData, currColLen, 
                    (char*)&hdfsAsciiSourceData_[attr->getOffset()],
                    attr, getHeap(), &diagsArea);
               } // numeric type with precision
             else
               {
                 str_cpy_all((char*)&hdfsAsciiSourceData_[attr->getOffset()],
                             sourceData, copyLen);
               }
             sourceData += currColLen;
           } // currColLen >= 0
       }

  err = 0;

  workAtp_->getTupp(extScanTdb().workAtpIndex_) = hdfsSqlTupp_;
  workAtp_->getTupp(extScanTdb().asciiTuppIndex_) = hdfsAsciiSourceTupp_;
  workAtp_->getTupp(extScanTdb().moveExprColsTuppIndex_) = moveExprColsTupp_;

  if (convertExpr())
    {
      ex_expr::exp_return_type evalRetCode =
        convertExpr()->eval(workAtp_, workAtp_);
      if (evalRetCode == ex_expr::EXPR_ERROR)
        err = -1;
      else
        err = 0;
    }
  
  return err;
}

Int32 ExExtStorageScanTcb::fixup()
{

  if (hdfsScanTdb().strawScanMode() && getGlobals()->getMyInstanceNumber() == (explainNodeId_ % getGlobals()->getNumOfInstances())){//getGlobals()->getNumOfInstances always not null, because strawScanMode() is false for Master plans.
	 QRLogger::log(CAT_SQL_HDFS_SCAN,
	                LL_DEBUG,
	                "ExtStorageScanTcb::fixup entered called by instance %d.",getGlobals()->getMyInstanceNumber()
	                );
	 CollIndex  entriesLength =  hdfsFileInfoListAsArray_.entries();
	 NAText entries[entriesLength];
	 char buf[100];
	 for(CollIndex i=0;i<entriesLength;i++){
		 HdfsFileInfo* hdfo =  hdfsFileInfoListAsArray_.at(i);
		 //copy filename but delete any | char and truncate to 5000. There is no impact for straw scan feature to have false file names equality due to deleting | or truncating to 5000
		 char* charptr = hdfo->fileName_.getPointer();
		 ULng32 fileChecksum = crc32(0L, Z_NULL, 0);
		 fileChecksum = crc32 (fileChecksum, (const unsigned char*)buf, strlen(buf));
		 snprintf(buf,100,"%u|%d|%d|%lld|%lld||%s| ",fileChecksum,i,hdfo->hdfsBlockNb_,(long long int)hdfo->getStartOffset(),(long long int)hdfo->getBytesToRead(),hdfo->splitUnitBlockLocations_.getPointer());
		 entries[i].assign(buf);
	 }

     exti_->initStrawScan(hdfsScanTdb().strawscanWebServer(), queryId_,explainNodeId_,true,entries,entriesLength);// for now hardcode isFactTable to true until we have compiler support
     QRLogger::log(CAT_SQL_HDFS_SCAN,
                   LL_DEBUG,
                   "ExtStorageScanTcb::fixup called by instance %d.",getGlobals()->getMyInstanceNumber()
                   );
  }
  return 0;
}

short ExExtStorageScanTcb::createColInfoVecs
(Queue *colNameList,
 Queue *colTypeList,
 TextVec &colNameVec,
 TextVec &colTypeVec)
{
  if ((! colNameList) ||
      (! colTypeList) ||
      (colNameList->isEmpty()) ||
      (colTypeList->isEmpty()) ||
      (colNameList->numEntries() !=
       colTypeList->numEntries()))
    return -1;

  colNameVec.clear();
  colTypeVec.clear();

  colNameList->position();
  for (Lng32 i = 0; i <  colNameList->numEntries(); i++) // for each col  
    {
      char * cn = (char*)colNameList->getNext();

      Text t(cn);
      colNameVec.push_back(t);
    }

  colTypeList->position();
  for (Lng32 i = 0; i <  colTypeList->numEntries(); i++) // for each col
    {
      Text typeInfo;

      Lng32 *temp;
      temp = (Int32*)colTypeList->getNext();
      typeInfo.append((char*)&temp[0], sizeof(Lng32)); // type
      typeInfo.append((char*)&temp[1], sizeof(Lng32)); // length
      typeInfo.append((char*)&temp[2], sizeof(Lng32)); // precision
      typeInfo.append((char*)&temp[3], sizeof(Lng32)); // scale

      colTypeVec.push_back(typeInfo);
    }  

  return 0;
}
            
void Queue::displayForPPIs()
{
   void* curr = getCurrEntryPtr();

   position(); // re-position to the start position

   ComTdbExtPPI* ppi = NULL;
   Int32 i=0;
   while ((ppi = (ComTdbExtPPI*)getNext()) != NULL)
   { 
      cout << "ppi[" << i++ << "]: ";
      ppi->display(cout);
   }

   position(curr); // re-position to the curr position
}

//
// process one term for either AND and OR expression and return
//  TRUE:   if we have not reached the end of the expression;
//  FALSE : if we have reached the end of the expression.
//
// Also use extScanTcb to help detect SQL NULL for the term, and
// set isaNullTerm if the term is a SQL NULL.
//
// A STARTNOT term is considered a reverse of a comparison term
// of form LESSTHAN, EQUALS, or LESSTHANEQUALS and as such does
// not alter the semantics of SQL NULL: 
//   NOT(EQUALS(<c>, NULL) evaluates to FALSE and is a SQL null
//   term.
NABoolean 
Queue::processOneTerm(ExExtStorageScanTcb* extScanTcb, NABoolean& isaNullTerm)
{
   ComTdbExtPPI* ppi = (ComTdbExtPPI*)getCurr();
   
   switch (ppi->type())
      {
        case ExtPushdownOperatorType::LESSTHAN:
        case ExtPushdownOperatorType::EQUALS:
        case ExtPushdownOperatorType::LESSTHANEQUALS:
          {
            Lng32 operIndex = ppi->operAttrIndex();

            if (operIndex >= 0)
            {
                ExpTupleDesc* extOperTD =
                       (extScanTcb->extScanTdb()).workCriDesc_->getTupleDescriptor
                       ((extScanTcb->extScanTdb()).extOperTuppIndex_);

                assert(extOperTD);

                Attributes * attr = extOperTD->getAttr(operIndex);
                if ((attr->getNullFlag()) &&
                    ExpTupleDesc::isNullValue(
                         &(extScanTcb->extOperRow_)[attr->getNullIndOffset()],
                         attr->getNullBitIndex(),
                         attr->getTupleFormat())
                   )
                 {
                   isaNullTerm = TRUE;
                 }
            }
          advance(); // skip term <, = or <=
          return TRUE;
          }

        case ExtPushdownOperatorType::ISNULL:
          advance(); // skip ISNULL
          return TRUE;

        case ExtPushdownOperatorType::END:
          advance(); // skip END
          return FALSE;

        // Since SQL nulls have been filtered out before
        // this scan is reached, we simply skip this IN PPI.
        case ExtPushdownOperatorType::IN: 
          advance(); // skip IN
          return TRUE;

        case ExtPushdownOperatorType::STARTNOT:
          advance();  // skip STARTNOT
          processOneTerm(extScanTcb, isaNullTerm);
          advance();  // skip END
          return TRUE;

        case ExtPushdownOperatorType::STARTAND:
          advance(); // skip STARTAND
          this->processNullsForAND(ppi, extScanTcb);
          isaNullTerm = ppi->isaNullTerm();
          return TRUE;

        case ExtPushdownOperatorType::STARTOR:
          advance(); // skip STARTOR
          this->processNullsForOR(ppi, extScanTcb);
          isaNullTerm = ppi->isaNullTerm();
          return TRUE;

        default:
          assert(0);
      }
  return FALSE;
}

// Scan each item for an AND predicate and 
//
//   1. If one term in an AND predicate involves NULL, 
//      remove the AND predicate;
//
void Queue::processNullsForAND(ComTdbExtPPI* ppi, ExExtStorageScanTcb* extScanTcb)
{
   NABoolean isaNullTerm = FALSE;
   // process every term for the AND until END 
   while (this->processOneTerm(extScanTcb, isaNullTerm)) {
      if ( isaNullTerm ) {
         ppi->setIsaNullTerm(TRUE);
      }
   }
   // When we come to here, the matching END is skipped.
   if ( ppi->isaNullTerm() ) {
      removeStartingFrom(ppi);
   }
}

// Scan each item for a OR predicate and 
//
//   1. If one term in an OR predicate involves NULL, 
//      remove the term;
//   2. If all terms in an OR predicate involve NULL, 
//      remove the OR predicate.
//
void Queue::processNullsForOR(ComTdbExtPPI* ppi, ExExtStorageScanTcb* extScanTcb)
{
   NABoolean allTermsAreNull = TRUE;
   NABoolean isaNullTerm = FALSE;
   void* ppiOfTerm = getCurr();

   // process every term for the OR until END 
   while (this->processOneTerm(extScanTcb, isaNullTerm)) {
      if ( !isaNullTerm ) {
        allTermsAreNull = FALSE;
      } else
         removeStartingFrom(ppiOfTerm);

      ppiOfTerm = getCurr();
   }

   // When we come to here, the matching END is skipped.
   
   if ( allTermsAreNull ) {
      ppi->setIsaNullTerm(TRUE);
      removeStartingFrom(ppi);
   }
}

// process NUll values for all PPI expressions contained
// in this Queue data structure. The idea is to scan each
// expression and perform an action if it is a SQL NULL 
// expression such as ISEQUALS(<column>, NULL). The action
// can be one of the following.
//
//   1. If one term in an AND predicate involves NULL, 
//      remove the AND predicate;
//   2. If one term in an OR predicate involves NULL, 
//      remove the term;
//   3. If all terms in an OR predicate involve NULL, 
//      remove the OR predicate.
//
Queue* Queue::processNulls(ExExtStorageScanTcb* extScanTcb)
{
   ComTdbExtPPI * ppi = NULL;

   while ((ppi = (ComTdbExtPPI*)this->getNext()) != NULL)
   {
      Lng32 type = ppi->type();

      switch (type ) {
        case ExtPushdownOperatorType::STARTAND:
          // Process all items belonging to the AND and possibly set the
          // null rejecting flag for the AND. ppiList is positioned at 
          // the next item after STARTAND ... ... END
          this->processNullsForAND(ppi, extScanTcb); 
          break;

        case ExtPushdownOperatorType::STARTOR:
          // Process all items belonging to the OR and possibly set the
          // null rejecting flag for the OR. ppiList is positioned at 
          // the next item after STARTOR ... ... END
          this->processNullsForOR(ppi, extScanTcb); 
          break;

        default:
          break;
      }
   }
   
   position();
   return this;
}

void ExExtStorageScanTcb::processBloomFilterData(
                            ComTdbExtPPI * ppi, // in
                            char* data, // in
                            Lng32 dataLen, // in
                            std::vector<filterRecord>& filterVec // in/out
                           )
{
    // For some unknown reasons, we could see null data coming into
    // the scan node, for example for tpcds q5, sf1000, parquet. 
    if ( dataLen == 0 || !data )
      return;

    BloomFilterRT sink(dataLen, getHeap());
    UInt32 unpackedLen = sink.unpack(data);

#ifdef DEBUG_SCAN_FILTER
    char buf[200];
    genFullMessage(buf, sizeof(buf), "ExOrAccess", 
                   extScanTdb().getExplainNodeId());

    fstream& out = getPrintHandle();
    out << buf << endl;

    out << "#entries in RS=" << sink.entries() << endl;
    sink.dump(out, "The content of the unpacked RS:");
    out.flush();
                                
    out.close();
#endif

   if ( sink.entries() == 0 || !sink.isEnabled() )
      return;

   // integrate the min and max values
   // into the bloom filter.
   Int32 n = extPPIvec_.size();

   // get the <= predicate, for max
   std::string maxVal = 
        OrcSearchArg::extractValueFromPredicate(extPPIvec_.at(n-1));

   // get the < predicate, for min
   std::string minVal = 
        OrcSearchArg::extractValueFromPredicate(extPPIvec_.at(n-3));


   if ( extScanTdb().removeMinMaxFromScanFilter() ) {
      extPPIvec_.pop_back(); // remove <=
      extPPIvec_.pop_back(); // remove END
      extPPIvec_.pop_back(); // remove <
      extPPIvec_.pop_back(); // remove NOT
   }
 
   std::vector<std::string> filter;
   sink.convertToOrcPredicates(
      filter, ppi->colName(),
      (ppi->colType()).data(), 
      ppi->getFilterId(), minVal, maxVal,
      FALSE /* do not allow IN */ ); 

   filterRecord fr(ppi->rowCount(), 
                   ppi->uec(), 
                   sink.entries(), 
                   sink.isEnabled(),
                   ppi->filterName(), 
                   minVal, // the min value, in len + ascii format
                                      // length is 4 bytes
                   maxVal, // the max value, in len + ascii format
                                      // length is 4 bytes
                   filter,
                   ppi->rowsToSurvive()
                  );

   filterVec.push_back(fr);

                                   
   QRLogger::log(CAT_SQL_EXE_MINMAX, LL_DEBUG,
                 "Ex ORC access: converting %d entries,",
                  sink.entries());
}

void 
ExExtStorageScanTcb::postProcessBloomFilters(
                  std::vector<filterRecord>& filterVec)
                             
{
   // Re-arrange filters in descending order of
   // selectivity and also trim off ones in the 
   // tail if possible. 
   //
   // maxSelectivity() is from 
   // CQD RANGE_OPTIMIZED_SCAN_SELECTIVITY.
   //
   // maxFilters() is from 
   // CQD RANGE_OPTIMIZED_SCAN_MAX_NUM_FILTERS.
   std::sort(filterVec.begin(), filterVec.end());
   
   std::vector<filterRecord>::iterator itr;
   int ct = 0;
   
   NABoolean lastEndRemoved = FALSE;
   
   std::string lastEntry;

   NABoolean active = FALSE;
   
   for (itr=filterVec.begin(); itr<filterVec.end(); ++itr)
   {
      if ( !(itr->isEnabled()) ) {
        active = FALSE;
      } else {
         if ( extScanTdb().maxFilters() > 0 &&  
              ct >= extScanTdb().maxFilters() )  
         {
           active = FALSE;
         } else {
           active = TRUE;
           ct++;
         }
      }

      if ( hdfsStats_ ) {
        Int16 filterId = itr->getFilterId();

        if ( filterId >= 0 ) {

           // Set # of rows affected for now.
           ScanFilterStats scanStats(filterId, 0, 
                                     itr->isEnabled(), active
                                    );


           hdfsStats_->
              addScanFilterStats(scanStats, ScanFilterStats::ADD);
        }
      }

      if ( !active ) {
         continue;
      }  

      if ( !lastEndRemoved ) {
         lastEntry = extPPIvec_[extPPIvec_.size()-1];
         extPPIvec_.pop_back(); // save and remove the last END
         lastEndRemoved = TRUE;
      }

      extPPIvec_.insert(extPPIvec_.end(), 
                        itr->getFilter().begin(), 
                        itr->getFilter().end());
      
      expectedRowsVec_.insert(expectedRowsVec_.end(), itr->rowsToSurvive());
  }

  if ( lastEndRemoved )
     extPPIvec_.push_back(lastEntry); // add back the last END
}

ExWorkProcRetcode ExExtStorageScanTcb::work()
{
  Lng32 retcode = 0;
  short rc = 0;

#ifdef NEED_INSTRUMENT_EXTSCAN
  workTimer_.start();
  rows_ = 0;
#endif
      
  while (!qparent_.down->isEmpty())
    {
      ex_queue_entry *pentry_down = qparent_.down->getHeadEntry();
      if (pentry_down->downState.request == ex_queue::GET_NOMORE)
	step_ = DONE;

      
      switch (step_)
	{
	case NOT_STARTED:
	  {
	    matches_ = 0;
	    
	    beginRangeNum_ = -1;
	    numRanges_ = -1;

	    if (getHdfsFileInfoListAsArray().isEmpty())
	      {
                step_ = DONE;
		break;
	      }

	    myInstNum_ = getGlobals()->getMyInstanceNumber();

            // if my inst num is outside the range of entries, then nothing
            // need to be retrieved. We are done.
            if (myInstNum_ >= hdfsScanTdb().getHdfsFileRangeBeginList()->entries())
              {
                step_ = DONE;
                break;
              }

	    beginRangeNum_ =  
	      *(Lng32*)extScanTdb().getHdfsFileRangeBeginList()->get(myInstNum_);

	    numRanges_ =  
	      *(Lng32*)extScanTdb().getHdfsFileRangeNumList()->get(myInstNum_);

	    currRangeNum_ = beginRangeNum_;

        if (hdfsScanTdb().strawScanMode()){
          executionNb_ = getGlobals()->castToExExeStmtGlobals()->castToExEspStmtGlobals()->getExecutionCount();
          sequenceNb_ = 0;
        }

	    if (numRanges_ > 0)
              step_ = SETUP_EXT_PPI;
            else
              step_ = DONE;
	  }
	  break;
        case SETUP_EXT_PPI:
          {
            if (! extScanTdb().listOfExtPPI())
              {
                step_ = SETUP_EXT_COL_INFO;
                break;
              }

            extPPIvec_.clear();

            ExpTupleDesc * extOperTD = NULL;

	    if (extOperExpr())
	    {
		ex_expr::exp_return_type evalRetCode =
		  extOperExpr()->eval(pentry_down->getAtp(), workAtp_);

                if (evalRetCode == ex_expr::EXPR_ERROR)
                  {
                    step_ = HANDLE_ERROR;
                    break;
                  }

                extOperTD =
                  extScanTdb().workCriDesc_->getTupleDescriptor
                  (extScanTdb().extOperTuppIndex_);
             }

            Queue *ppiList = extScanTdb().listOfExtPPI();
            ComTdbExtPPI * ppi = NULL;
            ppiList->position();

            Lng32 numPPIelems = ppiList->numEntries();
            ppiList->position();

            ppiList = ppiList->processNulls(this);

                                      

            if ( ppiList->isEmpty() ) { 
              // if the PPI list becomes empty due to NULL
              // procssing, skip all steps for the scan.
              step_ = CLOSE_EXT_CURSOR_AND_DONE;
            } else {
 
              std::vector<filterRecord> filterVec;
                 
              NABoolean canPushdownBloomFilters = 
                     extScanTdb().canPushdownBloomFilters();

              // setup PPIs 
              while ((ppi = (ComTdbExtPPI*)ppiList->getNext()) != NULL)
                {
                  OrcSearchArg arg;

                  Lng32 operIndex = ppi->operAttrIndex();
                  NABoolean searchArgAvailable = TRUE;

                   Lng32 dataLen = 0;
                   char * data = NULL;

                  if (operIndex >= 0)
                    {
                      Attributes * attr = extOperTD->getAttr(operIndex);
                      
                      NABoolean nullValue = FALSE;
                          
                      NABoolean isRS = 
                             ppi->type() == ExtPushdownOperatorType::IN &&
                             ppi->items_ == 1;

                      if ((attr->getNullFlag()) &&
                            (ExpTupleDesc::isNullValue
                             (&extOperRow_[attr->getNullIndOffset()],
                              attr->getNullBitIndex(),
                              attr->getTupleFormat())) && !isRS)
                          {
                            dataLen = 0;
                            data = NULL;
                          }
                      else
                          {
                            char * vcLenPtr = 
                              &extOperRow_[attr->getVCLenIndOffset()];
                            dataLen = attr->getLength(vcLenPtr);
                            data = &extOperRow_[attr->getOffset()];
                          }

                       if ( ppi->type() != ExtPushdownOperatorType::IN ) {

                          // regular PPI predicate (not IN). 
                          // Data length and the data

                          // operator type
                          arg.appendOperator(ppi->type()); 
                          arg.appendColName(ppi->colName());
                          arg.appendTypeIndicator(ppi->colType().data());
                          arg.appendValue(data, dataLen);

                       } else { // IN case


                          // Distinguish two forms of IN:
                          //    1. A regular list, when items_ >= 2
                          //    2. A range of values, when items_ == 1

                          if ( ppi->items_ >= 2 ) {

                             // Regular IN list
                           
                             // Items are strictly from the ppi. 
                             //
                             // First write out IN, column name and type
                             arg.appendOperator(ppi->type()); 
                             arg.appendColName(ppi->colName());
                             arg.appendTypeIndicator(ppi->colType().data());
                             arg.getText().append((char*)&(ppi->items_), sizeof(ppi->items_));

                             QRLogger::log(CAT_SQL_EXE_MINMAX, LL_DEBUG, "Items in an IN list:\n");

                             // Next process each data item 
                             for (Int32 i=0; i<ppi->items_; i++ ) {

                                arg.appendValue(data, dataLen);
                                if (data) {
                                  QRLogger::log(CAT_SQL_EXE_MINMAX, LL_DEBUG, "item=%.*s", dataLen, data);
                                }

                                // advance operIndex ptr to the next position 
                                // for attribute and the data, if we are not
                                // yet reach the last element in the IN list.
                                if ( i < ppi->items_ - 1 ) {
                                   attr = extOperTD->getAttr(++operIndex);

                                   char * vcLenPtr = 
                                     &extOperRow_[attr->getVCLenIndOffset()];
                                   dataLen = attr->getLength(vcLenPtr);
                                   data = &extOperRow_[attr->getOffset()];
                                }
                               }

                               QRLogger::log(CAT_SQL_EXE_MINMAX, LL_DEBUG, "\n");
                  
                            } else { // Range of values case

                             // For all RV cases, set searchArgAvailable to 
                             // FALSE so that we do not add arg to 
                             // extPPIvec_ when exit from this big ELSE block 
                             // of code.
                                             
                             // When RV in sink is empty, probably
                             // due to memory pressure. We will not
                             // alter the PPI structure like we
                             // do above when RS in sink is not empty.
                             // This means we will be just utilize the
                             // min/max expressions.
                             searchArgAvailable = FALSE;    

                             if ( canPushdownBloomFilters )
                             {
                               processBloomFilterData(ppi,
                                                      data, dataLen,
                                                      filterVec);
                             } 
                           } // end of rangeOfValues case
                       } // IN case
		    } // (operIndex >= 0)
                  else
                    {
                      // operand index == -1, no data from the row.
                      // Must output the correct colName for ISNULL.
                      arg.appendOperator(ppi->type()); 
                      arg.appendColName(ppi->colName());
                      arg.appendTypeIndicator(ppi->colType().data());
                      arg.appendValue(NULL, 0);

                    }

                  if ( searchArgAvailable ) {

#if 0
                   fstream& out = getPrintHandle();

                   out << "Push to extPPIvec_: ";
                   ppi->display(out);

                   out << ", dataLen=" << dataLen;
                   out << ", data=";
                   if ( data )
                    {
                      for (int i=0; i<dataLen; i++ ) {
                        out << data[i];
                      }
                    } else 
                      out << "NULL";
                   out << endl;
                   out.close();
#endif

                     extPPIvec_.push_back(arg.getText());
                  }
                }

                             
              if ( canPushdownBloomFilters ) {
                               
                  postProcessBloomFilters(filterVec);

              }
           
              step_ = SETUP_EXT_COL_INFO;
            }
          }
          break;

        case SETUP_EXT_COL_INFO:
          {
            if ((! extScanTdb().colNameList()) ||
                (! extScanTdb().colTypeList()))
              {
                step_ = INIT_EXT_CURSOR;
                break;
              }
            
            createColInfoVecs(extScanTdb().colNameList(),
                              extScanTdb().colTypeList(),
                              colNameVec_,
                              colTypeVec_);

            step_ = INIT_EXT_CURSOR;
          }
          break;

	case INIT_EXT_CURSOR:
	  {

#ifdef NEED_INSTRUMENT_EXTSCAN
            Timer x;
            x.start();
#endif
            int rangeNum;
            if (hdfsScanTdb().strawScanMode()){
              rangeNum = getAndInitNextSelectedRangeUsingStrawScan();
              if (rangeNum == -2){
            	  step_ = HANDLE_ERROR;
            	  break;
              }
            }
            else
              rangeNum = getAndInitNextSelectedRange(false,false);

#ifdef NEED_INSTRUMENT_EXTSCAN
            x.stop();
            Int64 et = x.elapsedTime();

            QRLogger::log(CAT_SQL_HDFS_EXT_SCAN_TCB,
                          LL_DEBUG,
                          "ExExtStorageScanTcb:work() selectRange: workTime=%s",
                          reportTimeDiff(et));
#endif

            if (rangeNum >= 0)
              {
                // move to the next range
                extStartRowNum_ = hdfo_->getStartRow();
                extNumRows_ = hdfo_->getNumRows();
                sprintf(cursorId_, "%d", currRangeNum_);

                extStopRowNum_ = extNumRows_; // the length of the stripe to access.
                                              // -1 to pick all rows

                step_ = OPEN_EXT_CURSOR;
              }
            else
              step_ = DONE;
          }
	  break;

	case OPEN_EXT_CURSOR:
	  {
            
            TextVec tv;
            if (extScanTdb().extAllColInfoList())
              {
                extScanTdb().extAllColInfoList()->position();
                char * cn = NULL;
                while ((cn = (char*)extScanTdb().extAllColInfoList()->getNext()) != NULL)
                  {
                    Text t(cn);
                    tv.push_back(t);
                  }
              }


            int expectedRowSize = (hdfsScanTdb().hdfsBufSize_ > (long)INT_MAX) ? 
                                  INT_MAX : hdfsScanTdb().hdfsBufSize_;


            Lng32 flags = 0;
            if (extScanTdb().isParquetFile())
              {
                if (extScanTdb().parquetLegacyTS())
                  flags |= PARQUET_LEGACY_TS;
                if (extScanTdb().parquetSignedMinMax())
                  flags |= PARQUET_SIGNED_MIN_MAX;

                // Arrow Parquet Reader specific flags
                if (extScanTdb().skipColumChunkStatsCheck())
                  flags |= 
                     ComTdbExtStorageScan::PARQUET_CPP_SKIP_COLUMN_STATS_CHECK;

                if (extScanTdb().filterRowgroups())
                  flags |= 
                     ComTdbExtStorageScan::PARQUET_CPP_FILTER_ROWGROUPS;

                if (extScanTdb().filterPages())
                  flags |= 
                     ComTdbExtStorageScan::PARQUET_CPP_FILTER_PAGES;
              }

#ifdef NEED_INSTRUMENT_EXTSCAN
            Int64 startT = currentTimeStampInUsec();
#endif
            //Int32 extRowBufferLen = hdfsAsciiSourceBuffer_->getTupleSize();

            Int32 scanParallelism 
                      = (extScanTdb()).parquetScanParallelism();

            Int32 numThreads = scanParallelism;

            if ( numCols_ > 0 &&
                 (scanParallelism <= 0 || scanParallelism > numCols_ ))
               numThreads = numCols_;
            
            retcode = exti_->scanOpen(hdfsStats_, 
                                      hdfsScanTdb().tableName(),
                                      hdfsFileName_,
                                      expectedRowSize,
                                      extScanTdb().extMaxRowsToFill_,
                                      extScanTdb().extMaxAllocationInMB_,
                                      extScanTdb().getVectorBatchSize(),
                                      extStartRowNum_, extStopRowNum_,
                                      numCols_, whichCols_,
                                      &extPPIvec_,
                                      (extScanTdb().extAllColInfoList() ? &tv : NULL),
                                      (colNameVec_.size() > 0 ? &colNameVec_ : NULL),
                                      (colTypeVec_.size() > 0 ? &colTypeVec_ : NULL),
                                      (expectedRowsVec_.size() > 0 ? &expectedRowsVec_: NULL),
                                      hdfsScanTdb().parqSchStr(),
                                      flags,
                                      numThreads);

#ifdef NEED_INSTRUMENT_EXTSCAN
            totalScanOpenTime_ = currentTimeStampInUsec() - startT;
            totalScanTime_ = currentTimeStampInUsec() - startT;

            QRLogger::log(CAT_SQL_HDFS_EXT_SCAN_TCB,
                          LL_DEBUG,
                          "ExExtStorageScanTcb:work() scanOpen: file=%s, workTime=%s",
                          hdfsFileName_, reportTimeDiff(totalScanTime_));

            totalRows_ = 0;
#endif
                                      
            if (retcode < 0)
              {
		ContextCli *currContext = GetCliGlobals()->currContext();
                setupError(EXE_ERROR_FROM_LOB_INTERFACE, 
			   retcode,
			   "EXT scanOpen",
			   exti_->getErrorText(-retcode),
			   GetCliGlobals()->getJniErrorStr());

                step_ = HANDLE_ERROR;
                break;
              }

            if (extScanTdb().orcVectorizedScan() ||
                (extScanTdb().useArrowReader() && 
                 extScanTdb().directCopy())
               )
               step_ = GET_NEXT_ROW_IN_VECTOR; 
            else
	       step_ = GET_EXT_ROW;
	  }
          break;
        case GET_NEXT_ROW_IN_VECTOR:
          {
            retcode = exti_->nextRow();
            if (retcode < 0)
              {
		ContextCli *currContext = GetCliGlobals()->currContext();
                setupError(EXE_ERROR_FROM_LOB_INTERFACE, 
			   retcode, 
			   "EXT nextRow", 
			   exti_->getErrorText(-retcode),
			   GetCliGlobals()->getJniErrorStr());
                step_ = CLOSE_EXT_CURSOR_AND_ERROR;
                break;
              }
            if (retcode == 100)
              {
                step_ = CLOSE_EXT_CURSOR;
                break;
              }
            step_ = PROCESS_NEXT_ROW_IN_VECTOR;
          }
          break;  
        case PROCESS_NEXT_ROW_IN_VECTOR:
          {
	     ComDiagsArea *diags = NULL;
          
             Lng32 totalRowLen = 0;
             retcode = createSQRowFromVectors(diags, totalRowLen);

             if (retcode < 0)
              {
		ContextCli *currContext = GetCliGlobals()->currContext();
                setupError(EXE_ERROR_FROM_LOB_INTERFACE, 
			   retcode, 
			   "createSQRowFromVectors", 
			   exti_->getErrorText(-retcode),
			   GetCliGlobals()->getJniErrorStr());
                step_ = CLOSE_EXT_CURSOR_AND_ERROR;
                break;
              }

              exti_->doneNextRow(totalRowLen);

              step_ = APPLY_SELECT_PRED;
          }
          break;
        case GET_EXT_ROW:
          {
            extRow_ = 0;
            extRowLen_ = 0;
#ifdef NEED_INSTRUMENT_EXTSCAN
            Int64 startT = currentTimeStampInUsec();
#endif

            retcode = exti_->scanFetch(&extRow_,  // IN and OUT
	   			       extRowLen_, // out
	   			       extRowNum_, // out
	   			       numExtCols_, // out
				       hdfsStats_
				       );

#ifdef NEED_INSTRUMENT_EXTSCAN
            totalScanTime_ += currentTimeStampInUsec() - startT;
            totalScanFetchTime_ += currentTimeStampInUsec() - startT;
            scanFetchCalls_++;
#endif

            if (retcode < 0)
              {
		ContextCli *currContext = GetCliGlobals()->currContext();
                setupError(EXE_ERROR_FROM_LOB_INTERFACE, 
			   retcode, 
			   "EXT scanFetch", 
			   exti_->getErrorText(-retcode),
			   GetCliGlobals()->getJniErrorStr());
                step_ = CLOSE_EXT_CURSOR_AND_ERROR;
                break;
              }
            
            if (retcode == 100)
              {
                step_ = CLOSE_EXT_CURSOR;
                break;
              }

            step_ = PROCESS_EXT_ROW;
          }
          break;
          
	case PROCESS_EXT_ROW:
	  {
	    int formattedRowLength = 0;
	    ComDiagsArea *transformDiags = NULL;
            short err = 
	      extractAndTransformExtSourceToSqlRow(extRow_, extRowLen_,
                                                   numExtCols_, transformDiags);
            
	    if (err)
	      {
     		   if (transformDiags)
		          pentry_down->setDiagsArea(transformDiags);
           step_ = CLOSE_EXT_CURSOR_AND_ERROR;
		       break;
	      }	    
	    
            step_ = APPLY_SELECT_PRED;
    }
    break;
  case APPLY_SELECT_PRED:
          {
            if (virtColData_)
            {
              // for EXT, we don't have actual offsets inside the file or
              // block, just increment the offset by 1 for every row
              virtColData_->block_offset_inside_file++;
              virtColData_->row_number_in_range++;
            }

	    bool rowWillBeSelected = true;
	    if (selectPred())
	      {
		ex_expr::exp_return_type evalRetCode =
		  selectPred()->eval(pentry_down->getAtp(), workAtp_);
		if (evalRetCode == ex_expr::EXPR_FALSE)
		  rowWillBeSelected = false;
		else if (evalRetCode == ex_expr::EXPR_ERROR)
		  {
        step_ = CLOSE_EXT_CURSOR_AND_ERROR;
		    break;
		  }
		else 
		  ex_assert(evalRetCode == ex_expr::EXPR_TRUE,
			    "invalid return code from expr eval");
	      }
	    
	    if (rowWillBeSelected)
	      {
                if (moveColsConvertExpr())
                  {
                    ex_expr::exp_return_type evalRetCode =
                      moveColsConvertExpr()->eval(workAtp_, workAtp_);
                    if (evalRetCode == ex_expr::EXPR_ERROR)
                      {
                        step_ = CLOSE_EXT_CURSOR_AND_ERROR;
                        break;
                      }
                  }
		if (hdfsStats_)
		  hdfsStats_->incUsedRows();

		step_ = RETURN_ROW;
		break;
	      }
            if ( extScanTdb().orcVectorizedScan() ||
                (extScanTdb().useArrowReader() && 
                 extScanTdb().directCopy())
               )
               step_ = GET_NEXT_ROW_IN_VECTOR; 
            else
	       step_ = GET_EXT_ROW;
          }
          break;

	case RETURN_ROW:
	  {
	    if (qparent_.up->isFull()) {

#ifdef NEED_INSTRUMENT_EXTSCAN
              workTimer_.stop();

#ifdef NEED_INSTRUMENT_EXTSCAN_UPQUEUE
              isFull_ ++;

            Int64 et = workTimer_.elapsedTime();
            QRLogger::log(CAT_SQL_HDFS_EXT_SCAN_TCB,
                          LL_DEBUG,
                          "ExExtStorageScanTcb:work() upQ full: workTime=%s, rows=%ld, finalReturns=%ld, poolBlocked=%ld, isFull=%ld, done=%ld, errors=%ld",
                          reportTimeDiff(et),
                          rows_, finalReturns_, poolBlocked_, isFull_, done_, handle_errors_
                        );
#endif

#endif
	      return WORK_OK;
            }

#ifdef NEED_INSTRUMENT_EXTSCAN
            rows_++;
            totalRows_++;
#endif
	    
	    ex_queue_entry *up_entry = qparent_.up->getTailEntry();
	    up_entry->copyAtp(pentry_down);
	    up_entry->upState.parentIndex = 
	      pentry_down->downState.parentIndex;
	    up_entry->upState.downIndex = qparent_.down->getHeadIndex();
	    up_entry->upState.status = ex_queue::Q_OK_MMORE;
	    
	    if (moveExpr())
	      {
	        UInt32 maxRowLen = extScanTdb().outputRowLength_;
	        UInt32 rowLen = maxRowLen;
                
                if (extScanTdb().useCifDefrag() &&
                    !pool_->currentBufferHasEnoughSpace((Lng32)extScanTdb().outputRowLength_))
                  {
                    up_entry->getTupp(extScanTdb().tuppIndex_) = defragTd_;
                    defragTd_->setReferenceCount(1);
                    ex_expr::exp_return_type evalRetCode =
                      moveExpr()->eval(up_entry->getAtp(), workAtp_,0,-1,&rowLen);
                    if (evalRetCode ==  ex_expr::EXPR_ERROR)
                      {
                        // Get diags from up_entry onto pentry_down, which
                        // is where the HANDLE_ERROR step expects it.
                        ComDiagsArea *diagsArea = pentry_down->getDiagsArea();
                        if (diagsArea == NULL)
                          {
                            diagsArea =
                              ComDiagsArea::allocate(getGlobals()->getDefaultHeap());
                            pentry_down->setDiagsArea (diagsArea);
                          }
                        pentry_down->getDiagsArea()->
                          mergeAfter(*up_entry->getDiagsArea());
                        up_entry->setDiagsArea(NULL);
                        step_ = CLOSE_EXT_CURSOR_AND_ERROR;
                        break;
                      }
                    if (pool_->get_free_tuple(
                                              up_entry->getTupp(extScanTdb().tuppIndex_),
                                              rowLen)) {
#ifdef NEED_INSTRUMENT_EXTSCAN
                      workTimer_.stop();
                      poolBlocked_ ++;
#endif

                      return WORK_POOL_BLOCKED;
                    }

                    str_cpy_all(up_entry->getTupp(extScanTdb().tuppIndex_).getDataPointer(),
                                defragTd_->getTupleAddress(),
                                rowLen);
                    
                  }
                else
                  {
                    if (pool_->get_free_tuple(
                                              up_entry->getTupp(extScanTdb().tuppIndex_),
                                              (Lng32)extScanTdb().outputRowLength_)) {
#ifdef NEED_INSTRUMENT_EXTSCAN
                      workTimer_.stop();
                      poolBlocked_ ++;
#endif

                      return WORK_POOL_BLOCKED;
                    } 
                    ex_expr::exp_return_type evalRetCode =
                      moveExpr()->eval(up_entry->getAtp(), workAtp_,0,-1,&rowLen);
                    if (evalRetCode ==  ex_expr::EXPR_ERROR)
                      {
                        // Get diags from up_entry onto pentry_down, which
                        // is where the HANDLE_ERROR step expects it.
                        ComDiagsArea *diagsArea = pentry_down->getDiagsArea();
                        if (diagsArea == NULL)
                          {
                            diagsArea =
                              ComDiagsArea::allocate(getGlobals()->getDefaultHeap());
                            pentry_down->setDiagsArea (diagsArea);
                          }
                        pentry_down->getDiagsArea()->
                          mergeAfter(*up_entry->getDiagsArea());
                        up_entry->setDiagsArea(NULL);
                        step_ = CLOSE_EXT_CURSOR_AND_ERROR;
                        break;
                      }
                    if (extScanTdb().useCif() && rowLen != maxRowLen)
                      {
                        pool_->resizeLastTuple(rowLen,
                                               up_entry->getTupp(extScanTdb().tuppIndex_).getDataPointer());
                      }
                  }
	      }
	    
	    up_entry->upState.setMatchNo(++matches_);
	    qparent_.up->insert();
	    
	    // use ExOperStats now, to cover OPERATOR stats as well as 
	    // ALL stats. 
	    if (getStatsEntry())
	      getStatsEntry()->incActualRowsReturned();
	    
	    workAtp_->setDiagsArea(NULL);    // get rid of warnings.
	    
	    if ((pentry_down->downState.request == ex_queue::GET_N) &&
		(pentry_down->downState.requestValue == matches_))
	      step_ = CLOSE_EXT_CURSOR_AND_DONE;
	    else {
               if (extScanTdb().orcVectorizedScan() ||
                   (extScanTdb().useArrowReader() && 
                    extScanTdb().directCopy())
                  )
                  step_ = GET_NEXT_ROW_IN_VECTOR; 
               else
	          step_ = GET_EXT_ROW;
            }
	    break;
	  }

        case CLOSE_EXT_CURSOR:
        case CLOSE_EXT_CURSOR_AND_DONE:
        case CLOSE_EXT_CURSOR_AND_ERROR:
          {
#ifdef NEED_INSTRUMENT_EXTSCAN
            Int64 startT = currentTimeStampInUsec();
#endif

            retcode = exti_->scanClose();

#ifdef NEED_INSTRUMENT_EXTSCAN
            workTimer_.stop();

            totalScanTime_ += currentTimeStampInUsec() - startT;
            totalScanCloseTime_ += currentTimeStampInUsec() - startT;

            isFull_ ++;

            QRLogger::log(CAT_SQL_HDFS_EXT_SCAN_TCB,
                          LL_DEBUG,
                          "ExExtStorageScanTcb:work() scanClose: file=%s, totalRows=%ld, rows=%ld, finalReturns=%ld, poolBlocked=%ld, isFull=%ld, done=%ld, errors=%ld",
                          hdfsFileName_, 
                          totalRows_, rows_, 
                          finalReturns_, poolBlocked_, 
                          isFull_, done_, handle_errors_
                        );

            char totalScanTimeStr[200];
            char scanOpenTimeStr[200];
            char scanFetchTimeStr[200];
            char scanCloseTimeStr[200];

            snprintf(totalScanTimeStr, sizeof(totalScanTimeStr), "%s", reportTimeDiff(totalScanTime_));
            snprintf(scanOpenTimeStr, sizeof(scanOpenTimeStr), "%s", reportTimeDiff(totalScanOpenTime_));
            snprintf(scanFetchTimeStr, sizeof(scanFetchTimeStr), "%s", reportTimeDiff(totalScanFetchTime_));
            snprintf(scanCloseTimeStr, sizeof(scanCloseTimeStr), "%s", reportTimeDiff(totalScanCloseTime_));

            QRLogger::log(CAT_SQL_HDFS_EXT_SCAN_TCB,
                          LL_DEBUG,
                          "Total time breakdown for file %s: total=%s, open=%s, fetch=%s, close=%s",
                          hdfsFileName_, 
                          totalScanTimeStr,
                          scanOpenTimeStr,
                          scanFetchTimeStr,
                          scanCloseTimeStr
                        );
#endif
            if (retcode < 0)
              {
		ContextCli *currContext = GetCliGlobals()->currContext();
                setupError(EXE_ERROR_FROM_LOB_INTERFACE,
			   retcode, 
			   "EXT scanClose", 
                           exti_->getErrorText(-retcode),
			   GetCliGlobals()->getJniErrorStr());
                
                step_ = HANDLE_ERROR;
                break;
              }
            if (step_ == CLOSE_EXT_CURSOR_AND_ERROR) 
              {
                 step_ = HANDLE_ERROR;
                 break;
              }              
            if (step_ == CLOSE_EXT_CURSOR_AND_DONE)
              {
                step_ = DONE;
                break;
              }

            // move to the next file.
            currRangeNum_++;
            step_ = INIT_EXT_CURSOR;
            break;
          }
          break;
          
	case HANDLE_ERROR:
	  {
	    if (handleError(rc, (workAtp_ ? workAtp_->getDiagsArea() : NULL))) {

#ifdef NEED_INSTRUMENT_EXTSCAN
              workTimer_.stop();
              handle_errors_ ++;
#endif
	      return rc;
            }

	    step_ = DONE;
	  }
          break;

	case DONE:
	  {
#ifdef NEED_INSTRUMENT_EXTSCAN
            Int64 et = workTimer_.elapsedTime();

            QRLogger::log(CAT_SQL_HDFS_EXT_SCAN_TCB,
                          LL_DEBUG,
                          "ExExtStorageScanTcb:work() DONE: workTime=%s, rows=%ld, finalReturns=%ld, poolBlocked=%ld, isFull=%ld, done=%ld, errors=%ld",
                          reportTimeDiff(et),
                          rows_, finalReturns_, poolBlocked_, isFull_, done_, handle_errors_
                        );
#endif

	    if (handleDone(rc)) {

#ifdef NEED_INSTRUMENT_EXTSCAN
              workTimer_.stop();
              done_ ++;
#endif

	      return rc;
            }

	    step_ = NOT_STARTED;
	  }
          break;
	  
	default: 
	  {
	    break;
	  }
        } // switch
      
    } // while

#ifdef NEED_INSTRUMENT_EXTSCAN
  workTimer_.stop();
  finalReturns_ ++;
#endif
              
  return WORK_OK;
}

int ExExtStorageScanTcb::getAndInitNextSelectedRangeUsingStrawScan()
{
  HdfsFileInfo* hdfo = NULL;
  Lng32 rangeNum ;//call the func
  bool rangeIsSelected = false;

  Lng32 retcode;
  retcode = exti_->getNextRangeNumStrawScan(hdfsScanTdb().strawscanWebServer(),queryId_,explainNodeId_,sequenceNb_++, executionNb_,myInstNum_,nodeNumber_, true, rangeNum);
  if (retcode < 0)
  {
      ContextCli *currContext = GetCliGlobals()->currContext();
      setupError(EXE_ERROR_FROM_LOB_INTERFACE,
  	             retcode,
                 "EXT getAndInitNextSelectedRangeUsingStrawScan",
                 exti_->getErrorText(-retcode),
				 GetCliGlobals()->getJniErrorStr());
      return -2;
  }

  QRLogger::log(CAT_SQL_HDFS_SCAN,
                LL_DEBUG,
                "ExExtStorageScanTcb::getAndInitNextSelectedRangeUsingStrawScan() range list entries=%d,  range=%d.",
                getHdfsFileInfoListAsArray().entries(),
                rangeNum
                );

  while (!rangeIsSelected && rangeNum >= 0) //rangeNum will be -1 on end of ranges
    {
      hdfo = getRange(rangeNum);

      // eliminate empty ranges (e.g. sqoop-generated files)
      if (hdfo->getBytesToRead() > 0)
        {
          // initialize partition and virtual column with the
          // values for this range and see whether it qualifies

         initPartAndVirtColData(hdfo, rangeNum, false);

          if (partElimExpr())
            {
              // Try to eliminate the entire range based on the
              // partition elimination expression. Note that we call
              // it partition elimination expression, but we will
              // actually call it for each range, and it can
              // reference the INPUT__RANGE__NUMBER virtual column,
              // which is specific to the range.
              ex_expr::exp_return_type evalRetCode =
                partElimExpr()->eval(qparent_.down->getHeadEntry()->getAtp(),
                                     workAtp_);
              if (evalRetCode == ex_expr::EXPR_TRUE)
                rangeIsSelected = true;
            }
          else
            rangeIsSelected = true;
        }

      if (!rangeIsSelected){
          retcode = exti_->getNextRangeNumStrawScan(hdfsScanTdb().strawscanWebServer(),queryId_,explainNodeId_,sequenceNb_++, executionNb_,myInstNum_,nodeNumber_, true, rangeNum);
          if (retcode < 0)
          {
              ContextCli *currContext = GetCliGlobals()->currContext();
              setupError(EXE_ERROR_FROM_LOB_INTERFACE,
          	             retcode,
                         "EXT getAndInitNextSelectedRangeUsingStrawScan",
                         exti_->getErrorText(-retcode),
						 GetCliGlobals()->getJniErrorStr());
              return -2;
          }
      }
    }

    if (rangeIsSelected)
      {
        // prepare the TDB to scan this new, selected, range
        currRangeNum_ = rangeNum;
        hdfo_         = hdfo;
        hdfsFileName_ = hdfo_->fileName();
        hdfsOffset_   = hdfo_->getStartOffset();
        bytesLeft_    = hdfo_->getBytesToRead();
        // part and virt columns have already been set above
      }

    if (rangeIsSelected)
      return rangeNum;
    else
      return -1;
}

Lng32 ExExtStorageScanTcb::createSQRowFromVectors(ComDiagsArea *&diagsArea, Lng32& totalRowLen)
{
   Lng32 err = 0;

   totalRowLen = 0;

   ExpTupleDesc * asciiSourceTD =
     extScanTdb().workCriDesc_->getTupleDescriptor(extScanTdb().asciiTuppIndex_);
   if (asciiSourceTD->numAttrs() == 0) {
      // no columns need to be converted. For e.g. count(*) with no predicate
      return 0;
   }
  
   Attributes * attr = NULL;
   Lng32 currColLen;
   short nullVal;
   Int16 datetimeCode;
   char tempStr[101];
   bool convFromStr;
   Lng32 tempStrLen = sizeof(tempStr)-1;

   
   for (Lng32 colNo = 0; colNo < asciiSourceTD->numAttrs(); colNo++) {
      attr = asciiSourceTD->getAttr(colNo);
      currColLen = attr->getLength();
      if (attr->getVCIndicatorLength() > 0) {
          if ((err = exti_->getBytesCol(colNo, (BYTE *)&hdfsAsciiSourceData_[attr->getOffset()], currColLen, nullVal)) != 0)
             return err;
          if (nullVal == 0) {
             char * vcLenLoc = &hdfsAsciiSourceData_[attr->getVCLenIndOffset()];
             attr->setVarLength(currColLen, vcLenLoc);
          }
      }
      else if (DFS2REC::isSQLFixedChar(attr->getDatatype())) {
          if ((err = exti_->getBytesCol(colNo, (BYTE *)&hdfsAsciiSourceData_[attr->getOffset()], currColLen, nullVal)) != 0)
             return err;
          // copy space for the remaining column width
          if (nullVal == 0) {
             Lng32 remainLen = attr->getLength() - currColLen;
             if (remainLen > 0)
                 memset((BYTE *)&hdfsAsciiSourceData_[attr->getOffset()]+currColLen, ' ', remainLen); 
          }
      }
      else if (attr->isNumericWithPrecision()) {    
         switch (attr->getDatatype()) {
            case REC_BIN16_SIGNED:
            case REC_BIN32_SIGNED:
            case REC_BIN64_SIGNED:
            case REC_NUM_BIG_SIGNED:
               tempStrLen = sizeof(tempStr)-1;
               if ((err = exti_->getDecimalCol(colNo, attr->getDatatype(), (BYTE *)&hdfsAsciiSourceData_[attr->getOffset()], 
                      currColLen, nullVal, tempStr, tempStrLen, convFromStr)) != 0)
                  return err;
               if (nullVal == 0 && convFromStr) {
                  ex_expr::exp_return_type rc = convDoIt((char*)&tempStr, tempStrLen, REC_BYTE_F_ASCII, 0, 0, 
                                   (char*)&hdfsAsciiSourceData_[attr->getOffset()],
                                   attr->getLength(), attr->getDatatype(), 
                                   attr->getPrecision(), attr->getScale(),
                                   NULL, 0, getHeap(), &diagsArea,
                                   CONV_ASCII_BIGNUM, NULL, 0);
                  if (rc == ex_expr::EXPR_ERROR)
                    return -1;
               }
               break;
            default:
               return -1;
         }
      } else {
         switch (attr->getDatatype()) {
            case REC_BIN8_SIGNED:
            case REC_BIN16_SIGNED:
            case REC_BIN32_SIGNED:
            case REC_BIN64_SIGNED:
            case REC_BOOLEAN:
               if ((err = exti_->getNumericCol(colNo, attr->getDatatype(), (BYTE *)&hdfsAsciiSourceData_[attr->getOffset()], 
                      currColLen, nullVal)) != 0)
                  return err;
               break; 
            case REC_FLOAT32:
            case REC_FLOAT64:
               if ((err = exti_->getDoubleOrFloatCol(colNo, attr->getDatatype(), (BYTE *)&hdfsAsciiSourceData_[attr->getOffset()], 
                      currColLen, nullVal)) != 0)
                  return err;
               break; 
            case REC_DATETIME:
               datetimeCode = (attr->getLength() == 4 ? REC_DTCODE_DATE : REC_DTCODE_TIMESTAMP);
               if ((err = exti_->getDateOrTimestampCol(colNo, datetimeCode, (BYTE *)&hdfsAsciiSourceData_[attr->getOffset()], 
                      currColLen, nullVal)) != 0)
                  return err;
               break; 
            default:
               return -1; 
         }
      }
      if (attr->getNullFlag()) {
          if (nullVal == -1)
            *(short *)&hdfsAsciiSourceData_[attr->getNullIndOffset()] = -1;
          else
            *(short *)&hdfsAsciiSourceData_[attr->getNullIndOffset()] = 0;
      }
      else if (nullVal == -1)
         return -1;        

      totalRowLen += currColLen;
   }

   err = 0;

   workAtp_->getTupp(extScanTdb().workAtpIndex_) = hdfsSqlTupp_;
   workAtp_->getTupp(extScanTdb().asciiTuppIndex_) = hdfsAsciiSourceTupp_;
   workAtp_->getTupp(extScanTdb().moveExprColsTuppIndex_) = moveExprColsTupp_;

   if (convertExpr()) {
      ex_expr::exp_return_type evalRetCode =
        convertExpr()->eval(workAtp_, workAtp_);
      if (evalRetCode == ex_expr::EXPR_ERROR)
         err = -1;
      else
         err = 0;
   }
   return err;
}

ExExtStorageFastAggrTcb::ExExtStorageFastAggrTcb(
          const ComTdbExtStorageFastAggr &extAggrTdb, 
          ex_globals * glob ) :
  ExExtStorageScanTcb(extAggrTdb, glob),
  step_(NOT_STARTED)
{
  Space * space = (glob ? glob->getSpace() : 0);
  CollHeap * heap = (glob ? glob->getDefaultHeap() : 0);

  outputRow_ = NULL;
  extAggrRow_ = NULL;
  finalAggrRow_ = NULL;
  if (extAggrTdb.outputRowLength_ > 0)
    outputRow_ = new(glob->getDefaultHeap()) char[extAggrTdb.outputRowLength_];

  if (extAggrTdb.extAggrRowLength_ > 0)
    extAggrRow_ = new(glob->getDefaultHeap()) char[extAggrTdb.extAggrRowLength_];

  if (extAggrTdb.finalAggrRowLength_ > 0)
    finalAggrRow_ = new(glob->getDefaultHeap()) char[extAggrTdb.finalAggrRowLength_];

  pool_->get_free_tuple(workAtp_->getTupp(extAggrTdb.extAggrTuppIndex_), 0);
  workAtp_->getTupp(extAggrTdb.extAggrTuppIndex_)
    .setDataPointer(extAggrRow_);

  pool_->get_free_tuple(workAtp_->getTupp(extAggrTdb.workAtpIndex_), 0);
  workAtp_->getTupp(extAggrTdb.workAtpIndex_)
    .setDataPointer(finalAggrRow_);

  pool_->get_free_tuple(workAtp_->getTupp(extAggrTdb.moveExprColsTuppIndex_), 0);
  workAtp_->getTupp(extAggrTdb.moveExprColsTuppIndex_)
    .setDataPointer(outputRow_);

  // fixup expressions
  if (aggrExpr())
    aggrExpr()->fixup(0, getExpressionMode(), this,  space, heap, FALSE, glob);

  colStats_ = NULL;
}

ExExtStorageFastAggrTcb::~ExExtStorageFastAggrTcb()
{
}

ExWorkProcRetcode ExExtStorageFastAggrTcb::work()
{
  Lng32 retcode = 0;
  short rc = 0;

  while (!qparent_.down->isEmpty())
    {
      ex_queue_entry *pentry_down = qparent_.down->getHeadEntry();
      if (pentry_down->downState.request == ex_queue::GET_NOMORE)
	step_ = DONE;

      switch (step_)
	{
	case NOT_STARTED:
	  {
	    matches_ = 0;

	    hdfsStats_ = NULL;
	    if (getStatsEntry())
	      hdfsStats_ = getStatsEntry()->castToExHdfsScanStats();
            
	    //ex_assert(hdfsStats_, "hdfs stats cannot be null");

            extAggrTdb().getHdfsFileInfoList()->position();

            rowCount_ = 0;

            if (aggrExpr()->initializeAggr(workAtp_) ==
                ex_expr::EXPR_ERROR)
              {
                step_ = HANDLE_ERROR;
                break;
              }

	    beginRangeNum_ = -1;
	    numRanges_ = -1;

	    myInstNum_ = getGlobals()->getMyInstanceNumber();

	    if ( extAggrTdb().getHdfsFileRangeBeginList()->get(myInstNum_) )
	      beginRangeNum_ =  
	         *(Lng32*)extAggrTdb().getHdfsFileRangeBeginList()->get(myInstNum_);

	    if ( extAggrTdb().getHdfsFileRangeNumList()->get(myInstNum_) )
	      numRanges_ =  
	        *(Lng32*)extAggrTdb().getHdfsFileRangeNumList()->get(myInstNum_);

	    currRangeNum_ = beginRangeNum_;

            step_ = EXT_AGGR_INIT;
	  }
	  break;

	case EXT_AGGR_INIT:
	  {
            if ((extAggrTdb().getHdfsFileInfoList()->atEnd()) ||
                (currRangeNum_ >= (beginRangeNum_ + numRanges_)))
              {
                step_ = EXT_AGGR_HAVING_PRED;
                break;
              }

            // select a range to fetch stats from
            int rangeNum = getAndInitNextSelectedRange(false, false);

            extStartRowNum_ = hdfo_->getStartRow();
            extNumRows_ = hdfo_->getNumRows();
            
            hdfsFileName_ = hdfo_->fileName();

            sprintf(cursorId_, "%d", currRangeNum_);

            extStopRowNum_ = extNumRows_; // the length of the stripe to access.
                                          // -1 to access all rows.

            extAggrTdb().getAggrTypeList()->position();
            extAggrTdb().getHdfsColInfoList()->position();

            aggrNum_ = -1;

	    step_ = EXT_AGGR_OPEN;
	  }
	  break;

        case EXT_AGGR_OPEN:
          {
            retcode = exti_->open(NULL, NULL, hdfsFileName_, 0, 0, 0, 0,
                                  extStartRowNum_, extStopRowNum_);
            if (retcode == -OFR_ERROR_FILE_NOT_FOUND_EXCEPTION)
              {
                // file doesnt exist. Treat as no rows found.
                // Move to next file.
                step_ = EXT_AGGR_INIT;
                break;
              }

            if (retcode == -OFR_ERROR_GSS_EXCEPTION)
              {
                ContextCli *currContext = GetCliGlobals()->currContext();
                setupError(EXE_GSS_EXCEPTION_ERROR, retcode, 
                           "EXT open", 
                           exti_->getErrorText(-retcode),
			   GetCliGlobals()->getJniErrorStr());

                step_ = HANDLE_ERROR;
                break;
              }
            if (retcode < 0)
              {
		ContextCli *currContext = GetCliGlobals()->currContext();
                setupError(EXE_ERROR_FROM_LOB_INTERFACE, 
			   retcode, 
                           "EXT open", 
                           exti_->getErrorText(-retcode),
			   GetCliGlobals()->getJniErrorStr());

                step_ = HANDLE_ERROR;
                break;
              }

            step_ = EXT_AGGR_NEXT;
          }
          break;

        case EXT_AGGR_NEXT:
          {
            if (extAggrTdb().getAggrTypeList()->atEnd())
              {
                step_ = EXT_AGGR_CLOSE;
                break;
              }

            aggrNum_++;

            aggrType_ = 
              *(ComTdbExtStorageFastAggr::ExtStorageAggrType*)extAggrTdb()
              .getAggrTypeList()->getNext();

            HdfsColInfo * hco = 
              (HdfsColInfo*)extAggrTdb().getHdfsColInfoList()->getNext();
            colNum_  = hco->colNumber();
            colName_ = hco->colName();
            colType_ = hco->colType();

            step_ = EXT_AGGR_EVAL;
          }
          break;

        case EXT_AGGR_EVAL:
          {
            if (aggrType_ == ComTdbExtStorageFastAggr::COUNT_)
              step_ = EXT_AGGR_COUNT;
            else if (aggrType_ == ComTdbExtStorageFastAggr::COUNT_NONULL_)
              step_ = EXT_AGGR_COUNT_NONULL;
            else if (aggrType_ == ComTdbExtStorageFastAggr::MIN_)
              step_ = EXT_AGGR_MIN;
            else if (aggrType_ == ComTdbExtStorageFastAggr::MAX_)
              step_ = EXT_AGGR_MAX;
            else if (aggrType_ == ComTdbExtStorageFastAggr::SUM_)
              step_ = EXT_AGGR_SUM;
            else if (aggrType_ == ComTdbExtStorageFastAggr::ORC_NV_LOWER_BOUND_)
              step_ = ORC_AGGR_NV_LOWER_BOUND;
            else if (aggrType_ == ComTdbExtStorageFastAggr::ORC_NV_UPPER_BOUND_)
              step_ = ORC_AGGR_NV_UPPER_BOUND;
            else
              step_ = HANDLE_ERROR;
          }
          break;

        case EXT_AGGR_COUNT:
        case EXT_AGGR_COUNT_NONULL:
	case EXT_AGGR_MIN:
	case EXT_AGGR_MAX:
	case EXT_AGGR_SUM:
        case ORC_AGGR_NV_LOWER_BOUND:
        case ORC_AGGR_NV_UPPER_BOUND:
	  {
            if ((extAggrTdb().isOrcFile()) &&
                (aggrType_ == ComTdbExtStorageFastAggr::COUNT_NONULL_))
              {
                // should not reach here. Generator should not have chosen
                // this option.
                // ORC either returns count of all rows or count of column
                // values after removing null and duplicate values.
                // It doesn't return a count with only null values removed.
                step_ = HANDLE_ERROR;
                break;
              }

            retcode = exti_->getColStats(colNum_, colName_, colType_,
                                         &colStats_);
            if (retcode < 0)
              {
		ContextCli *currContext = GetCliGlobals()->currContext();
                setupError(EXE_ERROR_FROM_LOB_INTERFACE,
			   retcode, 
                           "EXT getColStats", 
                           exti_->getErrorText(-retcode),
			   GetCliGlobals()->getJniErrorStr());

                step_ = HANDLE_ERROR;
                break;
              }

	    ExpTupleDesc * extAggrTuppTD =
	      extAggrTdb().workCriDesc_->getTupleDescriptor
	      (extAggrTdb().extAggrTuppIndex_);
            
	    Attributes * attr = extAggrTuppTD->getAttr(aggrNum_);
	    if (! attr)
	      {
		step_ = HANDLE_ERROR;
		break;
	      }

            char * extAggrLoc = &extAggrRow_[attr->getOffset()];

            if (attr->getNullFlag())
              {
                char * nullLoc = &extAggrRow_[attr->getNullIndOffset()];
                *(short*)nullLoc = 0;
              }

            Int32 len = 0;
            HbaseStr *hbaseStr;

            // Refactoring the code a bit here. 
            // The ORC column stats reader in Java sets the number of values to 0 
            // if the requested column does not exist in the current ORC data file.
            // num vals (incl dups and nulls)
            long numVals = -1;
            hbaseStr = &colStats_->at(0);
            ex_assert(hbaseStr->len <= sizeof(numVals), "Insufficent length");
            memcpy(&numVals, hbaseStr->val, hbaseStr->len);

            if ((step_ == EXT_AGGR_COUNT) || (numVals == 0)) {
              memcpy(extAggrLoc, hbaseStr->val, hbaseStr->len);
              len = hbaseStr->len;
              step_ = EXT_AGGR_NEXT;
              break;
            }	      

            if (step_ == ORC_AGGR_NV_LOWER_BOUND || step_ == ORC_AGGR_NV_UPPER_BOUND)
              {
                // number of values (excluding dups and nulls)
                hbaseStr = &colStats_->at(2);
                ex_assert(hbaseStr->len <= attr->getLength(), "Insufficent extAggrLoc length");
                memcpy(extAggrLoc, hbaseStr->val, hbaseStr->len);
                len = hbaseStr->len;
                step_ = EXT_AGGR_NEXT;
                break;
              }

            // aggr type
            int type = -1;
            hbaseStr = &colStats_->at(1);
            ex_assert(hbaseStr->len <= sizeof(type), "Insufficient length");
            memcpy(&type, hbaseStr->val, hbaseStr->len);

             // num uniq vals (excl dups and nulls)
            long numUniqVals = -1;
            hbaseStr = &colStats_->at(2);
            ex_assert(hbaseStr->len <= sizeof(numUniqVals), "Insufficient length");
            memcpy(&numUniqVals, hbaseStr->val, hbaseStr->len);

            if (numUniqVals == 0)
              {
                if (attr->getNullFlag())
                  {
                    char * nullLoc = &extAggrRow_[attr->getNullIndOffset()];
                    *(short*)nullLoc = -1;
                  }
              }
            else
              {
                memset(extAggrLoc, ' ', attr->getLength());
                if (step_ == EXT_AGGR_MIN) 
                  {
                    hbaseStr = &colStats_->at(3);
                  }
                else if (step_ == EXT_AGGR_MAX)
                  {
                    hbaseStr = &colStats_->at(4);
                  }
                else if (step_ == EXT_AGGR_SUM)
                  {
                    hbaseStr = &colStats_->at(5);
                  }
                
                if (type == HIVE_DECIMAL_TYPE) 
                  {
                    // EXT decimal vals are accessed in traf as NUMERIC datatypes.
                    // These values are returned from java layer in 'string' format.
                    // Convert them from string to expected datatype.
                    ComDiagsArea *convDiagsArea = NULL;
                    ex_expr::exp_return_type rc =
                      convDoIt(hbaseStr->val, hbaseStr->len, REC_BYTE_F_ASCII, 0, 0,
                               extAggrLoc, attr->getLength(), attr->getDatatype(),
                               attr->getPrecision(), attr->getScale(),
                               NULL, 0, getHeap(), &convDiagsArea,
                               CONV_UNKNOWN, NULL, 0);
                    if (rc == ex_expr::EXPR_ERROR)
                      {
                        if (convDiagsArea)
                          pentry_down->setDiagsArea(convDiagsArea);
                        
                        step_ = HANDLE_ERROR;
                        break;
                      }
                  }
                else
                  {
                    // non-decimal types are returned in native format.
                    // Just copy them to target location.
                    ex_assert(hbaseStr->len <= attr->getLength(), "Insufficent extAggrLoc length");
                    memcpy(extAggrLoc, hbaseStr->val, hbaseStr->len);
                    len = hbaseStr->len;
                  }

                if (attr->getVCIndicatorLength() > 0)
                  {
                    char * vcLenLoc = &extAggrRow_[attr->getVCLenIndOffset()];
                    attr->setVarLength(len, vcLenLoc);
                  }
              }

            step_ = EXT_AGGR_NEXT;
          }
          break;

        case EXT_AGGR_CLOSE:
          {
            if (aggrExpr() && aggrExpr()->perrecExpr())
              {
                ex_expr::exp_return_type rc = 
                  aggrExpr()->perrecExpr()->eval(workAtp_, workAtp_);
                if (rc == ex_expr::EXPR_ERROR)
                  {
                    step_ = HANDLE_ERROR;
                    break;
                  }
              }

            retcode = exti_->close();
            if (retcode < 0)
              {
		ContextCli *currContext = GetCliGlobals()->currContext();
                setupError(EXE_ERROR_FROM_LOB_INTERFACE,
			   retcode, 
                           "EXT close", 
                           exti_->getErrorText(-retcode),
			   GetCliGlobals()->getJniErrorStr());

                step_ = HANDLE_ERROR;
                break;
              }

            currRangeNum_++;

            step_ = EXT_AGGR_INIT;
          }
          break;

        case EXT_AGGR_HAVING_PRED:
          {
            if (selectPred())
              {
                ex_expr::exp_return_type evalRetCode =
                  selectPred()->eval(pentry_down->getAtp(), workAtp_);
                if (evalRetCode == ex_expr::EXPR_ERROR)
                  {
                    step_ = HANDLE_ERROR;
                    break;
                  }

                if (evalRetCode == ex_expr::EXPR_FALSE)
                  {
                    step_ = DONE;
                    break;
                  }
              }

            step_ = EXT_AGGR_PROJECT;
          }
          break;

        case EXT_AGGR_PROJECT:
          {
            ex_expr::exp_return_type evalRetCode =
              convertExpr()->eval(workAtp_, workAtp_);
            if (evalRetCode ==  ex_expr::EXPR_ERROR)
	      {
                step_ = HANDLE_ERROR;
                break;
              }
            
            step_ = EXT_AGGR_RETURN;
	  }
	  break;

	case EXT_AGGR_RETURN:
	  {
	    if (qparent_.up->isFull())
	      return WORK_OK;

	    short rc = 0;
	    if (moveRowToUpQueue(outputRow_, extAggrTdb().outputRowLength_, 
                                 (Lng32)extAggrTdb().tuppIndex_,
				 &rc, FALSE))
	      return rc;
	    
	    step_ = DONE;
	  }
	  break;

	case HANDLE_ERROR:
	  {
	    if (handleError(rc, (workAtp_ ? workAtp_->getDiagsArea() : NULL)))
	      return rc;

	    step_ = DONE;
	  }
	  break;

	case DONE:
	  {
	    if (handleDone(rc))
	      return rc;
            if (colStats_ != NULL)
            {
               deleteNAArray(getHeap(), colStats_);
               colStats_ = NULL;
            }
	    step_ = NOT_STARTED;
	  }
	  break;

	} // switch
    } // while

  return WORK_OK;
}


