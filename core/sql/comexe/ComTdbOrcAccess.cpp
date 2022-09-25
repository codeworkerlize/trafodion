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

#include "ComTdbOrcAccess.h"

///////////////////////////////////////////////////////
// ComTdbExtStorageAccess
///////////////////////////////////////////////////////
// Dummy constructor for "unpack" routines.
ComTdbExtStorageAccess::ComTdbExtStorageAccess():
 ComTdbHdfsScan()
{};

// Constructor

ComTdbExtStorageAccess::ComTdbExtStorageAccess(
                               char * tableName,
                               short type,

                               ex_expr * select_pred,
                               ex_expr * move_expr,
                               ex_expr * convert_expr,
                               ex_expr * move_convert_expr,
                               ex_expr * part_elim_expr,

                               UInt16 convertSkipListSize,
                               Int16 * convertSkipList,
                               char * hostName,
                               tPort port,

                               Queue * hdfsFileInfoList,
                               Queue * hdfsFileRangeBeginList,
                               Queue * hdfsFileRangeNumList,
                               Queue * hdfsColInfoList,

                               char recordDelimiter,
                               char columnDelimiter,

                               Int64 hdfsBufSize,
                               UInt32 rangeTailIOSize,

                               Int32 numPartCols,
                               Int64 hdfsSqlMaxRecLen,
                               Int64 outputRowLength,
                               Int64 asciiRowLen,
                               Int64 moveColsRowLen,
                               Int32 partColsRowLength,
                               Int32 virtColsRowLength,

                               const unsigned short tuppIndex,
                               const unsigned short asciiTuppIndex,
                               const unsigned short workAtpIndex,
                               const unsigned short moveColsTuppIndex,
                               const unsigned short partColsTuppIndex,
                               const unsigned short virtColsTuppIndex,

                               ex_cri_desc * work_cri_desc,
                               ex_cri_desc * given_cri_desc,
                               ex_cri_desc * returned_cri_desc,
                               queue_index down,
                               queue_index up,
                               Cardinality estimatedRowCount,
                               Int32  numBuffers,
                               UInt32  bufferSize,

                               char * errCountTable,
                               char * loggingLocation,
                               char * errCountId,
                               char * hdfsRootDir,
                               Int64 modTSforDir,
                               Lng32 numOfPartCols,
                               Queue * hdfsDirsToCheck,

                               Queue * colNameList,
                               Queue * colTypeList,
                               char * parqSchStr
                               )
  : ComTdbHdfsScan( 
                   tableName,
                   type,
                   select_pred,
                   move_expr,
                   convert_expr,
                   move_convert_expr,
                   part_elim_expr,
                   convertSkipListSize, convertSkipList, hostName, port,
                   hdfsFileInfoList,
                   hdfsFileRangeBeginList,
                   hdfsFileRangeNumList,
                   hdfsColInfoList,
                   NULL, 0, // no compression info
                   recordDelimiter, columnDelimiter, NULL, NULL, 0, 
                   hdfsBufSize, 
                   rangeTailIOSize,
                   numPartCols,
                   hdfsSqlMaxRecLen,
                   outputRowLength, asciiRowLen, moveColsRowLen,
                   partColsRowLength,
                   virtColsRowLength,
                   tuppIndex, asciiTuppIndex, workAtpIndex, moveColsTuppIndex, 
                   0,
                   partColsTuppIndex,
                   virtColsTuppIndex,
                   work_cri_desc,
                   given_cri_desc,
                   returned_cri_desc,
                   down,
                   up,
                   estimatedRowCount,
                   numBuffers,
                   bufferSize,
                   errCountTable, loggingLocation, errCountId,
                   hdfsRootDir, modTSforDir, numOfPartCols, hdfsDirsToCheck,
                   colNameList, colTypeList, parqSchStr
                  )
{
}

ComTdbExtStorageAccess::~ComTdbExtStorageAccess()
{};


///////////////////////////////////////////////////////
// ComTdbExtStorageScan
///////////////////////////////////////////////////////
// Dummy constructor for "unpack" routines.
ComTdbExtStorageScan::ComTdbExtStorageScan():
 ComTdbExtStorageAccess()
{};

// Constructor

ComTdbExtStorageScan::ComTdbExtStorageScan(
                               char * tableName,
                               short type,
                               ex_expr * select_pred,
                               ex_expr * move_expr,
                               ex_expr * convert_expr,
                               ex_expr * move_convert_expr,
                               ex_expr * part_elim_expr,
                               ex_expr * extOperExpr,
                               UInt16 convertSkipListSize,
                               Int16 * convertSkipList,
                               char * hostName,
                               tPort port,
                               Queue * hdfsFileInfoList,
                               Queue * hdfsFileRangeBeginList,
                               Queue * hdfsFileRangeNumList,
                               Queue * hdfsColInfoList,
                               Queue * extAllColInfoList,
                               char recordDelimiter,
                               char columnDelimiter,
                               Int64 hdfsBufSize,
                               Int32 extMaxRowsToFill,
                               Int32 extMaxAllocationInMB,
                               UInt32 rangeTailIOSize,
                               Int32 numPartCols,
                               Queue * tdbListOfExtPPI,
                               Int64 hdfsSqlMaxRecLen,
                               Int64 outputRowLength,
                               Int64 asciiRowLen,
                               Int64 moveColsRowLen,
                               Int32 partColsRowLength,
                               Int32 virtColsRowLength,
                               Int64 extOperLength,
                               const unsigned short tuppIndex,
                               const unsigned short asciiTuppIndex,
                               const unsigned short workAtpIndex,
                               const unsigned short moveColsTuppIndex,
                               const unsigned short partColsTuppIndex,
                               const unsigned short virtColsTuppIndex,
                               const unsigned short extOperTuppIndex,
                               ex_cri_desc * work_cri_desc,
                               ex_cri_desc * given_cri_desc,
                               ex_cri_desc * returned_cri_desc,
                               queue_index down,
                               queue_index up,
                               Cardinality estimatedRowCount,
                               Int32  numBuffers,
                               UInt32  bufferSize,
                               char * errCountTable,
                               char * loggingLocation,
                               char * errCountId,

                               char * hdfsRootDir,
                               Int64  modTSforDir,
                               Lng32 numOfPartCols,
                               Queue * hdfsDirsToCheck,

                               Queue * colNameList,
                               Queue * colTypeList,

                               NABoolean removeMinMaxFromScanFilter,
                               float maxSelectivity,
                               Int32 maxFilters,
                               NABoolean useArrowReader,
                               NABoolean parquetFullScan,
                               Int32 parquetScanParallelism,
                               NABoolean skipCCStatsCheck,
                               NABoolean filterRowgroups,
                               NABoolean filterPages,
                               NABoolean directCopy,
                               char * parqSchStr
                             )
     : ComTdbExtStorageAccess
       ( 
            tableName,
            type,
            select_pred,
            move_expr,
            convert_expr,
            move_convert_expr,
            part_elim_expr,
            convertSkipListSize, convertSkipList, hostName, port,
            hdfsFileInfoList,
            hdfsFileRangeBeginList,
            hdfsFileRangeNumList,
            hdfsColInfoList,
            recordDelimiter, columnDelimiter, hdfsBufSize, 
            rangeTailIOSize,
            numPartCols,
            hdfsSqlMaxRecLen,
            outputRowLength, asciiRowLen, moveColsRowLen,
            partColsRowLength,
            virtColsRowLength,
            tuppIndex, asciiTuppIndex, workAtpIndex, moveColsTuppIndex, 
            partColsTuppIndex,
            virtColsTuppIndex,
            work_cri_desc,
            given_cri_desc,
            returned_cri_desc,
            down,
            up,
            estimatedRowCount,
            numBuffers,
            bufferSize,
            errCountTable, loggingLocation, errCountId,
            hdfsRootDir, modTSforDir, numOfPartCols, hdfsDirsToCheck,
            colNameList, colTypeList, parqSchStr
         ),
       extOperExpr_(extOperExpr),
       extOperLength_(extOperLength),
       extOperTuppIndex_(extOperTuppIndex),
       listOfExtPPI_(tdbListOfExtPPI),
       extAllColInfoList_(extAllColInfoList),
       extMaxRowsToFill_(extMaxRowsToFill),
       extMaxAllocationInMB_(extMaxAllocationInMB),
       maxSelectivity_(maxSelectivity),
       maxFilters_(maxFilters),
       parquetScanParallelism_(parquetScanParallelism)
{
  setNodeType(ComTdb::ex_EXT_STORAGE_SCAN);
  setEyeCatcher(eye_EXT_STORAGE_SCAN);
  setRemoveMinMaxFromScanFilter(removeMinMaxFromScanFilter);
  setUseArrowReader(useArrowReader);
  setParquetFullScan(parquetFullScan);
  setSkipColumnChunkStatsCheck(skipCCStatsCheck);
  setFilterRowgroups(filterRowgroups);
  setFilterPages(filterPages);
  setDirectCopy(directCopy);
}

ComTdbExtStorageScan::~ComTdbExtStorageScan()
{};

Long ComTdbExtStorageScan::pack(void * space)
{
  if (listOfExtPPI() && listOfExtPPI()->numEntries() > 0)
    {
      listOfExtPPI()->position();
      for (Lng32 i = 0; i < listOfExtPPI()->numEntries(); i++)
	{
	  ComTdbExtPPI * ppi = (ComTdbExtPPI*)listOfExtPPI()->getNext();
          ppi->colName_.pack(space);
          ppi->filterName_.pack(space);
	}
    }
 
  listOfExtPPI_.pack(space);

  extOperExpr_.pack(space);

  // pack elements in extAllColInfoList
  if (extAllColInfoList())
    {
      extAllColInfoList_.pack(space);
    }

  return ComTdbHdfsScan::pack(space);
}

Lng32 ComTdbExtStorageScan::unpack(void * base, void * reallocator)
{
  if(listOfExtPPI_.unpack(base, reallocator)) return -1;

  if (listOfExtPPI() && listOfExtPPI()->numEntries() > 0)
    {
      listOfExtPPI()->position();
      for (Lng32 i = 0; i < listOfExtPPI()->numEntries(); i++)
	{
	  ComTdbExtPPI * ppi = (ComTdbExtPPI*)listOfExtPPI()->getNext();
          if (ppi->colName_.unpack(base)) return -1;
          if (ppi->filterName_.unpack(base)) return -1;
	}
    }

  if (extAllColInfoList_.unpack(base, reallocator)) return -1;

  if(extOperExpr_.unpack(base, reallocator)) return -1;

  return ComTdbHdfsScan::unpack(base, reallocator);
}

void ComTdbExtStorageScan::displayContents(Space * space,ULng32 flag)
{
  ComTdb::displayContents(space,flag & 0xFFFFFFFE);

  if(flag & 0x00000008)
    {
      char buf[16384];

      str_sprintf(buf, "\nFor ComTdbExtStorageScan :");
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    }

  ComTdbHdfsScan::displayContentsBase(space, flag);

  if (listOfExtPPI() && listOfExtPPI()->numEntries() > 0)
    {
      char buf[500];
      str_sprintf(buf, "\nNumber of PPI entries: %d", 
                  listOfExtPPI()->numEntries());
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
       
      listOfExtPPI()->position();
      for (Lng32 i = 0; i < listOfExtPPI()->numEntries(); i++)
	{
	  ComTdbExtPPI * ppi = (ComTdbExtPPI*)listOfExtPPI()->getNext();

          str_sprintf(buf, "PPI: #%d", i+1);
          space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
          
          str_sprintf(buf, "  type: %s(%d)",
                      extPushdownOperatorTypeStr[ppi->type()], ppi->type());
          space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
          if (ppi->operAttrIndex_ >= 0)
            {
              str_sprintf(buf, "  operAttrIndex: %d", ppi->operAttrIndex_);
              space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
            }
          
          if (ppi->colName())
            {
              str_sprintf(buf, "  colName_: %s", ppi->colName());
              space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));  
            }
          
          if (ppi->rowCount() > 0.0)
            {
              str_sprintf(buf, "  rowCount_: %ld", ppi->rowCount());
              space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));  
            }
          
          if (ppi->uec() > 0.0)
            {
              str_sprintf(buf, "  uec_: %ld", ppi->uec());
              space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));  
            }
 	} // for

      if (extAllColInfoList())
        {
          str_sprintf(buf, "Num Of extAllColInfoList entries: %d",
                      extAllColInfoList()->entries());
          space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
         }

    }

  if(flag & 0x00000001)
    {
      displayExpression(space,flag);
      displayChildren(space,flag);
    }
}

Long ComTdbExtPPI::pack(void * space)
{
  //  operands_.pack(space);
  colName_.pack(space);
  filterName_.pack(space);

  return NAVersionedObject::pack(space);
}

Lng32 ComTdbExtPPI::unpack(void * base, void * reallocator)
{
  if(colName_.unpack(base)) return -1;
  if(filterName_.unpack(base)) return -1;
  
  return NAVersionedObject::unpack(base, reallocator);
}

///////////////////////////////////////////////////////////////
// ComTdbExtStorageFastAggr
///////////////////////////////////////////////////////////////

// Dummy constructor for "unpack" routines.
ComTdbExtStorageFastAggr::ComTdbExtStorageFastAggr():
 ComTdbExtStorageScan()
{
  setNodeType(ComTdb::ex_EXT_STORAGE_AGGR);
  setEyeCatcher(eye_EXT_STORAGE_AGGR);
};

// Constructor
ComTdbExtStorageFastAggr::ComTdbExtStorageFastAggr(
                                     char * tableName,
                                     short type,
                                     Queue * hdfsFileInfoList,
                                     Queue * hdfsFileRangeBeginList,
                                     Queue * hdfsFileRangeNumList,
                                     Queue * listOfAggrTypes,
                                     Queue * listOfAggrColInfo,
                                     ex_expr * aggr_expr,
                                     Int32 finalAggrRowLength,
                                     const unsigned short finalAggrTuppIndex,
                                     Int32 extAggrRowLength,
                                     const unsigned short extAggrTuppIndex,
                                     ex_expr * having_expr,
                                     ex_expr * proj_expr,
                                     Int64 projRowLen,
                                     const unsigned short projTuppIndex,
                                     const unsigned short returnedTuppIndex,
                                     ex_cri_desc * work_cri_desc,
                                     ex_cri_desc * given_cri_desc,
                                     ex_cri_desc * returned_cri_desc,
                                     queue_index down,
                                     queue_index up,
                                     Int32  numBuffers,
                                     UInt32  bufferSize,
                                     Int32 numPartCols
                                     )
  : ComTdbExtStorageScan( 
                   tableName,
                   //(short)ComTdbHdfsScan::EXT_,
                   type,
                   having_expr,
                   NULL,
                   proj_expr,
                   NULL, NULL, NULL,
                   0, NULL, NULL, 0,
                   hdfsFileInfoList,
                   hdfsFileRangeBeginList,
                   hdfsFileRangeNumList,
                   listOfAggrColInfo,
                   NULL,
                   0, 0, 0, 0, 0, 0, numPartCols, NULL, 0,
                   projRowLen, 
                   0, 0, 0, 0, 0,
                   returnedTuppIndex,
                   0,
                   finalAggrTuppIndex, 
                   projTuppIndex,
                   0, 0, 0,
                   work_cri_desc,
                   given_cri_desc,
                   returned_cri_desc,
                   down,
                   up,
                   0,
                   numBuffers,        // num_buffers - we use numInnerTuples_ instead.
                   bufferSize),       // buffer_size - we use numInnerTuples_ instead.
    aggrExpr_(aggr_expr),
    finalAggrRowLength_(finalAggrRowLength),
    extAggrRowLength_(extAggrRowLength),
    extAggrTuppIndex_(extAggrTuppIndex),
    aggrTypeList_(listOfAggrTypes)
{
  setNodeType(ComTdb::ex_EXT_STORAGE_AGGR);
  setEyeCatcher(eye_EXT_STORAGE_AGGR);
}

ComTdbExtStorageFastAggr::~ComTdbExtStorageFastAggr()
{
}

Long ComTdbExtStorageFastAggr::pack(void * space)
{
  aggrTypeList_.pack(space);
  aggrExpr_.pack(space);

  return ComTdbHdfsScan::pack(space);
}

Lng32 ComTdbExtStorageFastAggr::unpack(void * base, void * reallocator)
{
  if (aggrTypeList_.unpack(base, reallocator)) return -1;
  if (aggrExpr_.unpack(base, reallocator)) return -1;

  return ComTdbHdfsScan::unpack(base, reallocator);
}

void ComTdbExtStorageFastAggr::displayContents(Space * space,ULng32 flag)
{
  ComTdb::displayContents(space,flag & 0xFFFFFFFE);

  if(flag & 0x00000008)
    {
      char buf[16384];

      str_sprintf(buf, "\nFor ComTdbExtStorageFastAggr :");
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    }

  ComTdbHdfsScan::displayContentsBase(space, flag);

  if(flag & 0x00000001)
    {
      displayExpression(space,flag);
      displayChildren(space,flag);
    }
}

void ComTdbExtPPI::display(ostream& out)
{
   out << "type=" << type();
   out << ", attr index=" << operAttrIndex();
   out << ", ppi type=" << type();
   out << ", colType=" << colType().data();

   if ( colName() ) 
      out << ", colName=" << colName();

   out << ", rowCount=" << rowCount();
   out << ", uec=" << uec();
   out << ", rowsToSurvive=" << rowsToSurvive();

   out << endl;
}
