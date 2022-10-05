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
//
**********************************************************************/

#include "sqlcat/TrafDDLdesc.h"
#include "common/CmpCommon.h"
#include "SQLTypeDefs.h"

// -----------------------------------------------------------------------
// Allocate a column_desc and do simple initialization of several fields,
// based on what's passed in.  Many of the fields we just default,
// to either hardcoded values or to zero.  The callers,
// in arkcmplib + generator + optimizer, can set additional fields afterwards.
// -----------------------------------------------------------------------
TrafDesc *TrafMakeColumnDesc(const char *tablename, const char *colname,
                             int &colnumber,  // INOUT
                             int datatype, int length,
                             int &offset,  // INOUT
                             NABoolean null_flag, SQLCHARSET_CODE datacharset, NAMemory *space) {
#undef COLUMN
#define COLUMN returnDesc->columnsDesc()

  // Pass in the optional "passedDesc" if you just want to overwrite an
  // already existing desc.
  TrafDesc *returnDesc = TrafAllocateDDLdesc(DESC_COLUMNS_TYPE, space);

  COLUMN->colname = (char *)colname;

  COLUMN->colnumber = colnumber;
  COLUMN->datatype = datatype;
  COLUMN->length = length;
  COLUMN->offset = offset;
  COLUMN->setNullable(null_flag);

  // Hardcode some fields here.
  // All other fields (scale, precision, etc) default to zero!

  COLUMN->colclass = 'U';
  COLUMN->setDefaultClass(COM_NO_DEFAULT);

  if ((DFS2REC::isAnyCharacter(datatype)) || (datatype == REC_CLOB)) {
    if (datacharset == SQLCHARSETCODE_UNKNOWN) {
      COLUMN->character_set = CharInfo::DefaultCharSet;
      COLUMN->encoding_charset = CharInfo::DefaultCharSet;
    } else {
      COLUMN->character_set = (CharInfo::CharSet)datacharset;
      COLUMN->encoding_charset = (CharInfo::CharSet)datacharset;
    }
    COLUMN->collation_sequence = CharInfo::DefaultCollation;
    if (DFS2REC::isSQLVarChar(datatype)) offset += SQL_VARCHAR_HDR_SIZE;
  } else {  // datetime, interval, numeric, etc.
    COLUMN->datetimestart = COLUMN->datetimeend = REC_DATE_UNKNOWN;
  }

  colnumber++;

  offset += length;
  if (null_flag) offset += SQL_NULL_HDR_SIZE;

  return returnDesc;
}

// -----------------------------------------------------------------------
// Allocate one of the primitive structs and initialize to all zeroes.
// Uses HEAP (StatementHeap) of CmpCommon or space.
// -----------------------------------------------------------------------
TrafDesc *TrafAllocateDDLdesc(desc_nodetype nodetype, NAMemory *space) {
  size_t size = 0;
  TrafDesc *desc_ptr = NULL;

  switch (nodetype) {
    case DESC_CHECK_CONSTRNTS_TYPE:
      desc_ptr = new GENHEAP(space) TrafCheckConstrntsDesc();
      break;
    case DESC_COLUMNS_TYPE:
      desc_ptr = new GENHEAP(space) TrafColumnsDesc();
      break;
    case DESC_CONSTRNTS_TYPE:
      desc_ptr = new GENHEAP(space) TrafConstrntsDesc();
      break;
    case DESC_CONSTRNT_KEY_COLS_TYPE:
      desc_ptr = new GENHEAP(space) TrafConstrntKeyColsDesc();
      break;
    case DESC_FILES_TYPE:
      desc_ptr = new GENHEAP(space) TrafFilesDesc();
      break;
    case DESC_HBASE_RANGE_REGION_TYPE:
      desc_ptr = new GENHEAP(space) TrafHbaseRegionDesc();
      break;
    case DESC_HISTOGRAM_TYPE:
      desc_ptr = new GENHEAP(space) TrafHistogramDesc();
      break;
    case DESC_HIST_INTERVAL_TYPE:
      desc_ptr = new GENHEAP(space) TrafHistIntervalDesc();
      break;
    case DESC_INDEXES_TYPE:
      desc_ptr = new GENHEAP(space) TrafIndexesDesc();
      break;
    case DESC_KEYS_TYPE:
      desc_ptr = new GENHEAP(space) TrafKeysDesc();
      break;
    case DESC_LIBRARY_TYPE:
      desc_ptr = new GENHEAP(space) TrafLibraryDesc();
      break;
    case DESC_PARTNS_TYPE:
      desc_ptr = new GENHEAP(space) TrafPartnsDesc();
      break;
    case DESC_REF_CONSTRNTS_TYPE:
      desc_ptr = new GENHEAP(space) TrafRefConstrntsDesc();
      break;
    case DESC_ROUTINE_TYPE:
      desc_ptr = new GENHEAP(space) TrafRoutineDesc();
      break;
    case DESC_SEQUENCE_GENERATOR_TYPE:
      desc_ptr = new GENHEAP(space) TrafSequenceGeneratorDesc();
      break;
    case DESC_TABLE_TYPE:
      desc_ptr = new GENHEAP(space) TrafTableDesc();
      break;
    case DESC_TABLE_STATS_TYPE:
      desc_ptr = new GENHEAP(space) TrafTableStatsDesc();
      break;
    case DESC_VIEW_TYPE:
      desc_ptr = new GENHEAP(space) TrafViewDesc();
      break;
    case DESC_USING_MV_TYPE:
      desc_ptr = new GENHEAP(space) TrafUsingMvDesc();
      break;
    case DESC_PRIV_TYPE:
      desc_ptr = new GENHEAP(space) TrafPrivDesc();
      break;
    case DESC_PRIV_GRANTEE_TYPE:
      desc_ptr = new GENHEAP(space) TrafPrivGranteeDesc();
      break;
    case DESC_PRIV_BITMAP_TYPE:
      desc_ptr = new GENHEAP(space) TrafPrivBitmapDesc();
      break;
    case DESC_PARTITIONV2_TYPE:
      desc_ptr = new GENHEAP(space) TrafPartitionV2Desc();
      break;
    case DESC_PART_TYPE:
      desc_ptr = new GENHEAP(space) TrafPartDesc();
      break;
    default:
      assert(FALSE);
      break;
  }

  // if not being allocated from space, memset all bytes to 0.
  // If allocated from space, it will be set to 0 during space allocation.
  if ((!space) || (!space->isComSpace()))
    memset((char *)desc_ptr + sizeof(TrafDesc), 0, desc_ptr->getClassSize() - sizeof(TrafDesc));

  return desc_ptr;
}

TrafDesc::TrafDesc(UInt16 nodeType)
    : NAVersionedObject(nodeType), nodetype(nodeType), version(CURR_VERSION), descFlags(0), next(NULL) {}

int TrafDesc::validateSize() {
  if (getImageSize() != getClassSize()) return -1;

  return 0;
}

int TrafDesc::validateVersion() {
  if (version != CURR_VERSION) return -1;

  return 0;
}

int TrafDesc::migrateToNewVersion(NAVersionedObject *&newImage) {
  short tempimagesize = getClassSize();
  // -----------------------------------------------------------------
  // The base class implementation of migrateToNewVersion() is only
  // called with newImage == NULL when the same function is not
  // redefined at the subclass. That means no new version of that
  // subclass has been invented yet.
  // -----------------------------------------------------------------
  if (newImage == NULL) {
    if (validateSize()) return -1;

    if (validateVersion()) return -1;
  }

  return NAVersionedObject::migrateToNewVersion(newImage);
}

char *TrafDesc::findVTblPtr(short classID) {
  char *vtblptr = NULL;

  switch (classID) {
    case DESC_CHECK_CONSTRNTS_TYPE:
      GetVTblPtr(vtblptr, TrafCheckConstrntsDesc);
      break;
    case DESC_COLUMNS_TYPE:
      GetVTblPtr(vtblptr, TrafColumnsDesc);
      break;
    case DESC_CONSTRNTS_TYPE:
      GetVTblPtr(vtblptr, TrafConstrntsDesc);
      break;
    case DESC_CONSTRNT_KEY_COLS_TYPE:
      GetVTblPtr(vtblptr, TrafConstrntKeyColsDesc);
      break;
    case DESC_FILES_TYPE:
      GetVTblPtr(vtblptr, TrafFilesDesc);
      break;
    case DESC_HBASE_RANGE_REGION_TYPE:
      GetVTblPtr(vtblptr, TrafHbaseRegionDesc);
      break;
    case DESC_HISTOGRAM_TYPE:
      GetVTblPtr(vtblptr, TrafHistogramDesc);
      break;
    case DESC_HIST_INTERVAL_TYPE:
      GetVTblPtr(vtblptr, TrafHistIntervalDesc);
      break;
    case DESC_INDEXES_TYPE:
      GetVTblPtr(vtblptr, TrafIndexesDesc);
      break;
    case DESC_KEYS_TYPE:
      GetVTblPtr(vtblptr, TrafKeysDesc);
      break;
    case DESC_LIBRARY_TYPE:
      GetVTblPtr(vtblptr, TrafLibraryDesc);
      break;
    case DESC_PARTNS_TYPE:
      GetVTblPtr(vtblptr, TrafPartnsDesc);
      break;
    case DESC_REF_CONSTRNTS_TYPE:
      GetVTblPtr(vtblptr, TrafRefConstrntsDesc);
      break;
    case DESC_ROUTINE_TYPE:
      GetVTblPtr(vtblptr, TrafRoutineDesc);
      break;
    case DESC_SEQUENCE_GENERATOR_TYPE:
      GetVTblPtr(vtblptr, TrafSequenceGeneratorDesc);
      break;
    case DESC_TABLE_TYPE:
      GetVTblPtr(vtblptr, TrafTableDesc);
      break;
    case DESC_TABLE_STATS_TYPE:
      GetVTblPtr(vtblptr, TrafTableStatsDesc);
      break;
    case DESC_VIEW_TYPE:
      GetVTblPtr(vtblptr, TrafViewDesc);
      break;
    case DESC_USING_MV_TYPE:
      GetVTblPtr(vtblptr, TrafUsingMvDesc);
      break;
    case DESC_PRIV_TYPE:
      GetVTblPtr(vtblptr, TrafPrivDesc);
      break;
    case DESC_PRIV_GRANTEE_TYPE:
      GetVTblPtr(vtblptr, TrafPrivGranteeDesc);
      break;
    case DESC_PRIV_BITMAP_TYPE:
      GetVTblPtr(vtblptr, TrafPrivBitmapDesc);
      break;
    case DESC_PARTITIONV2_TYPE:
      GetVTblPtr(vtblptr, TrafPartitionV2Desc);
      break;
    case DESC_PART_TYPE:
      GetVTblPtr(vtblptr, TrafPartDesc);
      break;
    default:
      assert(FALSE);
      break;
  }

  return vtblptr;
}

short TrafDesc::copyFrom(TrafDesc *from, NAMemory *heap) {
  // skip virtual table pointer stored as first eight bytes (64 bit pointer)
  char *vtblPtr = (char *)this;
  char *dataLoc = vtblPtr + sizeof(NAVersionedObject);

  char *fromDataLoc = (char *)from + sizeof(NAVersionedObject);

  memcpy(dataLoc, fromDataLoc, getClassSize() - sizeof(NAVersionedObject));

  return 0;
}

TrafDesc *TrafDesc::copyDescList(TrafDesc *srcDescList, NAMemory *heap) {
  TrafDesc *retDescList = NULL;
  TrafDesc *srcDesc = srcDescList;
  TrafDesc *prevDesc = NULL;
  while (srcDesc) {
    TrafDesc *tgtDesc = TrafAllocateDDLdesc((desc_nodetype)srcDesc->nodetype, heap);

    if (retDescList == NULL) {
      retDescList = tgtDesc;
    } else
      prevDesc->next = tgtDesc;

    if (tgtDesc->copyFrom(srcDesc, heap))  // error
      return NULL;

    prevDesc = tgtDesc;
    srcDesc = srcDesc->next;
  }

  return retDescList;
}

void TrafDesc::deleteDescList(TrafDesc *srcDescList, NAMemory *heap) {
  TrafDesc *srcDesc = srcDescList;
  TrafDesc *nextDesc = NULL;
  while (srcDesc) {
    nextDesc = srcDesc->next;
    srcDesc->deallocMembers(heap);
    NADELETE(srcDesc, TrafDesc, heap);

    srcDesc = nextDesc;
  }
}

// pack and unpack methods for various descriptor structs

// TrafDesc::pack is a non-recursive pack.
// It goes down the list pointed to by 'next' field and packs
// each element. Before pack, the 'next' pointer of that element
// is set to NULL which causes only that one element to be packed.
// After pack, 'next' pointer is restored.
Long TrafDesc::pack(void *space) {
  DescStructPtr currDescPtr;
  TrafDesc *currDesc;
  TrafDesc *nextDesc;

  currDesc = next;
  next.packShallow(space);
  while (currDesc) {
    currDescPtr = currDesc;
    nextDesc = currDesc->next;
    currDesc->next = 0;
    currDescPtr.pack(space);
    currDesc->next = nextDesc;
    currDesc->next.packShallow(space);
    currDesc = nextDesc;
  }

  return NAVersionedObject::pack(space);
}

// TrafDesc::unpack is a non-recursive unpack.
// It goes down the list pointed to by 'next' field and unpacks
// each element. Before unpack, the 'next' pointer of that element
// is set to NULL which causes only that one element to be unpacked.
// After unpack, 'next' pointer is restored.
int TrafDesc::unpack(void *base, void *reallocator) {
  DescStructPtr currDescPtr;
  DescStructPtr nextDescPtr;
  TrafDesc *currDesc;

  currDescPtr = next;
  if (next.unpackShallow(base)) return -1;

  while (!currDescPtr.isNull()) {
    if (currDescPtr.unpackShallow(base)) return -1;
    currDesc = currDescPtr.getPointer();
    nextDescPtr = currDesc->next;
    currDesc->next = 0;
    currDescPtr.packShallow(base, 0);
    if (currDescPtr.unpack(base, reallocator)) return -1;
    currDesc->next = nextDescPtr;
    if (currDesc->next.unpackShallow(base)) return -1;

    currDescPtr = nextDescPtr;
  }

  return NAVersionedObject::unpack(base, reallocator);
}

short TrafDesc::allocAndCopy(char *src, char *&tgt, NAMemory *heap) {
  tgt = NULL;

  if (src) {
    tgt = new (heap) char[strlen(src) + 1];
    strcpy(tgt, src);

    setWasCopied(TRUE);
  }

  return 0;
}

NABoolean TrafDesc::deepCompare(const TrafDesc &other) const {
  TrafDesc *currThisDesc = (TrafDesc *)this;
  TrafDesc *currOtherDesc = (TrafDesc *)&other;

  while (!currThisDesc && !currOtherDesc) {
    if (!(*currThisDesc == *currOtherDesc)) return FALSE;

    currThisDesc = (currThisDesc->next).getPointer();
    currOtherDesc = (currOtherDesc->next).getPointer();
  }

  if (!currThisDesc || !currOtherDesc) return FALSE;

  return TRUE;
}

Long TrafCheckConstrntsDesc::pack(void *space) {
  constrnt_text = (constrnt_text ? (char *)(((Space *)space)->convertToOffset(constrnt_text)) : NULL);

  return TrafDesc::pack(space);
}

int TrafCheckConstrntsDesc::unpack(void *base, void *reallocator) {
  constrnt_text = (constrnt_text ? (char *)((char *)base - (Long)constrnt_text) : NULL);

  return TrafDesc::unpack(base, reallocator);
}

////////////////////////////////////////////
// TrafColumnsDesc
////////////////////////////////////////////
Long TrafColumnsDesc::pack(void *space) {
  colname = (colname ? (char *)(((Space *)space)->convertToOffset(colname)) : NULL);

  pictureText = (pictureText ? (char *)(((Space *)space)->convertToOffset(pictureText)) : NULL);

  defaultvalue = (defaultvalue ? (char *)(((Space *)space)->convertToOffset(defaultvalue)) : NULL);
  initDefaultValue = (initDefaultValue ? (char *)(((Space *)space)->convertToOffset(initDefaultValue)) : NULL);
  heading = (heading ? (char *)(((Space *)space)->convertToOffset(heading)) : NULL);
  computed_column_text =
      (computed_column_text ? (char *)(((Space *)space)->convertToOffset(computed_column_text)) : NULL);

  hbaseColFam = (hbaseColFam ? (char *)(((Space *)space)->convertToOffset(hbaseColFam)) : NULL);
  hbaseColQual = (hbaseColQual ? (char *)(((Space *)space)->convertToOffset(hbaseColQual)) : NULL);

  compDefnStr = (compDefnStr ? (char *)(((Space *)space)->convertToOffset(compDefnStr)) : NULL);

  return TrafDesc::pack(space);
}

int TrafColumnsDesc::unpack(void *base, void *reallocator) {
  colname = (colname ? (char *)((char *)base - (Long)colname) : NULL);

  pictureText = (pictureText ? (char *)((char *)base - (Long)pictureText) : NULL);

  defaultvalue = (defaultvalue ? (char *)((char *)base - (Long)defaultvalue) : NULL);
  initDefaultValue = (initDefaultValue ? (char *)((char *)base - (Long)initDefaultValue) : NULL);
  heading = (heading ? (char *)((char *)base - (Long)heading) : NULL);
  computed_column_text = (computed_column_text ? (char *)((char *)base - (Long)computed_column_text) : NULL);

  hbaseColFam = (hbaseColFam ? (char *)((char *)base - (Long)hbaseColFam) : NULL);
  hbaseColQual = (hbaseColQual ? (char *)((char *)base - (Long)hbaseColQual) : NULL);

  compDefnStr = (compDefnStr ? (char *)((char *)base - (Long)compDefnStr) : NULL);

  return TrafDesc::unpack(base, reallocator);
}

short TrafColumnsDesc::copyFrom(TrafDesc *from, NAMemory *heap) {
  if (from->nodetype != DESC_COLUMNS_TYPE) return -1;

  TrafDesc::copyFrom(from, heap);

  TrafColumnsDesc *tFrom = (TrafColumnsDesc *)from;

  if (allocAndCopy(tFrom->colname, colname, heap)) return -1;

  if (allocAndCopy(tFrom->pictureText, pictureText, heap)) return -1;

  if (allocAndCopy(tFrom->defaultvalue, defaultvalue, heap)) return -1;

  if (allocAndCopy(tFrom->initDefaultValue, initDefaultValue, heap)) return -1;

  if (allocAndCopy(tFrom->heading, heading, heap)) return -1;

  if (allocAndCopy(tFrom->computed_column_text, computed_column_text, heap)) return -1;

  if (allocAndCopy(tFrom->hbaseColFam, hbaseColFam, heap)) return -1;

  if (allocAndCopy(tFrom->hbaseColQual, hbaseColQual, heap)) return -1;

  if (allocAndCopy(tFrom->compDefnStr, compDefnStr, heap)) return -1;

  return 0;
}

void TrafColumnsDesc::deallocMembers(NAMemory *heap) {
  if (NOT wasCopied()) return;

  NADELETEBASIC(colname, heap);
  NADELETEBASIC(pictureText, heap);
  NADELETEBASIC(defaultvalue, heap);
  NADELETEBASIC(initDefaultValue, heap);
  NADELETEBASIC(heading, heap);
  NADELETEBASIC(computed_column_text, heap);
  NADELETEBASIC(hbaseColFam, heap);
  NADELETEBASIC(hbaseColQual, heap);
  NADELETEBASIC(compDefnStr, heap);

  return TrafDesc::deallocMembers(heap);
}

///////////////////////////////////////////
// TrafConstrntsDesc
///////////////////////////////////////////
Long TrafConstrntsDesc::pack(void *space) {
  constrntname = (constrntname ? (char *)(((Space *)space)->convertToOffset(constrntname)) : NULL);
  tablename = (tablename ? (char *)(((Space *)space)->convertToOffset(tablename)) : NULL);

  check_constrnts_desc.pack(space);
  constr_key_cols_desc.pack(space);
  referenced_constrnts_desc.pack(space);
  referencing_constrnts_desc.pack(space);

  return TrafDesc::pack(space);
}

int TrafConstrntsDesc::unpack(void *base, void *reallocator) {
  constrntname = (constrntname ? (char *)((char *)base - (Long)constrntname) : NULL);
  tablename = (tablename ? (char *)((char *)base - (Long)tablename) : NULL);

  if (check_constrnts_desc.unpack(base, reallocator)) return -1;
  if (constr_key_cols_desc.unpack(base, reallocator)) return -1;
  if (referenced_constrnts_desc.unpack(base, reallocator)) return -1;
  if (referencing_constrnts_desc.unpack(base, reallocator)) return -1;

  return TrafDesc::unpack(base, reallocator);
}

Long TrafConstrntKeyColsDesc::pack(void *space) {
  colname = (colname ? (char *)(((Space *)space)->convertToOffset(colname)) : NULL);

  return TrafDesc::pack(space);
}

int TrafConstrntKeyColsDesc::unpack(void *base, void *reallocator) {
  colname = (colname ? (char *)((char *)base - (Long)colname) : NULL);

  return TrafDesc::unpack(base, reallocator);
}

Long TrafFilesDesc::pack(void *space) {
  partns_desc.pack(space);

  return TrafDesc::pack(space);
}

int TrafFilesDesc::unpack(void *base, void *reallocator) {
  if (partns_desc.unpack(base, reallocator)) return -1;

  return TrafDesc::unpack(base, reallocator);
}

Long TrafHbaseRegionDesc::pack(void *space) {
  beginKey = (beginKey ? (char *)(((Space *)space)->convertToOffset(beginKey)) : NULL);
  endKey = (endKey ? (char *)(((Space *)space)->convertToOffset(endKey)) : NULL);

  return TrafDesc::pack(space);
}

int TrafHbaseRegionDesc::unpack(void *base, void *reallocator) {
  beginKey = (beginKey ? (char *)((char *)base - (Long)beginKey) : NULL);
  endKey = (endKey ? (char *)((char *)base - (Long)endKey) : NULL);

  return TrafDesc::unpack(base, reallocator);
}

//////////////////////////////////////////////
// class TrafHistogramDesc
//////////////////////////////////////////////
Long TrafHistogramDesc::pack(void *space) {
  tablename = (tablename ? (char *)(((Space *)space)->convertToOffset(tablename)) : NULL);

  low_value = (low_value ? (char *)(((Space *)space)->convertToOffset(low_value)) : NULL);
  high_value = (high_value ? (char *)(((Space *)space)->convertToOffset(high_value)) : NULL);

  // v5, expression text
  v5 = (v5 ? (char *)(((Space *)space)->convertToOffset(v5)) : NULL);

  return TrafDesc::pack(space);
}

int TrafHistogramDesc::unpack(void *base, void *reallocator) {
  tablename = (tablename ? (char *)((char *)base - (Long)tablename) : NULL);

  high_value = (high_value ? (char *)((char *)base - (Long)high_value) : NULL);
  low_value = (low_value ? (char *)((char *)base - (Long)low_value) : NULL);

  // v5, expression text
  v5 = (v5 ? (char *)((char *)base - (Long)v5) : NULL);

  return TrafDesc::unpack(base, reallocator);
}

short TrafHistogramDesc::copyFrom(TrafDesc *from, NAMemory *heap) {
  if (from->nodetype != DESC_HISTOGRAM_TYPE) return -1;

  TrafDesc::copyFrom(from, heap);

  TrafHistogramDesc *tFrom = (TrafHistogramDesc *)from;

  setWasCopied(TRUE);
  // low and high values stored as 4 bytes len followed by data.
  int len = *(int *)tFrom->low_value;
  low_value = new (heap) char[sizeof(len) + len];
  *(int *)low_value = len;
  str_cpy_all(&low_value[sizeof(len)], &tFrom->low_value[sizeof(len)], len);

  len = *(int *)tFrom->high_value;
  high_value = new (heap) char[sizeof(len) + len];
  *(int *)high_value = len;
  str_cpy_all(&high_value[sizeof(len)], &tFrom->high_value[sizeof(len)], len);

  // v5, expression text
  len = *(Int16 *)tFrom->v5;
  v5 = new (heap) char[sizeof(Int16) + len];
  *(Int16 *)v5 = len;
  str_cpy_all(&v5[sizeof(Int16)], &tFrom->v5[sizeof(Int16)], len);

  return 0;
}

void TrafHistogramDesc::deallocMembers(NAMemory *heap) {
  if (NOT wasCopied()) return;

  NADELETEBASIC(tablename, heap);
  NADELETEBASIC(low_value, heap);
  NADELETEBASIC(high_value, heap);
  // v5, expression text
  NADELETEBASIC(v5, heap);

  return TrafDesc::deallocMembers(heap);
}

//////////////////////////////////////////////
// class TrafHistIntervalDesc
//////////////////////////////////////////////
Long TrafHistIntervalDesc::pack(void *space) {
  interval_boundary = (interval_boundary ? (char *)(((Space *)space)->convertToOffset(interval_boundary)) : NULL);

  v5 = (v5 ? (char *)(((Space *)space)->convertToOffset(v5)) : NULL);

  return TrafDesc::pack(space);
}

int TrafHistIntervalDesc::unpack(void *base, void *reallocator) {
  interval_boundary = (interval_boundary ? (char *)((char *)base - (Long)interval_boundary) : NULL);

  v5 = (v5 ? (char *)((char *)base - (Long)v5) : NULL);

  return TrafDesc::unpack(base, reallocator);
}

short TrafHistIntervalDesc::copyFrom(TrafDesc *from, NAMemory *heap) {
  if (from->nodetype != DESC_HIST_INTERVAL_TYPE) return -1;

  TrafDesc::copyFrom(from, heap);

  TrafHistIntervalDesc *tFrom = (TrafHistIntervalDesc *)from;

  setWasCopied(TRUE);
  // interval_boundary and v5 stored as 4 bytes len followed by data.
  int len = *(int *)tFrom->interval_boundary;
  interval_boundary = new (heap) char[sizeof(len) + len];
  *(int *)interval_boundary = len;
  str_cpy_all(&interval_boundary[sizeof(len)], &tFrom->interval_boundary[sizeof(len)], len);

  len = *(int *)tFrom->v5;
  v5 = new (heap) char[sizeof(len) + len];
  *(int *)v5 = len;
  str_cpy_all(&v5[sizeof(len)], &tFrom->v5[sizeof(len)], len);

  return 0;
}

void TrafHistIntervalDesc::deallocMembers(NAMemory *heap) {
  if (NOT wasCopied()) return;

  NADELETEBASIC(interval_boundary, heap);
  NADELETEBASIC(v5, heap);

  return TrafDesc::deallocMembers(heap);
}

//////////////////////////////////////////////
// class TrafIndexesDesc
//////////////////////////////////////////////
Long TrafIndexesDesc::pack(void *space) {
  tablename = (tablename ? (char *)(((Space *)space)->convertToOffset(tablename)) : NULL);
  indexname = (indexname ? (char *)(((Space *)space)->convertToOffset(indexname)) : NULL);
  hbaseSplitClause = (hbaseSplitClause ? (char *)(((Space *)space)->convertToOffset(hbaseSplitClause)) : NULL);
  hbaseCreateOptions = (hbaseCreateOptions ? (char *)(((Space *)space)->convertToOffset(hbaseCreateOptions)) : NULL);

  files_desc.pack(space);
  keys_desc.pack(space);
  non_keys_desc.pack(space);
  hbase_regionkey_desc.pack(space);

  return TrafDesc::pack(space);
}

int TrafIndexesDesc::unpack(void *base, void *reallocator) {
  tablename = (tablename ? (char *)((char *)base - (Long)tablename) : NULL);
  indexname = (indexname ? (char *)((char *)base - (Long)indexname) : NULL);
  hbaseSplitClause = (hbaseSplitClause ? (char *)((char *)base - (Long)hbaseSplitClause) : NULL);
  hbaseCreateOptions = (hbaseCreateOptions ? (char *)((char *)base - (Long)hbaseCreateOptions) : NULL);

  if (files_desc.unpack(base, reallocator)) return -1;
  if (keys_desc.unpack(base, reallocator)) return -1;
  if (non_keys_desc.unpack(base, reallocator)) return -1;
  if (hbase_regionkey_desc.unpack(base, reallocator)) return -1;

  return TrafDesc::unpack(base, reallocator);
}

Long TrafKeysDesc::pack(void *space) {
  keyname = (keyname ? (char *)(((Space *)space)->convertToOffset(keyname)) : NULL);
  hbaseColFam = (hbaseColFam ? (char *)(((Space *)space)->convertToOffset(hbaseColFam)) : NULL);
  hbaseColQual = (hbaseColQual ? (char *)(((Space *)space)->convertToOffset(hbaseColQual)) : NULL);

  return TrafDesc::pack(space);
}

int TrafKeysDesc::unpack(void *base, void *reallocator) {
  keyname = (keyname ? (char *)((char *)base - (Long)keyname) : NULL);
  hbaseColFam = (hbaseColFam ? (char *)((char *)base - (Long)hbaseColFam) : NULL);
  hbaseColQual = (hbaseColQual ? (char *)((char *)base - (Long)hbaseColQual) : NULL);

  return TrafDesc::unpack(base, reallocator);
}

short TrafKeysDesc::copyFrom(TrafDesc *from, NAMemory *heap) {
  if (from->nodetype != DESC_KEYS_TYPE) return -1;

  TrafDesc::copyFrom(from, heap);

  TrafKeysDesc *tFrom = (TrafKeysDesc *)from;

  if (allocAndCopy(tFrom->keyname, keyname, heap)) return -1;

  if (allocAndCopy(tFrom->hbaseColFam, hbaseColFam, heap)) return -1;

  if (allocAndCopy(tFrom->hbaseColQual, hbaseColQual, heap)) return -1;

  return 0;
}

void TrafKeysDesc::deallocMembers(NAMemory *heap) {
  if (NOT wasCopied()) return;

  NADELETEBASIC(keyname, heap);
  NADELETEBASIC(hbaseColFam, heap);
  NADELETEBASIC(hbaseColQual, heap);

  return TrafDesc::deallocMembers(heap);
}

Long TrafLibraryDesc::pack(void *space) {
  libraryName = (libraryName ? (char *)(((Space *)space)->convertToOffset(libraryName)) : NULL);
  libraryFilename = (libraryFilename ? (char *)(((Space *)space)->convertToOffset(libraryFilename)) : NULL);

  return TrafDesc::pack(space);
}

int TrafLibraryDesc::unpack(void *base, void *reallocator) {
  libraryName = (libraryName ? (char *)((char *)base - (Long)libraryName) : NULL);
  libraryFilename = (libraryFilename ? (char *)((char *)base - (Long)libraryFilename) : NULL);

  return TrafDesc::unpack(base, reallocator);
}

Long TrafPartnsDesc::pack(void *space) {
  tablename = (tablename ? (char *)(((Space *)space)->convertToOffset(tablename)) : NULL);
  partitionname = (partitionname ? (char *)(((Space *)space)->convertToOffset(partitionname)) : NULL);
  logicalpartitionname =
      (logicalpartitionname ? (char *)(((Space *)space)->convertToOffset(logicalpartitionname)) : NULL);
  firstkey = (firstkey ? (char *)(((Space *)space)->convertToOffset(firstkey)) : NULL);
  encodedkey = (encodedkey ? (char *)(((Space *)space)->convertToOffset(encodedkey)) : NULL);
  lowKey = (lowKey ? (char *)(((Space *)space)->convertToOffset(lowKey)) : NULL);
  highKey = (highKey ? (char *)(((Space *)space)->convertToOffset(highKey)) : NULL);
  givenname = (givenname ? (char *)(((Space *)space)->convertToOffset(givenname)) : NULL);

  return TrafDesc::pack(space);
}

int TrafPartnsDesc::unpack(void *base, void *reallocator) {
  tablename = (tablename ? (char *)((char *)base - (Long)tablename) : NULL);
  partitionname = (partitionname ? (char *)((char *)base - (Long)partitionname) : NULL);
  logicalpartitionname = (logicalpartitionname ? (char *)((char *)base - (Long)logicalpartitionname) : NULL);
  firstkey = (firstkey ? (char *)((char *)base - (Long)firstkey) : NULL);
  encodedkey = (encodedkey ? (char *)((char *)base - (Long)encodedkey) : NULL);
  lowKey = (lowKey ? (char *)((char *)base - (Long)lowKey) : NULL);
  highKey = (highKey ? (char *)((char *)base - (Long)highKey) : NULL);
  givenname = (givenname ? (char *)((char *)base - (Long)givenname) : NULL);

  return TrafDesc::unpack(base, reallocator);
}

Long TrafRefConstrntsDesc::pack(void *space) {
  constrntname = (constrntname ? (char *)(((Space *)space)->convertToOffset(constrntname)) : NULL);
  tablename = (tablename ? (char *)(((Space *)space)->convertToOffset(tablename)) : NULL);

  return TrafDesc::pack(space);
}

int TrafRefConstrntsDesc::unpack(void *base, void *reallocator) {
  constrntname = (constrntname ? (char *)((char *)base - (Long)constrntname) : NULL);
  tablename = (tablename ? (char *)((char *)base - (Long)tablename) : NULL);

  return TrafDesc::unpack(base, reallocator);
}

Long TrafRoutineDesc::pack(void *space) {
  routineName = (routineName ? (char *)(((Space *)space)->convertToOffset(routineName)) : NULL);
  externalName = (externalName ? (char *)(((Space *)space)->convertToOffset(externalName)) : NULL);
  librarySqlName = (librarySqlName ? (char *)(((Space *)space)->convertToOffset(librarySqlName)) : NULL);
  libraryFileName = (libraryFileName ? (char *)(((Space *)space)->convertToOffset(libraryFileName)) : NULL);
  signature = (signature ? (char *)(((Space *)space)->convertToOffset(signature)) : NULL);
  libBlobHandle = (libBlobHandle ? (char *)(((Space *)space)->convertToOffset(libBlobHandle)) : NULL);
  libSchName = (libSchName ? (char *)(((Space *)space)->convertToOffset(libSchName)) : NULL);
  params.pack(space);

  return TrafDesc::pack(space);
}

int TrafRoutineDesc::unpack(void *base, void *reallocator) {
  routineName = (routineName ? (char *)((char *)base - (Long)routineName) : NULL);
  externalName = (externalName ? (char *)((char *)base - (Long)externalName) : NULL);
  librarySqlName = (librarySqlName ? (char *)((char *)base - (Long)librarySqlName) : NULL);
  libraryFileName = (libraryFileName ? (char *)((char *)base - (Long)libraryFileName) : NULL);
  signature = (signature ? (char *)((char *)base - (Long)signature) : NULL);
  libBlobHandle = (libBlobHandle ? (char *)((char *)base - (Long)libBlobHandle) : NULL);
  libSchName = (libSchName ? (char *)((char *)base - (Long)libSchName) : NULL);
  if (params.unpack(base, reallocator)) return -1;

  return TrafDesc::unpack(base, reallocator);
}

Long TrafSequenceGeneratorDesc::pack(void *space) {
  sgLocation = (sgLocation ? (char *)(((Space *)space)->convertToOffset(sgLocation)) : NULL);

  return TrafDesc::pack(space);
}

int TrafSequenceGeneratorDesc::unpack(void *base, void *reallocator) {
  sgLocation = (sgLocation ? (char *)((char *)base - (Long)sgLocation) : NULL);

  return TrafDesc::unpack(base, reallocator);
}

Long TrafTableDesc::pack(void *space) {
  tablename = (tablename ? (char *)(((Space *)space)->convertToOffset(tablename)) : NULL);
  snapshotName = (snapshotName ? (char *)(((Space *)space)->convertToOffset(snapshotName)) : NULL);
  default_col_fam = (default_col_fam ? (char *)(((Space *)space)->convertToOffset(default_col_fam)) : NULL);
  all_col_fams = (all_col_fams ? (char *)(((Space *)space)->convertToOffset(all_col_fams)) : NULL);

  tableNamespace = (tableNamespace ? (char *)(((Space *)space)->convertToOffset(tableNamespace)) : NULL);

  columns_desc.pack(space);
  indexes_desc.pack(space);
  constrnts_desc.pack(space);
  views_desc.pack(space);
  constrnts_tables_desc.pack(space);
  referenced_tables_desc.pack(space);
  referencing_tables_desc.pack(space);
  table_stats_desc.pack(space);
  files_desc.pack(space);
  hbase_regionkey_desc.pack(space);
  sequence_generator_desc.pack(space);
  priv_desc.pack(space);
  partitionv2_desc.pack(space);

  return TrafDesc::pack(space);
}

int TrafTableDesc::unpack(void *base, void *reallocator) {
  tablename = (tablename ? (char *)((char *)base - (Long)tablename) : NULL);
  snapshotName = (snapshotName ? (char *)((char *)base - (Long)snapshotName) : NULL);
  default_col_fam = (default_col_fam ? (char *)((char *)base - (Long)default_col_fam) : NULL);
  all_col_fams = (all_col_fams ? (char *)((char *)base - (Long)all_col_fams) : NULL);
  tableNamespace = (tableNamespace ? (char *)((char *)base - (Long)tableNamespace) : NULL);

  if (columns_desc.unpack(base, reallocator)) return -1;
  if (indexes_desc.unpack(base, reallocator)) return -1;
  if (constrnts_desc.unpack(base, reallocator)) return -1;
  if (views_desc.unpack(base, reallocator)) return -1;
  if (constrnts_tables_desc.unpack(base, reallocator)) return -1;
  if (referenced_tables_desc.unpack(base, reallocator)) return -1;
  if (referencing_tables_desc.unpack(base, reallocator)) return -1;
  if (table_stats_desc.unpack(base, reallocator)) return -1;
  if (files_desc.unpack(base, reallocator)) return -1;
  if (hbase_regionkey_desc.unpack(base, reallocator)) return -1;
  if (sequence_generator_desc.unpack(base, reallocator)) return -1;
  if (priv_desc.unpack(base, reallocator)) return -1;
  if (partitionv2_desc.unpack(base, reallocator)) return -1;

  return TrafDesc::unpack(base, reallocator);
}

Long TrafTableStatsDesc::pack(void *space) {
  histograms_desc.pack(space);
  hist_interval_desc.pack(space);

  return TrafDesc::pack(space);
}

int TrafTableStatsDesc::unpack(void *base, void *reallocator) {
  if (histograms_desc.unpack(base, reallocator)) return -1;
  if (hist_interval_desc.unpack(base, reallocator)) return -1;

  return TrafDesc::unpack(base, reallocator);
}

short TrafTableStatsDesc::copyFrom(TrafDesc *from, NAMemory *heap) {
  if (from->nodetype != DESC_TABLE_STATS_TYPE) return -1;

  TrafDesc::copyFrom(from, heap);

  TrafTableStatsDesc *tFrom = (TrafTableStatsDesc *)from;

  setWasCopied(TRUE);
  histograms_desc = TrafDesc::copyDescList(tFrom->histograms_desc, heap);

  hist_interval_desc = TrafDesc::copyDescList(tFrom->hist_interval_desc, heap);

  return 0;
}

void TrafTableStatsDesc::deallocMembers(NAMemory *heap) {
  if (NOT wasCopied()) return;

  deleteDescList(histograms_desc, heap);
  deleteDescList(hist_interval_desc, heap);

  return TrafDesc::deallocMembers(heap);
}

void TrafTableStatsDesc::initHistogramCursor() { currHistogramDesc = histograms_desc; }

void TrafTableStatsDesc::initHistintCursor() { currHistintDesc = hist_interval_desc; }

// Fetched values and datatypes must remain in sync with
// values retrieved in ustat/hs_read.cpp, method HSHistogrmCursor::get().
short TrafTableStatsDesc::fetchHistogramCursor(void *histogram_id, void *column_number, void *colcount,
                                               void *interval_count, void *rowcount, void *total_uec, void *stats_time,
                                               void *low_value, void *high_value, void *read_time, void *read_count,
                                               void *sample_secs, void *col_secs, void *sample_percent, void *cv,
                                               void *reason, void *v1, void *v2,
                                               // v5, expression text
                                               void *v5) {
  if (!currHistogramDesc) return 100;  // at EOD

  // return values from currHistogramDesc
  TrafHistogramDesc *h = currHistogramDesc->histogramDesc();

  *(long *)histogram_id = h->histogram_id;
  *(int *)column_number = h->column_number;
  *(int *)colcount = h->colcount;
  *(Int16 *)interval_count = h->interval_count;
  *(long *)rowcount = h->rowcount;
  *(long *)total_uec = h->total_uec;
  *(long *)stats_time = h->stats_time;

  // stored value has format: 4 bytes len plus data
  // returned value has format: 2 bytes len plus data
  short len = 0;
  char *ptr = NULL;
  if (h->low_value) {
    ptr = (char *)low_value;
    len = (Int16)(*(int *)h->low_value);
    *(Int16 *)ptr = len;
    str_cpy_all(&ptr[sizeof(len)], &h->low_value[sizeof(int)], len);
  }

  if (h->high_value) {
    ptr = (char *)high_value;
    len = (Int16)(*(int *)h->high_value);
    *(Int16 *)ptr = len;
    str_cpy_all(&ptr[sizeof(len)], &h->high_value[sizeof(int)], len);
  }

  *(long *)read_time = h->read_time;
  *(Int16 *)read_count = h->read_count;
  *(long *)sample_secs = h->sample_secs;
  *(long *)col_secs = h->col_secs;
  *(Int16 *)sample_percent = h->sample_percent;
  *(Float64 *)cv = h->cv;
  *(char *)reason = h->reason;
  *(long *)v1 = h->v1;
  *(long *)v2 = h->v2;

  // v5, expression text
  if (v5 && h->v5) {
    ptr = (char *)v5;
    len = (Int16)(*(int *)h->v5);
    *(Int16 *)ptr = len;
    str_cpy_all(&ptr[sizeof(Int16)], &h->v5[sizeof(Int16)], len);
  }

  // advance currHistogramDesc
  currHistogramDesc = currHistogramDesc->next;

  return 0;
}

// Fetched values and datatypes must remain in sync with
// values retrieved in ustat/hs_read.cpp, method HSHistintsCursor::get().
short TrafTableStatsDesc::fetchHistintCursor(void *histogram_id, void *interval_number, void *interval_rowcount,
                                             void *interval_uec, void *interval_boundary, void *std_dev_of_freq,
                                             void *v1, void *v2, void *v5) {
  if (!currHistintDesc) return 100;  // at EOD

  // return values from currHistintDesc
  TrafHistIntervalDesc *h = currHistintDesc->histIntervalDesc();

  *(long *)histogram_id = h->histogram_id;
  *(Int16 *)interval_number = h->interval_number;
  *(long *)interval_rowcount = h->interval_rowcount;
  *(long *)interval_uec = h->interval_uec;

  // stored value has format: 4 bytes len plus data
  // returned value has format: 2 bytes len plus data
  short len = 0;
  char *ptr = NULL;
  if (h->interval_boundary) {
    ptr = (char *)interval_boundary;
    len = (Int16)(*(int *)h->interval_boundary);
    *(Int16 *)ptr = len;
    str_cpy_all(&ptr[sizeof(len)], &h->interval_boundary[sizeof(int)], len);
  }

  *(Float64 *)std_dev_of_freq = h->std_dev_of_freq;
  *(long *)v1 = h->v1;
  *(long *)v2 = h->v2;

  if (h->v5) {
    ptr = (char *)v5;
    len = (Int16)(*(int *)h->v5);
    *(Int16 *)ptr = len;
    str_cpy_all(&ptr[sizeof(len)], &h->v5[sizeof(int)], len);
  }

  // advance currHistintDesc
  currHistintDesc = currHistintDesc->next;

  return 0;
}

Long TrafUsingMvDesc::pack(void *space) {
  mvName = (mvName ? (char *)(((Space *)space)->convertToOffset(mvName)) : NULL);

  return TrafDesc::pack(space);
}

int TrafUsingMvDesc::unpack(void *base, void *reallocator) {
  mvName = (mvName ? (char *)((char *)base - (Long)mvName) : NULL);

  return TrafDesc::unpack(base, reallocator);
}

Long TrafViewDesc::pack(void *space) {
  viewname = (viewname ? (char *)(((Space *)space)->convertToOffset(viewname)) : NULL);
  viewfilename = (viewfilename ? (char *)(((Space *)space)->convertToOffset(viewfilename)) : NULL);

  viewtext = (viewtext ? (char *)(((Space *)space)->convertToOffset(viewtext)) : NULL);
  viewchecktext = (viewchecktext ? (char *)(((Space *)space)->convertToOffset(viewchecktext)) : NULL);
  viewcolusages = (viewcolusages ? (char *)(((Space *)space)->convertToOffset(viewcolusages)) : NULL);

  return TrafDesc::pack(space);
}

int TrafViewDesc::unpack(void *base, void *reallocator) {
  viewname = (viewname ? (char *)((char *)base - (Long)viewname) : NULL);
  viewfilename = (viewfilename ? (char *)((char *)base - (Long)viewfilename) : NULL);

  viewtext = (viewtext ? (char *)((char *)base - (Long)viewtext) : NULL);
  viewchecktext = (viewchecktext ? (char *)((char *)base - (Long)viewchecktext) : NULL);
  viewcolusages = (viewcolusages ? (char *)((char *)base - (Long)viewcolusages) : NULL);

  return TrafDesc::unpack(base, reallocator);
}

Long TrafPrivDesc::pack(void *space) {
  privGrantees.pack(space);

  return TrafDesc::pack(space);
}

int TrafPrivDesc::unpack(void *base, void *reallocator) {
  if (privGrantees.unpack(base, reallocator)) return -1;
  return TrafDesc::unpack(base, reallocator);
}

Long TrafPrivGranteeDesc::pack(void *space) {
  schemaBitmap.pack(space);
  objectBitmap.pack(space);
  columnBitmaps.pack(space);

  return TrafDesc::pack(space);
}

int TrafPrivGranteeDesc::unpack(void *base, void *reallocator) {
  if (schemaBitmap.unpack(base, reallocator)) return -1;
  if (objectBitmap.unpack(base, reallocator)) return -1;
  if (columnBitmaps.unpack(base, reallocator)) return -1;

  return TrafDesc::unpack(base, reallocator);
}

Long TrafPartitionV2Desc::pack(void *space) {
  partitionColIdx = (partitionColIdx ? (char *)(((Space *)space)->convertToOffset(partitionColIdx)) : NULL);
  subpartitionColIdx = (subpartitionColIdx ? (char *)(((Space *)space)->convertToOffset(subpartitionColIdx)) : NULL);
  partitionInterval = (partitionInterval ? (char *)(((Space *)space)->convertToOffset(partitionInterval)) : NULL);
  subpartitionInterval =
      (subpartitionInterval ? (char *)(((Space *)space)->convertToOffset(subpartitionInterval)) : NULL);
  part_desc.pack(space);

  return TrafDesc::pack(space);
}

int TrafPartitionV2Desc::unpack(void *base, void *reallocator) {
  partitionColIdx = (partitionColIdx ? (char *)((char *)base - (Long)partitionColIdx) : NULL);
  subpartitionColIdx = (subpartitionColIdx ? (char *)((char *)base - (Long)subpartitionColIdx) : NULL);
  partitionInterval = (partitionInterval ? (char *)((char *)base - (Long)partitionInterval) : NULL);
  subpartitionInterval = (subpartitionInterval ? (char *)((char *)base - (Long)subpartitionInterval) : NULL);

  if (part_desc.unpack(base, reallocator)) return -1;

  return TrafDesc::unpack(base, reallocator);
}

Long TrafPartDesc::pack(void *space) {
  partitionName = (partitionName ? (char *)(((Space *)space)->convertToOffset(partitionName)) : NULL);
  partitionEntityName = (partitionEntityName ? (char *)(((Space *)space)->convertToOffset(partitionEntityName)) : NULL);
  partitionValueExpr = (partitionValueExpr ? (char *)(((Space *)space)->convertToOffset(partitionValueExpr)) : NULL);
  prevPartitionValueExpr =
      (prevPartitionValueExpr ? (char *)(((Space *)space)->convertToOffset(prevPartitionValueExpr)) : NULL);
  subpart_desc.pack(space);

  return TrafDesc::pack(space);
}

int TrafPartDesc::unpack(void *base, void *reallocator) {
  partitionName = (partitionName ? (char *)((char *)base - (Long)partitionName) : NULL);
  partitionEntityName = (partitionEntityName ? (char *)((char *)base - (Long)partitionEntityName) : NULL);
  partitionValueExpr = (partitionValueExpr ? (char *)((char *)base - (Long)partitionValueExpr) : NULL);
  prevPartitionValueExpr = (prevPartitionValueExpr ? (char *)((char *)base - (Long)prevPartitionValueExpr) : NULL);

  if (subpart_desc.unpack(base, reallocator)) return -1;

  return TrafDesc::unpack(base, reallocator);
}
