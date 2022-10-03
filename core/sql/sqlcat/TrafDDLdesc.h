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
#ifndef TRAF_DDL_DESC_H
#define TRAF_DDL_DESC_H

// ****************************************************************************
// This file contains DDLDesc classes. DDLDesc classes are used to store
// object definitions that are read from system and privilege manager metadata.
// The DDLDesc's are referenced by other classes such as NATable and SHOWDDL 
// that save and display metadata contents.  
//
// When DDL operations are performed, the associated DDLDesc structure is 
// flatten out and stored in the text table related to object.  When the
// compiler or DDL subsequently require this information, the flattened DDLDesc 
// is read from the metadata and expanded. This information can then be used 
// by the the different components - such as NATable.
//
// Why are there no initializers for most of these classes? The answer is
// that they are all allocated via the factory function TrafAllocateDDLdesc
// (declared at the end of this file). That function zeroes everything out.
// ****************************************************************************

#include "Platform.h"
#include "NAVersionedObject.h"
#include "charinfo.h"
#include "ComSmallDefs.h"

#define GENHEAP(h)    (h ? (NAMemory*)h : CmpCommon::statementHeap())

enum ConstraintType { UNIQUE_CONSTRAINT, PRIMARY_KEY_CONSTRAINT, REF_CONSTRAINT,
		      CHECK_CONSTRAINT
		    };

enum PartitionType  { NO_PARTITION, HASH_PARTITION, RANGE_PARTITION, LIST_PARTITION };

enum desc_nodetype {
  DESC_UNKNOWN_TYPE              = 0,
  DESC_CHECK_CONSTRNTS_TYPE      = 1,
  DESC_COLUMNS_TYPE              = 2,
  DESC_CONSTRNTS_TYPE            = 3,
  DESC_CONSTRNT_KEY_COLS_TYPE    = 4,
  DESC_FILES_TYPE                = 5,
  DESC_HBASE_RANGE_REGION_TYPE   = 6,
  DESC_HISTOGRAM_TYPE            = 7,
  DESC_HIST_INTERVAL_TYPE        = 8,
  DESC_INDEXES_TYPE              = 9,
  DESC_KEYS_TYPE                 = 10,
  DESC_PARTNS_TYPE               = 11,
  DESC_REF_CONSTRNTS_TYPE        = 12,
  DESC_TABLE_TYPE                = 13,
  DESC_USING_MV_TYPE             = 14,  
  DESC_VIEW_TYPE                 = 15,
  DESC_SCHEMA_LABEL_TYPE         = 16,
  DESC_SEQUENCE_GENERATOR_TYPE   = 17,
  DESC_ROUTINE_TYPE              = 18,
  DESC_LIBRARY_TYPE              = 19,
  DESC_TABLE_STATS_TYPE          = 20,
  DESC_PRIV_TYPE                 = 21,
  DESC_PRIV_GRANTEE_TYPE         = 22,
  DESC_PRIV_BITMAP_TYPE          = 23,
  DESC_PARTITIONV2_TYPE          = 24,
  DESC_PART_TYPE          = 25
};

class TrafDesc;
typedef NAVersionedObjectPtrTempl<TrafDesc> DescStructPtr;

class TrafCheckConstrntsDesc;
class TrafColumnsDesc;
class TrafConstrntsDesc;
class TrafConstrntKeyColsDesc;
class TrafHbaseRegionDesc;
class TrafHistogramDesc;
class TrafHistIntervalDesc;
class TrafFilesDesc;
class TrafKeysDesc;
class TrafIndexesDesc;
class TrafLibraryDesc;
class TrafPartnsDesc;
class TrafRefConstrntsDesc;
class TrafRoutineDesc;
class TrafSequenceGeneratorDesc;
class TrafTableDesc;
class TrafTableStatsDesc;
class TrafUsingMvDesc;
class TrafViewDesc;
class TrafPrivDesc;
class TrafPrivGranteeDesc;
class TrafPrivBitmapDesc;
class TrafPartDesc;
class TrafPartitionV2Desc;

class TrafDesc : public NAVersionedObject {
public:
  enum {CURR_VERSION = 1};

  enum DescFlags
    {
      // if this descriptor was allocated and copied into.
      // Currently used to deallocate copied fields.
      WAS_COPIED = 0x0001
    };

  TrafDesc(UInt16 nodeType);
  TrafDesc() : NAVersionedObject(-1) {}

  void setWasCopied(NABoolean v) 
  {(v ? descFlags |= WAS_COPIED : descFlags &= ~WAS_COPIED); };
  NABoolean wasCopied() { return (descFlags & WAS_COPIED) != 0; };

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID()
  {
    return 1;
  }

  virtual void populateImageVersionIDArray()
  {
    setImageVersionID(0,getClassVersionID());
  }

  virtual short getClassSize()      { return (short)sizeof(TrafDesc); }

  virtual Lng32 migrateToNewVersion(NAVersionedObject *&newImage);

  virtual char *findVTblPtr(short classID);

  virtual Long pack(void *space);
  virtual Lng32 unpack(void * base, void * reallocator);

  virtual short copyFrom(TrafDesc * from, NAMemory * heap);
  virtual void deallocMembers(NAMemory * heap) {}

  // allocate new list on heap and copy source descriptors into it
  static TrafDesc * copyDescList(TrafDesc * srcDescList, NAMemory * heap);
  static void deleteDescList(TrafDesc * srcDescList, NAMemory * heap);
  short allocAndCopy(char * src, char* &tgt, NAMemory * heap);

  Lng32 validateSize();
  Lng32 validateVersion();

  // Deep compare by chasing the next ptr all the way to the end
  NABoolean deepCompare(const TrafDesc& other) const;

  // Shallow compare by comparing the relevant data members in 
  // this and other. The next pointer is not followed.
  virtual NABoolean operator==(const TrafDesc& other) const
  {
     if ( this == &other )
       return TRUE;

     return ( nodetype == other.nodetype &&
              version == other.version &&
              descFlags == other.descFlags &&
              COMPARE_CHAR_PTRS(descExtension, other.descExtension)
            );
  }
 
  UInt16 nodetype;
  UInt16 version;
  UInt32 descFlags;
  DescStructPtr next;
  char* descExtension; // extension of descriptor, if it needs to be extended

  virtual TrafCheckConstrntsDesc *checkConstrntsDesc() const { return NULL; }
  virtual TrafColumnsDesc *columnsDesc() const { return NULL; }
  virtual TrafConstrntsDesc *constrntsDesc() const { return NULL; }
  virtual TrafConstrntKeyColsDesc *constrntKeyColsDesc() const { return NULL; }
  virtual TrafFilesDesc *filesDesc() const { return NULL; }
  virtual TrafHbaseRegionDesc *hbaseRegionDesc() const { return NULL; }
  virtual TrafHistogramDesc *histogramDesc() const { return NULL; }
  virtual TrafHistIntervalDesc *histIntervalDesc() const { return NULL; }
  virtual TrafKeysDesc *keysDesc() const { return NULL; }
  virtual TrafIndexesDesc *indexesDesc() const { return NULL; }
  virtual TrafLibraryDesc *libraryDesc() const { return NULL; }
  virtual TrafPartnsDesc *partnsDesc() const { return NULL; }
  virtual TrafRefConstrntsDesc *refConstrntsDesc() const { return NULL; }
  virtual TrafRoutineDesc *routineDesc() const { return NULL; }
  virtual TrafSequenceGeneratorDesc *sequenceGeneratorDesc() const { return NULL; }
  virtual TrafTableDesc *tableDesc() const { return NULL; }
  virtual TrafTableStatsDesc *tableStatsDesc() const { return NULL; }
  virtual TrafUsingMvDesc *usingMvDesc() const { return NULL; }
  virtual TrafViewDesc *viewDesc() const { return NULL; }
  virtual TrafPrivDesc *privDesc() const { return NULL; }
  virtual TrafPrivGranteeDesc *privGranteeDesc() const { return NULL; }
  virtual TrafPrivBitmapDesc *privBitmapDesc() const { return NULL; }
  virtual TrafPartitionV2Desc *partitionV2Desc() const { return NULL; }
  virtual TrafPartDesc *partDesc() const { return NULL; }

};

class TrafCheckConstrntsDesc : public TrafDesc {
public:
  // why almost no initializers? see note at top of file
  TrafCheckConstrntsDesc() : TrafDesc(DESC_CHECK_CONSTRNTS_TYPE)
  {}

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID()
  {
    return 1;
  }

  virtual void populateImageVersionIDArray()
  {
    setImageVersionID(0,getClassVersionID());
  }

  virtual short getClassSize()      { return (short)sizeof(TrafCheckConstrntsDesc); }
 
  virtual Long pack(void *space);
  virtual Lng32 unpack(void * base, void * reallocator);

  virtual TrafCheckConstrntsDesc *checkConstrntsDesc() const { return (TrafCheckConstrntsDesc*)this; }

  NABoolean operator==(const TrafCheckConstrntsDesc& other) const
  {
     return (TrafDesc::operator==(other) &&
             COMPARE_CHAR_PTRS(constrnt_text, other.constrnt_text));
  }

  char* constrnt_text;
  char filler[16];
};

class TrafColumnsDesc : public TrafDesc {
public:
  // why almost no initializers? see note at top of file
  TrafColumnsDesc() : TrafDesc(DESC_COLUMNS_TYPE) 
  {};

  virtual void deallocMembers(NAMemory * heap);

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID()
  {
    return 1;
  }

  virtual void populateImageVersionIDArray()
  {
    setImageVersionID(0,getClassVersionID());
  }

  virtual short getClassSize()      { return (short)sizeof(TrafColumnsDesc); }
 
  virtual Long pack(void *space);
  virtual Lng32 unpack(void * base, void * reallocator);

  virtual short copyFrom(TrafDesc * from, NAMemory * heap);

  virtual TrafColumnsDesc *columnsDesc() const { return (TrafColumnsDesc*)this; }

  enum ColumnsDescFlags
    { 
      NULLABLE           = 0x0001,  
      ADDED              = 0x0002,
      UPSHIFTED          = 0x0004,
      CASEINSENSITIVE    = 0x0008,
      OPTIONAL           = 0x0010
    };

  void setNullable(NABoolean v) 
  {(v ? columnsDescFlags |= NULLABLE : columnsDescFlags &= ~NULLABLE); };
  NABoolean isNullable() { return (columnsDescFlags & NULLABLE) != 0; };

  void setAdded(NABoolean v) 
  {(v ? columnsDescFlags |= ADDED : columnsDescFlags &= ~ADDED); };
  NABoolean isAdded() { return (columnsDescFlags & ADDED) != 0; };

  void setUpshifted(NABoolean v) 
  {(v ? columnsDescFlags |= UPSHIFTED : columnsDescFlags &= ~UPSHIFTED); };
  NABoolean isUpshifted() { return (columnsDescFlags & UPSHIFTED) != 0; };

  void setCaseInsensitive(NABoolean v) 
  {(v ? columnsDescFlags |= CASEINSENSITIVE : columnsDescFlags &= ~CASEINSENSITIVE); };
  NABoolean isCaseInsensitive() { return (columnsDescFlags & CASEINSENSITIVE) != 0; };

  void setOptional(NABoolean v) 
  {(v ? columnsDescFlags |= OPTIONAL : columnsDescFlags &= ~OPTIONAL); };
  NABoolean isOptional() { return (columnsDescFlags & OPTIONAL) != 0; };

  rec_datetime_field datetimeStart() 
  { return (rec_datetime_field)datetimestart;}
  rec_datetime_field datetimeEnd() 
  { return (rec_datetime_field)datetimeend;}

  ComColumnDefaultClass defaultClass() 
  { return (ComColumnDefaultClass)defaultClass_;}
  void setDefaultClass(ComColumnDefaultClass v)
  { defaultClass_ = (Int16)v;}

  CharInfo::CharSet characterSet() 
  { return (CharInfo::CharSet)character_set;}
  CharInfo::CharSet encodingCharset() 
  { return (CharInfo::CharSet)encoding_charset;}
  CharInfo::Collation  collationSequence()
  {return (CharInfo::Collation)collation_sequence; }

  ComParamDirection paramDirection() 
  { return (ComParamDirection)paramDirection_;}
  void setParamDirection(ComParamDirection v)
  {paramDirection_ = (Int16)v; }

  Int64 getColFlags()
  {return colFlags;}

  NABoolean operator==(const TrafColumnsDesc& other) const
  {
      return (TrafDesc::operator==(other) &&
             COMPARE_CHAR_PTRS(colname, other.colname) &&
             colnumber == other.colnumber &&
             datatype == other.datatype &&
             offset == other.offset &&
             length == other.length &&
             scale == other. scale &&
             precision == other. precision &&
             datetimestart == other.datetimestart &&
             datetimeend == other.datetimeend &&
             datetimefractprec == other.datetimefractprec &&
             intervalleadingprec == other.intervalleadingprec &&

             defaultClass_ == other.defaultClass_ &&
             character_set == other.character_set &&
             encoding_charset == other.encoding_charset &&
             collation_sequence == other.collation_sequence &&

             hbaseColFlags == other.hbaseColFlags &&
             paramDirection_ == other.paramDirection_ &&
             colclass == other.colclass &&
             colFlags == other.colFlags &&
             columnsDescFlags == other.columnsDescFlags &&

             COMPARE_CHAR_PTRS(pictureText, other.pictureText) &&
             COMPARE_CHAR_PTRS(defaultvalue, other.defaultvalue) &&
             COMPARE_CHAR_PTRS(initDefaultValue, other.initDefaultValue) &&
             COMPARE_CHAR_PTRS(heading, other.heading) &&
             COMPARE_CHAR_PTRS(computed_column_text, other.computed_column_text) &&
             COMPARE_CHAR_PTRS(hbaseColFam, other.hbaseColFam) &&
             COMPARE_CHAR_PTRS(hbaseColQual, other.hbaseColQual) &&
             COMPARE_CHAR_PTRS(compDefnStr, other.compDefnStr)
           );
  }

  char* colname;

  Int32 colnumber;
  Int32 datatype;

  Int32 offset;
  Lng32 length;

  Lng32 scale;
  Lng32 precision;

  Int16/*rec_datetime_field*/ datetimestart, datetimeend;
  Int16 datetimefractprec, intervalleadingprec;

  Int16/*ComColumnDefaultClass*/ defaultClass_;
  Int16/*CharInfo::CharSet*/     character_set;
  Int16/*CharInfo::CharSet*/     encoding_charset;
  Int16/*CharInfo::Collation*/   collation_sequence;

  ULng32 hbaseColFlags;
  Int16/*ComParamDirection*/ paramDirection_;
  char colclass; // 'S' -- system generated, 'U' -- user created
  char filler0;

  Int64 colFlags;
  Int64 columnsDescFlags; // my flags

  char* pictureText;
  char* defaultvalue;
  char* initDefaultValue;
  char* heading;
  char* computed_column_text;
  char* hbaseColFam;
  char* hbaseColQual;

  // datatype definition string for composite (array/row) columns
  char* compDefnStr;

  char filler[16];
};

class TrafConstrntKeyColsDesc : public TrafDesc {
public:
  enum ConsrntKeyDescFlags
    { 
      SYSTEM_KEY   = 0x0001
    };
  // why almost no initializers? see note at top of file
  TrafConstrntKeyColsDesc() : TrafDesc(DESC_CONSTRNT_KEY_COLS_TYPE)
  {
    constrntKeyColsDescFlags = 0;
  }

  void setSystemKey(NABoolean v) 
  {(v ? constrntKeyColsDescFlags |= SYSTEM_KEY: constrntKeyColsDescFlags&= ~SYSTEM_KEY); };
  NABoolean isSystemKey() { return (constrntKeyColsDescFlags & SYSTEM_KEY) != 0; };

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID()
  {
    return 1;
  }

  virtual void populateImageVersionIDArray()
  {
    setImageVersionID(0,getClassVersionID());
  }

  virtual short getClassSize()      { return (short)sizeof(TrafConstrntKeyColsDesc); }
 
  virtual Long pack(void *space);
  virtual Lng32 unpack(void * base, void * reallocator);

  virtual TrafConstrntKeyColsDesc *constrntKeyColsDesc() const { return (TrafConstrntKeyColsDesc*)this; }

  NABoolean operator==(const TrafConstrntKeyColsDesc& other) const
  {
      return (TrafDesc::operator==(other) &&
              COMPARE_CHAR_PTRS(colname, other.colname) &&
              position == other.position &&
              constrntKeyColsDescFlags == other.constrntKeyColsDescFlags);
  }

  char* colname;
  Int32  position;

  Int64 constrntKeyColsDescFlags; // my flags

  char filler[16];
};

class TrafConstrntsDesc : public TrafDesc {
public:
  // why almost no initializers? see note at top of file
  TrafConstrntsDesc() : TrafDesc(DESC_CONSTRNTS_TYPE)
  {}

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID()
  {
    return 1;
  }

  virtual void populateImageVersionIDArray()
  {
    setImageVersionID(0,getClassVersionID());
  }

  virtual short getClassSize()      { return (short)sizeof(TrafConstrntsDesc); }
 
  virtual Long pack(void *space);
  virtual Lng32 unpack(void * base, void * reallocator);

  virtual TrafConstrntsDesc *constrntsDesc() const { return (TrafConstrntsDesc*)this; }

  enum ConstrntsDescFlags
    { 
      ENFORCED           = 0x0001,
      NOT_SERIALIZED     = 0x0002
    };

  void setEnforced(NABoolean v) 
  {(v ? constrntsDescFlags |= ENFORCED : constrntsDescFlags &= ~ENFORCED); };
  NABoolean isEnforced() { return (constrntsDescFlags & ENFORCED) != 0; };

  void setNotSerialized(NABoolean v) 
  {(v ? constrntsDescFlags |= NOT_SERIALIZED : constrntsDescFlags &= ~NOT_SERIALIZED); };
  NABoolean notSerialized() { return (constrntsDescFlags & NOT_SERIALIZED) != 0; };
  NABoolean operator==(const TrafConstrntsDesc& other) const
  {
      return (TrafDesc::operator==(other) &&
              COMPARE_CHAR_PTRS(constrntname, other.constrntname) &&
              COMPARE_CHAR_PTRS(tablename, other.tablename) &&
              type == other.type &&
              colcount == other.colcount &&
              constrntsDescFlags == other.constrntsDescFlags &&
              COMPARE_DESC_SMART_PTR(check_constrnts_desc, other.check_constrnts_desc) &&
              COMPARE_DESC_SMART_PTR(constr_key_cols_desc, constr_key_cols_desc) &&
              COMPARE_DESC_SMART_PTR(referenced_constrnts_desc, other.referenced_constrnts_desc) &&
              COMPARE_DESC_SMART_PTR(referencing_constrnts_desc, other.referencing_constrnts_desc) 
             );
  }

  char* constrntname;
  char* tablename;

  Int16 /*ConstraintType*/ type;
  Int16 fillerInt16;
  Int32  colcount;

  Int64 constrntsDescFlags; // my flags

  DescStructPtr check_constrnts_desc;
  DescStructPtr constr_key_cols_desc;
  DescStructPtr referenced_constrnts_desc;
  DescStructPtr referencing_constrnts_desc;

  char filler[24];
};

class TrafFilesDesc : public TrafDesc {
public:
  // why almost no initializers? see note at top of file
  TrafFilesDesc() : TrafDesc(DESC_FILES_TYPE)
  {}

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID()
  {
    return 1;
  }

  virtual void populateImageVersionIDArray()
  {
    setImageVersionID(0,getClassVersionID());
  }

  virtual short getClassSize()      { return (short)sizeof(TrafFilesDesc); }
 
  virtual Long pack(void *space);
  virtual Lng32 unpack(void * base, void * reallocator);

  virtual TrafFilesDesc *filesDesc() const { return (TrafFilesDesc*)this; }

  enum FilesDescFlags
    { 
      AUDITED           = 0x0001
    };

  void setAudited(NABoolean v) 
  {(v ? filesDescFlags |= AUDITED : filesDescFlags &= ~AUDITED); };
  NABoolean isAudited() { return (filesDescFlags & AUDITED) != 0; };

  NABoolean operator==(const TrafFilesDesc& other) const
  {
      return (TrafDesc::operator==(other) &&
              filesDescFlags == other.filesDescFlags &&
              COMPARE_DESC_SMART_PTR(partns_desc, other.partns_desc));
  }

  Int64 filesDescFlags; // my flags
  DescStructPtr partns_desc;
};

class TrafHbaseRegionDesc : public TrafDesc {
public:
  // why almost no initializers? see note at top of file
  TrafHbaseRegionDesc() : TrafDesc(DESC_HBASE_RANGE_REGION_TYPE)
  {}

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID()
  {
    return 1;
  }

  virtual void populateImageVersionIDArray()
  {
    setImageVersionID(0,getClassVersionID());
  }

  virtual short getClassSize()      { return (short)sizeof(TrafHbaseRegionDesc); }
 
  virtual Long pack(void *space);
  virtual Lng32 unpack(void * base, void * reallocator);

  virtual TrafHbaseRegionDesc *hbaseRegionDesc() const { return (TrafHbaseRegionDesc*)this; }

  NABoolean operator==(const TrafHbaseRegionDesc& other) const
  {
      return (TrafDesc::operator==(other) &&
              hbaseRegionDescFlags == other.hbaseRegionDescFlags &&
              COMPARE_VOID_PTRS(beginKey, beginKeyLen, 
                                other.beginKey, other.beginKeyLen) &&
              COMPARE_VOID_PTRS(endKey, endKeyLen, 
                                other.endKey, other.endKeyLen)
             );
  }

  Int64  hbaseRegionDescFlags; // my flags

  Lng32  beginKeyLen;
  Lng32  endKeyLen;

  char*  beginKey;
  char*  endKey;

  char filler[16];
};

class TrafHistogramDesc : public TrafDesc {
public:
  // why almost no initializers? see note at top of file
  TrafHistogramDesc() : TrafDesc(DESC_HISTOGRAM_TYPE)
  {}

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID()
  {
    return 1;
  }

  virtual void populateImageVersionIDArray()
  {
    setImageVersionID(0,getClassVersionID());
  }

  virtual short getClassSize()      { return (short)sizeof(TrafHistogramDesc); }
 
  virtual Long pack(void *space);
  virtual Lng32 unpack(void * base, void * reallocator);

  virtual short copyFrom(TrafDesc * from, NAMemory * heap);
  virtual void deallocMembers(NAMemory * heap);

  virtual TrafHistogramDesc *histogramDesc() const { return (TrafHistogramDesc*)this; }

  NABoolean operator==(const TrafHistogramDesc& other) const
  {
      return (TrafDesc::operator==(other) &&
              COMPARE_CHAR_PTRS(tablename, other.tablename) &&
              table_uid == other.table_uid &&
              histogram_id == other.histogram_id &&
              col_position == other.col_position &&
              column_number == other.column_number &&
              colcount == other.colcount &&
              interval_count == other.interval_count &&
              rowcount == other.rowcount &&
              total_uec == other.total_uec &&
              COMPARE_CHAR_PTRS(low_value, other.low_value) &&
              COMPARE_CHAR_PTRS(high_value, other.high_value) &&
              read_time == other.read_time &&
              sample_secs == other.sample_secs &&
              col_secs == other.col_secs &&
              read_count == other.read_count &&
              sample_percent == other.sample_percent &&
              reason == other.reason &&
              cv == other.cv &&
              v1 == other.v1 &&
              v2 == other.v2 &&
              COMPARE_CHAR_PTRS(v5, other.v5) &&
              histogramDescFlags == other.histogramDescFlags
           );
  }

  char*   tablename;
  Int64   table_uid;
  Int64   histogram_id;

  Int32   col_position;
  Int32   column_number;
  Int32   colcount;
  Int32   interval_count;

  Int64   rowcount;
  Int64   total_uec;
  Int64   stats_time;
  char *  low_value;
  char *  high_value;

  Int64   read_time;
  Int64   sample_secs;
  Int64   col_secs;

  Int32   read_count;
  Int16   sample_percent;
  Int16   reason;

  Float64 cv;

  Int64   v1;
  Int64   v2;

  // expressionText_ comes from the V5 column; allow up to 4 bytes per char UCS2 -> UTF8
  // and 2 bytes for varchar len field and 1 byte to add a null terminator
  char *  v5;
  
  Int64 histogramDescFlags; // my flags

  char filler[128];
};

class TrafHistIntervalDesc : public TrafDesc {
public:
  // why almost no initializers? see note at top of file
  TrafHistIntervalDesc() : TrafDesc(DESC_HIST_INTERVAL_TYPE)
  {}

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID()
  {
    return 1;
  }

  virtual void populateImageVersionIDArray()
  {
    setImageVersionID(0,getClassVersionID());
  }

  virtual short getClassSize()      { return (short)sizeof(TrafHistIntervalDesc); }
 
  virtual Long pack(void *space);
  virtual Lng32 unpack(void * base, void * reallocator);

  virtual short copyFrom(TrafDesc * from, NAMemory * heap);
  virtual void deallocMembers(NAMemory * heap);

  virtual TrafHistIntervalDesc *histIntervalDesc() const { return (TrafHistIntervalDesc*)this; }

  NABoolean operator==(const TrafHistIntervalDesc& other) const
  {
      return (TrafDesc::operator==(other) &&
              histogram_id == other.histogram_id &&
              interval_number == other.interval_number &&
              interval_rowcount == other.interval_rowcount &&
              interval_uec == other.interval_uec &&
              COMPARE_CHAR_PTRS(interval_boundary, other.interval_boundary) &&
              std_dev_of_freq == other.std_dev_of_freq &&
              v1 == other.v1 &&
              v2 == other.v2 &&
              COMPARE_CHAR_PTRS(v5, other.v5) &&
              histIntervalDescFlags == other.histIntervalDescFlags
            );
  }


  Int64   histogram_id;

  Int32   interval_number;
  Int32   fillerInt32;

  Int64   interval_rowcount;
  Int64   interval_uec;

  char *  interval_boundary;
  Float64 std_dev_of_freq;

  Int64   v1;
  Int64   v2;

  char *  v5;

  Int64 histIntervalDescFlags; // my flags

  char filler[48];
};

class TrafIndexesDesc : public TrafDesc {
public:
  // why almost no initializers? see note at top of file
  TrafIndexesDesc() : TrafDesc(DESC_INDEXES_TYPE)
  {}

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID()
  {
    return 1;
  }

  virtual void populateImageVersionIDArray()
  {
    setImageVersionID(0,getClassVersionID());
  }

  virtual short getClassSize()      { return (short)sizeof(TrafIndexesDesc); }
 
  virtual Long pack(void *space);
  virtual Lng32 unpack(void * base, void * reallocator);

  virtual TrafIndexesDesc *indexesDesc() const { return (TrafIndexesDesc*)this; }

  // for ngram
  enum IndexesDescFlags
    { 
      SYSTEM_TABLE_CODE = 0x0001,  
      EXPLICIT          = 0x0002,
      VOLATILE          = 0x0004,
      IN_MEM_OBJ        = 0x0008,
      UNIQUE            = 0x0010,
      NGRAM             = 0x0020,
      PART_LOCAL_BASE   = 0x0040,
      PART_LOCAL        = 0x0080,
      PART_GLOBAL       = 0x0100,
    };

  void setSystemTableCode(NABoolean v) 
  {(v ? indexesDescFlags |= SYSTEM_TABLE_CODE : indexesDescFlags &= ~SYSTEM_TABLE_CODE); };
  NABoolean isSystemTableCode() { return (indexesDescFlags & SYSTEM_TABLE_CODE) != 0; };

  void setExplicit(NABoolean v) 
  {(v ? indexesDescFlags |= EXPLICIT : indexesDescFlags &= ~EXPLICIT); };
  NABoolean isExplicit() { return (indexesDescFlags & EXPLICIT) != 0; };

  void setVolatile(NABoolean v) 
  {(v ? indexesDescFlags |= VOLATILE : indexesDescFlags &= ~VOLATILE); };
  NABoolean isVolatile() { return (indexesDescFlags & VOLATILE) != 0; };

  void setInMemoryObject(NABoolean v) 
  {(v ? indexesDescFlags |= IN_MEM_OBJ : indexesDescFlags &= ~IN_MEM_OBJ); };
  NABoolean isInMemoryObject() { return (indexesDescFlags & IN_MEM_OBJ) != 0; };

  void setUnique(NABoolean v) 
  {(v ? indexesDescFlags |= UNIQUE : indexesDescFlags &= ~UNIQUE); };
  NABoolean isUnique() { return (indexesDescFlags & UNIQUE) != 0; };

  // for ngram
  void setNgram(NABoolean v) 
  {(v ? indexesDescFlags |= NGRAM : indexesDescFlags &= ~NGRAM); };
  NABoolean isNgram() { return (indexesDescFlags & NGRAM) != 0; };

  void setPartLocalBaseIndex(NABoolean v) 
  {(v ? indexesDescFlags |= PART_LOCAL_BASE : indexesDescFlags &= ~PART_LOCAL_BASE); };
  NABoolean isPartLocalBaseIndex() { return (indexesDescFlags & PART_LOCAL_BASE) != 0; };

  void setPartLocalIndex(NABoolean v) 
  {(v ? indexesDescFlags |= PART_LOCAL : indexesDescFlags &= ~PART_LOCAL); };
  NABoolean isPartLocalIndex() { return (indexesDescFlags & PART_LOCAL) != 0; };

  void setPartGlobalIndex(NABoolean v) 
  {(v ? indexesDescFlags |= PART_GLOBAL : indexesDescFlags &= ~PART_GLOBAL); };
  NABoolean isPartGlobalIndex() { return (indexesDescFlags & PART_GLOBAL) != 0; };
  
  ComPartitioningScheme partitioningScheme() 
  { return (ComPartitioningScheme)partitioningScheme_; }
  void setPartitioningScheme(ComPartitioningScheme v) 
  { partitioningScheme_ = (Int16)v; }
  ComRowFormat rowFormat() { return (ComRowFormat)rowFormat_; }
  void setRowFormat(ComRowFormat v) { rowFormat_ = (Int16)v; }

  NABoolean operator==(const TrafIndexesDesc& other) const
  {
      return (TrafDesc::operator==(other) &&
              COMPARE_CHAR_PTRS(tablename, other.tablename) &&
              COMPARE_CHAR_PTRS(indexname, other.indexname) &&
              indexUID == other.indexUID &&
              keytag == other.keytag &&
              record_length == other.record_length &&
              colcount == other.colcount &&
              blocksize == other.blocksize &&
              partitioningScheme_ == other.partitioningScheme_ &&
              rowFormat_ == other.rowFormat_ &&
              numInitialSaltRegions == other.numInitialSaltRegions &&
              numReplicas == other.numReplicas &&
              indexesDescFlags == other.indexesDescFlags &&
              COMPARE_CHAR_PTRS(hbaseSplitClause, other.hbaseSplitClause) &&
              COMPARE_CHAR_PTRS(hbaseCreateOptions, other.hbaseCreateOptions) &&
              COMPARE_DESC_SMART_PTR(files_desc, other.files_desc) &&
              COMPARE_DESC_SMART_PTR(keys_desc, other.keys_desc) &&
              COMPARE_DESC_SMART_PTR(non_keys_desc, other.non_keys_desc) &&
              COMPARE_DESC_SMART_PTR(hbase_regionkey_desc, other.hbase_regionkey_desc)
         );
  }

  char* tablename;  // name of the base table
  char* indexname;  // physical name of index. Different from ext_indexname
                    // for ARK tables.
  Int64 indexUID;

  Int32 keytag;
  Int32 record_length;
  Int32 colcount;
  Int32 blocksize;

  Int16 /*ComPartitioningScheme*/ partitioningScheme_; 
  Int16 /*ComRowFormat*/          rowFormat_;
  Lng32 numSaltPartns; // number of salted partns created for a seabase table.

  Lng32 numInitialSaltRegions; // initial # of regions created for salted table
  Int16 numReplicas;
  char filler0[2];

  Int64 indexesDescFlags; // my flags

  char*  hbaseSplitClause;
  char*  hbaseCreateOptions;

  DescStructPtr files_desc;

  // Clustering keys columns
  DescStructPtr keys_desc;

  // Columns that are not part of the clustering key.
  // Used specially for vertical partition column(s).
  DescStructPtr non_keys_desc;

  // for hbase's region keys
  DescStructPtr hbase_regionkey_desc;

  char filler[16];
};

class TrafKeysDesc : public TrafDesc {
public:
  // why almost no initializers? see note at top of file
  TrafKeysDesc() : TrafDesc(DESC_KEYS_TYPE)
  {}

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID()
  {
    return 1;
  }

  virtual void populateImageVersionIDArray()
  {
    setImageVersionID(0,getClassVersionID());
  }

  virtual short getClassSize()      { return (short)sizeof(TrafKeysDesc); }
 
  virtual Long pack(void *space);
  virtual Lng32 unpack(void * base, void * reallocator);

  virtual short copyFrom(TrafDesc * from, NAMemory * heap);
  virtual void deallocMembers(NAMemory * heap);

  virtual TrafKeysDesc *keysDesc() const { return (TrafKeysDesc*)this; }

  enum KeysDescFlags
    { 
      DESCENDING           = 0x0001,
      ADDNL_COL            = 0x0002
    };

  void setDescending(NABoolean v) 
  {(v ? keysDescFlags |= DESCENDING : keysDescFlags &= ~DESCENDING); };
  NABoolean isDescending() { return (keysDescFlags & DESCENDING) != 0; };

  void setAddnlCol(NABoolean v) 
  {(v ? keysDescFlags |= ADDNL_COL : keysDescFlags &= ~ADDNL_COL); };
  NABoolean isAddnlCol() { return (keysDescFlags & ADDNL_COL) != 0; };

  NABoolean operator==(const TrafKeysDesc& other) const
  {
      return (TrafDesc::operator==(other) &&
              COMPARE_CHAR_PTRS(keyname, other.keyname) &&
              keyseqnumber == other.keyseqnumber &&
              tablecolnumber == other.tablecolnumber &&
              keysDescFlags == other.keysDescFlags &&
              COMPARE_CHAR_PTRS(hbaseColFam, other.hbaseColFam) &&
              COMPARE_CHAR_PTRS(hbaseColQual, other.hbaseColQual) 
            );
  }

  char* keyname;
  Int32 keyseqnumber;
  Int32 tablecolnumber;

  Int64 keysDescFlags; // my flags

  char* hbaseColFam;
  char* hbaseColQual;

  char filler[16];
};

class TrafLibraryDesc : public TrafDesc {
public:
  // why almost no initializers? see note at top of file
  TrafLibraryDesc() : TrafDesc(DESC_LIBRARY_TYPE)
  {}

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID()
  {
    return 1;
  }

  virtual void populateImageVersionIDArray()
  {
    setImageVersionID(0,getClassVersionID());
  }

  virtual short getClassSize()      { return (short)sizeof(TrafLibraryDesc); }
 
  virtual Long pack(void *space);
  virtual Lng32 unpack(void * base, void * reallocator);

  virtual TrafLibraryDesc *libraryDesc() const { return (TrafLibraryDesc*)this; }

  NABoolean operator==(const TrafLibraryDesc& other) const
  {
      return (TrafDesc::operator==(other) &&
              COMPARE_CHAR_PTRS(libraryName, other.libraryName) &&
              COMPARE_CHAR_PTRS(libraryFilename, other.libraryFilename) &&
              libraryUID == other.libraryUID &&
              libraryVersion == other.libraryVersion &&
              libraryOwnerID == other.libraryOwnerID &&
              librarySchemaOwnerID == other.librarySchemaOwnerID
            );
  }

  char* libraryName;
  char* libraryFilename;
  Int64 libraryUID;
  Int32 libraryVersion;
  Int32 libraryOwnerID;
  Int32 librarySchemaOwnerID;

  char filler[20];
};

class TrafPartnsDesc : public TrafDesc {
public:
  // why almost no initializers? see note at top of file
  TrafPartnsDesc() : TrafDesc(DESC_PARTNS_TYPE)
  {}

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID()
  {
    return 1;
  }

  virtual void populateImageVersionIDArray()
  {
    setImageVersionID(0,getClassVersionID());
  }

  virtual short getClassSize()      { return (short)sizeof(TrafPartnsDesc); }
 
  virtual Long pack(void *space);
  virtual Lng32 unpack(void * base, void * reallocator);

  virtual TrafPartnsDesc *partnsDesc() const { return (TrafPartnsDesc*)this; }

  NABoolean operator==(const TrafPartnsDesc& other) const
  {
      return (TrafDesc::operator==(other) &&
              COMPARE_CHAR_PTRS(tablename, other.tablename) &&
              primarypartition == other.primarypartition &&
              COMPARE_CHAR_PTRS(partitionname, other.partitionname) &&
              COMPARE_CHAR_PTRS(logicalpartitionname, other.logicalpartitionname) &&
              COMPARE_VOID_PTRS(firstkey, firstkeylen, 
                                other.firstkey, other.firstkeylen) &&
              COMPARE_VOID_PTRS(encodedkey, encodedkeylen, 
                                other.encodedkey, other.encodedkeylen) &&
              COMPARE_CHAR_PTRS(lowKey, other.lowKey) &&
              COMPARE_CHAR_PTRS(highKey, other.highKey) &&
              indexlevel == other.indexlevel &&
              priExt == other.priExt &&
              secExt == other.secExt &&
              maxExt == other.maxExt &&
              COMPARE_CHAR_PTRS(givenname, other.givenname)
             );
  }

  char*  tablename;
  Int32 primarypartition;
  char*  partitionname;
  char*  logicalpartitionname;
  char*  firstkey;
  Lng32 firstkeylen;         //soln:10-031112-1256
  Lng32 encodedkeylen;
  char*  encodedkey;
  char*  lowKey;
  char*  highKey;
  Int32    indexlevel;
  Int32  priExt;
  Int32  secExt;
  Int32  maxExt;
  char*  givenname;
};

class TrafRefConstrntsDesc : public TrafDesc {
public:
  // why almost no initializers? see note at top of file
  TrafRefConstrntsDesc() : TrafDesc(DESC_REF_CONSTRNTS_TYPE)
  {}

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID()
  {
    return 1;
  }

  virtual void populateImageVersionIDArray()
  {
    setImageVersionID(0,getClassVersionID());
  }

  virtual short getClassSize()      { return (short)sizeof(TrafRefConstrntsDesc); }
 
  virtual Long pack(void *space);
  virtual Lng32 unpack(void * base, void * reallocator);

  virtual TrafRefConstrntsDesc *refConstrntsDesc() const { return (TrafRefConstrntsDesc*)this; }

  NABoolean operator==(const TrafRefConstrntsDesc& other) const
  {
      return (TrafDesc::operator==(other) &&
              refConstrntsDescFlags == other.refConstrntsDescFlags &&
              COMPARE_CHAR_PTRS(constrntname, other.constrntname) &&
              COMPARE_CHAR_PTRS(tablename, other.tablename) 
             );
  }

  Int64 refConstrntsDescFlags; // my flags
  char* constrntname;
  char* tablename;

  char filler[16];
};

class TrafRoutineDesc : public TrafDesc {
public:
  // why almost no initializers? see note at top of file
  TrafRoutineDesc() : TrafDesc(DESC_ROUTINE_TYPE)
  {}

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID()
  {
    return 1;
  }

  virtual void populateImageVersionIDArray()
  {
    setImageVersionID(0,getClassVersionID());
  }

  virtual short getClassSize()      { return (short)sizeof(TrafRoutineDesc); }
 
  virtual Long pack(void *space);
  virtual Lng32 unpack(void * base, void * reallocator);

  virtual TrafRoutineDesc *routineDesc() const { return (TrafRoutineDesc*)this; }

  NABoolean operator==(const TrafRoutineDesc& other) const
  {
      return (TrafDesc::operator==(other) &&
              objectUID == other.objectUID &&
              COMPARE_CHAR_PTRS(routineName, other.routineName) &&
              COMPARE_CHAR_PTRS(externalName, other.externalName) &&
              COMPARE_CHAR_PTRS(librarySqlName, other.librarySqlName) &&
              COMPARE_CHAR_PTRS(libraryFileName, other.libraryFileName) &&
              COMPARE_CHAR_PTRS(signature, other.signature) &&
              paramsCount == other.paramsCount &&
              COMPARE_DESC_SMART_PTR(params, other.params) &&
              language == other.language &&
              UDRType == other. UDRType &&
              sqlAccess == other.sqlAccess &&
              transactionAttributes == other.transactionAttributes &&
              maxResults == other.maxResults &&
              paramStyle == other.paramStyle &&
              isDeterministic == other.isDeterministic &&
              isCallOnNull == other.isCallOnNull &&
              isIsolate == other.isIsolate &&
              externalSecurity == other.externalSecurity &&
              executionMode == other.executionMode &&
              stateAreaSize == other.stateAreaSize &&
              parallelism == other.parallelism &&
              owner == other.owner &&
              schemaOwner == other.schemaOwner &&
              COMPARE_DESC_SMART_PTR(priv_desc, other.priv_desc) &&
              routineDescFlags == other.routineDescFlags &&
              libRedefTime == other.libRedefTime &&
              COMPARE_CHAR_PTRS(libBlobHandle, other.libBlobHandle) &&
              COMPARE_CHAR_PTRS(libSchName, other.libSchName) &&
              libVersion == other.libVersion &&
              libObjUID == other.libObjUID
         );
  }

  Int64 objectUID;
  char* routineName;
  char* externalName;
  char* librarySqlName;
  char* libraryFileName;
  char* signature;
  ComSInt32 paramsCount;
  DescStructPtr params;
  ComRoutineLanguage language;
  ComRoutineType UDRType;
  ComRoutineSQLAccess sqlAccess;
  ComRoutineTransactionAttributes transactionAttributes;
  ComSInt32 maxResults;
  ComRoutineParamStyle paramStyle;
  NABoolean isDeterministic;
  NABoolean isCallOnNull;
  NABoolean isIsolate;
  ComRoutineExternalSecurity externalSecurity;
  ComRoutineExecutionMode executionMode;
  Int32 stateAreaSize;
  ComRoutineParallelism parallelism;
  Int32 owner;
  Int32 schemaOwner;
  DescStructPtr priv_desc;

  Int64 routineDescFlags; // my flags
  Int64 libRedefTime; 
  char *libBlobHandle;
  char *libSchName;
  Int32 libVersion;
  Int64 libObjUID;
  char filler[24];
};

class TrafSequenceGeneratorDesc : public TrafDesc {
public:
  // why almost no initializers? see note at top of file
  TrafSequenceGeneratorDesc() : TrafDesc(DESC_SEQUENCE_GENERATOR_TYPE)
  {}

  enum SequenceGeneratorDescFlags
    { 
      XN_REPL_SYNC      = 0x0001,
      XN_REPL_ASYNC     = 0x0002,
    };

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID()
  {
    return 1;
  }

  virtual void populateImageVersionIDArray()
  {
    setImageVersionID(0,getClassVersionID());
  }

  virtual short getClassSize()      { return (short)sizeof(TrafSequenceGeneratorDesc); }
 
  virtual Long pack(void *space);
  virtual Lng32 unpack(void * base, void * reallocator);

  virtual TrafSequenceGeneratorDesc *sequenceGeneratorDesc() const { return (TrafSequenceGeneratorDesc*)this; }

  ComSequenceGeneratorType sgType() 
  { return (ComSequenceGeneratorType)sgType_; }
  void setSgType(ComSequenceGeneratorType v) 
  { sgType_ = (Int16)v; }

  void setXnReplSync(NABoolean v) 
  {(v ? sequenceGeneratorDescFlags |= XN_REPL_SYNC : sequenceGeneratorDescFlags &= ~XN_REPL_SYNC); };
  NABoolean isXnReplSync() { return (sequenceGeneratorDescFlags & XN_REPL_SYNC) != 0; };

  void setXnReplAsync(NABoolean v) 
  {(v ? sequenceGeneratorDescFlags |= XN_REPL_ASYNC : sequenceGeneratorDescFlags &= ~XN_REPL_ASYNC); };
  NABoolean isXnReplAsync() { return (sequenceGeneratorDescFlags & XN_REPL_ASYNC) != 0; };

  NABoolean operator==(const TrafSequenceGeneratorDesc& other) const
  {
      return (TrafDesc::operator==(other) &&
              startValue == other.startValue &&
              increment == other.increment &&
              sgType_ == other.sgType_ &&
              sqlDataType == other.sqlDataType &&
              fsDataType == other.fsDataType &&
              cycleOption == other.cycleOption &&
              maxValue == other.maxValue &&
              minValue == other.minValue &&
              cache == other.cache &&
              objectUID == other.objectUID &&
              sgLocation == other.sgLocation &&
              nextValue == other.nextValue &&
              redefTime == other.redefTime &&
              seqOrder == other.seqOrder &&
              sequenceGeneratorDescFlags == other.sequenceGeneratorDescFlags 
             );
  }

  Int64                     startValue;
  Int64                     increment;

  Int16 /*ComSequenceGeneratorType*/  sgType_;
  Int16 /*ComSQLDataType*/  sqlDataType;
  Int16 /*ComFSDataType*/   fsDataType;
  Int16                     cycleOption;

  Int64                     maxValue;
  Int64                     minValue;
  Int64                     cache;
  Int64                     objectUID;
  char*                     sgLocation;
  Int64                     nextValue;
  Int64                     redefTime;
  Int64                     seqOrder;

  Int64 sequenceGeneratorDescFlags; // my flags

  char filler[8];
};

class TrafTableDesc : public TrafDesc {
public:
  // why almost no initializers? see note at top of file
  TrafTableDesc() : TrafDesc(DESC_TABLE_TYPE)
  {}

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID()
  {
    return 1;
  }

  virtual void populateImageVersionIDArray()
  {
    setImageVersionID(0,getClassVersionID());
  }

  virtual short getClassSize()      { return (short)sizeof(TrafTableDesc); }
 
  virtual Long pack(void *space);
  virtual Lng32 unpack(void * base, void * reallocator);

  virtual TrafTableDesc *tableDesc() const { return (TrafTableDesc*)this; }

  enum TableDescFlags
    { 
      SYSTEM_TABLE_CODE = 0x0001,  
      UMD_TABLE         = 0x0002,
      MV_TABLE          = 0x0004,
      IUD_LOG           = 0x0008,
      MV_MD_OBJECT      = 0x0010,
      SYN_TRANS_DONE    = 0x0020,
      VOLATILE          = 0x0040,
      IN_MEM_OBJ        = 0x0080,
      DROPPABLE         = 0x0100,
      INSERT_ONLY       = 0x0200,
      XN_REPL_SYNC      = 0x0400,
      XN_REPL_ASYNC     = 0x0800
    };

  void setSystemTableCode(NABoolean v) 
  {(v ? tableDescFlags |= SYSTEM_TABLE_CODE : tableDescFlags &= ~SYSTEM_TABLE_CODE); };
  NABoolean isSystemTableCode() { return (tableDescFlags & SYSTEM_TABLE_CODE) != 0; };

  void setUMDTable(NABoolean v) 
  {(v ? tableDescFlags |= UMD_TABLE : tableDescFlags &= ~UMD_TABLE); };
  NABoolean isUMDTable() { return (tableDescFlags & UMD_TABLE) != 0; };

  void setMVTable(NABoolean v) 
  {(v ? tableDescFlags |= MV_TABLE : tableDescFlags &= ~MV_TABLE); };
  NABoolean isMVTable() { return (tableDescFlags & MV_TABLE) != 0; };

  void setIUDLog(NABoolean v) 
  {(v ? tableDescFlags |= IUD_LOG : tableDescFlags &= ~IUD_LOG); };
  NABoolean isIUDLog() { return (tableDescFlags & IUD_LOG) != 0; };

  void setMVMetadataObject(NABoolean v) 
  {(v ? tableDescFlags |= MV_MD_OBJECT : tableDescFlags &= ~MV_MD_OBJECT); };
  NABoolean isMVMetadataObject() { return (tableDescFlags & MV_MD_OBJECT) != 0; };

  void setSynonymTranslationDone(NABoolean v) 
  {(v ? tableDescFlags |= SYN_TRANS_DONE : tableDescFlags &= ~SYN_TRANS_DONE); };
  NABoolean isSynonymTranslationDone() { return (tableDescFlags & SYN_TRANS_DONE) != 0; };

  void setVolatileTable(NABoolean v) 
  {(v ? tableDescFlags |= VOLATILE : tableDescFlags &= ~VOLATILE); };
  NABoolean isVolatileTable() { return (tableDescFlags & VOLATILE) != 0; };

  void setInMemoryObject(NABoolean v) 
  {(v ? tableDescFlags |= IN_MEM_OBJ : tableDescFlags &= ~IN_MEM_OBJ); };
  NABoolean isInMemoryObject() { return (tableDescFlags & IN_MEM_OBJ) != 0; };

  void setDroppable(NABoolean v) 
  {(v ? tableDescFlags |= DROPPABLE : tableDescFlags &= ~DROPPABLE); };
  NABoolean isDroppable() { return (tableDescFlags & DROPPABLE) != 0; };

  void setInsertOnly(NABoolean v) 
  {(v ? tableDescFlags |= INSERT_ONLY : tableDescFlags &= ~INSERT_ONLY); };
  NABoolean isInsertOnly() { return (tableDescFlags & INSERT_ONLY) != 0; };

  void setXnReplSync(NABoolean v) 
  {(v ? tableDescFlags |= XN_REPL_SYNC : tableDescFlags &= ~XN_REPL_SYNC); };
  NABoolean isXnReplSync() { return (tableDescFlags & XN_REPL_SYNC) != 0; };

  void setXnReplAsync(NABoolean v) 
  {(v ? tableDescFlags |= XN_REPL_ASYNC : tableDescFlags &= ~XN_REPL_ASYNC); };
  NABoolean isXnReplAsync() { return (tableDescFlags & XN_REPL_ASYNC) != 0; };

  ComInsertMode insertMode() { return (ComInsertMode)insertMode_;}
  void setInsertMode(ComInsertMode v) {insertMode_ = (Int16)v;}

  ComPartitioningScheme partitioningScheme() 
  { return (ComPartitioningScheme)partitioningScheme_; }
  void setPartitioningScheme(ComPartitioningScheme v) 
  { partitioningScheme_ = (Int16)v; }
 
  ComRowFormat rowFormat() { return (ComRowFormat)rowFormat_; }
  void setRowFormat(ComRowFormat v) { rowFormat_ = (Int16)v; }
 
  ComObjectType objectType() { return (ComObjectType)objectType_; }
  void setObjectType(ComObjectType v) { objectType_ = (Int16)v; }

  ComStorageType storageType() { return (ComStorageType)storageType_; }
  void setStorageType(ComStorageType v) { storageType_ = (Int16)v; }

  NABoolean operator==(const TrafTableDesc& other) const
  {
      return (TrafDesc::operator==(other) &&
              COMPARE_CHAR_PTRS(tablename, other.tablename) &&
              redefTime == other.redefTime &&
              baseTableUID == other.baseTableUID &&
              mvAttributesBitmap == other.mvAttributesBitmap &&
              record_length == other.record_length &&
              colcount == other.colcount &&
              constr_count == other.constr_count  &&
              insertMode_ == other.insertMode_ &&
              partitioningScheme_ == other.partitioningScheme_ &&
              rowFormat_== other.rowFormat_ &&
              objectType_== other.objectType_ &&
              storageType_== other.storageType_ &&
              numLOBdatafiles_== other.numLOBdatafiles_ &&
              catUID == other.catUID &&
              schemaUID == other.schemaUID &&
              objectUID == other.objectUID &&
              objDataUID == other.objDataUID &&
              owner == other.owner &&
              schemaOwner == other.schemaOwner &&
              COMPARE_CHAR_PTRS(default_col_fam, other.default_col_fam) &&
              COMPARE_CHAR_PTRS(all_col_fams, other.all_col_fams) &&
              objectFlags == other.objectFlags &&
              tablesFlags == other.tablesFlags &&
              tableDescFlags == other.tableDescFlags &&
              COMPARE_DESC_SMART_PTR(columns_desc, other.columns_desc) &&
              COMPARE_DESC_SMART_PTR(indexes_desc, other.indexes_desc) &&
              COMPARE_DESC_SMART_PTR(constrnts_desc, other.constrnts_desc) &&
              COMPARE_DESC_SMART_PTR(constrnts_desc, other.constrnts_desc) &&
              COMPARE_DESC_SMART_PTR(views_desc, other.views_desc) &&
              COMPARE_DESC_SMART_PTR(constrnts_tables_desc, other.constrnts_tables_desc) &&
              COMPARE_DESC_SMART_PTR(referenced_tables_desc, other.referenced_tables_desc) &&
              COMPARE_DESC_SMART_PTR(referencing_tables_desc, other.referencing_tables_desc) &&
              COMPARE_DESC_SMART_PTR(table_stats_desc, other.table_stats_desc) &&
              COMPARE_DESC_SMART_PTR(files_desc, other.files_desc) &&
              COMPARE_DESC_SMART_PTR(priv_desc, other.priv_desc) &&
              COMPARE_DESC_SMART_PTR(hbase_regionkey_desc, other.hbase_regionkey_desc) &&
              COMPARE_DESC_SMART_PTR(sequence_generator_desc, other.sequence_generator_desc) &&
              COMPARE_CHAR_PTRS(tableNamespace, other.tableNamespace) &&
              COMPARE_DESC_SMART_PTR(partitionv2_desc, other.partitionv2_desc)
      );
  }

  char* tablename;

  Int64 createTime;
  Int64 redefTime;

  // if this descriptor is for an index that is being accessed as a table,
  // then baseTableUID is the uid of the base table for that index.
  Int64 baseTableUID;

  Int32 mvAttributesBitmap;
  Int32 record_length;
  Int32 colcount;
  Int32 constr_count;

  Int16 /*ComInsertMode*/ insertMode_;
  Int16 /*ComPartitioningScheme*/ partitioningScheme_; 
  Int16 /*ComRowFormat*/  rowFormat_;
  Int16 /*ComObjectType*/ objectType_; 

  Int16 /*ComStorageType*/ storageType_;

  // if this table has LOB columns which are created with multiple datafiles
  // for each column, this field represents the number of hdfs datafiles.
  // See CmpSeabaseDDL::createSeabaseTable2 where datafiles are created.
  Int16 numLOBdatafiles_;

  // next 4 bytes are fillers for future usage
  char filler0[4];

  Int64 catUID;
  Int64 schemaUID;
  Int64 objectUID;
  Int64 objDataUID;

  Lng32 owner;
  Lng32 schemaOwner;

  char * snapshotName;
  char*  default_col_fam;
  char*  all_col_fams;
  Int64 objectFlags;
  Int64 tablesFlags;

  Int64 tableDescFlags; // my flags

  DescStructPtr columns_desc;
  DescStructPtr indexes_desc;
  DescStructPtr constrnts_desc;
  DescStructPtr views_desc;
  DescStructPtr constrnts_tables_desc;
  DescStructPtr referenced_tables_desc;
  DescStructPtr referencing_tables_desc;
  DescStructPtr table_stats_desc;
  DescStructPtr files_desc;
  DescStructPtr priv_desc;
  // for hbase's region keys
  DescStructPtr hbase_regionkey_desc;
  DescStructPtr sequence_generator_desc;
  DescStructPtr partitionv2_desc;

  char* tableNamespace;

  char filler[24];
};

class TrafTableStatsDesc : public TrafDesc {
public:
  TrafTableStatsDesc() : TrafDesc(DESC_TABLE_STATS_TYPE)
  {}

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID()
  {
    return 1;
  }

  virtual void populateImageVersionIDArray()
  {
    setImageVersionID(0,getClassVersionID());
  }

  virtual short getClassSize()      { return (short)sizeof(TrafTableStatsDesc); }
 
  virtual Long pack(void *space);
  virtual Lng32 unpack(void * base, void * reallocator);

  virtual short copyFrom(TrafDesc * from, NAMemory * heap);
  virtual void deallocMembers(NAMemory * heap);

  virtual TrafTableStatsDesc *tableStatsDesc() const { return (TrafTableStatsDesc*)this; }

  enum TableStatsDescFlags
    { 
      FAST_STATS = 0x0001
    };

  void setFastStats(NABoolean v) 
  {(v ? tableStatsDescFlags |= FAST_STATS : tableStatsDescFlags &= ~FAST_STATS); };
  NABoolean isFastStats() { return (tableStatsDescFlags & FAST_STATS) != 0; };

  NABoolean operator==(const TrafTableStatsDesc& other) const
  {
      return (TrafDesc::operator==(other) &&
              rowcount == other.rowcount &&
              hbtBlockSize == other.hbtBlockSize &&
              hbtIndexLevels == other.hbtIndexLevels &&
              numHistograms == other.numHistograms &&
              numHistograms == other.numHistograms &&
              COMPARE_DESC_SMART_PTR(histograms_desc, 
                           other.histograms_desc) &&
              COMPARE_DESC_SMART_PTR(hist_interval_desc, 
                           other.hist_interval_desc) &&
              COMPARE_TRAFDESC_PTRS(currHistogramDesc, 
                           other.currHistogramDesc) &&
              COMPARE_TRAFDESC_PTRS(currHistintDesc, 
                           other.currHistintDesc)
             );
  }


  Float64 rowcount;
  Int32 hbtBlockSize;
  Int32 hbtIndexLevels;

  Int32 numHistograms;    // num of histograms_descs
  Int32 numHistIntervals; // num of hist_interval_descs

  Int64 tableStatsDescFlags; // my flags

  DescStructPtr histograms_desc;
  DescStructPtr hist_interval_desc;


  TrafDesc * currHistogramDesc;
  TrafDesc * currHistintDesc;

  // initializes the previous 2 cursor position, fetches current
  // contents and advances cursor.
  // returns 100 if cursor at EOD.
  void initHistogramCursor();
  void initHistintCursor();
  short fetchHistogramCursor
  (
       void * histogram_id,
       void * column_number,
       void * colcount,
       void * interval_count,
       void * rowcount,
       void * total_uec,
       void * stats_time,
       void * low_value,
       void * high_value,
       void * read_time,
       void * read_count,
       void * sample_secs,
       void * col_secs,
       void * sample_percent,
       void * cv,
       void * reason,
       void * v1,
       void * v2,
       // v5, expression text
       void * v5 = NULL
   );
  short fetchHistintCursor
  (
       void * histogram_id,
       void * interval_number,
       void * interval_rowcount,
       void * interval_uec,
       void * interval_boundary,
       void * std_dev_of_freq,
       void * v1,
       void * v2,
       void * v5
   );

  char filler[40];
};

class TrafUsingMvDesc : public TrafDesc {
public:
  // why almost no initializers? see note at top of file
  TrafUsingMvDesc() : TrafDesc(DESC_USING_MV_TYPE)
  {}

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID()
  {
    return 1;
  }

  virtual void populateImageVersionIDArray()
  {
    setImageVersionID(0,getClassVersionID());
  }

  virtual short getClassSize()      { return (short)sizeof(TrafUsingMvDesc); }
 
  virtual Long pack(void *space);
  virtual Lng32 unpack(void * base, void * reallocator);

  virtual TrafUsingMvDesc *usingMvDesc() const { return (TrafUsingMvDesc*)this; }

  ComMVRefreshType refreshType() { return (ComMVRefreshType)refreshType_;}
  void setRefreshType(ComMVRefreshType v) 
  { refreshType_ = (Int16)v;}


  NABoolean operator==(const TrafUsingMvDesc& other) const
  {
      return (TrafDesc::operator==(other) &&
              COMPARE_CHAR_PTRS(mvName, other.mvName) &&
              rewriteEnabled == other.rewriteEnabled &&
              isInitialized == other.isInitialized &&
              refreshType_ == other.refreshType_
             );
  }

  char* mvName;
  Int32  rewriteEnabled;
  Int32  isInitialized;
  Int16 /*ComMVRefreshType*/ refreshType_; // unknown here means "non incremental"

  char filler[14];
};

class TrafViewDesc : public TrafDesc {
public:
  // why almost no initializers? see note at top of file
  TrafViewDesc() : TrafDesc(DESC_VIEW_TYPE)
  {}

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID()
  {
    return 1;
  }

  virtual void populateImageVersionIDArray()
  {
    setImageVersionID(0,getClassVersionID());
  }

  virtual short getClassSize()      { return (short)sizeof(TrafViewDesc); }

  virtual Long pack(void *space);
  virtual Lng32 unpack(void * base, void * reallocator);

  virtual TrafViewDesc *viewDesc() const { return (TrafViewDesc*)this; }

  enum ViewDescFlags
    {
      UPDATABLE           = 0x0001,
      INSERTABLE          = 0x0002
    };

  void setUpdatable(NABoolean v)
  {(v ? viewDescFlags |= UPDATABLE : viewDescFlags &= ~UPDATABLE); };
  NABoolean isUpdatable() { return (viewDescFlags & UPDATABLE) != 0; };

  void setInsertable(NABoolean v)
  {(v ? viewDescFlags |= INSERTABLE : viewDescFlags &= ~INSERTABLE); };
  NABoolean isInsertable() { return (viewDescFlags & INSERTABLE) != 0; };


  NABoolean operator==(const TrafViewDesc& other) const
  {
      return (TrafDesc::operator==(other) &&
              COMPARE_CHAR_PTRS(viewname, other.viewname) &&
              COMPARE_CHAR_PTRS(viewfilename, other.viewfilename) &&
              COMPARE_CHAR_PTRS(viewtext, other.viewtext) &&
              COMPARE_CHAR_PTRS(viewchecktext, other.viewchecktext) &&
              COMPARE_CHAR_PTRS(viewcolusages, other.viewcolusages) &&
              viewDescFlags == other.viewDescFlags &&
              viewtextcharset == other.viewtextcharset 
             );
  }

  char*  viewname;
  char*  viewfilename;    // the physical file, to be Opened for auth-cking.
  char*  viewtext;
  char*  viewchecktext;
  char*  viewcolusages;

  Int64 viewDescFlags; // my flags

  Int16 /*CharInfo::CharSet*/ viewtextcharset;

  char filler[22];
};

// --------------------------- privilege descriptors ---------------------------
// The privilege descriptors are organized as follows:
//   privDesc - a TrafPrivDesc containing the privGrantees
//   privGrantees - a TrafPrivGranteeDesc descriptor containing:
//     grantee - int32 value for each user granted a privilege directly or
//               through a role granted to the user
//     schemaUid - int64 value identifying the schema 
//     schemaBitmap  - a TrafPrivBitmapDesc containing grante schema privs
//                     summarized across all grantors.
//     objectBitmap  - a TrafPrivBitmapDesc containing granted object privs
//                     summarized across all grantors
//     columnBitmaps - list of TrafPrivBitmapDesc, one per colummn, containing 
//                     granted column privs, summarized across all grantors
//   priv_bits desc - a TrafPrivBitmapDesc descriptor containing:
//     privBitmap - bitmap containing granted privs such as SELECT
//     privWGO - bitmap containing WGO for associated grant (privBitmap)
//     columnOrdinal - column number for bitmap, for objects, column
//                     number is not relavent so it is set to -1.
//   column bits - list of TrafPrivBitmapDesc
class TrafPrivDesc : public TrafDesc {
public:
  // why almost no initializers? see note at top of file
  TrafPrivDesc() : TrafDesc(DESC_PRIV_TYPE)
  {}

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID()
  {
    return 1;
  }

  virtual void populateImageVersionIDArray()
  {
    setImageVersionID(0,getClassVersionID());
  }

  virtual short getClassSize()      { return (short)sizeof(TrafPrivDesc); }

  virtual Long pack(void *space);
  virtual Lng32 unpack(void * base, void * reallocator);

  virtual TrafPrivDesc *privDesc() const { return (TrafPrivDesc*)this; }

  NABoolean operator==(const TrafPrivDesc& other) const
  {
      return (TrafDesc::operator==(other) &&
              COMPARE_DESC_SMART_PTR(privGrantees, other.privGrantees)
             );
  }

  DescStructPtr privGrantees;
  char filler[16];
};

class TrafPrivGranteeDesc : public TrafDesc {
public:
  // why almost no initializers? see note at top of file
  TrafPrivGranteeDesc() : TrafDesc(DESC_PRIV_GRANTEE_TYPE)
  {}

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID()
  {
    return 1;
  }

  virtual void populateImageVersionIDArray()
  {
    setImageVersionID(0,getClassVersionID());
  }

  virtual short getClassSize()      { return (short)sizeof(TrafPrivGranteeDesc); }

  virtual Long pack(void *space);
  virtual Lng32 unpack(void * base, void * reallocator);

  virtual TrafPrivGranteeDesc *privGranteeDesc() const { return (TrafPrivGranteeDesc*)this; }


  NABoolean operator==(const TrafPrivGranteeDesc& other) const
  {
      return (TrafDesc::operator==(other) &&
              grantee == other.grantee &&
              schemaUID == other.schemaUID &&
              COMPARE_DESC_SMART_PTR(schemaBitmap, other.schemaBitmap) &&
              COMPARE_DESC_SMART_PTR(objectBitmap, other.objectBitmap) &&
              COMPARE_DESC_SMART_PTR(columnBitmaps, other.columnBitmaps)
             );
  }

  Int32 grantee;
  Int64 schemaUID;
  DescStructPtr schemaBitmap;
  DescStructPtr objectBitmap;
  DescStructPtr columnBitmaps;
  char filler[4];
};

class TrafPrivBitmapDesc : public TrafDesc {
public:
  // why almost no initializers? see note at top of file
  TrafPrivBitmapDesc() : TrafDesc(DESC_PRIV_BITMAP_TYPE)
  {}

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID()
  {
    return 1;
  }

  virtual void populateImageVersionIDArray()
  {
    setImageVersionID(0,getClassVersionID());
  }

  virtual short getClassSize()      { return (short)sizeof(TrafPrivBitmapDesc); }

  //virtual Long pack(void *space);
  //virtual Lng32 unpack(void * base, void * reallocator);

  virtual TrafPrivBitmapDesc *privBitmapDesc() const { return (TrafPrivBitmapDesc*)this; }

  NABoolean operator==(const TrafPrivBitmapDesc& other) const
  {
      return (TrafDesc::operator==(other) &&
              columnOrdinal == other.columnOrdinal &&
              privBitmap == other.privBitmap &&
              privWGOBitmap == other.privWGOBitmap 
             );
  }

  Int32  columnOrdinal;
  Int64  privBitmap;
  Int64  privWGOBitmap;
  char filler[20];
};
// ------------------------- end privilege descriptors -------------------------

// If "space" is passed in, use it. (It might be an NAHeap or a ComSpace.)
// If "space" is null, use the statement heap.
TrafDesc *TrafAllocateDDLdesc(desc_nodetype nodetype, 
                              NAMemory * space);

TrafDesc *TrafMakeColumnDesc
(
     const char *tablename,
     const char *colname,
     Lng32 &colnumber,		  // INOUT
     Int32 datatype,
     Lng32 length,
     Lng32 &offset,		  // INOUT
     NABoolean null_flag,
     SQLCHARSET_CODE datacharset, // i.e., use CharInfo::DefaultCharSet;
     NAMemory * space
 );

class TrafPartDesc : public TrafDesc {
public:

  TrafPartDesc() : TrafDesc(DESC_PART_TYPE)
  {}

  virtual unsigned char getClassVersionID()
  {
    return 1;
  }

  virtual void populateImageVersionIDArray()
  {
    setImageVersionID(0,getClassVersionID());
  }

  NABoolean operator==(const TrafPartDesc &other) const
  {
    if (this == &other)
      return true;

    return ( 
            parentUid == other.parentUid &&
            partitionUid == other.partitionUid &&
            COMPARE_CHAR_PTRS(partitionName, other.partitionName) &&
            COMPARE_CHAR_PTRS(partitionEntityName, other.partitionEntityName) &&
            isSubPartition == other.isSubPartition &&
            hasSubPartition == other.hasSubPartition &&
            partPosition == other.partPosition &&
            COMPARE_CHAR_PTRS(partitionValueExpr, other.partitionValueExpr) &&
            partitionValueExprLen == other.partitionValueExprLen &&
            COMPARE_CHAR_PTRS(prevPartitionValueExpr, other.prevPartitionValueExpr) &&
            prevPartitionValueExprLen == other.prevPartitionValueExprLen &&
            isValid == other.isValid &&
            isReadonly == other.isReadonly &&
            isInMemory == other.isInMemory &&
            defTime == other.defTime &&
            flags == other.flags &&
            subpartitionCnt == other.subpartitionCnt &&
            COMPARE_DESC_SMART_PTR(subpart_desc, other.subpart_desc));
  }

  virtual short getClassSize()      { return (short)sizeof(TrafPartDesc); }
  virtual Long pack(void *space);
  virtual Lng32 unpack(void * base, void * reallocator);

  virtual TrafPartDesc *partDesc() const
  { return (TrafPartDesc *)this; }

  Int64 parentUid;
  Int64 partitionUid;
  char *partitionName;
  char *partitionEntityName;
  Int32 isSubPartition;
  Int32 hasSubPartition;
  Int32 partPosition;
  char* partitionValueExpr;
  Int32 partitionValueExprLen;
  char* prevPartitionValueExpr;
  Int32 prevPartitionValueExprLen;
  Int32 isValid;
  Int32 isReadonly;
  Int32 isInMemory;
  Int64 defTime;
  Int64 flags;
  Int32 subpartitionCnt;

  DescStructPtr subpart_desc; //TrafPartDesc
  char filler[8];
};

class TrafPartitionV2Desc : public TrafDesc {
public:

  TrafPartitionV2Desc() : TrafDesc(DESC_PARTITIONV2_TYPE)
  {}

  virtual unsigned char getClassVersionID()
  {
    return 1;
  }

  virtual void populateImageVersionIDArray()
  {
    setImageVersionID(0,getClassVersionID());
  }

  virtual NABoolean operator==(const TrafPartitionV2Desc& other) const
  {
    if (this == &other)
      return true;

    return (
      baseTableUid == other.baseTableUid &&
      partitionType == other.partitionType &&
      COMPARE_CHAR_PTRS(partitionColIdx, other.partitionColIdx) &&
      partitionColCount == other.partitionColCount &&
      subpartitionType == other.subpartitionType &&
      COMPARE_CHAR_PTRS(subpartitionColIdx, other.subpartitionColIdx) &&
      subpartitionColCount == other.subpartitionColCount &&
      COMPARE_CHAR_PTRS(partitionInterval, other.partitionInterval) &&
      COMPARE_CHAR_PTRS(subpartitionInterval, other.subpartitionInterval) &&
      partitionAutolist == other.partitionAutolist &&
      subpartitionAutolist ==  other.subpartitionAutolist &&
      flags == other.flags &&
      stlPartitionCnt == other.stlPartitionCnt &&
      COMPARE_DESC_SMART_PTR(part_desc, other.part_desc)
    );
  }
  virtual short getClassSize()      { return (short)sizeof(TrafPartitionV2Desc); }
  virtual Long pack(void *space);
  virtual Lng32 unpack(void * base, void * reallocator);

  virtual TrafPartitionV2Desc *partitionV2Desc() const
  { return (TrafPartitionV2Desc *)this; }

  Int64 baseTableUid;
  Int32 partitionType;
  char *partitionColIdx;
  Int32 partitionColCount;

  Int32 subpartitionType;
  char* subpartitionColIdx;
  Int32 subpartitionColCount;

  char *partitionInterval;
  char *subpartitionInterval;
  Int32 partitionAutolist;
  Int32 subpartitionAutolist;

  Int64 flags;
  Int32 stlPartitionCnt;
  DescStructPtr part_desc; 
  char filler[8];
};

#endif
