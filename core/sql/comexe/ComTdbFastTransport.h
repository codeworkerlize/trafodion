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
****************************************************************************
*
* File:         ComTdbFastTransport.h
* Description:  TDB class for user-defined routines
*
* Created:      11/05/2012
* Language:     C++
*
*
*
*
****************************************************************************
*/

#ifndef COM_TDB_FAST_TRANSPORT_H
#define COM_TDB_FAST_TRANSPORT_H

#include "ComTdb.h"
#include "ComQueue.h"

//
// Classes defined in this file
//

class  ComTdbFastExtract;


// ComTdbFastExtract class
//

class ComTdbFastExtract : public ComTdb
{
public:

  friend class ExFastExtractTdb;
  friend class ExFastExtractTcb;
  friend class ExHdfsFastExtractTcb;

  enum
  {
    TARGET_FILE                 = 0x0001,
    TARGET_SOCKET               = 0x0002,
    COMPRESS_LZO                = 0x0004,
    APPEND                      = 0x0008,
    INCLUDE_HEADER              = 0x0010,
    HIVE_INSERT                 = 0x0020,
    OVERWRITE_HIVE_TABLE        = 0x0040,
    SEQUENCE_FILE               = 0x0080,
    PRINT_DIAGS                 = 0x0100,
    HDFS_COMPRESSED             = 0x0200,
    ORC_FILE                    = 0x0400,
    CONTINUE_ON_ERROR           = 0x0800,
    PARQUET_FILE                = 0x1000,
    PARQUET_LEGACY_TS           = 0x2000,
    AVRO_FILE                   = 0x4000,
    ORC_BLOCK_PADDING           = 0x8000,
    HIVE_PART_AUTO_CREATE       = 0x10000,
    HIVE_PART_DISTR_LOCK        = 0x20000,
    PARQUET_ENABLE_DICTIONARY   = 0x40000
  };

  ComTdbFastExtract ()
  : ComTdb(ex_FAST_EXTRACT, "eye_FAST_EXTRACT")
  {}

  ComTdbFastExtract (
    ULng32 flags,
    Cardinality estimatedRowCount,
    char * targetName,
    char * hdfsHost,
    Lng32 hdfsPort,
    char * hiveSchemaName,
    char * hiveTableName,
    char * delimiter,
    char * header,
    char * nullString,
    char * recordSeparator,
    ex_cri_desc *criDescParent,
    ex_cri_desc *criDescReturned,
    ex_cri_desc *workCriDesc,
    queue_index downQueueMaxSize,
    queue_index upQueueMaxSize,
    Lng32 numOutputBuffers,
    ULng32 outputBufferSize,
    UInt16 numIOBuffers,
    UInt16 ioTimeout,
    UInt32 maxOpenPartitions,
    ex_expr *inputExpr,
    ex_expr *outputExpr,
    ex_expr *partStringExpr,
    ULng32 requestRowLen,
    ULng32 outputRowLen,
    ULng32 partStringRowLen,
    ex_expr * childDataExprs,
    ComTdb * childTdb,
    Space *space,
    unsigned short childDataTuppIndex,
    unsigned short cnvChildDataTuppIndex,
    unsigned short partStringTuppIndex,
    unsigned short tgtValsTuppIndex,
    ULng32 cnvChildDataRowLen,
    Int64 hdfBuffSize,
    Int16 replication,
    Queue * extColNameList,
    Queue * extColTypeList,
    Int64 stripeOrBlockSize,
    Int32 orcBufferSize,
    Int32 strideOrPageSize,
    char *compression,
    char *parqSchStr
    );

  virtual ~ComTdbFastExtract();

  //----------------------------------------------------------------------
  // Redefine virtual functions required for versioning
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID()
  {
    return 1;
  }
  virtual void populateImageVersionIDArray()
  {
    setImageVersionID(1,getClassVersionID());
    ComTdb::populateImageVersionIDArray();
  }
  virtual short getClassSize()
  {
    return (short) sizeof(ComTdbFastExtract);
  }

  //----------------------------------------------------------------------
  // Pack/unpack
  //----------------------------------------------------------------------
  Long pack(void *);
  Lng32 unpack(void *, void *);

  //----------------------------------------------------------------------
  // Other required TDB support functions
  //----------------------------------------------------------------------
  Int32 orderedQueueProtocol() const
  {
    return -1;
  }
  void display() const
  {
    // All TDBs have an no-op display() function. Not sure why.
  }
  virtual Int32 numChildren() const
  {
    return 1;
  }
  virtual const char *getNodeName() const
  {
    return "EX_FAST_EXTRACT";
  }
  virtual const ComTdb *getChild() const
  {
      return childTdb_;
  }

  virtual const ComTdb* getChild(Int32 pos) const
  {
    if (pos == 0)
      return childTdb_;
    else
      return NULL;
  }

  virtual Int32 numExpressions() const
  {
    if (partStringExpr_.isNull())
      return 3;
    else
      return 4;
  }
  virtual const char *getExpressionName(Int32 pos) const
  {
    if (pos == 0)
      return "inputExpr_";
    else if (pos == 1)
      return "outputExpr_";
    else if (pos == 2)
      return( "childDataExpr_" );
    else if (pos == 3)
      return "partStringExpr_";
    else
      return NULL;
  }

  virtual ex_expr *getExpressionNode(Int32 pos)
  {
    if (pos == 0)
      return inputExpr_;
    else if (pos == 1)
      return outputExpr_;
    else if (pos == 2)
      return childDataExpr_;
    else if (pos == 3)
      return partStringExpr_;
    else
      return NULL;
  }

  virtual void displayContents(Space *space, ULng32 flag);

  NABoolean getIsAppend() const
  {
    return ((flags_ & APPEND) != 0);
  }
  ;
  void setIsAppend(short value)
  {
    if (value)
      flags_ |= APPEND;
    else
      flags_ &= ~APPEND;
  }
  NABoolean getIsHiveInsert() const
  {
    return ((flags_ & HIVE_INSERT) != 0);
  }
  ;
  void setIsHiveInsert(short value)
  {
    if (value)
      flags_ |= HIVE_INSERT;
    else
      flags_ &= ~HIVE_INSERT;
  }
  NABoolean isOrcFile() const
  {
    return ((flags_ & ORC_FILE) != 0);
  }
  ;
  void setIsOrcFile(short value)
  {
    if (value)
      flags_ |= ORC_FILE;
    else
      flags_ &= ~ORC_FILE;
  }

  NABoolean isParquetFile() const
  {
    return ((flags_ & PARQUET_FILE) != 0);
  }
  ;
  void setIsParquetFile(short value)
  {
    if (value)
      flags_ |= PARQUET_FILE;
    else
      flags_ &= ~PARQUET_FILE;
  }

  NABoolean isAvroFile() const
  {
    return ((flags_ & AVRO_FILE) != 0);
  }
  ;
  void setIsAvroFile(short value)
  {
    if (value)
      flags_ |= AVRO_FILE;
    else
      flags_ &= ~AVRO_FILE;
  }

   NABoolean getIncludeHeader() const
  {
    return ((flags_ & INCLUDE_HEADER) != 0);
  }
  ;
  void setIncludeHeader(short value)
  {
    if (value)
      flags_ |= INCLUDE_HEADER;
    else
      flags_ &= ~INCLUDE_HEADER;
  }
  NABoolean getTargetFile() const
  {
    return ((flags_ & TARGET_FILE) != 0);
  }
  ;
  void setTargetFile(short value)
  {
    if (value)
    {
      flags_ |= TARGET_FILE;
      flags_ &= ~TARGET_SOCKET;
    }
    else
      flags_ &= ~TARGET_FILE;
  }
  NABoolean getTargetSocket() const
  {
    return ((flags_ & TARGET_SOCKET) != 0);
  }
  ;
  void setTargetSocket(short value)
  {
    if (value)
    {
      flags_ |= TARGET_SOCKET;
      flags_ &= ~TARGET_FILE;
    }
    else
      flags_ &= ~TARGET_SOCKET;
  }
  NABoolean getCompressLZO() const
  {
    return ((flags_ & COMPRESS_LZO) != 0);
  }
  ;
  void setCompressLZO(short value)
  {
    if (value)
      flags_ |= COMPRESS_LZO;
    else
      flags_ &= ~COMPRESS_LZO;
  }
  NABoolean getPrintDiags() const {return ((flags_ & PRINT_DIAGS) != 0);};
  void setPrintDiags(short value)
  {
	if (value)
	 flags_ |= PRINT_DIAGS;
	else
	 flags_ &= ~PRINT_DIAGS;
  }

  void setOverwriteHiveTable(short value)
  {
    if (value)
      flags_ |= OVERWRITE_HIVE_TABLE;
    else
      flags_ &= ~OVERWRITE_HIVE_TABLE;
  }
  NABoolean getOverwriteHiveTable() const
  {
    return ((flags_ & OVERWRITE_HIVE_TABLE) != 0);
  }
  ;

  void setSequenceFile(short value)
  {
    if (value)
      flags_ |= SEQUENCE_FILE;
    else
      flags_ &= ~SEQUENCE_FILE;
  }
  NABoolean getIsSequenceFile() const
  {
    return ((flags_ & SEQUENCE_FILE) != 0);
  }
  ;

  void setHdfsCompressed(UInt32 value)
  {
    if (value)
      flags_ |= HDFS_COMPRESSED;
    else
      flags_ &= ~HDFS_COMPRESSED;
  }
  NABoolean getHdfsCompressed() const
  {
    return ((flags_ & HDFS_COMPRESSED) != 0);
  }
  ;
  void setContinueOnError(NABoolean value)
  { value ? flags_ |= CONTINUE_ON_ERROR : flags_ &= ~CONTINUE_ON_ERROR; }
  NABoolean getContinueOnError() const
  { return ((flags_ & CONTINUE_ON_ERROR) != 0); }

  void setParquetLegacyTS(NABoolean v)
  {(v ? flags_ |= PARQUET_LEGACY_TS : flags_ &= ~PARQUET_LEGACY_TS); };
  NABoolean parquetLegacyTS() { return (flags_ & PARQUET_LEGACY_TS) != 0; };

  void setOrcBlockPadding(NABoolean v)
  {(v ? flags_ |= ORC_BLOCK_PADDING : flags_ &= ~ORC_BLOCK_PADDING); };
  NABoolean getOrcBlockPadding() { return (flags_ & ORC_BLOCK_PADDING) != 0; };

  void setParquetEnableDictionary(NABoolean v)
  {(v ? flags_ |= PARQUET_ENABLE_DICTIONARY : flags_ &= ~PARQUET_ENABLE_DICTIONARY); };
  NABoolean getParquetEnableDictionary() { return (flags_ & PARQUET_ENABLE_DICTIONARY) != 0; };
   
  void setHivePartAutoCreate(NABoolean v)
  {(v ? flags_ |= HIVE_PART_AUTO_CREATE : flags_ &= ~HIVE_PART_AUTO_CREATE); };
  NABoolean getHivePartAutoCreate() const { return (flags_ & HIVE_PART_AUTO_CREATE) != 0; };
  
  void setHivePartDistrLock(NABoolean v)
  {(v ? flags_ |= HIVE_PART_DISTR_LOCK : flags_ &= ~HIVE_PART_DISTR_LOCK); };
  NABoolean getHivePartDistrLock() const { return (flags_ & HIVE_PART_DISTR_LOCK) != 0; };
  
  inline const char *getTargetName() const { return targetName_; }
  inline const char *getHdfsHostName() const { return hdfsHostName_; }
  inline const Int32 getHdfsPortNum() const {return (Int32)hdfsPortNum_;}
  inline const char *getHiveTableName() const { return hiveTableName_; }
  inline const char *getHiveSchemaName() const { return hiveSchemaName_; }
  inline const char *getHeader() const { return header_; }
  inline const char *getNullString() const { return nullString_; }
  inline const char *getDelimiter() const { return delimiter_; }
  inline const char *getRecordSeparator() const
  { return recordSeparator_; }

  Int32 getNumIOBuffers() const {return (Int32)numIOBuffers_;}
  Int32 getIoTimeout() {return (Int32)ioTimeout_;}
  Int64 getHdfsIoBufferSize() const
  {
    return hdfsIOBufferSize_;
  }

  void setHdfsIoBufferSize(Int64 hdfsIoBufferSize)
  {
    hdfsIOBufferSize_ = hdfsIoBufferSize;
  }

  Int16 getHdfsReplication() const
  {
    return hdfsReplication_;
  }

  void setHdfsReplication(Int16 hdfsReplication)
  {
    hdfsReplication_ = hdfsReplication;
  }
  UInt32 getChildDataRowLen() const
  {
    return childDataRowLen_;
  }
  UInt32 getPartStringRowLen() const
  {
    return partStringRowLen_;
  }

  void setModTSforDir(Int64 v) { modTSforDir_ = v; }
  Int64 getModTSforDir() const { return modTSforDir_; }

  const Queue* extColNameList() const { return extColNameList_; }
  const Queue* extColTypeList() const { return extColTypeList_; }

  const Int64 getOrcStripeSize() const { return stripeOrBlockSize_; }
  const Int32 getOrcBufferSize() const { return bufSizeOrWriterMaxPadding_; }
  const Int32 getOrcRowIndexStride() const { return strideOrPageSize_; }
  const char* getOrcCompression() const { return compression_; }

  const Int64 getParquetBlockSize() const { return stripeOrBlockSize_; }
  const Int32 getParquetPageSize() const { return strideOrPageSize_; }
  const Int32 getParquetMaxPadding() const { return bufSizeOrWriterMaxPadding_; }
  const char* getParquetCompression() const { return compression_; }

  const char* getParqSchStr() const { return parqSchStr_; }
  void setHdfsIoByteArraySize(int size)             
    { hdfsIoByteArraySizeInKB_ = size; }                          
  Lng32 getHdfsIoByteArraySize()                             
    { return hdfsIoByteArraySizeInKB_; }
protected:
  NABasicPtr   targetName_;                                  // 00 - 07
  NABasicPtr   delimiter_;                                   // 08 - 15
  NABasicPtr   header_;                                      // 16 - 23
  NABasicPtr   nullString_;                                  // 24 - 31
  NABasicPtr   recordSeparator_;                             // 32 - 39
  ExExprPtr    inputExpr_;                                   // 40 - 47
  ExExprPtr    outputExpr_;                                  // 48 - 55
  ExExprPtr    partStringExpr_;                              // 56 - 63
  ExExprPtr    childDataExpr_;                               // 64 - 71
  ComTdbPtr    childTdb_;                                    // 72 - 79
  ExCriDescPtr workCriDesc_;                                 // 80 - 87
  UInt32       flags_;                                       // 88 - 91
  UInt32       requestRowLen_;                               // 92 - 95
  UInt32       outputRowLen_;                                // 96 - 99
  UInt32       partStringRowLen_;                            // 100 - 103
  UInt16       childDataTuppIndex_;                          // 104 - 105
  UInt16       cnvChildDataTuppIndex_;                       // 106 - 107
  UInt16       partStringTuppIndex_;                         // 108 - 109
  UInt16       numIOBuffers_;				     // 110 - 111
  Int32        hdfsPortNum_;                                 // 112 - 115
  NABasicPtr   hiveSchemaName_;                              // 116 - 123
  NABasicPtr   hiveTableName_;                               // 124 - 131
  Int64        hdfsIOBufferSize_;                            // 132 - 139
  NABasicPtr   hdfsHostName_  ;                              // 140 - 147
  UInt16       ioTimeout_;                                   // 148 - 149
  Int16        hdfsReplication_;                             // 150 - 151
  UInt32       maxOpenPartitions_;                           // 152 - 155
  UInt32       childDataRowLen_;                             // 156 - 159
  Int64        modTSforDir_;                                 // 160 - 167
  QueuePtr     extColNameList_;                              // 168 - 175
  QueuePtr     extColTypeList_;                              // 176 - 183
  UInt16       tgtValsTuppIndex_;                            // 184 - 185
  Lng32        hdfsIoByteArraySizeInKB_;                     // 186 - 189 
  char         filler0_[2];                                  // 190 - 191

  Int64        stripeOrBlockSize_;                           // 192 - 199
  Int32        bufSizeOrWriterMaxPadding_;                   // 200 - 203
  Int32        strideOrPageSize_;                            // 204 - 207

  // "NONE", "LZO", "SNAPPY", "ZLIB". Null terminated string.
  // Defined in enum orc.CompressionKind.
  // Default is ZLIB.
  char         compression_[16];                             // 208 - 223

  NABasicPtr   parqSchStr_;                                  // 224 - 231
  char fillerComTdbFastTransport_[8];                        // 232 - 239
};

#endif // COM_TDB_FAST_TRANSPORT_H

