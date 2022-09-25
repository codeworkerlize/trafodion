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
#ifndef EXP_LOB_OPER_V2_H
#define EXP_LOB_OPER_V2_H

class ExeCliInterface;
class ContextCli;
class ExLobV2;
class ExLobGlobals;

class ExpLobOperV2
{
public:
  ExpLobOperV2();

  ~ExpLobOperV2();

  enum DDLQueryType
    {
      // create the desc tables
      LOB_DDL_CREATE = 0,

      // alter the MD table and create LOB desc tables
      LOB_DDL_ALTER,
    
      // drops the desc tables
      LOB_DDL_DROP,

      // deletes all rows and all chunks from the desc table
      LOB_DDL_PURGEDATA,

      // cleanup LOBs. Cant use CLI_DROP as MD may be in an inconsistent state.
      LOB_DDL_CLEANUP,

      // read and return info from LOBMD table
      LOB_DDL_LOBMD_SELECT,
    };

  // Any changes to this list of enums need to be reflected in qTypeStr[]
  // defined in file ExpLobOperV2.cpp
  enum DMLQueryType
    {
      // inserts first/new chunk into the chunks table
      LOB_DML_INSERT = 0,

      // inserts next chunk and appends to existing row
      LOB_DML_INSERT_APPEND,

      // inserts first/new chunk into the chunks table. Called for update stmt.
      LOB_DML_UPDATE ,

      // inserts next chunk and appends to existing row. Called for update stmt.
      LOB_DML_UPDATE_APPEND,

      // deletes all chunks
      LOB_DML_DELETE,

      // prepares cursor select of all chunks. Does not return a row.
      // use SELECT_FETCH to retrieve rows.
      LOB_DML_SELECT_OPEN,

      // fetches data for the specified length and saves any remaining
      // data from that fetch. Remaining data will be returned with the
      // next fetch all.
      LOB_DML_SELECT_FETCH,

      // fetches data for the specified length. Does not save data.
      LOB_DML_SELECT_FETCH_AND_DONE,

      LOB_DML_SELECT_CLOSE,

      // does open, fetch, close.
      LOB_DML_SELECT_ALL,

      // returns length of lob given a lobhandle
      LOB_DML_SELECT_LOBLENGTH,
    };

  static const char* const qTypeStr[];
  const char* const getQTypeStr(DMLQueryType qt)
  {
    return qTypeStr[qt];
  };

  struct DDLArgs
  {
    /*IN*/     char * schName;
    /*IN*/     Lng32  schNameLen;
    /*IN*/     Lng32  *numLOBs;
    /*IN*/     DDLQueryType qType;
    /*IN*/     short *lobNumList;
    /*IN*/     short *lobTypList;
    /*IN*/     char* *lobLocList;
    /*IN*/     char* *lobColNameList;
    /*IN*/     Int32 *lobInlinedDataMaxLenList;
    /*IN*/     Int64 *lobHbaseDataMaxLenList;
    /*IN*/     char *lobHdfsServer;
    /*IN*/     Int32 lobHdfsPort;
    /*IN*/     Int64 lobMaxSize;
    /*IN*/     NABoolean lobTrace;
    /*IN*/     char * nameSpace; // namespace where lob tables are to be created
    /*IN*/     Int32 numSaltPartns;
    /*IN*/     Int32 replication; // 0=none, 1=sync, 2=async
    /*IN*/     Int32 numLOBdatafiles;
    /*IN*/     Int32 lobParallelDDL;  
    /*IN*/     Int32 dataInHbaseColLen;
    /*IN*/     Int32 lobChunkMaxLen;
    /*OUT*/    char errorInfo[1024];
  };

  Lng32 ddlInterface
  (
       /*IN*/     Int64  objectUID,
       /*IN*/     DDLArgs &args
   );

  // options and values set at code generation time and
  // used during query expression evaluation
  struct DMLOptions
  {
    Int64 lobSize;
    Int64 lobMaxSize;
    Int64 lobMaxChunkMemSize;
    Int64 lobGCLimit;
  
    // number of hdfs datafiles where lob data is stored.
    Int32 numLOBdatafiles;
    Int32 numSrcLOBdatafiles;
  
    Int32 lobDataInHbaseColLen;

    // lob data that can be inlined. 
    Int32 lobInlinedDataMaxLen;
  
    // lob data stored in hbase chunks table. 
    Int64 lobHbaseDataMaxLen;

    Int64 position_; // position from where to return or update data. 1 based.

    Int32 flags;
    Int32 filler1;

    // catalog.schema where lob chunks table is created
    short chunkTableSchNameLen;
    char  chunkTableSch[518];

    enum Options_Flags
      {
        LOB_XN_OPER   = 0x0001,
      };
    
    void setLobXnOper(NABoolean v) 
    {(v ? flags |= LOB_XN_OPER : flags &= ~LOB_XN_OPER); };
    NABoolean lobXnOper() { return ((flags & LOB_XN_OPER) != 0); };
  };

  Lng32 dmlInterface
  (
       /*IN*/     ExLobGlobals *lobGlobs,
       /*IN*/     char * inLobHandle,
       /*IN*/     Lng32  inLobHandleLen,
       /*IN*/     DMLQueryType qType,
       /*IN*/     DMLOptions &options,
       /*IN*/     char *lobHdfsStorageLocation,
       /*IN*/     CollHeap *heap,
       /*OUT*/    char *outHandle, // if passed in, returns modified handle
                                   // and inlined data if needed.
       /*OUT*/    Lng32 *outHandleLen, // length of modified handle + data
       /*INOUT*/  Int64 * dataOffset, /* IN: for insert, OUT: for select */
       /*INOUT*/  Int64 * dataLenPtr, /* length of data. */
                                      // IN: for insert, out: for select.
       /*INOUT*/  ExeCliInterface* *cliInterface = NULL
   );

private:
  enum Options_Flags
    {
      DATA_IN_HBASE   = 0x0001,
    };
  
  void setDataInHBase(NABoolean v) 
  {(v ? flags_ |= DATA_IN_HBASE : flags_ &= ~DATA_IN_HBASE); };
  NABoolean dataInHBase() { return ((flags_ & DATA_IN_HBASE) != 0); };
  
  Lng32 initLobPtr(ExLobGlobals *lobGlobs,
                   char *lobStorageLocation,
                   char *lobFileName,
                   ExLobV2* &lobPtr);
  
  Lng32 getLobAccessPtr(ExLobGlobals *lobGlobs,
                        char *lobStorageLocation,
                        char *lobFileName,
                        ExLobV2* &lobPtr);

  Lng32 insertData(ExLobGlobals *lobGlobs,
                   Int64 objectUID, 
                   char *inLobHandle, Int32 inLobHandleLen,
                   char *outLobHandle, Int32 *outLobHandleLen,
                   char *lobChunksTableName,
                   Int64 *lobUID, Int32 lobNum,
                   Int64 inDataLen, char *inDataPtr,
                   Int32 startChunkNum, 
                   ExpLobOperV2::DMLOptions &options,
                   char *lobHdfsStorageLocation,
                   NABoolean addToHdfsOnly,
                   NABoolean forUpdate,
                   ContextCli &currContext);
  
  Lng32 insertChunkInHbase(char *lobChunksTableName,
                           Int64 *lobUID, Int32 lobNum,
                           Int64 inDataLen, char *inDataPtr,
                           Int32 startChunkNum, 
                           Int32 &endChunkNum,
                           Int64 &insertedDataLen,
                           Int64 hdfsOffset,
                           DMLOptions &options,
                           NABoolean forUpdate,
                           ContextCli &currContext);

  Lng32 insertDataInHDFS(ExLobGlobals *lobGlobs,
                         Int64 inDataLen, char *inDataPtr,
                         DMLOptions &options,
                         Int64 objectUID, 
                         Int32 lobNum, Int64 *lobUID,
                         char *lobHdfsStorageLocation,
                         Int64 &hdfsOffset);

  Lng32 readDataFromHDFS(ExLobGlobals *lobGlobs,
                         Int64 outDataLen, char *outDataPtr,
                         DMLOptions &options,
                         Int64 objectUID,
                         Int32 lobNum, Int64 *lobUID,
                         char *lobHdfsStorageLocation,
                         Int64 hdfsOffset,
                         Int64 &dataLenReturned);

  Lng32 restoreCQDs(Lng32 cliRC);

  char *savedDataBuf_;
  Int64 savedDataBufLen_;
  char *savedDataPtr_;
  Int64 savedDataLen_;
  Int64 inlineDataLen_;
  Int64 flags_;

  // cliInterface pointers are used for:
  //   cliInterface1_: select
  //   cliInterface2_: insert
  //   cliInterface3_: insertUpd
  //   cliInterface4_: delete
  //   cliInterface5_: util
  //   cliInterface6_: cqds
  //   cliInterface7_: loblength
  ExeCliInterface *cliInterface1_;
  ExeCliInterface *cliInterface2_;
  ExeCliInterface *cliInterface3_;
  ExeCliInterface *cliInterface4_;
  ExeCliInterface *cliInterface5_;
  ExeCliInterface *cliInterface6_;
  ExeCliInterface *cliInterface7_;

};

#endif
