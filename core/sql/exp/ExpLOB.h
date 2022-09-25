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
#ifndef EXP_LOB_EXPR_H
#define EXP_LOB_EXPR_H


/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ExpLOB.h
 * Description:  
 *               
 *               
 * Created:      11/30/2012
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */


#include "exp_clause.h"
#include "ExpLOBenums.h"
#include "ExpLobOperV2.h"

class ContextCli;
class ExLobGlobals;
class ExeCliInterface;

class ExLobInMemoryDescChunksEntry;


/////////////////////////////////////////
// Class ExpLOBoper                    //
/////////////////////////////////////////
class ExpLOBoper : public ex_clause {

public:
  // Construction
  //
  ExpLOBoper();
  ExpLOBoper(OperatorTypeEnum oper_type,
	     short num_operands,
	     Attributes ** attr,
	     Space * space);
  ~ExpLOBoper();

  virtual ex_expr::exp_return_type pCodeGenerate(Space *space, UInt32 f);

  // Display
  //
  virtual void displayContents(Space * space, const char * displayStr, 
			       Int32 clauseNum, char * constsArea);
  
  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID()
  {
    return 1;

  }

  virtual void populateImageVersionIDArray()
  {
    setImageVersionID(1,getClassVersionID());
    ex_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------

  // LOB hdfs name format for permanent lobs.
  // Length:  30 bytes
  // 
  //     LOBP_<objectUid>_<LOBnum>
  //          <---20----> <--4--->
  static char * ExpGetLOBname(Int64 uid, Lng32 lobNum,
			      char * outBuf, Lng32 outBufLen);

  // when multiple hdfs data files are created for a lob, this method
  // returns lob name suffixed with datafile number. 
  // datafile number is 1-based.
  // if <= 0, ExpGetLOBname is returned.
  // 
  //     LOBP_<objectUid>_<LOBnum>_<fileNum>
  //          <---20----> <--4--->_<--4--->
  static char * ExpGetLOBnameWithFilenum(Int64 uid, Lng32 lobNum, Lng32 fileNum,
                                         char * outBuf, Lng32 outBufLen);

  // if keyToHash is provided, hash it and generate lobname with filenum suffix
  static char * ExpGetLOBname(Int64 uid, Lng32 lobNum, Int64 keyToHash,
                              Int32 numHashVals,
                              char * outBuf, Lng32 outBufLen);
  // hdfs directory where all lobs for this uid(table) will be stored.
  // Format: LOBP_<objectUid>
  //              <----20---> 
  static char * ExpGetLOBDirName(Int64 uid, char * outBuf, Lng32 outBufLen);


  static char * ExpGetLOBDescName(Lng32 schNameLen, char * schName, 
				  Int64 uid, Lng32 lobNum, 
				  char * outBuf, Lng32 outBufLen);

  static char * ExpGetLOBDescHandleObjNamePrefix(Int64 uid, 
                                                             char * outBuf, Lng32 outBufLen);

  static char * ExpGetLOBDescHandleName(Lng32 schNameLen, char * schName, 
					Int64 uid, Lng32 lobNum, 
					char * outBuf, Lng32 outBufLen);
  
  static char * ExpGetLOBDescChunksName(Lng32 schNameLen, char * schName, 
					Int64 uid, Lng32 lobNum, 
					char * outBuf, Lng32 outBufLen);


  static Lng32 ExpGetLOBnumFromDescName(char * descName, Lng32 descNameLen);
  
  static char * ExpGetLOBMDName(Lng32 schNameLen, char * schName,
				Int64 uid,  
				char * outBuf, Lng32 outBufLen);
  static void calculateNewOffsets(ExLobInMemoryDescChunksEntry *dcArray, Lng32 numEntries);
  static Lng32 compactLobDataFile(ExLobGlobals *lobGlob, ExLobInMemoryDescChunksEntry *dcArray, Int32 numEntries, char *tgtLobName, Int64 lobMaxChunkSize, NAHeap *lobHeap, ContextCli *currContext,char *hdfsServer, Int32 hdfsPort,char *lobLocation);
  static Int32 restoreLobDataFile(ExLobGlobals *lobGlob, char *lobName, NAHeap *lobHeap, ContextCli *currContext,char *hdfsServer, Int32 hdfsPort,char *lobLocation );
  static Int32 purgeBackupLobDataFile(ExLobGlobals *lobGlob,char *lobName, NAHeap *lobHeap, ContextCli *currContext, char *hdfsServer, Int32 hdfsPort, char *lobLocation);

  static Lng32 createLOB(ExLobGlobals * lobGlob, ContextCli *currContext,
			 char * lobLoc, Int32 hdfsPort, char *hdfsServer,
			 Int64 uid, Lng32 lobNum, Int64 lobMAxSize,
                         Int32 numLOBdatafiles);

  static Lng32 dropLOB(ExLobGlobals * lobGlob, ContextCli *currContext, 
		       char * lobLoc,Int32 hdfsPort, char *hdfsServer,
		       Int64 uid, Lng32 lobNum,
                       Int32 numLOBdatafiles);

  static Lng32 purgedataLOB(ExLobGlobals * lobGlob, 
			    char * lobLob,
			    Int64 uid, Lng32 lobNum,
                            Int32 numLOBdatafiles);

  static Lng32 initLOBglobal(ExLobGlobals *& lobGlob, NAHeap *heap, ContextCli *currContext,char *server, Int32 port, NABoolean isHiveRead = FALSE);
  static ExLobGlobals *initLOBglobal(NAHeap *parentHeap, ContextCli *currContext, NABoolean useLibHdfs, NABoolean useHdfsWriteLock,
                                              short hdfsWriteLockTimeout, NABoolean isHiveRead);
  static void deleteLOBglobal(ExLobGlobals *lobGlob, NAHeap *parentHeap);
  static void genLobLockId(Int64 objUid,Int32 lobNum, char *llid);
  static void lobHandleWriteData(Int64  dataLen,
                                 char  *data,
                                 char  *handlePtr);
  static void lobHandleReadData(Int32 dataLen,
                                char  *data,
                                char  *handlePtr,
                                Int32 &readLen);
  static Int32 lobHandleGetLobType(char  *handlePtr);
  static Int32 lobHandleGetInlineDataLength(char *handlePtr);
  static char* lobHandleGetInlineDataPosition(char  *handlePtr){
      LOBHandle *lobHandle = (LOBHandle*)handlePtr;
      LOBHandleV2 *lobHandleV2 = (LOBHandleV2*)handlePtr;
      
      if (isLOBHflagSet(lobHandle->flags_, LOBH_VERSION2))
        return (char*)lobHandleV2 + lobHandleV2->inlineDataOffset_;
      else
        return (char *)((char *)lobHandle + sizeof(LOBHandle) + lobHandle->schNameLen_);
  }
  // Extracts values from the LOB handle stored at ptr
  static Lng32 extractFromLOBhandle(Int16 *flags,
				    Lng32 *lobType,
				    Lng32 *lobNum,
				    Int64 *uid, 
				    Int64 *descSyskey, 
				    Int64 *descPartnKey,
				    short *schNameLen,
				    char * schName,
				    char * ptrToLobHandle,
				    Lng32 handleLen = 0);

  static Lng32 extractFromLOBhandleV2(Int16 *flags,
                                      Lng32 *lobType,
                                      Lng32 *lobNum,
                                      Int64 *objUID,
                                      Int32 *inlineDataLen,
                                      Int32 *inlineDataOffset,
                                      Int64 *lobUID,
                                      Int64 *lobSeq,
                                      char * ptrToLobHandle);

  static Lng32 extractFromLOBstring(Int64 *uid, 
				    Int32 *lobNum,
				    Int64 *descPartnKey,
				    Int64 *descSyskey,
				    Int16 *flags,
				    Int32 *lobType,
				    short *schNameLen,
				    char  *schName,
                                    Int32 *inlineDataLen,
                                    char *&inlineData,
				    char * handle,
				    Int32 handleLen);

  static Lng32 extractFromLOBstringV2(Int16 *flags,
                                      Lng32 *lobType,
                                      Lng32 *lobNum,
                                      Int64 *objectUID,
                                      Int64 *lobUID,
                                      Int64 *lobSeq,
                                      char *handleStr,
                                      Lng32 handleLen);

  // objectUID must be atleast 20+1 bytes long
  static Lng32 extractObjectUIDFromHandleString(char *objectUID,
                                                NABoolean &lobV2,
                                                char *handleStr,
                                                Lng32 handleLen);

  static Lng32 extractFieldsFromHandleString(char *handleStr,
                                             Lng32 handleLen,
                                             Int32 *lobV2,
                                             Int64 *objectUID,
                                             Int32 *inlineDataLen,
                                             Int32 *inlineDataOffset);
  
  // Generates LOB handle that is stored in the SQL row.
  // LOB handle max len:  512 bytes
  // <flags><LOBtype><LOBnum><objectUid><descKey><descTS><schNameLen><schName>
  // <--4--><---4---><--4---><----8----><---8---><--8---><-----2----><--vc--->
  static void genLOBhandle(Int64 uid, 
			   Lng32 lobNum,
			   Int32 lobType,
			   Int64 descKey, 
			   Int64 descTS,
			   Lng32 flags,
			   short schNameLen,
			   char  * schName,
			   Lng32 &handleLen,
			   char * ptr);

  // Generates LOB handle that is stored in the SQL row.
  // Version_2:
  // <flags><LOBtype><LOBnum><objectUid><unused><lobUID><lobSeq>
  // <--4--><---4---><--4---><----8----><---8--><--8---><--8--->
  static void genLOBhandleV2(Int64 uid, 
                             Lng32 lobNum,
                             Int32 lobType,
                             Int64 lobUID,
                             Int64 lobSeq,
                             Lng32 flags,
                             Lng32 &handleLen,
                             char * ptr);

  static void updLOBhandleDescKey(Int64 descSyskey,                     
                                  char * ptr);
  static void updLOBhandleType(Int32 lobType, char *ptr);

  static void updLOBhandleInlineData(char * handlePtr, 
                                     Int32 inlineDataLen,
                                     char *inlineData);

  static Lng32 genLOBhandleFromHandleString(char * lobHandleString,
					    Lng32 lobHandleStringLen,
					    char * lobHandle,
					    Lng32 &lobHandleLen);

  static Lng32 genLOBhandleFromHandleStringV2(char * lobHandleString,
                                              Lng32 lobHandleStringLen,
                                              char * lobHandle,
                                              Lng32 &lobHandleLen);
  
  static void extractLOBhandleInlineData(char * handlePtr, 
                                         Int32 extractDataMaxLen,
                                         char *inlineData,
                                         Int32 &copyLen);

  short &lobNum() {return lobNum_; }

  ComLobsStorageType lobStorageType() { return (ComLobsStorageType)lobStorageType_; }
  void setLobStorageType(ComLobsStorageType v) { lobStorageType_ = (short)v; };

  char * lobStorageLocation() { return lobStorageLocation_; }
  void setLobStorageLocation(char * loc) { strcpy(lobStorageLocation_, loc); }

  char * srcLobStorageLocation() { return srcLobStorageLocation_; }
  void setSrcLobStorageLocation(char * loc) 
  { 
    strcpy(srcLobStorageLocation_, loc); 
  }

  Long pack(void *);
  Lng32 unpack(void *, void * reallocator);

  void setDescSchNameLen(short v) { descSchNameLen_ = v; }
  
  virtual Lng32 initClause();
  void setLobMaxSize(Int64 maxsize) { lobMaxSize_ = maxsize;}
  Int64 getLobMaxSize() { return lobMaxSize_;}
  void setLobSize(Int64 lobsize) { lobSize_ = lobsize;}
  Int64 getLobSize() { return lobSize_;}
  void setLobMaxChunkMemSize(Int64 maxsize) { lobMaxChunkMemSize_ = maxsize;}
  Int64 getLobMaxChunkMemSize() { return lobMaxChunkMemSize_;}
  void setLobGCLimit(Int64 gclimit) { lobGCLimit_ = gclimit;}
  Int64 getLobGCLimit() { return lobGCLimit_;}
  void setLobInlineLimit(Int32 limit) { lobInlineLimit_ = limit;}
  Int32 getLobInlineLimit() { return lobInlineLimit_;}
  void setLobHdfsServer(char *hdfsServer)
  {strcpy(lobHdfsServer_,hdfsServer);}
  void setLobHdfsPort(Int32 hdfsPort)
  {lobHdfsPort_ = hdfsPort;}

  NABoolean areMultiDatafiles() { return (numLOBdatafiles_ > 0); }
  Int32 getNumLOBdatafiles() { return numLOBdatafiles_; }
  void setNumLOBdatafiles(Int32 n) { numLOBdatafiles_ = n; }

  NABoolean areSrcMultiDatafiles() { return (numSrcLOBdatafiles_ > 0); }
  Int32 getSrcNumLOBdatafiles() { return numSrcLOBdatafiles_; }
  void setSrcNumLOBdatafiles(Int32 n) { numSrcLOBdatafiles_ = n; }

  void setSrcLobV2(NABoolean v) 
  {(v ? flags_ |= SRC_LOB_VERSION2 : flags_ &= ~SRC_LOB_VERSION2); };
  NABoolean srcLobV2() { return ((flags_ & SRC_LOB_VERSION2) != 0); };

  void setLobV2(NABoolean v) 
  {(v ? flags_ |= LOB_VERSION2 : flags_ &= ~LOB_VERSION2); };
  NABoolean lobV2() { return ((flags_ & LOB_VERSION2) != 0); };

  void setOptions(ExpLobOperV2::DMLOptions &options,
                  Int64 lobSize, Int64 lobMaxSize, Int64 lobMaxChunkMemSize,
                  Int64 lobGCLimit,
                  Int32 numLOBdatafiles, Int32 numSrcLOBdatafiles,
                  Int32 lobInlinedDataMaxLen, Int64 lobHbaseDataMaxLen,
                  Int32 lobDataInHbaseColLen, short schNameLen, char *schName)
  {
    options.lobSize = lobSize;
    options.lobMaxSize = lobMaxSize;
    options.lobMaxChunkMemSize = lobMaxChunkMemSize;
    options.lobGCLimit = lobGCLimit;
    options.numLOBdatafiles = numLOBdatafiles;
    options.numSrcLOBdatafiles = numSrcLOBdatafiles;
    options.lobInlinedDataMaxLen = lobInlinedDataMaxLen;
    options.lobHbaseDataMaxLen = lobHbaseDataMaxLen;
    options.lobDataInHbaseColLen = lobDataInHbaseColLen;
    options.chunkTableSchNameLen = schNameLen;
    options.chunkTableSch[0] = 0;
    if ((schNameLen > 0) && (schNameLen <= 512) && (schName))
      strcpy(options.chunkTableSch, schName);
    options.flags = 0;
    options.position_ = 1;
  }
  ExpLobOperV2::DMLOptions &getOptions() { return options_; }
  ExpLobOperV2::DMLOptions &getSrcOptions() { return srcOptions_; }

  // these flags are set in lob handle
  enum LOBH_Flags
    {
      LOBH_VERSION2 = 0x0001,
    };

  static NABoolean isLOBHflagSet(Int32 flags, LOBH_Flags flagbit)
  {
    return (flags & flagbit) != 0; 
  };

 protected:
  typedef enum
  {
    DO_NOTHING_,
    CHECK_STATUS_,
    START_LOB_OPER_
  } LobOperStatus;
  
  void createLOBhandleString(Int16 flags,
			     Int32 lobType,
			     Int64 uid, 
			     Lng32 lobNum,
			     Int64 descKey, 
			     Int64 descTS,
			     Int16 schNameLen,
			     char * schName,
			     char * ptrToLobHandle);

  void createLOBhandleStringV2(Int16 flags,
                               Lng32 lobType,
                               Lng32 lobNum,
                               Int64 uid, 
                               Int32 inlineDataLen,
                               Int32 inlineDataOffset,
                               Int64 lobUID,
                               Int64 lobSeq,
                               char * lobHandleBuf);

  struct LOBHandle
  {
    Int32 flags_;
    Int32 lobType_;
    Lng32 lobNum_;
    Int64 objUID_;
    Int64 descSyskey_;
    Int64 descPartnkey_;
    short schNameLen_;
    char  schName_;
  };

  // first 5 fields are at fixed offset for lob V1.
  struct LOBHandleV1ext
  {
    char header[4];
    char flags[4];
    char lobType[4];
    char lobNum[4];
    char objectUID[20];
  };

  struct LOBHandleV2
  {
    Int32 flags_;
    Int32 lobType_;
    Lng32 lobNum_;
    Int64 objUID_;
    Int32 inlineDataLen_;
    Int32 inlineDataOffset_;
    Int64 lobUID_;
    Int64 lobSeq_;
  };

  struct LOBHandleV2ext
  {
    char header[4];
    char flags[4];
    char lobType[4];
    char lobNum[4];
    char objectUID[20];
    char inlineDataLen[10];
    char inlineDataOffset[10];
    char lobUID[20];
    char lobSeq[20];
  };

  static void setLOBHflag(Int32 &flags, LOBH_Flags flagbit)
  {
    flags |= flagbit;
  };
  static void resetLOBHflag(Int32 &flags, LOBH_Flags flagbit)
  {
    flags &= ~flagbit;
  };
  Lng32 checkLobOperStatus();

protected:
  // these flags are set in expressions
  enum Flags
  {
    // lob related util operations are on newer lob format.
    LOB_VERSION2      = 0x0001,

    // operations where source table is involved (insert...select)
    SRC_LOB_VERSION2  = 0x0002,
  };

  char * descSchName() { return descSchName_; }

  char * getLobHdfsServer() { return (strlen(lobHdfsServer_) == 0 ? NULL : lobHdfsServer_); }
  Lng32 getLobHdfsPort() { return lobHdfsPort_; }
 

  short flags_;      // 00-02

  short lobNum_;
  
  short lobStorageType_;

  short lobHandleLenSaved_;

  // identifier returned by ExLobsOper during a nowaited operation. 
  // Used to check status of the request.
  Int64 requestTag_;

  char lobHandleSaved_[LOB_HANDLE_LEN];

  char outLobHandle_[LOB_HANDLE_LEN];
  Int32 outHandleLen_;

  char blackBox_[MAX_BLACK_BOX_LEN];
  Int64 blackBoxLen_;

  char lobStorageLocation_[MAX_LOB_FILE_NAME_LEN];

  // src lob location, if needed (ex: lob of select in an insert...select)
  char srcLobStorageLocation_[MAX_LOB_FILE_NAME_LEN];

  char lobHdfsServer_[256];
  Lng32 lobHdfsPort_;

  short descSchNameLen_;
  char  descSchName_[ComAnsiNamePart::MAX_IDENTIFIER_EXT_LEN+1];
  Int64 lobSize_;
  Int64 lobMaxSize_;
  Int64 lobMaxChunkMemSize_;
  Int64 lobGCLimit_;
  Int32 lobInlineLimit_;

  // number of hdfs datafiles where lob data is stored.
  Int32 numLOBdatafiles_;
  Int32 numSrcLOBdatafiles_;

  char filler0_[4];

  ExpLobOperV2::DMLOptions options_;
  ExpLobOperV2::DMLOptions srcOptions_;

  ExpLobOperV2 lobOperV2_;
}
;
typedef enum LOBInlineStatus
  {
    IL_SUCCESS=0,
    IL_ERROR,
    IL_TRANSITION_TO_OL   
  }LOBInline_status;

class ExpLOBiud : public ExpLOBoper {
 public:
  ExpLOBiud(OperatorTypeEnum oper_type,
	    Lng32 numAttrs,
	    Attributes ** attr, 
	    Int64 objectUID,
	    short descSchNameLen,
	    char * descSchName,
	    Space * space);
  ExpLOBiud();

  
  ex_expr::exp_return_type insertDesc(char *op_data[],
                                      char *lobData,
                                      Int64 lobLen,
                                      Int64 lobHdfsDataOffset,
                                      Int64 descTS,
                                      char *&result,
                                      CollHeap*h,
                                      ComDiagsArea** diagsArea);
  
  ex_expr::exp_return_type insertData(Lng32 handleLen,
                                      char *&handle,
                                      char *lobData,
                                      Int64 lobLen,
                                      Int64 descTS,
                                      Int32 numDataFiles,
                                      Int64 &hdfsDataOffset,
                                      CollHeap*h,
                                      ComDiagsArea** diagsArea);

  ex_expr::exp_return_type insertV2(char *op_data[],
                                    Int64 *lobUID,
                                    Int32 lobNum,
                                    Int32 chunkNum,
                                    Int64 lobLen,
                                    char *lobData,
                                    char *&result,
                                    CollHeap*h,
                                    ComDiagsArea** diagsArea
                                    );

  ex_expr::exp_return_type insertSelectFileV2(char * inLobHandle,
                                              Int32   inLobHandleLen,
                                              char *op_data[],
                                              Int64 *lobUID,
                                              Int32 lobNum,
                                              Int64 srcDataSize,
                                              char *&result,
                                              NABoolean forUpdate,
                                              CollHeap*h,
                                              ComDiagsArea** diagsArea
                                              );  

  LOBInline_status insertInlineData(char *op_data[],
                                    char *lobData,
                                    Int64 lobLen,
                                    CollHeap *h,
                                    ComDiagsArea **diagsArea);
  
  LOBInline_status updateInlineData(char *op_data[],
                                    char *lobData,
                                    Int64 lobLen,
                                    ComDiagsArea **diagsArea);
  LOBInline_status appendInlineData(char *op_data[],
                                    char *lobData,
                                    Int64 lobLen,
                                    ComDiagsArea **diagsArea);
  ex_expr::exp_return_type updateOutlineToInlineData();
  ex_expr::exp_return_type moveInlineToOutlineData(char *op_data[],CollHeap *h, ComDiagsArea **diagsArea );
  ex_expr::exp_return_type updateInlineToOutlineData(char *op_data[],CollHeap *h, ComDiagsArea **diagsArea );
  ex_expr::exp_return_type updateOutlineToInlineData(char *op_data[],char *data, Int32 lobLen,CollHeap *h, ComDiagsArea **diagsArea );

  NABoolean isAppend()
  {
    return ((liudFlags_ & IS_APPEND) != 0);
  };

  inline void setIsAppend(NABoolean v)
  {
    (v) ? liudFlags_ |= IS_APPEND: liudFlags_ &= ~IS_APPEND;
  };

  NABoolean fromString()
  {
    return ((liudFlags_ & FROM_STRING) != 0);
  };

  inline void setFromString(NABoolean v)
  {
    (v) ? liudFlags_ |= FROM_STRING: liudFlags_ &= ~FROM_STRING;
  };
  NABoolean fromBuffer()
  {
    return ((liudFlags_ & FROM_BUFFER) != 0);
  };
  inline void setFromBuffer(NABoolean v)
  {
    (v) ? liudFlags_ |= FROM_BUFFER: liudFlags_ &= ~FROM_BUFFER;
  };
  inline void setFromEmpty(NABoolean v)
  {
    (v) ? liudFlags_ |= FROM_EMPTY: liudFlags_ &= ~FROM_EMPTY;
  };
  NABoolean fromEmpty()
  {
    return ((liudFlags_ & FROM_EMPTY) != 0);
  };
 
  NABoolean fromFile()
  {
    return ((liudFlags_ & FROM_FILE) != 0);
  };

  inline void setFromFile(NABoolean v)
  {
    (v) ? liudFlags_ |= FROM_FILE: liudFlags_ &= ~FROM_FILE;
  };

  NABoolean fromLoad()
  {
    return ((liudFlags_ & FROM_LOAD) != 0);
  };

  inline void setFromLoad(NABoolean v)
  {
    (v) ? liudFlags_ |= FROM_LOAD: liudFlags_ &= ~FROM_LOAD;
  };

  NABoolean fromLob()
  {
    return ((liudFlags_ & FROM_LOB) != 0);
  };

  inline void setFromLob(NABoolean v)
  {
    (v) ? liudFlags_ |= FROM_LOB: liudFlags_ &= ~FROM_LOB;
  };
  NABoolean fromLobExternal()
  {
    return ((liudFlags_ & FROM_LOB_EXTERNAL) != 0);
  };

  inline void setFromLobExternal(NABoolean v)
  {
    (v) ? liudFlags_ |= FROM_LOB_EXTERNAL: liudFlags_ &= ~FROM_LOB_EXTERNAL;
  };

  NABoolean fromExternal()
  {
    return ((liudFlags_ & FROM_EXTERNAL) != 0);
  };

  inline void setFromExternal(NABoolean v)
  {
    (v) ? liudFlags_ |= FROM_EXTERNAL: liudFlags_ &= ~FROM_EXTERNAL;
  };
 
 NABoolean lobLocking()
  {
    return ((liudFlags_ & LOB_LOCKING) != 0);
  };

  inline void setLobLocking(NABoolean v)
  {
    (v) ? liudFlags_ |= LOB_LOCKING: liudFlags_ &= ~LOB_LOCKING;
  };
  ex_expr::exp_return_type getLobInputLength(char *op_data[],Int64 &length,CollHeap *h,ComDiagsArea **diagsArea);
  ex_expr::exp_return_type getLobInputLengthV2(char *op_data[],
                                               Int64 &length,
                                               CollHeap *h,
                                               ComDiagsArea **diagsArea);
  void getLobInputData(char *op_data[], char*&lobData, Int64 lobLen,CollHeap *h);
 
 protected:
  Int64 objectUID_;

  enum
  {
    IS_APPEND          = 0x0001,
    FROM_STRING        = 0x0002,
    FROM_FILE          = 0x0004,
    FROM_LOAD          = 0x0008,
    FROM_LOB           = 0x0010,
    FROM_EXTERNAL      = 0x0020,
    FROM_BUFFER        = 0x0040,
    FROM_EMPTY         = 0x0080,
    FROM_LOB_EXTERNAL  = 0x0100,
    LOB_LOCKING        = 0x0200

  };

  Lng32 liudFlags_;
  char filler1_[4];
};

class ExpLOBinsert : public ExpLOBiud {
public:
  ExpLOBinsert(OperatorTypeEnum oper_type,
	       Lng32 numAttrs,
	       Attributes ** attr, 
	       Int64 objectUID,
	       short descSchNameLen,
	       char * descSchName,
	       Space * space);
  ExpLOBinsert();


  virtual ex_expr::exp_return_type eval(char *op_data[],
					CollHeap*,
					ComDiagsArea** diagsArea = 0);

  ex_expr::exp_return_type evalV2(char *op_data[],
                                  CollHeap*,
                                  ComDiagsArea** diagsArea = 0);

  ex_expr::exp_return_type insertSelectV1fromV2(char *op_data[],
                                                CollHeap*h,
                                                ComDiagsArea** diagsArea
                                                );  

  ex_expr::exp_return_type insertSelectV2fromV1(char *op_data[],
                                                Int64 *lobUID,
                                                Int32 lobNum,
                                                char *&result,
                                                CollHeap*h,
                                                ComDiagsArea** diagsArea
                                                );  

  ex_expr::exp_return_type insertSelectV2(char *op_data[],
                                          Int64 *lobUID,
                                          Int32 lobNum,
                                          char *&result,
                                          CollHeap*h,
                                          ComDiagsArea** diagsArea
                                          );  

  // Display
  //
  virtual void displayContents(Space * space, const char * displayStr, 
			       Int32 clauseNum, char * constsArea);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID()
  {
    return 1;
  }

  virtual void populateImageVersionIDArray()
  {
    setImageVersionID(2,getClassVersionID());
    ExpLOBoper::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
  
 private:
 
  Lng32 liFlags_;
  char filler1_[4];
 
};

class ExpLOBdelete : public ExpLOBiud {
public:
  ExpLOBdelete(OperatorTypeEnum oper_type,
	       Attributes ** attr, 
	       Space * space);
  ExpLOBdelete();

  virtual ex_expr::exp_return_type eval(char *op_data[],
					CollHeap*,
					ComDiagsArea** diagsArea = 0);
  
  ex_expr::exp_return_type evalV2(char *op_data[],
                                  CollHeap*,
                                  ComDiagsArea** diagsArea = 0);

  Int32 isNullRelevant() const { return 0; };

  // Display
  //
  virtual void displayContents(Space * space, const char * displayStr, 
			       Int32 clauseNum, char * constsArea);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID()
  {
    return 1;
  }

  virtual void populateImageVersionIDArray()
  {
    setImageVersionID(2,getClassVersionID());
    ExpLOBoper::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------

 private:

  Lng32 ldFlags_;
  char  filler1_[4];
};

class ExpLOBupdate : public ExpLOBiud {
public:
  ExpLOBupdate(OperatorTypeEnum oper_type,
	       Lng32 numAttrs,
	       Attributes ** attr, 
	       Int64 objectUID,
	       short descSchNameLen,
	       char * descSchName,
	       Space * space);
  ExpLOBupdate();

  // Null Semantics
  //
  Int32 isNullInNullOut() const { return 0; };
  Int32 isNullRelevant() const { return 1; };

  virtual ex_expr::exp_return_type processNulls(char *op_data[], CollHeap *heap,
						ComDiagsArea **diagsArea);

  ex_expr::exp_return_type processNullsV2(char *op_data[], CollHeap *heap,
                                          ComDiagsArea **diagsArea);
  
  virtual ex_expr::exp_return_type eval(char *op_data[],
					CollHeap*,
					ComDiagsArea** diagsArea = 0);

  ex_expr::exp_return_type evalV2(char *op_data[],
                                  CollHeap*,
                                  ComDiagsArea** diagsArea = 0);

  // Display
  //
  virtual void displayContents(Space * space, const char * displayStr, 
			       Int32 clauseNum, char * constsArea);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID()
  {
    return 1;
  }

  virtual void populateImageVersionIDArray()
  {
    setImageVersionID(2,getClassVersionID());
    ExpLOBoper::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------


 private:
  Lng32 luFlags_;
  short nullHandle_;
  char  filler1_[2];
};

class ExpLOBselect : public ExpLOBoper {
public:
  ExpLOBselect(OperatorTypeEnum oper_type,
	       Attributes ** attr, 
	       Space * space);
  ExpLOBselect();

  virtual ex_expr::exp_return_type eval(char *op_data[],
					CollHeap*,
					ComDiagsArea** diagsArea = 0);
  
  // Display
  //
  virtual void displayContents(Space * space, const char * displayStr, 
			       Int32 clauseNum, char * constsArea);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID()
  {
    return 1;
  }

  virtual void populateImageVersionIDArray()
  {
    setImageVersionID(2,getClassVersionID());
    ExpLOBoper::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
   NABoolean toLob()
  {
    return ((lsFlags_ & TO_LOB) != 0);
  };

  inline void setToLob(NABoolean v)
  {
    (v) ? lsFlags_ |= TO_LOB: lsFlags_ &= ~TO_LOB;
  };
  NABoolean toFile()
  {
    return ((lsFlags_ & TO_FILE) != 0);
  };

  inline void setToFile(NABoolean v)
  {
    (v) ? lsFlags_ |= TO_FILE: lsFlags_ &= ~TO_FILE;
  };
  void setTgtFile(char *tgtFile)
  {
    strcpy(tgtFile_,tgtFile);
  }
  char *getTgtFile()
  {
    return tgtFile_;
  }
  void setTgtLocation(char *tgtLoc)
  {
    strcpy(tgtLocation_,tgtLoc);
  }
  char *getTgtLocation()
  {
    return tgtLocation_;
  }
  
 private:
   enum
  {
    
    TO_LOB             = 0x0001,
    TO_FILE            = 0x0002
  };
 
 
  Lng32 lsFlags_;
  char  filler1_[4];
  char tgtLocation_[512];
  char tgtFile_[512];
};

class ExpLOBconvert : public ExpLOBoper {
public:
  ExpLOBconvert(OperatorTypeEnum oper_type,
		Attributes ** attr, 
		Space * space);
  ExpLOBconvert();

  virtual ex_expr::exp_return_type eval(char *op_data[],
					CollHeap*,
					ComDiagsArea** diagsArea = 0);
  
  ex_expr::exp_return_type evalV2(char *op_data[],
                                  CollHeap*,
                                  ComDiagsArea** diagsArea = 0);

  // Display
  //
  virtual void displayContents(Space * space, const char * displayStr, 
			       Int32 clauseNum, char * constsArea);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID()
  {
    return 1;
  }

  virtual void populateImageVersionIDArray()
  {
    setImageVersionID(2,getClassVersionID());
    ExpLOBoper::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------

  NABoolean toString()
  {
    return ((lcFlags_ & TO_STRING) != 0);
  };

  inline void setToString(NABoolean v)
  {
    (v) ? lcFlags_ |= TO_STRING: lcFlags_ &= ~TO_STRING;
  };

  NABoolean toLob()
  {
    return ((lcFlags_ & TO_LOB) != 0);
  };

  inline void setToLob(NABoolean v)
  {
    (v) ? lcFlags_ |= TO_LOB: lcFlags_ &= ~TO_LOB;
  };
  NABoolean toFile()
  {
    return ((lcFlags_ & TO_FILE) != 0);
  };

  inline void setToFile(NABoolean v)
  {
    (v) ? lcFlags_ |= TO_FILE: lcFlags_ &= ~TO_FILE;
  };
   void setConvertSize(Int64 size)
  {
    convertSize_ = size;
  }
  Int64 getConvertSize()
  {
    return convertSize_;
  }
  void setTgtFile(char *tgtFile)
  {
    tgtFileName_ = tgtFile;
  }
  char *getTgtFile()
  {
    return tgtFileName_;
  }

private:
  enum
  { 
    TO_STRING          = 0x0001,
    TO_LOB             = 0x0002,
    TO_FILE            = 0x0004
  };
 

  Lng32 lcFlags_;
  char  filler1_[4];
  Int64 convertSize_;
  char * tgtFileName_;
};

class ExpLOBconvertHandle : public ExpLOBoper {
public:
  ExpLOBconvertHandle(OperatorTypeEnum oper_type,
		      Attributes ** attr, 
		      Space * space);
  ExpLOBconvertHandle();

  virtual ex_expr::exp_return_type eval(char *op_data[],
					CollHeap*,
					ComDiagsArea** diagsArea = 0);
  
  // Display
  //
  virtual void displayContents(Space * space, const char * displayStr, 
			       Int32 clauseNum, char * constsArea);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID()
  {
    return 1;
  }
 
  virtual void populateImageVersionIDArray()
  {
    setImageVersionID(2,getClassVersionID());
    ExpLOBoper::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------

  NABoolean toHandleString()
  {
    return ((lchFlags_ & TO_HANDLE_STRING) != 0);
  };

  inline void setToHandleString(NABoolean v)
  {
    (v) ? lchFlags_ |= TO_HANDLE_STRING: lchFlags_ &= ~TO_HANDLE_STRING;
  };
  NABoolean toHandleStringWithData()
  {
    return ((lchFlags_ & TO_HANDLE_STRING_WITH_DATA) != 0);
  };

  inline void setToHandleStringWithData(NABoolean v)
  {
    (v) ? lchFlags_ |= TO_HANDLE_STRING_WITH_DATA: lchFlags_ &= ~TO_HANDLE_STRING_WITH_DATA;
  };

  NABoolean toLob()
  {
    return ((lchFlags_ & TO_LOB) != 0);
  };

  inline void setToLob(NABoolean v)
  {
    (v) ? lchFlags_ |= TO_LOB: lchFlags_ &= ~TO_LOB;
  };
 private:
  enum
  {
    TO_LOB             = 0x0001,
    TO_HANDLE_STRING   = 0x0002,
    TO_HANDLE_STRING_WITH_DATA = 0x0004,
  };


  Lng32 lchFlags_;
  char  filler1_[4];
};

class ExpLOBlength : public ExpLOBoper {
public:
  ExpLOBlength(OperatorTypeEnum oper_type,
               Attributes ** attr, 
               Space * space);
  ExpLOBlength();

  virtual ex_expr::exp_return_type eval(char *op_data[],
					CollHeap*,
					ComDiagsArea** diagsArea = 0);
  
  ex_expr::exp_return_type evalV2(char *op_data[],
                                  CollHeap*,
                                  ComDiagsArea** diagsArea = 0);

  // Display
  //
  virtual void displayContents(Space * space, const char * displayStr, 
			       Int32 clauseNum, char * constsArea);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID()
  {
    return 1;
  }
 
  virtual void populateImageVersionIDArray()
  {
    setImageVersionID(2,getClassVersionID());
    ExpLOBoper::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------

 private:
  enum
  {
    OCTET_LENGTH             = 0x0001,
    CHAR_LENGTH              = 0x0002,
  };

  Lng32 llFlags_;
  char  filler1_[4];
};

/////////////////////////////////////////
// Class ExpLOBfunction                //
/////////////////////////////////////////
class ExpLOBfunction : public ExpLOBoper {

public:
  // Construction
  //
  ExpLOBfunction();
  ExpLOBfunction(OperatorTypeEnum oper_type,
		 short num_operands,
		 Attributes ** attr,
		 Space * space);

 protected:

 private:
  Int64 funcFlags_;
}
;

/////////////////////////////////////////
// Class ExpLOBfuncSubstring                
/////////////////////////////////////////
class ExpLOBfuncSubstring : public ExpLOBfunction {

public:
  // Construction
  //
  ExpLOBfuncSubstring();
  ExpLOBfuncSubstring(OperatorTypeEnum oper_type,
		      short num_operands,
		      Attributes ** attr,
		      Space * space);

  // Display
  //
  virtual void displayContents(Space * space, const char * displayStr, 
			       Int32 clauseNum, char * constsArea);

  virtual ex_expr::exp_return_type eval(char *op_data[], CollHeap*, 
					ComDiagsArea** = 0); 
  
  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID()
  {
    return 1;
  }

  virtual void populateImageVersionIDArray()
  {
    setImageVersionID(2,getClassVersionID());
    ExpLOBoper::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
 
 private:
}
;

#endif
