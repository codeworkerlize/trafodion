
#ifndef EXP_LOB_INTERFACE_H
#define EXP_LOB_INTERFACE_H

#include "NAVersionedObject.h"
#include "ComQueue.h"
#include "ExpCompressionWA.h"
#include "ex_globals.h"
#include "ExStats.h"

class HdfsFileInfo
{
 public:
  HdfsFileInfo() {
     entryNum_ = -1;
     startOffset_ = -1;
     bytesToRead_ = 0;
     compressionTypeIx_ = 0;
     compressionMethod_ = ComCompressionInfo::UNKNOWN_COMPRESSION;
     flags_ = 0;
  }
  char * fileName() { return fileName_; }

  // used for text/seq file access
  Int64 getStartOffset() { return startOffset_; }
  Int64 getBytesToRead() { return bytesToRead_; }

  // used for ORC access
  Int64 getStartRow() { return startOffset_; }
  Int64 getNumRows() { return bytesToRead_; }

  // used for partitioned Hive tables
  const char *getPartColValues() const { return partColValues_; }
  Int16 getCompressionTypeIx() const { return compressionTypeIx_; }

  Int16 getCompressionMethod() const { return compressionMethod_; }

  Lng32 getFlags() { return flags_; }

  void setFileIsLocal(NABoolean v)
  {(v ? flags_ |= HDFSFILEFLAGS_LOCAL : flags_ &= ~HDFSFILEFLAGS_LOCAL); };
  NABoolean fileIsLocal() { return (flags_ & HDFSFILEFLAGS_LOCAL) != 0; };

  void setFileIsSplitBegin(NABoolean v)
  {(v ? flags_ |= HDFSFILE_IS_SPLIT_BEGIN : flags_ &= ~HDFSFILE_IS_SPLIT_BEGIN); };
  NABoolean fileIsSplitBegin() { return (flags_ & HDFSFILE_IS_SPLIT_BEGIN) != 0; };

  void setFileIsSplitEnd(NABoolean v)
  {(v ? flags_ |= HDFSFILE_IS_SPLIT_END : flags_ &= ~HDFSFILE_IS_SPLIT_END); };
  NABoolean fileIsSplitEnd() { return (flags_ & HDFSFILE_IS_SPLIT_END) != 0; };

  char * getSplitUnitBlockLocations() {return splitUnitBlockLocations_;};
  Lng32 getHdfsBlockNb() {return hdfsBlockNb_;};


  enum HdfsFileInfoFlags 
  { 
    HDFSFILEFLAGS_LOCAL          = 0x0001,
    HDFSFILE_IS_SPLIT_BEGIN      = 0x0002,
    HDFSFILE_IS_SPLIT_END        = 0x0004
  };
  Lng32 entryNum_; // 0 based, first entry is entry num 0.
  Lng32 flags_;
  NABasicPtr  fileName_;
  Int64 startOffset_;
  Int64 bytesToRead_;
  NABasicPtr partColValues_;
  Int16 compressionTypeIx_;
  Int16 compressionMethod_;
  Lng32 hdfsBlockNb_;         // used by StrawScan feature
  NABasicPtr splitUnitBlockLocations_; // best hdfs block node index locations stored as string blank separated.
  	  	  	  	  	  	      // If stripe span accross multiple block, pick the one is biggest percentage of file
  	  	  	  	  	  	  	  // used by straw scan feature.

};

typedef HdfsFileInfo* HdfsFileInfoPtr;
typedef NAArray<HdfsFileInfoPtr> HdfsFileInfoArray;

class HdfsColInfo
{
 public:
  HdfsColInfo(char * cname, Lng32 cnum, Lng32 ctype) :
       colName_(cname), colNum_(cnum), colType_(ctype)
  {}

  char * colName() { return colName_; }
  Lng32  colNumber() { return colNum_; }
  Lng32  colType() { return colType_; }

  Lng32 getFlags() { return flags_; }

  Lng32 colNum_; // 0 based. Number of first col in file is 0.
  Int16 colType_; // enum HiveProtoTypeKind defined in common/ComSmallDefs.h
  Int16 flags_;
  NABasicPtr colName_;
};

typedef HdfsFileInfo* HdfsFileInfoPtr;
typedef NAArray<HdfsFileInfoPtr> HdfsFileInfoArray;

#include "ExpLOBaccess.h"

#define LOB_ACCESS_SUCCESS 0
#define LOB_ACCESS_PREEMPT 1
enum ExpLOBinterfaceInputFlags
  {
    TRUNCATE_TGT_FILE_ =        0x0001,
    CREATE_TGT_FILE_   =        0x0002,
    ERROR_IF_TGT_FILE_EXISTS_ =  0x0004
  };

Lng32 ExpLOBinterfaceInit(ExLobGlobals *& lobGlob, NAHeap *lobHeap, ContextCli *currContext,NABoolean isHiveRead, char *hdfsServer=(char *)"default", Int32 port=0);

Lng32 ExpLOBinterfaceCleanup(ExLobGlobals *& lobGlob);

Lng32 ExpLOBinterfaceCreate(ExLobGlobals * lobGlob, 
			    char * lobName,
			    char * lobLoc,
			    Lng32 lobType = (Lng32)Lob_Outline,
			    char * lobHdfsServer = (char *)"default",
			    Int64 lobMaxSize = 0,
			    Lng32 lobHdfsPort = 0,
	                    int    bufferSize = 0,
	                    short  replication =0,
	                    int    blocksize=0);

Lng32 ExpLOBinterfaceDrop(ExLobGlobals * lobGlob,
			  char * lobHdfsServer ,
			  Lng32 lobHdfsPort ,
			  char * lobName,
			  char * lobLoc);

Lng32 ExpLOBInterfacePurgedata(ExLobGlobals * lobGlob, 			      
			       char * lobName,
			       char * lobLoc);

Lng32 ExpLOBinterfaceCloseFile(ExLobGlobals * lobGlob, 
                               ExHdfsScanStats *hdfsAccessStats,
			       char * lobName,
			       char * lobLoc,
			       Lng32 lobType,
			       char * lobHdfsServer ,
			       Lng32 lobHdfsPort );

Lng32 ExpLOBInterfaceInsertSelect(ExLobGlobals * exLobGlob, 
                                  ExHdfsScanStats *hdfsAccessStats,
				  char * lobHdfsServer ,
				  Lng32 lobHdfsPort ,
				  char * tgtLobName,
                                  LobsSubOper so,
				  char * lobStorageLocation,
                                  char * srcLobStorageLocation,
                                  Int32 lobType,
                                  Int64 xnId,
				   Lng32 handleLen,
				  char * lobHandle,
                                  Int32 * outHandleLen,
                                  char * outLobHandle,
                                  char * lobData,
                                  Int64 lobDataLen,
                                  char *blackBox,
                                  Int64 blackBoxLen,
                                  Int64 lobMaxSize,
                                  Int64 lobMaxChunkMemSize ,
                                  Int64 lobGCLimit ,
                                  int    bufferSize = 0,
                                  short  replication =0,
                                  int    blocksize=0,
                                  Int32  numSrcLOBdatafiles=-1
                                  );

Lng32 ExpLOBInterfaceInsert(ExLobGlobals * lobGlob, 
                            ExHdfsScanStats *hdfsAccessStats,
			    char * tgtLobName,
			    char * lobLocation,
			    Lng32 lobType,
			    char * lobHdfsServer,
			    Lng32 lobHdfsPort,

			    Lng32 handleLen,
			    char * lobHandle,
			    Int32 * outHandleLen,
			    char * outLobHandle,

			    Int64 blackBoxLen,
			    char * blackBox,

			    Int64 &requestTag,
			    Int64 xnId,

			    Int64 &tgtDescSyskey,
			    LobsOper lo,

			    Lng32 * cliError = 0,

			    LobsSubOper so = Lob_Memory,
			    Lng32 waited = 0,

			    char * srcLobData = NULL, 
			    Int64  srcLobLen  = 0,
			    Int64 lobMaxSize = 0,
			    Int64 lobMaxChunkMemSize = 0,
                            Int64 lobGCLimit = 0,
			    int    bufferSize = 0,
			    short  replication =0,
			    int    blocksize=0
			    );

Lng32 ExpLOBInterfaceUpdate(ExLobGlobals * lobGlob, 
                            ExHdfsScanStats *hdfsAccessStats,
			    char * lobHdfsServer ,
			    Lng32 lobHdfsPort,	 
			    char * tgtLobName,
			    char * lobLocation,
			    Lng32 handleLen,
			    char * lobHandle,
			    Int32 *outHandleLen,
			    char * outLobHandle,
			    Int64 &requestTag,
			    Int64 xnId,	
			    
			    Lng32 checkStatus,
			    Lng32 waitedOp,

			    LobsSubOper so,
			    Int64 &tgtDescSyskey,

			    Int64 tgtLobLen,

			    char * srcLobData, 
			    char * srcLobName, 
			    short srcDescSchNameLen,
			    char * srcDescSchName,
			    Int64 srcDescKey, 
			    Int64 srcDescTS,
			    Int64 lobMaxSize = 0,
			    Int64 lobMaxChunkMemSize = 0,
                            Int64 lobGCLimit = 0);

Lng32 ExpLOBInterfaceUpdateAppend(ExLobGlobals * lobGlob, 
                                  ExHdfsScanStats *hdfsAccessStats,
				  char * lobHdfsServer ,
				  Lng32 lobHdfsPort ,
				  char * tgtLobName,
				  char * lobLocation,
				  Lng32 handleLen,
				  char * lobHandle,
				  Int32 *outHandleLen,
				  char * outLobHandle,
				  Int64 &requestTag,
				  Int64 xnId,	
				  
				  Lng32 checkStatus,
				  Lng32 waitedOp,
				  
				  LobsSubOper so,
				  Int64 &tgtDescSyskey,
				  
				  Int64 tgtLobLen,
				  
				  char * srcLobData, 
				  char * srcLobName, 
				  short srcDescSchNameLen,
				  char * srcDescSchName,
				  Int64 srcDescKey, 
				  Int64 srcDescTS,
				  Int64 lobMaxSize = 0,
				  Int64 lobMaxChunkMemSize = 0,
                                  Int64 lobGCLimit = 0
				  );

Lng32 ExpLOBInterfaceDelete(ExLobGlobals * lobGlob, 
                            ExHdfsScanStats *hdfsAccessStats,
			    char * lobHdfsServer ,
			    Lng32 lobHdfsPort ,
			    char * lobName,
			    char * lobLocation,
			    Lng32 handleLen,
			    char * lobHandle,
			    Int64 &requestTag,
			    Int64 xnId,
			    Int64 descSyskey,
			    Lng32 checkStatus,
			    Lng32 waitedOp);

Lng32 ExpLOBInterfaceSelect(ExLobGlobals * lobGlob, 
                            ExHdfsScanStats *hdfsAccessStats,
			    char * lobName, 
			    char * lobLoc,
			    Lng32  lobType,
			    char * lobHdfsServer,
			    Lng32 lobHdfsPort,
                           
			    Lng32 handleLen,
			    char * lobHandle,
			    Int64 &requestTag,
                            LobsSubOper so,
			    Int64 xnId,
			    Lng32 checkStatus,
			    Lng32 waited,

			    Int64 offset, Int64 inLen, 
			    Int64 &outLen, char * lobData,
			    Int64 lobMaxChunkMemlen,
			    Int32 inputFlags=0);

Lng32 ExpLOBInterfaceSelectCursor(ExLobGlobals * lobGlob, 
                                  ExHdfsScanStats *hdfsAccessStats,
				  char * lobName, 
				  char * lobLoc,
				  Lng32 lobType,
				  char * lobHdfsServer,
				  Lng32 lobHdfsPort,

				  Int32 handleLen,  
				  char * lobHandle,
				  Int64 cusrorBytes,
				  char *cursorId,
				  Int64 &requestTag,
				  LobsSubOper so,
				  Lng32 checkStatus,
				  Lng32 waitedOp,

                                  Int64 offset, Int64 inLen, 
			          Int64 &outLen, Int64 &uncompressedOutLen,
				  char * lobData,
				  ExpCompressionWA * compressionWA,
				  Lng32 oper, // 1: open. 2: fetch. 3: close
                                  Lng32 openType, // 0: not applicable. 1: preOpen. 2: mustOpen. 
                                  Int32 *hdfsDetailError = NULL
				  );

char * getLobErrStr(Lng32 errEnum);

Lng32 ExpLOBinterfacePerformGC(ExLobGlobals *& lobGlob, char *lobName,void *descChunksArray, Int32 numEntries, char *hdfsServer, Int32 hdfsPort,char *LOBlOC,Int64 lobMaxChunkMemSize);
Lng32 ExpLOBinterfaceRestoreLobDataFile(ExLobGlobals *& lobGlob, char *hdfsServer, Int32 hdfsPort,char *lobLoc,char *lobName);
Lng32 ExpLOBinterfacePurgeBackupLobDataFile(ExLobGlobals *& lobGlob,  char *hdfsServer, Int32 hdfsPort,char *lobLoc,char *lobName);

Lng32 ExpLOBinterfaceEmptyDirectory(ExLobGlobals * lobGlob,
                            char * lobName,
                            char * lobLoc,
                            Lng32 lobType = (Lng32)Lob_Empty_Directory,
			    char * lobHdfsServer = (char *)"default",
                            Lng32 lobHdfsPort = 0,
                            int    bufferSize = 0,
                            short  replication =0,
                            int    blocksize=0);

Lng32 ExpLOBInterfaceGetLobLength(ExLobGlobals * exLobGlob, 
				  char * lobName, 
				  char * lobLoc,
				  Lng32 lobType,
				  char * lobHdfsServer,
				  Lng32 lobHdfsPort,
				  Int32 handleLen, 
				  char * lobHandle,
			          Int64 &outLobLen 
                                  
				  );
Lng32 ExpLOBInterfaceGetFileSize(ExLobGlobals * exLobGlob, 
                                 char * filename,
                                 char * lobHdfsServer,
                                 Lng32 lobHdfsPort,	
                                 Int64 &outFileSize
                                 
                                 );
Lng32 ExpLOBInterfaceReadSourceFile(ExLobGlobals * exLobGlob,
                                     char * filename, 
                                     char * lobHdfsServer,
                                     Lng32 lobHdfsPort,	      
                                     Int64 readOffset,
                                     Int64 allocMemSize,
                                     char *&retBuf,
                                     Int64 &retReadLen);



Lng32 ExpLOBInterfaceGetFileName(ExLobGlobals * exLobGlob, 
				  char * lobName, 
				  char * lobLoc,
				  Lng32 lobType,
				  char * lobHdfsServer,
				  Lng32 lobHdfsPort,
				  Int32 handleLen, 
                                  char * lobHandle,  
                                 char * outFileName,
                                  Int32 &outFileLen);

Lng32 ExpLOBInterfaceGetOffset(ExLobGlobals * exLobGlob, 
				  char * lobName, 
				  char * lobLoc,
				  Lng32 lobType,
				  char * lobHdfsServer,
				  Lng32 lobHdfsPort,
				  Int32 handleLen, 
				  char * lobHandle,
			          Int64 &outLobOffset 
                                  
				  );

#endif




