
#ifndef EXP_LOB_INTERFACE_H
#define EXP_LOB_INTERFACE_H

#include "ExpCompressionWA.h"
#include "comexe/ComQueue.h"
#include "executor/ExStats.h"
#include "executor/ex_globals.h"
#include "export/NAVersionedObject.h"

class HdfsFileInfo {
 public:
  HdfsFileInfo() {
    entryNum_ = -1;
    startOffset_ = -1;
    bytesToRead_ = 0;
    compressionTypeIx_ = 0;
    compressionMethod_ = ComCompressionInfo::UNKNOWN_COMPRESSION;
    flags_ = 0;
  }
  char *fileName() { return fileName_; }

  // used for text/seq file access
  long getStartOffset() { return startOffset_; }
  long getBytesToRead() { return bytesToRead_; }

  // used for ORC access
  long getStartRow() { return startOffset_; }
  long getNumRows() { return bytesToRead_; }

  // used for partitioned Hive tables
  const char *getPartColValues() const { return partColValues_; }
  Int16 getCompressionTypeIx() const { return compressionTypeIx_; }

  Int16 getCompressionMethod() const { return compressionMethod_; }

  int getFlags() { return flags_; }

  void setFileIsLocal(NABoolean v) { (v ? flags_ |= HDFSFILEFLAGS_LOCAL : flags_ &= ~HDFSFILEFLAGS_LOCAL); };
  NABoolean fileIsLocal() { return (flags_ & HDFSFILEFLAGS_LOCAL) != 0; };

  void setFileIsSplitBegin(NABoolean v) {
    (v ? flags_ |= HDFSFILE_IS_SPLIT_BEGIN : flags_ &= ~HDFSFILE_IS_SPLIT_BEGIN);
  };
  NABoolean fileIsSplitBegin() { return (flags_ & HDFSFILE_IS_SPLIT_BEGIN) != 0; };

  void setFileIsSplitEnd(NABoolean v) { (v ? flags_ |= HDFSFILE_IS_SPLIT_END : flags_ &= ~HDFSFILE_IS_SPLIT_END); };
  NABoolean fileIsSplitEnd() { return (flags_ & HDFSFILE_IS_SPLIT_END) != 0; };

  char *getSplitUnitBlockLocations() { return splitUnitBlockLocations_; };
  int getHdfsBlockNb() { return hdfsBlockNb_; };

  enum HdfsFileInfoFlags {
    HDFSFILEFLAGS_LOCAL = 0x0001,
    HDFSFILE_IS_SPLIT_BEGIN = 0x0002,
    HDFSFILE_IS_SPLIT_END = 0x0004
  };
  int entryNum_;  // 0 based, first entry is entry num 0.
  int flags_;
  NABasicPtr fileName_;
  long startOffset_;
  long bytesToRead_;
  NABasicPtr partColValues_;
  Int16 compressionTypeIx_;
  Int16 compressionMethod_;
  int hdfsBlockNb_;                     // used by StrawScan feature
  NABasicPtr splitUnitBlockLocations_;  // best hdfs block node index locations stored as string blank separated.
                                        // If stripe span accross multiple block, pick the one is biggest percentage of
                                        // file used by straw scan feature.
};

typedef HdfsFileInfo *HdfsFileInfoPtr;
typedef NAArray<HdfsFileInfoPtr> HdfsFileInfoArray;

class HdfsColInfo {
 public:
  HdfsColInfo(char *cname, int cnum, int ctype) : colName_(cname), colNum_(cnum), colType_(ctype) {}

  char *colName() { return colName_; }
  int colNumber() { return colNum_; }
  int colType() { return colType_; }

  int getFlags() { return flags_; }

  int colNum_;     // 0 based. Number of first col in file is 0.
  Int16 colType_;  // enum HiveProtoTypeKind defined in common/ComSmallDefs.h
  Int16 flags_;
  NABasicPtr colName_;
};

typedef HdfsFileInfo *HdfsFileInfoPtr;
typedef NAArray<HdfsFileInfoPtr> HdfsFileInfoArray;

#define LOB_ACCESS_SUCCESS 0
#define LOB_ACCESS_PREEMPT 1
enum ExpLOBinterfaceInputFlags {
  TRUNCATE_TGT_FILE_ = 0x0001,
  CREATE_TGT_FILE_ = 0x0002,
  ERROR_IF_TGT_FILE_EXISTS_ = 0x0004
};

char *getLobErrStr(int errEnum);

#endif
