

#ifndef COM_COMPRESSION_INFO_H
#define COM_COMPRESSION_INFO_H

#include "export/NAVersionedObject.h"

class ComCompressionInfo : public NAVersionedObject {
 public:
  // Update the COMPRESSION_TYPE[] at org/trafodion/sql/HDFSClient.java when new enum is added
  enum CompressionMethod {
    UNKNOWN_COMPRESSION = 0,  // unable to determine compression method
    UNCOMPRESSED,             // file is not compressed
    LZO_DEFLATE,              // using LZO deflate compression
    DEFLATE,                  // using DEFLATE compression
    GZIP,                     // using GZIP compression
    LZOP,                     // using LZOP compression
    SUPPORTED_COMPRESSIONS
  };  // Add any compression type above this line

  ComCompressionInfo(CompressionMethod cm = UNKNOWN_COMPRESSION) : NAVersionedObject(-1), compressionMethod_(cm) {}

  virtual ~ComCompressionInfo();

  bool operator==(const ComCompressionInfo &o) const { return compressionMethod_ == o.compressionMethod_; }

  CompressionMethod getCompressionMethod() const { return compressionMethod_; }
  void setCompressionMethod(const char *fileName);

  NABoolean isCompressed() const {
    return (compressionMethod_ != UNCOMPRESSED && compressionMethod_ != UNKNOWN_COMPRESSION);
  }

  NABoolean splitsAllowed() const { return !isCompressed(); }

  Int64 getMinScratchBufferSize() const;

  // try to determine the compression method just from a file name
  static CompressionMethod getCompressionMethodFromFileName(const char *f);

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual char *findVTblPtr(short classID);
  virtual unsigned char getClassVersionID();
  virtual void populateImageVersionIDArray();
  virtual short getClassSize();
  virtual Long pack(void *space);
  virtual Lng32 unpack(void *base, void *reallocator);

 private:
  CompressionMethod compressionMethod_;
};

#endif
