
/* -*-C++-*-
****************************************************************************
*
* File:         ExpCompressionWA.h
* Description:  Classes for compression WA. Includes scratch space management
*               for compression libraries as well as derived classes for each
*               compression type with appropriate methods to initialize and
*               decompress.
*
* Created:      4/21/2016
* Language:     C++
*
*
*
****************************************************************************
*/

#ifndef EXP_COMPRESSIONWA_H
#define EXP_COMPRESSIONWA_H

#include "comexe/ComCompressionInfo.h"
#include "common/NAHeap.h"

class ExpCompressionWA : public NABasicObject {
 public:
  enum CompressionReturnCode {
    COMPRESS_SUCCESS,
    COMPRESS_FAILURE,
    COMPRESS_CONTINUE,
    COMPRESS_NOT_INITIALIZED,
    COMPRESS_HEADER_ERROR
  };

  ExpCompressionWA(const ComCompressionInfo *ci, CollHeap *heap);
  virtual ~ExpCompressionWA();

  static ExpCompressionWA *createCompressionWA(const ComCompressionInfo *ci, CollHeap *heap);

  virtual CompressionReturnCode decompress(char *src, long srcLength, char *target, long targetMaxLen,
                                           long &compressedBytesRead, long &uncompressedBytesProduced) = 0;

  virtual CompressionReturnCode initCompressionLib() = 0;

  virtual const char *getText() = 0;

  virtual void resetHeaderSeen(){};

  char *getScratchBuf() { return compScratchBuffer_; }
  char *getScratchBufHead() { return compScratchBuffer_ + compScratchBufferUsedSize_; }
  UInt32 getScratchBufMaxSize() const { return compScratchBufferMaxSize_; }
  UInt32 getScratchBufUsedSize() const { return compScratchBufferUsedSize_; }
  void setScratchBufUsedSize(UInt32 val) { compScratchBufferUsedSize_ = val; }
  void addToScratchBufUsedSize(UInt32 val) { compScratchBufferUsedSize_ += val; }

 protected:
  const ComCompressionInfo *compInfo_;
  char *compScratchBuffer_;
  UInt32 compScratchBufferMaxSize_;
  UInt32 compScratchBufferUsedSize_;
  CollHeap *heap_;
};

class ExpLzoCompressionWA : public ExpCompressionWA {
 public:
  ExpLzoCompressionWA(const ComCompressionInfo *ci, CollHeap *heap);

  virtual CompressionReturnCode decompress(char *src, long srcLength, char *target, long targetMaxLen,
                                           long &compressedBytesRead, long &uncompressedBytesProduced);

  virtual CompressionReturnCode initCompressionLib();

  virtual const char *getText() { return (const char *)"LZO"; }

 protected:
  UInt32 flags_;
};

class ExpLzopCompressionWA : public ExpLzoCompressionWA {
 public:
  ExpLzopCompressionWA(const ComCompressionInfo *ci, CollHeap *heap);

  virtual CompressionReturnCode decompress(char *src, long srcLength, char *target, long targetMaxLen,
                                           long &compressedBytesRead, long &uncompressedBytesProduced);

  CompressionReturnCode processHeader(char *src, long srcLength, UInt32 &bytesRead);

  virtual void resetHeaderSeen() { headerSeen_ = FALSE; }

  virtual const char *getText() { return (const char *)"LZOP"; }

 private:
  NABoolean headerSeen_;
};

class ExpDeflateCompressionWA : public ExpCompressionWA {
 public:
  ExpDeflateCompressionWA(const ComCompressionInfo *ci, CollHeap *heap);
  virtual ~ExpDeflateCompressionWA();

  virtual CompressionReturnCode decompress(char *src, long srcLength, char *target, long targetMaxLen,
                                           long &compressedBytesRead, long &uncompressedBytesProduced);

  virtual CompressionReturnCode initCompressionLib();

  virtual const char *getText() { return (const char *)"DEFLATE"; }
};

class ExpGzipCompressionWA : public ExpDeflateCompressionWA {
 public:
  ExpGzipCompressionWA(const ComCompressionInfo *ci, CollHeap *heap);
  // use destructor of ExpDeflateCompressionWA
  // use decompress method from ExpDeflateCompressionWA

  virtual CompressionReturnCode initCompressionLib();

  virtual const char *getText() { return (const char *)"GZIP"; }
};

#endif /* EXP_COMPRESSIONWA_H */
