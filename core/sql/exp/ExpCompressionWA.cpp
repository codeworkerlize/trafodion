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
* File:         ExpCompressionWA.cpp
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

#include <lzo/lzoconf.h>
#include <lzo/lzo1x.h>
#include "zlib.h"  // moving this include to a different file could cause
// collisions in the definition for ULng32 between zconf.h and Platform.h
#include "ExpCompressionWA.h"

static Int32 read8(char *buf) {
  unsigned char bb = '\0';
  unsigned char *b = (unsigned char *)buf;
  if (b) bb = *b;
  return bb;
}

static UInt32 read16(char *buf) {
  UInt32 v;
  unsigned char *b = (unsigned char *)buf;
  v = (UInt32)b[1] << 0;
  v |= (UInt32)b[0] << 8;
  return v;
}

static UInt32 read32(char *buf) {
  UInt32 v;
  unsigned char *b = (unsigned char *)buf;
  v = (UInt32)b[3] << 0;
  v |= (UInt32)b[2] << 8;
  v |= (UInt32)b[1] << 16;
  v |= (UInt32)b[0] << 24;
  return v;
}

static const unsigned char lzop_magic[9] = {0x89, 0x4c, 0x5a, 0x4f, 0x00, 0x0d, 0x0a, 0x1a, 0x0a};

#define F_ADLER32_D     0x00000001L
#define F_ADLER32_C     0x00000002L
#define F_H_EXTRA_FIELD 0x00000040L
#define F_CRC32_D       0x00000100L
#define F_CRC32_C       0x00000200L
#define F_H_FILTER      0x00000800L
#define LZOP_VERSION    0x1030

ExpCompressionWA::ExpCompressionWA(const ComCompressionInfo *ci, CollHeap *heap)
    : compInfo_(ci), heap_(heap), compScratchBufferUsedSize_(0) {
  compScratchBufferMaxSize_ = ci->getMinScratchBufferSize();
  if (compScratchBufferMaxSize_ > 0)
    compScratchBuffer_ = new (heap) char[compScratchBufferMaxSize_];
  else
    compScratchBuffer_ = NULL;
}

ExpCompressionWA::~ExpCompressionWA() {
  if (compScratchBuffer_) {
    NADELETEBASIC(compScratchBuffer_, heap_);
    compScratchBuffer_ = NULL;
  }
}

ExpCompressionWA *ExpCompressionWA::createCompressionWA(const ComCompressionInfo *ci, CollHeap *heap) {
  switch (ci->getCompressionMethod()) {
    case ComCompressionInfo::LZO_DEFLATE:
      return new (heap) ExpLzoCompressionWA(ci, heap);
    case ComCompressionInfo::LZOP:
      return new (heap) ExpLzopCompressionWA(ci, heap);
    case ComCompressionInfo::DEFLATE:
      return new (heap) ExpDeflateCompressionWA(ci, heap);
    case ComCompressionInfo::GZIP:
      return new (heap) ExpGzipCompressionWA(ci, heap);
    default:
      return NULL;
  }
}

ExpLzoCompressionWA::ExpLzoCompressionWA(const ComCompressionInfo *ci, CollHeap *heap)
    : ExpCompressionWA(ci, heap), flags_(0) {}

ExpCompressionWA::CompressionReturnCode ExpLzoCompressionWA::decompress(char *src, long srcLength, char *target,
                                                                        long targetMaxLen, long &compressedBytesRead,
                                                                        long &uncompressedBytesProduced) {
  lzo_uint newUncompressedLen = 0;
  UInt32 uncompressedLen = 0;
  UInt32 compressedLen = 0;
  UInt32 compressionBlockSize = 256 * 1024;
  UInt32 bytesToCopy = 0;  // local convenience variable
  compressedBytesRead = 0;
  uncompressedBytesProduced = 0;
  NABoolean hasCheckSum = ((flags_ & F_ADLER32_D) || (flags_ & F_CRC32_D));
  UInt32 checkSum = 0;
  UInt32 blockHeaderLen = 8;
  if (hasCheckSum) blockHeaderLen += 4;

  if (targetMaxLen < compressionBlockSize) return COMPRESS_FAILURE;

  while ((compressedBytesRead < srcLength) && (uncompressedBytesProduced < targetMaxLen)) {
    if (getScratchBufUsedSize() > 0) {
      // remnant from previous src present
      // copy one compressed block from current src and process it
      if (getScratchBufUsedSize() < blockHeaderLen) {
        memcpy(getScratchBufHead(),
               src,  // compressedBytesRead == 0
               blockHeaderLen - getScratchBufUsedSize());
        compressedBytesRead += blockHeaderLen - getScratchBufUsedSize();
        setScratchBufUsedSize(blockHeaderLen);
      }
      uncompressedLen = read32(getScratchBuf());
      compressedLen = read32(getScratchBuf() + 4);
      if (hasCheckSum) checkSum = read32(getScratchBuf() + 8);
      if (uncompressedLen > compressionBlockSize || compressedLen > compressionBlockSize || compressedLen == 0)
        return COMPRESS_FAILURE;

      if (compressedLen > (getScratchBufUsedSize() - blockHeaderLen) + srcLength - compressedBytesRead) {
        // less than compressedLen bytes in src + scratch,
        // save remnant in scratch
        // append data from next src, then uncompress
        memcpy(getScratchBufHead(), src + compressedBytesRead, srcLength - compressedBytesRead);
        addToScratchBufUsedSize(srcLength - compressedBytesRead);
        compressedBytesRead = srcLength;  // have read src completely
      } else                              // we have next compression block in scratch+src
      {
        if (uncompressedLen < (targetMaxLen - uncompressedBytesProduced)) {
          bytesToCopy = compressedLen - (getScratchBufUsedSize() - blockHeaderLen);
          memcpy(getScratchBufHead(), src + compressedBytesRead, bytesToCopy);
          setScratchBufUsedSize(compressedLen + blockHeaderLen);
          newUncompressedLen = uncompressedLen;
          int r = lzo1x_decompress_safe((unsigned char *)getScratchBuf() + blockHeaderLen, compressedLen,
                                        (unsigned char *)target, &newUncompressedLen, NULL);
          if (r != LZO_E_OK || newUncompressedLen != uncompressedLen) return COMPRESS_FAILURE;
          target += uncompressedLen;
          uncompressedBytesProduced += uncompressedLen;
          compressedBytesRead += bytesToCopy;
          setScratchBufUsedSize(0);
        } else {
          // not enough room in target to place uncompressed data
          // but we have compressed data in scratch. This should not happen
          // since in "else if/else" branches we avoid writing to scratch if
          // there is less than blocksize space available in target
          return COMPRESS_FAILURE;
        }
      }
    } else if (srcLength - compressedBytesRead >= blockHeaderLen) {
      uncompressedLen = read32(src + compressedBytesRead);
      compressedLen = read32(src + compressedBytesRead + 4);
      if (hasCheckSum) checkSum = read32(src + compressedBytesRead + 8);
      if (uncompressedLen > compressionBlockSize || compressedLen > compressionBlockSize || compressedLen == 0)
        return COMPRESS_FAILURE;

      if (compressedLen > srcLength - compressedBytesRead - blockHeaderLen) {
        // less than compressedLen bytes in current src,
        // save remnant in scratch and return if target has room for next block
        // else just return without writing to scratch
        if ((targetMaxLen - uncompressedBytesProduced) >= compressionBlockSize) {
          memcpy(getScratchBuf(), src + compressedBytesRead, srcLength - compressedBytesRead);
          setScratchBufUsedSize(srcLength - compressedBytesRead);
          compressedBytesRead = srcLength;
        } else
          return COMPRESS_CONTINUE;
      } else {
        if (uncompressedLen < (targetMaxLen - uncompressedBytesProduced)) {
          newUncompressedLen = uncompressedLen;
          int r = lzo1x_decompress_safe((unsigned char *)src + compressedBytesRead + blockHeaderLen, compressedLen,
                                        (unsigned char *)target, &newUncompressedLen, NULL);
          if (r != LZO_E_OK || newUncompressedLen != uncompressedLen) return COMPRESS_FAILURE;

          target += uncompressedLen;
          uncompressedBytesProduced += uncompressedLen;
          compressedBytesRead += compressedLen + blockHeaderLen;
        } else {
          // not enough room in target to place uncompressed data
          // keep compressed data in src and return from this method
          // to get next target
          return COMPRESS_CONTINUE;
        }
      }
    } else {
      // less than blockHeaderLen bytes in current src, save remnant in scratch
      // but only if target has space > blockLen
      if ((targetMaxLen - uncompressedBytesProduced) >= compressionBlockSize) {
        memcpy(getScratchBuf(), src + compressedBytesRead, srcLength - compressedBytesRead);
        setScratchBufUsedSize(srcLength - compressedBytesRead);
        compressedBytesRead = srcLength;
      } else
        return COMPRESS_CONTINUE;
    }
  }
  return COMPRESS_SUCCESS;
}

ExpCompressionWA::CompressionReturnCode ExpLzoCompressionWA::initCompressionLib() {
  if (lzo_init() != LZO_E_OK) return COMPRESS_NOT_INITIALIZED;
  return COMPRESS_SUCCESS;
}

ExpLzopCompressionWA::ExpLzopCompressionWA(const ComCompressionInfo *ci, CollHeap *heap)
    : ExpLzoCompressionWA(ci, heap), headerSeen_(FALSE) {}

ExpCompressionWA::CompressionReturnCode ExpLzopCompressionWA::decompress(char *src, long srcLength, char *target,
                                                                         long targetMaxLen, long &compressedBytesRead,
                                                                         long &uncompressedBytesProduced) {
  UInt32 headerLen = 0;
  ExpCompressionWA::CompressionReturnCode rc;
  if (!headerSeen_) {
    rc = processHeader(src, srcLength, headerLen);
    if (rc != COMPRESS_SUCCESS) return rc;
    src = src + headerLen;
    srcLength -= headerLen;
  }
  rc = ExpLzoCompressionWA::decompress(src, srcLength, target, targetMaxLen, compressedBytesRead,
                                       uncompressedBytesProduced);
  compressedBytesRead += headerLen;
  return rc;
}

ExpCompressionWA::CompressionReturnCode ExpLzopCompressionWA::processHeader(char *src, long srcLength,
                                                                            UInt32 &headerLen) {
  UInt32 offset = 0;
  if ((srcLength > sizeof(lzop_magic)) && memcmp(src, lzop_magic, sizeof(lzop_magic)) != 0)
    return COMPRESS_HEADER_ERROR;
  offset += sizeof(lzop_magic);

  UInt32 version, libVersion, flags, temp32;
  version = read16(src + offset);
  if (version < 0x0900) return COMPRESS_HEADER_ERROR;
  offset += 2;

  libVersion = read16(src + offset);
  offset += 2;
  if (version >= 0x0940) {
    libVersion = read16(src + offset);
    if ((libVersion > LZOP_VERSION) || (libVersion < 0x0900)) return COMPRESS_HEADER_ERROR;
    offset += 2;
  }

  Int32 temp8, len;
  temp8 = read8(src + offset);
  offset++;
  if (version >= 0x0940) {
    temp8 = read8(src + offset);
    offset++;
  }
  flags = read32(src + offset);
  offset += 4;
  if (flags & F_H_FILTER) {
    temp32 = read32(src + offset);
    offset += 4;
  }
  temp32 = read32(src + offset);
  offset += 4;
  temp32 = read32(src + offset);
  offset += 4;
  if (version >= 0x0940) {
    temp32 = read32(src + offset);
    offset += 4;
  }

  len = read8(src + offset);  // length of filename
  offset++;
  if (len > 0) {
    if (len >= srcLength) return COMPRESS_HEADER_ERROR;
    offset += len;  // skip filename
  }

  temp32 = read32(src + offset);
  offset += 4;

  /* skip extra field [not used yet] */
  if (flags & F_H_EXTRA_FIELD) {
    temp32 = read32(src + offset);
    offset += 4;
    for (int k = 0; k < temp32; k++) {
      temp8 = read8(src + offset);
      offset++;
    }
    temp32 = read32(src + offset);
    offset += 4;
  }
  if ((flags & F_CRC32_C) || (flags & F_ADLER32_C))
    return COMPRESS_HEADER_ERROR;  // checksum for compressed block not supp.

  headerLen = offset;
  headerSeen_ = TRUE;
  flags_ = flags;
  return COMPRESS_SUCCESS;
}

ExpDeflateCompressionWA::ExpDeflateCompressionWA(const ComCompressionInfo *ci, CollHeap *heap)
    : ExpCompressionWA(ci, heap) {
  // scratch space for Deflate is maintained internally by the library
  // in a z_stream object. We create a z_stream object on heap_ and point to
  // it from compSctachBuffer_. However te zlib library will allocate more
  // memory and place a pointer to it in one of the data members of z_stream.
  // Currently this will be on global heap and not NAHeap. This memory will
  // be returned by zlib as long as we call inflateEnd in the exit path.
  // We could look into passing alloc/dealloc routines to zlib so that
  // internal memory used by zlib also comes from NAHeap. Thsi is not done yet.
  // Comments in zlib manual indicate that about 256KB of memory could be
  // allocated internally by zlib and pointed to by members of z_stream.
  // scratchBufferUsedSize_, scaratchBufferMaxSize_ do not include this memory.
  // Descrtuctor will call InflateEnd to handle cancel.x
  compScratchBuffer_ = (char *)new (heap_) z_stream;
  compScratchBufferUsedSize_ = sizeof(z_stream);
  compScratchBufferMaxSize_ = sizeof(z_stream);
}

ExpDeflateCompressionWA::~ExpDeflateCompressionWA() {
  if (compScratchBuffer_) {
    z_stream *strm = (z_stream *)compScratchBuffer_;
    (void)inflateEnd(strm);
  }
}

ExpCompressionWA::CompressionReturnCode ExpDeflateCompressionWA::decompress(char *src, long srcLength, char *target,
                                                                            long targetMaxLen,
                                                                            long &compressedBytesRead,
                                                                            long &uncompressedBytesProduced) {
  int ret;
  UInt32 compressionBlockSize = 256 * 1024;
  z_stream *strm = (z_stream *)compScratchBuffer_;
  unsigned char *in = (unsigned char *)src;
  unsigned char *out = (unsigned char *)target;
  compressedBytesRead = 0;
  uncompressedBytesProduced = 0;

  if (strm->avail_in == 0) {
    strm->avail_in = (UInt32)srcLength;
    if (strm->avail_in == 0) return COMPRESS_SUCCESS;
    strm->next_in = in;
  }
  strm->avail_out = (UInt32)targetMaxLen;
  if (strm->avail_out == 0) return COMPRESS_FAILURE;
  strm->next_out = out;

  ret = inflate(strm, Z_NO_FLUSH);
  switch (ret) {
    case Z_NEED_DICT:
    case Z_DATA_ERROR:
    case Z_MEM_ERROR:
    case Z_STREAM_ERROR:
      (void)inflateEnd(strm);
      return COMPRESS_FAILURE;
  }
  uncompressedBytesProduced = targetMaxLen - strm->avail_out;
  compressedBytesRead = srcLength - strm->avail_in;

  if (ret == Z_STREAM_END) {
    // reinitialize internal state for potential next file.
    // If there are no more files destructor will clean up internal state.
    //(void)inflateEnd(strm);
    // return initCompressionLib();
    ret = inflateReset(strm);
    return (ret == Z_OK) ? COMPRESS_SUCCESS : COMPRESS_FAILURE;

  } else if ((strm->avail_out == 0) || (strm->avail_in == 0)) {
    strm->avail_in = 0;
    strm->avail_out = 0;
    return COMPRESS_SUCCESS;
  } else
    return COMPRESS_FAILURE;
}

ExpCompressionWA::CompressionReturnCode ExpDeflateCompressionWA::initCompressionLib() {
  z_stream *strm = (z_stream *)compScratchBuffer_;
  int ret;

  /* allocate inflate state */
  strm->zalloc = Z_NULL;
  strm->zfree = Z_NULL;
  strm->opaque = Z_NULL;
  strm->avail_in = 0;
  strm->next_in = Z_NULL;
  ret = inflateInit(strm);
  if (ret != Z_OK) return COMPRESS_NOT_INITIALIZED;
  return COMPRESS_SUCCESS;
}

ExpGzipCompressionWA::ExpGzipCompressionWA(const ComCompressionInfo *ci, CollHeap *heap)
    : ExpDeflateCompressionWA(ci, heap) {}

ExpCompressionWA::CompressionReturnCode ExpGzipCompressionWA::initCompressionLib() {
  z_stream *strm = (z_stream *)compScratchBuffer_;
  int ret;

  /* allocate inflate state */
  strm->zalloc = Z_NULL;
  strm->zfree = Z_NULL;
  strm->opaque = Z_NULL;
  strm->avail_in = 0;
  strm->next_in = Z_NULL;
  /*  max_wbits = 15, see zconf.h. For an explanation of the number 16 please
      see comment in /usr/include/zlib.h for inflateInit2. */
  ret = inflateInit2(strm, MAX_WBITS + 16);
  if (ret != Z_OK) return COMPRESS_NOT_INITIALIZED;
  return COMPRESS_SUCCESS;
}
