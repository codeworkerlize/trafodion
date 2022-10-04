

#include "comexe/ComCompressionInfo.h"

ComCompressionInfo::~ComCompressionInfo() {}

void ComCompressionInfo::setCompressionMethod(const char *fileName) {
  compressionMethod_ = getCompressionMethodFromFileName(fileName);
}

long ComCompressionInfo::getMinScratchBufferSize() const {
  switch (compressionMethod_) {
    case LZO_DEFLATE:
    case LZOP:
      return 256 * 1024;
    case DEFLATE:
    case GZIP:
      return 0;  // scratch is handled by zlib library
    default:
      return 0;
  }
}

ComCompressionInfo::CompressionMethod ComCompressionInfo::getCompressionMethodFromFileName(const char *f) {
  const char *ret = strcasestr(f, ".lzo_deflate");
  if (ret) return LZO_DEFLATE;
  ret = strcasestr(f, ".lzo");
  if (ret) return LZOP;
  ret = strcasestr(f, ".deflate");
  if (ret) return DEFLATE;
  ret = strcasestr(f, ".gz");
  if (ret) return GZIP;

  return UNCOMPRESSED;
}

// virtual methods overridden from NAVersionedObject base class

char *ComCompressionInfo::findVTblPtr(short classID) {
  char *vtblPtr;
  GetVTblPtr(vtblPtr, ComCompressionInfo);

  return vtblPtr;
}

unsigned char ComCompressionInfo::getClassVersionID() { return 1; }

void ComCompressionInfo::populateImageVersionIDArray() { setImageVersionID(0, getClassVersionID()); }

short ComCompressionInfo::getClassSize() { return (short)sizeof(ComCompressionInfo); }

Long ComCompressionInfo::pack(void *space) { return NAVersionedObject::pack(space); }

int ComCompressionInfo::unpack(void *base, void *reallocator) { return NAVersionedObject::unpack(base, reallocator); }
