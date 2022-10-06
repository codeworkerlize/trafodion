//**********************************************************************

// **********************************************************************

//
// BufferReference.cpp
//

#include "BufferReference.h"
#include "ex_ex.h"

namespace ExOverflow {
BufferReference::BufferReference(ByteCount bufferSize) : buffer_(NULL), maxOffset_(bufferSize), offset_(0) {}

BufferReference::BufferReference(const BufferReference &br) {
  buffer_ = br.buffer_;
  maxOffset_ = br.maxOffset_;
  offset_ = br.offset_;
}

BufferReference::~BufferReference(void) {}

BufferReference &BufferReference::operator=(const BufferReference &rhs) {
  if (this != &rhs) {
    buffer_ = rhs.buffer_;
    maxOffset_ = rhs.maxOffset_;
    offset_ = rhs.offset_;
  }
  return *this;
}

void BufferReference::advance(ByteCount numBytes) {
  ex_assert((bytesAvailable() >= numBytes), "advance past end of buffer");
  offset_ += numBytes;
}

ByteCount BufferReference::bytesAvailable(void) const {
  ByteCount nBytes = 0;
  if (isValid()) {
    ex_assert((maxOffset_ > 0), "maxOffset_ not initialized");
    ex_assert((offset_ <= maxOffset_), "invalid offset");
    nBytes = (maxOffset_ - offset_);
  }

  return nBytes;
}

void BufferReference::invalidate(void) {
  buffer_ = NULL;
  offset_ = 0;
}

bool BufferReference::sameBuffer(const BufferReference &left, const BufferReference &right) {
  return left.isValid() && (left.buffer_ == right.buffer_);
}

bool BufferReference::sameBuffer(const BufferReference &left, const char *right) {
  return left.isValid() && (left.buffer_ == right);
}

void BufferReference::set(char *buffer, BufferOffset offset) {
  ex_assert((buffer != NULL), "set NULL buffer reference");

  buffer_ = buffer;
  offset_ = offset;
}
}  // namespace ExOverflow
