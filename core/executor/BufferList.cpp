// **********************************************************************

// **********************************************************************
//
// BufferList.cpp
//
#include "BufferList.h"

namespace ExOverflow {

BufferList::BufferList(NAMemory *heap) : bufferList_(heap) {}

BufferList::~BufferList(void) {}

char *BufferList::back(void) { return static_cast<char *>(bufferList_.getTail()); }

char *BufferList::current(void) { return static_cast<char *>(bufferList_.getCurr()); }

bool BufferList::empty(void) { return (bufferList_.isEmpty() != 0); }

char *BufferList::front(void) { return static_cast<char *>(bufferList_.getHead()); }

char *BufferList::next(void) {
  if (bufferList_.atEnd())
    return NULL;
  else {
    bufferList_.advance();
    return this->current();
  }
}

char *BufferList::pop(void) {
  char *bufptr = NULL;

  if (!empty()) {
    bufptr = front();
    bufferList_.remove();
  }

  return bufptr;
}

void BufferList::position(void) { bufferList_.position(); }

void BufferList::push_back(char *buffer) {
  if (buffer != NULL) {
    bufferList_.insert(buffer);
  }
}

UInt32 BufferList::size(void) { return static_cast<UInt32>(bufferList_.numEntries()); }

}  // namespace ExOverflow
