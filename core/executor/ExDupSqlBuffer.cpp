//**********************************************************************

//
//**********************************************************************
//
// ExDupSqlBuffer.cpp

#include "ExDupSqlBuffer.h"

ExDupSqlBuffer::ExDupSqlBuffer(UInt32 nTuples, UInt32 tupleSize, UInt32 nReserve, NAMemory *heap)
    : ExSimpleSQLBuffer(static_cast<int>(nTuples), static_cast<int>(tupleSize), heap),
      dupCurrent_(NULL),
      dupHead_(NULL),
      dupTail_(NULL),
      maxDups_(0),
      nDups_(0) {
  if (nTuples > nReserve) {
    maxDups_ = nTuples - nReserve;
  }
};

ExDupSqlBuffer::ExDupSqlBuffer(UInt32 nBuffers, UInt32 bufferSize, UInt32 nReserve, UInt32 tupleSize, NAMemory *heap)
    : ExSimpleSQLBuffer(static_cast<int>(nBuffers), static_cast<int>(bufferSize), static_cast<int>(tupleSize),
                        heap),
      dupCurrent_(NULL),
      dupHead_(NULL),
      dupTail_(NULL),
      maxDups_(0),
      nDups_(0) {
  UInt32 nTuples = static_cast<UInt32>(getNumTuples());
  if (nTuples > nReserve) {
    maxDups_ = nTuples - nReserve;
  }
}

ExDupSqlBuffer::~ExDupSqlBuffer(){};

void ExDupSqlBuffer::finishDups(void) {
  if (dupHead_) {
    ExSimpleSQLBufferEntry *usedList = getUsedList();
    if (usedList) {
      dupTail_->setNext(usedList);
    }
    setUsedList(dupHead_);
    dupHead_ = NULL;
    dupTail_ = NULL;
    dupCurrent_ = NULL;
    nDups_ = 0;
  }
}

bool ExDupSqlBuffer::getDupTuple(tupp &tupp) {
  // Preserve tuple order by adding entries to the end of the duplicate list.

  bool haveEntry = false;

  if (nDups_ < maxDups_) {
    ExSimpleSQLBufferEntry *entry = getFreeEntry();
    haveEntry = (entry != NULL);
    if (haveEntry) {
      ++nDups_;
      if (!dupHead_) {
        dupHead_ = entry;
      }
      if (dupTail_) {
        dupTail_->setNext(entry);
      }
      dupTail_ = entry;

      tupp = &entry->tuppDesc_;
    }
  }

  return haveEntry;
};
