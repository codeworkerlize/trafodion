/* -*-C++-*-
******************************************************************************
*
* File:         ExTrieTable.cpp
* RCS:          $Id
* Description:  ExTrieTable class Implementation
* Created:      7/1/97
* Modified:     $Author
* Language:     C++
* Status:       $State
*
*

*
*
******************************************************************************
*/

// Includes
//
#include "ExTrieTable.h"

ExTrieTable::ExTrieTable(int keySize, int dataSize, int memSize, CollHeap *heap)
    : keySize_(keySize),
      dataSize_(dataSize),
      memSize_(memSize),
      heap_(heap),
      memory_(NULL),
      maximumNumberTuples_(0),
      minimumNumberTuples_(0),
      rootTrie_(0),
      nextTrie_(0),
      rootTuple_(0),
      numberTuples_(0),
      returnRow_(0) {
  // Attempt to allocate the requested memory. If that fails, try to at
  // least allocate enough memory for a few tuples. Otherwise, simply
  // return and the caller will check getMaximumNumberTuples and realize
  // that the table cannot store any tuples.
  //
  const int minimumMemorySize = sizeof(short *) * 256 * keySize_ + dataSize_;
  memSize_ *= 2;
  while (!memory_ && (memSize_ > minimumMemorySize)) {
    memSize_ /= 2;
    memSize_ &= ~0x07;
    memory_ = new (heap_) char[memSize_];
  }
  if (!memory_) return;

  // Compute the maximum and minimum number of tuples that can be stored
  // in the table. The maximum number of tuples occurs if all Trie's are
  // shared except the last one. The minimum number of tuples occurs if
  // no Trie's are shared.
  //
  maximumNumberTuples_ = (memSize_ - sizeof(ExTrie) * 255 * keySize_) / (sizeof(ExTrie) * 256 + dataSize_);
  minimumNumberTuples_ = (memSize_ - sizeof(ExTrie) * 255 * keySize_) / (sizeof(ExTrie) * 256 * keySize_ + dataSize_);

  // The Trie's grow from the bottom of allocated memory up. Thus, the root
  // Trie is at memory_.
  //
  rootTrie_ = (ExTrie)memory_;
  nextTrie_ = rootTrie_ + 256;

  // Initialize the root Trie.
  //
  for (int i = 0; i < 256; i++) rootTrie_[i] = 0;

  // The data tuples grow from the top of allocated memory down. Thus,
  // the first data tuple is at memory_ + memSize_ - dataSize_ - 1.
  //
  rootTuple_ = memory_ + memSize_ - dataSize_ - 1;
}

ExTrieTable::~ExTrieTable() {
  if (memory_) NADELETEBASIC(memory_, heap_);
  memory_ = NULL;
}

int ExTrieTable::findOrAdd(char *key) {
  ExTrie trie = rootTrie_;
  ExTrie lastTrie = 0;
  int i = 0;
  for (; i < keySize_ && trie; i++) {
    lastTrie = trie;
    trie = (char **)trie[key[i]];
  }

  // If i is keySize_ and trie is not null, then the group has been found
  // and is indicated by trie.
  //
  if (trie && (i == keySize_)) {
    data_ = (char *)trie;
    return 1;
  }

  // Addition Trie's and a data tuple must be added. Check to make sure
  // there is enough memory to complete the operation. If not, return 0
  // to indicate failure.
  //
  if ((char *)(nextTrie_ + (keySize_ - i) * 256) > (rootTuple_ - (numberTuples_ + 1) * dataSize_)) return 0;

  // If i-1 is less than keySize_-1, then additional Trie's need to be
  // added.
  //
  int j = i - 1;
  for (; j < keySize_ - 1; j++) {
    lastTrie[key[j]] = (char *)nextTrie_;
    lastTrie = nextTrie_;
    nextTrie_ += 256;
  }

  // At this point all of the necessary Trie's exist but no data tuple has
  // been allocated. Allocate the data tuple, set the pointer in the Trie,
  // and return.
  //
  lastTrie[key[j]] = data_ = rootTuple_ - (numberTuples_++) * dataSize_;
  return -1;
}
