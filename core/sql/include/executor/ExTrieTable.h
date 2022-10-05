/* -*-C++-*-
******************************************************************************
*
* File:         ExTrieTable.h
* RCS:          $Id
* Description:  ExTrieTable class declaration
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
#ifndef __ExTrieTable_h__
#define __ExTrieTable_h__

// Includes
//
#include "executor/ex_god.h"

// External forward declarations
//
class NAMemory;

typedef char **ExTrie;

class ExTrieTable : public ExGod {
 public:
  ExTrieTable(int keySize, int dataSize, int memSize, NAMemory *heap);
  ~ExTrieTable();

  int getMaximumNumberTuples() const { return maximumNumberTuples_; };
  int getMinimumNumberTuples() const { return minimumNumberTuples_; };
  char *getData() const { return data_; };

  char *getReturnRow() const {
    if (returnRow_ < numberTuples_) return rootTuple_ - returnRow_ * dataSize_;
    return 0;
  };
  void advanceReturnRow() { returnRow_++; };
  void resetReturnRow() { returnRow_ = 0; };

  int findOrAdd(char *key);

 private:
  int keySize_;
  int dataSize_;
  int memSize_;
  NAMemory *heap_;

  char *memory_;
  int maximumNumberTuples_;
  int minimumNumberTuples_;

  ExTrie rootTrie_;
  ExTrie nextTrie_;
  char *data_;

  char *rootTuple_;
  int numberTuples_;
  int returnRow_;
};

#endif
