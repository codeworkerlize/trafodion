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
