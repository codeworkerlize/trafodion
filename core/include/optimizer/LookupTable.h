#ifndef LOOKUPTABLE_H
#define LOOKUPTABLE_H
/* -*-C++-*-
*************************************************************************
*
* File:         LookupTable.h
* Description:  Lookup table template class
*               i.e. a class that implements a 2-dimensional array
* Created:      09/07/00
* Language:     C++
*
*

*
*
*************************************************************************
*/

#include "common/CmpCommon.h"
#include "export/NABasicObject.h"

// ----------------------------------------------------------------------
// contents of this file
// ----------------------------------------------------------------------
template <class T>
class LookupTable;

template <class T>
class LookupTable : public NABasicObject {
 public:
  LookupTable();
  LookupTable(int numRows, int numCols,
              CollHeap *heap = 0);                            // constructor
  LookupTable(const LookupTable &other, CollHeap *heap = 0);  // copy constructor

  virtual ~LookupTable();  // destructor

  LookupTable &operator=(const LookupTable &other);

  const T &getValue(int rowNum, int colNum) const;
  T &operator[](int elem);
  void setValue(int rowNum, int colNum, const T &value);

 private:
  int numRows_;
  int numCols_;
  CollHeap *heap_;
  T *arr_;

};  // LookupTable

template <class T>
LookupTable<T>::LookupTable() : numRows_(0), numCols_(0), heap_(NULL), arr_(NULL) {}

// constructor
template <class T>
LookupTable<T>::LookupTable(int numRows, int numCols, CollHeap *heap)
    : numRows_(numRows), numCols_(numCols), heap_(heap) {
  DCMPASSERT(numRows > 0);
  DCMPASSERT(numCols > 0);
  arr_ = new (heap_) T[numRows * numCols];
}

// accessor
template <class T>
const T &LookupTable<T>::getValue(int rowNum, int colNum) const {
  DCMPASSERT((rowNum >= 0) AND(rowNum < numRows_) AND(colNum >= 0) AND(colNum < numCols_));

  int index = (rowNum * numCols_) + colNum;

  DCMPASSERT(arr_ != NULL);

  return arr_[index];
}

// mutator
template <class T>
void LookupTable<T>::setValue(int rowNum, int colNum, const T &value) {
  CMPASSERT((rowNum >= 0) AND(rowNum < numRows_) AND(colNum >= 0) AND(colNum < numCols_));

  int index = (rowNum * numCols_) + colNum;

  DCMPASSERT(arr_ != NULL);

  arr_[index] = value;
}

// copy constructor
template <class T>
LookupTable<T>::LookupTable(const LookupTable &other, CollHeap *heap)
    : numRows_(other.numRows_), numCols_(other.numCols_), heap_((heap == NULL) ? other.heap_ : heap) {
  arr_ = new (heap_) T[numRows_ * numCols_];
  int x, y;
  int index;
  for (x = 0; x < numRows_; x++) {
    for (y = 0; y < numCols_; y++) {
      index = (x * numCols_) + y;
      arr_[index] = other.arr_[index];
    }
  }
}

// equal operator
template <class T>
LookupTable<T> &LookupTable<T>::operator=(const LookupTable &other) {
  // Test for assignment to itself
  if (this == &other) return *this;

  if (arr_ != NULL) delete[] arr_;  // free up existing memory

  numRows_ = other.numRows_;
  numCols_ = other.numCols_;

  arr_ = new (heap_) T[numRows_ * numCols_];
  int x, y;
  int index;
  for (x = 0; x < numRows_; x++) {
    for (y = 0; y < numCols_; y++) {
      index = (x * numCols_) + y;
      arr_[index] = other.arr_[index];
    }
  }

  return *this;
}

// destructor
template <class T>
LookupTable<T>::~LookupTable() {
  if (arr_ != NULL) {
    delete[] arr_;
    arr_ = NULL;
  }
}

#endif /* LOOKUPTABLE_H */
