
#ifndef INCLUDE_VARINT_H
#define INCLUDE_VARINT_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         VarInt.h
 * Description:  Array of unsigned integers with bit widths from 1-32
 * Created:      1/11/2013
 * Language:     C++
 *
 *
 *****************************************************************************
 */

#include "common/Platform.h"
#include "common/NABoolean.h"
#include "common/NAMemory.h"
#include <iostream>

int pack(char *&buffer, UInt16 x);
int pack(char *&buffer, int x);
int pack(char *&buffer, int x);
int pack(char *&buffer, UInt64 x);
int pack(char *&buffer, int *ptr, int x);

int unpack(char *&buffer, UInt16 &x);
int unpack(char *&buffer, int &x);
int unpack(char *&buffer, int &x);
int unpack(char *&buffer, UInt64 &x);
int unpack(char *&buffer, int *&ptr, int &x, NAHeap *heap);

//----------------------------------------------------------------------------------
// Array of unsigned integers with a bit width between 1 and 32.
//
// - Constructor specifies number of entries, bit width, and heap.
// - get method and indexing operator [] return the i-th element
// - put method sets i-th element, values > max are mapped to max
//   (returns TRUE if overflow happened)
// - add method adds a value to i-th element, returns TRUE if overflow happened
// - sub method subtracts a value from i-th element,
//    + returns TRUE if value is at max and does nothing
//    + returns TRUE if value underflows (and sets i-th item to 0)
//----------------------------------------------------------------------------------

class VarUIntArray {
  static const int BitsPerWord = 32;
  static const int AllOnes = 0xFFFFFFFF;

 public:
  // create an array with a given length of number of bits per entry (up to 32 allowed)
  // and initialize it to zeroes
  VarUIntArray(int numEntries, int numBits, NAHeap *heap);

  // copy constructor
  VarUIntArray(const VarUIntArray &, NAHeap *heap);

  // create an array with no space allocated
  VarUIntArray(NAHeap *heap);

  ~VarUIntArray() { NADELETEBASIC(counters_, heap_); }

  void clear();

  // get array entry at index ix
  int get(int ix) const { return (*this)[ix]; }
  int operator[](int ix) const;

  // overwrite array entry at index ix with a new value,
  // return TRUE if an overflow occurred
  NABoolean put(int ix, int val, NABoolean *prevBitOff = NULL);

  // add to existing value, return TRUE if overflow occurred
  NABoolean add(int ix, int val, int &result);

  // subtract from existing value, return TRUE if value
  // did overflow in the past.
  NABoolean sub(int ix, int val, int &minuend);

  // merge two arrays together. Each resultant bit is
  // the OR of the two corresponding bits.
  // Return FALSE if the attributes of the two arrays
  // are not the same.
  NABoolean mergeViaOR(const VarUIntArray &);

  // maximum value  (this value is used to indicate overflow)
  int getMaxVal() const { return maxVal_; }

  UInt32 entries() const { return numEntries_; };
  UInt32 numBits() const { return numBits_; };

  // return # of bits set.
  UInt32 bitsSet();

  // estimate amount of memory needed to store x entries with b bits per entry
  // The estimation is expressed in bytes.
  static UInt64 estimateMemoryInBytes(int x, int b);

  static UInt32 minPackedLength();
  UInt32 getPackedLength();

  // Modeled based on the following method
  // IpcMessageObjSize ComCondition::packObjIntoMessage(char* buffer,
  //                         NABoolean swapBytes)
  int packIntoBuffer(char *&buffer);
  int unpackBuffer(char *&buffer);

  void dump(ostream &, const char *msg = NULL);

 private:
  int numEntries_;
  int numWords_;
  int numBits_;
  int maxVal_;
  int *counters_;

  NAHeap *heap_;
};

typedef VarUIntArray *VarUIntArrayPtr;

#endif
