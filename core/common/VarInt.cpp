
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         VarInt.cpp
 * Description:  Array of unsigned integers with bit widths from 1-32
 * Created:      1/11/2013
 * Language:     C++
 *
 *
 *****************************************************************************
 */

#include "VarInt.h"

#include <assert.h>
#include <math.h>
#include <stdio.h>
#include <string.h>

#include "common/Int64.h"
#include "common/str.h"

// ----------------------------------------------------------------------------
// helper methods to pack and unpack
// ----------------------------------------------------------------------------

int pack(char *&buffer, UInt16 x) {
  int sz = sizeof(UInt16);
  str_cpy_all((char *)buffer, (char *)&x, sz);
  buffer += sz;
  return sz;
}

int pack(char *&buffer, int x) {
  int sz = sizeof(int);
  str_cpy_all((char *)buffer, (char *)&x, sz);
  buffer += sz;
  return sz;
}

int pack(char *&buffer, int x) {
  int sz = sizeof(int);
  str_cpy_all((char *)buffer, (char *)&x, sz);
  buffer += sz;
  return sz;
}

int pack(char *&buffer, UInt64 x) {
  int sz = sizeof(UInt64);
  str_cpy_all((char *)buffer, (char *)&x, sz);
  buffer += sz;
  return sz;
}

int pack(char *&buffer, int *ptr, int x) {
  int sz = sizeof(int);
  str_cpy_all((char *)buffer, (char *)&x, sz);
  buffer += sz;

  int sz1 = x * sz;
  str_cpy_all((char *)buffer, (char *)ptr, sz1);
  buffer += sz1;
  return sz + sz1;
}

int unpack(char *&buffer, UInt16 &x) {
  int sz = sizeof(UInt16);
  str_cpy_all((char *)&x, buffer, sz);
  buffer += sz;
  return sz;
}

int unpack(char *&buffer, int &x) {
  int sz = sizeof(int);
  str_cpy_all((char *)&x, buffer, sz);
  buffer += sz;
  return sz;
}

int unpack(char *&buffer, int &x) {
  int sz = sizeof(int);
  str_cpy_all((char *)&x, buffer, sz);
  buffer += sz;
  return sz;
}

int unpack(char *&buffer, UInt64 &x) {
  int sz = sizeof(UInt64);
  str_cpy_all((char *)&x, buffer, sz);
  buffer += sz;
  return sz;
}

int unpack(char *&buffer, int *&ptr, int &x, NAHeap *heap) {
  int y = 0;  // unpacked # of UINT32s
  int sz = unpack(buffer, y);

  if (y > x) {
    if (ptr) NADELETEBASIC(ptr, heap);

    ptr = new (heap) int[y];
    assert(ptr != NULL);
  }

  int sz1 = y * sizeof(int);

  if (y > 0) str_cpy_all((char *)ptr, buffer, sz1);

  x = y;

  buffer += sz1;
  return sz + sz1;
}

////////////////////////////////////////////////////////////////////

UInt64 VarUIntArray::estimateMemoryInBytes(int x, int b) {
  return (x * b + BitsPerWord - 1) / BitsPerWord * sizeof(int) + sizeof(VarUIntArray);
}

UInt32 VarUIntArray::minPackedLength() {
  return sizeof(numEntries_) + sizeof(numBits_) + sizeof(maxVal_) + sizeof(numWords_);
}

UInt32 VarUIntArray::getPackedLength() { return minPackedLength() + sizeof(numWords_) * numWords_; }

VarUIntArray::VarUIntArray(int numEntries, int numBits, NAHeap *heap)
    : counters_(NULL), numEntries_(numEntries), numBits_(numBits), heap_(heap) {
  assert(numBits > 0 && numBits <= BitsPerWord);

  numWords_ = (int)((((UInt64)numEntries) * numBits + BitsPerWord - 1) / BitsPerWord);

  if (numWords_ > 0) {
    counters_ = new (heap_) int[numWords_];
    memset(counters_, 0, numWords_ * sizeof(int));
  }

  maxVal_ = AllOnes >> (BitsPerWord - numBits);
}

VarUIntArray::VarUIntArray(NAHeap *heap)
    : numEntries_(0), numWords_(0), numBits_(0), heap_(heap), counters_(NULL), maxVal_(0) {}

VarUIntArray::VarUIntArray(const VarUIntArray &x, NAHeap *heap)
    : numEntries_(x.numEntries_), numWords_(x.numWords_), numBits_(x.numBits_), maxVal_(x.maxVal_), heap_(heap) {
  counters_ = new (heap_) int[numWords_];
  str_cpy_all((char *)counters_, (char *)x.counters_, numWords_ * sizeof(int));
}

void VarUIntArray::clear() { memset(counters_, 0, numWords_ * sizeof(int)); }

int VarUIntArray::operator[](int ix) const {
  assert(ix >= 0 && ix < numEntries_);

  // compute bit and word offset of our counter
  int lowBit = ix * numBits_;
  int lowWord = lowBit >> 5;  // / BitsPerWord(which is 32);
  int lowBitInWord = lowBit % BitsPerWord;
  int nextCounterStart = lowBitInWord + numBits_;

  // take the word where our counter starts, remove leading bits by
  // left-shifting, then shift our counter to the right part of the result
  int result = (counters_[lowWord] << lowBitInWord) >> (BitsPerWord - numBits_);

  // test if our counter continues into the next word
  if (nextCounterStart > BitsPerWord) {
    int numRemainingBits = nextCounterStart - BitsPerWord;

    // set rightmost numRemaining bits in result to 0
    result &= (AllOnes << numRemainingBits);
    // add remaining bits from next word
    result |= counters_[lowWord + 1] >> (BitsPerWord - numRemainingBits);
  }

  return result;
}

NABoolean VarUIntArray::put(int ix, int val, NABoolean *prevBitsOff) {
  assert(ix >= 0 && ix < numEntries_);

  if (val > maxVal_) val = maxVal_;

  // compute bit and word offset of our counter
  int lowBit = (ix * numBits_);
  int lowWord = lowBit >> 5;  // / BitsPerWord(which is 32);
  int lowBitInWord = lowBit % BitsPerWord;
  int nextCounterStart = lowBitInWord + numBits_;

  // value to put, shifted to the correct position
  int shiftedPut = val;
  // mask indicating our counter in the word
  int putMask = (AllOnes >> lowBitInWord);

  if (nextCounterStart > BitsPerWord) {
    // putMask is good to go for 1st word
    // eliminate trailing bits from val
    shiftedPut >>= nextCounterStart - BitsPerWord;
  } else {
    // eliminate following counters' bits from putMask
    putMask &= AllOnes << (BitsPerWord - nextCounterStart);

    // left-shift value to put
    shiftedPut <<= BitsPerWord - nextCounterStart;
  }

  if (prevBitsOff) *prevBitsOff = (counters_[lowWord] & putMask) ? FALSE : TRUE;

  // zero out old counter value in first (maybe only) word
  // putMask has ones for bits that belong to our counter
  counters_[lowWord] &= ~putMask;

  // drop in new counter value in first (maybe only) word
  counters_[lowWord] |= shiftedPut;

  // does counter extend into a second word?
  if (nextCounterStart > BitsPerWord) {
    putMask = AllOnes >> (nextCounterStart - BitsPerWord);
    // shift value to the left, leaving only the remaining bits that
    // need to go into the second word
    shiftedPut = val << (BitsPerWord - (nextCounterStart - BitsPerWord));

    if (prevBitsOff) (*prevBitsOff) &= (counters_[lowWord + 1] & putMask) ? FALSE : TRUE;

    // zero out old counter value in second word
    // here, putMask has zeroes for bits that belong to our counter
    counters_[lowWord + 1] &= ~putMask;

    // drop in new counter value in first (maybe only) word
    counters_[lowWord + 1] |= shiftedPut;
  }

  return (val == maxVal_);
}

NABoolean VarUIntArray::add(int ix, int val, int &result) {
  int oldVal = (*this)[ix];

  // check for overflow, both for exceeding maxVal_ and for overflow of int
  result = oldVal + val;
  if (result > maxVal_ || result < oldVal || result < val) result = maxVal_;

  return put(ix, result);
}

NABoolean VarUIntArray::sub(int ix, int val, int &minuend) {
  int result = minuend = (*this)[ix];

  // don't change and return TRUE, if counter overflown before
  if (result == maxVal_) return TRUE;

  // don't let the value go below zero, indicate underflow
  if (val > result) {
    put(ix, 0);
    return TRUE;
  }

  // normal case, subtract val from current counter value
  result -= val;
  return put(ix, result);
}

NABoolean VarUIntArray::mergeViaOR(const VarUIntArray &other) {
  if (entries() != other.entries()) return FALSE;

  if (getMaxVal() != other.getMaxVal()) return FALSE;

  if (numBits() != other.numBits()) return FALSE;

  for (UInt32 i = 0; i < numWords_; i++) counters_[i] |= other.counters_[i];

  return TRUE;
}

UInt32 VarUIntArray::bitsSet() {
  UInt32 ct = 0;

  for (UInt32 i = 0; i < numWords_; i++) {
    if (counters_[i]) {
      UInt32 x = counters_[i];
      for (int k = 0; k < 32; k++) {
        if (x & 0x00000001) ct++;

        x >>= 1;
      }
    }
  }
  return ct;
}

void VarUIntArray::dump(ostream &out, const char *msg) {
  if (msg) out << msg << endl;

  // The following implemnents FNV-1 hash described at
  // https://en.wikipedia.org/wiki/Fowler–Noll–Vo_hash_function
  // const UInt32 prime = 16777619;
  // long hash = 0x811c9dc5;  // offset basis
  UInt32 prime = 16777619;
  UInt32 hash = 0x811c9dc5;  // offset basis

  for (UInt32 i = 0; i < numWords_; i++) {
    hash ^= counters_[i];
    hash *= prime;
  }

  out << "VarUIntArray: entries=" << numEntries_;
  out << ", bits/entry=" << numBits_;
  out << ", maxVal=" << maxVal_;
  out << ", num words used=" << numWords_;
  out << ", hash(hex)=" << std::hex << hash << std::dec << endl;

#if 0
   for ( UInt32 i=0; i<numWords_; i++ ) {

     if ( counters_[i] != 0 ) {
        out << "word[" << i << "]=";
        out << std::hex << counters_[i] << std::dec << endl;
     }
   }

   for ( UInt32 i=0; i<numEntries_; i++ ) {
     if ( (*this)[i] == 1 )
        out << "bit[" << i << "]=1" << endl;
    }
#endif
}

int VarUIntArray::packIntoBuffer(char *&buffer) {
  int size = 0;
  size += pack(buffer, numEntries_);
  size += pack(buffer, numBits_);
  size += pack(buffer, maxVal_);
  size += pack(buffer, counters_, numWords_);  // will pack numWords_ up front

  return size;
}

int VarUIntArray::unpackBuffer(char *&buffer) {
  int sz = unpack(buffer, numEntries_);

  sz += unpack(buffer, numBits_);
  sz += unpack(buffer, maxVal_);

  sz += unpack(buffer, counters_, numWords_, heap_);

  return sz;
}

// simple test of the program, may need to comment out some calls to avoid unresolved functions
/*
int main(int argc, char *argv[])
{
  VarUIntArray twoBitArray  (100, 2);
  VarUIntArray threeBitArray(100, 3);
  VarUIntArray fourBitArray (100, 4);
  VarUIntArray fiveBitArray (100, 5);
  VarUIntArray eightBitArray(100, 8);

  for (int i=0; i< 100; i++)
    {
      twoBitArray.put(i,   i % 2);
      threeBitArray.put(i, i % 8);
      fourBitArray.put(i,  i % 16);
      fiveBitArray.put(i,  i % 32);
      eightBitArray.put(i, i % 256);
    }

 for (int i=0; i< 100; i++)
    {
      printf("i: %4d %4d %4d %4d %4d %4d\n",
             i,
             twoBitArray[i],
             threeBitArray[i],
             fourBitArray[i],
             fiveBitArray[i],
             eightBitArray[i]);
    }

 for (int i=0; i<20; i++)
   {
     twoBitArray.put(i,0);
     threeBitArray.put(i,0);

     NABoolean r20 = twoBitArray.add(i, 5);
     NABoolean r30 = threeBitArray.add(i, 5);

     NABoolean r21 = twoBitArray.sub(i, 5);
     NABoolean r31 = threeBitArray.sub(i, 5);

     printf("%d %d %d %d %d %d %d\n", i,
            twoBitArray[i], r20, r21,
            threeBitArray[i], r30, r31);
   }

 return 0;
}
*/
