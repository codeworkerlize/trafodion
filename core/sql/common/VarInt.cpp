/**********************************************************************
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
**********************************************************************/
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

#include <string.h>
#include <assert.h>
#include <stdio.h>
#include <math.h>
#include "VarInt.h"
#include "str.h"
#include "Int64.h"

// ----------------------------------------------------------------------------
// helper methods to pack and unpack
// ----------------------------------------------------------------------------

ULng32 pack(char*& buffer, UInt16 x)
{
    ULng32 sz = sizeof(UInt16);
    str_cpy_all((char*)buffer, (char*)&x, sz);
    buffer += sz;
    return sz;
}

ULng32 pack(char*& buffer, ULng32 x)
{
    ULng32 sz = sizeof(ULng32);
    str_cpy_all((char*)buffer, (char*)&x, sz);
    buffer += sz;
    return sz;
}

ULng32 pack(char*& buffer, Int32 x)
{
    ULng32 sz = sizeof(Int32);
    str_cpy_all((char*)buffer, (char*)&x, sz);
    buffer += sz;
    return sz;
}

ULng32 pack(char*& buffer, UInt64 x)
{
    ULng32 sz = sizeof(UInt64);
    str_cpy_all((char*)buffer, (char*)&x, sz);
    buffer += sz;
    return sz;
}

ULng32 pack(char*& buffer, ULng32* ptr, ULng32 x)
{
    ULng32 sz = sizeof(ULng32);
    str_cpy_all((char*)buffer, (char*)&x, sz);
    buffer += sz;

    ULng32 sz1 = x*sz;
    str_cpy_all((char*)buffer, (char*)ptr, sz1);
    buffer += sz1;
    return sz+sz1;
}


ULng32 unpack(char*& buffer, UInt16& x)
{
    ULng32 sz = sizeof(UInt16);
    str_cpy_all((char*)&x, buffer, sz);
    buffer += sz;
    return sz;
}

ULng32 unpack(char*& buffer, ULng32& x)
{
    ULng32 sz = sizeof(ULng32);
    str_cpy_all((char*)&x, buffer, sz);
    buffer += sz;
    return sz;
}

ULng32 unpack(char*& buffer, Int32& x)
{
    ULng32 sz = sizeof(Int32);
    str_cpy_all((char*)&x, buffer, sz);
    buffer += sz;
    return sz;
}

ULng32 unpack(char*& buffer, UInt64& x)
{
    ULng32 sz = sizeof(UInt64);
    str_cpy_all((char*)&x, buffer, sz);
    buffer += sz;
    return sz;
}


ULng32 unpack(char*& buffer, ULng32*& ptr, ULng32& x, NAHeap* heap)
{
    ULng32 y = 0; // unpacked # of UINT32s
    ULng32 sz = unpack(buffer, y);

    if ( y > x ) {

      if ( ptr )
         NADELETEBASIC(ptr, heap);

      ptr = new (heap) ULng32[y];
      assert(ptr != NULL);
    }

    ULng32 sz1 = y * sizeof(ULng32);

    if ( y > 0 ) 
      str_cpy_all((char*)ptr, buffer, sz1);

    x = y;

    buffer += sz1;
    return sz+sz1;
}

////////////////////////////////////////////////////////////////////

UInt64 VarUIntArray::estimateMemoryInBytes(ULng32 x, ULng32 b)
{
  return (x*b + BitsPerWord - 1)/BitsPerWord * sizeof(ULng32) + sizeof(VarUIntArray);
}

UInt32 VarUIntArray::minPackedLength()
{
  return sizeof(numEntries_) +
         sizeof(numBits_) + 
         sizeof(maxVal_) +
         sizeof(numWords_);
}

UInt32 VarUIntArray::getPackedLength()
{
   return minPackedLength() + sizeof(numWords_) * numWords_;
}

VarUIntArray::VarUIntArray(ULng32 numEntries,
                           ULng32 numBits, NAHeap* heap) : 
counters_(NULL), numEntries_(numEntries), numBits_(numBits), heap_(heap)
{ 
  assert(numBits > 0 && numBits <= BitsPerWord);

  numWords_ = (ULng32)((((UInt64)numEntries)*numBits + BitsPerWord - 1)/BitsPerWord);

  if ( numWords_ > 0 ) {
     counters_ = new (heap_) ULng32[numWords_];
     memset(counters_, 0, numWords_ * sizeof(ULng32));
  }

  maxVal_ = AllOnes >> (BitsPerWord - numBits);
}


VarUIntArray::VarUIntArray(NAHeap* heap) :
     numEntries_(0), numWords_(0), numBits_(0), heap_(heap), counters_(NULL), maxVal_(0)
{
}


VarUIntArray::VarUIntArray(const VarUIntArray& x, NAHeap* heap) :
     numEntries_(x.numEntries_), numWords_(x.numWords_), numBits_(x.numBits_),
     maxVal_(x.maxVal_), heap_(heap)
{
  counters_ = new (heap_) ULng32[numWords_];
  str_cpy_all((char*)counters_, (char*)x.counters_, numWords_ * sizeof(ULng32));
}


void VarUIntArray::clear()
{
  memset(counters_, 0, numWords_ * sizeof(ULng32));
}


ULng32 VarUIntArray::operator[](ULng32 ix) const
{ 
  assert(ix >= 0 && ix < numEntries_);

  // compute bit and word offset of our counter
  ULng32 lowBit = ix * numBits_;
  ULng32 lowWord = lowBit >> 5; // / BitsPerWord(which is 32);
  ULng32 lowBitInWord = lowBit % BitsPerWord;
  ULng32 nextCounterStart = lowBitInWord + numBits_;

  // take the word where our counter starts, remove leading bits by
  // left-shifting, then shift our counter to the right part of the result
  ULng32 result = (counters_[lowWord] << lowBitInWord) >> (BitsPerWord - numBits_);

  // test if our counter continues into the next word
  if (nextCounterStart > BitsPerWord)
    {
      ULng32 numRemainingBits = nextCounterStart - BitsPerWord;

      // set rightmost numRemaining bits in result to 0
      result &= (AllOnes << numRemainingBits);
      // add remaining bits from next word
      result |= counters_[lowWord+1] >> (BitsPerWord - numRemainingBits);
    }

  return result;
}


NABoolean VarUIntArray::put(ULng32 ix, ULng32 val, NABoolean* prevBitsOff)
{
  assert(ix >= 0 && ix < numEntries_);

  if (val > maxVal_)
    val = maxVal_;

  // compute bit and word offset of our counter
  ULng32 lowBit = (ix * numBits_);
  ULng32 lowWord = lowBit >> 5; // / BitsPerWord(which is 32);
  ULng32 lowBitInWord = lowBit % BitsPerWord;
  ULng32 nextCounterStart = lowBitInWord + numBits_;

  // value to put, shifted to the correct position
  ULng32 shiftedPut = val;
  // mask indicating our counter in the word
  ULng32 putMask = (AllOnes >> lowBitInWord);

  if (nextCounterStart > BitsPerWord)
    {
      // putMask is good to go for 1st word
      // eliminate trailing bits from val
      shiftedPut >>= nextCounterStart - BitsPerWord;
    }
  else
    {
      // eliminate following counters' bits from putMask
      putMask &= AllOnes << (BitsPerWord - nextCounterStart);

      // left-shift value to put
      shiftedPut <<= BitsPerWord - nextCounterStart;
    }

  if ( prevBitsOff )
    *prevBitsOff = (counters_[lowWord] & putMask) ? FALSE : TRUE;

  // zero out old counter value in first (maybe only) word
  // putMask has ones for bits that belong to our counter
  counters_[lowWord] &= ~putMask;


  // drop in new counter value in first (maybe only) word
  counters_[lowWord] |= shiftedPut;


  // does counter extend into a second word?
  if (nextCounterStart > BitsPerWord)
    {
      putMask = AllOnes >> (nextCounterStart - BitsPerWord);
      // shift value to the left, leaving only the remaining bits that
      // need to go into the second word
      shiftedPut = val << (BitsPerWord - (nextCounterStart - BitsPerWord));

      if ( prevBitsOff )
         (*prevBitsOff) &= (counters_[lowWord+1] & putMask) ? FALSE : TRUE;

      // zero out old counter value in second word
      // here, putMask has zeroes for bits that belong to our counter
      counters_[lowWord+1] &= ~putMask;

      // drop in new counter value in first (maybe only) word
      counters_[lowWord+1] |= shiftedPut;
   }

  return (val == maxVal_);
}

NABoolean VarUIntArray::add(ULng32 ix, ULng32 val, ULng32& result)
{
  ULng32 oldVal = (*this)[ix];

  // check for overflow, both for exceeding maxVal_ and for overflow of ULng32
  result = oldVal + val;
  if (result > maxVal_ || result < oldVal || result < val)
    result = maxVal_;

  return put(ix, result);
}

NABoolean VarUIntArray::sub(ULng32 ix, ULng32 val, ULng32& minuend)
{
  ULng32 result = minuend = (*this)[ix];

  // don't change and return TRUE, if counter overflown before
  if (result == maxVal_)
    return TRUE;

  // don't let the value go below zero, indicate underflow
  if (val > result)
    {
      put(ix, 0);
      return TRUE;
    }

  // normal case, subtract val from current counter value
  result -= val;
  return put(ix, result);
}
  
NABoolean VarUIntArray::mergeViaOR(const VarUIntArray& other)
{
  if ( entries() != other.entries() )
    return FALSE;

  if ( getMaxVal() != other.getMaxVal() )
    return FALSE;

  if ( numBits() != other.numBits() )
    return FALSE;

  for (UInt32 i=0; i<numWords_; i++)
     counters_[i] |= other.counters_[i];

  return TRUE;
}

UInt32 VarUIntArray::bitsSet()
{
   UInt32 ct = 0;

   for ( UInt32 i=0; i<numWords_; i++ ) {
      if ( counters_[i] ) {

         UInt32 x = counters_[i];
         for (int k=0; k<32; k++ ) {
            if ( x & 0x00000001 )
               ct++;

            x >>= 1;
         }
      }
   }
   return ct;
}

void VarUIntArray::dump(ostream& out, const char* msg)
{
   if ( msg )
      out << msg << endl;

   // The following implemnents FNV-1 hash described at 
   // https://en.wikipedia.org/wiki/Fowler–Noll–Vo_hash_function 
   //const UInt32 prime = 16777619; 
   //Int64 hash = 0x811c9dc5;  // offset basis
   UInt32 prime = 16777619; 
   UInt32 hash = 0x811c9dc5;  // offset basis

   for ( UInt32 i=0; i<numWords_; i++ ) {

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

ULng32 VarUIntArray::packIntoBuffer(char*& buffer)
{
   ULng32 size = 0;
   size += pack(buffer, numEntries_);
   size += pack(buffer, numBits_);
   size += pack(buffer, maxVal_);
   size += pack(buffer, counters_, numWords_); // will pack numWords_ up front

   return size;
}

ULng32 VarUIntArray::unpackBuffer(char*& buffer)
{
   ULng32 sz = unpack(buffer, numEntries_);

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

  for (ULng32 i=0; i< 100; i++)
    {
      twoBitArray.put(i,   i % 2);
      threeBitArray.put(i, i % 8);
      fourBitArray.put(i,  i % 16);
      fiveBitArray.put(i,  i % 32);
      eightBitArray.put(i, i % 256);
    }
 
 for (ULng32 i=0; i< 100; i++)
    {
      printf("i: %4d %4d %4d %4d %4d %4d\n",
             i,
             twoBitArray[i],
             threeBitArray[i],
             fourBitArray[i],
             fiveBitArray[i],
             eightBitArray[i]);
    }

 for (ULng32 i=0; i<20; i++)
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
