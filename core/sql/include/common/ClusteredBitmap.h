//*********************************************************************
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
//*********************************************************************

//*********************************************************************
//
// File:         ClusteredBitmap.h
// Description:  This contains the class definition for the
//               ClusteredBitmap. This class provides a bitmap for
//               values that may be high and clustered around smaller
//               ranges of numbers.  It was implemented because some
//               queries cause the ValueIds to become very large and
//               a full bitmap is impractical for storing very large
//               ranges of numbers.
// Created:      1/20/2008
// Language:     C++
//
//*********************************************************************
#ifndef _CLUSTERED_BITMAP_H
#define _CLUSTERED_BITMAP_H

//*********************************************************************
// The ClusteredBitmap class contains a bitmap that allows high, sparse
// values where bits within the bitmap are clustered around certain
// groups of numbers.  For instance, ValueIds may have a few low
// numbers, some in the middle range, and some in the high range.
// Storing the ValueIds in this class uses a lot less memory than
// storing a complete bitmap for the ValueIds and may have performance
// improvements when compared to NASubCollection.  This class also
// only stores 32-bit values.
//
// When setting bits within the bitmap on a platform that handles 64
// bits at a time, the following bits of the value are used to find
// the bit within the bitmap:
//
//  Value Bits:  Description:
//      31-9     Lookup value in bitsToBitmap::significantBits_.
//      8-6      64-bit word within a 64-byte bitmap (8 64-bit words).
//      5-0      Bit within a 64-bit integer.
//
// For platforms that handle 32-bits at a time, the following bits
// are used:
//
//  Value Bits:  Description:
//      31-9     Lookup value in bitsToBitmap::significantBits_.
//      8-5      32-bit word within a 64-byte bitmap (16 32-bit words).
//      4-0      Bit within a 32-bit integer.
//
// Each bitmap that is allocated is 64-bytes, which allows 512
// bits per bitmap.  The bitmap is pointed to by
// bitsToBitmap::bitmap_.
//
// The bitsToBitmap struct is used within an array sorted by
// the significantBits_ value.  When a bitmap is allocated for
// a value, bits 31 through 9 are stored within the significantBits_
// location, and bitmap_ points to the bitmap for the less
// significant bits (bits 8-0).
//*********************************************************************

#include "qmscommon/Range.h"
#include "common/BloomFilter.h"

class ClusteredBitmap : public NABasicObject {
  friend class ClusteredBitmapIterator;

 public:
  // The cb_int_t type determines whether many of the operations work
  // on 64 or 32 bits at a time.  It gives a significant speed benefit
  // on 64-bit platforms that provide a 64-bit population count.
  typedef UInt32 cb_int_t;

 public:
  // Constructors
  ClusteredBitmap(CollHeap *heap);
  ClusteredBitmap(const ClusteredBitmap &other, CollHeap *heap);
  ClusteredBitmap(const ClusteredBitmap &other);

  // Destructor
  virtual ~ClusteredBitmap();

  // Access operators
  NABoolean nextUsed(CollIndex &start) const;
  CollIndex entries() const { return entries_; }
  NABoolean isEmpty() const { return entries_ == 0; }

  // Reset the bitmap to an empty state.
  void clear();

  // Get hash value based on set bits in the bitmap.
  int hash() const;

  // Insert values or other sets into into the bitmap.
  ClusteredBitmap &addSet(const ClusteredBitmap &other);
  ClusteredBitmap &addElement(UInt32 value);
  ClusteredBitmap &insert(UInt32 value) { return addElement(value); }
  ClusteredBitmap &insert(const ClusteredBitmap &other) { return addSet(other); }

  // Remove values or other sets from the bitmap
  ClusteredBitmap &subtractElement(UInt32 value);
  ClusteredBitmap &subtractSet(const ClusteredBitmap &other);
  ClusteredBitmap &remove(UInt32 value) { return subtractElement(value); }
  ClusteredBitmap &remove(const ClusteredBitmap &other) { return subtractSet(other); }

  // Check whether items exist
  NABoolean contains(const ClusteredBitmap &other) const;
  NABoolean contains(const UInt32 value) const;

  // Other logical operations on this bitmap
  ClusteredBitmap &intersectSet(const ClusteredBitmap &other);

  // Overloaded logical operators
  ClusteredBitmap &operator+=(const ClusteredBitmap &other) { return addSet(other); }
  ClusteredBitmap &operator+=(UInt32 value) { return addElement(value); }
  ClusteredBitmap &operator-=(const ClusteredBitmap &other) { return subtractSet(other); }
  ClusteredBitmap &operator-=(UInt32 value) { return subtractElement(value); }

  // Assignment operator
  ClusteredBitmap &operator=(const ClusteredBitmap &other);

  // Equality and inequality operators
  NABoolean operator==(const ClusteredBitmap &other) const;
  NABoolean operator!=(const ClusteredBitmap &other) const { return !operator==(other); }

  UInt32 countOfBitsSet();

  UInt32 totalSize();

  // get the length in bytes if this is packed.
  UInt32 getPackedLength();

  // Serialize this to a string of bytes.
  // Return the number of bytes of the serialized object.
  // Assume that buf has enough space to hold the entire object.
  UInt32 pack(char *buf);

  // De-serialize from buf, a string of bytes.
  // Return the number of bytes read from buf.
  UInt32 unpack(const char *buf);

  void dump(ostream &out, const char *title = NULL);

  // Convert to list formm: [<length> <value> ]*
  NABoolean convertToList(std::string &text);

 private:
#define CB_BITS_PER_WORD 32

  // The following struct is used within an array to map the most
  // significant bits of an index to the bitmap that contains the
  // bitmap.
  struct bitsToBitmap {
    UInt32 significantBits_;
    cb_int_t *bitmap_;
  };

  // Constants
  static const UInt32 SIGNIFICANT_BITS_MASK = 0xFFFFFE00;
#if (CB_BITS_PER_WORD == 32)
  static const UInt32 WORD_BITS_SHIFT = 5;
  static const UInt32 WORD_BITS_MASK = 0xF;
  static const UInt32 BITS_MASK = 0x1F;
  static const UInt32 WORDS_PER_BITMAP = 16;
#else
  static const UInt32 WORD_BITS_SHIFT = 6;
  static const UInt32 WORD_BITS_MASK = 0x7;
  static const UInt32 BITS_MASK = 0x3F;
  static const UInt32 WORDS_PER_BITMAP = 8;
#endif

  // Return the significant bits of a value
  static UInt32 significantBits(UInt32 value);

  // Return the word of the bitmap that contains the value
  static UInt32 word(UInt32 value);

  // Return the shifted bit value within the word for the value
  static cb_int_t shiftedBit(UInt32 value);

  // Returns the number of bits set in an integer.
  static UInt32 popCnt(cb_int_t value);

  // Find the first set bit of the value.
  static cb_int_t firstSetBit(cb_int_t value);

  // Initialize the 64-byte bitmap to zero.
  static void initializeBitmap(cb_int_t *bits);

  // Count the number of set bits in a 64-byte bitmap.
  static int countBits(cb_int_t *bits);

  // Copy the 64-byte bitmap to another 64-byte bitmap
  static void copyBits(cb_int_t *tobits, cb_int_t *frombits);

  // Copy the 64-byte bitmap to another 64-byte bitmap.  Also return
  // the number of bits that are set.
  static int copyCountBits(cb_int_t *tobits, cb_int_t *frombits);

  // Logically-OR two bit arrays storing the results in the first array
  static int orBits(cb_int_t *tobits, cb_int_t *frombits);

  // Logically-AND two bit arrays storing the results in the first array
  static int andBits(cb_int_t *tobits, cb_int_t *otherbits);

  // Clear bits in the first array that are set in the second array.
  static int clearBits(cb_int_t *tobits, cb_int_t *frombits);

  // Increase the size of the bitmap if needed
  void increaseBitmapMapSize(UInt32 numNeeded);

  // Allocate a new bitsToBitmap entry and a new bitmap
  cb_int_t *insertMap(UInt32 significantBits);

  // Returns the bitsToBitmap that is equal to the significant
  // bits or is the first greater than the passed in signficant
  // bits.
  bitsToBitmap *findEqualOrGreaterBitmapMap(UInt32 sigBits) const;

  // Returns the bitsToBitmap[].bitmap_ for a particular value.
  cb_int_t *findBitmap(UInt32 value) const;

  // dump all integer values (corresponding to the bits
  // set in bmap) to out.
  void dump(ostream &out, const bitsToBitmap &bmap, const char *title = NULL);

  // Convert to list formm: [<length> <value> ]*
  NABoolean convertToList(std::string &text, const bitsToBitmap &bmap);

  UInt32 pack(char *buf, const bitsToBitmap &bmap);
  UInt32 unpack(const char *buf, bitsToBitmap &bmap);

  void releaseMemory();

  CollHeap *heap_;           // NAHeap to allocate internal memory
  bitsToBitmap *bitmapMap_;  // Pointer to mapping of bitmaps
  UInt32 numBitmaps_;        // Number of bitmaps in mapping
  UInt32 maxBitmaps_;        // Size of bitmap map buffer (in elements)
  UInt32 entries_;           // Number of set bits
};

class ClusteredBitmapIterator : public NABasicObject {
 public:
  ClusteredBitmapIterator(ClusteredBitmap &x) : bitMap_(x) { init(); };

  ~ClusteredBitmapIterator(){};

  void init() { idx_ = 0; };
  ClusteredBitmap::bitsToBitmap *getEntry();
  void advance();

 private:
  ClusteredBitmap &bitMap_;
  int idx_;
};

// An abstract class to represent ranges of values.
class RangeOfValues : public NABasicObject {
 public:
  RangeOfValues(UInt32 packedSpaceCap, CollHeap *heap);
  ~RangeOfValues(){};

  virtual RangeOfValues &operator+=(const RangeOfValues &other) = 0;

  virtual NABoolean operator==(const RangeOfValues &other) const = 0;

  virtual RangeOfValues *clone(UInt32 packedSpaceCap, CollHeap *heap) = 0;

  virtual NABoolean insert(int value) = 0;
  virtual NABoolean insert(UInt32 value) = 0;
  virtual NABoolean insert(long value) = 0;
  virtual NABoolean insert(char *, int) = 0;
  virtual NABoolean insert(wchar_t *, int) = 0;
  virtual NABoolean insertDate(char *, int) = 0;
  virtual NABoolean insertTime(char *, int) = 0;
  virtual NABoolean insertTimestamp(char *, int) = 0;

  virtual NABoolean lookup(int value) = 0;
  virtual NABoolean lookup(UInt32 value) = 0;
  virtual NABoolean lookup(long value) = 0;
  virtual NABoolean lookup(char *, int) = 0;
  virtual NABoolean lookup(wchar_t *, int) = 0;
  virtual NABoolean lookupDate(char *, int) = 0;
  virtual NABoolean lookupTime(char *, int) = 0;
  virtual NABoolean lookupTimestamp(char *, int) = 0;

  virtual RangeOfValues &remove(int value) { return *this; };

  // total number of ranges
  virtual UInt32 entries(NABoolean estimate = FALSE) = 0;

  virtual void clear() = 0;

  virtual UInt32 getPackedLength() { return minPackedLength(); }

  UInt32 totalSizeCap() { return totalSizeCap_; };

  // pack the data structure into buf with enough space.
  virtual UInt32 pack(char *buf);

  // unpack the data structure from buf.
  // Assume unlimited memory to hold the  data structure.
  virtual UInt32 unpack(char *buf);

  // dump the content to out.
  virtual void dump(ostream &out, const char *title = NULL) = 0;

  // The enabled/disabled status.
  NABoolean isEnabled() { return enabled_; };
  void setEnable(NABoolean x) { enabled_ = x; };

  // The useful status, when enabled.
  virtual NABoolean isUseful(int maxNumEntries = -1) { return TRUE; };

  static UInt32 minPackedLength() { return sizeof(UInt32) + sizeof(NABoolean); }

 private:
  UInt32 totalSizeCap_;
  NABoolean enabled_;

 protected:
  CollHeap *heap_;
};  // RangeOfValues

class ClusteredBitmapForIntegers : public RangeOfValues {
 public:
  ClusteredBitmapForIntegers(UInt32 packedSpaceCap, CollHeap *heap);
  ~ClusteredBitmapForIntegers(){};

  RangeOfValues &operator+=(const RangeOfValues &other) {
    const ClusteredBitmapForIntegers *castedOther = dynamic_cast<const ClusteredBitmapForIntegers *>(&other);

    bitMapPos_ += castedOther->bitMapPos_;
    bitMapNeg_ += castedOther->bitMapNeg_;
    return *this;
  }

  NABoolean operator==(const RangeOfValues &other) const {
    const ClusteredBitmapForIntegers *castedOther = dynamic_cast<const ClusteredBitmapForIntegers *>(&other);

    return bitMapPos_ == castedOther->bitMapPos_ && bitMapNeg_ == castedOther->bitMapNeg_;
  }

  RangeOfValues *clone(UInt32 packedSpaceCap, CollHeap *heap) {
    return new (heap) ClusteredBitmapForIntegers(packedSpaceCap, heap);
  }

  NABoolean insert(int value);
  NABoolean insert(UInt32 value);
  NABoolean insert(long value) { return TRUE; };
  NABoolean insert(char *, int) { return TRUE; };
  NABoolean insert(wchar_t *, int) { return TRUE; };
  NABoolean insertDate(char *, int) { return TRUE; };
  NABoolean insertTime(char *, int) { return TRUE; };
  NABoolean insertTimestamp(char *, int) { return TRUE; };

  NABoolean lookup(int value) { return FALSE; };
  NABoolean lookup(UInt32 value) { return FALSE; };
  NABoolean lookup(long value) { return FALSE; };
  NABoolean lookup(char *, int) { return FALSE; };
  NABoolean lookup(wchar_t *, int) { return FALSE; };
  NABoolean lookupDate(char *, int) { return FALSE; };
  NABoolean lookupTime(char *, int) { return FALSE; };
  NABoolean lookupTimestamp(char *, int) { return FALSE; };

  RangeOfValues &remove(int value);

  UInt32 entries(NABoolean estimate = FALSE);

  void clear() {
    bitMapPos_.clear();
    bitMapNeg_.clear();
  };

  UInt32 totalSize();

  UInt32 getPackedLength() {
    return RangeOfValues::getPackedLength() + bitMapPos_.getPackedLength() + bitMapNeg_.getPackedLength();
  };

  // pack the data structure into buf with enough space.
  UInt32 pack(char *buf);

  // unpack the data structure from buf.
  // Assume unlimited memory to hold the  data structure.
  UInt32 unpack(char *buf);

  // dump the content to out.
  void dump(ostream &out, const char *title = NULL);

 private:
  ClusteredBitmap bitMapPos_;  // to hold 0 and positive values
  ClusteredBitmap bitMapNeg_;  // to hold negative values
};                             // ClusteredBitmapForIntegers

class RangeSpecRT : public RangeOfValues {
 public:
  RangeSpecRT(UInt32 packedSpaceCap, CollHeap *heap);
  ~RangeSpecRT(){};

  RangeOfValues &operator+=(const RangeOfValues &other);
  NABoolean operator==(const RangeOfValues &other) const;

  RangeOfValues *clone(UInt32 packedSpaceCap, CollHeap *heap) { return new (heap) RangeSpecRT(packedSpaceCap, heap); }

  NABoolean insert(int value);
  NABoolean insert(UInt32 value);
  NABoolean insert(long value);
  NABoolean insert(char *, int);
  NABoolean insert(wchar_t *, int);
  NABoolean insertDate(char *, int);
  NABoolean insertTime(char *, int);
  NABoolean insertTimestamp(char *, int);

  NABoolean lookup(int value);
  NABoolean lookup(UInt32 value);
  NABoolean lookup(long value);
  NABoolean lookup(char *, int);
  NABoolean lookup(wchar_t *, int);
  NABoolean lookupDate(char *, int);
  NABoolean lookupTime(char *, int);
  NABoolean lookupTimestamp(char *, int);

  RangeOfValues &remove(int value);

  UInt32 entries(NABoolean estimate = FALSE) { return rs_.getTotalRanges(); };

  void clear() { rs_.clear(); };

  // UInt32 totalSize()
  //{ return RangeOfValues::totalSize() + rs_.totalSize(); };

  UInt32 getPackedLength() { return RangeOfValues::getPackedLength() + rs_.getPackedLength(); };

  // pack the data structure into buf with enough space.
  UInt32 pack(char *buf);

  // unpack the data structure from buf.
  // Assume unlimited memory to hold the  data structure.
  UInt32 unpack(char *buf);

  // dump the content to out.
  void dump(ostream &out, const char *title = NULL);

  static UInt32 minPackedLength() { return RangeOfValues::minPackedLength() + 4; }

 private:
  RangeSpec rs_;
};

class BloomFilterRT : public RangeOfValues {
 public:
  BloomFilterRT(UInt32 packedSpaceCap, CollHeap *heap);
  ~BloomFilterRT(){};

  RangeOfValues &operator+=(const RangeOfValues &other);
  NABoolean operator==(const RangeOfValues &other) const;

  RangeOfValues *clone(UInt32 packedSpaceCap, CollHeap *heap) { return new (heap) BloomFilterRT(packedSpaceCap, heap); }

  // Compute the hash with the data
  // in big endian byte order.
  NABoolean insert(int value);
  NABoolean insert(UInt32 value);
  NABoolean insert(long value);
  NABoolean insert(wchar_t *, int);

  // Compute the hash with the data
  // in the order supplied
  NABoolean insert(char *, int);

  // Compute the hash with the data
  // in the ASCII format
  NABoolean insertDate(char *, int);
  NABoolean insertTime(char *, int);
  NABoolean insertTimestamp(char *, int);

  // Compute the hash with the data
  // in big endian byte order.
  NABoolean lookup(int value);
  NABoolean lookup(UInt32 value);
  NABoolean lookup(long value);
  NABoolean lookup(wchar_t *, int);

  // Compute the hash with the data
  // in the order supplied
  NABoolean lookup(char *, int);

  // Compute the hash with the data
  // in the ASCII format
  NABoolean lookupDate(char *, int);
  NABoolean lookupTime(char *, int);
  NABoolean lookupTimestamp(char *, int);

  UInt32 entries(NABoolean estimate = FALSE) { return rbf_.entries(estimate); };

  void clear() { rbf_.clear(); }

  NABoolean isUseful(int maxNumEntries = -1) { return rbf_.isUseful(maxNumEntries); }

  UInt32 getPackedLength() { return RangeOfValues::getPackedLength() + rbf_.getPackedLength(); }

  // pack the data structure into buf with enough space.
  UInt32 pack(char *buf);

  // unpack the data structure from buf.
  // Assume unlimited memory to hold the  data structure.
  UInt32 unpack(char *buf);

  // dump the content to out.
  void dump(ostream &out, const char *title = NULL);

  static UInt32 minPackedLength() { return RangeOfValues::minPackedLength() + RegularBloomFilter::minPackedLength(); }

  static int computeMaxLength(UInt32 m, float p = FALSE_POSITIVE_PROBABILITY);

 protected:
  RegularBloomFilter rbf_;
};

// No bytes swapping inside insert() or lookup().
// To be used with native C++ Parquet Reader, such as
// the Arrow Parquet reader.
class NativeBloomFilterRT : public BloomFilterRT {
 public:
  NativeBloomFilterRT(UInt32 packedSpaceCap, CollHeap *heap) : BloomFilterRT(packedSpaceCap, heap) {}
  ~NativeBloomFilterRT(){};

  RangeOfValues *clone(UInt32 packedSpaceCap, CollHeap *heap) {
    return new (heap) NativeBloomFilterRT(packedSpaceCap, heap);
  }

  // Compute the hash with the data
  // in big endian byte order.
  NABoolean insert(int value);
  NABoolean insert(UInt32 value);
  NABoolean insert(long value);
  NABoolean insert(wchar_t *, int);

  // Compute the hash with the data
  // in big endian byte order.
  NABoolean lookup(int value);
  NABoolean lookup(UInt32 value);
  NABoolean lookup(long value);
  NABoolean lookup(wchar_t *, int);

 private:
};

#endif
