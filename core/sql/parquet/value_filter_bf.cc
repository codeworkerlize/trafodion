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
 * File:         value_filter_bf.cc
 *
 * Description:  All bloom filter filter related code.
 *
 * Created:      9/2/2018
 * Language:     C++
 *
 *
 *****************************************************************************
 */
#include "parquet/value_filter.h"
#include "parquet/types.h"
#include "parquet/util/stopwatch.h"
#include <cstdint>
#include <iostream>

#define MAX_NUM_HASH_FUNCS 5
#define FALSE_POSITIVE_PROBABILITY  0.1 

const uint32_t char_size  = 0x08;    // 8 bits in 1 char(unsigned)

uint32_t unpack(char*& buffer, uint16_t& x)
{
    uint32_t sz = sizeof(uint16_t);
    strncpy((char*)&x, buffer, sz);
    buffer += sz;
    return sz;
}

uint32_t unpack(char*& buffer, uint32_t& x)
{
    uint32_t sz = sizeof(uint32_t);
    strncpy((char*)&x, buffer, sz);
    buffer += sz;
    return sz;
}

uint32_t unpack(char*& buffer, int32_t& x)
{
    uint32_t sz = sizeof(int32_t);
    strncpy((char*)&x, buffer, sz);
    buffer += sz;
    return sz;
}

uint32_t unpack(char*& buffer, uint32_t*& ptr, uint32_t& numWords)
{
    uint32_t sz = unpack(buffer, numWords);

    if ( ptr )
      delete ptr;

    ptr = new uint32_t[numWords];
    assert(ptr != NULL);

    uint32_t sz1 = numWords * sizeof(uint32_t);

    if ( sz1 > 0 ) 
      memcpy((char*)ptr, buffer, sz1);


    buffer += sz1;
    return sz+sz1;
}

static float ln(float x)
{ return (float)(log(x) / log(2.0)); }

template <typename DType>
class BloomFilterEval: public TypedValueFilter<DType> 
{
public:
  BloomFilterEval();
  ~BloomFilterEval();

  uint32_t numBytes() { return m_ / char_size; }
  uint32_t numFunctions() { return k_; }

  // set the key length info for all keys, useful when dealing
  // keys of fixed length (e.g. the info is SWAP_FOUR for SQL INTEGER).
  void setKenLengthInfo(int32_t x) { keyLenInfo_ = x; };

  virtual uint32_t unpackBuffer(char*& buffer, int len);

  uint32_t minimalPackLen();

  void eval(std::shared_ptr<arrow::NumericArray<DType>>* values) ;
  void eval(std::shared_ptr<arrow::StringArray>* values) ;
  void eval(std::shared_ptr<arrow::BooleanArray>* values) ;

  void display();

  // init the data members with values stored in the data array
  // of length len. 
  int32_t init(const char* data, int len);

  void disableMe() { disabled_ = true; }
  bool isDisabled() { return disabled_; }

  bool contain(const char* key, uint32_t key_len);

protected:
  uint32_t check_bit(uint32_t ix) const;
  uint32_t totalBitsSet(bool displayBitsSet = false) const;
  uint32_t getSignatureHash() const;

  void postProcessing(int64_t len, int64_t valuesKept);

protected:
  uint32_t m_;  // hash table size in bits
  uint16_t k_;  // the number of hash functions
  int32_t keyLenInfo_;  

  uint32_t entries_; // number of items in the filter

  uint32_t numEntries_; // number of entries in the hash table
  uint32_t numWords_;   // number of words needed 
  uint32_t numBits_;    // number of bits per entry (1)
  uint32_t maxVal_;     // max value (1)
  uint32_t *counters_;  // the array of words

  bool disabled_;
  int64_t totalValuesProcessed_;
};

template <typename DType>
BloomFilterEval<DType>::BloomFilterEval() : 
  keyLenInfo_(0), 
  m_(0), 
  k_(0),
  numEntries_(0),
  numWords_(0),
  numBits_(0),
  maxVal_(0),
  counters_(NULL),
  disabled_(false)
{}

template <typename DType>
BloomFilterEval<DType>::~BloomFilterEval() 
{
   delete counters_;
}

template <typename DType>
uint32_t BloomFilterEval<DType>::minimalPackLen()
{
   return sizeof(m_) + sizeof(k_) +
          sizeof(keyLenInfo_) + 
          sizeof(entries_) +
          sizeof(numEntries_) +
          sizeof(numBits_) + sizeof(maxVal_) + sizeof(numWords_);
}

template <typename DType>
uint32_t BloomFilterEval<DType>::unpackBuffer(char*& buffer, int len)
{
  if ( len < minimalPackLen() )
     return 0;

   uint32_t size = unpack(buffer, m_);
   size += unpack(buffer, k_);
   size += unpack(buffer, keyLenInfo_);

   size += unpack(buffer, entries_);

   size += unpack(buffer, numEntries_);
   size += unpack(buffer, numBits_);
   size += unpack(buffer, maxVal_);

   size += unpack(buffer, counters_, numWords_);
   
   return size;
}
  
template <typename DType>
int32_t BloomFilterEval<DType>::init(const char* data, int len) 
{
   char* buf = const_cast<char*>(data);
   uint32_t sz = this->unpackBuffer(buf, len);

   return ( sz == len ) ? 0 : -1;
}

#define CONSTRUCT_BLOOMFILTER(elemType, data, len) \
   BloomFilterEval<elemType>* bf =  \
            new BloomFilterEval<elemType>(); \
   int32_t rc = bf->init(data, len); \
   if ( rc == 0 ) return bf; \
   delete bf; \
   return NULL

ValueFilter* 
ValueFilter::MakeBloomFilterEval(bool , const char* data, int len) 
{ CONSTRUCT_BLOOMFILTER(arrow::BooleanType, data, len); }

ValueFilter* 
ValueFilter::MakeBloomFilterEval(int8_t, const char* data, int len) 
{ CONSTRUCT_BLOOMFILTER(arrow::Int8Type, data, len); }

ValueFilter* 
ValueFilter::MakeBloomFilterEval(int16_t, const char* data, int len) 
{ CONSTRUCT_BLOOMFILTER(arrow::Int16Type, data, len); }

ValueFilter* 
ValueFilter::MakeBloomFilterEval(int32_t, const char* data, int len) 
{ CONSTRUCT_BLOOMFILTER(arrow::Int32Type, data, len); }

ValueFilter* 
ValueFilter::MakeBloomFilterEval(int64_t, const char* data, int len) 
{ CONSTRUCT_BLOOMFILTER(arrow::Int64Type, data, len); }

ValueFilter* 
ValueFilter::MakeBloomFilterEval(float, const char* data, int len) 
{ CONSTRUCT_BLOOMFILTER(arrow::FloatType, data, len); }

ValueFilter* 
ValueFilter::MakeBloomFilterEval(double, const char* data, int len) 
{ CONSTRUCT_BLOOMFILTER(arrow::DoubleType, data, len); }

ValueFilter* 
ValueFilter::MakeBloomFilterEval(parquet::Int96, const char* data, int len) 
{ CONSTRUCT_BLOOMFILTER(arrow::Time96Type, data, len); }

ValueFilter* 
ValueFilter::MakeBloomFilterEval(std::string&, const char* data, int len) 
{ CONSTRUCT_BLOOMFILTER(std::string, data, len); }

template <typename DType>
void BloomFilterEval<DType>::display() 
{
   std::cout << "Bloom filter: ";
   std::cout << "m_=" << m_;
   std::cout << ", k_=" << k_;
   std::cout << ", numEntries_=" << numEntries_;
   uint32_t bitsSet = totalBitsSet(false);
   std::cout << ", bitsSetInHashTable=" << bitsSet;

   std::cout << ", hash(hex)=" << std::hex << getSignatureHash()<< std::dec << std::endl;
}

template <typename DType>
uint32_t BloomFilterEval<DType>::getSignatureHash() const
{
   uint32_t prime = 16777619; 
   uint32_t hash = 0x811c9dc5;  // offset basis

   for ( uint32_t i=0; i<numWords_; i++ ) {
     hash ^= counters_[i]; 
     hash *= prime; 
   }
   return hash;
}

template <typename DType>
uint32_t BloomFilterEval<DType>::totalBitsSet(bool displayBitsSet) const
{
  uint32_t totalBitsSet = 0;

  if ( displayBitsSet )
      std::cout << std::endl;
  
  for ( int i=0; i<numWords_; i++ ) {

     uint32_t x = counters_[i];

     if ( x > 0 ) {

        if ( displayBitsSet )
           std::cout << "hashtable[" << i<< "]=" 
                     << std::hex
                     << x 
                     << std::dec
                     << std::endl;

        for ( int j=0; j<32; j++ ) {
          totalBitsSet += x & 0x1;
          x >>= 1;
        }
     } 
  }
  
  return totalBitsSet;
}

// This part of the code is an exact duplication of ExHDPHash::hash() 
// contained in exp_function.cpp for the purpose of avoiding a dependency
// of the executor to the arrow reader.
static const uint32_t randomHashValues[256] = {
    0x905ebe29, 0x95ff0b84, 0xe5357ed6, 0x2cffae90,
    0x8350b3f1, 0x1748a7eb, 0x2a0695db, 0x1e7ca00c,
    0x60f80c24, 0x9a41fe1c, 0xa985a647, 0x0ed7e512,
    0xcd34ef43, 0xe06325a6, 0xecbf735a, 0x76540d38,
    0x35cba55d, 0xff539efc, 0x64545d45, 0xd7112c0d,
    0x17e09e1c, 0x02359d32, 0x45976350, 0xd630a578,
    0x34cd0c12, 0x754546f6, 0x1bf4f249, 0xbc65c34f,
    0x5c932f44, 0x6cb0d8d0, 0xfd0e7030, 0x2b160e3b,
    0x101daff6, 0x25bbcb9d, 0xe7eca21f, 0x6d3b24ca,
    0xaef7e6b9, 0xd212f049, 0x2de2817e, 0x2792bcd5,
    0x67f794b2, 0xaec6f7cc, 0x79a3e367, 0xd5a85114,
    0xa98ecc2d, 0xf373e266, 0x58ae2757, 0xd8faa0ff,
    0x45e7eb61, 0xbd72ba1e, 0xc28f6b16, 0x804bc2e6,
    0xfed74984, 0x881cd177, 0xa02647e8, 0xd799d053,
    0xbe143d12, 0x49177474, 0xbbc0c5f4, 0x99f7fe9f,
    0x24fc1559, 0xce0925cf, 0x1dded5f4, 0x1d1a2cd3,
    0xafe3ef48, 0x6fd5d075, 0x4a63bc1d, 0x93aa36c0,
    0x2942d778, 0xb26a2444, 0x5616cc50, 0x7565c161,
    0xa006197b, 0xee700b07, 0x4a236a82, 0x693db870,
    0x9a919e64, 0x995b05b1, 0xd4659569, 0x90e45846,
    0xbca11996, 0x3e345cd9, 0xb29a9967, 0x7e9e66f7,
    0x9ce136d0, 0xcde74e76, 0xde56e4bb, 0xba4dc6ae,
    0xf9d40779, 0x4e5c0bdb, 0xde14f9e5, 0x278f8745,
    0x13ce0128, 0x8bb308f5, 0x4c41a359, 0x273d1927,
    0x50338e76, 0xdfceb7c2, 0xf1b86f68, 0xc8b12d6a,
    0xf4cb0e08, 0xa74b4b14, 0x81571c6a, 0xebc4a928,
    0x1d6d5fd6, 0x7f4bbc87, 0x61ba542f, 0x9b06d11d,
    0xb53ae1c1, 0xdcc2a6c0, 0x7f04f8a8, 0x8da9d186,
    0xa168e054, 0x21ed0ce7, 0x9ca9e9d1, 0x0e01fb38,
    0xd8b6b1d9, 0xb8d10266, 0x203a9de1, 0x37ba3ffe,
    0x9fefb09f, 0x5e4cb3e2, 0xcecd03b4, 0xcc270838,
    0xa1619089, 0x22995679, 0x6dcd6b78, 0x8c50f9b1,
    0x1c354ada, 0x48a0f13e, 0xca7b4696, 0x5c1fe8bf,
    0xdd0f433f, 0x8aa411f1, 0x149b2ee3, 0x181d16a1,
    0x3b84b01d, 0xee745103, 0x0f230907, 0x663d1014,
    0xd614181b, 0xb1b88cc9, 0x015f672c, 0x660ea636,
    0x4107c7f3, 0x6f0d8afe, 0xf0aeffeb, 0x93b25fa0,
    0x620c9075, 0x155a4d7e, 0x10fdbd73, 0xb162eabe,
    0xaf9605db, 0xba35d441, 0xde327cfa, 0x15a6fd70,
    0x0f2f4b54, 0xfb1b4995, 0xec092e68, 0x37ebade6,
    0x850f63ca, 0xe72a879f, 0xc823f741, 0xc6f114b8,
    0x74e461f6, 0x1d01ad14, 0xfe1ed7d3, 0x306b9444,
    0x9ebd40a6, 0x3275b333, 0xa8540ca1, 0xeb8d394c,
    0xa2aef54c, 0xf12d0705, 0x8974e70e, 0x59ae82cf,
    0x32469aca, 0x973325d8, 0x27ba604d, 0x9aeb7827,
    0xaf0af97c, 0x9783e6f8, 0xe0725a87, 0x2f02d864,
    0x717a0587, 0x0c90d7b0, 0x6828b84e, 0xba08ebe7,
    0x65cf8360, 0x63132f80, 0xbb8d4a41, 0xbd5b8b41,
    0x459f019f, 0x5e68369f, 0xe855f000, 0xa79a634c,
    0x172c7704, 0x07337ab3, 0xb2926453, 0x11084c8a,
    0x328689ca, 0xa7e3efcf, 0x8b9a5695, 0x76b65bbe,
    0x87bb5a2a, 0x5f73e6ad, 0xcf59b265, 0x4fe46ec9,
    0x52561232, 0x70db002c, 0xc21d1b8f, 0xd7ceb1c6,
    0xff4a97c8, 0xdd21c90b, 0x48c14c38, 0x64262c68,
    0x74c5d3f9, 0x66bf60e7, 0xce804348, 0x98585792,
    0x7619fc86, 0x91de3f72, 0x57f5191c, 0x576d9737,
    0x5f4535b0, 0xb9ee8ef5, 0x2e9eff6c, 0xc7c9f874,
    0xe6ac0843, 0xd93b8c08, 0x2f34a779, 0x407799eb,
    0x2b9904e0, 0x14bb018f, 0x1fcf367b, 0x7975c362,
    0xba31448f, 0xa59286f7, 0x1255244a, 0xd685169b,
    0xc791ec84, 0x3b5461b1, 0x4822924a, 0x26d86175,
    0x596e6b2f, 0x6a157bef, 0x8bc98a9b, 0xa8220343,
    0x91eaad8a, 0x42b89a9e, 0x7c9b5f81, 0xb5f9ec6c,
    0xd999ef9e, 0xa547f6a3, 0xc391f010, 0xe9d8bb43
  };

static uint32_t hash8(const char *data)
{
  unsigned char *valp = (unsigned char *)data;
  uint32_t hashValue = 0;

  hashValue = randomHashValues[*valp++];
  hashValue = (hashValue << 1 | hashValue >> 31) ^ randomHashValues[*valp++];
  hashValue = (hashValue << 1 | hashValue >> 31) ^ randomHashValues[*valp++];
  hashValue = (hashValue << 1 | hashValue >> 31) ^ randomHashValues[*valp++];
  hashValue = (hashValue << 1 | hashValue >> 31) ^ randomHashValues[*valp++];
  hashValue = (hashValue << 1 | hashValue >> 31) ^ randomHashValues[*valp++];
  hashValue = (hashValue << 1 | hashValue >> 31) ^ randomHashValues[*valp++];
  hashValue = (hashValue << 1 | hashValue >> 31) ^ randomHashValues[*valp++];

  return hashValue;
}

static uint32_t ExHDPHash(const char *data, int32_t length)
{
  uint32_t hashValue = 0;
  unsigned char *valp = (unsigned char *)data;
  int32_t iter = 0; // iterator over the key bytes, if needed
  
  // Speedup for long keys - compute first 8 bytes fast (the rest with a loop)
  if ( length >= 8 ) {
    hashValue = hash8(data);

    // continue with the 9-th byte (only when length > 8 )
    valp = (unsigned char *)&data[8];
    iter = 8; 
  }

  for(; iter < length; iter++) {
    // Make sure the hashValue is sensitive to the byte position.
    // One bit circular shift.
    hashValue = 
     (hashValue << 1 | hashValue >> 31) ^ randomHashValues[*valp++];

  }

  return hashValue;
}

uint32_t computeFinalHash(uint32_t hash, int32_t hashFunc, uint32_t bits)
{
   char hashNum = (char) hashFunc;
   uint32_t h2 = ExHDPHash((char*)&hashNum, sizeof(hashNum));

   return (uint32_t)(((hash<<1 | hash>>31) ^ h2) % bits);
}

static const uint32_t BitsPerWord = 32;
static const uint32_t AllOnes = 0xFFFFFFFF;

template <typename DType>
uint32_t BloomFilterEval<DType>::check_bit(uint32_t ix) const
{
  assert(ix >= 0 && ix < numEntries_);

  // compute bit and word offset of our counter
  uint32_t lowBit = (ix * numBits_);
  uint32_t lowWord = lowBit / BitsPerWord;
  uint32_t lowBitInWord = lowBit % BitsPerWord;
  uint32_t nextCounterStart = lowBitInWord + numBits_;

  // take the word where our counter starts, remove leading bits by
  // left-shifting, then shift our counter to the right part of the result
  uint32_t result = (counters_[lowWord] << lowBitInWord) >> (BitsPerWord - numBits_);

  // test if our counter continues into the next word
  if (nextCounterStart > BitsPerWord)
    {
      uint32_t numRemainingBits = nextCounterStart - BitsPerWord;

      // set rightmost numRemaining bits in result to 0
      result &= (AllOnes << numRemainingBits);
      // add remaining bits from next word
      result |= counters_[lowWord+1] >> (BitsPerWord - numRemainingBits);
    }

  return result;
}

//#define DEBUG_BLOOM_FILTER_CONTAIN 1

template <typename DType>
bool BloomFilterEval<DType>::contain(const char* key, uint32_t key_len)
{
   uint32_t hashValueCommon = ExHDPHash(key, key_len);

#ifdef DEBUG_BLOOM_FILTER_CONTAIN
   std::cout << "BFV::contain(): hashValueCommon=" << hashValueCommon << std::endl;
#endif

   for(int32_t i=0; i<k_; i++)
   {
      uint32_t hash_index = computeFinalHash(hashValueCommon, i, m_);

#ifdef DEBUG_BLOOM_FILTER_CONTAIN
   std::cout << i << "th hash=" << hash_index << std::endl;
#endif

      if ( check_bit(hash_index) == 0 ) { // is the bit on?
#ifdef DEBUG_BLOOM_FILTER_CONTAIN
        std::cout << "result is false" << std::endl;
#endif
        return false;
      }
   }

#ifdef DEBUG_BLOOM_FILTER_CONTAIN
   std::cout << "result is true" << std::endl;
#endif

   return true;
}

//#define DEBUG_BLOOM_FILTER_EVAL 1
//#define DEBUG_BLOOM_FILTER_POST_PROCESSING 1

#define SHOW_RESULT_FOR_VALUE \
      if ( contain((char*)&value, sizeof(value)) ) { \
        std::cout << "<<< At idx=" << i; \
        std::cout << ", bit=1"; \
        std::cout << ", value=" << value << std::endl;  \
      } 

#define SHOW_RESULT_FOR_STRING \
      if ( contain(value.data(), value.length()) ) { \
        std::cout << "<<< At idx=" << i; \
        std::cout << ", bit=1"; \
        std::cout << ", value=" << value << std::endl;  \
      }

#define SHOW_RESULT_FOR_BOOL\
      if ( contain((char*)&value, 1)) { \
        std::cout << "<<< At idx=" << i; \
        std::cout << ", bit=1"; \
        std::cout << ", value=" << value << std::endl;  \
      }

template <class DType, class VType>
void evalOneNumericSpan(VType* vArray, int64_t offset, int64_t len, 
                        BloomFilterEval<DType>* vFilter, 
                        uint8_t* selectedBits,
                        int elemSize, int64_t& valuesKept
                        )
{
   int64_t num_hits = 0;

   int64_t idx = offset;     // bit index to valueArray[]

   int firstFew = offset % 8;

   if ( firstFew > 0 )
     firstFew = 8 - firstFew;

   for (int j=0; j<firstFew; j++ ) {

     if ( vFilter->contain((char*)&vArray[idx], elemSize) ) {
        arrow::BitUtil::AndBit(selectedBits, idx, true);
        valuesKept++;
     }

     idx++;
   }

   const int numSmallEvals = (len-firstFew) / 8;
   int leftOver = (len-firstFew) % 8;
   uint8_t oneSmallEval = 0;

   for (int j=0; j<numSmallEvals; j++ ) {

        if ( selectedBits[j] ) {

           // Evaluate the predicate for the current 
           // 8 values
           oneSmallEval = 0;
   
           if ( vFilter->contain((char*)&vArray[idx++], elemSize) ) {
             oneSmallEval |= arrow::BitUtil::kBitmask[0];
             valuesKept++;
           }
   
           if ( vFilter->contain((char*)&vArray[idx++], elemSize) ) {
             oneSmallEval |= arrow::BitUtil::kBitmask[1];
             valuesKept++;
           }
   
           if ( vFilter->contain((char*)&vArray[idx++], elemSize) ) {
             oneSmallEval |= arrow::BitUtil::kBitmask[2];
             valuesKept++;
           }
   
           if ( vFilter->contain((char*)&vArray[idx++], elemSize) ) {
             oneSmallEval |= arrow::BitUtil::kBitmask[3];
             valuesKept++;
           }
   
           if ( vFilter->contain((char*)&vArray[idx++], elemSize) ) {
             oneSmallEval |= arrow::BitUtil::kBitmask[4];
             valuesKept++;
           }
   
           if ( vFilter->contain((char*)&vArray[idx++], elemSize) ) {
             oneSmallEval |= arrow::BitUtil::kBitmask[5];
             valuesKept++;
           }
   
           if ( vFilter->contain((char*)&vArray[idx++], elemSize) ) {
             oneSmallEval |= arrow::BitUtil::kBitmask[6];
             valuesKept++;
           }
   
           if ( vFilter->contain((char*)&vArray[idx++], elemSize) ) {
             oneSmallEval |= arrow::BitUtil::kBitmask[7];
             valuesKept++;
           }
   
           selectedBits[j] &= oneSmallEval;
        } else
           idx += 8;
   }

   for (int j=0; j<leftOver; j++ ) {
     if ( vFilter->contain((char*)&vArray[idx], elemSize) ) {
        arrow::BitUtil::AndBit(selectedBits, idx, true);
        valuesKept++;
     }
     idx++;
   }
}

template <typename DType>
void BloomFilterEval<DType>::eval(
   std::shared_ptr<arrow::NumericArray<DType>>* values
                                  )
{
   if ( !values || isDisabled() || (*values)->length() == 0) 
     return;

   auto vArray = (*values)->raw_values();
   int64_t len = (*values)->length();

   uint8_t* selectedBits = 
         const_cast<uint8_t*>((*values)->selected_bitmap_data());

   uint8_t* nullBits = 
         const_cast<uint8_t*>((*values)->null_bitmap_data());

   // If the null bitmap is available, use it to clear
   // the selected bitmap. 
   if ( nullBits )
     arrow::BitUtil::applySqlIsNotNull(selectedBits, nullBits, len);


   int elemSize = sizeof(vArray[0]);

   const skipVecType& vec = (*values)->getSkipVec();

#ifdef DEBUG_BLOOM_FILTER_EVAL 
   std::cout << "Apply Bloom predicate(s) for NumericArray<DType>:";
   this->display();
   std::cout << ", source len=" << len;

   ::arrow::internal::BitmapReader reader(selectedBits, 0, len);

   std::cout << ", #bits set in selected bitmap=" 
             << reader.totalBitsSet() << std::endl;

   ::arrow::internal::BitmapReader readerN(nullBits, 0, len);
   std::cout << ", #bits set in null bitmap=" 
             << readerN.totalBitsSet();

   std::cout << "skipVec=" << vec << std::endl;

   parquet::StopWatch x;
   x.Start();
#endif

   int64_t valuesKept = 0;

   int64_t offset = 0;

   for (auto skip : vec) {

      switch (skip.code) {
        case skipType::REJECT:
          break;

        case skipType::IN_RANGE:

          if ( this->canSkipInRangeSpan() )
             break;
          // otherwise fall through to the OVERLAP case 
          // below to evaluate

        case skipType::OVERLAP:
          evalOneNumericSpan(vArray, offset, skip.len, this, 
                             selectedBits, elemSize, valuesKept);
          break;
      }

      offset += skip.len;
   }

#ifdef DEBUG_BLOOM_FILTER_EVAL 
   std::cout << "Done! total eval time (us)=" << x.Stop()
             << ", valuesKept=" << valuesKept
             << ", final #bits set in selected bitmap=" << reader.totalBitsSet() 
             << std::endl 
             << std::endl;
#endif

   postProcessing(len, valuesKept);
}

template <typename DType>
void 
BloomFilterEval<DType>::postProcessing(int64_t len, int64_t valuesKept)
{
   this->incValuesKept(valuesKept);
   this->incValuesRemoved(len-valuesKept);

#ifdef DEBUG_BLOOM_FILTER_POST_PROCESSING
   std::cout << "Calling BloomFilterEval::postProcessing(): " 
             << "this length=" << len 
             << ", totalValuesKept=" << this->getValuesKept()
             << ", totalValuesRemoved=" << this->getValuesRemoved()
             << ", RowsToSurvive=" << this->getRowsToSurvive()
             << std::endl;
#endif

   if ( this->getRowsToSurvive() > 0 ) 
   {
      if ( this->getValuesKept() > this->getRowsToSurvive() ) 
      {
        this->disableMe();
#ifdef DEBUG_BLOOM_FILTER_POST_PROCESSING
        std::cout << "disableMe() called" << std::endl;
#endif
      } 
   }
}

// -----------------------------------------------------------
//  String data type
// -----------------------------------------------------------
void evalOneStringSpan(std::shared_ptr<arrow::StringArray>* values, 
                       int64_t offset, int64_t len, 
                       BloomFilterEval<std::string>* vFilter, 
                       uint8_t* selectedBits,
                       int64_t& valuesKept
                       )
{
   int64_t num_hits = 0;

   int64_t idx = offset;     // bit index to valueArray[]

   int firstFew = offset % 8;

   if ( firstFew > 0 )
     firstFew = 8 - firstFew;

   const uint8_t* data = NULL;
   int32_t dataLen = 0;

   for (int j=0; j<firstFew; j++ ) {

     data = (*values)->GetValue(idx, &dataLen);
     if ( vFilter->contain((char*)data, dataLen) ) {
        arrow::BitUtil::AndBit(selectedBits, idx, true);
        valuesKept++;
     }

     idx++;
   }

   const int numSmallEvals = (len-firstFew) / 8;
   int leftOver = (len-firstFew) % 8;
   uint8_t oneSmallEval = 0;

   for (int j=0; j<numSmallEvals; j++ ) {

        if ( selectedBits[j] ) {

           // Evaluate the predicate for the current 
           // 8 values
           oneSmallEval = 0;
   
           data = (*values)->GetValue(idx++, &dataLen);
           if ( vFilter->contain((char*)data, dataLen) ) {
             oneSmallEval |= arrow::BitUtil::kBitmask[0];
             valuesKept++;
           }
   
           data = (*values)->GetValue(idx++, &dataLen);
           if ( vFilter->contain((char*)data, dataLen) ) {
             oneSmallEval |= arrow::BitUtil::kBitmask[1];
             valuesKept++;
           }
   
           data = (*values)->GetValue(idx++, &dataLen);
           if ( vFilter->contain((char*)data, dataLen) ) {
             oneSmallEval |= arrow::BitUtil::kBitmask[2];
             valuesKept++;
           }
   
           data = (*values)->GetValue(idx++, &dataLen);
           if ( vFilter->contain((char*)data, dataLen) ) {
             oneSmallEval |= arrow::BitUtil::kBitmask[3];
             valuesKept++;
           }
   
           data = (*values)->GetValue(idx++, &dataLen);
           if ( vFilter->contain((char*)data, dataLen) ) {
             oneSmallEval |= arrow::BitUtil::kBitmask[4];
             valuesKept++;
           }
   
           data = (*values)->GetValue(idx++, &dataLen);
           if ( vFilter->contain((char*)data, dataLen) ) {
             oneSmallEval |= arrow::BitUtil::kBitmask[5];
             valuesKept++;
           }
   
           data = (*values)->GetValue(idx++, &dataLen);
           if ( vFilter->contain((char*)data, dataLen) ) {
             oneSmallEval |= arrow::BitUtil::kBitmask[6];
             valuesKept++;
           }
   
           data = (*values)->GetValue(idx++, &dataLen);
           if ( vFilter->contain((char*)data, dataLen) ) {
             oneSmallEval |= arrow::BitUtil::kBitmask[7];
             valuesKept++;
           }
   
           selectedBits[j] &= oneSmallEval;
        } else
           idx += 8;
   }

   for (int j=0; j<leftOver; j++ ) {
     data = (*values)->GetValue(idx, &dataLen);
     if ( vFilter->contain((char*)data, dataLen) ) {
        arrow::BitUtil::AndBit(selectedBits, idx, true);
        valuesKept++;
     }
     idx++;
   }
}

template <>
void BloomFilterEval<std::string>::eval(
      std::shared_ptr<arrow::StringArray>* values
                                       )
{
   if ( !values || isDisabled() || (*values)->length() == 0) 
     return;

   int64_t len = (*values)->length();

   uint8_t* selectedBits = 
         const_cast<uint8_t*>((*values)->selected_bitmap_data());

   uint8_t* nullBits = 
         const_cast<uint8_t*>((*values)->null_bitmap_data());

   // If the null bitmap is available, use it to clear
   // the selected bitmap. 
   if ( nullBits )
     arrow::BitUtil::applySqlIsNotNull(selectedBits, nullBits, len);

   const skipVecType& vec = (*values)->getSkipVec();

#ifdef DEBUG_BLOOM_FILTER_EVAL 
   std::cout << "Apply Bloom predicate(s) for StringArray:";
   this->display();
   std::cout << ", source len=" << len;

   ::arrow::internal::BitmapReader reader(selectedBits, 0, len);

   std::cout << ", #bits set in selected bitmap=" 
             << reader.totalBitsSet() << std::endl;

   ::arrow::internal::BitmapReader readerN(nullBits, 0, len);
   std::cout << ", #bits set in null bitmap=" 
             << readerN.totalBitsSet() << std::endl;

   std::cout << "skipVec=" << vec << std::endl;

   parquet::StopWatch x;
   x.Start();
#endif

   int64_t valuesKept = 0;

   int64_t offset = 0;

   for (auto skip : vec) {

      switch (skip.code) {
        case skipType::REJECT:
          break;

        case skipType::IN_RANGE:

          if ( this->canSkipInRangeSpan() )
             break;
          // otherwise fall through to the OVERLAP case 
          // below to evaluate

        case skipType::OVERLAP:
          evalOneStringSpan(values, offset, skip.len, this, 
                            selectedBits, valuesKept);
          break;
      }

      offset += skip.len;
   }

#ifdef DEBUG_BLOOM_FILTER_EVAL 
   std::cout << "Done! total eval time (us)=" << x.Stop()
             << ", valuesKept=" << valuesKept
             << ", final #bits set in selected bitmap=" << reader.totalBitsSet() 
             << std::endl 
             << std::endl;
#endif

   postProcessing(len, valuesKept);

}

// --------------------------------------------------------
//  Boolean type
// --------------------------------------------------------
void evalOneBooleanSpan(std::shared_ptr<arrow::BooleanArray>* values, 
                        int64_t offset, int64_t len, 
                        BloomFilterEval<arrow::BooleanType>* vFilter, 
                        uint8_t* selectedBits,
                        int64_t& valuesKept
                        )
{
   int64_t num_hits = 0;

   int64_t idx = offset;     // bit index to valueArray[]

   int firstFew = offset % 8;

   if ( firstFew > 0 )
     firstFew = 8 - firstFew;

   const uint8_t* data = NULL;
   int32_t dataLen = 0;

   for (int j=0; j<firstFew; j++ ) {

     auto value = (*values)->Value(idx);
     if ( vFilter->contain((char*)&value, 1) ) {
        arrow::BitUtil::AndBit(selectedBits, idx, true);
        valuesKept++;
     }

     idx++;
   }

   const int numSmallEvals = (len-firstFew) / 8;
   int leftOver = (len-firstFew) % 8;
   uint8_t oneSmallEval = 0;

   for (int j=0; j<numSmallEvals; j++ ) {

        if ( selectedBits[j] ) {

           // Evaluate the predicate for the current 
           // 8 values
           oneSmallEval = 0;
   
           auto value = (*values)->Value(idx++);
           if ( vFilter->contain((char*)&value, 1) ) {
             oneSmallEval |= arrow::BitUtil::kBitmask[0];
             valuesKept++;
           }
   
           value = (*values)->Value(idx++);
           if ( vFilter->contain((char*)&value, 1) ) {
             oneSmallEval |= arrow::BitUtil::kBitmask[1];
             valuesKept++;
           }
   
           value = (*values)->Value(idx++);
           if ( vFilter->contain((char*)&value, 1) ) {
             oneSmallEval |= arrow::BitUtil::kBitmask[2];
             valuesKept++;
           }
   
           value = (*values)->Value(idx++);
           if ( vFilter->contain((char*)&value, 1) ) {
             oneSmallEval |= arrow::BitUtil::kBitmask[3];
             valuesKept++;
           }
   
           value = (*values)->Value(idx++);
           if ( vFilter->contain((char*)&value, 1) ) {
             oneSmallEval |= arrow::BitUtil::kBitmask[4];
             valuesKept++;
           }
   
           value = (*values)->Value(idx++);
           if ( vFilter->contain((char*)&value, 1) ) {
             oneSmallEval |= arrow::BitUtil::kBitmask[5];
             valuesKept++;
           }
   
           value = (*values)->Value(idx++);
           if ( vFilter->contain((char*)&value, 1) ) {
             oneSmallEval |= arrow::BitUtil::kBitmask[6];
             valuesKept++;
           }
   
           value = (*values)->Value(idx++);
           if ( vFilter->contain((char*)&value, 1) ) {
             oneSmallEval |= arrow::BitUtil::kBitmask[7];
             valuesKept++;
           }
   
           selectedBits[j] &= oneSmallEval;
        } else
           idx += 8;
   }

   for (int j=0; j<leftOver; j++ ) {
     auto value = (*values)->Value(idx);
     if ( vFilter->contain((char*)&value, 1) ) {
        arrow::BitUtil::AndBit(selectedBits, idx, true);
        valuesKept++;
     }
     idx++;
   }
}

template <>
void BloomFilterEval<arrow::BooleanType>::eval(
      std::shared_ptr<arrow::BooleanArray>* values
                                       )
{
   if ( !values || isDisabled() || (*values)->length() == 0) 
     return;

   int64_t len = (*values)->length();

   uint8_t* selectedBits = 
         const_cast<uint8_t*>((*values)->selected_bitmap_data());

   uint8_t* nullBits = 
         const_cast<uint8_t*>((*values)->null_bitmap_data());

   // If the null bitmap is available, use it to clear
   // the selected bitmap. 
   if ( nullBits )
     arrow::BitUtil::applySqlIsNotNull(selectedBits, nullBits, len);

   const skipVecType& vec = (*values)->getSkipVec();

#ifdef DEBUG_BLOOM_FILTER_EVAL 
   std::cout << "Apply Bloom predicate(s) for BooleanArray:";
   this->display();
   std::cout << ", source len=" << len;

   ::arrow::internal::BitmapReader reader(selectedBits, 0, len);

   std::cout << ", #bits set in selected bitmap=" 
             << reader.totalBitsSet() << std::endl;

   ::arrow::internal::BitmapReader readerN(nullBits, 0, len);
   std::cout << ", #bits set in null bitmap=" 
             << readerN.totalBitsSet();

   std::cout << "skipVec=" << vec << std::endl;

   parquet::StopWatch x;
   x.Start();
#endif

   int64_t valuesKept = 0;

   int64_t offset = 0;

   for (auto skip : vec) {

      switch (skip.code) {
        case skipType::REJECT:
          break;

        case skipType::IN_RANGE:

          if ( this->canSkipInRangeSpan() )
             break;
          // otherwise fall through to the OVERLAP case 
          // below to evaluate

        case skipType::OVERLAP:
          evalOneBooleanSpan(values, offset, skip.len, this, 
                              selectedBits, valuesKept);
          break;
      }

      offset += skip.len;
   }

#ifdef DEBUG_BLOOM_FILTER_EVAL 
   std::cout << "Done! total eval time (us)=" << x.Stop()
             << ", valuesKept=" << valuesKept
             << ", final #bits set in selected bitmap=" << reader.totalBitsSet() 
             << std::endl 
             << std::endl;
#endif

   postProcessing(len, valuesKept);
}

// about NumericArray
eval_boilerplateA(BloomFilterEval, std::string, arrow::NumericArray);
eval_boilerplateA(BloomFilterEval, arrow::BooleanType, arrow::NumericArray);

// about StringArray 
eval_boilerplateB(BloomFilterEval, arrow::Int8Type, arrow::StringArray);
eval_boilerplateB(BloomFilterEval, arrow::Int16Type, arrow::StringArray);
eval_boilerplateB(BloomFilterEval, arrow::Int32Type, arrow::StringArray);
eval_boilerplateB(BloomFilterEval, arrow::Int64Type, arrow::StringArray);
eval_boilerplateB(BloomFilterEval, arrow::FloatType, arrow::StringArray);
eval_boilerplateB(BloomFilterEval, arrow::DoubleType, arrow::StringArray);
eval_boilerplateB(BloomFilterEval, arrow::Time96Type, arrow::StringArray);
eval_boilerplateB(BloomFilterEval, arrow::BooleanType, arrow::StringArray);


// about BooleanArray
eval_boilerplateB(BloomFilterEval, arrow::Int8Type, arrow::BooleanArray);
eval_boilerplateB(BloomFilterEval, arrow::Int16Type, arrow::BooleanArray);
eval_boilerplateB(BloomFilterEval, arrow::Int32Type, arrow::BooleanArray);
eval_boilerplateB(BloomFilterEval, arrow::Int64Type, arrow::BooleanArray);
eval_boilerplateB(BloomFilterEval, arrow::FloatType, arrow::BooleanArray);
eval_boilerplateB(BloomFilterEval, arrow::DoubleType, arrow::BooleanArray);
eval_boilerplateB(BloomFilterEval, arrow::Time96Type, arrow::BooleanArray);
eval_boilerplateB(BloomFilterEval, std::string,       arrow::BooleanArray);

