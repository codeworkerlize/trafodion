

/*
 **************************************************************************
 *                                                                        *
 *          Counting Bloom Filters                                        *
 *                                                                        *
 *                                                                        *
 *                                                                        *
 **************************************************************************
 */

#ifndef INCLUDE_BLOOM_FILTER_H
#define INCLUDE_BLOOM_FILTER_H

#include <string>
#include <vector>

#include "VarInt.h"
#include "common/Collections.h"
#include "common/ComSysUtils.h"
#include "common/NABoolean.h"
#include "common/NAMemory.h"
#include "common/Platform.h"

#define MAX_NUM_HASH_FUNCS         5
#define FALSE_POSITIVE_PROBABILITY 0.1

const UInt32 char_size = 0x08;  // 8 bits in 1 char(unsigned)

int computeHashTableSizeInBytes(UInt32 m,  // Input: # of bits for the hash table
                                float p    // Input: probability of false positive
);

class BloomFilter : public NABasicObject {
 public:
  BloomFilter(NAHeap *heap, UInt32 maxHashFuncs,
              UInt32 n,  // # of distinct elements
              float p    // probability of false positives
  );
  BloomFilter(UInt32 maxHashFuncs,
              UInt32 bytes,  // hash table size in bytes
              float p,       // probability of false positives
              NAHeap *heap);
  BloomFilter(NAHeap *heap);
  ~BloomFilter();

  double density();

  UInt32 numBytes() { return m_ / char_size; }
  UInt32 numFunctions() { return k_; }

  // set the key length info for all keys, useful when dealing
  // keys of fixed length (e.g. the info is SWAP_FOUR for SQL INTEGER).
  void setKenLengthInfo(int x) { keyLenInfo_ = x; };

  virtual int packIntoBuffer(char *&buffer);
  virtual int unpackBuffer(char *&buffer);

  static UInt32 minPackedLength();

  virtual void dump(ostream &out, const char *msg = NULL);

 protected:
  UInt32 computeFinalHash(UInt32 hash, int hashFunc);
  static UInt32 computeFinalHash(UInt32 hash, int hashFunc, UInt32 bits);

 protected:
  UInt32 m_;  // hash table size in bits
  UInt16 k_;  // the number of hash functions
  int keyLenInfo_;

  NAHeap *heap_;
};

class simple_cbf_key : public NABasicObject {
 public:
  simple_cbf_key(char *key = NULL, UInt32 key_len = 0, NAHeap *heap = NULL);
  simple_cbf_key(const simple_cbf_key &);
  ~simple_cbf_key();

  // equality operator
  NABoolean operator==(const simple_cbf_key &other) const {
    return (keyLen_ == other.keyLen_ && strncmp(key_, other.key_, keyLen_) == 0);
  }

  simple_cbf_key &operator=(const simple_cbf_key &other);

  char *getKey() const { return key_; };
  UInt32 getKeyLen() const { return keyLen_; };

  friend int scbfHashFunc(const simple_cbf_key &key);

 protected:
  UInt32 keyLen_;
  char *key_;
  NAHeap *heap_;
};

// The data structure to represent a key to look up the
// overflow table.
//
// The bucket_ (0-based) field stores the bucket number (the interval in
// a histogram) that the key is associated with.
class cbf_key : public simple_cbf_key {
 public:
  enum MFV_ENUM { NONE = 2, MFV = 0, MFV2 = 1 };

  cbf_key(char *key, UInt32 key_len, UInt32 bucket, cbf_key::MFV_ENUM mfv, NAHeap *heap);
  // This one declared but not defined in fast-stats prototype.
  cbf_key(char *key = NULL, UInt32 key_len = 0, NAHeap *heap = NULL);
  cbf_key(const cbf_key &);
  ~cbf_key() {}

  cbf_key &operator=(const cbf_key &other);

  // equality operator
  //   NABoolean operator ==(const cbf_key& other) const
  //   {
  //      return ( keyLen_ == other.keyLen_ &&
  //               strncmp(key_, other.key_, keyLen_) == 0 );
  //   }

  // char* getKey() const { return key_; };
  // UInt32 getKeyLen() const { return keyLen_; };
  UInt32 getBucket() const { return bucket_; };

  MFV_ENUM getMFV() const { return mfv_; }

  friend int cbfHashFunc(const cbf_key &key);

 protected:
  // char* key_;
  // UInt32 keyLen_;

  UInt32 bucket_;
  MFV_ENUM mfv_;

  // NAHeap* heap_;
};

class freqStruct : public NABasicObject {
 public:
  freqStruct(UInt64 freq = 0, UInt32 bucket = 0) : freq_(freq), bucket_(bucket){};
  virtual ~freqStruct(){};

  void setFreq(UInt64 f) { freq_ = f; };
  void setBucket(UInt32 b) { bucket_ = b; };

  UInt64 getFreq() { return freq_; };
  UInt32 getBucket() { return bucket_; };

  int operator==(freqStruct &);

 protected:
  UInt64 freq_;
  int bucket_;
};

class CountingBloomFilter : public BloomFilter {
 public:
  CountingBloomFilter(NAHeap *heap, UInt32 maxHashFuncs,
                      UInt32 n,                // # of distinct elements
                      float p,                 // probability of false positives
                      UInt32 nonOverflowFreq,  // non-overflow freq of n elements
                      UInt32 avgSkewedElements,
                      // # of buckets that keys will fall in.
                      // Assume each key maps to one
                      // bucket only
                      UInt32 numBuckets = 1);

  CountingBloomFilter(NAHeap *heap);
  ~CountingBloomFilter();

  // Clear the data structure by removing all frequency info.
  void clear();

  enum INSERT_ENUM { NORMAL = 0, NEW_MFV = 1, NO_SLOT = 2, PARAM_ERROR = 3 };

  // Insert into CBF a key. The bucketIdx is the bucket number
  // (0-based) of the bucket that the key falls in. A key belongs to one
  // bucket only.
  //
  // If the key is ready present in CBF, its frequency increases.
  //
  // CBF returns TRUE most of time, except when a key is with a NONE MFV status
  // and its frequency overflows into the synoposis table when the CBF is
  // CountingBloomFilterWithKnownSkews.
  CountingBloomFilter::INSERT_ENUM insert(char *key, UInt32 key_len, UInt32 bucketIdx = 0,
                                          cbf_key::MFV_ENUM = cbf_key::NONE);

  // Remove from CBF a key. The bucketIdx is the bucket number
  // (0-based) of the bucket that the key falls in. A key belongs to one
  // bucket only.
  //
  // If the key is ready present in CBF with a positive frequency , its frequency decreases.
  // When the frequency becomes zero, the key is removed from CBF.
  //
  //  Return TRUE when the frequency is positive before the deletion
  //         FALSE when the frequency is 0 before the deletion
  //
  NABoolean remove(char *key, UInt32 key_len, UInt32 bucketIdx = 0, cbf_key::MFV_ENUM = cbf_key::NONE);

  // Check if the key is present in CBF. Returns TRUE
  NABoolean contain(char *, UInt32 key_len, UInt64 *freq = NULL, UInt32 *bucket = NULL);

  // Method to fetch freq of freq in non-overflow area
  // Usage:
  //   for ( CollIndex b = 0; b<cbf.numBuckets(); b++ ) {
  //     for ( CollIndex i = 0; i<cbf.lowF2s(b).entries(); i++ ) {
  //        UInt32 freq = cbf.lowF2s(b)[i]; // freq is fi
  //     }
  //     UInt32 uec = cbf.uec(b);       // uec is # of unique value in bucket b
  //     UInt32 tf  = cbf.totalFreq(b); // tf is total frequency in bucket b
  //   }
  UInt32 numBuckets() { return numBuckets_; };
  VarUIntArray &lowF2s(UInt32 bucket = 1) { return *freq2sL_[bucket]; };

  UInt32 uec(UInt32 bucket = 1) { return uecs_[bucket]; };
  UInt32 totalFreq(UInt32 bucket = 1) { return totalFreqs_[bucket]; };

  // fetch a particular freq and freqfreq from overflow area.
  //
  // Usage:
  // cbf.computeOverflowF2s();
  // for (COllIndex i=0; i<cbf.getOverflowEntries(); i++) {
  //    x=cbf.highF2(i, y, b); // x, y and b are freqFreq, freq and
  // bucket# of the ith entry
  // in overflow area, respectively.
  // }
  virtual UInt32 getOverflowEntries() = 0;
  virtual UInt64 highF2(UInt32 i, UInt64 &freq, UInt32 &bucket);

  // Compute f2s for overflow entries. Must be called before
  // the f2s can be fetched through highF2() calls.
  virtual void computeOverflowF2s() = 0;

  // return total freq for both low and high freq items.
  UInt64 totalFreqForAll();

  // debug helper methods
  void printfreqfreq();
  void display_key(const char *msg, char *, UInt32 key_len, UInt32 bucket = 0);
  void display_hash_entry(const char *msg, UInt32 hash);
  void display_msg(const char *msg);

  virtual NABoolean canHandleArbitrarySkewedValue() = 0;

  void computeSumOfFrequencySquared(double *sumSq, int sz);

  // Save the current Fi values. Only one copy is saved.
  void saveFi();

  // Compute the max std dev of [fi(after) - fi(before)] for all buckets
  double computeMaxStdDevForDeltaOfFi();

  // Compute the standard deviation of variable = [fi(after) - fi(before)].
  // fi(before) is the Fi copy saved with call to saveACopyOfFi(),
  // fi(after) is the fi currently contained in cbf.
  double computeStdDevForDeltaOfFi(int i);

  static UInt64 estimateMemoryInBytes(UInt32 maxHashFuncs,
                                      UInt32 n,  // # of distinct elements
                                      float p,   // probability of false positives
                                                 // non-overflow freq of n elements
                                      UInt32 nonOverflowFreq, UInt32 avgSkewedElements,
                                      // # of buckets that keys will fall in.
                                      // Assume each key maps to one
                                      // bucket only
                                      UInt32 numBuckets = 1);

  int packIntoBuffer(char *&buffer);
  int unpackBuffer(char *&buffer);

  void setTotalMemSize(int x) { totalMemSz_ = x; };
  int getTotalMemSize() { return totalMemSz_; };

  void outputParams(char *buffer) { strcpy(buffer, paramBuf); };

  void setLogFile(char *filename) { filename_ = filename; };

 protected:
  virtual void insertIntoOverflowTable(const cbf_key &) = 0;
  virtual void removeFromOverflowTable(const cbf_key &) = 0;
  virtual UInt64 *searchOverflowTable(const cbf_key &) = 0;
  virtual void computeSumOfFrequencySquaredHighFreq(double *sumSq) = 0;

 protected:
  VarUIntArray freqsL_;  // counters for low freq keys

  VarUIntArrayPtr *freq2sL_;  // freq of freq in non-overflow area
  VarUIntArray uecs_;         // unique values per bucket
  VarUIntArray totalFreqs_;   // total frequency per bucket
  UInt32 numBuckets_;         // # of buckets dividing keys into disjoint sets

  freqStruct *freqsH_;        // freq in overflow area.
  UInt64 *freq2sH_;           // freq of freq in overflow area
  UInt32 actualOverflowF2s_;  // actual # of freq of freq in overflow area.

  VarUIntArrayPtr *savedFreq2sL_;  // Saved copy of freq of freq in
                                   // non-overflow area

  int totalMemSz_;  // total mem footprint when pack into a buffer

  VarUIntArray bucketNums_;  // Bucket nums for low freq keys. That is,
                             // bucketNums_[x] is the bucket # for
                             // the key whose low frequency is
                             // freqsL_[x]. We do not allow two
                             // keys from different buckets to collide.
                             // bucketNums_[x] == 0 when the slot
                             // is not taken by any key.

  char paramBuf[200];  // to save the params used by debugging

  char *filename_;
};

// A general CBF where the # of potential overflow keys is unbounded.
class GeneralCountingBloomFilter : public CountingBloomFilter {
 public:
  GeneralCountingBloomFilter(NAHeap *heap,
                             UInt32 maxHashFuncs,     // # of distinct elements
                             UInt32 n,                // # of distinct elements
                             float p,                 // probability of false positives
                             UInt32 nonOverflowFreq,  // non-overflow freq of n elements
                             UInt32 avgSkewedElements,
                             // # of buckets that keys will fall in.
                             // Assume each key maps to one
                             // bucket only
                             UInt32 numBuckets = 1);

  ~GeneralCountingBloomFilter();

  virtual UInt32 getOverflowEntries() { return overflowCountTable_.entries(); };

  NABoolean canHandleArbitrarySkewedValue() { return TRUE; };

  void computeSumOfFrequencySquaredHighFreq(double *sumSq);

 protected:
  virtual void insertIntoOverflowTable(const cbf_key &);
  virtual void removeFromOverflowTable(const cbf_key &);

  virtual UInt64 *searchOverflowTable(const cbf_key &key) { return overflowCountTable_.getFirstValue(&key); };

  virtual void computeOverflowF2s();

  static UInt64 estimateMemoryInBytesForOverflowTable(UInt32 avgSkewedElements, UInt32 numBuckets);

 protected:
  NAHashDictionary<cbf_key, UInt64> overflowCountTable_;
};

//
// A CBF where the # of overflow keys is bounded (to 2 per bucket now).
//
class CountingBloomFilterWithKnownSkews : public CountingBloomFilter {
 public:
  CountingBloomFilterWithKnownSkews(NAHeap *heap, UInt32 maxHashFuncs,
                                    UInt32 n,                  // expected # of distinct keys with low frequency
                                    float p,                   // probability of false positives
                                    UInt32 nonOverflowFreq,    // avg frequency of keys with low frequency
                                    UInt32 avgSkewedElements,  // expected # of distinct keys with high frequency
                                    UInt32 numBuckets = 1      // # of buckets that keys will fall in. Assume each key
                                                               // maps to one  bucket only
  );

  CountingBloomFilterWithKnownSkews(NAHeap *heap);

  ~CountingBloomFilterWithKnownSkews();

  static UInt64 estimateMemoryInBytes(UInt32 maxHashFuncs,
                                      UInt32 n,  // # of distinct elements
                                      float p,   // probability of false positives
                                                 // non-overflow freq of n elements
                                      UInt32 nonOverflowFreq, UInt32 avgSkewedElements,
                                      // # of buckets that keys will fall in.
                                      // Assume each key maps to one
                                      // bucket only
                                      UInt32 numBuckets = 1);

  virtual UInt32 getOverflowEntries() { return actualOverflowF2s_; };
  virtual UInt64 highF2(UInt32 i, UInt64 &freq, UInt32 &bucket);

  int packIntoBuffer(char *&buffer);
  int unpackBuffer(char *&buffer);

 protected:
  virtual void insertIntoOverflowTable(const cbf_key &);
  virtual void removeFromOverflowTable(const cbf_key &);

  virtual UInt64 *searchOverflowTable(const cbf_key &key);

  virtual void computeOverflowF2s(){};

  NABoolean canHandleArbitrarySkewedValue() { return FALSE; };

  UInt32 indexInOverflowArray(const cbf_key &key) { return 2 * key.getBucket() + (UInt32)key.getMFV(); }

  static UInt64 estimateMemoryInBytesForOverflowTable(UInt32 avgSkewedElements, UInt32 numBuckets);

  void computeSumOfFrequencySquaredHighFreq(double *sumSq);

 protected:
};

//-----------------------  new code for faststats ---------------------------

class FastStatsCountingBloomFilter : public BloomFilter {
 public:
  FastStatsCountingBloomFilter(NAHeap *heap,
                               UInt32 maxHashFuncs,       // # of distinct elements
                               UInt32 n,                  // # of distinct elements
                               float p,                   // probability of false positives
                               UInt32 maxNonOverflowFreq  // max of non-overflow
                                                          // freq of n elements
  );

  ~FastStatsCountingBloomFilter();

  NABoolean insert(char *key, UInt32 key_len, UInt32 frequency = 1);
  NABoolean remove(char *key, UInt32 key_len);
  NABoolean contain(char *key, UInt32 key_len, UInt64 *freq = 0);

  virtual UInt32 getOverflowEntries() { return overflowCountTable_.entries(); };

  NABoolean canHandleArbitrarySkewedValue() { return TRUE; };

  void computeSumOfFrequencySquaredHighFreq(double *sumSq);

  void printfreq(const char *colNames);

  const NAList<simple_cbf_key> &getAllKeys() { return keys_; };

  UInt64 getSizeInBytes(UInt32 maxHashFuncs,
                        UInt32 n,                // # of distinct elements
                        float p,                 // probability of false positives
                        UInt32 nonOverflowFreq,  // non-overflow freq
                        NABoolean isChar, int actualFixAmount);

 protected:
  virtual void insertIntoOverflowTable(const simple_cbf_key &, UInt32 freq = 1);
  virtual void removeFromOverflowTable(const simple_cbf_key &);

  virtual UInt64 *searchOverflowTable(const simple_cbf_key &key) { return overflowCountTable_.getFirstValue(&key); };

  virtual void computeOverflowF2s();

  static UInt64 estimateMemoryInBytesForOverflowTable(UInt32 avgSkewedElements, UInt32 numBuckets);

 protected:
  VarUIntArray counters_;  // counters for low freq keys
  NAList<simple_cbf_key> keys_;
  NAHashDictionary<simple_cbf_key, UInt64> overflowCountTable_;
};

// A regular bloom filter where each hash function turns on one bit
// in the hash table.
class RegularBloomFilter : public BloomFilter {
 public:
  RegularBloomFilter(NAHeap *heap,
                     UInt32 bytes,  // usable bytes for the filter
                     UInt32 maxHashFuncs = MAX_NUM_HASH_FUNCS, float p = FALSE_POSITIVE_PROBABILITY);
  RegularBloomFilter(NAHeap *heap);

  ~RegularBloomFilter(){};

  int packIntoBuffer(char *&buffer);
  int unpackBuffer(char *&buffer);

  NABoolean insert(char *key, UInt32 key_len);
  NABoolean contain(char *key, UInt32 key_len);
  NABoolean merge(RegularBloomFilter &);

  // Estimate number of entries (keys) inserted into the filter.
  // This method is called automatically in method unpackBuffer().
  float estimateEntries();

  // current false probability
  float currentFalseProbability();

  // determine if the filter's falase probability is under a
  // threshold and therefore still useful.
  NABoolean isUseful(int maxNumEntries = -1, float falseProbThreshold = 0.2);

  // return # of distinct keys in the table.
  int entries(NABoolean estimate = FALSE /* ignored */);

  int bitsSet() { return hashTable_.bitsSet(); }

  void clear() {
    entries_ = 0;
    hashTable_.clear();
  }

  int getPackedLength();
  static UInt32 minPackedLength();

  void dump(ostream &out, const char *msg = NULL);

  void insert(const char *dataFile);
  void lookup(const char *dataFile);
  static void test(const char *dataFile, UInt32 maxFuncs, UInt32 bytes, double prob);
  static void testHDPHash(const char *dataFile);
  static void testMultiHashInt(const char *dataFile, UInt32 maxFuncs, UInt32 bits);
  static void testMultiHashTimestamp(const char *dataFile, UInt32 maxFuncs, UInt32 bits);

 protected:
  UInt32 entries_;  // The number of keys inserted

  VarUIntArray hashTable_;  // the hash table
};

#endif
