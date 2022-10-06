/* -*-C++-*-
****************************************************************************
*
* File:         HashRow.h
* Description:
*
* Created:      5/6/98
* Language:     C++
*
*

*
*
****************************************************************************
*/

#ifndef COMHASHROW_H
#define COMHASHROW_H

#include "common/BaseTypes.h"
#include "export/NABasicObject.h"

/////////////////////////////////////////////////////
// The HashRow here describes only the row header.
// The expressions working on the rows know where to
// find the data
// The class is NOT derived from ExGod! HashRow is
// never allocated with new. Instead, it is just an
// "overlay" on an allocated buffer.
/////////////////////////////////////////////////////

#define MASK31 0x7FFFFFFF
class HashRow {
  friend class HashTableCursor;
  friend class HashTable;
  friend class HashBufferSerial;

 public:
  inline HashRow(){};
  inline ~HashRow(){};
  void print(int rowlength);
  inline SimpleHashValue hashValue() const { return hashValue_ & MASK31; }

  // Return the raw hash value (no masking)
  inline SimpleHashValue hashValueRaw() const { return hashValue_; }

  inline void setHashValue(SimpleHashValue hv) { hashValue_ = hv & MASK31; }

  // Set the hash value to the raw hash value (no masking)
  inline void setHashValueRaw(SimpleHashValue hv) { hashValue_ = hv; }

  inline NABoolean bitSet() const { return ((hashValue_ & ~MASK31) != 0L); }
  inline void setBit(NABoolean val) {
    if (val)
      hashValue_ |= ~MASK31;
    else
      hashValue_ &= MASK31;
  }
  inline void setNext(HashRow *next) { next_ = next; }
  inline HashRow *next() const { return next_; }

  inline char *getData() const { return ((char *)this) + sizeof(HashRow); }

  inline UInt32 getRowLength() const { return *((UInt32 *)getData()); }

 private:
  inline void setRowLength(UInt32 rowLen) { *((UInt32 *)getData()) = rowLen; }

#ifdef CHECK_EYE
  inline void setEye() { eye_ = 42424242; }
  inline void checkEye() { assert(eye_ == 42424242); }

  UInt32 eye_;
#else
  inline void setEye() {}
  inline void checkEye() {}
#endif

  SimpleHashValue hashValue_;
  HashRow *next_;
};

#endif
