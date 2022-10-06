
/* -*-C++-*-
****************************************************************************
*
* File:         HashBufferHeader.h
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

#ifndef HASH_BUFFER_HEADER_H
#define HASH_BUFFER_HEADER_H

/////////////////////////////////////////////////////////////////////////////
// the HashBufferHeader describes the first few bytes of a buffer written
// to a temporary file.
// The class is NOT derived from NABasicObject. HashBufferHeader is never
// allocated with new. Instead, it is just an "overlay" on an allocated
// buffer.
/////////////////////////////////////////////////////////////////////////////
class HashBufferHeader {
  friend class HashBuffer;
  friend class HashBufferSerial;

 private:
  HashBufferHeader();
  ~HashBufferHeader(){};
  inline int getRowCount() const;
  inline void setRowCount(int rowCount);
  inline void incRowCount();

  int rowCount_;     // # rows in buffer (pointer to next free row)
  int bucketCount_;  // the buffer contains row from bucketCount_
                     // buckets. This is used in phase 3 on the
                     // hash join. If the cluster is a inner during
                     // this phase and there was a cluster split,
                     // not all rows in the buffer are chained
};

/////////////////////////////////////////////////////////////////////////////
// inline functions of HashBufferHeader
/////////////////////////////////////////////////////////////////////////////

inline int HashBufferHeader::getRowCount() const { return rowCount_; };

inline void HashBufferHeader::setRowCount(int rowCount) { rowCount_ = rowCount; };

inline void HashBufferHeader::incRowCount() { rowCount_++; };

#endif
