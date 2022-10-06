
#ifndef SQL_BUFFER_SIZE_H
#define SQL_BUFFER_SIZE_H

/* -*-C++-*-
 *****************************************************************************
 *
 * File:         sql_buffer_neededsize.h
 * Description:  Definition for static method SqlBufferNeededSize
 *
 *
 * Created:      7/10/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

//
// NOTE : Definition for SqlBufferNeededSize and SqlBufferGetTuppSize moved to this file as part of warning
// elimination effort. If these static methods are defined in sql_buffer.h we get
// warning 262 (function defined but not referenced) when compiling various files such
// as ex_transaction.cpp. These functions are not used by all cpp files that include sql_buffer.h
// Therefore we moved SqlBufferNeededSize and SqlBufferGetTuppSize to this file and changed the cpp files that call
// SqlBufferNeededSize or SqlBufferGetTuppSize to include this file (11/01/2003)
//

static inline int SqlBufferHeaderSize(SqlBufferHeader::BufferType bufType) {
  // Return the buffer header size
  int headerSize = 0;
  switch (bufType) {
    case SqlBufferHeader::DENSE_:
      headerSize = sizeof(SqlBufferDense);
      break;

    case SqlBufferHeader::OLT_:
      headerSize = sizeof(SqlBufferOlt);
      break;

    case SqlBufferHeader::NORMAL_:
      headerSize = sizeof(SqlBufferNormal);
      break;

    default:
      headerSize = sizeof(SqlBufferNormal);
      break;
  }

  headerSize = ROUND8(headerSize);

  return (headerSize);
}

static inline int SqlBufferGetTuppSize(int recordLength = 0,
                                          SqlBufferHeader::BufferType bufType = SqlBufferHeader::NORMAL_) {
  int sizeofTuppDescriptor =
      ((bufType == SqlBufferHeader::DENSE_) ? ROUND8(sizeof(TupleDescInfo)) : sizeof(tupp_descriptor));

  return ROUND8(recordLength) + sizeofTuppDescriptor;
}

static inline int SqlBufferNeededSize(int numTuples = 0, int recordLength = 0,
                                         SqlBufferHeader::BufferType bufType = SqlBufferHeader::NORMAL_) {
  // Return the header size plus the size of any tuple descriptors
  // beyond the first (which is included in the header) plus the
  // size for the aligned data.
  int headerSize = SqlBufferHeaderSize(bufType);
  return (headerSize + (numTuples * SqlBufferGetTuppSize(recordLength, bufType)));
}

#endif
