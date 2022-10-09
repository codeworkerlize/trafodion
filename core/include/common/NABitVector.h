
#ifndef NA_BITVECTOR_H
#define NA_BITVECTOR_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         NABitVector.h
 * Description:  A bit vector
 *               Similar to NASubArray, but with no superset.
 *
 * Created:      of course
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

// -----------------------------------------------------------------------

// -----------------------------------------------------------------------
// A bit vector.
// The bit vector class is not a template itself, but it is implemented
// using the template for a subarray. Note that the template type does
// not matter, the choice of <CollIndex> is arbitrary. The NASubCollection
// class is a specialization of a bit vector, and the more general type
// of a bit vector is now derived from the special type. This is mostly
// for historical reasons, some day we might change this to be the other
// way round.
// -----------------------------------------------------------------------

#include "common/BaseTypes.h"
#include "common/Collections.h"

class NABitVector : public NASubArray<CollIndex> {
 public:
  NABitVector(CollHeap *heap = NULL) : NASubArray<CollIndex>(NULL, heap) {}
  NABitVector(const NABitVector &other, CollHeap *heap = NULL) : NASubArray<CollIndex>(other, heap) {}
  void setBit(CollIndex b) { addElement(b); }
  void resetBit(CollIndex b) { subtractElement(b); }
};

#endif /* NA_BITVECTOR_H */
