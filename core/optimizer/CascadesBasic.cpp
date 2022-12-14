
/* -*-C++-*-
******************************************************************************
*
* File:         CascadesBasic.C
* Description:  Basic optimizer data types
* Created:      11/09/94
* Language:     C++
*
*
******************************************************************************
*/

#include "optimizer/CascadesBasic.h"

#include "optimizer/Analyzer.h"
#include "optimizer/Sqlcomp.h"

// -----------------------------------------------------------------------
// methods for class HashValue
// -----------------------------------------------------------------------

HashValue &HashValue::operator^=(const NAString &other) {
  val_ ^= other.hash();
  return *this;
}

HashValue &HashValue::operator^=(const ValueId &other) { return operator^=(other.getValueDesc()); }

HashValue &HashValue::operator^=(const ValueIdSet &other) {
  val_ ^= other.hash();
  return *this;
}

HashValue &HashValue::operator^=(const CANodeIdSet &other) {
  val_ ^= other.hash();
  return *this;
}

// -----------------------------------------------------------------------
//  Methods for class ReferenceCounter
//  NOTE: referenceCount_ is incremented when referenced, not when constructed.
// -----------------------------------------------------------------------
ReferenceCounter::ReferenceCounter() {
  referenceCount_ = 0;  // incremented when referenced, not constructed
}

// This copy ctor has the same semantics as the default C++ copy ctor,
// although it seems like with a copy we should start out with a fresh new
// zero in this's referenceCount_.  However, preexisting code assumes the
// default behavior, which we here just make explicit.
// I believe the only place making that assumption is
// sqlgen/GenPreCode, which "nm -o" shows makes one or more calls to
// ___ct__16ReferenceCounterFRC16ReferenceCounter (this ctor).
//
ReferenceCounter::ReferenceCounter(const ReferenceCounter &src) {
  referenceCount_ = src.referenceCount_;  // counterintuitive: not "= 0;"
}

// This assignment op has the same semantics as the default C++ one.
// See puzzled warnings immediately above, although
// "nm -o" on all .a library files does not currently show any references to
// ___as__16ReferenceCounterFRC16ReferenceCounter (this method), however.
//
ReferenceCounter &ReferenceCounter::operator=(const ReferenceCounter &src) {
  referenceCount_ = src.referenceCount_;  // counterintuitive: not "= 0;"
  return (*this);
}

ReferenceCounter::~ReferenceCounter() {
  if (referenceCount_ > 0)
    ABORT("premature destruction");
  else if (referenceCount_ < 0)
    ABORT("negative reference count value");
  referenceCount_ = -1;  // to detect really weird problems
}

void ReferenceCounter::incrementReferenceCount(int delta) {
  if (delta <= 0) ABORT("illegal increment delta for reference count");
  if (referenceCount_ < 0) ABORT("illegal reference count");

  referenceCount_ += delta;
}

void ReferenceCounter::decrementReferenceCount(int delta) {
  if (delta <= 0) ABORT("illegal decrement delta for reference count");
  if (referenceCount_ < delta) {
#ifndef NDEBUG
    // If this pointer is a dangling pointer
    // (e.g., a CutOp's groupAttributes pointer -- CutOp in persistent
    // global Rules, but a prior CMPASSERT in optimize() didn't let
    // the CutOp's ga to gracefully revert to NULL, and *ga on stmtHeap
    // has of course at this point been deallocated),
    // ignore it for now!
    void *h = (void *)collHeap();
    if (h == (void *)0xfafafafa || h == (void *)0xfdfdfdfd || h == (void *)0xcdcdcdcd || h == (void *)NULL) {
      cerr << "\ndecrRefCount: probably-dangling-pointer " << (void *)this << " (h_ = " << h << ")" << endl;

      // This assertion most likely will be caught by handler in opt.cpp,
      // and the next statement will get a fresh set of rules
      // (so compilation will continue after this one bobble).
      CMPASSERT(FALSE);
      return;
    }
#endif

    ABORT("illegal reference count decrement");
  }

  referenceCount_ -= delta;
  if (referenceCount_ == 0) delete this;
}

int ReferenceCounter::getReferenceCount() const { return (referenceCount_); }
