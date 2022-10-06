

#ifndef EXP_ATP_H
#define EXP_ATP_H

// Local defines for assertions
//
//#define ExAtpAssert(a,b) ex_assert(a,b)
#define ExAtpAssert(a, b)

// Includes
//
#include "common/CommonLogger.h"

#include "export/ComDiags.h"
#include "exp/ExpCriDesc.h"
#include "exp/ExpSqlTupp.h"
#include "common/ComRtUtils.h"

// forward declarations
class ex_queue_entry;
class ExStatisticsArea;

//////////////////////////////////////////////////////////////////////////////
// CRI - Composite row instance
//

// Composite row instance is an array of pointers to records
// fragments

//
// An atp is an array of tupp's. Each tupp represents
// a pointer to a tuple.
//
// The first member of the array is actually a pointer to the composite row
// descriptor.

class atp_struct {
 public:
  inline ex_cri_desc *getCriDesc() const;
  inline tupp &getTupp(int i);
  inline tupp &getTuppForNativeExpr(long i);
  inline void setCriDesc(ex_cri_desc *p);
  // defined in ex_queue.h
  inline void copyAtp(const ex_queue_entry *from);
  inline void copyAtp(atp_struct *from);
  inline void copyPartialAtp(atp_struct *from);
  inline void copyPartialAtp(atp_struct *from, short first_tupp, short last_tupp);
  inline void copyPartialAtp(atp_struct *from, short my_first_tupp, short from_first_tupp, short last_tupp);
  inline unsigned short numTuples() const;
  inline void release();  // de-initialize
  inline void releasePartialAtp(short from_first_tupp, short last_tupp);

  //
  // Methods for manipulating diagnostics area.
  //
  inline ComDiagsArea *getDiagsArea() const;
  inline void setDiagsArea(ComDiagsArea *diagsArea);
  inline void initDiagsArea(ComDiagsArea *diagsArea);
  Long pack(void *space);
  int unpack(int base);

  // ****  information for GUI  *** -------------
  inline void set_tag(int tag);
  inline int get_tag() const;
  inline void unset_tag();

  // The passed-in cri will be used if it is not NULL. Otherwise
  // the cri associated with the atp is used.
  void display(const char *title = "", ex_cri_desc *cri = NULL);

  // print the content of a data field, based on the data type dt
  void print(char *title, Int16 dt, char *ptr, UInt32 len);

  void dumpATupp(ostream &out, const char *title, Int16 dt, char *ptr, UInt32 len);

  void dump(ostream &out, const char *title, int idx = -1, ex_cri_desc *cri = NULL);
  void dump(const char *title, int idx = -1, ex_cri_desc *cri = NULL);

  // display the idx tupp as a min/max tuple
  void dumpMinMaxTupp(const char *title, int idx, ex_cri_desc *cri = NULL);
  void dumpMinMaxTupp(ostream &, const char *title, int idx, ex_cri_desc *cri = NULL);
  void dumpMinMaxTupp2Log(logLevel level, const char *title, int idx, ex_cri_desc *cri = NULL);

  // return TRUE and set result argument to the value of the integer data
  // (32 bit or less) for tupp at index i, if the tupp is 16 or 32 bit
  // signed. If the data type is incorect or TUPP does not exist, return
  // FALSE.
  NABoolean getRegularIntSigned(int i, int &result);

  // return TRUE and set result argument to the value of the integer data
  // (32 bit or less) for tupp at index i, if the tupp is 16 or 32 bit
  // unsigned. If the data type is incorect or TUPP does not exist, return
  // FALSE.
  NABoolean getRegularIntUnsigned(int i, UInt32 &result);

 protected:
  // On return:
  //   void*: NULL. can not find the TUPP at index i
  //   dt: is undefined.
  //
  //   void: not NULL. The pointer to the TUPP data
  //   dt: the data type of the TUPP at index i
  void *checkAndGetMetaData(int i, Int16 &dt);

  //
  //---------------------------------------------

  //  inline              ~atp_struct();	// destructor

 private:
  int tagged_;             // TRUE or FALSE;
  ex_cri_desc *criDesc_;     // descriptor (num tupps, etc.)
  ComDiagsArea *diagsArea_;  // diagnostics area
  // the following data member is just a filler. Statistics areas are not passed
  // thru atps anymore. Pulling the data meber out would cause a reload of
  // the database, because atps are stored on disk for some dp2 expressions.
  ExStatisticsArea *statsArea_;
  tupp tuppArray_[1];  // array of tuple pointers--the actual length of
                       // this array is determined by the amount of space
                       // allocated for an atp struct instance.  e.g., if
                       // sizeof(atp_struct) + 2*sizeof(tupp) bytes of memory
                       // are allocated, then the length is 3
};

// Constructor of an Atp given a cri descriptor.  Can't make it a simple
// constructor since we need to allocate a variable length array.  In
// other words, the length of an ATP object is not statically known,
// but rather determined dynamically by the number of tuple pointers in
// the ATP.
// TBD: move to exp/ExpAtp.h

atp_struct *allocateAtp(int numTuples, CollHeap *space);

atp_struct *allocateAtp(ex_cri_desc *criDesc, CollHeap *space);

// Create an atp inside a pre-allocated buffer instead of allocating it from
// a space object. Used by ExpDP2Expr.
atp_struct *createAtpInBuffer(ex_cri_desc *criDesc, char *&buf);

void deallocateAtp(atp_struct *atp, CollHeap *space);

atp_struct *allocateAtpArray(ex_cri_desc *criDesc, int cnt, int *atpSize, CollHeap *space,
                             NABoolean failureIsFatal = FALSE);

void deallocateAtpArray(atp_struct **atp, CollHeap *space);

//
// inline procedures for atp_struct
//
inline ex_cri_desc *atp_struct::getCriDesc() const { return criDesc_; }

inline tupp &atp_struct::getTupp(int i) {
#ifdef _DEBUG
  if (i >= criDesc_->noTuples()) {
    displayCurrentStack(3);
    assert(0);
  }
#endif
  return tuppArray_[i];
}

//
// Used only when native expressions are generated.  Does not have assert.
//
inline tupp &atp_struct::getTuppForNativeExpr(long i) { return tuppArray_[i]; }

inline void atp_struct::setCriDesc(ex_cri_desc *p) { criDesc_ = p; };

inline unsigned short atp_struct::numTuples() const { return getCriDesc()->noTuples(); };

inline ComDiagsArea *atp_struct::getDiagsArea() const { return diagsArea_; }

inline void atp_struct::setDiagsArea(ComDiagsArea *diagsArea) {
  if (diagsArea_) diagsArea_->decrRefCount();

  diagsArea_ = diagsArea;
}

inline void atp_struct::initDiagsArea(ComDiagsArea *diagsArea) { diagsArea_ = diagsArea; }

inline void atp_struct::set_tag(int tag) { tagged_ = (short)tag; }

inline int atp_struct::get_tag() const { return (int)tagged_; }

inline void atp_struct::unset_tag() { tagged_ = 0; /* FALSE */ }

inline void atp_struct::copyAtp(atp_struct *from) {
  // copy the tupps from one atp to the other.
  // release those tupps in the target atp that are not being copied

  int i = numTuples();
  int j = from->numTuples();

  // release the entries past the number of tuples in the source if any
  while (i > j) {
    i--;
    this->getTupp(i).release();  // zero tupp
  }

  // copy the remaining entries
  while (i > 0) {
    i--;
    this->getTupp(i) = from->getTupp(i);  // copy tupp
  }

  set_tag(from->get_tag());

  if (from->getDiagsArea()) from->getDiagsArea()->incrRefCount();

  setDiagsArea(from->getDiagsArea());
}

// copies tupps first_tupp thru last_tupp from 'from'(source)
// to 'this' (target). tupp numbers are zero based.
inline void atp_struct::copyPartialAtp(atp_struct *from, short first_tupp, short last_tupp) {
  for (short i = first_tupp; i <= last_tupp; i++) {
    this->getTupp(i) = from->getTupp(i);  // copy tupp
  }
  set_tag(from->get_tag());
}

// copies tupps first_tupp thru last_tupp from 'from'(source)
// to 'this' (target). Copies starting at my_first_tupp.
// tupp numbers are zero based.
inline void atp_struct::copyPartialAtp(atp_struct *from, short my_first_tupp, short from_first_tupp, short last_tupp) {
  short j = my_first_tupp;

  for (short i = from_first_tupp; i <= last_tupp; i++, j++) {
    this->getTupp(j) = from->getTupp(i);  // copy tupp
  }
}

inline void atp_struct::copyPartialAtp(atp_struct *from) { copyPartialAtp(from, 0, from->numTuples() - 1); }

// De-initialize atp. deallocation is done elsewhere
// (probably done incorrectly)
inline void atp_struct::release() {
  ExAtpAssert(criDesc_, "atp_struct::release() has no criDesc");

  // Release each tupp.
  //

  unsigned short numTups = criDesc_->noTuples();
  for (int i = 0; i < numTups; i++) {
    tuppArray_[i].release();
  }

  // null-out diags area pointer; setDiagsArea releases existing
  // reference to diags area if it is non-null
  setDiagsArea(0);
}

inline void atp_struct::releasePartialAtp(short from_first_tupp, short last_tupp) {
  // Release each tupp.
  //
  for (int i = from_first_tupp; i <= last_tupp; i++) {
    tuppArray_[i].release();
  }

  // null-out diags area pointer; setDiagsArea releases existing
  // reference to diags area if it is non-null
  setDiagsArea(0);
}

#endif
