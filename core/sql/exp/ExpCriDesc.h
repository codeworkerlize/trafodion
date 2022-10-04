

#ifndef EXP_CRI_DESC_H
#define EXP_CRI_DESC_H

// forward declarations
class ExpTupleDesc;
class ex_cri_desc;

#include "export/NAVersionedObject.h"
#include "exp/exp_tuple_desc.h"

// ---------------------------------------------------------------------
// Template instantiation to produce a 64-bit pointer emulator class
// for ExCriDesc
// ---------------------------------------------------------------------
typedef NAVersionedObjectPtrTempl<ex_cri_desc> ExCriDescPtr;

//
// An ex_cri_desc is a data structure that describes what information
// is pointed to by an atp_struct.
// (atp = array of tupp pointers, cri = composite row instance)
// The two acronyms ATP and CRI are there for historical reasons, we
// use ex_cri_desc for the descriptive part and ATP for the actual data,
// but the underlying idea is the same.
//
// Each atp_struct has a pointer to a cri_desc followed by an array tupp
// pointers. The cri_desc describes the each of the tupps pointed to.
//

class ex_cri_desc : public NAVersionedObject {
  enum { PACKED = 0x0001 };

  // pointer to array
  ExpTupleDescPtrPtr tupleDesc_;  // 00-07
  UInt32 flags_;                  // 08-11
  const UInt16 numTuples_;        // 12-13
  // ---------------------------------------------------------------------
  // Fillers for potential future extensions without changing class size.
  // When a new member is added, size of this filler should be reduced so
  // that the size of the object remains the same (and is modulo 8).
  // ---------------------------------------------------------------------
  char fillers_[10];  // 14-23

 public:
  ex_cri_desc(const unsigned short numTuples, void *space_);  // constructor

  ex_cri_desc() : NAVersionedObject(-1), numTuples_(0) {}

  inline unsigned short noTuples() const;

  inline ExpTupleDesc *getTupleDescriptor(const unsigned short tupleNo) const;

  inline void setTupleDescriptor(const unsigned short tupleNo, ExpTupleDesc *tupleDesc);

  Long pack(void *);
  int unpack(void *, void *reallocator);

  void display(const char *title = "");

  void display(Int32 pid, Int32 ExNodeId, const char *title = "");

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() { setImageVersionID(0, getClassVersionID()); }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
};  // descriptor for cri

inline unsigned short ex_cri_desc::noTuples() const { return numTuples_; };

inline ExpTupleDesc *ex_cri_desc::getTupleDescriptor(const unsigned short tupleNo) const {
  // Don't use ex_assert since class is shared with generator.
  // ex_assert(tupleNo < numTuples_ , "ex_cri_desc::setTupleDescriptor() tuple index out of range");
  return tupleDesc_[tupleNo];
};

inline void ex_cri_desc::setTupleDescriptor(const unsigned short tupleNo, ExpTupleDesc *tupleDesc) {
  // Don't use ex_assert since class is shared with generator.
  // ex_assert(tupleNo < numTuples_ , "ex_cri_desc::getTupleDescriptor() tuple index out of range");
  tupleDesc_[tupleNo] = tupleDesc;
};

#endif
