
/* -*-C++-*-
****************************************************************************
*
* File:         ExpCriDesc.cpp (previously /executor/ex_cri_desc.cpp)
* Description:
*
* Created:      5/6/98
* Language:     C++
*
*
*
****************************************************************************
*/

#include "common/Platform.h"

#include "comexe/ComPackDefs.h"
#include "exp/ExpCriDesc.h"
#include "common/ComSpace.h"
#include "exp/exp_tuple_desc.h"
#include "export/NAStringDef.h"
#include <iostream>

ex_cri_desc::ex_cri_desc(const unsigned short numTuples, void *space_) : numTuples_(numTuples), NAVersionedObject(-1) {
  tupleDesc_ = (ExpTupleDescPtr *)(((Space *)space_)->allocateAlignedSpace(numTuples_ * sizeof(ExpTupleDescPtr)));

  unsigned short i;
  for (i = 0; i < numTuples; i++) {
    tupleDesc_[i] = (ExpTupleDescPtrPtr)NULL;
  };

  flags_ = 0;
};

Long ex_cri_desc::pack(void *space) {
  if (!(flags_ & PACKED))  // REVISIT
  {
    tupleDesc_.pack(space, numTuples_);
    flags_ |= PACKED;
  }
  return NAVersionedObject::pack(space);
}

int ex_cri_desc::unpack(void *base, void *reallocator) {
  if (flags_ & PACKED)  // REVISIT
  {
    if (tupleDesc_.unpack(base, numTuples_, reallocator)) return -1;
    flags_ &= ~PACKED;
  }
  return NAVersionedObject::unpack(base, reallocator);
}

void ex_cri_desc::display(int pid, int exNodeId, const char *title) {
  char buf[100];
  snprintf(buf, sizeof(buf), "pid=%d, exNodeId=%d", pid, exNodeId);

  if (!title) {
    display(buf);
  } else {
    NAString msg(buf);
    msg += ", ";
    msg += title;
    display(msg.data());
  }
}

void ex_cri_desc::display(const char *title) {
  cout << title;

  for (int k = 0; k < noTuples(); k++) {
    ExpTupleDesc *tDesc = getTupleDescriptor(k);
    cout << "tupp(" << k << ")" << tDesc << endl;

    if (tDesc) {
      UInt32 attrs = tDesc->numAttrs();
      cout << "num of attris=" << attrs << endl;
      for (int j = 0; j < attrs; j++) {
        cout << "	attr[" << j << "]=" << tDesc->getAttr(j) << endl;
      }
    }
  }
}
