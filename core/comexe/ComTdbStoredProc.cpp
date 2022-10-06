
/* -*-C++-*-
****************************************************************************
*
* File:         ComTdbStoredProc.cpp
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

#include "comexe/ComTdbStoredProc.h"

#include "comexe/ComTdbCommon.h"

// ---------------------------------------------------------------------
// ComTdbStoredProc::ComTdbStoredProc()
// ---------------------------------------------------------------------
ComTdbStoredProc::ComTdbStoredProc(char *spName, ex_expr *inputExpr, int inputRowlen, ex_expr *outputExpr,
                                   int outputRowlen, ex_cri_desc *workCriDesc, const unsigned short workAtpIndex,
                                   ex_cri_desc *criDescDown, ex_cri_desc *criDescUp, ExSPInputOutput *extractInputExpr,
                                   ExSPInputOutput *moveOutputExpr, queue_index fromParent, queue_index toParent,
                                   Cardinality estimatedRowCount, int numBuffers, int bufferSize, ex_expr *predExpr,
                                   UInt16 arkcmpInfo)
    : ComTdb(ComTdb::ex_STORED_PROC, eye_STORED_PROC, estimatedRowCount, criDescDown, criDescUp, fromParent, toParent,
             numBuffers, bufferSize),
      spName_(spName),
      inputExpr_(inputExpr),
      inputRowlen_(inputRowlen),
      outputExpr_(outputExpr),
      outputRowlen_(outputRowlen),
      workCriDesc_(workCriDesc),
      workAtpIndex_(workAtpIndex),
      extractInputExpr_(extractInputExpr),
      moveOutputExpr_(moveOutputExpr),
      flags_(0),
      predExpr_(predExpr),
      arkcmpInfo_(arkcmpInfo) {}

ComTdbStoredProc::~ComTdbStoredProc() {}

Long ComTdbStoredProc::pack(void *space) {
  spName_.pack(space);
  workCriDesc_.pack(space);
  inputExpr_.pack(space);
  outputExpr_.pack(space);
  predExpr_.pack(space);

  // The contents of extractInputExpr_ and moveOutputExpr_ are packed
  // separately outside of this function in RelInternalSP::codeGen() via a
  // call to generateSPIOExpr() (both in generator/GenStoredProc.cpp).
  //
  // Offsets of pointers in these expressions are based on a different base
  // address (the beginning of the expressions themselves) since they are
  // sent by the executor to the hosting process of the SP without being
  // unpacked first. The hosting process will then unpack those expressions.
  //
  extractInputExpr_.packShallow(space);
  moveOutputExpr_.packShallow(space);

  return ComTdb::pack(space);
}

int ComTdbStoredProc::unpack(void *base, void *reallocator) {
  if (spName_.unpack(base)) return -1;
  if (workCriDesc_.unpack(base, reallocator)) return -1;
  if (inputExpr_.unpack(base, reallocator)) return -1;
  if (outputExpr_.unpack(base, reallocator)) return -1;
  if (predExpr_.unpack(base, reallocator)) return -1;

  // DO NOT unpack the contents of extractInput and moveOutput expressions
  // here. The executor will send them in their packed forms to the hosting
  // process of the SP, where they will then be unpacked.
  //
  if (extractInputExpr_.unpackShallow(base)) return -1;
  if (moveOutputExpr_.unpackShallow(base)) return -1;

  return ComTdb::unpack(base, reallocator);
}

/////////////////////////////////////////////////
ExSPInputOutput::ExSPInputOutput() : NAVersionedObject(-1) {
  str_cpy_all(eyeCatcher_, "SPIO", 4);
  totalLen_ = 0;
  tupleDesc_ = (ExpTupleDescPtr)NULL;
  caseIndexArray_ = (Int16Ptr)NULL;
  flags_ = 0;
}

void ExSPInputOutput::initialize(ExpTupleDesc *tupleDesc, int totalLen, ConvInstruction *caseIndexArray) {
  tupleDesc_ = tupleDesc;
  totalLen_ = totalLen;
  caseIndexArray_ = Int16Ptr((Int16 *)caseIndexArray);
}

Long ExSPInputOutput::pack(void *space) {
  tupleDesc_.pack(space);
  caseIndexArray_.pack(space);
  return NAVersionedObject::pack(space);
}

int ExSPInputOutput::unpack(void *base, void *reallocator) {
  if (tupleDesc_.unpack(base, reallocator)) return -1;
  if (caseIndexArray_.unpack(base)) return -1;
  return NAVersionedObject::unpack(base, reallocator);
}
