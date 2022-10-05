

#include "common/stringBuf.h"

// Explicit function template argument lists are not supported yet
// in expression contexts. Use non-template argument instead.
NAWcharBuf *checkSpace(CollHeap *heap, int sourceLen, NAWcharBuf *&target, NABoolean addNullAtEnd) {
  int olen = sourceLen;
  if (addNullAtEnd) olen++;

  NAWcharBuf *finalTarget = target;

#ifndef MODULE_DEBUG
  // SQL/MX
  if (target) {
    if (target->getBufSize() < olen) finalTarget = NULL;

  } else {
    if (heap)
      finalTarget = new (heap) stringBuf<NAWchar>(olen, heap);
    else
      finalTarget = new stringBuf<NAWchar>(olen);
  }

#else   // MODULE_DEBUG
  if (target) {
    if (target->getBufSize() < olen) {
      finalTarget = NULL;
    }
  } else
    finalTarget = new stringBuf<NAWchar>(olen);
#endif  // MODULE_DEBUG

  // side-effect the pass-in target buffer if addNullAtEnd is TRUE.
  if (addNullAtEnd && target && target->getBufSize() > 0) {
    (target->data())[0] = NULL;
    // keep existing behavior for now // target->setStrLen(0);
  }

  return finalTarget;
}

charBuf *checkSpace(CollHeap *heap, int sourceLen, charBuf *&target, NABoolean addNullAtEnd) {
  int olen = sourceLen;
  if (addNullAtEnd) olen++;

  charBuf *finalTarget = target;

#ifndef MODULE_DEBUG
  // SQL/MX
  if (target) {
    if (target->getBufSize() < olen) finalTarget = NULL;

  } else {
    if (heap)
      finalTarget = new (heap) charBuf(olen, heap);
    else
      finalTarget = new charBuf(olen);
  }
#else   // MODULE_DEBUG
  if (target) {
    if (target->getBufSize() < olen) {
      finalTarget = NULL;
    }
  } else
    finalTarget = new charBuf(olen);
#endif  // MODULE_DEBUG

  // side-effect the pass-in target buffer if addNullAtEnd is TRUE.
  if (addNullAtEnd && target && target->getBufSize() > 0) {
    (target->data())[0] = NULL;
    // keep existing behavior for now // target->setStrLen(0);
  }

  return finalTarget;
}
