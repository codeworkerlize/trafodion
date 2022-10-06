/* -*-C++-*-
****************************************************************************
*
* File:         HeapID.cpp
*
* Description:  Assign HeapID for a defined heap.
*
* Created:      3/1/99
* Language:     C++
*
*

*
****************************************************************************
*/

#include "export/HeapLog.h"

#ifdef NA_DEBUG_HEAPLOG
// -----------------------------------------------------------------------
// Constructor and destructor.
// -----------------------------------------------------------------------
HeapID::HeapID() {
  // Assign a unique ID.
  heapNum = HeapLogRoot::assignHeapNum();
}

HeapID::~HeapID() {
  if (heapNum >= 0 && HeapLogRoot::log != NULL) HeapLogRoot::deleteLogSegment(heapNum, TRUE);
}

#endif
