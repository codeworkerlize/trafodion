
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         HeapID.h
 * Description:  This file contains the declaration of HeapID class to
 *               associate heap and heaplog.
 *
 * Created:      3/1/99
 * Language:     C++
 *
 *
 *
 ****************************************************************************
 */

#ifndef HEAPID__H
#define HEAPID__H

#include "common/Platform.h"

// -----------------------------------------------------------------------
// NA_DEBUG_HEAPLOG indicates this is a debug build on NT and
// heaplog is importable.  However, it can be enabled for NSK
// platform later.
// -----------------------------------------------------------------------
#ifndef NA_DEBUG_HEAPLOG
#if (defined(_DEBUG) || defined(NSK_MEMDEBUG)) && !defined(__NOIMPORT_HEAPL)
#define NA_DEBUG_HEAPLOG
#endif
#endif

// -----------------------------------------------------------------------
#ifdef NA_DEBUG_HEAPLOG
// -----------------------------------------------------------------------

class HeapID {
 public:
  HeapID();

  ~HeapID();

// -----------------------------------------------------------------------
#else
// -----------------------------------------------------------------------

class HeapID {
 public:
  HeapID() : heapNum(-1) {}

  ~HeapID() {}

#endif
  int heapNum;
};

#endif
