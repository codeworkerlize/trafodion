/* -*-C++-*-
 *****************************************************************************
 *
 * File:         <file>
 * RCS:          $Id: NewDel.cpp,v 1.1 2007/10/09 19:40:37  Exp $
 * Description:
 *
 *
 * Created:      7/10/95
 * Modified:     $ $Date: 2007/10/09 19:40:37 $ (GMT)
 * Language:     C++
 * Status:       $State: Exp $
 *
 *

 *
 *
 *****************************************************************************
 */

// -----------------------------------------------------------------------
// Change history:
//
// $Log: NewDel.cpp,v $
// Revision 1.1  2007/10/09 19:40:37
// Add the NeoQuest version of SQL to support closer collaboration during dev
//
// Revision 1.3  1998/06/16 18:02:55
// (de)allocateSpace -> (de)allocateMemory
//
// Revision 1.2  1997/04/23 00:31:54
// Merge of MDAM/Costing changes into SDK thread
//
// Revision 1.1.1.1.2.1  1997/04/11 23:25:30
// Checking in partially resolved conflicts from merge with MDAM/Costing
// thread. Final fixes, if needed, will follow later.
//
// Revision 1.3.4.1  1997/04/10 18:33:59
// *** empty log message ***
//
// Revision 1.1.1.1  1997/03/28 01:40:16
// These are the source files from SourceSafe.
//
//
// Revision 1.3  1997/01/16 00:31:04
// new memory managment (selectable/flat segments on NSK)
// fixed bug in hybrid hash join
// new hash function
//
// Revision 1.2  1996/10/01 23:29:59
// Take away the global new/delete overloading.
//
// Revision 1.1  1996/06/19 15:56:13
// Initial revision
//
//
// -----------------------------------------------------------------------

// -----------------------------------------------------------------------
// This file contains the implementation of
// PushGlobalMemory, PopGlobalMemory and CurrentGlobalMemory routines.
// -----------------------------------------------------------------------

#include "sqlcomp/NewDel.h"

#include <stdlib.h>

#include <iostream>

#include "common/NAHeap.h"
#include "export/HeapLog.h"

#if (defined(_DEBUG) || defined(NSK_MEMDEBUG))
//
// NOTE: The variable globalCmpNewHandler ends up being used only by
//       tdm_arkcmp, not sqlci, so it is not a concern for sqlci.
//
THREAD_P PNH globalCmpNewHandler = 0;

const int DEFAULT_SYSTEM_HEAP_ID = 999;

void *operator new(size_t size) {
  void *p = malloc(size);
  if (!p && globalCmpNewHandler) {
    globalCmpNewHandler();
  }
  HEAPLOG_ADD_ENTRY(p, size, DEFAULT_SYSTEM_HEAP_ID, "Default System Heap");
  memset(p, 0, size);
  return p;
}

void operator delete(void *ptr) {
  HEAPLOG_DELETE_ENTRY(ptr, DEFAULT_SYSTEM_HEAP_ID);
  free(ptr);
}

void *operator new[](size_t size) {
  if (size == 0) {
    // if we come to here, that means someone has code like this:
    // A* a = new A[0];
    // we should really do assert here ...
    //
    return 0;
  }

  void *p = malloc(size);
  if (!p && globalCmpNewHandler) {
    globalCmpNewHandler();
  }
  HEAPLOG_ADD_ENTRY(p, size, DEFAULT_SYSTEM_HEAP_ID, "Default System Heap");
  // printf("M\tGlobalND\t%10x\t%10x\t%10x\n", p, size, (char *) p + size);
  memset(p, 0, size);
  return p;  // malloc(size);
}
void operator delete[](void *ptr) {
  HEAPLOG_DELETE_ENTRY(ptr, DEFAULT_SYSTEM_HEAP_ID);
  free(ptr);
  // printf("F\tGlobalND\t%10x\n", ptr);
}

PNH CmpSetNewHandler(PNH handler) {
  PNH old = globalCmpNewHandler;
  globalCmpNewHandler = handler;
  return old;
}

// Code not used
PNH CmpGetNewHandler() { return globalCmpNewHandler; }
#endif
