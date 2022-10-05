#ifndef ELEMDDLCOLNAMEARRAY_H
#define ELEMDDLCOLNAMEARRAY_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLColNameArray.h
 * Description:  class for an array of pointers pointing to instances of
 *               class ElemDDLColName
 *
 *
 * Created:      5/26/95
 * Language:     C++
 *
 *

 *
 *
 *****************************************************************************
 */

#ifndef SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#define SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#endif

#include "common/Collections.h"
#include "parser/ElemDDLColName.h"
#include "parser/SqlParserGlobals.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLColNameArray;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
class NAMemory;

// -----------------------------------------------------------------------
// Definition of class ElemDDLColNameArray
// -----------------------------------------------------------------------
class ElemDDLColNameArray : public LIST(ElemDDLColName *) {
 public:
  // constructor
  ElemDDLColNameArray(NAMemory *heap = PARSERHEAP());

  // virtual destructor
  virtual ~ElemDDLColNameArray();

 private:
};  // class ElemDDLColNameArray

#endif /* ELEMDDLCOLNAMEARRAY_H */
