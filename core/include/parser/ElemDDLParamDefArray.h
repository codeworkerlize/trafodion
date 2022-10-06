/* -*-C++-*- */

#ifndef ELEMDDLPARAMDEFARRAY_H
#define ELEMDDLPARAMDEFARRAY_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLParamDefArray.h
 * Description:  class for an array of pointers pointing to instances of
 *               class ElemDDLParamDef
 *
 *
 * Created:      10/12/1999
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "common/Collections.h"
#ifndef SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#define SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#endif
#include "parser/ElemDDLParamDef.h"
#include "parser/ElemDDLPassThroughParamDef.h"
#include "parser/SqlParserGlobals.h"

// -----------------------------------------------------------------------
// Definition of class ElemDDLParamDefArray
// -----------------------------------------------------------------------
class ElemDDLParamDefArray : public LIST(ElemDDLParamDef *) {
 public:
  // constructor
  ElemDDLParamDefArray(CollHeap *heap = PARSERHEAP()) : LIST(ElemDDLParamDef *)(heap) {}

  // virtual destructor
  virtual ~ElemDDLParamDefArray() {}

 private:
};  // class ElemDDLParamDefArray

// -----------------------------------------------------------------------
// Definition of class ElemDDLPassThroughParamDefArray
// -----------------------------------------------------------------------
class ElemDDLPassThroughParamDefArray : public LIST(ElemDDLPassThroughParamDef *) {
 public:
  // constructor
  ElemDDLPassThroughParamDefArray(CollHeap *heap = PARSERHEAP()) : LIST(ElemDDLPassThroughParamDef *)(heap) {}

  // virtual destructor
  virtual ~ElemDDLPassThroughParamDefArray() {}

 private:
};  // class ElemDDLPassThroughParamDefArray

#endif /* ELEMDDLPARAMDEFARRAY_H */
