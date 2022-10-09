/* -*-C++-*- */

#ifndef ELEMDDLUDFPARALLELISM_H
#define ELEMDDLUDFPARALLELISM_H
/* -*-C++-*-
******************************************************************************
*
* File:         ElemDDLUdfParallelism.h
* Description:  class for Udf Parallelism Attribute (parse node) elements in
*               DDL statements
*
*
* Created:      7/14/09
* Language:     C++
*
*
*
*
******************************************************************************
*/

#include "parser/ElemDDLNode.h"

class ElemDDLUdfParallelism : public ElemDDLNode {
 public:
  // default constructor
  ElemDDLUdfParallelism(ComRoutineParallelism parallelismChoice);

  // virtual destructor
  virtual ~ElemDDLUdfParallelism(void);

  // cast
  virtual ElemDDLUdfParallelism *castToElemDDLUdfParallelism(void);

  // accessors
  inline const ComRoutineParallelism getParallelism(void) const { return parallelism_; }

  inline NABoolean getCanBeParallel(void) const { return parallelism_ NEQ COM_ROUTINE_NO_PARALLELISM; }

  //
  // methods for tracing
  //

  virtual NATraceList getDetailInfo() const;
  virtual const NAString getText() const;

 private:
  ComRoutineParallelism parallelism_;

};  // class ElemDDLUdfParallelism

#endif /* ELEMDDLUDFPARALLELISM_H */
