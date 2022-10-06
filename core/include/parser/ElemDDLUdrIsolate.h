
#ifndef ELEMDDLUDRISOLATE_H
#define ELEMDDLUDRISOLATE_H
/* -*-C++-*-
******************************************************************************
*
* File:         ElemDDLUdrIsolate.h
* Description:  class for Audit File Attribute (parse node) elements in
*               DDL statements
*
*
* Created:      4/21/95
* Language:     C++
*
*
*
*
******************************************************************************
*/

#include "common/ComSmallDefs.h"
#include "ElemDDLNode.h"

class ElemDDLUdrIsolate : public ElemDDLNode {
 public:
  // default constructor
  ElemDDLUdrIsolate(NABoolean theIsolate);

  // virtual destructor
  virtual ~ElemDDLUdrIsolate(void);

  // cast
  virtual ElemDDLUdrIsolate *castToElemDDLUdrIsolate(void);

  // accessor
  inline const NABoolean getIsolate(void) const { return isolate_; }

  //
  // methods for tracing
  //

  virtual NATraceList getDetailInfo() const;
  virtual const NAString getText() const;

 private:
  NABoolean isolate_;

};  // class ElemDDLUdrIsolate

#endif /* ELEMDDLUDRISOLATE_H */
