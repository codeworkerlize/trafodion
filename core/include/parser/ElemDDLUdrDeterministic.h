
#ifndef ELEMDDLUDRDETERMINISTIC_H
#define ELEMDDLUDRDETERMINISTIC_H
/* -*-C++-*-
******************************************************************************
*
* File:         ElemDDLUdrDeterministic.h
* Description:  class for UDR Deterministic (parse node) elements in
*               DDL statements
*
*
* Created:      10/08/1999
* Language:     C++
*
*
*
*
******************************************************************************
*/

#include "ElemDDLNode.h"
#include "common/ComSmallDefs.h"

class ElemDDLUdrDeterministic : public ElemDDLNode {
 public:
  // default constructor
  ElemDDLUdrDeterministic(NABoolean theDeterministic);

  // virtual destructor
  virtual ~ElemDDLUdrDeterministic(void);

  // cast
  virtual ElemDDLUdrDeterministic *castToElemDDLUdrDeterministic(void);

  // accessor
  inline const NABoolean getDeterministic(void) const { return deterministic_; }

  //
  // methods for tracing
  //

  virtual NATraceList getDetailInfo() const;
  virtual const NAString getText() const;

 private:
  NABoolean deterministic_;

};  // class ElemDDLUdrDeterministic

#endif /* ELEMDDLUDRDETERMINISTIC_H */
