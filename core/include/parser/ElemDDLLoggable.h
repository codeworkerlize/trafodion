
#ifndef ELEMDDLLOGGABLE_H
#define ELEMDDLLOGGABLE_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLLoggable.h
 * Description:  column LOGGABLE attribute
 *
 *
 * Created:      05/18/2000
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "parser/ElemDDLNode.h"

class ElemDDLLoggable : public ElemDDLNode {
 public:
  ElemDDLLoggable(NABoolean loggableVal) : ElemDDLNode(ELM_LOGGABLE), loggable_(loggableVal) {}

  // virtual destructor
  virtual ~ElemDDLLoggable() {}

  // cast
  virtual ElemDDLLoggable *castToElemDDLLoggable() { return this; }
  virtual NABoolean getIsLoggable() const { return loggable_; }

  // methods for tracing
  //	virtual const NAString displayLabel2() const;
  //	virtual const NAString getText() const;

 private:
  NABoolean loggable_;

};  // class ElemDDLElemDDLLoggableAttribute

#endif  // ELEMDDLLOGGABLE_H
