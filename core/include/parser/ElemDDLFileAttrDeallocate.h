#ifndef ELEMDDLFILEATTRDEALLOCATE_H
#define ELEMDDLFILEATTRDEALLOCATE_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLFileAttrDeallocate.h
 * Description:  class representing Deallocate file attribute clause
 *               in DDL statements
 *
 *
 * Created:      9/29/95
 * Language:     C++
 *
 *

 *
 *
 *****************************************************************************
 */

#include "parser/ElemDDLFileAttr.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLFileAttrDeallocate;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// definition of class ElemDDLFileAttrDeallocate
// -----------------------------------------------------------------------
class ElemDDLFileAttrDeallocate : public ElemDDLFileAttr {
 public:
  // default constructor
  ElemDDLFileAttrDeallocate();

  // virtual destructor
  virtual ~ElemDDLFileAttrDeallocate();

  // cast
  virtual ElemDDLFileAttrDeallocate *castToElemDDLFileAttrDeallocate();

  // methods for tracing
  virtual const NAString getText() const;

 private:
};  // class ElemDDLFileAttrDeallocate

// -----------------------------------------------------------------------
// definitions of inline methods for class ElemDDLFileAttrDeallocate
// -----------------------------------------------------------------------

// constructor
inline ElemDDLFileAttrDeallocate::ElemDDLFileAttrDeallocate() : ElemDDLFileAttr(ELM_FILE_ATTR_DEALLOCATE_ELEM) {}

#endif  // ELEMDDLFILEATTRDEALLOCATE_H
