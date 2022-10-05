
#ifndef ELEMDDLFILEATTRALLOCATE_H
#define ELEMDDLFILEATTRALLOCATE_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLFileAttrAllocate.h
 * Description:  class for File Disk Space (parse node) elements in
 *               Allocate clause in DDL statements
 *
 *
 * Created:      4/21/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "common/ComSmallDefs.h"
#include "ElemDDLFileAttr.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLFileAttrAllocate;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// definition of class ElemDDLFileAttrAllocate
// -----------------------------------------------------------------------
class ElemDDLFileAttrAllocate : public ElemDDLFileAttr {
 public:
  enum { DEFAULT_EXTENTS_TO_ALLOCATE = 0 };

  // constructors
  ElemDDLFileAttrAllocate(ComSInt16 size);

  // virtual destructor
  virtual ~ElemDDLFileAttrAllocate();

  // cast
  virtual ElemDDLFileAttrAllocate *castToElemDDLFileAttrAllocate();

  // accessors
  inline const ComSInt16 getExtentsToAllocate() const;

  // member functions for tracing
  virtual const NAString getText() const;
  virtual const NAString displayLabel1() const;

 private:
  //
  // methods
  //

  NABoolean isLegalAllocateValue(ComSInt16 extentsToAllocate) const;
  void initializeExtentsToAllocate(ComSInt16 extentsToAllocate);

  //
  // data members
  //
  short extentsToAllocate_;

};  // class ElemDDLFileAttrAllocate

// -----------------------------------------------------------------------
// definition of class ElemDDLFileAttrAllocate
// -----------------------------------------------------------------------

//
// accessors
//

inline const ComSInt16 ElemDDLFileAttrAllocate::getExtentsToAllocate() const { return extentsToAllocate_; }

#endif  // ELEMDDLFILEATTRALLOCATE_H
