
#ifndef ELEMDDLFILEATTRMAXEXTENTS_H
#define ELEMDDLFILEATTRMAXEXTENTS_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLFileAttrMaxExtents.h
 * Description:  class for MaxExtents File Attribute (parse node) elements
 *               in MaxExtents clause in DDL statements
 *
 * Created:      09/12/01
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "common/ComUnits.h"
#include "ElemDDLFileAttr.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLFileAttrMaxExtents;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// definition of class ElemDDLFileAttrExtents
// -----------------------------------------------------------------------
class ElemDDLFileAttrMaxExtents : public ElemDDLFileAttr {
 public:
  enum { DEFAULT_MAX_EXTENT = COM_MAX_EXTENT };

  // constructors
  ElemDDLFileAttrMaxExtents();
  ElemDDLFileAttrMaxExtents(int maxExtent);

  // virtual destructor
  virtual ~ElemDDLFileAttrMaxExtents();

  // cast
  virtual ElemDDLFileAttrMaxExtents *castToElemDDLFileAttrMaxExtents();

  // accessors
  inline int getMaxExtents() const;

  // methods for tracing
  //  virtual const NAString getText() const;
  //  virtual const NAString displayLabel1() const;
  //  virtual const NAString displayLabel2() const;
  //  virtual const NAString displayLabel3() const;

 private:
  //
  // methods
  //

  NABoolean isLegalMaxExtentValue(int maxExtent) const;
  void initializeMaxExtents(int maxExtent);

  //
  // data members
  //

  int maxExt_;

};  // class ElemDDLFileAttrMaxExtents

//
// helpers
//

void ParSetDefaultMaxExtents(int &maxExt);

// -----------------------------------------------------------------------
// definitions of inline methods for class ElemDDLFileAttrMaxExtents
// -----------------------------------------------------------------------

//
// accessors
//

inline int ElemDDLFileAttrMaxExtents::getMaxExtents() const { return maxExt_; }

#endif  // ELEMDDLFILEATTRMAXEXTENTS_H
