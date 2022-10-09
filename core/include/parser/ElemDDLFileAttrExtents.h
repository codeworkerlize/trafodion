
#ifndef ELEMDDLFILEATTREXTENTS_H
#define ELEMDDLFILEATTREXTENTS_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLFileAttrExtents.h
 * Description:  class for Extents File Attribute (parse node) elements
 *               in Extents clause in DDL statements
 *
 * Created:      09/12/01
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "parser/ElemDDLFileAttr.h"
#include "common/ComUnits.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLFileAttrExtents;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// definition of class ElemDDLFileAttrExtents
// -----------------------------------------------------------------------
class ElemDDLFileAttrExtents : public ElemDDLFileAttr {
 public:
  enum { DEFAULT_PRI_EXTENT = COM_PRI_EXTENT };
  enum { DEFAULT_SEC_EXTENT = COM_SEC_EXTENT };

  // constructors
  ElemDDLFileAttrExtents();
  ElemDDLFileAttrExtents(int priExt);
  ElemDDLFileAttrExtents(int priExt, int secExt);

  // virtual destructor
  virtual ~ElemDDLFileAttrExtents();

  // cast
  virtual ElemDDLFileAttrExtents *castToElemDDLFileAttrExtents();

  // accessors
  inline int getPriExtents() const;
  inline int getSecExtents() const;

  // methods for tracing
  // virtual const NAString getText() const;
  // virtual const NAString displayLabel1() const;
  // virtual const NAString displayLabel2() const;
  // virtual const NAString displayLabel3() const;

 private:
  //
  // methods
  //

  NABoolean isLegalExtentValue(int extent) const;
  void initializeExtents(int priExt, int secExt);
  void initializePriExtent(int priExt);

  //
  // data members
  //

  int priExt_;
  int secExt_;

};  // class ElemDDLFileAttrExtents

//
// helpers
//

void ParSetDefaultExtents(int &priExt, int &secExt);
void ParSetDefaultPriExtent(int &priExt);

// -----------------------------------------------------------------------
// definitions of inline methods for class ElemDDLFileAttrExtents
// -----------------------------------------------------------------------

//
// accessors
//

inline int ElemDDLFileAttrExtents::getPriExtents() const { return priExt_; }

inline int ElemDDLFileAttrExtents::getSecExtents() const { return secExt_; }

#endif  // ELEMDDLFILEATTREXTENTS_H
