
#ifndef ELEMDDLFILEATTRMAXSIZE_H
#define ELEMDDLFILEATTRMAXSIZE_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLFileAttrMaxSize.h
 * Description:  class for MaxSize File Attribute (parse node) elements
 *               in MaxSize clause in DDL statements
 *
 * Created:      4/20/95
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
class ElemDDLFileAttrMaxSize;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// definition of class ElemDDLFileAttrMaxSize
// -----------------------------------------------------------------------
class ElemDDLFileAttrMaxSize : public ElemDDLFileAttr {
 public:
  enum { DEFAULT_MAX_SIZE_IN_BYTES = COM_MAX_PART_SIZE_IN_BYTES };
  enum { DEFAULT_MAX_SIZE_UNIT = COM_BYTES };

  // constructors
  ElemDDLFileAttrMaxSize();
  ElemDDLFileAttrMaxSize(int maxSize);
  ElemDDLFileAttrMaxSize(int maxSize, ComUnits maxSizeUnit);

  // virtual destructor
  virtual ~ElemDDLFileAttrMaxSize();

  // cast
  virtual ElemDDLFileAttrMaxSize *castToElemDDLFileAttrMaxSize();

  // accessors
  inline int getMaxSize() const;
  inline ComUnits getMaxSizeUnit() const;
  NAString getMaxSizeUnitAsNAString() const;
  inline NABoolean isUnbounded() const;

  // methods for tracing
  virtual const NAString getText() const;
  virtual const NAString displayLabel1() const;
  virtual const NAString displayLabel2() const;
  virtual const NAString displayLabel3() const;

  // method for building text
  virtual NAString getSyntax() const;

 private:
  //
  // methods
  //

  NABoolean isLegalMaxSizeValue(int maxSize) const;
  void initializeMaxSize(int maxSize);

  //
  // data members
  //

  NABoolean isUnbounded_;
  int maxSize_;
  ComUnits maxSizeUnit_;

};  // class ElemDDLFileAttrMaxSize

//
// helpers
//

void ParSetDefaultMaxSize(int &maxSize, ComUnits &maxSizeUnit);

// -----------------------------------------------------------------------
// definitions of inline methods for class ElemDDLFileAttrMaxSize
// -----------------------------------------------------------------------

//
// accessors
//

inline int ElemDDLFileAttrMaxSize::getMaxSize() const { return maxSize_; }

inline ComUnits ElemDDLFileAttrMaxSize::getMaxSizeUnit() const { return maxSizeUnit_; }

inline NABoolean ElemDDLFileAttrMaxSize::isUnbounded() const { return isUnbounded_; }

#endif  // ELEMDDLFILEATTRMAXSIZE_H
