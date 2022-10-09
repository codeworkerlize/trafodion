
#ifndef ELEMDDLFILEATTRBLOCKSIZE_H
#define ELEMDDLFILEATTRBLOCKSIZE_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLFileAttrBlockSize.h
 * Description:  class for File Block Size (parse node) elements in
 *               DDL statements
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

#include "parser/ElemDDLFileAttr.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLFileAttrBlockSize;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// definition of class ElemDDLFileAttrBlockSize
// -----------------------------------------------------------------------
class ElemDDLFileAttrBlockSize : public ElemDDLFileAttr {
 public:
  enum { DEFAULT_BLOCK_SIZE = 32768 };

  // default constructor
  ElemDDLFileAttrBlockSize(int blockSizeInBytes = DEFAULT_BLOCK_SIZE);

  // virtual destructor
  virtual ~ElemDDLFileAttrBlockSize();

  // cast
  virtual ElemDDLFileAttrBlockSize *castToElemDDLFileAttrBlockSize();

  // accessors
  inline int getBlockSize() const;

  // member functions for tracing
  virtual const NAString getText() const;
  virtual const NAString displayLabel1() const;

  // method for building text
  virtual NAString getSyntax() const;

 private:
  //
  // method(s)
  //

  NABoolean isLegalBlockSizeValue(int blockSize) const;

  //
  // data member(s)
  //

  int blockSizeInBytes_;

};  // class ElemDDLFileAttrBlockSize

// -----------------------------------------------------------------------
// definitions of inline methods for class ElemDDLFileAttrBlockSize
// -----------------------------------------------------------------------

inline int ElemDDLFileAttrBlockSize::getBlockSize() const { return blockSizeInBytes_; }

#endif  // ELEMDDLFILEATTRBLOCKSIZE_H
