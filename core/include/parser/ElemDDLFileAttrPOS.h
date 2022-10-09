#ifndef ELEMDDLFILEATTRPOS_H
#define ELEMDDLFILEATTRPOS_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLFileAttrPOS.h
 * Description:  class representing POS file attribute clause
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
class ElemDDLFileAttrPOSNumPartns;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// definition of class ElemDDLFileAttrPOS
// -----------------------------------------------------------------------
class ElemDDLFileAttrPOSNumPartns : public ElemDDLFileAttr {
 public:
  // default constructor
  ElemDDLFileAttrPOSNumPartns(ComSInt32 numPartns);
  // virtual destructor
  virtual ~ElemDDLFileAttrPOSNumPartns();

  // cast
  virtual ElemDDLFileAttrPOSNumPartns *castToElemDDLFileAttrPOSNumPartns();

  const ComSInt32 getPOSNumPartns() const { return posNumPartns_; }

  // methods for tracing
  virtual const NAString getText() const;

 private:
  ComSInt32 posNumPartns_;
};  // class ElemDDLFileAttrPOSNumPartns

// -----------------------------------------------------------------------
// definitions of inline methods for class ElemDDLFileAttrPOSNumPartns
// -----------------------------------------------------------------------

// -----------------------------------------------------------------------
// definition of class ElemDDLFileAttrPOSDiskPool
// -----------------------------------------------------------------------
class ElemDDLFileAttrPOSDiskPool : public ElemDDLFileAttr {
 public:
  // default constructor
  ElemDDLFileAttrPOSDiskPool(ComSInt32 diskPool, ComSInt32 numDiskPools);
  // virtual destructor
  virtual ~ElemDDLFileAttrPOSDiskPool();

  // cast
  virtual ElemDDLFileAttrPOSDiskPool *castToElemDDLFileAttrPOSDiskPool();

  const ComSInt32 getPOSDiskPool() const { return posDiskPool_; }

  const ComSInt32 getPOSNumDiskPools() const { return posNumDiskPools_; }

  // methods for tracing
  virtual const NAString getText() const;

 private:
  ComSInt32 posDiskPool_;
  ComSInt32 posNumDiskPools_;
};  // class ElemDDLFileAttrPOSDiskPool

// -----------------------------------------------------------------------
// definition of class ElemDDLFileAttrPOSNumPartns
// -----------------------------------------------------------------------
class ElemDDLFileAttrPOSTableSize : public ElemDDLFileAttr {
 public:
  // default constructor
  ElemDDLFileAttrPOSTableSize(ComSInt32 initialTableSize, ComSInt32 maxTableSize, double numRows,
                              ComSInt32 indexLevels = -1, ComSInt64 partnEOF = -1);

  // virtual destructor
  virtual ~ElemDDLFileAttrPOSTableSize();

  // cast
  virtual ElemDDLFileAttrPOSTableSize *castToElemDDLFileAttrPOSTableSize();

  const ComSInt32 getPOSInitialTableSize() const { return posInitialTableSize_; }

  const ComSInt32 getPOSMaxTableSize() const { return posMaxTableSize_; }

  const double getNumRows() const { return numRows_; }

  const ComSInt32 getIndexLevels() const { return indexLevels_; }

  const ComSInt64 getPartnEOF() const { return partnEOF_; }

  // methods for tracing
  virtual const NAString getText() const;

 private:
  ComSInt32 posInitialTableSize_;
  ComSInt32 posMaxTableSize_;

  // estimated number of rows specified by user.
  double numRows_;

  // max of index levels of partitions that belong to this table.
  // Used for inMemory table definitions only.
  ComSInt32 indexLevels_;

  // EOF of each partition belonging to this table.
  // Used for inMemory table definitions only.
  ComSInt64 partnEOF_;

};  // class ElemDDLFileAttrPOSTableSize

// -----------------------------------------------------------------------
// definitions of inline methods for class ElemDDLFileAttrPOSTableSize
// -----------------------------------------------------------------------

// -----------------------------------------------------------------------
// definition of class ElemDDLFileAttrPOSIgnore
// -----------------------------------------------------------------------
class ElemDDLFileAttrPOSIgnore : public ElemDDLFileAttr {
 public:
  // default constructor
  ElemDDLFileAttrPOSIgnore(ComBoolean ignorePOS);

  // virtual destructor
  virtual ~ElemDDLFileAttrPOSIgnore();

  // cast
  virtual ElemDDLFileAttrPOSIgnore *castToElemDDLFileAttrPOSIgnore();

  const ComSInt32 getIgnorePOS() const { return posIgnore_; }

  // methods for tracing
  virtual const NAString getText() const;

 private:
  ComBoolean posIgnore_;
};  // class ElemDDLFileAttrPOSIgnore

#endif  // ELEMDDLFILEATTRPOS_H
