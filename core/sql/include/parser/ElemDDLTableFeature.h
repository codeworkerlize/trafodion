
#ifndef ELEMDDLTABLEFEATURE_H
#define ELEMDDLTABLEFEATURE_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLTableFeature.h
 * Description:  Describes table options: [NOT] DROPPABLE & INSERT_ONLY
 *
 *
 * Created:      04/02/2012
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "ElemDDLNode.h"

class ElemDDLTableFeature : public ElemDDLNode {
 public:
  ElemDDLTableFeature(ComTableFeature tableFeature)
      : ElemDDLNode(ELM_TABLE_FEATURE_ELEM), tableFeature_(tableFeature) {}

  // virtual destructor
  virtual ~ElemDDLTableFeature() {}

  // cast
  virtual ElemDDLTableFeature *castToElemDDLTableFeature() { return this; }
  virtual ComTableFeature getTableFeature() const { return tableFeature_; }
  const NABoolean isDroppable() const {
    return ((tableFeature_ == COM_DROPPABLE) || (tableFeature_ == COM_DROPPABLE_INSERT_ONLY));
  }

  const NABoolean isInsertOnly() const {
    return ((tableFeature_ == COM_DROPPABLE_INSERT_ONLY) || (tableFeature_ == COM_NOT_DROPPABLE_INSERT_ONLY));
  }

  // methods for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString getText() const;

 private:
  ComTableFeature tableFeature_;

};  // class ElemDDLTableFeature

#endif  // ELEMDDLTABLEFEATURE_H
