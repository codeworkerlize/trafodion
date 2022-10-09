
#ifndef ELEMDDLHBASEOPTIONS_H
#define ELEMDDLHBASEOPTIONS_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLHbaseoptions.h
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

#include "parser/ElemDDLNode.h"
#include "exp/ExpHbaseDefs.h"

class ElemDDLHbaseOptions : public ElemDDLNode {
 public:
  ElemDDLHbaseOptions(NAList<HbaseCreateOption *> *hbaseOptions, CollHeap *heap)
      : ElemDDLNode(ELM_HBASE_OPTIONS_ELEM), hbaseOptions_(heap) {
    for (CollIndex i = 0; i < hbaseOptions->entries(); i++) {
      HbaseCreateOption *hbo = new (heap) HbaseCreateOption(((*hbaseOptions)[i])->key(), ((*hbaseOptions)[i])->val());

      hbaseOptions_.insert(hbo);
    }
  }

  // virtual destructor
  virtual ~ElemDDLHbaseOptions() {}

  // cast
  virtual ElemDDLHbaseOptions *castToElemDDLHbaseOptions() { return this; }

  NAList<HbaseCreateOption *> &getHbaseOptions() { return hbaseOptions_; }

  // methods for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString getText() const;

 private:
  NAList<HbaseCreateOption *> hbaseOptions_;

};  // class ElemDDLHbaseOptions

#endif  // ELEMDDLHBASEOPTIONS_H
