
/* -*-C++-*-
******************************************************************************
*
* File:         Inlining.cpp
* Description:  Methods for the inliningInfo
*
* Created:      6/26/01
* Language:     C++
* Status:       $State: Exp $
*
*
******************************************************************************
*/
#include "optimizer/ItemSample.h"
#include "optimizer/BindWA.h"
#include "optimizer/ColumnDesc.h"
#include "optimizer/Inlining.h"
#include "optimizer/ItemColRef.h"
#include "optimizer/ItemOther.h"
#include "optimizer/NormWA.h"
#include "optimizer/RETDesc.h"
#include "optimizer/RelUpdate.h"
#include "optimizer/SchemaDB.h"
#include "optimizer/TableDesc.h"
#include "optimizer/Triggers.h"

// These "global" column names are used for inlining.
const char InliningInfo::execIdVirtualColName_[] = "@EXECID";
const char InliningInfo::epochVirtualColName_[] = "@CURRENT_EPOCH";
const char InliningInfo::mvLogTsColName_[] = "@MVLOG_TS";
const char InliningInfo::rowtypeVirtualColName_[] = "@ROW_TYPE";
const char InliningInfo::rowcountVirtualColName_[] = "@ROW_COUNT";

InliningInfo::~InliningInfo() {
  delete forceCardinalityInfo_;
  delete triggerBindInfo_;
}

//-----------------------------------------------------------------------------
// Merge other InliningInfo object into this.
//-----------------------------------------------------------------------------

void InliningInfo::merge(InliningInfo *other) {
  if (other != NULL) {
    flags_ |= other->flags_;

    // Only one (at most) of the two merged objects can have
    // a triggerObject.
    CMPASSERT((other->triggerObject_ == NULL) || (triggerObject_ == NULL))
    if (other->triggerObject_ != NULL) {
      triggerObject_ = other->triggerObject_;
    }

    if (other->forceCardinalityInfo_ != NULL) {
      forceCardinalityInfo_ = new (CmpCommon::statementHeap()) ForceCardinalityInfo(*(other->forceCardinalityInfo_));
    }

    if (other->triggerBindInfo_ != NULL) {
      triggerBindInfo_ = new (CmpCommon::statementHeap()) TriggerBindInfo(*(other->triggerBindInfo_));
    }
  }
}

//-----------------------------------------------------------------------------
// getNewCardinality() returns the value of  cardinality * cardinalityFactor_ .
// The cardinality is taken from either cardinality_ or when cardinality_ is
// zero from oldCardinality parameter.
//-----------------------------------------------------------------------------

CostScalar InliningInfo::getNewCardinality(CostScalar oldCardinality) const {
  CMPASSERT(forceCardinalityInfo_);

  if (forceCardinalityInfo_->cardinality_ != 0) {
    return MIN_ONE_CS(forceCardinalityInfo_->cardinality_ * forceCardinalityInfo_->cardinalityFactor_);
  }

  return MIN_ONE_CS(oldCardinality * forceCardinalityInfo_->cardinalityFactor_);
}

//-----------------------------------------------------------------------------
//-----------------------------------------------------------------------------

void InliningInfo::BuildForceCardinalityInfo(double cardinalityFactor, Cardinality cardinality, CollHeap *heap) {
  forceCardinalityInfo_ = new (heap) ForceCardinalityInfo();

  forceCardinalityInfo_->cardinalityFactor_ = cardinalityFactor;
  forceCardinalityInfo_->cardinality_ = cardinality;
}

//-----------------------------------------------------------------------------
// Constructs the TriggerBindInfo object, and stores the Execute-ID of the
// current query and the current IUD No. for further use at triggers'
// transformation time.
//-----------------------------------------------------------------------------

void InliningInfo::buildTriggerBindInfo(BindWA *bindWA, RETDesc *RETDesc, CollHeap *heap) {
  triggerBindInfo_ = new (heap) TriggerBindInfo(heap);

  // store the Execute-ID of the current query
  triggerBindInfo_->setExecuteId();
  triggerBindInfo_->getExecuteId()->bindNode(bindWA);

  // store the current backbone IUD No.
  triggerBindInfo_->setBackboneIudNum(bindWA->getUniqueIudNum());

  // Copy all user and system columns from the given RETDesc for use during
  // sub-trees constructions in triggers' transformation phase.

  ColumnDescList *copiedList = new (heap) ColumnDescList(heap);
  CollIndex i;
  for (i = 0; i < RETDesc->getColumnList()->entries(); i++) {
    ColumnDesc *colDesc = RETDesc->getColumnList()->at(i);
    copiedList->insert(new (heap) ColumnDesc(*colDesc));
  }

  for (i = 0; i < RETDesc->getSystemColumnList()->entries(); i++) {
    ColumnDesc *colDesc = RETDesc->getSystemColumnList()->at(i);
    copiedList->insert(new (heap) ColumnDesc(*colDesc));
  }
  triggerBindInfo_->setIudColumnList(copiedList);
}

//----------------------------------------------------------------------------
// copy Ctor of ForceCardinalityInfo object
//----------------------------------------------------------------------------

ForceCardinalityInfo::ForceCardinalityInfo(const ForceCardinalityInfo &other) {
  cardinality_ = other.cardinality_;

  cardinalityFactor_ = other.cardinalityFactor_;
}

//-----------------------------------------------------------------------------
// copy Ctor of TriggerBindInfo object
//-----------------------------------------------------------------------------

TriggerBindInfo::TriggerBindInfo(const TriggerBindInfo &other) {
  heap_ = other.heap_;
  if (other.exeId_) {
    exeId_ = new (heap_) UniqueExecuteId();

    // Would like to bind this new copy, but we don't have a BindWA
    // handy.  Since this is an "execute-once" function, and so it
    // should always bind to the same ValueId, simply copy the ValueId
    // from the original.  (Alternatively, could we safely share the
    // original ItemExpr and avoid making a copy?)
    //
    exeId_->setValueId(other.exeId_->getValueId());
  }
  backboneIudNum_ = other.backboneIudNum_;

  origIudColumnList_ = other.origIudColumnList_;
}

//-----------------------------------------------------------------------------
// Normilizes all the data members that need to be normalized. This method is
// called from GenericUpdate::rewriteNode in NormRelExpr.cpp
//-----------------------------------------------------------------------------

void TriggerBindInfo::normalizeMembers(NormWA &normWA) {
  // Normalize the execute-id
  exeId_->normalizeNode(normWA);
}
