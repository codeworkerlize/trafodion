
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLCreateMVOneAttributeTableList.cpp
 * Description:  class representing Create MV changes clause
 *
 *
 * Created:      11/27/99
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "ElemDDLCreateMVOneAttributeTableList.h"

ElemDDLCreateMVOneAttributeTableList::ElemDDLCreateMVOneAttributeTableList(ComMVSUsedTableAttribute type,
                                                                           ElemDDLNode *pTableList)
    : ElemDDLNode(ELM_CREATE_MV_ONE_ATTRIBUTE_TABLE_LIST), type_(type), pTableList_(pTableList) {}

QualifiedName &ElemDDLCreateMVOneAttributeTableList::getFirstTableInList() {
  listIndex_ = 0;
  return getNextTableInList();
}
ComBoolean ElemDDLCreateMVOneAttributeTableList::listHasMoreTables() const {
  return (listIndex_ < pTableList_->entries()) ? TRUE : FALSE;
}

QualifiedName &ElemDDLCreateMVOneAttributeTableList::getNextTableInList() {
  ComASSERT(TRUE == listHasMoreTables());

  return ((*pTableList_)[listIndex_++]->castToElemDDLQualName())->getQualifiedName();
}

ComMVSUsedTableAttribute ElemDDLCreateMVOneAttributeTableList::getType() const { return type_; }

// methods for tracing
const NAString ElemDDLCreateMVOneAttributeTableList::displayLabel1() const {
  switch (type_) {
    case COM_IGNORE_CHANGES:
      return NAString("IGNORE CAHNGES ON");
    case COM_INSERT_ONLY:
      return NAString("INSERT ONLY ARE");
  }

  return "UNKNOWN TYPE";
}

const NAString ElemDDLCreateMVOneAttributeTableList::getText() const { return "ElemDDLCreateMVOneAttributeTableList"; }
