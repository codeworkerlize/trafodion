
#ifndef ELEMDDL_CREATE_MV_ONE_ATTRIBUTE_TABLE_LIST_H
#define ELEMDDL_CREATE_MV_ONE_ATTRIBUTE_TABLE_LIST_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLCreateMVOneAttributeTableList.h
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

#include "ElemDDLNode.h"
#include "common/NAString.h"

#include "common/ComSmallDefs.h"
#include "optimizer/ObjectNames.h"
#include "common/ComASSERT.h"
#include "parser/ElemDDLQualName.h"

// ElemDDLCreateMVChangesClause

//----------------------------------------------------------------------------
class ElemDDLCreateMVOneAttributeTableList : public ElemDDLNode {
 public:
  /*
        enum listType {	IGNORE_CHNAGES_ON,
                                        INSERTONLY_ARE,
                                        UNKNOWN_CHANGES			};
*/
  ElemDDLCreateMVOneAttributeTableList(ComMVSUsedTableAttribute type, ElemDDLNode *pTableList);

  virtual ~ElemDDLCreateMVOneAttributeTableList() {}

  virtual ElemDDLCreateMVOneAttributeTableList *castToElemDDLCreateMVOneAttributeTableList() { return this; }

  QualifiedName &getFirstTableInList();
  ComBoolean listHasMoreTables() const;
  QualifiedName &getNextTableInList();

  ComMVSUsedTableAttribute getType() const;
  ElemDDLNode *getTableList() { return pTableList_; }

  // methods for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString getText() const;

 private:
  const ComMVSUsedTableAttribute type_;
  ElemDDLNode *pTableList_;
  CollIndex listIndex_;

};  // class ElemDDLCreateMVOneAttributeTableList

#endif  // ELEMDDL_CREATE_MV_ONE_ATTRIBUTE_TABLE_LIST_H
