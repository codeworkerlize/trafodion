
#ifndef STMTDDLALTERMRVGROUP_H
#define STMTDDLALTERMRVGROUP_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLAlterMvRGroup.h
 * Description:  class representing Create MV group Statement parser nodes
 *
 *
 * Created:      5/06/99
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#ifndef OZY_TEST
#define OZY_TEST
#endif

#include "ElemDDLNode.h"
#include "common/NAString.h"

#include "StmtDDLNode.h"
#include "common/ComSmallDefs.h"

class StmtDDLAlterMvRGroup;

class StmtDDLAlterMvRGroup : public StmtDDLNode {
 public:
  enum alterMvGroupType {
    ADD_ACTION,
    REMOVE_ACTION

#ifdef OZY_TEST

    ,
    testRedefTimeStamp_action,
    testOpenBlownAway_action

#endif

  };

  StmtDDLAlterMvRGroup(const QualifiedName &mvRGroupName, alterMvGroupType action, ElemDDLNode *pMVList);

  virtual ~StmtDDLAlterMvRGroup();

  virtual StmtDDLAlterMvRGroup *castToStmtDDLAlterMvRGroup();

  inline const NAString getMvRGroupName() const;
  inline const QualifiedName &getMvRGroupNameAsQualifiedName() const;
  inline QualifiedName &getMvRGroupNameAsQualifiedName();

  QualifiedName &getFirstMvInList();
  ComBoolean listHasMoreMVs() const;
  QualifiedName &getNextMvInList();

  alterMvGroupType getAction() const;

  ExprNode *bindNode(BindWA *pBindWA);

  // methods for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString getText() const;

 private:
  QualifiedName mvRGroupQualName_;  // The syntax of an mv group name is
                                    // [ [ catalog-name . ] schema-name . ] mvrg-name

  ElemDDLNode *pMVList_;
  const alterMvGroupType action_;
  CollIndex listIndex_;

};  // class StmtDDLAlterMvRGroup

//----------------------------------------------------------------------------
inline const NAString StmtDDLAlterMvRGroup::getMvRGroupName() const {
  return mvRGroupQualName_.getQualifiedNameAsAnsiString();
}

inline QualifiedName &StmtDDLAlterMvRGroup::getMvRGroupNameAsQualifiedName() { return mvRGroupQualName_; }

inline const QualifiedName &StmtDDLAlterMvRGroup::getMvRGroupNameAsQualifiedName() const { return mvRGroupQualName_; }

#endif  // STMTDDLALTERMRVGROUP_H
