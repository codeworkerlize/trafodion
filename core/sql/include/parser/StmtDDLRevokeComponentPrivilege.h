#ifndef STMTDDLREVOKECOMPONENTPRIVILEGE_H
#define STMTDDLREVOKECOMPONENTPRIVILEGE_H

/* -*-C++-*-
******************************************************************************
*
* File:          StmtDDLRevokeComponentPrivilege.h
* RCS:           $Id:
* Description:   class for parse node representing the
*                "revoke component privilege" statement.
*
*
* Revoked:       06/24/2011
* Language:      C++
*
*
*
*
******************************************************************************
*/

#ifndef SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#define SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#endif
#include "parser/SqlParserGlobals.h"

#include "common/ComSmallDefs.h"
#include "StmtDDLNode.h"

class StmtDDLRevokeComponentPrivilege : public StmtDDLNode {
 public:
  // constructor
  StmtDDLRevokeComponentPrivilege(const ConstStringList *pComponentPrivilegeNameList, const NAString &componentName,
                                  const NAString &userRoleName, const NABoolean isGrantOptionForClauseSpecified,
                                  ElemDDLNode *pOptionalGrantedBy, /* optional granted by */
                                  CollHeap *heap = PARSERHEAP());

  // virtual destructor
  virtual ~StmtDDLRevokeComponentPrivilege();

  // virtual safe cast-down function

  virtual StmtDDLRevokeComponentPrivilege *castToStmtDDLRevokeComponentPrivilege();

  // accessors

  inline const ConstStringList &getComponentPrivilegeNameList() const { return *pComponentPrivilegeNameList_; }
  inline const NAString &getComponentName() const { return componentName_; }
  inline const NAString &getUserRoleName() const { return userRoleName_; }
  inline NABoolean isGrantOptionForSpecified() const { return isGrantOptionForSpec_; }
  inline ElemDDLGrantee *getGrantedBy() const { return grantedBy_; };

  // for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString displayLabel2() const;
  virtual NATraceList getDetailInfo() const;
  virtual const NAString getText();

 private:
  // not-supported methods

  StmtDDLRevokeComponentPrivilege();                                                    // DO NOT USE
  StmtDDLRevokeComponentPrivilege(const StmtDDLRevokeComponentPrivilege &);             // DO NOT USE
  StmtDDLRevokeComponentPrivilege &operator=(const StmtDDLRevokeComponentPrivilege &);  // DO NOT USE

  // Data members

  const ConstStringList *pComponentPrivilegeNameList_;
  NAString componentName_;
  NAString userRoleName_;
  NABoolean isGrantOptionForSpec_;
  ElemDDLGrantee *grantedBy_;
};

#endif  // STMTDDLREVOKECOMPONENTPRIVILEGE_H
