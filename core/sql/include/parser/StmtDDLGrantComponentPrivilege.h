#ifndef STMTDDLGRANTCOMPONENTPRIVILEGE_H
#define STMTDDLGRANTCOMPONENTPRIVILEGE_H

/* -*-C++-*-
******************************************************************************
*
* File:          StmtDDLGrantComponentPrivilege.h
* RCS:           $Id:
* Description:   class for parse node representing the
*                "grant component privilege" statement.
*
*
* Grantd:       06/24/2011
* Language:      C++
*
*
*
*
******************************************************************************
*/

#include "common/ComSmallDefs.h"
#include "parser/StmtDDLNode.h"

#ifndef SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#define SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#endif
#include "parser/SqlParserGlobals.h"

class StmtDDLGrantComponentPrivilege : public StmtDDLNode {
 public:
  // constructor
  StmtDDLGrantComponentPrivilege(const ConstStringList *pComponentPrivilegeNameList, const NAString &componentName,
                                 const NAString &userRoleName, const NABoolean isWithGrantOptionClauseSpecified,
                                 ElemDDLNode *pOptionalGrantedBy, /* optional granted by */
                                 CollHeap *heap = PARSERHEAP());

  // virtual destructor
  virtual ~StmtDDLGrantComponentPrivilege();

  // virtual safe cast-down function

  virtual StmtDDLGrantComponentPrivilege *castToStmtDDLGrantComponentPrivilege();

  // accessors

  inline const ConstStringList &getComponentPrivilegeNameList() const { return *pComponentPrivilegeNameList_; }
  inline const NAString &getComponentName() const { return componentName_; }
  inline const NAString &getUserRoleName() const { return userRoleName_; }
  inline NABoolean isWithGrantOptionSpecified() const { return isWithGrantOptionSpec_; }
  inline ElemDDLGrantee *getGrantedBy() const { return grantedBy_; };

  // for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString displayLabel2() const;
  virtual NATraceList getDetailInfo() const;
  virtual const NAString getText();

 private:
  // not-supported methods

  StmtDDLGrantComponentPrivilege();                                                   // DO NOT USE
  StmtDDLGrantComponentPrivilege(const StmtDDLGrantComponentPrivilege &);             // DO NOT USE
  StmtDDLGrantComponentPrivilege &operator=(const StmtDDLGrantComponentPrivilege &);  // DO NOT USE

  // Data members

  const ConstStringList *pComponentPrivilegeNameList_;
  NAString componentName_;
  NAString userRoleName_;
  NABoolean isWithGrantOptionSpec_;
  ElemDDLGrantee *grantedBy_;
};

#endif  // STMTDDLGRANTCOMPONENTPRIVILEGE_H
