#ifndef STMTDDLCREATECOMPONENTPRIVILEGE_H
#define STMTDDLCREATECOMPONENTPRIVILEGE_H

/* -*-C++-*-
******************************************************************************
*
* File:          StmtDDLCreateComponentPrivilege.h
* RCS:           $Id:
* Description:   class for parse node representing the
*                "create component privilege" statement.
*
*
* Created:       06/24/2011
* Language:      C++
*
*
*
*
******************************************************************************
*/

#include "common/ComSmallDefs.h"
#include "StmtDDLNode.h"

class StmtDDLCreateComponentPrivilege : public StmtDDLNode

{
 public:
  // constructor
  StmtDDLCreateComponentPrivilege(const NAString &componentPrivilegeName,
                                  const NAString &componentPrivilegeAbbreviation, const NAString &componentName,
                                  const NABoolean isSystem, const NAString &componentPrivilegeDetailInformation,
                                  CollHeap *heap = PARSERHEAP());

  // Virtual Destructor
  virtual ~StmtDDLCreateComponentPrivilege();

  // Cast

  virtual StmtDDLCreateComponentPrivilege *castToStmtDDLCreateComponentPrivilege();

  // accessors

  inline const NAString &getComponentPrivilegeName() const { return componentPrivilegeName_; }
  inline const NAString &getComponentPrivilegeAbbreviation() const { return componentPrivilegeAbbreviation_; }
  inline const NAString &getComponentName() const { return componentName_; }
  inline const NAString &getComponentPrivilegeDetailInformation() const { return componentPrivilegeDetailInformation_; }
  inline NABoolean isSystem() const { return isSystem_; }

  // for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString displayLabel2() const;
  virtual const NAString getText() const;

 private:
  // not-supported methods

  StmtDDLCreateComponentPrivilege();                                                    // DO NOT USE
  StmtDDLCreateComponentPrivilege(const StmtDDLCreateComponentPrivilege &);             // DO NOT USE
  StmtDDLCreateComponentPrivilege &operator=(const StmtDDLCreateComponentPrivilege &);  // DO NOT USE

  // Data members

  NAString componentPrivilegeName_;
  NAString componentPrivilegeAbbreviation_;
  NAString componentName_;
  NABoolean isSystem_;
  NAString componentPrivilegeDetailInformation_;
};

#endif  // STMTDDLCREATECOMPONENTPRIVILEGE_H
