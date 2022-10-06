#ifndef STMTDDLCREATEROLE_H
#define STMTDDLCREATEROLE_H

/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLCreateRole.h
 * Description:  Class for parse nodes representing create and drop role
 *               statements.  Patterned after StmtDDLRegisterUser.h.
 *
 * Created:      April 2011
 * Language:     C++
 *
 *****************************************************************************
 */

#include "ElemDDLLocation.h"
#include "common/ComLocationNames.h"
#include "common/ComSmallDefs.h"
#include "parser/StmtDDLNode.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLCreateRole;
class StmtDDLCreateRoleArray;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None

// -----------------------------------------------------------------------
// Create and drop role statements
// -----------------------------------------------------------------------
class StmtDDLCreateRole : public StmtDDLNode {
 public:
  // constructors
  // create role
  StmtDDLCreateRole(const NAString &roleName, ElemDDLNode *pOwner, NABoolean adminRole, NAString *tenantName,
                    CollHeap *heap);
  // drop role; the only drop behavior supported by ANSI is restrict
  StmtDDLCreateRole(const NAString &roleName, CollHeap *heap);

  // virtual destructor
  virtual ~StmtDDLCreateRole();

  // cast
  virtual StmtDDLCreateRole *castToStmtDDLCreateRole();

  // for binding
  ExprNode *bindNode(BindWA *bindWAPtr);

  // accessors

  inline const NAString &getRoleName() const;
  inline const NAString &getTenantName() const;

  // returns TRUE if "CREATE ROLE" command, returns FALSE if "DROP ROLE" command
  inline const NABoolean isCreateRole() const;
  // returns TRUE if WITH ADMIN CURRENT_USER or no WITH ADMIN option was specified
  inline const NABoolean isCurrentUserSpecified() const;
  // getOwner will be NULL if isCurrentUserSpecified is TRUE
  inline const ElemDDLGrantee *getOwner() const;
  // returns TRUE if this is a tenant admin role
  inline const NABoolean isAdminRole() const;

  // for tracing

  virtual const NAString displayLabel1() const;
  virtual const NAString getText() const;

 private:
  // ---------------------------------------------------------------------
  // private data members
  // ---------------------------------------------------------------------

  NAString roleName_;
  NABoolean isCreateRole_;
  NABoolean isCurrentUserSpecified_;
  ElemDDLGrantee *pOwner_;
  NABoolean adminRole_;
  NAString tenantName_;

};  // class StmtDDLCreateRole

// -----------------------------------------------------------------------
// definitions of inline methods for class StmtDDLCreateRole
// -----------------------------------------------------------------------

//
// accessors
//

inline const NAString &StmtDDLCreateRole::getRoleName() const { return roleName_; }

inline const NAString &StmtDDLCreateRole::getTenantName() const { return tenantName_; }

inline const NABoolean StmtDDLCreateRole::isCreateRole() const { return isCreateRole_; }

inline const NABoolean StmtDDLCreateRole::isCurrentUserSpecified() const { return (pOwner_ == NULL); }

inline const ElemDDLGrantee *StmtDDLCreateRole::getOwner() const { return pOwner_; }

inline const NABoolean StmtDDLCreateRole::isAdminRole() const { return (adminRole_); }

// -----------------------------------------------------------------------
// Definition of class StmtDDLCreateRoleArray
// -----------------------------------------------------------------------
class StmtDDLCreateRoleArray : public LIST(StmtDDLCreateRole *) {
 public:
  // constructor
  StmtDDLCreateRoleArray(CollHeap *heap) : LIST(StmtDDLCreateRole *)(heap) {}

  // virtual destructor
  virtual ~StmtDDLCreateRoleArray();

 private:
};  // class StmtDDLCreateRoleArray

#endif  // STMTDDLCREATEROLE_H
