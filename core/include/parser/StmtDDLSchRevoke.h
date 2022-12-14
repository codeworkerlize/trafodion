#ifndef STMTDDLSCHREVOKE_H
#define STMTDDLSCHREVOKE_H
/* -*-C++-*-
 *****************************************************************************


 *
 * File:         StmtDDLSchRevoke.h
 * Description:  class for parse nodes representing Revoke DDL statements
 *
 * Created:      03/15/2007
 * Language:     C++
 *
 *
 *****************************************************************************
 */

#include "parser/ElemDDLGranteeArray.h"
#include "parser/ElemDDLPrivActions.h"
#include "common/ComSmallDefs.h"
#include "parser/StmtDDLNode.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLSchRevoke;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None

// -----------------------------------------------------------------------
// definition of class StmtDDLSchRevoke
// -----------------------------------------------------------------------
class StmtDDLSchRevoke : public StmtDDLNode {
 public:
  // constructor
  StmtDDLSchRevoke(NABoolean isGrantOptionFor, ElemDDLNode *pPrivileges, const ElemDDLSchemaName &aSchemaNameParseNode,
                   ElemDDLNode *pGranteeList, ComDropBehavior dropBehavior, ElemDDLNode *pByGrantorOption,
                   CollHeap *heap = PARSERHEAP());

  // virtual destructor
  virtual ~StmtDDLSchRevoke();

  // cast
  virtual StmtDDLSchRevoke *castToStmtDDLSchRevoke();

  //
  // accessors
  //

  virtual int getArity() const;
  virtual ExprNode *getChild(int index);

  inline ComDropBehavior getDropBehavior() const;

  // returns the array of pointers pointing to parse
  // nodes representing grantees.  This array contains
  // at least one element.
  inline const ElemDDLGranteeArray &getGranteeArray() const;
  inline ElemDDLGranteeArray &getGranteeArray();

  // returns the name of the schema appearing in the
  // Revoke statement
  inline NAString getSchemaName() const;
  inline SchemaName getSchemaQualName() const;

  // returns the array of pointer pointing to parse
  // nodes representing privilege actions.  If the All
  // Privileges phrase appears in the Revoke statement,
  // the returned array is empty.  The method
  // isAllPrivileges() may be used to find out whether
  // the All Privileges phrase appears or not.

  inline const ElemDDLPrivActArray &getPrivilegeActionArray() const;
  inline ElemDDLPrivActArray &getPrivilegeActionArray();

  // returns TRUE if the All Privileges phrase appears
  // in the Revoke statement; returns FALSE otherwise.
  //
  // Note that getPrivilegeActionArray() returns an
  // empty array when the the All Privileges phrase
  // appears in the Revoke statement.
  inline NABoolean isAllPrivilegesSpecified() const;

  inline NABoolean isAllDDLPrivilegesSpecified() const;

  inline NABoolean isAllDMLPrivilegesSpecified() const;

  inline NABoolean isAllOtherPrivilegesSpecified() const;

  // returns TRUE if the Grant Option For phrase appears
  // in the Revoke statement; returns FALSE otherwise.
  inline NABoolean isGrantOptionForSpecified() const;

  // returns TRUE if the BY <grantor> phrase appears
  // in the GRANT SCHEMA statement; returns FALSE otherwise.
  inline NABoolean isByGrantorOptionSpecified() const;

  // returns pointer to the optional "by grantor"
  // (in the form of an ElemDDLGrantee).
  inline const ElemDDLGrantee *getByGrantor() const;

  // mutator
  virtual void setChild(int index, ExprNode *pChildNode);

  // for processing
  ExprNode *bindNode(BindWA *bindWAPtr);

  // for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString displayLabel2() const;
  virtual NATraceList getDetailInfo() const;
  virtual const NAString getText() const;

 private:
  NAString schemaName_;
  SchemaName schemaQualName_;

  // privilege actions
  NABoolean isAllDDLPrivileges_;
  NABoolean isAllDMLPrivileges_;
  NABoolean isAllOtherPrivileges_;
  ElemDDLPrivActArray privActArray_;

  // grantees
  ElemDDLGranteeArray granteeArray_;

  // grant option for
  NABoolean isGrantOptionForSpec_;

  // drop behavior
  ComDropBehavior dropBehavior_;

  // by grantor option specified?
  NABoolean isByGrantorOptionSpec_;

  // by grantor option value
  ElemDDLGrantee *byGrantor_;

  // pointers to child parse nodes

  enum { INDEX_PRIVILEGES = 0, INDEX_GRANTEE_LIST, INDEX_BY_GRANTOR_OPTION, MAX_STMT_DDL_REVOKE_ARITY };

  ElemDDLNode *children_[MAX_STMT_DDL_REVOKE_ARITY];

};  // class StmtDDLSchRevoke

// -----------------------------------------------------------------------
// definitions of inline methods for class StmtDDLSchRevoke
// -----------------------------------------------------------------------

//
// accessors
//

inline ComDropBehavior StmtDDLSchRevoke::getDropBehavior() const { return dropBehavior_; }

inline ElemDDLGranteeArray &StmtDDLSchRevoke::getGranteeArray() { return granteeArray_; }

inline const ElemDDLGranteeArray &StmtDDLSchRevoke::getGranteeArray() const { return granteeArray_; }

inline NAString StmtDDLSchRevoke::getSchemaName() const { return schemaName_; }

inline SchemaName StmtDDLSchRevoke::getSchemaQualName() const { return schemaQualName_; }

inline ElemDDLPrivActArray &StmtDDLSchRevoke::getPrivilegeActionArray() { return privActArray_; }

inline const ElemDDLPrivActArray &StmtDDLSchRevoke::getPrivilegeActionArray() const { return privActArray_; }

inline NABoolean StmtDDLSchRevoke::isAllPrivilegesSpecified() const {
  return (isAllDDLPrivileges_ && isAllDMLPrivileges_);
}

inline NABoolean StmtDDLSchRevoke::isAllDMLPrivilegesSpecified() const { return isAllDMLPrivileges_; }

inline NABoolean StmtDDLSchRevoke::isAllDDLPrivilegesSpecified() const { return isAllDDLPrivileges_; }

inline NABoolean StmtDDLSchRevoke::isAllOtherPrivilegesSpecified() const { return isAllOtherPrivileges_; }

inline NABoolean StmtDDLSchRevoke::isGrantOptionForSpecified() const { return isGrantOptionForSpec_; }

inline NABoolean StmtDDLSchRevoke::isByGrantorOptionSpecified() const { return isByGrantorOptionSpec_; }

inline const ElemDDLGrantee *StmtDDLSchRevoke::getByGrantor() const { return byGrantor_; }

#endif  // STMTDDLSCHREVOKE_H
