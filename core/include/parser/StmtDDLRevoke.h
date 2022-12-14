#ifndef STMTDDLREVOKE_H
#define STMTDDLREVOKE_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLRevoke.h
 * Description:  class for parse nodes representing Revoke DDL statements
 *
 *
 * Created:      12/15/95
 * Language:     C++
 *
 *

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
class StmtDDLRevoke;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None

// -----------------------------------------------------------------------
// definition of class StmtDDLRevoke
// -----------------------------------------------------------------------
class StmtDDLRevoke : public StmtDDLNode {
 public:
  // constructor
  StmtDDLRevoke(NABoolean isGrantOptionFor, ElemDDLNode *pPrivileges, const QualifiedName &objectName,
                ElemDDLNode *pGranteeList, ComDropBehavior dropBehavior, ElemDDLNode *pByGrantorOption,
                QualifiedName *actionName, CollHeap *heap = PARSERHEAP());

  // virtual destructor
  virtual ~StmtDDLRevoke();

  // cast
  virtual StmtDDLRevoke *castToStmtDDLRevoke();

  //
  // accessors
  //

  virtual int getArity() const;
  virtual ExprNode *getChild(int index);

  inline ComDropBehavior getDropBehavior() const;

  inline const ElemDDLGranteeArray &getGranteeArray() const;
  inline ElemDDLGranteeArray &getGranteeArray();

  // returns the array of pointers pointing to parse
  // nodes representing grantees.  This array contains
  // at least one element.

  inline const QualifiedName &getRevokeNameAsQualifiedName() const;
  inline QualifiedName &getRevokeNameAsQualifiedName();

  inline NAString getObjectName() const;
  inline NAString getOrigObjectName() const { return origObjectName_; }

  void setObjectQualName(QualifiedName qn) {
    objectQualName_ = qn;
    objectName_ = qn.getQualifiedNameAsAnsiString();
  }

  inline const QualifiedName *getActionNameAsQualifiedName() const;

  // returns the name of the object appearing in the
  // Revoke statement

  inline const ElemDDLPrivActArray &getPrivilegeActionArray() const;
  inline ElemDDLPrivActArray &getPrivilegeActionArray();

  // returns the array of pointer pointing to parse
  // nodes representing privilege actions.  If the All
  // Privileges phrase appears in the Revoke statement,
  // the returned array is empty.  The method
  // isAllPrivileges() may be used to find out whether
  // the All Privileges phrase appears or not.

  inline NAString getTableName() const;

  // same as getObjectName()

  inline NABoolean isAllPrivilegesSpecified() const;

  // returns TRUE if the All Privileges phrase appears
  // in the Revoke statement; returns FALSE otherwise.
  //
  // Note that getPrivilegeActionArray() returns an
  // empty array when the the All Privileges phrase
  // appears in the Revoke statement.

  inline NABoolean isGrantOptionForSpecified() const;

  // returns TRUE if the Grant Option For phrase appears
  // in the Revoke statement; returns FALSE otherwise.

  inline NABoolean isByGrantorOptionSpecified() const;

  // returns TRUE if the As <grantor) phrase appears
  // in the Revoke statement; returns FALSE otherwise.

  inline const ElemDDLGrantee *getByGrantor() const;
  // returns pointer to the optional "by grantor"
  // (in the form of an ElemDDLGrantee).

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
  NAString origObjectName_;
  NAString objectName_;
  QualifiedName objectQualName_;

  // For action clause
  QualifiedName *actionQualName_;

  // privilege actions
  NABoolean isAllPrivileges_;
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

};  // class StmtDDLRevoke

// -----------------------------------------------------------------------
// definitions of inline methods for class StmtDDLRevoke
// -----------------------------------------------------------------------

//
// accessors
//

inline QualifiedName &StmtDDLRevoke::getRevokeNameAsQualifiedName() { return objectQualName_; }

inline const QualifiedName &StmtDDLRevoke::getRevokeNameAsQualifiedName() const { return objectQualName_; }

inline ComDropBehavior StmtDDLRevoke::getDropBehavior() const { return dropBehavior_; }

inline ElemDDLGranteeArray &StmtDDLRevoke::getGranteeArray() { return granteeArray_; }

inline const ElemDDLGranteeArray &StmtDDLRevoke::getGranteeArray() const { return granteeArray_; }

inline NAString StmtDDLRevoke::getObjectName() const { return objectName_; }

inline ElemDDLPrivActArray &StmtDDLRevoke::getPrivilegeActionArray() { return privActArray_; }

inline const ElemDDLPrivActArray &StmtDDLRevoke::getPrivilegeActionArray() const { return privActArray_; }

inline NAString StmtDDLRevoke::getTableName() const { return objectName_; }

inline const QualifiedName *StmtDDLRevoke::getActionNameAsQualifiedName() const { return actionQualName_; }

inline NABoolean StmtDDLRevoke::isAllPrivilegesSpecified() const { return isAllPrivileges_; }

inline NABoolean StmtDDLRevoke::isGrantOptionForSpecified() const { return isGrantOptionForSpec_; }

inline NABoolean StmtDDLRevoke::isByGrantorOptionSpecified() const { return isByGrantorOptionSpec_; }

inline const ElemDDLGrantee *StmtDDLRevoke::getByGrantor() const { return byGrantor_; }
#endif  // STMTDDLREVOKE_H
