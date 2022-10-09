
#ifndef ELEMDDLPRIVILEGES_H
#define ELEMDDLPRIVILEGES_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLPrivileges.h
 * Description:  class for a parse node representing the privileges
 *               appearing in a Grant DDL statement.
 *
 *
 * Created:      10/16/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "parser/ElemDDLNode.h"
#include "common/ComASSERT.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLPrivileges;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// definition of class ElemDDLPrivileges
// -----------------------------------------------------------------------
class ElemDDLPrivileges : public ElemDDLNode {
 public:
  // constructors
  ElemDDLPrivileges(ElemDDLNode::WhichAll typeOfAll)
      : ElemDDLNode(ELM_PRIVILEGES_ELEM),
        isAllDMLPrivileges_(TRUE),
        isAllDDLPrivileges_(TRUE),
        isAllOtherPrivileges_(TRUE) {
    ComASSERT(typeOfAll == ElemDDLNode::ALL);
    setChild(INDEX_PRIVILEGE_ACTION_LIST, NULL);
  }

  ElemDDLPrivileges(ElemDDLNode *pPrivilegeActions)
      : ElemDDLNode(ELM_PRIVILEGES_ELEM),
        isAllDMLPrivileges_(FALSE),
        isAllDDLPrivileges_(FALSE),
        isAllOtherPrivileges_(FALSE) {
    ComASSERT(pPrivilegeActions NEQ NULL);
    setChild(INDEX_PRIVILEGE_ACTION_LIST, pPrivilegeActions);
    isAllDMLPrivileges_ = containsPriv(ELM_PRIV_ACT_ALL_DML_ELEM);
    isAllDDLPrivileges_ = containsPriv(ELM_PRIV_ACT_ALL_DDL_ELEM);
    isAllOtherPrivileges_ = containsPriv(ELM_PRIV_ACT_ALL_OTHER_ELEM);
  }

  // virtual destructor
  virtual ~ElemDDLPrivileges();

  // cast
  virtual ElemDDLPrivileges *castToElemDDLPrivileges();

  //
  // accessors
  //

  virtual int getArity() const;
  virtual ExprNode *getChild(int index);

  inline ElemDDLNode *getPrivilegeActionList() const;

  // returns NULL when the phrase ALL PRIVILEGES appears;
  // otherwise returns a pointer to a parse node representing
  // a privilege action or a list of privilege actions.

  // returns TRUE if the phrase ALL PRIVILEGES appears;
  // returns FALSE otherwise.

  inline NABoolean isAllPrivileges() const;

  inline NABoolean isAllDDLPrivileges() const;

  inline NABoolean isAllDMLPrivileges() const;

  inline NABoolean isAllOtherPrivileges() const;

  // mutator
  virtual void setChild(int index, ExprNode *pElemDDLNode);

  // methods for tracing
  virtual const NAString displayLabel1() const;
  virtual NATraceList getDetailInfo() const;
  virtual const NAString getText() const;

  NABoolean containsColumnPrivs() const;

  NABoolean containsPriv(OperatorTypeEnum whichPriv) const;

 private:
  NABoolean isAllDDLPrivileges_;
  NABoolean isAllDMLPrivileges_;
  NABoolean isAllOtherPrivileges_;

  // pointer to child parse node

  enum { INDEX_PRIVILEGE_ACTION_LIST = 0, MAX_ELEM_DDL_PRIVILEGES_ARITY };

  ElemDDLNode *children_[MAX_ELEM_DDL_PRIVILEGES_ARITY];

};  // class ElemDDLPrivileges

// -----------------------------------------------------------------------
// definitions of inline methods for class ElemDDLPrivileges
// -----------------------------------------------------------------------

//
// accessors
//

inline ElemDDLNode *ElemDDLPrivileges::getPrivilegeActionList() const { return children_[INDEX_PRIVILEGE_ACTION_LIST]; }

inline NABoolean ElemDDLPrivileges::isAllPrivileges() const {
  return (isAllDDLPrivileges_ && isAllDMLPrivileges_ && isAllOtherPrivileges_);
}

inline NABoolean ElemDDLPrivileges::isAllDDLPrivileges() const { return isAllDDLPrivileges_; }

inline NABoolean ElemDDLPrivileges::isAllDMLPrivileges() const { return isAllDMLPrivileges_; }

inline NABoolean ElemDDLPrivileges::isAllOtherPrivileges() const { return isAllOtherPrivileges_; }

#endif  // ELEMDDLPRIVILEGES_H
