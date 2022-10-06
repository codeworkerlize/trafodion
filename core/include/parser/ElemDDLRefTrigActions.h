
#ifndef ELEMDDLREFTRIGACTIONS_H
#define ELEMDDLREFTRIGACTIONS_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLRefTrigActions.h
 * Description:  classes representing rules specified in referential
 *               triggerred action phrase in References clause in
 *               DDL statements
 *
 *
 * Created:      10/27/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "ElemDDLRefActions.h"
#include "common/ComSmallDefs.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLRefTrigAct;
class ElemDDLRefTrigActDeleteRule;
class ElemDDLRefTrigActUpdateRule;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None

// -----------------------------------------------------------------------
// definition of base class ElemDDLRefTrigAct
// -----------------------------------------------------------------------
class ElemDDLRefTrigAct : public ElemDDLNode {
 public:
  // default constructor
  ElemDDLRefTrigAct(OperatorTypeEnum operatorType = ELM_ANY_REF_TRIG_ACT_RULE_ELEM,
                    ElemDDLNode *pReferentialAction = NULL)
      : ElemDDLNode(operatorType) {
    children_[INDEX_REFERENTIAL_ACTION] = pReferentialAction;
  }

  // virtual destructor
  virtual ~ElemDDLRefTrigAct();

  // cast
  virtual ElemDDLRefTrigAct *castToElemDDLRefTrigAct();

  // accessors
  virtual int getArity() const;
  virtual ExprNode *getChild(int index);
  inline ElemDDLRefAct *getReferentialAction() const;

  // mutator
  virtual void setChild(int index, ExprNode *pElemDDLNode);

  // methods for tracing
  virtual NATraceList getDetailInfo() const;
  virtual const NAString getText() const;

 private:
  // pointers to child parse node

  enum { INDEX_REFERENTIAL_ACTION = 0, MAX_ELEM_DDL_REF_TRIG_ACT_ARITY };

  ElemDDLNode *children_[MAX_ELEM_DDL_REF_TRIG_ACT_ARITY];

};  // class ElemDDLRefTrigAct

// -----------------------------------------------------------------------
// definition of class ElemDDLRefTrigActDeleteRule
// -----------------------------------------------------------------------
class ElemDDLRefTrigActDeleteRule : public ElemDDLRefTrigAct {
 public:
  // constructor
  ElemDDLRefTrigActDeleteRule(ElemDDLNode *pReferentialAction);

  // virtual destructor
  virtual ~ElemDDLRefTrigActDeleteRule();

  // cast
  virtual ElemDDLRefTrigActDeleteRule *castToElemDDLRefTrigActDeleteRule();

  // accessor
  inline ComRCDeleteRule getDeleteRule() const;

  // methods for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString getText() const;

 private:
  ComRCDeleteRule deleteRule_;  // an enum

};  // class ElemDDLRefTrigActDeleteRule

// -----------------------------------------------------------------------
// definition of class ElemDDLRefTrigActUpdateRule
// -----------------------------------------------------------------------
class ElemDDLRefTrigActUpdateRule : public ElemDDLRefTrigAct {
 public:
  // constructor
  ElemDDLRefTrigActUpdateRule(ElemDDLNode *pReferentialAction);

  // virtual destructor
  virtual ~ElemDDLRefTrigActUpdateRule();

  // accessor
  inline ComRCUpdateRule getUpdateRule() const;

  // cast
  virtual ElemDDLRefTrigActUpdateRule *castToElemDDLRefTrigActUpdateRule();

  // methods for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString getText() const;

 private:
  ComRCUpdateRule updateRule_;  // an enum

};  // class ElemDDLRefTrigActUpdateRule

// -----------------------------------------------------------------------
// definitions of inline methods for class ElemDDLRefTrigAct
// -----------------------------------------------------------------------
//
// accessor
//

inline ElemDDLRefAct *ElemDDLRefTrigAct::getReferentialAction() const {
  return children_[INDEX_REFERENTIAL_ACTION]->castToElemDDLRefAct();
}

// -----------------------------------------------------------------------
// definitions of inline methods for class ElemDDLRefTrigActDeleteRule
// -----------------------------------------------------------------------

//
// accessor
//

inline ComRCDeleteRule ElemDDLRefTrigActDeleteRule::getDeleteRule() const { return deleteRule_; }

// -----------------------------------------------------------------------
// definitions of inline methods for class ElemDDLRefTrigActUpdateRule
// -----------------------------------------------------------------------

//
// accessor
//

inline ComRCUpdateRule ElemDDLRefTrigActUpdateRule::getUpdateRule() const { return updateRule_; }

#endif  // ELEMDDLREFTRIGACTIONS_H
