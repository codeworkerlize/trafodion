#ifndef ELEMDDLCONSTRAINTRI_H
#define ELEMDDLCONSTRAINTRI_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLConstraintRI.h
 * Description:  class for Referential Integrity constraint definitions
 *               in DDL statements
 *
 *
 * Created:      3/29/95
 * Language:     C++
 *
 *

 *
 *
 *****************************************************************************
 */

#include "ElemDDLColNameArray.h"
#include "ElemDDLConstraint.h"
#include "common/ComSmallDefs.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLConstraintRI;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// Referential Integrity Constraint Definition elements in DDL statements
// -----------------------------------------------------------------------
class ElemDDLConstraintRI : public ElemDDLConstraint {
 public:
  // default constructor
  ElemDDLConstraintRI(ElemDDLNode *pReferencedTableAndColumns = NULL,
                      ComRCMatchOption matchType = COM_NONE_MATCH_OPTION,
                      ElemDDLNode *pReferentialTriggerActions = NULL, CollHeap *heap = PARSERHEAP());

  // virtual destructor
  virtual ~ElemDDLConstraintRI();

  // cast
  virtual ElemDDLConstraintRI *castToElemDDLConstraintRI();

  //
  // accessors
  //

  virtual int getArity() const;
  virtual ExprNode *getChild(int index);

  inline ComRCDeleteRule getDeleteRule() const;

  // returns COM_UNKNOWN_DELETE_RULE when Delete
  // rule does not appear.

  inline ComRCMatchOption getMatchType() const;
  NAString getMatchTypeAsNAString() const;
  inline ElemDDLColNameArray &getReferencedColumns();
  inline const ElemDDLColNameArray &getReferencedColumns() const;

  NAString getReferencedTableName() const;

  // returns the externally-formatted name of the
  // referenced table.  If this routine is invoked
  // after the parse node is bound, the returned
  // name is guaranteed to be fully-expanded.

  inline ElemDDLNode *getReferencingColumnNameList() const;

  // returns the pointer pointing to either a parse node
  // representing a column name or a left-skewed binary
  // tree representing a (left linear tree) list of
  // parse nodes representing column names.
  //
  // Note that the method returns the NULL pointer value
  // if the constraint is a column constraint and the method
  // is invoked before the construction of the Column
  // Definition parse node.  (During the construction of the
  // Column Definition parse node, a new Column Name parse
  // node is created, for the column constraint, to contain
  // the name of the column.)

  inline ElemDDLColNameArray &getReferencingColumns();
  inline const ElemDDLColNameArray &getReferencingColumns() const;

  inline ComRCUpdateRule getUpdateRule() const;

  // returns COM_UNKNOWN_UPDATE_RULE when Update
  // rule does not appear.

  inline NABoolean isDeleteRuleSpecified() const;
  inline NABoolean isUpdateRuleSpecified() const;

  // mutators
  virtual void setChild(int index, ExprNode *pChildNode);
  void setReferencingColumnNameList(ElemDDLNode *pReferencingColumnNameList);

  // methods for tracing
  virtual const NAString displayLabel2() const;
  virtual NATraceList getDetailInfo() const;
  virtual const NAString getText() const;

  // methods for processing
  virtual ExprNode *bindNode(BindWA *pBindWA);

 private:
  //
  // methods
  //

  ElemDDLReferences *getReferencesNode() const;

  void setReferentialTriggeredAction(ElemDDLRefTrigAct *pRefTrigAct);

  //
  // data members
  //

  ElemDDLColNameArray referencingColumnNameArray_;
  ElemDDLColNameArray referencedColumnNameArray_;

  ComRCMatchOption matchType_;

  NABoolean isDeleteRuleSpec_;
  ComRCDeleteRule deleteRule_;

  NABoolean isUpdateRuleSpec_;
  ComRCUpdateRule updateRule_;

  // pointers to child parse nodes

  enum {
    INDEX_REFERENCING_COLUMN_NAME_LIST = MAX_ELEM_DDL_CONSTRAINT_ARITY,
    INDEX_REFERENCED_TABLE_AND_COLUMNS,
    INDEX_REFERENTIAL_TRIGGERED_ACTIONS,
    MAX_ELEM_DDL_CONSTRAINT_RI_ARITY
  };

  ElemDDLNode *children_[MAX_ELEM_DDL_CONSTRAINT_RI_ARITY];

};  // class ElemDDLConstraintRI

// -----------------------------------------------------------------------
// definitions of inline methods for class ElemDDLConstraintRI
// -----------------------------------------------------------------------

//
// accessors
//

inline ComRCDeleteRule ElemDDLConstraintRI::getDeleteRule() const { return deleteRule_; }

inline ComRCMatchOption ElemDDLConstraintRI::getMatchType() const { return matchType_; }

inline ElemDDLColNameArray &ElemDDLConstraintRI::getReferencedColumns() { return referencedColumnNameArray_; }

inline const ElemDDLColNameArray &ElemDDLConstraintRI::getReferencedColumns() const {
  return referencedColumnNameArray_;
}

inline ElemDDLNode *ElemDDLConstraintRI::getReferencingColumnNameList() const {
  return children_[INDEX_REFERENCING_COLUMN_NAME_LIST];
}

inline ElemDDLColNameArray &ElemDDLConstraintRI::getReferencingColumns() { return referencingColumnNameArray_; }

inline const ElemDDLColNameArray &ElemDDLConstraintRI::getReferencingColumns() const {
  return referencingColumnNameArray_;
}

inline ComRCUpdateRule ElemDDLConstraintRI::getUpdateRule() const { return updateRule_; }

inline NABoolean ElemDDLConstraintRI::isDeleteRuleSpecified() const { return isDeleteRuleSpec_; }

inline NABoolean ElemDDLConstraintRI::isUpdateRuleSpecified() const { return isUpdateRuleSpec_; }

#endif  // ELEMDDLCONSTRAINTRI_H
