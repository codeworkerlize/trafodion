
#ifndef ELEMDDLPARTITIONCLAUSE_H
#define ELEMDDLPARTITIONCLAUSE_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLPartitionClause.h
 * Description:  class for parse nodes representing partition clauses
 *               in DDL statements.  Note that this class is derived
 *               from class ElemDDLNode instead of class ElemDDLPartition.
 *
 *
 * Created:      10/4/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "ElemDDLNode.h"
#include "common/ComSmallDefs.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLPartitionClause;
class ElemDDLPartitionClauseV2;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// definition of class ElemDDLPartitionClause
// -----------------------------------------------------------------------
class ElemDDLPartitionClause : public ElemDDLNode {
 public:
  // constructor
  ElemDDLPartitionClause(ElemDDLNode *pPartitionDefBody, ElemDDLNode *pPartitionByOption,
                         ComPartitioningScheme partitionType)
      : ElemDDLNode(ELM_PARTITION_CLAUSE_ELEM) {
    setChild(INDEX_PARTITION_DEFINITION_BODY, pPartitionDefBody);
    setChild(INDEX_PARTITION_BY_OPTION, pPartitionByOption);
    partitionType_ = partitionType;
    isForSplit_ = FALSE;
  }

  // virtual destructor
  virtual ~ElemDDLPartitionClause();

  // cast
  virtual ElemDDLPartitionClause *castToElemDDLPartitionClause();

  // accessors
  virtual int getArity() const;
  virtual ExprNode *getChild(int index);
  inline ElemDDLNode *getPartitionDefBody() const;
  inline ElemDDLNode *getPartitionByOption() const;
  inline ComPartitioningScheme getPartitionType() const;
  inline ComBoolean getIsForSplit() const;

  // mutator
  virtual void setChild(int index, ExprNode *pElemDDLNode);
  inline void setIsForSplit(ComBoolean split);

  // method for tracing
  virtual const NAString getText() const;

  // method for building text
  virtual NAString getSyntax() const;

 private:
  // pointers to child parse nodes

  enum { INDEX_PARTITION_DEFINITION_BODY = 0, INDEX_PARTITION_BY_OPTION, MAX_ELEM_DDL_PARTITION_CLAUSE_ARITY };

  ElemDDLNode *children_[MAX_ELEM_DDL_PARTITION_CLAUSE_ARITY];

  ComPartitioningScheme partitionType_;
  ComBoolean isForSplit_;

};  // class ElemDDLPartitionClause

// -----------------------------------------------------------------------
// definitions of inline methods for class ElemDDLPartitionClause
// -----------------------------------------------------------------------

inline ElemDDLNode *ElemDDLPartitionClause::getPartitionDefBody() const {
  return children_[INDEX_PARTITION_DEFINITION_BODY];
}

inline ElemDDLNode *ElemDDLPartitionClause::getPartitionByOption() const {
  return children_[INDEX_PARTITION_BY_OPTION];
}

inline ComPartitioningScheme ElemDDLPartitionClause::getPartitionType() const { return partitionType_; }

inline ComBoolean ElemDDLPartitionClause::getIsForSplit() const { return isForSplit_; }

inline void ElemDDLPartitionClause::setIsForSplit(ComBoolean split) { isForSplit_ = split; }

// this is the new partition table implement, which is
// compatible with famous "O"
class ElemDDLPartitionClauseV2 : public ElemDDLNode {
 public:
  ElemDDLPartitionClauseV2(ElemDDLNode *pPartitionDefBody, ElemDDLNode *pPartitionColList,
                           ComPartitioningSchemeV2 partitionType, NABoolean isSubpartition = false)
      : ElemDDLNode(ELM_PARTITION_CLAUSE_V2_ELEM), partitionType_(partitionType), isSubpartition_(isSubpartition) {
    setChild(INDEX_PARTITION_DEFINITION_BODY, pPartitionDefBody);
    setChild(INDEX_PARTITION_COLUMN_LIST, pPartitionColList);
    setChild(INDEX_SUBPARTITION_CLAUSE, NULL);
  }

  // virtual destructor
  virtual ~ElemDDLPartitionClauseV2();

  // cast
  virtual ElemDDLPartitionClauseV2 *castToElemDDLPartitionClauseV2();

  // accessors
  virtual int getArity() const;
  virtual ExprNode *getChild(int index);
  inline ElemDDLNode *getPartitionDefBody() const;
  inline ElemDDLNode *getPartitionColList() const;
  inline ElemDDLNode *getSubpartitionClause() const;
  inline ComPartitioningSchemeV2 getPartitionType() const { return partitionType_; }
  inline NABoolean isSubpartition() const { return isSubpartition_; }

  // mutator
  virtual void setChild(int index, ExprNode *pElemDDLNode);

  // method for tracing
  virtual const NAString getText() const;

  // method for building text
  virtual NAString getSyntax() const;

 private:
  // pointers to child parse nodes

  enum {
    INDEX_PARTITION_DEFINITION_BODY = 0,
    INDEX_PARTITION_COLUMN_LIST,
    INDEX_SUBPARTITION_CLAUSE,
    MAX_ELEM_DDL_PARTITION_CLAUSE_ARITY_V2
  };

  ElemDDLNode *children_[MAX_ELEM_DDL_PARTITION_CLAUSE_ARITY_V2];

  NABoolean isSubpartition_;
  ComPartitioningSchemeV2 partitionType_;

};  // class ElemDDLPartitionClauseV2

// -----------------------------------------------------------------------
// definitions of inline methods for class ElemDDLPartitionClauseV2
// -----------------------------------------------------------------------

inline ElemDDLNode *ElemDDLPartitionClauseV2::getPartitionDefBody() const {
  return children_[INDEX_PARTITION_DEFINITION_BODY];
}

inline ElemDDLNode *ElemDDLPartitionClauseV2::getPartitionColList() const {
  return children_[INDEX_PARTITION_COLUMN_LIST];
}

inline ElemDDLNode *ElemDDLPartitionClauseV2::getSubpartitionClause() const {
  return children_[INDEX_SUBPARTITION_CLAUSE];
}

#endif  // ELEMDDLPARTITIONCLAUSE_H
