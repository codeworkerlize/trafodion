
#ifndef ELEMDDLREFERENCES_H
#define ELEMDDLREFERENCES_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLReferences.h
 * Description:  class for referenced table and columns in REFERENCES
 *               clause in referential integrity constraint definitions
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

#include "parser/ElemDDLNode.h"
#include "optimizer/ObjectNames.h"
// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLReferences;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// Referenced table and columns in Referential Integrity Constraint
// Definition elements in DDL statements.
// -----------------------------------------------------------------------
class ElemDDLReferences : public ElemDDLNode {
 public:
  // default constructor
  ElemDDLReferences(const QualifiedName &referencedTableName, ElemDDLNode *pReferencedColumnList = NULL)
      : ElemDDLNode(ELM_REFERENCES_ELEM), referencedTableQualName_(referencedTableName, PARSERHEAP()) {
    setChild(INDEX_REFERENCED_COLUMN_LIST, pReferencedColumnList);
  }

  // virtual destructor
  virtual ~ElemDDLReferences();

  // cast
  virtual ElemDDLReferences *castToElemDDLReferences();

  // accessors
  virtual int getArity() const;
  virtual ExprNode *getChild(int index);
  inline ElemDDLNode *getReferencedColumns() const;

  NAString getReferencedTableName() const;

  // returns the externally-formatted name of the
  // referenced table.  If this routine is invoked
  // after the parse node is bound, the returned
  // name is guaranteed to be fully-expanded.

  inline const QualifiedName &getReferencedNameAsQualifiedName() const;
  inline QualifiedName &getReferencedNameAsQualifiedName();

  // mutators
  virtual void setChild(int index, ExprNode *pElemDDLNode);

  // methods for tracing
  virtual const NAString displayLabel1() const;
  virtual NATraceList getDetailInfo() const;
  virtual const NAString getText() const;

  // methods for processing
  virtual ExprNode *bindNode(BindWA *pBindWA);

 private:
  // ---------------------------------------------------------------------
  // private methods
  // ---------------------------------------------------------------------

  ElemDDLReferences();                                    // DO NOT USE
  ElemDDLReferences(const NAString &referencedTableName,  // DO NOT USE
                    ElemDDLNode *pReferencedColumnList = NULL);
  ElemDDLReferences(const ElemDDLReferences &);             // DO NOT USE
  ElemDDLReferences &operator=(const ElemDDLReferences &);  // DO NOT USE

  // ---------------------------------------------------------------------
  // private data members
  // ---------------------------------------------------------------------

  QualifiedName referencedTableQualName_;

  // pointer to child parse node

  enum { INDEX_REFERENCED_COLUMN_LIST = 0, MAX_ELEM_DDL_REFERENCES_ARITY };

  ElemDDLNode *children_[MAX_ELEM_DDL_REFERENCES_ARITY];

};  // class ElemDDLReferences

// -----------------------------------------------------------------------
// definitions of inline methods for class ElemDDLReferences
// -----------------------------------------------------------------------
//
// accessors
//

inline QualifiedName &ElemDDLReferences::getReferencedNameAsQualifiedName() { return referencedTableQualName_; }

inline const QualifiedName &ElemDDLReferences::getReferencedNameAsQualifiedName() const {
  return referencedTableQualName_;
}

inline ElemDDLNode *ElemDDLReferences::getReferencedColumns() const { return children_[INDEX_REFERENCED_COLUMN_LIST]; }

#endif  // ELEMDDLREFERENCES_H
