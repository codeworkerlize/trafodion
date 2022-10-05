
#ifndef STMTDDLCREATEMVRGROUP_H
#define STMTDDLCREATEMVRGROUP_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLCreateMvRGroup.h
 * Description:  class representing Create MV group Statement parser nodes
 *
 *
 * Created:      5/06/99
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "ElemDDLNode.h"
#include "common/NAString.h"
#include "StmtDDLNode.h"
#include "common/ComSmallDefs.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLCreateMvRGroup;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None

// -----------------------------------------------------------------------
// Create MV group statement
// -----------------------------------------------------------------------
class StmtDDLCreateMvRGroup : public StmtDDLNode {
 public:
  // initialize constructor
  StmtDDLCreateMvRGroup(const QualifiedName &mvGroupName, CollHeap *heap = PARSERHEAP());

  // virtual destructor
  virtual ~StmtDDLCreateMvRGroup();

  // cast
  virtual StmtDDLCreateMvRGroup *castToStmtDDLCreateMvRGroup();

  //
  // accessors
  //

  inline const NAString getMvRGroupName() const;
  inline const QualifiedName &getMvRGroupNameAsQualifiedName() const;
  inline QualifiedName &getMvRGroupNameAsQualifiedName();

  //
  // mutators
  //

  //
  // method for binding
  //

  ExprNode *bindNode(BindWA *pBindWA);

  //
  // methods for tracing
  //

  virtual const NAString displayLabel1() const;
  //  virtual NATraceList getDetailInfo() const;
  virtual const NAString getText() const;

 private:
  // ---------------------------------------------------------------------
  // private data members
  // ---------------------------------------------------------------------

  //  NAString mvRGroupName_;

  // The syntax of table name is
  // [ [ catalog-name . ] schema-name . ] table-name
  QualifiedName mvRGroupQualName_;

};  // class StmtDDLCreateMvGroup

// -----------------------------------------------------------------------
// definitions of inline methods for class StmtDDLCreateMvGroup
// -----------------------------------------------------------------------

inline const NAString StmtDDLCreateMvRGroup::getMvRGroupName() const {
  return mvRGroupQualName_.getQualifiedNameAsAnsiString();
}

inline QualifiedName &StmtDDLCreateMvRGroup::getMvRGroupNameAsQualifiedName() { return mvRGroupQualName_; }

inline const QualifiedName &StmtDDLCreateMvRGroup::getMvRGroupNameAsQualifiedName() const { return mvRGroupQualName_; }

#endif  // STMTDDLCREATEMRVGROUP_H
