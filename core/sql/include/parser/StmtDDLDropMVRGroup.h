
#ifndef STMTDDLDROPMRVGROUP_H
#define STMTDDLDROPMRVGROUP_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLDropMvRGroup.h
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
class StmtDDLDropMvRGroup;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None

// -----------------------------------------------------------------------
// Create MV group statement
// -----------------------------------------------------------------------
class StmtDDLDropMvRGroup : public StmtDDLNode {
 public:
  // initialize constructor
  StmtDDLDropMvRGroup(const QualifiedName &mvGroupName);
  // CollHeap    * heap = PARSERHEAP());

  // virtual destructor
  virtual ~StmtDDLDropMvRGroup();

  // cast
  virtual StmtDDLDropMvRGroup *castToStmtDDLDropMvRGroup();

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

};  // class StmtDDLDropMvRGroup

// -----------------------------------------------------------------------
// definitions of inline methods for class StmtDDLCreateMvGroup
// -----------------------------------------------------------------------

// get index name
inline const NAString StmtDDLDropMvRGroup::getMvRGroupName() const {
  // return mvRGroupName_;
  return mvRGroupQualName_.getQualifiedNameAsAnsiString();
}

inline QualifiedName &StmtDDLDropMvRGroup::getMvRGroupNameAsQualifiedName() { return mvRGroupQualName_; }

inline const QualifiedName &StmtDDLDropMvRGroup::getMvRGroupNameAsQualifiedName() const { return mvRGroupQualName_; }

#endif  // STMTDDLDROPMRVGROUP_H
