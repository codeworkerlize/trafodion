
#ifndef STMTDDLALTERVIEW_H
#define STMTDDLALTERVIEW_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLAlterView.h
 * Description:  class for parse node representing Alter View statements
 *
 *
 * Created:      3/7/06
 * Language:     C++
 * Status:       $State: Exp $
 *
 *****************************************************************************
 */

// -----------------------------------------------------------------------
// Change history:
//
// Revision 1.0  2006/03/07 20:00:00
// Initial revision
//
//
//
// -----------------------------------------------------------------------

#include "common/BaseTypes.h"
#include "common/ComSmallDefs.h"
#include "parser/StmtDDLNode.h"

//----------------------------------------------------------------------------
// forward references
//----------------------------------------------------------------------------

class StmtDDLAlterView;
class QualifiedName;

//----------------------------------------------------------------------------
class StmtDDLAlterView : public StmtDDLNode {
 public:
  enum AlterType { RENAME, COMPILE };

  StmtDDLAlterView(QualifiedName &viewName, const NAString &newName);
  StmtDDLAlterView(QualifiedName &viewName, const NABoolean cascade);

  virtual ~StmtDDLAlterView();

  virtual StmtDDLAlterView *castToStmtDDLAlterView();
  ExprNode *bindNode(BindWA *bindWAPtr);

  virtual const NAString displayLabel1() const;
  virtual const NAString getText() const;

  // accessors

  const NAString getViewName() const { return viewQualName_.getQualifiedNameAsAnsiString(); }

  const QualifiedName &getViewNameAsQualifiedName() const;
  QualifiedName &getViewNameAsQualifiedName();

  AlterType getAlterType() const { return alterType_; }

  const NAString &getNewName() const { return newName_; }

  const NABoolean getCascade() const { return cascade_; }

 private:
  AlterType alterType_;
  QualifiedName viewQualName_;
  NAString newName_;
  NABoolean cascade_;

  // DO NOT USE the following methods

  StmtDDLAlterView();
  StmtDDLAlterView(const StmtDDLAlterView &o);
  StmtDDLAlterView &operator=(const StmtDDLAlterView &o);

};  // class StmtDDLAlterView

// -----------------------------------------------------------------------
// definitions of inline methods for class StmtDDLAlterView
// -----------------------------------------------------------------------

// None

#endif  // STMTDDLALTERVIEW_H
