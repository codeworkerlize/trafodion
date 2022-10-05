#ifndef STMTDDLDROPVIEW_H
#define STMTDDLDROPVIEW_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLDropView.h
 * Description:  class for parse node representing Drop View statements
 *
 *
 * Created:      11/15/95
 * Language:     C++
 *
 *

 *
 *
 *****************************************************************************
 */

#include "common/ComSmallDefs.h"
#include "StmtDDLNode.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLDropView;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None

// -----------------------------------------------------------------------
// Create Catalog statement
// -----------------------------------------------------------------------
class StmtDDLDropView : public StmtDDLNode {
 public:
  // constructor
  StmtDDLDropView(const QualifiedName &tableQualName, ComDropBehavior dropBehavior, NABoolean cleanupSpec,
                  NABoolean validateSpec, NAString *pLogFile);

  // virtual destructor
  virtual ~StmtDDLDropView();

  // cast
  virtual StmtDDLDropView *castToStmtDDLDropView();

  // accessors
  inline ComDropBehavior getDropBehavior() const;
  inline const NAString getViewName() const;
  inline const QualifiedName &getViewNameAsQualifiedName() const;
  inline QualifiedName &getViewNameAsQualifiedName();
  inline const NABoolean isCleanupSpecified() const;
  inline const NABoolean isValidateSpecified() const;
  inline const NABoolean isLogFileSpecified() const;
  inline const NAString &getLogFile() const;

  const NABoolean dropIfExists() const { return dropIfExists_; }
  void setDropIfExists(NABoolean v) { dropIfExists_ = v; }

  // for binding
  ExprNode *bindNode(BindWA *bindWAPtr);

  // for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString displayLabel2() const;
  virtual const NAString getText() const;

 private:
  QualifiedName viewQualName_;
  ComDropBehavior dropBehavior_;
  NABoolean isCleanupSpec_;
  NABoolean isValidateSpec_;
  NAString *pLogFile_;

  // drop only if view exists. Otherwise just return.
  NABoolean dropIfExists_;

};  // class StmtDDLDropView

// -----------------------------------------------------------------------
// definitions of inline methods for class StmtDDLDropView
// -----------------------------------------------------------------------

//
// accessors
//

inline QualifiedName &StmtDDLDropView::getViewNameAsQualifiedName() { return viewQualName_; }

inline const QualifiedName &StmtDDLDropView::getViewNameAsQualifiedName() const { return viewQualName_; }

inline ComDropBehavior StmtDDLDropView::getDropBehavior() const { return dropBehavior_; }

inline const NAString StmtDDLDropView::getViewName() const { return viewQualName_.getQualifiedNameAsAnsiString(); }

inline const NABoolean StmtDDLDropView::isCleanupSpecified() const { return isCleanupSpec_; }

inline const NABoolean StmtDDLDropView::isValidateSpecified() const { return isValidateSpec_; }

inline const NABoolean StmtDDLDropView::isLogFileSpecified() const {
  if (pLogFile_) return TRUE;
  return FALSE;
}

inline const NAString &StmtDDLDropView::getLogFile() const {
  ComASSERT(pLogFile_ NEQ NULL);
  return *pLogFile_;
}

#endif  // STMTDDLDROPVIEW_H
