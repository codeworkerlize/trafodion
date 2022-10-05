
#ifndef STMTDDLDROPCATALOG_H
#define STMTDDLDROPCATALOG_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLDropCatalog.h
 * Description:  class for parse node representing Drop Catalog statements
 *
 *
 * Created:      11/14/95
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
class StmtDDLDropCatalog;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None

// -----------------------------------------------------------------------
// Create Catalog statement
// -----------------------------------------------------------------------
class StmtDDLDropCatalog : public StmtDDLNode {
 public:
  // constructor
  StmtDDLDropCatalog(const NAString &catalogName, ComDropBehavior dropBehavior);

  // virtual destructor
  virtual ~StmtDDLDropCatalog();

  // cast
  virtual StmtDDLDropCatalog *castToStmtDDLDropCatalog();

  // accessor
  inline const NAString &getCatalogName() const;
  inline ComDropBehavior getDropBehavior() const;

  // for binding
  ExprNode *bindNode(BindWA *bindWAPtr);

  // for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString getText() const;

 private:
  NAString catalogName_;
  ComDropBehavior dropBehavior_;

};  // class StmtDDLDropCatalog

// -----------------------------------------------------------------------
// definitions of inline methods for class StmtDDLDropCatalog
// -----------------------------------------------------------------------

//
// accessor
//

inline const NAString &StmtDDLDropCatalog::getCatalogName() const { return catalogName_; }

inline ComDropBehavior StmtDDLDropCatalog::getDropBehavior() const { return dropBehavior_; }

#endif  // STMTDDLDROPCATALOG_H
