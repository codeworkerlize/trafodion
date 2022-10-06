#ifndef STMTDDLGIVECATALOG_H
#define STMTDDLGIVECATALOG_H

/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLGiveCatalog.h
 * Description:  class for parse nodes representing Give Catalog
 *               statements
 *
 *
 * Created:      7/11/2006
 * Language:     C++
 *
 *****************************************************************************
 */

#include "ElemDDLLocation.h"
#include "common/ComLocationNames.h"
#include "parser/StmtDDLNode.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLGiveCatalog;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None

// -----------------------------------------------------------------------
// Give Catalog statement
// -----------------------------------------------------------------------
class StmtDDLGiveCatalog : public StmtDDLNode {
 public:
  // default constructor
  StmtDDLGiveCatalog(const NAString &aCatalogName, const NAString &aUserID);

  // virtual destructor
  virtual ~StmtDDLGiveCatalog();

  // cast
  virtual StmtDDLGiveCatalog *castToStmtDDLGiveCatalog();

  //
  // accessors
  //

  virtual int getArity() const;

  inline const NAString &getCatalogName() const;

  virtual ExprNode *getChild(int index);

  inline const NAString &getUserID() const;

  // mutator
  virtual void setChild(int index, ExprNode *pChildNode);
  // for binding
  ExprNode *bindNode(BindWA *bindWAPtr);

  // for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString displayLabel2() const;
  virtual const NAString getText() const;

 private:
  // ---------------------------------------------------------------------
  // private methods
  // ---------------------------------------------------------------------

  //
  // mutator
  //

  void setAttribute(ElemDDLNode *pAttrNode);

  // Get the information in the parse node pointed by parameter
  // pAttrNode.  Update the corresponding data member (in this
  // class) accordingly.  Also check for duplicate clauses.
  // Get the information in the parse node pointed by parameter
  // pAttrNode.  Update the corresponding data member (in this
  // class) accordingly.  Also check for duplicate clauses.

  // ---------------------------------------------------------------------
  // private data members
  // ---------------------------------------------------------------------

  NAString catalogName_;
  NAString userID_;

  // The flags is...Spec_ are used to
  // check for duplicate clauses

  // pointer to child parse node

  enum { INDEX_GIVE_CATALOG_ATTRIBUTE_LIST = 0, MAX_STMT_DDL_GIVE_CATALOG_ARITY };

  ElemDDLNode *attributeList_;

};  // class StmtDDLGiveCatalog

// -----------------------------------------------------------------------
// definitions of inline methods for class StmtDDLGiveCatalog
// -----------------------------------------------------------------------

//
// accessors
//

inline const NAString &StmtDDLGiveCatalog::getCatalogName() const { return catalogName_; }

inline const NAString &StmtDDLGiveCatalog::getUserID() const { return userID_; }

#endif  // STMTDDLGIVECATALOG_H
