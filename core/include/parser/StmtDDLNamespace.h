#ifndef STMTDDLNAMESPACE_H
#define STMTDDLNAMESPACE_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLNamespace.h
 * Description:  class for parse nodes representing create/drop namespace stmts
 *
 *
 * Created:
 * Language:     C++
 *
 *

 *
 *
 *****************************************************************************
 */

#include "common/ComUnits.h"
#include "parser/ElemDDLHbaseOptions.h"
#include "parser/StmtDDLNode.h"

#define SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#include "parser/SqlParserGlobals.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLNamespace;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None

// -----------------------------------------------------------------------
// definition of class StmtDDLNamespace
// -----------------------------------------------------------------------
class StmtDDLNamespace : public StmtDDLNode {
 public:
  // constructor
  StmtDDLNamespace(ComNamespaceOper oper, NAString &namespace1, NABoolean existsClause,
                   ElemDDLHbaseOptions *quotaOptions = NULL, CollHeap *heap = PARSERHEAP())
      : StmtDDLNode(DDL_NAMESPACE),
        oper_(oper),
        existsClause_(existsClause),
        namespace_(namespace1),
        quotaOptions_(quotaOptions) {
    // create/drop namespace commands are non-transactional
    setDdlXns(FALSE);
  }

  // virtual destructor
  virtual ~StmtDDLNamespace() {}

  // cast
  virtual StmtDDLNamespace *castToStmtDDLNamespace() { return this; }

  //
  // accessors
  //
  const NAString &getNamespace() const { return namespace_; }
  const ComNamespaceOper &namespaceOper() const { return oper_; }
  const NABoolean &existsClause() const { return existsClause_; }

  NAList<HbaseCreateOption *> *getQuotaOptions() {
    if (quotaOptions_)
      return &quotaOptions_->getHbaseOptions();
    else
      return NULL;
  }

  // for processing
  //  ExprNode * bindNode(BindWA *bindWAPtr);

 private:
  NAString namespace_;
  ComNamespaceOper oper_;
  // for create, indicates "if not exists" clause is specified
  // for drop,   indicates "if exists" clause is specified
  NABoolean existsClause_;

  ElemDDLHbaseOptions *quotaOptions_;
};  // class StmtDDLNamespace

#endif  // STMTDDLNAMESPACE_H
