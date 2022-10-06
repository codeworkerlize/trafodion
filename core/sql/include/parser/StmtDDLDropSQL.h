
#ifndef STMTDDLDROPSQL_H
#define STMTDDLDROPSQL_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLDropSQL.h
 * Description:  class for parse node representing Drop SQL statements
 *
 *
 * Created:      03/29/96
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "common/ComSmallDefs.h"
#include "parser/StmtDDLNode.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLDropSQL;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None

// -----------------------------------------------------------------------
// Drop SQL  statement
// -----------------------------------------------------------------------
class StmtDDLDropSQL : public StmtDDLNode {
 public:
  // constructor
  StmtDDLDropSQL(ComDropBehavior dropBehavior);

  // virtual destructor
  virtual ~StmtDDLDropSQL();

  // cast
  virtual StmtDDLDropSQL *castToStmtDDLDropSQL();

  // accessor
  inline ComDropBehavior getDropBehavior() const;

  // for tracing
  virtual const NAString getText() const;

 private:
  ComDropBehavior dropBehavior_;

};  // class StmtDDLDropSQL

// -----------------------------------------------------------------------
// definitions of inline methods for class StmtDDLDropSQL
// -----------------------------------------------------------------------
inline ComDropBehavior StmtDDLDropSQL::getDropBehavior() const { return dropBehavior_; }

#endif  // STMTDDLDROPSQL_H
