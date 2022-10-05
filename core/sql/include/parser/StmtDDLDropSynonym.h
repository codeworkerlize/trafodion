#ifndef STMTDDLDROPSYNONYM_H
#define STMTDDLDROPSYNONYM_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLDropSynonym.h
 * Description:  class for parse node representing Drop Synonym statements
 *
 *
 * Created:      01/27/06
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
class StmtDDLDropSynonym;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None

// -----------------------------------------------------------------------
// Drop Synonym statement
// -----------------------------------------------------------------------
class StmtDDLDropSynonym : public StmtDDLNode {
 public:
  // constructor
  StmtDDLDropSynonym();
  StmtDDLDropSynonym(const QualifiedName &synonymName);

  // virtual destructor
  virtual ~StmtDDLDropSynonym();

  // cast
  virtual StmtDDLDropSynonym *castToStmtDDLDropSynonym();

  // accessors

  inline const NAString getSynonymName() const;

  // for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString getText() const;

  // for binding
  ExprNode *bindNode(BindWA *bindWAPtr);

 private:
  QualifiedName synonymName_;

};  // class StmtDDLDropSynonym

// -----------------------------------------------------------------------
// definitions of inline methods for class StmtDDLDropSynonym
// -----------------------------------------------------------------------

//
// accessors
//

inline const NAString StmtDDLDropSynonym::getSynonymName() const {
  NAString synonymName = synonymName_.getQualifiedNameAsAnsiString();
  return synonymName;
}

#endif  // STMTDDLDROPSYNONYM_H
