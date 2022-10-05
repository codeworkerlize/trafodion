#ifndef STMTDDLALTERSYNONYM_H
#define STMTDDLALTERSYNONYM_H
/*
******************************************************************************
*
* File:          StmtDDLSynonym.h
* RCS:           $Id:
* Description:   class for parse node representing ALTER SYNONYM statement.
*
* Created:       1/27/2006
* Language:      C++
*
*
*

******************************************************************************
*/
#include "common/ComSmallDefs.h"
#include "StmtDDLNode.h"

class StmtDDLAlterSynonym : public StmtDDLNode

{
 public:
  // constructor

  StmtDDLAlterSynonym();
  StmtDDLAlterSynonym(const QualifiedName &synonymName, const QualifiedName &objectReference);

  // Virtual Destructor
  virtual ~StmtDDLAlterSynonym();

  // Cast

  virtual StmtDDLAlterSynonym *castToStmtDDLAlterSynonym();

  //
  // method for binding
  //

  ExprNode *bindNode(BindWA *bindWAPtr);

  // accessors

  inline const NAString getSynonymName() const;
  inline const NAString getObjectReference() const;

  // for tracing

  virtual const NAString displayLabel1() const;
  virtual const NAString displayLabel2() const;
  virtual const NAString getText() const;

 private:
  QualifiedName synonymName_;
  QualifiedName objectReference_;
};

//----------------------------------------------------------------------------
// definitions of inline methods for class StmtDDLSynonym
//----------------------------------------------------------------------------

//
// accessors
//

inline const NAString StmtDDLAlterSynonym::getSynonymName() const {
  NAString synonymName = synonymName_.getQualifiedNameAsAnsiString();
  return synonymName;
}

inline const NAString StmtDDLAlterSynonym::getObjectReference() const {
  NAString objectReference = objectReference_.getQualifiedNameAsAnsiString();
  return objectReference;
}

#endif  // STMTDDLALTERSYNONYM
