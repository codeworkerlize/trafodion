#ifndef STMTDDLCREATESYNONYM_H
#define STMTDDLCREATESYNONYM_H
/*
******************************************************************************
*
* File:          StmtDDLCreateSynonym.h
* RCS:           $Id:
* Description:   class for parse node representing create synonym
*                statement.
*
*
* Created:       01/27/06
* Language:      C++
*
*
*

*
******************************************************************************
*/
#include "common/ComSmallDefs.h"
#include "parser/StmtDDLNode.h"

class StmtDDLCreateSynonym : public StmtDDLNode

{
 public:
  // constructor

  StmtDDLCreateSynonym();
  StmtDDLCreateSynonym(const QualifiedName &synonymname, const QualifiedName &objectreference, ElemDDLNode *pOwner);

  // Virtual Destructor
  virtual ~StmtDDLCreateSynonym();

  // Cast

  virtual StmtDDLCreateSynonym *castToStmtDDLCreateSynonym();

  //
  // method for binding
  //

  ExprNode *bindNode(BindWA *bindWAPtr);

  // accessors

  inline const NAString getSynonymName() const;
  inline const NAString getObjectReference() const;

  inline const NABoolean isOwnerSpecified() const;
  inline const ElemDDLGrantee *getOwner() const;

  // for tracing

  virtual const NAString displayLabel1() const;
  virtual const NAString displayLabel2() const;
  virtual const NAString getText() const;

  // mutator

 private:
  QualifiedName synonymName_;
  QualifiedName objectReference_;
  ElemDDLGrantee *pOwner_;
};

//----------------------------------------------------------------------------
// definitions of inline methods for class StmtDDLCreateSynonym
//----------------------------------------------------------------------------

//
// accessors
//

inline const NAString StmtDDLCreateSynonym::getSynonymName() const {
  NAString synonymName = synonymName_.getQualifiedNameAsAnsiString();
  return synonymName;
}

inline const NAString StmtDDLCreateSynonym::getObjectReference() const {
  NAString objectReference = objectReference_.getQualifiedNameAsAnsiString();
  return objectReference;
}

inline const NABoolean StmtDDLCreateSynonym::isOwnerSpecified() const { return pOwner_ ? TRUE : FALSE; }

inline const ElemDDLGrantee *StmtDDLCreateSynonym::getOwner() const { return pOwner_; }

#endif  // STMTDDLCREATESYNONYM_H
