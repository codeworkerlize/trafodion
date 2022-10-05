
#ifndef STMTDDLCOMMENTON_H
#define STMTDDLCOMMENTON_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLCommentOn.h
 * Description:  class for Comment On Statement (parser node)
 *
 *
 * Created:      8/2/17
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
// forward references
// -----------------------------------------------------------------------
// None

// -----------------------------------------------------------------------
// Comment On statement
// -----------------------------------------------------------------------
class StmtDDLCommentOn : public StmtDDLNode {
 public:
  enum COMMENT_ON_TYPES {
    COMMENT_ON_TYPE_TABLE,
    COMMENT_ON_TYPE_COLUMN,
    COMMENT_ON_TYPE_SCHEMA,
    COMMENT_ON_TYPE_VIEW,
    COMMENT_ON_TYPE_INDEX,
    COMMENT_ON_TYPE_LIBRARY,
    COMMENT_ON_TYPE_PROCEDURE,
    COMMENT_ON_TYPE_FUNCTION,
    COMMENT_ON_TYPE_SEQUENCE,
    COMMENT_ON_TYPE_UNKNOWN
  };

  // (default) constructor
  StmtDDLCommentOn(COMMENT_ON_TYPES objType, const QualifiedName &objName, const NAString &commentStr, CollHeap *heap);
  StmtDDLCommentOn(COMMENT_ON_TYPES objType, const QualifiedName &objName, const NAString &commentStr,
                   ColReference *colRef, CollHeap *heap);

  // virtual destructor
  virtual ~StmtDDLCommentOn();

  // cast
  virtual StmtDDLCommentOn *castToStmtDDLCommentOn();

  // ---------------------------------------------------------------------
  // accessors
  // ---------------------------------------------------------------------

  // methods relating to parse tree
  // virtual int getArity() const;
  // virtual ExprNode * getChild(int index);

  // method for binding
  ExprNode *bindNode(BindWA *bindWAPtr);

  // accessors

  inline QualifiedName &getObjectNameAsQualifiedName();
  inline const QualifiedName &getObjectNameAsQualifiedName() const;

  inline const enum COMMENT_ON_TYPES getObjectType() { return type_; }
  inline const NAString getObjectName() const;
  inline const NAString &getComment() const { return comment_; }
  NAString getCommentEscaped();
  inline const NAString &getColName() const { return colRef_->getColRefNameObj().getColName(); }
  inline NABoolean getIsViewCol() { return isViewCol_; }
  inline int getColNum() { return colNum_; }

  inline int getVersion() { return 1; }

  // for tracing

  /*  virtual const NAString displayLabel1() const;
    virtual const NAString displayLabel2() const;
    virtual NATraceList getDetailInfo() const;
    virtual const NAString getText() const;
  */

 private:
  enum COMMENT_ON_TYPES type_;
  QualifiedName objectName_;
  ColReference *colRef_;
  NABoolean isViewCol_;
  int colNum_;

  const NAString &comment_;

};  // class StmtDDLCreateTable

inline const NAString StmtDDLCommentOn::getObjectName() const {
  NAString objectName = objectName_.getQualifiedNameAsAnsiString();

  return objectName;
}

inline QualifiedName &StmtDDLCommentOn::getObjectNameAsQualifiedName() { return objectName_; }

inline const QualifiedName &StmtDDLCommentOn::getObjectNameAsQualifiedName() const { return objectName_; }

#endif  // STMTDDLCREATETABLE_H
