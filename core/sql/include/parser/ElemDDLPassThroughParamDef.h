/* -*-C++-*- */

#ifndef ELEMDDLPASSTHROUGHPARAMDEF_H
#define ELEMDDLPASSTHROUGHPARAMDEF_H
/* -*-C++-*-
******************************************************************************
*
* File:         ElemDDLPassThroughParamDef.h
* Description:  class for UDF Pass Through parameters in DDL statements
*
*
* Created:      7/14/09
* Language:     C++
*
*
*
*
******************************************************************************
*/

#include "ElemDDLNode.h"
#include "ElemDDLParamName.h"
#include "common/ComSmallDefs.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLPassThroughParamDef;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
class ItemExpr;

// -----------------------------------------------------------------------
// Pass Through Param Definition elements in DDL statements.
// -----------------------------------------------------------------------
class ElemDDLPassThroughParamDef : public ElemDDLNode {
 public:
  enum EPassThroughParamDefKind { eADD_PASS_THROUGH_INPUT, eALTER_PASS_THROUGH_INPUT };

  // default constructor
  ElemDDLPassThroughParamDef(CollHeap *heap = PARSERHEAP());
  // other constructors
  ElemDDLPassThroughParamDef(ItemExpr *passThroughValueExpr, CollHeap *heap = PARSERHEAP());
  ElemDDLPassThroughParamDef(const NAString &fileOssPathName, CollHeap *heap = PARSERHEAP());

  // virtual destructor
  virtual ~ElemDDLPassThroughParamDef();

  // cast
  virtual ElemDDLPassThroughParamDef *castToElemDDLPassThroughParamDef();

  //
  // accessors
  //

  virtual int getArity() const;
  virtual ExprNode *getChild(long index);

  inline ItemExpr *getPassThroughValueExpr() const;
  inline const NAString &getParamName() const;
  inline const ComSInt32 getParamIndex() const;
  inline const ComSInt32 getParamPosition() const { return getParamIndex(); }
  inline const NABoolean isParamIndexSpecified() const;
  inline const ComParamDirection getParamDirection(void) const;

  inline const NAString &getFileOssPathName() const;
  enum EFileContentFormat { eFILE_CONTENT_FORMAT_BINARY, eFILE_CONTENT_FORMAT_TEXT };
  inline const EFileContentFormat getFileContentFormat() const;

  inline const ComRoutinePassThroughInputType getPassThroughInputType() const;

  inline EPassThroughParamDefKind getPassThroughParamDefKind() const;

  virtual const NAString displayLabel1() const;
  virtual const NAString displayLabel2() const;
  virtual const NAString getText() const;

  //
  // mutators
  //

  inline void setParamIndex(unsigned int paramIndex);
  inline void setParamPosition(unsigned int paramPos) { setParamIndex(paramPos); }
  inline void setParamDirection(ComParamDirection direction);
  inline void setParamName(ElemDDLParamName *pParamNameNode);
  inline void setFileContentFormat(EFileContentFormat format);
  inline void setPassThroughInputType(ComRoutinePassThroughInputType type);
  inline void setPassThroughParamDefKind(EPassThroughParamDefKind kind);

 private:
  // copy contructor
  ElemDDLPassThroughParamDef(const ElemDDLPassThroughParamDef &orig, CollHeap *h = 0);  // not written

  // ---------------------------------------------------------------------
  // private data members
  // ---------------------------------------------------------------------

  EPassThroughParamDefKind passThroughParamDefKind_;
  ComParamDirection paramDirection_;  // COM_INPUT_PARAM
  ComSInt32 paramIndex_;
  NABoolean isParamIndexSpec_;
  NAString paramName_;  // an empty string - not yet supported
  ItemExpr *passThroughValueExpr_;
  NAString fileOssPathName_;
  EFileContentFormat fileContentFormat_;  // eFILE_CONTENT_FORMAT_BINARY
  ComRoutinePassThroughInputType passThroughInputType_;

};  // class ElemDDLPassThroughParamDef

// -----------------------------------------------------------------------
// definitions of inline methods for class ElemDDLPassThroughParamDef
// -----------------------------------------------------------------------

inline const ComSInt32 ElemDDLPassThroughParamDef::getParamIndex() const { return paramIndex_; }

inline const NABoolean ElemDDLPassThroughParamDef::isParamIndexSpecified() const { return isParamIndexSpec_; }

inline const NAString &ElemDDLPassThroughParamDef::getParamName() const { return paramName_; }

inline ItemExpr *ElemDDLPassThroughParamDef::getPassThroughValueExpr() const { return passThroughValueExpr_; }

inline const NAString &ElemDDLPassThroughParamDef::getFileOssPathName() const { return fileOssPathName_; }

inline const ElemDDLPassThroughParamDef::EFileContentFormat ElemDDLPassThroughParamDef::getFileContentFormat() const {
  return fileContentFormat_;
}

inline const ComParamDirection ElemDDLPassThroughParamDef::getParamDirection() const { return paramDirection_; }

inline const ComRoutinePassThroughInputType ElemDDLPassThroughParamDef::getPassThroughInputType() const {
  return passThroughInputType_;
}

inline ElemDDLPassThroughParamDef::EPassThroughParamDefKind ElemDDLPassThroughParamDef::getPassThroughParamDefKind()
    const {
  return passThroughParamDefKind_;
}

//
// mutators
//

inline void ElemDDLPassThroughParamDef::setParamIndex(unsigned int paramIndex) {
  paramIndex_ = static_cast<ComSInt32>(paramIndex);
  isParamIndexSpec_ = TRUE;
}

inline void ElemDDLPassThroughParamDef::setParamName(ElemDDLParamName *pParamNameNode) {
  if (pParamNameNode NEQ NULL)
    paramName_ = pParamNameNode->getParamName();
  else
    paramName_ = "";
}

inline void ElemDDLPassThroughParamDef::setFileContentFormat(ElemDDLPassThroughParamDef::EFileContentFormat format) {
  fileContentFormat_ = format;
}

inline void ElemDDLPassThroughParamDef::setParamDirection(ComParamDirection direction) { paramDirection_ = direction; }

inline void ElemDDLPassThroughParamDef::setPassThroughInputType(ComRoutinePassThroughInputType type) {
  passThroughInputType_ = type;
}

inline void ElemDDLPassThroughParamDef::setPassThroughParamDefKind(
    ElemDDLPassThroughParamDef::EPassThroughParamDefKind kind) {
  passThroughParamDefKind_ = kind;
}

#endif  // ELEMDDLPASSTHROUGHPARAMDEF_H
