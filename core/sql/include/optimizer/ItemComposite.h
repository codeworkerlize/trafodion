
#ifndef ITEMCOMPOSITE_H
#define ITEMCOMPOSITE_H
/* -*-C++-*-
******************************************************************************
*
* File:         ItemComposite.h
* Description:  Composite item expressions
*
* Created:
* Language:     C++
*
******************************************************************************
*/

#include "optimizer/ItemExpr.h"
#include "optimizer/ItemFunc.h"

////////////////////////////////////////////////////////////////////
// class CompositeArrayLength
// Returns number of direct array elements.
////////////////////////////////////////////////////////////////////
class CompositeArrayLength : public BuiltinFunction {
 public:
  CompositeArrayLength(ItemExpr *valPtr)
      : BuiltinFunction(ITM_COMPOSITE_ARRAY_LENGTH, CmpCommon::statementHeap(), 1, valPtr) {}

  // virtual destructor
  virtual ~CompositeArrayLength(){};

  virtual NABoolean isCacheableExpr(CacheWA &cwa);

  // get the degree of this node
  virtual int getArity() const { return 1; }

  virtual ItemExpr *bindNode(BindWA *bindWA);

  // method to do code generation
  virtual short codeGen(Generator *);

  // a virtual function for type propagating the node
  virtual const NAType *synthesizeType();

  virtual ItemExpr *copyTopNode(ItemExpr *derivedNode = NULL, CollHeap *outHeap = 0);

  // get a printable string that identifies the operator
  const NAString getText() const;

 private:
};  // class CompositeArrayLength

//////////////////////////////////////////////
// class CompositeCreate
// Created when   ARRAY(list_of_values) or ROW(list_of_values)
// is specified.
//////////////////////////////////////////////
class CompositeCreate : public ItemExpr {
 public:
  enum { ARRAY_TYPE = 1, ROW_TYPE = 2 };

  CompositeCreate(ItemExpr *valPtr, short type) : ItemExpr(ITM_COMPOSITE_CREATE, valPtr), type_(type) {}

  // virtual destructor
  virtual ~CompositeCreate(){};

  virtual NABoolean isCacheableExpr(CacheWA &cwa) { return FALSE; };

  // get the degree of this node
  virtual int getArity() const { return 1; }

  // method to do precode generation
  virtual ItemExpr *bindNode(BindWA *bindWA);

  // virtual method to fixup tree for code generation.
  virtual ItemExpr *preCodeGen(Generator *);

  // method to do code generation
  virtual short codeGen(Generator *);

  // a virtual function for type propagating the node
  virtual const NAType *synthesizeType();

  virtual ItemExpr *copyTopNode(ItemExpr *derivedNode = NULL, CollHeap *outHeap = 0);

  // get a printable string that identifies the operator
  const NAString getText() const { return NAString("composite_create") + (type_ == ARRAY_TYPE ? "(array)" : "(row)"); }

  ValueIdList &elementsVIDlist() { return elemVidList_; }

  short getType() { return type_; }

 private:
  short type_;
  ValueIdList elemVidList_;
};  // class CompositeCreate

////////////////////////////////////////////////////////
// class CompositeDisplay
// User to display composite array or row structures.
// Array display format:  [ elem1, elem2, ... elemN ]
// Row display format:    ( elem1, elem2, ... ekemN ]
//  elem may be primitive or composite.
//
// primitive elements are displayed in their native format
// with truncated leading and trailing spaces.
//
//  For ex:  composite array of row will be displayed as:
//           [ (row1), (row2...) ]
//
////////////////////////////////////////////////////////
class CompositeDisplay : public ItemExpr {
 public:
  CompositeDisplay(ItemExpr *valPtr, NABoolean isInternal = TRUE) : ItemExpr(ITM_COMPOSITE_DISPLAY, valPtr) {}

  // virtual destructor
  virtual ~CompositeDisplay(){};

  // get the degree of this node
  virtual int getArity() const { return 1; }

  // method to do precode generation
  virtual ItemExpr *bindNode(BindWA *bindWA);

  // method to do code generation
  virtual short codeGen(Generator *);

  // a virtual function for type propagating the node
  virtual const NAType *synthesizeType();

  virtual ItemExpr *copyTopNode(ItemExpr *derivedNode = NULL, CollHeap *outHeap = 0);

  // get a printable string that identifies the operator
  const NAString getText() const { return "composite_display"; }

  const NABoolean isInternal() { return isInternal_; }

 private:
  // created internally and not as an explicit user function
  NABoolean isInternal_;
};  // class CompositeDisplay

////////////////////////////////////////////////////////////////////
// class CompositeCast
// Created when CAST of array to array, or row to row is done.
////////////////////////////////////////////////////////////////////
class CompositeCast : public ItemExpr {
 public:
  CompositeCast(ItemExpr *valPtr, const NAType *type, NABoolean isExtendOrTruncate = FALSE)
      : ItemExpr(ITM_COMPOSITE_CAST, valPtr), type_(type), isExtendOrTruncate_(isExtendOrTruncate) {}

  // virtual destructor
  virtual ~CompositeCast(){};

  // get the degree of this node
  virtual int getArity() const { return 1; }

  ItemExpr *arrayExtendOrTruncate(BindWA *bindWA);

  // method to do precode generation
  virtual ItemExpr *bindNode(BindWA *bindWA);

  // method to do code generation
  virtual short codeGen(Generator *);

  // a virtual function for type propagating the node
  virtual const NAType *synthesizeType();

  virtual ItemExpr *copyTopNode(ItemExpr *derivedNode = NULL, CollHeap *outHeap = 0);

  // get a printable string that identifies the operator
  const NAString getText() const;

 protected:
  // type of target type.
  const NAType *type_;

  NABoolean isExtendOrTruncate_;
  NABoolean isTruncate_;
};  // class CompositeCast

////////////////////////////////////////////////////////////////////
// class CompositeCastForHive
// Created when underlying hive composite value is to be converted
// to or from traf composite type.
////////////////////////////////////////////////////////////////////
class CompositeHiveCast : public CompositeCast {
 public:
  CompositeHiveCast(ItemExpr *valPtr, const NAType *type, NABoolean fromHive)
      : CompositeCast(valPtr, type), fromHive_(fromHive) {
    setOperatorType(ITM_COMPOSITE_HIVE_CAST);
  }

  // virtual destructor
  virtual ~CompositeHiveCast(){};

  // method to do precode generation
  virtual ItemExpr *bindNode(BindWA *bindWA);

  // method to do code generation
  virtual short codeGen(Generator *);

  virtual ItemExpr *copyTopNode(ItemExpr *derivedNode = NULL, CollHeap *outHeap = 0);

  // get a printable string that identifies the operator
  const NAString getText() const;

 private:
  // if TRUE, convert hive to traf. If FALSE, convert traf to hive.
  NABoolean fromHive_;
};  // class CompositeHiveCast

////////////////////////////////////////////////////////////////////
// class CompositeConcat
//   Concatenation of 2 arrays.
////////////////////////////////////////////////////////////////////
class CompositeConcat : public BuiltinFunction {
 public:
  CompositeConcat(ItemExpr *val1Ptr, ItemExpr *val2Ptr)
      : BuiltinFunction(ITM_COMPOSITE_CONCAT, CmpCommon::statementHeap(), 2, val1Ptr, val2Ptr), concatExpr_(NULL) {}

  // virtual destructor
  virtual ~CompositeConcat(){};

  virtual NABoolean isCacheableExpr(CacheWA &cwa);

  // method to do precode generation
  virtual ItemExpr *bindNode(BindWA *bindWA);

  // method to do code generation
  virtual short codeGen(Generator *);

  // a virtual function for type propagating the node
  virtual const NAType *synthesizeType();

  virtual ItemExpr *copyTopNode(ItemExpr *derivedNode = NULL, CollHeap *outHeap = 0);

  // get a printable string that identifies the operator
  const NAString getText() const { return "composite_concat"; }

  ItemExpr *concatExpr() { return concatExpr_; }

 private:
  ItemExpr *concatExpr_;
};  // class CompositeConcat

////////////////////////////////////////////////////////////////////
// class CompositeExtract
////////////////////////////////////////////////////////////////////
class CompositeExtract : public BuiltinFunction {
 public:
  // extract elem number elemNum from the composite array or row operand.
  CompositeExtract(ItemExpr *valPtr, int elemNum)
      : BuiltinFunction(ITM_COMPOSITE_EXTRACT, CmpCommon::statementHeap(), 1, valPtr),
        elemNum_(elemNum),
        names_(CmpCommon::statementHeap()),
        indexes_(CmpCommon::statementHeap()),
        resultType_(NULL),
        attrIndexList_(CmpCommon::statementHeap()),
        attrTypeList_(CmpCommon::statementHeap()) {}

  // extract elem specified as a.b[1].c...
  // See arrayIndexList_ and arrayTypeList_ below.
  CompositeExtract(ItemExpr *valPtr, NAList<NAString> &names, NAList<UInt32> &indexes)
      : BuiltinFunction(ITM_COMPOSITE_EXTRACT, CmpCommon::statementHeap(), 1, valPtr),
        names_(names, CmpCommon::statementHeap()),
        indexes_(indexes, CmpCommon::statementHeap()),
        elemNum_(-1),
        resultType_(NULL),
        attrIndexList_(CmpCommon::statementHeap()),
        attrTypeList_(CmpCommon::statementHeap()) {}

  // extract elem number specified through elemNumExpr.
  CompositeExtract(ItemExpr *valPtr, ItemExpr *elemNumExpr)
      : BuiltinFunction(ITM_COMPOSITE_EXTRACT, CmpCommon::statementHeap(), 2, valPtr, elemNumExpr),
        elemNum_(-1),
        names_(CmpCommon::statementHeap()),
        indexes_(CmpCommon::statementHeap()),
        resultType_(NULL),
        attrIndexList_(CmpCommon::statementHeap()),
        attrTypeList_(CmpCommon::statementHeap()) {}

  // virtual destructor
  virtual ~CompositeExtract(){};

  // currently not cacheable.
  virtual NABoolean isCacheableExpr(CacheWA &cwa);

  // method to do precode generation
  virtual ItemExpr *bindNode(BindWA *bindWA);

  // method to do code generation
  virtual short codeGen(Generator *);

  // a virtual function for type propagating the node
  virtual const NAType *synthesizeType();

  virtual ItemExpr *copyTopNode(ItemExpr *derivedNode = NULL, CollHeap *outHeap = 0);

  // get a printable string that identifies the operator
  const NAString getText() const;

 private:
  // element to be extracted. 1-based (first elem is 1)
  int elemNum_;

  const NAType *resultType_;

  NAList<NAString> names_;
  NAList<UInt32> indexes_;

  // each entry represents the index/type into the corresponding composite
  // attribute list. Used to find out the value to be extracted.
  // For ex: {b1}[3].b10  where b1 is row and b10 is char, will have 2 entries.
  //         First entry will be 3,221(REC_ROW) and second will be 0,0(ASCII_F)
  NAList<int> attrIndexList_;
  NAList<int> attrTypeList_;
};  // class CompositeExtract

#endif
