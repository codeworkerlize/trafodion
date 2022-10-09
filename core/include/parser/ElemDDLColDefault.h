
#ifndef ELEMDDLCOLDEFAULT_H
#define ELEMDDLCOLDEFAULT_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLColDefault.h
 * Description:  class for Column Default Value elements in DDL statements
 *
 *
 * Created:      4/12/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "parser/ElemDDLNode.h"
#include "optimizer/ItemExpr.h"
#include "parser/ElemDDLSGOptions.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLColDefault;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// Column Default Value elements in DDL statements
// -----------------------------------------------------------------------
class ElemDDLColDefault : public ElemDDLNode {
 public:
  enum colDefaultTypeEnum { COL_NO_DEFAULT, COL_DEFAULT, COL_FUNCTION_DEFAULT, COL_COMPUTED_DEFAULT };

  // default constructor
  ElemDDLColDefault(colDefaultTypeEnum columnDefaultType = COL_NO_DEFAULT, ItemExpr *defaultValueExpr = NULL,
                    ElemDDLSGOptions *sgOptions = NULL)
      : ElemDDLNode(ELM_COL_DEFAULT_ELEM),
        columnDefaultType_(columnDefaultType),
        defaultValueExpr_(defaultValueExpr),
        sgOptions_(sgOptions),
        defaultExprString_(""),
        sgLocation_(NULL) {}

  // virtual destructor
  virtual ~ElemDDLColDefault();

  // cast
  virtual ElemDDLColDefault *castToElemDDLColDefault();

  //
  // accessors
  //

  virtual int getArity() const;
  virtual ExprNode *getChild(int index);

  inline colDefaultTypeEnum getColumnDefaultType() const;

  inline ItemExpr *getDefaultValueExpr() const;
  inline const NAString &getDefaultExprString() const;

  // returns the pointer to the (only) child parse node
  // pointing to an ItemExpr node representing the
  // specified default value.

  inline ElemDDLSGOptions *getSGOptions() const;
  inline NAString *getSGLocation() const;
  inline const NAString &getComputedDefaultExpr() const;

  // mutator
  virtual void setChild(int index, ExprNode *pChildNode);
  inline void setDefaultValueExpr(ItemExpr *pDefaultValueExpr);
  inline void setDefaultExprString(const NAString &str);
  inline void setSGOptions(ElemDDLSGOptions *pSGOptions);
  inline void setSGLocation(NAString *pLocation);
  inline void setComputedDefaultExpr(const NAString &computedDefaultExpr);

  // methods for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString displayLabel2() const;
  virtual const NAString getText() const;

 private:
  colDefaultTypeEnum columnDefaultType_;

  // pointer to child parse node

  enum { INDEX_DEFAULT_VALUE_EXPR = 0, MAX_ELEM_DDL_COL_DEFAULT_ARITY };

  ItemExpr *defaultValueExpr_;
  NAString computedDefaultExpr_;
  NAString defaultExprString_;

  ElemDDLSGOptions *sgOptions_;
  NAString *sgLocation_;

};  // class ElemDDLColDefault

// -----------------------------------------------------------------------
// definitions of inline methods for class ElemDDLColDefault
// -----------------------------------------------------------------------

//
// accessors
//

inline ElemDDLColDefault::colDefaultTypeEnum ElemDDLColDefault::getColumnDefaultType() const {
  return columnDefaultType_;
}

inline ItemExpr *ElemDDLColDefault::getDefaultValueExpr() const { return defaultValueExpr_; }

inline void ElemDDLColDefault::setDefaultValueExpr(ItemExpr *pDefaultValueExpr) {
  setChild(INDEX_DEFAULT_VALUE_EXPR, pDefaultValueExpr);
}

inline void ElemDDLColDefault::setDefaultExprString(const NAString &str) { defaultExprString_ = str; }

inline void ElemDDLColDefault::setSGOptions(ElemDDLSGOptions *pSGOptions) { sgOptions_ = pSGOptions; }

inline void ElemDDLColDefault::setSGLocation(NAString *pLocation) { sgLocation_ = pLocation; }

inline void ElemDDLColDefault::setComputedDefaultExpr(const NAString &computedDefaultExpr) {
  computedDefaultExpr_ = computedDefaultExpr;
}

inline ElemDDLSGOptions *ElemDDLColDefault::getSGOptions() const { return sgOptions_; }

inline NAString *ElemDDLColDefault::getSGLocation() const { return sgLocation_; }

inline const NAString &ElemDDLColDefault::getComputedDefaultExpr() const { return computedDefaultExpr_; }

inline const NAString &ElemDDLColDefault::getDefaultExprString() const { return defaultExprString_; }
#endif  // ELEMDDLCOLDEFAULT_H
