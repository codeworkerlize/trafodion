#ifndef ELEMDDLCOLREF_H
#define ELEMDDLCOLREF_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLColRef.h
 * Description:  class for Column Name and Ordering Specification
 *               elements in DDL statements
 *
 *
 * Created:      4/21/95
 * Language:     C++
 *
 *

 *
 *
 *****************************************************************************
 */

#include "parser/ElemDDLNode.h"
#include "common/ComSmallDefs.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLColRef;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// Parse node containing Column Name and Ordering Specification
// information in DDL statements
// -----------------------------------------------------------------------
class ElemDDLColRef : public ElemDDLNode {
 public:
  // constructors
  ElemDDLColRef(ComColumnOrdering colOrdering = COM_UNKNOWN_ORDER, CollHeap *h = 0)
      : ElemDDLNode(ELM_COL_REF_ELEM),
        columnName_(h),
        divisionExpr_(NULL),
        divisionKeySeqNum_(0),
        columnOrdering_(colOrdering) {
    if (colOrdering == COM_UNKNOWN_ORDER) {  // Mark it as not user-specified and override it with COM_ASCENDING_ORDER.
      userSpecifiedColumnOrdering_ = FALSE;
      columnOrdering_ = COM_ASCENDING_ORDER;
    } else
      // Mark it as user-specified.
      userSpecifiedColumnOrdering_ = TRUE;
  }

  ElemDDLColRef(const NAString &colName, ComColumnOrdering colOrdering = COM_UNKNOWN_ORDER, CollHeap *h = 0)
      : ElemDDLNode(ELM_COL_REF_ELEM),
        columnName_(colName, h),
        divisionExpr_(NULL),
        divisionKeySeqNum_(0),
        columnOrdering_(colOrdering) {
    if (colOrdering == COM_UNKNOWN_ORDER) {  // Mark it as not user-specified and override it with COM_ASCENDING_ORDER.
      userSpecifiedColumnOrdering_ = FALSE;
      columnOrdering_ = COM_ASCENDING_ORDER;
    } else
      // Mark it as user-specified.
      userSpecifiedColumnOrdering_ = TRUE;
  }

  // copy ctor
  ElemDDLColRef(const ElemDDLColRef &orig, CollHeap *h = 0);  // not written

  // virtual destructor
  virtual ~ElemDDLColRef();

  // cast
  virtual ElemDDLColRef *castToElemDDLColRef();

  // accessors
  inline const NAString &getColumnName() const;
  inline ComColumnOrdering getColumnOrdering() const;
  NAString getColumnOrderingAsNAString() const;
  NABoolean isColumnOrderingSpecified() const;
  inline ItemExpr *getDivisionExpression() { return divisionExpr_; }
  inline ComSInt32 getDivisionKeySequenceNumber() const { return divisionKeySeqNum_; }

  // mutator
  inline void setColumnName(const NAString &columnName);
  inline void setDivisionExpression(ItemExpr *pDivExpr) { divisionExpr_ = pDivExpr; }
  inline void setDivisionKeySequenceNumber(ComSInt32 seqNum) {
#ifdef NA_SQ_SMD_DIV_COL
    divisionKeySeqNum_ = seqNum;
#endif
  }
  inline void setColumnOrdering(ComColumnOrdering eSortingOrder) { columnOrdering_ = eSortingOrder; }

  // member functions for tracing
  virtual const NAString getText() const;
  virtual const NAString displayLabel1() const;
  virtual const NAString displayLabel2() const;

 private:
  NAString columnName_;
  ComColumnOrdering columnOrdering_;
  NABoolean userSpecifiedColumnOrdering_;
  ItemExpr *divisionExpr_;
  ComSInt32 divisionKeySeqNum_;

};  // class ElemDDLColRef

// -----------------------------------------------------------------------
// definitions of inline methods of class ElemDDLColRef
// -----------------------------------------------------------------------

//
// accessors
//

inline const NAString &ElemDDLColRef::getColumnName() const { return columnName_; }

inline ComColumnOrdering ElemDDLColRef::getColumnOrdering() const { return columnOrdering_; }

inline NABoolean ElemDDLColRef::isColumnOrderingSpecified() const { return userSpecifiedColumnOrdering_; }

//
// mutator
//

inline void ElemDDLColRef::setColumnName(const NAString &columnName) { columnName_ = columnName; }

#endif  // ELEMDDLCOLREF_H
