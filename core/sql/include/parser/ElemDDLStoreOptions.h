
#ifndef ELEMDDLSTOREOPTIONS_H
#define ELEMDDLSTOREOPTIONS_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLStoreOptions.h
 * Description:  classes representing Store By options specified in
 *               Create Table DDL statements
 *
 *
 * Created:      10/30/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "parser/ElemDDLColRefArray.h"
#include "ElemDDLNode.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLStoreOpt;
class ElemDDLStoreOptDefault;
class ElemDDLStoreOptEntryOrder;
class ElemDDLStoreOptKeyColumnList;
class ElemDDLStoreOptNondroppablePK;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None

// -----------------------------------------------------------------------
// definition of base class ElemDDLStoreOpt
// -----------------------------------------------------------------------
class ElemDDLStoreOpt : public ElemDDLNode {
 public:
  // default constructor
  ElemDDLStoreOpt(OperatorTypeEnum operatorType = ELM_ANY_STORE_OPT_ELEM)
      : ElemDDLNode(operatorType), isNullableSpecified_(FALSE) {}

  // virtual destructor
  virtual ~ElemDDLStoreOpt();

  // cast
  virtual ElemDDLStoreOpt *castToElemDDLStoreOpt();

  // methods for tracing
  virtual NATraceList getDetailInfo() const;
  virtual const NAString getText() const;

  NABoolean isNullableSpecified() { return isNullableSpecified_; }
  void setIsNullableSpecified(NABoolean isNullable) { isNullableSpecified_ = isNullable; }

 private:
  NABoolean isNullableSpecified_;  // if set, store by key is nullable

};  // class ElemDDLStoreOpt

// -----------------------------------------------------------------------
// definition of class ElemDDLStoreOptDefault
// -----------------------------------------------------------------------
class ElemDDLStoreOptDefault : public ElemDDLStoreOpt {
 public:
  // constructor
  ElemDDLStoreOptDefault() : ElemDDLStoreOpt(ELM_STORE_OPT_DEFAULT_ELEM) {}

  // virtual destructor
  virtual ~ElemDDLStoreOptDefault();

  // cast
  virtual ElemDDLStoreOptDefault *castToElemDDLStoreOptDefault();

  // methods for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString getText() const;

 private:
};  // class ElemDDLStoreOptDefault

// -----------------------------------------------------------------------
// definition of class ElemDDLStoreOptEntryOrder
// -----------------------------------------------------------------------
class ElemDDLStoreOptEntryOrder : public ElemDDLStoreOpt {
 public:
  // constructor
  ElemDDLStoreOptEntryOrder() : ElemDDLStoreOpt(ELM_STORE_OPT_ENTRY_ORDER_ELEM) {}

  // virtual destructor
  virtual ~ElemDDLStoreOptEntryOrder();

  // cast
  virtual ElemDDLStoreOptEntryOrder *castToElemDDLStoreOptEntryOrder();

  // methods for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString getText() const;

 private:
};  // class ElemDDLStoreOptEntryOrder

// -----------------------------------------------------------------------
// definition of class ElemDDLStoreOptKeyColumnList
// -----------------------------------------------------------------------
class ElemDDLStoreOptKeyColumnList : public ElemDDLStoreOpt {
 public:
  // constructor
  ElemDDLStoreOptKeyColumnList(ElemDDLNode *pKeyColumnList, NABoolean uniqueStoreBy = FALSE,
                               NABoolean uniqueStoreByKeylist = FALSE, NABoolean pkeyStoreByKeyList = FALSE,
                               CollHeap *heap = PARSERHEAP());

  // virtual destructor
  virtual ~ElemDDLStoreOptKeyColumnList();

  // casting
  virtual ElemDDLStoreOptKeyColumnList *castToElemDDLStoreOptKeyColumnList();

  //
  // accessors
  //

  virtual int getArity() const;

  virtual ExprNode *getChild(int index);

  inline const ElemDDLColRefArray &getKeyColumnArray() const;
  inline ElemDDLColRefArray &getKeyColumnArray();

  inline ElemDDLNode *getKeyColumnList() const;

  inline NABoolean isUniqueStoreBy() { return uniqueStoreBy_; }

  inline NABoolean isUniqueStoreByKeylist() { return uniqueStoreByKeylist_; }

  inline NABoolean isPkeyStoreByKeylist() { return pkeyStoreByKeylist_; }

  NABoolean isPkeyNotSerialized() { return (ser_ == ComPkeySerialization::COM_NOT_SERIALIZED); }
  void setSerializedOption(ComPkeySerialization ser) { ser_ = ser; }
  ComPkeySerialization getSerializedOption() { return ser_; }

  // mutator
  virtual void setChild(int index, ExprNode *pElemDDLNode);

  // methods for tracing
  virtual const NAString displayLabel1() const;
  virtual NATraceList getDetailInfo() const;
  virtual const NAString getText() const;

 private:
  //
  // data members
  //

  ElemDDLColRefArray keyColumnArray_;

  NABoolean uniqueStoreBy_;  // UNIQUE specified for PRIMARY key

  NABoolean uniqueStoreByKeylist_;  // UNIQUE specified for cluster columns

  NABoolean pkeyStoreByKeylist_;  // PRIMARY KEY specified with column list

  // pointers to child parse node

  enum { INDEX_KEY_COLUMN_LIST = 0, MAX_ELEM_DDL_STORE_OPT_KEY_COLUMN_LIST_ARITY };

  ElemDDLNode *children_[MAX_ELEM_DDL_STORE_OPT_KEY_COLUMN_LIST_ARITY];

  // if set to SERIALIZED, then pkey will be encoded before passint to hbase.
  // if set to NOT_SERIALIZED, then primary key will not be encoded before
  // passing on to HBase.
  // Used when accessing external HBase tables where data may not be stored
  // in serialized mode.
  // if not specified, then will be determined based on table type.
  ComPkeySerialization ser_;

};  // class ElemDDLStoreOptKeyColumnList

// -----------------------------------------------------------------------
// definition of class ElemDDLStoreOptNondroppablePK
// -----------------------------------------------------------------------
class ElemDDLStoreOptNondroppablePK : public ElemDDLStoreOpt {
 public:
  // constructor
  ElemDDLStoreOptNondroppablePK(NABoolean uniqueStoreByPrimaryKey = FALSE);

  // virtual destructor

  // virtual destructor
  virtual ~ElemDDLStoreOptNondroppablePK();

  // cast
  virtual ElemDDLStoreOptNondroppablePK *castToElemDDLStoreOptNondroppablePK();

  //
  // accessors
  //

  inline NABoolean isUniqueStoreByPrimaryKey() { return uniqueStoreByPrimaryKey_; }

  // methods for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString getText() const;

 private:
  //
  // data members
  //

  NABoolean uniqueStoreByPrimaryKey_;  // UNIQUE specified for PRIMARY key

};  // class ElemDDLStoreOptNondroppablePK

// -----------------------------------------------------------------------
// definitions of inline methods for class ElemDDLStoreOptKeyColumnList
// -----------------------------------------------------------------------

//
// accessor
//

inline ElemDDLColRefArray &ElemDDLStoreOptKeyColumnList::getKeyColumnArray() { return keyColumnArray_; }

inline const ElemDDLColRefArray &ElemDDLStoreOptKeyColumnList::getKeyColumnArray() const { return keyColumnArray_; }

inline ElemDDLNode *ElemDDLStoreOptKeyColumnList::getKeyColumnList() const { return children_[INDEX_KEY_COLUMN_LIST]; }

#endif  // ELEMDDLSTOREOPTIONS_H
