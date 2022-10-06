#ifndef ELEMDDLSALTOPTIONS_H
#define ELEMDDLSALTOPTIONS_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLSaltOptions.h
 * Description:  Classes representing SALT BY clause specified in
 *               create DDL for HBase objects
 *
 *
 * Created:      8/27/2013
 * Language:     C++
 *
 *

 *
 *
 *****************************************************************************
 */

#include "ElemDDLNode.h"
#include "ParNameLocList.h"
#include "common/ComSmallDefs.h"
#include "parser/ElemDDLColRefArray.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLSaltOptionsClause;

// -----------------------------------------------------------------------
// class ElemDDLSaltOptionsClause
// -----------------------------------------------------------------------

class ElemDDLSaltOptionsClause : public ElemDDLNode {
 public:
  //
  // constructors
  //

  ElemDDLSaltOptionsClause(ElemDDLNode *pSaltExprTree, int numPartitions, int numRegions = -1);

  ElemDDLSaltOptionsClause(NABoolean likeTable);

  // virtual destructor
  virtual ~ElemDDLSaltOptionsClause();

  // casting
  virtual ElemDDLSaltOptionsClause *castToElemDDLSaltOptionsClause();

  //
  // accessors
  //

  inline int getNumPartitions() const { return numPartitions_; }

  inline int getNumInitialRegions() const { return numInitialRegions_; }

  inline ElemDDLColRefArray &getSaltColRefArray() { return saltColumnArray_; }

  // get the degree of this node
  virtual int getArity() const;
  virtual ExprNode *getChild(int index);
  // name we choose for the system column that contains the salting value
  static const char *getSaltSysColName() { return "_SALT_"; }

  //
  // mutators
  //

  void setChild(int index, ExprNode *pChildNode);

  void setNumPartns(int numPartns) { numPartitions_ = numPartns; }

  //
  // methods for tracing and/or building text
  //

  virtual const NAString getText() const;

  NABoolean getLikeTable() const;

  void unparseIt(NAString &result) const;

 private:
  //
  // Private methods
  //

  ElemDDLSaltOptionsClause(const ElemDDLSaltOptionsClause &rhs);             // not defined - DO NOT USE
  ElemDDLSaltOptionsClause &operator=(const ElemDDLSaltOptionsClause &rhs);  // not defined - DO NOT USE

  //
  // data members
  //

  int numPartitions_;
  int numInitialRegions_;
  ElemDDLColRefArray saltColumnArray_;
  NABoolean likeTable_;  // salt an index like its base table

  // pointers to child parse node

  enum { INDEX_SALT_COLUMN_LIST = 0, MAX_ELEM_DDL_SALT_OPT_KEY_COLUMN_LIST_ARITY };

  ElemDDLNode *children_[MAX_ELEM_DDL_SALT_OPT_KEY_COLUMN_LIST_ARITY];

};  // class ElemDDLSaltOptionsClause

#endif  // ELEMDDLSALTOPTIONSCLAUSE_H
