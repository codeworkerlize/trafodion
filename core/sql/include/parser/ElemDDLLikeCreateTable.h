
#ifndef ELEMDDLLIKECREATETABLE_H
#define ELEMDDLLIKECREATETABLE_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLLikeCreateTable.h
 * Description:  class for Like clause in DDL Create Table statements
 *
 *
 * Created:      6/5/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "ElemDDLLike.h"
#include "ParDDLLikeOptsCreateTable.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLLikeCreateTable;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None

// -----------------------------------------------------------------------
// Definition of class ElemDDLLikeCreateTable
// -----------------------------------------------------------------------
class ElemDDLLikeCreateTable : public ElemDDLLike {
 public:
  // constructor
  ElemDDLLikeCreateTable(const CorrName &sourceTableName = CorrName("", PARSERHEAP()), ElemDDLNode *pLikeOptions = NULL,
                         NABoolean forExtTable = FALSE, CollHeap *h = 0);

  // copy ctor
  ElemDDLLikeCreateTable(const ElemDDLLikeCreateTable &orig,
                         CollHeap *h = 0);  // not written

  // virtual destructor
  virtual ~ElemDDLLikeCreateTable();

  // cast
  virtual ElemDDLLikeCreateTable *castToElemDDLLikeCreateTable();

  // accessors

  const ParDDLLikeOptsCreateTable &getLikeOptions() const { return likeOptions_; }

  ParDDLLikeOptsCreateTable &getLikeOptions() { return likeOptions_; }

  // method for tracing
  virtual const NAString getText() const;

  inline const QualifiedName &getDDLLikeCreateTableNameAsQualifiedName() const;
  inline QualifiedName &getDDLLikeCreateTableNameAsQualifiedName();

  const NABoolean forExtTable() const { return forExtTable_; }

 private:
  ParDDLLikeOptsCreateTable likeOptions_;

  // if true, this was created to handle the 'for' clause of an external table
  NABoolean forExtTable_;

};  // class ElemDDLLikeCreateTable

#endif /* ELEMDDLLIKECREATETABLE_H */
