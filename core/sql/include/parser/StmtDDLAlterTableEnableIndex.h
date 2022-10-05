#ifndef STMTDDLALTERTABLEENABLEIDX_H
#define STMTDDLALTERTABLEENABLEIDX_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLAlterTableEnableIndex.h
 * Description:  class for Alter Table <table-name>
 *                  ENABLE ALL INDEXES and
 *                  ENABLE INDEX <index>
 *               DDL statements
 *
 *               The methods in this class are defined either in this
 *               header file or the source file StmtDDLAlter.cpp
 *
 *
 * Created:
 * Language:     C++
 *
 *

 *
 *
 *****************************************************************************
 */

#include "StmtDDLAlterTable.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLAlterTableEnableIndex;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// definition of class StmtDDLAlterTableEnableIndex
// -----------------------------------------------------------------------
class StmtDDLAlterTableEnableIndex : public StmtDDLAlterTable {
 public:
  // constructor
  StmtDDLAlterTableEnableIndex(NAString &indexName, NABoolean allIndexes, NABoolean allUniqueIndexes);

  // virtual destructor
  virtual ~StmtDDLAlterTableEnableIndex();

  // cast
  virtual StmtDDLAlterTableEnableIndex *castToStmtDDLAlterTableEnableIndex();

  // accessors
  inline const NAString &getIndexName() const;
  inline const NAString &getPartitionName() const;
  inline const NABoolean getAllIndexes() const;
  inline const NABoolean getAllUniqueIndexes() const;
  inline const NABoolean isProcessIndexOnPartition() const;

  // ---------------------------------------------------------------------
  // mutators
  // ---------------------------------------------------------------------

  // This method collects information in the parse sub-tree and copies it
  // to the current parse node.
  void synthesize();
  inline void setIndexName(NAString &indexName);
  inline void setAllIndexes(NABoolean allIndexes);
  inline void setAllUniqueIndexes(NABoolean allIndexes);
  inline void setIndexOnPartition(NABoolean v);
  inline void setPartitionName(NAString &paritionName) { partitionName_ = paritionName; }

  // methods for tracing
  const NAString displayLabel2() const;
  virtual const NAString getText() const;

  //
  // please do not use the following methods
  //

  StmtDDLAlterTableEnableIndex();
  StmtDDLAlterTableEnableIndex(const StmtDDLAlterTableEnableIndex &);
  StmtDDLAlterTableEnableIndex &operator=(const StmtDDLAlterTableEnableIndex &);

 private:
  NAString indexName_;
  NAString partitionName_;
  NABoolean allIndexes_;
  NABoolean allUniqueIndexes_;
  NABoolean isIndexOnPartition_;

};  // class StmtDDLAlterTableEnableIndex

// -----------------------------------------------------------------------
// definitions of inline methods for class StmtDDLAlterTableEnableIndex
// -----------------------------------------------------------------------

inline const NAString &StmtDDLAlterTableEnableIndex::getIndexName() const { return indexName_; }

inline const NAString &StmtDDLAlterTableEnableIndex::getPartitionName() const { return partitionName_; }

inline const NABoolean StmtDDLAlterTableEnableIndex::getAllIndexes() const { return allIndexes_; }

inline const NABoolean StmtDDLAlterTableEnableIndex::getAllUniqueIndexes() const { return allUniqueIndexes_; }

inline const NABoolean StmtDDLAlterTableEnableIndex::isProcessIndexOnPartition() const { return isIndexOnPartition_; }

inline void StmtDDLAlterTableEnableIndex::setIndexName(NAString &indexName) { indexName_ = indexName; }

inline void StmtDDLAlterTableEnableIndex::setAllIndexes(NABoolean allIndexes) { allIndexes_ = allIndexes; }

inline void StmtDDLAlterTableEnableIndex::setAllUniqueIndexes(NABoolean allUniqueIndexes) {
  allUniqueIndexes_ = allUniqueIndexes;
}

inline void StmtDDLAlterTableEnableIndex::setIndexOnPartition(NABoolean v) { isIndexOnPartition_ = v; }

#endif  // STMTDDLALTERTABLEENABLEIDX_H
