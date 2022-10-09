#ifndef STMTDDLALTERTABLEDISABLEIDX_H
#define STMTDDLALTERTABLEDISABLEIDX_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLAlterTableDisableIndex.h
 * Description:  class for Alter Table <table-name>
 *                  DISABLE ALL INDEXES and
 *                  DISABLE INDEX <index>
 *               DDL statements
 *
 *               The methods in this class are defined either in this
 *               header file or the source file StmtDDLAlter.cpp
 *
 *
 * Created:     03/09/07
 * Language:     C++
 *
 *

 *
 *
 *****************************************************************************
 */

#include "parser/StmtDDLAlterTable.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLAlterTableDisableIndex;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// definition of class StmtDDLAlterTableDisableIndex
// -----------------------------------------------------------------------
class StmtDDLAlterTableDisableIndex : public StmtDDLAlterTable {
 public:
  // constructor
  StmtDDLAlterTableDisableIndex(NAString &indexName, NABoolean allIndexes, NABoolean allUniqueIndexes);

  // virtual destructor
  virtual ~StmtDDLAlterTableDisableIndex();

  // cast
  virtual StmtDDLAlterTableDisableIndex *castToStmtDDLAlterTableDisableIndex();

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

  StmtDDLAlterTableDisableIndex();
  StmtDDLAlterTableDisableIndex(const StmtDDLAlterTableDisableIndex &);
  StmtDDLAlterTableDisableIndex &operator=(const StmtDDLAlterTableDisableIndex &);

 private:
  NAString indexName_;
  NAString partitionName_;
  NABoolean allIndexes_;
  NABoolean allUniqueIndexes_;
  NABoolean isIndexOnPartition_;

};  // class StmtDDLAlterTableDisableIndex

// -----------------------------------------------------------------------
// definitions of inline methods for class StmtDDLAlterTableDisableIndex
// -----------------------------------------------------------------------

inline const NAString &StmtDDLAlterTableDisableIndex::getIndexName() const { return indexName_; }

inline const NAString &StmtDDLAlterTableDisableIndex::getPartitionName() const { return partitionName_; }

inline const NABoolean StmtDDLAlterTableDisableIndex::getAllIndexes() const { return allIndexes_; }

inline const NABoolean StmtDDLAlterTableDisableIndex::getAllUniqueIndexes() const { return allUniqueIndexes_; }

inline const NABoolean StmtDDLAlterTableDisableIndex::isProcessIndexOnPartition() const { return isIndexOnPartition_; }

inline void StmtDDLAlterTableDisableIndex::setIndexName(NAString &indexName) { indexName_ = indexName; }

inline void StmtDDLAlterTableDisableIndex::setAllIndexes(NABoolean allIndexes) { allIndexes_ = allIndexes; }

inline void StmtDDLAlterTableDisableIndex::setAllUniqueIndexes(NABoolean allUniqueIndexes) {
  allUniqueIndexes_ = allUniqueIndexes;
}

inline void StmtDDLAlterTableDisableIndex::setIndexOnPartition(NABoolean v) { isIndexOnPartition_ = v; }

#endif  // STMTDDLALTERTABLEDISABLEIDX_H
