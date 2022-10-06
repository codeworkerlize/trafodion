
#ifndef STMTDDLPOPULATEINDEX_H
#define STMTDDLPOPULATEINDEX_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLPopulateIndex.h
 * Description:  class representing Populate Index Statement parser nodes
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

#include "ElemDDLNode.h"
#include "common/ComSmallDefs.h"
#include "common/NAString.h"
#include "parser/StmtDDLNode.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLPopulateIndex;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None

// -----------------------------------------------------------------------
// Populate Index statement
// -----------------------------------------------------------------------
class StmtDDLPopulateIndex : public StmtDDLNode {
 public:
  // initialize constructor
  StmtDDLPopulateIndex(NABoolean populateAll, NABoolean populateAllUnique, NABoolean purgedataSpecified,
                       const NAString &anIndexName,  // dummy name, if populateAll*
                       const QualifiedName &aTableName, CollHeap *heap = PARSERHEAP());

  // virtual destructor
  virtual ~StmtDDLPopulateIndex();

  // cast
  virtual StmtDDLPopulateIndex *castToStmtDDLPopulateIndex();

  //
  // accessors
  //

  // methods relating to parse tree
  virtual int getArity() const;
  virtual ExprNode *getChild(int index);

  inline const QualifiedName &getOrigTableNameAsQualifiedName() const;
  inline QualifiedName &getOrigTableNameAsQualifiedName();

  inline const NAString &getIndexName() const;
  inline const NAString &getPartitionName() const;
  inline const QualifiedName &getIndexNameAsQualifiedName() const;
  inline QualifiedName &getIndexNameAsQualifiedName();

  inline const QualifiedName &getTableNameAsQualifiedName() const;
  inline QualifiedName &getTableNameAsQualifiedName();

  // returns table name, in external format.
  const NAString getTableName() const;

  ExprNode *bindNode(BindWA *pBindWA);

  //
  // method for collecting information
  //

  void synthesize();

  // collects information in the parse sub-tree and
  // copy/move them to the current parse node.

  //
  // methods for tracing
  //

  virtual const NAString displayLabel1() const;
  virtual const NAString displayLabel2() const;
  //  virtual NATraceList getDetailInfo() const;
  virtual const NAString getText() const;

  virtual NABoolean explainSupported() { return TRUE; }

  NABoolean populateAll() { return populateAll_; }
  NABoolean populateAllUnique() { return populateAllUnique_; }
  NABoolean purgedataSpecified() { return purgedataSpecified_; }
  NABoolean populateIndexOnPartition() { return populateIndexOnPartition_; }
  void setIsPopulateIndexOnPartition() { populateIndexOnPartition_ = TRUE; }
  void setPartitionName(NAString &paritionName) { partitionName_ = paritionName; }

 private:
  // ---------------------------------------------------------------------
  // private methods
  // ---------------------------------------------------------------------
  NABoolean populateAll_;
  NABoolean populateAllUnique_;
  NABoolean purgedataSpecified_;
  NABoolean populateIndexOnPartition_;

  //
  // please do not use the following methods
  //

  StmtDDLPopulateIndex();                                         // DO NOT USE
  StmtDDLPopulateIndex(const StmtDDLPopulateIndex &);             // DO NOT USE
  StmtDDLPopulateIndex &operator=(const StmtDDLPopulateIndex &);  // DO NOT USE

  // ---------------------------------------------------------------------
  // private data members
  // ---------------------------------------------------------------------

  // index name can only be a simple name
  NAString indexName_;
  NAString partitionName_;
  QualifiedName indexQualName_;

  // the tablename specified by user in the create stmt.
  // This name is not fully qualified during bind phase.
  QualifiedName origTableQualName_;

  // The syntax of table name is
  // [ [ catalog-name . ] schema-name . ] table-name

  QualifiedName tableQualName_;

};  // class StmtDDLPopulateIndex

// -----------------------------------------------------------------------
// definitions of inline methods for class StmtDDLPopulateIndex
// -----------------------------------------------------------------------
inline QualifiedName &StmtDDLPopulateIndex::getOrigTableNameAsQualifiedName() { return origTableQualName_; }

inline const QualifiedName &StmtDDLPopulateIndex::getOrigTableNameAsQualifiedName() const { return origTableQualName_; }

inline QualifiedName &StmtDDLPopulateIndex::getIndexNameAsQualifiedName() { return indexQualName_; }

inline const QualifiedName &StmtDDLPopulateIndex::getIndexNameAsQualifiedName() const { return indexQualName_; }

inline QualifiedName &StmtDDLPopulateIndex::getTableNameAsQualifiedName() { return tableQualName_; }

inline const QualifiedName &StmtDDLPopulateIndex::getTableNameAsQualifiedName() const { return tableQualName_; }

// get index name
inline const NAString &StmtDDLPopulateIndex::getIndexName() const { return indexName_; }

inline const NAString &StmtDDLPopulateIndex::getPartitionName() const { return partitionName_; }

#endif  // STMTDDLPOPULATEINDEX_H
