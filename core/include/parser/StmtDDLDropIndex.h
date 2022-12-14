
#ifndef STMTDDLDROPINDEX_H
#define STMTDDLDROPINDEX_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLDropIndex.h
 * Description:  class for parse node representing Drop Index statements
 *
 *
 * Created:      11/14/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "common/ComSmallDefs.h"
#include "parser/StmtDDLNode.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLDropIndex;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None

// -----------------------------------------------------------------------
// Create Catalog statement
// -----------------------------------------------------------------------
class StmtDDLDropIndex : public StmtDDLNode {
 public:
  // constructor
  StmtDDLDropIndex(const QualifiedName &tableQualName, ComDropBehavior dropBehavior, NABoolean cleanupSpec = FALSE,
                   NABoolean validateSpec = FALSE, NAString *pLogFile = NULL);

  // virtual destructor
  virtual ~StmtDDLDropIndex();

  // cast
  virtual StmtDDLDropIndex *castToStmtDDLDropIndex();

  void synthesize();

  // accessor
  inline const QualifiedName &getOrigIndexNameAsQualifiedName() const;
  inline QualifiedName &getOrigIndexNameAsQualifiedName();

  inline const NAString getIndexName() const;
  inline const QualifiedName &getIndexNameAsQualifiedName() const;
  inline QualifiedName &getIndexNameAsQualifiedName();
  inline ComDropBehavior getDropBehavior() const;
  inline const NABoolean isCleanupSpecified() const;
  inline const NABoolean isValidateSpecified() const;
  inline const NABoolean isLogFileSpecified() const;
  inline const NAString &getLogFile() const;

  // for binding
  ExprNode *bindNode(BindWA *bindWAPtr);

  // for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString displayLabel2() const;
  virtual const NAString getText() const;

  const NABoolean dropIfExists() const { return dropIfExists_; }
  void setDropIfExists(NABoolean v) { dropIfExists_ = v; }
  void setPartitionIndexType(PARTITION_INDEX_TYPE type) { partitionIndexType_ = type; }
  inline PARTITION_INDEX_TYPE getPartitionIndexType() const;

 private:
  // the indexname specified by user in the drop stmt.
  // This name is not fully qualified during bind phase.
  QualifiedName origIndexQualName_;

  QualifiedName indexQualName_;
  ComDropBehavior dropBehavior_;
  NABoolean isCleanupSpec_;
  NABoolean isValidateSpec_;
  NAString *pLogFile_;

  // drop only if index exists. Otherwise just return.
  NABoolean dropIfExists_;

  PARTITION_INDEX_TYPE partitionIndexType_;
};  // class StmtDDLDropIndex

// -----------------------------------------------------------------------
// definitions of inline methods for class StmtDDLDropIndex
// -----------------------------------------------------------------------

//
// accessor
//

inline QualifiedName &StmtDDLDropIndex::getOrigIndexNameAsQualifiedName() { return origIndexQualName_; }

inline const QualifiedName &StmtDDLDropIndex::getOrigIndexNameAsQualifiedName() const { return origIndexQualName_; }

inline QualifiedName &StmtDDLDropIndex::getIndexNameAsQualifiedName() { return indexQualName_; }

inline const QualifiedName &StmtDDLDropIndex::getIndexNameAsQualifiedName() const { return indexQualName_; }

inline const NAString StmtDDLDropIndex::getIndexName() const { return indexQualName_.getQualifiedNameAsAnsiString(); }

inline ComDropBehavior StmtDDLDropIndex::getDropBehavior() const { return dropBehavior_; }

inline const NABoolean StmtDDLDropIndex::isCleanupSpecified() const { return isCleanupSpec_; }

inline const NABoolean StmtDDLDropIndex::isValidateSpecified() const { return isValidateSpec_; }

inline const NABoolean StmtDDLDropIndex::isLogFileSpecified() const {
  if (pLogFile_ == NULL)
    return FALSE;
  else
    return TRUE;
}

inline const NAString &StmtDDLDropIndex::getLogFile() const {
  ComASSERT(pLogFile_ NEQ NULL);
  return *pLogFile_;
}

inline StmtDDLNode::PARTITION_INDEX_TYPE StmtDDLDropIndex::getPartitionIndexType() const { return partitionIndexType_; }
#endif  // STMTDDLDROPINDEX_H
