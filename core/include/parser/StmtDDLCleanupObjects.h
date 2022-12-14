
#ifndef STMTDDLCLEANUPOBJECTS_H
#define STMTDDLCLEANUPOBJECTS_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLCleanupObjects.h
 * Description:
 *
 *
 * Created:
 * Language:     C++
 *
 *
 *****************************************************************************
 */

#include "parser/ElemDDLNode.h"
#include "common/ComSmallDefs.h"
#include "common/NAString.h"
#include "parser/StmtDDLNode.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLCleanupObjects;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None

// -----------------------------------------------------------------------
// Cleanup Objects statement
// -----------------------------------------------------------------------
class StmtDDLCleanupObjects : public StmtDDLNode {
 public:
  enum ObjectType {
    TABLE_,
    OBJECT_UID_,
    INDEX_,
    VIEW_,
    SEQUENCE_,
    SCHEMA_PRIVATE_,
    SCHEMA_SHARED_,
    //    HIVE_TABLE_,
    //    HIVE_VIEW_,
    HBASE_TABLE_,
    UNKNOWN_,
    OBSOLETE_
  };

  // initialize constructor
  StmtDDLCleanupObjects(ObjectType type, const NAString &param1, const NAString *param2, CollHeap *heap);

  // virtual destructor
  virtual ~StmtDDLCleanupObjects();

  // cast
  virtual StmtDDLCleanupObjects *castToStmtDDLCleanupObjects();

  //
  // accessors
  //

  // methods relating to parse tree
  virtual int getArity() const;
  virtual ExprNode *getChild(int index);

  inline const QualifiedName &getOrigTableNameAsQualifiedName() const;
  inline QualifiedName &getOrigTableNameAsQualifiedName();
  inline const QualifiedName *getTableNameAsQualifiedName() const;
  inline QualifiedName *getTableNameAsQualifiedName();

  inline const NAString getPartitionName() { return partition_name_; }
  inline void setPartitionName(const NAString *name) { partition_name_ = *name; }
  // returns table name, in external format.
  const NAString getTableName() const;

  const long getObjectUID() const { return objectUID_; }

  const ObjectType getType() const { return type_; }

  ExprNode *bindNode(BindWA *pBindWA);

  //
  // methods for tracing
  //

  //  virtual const NAString displayLabel1() const;
  //  virtual const NAString displayLabel2() const;
  virtual const NAString getText() const;

  NABoolean stopOnError() { return stopOnError_; }
  void setStopOnError(NABoolean v) { stopOnError_ = v; }

  NABoolean getStatus() { return getStatus_; }
  void setGetStatus(NABoolean v) { getStatus_ = v; }

  NABoolean checkOnly() { return checkOnly_; }
  void setCheckOnly(NABoolean v) { checkOnly_ = v; }

  NABoolean returnDetails() { return returnDetails_; }
  void setReturnDetails(NABoolean v) { returnDetails_ = v; }

  NABoolean noHBaseDrop() { return noHBaseDrop_; }
  void setNoHBaseDrop(NABoolean v) { noHBaseDrop_ = v; }

 private:
  ObjectType type_;
  NAString param1_;
  NAString param2_;

  // specified partition or subpartition name.
  NAString partition_name_;

  //
  // please do not use the following methods
  //

  StmtDDLCleanupObjects();                                          // DO NOT USE
  StmtDDLCleanupObjects(const StmtDDLCleanupObjects &);             // DO NOT USE
  StmtDDLCleanupObjects &operator=(const StmtDDLCleanupObjects &);  // DO NOT USE

  // ---------------------------------------------------------------------
  // private data members
  // ---------------------------------------------------------------------

  // the tablename specified by user in the drop stmt.
  // This name is not fully qualified during bind phase.
  QualifiedName origTableQualName_;

  // The syntax of table name is
  // [ [ catalog-name . ] schema-name . ] table-name
  QualifiedName *tableQualName_;

  long objectUID_;

  NABoolean stopOnError_;

  NABoolean getStatus_;
  NABoolean checkOnly_;
  NABoolean returnDetails_;
  NABoolean noHBaseDrop_;
};  // class StmtDDLCleanupObjects

// -----------------------------------------------------------------------
// definitions of inline methods for class StmtDDLCleanupObjects
// -----------------------------------------------------------------------
inline QualifiedName &StmtDDLCleanupObjects::getOrigTableNameAsQualifiedName() { return origTableQualName_; }

inline const QualifiedName &StmtDDLCleanupObjects::getOrigTableNameAsQualifiedName() const {
  return origTableQualName_;
}

inline QualifiedName *StmtDDLCleanupObjects::getTableNameAsQualifiedName() { return tableQualName_; }

inline const QualifiedName *StmtDDLCleanupObjects::getTableNameAsQualifiedName() const { return tableQualName_; }

#endif  // STMTDDLCLEANUPOBJECTS_H
