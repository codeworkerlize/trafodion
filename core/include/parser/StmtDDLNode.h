
#ifndef STMTDDLNODE_H
#define STMTDDLNODE_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLNode.C
 * Description:  Base class for DDL statements
 *
 *
 * Created:      3/9/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "optimizer/ObjectNames.h"
#include "ElemDDLNode.h"
#include "cli/sqlcli.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLNode;

// -----------------------------------------------------------------------
// Helpful Definitions. Moved to optimizer/ObjectNames.h
// -----------------------------------------------------------------------
// typedef LIST(const NAString *)                     ConstStringList;

// -----------------------------------------------------------------------
// A generic node for DDL statements.
// -----------------------------------------------------------------------
class StmtDDLNode : public ElemDDLNode {
 public:
  enum PARTITION_INDEX_TYPE {
    PARTITION_INDEX_NONE = -1,
    PARTITION_INDEX_NORMAL = 0,
    PARTITION_INDEX_LOCAL,
    PARTITION_INDEX_GLOBAL
  };
  // default constructor
  StmtDDLNode(OperatorTypeEnum otype = DDL_ANY_STMT);

  // virtual destructor
  virtual ~StmtDDLNode();

  // cast
  virtual StmtDDLNode *castToStmtDDLNode();
  virtual const StmtDDLNode *castToStmtDDLNode() const;

  // method for processing
  virtual ExprNode *bindNode(BindWA *);

  // methods for tracing
  virtual const NAString getText() const;
  virtual void unparse(NAString &result, PhaseEnum phase = OPTIMIZER_PHASE, UnparseFormatEnum form = USER_FORMAT,
                       TableDesc *tabId = NULL) const;

  //++ MV OZ
  // sometimes the statement is called from withing the catalog and is for
  // different table typed that differ form regular tables.
  // this I call - an expertise.

  // since these methods were added long after the whole system was up
  // if you want to relly on them be sure you set the field properly
  inline void setExpertiseTableType(ComTableType tableType);
  inline void setExpertiseObjectClass(ComObjectClass objectClass);

  inline ComTableType getTableType() const;
  inline ComObjectClass getObjectClass() const;

  // Volatile Table
  NABoolean isVolatile() { return isVolatile_; }
  NABoolean isVolatile() const { return isVolatile_; }
  void setIsVolatile(NABoolean vt) { isVolatile_ = vt; }
  NABoolean processAsExeUtil() { return exeUtil_; }
  void setProcessAsExeUtil(NABoolean eu) { exeUtil_ = eu; }

  // External object
  NABoolean isExternal() { return isExternal_; }
  NABoolean isExternal() const { return isExternal_; }
  void setIsExternal(NABoolean e) { isExternal_ = e; }
  NABoolean isImplicitExternal() { return isImplicitExternal_; }
  NABoolean isImplicitExternal() const { return isImplicitExternal_; }
  void setIsImplicitExternal(NABoolean e) { isImplicitExternal_ = e; }

  // Ghost Object
  NABoolean isGhostObject() { return isGhostObject_; }
  NABoolean isGhostObject() const { return isGhostObject_; }
  void setIsGhostObject(NABoolean g) { isGhostObject_ = g; }

  // object definition only in memory with no metadata or label.
  NABoolean isInMemoryObjectDefn() { return inMemoryObjectDefn_; }
  NABoolean isInMemoryObjectDefn() const { return inMemoryObjectDefn_; }
  void setInMemoryObjectDefn(NABoolean im) { inMemoryObjectDefn_ = im; }

  // returns TRUE, if parallel DDL op could be performed for an
  // object with 'numPartitions'.
  static NABoolean performParallelOp(int numPartitions);

  virtual NABoolean explainSupported() { return FALSE; }

  NABoolean ddlXns() { return ddlXns_; }
  void setDdlXns(NABoolean v) { ddlXns_ = v; }

  NABoolean isPartition() { return isPartition_; }
  NABoolean isPartition() const { return isPartition_; }
  void setIsPartition(NABoolean p) { isPartition_ = p; }

 private:
  ComTableType tableType_;
  ComObjectClass objectClass_;

  // if this DDL operation was specified using VOLATILE syntax.
  NABoolean isVolatile_;

  // this object was only created in mxcmp memory(catman cache, NAtable
  // cache. It doesn't exist in metadata or physical labels.
  // Used to test different access plans without actually creating
  // the object.
  NABoolean inMemoryObjectDefn_;

  // if this node is to be processed as an ExeUtil expr.
  NABoolean exeUtil_;

  // if this DDL operation was specified using GHOST syntax.
  NABoolean isGhostObject_;

  // if this DDL operation was specified using EXTERNAL syntax.
  NABoolean isExternal_;

  // if this DDL was specified using 'IMPLICIT EXTERNAL' syntax
  // through the use of 'create implicit external'... stmt.
  // Used when external table on hive tables is created for internal usage
  // (for ex: for privs, ustat, views)
  // Valid only if isExternal_ is set.
  NABoolean isImplicitExternal_;

  // if TRUE, this ddl operation will run using DTM transactions
  NABoolean ddlXns_;

  NABoolean isPartition_;
};  // class StmtDDLNode

// -----------------------------------------------------------------------
// definitions of inline methods for class StmtDDLNode
// -----------------------------------------------------------------------

inline void StmtDDLNode::setExpertiseTableType(ComTableType tableType) { tableType_ = tableType; }

inline void StmtDDLNode::setExpertiseObjectClass(ComObjectClass objectClass) { objectClass_ = objectClass; }

inline ComTableType StmtDDLNode::getTableType() const { return tableType_; }

inline ComObjectClass StmtDDLNode::getObjectClass() const { return objectClass_; }

#endif  // STMTDDLNODE_H
