#ifndef STMTDDLALTERSCHEMAHDFSCACHE_H
#define STMTDDLALTERSCHEMAHDFSCACHE_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLAlterSchemaHDFSCache.h
 * Description:  base class for Alter Schema statements
 *
 *
 * Created:      10/31/2012
 * Language:     C++
 *
 *


 *
 *
 *****************************************************************************
 */

#include "StmtDDLNode.h"

class StmtDDLAlterSchemaHDFSCache : public StmtDDLNode {
 public:
  // constructors
  StmtDDLAlterSchemaHDFSCache(const SchemaName &schema, const NAString &pool, NABoolean atc,
                              NAMemory *heap = PARSERHEAP())
      : StmtDDLNode(DDL_ALTER_SCHEMA_HDFS_CACHE),
        schemaName_(schema, heap),
        poolName_(pool, heap),
        isAddToCache_(atc) {}

  // virtual destructor
  virtual ~StmtDDLAlterSchemaHDFSCache(){};

  // cast
  virtual StmtDDLAlterSchemaHDFSCache *castToStmtDDLAlterSchemaHDFSCache() { return this; };

  // methods for tracing
  virtual const NAString getText() const { return "StmtDDLAlterSchemaHDFSCache"; };

  // method for binding
  ExprNode *bindNode(BindWA *bindWAPtr) {
    markAsBound();
    return this;
  };

  NAString &poolName() { return poolName_; }

  NABoolean &isAddToCache() { return isAddToCache_; }

  SchemaName &schemaName() { return schemaName_; }

 private:
  SchemaName schemaName_;
  NAString poolName_;
  NABoolean isAddToCache_;

};  // class StmtDDLAlterSchemaHDFSCache

#endif  // STMTDDLALTERSCHEMAHDFSCACHE_H
