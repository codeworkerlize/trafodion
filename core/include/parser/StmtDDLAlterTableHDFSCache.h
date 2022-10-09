#ifndef STMTDDLALTERTABLEHDFSCACHE_H
#define STMTDDLALTERTABLEHDFSCACHE_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLAlterTableHDFSCache.h
 * Description:  class for
 *               --Add table to HDFS cache pool
 *               Alter Table <table-name> CACHE IN <pool-name>
 *               --Remove table from HDFS cache pool
 *               Alter Table <table-name> DECACHE FROM <pool-name>
 *
 *
 *
 *

 *
 *
 *****************************************************************************
 */

#include "parser/StmtDDLAlterTable.h"

class StmtDDLAlterTableHDFSCache : public StmtDDLAlterTable {
 public:
  // constructor
  StmtDDLAlterTableHDFSCache(const NAString &pool, NABoolean atc, NAMemory *heap = PARSERHEAP());

  // virtual destructor
  virtual ~StmtDDLAlterTableHDFSCache();
  // cast
  virtual StmtDDLAlterTableHDFSCache *castToStmtDDLAlterTableHDFSCache();

  // method for tracing
  virtual const NAString getText() const { return "StmtDDLAlterTableHDFSCache"; }

  NAString &poolName() { return poolName_; }

  NABoolean &isAddToCache() { return isAddToCache_; }

 private:
  NAString poolName_;

  NABoolean isAddToCache_;

};  // class StmtDDLAlterTableHDFSCache

#endif  // STMTDDLALTERTABLEHDFSCACHE_H
