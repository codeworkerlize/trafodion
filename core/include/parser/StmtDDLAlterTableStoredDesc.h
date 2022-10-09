
#ifndef STMTDDLALTERTABLESTOREDDESC_H
#define STMTDDLALTERTABLESTOREDDESC_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLAlterTableStoredDesc.h
 * Description:
 *   Alter Table <table-name> [generate|delete|enable|disable] stored descriptor
 *               DDL statements
 *
 *
 * Created:      7/26/2016
 * Language:     C++
 *
 *****************************************************************************
 */

#include "parser/StmtDDLAlterTable.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLAlterTableStoredDesc;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// definition of class StmtDDLAlterTableStoredDesc
// -----------------------------------------------------------------------
class StmtDDLAlterTableStoredDesc : public StmtDDLAlterTable {
 public:
  enum AlterStoredDescType {
    CHECK_DESC,
    CHECK_STATS,
    GENERATE_DESC,
    GENERATE_STATS,
    GENERATE_STATS_FORCE,
    GENERATE_STATS_INTERNAL,  // only used in UpdateStats, will not remove natable. So will not cause ERROR 8738.
    DELETE_DESC,
    DELETE_STATS,
    ENABLE,
    DISABLE
  };

  // constructor
  StmtDDLAlterTableStoredDesc(const AlterStoredDescType type)
      : StmtDDLAlterTable(DDL_ALTER_TABLE_STORED_DESC), type_(type) {}

  // virtual destructor
  virtual ~StmtDDLAlterTableStoredDesc() {}

  // cast
  virtual StmtDDLAlterTableStoredDesc *castToStmtDDLAlterTableStoredDesc() { return this; }

  // accessors
  AlterStoredDescType getType() { return type_; }

  // method for tracing
  virtual const NAString getText() const { return "StmtDDLAlterTableStoredDesc"; }

 private:
  AlterStoredDescType type_;

};  // class StmtDDLAlterTableStoredDesc

// -----------------------------------------------------------------------
// definitions of inline methods for class StmtDDLAlterTableStoredDesc
// -----------------------------------------------------------------------

#endif  // STMTDDLALTERTABLESTOREDDESC_H
