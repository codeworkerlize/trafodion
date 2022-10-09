
/* -*-C++-*-
******************************************************************************
*
* File:         StmtDDLMisc.cpp
* Description:  Misc Stmt nodes
* Created:
* Language:     C++
*
*
*
*
******************************************************************************
*/

#include "common/BaseTypes.h"
#include "common/ComASSERT.h"
#include "common/ComOperators.h"
#include "parser/StmtDDLCleanupObjects.h"

#define SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#include "parser/SqlParserGlobals.h"

StmtDDLCleanupObjects::StmtDDLCleanupObjects(ObjectType type, const NAString &param1, const NAString *param2,
                                             CollHeap *heap)
    : StmtDDLNode(DDL_CLEANUP_OBJECTS),
      type_(type),
      param1_(param1),
      param2_(param2 ? *param2 : ""),
      partition_name_(""),
      tableQualName_(NULL),
      objectUID_(-1),
      stopOnError_(FALSE),
      getStatus_(FALSE),
      checkOnly_(FALSE),
      returnDetails_(FALSE),
      noHBaseDrop_(FALSE) {}

StmtDDLCleanupObjects::~StmtDDLCleanupObjects() {}

StmtDDLCleanupObjects *StmtDDLCleanupObjects::castToStmtDDLCleanupObjects() { return this; }

int StmtDDLCleanupObjects::getArity() const { return 0; }

ExprNode *StmtDDLCleanupObjects::getChild(int index) { return NULL; }

const NAString StmtDDLCleanupObjects::getText() const { return "StmtDDLCleanupObjects"; }

ExprNode *StmtDDLCleanupObjects::bindNode(BindWA *pBindWA) {
  ComASSERT(pBindWA);

  //
  // expands table name
  //

  if ((type_ == TABLE_) || (type_ == INDEX_) || (type_ == SEQUENCE_) || (type_ == VIEW_) ||
      (type_ == SCHEMA_PRIVATE_) || (type_ == SCHEMA_SHARED_) || (type_ == HBASE_TABLE_) || (type_ == UNKNOWN_)) {
    ComObjectName tableName(param1_);
    tableQualName_ = new (PARSERHEAP())
        QualifiedName(tableName.getObjectNamePart().getInternalName(), tableName.getSchemaNamePart().getInternalName(),
                      tableName.getCatalogNamePart().getInternalName());

    // remember the original table name specified by user
    origTableQualName_ = *tableQualName_;

    if (applyDefaultsAndValidateObject(pBindWA, tableQualName_)) {
      pBindWA->setErrStatus();
      return this;
    }

    if (NOT param2_.isNull()) objectUID_ = atoInt64(param2_.data());
  } else if (type_ == OBJECT_UID_) {
    objectUID_ = atoInt64(param1_.data());
  } else {
    pBindWA->setErrStatus();
    return this;
  }

  //
  // sets a flag to let user know that the parse has
  // been bound.
  //
  markAsBound();

  return this;
}
