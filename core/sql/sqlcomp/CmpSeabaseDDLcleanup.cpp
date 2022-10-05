/**********************************************************************
// @@@ START COPYRIGHT @@@
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// @@@ END COPYRIGHT @@@
**********************************************************************/

/* -*-C++-*-
 *****************************************************************************
 *
 * File:         CmpSeabaseDDLcleanup.cpp
 * Description:  Implements cleanup of metadata.
 *
 *
 * Created:
 * Language:     C++
 *
 *
 *****************************************************************************
 */

#include "sqlcomp/CmpSeabaseDDLincludes.h"
#include "sqlcomp/CmpDDLCatErrorCodes.h"
#include "sqlcomp/CmpSeabaseDDLcleanup.h"
#include "sqlcomp/PrivMgrMDDefs.h"
#include "sqlcomp/PrivMgrComponentPrivileges.h"

//////////////////////////////////////////////////////////////////////////
// Methods related to cleanup of objects from metadata.
//////////////////////////////////////////////////////////////////////////
CmpSeabaseMDcleanup::CmpSeabaseMDcleanup(NAHeap *heap)
    : CmpSeabaseDDL(heap),
      objUID_(-1),
      objectFlags_(-1),
      objDataUID_(-1),
      objectOwner_(-1),
      indexesUIDlist_(NULL),
      uniqueConstrUIDlist_(NULL),
      refConstrUIDlist_(NULL),
      seqUIDlist_(NULL),
      usingViewsList_(NULL),
      lobV2_(FALSE),
      numLOBs_(0),
      lobNumList_(NULL),
      lobTypList_(NULL),
      lobLocList_(NULL),
      stopOnError_(FALSE),
      cleanupMetadataEntries_(FALSE),
      checkOnly_(FALSE),
      returnDetails_(FALSE),
      hasInferiorPartitons_(FALSE),
      returnDetailsList_(NULL),
      noHBaseDrop_(FALSE),
      currReturnEntry_(0),
      numOrphanMetadataEntries_(0),
      numOrphanHbaseEntries_(0),
      numOrphanObjectsEntries_(0),
      numOrphanViewsEntries_(0),
      numInconsistentPartitionEntries_(0),
      numInconsistentHiveEntries_(0),
      numInconsistentPrivEntries_(0),
      numInconsistentGroupEntries_(0),
      numInconsistentTextEntries_(0),
      isHive_(FALSE){};

long CmpSeabaseMDcleanup::getCleanupObjectUID(ExeCliInterface *cliInterface, const char *catName, const char *schName,
                                               const char *objName, const char *inObjType, char *outObjType,
                                               int &objectOwner, long *objectFlags, long *objDataUID) {
  int cliRC = 0;
  long objUID = -1;
  objectOwner = -1;

  ExeCliInterface cqdCliInterface(STMTHEAP);

  // find object uid of this object from OBJECTS table
  char shapeBuf[1000];
  str_sprintf(shapeBuf, "control query shape scan (path '%s.\"%s\".%s')", getSystemCatalog(), SEABASE_MD_SCHEMA,
              SEABASE_OBJECTS);
  if (cqdCliInterface.setCQS(shapeBuf)) {
    cqdCliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  objUID = getObjectUID(cliInterface, catName, schName, objName,
                        (strcmp(inObjType, COM_UNKNOWN_OBJECT_LIT) == 0 ? NULL : inObjType), objDataUID, NULL,
                        outObjType, FALSE, TRUE, objectFlags);
  cqdCliInterface.resetCQS(CmpCommon::diags());
  if (objUID != -1)  // object exists
  {
    // find owner
    cliRC = getObjectOwner(cliInterface, catName, schName, objName,
                           (strcmp(inObjType, COM_UNKNOWN_OBJECT_LIT) == 0 ? NULL : inObjType), &objectOwner);
    if (cliRC < 0) {
      // error trying to get owner. Ignore it and return invalid owner.
      CmpCommon::diags()->clear();
      objectOwner = -1;
    }

    return objUID;
  }

  // didnt find it in OBJECTS table. Look for it in OBJECTS_UNIQ_IDX table
  CmpCommon::diags()->clear();
  objectOwner = -1;  // index does not contain owner info.
  objUID = getObjectUID(cliInterface, catName, schName, objName,
                        (strcmp(inObjType, COM_UNKNOWN_OBJECT_LIT) == 0 ? NULL : inObjType), objDataUID, NULL,
                        outObjType, TRUE, TRUE, objectFlags);
  if (objUID != -1) return objUID;

  // find object_uid of missing table by doing a select on COLUMNS with
  // the specified column name and check that the corresponding object_uid
  // doesn't exist in OBJECTS table
  // *** TBD  ***

  return -1;
}

short CmpSeabaseMDcleanup::getCleanupObjectName(ExeCliInterface *cliInterface, long objUID, NAString &catName,
                                                NAString &schName, NAString &objName, NAString &objType,
                                                int &objectOwner, long *objectFlags, long *objDataUID) {
  int cliRC = 0;
  char objTypeBuf[10];

  objectOwner = -1;
  if (objectFlags != NULL) {
    *objectFlags = -1;
  }

  // look in objects idx
  cliRC = getObjectName(cliInterface, objUID, catName, schName, objName, objTypeBuf, FALSE, TRUE);
  if ((cliRC < 0) && (cliRC != -1389)) return -1;

  if (cliRC == -1389)  // not found
  {
    CmpCommon::diags()->clear();

    // look in objects table
    cliRC = getObjectName(cliInterface, objUID, catName, schName, objName, objTypeBuf, TRUE, FALSE);
    if ((cliRC < 0) && (cliRC != -1389)) return -1;

    if (cliRC == -1389)  // not found
    {
      CmpCommon::diags()->clear();
      objName = "";
    }
  }

  if (cliRC == -1389)  // not found
  {
    // assume object is base table type.
    objType = COM_BASE_TABLE_OBJECT_LIT;
  } else {
    // found this object id
    objType = objTypeBuf;

    // find the owner
    cliRC = getObjectOwner(cliInterface, catName, schName, objName, objType, &objectOwner);
    if (cliRC < 0) {
      // error trying to get owner or not found. Ignore it and return invalid owner.
      CmpCommon::diags()->clear();
      objectOwner = -1;
    }

    if (objectFlags != NULL) {
      // find the flags
      cliRC = getObjectFlags(cliInterface, catName, schName, objName, objType, objectFlags);
      if (cliRC < 0) {
        // error trying to get flags or not found. Ignore it and return invalid flags.
        CmpCommon::diags()->clear();
        *objectFlags = -1;
      }
    }
  }

  return 0;
}

short CmpSeabaseMDcleanup::hasInferiorPartitons(ExeCliInterface *cliInterface, long objUID) {
  int cliRC = 0;
  char query[1000];

  str_sprintf(query, "select count(parent_uid) from %s.\"%s\".%s where parent_uid = %ld", getSystemCatalog(),
              SEABASE_MD_SCHEMA, SEABASE_PARTITIONS, objUID);

  int len = 0;
  long rowCount = 0;
  cliRC = cliInterface->executeImmediate(query, (char *)&rowCount, &len, FALSE);

  // If unexpected error occurred
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  return (rowCount > 0) ? 1 : 0;
}

short CmpSeabaseMDcleanup::validateInputValues(StmtDDLCleanupObjects *stmtCleanupNode, ExeCliInterface *cliInterface) {
  int cliRC;

  objUID_ = -1;
  objectOwner_ = -1;
  objType_ = "";

  NAString inObjType;
  if (stmtCleanupNode->getType() == StmtDDLCleanupObjects::TABLE_)
    inObjType = COM_BASE_TABLE_OBJECT_LIT;
  else if (stmtCleanupNode->getType() == StmtDDLCleanupObjects::INDEX_)
    inObjType = COM_INDEX_OBJECT_LIT;
  else if (stmtCleanupNode->getType() == StmtDDLCleanupObjects::SEQUENCE_)
    inObjType = COM_SEQUENCE_GENERATOR_OBJECT_LIT;
  else if (stmtCleanupNode->getType() == StmtDDLCleanupObjects::VIEW_)
    inObjType = COM_VIEW_OBJECT_LIT;
  else if (stmtCleanupNode->getType() == StmtDDLCleanupObjects::SCHEMA_PRIVATE_)
    inObjType = COM_PRIVATE_SCHEMA_OBJECT_LIT;
  else if (stmtCleanupNode->getType() == StmtDDLCleanupObjects::SCHEMA_SHARED_)
    inObjType = COM_SHARED_SCHEMA_OBJECT_LIT;
  else if (stmtCleanupNode->getType() == StmtDDLCleanupObjects::UNKNOWN_)
    inObjType = COM_UNKNOWN_OBJECT_LIT;

  if (NOT inObjType.isNull()) {
    QualifiedName *qn = stmtCleanupNode->getTableNameAsQualifiedName();
    catName_ = qn->getCatalogName();
    schName_ = qn->getSchemaName();
    if (stmtCleanupNode->isPartition()) {
      char partitionTableName[256];
      getPartitionTableName(partitionTableName, 256, qn->getObjectName(), stmtCleanupNode->getPartitionName());
      objName_ = partitionTableName;
    } else {
      objName_ = qn->getObjectName();
    }

    char outObjType[10];
    objUID_ = getCleanupObjectUID(cliInterface, catName_.data(), schName_.data(), objName_.data(), inObjType.data(),
                                  outObjType, objectOwner_, &objectFlags_, &objDataUID_);
    if ((objUID_ == -1) &&  // not found
        (inObjType != COM_UNKNOWN_OBJECT_LIT && inObjType != COM_PRIVATE_SCHEMA_OBJECT_LIT &&
         inObjType != COM_SHARED_SCHEMA_OBJECT_LIT))  // type explicitly specified
    {
      // check if there is another object type with the same name.
      objUID_ = getCleanupObjectUID(cliInterface, catName_.data(), schName_.data(), objName_.data(),
                                    COM_UNKNOWN_OBJECT_LIT, outObjType, objectOwner_, &objectFlags_, &objDataUID_);
      if ((objUID_ != -1) &&  // found it
          (inObjType != outObjType)) {
        *CmpCommon::diags() << DgSqlCode(-4256);
        return -1;
      }
    }

    if (objUID_ != -1 && stmtCleanupNode->getType() == StmtDDLCleanupObjects::INDEX_ &&
        CmpSeabaseDDL::isMDflagsSet(objectFlags_, MD_OBJECTS_READ_ONLY_ENABLED) &&
        !Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)) {
      *CmpCommon::diags() << DgSqlCode(-8644);
      return -1;
    }

    if ((objUID_ != -1) && (stmtCleanupNode->getObjectUID() != -1) && (objUID_ != stmtCleanupNode->getObjectUID())) {
      *CmpCommon::diags() << DgSqlCode(-4253);
      return -1;
    }

    objType_ = "";
    if (inObjType == COM_UNKNOWN_OBJECT_LIT) {
      if (objUID_ == -1) {
        objType_ = COM_BASE_TABLE_OBJECT_LIT;
        objectOwner_ = -1;
      } else
        objType_ = outObjType;
    } else
      objType_ = inObjType;

    if ((objUID_ == -1) && (stmtCleanupNode->getObjectUID() != -1)) {
      CmpCommon::diags()->clear();
      objUID_ = stmtCleanupNode->getObjectUID();
    }
  } else if (stmtCleanupNode->getType() == StmtDDLCleanupObjects::OBJECT_UID_) {
    objUID_ = stmtCleanupNode->getObjectUID();

    cliRC = getCleanupObjectName(cliInterface, objUID_, catName_, schName_, objName_, objType_, objectOwner_,
                                 &objectFlags_, &objDataUID_);
    if (cliRC < 0) return -1;

  } else if (stmtCleanupNode->getType() == StmtDDLCleanupObjects::OBSOLETE_) {
    cleanupMetadataEntries_ = TRUE;
    objectOwner_ = -1;
  }

  hasInferiorPartitons_ = FALSE;
  if ((objType_ == COM_BASE_TABLE_OBJECT_LIT) && (objUID_ != -1)) {
    cliRC = hasInferiorPartitons(cliInterface, objUID_);
    if (cliRC < 0) {
      // Ignore it and regard as having Inferior Partitons.
      CmpCommon::diags()->clear();
      hasInferiorPartitons_ = TRUE;
    } else {
      hasInferiorPartitons_ = (cliRC > 0) ? TRUE : FALSE;
    }
  }

  isHive_ = FALSE;
  if (catName_ == HIVE_SYSTEM_CATALOG) isHive_ = TRUE;

  // generate hbase name that will be used to drop underlying hbase object
  extNameForHbase_ = "";

  // Table that has Inferior Partitons don't have hbase table.
  // But for cleanup, usually generate a name.
  if ((NOT isHive_) && ((objType_ == COM_BASE_TABLE_OBJECT_LIT) || (objType_ == COM_INDEX_OBJECT_LIT))) {
    const char *colonPos = NULL;
    if ((objUID_ == -1) && (inObjType == COM_UNKNOWN_OBJECT_LIT) &&
        (stmtCleanupNode->getOrigTableNameAsQualifiedName().getCatalogName().isNull()) &&
        (stmtCleanupNode->getOrigTableNameAsQualifiedName().getSchemaName().isNull())) {
      // if the specified name was not qualified with cat/sch and contains
      // namespace prefix, then use objectName as the hbase name.
      colonPos = strchr(objName_.data(), ':');
    }

    if (colonPos)
      extNameForHbase_ = objName_;
    else {
      if (NOT(catName_.isNull() || schName_.isNull() || objName_.isNull())) {
        NAString nameSpace;
        if (objUID_ > 0) {
          if (getTextFromMD(cliInterface, objUID_, COM_OBJECT_NAMESPACE, 0, nameSpace)) {
            return -1;
          }
        }

        extNameForHbase_ = genHBaseObjName(catName_, schName_, objName_, nameSpace, objDataUID_);
      }
    }
  }

  // If index, get base table information
  // This is needed to remove base table from caches so new definitions are available
  if (objType_ == COM_INDEX_OBJECT_LIT) {
    NAString btCatName;
    NAString btSchName;
    long btUID;
    int btObjOwner = 0;
    int btSchemaOwner = 0;

    // If parent table is not found, just continue
    if (getBaseTable(cliInterface, catName_, schName_, objName_, btCatName, btSchName, btObjName_, btUID, btObjOwner,
                     btSchemaOwner) < 0) {
      CmpCommon::diags()->clear();
      btObjName_ = "";
    }
  }

  // Make sure user has necessary privileges to perform drop
  if (!isDDLOperationAuthorized(SQLOperation::DROP_TABLE, objectOwner_, -1, objUID_, PrivMgr::ObjectLitToEnum(objType_),
                                NULL))

  {
    *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);

    return -1;
  }

  if (stmtCleanupNode->checkOnly()) checkOnly_ = TRUE;

  if (stmtCleanupNode->returnDetails()) returnDetails_ = TRUE;

  if (stmtCleanupNode->noHBaseDrop()) noHBaseDrop_ = TRUE;

  return 0;
}

short CmpSeabaseMDcleanup::processCleanupErrors(ExeCliInterface *cliInterface, NABoolean &errorSeen) {
  if (cliInterface) cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());

  if (stopOnError_) return -1;

  CmpCommon::diags()->negateAllErrors();

  errorSeen = TRUE;

  return 0;
}

short CmpSeabaseMDcleanup::gatherDependentObjects(ExeCliInterface *cliInterface, ExpHbaseInterface *ehi) {
  int cliRC = 0;
  char query[2000];

  NABoolean errorSeen = FALSE;
  if (objType_ == COM_BASE_TABLE_OBJECT_LIT) {
    // generate dependent index uid list
    str_sprintf(query, "select index_uid from %s.\"%s\".%s where base_table_uid = %ld", getSystemCatalog(),
                SEABASE_MD_SCHEMA, SEABASE_INDEXES, objUID_);

    indexesUIDlist_ = NULL;
    cliRC = cliInterface->fetchAllRows(indexesUIDlist_, query, 0, FALSE, FALSE, TRUE);
    if (cliRC < 0) {
      if (processCleanupErrors(cliInterface, errorSeen)) return -1;
    }

    // generate unique constr list
    str_sprintf(query, "select constraint_uid from %s.\"%s\".%s where table_uid = %ld and constraint_type = 'U' ",
                getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_TABLE_CONSTRAINTS, objUID_);

    uniqueConstrUIDlist_ = NULL;
    cliRC = cliInterface->fetchAllRows(uniqueConstrUIDlist_, query, 0, FALSE, FALSE, TRUE);
    if (cliRC < 0) {
      if (processCleanupErrors(cliInterface, errorSeen)) return -1;
    }

    // generate ref constr list
    str_sprintf(query, "select constraint_uid from %s.\"%s\".%s where table_uid = %ld and constraint_type = 'F' ",
                getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_TABLE_CONSTRAINTS, objUID_);

    refConstrUIDlist_ = NULL;
    cliRC = cliInterface->fetchAllRows(refConstrUIDlist_, query, 0, FALSE, FALSE, TRUE);
    if (cliRC < 0) {
      if (processCleanupErrors(cliInterface, errorSeen)) return -1;
    }

    // generate sequences list for identity columns
    str_sprintf(
        query,
        "select seq_uid from %s.\"%s\".%s where seq_uid in (select object_uid from %s.\"%s\".%s where catalog_name = "
        "'%s' and schema_name = '%s' and object_name like '\\_%s\\_%s\\_%s\\_%%' escape '\\' and object_type = 'SG')",
        getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_SEQ_GEN, getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS,
        catName_.data(), schName_.data(), catName_.data(), schName_.data(), objName_.data());

    seqUIDlist_ = NULL;
    cliRC = cliInterface->fetchAllRows(seqUIDlist_, query, 0, FALSE, FALSE, TRUE);
    if (cliRC < 0) {
      if (processCleanupErrors(cliInterface, errorSeen)) return -1;
    }

  }  // COM_BASE_TABLE_OBJECT

  if ((objType_ == COM_BASE_TABLE_OBJECT_LIT) || (objType_ == COM_VIEW_OBJECT_LIT)) {
    // get views that use this object.
    cliRC = getUsingViews(cliInterface, objUID_, usingViewsList_);
    if (cliRC < 0) {
      if (processCleanupErrors(NULL, errorSeen)) return -1;
    }
  }

  if (isHive_) {
    // if this hive table has an external table, get its uid
    NAString extTableName;
    extTableName = ComConvertNativeNameToTrafName(catName_, schName_, objName_);
    if (NOT extTableName.isNull()) {
      QualifiedName qn(extTableName, 3);
      long extObjUID = getObjectUID(cliInterface, qn.getCatalogName(), qn.getSchemaName(), qn.getObjectName(),
                                     COM_BASE_TABLE_OBJECT_LIT, NULL, NULL, NULL, FALSE, FALSE);
      if (extObjUID > 0) {
        char query[1000];
        str_sprintf(query, "cleanup uid %ld", extObjUID);
        cliRC = cliInterface->executeImmediate(query);
        if (cliRC < 0) {
          cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
          return -1;
        }
      }
    }
  }

  if (errorSeen)
    return -1;
  else
    return 0;
}

short CmpSeabaseMDcleanup::deleteMDentries(ExeCliInterface *cliInterface) {
  int cliRC = 0;
  char query[1000];

  NABoolean errorSeen = FALSE;

  // OBJECTS table

  // Must first hide the index to OBJECTS, because the delete plan would otherwise
  // likely access OBJECTS_UNIQ_IDX first then join that to OBJECTS (as OBJECT_UID
  // is the leading part of the index key). If the index row were missing, we'd
  // fail to delete the base table row. Right now OBJECTS is the only metadata
  // table with an index, so this is the only place we need to take this precaution.

  cliRC = cliInterface->holdAndSetCQD("HIDE_INDEXES", "ALL", NULL);
  if (cliRC < 0) {
    if (processCleanupErrors(cliInterface, errorSeen)) return -1;
  }

  // Now delete from OBJECTS (but not its index)

  str_sprintf(query, "delete from %s.\"%s\".%s where object_uid = %ld", getSystemCatalog(), SEABASE_MD_SCHEMA,
              SEABASE_OBJECTS, objUID_);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    if (processCleanupErrors(cliInterface, errorSeen)) return -1;
  }

  // Restore previous setting of CQD HIDE_INDEXES

  cliRC = cliInterface->restoreCQD("HIDE_INDEXES", NULL);
  if (cliRC < 0) {
    if (processCleanupErrors(cliInterface, errorSeen)) return -1;
  }

  // OBJECTS index
  str_sprintf(query, "delete from table(index_table %s.\"%s\".%s) where \"OBJECT_UID@\" = %ld", getSystemCatalog(),
              SEABASE_MD_SCHEMA, SEABASE_OBJECTS_UNIQ_IDX, objUID_);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    if (processCleanupErrors(cliInterface, errorSeen)) return -1;
  }

  // COLUMNS table
  str_sprintf(query, "delete from %s.\"%s\".%s where object_uid = %ld", getSystemCatalog(), SEABASE_MD_SCHEMA,
              SEABASE_COLUMNS, objUID_);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    if (processCleanupErrors(cliInterface, errorSeen)) return -1;
  }

  // KEYS table
  str_sprintf(query, "delete from %s.\"%s\".%s where object_uid = %ld", getSystemCatalog(), SEABASE_MD_SCHEMA,
              SEABASE_KEYS, objUID_);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    if (processCleanupErrors(cliInterface, errorSeen)) return -1;
  }

  // TABLES table
  str_sprintf(query, "delete from %s.\"%s\".%s where table_uid = %ld", getSystemCatalog(), SEABASE_MD_SCHEMA,
              SEABASE_TABLES, objUID_);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    if (processCleanupErrors(cliInterface, errorSeen)) return -1;
  }

  // delete index entries
  if (objType_ == COM_INDEX_OBJECT_LIT)
    str_sprintf(query, "delete from %s.\"%s\".%s where index_uid = %ld ", getSystemCatalog(), SEABASE_MD_SCHEMA,
                SEABASE_INDEXES, objUID_);
  else
    str_sprintf(query, "delete from %s.\"%s\".%s where base_table_uid = %ld", getSystemCatalog(), SEABASE_MD_SCHEMA,
                SEABASE_INDEXES, objUID_);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    if (processCleanupErrors(cliInterface, errorSeen)) return -1;
  }

  // TEXT entries
  str_sprintf(query, "delete from %s.\"%s\".%s where text_uid = %ld", getSystemCatalog(), SEABASE_MD_SCHEMA,
              SEABASE_TEXT, objUID_);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    if (processCleanupErrors(cliInterface, errorSeen)) return -1;
  }

  // LOB MD entries
  str_sprintf(query, "delete from %s.\"%s\".%s where table_uid = %ld", getSystemCatalog(), SEABASE_MD_SCHEMA,
              SEABASE_LOB_COLUMNS, objUID_);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    // if LOB_COLUMNS does not exist, ignore it. This could be the
    // case where MD has not been updated to include V2 lob_columns.
    if ((cliRC != -4082) && (cliRC != -8448)) {
      if (processCleanupErrors(cliInterface, errorSeen)) return -1;
    }
  }

  if (errorSeen)
    return -1;
  else
    return 0;
}

short CmpSeabaseMDcleanup::deleteMDConstrEntries(ExeCliInterface *cliInterface) {
  int cliRC = 0;
  char query[1000];

  if (uniqueConstrUIDlist_) {
    uniqueConstrUIDlist_->position();
    for (size_t i = 0; i < uniqueConstrUIDlist_->numEntries(); i++) {
      long ucUID;

      OutputInfo *oi = (OutputInfo *)uniqueConstrUIDlist_->getCurr();
      ucUID = *(long *)oi->get(0);

      // If unique constraint is being referenced by other tables, remove foreign key constraints
      // First get the referencing table information
      Queue *referencingTableQueue = NULL;
      NAList<NAString> referencingTables(STMTHEAP);
      NAList<NABoolean> storedDescs(STMTHEAP);
      str_sprintf(query,
                  "select flags, trim(catalog_name) || '.\"' || "
                  "trim(schema_name) || '\".\"' || trim(object_name) || '\"' "
                  "from %s.\"%s\".%s where object_uid in "
                  "(select table_uid from %s.\"%s\".%s "
                  "where constraint_uid in (select ref_constraint_uid from %s.\"%s\".%s "
                  "where unique_constraint_uid = %ld))",
                  getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS, getSystemCatalog(), SEABASE_MD_SCHEMA,
                  SEABASE_TABLE_CONSTRAINTS, getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_REF_CONSTRAINTS, ucUID);
      cliRC = cliInterface->fetchAllRows(referencingTableQueue, query, 0, FALSE, FALSE, TRUE);
      if (cliRC < 0) {
        cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
        return -1;
      }

      referencingTableQueue->position();
      for (size_t j = 0; j < referencingTableQueue->numEntries(); j++) {
        OutputInfo *roi = (OutputInfo *)referencingTableQueue->getCurr();
        long flags = *(long *)roi->get(0);
        NABoolean storedDesc = ((flags & MD_OBJECTS_STORED_DESC) != 0);
        storedDescs.insert(storedDesc);
        NAString referencingTable = (char *)roi->get(1);
        referencingTables.insert(referencingTable);
        referencingTableQueue->advance();
      }

      // Remove the referencing FOREIGN KEY constraint, part 1 objects table
      str_sprintf(query,
                  "delete from %s.\"%s\".%s where object_uid in "
                  "(select foreign_constraint_uid from %s.\"%s\".%s "
                  "where unique_constraint_uid = %ld)",
                  getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS, getSystemCatalog(), SEABASE_MD_SCHEMA,
                  SEABASE_UNIQUE_REF_CONSTR_USAGE, ucUID);
      cliRC = cliInterface->executeImmediate(query);
      if (cliRC < 0) {
        cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
        return -1;
      }

      // Remove the referencing FOREIGN KEY constraint, part 2 table_constraints
      str_sprintf(query,
                  "delete from %s.\"%s\".%s where constraint_uid in "
                  "(select foreign_constraint_uid from %s.\"%s\".%s "
                  "where unique_constraint_uid = %ld)",
                  getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_TABLE_CONSTRAINTS, getSystemCatalog(),
                  SEABASE_MD_SCHEMA, SEABASE_UNIQUE_REF_CONSTR_USAGE, ucUID);
      cliRC = cliInterface->executeImmediate(query);
      if (cliRC < 0) {
        cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
        return -1;
      }

      // ref constraints
      str_sprintf(query, "delete from %s.\"%s\".%s where unique_constraint_uid = %ld", getSystemCatalog(),
                  SEABASE_MD_SCHEMA, SEABASE_REF_CONSTRAINTS, ucUID);
      cliRC = cliInterface->executeImmediate(query);
      if (cliRC < 0) {
        cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
        return -1;
      }

      // unique constraint usage
      str_sprintf(query, "delete from %s.\"%s\".%s where unique_constraint_uid = %ld", getSystemCatalog(),
                  SEABASE_MD_SCHEMA, SEABASE_UNIQUE_REF_CONSTR_USAGE, ucUID);
      cliRC = cliInterface->executeImmediate(query);
      if (cliRC < 0) {
        cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
        return -1;
      }

      // Update stored descriptor/shared cache to remove the RI constraint
      // from the referencing table
      for (int k = 0; k < referencingTables.entries(); k++) {
        NABoolean storedDesc = storedDescs[k];
        if (storedDesc) {
          NAString referencingTable = referencingTables[k];
          str_sprintf(query, "alter table %s generate stored desc", (char *)referencingTable.data());
          cliRC = cliInterface->executeImmediate(query);
          if (cliRC < 0) {
            cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
            return -1;
          }
        }
      }

      uniqueConstrUIDlist_->advance();
    }  // for
  }

  if (refConstrUIDlist_) {
    refConstrUIDlist_->position();
    for (size_t i = 0; i < refConstrUIDlist_->numEntries(); i++) {
      long rcUID;

      OutputInfo *oi = (OutputInfo *)refConstrUIDlist_->getCurr();
      rcUID = *(long *)oi->get(0);
      str_sprintf(query, "delete from %s.\"%s\".%s where ref_constraint_uid = %ld", getSystemCatalog(),
                  SEABASE_MD_SCHEMA, SEABASE_REF_CONSTRAINTS, rcUID);
      cliRC = cliInterface->executeImmediate(query);
      if (cliRC < 0) {
        cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
        return -1;
      }

      str_sprintf(query, "delete from %s.\"%s\".%s where foreign_constraint_uid = %ld", getSystemCatalog(),
                  SEABASE_MD_SCHEMA, SEABASE_UNIQUE_REF_CONSTR_USAGE, rcUID);
      cliRC = cliInterface->executeImmediate(query);
      if (cliRC < 0) {
        cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
        return -1;
      }

      refConstrUIDlist_->advance();
    }  // for
  }

  str_sprintf(
      query,
      "delete from %s.\"%s\".%s where object_uid in (select constraint_uid from %s.\"%s\".%s where table_uid = %ld)",
      getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS, getSystemCatalog(), SEABASE_MD_SCHEMA,
      SEABASE_TABLE_CONSTRAINTS, objUID_);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  str_sprintf(query, "delete from %s.\"%s\".%s where table_uid = %ld", getSystemCatalog(), SEABASE_MD_SCHEMA,
              SEABASE_TABLE_CONSTRAINTS, objUID_);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  return 0;
}

short CmpSeabaseMDcleanup::deleteMDViewEntries(ExeCliInterface *cliInterface) {
  int cliRC = 0;
  char query[1000];

  str_sprintf(query, "delete from %s.\"%s\".%s where view_uid = %ld", getSystemCatalog(), SEABASE_MD_SCHEMA,
              SEABASE_VIEWS, objUID_);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  str_sprintf(query, "delete from %s.\"%s\".%s where used_object_uid = %ld and used_object_type = '%s' ",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_VIEWS_USAGE, objUID_, objType_.data());
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  return 0;
}

short CmpSeabaseMDcleanup::deleteHistogramEntries(ExeCliInterface *cliInterface) {
  if (isHistogramTable(objName_)) return 0;

  int cliRC = 0;
  char query[1000];

  if ((objType_ == COM_BASE_TABLE_OBJECT_LIT) && (objUID_ > 0) && (NOT catName_.isNull()) && (NOT schName_.isNull())) {
    if (NOT isHive_) {
      if (dropSeabaseStats(cliInterface, catName_.data(), schName_.data(), objUID_)) return -1;
    } else {
      if (dropSeabaseStats(cliInterface, HIVE_STATS_CATALOG, HIVE_STATS_SCHEMA_NO_QUOTES, objUID_)) return -1;
    }
  }

  return 0;
}

short CmpSeabaseMDcleanup::dropIndexes(ExeCliInterface *cliInterface) {
  int cliRC = 0;
  char query[1000];

  if ((!indexesUIDlist_) || (indexesUIDlist_->numEntries() == 0)) return 0;

  indexesUIDlist_->position();
  for (size_t i = 0; i < indexesUIDlist_->numEntries(); i++) {
    long iUID;

    OutputInfo *oi = (OutputInfo *)indexesUIDlist_->getCurr();
    iUID = *(long *)oi->get(0);

    str_sprintf(query, "cleanup uid %ld", iUID);
    cliRC = cliInterface->executeImmediate(query);
    if (cliRC < 0) {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      return -1;
    }

    indexesUIDlist_->advance();
  }

  return 0;
}

short CmpSeabaseMDcleanup::dropSequences(ExeCliInterface *cliInterface) {
  int cliRC = 0;
  char query[1000];

  if ((!seqUIDlist_) || (seqUIDlist_->numEntries() == 0)) return 0;

  seqUIDlist_->position();
  for (size_t i = 0; i < seqUIDlist_->numEntries(); i++) {
    long iUID;

    OutputInfo *oi = (OutputInfo *)seqUIDlist_->getCurr();
    iUID = *(long *)oi->get(0);

    str_sprintf(query, "cleanup uid %ld", iUID);
    cliRC = cliInterface->executeImmediate(query);
    if (cliRC < 0) {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      return -1;
    }

    seqUIDlist_->advance();
  }

  return 0;
}

short CmpSeabaseMDcleanup::dropUsingViews(ExeCliInterface *cliInterface) {
  int cliRC = 0;
  char query[1000];

  if ((!usingViewsList_) || (usingViewsList_->numEntries() == 0)) return 0;

  usingViewsList_->position();
  for (size_t i = 0; i < usingViewsList_->numEntries(); i++) {
    OutputInfo *oi = (OutputInfo *)usingViewsList_->getCurr();
    char *viewName = (char *)oi->get(0);

    str_sprintf(query, "cleanup view %s ", viewName);
    cliRC = cliInterface->executeImmediate(query);
    if (cliRC < 0) {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      return -1;
    }

    usingViewsList_->advance();
  }

  return 0;
}

short CmpSeabaseMDcleanup::deletePrivs(ExeCliInterface *cliInterface) {
  int cliRC = 0;
  char query[1000];

  if (NOT isAuthorizationEnabled()) return 0;

  str_sprintf(query, "delete from %s.\"%s\".%s where object_uid = %ld", getSystemCatalog(), SEABASE_PRIVMGR_SCHEMA,
              PRIVMGR_OBJECT_PRIVILEGES, objUID_);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  str_sprintf(query, "delete from %s.\"%s\".%s where object_uid = %ld", getSystemCatalog(), SEABASE_PRIVMGR_SCHEMA,
              PRIVMGR_COLUMN_PRIVILEGES, objUID_);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  return 0;
}

short CmpSeabaseMDcleanup::deleteSchemaPrivs(ExeCliInterface *cliInterface) {
  int cliRC = 0;
  char query[1000];

  if (NOT isAuthorizationEnabled()) return 0;

  str_sprintf(query, "delete from %s.\"%s\".%s where schema_uid = %ld", getSystemCatalog(), SEABASE_PRIVMGR_SCHEMA,
              PRIVMGR_SCHEMA_PRIVILEGES, objUID_);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  return 0;
}

short CmpSeabaseMDcleanup::deleteTenantSchemaUsages(ExeCliInterface *cliInterface) {
  int cliRC = 0;

  cliRC = isTenantMetadataInitialized(cliInterface);
  if (cliRC != 1) return cliRC;

  char query[1000];

  // Remove usage
  str_sprintf(query, "delete from %s.\"%s\".%s where usage_uid = %ld and usage_type = 'S'", getSystemCatalog(),
              SEABASE_TENANT_SCHEMA, SEABASE_TENANT_USAGE, objUID_);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  // Set default schema to none (0)
  str_sprintf(query,
              "update %s.\"%s\".%s set default_schema_uid = 0 "
              "where default_schema_uid = %ld",
              getSystemCatalog(), SEABASE_TENANT_SCHEMA, SEABASE_TENANTS, objUID_);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  return 0;
}

short CmpSeabaseMDcleanup::deletePartitionEntries(ExeCliInterface *cliInterface) {
  int cliRC = 0;
  char query[1000];

  NABoolean errorSeen = FALSE;
  if (isMDflagsSet(objectFlags_, MD_PARTITION_TABLE_V2)) {
    str_sprintf(query, "delete from %s.\"%s\".%s where table_uid = %ld", getSystemCatalog(), SEABASE_MD_SCHEMA,
                SEABASE_TABLE_PARTITION, objUID_);
    cliRC = cliInterface->executeImmediate(query);
    if (cliRC < 0) {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      return -1;
    }
  } else if (isMDflagsSet(objectFlags_, MD_PARTITION_V2)) {
    str_sprintf(query, "delete from %s.\"%s\".%s where partition_uid = %ld", getSystemCatalog(), SEABASE_MD_SCHEMA,
                SEABASE_PARTITIONS, objUID_);
    cliRC = cliInterface->executeImmediate(query);
    if (cliRC < 0) {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      return -1;
    }
  }

  return 0;
}

void CmpSeabaseMDcleanup::cleanupSchemaObjects(ExeCliInterface *cliInterface) {
  int cliRC = 0;
  char query[1000];
  long schemaUid = 0;

  NABoolean errorSeen = FALSE;

  if (objUID_ == -1) {
    CmpCommon::diags()->clear();

    ComSchemaName sn(catName_, schName_);
    *CmpCommon::diags() << DgSqlCode(4255) << DgSchemaName(sn.getExternalName().data());
  }

  Queue *schObjList = NULL;
  str_sprintf(
      query,
      "select object_uid, object_type, object_name from %s.\"%s\".%s where catalog_name = '%s' and schema_name = '%s' ",
      getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS, catName_.data(), schName_.data());

  schObjList = NULL;
  cliRC = cliInterface->fetchAllRows(schObjList, query, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    if (processCleanupErrors(cliInterface, errorSeen)) return;
  }

  stopOnError_ = FALSE;
  NABoolean cannotDropSchema = FALSE;

  // Drop histogram tables first
  schObjList->position();
  for (size_t i = 0; i < schObjList->numEntries(); i++) {
    OutputInfo *oi = (OutputInfo *)schObjList->getCurr();
    long uid = *(long *)oi->get(0);
    NAString obj_name((char *)oi->get(2));
    if (isHistogramTable(obj_name)) {
      str_sprintf(query, "cleanup uid %ld", uid);
      cliRC = cliInterface->executeImmediate(query);
      if (cliRC < 0) {
        if (processCleanupErrors(NULL, errorSeen)) return;
      }

      CorrName cn(obj_name, STMTHEAP, schName_, catName_);
      ActiveSchemaDB()->getNATableDB()->removeNATable(cn, ComQiScope::REMOVE_FROM_ALL_USERS, COM_BASE_TABLE_OBJECT,
                                                      FALSE, FALSE);
    } else if (obj_name == NAString(SEABASE_SCHEMA_OBJECTNAME))
      schemaUid = uid;

    schObjList->advance();
  }

  // Now drop remaining objects
  schObjList->position();
  for (size_t i = 0; i < schObjList->numEntries(); i++) {
    OutputInfo *oi = (OutputInfo *)schObjList->getCurr();
    long uid = *(long *)oi->get(0);

    NAString obj_type((char *)oi->get(1));
    NAString obj_name((char *)oi->get(2));

    if ((obj_type == COM_LIBRARY_OBJECT_LIT) || (obj_type == COM_STORED_PROCEDURE_OBJECT_LIT) ||
        (obj_type == COM_USER_DEFINED_ROUTINE_OBJECT)) {
      schObjList->advance();
      cannotDropSchema = TRUE;
      continue;
    }

    if (NOT((obj_type == COM_BASE_TABLE_OBJECT_LIT) || (obj_type == COM_INDEX_OBJECT_LIT) ||
            (obj_type == COM_VIEW_OBJECT_LIT) || (obj_type == COM_SEQUENCE_GENERATOR_OBJECT_LIT))) {
      schObjList->advance();
      continue;
    }

    CorrName cn(obj_name, STMTHEAP, schName_, catName_);
    if (obj_type == COM_BASE_TABLE_OBJECT_LIT) {
      if (lockObjectDDL(cn.getQualifiedNameObj(), COM_BASE_TABLE_OBJECT, cn.isVolatile(), cn.isExternal()) < 0) {
        return;
      }
    }

    str_sprintf(query, "cleanup uid %ld", uid);
    cliRC = cliInterface->executeImmediate(query);
    if (cliRC < 0) {
      if (processCleanupErrors(NULL, errorSeen)) return;
    }

    // Remove from shared cache, if it exists
    if (obj_type == COM_BASE_TABLE_OBJECT_LIT || obj_type == COM_INDEX_OBJECT_LIT) {
      insertIntoSharedCacheList(catName_, schName_, obj_name,
                                (obj_type == COM_BASE_TABLE_OBJECT_LIT) ? COM_BASE_TABLE_OBJECT : COM_INDEX_OBJECT,
                                SharedCacheDDLInfo::DDL_DELETE);

      insertIntoSharedCacheList(catName_, schName_, obj_name,
                                (obj_type == COM_BASE_TABLE_OBJECT_LIT) ? COM_BASE_TABLE_OBJECT : COM_INDEX_OBJECT,
                                SharedCacheDDLInfo::DDL_DELETE, SharedCacheDDLInfo::SHARED_DATA_CACHE);
    }

    ActiveSchemaDB()->getNATableDB()->removeNATable(cn, ComQiScope::REMOVE_FROM_ALL_USERS, COM_BASE_TABLE_OBJECT, FALSE,
                                                    FALSE);

    if (obj_type == COM_BASE_TABLE_OBJECT_LIT) {
      if (unlockObjectDDL(cn.getQualifiedNameObj(), COM_BASE_TABLE_OBJECT, cn.isVolatile(), cn.isExternal()) < 0) {
        return;
      }
    }

    schObjList->advance();
  }  // for

  cliRC = deleteSchemaPrivs(cliInterface);
  if (cliRC < 0) {
    if (processCleanupErrors(NULL, errorSeen)) return;
  }

  cliRC = deleteTenantSchemaUsages(cliInterface);
  if (cliRC < 0) {
    if (processCleanupErrors(NULL, errorSeen)) return;
  }

  if (NOT cannotDropSchema) {
    // delete schema object row from objects table
    str_sprintf(query,
                "delete from  %s.\"%s\".%s where catalog_name = '%s' and schema_name  = '%s' and object_name = '%s' ",
                getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS, (char *)catName_.data(),
                (char *)schName_.data(), SEABASE_SCHEMA_OBJECTNAME);
    cliRC = cliInterface->executeImmediate(query);
    if (cliRC < 0) {
      if (processCleanupErrors(NULL, errorSeen)) return;
    }

    // delete schema object row from text table (eg. stored desc, namespace)
    str_sprintf(query, "delete from  %s.\"%s\".%s where text_uid = %ld ", getSystemCatalog(), SEABASE_MD_SCHEMA,
                SEABASE_TEXT, schemaUid);
    cliRC = cliInterface->executeImmediate(query);
    if (cliRC < 0) {
      if (processCleanupErrors(NULL, errorSeen)) return;
    }

    // Remove from shared cache, if exists
    insertIntoSharedCacheList(
        catName_, schName_, NAString(SEABASE_SCHEMA_OBJECTNAME),
        (objType_ == COM_SHARED_SCHEMA_OBJECT_LIT ? COM_SHARED_SCHEMA_OBJECT : COM_PRIVATE_SCHEMA_OBJECT),
        SharedCacheDDLInfo::DDL_DELETE);

    CorrName cn(SEABASE_SCHEMA_OBJECTNAME, STMTHEAP, schName_, catName_);
    ActiveSchemaDB()->getNATableDB()->removeNATable(
        cn, ComQiScope::REMOVE_FROM_ALL_USERS,
        (objType_ == COM_SHARED_SCHEMA_OBJECT_LIT ? COM_SHARED_SCHEMA_OBJECT : COM_PRIVATE_SCHEMA_OBJECT), FALSE,
        FALSE);
  }

  if (!ComIsTrafodionReservedSchemaName(schName_) && isMDflagsSet(objectFlags_, MD_OBJECTS_INCR_BACKUP_ENABLED)) {
    cliRC = updateSeabaseXDCDDLTable(cliInterface, catName_.data(), schName_.data(), objName_.data(), objType_);
    if (cliRC < 0) {
      if (processCleanupErrors(NULL, errorSeen)) return;
    }
  }

  finalizeSharedCache();
  return;
}

short CmpSeabaseMDcleanup::cleanupUIDs(ExeCliInterface *cliInterface, Queue *entriesList, CmpDDLwithStatusInfo *dws) {
  int cliRC = 0;
  char query[1000];
  NABoolean errorSeen = FALSE;

  if (checkOnly_) return 0;

  entriesList->position();
  for (size_t i = 0; i < entriesList->numEntries(); i++) {
    OutputInfo *oi = (OutputInfo *)entriesList->getCurr();
    long objUID = *(long *)oi->get(0);

    str_sprintf(query, "cleanup uid %ld", objUID);
    cliRC = cliInterface->executeImmediate(query);
    if (cliRC < 0) {
      if (processCleanupErrors(NULL, errorSeen)) return -1;
    }

    entriesList->advance();
  }

  return 0;
}

void CmpSeabaseMDcleanup::cleanupHBaseObject(const StmtDDLCleanupObjects *stmtCleanupNode,
                                             ExeCliInterface *cliInterface) {
  int cliRC = 0;
  char query[1000];
  NABoolean errorSeen = FALSE;

  NAString objName(stmtCleanupNode->getTableNameAsQualifiedName()->getObjectName());

  // drop external table
  str_sprintf(query, "drop external table if exists %s;", objName.data());
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    if (processCleanupErrors(NULL, errorSeen)) return;
  }

  // unregister registered table
  str_sprintf(query, "unregister hbase table if exists %s cleanup;", objName.data());
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    if (processCleanupErrors(NULL, errorSeen)) return;
  }

  return;
}

short CmpSeabaseMDcleanup::addReturnDetailsEntry(ExeCliInterface *cliInterface, Queue *&list, const char *value,
                                                 NABoolean init, NABoolean isUID) {
  if (NOT returnDetails_) return 0;

  if (init) {
    cliInterface->initializeInfoList(list, TRUE);
    currReturnEntry_ = 0;

    return 0;
  }

  currReturnEntry_++;

  OutputInfo *oi = new (STMTHEAP) OutputInfo(1);

  char *r = new (STMTHEAP) char[100 + strlen(value) + 1];
  str_sprintf(r, "    Entry #%d%s %s", currReturnEntry_, (isUID ? "(UID):   " : "(OBJECT):"), value);
  oi->insert(0, r, strlen(r) + 1);
  list->insert(oi);

  return 0;
}

short CmpSeabaseMDcleanup::addReturnDetailsEntryForText(ExeCliInterface *cliInterface, Queue *&list, long objUID,
                                                        int objType, NABoolean init) {
  if (NOT returnDetails_) return 0;

  if (init) {
    cliInterface->initializeInfoList(list, TRUE);
    currReturnEntry_ = 0;

    return 0;
  }

  currReturnEntry_++;

  OutputInfo *oi = new (STMTHEAP) OutputInfo(1);

  char *r = new (STMTHEAP) char[123];
  NAString textType;

  {
    switch (objType) {
      case COM_VIEW_TEXT:
        textType = "VIEW_TEXT        ";
        break;

      case COM_CHECK_CONSTR_TEXT:
        textType = "CHECK_CONSTR     ";
        break;

      case COM_HBASE_OPTIONS_TEXT:
        textType = "HBASE_OPTIONS    ";
        break;

        // case COM_TABLE_COMMENT_TEXT:
        //   textType = "TABLE_COMMENT_TEXT";
        //   break;

      case COM_OBJECT_COMMENT_TEXT:
        textType = "HBASE_OPTIONS    ";
        break;

      case COM_COMPUTED_COL_TEXT:
        textType = "COMPUTED_COL     ";
        break;

      case COM_HBASE_COL_FAMILY_TEXT:
        textType = "HBASE_COL_FAMILY ";
        break;

      case COM_HBASE_SPLIT_TEXT:
        textType = "HBASE_SPLIT      ";
        break;

      case COM_STORED_DESC_TEXT:
        textType = "STORED_DESC      ";
        break;

      case COM_VIEW_REF_COLS_TEXT:
        textType = "VIEW_REF_COLS    ";
        break;

      case COM_OBJECT_NAMESPACE:
        textType = "OBJECT_NAMESPACE ";
        break;

      case COM_LOCKED_OBJECT_BR_TAG:
        textType = "LOCKED_OBJ_BR_TAG";
        break;

      case COM_HOTCOLD_TEXT:
        textType = "HOTCOLD_TEXT     ";
        break;

      case COM_COLUMN_COMMENT_TEXT:
        textType = "COLUMN_COMMENT   ";
        break;

      case COM_OBJECT_NGRAM_TEXT:
        textType = "OBJECT_NGRAM     ";
        break;

      case COM_ROUTINE_TEXT:
        textType = "ROUTINE_TEXT     ";
        break;

      case COM_COMPOSITE_DEFN_TEXT:
        textType = "COMPOSITE_DEFN   ";
        break;

      case COM_TRIGGER_TEXT:
        textType = "TRIGGER_TEXT     ";
        break;

      case COM_USER_QUERYCACHE_TEXT:
        textType = "USER_QUERYCACHE  ";
        break;

      default:
        textType = "UNKOWN_OPTION    ";
        break;
    }
  }

  str_sprintf(r, "    Entry #%d(%s):  %ld", currReturnEntry_, textType.data(), objUID);

  oi->insert(0, r, strlen(r) + 1);
  list->insert(oi);

  return 0;
}

short CmpSeabaseMDcleanup::addReturnDetailsEntryFromList(ExeCliInterface *cliInterface, Queue *fromList,
                                                         int fromIndex, Queue *toList, NABoolean isUID,
                                                         NABoolean processTextInfo) {
  int cliRC = 0;
  char query[1000];
  NABoolean errorSeen = FALSE;

  if (NOT returnDetails_) return 0;

  fromList->position();
  for (size_t i = 0; i < fromList->numEntries(); i++) {
    OutputInfo *oi = (OutputInfo *)fromList->getCurr();
    char *val = (char *)oi->get(fromIndex);

    if (processTextInfo) {
      long objUID = *(long *)oi->get(0);
      int objType = *(int *)oi->get(1);
      if (addReturnDetailsEntryForText(cliInterface, toList, objUID, objType, FALSE)) return -1;
    } else {
      if (addReturnDetailsEntry(cliInterface, toList, val, FALSE, isUID)) return -1;
    }

    fromList->advance();
  }

  return 0;
}

short CmpSeabaseMDcleanup::cleanupOrphanObjectsEntries(ExeCliInterface *cliInterface, ExpHbaseInterface *ehi,
                                                       CmpDDLwithStatusInfo *dws) {
  int cliRC = 0;
  char query[2000];
  NABoolean errorSeen = FALSE;

  // find out all entries which do not have corresponding hbase objects.
  // Do not include metadata, repository and external tables.
  // Level 1 patitions don't have record in objects table.
  str_sprintf(
      query,
      "select O.object_uid, 0, trim(O.catalog_name), trim(O.schema_name), trim(O.object_name), case when T.text is not "
      "null then trim(T.text) else '' end from %s.\"%s\".%s O left join %s.\"%s\".%s T on O.object_uid = T.text_uid "
      "and T.text_type = %d where O.catalog_name = '%s' and O.schema_name not in ( '_MD_', '_REPOS_', '_PRIVMGR_MD_') "
      "and O.schema_name not like '|_HV|_%%|_' escape '|'  and O.schema_name not like '|_HB|_%%|_' escape '|' and "
      "(O.object_type = 'BT' or O.object_type = 'IX') and (bitand(O.flags, %d) = 0) ",
      getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS, getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_TEXT,
      COM_OBJECT_NAMESPACE, getSystemCatalog(), MD_PARTITION_TABLE_V2);

  cliRC = cliInterface->fetchRowsPrologue(query);
  if (cliRC < 0) {
    if (processCleanupErrors(cliInterface, errorSeen)) return -1;
  }

  obsoleteEntriesList_ = new (STMTHEAP) Queue(STMTHEAP);
  returnDetailsList_ = NULL;
  addReturnDetailsEntry(cliInterface, returnDetailsList_, NULL, TRUE);

  while (cliRC != 100) {
    cliRC = cliInterface->fetch();
    if (cliRC < 0) {
      if (processCleanupErrors(cliInterface, errorSeen)) return -1;
    }

    if (cliRC != 100) {
      char *ptr;
      int len;

      cliInterface->getPtrAndLen(1, ptr, len);
      long objUID = *(long *)ptr;

      cliInterface->getPtrAndLen(2, ptr, len);
      long objDataUID = *(long *)ptr;

      cliInterface->getPtrAndLen(3, ptr, len);
      NAString catName(ptr, len);

      cliInterface->getPtrAndLen(4, ptr, len);
      NAString schName(ptr, len);

      cliInterface->getPtrAndLen(5, ptr, len);
      NAString objName(ptr, len);

      cliInterface->getPtrAndLen(6, ptr, len);
      NAString nameSpace(ptr, len);

      const NAString extNameForHbase = genHBaseObjName(catName, schName, objName, nameSpace, objDataUID);

      // check to see if this object exists in hbase
      short rc = existsInHbase(extNameForHbase, ehi);  // exists
      if (rc == 0)                                     // does not exist
      {
        OutputInfo *oi = new (STMTHEAP) OutputInfo(1);
        char *r = new (STMTHEAP) char[sizeof(long)];
        str_cpy_all(r, (char *)&objUID, sizeof(long));
        oi->insert(0, r, sizeof(long));

        obsoleteEntriesList_->insert(oi);

        NAString trafName = genHBaseObjName(catName, schName, objName, NAString(""));
        if (addReturnDetailsEntry(cliInterface, returnDetailsList_, trafName.data(), FALSE)) return -1;
      }
    }
  }

  cliRC = cliInterface->fetchRowsEpilogue(0);
  if (cliRC < 0) {
    if (processCleanupErrors(cliInterface, errorSeen)) return -1;
  }

  if (cleanupUIDs(cliInterface, obsoleteEntriesList_, dws)) return -1;

  numOrphanMetadataEntries_ = obsoleteEntriesList_->numEntries();

  return 0;
}

// cleanup user objects that exist in hbase but not in metadata.
short CmpSeabaseMDcleanup::cleanupOrphanHbaseEntries(ExeCliInterface *cliInterface, ExpHbaseInterface *ehi,
                                                     CmpDDLwithStatusInfo *dws) {
  int cliRC = 0;
  char query[1000];
  NABoolean errorSeen = FALSE;

  // list all hbase objects that start with TRAFODION.*
  NAArray<HbaseStr> *listArray = ehi->listAll("");
  if (!listArray) return -1;

  returnDetailsList_ = NULL;
  addReturnDetailsEntry(cliInterface, returnDetailsList_, NULL, TRUE);

  numOrphanHbaseEntries_ = 0;
  for (int i = 0; i < listArray->entries(); i++) {
    char cBuf[1000];

    HbaseStr *hbaseStr = &listArray->at(i);
    if (hbaseStr->len >= sizeof(cBuf)) hbaseStr->len = sizeof(cBuf) - 1;
    strncpy(cBuf, hbaseStr->val, hbaseStr->len);
    cBuf[hbaseStr->len] = '\0';
    char *c = cBuf;
    int numParts = 0;
    char *parts[4];
    NAString originStr = NAString(cBuf);
    LateNameInfo::extractParts(c, cBuf, numParts, parts, FALSE);

    NAString catalogNamePart;
    NAString schemaNamePart;
    NAString objectNamePart;
    NAString objDataUID;

    // numParts == 3 , we check the table format "cat.sch.table",
    // some system table and statistics table are in this format
    // numParts == 2 , we chack the table format "NAMESPQCE:TRAFODION__DATAUID"
    // this is the new format used by new hbase table format
    if (numParts == 3) {
      catalogNamePart = NAString(parts[0]);
      schemaNamePart = NAString(parts[1]);
      objectNamePart = NAString(parts[2]);
    } else if (numParts == 2) {
      catalogNamePart = NAString(parts[0]);
      objDataUID = NAString(parts[1]);
    } else
      continue;

    // cout << " catalogNamePart : " << catalogNamePart.data();
    // if (schemaNamePart.data())
    //   cout << ", schemaNamePart : " << schemaNamePart.data();
    // if (objectNamePart.data())
    //   cout << ", objectNamePart : " << objectNamePart.data();
    // cout << ", objDataUID : " << objDataUID.data() << endl;

    // see if catalogNamePart has namespace prefix
    NAString nameSpace;
    const char *colonPos = strchr(catalogNamePart, ':');
    if (colonPos) {
      nameSpace = NAString(catalogNamePart.data(), (colonPos - catalogNamePart));
      catalogNamePart = NAString(&catalogNamePart.data()[colonPos - catalogNamePart + 1]);
    }

    // if namespace is not a TRAF namespace, skip it.
    // if catalog name is not a TRAFODION catalog, skip it.
    if ((NOT nameSpace.isNull()) && ((nameSpace.length() < TRAF_NAMESPACE_PREFIX_LEN) ||
                                     (NAString(nameSpace(0, TRAF_NAMESPACE_PREFIX_LEN)) != TRAF_NAMESPACE_PREFIX))) {
      // not traf namespace. skip it.
      continue;
    }

    if (catalogNamePart != TRAFODION_SYSTEM_CATALOG) {
      // not traf cat. skip it.
      continue;
    }

    NAString extHbaseName;
    NAString extTableName;
    long objUID = -1;

    if (numParts == 3) {
      extTableName = catalogNamePart + "." + "\"" + schemaNamePart + "\"" + "." + "\"" + objectNamePart + "\"";

      extHbaseName = genHBaseObjName(catalogNamePart, schemaNamePart, objectNamePart, nameSpace);

      if ((schemaNamePart == SEABASE_MD_SCHEMA) || (schemaNamePart == SEABASE_REPOS_SCHEMA) ||
          (schemaNamePart == SEABASE_DTM_SCHEMA) || (schemaNamePart == SEABASE_PRIVMGR_SCHEMA))
        continue;

      // check if this object exists in metadata.
      objUID = getObjectUID(cliInterface, catalogNamePart.data(), schemaNamePart.data(), objectNamePart.data(), NULL);
      if (objUID != -1)  // found in metadata
        continue;

      // not found or error. Clear diags and continue.
      CmpCommon::diags()->clear();

      numOrphanHbaseEntries_++;

      if (addReturnDetailsEntry(cliInterface, returnDetailsList_, extHbaseName.data(), FALSE)) return -1;

      if (checkOnly_) continue;

      // this object could be a table or an index. Try to cleanup both.
      str_sprintf(query, "cleanup table %s", extTableName.data());
      cliRC = cliInterface->executeImmediate(query);
      if (cliRC < 0) {
        if (processCleanupErrors(NULL, errorSeen)) return -1;
      }

      str_sprintf(query, "cleanup index %s", extTableName.data());
      cliRC = cliInterface->executeImmediate(query);
      if (cliRC < 0) {
        if (processCleanupErrors(NULL, errorSeen)) return -1;
      }

      if (NOT nameSpace.isNull()) {
        str_sprintf(query, "cleanup object \"%s\"", extHbaseName.data());
        cliRC = cliInterface->executeImmediate(query);
        if (cliRC < 0) {
          if (processCleanupErrors(NULL, errorSeen)) return -1;
        }
      }
    } else if (numParts == 2) {
      objUID = getObjectUID(cliInterface, atoInt64(objDataUID.data()), FALSE);
      if (objUID != -1)  // found in metadata
        continue;

      // not found or error. Clear diags and continue.
      CmpCommon::diags()->clear();
      numOrphanHbaseEntries_++;

      if (addReturnDetailsEntry(cliInterface, returnDetailsList_, originStr.data(), FALSE)) return -1;

      if (checkOnly_) continue;

      str_sprintf(query, "cleanup object \"%s\"", originStr.data());
      cliRC = cliInterface->executeImmediate(query);
      if (cliRC < 0) {
        if (processCleanupErrors(NULL, errorSeen)) return -1;
      }
    }

  }  // for
  deleteNAArray(ehi->getHeap(), listArray);
  return 0;
}

short CmpSeabaseMDcleanup::cleanupInconsistentObjectsEntries(ExeCliInterface *cliInterface, ExpHbaseInterface *ehi,
                                                             CmpDDLwithStatusInfo *dws) {
  int cliRC = 0;
  char query[1000];
  NABoolean errorSeen = FALSE;

  numOrphanObjectsEntries_ = 0;

  obsoleteEntriesList_ = NULL;
  returnDetailsList_ = NULL;

  addReturnDetailsEntry(cliInterface, returnDetailsList_, NULL, TRUE);

  str_sprintf(query, "control query shape join(scan(path '%s'), scan(path '%s'))", SEABASE_OBJECTS,
              SEABASE_OBJECTS_UNIQ_IDX);

  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    if (processCleanupErrors(cliInterface, errorSeen)) goto error_return2;
  }

  // find out all entries that exist in OBJECTS but not in OBJECTS_UNIQ_IDX
  str_sprintf(query,
              "select object_uid, trim(catalog_name) || '.'  || trim(schema_name) || '.' || trim(object_name)  from "
              "%s.\"%s\".%s  where catalog_name = '%s' and schema_name not in ( '_MD_', '_REPOS_', '_PRIVMGR_MD_') and "
              "object_uid not in (select \"OBJECT_UID@\"  from table(index_table %s.\"%s\".%s))",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS, getSystemCatalog(), getSystemCatalog(),
              SEABASE_MD_SCHEMA, SEABASE_OBJECTS_UNIQ_IDX);

  cliRC = cliInterface->fetchAllRows(obsoleteEntriesList_, query, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    if (processCleanupErrors(cliInterface, errorSeen)) goto error_return1;
  }

  if (cleanupUIDs(cliInterface, obsoleteEntriesList_, dws)) goto error_return1;

  addReturnDetailsEntryFromList(cliInterface, obsoleteEntriesList_, 1 /*0 based*/, returnDetailsList_);

  numOrphanObjectsEntries_ += obsoleteEntriesList_->numEntries();

  // find out all entries that exist in OBJECTS_UNIQ_IDX but not in OBJECTS
  str_sprintf(query, "control query shape join(scan(path '%s'), scan(path '%s'))", SEABASE_OBJECTS_UNIQ_IDX,
              SEABASE_OBJECTS);

  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    if (processCleanupErrors(cliInterface, errorSeen)) goto error_return1;
  }

  str_sprintf(query,
              "select \"OBJECT_UID@\", trim(catalog_name) || '.'  || trim(schema_name) || '.' || trim(object_name)  "
              "from table(index_table %s.\"%s\".%s)  where catalog_name = '%s' and schema_name not in ( '_MD_', "
              "'_REPOS_', '_PRIVMGR_MD_') and \"OBJECT_UID@\" not in (select object_uid from %s.\"%s\".%s)",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS_UNIQ_IDX, getSystemCatalog(), getSystemCatalog(),
              SEABASE_MD_SCHEMA, SEABASE_OBJECTS);

  cliRC = cliInterface->fetchAllRows(obsoleteEntriesList_, query, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    if (processCleanupErrors(cliInterface, errorSeen)) goto error_return1;
  }

  cliRC = cliInterface->resetCQS(CmpCommon::diags());
  if (cliRC < 0) {
    if (processCleanupErrors(cliInterface, errorSeen)) goto error_return2;
  }

  if (cleanupUIDs(cliInterface, obsoleteEntriesList_, dws)) goto error_return1;

  addReturnDetailsEntryFromList(cliInterface, obsoleteEntriesList_, 1 /*0 based*/, returnDetailsList_);

  numOrphanObjectsEntries_ += obsoleteEntriesList_->numEntries();

  // cleanup entries in columns table which are not in objects table
  str_sprintf(query, "control query shape groupby(join(scan(path '%s'), scan(path '%s')))", SEABASE_COLUMNS,
              SEABASE_OBJECTS);

  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    if (processCleanupErrors(cliInterface, errorSeen)) goto error_return1;
  }

  str_sprintf(query,
              "select distinct object_uid, cast(object_uid as varchar(30) not null) from %s.\"%s\".%s where object_uid "
              "not in (select object_uid from %s.\"%s\".%s) ",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_COLUMNS, getSystemCatalog(), SEABASE_MD_SCHEMA,
              SEABASE_OBJECTS);

  cliRC = cliInterface->fetchAllRows(obsoleteEntriesList_, query, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    if (processCleanupErrors(cliInterface, errorSeen)) goto error_return1;
  }

  cliRC = cliInterface->resetCQS(CmpCommon::diags());
  if (cliRC < 0) {
    if (processCleanupErrors(cliInterface, errorSeen)) goto error_return2;
  }

  if (cleanupUIDs(cliInterface, obsoleteEntriesList_, dws)) goto error_return2;

  addReturnDetailsEntryFromList(cliInterface, obsoleteEntriesList_, 1 /*0 based*/, returnDetailsList_, TRUE /*UID*/);

  numOrphanObjectsEntries_ += obsoleteEntriesList_->numEntries();

  return 0;

error_return1:
  cliInterface->resetCQS(CmpCommon::diags());

error_return2:
  return -1;
}

short CmpSeabaseMDcleanup::cleanupOrphanViewsEntries(ExeCliInterface *cliInterface, ExpHbaseInterface *ehi,
                                                     CmpDDLwithStatusInfo *dws) {
  int cliRC = 0;
  char query[1000];
  NABoolean errorSeen = FALSE;

  numOrphanViewsEntries_ = 0;
  obsoleteEntriesList_ = NULL;

  returnDetailsList_ = NULL;
  addReturnDetailsEntry(cliInterface, returnDetailsList_, NULL, TRUE);

  // find out all entries in views_usage that dont have corresponding used object
  str_sprintf(query, "control query shape groupby(join(scan(path '%s'), scan(path '%s')))", SEABASE_VIEWS_USAGE,
              SEABASE_OBJECTS);

  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    if (processCleanupErrors(cliInterface, errorSeen)) return -1;
  }

  str_sprintf(query,
              "select distinct used_object_uid, cast(used_object_uid as varchar(30) not null) from %s.\"%s\".%s  where "
              "used_object_uid not in (select object_uid from %s.\"%s\".%s)",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_VIEWS_USAGE, getSystemCatalog(), SEABASE_MD_SCHEMA,
              SEABASE_OBJECTS);

  cliRC = cliInterface->fetchAllRows(obsoleteEntriesList_, query, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    if (processCleanupErrors(cliInterface, errorSeen)) {
      cliInterface->resetCQS(CmpCommon::diags());
      return -1;
    }
  }

  cliRC = cliInterface->resetCQS(CmpCommon::diags());
  if (cliRC < 0) {
    if (processCleanupErrors(cliInterface, errorSeen)) return -1;
  }

  if (cleanupUIDs(cliInterface, obsoleteEntriesList_, dws)) return -1;

  addReturnDetailsEntryFromList(cliInterface, obsoleteEntriesList_, 1 /*0 based*/, returnDetailsList_, TRUE /*UID*/);

  numOrphanViewsEntries_ += obsoleteEntriesList_->numEntries();

  // find out all entries in VIEWS that do not exist in OBJECTS
  str_sprintf(query, "control query shape join(scan(path '%s'), scan(path '%s'))", SEABASE_VIEWS, SEABASE_OBJECTS);

  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    if (processCleanupErrors(cliInterface, errorSeen)) return -1;
  }

  str_sprintf(query,
              "select view_uid, cast(view_uid as varchar(30) not null) from %s.\"%s\".%s  where view_uid not in "
              "(select object_uid from %s.\"%s\".%s)",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_VIEWS, getSystemCatalog(), SEABASE_MD_SCHEMA,
              SEABASE_OBJECTS);

  cliRC = cliInterface->fetchAllRows(obsoleteEntriesList_, query, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    if (processCleanupErrors(cliInterface, errorSeen)) {
      cliInterface->resetCQS(CmpCommon::diags());
      return -1;
    }
  }

  cliRC = cliInterface->resetCQS(CmpCommon::diags());
  if (cliRC < 0) {
    if (processCleanupErrors(cliInterface, errorSeen)) return -1;
  }

  if (cleanupUIDs(cliInterface, obsoleteEntriesList_, dws)) return -1;

  addReturnDetailsEntryFromList(cliInterface, obsoleteEntriesList_, 1 /*0 based*/, returnDetailsList_, TRUE /*UID*/);

  numOrphanViewsEntries_ += obsoleteEntriesList_->numEntries();

  return 0;
}

short CmpSeabaseMDcleanup::cleanupInconsistentPartitionEntries(ExeCliInterface *cliInterface, ExpHbaseInterface *ehi,
                                                               CmpDDLwithStatusInfo *dws) {
  int cliRC = 0;
  char query[2000];
  NABoolean errorSeen = FALSE;

  numInconsistentPartitionEntries_ = 0;
  obsoleteEntriesList_ = NULL;

  returnDetailsList_ = NULL;
  addReturnDetailsEntry(cliInterface, returnDetailsList_, NULL, TRUE);

  // find out all entries which do not have parent objects.
  str_sprintf(query,
              "select cast(tp.partition_uid as varchar(30) not null) from %s.\"%s\".%s tp "
              "where not exists (select table_uid from %s.\"%s\".%s "
              "where tp.parent_uid = table_uid) ",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_PARTITIONS, getSystemCatalog(), SEABASE_MD_SCHEMA,
              SEABASE_TABLE_PARTITION);
  cliRC = cliInterface->fetchAllRows(obsoleteEntriesList_, query, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    if (processCleanupErrors(cliInterface, errorSeen)) {
      return -1;
    }
  }

  if (cleanupUIDs(cliInterface, obsoleteEntriesList_, dws)) return -1;

  addReturnDetailsEntryFromList(cliInterface, obsoleteEntriesList_, 0 /*0 based*/, returnDetailsList_, TRUE /*UID*/);

  numInconsistentPartitionEntries_ += obsoleteEntriesList_->numEntries();

  return 0;
}

// remove privileges defined for users and roles that are  no longer defined
// in the metadata
//
// Two types of inconsistencies are checked:
//    - The authID has been granted component privileges but user doesn't exist.
//      There was a bug where component_privileges were not removed when the
//      user was unregistered.
//    - The authID defined in auths does not match the authID defined in other
//      metadata tables.  This can happen after a restore where the authIDs on
//      the source system do not match the target system.
short CmpSeabaseMDcleanup::cleanupInconsistentPrivEntries(ExeCliInterface *cliInterface, ExpHbaseInterface *ehi) {
  int cliRC = 0;
  char query[4000];
  NABoolean errorSeen = FALSE;

  numInconsistentPrivEntries_ = 0;
  returnDetailsList_ = NULL;
  addReturnDetailsEntry(cliInterface, returnDetailsList_, NULL, TRUE);

  char privMgrMDLoc[sizeof(SEABASE_PRIVMGR_SCHEMA) + 100];
  sprintf(privMgrMDLoc, "%s.\"%s\"", getSystemCatalog(), SEABASE_PRIVMGR_SCHEMA);
  PrivMgrComponentPrivileges compPriv(privMgrMDLoc, CmpCommon::diags());

  // Get privileges defined in component privileges but not in auths
  str_sprintf(query,
              "select grantee_id, grantee_name from %s.\"%s\".%s "
              "where grantee_id > 0 and "
              "grantee_id not in (select auth_id from %s.\"%s\".%s)",
              getSystemCatalog(), SEABASE_PRIVMGR_SCHEMA, PRIVMGR_COMPONENT_PRIVILEGES, getSystemCatalog(),
              SEABASE_MD_SCHEMA, SEABASE_AUTHS);
  Queue *orphanPrivObjs = NULL;
  cliRC = cliInterface->fetchAllRows(orphanPrivObjs, query, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    if (processCleanupErrors(cliInterface, errorSeen)) return -1;
  }

  numInconsistentPrivEntries_ += orphanPrivObjs->numEntries();

  orphanPrivObjs->position();
  for (size_t i = 0; i < orphanPrivObjs->numEntries(); i++) {
    OutputInfo *oi = (OutputInfo *)orphanPrivObjs->getCurr();
    int authID = *(int *)oi->get(0);
    char intStr[20];
    str_itoa(authID, intStr);
    NAString msg(intStr);
    if (!checkOnly_) msg += " removed orphan";
    msg += CmpSeabaseDDLauth::isUserID(authID) ? " (user " : " (role ";
    msg += oi->get(1);
    msg += ")";

    if (addReturnDetailsEntry(cliInterface, returnDetailsList_, msg.data(), FALSE, TRUE)) return -1;

    if (NOT checkOnly_) {
      if (!compPriv.dropAllForGrantee(authID)) {
        cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
        return -1;
      }
    }

    orphanPrivObjs->advance();
  }

  numOrphanMetadataEntries_ = orphanPrivObjs->numEntries();

  // Cleanup mismatches between AUTHS table and existing authIDs
  NASet<NAString> authIDList(STMTHEAP);

  // Find any inconsistencies between the authID stored in AUTHS table and
  // authID stored in other metadata tables
  //    -2 - error occurred while gathering inconsistencies
  //    -1 - unexpected error occurred
  //     0 - cleanup successful
  cliRC = checkAndFixupAuthIDs(cliInterface, checkOnly_, authIDList);
  if (cliRC == -2) {
    // Change errors to warnings and continue cleanup
    // Errors are already copied to cliInterface so send a NULL pointer
    if (processCleanupErrors(NULL, errorSeen)) return -1;
  } else if (cliRC < 0)
    return -1;

  numInconsistentPrivEntries_ += authIDList.entries();

  for (size_t i = 0; i < authIDList.entries(); i++) {
    if (addReturnDetailsEntry(cliInterface, returnDetailsList_, authIDList[i].data(), FALSE, TRUE)) return -1;
  }

  numOrphanMetadataEntries_ += authIDList.entries();
  return 0;
}

// remove tenant group usages for groups that are no longer registered
short CmpSeabaseMDcleanup::cleanupInconsistentGroupEntries(ExeCliInterface *cliInterface, ExpHbaseInterface *ehi) {
  int cliRC = 0;
  char query[4000];
  NABoolean errorSeen = FALSE;

  numInconsistentGroupEntries_ = 0;
  returnDetailsList_ = NULL;
  addReturnDetailsEntry(cliInterface, returnDetailsList_, NULL, TRUE);

  // If multi-tenancy is not enabled, just return
  bool enabled(msg_license_multitenancy_enabled());
  if (!enabled) return 0;

  // Get user groups assigned to tenants that no longer exist
  str_sprintf(query,
              "select tenant_id, usage_uid from %s.\"%s\".%s "
              "where usage_type = 'G' and "
              "usage_uid not in (select auth_id from %s.\"%s\".%s)",
              getSystemCatalog(), SEABASE_TENANT_SCHEMA, SEABASE_TENANT_USAGE, getSystemCatalog(), SEABASE_MD_SCHEMA,
              SEABASE_AUTHS);
  Queue *orphanGroups = NULL;
  cliRC = cliInterface->fetchAllRows(orphanGroups, query, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    if (processCleanupErrors(cliInterface, errorSeen)) return -1;
  }

  numInconsistentGroupEntries_ += orphanGroups->numEntries();

  orphanGroups->position();
  for (size_t i = 0; i < orphanGroups->numEntries(); i++) {
    OutputInfo *oi = (OutputInfo *)orphanGroups->getCurr();
    int tenantID = *(int *)oi->get(0);
    long usageUID = *(long *)oi->get(1);
    char intStr[100];
    str_ltoa(usageUID, intStr);

    if (addReturnDetailsEntry(cliInterface, returnDetailsList_, intStr, FALSE, TRUE)) return -1;

    if (NOT checkOnly_) {
      str_sprintf(query,
                  "delete from %s.\"%s\".%s where "
                  "tenant_id = %d and usage_uid = %ld",
                  getSystemCatalog(), SEABASE_TENANT_SCHEMA, SEABASE_TENANT_USAGE, tenantID, usageUID);

      cliRC = cliInterface->executeImmediate(query);
      if (cliRC < 0) {
        cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
        return -1;
      }
    }

    orphanGroups->advance();
  }

  numOrphanMetadataEntries_ = orphanGroups->numEntries();

  return 0;
}

short CmpSeabaseMDcleanup::cleanupInconsistentTextEntries(ExeCliInterface *cliInterface, ExpHbaseInterface *ehi,
                                                          CmpDDLwithStatusInfo *dws) {
  int cliRC = 0;
  char query[1000];
  NABoolean errorSeen = FALSE;

  numInconsistentTextEntries_ = 0;

  obsoleteEntriesList_ = NULL;
  returnDetailsList_ = NULL;

  addReturnDetailsEntry(cliInterface, returnDetailsList_, NULL, TRUE);

  str_sprintf(query, "control query shape join(scan(path '%s'), scan(path '%s'))", SEABASE_TEXT, SEABASE_OBJECTS);

  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    if (processCleanupErrors(cliInterface, errorSeen)) goto error_return2;
  }

  // find out all entries that exist in TEXT but not in OBJECTS
  // CHECK_CONSTR_TYPE is not using object_uid to associate text tables, not check there
  str_sprintf(query,
              "select TEXT_UID, TEXT_TYPE  from %s.\"%s\".%s  where TEXT_UID not in (select OBJECT_UID  from "
              "%s.\"%s\".%s) AND TEXT_TYPE != 1",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_TEXT, getSystemCatalog(), SEABASE_MD_SCHEMA,
              SEABASE_OBJECTS);

  cliRC = cliInterface->fetchAllRows(obsoleteEntriesList_, query, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    if (processCleanupErrors(cliInterface, errorSeen)) goto error_return1;
  }

  cliRC = cliInterface->resetCQS(CmpCommon::diags());
  if (cliRC < 0) {
    if (processCleanupErrors(cliInterface, errorSeen)) return -1;
  }

  if (cleanupUIDs(cliInterface, obsoleteEntriesList_, dws)) goto error_return1;

  addReturnDetailsEntryFromList(cliInterface, obsoleteEntriesList_, 0 /*0 based*/, returnDetailsList_, FALSE, TRUE);

  numInconsistentTextEntries_ += obsoleteEntriesList_->numEntries();
  return 0;

error_return1:
  cliInterface->resetCQS(CmpCommon::diags());

error_return2:
  return -1;
}

void CmpSeabaseMDcleanup::cleanupInferiorPartitionEntries(ExeCliInterface *cliInterface) {
  int cliRC = 0;
  char query[1000];
  NABoolean errorSeen = FALSE;
  CMPASSERT(objUID_ != -1);

  str_sprintf(query, "select partition_uid from %s.\"%s\".%s where parent_uid = %ld ", getSystemCatalog(),
              SEABASE_MD_SCHEMA, SEABASE_PARTITIONS, objUID_);

  Queue *ipeList = NULL;
  cliRC = cliInterface->fetchAllRows(ipeList, query, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    if (processCleanupErrors(cliInterface, errorSeen)) return;
  }

  stopOnError_ = FALSE;
  ipeList->position();
  for (size_t i = 0; i < ipeList->numEntries(); i++) {
    OutputInfo *oi = (OutputInfo *)ipeList->getCurr();
    long uid = *(long *)oi->get(0);
    str_sprintf(query, "cleanup uid %ld", uid);
    cliRC = cliInterface->executeImmediate(query);
    if (cliRC < 0) {
      if (processCleanupErrors(NULL, errorSeen)) return;
    }
    ipeList->advance();
  }

  return;
}

void CmpSeabaseMDcleanup::cleanupMetadataEntries(ExeCliInterface *cliInterface, ExpHbaseInterface *ehi,
                                                 CmpDDLwithStatusInfo *dws) {
  int cliRC = 0;
  char query[1000];
  NABoolean errorSeen = FALSE;

  char buf[500];

  dws->setBlackBoxLen(0);
  dws->setBlackBox(NULL);

  while (1)  // exit via return stmt in switch
  {
    switch (dws->step()) {
      case START_CLEANUP: {
        if (checkOnly_)
          dws->setMsg("Metadata Cleanup: started, check only");
        else
          dws->setMsg("Metadata Cleanup: started");
        dws->setStep(ORPHAN_OBJECTS_ENTRIES);
        dws->setSubstep(0);
        dws->setEndStep(TRUE);

        return;
      } break;

      case ORPHAN_OBJECTS_ENTRIES: {
        switch (dws->subStep()) {
          case 0: {
            dws->setMsg("  Start: Cleanup Orphan Objects Entries");
            dws->subStep()++;
            dws->setEndStep(FALSE);

            return;
          } break;

          case 1: {
            if (cleanupOrphanObjectsEntries(cliInterface, ehi, dws)) return;

            str_sprintf(buf, "  End:   Cleanup Orphan Objects Entries (%d %s %s)", numOrphanMetadataEntries_,
                        (numOrphanMetadataEntries_ == 1 ? "entry" : "entries"), (checkOnly_ ? "found" : "cleaned up"));
            dws->setMsg(buf);

            int blackBoxLen = 0;
            char *blackBox = NULL;
            populateBlackBox(cliInterface, returnDetailsList_, blackBoxLen, blackBox);

            dws->setBlackBoxLen(blackBoxLen);
            dws->setBlackBox(blackBox);

            dws->setStep(HBASE_ENTRIES);
            dws->setSubstep(0);
            dws->setEndStep(TRUE);

            return;
          } break;
        }  // switch
      }    // MD_ENTRIES
      break;

      case HBASE_ENTRIES: {
        switch (dws->subStep()) {
          case 0: {
            dws->setMsg("  Start: Cleanup Orphan Hbase Entries");
            dws->subStep()++;
            dws->setEndStep(FALSE);

            return;
          } break;

          case 1: {
            if (cleanupOrphanHbaseEntries(cliInterface, ehi, dws)) return;

            str_sprintf(buf, "  End:   Cleanup Orphan Hbase Entries (%d %s %s)", numOrphanHbaseEntries_,
                        (numOrphanHbaseEntries_ == 1 ? "entry" : "entries"), (checkOnly_ ? "found" : "cleaned up"));

            int blackBoxLen = 0;
            char *blackBox = NULL;
            populateBlackBox(cliInterface, returnDetailsList_, blackBoxLen, blackBox);

            dws->setBlackBoxLen(blackBoxLen);
            dws->setBlackBox(blackBox);

            dws->setMsg(buf);
            dws->setStep(INCONSISTENT_OBJECTS_ENTRIES);
            dws->setSubstep(0);
            dws->setEndStep(TRUE);

            return;
          } break;
        }  // switch
      }    // HBASE_ENTRIES
      break;

      case INCONSISTENT_OBJECTS_ENTRIES: {
        switch (dws->subStep()) {
          case 0: {
            dws->setMsg("  Start: Cleanup Inconsistent Objects Entries");
            dws->subStep()++;
            dws->setEndStep(FALSE);

            return;
          } break;

          case 1: {
            if (cleanupInconsistentObjectsEntries(cliInterface, ehi, dws)) return;

            str_sprintf(buf, "  End:   Cleanup Inconsistent Objects Entries (%d %s %s)", numOrphanObjectsEntries_,
                        (numOrphanObjectsEntries_ == 1 ? "entry" : "entries"), (checkOnly_ ? "found" : "cleaned up"));

            int blackBoxLen = 0;
            char *blackBox = NULL;
            populateBlackBox(cliInterface, returnDetailsList_, blackBoxLen, blackBox);

            dws->setBlackBoxLen(blackBoxLen);
            dws->setBlackBox(blackBox);

            dws->setMsg(buf);
            dws->setStep(INCONSISTENT_PARTITIONS_ENTRIES);
            dws->setSubstep(0);
            dws->setEndStep(TRUE);

            return;
          } break;
        }  // switch
      }    // OBJECTS_ENTRIES
      break;

      case INCONSISTENT_PARTITIONS_ENTRIES: {
        switch (dws->subStep()) {
          case 0: {
            dws->setMsg("  Start: Cleanup Inconsistent Partitions Entries");
            dws->subStep()++;
            dws->setEndStep(FALSE);

            return;
          } break;

          case 1: {
            if (cleanupInconsistentPartitionEntries(cliInterface, ehi, dws)) return;

            str_sprintf(buf, "  End:   Cleanup Inconsistent Partitions Entries (%d %s %s)",
                        numInconsistentPartitionEntries_, (numInconsistentPartitionEntries_ == 1 ? "entry" : "entries"),
                        (checkOnly_ ? "found" : "cleaned up"));

            int blackBoxLen = 0;
            char *blackBox = NULL;
            populateBlackBox(cliInterface, returnDetailsList_, blackBoxLen, blackBox);

            dws->setBlackBoxLen(blackBoxLen);
            dws->setBlackBox(blackBox);

            dws->setMsg(buf);
            dws->setStep(VIEWS_ENTRIES);
            dws->setSubstep(0);
            dws->setEndStep(TRUE);

            return;
          } break;
        }  // switch
      }    // INCONSISTENT_PARTITIONS_ENTRIES

      case VIEWS_ENTRIES: {
        switch (dws->subStep()) {
          case 0: {
            dws->setMsg("  Start: Cleanup Inconsistent Views Entries");
            dws->subStep()++;
            dws->setEndStep(FALSE);

            return;
          } break;

          case 1: {
            if (cleanupOrphanViewsEntries(cliInterface, ehi, dws)) return;

            str_sprintf(buf, "  End:   Cleanup Inconsistent Views Entries (%d %s %s)", numOrphanViewsEntries_,
                        (numOrphanViewsEntries_ == 1 ? "entry" : "entries"), (checkOnly_ ? "found" : "cleaned up"));

            int blackBoxLen = 0;
            char *blackBox = NULL;
            populateBlackBox(cliInterface, returnDetailsList_, blackBoxLen, blackBox);

            dws->setBlackBoxLen(blackBoxLen);
            dws->setBlackBox(blackBox);

            if ((numOrphanViewsEntries_ == 0) && (blackBoxLen > 0)) {
              str_sprintf(buf,
                          "  End:   Cleanup Inconsistent Views Entries (%d %s %s) [internal error: blackBoxLen = %d] ",
                          numOrphanViewsEntries_, (numOrphanViewsEntries_ == 1 ? "entry" : "entries"),
                          (checkOnly_ ? "found" : "cleaned up"), blackBoxLen);
            }

            dws->setMsg(buf);
            dws->setStep(PRIV_ENTRIES);
            dws->setSubstep(0);
            dws->setEndStep(TRUE);

            return;
          } break;
        }  // switch
      }    // VIEWS_ENTRIES

      case PRIV_ENTRIES: {
        switch (dws->subStep()) {
          case 0: {
            dws->setMsg("  Start: Cleanup Inconsistent Privilege Entries");
            dws->subStep()++;
            dws->setEndStep(FALSE);

            return;
          } break;

          case 1: {
            if (isAuthorizationEnabled()) {
              if (cleanupInconsistentPrivEntries(cliInterface, ehi)) return;
            } else {
              numInconsistentPrivEntries_ = 0;
              returnDetailsList_ = NULL;
            }

            str_sprintf(buf, "  End:   Cleanup Inconsistent Privilege Entries (%d %s %s)", numInconsistentPrivEntries_,
                        (numInconsistentPrivEntries_ == 1 ? "entry" : "entries"),
                        (checkOnly_ ? "found" : "cleaned up"));

            int blackBoxLen = 0;
            char *blackBox = NULL;
            populateBlackBox(cliInterface, returnDetailsList_, blackBoxLen, blackBox);

            dws->setBlackBoxLen(blackBoxLen);
            dws->setBlackBox(blackBox);

            dws->setMsg(buf);
            dws->setStep(GROUP_ENTRIES);
            dws->setSubstep(0);
            dws->setEndStep(TRUE);

            return;
          } break;
        }  // switch
      }    // PRIV_ENTRIES

      case GROUP_ENTRIES: {
        switch (dws->subStep()) {
          case 0: {
            dws->setMsg("  Start: Cleanup Inconsistent User Group Entries");
            dws->subStep()++;
            dws->setEndStep(FALSE);

            return;
          } break;

          case 1: {
            if (cleanupInconsistentGroupEntries(cliInterface, ehi) == -1) return;

            str_sprintf(buf, "  End:   Cleanup Inconsistent User Group Entries (%d %s %s)",
                        numInconsistentGroupEntries_, (numInconsistentGroupEntries_ == 1 ? "entry" : "entries"),
                        (checkOnly_ ? "found" : "cleaned up"));

            int blackBoxLen = 0;
            char *blackBox = NULL;
            populateBlackBox(cliInterface, returnDetailsList_, blackBoxLen, blackBox);

            dws->setBlackBoxLen(blackBoxLen);
            dws->setBlackBox(blackBox);

            dws->setMsg(buf);
            dws->setStep(INCONSISTENT_TEXT_ENTRIES);
            dws->setSubstep(0);
            dws->setEndStep(TRUE);

            return;
          } break;
        }  // switch
      }    // GROUP_ENTRIES

      case INCONSISTENT_TEXT_ENTRIES: {
        switch (dws->subStep()) {
          case 0: {
            dws->setMsg("  Start: Cleanup Inconsistent TEXT Entries");
            dws->subStep()++;
            dws->setEndStep(FALSE);

            return;
          } break;

          case 1: {
            if (cleanupInconsistentTextEntries(cliInterface, ehi, dws)) return;

            str_sprintf(buf, "  End:   Cleanup Inconsistent TEXT Entries (%d %s %s)", numInconsistentTextEntries_,
                        (numInconsistentTextEntries_ == 1 ? "entry" : "entries"),
                        (checkOnly_ ? "found" : "cleaned up"));

            int blackBoxLen = 0;
            char *blackBox = NULL;
            populateBlackBox(cliInterface, returnDetailsList_, blackBoxLen, blackBox);

            dws->setBlackBoxLen(blackBoxLen);
            dws->setBlackBox(blackBox);

            dws->setMsg(buf);
            dws->setStep(DONE_CLEANUP);
            dws->setSubstep(0);
            dws->setEndStep(TRUE);

            return;
          } break;
        }  // switch
      }    // OBJECTS_ENTRIES
      break;

      case DONE_CLEANUP: {
        dws->setMsg("Metadata Cleanup: done");
        dws->setEndStep(TRUE);
        dws->setSubstep(0);
        dws->setDone(TRUE);

        return;
      } break;

      default:
        return;

    }  // switch

  }  // while

  return;
}

void CmpSeabaseMDcleanup::cleanupObjects(StmtDDLCleanupObjects *stmtCleanupNode, NAString &currCatName,
                                         NAString &currSchName, CmpDDLwithStatusInfo *dws) {
  int cliRC = 0;
  ExeCliInterface cliInterface(STMTHEAP);

  if ((xnInProgress(&cliInterface)) && (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL))) {
    *CmpCommon::diags() << DgSqlCode(-20125) << DgString0("This CLEANUP");
    return;
  }

  // run each MD update in its own transaction. That way we can remove
  // as much as we can.
  cliInterface.autoCommit(TRUE);

  if (stmtCleanupNode) {
    if (validateInputValues(stmtCleanupNode, &cliInterface)) return;
  }

  if (NOT(catName_.isNull() || schName_.isNull() || objName_.isNull())) {
    NAString *objName = NULL;
    if (objType_ == COM_BASE_TABLE_OBJECT_LIT) {
      objName = &objName_;
    } else if (objType_ == COM_INDEX_OBJECT_LIT) {
      objName = &btObjName_;
    }
    if (objName != NULL && !objName->isNull()) {
      CorrName cn(*objName, STMTHEAP, schName_, catName_);
      if (lockObjectDDL(cn.getQualifiedNameObj(), COM_BASE_TABLE_OBJECT, cn.isVolatile(), cn.isExternal()) < 0) {
        return;
      }
    }
  }

  ExpHbaseInterface *ehi = allocEHI(COM_STORAGE_HBASE);
  if (ehi == NULL) {
    return;
  }

  checkOnly_ = FALSE;
  if (dws && dws->getMDcleanup()) {
    if (dws->getCheckOnly()) checkOnly_ = TRUE;
    if (dws->getReturnDetails()) returnDetails_ = TRUE;
    cleanupMetadataEntries(&cliInterface, ehi, dws);
    return;
  }

  if (cleanupMetadataEntries_) {
    return cleanupMetadataEntries(&cliInterface, ehi, NULL);
  }

  if ((objType_ == COM_PRIVATE_SCHEMA_OBJECT_LIT) || (objType_ == COM_SHARED_SCHEMA_OBJECT_LIT)) {
    return cleanupSchemaObjects(&cliInterface);
  }

  if (stmtCleanupNode && (stmtCleanupNode->getType() == StmtDDLCleanupObjects::HBASE_TABLE_)) {
    return cleanupHBaseObject(stmtCleanupNode, &cliInterface);
  }

  if (gatherDependentObjects(&cliInterface, ehi)) {
    if (stopOnError_) goto label_return;
    objUID_ == -1;
  }

  if (hasInferiorPartitons_) {
    cleanupInferiorPartitionEntries(&cliInterface);
  }

  if ((((objType_ == COM_BASE_TABLE_OBJECT_LIT) && (!hasInferiorPartitons_)) || (objType_ == COM_INDEX_OBJECT_LIT)) &&
      (NOT isHive_) && (extNameForHbase_.isNull())) {
    // add warning that name couldnt be found. Hbase object cannot be removed.
    CmpCommon::diags()->clear();
    *CmpCommon::diags() << DgSqlCode(4250);
  } else if (((objType_ != COM_PRIVATE_SCHEMA_OBJECT_LIT) && (objType_ != COM_SHARED_SCHEMA_OBJECT_LIT) &&
              (objType_ != COM_VIEW_OBJECT_LIT)) &&
             (objUID_ == -1)) {
    CmpCommon::diags()->clear();
    *CmpCommon::diags() << DgSqlCode(4251);
  }

  cliRC = deleteMDentries(&cliInterface);
  if (cliRC < 0)
    if (stopOnError_) goto label_error;

  cliRC = deletePartitionEntries(&cliInterface);
  if (cliRC < 0)
    if (stopOnError_) goto label_error;

  cliRC = deleteMDConstrEntries(&cliInterface);
  if (cliRC < 0)
    if (stopOnError_) goto label_error;

  cliRC = deleteMDViewEntries(&cliInterface);
  if (cliRC < 0)
    if (stopOnError_) goto label_error;

  cliRC = deleteHistogramEntries(&cliInterface);
  if (cliRC < 0)
    if (stopOnError_) goto label_error;

  if ((NOT isHive_) && (NOT extNameForHbase_.isNull()) && (NOT noHBaseDrop_)) {
    HbaseStr hbaseObject;
    hbaseObject.val = (char *)extNameForHbase_.data();
    hbaseObject.len = extNameForHbase_.length();

    // drop this object from hbase
    cliRC = dropHbaseTable(ehi, &hbaseObject, FALSE, FALSE);
    if (cliRC)
      if (stopOnError_) goto label_return;
  }

  cliRC = dropIndexes(&cliInterface);
  if (cliRC)
    if (stopOnError_) goto label_return;

  cliRC = dropSequences(&cliInterface);
  if (cliRC)
    if (stopOnError_) goto label_return;

  cliRC = dropUsingViews(&cliInterface);
  if (cliRC)
    if (stopOnError_) goto label_return;

  cliRC = deletePrivs(&cliInterface);
  if (cliRC)
    if (stopOnError_) goto label_return;

  if ((!ComIsTrafodionReservedSchemaName(schName_)) && (NOT(catName_.isNull() || schName_.isNull())) &&
      (objType_ == COM_BASE_TABLE_OBJECT_LIT || objType_ == COM_INDEX_OBJECT_LIT) &&
      ((stmtCleanupNode->getType() == StmtDDLCleanupObjects::TABLE_ && NOT objName_.isNull()) ||
       (stmtCleanupNode->getType() == StmtDDLCleanupObjects::INDEX_) && NOT btObjName_.isNull()) &&
      (NOT isHive_) && (NOT isLOBDependentNameMatch(objName_))) {
    if ((objUID_ != -1 && isMDflagsSet(objectFlags_, MD_OBJECTS_INCR_BACKUP_ENABLED)) || objUID_ == -1) {
      cliRC = updateSeabaseXDCDDLTable(&cliInterface, catName_.data(), schName_.data(),
                                       (objType_ == COM_INDEX_OBJECT_LIT) ? btObjName_.data() : objName_.data(),
                                       COM_BASE_TABLE_OBJECT_LIT);
      if (cliRC < 0)
        if (stopOnError_) goto label_return;
    }
  }
  deallocEHI(ehi);

  if (NOT(catName_.isNull() || schName_.isNull() || objName_.isNull())) {
    ComObjectType ot;
    if (objType_ == COM_VIEW_OBJECT_LIT)
      ot = COM_VIEW_OBJECT;
    else if (objType_ == COM_INDEX_OBJECT_LIT)
      ot = COM_INDEX_OBJECT;
    else
      ot = COM_BASE_TABLE_OBJECT;

    CorrName cn(objName_, STMTHEAP, schName_, catName_);

    // Remove from shared cache, if it exists
    if (ot == COM_BASE_TABLE_OBJECT || ot == COM_INDEX_OBJECT) {
      insertIntoSharedCacheList(catName_, schName_, objName_, ot, SharedCacheDDLInfo::DDL_DELETE);
      insertIntoSharedCacheList(catName_, schName_, objName_, ot, SharedCacheDDLInfo::DDL_DELETE,
                                SharedCacheDDLInfo::SHARED_DATA_CACHE);
    }

    ActiveSchemaDB()->getNATableDB()->removeNATable(cn, ComQiScope::REMOVE_FROM_ALL_USERS, ot, FALSE, FALSE, objUID_);
  }

  // If objType_ is index, remove base object from cache
  if (objType_ == COM_INDEX_OBJECT_LIT && (btObjName_.length() > 0)) {
    // Update shared cache
    insertIntoSharedCacheList(catName_, schName_, btObjName_, COM_BASE_TABLE_OBJECT, SharedCacheDDLInfo::DDL_DELETE);

    /*insertIntoSharedCacheList(catName_, schName_, btObjName_,
                              COM_INDEX_OBJECT,
                              SharedCacheDDLInfo::DDL_DELETE,
                              SharedCacheDDLInfo::SHARED_DATA_CACHE);*/

    CorrName cnbt(btObjName_, STMTHEAP, schName_, catName_);
    ActiveSchemaDB()->getNATableDB()->removeNATable(cnbt, ComQiScope::REMOVE_FROM_ALL_USERS, COM_BASE_TABLE_OBJECT,
                                                    FALSE, FALSE);
  }

  finalizeSharedCache();

  // Reset epoch values
  if (objType_ == COM_BASE_TABLE_OBJECT_LIT) {
    // TDB - skip checks for histogram tables
    CorrName cn(objName_, STMTHEAP, schName_, catName_);
    NAString tableName = cn.getQualifiedNameAsString();

    if (!CmpSeabaseDDL::isHistogramTable(tableName)) {
      ObjectEpochCacheEntryName oecName(STMTHEAP, tableName, COM_BASE_TABLE_OBJECT, 0 /*redefTime*/);
      CliGlobals *cliGlobals = GetCliGlobals();
      StatsGlobals *statsGlobals = cliGlobals->getStatsGlobals();
      ObjectEpochCache *objectEpochCache = statsGlobals->getObjectEpochCache();
      ObjectEpochCacheEntry *oecEntry = NULL;

      ObjectEpochCacheKey key(tableName, oecName.getHash(), COM_BASE_TABLE_OBJECT);

      short retcode = objectEpochCache->getEpochEntry(oecEntry /* out */, key);
      if (retcode == ObjectEpochCache::OEC_ERROR_OK) {
        // TDB - any error handling required?
        modifyObjectEpochCacheAbortDDL(&oecName, oecEntry->epoch(), true);

        char msgBuf[tableName.length() + 200];
        snprintf(msgBuf, sizeof(msgBuf),
                 "DDL cleanup, Name: %s, Epoch: %d, "
                 " Flags: %d",
                 tableName.data(), oecEntry->epoch(), oecEntry->flags());
        QRLogger::log(CAT_SQL_EXE, LL_DEBUG, "%s", msgBuf);
        char *oec = getenv("TEST_OBJECT_EPOCH");
        if (oec) cout << msgBuf << endl;
      }
    }
  }

  return;

label_error:
  if ((objType_ == COM_BASE_TABLE_OBJECT_LIT) && (NOT extNameForHbase_.isNull())) {
    CorrName cn(objName_, STMTHEAP, schName_, catName_);
    ActiveSchemaDB()->getNATableDB()->removeNATable(cn, ComQiScope::REMOVE_FROM_ALL_USERS, COM_BASE_TABLE_OBJECT, FALSE,
                                                    FALSE);
  }
  finalizeSharedCache();

label_return:
  deallocEHI(ehi);

  return;
}
