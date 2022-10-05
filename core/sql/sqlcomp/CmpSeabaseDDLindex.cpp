

/* -*-C++-*-
 *****************************************************************************
 *
 * File:         CmpSeabaseDDLindex.cpp
 * Description:  Implements ddl operations for Seabase indexes.
 *
 *
 * Created:     6/30/2013
 * Language:     C++
 *
 *
 *****************************************************************************
 */

#define SQLPARSERGLOBALS_FLAGS  // must precede all #include's
#define SQLPARSERGLOBALS_NADEFAULTS

#include "common/ComObjectName.h"

#include "parser/StmtDDLCreateIndex.h"
#include "parser/StmtDDLPopulateIndex.h"
#include "parser/StmtDDLDropIndex.h"
#include "parser/StmtDDLAlterTableEnableIndex.h"
#include "parser/StmtDDLAlterTableDisableIndex.h"
#include "parser/StmtDDLAlterIndexHBaseOptions.h"

#include "sqlcomp/CmpDDLCatErrorCodes.h"
#include "parser/ElemDDLHbaseOptions.h"

#include "optimizer/SchemaDB.h"
#include "sqlcomp/CmpSeabaseDDL.h"
#include "sqlcomp/CmpDescribe.h"

#include "exp/ExpHbaseInterface.h"

#include "executor/ExExeUtilCli.h"
#include "generator/Generator.h"
#include "cli/Context.h"

#include "common/ComCextdecs.h"
#include "common/ComUser.h"

#include "common/NumericType.h"

#include "sqlcomp/PrivMgrCommands.h"
#include "common/ngram.h"

short CmpSeabaseDDL::updateColAndKeyInfo(const NAColumn *keyCol, CollIndex i, NABoolean alignedFormat,
                                         NAString &defaultColFam, short nonKeyColType, short ordering,
                                         const char *colNameSuffix, ComTdbVirtTableColumnInfo *colInfoEntry,
                                         ComTdbVirtTableKeyInfo *keyInfoEntry, int &keyLength, NAMemory *heap) {
  int retcode = 0;

  // update column info for the index
  char *col_name = new (heap) char[strlen(keyCol->getColName().data()) + 2 + 1];
  strcpy(col_name, keyCol->getColName().data());
  if (colNameSuffix) strcat(col_name, colNameSuffix);
  colInfoEntry->colName = col_name;

  colInfoEntry->colNumber = i;
  colInfoEntry->columnClass = COM_USER_COLUMN;

  const NAType *naType = keyCol->getType();

  int precision = 0;
  int scale = 0;
  int dtStart = 0;
  int dtEnd = 0;
  int upshifted = 0;
  NAString charsetStr;
  SQLCHARSET_CODE charset = SQLCHARSETCODE_UNKNOWN;
  CharInfo::Collation collationSequence = CharInfo::DefaultCollation;
  int colFlags = 0;

  retcode = CmpSeabaseDDL::getTypeInfo(naType, alignedFormat, FALSE, colInfoEntry->datatype, colInfoEntry->length,
                                       precision, scale, dtStart, dtEnd, upshifted, colInfoEntry->nullable, charsetStr,
                                       collationSequence, colFlags);
  if (retcode) {
    if (collationSequence != CharInfo::DefaultCollation) {
      // collation not supported
      *CmpCommon::diags() << DgSqlCode(-4069) << DgColumnName(ToAnsiIdentifier(col_name))
                          << DgString0(CharInfo::getCollationName(collationSequence));
    }

    return -1;
  }

  colInfoEntry->precision = precision;
  colInfoEntry->scale = scale;
  colInfoEntry->dtStart = dtStart;
  colInfoEntry->dtEnd = dtEnd;
  colInfoEntry->upshifted = upshifted;
  colInfoEntry->charset = (SQLCHARSET_CODE)CharInfo::getCharSetEnum(charsetStr.data());
  ;
  colInfoEntry->defaultClass = COM_NO_DEFAULT;
  colInfoEntry->defVal = NULL;
  colInfoEntry->colHeading = NULL;
  if (keyCol->getHeading()) {
    char *h = (char *)keyCol->getHeading();
    int hlen = strlen(h);
    char *head_val = new (heap) char[hlen + 1];
    strcpy(head_val, h);
    colInfoEntry->colHeading = head_val;
  }

  colInfoEntry->hbaseColFam = new (heap) char[strlen(defaultColFam.data()) + 1];
  strcpy((char *)colInfoEntry->hbaseColFam, defaultColFam.data());

  char idxNumStr[40];
  idxNumStr[0] = '@';
  str_itoa(i + 1, &idxNumStr[1]);

  colInfoEntry->hbaseColQual = new (heap) char[strlen(idxNumStr) + 1];
  strcpy((char *)colInfoEntry->hbaseColQual, idxNumStr);

  colInfoEntry->hbaseColFlags = keyCol->getHbaseColFlags();
  strcpy(colInfoEntry->paramDirection, COM_UNKNOWN_PARAM_DIRECTION_LIT);
  colInfoEntry->isOptional = FALSE;

  if (naType->getTypeQualifier() == NA_CHARACTER_TYPE) {
    CharType *char_t = (CharType *)naType;
    if (char_t->isVarchar2()) colInfoEntry->colFlags |= SEABASE_COLUMN_IS_TYPE_VARCHAR2;
  }

  // add base table keys for non-unique index
  keyInfoEntry->colName = col_name;
  keyInfoEntry->keySeqNum = i + 1;
  keyInfoEntry->tableColNum = keyCol->getPosition();
  keyInfoEntry->ordering = ordering;
  //    (keyCol->getClusteringKeyOrdering() == ASCENDING ? 0 : 1);

  keyInfoEntry->nonKeyCol = nonKeyColType;
  if (nonKeyColType == 0) keyLength += naType->getEncodedKeyLength();

  keyInfoEntry->hbaseColFam = new (heap) char[strlen(defaultColFam.data()) + 1];
  strcpy((char *)keyInfoEntry->hbaseColFam, defaultColFam);

  char qualNumStr[40];
  str_sprintf(qualNumStr, "@%d", keyInfoEntry->keySeqNum);

  keyInfoEntry->hbaseColQual = new (CTXTHEAP) char[strlen(qualNumStr) + 1];
  strcpy((char *)keyInfoEntry->hbaseColQual, qualNumStr);

  return 0;
}

short CmpSeabaseDDL::createIndexColAndKeyInfoArrays(
    ElemDDLColRefArray &indexColRefArray, ElemDDLColRefArray &addnlColRefArray, NABoolean isUnique,
    NABoolean isNgram,  // ngram index
    NABoolean hasSyskey, NABoolean alignedFormat, NAString &defaultColFam, const NAColumnArray &baseTableNAColArray,
    const NAColumnArray &baseTableKeyArr, int &keyColCount, int &nonKeyColCount, int &totalColCount,
    ComTdbVirtTableColumnInfo *&colInfoArray, ComTdbVirtTableKeyInfo *&keyInfoArray, NAList<NAString> &selColList,
    int &keyLength, NAMemory *heap, int numSaltPartns) {
  int retcode = 0;
  keyLength = 0;

  keyColCount = indexColRefArray.entries();
  nonKeyColCount = 0;

  int baseTableKeyCount = baseTableKeyArr.entries();

  if (isUnique)
    nonKeyColCount = baseTableKeyCount;
  else
    keyColCount += baseTableKeyCount;

  int totalKeyColCount = keyColCount + nonKeyColCount;
  int addnlColCount = addnlColRefArray.entries();
  totalColCount = totalKeyColCount + addnlColCount;
  nonKeyColCount += addnlColCount;

  colInfoArray = new (heap) ComTdbVirtTableColumnInfo[totalColCount];
  keyInfoArray = new (heap) ComTdbVirtTableKeyInfo[totalColCount];

  CollIndex i = 0;
  NABoolean syskeyOnly = TRUE;
  NABoolean syskeySpecified = FALSE;
  NABoolean incorrectSyskeyPos = FALSE;
  for (i = 0; i < indexColRefArray.entries(); i++) {
    ElemDDLColRef *nodeKeyCol = indexColRefArray[i];

    const NAColumn *tableCol = baseTableNAColArray.getColumn(nodeKeyCol->getColumnName());
    if (tableCol == NULL) {
      *CmpCommon::diags() << DgSqlCode(-1009) << DgColumnName(ToAnsiIdentifier(nodeKeyCol->getColumnName()));

      return -1;
    }
    // ngram index only support CHAR and VARCHAR
    if (isNgram && !tableCol->isClusteringKey() &&
        !((strcmp(tableCol->getType()->getTypeName().data(), "CHAR") == 0 ||
           strcmp(tableCol->getType()->getTypeName().data(), "VARCHAR") == 0) &&
          (tableCol->getType()->getCharSet() == CharInfo::ISO88591 ||
           tableCol->getType()->getCharSet() == CharInfo::UTF8))) {
      *CmpCommon::diags() << DgSqlCode(-CAT_NGRAM_INDEX_COL_ON_NONE_CHAR_VARCHAR_COLUMN);
      return -1;
    }

    if (strcmp(nodeKeyCol->getColumnName(), "SYSKEY") != 0)
      syskeyOnly = FALSE;
    else {
      syskeySpecified = TRUE;
      if (i < (indexColRefArray.entries() - 1)) incorrectSyskeyPos = TRUE;
    }

    const NAType *naType = tableCol->getType();
    if (naType->getFSDatatype() == REC_BLOB || naType->getFSDatatype() == REC_CLOB ||
        naType->getFSDatatype() == REC_ROW || naType->getFSDatatype() == REC_ARRAY)
    // Cannot allow LOB, ROW or ARRAY in primary or clustering key
    {
      NAString type;
      if (naType->getFSDatatype() == REC_BLOB || naType->getFSDatatype() == REC_CLOB)
        type = "BLOB/CLOB";
      else if (naType->getFSDatatype() == REC_ROW)
        type = "ROW";
      else
        type = "ARRAY";
      *CmpCommon::diags() << DgSqlCode(-CAT_LOB_COL_CANNOT_BE_INDEX_OR_KEY) << DgColumnName(nodeKeyCol->getColumnName())
                          << DgString0(type);
      processReturn();
      return -1;
    }

    NAString newColName(nodeKeyCol->getColumnName());
    NAString colNameSuffix("@");
    newColName += colNameSuffix;
    if (baseTableKeyArr.getColumn(newColName)) colNameSuffix += "@";
    retcode = updateColAndKeyInfo(tableCol, i, alignedFormat, defaultColFam, 0,
                                  (nodeKeyCol->getColumnOrdering() == COM_ASCENDING_ORDER ? 0 : 1),
                                  colNameSuffix.data(), &colInfoArray[i], &keyInfoArray[i], keyLength, heap);
    selColList.insert(tableCol->getColName());
  }  // for

  if ((syskeyOnly) && (hasSyskey)) {
    *CmpCommon::diags() << DgSqlCode(-1112);

    return -1;
  }

  if ((syskeySpecified && incorrectSyskeyPos) && (hasSyskey)) {
    *CmpCommon::diags() << DgSqlCode(-1089);

    return -1;
  }

  // add base table primary key info
  CollIndex j = 0;
  NABoolean duplicateColFound = FALSE;
  while (i < totalKeyColCount) {
    const NAColumn *keyCol = baseTableKeyArr[j];

    // If an index is being created on a subset of the base table's key
    // columns, then those columns have already been added in the loop above
    // We will skip them here, so that the index does not have the same
    // column twice.
    duplicateColFound = FALSE;
    for (int k = 0; (k < indexColRefArray.entries() && !duplicateColFound); k++) {
      if (keyInfoArray[k].tableColNum == keyCol->getPosition()) {
        duplicateColFound = TRUE;
        totalKeyColCount--;
        totalColCount--;
        if (isUnique)
          nonKeyColCount--;
        else
          keyColCount--;
        j++;
      }
    }
    if (duplicateColFound) continue;  // do not add this col here since it has already been added

    retcode = updateColAndKeyInfo(keyCol, i, alignedFormat, defaultColFam, (isUnique ? 1 : 0),
                                  (keyCol->getClusteringKeyOrdering() == ASCENDING ? 0 : 1), NULL, &colInfoArray[i],
                                  &keyInfoArray[i], keyLength, heap);

    selColList.insert(keyCol->getColName());

    j++;
    i++;
  }

  // add additional col info
  j = 0;
  duplicateColFound = FALSE;
  while (j < addnlColRefArray.entries()) {
    ElemDDLColRef *nodeKeyCol = addnlColRefArray[j];
    const NAColumn *keyCol = baseTableNAColArray.getColumn(nodeKeyCol->getColumnName());

    if (!keyCol) {
      *CmpCommon::diags() << DgSqlCode(-1009) << DgColumnName(ToAnsiIdentifier(nodeKeyCol->getColumnName()));

      return -1;
    }

    if (keyCol->getType()->isComposite()) {
      NAString type;
      if (keyCol->getType()->getFSDatatype() == REC_ROW)
        type = "ROW";
      else
        type = "ARRAY";

      *CmpCommon::diags() << DgSqlCode(-CAT_LOB_COL_CANNOT_BE_INDEX_OR_KEY)
                          << DgColumnName(ToAnsiIdentifier(nodeKeyCol->getColumnName())) << DgString0(type);
      return -1;
    }

    // skip duplicate columns
    duplicateColFound = FALSE;
    for (int k = 0; (k < selColList.entries() && !duplicateColFound); k++) {
      if (keyCol->getColName() == selColList[k]) {
        duplicateColFound = TRUE;
        totalColCount--;
        nonKeyColCount--;
      }
    }

    if (duplicateColFound) {
      j++;
      continue;  // do not add this col here since it has already been added
    }

    retcode = updateColAndKeyInfo(keyCol, i, alignedFormat, defaultColFam, 2 /*addnl col*/, 0, NULL, &colInfoArray[i],
                                  &keyInfoArray[i], keyLength, heap);
    selColList.insert(keyCol->getColName());

    j++;
    i++;
  }

  if (numSaltPartns > 0 && keyLength > MAX_HBASE_ROWKEY_LEN_SALT_PARTIONS) {
    *CmpCommon::diags() << DgSqlCode(-CAT_ROWKEY_LEN_TOO_LARGE_USING_SALT_PARTIONS) << DgInt0(keyLength)
                        << DgInt1(MAX_HBASE_ROWKEY_LEN_SALT_PARTIONS);
    return -1;
  } else if (keyLength > MAX_HBASE_ROWKEY_LEN) {
    *CmpCommon::diags() << DgSqlCode(-CAT_ROWKEY_LEN_TOO_LARGE) << DgInt0(keyLength) << DgInt1(MAX_HBASE_ROWKEY_LEN);
    return -1;
  }

  return 0;
}

int CmpSeabaseDDL::createSeabasePartitionIndex(ExeCliInterface *cliInterface, StmtDDLCreateIndex *createIndexNode,
                                                 ElemDDLColRefArray &indexColRefArray, const char *partitionTableName,
                                                 const NAString &currCatName, const NAString &currSchName,
                                                 const NAString &baseIndexName) {
  int ret;

  NAString partIdxName;
  NAString col;
  for (int i = 0; i < indexColRefArray.entries(); i++) {
    if (i != 0) col += ", ";
    col += indexColRefArray[i]->getColumnName();
    if (indexColRefArray[i]->getColumnOrdering() == COM_DESCENDING_ORDER) col += " DESC";
  }

  NAString indexType;
  if (createIndexNode->isUniqueSpecified())
    indexType = "UNIQUE";
  else if (createIndexNode->isNgramSpecified())
    indexType = "NGRAM";

  char createindex[1000];

  {
    char partitionIndexName[256];
    getPartitionIndexName(partitionIndexName, 256, baseIndexName, partitionTableName);
    partIdxName = partitionIndexName;
    snprintf(createindex, sizeof(createindex), "create partition local %s index %s on  %s.%s.%s(%s)", indexType.data(),
             partitionIndexName, currCatName.data(), currSchName.data(), partitionTableName, col.data());
  }

  NAString query(createindex);
  if (createIndexNode->isNoPopulateOptionSpecified()) query += " no populate";

  ret = cliInterface->executeImmediate((char *)query.data());
  return ret;
}

void CmpSeabaseDDL::createSeabaseIndex(StmtDDLCreateIndex *createIndexNode, NAString &currCatName,
                                       NAString &currSchName) {
  int retcode = 0;
  int cliRC = 0;
  long transid;

  NABoolean isPartitionV2Table = FALSE;
  NABoolean isPartitionEntityTable = FALSE;
  NABoolean errorToken = FALSE;

  ComObjectName tableName(createIndexNode->getTableName());
  ComAnsiNamePart currCatAnsiName(currCatName);
  ComAnsiNamePart currSchAnsiName(currSchName);
  tableName.applyDefaults(currCatAnsiName, currSchAnsiName);
  NAString btCatalogNamePart = tableName.getCatalogNamePartAsAnsiString();
  NAString btSchemaNamePart = tableName.getSchemaNamePartAsAnsiString(TRUE);
  NAString btObjectNamePart = tableName.getObjectNamePartAsAnsiString(TRUE);
  NAString extTableName = tableName.getExternalName(TRUE);
  NAString tabName = (NAString &)createIndexNode->getTableName();

  NABoolean schNameSpecified = (NOT createIndexNode->getOrigTableNameAsQualifiedName().getSchemaName().isNull());

  ComObjectName indexName(createIndexNode->getIndexName());
  indexName.applyDefaults(btCatalogNamePart, btSchemaNamePart);

  NAString catalogNamePart = indexName.getCatalogNamePartAsAnsiString();
  NAString schemaNamePart = indexName.getSchemaNamePartAsAnsiString(TRUE);
  NAString objectNamePart = indexName.getObjectNamePartAsAnsiString(TRUE);
  NAString extIndexName = indexName.getExternalName(TRUE);
  NABoolean alignedFormatNotAllowed = FALSE;

  if (((isSeabaseReservedSchema(indexName)) || (ComIsTrafodionExternalSchemaName(schemaNamePart))) &&
      (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL))) {
    *CmpCommon::diags() << DgSqlCode(-1118) << DgTableName(extIndexName);
    return;
  }

  // cannot create ngram index if the feature is disabled by "CQD NGRAM_DISABLE_IN_NATABLE 'on';"
  if (createIndexNode->isNgramSpecified() && CmpCommon::getDefault(NGRAM_DISABLE_IN_NATABLE) == DF_ON) {
    *CmpCommon::diags() << DgSqlCode(-CAT_NGRAM_INDEX_DISABLED);
    return;
  }

  ExeCliInterface cliInterface(STMTHEAP, NULL, NULL, CmpCommon::context()->sqlSession()->getParentQid());
  NABoolean isVolatileTable = FALSE;
  ComObjectName volTabName;

  if ((NOT createIndexNode->isVolatile()) && (CmpCommon::context()->sqlSession()->volatileSchemaInUse())) {
    QualifiedName *qn = CmpCommon::context()->sqlSession()->updateVolatileQualifiedName(
        createIndexNode->getOrigTableNameAsQualifiedName().getObjectName());

    if (qn == NULL) {
      *CmpCommon::diags() << DgSqlCode(-CAT_UNABLE_TO_DROP_OBJECT)
                          << DgTableName(
                                 createIndexNode->getOrigTableNameAsQualifiedName().getQualifiedNameAsAnsiString(TRUE));

      processReturn();

      return;
    }

    volTabName = qn->getQualifiedNameAsAnsiString();
    volTabName.applyDefaults(currCatAnsiName, currSchAnsiName);

    NAString vtCatNamePart = volTabName.getCatalogNamePartAsAnsiString();
    NAString vtSchNamePart = volTabName.getSchemaNamePartAsAnsiString(TRUE);
    NAString vtObjNamePart = volTabName.getObjectNamePartAsAnsiString(TRUE);

    retcode = existsInSeabaseMDTable(&cliInterface, vtCatNamePart, vtSchNamePart, vtObjNamePart, COM_BASE_TABLE_OBJECT);

    if (retcode < 0) {
      processReturn();

      return;
    }

    if (retcode == 1) {
      // table found in volatile schema
      // Validate volatile table name.
      if (CmpCommon::context()->sqlSession()->validateVolatileQualifiedName(
              createIndexNode->getOrigTableNameAsQualifiedName())) {
        // Valid volatile table. Create index on it.
        extTableName = volTabName.getExternalName(TRUE);
        isVolatileTable = TRUE;

        btCatalogNamePart = vtCatNamePart;
        btSchemaNamePart = vtSchNamePart;
      } else {
        // volatile table found but the name is not a valid
        // volatile name. Look for the input name in the regular
        // schema.
        // But first clear the diags area.
        CmpCommon::diags()->clear();
      }
    } else {
      CmpCommon::diags()->clear();
    }
  }

  if (createIndexNode->isPartition() &&
      createIndexNode->getPartitionIndexType() == StmtDDLNode::PARTITION_INDEX_NORMAL) {
    NAString newBtname;
    if (isExistsPartitionName(&cliInterface, btCatalogNamePart, btSchemaNamePart, btObjectNamePart,
                              createIndexNode->getPartitionName(), newBtname)) {
      ComObjectName partEntityName(btCatalogNamePart, btSchemaNamePart, newBtname);
      btObjectNamePart = newBtname;
      extTableName = partEntityName.getExternalName(TRUE);
      tabName = extTableName;
      tableName = partEntityName;
    } else
      errorToken = TRUE;
  }

  retcode =
      lockObjectDDL(tableName, COM_BASE_TABLE_OBJECT, createIndexNode->isVolatile(), createIndexNode->isExternal());
  if (retcode < 0) {
    processReturn();
    return;
  }

  retcode = lookForTableInMD(&cliInterface, btCatalogNamePart, btSchemaNamePart, btObjectNamePart, schNameSpecified,
                             FALSE, tableName, tabName, extTableName);
  if (retcode < 0) {
    processReturn();

    return;
  }

  ActiveSchemaDB()->getNATableDB()->useCache();

  BindWA bindWA(ActiveSchemaDB(), CmpCommon::context(), TRUE /*inDDL*/);
  CorrName cn(tableName.getObjectNamePart().getInternalName(), STMTHEAP, btSchemaNamePart, btCatalogNamePart);

  NATable *naTable = bindWA.getNATableInternal(cn);
  if (naTable == NULL || bindWA.errStatus()) {
    if (!CmpCommon::diags()->contains(-4264)) CmpCommon::diags()->clear();

    if (!CmpCommon::diags()->contains(-4082)) {
      if (createIndexNode->isVolatile())
        *CmpCommon::diags() << DgSqlCode(-4082) << DgTableName(tableName.getObjectNamePart().getInternalName());
      else {
        *CmpCommon::diags() << DgSqlCode(-4082) << DgTableName(cn.getExposedNameAsAnsiString());
      }
    }

    processReturn();

    return;
  }

  isPartitionV2Table = naTable->isPartitionV2Table();
  isPartitionEntityTable = naTable->isPartitionEntityTable();

  // global index lock every partition
  if (naTable->isPartitionV2Table() &&
      createIndexNode->getPartitionIndexType() == StmtDDLNode::PARTITION_INDEX_GLOBAL) {
    retcode = lockObjectDDL(naTable, catalogNamePart, schemaNamePart, TRUE);
    if (retcode < 0) {
      processReturn();
      return;
    }
  }

  if ((isPartitionV2Table || isPartitionEntityTable) && NOT createIndexNode->isPartition()) {
    *CmpCommon::diags() << DgSqlCode(-1229) << DgString0("CREATE INDEX ON A PARTITION TABLE WITHOUT PARTITION INFO");
    processReturn();
    return;
  }

  if (NOT(isPartitionV2Table || isPartitionEntityTable) && createIndexNode->isPartition()) {
    *CmpCommon::diags() << DgSqlCode(-1229) << DgString0("CREATE PARTITION INDEX ON NOT A PARTITION TABLE");
    processReturn();
    return;
  }

  if ((isPartitionV2Table || isPartitionEntityTable) && createIndexNode->isPartition() && errorToken) {
    *CmpCommon::diags() << DgSqlCode(-1097) << DgString0(createIndexNode->getPartitionName());
    processReturn();
    return;
  }

  if (isLockedForBR(naTable->objectUid().castToInt64(), naTable->getSchemaUid().castToInt64(), &cliInterface, cn)) {
    processReturn();
    return;
  }

  long btObjUID = naTable->objectUid().castToInt64();
  NAString btNamespace = naTable->getNamespace();
  NABoolean btRowIdEncryption = naTable->useRowIdEncryption();
  NABoolean btDataEncryption = naTable->useDataEncryption();

  if (naTable->isHbaseMapTable()) {
    // not supported
    *CmpCommon::diags() << DgSqlCode(-3242) << DgString0("Cannot create index on an HBase mapped table.");

    processReturn();

    return;
  }

  NAString &trafColFam = naTable->defaultTrafColFam();

  // Verify that current user has authority to create an index
  // The user must own the base table or have the ALTER_TABLE privilege or
  // have the CREATE_INDEX privilege
  if (!isDDLOperationAuthorized(SQLOperation::ALTER_TABLE, naTable->getOwner(), naTable->getSchemaOwner(), btObjUID,
                                COM_BASE_TABLE_OBJECT, naTable->getPrivInfo()) &&
      !isDDLOperationAuthorized(SQLOperation::CREATE_INDEX, naTable->getOwner(), naTable->getSchemaOwner(), btObjUID,
                                COM_BASE_TABLE_OBJECT, naTable->getPrivInfo())) {
    *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);

    processReturn();

    return;
  }

  if (naTable->getObjectType() != COM_BASE_TABLE_OBJECT) {
    *CmpCommon::diags() << DgSqlCode(-1127) << DgTableName(extTableName);

    processReturn();

    return;
  }

  // can only create a volatile index on a volatile table
  if ((NOT naTable->isVolatileTable()) && (createIndexNode->isVolatile())) {
    *CmpCommon::diags() << DgSqlCode(-CAT_VOLATILE_OPERATION_ON_REGULAR_OBJECT)
                        << DgTableName(createIndexNode->getTableName());

    processReturn();

    return;
  }

  if ((createIndexNode->isNoPopulateOptionSpecified()) &&
      (createIndexNode->isVolatile() || naTable->isVolatileTable())) {
    *CmpCommon::diags() << DgSqlCode(-CAT_NO_POPULATE_VOLATILE_INDEX) << DgString0(extIndexName)
                        << DgTableName(extTableName);

    processReturn();

    return;
  }
  ParDDLFileAttrsCreateIndex &fileAttribs = createIndexNode->getFileAttributes();

  if ((fileAttribs.isStorageTypeSpecified()) && (naTable->storageType() != fileAttribs.storageType())) {
    // error. Base table and index must be on the same storage.
    *CmpCommon::diags() << DgSqlCode(-3242) << DgString0("Base table and index must be on the same storage.");
    processReturn();
    return;
  }
  const ComStorageType storageType = naTable->storageType();
  NABoolean isMonarch = (storageType == COM_STORAGE_MONARCH);

  ExpHbaseInterface *ehi = allocEHI(storageType);
  if (ehi == NULL) return;

  NABoolean alignedFormat = FALSE;
  if (fileAttribs.isRowFormatSpecified() == TRUE) {
    if (fileAttribs.getRowFormat() == ElemDDLFileAttrRowFormat::eALIGNED) {
      if (alignedFormatNotAllowed) {
        *CmpCommon::diags() << DgSqlCode(-4223)
                            << DgString0("Column Family specification on columns of an aligned format index is");
        processReturn();
      }
      alignedFormat = TRUE;
    }
  } else if (CmpCommon::getDefault(TRAF_INDEX_ALIGNED_ROW_FORMAT) == DF_ON) {
    if (NOT isSeabaseReservedSchema(tableName)) alignedFormat = TRUE;
  } else if (naTable->isSQLMXAlignedTable())
    alignedFormat = TRUE;

  if (alignedFormatNotAllowed) alignedFormat = FALSE;

  if (isMonarch) alignedFormat = FALSE;

  if (alignedFormat) trafColFam = SEABASE_DEFAULT_COL_FAMILY;

  // cannot create ngran index and columon index on the same column
  // will delete this limitation later
  const NAString &colName = createIndexNode->getColRefArray()[0]->getColumnName();
  const NAFileSetList &indexList = naTable->getIndexList();
  for (int i = 0; i < indexList.entries(); i++) {
    const NAFileSet *naf = indexList[i];
    if (naf->ngramIndex() == createIndexNode->isNgramSpecified()) continue;
    const NAColumnArray &indexColumns = naf->getIndexKeyColumns();
    int colNum = indexColumns.entries();
    for (int j = 0; j < colNum; j++) {
      if (indexColumns[j]->getColName() == colName) {
        // This is a base table with clustering keys
        if (naTable->objectUid().castToInt64() == naf->getIndexUID())
          *CmpCommon::diags() << DgSqlCode(-CAT_NGRAM_INDEX_COL_CONFLICT_WITH_CLUSTERING_KEY) << DgString0(colName);
        else  // index
          *CmpCommon::diags() << DgSqlCode(-CAT_NGRAM_INDEX_CONFLICT_WITH_COMMON_INDEX);
        deallocEHI(ehi);
        processReturn();
        return;
      }
    }
  }

  if ((naTable->isVolatileTable()) && (CmpCommon::context()->sqlSession()->volatileSchemaInUse()) &&
      (NOT createIndexNode->isVolatile())) {
    // create volatile index
    QualifiedName *qn = CmpCommon::context()->sqlSession()->updateVolatileQualifiedName(objectNamePart);

    catalogNamePart = qn->getCatalogName();
    schemaNamePart = qn->getSchemaName();
    extIndexName = qn->getQualifiedNameAsAnsiString();
  }

  NAString extNameForHbase;

  retcode = existsInSeabaseMDTable(&cliInterface, catalogNamePart, schemaNamePart, objectNamePart, COM_INDEX_OBJECT,
                                   FALSE /*valid or invalid object*/);
  if (retcode < 0) {
    deallocEHI(ehi);

    processReturn();

    return;
  }

  if (retcode == 0)  // doesn't exist
  {
    retcode = existsInSeabaseMDTable(&cliInterface, catalogNamePart, schemaNamePart, objectNamePart,
                                     COM_UNKNOWN_OBJECT /*check for any object with this name*/, TRUE /*valid object*/);
    if (retcode < 0) {
      deallocEHI(ehi);

      processReturn();

      return;
    }
  }

  if (retcode == 1)  // already exists
  {
    if (NOT createIndexNode->createIfNotExists()) {
      if (createIndexNode->isVolatile())
        *CmpCommon::diags() << DgSqlCode(-1390) << DgString0(objectNamePart);
      else
        *CmpCommon::diags() << DgSqlCode(-1390) << DgString0(extIndexName);
    }

    deallocEHI(ehi);

    processReturn();

    return;
  }

  if (naTable->readOnlyEnabled() && !Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)) {
    *CmpCommon::diags() << DgSqlCode(-8644);
    processReturn();
    return;
  }

  const NAColumnArray &naColArray = naTable->getNAColumnArray();
  ElemDDLColRefArray &indexColRefArray = createIndexNode->getColRefArray();
  const NAFileSet *nafs = naTable->getClusteringIndex();
  const NAColumnArray &baseTableKeyArr = nafs->getIndexKeyColumns();
  int numSaltPartns = 0;
  int numInitialSaltRegions = 1;
  int numSplits = 0;
  Int16 numTrafReplicas = naTable->getNumTrafReplicas();
  CollIndex numPrefixColumns = 0;

  if (numTrafReplicas > 0) {
    NAString replicaColName(ElemDDLReplicateClause::getReplicaSysColName());
    ElemDDLColRef *edcrr = new (STMTHEAP) ElemDDLColRef(replicaColName, COM_ASCENDING_ORDER, STMTHEAP);

    // REPLICA column will be the first column in the index
    indexColRefArray.insertAt(numPrefixColumns, edcrr);
    numPrefixColumns++;
  }

  if (createIndexNode->getSaltOptions() && createIndexNode->getSaltOptions()->getLikeTable()) {
    if (createIndexNode->isUniqueSpecified()) {
      // TBD: allow unique indexes on a superset of the SALT BY columns
      *CmpCommon::diags() << DgSqlCode(-CAT_INVALID_SALTED_UNIQUE_IDX) << DgString0(extIndexName);
      deallocEHI(ehi);
      processReturn();
      return;
    }
    // verify base table is salted
    if (naTable->hasSaltedColumn()) {
      createIndexNode->getSaltOptions()->setNumPartns(nafs->numSaltPartns());
      NAString saltColName;
      for (CollIndex c = 0; c < baseTableKeyArr.entries(); c++)
        if (baseTableKeyArr[c]->isSaltColumn()) {
          saltColName = baseTableKeyArr[c]->getColName();
          break;
        }

      ElemDDLColRef *saltColRef =
          new (STMTHEAP) ElemDDLColRef(saltColName /*column_name*/, COM_UNKNOWN_ORDER /*default val*/, STMTHEAP);
      // SALT column will be the first column in the index, after REPLICA col
      indexColRefArray.insertAt(numPrefixColumns, saltColRef);
      numPrefixColumns++;
      numSaltPartns = nafs->numSaltPartns();
      numInitialSaltRegions = nafs->numInitialSaltRegions();
      numSplits = numInitialSaltRegions - 1;  // splits due to replicas are
      // calculated in createEncodedKeysBuffer()
    } else {
      // give a warning that table is not salted
      *CmpCommon::diags() << DgSqlCode(CAT_INVALID_SALT_LIKE_CLAUSE)
                          << DgString0(createIndexNode->isVolatile() ? btObjectNamePart : extTableName)
                          << DgString1(createIndexNode->isVolatile() ? objectNamePart : extIndexName);
    }
  }

  if (createIndexNode->getDivisionType() == ElemDDLDivisionClause::DIVISION_LIKE_TABLE) {
    if (createIndexNode->isUniqueSpecified()) {
      // TBD: Allow unique indexes on a superset of the division by columns
      *CmpCommon::diags() << DgSqlCode(-1402) << DgTableName(extIndexName) << DgString0(extTableName);
      deallocEHI(ehi);
      processReturn();
      return;
    }

    int numDivisioningColumns = 0;

    for (CollIndex c = 0; c < baseTableKeyArr.entries(); c++)
      if (baseTableKeyArr[c]->isDivisioningColumn()) {
        ElemDDLColRef *divColRef =
            new (STMTHEAP) ElemDDLColRef(baseTableKeyArr[c]->getColName(), COM_UNKNOWN_ORDER /*default val*/, STMTHEAP);
        // divisioning columns go after the salt but before any user columns
        indexColRefArray.insertAt(numPrefixColumns, divColRef);
        numPrefixColumns++;
        numDivisioningColumns++;
      }

    if (numDivisioningColumns == 0) {
      // give a warning that table is not divisioned
      *CmpCommon::diags() << DgSqlCode(4248) << DgString0(extTableName) << DgString1(extIndexName);
    }
  }

  // for Unique local index on partition table, partitioning columns must form a subset of key columns of a UNIQUE
  if (isPartitionV2Table && createIndexNode->isUniqueSpecified() &&
      (createIndexNode->getPartitionIndexType() != StmtDDLNode::PARTITION_INDEX_GLOBAL)) {
    std::vector<int> indexPosition;
    std::vector<int> colIdxArray;
    for (int i = 0; i < indexColRefArray.entries(); i++) {
      const NAColumn *tableCol = naColArray.getColumn(indexColRefArray[i]->getColumnName());
      if (tableCol == NULL) {
        *CmpCommon::diags() << DgSqlCode(-1009) << DgColumnName(ToAnsiIdentifier(indexColRefArray[i]->getColumnName()));
        return;
      }
      indexPosition.push_back(tableCol->getPosition());
    }

    for (int i = 0; i < naTable->getPartitionColCount(); i++)
      colIdxArray.push_back(naTable->getPartitionColIdxArray()[i]);

    std::sort(colIdxArray.begin(), colIdxArray.end());
    std::sort(indexPosition.begin(), indexPosition.end());

    if (!std::includes(indexPosition.begin(), indexPosition.end(), colIdxArray.begin(), colIdxArray.end())) {
      *CmpCommon::diags() << DgSqlCode(-4194);
      return;
    }
  }

  int keyColCount = 0;
  int nonKeyColCount = 0;
  int totalColCount = 0;
  int keyLength = 0;

  ComTdbVirtTableColumnInfo *colInfoArray = NULL;
  ComTdbVirtTableKeyInfo *keyInfoArray = NULL;

  NAList<NAString> selColList(STMTHEAP);

  if (createIndexColAndKeyInfoArrays(indexColRefArray, createIndexNode->getAddnlColRefArray(),
                                     createIndexNode->isUniqueSpecified(),
                                     createIndexNode->isNgramSpecified(),  // ngram index
                                     naTable->getClusteringIndex()->hasSyskey(), alignedFormat, trafColFam, naColArray,
                                     baseTableKeyArr, keyColCount, nonKeyColCount, totalColCount, colInfoArray,
                                     keyInfoArray, selColList, keyLength, STMTHEAP, numSaltPartns)) {
    deallocEHI(ehi);
    processReturn();
    return;
  }

  char **encodedKeysBuffer = NULL;
  if ((numSplits > 0) || (numTrafReplicas > 0)) {
    TrafDesc *colDescs = convertVirtTableColumnInfoArrayToDescStructs(&tableName, colInfoArray, totalColCount);
    TrafDesc *keyDescs = convertVirtTableKeyInfoArrayToDescStructs(keyInfoArray, colInfoArray, keyColCount);

    if (createEncodedKeysBuffer(encodedKeysBuffer /*out*/, numSplits /*out*/, colDescs, keyDescs, nafs->numSaltPartns(),
                                numSplits, NULL, nafs->numTrafReplicas(), keyColCount, keyLength, TRUE)) {
      deallocEHI(ehi);
      processReturn();
      return;
    }
  }

  ComTdbVirtTableTableInfo *tableInfo = new (STMTHEAP) ComTdbVirtTableTableInfo[1];
  tableInfo->tableName = NULL, tableInfo->createTime = 0;
  tableInfo->redefTime = 0;
  tableInfo->objUID = 0;
  tableInfo->objDataUID = 0;
  tableInfo->schemaUID = naTable->getSchemaUid().castToInt64();
  tableInfo->objOwnerID = naTable->getOwner();
  tableInfo->schemaOwnerID = naTable->getSchemaOwner();

  tableInfo->isAudited = (nafs->isAudited() ? 1 : 0);

  tableInfo->validDef = 0;
  tableInfo->hbaseCreateOptions = NULL;
  tableInfo->numSaltPartns = numSaltPartns;
  tableInfo->numInitialSaltRegions = numInitialSaltRegions;
  tableInfo->hbaseSplitClause = NULL;
  tableInfo->rowFormat = (alignedFormat ? COM_ALIGNED_FORMAT_TYPE : COM_HBASE_FORMAT_TYPE);
  tableInfo->tableNamespace = (NOT btNamespace.isNull() ? btNamespace.data() : NULL);
  if (btRowIdEncryption) CmpSeabaseDDL::setMDflags(tableInfo->objectFlags, MD_OBJECTS_ENCRYPT_ROWID_FLG);
  if (btDataEncryption) CmpSeabaseDDL::setMDflags(tableInfo->objectFlags, MD_OBJECTS_ENCRYPT_DATA_FLG);

  if (naTable->incrBackupEnabled()) {
    CmpSeabaseDDL::setMDflags(tableInfo->objectFlags, MD_OBJECTS_INCR_BACKUP_ENABLED);
  }

  if (naTable->readOnlyEnabled()) {
    CmpSeabaseDDL::setMDflags(tableInfo->objectFlags, MD_OBJECTS_READ_ONLY_ENABLED);
  }

  ComTdbVirtTableIndexInfo *ii = new (STMTHEAP) ComTdbVirtTableIndexInfo();
  ii->baseTableName = (char *)extTableName.data();
  ii->indexName = (char *)extIndexName.data();
  ii->keytag = 1;
  ii->isUnique = createIndexNode->isUniqueSpecified() ? 1 : 0;
  // initialize ngram index flag
  ii->isNgram = createIndexNode->isNgramSpecified() ? 1 : 0;
  ii->isExplicit = 1;
  ii->keyColCount = keyColCount;
  ii->nonKeyColCount = nonKeyColCount;
  ii->keyInfoArray = NULL;  // keyInfoArray;

  NABoolean removePartNATable = FALSE;

  if (naTable->isPartitionV2Table() &&
      ((createIndexNode->getPartitionIndexType() != StmtDDLNode::PARTITION_INDEX_LOCAL) &&
       (createIndexNode->getPartitionIndexType() != StmtDDLNode::PARTITION_INDEX_GLOBAL))) {
    // not supported
    *CmpCommon::diags() << DgSqlCode(-3242) << DgString0("Cannot create normal index on a partition table.");
    processReturn();

    return;
  }

  // partitionV2 table will be flaged too
  if ((naTable->isPartitionV2Table()) &&
      createIndexNode->getPartitionIndexType() == StmtDDLNode::PARTITION_INDEX_LOCAL) {  // local partition index
    CmpSeabaseDDL::setMDflags(ii->indexFlags, MD_IS_LOCAL_BASE_PARTITION_FLG);
    CmpSeabaseDDL::setMDflags(tableInfo->objectFlags, MD_PARTITION_TABLE_V2);
    removePartNATable = TRUE;
  } else if (naTable->isPartitionEntityTable() &&
             createIndexNode->getPartitionIndexType() == StmtDDLNode::PARTITION_INDEX_LOCAL) {  // local partition index
    CmpSeabaseDDL::setMDflags(ii->indexFlags, MD_IS_LOCAL_PARTITION_FLG);
    CmpSeabaseDDL::setMDflags(tableInfo->objectFlags, MD_PARTITION_V2);
    removePartNATable = TRUE;
  } else if ((naTable->isPartitionEntityTable() || (naTable->isPartitionV2Table())) &&
             createIndexNode->getPartitionIndexType() == StmtDDLNode::PARTITION_INDEX_GLOBAL)
    CmpSeabaseDDL::setMDflags(ii->indexFlags, MD_IS_GLOBAL_PARTITION_FLG),
        removePartNATable = TRUE;  // global partiion index
  else if (naTable->isPartitionEntityTable() &&
           createIndexNode->getPartitionIndexType() == StmtDDLNode::PARTITION_INDEX_NORMAL) {
    CmpSeabaseDDL::setMDflags(ii->indexFlags, MD_IS_NORMAL_INDEX_FLG);  // normal partition index
    CmpSeabaseDDL::setMDflags(tableInfo->objectFlags, MD_PARTITION_V2);
  } else
    CmpSeabaseDDL::setMDflags(ii->indexFlags, MD_IS_NORMAL_INDEX_FLG);  // normal index

  NAList<HbaseCreateOption *> hbaseCreateOptions(STMTHEAP);
  NAString hco;

  if (alignedFormat) {
    hco += "ROW_FORMAT=>ALIGNED ";
  }

  short retVal =
      setupHbaseOptions(createIndexNode->getHbaseOptionsClause(), numSplits, extIndexName, hbaseCreateOptions, hco);
  if (retVal) {
    deallocEHI(ehi);
    processReturn();
    return;
  }
  tableInfo->hbaseCreateOptions = (hco.isNull() ? NULL : hco.data());

  // Create the index (offline) in its own transaction
  NABoolean xnWasStartedHere = FALSE;
  long objUID = -1;
  long objDataUID = -1;
  if (beginXnIfNotInProgress(&cliInterface, xnWasStartedHere, TRUE /* clearDDLList */, FALSE /*clearObjectLocks*/)) {
    deallocEHI(ehi);
    processReturn();
    return;
  }

  // Set the epoch value to allow read accesses
  if (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)) {
    retcode = ddlSetObjectEpoch(naTable, STMTHEAP, true /*allow reads*/, false /*not continuing*/);
    if (retcode < 0) {
      deallocEHI(ehi);
      processReturn();
      goto label_error;
    }
  }

  if (updateSeabaseMDTable(&cliInterface, catalogNamePart, schemaNamePart, objectNamePart, COM_INDEX_OBJECT, "N",
                           tableInfo, totalColCount, colInfoArray, totalColCount, keyInfoArray,
                           1,  // numIndex
                           ii, objUID)) {
    goto label_error;
  }

  objDataUID = objUID;

  extNameForHbase = genHBaseObjName(catalogNamePart, schemaNamePart, objectNamePart, btNamespace, objDataUID);

  HbaseStr hbaseIndex;
  hbaseIndex.val = (char *)extNameForHbase.data();
  hbaseIndex.len = extNameForHbase.length();

  if (NOT isPartitionV2Table ||
      (isPartitionV2Table && createIndexNode->getPartitionIndexType() == StmtDDLNode::PARTITION_INDEX_GLOBAL)) {
    HbaseStr hbaseTable;
    hbaseTable.val = (char *)extNameForHbase.data();
    hbaseTable.len = extNameForHbase.length();
    if (isMonarch) {
      NAList<HbaseStr> monarchCols(STMTHEAP);
      for (int i = 0; i < totalColCount; i++)
      //      for (int i = 0; i < indexColRefArray.entries(); i++)
      {
        NAString *nas = new (STMTHEAP) NAString();

        NAString colFam(colInfoArray[i].hbaseColFam);
        *nas = colFam;
        *nas += ":";

        char *colQualPtr = (char *)colInfoArray[i].hbaseColQual;
        int colQualLen = strlen(colQualPtr);
        if (colQualPtr[0] == '@') {
          *nas += "@";
          colQualPtr++;
          colQualLen--;
        }

        NAString colQual;
        HbaseAccess::convNumToId(colQualPtr, colQualLen, colQual);
        *nas += colQual;

        HbaseStr hbs;
        hbs.val = (char *)nas->data();
        hbs.len = nas->length();

        monarchCols.insert(hbs);
      }

      retcode = createMonarchTable(ehi, &hbaseTable, MonarchTableType::RANGE_PARTITIONED, monarchCols,
                                   &hbaseCreateOptions, numSplits, keyLength, encodedKeysBuffer);
    } else {
      NABoolean incrBackupEnabled = naTable->incrBackupEnabled();
      int enableSnapshot = CmpCommon::getDefaultNumeric(TM_SNAPSHOT_TABLE_CREATES);
      NABoolean snapShotOption = FALSE;
      if (incrBackupEnabled && enableSnapshot) snapShotOption = TRUE;

      retcode = createHbaseTable(ehi, &hbaseIndex, trafColFam.data(), &hbaseCreateOptions, numSplits, keyLength,
                                 encodedKeysBuffer, FALSE, createIndexNode->ddlXns(), snapShotOption);
    }

    if (retcode < 0) {
      goto label_error;
    }
  }

  // create index on partition
  if (isPartitionV2Table && createIndexNode->getPartitionIndexType() == StmtDDLNode::PARTITION_INDEX_LOCAL) {
    int ret;
    NAString pName;
    for (int i = 0; i < naTable->getNAPartitionArray().entries(); i++) {
      const char *partitionTableName = naTable->getNAPartitionArray()[i]->getPartitionEntityName();
      if (partitionTableName) {
        ret = createSeabasePartitionIndex(&cliInterface, createIndexNode, indexColRefArray, partitionTableName,
                                          catalogNamePart, schemaNamePart, objectNamePart);
        if (ret < 0) {
          cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
          deallocEHI(ehi);
          processReturn();
          return;
        }
      }
    }
  }

  endXnIfStartedHere(&cliInterface, xnWasStartedHere, 0, FALSE /*modifyEpoch*/, FALSE /*clearObjectLocks*/);

  // Populate the index outside of transaction
  if (NOT createIndexNode->isNoPopulateOptionSpecified()) {
    NABoolean useLoad = ((CmpCommon::getDefault(TRAF_LOAD_USE_FOR_INDEXES) == DF_ON) && (numTrafReplicas < 2));

    NABoolean indexOpt = (CmpCommon::getDefault(TRAF_INDEX_CREATE_OPT) == DF_ON);

    if (indexOpt && NOT isPartitionV2Table) {
      // validate that table is empty.
      // If table is empty, no need to load data into the index.
      NAString extTableNameForHbase =
          genHBaseObjName(btCatalogNamePart, btSchemaNamePart, btObjectNamePart, btNamespace, btObjUID);

      HbaseStr tblName;
      tblName.val = (char *)extTableNameForHbase.data();
      tblName.len = extNameForHbase.length();
      retcode = ehi->isEmpty(tblName);
      if (retcode < 0) {
        goto label_error_drop_index;
      }

      if (retcode == 0)  // not empty
        indexOpt = FALSE;
    }

    if (NOT indexOpt && (NOT isPartitionV2Table || (isPartitionV2Table && createIndexNode->getPartitionIndexType() ==
                                                                              StmtDDLNode::PARTITION_INDEX_GLOBAL))) {
      if (createIndexNode->isNgramSpecified()) {
        // populate index
        if (populateNgramIndexFromTable(&cliInterface, createIndexNode->isUniqueSpecified(), extIndexName,
                                        isVolatileTable ? volTabName : tableName, selColList, useLoad)) {
          goto label_error_drop_index;
        }
      } else {
        // populate index
        if (populateSeabaseIndexFromTable(&cliInterface, createIndexNode->isUniqueSpecified(), extIndexName,
                                          isVolatileTable ? volTabName : tableName, selColList, useLoad)) {
          goto label_error_drop_index;
        }
      }
    }

    // Update the epoch value to lock out other user access
    // For indexes created with NO POPULATE, we don't escalate since it changes nothing
    if (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)) {
      retcode = ddlSetObjectEpoch(naTable, STMTHEAP, false /*allow reads*/, true /*is continuing*/);
      if (retcode < 0) {
        goto label_error_drop_index;
      }
    }

    // Make index online in its own transaction.
    // If NO POPULATE specified, the index remains offline
    if (beginXnIfNotInProgress(&cliInterface, xnWasStartedHere, FALSE /*clearDDLList*/, FALSE /*clearObjectLocks*/)) {
      goto label_error_drop_index;
    }

    if (updateObjectValidDef(&cliInterface, catalogNamePart, schemaNamePart, objectNamePart, COM_INDEX_OBJECT_LIT,
                             "Y")) {
      goto label_error_drop_index;
    }

    if (createSnapshotForIncrBackup(&cliInterface, naTable->incrBackupEnabled(), naTable->getNamespace(),
                                    catalogNamePart, schemaNamePart, objectNamePart, objDataUID)) {
      goto label_error_drop_index;
    }

    endXnIfStartedHere(&cliInterface, xnWasStartedHere, 0, FALSE /*modifyEpoch*/, FALSE /*clearObjectLocks*/);
  }

  // Update the object redefine time in its own transaction.
  // Not sure if this is required for indexes with NO POPULATE but ...
  if (beginXnIfNotInProgress(&cliInterface, xnWasStartedHere, FALSE /*clearDDLList*/, FALSE /*clearObjectLocks*/)) {
    goto label_error_drop_index;
  }

  if (naTable->incrBackupEnabled()) {
    if (updateSeabaseXDCDDLTable(&cliInterface, btCatalogNamePart.data(), btSchemaNamePart.data(),
                                 btObjectNamePart.data(), COM_BASE_TABLE_OBJECT_LIT)) {
      goto label_error_drop_index;
    }
  }
  // Update the base table and shared cache
  if (updateObjectRedefTime(&cliInterface, btCatalogNamePart, btSchemaNamePart, btObjectNamePart,
                            COM_BASE_TABLE_OBJECT_LIT, -1, btObjUID, naTable->isStoredDesc(),
                            naTable->storedStatsAvail())) {
    goto label_error_drop_index;
  }

  if (naTable->isStoredDesc()) {
    insertIntoSharedCacheList(btCatalogNamePart, btSchemaNamePart, btObjectNamePart, COM_BASE_TABLE_OBJECT,
                              SharedCacheDDLInfo::DDL_UPDATE);
  }

  // update the index and shared cache
  if (naTable->isStoredDesc() || (CmpCommon::getDefault(TRAF_STORE_OBJECT_DESC) == DF_ON)) {
    if (updateObjectRedefTime(&cliInterface, catalogNamePart, schemaNamePart, objectNamePart, COM_INDEX_OBJECT_LIT, -1,
                              objUID, naTable->isStoredDesc(), naTable->storedStatsAvail())) {
      goto label_error_drop_index;
    }

    insertIntoSharedCacheList(catalogNamePart, schemaNamePart, objectNamePart, COM_INDEX_OBJECT,
                              SharedCacheDDLInfo::DDL_INSERT);
  }

  // Force all tables to update their cached entries
  // Not sure if this is required for indexes with NO POPULATE but ...
  //    added "removePartNATable" for remove natable for partition entity table
  //    we use a INTERNAL sql to create local partition index
  if (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL) || removePartNATable) {
    ActiveSchemaDB()->getNATableDB()->removeNATable(cn, ComQiScope::REMOVE_FROM_ALL_USERS, COM_BASE_TABLE_OBJECT,
                                                    createIndexNode->ddlXns(), FALSE);
  }

  /*remove natable for every partition if global index*/
  if (isPartitionV2Table && createIndexNode->getPartitionIndexType() == StmtDDLNode::PARTITION_INDEX_GLOBAL) {
    const NAPartitionArray &partitionArray = naTable->getPartitionArray();
    for (int i = 0; i < partitionArray.entries(); i++) {
      if (partitionArray[i]->hasSubPartition()) {
        const NAPartitionArray *subpArray = partitionArray[i]->getSubPartitions();
        for (int j = 0; j < subpArray->entries(); j++) {
          CorrName pcn((*subpArray)[j]->getPartitionEntityName(), STMTHEAP, schemaNamePart, catalogNamePart);
          ActiveSchemaDB()->getNATableDB()->removeNATable(pcn, ComQiScope::REMOVE_FROM_ALL_USERS, COM_BASE_TABLE_OBJECT,
                                                          createIndexNode->ddlXns(), FALSE);
        }
      } else {
        CorrName pcn(partitionArray[i]->getPartitionEntityName(), STMTHEAP, schemaNamePart, catalogNamePart);
        ActiveSchemaDB()->getNATableDB()->removeNATable(pcn, ComQiScope::REMOVE_FROM_ALL_USERS, COM_BASE_TABLE_OBJECT,
                                                        createIndexNode->ddlXns(), FALSE);
      }
    }
  }

  // end transaction (if not in a user transaction), will update shared cache,
  // send QI keys, and remove the DDL Lock
  endXnIfStartedHere(&cliInterface, xnWasStartedHere, 0);

  deallocEHI(ehi);

  return;

label_error:
  // Something happened, go see if transid is still active.  If not, reset DDL locks
  // Shared cache list has not been changed, so no need to free it
  if (GetCliGlobals()->currContext()->getTransaction()->getCurrentXnId(&transid) != 0) ddlResetObjectEpochs(TRUE, TRUE);

  // There is an active transction, this calls rollback and processes the DDLObj
  // and shared cache lists
  endXnIfStartedHere(&cliInterface, xnWasStartedHere, -1);

  // Just in case, remove DDL Lock
  if (GetCliGlobals()->currContext()->getTransaction()->getCurrentXnId(&transid) != 0) {
    CmpCommon::context()->ddlObjsList().endDDLOp(FALSE, CmpCommon::diags());
  }

  deallocEHI(ehi);
  return;

label_error_drop_index:
  // Something happened, go see if transid is still active.  If not, reset DDL locks
  // Shared cache list is changed within the last transaction, so endXnIfStartedHere
  // will adjust the list.
  if (GetCliGlobals()->currContext()->getTransaction()->getCurrentXnId(&transid) != 0) ddlResetObjectEpochs(TRUE, TRUE);
  endXnIfStartedHere(&cliInterface, xnWasStartedHere, -1);
  cleanupObjectAfterError(cliInterface, catalogNamePart, schemaNamePart, objectNamePart, COM_INDEX_OBJECT, &btNamespace,
                          FALSE);

  // Just in case, remove DDL Lock
  if (GetCliGlobals()->currContext()->getTransaction()->getCurrentXnId(&transid) != 0) {
    CmpCommon::context()->ddlObjsList().endDDLOp(FALSE, CmpCommon::diags());
  }

  deallocEHI(ehi);
  return;
}

short CmpSeabaseDDL::populateSeabaseIndexFromTable(ExeCliInterface *cliInterface, NABoolean isUnique,
                                                   const NAString &indexName, const ComObjectName &tableName,
                                                   NAList<NAString> &selColList, NABoolean useLoad) {
  int cliRC = 0;
  int saltRC = 0;

  NABoolean useHiveSrc = FALSE;
  NAString saltText;
  NAString hiveSrc = CmpCommon::getDefaultString(USE_HIVE_SOURCE);
  if (!hiveSrc.isNull()) {
    for (CollIndex i = 0; i < selColList.entries(); i++) {
      NAString &colName = selColList[i];
      if (colName == "SYSKEY") break;
      if (i == selColList.entries() - 1) useHiveSrc = TRUE;
    }
    if (useHiveSrc) {
      saltRC = getSaltText(cliInterface, tableName.getCatalogNamePartAsAnsiString().data(),
                           tableName.getSchemaNamePartAsAnsiString().data(),
                           tableName.getObjectNamePartAsAnsiString().data(), COM_BASE_TABLE_OBJECT_LIT, saltText);
      if (saltRC < 0) useHiveSrc = FALSE;
    }
  }

  NAString query = (isUnique ? "insert with no rollback " : "upsert using load ");
  if (useLoad) {
    // index table only option is used internally and is used to populate
    // the index table
    query = " Load with no output, no recovery, Index table only ";
  }
  query += "into table(index_table ";
  query += indexName;
  query += " ) select ";
  for (CollIndex i = 0; i < selColList.entries(); i++) {
    NAString &colName = selColList[i];
    if ((colName == "_SALT_") && useHiveSrc)
      query += saltText;
    else {
      query += "\"";
      query += colName;
      query += "\"";
    }

    if (i < selColList.entries() - 1) query += ",";
  }

  query += " from ";
  if (useHiveSrc) {
    query += "HIVE.HIVE.";
    query += tableName.getObjectNamePartAsAnsiString();
    query += hiveSrc;  // will not work for delim tab names
  } else
    query += tableName.getExternalName(TRUE);

  query += " ; ";

  UInt32 savedCliParserFlags = 0;
  SQL_EXEC_GetParserFlagsForExSqlComp_Internal(savedCliParserFlags);

  cliRC = cliInterface->holdAndSetCQDs(
      "ALLOW_DML_ON_NONAUDITED_TABLE, ATTEMPT_ESP_PARALLELISM, HIDE_INDEXES",
      "ALLOW_DML_ON_NONAUDITED_TABLE 'ON', ATTEMPT_ESP_PARALLELISM 'ON', HIDE_INDEXES 'ALL'");
  if (cliRC < 0) {
    return -1;
  }

  if (useLoad) {
    cliRC = cliInterface->holdAndSetCQD("TRAF_LOAD_FORCE_CIF", "OFF");
    if (cliRC < 0) {
      cliInterface->restoreCQDs("ALLOW_DML_ON_NONAUDITED_TABLE, ATTEMPT_ESP_PARALLELISM, HIDE_INDEXES");
      return -1;
    }
  }
  SQL_EXEC_SetParserFlagsForExSqlComp_Internal(ALLOW_SPECIALTABLETYPE);
  SQL_EXEC_SetParserFlagsForExSqlComp_Internal(ALLOW_VOLATILE_SCHEMA_IN_TABLE_NAME);
  // XXX: for object lock debug
  LOGDEBUG(CAT_SQL_LOCK, "Populate index %s for table %s", indexName.data(), tableName.getExternalName().data());
  cliRC = cliInterface->executeImmediate(query);

  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());

    if ((isUnique) && (CmpCommon::diags()->mainSQLCODE() == -EXE_DUPLICATE_ENTIRE_RECORD)) {
      *CmpCommon::diags() << DgSqlCode(-CAT_UNIQUE_INDEX_LOAD_FAILED_WITH_DUPLICATE_ROWS) << DgTableName(indexName);
    } else {
      *CmpCommon::diags() << DgSqlCode(-CAT_CLI_LOAD_INDEX) << DgTableName(indexName);
    }
  }

  //  SQL_EXEC_ResetParserFlagsForExSqlComp_Internal(ALLOW_SPECIALTABLETYPE);
  //  SQL_EXEC_ResetParserFlagsForExSqlComp_Internal(ALLOW_VOLATILE_SCHEMA_IN_TABLE_NAME);
  SQL_EXEC_AssignParserFlagsForExSqlComp_Internal(savedCliParserFlags);

  cliInterface->restoreCQDs("ALLOW_DML_ON_NONAUDITED_TABLE, ATTEMPT_ESP_PARALLELISM, HIDE_INDEXES");
  if (useLoad) cliInterface->restoreCQD("TRAF_LOAD_FORCE_CIF");

  if (cliRC < 0) return -1;

  return 0;
}

short CmpSeabaseDDL::populateNgramIndexFromTable(ExeCliInterface *cliInterface, NABoolean isUnique,
                                                 const NAString &indexName, const ComObjectName &tableName,
                                                 NAList<NAString> &selColList, NABoolean useLoad) {
  int cliRC = 0;
  int saltRC = 0;

  NABoolean useHiveSrc = FALSE;
  NAString saltText;
  NAString hiveSrc = CmpCommon::getDefaultString(USE_HIVE_SOURCE);
  if (!hiveSrc.isNull()) {
    for (CollIndex i = 0; i < selColList.entries(); i++) {
      NAString &colName = selColList[i];
      if (colName == "SYSKEY") break;
      if (i == selColList.entries() - 1) useHiveSrc = TRUE;
    }
    if (useHiveSrc) {
      saltRC = getSaltText(cliInterface, tableName.getCatalogNamePartAsAnsiString().data(),
                           tableName.getSchemaNamePartAsAnsiString().data(),
                           tableName.getObjectNamePartAsAnsiString().data(), COM_BASE_TABLE_OBJECT_LIT, saltText);
      if (saltRC < 0) useHiveSrc = FALSE;
    }
  }

  NAString query = (isUnique ? "insert with no rollback " : "upsert using load ");
  if (useLoad) {
    // index table only option is used internally and is used to populate
    // the index table
    query = " Load with no output, no recovery, Index table only ";
  }
  query += "into table(index_table ";
  query += indexName;
  query += " ) select * from udf(generate_ngram(table(select ";
  for (CollIndex i = 0; i < selColList.entries(); i++) {
    NAString &colName = selColList[i];
    if ((colName == "_SALT_") && useHiveSrc)
      query += saltText;
    else {
      query += "\"";
      query += colName;
      query += "\"";
    }

    if (i < selColList.entries() - 1) query += ",";
  }

  query += " from ";
  if (useHiveSrc) {
    query += "HIVE.HIVE.";
    query += tableName.getObjectNamePartAsAnsiString();
    query += hiveSrc;  // will not work for delim tab names
  } else
    query += tableName.getExternalName(TRUE);

  // query += " for read uncommitted access), '";
  // workaround for assert error in OptPhysRelExpr.cpp RelExpr::replacePivs()
  // CMPASSERT(myPartFunc->getPartitionInputValuesLayout().entries() ==
  //            childPartFunc->getPartitionInputValuesLayout().entries());
  query += "<<+cardinality 1e5 >> for read uncommitted access), '";
  if (selColList[0] == "_SALT_")
    query += selColList[1];
  else
    query += selColList[0];
  query += "'))";

  UInt32 savedCliParserFlags = 0;
  SQL_EXEC_GetParserFlagsForExSqlComp_Internal(savedCliParserFlags);

  cliRC = cliInterface->holdAndSetCQDs(
      "ALLOW_DML_ON_NONAUDITED_TABLE, ATTEMPT_ESP_PARALLELISM, HIDE_INDEXES",
      "ALLOW_DML_ON_NONAUDITED_TABLE 'ON', ATTEMPT_ESP_PARALLELISM 'ON', HIDE_INDEXES 'ALL'");
  if (cliRC < 0) {
    return -1;
  }

  if (useLoad) {
    cliRC = cliInterface->holdAndSetCQD("TRAF_LOAD_FORCE_CIF", "OFF");
    if (cliRC < 0) {
      cliInterface->restoreCQDs("ALLOW_DML_ON_NONAUDITED_TABLE, ATTEMPT_ESP_PARALLELISM, HIDE_INDEXES");
      return -1;
    }
  }
  SQL_EXEC_SetParserFlagsForExSqlComp_Internal(ALLOW_SPECIALTABLETYPE);
  SQL_EXEC_SetParserFlagsForExSqlComp_Internal(ALLOW_VOLATILE_SCHEMA_IN_TABLE_NAME);
  cliRC = cliInterface->executeImmediate(query);

  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());

    if ((isUnique) && (CmpCommon::diags()->mainSQLCODE() == -EXE_DUPLICATE_ENTIRE_RECORD)) {
      *CmpCommon::diags() << DgSqlCode(-CAT_UNIQUE_INDEX_LOAD_FAILED_WITH_DUPLICATE_ROWS) << DgTableName(indexName);
    } else {
      *CmpCommon::diags() << DgSqlCode(-CAT_CLI_LOAD_INDEX) << DgTableName(indexName);
    }
  }

  //  SQL_EXEC_ResetParserFlagsForExSqlComp_Internal(ALLOW_SPECIALTABLETYPE);
  //  SQL_EXEC_ResetParserFlagsForExSqlComp_Internal(ALLOW_VOLATILE_SCHEMA_IN_TABLE_NAME);
  SQL_EXEC_AssignParserFlagsForExSqlComp_Internal(savedCliParserFlags);

  cliInterface->restoreCQDs("ALLOW_DML_ON_NONAUDITED_TABLE, ATTEMPT_ESP_PARALLELISM, HIDE_INDEXES");
  if (useLoad) cliInterface->restoreCQD("TRAF_LOAD_FORCE_CIF");

  if (cliRC < 0) return -1;

  return 0;
}

int CmpSeabaseDDL::populateSeabasePartIndex(NABoolean ispopulateForAll, NABoolean isPurgedataSpecified,
                                            NABoolean isUnique, const char *tableName, const char *indexName,
                                            ExeCliInterface *cliInterface) {
  int retcode;
  NAString populateQuery;

  if (ispopulateForAll)
    populateQuery.format("POPULATE ALL %s INDEXES ON %s %s;", isUnique ? "UNIQUE" : "", tableName,
                         isPurgedataSpecified ? "PURGEDATA" : "");
  else
    populateQuery.format("POPULATE INDEX %s ON %s %s;", indexName, tableName, isPurgedataSpecified ? "PURGEDATA" : "");
  retcode = cliInterface->executeImmediate(populateQuery.data());
  if (retcode < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }
  return retcode;
}

void CmpSeabaseDDL::populateSeabaseIndex(StmtDDLPopulateIndex *populateIndexNode, NAString &currCatName,
                                         NAString &currSchName) {
  int retcode = 0;
  long transid;

  ComObjectName tableName(populateIndexNode->getTableName());
  ComAnsiNamePart currCatAnsiName(currCatName);
  ComAnsiNamePart currSchAnsiName(currSchName);
  tableName.applyDefaults(currCatAnsiName, currSchAnsiName);
  const NAString btCatalogNamePart = tableName.getCatalogNamePartAsAnsiString();
  const NAString btSchemaNamePart = tableName.getSchemaNamePartAsAnsiString(TRUE);
  NAString btObjectNamePart = tableName.getObjectNamePartAsAnsiString(TRUE);
  const NAString extTableName = tableName.getExternalName(TRUE);

  ComObjectName indexName(populateIndexNode->getIndexName());
  indexName.applyDefaults(btCatalogNamePart, btSchemaNamePart);

  const NAString catalogNamePart = indexName.getCatalogNamePartAsAnsiString();
  const NAString schemaNamePart = indexName.getSchemaNamePartAsAnsiString(TRUE);
  NAString objectNamePart = indexName.getObjectNamePartAsAnsiString(TRUE);
  const NAString extIndexName = indexName.getExternalName(TRUE);

  if (isSeabaseReservedSchema(indexName)) {
    *CmpCommon::diags() << DgSqlCode(-1118) << DgTableName(extTableName);
    return;
  }

  // special tables, like index, can only be purged in internal mode with special
  // flag settings. Otherwise, it can make database inconsistent.
  if ((populateIndexNode->purgedataSpecified()) && (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL))) {
    *CmpCommon::diags() << DgSqlCode(-1010);
    return;
  }

  if (lockObjectDDL(tableName, COM_BASE_TABLE_OBJECT, populateIndexNode->isVolatile(),
                    populateIndexNode->isExternal()) < 0) {
    return;
  }

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());

  //  If an index was created with NO POPULATE option, then
  // it will be marked as invalid in metadata. The regular table descriptor will
  // not have information about invalid indexes as we dont want those to
  // be used in queries.
  // Create a table descriptor that contains information on both valid and
  // invalid indexes. Pass that to getNATable method which will use this
  // table desc to create the NATable struct.
  TrafDesc *tableDesc = getSeabaseTableDesc(tableName.getCatalogNamePart().getInternalName(),
                                            tableName.getSchemaNamePart().getInternalName(),
                                            tableName.getObjectNamePart().getInternalName(), COM_BASE_TABLE_OBJECT,
                                            TRUE /*return info on valid and invalid indexes */);

  BindWA bindWA(ActiveSchemaDB(), CmpCommon::context(), TRUE /*inDDL*/);
  CorrName cn(tableName.getObjectNamePart().getInternalName(), STMTHEAP,
              tableName.getSchemaNamePart().getInternalName(), tableName.getCatalogNamePart().getInternalName());

  NATable *naTable = bindWA.getNATable(cn, TRUE, tableDesc);
  if (naTable == NULL || bindWA.errStatus()) {
    *CmpCommon::diags() << DgSqlCode(-4082) << DgTableName(cn.getExposedNameAsAnsiString());

    processReturn();

    return;
  }

  if (naTable->readOnlyEnabled() && !Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)) {
    *CmpCommon::diags() << DgSqlCode(-8644);
    processReturn();
    return;
  }

  // Verify that current user has authority to populate the index
  // User must be DB__ROOT or have privileges
  PrivMgrUserPrivs *privs = naTable->getPrivInfo();
  if (isAuthorizationEnabled() && privs == NULL) {
    *CmpCommon::diags() << DgSqlCode(-CAT_UNABLE_TO_RETRIEVE_PRIVS);

    processReturn();

    return;
  }

  // Requester must have SELECT and INSERT privileges
  if (!ComUser::isRootUserID() && isAuthorizationEnabled() && !Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)) {
    NABoolean hasPriv = TRUE;
    if (!privs->hasSelectPriv()) {
      hasPriv = FALSE;
      *CmpCommon::diags() << DgSqlCode(-4481) << DgString0("SELECT") << DgString1(extTableName.data());
    }

    if (!privs->hasInsertPriv()) {
      hasPriv = FALSE;
      *CmpCommon::diags() << DgSqlCode(-4481) << DgString0("INSERT") << DgString1(extTableName.data());
    }
    if (!hasPriv) {
      processReturn();

      return;
    }
  }

  // Update the epoch value to lock out other user access
  if (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)) {
    retcode = ddlSetObjectEpoch(naTable, STMTHEAP, true, false);
    if (retcode < 0) {
      processReturn();
      return;
    }
  }
  NABoolean continueObjectEpochCalled = FALSE;

  const NAFileSetList &indexList = naTable->getIndexList();
  NABoolean xnWasStartedHere = FALSE;
  NABoolean foundIndex = FALSE;
  NABoolean dontForceCleanup = FALSE;
  NABoolean isPartitionTable = FALSE;
  NABoolean ispopulateForAll = FALSE;
  NABoolean ispopulateForASingle = FALSE;

  if (naTable->isPartitionV2Table()) isPartitionTable = TRUE;

  if (populateIndexNode->populateIndexOnPartition() && !isPartitionTable) {
    *CmpCommon::diags() << DgSqlCode(-3242) << DgString0("POPULATE PARTITION INDEX ON A REGULAR TABLE");
    goto label_return;
  }

  if (populateIndexNode->populateIndexOnPartition()) {
    const NAPartitionArray &partArray = naTable->getPartitionArray();
    NAString popuPartitionName = populateIndexNode->getPartitionName();
    NAString partEntityName;
    NABoolean isMatched = FALSE;

    for (int i = 0; i < partArray.entries(); i++) {
      NAPartition *partition = partArray[i];
      const char *partitionName = partition->getPartitionName();

      if (strcmp(popuPartitionName.data(), partitionName) == 0) {
        isMatched = TRUE;
        partEntityName = partition->getPartitionEntityName();
      }
    }

    if (!isMatched) {
      // The specified partition partName does not exist.
      *CmpCommon::diags() << DgSqlCode(-1097) << DgString0(popuPartitionName.data());
      goto label_return;
    }
    retcode = populateSeabasePartIndex(FALSE, populateIndexNode->purgedataSpecified(), FALSE, partEntityName.data(),
                                       objectNamePart.data(), &cliInterface);

    if (retcode < 0) {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      goto label_return;
    }
  } else if (isPartitionTable) {
    if (beginXnIfNotInProgress(&cliInterface, xnWasStartedHere, FALSE /*clearDDLObjList*/,
                               FALSE /*clearObjectLocks*/)) {
      processReturn();
      goto purgedata_return;
    }
    int index = -1;
    const NAPartitionArray &partArray = naTable->getPartitionArray();
    // if populate all index on table
    // update index status on base table
    for (int i = 0; i < indexList.entries(); i++) {
      const NAFileSet *naf = indexList[i];
      if (naf->getKeytag() == 0) continue;

      const QualifiedName &qn = naf->getFileSetName();
      const NAString &nafIndexName = qn.getQualifiedNameAsAnsiString(TRUE);
      objectNamePart = qn.getObjectName().data();
      if (populateIndexNode->populateAll() || (populateIndexNode->populateAllUnique() && naf->uniqueIndex()) ||
          extIndexName == nafIndexName) {
        foundIndex = TRUE;
        index = i;
        if (updateObjectValidDef(&cliInterface, catalogNamePart, schemaNamePart, objectNamePart, COM_INDEX_OBJECT_LIT,
                                 "Y")) {
          processReturn();
          goto label_rollback;
        }
      }
    }

    if (populateIndexNode->populateAll() || populateIndexNode->populateAllUnique())
      ispopulateForAll = TRUE;
    else if (foundIndex)
      ispopulateForASingle = TRUE;
    else {
      *CmpCommon::diags() << DgSqlCode(-CAT_OBJECT_DOES_NOT_EXIST_IN_TRAFODION) << DgString0(extIndexName);

      goto label_rollback;
    }

    // populate all index on partitions
    for (int i = 0; i < naTable->FisrtLevelPartitionCount(); i++) {
      char partitionIndexName[256];
      if (ispopulateForASingle) {
        objectNamePart = indexList[index]->getFileSetName().getObjectName().data();
        getPartitionIndexName(partitionIndexName, 256, objectNamePart, partArray[i]->getPartitionEntityName());
      }
      // else
      //   partitionIndexName = nullptr;

      retcode = populateSeabasePartIndex(ispopulateForAll,                         // is populate all index
                                         populateIndexNode->purgedataSpecified(),  // is purgedata Specified
                                         populateIndexNode->populateAllUnique(),   // is populate for Unique index
                                         partArray[i]->getPartitionEntityName(),   // on entity partition
                                         partitionIndexName,                       // index name
                                         &cliInterface);
      if (retcode < 0) {
        cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
        goto label_rollback;
      }
    }
    endXnIfStartedHere(&cliInterface, xnWasStartedHere, 0, FALSE /*modifyEpoch*/, FALSE /*clearObjectLocks*/);
  } else {
    for (int i = 0; i < indexList.entries(); i++) {
      const NAFileSet *naf = indexList[i];
      if (naf->getKeytag() == 0) continue;

      const QualifiedName &qn = naf->getFileSetName();
      const NAString &nafIndexName = qn.getQualifiedNameAsAnsiString(TRUE);

      if ((populateIndexNode->populateAll()) || (populateIndexNode->populateAllUnique() && naf->uniqueIndex()) ||
          (extIndexName == nafIndexName)) {
        if (populateIndexNode->populateAll() || populateIndexNode->populateAllUnique()) {
          objectNamePart = qn.getObjectName().data();
        } else
          foundIndex = TRUE;

        // check if nafIndexName is a valid index. Is so, it has already been
        // populated. Skip it.
        NABoolean isValid = existsInSeabaseMDTable(&cliInterface, qn.getCatalogName().data(), qn.getSchemaName().data(),
                                                   qn.getObjectName().data());
        if (isValid) {
          if ((populateIndexNode->populateAll()) || (populateIndexNode->populateAllUnique() && naf->uniqueIndex()))
            continue;
          else {
            // we get error here if user are command
            // POPULATE INDEX online
            *CmpCommon::diags() << DgSqlCode(-20205) << DgString0(extIndexName);
            processReturn();
            goto label_return;
          }
        }
        long objDataUID;
        long indexUID = getObjectUID(&cliInterface, catalogNamePart.data(), schemaNamePart.data(),
                                      objectNamePart.data(), COM_INDEX_OBJECT_LIT, &objDataUID);

        if (populateIndexNode->purgedataSpecified()) {
          // Update object Epoch since a purgedata is called
          if (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL) && continueObjectEpochCalled == FALSE) {
            retcode = ddlSetObjectEpoch(naTable, STMTHEAP, false, true);
            if (retcode < 0) goto label_return;
            continueObjectEpochCalled = TRUE;
          }

          // there is no error yet, but this method will call purgedata on the index
          retcode = purgedataObjectAfterError(cliInterface, catalogNamePart, schemaNamePart, objectNamePart,
                                              COM_INDEX_OBJECT, dontForceCleanup);
          if (retcode < 0) goto label_return;
        }

        NAList<NAString> selColList(STMTHEAP);

        for (int ii = 0; ii < naf->getAllColumns().entries(); ii++) {
          NAColumn *nac = naf->getAllColumns()[ii];

          const NAString &colName = nac->getColName();

          selColList.insert(colName);
        }

        // make the index unaudited during population to avoid transactional overhead.
        if (updateObjectAuditAttr(&cliInterface, catalogNamePart, schemaNamePart, objectNamePart, FALSE,
                                  COM_INDEX_OBJECT_LIT)) {
          processReturn();

          goto label_return;
        }

        NABoolean useLoad =
            ((CmpCommon::getDefault(TRAF_LOAD_USE_FOR_INDEXES) == DF_ON) && (naTable->getNumTrafReplicas() < 2));

        if (populateSeabaseIndexFromTable(&cliInterface, naf->uniqueIndex(), nafIndexName, tableName, selColList,
                                          useLoad)) {
          processReturn();
          goto purgedata_return;
        }

        if (updateObjectAuditAttr(&cliInterface, catalogNamePart, schemaNamePart, objectNamePart, TRUE,
                                  COM_INDEX_OBJECT_LIT)) {
          processReturn();
          goto purgedata_return;
        }

        if (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL) && continueObjectEpochCalled == FALSE) {
          retcode = ddlSetObjectEpoch(naTable, STMTHEAP, false, true);
          if (retcode < 0) goto purgedata_return;
          continueObjectEpochCalled = TRUE;
        }

        if (beginXnIfNotInProgress(&cliInterface, xnWasStartedHere, FALSE /*clearDDLObjList*/,
                                   FALSE /*clearObjectLocks*/)) {
          processReturn();
          goto purgedata_return;
        }
        if (updateObjectValidDef(&cliInterface, catalogNamePart, schemaNamePart, objectNamePart, COM_INDEX_OBJECT_LIT,
                                 "Y")) {
          processReturn();
          goto purgedata_return;
        }

        endXnIfStartedHere(&cliInterface, xnWasStartedHere, 0, FALSE /*modifyEpoch*/, FALSE /*clearObjectLocks*/);

        if (createSnapshotForIncrBackup(&cliInterface, naTable->incrBackupEnabled(), naTable->getNamespace(),
                                        catalogNamePart, schemaNamePart, objectNamePart, objDataUID)) {
          processReturn();
          goto purgedata_return;
        }
      }
    }  // for
  }

  if ((NOT(populateIndexNode->populateAll() || populateIndexNode->populateAllUnique())) && (NOT foundIndex) &&
      (NOT isPartitionTable)) {
    *CmpCommon::diags() << DgSqlCode(-CAT_OBJECT_DOES_NOT_EXIST_IN_TRAFODION) << DgString0(extIndexName);

    ddlResetObjectEpochs(TRUE, TRUE);

    // Release all DDL object locks
    LOGTRACE(CAT_SQL_LOCK, "Release all DDL object locks");
    CmpCommon::context()->releaseAllDDLObjectLocks();

    processReturn();
    return;
  }

  if (indexList.entries() > 0 || isPartitionTable) {
    if (naTable->incrBackupEnabled()) {
      if (updateSeabaseXDCDDLTable(&cliInterface, btCatalogNamePart.data(), btSchemaNamePart.data(),
                                   btObjectNamePart.data(), COM_BASE_TABLE_OBJECT_LIT)) {
        processReturn();
        goto purgedata_return;
      }
    }

    long btObjUID = naTable->objectUid().castToInt64();
    if (updateObjectRedefTime(&cliInterface, btCatalogNamePart, btSchemaNamePart, btObjectNamePart,
                              COM_BASE_TABLE_OBJECT_LIT, -1, btObjUID, naTable->isStoredDesc(),
                              naTable->storedStatsAvail())) {
      processReturn();
      goto purgedata_return;
    }

    // Add to shared cache list
    if (naTable->isStoredDesc())
      insertIntoSharedCacheList(btCatalogNamePart, btSchemaNamePart, btObjectNamePart, COM_BASE_TABLE_OBJECT,
                                SharedCacheDDLInfo::DDL_UPDATE);

    /**
     * When we do as follow:
     *
     *  SQL: LOAD WITH REBUILD INDEXES INTO t1 VALUES(1),(2);
     *
     * it's include some substatements like this:
     *  ...
     *  SQL1: ALTER TABLE t1 DISABLE ALL INDEXES;
     *  SQL2: LOAD TRANSFORM INTO t1 VALUES(1),(2)
     *  SQL3: POPULATE ALL INDEXES ON t1;
     *  ...
     * after the SQL2 was done, SQL3 will be treated as
     * an internal operation and executed. So the removeNATable()
     * will not be executed here, QueryCache also not be cleared.
     *
     * But, when we execute 'LOAD INTO t1 VALUES(1),(2)' again,
     * It's also include SQL3 substatement, and will use QueryPlan
     * which from QueryCache, There will report an error because of
     * this QueryPlan has no index operations.
     * So i canceled the following line.
     **/
    // if (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL))
    {
      ActiveSchemaDB()->getNATableDB()->removeNATable(cn, ComQiScope::REMOVE_FROM_ALL_USERS, COM_BASE_TABLE_OBJECT,
                                                      populateIndexNode->ddlXns(), FALSE);
    }
  }

label_return:
  if (GetCliGlobals()->currContext()->getTransaction()->getCurrentXnId(&transid) != 0) {
    finalizeSharedCache();

    // Release all DDL object locks
    LOGTRACE(CAT_SQL_LOCK, "Release all DDL object locks");
    CmpCommon::context()->releaseAllDDLObjectLocks();

    ddlResetObjectEpochs(FALSE, TRUE);

    // Release all DDL object locks
    LOGTRACE(CAT_SQL_LOCK, "Release all DDL object locks");
    CmpCommon::context()->releaseAllDDLObjectLocks();
  }

  processReturn();
  return;

label_rollback:
  endXnIfStartedHere(&cliInterface, xnWasStartedHere, -1, FALSE /*modifyEpochs*/, FALSE /*clearObjectLocks*/);
  goto label_return;

purgedata_return:
  if (GetCliGlobals()->currContext()->getTransaction()->getCurrentXnId(&transid) != 0) {
    ddlResetObjectEpochs(TRUE, TRUE);

    // Release all DDL object locks
    LOGTRACE(CAT_SQL_LOCK, "Release all DDL object locks");
    CmpCommon::context()->releaseAllDDLObjectLocks();
  }

  endXnIfStartedHere(&cliInterface, xnWasStartedHere, -1);
  updateObjectAuditAttr(&cliInterface, catalogNamePart, schemaNamePart, objectNamePart, TRUE, COM_INDEX_OBJECT_LIT);
  purgedataObjectAfterError(cliInterface, catalogNamePart, schemaNamePart, objectNamePart, COM_INDEX_OBJECT,
                            dontForceCleanup);
  processReturn();
  return;
}

int CmpSeabaseDDL::dropSeabasePartitionIndex(ExeCliInterface *cliInterface, NAString &currCatName,
                                               NAString &currSchName, const NAString &baseIndexName,
                                               const char *partitionTableName) {
  int ret;
  char buf[500];

  char partitionIndexName[256];
  getPartitionIndexName(partitionIndexName, 256, baseIndexName, partitionTableName);
  str_sprintf(buf, "DROP LOCAL INDEX %s.\"%s\".%s", currCatName.data(), currSchName.data(), partitionIndexName);

  ret = cliInterface->executeImmediate(buf);
  return ret;
}

void CmpSeabaseDDL::dropSeabaseIndex(StmtDDLDropIndex *dropIndexNode, NAString &currCatName, NAString &currSchName) {
  int retcode = 0;
  int cliRC = 0;
  long transid = 0;

  NAString idxName = dropIndexNode->getIndexName();

  ComObjectName indexName(idxName);
  ComAnsiNamePart currCatAnsiName(currCatName);
  ComAnsiNamePart currSchAnsiName(currSchName);
  indexName.applyDefaults(currCatAnsiName, currSchAnsiName);

  NAString catalogNamePart = indexName.getCatalogNamePartAsAnsiString();
  NAString schemaNamePart = indexName.getSchemaNamePartAsAnsiString(TRUE);
  NAString objectNamePart = indexName.getObjectNamePartAsAnsiString(TRUE);
  const NAString extIndexName = indexName.getExternalName(TRUE);

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());

  NABoolean isVolatile = FALSE;

  if ((dropIndexNode->isVolatile()) && (CmpCommon::context()->sqlSession()->volatileSchemaInUse())) isVolatile = TRUE;

  if ((NOT dropIndexNode->isVolatile()) && (CmpCommon::context()->sqlSession()->volatileSchemaInUse())) {
    QualifiedName *qn = CmpCommon::context()->sqlSession()->updateVolatileQualifiedName(objectNamePart);

    if (qn == NULL) {
      *CmpCommon::diags() << DgSqlCode(-CAT_UNABLE_TO_DROP_OBJECT) << DgTableName(objectNamePart);

      processReturn();

      return;
    }

    ComObjectName volTabName(qn->getQualifiedNameAsAnsiString());
    volTabName.applyDefaults(currCatAnsiName, currSchAnsiName);

    NAString vtCatNamePart = volTabName.getCatalogNamePartAsAnsiString();
    NAString vtSchNamePart = volTabName.getSchemaNamePartAsAnsiString(TRUE);
    NAString vtObjNamePart = volTabName.getObjectNamePartAsAnsiString(TRUE);

    retcode = existsInSeabaseMDTable(&cliInterface, vtCatNamePart, vtSchNamePart, vtObjNamePart, COM_INDEX_OBJECT);

    if (retcode < 0) {
      processReturn();

      return;
    }

    if (retcode == 1) {
      // table found in volatile schema
      // Validate volatile table name.
      if (CmpCommon::context()->sqlSession()->validateVolatileQualifiedName(
              dropIndexNode->getOrigIndexNameAsQualifiedName())) {
        // Valid volatile table. Drop it.
        idxName = volTabName.getExternalName(TRUE);

        catalogNamePart = vtCatNamePart;
        schemaNamePart = vtSchNamePart;
        objectNamePart = vtObjNamePart;

        isVolatile = TRUE;
      } else {
        // volatile table found but the name is not a valid
        // volatile name. Look for the input name in the regular
        // schema.
        // But first clear the diags area.
        CmpCommon::diags()->clear();
      }
    } else {
      CmpCommon::diags()->clear();
    }
  }

  retcode = existsInSeabaseMDTable(&cliInterface, catalogNamePart, schemaNamePart, objectNamePart, COM_INDEX_OBJECT,
                                   TRUE, TRUE, TRUE);

  // if index exists but is marked as invalid in metadata, treat it
  // as valid index so it could be dropped.
  if ((retcode < 0) && (CmpCommon::diags()->mainSQLCODE() == -4254)) {
    CmpCommon::diags()->clear();
    retcode = 1;
  }

  if (retcode < 0) {
    processReturn();

    return;
  }

  if (retcode == 0)  // does not exist
  {
    if (NOT dropIndexNode->dropIfExists()) {
      if (isVolatile)
        *CmpCommon::diags() << DgSqlCode(-CAT_OBJECT_DOES_NOT_EXIST_IN_TRAFODION) << DgString0(objectNamePart);
      else
        *CmpCommon::diags() << DgSqlCode(-CAT_OBJECT_DOES_NOT_EXIST_IN_TRAFODION) << DgString0(extIndexName);
    }

    processReturn();

    return;
  }

  // if 'no check' option is specified, then dont check for dependent constraints.
  // if cascade option is specified, then drop dependent constraints on this index.
  //

  // get base table name and base table uid
  NAString btCatName;
  NAString btSchName;
  NAString btObjName;
  long btUID;
  int btObjOwner = 0;
  int btSchemaOwner = 0;
  if (getBaseTable(&cliInterface, catalogNamePart, schemaNamePart, objectNamePart, btCatName, btSchName, btObjName,
                   btUID, btObjOwner, btSchemaOwner)) {
    processReturn();

    return;
  }

  // check whether index table is read only?
  if (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)) {
    long numEntries = 0;
    int len;
    char queryStr[1000];
    str_sprintf(queryStr,
                "SELECT COUNT(*) FROM %s.\"%s\".%s WHERE BITAND(FLAGS, %d) != 0 AND CATALOG_NAME= '%s' AND "
                "SCHEMA_NAME='%s' AND OBJECT_NAME='%s' AND OBJECT_TYPE='IX'",
                TRAFODION_SYSTEM_CATALOG, SEABASE_MD_SCHEMA, SEABASE_OBJECTS, MD_OBJECTS_READ_ONLY_ENABLED,
                catalogNamePart.data(), schemaNamePart.data(), objectNamePart.data());
    if (cliInterface.executeImmediate(queryStr, (char *)&numEntries, &len, FALSE) < 0) {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      processReturn();
      return;
    }

    if (numEntries != 0) {  // read only index can not be dropped
      *CmpCommon::diags() << DgSqlCode(-8644);
      processReturn();
      return;
    }
  }

  BindWA bindWA(ActiveSchemaDB(), CmpCommon::context(), TRUE /*inDDL*/);

  CorrName cn(btObjName, STMTHEAP, btSchName, btCatName);

  retcode = lockObjectDDL(cn.getQualifiedNameObj(), COM_BASE_TABLE_OBJECT, dropIndexNode->isVolatile(),
                          dropIndexNode->isExternal());
  if (retcode < 0) {
    processReturn();
    return;
  }

  NATable *naTable = bindWA.getNATable(cn);
  if (naTable == NULL || bindWA.errStatus()) {
    *CmpCommon::diags() << DgSqlCode(-4082) << DgTableName(extIndexName);

    processReturn();
    return;
  }

  if (isLockedForBR(naTable->objectUid().castToInt64(), naTable->getSchemaUid().castToInt64(), &cliInterface, cn)) {
    processReturn();
    return;
  }

  NAString tableNamespace = naTable->getNamespace();

  ExpHbaseInterface *ehi = allocEHI(naTable->storageType());
  if (ehi == NULL) {
    processReturn();

    return;
  }

  // Update the epoch value to lock out other user access
  if (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)) {
    retcode = ddlSetObjectEpoch(naTable, STMTHEAP);
    if (retcode < 0) {
      processReturn();
      return;
    }
  }

  // Verify that current user has authority to drop the index
  if ((!isDDLOperationAuthorized(SQLOperation::DROP_INDEX, btObjOwner, btSchemaOwner, btUID, COM_BASE_TABLE_OBJECT,
                                 naTable->getPrivInfo())) &&
      (!isDDLOperationAuthorized(SQLOperation::ALTER_TABLE, btObjOwner, btSchemaOwner, btUID, COM_BASE_TABLE_OBJECT,
                                 naTable->getPrivInfo()))) {
    *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);

    processReturn();

    return;
  }

  long indexUID = getObjectUID(&cliInterface, catalogNamePart.data(), schemaNamePart.data(), objectNamePart.data(),
                                COM_INDEX_OBJECT_LIT);
  if (indexUID < 0) {
    processReturn();
    deallocEHI(ehi);
    return;
  }

  long indexFlags = getIndexFlags(&cliInterface, btUID, indexUID);

  if (naTable->isPartitionV2Table() && CmpSeabaseDDL::isMDflagsSet(indexFlags, MD_IS_GLOBAL_PARTITION_FLG)) {
    retcode = lockObjectDDL(naTable, catalogNamePart, schemaNamePart, TRUE);
    if (retcode < 0) {
      processReturn();
      return;
    }
  }

  if (dropIndexNode->getDropBehavior() != COM_NO_CHECK_DROP_BEHAVIOR) {
    NAString constrCatName;
    NAString constrSchName;
    NAString constrObjName;
    long constrUID = getConstraintOnIndex(&cliInterface, btUID, indexUID, COM_UNIQUE_CONSTRAINT_LIT, constrCatName,
                                           constrSchName, constrObjName);
    if (constrUID > 0) {
      // constraint exists
      if (dropIndexNode->getDropBehavior() != COM_CASCADE_DROP_BEHAVIOR) {
        *CmpCommon::diags() << DgSqlCode(-CAT_DEPENDENT_CONSTRAINT_EXISTS);

        processReturn();

        deallocEHI(ehi);

        return;
      }

      // drop the constraint
      char buf[4000];
      str_sprintf(buf, "alter table \"%s\".\"%s\".\"%s\" drop constraint %s.%s.%s no check", btCatName.data(),
                  btSchName.data(), btObjName.data(), constrCatName.data(), constrSchName.data(), constrObjName.data());

      cliRC = cliInterface.executeImmediate(buf);

      if (cliRC < 0) {
        cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

        processReturn();

        deallocEHI(ehi);

        return;
      }
    }

    constrUID = getConstraintOnIndex(&cliInterface, btUID, indexUID, COM_FOREIGN_KEY_CONSTRAINT_LIT, constrCatName,
                                     constrSchName, constrObjName);
    if (constrUID > 0) {
      // constraint exists
      if (dropIndexNode->getDropBehavior() != COM_CASCADE_DROP_BEHAVIOR) {
        *CmpCommon::diags() << DgSqlCode(-CAT_DEPENDENT_CONSTRAINT_EXISTS);

        processReturn();

        deallocEHI(ehi);

        return;
      }

      // drop the constraint
      char buf[4000];
      str_sprintf(buf, "alter table \"%s\".\"%s\".\"%s\" drop constraint %s.%s.%s no check", btCatName.data(),
                  btSchName.data(), btObjName.data(), constrCatName.data(), constrSchName.data(), constrObjName.data());

      cliRC = cliInterface.executeImmediate(buf);

      if (cliRC < 0) {
        cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

        processReturn();

        deallocEHI(ehi);

        return;
      }
    }
  }

  // add X lock
  if (lockRequired(ehi, naTable, catalogNamePart, schemaNamePart, objectNamePart, HBaseLockMode::LOCK_X,
                   FALSE /* useHbaseXN */, FALSE /* replSync */, FALSE /* incrementalBackup */,
                   FALSE /* asyncOperation */, TRUE /* noConflictCheck */, TRUE /* registerRegion */)) {
    processReturn();
    return;
  }

  // TEMP:  until DDL and BR locks can be co-ordinated, check
  // again for a BR lock. Still a window where this check succeeds
  // and BR gets the lock and tries to backup the non-existent index
  if (isLockedForBR(naTable->objectUid().castToInt64(), naTable->getSchemaUid().castToInt64(), &cliInterface, cn)) {
    processReturn();
    return;
  }

  // can not direct drop partition local index
  if (CmpSeabaseDDL::isMDflagsSet(indexFlags, MD_IS_LOCAL_PARTITION_FLG) &&
      dropIndexNode->getPartitionIndexType() == StmtDDLNode::PARTITION_INDEX_NONE &&
      naTable->isPartitionEntityTable()) {
    *CmpCommon::diags() << DgSqlCode(-4265) << DgString0(idxName.data());
    processReturn();
    return;
  }

  if (naTable->isPartitionV2Table() && NOT CmpSeabaseDDL::isMDflagsSet(indexFlags, MD_IS_GLOBAL_PARTITION_FLG)) {
    int ret;
    // drop index for every partition if exist
    for (int i = 0; i < naTable->getNAPartitionArray().entries(); i++) {
      const char *partitionTableName = naTable->getNAPartitionArray()[i]->getPartitionEntityName();
      if (partitionTableName) {
        ret = dropSeabasePartitionIndex(&cliInterface, catalogNamePart, schemaNamePart, objectNamePart,
                                        partitionTableName);

        if (ret < 0) {
          cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
          deallocEHI(ehi);
          processReturn();
          return;
        }
      }
    }
  }

  NABoolean dropFromStorage = TRUE;
  NABoolean dropFromMD = TRUE;
  if (dropSeabaseObject(ehi, idxName, currCatName, currSchName, COM_INDEX_OBJECT, dropIndexNode->ddlXns(), dropFromMD,
                        dropFromStorage, naTable->isMonarch(), tableNamespace)) {
    processReturn();
    deallocEHI(ehi);

    return;
  }

  ActiveSchemaDB()->getNATableDB()->useCache();

  // save the current parserflags setting
  int savedParserFlags = Get_SqlParser_Flags(0xFFFFFFFF);
  Set_SqlParser_Flags(ALLOW_VOLATILE_SCHEMA_IN_TABLE_NAME);

  // Save parser flags settings so they can be restored later
  Set_SqlParser_Flags(savedParserFlags);
  if (naTable->incrBackupEnabled()) {
    if (updateSeabaseXDCDDLTable(&cliInterface, btCatName.data(), btSchName.data(), btObjName.data(),
                                 COM_BASE_TABLE_OBJECT_LIT)) {
      processReturn();
      deallocEHI(ehi);
      return;
    }
  }

  if (updateObjectRedefTime(&cliInterface, btCatName, btSchName, btObjName, COM_BASE_TABLE_OBJECT_LIT, -1, btUID,
                            naTable->isStoredDesc(), naTable->storedStatsAvail())) {
    processReturn();
    deallocEHI(ehi);

    return;
  }

  // Add to shared cache list
  if (naTable->isStoredDesc()) {
    insertIntoSharedCacheList(btCatName, btSchName, btObjName, COM_BASE_TABLE_OBJECT, SharedCacheDDLInfo::DDL_UPDATE);
    insertIntoSharedCacheList(catalogNamePart, schemaNamePart, objectNamePart, COM_INDEX_OBJECT,
                              SharedCacheDDLInfo::DDL_DELETE);
  }

  // remove NATable for the base table of this index
  ActiveSchemaDB()->getNATableDB()->removeNATable(cn, ComQiScope::REMOVE_FROM_ALL_USERS, COM_BASE_TABLE_OBJECT,
                                                  dropIndexNode->ddlXns(), FALSE);

  // remove NATable for this index in its real form as well as in its index_table
  // standalone format
  CorrName cni(objectNamePart, STMTHEAP, schemaNamePart, catalogNamePart);
  ActiveSchemaDB()->getNATableDB()->removeNATable(cni, ComQiScope::REMOVE_FROM_ALL_USERS, COM_INDEX_OBJECT,
                                                  dropIndexNode->ddlXns(), FALSE, indexUID);
  cni.setSpecialType(ExtendedQualName::INDEX_TABLE);
  ActiveSchemaDB()->getNATableDB()->removeNATable(cni, ComQiScope::REMOVE_MINE_ONLY, COM_INDEX_OBJECT,
                                                  dropIndexNode->ddlXns(), FALSE);

  /*remove natable for every partition if global index*/
  if (naTable->isPartitionV2Table() && CmpSeabaseDDL::isMDflagsSet(indexFlags, MD_IS_GLOBAL_PARTITION_FLG)) {
    const NAPartitionArray &partitionArray = naTable->getPartitionArray();
    for (int i = 0; i < partitionArray.entries(); i++) {
      if (partitionArray[i]->hasSubPartition()) {
        const NAPartitionArray *subpArray = partitionArray[i]->getSubPartitions();
        for (int j = 0; j < subpArray->entries(); j++) {
          CorrName pcn((*subpArray)[j]->getPartitionEntityName(), STMTHEAP, schemaNamePart, catalogNamePart);
          ActiveSchemaDB()->getNATableDB()->removeNATable(pcn, ComQiScope::REMOVE_FROM_ALL_USERS, COM_BASE_TABLE_OBJECT,
                                                          dropIndexNode->ddlXns(), FALSE);
        }
      } else {
        CorrName pcn(partitionArray[i]->getPartitionEntityName(), STMTHEAP, schemaNamePart, catalogNamePart);
        ActiveSchemaDB()->getNATableDB()->removeNATable(pcn, ComQiScope::REMOVE_FROM_ALL_USERS, COM_BASE_TABLE_OBJECT,
                                                        dropIndexNode->ddlXns(), FALSE);
      }
    }
  }
  //  processReturn();

  deallocEHI(ehi);

  return;
}

void CmpSeabaseDDL::alterSeabaseTableDisableOrEnableIndex(ExprNode *ddlNode, NAString &currCatName,
                                                          NAString &currSchName) {
  int retcode = 0;

  if (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL) &&
      (CmpCommon::getDefault(TRAF_ALLOW_DISABLE_ENABLE_INDEXES) == DF_OFF)) {
    *CmpCommon::diags() << DgSqlCode(-CAT_UNSUPPORTED_COMMAND_ERROR);
    processReturn();
    return;
  }

  const StmtDDLAlterTableDisableIndex *alterDisableIndexNode =
      ddlNode->castToStmtDDLNode()->castToStmtDDLAlterTableDisableIndex();

  const StmtDDLAlterTableEnableIndex *alterEnableIndexNode =
      ddlNode->castToStmtDDLNode()->castToStmtDDLAlterTableEnableIndex();

  NABoolean isDisable = (ddlNode->getOperatorType() == DDL_ALTER_TABLE_DISABLE_INDEX);

  const QualifiedName &entityName = (isDisable ? alterDisableIndexNode->getTableNameAsQualifiedName()
                                               : alterEnableIndexNode->getTableNameAsQualifiedName());

  const NAString &btCatName = entityName.getCatalogName();
  const NAString &btSchName = entityName.getSchemaName();
  const NAString &btObjName = entityName.getObjectName();
  NAString extBTname = btCatName + "." + btSchName + "." + btObjName;

  const NAString &idxName = (isDisable ? alterDisableIndexNode->getIndexName() : alterEnableIndexNode->getIndexName());

  ComObjectName indexName(idxName);
  ComAnsiNamePart currCatAnsiName(NOT btCatName.isNull() ? btCatName : currCatName);
  ComAnsiNamePart currSchAnsiName(NOT btSchName.isNull() ? btSchName : currSchName);
  indexName.applyDefaults(currCatAnsiName, currSchAnsiName);

  const NAString catalogNamePart = indexName.getCatalogNamePartAsAnsiString();
  const NAString schemaNamePart = indexName.getSchemaNamePartAsAnsiString(TRUE);
  const NAString objectNamePart = indexName.getObjectNamePartAsAnsiString(TRUE);
  const NAString extIndexName = indexName.getExternalName(TRUE);

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());

  retcode = lockObjectDDL((StmtDDLAlterTable *)ddlNode);
  if (retcode < 0) {
    processReturn();
    return;
  }

  retcode =
      existsInSeabaseMDTable(&cliInterface, btCatName, btSchName, btObjName, COM_BASE_TABLE_OBJECT, FALSE, TRUE, TRUE);
  if (retcode < 0) {
    processReturn();

    return;
  }

  if (retcode == 0)  // does not exist
  {
    *CmpCommon::diags() << DgSqlCode(-CAT_OBJECT_DOES_NOT_EXIST_IN_TRAFODION) << DgString0(extBTname);

    processReturn();

    return;
  }

  BindWA bindWA(ActiveSchemaDB(), CmpCommon::context(), TRUE /*inDDL*/);
  CorrName cn(btObjName, STMTHEAP, btSchName, btCatName);
  NATable *naTable = bindWA.getNATable(cn);
  if (naTable == NULL || bindWA.errStatus()) {
    *CmpCommon::diags() << DgSqlCode(-4082) << DgTableName(extIndexName);

    processReturn();
    return;
  }

  if (naTable->readOnlyEnabled() && !Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)) {
    *CmpCommon::diags() << DgSqlCode(-8644);
    processReturn();
    return;
  }

  retcode = existsInSeabaseMDTable(&cliInterface, catalogNamePart, schemaNamePart, objectNamePart, COM_INDEX_OBJECT,
                                   FALSE, TRUE, TRUE);
  if (retcode < 0) {
    processReturn();

    return;
  }

  if (retcode == 0)  // does not exist
  {
    *CmpCommon::diags() << DgSqlCode(-CAT_OBJECT_DOES_NOT_EXIST_IN_TRAFODION) << DgString0(extIndexName);

    processReturn();

    return;
  }

  NABoolean isProcessIndexOnPartition = FALSE;
  isProcessIndexOnPartition = (isDisable ? alterDisableIndexNode->isProcessIndexOnPartition()
                                         : alterEnableIndexNode->isProcessIndexOnPartition());

  // check index is match the table
  if (!isProcessIndexOnPartition) {
    NAString query;
    query.format(
        " select object_uid from  %s.\"%s\".%s where     "
        " object_uid = (                                 "
        "   select i.BASE_TABLE_UID                      "
        "    from   %s.\"%s\".%s i join  %s.\"%s\".%s o  "
        "    on i.index_uid = o.object_uid               "
        "    where o.catalog_name = '%s' and o.schema_name = '%s' and o.Object_Name = '%s' and object_type='IX'"
        " ) and                                          "
        " catalog_name = '%s' and schema_name = '%s' and Object_Name = '%s'",
        getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS, getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_INDEXES,
        getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS, catalogNamePart.data(), schemaNamePart.data(),
        objectNamePart.data(), btCatName.data(), btSchName.data(), btObjName.data());

    Queue *indexes = NULL;
    retcode = cliInterface.fetchAllRows(indexes, query.data(), 0, FALSE, FALSE, TRUE);
    if (retcode < 0) {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      return;
    }

    if (indexes->numEntries() == 0) {
      *CmpCommon::diags() << DgSqlCode(-3242) << DgString0("index is mismatch of table");
      processReturn();
      return;
    }
  }

  long btUID = 0;
  int btObjOwner = 0;
  int btSchemaOwner = 0;
  long btObjectFlags = 0;
  long objDataUID = 0;
  if ((btUID = getObjectInfo(&cliInterface, btCatName, btSchName, btObjName, COM_BASE_TABLE_OBJECT, btObjOwner,
                             btSchemaOwner, btObjectFlags, btUID, objDataUID)) < 0) {
    processReturn();

    return;
  }

  // Verify that current user has authority to drop the index
  if ((!isDDLOperationAuthorized(SQLOperation::DROP_INDEX, btObjOwner, btSchemaOwner, btUID, COM_BASE_TABLE_OBJECT,
                                 NULL)) &&
      (!isDDLOperationAuthorized(SQLOperation::ALTER_TABLE, btObjOwner, btSchemaOwner, btUID, COM_BASE_TABLE_OBJECT,
                                 NULL))) {
    *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);

    processReturn();

    return;
  }

  NABoolean isPartitionTable = FALSE;

  if (naTable->isPartitionV2Table()) isPartitionTable = TRUE;

  if (isProcessIndexOnPartition && !isPartitionTable) {
    *CmpCommon::diags() << DgSqlCode(-3242) << DgString0("POPULATE PARTITION INDEX ON A REGULAR TABLE");
    processReturn();
    return;
  }

  if (isProcessIndexOnPartition) {
    const NAPartitionArray &partArray = naTable->getPartitionArray();
    NAString alterPartitionName =
        (isDisable ? alterDisableIndexNode->getPartitionName() : alterEnableIndexNode->getPartitionName());
    NABoolean isMatched = FALSE;
    int recode = -1;
    for (int i = 0; i < partArray.entries(); i++) {
      NAPartition *partition = partArray[i];
      const char *partitionName = partition->getPartitionName();

      if (strcmp(alterPartitionName.data(), partitionName) == 0) {
        isMatched = TRUE;
        recode = i;
      }
    }
    if (!isMatched) {
      // The specified partition partName does not exist.
      *CmpCommon::diags() << DgSqlCode(-1097) << DgString0(alterPartitionName.data());
      processReturn();
      return;
    }

    if (alterSeabaseTableDisableOrEnableIndex(btCatName.data(), btSchName.data(), objectNamePart.data(),
                                              partArray[recode]->getPartitionEntityName(), isDisable)) {
      processReturn();
      return;
    }
  } else {
    if (!isDisable && (CmpCommon::getDefault(TRAF_ALLOW_POPULATE_ON_ENABLE_INDEXES) == DF_ON)) {
      char buf[4000];
      sprintf(buf, " POPULATE INDEX \"%s\" ON \"%s\".\"%s\".\"%s\";", objectNamePart.data(), btCatName.data(),
              btSchName.data(), btObjName.data());
      retcode = cliInterface.executeImmediate(buf);
      if (retcode < 0) {
        cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
        processReturn();
        return;
      }
    }

    // update flag for base table
    if (updateObjectValidDef(&cliInterface, catalogNamePart, schemaNamePart, objectNamePart, COM_INDEX_OBJECT_LIT,
                             (isDisable ? "N" : "Y"))) {
      processReturn();
      return;
    }

    if (isPartitionTable) {
      // update flags for every partition table
      const NAPartitionArray &partArray = naTable->getPartitionArray();
      for (int i = 0; i < naTable->FisrtLevelPartitionCount(); i++) {
        char partitionIndexName[256];
        getPartitionIndexName(partitionIndexName, 256, objectNamePart, partArray[i]->getPartitionEntityName());
        if (alterSeabaseTableDisableOrEnableIndex(btCatName.data(), btSchName.data(), partitionIndexName,
                                                  partArray[i]->getPartitionEntityName(), isDisable)) {
          processReturn();
          return;
        }
      }
    }
  }

  // Update the epoch value to lock out other user access
  if (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)) {
    if (ddlSetObjectEpoch(naTable, STMTHEAP) < 0) {
      processReturn();
      return;
    }
  }

  if (naTable->incrBackupEnabled()) {
    retcode = updateSeabaseXDCDDLTable(&cliInterface, btCatName.data(), btSchName.data(), btObjName.data(),
                                       COM_BASE_TABLE_OBJECT_LIT);
    if (retcode < 0) {
      processReturn();
      return;
    }
  }

  if (updateObjectRedefTime(&cliInterface, btCatName, btSchName, btObjName, COM_BASE_TABLE_OBJECT_LIT, -1, btUID,
                            naTable->isStoredDesc(), naTable->storedStatsAvail())) {
    processReturn();

    return;
  }

  // Updated shared cache to reflect the change
  if (naTable->isStoredDesc()) {
    if (isDisable) {
      insertIntoSharedCacheList(catalogNamePart, schemaNamePart, objectNamePart, COM_INDEX_OBJECT,
                                SharedCacheDDLInfo::DDL_DISABLE);
    } else {
      insertIntoSharedCacheList(catalogNamePart, schemaNamePart, objectNamePart, COM_INDEX_OBJECT,
                                SharedCacheDDLInfo::DDL_ENABLE);
    }

    insertIntoSharedCacheList(btCatName, btSchName, btObjName, COM_BASE_TABLE_OBJECT, SharedCacheDDLInfo::DDL_UPDATE);
  }

  // remove NATable for the base table of this index
  ActiveSchemaDB()->getNATableDB()->removeNATable(cn, ComQiScope::REMOVE_FROM_ALL_USERS, COM_BASE_TABLE_OBJECT,
                                                  ddlNode->castToStmtDDLNode()->ddlXns(), FALSE);
  // Also, remove index.
  CorrName cni(objectNamePart, STMTHEAP, schemaNamePart, catalogNamePart);
  ActiveSchemaDB()->getNATableDB()->removeNATable(cni, ComQiScope::REMOVE_FROM_ALL_USERS, COM_INDEX_OBJECT,
                                                  ddlNode->castToStmtDDLNode()->ddlXns(), FALSE);

  //  processReturn();

  return;
}

short CmpSeabaseDDL::alterSeabaseTableDisableOrEnableIndex(const char *catName, const char *schName,
                                                           const char *idxName, const char *tabName,
                                                           NABoolean isDisable) {
  char buf[4000];
  int cliRC = 0;

  if (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL) &&
      (CmpCommon::getDefault(TRAF_ALLOW_DISABLE_ENABLE_INDEXES) == DF_OFF)) {
    *CmpCommon::diags() << DgSqlCode(-CAT_UNSUPPORTED_COMMAND_ERROR);
    processReturn();
    return -1;
  }

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());

  sprintf(buf, " ALTER TABLE \"%s\".\"%s\".\"%s\"  %s INDEX \"%s\" ;", catName, schName, tabName,
          isDisable ? "DISABLE" : "ENABLE", idxName);

  cliRC = cliInterface.executeImmediate(buf);
  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  return 0;
}
void CmpSeabaseDDL::alterSeabaseTableDisableOrEnableAllIndexes(ExprNode *ddlNode, NAString &currCatName,
                                                               NAString &currSchName, NAString &tabName,
                                                               NABoolean allUniquesOnly) {
  int cliRC = 0;
  char buf[4000];

  if (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL) &&
      (CmpCommon::getDefault(TRAF_ALLOW_DISABLE_ENABLE_INDEXES) == DF_OFF)) {
    *CmpCommon::diags() << DgSqlCode(-CAT_UNSUPPORTED_COMMAND_ERROR);
    processReturn();
    return;
  }

  NABoolean isDisable = (ddlNode->getOperatorType() == DDL_ALTER_TABLE_DISABLE_INDEX);

  ComObjectName tableName(tabName);

  const NAString catalogNamePart = tableName.getCatalogNamePartAsAnsiString();
  const NAString schemaNamePart = tableName.getSchemaNamePartAsAnsiString(TRUE);
  const NAString objectNamePart = tableName.getObjectNamePartAsAnsiString(TRUE);

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());

  int retcode = lockObjectDDL(tableName, COM_BASE_TABLE_OBJECT, false /* isVolatile */, false /* isExternal */);
  if (retcode < 0) {
    processReturn();
    return;
  }

  // Fix for launchpad bug 1381621
  retcode = existsInSeabaseMDTable(&cliInterface, catalogNamePart, schemaNamePart, objectNamePart,
                                   COM_BASE_TABLE_OBJECT, TRUE, TRUE, TRUE);
  if (retcode < 0) {
    processReturn();
    return;
  }

  if (retcode == 0)  // does not exist
  {
    *CmpCommon::diags() << DgSqlCode(-CAT_OBJECT_DOES_NOT_EXIST_IN_TRAFODION)
                        << DgString0(tableName.getExternalName(TRUE));
    processReturn();
    return;
  }

  str_sprintf(buf,
              " select catalog_name,schema_name,object_name from  %s.\"%s\".%s  "
              " where object_uid in ( select i.index_uid from "
              " %s.\"%s\".%s i "
              " join    %s.\"%s\".%s  o2 on i.base_table_uid=o2.object_uid "
              " where  o2.catalog_name= '%s' AND o2.schema_name='%s' AND o2.Object_Name='%s' "
              " %s "
              " and object_type='IX' ; ",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS, getSystemCatalog(), SEABASE_MD_SCHEMA,
              SEABASE_INDEXES, getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS, catalogNamePart.data(),
              schemaNamePart.data(), objectNamePart.data(),  // table name in this case
              allUniquesOnly ? " AND is_unique = 1 )" : ")");

  Queue *indexes = NULL;
  cliRC = cliInterface.fetchAllRows(indexes, buf, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    return;
  }

  CorrName cn(objectNamePart, STMTHEAP, schemaNamePart, catalogNamePart);

  BindWA bindWA(ActiveSchemaDB(), CmpCommon::context(), TRUE /*inDDL*/);
  NATable *naTable = bindWA.getNATable(cn);

  NABoolean isPartitionTable = FALSE;
  if (naTable->isPartitionV2Table()) isPartitionTable = TRUE;

  if (isPartitionTable) {
    const NAPartitionArray &partArray = naTable->getPartitionArray();
    for (int i = 0; i < partArray.entries(); i++) {
      NAString queryBuf;
      queryBuf.format("ALTER TABLE %s.\"%s\".%s %s ALL %s INDEXES;", catalogNamePart.data(), schemaNamePart.data(),
                      partArray[i]->getPartitionEntityName(), (isDisable ? "DISABLE" : "ENABLE"),
                      (allUniquesOnly ? "Unique" : ""));

      cliRC = cliInterface.executeImmediate(queryBuf.data());
      if (cliRC < 0) {
        cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
        return;
      }
    }
  }

  NABoolean needRestoreIBAttr = naTable->incrBackupEnabled();
  if (needRestoreIBAttr == true) {
    naTable->setIncrBackupEnabled(FALSE);
    dropIBAttrForTable(naTable, &cliInterface);
  }

  if (indexes) {
    // table has no index -- return
    if (indexes->numEntries() == 0) {
      // restore the flag before using it
      if (needRestoreIBAttr == true) {
        naTable->setIncrBackupEnabled(TRUE);
        addIBAttrForTable(naTable, &cliInterface);
        needRestoreIBAttr = false;
      }
      return;
    }

    char *catName = NULL;
    char *schName = NULL;
    indexes->position();
    for (int ii = 0; ii < indexes->numEntries(); ii++) {
      OutputInfo *idx = (OutputInfo *)indexes->getNext();

      catName = idx->get(0);
      schName = idx->get(1);
      char *idxName = idx->get(2);

      if (alterSeabaseTableDisableOrEnableIndex(catName, schName, idxName, objectNamePart, isDisable)) {
        // restore the flag before using it
        if (needRestoreIBAttr == true) {
          naTable->setIncrBackupEnabled(TRUE);
          addIBAttrForTable(naTable, &cliInterface);
          needRestoreIBAttr = false;
        }
        return;
      }
      if (naTable->isStoredDesc()) {
        if (isDisable) {
          insertIntoSharedCacheList(catName, schName, idxName, COM_INDEX_OBJECT, SharedCacheDDLInfo::DDL_DISABLE);
        } else {
          insertIntoSharedCacheList(catName, schName, idxName, COM_INDEX_OBJECT, SharedCacheDDLInfo::DDL_ENABLE);
        }
      }
    }

    // restore the flag before using it
    if (needRestoreIBAttr == true) {
      naTable->setIncrBackupEnabled(TRUE);
      addIBAttrForTable(naTable, &cliInterface);
      needRestoreIBAttr = false;
    }

    if (naTable->incrBackupEnabled()) {
      cliRC = updateSeabaseXDCDDLTable(&cliInterface, catalogNamePart.data(), schemaNamePart.data(),
                                       objectNamePart.data(), COM_BASE_TABLE_OBJECT_LIT);
      if (cliRC < 0) {
        processReturn();
        return;
      }
    }

    // Updated shared cache to reflect the change
    if (naTable->isStoredDesc()) {
      insertIntoSharedCacheList(catalogNamePart, schemaNamePart, objectNamePart, COM_BASE_TABLE_OBJECT,
                                SharedCacheDDLInfo::DDL_UPDATE);
    }

    CorrName cn(objectNamePart, STMTHEAP, NAString(schName), NAString(catName));
    ActiveSchemaDB()->getNATableDB()->removeNATable(cn, ComQiScope::REMOVE_FROM_ALL_USERS, COM_BASE_TABLE_OBJECT,
                                                    ddlNode->castToStmtDDLNode()->ddlXns(), FALSE);
  }

  return;
}

short CmpSeabaseDDL::alterSeabaseIndexStoredDesc(ExeCliInterface &cliInterface, const NAString &btCatName,
                                                 const NAString &btSchName, const NAString &btObjName,
                                                 const NABoolean &enable) {
  int cliRC = 0;
  char buf[4000];

  str_sprintf(buf,
              "select o.catalog_name, o.schema_name, o.object_name, index_uid "
              "from %s.\"%s\".%s i, %s.\"%s\".%s o "
              "where base_table_uid = "
              " (select object_uid from %s.\"%s\".%s "
              "  where catalog_name = '%s' and schema_name = '%s' "
              "    and object_name = '%s' and object_type = 'BT'"
              " ) "
              "and index_uid = o.object_uid and keytag > 0",
              getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_INDEXES, getSystemCatalog(), SEABASE_MD_SCHEMA,
              SEABASE_OBJECTS, getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS, btCatName.data(),
              btSchName.data(), btObjName.data());

  Queue *indexes = NULL;
  cliRC = cliInterface.fetchAllRows(indexes, buf, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  if (indexes) {
    // table has no index -- return
    if (indexes->numEntries() == 0) return 0;

    indexes->position();
    for (int i = 0; i < indexes->numEntries(); i++) {
      OutputInfo *idx = (OutputInfo *)indexes->getNext();
      cliRC = updateObjectRedefTime(&cliInterface,
                                    idx->get(0),  // catName
                                    idx->get(1),  // schName
                                    idx->get(2),  // ndxName
                                    COM_INDEX_OBJECT_LIT, -1, *(long *)idx->get(3), enable, FALSE);
      if (cliRC < 0) return -1;
    }
  }
  return 0;
}

void CmpSeabaseDDL::alterSeabaseIndexHBaseOptions(StmtDDLAlterIndexHBaseOptions *hbaseOptionsNode,
                                                  NAString &currCatName, NAString &currSchName) {
  int retcode = 0;
  int cliRC = 0;

  NAString idxName = hbaseOptionsNode->getIndexName();

  ComObjectName indexName(idxName);
  ComAnsiNamePart currCatAnsiName(currCatName);
  ComAnsiNamePart currSchAnsiName(currSchName);
  indexName.applyDefaults(currCatAnsiName, currSchAnsiName);

  NAString catalogNamePart = indexName.getCatalogNamePartAsAnsiString();
  NAString schemaNamePart = indexName.getSchemaNamePartAsAnsiString(TRUE);
  NAString objectNamePart = indexName.getObjectNamePartAsAnsiString(TRUE);
  const NAString extIndexName = indexName.getExternalName(TRUE);

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());

  // Disallow this ALTER on system metadata schema objects

  if ((isSeabaseReservedSchema(indexName)) && (!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL))) {
    *CmpCommon::diags() << DgSqlCode(-CAT_ALTER_NOT_ALLOWED_IN_SMD) << DgTableName(extIndexName);

    processReturn();
    return;
  }

  // Make sure this object exists

  retcode = existsInSeabaseMDTable(&cliInterface, catalogNamePart, schemaNamePart, objectNamePart, COM_INDEX_OBJECT,
                                   (Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL) ? FALSE : TRUE), TRUE, TRUE);
  if (retcode < 0)  // some error occurred
  {
    processReturn();

    return;
  } else if (retcode == 0) {
    CmpCommon::diags()->clear();

    *CmpCommon::diags() << DgSqlCode(-CAT_OBJECT_DOES_NOT_EXIST_IN_TRAFODION) << DgString0(extIndexName);

    processReturn();
    return;
  }

  BindWA bindWA(ActiveSchemaDB(), CmpCommon::context(), TRUE /*inDDL*/);

  // Get base table name and base table uid

  NAString btCatName;
  NAString btSchName;
  NAString btObjName;
  long btUID;
  int btObjOwner = 0;
  int btSchemaOwner = 0;
  if (getBaseTable(&cliInterface, catalogNamePart, schemaNamePart, objectNamePart, btCatName, btSchName, btObjName,
                   btUID, btObjOwner, btSchemaOwner)) {
    processReturn();

    return;
  }

  CorrName cn(btObjName, STMTHEAP, btSchName, btCatName);

  NATable *naTable = bindWA.getNATable(cn);
  if (naTable == NULL || bindWA.errStatus()) {
    // shouldn't happen, actually, since getBaseTable above succeeded

    CmpCommon::diags()->clear();
    *CmpCommon::diags() << DgSqlCode(-CAT_OBJECT_DOES_NOT_EXIST_IN_TRAFODION) << DgString0(extIndexName);

    processReturn();
    return;
  }

  ExpHbaseInterface *ehi = allocEHI(naTable->storageType());
  if (ehi == NULL) {
    processReturn();
    return;
  }

  // Make sure user has the privilege to perform the ALTER

  if (!isDDLOperationAuthorized(SQLOperation::ALTER_TABLE, btObjOwner, btSchemaOwner, btUID, COM_BASE_TABLE_OBJECT,
                                naTable->getPrivInfo())) {
    *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);
    deallocEHI(ehi);
    processReturn();
    return;
  }

  CmpCommon::diags()->clear();

  // Get the object UID so we can update the metadata

  long objDataUID;
  long objUID = getObjectUID(&cliInterface, catalogNamePart.data(), schemaNamePart.data(), objectNamePart.data(),
                              COM_INDEX_OBJECT_LIT, &objDataUID);
  if (objUID < 0) {
    deallocEHI(ehi);
    processReturn();
    return;
  }

  // update HBase options in the metadata

  ElemDDLHbaseOptions *edhbo = hbaseOptionsNode->getHBaseOptions();
  short result = updateHbaseOptionsInMetadata(&cliInterface, objUID, edhbo);

  if (result < 0) {
    deallocEHI(ehi);
    processReturn();
    return;
  }

  // tell HBase to change the options

  NAString extNameForHbase =
      genHBaseObjName(catalogNamePart, schemaNamePart, objectNamePart, naTable->getNamespace(), objDataUID);

  NAList<NAString> nal(STMTHEAP);
  nal.insert(naTable->defaultUserColFam());
  HbaseStr hbaseTable;
  hbaseTable.val = (char *)extNameForHbase.data();
  hbaseTable.len = extNameForHbase.length();
  result = alterHbaseTable(ehi, &hbaseTable, nal, &(edhbo->getHbaseOptions()), hbaseOptionsNode->ddlXns());
  if (result < 0) {
    deallocEHI(ehi);
    processReturn();
    return;
  }

  if (naTable->incrBackupEnabled()) {
    if (updateSeabaseXDCDDLTable(&cliInterface, catalogNamePart.data(), schemaNamePart.data(), objectNamePart.data(),
                                 COM_INDEX_OBJECT_LIT)) {
      deallocEHI(ehi);
      processReturn();
      return;
    }
  }

  if (updateObjectRedefTime(&cliInterface, btCatName, btSchName, btObjName, COM_BASE_TABLE_OBJECT_LIT, -1, btUID)) {
    deallocEHI(ehi);
    processReturn();

    return;
  }

  // Assume that changing hbase options does not affect the parent table
  // stored desc.

  // Update index desc
  if (updateObjectRedefTime(&cliInterface, catalogNamePart, schemaNamePart, objectNamePart, COM_INDEX_OBJECT_LIT, -1,
                            objUID, naTable->isStoredDesc(), naTable->storedStatsAvail())) {
    deallocEHI(ehi);
    processReturn();

    return;
  }

  if (naTable->isStoredDesc()) {
    insertIntoSharedCacheList(btCatName, btSchName, btObjName, COM_BASE_TABLE_OBJECT, SharedCacheDDLInfo::DDL_UPDATE);

    insertIntoSharedCacheList(catalogNamePart, schemaNamePart, objectNamePart, COM_INDEX_OBJECT,
                              SharedCacheDDLInfo::DDL_UPDATE);

    long transid;
    CliGlobals *cliGlobals = GetCliGlobals();
    if (cliGlobals->currContext()->getTransaction()->getCurrentXnId(&transid) != 0) finalizeSharedCache();
  }

  // invalidate cached NATable info on this table for all users

  ActiveSchemaDB()->getNATableDB()->removeNATable(cn, ComQiScope::REMOVE_FROM_ALL_USERS, COM_BASE_TABLE_OBJECT,
                                                  hbaseOptionsNode->ddlXns(), FALSE);

  deallocEHI(ehi);

  return;
}

// -----------------------------------------------------------------------------
// Method:  alterSeabaseIndexAttribute
//
// This method update all the indexes associated with its parent table defined
// in naTable to set/reset the requested attribute.
//
// In:  the fileAttrs to update
//      description of the parent table
// -----------------------------------------------------------------------------
int CmpSeabaseDDL::alterSeabaseIndexAttributes(ExeCliInterface *cliInterface, ParDDLFileAttrsAlterTable &fileAttrs,
                                                 NATable *naTable, int alterReadonlyOP) {
  char queryBuf[1000];
  int cliRC = 0;
  if (naTable->hasSecondaryIndexes()) {
    const NAFileSetList &naFsList = naTable->getIndexList();
    for (int i = 0; i < naFsList.entries(); i++) {
      const NAFileSet *naFS = naFsList[i];

      // skip clustering index
      if (naFS->getKeytag() == 0) continue;

      const QualifiedName &indexName = naFS->getFileSetName();
      long indexUID = naFS->getIndexUID();

      // Either enable or disable incremental backup
      if (fileAttrs.isIncrBackupSpecified() || fileAttrs.isReadOnlySpecified()) {
        // get current table flags so bit adjustment can be performed
        str_sprintf(queryBuf, "select flags from %s.\"%s\".%s where object_uid = %ld", getSystemCatalog(),
                    SEABASE_MD_SCHEMA, SEABASE_OBJECTS, indexUID);
        long flags = 0;
        int flagsLen = sizeof(long);
        long rowsAffected = 0;
        cliRC = cliInterface->executeImmediateCEFC(queryBuf, NULL, 0, (char *)&flags, &flagsLen, &rowsAffected);
        if (cliRC < 0) {
          cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
          return -1;
        }

        if (cliRC == 100)  // did not find the row
        {
          *CmpCommon::diags() << DgSqlCode(-1389) << DgString0(indexName.getQualifiedNameAsAnsiString());
          return -1;
        }

        // update flags with new options in OBJECTS metadata.
        long newFlags = flags;

        if (fileAttrs.isIncrBackupSpecified()) {
          if (fileAttrs.incrBackupEnabled())
            CmpSeabaseDDL::setMDflags(newFlags, MD_OBJECTS_INCR_BACKUP_ENABLED);
          else
            CmpSeabaseDDL::resetMDflags(newFlags, MD_OBJECTS_INCR_BACKUP_ENABLED);
        }

        if (fileAttrs.isReadOnlySpecified()) {
          if (fileAttrs.readOnlyEnabled())
            CmpSeabaseDDL::setMDflags(newFlags, MD_OBJECTS_READ_ONLY_ENABLED);
          else
            CmpSeabaseDDL::resetMDflags(newFlags, MD_OBJECTS_READ_ONLY_ENABLED);
        }

        str_sprintf(queryBuf, "update %s.\"%s\".%s set flags = %ld where object_uid = %ld ", getSystemCatalog(),
                    SEABASE_MD_SCHEMA, SEABASE_OBJECTS, newFlags, indexUID);

        cliRC = cliInterface->executeImmediate(queryBuf);
        if (cliRC < 0) {
          cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
          return -1;
        }

        if (alterReadonlyOP != 0)
          insertIntoSharedCacheList(
              indexName.getCatalogName(), indexName.getSchemaName(), indexName.getObjectName(), COM_INDEX_OBJECT,
              (alterReadonlyOP == 1 ? SharedCacheDDLInfo::DDL_UPDATE : SharedCacheDDLInfo::DDL_DELETE),
              SharedCacheDDLInfo::SHARED_DATA_CACHE);

      }  // incr backup specified

      // Add other attributes that may need to be updated when the parent
      // changes

    }  // processing secondary indexes
  }    // has secondary indexes
  return 0;
}
