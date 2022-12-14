/* -*-C++-*- */

/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLAlter.C
 * Description:  contains definitions of non-inline methods for classes
 *               representing DDL Alter Statements
 *
 *               Also contains definitions of non-inline methods for classes
 *               relating to check constraint usages
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

#include "common/BaseTypes.h"
#include "common/ComASSERT.h"
#include "common/ComOperators.h"
#include "parser/AllElemDDLFileAttr.h"
#include "parser/AllStmtDDLAlter.h"  // MV - RG
#include "parser/AllStmtDDLAlterTable.h"
#include "parser/ElemDDLConstraint.h"
#include "parser/ElemDDLConstraintCheck.h"
#include "parser/ElemDDLConstraintPK.h"
#include "parser/ElemDDLFileAttrClause.h"
#include "parser/ElemDDLHbaseOptions.h"
#include "parser/ElemDDLLibClientFilename.h"
#include "parser/ElemDDLLibClientName.h"
#include "parser/ElemDDLQualName.h"  // MV - RG
#include "parser/StmtDDLAlterAuditConfig.h"
#include "parser/StmtDDLAlterCatalog.h"
#include "parser/StmtDDLAlterIndexAttribute.h"
#include "parser/StmtDDLAlterIndexHBaseOptions.h"
#include "parser/StmtDDLAlterLibrary.h"
#include "parser/StmtDDLAlterSchema.h"
#include "parser/StmtDDLAlterSharedCache.h"
#include "parser/StmtDDLAlterSynonym.h"
#include "parser/StmtDDLAlterTableAlterColumn.h"
#include "parser/StmtDDLAlterTableDisableIndex.h"
#include "parser/StmtDDLAlterTableEnableIndex.h"
#include "parser/StmtDDLAlterTableSplitPartition.h"
#include "parser/StmtDDLAlterTableToggleConstraint.h"
#include "parser/StmtDDLAlterTableTruncatePartition.h"
#include "parser/StmtDDLAlterTrigger.h"
#include "parser/StmtDDLAlterView.h"

// -----------------------------------------------------------------------
// definitions of non-inline methods for class ParCheckConstraintColUsage
// -----------------------------------------------------------------------

//
// default constructor
//

ParCheckConstraintColUsage::ParCheckConstraintColUsage(CollHeap *h)
    : isInSelectList_(FALSE), tableName_(h), columnName_(h) {}

//
// initialize constructor
//

ParCheckConstraintColUsage::ParCheckConstraintColUsage(const ColRefName &colName, const NABoolean isInSelectList,
                                                       CollHeap *h)
    : tableName_(colName.getCorrNameObj().getQualifiedNameObj(), h),
      columnName_(colName.getColName(), h),
      isInSelectList_(isInSelectList) {}

//
// virtual destructor
//

ParCheckConstraintColUsage::~ParCheckConstraintColUsage() {}

//
// operator
//

NABoolean ParCheckConstraintColUsage::operator==(const ParCheckConstraintColUsage &rhs) const {
  if (this EQU & rhs) {
    return TRUE;
  }
  return (getColumnName() EQU rhs.getColumnName() AND getTableQualName()
              EQU rhs.getTableQualName() AND isInSelectList() EQU rhs.isInSelectList());
}

// -----------------------------------------------------------------------
// definitions of non-inline methods for class ParCheckConstraintColUsageList
// -----------------------------------------------------------------------

//
// constructor
//

ParCheckConstraintColUsageList::ParCheckConstraintColUsageList(const ParCheckConstraintColUsageList &other,
                                                               CollHeap *heap)
    : LIST(ParCheckConstraintColUsage *)(other, heap), heap_(heap) {}

//
// virtual destructor
//

ParCheckConstraintColUsageList::~ParCheckConstraintColUsageList() {
  for (CollIndex i = 0; i < entries(); i++) {
    // KSKSKS
    delete &operator[](i);
    //    NADELETE(&operator[](i), ParCheckConstraintColUsage, heap_);
    // KSKSKS
  }
}

//
// operator
//

ParCheckConstraintColUsageList &ParCheckConstraintColUsageList::operator=(const ParCheckConstraintColUsageList &rhs) {
  if (this EQU & rhs) return *this;
  clear();
  copy(rhs);
  return *this;
}

//
// accessor
//

ParCheckConstraintColUsage *const ParCheckConstraintColUsageList::find(const ColRefName &colName) {
  for (CollIndex i = 0; i < entries(); i++) {
    if (operator[](i)
            .getColumnName() EQU colName.getColName() AND
            operator[](i)
            .getTableQualName() EQU colName.getCorrNameObj()
            .getQualifiedNameObj()) {
      return &operator[](i);
    }
  }
  return NULL;
}

//
// mutators
//

void ParCheckConstraintColUsageList::clear() {
  for (CollIndex i = 0; i < entries(); i++) {
    // KSKSKS
    delete &operator[](i);
    //    NADELETE(&operator[](i), ParCheckConstraintColUsage, heap_);
    // KSKSKS
  }
  LIST(ParCheckConstraintColUsage *)::clear();
  // leave data member heap_ alone (it's only set once, by the constructor).
}

void ParCheckConstraintColUsageList::copy(const ParCheckConstraintColUsageList &rhs) {
  // DO NOT set the heap_ field.
  // It's already been set by the constructor
  // heap_ = rhs.heap_;

  for (CollIndex i = 0; i < rhs.entries(); i++) {
    CorrName corrName(rhs[i].getTableQualName());
    ColRefName colRefName(rhs[i].getColumnName(), corrName);
    insert(colRefName, rhs[i].isInSelectList());
  }
}

void ParCheckConstraintColUsageList::insert(const ColRefName &colName, const NABoolean isInSelectList) {
  ParCheckConstraintColUsage *const pCu = find(colName);
  if (pCu EQU NULL)  // not found
  {
    // ok to insert
    ParCheckConstraintColUsage *cu = new (heap_) ParCheckConstraintColUsage(colName, isInSelectList, heap_);
    LIST(ParCheckConstraintColUsage *)::insert(cu);
  } else  // found
  {
    if (NOT pCu->isInSelectList()) {
      pCu->setIsInSelectList(isInSelectList);
    }
  }
}

// -----------------------------------------------------------------------
// definitions of non-inline methods for class ParCheckConstraintUsages
// -----------------------------------------------------------------------

//
// virtual destructor
//

ParCheckConstraintUsages::~ParCheckConstraintUsages() {}

// -----------------------------------------------------------------------
// methods for class StmtDDLAlterAuditConfig
// -----------------------------------------------------------------------

//
// constructor
//
// constructor used for ALTER AUDIT CONFIG
StmtDDLAlterAuditConfig::StmtDDLAlterAuditConfig(const NAString &logName, const NAString &columns,
                                                 const NAString &values)
    : StmtDDLNode(DDL_ALTER_AUDIT_CONFIG),
      logName_(logName),
      columns_(columns),
      values_(values)

{}

// virtual destructor
StmtDDLAlterAuditConfig::~StmtDDLAlterAuditConfig() {}

// virtual cast
StmtDDLAlterAuditConfig *StmtDDLAlterAuditConfig::castToStmtDDLAlterAuditConfig() { return this; }

//
// methods for tracing
//

const NAString StmtDDLAlterAuditConfig::displayLabel1() const { return NAString("Log name: ") + getLogName(); }

const NAString StmtDDLAlterAuditConfig::displayLabel2() const {
  return NAString("Columns: ") + getColumns() + NAString("Values: ") + getValues();
}

const NAString StmtDDLAlterAuditConfig::getText() const { return "StmtDDLAlterAuditConfig"; }

// -----------------------------------------------------------------------
// Methods for class StmtDDLAlterCatalog
// -----------------------------------------------------------------------

// This constructor is used by:
//    ALTER CATALOG <cat> <enable/disable> SCHEMA <schema name>
StmtDDLAlterCatalog::StmtDDLAlterCatalog(const NAString &catalogName, NABoolean isEnable,
                                         const ElemDDLSchemaName &aSchemaNameParseNode, CollHeap *heap)

    : StmtDDLNode(DDL_ALTER_CATALOG),
      catalogName_(catalogName, heap),
      schemaName_(heap),
      schemaQualName_(aSchemaNameParseNode.getSchemaName(), heap),
      enableStatus_(isEnable),
      disableEnableCreates_(FALSE),
      disableEnableAllCreates_(FALSE),
      isAllSchemaPrivileges_(FALSE) {}

// This constructor is used by:
//    ALTER CATALOG <cat> <enable/disable> ALL SCHEMA and
//    ALTER CATALOG <cat> <enable/disable> CREATE
StmtDDLAlterCatalog::StmtDDLAlterCatalog(const NAString &catalogName, NABoolean isEnable,
                                         NABoolean disableEnableCreates)
    : StmtDDLNode(DDL_ALTER_CATALOG),
      catalogName_(catalogName, PARSERHEAP()),
      schemaName_(PARSERHEAP()),
      schemaQualName_(PARSERHEAP()),
      enableStatus_(isEnable) {
  if (disableEnableCreates) {
    isAllSchemaPrivileges_ = FALSE;
    disableEnableCreates_ = TRUE;
    disableEnableAllCreates_ = FALSE;
  } else {
    isAllSchemaPrivileges_ = TRUE;
    disableEnableCreates_ = FALSE;
    disableEnableAllCreates_ = FALSE;
  }
}

// This constructure is used by:
//    ALTER ALL CATALOG <enable/disable> CREATE and
//    ALTER ALL CATALOGS <enable/disable> CREATE
StmtDDLAlterCatalog::StmtDDLAlterCatalog(NABoolean isEnable)
    : StmtDDLNode(DDL_ALTER_CATALOG),
      catalogName_(PARSERHEAP()),
      schemaName_(PARSERHEAP()),
      schemaQualName_(PARSERHEAP()),
      enableStatus_(isEnable),
      isAllSchemaPrivileges_(FALSE),
      disableEnableCreates_(FALSE),
      disableEnableAllCreates_(TRUE) {}

// This constructor is used by:
//    ALTER CATALOG <cat> <enable/disable> CREATE IN SCHEMA <schema name>
StmtDDLAlterCatalog::StmtDDLAlterCatalog(const NAString &catalogName, NABoolean isEnable,
                                         NABoolean disableEnableCreates, const ElemDDLSchemaName &aSchemaNameParseNode,
                                         CollHeap *heap)

    : StmtDDLNode(DDL_ALTER_CATALOG),
      catalogName_(catalogName, heap),
      schemaName_(heap),
      schemaQualName_(aSchemaNameParseNode.getSchemaName(), heap),
      enableStatus_(isEnable),
      disableEnableCreates_(disableEnableCreates),
      disableEnableAllCreates_(FALSE),
      isAllSchemaPrivileges_(FALSE) {}

StmtDDLAlterCatalog::~StmtDDLAlterCatalog() {}

// cast

StmtDDLAlterCatalog *StmtDDLAlterCatalog::castToStmtDDLAlterCatalog() { return this; }

// for tracing
const NAString StmtDDLAlterCatalog::displayLabel1() const { return NAString("Catalog name: ") + getCatalogName(); }

int StmtDDLAlterCatalog::getArity() const { return MAX_STMT_DDL_ALTER_CATALOG_ARITY; }

const NAString StmtDDLAlterCatalog::getText() const { return "StmtDDLAlterCatalog"; }

void StmtDDLAlterCatalog::setSchemaName(const ElemDDLSchemaName &aSchemaName) {
  schemaQualName_ = aSchemaName.getSchemaName();
}
void StmtDDLAlterCatalog::setAllPrivileges(NABoolean isAll) { isAllSchemaPrivileges_ = isAll; }

// -----------------------------------------------------------------------
// Methods for class StmtDDLAlterSchema
// -----------------------------------------------------------------------

void StmtDDLAlterSchema::initChecks() {
  if (schemaQualName_.getCatalogName().isNull()) {
    schemaName_ = ToAnsiIdentifier(schemaQualName_.getSchemaName());
  } else {
    schemaName_ =
        ToAnsiIdentifier(schemaQualName_.getCatalogName()) + "." + ToAnsiIdentifier(schemaQualName_.getSchemaName());
  }

  // If the schema name specified is reserved name, users cannot drop them.
  // They can only be dropped internally.
  if ((!Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)) &&
      (ComIsTrafodionReservedSchemaName(schemaQualName_.getSchemaName())) &&
      (!ComIsTrafodionExternalSchemaName(schemaQualName_.getSchemaName()))) {
    // error.
    *SqlParser_Diags << DgSqlCode(-1430) << DgSchemaName(schemaName_);
  }
}

StmtDDLAlterSchema::StmtDDLAlterSchema(const ElemDDLSchemaName &aSchemaNameParseNode, CollHeap *heap)
    : StmtDDLNode(DDL_ALTER_SCHEMA),
      schemaQualName_(aSchemaNameParseNode.getSchemaName(), heap),
      dropAllTables_(TRUE),
      renameSchema_(FALSE),
      alterStoredDesc_(FALSE) {
  initChecks();
}

StmtDDLAlterSchema::StmtDDLAlterSchema(const ElemDDLSchemaName &aSchemaNameParseNode, NAString &renamedSchName,
                                       CollHeap *heap)
    : StmtDDLNode(DDL_ALTER_SCHEMA),
      schemaQualName_(aSchemaNameParseNode.getSchemaName(), heap),
      dropAllTables_(FALSE),
      renameSchema_(TRUE),
      renamedSchName_(renamedSchName),
      alterStoredDesc_(FALSE) {
  initChecks();
}

StmtDDLAlterSchema::StmtDDLAlterSchema(const ElemDDLSchemaName &aSchemaNameParseNode,
                                       const StmtDDLAlterTableStoredDesc::AlterStoredDescType oper, CollHeap *heap)

    : StmtDDLNode(DDL_ALTER_SCHEMA),
      schemaQualName_(aSchemaNameParseNode.getSchemaName(), heap),
      dropAllTables_(FALSE),
      renameSchema_(FALSE),
      alterStoredDesc_(TRUE),
      storedDescOper_(oper) {
  initChecks();
}

StmtDDLAlterSchema::~StmtDDLAlterSchema() {}

// cast

StmtDDLAlterSchema *StmtDDLAlterSchema::castToStmtDDLAlterSchema() { return this; }

// for tracing
const NAString StmtDDLAlterSchema::displayLabel1() const { return NAString("Schema name: ") + getSchemaName(); }

const NAString StmtDDLAlterSchema::getText() const { return "StmtDDLAlterSchema"; }

// -----------------------------------------------------------------------
// Methods for class StmtDDLAlterSynonym
// -----------------------------------------------------------------------

//
// Constructor
//

StmtDDLAlterSynonym::StmtDDLAlterSynonym(const QualifiedName &synonymName, const QualifiedName &objectReference)
    : StmtDDLNode(DDL_ALTER_SYNONYM),
      synonymName_(synonymName, PARSERHEAP()),
      objectReference_(objectReference, PARSERHEAP()) {}

//
// Virtual Destructor
// garbage collection is being done automatically by the NAString Class
//

StmtDDLAlterSynonym::~StmtDDLAlterSynonym() {}

// cast

StmtDDLAlterSynonym *StmtDDLAlterSynonym::castToStmtDDLAlterSynonym() { return this; }

// for tracing

const NAString StmtDDLAlterSynonym::displayLabel1() const { return NAString("Synonym name: ") + getSynonymName(); }

const NAString StmtDDLAlterSynonym::displayLabel2() const {
  return NAString("Object reference: ") + getObjectReference();
}

const NAString StmtDDLAlterSynonym::getText() const { return "StmtDDLAlterSynonym"; }

// -----------------------------------------------------------------------
// methods for class StmtDDLAlterIndex
// -----------------------------------------------------------------------

StmtDDLAlterIndex::StmtDDLAlterIndex()
    : StmtDDLNode(DDL_ANY_ALTER_INDEX_STMT),
      alterIndexAction_(NULL),
      indexName_(PARSERHEAP()),
      indexQualName_(PARSERHEAP()) {}

StmtDDLAlterIndex::StmtDDLAlterIndex(OperatorTypeEnum operatorType)
    : StmtDDLNode(operatorType), alterIndexAction_(NULL), indexName_(PARSERHEAP()), indexQualName_(PARSERHEAP()) {}

StmtDDLAlterIndex::StmtDDLAlterIndex(OperatorTypeEnum operatorType, ElemDDLNode *pAlterIndexAction)
    : StmtDDLNode(operatorType),
      alterIndexAction_(pAlterIndexAction),
      indexName_(PARSERHEAP()),
      indexQualName_(PARSERHEAP()) {}

StmtDDLAlterIndex::StmtDDLAlterIndex(OperatorTypeEnum operatorType, const QualifiedName &indexName,
                                     ElemDDLNode *pAlterIndexAction)
    : StmtDDLNode(operatorType),
      indexName_(PARSERHEAP()),
      indexQualName_(indexName, PARSERHEAP()),
      alterIndexAction_(pAlterIndexAction) {
  indexName_ = indexQualName_.getQualifiedNameAsAnsiString();
}

// virtual destructor
StmtDDLAlterIndex::~StmtDDLAlterIndex() {
  // delete all child parse nodes

  for (int i = 0; i < getArity(); i++) {
    delete getChild(i);
  }
}

// cast virtual function
StmtDDLAlterIndex *StmtDDLAlterIndex::castToStmtDDLAlterIndex() { return this; }

//
// accessors
//

int StmtDDLAlterIndex::getArity() const { return MAX_STMT_DDL_ALTER_INDEX_ARITY; }

ExprNode *StmtDDLAlterIndex::getChild(int index) {
  ComASSERT(index EQU INDEX_ALTER_INDEX_ACTION);
  return alterIndexAction_;
}

//
// mutators
//

void StmtDDLAlterIndex::setChild(int index, ExprNode *pChildNode) {
  ComASSERT(index EQU INDEX_ALTER_INDEX_ACTION);
  if (pChildNode NEQ NULL) {
    ComASSERT(pChildNode->castToElemDDLNode() NEQ NULL);
    alterIndexAction_ = pChildNode->castToElemDDLNode();
  } else
    alterIndexAction_ = NULL;
}

void StmtDDLAlterIndex::setIndexName(const QualifiedName &indexName) { indexQualName_ = indexName; }

//
// methods for tracing
//

const NAString StmtDDLAlterIndex::getText() const { return "StmtDDLAlterIndex"; }

const NAString StmtDDLAlterIndex::displayLabel1() const { return NAString("Index name: ") + getIndexName(); }

// -----------------------------------------------------------------------
// methods for class StmtDDLAlterIndexAttribute
// -----------------------------------------------------------------------

//
// constructor
//
StmtDDLAlterIndexAttribute::StmtDDLAlterIndexAttribute(ElemDDLNode *pFileAttrNode)
    : StmtDDLAlterIndex(DDL_ALTER_INDEX_ATTRIBUTE, pFileAttrNode) {
  // Traverse the File Attribute List parse sub-tree to extract the
  // information about the specified file attributes.  Store this
  // information in data member fileAttributes_.

  ComASSERT(getAlterIndexAction() NEQ NULL);
  ElemDDLFileAttrClause *pFileAttrClause = getAlterIndexAction()->castToElemDDLFileAttrClause();
  ComASSERT(pFileAttrClause NEQ NULL);
  ElemDDLNode *pFileAttrList = pFileAttrClause->getFileAttrDefBody();
  ComASSERT(pFileAttrList NEQ NULL);
  for (CollIndex i = 0; i < pFileAttrList->entries(); i++) {
    fileAttributes_.setFileAttr((*pFileAttrList)[i]->castToElemDDLFileAttr());
  }
}

// virtual destructor
StmtDDLAlterIndexAttribute::~StmtDDLAlterIndexAttribute() {}

// cast virtual function
StmtDDLAlterIndexAttribute *StmtDDLAlterIndexAttribute::castToStmtDDLAlterIndexAttribute() { return this; }

//
// methods for tracing
//

NATraceList StmtDDLAlterIndexAttribute::getDetailInfo() const {
  NAString detailText;
  NATraceList detailTextList;

  //
  // index name
  //

  detailTextList.append(displayLabel1());

  //
  // file attributes
  //

  detailTextList.append("File attributes: ");

  const ParDDLFileAttrsAlterIndex fileAttribs = getFileAttributes();
  detailTextList.append("    ", fileAttribs.getDetailInfo());

  return detailTextList;
}

const NAString StmtDDLAlterIndexAttribute::getText() const { return "StmtDDLAlterIndexAttribute"; }

// -----------------------------------------------------------------------
// methods for class StmtDDLAlterIndexHBaseOptions
// -----------------------------------------------------------------------

StmtDDLAlterIndexHBaseOptions::StmtDDLAlterIndexHBaseOptions(ElemDDLHbaseOptions *pHBaseOptions)
    : StmtDDLAlterIndex(DDL_ALTER_INDEX_ALTER_HBASE_OPTIONS), pHBaseOptions_(pHBaseOptions) {
  // nothing else to do
}

// virtual destructor
StmtDDLAlterIndexHBaseOptions::~StmtDDLAlterIndexHBaseOptions() {
  // delete the things I own
  delete pHBaseOptions_;
}

// cast
StmtDDLAlterIndexHBaseOptions *StmtDDLAlterIndexHBaseOptions::castToStmtDDLAlterIndexHBaseOptions() { return this; }

// method for tracing
NATraceList StmtDDLAlterIndexHBaseOptions::getDetailInfo() const {
  NAString detailText;
  NATraceList detailTextList;

  ComASSERT(pHBaseOptions_ NEQ NULL);
  // detailTextList.append(pHBaseOptions_); figure out what to do here later

  return detailTextList;
}

const NAString StmtDDLAlterIndexHBaseOptions::getText() const { return "StmtDDLAlterIndexHBaseOptions"; }

// -----------------------------------------------------------------------
// methods for class StmtDDLAlterTable
// -----------------------------------------------------------------------

//
// constructors
//

StmtDDLAlterTable::StmtDDLAlterTable()
    : StmtDDLNode(DDL_ANY_ALTER_TABLE_STMT),
      tableQualName_(PARSERHEAP()),
      alterTableAction_(NULL),
      isParseSubTreeDestroyedByDestructor_(TRUE),
      isDroppable_(TRUE),
      isOnline_(TRUE),
      forPurgedata_(FALSE),
      partitionEntityType_(OPT_NOT_PARTITION) {}

StmtDDLAlterTable::StmtDDLAlterTable(OperatorTypeEnum operatorType)
    : StmtDDLNode(operatorType),
      tableQualName_(PARSERHEAP()),
      alterTableAction_(NULL),
      isParseSubTreeDestroyedByDestructor_(TRUE),
      isDroppable_(TRUE),
      isOnline_(TRUE),
      forPurgedata_(FALSE),
      partitionEntityType_(OPT_NOT_PARTITION) {}

StmtDDLAlterTable::StmtDDLAlterTable(OperatorTypeEnum operatorType, ElemDDLNode *pAlterTableAction)
    : StmtDDLNode(operatorType),
      tableQualName_(PARSERHEAP()),
      alterTableAction_(pAlterTableAction),
      isParseSubTreeDestroyedByDestructor_(TRUE),
      isDroppable_(TRUE),
      isOnline_(TRUE),
      forPurgedata_(FALSE),
      partitionEntityType_(OPT_NOT_PARTITION) {}

StmtDDLAlterTable::StmtDDLAlterTable(OperatorTypeEnum operatorType, const QualifiedName &tableQualName,
                                     ElemDDLNode *pAlterTableAction)
    : StmtDDLNode(operatorType),
      tableQualName_(tableQualName, PARSERHEAP()),
      alterTableAction_(pAlterTableAction),
      isParseSubTreeDestroyedByDestructor_(TRUE),
      isDroppable_(TRUE),
      isOnline_(TRUE),
      forPurgedata_(FALSE),
      partitionEntityType_(OPT_NOT_PARTITION) {}

// virtual destructor
StmtDDLAlterTable::~StmtDDLAlterTable() {
  //
  // delete all children if the flag isParseSubTreeDestroyedByDestructor_
  // is set.  For more detail information about this flag, please read
  // the comments about this flag in the header file StmtDDLAlterTable.h.
  //
  if (isParseSubTreeDestroyedByDestructor_) {
    for (int i = 0; i < getArity(); i++) {
      delete getChild(i);
    }
  }
}

// cast virtual function
StmtDDLAlterTable *StmtDDLAlterTable::castToStmtDDLAlterTable() { return this; }

//
// accessors
//

int StmtDDLAlterTable::getArity() const { return MAX_STMT_DDL_ALTER_TABLE_ARITY; }

ExprNode *StmtDDLAlterTable::getChild(int index) {
  ComASSERT(index EQU INDEX_ALTER_TABLE_ACTION);
  return alterTableAction_;
}

//
// mutators
//

void StmtDDLAlterTable::setChild(int index, ExprNode *pChildNode) {
  ComASSERT(index EQU INDEX_ALTER_TABLE_ACTION);
  if (pChildNode NEQ NULL) {
    ComASSERT(pChildNode->castToElemDDLNode() NEQ NULL);
    alterTableAction_ = pChildNode->castToElemDDLNode();
  } else
    alterTableAction_ = NULL;
}

void StmtDDLAlterTable::setTableName(const QualifiedName &tableQualName) { tableQualName_ = tableQualName; }

//
// methods for tracing
//

const NAString StmtDDLAlterTable::getText() const { return "StmtDDLAlterTable"; }

StmtDDLAlterTable::PartitionEntityType StmtDDLAlterTable::getPartEntityType() const { return partitionEntityType_; }

const NAString StmtDDLAlterTable::displayLabel1() const { return NAString("Table name: ") + getTableName(); }

// -----------------------------------------------------------------------
// methods for class StmtDDLAddConstraint
// -----------------------------------------------------------------------

// virtual destructor
StmtDDLAddConstraint::~StmtDDLAddConstraint() {}

// cast virtual function
StmtDDLAddConstraint *StmtDDLAddConstraint::castToStmtDDLAddConstraint() { return this; }

//
// accessors
//

const NAString StmtDDLAddConstraint::getConstraintName() const {
  ComASSERT(getAlterTableAction() NEQ NULL);
  ElemDDLConstraint *pConstraint = getAlterTableAction()->castToElemDDLConstraint();
  ComASSERT(pConstraint NEQ NULL);

  return pConstraint->getConstraintName();
}

const QualifiedName &StmtDDLAddConstraint::getConstraintNameAsQualifiedName() const {
  return ((StmtDDLAddConstraint *)this)->getConstraintNameAsQualifiedName();
}

QualifiedName &StmtDDLAddConstraint::getConstraintNameAsQualifiedName() {
  ComASSERT(getAlterTableAction() NEQ NULL);
  ElemDDLConstraint *pConstraint = getAlterTableAction()->castToElemDDLConstraint();
  ComASSERT(pConstraint NEQ NULL);

  return pConstraint->getConstraintNameAsQualifiedName();
}

NABoolean StmtDDLAddConstraint::isDeferrable() const {
  ComASSERT(getAlterTableAction() NEQ NULL);
  ElemDDLConstraint *pConstraint = getAlterTableAction()->castToElemDDLConstraint();
  ComASSERT(pConstraint NEQ NULL);

  return pConstraint->isDeferrable();
}

NABoolean StmtDDLAddConstraint::isDroppable() const {
  ComASSERT(getAlterTableAction() NEQ NULL);
  ElemDDLConstraint *pConstraint = getAlterTableAction()->castToElemDDLConstraint();
  ComASSERT(pConstraint NEQ NULL);

  StmtDDLAddConstraint *ncThis = (StmtDDLAddConstraint *)this;
  StmtDDLAddConstraintPK *pk = ncThis->castToStmtDDLAddConstraintPK();
  if (pk NEQ NULL AND pk->isAlwaysDroppable()) {
    // Primary Key constrainst created in the ALTER TABLE <table-name>
    // ADD CONSTRAINT statement specified by the user is always droppable.
    return TRUE;
  } else {
    return pConstraint->isDroppable();
  }
}

NABoolean StmtDDLAddConstraint::isDroppableSpecifiedExplicitly() const {
  ComASSERT(getAlterTableAction() NEQ NULL);
  ElemDDLConstraint *pConstraint = getAlterTableAction()->castToElemDDLConstraint();
  ComASSERT(pConstraint NEQ NULL);

  return pConstraint->isDroppableSpecifiedExplicitly();
}

NABoolean StmtDDLAddConstraint::isNotDroppableSpecifiedExplicitly() const {
  ComASSERT(getAlterTableAction() NEQ NULL);
  ElemDDLConstraint *pConstraint = getAlterTableAction()->castToElemDDLConstraint();
  ComASSERT(pConstraint NEQ NULL);

  return pConstraint->isNotDroppableSpecifiedExplicitly();
}

//
// mutators
//

void StmtDDLAddConstraint::setDroppableFlag(const NABoolean setting) {
  ComASSERT(getAlterTableAction() NEQ NULL);
  ElemDDLConstraint *pConstraint = getAlterTableAction()->castToElemDDLConstraint();
  ComASSERT(pConstraint NEQ NULL);

  pConstraint->setDroppableFlag(setting);
}

//
// methods for tracing
//

NATraceList StmtDDLAddConstraint::getDetailInfo() const {
  NAString detailText;
  NATraceList detailTextList;

  ElemDDLConstraint *pConstraint = getConstraint();
  ComASSERT(pConstraint NEQ NULL);
  detailTextList.append(pConstraint->getDetailInfo());

  return detailTextList;

}  // StmtDDLAddConstraint::getDetailInfo()

const NAString StmtDDLAddConstraint::getText() const { return "StmtDDLAddConstraint"; }

// -----------------------------------------------------------------------
// methods for class StmtDDLAddConstraintArray
// -----------------------------------------------------------------------

// virtual destructor
StmtDDLAddConstraintArray::~StmtDDLAddConstraintArray() {}

// -----------------------------------------------------------------------
// methods for class StmtDDLAddConstraintCheck
// -----------------------------------------------------------------------

//
// constructors
//

StmtDDLAddConstraintCheck::StmtDDLAddConstraintCheck(ElemDDLNode *pElemDDLConstraintCheck)
    : StmtDDLAddConstraint(DDL_ALTER_TABLE_ADD_CONSTRAINT_CHECK, pElemDDLConstraintCheck) {
  init(pElemDDLConstraintCheck);
}

StmtDDLAddConstraintCheck::StmtDDLAddConstraintCheck(const QualifiedName &tableQualName,
                                                     ElemDDLNode *pElemDDLConstraintCheck)
    : StmtDDLAddConstraint(DDL_ALTER_TABLE_ADD_CONSTRAINT_CHECK, tableQualName, pElemDDLConstraintCheck) {
  init(pElemDDLConstraintCheck);
}

void StmtDDLAddConstraintCheck::init(ElemDDLNode *pElemDDLConstraintCheck) {
  ComASSERT(pElemDDLConstraintCheck NEQ NULL AND pElemDDLConstraintCheck->castToElemDDLConstraintCheck() NEQ NULL);
  ElemDDLConstraintCheck *pCkCnstrnt = pElemDDLConstraintCheck->castToElemDDLConstraintCheck();
  endPos_ = pCkCnstrnt->getEndPosition();
  startPos_ = pCkCnstrnt->getStartPosition();
  nameLocList_ = pCkCnstrnt->getNameLocList();
}

// virtual destructor
StmtDDLAddConstraintCheck::~StmtDDLAddConstraintCheck() {}

// cast virtual function
StmtDDLAddConstraintCheck *StmtDDLAddConstraintCheck::castToStmtDDLAddConstraintCheck() { return this; }

//
// accessors
//

ItemExpr *StmtDDLAddConstraintCheck::getSearchCondition() const {
  ElemDDLConstraintCheck *pCkCnstrnt = getAlterTableAction()->castToElemDDLConstraintCheck();
  ComASSERT(pCkCnstrnt NEQ NULL);
  return pCkCnstrnt->getSearchCondition();
}

//
// methods for tracing
//

const NAString StmtDDLAddConstraintCheck::getText() const { return "StmtDDLAddConstraintCheck"; }

// -----------------------------------------------------------------------
// methods for class StmtDDLAddConstraintCheckArray
// -----------------------------------------------------------------------

// virtual destructor
StmtDDLAddConstraintCheckArray::~StmtDDLAddConstraintCheckArray() {}

// -----------------------------------------------------------------------
// methods for class StmtDDLAddConstraintPK
// -----------------------------------------------------------------------

// constructors
StmtDDLAddConstraintPK::StmtDDLAddConstraintPK(ElemDDLNode *pElemDDLConstraintPK, const NABoolean isAlwaysDroppable)
    : StmtDDLAddConstraintUnique(DDL_ALTER_TABLE_ADD_CONSTRAINT_PRIMARY_KEY, pElemDDLConstraintPK),
      isAlwaysDroppable_(isAlwaysDroppable) {
  if (isAlwaysDroppable AND isNotDroppableSpecifiedExplicitly()) {
    // Primary key constraint defined in ALTER TABLE statement is always
    // droppable.  Please remove the NOT DROPPABLE clause.
    // Note that for the ALTER TABLE ADD COLUMN statement, this check
    // cannot be made in the constructor, since this statement uses
    // the code for CREATE TABLE to parse its column definition.
    // The test for this error was therefore put into function
    // CatAlterTableAddColumn.
    *SqlParser_Diags << DgSqlCode(-3067);
  }
}
StmtDDLAddConstraintPK::StmtDDLAddConstraintPK(const QualifiedName &tableQualName, ElemDDLNode *pElemDDLConstraintPK,
                                               const NABoolean isAlwaysDroppable)
    : StmtDDLAddConstraintUnique(DDL_ALTER_TABLE_ADD_CONSTRAINT_PRIMARY_KEY, tableQualName, pElemDDLConstraintPK),
      isAlwaysDroppable_(isAlwaysDroppable) {
  if (isAlwaysDroppable AND isNotDroppableSpecifiedExplicitly()) {
    // Primary key constraint defined in ALTER TABLE statement is always
    // droppable.  Please remove the NOT DROPPABLE clause.
    // Note that for the ALTER TABLE ADD COLUMN statement, this check
    // cannot be made in the constructor, since this statement uses
    // the code for CREATE TABLE to parse its column definition.
    // The test for this error was therefore put into function
    // CatAlterTableAddColumn.
    *SqlParser_Diags << DgSqlCode(-3067);
  }
}

// virtual destructor
StmtDDLAddConstraintPK::~StmtDDLAddConstraintPK() {}

// cast virtual function
StmtDDLAddConstraintPK *StmtDDLAddConstraintPK::castToStmtDDLAddConstraintPK() { return this; }

// methods for tracing

const NAString StmtDDLAddConstraintPK::getText() const { return "StmtDDLAddConstraintPK"; }

// -----------------------------------------------------------------------
// methods for class StmtDDLAddConstraintRI
// -----------------------------------------------------------------------

// virtual destructor
StmtDDLAddConstraintRI::~StmtDDLAddConstraintRI() {}

// cast virtual function
StmtDDLAddConstraintRI *StmtDDLAddConstraintRI::castToStmtDDLAddConstraintRI() { return this; }

//
// accessors
//

ComRCDeleteRule StmtDDLAddConstraintRI::getDeleteRule() const {
  return getConstraint()->castToElemDDLConstraintRI()->getDeleteRule();
}

ComRCMatchOption StmtDDLAddConstraintRI::getMatchType() const {
  return getConstraint()->castToElemDDLConstraintRI()->getMatchType();
}

ElemDDLColNameArray &StmtDDLAddConstraintRI::getReferencedColumns() {
  return getConstraint()->castToElemDDLConstraintRI()->getReferencedColumns();
}

const ElemDDLColNameArray &StmtDDLAddConstraintRI::getReferencedColumns() const {
  return getConstraint()->castToElemDDLConstraintRI()->getReferencedColumns();
}

NAString StmtDDLAddConstraintRI::getReferencedTableName() const {
  return getConstraint()->castToElemDDLConstraintRI()->getReferencedTableName();
}

const ElemDDLColNameArray &StmtDDLAddConstraintRI::getReferencingColumns() const {
  return getConstraint()->castToElemDDLConstraintRI()->getReferencingColumns();
}

ElemDDLColNameArray &StmtDDLAddConstraintRI::getReferencingColumns() {
  return getConstraint()->castToElemDDLConstraintRI()->getReferencingColumns();
}

ComRCUpdateRule StmtDDLAddConstraintRI::getUpdateRule() const {
  return getConstraint()->castToElemDDLConstraintRI()->getUpdateRule();
}

NABoolean StmtDDLAddConstraintRI::isDeleteRuleSpecified() const {
  return getConstraint()->castToElemDDLConstraintRI()->isDeleteRuleSpecified();
}

NABoolean StmtDDLAddConstraintRI::isUpdateRuleSpecified() const {
  return getConstraint()->castToElemDDLConstraintRI()->isUpdateRuleSpecified();
}

// methods for tracing

const NAString StmtDDLAddConstraintRI::getText() const { return "StmtDDLAddConstraintRI"; }

// -----------------------------------------------------------------------
// methods for class StmtDDLAddConstraintRIArray
// -----------------------------------------------------------------------

// virtual destructor
StmtDDLAddConstraintRIArray::~StmtDDLAddConstraintRIArray() {}

// -----------------------------------------------------------------------
// methods for class StmtDDLAddConstraintUnique
// -----------------------------------------------------------------------

// virtual destructor
StmtDDLAddConstraintUnique::~StmtDDLAddConstraintUnique() {}

// cast virtual function
StmtDDLAddConstraintUnique *StmtDDLAddConstraintUnique::castToStmtDDLAddConstraintUnique() { return this; }

// methods for tracing

const NAString StmtDDLAddConstraintUnique::getText() const { return "StmtDDLAddConstraintUnique"; }

// -----------------------------------------------------------------------
// methods for class StmtDDLAddConstraintUniqueArray
// -----------------------------------------------------------------------

// virtual destructor
StmtDDLAddConstraintUniqueArray::~StmtDDLAddConstraintUniqueArray() {}

// -----------------------------------------------------------------------
// methods for class StmtDDLAlterTableAttribute
// -----------------------------------------------------------------------

//
// constructor
//
StmtDDLAlterTableAttribute::StmtDDLAlterTableAttribute(ElemDDLNode *pFileAttrNode)
    : StmtDDLAlterTable(DDL_ALTER_TABLE_ATTRIBUTE, pFileAttrNode) {
  // Traverse the File Attribute List parse sub-tree to extract the
  // information about the specified file attributes.  Store this
  // information in data member fileAttributes_.

  ComASSERT(getAlterTableAction() NEQ NULL);
  ElemDDLFileAttrClause *pFileAttrClause = getAlterTableAction()->castToElemDDLFileAttrClause();
  ComASSERT(pFileAttrClause NEQ NULL);
  ElemDDLNode *pFileAttrList = pFileAttrClause->getFileAttrDefBody();
  ComASSERT(pFileAttrList NEQ NULL);
  for (CollIndex i = 0; i < pFileAttrList->entries(); i++) {
    fileAttributes_.setFileAttr((*pFileAttrList)[i]->castToElemDDLFileAttr());
  }
}

// virtual destructor
StmtDDLAlterTableAttribute::~StmtDDLAlterTableAttribute() {}

// cast virtual function
StmtDDLAlterTableAttribute *StmtDDLAlterTableAttribute::castToStmtDDLAlterTableAttribute() { return this; }

//
// methods for tracing
//

NATraceList StmtDDLAlterTableAttribute::getDetailInfo() const {
  NAString detailText;
  NATraceList detailTextList;

  //
  // table name
  //

  detailTextList.append(displayLabel1());

  //
  // file attributes
  //

  detailTextList.append("File attributes: ");

  const ParDDLFileAttrsAlterTable fileAttribs = getFileAttributes();
  detailTextList.append("    ", fileAttribs.getDetailInfo());

  return detailTextList;
}

const NAString StmtDDLAlterTableAttribute::getText() const { return "StmtDDLAlterTableAttribute"; }

// -----------------------------------------------------------------------
// methods for class StmtDDLAlterTableColumn
// -----------------------------------------------------------------------

// virtual destructor
StmtDDLAlterTableColumn::~StmtDDLAlterTableColumn() {}

// cast virtual function
StmtDDLAlterTableColumn *StmtDDLAlterTableColumn::castToStmtDDLAlterTableColumn() { return this; }

//
// methods for tracing
//

const NAString StmtDDLAlterTableColumn::getText() const { return "StmtDDLAlterTableColumn"; }

// -----------------------------------------------------------------------
// methods for class StmtDDLAlterTableMove
// -----------------------------------------------------------------------

// virtual destructor
StmtDDLAlterTableMove::~StmtDDLAlterTableMove() {}

// cast virtual function
StmtDDLAlterTableMove *StmtDDLAlterTableMove::castToStmtDDLAlterTableMove() { return this; }

//
// methods for tracing
//

const NAString StmtDDLAlterTableMove::getText() const { return "StmtDDLAlterTableMove"; }

// -----------------------------------------------------------------------
// methods for class StmtDDLAlterTablePartition
// -----------------------------------------------------------------------
// constructor
StmtDDLAlterTablePartition::StmtDDLAlterTablePartition(ElemDDLNode *pPartitionAction)
    : StmtDDLAlterTable(DDL_ALTER_TABLE_PARTITION, pPartitionAction) {}

// virtual destructor
StmtDDLAlterTablePartition::~StmtDDLAlterTablePartition() {}

// cast virtual function
StmtDDLAlterTablePartition *StmtDDLAlterTablePartition::castToStmtDDLAlterTablePartition() { return this; }

//
// methods for tracing
//

const NAString StmtDDLAlterTablePartition::getText() const { return "StmtDDLAlterTablePartition"; }

// -----------------------------------------------------------------------
// methods for class StmtDDLAlterTableAddPartition
// -----------------------------------------------------------------------
// constructor
StmtDDLAlterTableAddPartition::StmtDDLAlterTableAddPartition(ElemDDLNode *tgtPartition)
    : StmtDDLAlterTable(DDL_ALTER_TABLE_ADD_PARTITION), targetPartitions_(tgtPartition) {}

// virtual destructor
StmtDDLAlterTableAddPartition::~StmtDDLAlterTableAddPartition() {}

// cast virtual function
StmtDDLAlterTableAddPartition *StmtDDLAlterTableAddPartition::castToStmtDDLAlterTableAddPartition() { return this; }

//
// methods for tracing
//

const NAString StmtDDLAlterTableAddPartition::getText() const { return "StmtDDLAlterTableAddPartition"; }

NABoolean StmtDDLAlterTableAddPartition::isAddSinglePartition() const {
  return targetPartitions_->castToElemDDLPartitionV2() != NULL ? TRUE : FALSE;
}

// -----------------------------------------------------------------------
// methods for class StmtDDLAlterTableMountPartition
// -----------------------------------------------------------------------
// constructor
StmtDDLAlterTableMountPartition::StmtDDLAlterTableMountPartition(ElemDDLPartitionV2 *tgtPartition, NAString s,
                                                                 NABoolean v)
    : StmtDDLAlterTable(DDL_ALTER_TABLE_MOUNT_PARTITION),
      targetPartition_(tgtPartition),
      validation_(v),
      targetPartitionName_(s) {}

// virtual destructor
StmtDDLAlterTableMountPartition::~StmtDDLAlterTableMountPartition() {}

// cast virtual function
StmtDDLAlterTableMountPartition *StmtDDLAlterTableMountPartition::castToStmtDDLAlterTableMountPartition() {
  return this;
}

//
// methods for tracing
//

const NAString StmtDDLAlterTableMountPartition::getText() const { return "StmtDDLAlterTableMountPartition"; }

// -----------------------------------------------------------------------
// methods for class StmtDDLAlterTableUnmountPartition
// -----------------------------------------------------------------------
// constructor
StmtDDLAlterTableUnmountPartition::StmtDDLAlterTableUnmountPartition(ElemDDLNode *pPartitionAction)
    : StmtDDLAlterTable(DDL_ALTER_TABLE_UNMOUNT_PARTITION, pPartitionAction) {}

// virtual destructor
StmtDDLAlterTableUnmountPartition::~StmtDDLAlterTableUnmountPartition() {}

// cast virtual function
StmtDDLAlterTableUnmountPartition *StmtDDLAlterTableUnmountPartition::castToStmtDDLAlterTableUnmountPartition() {
  return this;
}

//
// methods for tracing
//

const NAString StmtDDLAlterTableUnmountPartition::getText() const { return "StmtDDLAlterTableUnmountPartition"; }

//------------------------------------------------------------------
// StmtDDLAlterTableUnmountParititon_visit
//
// Parameter pAltTabUnmountPartNode points to the Alter Table Unmount Partition
//   parse node.
// Parameter pElement points to a column constraint definition
//   parse node in the left linear tree list.
//     This tree is a sub-tree in the Alter Table Unmount partition parse node.
// Parameter index contains the index of the parse node
//   pointed by pElement in the (left linear tree) list.
//------------------------------------------------------------------
void StmtDDLAlterTableUnmountParititon_visit(ElemDDLNode *pAltTabUnmountPartNode, CollIndex /* index */,
                                             ElemDDLNode *pElement) {
  ComASSERT(pAltTabUnmountPartNode NEQ NULL AND pElement NEQ NULL);

  StmtDDLAlterTableUnmountPartition *pUnmountPart = pAltTabUnmountPartNode->castToStmtDDLAlterTableUnmountPartition();
  ComASSERT(pUnmountPart NEQ NULL);

  ElemDDLPartitionNameAndForValues *pPartNameFVS = pElement->castToElemDDLPartitionNameAndForValues();
  ComASSERT(pPartNameFVS NEQ NULL);

  pUnmountPart->getPartitionNameAndForValuesArray().insert(pPartNameFVS);
}  // StmtDDLAlterTableUnmountParititon_visit

//
// Collect information in the parse sub-tree and copy it
// to the current parse node.
//
void StmtDDLAlterTableUnmountPartition::synthesize() {
  ElemDDLNode *UnmountPart = getAlterTableAction();
  ComASSERT(UnmountPart NEQ NULL);

  UnmountPart->traverseList(this, StmtDDLAlterTableUnmountParititon_visit);
  ComASSERT(partNameAndForValuesArray_.entries() > 0);
}

//------------------------------------------------------------------
// StmtDDLAlterTableTruncateParititon_visit
//
// Parameter pAltTabTruncatePartNode points to the Alter Table Truncate Partition
//   parse node.
// Parameter pElement points to a column constraint definition
//   parse node in the left linear tree list.
//     This tree is a sub-tree in the Alter Table Truncate partition parse node.
// Parameter index contains the index of the parse node
//   pointed by pElement in the (left linear tree) list.
//------------------------------------------------------------------
void StmtDDLAlterTableTruncateParititon_visit(ElemDDLNode *pAltTabTruncatePartNode, CollIndex /* index */,
                                              ElemDDLNode *pElement) {
  ComASSERT(pAltTabTruncatePartNode NEQ NULL AND pElement NEQ NULL);
  StmtDDLAlterTableTruncatePartition *pTruncatePart =
      pAltTabTruncatePartNode->castToStmtDDLAlterTableTruncatePartition();
  ComASSERT(pTruncatePart NEQ NULL);

  ElemDDLPartitionNameAndForValues *pPartNameFVS = pElement->castToElemDDLPartitionNameAndForValues();
  ComASSERT(pPartNameFVS NEQ NULL);

  pTruncatePart->getPartNameAndForValuesArray().insert(pPartNameFVS);
}
//
// Collect information in the parse sub-tree and copy it
// to the current parse node.
//
void StmtDDLAlterTableTruncatePartition::synthesize() {
  ElemDDLNode *truncatePart = getAlterTableAction();
  ComASSERT(truncatePart NEQ NULL);

  truncatePart->traverseList(this, StmtDDLAlterTableTruncateParititon_visit);
  ComASSERT(partNameAndForValuesArray_.entries() > 0);
}

// -----------------------------------------------------------------------
// methods for class StmtDDLAlterTableDropPartition
// -----------------------------------------------------------------------

// virtual destructor
StmtDDLAlterTableDropPartition::~StmtDDLAlterTableDropPartition() {}

// cast virtual function
StmtDDLAlterTableDropPartition *StmtDDLAlterTableDropPartition::castToStmtDDLAlterTableDropPartition() { return this; }

//------------------------------------------------------------------
// StmtDDLAlterTableDropParititon_visit
//
// Parameter pAltTabDropPartNode points to the Alter Table Drop Partition
//   parse node.
// Parameter pElement points to a column constraint definition
//   parse node in the left linear tree list.
//     This tree is a sub-tree in the Alter Table Drop partition parse node.
// Parameter index contains the index of the parse node
//   pointed by pElement in the (left linear tree) list.
//------------------------------------------------------------------
void StmtDDLAlterTableDropParititon_visit(ElemDDLNode *pAltTabDropPartNode, CollIndex /* index */,
                                          ElemDDLNode *pElement) {
  ComASSERT(pAltTabDropPartNode NEQ NULL AND pElement NEQ NULL);

  StmtDDLAlterTableDropPartition *pDropPart = pAltTabDropPartNode->castToStmtDDLAlterTableDropPartition();
  ComASSERT(pDropPart NEQ NULL);

  ElemDDLPartitionNameAndForValues *pPartNameFVS = pElement->castToElemDDLPartitionNameAndForValues();
  ComASSERT(pPartNameFVS NEQ NULL);

  pDropPart->getPartitionNameAndForValuesArray().insert(pPartNameFVS);
}  // StmtDDLAlterTableDropParititon_visit

//
// Collect information in the parse sub-tree and copy it
// to the current parse node.
//
void StmtDDLAlterTableDropPartition::synthesize() {
  ElemDDLNode *dropPart = getAlterTableAction();
  ComASSERT(dropPart NEQ NULL);

  dropPart->traverseList(this, StmtDDLAlterTableDropParititon_visit);
  ComASSERT(partNameAndForValuesArray_.entries() > 0);
}

const NAString StmtDDLAlterTableDropPartition::getText() const { return "StmtDDLAlterTableDropPartition"; }

// -----------------------------------------------------------------------
// methods for class StmtDDLAlterTableRename
// -----------------------------------------------------------------------

// virtual destructor
StmtDDLAlterTableRename::~StmtDDLAlterTableRename() {}

// cast virtual function
StmtDDLAlterTableRename *StmtDDLAlterTableRename::castToStmtDDLAlterTableRename() { return this; }

//
// methods for tracing
//

const NAString StmtDDLAlterTableRename::getText() const { return "StmtDDLAlterTableRename"; }

// -----------------------------------------------------------------------
// methods for class StmtDDLAlterTableNamespace
// -----------------------------------------------------------------------

// virtual destructor
StmtDDLAlterTableNamespace::~StmtDDLAlterTableNamespace() {}

// cast virtual function
StmtDDLAlterTableNamespace *StmtDDLAlterTableNamespace::castToStmtDDLAlterTableNamespace() { return this; }

//
// methods for tracing
//

const NAString StmtDDLAlterTableNamespace::getText() const { return "StmtDDLAlterTableNamespace"; }

// -----------------------------------------------------------------------
// methods for class StmtDDLAlterTableResetDDLLock
// -----------------------------------------------------------------------

// virtual destructor
StmtDDLAlterTableResetDDLLock::~StmtDDLAlterTableResetDDLLock() {}

// cast virtual function
StmtDDLAlterTableResetDDLLock *StmtDDLAlterTableResetDDLLock::castToStmtDDLAlterTableResetDDLLock() { return this; }

//
// methods for tracing
//

const NAString StmtDDLAlterTableResetDDLLock::getText() const { return "StmtDDLAlterTableResetDDLLock"; }

// -----------------------------------------------------------------------
// methods for class StmtDDLAlterTableSetConstraint
// -----------------------------------------------------------------------

// virtual destructor
StmtDDLAlterTableSetConstraint::~StmtDDLAlterTableSetConstraint() {}

// cast virtual function
StmtDDLAlterTableSetConstraint *StmtDDLAlterTableSetConstraint::castToStmtDDLAlterTableSetConstraint() { return this; }

//
// methods for tracing
//

const NAString StmtDDLAlterTableSetConstraint::getText() const { return "StmtDDLAlterTableSetConstraint"; }

// --------------------------------------------------------------------
// methods for class StmtDDLAlterTableDisableIndex
// --------------------------------------------------------------------

//
// constructor
//

StmtDDLAlterTableDisableIndex::StmtDDLAlterTableDisableIndex(NAString &indexName, NABoolean allIndexes,
                                                             NABoolean allUniqueIndexes)
    : StmtDDLAlterTable(DDL_ALTER_TABLE_DISABLE_INDEX),
      indexName_(indexName, PARSERHEAP()),
      allIndexes_(allIndexes),
      allUniqueIndexes_(allUniqueIndexes) {
  isIndexOnPartition_ = FALSE;
  partitionName_ = NAString("DUMMY", PARSERHEAP());
}

//
// Virtual destructor
//

StmtDDLAlterTableDisableIndex::~StmtDDLAlterTableDisableIndex() {}

//
// Cast function: to provide the safe castdown to the current object
//

StmtDDLAlterTableDisableIndex *StmtDDLAlterTableDisableIndex::castToStmtDDLAlterTableDisableIndex() { return this; }

//
// accessors
//

//
// for tracing
//

const NAString StmtDDLAlterTableDisableIndex::displayLabel2() const {
  return NAString("Index name: ") + getIndexName();
}

const NAString StmtDDLAlterTableDisableIndex::getText() const { return "StmtAlterTableDisableIndex"; }

// --------------------------------------------------------------------
// methods for class StmtDDLAlterTableEnableIndex
// --------------------------------------------------------------------

//
// constructor
//

StmtDDLAlterTableEnableIndex::StmtDDLAlterTableEnableIndex(NAString &indexName, NABoolean allIndexes,
                                                           NABoolean allUniqueIndexes)
    : StmtDDLAlterTable(DDL_ALTER_TABLE_ENABLE_INDEX),
      indexName_(indexName, PARSERHEAP()),
      allIndexes_(allIndexes),
      allUniqueIndexes_(allUniqueIndexes) {
  isIndexOnPartition_ = FALSE;
  partitionName_ = NAString("DUMMY", PARSERHEAP());
}

//
// Virtual destructor
//

StmtDDLAlterTableEnableIndex::~StmtDDLAlterTableEnableIndex() {}

//
// Cast function: to provide the safe castdown to the current object
//

StmtDDLAlterTableEnableIndex *StmtDDLAlterTableEnableIndex::castToStmtDDLAlterTableEnableIndex() { return this; }

//
// accessors
//

//
// for tracing
//

const NAString StmtDDLAlterTableEnableIndex::displayLabel2() const { return NAString("Index name: ") + getIndexName(); }

const NAString StmtDDLAlterTableEnableIndex::getText() const { return "StmtAlterTableEnableIndex"; }

// --------------------------------------------------------------------
// methods for class StmtDDLAlterTableToggleConstraint
// --------------------------------------------------------------------

//
// constructor
//

StmtDDLAlterTableToggleConstraint::StmtDDLAlterTableToggleConstraint(const QualifiedName &constraintQualifiedName,
                                                                     NABoolean allConstraints, NABoolean setDisabled,
                                                                     NABoolean validateConstraint)
    : StmtDDLAlterTable(DDL_ALTER_TABLE_TOGGLE_CONSTRAINT),
      constraintQualName_(constraintQualifiedName, PARSERHEAP()),
      allConstraints_(allConstraints),
      setDisabled_(setDisabled),
      validateConstraint_(validateConstraint) {}

//
// Virtual destructor
//

StmtDDLAlterTableToggleConstraint::~StmtDDLAlterTableToggleConstraint() {}

//
// Cast function: to provide the safe castdown to the current object
//

StmtDDLAlterTableToggleConstraint *StmtDDLAlterTableToggleConstraint::castToStmtDDLAlterTableToggleConstraint() {
  return this;
}

//
// accessors
//
const NAString StmtDDLAlterTableToggleConstraint::getConstraintName() const {
  return constraintQualName_.getQualifiedNameAsAnsiString();
}

//
// for tracing
//

const NAString StmtDDLAlterTableToggleConstraint::displayLabel2() const {
  return NAString("Constraint name: ") + getConstraintName();
}

const NAString StmtDDLAlterTableToggleConstraint::getText() const { return "StmtAlterTableToggleConstraint"; }

// -----------------------------------------------------------------------
// methods for class StmtDDLDropConstraint
// -----------------------------------------------------------------------

//
// constructor
//

StmtDDLDropConstraint::StmtDDLDropConstraint(const QualifiedName &constraintQualifiedName, ComDropBehavior dropbehavior)

    : StmtDDLAlterTable(DDL_ALTER_TABLE_DROP_CONSTRAINT),
      constraintQualName_(constraintQualifiedName, PARSERHEAP()),
      dropBehavior_(dropbehavior) {}

//
// Virtual destructor
//

StmtDDLDropConstraint::~StmtDDLDropConstraint() {}

//
// Cast function: to provide the safe castdown to the current object
//

StmtDDLDropConstraint *StmtDDLDropConstraint::castToStmtDDLDropConstraint() { return this; }

//
// accessors
//

const NAString StmtDDLDropConstraint::getConstraintName() const {
  return constraintQualName_.getQualifiedNameAsAnsiString();
}

//
// for tracing
//

const NAString StmtDDLDropConstraint::displayLabel2() const {
  return NAString("Constraint name: ") + getConstraintName();
}

const NAString StmtDDLDropConstraint::getText() const { return "StmtAlterTableDropContraint"; }

const NAString StmtDDLDropConstraint::displayLabel3() const {
  NAString label2("Drop Behavior: ");
  switch (getDropBehavior()) {
    case COM_CASCADE_DROP_BEHAVIOR:
      return label2 + "Cascade";
    case COM_RESTRICT_DROP_BEHAVIOR:
      return label2 + "Restrict";
    default:
      NAAbort("StmtDDLAlter.C", __LINE__, "Internal logic error");
      return NAString();
  }
}

//----------------------------------------------------------------------------
// CLASS StmtDDLAlterTableAlterColumnLoggable
//----------------------------------------------------------------------------

StmtDDLAlterTableAlterColumnLoggable::StmtDDLAlterTableAlterColumnLoggable(ElemDDLNode *pColumnDefinition,
                                                                           NABoolean loggableVal, CollHeap *heap)
    : StmtDDLAlterTable(DDL_ALTER_TABLE_ALTER_COLUMN_LOGGABLE, QualifiedName(PARSERHEAP()) /*no table name*/,
                        pColumnDefinition),
      columnName_(heap),
      loggable_(loggableVal) {}

StmtDDLAlterTableAlterColumnLoggable::StmtDDLAlterTableAlterColumnLoggable(NAString columnName, NABoolean loggableVal,
                                                                           CollHeap *heap)
    : StmtDDLAlterTable(DDL_ALTER_TABLE_ALTER_COLUMN_LOGGABLE), columnName_(columnName, heap), loggable_(loggableVal) {}

StmtDDLAlterTableAlterColumnLoggable::~StmtDDLAlterTableAlterColumnLoggable() {}

StmtDDLAlterTableAlterColumnLoggable *
StmtDDLAlterTableAlterColumnLoggable::castToStmtDDLAlterTableAlterColumnLoggable() {
  return this;
}

//----------------------------------------------------------------------------
// CLASS StmtDDLAlterTableAlterColumnSetSGOption
//----------------------------------------------------------------------------
StmtDDLAlterTableAlterColumnSetSGOption::StmtDDLAlterTableAlterColumnSetSGOption(const NAString &columnName,
                                                                                 ElemDDLSGOptions *pSGOptions,
                                                                                 CollHeap *heap)
    : StmtDDLAlterTable(DDL_ALTER_TABLE_ALTER_COLUMN_SET_SG_OPTION, QualifiedName(PARSERHEAP()) /*no table name*/,
                        NULL),
      columnName_(columnName, heap),
      pSGOptions_(pSGOptions) {}

//
// Virtual destructor
//

StmtDDLAlterTableAlterColumnSetSGOption::~StmtDDLAlterTableAlterColumnSetSGOption() {
  if (pSGOptions_) delete pSGOptions_;

  if (columnName_) delete columnName_;
}

//
// Cast function: to provide the safe castdown to the current object
//

StmtDDLAlterTableAlterColumnSetSGOption *
StmtDDLAlterTableAlterColumnSetSGOption::castToStmtDDLAlterTableAlterColumnSetSGOption() {
  return this;
}

const NAString StmtDDLAlterTableAlterColumnSetSGOption::getText() const {
  return "StmtDDLAlterTableAlterColumnSetSGOption";
}

// -----------------------------------------------------------------------
// methods for class StmtDDLAlterTableAddColumn
// -----------------------------------------------------------------------

// constructor
StmtDDLAlterTableAddColumn::StmtDDLAlterTableAddColumn(ElemDDLNode *pColumnDefinition, CollHeap *heap)
    : StmtDDLAlterTable(DDL_ALTER_TABLE_ADD_COLUMN, QualifiedName(PARSERHEAP()) /*no table name*/, pColumnDefinition),
      pColumnToAdd_(pColumnDefinition),
      columnDefArray_(heap),
      addConstraintCheckArray_(heap),
      addConstraintRIArray_(heap),
      addConstraintUniqueArray_(heap),
      addConstraintArray_(heap),
      pAddConstraintPK_(NULL),
      addIfNotExists_(FALSE) {}

// virtual destructor
StmtDDLAlterTableAddColumn::~StmtDDLAlterTableAddColumn() {
  // Delete the kludge parse nodes derived from class
  // StmtDDLAddConstraint.  For more information, please read
  // the contents of the header file StmtDDLAlterTableAddColumn.h.

  StmtDDLAddConstraint *pAddConstraint;
  while (addConstraintArray_.getFirst(pAddConstraint)) {
    delete pAddConstraint;
  }

  // Delete all children

  for (int i = 0; i < getArity(); i++) {
    delete getChild(i);
  }
}

// cast virtual function
StmtDDLAlterTableAddColumn *StmtDDLAlterTableAddColumn::castToStmtDDLAlterTableAddColumn() { return this; }

// -----------------------------------------------------------------------
// methods for class StmtDDLAlterTableDropColumn
// -----------------------------------------------------------------------

// constructor
StmtDDLAlterTableDropColumn::StmtDDLAlterTableDropColumn(NAString &colName, CollHeap *heap)
    : StmtDDLAlterTable(DDL_ALTER_TABLE_DROP_COLUMN, QualifiedName(PARSERHEAP()) /*no table name*/, NULL),
      colName_(colName),
      dropIfExists_(FALSE) {}

// virtual destructor
StmtDDLAlterTableDropColumn::~StmtDDLAlterTableDropColumn() {}

// cast virtual function
StmtDDLAlterTableDropColumn *StmtDDLAlterTableDropColumn::castToStmtDDLAlterTableDropColumn() { return this; }

void StmtDDLAlterTableAddColumn_visit(ElemDDLNode *pAltTabAddColNode, CollIndex /* index */, ElemDDLNode *pElement);
//
// Collect information in the parse sub-tree and copy it
// to the current parse node.
//
void StmtDDLAlterTableAddColumn::synthesize() {
  ElemDDLNode *theColumn = getColToAdd();
  if (theColumn NEQ NULL) {
    // If pConstraint is not NULL, it points to a (left linear tree) list
    // of column constraint definitions. Traverse this parse sub-tree.
    // For each column constraint definition (except for Not Null
    // constraint), add it to the contraint list corresponding to its
    // constraint type (Check, RI, or Unique).

    theColumn->traverseList(this, StmtDDLAlterTableAddColumn_visit);

    ElemDDLColDefArray ColDefArray = getColDefArray();
    ElemDDLColDef *pColDef = ColDefArray[0];
    if (NOT pColDef->getColumnFamily().isNull()) {
      // TEMPTEMP
      // Currently, DTM doesnt handle add columns with an explicit
      // column family as a transactional operation.
      // Do not use ddl xns until that bug is fixed.
      setDdlXns(FALSE);
    }
  }
}  // StmtDDLAlterTableAddColumn::synthesize()

//------------------------------------------------------------------
// StmtDDLAlterTableAddColumn_visit
//
// Parameter pAltTabAddColNode points to the Alter Table Add Column
//   parse node.
// Parameter pElement points to a column constraint definition
//   parse node in the left linear tree list.
//     This tree is a sub-tree in the Alter Table Add Column parse node.
// Parameter index contains the index of the parse node
//   pointed by pElement in the (left linear tree) list.
//------------------------------------------------------------------
void StmtDDLAlterTableAddColumn_visit(ElemDDLNode *pAltTabAddColNode, CollIndex /* index */, ElemDDLNode *pElement) {
  ComASSERT(pAltTabAddColNode NEQ NULL AND pAltTabAddColNode->castToStmtDDLAlterTableAddColumn()
                NEQ NULL AND pElement NEQ NULL);

  StmtDDLAlterTableAddColumn *pAltTabAddCol = pAltTabAddColNode->castToStmtDDLAlterTableAddColumn();
  if (pElement->castToElemDDLColDef() NEQ NULL) {
    ElemDDLColDef *pColDef = pElement->castToElemDDLColDef();
    pAltTabAddCol->getColDefArray().insert(pColDef);
    if (pColDef->getIsConstraintPKSpecified()) {
      pAltTabAddCol->setConstraint(pColDef->getConstraintPK());
    }
    //
    // For each column constraint definition (except for
    // not null and primary key constraints), creates
    // a corresponding table constraint definition and
    // then insert the newly create parse node to the
    // appropriate table constraint array.  This arrangement
    // helps the processing of constraint definitions in
    // create table statement.
    //
    for (CollIndex i = 0; i < pColDef->getConstraintArray().entries(); i++) {
      pAltTabAddCol->setConstraint(pColDef->getConstraintArray()[i]);
    }
  } else
    *SqlParser_Diags << DgSqlCode(-1001);

}  // StmtDDLAlterTableAddColumn_visit()

void StmtDDLAlterTableAddColumn::setConstraint(ElemDDLNode *pElement) {
  switch (pElement->getOperatorType()) {
    case ELM_CONSTRAINT_CHECK_ELEM: {
      ComASSERT(pElement->castToElemDDLConstraintCheck() NEQ NULL);

      StmtDDLAddConstraintCheck *pAddConstraintCheck =
          new (PARSERHEAP()) StmtDDLAddConstraintCheck(getTableNameAsQualifiedName(), pElement);

      pAddConstraintCheck->setIsParseSubTreeDestroyedByDestructor(FALSE);
      addConstraintArray_.insert(pAddConstraintCheck);

      addConstraintCheckArray_.insert(pAddConstraintCheck);
    } break;

    case ELM_CONSTRAINT_REFERENTIAL_INTEGRITY_ELEM: {
      ComASSERT(pElement->castToElemDDLConstraintRI() NEQ NULL);

      StmtDDLAddConstraintRI *pAddConstraintRI =
          new (PARSERHEAP()) StmtDDLAddConstraintRI(getTableNameAsQualifiedName(), pElement);

      pAddConstraintRI->setIsParseSubTreeDestroyedByDestructor(FALSE);
      addConstraintArray_.insert(pAddConstraintRI);

      addConstraintRIArray_.insert(pAddConstraintRI);
    } break;

    case ELM_CONSTRAINT_UNIQUE_ELEM: {
      ComASSERT(pElement->castToElemDDLConstraintUnique() NEQ NULL);

      StmtDDLAddConstraintUnique *pAddConstraintUnique =
          new (PARSERHEAP()) StmtDDLAddConstraintUnique(getTableNameAsQualifiedName(), pElement);

      pAddConstraintUnique->setIsParseSubTreeDestroyedByDestructor(FALSE);
      addConstraintArray_.insert(pAddConstraintUnique);

      addConstraintUniqueArray_.insert(pAddConstraintUnique);
    } break;

    case ELM_CONSTRAINT_PRIMARY_KEY_ELEM:
    case ELM_CONSTRAINT_PRIMARY_KEY_COLUMN_ELEM: {
      pAddConstraintPK_ = new (PARSERHEAP()) StmtDDLAddConstraintPK(getTableNameAsQualifiedName(), pElement);

      pAddConstraintPK_->setIsParseSubTreeDestroyedByDestructor(FALSE);
      addConstraintArray_.insert(pAddConstraintPK_);
    } break;

    default:
      NAAbort("StmtDDLAlter.C", __LINE__, "internal logic error");
      break;
  }
}  // StmtDDLAlterTableAddColumn::setConstraint()

//
// methods for tracing
//

const NAString StmtDDLAlterTableAddColumn::getText() const { return "StmtDDLAlterTableAddColumn"; }

// -----------------------------------------------------------------------
// methods for class StmtDDLAlterTableHBaseOptions
// -----------------------------------------------------------------------

StmtDDLAlterTableHBaseOptions::StmtDDLAlterTableHBaseOptions(ElemDDLHbaseOptions *pHBaseOptions)
    : StmtDDLAlterTable(DDL_ALTER_TABLE_ALTER_HBASE_OPTIONS), pHBaseOptions_(pHBaseOptions) {
  // nothing else to do
}

// virtual destructor
StmtDDLAlterTableHBaseOptions::~StmtDDLAlterTableHBaseOptions() {
  // delete the things I own
  delete pHBaseOptions_;
}

// cast
StmtDDLAlterTableHBaseOptions *StmtDDLAlterTableHBaseOptions::castToStmtDDLAlterTableHBaseOptions() { return this; }

// method for tracing
NATraceList StmtDDLAlterTableHBaseOptions::getDetailInfo() const {
  NAString detailText;
  NATraceList detailTextList;

  ComASSERT(pHBaseOptions_ NEQ NULL);
  // detailTextList.append(pHBaseOptions_); figure out what to do here later

  return detailTextList;
}

const NAString StmtDDLAlterTableHBaseOptions::getText() const { return "StmtDDLAlterTableHBaseOptions"; }

// -----------------------------------------------------------------------
// methods for class StmtDDLAlterLibrary
// -----------------------------------------------------------------------

//
// constructor
//
// constructor used for ALTER LIBRARY
StmtDDLAlterLibrary::StmtDDLAlterLibrary(const QualifiedName &libraryName, const NAString &fileName,
                                         ElemDDLNode *clientName, ElemDDLNode *clientFilename, CollHeap *heap)
    : StmtDDLNode(DDL_ALTER_LIBRARY),
      libraryName_(libraryName, heap),
      fileName_(fileName),
      clientName_("", heap),
      clientFilename_("", heap)

{
  ElemDDLLibClientName *clientNameNode = NULL;
  ElemDDLLibClientFilename *clientFilenameNode = NULL;

  if (clientName != NULL) {
    clientNameNode = clientName->castToElemDDLLibClientName();

    if (clientNameNode != NULL) clientName_ = clientNameNode->getClientName();
  }

  if (clientFilename != NULL) {
    clientFilenameNode = clientFilename->castToElemDDLLibClientFilename();

    if (clientFilenameNode != NULL) clientFilename_ = clientFilenameNode->getFilename();
  }
}

// virtual destructor
StmtDDLAlterLibrary::~StmtDDLAlterLibrary() {}

// virtual cast
StmtDDLAlterLibrary *StmtDDLAlterLibrary::castToStmtDDLAlterLibrary() { return this; }

//
// methods for tracing
//

const NAString StmtDDLAlterLibrary::displayLabel1() const { return NAString("Library name: ") + getLibraryName(); }

const NAString StmtDDLAlterLibrary::displayLabel2() const { return NAString("Filename: ") + getFilename(); }

const NAString StmtDDLAlterLibrary::getText() const { return "StmtDDLAlterLibrary"; }

//
// Virtual destructor
//

StmtDDLAlterTrigger::~StmtDDLAlterTrigger() {}

// cast virtual function
StmtDDLAlterTrigger *StmtDDLAlterTrigger::castToStmtDDLAlterTrigger() { return this; }

//
// methods for tracing
//

const NAString StmtDDLAlterTrigger::getText() const { return "StmtDDLAlterTrigger"; }

const NAString StmtDDLAlterTrigger::displayLabel1() const {
  return NAString("TriggerOrTable name: ") + getTriggerOrTableName();
}

// -----------------------------------------------------------------------
// methods for class StmtDDLAlterView
// -----------------------------------------------------------------------

// constructor
StmtDDLAlterView::StmtDDLAlterView(QualifiedName &viewName, const NAString &newName)
    : StmtDDLNode(DDL_ALTER_VIEW),
      alterType_(RENAME),
      viewQualName_(viewName, PARSERHEAP()),
      newName_(newName, PARSERHEAP()),
      cascade_(FALSE) {}

// constructor
StmtDDLAlterView::StmtDDLAlterView(QualifiedName &viewName, const NABoolean cascade)
    : StmtDDLNode(DDL_ALTER_VIEW), alterType_(COMPILE), viewQualName_(viewName, PARSERHEAP()), cascade_(cascade) {}

// virtual destructor
StmtDDLAlterView::~StmtDDLAlterView() {}

// cast virtual function
StmtDDLAlterView *StmtDDLAlterView::castToStmtDDLAlterView() { return this; }

//
// methods for tracing
//

// virtual
const NAString StmtDDLAlterView::displayLabel1() const { return NAString("View name: ") + getViewName(); }

// virtual
const NAString StmtDDLAlterView::getText() const { return "StmtDDLAlterView"; }

//
// accessors
//

QualifiedName &StmtDDLAlterView::getViewNameAsQualifiedName() { return viewQualName_; }

const QualifiedName &StmtDDLAlterView::getViewNameAsQualifiedName() const { return viewQualName_; }

// -----------------------------------------------------------------------
// methods for class StmtDDLAlterRoutine
// -----------------------------------------------------------------------

// constructor
StmtDDLAlterRoutine::StmtDDLAlterRoutine(ComAnsiNameSpace eNameSpace, const QualifiedName &aRoutineName,
                                         const QualifiedName &anActionName, ComRoutineType eRoutineType,
                                         ElemDDLNode *pAlterPassThroughParamParseTree,
                                         ElemDDLNode *pAddPassThroughParamParseTree,
                                         ElemDDLNode *pRoutineAttributesParseTree, CollHeap *heap)
    : StmtDDLCreateRoutine(aRoutineName, anActionName,
                           NULL,                           // in - ElemDDLNode * pParamList
                           NULL,                           // in - ElemDDLNode * pReturnsList
                           pAddPassThroughParamParseTree,  // in - ElemDDLNode * pPassThroughParamList
                           pRoutineAttributesParseTree,    // in - ElemDDLNode * pOptionList
                           eRoutineType,                   // in - ComRoutinetype
                           heap),                          // in - CollHeap * heap);
      nameSpace_(eNameSpace),
      alterPassThroughInputsParseTree_(pAlterPassThroughParamParseTree) {
  setOperatorType(DDL_ALTER_ROUTINE);  // overwrite the setting done by StmtDDLCreateRoutine() call
}

// virtual destructor
StmtDDLAlterRoutine::~StmtDDLAlterRoutine() {}

// virtual cast
StmtDDLAlterRoutine *StmtDDLAlterRoutine::castToStmtDDLAlterRoutine() { return this; }

//
// helper
//

void StmtDDLAlterRoutine::synthesize() {
  ElemDDLNode *pPassThroughList = getAlterPassThroughInputsParseTree();
  if (pPassThroughList NEQ NULL) {
    CollIndex i4 = 0;
    CollIndex nbrPassThroughList = pPassThroughList->entries();
    ElemDDLPassThroughParamDef *passThroughDef = NULL;
    for (i4 = 0; i4 < nbrPassThroughList; i4++) {
      passThroughDef = (*pPassThroughList)[i4]->castToElemDDLPassThroughParamDef();
      ComASSERT(passThroughDef NEQ NULL);
      alterPassThroughParamArray_.insert(passThroughDef);  // ALTER PASS THROUGH INPUTS
    }                                                      // for (i4 = 0; i4 < nbrPassThroughList; i4++)
  }

  StmtDDLCreateRoutine::synthesize();

}  // StmtDDLAlterRoutine::synthesize()

//
// virtual tracing functions
//

const NAString StmtDDLAlterRoutine::displayLabel1(void) const { return NAString("Routine name: ") + getRoutineName(); }

const NAString StmtDDLAlterRoutine::displayLabel2(void) const {
  NAString actionName(getActionNameAsAnsiString());
  if (NOT actionName.isNull())
    return "Routine action name: " + actionName;
  else
    return "No routine action name";
}

NATraceList StmtDDLAlterRoutine::getDetailInfo(void) const {
  NAString detailText;
  NATraceList detailTextList;

  //
  // routine name and routine action name
  //

  detailTextList.append(displayLabel1());
  detailTextList.append(displayLabel2());

  return detailTextList;
}

const NAString StmtDDLAlterRoutine::getText(void) const { return "StmtDDLAlterKRoutine"; }

// -----------------------------------------------------------------------
// methods for class StmtDDLAlterUser
// -----------------------------------------------------------------------

//
// constructor
//
// constructor used for ALTER USER
StmtDDLAlterUser::StmtDDLAlterUser(const NAString &databaseUsername, AlterUserCmdSubType cmdSubType,
                                   const NAString *pExternalName, NAString *pConfig, NABoolean isValidUser,
                                   CollHeap *heap, const NAString *authPassword)
    : StmtDDLNode(DDL_ALTER_USER),
      databaseUserName_(databaseUsername, heap),
      alterUserCmdSubType_(cmdSubType),
      isValidUser_(isValidUser),
      groupList_(NULL),
      isSetupWithDefaultPassword_(TRUE)

{
  if (pExternalName == NULL)
    externalUserName_ = ComString("", heap);
  else {
    NAString userName(*pExternalName, heap);
    externalUserName_ = userName;
  }

  if (pConfig == NULL)
    config_ = ComString("", heap);
  else {
    NAString config(*pConfig, heap);
    config_ = config;
  }

  if (cmdSubType == StmtDDLAlterUser::SET_USER_PASSWORD) {
    if (authPassword == NULL) {
      // NAString password(databaseUserName_, heap);
      // set a defalut password
      NAString password(AUTH_DEFAULT_WORD, heap);
      authPassword_ = password;
    } else {
      NAString password(*authPassword, heap);
      authPassword_ = password;
      isSetupWithDefaultPassword_ = FALSE;
    }
  }
}

// virtual destructor
StmtDDLAlterUser::~StmtDDLAlterUser() {
  if (this->groupList_) {
    delete groupList_;
  }
}

// virtual cast
StmtDDLAlterUser *StmtDDLAlterUser::castToStmtDDLAlterUser() { return this; }

//
// methods for tracing
//

const NAString StmtDDLAlterUser::displayLabel1() const {
  return NAString("Database username: ") + getDatabaseUsername();
}

const NAString StmtDDLAlterUser::displayLabel2() const {
  if (NOT getExternalUsername().isNull()) return NAString("External username: ") + getExternalUsername();

  return NAString("External username not specified.");
}

const NAString StmtDDLAlterUser::getText() const { return "StmtDDLAlterUser"; }

StmtDDLAlterTableHDFSCache::StmtDDLAlterTableHDFSCache(const NAString &pool, NABoolean atc, NAMemory *heap)
    : StmtDDLAlterTable(DDL_ALTER_TABLE_HDFS_CACHE), poolName_(pool, heap), isAddToCache_(atc) {}

StmtDDLAlterTableHDFSCache::~StmtDDLAlterTableHDFSCache() {}

StmtDDLAlterTableHDFSCache *StmtDDLAlterTableHDFSCache::castToStmtDDLAlterTableHDFSCache() { return this; }

// StmtDDLAlterSharedCache
StmtDDLAlterSharedCache::StmtDDLAlterSharedCache(enum StmtDDLAlterSharedCache::Subject s, const QualifiedName &name,
                                                 enum StmtDDLAlterSharedCache::Options x, NABoolean isInternal,
                                                 NAMemory *heap)
    : StmtDDLNode(DDL_ALTER_SHARED_CACHE),
      subject_(s),
      qualName_(name, heap),
      options_(x),
      internal_(isInternal),
      sharedObjType_(StmtDDLAlterSharedCache::INVALID_) {}

StmtDDLAlterSharedCache::StmtDDLAlterSharedCache(enum StmtDDLAlterSharedCache::Subject s,
                                                 enum StmtDDLAlterSharedCache::Options x, NAMemory *heap)
    : StmtDDLNode(DDL_ALTER_SHARED_CACHE),
      qualName_(SEABASE_SCHEMA_OBJECTNAME, "", "", heap),
      subject_(s),
      options_(x),
      internal_(FALSE),
      sharedObjType_(StmtDDLAlterSharedCache::INVALID_) {}

// -----------------------------------------------------------------------
// methods for class StmtDDLAlterTableTruncatePartition
// -----------------------------------------------------------------------
StmtDDLAlterTableTruncatePartition::StmtDDLAlterTableTruncatePartition(ElemDDLNode *pPartitionAction, UINT globalIdxAct,
                                                                       CollHeap *heap)
    : StmtDDLAlterTable(DDL_ALTER_TABLE_TRUNCATE_PARTITION, pPartitionAction), globalIdxAct_(globalIdxAct) {}

/*
StmtDDLAlterTableTruncatePartition::StmtDDLAlterTableTruncatePartition(ItemExpr *partitionKey,
                                                                                  NABoolean isForClause,
                                                                                  UINT globalIdxAct,
                                                                                  CollHeap *heap)
  :StmtDDLAlterTable(DDL_ALTER_TABLE_TRUNCATE_PARTITION),
  partitionKey_(partitionKey),
  isForClause_(isForClause),
  globalIdxAct_(globalIdxAct)
{
}
*/

StmtDDLAlterTableTruncatePartition::~StmtDDLAlterTableTruncatePartition() {}

StmtDDLAlterTableTruncatePartition *StmtDDLAlterTableTruncatePartition::castToStmtDDLAlterTableTruncatePartition() {
  return this;
}

const NAString StmtDDLAlterTableTruncatePartition::getText() const { return "StmtDDLAlterTableTruncatePartition"; }

StmtDDLAlterTableMergePartition::StmtDDLAlterTableMergePartition(ElemDDLNode *srcPartitions, NAString tgtPartition)
    : StmtDDLAlterTable(DDL_ALTER_TABLE_MERGE_PARTITION),
      sourcePartitions_(srcPartitions),
      targetPartition_(tgtPartition),
      beginPartition_(NULL),
      endPartition_(NULL),
      tgtPart_(NULL),
      sortedSrcPart_(PARSERHEAP()) {}

StmtDDLAlterTableMergePartition::StmtDDLAlterTableMergePartition(ElemDDLNode *beginPartition, ElemDDLNode *endPartition,
                                                                 NAString tgtPartition)
    : StmtDDLAlterTable(DDL_ALTER_TABLE_MERGE_PARTITION),
      sourcePartitions_(NULL),
      targetPartition_(tgtPartition),
      beginPartition_(beginPartition),
      endPartition_(endPartition),
      tgtPart_(NULL),
      sortedSrcPart_(PARSERHEAP()) {}

StmtDDLAlterTableMergePartition::~StmtDDLAlterTableMergePartition() {}

StmtDDLAlterTableMergePartition *StmtDDLAlterTableMergePartition::castToStmtDDLAlterTableMergePartition() {
  return this;
}

const NAString StmtDDLAlterTableMergePartition::getText() const { return "StmtDDLAlterTableMergePartition"; }

StmtDDLAlterTableExchangePartition::StmtDDLAlterTableExchangePartition(NABoolean isSubPart, ElemDDLNode *partition,
                                                                       QualifiedName exchangeTableName)
    : StmtDDLAlterTable(DDL_ALTER_TABLE_EXCHANGE_PARTITION),
      isSubPartition_(isSubPart),
      partition_(partition),
      naPartition_(NULL),
      exchangeTableName_(exchangeTableName, PARSERHEAP()) {}

StmtDDLAlterTableExchangePartition::~StmtDDLAlterTableExchangePartition() {}

StmtDDLAlterTableExchangePartition *StmtDDLAlterTableExchangePartition::castToStmtDDLAlterTableExchangePartition() {
  return this;
}

const NAString StmtDDLAlterTableExchangePartition::getText() const { return "StmtDDLAlterTableExchangePartition"; }

// -----------------------------------------------------------------------
// methods for class StmtDDLAlterTableRenamePartition
// -----------------------------------------------------------------------

StmtDDLAlterTableRenamePartition::StmtDDLAlterTableRenamePartition(NAString &oldPartName, NAString &newPartName,
                                                                   NABoolean isForClause, CollHeap *heap)
    : StmtDDLAlterTable(DDL_ALTER_TABLE_RENAME_PARTITION),
      oldPartName_(oldPartName),
      newPartName_(newPartName),
      isForClause_(isForClause) {}

StmtDDLAlterTableRenamePartition::StmtDDLAlterTableRenamePartition(ItemExpr *partitionKey, NAString &newPartName,
                                                                   NABoolean isForClause, CollHeap *heap)
    : StmtDDLAlterTable(DDL_ALTER_TABLE_RENAME_PARTITION),
      newPartName_(newPartName),
      partitionKey_(partitionKey),
      isForClause_(isForClause) {}

StmtDDLAlterTableRenamePartition::~StmtDDLAlterTableRenamePartition() {}

StmtDDLAlterTableRenamePartition *StmtDDLAlterTableRenamePartition::castToStmtDDLAlterTableRenamePartition() {
  return this;
}

const NAString StmtDDLAlterTableRenamePartition::getText() const { return "StmtDDLAlterTableRenamePartition"; }

// -----------------------------------------------------------------------
// methods for class StmtDDLAlterTableSplitPartition
// -----------------------------------------------------------------------

StmtDDLAlterTableSplitPartition::StmtDDLAlterTableSplitPartition(PartitionEntityType entityType, ElemDDLNode *partition,
                                                                 ItemExpr *splitKey, NAString &newSP1, NAString &newSP2)
    : StmtDDLAlterTable(DDL_ALTER_TABLE_SPLIT_PARTITION),
      partition_(partition),
      naPartition_(NULL),
      splitedKey_(splitKey) {
  setPartEntityType(entityType);
  splitPartNameList_ = new (PARSERHEAP()) LIST(NAString)(PARSERHEAP());
  splitPartNameList_->insert(newSP1);
  splitPartNameList_->insert(newSP2);
  splitstatus_[0] = splitstatus_[1] = 0;
}

StmtDDLAlterTableSplitPartition::~StmtDDLAlterTableSplitPartition() {}

StmtDDLAlterTableSplitPartition *StmtDDLAlterTableSplitPartition::castToStmtDDLAlterTableSplitPartition() {
  return this;
}

const NAString StmtDDLAlterTableSplitPartition::getText() const { return "StmtDDLAlterTableSplitPartition"; }

//
// End of File
//
