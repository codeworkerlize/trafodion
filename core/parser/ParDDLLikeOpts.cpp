/* -*-C++-*-

 *****************************************************************************
 *
 * File:         ParDDLLikeOpts.C
 * Description:  methods for class ParDDLLikeOpts and classes derived
 *               from class ParDDLLikeOpts
 *
 * Created:      6/6/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "parser/ParDDLLikeOpts.h"

#include "common/BaseTypes.h"
#include "common/ComASSERT.h"
#include "common/ComOperators.h"
#include "common/OperTypeEnum.h"
#include "parser/ElemDDLLikeOptions.h"
#include "parser/ElemDDLSaltOptions.h"
#include "parser/ParDDLLikeOptsCreateTable.h"

#ifndef SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#define SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#endif

#include "parser/SqlParserGlobals.h"

// -----------------------------------------------------------------------
// methods for class ParDDLLikeOpts
// -----------------------------------------------------------------------

//
// virtual destructor
//
ParDDLLikeOpts::~ParDDLLikeOpts() {}

//
// assignment operator
//
ParDDLLikeOpts &ParDDLLikeOpts::operator=(const ParDDLLikeOpts &likeOptions) {
  if (this EQU & likeOptions) return *this;

  likeOptsNodeType_ = likeOptions.likeOptsNodeType_;
  return *this;
}

// -----------------------------------------------------------------------
// methods for class ParDDLLikeOptsCreateTable
// -----------------------------------------------------------------------

//
// constructor
//
ParDDLLikeOptsCreateTable::ParDDLLikeOptsCreateTable() : ParDDLLikeOpts(ParDDLLikeOpts::LIKE_OPTS_CREATE_TABLE) {
  initializeDataMembers();
}

//
// virtual destructor
//
ParDDLLikeOptsCreateTable::~ParDDLLikeOptsCreateTable() {}

//
// assignment operator
//
ParDDLLikeOptsCreateTable &ParDDLLikeOptsCreateTable::operator=(const ParDDLLikeOptsCreateTable &likeOptions) {
  if (this EQU & likeOptions) return *this;

  ParDDLLikeOpts::operator=(likeOptions);

  isLikeOptWithCommentsSpec_ = likeOptions.isLikeOptWithCommentsSpec_;
  isLikeOptWithoutConstraintsSpec_ = likeOptions.isLikeOptWithoutConstraintsSpec_;
  isLikeOptWithoutIndexesSpec_ = likeOptions.isLikeOptWithoutIndexesSpec_;
  isLikeOptWithHeadingsSpec_ = likeOptions.isLikeOptWithHeadingsSpec_;
  isLikeOptWithHelpTextSpec_ = likeOptions.isLikeOptWithHelpTextSpec_;
  isLikeOptWithHorizontalPartitionsSpec_ = likeOptions.isLikeOptWithHorizontalPartitionsSpec_;
  isLikeOptWithoutSaltSpec_ = likeOptions.isLikeOptWithoutSaltSpec_;
  isLikeOptSaltClauseSpec_ = likeOptions.isLikeOptSaltClauseSpec_;
  isLikeOptWithoutDivisionSpec_ = likeOptions.isLikeOptWithoutDivisionSpec_;
  isLikeOptLimitColumnLengthSpec_ = likeOptions.isLikeOptLimitColumnLengthSpec_;
  isLikeOptWithoutRowFormatSpec_ = likeOptions.isLikeOptWithoutRowFormatSpec_;
  isLikeOptWithoutLobColumnsSpec_ = likeOptions.isLikeOptWithoutLobColumnsSpec_;
  isLikeOptWithoutNamespaceSpec_ = likeOptions.isLikeOptWithoutNamespaceSpec_;
  isLikeOptWithDataSpec_ = likeOptions.isLikeOptWithDataSpec_;
  isLikeOptWithComments_ = likeOptions.isLikeOptWithComments_;
  isLikeOptWithoutConstraints_ = likeOptions.isLikeOptWithoutConstraints_;
  isLikeOptWithoutIndexes_ = likeOptions.isLikeOptWithoutIndexes_;
  isLikeOptWithHeadings_ = likeOptions.isLikeOptWithHeadings_;
  isLikeOptWithHelpText_ = likeOptions.isLikeOptWithHelpText_;
  isLikeOptWithHorizontalPartitions_ = likeOptions.isLikeOptWithHorizontalPartitions_;
  isLikeOptWithoutSalt_ = likeOptions.isLikeOptWithoutSalt_;
  isLikeOptWithoutDivision_ = likeOptions.isLikeOptWithoutDivision_;
  isLikeOptColumnLengthLimit_ = likeOptions.isLikeOptColumnLengthLimit_;
  isLikeOptWithoutRowFormat_ = likeOptions.isLikeOptWithoutRowFormat_;
  isLikeOptWithoutLobColumns_ = likeOptions.isLikeOptWithoutLobColumns_;
  isLikeOptWithoutNamespace_ = likeOptions.isLikeOptWithoutNamespace_;
  likeOptHiveOptions_ = likeOptions.likeOptHiveOptions_;
  isLikeOptWithData_ = likeOptions.isLikeOptWithData_;
  isLikeOptWithoutRegionReplicationSpec_ = likeOptions.isLikeOptWithoutRegionReplicationSpec_;
  isLikeOptWithoutIncrBackupSpec_ = likeOptions.isLikeOptWithoutIncrBackupSpec_;
  isLikeOptWithoutRegionReplication_ = likeOptions.isLikeOptWithoutRegionReplication_;
  isLikeOptWithoutIncrBackup_ = likeOptions.isLikeOptWithoutIncrBackup_;

  if (this != &likeOptions)  // make sure not assigning to self
  {
    if (likeOptions.isLikeOptSaltClause_) {
      delete isLikeOptSaltClause_;
      isLikeOptSaltClause_ = new (PARSERHEAP()) NAString(*likeOptions.isLikeOptSaltClause_);
    } else if (isLikeOptSaltClause_) {
      delete isLikeOptSaltClause_;
      isLikeOptSaltClause_ = NULL;
    }
    // else both are NULL; nothing to do
  }

  return *this;
}

//
// mutators
//

void ParDDLLikeOptsCreateTable::initializeDataMembers() {
  isLikeOptWithCommentsSpec_ = FALSE;
  isLikeOptWithoutConstraintsSpec_ = FALSE;
  isLikeOptWithHeadingsSpec_ = FALSE;
  isLikeOptWithoutIndexesSpec_ = FALSE;
  isLikeOptWithHelpTextSpec_ = FALSE;
  isLikeOptWithHorizontalPartitionsSpec_ = FALSE;
  isLikeOptWithoutSaltSpec_ = FALSE;
  isLikeOptSaltClauseSpec_ = FALSE;
  isLikeOptWithoutDivisionSpec_ = FALSE;
  isLikeOptLimitColumnLengthSpec_ = FALSE;
  isLikeOptWithoutRowFormatSpec_ = FALSE;
  isLikeOptWithoutLobColumnsSpec_ = FALSE;
  isLikeOptWithoutNamespaceSpec_ = FALSE;
  isLikeOptWithDataSpec_ = FALSE;
  isLikeOptWithComments_ = FALSE;
  isLikeOptWithoutConstraints_ = FALSE;
  isLikeOptWithoutIndexes_ = FALSE;
  isLikeOptWithHeadings_ = FALSE;
  isLikeOptWithHelpText_ = FALSE;
  isLikeOptWithHorizontalPartitions_ = FALSE;
  isLikeOptWithoutSalt_ = FALSE;
  isLikeOptSaltClause_ = NULL;
  isLikeOptWithoutDivision_ = FALSE;
  isLikeOptColumnLengthLimit_ = UINT_MAX;
  isLikeOptWithoutRowFormat_ = FALSE;
  isLikeOptWithoutLobColumns_ = FALSE;
  isLikeOptWithoutNamespace_ = FALSE;
  isLikeOptWithData_ = FALSE;
  isLikeOptWithoutRegionReplication_ = FALSE;
  isLikeOptWithoutRegionReplicationSpec_ = FALSE;
  isLikeOptWithoutIncrBackupSpec_ = FALSE;
  isLikeOptWithoutIncrBackup_ = FALSE;
}

void ParDDLLikeOptsCreateTable::setLikeOption(ElemDDLLikeOpt *pLikeOption) {
  ComASSERT(pLikeOption != NULL);

  switch (pLikeOption->getOperatorType()) {
    case ELM_LIKE_OPT_WITHOUT_CONSTRAINTS_ELEM:
      if (isLikeOptWithoutConstraintsSpec_) {
        *SqlParser_Diags << DgSqlCode(-3149);

        //  "*** Error *** Duplicate WITHOUT CONSTRAINTS phrases "
        //  << "in LIKE clause" << endl;
      }
      ComASSERT(pLikeOption->castToElemDDLLikeOptWithoutConstraints() != NULL);
      isLikeOptWithoutConstraints_ = TRUE;
      isLikeOptWithoutConstraintsSpec_ = TRUE;
      break;

    case ELM_LIKE_OPT_WITHOUT_INDEXES_ELEM:
      if (isLikeOptWithoutIndexesSpec_) {
        *SqlParser_Diags << DgSqlCode(-3152) << DgString0("WITHOUT_INDEXES");
        //	"*** Error *** Duplicate WITHOUT INDEXES phrases "
        //	in LIKE clause in CREATE TABLE statement.
      }
      ComASSERT(pLikeOption->castToElemDDLLikeOptWithoutIndexes() != NULL);
      isLikeOptWithoutIndexes_ = TRUE;
      isLikeOptWithoutIndexesSpec_ = TRUE;
      break;

    case ELM_LIKE_OPT_WITH_HEADINGS_ELEM:
      if (isLikeOptWithHeadingsSpec_) {
        *SqlParser_Diags << DgSqlCode(-3150);
        //      cout << "*** Error *** Duplicate WITH HEADING phrases "
        //  << "in LIKE clause" << endl;
      }
      ComASSERT(pLikeOption->castToElemDDLLikeOptWithHeadings() != NULL);
      isLikeOptWithHeadings_ = TRUE;
      isLikeOptWithHeadingsSpec_ = TRUE;
      break;

    case ELM_LIKE_OPT_WITH_HORIZONTAL_PARTITIONS_ELEM:
      if (isLikeOptWithHorizontalPartitionsSpec_) {
        *SqlParser_Diags << DgSqlCode(-3151);
        // cout << "*** Error *** Duplicate WITH HORIZONTAL PARTITIONS phrases "
        //  << "in LIKE clause" << endl;
      }
      ComASSERT(pLikeOption->castToElemDDLLikeOptWithHorizontalPartitions() != NULL);
      isLikeOptWithHorizontalPartitions_ = TRUE;
      isLikeOptWithHorizontalPartitionsSpec_ = TRUE;
      break;

    case ELM_LIKE_OPT_WITHOUT_SALT_ELEM:
      if (isLikeOptWithoutSaltSpec_) {
        // ERROR[3152] Duplicate WITHOUT SALT phrases were specified
        //             in LIKE clause in CREATE TABLE statement.
        *SqlParser_Diags << DgSqlCode(-3152) << DgString0("SALT");
      }
      if (isLikeOptSaltClauseSpec_) {
        // ERROR[3154] The WITHOUT SALT clause is not allowed with the SALT clause.
        *SqlParser_Diags << DgSqlCode(-3154) << DgString0("WITHOUT SALT") << DgString1("SALT");
      }
      ComASSERT(pLikeOption->castToElemDDLLikeOptWithoutSalt() != NULL);
      isLikeOptWithoutSalt_ = TRUE;
      isLikeOptWithoutSaltSpec_ = TRUE;
      break;

    case ELM_LIKE_OPT_SALT_CLAUSE_ELEM: {  // braces needed since we declare some variables in this case
      if (isLikeOptSaltClauseSpec_) {
        // ERROR[3183] Duplicate SALT clauses were specified.
        *SqlParser_Diags << DgSqlCode(-3183) << DgString0("SALT");
      }
      if (isLikeOptWithoutSaltSpec_) {
        // ERROR[3154] The WITHOUT SALT clause is not allowed with the SALT clause.
        *SqlParser_Diags << DgSqlCode(-3154) << DgString0("WITHOUT SALT") << DgString1("SALT");
      }
      ComASSERT(pLikeOption->castToElemDDLLikeSaltClause() != NULL);
      isLikeOptSaltClauseSpec_ = TRUE;
      isLikeOptSaltClause_ = new (PARSERHEAP()) NAString();
      ElemDDLLikeSaltClause *saltClauseWrapper = pLikeOption->castToElemDDLLikeSaltClause();
      const ElemDDLSaltOptionsClause *saltOptions = saltClauseWrapper->getSaltClause();
      saltOptions->unparseIt(*isLikeOptSaltClause_ /* side-effected */);
      isLikeOptWithoutSalt_ = TRUE;  // suppresses any SALT clause from the source table
    } break;

    case ELM_LIKE_OPT_WITHOUT_DIVISION_ELEM:
      if (isLikeOptWithoutDivisionSpec_) {
        // ERROR[3152] Duplicate WITHOUT DIVISION phrases were specified
        //             in LIKE clause in CREATE TABLE statement.
        *SqlParser_Diags << DgSqlCode(-3152) << DgString0("DIVISION");
      }
      ComASSERT(pLikeOption->castToElemDDLLikeOptWithoutDivision() != NULL);
      isLikeOptWithoutDivision_ = TRUE;
      isLikeOptWithoutDivisionSpec_ = TRUE;
      break;

    case ELM_LIKE_OPT_LIMIT_COLUMN_LENGTH: {
      if (isLikeOptLimitColumnLengthSpec_) {
        // ERROR[3152] Duplicate LIMIT COLUMN LENGTH phrases were specified
        //             in LIKE clause in CREATE TABLE statement.
        *SqlParser_Diags << DgSqlCode(-3152) << DgString0("LIMIT COLUMN LENGTH");
      }
      ComASSERT(pLikeOption->castToElemDDLLikeLimitColumnLength() != NULL);
      ElemDDLLikeLimitColumnLength *limitColumnLength = pLikeOption->castToElemDDLLikeLimitColumnLength();
      isLikeOptColumnLengthLimit_ = limitColumnLength->getColumnLengthLimit();
      isLikeOptLimitColumnLengthSpec_ = TRUE;
    } break;

    case ELM_LIKE_OPT_WITHOUT_ROW_FORMAT_ELEM:
      if (isLikeOptWithoutRowFormatSpec_) {
        // ERROR[3152] Duplicate WITHOUT ROW FORMAT phrases were specified
        //             in LIKE clause in CREATE TABLE statement.
        *SqlParser_Diags << DgSqlCode(-3152) << DgString0("ROW FORMAT");
      }
      ComASSERT(pLikeOption->castToElemDDLLikeOptWithoutRowFormat() != NULL);
      isLikeOptWithoutRowFormat_ = TRUE;
      isLikeOptWithoutRowFormatSpec_ = TRUE;
      break;

    case ELM_LIKE_OPT_WITHOUT_LOB_COLUMNS:
      if (isLikeOptWithoutLobColumnsSpec_) {
        // ERROR[3152] Duplicate WITHOUT LOB COLUMNS phrases were specified
        //             in LIKE clause in CREATE TABLE statement.
        *SqlParser_Diags << DgSqlCode(-3152) << DgString0("LOB COLUMNS");
      }
      ComASSERT(pLikeOption->castToElemDDLLikeOptWithoutLobColumns() != NULL);
      isLikeOptWithoutLobColumns_ = TRUE;
      isLikeOptWithoutLobColumnsSpec_ = TRUE;
      break;

    case ELM_LIKE_OPT_WITHOUT_NAMESPACE:
      if (isLikeOptWithoutNamespaceSpec_) {
        // ERROR[3152] Duplicate WITHOUT NAME SPACE phrases were specified
        //             in LIKE clause in CREATE TABLE statement.
        *SqlParser_Diags << DgSqlCode(-3152) << DgString0("NAME SPACE");
      }
      ComASSERT(pLikeOption->castToElemDDLLikeOptWithoutNamespace() != NULL);
      isLikeOptWithoutNamespace_ = TRUE;
      isLikeOptWithoutNamespaceSpec_ = TRUE;
      break;

    case ELM_LIKE_OPT_WITH_DATA:
      if (isLikeOptWithDataSpec_) {
        // ERROR[3152] Duplicate WITH DATA phrases were specified
        //             in LIKE clause in CREATE TABLE statement.
        *SqlParser_Diags << DgSqlCode(-3152) << DgString0("WITH DATA");
      }
      ComASSERT(pLikeOption->castToElemDDLLikeOptWithData() != NULL);
      isLikeOptWithData_ = TRUE;
      isLikeOptWithDataSpec_ = TRUE;
      break;

    case ELM_LIKE_OPT_WITHOUT_REGION_REPLICATION:
      if (isLikeOptWithoutRegionReplicationSpec_) {
        // ERROR[3152] Duplicate WITHOUT REGION_REPLICATION phrases were specified
        //             in LIKE clause in CREATE TABLE statement.
        *SqlParser_Diags << DgSqlCode(-3152) << DgString0("WITHOUT REGION_REPLICATION");
      }
      ComASSERT(pLikeOption->castToElemDDLLikeOptWithoutRegionReplication() != NULL);
      isLikeOptWithoutRegionReplication_ = TRUE;
      isLikeOptWithoutRegionReplicationSpec_ = TRUE;
      break;
    case ELM_LIKE_OPT_WITHOUT_INCREMENTAL_BACKUP:
      if (isLikeOptWithoutIncrBackupSpec_) {
        // ERROR[3152] Duplicate WITHOUT REGION_REPLICATION phrases were specified
        //             in LIKE clause in CREATE TABLE statement.
        *SqlParser_Diags << DgSqlCode(-3152) << DgString0("WITHOUT INCREMENTAL BACKUP");
      }
      ComASSERT(pLikeOption->castToElemDDLLikeOptWithoutIncrBackup() != NULL);
      isLikeOptWithoutIncrBackup_ = TRUE;
      isLikeOptWithoutIncrBackupSpec_ = TRUE;
      break;
    default:
      NAAbort("ParDDLLikeOpts.C", __LINE__, "internal logic error");
      break;
  }
}  // ParDDLLikeOptsCreateTable::setLikeOption()

//
// End of File
//
