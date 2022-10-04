

#ifndef _CMP_SEABASE_DDL_INCLUDES_H_
#define _CMP_SEABASE_DDL_INCLUDES_H_

// declaration of the yacc parser and its result
#ifndef SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#define SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#endif
#ifndef SQLPARSERGLOBALS_LEX_AND_PARSE
#define SQLPARSERGLOBALS_LEX_AND_PARSE
#endif

#define SQLPARSERGLOBALS_FLAGS
#define SQLPARSERGLOBALS_NADEFAULTS_SET
#include "parser/SqlParserGlobalsCmn.h"

#include "common/ComObjectName.h"
#include "common/ComUser.h"

#include "parser/StmtDDLCreateTable.h"
#include "parser/StmtDDLCreateTrigger.h"
#include "parser/StmtDDLDropTrigger.h"
#include "parser/StmtDDLDropTable.h"
#include "parser/StmtDDLAlterTableRename.h"
#include "parser/StmtDDLAlterTableStoredDesc.h"
#include "parser/StmtDDLCreateIndex.h"
#include "parser/StmtDDLPopulateIndex.h"
#include "parser/StmtDDLDropIndex.h"
#include "parser/StmtDDLAlterIndexHBaseOptions.h"
#include "parser/StmtDDLAlterTableAddColumn.h"
#include "parser/StmtDDLAlterTableDropColumn.h"
#include "parser/StmtDDLAlterTableAlterColumn.h"
#include "parser/StmtDDLAlterTableAlterColumnSetSGOption.h"
#include "parser/StmtDDLAlterTableHBaseOptions.h"
#include "parser/StmtDDLAlterTablePartition.h"
#include "parser/StmtDDLAlterTableAddPartition.h"
#include "parser/StmtDDLAlterTableMountPartition.h"
#include "parser/StmtDDLAlterTableUnmountPartition.h"
#include "parser/StmtDDLAlterTableDropPartition.h"
#include "parser/StmtDDLAlterTableTruncatePartition.h"
#include "parser/StmtDDLAlterTableRenamePartition.h"
#include "parser/StmtDDLAlterTableSplitPartition.h"
#include "parser/StmtDDLAddConstraintPK.h"
#include "parser/StmtDDLAddConstraintRIArray.h"
#include "parser/StmtDDLAddConstraintUniqueArray.h"
#include "parser/StmtDDLGrant.h"
#include "parser/StmtDDLRevoke.h"
#include "parser/StmtDDLDropSchema.h"
#include "parser/StmtDDLRegisterUser.h"
#include "parser/StmtDDLRegisterComponent.h"
#include "parser/StmtDDLCreateView.h"
#include "parser/StmtDDLAlterTableDisableIndex.h"
#include "parser/StmtDDLAlterTableEnableIndex.h"
#include "parser/StmtDDLCreateDropSequence.h"
#include "parser/StmtDDLCreateComponentPrivilege.h"
#include "parser/StmtDDLDropComponentPrivilege.h"
#include "parser/StmtDDLGrantComponentPrivilege.h"
#include "parser/StmtDDLRevokeComponentPrivilege.h"
#include "parser/StmtDDLRegisterComponent.h"
#include "parser/StmtDDLCleanupObjects.h"
#include "parser/StmtDDLAlterTableAttribute.h"
#include "parser/StmtDDLRegOrUnregHive.h"
#include "parser/StmtDDLNamespace.h"
#include "parser/StmtDDLCommentOn.h"

#include "parser/ElemDDLHbaseOptions.h"
#include "parser/ElemDDLParamDefArray.h"
#include "parser/ElemDDLParamDef.h"
#include "parser/ElemDDLConstraintPK.h"
#include "parser/StmtDDLDropConstraint.h"
#include "parser/ElemDDLSGOptions.h"
#include "parser/ElemDDLList.h"
#include "parser/ElemDDLQualName.h"

#include "sqlcomp/CmpDDLCatErrorCodes.h"

#include "optimizer/SchemaDB.h"
#include "sqlcomp/CmpSeabaseDDL.h"
#include "sqlcomp/CmpSeabaseDDLupgrade.h"
#include "sqlcomp/CmpDescribe.h"

#include "exp/ExpHbaseInterface.h"

#include "executor/ExExeUtilCli.h"
#include "generator/Generator.h"

#include "common/ComCextdecs.h"

// get software major and minor versions from -D defs defined in sqlcomp/Makefile.
// These defs pick up values from export vars defined in sqf/sqenvcom.sh.
#define SOFTWARE_MAJOR_VERSION  TRAF_SOFTWARE_VERS_MAJOR
#define SOFTWARE_MINOR_VERSION  TRAF_SOFTWARE_VERS_MINOR
#define SOFTWARE_UPDATE_VERSION TRAF_SOFTWARE_VERS_UPDATE
// multiplier to encode minor & update versions into single value
// increased multiplier allows for more than single digit update number
// smaller multiplier allows compatibility to decode old versions
#define VERSION_MULTIPLE_LARGE   1000
#define VERSION_MULTIPLE_SMALL   10
#define HBASE_OPTIONS_MAX_LENGTH 6000

// new metadata version 2.6.0 changed for release 2.6.0.
// old metadata version 2.1
enum {
  METADATA_MAJOR_VERSION = 2,
  METADATA_OLD_MAJOR_VERSION = 2,
  METADATA_MINOR_VERSION = 6,
  METADATA_OLD_MINOR_VERSION = 1,
  METADATA_UPDATE_VERSION = 0,
  METADATA_OLD_UPDATE_VERSION = 0,
  DATAFORMAT_MAJOR_VERSION = 1,
  DATAFORMAT_MINOR_VERSION = 1
};

#endif
