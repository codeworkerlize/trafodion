
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         AllStmtDDLAlterTable.h
 * Description:  a header file that includes all header files
 *               defining classes relating to Alter Table statements.
 *
 *
 * Created:      9/22/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "StmtDDLAddConstraint.h"
#include "StmtDDLAddConstraintArray.h"
#include "StmtDDLAddConstraintCheck.h"
#include "StmtDDLAddConstraintCheckArray.h"
#include "StmtDDLAddConstraintRI.h"
#include "StmtDDLAddConstraintUnique.h"
#include "StmtDDLAlterTable.h"
#include "StmtDDLAlterTableAlterColumnLoggable.h"  //++ MV
#include "parser/StmtDDLAddConstraintPK.h"
#include "parser/StmtDDLAddConstraintRIArray.h"
#include "parser/StmtDDLAddConstraintUniqueArray.h"
#include "parser/StmtDDLAlterTableAddColumn.h"
#include "parser/StmtDDLAlterTableDisableIndex.h"
#include "parser/StmtDDLAlterTableDropColumn.h"
#include "parser/StmtDDLAlterTableEnableIndex.h"
#include "parser/StmtDDLAlterTableHDFSCache.h"
#include "parser/StmtDDLDropConstraint.h"
// #include "parser/StmtDDLAlterTableAlterColumn.h"
#include "StmtDDLAlterTableColumn.h"
#include "StmtDDLAlterTableMove.h"
#include "StmtDDLAlterTableNamespace.h"
#include "StmtDDLAlterTableSetConstraint.h"
#include "StmtDDLAlterTableToggleConstraint.h"
#include "parser/StmtDDLAlterTableAddPartition.h"
#include "parser/StmtDDLAlterTableAlterColumn.h"
#include "parser/StmtDDLAlterTableAlterColumnSetSGOption.h"
#include "parser/StmtDDLAlterTableAttribute.h"
#include "parser/StmtDDLAlterTableDropPartition.h"
#include "parser/StmtDDLAlterTableHBaseOptions.h"
#include "parser/StmtDDLAlterTableMountPartition.h"
#include "parser/StmtDDLAlterTablePartition.h"
#include "parser/StmtDDLAlterTableRename.h"
#include "parser/StmtDDLAlterTableRenamePartition.h"
#include "parser/StmtDDLAlterTableResetDDLLock.h"
#include "parser/StmtDDLAlterTableSplitPartition.h"
#include "parser/StmtDDLAlterTableStoredDesc.h"
#include "parser/StmtDDLAlterTableTruncatePartition.h"
#include "parser/StmtDDLAlterTableUnmountPartition.h"

//
// End of File
//
