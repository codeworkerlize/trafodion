
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         AllStmtDDL.h
 * Description:  a header file that includes StmtDDLNode.h and all the files
 *               that define classes derived from StmtDDLNode.
 *
 *
 * Created:      3/30/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "parser/StmtDDLNode.h"
#include "parser/AllStmtDDLAlterTable.h"
#include "parser/AllStmtDDLCreate.h"
#include "parser/AllStmtDDLDrop.h"
#include "parser/AllStmtDDLAlter.h"  // MV - RG
#include "parser/AllStmtDDLGive.h"
#include "parser/StmtDDLAlterCatalog.h"
#include "parser/StmtDDLAlterSchema.h"
#include "parser/StmtDDLAlterTrigger.h"
#include "parser/StmtDDLAlterIndexAttribute.h"
#include "parser/StmtDDLAlterIndexHBaseOptions.h"
#include "parser/StmtDDLAlterView.h"
#include "parser/StmtDDLAlterUser.h"
#include "parser/StmtDDLAlterDatabase.h"
#include "parser/StmtDDLGrant.h"
#include "parser/StmtDDLGrantComponentPrivilege.h"
#include "parser/StmtDDLSchGrant.h"
#include "parser/StmtDDLRevoke.h"
#include "parser/StmtDDLRevokeComponentPrivilege.h"
#include "parser/StmtDDLSchRevoke.h"
#include "parser/StmtDDLRegisterComponent.h"
#include "parser/StmtDDLRegisterUser.h"
#include "parser/StmtDDLUserGroup.h"
#include "parser/StmtDDLTenant.h"
#include "parser/StmtDDLResourceGroup.h"
#include "parser/StmtDDLCreateRole.h"
#include "parser/StmtDDLRoleGrant.h"
#include "parser/StmtDDLCleanupObjects.h"
#include "parser/StmtDDLAlterSchemaHDFSCache.h"
#include "parser/StmtDDLAlterSharedCache.h"
