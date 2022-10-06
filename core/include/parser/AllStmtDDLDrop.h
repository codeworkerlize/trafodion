
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         AllStmtDDLDrop.h
 * Description:  a header file that includes classes supporting
 *               DROP statement parse nodes.
 *
 *
 * Created:      3/29/96
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "StmtDDLDropModule.h"
#include "StmtDDLDropSQL.h"
#include "StmtDDLDropCatalog.h"
// #include "StmtDDLDropColumn.h"
#include "parser/StmtDDLDropComponentPrivilege.h"
// #include "parser/StmtDDLDropConstraint.h"

#include "parser/StmtDDLDropIndex.h"
#include "parser/StmtDDLDropLibrary.h"
#include "parser/StmtDDLDropRoutine.h"
#include "parser/StmtDDLDropSchema.h"
#include "parser/StmtDDLDropTable.h"
#include "parser/StmtDDLDropTrigger.h"
#include "parser/StmtDDLDropView.h"
#include "StmtDDLDropSynonym.h"
#include "StmtDDLDropExceptionTable.h"
#include "parser/StmtDDLNamespace.h"
