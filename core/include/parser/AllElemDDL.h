
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         AllElemDDL.h
 * Description:  a header file that includes ElemDDLNode.h and all header
 *               files that define classes derived from class ElemDDLNode.
 *
 * Created:      3/30/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "AllElemDDLCol.h"
#include "AllElemDDLConstraint.h"
#include "AllElemDDLConstraintAttr.h"
#include "AllElemDDLFileAttr.h"
#include "AllElemDDLLike.h"
#include "AllElemDDLList.h"
#include "AllElemDDLParam.h"
#include "AllElemDDLPartition.h"
#include "AllElemDDLUdr.h"
#include "ElemDDLAlterTableMove.h"
#include "ElemDDLDivisionClause.h"
#include "ElemDDLGranteeArray.h"
#include "ElemDDLIndexPopulateOption.h"
#include "ElemDDLIndexScopeOption.h"
#include "ElemDDLKeyValue.h"
#include "ElemDDLLibClientFilename.h"
#include "ElemDDLLibClientName.h"
#include "ElemDDLLibPathName.h"
#include "ElemDDLLibrary.h"
#include "ElemDDLLikeOptions.h"
#include "ElemDDLLobAttrs.h"
#include "ElemDDLLocation.h"
#include "ElemDDLLoggable.h"
#include "ElemDDLNode.h"
#include "ElemDDLParallelExec.h"
#include "ElemDDLPrivActions.h"
#include "ElemDDLPrivileges.h"
#include "ElemDDLRefActions.h"
#include "ElemDDLRefTrigActions.h"
#include "ElemDDLReferences.h"
#include "ElemDDLReplicateClause.h"
#include "ElemDDLSGOption.h"
#include "ElemDDLSaltOptions.h"
#include "ElemDDLSchemaName.h"
#include "ElemDDLTableFeature.h"
#include "ElemDDLTenantGroup.h"
#include "ElemDDLTenantOption.h"
#include "ElemDDLTenantResourceGroup.h"
#include "ElemDDLTenantSchema.h"
#include "ElemDDLWithCheckOption.h"
#include "ElemDDLWithGrantOption.h"
#include "parser/ElemDDLFileAttrMisc.h"
#include "parser/ElemDDLGrantee.h"
#include "parser/ElemDDLGroup.h"
#include "parser/ElemDDLHbaseOptions.h"
#include "parser/ElemDDLPassThroughParamDef.h"
#include "parser/ElemDDLQualName.h"  // OZ
#include "parser/ElemDDLSGOptions.h"
#include "parser/ElemDDLStoreOptions.h"

//
// End of File
//
