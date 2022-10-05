
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

#include "ElemDDLNode.h"
#include "AllElemDDLCol.h"
#include "AllElemDDLConstraint.h"
#include "AllElemDDLConstraintAttr.h"
#include "AllElemDDLFileAttr.h"
#include "AllElemDDLLike.h"
#include "AllElemDDLList.h"
#include "AllElemDDLParam.h"
#include "AllElemDDLPartition.h"
#include "AllElemDDLUdr.h"
#include "ElemDDLLobAttrs.h"
#include "ElemDDLLoggable.h"
#include "ElemDDLAlterTableMove.h"
#include "parser/ElemDDLGrantee.h"
#include "ElemDDLGranteeArray.h"
#include "ElemDDLKeyValue.h"
#include "ElemDDLLibrary.h"
#include "ElemDDLLibClientFilename.h"
#include "ElemDDLLibClientName.h"
#include "ElemDDLLibPathName.h"
#include "ElemDDLLikeOptions.h"
#include "ElemDDLLocation.h"
#include "ElemDDLTableFeature.h"
#include "parser/ElemDDLHbaseOptions.h"
#include "ElemDDLParallelExec.h"
#include "ElemDDLPassThroughParamDef.h"
#include "ElemDDLReferences.h"
#include "ElemDDLSGOption.h"
#include "parser/ElemDDLSGOptions.h"
#include "ElemDDLSchemaName.h"
#include "ElemDDLPrivActions.h"
#include "ElemDDLPrivileges.h"
#include "ElemDDLRefActions.h"
#include "ElemDDLRefTrigActions.h"
#include "ElemDDLDivisionClause.h"
#include "ElemDDLSaltOptions.h"
#include "ElemDDLReplicateClause.h"
#include "parser/ElemDDLStoreOptions.h"
#include "ElemDDLTenantOption.h"
#include "ElemDDLTenantSchema.h"
#include "ElemDDLTenantGroup.h"
#include "ElemDDLTenantResourceGroup.h"
#include "ElemDDLWithCheckOption.h"
#include "ElemDDLWithGrantOption.h"
#include "ElemDDLIndexPopulateOption.h"
#include "ElemDDLIndexScopeOption.h"
#include "parser/ElemDDLQualName.h"                // OZ
#include "ElemDDLCreateMVOneAttributeTableList.h"  // MV OZ
#include "ElemDDLFileAttrMisc.h"
#include "parser/ElemDDLGroup.h"

//
// End of File
//
