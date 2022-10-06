
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         AllElemDDLFileAttr.h
 * Description:  a header file that includes ElemDDLFileAttr.h and all
 *               header files that define classes derived from class
 *               ElemDDLFileAttr.  This head file also includes the
 *               header file ElemDDLFileAttrClause.h which defines
 *               class ElemDDLFileAttrClause representing a parse node
 *               representing a file Attribute(s) clause in a DDL
 *               statement.  Note that class ElemDDLFileAttrClause is
 *               derived from class ElemDDLNode instead of class
 *               ElemDDLFileAttr.
 *
 *
 * Created:      5/30/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "ElemDDLFileAttr.h"
#include "ElemDDLFileAttrAllocate.h"
#include "ElemDDLFileAttrAudit.h"
#include "ElemDDLFileAttrAuditCompress.h"
#include "ElemDDLFileAttrBlockSize.h"
#include "ElemDDLFileAttrBuffered.h"
#include "ElemDDLFileAttrClause.h"
#include "ElemDDLFileAttrClearOnPurge.h"
#include "ElemDDLFileAttrDCompress.h"
#include "ElemDDLFileAttrDeallocate.h"
#include "ElemDDLFileAttrExtents.h"
#include "ElemDDLFileAttrICompress.h"
#include "ElemDDLFileAttrInsertLog.h"
#include "ElemDDLFileAttrLockOnRefresh.h"
#include "ElemDDLFileAttrMaxExtents.h"
#include "ElemDDLFileAttrMaxSize.h"
#include "ElemDDLFileAttrNoLabelUpdate.h"
#include "ElemDDLFileAttrOwner.h"
#include "ElemDDLFileAttrPOS.h"
#include "parser/ElemDDLFileAttrMisc.h"
#include "parser/ElemDDLFileAttrRangeLog.h"
