
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

#include "parser/ElemDDLFileAttr.h"
#include "parser/ElemDDLFileAttrAllocate.h"
#include "parser/ElemDDLFileAttrAudit.h"
#include "parser/ElemDDLFileAttrAuditCompress.h"
#include "parser/ElemDDLFileAttrBlockSize.h"
#include "parser/ElemDDLFileAttrBuffered.h"
#include "parser/ElemDDLFileAttrClause.h"
#include "parser/ElemDDLFileAttrClearOnPurge.h"
#include "parser/ElemDDLFileAttrDCompress.h"
#include "parser/ElemDDLFileAttrDeallocate.h"
#include "parser/ElemDDLFileAttrExtents.h"
#include "parser/ElemDDLFileAttrICompress.h"
#include "parser/ElemDDLFileAttrInsertLog.h"
#include "parser/ElemDDLFileAttrLockOnRefresh.h"
#include "parser/ElemDDLFileAttrMaxExtents.h"
#include "parser/ElemDDLFileAttrMaxSize.h"
#include "parser/ElemDDLFileAttrMisc.h"
#include "parser/ElemDDLFileAttrNoLabelUpdate.h"
#include "parser/ElemDDLFileAttrOwner.h"
#include "parser/ElemDDLFileAttrPOS.h"
#include "parser/ElemDDLFileAttrRangeLog.h"
