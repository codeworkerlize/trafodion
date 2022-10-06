/* -*-C++-*-

 *****************************************************************************
 *
 * File:         ElemDDLFileAttr.C
 * Description:  methods for classes representing file attributes and
 *               file Attribute(s) clauses
 *
 * Created:      9/20/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "AllElemDDLFileAttr.h"
#include "common/BaseTypes.h"
#include "common/ComASSERT.h"
#include "common/ComOperators.h"
#include "export/ComDiags.h"
#define SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#include "parser/SqlParserGlobals.h"

// -----------------------------------------------------------------------
// methods for class ElemDDLFileAttr
// -----------------------------------------------------------------------

// virtual destructor
ElemDDLFileAttr::~ElemDDLFileAttr() {}

// cast
ElemDDLFileAttr *ElemDDLFileAttr::castToElemDDLFileAttr() { return this; }

//
// given a size unit enumerated constant,
// return an appropriate NAString.
//
NAString ElemDDLFileAttr::convertSizeUnitToNAString(ComUnits sizeUnit) const {
  switch (sizeUnit) {
    case COM_BYTES:
      return NAString(COM_BYTES_LIT);
    case COM_GBYTES:
      return NAString(COM_GBYTES_LIT);
    case COM_KBYTES:
      return NAString(COM_KBYTES_LIT);
    case COM_MBYTES:
      return NAString(COM_MBYTES_LIT);
    default:
      ABORT("internal logic error");
      return NAString();
  }
}

// methods for tracing

const NAString ElemDDLFileAttr::getText() const {
  ABORT("internal logic error");
  return "ElemDDLFileAttr";
}

// -----------------------------------------------------------------------
// methods for class ElemDDLFileAttrAllocate
// -----------------------------------------------------------------------

//
// constructors
//

ElemDDLFileAttrAllocate::ElemDDLFileAttrAllocate(ComSInt16 extentsToAllocate)
    : ElemDDLFileAttr(ELM_FILE_ATTR_ALLOCATE_ELEM) {
  initializeExtentsToAllocate(extentsToAllocate);
}

// virtual destructor
ElemDDLFileAttrAllocate::~ElemDDLFileAttrAllocate() {}

// cast
ElemDDLFileAttrAllocate *ElemDDLFileAttrAllocate::castToElemDDLFileAttrAllocate() { return this; }

// is specified extents to allocate a legal value?
NABoolean ElemDDLFileAttrAllocate::isLegalAllocateValue(ComSInt16 extentsToAllocate) const {
  if (extentsToAllocate >= 1 && extentsToAllocate <= COM_MAX_MAXEXTENTS) {
    return TRUE;
  } else {
    return FALSE;
  }
}

//
// mutator
//

void ElemDDLFileAttrAllocate::initializeExtentsToAllocate(ComSInt16 extentsToAllocate) {
  if (isLegalAllocateValue(extentsToAllocate)) {
    extentsToAllocate_ = extentsToAllocate;
  } else {
    // Illegal size value in ALLOCATE phrase.
    *SqlParser_Diags << DgSqlCode(-3057) << DgInt0(COM_MAX_MAXEXTENTS);
  }
}

//
// trace
//

const NAString ElemDDLFileAttrAllocate::displayLabel1() const {
  char buffer[80];
  sprintf(buffer, "%d", getExtentsToAllocate());
  return NAString("Allocate Extents: ") + NAString(buffer);
}

const NAString ElemDDLFileAttrAllocate::getText() const { return "ElemDDLFileAttrAllocate"; }

// -----------------------------------------------------------------------
// methods for class ElemDDLFileAttrAudit
// -----------------------------------------------------------------------

// virtual destructor
ElemDDLFileAttrAudit::~ElemDDLFileAttrAudit() {}

// cast
ElemDDLFileAttrAudit *ElemDDLFileAttrAudit::castToElemDDLFileAttrAudit() { return this; }

// methods for tracing

const NAString ElemDDLFileAttrAudit::getText() const { return "ElemDDLFileAttrAudit"; }

const NAString ElemDDLFileAttrAudit::displayLabel1() const { return NAString("Is audited? ") + YesNo(getIsAudit()); }

// -----------------------------------------------------------------------
// methods for class ElemDDLFileAttrAuditCompress
// -----------------------------------------------------------------------

// virtual destructor
ElemDDLFileAttrAuditCompress::~ElemDDLFileAttrAuditCompress() {}

// cast
ElemDDLFileAttrAuditCompress *ElemDDLFileAttrAuditCompress::castToElemDDLFileAttrAuditCompress() { return this; }

// trace

const NAString ElemDDLFileAttrAuditCompress::getText() const { return "ElemDDLFileAttrAuditCompress"; }

const NAString ElemDDLFileAttrAuditCompress::displayLabel1() const {
  return NAString("Is audit-compress? ") + YesNo(getIsAuditCompress());
}

//----------------------------------------------------------------------------
// method for building text
// virtual
NAString ElemDDLFileAttrAuditCompress::getSyntax() const {
  NAString syntax;

  if (FALSE == isAuditCompress_) {
    syntax += "NO ";
  }

  syntax += "AUDITCOMPRESS";

  return syntax;
}  // getSyntax

// -----------------------------------------------------------------------
// methods for class ElemDDLFileAttrBlockSize
// -----------------------------------------------------------------------

// constructor
ElemDDLFileAttrBlockSize::ElemDDLFileAttrBlockSize(int blockSizeInBytes)
    : ElemDDLFileAttr(ELM_FILE_ATTR_BLOCK_SIZE_ELEM) {
  if (isLegalBlockSizeValue(blockSizeInBytes)) {
    // On Linux, only 32K blocksize is supported. For other specified
    // blocksize, a warning is raised and the default 32K will be used.
    if (blockSizeInBytes == 32768)
      blockSizeInBytes_ = blockSizeInBytes;
    else {
      *SqlParser_Diags << DgSqlCode(+3250);
      blockSizeInBytes_ = DEFAULT_BLOCK_SIZE;
    }
  } else {
    // Block size can only be 4096, 8192, 16384, 32768
    *SqlParser_Diags << DgSqlCode(-3058);
    blockSizeInBytes_ = DEFAULT_BLOCK_SIZE;
  }
}

// virtual destructor
ElemDDLFileAttrBlockSize::~ElemDDLFileAttrBlockSize() {}

// is the input value a legal block size value?
NABoolean ElemDDLFileAttrBlockSize::isLegalBlockSizeValue(int blockSize) const {
  // Determine whether the DB Limits checks for large blocks, rows, and keys
  // should be used.
  NABoolean useDBLimits = ((CmpCommon::getDefault(CAT_LARGE_BLOCKS_LARGE_KEYS) == DF_ON) ||
                           (CmpCommon::getDefault(CREATE_OBJECTS_IN_METADATA_ONLY) == DF_ON))
                              ? TRUE
                              : FALSE;

  switch (blockSize) {
    // case  512 :
    // case 1024 :
    // case 2048 :
    case 4096:
    case 32768:
      return TRUE;

    case 8192:
    case 16384:
      if (useDBLimits)
        return TRUE;
      else
        return FALSE;

    default:
      return FALSE;
  }
}

// cast
ElemDDLFileAttrBlockSize *ElemDDLFileAttrBlockSize::castToElemDDLFileAttrBlockSize() { return this; }

//
// trace
//

const NAString ElemDDLFileAttrBlockSize::getText() const { return "ElemDDLFileAttrBlockSize"; }

const NAString ElemDDLFileAttrBlockSize::displayLabel1() const {
  char buffer[80];
  sprintf(buffer, "%d", getBlockSize());
  return NAString("Block size: ") + NAString(buffer);
}

//----------------------------------------------------------------------------
// method for building text
// virtual
NAString ElemDDLFileAttrBlockSize::getSyntax() const {
  NAString syntax = "BLOCKSIZE ";

  syntax += LongToNAString(blockSizeInBytes_);

  return syntax;
}  // getSyntax()

// -----------------------------------------------------------------------
// methods for class ElemDDLFileAttrBuffered
// -----------------------------------------------------------------------

// virtual destructor
ElemDDLFileAttrBuffered::~ElemDDLFileAttrBuffered() {}

// cast
ElemDDLFileAttrBuffered *ElemDDLFileAttrBuffered::castToElemDDLFileAttrBuffered() { return this; }

// trace

const NAString ElemDDLFileAttrBuffered::getText() const { return "ElemDDLFileAttrBuffered"; }

const NAString ElemDDLFileAttrBuffered::displayLabel1() const {
  return NAString("Is buffered? ") + YesNo(getIsBuffered());
}

//----------------------------------------------------------------------------
// method for building text
// virtual
NAString ElemDDLFileAttrBuffered::getSyntax() const {
  NAString syntax;

  if (FALSE == isBuffered_) {
    syntax += "NO ";
  }

  syntax += "BUFFERED";

  return syntax;
}

// ---------------------------------------------------------------------

// methods for class ElemDDLFileAttrCompression
// ---------------------------------------------------------------------

// virtual destructor
ElemDDLFileAttrCompression::~ElemDDLFileAttrCompression() {}

// cast
ElemDDLFileAttrCompression *ElemDDLFileAttrCompression::castToElemDDLFileAttrCompression() { return this; }

// trace

// to be added if required

// -----------------------------------------------------------------------
// methods for class ElemDDLFileAttrClause
//
//   Note that class ElemDDLFileAttrClause is not derived from class
//   ElemDDLFileAttr.  The former is derived from class ElemDDLNode.
// -----------------------------------------------------------------------

// virtual destructor
ElemDDLFileAttrClause::~ElemDDLFileAttrClause() {}

// cast virtual function
ElemDDLFileAttrClause *ElemDDLFileAttrClause::castToElemDDLFileAttrClause() { return this; }

//
// accessors
//

// get the degree of this node
int ElemDDLFileAttrClause::getArity() const { return MAX_ELEM_DDL_FILE_ATTR_CLAUSE_ARITY; }

ExprNode *ElemDDLFileAttrClause::getChild(int index) {
  ComASSERT(index >= 0 AND index < getArity());
  return children_[index];
}

//
// mutators
//

void ElemDDLFileAttrClause::setChild(int index, ExprNode *pChildNode) {
  ComASSERT(index >= 0 AND index < getArity());
  if (pChildNode NEQ NULL) {
    ComASSERT(pChildNode->castToElemDDLNode() NEQ NULL);
    children_[index] = pChildNode->castToElemDDLNode();
  } else {
    children_[index] = NULL;
  }
}

const NAString ElemDDLFileAttrClause::getText() const { return "ElemDDLFileAttrClause"; }

//----------------------------------------------------------------------------
// method for building text
// virtual
NAString ElemDDLFileAttrClause::getSyntax() const {
  NAString syntax = "ATTRIBUTES";
  ElemDDLNode *pFileAttrs = getFileAttrDefBody();
  ComASSERT(pFileAttrs NEQ NULL);

  for (CollIndex i = 0; i < pFileAttrs->entries(); i++) {
    syntax += " ";
    syntax += (*pFileAttrs)[i]->castToElemDDLFileAttr()->getSyntax();
  }

  syntax += " ";

  return syntax;

}  // getSyntax

//----------------------------------------------------------------------------
// methods for class ElemDDLFileAttrClearOnPurge
//----------------------------------------------------------------------------

// virtual destructor
ElemDDLFileAttrClearOnPurge::~ElemDDLFileAttrClearOnPurge() {}

// cast
ElemDDLFileAttrClearOnPurge *ElemDDLFileAttrClearOnPurge::castToElemDDLFileAttrClearOnPurge() { return this; }

// trace

const NAString ElemDDLFileAttrClearOnPurge::getText() const { return "ElemDDLFileAttrClearOnPurge"; }

const NAString ElemDDLFileAttrClearOnPurge::displayLabel1() const {
  return NAString("Is clear-on-purge? ") + YesNo(getIsClearOnPurge());
}

// method for building text
// virtual
NAString ElemDDLFileAttrClearOnPurge::getSyntax() const {
  NAString syntax;

  if (FALSE == isClearOnPurge_) {
    syntax += "NO ";
  }

  syntax += "CLEARONPURGE";

  return syntax;

}  // getSyntax

//----------------------------------------------------------------------------
// methods for class ElemDDLFileAttrDCompress
//----------------------------------------------------------------------------

// virtual destructor
ElemDDLFileAttrDCompress::~ElemDDLFileAttrDCompress() {}

// cast
ElemDDLFileAttrDCompress *ElemDDLFileAttrDCompress::castToElemDDLFileAttrDCompress() { return this; }

// trace

const NAString ElemDDLFileAttrDCompress::getText() const { return "ElemDDLFileAttrDCompress"; }

const NAString ElemDDLFileAttrDCompress::displayLabel1() const {
  return NAString("Is d-compress? ") + YesNo(getIsDCompress());
}

// method for building text
// virtual
NAString ElemDDLFileAttrDCompress::getSyntax() const {
  NAString syntax;

  if (FALSE == isDCompress_) {
    syntax += "NO ";
  }

  syntax += "DCOMPRESS";

  return syntax;

}  // getSyntax

// -----------------------------------------------------------------------
// methods for class ElemDDLFileAttrDeallocate
// -----------------------------------------------------------------------

// virtual destructor
ElemDDLFileAttrDeallocate::~ElemDDLFileAttrDeallocate() {}

// cast
ElemDDLFileAttrDeallocate *ElemDDLFileAttrDeallocate::castToElemDDLFileAttrDeallocate() { return this; }

// trace

const NAString ElemDDLFileAttrDeallocate::getText() const { return "ElemDDLFileAttrDeallocate"; }

//----------------------------------------------------------------------------
// methods for class ElemDDLFileAttrICompress
//----------------------------------------------------------------------------

// virtual destructor
ElemDDLFileAttrICompress::~ElemDDLFileAttrICompress() {}

// cast
ElemDDLFileAttrICompress *ElemDDLFileAttrICompress::castToElemDDLFileAttrICompress() { return this; }

// trace

const NAString ElemDDLFileAttrICompress::getText() const { return "ElemDDLFileAttrICompress"; }

const NAString ElemDDLFileAttrICompress::displayLabel1() const {
  return NAString("Is i-compress? ") + YesNo(getIsICompress());
}

// method for building text
// virtual
NAString ElemDDLFileAttrICompress::getSyntax() const {
  NAString syntax;

  if (FALSE == isICompress_) {
    syntax += "NO ";
  }

  syntax += "ICOMPRESS";

  return syntax;

}  // getSyntax

// -----------------------------------------------------------------------
// methods for class ElemDDLFileAttrRangeLog
// -----------------------------------------------------------------------

// virtual destructor
ElemDDLFileAttrRangeLog::~ElemDDLFileAttrRangeLog() {}

// cast
ElemDDLFileAttrRangeLog *ElemDDLFileAttrRangeLog::castToElemDDLFileAttrRangeLog() { return this; }

// trace

const NAString ElemDDLFileAttrRangeLog::getText() const { return "ElemDDLFileAttrRangeLog"; }

const NAString ElemDDLFileAttrRangeLog::displayLabel1() const {
  return NAString("rangelog type: ") + getRangeLogTypeAsNAString(rangelogType_);
}

NAString ElemDDLFileAttrRangeLog::getRangeLogTypeAsNAString(ComRangeLogType type) {
  switch (type) {
    case COM_NO_RANGELOG:
      return "NO";

    case COM_AUTO_RANGELOG:
      return "AUTOMATIC";

    case COM_MANUAL_RANGELOG:
      return "MANUAL";

    case COM_MIXED_RANGELOG:
      return "MIXED";
  }
  return "NO";
}

// method for building text
// virtual
NAString ElemDDLFileAttrRangeLog::getSyntax() const {
  NAString syntax = getRangeLogTypeAsNAString(rangelogType_);

  syntax += " RANGELOG";

  return syntax;

}  // getSyntax()

//----------------------------------------------------------------------------
// methods for class ElemDDLFileAttrLockOnRefresh
//----------------------------------------------------------------------------

ElemDDLFileAttrLockOnRefresh::~ElemDDLFileAttrLockOnRefresh() {}

ElemDDLFileAttrLockOnRefresh *ElemDDLFileAttrLockOnRefresh::castToElemDDLFileAttrLockOnRefresh() { return this; }

const NAString ElemDDLFileAttrLockOnRefresh::getText() const { return "ElemDDLFileAttrLockOnRefresh"; }

const NAString ElemDDLFileAttrLockOnRefresh::displayLabel1() const {
  return NAString("LOCKONREFRESH?    ") + YesNo(isLockOnRefresh());
}

// method for building text
// virtual
NAString ElemDDLFileAttrLockOnRefresh::getSyntax() const {
  NAString syntax;

  if (FALSE == isLockOnRefresh_) {
    syntax += "NO ";
  }

  syntax += "LOCKONREFRESH";

  return syntax;
}

//----------------------------------------------------------------------------
// methods for class ElemDDLFileAttrInsertLog
//----------------------------------------------------------------------------

ElemDDLFileAttrInsertLog::~ElemDDLFileAttrInsertLog() {}

ElemDDLFileAttrInsertLog *ElemDDLFileAttrInsertLog::castToElemDDLFileAttrInsertLog() { return this; }

const NAString ElemDDLFileAttrInsertLog::getText() const { return "ElemDDLFileAttrInsertLog"; }

const NAString ElemDDLFileAttrInsertLog::displayLabel1() const {
  return NAString("INSERTLOG?    ") + YesNo(isInsertLog());
}

// method for building text
// virtual
NAString ElemDDLFileAttrInsertLog::getSyntax() const {
  NAString syntax;

  if (FALSE == isInsertLog_) {
    syntax += "NO ";
  }

  syntax += "INSERTLOG";

  return syntax;
}

//
// constructors
//

ElemDDLFileAttrMaxSize::ElemDDLFileAttrMaxSize() : ElemDDLFileAttr(ELM_FILE_ATTR_MAX_SIZE_ELEM), isUnbounded_(TRUE) {
  ParSetDefaultMaxSize(maxSize_, maxSizeUnit_);
}

ElemDDLFileAttrMaxSize::ElemDDLFileAttrMaxSize(int maxSize)
    : ElemDDLFileAttr(ELM_FILE_ATTR_MAX_SIZE_ELEM), isUnbounded_(FALSE), maxSizeUnit_((ComUnits)DEFAULT_MAX_SIZE_UNIT) {
  initializeMaxSize(maxSize);
}

ElemDDLFileAttrMaxSize::ElemDDLFileAttrMaxSize(int maxSize, ComUnits maxSizeUnit)
    : ElemDDLFileAttr(ELM_FILE_ATTR_MAX_SIZE_ELEM), isUnbounded_(FALSE), maxSizeUnit_(maxSizeUnit) {
  initializeMaxSize(maxSize);
}

// virtual destructor
ElemDDLFileAttrMaxSize::~ElemDDLFileAttrMaxSize() {}

// cast
ElemDDLFileAttrMaxSize *ElemDDLFileAttrMaxSize::castToElemDDLFileAttrMaxSize() { return this; }

//
// accessor
//

NAString ElemDDLFileAttrMaxSize::getMaxSizeUnitAsNAString() const {
  return convertSizeUnitToNAString(getMaxSizeUnit());
}

// method for building text
// virtual
NAString ElemDDLFileAttrMaxSize::getSyntax() const {
  NAString syntax = "MAXSIZE ";

  if (TRUE == isUnbounded_) {
    syntax += "UNBOUNDED";
  } else {
    syntax += LongToNAString(maxSize_);
    syntax += " ";

    switch (maxSizeUnit_) {
      case COM_BYTES:
        syntax += "";
        break;
      case COM_KBYTES:
        syntax += "K";
        break;
      case COM_MBYTES:
        syntax += "M";
        break;
      case COM_GBYTES:
        syntax += "G";
        break;
      default:
        ComASSERT(FALSE);
        break;
    }
  }

  return syntax;

}  // getSyntax

// is specified maximum size a legal value?
NABoolean ElemDDLFileAttrMaxSize::isLegalMaxSizeValue(int maxSize) const {
  if (maxSize >= 0) {
    return TRUE;
  } else {
    return FALSE;
  }
}

//
// mutator
//

void ElemDDLFileAttrMaxSize::initializeMaxSize(int maxSize) {
  if (isLegalMaxSizeValue(maxSize)) {
    maxSize_ = maxSize;
  } else {
    // Illegal size value in MAXSIZE phrase.
    *SqlParser_Diags << DgSqlCode(-3059);
    ParSetDefaultMaxSize(maxSize_, maxSizeUnit_);
  }
}

//
// helpers
//

void ParSetDefaultMaxSize(int &maxSize, ComUnits &maxSizeUnit) {
  maxSize = ElemDDLFileAttrMaxSize::DEFAULT_MAX_SIZE_IN_BYTES;
  maxSizeUnit = COM_BYTES;
}

//
// methods for tracing
//

const NAString ElemDDLFileAttrMaxSize::getText() const { return "ElemDDLFileAttrMaxSize"; }

const NAString ElemDDLFileAttrMaxSize::displayLabel1() const {
  return NAString("Is unbounded? ") + YesNo(isUnbounded());
}

const NAString ElemDDLFileAttrMaxSize::displayLabel2() const {
  char buffer[80];
  sprintf(buffer, "%d", getMaxSize());
  return NAString("Size:         ") + NAString(buffer);
}

const NAString ElemDDLFileAttrMaxSize::displayLabel3() const {
  return NAString("Size unit:    ") + getMaxSizeUnitAsNAString();
}

// -----------------------------------------------------------------------
// methods for class ElemDDLFileAttrExtents
// -----------------------------------------------------------------------

//
// constructors
//

ElemDDLFileAttrExtents::ElemDDLFileAttrExtents() : ElemDDLFileAttr(ELM_FILE_ATTR_EXTENT_ELEM) {
  ParSetDefaultExtents(priExt_, secExt_);
}

ElemDDLFileAttrExtents::ElemDDLFileAttrExtents(int priExt, int secExt) : ElemDDLFileAttr(ELM_FILE_ATTR_EXTENT_ELEM) {
  initializeExtents(priExt, secExt);
}

ElemDDLFileAttrExtents::ElemDDLFileAttrExtents(int priExt) : ElemDDLFileAttr(ELM_FILE_ATTR_EXTENT_ELEM) {
  initializePriExtent(priExt);
}

// virtual destructor
ElemDDLFileAttrExtents::~ElemDDLFileAttrExtents() {}

// cast
ElemDDLFileAttrExtents *ElemDDLFileAttrExtents::castToElemDDLFileAttrExtents() { return this; }

// is specified extent a legal value?
NABoolean ElemDDLFileAttrExtents::isLegalExtentValue(int Ext) const {
  if (Ext >= 0) {
    return TRUE;
  } else {
    return FALSE;
  }
}

//
// mutator
//

void ElemDDLFileAttrExtents::initializeExtents(int priExt, int secExt)

{
  if (isLegalExtentValue(priExt) && isLegalExtentValue(secExt)) {
    priExt_ = priExt;
    secExt_ = secExt;
  } else {
    // Illegal size value in EXTENT phrase.
    *SqlParser_Diags << DgSqlCode(-3059);
    ParSetDefaultExtents(priExt_, secExt_);
  }
}

void ElemDDLFileAttrExtents::initializePriExtent(int priExt) {
  if (isLegalExtentValue(priExt)) {
    priExt_ = priExt;
    secExt_ = ElemDDLFileAttrExtents::DEFAULT_SEC_EXTENT;
  } else {
    // Illegal size value in EXTENT phrase.
    *SqlParser_Diags << DgSqlCode(-3059);
    ParSetDefaultExtents(priExt_, secExt_);
  }
}

//
// helpers
//

void ParSetDefaultExtents(int &priExt, int &secExt) {
  priExt = ElemDDLFileAttrExtents::DEFAULT_PRI_EXTENT;
  secExt = ElemDDLFileAttrExtents::DEFAULT_SEC_EXTENT;
}

//
// methods for tracing - Do we need this for Extents ? Verify and then add.
//

// -----------------------------------------------------------------------
// methods for class ElemDDLFileAttrMaxExtents
// -----------------------------------------------------------------------

// Constructors

ElemDDLFileAttrMaxExtents::ElemDDLFileAttrMaxExtents() : ElemDDLFileAttr(ELM_FILE_ATTR_MAXEXTENTS_ELEM) {
  ParSetDefaultMaxExtents(maxExt_);
}

ElemDDLFileAttrMaxExtents::ElemDDLFileAttrMaxExtents(int maxExt) : ElemDDLFileAttr(ELM_FILE_ATTR_MAXEXTENTS_ELEM) {
  initializeMaxExtents(maxExt);
}

// virtual destructor
ElemDDLFileAttrMaxExtents::~ElemDDLFileAttrMaxExtents() {}

// cast
ElemDDLFileAttrMaxExtents *ElemDDLFileAttrMaxExtents::castToElemDDLFileAttrMaxExtents() { return this; }

// is specified extent a legal value?
NABoolean ElemDDLFileAttrMaxExtents::isLegalMaxExtentValue(int maxExt) const {
  if (maxExt >= 0) {
    return TRUE;
  } else {
    return FALSE;
  }
}

//
// mutator
//

void ElemDDLFileAttrMaxExtents::initializeMaxExtents(int maxExt) {
  if (isLegalMaxExtentValue(maxExt)) {
    maxExt_ = maxExt;
  } else {
    // Illegal size value in MAXEXTENT phrase.
    *SqlParser_Diags << DgSqlCode(-3059);
    ParSetDefaultMaxExtents(maxExt_);
  }
}

//
// helpers
//

void ParSetDefaultMaxExtents(int &maxExt) { maxExt = ElemDDLFileAttrMaxExtents::DEFAULT_MAX_EXTENT; }

//
// methods for tracing - Do we need this for Extents ? Verify and then add.
//

// -----------------------------------------------------------------------
// methods for class ElemDDLFileAttrUID
// -----------------------------------------------------------------------

// Constructors

ElemDDLFileAttrUID::ElemDDLFileAttrUID(long UID) : ElemDDLFileAttr(ELM_FILE_ATTR_UID_ELEM), UID_(UID) {}

// virtual destructor
ElemDDLFileAttrUID::~ElemDDLFileAttrUID() {}

// cast
ElemDDLFileAttrUID *ElemDDLFileAttrUID::castToElemDDLFileAttrUID() { return this; }

// -----------------------------------------------------------------------
// methods for class ElemDDLFileAttrRowFormat
// -----------------------------------------------------------------------

// Constructors

ElemDDLFileAttrRowFormat::ElemDDLFileAttrRowFormat(ElemDDLFileAttrRowFormat::ERowFormat rowFormat)
    : ElemDDLFileAttr(ELM_FILE_ATTR_ROW_FORMAT_ELEM), eRowFormat_(rowFormat) {}

// virtual destructor
ElemDDLFileAttrRowFormat::~ElemDDLFileAttrRowFormat() {}

// cast
ElemDDLFileAttrRowFormat *ElemDDLFileAttrRowFormat::castToElemDDLFileAttrRowFormat() { return this; }

//----------------------------------------------------------------------------
// method for building text
// virtual
NAString ElemDDLFileAttrRowFormat::getSyntax() const { return ""; }  // getSyntax()

// -----------------------------------------------------------------------
// methods for class ElemDDLFileAttrPOSNumPartns
// -----------------------------------------------------------------------

ElemDDLFileAttrPOSNumPartns::ElemDDLFileAttrPOSNumPartns(ComSInt32 numPartns)
    : ElemDDLFileAttr(ELM_FILE_ATTR_POS_NUM_PARTNS_ELEM) {
  if (numPartns <= 0) {
    // Illegal size value in NUMBER OF PARTITIONS phrase.
    // *SqlParser_Diags << DgSqlCode(-3057) << DgInt0(COM_MAX_MAXEXTENTS);
  }

  posNumPartns_ = numPartns;
}

// virtual destructor
ElemDDLFileAttrPOSNumPartns::~ElemDDLFileAttrPOSNumPartns() {}

// cast
ElemDDLFileAttrPOSNumPartns *ElemDDLFileAttrPOSNumPartns::castToElemDDLFileAttrPOSNumPartns() { return this; }

//
// trace
//

const NAString ElemDDLFileAttrPOSNumPartns::getText() const { return "ElemDDLFileAttrPOSNumPartns"; }

// -----------------------------------------------------------------------
// methods for class ElemDDLFileAttrPOSDiskPool
// -----------------------------------------------------------------------

//
// constructors
//

ElemDDLFileAttrPOSDiskPool::ElemDDLFileAttrPOSDiskPool(ComSInt32 diskPool, ComSInt32 numDiskPools)
    : ElemDDLFileAttr(ELM_FILE_ATTR_POS_DISK_POOL_ELEM) {
  if (diskPool <= 0) {
    // Illegal size value in DISK POOL phrase.
    *SqlParser_Diags << DgSqlCode(-3419) << DgInt0(diskPool);
  }

  if (numDiskPools <= 0) {
    // Illegal size value in NUM DISK POOLS phrase.
    *SqlParser_Diags << DgSqlCode(-3420) << DgInt0(numDiskPools);
  }

  // Fix for CR 5234
  if (diskPool == MAX_COMSINT32)
    posDiskPool_ = 0;
  else
    posDiskPool_ = diskPool;

  if (numDiskPools == MAX_COMSINT32)
    posNumDiskPools_ = 0;
  else
    posNumDiskPools_ = numDiskPools;
}

// virtual destructor
ElemDDLFileAttrPOSDiskPool::~ElemDDLFileAttrPOSDiskPool() {}

// cast
ElemDDLFileAttrPOSDiskPool *ElemDDLFileAttrPOSDiskPool::castToElemDDLFileAttrPOSDiskPool() { return this; }

//
// trace
//

const NAString ElemDDLFileAttrPOSDiskPool::getText() const { return "ElemDDLFileAttrPOSDiskPool"; }

// -----------------------------------------------------------------------
// methods for class ElemDDLFileAttrPOSTableSize
// -----------------------------------------------------------------------

//
// constructors
//

ElemDDLFileAttrPOSTableSize::ElemDDLFileAttrPOSTableSize(ComSInt32 initialTableSize, ComSInt32 maxTableSize,
                                                         double numRows, ComSInt32 indexLevels, ComSInt64 partnEOF)
    : ElemDDLFileAttr(ELM_FILE_ATTR_POS_TABLE_SIZE_ELEM) {
  posInitialTableSize_ = initialTableSize;
  posMaxTableSize_ = maxTableSize;
  numRows_ = numRows;
  indexLevels_ = indexLevels;
  partnEOF_ = partnEOF;
}

// virtual destructor
ElemDDLFileAttrPOSTableSize::~ElemDDLFileAttrPOSTableSize() {}

// cast
ElemDDLFileAttrPOSTableSize *ElemDDLFileAttrPOSTableSize::castToElemDDLFileAttrPOSTableSize() { return this; }

//
// trace
//

const NAString ElemDDLFileAttrPOSTableSize::getText() const { return "ElemDDLFileAttrPOSTableSize"; }

// -----------------------------------------------------------------------
// methods for class ElemDDLFileAttrPOSIgnore
// -----------------------------------------------------------------------

//
// constructors
//

ElemDDLFileAttrPOSIgnore::ElemDDLFileAttrPOSIgnore(ComBoolean posIgnore)
    : ElemDDLFileAttr(ELM_FILE_ATTR_POS_IGNORE_ELEM) {
  posIgnore_ = posIgnore;
}

// virtual destructor
ElemDDLFileAttrPOSIgnore::~ElemDDLFileAttrPOSIgnore() {}

// cast
ElemDDLFileAttrPOSIgnore *ElemDDLFileAttrPOSIgnore::castToElemDDLFileAttrPOSIgnore() { return this; }

//
// trace
//

const NAString ElemDDLFileAttrPOSIgnore::getText() const { return "ElemDDLFileAttrPOSIgnore"; }

// -----------------------------------------------------------------------
// methods for class ElemDDLFileAttrNoLabelUpdate
// -----------------------------------------------------------------------

// virtual destructor
ElemDDLFileAttrNoLabelUpdate::~ElemDDLFileAttrNoLabelUpdate() {}

// cast
ElemDDLFileAttrNoLabelUpdate *ElemDDLFileAttrNoLabelUpdate::castToElemDDLFileAttrNoLabelUpdate() { return this; }

// trace

const NAString ElemDDLFileAttrNoLabelUpdate::getText() const { return "ElemDDLFileAttrNoLabelUpdate"; }

const NAString ElemDDLFileAttrNoLabelUpdate::displayLabel1() const {
  return NAString("Is no-label-update? ") + YesNo(getIsNoLabelUpdate());
}

// -----------------------------------------------------------------------
// methods for class ElemDDLFileAttrOwner
// -----------------------------------------------------------------------

// virtual destructor
ElemDDLFileAttrOwner::~ElemDDLFileAttrOwner() {}

// cast
ElemDDLFileAttrOwner *ElemDDLFileAttrOwner::castToElemDDLFileAttrOwner() { return this; }

// trace

const NAString ElemDDLFileAttrOwner::getText() const { return "ElemDDLFileAttrOwner"; }

const NAString ElemDDLFileAttrOwner::displayLabel1() const { return NAString("Is owner-specified? ") + objectOwner_; }

//----------------------------------------------------------------------------
// method for building text
// virtual
NAString ElemDDLFileAttrOwner::getSyntax() const {
  NAString syntax("BY ");
  syntax += getOwner();
  return syntax;
}  // getSyntax

ElemDDLLobStorageOptions::ElemDDLLobStorageOptions(NAList<LobStorageOption *> *lobStorageOptions, CollHeap *heap)
    : ElemDDLNode(ELM_LOB_STORAGE_OPTIONS_ELEM), lobStorageOptions_(heap) {
  inlineDataMaxBytes_ = 0;
  inlineDataMaxChars_ = 0;
  hbaseDataMaxLen_ = 0;
  chunksColMaxLen_ = 0;
  numChunksPartitions_ = 0;

  if (lobStorageOptions) {
    for (CollIndex i = 0; i < lobStorageOptions->entries(); i++) {
      LobStorageOption *hbo =
          new (heap) LobStorageOption(((*lobStorageOptions)[i])->key(), ((*lobStorageOptions)[i])->val());

      lobStorageOptions_.insert(hbo);
    }
  }
}

short ElemDDLLobStorageOptions::synthesize(NAString &invalidOption) {
  inlineDataMaxBytes_ = (long)CmpCommon::getDefaultNumeric(TRAF_LOB_INLINED_DATA_MAXBYTES);
  inlineDataMaxChars_ = (long)CmpCommon::getDefaultNumeric(TRAF_LOB_INLINED_DATA_MAXCHARS);
  hbaseDataMaxLen_ = (long)CmpCommon::getDefaultNumeric(TRAF_LOB_HBASE_DATA_MAXLEN_DDL);
  if (hbaseDataMaxLen_ == -1) hbaseDataMaxLen_ = LOB_HBASE_DATA_MAXLEN_VAL;
  chunksColMaxLen_ = (long)CmpCommon::getDefaultNumeric(TRAF_LOB_DATA_IN_HBASE_COL_LEN);
  numChunksPartitions_ = 0;

  for (CollIndex i = 0; i < lobStorageOptions_.entries(); i++) {
    if (((lobStorageOptions_)[i])->key() == "INLINE_DATA_MAXBYTES")
      inlineDataMaxBytes_ = ((lobStorageOptions_)[i])->val();
    else if (((lobStorageOptions_)[i])->key() == "INLINE_DATA_MAXCHARS")
      inlineDataMaxChars_ = ((lobStorageOptions_)[i])->val();
    else if (((lobStorageOptions_)[i])->key() == "HBASE_DATA_MAXLEN")
      hbaseDataMaxLen_ = ((lobStorageOptions_)[i])->val();
    else if (((lobStorageOptions_)[i])->key() == "CHUNKS_COL_MAXLEN")
      chunksColMaxLen_ = ((lobStorageOptions_)[i])->val();
    else if (((lobStorageOptions_)[i])->key() == "NUM_CHUNKS_PARTITIONS")
      numChunksPartitions_ = ((lobStorageOptions_)[i])->val();
    else {
      invalidOption = ((lobStorageOptions_)[i])->key();

      return -1;
    }
  }

  return 0;
}

//
//
// End of File
//
