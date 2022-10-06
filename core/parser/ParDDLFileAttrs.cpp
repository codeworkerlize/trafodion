/* -*-C++-*-

 *****************************************************************************
 *
 * File:         ParDDLFileAttrs.C
 * Description:  methods for class ParDDLFileAttrs and classes derived
 *               from class ParDDLFileAttrs
 *
 * Created:      5/30/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "ParDDLFileAttrs.h"

#include "AllElemDDLFileAttr.h"
#include "ElemDDLNode.h"
#include "ParDDLFileAttrsAlterIndex.h"
#include "ParDDLFileAttrsAlterTable.h"
#include "ParDDLFileAttrsCreateIndex.h"
#include "ParDDLFileAttrsCreateTable.h"
#include "common/BaseTypes.h"
#include "common/ComASSERT.h"
#include "common/ComOperators.h"
#include "common/ComUnits.h"
#include "export/ComDiags.h"
#define SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#include "common/NAString.h"
#include "parser/SqlParserGlobals.h"
#include "seabed/ms.h"

// -----------------------------------------------------------------------
// methods for class ParDDLFileAttrs
// -----------------------------------------------------------------------

// virtual destructor
ParDDLFileAttrs::~ParDDLFileAttrs() {}

// mutator
void ParDDLFileAttrs::copy(const ParDDLFileAttrs &rhs) { fileAttrsNodeType_ = rhs.fileAttrsNodeType_; }

// -----------------------------------------------------------------------
// methods for class ParDDLFileAttrsAlterIndex
// -----------------------------------------------------------------------

//
// constructor
//
ParDDLFileAttrsAlterIndex::ParDDLFileAttrsAlterIndex(ParDDLFileAttrs::fileAttrsNodeTypeEnum fileAttrsNodeType)
    : ParDDLFileAttrs(fileAttrsNodeType) {
  initializeDataMembers();
}

//
// copy constructor
//
ParDDLFileAttrsAlterIndex::ParDDLFileAttrsAlterIndex(const ParDDLFileAttrsAlterIndex &alterIndexFileAttributes) {
  copy(alterIndexFileAttributes);
}

//
// virtual destructor
//
ParDDLFileAttrsAlterIndex::~ParDDLFileAttrsAlterIndex() {}

//
// assignment operator
//
ParDDLFileAttrsAlterIndex &ParDDLFileAttrsAlterIndex::operator=(const ParDDLFileAttrsAlterIndex &rhs) {
  if (this EQU & rhs) {
    return *this;
  }

  copy(rhs);

  return *this;

}  // ParDDLFileAttrsAlterIndex::operator=()

//
// accessors
//

NAString ParDDLFileAttrsAlterIndex::getMaxSizeUnitAsNAString() const {
  ElemDDLFileAttrMaxSize maxSizeFileAttr(getMaxSize(), getMaxSizeUnit());
  return maxSizeFileAttr.getMaxSizeUnitAsNAString();
}

//
// mutators
//

void ParDDLFileAttrsAlterIndex::copy(const ParDDLFileAttrsAlterIndex &rhs) {
  ParDDLFileAttrs::copy(rhs);

  // ALLOCATE
  isAllocateSpec_ = rhs.isAllocateSpec_;
  extentsToAllocate_ = rhs.extentsToAllocate_;

  // [ NO ] AUDITCOMPRESS
  isAuditCompressSpec_ = rhs.isAuditCompressSpec_;
  isAuditCompress_ = rhs.isAuditCompress_;

  // [ NO ] BUFFERED
  isBufferedSpec_ = rhs.isBufferedSpec_;
  isBuffered_ = rhs.isBuffered_;

  // [ NO ] CLEARONPURGE
  isClearOnPurgeSpec_ = rhs.isClearOnPurgeSpec_;
  isClearOnPurge_ = rhs.isClearOnPurge_;

  // COMPRESSION TYPE { NONE | HARDWARE | SOFTWARE }
  isCompressionTypeSpec_ = rhs.isCompressionTypeSpec_;
  compressionType_ = rhs.compressionType_;

  // DEALLOCATE
  isDeallocateSpec_ = rhs.isDeallocateSpec_;

  // MAXSIZE
  isMaxSizeSpec_ = rhs.isMaxSizeSpec_;
  isMaxSizeUnbounded_ = rhs.isMaxSizeUnbounded_;
  maxSize_ = rhs.maxSize_;
  maxSizeUnit_ = rhs.maxSizeUnit_;

  // EXTENT
  isExtentSpec_ = rhs.isExtentSpec_;
  priExt_ = rhs.priExt_;
  secExt_ = rhs.secExt_;

  // MAXEXTENTs
  isMaxExtentSpec_ = rhs.isMaxExtentSpec_;
  maxExt_ = rhs.maxExt_;

  // NO LABEL UPDATE
  isNoLabelUpdateSpec_ = rhs.isNoLabelUpdateSpec_;
  noLabelUpdate_ = rhs.noLabelUpdate_;

}  // ParDDLFileAttrsAlterIndex::copy()

//
// private method invoked by the constructor to
// initialize data members in the class with the
// appropriate default values
//
void ParDDLFileAttrsAlterIndex::initializeDataMembers() {
  resetAllIsSpecDataMembers();

  // ALLOCATE
  //
  //   The following data members have no meanings when the
  //   Allocate phrase is not specified.
  //
  extentsToAllocate_ = ElemDDLFileAttrAllocate::DEFAULT_EXTENTS_TO_ALLOCATE;

  // [ NO ] AUDITCOMPRESS
  //
  //   The following data member has no meaning when the
  //   [No]AuditCompress phrase does not appear.
  //
  isAuditCompress_ = TRUE;

  // [ NO ] BUFFERED
  //
  //   The following data member has no meaning when the
  //   [No]Buffered phrase does not appear.
  //
  isBuffered_ = TRUE;

  // [ NO ] CLEARONPURGE
  //
  //   The following data member has no meaning when the
  //   [No]ClearOnPurge phrase does not appear.
  //
  isClearOnPurge_ = FALSE;

  // COMPRESSION TYPE { NONE | HARDWARE | SOFTWARE }
  compressionType_ = COM_NO_COMPRESSION;

  // DEALLOCATE
  //
  //   no other data members besides isDeallocateSpec_
  //

  // MAXSIZE
  //
  //   The following data members have no meanings when the
  //   MaxSize phrase does not appear.
  //
  isMaxSizeUnbounded_ = FALSE;
  ParSetDefaultMaxSize(maxSize_, maxSizeUnit_);

  // EXTENT
  //
  //   The following data members have no meanings when the
  //   Extent phrase does not appear.
  //

  ParSetDefaultExtents(priExt_, secExt_);

  // MAXEXTENTS
  //
  //   The following data members have no meanings when the
  //   MaxExtents phrase does not appear.
  //
  ParSetDefaultMaxExtents(maxExt_);

  // NO LABEL UPDATE
  noLabelUpdate_ = FALSE;
}  // ParDDLFileAttrsAlterIndex::initializeDataMembers()

//
// reset all is...Spec_ data members
//
void ParDDLFileAttrsAlterIndex::resetAllIsSpecDataMembers() {
  // ALLOCATE
  isAllocateSpec_ = FALSE;

  // [ NO ] AUDITCOMPRESS
  isAuditCompressSpec_ = FALSE;

  // BUFFERED
  isBufferedSpec_ = FALSE;

  // [ NO ] CLEARONPURGE
  isClearOnPurgeSpec_ = FALSE;

  // COMPRESSION
  isCompressionTypeSpec_ = FALSE;

  // DEALLOCATE
  isDeallocateSpec_ = FALSE;

  // MAXSIZE
  isMaxSizeSpec_ = FALSE;

  // EXTENT
  isExtentSpec_ = FALSE;

  // MAXEXTENTS
  isMaxExtentSpec_ = FALSE;

  // NO LABEL UPDATE
  isNoLabelUpdateSpec_ = FALSE;
}  // ParDDLFileAttrsAlterIndex::resetAllIsSpecDataMembers()

//
// set a file attribute
//
void ParDDLFileAttrsAlterIndex::setFileAttr(ElemDDLFileAttr *pFileAttr) {
  ComASSERT(pFileAttr NEQ NULL);

  switch (pFileAttr->getOperatorType()) {
    case ELM_FILE_ATTR_ALLOCATE_ELEM:
      if (isDeallocateSpec_) {
        // ALLOCATE and DEALLOCATE phrases can not appear in the
        // same ALTER INDEX statement.
        *SqlParser_Diags << DgSqlCode(-3068);
      }
      if (isAllocateSpec_) {
        // Duplicate ALLOCATE phrases.
        *SqlParser_Diags << DgSqlCode(-3081);
      }
      isAllocateSpec_ = TRUE;
      ComASSERT(pFileAttr->castToElemDDLFileAttrAllocate() NEQ NULL);
      {
        ElemDDLFileAttrAllocate *pAlloc = pFileAttr->castToElemDDLFileAttrAllocate();
        extentsToAllocate_ = pAlloc->getExtentsToAllocate();
      }
      break;

    case ELM_FILE_ATTR_AUDIT_ELEM:
      //
      // The grammar productions allow the [no]audit phrase
      // to appear within an Alter Index ... Attribute(s)
      // statement (syntactically).  Enforces the restriction
      // by using semantic actions.
      //
      // The [NO]AUDIT clause is not supported.
      *SqlParser_Diags << DgSqlCode(-3070);
      break;

    case ELM_FILE_ATTR_AUDIT_COMPRESS_ELEM:
      if (isAuditCompressSpec_) {
        // Duplicate [NO]AUDITCOMPRESS phrases.
        *SqlParser_Diags << DgSqlCode(-3071);
      }
      ComASSERT(pFileAttr->castToElemDDLFileAttrAuditCompress() NEQ NULL);
      isAuditCompress_ = pFileAttr->castToElemDDLFileAttrAuditCompress()->getIsAuditCompress();
      isAuditCompressSpec_ = TRUE;
      break;

    case ELM_FILE_ATTR_BLOCK_SIZE_ELEM:
      //
      // The grammar productions allow the Blocksize phrase
      // to appear within an Alter Index statement (syntactically).
      // Enforces the restriction by using semantic actions.
      //
      // BLOCKSIZE phrase not allowed "
      // in an Alter Index ... Attribute(s) statement.
      *SqlParser_Diags << DgSqlCode(-3072) << DgString0("BLOCKSIZE") << DgString1("INDEX");
      break;

    case ELM_FILE_ATTR_BUFFERED_ELEM:
      if (isBufferedSpec_) {
        // Duplicate [NO]BUFFERED phrases.
        *SqlParser_Diags << DgSqlCode(-3085);
      }
      ComASSERT(pFileAttr->castToElemDDLFileAttrBuffered() NEQ NULL);
      isBuffered_ = pFileAttr->castToElemDDLFileAttrBuffered()->getIsBuffered();
      isBufferedSpec_ = TRUE;
      break;

    case ELM_FILE_ATTR_CLEAR_ON_PURGE_ELEM:
      if (isClearOnPurgeSpec_) {
        // Duplicate [NO]CLEARONPURGE phrases.
        *SqlParser_Diags << DgSqlCode(-3086);
      }
      ComASSERT(pFileAttr->castToElemDDLFileAttrClearOnPurge() NEQ NULL);
      isClearOnPurge_ = pFileAttr->castToElemDDLFileAttrClearOnPurge()->getIsClearOnPurge();
      isClearOnPurgeSpec_ = TRUE;
      break;

    case ELM_FILE_ATTR_COMPRESSION_ELEM:
      *SqlParser_Diags << DgSqlCode(-3282);
      break;

    case ELM_FILE_ATTR_D_COMPRESS_ELEM:
      //
      // The grammar productions allow the [no]dcompress phrase
      // to appear within an Alter Index statement (syntactically).
      // Enforces the restriction by using semantic actions.
      //
      // [NO]DCOMPRESS phrase not allowed in an Alter Index ... Attribute(s)
      // statement.
      *SqlParser_Diags << DgSqlCode(-3075);
      break;

    case ELM_FILE_ATTR_DEALLOCATE_ELEM:
      if (isAllocateSpec_) {
        // ALLOCATE and DEALLOCATE phrases can not appear in the same
        // ALTER INDEX statement.
        *SqlParser_Diags << DgSqlCode(-3068);
      }
      if (isDeallocateSpec_) {
        // Duplicate DEALLOCATE phrases.
        *SqlParser_Diags << DgSqlCode(-3076);
      }
      isDeallocateSpec_ = TRUE;
      break;

    case ELM_FILE_ATTR_I_COMPRESS_ELEM:
      //
      // The grammar productions allow the [no]icompress phrase
      // to appear within an Alter Index statement (syntactically).
      // Enforces the restriction by using semantic actions.
      //
      // [NO]ICOMPRESS phrase not allowed in an Alter Index ... Attribute(s)
      // statement.
      *SqlParser_Diags << DgSqlCode(-3077);
      break;

    case ELM_FILE_ATTR_EXTENT_ELEM:
      // DOnt allow EXTENT in ALTER statements
      *SqlParser_Diags << DgSqlCode(-3194);
      break;

    case ELM_FILE_ATTR_MAXEXTENTS_ELEM:
      if (isMaxExtentSpec_) {
        // Duplicate MAXEXTENTS phrases.
        *SqlParser_Diags << DgSqlCode(-3079);
      }
      isMaxExtentSpec_ = TRUE;
      ComASSERT(pFileAttr->castToElemDDLFileAttrMaxExtents() NEQ NULL);
      {
        ElemDDLFileAttrMaxExtents *pMaxExtents = pFileAttr->castToElemDDLFileAttrMaxExtents();
        /////////////////////////////////////////////////////////////////
        // error checking while specifying the MAXEXTENTS attribute clause
        /////////////////////////////////////////////////////////////////
        int maxext = pMaxExtents->getMaxExtents();
        if ((maxext <= 0) || (maxext > COM_MAX_MAXEXTENTS)) {
          *SqlParser_Diags << DgSqlCode(-3191);
        } else {
          maxExt_ = pMaxExtents->getMaxExtents();
        }
      }
      break;

    case ELM_FILE_ATTR_ROW_FORMAT_ELEM:
      //
      // The grammar productions allow the Row Format phrase
      // to appear within an Alter Index statement (syntactically).
      // Enforces the restriction by using semantic actions.
      //
      // The Row FORMAT clause is not allowed in
      // the ALTER INDEX ... ATTRIBUTE(s) statement.
      *SqlParser_Diags << DgSqlCode(-3072) << DgString0("Row FORMAT") << DgString1("INDEX");
      break;

    case ELM_FILE_ATTR_NO_LABEL_UPDATE_ELEM:
      if (isNoLabelUpdateSpec_) {
        // Duplicate NO LABEL UPDATE phrases.
        *SqlParser_Diags << DgSqlCode(-3099);
      }
      ComASSERT(pFileAttr->castToElemDDLFileAttrNoLabelUpdate() NEQ NULL);
      noLabelUpdate_ = pFileAttr->castToElemDDLFileAttrNoLabelUpdate()->getIsNoLabelUpdate();
      isNoLabelUpdateSpec_ = TRUE;
      break;

    case ELM_FILE_ATTR_NAMESPACE_ELEM:
      //
      // base table's namespace will be used for index.
      // Cannot be explicitly specified.
      *SqlParser_Diags << DgSqlCode(-3242)
                       << DgString0(" Base table namespace will be used for index. Cannot be explicitly specified.");
      break;

    case ELM_FILE_ATTR_ENCRYPT_ELEM:
      //
      // base table's encryption will be used for index.
      // Cannot be explicitly specified.
      *SqlParser_Diags << DgSqlCode(-3242)
                       << DgString0(" Base table encryption will be used for index. Cannot be explicitly specified.");
      break;

    case ELM_FILE_ATTR_STORED_DESC_ELEM:
      //
      // The STORED DESC clause is not allowed in
      // the ALTER INDEX ... ATTRIBUTE(s) statement.
      *SqlParser_Diags << DgSqlCode(-3072) << DgString0("STORED DESC") << DgString1("INDEX");
      break;

    case ELM_FILE_ATTR_INCR_BACKUP_ELEM:
      //
      // The INCREMENTAL clause is not allowed in
      // the ALTER INDEX ... ATTRIBUTE(s) statement.
      *SqlParser_Diags << DgSqlCode(-3072) << DgString0("INCREMENTAL") << DgString1("INDEX");
      break;

    default:
      *SqlParser_Diags << DgSqlCode(-3072) << DgString0("SPECIFIED") << DgString1("INDEX");
      break;
  }
}  // ParDDLFileAttrsAlterIndex::setFileAttr()

//
// trace
//

NATraceList ParDDLFileAttrsAlterIndex::getDetailInfo() const {
  NAString detailText;
  NATraceList detailTextList;

  // ALLOCATE

  if (isAllocateSpecified()) {
    detailText = "extents to allocate: ";
    // cast to long needed to suppress warning
    detailText += LongToNAString((int)getExtentsToAllocate());
    detailTextList.append(detailText);
  } else {
    detailTextList.append("allocate not spec");
  }

  // [ NO ] AUDITCOMPRESS

  if (isAuditCompressSpecified()) {
    detailText = "audcompr?      ";
    detailText += YesNo(getIsAuditCompress());
    detailTextList.append(detailText);
  } else {
    detailTextList.append("[no]auditcompress not spec");
  }

  // [ NO ] BUFFERED

  if (isBufferedSpecified()) {
    detailText = "buffered?      ";
    detailText += YesNo(getIsBuffered());
    detailTextList.append(detailText);
  } else {
    detailTextList.append("[no]buffered not spec");
  }

  // [ NO ] CLEARONPURGE

  if (isClearOnPurgeSpecified()) {
    detailText = "clrpurge?      ";
    detailText += YesNo(getIsClearOnPurge());
    detailTextList.append(detailText);
  } else {
    detailTextList.append("[no]clearonpurge not spec");
  }

  // DEALLOCATE

  if (isDeallocateSpecified()) {
    detailTextList.append("deallocate specified");
  } else {
    detailTextList.append("deallocate not spec");
  }

  // MAXSIZE

  if (isMaxSizeSpecified()) {
    if (isMaxSizeUnbounded()) {
      detailText = "maxsizunbound? ";
      detailText += YesNo(isMaxSizeUnbounded());
      detailTextList.append(detailText);
    } else {
      detailText = "maximum size:  ";
      detailText += LongToNAString((int)getMaxSize());
      detailTextList.append(detailText);

      detailText = "max size unit: ";
      detailText += getMaxSizeUnitAsNAString();
      detailTextList.append(detailText);
    }
  } else {
    detailTextList.append("maxsize not spec");
  }

  // EXTENT

  if (isExtentSpecified()) {
    detailText = "Extent Specified";
    detailTextList.append(detailText);
  } else {
    detailTextList.append("Extent Unspecified!");
  }

  // MAXEXTENTS

  if (isMaxExtentSpecified()) {
    detailText = "MaxExtents Specified";
    detailTextList.append(detailText);
  } else {
    detailTextList.append("MaxExtents Unspecified!");
  }

  return detailTextList;

}  // ParDDLFileAttrsAlterIndex::getDetailInfo()

// -----------------------------------------------------------------------
// methods for class ParDDLFileAttrsAlterTable
// -----------------------------------------------------------------------

//
// constructor
//
ParDDLFileAttrsAlterTable::ParDDLFileAttrsAlterTable(ParDDLFileAttrs::fileAttrsNodeTypeEnum fileAttrsNodeType)
    : ParDDLFileAttrs(fileAttrsNodeType) {
  initializeDataMembers();
}

//
// copy constructor
//
ParDDLFileAttrsAlterTable::ParDDLFileAttrsAlterTable(const ParDDLFileAttrsAlterTable &alterTableFileAttributes) {
  copy(alterTableFileAttributes);
}

//
// virtual destructor
//
ParDDLFileAttrsAlterTable::~ParDDLFileAttrsAlterTable() {}

//
// assignment operator
//
ParDDLFileAttrsAlterTable &ParDDLFileAttrsAlterTable::operator=(const ParDDLFileAttrsAlterTable &rhs) {
  if (this EQU & rhs) {
    return *this;
  }

  copy(rhs);

  return *this;

}  // ParDDLFileAttrsAlterTable::operator=()

//
// accessors
//

NAString ParDDLFileAttrsAlterTable::getMaxSizeUnitAsNAString() const {
  ElemDDLFileAttrMaxSize maxSizeFileAttr(getMaxSize(), getMaxSizeUnit());
  return maxSizeFileAttr.getMaxSizeUnitAsNAString();
}

//
// mutators
//

void ParDDLFileAttrsAlterTable::copy(const ParDDLFileAttrsAlterTable &rhs) {
  ParDDLFileAttrs::copy(rhs);

  // ALLOCATE
  isAllocateSpec_ = rhs.isAllocateSpec_;
  extentsToAllocate_ = rhs.extentsToAllocate_;

  // [ NO ] AUDIT
  isAuditSpec_ = rhs.isAuditSpec_;
  isAudit_ = rhs.isAudit_;

  // [ NO ] AUDITCOMPRESS
  isAuditCompressSpec_ = rhs.isAuditCompressSpec_;
  isAuditCompress_ = rhs.isAuditCompress_;

  // [ NO ] BUFFERED
  isBufferedSpec_ = rhs.isBufferedSpec_;
  isBuffered_ = rhs.isBuffered_;

  // [ NO ] CLEARONPURGE
  isClearOnPurgeSpec_ = rhs.isClearOnPurgeSpec_;
  isClearOnPurge_ = rhs.isClearOnPurge_;

  // COMPRESSION
  isCompressionTypeSpec_ = rhs.isCompressionTypeSpec_;
  compressionType_ = rhs.compressionType_;

  // DEALLOCATE
  isDeallocateSpec_ = rhs.isDeallocateSpec_;

  // MAXSIZE
  isMaxSizeSpec_ = rhs.isMaxSizeSpec_;
  isMaxSizeUnbounded_ = rhs.isMaxSizeUnbounded_;
  maxSize_ = rhs.maxSize_;
  maxSizeUnit_ = rhs.maxSizeUnit_;

  // EXTENT
  isExtentSpec_ = rhs.isExtentSpec_;
  priExt_ = rhs.priExt_;
  secExt_ = rhs.secExt_;

  // MAXEXTENTs
  isMaxExtentSpec_ = rhs.isMaxExtentSpec_;
  maxExt_ = rhs.maxExt_;

  // RANGELOG
  isRangeLogSpec_ = rhs.isRangeLogSpec_;
  rangelogType_ = rhs.rangelogType_;

  // LOCKONREFRESH
  isLockOnRefreshSpec_ = rhs.isLockOnRefreshSpec_;
  isLockOnRefresh_ = rhs.isLockOnRefresh_;

  // INSERTLOG
  isInsertLogSpec_ = rhs.isInsertLogSpec_;
  isInsertLog_ = rhs.isInsertLog_;

  // MVS ALLOWED
  isMvsAllowedSpec_ = rhs.isMvsAllowedSpec_;
  mvsAllowedType_ = rhs.mvsAllowedType_;

  isXnReplSpec_ = rhs.isXnReplSpec_;
  xnRepl_ = rhs.xnRepl_;

  isIncrBackupSpec_ = rhs.isIncrBackupSpec_;
  incrBackupEnabled_ = rhs.incrBackupEnabled_;

  isReadOnlySpec_ = rhs.isReadOnlySpec_;
  readOnlyEnabled_ = rhs.readOnlyEnabled_;

}  // ParDDLFileAttrsAlterTable::copy()

//
// private method invoked by the constructor to
// initialize data members in the class with the
// appropriate default values
//
void ParDDLFileAttrsAlterTable::initializeDataMembers() {
  resetAllIsSpecDataMembers();

  // ALLOCATE
  //
  //   The following data members have no meanings when the
  //   Allocate phrase is not specified.
  //
  extentsToAllocate_ = ElemDDLFileAttrAllocate::DEFAULT_EXTENTS_TO_ALLOCATE;

  // [ NO ] AUDIT
  //
  //   The following data member has no meaning when the
  //   [No]Audit phrase does not appear.
  //
  isAudit_ = TRUE;

  // [ NO ] AUDITCOMPRESS
  //
  //   The following data member has no meaning when the
  //   [No]AuditCompress phrase does not appear.
  //
  isAuditCompress_ = TRUE;

  // [ NO ] BUFFERED
  //
  //   The following data member has no meaning when the
  //   [No]Buffered phrase does not appear.
  //
  isBuffered_ = TRUE;

  // [ NO ] CLEARONPURGE
  //
  //   The following data member has no meaning when the
  //   [No]ClearOnPurge phrase does not appear.
  //
  isClearOnPurge_ = FALSE;

  // COMPRESSION TYPE { NONE | HARDWARE | SOFTWARE }
  compressionType_ = COM_NO_COMPRESSION;

  // DEALLOCATE
  //
  //   no other data members besides isDeallocateSpec_
  //

  // MAXSIZE
  //
  //   The following data members have no meanings when the
  //   MaxSize phrase does not appear.
  //
  isMaxSizeUnbounded_ = FALSE;
  ParSetDefaultMaxSize(maxSize_, maxSizeUnit_);

  // EXTENT
  //
  // KS uncommented this while implementing EXTENTS JUNE 2002

  ParSetDefaultExtents(priExt_, secExt_);

  // MAXEXTENTS
  //
  //  KS uncommented this while implementing EXTENTS JUNE 2002

  ParSetDefaultMaxExtents(maxExt_);

  // RANGELOG
  //
  //   The following data member has no meaning when the
  //   RANGELOG phrase does not appear.
  //
  rangelogType_ = COM_NO_RANGELOG;

  // LOCKONREFRESH
  //

  isLockOnRefresh_ = TRUE;

  // INSERTLOG
  //

  isInsertLog_ = FALSE;

  // MVS ALLOWED
  //

  mvsAllowedType_ = COM_NO_MVS_ALLOWED;

  xnRepl_ = COM_REPL_NONE;

  incrBackupEnabled_ = FALSE;

}  // ParDDLFileAttrsAlterTable::initializeDataMembers()

//
// reset all is...Spec_ data members
//
void ParDDLFileAttrsAlterTable::resetAllIsSpecDataMembers() {
  // ALLOCATE
  isAllocateSpec_ = FALSE;

  // [ NO ] AUDIT
  isAuditSpec_ = FALSE;

  // [ NO ] AUDITCOMPRESS
  isAuditCompressSpec_ = FALSE;

  // BUFFERED
  isBufferedSpec_ = FALSE;

  // [ NO ] CLEARONPURGE
  isClearOnPurgeSpec_ = FALSE;

  // COMPRESSION
  isCompressionTypeSpec_ = FALSE;

  // DEALLOCATE
  isDeallocateSpec_ = FALSE;

  // MAXSIZE
  isMaxSizeSpec_ = FALSE;

  // EXTENT
  isExtentSpec_ = FALSE;

  // MAXEXTENTS
  isMaxExtentSpec_ = FALSE;

  // [NO] RANGELOG
  isRangeLogSpec_ = FALSE;

  // [NO] LOCKONREFRESH
  isLockOnRefreshSpec_ = FALSE;

  // INSERTLOG
  isInsertLogSpec_ = FALSE;

  // MVS ALLOWED
  isMvsAllowedSpec_ = FALSE;

  // NO LABEL UPDATE
  isNoLabelUpdateSpec_ = FALSE;

  isXnReplSpec_ = FALSE;

  isIncrBackupSpec_ = FALSE;

  isReadOnlySpec_ = FALSE;

}  // ParDDLFileAttrsAlterTable::resetAllIsSpecDataMembers()

//
// set a file attribute
//
void ParDDLFileAttrsAlterTable::setFileAttr(ElemDDLFileAttr *pFileAttr) {
  ComASSERT(pFileAttr NEQ NULL);

  switch (pFileAttr->getOperatorType()) {
    case ELM_FILE_ATTR_ALLOCATE_ELEM:
      if (isDeallocateSpec_) {
        // ALLOCATE and DEALLOCATE phrases can not appear in the same
        // ALTER TABLE statement.
        *SqlParser_Diags << DgSqlCode(-3068);
      }
      if (isAllocateSpec_) {
        // Duplicate ALLOCATE phrases.
        *SqlParser_Diags << DgSqlCode(-3081);
      }
      isAllocateSpec_ = TRUE;
      ComASSERT(pFileAttr->castToElemDDLFileAttrAllocate() NEQ NULL);
      {
        ElemDDLFileAttrAllocate *pAlloc = pFileAttr->castToElemDDLFileAttrAllocate();
        extentsToAllocate_ = pAlloc->getExtentsToAllocate();
      }
      break;

    case ELM_FILE_ATTR_AUDIT_ELEM:
      if (isAuditSpec_) {
        // Duplicate [NO]AUDIT phrases.
        *SqlParser_Diags << DgSqlCode(-3082);
      }
      ComASSERT(pFileAttr->castToElemDDLFileAttrAudit() NEQ NULL);
      isAudit_ = pFileAttr->castToElemDDLFileAttrAudit()->getIsAudit();
      isAuditSpec_ = TRUE;
      break;

    case ELM_FILE_ATTR_AUDIT_COMPRESS_ELEM:
      if (isAuditCompressSpec_) {
        // Duplicate [NO]AUDITCOMPRESS phrases.
        *SqlParser_Diags << DgSqlCode(-3083);
      }
      ComASSERT(pFileAttr->castToElemDDLFileAttrAuditCompress() NEQ NULL);
      isAuditCompress_ = pFileAttr->castToElemDDLFileAttrAuditCompress()->getIsAuditCompress();
      isAuditCompressSpec_ = TRUE;
      break;

    case ELM_FILE_ATTR_BLOCK_SIZE_ELEM:
      //
      // The grammar productions allow the Blocksize phrase
      // to appear within an Alter Table statement (syntactically).
      // Enforces the restriction by using semantic actions.
      //

      // BLOCKSIZE phrase not allowed in an Alter Table ... Attribute(s)
      // statement.
      *SqlParser_Diags << DgSqlCode(-3084);
      break;

    case ELM_FILE_ATTR_BUFFERED_ELEM:
      if (isBufferedSpec_) {
        // Duplicate [NO]BUFFERED phrases.
        *SqlParser_Diags << DgSqlCode(-3085);
      }
      ComASSERT(pFileAttr->castToElemDDLFileAttrBuffered() NEQ NULL);
      isBuffered_ = pFileAttr->castToElemDDLFileAttrBuffered()->getIsBuffered();
      isBufferedSpec_ = TRUE;
      break;

    case ELM_FILE_ATTR_CLEAR_ON_PURGE_ELEM:
      if (isClearOnPurgeSpec_) {
        // Duplicate [NO]CLEARONPURGE phrases.
        *SqlParser_Diags << DgSqlCode(-3086);
      }
      ComASSERT(pFileAttr->castToElemDDLFileAttrClearOnPurge() NEQ NULL);
      isClearOnPurge_ = pFileAttr->castToElemDDLFileAttrClearOnPurge()->getIsClearOnPurge();
      isClearOnPurgeSpec_ = TRUE;
      break;

    case ELM_FILE_ATTR_COMPRESSION_ELEM:
      // Not supported for alter
      *SqlParser_Diags << DgSqlCode(-3282);
      break;

    case ELM_FILE_ATTR_D_COMPRESS_ELEM:
      //
      // The grammar productions allow the [no]dcompress phrase
      // to appear within an Alter Table statement (syntactically).
      // Enforces the restriction by using semantic actions.
      //

      // [NO]DCOMPRESS phrase not allowed in an Alter Table ... Attribute(s)
      // statement.
      *SqlParser_Diags << DgSqlCode(-3087);
      break;

    case ELM_FILE_ATTR_DEALLOCATE_ELEM:
      if (isAllocateSpec_) {
        // ALLOCATE and DEALLOCATE phrases can not appear in the same
        // ALTER TABLE statement.
        *SqlParser_Diags << DgSqlCode(-3088);
      }
      if (isDeallocateSpec_) {
        // Duplicate DEALLOCATE phrases.
        *SqlParser_Diags << DgSqlCode(-3076);
      }
      isDeallocateSpec_ = TRUE;
      break;

    case ELM_FILE_ATTR_I_COMPRESS_ELEM:
      //
      // The grammar productions allow the [no]icompress phrase
      // to appear within an Alter Table statement (syntactically).
      // Enforces the restriction by using semantic actions.
      //
      // [NO]ICOMPRESS phrase not allowed in an Alter Table ... Attribute(s)
      // statement.
      *SqlParser_Diags << DgSqlCode(-3089);
      break;

    case ELM_FILE_ATTR_EXTENT_ELEM:
      // Dont allow EXTENT in ALTER statements.
      *SqlParser_Diags << DgSqlCode(-3194);
      break;

    case ELM_FILE_ATTR_MAXEXTENTS_ELEM:
      if (isMaxExtentSpec_) {
        // Duplicate MAXEXTENTS phrases.
        *SqlParser_Diags << DgSqlCode(-3079);
      }
      isMaxExtentSpec_ = TRUE;
      ComASSERT(pFileAttr->castToElemDDLFileAttrMaxExtents() NEQ NULL);
      {
        ElemDDLFileAttrMaxExtents *pMaxExtents = pFileAttr->castToElemDDLFileAttrMaxExtents();
        // error checking for limits when we specify the MAXEXTENTS clause
        int maxext = pMaxExtents->getMaxExtents();
        if ((maxext <= 0) || (maxext > COM_MAX_MAXEXTENTS)) {
          *SqlParser_Diags << DgSqlCode(-3191);
        } else {
          maxExt_ = pMaxExtents->getMaxExtents();
        }
      }
      break;

    case ELM_FILE_ATTR_ROW_FORMAT_ELEM:
      //
      // The grammar productions allow the Row Format phrase
      // to appear within an Alter Table statement (syntactically).
      // Enforces the restriction by using semantic actions.
      //
      // The Row FORMAT clause is not allowed in
      // the ALTER TABLE ... ATTRIBUTE(s) statement.
      *SqlParser_Diags << DgSqlCode(-3072) << DgString0("Row FORMAT") << DgString1("TABLE");
      break;

    case ELM_FILE_ATTR_NO_LABEL_UPDATE_ELEM:
      if (isNoLabelUpdateSpec_) {
        // Duplicate NO LABEL UPDATE phrases.
        *SqlParser_Diags << DgSqlCode(-3099);
      }
      ComASSERT(pFileAttr->castToElemDDLFileAttrNoLabelUpdate() NEQ NULL);
      noLabelUpdate_ = pFileAttr->castToElemDDLFileAttrNoLabelUpdate()->getIsNoLabelUpdate();
      isNoLabelUpdateSpec_ = TRUE;
      break;

    case ELM_FILE_ATTR_RANGE_LOG_ELEM:
      if (isRangeLogSpec_) {
        // Duplicate RANGELOG phrases.
        *SqlParser_Diags << DgSqlCode(-12056);
      }
      ComASSERT(pFileAttr->castToElemDDLFileAttrRangeLog() NEQ NULL);
      rangelogType_ = pFileAttr->castToElemDDLFileAttrRangeLog()->getRangelogType();
      isRangeLogSpec_ = TRUE;
      break;

    case ELM_FILE_ATTR_LOCK_ON_REFRESH_ELEM:
      if (isLockOnRefreshSpec_) {
        // Duplicate [NO]LOCKONREFRESH  phrases.
        *SqlParser_Diags << DgSqlCode(-12055);
      }
      ComASSERT(pFileAttr->castToElemDDLFileAttrLockOnRefresh() NEQ NULL);
      isLockOnRefresh_ = pFileAttr->castToElemDDLFileAttrLockOnRefresh()->isLockOnRefresh();
      isLockOnRefreshSpec_ = TRUE;
      break;

    case ELM_FILE_ATTR_INSERT_LOG_ELEM:
      if (isInsertLogSpec_) {
        // Duplicate [NO]LOCKONREFRESH  phrases.
        *SqlParser_Diags << DgSqlCode(-12057);
      }
      ComASSERT(pFileAttr->castToElemDDLFileAttrInsertLog() NEQ NULL);
      isInsertLog_ = pFileAttr->castToElemDDLFileAttrInsertLog()->isInsertLog();
      isInsertLogSpec_ = TRUE;
      break;

    case ELM_FILE_ATTR_MVS_ALLOWED_ELEM:
      if (isMvsAllowedSpec_) {
        // Duplicate RANGELOG phrases.
        *SqlParser_Diags << DgSqlCode(-12058);
      }
      ComASSERT(pFileAttr->castToElemDDLFileAttrMvsAllowed() NEQ NULL);
      mvsAllowedType_ = pFileAttr->castToElemDDLFileAttrMvsAllowed()->getMvsAllowedType();
      isMvsAllowedSpec_ = TRUE;
      break;

    case ELM_FILE_ATTR_XN_REPL_ELEM:
      if (NOT msg_license_advanced_enabled()) {
        *SqlParser_Diags << DgSqlCode(-4222) << DgString0("Replication");
      }

      if (isXnReplSpec_) {
        // Duplicate sync xn phrases.
        *SqlParser_Diags << DgSqlCode(-3183) << DgString0("replication");
      }
      isXnReplSpec_ = TRUE;
      ComASSERT(pFileAttr->castToElemDDLFileAttrXnRepl() NEQ NULL);
      {
        ElemDDLFileAttrXnRepl *pXnRepl = pFileAttr->castToElemDDLFileAttrXnRepl();
        xnRepl_ = pXnRepl->xnRepl();
      }
      break;

      // cannot alter namespace
    case ELM_FILE_ATTR_NAMESPACE_ELEM:
      *SqlParser_Diags << DgSqlCode(-3242) << DgString0(" Cannot alter namespace attribute.");
      break;

      // cannot alter encryption
    case ELM_FILE_ATTR_ENCRYPT_ELEM:
      *SqlParser_Diags << DgSqlCode(-3242) << DgString0(" Cannot alter encrypt attribute.");
      break;

    case ELM_FILE_ATTR_INCR_BACKUP_ELEM:
      if (isIncrBackupSpec_) {
        // Duplicate INCR BACKUP phrases.
        *SqlParser_Diags << DgSqlCode(-3082);
      }
      ComASSERT(pFileAttr->castToElemDDLFileAttrIncrBackup() NEQ NULL);

      incrBackupEnabled_ = pFileAttr->castToElemDDLFileAttrIncrBackup()->incrBackupEnabled();
      isIncrBackupSpec_ = TRUE;
      break;

    case ELM_FILE_ATTR_STORED_DESC_ELEM:
      //
      // The STORED DESC clause is not allowed in
      // the ALTER TABLE ... ATTRIBUTE(s) statement.
      *SqlParser_Diags << DgSqlCode(-3072) << DgString0("STORED DESC") << DgString1("TABLE");
      break;

    case ELM_FILE_ATTR_READ_ONLY_ELEM:
      if (isReadOnlySpec_) {
        // Duplicate INCR BACKUP phrases.
        *SqlParser_Diags << DgSqlCode(-3082);
      }
      ComASSERT(pFileAttr->castToElemDDLFileAttrReadOnly() NEQ NULL);

      readOnlyEnabled_ = pFileAttr->castToElemDDLFileAttrReadOnly()->readOnlyEnabled();
      isReadOnlySpec_ = TRUE;
      break;

    default:
      NAAbort("ParDDLFileAttrs.C", __LINE__, "internal logic error");
      break;
  }
}  // ParDDLFileAttrsAlterTable::setFileAttr()

//
// trace
//

NATraceList ParDDLFileAttrsAlterTable::getDetailInfo() const {
  NAString detailText;
  NATraceList detailTextList;

  // ALLOCATE

  if (isAllocateSpecified()) {
    detailText = "extents to allocate: ";
    // cast to long needed to suppress warning
    detailText += LongToNAString((int)getExtentsToAllocate());
    detailTextList.append(detailText);
  } else {
    detailTextList.append("allocate not spec");
  }

  // [ NO ] AUDIT

  if (isAuditSpecified()) {
    detailText = "audited?       ";
    detailText += YesNo(getIsAudit());
    detailTextList.append(detailText);
  } else {
    detailTextList.append("[no]audit not spec");
  }

  // [ NO ] AUDITCOMPRESS

  if (isAuditCompressSpecified()) {
    detailText = "audcompr?      ";
    detailText += YesNo(getIsAuditCompress());
    detailTextList.append(detailText);
  } else {
    detailTextList.append("[no]auditcompress not spec");
  }

  // [ NO ] BUFFERED

  if (isBufferedSpecified()) {
    detailText = "buffered?      ";
    detailText += YesNo(getIsBuffered());
    detailTextList.append(detailText);
  } else {
    detailTextList.append("[no]buffered not spec");
  }

  // [ NO ] CLEARONPURGE

  if (isClearOnPurgeSpecified()) {
    detailText = "clrpurge?      ";
    detailText += YesNo(getIsClearOnPurge());
    detailTextList.append(detailText);
  } else {
    detailTextList.append("[no]clearonpurge not spec");
  }

  // DEALLOCATE

  if (isDeallocateSpecified()) {
    detailTextList.append("deallocate specified");
  } else {
    detailTextList.append("deallocate not spec");
  }

  // MAXSIZE

  if (isMaxSizeSpecified()) {
    if (isMaxSizeUnbounded()) {
      detailText = "maxsizunbound? ";
      detailText += YesNo(isMaxSizeUnbounded());
      detailTextList.append(detailText);
    } else {
      detailText = "maximum size:  ";
      detailText += LongToNAString((int)getMaxSize());
      detailTextList.append(detailText);

      detailText = "max size unit: ";
      detailText += getMaxSizeUnitAsNAString();
      detailTextList.append(detailText);
    }
  } else {
    detailTextList.append("maxsize not spec");
  }

  return detailTextList;

}  // ParDDLFileAttrsAlterTable::getDetailInfo()

// -----------------------------------------------------------------------
// methods for class ParDDLFileAttrsCreateIndex
// -----------------------------------------------------------------------

//
// constructor
//
ParDDLFileAttrsCreateIndex::ParDDLFileAttrsCreateIndex(ParDDLFileAttrs::fileAttrsNodeTypeEnum fileAttrsNodeType)
    : ParDDLFileAttrs(fileAttrsNodeType) {
  initializeDataMembers();
}

//
// copy constructor
//
ParDDLFileAttrsCreateIndex::ParDDLFileAttrsCreateIndex(const ParDDLFileAttrsCreateIndex &createIndexFileAttributes) {
  copy(createIndexFileAttributes);
}

//
// virtual destructor
//
ParDDLFileAttrsCreateIndex::~ParDDLFileAttrsCreateIndex() {}

//
// assignment operator
//
ParDDLFileAttrsCreateIndex &ParDDLFileAttrsCreateIndex::operator=(const ParDDLFileAttrsCreateIndex &rhs) {
  if (this EQU & rhs) {
    return *this;
  }

  copy(rhs);

  return *this;

}  // ParDDLFileAttrsCreateIndex::operator=()

//
// accessors
//

NAString ParDDLFileAttrsCreateIndex::getMaxSizeUnitAsNAString() const {
  ElemDDLFileAttrMaxSize maxSizeFileAttr(getMaxSize(), getMaxSizeUnit());
  return maxSizeFileAttr.getMaxSizeUnitAsNAString();
}

//
// mutators
//

void ParDDLFileAttrsCreateIndex::copy(const ParDDLFileAttrsCreateIndex &rhs) {
  ParDDLFileAttrs::copy(rhs);

  // ALLOCATE
  extentsToAllocate_ = rhs.extentsToAllocate_;

  // [ NO ] AUDITCOMPRESS
  isAuditCompressSpec_ = rhs.isAuditCompressSpec_;
  isAuditCompress_ = rhs.isAuditCompress_;

  // BLOCKSIZE
  isBlockSizeSpec_ = rhs.isBlockSizeSpec_;
  blockSize_ = rhs.blockSize_;

  // [ NO ] BUFFERED
  isBufferedSpec_ = rhs.isBufferedSpec_;
  isBuffered_ = rhs.isBuffered_;

  // [ NO ] CLEARONPURGE
  isClearOnPurgeSpec_ = rhs.isClearOnPurgeSpec_;
  isClearOnPurge_ = rhs.isClearOnPurge_;

  // COMPRESSION
  isCompressionTypeSpec_ = rhs.isCompressionTypeSpec_;
  compressionType_ = rhs.compressionType_;

  // [ NO ] DCOMPRESS
  isDCompressSpec_ = rhs.isDCompressSpec_;
  isDCompress_ = rhs.isDCompress_;

  // [ NO ] ICOMPRESS
  isICompressSpec_ = rhs.isICompressSpec_;
  isICompress_ = rhs.isICompress_;

  // MAXSIZE
  isMaxSizeSpec_ = rhs.isMaxSizeSpec_;
  isMaxSizeUnbounded_ = rhs.isMaxSizeUnbounded_;
  maxSize_ = rhs.maxSize_;
  maxSizeUnit_ = rhs.maxSizeUnit_;

  // EXTENT
  isExtentSpec_ = rhs.isExtentSpec_;
  priExt_ = rhs.priExt_;
  secExt_ = rhs.secExt_;

  // MAXEXTENTs
  isMaxExtentSpec_ = rhs.isMaxExtentSpec_;
  maxExt_ = rhs.maxExt_;

  // UID
  isUIDSpec_ = rhs.isUIDSpec_;
  UID_ = rhs.UID_;

  // { ALIGNED | PACKED } FORMAT
  isRowFormatSpec_ = rhs.isRowFormatSpec_;
  eRowFormat_ = rhs.eRowFormat_;

  isStorageTypeSpec_ = rhs.isStorageTypeSpec_;
  storageType_ = rhs.storageType_;

}  // ParDDLFileAttrsCreateIndex::copy()

//
// private method invoked by the constructor to
// initialize data members in the class with the
// appropriate default values
//
void ParDDLFileAttrsCreateIndex::initializeDataMembers() {
  resetAllIsSpecDataMembers();

  // ALLOCATE
  extentsToAllocate_ = ElemDDLFileAttrAllocate::DEFAULT_EXTENTS_TO_ALLOCATE;

  // [ NO ] AUDITCOMPRESS
  //
  //   Default is TRUE which means to include only changed columns.
  //
  isAuditCompress_ = TRUE;

  // BLOCKSIZE
  blockSize_ = ElemDDLFileAttrBlockSize::DEFAULT_BLOCK_SIZE;

  // [ NO ] BUFFERED
  //
  //   turns buffering on or off.
  //
  //   For Create Index statements, the default is the corresponding
  //   value in the underlying table.  The parser cannot determine
  //   the default value.
  //
  //   For Create Table statements, default is on when audited,
  //   else it is off.  Can not determine the default value during
  //   instantiation time.  Set the value to TRUE for now.
  //
  isBuffered_ = TRUE;

  // [ NO ] CLEARONPURGE
  //
  //   For Create Index statements, the default is the corresponding
  //   value in the underlying table.  The parser cannot determine
  //   the default value.
  //
  //   For Create Table statements, the default is FALSE (no erasure).
  //
  isClearOnPurge_ = FALSE;

  // COMPRESSION TYPE { NONE | HARDWARE | SOFTWARE }
  compressionType_ = COM_NO_COMPRESSION;

  // [ NO ] DCOMPRESS
  //
  //   constrols key compression in data blocks.
  //
  //   For Create Index statements, the default is the corresponding
  //   value in the underlying table.  The parser cannot determine
  //   the default value.
  //
  //   For Create Table statements, the default is FALSE (no
  //   compression).
  //
  isDCompress_ = FALSE;

  // [ NO ] ICOMPRESS
  //
  //   constrols key compression in index blocks.
  //
  //   For Create Index statements, the default is the corresponding
  //   value in the underlying table.  The parser cannot determine
  //   the default value.
  //
  //   For Create Table statements, the default is FALSE (no
  //   compression).
  //
  isICompress_ = FALSE;

  // MAXSIZE
  isMaxSizeUnbounded_ = FALSE;
  ParSetDefaultMaxSize(maxSize_, maxSizeUnit_);

  // EXTENT
  //
  //   The following data members have no meanings when the
  //   Extent phrase does not appear.
  //

  ParSetDefaultExtents(priExt_, secExt_);

  // MAXEXTENTS
  //
  //   The following data members have no meanings when the
  //   MaxExtents phrase does not appear.
  //
  ParSetDefaultMaxExtents(maxExt_);

  // UID clause
  UID_ = 0;

  // { ALIGNED | PACKED } FORMAT
  eRowFormat_ = ElemDDLFileAttrRowFormat::eUNSPECIFIED;

  storageType_ = COM_STORAGE_HBASE;

}  // ParDDLFileAttrsCreateIndex::initializeDataMembers()

//
// reset all is...Spec_ data members
//
void ParDDLFileAttrsCreateIndex::resetAllIsSpecDataMembers() {
  // ALLOCATE
  isAllocateSpec_ = FALSE;

  // [ NO ] AUDITCOMPRESS
  isAuditCompressSpec_ = FALSE;

  // BLOCKSIZE
  isBlockSizeSpec_ = FALSE;

  // BUFFERED
  isBufferedSpec_ = FALSE;

  // [ NO ] CLEARONPURGE
  isClearOnPurgeSpec_ = FALSE;

  // COMPRESSION
  isCompressionTypeSpec_ = FALSE;

  // [ NO ] DCOMPRESS
  isDCompressSpec_ = FALSE;

  // [ NO ] ICOMPRESS
  isICompressSpec_ = FALSE;

  // MAXSIZE
  isMaxSizeSpec_ = FALSE;

  // EXTENT
  isExtentSpec_ = FALSE;

  // EXTENT
  isMaxExtentSpec_ = FALSE;

  // UID
  isUIDSpec_ = FALSE;

  // { ALIGNED | PACKED } FORMAT
  isRowFormatSpec_ = FALSE;

  isStorageTypeSpec_ = FALSE;

}  // ParDDLFileAttrsCreateIndex::resetAllIsSpecDataMembers()

//
// set a file attribute
//
void ParDDLFileAttrsCreateIndex::setFileAttr(ElemDDLFileAttr *pFileAttr) {
  ComASSERT(pFileAttr NEQ NULL);

  switch (pFileAttr->getOperatorType()) {
    case ELM_FILE_ATTR_ALLOCATE_ELEM:
      if (isAllocateSpec_) {
        // Duplicate ALLOCATE phrases.
        *SqlParser_Diags << DgSqlCode(-3081);
      }
      isAllocateSpec_ = TRUE;
      ComASSERT(pFileAttr->castToElemDDLFileAttrAllocate() NEQ NULL);
      {
        ElemDDLFileAttrAllocate *pAlloc = pFileAttr->castToElemDDLFileAttrAllocate();
        extentsToAllocate_ = pAlloc->getExtentsToAllocate();
      }
      break;

    case ELM_FILE_ATTR_STORAGE_TYPE_ELEM:
      if (isStorageTypeSpec_) {
        // Duplicate sync xn phrases.
        *SqlParser_Diags << DgSqlCode(-3183) << DgString0("storage");
      }
      isStorageTypeSpec_ = TRUE;
      ComASSERT(pFileAttr->castToElemDDLFileAttrStorageType() NEQ NULL);
      {
        ElemDDLFileAttrStorageType *pStorageType = pFileAttr->castToElemDDLFileAttrStorageType();
        storageType_ = pStorageType->storageType();
      }
      break;

    case ELM_FILE_ATTR_AUDIT_ELEM:
      //
      // The grammar productions allow the [no]audit phrase to appear
      // within a Create Index statement (syntactically).  Enforces
      // the restriction by using semantic actions.
      //
      // [NO]AUDIT phrase not allowed in CREATE INDEX statement.
      *SqlParser_Diags << DgSqlCode(-3091);
      break;

    case ELM_FILE_ATTR_AUDIT_COMPRESS_ELEM:
      if (isAuditCompressSpec_) {
        // Duplicate [NO]AUDITCOMPRESS phrases.
        *SqlParser_Diags << DgSqlCode(-3083);
      }
      ComASSERT(pFileAttr->castToElemDDLFileAttrAuditCompress() NEQ NULL);
      isAuditCompress_ = pFileAttr->castToElemDDLFileAttrAuditCompress()->getIsAuditCompress();
      isAuditCompressSpec_ = TRUE;
      break;

    case ELM_FILE_ATTR_BLOCK_SIZE_ELEM:
      if (isBlockSizeSpec_) {
        // Duplicate BLOCKSIZE phrases.
        *SqlParser_Diags << DgSqlCode(-3092);
      }
      ComASSERT(pFileAttr->castToElemDDLFileAttrBlockSize() NEQ NULL);
      blockSize_ = pFileAttr->castToElemDDLFileAttrBlockSize()->getBlockSize();
      isBlockSizeSpec_ = TRUE;
      break;

    case ELM_FILE_ATTR_BUFFERED_ELEM:
      if (isBufferedSpec_) {
        // Duplicate [NO]BUFFERED phrases.
        *SqlParser_Diags << DgSqlCode(-3085);
      }
      ComASSERT(pFileAttr->castToElemDDLFileAttrBuffered() NEQ NULL);
      isBuffered_ = pFileAttr->castToElemDDLFileAttrBuffered()->getIsBuffered();
      isBufferedSpec_ = TRUE;
      break;

    case ELM_FILE_ATTR_CLEAR_ON_PURGE_ELEM:
      if (isClearOnPurgeSpec_) {
        // Duplicate [NO]CLEARONPURGE phrases.
        *SqlParser_Diags << DgSqlCode(-3086);
      }
      ComASSERT(pFileAttr->castToElemDDLFileAttrClearOnPurge() NEQ NULL);
      isClearOnPurge_ = pFileAttr->castToElemDDLFileAttrClearOnPurge()->getIsClearOnPurge();
      isClearOnPurgeSpec_ = TRUE;
      break;

    case ELM_FILE_ATTR_COMPRESSION_ELEM:
      if (isCompressionTypeSpec_) {
        // Duplicate COMPRESSION phrases.
        *SqlParser_Diags << DgSqlCode(-3283);
      }
      ComASSERT(pFileAttr->castToElemDDLFileAttrCompression() NEQ NULL);
      compressionType_ = pFileAttr->castToElemDDLFileAttrCompression()->getCompressionType();
      isCompressionTypeSpec_ = TRUE;
      break;

    case ELM_FILE_ATTR_D_COMPRESS_ELEM:
      if (isDCompressSpec_) {
        // Duplicate [NO]DCOMPRESS phrases.
        *SqlParser_Diags << DgSqlCode(-3093);
      }
      ComASSERT(pFileAttr->castToElemDDLFileAttrDCompress() NEQ NULL);
      isDCompress_ = pFileAttr->castToElemDDLFileAttrDCompress()->getIsDCompress();
      isDCompressSpec_ = TRUE;
      break;

    case ELM_FILE_ATTR_DEALLOCATE_ELEM:
      //
      // The grammar productions allow the Deallocate phrase to appear
      // within a Create Index statement (syntactically).  Enforces
      // the restriction by using semantic actions.
      //
      // DEALLOCATE phrase not allowed in CREATE INDEX statement.
      *SqlParser_Diags << DgSqlCode(-3094);
      break;

    case ELM_FILE_ATTR_I_COMPRESS_ELEM:
      if (isICompressSpec_) {
        // Duplicate [NO]ICOMPRESS phrases" << endl;
        *SqlParser_Diags << DgSqlCode(-3095);
      }
      ComASSERT(pFileAttr->castToElemDDLFileAttrICompress() NEQ NULL);
      isICompress_ = pFileAttr->castToElemDDLFileAttrICompress()->getIsICompress();
      isICompressSpec_ = TRUE;
      break;

    case ELM_FILE_ATTR_EXTENT_ELEM:
      if (isExtentSpec_) {
        // Duplicate EXTENT phrases.
        *SqlParser_Diags << DgSqlCode(-3079);
      }
      isExtentSpec_ = TRUE;
      ComASSERT(pFileAttr->castToElemDDLFileAttrExtents() NEQ NULL);
      {
        ElemDDLFileAttrExtents *pExtents = pFileAttr->castToElemDDLFileAttrExtents();
        priExt_ = pExtents->getPriExtents();
        secExt_ = pExtents->getSecExtents();
      }
      break;

    case ELM_FILE_ATTR_MAXEXTENTS_ELEM:
      if (isMaxExtentSpec_) {
        // Duplicate MAXEXTENTS phrases.
        *SqlParser_Diags << DgSqlCode(-3079);
      }
      isMaxExtentSpec_ = TRUE;
      ComASSERT(pFileAttr->castToElemDDLFileAttrMaxExtents() NEQ NULL);
      {
        ElemDDLFileAttrMaxExtents *pMaxExtents = pFileAttr->castToElemDDLFileAttrMaxExtents();
        //////////////////////////////////////////////////////
        // error checking when we define the MAXEXTENTS clause
        //////////////////////////////////////////////////////
        int maxext = pMaxExtents->getMaxExtents();
        if ((maxext <= 0) || (maxext > COM_MAX_MAXEXTENTS)) {
          *SqlParser_Diags << DgSqlCode(-3191);
        } else
          maxExt_ = pMaxExtents->getMaxExtents();
      }
      break;

    case ELM_FILE_ATTR_UID_ELEM:
      if (isUIDSpec_) {
        // Duplicate UID phrases.
        *SqlParser_Diags << DgSqlCode(-3183) << DgString0("UID");
      }
      isUIDSpec_ = TRUE;
      ComASSERT(pFileAttr->castToElemDDLFileAttrUID() NEQ NULL);
      {
        ElemDDLFileAttrUID *pUID = pFileAttr->castToElemDDLFileAttrUID();
        UID_ = pUID->getUID();
      }
      break;

    case ELM_FILE_ATTR_ROW_FORMAT_ELEM:
      if (isRowFormatSpec_) {
        // Duplicate Row FORMAT phrases.
        *SqlParser_Diags << DgSqlCode(-3183) << DgString0("Row FORMAT");
      }
      isRowFormatSpec_ = TRUE;
      ComASSERT(pFileAttr->castToElemDDLFileAttrRowFormat() NEQ NULL);
      {
        ElemDDLFileAttrRowFormat *pRowFormat = pFileAttr->castToElemDDLFileAttrRowFormat();
        eRowFormat_ = pRowFormat->getRowFormat();
      }
      break;

    case ELM_FILE_ATTR_XN_REPL_ELEM:
      // this option on an index is not yet supported.
      *SqlParser_Diags << DgSqlCode(-3242) << DgString0("Cannot specify replication attribute in create index.");

      break;

    default:
      // NAAbort("ParDDLFileAttrs.C", __LINE__, "internal logic error");
      *SqlParser_Diags << DgSqlCode(-3242) << DgString0("Unsupported attribute in create index.");
      break;
  }
}  // ParDDLFileAttrsCreateIndex::setFileAttr()

//
// trace
//

NATraceList ParDDLFileAttrsCreateIndex::getDetailInfo() const {
  NAString detailText;
  NATraceList detailTextList;

  detailText = "extents to allocate: ";
  // cast to long needed to suppress warning
  detailText += LongToNAString((int)getExtentsToAllocate());
  detailTextList.append(detailText);

  detailText = "audcompr?      ";
  detailText += YesNo(getIsAuditCompress());
  detailTextList.append(detailText);

  detailText = "blocksize:     ";
  detailText += LongToNAString((int)getBlockSize());
  detailTextList.append(detailText);

  detailText = "buffered?      ";
  detailText += YesNo(getIsBuffered());
  detailTextList.append(detailText);

  detailText = "clrpurge?      ";
  detailText += YesNo(getIsClearOnPurge());
  detailTextList.append(detailText);

  if (isCompressionTypeSpecified()) {
    detailText = "compression?    ";
    switch (getCompressionType()) {
      case COM_NO_COMPRESSION:
        detailText += "N  ";
        break;
      case COM_HARDWARE_COMPRESSION:
        detailText += "H ";
        break;
      case COM_SOFTWARE_COMPRESSION:
        detailText += "S ";
        break;
      default:
        break;
    }
    detailTextList.append(detailText);
  } else
    detailTextList.append("compression not spec");

  detailText = "datacomp?      ";
  detailText += YesNo(getIsDCompress());
  detailTextList.append(detailText);

  detailText = "ixcompr?       ";
  detailText += YesNo(getIsICompress());
  detailTextList.append(detailText);

  detailText = "maxsizunbound? ";
  detailText += YesNo(isMaxSizeUnbounded());
  detailTextList.append(detailText);

  if (NOT isMaxSizeUnbounded()) {
    detailText = "maximum size:  ";
    detailText += LongToNAString((int)getMaxSize());
    detailTextList.append(detailText);

    detailText = "max size unit: ";
    detailText += getMaxSizeUnitAsNAString();
    detailTextList.append(detailText);
  }

  return detailTextList;

}  // ParDDLFileAttrsCreateIndex::getDetailInfo()

// -----------------------------------------------------------------------
// methods for class ParDDLFileAttrsCreateTable
// -----------------------------------------------------------------------

//
// constructor
//
ParDDLFileAttrsCreateTable::ParDDLFileAttrsCreateTable(fileAttrsNodeTypeEnum fileAttrsNodeType)

    : ParDDLFileAttrsCreateIndex(fileAttrsNodeType)

{
  initializeDataMembers();
}

//
// copy constructor
//
ParDDLFileAttrsCreateTable::ParDDLFileAttrsCreateTable(const ParDDLFileAttrsCreateTable &createTableFileAttributes) {
  copy(createTableFileAttributes);
}

//
// virtual destructor
//
ParDDLFileAttrsCreateTable::~ParDDLFileAttrsCreateTable() {}

//
// assignment operator
//
ParDDLFileAttrsCreateTable &ParDDLFileAttrsCreateTable::operator=(const ParDDLFileAttrsCreateTable &rhs) {
  if (this EQU & rhs) {
    return *this;
  }

  copy(rhs);

  return *this;

}  // ParDDLFileAttrsCreateTable::operator=()

//
// mutators
//

void ParDDLFileAttrsCreateTable::copy(const ParDDLFileAttrsCreateTable &rhs) {
  ParDDLFileAttrsCreateIndex::copy(rhs);

  // ALLOCATE
  //   inherits from class ParDDLFileAttrsCreateIndex

  // [ NO ] AUDIT
  isAuditSpec_ = rhs.isAuditSpec_;
  isAudit_ = rhs.isAudit_;

  // COMPRESSION
  isCompressionTypeSpec_ = rhs.isCompressionTypeSpec_;
  compressionType_ = rhs.compressionType_;

  // {NO | AUTOMATIC | MANUAL | MIXED } RANGELOG
  isRangeLogSpec_ = rhs.isRangeLogSpec_;
  rangelogType_ = rhs.rangelogType_;

  // [NO] LOCKONREFRESH
  isLockOnRefreshSpec_ = rhs.isLockOnRefreshSpec_;
  isLockOnRefresh_ = rhs.isLockOnRefresh_;

  // INSERTLOG
  isInsertLogSpec_ = rhs.isInsertLogSpec_;
  isInsertLog_ = rhs.isInsertLog_;

  // MVS ALLOWED
  isMvsAllowedSpec_ = rhs.isMvsAllowedSpec_;
  mvsAllowedType_ = rhs.mvsAllowedType_;

  // COMMIT EACH
  isMvCommitEachSpec_ = rhs.isMvCommitEachSpec_;
  commitEachNRows_ = rhs.commitEachNRows_;

  // MV AUDIT
  isMvAuditSpec_ = rhs.isMvAuditSpec_;
  mvAuditType_ = rhs.mvAuditType_;

  // OWNER
  isOwnerSpec_ = rhs.isOwnerSpec_;
  owner_ = rhs.owner_;

  // default column family
  isColFamSpec_ = rhs.isColFamSpec_;
  colFam_ = rhs.colFam_;

  isXnReplSpec_ = rhs.isXnReplSpec_;
  xnRepl_ = rhs.xnRepl_;

  isStorageTypeSpec_ = rhs.isStorageTypeSpec_;
  storageType_ = rhs.storageType_;

  isEncryptionSpec_ = rhs.isEncryptionSpec_;
  encryptRowid_ = rhs.encryptRowid_;
  encryptData_ = rhs.encryptData_;

  isStoredDescSpec_ = rhs.isStoredDescSpec_;
  storedDesc_ = rhs.storedDesc_;

  isIncrBackupSpec_ = rhs.isIncrBackupSpec_;
  incrBackupEnabled_ = rhs.incrBackupEnabled_;

  // [ NO ] AUDITCOMPRESS
  //   inherits from class ParDDLFileAttrsCreateIndex

  // BLOCKSIZE
  //   inherits from class ParDDLFileAttrsCreateIndex

  // [ NO ] BUFFERED
  //   inherits from class ParDDLFileAttrsCreateIndex

  // [ NO ] CLEARONPURGE
  //   inherits from class ParDDLFileAttrsCreateIndex

  // [ NO ] DCOMPRESS
  //   inherits from class ParDDLFileAttrsCreateIndex

  // [ NO ] ICOMPRESS
  //   inherits from class ParDDLFileAttrsCreateIndex

  // LOCKLENGTH
  //   inherits from class ParDDLFileAttrsCreateIndex

  // MAXSIZE
  //   inherits from class ParDDLFileAttrsCreateIndex

}  // ParDDLFileAttrsCreateTable::copy()

//
// private method invoked by the constructor to
// initialize data members in the class with the
// appropriate default values
//
void ParDDLFileAttrsCreateTable::initializeDataMembers() {
  resetAllIsSpecDataMembers();

  // ALLOCATE
  //   inherits from class ParDDLFileAttrsCreateIndex

  // [ NO ] AUDIT
  //   controls TMF auditing.  Default is AUDIT.
  isAudit_ = TRUE;

  // COMPRESSION
  compressionType_ = COM_NO_COMPRESSION;

  // RANGELOG
  rangelogType_ = COM_NO_RANGELOG;

  // [NO] LOCKONREFRESH
  isLockOnRefresh_ = TRUE;

  // INSERTLOG
  isInsertLog_ = FALSE;

  // MVS ALLOWED
  mvsAllowedType_ = COM_NO_MVS_ALLOWED;

  // COMMIT EACH nrows
  commitEachNRows_ = 0;

  // MV AUDIT
  mvAuditType_ = COM_MV_AUDIT;

  // [ NO ] AUDITCOMPRESS
  //   inherits from class ParDDLFileAttrsCreateIndex

  // BLOCKSIZE
  //   inherits from class ParDDLFileAttrsCreateIndex

  // [ NO ] BUFFERED
  //   inherits from class ParDDLFileAttrsCreateIndex

  // [ NO ] CLEARONPURGE

  // LOCKLENGTH
  //   inherits from class ParDDLFileAttrsCreateIndex

  // MAXSIZE
  //   inherits from class ParDDLFileAttrsCreateIndex

  // EXTENT
  //   inherits from class ParDDLFileAttrsCreateIndex

  // MAXEXTENT
  //   inherits from class ParDDLFileAttrsCreateIndex

  xnRepl_ = COM_REPL_NONE;
  incrBackupEnabled_ = FALSE;

  storageType_ = COM_STORAGE_HBASE;

  encryptRowid_ = FALSE;
  encryptData_ = FALSE;

  storedDesc_ = FALSE;
}  // ParDDLFileAttrsCreateTable::initializeDataMembers()

//
// reset all Is...Spec_ data members
//
void ParDDLFileAttrsCreateTable::resetAllIsSpecDataMembers() {
  // ALLOCATE
  //   inherits from class ParDDLFileAttrsCreateIndex

  // [ NO ] AUDIT
  isAuditSpec_ = FALSE;

  // COMPRESSION
  isCompressionTypeSpec_ = FALSE;

  // RANGELOG
  isRangeLogSpec_ = FALSE;

  // [NO] LOCKONREFRESH
  isLockOnRefreshSpec_ = FALSE;

  // INSERTLOG
  isInsertLogSpec_ = FALSE;

  // MVS ALLOWED
  isMvsAllowedSpec_ = FALSE;

  // COMMIT EACH
  isMvCommitEachSpec_ = FALSE;

  // MV AUDIT
  isMvAuditSpec_ = FALSE;

  // OWNER
  isOwnerSpec_ = FALSE;

  isColFamSpec_ = FALSE;

  isXnReplSpec_ = FALSE;

  isStorageTypeSpec_ = FALSE;

  isNamespaceSpec_ = FALSE;

  isEncryptionSpec_ = FALSE;

  isStoredDescSpec_ = FALSE;

  isIncrBackupSpec_ = FALSE;

  isReadOnlySpec_ = FALSE;

  // [ NO ] AUDITCOMPRESS
  //   inherits from class ParDDLFileAttrsCreateIndex

  // BLOCKSIZE
  //   inherits from class ParDDLFileAttrsCreateIndex

  // [ NO ] BUFFERED
  //   inherits from class ParDDLFileAttrsCreateIndex

  // [ NO ] CLEARONPURGE
  //   inherits from class ParDDLFileAttrsCreateIndex

  // LOCKLENGTH
  //   inherits from class ParDDLFileAttrsCreateIndex

  // MAXSIZE
  //   inherits from class ParDDLFileAttrsCreateIndex

  // EXTENT
  //   inherits from class ParDDLFileAttrsCreateIndex

  // MAXEXTENT
  //   inherits from class ParDDLFileAttrsCreateIndex

}  // ParDDLFileAttrsCreateTable::resetAllIsSpecDataMembers()

//
// set a file attribute
//
void ParDDLFileAttrsCreateTable::setFileAttr(ElemDDLFileAttr *pFileAttr) {
  ComASSERT(pFileAttr NEQ NULL);

  switch (pFileAttr->getOperatorType()) {
    case ELM_FILE_ATTR_ALLOCATE_ELEM:
    case ELM_FILE_ATTR_AUDIT_COMPRESS_ELEM:
    case ELM_FILE_ATTR_BLOCK_SIZE_ELEM:
    case ELM_FILE_ATTR_BUFFERED_ELEM:
    case ELM_FILE_ATTR_CLEAR_ON_PURGE_ELEM:
    case ELM_FILE_ATTR_D_COMPRESS_ELEM:
    case ELM_FILE_ATTR_I_COMPRESS_ELEM:
    case ELM_FILE_ATTR_EXTENT_ELEM:
    case ELM_FILE_ATTR_MAXEXTENTS_ELEM:
    case ELM_FILE_ATTR_UID_ELEM:
    case ELM_FILE_ATTR_ROW_FORMAT_ELEM:
      ParDDLFileAttrsCreateIndex::setFileAttr(pFileAttr);
      break;

    case ELM_FILE_ATTR_COL_FAM_ELEM:
      if (isColFamSpec_) {
        // Duplicate Col Family phrases.
        *SqlParser_Diags << DgSqlCode(-3183) << DgString0("Column Family");
      }
      isColFamSpec_ = TRUE;
      ComASSERT(pFileAttr->castToElemDDLFileAttrColFam() NEQ NULL);
      {
        ElemDDLFileAttrColFam *pColFam = pFileAttr->castToElemDDLFileAttrColFam();
        colFam_ = pColFam->getColFam();
      }
      break;

    case ELM_FILE_ATTR_XN_REPL_ELEM:
      if (NOT msg_license_advanced_enabled()) {
        *SqlParser_Diags << DgSqlCode(-4222) << DgString0("Replication");
      }

      if (isXnReplSpec_) {
        // Duplicate sync xn phrases.
        *SqlParser_Diags << DgSqlCode(-3183) << DgString0("replication");
      }
      isXnReplSpec_ = TRUE;
      ComASSERT(pFileAttr->castToElemDDLFileAttrXnRepl() NEQ NULL);
      {
        ElemDDLFileAttrXnRepl *pXnRepl = pFileAttr->castToElemDDLFileAttrXnRepl();
        xnRepl_ = pXnRepl->xnRepl();
      }
      break;

    case ELM_FILE_ATTR_STORAGE_TYPE_ELEM:
      if (isStorageTypeSpec_) {
        // Duplicate sync xn phrases.
        *SqlParser_Diags << DgSqlCode(-3183) << DgString0("storage");
      }
      isStorageTypeSpec_ = TRUE;
      ComASSERT(pFileAttr->castToElemDDLFileAttrStorageType() NEQ NULL);
      {
        ElemDDLFileAttrStorageType *pStorageType = pFileAttr->castToElemDDLFileAttrStorageType();
        storageType_ = pStorageType->storageType();
      }
      break;

    case ELM_FILE_ATTR_STORED_DESC_ELEM:
      if (isStoredDescSpec_) {
        // Duplicate
        *SqlParser_Diags << DgSqlCode(-3183) << DgString0("generate stored descriptor");
      }
      isStoredDescSpec_ = TRUE;
      ComASSERT(pFileAttr->castToElemDDLFileAttrStoredDesc() NEQ NULL);
      {
        ElemDDLFileAttrStoredDesc *pStoredDesc = pFileAttr->castToElemDDLFileAttrStoredDesc();
        storedDesc_ = TRUE;
      }
      break;

    case ELM_FILE_ATTR_AUDIT_ELEM:

      // MvAudit is only for MVs. Mvs cannot have audit it the ATTRIBUTE
      // clause
      if (isAuditSpec_ || isMvAuditSpec_) {
        // Duplicate [NO]AUDIT phrases.
        *SqlParser_Diags << DgSqlCode(-3082);
      }
      ComASSERT(pFileAttr->castToElemDDLFileAttrAudit() NEQ NULL);
      isAudit_ = pFileAttr->castToElemDDLFileAttrAudit()->getIsAudit();
      isAuditSpec_ = TRUE;
      break;

    case ELM_FILE_ATTR_COMPRESSION_ELEM:
      if (isCompressionTypeSpec_) {
        // Duplicate COMPRESSION phrases.
        *SqlParser_Diags << DgSqlCode(-3283);
      }
      ComASSERT(pFileAttr->castToElemDDLFileAttrCompression() NEQ NULL);
      compressionType_ = pFileAttr->castToElemDDLFileAttrCompression()->getCompressionType();

      isCompressionTypeSpec_ = TRUE;
      break;

    case ELM_FILE_ATTR_RANGE_LOG_ELEM:
      if (isRangeLogSpec_) {
        // Duplicate RANGELOG phrases.
        *SqlParser_Diags << DgSqlCode(-12055);
      }
      ComASSERT(pFileAttr->castToElemDDLFileAttrRangeLog() NEQ NULL);
      rangelogType_ = pFileAttr->castToElemDDLFileAttrRangeLog()->getRangelogType();
      isRangeLogSpec_ = TRUE;
      break;

    case ELM_FILE_ATTR_LOCK_ON_REFRESH_ELEM:
      if (isLockOnRefreshSpec_) {
        // Duplicate [NO]LOCKONREFRESH phrases.
        *SqlParser_Diags << DgSqlCode(-12055);
      }
      ComASSERT(pFileAttr->castToElemDDLFileAttrLockOnRefresh() NEQ NULL);
      isLockOnRefresh_ = pFileAttr->castToElemDDLFileAttrLockOnRefresh()->isLockOnRefresh();
      isLockOnRefreshSpec_ = TRUE;
      break;

    case ELM_FILE_ATTR_INSERT_LOG_ELEM:
      if (isInsertLogSpec_) {
        // Duplicate [NO]LOCKONREFRESH  phrases.
        *SqlParser_Diags << DgSqlCode(-12057);
      }
      ComASSERT(pFileAttr->castToElemDDLFileAttrInsertLog() NEQ NULL);
      isInsertLog_ = pFileAttr->castToElemDDLFileAttrInsertLog()->isInsertLog();
      isInsertLogSpec_ = TRUE;
      break;

    case ELM_FILE_ATTR_MVS_ALLOWED_ELEM:
      if (isMvsAllowedSpec_) {
        // Duplicate RANGELOG phrases.
        *SqlParser_Diags << DgSqlCode(-12058);
      }
      ComASSERT(pFileAttr->castToElemDDLFileAttrMvsAllowed() NEQ NULL);
      mvsAllowedType_ = pFileAttr->castToElemDDLFileAttrMvsAllowed()->getMvsAllowedType();
      isMvsAllowedSpec_ = TRUE;
      break;

    case ELM_FILE_ATTR_MV_COMMIT_EACH_ELEM:
      if (isMvCommitEachSpec_) {
        // Duplicate COMMIT EACH phrases.
        *SqlParser_Diags << DgSqlCode(-12059);
      }
      ComASSERT(pFileAttr->castToElemDDLFileAttrMVCommitEach() NEQ NULL);
      commitEachNRows_ = pFileAttr->castToElemDDLFileAttrMVCommitEach()->getNRows();
      isMvCommitEachSpec_ = TRUE;
      break;

    case ELM_FILE_ATTR_OWNER_ELEM:
      if (isOwnerSpec_) *SqlParser_Diags << DgSqlCode(-3082);
      ComASSERT(pFileAttr->castToElemDDLFileAttrOwner() NEQ NULL);
      owner_ = pFileAttr->castToElemDDLFileAttrOwner()->getOwner();
      isOwnerSpec_ = TRUE;
      break;

    case ELM_FILE_ATTR_DEALLOCATE_ELEM:
      //
      // The grammar productions allow the Deallocate phrase to appear
      // within a Create Table statement (syntactically).  Enforces
      // the restriction by using semantic actions.
      //
      // DEALLOCATE phrase not allowed in in CREATE TABLE statement.
      *SqlParser_Diags << DgSqlCode(-3097);
      break;

    case ELM_FILE_ATTR_NAMESPACE_ELEM:
      if (isNamespaceSpec_) {
        // Duplicate NAMESPACE phrases.
        *SqlParser_Diags << DgSqlCode(-3082);
      }
      ComASSERT(pFileAttr->castToElemDDLFileAttrNamespace() NEQ NULL);

      namespace_ = pFileAttr->castToElemDDLFileAttrNamespace()->getNamespace();
      isNamespaceSpec_ = TRUE;
      break;

    case ELM_FILE_ATTR_ENCRYPT_ELEM:
      if (isEncryptionSpec_) {
        // Duplicate ENCRYPT phrases.
        *SqlParser_Diags << DgSqlCode(-3082);
      }
      ComASSERT(pFileAttr->castToElemDDLFileAttrEncrypt() NEQ NULL);

      encryptionOptions_ = (NAString *)pFileAttr->castToElemDDLFileAttrEncrypt()->getEncryptionOptions();
      isEncryptionSpec_ = TRUE;
      if (NOT ComEncryption::validateEncryptionOptions(encryptionOptions_, encryptRowid_, encryptData_)) {
        *SqlParser_Diags
            << DgSqlCode(-3066)
            << DgString0(
                   "Invalid encryption options specified. Valid options must be 'r', 'd' or a combination of them.");
      }
      break;

    case ELM_FILE_ATTR_INCR_BACKUP_ELEM:
      if (isIncrBackupSpec_) {
        // Duplicate INCR BACKUP phrases.
        *SqlParser_Diags << DgSqlCode(-3082);
      }
      ComASSERT(pFileAttr->castToElemDDLFileAttrIncrBackup() NEQ NULL);

      incrBackupEnabled_ = pFileAttr->castToElemDDLFileAttrIncrBackup()->incrBackupEnabled();
      isIncrBackupSpec_ = TRUE;
      break;

    case ELM_FILE_ATTR_READ_ONLY_ELEM:
      if (isReadOnlySpec_) {
        *SqlParser_Diags << DgSqlCode(-3082);
      }

      ComASSERT(pFileAttr->castToElemDDLFileAttrReadOnly() NEQ NULL);
      readOnlyEnabled_ = pFileAttr->castToElemDDLFileAttrReadOnly()->readOnlyEnabled();
      isReadOnlySpec_ = TRUE;
      break;

    default:
      NAAbort("ParDDLFileAttrs.C", __LINE__, "internal logic error");
      break;
  }
}  // ParDDLFileAttrsCreateTable::setFileAttr()

//
// trace
//

NATraceList ParDDLFileAttrsCreateTable::getDetailInfo() const {
  NAString detailText;
  NATraceList detailTextList = ParDDLFileAttrsCreateIndex::getDetailInfo();

  detailText = "audited?       ";
  detailText += YesNo(getIsAudit());

  if (isCompressionTypeSpecified()) {
    detailText = "compression?    ";
    switch (getCompressionType()) {
      case COM_NO_COMPRESSION:
        detailText += "N  ";
        break;
      case COM_HARDWARE_COMPRESSION:
        detailText += "H ";
        break;
      case COM_SOFTWARE_COMPRESSION:
        detailText += "S ";
        break;
      default:
        break;
    }
    detailTextList.append(detailText);
  } else
    detailTextList.append("compression not spec");

  detailText += "	range log?       ";
  detailText += ElemDDLFileAttrRangeLog::getRangeLogTypeAsNAString(getRangelogType());

  detailText += "	insertlog?		";
  detailText += YesNo(isInsertLog());

  detailTextList.append(detailText);

  return detailTextList;

}  // ParDDLFileAttrsCreateTable::getDetailInfo()

//
// End of File
//
