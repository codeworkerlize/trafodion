
#ifndef PARDDLFILEATTRSCREATETABLE_H
#define PARDDLFILEATTRSCREATETABLE_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ParDDLFileAttrsCreateTable.h
 * Description:  class to contain all legal file attributes associating
 *               with the DDL statement Create Table -- The parser
 *               constructs a parse node for each file attribute specified
 *               in a DDL statement.  Collecting all file attributes
 *               to a single object (node) helps the user to access
 *               the file attribute information easier.  The user does
 *               not need to traverse the parse sub-tree to look for
 *               each file attribute parse node associating with the
 *               DDL statement.  Default values will be assigned to file
 *               attributes that are not specified in the DDL statement.
 *
 *               Class ParDDLFileAttrsCreateTable does not represent a
 *               parse node.  The class StmtDDLCreateTable representing
 *               a Create Table parse node contains (has a has-a
 *               relationship with) the class ParDDLFileAttrsCreateTable.
 *
 *
 * Created:      5/25/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "parser/ParDDLFileAttrsCreateIndex.h"
#include "parser/ElemDDLFileAttrRangeLog.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ParDDLFileAttrsCreateTable;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None

// -----------------------------------------------------------------------
// definition of class ParDDLFileAttrsCreateTable
// -----------------------------------------------------------------------
class ParDDLFileAttrsCreateTable : public ParDDLFileAttrsCreateIndex {
 public:
  //
  // constructors
  //

  // default constructor
  ParDDLFileAttrsCreateTable(
      ParDDLFileAttrs::fileAttrsNodeTypeEnum fileAttrsNodeType = ParDDLFileAttrs::FILE_ATTRS_CREATE_TABLE);

  // copy constructor
  ParDDLFileAttrsCreateTable(const ParDDLFileAttrsCreateTable &createTableFileAttributes);

  // virtual destructor
  virtual ~ParDDLFileAttrsCreateTable();

  // assignment operator
  ParDDLFileAttrsCreateTable &operator=(const ParDDLFileAttrsCreateTable &rhs);

  //
  // accessors
  //

  inline NABoolean getIsAudit() const;

  inline NABoolean isAuditSpecified() const;

  // returns TRUE if Audit phrase appears;
  // returns FALSE otherwise.

  inline NABoolean isAudited() const;

  // same as getIsAudit()

  NABoolean isNamespaceSpecified() const { return isNamespaceSpec_; }
  NAString getNamespace() const { return namespace_; }

  NABoolean isEncryptionSpecified() const { return isEncryptionSpec_; }
  NAString *getEncryptionOptions() const { return encryptionOptions_; }
  NABoolean encryptRowid() const { return encryptRowid_; }
  NABoolean encryptData() const { return encryptData_; }

  NABoolean isIncrBackupSpecified() const { return isIncrBackupSpec_; }
  NABoolean incrBackupEnabled() const { return incrBackupEnabled_; }

  NABoolean isReadOnlySpecified() const { return isReadOnlySpec_; }
  NABoolean readOnlyEnabled() const { return readOnlyEnabled_; }

  inline ComRangeLogType getRangelogType() const;

  inline NABoolean isRangeLogSpecified() const;

  inline NABoolean isLockOnRefresh() const;

  inline NABoolean isLockOnRefreshSpecified() const;

  NABoolean isRangeLog() const {
    // XXXXXXXXMVSXXXXX BITMAP
    printf("depricated rangelog attribute\n");
    return TRUE;
  }

  inline NABoolean isInsertLog() const;

  inline NABoolean isInsertLogSpecified() const;

  inline ComMvsAllowed getMvsAllowedType() const;

  inline NABoolean isMvsAllowedSpecified() const;

  inline int getCommitEach() const;

  inline NABoolean isCommitEachSpecified() const;

  inline ComMvAuditType getMvAuditType() const;

  inline NABoolean isMvAuditSpecified() const;

  inline NABoolean isOwnerSpecified() const;

  inline const NAString &getOwner() const;

  inline NABoolean isCompressionTypeSpecified() const;

  inline ComCompressionType getCompressionType() const;

  inline NABoolean isColFamSpecified() const;

  inline NAString getColFam() const;

  inline NABoolean isXnReplSpecified() const;

  inline ComReplType xnRepl() const;

  inline NABoolean isStorageTypeSpecified() const;

  inline ComStorageType storageType() const;

  void setStorageType(ComStorageType st) { storageType_ = st; }

  //
  // mutators
  //

  inline NABoolean isStoredDescSpecified() const { return isStoredDescSpec_; }

  inline NABoolean storedDesc() const { return storedDesc_; }

  void setStoredDesc(NABoolean st) { storedDesc_ = st; }

  inline void setDefaultValueForBuffered();

  // If the Buffered phrase does not appear, turns buffering
  // on when audited; otherwise turns buffering off.  If
  // the Buffered phrase appears, this method does nothing.
  //
  // Since this method depends on whether auditing is on or
  // off, this method should be invoked after the entire file
  // attributes clause is scanned.

  inline void setDefaultValueForColFam();

  void setFileAttr(ElemDDLFileAttr *pFileAttrParseNode);

  // trace
  NATraceList getDetailInfo() const;

 private:
  // ---------------------------------------------------------------------
  // private methods
  // ---------------------------------------------------------------------

  void copy(const ParDDLFileAttrsCreateTable &createTableFileAttributes);
  void initializeDataMembers();
  void resetAllIsSpecDataMembers();

  // ---------------------------------------------------------------------
  // private data members
  // ---------------------------------------------------------------------

  // The flags is...Spec_ shows whether the corresponding file
  // attributes were specified in the DDL statement or not.  They
  // are used by the parser to look for duplicate clauses.

  // ALLOCATE
  //   inherits from class ParDDLFileAttrsCreateIndex

  // [ NO ] AUDIT
  //
  //   controls TMF auditing.  Default is TRUE (AUDIT).
  //
  NABoolean isAuditSpec_;
  NABoolean isAudit_;

  NABoolean isNamespaceSpec_;
  NAString namespace_;

  NABoolean isEncryptionSpec_;
  NAString *encryptionOptions_;
  NABoolean encryptRowid_;
  NABoolean encryptData_;

  NABoolean isIncrBackupSpec_;
  NABoolean incrBackupEnabled_;

  NABoolean isReadOnlySpec_;
  NABoolean readOnlyEnabled_;

  //++ MV

  // RANGELOG
  NABoolean isRangeLogSpec_;
  ComRangeLogType rangelogType_;

  // [NO] LOCKONREFRESH
  NABoolean isLockOnRefreshSpec_;
  NABoolean isLockOnRefresh_;

  // [NO] INSERTLOG
  NABoolean isInsertLogSpec_;
  NABoolean isInsertLog_;

  // MVS ALLOWED
  NABoolean isMvsAllowedSpec_;
  ComMvsAllowed mvsAllowedType_;

  // COMMIT EACH nrows
  NABoolean isMvCommitEachSpec_;
  int commitEachNRows_;

  // MV AUDIT
  NABoolean isMvAuditSpec_;
  ComMvAuditType mvAuditType_;

  // OWNER
  NABoolean isOwnerSpec_;
  NAString owner_;

  // COMPRESSION
  NABoolean isCompressionTypeSpec_;
  ComCompressionType compressionType_;

  // COLUMN FAMILY
  NABoolean isColFamSpec_;
  NAString colFam_;

  // transaction replication across multiple clusters
  NABoolean isXnReplSpec_;
  ComReplType xnRepl_;

  NABoolean isStorageTypeSpec_;
  ComStorageType storageType_;

  NABoolean isStoredDescSpec_;
  NABoolean storedDesc_;

  //-- MV

  // ALLOCATE
  //   inherits from class ParDDLFileAttrsCreateIndex

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

  // EXTENT
  //   inherits from class ParDDLFileAttrsCreateIndex

  // MAXEXTENTS
  //   inherits from class ParDDLFileAttrsCreateIndex

  // UID
  //   inherits from class ParDDLFileAttrsCreateIndex

  // { ALIGNED | PACKED } FORMAT
  //   inherits from class ParDDLFileAttrsCreateIndex

};  // class ParDDLFileAttrsCreateTable

// -----------------------------------------------------------------------
// definitions of inline methods for class ParDDLFileAttrsCreateTable
// -----------------------------------------------------------------------

//
// accessors
//

inline NABoolean ParDDLFileAttrsCreateTable::getIsAudit() const { return isAudit_; }

// is Audit phrase appeared?
inline NABoolean ParDDLFileAttrsCreateTable::isAuditSpecified() const { return isAuditSpec_; }

inline NABoolean ParDDLFileAttrsCreateTable::isAudited() const { return getIsAudit(); }

//----------------------------------------------------------------------------
//++ MV

inline ComRangeLogType ParDDLFileAttrsCreateTable::getRangelogType() const { return rangelogType_; }

inline NABoolean ParDDLFileAttrsCreateTable::isRangeLogSpecified() const { return isRangeLogSpec_; }

inline NABoolean ParDDLFileAttrsCreateTable::isLockOnRefresh() const { return isLockOnRefresh_; }

inline NABoolean ParDDLFileAttrsCreateTable::isLockOnRefreshSpecified() const { return isLockOnRefreshSpec_; }

inline NABoolean ParDDLFileAttrsCreateTable::isInsertLog() const { return isInsertLog_; }

inline NABoolean ParDDLFileAttrsCreateTable::isInsertLogSpecified() const { return isInsertLogSpec_; }

inline ComMvsAllowed ParDDLFileAttrsCreateTable::getMvsAllowedType() const { return mvsAllowedType_; }

inline NABoolean ParDDLFileAttrsCreateTable::isMvsAllowedSpecified() const { return isMvsAllowedSpec_; }

int ParDDLFileAttrsCreateTable::getCommitEach() const { return commitEachNRows_; }

NABoolean ParDDLFileAttrsCreateTable::isCommitEachSpecified() const { return isMvCommitEachSpec_; }

ComMvAuditType ParDDLFileAttrsCreateTable::getMvAuditType() const { return mvAuditType_; }

NABoolean ParDDLFileAttrsCreateTable::isMvAuditSpecified() const { return isMvAuditSpec_; }

NABoolean ParDDLFileAttrsCreateTable::isOwnerSpecified() const { return isOwnerSpec_; }

const NAString &ParDDLFileAttrsCreateTable::getOwner() const { return owner_; }

NABoolean ParDDLFileAttrsCreateTable::isCompressionTypeSpecified() const { return isCompressionTypeSpec_; }

ComCompressionType ParDDLFileAttrsCreateTable::getCompressionType() const { return compressionType_; }
//-- MV
//----------------------------------------------------------------------------

// is the Col Family phrase specified?
inline NABoolean ParDDLFileAttrsCreateTable::isColFamSpecified() const { return isColFamSpec_; }

inline NAString ParDDLFileAttrsCreateTable::getColFam() const { return colFam_; }

inline NABoolean ParDDLFileAttrsCreateTable::isXnReplSpecified() const { return isXnReplSpec_; }

inline ComReplType ParDDLFileAttrsCreateTable::xnRepl() const { return xnRepl_; }

inline NABoolean ParDDLFileAttrsCreateTable::isStorageTypeSpecified() const { return isStorageTypeSpec_; }

inline ComStorageType ParDDLFileAttrsCreateTable::storageType() const { return storageType_; }

// void ParDDLFileAttrsCreateTable::setStorageType(ComStorageType st)
//{ storageType_ = st; }

//
// mutator
//

//
// Sets the default value for Buffered phrase.  This method
// only applies to Create Table statements.  The parser
// can not compute the default value for the Buffered file
// attribute in Create Index statements.
//
// If the Buffered phrase does not appear, turns buffering
// on when audited; otherwise turns buffering off.  If
// the Buffered phrase appears, this method does nothing.
//
// Since this method depends on whether auditing is on or
// off, this method should be invoked after the entire file
// attributes clause is scanned.
//
inline void ParDDLFileAttrsCreateTable::setDefaultValueForBuffered() {
  if (NOT isBufferedSpecified()) {
    setIsBuffered(isAudited());
  }
}

inline void ParDDLFileAttrsCreateTable::setDefaultValueForColFam() {
  if (NOT isColFamSpecified()) {
    colFam_ = SEABASE_DEFAULT_COL_FAMILY;
  }
}

#endif  // PARDDLFILEATTRSCREATETABLE_H
