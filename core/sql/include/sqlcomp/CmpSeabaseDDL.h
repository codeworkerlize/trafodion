// @@@ START COPYRIGHT @@@
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// @@@ END COPYRIGHT @@@

/*
 *****************************************************************************
 *
 * File:         CmpSeabaseDDL.h
 * Description:  This file describes the DDL classes for Trafodion
 *
 * Contents:
 *   Metadata table descriptors
 *   class CmpSeabaseDDL
 *
 *****************************************************************************
 */

#ifndef _CMP_SEABASE_DDL_H_
#define _CMP_SEABASE_DDL_H_

#include "comexe/ComTdb.h"
#include "exp/ExpHbaseDefs.h"
#include "exp/ExpLOBenums.h"
#include "sqlcomp/NADefaults.h"
#include "optimizer/NAColumn.h"
#include "optimizer/NAFileSet.h"
#include "comexe/CmpMessage.h"
#include "sqlcomp/PrivMgrDefs.h"
#include "sqlcomp/PrivMgrMD.h"
#include "parser/ElemDDLHbaseOptions.h"
#include "arkcmp/CmpContext.h"
#include "sqlcomp/parser.h"
#include "sqlcomp/PrivMgrMDDefs.h"
#include "sqlcomp/CmpSeabaseBackupAttrs.h"
#include "parser/StmtDDLAlterTableStoredDesc.h"
#include "parser/StmtDDLAlterSharedCache.h"
#include <algorithm>
#include <stack>

class ExpHbaseInterface;
class ExeCliInterface;
class Queue;

class StmtDDLCreateTable;
class StmtDDLDropTable;

class StmtDDLCreateHbaseTable;
class StmtDDLDropHbaseTable;

class StmtDDLCreateIndex;
class StmtDDLDropIndex;
class StmtDDLPopulateIndex;

class StmtDDLAlterTableRename;

class StmtDDLAlterTableAddColumn;
class StmtDDLAlterTableDropColumn;

class StmtDDLAlterTableAlterColumnSetSGOption;

class StmtDDLCreateView;
class StmtDDLDropView;

class StmtDDLCreateLibrary;
class StmtDDLDropLibrary;

class StmtDDLCreateRoutine;
class StmtDDLDropRoutine;

class StmtDDLCreateSequence;
class StmtDDLDropSequence;

class StmtDDLDropSchema;
class StmtDDLAlterSchema;

// Classes for user management
class StmtDDLRegisterUser;
class StmtDDLAlterUser;
class CmpSeabaseDDLauth;
class StmtDDLRegisterComponent;
class PrivMgrComponent;
class StmtDDLUserGroup;

// classes for constraints
class StmtDDLAddConstraint;
class StmtDDLAddConstraintPK;
class StmtDDLAddConstraintUnique;
class StmtDDLAddConstraintRIArray;
class StmtDDLAddConstraintUniqueArray;
class StmtDDLAddConstraintCheckArray;
class StmtDDLDropConstraint;
class StmtDDLAddConstraintCheck;

class ElemDDLColDefArray;
class ElemDDLColRefArray;
class ElemDDLParamDefArray;
class ElemDDLPartitionClause;

class DDLExpr;
class DDLNode;

class RelDumpLoad;
class RelBackupRestore;

class NADefaults;

class NAType;

struct TrafDesc;
class OutputInfo;

class HbaseCreateOption;
class ParDDLFileAttrs;
class ParDDLFileAttrsAlterTable;

class Parser;

class NAColumnArray;

class TrafRoutineDesc;
struct MDDescsInfo;

class CmpDDLwithStatusInfo;

class StmtDDLAlterTableHDFSCache;
class StmtDDLAlterSchemaHDFSCache;

class TenantSchemaInfo;
class TenantSchemaInfoList;

class ObjectEpochCacheEntryName;

class IndependentLockController;

class StmtDDLAlterTableDropPartition;
class StmtDDLAlterTableMergePartition;
class StmtDDLAlterTableExchangePartition;

class ElemDDLPartitionV2Array;

class ElemDDLPartitionNameAndForValuesArray;

#include "sqlcomp/CmpSeabaseDDLmd.h"

// The define below gives the maximum rowID length that we will permit
// for Trafodion tables and indexes. The actual HBase limit is more
// complicated: For Puts, HBase compares the key length to
// HConstants.MAX_ROW_LENGTH (= Short.MAX_VALUE = 32767). It raises an
// exception if the key length is greater than that. But there are also
// some internal data structures that HBase uses (the WAL perhaps?) that
// are keyed. Experiments show that a Trafodion key of length n causes
// a hang if n + strlen(Trafodion object name) + 16 > 32767. The HBase
// log in these cases shows an IllegalArgumentException Row > 32767 in
// this case. So it seems best to limit Trafodion key lengths to something
// sufficiently smaller than 32767 so we don't hit these hangs. A value
// of 32000 seems safe, since the longest Trafodion table name will be
// TRAFODION.something.something, with each of the somethings topping out
// at 256 bytes.
#define MAX_HBASE_ROWKEY_LEN 32000

// The define below gives the maximum Key RowID Length when creating
// table using salt partitions clause, Error[8448] occurs once key length
// greater than the limit when `delete clause` executed on that table. It
// seems to be the best limit value according to test.
#define MAX_HBASE_ROWKEY_LEN_SALT_PARTIONS 1490

#define SEABASEDDL_INTERNAL_ERROR(text)                                                                      \
  *CmpCommon::diags() << DgSqlCode(-CAT_INTERNAL_EXCEPTION_ERROR) << DgString0(__FILE__) << DgInt0(__LINE__) \
                      << DgString1(text)

#define CONCAT_CATSCH(tgt, catname, schname) \
  (tgt = NAString(catname) + NAString(".\"") + NAString(schname) + NAString("\""))

#define HBASE_OPTION_MAX_INTEGER_LENGTH 5

struct objectRefdByMe {
  Int64 objectUID;
  NAString objectType;
  NAString objectName;
  NAString schemaName;
  NAString catalogName;
};

struct VersionInfo {
  Int64 dbMajorVersion_;
  Int64 dbMinorVersion_;
  Int64 dbUpdateVersion_;
  NAString dbVersionStr_;
  Int64 mdMajorVersion_;
  Int64 mdMinorVersion_;
  Int64 mdUpdateVersion_;
  NAString mdVersionStr_;
  Int64 prMajorVersion_;
  Int64 prMinorVersion_;
  Int64 prUpdateVersion_;
  NAString prVersionStr_;

  VersionInfo()
      : dbMajorVersion_(0),
        dbMinorVersion_(0),
        dbUpdateVersion_(0),
        mdMajorVersion_(0),
        mdMinorVersion_(0),
        mdUpdateVersion_(0),
        prMajorVersion_(0),
        prMinorVersion_(0),
        prUpdateVersion_(0) {}
};

// This class encapsulates the information needed to
// preload an NATable object for a table.
class PreloadInfo {
 public:
  PreloadInfo() {}

  PreloadInfo(NAString &cName, NAString &sName, NAString &oName, NAString &oType, NAString &gText)
      : catName(cName), schName(sName), objName(oName), objType(oType), gluedText(gText) {}

  PreloadInfo(PreloadInfo &other)
      : catName(other.catName),
        schName(other.schName),
        objName(other.objName),
        objType(other.objType),
        gluedText(other.gluedText) {}

  ~PreloadInfo() {}

 public:
  NAString catName;
  NAString schName;
  NAString objName;
  NAString objType;
  NAString gluedText;
};

class SystemObjectInfoKey {
 public:
  SystemObjectInfoKey(const char *cName, const char *sName, const char *oName,
                      ComObjectType oType = ComObjectType::COM_UNKNOWN_OBJECT, NABoolean cvd = FALSE,
                      NAHeap *heap = NULL)
      : catName(cName, heap), schName(sName, heap), objName(oName, heap), objectType(oType), checkForValidDef(cvd) {}
  ~SystemObjectInfoKey(){};

  SystemObjectInfoKey(SystemObjectInfoKey &other)
      : catName(other.catName),
        schName(other.schName),
        objName(other.objName),
        objectType(other.objectType),
        checkForValidDef(other.checkForValidDef) {}

  NABoolean operator==(const SystemObjectInfoKey &other) {
    return this->catName == other.catName && this->schName == other.schName && this->objName == other.objName &&
           this->objectType == other.objectType && this->checkForValidDef == other.checkForValidDef;
  }

  // Indicate if the system object can be dropped through SQL.
  NABoolean isDroppable();

  NAString catName;
  NAString schName;
  NAString objName;
  ComObjectType objectType;
  NABoolean checkForValidDef;
};

class SystemObjectInfoValue {
 public:
  SystemObjectInfoValue(Int64 objUID = 0, Int32 objOwner = 0, Int32 schOwner = 0, Int64 flags = 0, Int64 cTime = 0)
      : objectUID(objUID), objectOwner(objOwner), schemaOwner(schOwner), objectFlags(flags), createTime(cTime) {}

  ~SystemObjectInfoValue() {}

  Int64 objectUID;
  Int32 objectOwner;
  Int32 schemaOwner;
  Int64 objectFlags;
  Int64 createTime;
};

typedef NAHashDictionary<SystemObjectInfoKey, SystemObjectInfoValue> SystemObjectInfoHashTableType;

// This class is used to release DDL locks for DDL operations that
// cannot be done in a user transaction. The class decouples DDL lock
// release from transaction commit/abort logic. By creating an instance
// of this class as a stack variable in the method implementing the
// DDL operation, we guarantee that locks will be released upon exit
// from the method, whether it be a normal exit or via an exception.
//
// If we refactor the DDL logic to formalize the notion of scope of
// an operation, this class would be created as a stack variable at
// the beginning of every DDL operation method. It would include the
// mode of the operation: whether it requires multiple transactions,
// or can run in an inherited transaction or whether it is starting
// its own transaction.
class DDLScopeInterface {
 public:
  DDLScopeInterface(CmpContext *currentContext, ComDiagsArea *diags);
  ~DDLScopeInterface();

  void allClear() { allClear_ = true; };  // indicate successful DDL operation

 private:
  bool allClear_;  // if true, call EndDDL; if false, call AbortDDL
  ComDiagsArea *diags_;
  CmpContext *currentContext_;
};

class UpdateObjRefTimeParam {
 public:
  UpdateObjRefTimeParam(Int64 rt = -1, Int64 objUID = -1, NABoolean force = FALSE, NABoolean genStats = FALSE,
                        NABoolean flushData = FALSE);

  Int64 redefTime_;
  Int64 objUID_;
  NABoolean genDesc_;
  NABoolean genStats_;
  NABoolean flushData_;
};

class RemoveNATableParam {
 public:
  RemoveNATableParam(ComQiScope qiScope, NABoolean ddlXns, NABoolean atCommit, Int64 objUID = 0,
                     NABoolean noCheck = FALSE);

  ComQiScope qiScope_;
  NABoolean ddlXns_;
  NABoolean atCommit_;
  Int64 objUID_;
  NABoolean noCheck_;
};

class CmpSeabaseDDL {
  friend class PrivMgrMDAdmin;
  friend class sharedDescriptorCache;
  friend class NATableDB;

 public:
  CmpSeabaseDDL(NAHeap *heap, NABoolean syscatInit = TRUE);
  ~CmpSeabaseDDL();
  static NABoolean isSystemSchema(NAString &schName);
  static NABoolean isSeabase(const NAString &catName);

  static NABoolean isHbase(const NAString &catName);

  static bool isHistogramTable(const NAString &tabName);
  static bool isSampleTable(const NAString &tabName);
  static NABoolean isLOBDependentNameMatch(const NAString &name);
  static NABoolean isSeabaseMD(const NAString &catName, const NAString &schName, const NAString &objName);

  static ComBoolean isSeabaseMD(const ComObjectName &name);

  static NABoolean isSeabasePrivMgrMD(const NAString &catName, const NAString &schName);

  static ComBoolean isSeabasePrivMgrMD(const ComObjectName &name);

  static NABoolean isREPOSSchema(const NAString &catName, const NAString &schName);

  static ComBoolean isREPOSSchema(const ComObjectName &name);

  static NABoolean isXDCSchema(const NAString &catName, const NAString &schName);

  static ComBoolean isXDCSchema(const ComObjectName &name);

  static NABoolean isUserUpdatableSeabaseMD(const NAString &catName, const NAString &schName, const NAString &objName);

  static NABoolean isSeabaseReservedSchema(const NAString &catName, const NAString &schName);

  static NABoolean isSeabaseExternalSchema(const NAString &catName, const NAString &schName);

  static NABoolean isTrafBackupSchema(const NAString &catName, const NAString &schName);

  static short isSPSQLPackageRoutine(const char *routineName, ExeCliInterface *cliInterface, const SchemaName &sn,
                                     Queue *inPkgQueue = NULL);

  static short getTextFromMD(const char *catalogName, ExeCliInterface *cliInterface, Int64 constrUID,
                             ComTextType textType, Lng32 textSubID, NAString &constrText, NABoolean binaryData = FALSE);

  static short getPartIndexName(ExeCliInterface *cliInterface, const NAString &schemaName, const NAString &index_name,
                                const NAString &partitionName, NAString &partitionIndexName);

  static short createHistogramTables(ExeCliInterface *cliInterface, const NAString &schemaName,
                                     const NABoolean ignoreIfExists, NAString &tableNotCreated);

  static void getPartitionTableName(char *buf, int bufLen, const NAString &baseTableName,
                                    const NAString &partitionName);

  static void getPartitionIndexName(char *buf, int bufLen, const NAString &baseIndexName,
                                    const char *partitionTableName);

  static std::vector<std::string> getHistogramTables();

  static short invalidateStats(Int64 tableUID);

  static Int32 createObjectEpochCacheEntry(ObjectEpochCacheEntryName *name);

  static Int32 modifyObjectEpochCacheStartDDL(ObjectEpochCacheEntryName *name, UInt32 expectedCurrentEpoch,
                                              bool readsAllowed);

  static Int32 modifyObjectEpochCacheContinueDDL(ObjectEpochCacheEntryName *name, UInt32 expectedCurrentEpoch);

  static Int32 modifyObjectEpochCacheEndDDL(ObjectEpochCacheEntryName *name, UInt32 expectedCurrentEpoch);

  static Int32 modifyObjectEpochCacheAbortDDL(ObjectEpochCacheEntryName *name, UInt32 expectedCurrentEpoch, bool force);

  static short ddlResetObjectEpochs(NABoolean doAbort, NABoolean clearList, ComDiagsArea *diags = NULL);
  short ddlSetObjectEpoch(const NATable *naTable, NAHeap *heap, bool allowReads = false, bool isContinue = false);
  short ddlSetObjectEpoch(const StmtDDLCreateTable *createTableNode, NAHeap *heap, bool allowReads = false,
                          bool isContinue = false);
  short ddlSetObjectEpochHelper(const QualifiedName &qualName, ComObjectType objectType, Int64 objectUID,
                                Int64 redefTime, bool isVolatile, bool isExternal, NAHeap *heap,
                                bool allowReads = false, bool isContinue = false);

  short lockObjectDDL(const NATable *naTable);
  short lockObjectDDL(const StmtDDLCreateTable *createTableNode);
  short lockObjectDDL(const StmtDDLAlterTable *alterTableNode);
  short lockObjectDDL(const QualifiedName &qualName, ComObjectType objectType, bool isVolatile, bool isExternal);
  short lockObjectDDL(const ComObjectName &objectName, ComObjectType objectType, bool isVolatile, bool isExternal);
  short unlockObjectDDL(const QualifiedName &qualName, ComObjectType objectType, bool isVolatile, bool isExternal);
  short lockObjectDDL(const NATable *naTable, NAString &catalogPart, NAString &schemaPart,
                      NABoolean isPartitionV2Table);

 public:
  short genHbaseRegionDescs(TrafDesc *desc, const NAString &catName, const NAString &schName, const NAString &objName,
                            const NAString &nameSpace);

  NABoolean isAuthorizationEnabled();

  short existsInHbase(const NAString &objName, ExpHbaseInterface *ehi);

  Lng32 tableIsEmpty(const NAString &objName, ExpHbaseInterface *ehi);

  void processSystemCatalog(NADefaults *defs);

  short isPrivMgrMetadataInitialized(NADefaults *defs = NULL, NABoolean checkAllPrivTables = FALSE);

  short readAndInitDefaultsFromSeabaseDefaultsTable(NADefaults::Provenance overwriteIfNotYet, Int32 errOrWarn,
                                                    NADefaults *defs);

  short getSystemSoftwareVersion(Int64 &softMajVers, Int64 &softMinVers, Int64 &softUpdVers);

  static short getVersionInfo(VersionInfo &versionInfo);

  short validateVersions(NADefaults *defs, ExpHbaseInterface *inEHI = NULL, Int64 *mdMajorVersion = NULL,
                         Int64 *mdMinorVersion = NULL, Int64 *mdUpdateVersion = NULL, Int64 *sysSWMajorVersion = NULL,
                         Int64 *sysSWMinorVersion = NULL, Int64 *sysSWUpdVersion = NULL, Int64 *mdSWMajorVersion = NULL,
                         Int64 *mdSWMinorVersion = NULL, Int64 *mdSWUpdateVersion = NULL, Lng32 *hbaseErr = NULL,
                         NAString *hbaseErrStr = NULL);

  short executeSeabaseDDL(DDLExpr *ddlExpr, ExprNode *ddlNode, NAString &currCatName, NAString &currSchName,
                          CmpDDLwithStatusInfo *dws = NULL);

  // includeBRLockedTables: 0, dont return tables locked by BR
  //                        1, return tables locked by Backup
  //                        2, return tables locked by Restore
  //                        3, return tables locked by Backup or Restore
  TrafDesc *getSeabaseTableDesc(const NAString &catName, const NAString &schName, const NAString &objName,
                                const ComObjectType objType, NABoolean includeInvalidDefs = FALSE,
                                short includeBRLockedTables = 0);

  // chech whether sample table exist, if true, return sampleTableName
  static NABoolean isSampleExist(const NAString &catName, const NAString &schName, const Int64 tableUID,
                                 NAString &sampleTableName, Int64 &sampleTableUid);

  // drop sample table and clear infomation of sample table in SB_PERSISTENT_SAMPLES
  short dropSample(const NAString &catalogNamePart, const NAString &schemaNamePart, const Int64 tableUID,
                   const NAString &sampleTableName);

  short getSeabaseObjectComment(Int64 object_uid, enum ComObjectType object_type,
                                ComTdbVirtObjCommentInfo &comment_info, CollHeap *heap);

  short getObjectOwner(ExeCliInterface *cliInterface, const char *catName, const char *schName, const char *objName,
                       const char *objType, Int32 *objectOwner);
  short getObjectFlags(ExeCliInterface *cliInterface, const char *catName, const char *schName, const char *objName,
                       const char *objType, Int64 *objectOwner);

  static bool describeSchema(const NAString &catalogName, const NAString &schemaName, const bool checkPrivs,
                             NABoolean isHiveRegistered, std::vector<std::string> &outlines);

  static Int64 getObjectTypeandOwner(ExeCliInterface *cliInterface, const char *catName, const char *schName,
                                     const char *objName, ComObjectType &objectType, Int32 &objectOwner,
                                     Int64 *objectFlags = NULL);

  short getSaltText(ExeCliInterface *cliInterface, const char *catName, const char *schName, const char *objName,
                    const char *inObjType, NAString &saltText);

  short getTrafReplicaNum(ExeCliInterface *cliInterface, const char *catName, const char *schName, const char *objName,
                          const char *inObjType, Int16 &numTrafReplicas);

  static short genHbaseCreateOptions(const char *hbaseCreateOptionsStr,
                                     NAList<HbaseCreateOption *> *&hbaseCreateOptions, NAMemory *heap, size_t *beginPos,
                                     size_t *endPos);

  static short genHbaseOptionsMetadataString(const NAList<HbaseCreateOption *> &hbaseCreateOptions,
                                             NAString &hbaseOptionsMetadataString /* out */);

  short updateHbaseOptionsInMetadata(ExeCliInterface *cliInterface, Int64 objectUID, ElemDDLHbaseOptions *edhbo);

  TrafDesc *getSeabaseLibraryDesc(const NAString &catName, const NAString &schName, const NAString &libraryName);

  TrafDesc *getSeabaseRoutineDesc(const NAString &catName, const NAString &schName, const NAString &objName);

  static NABoolean getOldMDInfo(const MDTableInfo &mdti, const char *&oldName, const QString *&oldDDL,
                                Lng32 &sizeOfOldDDL);

  static NABoolean getOldMDPrivInfo(const PrivMgrTableStruct &mdti, const char *&oldName, const QString *&oldDDL,
                                    Lng32 &sizeOfoldDDL);

  short createMDdescs(MDDescsInfo *&);

  static NAString getSystemCatalogStatic();

  static NABoolean isEncodingNeededForSerialization(NAColumn *nac);

  int32_t verifyDDLCreateOperationAuthorized(ExeCliInterface *cliInterface, SQLOperation operation,
                                             const NAString &catalogName, const NAString &schemaName,
                                             ComSchemaClass &schemaClass, Int32 &objectOwner, Int32 &schemaOwner,
                                             Int64 *schemaUID = NULL, Int64 *schemaObjectFlags = NULL);

  bool isDDLOperationAuthorized(SQLOperation operation, const Int32 objOwnerId, const Int32 schemaOwnerID,
                                const Int64 objectUID, const ComObjectType objectType,
                                const PrivMgrUserPrivs *privInfo);

  static Int64 authIDOwnsResources(Int32 authID, NAString &resourceNames);

  static NABoolean enabledForSerialization(NAColumn *nac);

  static NABoolean isSerialized(ULng32 flags) { return (flags & NAColumn::SEABASE_SERIALIZED) != 0; }

  short buildColInfoArray(ComObjectType objType, NABoolean isMetadataHistOrReposObject, ElemDDLColDefArray *colArray,
                          ComTdbVirtTableColumnInfo *colInfoArray, NABoolean implicitPK, NABoolean alignedFormat,
                          Lng32 *identityColPos = NULL, std::vector<NAString> *userColFamVec = NULL,
                          std::vector<NAString> *trafColFamVec = NULL, const char *defaultColFam = NULL,
                          NAMemory *heap = NULL);

  // The next three methods do use anything from the CmpSeabaseDDL class.
  // They are placed here as a packaging convinience, to avoid code
  // duplication that would occur if non-member static functions were used.
  // These methods convert VirtTable*Info classes to corresponding TrafDesc
  // objects
  void convertVirtTableColumnInfoToDescStruct(const ComTdbVirtTableColumnInfo *colInfo, const ComObjectName *objectName,
                                              TrafDesc *column_desc);

  TrafDesc *convertVirtTableColumnInfoArrayToDescStructs(const ComObjectName *objectName,
                                                         const ComTdbVirtTableColumnInfo *colInfoArray, Lng32 numCols);

  TrafDesc *convertVirtTableKeyInfoArrayToDescStructs(const ComTdbVirtTableKeyInfo *keyInfoArray,
                                                      const ComTdbVirtTableColumnInfo *colInfoArray, Lng32 numKeys);

  Int64 getObjectUID(ExeCliInterface *cliInterface, const char *catName, const char *schName, const char *objName,
                     const char *inObjType, Int64 *objDataUID = NULL, const char *inObjTypeStr = NULL,
                     char *outObjType = NULL, NABoolean lookInObjectsIdx = FALSE, NABoolean reportErrorNow = TRUE,
                     Int64 *objectFlags = NULL);

  Int64 getObjectUID(ExeCliInterface *cliInterface, Int64 objDataUID, NABoolean isOBJUID);

  // TODO: get schema uid
  Int64 getSchemaUID(const char *catName, const char *schName);
  // TODO: get schema name
  short getSchemaName(const char *schUID, NAString &schName);

  Int64 getObjectInfo(ExeCliInterface *cliInterface, const char *catName, const char *schName, const char *objName,
                      const ComObjectType objectType, Int32 &objectOwner, Int32 &schemaOwner, Int64 &objectFlags,
                      Int64 &objDataUID, bool reportErrorNow = true, NABoolean checkForValidDef = FALSE,
                      Int64 *createTime = NULL, Int64 *redefTime = NULL);

  Int64 getSystemObjectInfo(ExeCliInterface *cliInterface, const char *catName, const char *schName,
                            const char *objName, const ComObjectType objectType, Int32 &objectOwner, Int32 &schemaOwner,
                            Int64 &objectFlags, Int64 &objDataUID, bool reportErrorNow = true,
                            NABoolean checkForValidDef = FALSE, Int64 *createTime = NULL);

  Lng32 getIndexInfo(ExeCliInterface &cliInterface, NABoolean includeInvalidDefs, Int64 objUID, Queue *&indexQueue);

  static short getObjectName(ExeCliInterface *cliInterface, Int64 objUID, NAString &catName, NAString &schName,
                             NAString &objName, char *outObjType = NULL, NABoolean lookInObjects = FALSE,
                             NABoolean lookInObjectsIdx = FALSE);

  short getObjectValidDef(ExeCliInterface *cliInterface, const char *catName, const char *schName, const char *objName,
                          const ComObjectType objectType, NABoolean &validDef);

  static short genTrafColFam(int index, NAString &trafColFam);

  static short extractTrafColFam(const NAString &trafColFam, int &index);

  short processColFamily(NAString &inColFamily, NAString &outColFamily, std::vector<NAString> *userColFamVec,
                         std::vector<NAString> *trafColFamVec);

  short switchCompiler(Int32 cntxtType = CmpContextInfo::CMPCONTEXT_TYPE_META);

  short switchBackCompiler();

  ExpHbaseInterface *allocEHI(ComStorageType storageType);

  short ddlInvalidateNATables(NABoolean isAbort, Int32 processSP = 0, Int64 svptId = 0);
  short deleteFromSharedCache(ExeCliInterface *cliInterface, const NAString objName, ComObjectType objType,
                              NAString cmd);
  short finalizeSharedCache();
  void insertIntoSharedCacheList(const NAString &catName, const NAString &schName, const NAString &objName,
                                 ComObjectType objType, SharedCacheDDLInfo::DDLOperation ddlOperation,
                                 SharedCacheDDLInfo::CachedType cachedType = SharedCacheDDLInfo::SHARED_DESC_CACHE);

  void deallocEHI(ExpHbaseInterface *&ehi);

  void deallocBRCEHI(ExpHbaseInterface *&ehi);

  short dropCreateReservedNamespaces(ExpHbaseInterface *ehi, NABoolean drop, NABoolean create);
  short createNamespace(ExpHbaseInterface *ehi, NAString &ns, NABoolean createReservedNS);

  static void setMDflags(Int64 &flags,  // INOUT
                         Int64 bitFlags) {
    flags |= bitFlags;
  }

  static void resetMDflags(Int64 &flags,  // INOUT
                           Int64 bitFlags) {
    flags &= ~bitFlags;
  }

  static NABoolean isMDflagsSet(Int64 flags, Int64 bitFlags) { return (flags & bitFlags) != 0; }

  enum {
    // set if descr is to be generated in packed format to be stored in metadata
    GEN_PACKED_DESC = 0x0004,

    // set if stored object descriptor is to be read from metadata.
    READ_OBJECT_DESC = 0x0008,

    // set if stats are to be generated in packed format and stored along
    // with the descriptor
    GEN_STATS = 0x0010,

    // flush rows to disk and then estimate stats
    FLUSH_DATA = 0x0020
  };

  enum { MD_TABLE_CONSTRAINTS_PKEY_NOT_SERIALIZED_FLG = 0x0001 };

 protected:
  void setFlags(ULng32 &flags, ULng32 flagbits) { flags |= flagbits; }

  void resetFlags(ULng32 &flags, ULng32 flagbits) { flags &= ~flagbits; }

  inline const char *getMDSchema() { return seabaseMDSchema_.data(); };

  const char *getSystemCatalog();

  ComBoolean isSeabaseReservedSchema(const ComObjectName &name);

  ComBoolean isSeabase(const ComObjectName &name);

  ComBoolean isHbase(const ComObjectName &name);

  short doNamespaceCheck(ExpHbaseInterface *ehi, NABoolean &isDef, NABoolean &isRes);
  short isMetadataInitialized(ExpHbaseInterface *ehi = NULL);
  short isOldMetadataInitialized(ExpHbaseInterface *ehi);

  ExpHbaseInterface *allocEHI(const char *server, const char *zkport, NABoolean raiseError, ComStorageType storageType);

  void saveAllFlags();
  void setAllFlags();
  void restoreAllFlags();

  // if prevContext is defined, get user CQDs from the controlDB of
  // previous context and send them to the new cmp context
  short sendAllControlsAndFlags(CmpContext *prevContext = NULL, Int32 cntxtType = -1);

  void restoreAllControlsAndFlags();

  void processReturn(Lng32 retcode = 0);

  // construct and return the column name value as stored with hbase rows.
  // colNum is 0-based (first col is 0)
  void getColName(const ComTdbVirtTableColumnInfo columnInfo[], Lng32 colNum, NAString &colName);

  void getColName(const char *colFam, const char *colQual, NAString &colName);

  TrafDesc *getSeabaseRoutineDescInternal(const NAString &catName, const NAString &schName, const NAString &objName);

 public:
  // note: this function expects hbaseCreateOptionsArray to have
  // HBASE_MAX_OPTIONS elements
  static short generateHbaseOptionsArray(NAText *hbaseCreateOptionsArray,
                                         NAList<HbaseCreateOption *> *hbaseCreateOptions);

  static short createHbaseTable(ExpHbaseInterface *ehi, HbaseStr *table, std::vector<NAString> &collFamVec,
                                NAList<HbaseCreateOption *> *hbaseCreateOptions = NULL, const int numSplits = 0,
                                const int keyLength = 0, char **encodedKeysBuffer = NULL, NABoolean doRetry = TRUE,
                                NABoolean ddlXns = FALSE, NABoolean incrBackupEnabled = FALSE,
                                NABoolean createIfNotExists = FALSE, NABoolean isMVCC = TRUE);

 protected:
  static short createHbaseTable(ExpHbaseInterface *ehi, HbaseStr *table, const char *cf1,
                                NAList<HbaseCreateOption *> *hbaseCreateOptions = NULL, const int numSplits = 0,
                                const int keyLength = 0, char **encodedKeysBuffer = NULL, NABoolean doRetry = FALSE,
                                NABoolean ddlXns = FALSE, NABoolean incrBackupEnabled = FALSE,
                                NABoolean createIfNotExists = FALSE, NABoolean isMVCC = TRUE);

  short createMonarchTable(ExpHbaseInterface *ehi, HbaseStr *table, const int tableType, NAList<HbaseStr> &cols,
                           NAList<HbaseCreateOption *> *inMonarchCreateOptions = NULL, const int numSplits = 0,
                           const int keyLength = 0, char **encodedKeysBuffer = NULL, NABoolean doRetry = TRUE);

  short alterHbaseTable(ExpHbaseInterface *ehi, HbaseStr *table, NAList<NAString> &allColFams,
                        NAList<HbaseCreateOption *> *hbaseCreateOptions, NABoolean ddlXns);

 public:
  static short dropHbaseTable(ExpHbaseInterface *ehi, HbaseStr *table, NABoolean asyncDrop, NABoolean ddlXns0);

 protected:
  short dropMonarchTable(HbaseStr *table, NABoolean asyncDrop, NABoolean ddlXns);

  short copyHbaseTable(ExpHbaseInterface *ehi, HbaseStr *currTable, HbaseStr *oldTable,
                       NABoolean force = FALSE /*remove tgt before copy*/);

  NABoolean xnInProgress(ExeCliInterface *cliInterface);
  short beginXn(ExeCliInterface *cliInterface);
  short commitXn(ExeCliInterface *cliInterface);
  short rollbackXn(ExeCliInterface *cliInterface);
  short autoCommit(ExeCliInterface *cliInterface, NABoolean v);
  short beginXnIfNotInProgress(ExeCliInterface *cliInterface, NABoolean &xnWasStartedHere,
                               NABoolean clearDDLList = TRUE, NABoolean clearObjectLocks = TRUE);
  short endXnIfStartedHere(ExeCliInterface *cliInterface, NABoolean &xnWasStartedHere, Int32 cliRC,
                           NABoolean modifyEpochs = TRUE, NABoolean clearObjectLocks = TRUE);

  short dropSeabaseObject(ExpHbaseInterface *ehi, const NAString &objName, NAString &currCatName, NAString &currSchName,
                          const ComObjectType objType, NABoolean ddlXns, NABoolean dropFromMD,
                          NABoolean dropFromStorage, NABoolean isMonarch, NAString tableNamespace,
                          NABoolean updPrivs = TRUE, Int64 *dataUID = NULL, NAString **objDataUIDName = NULL,
                          Int16 recoderHbName = 0);
  // 0 is do nothing
  // 1 is recode hbase name
  // 2 is use the input hbase name

  short dropSeabaseStats(ExeCliInterface *cliInterface, const char *catName, const char *schName, Int64 tableUID);

  short checkDefaultValue(const NAString &colExtName, const NAType *inColType, ElemDDLColDef *colNode);

  short getTypeInfo(const NAType *naType, NABoolean alignedFormat, Lng32 serializedOption, Lng32 &datatype,
                    Lng32 &length, Lng32 &precision, Lng32 &scale, Lng32 &dtStart, Lng32 &dtEnd, Lng32 &upshifted,
                    Lng32 &nullable, NAString &charset, CharInfo::Collation &collationSequence, ULng32 &colFlags);

  short getColInfo(ElemDDLColDef *colNode, NABoolean isMetadataHistOrReposColumn, NAString &colFamily,
                   NAString &colName, NABoolean alignedFormat, Lng32 &datatype, Lng32 &length, Lng32 &precision,
                   Lng32 &scale, Lng32 &dtStart, Lng32 &dtEnd, Lng32 &upshifted, Lng32 &nullable, NAString &charset,
                   ComColumnClass &colClass, ComColumnDefaultClass &defaultClass, NAString &defVal, NAString &heading,
                   ComLobsStorageType &lobStorage, NAString &compDefnStr, ULng32 &hbaseColFlags, Int64 &colFlags);

  short getNAColumnFromColDef(ElemDDLColDef *colNode, NAColumn *&naCol);

  short createRowId(NAString &key, NAString &part1, Lng32 part1MaxLen, NAString &part2, Lng32 part2MaxLen,
                    NAString &part3, Lng32 part3MaxLen, NAString &part4, Lng32 part4MaxLen);

  short existsInSeabaseMDTable(ExeCliInterface *cliInterface, const char *catName, const char *schName,
                               const char *objName, const ComObjectType objectType = COM_UNKNOWN_OBJECT,
                               NABoolean checkForValidDef = TRUE, NABoolean checkForValidHbaseName = TRUE,
                               NABoolean returnInvalidStateError = FALSE);

  Int64 getConstraintOnIndex(ExeCliInterface *cliInterface, Int64 btUID, Int64 indexUID, const char *constrType,
                             NAString &catName, NAString &schName, NAString &objName);

  short getBaseTable(ExeCliInterface *cliInterface, const NAString &indexCatName, const NAString &indexSchName,
                     const NAString &indexObjName, NAString &btCatName, NAString &btSchName, NAString &btObjName,
                     Int64 &btUID, Int32 &btObjOwner, Int32 &btSchemaOwner);

  short getUsingObject(ExeCliInterface *cliInterface, Int64 objUID, NAString &usingObjName);

  short getUsingRoutines(ExeCliInterface *cliInterface, Int64 objUID, Queue *&usingRoutinesQueue);

  short getUsingViews(ExeCliInterface *cliInterface, Int64 objectUID, Queue *&usingViewsQueue);

  short getUsingViews(ExeCliInterface *cliInterface, Int64 objectUID, const NAString &colName, Queue *&usingViewsQueue);

  short getAllUsingViews(ExeCliInterface *cliInterface, NAString &catName, NAString &schName, NAString &objName,
                         Queue *&usingViewsQueue);

  void handleDDLCreateAuthorizationError(int32_t SQLErrorCode, const NAString &catalogName, const NAString &schemaName);

  short updateSeabaseMDObjectsTable(ExeCliInterface *cliInterface, const char *catName, const char *schName,
                                    const char *objName, const ComObjectType &objectType, const char *validDef,
                                    Int32 objOwnerID, Int32 schemaOwnerID, Int64 objectFlags, Int64 &inUID);

  short deleteFromSeabaseMDObjectsTable(ExeCliInterface *cliInterface, const char *catName, const char *schName,
                                        const char *objName, const ComObjectType &objectType);

  short getAllIndexes(ExeCliInterface *cliInterface, Int64 objUID, NABoolean includeInvalidDefs,
                      Queue *&indexInfoQueue);

  short updateSeabaseMDTable(ExeCliInterface *cliInterface, const char *catName, const char *schName,
                             const char *objName, const ComObjectType &objectType, const char *validDef,
                             ComTdbVirtTableTableInfo *tableInfo, Lng32 numCols,
                             const ComTdbVirtTableColumnInfo *colInfo, Lng32 numKeys,
                             const ComTdbVirtTableKeyInfo *keyInfo, Lng32 numIndexes,
                             const ComTdbVirtTableIndexInfo *indexInfo, Int64 &inUID, NABoolean updPrivs = TRUE);
  short updateSeabaseMDPartition(ExeCliInterface *cliInterface, StmtDDLCreateTable *createTableNode,
                                 const char *catName, const char *schName, const char *entityName, Int64 btUid,
                                 Lng32 *partitionColIdx, Lng32 *subpartitionColIdx);

  short updateSeabaseTablePartitions(ExeCliInterface *cliInterface, Int64 parentUid, const char *partName,
                                     const char *partEntityName, Int64 partitionUid, const char *partitionLevel,
                                     Int32 partitionPos, const char *partitionExp, const char *status = NULL,
                                     const char *readonly = NULL, const char *inMemory = NULL, Int64 flags = 0);

  short addSeabaseMDPartition(ExeCliInterface *cliInterface, ElemDDLPartitionV2 *pPartition, const char *catName,
                              const char *schName, const char *entityName, Int64 btUid, int index, Int64 partUid = -1,
                              const char *partName = NULL);

  short deleteFromSeabaseMDTable(ExeCliInterface *cliInterface, const char *catName, const char *schName,
                                 const char *objName, const ComObjectType objType);

  short updateSeabaseMDSecondaryIndexes(ExeCliInterface *cliInterface);
  short updateSeabaseMDLibrary(ExeCliInterface *cliInterface, const char *catName, const char *schName,
                               const char *libName, const char *libPath, const Int32 libVersion, Int64 &libObjUID,
                               const Int32 ownerID, const Int32 schemaOwnerID);

  short updateSeabaseMDSPJ(ExeCliInterface *cliInterface, const Int64 libObjUID, const Int32 ownerID,
                           const Int32 schemaOwnerID, const ComTdbVirtTableRoutineInfo *routineInfo, Lng32 numCols,
                           const ComTdbVirtTableColumnInfo *colInfo);

  short deleteConstraintInfoFromSeabaseMDTables(ExeCliInterface *cliInterface, Int64 tableUID,
                                                Int64 otherTableUID,  // valid for ref constrs
                                                Int64 constrUID,
                                                Int64 otherConstrUID,  // valid for ref constrs
                                                const char *constrCatName, const char *constrSchName,
                                                const char *constrObjName, const ComObjectType constrType);

  short updateObjectName(ExeCliInterface *cliInterface, Int64 objUID, const char *catName, const char *schName,
                         const char *objName);

  // retrieved stored desc from metadata, check if it is good,
  // and set retDesc, if passed in.
  short checkAndGetStoredObjectDesc(ExeCliInterface *cliInterface, Int64 objUID, TrafDesc **retDesc,
                                    NABoolean checkStats, NAHeap *heap);

  short updateObjectRedefTime(ExeCliInterface *cliInterface, const NAString &catName, const NAString &schName,
                              const NAString &objName, const char *objType, Int64 rt = -1, Int64 objUID = -1,
                              NABoolean force = FALSE, NABoolean genStats = FALSE, NABoolean flushData = FALSE);

  short updateObjectStats(ExeCliInterface *cliInterface, const NAString &catName, const NAString &schName,
                          const NAString &objName, const char *objType, Int64 rt = -1, Int64 objUID = -1,
                          NABoolean force = FALSE, NABoolean flushData = FALSE);

  short updateObjectValidDef(ExeCliInterface *cliInterface, const char *catName, const char *schName,
                             const char *objName, const char *objType, const char *validDef);

  short updateObjectAuditAttr(ExeCliInterface *cliInterface, const char *catName, const char *schName,
                              const char *objName, NABoolean audited, const NAString &objType);

  short updateObjectFlags(ExeCliInterface *cliInterface, const Int64 objUID, const Int64 inFlags, NABoolean reset);

  // subID: 0, for text that belongs to table. colNumber, for column based text.
  short updateTextTable(ExeCliInterface *cliInterface, Int64 objUID, ComTextType textType, Lng32 subID,
                        NAString &textInputData, char *binaryInputData = NULL, Lng32 binaryInputDataLen = -1,
                        NABoolean withDelete = FALSE);  // del before ins

  // input data in non-char format.
  short updateTextTableWithBinaryData(ExeCliInterface *cliInterface, Int64 objUID, ComTextType textType, Lng32 subID,
                                      char *data, Int32 dataLen, NABoolean withDelete);

  short deleteFromTextTable(ExeCliInterface *cliInterface, Int64 objUID, ComTextType textType, Lng32 subID);

  ItemExpr *bindDivisionExprAtDDLTime(ItemExpr *expr, NAColumnArray *availableCols, NAHeap *heap);
  short validateDivisionByExprForDDL(ItemExpr *divExpr);

  short validatePartitionByExpr(ElemDDLPartitionV2Array *partitionV2Array, TrafDesc *colDescs,
                                TrafDesc *partitionColDescs, Int32 partColCnt, Int32 pLength,
                                NAHashDictionary<NAString, Int32> *partitionNameMap = NULL);

  short validatePartitionRange(ElemDDLPartitionV2Array *partitionV2Array, Int32 colCnt);

  short createEncodedKeysBuffer(char **&encodedKeysBuffer, int &numSplits, TrafDesc *colDescs, TrafDesc *keyDescs,
                                int numSaltPartitions, Lng32 numSaltSplits, NAString *splitByClause,
                                Int16 numTrafReplicas, Lng32 numKeys, Lng32 keyLength, NABoolean isIndex);

  short validateRoutine(ExeCliInterface *cliInterface, const char *className, const char *methodName,
                        const char *externalPath, char *signature, Int32 numSqlParam, Int32 maxResultSets,
                        const char *optionalSig);

  short populateKeyInfo(ComTdbVirtTableKeyInfo &keyInfo, OutputInfo *oi, NABoolean isIndex = FALSE);

  short dropMDTable(ExpHbaseInterface *ehi, const char *tab);

  short populateSeabaseIndexFromTable(ExeCliInterface *cliInterface, NABoolean uniqueIndex, const NAString &indexName,
                                      const ComObjectName &tableName, NAList<NAString> &selColList, NABoolean useLoad);
  // Populate data for ngram index
  short populateNgramIndexFromTable(ExeCliInterface *cliInterface, NABoolean uniqueIndex, const NAString &indexName,
                                    const ComObjectName &tableName, NAList<NAString> &selColList, NABoolean useLoad);

  short buildViewText(StmtDDLCreateView *createViewParseNode, NAString &viewText);

  short buildViewColInfo(StmtDDLCreateView *createViewParseNode, ElemDDLColDefArray *colDefArray);

  short buildViewTblColUsage(const StmtDDLCreateView *createViewParseNode,
                             const ComTdbVirtTableColumnInfo *colInfoArray, const Int64 viewObjUID,
                             NAString &viewColUsageText);

  short buildColInfoArray(ElemDDLParamDefArray *paramArray, ComTdbVirtTableColumnInfo *colInfoArray);

  short buildKeyInfoArray(ElemDDLColDefArray *colArray, NAColumnArray *nacolArray, ElemDDLColRefArray *keyArray,
                          ComTdbVirtTableColumnInfo *colInfoArray, ComTdbVirtTableKeyInfo *keyInfoArray,
                          NABoolean allowNullableUniqueConstr, Lng32 *keyLength = NULL, NAMemory *heap = NULL);

  const char *computeCheckOption(StmtDDLCreateView *createViewParseNode);

  short updateViewUsage(StmtDDLCreateView *createViewParseNode, Int64 viewUID, ExeCliInterface *cliInterface,
                        NAList<objectRefdByMe> *lockedBaseTables /* out */);

  short gatherViewPrivileges(const StmtDDLCreateView *createViewParseNode, ExeCliInterface *cliInterface,
                             NABoolean viewCreator, Int32 userID, PrivMgrBitmap &privilegesBitmap,
                             PrivMgrBitmap &grantableBitmap);

  short getListOfReferencedTables(ExeCliInterface *cliInterface, const Int64 objectUID,
                                  NAList<objectRefdByMe> &tablesList);

  short getListOfDirectlyReferencedObjects(ExeCliInterface *cliInterface, const Int64 objectUID,
                                           NAList<objectRefdByMe> &objectsList);

  short constraintErrorChecks(ExeCliInterface *cliInterface, StmtDDLAddConstraint *addConstrNode, NATable *naTable,
                              ComConstraintType ct, NAList<NAString> &keyColList);

  short updateConstraintMD(NAList<NAString> &keyColList, NAList<NAString> &keyColOrderList, NAString &uniqueStr,
                           Int64 tableUID, Int64 uniqueUID, NATable *naTable, ComConstraintType ct, NABoolean enforced,
                           ExeCliInterface *cliInterface);

  short updateRIConstraintMD(Int64 ringConstrUID, Int64 refdConstrUID, ExeCliInterface *cliInterface);

  short updatePKeyInfo(StmtDDLAddConstraintPK *addPKNode, const char *catName, const char *schName, const char *objName,
                       const Int32 ownerID, const Int32 schemaOwnerID, Lng32 numKeys, Int64 *outPkeyUID,
                       Int64 *outTableUID, const ComTdbVirtTableKeyInfo *keyInfoArray, ExeCliInterface *cliInterface);

  short getPKeyInfoForTable(const char *catName, const char *schName, const char *objName,
                            ExeCliInterface *cliInterface, NAString &constrName, Int64 &constrUID,
                            NABoolean errorIfNotExists = TRUE);

  short updateRIInfo(StmtDDLAddConstraintRIArray &riArray, const char *catName, const char *schName,
                     const char *objName, ExeCliInterface *cliInterface);

  short genUniqueName(StmtDDLAddConstraint *addUniqueNode, Int32 *unnamedConstrNum, NAString &pkeyName,
                      const char *catName = NULL, const char *schName = NULL, const char *objName = NULL);

  short updateIndexInfo(NAList<NAString> &ringKeyColList, NAList<NAString> &ringKeyColOrderList,
                        NAList<NAString> &refdKeyColList, NAString &uniqueStr, Int64 constrUID, const char *catName,
                        const char *schName, const char *objName, NATable *naTable,
                        NABoolean isUnique,            // TRUE: uniq constr. FALSE: ref constr.
                        NABoolean noPopulate,          // TRUE, dont populate index
                        NABoolean isEnforced,          // TRUE: contraint is enforced
                        NABoolean sameSequenceOfCols,  // FALSE, allow "similar" indexes
                        ExeCliInterface *cliInterface);

  short createMetadataViews(ExeCliInterface *cliInterface);
  short dropMetadataViews(ExeCliInterface *cliInterface);

  int addSchemaObject(ExeCliInterface &cliInterface, const ComSchemaName &schemaName, ComSchemaClass schemaClass,
                      Int32 ownerID, NABoolean ignoreIfExists, NAString namespace1, NABoolean rowIdEncrypt,
                      NABoolean dataEncrypt, NABoolean storedDesc, NABoolean incrBackupEnabled);

  short createDefaultSystemSchema(ExeCliInterface *cliInterface);
  short createSchemaObjects(ExeCliInterface *cliInterface);
  short createLibrariesObject(ExeCliInterface *cliInterface);
  short extractLibrary(ExeCliInterface *cliInterface, char *libHandle, char *cachedLibName);
  void createSeabaseSchema(StmtDDLCreateSchema *createSchemaNode, NAString &currCatName);

  void cleanupObjectAfterError(ExeCliInterface &cliInterface, const NAString &catName, const NAString &schName,
                               const NAString &objName, const ComObjectType objectType, const NAString *nameSpace,
                               NABoolean ddlXns);

  NABoolean appendErrorObjName(char *errorObjs, const char *objName);

  // if this object is locked due to BR and ddl operations are disallowed
  // during BR, return an error.
  short isLockedForBR(Int64 objUID, Int64 schUID, ExeCliInterface *cliInterface, const CorrName &corrName);

  short setupAndErrorChecks(NAString &tabName, QualifiedName &origTableName, NAString &currCatName,
                            NAString &currSchName, NAString &catalogNamePart, NAString &schemaNamePart,
                            NAString &objectNamePart, NAString &extTableName, NAString &extNameForHbase, CorrName &cn,
                            NATable **naTable, NABoolean volTabSupported, NABoolean hbaseMapSupported,
                            ExeCliInterface *cliInterface, const ComObjectType objectType = COM_BASE_TABLE_OBJECT,
                            SQLOperation operation = SQLOperation::ALTER_TABLE, NABoolean isExternal = FALSE,
                            NABoolean processHiatus = FALSE, NABoolean createHiatusIfNotExist = FALSE);

  Lng32 purgedataObjectAfterError(ExeCliInterface &cliInterface, const NAString &catName, const NAString &schName,
                                  const NAString &objName, const ComObjectType objectType, NABoolean dontForceCleanup);

  short createSeabaseTable2(ExeCliInterface &cliInterface, StmtDDLCreateTable *createTableNode, NAString &currCatName,
                            NAString &currSchName, NABoolean isCompound, Int64 &objUID, NABoolean &schemaStoredDesc,
                            NABoolean &schemaIncBack);

  void createSeabaseTable(StmtDDLCreateTable *createTableNode, NAString &currCatName, NAString &currSchName,
                          NABoolean isCompound = FALSE, Int64 *retObjUID = NULL, NABoolean *genStoredDesc = NULL,
                          NABoolean *schemaIncBack = NULL);

  Lng32 createSeabasePartitionTable(ExeCliInterface *cliInterface, const NAString &partName,
                                    const NAString &currCatName, const NAString &currSchName,
                                    const NAString &baseTableName);

  Lng32 createSeabasePartitionTableWithInfo(ExeCliInterface *cliInterface, const NAString &partName,
                                            const NAString &currCatName, const NAString &currSchName,
                                            const NAString &baseTableName, Int64 objUID, Int32 partPositoin,
                                            NAString &PKey, NAString &pEntityNmaes, NAString &PKcolName, NAString &PK,
                                            int i, char *partitionTableName);

  void createSeabaseTableCompound(StmtDDLCreateTable *createTableNode, NAString &currCatName, NAString &currSchName);

  short createSeabaseTableLike2(CorrName &cn, const NAString &likeTabName, NABoolean withPartns = FALSE,
                                NABoolean withoutSalt = FALSE, NABoolean withoutDivision = FALSE,
                                NABoolean withoutRowFormat = FALSE);

  void createSeabaseTableLike(ExeCliInterface *cliInterface, StmtDDLCreateTable *createTableNode, NAString &currCatName,
                              NAString &currSchName);

  short createSeabaseTableExternal(ExeCliInterface &cliInterface, StmtDDLCreateTable *createTableNode,
                                   const ComObjectName &tgtTableName, const ComObjectName &srcTableName);

  void createSeabaseTriggers(ExeCliInterface *cliInterface, StmtDDLCreateTrigger *CreateTrigger, NAString &currCatName,
                             NAString &currSchName);

  short dropTriggerFromMDAndSPSQL(ExeCliInterface *cliInterface, ExpHbaseInterface *ehi, NAString &currCatName,
                                  NAString &currSchName, const NAString &triggerName, NABoolean ddlXns, Int64 objUID);

  void dropSeabaseTriggers(ExeCliInterface *cliInterface, StmtDDLDropTrigger *DropTrigger, NAString &currCatName,
                           NAString &currSchName);

  short checkAndFixupAuthIDs(ExeCliInterface *cliInterface, NABoolean checkOnly, NASet<NAString> &authIDList);

  short setLockError(short retcode);

  short lockRequired(ExpHbaseInterface *ehi, NATable *naTable, const NAString catalogNamePart,
                     const NAString schemaNamePart, const NAString objectNamePart, HBaseLockMode lockMode,
                     NABoolean useHbaseXn, const NABoolean replSync, const NABoolean incrementalBackup,
                     NABoolean asyncOperation, NABoolean noConflictCheck, NABoolean registerRegion);

  short lockRequired(const NATable *naTable, const NAString catalogNamePart, const NAString schemaNamePart,
                     const NAString objectNamePart, HBaseLockMode lockMode, NABoolean useHbaseXn,
                     const NABoolean replSync, const NABoolean incrementalBackup, NABoolean asyncOperation,
                     NABoolean noConflictCheck, NABoolean registerRegion);

  short lockRequired(ExpHbaseInterface *ehi, const NAString extNameForHbase, HBaseLockMode lockMode,
                     NABoolean useHbaseXn, const NABoolean replSync, const NABoolean incrementalBackup,
                     NABoolean asyncOperation, NABoolean noConflictCheck, NABoolean registerRegion);

  Int64 fetchObjectInfo(ExeCliInterface *cliInterface, const char *catName, const char *schName, const char *objName,
                        const ComObjectType objectType, Int32 &objectOwner, Int32 &schemaOwner, Int64 &objectFlags,
                        Int64 &objDataUID, bool reportErrorNow = true, NABoolean checkForValidDef = FALSE,
                        Int64 *createTime = NULL, Int64 *redefTime = NULL);

 public:
 protected:
  // makes a copy of underlying hbase table
  short cloneHbaseTable(const NAString &srcTable, const NAString &clonedTable, ExpHbaseInterface *inEHI);

  // add for binlog support
  short dropIBAttrForTable(const NATable *naTable, ExeCliInterface *cliInterface);
  NABoolean isTableHasIBAttr(const NATable *naTable, ExpHbaseInterface *ehi);

  short addIBAttrForTable(const NATable *naTable, ExeCliInterface *cliInterface);

  // makes a copy of traf metadata and underlying hbase table
  short cloneSeabaseTable(const NAString &srcTableNameStr, Int64 srcObjUID, const NAString &clonedTableNameStr,
                          const NATable *naTable, ExpHbaseInterface *ehi, ExeCliInterface *cilInterface,
                          NABoolean withCreate);

  short cloneAndTruncateTable(const NATable *naTable,  // IN: source table
                              NAString &tempTable,     // OUT: temp table
                              ExpHbaseInterface *ehi, ExeCliInterface *cliInterface);

  short dropSeabaseTable2(ExeCliInterface *cliInterface, StmtDDLDropTable *dropTableNode, NAString &currCatName,
                          NAString &currSchName);

  void dropSeabaseTable(StmtDDLDropTable *dropTableNode, NAString &currCatName, NAString &currSchName);
  Lng32 dropSeabasePartitionTable(ExeCliInterface *cliInterface, NAString &currCatName, NAString &currSchName,
                                  const NAString &btTableName, NABoolean needCascade = FALSE);

  Lng32 dropSeabasePartitionTable(ExeCliInterface *cliInterface, StmtDDLAlterTableDropPartition *alterDropPartition,
                                  NAPartition *partInfo, NAString &currCatName, NAString &currSchName);

  Lng32 deleteSeabasePartitionMDData(ExeCliInterface *cliInterface, const Int64 tableUID);
  Lng32 updateSeabasePartitionbtMDDataPosition(ExeCliInterface *cliInterface, const Int64 partitionUID);
  Lng32 deleteSeabasePartitionbtMDData(ExeCliInterface *cliInterface, const Int64 partitionUID);

  void createSeabaseIndex(StmtDDLCreateIndex *createIndexNode, NAString &currCatName, NAString &currSchName);

  Lng32 createSeabasePartitionIndex(ExeCliInterface *cliInterface, StmtDDLCreateIndex *createIndexNode,
                                    ElemDDLColRefArray &indexColRefArray, const char *partitionTableName,
                                    const NAString &currCatName, const NAString &currSchName,
                                    const NAString &baseIndexName);

  void populateSeabaseIndex(StmtDDLPopulateIndex *populateIndexNode, NAString &currCatName, NAString &currSchName);

  void dropSeabaseIndex(StmtDDLDropIndex *dropIndexNode, NAString &currCatName, NAString &currSchName);

  Lng32 dropSeabasePartitionIndex(ExeCliInterface *cliInterface, NAString &currCatName, NAString &currSchName,
                                  const NAString &baseIndexName, const char *partitionTableName);

  int populateSeabasePartIndex(NABoolean ispopulateForAll, NABoolean isPurgedataSpecified, NABoolean isUnique,
                               const char *tableName, const char *indexName, ExeCliInterface *cliInterface);

  void renameSeabaseTable(StmtDDLAlterTableRename *renameTableNode, NAString &currCatName, NAString &currSchName);

  void alterSeabaseTableStoredDesc(StmtDDLAlterTableStoredDesc *alterStoredDesc, NAString &currCatName,
                                   NAString &currSchName);

  void alterSeabaseTableHBaseOptions(StmtDDLAlterTableHBaseOptions *hbaseOptionsNode, NAString &currCatName,
                                     NAString &currSchName);

  void alterSeabaseIndexHBaseOptions(StmtDDLAlterIndexHBaseOptions *hbaseOptionsNode, NAString &currCatName,
                                     NAString &currSchName);

  void alterSeabaseTableAttribute(StmtDDLAlterTableAttribute *alterTabAttr, NAString &currCatName,
                                  NAString &currSchName);
  Int32 alterSeabaseIndexAttributes(ExeCliInterface *cliInterface, ParDDLFileAttrsAlterTable &fileAttrs,
                                    NATable *naTable, int alterReadonlyOP = 0);

  Int32 getNextUnnamedConstrNum(NATable *naTable);

  void addConstraints(ComObjectName &tableName, ComAnsiNamePart &currCatAnsiName, ComAnsiNamePart &currSchAnsiName,
                      StmtDDLNode *ddlNode, StmtDDLAddConstraintPK *pkConstr,
                      StmtDDLAddConstraintUniqueArray &uniqueConstrArr, StmtDDLAddConstraintRIArray &riConstrArr,
                      StmtDDLAddConstraintCheckArray &checkConstrArr, Int32 unnamedConstrNum,
                      NABoolean isCompound = FALSE, NABoolean isPartitionV2 = FALSE);

  void alterTablePartition(StmtDDLAlterTablePartition *alterPartition, NAString &currCatName, NAString &currSchName);

  void alterTableAddPartition(StmtDDLAlterTableAddPartition *alterAddPartition, NAString &currCatName,
                              NAString &currSchName);

  Lng32 unmountSeabasePartitionTable(ExeCliInterface *cliInterface, NAPartition *partInfo, NAString &currCatName,
                                     NAString &currSchName, NAString &nameSpace, ExpHbaseInterface *ehi,
                                     NABoolean ddlXns);

  void alterTableMountPartition(StmtDDLAlterTableMountPartition *alterMountPartition, NAString &currCatName,
                                NAString &currSchName);

  void alterTableUnmountPartition(StmtDDLAlterTableUnmountPartition *alterUnmountPartition, NAString &currCatName,
                                  NAString &currSchName);

  void alterTableMergePartition(StmtDDLAlterTableMergePartition *mergePartition, NAString &currCatName,
                                NAString &currSchName);

  void alterTableExchangePartition(StmtDDLAlterTableExchangePartition *exchangePartition, NAString &currCatName,
                                   NAString &currSchName);

  void alterSeabaseTableAddColumn(StmtDDLAlterTableAddColumn *alterAddColNode, NAString &currCatName,
                                  NAString &currSchName);

  short updateMDforDropCol(ExeCliInterface &cliInterface, const NATable *naTable, Lng32 dropColNum, const NAType *type);

  short alignedFormatTableDropColumn(const NAString &catalogNamePart, const NAString &schemaNamePart,
                                     const NAString &objectNamePart, const NATable *naTable, const NAString &altColName,
                                     ElemDDLColDef *pColDef, NABoolean ddlXns, NAList<NAString> &viewNameList,
                                     NAList<NAString> &viewDefnList);

  short hbaseFormatTableDropColumn(ExpHbaseInterface *ehi, const NAString &catalogNamePart,
                                   const NAString &schemaNamePart, const NAString &objectNamePart,
                                   const NATable *naTable, const NAString &altColName, const NAColumn *nacol,
                                   NABoolean ddlXns, NAList<NAString> &viewNameList, NAList<NAString> &viewDefnList);

  void alterSeabaseTableDropColumn(StmtDDLAlterTableDropColumn *alterDropColNode, NAString &currCatName,
                                   NAString &currSchName);

  void alterSeabaseTableDropPartition(StmtDDLAlterTableDropPartition *alterDropPartition, NAString &currCatName,
                                      NAString &currSchName);

  short saveAndDropUsingViews(Int64 objUID, ExeCliInterface *cliInterface, NAList<NAString> &viewNameList,
                              NAList<NAString> &viewDefnList);

  short recreateUsingViews(ExeCliInterface *cliInterface, NAList<NAString> &viewNameList,
                           NAList<NAString> &viewDefnList, NABoolean ddlXns);

  void alterSeabaseTableAlterIdentityColumn(StmtDDLAlterTableAlterColumnSetSGOption *alterIdentityColNode,
                                            NAString &currCatName, NAString &currSchName);

  short mdOnlyAlterColumnAttr(const NAString &catalogNamePart, const NAString &schemaNamePart,
                              const NAString &objectNamePart, const NATable *naTable, const NAColumn *naCol,
                              NAType *newType, StmtDDLAlterTableAlterColumnDatatype *alterColNode,
                              NAList<NAString> &viewNameList, NAList<NAString> &viewDefnList,
                              NABoolean isAlterForSample = FALSE, Int64 sampleTableUid = 0);

  short hbaseFormatTableAlterColumnAttr(const NAString &catalogNamePart, const NAString &schemaNamePart,
                                        const NAString &objectNamePart, const NATable *naTable, const NAColumn *naCol,
                                        NAType *newType, StmtDDLAlterTableAlterColumnDatatype *alterColNode);

  short alignedFormatTableAlterColumnAttr(const NAString &catalogNamePart, const NAString &schemaNamePart,
                                          const NAString &objectNamePart, const NATable *naTable,
                                          const NAString &altColName, ElemDDLColDef *pColDef, NABoolean ddlXns,
                                          NAList<NAString> &viewNameList, NAList<NAString> &viewDefnList);

  void alterSeabaseTableAlterColumnDatatype(StmtDDLAlterTableAlterColumnDatatype *alterColumnDatatype,
                                            NAString &currCatName, NAString &currSchName,
                                            NABoolean isMaintenanceWindowOFF);

  Lng32 updatePKeysTable(ExeCliInterface *cliInterface, const char *renamedColName, Int64 objectUid,
                         const char *colName, NABoolean isIndexColumn = FALSE);

  void alterSeabaseTableAlterColumnRename(StmtDDLAlterTableAlterColumnRename *alterColumnDatatype,
                                          NAString &currCatName, NAString &currSchName);

  void alterSeabaseTableAddPKeyConstraint(StmtDDLAddConstraint *alterAddConstraint, NAString &currCatName,
                                          NAString &currSchName);

  void alterSeabaseTableAddUniqueConstraint(StmtDDLAddConstraint *alterAddConstraint, NAString &currCatName,
                                            NAString &currSchName);

  short isCircularDependent(CorrName &ringTable, CorrName &refdTable, CorrName &origRingTable, BindWA *bindWA);

  void alterSeabaseTableAddRIConstraint(StmtDDLAddConstraint *alterAddConstraint, NAString &currCatName,
                                        NAString &currSchName);

  short getCheckConstraintText(StmtDDLAddConstraintCheck *addCheckNode, NAString &checkConstrText,
                               NABoolean useNoneExpandedName = false);

  short getTextFromMD(ExeCliInterface *cliInterface, Int64 constrUID, ComTextType textType, Lng32 textSubID,
                      NAString &constrText, NABoolean binaryData = FALSE);

  void alterSeabaseTableAddCheckConstraint(StmtDDLAddConstraint *alterAddConstraint, NAString &currCatName,
                                           NAString &currSchName);

  void alterSeabaseTableDropConstraint(StmtDDLDropConstraint *alterDropConstraint, NAString &currCatName,
                                       NAString &currSchName);

  void alterSeabaseTableDisableOrEnableIndex(ExprNode *ddlNode, NAString &currCatName, NAString &currSchName);

  void alterSeabaseTableDisableOrEnableAllIndexes(ExprNode *ddlNode, NAString &currCatName, NAString &currSchName,
                                                  NAString &tableName, NABoolean allUniquesOnly);
  short alterSeabaseTableDisableOrEnableIndex(const char *catName, const char *schName, const char *idxName,
                                              const char *tabName, NABoolean isDisable);

  short alterSeabaseIndexStoredDesc(ExeCliInterface &cliInterface, const NAString &btCatName, const NAString &btSchName,
                                    const NAString &btObjName, const NABoolean &enable);

  void alterSeabaseTableResetDDLLock(ExeCliInterface *cliInterface, StmtDDLAlterTableResetDDLLock *alterResetLock,
                                     NAString &currCatName, NAString &currSchName);

  void createSeabaseView(StmtDDLCreateView *createViewNode, NAString &currCatName, NAString &currSchName);

  void dropSeabaseView(StmtDDLDropView *dropViewNode, NAString &currCatName, NAString &currSchName);

  void createSeabaseLibrary(StmtDDLCreateLibrary *createLibraryNode, NAString &currCatName, NAString &currSchName);

  void createSeabaseLibrary2(StmtDDLCreateLibrary *createLibraryNode, NAString &currCatName, NAString &currSchName);
  void registerSeabaseUser(StmtDDLRegisterUser *registerUserNode);

  void alterSeabaseUser(StmtDDLAlterUser *alterUserNode);
  void registerSeabaseComponent(StmtDDLRegisterComponent *registerComponentNode);

  void dropSeabaseLibrary(StmtDDLDropLibrary *dropLibraryNode, NAString &currCatName, NAString &currSchName);

  void alterSeabaseLibrary(StmtDDLAlterLibrary *alterLibraryNode, NAString &currCatName, NAString &currSchName);

  void createSeabasePackage(ExeCliInterface *cliInterface, StmtDDLCreatePackage *createPackageNode,
                            NAString &currCatName, NAString &currSchName);

  short dropPackageFromMDAndSPSQL(ExeCliInterface *cliInterface, NAString &currCatName, NAString &currSchName,
                                  const NAString &routineName, NABoolean ddlXns);

  void dropSeabasePackage(ExeCliInterface *cliInterface, StmtDDLDropPackage *dropPackageNode, NAString &currCatName,
                          NAString &currSchName);

  void alterSeabaseLibrary2(StmtDDLAlterLibrary *alterLibraryNode, NAString &currCatName, NAString &currSchName);
  short isLibBlobStoreValid(ExeCliInterface *cliInterface);

  void createSeabaseRoutine(StmtDDLCreateRoutine *createRoutineNode, NAString &currCatName, NAString &currSchName);

  short createSPSQLRoutine(ExeCliInterface *cliInterface, NAString *quotedSql);

  void dropSeabaseRoutine(StmtDDLDropRoutine *dropRoutineNode, NAString &currCatName, NAString &currSchName);

  short dropRoutineFromMDAndSPSQL(ExeCliInterface *cliInterface, ExpHbaseInterface *ehi, NAString &currCatName,
                                  NAString &currSchName, const NAString &routineName, ComRoutineType routineType,
                                  NABoolean ddlXns, Int64 &objUID);
  short createSPSQLProcs(ExeCliInterface *cliInterface);

  short createSeabaseLibmgr(ExeCliInterface *cliInterface);
  short upgradeSeabaseLibmgr(ExeCliInterface *inCliInterface);
  short upgradeSeabaseLibmgr2(ExeCliInterface *inCliInterface);
  short dropSeabaseLibmgr(ExeCliInterface *inCliInterface);
  short createLibmgrProcs(ExeCliInterface *cliInterface);
  short grantLibmgrPrivs(ExeCliInterface *cliInterface);
  short createSeabaseLibmgrCPPLib(ExeCliInterface *cliInterface);

  // Tenant commands are implemented in CmpSeabaseTenant
  short createSeabaseTenant(ExeCliInterface *cliInterface, NABoolean addCompPrivs, NABoolean reportNotEnabled);
  short dropSeabaseTenant(ExeCliInterface *cliInterface);
  short getTenantNSList(ExeCliInterface *cliInterface, NAList<NAString *> *&tenantNSList);
  short getTenantNodeList(ExeCliInterface *cliInterface, NAList<NAString *> *&tenantNodeList);
  short getTenantSchemaList(ExeCliInterface *cliInterface, TenantSchemaInfoList *&tenantSchemaList);
  short isTenantMetadataInitialized(ExeCliInterface *cliInterface);
  short isTenantSchema(ExeCliInterface *cliInterface, const Int64 &schemaUID);
  void registerSeabaseTenant(ExeCliInterface *cliInterface, StmtDDLTenant *pNode);
  void unregisterSeabaseTenant(ExeCliInterface *cliInterface, StmtDDLTenant *pNode);
  short upgradeSeabaseTenant(ExeCliInterface *cliInterface);
  // End tenant commands

  // Resource group commands are implemented in CmpSeabaseTenant
  void createSeabaseRGroup(ExeCliInterface *cliInterface, StmtDDLResourceGroup *pNode);
  void alterSeabaseRGroup(ExeCliInterface *cliInterface, StmtDDLResourceGroup *pNode);
  void dropSeabaseRGroup(ExeCliInterface *cliInterface, const StmtDDLResourceGroup *pNode);
  // End rgroup commands

  short registerNativeTable(const NAString &catalogNamePart, const NAString &schemaNamePart,
                            const NAString &objectNamePart, Int32 objOwnerId, Int32 schemaOwnerId,
                            ExeCliInterface &cliInterface, NABoolean isRegister, NABoolean isInternal);

  short unregisterNativeTable(const NAString &catalogNamePart, const NAString &schemaNamePart,
                              const NAString &objectNamePart, ExeCliInterface &cliInterface,
                              ComObjectType objType = COM_BASE_TABLE_OBJECT);

  short unregisterHiveSchema(const NAString &catalogNamePart, const NAString &schemaNamePart,
                             ExeCliInterface &cliInterface, NABoolean cascade);

  short adjustHiveExternalSchemas(ExeCliInterface *cliInterface);

  void createSeabaseSequence(StmtDDLCreateSequence *createSequenceNode, NAString &currCatName, NAString &currSchName);

  void alterSeabaseSequence(StmtDDLCreateSequence *alterSequenceNode, NAString &currCatName, NAString &currSchName);

  void dropSeabaseSequence(StmtDDLDropSequence *dropSequenceNode, NAString &currCatName, NAString &currSchName);

  void seabaseGrantRevoke(StmtDDLNode *stmtDDLNode, NABoolean isGrant, NAString &currCatName, NAString &currSchName,
                          NABoolean internalCall = FALSE);

  void hbaseGrantRevoke(StmtDDLNode *stmtDDLNode, NABoolean isGrant, NAString &currCatName, NAString &currSchName);

  void grantRevokeSchema(StmtDDLNode *stmtDDLNode, NABoolean isGrant, NAString &currCatName, NAString &currSchName);

  void dropSeabaseSchema(StmtDDLDropSchema *dropSchemaNode);
  bool dropOneTable(ExeCliInterface &cliInterface, const char *catalogName, const char *schemaName,
                    const char *objectName, bool isVolatile, bool ifExists, bool ddlXns);

  void alterSeabaseSchema(StmtDDLAlterSchema *alterSchemaNode);

  NABoolean isSchemaObjectName(const NAString objName) {
    ComObjectName objNameParts(objName, COM_TABLE_NAME);
    const NAString objectNamePart = objNameParts.getObjectNamePartAsAnsiString(TRUE);
    return (objectNamePart == SEABASE_SCHEMA_OBJECTNAME);
  }

  Lng32 alterSchemaTableDesc(ExeCliInterface *cliInterface, const StmtDDLAlterTableStoredDesc::AlterStoredDescType sdo,
                             const Int64 schUID, const ComObjectType objType, const NAString catName,
                             const NAString schName, const NABoolean ddlXns);

  Lng32 alterSchemaTableDesc(ExeCliInterface *cliInterface, const StmtDDLAlterTableStoredDesc::AlterStoredDescType sdo,
                             const NAString objectName, const NABoolean ddlXns);

  bool dropOneTableorView(ExeCliInterface &cliInterface, const char *objectName, ComObjectType objectType,
                          bool isVolatile);

  void createNativeHbaseTable(ExeCliInterface *cliInterface, StmtDDLCreateHbaseTable *createTableNode,
                              NAString &currCatName, NAString &currSchName);

  void dropNativeHbaseTable(ExeCliInterface *cliInterface, StmtDDLDropHbaseTable *dropTableNode, NAString &currCatName,
                            NAString &currSchName);
  void alterSeabaseTableHDFSCache(StmtDDLAlterTableHDFSCache *alterTableHdfsCache, NAString &currCatName,
                                  NAString &currSchName);

  void alterSeabaseSchemaHDFSCache(StmtDDLAlterSchemaHDFSCache *alterSchemaHdfsCache);

  short processNamespaceOperations(StmtDDLNamespace *ns);

  void dropSeabaseMD(NABoolean ddlXns, NABoolean forceOption);
  void createSeabaseMDviews();
  void dropSeabaseMDviews();
  void createSeabaseSchemaObjects();
  void updateVersion();

  short initTrafMD(CmpDDLwithStatusInfo *mdti);

  short createPrivMgrRepos(ExeCliInterface *cliInterface, NABoolean ddlXns);
  short initSeabaseAuthorization(ExeCliInterface *cliInterface, NABoolean ddlXns, NABoolean isUpgrade,
                                 std::vector<std::string> &tablesCreated);

  void dropSeabaseAuthorization(ExeCliInterface *cliInterface, NABoolean doCleanup = FALSE);

  void doSeabaseCommentOn(StmtDDLCommentOn *commentOnNode, NAString &currCatName, NAString &currSchName);

  NABoolean insertPrivMgrInfo(const Int64 objUID, const NAString &objName, const ComObjectType objectType,
                              const Int32 objOwnerID, const Int32 schemaOwnerID, const Int32 creatorID);

  NABoolean deletePrivMgrInfo(const NAString &objName, const Int64 objUID, const ComObjectType objType);

  short dropSeabaseObjectsFromHbase(const char *pattern, NABoolean ddlXns);
  short updateSeabaseAuths(ExeCliInterface *cliInterface, const char *sysCat);

  short recreateHbaseTable(const NAString &catalogNamePart, const NAString &schemaNamePart,
                           const NAString &objectNamePart, NATable *naTable, ExpHbaseInterface *ehi);

  short truncateHbaseTable(const NAString &catalogNamePart, const NAString &schemaNamePart,
                           const NAString &objectNamePart, const NAString &nameSpace,
                           const NABoolean hasSaltOrReplicaCol, const NABoolean dtmTruncate,
                           const NABoolean incrBackupEnabled, Int64 objDataUID, ExpHbaseInterface *ehi);

  void purgedataHbaseTable(DDLExpr *ddlExpr, NAString &currCatName, NAString &currSchName);

  void alterSeabaseTableTruncatePartition(StmtDDLAlterTableTruncatePartition *alterTableTrunPartitionNode,
                                          NAString &currCatName, NAString &currSchName);
  void alterSeabaseTableRenamePartition(StmtDDLAlterTableRenamePartition *alterTableRenamePartNode,
                                        NAString &currCatName, NAString &currSchName);

  void alterSeabaseTableSplitPartition(StmtDDLAlterTableSplitPartition *alterTableSplitPartNode, NAString &currCatName,
                                       NAString &currSchName);

  short updateTablePartitionsPartitionName(ExeCliInterface *cliInterface, NAString &currCatName, NAString &currSchName,
                                           NAString &oldPtNm, NAString &newPtNm, Int64 parentUid);
  NABoolean isExistsPartitionName(NATable *naTable, NAString &partitionName);
  NABoolean isExistsPartitionName(NATable *naTable, NAList<NAString> &partitionNames);
  NABoolean isExistsPartitionName(ExeCliInterface *cliInterface, const NAString &currCatName,
                                  const NAString &currSchName, const NAString &currObjName,
                                  const NAString &partitionName, NAString &partEntityName);

  short createRepos(ExeCliInterface *cliInterface);
  short dropRepos(ExeCliInterface *cliInterface, NABoolean oldRepos = FALSE, NABoolean dropSchema = TRUE,
                  NABoolean inRecovery = FALSE);
  short alterRenameRepos(ExeCliInterface *cliInterface, NABoolean newToOld);
  short copyOldReposToNew(ExeCliInterface *cliInterface);

  short dropAndLogReposViews(ExeCliInterface *cliInterface, NABoolean &someViewSaved /* out */);
  short createLibraries(ExeCliInterface *cliInterface);
  short dropLibraries(ExeCliInterface *cliInterface, NABoolean oldLibraries = FALSE, NABoolean inRecovery = FALSE);
  short alterRenameLibraries(ExeCliInterface *cliInterface, NABoolean newToOld);
  short copyOldLibrariesToNew(ExeCliInterface *cliInterface);
  short checkForOldLibraries(ExeCliInterface *cliInterface);
  void processXDCMeta(NABoolean create, NABoolean drop, NABoolean upgrade);
  short dropXDCMeta(ExeCliInterface *cliInterface, NABoolean dropSchema = true);
  short createXDCMeta(ExeCliInterface *cliInterface);
  void processMDPartTables(NABoolean create, NABoolean drop, NABoolean upgrade);
  short dropMDPartTables(ExeCliInterface *cliInterface);
  short createMDPartTables(ExeCliInterface *cliInterface);

 public:
  static short isValidHbaseName(const char *str);

  short upgradeRepos(ExeCliInterface *cliInterface, CmpDDLwithStatusInfo *mdui);
  short upgradeReposComplete(ExeCliInterface *cliInterface, CmpDDLwithStatusInfo *mdui);
  short upgradeReposUndo(ExeCliInterface *cliInterface, CmpDDLwithStatusInfo *mdui);

  short upgradeNamespace(ExeCliInterface *cliInterface);
  short createOrDropLobMD(ExeCliInterface *cliInterface, NABoolean isCreate);

  static NAString genHBaseObjName(const NAString &catName, const NAString &schName, const NAString &objName,
                                  const NAString &nameSpace, Int64 dataUID = 0);

  short upgradeLibraries(ExeCliInterface *cliInterface, CmpDDLwithStatusInfo *mdui);
  short upgradeLibrariesComplete(ExeCliInterface *cliInterface, CmpDDLwithStatusInfo *mdui);
  short upgradeLibrariesUndo(ExeCliInterface *cliInterface, CmpDDLwithStatusInfo *mdui);

  // decode a stored descriptor
  static TrafDesc *decodeStoredDesc(NAString &gluedText);

 protected:
  short updateBackupOperationMetrics(RelBackupRestore *brExpr, ExeCliInterface *cliInterface, Int64 operStartTime,
                                     Int64 operEndTime);
  short updateBackupObjectsMetrics(RelBackupRestore *brExpr, ExeCliInterface *cliInterface);

  short createBackupRepos(ExeCliInterface *cliInterface);
  short dropBackupRepos(ExeCliInterface *cliInterface);

  void processRepository(NABoolean createR, NABoolean dropR, NABoolean upgradeR);

  void processBackupRepository(NABoolean createR, NABoolean dropR, NABoolean upgradeR);

  short updateSeabaseVersions(ExeCliInterface *cliInterface, const char *sysCat, Lng32 majorVersion = -1);

  short getSpecialTableInfo(NAMemory *heap, const NAString &catName, const NAString &schName, const NAString &objName,
                            const NAString &extTableName, const ComObjectType &objType,
                            ComTdbVirtTableTableInfo *&tableInfo);

  TrafDesc *getSeabaseMDTableDesc(const NAString &catName, const NAString &schName, const NAString &objName,
                                  const ComObjectType objType);

  TrafDesc *getSeabaseHistTableDesc(const NAString &catName, const NAString &schName, const NAString &objName);

  TrafDesc *getTrafBackupMDTableDesc(const NAString &catName, const NAString &schName, const NAString &objName);
  Lng32 getTableDefForBinlogOp(ElemDDLColDefArray *, ElemDDLColRefArray *, NAString &, NAString &, NABoolean pk);

  ComTdbVirtTableSequenceInfo *getSeabaseSequenceInfo(const NAString &catName, const NAString &schName,
                                                      const NAString &seqName, NAString &extSeqName, Int32 &objectOwner,
                                                      Int32 &schemaOwner, Int64 &seqUID);

  TrafDesc *getSeabaseSequenceDesc(const NAString &catName, const NAString &schName, const NAString &seqName);

  TrafDesc *getSeabaseSchemaDesc(const NAString &catName, const NAString &schName, const Int32 ctlFlags,
                                 Int32 &packedDescLen);

  ComTdbVirtTablePrivInfo *getSeabasePrivInfo(ExeCliInterface *cliInterface, const NAString &catName,
                                              const NAString &schName, const Int64 objUID, const ComObjectType objType,
                                              const Int32 schemaOwner);

  Lng32 getSeabaseColumnInfo(ExeCliInterface *cliInterface, Int64 objUID, const NAString &catName,
                             const NAString &schName, const NAString &objName, char *direction,
                             NABoolean *tableIsSalted, Int16 *numTrafReplicas, Lng32 *identityColPos,
                             Lng32 *partialRowSize, Lng32 *numCols, ComTdbVirtTableColumnInfo **colInfoArray);

  TrafDesc *getSeabaseUserTableDesc(const NAString &catName, const NAString &schName, const NAString &objName,
                                    const ComObjectType objType, NABoolean includeInvalidDefs,
                                    short includeBRLockedTables, Int32 ctlFlags, Int32 &packedDescLen, Int32 cntxtType);

  Lng32 createNATableFromStoredDesc(NATableDB *natDB, BindWA &bindWA, PreloadInfo &info, NABoolean doNotReloadIfExists);

  Lng32 createNATableForSystemObjects(BindWA &bindWA, NAString &catName, NAString &schName, NAString &objName,
                                      NABoolean isIndex, NABoolean doNotReloadIfExists);

  Lng32 generatePreloadInfoForUserTables(QualifiedName &inSchName, ExeCliInterface &cliInterface,
                                         NAList<PreloadInfo> &userTables, NAList<PreloadInfo> &statsTables);

  Lng32 loadTrafSystemMetadataInCache(CmpContext *metaContext, NABoolean doNotReloadIfExists);

  Lng32 loadTrafMDSystemTableIntoCache(NAString &tableName, CmpContext *context, NABoolean doNotReloadIfExists);

  Lng32 loadTrafUserMetadataInCache(CmpContext *cmpContext, NAList<PreloadInfo> &tables, NABoolean doNotReloadIfExists);

  Lng32 loadTrafMetadataInCache(QualifiedName &schName);

  void generateQueryToFetchUserTableDescriptors(QualifiedName &inSchNam, char *buffer, Lng32 bufLen);

  Lng32 loadTrafMetadataIntoSharedCache(QualifiedName &schName, NABoolean loadLocalIfEmpty,
                                        CmpDDLwithStatusInfo *dws = NULL);

  Lng32 loadTrafDataIntoSharedCache(QualifiedName &schName, NABoolean loadLocalIfEmpty,
                                    CmpDDLwithStatusInfo *dws = NULL);

  void generateQueryToFetchOneTableDescriptor(const QualifiedName &tableName, char *buffer, Lng32 bufLen);

  Lng32 updateTrafMetadataSharedCacheForTable(const QualifiedName &tatbleName, NABoolean insertOnly,
                                              NABoolean errorAsWarning);
  Lng32 checkTrafMetadataSharedCacheForTable(const QualifiedName &tableName, CmpDDLwithStatusInfo *dws);
  Lng32 checkTrafMetadataSharedCacheForSchema(const QualifiedName &schemaName, CmpDDLwithStatusInfo *dws);
  Lng32 checkTrafMetadataSharedCacheAll(CmpDDLwithStatusInfo *dws);

  Lng32 alterSharedDescCache(StmtDDLAlterSharedCache *ascNode, CmpDDLwithStatusInfo *dws = NULL);

  Lng32 updateTrafDataSharedCacheForTable(const vector<pair<QualifiedName, int>> &tables, NABoolean insert,
                                          NABoolean errorAsWarning);
  Lng32 checkTrafDataSharedCacheForTable(const QualifiedName &tableName, CmpDDLwithStatusInfo *dws);
  Lng32 checkTrafDataSharedCacheForSchema(const QualifiedName &schemaName, CmpDDLwithStatusInfo *dws);
  Lng32 checkTrafDataSharedCacheAll(CmpDDLwithStatusInfo *dws, bool showDetails);

  Lng32 alterSharedDataCache(StmtDDLAlterSharedCache *ascNode, CmpDDLwithStatusInfo *dws = NULL);

  static NABoolean getMDtableInfo(const ComObjectName &ansiName, ComTdbVirtTableTableInfo *&tableInfo,
                                  Lng32 &colInfoSize, const ComTdbVirtTableColumnInfo *&colInfo, Lng32 &keyInfoSize,
                                  const ComTdbVirtTableKeyInfo *&keyInfo, Lng32 &indexInfoSize,
                                  const ComTdbVirtTableIndexInfo *&indexInfo, const ComObjectType objType,
                                  NAString &nameSpace);

  void giveSeabaseAll(StmtDDLGiveAll *giveAllParseNode);

  void giveSeabaseObject(StmtDDLGiveObject *giveObjectNode);

  void giveSeabaseSchema(StmtDDLGiveSchema *giveSchemaNode, NAString &currentCatalogName);

  void glueQueryFragments(Lng32 queryArraySize, const QString *queryArray, char *&gluedQuery, Lng32 &gluedQuerySize);

  short convertColAndKeyInfoArrays(Lng32 btNumCols,                            // IN
                                   ComTdbVirtTableColumnInfo *btColInfoArray,  // IN
                                   Lng32 btNumKeys,                            // IN
                                   ComTdbVirtTableKeyInfo *btKeyInfoArray,     // IN
                                   NAColumnArray *naColArray, NAColumnArray *naKeyArr);

  short processDDLandCreateDescs(Parser &parser, const QString *ddl, Lng32 sizeOfddl,

                                 const NAString &defCatName, const NAString &defSchName, const NAString &nameSpace,

                                 NABoolean isIndexTable,

                                 Lng32 btNumCols,                            // IN
                                 ComTdbVirtTableColumnInfo *btColInfoArray,  // IN
                                 Lng32 btNumKeys,                            // IN
                                 ComTdbVirtTableKeyInfo *btKeyInfoArray,     // IN

                                 Lng32 &numCols, ComTdbVirtTableColumnInfo *&colInfoArray, Lng32 &numKeys,
                                 ComTdbVirtTableKeyInfo *&keyInfoArray, ComTdbVirtTableTableInfo *&tableInfo,
                                 ComTdbVirtTableIndexInfo *&indexInfo);

  short updateColAndKeyInfo(const NAColumn *keyCol, CollIndex i, NABoolean alignedFormat, NAString &defaultColFam,
                            short nonKeyColType, short ordering, const char *colNameSuffix,
                            ComTdbVirtTableColumnInfo *colInfoEntry, ComTdbVirtTableKeyInfo *keyInfoEntry,
                            Lng32 &keyLength, NAMemory *heap);

  short createIndexColAndKeyInfoArrays(ElemDDLColRefArray &indexColRefArray, ElemDDLColRefArray &addnlColRefArray,
                                       NABoolean isUnique,
                                       NABoolean isNgram,  // ngram index
                                       NABoolean hasSyskey, NABoolean alignedFormat, NAString &defaultColFam,
                                       const NAColumnArray &baseTableNAColArray, const NAColumnArray &baseTableKeyArr,
                                       Lng32 &keyColCount, Lng32 &nonKeyColCount, Lng32 &totalColCount,
                                       ComTdbVirtTableColumnInfo *&colInfoArray, ComTdbVirtTableKeyInfo *&keyInfoArray,
                                       NAList<NAString> &selColList, Lng32 &keyLength, NAMemory *heap,
                                       Int32 numSaltPartns = 0);

  // called by both Create Table and Create Index code
  // Given the optionsclause (from parser) and numSplits (parser/cqd/infered)
  // this method the produced hbaseOptions in a list that can be more
  // easily provided to the HBase create table API as well as in a string
  // that can be stored in Trafodion metadata. Returns 0 on success and a
  // negative value to indicate failure.
  short setupHbaseOptions(ElemDDLHbaseOptions *hbaseOptionsClause,          // in
                          Int32 numSplits,                                  // in
                          const NAString &objName,                          // in for err handling
                          NAList<HbaseCreateOption *> &hbaseCreateOptions,  // out
                          NAString &hco);                                   // out

  void populateBlackBox(ExeCliInterface *cliInterface, Queue *returnDetailsList, Int32 &blackBoxLen, char *&blackBox,
                        NABoolean queueHasOutputInfo = TRUE);

  //////////////////////////////////////
  // Start methods for Backup/Restore
  //////////////////////////////////////
  enum BRSteps {
    BR_START,
    ORPHAN_OBJECTS_ENTRIES,
    HBASE_ENTRIES,
    INCONSISTENT_OBJECTS_ENTRIES,
    VIEWS_ENTRIES,
    PRIV_ENTRIES,
    DONE_CLEANUP
  };

  ExpHbaseInterface *allocBRCEHI();

  Lng32 getSeabasePartitionV2Info(ExeCliInterface *cliInterface, Int64 btUid,
                                  ComTdbVirtTablePartitionV2Info **outPartionV2InfoArray);

  Lng32 getSeabasePartitionBoundaryInfo(ExeCliInterface *cliInterface, Int64 btUid,
                                        ComTdbVirtTablePartitionV2Info **outPartionV2InfoArray);

  NABoolean compareDDLs(ExeCliInterface *cliInterface, NATable *naTabA, NATable *naTabB,
                        NABoolean ignoreClusteringKey = FALSE, NABoolean ignoreSaltUsing = FALSE,
                        NABoolean ignoreSaltNum = FALSE,
                        NABoolean ignoreV2Partition = TRUE,  // new range partition
                        NABoolean ignoreIndex = TRUE);

 public:
  static NABoolean isValidBackupTagName(const char *str);

  NAHeap *getHeap() { return heap_; }

  // Disable and clear the small system object cache
  // Used prior to MD/language manager upgrade to prevent
  // the small system object cache to hold stale entries,
  // mostly due to the OBJECT_UID change during the upgrade.
  void disableAndClearSystemObjectCache();

  // Enable the small system object cache. Used after
  // MD/language manager upgrade.
  void enableSystemObjectCache();

 protected:
  short backupInit(const char *oper, NABoolean lock);

  short expandAndValidate(ExeCliInterface *cliInterface, ElemDDLQualName *dn, const char *objectType,
                          NABoolean validateObjects, NAString &currCatName, NAString &currSchName);

  short validateInputNames(ExeCliInterface *cliInterface, RelBackupRestore *brExpr, NABoolean validateObjects,
                           NAString &currCatName, NAString &currSchName);

  short genObjectsToBackup(RelBackupRestore *brExpr, NAString &objUIDsWherePred, ExeCliInterface *cliInterface,
                           CmpDDLwithStatusInfo *dws);
  short genObjectsToRestore(RelBackupRestore *brExpr, NAArray<HbaseStr> *mdList, NAString &objUIDsWherePred,
                            ExeCliInterface *cliInterface, CmpDDLwithStatusInfo *dws);

  short processBRShowObjects(RelBackupRestore *brExpr, NAArray<HbaseStr> *mdList, NAString &objUIDsWherePred,
                             ExeCliInterface *cliInterface, CmpDDLwithStatusInfo *dws);

  short createBackupMDschema(RelBackupRestore *ddlExpr, ExeCliInterface *cliInterface, NAString &backupSch,
                             const char *oper);

  short truncateBackupMD(NAString &backupSch, ExeCliInterface *cliInterface);

  short invalidateTgtObjects(ExeCliInterface *cliInterface, char *objUIDsWherePred, char *tgtSch, char *srcSch,
                             SharedCacheDDLInfoList &sharedCacheList);

  short deleteTgtObjectsFromMD(RelBackupRestore *brExpr, ExeCliInterface *cliInterface, NAString &objUIDsWherePred,
                               char *tgtSch, char *tgtSch2, char *srcSch);

  short cleanupTgtObjectsFromMD(ExeCliInterface *cliInterface, char *objUIDsWherePred, char *srcSch);

  short copyBRSrcToTgtBasic(RelBackupRestore *brExpr, ExeCliInterface *cliInterface, const char *objUIDsWherePred,
                            const char *srcSch1, const char *srcSch2, const char *tgtSch1, const char *tgtSch2,
                            NAString *schPrivsList = NULL);

  short copyBRSrcToTgt(RelBackupRestore *brExpr, ExeCliInterface *cliInterface, const char *objUIDsWherePred,
                       const char *srcSch1, const char *srcSch2, const char *tgtSch1, const char *tgtSch2,
                       NAString *schPrivsList = NULL);

  // backup the whole system
  short backupSystem(RelBackupRestore *ddlExpr, ExeCliInterface *cliInterface, CmpDDLwithStatusInfo *dws);

  short backupHistStats(ExeCliInterface *cliInterface, ElemDDLList *tableList, char *backupSch);

  short restoreHistStats(ExeCliInterface *cliInterface, char *backupSch);

  short createProgressTable(ExeCliInterface *cliInterface);

  short deleteFromProgressTable(ExeCliInterface *cliInterface, RelBackupRestore *brExpr, const char *oper);

  short insertProgressTable(ExeCliInterface *cliInterface, RelBackupRestore *brExpr, const char *oper,
                            const char *stepText, const char *state, Int32 param1, Int32 param2);

  Int32 getBREIOperCount(ExeCliInterface *cliInterface, const char *oper, const char *tag);

  short dropProgressTable(ExeCliInterface *cliInterface);
  short truncateProgressTable(ExeCliInterface *cliInterface);

  short updateProgressTable(ExeCliInterface *cliInterface, const char *backupTag, const char *oper,
                            const char *stepText, const char *state, Int32 param2, Int32 perc);

  short getProgressStatus(RelBackupRestore *brExpr, ExeCliInterface *cliInterface, CmpDDLwithStatusInfo *dws);

  // regular or incremental backup of subset of objects (schemas or tables)
  short backupSubset(RelBackupRestore *ddlExpr, ExeCliInterface *cliInterface, NAString &currCatName,
                     NAString &currSchName, CmpDDLwithStatusInfo *dws);

  short createSnapshotForIncrBackup(ExeCliInterface *cliInterface, const NABoolean incrBackupEnabled,
                                    const NAString &btNamespace, const NAString &catalogNamePart,
                                    const NAString &schemaNamePart, const NAString &objectNamePart, Int64 objDataUID);

  short setHiatus(const NAString &hiatusObjectName, NABoolean resetHiatus, NABoolean createSnapIfNotExist = FALSE,
                  NABoolean ignoreSnapIfNotExist = FALSE);

  // restore all or subset of objects
  short restore(RelBackupRestore *ddlExpr, ExeCliInterface *cliInterface, NAString &currCatName, NAString &currSchName,
                CmpDDLwithStatusInfo *dws);

  short restoreSystem(RelBackupRestore *brExpr, ExeCliInterface *cliInterface, NAString &currCatName,
                      NAString &currSchName, CmpDDLwithStatusInfo *dws, ExpHbaseInterface *ehi, Int32 restoreThreads);

  short restoreSubset(RelBackupRestore *brExpr, ExeCliInterface *cliInterface, NAString &currCatName,
                      NAString &currSchName, CmpDDLwithStatusInfo *dws, ExpHbaseInterface *ehi, Int32 restoreThreads);

  short returnObjectsList(ExeCliInterface *cliInterface, const char *objUIDsWherePred, char *srcSch,
                          NAArray<HbaseStr> *&objectsList);

  short showBRObjects(RelBackupRestore *brExpr, NAString &qualBackupSch, NAArray<HbaseStr> *mdList,
                      NAArray<HbaseStr> *objectsList, CmpDDLwithStatusInfo *dws, ExeCliInterface *cliInterface);

  short deleteSnapshot(ExpHbaseInterface *ehi, const char *oper, const char *backupTag, NABoolean ifExists,
                       NABoolean cascade = FALSE,  // drop dependent incr backups
                       NABoolean force = FALSE,    // drop even if this is the last backup
                       NABoolean skipLock = FALSE);

  short dropBackupSnapshots(RelBackupRestore *ddlExpr, ExeCliInterface *cliInterface);
  short dropBackupMD(RelBackupRestore *ddlExpr, ExeCliInterface *cliInterface, NABoolean checkPrivs = TRUE);
  short dropBackupTags(RelBackupRestore *ddlExpr, ExeCliInterface *cliInterface);
  short getBackupMD(RelBackupRestore *ddlExpr, ExeCliInterface *cliInterface, CmpDDLwithStatusInfo *dws);

  short getBackupTags(RelBackupRestore *ddlExpr, ExeCliInterface *cliInterface, CmpDDLwithStatusInfo *dws);

  short getVersionOfBackup(RelBackupRestore *ddlExpr, ExeCliInterface *cliInterface, CmpDDLwithStatusInfo *dws);

  short exportOrImportBackup(RelBackupRestore *ddlExpr, ExeCliInterface *cliInterface);

  short getBackupSnapshots(RelBackupRestore *ddlExpr, ExeCliInterface *cliInterface, CmpDDLwithStatusInfo *dws);

  short lockSQL();
  short unlockSQL();
  NABoolean isSQLLocked();
  short lockAll();
  void unlockAll();

  Int64 getIndexFlags(ExeCliInterface *cliInterface, Int64 objUid, Int64 indexUID);

  short checkIfBackupMDisLocked(RelBackupRestore *brExpr, ExeCliInterface *cliInterface, const char *inOper,
                                NAString &oper, NAString &tag);

  short checkIfObjectsLockedInMD(ExeCliInterface *cliInterface, char *objUIDsWherePred, char *srcSch,
                                 const char *operation, char *backupTag, NAString &lockedObjs);

  short isTruncateTableLockedInMD(ExeCliInterface *cliInterface, NAString &lockedObjs);

  Lng32 isBRInProgress(ExeCliInterface *cliInterface, const char *catalogName, const char *schemaName,
                       const char *objectName, Int64 &objUid);

  short lockObjectsInMD(ExeCliInterface *cliInterface, const char *oper, char *objUIDsWherePred, char *srcSch,
                        char *backupTag,

                        // lock schemas, if full backup or schemas only
                        // backup is being done
                        NABoolean schemasOnlyBackup);

  short lockBackupMD(RelBackupRestore *brExpr, ExeCliInterface *cliInterface, const char *oper, NABoolean override);

  short unlockBackupMD(RelBackupRestore *brExpr, ExeCliInterface *cliInterface, const char *oper);

  short unlockObjectsInMD(ExeCliInterface *cliInterface, char *objUIDsWherePred, char *srcSch,
                          NABoolean schemasOnlyBackup);

  short getBackupLockedObjects(RelBackupRestore *ddlExpr, ExeCliInterface *cliInterface, CmpDDLwithStatusInfo *dws);

  short cleanupLockedObjects(RelBackupRestore *brExpr, ExeCliInterface *cliInterface);

  short cleanupLockForTag(RelBackupRestore *brExpr, ExeCliInterface *cliInterface, ExpHbaseInterface *ehi);

  short createBackupTags(RelBackupRestore *brExpr, ExeCliInterface *cliInterface);

  short createNamespacesForRestore(RelBackupRestore *brExpr, std::vector<std::string> *schemaListTV,
                                   std::vector<std::string> *tableListTV, NABoolean createrReservedNS,
                                   ExpHbaseInterface *ehi, Int32 restoreThreads);

  short cleanupBackupMetadata(ExeCliInterface *cliInterface, NAString &backupTag);

  short cleanupLinkedBackupMD(ExeCliInterface *cliInterface, NAArray<HbaseStr> *backupList);

  short genLobLocation(Int64 objUID, const char *schName, ExeCliInterface *cliInterface,
                       // TextVec &lobLocList,
                       std::vector<std::string> &lobLocList, Int32 &numLOBfiles, NABoolean forDisplay);

  short genLobObjs(ElemDDLList *tableList, char *srcSch, ExeCliInterface *cliInterface, Queue *&lobsQueue,
                   NAString &lobObjs);

  short genIndexObjs(ElemDDLList *tableList, char *srcSch, ExeCliInterface *cliInterface, Queue *&indexesQueue,
                     NAString &indexesObjs);

  short genReferencedObjs(ElemDDLList *tableList, char *srcSch, ExeCliInterface *cliInterface, Queue *&refsQueue,
                          NAString &refsObjs);

  short genLibsUsedProcs(NAString libsToBackup, NAString &libsUsedProcs, ExeCliInterface *cliInterface);

  short genProcsUsedLibs(NAString procsToBackup, NAString &procsUsedLibs, ExeCliInterface *cliInterface);

  short genListOfObjs(ElemDDLList *objList, NAString &objsToBackup, NABoolean &prependOR, NAString *schPrivsList);

  short genBRObjUIDsWherePred(RelBackupRestore *brExpr, ElemDDLList *schemaList, ElemDDLList *tableList,
                              ElemDDLList *viewList, ElemDDLList *procList, ElemDDLList *libList, NAString &currCatName,
                              NAString &currSchName, NAString &objUIDsWherePred, char *srcSch,
                              ExeCliInterface *cliInterface, Queue *&lobsQueue, Queue *&indexesQueue, Queue *&refsQueue,
                              NAString *schPrivsList,

                              // return TRUE, if full backup or schemas only
                              // backup is being done.
                              NABoolean &schemasOnlyBackup);

  short populateDiags(const char *oper, const char *errMsg, const char *string0, const char *string1, int retcode);

  NABoolean containsTagInSnapshotList(NAArray<HbaseStr> *snapshotList, char *brExprTag, NAString &oper,
                                      NAString &status);

  // If backup tags is imported from others cluster, we need to get it's timestamp
  // from snapshot attributes, rather than from metadata.
  NABoolean getTimeStampFromSnapshotList(NAArray<HbaseStr> *snapshotList, char *brExprTag, char *timeStamp,
                                         ComDiagsArea **diagsArea);

  short checkCompatibility(BackupAttrList *attrList, NAString &errorMsg);

  short getExtendedAttributes(ExpHbaseInterface *ehi, const char *backupTag, BackupAttrList *attrList,
                              NAString &errorMsg);

  Int32 getOwnerList(ExpHbaseInterface *ehi, NAArray<HbaseStr> *backupList, std::vector<string> &ownerList);
  short getUsersAndRoles(ExeCliInterface *cliInterface, const char *objUIDsWherePred, const char *srcSch, Lng32 &numUR,
                         NAString &userOrRoles);
  short checkUsersAndRoles(ExeCliInterface *cliInterface, const char *userOrRoles, NAString &lackedUR,
                           std::map<Int32, NAString> &userMap);

  short verifyBRAuthority(const char *privsWherePred, ExeCliInterface *cliInterface, NABoolean checkShowPrivilege,
                          NABoolean isBackup, const char *schToUse, const NABoolean checkSpecificPrivs = FALSE,
                          const char *owner = NULL);

  short checkBackupAuthIDs(ExeCliInterface *cliInterface);

  short fixBackupAuthIDs(ExeCliInterface *cliInterface, NAString &backupScheName,
                         std::map<Int32, NAString> &unMatchedUserIDs);

  //////////////////////////////////////
  // End methods for Backup/Restore
  //////////////////////////////////////

  short dumpMetadataOfObjects(RelDumpLoad *ddlExpr, ExeCliInterface *cliInterface);

  short loadMetadataOfObjects(RelDumpLoad *ddlExpr, ExeCliInterface *cliInterface);

  short generateStoredStats(Int64 tableUID, Int64 objDataUID, const NAString &catName, const NAString &schName,
                            const NAString &objName, const NAString &nameSpace, Int32 ctlFlags, Int32 partialRowSize,
                            Int32 numCols, ExpHbaseInterface *ehi, ComTdbVirtTableStatsInfo *&statsInfo);

  short lookForTableInMD(ExeCliInterface *cliInterface, NAString &catNamePart, NAString &schNamePart,
                         NAString &objNamePart, NABoolean schNameSpecified, NABoolean isHbaseMapSpecified,
                         ComObjectName &tableName, NAString &tabName, NAString &extTableName,
                         const ComObjectType objectType = COM_BASE_TABLE_OBJECT,
                         NABoolean checkForValidHbaseName = TRUE);

  short genLobChunksTableName(ExeCliInterface *cliInterface, const char *catName, const char *schName,
                              const char *objName, const char *chunksSchName, NAString &fullyQualChunksName,
                              NAString *chunksObjName = NULL);

  static NABoolean getBootstrapMode() { return bootstrapMode_; }
  static void setBootstrapMode(NABoolean v) { bootstrapMode_ = v; }

  short updateSeabaseXDCDDLTable(ExeCliInterface *cliInterface, const char *catName, const char *schName,
                                 const char *objName, const char *objType);

  short deleteFromXDCDDLTable(ExeCliInterface *cliInterface, const char *catName, const char *schName,
                              const char *objName, const char *objType);

  Lng32 lockTruncateTable(ExeCliInterface *cliInterface, Int64 objUid);
  Lng32 unlockTruncateTable(ExeCliInterface *cliInterface, Int64 objUid);

  enum CacheAndStoredDescOp {
    UPDATE_SHARED_CACHE = 0x00000001,
    UPDATE_REDEF_TIME = 0x00000002,
    UPDATE_XDC_DDL_TABLE = 0x00000004,
    REMOVE_NATABLE = 0x00000008
  };

  static void setCacheAndStoredDescOp(UInt32 &op, UInt32 bitOp) { op |= bitOp; }

  static void resetCacheAndStoredDescOp(UInt32 &op, UInt32 bitOp) { op &= ~bitOp; }

  static NABoolean isCacheAndStoredDescOpSet(UInt32 op, UInt32 bitOp) { return (op & bitOp) != 0; }

  Lng32 updateCachesAndStoredDesc(ExeCliInterface *cliInterface, const NAString &catName, const NAString &schName,
                                  const NAString &objName, ComObjectType objType, UInt32 operations,
                                  RemoveNATableParam *rmNATableParam = NULL,
                                  UpdateObjRefTimeParam *updOjbRedefParam = NULL,
                                  SharedCacheDDLInfo::DDLOperation ddlOp = SharedCacheDDLInfo::DDL_UNKNOWN);

  Lng32 updateCachesAndStoredDescByNATable(ExeCliInterface *cliInterface, const NAString &catName,
                                           const NAString &schName, const NAString &objName, ComObjectType objType,
                                           UInt32 operations, NATable *natable,
                                           RemoveNATableParam *rmNATableParam = NULL,
                                           SharedCacheDDLInfo::DDLOperation ddlOp = SharedCacheDDLInfo::DDL_UNKNOWN);

  // Check the values(for(), partition name) we input is valid or not
  // dupliPartName: duplicate partition name
  // partEntityNameVec: all partition entity name (include for() and partition name which we input)
  // partEntityNameMap: the map of partition entity name to partition name
  //
  // return :
  // -1: error
  // 0: valid
  // 1: duplicate
  // 2: invalid or out-of-range for()
  // 3: partition name we specied not exist
  Lng32 checkPartition(ElemDDLPartitionNameAndForValuesArray *partArray, TableDesc *tableDesc, BindWA &bindWA,
                       NAString &objectName, NAString &invalidPartName, /* out */
                       std::vector<NAString> *partEntityNameVec = NULL /* out */,
                       std::map<NAString, NAString> *partEntityNameMap = NULL /* out */);

 public:
  inline void setDDLQuery(char *query) { ddlQuery_ = query; }
  inline void setDDLLen(Lng32 len) { ddlLength_ = len; }

 private:
  enum { NUM_MAX_PARAMS = 20 };

  NAHeap *heap_;
  stack<ULng32> savedCmpParserFlags_;
  stack<ULng32> savedCliParserFlags_;

  NAString seabaseSysCat_;
  NAString seabaseMDSchema_; /* Qualified metadata schema */

  // saved CQD values

  const char *param_[NUM_MAX_PARAMS];

  NABoolean cmpSwitched_;

  // if an object was marked as a 'hiatus' object, its name is
  // saved here.
  NAString hiatusObjectName_;

  static THREAD_P NABoolean bootstrapMode_;

  static const NAString systemSchemas[16];

  // A cache to hold UID etc information about system tables/indices.
  static THREAD_P SystemObjectInfoHashTableType *systemObjectInfoCache_;
  static THREAD_P NABoolean systemObjectInfoCacheEnabled_;
  // for DDL XDC, we do not own this memory
  char *ddlQuery_;
  Lng32 ddlLength_;
  IndependentLockController *dlockForStoredDesc_;
};

#endif  // _CMP_SEABASE_DDL_H_
