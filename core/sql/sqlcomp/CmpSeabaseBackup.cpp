/**********************************************************************
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
**********************************************************************/

/* -*-C++-*-
 *****************************************************************************
 *
 * File:         CmpSeabaseBackup.cpp
 * Description:  Implements online backup/restore, export/import, delete/list
 *
 *
 * Created:
 * Language:     C++
 *
 * This code calls the following methods to initialize ExpHbaseInterface:
 *
 *   allocBRCEHI
 *   allocEHI(COM_STORAGE_HBASE);
 *
 * allocBRCEHI is a wrapper for allocEHI, so they both do the same thing
 *
 *****************************************************************************
 */
#include "sqlcomp/CmpSeabaseDDLincludes.h"
#include "optimizer/RelExeUtil.h"
#include "cli/Globals.h"
#include "cli/Context.h"
#include "runtimestats/SqlStats.h"
#include "common/feerrors.h"
#include "dtm/tm.h"
#include "sqlcomp/PrivMgrComponentPrivileges.h"
#include "sqlcomp/PrivMgrCommands.h"
#include "sqlcomp/CmpSeabaseDDLrepos.h"
#include "sqlcomp/CmpSeabaseBackupAttrs.h"
#include "comexe/LateBindInfo.h"
#include "exp/exp_datetime.h"

static THREAD_P int stepSeq = 0;
static THREAD_P int operCnt = 0;

struct NativeHBaseTableInfo {
  const char *catName;
  const char *schName;
  const char *objName;
};

static const NativeHBaseTableInfo nativeHBaseTables[] = {
    {TRAFODION_SYSCAT_LIT, SEABASE_DTM_SCHEMA, SEABASE_BINLOG_READER}, {ESG_SEQ_CAT, ESG_SEQ_SCHEMA, ESG_SEQ_TABLE}};

static NABoolean isHBaseNativeTable(NAString &tableName) {
  NAList<NAString> fields(STMTHEAP);
  tableName.split('.', fields);
  if (fields.entries() <= 1) return FALSE;
  for (int i = 0; i < sizeof(nativeHBaseTables) / sizeof(NativeHBaseTableInfo); i++) {
    NAString nativeTable = "";
    if (fields.entries() == 3) nativeTable += NAString(nativeHBaseTables[i].catName) + ".";
    nativeTable += NAString(nativeHBaseTables[i].schName) + "." + NAString(nativeHBaseTables[i].objName);
    if (tableName == nativeTable) return TRUE;
  }
  return FALSE;
}

short CmpSeabaseDDL::lockSQL() {
  CliGlobals *cliGlobals = GetCliGlobals();
  if (cliGlobals->getStatsGlobals() == NULL) {
    *CmpCommon::diags() << DgSqlCode(-CAT_INTERNAL_EXCEPTION_ERROR);
    return -1;
  }

  ExSsmpManager *ssmpManager = cliGlobals->currContext()->getSsmpManager();
  if (ssmpManager == NULL) {
    *CmpCommon::diags() << DgSqlCode(-CAT_INTERNAL_EXCEPTION_ERROR);
    return -1;
  }

  ComDiagsArea *tempDiagsArea = CmpCommon::diags();

  IpcServer *ssmpServer =
      ssmpManager->getSsmpServer(heap_, cliGlobals->myNodeName(), cliGlobals->myCpu(), tempDiagsArea);
  if (ssmpServer == NULL) {
    *CmpCommon::diags() << DgSqlCode(-CAT_INTERNAL_EXCEPTION_ERROR);
    return -1;
  }

  SsmpClientMsgStream *ssmpMsgStream = new (cliGlobals->getIpcHeap())
      SsmpClientMsgStream((NAHeap *)cliGlobals->getIpcHeap(), ssmpManager, tempDiagsArea);

  ssmpMsgStream->addRecipient(ssmpServer->getControlConnection());

  SnapshotLockRequest *slMsg = new (cliGlobals->getIpcHeap()) SnapshotLockRequest(cliGlobals->getIpcHeap());

  *ssmpMsgStream << *slMsg;

  // Call send with no timeout.
  ssmpMsgStream->send();

  // I/O is now complete.
  slMsg->decrRefCount();

  cliGlobals->getEnvironment()->deleteCompletedMessages();
  ssmpManager->cleanupDeletedSsmpServers();
  return 0;
}

short CmpSeabaseDDL::unlockSQL() {
  CliGlobals *cliGlobals = GetCliGlobals();
  if (cliGlobals->getStatsGlobals() == NULL) {
    *CmpCommon::diags() << DgSqlCode(-CAT_INTERNAL_EXCEPTION_ERROR);
    return -1;
  }

  ExSsmpManager *ssmpManager = cliGlobals->currContext()->getSsmpManager();
  if (ssmpManager == NULL) {
    *CmpCommon::diags() << DgSqlCode(-CAT_INTERNAL_EXCEPTION_ERROR);
    return -1;
  }

  ComDiagsArea *tempDiagsArea = CmpCommon::diags();

  IpcServer *ssmpServer =
      ssmpManager->getSsmpServer(heap_, cliGlobals->myNodeName(), cliGlobals->myCpu(), tempDiagsArea);
  if (ssmpServer == NULL) {
    *CmpCommon::diags() << DgSqlCode(-CAT_INTERNAL_EXCEPTION_ERROR);
    return -1;
  }

  SsmpClientMsgStream *ssmpMsgStream = new (cliGlobals->getIpcHeap())
      SsmpClientMsgStream((NAHeap *)cliGlobals->getIpcHeap(), ssmpManager, tempDiagsArea);

  ssmpMsgStream->addRecipient(ssmpServer->getControlConnection());

  SnapshotUnLockRequest *sulMsg = new (cliGlobals->getIpcHeap()) SnapshotUnLockRequest(cliGlobals->getIpcHeap());

  *ssmpMsgStream << *sulMsg;

  // Call send with no timeout.
  ssmpMsgStream->send();

  // I/O is now complete.
  sulMsg->decrRefCount();

  cliGlobals->getEnvironment()->deleteCompletedMessages();
  ssmpManager->cleanupDeletedSsmpServers();
  return 0;
}

NABoolean CmpSeabaseDDL::isSQLLocked() {
  StatsGlobals *statsGlobals = GetCliGlobals()->getStatsGlobals();
  CMPASSERT(statsGlobals != NULL);

  return (statsGlobals->isSnapshotInProgress());
}

short CmpSeabaseDDL::lockAll() {
  if (isSQLLocked()) {
    *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("This")
                        << DgString1(
                               "Reason: Another backup or restore operation is in progress and need to finish first.");

    return -1;
  }

  short rc;

  rc = lockSQL();
  if (rc) {
    *CmpCommon::diags() << DgSqlCode(-5051) << DgString0("lock") << DgInt0(rc);

    return -1;
  }

  rc = DTM_LOCKTM();
  if (rc != FEOK) {
    *CmpCommon::diags() << DgSqlCode(-5051) << DgString0("lock") << DgInt0(rc);

    // make sure sql unlock succeeds
    rc = unlockSQL();
    return -1;
  }

  return 0;
}

void CmpSeabaseDDL::unlockAll() {
  short rc = DTM_UNLOCKTM();
  if (rc != 0) {
    *CmpCommon::diags() << DgSqlCode(-5051) << DgString0("unlock") << DgInt0(rc);

    return;
  }

  rc = unlockSQL();
  if (rc != 0) {
    *CmpCommon::diags() << DgSqlCode(-5051) << DgString0("unlock") << DgInt0(rc);

    return;
  }
}

short CmpSeabaseDDL::backupInit(const char *oper, NABoolean lock) {
  short error = 0;
  short rc = 0;
  int retcode = 0;

  if (isSQLLocked()) {
    *CmpCommon::diags() << DgSqlCode(-5050) << DgString0(oper)
                        << DgString1(
                               "Reason: Another backup or restore operation is in progress and need to finish first.");

    return -1;
  }

  // Lock before getting the list of snapshot tables.
  // This ensures we get a consistent list.
  if (lock) {
    rc = lockAll();
    if (rc != FEOK) {
      // diags area has already been set.

      return -1;
    }
  }

  return 0;
}

struct BackupMDsInfo {
  const char *catName;
  const char *schName;
  const char *objName;
  const NABoolean hasLob;
};

static const BackupMDsInfo backupMDs[] = {
    {TRAFODION_SYSTEM_CATALOG, SEABASE_MD_SCHEMA, SEABASE_COLUMNS, FALSE},
    {TRAFODION_SYSTEM_CATALOG, SEABASE_MD_SCHEMA, SEABASE_INDEXES, FALSE},
    {TRAFODION_SYSTEM_CATALOG, SEABASE_MD_SCHEMA, SEABASE_KEYS, FALSE},
    {TRAFODION_SYSTEM_CATALOG, SEABASE_MD_SCHEMA, SEABASE_LIBRARIES, TRUE},
    {TRAFODION_SYSTEM_CATALOG, SEABASE_MD_SCHEMA, SEABASE_LIBRARIES_USAGE, FALSE},
    {TRAFODION_SYSTEM_CATALOG, SEABASE_MD_SCHEMA, SEABASE_LOB_COLUMNS, FALSE},
    {TRAFODION_SYSTEM_CATALOG, SEABASE_MD_SCHEMA, SEABASE_OBJECTS, FALSE},
    {TRAFODION_SYSTEM_CATALOG, SEABASE_MD_SCHEMA, SEABASE_REF_CONSTRAINTS, FALSE},
    {TRAFODION_SYSTEM_CATALOG, SEABASE_MD_SCHEMA, SEABASE_ROUTINES, FALSE},
    {TRAFODION_SYSTEM_CATALOG, SEABASE_MD_SCHEMA, SEABASE_SEQ_GEN, FALSE},
    {TRAFODION_SYSTEM_CATALOG, SEABASE_MD_SCHEMA, SEABASE_TABLES, FALSE},
    {TRAFODION_SYSTEM_CATALOG, SEABASE_MD_SCHEMA, SEABASE_TABLE_CONSTRAINTS, FALSE},
    {TRAFODION_SYSTEM_CATALOG, SEABASE_MD_SCHEMA, SEABASE_TEXT, FALSE},
    {TRAFODION_SYSTEM_CATALOG, SEABASE_MD_SCHEMA, SEABASE_UNIQUE_REF_CONSTR_USAGE, FALSE},
    {TRAFODION_SYSTEM_CATALOG, SEABASE_MD_SCHEMA, SEABASE_VIEWS, FALSE},
    {TRAFODION_SYSTEM_CATALOG, SEABASE_MD_SCHEMA, SEABASE_VIEWS_USAGE, FALSE},

    {TRAFODION_SYSTEM_CATALOG, SEABASE_PRIVMGR_SCHEMA, PRIVMGR_COLUMN_PRIVILEGES, FALSE},
    {TRAFODION_SYSTEM_CATALOG, SEABASE_PRIVMGR_SCHEMA, PRIVMGR_OBJECT_PRIVILEGES, FALSE},
    {TRAFODION_SYSTEM_CATALOG, SEABASE_PRIVMGR_SCHEMA, PRIVMGR_SCHEMA_PRIVILEGES, FALSE},

    {TRAFODION_SYSTEM_CATALOG, SEABASE_SYSTEM_SCHEMA, HBASE_HIST_NAME, FALSE},
    {TRAFODION_SYSTEM_CATALOG, SEABASE_SYSTEM_SCHEMA, HBASE_HISTINT_NAME, FALSE},
    {TRAFODION_SYSTEM_CATALOG, SEABASE_SYSTEM_SCHEMA, HBASE_PERS_SAMP_NAME, FALSE}};

// create backup schema of format:  _BACKUP_<backuptag>_
static short genBackupSchemaName(const char *backupTag, NAString &backupSch) {
  backupSch = NAString(TRAF_BACKUP_MD_PREFIX);
  backupSch += backupTag;
  backupSch += "_";

  return 0;
}

// if catName passed in, use it and return fully qualified name with
// schema double quoted.
// Otherwise return sch.obj name without double quotes
short CmpSeabaseDDL::genLobChunksTableName(ExeCliInterface *cliInterface, const char *catName, const char *schName,
                                           const char *objName, const char *chunksSchName,
                                           NAString &fullyQualChunksName, NAString *chunksObjName) {
  long objUID = getObjectUID(cliInterface, (catName ? catName : TRAFODION_SYSCAT_LIT), schName, objName,
                              COM_BASE_TABLE_OBJECT_LIT, NULL, NULL, NULL, FALSE, TRUE);
  if (objUID < 0) return -1;  // does not exist

  char buf[1000];
  if (catName)
    str_sprintf(buf, "%s.\"%s\".%s%020ld", catName, chunksSchName, LOB_CHUNKS_V2_PREFIX, objUID);
  else
    str_sprintf(buf, "%s.%s.%s%020ld", TRAFODION_SYSCAT_LIT, chunksSchName, LOB_CHUNKS_V2_PREFIX, objUID);

  fullyQualChunksName = buf;

  if (chunksObjName) {
    str_sprintf(buf, "%s%020ld", LOB_CHUNKS_V2_PREFIX, objUID);
    *chunksObjName = buf;
  }

  return 0;
}

// ----------------------------------------------------------------------------
// method: unlockSnapshotMeta
//
// operationLock is called by backup restore for the following:
//    backupObjects
//    setHiatus
//    createSnapshotForIncrBackup
//    restoreObjects
//    deleteBackup
//
// Generally, when a failure occurs, the snapshot meta lock is removed.
// However, there are some cases where a lock may not have been removed.
// This method can be called to determine if there is a lock and try to
// remove it - if the passed in tag matches the tag holding the lock
//
// lockedTag returns:
//    empty string - no locks exist
//    "?" - error trying to get a lock
//    other value - lock holder
//
// returns:
//   1 - snapshot metadata is locked by a different tag
//   0 - lock no longer exists, unable to retrieve lock data, or lock was removed
//  -1 - error trying to unlock snapshot meta
// ----------------------------------------------------------------------------
static short unlockSnapshotMeta(ExpHbaseInterface *ehi, NAString backupTag, NAString &lockedTag) {
  lockedTag = ehi->lockHolder();

  // snapshot meta is not locked or unable to get lock information
  if (lockedTag.length() == 0) return 0;

  if (lockedTag == "?") return -1;

  // snapshot meta is locked by specified tag, unlock
  if (lockedTag == backupTag) return ehi->operationUnlock(lockedTag, FALSE, FALSE);

  return 1;
}

// ----------------------------------------------------------------------------
// method: hasAccess
//
// Checks to see if current user is elevated, is the owner, or has been granted
// SHOW privilege (checkForShow is true).
//
// Returns true or false.
//
// Differs from verifyBRAuthority that also checks stored metadata for
// appropriate privileges, and if not found, adds an error to the ComDiags
// area.
// ----------------------------------------------------------------------------
static bool hasAccess(const char *owner, bool checkForShow) {
  if (!CmpCommon::context()->isAuthorizationEnabled()) return true;

  // If current user is the owner, okay
  if (owner != NULL) {
    if (strcmp(ComUser::getCurrentUsername(), owner) == 0) return true;
  }

  // If elevated, okay
  if (ComUser::currentUserHasElevatedPrivs(NULL /*don't populate diags*/)) return true;

  // If current user has associated component privileges, okay
  int userID = ComUser::getCurrentUser();
  PrivMgrComponentPrivileges compPriv;
  if (compPriv.hasSQLPriv(userID, SQLOperation::MANAGE_LOAD) ||
      (checkForShow && compPriv.hasSQLPriv(userID, SQLOperation::SHOW, true)))
    return true;

  return false;
}

//////////////////////////////////////////////////////////////////////
// Table where progress status for BREI (Backup/Restore/Export/Import)
// operations is updated.
//
// Some of the columns are updated from backuprestore client in java.
// It uses hbase put api to do that.
//
//
// If the number of columns/types/size/order in progress table
// is changed, then make sure to update the corresponding code in
// method updateBREIprogress in file BackupRestoreClient.java.
//
//////////////////////////////////////////////////////////////////////
short CmpSeabaseDDL::createProgressTable(ExeCliInterface *cliInterface) {
  int cliRC = 0;

  // create table, if doesnt exist, where progress status will be updated
  char psQuery[2000];
  str_sprintf(
      psQuery,
      "create table if not exists %s.\"%s\".\"%s\" (oper char(20) character set iso88591 not null, subOper varchar(20) "
      "character set iso88591, stepText char(40) character set iso88591 not null, state varchar(25) character set "
      "iso88591, tag char(64) character set iso88591 not null, startTime largeint unsigned not null not serialized, "
      "endTime largeint unsigned not null not serialized, param1 int unsigned not null not serialized, param2 int "
      "unsigned not null not serialized, perc int unsigned not null not serialized, queryId varchar(160) character set "
      "iso88591, stepSeq int not null, tag_oper_cnt char(8) character set iso88591 not null, primary key(oper, tag, "
      "stepText, tag_oper_cnt)) hbase_options (max_versions = '1') attribute hbase format",
      TRAFODION_SYSCAT_LIT, SEABASE_REPOS_SCHEMA, "BREI_PROGRESS_STATUS_TABLE");
  cliRC = cliInterface->executeImmediate(psQuery);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());

    *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("BACKUP")
                        << DgString1("Reason: Error occurred while creating progress status table.");

    return -1;
  }

  return 0;
}

short CmpSeabaseDDL::dropProgressTable(ExeCliInterface *cliInterface) {
  int cliRC = 0;

  char psQuery[2000];
  str_sprintf(psQuery, "drop table if exists %s.\"%s\".\"%s\" ", TRAFODION_SYSCAT_LIT, SEABASE_REPOS_SCHEMA,
              "BREI_PROGRESS_STATUS_TABLE");
  cliRC = cliInterface->executeImmediate(psQuery);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());

    *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("CLEANUP")
                        << DgString1("Reason: Error occurred while dropping progress status table.");

    return -1;
  }

  return 0;
}

short CmpSeabaseDDL::truncateProgressTable(ExeCliInterface *cliInterface) {
  int cliRC = 0;

  char psQuery[2000];
  str_sprintf(psQuery, "truncate table %s.\"%s\".\"%s\" ", TRAFODION_SYSCAT_LIT, SEABASE_REPOS_SCHEMA,
              "BREI_PROGRESS_STATUS_TABLE");
  cliRC = cliInterface->executeImmediate(psQuery);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());

    *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("CLEANUP")
                        << DgString1("Reason: Error occurred while truncating progress status table.");

    return -1;
  }

  return 0;
}

short CmpSeabaseDDL::deleteFromProgressTable(ExeCliInterface *cliInterface, RelBackupRestore *brExpr,
                                             const char *oper) {
  int cliRC = 0;
  char psQuery[2000];

  // delete existing status rows for specified 'oper'.
  // Rows will be deleted if they are for the current tag,
  // or if 'oper' was previously finalized,
  // or previous operation resulted in error,
  // or if previous operation was started prior to current time.
  str_sprintf(psQuery,
              "delete from %s.\"%s\".\"%s\" where oper = '%s' and (tag = '%s' or tag in (select tag from "
              "%s.\"%s\".\"%s\" where oper = '%s' and (steptext = 'Finalize' or state = 'Failed' or starttime <= "
              "juliantimestamp(current_timestamp_utc) ) ) )",
              TRAFODION_SYSCAT_LIT, SEABASE_REPOS_SCHEMA, "BREI_PROGRESS_STATUS_TABLE", oper,
              brExpr->backupTag().data(), TRAFODION_SYSCAT_LIT, SEABASE_REPOS_SCHEMA, "BREI_PROGRESS_STATUS_TABLE",
              oper);

  cliRC = cliInterface->executeImmediate(psQuery);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());

    *CmpCommon::diags() << DgSqlCode(-5050) << DgString0(oper)
                        << DgString1("Reason: Error occurred while deleting from progress status table.");

    return -1;
  }

  stepSeq = 1;

  return 0;
}

int CmpSeabaseDDL::getBREIOperCount(ExeCliInterface *cliInterface, const char *oper, const char *tag) {
  int cliRC = 0;
  char psQuery[300];
  NAString stepText = "Initialize";

  str_sprintf(psQuery,
              "select count(tag_oper_cnt) from %s.\"%s\".\"%s\" where oper='%s' and tag='%s' and steptext='%s';",
              TRAFODION_SYSCAT_LIT, SEABASE_REPOS_SCHEMA, "BREI_PROGRESS_STATUS_TABLE", oper, tag, stepText.data());

  cliRC = cliInterface->fetchRowsPrologue(psQuery);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    *CmpCommon::diags() << DgSqlCode(-5050) << DgString0(oper)
                        << DgString1("Reason: Error occurred while select oper count from progress status table.");

    return -1;
  }

  cliRC = cliInterface->fetch();
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    *CmpCommon::diags() << DgSqlCode(-5050) << DgString0(oper)
                        << DgString1("Reason: Error occurred while select oper count from progress status table.");

    return -1;
  }

  char *ptr = NULL;
  int len = 0;
  cliInterface->getPtrAndLen(1, ptr, len);
  int oper_cnt = *(int *)ptr;
  return oper_cnt;
}

short CmpSeabaseDDL::insertProgressTable(ExeCliInterface *cliInterface, RelBackupRestore *brExpr, const char *oper,
                                         const char *stepText, const char *state, int param1, int param2) {
  int cliRC = 0;
  char psQuery[2000];

  if (brExpr->showObjects()) return 0;

  NAString backupTag(brExpr->backupTag().data());

  // if 'Initialized' step is being done, then find out the type of
  // backup(regular or incremental) associated with the specified tag.
  // For Backup operation, type is specified as part of command.
  // For other operations, find it based on the specified backupTag.
  NAString subOper;
  if (NAString(stepText) == "Initialize") {
    if (strcmp(oper, "BACKUP") == 0)
      subOper = (brExpr->getIncremental() ? "INCREMENTAL" : "REGULAR");
    else {
      ExpHbaseInterface *ehi = allocBRCEHI();
      if (ehi == NULL) {
        //  Diagnostic already populated.
        return -1;
      }

      NAArray<HbaseStr> *snapshotList = NULL;
      int retcode = ehi->listAllBackups(&snapshotList, FALSE, FALSE);
      if (retcode < 0) {
        populateDiags("GET BACKUP TAGS", "Error returned during list all backups.", "ExpHbaseInterface::listAllBackups",
                      getHbaseErrStr(-retcode), -retcode);
        ehi->close();
        return -1;
      }

      if (snapshotList && (snapshotList->entries() > 0)) {
        NAString backupOper;
        NAString backupStatus;

        NABoolean tagInSnapshotList =
            containsTagInSnapshotList(snapshotList, (char *)backupTag.data(), backupOper, backupStatus);

        if ((backupOper == "REGULAR") || (backupOper == "INCREMENTAL")) subOper = backupOper;
      }
    }
  }

  char str_cnt[8];
  str_sprintf(
      psQuery,
      "insert into %s.\"%s\".\"%s\" values ('%s', '%s', '%s', '%s', '%s', juliantimestamp(current_timestamp_utc), "
      "juliantimestamp(current_timestamp_utc), %d, %d, 0, '%s', %d, '%s');",
      TRAFODION_SYSCAT_LIT, SEABASE_REPOS_SCHEMA, "BREI_PROGRESS_STATUS_TABLE", oper,
      (subOper.isNull() ? " " : subOper.data()), stepText, state, backupTag.data(), param1, param2,
      CmpCommon::context()->sqlSession()->getParentQid(), stepSeq, str_itoa(operCnt, str_cnt));

  cliInterface->executeImmediate("cqd query_cache '0'");
  cliRC = cliInterface->executeImmediate(psQuery);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());

    cliInterface->executeImmediate("cqd query_cache reset");
    *CmpCommon::diags() << DgSqlCode(-5050) << DgString0(oper)
                        << DgString1("Reason: Error occurred while updating progress status table.");

    return -1;
  }

  cliInterface->executeImmediate("cqd query_cache reset");

  stepSeq++;

  return 0;
}

short CmpSeabaseDDL::updateProgressTable(ExeCliInterface *cliInterface, const char *backupTag, const char *oper,
                                         const char *stepText, const char *state, int param2, int perc) {
  int cliRC = 0;
  char psQuery[2000];

  // if progress indication is not to be returned, dont update progress status table
  if ((CmpCommon::getDefaultNumeric(TRAF_BREI_PROGRESS_UPDATE_DELAY) == -1) && (NAString(state) != "Completed"))
    return 0;

  char str_cnt[8];
  if (perc < 0)
    str_sprintf(psQuery,
                "update %s.\"%s\".\"%s\" set state = '%s', endTime = juliantimestamp(current_timestamp_utc) where oper "
                "= '%s' and stepText = '%s' and tag = '%s' and tag_oper_cnt = '%s';",
                TRAFODION_SYSCAT_LIT, SEABASE_REPOS_SCHEMA, "BREI_PROGRESS_STATUS_TABLE", state, oper, stepText,
                backupTag, str_itoa(operCnt, str_cnt));
  else
    str_sprintf(psQuery,
                "update %s.\"%s\".\"%s\" set state = '%s', endTime = juliantimestamp(current_timestamp_utc), param2 = "
                "%d, perc = %d where oper = '%s' and stepText = '%s' and tag = '%s' and tag_oper_cnt = '%s';",
                TRAFODION_SYSCAT_LIT, SEABASE_REPOS_SCHEMA, "BREI_PROGRESS_STATUS_TABLE", state, param2, perc, oper,
                stepText, backupTag, str_itoa(operCnt, str_cnt));

  cliInterface->executeImmediate("cqd query_cache '0'");
  cliRC = cliInterface->executeImmediate(psQuery);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());

    cliInterface->executeImmediate("cqd query_cache reset");

    *CmpCommon::diags() << DgSqlCode(-5050) << DgString0(oper)
                        << DgString1("Reason: Error occurred while updating progress status table.");

    return -1;
  }

  cliInterface->executeImmediate("cqd query_cache reset");

  return 0;
}

static short updateQueueWithOI(const char *v, Queue *q, NAHeap *h, int inLen = -1) {
  OutputInfo *oi = new (h) OutputInfo(1);
  int len = (inLen == -1 ? strlen(v) : inLen);
  char *brVal = new (h) char[len + 1];
  if (inLen == -1)
    strcpy(brVal, v);
  else
    str_cpy_and_null(brVal, v, len, '\0', ' ', TRUE);
  oi->insert(0, brVal);
  q->insert(oi);

  return 0;
}

short CmpSeabaseDDL::getProgressStatus(RelBackupRestore *brExpr, ExeCliInterface *cliInterface,
                                       CmpDDLwithStatusInfo *dws) {
  int cliRC = 0;
  char psQuery[2000];
  Queue *psQueue = NULL;

  // Multiple operations are not supported and only one may potentially
  // succeed.
  // If parallel operations are being updated into progress table,
  // show status for the latest operation.
  str_sprintf(psQuery,
              "select [first 1] trim(tag), tag_oper_cnt from %s.\"%s\".\"%s\" where oper = '%s' and stepSeq = 1 and "
              "steptext = 'Initialize' order by starttime desc ",
              TRAFODION_SYSCAT_LIT, SEABASE_REPOS_SCHEMA, "BREI_PROGRESS_STATUS_TABLE", brExpr->getBREIoper().data());

  cliRC = cliInterface->fetchRowsPrologue(psQuery);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());

    if (!CmpCommon::diags()->contains(-4082))  // ignore 'not exist' error
      return -1;

    CmpCommon::diags()->clear();
  }
  cliRC = cliInterface->fetch();
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    *CmpCommon::diags() << DgSqlCode(-5050)
                        << DgString1("Reason: Error occurred while fetch rows from progress status table.");

    return -1;
  }

  NAString tagStr = "";
  NAString cntStr = "";
  // did not find the row
  if (cliRC == 100) {
    tagStr = "";
    cntStr = "";
  } else {
    char *result = NULL;
    int result_len = 0;
    cliInterface->getPtrAndLen(1, result, result_len);
    tagStr = NAString(" and tag = '") + tagStr.append(result, result_len) + NAString("'");

    cliInterface->getPtrAndLen(2, result, result_len);
    cntStr = NAString(" and tag_oper_cnt = '") + cntStr.append(result, result_len) + NAString("'");
  }

  str_sprintf(psQuery,
              "select stepText, state, tag, cast(case when endtime > starttime then endtime - starttime else 0 end as "
              "largeint not null), param1, param2, perc, cast(converttimestamp(startTime, to local) as varchar(20)), "
              "cast(converttimestamp(endTime, to local) as varchar(20)), startTime, endTime from %s.\"%s\".\"%s\" "
              "where oper = '%s' %s %s order by stepSeq",
              TRAFODION_SYSCAT_LIT, SEABASE_REPOS_SCHEMA, "BREI_PROGRESS_STATUS_TABLE", brExpr->getBREIoper().data(),
              tagStr.data(), cntStr.data());

  cliRC = cliInterface->fetchAllRows(psQueue, psQuery, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());

    if (!CmpCommon::diags()->contains(-4082))  // ignore 'not exist' error
      return -1;

    CmpCommon::diags()->clear();
    psQueue = NULL;
  }

  Queue *returnQueue = NULL;
  cliInterface->initializeInfoList(returnQueue, TRUE);

  // operation progress does not exist in status table
  if ((!psQueue) || (psQueue->numEntries() == 0)) {
    int blackBoxLen = 0;
    char *blackBox = NULL;

    populateBlackBox(cliInterface, returnQueue, blackBoxLen, blackBox);

    dws->setBlackBoxLen(blackBoxLen);
    dws->setBlackBox(blackBox);

    dws->setStep(-1);
    dws->setSubstep(0);
    dws->setDone(TRUE);

    return 0;
  }

  // get tag from first entry
  OutputInfo *vi = NULL;
  NAString tag;
  char *ptr = NULL;
  int len = 0;
  if (psQueue->numEntries() > 0) {
    psQueue->position();
    vi = (OutputInfo *)psQueue->getCurr();

    ptr = NULL;
    len = 0;
    vi->get(2, ptr, len);

    tag = NAString(ptr, len);
    tag = tag.strip();
  }

  updateQueueWithOI(" ", returnQueue, STMTHEAP);
  NAString header = NAString(brExpr->getBREIoper() + " Progress Status for tag \"" + tag + "\"");
  updateQueueWithOI(header.data(), returnQueue, STMTHEAP);
  char dashBuf[200];
  str_pad(dashBuf, header.length(), '=');
  dashBuf[header.length()] = 0;
  updateQueueWithOI(dashBuf, returnQueue, STMTHEAP);
  updateQueueWithOI(" ", returnQueue, STMTHEAP);
  updateQueueWithOI(" ", returnQueue, STMTHEAP);

  char retBuf[300];
  str_sprintf(retBuf, "%-18s %-12s %-9s %-15s", "Step", "State", "ET", "Progress");
  updateQueueWithOI(retBuf, returnQueue, STMTHEAP);
  str_sprintf(retBuf, "%-18s %-12s %-9s %-15s", "======", "=======", "====", "==========");
  updateQueueWithOI(retBuf, returnQueue, STMTHEAP);
  updateQueueWithOI(" ", returnQueue, STMTHEAP);

  psQueue->position();
  NAString startTime;
  NAString endTime;
  long startTimeJTS = 0;
  long endTimeJTS = 0;
  long et = 0;
  int hh, mm, ss;
  for (int idx = 0; idx < psQueue->numEntries(); idx++) {
    vi = (OutputInfo *)psQueue->getCurr();

    vi->get(0, ptr, len);
    NAString step(ptr, len);
    step = step.strip(NAString::trailing);

    vi->get(1, ptr, len);
    NAString state(ptr, len);

    et = *(long *)vi->get(3) / 1000000;
    ss = et % 60;
    et = et / 60;
    mm = et % 60;
    et = et / 60;
    hh = et;
    char intBuf[100];
    str_sprintf(intBuf, "%02d:%02d:%02d", hh, mm, ss);

    int param1 = *(int *)vi->get(4);  // total num
    int param2 = *(int *)vi->get(5);  // curr num

    int perc = *(int *)vi->get(6);

    if (idx == 0) {
      vi->get(7, ptr, len);
      startTime = NAString(ptr, len);
      startTimeJTS = *(long *)vi->get(9);
    } else if ((idx == psQueue->numEntries() - 1) && (step == "Finalize") && (state == "Completed")) {
      vi->get(8, ptr, len);
      endTime = NAString(ptr, len);
      endTimeJTS = *(long *)vi->get(10);
    }

    char progressBuf[200];
    if (param1 > 0) {
      str_sprintf(progressBuf, "%d%%(%d/%d)", perc, param2, param1);
    } else if (perc >= 0) {
      str_sprintf(progressBuf, "%d%%", perc);
    } else
      strcpy(progressBuf, " ");

    str_sprintf(retBuf, "%-18s %-12s %-9s %-15s", step.data(), state.data(), intBuf /*et.data()*/, progressBuf);

    updateQueueWithOI(retBuf, returnQueue, STMTHEAP);

    psQueue->advance();
  }  // for

  if ((!startTime.isNull()) && (!endTime.isNull())) {
    updateQueueWithOI(" ", returnQueue, STMTHEAP);
    updateQueueWithOI(" ", returnQueue, STMTHEAP);

    header = NAString(brExpr->getBREIoper() + " End Status for tag \"" + tag + "\"");
    updateQueueWithOI(header.data(), returnQueue, STMTHEAP);
    str_pad(dashBuf, header.length(), '=');
    dashBuf[header.length()] = 0;
    updateQueueWithOI(dashBuf, returnQueue, STMTHEAP);
    updateQueueWithOI(" ", returnQueue, STMTHEAP);

    et = (endTimeJTS - startTimeJTS) / 1000000;
    ss = et % 60;
    et = et / 60;
    mm = et % 60;
    et = et / 60;
    hh = et;
    str_sprintf(retBuf, "StartTime: %s (local time)", startTime.data());
    updateQueueWithOI(retBuf, returnQueue, STMTHEAP);
    str_sprintf(retBuf, "EndTime:   %s (local time)", endTime.data());
    updateQueueWithOI(retBuf, returnQueue, STMTHEAP);
    str_sprintf(retBuf, "ET:        %02d:%02d:%02d (hh:mm:ss)", hh, mm, ss);
    updateQueueWithOI(retBuf, returnQueue, STMTHEAP);
  }

  updateQueueWithOI(" ", returnQueue, STMTHEAP);

  int blackBoxLen = 0;
  char *blackBox = NULL;

  populateBlackBox(cliInterface, returnQueue, blackBoxLen, blackBox);

  dws->setBlackBoxLen(blackBoxLen);
  dws->setBlackBox(blackBox);

  dws->setStep(-1);
  dws->setSubstep(0);
  dws->setDone(TRUE);

  return 0;
}

short CmpSeabaseDDL::truncateBackupMD(NAString &backupSch, ExeCliInterface *cliInterface) {
  int cliRC = 0;
  char query[2000];

  ExpHbaseInterface *ehi = allocEHI(COM_STORAGE_HBASE);
  if (ehi == NULL) return -1;

  // if Backup OBJECTS MD table is empty, no need to delete anything.
  NAString qTableName;
  qTableName = NAString(TRAF_RESERVED_NAMESPACE3) + ":" + NAString(TRAFODION_SYSCAT_LIT) + "." + backupSch + "." +
               NAString(SEABASE_OBJECTS);
  HbaseStr hbaseTable;
  hbaseTable.val = (char *)qTableName.data();
  hbaseTable.len = qTableName.length();
  if (ehi->isEmpty(hbaseTable)) {
    ehi->close();
    return 0;
  }

  for (int i = 0; i < sizeof(backupMDs) / sizeof(BackupMDsInfo); i++) {
    NAString lobChunksFullyQualObjName;
    NAString lobChunksObjName;

    if (backupMDs[i].hasLob) {
      if (genLobChunksTableName(cliInterface, backupMDs[i].catName, backupSch.data(), backupMDs[i].objName,
                                backupSch.data(), lobChunksFullyQualObjName, &lobChunksObjName) < 0)
        return -1;  // does not exist

      str_sprintf(query, "delete with no rollback from %s;", lobChunksFullyQualObjName.data());
    } else {
      str_sprintf(query, "delete with no rollback from %s.\"%s\".%s;", TRAFODION_SYSCAT_LIT, backupSch.data(),
                  backupMDs[i].objName);
    }

    cliRC = cliInterface->executeImmediate(query);

    // if delete fails, then truncate the table
    if (cliRC < 0) {
      qTableName = NAString(TRAF_RESERVED_NAMESPACE3) + ":" + NAString(backupMDs[i].catName) + "." + backupSch + "." +
                   NAString(backupMDs[i].hasLob ? lobChunksObjName.data() : backupMDs[i].objName);

      hbaseTable.val = (char *)qTableName.data();
      hbaseTable.len = qTableName.length();

      ehi->truncate(hbaseTable, TRUE, TRUE);
    }

#ifdef __ignore
    if (backupMDs[i].hasLob) {
      BindWA bindWA(ActiveSchemaDB(), CmpCommon::context(), FALSE);

      CorrName cn(backupMDs[i].objName, STMTHEAP, backupSch, backupMDs[i].catName);
      NATable *naTable = bindWA.getNATable(cn);
      if (naTable == NULL || bindWA.errStatus()) {
        ehi->close();
        return -1;
      }

    }  // has lob
#endif
  }

  ehi->close();

  return 0;
}

// operTag has the format:
//   <tag>("oper")
//     "oper" is BACKUP or RESTORE.
static short extractFromOperTag(char *operTag, NAString &oper, NAString &tag) {
  char *p = strchr(operTag, '(');
  if (!p) return -1;

  tag.clear();

  p += 1;
  if (memcmp(p, "BACKUP", strlen("BACKUP")) == 0) {
    oper = "BACKUP";
    tag.append(operTag, (p - operTag - 1));
  } else {
    oper = "RESTORE";
    tag.append(operTag, (p - operTag - 1));
  }

  return 0;
}

// return: 0, if not exists
//         1, if locked.
//         2, not locked
//        -1, if error.
short CmpSeabaseDDL::checkIfBackupMDisLocked(RelBackupRestore *brExpr, ExeCliInterface *cliInterface,
                                             const char *inOper, NAString &oper, NAString &tag) {
  int cliRC = 0;

  char query[1000];

  NAString backupSch;
  genBackupSchemaName(brExpr->backupTag(), backupSch);

  // check if backup schema exists in MD
  long objUID = getObjectUID(cliInterface, TRAFODION_SYSCAT_LIT, backupSch.data(), SEABASE_SCHEMA_OBJECTNAME,
                              COM_PRIVATE_SCHEMA_OBJECT_LIT, NULL, NULL, NULL, FALSE, FALSE);
  if (objUID < 0) return 0;  // does not exist

  str_sprintf(query, "select trim(text) from %s.\"%s\".%s where text_uid = %ld and text_type = 10",
              TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_TEXT, objUID);
  Queue *objsQueue = NULL;
  cliRC = cliInterface->fetchAllRows(objsQueue, query, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  if (objsQueue->numEntries() > 1) return -1;  // error, should only have max one entry

  if (objsQueue->numEntries() == 0) return 2;  // doesnt exist in TEXT, not locked

  objsQueue->position();
  OutputInfo *vi = (OutputInfo *)objsQueue->getNext();

  if (extractFromOperTag((char *)vi->get(0), oper, tag)) return -1;

  // backup MD is locked.
  if (strcmp(inOper, "BACKUP") == 0) {
    updateProgressTable(cliInterface, brExpr->backupTag().data(), inOper, "SetupMetadata", "Failed", -1, -1);
  }

  return 1;
}

short CmpSeabaseDDL::lockBackupMD(RelBackupRestore *brExpr, ExeCliInterface *cliInterface, const char *oper,
                                  NABoolean override) {
  int cliRC = 0;

  // check if backup schema exists in MD
  NAString backupSch;
  genBackupSchemaName(brExpr->backupTag(), backupSch);

  long objUID = getObjectUID(cliInterface, TRAFODION_SYSCAT_LIT, backupSch.data(), SEABASE_SCHEMA_OBJECTNAME,
                              COM_PRIVATE_SCHEMA_OBJECT_LIT, NULL, NULL, NULL, FALSE, FALSE);
  if (objUID < 0) return 0;  // nothing to lock, object doesnt exist

  NABoolean xnWasStartedHere = FALSE;
  if (beginXnIfNotInProgress(cliInterface, xnWasStartedHere)) return -1;

  NAString operTag(brExpr->backupTag());
  operTag += NAString("(") + oper + ")";

  char query[2000];
  str_sprintf(query, "%s into %s.\"%s\".%s values (%ld, %d, 0, 0, 0, '%s')", (override ? "upsert" : "insert"),
              TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_TEXT, objUID, COM_LOCKED_OBJECT_BR_TAG, operTag.data());
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_return;
  }

label_return:
  if (endXnIfStartedHere(cliInterface, xnWasStartedHere, (cliRC < 0 ? -1 : 0))) return -1;

  return (cliRC < 0 ? -1 : 0);
}

short CmpSeabaseDDL::unlockBackupMD(RelBackupRestore *brExpr, ExeCliInterface *cliInterface, const char *oper) {
  int cliRC = 0;

  // check if backup schema exists in MD
  NAString backupSch;
  genBackupSchemaName(brExpr->backupTag(), backupSch);

  long objUID = getObjectUID(cliInterface, TRAFODION_SYSCAT_LIT, backupSch.data(), SEABASE_SCHEMA_OBJECTNAME,
                              COM_PRIVATE_SCHEMA_OBJECT_LIT, NULL, NULL, NULL, FALSE, FALSE);
  if (objUID < 0) return 0;  // nothing to unlock, object doesnt exist

  NABoolean xnWasStartedHere = FALSE;
  if (beginXnIfNotInProgress(cliInterface, xnWasStartedHere)) return -1;

  char query[2000];
  str_sprintf(query, "delete from %s.\"%s\".%s where text_uid = %ld and text_type = %d", TRAFODION_SYSCAT_LIT,
              SEABASE_MD_SCHEMA, SEABASE_TEXT, objUID, COM_LOCKED_OBJECT_BR_TAG);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_return;
  }

label_return:
  if (endXnIfStartedHere(cliInterface, xnWasStartedHere, (cliRC < 0 ? -1 : 0))) return -1;

  return (cliRC < 0 ? -1 : 0);
}

static short cleanupBackupSchema(ExeCliInterface *cliInterface, NAString &backupSch) {
  int cliRC = 0;

  char query[1000];
  str_sprintf(query, "drop schema if exists %s.\"%s\" cascade;", TRAFODION_SYSCAT_LIT, backupSch.data());
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    str_sprintf(query, "cleanup schema %s.\"%s\";", TRAFODION_SYSCAT_LIT, backupSch.data());
    cliRC = cliInterface->executeImmediate(query);
    if (cliRC < 0) {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      return -1;
    }
  }

  // while current backup tag is import from other cluster and had been restored,
  // we need to remove the residual PRIMARY_KEY_CONSTRAINTS in metadata.
  str_sprintf(query,
              "delete from %s.\"%s\".%s where catalog_name = '%s' "
              "and schema_name = '%s' and object_type = 'PK' "
              "and object_uid not in (select table_uid from %s.\"%s\".%s);",
              TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_OBJECTS, TRAFODION_SYSCAT_LIT, backupSch.data(),
              TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_TABLES);

  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }
  return 0;
}

short CmpSeabaseDDL::createBackupMDschema(RelBackupRestore *brExpr, ExeCliInterface *cliInterface, NAString &backupSch,
                                          const char *oper) {
  short retcode = 0;
  int cliRC = 0;

  char query[1000];

  // create a temp schema of format:  _BACKUP_<backuptag>_
  genBackupSchemaName(brExpr->backupTag(), backupSch);

  // check if backup schema exists in MD
  ComObjectName objName(TRAFODION_SYSCAT_LIT, backupSch, NAString(SEABASE_SCHEMA_OBJECTNAME), COM_TABLE_NAME, TRUE);
  NAString objectNamePart = objName.getObjectNamePartAsAnsiString(TRUE);
  retcode =
      existsInSeabaseMDTable(cliInterface, TRAFODION_SYSCAT_LIT, backupSch, objectNamePart, COM_UNKNOWN_OBJECT, FALSE);
  if (retcode < 0) return -1;

  NABoolean backupSchExists = (retcode == 1);

  // cleanup existing schema
  if (backupSchExists && brExpr->dropBackupMD()) {
    if (cleanupBackupSchema(cliInterface, backupSch)) return -1;

    backupSchExists = FALSE;
  }  // drop existing schema

  cliRC = cliInterface->holdAndSetCQD("TRAF_LOB_HBASE_DATA_MAXLEN_DDL", "-1");

  int numTables = sizeof(backupMDs) / sizeof(BackupMDsInfo);
  if (insertProgressTable(cliInterface, brExpr, oper, (NOT backupSchExists ? "CreateMetadata" : "TruncateMetadata"),
                          "Initiated", numTables, 0) < 0) {
    retcode = -1;
    goto label_return;
  }

  if (NOT backupSchExists) {
    // If hist tables do not exist in default sys schema(TRAFODION.SEABASE),
    // then create them now.
    NAString tableNotCreated;
    if (createHistogramTables(cliInterface, NAString(TRAFODION_SYSCAT_LIT) + "." + SEABASE_SYSTEM_SCHEMA, TRUE,
                              tableNotCreated)) {
      // diags aready populated.

      retcode = -1;
      goto label_return;
    }

    str_sprintf(query, "create schema if not exists %s.\"%s\" namespace '%s';", TRAFODION_SYSCAT_LIT, backupSch.data(),
                TRAF_RESERVED_NAMESPACE3);
    cliRC = cliInterface->executeImmediate(query);
    if (cliRC < 0) {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      retcode = -1;
      goto label_return;
    }

    for (int i = 0; i < numTables; i++) {
      str_sprintf(query, "create table if not exists %s.\"%s\".%s like %s.\"%s\".%s without namespace;",
                  TRAFODION_SYSCAT_LIT, backupSch.data(), backupMDs[i].objName, backupMDs[i].catName,
                  backupMDs[i].schName, backupMDs[i].objName);

      cliRC = cliInterface->executeImmediate(query);
      if (cliRC < 0) {
        cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());

        retcode = -1;
        goto label_return;
      }

      int perc = (i * 100) / numTables;
      if (updateProgressTable(cliInterface, brExpr->backupTag().data(), oper, "CreateMetadata", "Initiated", i, perc) <
          0) {
        retcode = -1;
        goto label_return;
      }
    }     // for
  } else  // backup sch already exists. Empty it before proceeding
  {
    if (truncateBackupMD(backupSch, cliInterface)) {
      retcode = -1;
      goto label_return;
    }

    retcode = 1;  // backup schema already exists
  }

  if (updateProgressTable(cliInterface, brExpr->backupTag().data(), oper,
                          (NOT backupSchExists ? "CreateMetadata" : "TruncateMetadata"), "Completed", numTables,
                          100) < 0) {
    retcode = -1;
    goto label_return;
  }

label_return:
  cliInterface->restoreCQD("TRAF_LOB_VERSION2");
  cliInterface->restoreCQD("TRAF_LOB_HBASE_DATA_MAXLEN_DDL");

  return retcode;
}

short CmpSeabaseDDL::createBackupTags(RelBackupRestore *brExpr, ExeCliInterface *cliInterface) {
  short retcode = 0;

  if (createProgressTable(cliInterface)) return -1;

  if (brExpr->tagsList() && brExpr->tagsList()->entries() > 0)  // create user specified tags
  {
    for (int i = 0; i < brExpr->tagsList()->entries(); i++) {
      const NAString *cs = (*brExpr->tagsList())[i];

      brExpr->setBackupTag(*cs);

      NAString backupSch;
      retcode = createBackupMDschema(brExpr, cliInterface, backupSch, "BACKUP");
      if (retcode < 0) return -1;
    }
  } else if (brExpr->numSysTags() > 0)  // create system tags
  {
    // system tag format:  TRAF_SYSTAG_<unique-id>
    for (int i = 0; i < brExpr->numSysTags(); i++) {
      ComUID comUID;
      comUID.make_UID();
      long systag = comUID.get_value();

      char buf[100];
      str_sprintf(buf, "TRAF_SYSTAG_%ld", systag);

      brExpr->setBackupTag(NAString(buf));

      NAString backupSch;
      retcode = createBackupMDschema(brExpr, cliInterface, backupSch, "BACKUP");
      if (retcode < 0) return -1;
    }
  }

  return 0;
}

// generate location of hdfs datafiles used for LOB columns in table objUID
// and return in lobLocList.
// Returned values have 2 formats:
//  pivoted:   all filenames are comma separated in one entry
//  unpivoted: each filename is returned as one entry.
short CmpSeabaseDDL::genLobLocation(long objUID, const char *schName, ExeCliInterface *cliInterface,
                                    TextVec &lobLocList, int &numLOBfiles, NABoolean forDisplay) {
  int cliRC = 0;

  numLOBfiles = 0;

  // if objUID is 0, return dummy zero length string
  if (objUID == 0) {
    lobLocList.push_back("");
    return 0;
  }

  char query[1000];

  char oldLobMDName[100];
  str_sprintf(oldLobMDName, "LOBMD__%020ld", objUID);
  int retcode = existsInSeabaseMDTable(cliInterface, TRAFODION_SYSCAT_LIT, schName, oldLobMDName,
                                         COM_BASE_TABLE_OBJECT, TRUE, TRUE);
  if (retcode == 1)  // exists. old lob name
  {
    if (forDisplay)  // get all lob files
      sprintf(query,
              "select location || '/LOBP_%020ld_' || cast(repeat('0', 4 - char_length(lobnum), 10) as varchar(4) "
              "character set iso88591 not null) || lobnum from %s.\"%s\".LOBMD__%020ld",
              objUID, TRAFODION_SYSCAT_LIT, schName, objUID);
    else  // concatenate and get all distinct locations
      sprintf(query, "select pivot_group(a) from (select distinct location from %s.\"%s\".LOBMD__%020ld) x(a)",
              TRAFODION_SYSCAT_LIT, schName, objUID);
  } else {
    // V2 lob
    if (forDisplay)  // get all lob files
      sprintf(
          query,
          "select location || '/LOBP_%020ld_' || cast(repeat('0', 4 - char_length(lobnum), 10) as varchar(4) character "
          "set iso88591 not null) || lobnum from %s.\"%s\".%s where table_uid = %ld and num_lob_files > 0",
          objUID, TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_LOB_COLUMNS, objUID);
    else  // concatenate and get all distinct locations
      sprintf(query,
              "select pivot_group(a) from (select distinct location from %s.\"%s\".%s where table_uid = %ld and "
              "num_lob_files > 0) x(a) having pivot_group(a) is not null",
              TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_LOB_COLUMNS, objUID);
  }

  Queue *lobLocsQueue = NULL;
  cliRC = cliInterface->fetchAllRows(lobLocsQueue, query, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  lobLocsQueue->position();
  for (int idx = 0; idx < lobLocsQueue->numEntries(); idx++) {
    OutputInfo *vi = (OutputInfo *)lobLocsQueue->getCurr();

    char *ptr = NULL;
    int len = 0;
    vi->get(0, ptr, len);
    NAString loc(ptr, len);
    if (forDisplay) loc.prepend("  ");
    lobLocList.push_back(loc.data());

    lobLocsQueue->advance();
  }

  numLOBfiles = lobLocsQueue->numEntries();

  if (numLOBfiles == 0) lobLocList.push_back("");

  return 0;
}

short CmpSeabaseDDL::genLobObjs(ElemDDLList *tableList, char *srcSch, ExeCliInterface *cliInterface, Queue *&lobsQueue,
                                NAString &lobObjs) {
  int cliRC = 0;

  if ((!tableList) || (tableList->entries() == 0)) return 0;

  if (!cliInterface) return 0;

  NAString lobsQuery;
  lobsQuery =
      " select trim(O2.schema_name) || '.' || trim(O2.object_name) "
      " from ";
  lobsQuery += NAString("" TRAFODION_SYSTEM_CATALOG ".");
  lobsQuery += NAString("\"") + srcSch + "\"";
  lobsQuery += "." SEABASE_OBJECTS " O, ";
  lobsQuery += NAString("" TRAFODION_SYSTEM_CATALOG ".");
  lobsQuery += NAString("\"") + srcSch + "\"";
  lobsQuery += "." SEABASE_OBJECTS " o2 ";
  lobsQuery +=
      " where  (O2.catalog_name, O2.schema_name) = (O.catalog_name, O.schema_name) "
      " and (O2.object_name like 'LOBDescChunks|_|_' || lpad(cast(O.object_uid as varchar(20)),20,'0') || '|_' || '%%' "
      "escape '|' or "
      " O2.object_name like 'LOBDescHandle|_|_' || lpad(cast(O.object_uid as varchar(20)),20,'0') || '|_' || '%%' "
      "escape '|' or "
      " O2.object_name like 'LOBCHUNKS|_|_' || lpad(cast(O.object_uid as varchar(20)),20,'0') || '%%' escape '|' or "
      " O2.object_name like 'LOBMD|_|_' || lpad(cast(O.object_uid as varchar(20)),20,'0') || '%%' escape '|' ) ";

  lobsQuery += "and (trim(O.catalog_name) || '.' || trim(O.schema_name) || '.' ||  trim(O.object_name) in (";
  for (CollIndex i = 0; i < tableList->entries(); i++) {
    ElemDDLQualName *dn = ((*tableList)[i])->castToElemDDLQualName();

    lobsQuery += "'";
    lobsQuery += dn->getQualifiedName().getCatalogName();
    lobsQuery += ".";
    lobsQuery += dn->getQualifiedName().getSchemaName();
    lobsQuery += ".";
    lobsQuery += dn->getQualifiedName().getObjectName();

    lobsQuery += "' ";

    if (i < tableList->entries() - 1) lobsQuery += ", ";
  }
  lobsQuery += ")";
  lobsQuery += ")";
  lobsQueue = NULL;
  cliRC = cliInterface->fetchAllRows(lobsQueue, lobsQuery, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  if (lobsQueue->numEntries() == 0) return 0;

  for (int idx = 0; idx < lobsQueue->numEntries(); idx++) {
    OutputInfo *vi = (OutputInfo *)lobsQueue->getNext();

    lobObjs += "'";
    lobObjs += (char *)vi->get(0);
    lobObjs += "'";
    if (idx < lobsQueue->numEntries() - 1) lobObjs += ", ";
  }

  return 0;
}

short CmpSeabaseDDL::genIndexObjs(ElemDDLList *tableList, char *srcSch, ExeCliInterface *cliInterface,
                                  Queue *&indexesQueue, NAString &indexesObjs) {
  int cliRC = 0;

  if ((!tableList) || (tableList->entries() == 0)) return 0;

  if (!cliInterface) return 0;

  NAString indexesQuery;
  indexesQuery +=
      " select trim(schema_name) || '.' || trim(object_name) " + NAString(" from " TRAFODION_SYSTEM_CATALOG ".") +
      "\"" + srcSch + "\"" + "." SEABASE_OBJECTS " where object_uid in " +
      " (select index_uid from " TRAFODION_SYSTEM_CATALOG "." + "\"" + srcSch + "\"" +
      "." SEABASE_INDEXES " where base_table_uid in " + " (select object_uid from " TRAFODION_SYSTEM_CATALOG "." +
      "\"" + srcSch + "\"" + "." SEABASE_OBJECTS " where trim(catalog_name) || '.' || trim(schema_name) || " +
      "'.' || trim(object_name) in ";

  indexesQuery += "(";
  for (CollIndex i = 0; i < tableList->entries(); i++) {
    ElemDDLQualName *dn = ((*tableList)[i])->castToElemDDLQualName();

    indexesQuery += "'";
    indexesQuery += dn->getQualifiedName().getCatalogName();
    indexesQuery += ".";
    indexesQuery += dn->getQualifiedName().getSchemaName();
    indexesQuery += ".";
    indexesQuery += dn->getQualifiedName().getObjectName();

    indexesQuery += "' ";

    if (i < tableList->entries() - 1) indexesQuery += ", ";
  }
  indexesQuery += ")";
  indexesQuery += "))";

  indexesQueue = NULL;
  cliRC = cliInterface->fetchAllRows(indexesQueue, indexesQuery, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  if (indexesQueue->numEntries() == 0) return 0;

  for (int idx = 0; idx < indexesQueue->numEntries(); idx++) {
    OutputInfo *vi = (OutputInfo *)indexesQueue->getNext();

    indexesObjs += "'";
    indexesObjs += (char *)vi->get(0);
    indexesObjs += "'";
    if (idx < indexesQueue->numEntries() - 1) indexesObjs += ", ";
  }

  return 0;
}

short CmpSeabaseDDL::genReferencedObjs(ElemDDLList *tableList, char *srcSch, ExeCliInterface *cliInterface,
                                       Queue *&refsQueue, NAString &refsObjs) {
  int cliRC = 0;

  if ((!tableList) || (tableList->entries() == 0)) return 0;

  if (!cliInterface) return 0;

  NAString refsQuery;
  refsQuery += " select trim(schema_name) || '.' || trim(object_name) " +
               NAString(" from " TRAFODION_SYSTEM_CATALOG ".") + "\"" + srcSch + "\"" +
               "." SEABASE_OBJECTS " where object_uid in " + " (select table_uid from " TRAFODION_SYSTEM_CATALOG "." +
               "\"" + srcSch + "\"" + "." SEABASE_TABLE_CONSTRAINTS " where constraint_uid in " +
               " (select unique_constraint_uid from " TRAFODION_SYSTEM_CATALOG "." + "\"" + srcSch + "\"" +
               "." SEABASE_REF_CONSTRAINTS " where ref_constraint_uid in " +
               " (select constraint_uid from " TRAFODION_SYSTEM_CATALOG "." + "\"" + srcSch + "\"" +
               "." SEABASE_TABLE_CONSTRAINTS " where constraint_type = 'F' and table_uid in " +
               " (select object_uid from " TRAFODION_SYSTEM_CATALOG "." + "\"" + srcSch + "\"" +
               "." SEABASE_OBJECTS " where trim(catalog_name) || '.' || trim(schema_name) || " +
               "'.' || trim(object_name) in ";

  refsQuery += "(";
  for (CollIndex i = 0; i < tableList->entries(); i++) {
    ElemDDLQualName *dn = ((*tableList)[i])->castToElemDDLQualName();

    refsQuery += "'";
    refsQuery += dn->getQualifiedName().getCatalogName();
    refsQuery += ".";
    refsQuery += dn->getQualifiedName().getSchemaName();
    refsQuery += ".";
    refsQuery += dn->getQualifiedName().getObjectName();

    refsQuery += "' ";

    if (i < tableList->entries() - 1) refsQuery += ", ";
  }
  refsQuery += ")";
  refsQuery += "))))";

  refsQueue = NULL;
  cliRC = cliInterface->fetchAllRows(refsQueue, refsQuery, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  if (refsQueue->numEntries() == 0) return 0;

  for (int idx = 0; idx < refsQueue->numEntries(); idx++) {
    OutputInfo *vi = (OutputInfo *)refsQueue->getNext();

    refsObjs += "'";
    refsObjs += (char *)vi->get(0);
    refsObjs += "'";
    if (idx < refsQueue->numEntries() - 1) refsObjs += ", ";
  }

  return 0;
}

short CmpSeabaseDDL::genLibsUsedProcs(NAString libsToBackup, NAString &libsUsedProcs, ExeCliInterface *cliInterface) {
  int cliRC = 0;

  libsToBackup = libsToBackup.strip(NAString::trailing, ',');

  char query[2000 + libsToBackup.length()];

  str_sprintf(query,
              "select schema_name || '.' || object_name from %s.\"%s\".%s where object_uid in (select used_udr_uid "
              "from %s.\"%s\".%s where using_library_uid in (select object_uid from %s.\"%s\".%s where (schema_name || "
              "'.' || object_name in (%s))))",
              TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_OBJECTS, TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA,
              SEABASE_LIBRARIES_USAGE, TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_OBJECTS, libsToBackup.data());

  Queue *procsQueue = NULL;
  cliRC = cliInterface->fetchAllRows(procsQueue, query, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  if (procsQueue->numEntries() == 0) return 0;

  for (int idx = 0; idx < procsQueue->numEntries(); idx++) {
    OutputInfo *vi = (OutputInfo *)procsQueue->getNext();

    libsUsedProcs += "'";
    libsUsedProcs += (char *)vi->get(0);
    libsUsedProcs += "'";

    if (idx < procsQueue->entries() - 1) libsUsedProcs += ", ";
  }

  return 0;
}

short CmpSeabaseDDL::genProcsUsedLibs(NAString procsToBackup, NAString &procsUsedLibs, ExeCliInterface *cliInterface) {
  int cliRC = 0;

  procsToBackup = procsToBackup.strip(NAString::trailing, ',');

  char query[2000 + procsToBackup.length()];

  str_sprintf(query,
              "select schema_name || '.' || object_name from %s.\"%s\".%s where object_uid in (select library_uid from "
              "%s.\"%s\".%s where udr_uid in (select object_uid from %s.\"%s\".%s where (schema_name || '.' || "
              "object_name in (%s))))",
              TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_OBJECTS, TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA,
              SEABASE_ROUTINES, TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_OBJECTS, procsToBackup.data());

  Queue *libsQueue = NULL;
  cliRC = cliInterface->fetchAllRows(libsQueue, query, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  if (libsQueue->numEntries() == 0) return 0;

  for (int idx = 0; idx < libsQueue->numEntries(); idx++) {
    OutputInfo *vi = (OutputInfo *)libsQueue->getNext();

    procsUsedLibs += "'";
    procsUsedLibs += (char *)vi->get(0);
    procsUsedLibs += "'";

    if (idx < libsQueue->entries() - 1) procsUsedLibs += ", ";
  }

  return 0;
}

short CmpSeabaseDDL::genListOfObjs(ElemDDLList *objList, NAString &objsToBackup, NABoolean &prependOR,
                                   NAString *schPrivsList) {
  if (objList && (objList->entries() > 0)) {
    for (CollIndex i = 0; i < objList->entries(); i++) {
      ElemDDLQualName *dn = ((*objList)[i])->castToElemDDLQualName();

      NAString tableName = "";
      if (NOT dn->getQualifiedName().getCatalogName().isNull())
        tableName += dn->getQualifiedName().getCatalogName() + ".";
      if (NOT dn->getQualifiedName().getSchemaName().isNull())
        tableName += dn->getQualifiedName().getSchemaName() + ".";
      tableName += dn->getQualifiedName().getObjectName();
      if (isHBaseNativeTable(tableName)) continue;

      objsToBackup += "'" + dn->getQualifiedName().getSchemaName() + ".";
      objsToBackup += dn->getQualifiedName().getObjectName();
      objsToBackup += "' ";

      objsToBackup += ",";

      if (schPrivsList) {
        ComObjectName objName(dn->getQualifiedName().getCatalogName(),
                              dn->getQualifiedName().getUnqualifiedSchemaNameAsAnsiString(),
                              dn->getQualifiedName().getUnqualifiedObjectNameAsAnsiString());

        *schPrivsList += "'TRAFODION.";
        *schPrivsList += objName.getSchemaNamePartAsAnsiString(TRUE);
        *schPrivsList += "'";
        if (i < objList->entries() - 1) *schPrivsList += ", ";
      }
    }
  }

  return 0;
}

short CmpSeabaseDDL::genBRObjUIDsWherePred(RelBackupRestore *brExpr, ElemDDLList *schemaList, ElemDDLList *tableList,
                                           ElemDDLList *viewList, ElemDDLList *procList, ElemDDLList *libList,
                                           NAString &currCatName, NAString &currSchName, NAString &objUIDsWherePred,
                                           char *srcSch, ExeCliInterface *cliInterface, Queue *&lobsQueue,
                                           Queue *&indexesQueue, Queue *&refsQueue, NAString *schPrivsList,

                                           // return TRUE, if full backup or schemas only
                                           // backup is being done.
                                           NABoolean &schemasOnlyBackup) {
  objUIDsWherePred.clear();

  schemasOnlyBackup = FALSE;
  NABoolean prependOR = FALSE;
  // generate list of specified schemas
  NAString schsToBackup;
  if (schemaList && (schemaList->entries() > 0)) {
    schsToBackup = " O.schema_name in ( ";
    for (CollIndex i = 0; i < schemaList->entries(); i++) {
      ElemDDLQualName *dn = ((*schemaList)[i])->castToElemDDLQualName();

      schsToBackup += "'";
      schsToBackup += dn->getQualifiedName().getObjectName();
      schsToBackup += "' ";

      if (i < schemaList->entries() - 1) schsToBackup += ", ";
    }

    schsToBackup += " ) ";
    prependOR = TRUE;
  }

  // generate list of specified tables
  NAString tablesToBackup;
  if (genListOfObjs(tableList, tablesToBackup, prependOR, schPrivsList)) return -1;

  // generate list of specified views
  NAString viewsToBackup;
  if (genListOfObjs(viewList, viewsToBackup, prependOR, schPrivsList)) return -1;

  // generate list of specified procs
  NAString procsToBackup;
  if (genListOfObjs(procList, procsToBackup, prependOR, schPrivsList)) return -1;

  NAString procsUsedLibs;
  if (!procsToBackup.isNull()) {
    // add libraries that depend on the procedures being backed up
    if (genProcsUsedLibs(procsToBackup, procsUsedLibs, cliInterface)) return -1;
  }

  // generate list of specified libs
  NAString libsToBackup;
  if (genListOfObjs(libList, libsToBackup, prependOR, schPrivsList)) return -1;

  NAString libsUsedProcs;
  if (!libsToBackup.isNull()) {
    // add procedures/functions that depend on the libraries being backed up
    if (genLibsUsedProcs(libsToBackup, libsUsedProcs, cliInterface)) return -1;
  }

  NAString objsToBackup;
  if (!tablesToBackup.isNull() || !viewsToBackup.isNull() || !procsToBackup.isNull() || !libsToBackup.isNull()) {
    if (prependOR) objsToBackup = " or ";
    objsToBackup += " ((trim(O.schema_name) || '.' || trim(O.object_name)) in ( ";
    objsToBackup += tablesToBackup;
    objsToBackup += viewsToBackup;
    objsToBackup += procsToBackup;
    objsToBackup += libsToBackup;
    objsToBackup += libsUsedProcs;
    objsToBackup += procsUsedLibs;

    // remove trailing ','
    objsToBackup = objsToBackup.strip(NAString::trailing, ',');
    objsToBackup += " )) ";
  }

  // If tablesToBackup list is specified, add LOB objects corresponding
  // to lob columns in those tables, and add all dependent indexes for
  // the specified tables.
  lobsQueue = NULL;
  NAString lobObjs;
  indexesQueue = NULL;
  NAString indexesObjs;
  refsQueue = NULL;
  NAString refsObjs;
  if (NOT tablesToBackup.isNull()) {
    if (genLobObjs(tableList, srcSch, cliInterface, lobsQueue, lobObjs)) return -1;

    if (NOT lobObjs.isNull()) {
      lobObjs.prepend(" or ( (trim(O.schema_name) || '.' || trim(O.object_name)) in ( ");
      lobObjs += ")";
      lobObjs += ")";
    }

    if (genIndexObjs(tableList, srcSch, cliInterface, indexesQueue, indexesObjs)) return -1;

    if (NOT indexesObjs.isNull()) {
      indexesObjs.prepend(" or ( (trim(O.schema_name) || '.' || trim(O.object_name)) in ( ");
      indexesObjs += ")";
      indexesObjs += ")";
    }

    if (genReferencedObjs(tableList, srcSch, cliInterface, refsQueue, refsObjs)) return -1;

    if (NOT refsObjs.isNull()) {
      refsObjs.prepend(" or ( (trim(O.schema_name) || '.' || trim(O.object_name)) in ( ");
      refsObjs += ")";
      refsObjs += ")";
    }
  }  // tablesToBackup

  char wherePred[2000 + schsToBackup.length() + objsToBackup.length() + lobObjs.length() + indexesObjs.length() +
                 refsObjs.length()];
  if (schsToBackup.isNull() && objsToBackup.isNull()) {
    NAString wherep =
        " ( O.catalog_name = '%s' "
        " and O.schema_name not like 'VOLATILE_SCHEMA_%%' "
        " and O.schema_name not like '|_HV|_%%|_' escape '|' "
        " and O.schema_name not like '|_HB|_MAP|_' escape '|' "
        " and O.schema_name not like '|_BACKUP|_%%' escape '|' "
        " and O.schema_name not like '|_%%|_' escape '|' "
        " and O.object_name not like 'TRAF_SAMPLE%%' ) ";

    str_sprintf(wherePred, wherep, TRAFODION_SYSCAT_LIT);
  } else {
    str_sprintf(wherePred,
                " ( O.catalog_name = '%s' and (%s %s %s %s %s ) and O.object_name not like 'TRAF_SAMPLE%%' ) ",
                TRAFODION_SYSCAT_LIT, (schsToBackup.isNull() ? " " : schsToBackup.data()),
                (objsToBackup.isNull() ? " " : objsToBackup.data()), (lobObjs.isNull() ? " " : lobObjs.data()),
                (indexesObjs.isNull() ? " " : indexesObjs.data()), (refsObjs.isNull() ? " " : refsObjs.data()));
  }

  objUIDsWherePred = wherePred;

  if (((!tableList) || (tableList->entries() == 0)) && ((!viewList) || (viewList->entries() == 0)) &&
      ((!procList) || (procList->entries() == 0)) && ((!libList) || (libList->entries() == 0))) {
    // full backup or schemas only backup
    schemasOnlyBackup = TRUE;
  }

  return 0;
}

short CmpSeabaseDDL::cleanupTgtObjectsFromMD(ExeCliInterface *cliInterface, char *objUIDsWherePred, char *srcSch) {
  int cliRC = 0;

  Queue *objsQueue = NULL;

  char query[1000 + strlen(objUIDsWherePred)];
  str_sprintf(query, " select catalog_name, schema_name, object_name, object_type from %s.\"%s\".%s O where %s ",
              TRAFODION_SYSCAT_LIT, srcSch, SEABASE_OBJECTS, objUIDsWherePred);
  cliRC = cliInterface->fetchAllRows(objsQueue, query, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  for (int idx = 0; idx < objsQueue->numEntries(); idx++) {
    OutputInfo *vi = (OutputInfo *)objsQueue->getNext();

    NAString catNamePart((char *)vi->get(0));
    NAString schNamePart((char *)vi->get(1));
    NAString objNamePart((char *)vi->get(2));
    NAString objType((char *)vi->get(3));

    if (objType == COM_BASE_TABLE_OBJECT_LIT)
      str_sprintf(
          query, "cleanup %s %s.\"%s\".\"%s\", no hbase drop",
          (objType == COM_BASE_TABLE_OBJECT_LIT ? "table" : (objType == COM_INDEX_OBJECT_LIT ? "index" : "object")),
          catNamePart.data(), schNamePart.data(), objNamePart.data());
    cliRC = cliInterface->executeImmediate(query);
    if (cliRC < 0) {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      return -1;
    }
  }

  return 0;
}

short CmpSeabaseDDL::invalidateTgtObjects(ExeCliInterface *cliInterface, char *objUIDsWherePred, char *tgtSch,
                                          char *srcSch, SharedCacheDDLInfoList &sharedCacheList) {
  int cliRC = 0;

  Queue *objsQueue = NULL;

  char objsQry[1000 + strlen(objUIDsWherePred)];
  str_sprintf(objsQry,
              " select O.catalog_name, O.schema_name, O.object_name, O.object_type, O.flags"
              " from %s.\"%s\".%s O join %s.\"%s\".%s B"
              " on O.catalog_name = B.catalog_name and O.schema_name = B.schema_name"
              " and O.object_name = B.object_name where %s"
              " and B.object_type in ('" COM_BASE_TABLE_OBJECT_LIT "', '" COM_INDEX_OBJECT_LIT "');",
              TRAFODION_SYSCAT_LIT, tgtSch, SEABASE_OBJECTS, TRAFODION_SYSCAT_LIT, srcSch, SEABASE_OBJECTS,
              objUIDsWherePred);
  cliRC = cliInterface->fetchAllRows(objsQueue, objsQry, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  for (int idx = 0; idx < objsQueue->numEntries(); idx++) {
    OutputInfo *vi = (OutputInfo *)objsQueue->getNext();

    NAString catName((char *)vi->get(0));
    NAString schName((char *)vi->get(1));
    NAString objName((char *)vi->get(2));
    NAString objType((char *)vi->get(3));
    long objectFlags = *(long *)vi->get(4);

    // If shared cache enabled, remove from cache
    if ((CmpCommon::getDefault(TRAF_ENABLE_METADATA_LOAD_IN_SHARED_CACHE) == DF_ON) &&
        (isMDflagsSet(objectFlags, MD_OBJECTS_STORED_DESC))) {
      // For now, skip indexes - will be added later
      if (objType == COM_INDEX_OBJECT_LIT) continue;
      NAString cachedObjName = catName + NAString(".\"") + schName + NAString("\"");
      if (objType == COM_INDEX_OBJECT_LIT || objType == COM_BASE_TABLE_OBJECT_LIT)
        cachedObjName += NAString(".\"") + objName + '"';

      SharedCacheDDLInfo newDDLObj(
          cachedObjName,
          (ComObjectType)(
              (strcmp(objType, COM_BASE_TABLE_OBJECT_LIT) == 0)
                  ? COM_BASE_TABLE_OBJECT
                  : ((strcmp(objType, COM_INDEX_OBJECT_LIT) == 0) ? COM_INDEX_OBJECT : COM_PRIVATE_SCHEMA_OBJECT)),
          SharedCacheDDLInfo::DDL_DELETE);
      sharedCacheList.insertEntry(newDDLObj);
    }

    CorrName cn(objName, STMTHEAP, schName, catName);
    ActiveSchemaDB()->getNATableDB()->removeNATable(
        cn, ComQiScope::REMOVE_FROM_ALL_USERS,
        (ComObjectType)(
            (strcmp(objType, COM_BASE_TABLE_OBJECT_LIT) == 0)
                ? COM_BASE_TABLE_OBJECT
                : ((strcmp(objType, COM_INDEX_OBJECT_LIT) == 0) ? COM_INDEX_OBJECT : COM_BASE_TABLE_OBJECT)),
        FALSE, FALSE, 0, TRUE /* noCheck */);

  }  // for

  return 0;
}

short CmpSeabaseDDL::deleteTgtObjectsFromMD(RelBackupRestore *brExpr, ExeCliInterface *cliInterface,
                                            NAString &objUIDsWherePred, char *tgtSch, char *tgtSch2, char *srcSch) {
  int cliRC;

  char objUIDsQry[1000 + objUIDsWherePred.length()];
  str_sprintf(objUIDsQry, " ( select object_uid from %s.\"%s\".%s O where %s ) ", TRAFODION_SYSCAT_LIT, srcSch,
              SEABASE_OBJECTS, objUIDsWherePred.data());

  char query[4000 + sizeof(objUIDsQry)];

  cliRC = cliInterface->holdAndSetCQDs(
      "NESTED_JOINS, MERGE_JOINS, HASH_JOINS, TRAF_BR_LOCKED_OBJECTS_QUERY_TYPE",
      "NESTED_JOINS 'OFF', MERGE_JOINS 'OFF', HASH_JOINS 'ON', TRAF_BR_LOCKED_OBJECTS_QUERY_TYPE '1' ");
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  str_sprintf(query, "delete from %s.\"%s\".%s where object_uid in %s", TRAFODION_SYSCAT_LIT, tgtSch, SEABASE_OBJECTS,
              objUIDsQry);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_return;
  }

  str_sprintf(query, "delete from %s.\"%s\".%s where object_uid in %s", TRAFODION_SYSCAT_LIT, tgtSch, SEABASE_COLUMNS,
              objUIDsQry);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_return;
  }

  str_sprintf(query, "delete from %s.\"%s\".%s where object_uid in %s", TRAFODION_SYSCAT_LIT, tgtSch, SEABASE_KEYS,
              objUIDsQry);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_return;
  }

  str_sprintf(query, "delete from %s.\"%s\".%s where table_uid in %s", TRAFODION_SYSCAT_LIT, tgtSch, SEABASE_TABLES,
              objUIDsQry);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_return;
  }

  str_sprintf(query, "delete from %s.\"%s\".%s where base_table_uid in %s", TRAFODION_SYSCAT_LIT, tgtSch,
              SEABASE_INDEXES, objUIDsQry);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_return;
  }

  str_sprintf(query, "delete from %s.\"%s\".%s where text_uid in %s ", TRAFODION_SYSCAT_LIT, tgtSch, SEABASE_TEXT,
              objUIDsQry);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_return;
  }

  // delete from ROUTINES, LIBRARIES and LIBRARIES_USAGE tables.
  str_sprintf(query, "delete from %s.\"%s\".%s where udr_uid in %s ", TRAFODION_SYSCAT_LIT, tgtSch, SEABASE_ROUTINES,
              objUIDsQry);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_return;
  }

  str_sprintf(query, "delete from %s.\"%s\".%s where library_uid in %s ", TRAFODION_SYSCAT_LIT, tgtSch,
              SEABASE_LIBRARIES, objUIDsQry);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_return;
  }

  str_sprintf(query, "delete from %s.\"%s\".%s where using_library_uid in %s ", TRAFODION_SYSCAT_LIT, tgtSch,
              SEABASE_LIBRARIES_USAGE, objUIDsQry);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_return;
  }

  // delete lob col info
  str_sprintf(query, "delete from %s.\"%s\".%s where table_uid in %s ", TRAFODION_SYSCAT_LIT, tgtSch,
              SEABASE_LOB_COLUMNS, objUIDsQry);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_return;
  }

  // delete check constraint entries from TEXT table
  str_sprintf(query,
              "delete from %s.\"%s\".%s where text_uid in (select constraint_uid from %s.\"%s\".%s where table_uid in "
              "%s and constraint_type = 'C')",
              TRAFODION_SYSCAT_LIT, tgtSch, SEABASE_TEXT, TRAFODION_SYSCAT_LIT, srcSch, SEABASE_TABLE_CONSTRAINTS,
              objUIDsQry);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_return;
  }

  str_sprintf(query, "delete from %s.\"%s\".%s where table_uid in %s", TRAFODION_SYSCAT_LIT, tgtSch,
              SEABASE_TABLE_CONSTRAINTS, objUIDsQry);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_return;
  }

  // delete unique constraints
  str_sprintf(query,
              "delete from %s.\"%s\".%s where unique_constraint_uid in (select constraint_uid from %s.\"%s\".%s where "
              "table_uid in %s and constraint_type = 'U')",
              TRAFODION_SYSCAT_LIT, tgtSch, SEABASE_REF_CONSTRAINTS, TRAFODION_SYSCAT_LIT, srcSch,
              SEABASE_TABLE_CONSTRAINTS, objUIDsQry);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_return;
  }

  str_sprintf(query,
              "delete from %s.\"%s\".%s where unique_constraint_uid in (select constraint_uid from %s.\"%s\".%s where "
              "table_uid in %s and constraint_type = 'U')",
              TRAFODION_SYSCAT_LIT, tgtSch, SEABASE_UNIQUE_REF_CONSTR_USAGE, TRAFODION_SYSCAT_LIT, srcSch,
              SEABASE_TABLE_CONSTRAINTS, objUIDsQry);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_return;
  }

  // delete ref constraints
  str_sprintf(query,
              "delete from %s.\"%s\".%s where ref_constraint_uid in (select constraint_uid from %s.\"%s\".%s where "
              "table_uid in %s and constraint_type = 'F')",
              TRAFODION_SYSCAT_LIT, tgtSch, SEABASE_REF_CONSTRAINTS, TRAFODION_SYSCAT_LIT, srcSch,
              SEABASE_TABLE_CONSTRAINTS, objUIDsQry);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_return;
  }

  str_sprintf(query,
              "delete from %s.\"%s\".%s where foreign_constraint_uid in (select constraint_uid from %s.\"%s\".%s where "
              "table_uid in %s and constraint_type = 'F')",
              TRAFODION_SYSCAT_LIT, tgtSch, SEABASE_UNIQUE_REF_CONSTR_USAGE, TRAFODION_SYSCAT_LIT, srcSch,
              SEABASE_TABLE_CONSTRAINTS, objUIDsQry);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_return;
  }

  // delete info about constraint from OBJECTS table
  str_sprintf(
      query,
      "delete from %s.\"%s\".%s where object_uid in (select constraint_uid from %s.\"%s\".%s where table_uid in %s)",
      TRAFODION_SYSCAT_LIT, tgtSch, SEABASE_OBJECTS, TRAFODION_SYSCAT_LIT, srcSch, SEABASE_TABLE_CONSTRAINTS,
      objUIDsQry);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_return;
  }

  // delete info about constraints from KEYS table
  str_sprintf(
      query,
      "delete from %s.\"%s\".%s where object_uid in (select constraint_uid from %s.\"%s\".%s where table_uid in %s)",
      TRAFODION_SYSCAT_LIT, tgtSch, SEABASE_KEYS, TRAFODION_SYSCAT_LIT, srcSch, SEABASE_TABLE_CONSTRAINTS, objUIDsQry);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_return;
  }

  // delete info about indexes from OBJECTS table
  str_sprintf(
      query,
      "delete from %s.\"%s\".%s where object_uid in (select index_uid from %s.\"%s\".%s where base_table_uid in %s)",
      TRAFODION_SYSCAT_LIT, tgtSch, SEABASE_OBJECTS, TRAFODION_SYSCAT_LIT, srcSch, SEABASE_INDEXES, objUIDsQry);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_return;
  }

  // delete info about indexes from TABLES table
  str_sprintf(
      query,
      "delete from %s.\"%s\".%s where table_uid in (select index_uid from %s.\"%s\".%s where base_table_uid in %s)",
      TRAFODION_SYSCAT_LIT, tgtSch, SEABASE_TABLES, TRAFODION_SYSCAT_LIT, srcSch, SEABASE_INDEXES, objUIDsQry);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_return;
  }

  // delete info about indexes from KEYS table
  str_sprintf(
      query,
      "delete from %s.\"%s\".%s where object_uid in (select index_uid from %s.\"%s\".%s where base_table_uid in %s)",
      TRAFODION_SYSCAT_LIT, tgtSch, SEABASE_KEYS, TRAFODION_SYSCAT_LIT, srcSch, SEABASE_INDEXES, objUIDsQry);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_return;
  }

  // delete identity cols
  str_sprintf(query,
              "delete from %s.\"%s\".%s where seq_uid in "
              "(select O.object_uid from %s.\"%s\".%s O, %s.\"%s\".%s O2 "
              " where  (O2.catalog_name, O2.schema_name) = (O.catalog_name, O.schema_name) "
              " and O2.object_type = 'SG' "
              " and %s )",
              TRAFODION_SYSCAT_LIT, tgtSch, SEABASE_SEQ_GEN, TRAFODION_SYSCAT_LIT, tgtSch, SEABASE_OBJECTS,
              TRAFODION_SYSCAT_LIT, srcSch, SEABASE_OBJECTS, objUIDsWherePred.data());
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_return;
  }

  str_sprintf(query,
              "delete from %s.\"%s\".%s where object_uid in "
              "(select O.object_uid from %s.\"%s\".%s O, %s.\"%s\".%s O2 "
              " where  (O2.catalog_name, O2.schema_name) = (O.catalog_name, O.schema_name) "
              " and O2.object_type = 'SG' "
              " and %s )",
              TRAFODION_SYSCAT_LIT, tgtSch, SEABASE_OBJECTS, TRAFODION_SYSCAT_LIT, srcSch, SEABASE_OBJECTS,
              TRAFODION_SYSCAT_LIT, srcSch, SEABASE_OBJECTS, objUIDsWherePred.data());
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_return;
  }

  // Delete privs
  if (isAuthorizationEnabled()) {
    str_sprintf(query, "delete from %s.\"%s\".%s where object_uid in %s", TRAFODION_SYSCAT_LIT, tgtSch2,
                PRIVMGR_OBJECT_PRIVILEGES, objUIDsQry);
    cliRC = cliInterface->executeImmediate(query);
    if (cliRC < 0) {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      goto label_return;
    }

    str_sprintf(query, "delete from %s.\"%s\".%s where object_uid in %s", TRAFODION_SYSCAT_LIT, tgtSch2,
                PRIVMGR_COLUMN_PRIVILEGES, objUIDsQry);
    cliRC = cliInterface->executeImmediate(query);
    if (cliRC < 0) {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      goto label_return;
    }
  }
  cliInterface->restoreCQDs("MERGE_JOINS, NESTED_JOINS, HASH_JOINS, TRAF_BR_LOCKED_OBJECTS_QUERY_TYPE");
  return 0;

label_return:
  cliInterface->restoreCQDs("MERGE_JOINS, NESTED_JOINS, HASH_JOINS, TRAF_BR_LOCKED_OBJECTS_QUERY_TYPE");
  return -1;
}

short CmpSeabaseDDL::copyBRSrcToTgtBasic(RelBackupRestore *brExpr, ExeCliInterface *cliInterface,
                                         const char *objUIDsWherePred, const char *srcSch1, const char *srcSch2,
                                         const char *tgtSch1, const char *tgtSch2, NAString *schPrivsList) {
  int cliRC;

  char objUIDsQry[1000 + strlen(objUIDsWherePred)];
  str_sprintf(objUIDsQry, " ( select O.object_uid from %s.\"%s\".%s O where %s for skip conflict access) ",
              TRAFODION_SYSCAT_LIT, srcSch1, SEABASE_OBJECTS, objUIDsWherePred);

  long rowCount = 0;
  char query[1000 + sizeof(objUIDsQry)];

  cliRC = cliInterface->holdAndSetCQDs(
      "NESTED_JOINS, MERGE_JOINS, HASH_JOINS, TRAF_BR_LOCKED_OBJECTS_QUERY_TYPE",
      "NESTED_JOINS 'OFF', MERGE_JOINS 'OFF', HASH_JOINS 'ON', TRAF_BR_LOCKED_OBJECTS_QUERY_TYPE '1' ");
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  str_sprintf(query, "upsert into %s.\"%s\".%s select * from %s.\"%s\".%s O where %s for skip conflict access",
              TRAFODION_SYSCAT_LIT, tgtSch1, SEABASE_OBJECTS, TRAFODION_SYSCAT_LIT, srcSch1, SEABASE_OBJECTS,
              objUIDsWherePred);
  cliRC = cliInterface->executeImmediate(query, NULL, NULL, TRUE, &rowCount);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_error;
  }

  str_sprintf(
      query, "upsert into %s.\"%s\".%s select * from %s.\"%s\".%s  where object_uid in %s  for skip conflict access ",
      TRAFODION_SYSCAT_LIT, tgtSch1, SEABASE_COLUMNS, TRAFODION_SYSCAT_LIT, srcSch1, SEABASE_COLUMNS, objUIDsQry);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_error;
  }

  str_sprintf(query,
              "upsert into %s.\"%s\".%s select * from %s.\"%s\".%s  where object_uid in %s  for skip conflict access ",
              TRAFODION_SYSCAT_LIT, tgtSch1, SEABASE_KEYS, TRAFODION_SYSCAT_LIT, srcSch1, SEABASE_KEYS, objUIDsQry);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_error;
  }

  str_sprintf(query,
              "upsert into %s.\"%s\".%s select * from %s.\"%s\".%s  where table_uid in %s  for skip conflict access ",
              TRAFODION_SYSCAT_LIT, tgtSch1, SEABASE_TABLES, TRAFODION_SYSCAT_LIT, srcSch1, SEABASE_TABLES, objUIDsQry);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_error;
  }

  str_sprintf(query,
              "upsert into %s.\"%s\".%s select * from %s.\"%s\".%s  where table_uid in %s  for skip conflict access ",
              TRAFODION_SYSCAT_LIT, tgtSch1, SEABASE_TABLE_CONSTRAINTS, TRAFODION_SYSCAT_LIT, srcSch1,
              SEABASE_TABLE_CONSTRAINTS, objUIDsQry);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_error;
  }

  str_sprintf(query,
              "upsert into %s.\"%s\".%s select * from %s.\"%s\".%s  where text_uid in %s and text_type != %d for skip "
              "conflict access ",
              TRAFODION_SYSCAT_LIT, tgtSch1, SEABASE_TEXT, TRAFODION_SYSCAT_LIT, srcSch1, SEABASE_TEXT, objUIDsQry,
              COM_USER_QUERYCACHE_TEXT);
  {
    long numRows = 0;
    cliRC = cliInterface->executeImmediate(query, NULL, NULL, TRUE, &numRows);
    if (cliRC < 0) {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      goto label_error;
    }
  }

  str_sprintf(query,
              "upsert into %s.\"%s\".%s select * from %s.\"%s\".%s  where table_uid in %s for skip conflict access ",
              TRAFODION_SYSCAT_LIT, tgtSch1, SEABASE_LOB_COLUMNS, TRAFODION_SYSCAT_LIT, srcSch1, SEABASE_LOB_COLUMNS,
              objUIDsQry);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_error;
  }

  // upsert constraint entries in OBJECTS table
  str_sprintf(query,
              "upsert into %s.\"%s\".%s select * from %s.\"%s\".%s where object_uid in (select constraint_uid from "
              "%s.\"%s\".%s where table_uid in %s ) for skip conflict access ",
              TRAFODION_SYSCAT_LIT, tgtSch1, SEABASE_OBJECTS, TRAFODION_SYSCAT_LIT, srcSch1, SEABASE_OBJECTS,
              TRAFODION_SYSCAT_LIT, srcSch1, SEABASE_TABLE_CONSTRAINTS, objUIDsQry);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_error;
  }

  // upsert constraint entries in KEYS table
  str_sprintf(query,
              "upsert into %s.\"%s\".%s select * from %s.\"%s\".%s where object_uid in (select constraint_uid from "
              "%s.\"%s\".%s where table_uid in %s ) for skip conflict access ",
              TRAFODION_SYSCAT_LIT, tgtSch1, SEABASE_KEYS, TRAFODION_SYSCAT_LIT, srcSch1, SEABASE_KEYS,
              TRAFODION_SYSCAT_LIT, srcSch1, SEABASE_TABLE_CONSTRAINTS, objUIDsQry);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_error;
  }

  cliInterface->restoreCQDs("MERGE_JOINS, NESTED_JOINS, HASH_JOINS, TRAF_BR_LOCKED_OBJECTS_QUERY_TYPE");
  return 0;

label_error:
  cliInterface->restoreCQDs("MERGE_JOINS, NESTED_JOINS, HASH_JOINS, TRAF_BR_LOCKED_OBJECTS_QUERY_TYPE");
  return -1;
}

short CmpSeabaseDDL::copyBRSrcToTgt(RelBackupRestore *brExpr, ExeCliInterface *cliInterface,
                                    const char *objUIDsWherePred, const char *srcSch1, const char *srcSch2,
                                    const char *tgtSch1, const char *tgtSch2, NAString *schPrivsList) {
  int cliRC;

  // if metadata is being backed up, then MD tables will be locked
  // It will return an error during upsert stmt compile.
  // temporarily set cqd traf_br_locked_objects_query_type to not return an
  // error if it is locked.
  cliRC = cliInterface->holdAndSetCQDs(
      "NESTED_JOINS, MERGE_JOINS, HASH_JOINS, TRAF_BR_LOCKED_OBJECTS_QUERY_TYPE",
      "NESTED_JOINS 'OFF', MERGE_JOINS 'OFF', HASH_JOINS 'ON', TRAF_BR_LOCKED_OBJECTS_QUERY_TYPE '1' ");
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  NABoolean xnWasStartedHere = FALSE;
  if (beginXnIfNotInProgress(cliInterface, xnWasStartedHere)) {
    return -1;
  }

  char objUIDsQry[1000 + strlen(objUIDsWherePred)];
  str_sprintf(objUIDsQry, " ( select O.object_uid from %s.\"%s\".%s O where %s for skip conflict access) ",
              TRAFODION_SYSCAT_LIT, srcSch1, SEABASE_OBJECTS, objUIDsWherePred);

  char query[1000 + sizeof(objUIDsQry)];

  // first backup/restore basic objects from backup schema.
  // This is needed to lob insert...select can work correctly after
  // backup/restore.
  char backupSchPredBuf[1000];
  NAString backupSchPred;
  str_sprintf(backupSchPredBuf, " (O.catalog_name = '%s' and O.schema_name = '%s' and O.object_type = 'BT') ",
              TRAFODION_SYSCAT_LIT, (brExpr->backup() ? tgtSch1 : srcSch1));
  backupSchPred = backupSchPredBuf;
  if (copyBRSrcToTgtBasic(brExpr, cliInterface, backupSchPred, srcSch1, srcSch2, tgtSch1, tgtSch2, schPrivsList) < 0) {
    goto label_error;
  }

  // now copy regular objects from src to tgt.
  if (copyBRSrcToTgtBasic(brExpr, cliInterface, objUIDsWherePred, srcSch1, srcSch2, tgtSch1, tgtSch2, schPrivsList) <
      0) {
    goto label_error;
  }

  // upsert check constraint text
  str_sprintf(query,
              "upsert into %s.\"%s\".%s select * from %s.\"%s\".%s where text_uid in (select constraint_uid from "
              "%s.\"%s\".%s where constraint_type = 'C' and table_uid in %s )  for skip conflict access ",
              TRAFODION_SYSCAT_LIT, tgtSch1, SEABASE_TEXT, TRAFODION_SYSCAT_LIT, srcSch1, SEABASE_TEXT,
              TRAFODION_SYSCAT_LIT, srcSch1, SEABASE_TABLE_CONSTRAINTS, objUIDsQry);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_error;
  }

  str_sprintf(
      query,
      "upsert into %s.\"%s\".%s select * from %s.\"%s\".%s  where base_table_uid in %s for skip conflict access ",
      TRAFODION_SYSCAT_LIT, tgtSch1, SEABASE_INDEXES, TRAFODION_SYSCAT_LIT, srcSch1, SEABASE_INDEXES, objUIDsQry);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_error;
  }

  str_sprintf(
      query, "upsert into %s.\"%s\".%s select * from %s.\"%s\".%s  where seq_uid in %s for skip conflict access ",
      TRAFODION_SYSCAT_LIT, tgtSch1, SEABASE_SEQ_GEN, TRAFODION_SYSCAT_LIT, srcSch1, SEABASE_SEQ_GEN, objUIDsQry);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_error;
  }

  str_sprintf(query,
              "upsert into %s.\"%s\".%s select * from %s.\"%s\".%s  where view_uid in %s for skip conflict access ",
              TRAFODION_SYSCAT_LIT, tgtSch1, SEABASE_VIEWS, TRAFODION_SYSCAT_LIT, srcSch1, SEABASE_VIEWS, objUIDsQry);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_error;
  }

  str_sprintf(
      query,
      "upsert into %s.\"%s\".%s select * from %s.\"%s\".%s  where using_view_uid in %s for skip conflict access ",
      TRAFODION_SYSCAT_LIT, tgtSch1, SEABASE_VIEWS_USAGE, TRAFODION_SYSCAT_LIT, srcSch1, SEABASE_VIEWS_USAGE,
      objUIDsQry);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_error;
  }

  // copy data from ROUTINES, LIBRARIES and LIBRARIES_USAGE tables.
  str_sprintf(
      query, "upsert into %s.\"%s\".%s select * from %s.\"%s\".%s  where udr_uid in %s for skip conflict access ",
      TRAFODION_SYSCAT_LIT, tgtSch1, SEABASE_ROUTINES, TRAFODION_SYSCAT_LIT, srcSch1, SEABASE_ROUTINES, objUIDsQry);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_error;
  }

  // Update routine info after restore routines table
  if (brExpr->restore()) {
    // Replace the LIBRARY_UID field value of table ROUTINES with library UID in the target metedata,
    // cause if this backup tags is import from other cluster,
    // the value of LIBRARY_UID in table ROUTINES which contained in the source schema is invalid.
    str_sprintf(
        query,
        "update %s.\"%s\".%s set library_uid = (select T1.object_uid from %s.\"%s\".%s T1 where T1.object_name= '%s' "
        "and T1.object_type='%s') where external_name like 'org.trafodion.sql.udr.spsql.SPSQL.callSPSQL%%';",
        TRAFODION_SYSCAT_LIT, tgtSch1, SEABASE_ROUTINES, TRAFODION_SYSCAT_LIT, tgtSch1, SEABASE_OBJECTS,
        SEABASE_SPSQL_LIBRARY, COM_LIBRARY_OBJECT_LIT);
    cliRC = cliInterface->executeImmediate(query);
    if (cliRC < 0) {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      goto label_error;
    }
  }

  str_sprintf(
      query, "upsert into %s.\"%s\".%s select * from %s.\"%s\".%s  where library_uid in %s for skip conflict access ",
      TRAFODION_SYSCAT_LIT, tgtSch1, SEABASE_LIBRARIES, TRAFODION_SYSCAT_LIT, srcSch1, SEABASE_LIBRARIES, objUIDsQry);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_error;
  }

  str_sprintf(
      query,
      "upsert into %s.\"%s\".%s select * from %s.\"%s\".%s  where using_library_uid in %s for skip conflict access ",
      TRAFODION_SYSCAT_LIT, tgtSch1, SEABASE_LIBRARIES_USAGE, TRAFODION_SYSCAT_LIT, srcSch1, SEABASE_LIBRARIES_USAGE,
      objUIDsQry);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_error;
  }

  str_sprintf(
      query,
      "upsert into %s.\"%s\".%s select * from %s.\"%s\".%s where unique_constraint_uid in (select constraint_uid from "
      "%s.\"%s\".%s where constraint_type = 'U' and table_uid in %s )  for skip conflict access ",
      TRAFODION_SYSCAT_LIT, tgtSch1, SEABASE_REF_CONSTRAINTS, TRAFODION_SYSCAT_LIT, srcSch1, SEABASE_REF_CONSTRAINTS,
      TRAFODION_SYSCAT_LIT, srcSch1, SEABASE_TABLE_CONSTRAINTS, objUIDsQry);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_error;
  }

  str_sprintf(
      query,
      "upsert into %s.\"%s\".%s select * from %s.\"%s\".%s where unique_constraint_uid in (select constraint_uid from "
      "%s.\"%s\".%s where constraint_type = 'U' and table_uid in %s ) for skip conflict access ",
      TRAFODION_SYSCAT_LIT, tgtSch1, SEABASE_UNIQUE_REF_CONSTR_USAGE, TRAFODION_SYSCAT_LIT, srcSch1,
      SEABASE_UNIQUE_REF_CONSTR_USAGE, TRAFODION_SYSCAT_LIT, srcSch1, SEABASE_TABLE_CONSTRAINTS, objUIDsQry);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_error;
  }

  str_sprintf(query,
              "upsert into %s.\"%s\".%s select * from %s.\"%s\".%s where ref_constraint_uid in (select constraint_uid "
              "from %s.\"%s\".%s where constraint_type = 'F' and table_uid in %s ) for skip conflict access ",
              TRAFODION_SYSCAT_LIT, tgtSch1, SEABASE_REF_CONSTRAINTS, TRAFODION_SYSCAT_LIT, srcSch1,
              SEABASE_REF_CONSTRAINTS, TRAFODION_SYSCAT_LIT, srcSch1, SEABASE_TABLE_CONSTRAINTS, objUIDsQry);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_error;
  }

  str_sprintf(
      query,
      "upsert into %s.\"%s\".%s select * from %s.\"%s\".%s where foreign_constraint_uid in (select constraint_uid from "
      "%s.\"%s\".%s where constraint_type = 'F' and table_uid in %s ) for skip conflict access ",
      TRAFODION_SYSCAT_LIT, tgtSch1, SEABASE_UNIQUE_REF_CONSTR_USAGE, TRAFODION_SYSCAT_LIT, srcSch1,
      SEABASE_UNIQUE_REF_CONSTR_USAGE, TRAFODION_SYSCAT_LIT, srcSch1, SEABASE_TABLE_CONSTRAINTS, objUIDsQry);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_error;
  }

  // upsert indexes entries in OBJECTS table
  str_sprintf(query,
              "upsert into %s.\"%s\".%s select * from %s.\"%s\".%s where object_uid in (select index_uid from "
              "%s.\"%s\".%s where base_table_uid in %s ) for skip conflict access ",
              TRAFODION_SYSCAT_LIT, tgtSch1, SEABASE_OBJECTS, TRAFODION_SYSCAT_LIT, srcSch1, SEABASE_OBJECTS,
              TRAFODION_SYSCAT_LIT, srcSch1, SEABASE_INDEXES, objUIDsQry);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_error;
  }

  // upsert indexes entries in TABLES table
  str_sprintf(query,
              "upsert into %s.\"%s\".%s select * from %s.\"%s\".%s where table_uid in (select index_uid from "
              "%s.\"%s\".%s where base_table_uid in %s ) for skip conflict access ",
              TRAFODION_SYSCAT_LIT, tgtSch1, SEABASE_TABLES, TRAFODION_SYSCAT_LIT, srcSch1, SEABASE_TABLES,
              TRAFODION_SYSCAT_LIT, srcSch1, SEABASE_INDEXES, objUIDsQry);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_error;
  }

  // upsert index entries in KEYS table
  str_sprintf(query,
              "upsert into %s.\"%s\".%s select * from %s.\"%s\".%s where object_uid in (select index_uid from "
              "%s.\"%s\".%s where base_table_uid in %s ) for skip conflict access ",
              TRAFODION_SYSCAT_LIT, tgtSch1, SEABASE_KEYS, TRAFODION_SYSCAT_LIT, srcSch1, SEABASE_KEYS,
              TRAFODION_SYSCAT_LIT, srcSch1, SEABASE_INDEXES, objUIDsQry);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_error;
  }

  str_sprintf(query,
              "upsert into %s.\"%s\".%s select * from %s.\"%s\".%s "
              " where seq_uid in "
              "(select O2.object_uid from %s.\"%s\".%s O, %s.\"%s\".%s O2 "
              " where  (O2.catalog_name, O2.schema_name) = (O.catalog_name, O.schema_name) "
              " and O2.object_type = 'SG' "
              " and %s ) for skip conflict access ",
              TRAFODION_SYSCAT_LIT, tgtSch1, SEABASE_SEQ_GEN, TRAFODION_SYSCAT_LIT, srcSch1, SEABASE_SEQ_GEN,
              TRAFODION_SYSCAT_LIT, srcSch1, SEABASE_OBJECTS, TRAFODION_SYSCAT_LIT, srcSch1, SEABASE_OBJECTS,
              objUIDsWherePred);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_error;
  }

  str_sprintf(query,
              "upsert into %s.\"%s\".%s select * from %s.\"%s\".%s "
              " where object_uid in "
              "(select O2.object_uid from %s.\"%s\".%s O, %s.\"%s\".%s O2 "
              " where  (O2.catalog_name, O2.schema_name) = (O.catalog_name, O.schema_name) "
              " and O2.object_type = 'SG' "
              " and %s ) for skip conflict access ",
              TRAFODION_SYSCAT_LIT, tgtSch1, SEABASE_OBJECTS, TRAFODION_SYSCAT_LIT, srcSch1, SEABASE_OBJECTS,
              TRAFODION_SYSCAT_LIT, srcSch1, SEABASE_OBJECTS, TRAFODION_SYSCAT_LIT, srcSch1, SEABASE_OBJECTS,
              objUIDsWherePred);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_error;
  }

  // privileges
  if (isAuthorizationEnabled()) {
    str_sprintf(query,
                "upsert into %s.\"%s\".%s select * from %s.\"%s\".%s  where object_uid in %s for skip conflict access ",
                TRAFODION_SYSCAT_LIT, tgtSch2, PRIVMGR_OBJECT_PRIVILEGES, TRAFODION_SYSCAT_LIT, srcSch2,
                PRIVMGR_OBJECT_PRIVILEGES, objUIDsQry);
    cliRC = cliInterface->executeImmediate(query);
    if (cliRC < 0) {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      goto label_error;
    }

    str_sprintf(query,
                "upsert into %s.\"%s\".%s select * from %s.\"%s\".%s  where object_uid in %s for skip conflict access ",
                TRAFODION_SYSCAT_LIT, tgtSch2, PRIVMGR_COLUMN_PRIVILEGES, TRAFODION_SYSCAT_LIT, srcSch2,
                PRIVMGR_COLUMN_PRIVILEGES, objUIDsQry);
    cliRC = cliInterface->executeImmediate(query);
    if (cliRC < 0) {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      goto label_error;
    }

    str_sprintf(query,
                "upsert into %s.\"%s\".%s select * from %s.\"%s\".%s  where schema_uid in %s for skip conflict access ",
                TRAFODION_SYSCAT_LIT, tgtSch2, PRIVMGR_SCHEMA_PRIVILEGES, TRAFODION_SYSCAT_LIT, srcSch2,
                PRIVMGR_SCHEMA_PRIVILEGES, objUIDsQry);
    cliRC = cliInterface->executeImmediate(query);
    if (cliRC < 0) {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      goto label_error;
    }

    // get schemas associated with list of objects
    if (schPrivsList && schPrivsList->length() > 0) {
      str_sprintf(query,
                  "upsert into %s.\"%s\".%s select * from %s.\"%s\".%s  where "
                  "catalog_name || '.' || schema_name in (%s) and object_type in ('SS', 'PS') "
                  "for skip conflict access ",
                  TRAFODION_SYSCAT_LIT, tgtSch1, SEABASE_OBJECTS, TRAFODION_SYSCAT_LIT, srcSch1, SEABASE_OBJECTS,
                  schPrivsList->data());
      cliRC = cliInterface->executeImmediate(query);
      if (cliRC < 0) {
        cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
        goto label_error;
      }

      str_sprintf(query,
                  "upsert into %s.\"%s\".%s select * from %s.\"%s\".%s  where "
                  "replace(schema_name,'\"','') in (%s) for skip conflict access ",
                  TRAFODION_SYSCAT_LIT, tgtSch2, PRIVMGR_SCHEMA_PRIVILEGES, TRAFODION_SYSCAT_LIT, srcSch2,
                  PRIVMGR_SCHEMA_PRIVILEGES, schPrivsList->data());
      cliRC = cliInterface->executeImmediate(query);
      if (cliRC < 0) {
        cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
        goto label_error;
      }
    }
  }

  // copy schema info
  if (brExpr->restore() && brExpr->tableList()) {
    for (CollIndex i = 0; i < brExpr->tableList()->entries(); i++) {
      ElemDDLQualName *dn = ((*brExpr->tableList())[i])->castToElemDDLQualName();
      NAString schemaName;
      if (NOT dn->getQualifiedName().getSchemaName().isNull()) schemaName = dn->getQualifiedName().getSchemaName();
      if (NOT schemaName.isNull()) {
        ComObjectType objectType;
        int schemaOwnerID = 0;
        long schemaUID = getObjectTypeandOwner(cliInterface, TRAFODION_SYSCAT_LIT, schemaName.data(),
                                                SEABASE_SCHEMA_OBJECTNAME, objectType, schemaOwnerID);

        if (schemaUID == -1) {
          // schema does not exist
          snprintf(query, sizeof(query),
                   "upsert into %s.\"%s\".%s select * from %s.\"%s\".%s  where "
                   "schema_name = '%s' and object_name = '%s' for skip conflict access;",
                   TRAFODION_SYSCAT_LIT, tgtSch1, SEABASE_OBJECTS, TRAFODION_SYSCAT_LIT, srcSch1, SEABASE_OBJECTS,
                   schemaName.data(), SEABASE_SCHEMA_OBJECTNAME);
          cliRC = cliInterface->executeImmediate(query);
          if (cliRC < 0) {
            cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
            goto label_error;
          }
        }
      }
    }
  }

  // restore CQDs.
  cliInterface->restoreCQDs("MERGE_JOINS, NESTED_JOINS, HASH_JOINS, TRAF_BR_LOCKED_OBJECTS_QUERY_TYPE");

  if (endXnIfStartedHere(cliInterface, xnWasStartedHere, 0)) return -1;

  return 0;

label_error:

  // restore CQDs.
  cliInterface->restoreCQDs("MERGE_JOINS, NESTED_JOINS, HASH_JOINS, TRAF_BR_LOCKED_OBJECTS_QUERY_TYPE");

  endXnIfStartedHere(cliInterface, xnWasStartedHere, -1);
  return -1;
}

short CmpSeabaseDDL::backupHistStats(ExeCliInterface *cliInterface, ElemDDLList *tableList, char *backupSch) {
  int cliRC = 0;

  if ((!tableList) || (tableList->entries() == 0)) return 0;

  NABoolean xnWasStartedHere = FALSE;
  if (beginXnIfNotInProgress(cliInterface, xnWasStartedHere)) return -1;

  char query[2000];

  // for each table specified in the table list, copy hist stats from
  // corresponding hist tables to backupMD hist tables.
  for (CollIndex i = 0; i < tableList->entries(); i++) {
    ElemDDLQualName *dn = ((*tableList)[i])->castToElemDDLQualName();
    NAString tableName = "";
    if (NOT dn->getQualifiedName().getCatalogName().isNull())
      tableName += dn->getQualifiedName().getCatalogName() + ".";
    if (NOT dn->getQualifiedName().getSchemaName().isNull()) tableName += dn->getQualifiedName().getSchemaName() + ".";
    tableName += dn->getQualifiedName().getObjectName();
    if (isHBaseNativeTable(tableName)) continue;

    str_sprintf(query,
                "upsert into %s.\"%s\".%s select * from %s.\"%s\".%s where table_uid = (select object_uid from "
                "%s.\"%s\".%s where catalog_name = '%s' and schema_name = '%s' and object_name = '%s')",
                TRAFODION_SYSCAT_LIT, backupSch, HBASE_HIST_NAME, TRAFODION_SYSCAT_LIT,
                dn->getQualifiedName().getSchemaName().data(), HBASE_HIST_NAME, TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA,
                SEABASE_OBJECTS, TRAFODION_SYSCAT_LIT, dn->getQualifiedName().getSchemaName().data(),
                dn->getQualifiedName().getObjectName().data());
    cliRC = cliInterface->executeImmediate(query);
    if (cliRC < 0) {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      goto label_error;
    }

    str_sprintf(query,
                "upsert into %s.\"%s\".%s select * from %s.\"%s\".%s where table_uid = (select object_uid from "
                "%s.\"%s\".%s where catalog_name = '%s' and schema_name = '%s' and object_name = '%s')",
                TRAFODION_SYSCAT_LIT, backupSch, HBASE_HISTINT_NAME, TRAFODION_SYSCAT_LIT,
                dn->getQualifiedName().getSchemaName().data(), HBASE_HISTINT_NAME, TRAFODION_SYSCAT_LIT,
                SEABASE_MD_SCHEMA, SEABASE_OBJECTS, TRAFODION_SYSCAT_LIT, dn->getQualifiedName().getSchemaName().data(),
                dn->getQualifiedName().getObjectName().data());
    cliRC = cliInterface->executeImmediate(query);
    if (cliRC < 0) {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      goto label_error;
    }

    str_sprintf(query,
                "upsert into %s.\"%s\".%s select * from %s.\"%s\".%s where table_uid = (select object_uid from "
                "%s.\"%s\".%s where catalog_name = '%s' and schema_name = '%s' and object_name = '%s')",
                TRAFODION_SYSCAT_LIT, backupSch, HBASE_PERS_SAMP_NAME, TRAFODION_SYSCAT_LIT,
                dn->getQualifiedName().getSchemaName().data(), HBASE_PERS_SAMP_NAME, TRAFODION_SYSCAT_LIT,
                SEABASE_MD_SCHEMA, SEABASE_OBJECTS, TRAFODION_SYSCAT_LIT, dn->getQualifiedName().getSchemaName().data(),
                dn->getQualifiedName().getObjectName().data());
    cliRC = cliInterface->executeImmediate(query);
    if (cliRC < 0) {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      goto label_error;
    }
  }

  if (endXnIfStartedHere(cliInterface, xnWasStartedHere, 0)) return -1;

  return 0;

label_error:
  endXnIfStartedHere(cliInterface, xnWasStartedHere, -1);

  return -1;
}

short CmpSeabaseDDL::expandAndValidate(ExeCliInterface *cliInterface, ElemDDLQualName *dn, const char *objectType,
                                       NABoolean validateObjects, NAString &currCatName, NAString &currSchName) {
  if (dn->getQualifiedName().numberExpanded() > 3) {
    *CmpCommon::diags()
        << DgSqlCode(-5050) << DgString0("BACKUP")
        << DgString1(" Reason: Incorrect object name format was specified. It cannot contain more than 3 parts.");

    return -1;
  }

  if (dn->getQualifiedName().numberExpanded() < 3) {
    ComSchemaName csn(currCatName, currSchName);

    SchemaName sn(csn.getSchemaNamePart().getInternalName(), csn.getCatalogNamePart().getInternalName());
    dn->getQualifiedName().applyDefaults(sn);
  }

  if (validateObjects) {
    if (CmpSqlSession::isValidVolatileSchemaName((NAString &)dn->getQualifiedName().getSchemaName())) {
      *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("BACKUP")
                          << DgString1(" Reason: Volatile objects cannot be backed up.");

      return -1;
    }
    CmpCommon::diags()->clear();

    CmpCommon::diags()->clear();

    ComObjectName objName(dn->getQualifiedName().getCatalogName(),
                          dn->getQualifiedName().getUnqualifiedSchemaNameAsAnsiString(),
                          dn->getQualifiedName().getUnqualifiedObjectNameAsAnsiString());

    long objUID = getObjectUID(
        cliInterface, objName.getCatalogNamePartAsAnsiString(), objName.getSchemaNamePartAsAnsiString(TRUE),
        objName.getObjectNamePartAsAnsiString(TRUE), objectType, NULL, NULL, NULL, FALSE, FALSE);

    if (objUID <= 0) {
      NAString type(objectType);
      NAString typeName;
      if (type == COM_BASE_TABLE_OBJECT_LIT)
        typeName = "table";
      else if (type == COM_VIEW_OBJECT_LIT)
        typeName = "view";
      else if ((type == COM_USER_DEFINED_ROUTINE_OBJECT_LIT) || (type == COM_STORED_PROCEDURE_OBJECT_LIT))
        typeName = "routine";
      else if (type == COM_LIBRARY_OBJECT_LIT)
        typeName = "library";

      NAString reasonStr;
      if (typeName.isNull())
        reasonStr = NAString("Specified object ") + dn->getQualifiedName().getQualifiedNameAsString() + NAString(".");
      else
        reasonStr = NAString("Specified object ") + dn->getQualifiedName().getQualifiedNameAsString() +
                    NAString(" does not exist or is not a ") + typeName + NAString(".");
      *CmpCommon::diags() << DgSqlCode(-3242) << DgString0(reasonStr);

      return -1;
    }
  }  // validate object

  return 0;
}

short CmpSeabaseDDL::validateInputNames(ExeCliInterface *cliInterface, RelBackupRestore *brExpr,
                                        NABoolean validateObjects, NAString &currCatName, NAString &currSchName) {
  ElemDDLList *schemaList = brExpr->schemaList();
  ElemDDLList *tableList = brExpr->tableList();
  ElemDDLList *viewList = brExpr->viewList();
  ElemDDLList *procList = brExpr->procList();
  ElemDDLList *libList = brExpr->libList();
  if (schemaList && (schemaList->entries() > 0)) {
    for (CollIndex i = 0; i < schemaList->entries(); i++) {
      ElemDDLQualName *dn = ((*schemaList)[i])->castToElemDDLQualName();

      if (dn->getQualifiedName().numberExpanded() > 2) {
        *CmpCommon::diags()
            << DgSqlCode(-5050) << DgString0("BACKUP")
            << DgString1(" Reason: Incorrect object name format was specified. It cannot contain more than 2 parts.");

        return -1;
      }

      NAString dummyCat("DummyCat");
      SchemaName sn(currCatName, dummyCat);
      dn->getQualifiedName().applyDefaults(sn);

      if (validateObjects) {
        if (CmpSqlSession::isValidVolatileSchemaName((NAString &)dn->getQualifiedName().getSchemaName())) {
          *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("BACKUP")
                              << DgString1(" Reason: Volatile objects cannot be backed up.");

          return -1;
        }

        long objUID =
            getObjectUID(cliInterface, dn->getQualifiedName().getSchemaName(), dn->getQualifiedName().getObjectName(),
                         SEABASE_SCHEMA_OBJECTNAME, COM_SHARED_SCHEMA_OBJECT_LIT, NULL, NULL, NULL, FALSE, FALSE);

        if (objUID <= 0) {
          // could be private schema
          objUID =
              getObjectUID(cliInterface, dn->getQualifiedName().getSchemaName(), dn->getQualifiedName().getObjectName(),
                           SEABASE_SCHEMA_OBJECTNAME, COM_PRIVATE_SCHEMA_OBJECT_LIT, NULL, NULL, NULL, FALSE, FALSE);
        }

        if (objUID <= 0) {
          *CmpCommon::diags() << DgSqlCode(-4082)
                              << DgTableName(dn->getQualifiedName().getSchemaName() + "." +
                                             dn->getQualifiedName().getObjectName());

          return -1;
        }
      }
    }  // for
  }    // if

  if (tableList && (tableList->entries() > 0)) {
    for (CollIndex i = 0; i < tableList->entries(); i++) {
      ElemDDLQualName *dn = ((*tableList)[i])->castToElemDDLQualName();

      NAString tableName = "";
      if (NOT dn->getQualifiedName().getCatalogName().isNull())
        tableName += dn->getQualifiedName().getCatalogName() + ".";
      if (NOT dn->getQualifiedName().getSchemaName().isNull())
        tableName += dn->getQualifiedName().getSchemaName() + ".";
      tableName += dn->getQualifiedName().getObjectName();
      if (isHBaseNativeTable(tableName)) continue;
      if (expandAndValidate(cliInterface, dn, COM_BASE_TABLE_OBJECT_LIT, validateObjects, currCatName, currSchName)) {
        return -1;
      }

    }  // for
  }    // if

  if (viewList && (viewList->entries() > 0)) {
    for (CollIndex i = 0; i < viewList->entries(); i++) {
      ElemDDLQualName *dn = ((*viewList)[i])->castToElemDDLQualName();
      if (expandAndValidate(cliInterface, dn, COM_VIEW_OBJECT_LIT, validateObjects, currCatName, currSchName)) {
        return -1;
      }
    }  // for
  }    // if

  if (procList && (procList->entries() > 0)) {
    for (CollIndex i = 0; i < procList->entries(); i++) {
      ElemDDLQualName *dn = ((*procList)[i])->castToElemDDLQualName();
      if (expandAndValidate(cliInterface, dn, COM_USER_DEFINED_ROUTINE_OBJECT_LIT, validateObjects, currCatName,
                            currSchName)) {
        return -1;
      }
    }  // for
  }    // if

  if (libList && (libList->entries() > 0)) {
    for (CollIndex i = 0; i < libList->entries(); i++) {
      ElemDDLQualName *dn = ((*libList)[i])->castToElemDDLQualName();
      if (expandAndValidate(cliInterface, dn, COM_LIBRARY_OBJECT_LIT, validateObjects, currCatName, currSchName)) {
        return -1;
      }
    }  // for
  }    // if

  return 0;
}

short CmpSeabaseDDL::processBRShowObjects(RelBackupRestore *brExpr, NAArray<HbaseStr> *mdList,
                                          NAString &objUIDsWherePred, ExeCliInterface *cliInterface,
                                          CmpDDLwithStatusInfo *dws) {
  int cliRC = 0;

  Queue *objsQueue = NULL;
  cliInterface->initializeInfoList(objsQueue, TRUE);

  updateQueueWithOI(" ", objsQueue, STMTHEAP);
  updateQueueWithOI("MetaData Objects", objsQueue, STMTHEAP);
  updateQueueWithOI("================", objsQueue, STMTHEAP);
  updateQueueWithOI(" ", objsQueue, STMTHEAP);

  NAString backupSch;
  genBackupSchemaName(brExpr->backupTag(), backupSch);

  NAString sourceSch(brExpr->backup() ? SEABASE_MD_SCHEMA : backupSch);

  // find out if libraries need to be backed up.
  char objsQry[1000 + objUIDsWherePred.length()];
  NABoolean libsToBeBackedUp = FALSE;
  if (brExpr->backup()) {
    str_sprintf(objsQry,
                " select count(*) from %s.\"%s\".%s O where %s and O.object_type in ('" COM_LIBRARY_OBJECT_LIT "') ",
                TRAFODION_SYSCAT_LIT, sourceSch.data(), SEABASE_OBJECTS, objUIDsWherePred.data());
    long rowCount = 0;
    int length = 0;
    cliRC = cliInterface->executeImmediate(objsQry, (char *)&rowCount, &length, FALSE);
    if (cliRC < 0) {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      return -1;
    }
    if (rowCount > 0) libsToBeBackedUp = TRUE;
  }

  int numMDs = 0;
  int numTables = 0;
  int numIndexes = 0;
  int numViews = 0;
  int numRoutines = 0;
  int numLibs = 0;
  int numLOBs = 0;
  int lobFiles = 0;
  TextVec lobLocList;

  // MD and user objects are shown in ansi identifier format.
  // cat/sch/obj parts are double quoted if needed.
  // Method ToAnsiIdentifier determines when they should be double quoted.
  if (mdList) {
    for (int i = 0; i < mdList->entries(); i++) {
      int len = mdList->at(i).len;
      const char *val = mdList->at(i).val;

      const char *val2 = val;
      const char *colonPos = strchr(val, ':');
      NAString nameSpace;
      if (colonPos) {
        nameSpace = NAString(val, (colonPos - val + 1));
        val2 = colonPos + 1;
      }
      char *parts[3];
      char outBuffer[1000];
      int numParts = 0;
      LateNameInfo::extractParts(val2, outBuffer, numParts, parts, FALSE);
      NAString catName = ToAnsiIdentifier(NAString(parts[0]));
      NAString schName = ToAnsiIdentifier(NAString(parts[1]));
      NAString objName = ToAnsiIdentifier(NAString(parts[2]));

      NAString row = nameSpace + catName + NAString(".") + schName + "." + objName;

      len = row.length();
      char *brVal = new (STMTHEAP) char[len + 1];
      strncpy(brVal, row.data(), len);
      brVal[len] = 0;

      OutputInfo *oi = new (STMTHEAP) OutputInfo(1);
      oi->insert(0, brVal, len + 1);

      objsQueue->insert(oi);
    }
    numMDs = mdList->entries();
  } else {
    NAString rsrvdStr(TRAF_RESERVED_NAMESPACE3);
    rsrvdStr += ":";
    numMDs = 0;
    for (int i = 0; i < sizeof(backupMDs) / sizeof(BackupMDsInfo); i++) {
      NAString catName = ToAnsiIdentifier(NAString(backupMDs[i].catName));
      NAString schName = ToAnsiIdentifier(NAString(backupSch));
      NAString objName = ToAnsiIdentifier(NAString(backupMDs[i].objName));

      NAString row = rsrvdStr + catName + NAString(".") + schName + "." + objName;
      updateQueueWithOI(row.data(), objsQueue, STMTHEAP);

      if (backupMDs[i].hasLob) {
        NAString lobChunksObjName;
        if (genLobChunksTableName(cliInterface, backupMDs[i].catName, backupMDs[i].schName, backupMDs[i].objName,
                                  backupSch.data(), lobChunksObjName) < 0)
          return -1;  // does not exist

        lobChunksObjName.prepend(":");
        lobChunksObjName.prepend(TRAF_RESERVED_NAMESPACE3);

        updateQueueWithOI(lobChunksObjName.data(), objsQueue, STMTHEAP);
        numMDs++;
      }

      numMDs++;
    }  // for
  }

  if ((brExpr->backup()) && (lobLocList.size() > 0) && (libsToBeBackedUp)) {
    // add hdfs lob objects that need to be backed up
    updateQueueWithOI(" ", objsQueue, STMTHEAP);
    updateQueueWithOI("Metadata LOB Data Files", objsQueue, STMTHEAP);
    updateQueueWithOI("=======================", objsQueue, STMTHEAP);
    updateQueueWithOI(" ", objsQueue, STMTHEAP);

    for (int idx = 0; idx < lobLocList.size(); idx++) {
      Text &lobLoc = lobLocList[idx];
      updateQueueWithOI(lobLoc.c_str(), objsQueue, STMTHEAP);
    }
  }

  if (NOT brExpr->brMetadata()) {
    updateQueueWithOI(" ", objsQueue, STMTHEAP);
    updateQueueWithOI("User objects", objsQueue, STMTHEAP);
    updateQueueWithOI("============", objsQueue, STMTHEAP);
    updateQueueWithOI(" ", objsQueue, STMTHEAP);
  } else {
    updateQueueWithOI(" ", objsQueue, STMTHEAP);
    updateQueueWithOI("User Objects (Metadata only)", objsQueue, STMTHEAP);
    updateQueueWithOI("============================", objsQueue, STMTHEAP);
    updateQueueWithOI(" ", objsQueue, STMTHEAP);
  }

  Queue *userObjsQueue = NULL;
  str_sprintf(objsQry,
              " select case object_type when 'BT' then 'TABLE:   ' when 'IX' then 'INDEX:   ' when 'VI' then 'VIEW:    "
              "' when 'UR' then 'ROUTINE: ' when 'LB' then 'LIBRARY: ' else ' ' end, case when T.text is not null then "
              "trim(T.text) || ':' else '' end,  trim(O.catalog_name), trim(O.schema_name), trim(O.object_name) from "
              "%s.\"%s\".%s O left join %s.\"%s\".%s T on O.object_uid = T.text_uid and T.text_type = %d where %s and "
              "O.object_type in ('" COM_BASE_TABLE_OBJECT_LIT "', '" COM_INDEX_OBJECT_LIT "', '" COM_VIEW_OBJECT_LIT
              "', '" COM_USER_DEFINED_ROUTINE_OBJECT_LIT "', '" COM_LIBRARY_OBJECT_LIT
              "') order by case object_type when 'BT' then 1 when 'IX' then 2 when 'VI' then 3 when 'UR' then 4 when "
              "'LB' then 5 else 6 end, 3,4,5 ",
              TRAFODION_SYSCAT_LIT, sourceSch.data(), SEABASE_OBJECTS, TRAFODION_SYSCAT_LIT, sourceSch.data(),
              SEABASE_TEXT, COM_OBJECT_NAMESPACE, objUIDsWherePred.data());

  cliRC = cliInterface->fetchAllRows(userObjsQueue, objsQry, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  if (userObjsQueue->numEntries() == 0) {
    *CmpCommon::diags() << DgSqlCode(-5050) << DgString0(brExpr->backup() ? "BACKUP" : "RESTORE")
                        << DgString1("Reason: Specified list contains no objects.");

    return -1;
  }

  for (int idx = 0; idx < userObjsQueue->numEntries(); idx++) {
    OutputInfo *vi = (OutputInfo *)userObjsQueue->getNext();
    char *ptr = NULL;
    int len = 0;

    vi->get(0, ptr, len);
    NAString obj(ptr, len);
    if (obj.index("TABLE: ") == 0)
      numTables++;
    else if (obj.index("INDEX: ") == 0)
      numIndexes++;
    else if (obj.index("VIEW: ") == 0)
      numViews++;
    else if (obj.index("ROUTINE: ") == 0)
      numRoutines++;
    else if (obj.index("LIBRARY: ") == 0)
      numLibs++;

    vi->get(1, ptr, len);
    NAString nameSpace;
    if (ptr && (len > 0)) nameSpace = NAString(ptr, len);

    vi->get(2, ptr, len);
    NAString catName(ptr, len);
    catName = ToAnsiIdentifier(catName);

    vi->get(3, ptr, len);
    NAString schName(ptr, len);
    schName = ToAnsiIdentifier(schName);

    vi->get(4, ptr, len);
    NAString objName(ptr, len);
    objName = ToAnsiIdentifier(objName);

    NAString fullRow(obj + (NOT nameSpace.isNull() ? nameSpace : NAString("")) + catName + "." + schName + "." +
                     objName);
    updateQueueWithOI(fullRow.data(), objsQueue, STMTHEAP);
  }

  Queue *lobLocQueue = NULL;
  str_sprintf(objsQry,
              "select O.object_uid, trim(O.schema_name), trim(O.catalog_name) || '.' || trim(O.schema_name) || '.' || "
              "trim(O.object_name) from %s.\"%s\".%s O, %s.\"%s\".%s C where O.object_uid = C.object_uid and "
              "C.fs_data_type in (160,161) and %s and O.object_type in ('" COM_BASE_TABLE_OBJECT_LIT
              "', '" COM_INDEX_OBJECT_LIT "') group by 1,2,3",
              TRAFODION_SYSCAT_LIT, sourceSch.data(), SEABASE_OBJECTS, TRAFODION_SYSCAT_LIT, sourceSch.data(),
              SEABASE_COLUMNS, objUIDsWherePred.data());

  cliRC = cliInterface->fetchAllRows(lobLocQueue, objsQry, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  // LOB data files are displayed as follows for backup only.
  //
  // For restore, they are not displayed as the LOBMD_* tables
  // need to be restored before displaying lob information
  // contained in it. Since these tables are part of user data,
  // they are not restored for display.
  // They are only restored with actual restore.
  //
  // Ex: if tablename has 2 lobs, then display will be:
  //
  //   cat.sch.tablename:
  //     /location/LOBP_1
  //     /location/LOBP_2
  //
  int numLOBfiles = 0;
  if (brExpr->backup()) {
    TextVec lobLocList;
    for (int idx = 0; idx < lobLocQueue->numEntries(); idx++) {
      OutputInfo *vi = (OutputInfo *)lobLocQueue->getNext();
      char *ptr = NULL;
      int len = 0;

      long objUID = *(long *)vi->get(0);
      vi->get(1, ptr, len);
      NAString sch(ptr, len);

      vi->get(2, ptr, len);
      NAString obj(ptr, len);
      obj += ":";
      lobLocList.push_back(obj.data());

      int lobFiles = 0;
      cliRC = genLobLocation(objUID, sch.data(), cliInterface, lobLocList, lobFiles, TRUE);
      if (cliRC < 0) {
        return -1;
      }
      numLOBfiles += lobFiles;
    }  // for

    if (numLOBfiles > 0) {
      if (lobLocQueue->numEntries() > 0) {
        // add hdfs lob objects that need to be backed up
        updateQueueWithOI(" ", objsQueue, STMTHEAP);
        updateQueueWithOI("LOB Data Files", objsQueue, STMTHEAP);
        updateQueueWithOI("==============", objsQueue, STMTHEAP);
        updateQueueWithOI(" ", objsQueue, STMTHEAP);
      }

      for (int idx = 0; idx < lobLocList.size(); idx++) {
        Text &lobLoc = lobLocList[idx];
        updateQueueWithOI(lobLoc.c_str(), objsQueue, STMTHEAP);
      }
    }
  }

  updateQueueWithOI(" ", objsQueue, STMTHEAP);

  if (brExpr->backup()) {
    updateQueueWithOI("Backup Stats", objsQueue, STMTHEAP);
    updateQueueWithOI("============", objsQueue, STMTHEAP);
  } else {
    updateQueueWithOI("Restore Stats", objsQueue, STMTHEAP);
    updateQueueWithOI("=============", objsQueue, STMTHEAP);
  }

  char numBuf[20];
  int totalObjs = numMDs + numTables + numIndexes + numViews + numRoutines + numLibs + numLOBfiles;
  updateQueueWithOI(NAString("  Total:     ") + str_itoa(totalObjs, numBuf), objsQueue, STMTHEAP);
  if (numMDs > 0) updateQueueWithOI(NAString("  Metadata:  ") + str_itoa(numMDs, numBuf), objsQueue, STMTHEAP);
  if (numTables > 0) updateQueueWithOI(NAString("  Tables:    ") + str_itoa(numTables, numBuf), objsQueue, STMTHEAP);
  if (numIndexes > 0) updateQueueWithOI(NAString("  Indexes:   ") + str_itoa(numIndexes, numBuf), objsQueue, STMTHEAP);
  if (numViews > 0) updateQueueWithOI(NAString("  Views:     ") + str_itoa(numViews, numBuf), objsQueue, STMTHEAP);
  if (numRoutines > 0)
    updateQueueWithOI(NAString("  Routines:  ") + str_itoa(numRoutines, numBuf), objsQueue, STMTHEAP);
  if (numLibs > 0) updateQueueWithOI(NAString("  Libraries: ") + str_itoa(numLibs, numBuf), objsQueue, STMTHEAP);
  if (numLOBfiles > 0)
    updateQueueWithOI(NAString("  LOBFiles:  ") + str_itoa(numLOBfiles, numBuf), objsQueue, STMTHEAP);
  updateQueueWithOI(" ", objsQueue, STMTHEAP);

  int blackBoxLen = 0;
  char *blackBox = NULL;

  populateBlackBox(cliInterface, objsQueue, blackBoxLen, blackBox);

  dws->setBlackBoxLen(blackBoxLen);
  dws->setBlackBox(blackBox);

  dws->setStep(-1);
  dws->setSubstep(0);
  dws->setDone(TRUE);

  return 0;
}

// split input string into tokens separated by delim.
static std::vector<string> split(const std::string &s, char delim) {
  std::stringstream ss(s);
  std::string item;
  std::vector<std::string> tokens;
  while (getline(ss, item, delim)) {
    tokens.push_back(item);
  }

  return tokens;
}

static NABoolean adjacentSpaces(char lhs, char rhs) { return (lhs == rhs) && (lhs == ' '); }

static void extractTokens(std::string &s, std::vector<string> &tokens) {
  //  std::string s(val);
  std::string::iterator new_end = std::unique(s.begin(), s.end(), adjacentSpaces);
  s.erase(new_end, s.end());

  tokens = split(s, ' ');
}

// inputStr contains backup info returned by java layer.
// Format: <tag> <BackupTime> <BackupStatus> <BackupOper>
// return TRUE if inputStr contains brExprTag. FALSE, otherwise.
static NABoolean containsTag(const char *inputStr, int inputStrLen, char *brExprTag, NAString &oper,
                             NAString &status) {
  if ((!inputStr) || (inputStrLen == 0) || (!brExprTag)) return FALSE;

  char inputStrBuf[inputStrLen + 1];

  strncpy(inputStrBuf, inputStr, inputStrLen);
  inputStrBuf[inputStrLen] = 0;

  if (strstr(inputStrBuf, brExprTag) == inputStrBuf) {
    std::string s(inputStrBuf);
    std::vector<string> tokens = split(s, ' ');

    if ((!tokens.empty()) && (tokens.size() == 4)) {
      status = tokens[2].c_str();
      oper = tokens[3].c_str();
    }

    return TRUE;
  } else
    return FALSE;
}

NABoolean CmpSeabaseDDL::containsTagInSnapshotList(NAArray<HbaseStr> *snapshotList, char *brExprTag, NAString &oper,
                                                   NAString &status) {
  if ((!snapshotList) || (snapshotList->entries() == 0)) return FALSE;

  int numEntries = snapshotList->entries();

  oper.clear();
  status.clear();

  NABoolean found = FALSE;
  for (int i = 0; (i < numEntries); i++) {
    int len = snapshotList->at(i).len;
    char *val = snapshotList->at(i).val;

    // remove adjacent spaces and replace with one space.
    std::string str(val, len);
    std::string::iterator new_end = std::unique(str.begin(), str.end(), adjacentSpaces);
    str.erase(new_end, str.end());

    if (containsTag(str.c_str(), str.length(), brExprTag, oper, status)) return TRUE;
  }

  return FALSE;
}

NABoolean CmpSeabaseDDL::getTimeStampFromSnapshotList(NAArray<HbaseStr> *snapshotList, char *brExprTag, char *timeStamp,
                                                      ComDiagsArea **diagsArea) {
  if ((!snapshotList) || (snapshotList->entries() == 0)) return FALSE;

  for (int i = 0; i < snapshotList->entries(); i++) {
    int len = snapshotList->at(i).len;
    char *val = snapshotList->at(i).val;

    std::string s(val, len);

    std::vector<string> tokens;
    extractTokens(s, tokens);
    if ((tokens.empty()) || (tokens.size() != 4)) continue;
    if (strcmp(tokens[0].c_str(), brExprTag) == 0) {
      if (CmpCommon::context()->gmtDiff() != 0) {
        ExpDatetime::convAsciiDatetimeToUtcOrLocal((char *)tokens[1].c_str(), strlen(tokens[1].c_str()), timeStamp,
                                                   strlen(tokens[1].c_str()),
                                                   (long)CmpCommon::context()->gmtDiff() * 60 * 1000000,
                                                   FALSE,  // toLocal
                                                   STMTHEAP, diagsArea);
        timeStamp[10] = ':';  // add ':' separator between date and time
        return TRUE;
      }
    }
  }
  return FALSE;
}

short CmpSeabaseDDL::populateDiags(const char *oper, const char *errMsg, const char *string0, const char *string1,
                                   int retcode) {
  NAString reason("Reason: ");
  reason += errMsg;
  reason += " See next error for details: ";
  reason += GetCliGlobals()->getJniErrorStr();
  *CmpCommon::diags() << DgSqlCode(-5050) << DgString0(oper) << DgString1(reason);

  /*
  *CmpCommon::diags() << DgSqlCode(-8448)
                      << DgString0((char*)string0)
                      << DgString1(string1 ? (char*)string1 : "")
                      << DgInt0(retcode)
                      << DgString2((char*)GetCliGlobals()->getJniErrorStr());
  */

  return -1;
}

static short injectError(const char *errEnvvar) {
  if (getenv(errEnvvar)) {
    NAString reason("Error injected at ");
    reason += errEnvvar;
    reason += ".";
    *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("This") << DgString1(reason);

    return -1;
  }
  return 0;
}

short CmpSeabaseDDL::backupSubset(RelBackupRestore *brExpr, ExeCliInterface *cliInterface, NAString &currCatName,
                                  NAString &currSchName, CmpDDLwithStatusInfo *dws) {
  int retcode = 0;
  int cliRC = 0;

  long time0, time1, time2, time3, time4, time5, time6, time7, time8, time9;
  time0 = NA_JulianTimestamp();

  if (createProgressTable(cliInterface)) return -1;

  stepSeq = 1;
  operCnt = getBREIOperCount(cliInterface, "BACKUP", brExpr->backupTag().data());
  if (operCnt < 0) return -1;
  operCnt++;

  if (insertProgressTable(cliInterface, brExpr, "BACKUP", "Initialize", "Initiated", 0, 0) < 0) {
    return -1;
  }

  if (backupInit("BACKUP", FALSE) || validateInputNames(cliInterface, brExpr, TRUE, currCatName, currSchName)) {
    updateProgressTable(cliInterface, brExpr->backupTag().data(), "BACKUP", "Initialize", "Failed", -1, -1);
    return -1;
  }

  NAString objUIDsWherePred;
  NAString chunksPred;
  NAString lobChunksObjName;
  NAString schPrivsList;
  Queue *lobsQueue = NULL;
  Queue *indexesQueue = NULL;
  Queue *refsQueue = NULL;
  NABoolean schemasOnlyBackup = FALSE;
  retcode =
      genBRObjUIDsWherePred(brExpr, brExpr->schemaList(), brExpr->tableList(), brExpr->viewList(), brExpr->procList(),
                            brExpr->libList(), currCatName, currSchName, objUIDsWherePred, (char *)"_MD_", cliInterface,
                            lobsQueue, indexesQueue, refsQueue, &schPrivsList, schemasOnlyBackup);
  if (retcode < 0) return -1;

  retcode = verifyBRAuthority(objUIDsWherePred.data(), cliInterface, brExpr->showObjects(), TRUE, SEABASE_MD_SCHEMA,
                              TRUE, NULL /*owner*/);
  if (retcode < 0) {
    *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("BACKUP") << DgString1("Reason: No privilege.");
    return -1;
  }

  ExpHbaseInterface *ehi = NULL;
  NAString qTableName;
  TextVec mdTablesList;
  TextVec userTablesList;
  TextVec allTablesList;
  TextVec allTablesFlagsList;
  TextVec lobLocList;
  char query[1000 + objUIDsWherePred.length()];
  Queue *tableQueue = NULL;
  NAString backupSch;
  NABoolean xnWasStartedHere = FALSE;
  NAString lockedObjs;
  int backupThreads = CmpCommon::getDefaultNumeric(TRAF_BACKUP_PARALLEL);
  BackupAttrList *attrList = NULL;
  char *bkAttributeString = NULL;

  ehi = allocBRCEHI();
  if (ehi == NULL) {
    // Diagnostic already populated.
    return -1;
  }

  if (dws && brExpr->showObjects()) {
    retcode = processBRShowObjects(brExpr, NULL, objUIDsWherePred, cliInterface, dws);
    if (retcode < 0) return -1;

    return 0;
  }

  // check if operation is locked
  NAString lockedTag = ehi->lockHolder();
  if (lockedTag.length() > 0) {
    if (lockedTag == "?")
      populateDiags("BACKUP", "Error occurred while checking snapshot metadata. ", "ExpHbaseInterface::backupSubset",
                    NULL, -1);
    else {
      NAString lockedMsg = "Reason: Snapshot metadata is currently locked for Tag: " + lockedTag;
      *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("BACKUP") << DgString1(lockedMsg.data());
    }
    updateProgressTable(cliInterface, brExpr->backupTag().data(), "BACKUP", "Initialize", "Failed", -1, -1);
    return -1;
  }

  if (updateProgressTable(cliInterface, brExpr->backupTag().data(), "BACKUP", "Initialize", "Completed", 0, 100) < 0) {
    return -1;
  }

  time1 = NA_JulianTimestamp();
  long operStartTime = NA_JulianTimestamp();

  // check to see if this backup tag exists
  NAString backupType = ehi->getBackupType(brExpr->backupTag());
  if (!backupType.isNull())  // tag exists
  {
    if (brExpr->getOverride())  // override it
    {
      NABoolean dropAllDependentTags = FALSE;
      NAArray<HbaseStr> *backupList = NULL;
      if (backupType == "REGULAR") {
        dropAllDependentTags = TRUE;
        backupList = ehi->getLinkedBackupTags(brExpr->backupTag());
      }
      // override will drop all dependent tags
      retcode = deleteSnapshot(ehi, (char *)"BACKUP", brExpr->backupTag().data(), FALSE, TRUE, dropAllDependentTags);
      // Diagnostic already populated.
      if (retcode < 0) {
        ehi->close();
        return -1;
      }

      if (backupList && backupList->entries() > 0) {
        // when drop force, the backup metadata table had beed deleted from hbase
        // then we should be cleanup trafodion metadata
        retcode = cleanupLinkedBackupMD(cliInterface, backupList);
        if (retcode < 0) {
          ehi->close();
          return -1;
        }
      }
    } else {
      *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("BACKUP")
                          << DgString1("Reason: Specified tag already exists.");

      ehi->close();
      return -1;
    }
  }

  // create BACKUP MD schema
  retcode = createBackupMDschema(brExpr, cliInterface, backupSch, "BACKUP");
  if (retcode < 0) {
    *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("BACKUP")
                        << DgString1("Reason: Error occurred while creating Backup Metadata.");

    updateProgressTable(cliInterface, brExpr->backupTag().data(), "BACKUP", "Initialize", "Failed", -1, -1);
    return -1;
  }

  time2 = NA_JulianTimestamp();

  NABoolean backupSchExists = FALSE;
  if (retcode == 1) backupSchExists = TRUE;

  int totalSetupMDsteps = 4;
  int currSetupMDstep = 0;
  if (insertProgressTable(cliInterface, brExpr, "BACKUP", "SetupMetadata", "Initiated", totalSetupMDsteps,
                          currSetupMDstep) < 0) {
    return -1;
  }
  currSetupMDstep++;

  // check if backup MD exists and is locked. If it is, return error.
  // It needs to be unlocked before proceeding.
  NAString tag;
  NAString oper;
  retcode = checkIfBackupMDisLocked(brExpr, cliInterface, "BACKUP", oper, tag);
  if (retcode < 0) {
    *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("BACKUP")
                        << DgString1("Reason: Error occurred while checking for locked Backup Metadata.");
    updateProgressTable(cliInterface, brExpr->backupTag().data(), "BACKUP", "SetupMetadata", "Failed", -1, -1);
    return -1;
  }

  if (retcode == 1)  // Backup MD is locked
  {
    NAString details(" Details: OtherOperation=");
    details += oper + ", OtherTag=" + tag;
    if (NOT(tag == brExpr->backupTag())) {
      *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("BACKUP")
                          << DgString1(
                                 NAString("Reason: Stored tag in TEXT table does not match the tag that is part of "
                                          "Backup Metadata name. This should not happen and is an internal error. ") +
                                 details);
      return -1;
    } else if (NOT(oper == "BACKUP")) {
      *CmpCommon::diags()
          << DgSqlCode(-5050) << DgString0("BACKUP")
          << DgString1(
                 NAString("Reason: Backup Metadata is locked by a different operation which need to finish first.") +
                 details);
      return -1;
    } else if (NOT brExpr->getOverride()) {
      *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("BACKUP")
                          << DgString1(NAString("Reason: Backup Metadata is locked by another BACKUP operation. It "
                                                "needs to finish first or OVERRIDE option needs to be specified.") +
                                       details);
      return -1;
    }
  }

  if (updateProgressTable(cliInterface, brExpr->backupTag().data(), "BACKUP", "SetupMetadata", "Initiated",
                          currSetupMDstep, (currSetupMDstep * 100) / totalSetupMDsteps) < 0) {
    return -1;
  }
  currSetupMDstep++;

  // create backup metadata
  attrList = new (STMTHEAP) BackupAttrList(STMTHEAP);
  attrList->addOwner(ComUser::getCurrentUsername());
  if (attrList->addVersions() != 0) {
    *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("BACKUP")
                        << DgString1("Error returned retrieving version information.");
    return -1;
  }

  NAString userOrRoles;
  int numUR = 0;
  retcode = getUsersAndRoles(cliInterface, objUIDsWherePred.data(), SEABASE_MD_SCHEMA, numUR, userOrRoles);
  if (retcode < 0) {
    *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("BACKUP")
                        << DgString1("Error returned generating related users and roles.");
    return -1;
  }
  attrList->addUserAndRoles(userOrRoles.data());

  NAString errorMsg;
  retcode = attrList->pack(bkAttributeString, errorMsg);
  if (retcode < 0) {
    errorMsg.prepend("Error returned generating backup attribute string, ");
    *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("BACKUP") << DgString1(errorMsg.data());
    return -1;
  }

  NAString logMsg("Extended attributes stored for backup tag: ");
  logMsg += brExpr->backupTag() + ": " + bkAttributeString;
  QRLogger::log(CAT_SQL_EXE, LL_DEBUG, "%s", logMsg.data());

  // check if objects to be backed up are locked by another BR command
  retcode = checkIfObjectsLockedInMD(cliInterface, (char *)objUIDsWherePred.data(), (char *)SEABASE_MD_SCHEMA, "BACKUP",
                                     (char *)brExpr->backupTag().data(), lockedObjs);
  if (retcode != 0) {
    if (retcode < 0) {
      *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("BACKUP")
                          << DgString1("Reason: Error occurred while checking for locked objects in Metadata.");
      return -1;
    }

    if (retcode == 1) {
      *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("BACKUP")
                          << DgString1(
                                 "Reason: Some of the objects to be backed up are locked. This could be due to another "
                                 "backup or restore operation in progress.");

      return -1;
    } else if (retcode == 2) {
      *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("BACKUP")
                          << DgString1(
                                 "Reason: Some of the objects to be backed up are locked but are not part of the "
                                 "specified tag or were not locked for a BACKUP operation. They cannot be overridden. "
                                 "Use GET ALL LOCKED OBJECTS command to see the list of locked objects.");

      return -1;
    }  // override

    return -1;  // should not reach here
  }

  // lock backup MD
  retcode = lockBackupMD(brExpr, cliInterface, "BACKUP", brExpr->getOverride());
  if (retcode < 0) {
    *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("BACKUP")
                        << DgString1("Reason: Error occurred while locking Backup Metadata.");
    return -1;
  }

  if (injectError("BACKUP_ERR_INJECT_1")) {
    return -1;
  }

  time3 = NA_JulianTimestamp();

  // lock objects to be backed up
  if (lockObjectsInMD(cliInterface, "BACKUP", (char *)objUIDsWherePred.data(), (char *)SEABASE_MD_SCHEMA,
                      (char *)brExpr->backupTag().data(), schemasOnlyBackup)) {
    // retry lock once more in case there are concurrent operations (error 8616)
    QRLogger::log(CAT_SQL_EXE, LL_WARN, "Retrying lockObjectsInMD", logMsg.data());
    sleep(5);
    if (lockObjectsInMD(cliInterface, "BACKUP", (char *)objUIDsWherePred.data(), (char *)SEABASE_MD_SCHEMA,
                        (char *)brExpr->backupTag().data(), schemasOnlyBackup)) {
      // Unable to get lock, error out
      unlockBackupMD(brExpr, cliInterface, "BACKUP");
      *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("BACKUP")
                          << DgString1("Reason: Error returned while locking objects in MD.");
      return -1;
    }
  }

  // check if any truncate in progress
  int retry = 1;
  NAString truncateLockObj;
  while (retry <= 20) {
    truncateLockObj.clear();
    cliRC = isTruncateTableLockedInMD(cliInterface, truncateLockObj);
    if (cliRC != 0) {
      if (cliRC == 1)
        QRLogger::log(CAT_SQL_EXE, LL_WARN, "Truncate/purgedata %s is in progress, sleep 10s, retry count = %d",
                      truncateLockObj.data(), retry);
      else
        QRLogger::log(CAT_SQL_EXE, LL_WARN, "Error while doing isTruncateTableLockedInMD check, cliRC = %d", cliRC);
      sleep(10);
    } else
      break;
    ++retry;
  }
  if (retry > 20) {
    *CmpCommon::diags()
        << DgSqlCode(-5050) << DgString0("BACKUP")
        << DgString1("Reason: Another operation that will block backup is in progress and need to finish first.");
    retcode = -HBASE_BACKUP_NONFATAL_ERROR;
    goto label_return;
  }

  if (updateProgressTable(cliInterface, brExpr->backupTag().data(), "BACKUP", "SetupMetadata", "Initiated",
                          currSetupMDstep, (currSetupMDstep * 100) / totalSetupMDsteps) < 0) {
    unlockBackupMD(brExpr, cliInterface, "BACKUP");
    return -1;
  }
  currSetupMDstep++;

  time4 = NA_JulianTimestamp();

  // create list of all user HBase tablenames including namespace prefix.
  // This list will be sent to be backed up.
  str_sprintf(query,
              "select case when T.text is not null then trim(T.text) || ':' else '' end || trim(O.catalog_name) || '.' "
              "|| trim(O.schema_name) || '.' || trim(O.object_name), O.flags, NVL(C.object_uid, 0), "
              "trim(O.schema_name) from %s.\"%s\".%s O left join %s.\"%s\".%s T on O.object_uid = T.text_uid and "
              "T.text_type = %d left join %s.\"%s\".%s C on O.object_uid = C.object_uid and C.fs_data_type in "
              "(160,161) where %s and O.object_type in ('" COM_BASE_TABLE_OBJECT_LIT "', '" COM_INDEX_OBJECT_LIT
              "') and O.object_name not like 'TRAF_SAMPLE%%' group by 1,2,3,4",
              TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_OBJECTS, TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA,
              SEABASE_TEXT, COM_OBJECT_NAMESPACE, TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_COLUMNS,
              objUIDsWherePred.data());

  retcode = cliInterface->fetchAllRows(tableQueue, query, 0, FALSE, FALSE, TRUE);
  if (retcode < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_return;
  }

  retcode = copyBRSrcToTgt(brExpr, cliInterface, objUIDsWherePred.data(), SEABASE_MD_SCHEMA, SEABASE_PRIVMGR_SCHEMA,
                           backupSch.data(), backupSch.data(), &schPrivsList);
  if (retcode < 0) {
    *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("BACKUP")
                        << DgString1("Reason: Error occured during update of backup metadata.");

    goto label_return;
  }

  if (injectError("BACKUP_ERR_INJECT_2")) {
    return -1;
  }

  if (updateProgressTable(cliInterface, brExpr->backupTag().data(), "BACKUP", "SetupMetadata", "Initiated",
                          currSetupMDstep, (currSetupMDstep * 100) / totalSetupMDsteps) < 0) {
    return -1;
  }
  currSetupMDstep++;

  retcode = backupHistStats(cliInterface, brExpr->tableList(), (char *)backupSch.data());
  if (retcode < 0) {
    *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("BACKUP")
                        << DgString1("Reason: Error occured during update of hist stats.");

    goto label_return;
  }

  for (int i = 0; i < sizeof(backupMDs) / sizeof(BackupMDsInfo); i++) {
    qTableName = NAString(TRAF_RESERVED_NAMESPACE3) + ":" + NAString(backupMDs[i].catName) + "." + backupSch + "." +
                 NAString(backupMDs[i].objName);
    mdTablesList.push_back(qTableName.data());

    // First character indicates incremental table.
    // Second character indicates whether table name includes backup Tag name
    // with it. It is set 'Y' here because backup MD tables contain tag name
    // included with it.
    allTablesFlagsList.push_back("NY");

    if (backupMDs[i].hasLob) {
      NAString lobChunksObjName;
      if (genLobChunksTableName(cliInterface,
                                NULL,  // backupMDs[i].catName,
                                backupSch.data(), backupMDs[i].objName, backupSch.data(), lobChunksObjName) < 0)
        return -1;  // does not exist

      // prefix namespace
      lobChunksObjName.prepend(":");
      lobChunksObjName.prepend(TRAF_RESERVED_NAMESPACE3);
      mdTablesList.push_back(lobChunksObjName.data());
      allTablesFlagsList.push_back("NY");
      lobLocList.push_back("");
    }

    lobLocList.push_back("");
  }

  if (NOT brExpr->brMetadata()) {
    for (int idx = 0; idx < tableQueue->numEntries(); idx++) {
      OutputInfo *vi = (OutputInfo *)tableQueue->getNext();
      char *obj = NULL;
      int objLen = 0;
      vi->get(0, obj, objLen);

      userTablesList.push_back(obj);

      long flags = *(long *)vi->get(1);
      if (isMDflagsSet(flags, MD_OBJECTS_INCR_BACKUP_ENABLED))
        allTablesFlagsList.push_back("YN");
      else
        allTablesFlagsList.push_back("NN");

      long objUID = *(long *)vi->get(2);
      vi->get(3, obj, objLen);

      int lobFiles = 0;
      retcode = genLobLocation(objUID, obj, cliInterface, lobLocList, lobFiles, FALSE);
      if (retcode < 0) {
        goto label_return;
      }
    }
  }

  // include table binlog_reader
  if (CmpCommon::getDefault(TRAF_BACKUP_INCLUDE_BINLOG) == DF_ON) {
    QRLogger::log(CAT_SQL_EXE, LL_INFO, "Current backup %s include binlog table.", brExpr->backupTag().data());
    NAString binlog_table = NAString(TRAF_RESERVED_NAMESPACE5) + ":" + NAString(TRAFODION_SYSCAT_LIT) + "." +
                            NAString(SEABASE_DTM_SCHEMA) + "." + NAString(SEABASE_BINLOG_READER);
    userTablesList.push_back(binlog_table.data());
    allTablesFlagsList.push_back("NN");
    lobLocList.push_back("");
  }

  // create list of hdfs files for all tables with LOB columns

  allTablesList.insert(allTablesList.end(), mdTablesList.begin(), mdTablesList.end());
  allTablesList.insert(allTablesList.end(), userTablesList.begin(), userTablesList.end());

  time5 = NA_JulianTimestamp();

  if (updateProgressTable(cliInterface, brExpr->backupTag().data(), "BACKUP", "SetupMetadata", "Completed",
                          currSetupMDstep, (currSetupMDstep * 100) / totalSetupMDsteps) < 0) {
    return -1;
  }

  if (insertProgressTable(cliInterface, brExpr, "BACKUP", "BackupObjects", "Initiated", allTablesList.size(), 0) < 0) {
    return -1;
  }

  retcode = ehi->backupObjects(allTablesList, allTablesFlagsList, lobLocList, brExpr->backupTag(), bkAttributeString,
                               (brExpr->getIncremental() ? "INCREMENTAL" : "REGULAR"), backupThreads,
                               CmpCommon::getDefaultNumeric(TRAF_BREI_PROGRESS_UPDATE_DELAY));
  time6 = NA_JulianTimestamp();
  if (retcode < 0) {
    populateDiags("BACKUP", "Error returned from backupObjects method.", "ExpHbaseInterface::backupObjects", NULL, -1);

    deleteSnapshot(ehi, (char *)"BACKUP", (char *)brExpr->backupTag().data(), TRUE);

    updateProgressTable(cliInterface, brExpr->backupTag().data(), "BACKUP", "BackupObjects", "Failed", -1, -1);

    retcode = -HBASE_BACKUP_NONFATAL_ERROR;

    goto label_return;
  }

  if (injectError("BACKUP_ERR_INJECT_3")) {
    deleteSnapshot(ehi, (char *)"BACKUP", (char *)brExpr->backupTag().data(), TRUE);

    retcode = -HBASE_BACKUP_NONFATAL_ERROR;

    goto label_return;
  }

  if (injectError("BACKUP_ERR_INJECT_3_2")) {
    deleteSnapshot(ehi, (char *)"BACKUP", (char *)brExpr->backupTag().data(), TRUE);

    retcode = -1;

    goto label_return;
  }

  if (updateProgressTable(cliInterface, brExpr->backupTag().data(), "BACKUP", "BackupObjects", "Completed", -1, -1) <
      0) {
    return -1;
  }

  retcode = 0;

label_return:

  if ((retcode == 0) || (retcode == -HBASE_BACKUP_NONFATAL_ERROR)) {
    if (unlockObjectsInMD(cliInterface, (char *)objUIDsWherePred.data(), (char *)SEABASE_MD_SCHEMA,
                          schemasOnlyBackup)) {
      // Retry unlock once more in case there are concurrent operations (error 8616)
      sleep(5);
      QRLogger::log(CAT_SQL_EXE, LL_WARN, "Retrying unlockObjectsInMD", logMsg.data());
      if (unlockObjectsInMD(cliInterface, (char *)objUIDsWherePred.data(), (char *)SEABASE_MD_SCHEMA,
                            schemasOnlyBackup)) {
        *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("BACKUP")
                            << DgString1("Reason: Error returned while unlocking objects in MD.");
        retcode = -1;
      }
    }
  }

  if (injectError("BACKUP_ERR_INJECT_4")) {
    retcode = -1;
  }

  if ((retcode == 0) || (retcode == -HBASE_BACKUP_NONFATAL_ERROR)) {
    if (unlockBackupMD(brExpr, cliInterface, "BACKUP")) {
      retcode = -1;
    }
  }

  time7 = NA_JulianTimestamp();
  if (insertProgressTable(cliInterface, brExpr, "BACKUP", "Finalize", "Initiated", 0, 0) < 0) {
    return -1;
  }

  // If shared cache enabled, update cache for each affected schema, table or index
  if ((retcode == 0) && (NOT brExpr->showObjects())) {
    retcode = ehi->finalizeBackup(brExpr->backupTag(), (brExpr->getIncremental() ? "INCR" : "REGULAR"));
    if (retcode < 0) {
      populateDiags("BACKUP", "Error returned during finalization of backup.", "ExpHbaseInterface::finalizeBackup",
                    getHbaseErrStr(-retcode), -retcode);
    }
  }

  // drop backup schema before return
  if (brExpr->dropBackupMD()) {
    str_sprintf(query, "drop schema if exists %s.\"%s\" cascade;", TRAFODION_SYSCAT_LIT, backupSch.data());
    cliRC = cliInterface->executeImmediate(query);
    if (cliRC < 0) {
      str_sprintf(query, "cleanup schema %s.\"%s\";", TRAFODION_SYSCAT_LIT, backupSch.data());
      cliRC = cliInterface->executeImmediate(query);
      if (cliRC < 0) {
        cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
        retcode = -1;
      }
    }
  }  // cleanup existing schema
  else {
    if (truncateBackupMD(backupSch, cliInterface)) {
      retcode = -1;
    }
  }

  if ((retcode == 0) && (backupSchExists)) {
    // update redeftime of backup schema in OBJECTS table to indicate
    // backup time.
    str_sprintf(query,
                "update %s.\"%s\".%s set redef_time = %ld where catalog_name = '%s' and schema_name = '%s' and "
                "object_name = '__SCHEMA__'",
                TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_OBJECTS, operStartTime, TRAFODION_SYSCAT_LIT,
                backupSch.data());

    cliRC = cliInterface->executeImmediate(query);
    if (cliRC < 0) {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      retcode = -1;
    }
  }

  time8 = NA_JulianTimestamp();
  if (updateProgressTable(cliInterface, brExpr->backupTag().data(), "BACKUP", "Finalize", "Completed", 0, 100) < 0) {
    return -1;
  }

  // export backup, if needed
  int exportRetcode = 0;
  if ((retcode == 0) && brExpr->exportBackup()) {
    int exportImportThreads = CmpCommon::getDefaultNumeric(TRAF_EXPORT_IMPORT_PARALLEL);

    retcode = ehi->exportOrImportBackup(brExpr->backupTag(), TRUE, brExpr->getOverride(),
                                        brExpr->exportImportLocation().data(), exportImportThreads,
                                        CmpCommon::getDefaultNumeric(TRAF_BREI_PROGRESS_UPDATE_DELAY));
    if (retcode < 0) {
      NAString reasonStr;
      reasonStr =
          "Backup completed but export failed due to an error returned from exportOrImportBackup method. To redo just "
          "the export, issue a separate 'export backup' command with tag '";
      reasonStr += brExpr->backupTag();
      reasonStr += "'";
      populateDiags("EXPORT BACKUP", reasonStr, "ExpHbaseInterface::exportOrImportBackup", getHbaseErrStr(-retcode),
                    -retcode);
    }

    time9 = NA_JulianTimestamp();
  } else
    time9 = time8;

  if ((retcode == 0) && (brExpr->returnStatus())) {
    int blackBoxLen = 0;
    char *blackBox = NULL;

    Queue *statusQueue = NULL;
    cliInterface->initializeInfoList(statusQueue, TRUE);

    updateQueueWithOI(" ", statusQueue, STMTHEAP);
    updateQueueWithOI("Backup Status (HH:MM:SS)", statusQueue, STMTHEAP);
    updateQueueWithOI("========================", statusQueue, STMTHEAP);
    updateQueueWithOI(" ", statusQueue, STMTHEAP);

    char statusBuf[200];
    char timeStrBuf[100];

    ExExeUtilTcb::getTimeAsString((time1 - time0), timeStrBuf, TRUE);
    str_sprintf(statusBuf, "Backup Initialization       = %s", timeStrBuf);
    updateQueueWithOI(statusBuf, statusQueue, STMTHEAP);

    ExExeUtilTcb::getTimeAsString((time2 - time1), timeStrBuf, TRUE);
    str_sprintf(statusBuf, "Backup Metadata Creation    = %s", timeStrBuf);
    updateQueueWithOI(statusBuf, statusQueue, STMTHEAP);

    ExExeUtilTcb::getTimeAsString((time4 - time2), timeStrBuf, TRUE);
    str_sprintf(statusBuf, "Backup Objects Lock         = %s", timeStrBuf);
    updateQueueWithOI(statusBuf, statusQueue, STMTHEAP);

    ExExeUtilTcb::getTimeAsString((time5 - time4), timeStrBuf, TRUE);
    str_sprintf(statusBuf, "Backup Metadata Update      = %s", timeStrBuf);
    updateQueueWithOI(statusBuf, statusQueue, STMTHEAP);

    ExExeUtilTcb::getTimeAsString((time6 - time5), timeStrBuf, TRUE);
    str_sprintf(statusBuf, "Backup Snapshots Creation   = %s", timeStrBuf);
    updateQueueWithOI(statusBuf, statusQueue, STMTHEAP);

    ExExeUtilTcb::getTimeAsString((time7 - time6), timeStrBuf, TRUE);
    str_sprintf(statusBuf, "Backup Objects Unlock       = %s", timeStrBuf);
    updateQueueWithOI(statusBuf, statusQueue, STMTHEAP);

    ExExeUtilTcb::getTimeAsString((time8 - time7), timeStrBuf, TRUE);
    str_sprintf(statusBuf, "Backup Objects Finalization = %s", timeStrBuf);
    updateQueueWithOI(statusBuf, statusQueue, STMTHEAP);

    if (brExpr->exportBackup()) {
      ExExeUtilTcb::getTimeAsString((time9 - time8), timeStrBuf, TRUE);
      str_sprintf(statusBuf, "Backup Objects Export       = %s", timeStrBuf);
      updateQueueWithOI(statusBuf, statusQueue, STMTHEAP);
    }

    ExExeUtilTcb::getTimeAsString((time9 - time0), timeStrBuf, TRUE);
    str_sprintf(statusBuf, "Backup Total Time           = %s", timeStrBuf);
    updateQueueWithOI(statusBuf, statusQueue, STMTHEAP);

    updateQueueWithOI(" ", statusQueue, STMTHEAP);

    populateBlackBox(cliInterface, statusQueue, blackBoxLen, blackBox);

    dws->setBlackBoxLen(blackBoxLen);
    dws->setBlackBox(blackBox);

    dws->setStep(-1);
    dws->setSubstep(0);
    dws->setDone(TRUE);
  }

  if ((retcode == 0) && (brExpr->returnTag())) {
    int blackBoxLen = 0;
    char *blackBox = NULL;

    Queue *statusQueue = NULL;
    cliInterface->initializeInfoList(statusQueue, TRUE);

    updateQueueWithOI(brExpr->backupTag(), statusQueue, STMTHEAP);

    populateBlackBox(cliInterface, statusQueue, blackBoxLen, blackBox);

    dws->setBlackBoxLen(blackBoxLen);
    dws->setBlackBox(blackBox);

    dws->setStep(-1);
    dws->setSubstep(0);
    dws->setDone(TRUE);
  }

  ehi->close();
  ehi = NULL;

  return (retcode < 0 ? -1 : 0);

label_return2:
  endXnIfStartedHere(cliInterface, xnWasStartedHere, 0);
  return (retcode < 0 ? -1 : 0);
}

short CmpSeabaseDDL::updateBackupOperationMetrics(RelBackupRestore *brExpr, ExeCliInterface *cliInterface,
                                                  long operStartTime, long operEndTime) {
  int cliRC = 0;

  if (CmpCommon::getDefault(TRAF_BACKUP_REPOSITORY_SUPPORT) == DF_OFF) return 0;

  if (NOT((brExpr->backup()) || (brExpr->backupSystem()) || (brExpr->restore()) || (brExpr->dropBackup()) ||
          (brExpr->exportBackup()) || (brExpr->importBackup()))) {
    return 0;
  }

  // if backup repos doesn't exist, return
  long objUID = getObjectUID(cliInterface, TRAFODION_SYSTEM_CATALOG, SEABASE_REPOS_SCHEMA,
                              TRAF_BACKUP_OPERATION_METRICS, COM_BASE_TABLE_OBJECT_LIT, NULL, NULL, NULL, FALSE, FALSE);
  if (objUID < 0) return 0;  // does not exist

  NAString backupTag(brExpr->backupTag());

  char oper_name[32];
  char oper_type[32];
  strcpy(oper_name, " ");
  strcpy(oper_type, " ");

  if (brExpr->backupSystem()) {
    strcpy(oper_name, BACKUP_TRAFODION_LIT);
    strcpy(oper_type, OPER_TYPE_SYSTEM);
  } else if (brExpr->backup()) {
    strcpy(oper_name, BACKUP_TRAFODION_LIT);

    if (brExpr->getIncremental())
      strcpy(oper_type, OPER_TYPE_INCR_SUBSET);
    else
      strcpy(oper_type, OPER_TYPE_REGULAR_SUBSET);
  } else if (brExpr->restore())
    strcpy(oper_name, RESTORE_TRAFODION_LIT);
  else if (brExpr->dropBackup()) {
    strcpy(oper_name, DROP_BACKUP_LIT);

    if (brExpr->dropAllBackups()) {
      strcpy(oper_type, OPER_TYPE_ALL);

      // generate a dummy tag
      char dummyTag[40];
      backupTag = NAString("DROP_ALL_") + str_ltoa(brExpr->brUID(), dummyTag);
    }
  } else if (brExpr->exportBackup())
    strcpy(oper_name, EXPORT_BACKUP_LIT);
  else if (brExpr->importBackup())
    strcpy(oper_name, IMPORT_BACKUP_LIT);

  char query[5000];

  str_sprintf(
      query,
      "insert into " TRAFODION_SYSTEM_CATALOG ".\"" SEABASE_REPOS_SCHEMA "\"." TRAF_BACKUP_OPERATION_METRICS
      " values ('%s', %ld, converttimestamp(%ld), converttimestamp(%ld), %ld, '%s', '%s', '%s', %ld, %ld, %ld) ",
      backupTag.data(), brExpr->brUID(), operStartTime, operEndTime, (operEndTime - operStartTime), oper_name,
      oper_type, OPER_STATUS_COMPLETED, 0L, 0L, 0L);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  return 0;
}

short CmpSeabaseDDL::updateBackupObjectsMetrics(RelBackupRestore *brExpr, ExeCliInterface *cliInterface) {
  int cliRC = 0;

  if (CmpCommon::getDefault(TRAF_BACKUP_REPOSITORY_SUPPORT) == DF_OFF) return 0;

  if (NOT((brExpr->backup()) || (brExpr->backupSystem()) || (brExpr->restore()) || (brExpr->dropBackup()) ||
          (brExpr->exportBackup()) || (brExpr->importBackup()))) {
    return 0;
  }

  return 0;
}

short CmpSeabaseDDL::createSnapshotForIncrBackup(ExeCliInterface *cliInterface, NABoolean incrBackupEnabled,
                                                 const NAString &btNamespace, const NAString &catalogNamePart,
                                                 const NAString &schemaNamePart, const NAString &objectNamePart,
                                                 long objDataUID) {
  int enableSnapshot = CmpCommon::getDefaultNumeric(TM_SNAPSHOT_TABLE_CREATES);
  if (enableSnapshot == 0)  // No need to call JNI::createSnapshotForIncrBackup
    return 0;

  int retcode = 0;
  int cliRC = 0;

  if (NOT incrBackupEnabled) return 0;

  ExpHbaseInterface *ehi = allocBRCEHI();
  if (ehi == NULL) {
    // Diagnostic already populated.
    return -1;
  }

  NAString extNameForHbase = genHBaseObjName(catalogNamePart, schemaNamePart, objectNamePart, btNamespace);

  HbaseStr hbaseTable;
  hbaseTable.val = (char *)extNameForHbase.data();
  hbaseTable.len = extNameForHbase.length();

  std::vector<Text> vt;
  Text t(hbaseTable.val);
  vt.push_back(t);
  retcode = ehi->createSnapshotForIncrBackup(vt);
  if (retcode < 0) {
    populateDiags("BACKUP", "Error returned from createSnapshotForIncrBackup method.",
                  "ExpHbaseInterface::createSnapshotForIncrBackup", NULL, -1);

    deallocBRCEHI(ehi);
    return -1;
  }

  deallocBRCEHI(ehi);
  return 0;
}

short CmpSeabaseDDL::setHiatus(const NAString &hiatusObjectName, NABoolean resetHiatus, NABoolean createSnapIfNotExist,
                               NABoolean ignoreSnapIfNotExist) {
  int retcode = 0;
  int cliRC = 0;
  int parallelThreads = CmpCommon::getDefaultNumeric(TRAF_BACKUP_PARALLEL);

  ExpHbaseInterface *ehi = allocBRCEHI();
  if (ehi == NULL) {
    // Diagnostic already populated.
    return -1;
  }

  if (injectError("BACKUP_ERR_INJECT_BEFORE_HIATUS")) {
    deallocBRCEHI(ehi);
    return -1;
  }

  if (resetHiatus)
    retcode = ehi->clearHiatus(hiatusObjectName);
  else
    retcode = ehi->setHiatus(hiatusObjectName, false, createSnapIfNotExist, ignoreSnapIfNotExist, parallelThreads);

  if (retcode < 0) {
    populateDiags("BACKUP", "Error returned from setHiatus method.", "ExpHbaseInterface::setHiatus", NULL, -1);

    deallocBRCEHI(ehi);
    return -1;
  }

  deallocBRCEHI(ehi);
  return 0;
}

short CmpSeabaseDDL::backupSystem(RelBackupRestore *brExpr, ExeCliInterface *cliInterface, CmpDDLwithStatusInfo *dws) {
  short error = 0;
  short rc = 0;
  int retcode = 0;
  char query[1000];
  int cliRC = 0;
  Queue *objsQueue = NULL;
  TextVec tableList;
  TextVec tableFlagsList;
  TextVec lobLocList;
  int backupThreads = CmpCommon::getDefaultNumeric(TRAF_BACKUP_PARALLEL);
  BackupAttrList *attrList = NULL;
  char *bkAttributeString = NULL;
  NAString errorMsg;
  NAString logMsg;
  int pitEnabled = 0;
  char *lv_env = getenv("TM_USE_PIT_RECOVERY");
  if (lv_env) {
    pitEnabled = atoi(lv_env);
  }

  ExpHbaseInterface *ehi = allocBRCEHI();
  if (ehi == NULL) {
    // Diagnostic already populated.
    return -1;
  }

  // check to see if this backup tag exists. If it does, return error.
  // Skip the check for show objects or if override option is specified.
  NAString backupType = ehi->getBackupType(brExpr->backupTag());
  if ((NOT backupType.isNull()) && (NOT brExpr->showObjects()) && (NOT brExpr->getOverride())) {
    *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("BACKUP")
                        << DgString1("Reason: Specified tag already exists.");

    ehi->close();
    return -1;
  }

  // lock objects, dont lock if show objects
  retcode = backupInit("BACKUP", (NOT brExpr->showObjects()));
  if (retcode < 0) {
    ehi->close();
    return -1;
  }

  // check if operation is locked
  NAString lockedTag = ehi->lockHolder();
  if (lockedTag.length() > 0) {
    if (lockedTag == "?")
      populateDiags("BACKUP", "Error occurred while checking snapshot metadata. ", "ExpHbaseInterface::backupSystem",
                    NULL, -1);
    else {
      NAString lockedMsg = "Reason: Snapshot metadata is currently locked for Tag: " + lockedTag;
      *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("BACKUP") << DgString1(lockedMsg.data());
    }
    return -1;
  }

  // check for elevated privileges only
  retcode =
      verifyBRAuthority(NULL, cliInterface, brExpr->showObjects(), TRUE, SEABASE_MD_SCHEMA, FALSE, NULL /*owner*/);
  if (retcode < 0) {
    *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("BACKUP") << DgString1("Reason: No privilege.");
    goto label_return;
  }

  // create backup metadata
  attrList = new (STMTHEAP) BackupAttrList(STMTHEAP);
  attrList->addOwner(ComUser::getCurrentUsername());
  if (attrList->addVersions() != 0) {
    *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("BACKUP")
                        << DgString1("Error returned retrieving version information.");
    retcode = -1;
    goto label_return;
  }

  retcode = attrList->pack(bkAttributeString, errorMsg);
  if (retcode < 0) {
    errorMsg.prepend("Error returned generating backup attribute string, ");
    *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("BACKUP") << DgString1(errorMsg.data());
    goto label_return;
  }

  logMsg = "Extended attributes stored for backup tag ";
  logMsg += brExpr->backupTag() + ": " + bkAttributeString;
  QRLogger::log(CAT_SQL_EXE, LL_INFO, "%s", logMsg.data());

  if (brExpr->brMetadata()) {
    str_sprintf(query,
                "select case when T.text is not null then trim(T.text) || ':' when T.text is null and O.schema_name in "
                "('" SEABASE_MD_SCHEMA "') then '" TRAF_RESERVED_NAMESPACE1
                ":' else '' end || trim(catalog_name) || '.' ||  trim(schema_name) || '.' || trim(object_name) from "
                "%s.\"%s\".%s O left join %s.\"%s\".%s T on O.object_uid = T.text_uid and T.text_type = %d where "
                "O.catalog_name = '%s' and O.schema_name in('" SEABASE_MD_SCHEMA "', '" SEABASE_REPOS_SCHEMA
                "', '" SEABASE_PRIVMGR_SCHEMA "', '" SEABASE_TENANT_SCHEMA
                "') and O.object_type in ('" COM_BASE_TABLE_OBJECT_LIT "', '" COM_INDEX_OBJECT_LIT
                "') and O.object_name not like 'TRAF_SAMPLE%%'",
                TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_OBJECTS, TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA,
                SEABASE_TEXT, COM_OBJECT_NAMESPACE, TRAFODION_SYSCAT_LIT);
  } else {
    str_sprintf(
        query,
        "select case when T.text is not null then trim(T.text) || ':' when T.text is null and O.schema_name in "
        "('" SEABASE_MD_SCHEMA "') then '" TRAF_RESERVED_NAMESPACE1
        ":'else '' end || trim(O.catalog_name) || '.' ||  trim(O.schema_name) || '.' || trim(O.object_name), "
        "NVL(C.object_uid, 0), trim(O.schema_name) from %s.\"%s\".%s O left join %s.\"%s\".%s T on O.object_uid = "
        "T.text_uid and T.text_type = %d left join %s.\"%s\".%s C on O.object_uid = C.object_uid and C.fs_data_type in "
        "(160,161) where O.catalog_name = '%s' and schema_name not like '|_HV|_%%|_' escape '|'  and O.schema_name not "
        "like '|_HB|_MAP|_' escape '|' and schema_name not like 'VOLATILE_SCHEMA_%%' and schema_name not like "
        "'|_BACKUP|_%%' escape '|' and O.object_type in ('" COM_BASE_TABLE_OBJECT_LIT "', '" COM_INDEX_OBJECT_LIT
        "') and O.object_name not like 'TRAF_SAMPLE%%' group by 1,2,3 union all select trim(a), 0, '' from (get "
        "external hbase objects, match 'TRAF_RSRVD_%%') x(a) where a not like 'TRAF_RSRVD_7:%%' order by 1",
        TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_OBJECTS, TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_TEXT,
        COM_OBJECT_NAMESPACE, TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_COLUMNS, TRAFODION_SYSCAT_LIT);
  }

  cliInterface->initializeInfoList(objsQueue, TRUE);

  cliRC = cliInterface->fetchAllRows(objsQueue, query, 0, FALSE, FALSE, FALSE);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());

    retcode = -1;

    goto label_return;
  }

  if (dws && brExpr->showObjects()) {
    Queue *showObjsQueue = NULL;
    cliInterface->initializeInfoList(showObjsQueue, TRUE);

    updateQueueWithOI(" ", showObjsQueue, STMTHEAP);
    updateQueueWithOI("Objects to be backed up", showObjsQueue, STMTHEAP);
    updateQueueWithOI("=======================", showObjsQueue, STMTHEAP);
    updateQueueWithOI(" ", showObjsQueue, STMTHEAP);

    for (int idx = 0; idx < objsQueue->numEntries(); idx++) {
      char *obj = NULL;
      int objLen = 0;

      OutputInfo *vi = (OutputInfo *)objsQueue->getNext();

      vi->get(0, obj, objLen);
      updateQueueWithOI(obj, showObjsQueue, STMTHEAP, objLen);

      long objUID = *(long *)vi->get(1);
      if (objUID != 0) {
        vi->get(2, obj, objLen);
        lobLocList.clear();

        int lobFiles = 0;
        retcode = genLobLocation(objUID, obj, cliInterface, lobLocList, lobFiles, TRUE);
        for (int idx = 0; idx < lobLocList.size(); idx++) {
          Text &lobLoc = lobLocList[idx];
          updateQueueWithOI(lobLoc.c_str(), showObjsQueue, STMTHEAP);
        }
      }
    }  // for

    int blackBoxLen = 0;
    char *blackBox = NULL;

    populateBlackBox(cliInterface, showObjsQueue, blackBoxLen, blackBox);

    dws->setBlackBoxLen(blackBoxLen);
    dws->setBlackBox(blackBox);

    dws->setStep(-1);
    dws->setSubstep(0);
    dws->setDone(TRUE);

    ehi->close();
    return 0;
  }

  for (int idx = 0; idx < objsQueue->numEntries(); idx++) {
    char *obj = NULL;
    int objLen = 0;

    OutputInfo *vi = (OutputInfo *)objsQueue->getNext();

    vi->get(0, obj, objLen);
    tableList.push_back(obj);

    // for system backup, incremental table type flag and
    // tableNameIncludesTag flag does not matter. But setting the flags
    // here so that flags do no show up as null on java side.
    tableFlagsList.push_back("NN");

    long objUID = *(long *)vi->get(1);
    if (objUID == 0) {
      lobLocList.push_back("");
    } else {
      vi->get(2, obj, objLen);
      int lobFiles = 0;
      retcode = genLobLocation(objUID, obj, cliInterface, lobLocList, lobFiles, FALSE);
    }
  }  // for

  // SYSTEM backuptype includes all user objects and MD objects. This backup
  // is not stored in SNAPSHOT_META instead it is stored in /user/trafodion/backupsys
  // sandbox. Likewise for SYSTEM_META backuptype which includes only MD objects.
  // SYSTEM_PIT type of backups are essentially all user and MD objects and do
  // NOT include SQL BOX ( BACKUP_MD_* objects). This backup type is treated as
  // REGULAR backup type and stored in SNAPSHOT_META( because PIT looks for these
  // objects to associate mutations).
  // Because all of these backuptypes requires full system initilize dropped before
  // a restore, all of them have a SYSTEM keyword in their types.
  backupType = "SYSTEM";

  // Treat META as SYSTEM for now. Can enhance later.
  // if (brExpr->brMetadata())
  //  {
  //    backupType = "SYSTEM_META";
  //  }
  if (pitEnabled) {
    backupType = "SYSTEM_PIT";
  }

  retcode =
      ehi->backupObjects(tableList, tableFlagsList, lobLocList, brExpr->backupTag(), bkAttributeString, backupType,
                         backupThreads, CmpCommon::getDefaultNumeric(TRAF_BREI_PROGRESS_UPDATE_DELAY));
  if (retcode < 0) {
    populateDiags("BACKUP", "Error returned from backupObjects method.", "ExpHbaseInterface::backupObjects",
                  getHbaseErrStr(-retcode), -retcode);

    deleteSnapshot(ehi, (char *)"BACKUP", (char *)brExpr->backupTag().data(), TRUE);
  }

  if ((retcode == 0) && (NOT brExpr->showObjects())) {
    retcode = ehi->finalizeBackup(brExpr->backupTag(), backupType);
    if (retcode < 0) {
      populateDiags("BACKUP", "Error returned during finalization of backup.", "ExpHbaseInterface::finalizeBackup",
                    getHbaseErrStr(-retcode), -retcode);
    }
  }

label_return:
  // deallocate, not needed anymore.
  ehi->close();

  // unlockAll
  if (NOT brExpr->showObjects()) {
    unlockAll();
  }

  return (retcode < 0 ? -1 : 0);
}

short CmpSeabaseDDL::showBRObjects(RelBackupRestore *brExpr, NAString &qualBackupSch, NAArray<HbaseStr> *mdList,
                                   NAArray<HbaseStr> *objectsList, CmpDDLwithStatusInfo *dws,
                                   ExeCliInterface *cliInterface) {
  if ((!dws) || (NOT brExpr->showObjects())) return 0;

  if ((!mdList) && (!objectsList)) return 0;

  Queue *objsQueue = NULL;

  int mdEntries = 0;
  int numEntries = 0;
  if (mdList) {
    mdEntries = mdList->entries();
  }

  if (objectsList) {
    numEntries = objectsList->entries();
  }

  int blackBoxLen = 0;
  char *blackBox = NULL;

  cliInterface->initializeInfoList(objsQueue, TRUE);

  if (mdList) {
    updateQueueWithOI(" ", objsQueue, STMTHEAP);
    updateQueueWithOI("MetaData objects to be restored", objsQueue, STMTHEAP);
    updateQueueWithOI("===============================", objsQueue, STMTHEAP);
    updateQueueWithOI(" ", objsQueue, STMTHEAP);

    for (int i = 0; i < mdEntries; i++) {
      int len = mdList->at(i).len;
      char *val = mdList->at(i).val;

      char *brVal = new (STMTHEAP) char[len + 1];
      strncpy(brVal, val, len);
      brVal[len] = 0;

      OutputInfo *oi = new (STMTHEAP) OutputInfo(1);
      oi->insert(0, brVal, len + 1);

      objsQueue->insert(oi);
    }
  }

  if (objectsList) {
    if (NOT brExpr->brMetadata()) {
      updateQueueWithOI(" ", objsQueue, STMTHEAP);
      updateQueueWithOI("User objects to be restored", objsQueue, STMTHEAP);
      updateQueueWithOI("===========================", objsQueue, STMTHEAP);
      updateQueueWithOI(" ", objsQueue, STMTHEAP);
    } else {
      updateQueueWithOI(" ", objsQueue, STMTHEAP);
      updateQueueWithOI("User objects to be restored (Metadata only)", objsQueue, STMTHEAP);
      updateQueueWithOI("===========================================", objsQueue, STMTHEAP);
      updateQueueWithOI(" ", objsQueue, STMTHEAP);
    }

    for (int i = 0; i < numEntries; i++) {
      int len = objectsList->at(i).len;
      char *val = objectsList->at(i).val;

      char *brVal = new (STMTHEAP) char[len + 1];
      strncpy(brVal, val, len);
      brVal[len] = 0;

      if ((NOT qualBackupSch.isNull()) && (strstr(val, qualBackupSch.data()) != NULL)) continue;

      OutputInfo *oi = new (STMTHEAP) OutputInfo(1);
      oi->insert(0, brVal, len + 1);

      objsQueue->insert(oi);
    }
  }

  populateBlackBox(cliInterface, objsQueue, blackBoxLen, blackBox);

  dws->setBlackBoxLen(blackBoxLen);
  dws->setBlackBox(blackBox);

  dws->setStep(-1);
  dws->setSubstep(0);
  dws->setDone(TRUE);

  return 0;
}

// creates namespaces, if not already created,
// that will be needed to restore objects.
short CmpSeabaseDDL::createNamespacesForRestore(RelBackupRestore *brExpr, TextVec *schemaListTV, TextVec *tableListTV,
                                                NABoolean createReservedNS, ExpHbaseInterface *ehi,
                                                int restoreThreads) {
  // no need to create namespaces if objects are not being restored.
  if (brExpr->showObjects()) return 0;

  NAArray<HbaseStr> *objectsList = NULL;
  objectsList = ehi->restoreObjects(
      brExpr->backupTag(), schemaListTV, tableListTV, (brExpr->restoreToTS() ? brExpr->getTimestampVal() : ""),
      TRUE,  // show objects
      FALSE, FALSE, restoreThreads, CmpCommon::getDefaultNumeric(TRAF_BREI_PROGRESS_UPDATE_DELAY));

  // Remove Snapshot meta lock (if exists) for this tag
  NAString lockedTag;
  if (CmpCommon::getDefault(TRAF_BR_REMOVE_SM_LOCKS) == DF_ON) {
    if (unlockSnapshotMeta(ehi, brExpr->backupTag(), lockedTag) < 0) {
      populateDiags("RESTORE",
                    "Error occurred while unlocking snapshot metadata. "
                    "Perform GET ALL LOCKED OBJECTS to verify lock has been removed. ",
                    "ExpHbaseInterface::restoreSubset", NULL, -1);
      return -1;
    }
  }

  if (!objectsList) {
    if ((schemaListTV && schemaListTV->size() > 0) || (tableListTV && tableListTV->size() > 0)) {
      populateDiags("RESTORE", "Specified tag does not contain the specified objects or they could not be restored.",
                    "ExpHbaseInterface::restoreObjects", NULL, -1);
    } else {
      populateDiags("RESTORE", "Specified tag contains no objects or they could not be restored.",
                    "ExpHbaseInterface::restoreObjects", NULL, -1);
    }

    return -1;
  }

  std::vector<std::string> nsVector;
  for (int i = 0; i < objectsList->entries(); i++) {
    int len = objectsList->at(i).len;
    char *val = objectsList->at(i).val;
    std::string objectName(val, len);
    char *onstr = (char *)objectName.c_str();
    char *colonPos = strchr(onstr, ':');
    if (colonPos) {
      std::string ns = objectName.substr(0, (colonPos - onstr));
      nsVector.push_back(ns);
    }
  }

  if (!nsVector.empty()) {
    ExpHbaseInterface *ehihbase = allocEHI(COM_STORAGE_HBASE);
    if (ehihbase == NULL) return -1;

    std::sort(nsVector.begin(), nsVector.end());
    nsVector.erase(unique(nsVector.begin(), nsVector.end()), nsVector.end());

    for (int i = 0; i < nsVector.size(); i++) {
      NAString ns(nsVector[i]);
      if (createNamespace(ehihbase, ns, createReservedNS)) return -1;
    }

    ehihbase->close();
  }

  return 0;
}

short CmpSeabaseDDL::restoreSystem(RelBackupRestore *brExpr, ExeCliInterface *cliInterface, NAString &currCatName,
                                   NAString &currSchName, CmpDDLwithStatusInfo *dws, ExpHbaseInterface *ehi,
                                   int restoreThreads) {
  short retcode = 0;

  // Must be DB__ROOT to match privileges for initialize trafidion [, drop]
  if (!ComUser::isRootUserID()) {
    *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("RESTORE") << DgString1("Reason: No privilege.");
    return -1;
  }

  NAArray<HbaseStr> *objectsList = NULL;

  if ((NOT brExpr->showObjects()) && (NOT CmpCommon::context()->isUninitializedSeabase())) {
    *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("RESTORE")
                        << DgString1("Reason: Trafodion must be dropped before restoring a SYSTEM backup.");

    return -1;
  }

  if (brExpr->schemaList() || brExpr->tableList()) {
    *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("RESTORE")
                        << DgString1("Reason: Schema or Table list cannot be specified for a SYSTEM backup.");

    return -1;
  }

  // check if operation is locked
  NAString lockedTag = ehi->lockHolder();
  if (lockedTag.length() > 0) {
    if (lockedTag == "?")
      populateDiags("RESTORE", "Error occurred while checking snapshot metadata. ", "ExpHbaseInterface::restoreSystem",
                    NULL, -1);
    else {
      NAString lockedMsg = "Reason: Snapshot metadata is currently locked for Tag: " + lockedTag;
      *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("RESTORE") << DgString1(lockedMsg.data());
    }
    return -1;
  }

  // Get extended attributes
  BackupAttrList *attrList = new (STMTHEAP) BackupAttrList(STMTHEAP);
  NAString errorMsg;
  if (getExtendedAttributes(ehi, brExpr->backupTag(), attrList, errorMsg) < 0) {
    errorMsg.prepend("Reason: ");
    *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("RESTORE") << DgString1(errorMsg.data());
    return -1;
  }

  // Check that the backup metadata is compatible with the current release
  // If retcode == 1, then compatibility was not checked because extended
  // attribute information did not exist - continue anyway.
  retcode = checkCompatibility(attrList, errorMsg);
  if (retcode < 0) {
    errorMsg.prepend("Reason: ");
    *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("RESTORE") << DgString1(errorMsg.data());
    return -1;
  }

  if (NOT brExpr->showObjects()) {
    if (dropCreateReservedNamespaces(ehi, FALSE, TRUE)) return -1;

    if (createNamespacesForRestore(brExpr, NULL, NULL, TRUE, ehi, restoreThreads)) {
      return -1;
    }
  }

  objectsList = ehi->restoreObjects(brExpr->backupTag(), NULL, NULL,
                                    (brExpr->restoreToTS() ? brExpr->getTimestampVal() : ""), brExpr->showObjects(),
                                    FALSE,  // dont save current objects before restore
                                    FALSE,  // dont restore saved objects
                                    restoreThreads, CmpCommon::getDefaultNumeric(TRAF_BREI_PROGRESS_UPDATE_DELAY));

  // Remove Snapshot meta lock (if exists) for this tag
  if (CmpCommon::getDefault(TRAF_BR_REMOVE_SM_LOCKS) == DF_ON) {
    if (unlockSnapshotMeta(ehi, brExpr->backupTag(), lockedTag) < 0) {
      populateDiags("RESTORE",
                    "Error occurred while unlocking snapshot metadata. "
                    "Perform GET ALL LOCKED OBJECTS to verify lock has been removed. ",
                    "ExpHbaseInterface::restoreSubset", NULL, -1);
      return -1;
    }
  }

  if (!objectsList) {
    populateDiags("RESTORE", "Specified tag contains no objects or could not be restored.",
                  "ExpHbaseInterface::restoreObjects", NULL, -1);

    return -1;
  }

  if (brExpr->showObjects()) {
    NAString qualBackupSch;
    return showBRObjects(brExpr, qualBackupSch, NULL, objectsList, dws, cliInterface);
  }

  return 0;
}

// RETURN: 0, if objects are not locked.
//         1, if some objects are locked.
//         2, if some objects are locked and some are not part of this tag
//         -1, some other error.
short CmpSeabaseDDL::checkIfObjectsLockedInMD(ExeCliInterface *cliInterface, char *objUIDsWherePred, char *srcSch,
                                              const char *operation, char *backupTag, NAString &lockedObjs) {
  int cliRC = 0;
  Queue *objsQueue = NULL;

  char objsQry[2000 + (objUIDsWherePred ? strlen(objUIDsWherePred) : 0)];

  cliRC = cliInterface->holdAndSetCQDs(
      "NESTED_JOINS, MERGE_JOINS, HASH_JOINS, TRAF_BR_LOCKED_OBJECTS_QUERY_TYPE",
      "NESTED_JOINS 'OFF', MERGE_JOINS 'OFF', HASH_JOINS 'ON', TRAF_BR_LOCKED_OBJECTS_QUERY_TYPE '1' ");
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  if (objUIDsWherePred) {
    str_sprintf(objsQry,
                " select trim(T.text), trim(O.schema_name) || '.' || trim(O.object_name) from %s.\"%s\".%s O, "
                "%s.\"%s\".%s T where O.object_uid = T.text_uid and bitand(O.flags, %d) != 0 and T.text_type = 10 and "
                "O.object_uid in (select object_uid from %s.\"%s\".%s where %s and object_type in ('BT', 'IX') and "
                "object_name not in ('" HBASE_HIST_NAME "', '" HBASE_HISTINT_NAME "', '" HBASE_PERS_SAMP_NAME "')) ",
                TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_OBJECTS, TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA,
                SEABASE_TEXT, MD_OBJECTS_BR_IN_PROGRESS, TRAFODION_SYSCAT_LIT, srcSch, SEABASE_OBJECTS,
                objUIDsWherePred);
  } else {
    str_sprintf(objsQry,
                " select trim(T.text), trim(O.schema_name) || '.' || trim(O.object_name) from %s.\"%s\".%s O, "
                "%s.\"%s\".%s T where O.object_uid = T.text_uid and bitand(O.flags, %d) != 0 and T.text_type = 10 and "
                "O.object_type in ('BT', 'IX') and object_name not in ('" HBASE_HIST_NAME "', '" HBASE_HISTINT_NAME
                "', '" HBASE_PERS_SAMP_NAME "') ",
                TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_OBJECTS, TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA,
                SEABASE_TEXT, MD_OBJECTS_BR_IN_PROGRESS);
  }
  cliRC = cliInterface->fetchAllRows(objsQueue, objsQry, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    cliInterface->restoreCQDs("MERGE_JOINS, NESTED_JOINS, HASH_JOINS, TRAF_BR_LOCKED_OBJECTS_QUERY_TYPE");
    return -1;
  }
  cliInterface->restoreCQDs("MERGE_JOINS, NESTED_JOINS, HASH_JOINS, TRAF_BR_LOCKED_OBJECTS_QUERY_TYPE");

  if (objsQueue->numEntries() == 0) return 0;

  NABoolean someObjsNotPartOfThisTag = FALSE;
  for (int idx = 0; idx < objsQueue->numEntries(); idx++) {
    OutputInfo *vi = (OutputInfo *)objsQueue->getNext();

    NAString oper;
    NAString tag;
    if (extractFromOperTag((char *)vi->get(0), oper, tag)) continue;

    if ((NOT(tag == backupTag)) || (operation && (NOT(oper == operation)))) {
      someObjsNotPartOfThisTag = TRUE;
    }

    NAString objName((char *)vi->get(1));
    lockedObjs += objName;
    if (idx < (objsQueue->numEntries() - 1)) lockedObjs += ", ";
  }  // for

  // objects are locked.
  if (operation && strcmp(operation, "BACKUP") == 0) {
    updateProgressTable(cliInterface, backupTag, operation, "SetupMetadata", "Failed", -1, -1);
  }

  if (someObjsNotPartOfThisTag)
    return 2;
  else
    return 1;
}

short CmpSeabaseDDL::lockObjectsInMD(ExeCliInterface *cliInterface, const char *oper, char *objUIDsWherePred,
                                     char *srcSch, char *backupTag, NABoolean schemasOnlyBackup) {
  int cliRC = 0;

  NAString operTag(backupTag);

  if ((schemasOnlyBackup) && (CmpCommon::getDefault(TRAF_LOCK_BACKUP_SCHEMA) == DF_OFF)) {
    schemasOnlyBackup = FALSE;
  }

  NABoolean xnWasStartedHere = FALSE;
  if (beginXnIfNotInProgress(cliInterface, xnWasStartedHere)) return -1;

  char query[2000 + strlen(objUIDsWherePred)];

  cliRC = cliInterface->holdAndSetCQDs(
      "NESTED_JOINS, MERGE_JOINS, HASH_JOINS, TRAF_BR_LOCKED_OBJECTS_QUERY_TYPE",
      "NESTED_JOINS 'OFF', MERGE_JOINS 'OFF', HASH_JOINS 'ON', TRAF_BR_LOCKED_OBJECTS_QUERY_TYPE '1' ");
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  str_sprintf(query,
              "update %s.\"%s\".%s set flags = bitor(flags, %d) where object_uid in (select object_uid from "
              "%s.\"%s\".%s O where %s and object_type in ('BT', 'IX' %s) and object_name not in ('" HBASE_HIST_NAME
              "', '" HBASE_HISTINT_NAME "', '" HBASE_PERS_SAMP_NAME "') %s )",
              TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_OBJECTS, MD_OBJECTS_BR_IN_PROGRESS, TRAFODION_SYSCAT_LIT,
              srcSch, SEABASE_OBJECTS, objUIDsWherePred, (schemasOnlyBackup ? " , 'PS', 'SS' " : ""),
              (schemasOnlyBackup ? " and object_name = '__SCHEMA__' " : ""));
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_return;
  }

  operTag += NAString("(") + oper + ")";
  str_sprintf(query,
              "upsert into %s.\"%s\".%s select object_uid, %d, 0, 0, 0, '%s' from %s.\"%s\".%s O where %s and "
              "object_type in ('BT', 'IX' %s) and object_name not in ('" HBASE_HIST_NAME "', '" HBASE_HISTINT_NAME
              "', '" HBASE_PERS_SAMP_NAME "') %s ",
              TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_TEXT, COM_LOCKED_OBJECT_BR_TAG, operTag.data(),
              TRAFODION_SYSCAT_LIT, srcSch, SEABASE_OBJECTS, objUIDsWherePred,
              (schemasOnlyBackup ? " , 'PS', 'SS' " : ""),
              (schemasOnlyBackup ? " and object_name = '__SCHEMA__'  " : ""));
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_return;
  }

label_return:
  cliInterface->restoreCQDs("MERGE_JOINS, NESTED_JOINS, HASH_JOINS, TRAF_BR_LOCKED_OBJECTS_QUERY_TYPE");
  if (endXnIfStartedHere(cliInterface, xnWasStartedHere, (cliRC < 0 ? -1 : 0))) return -1;

  return (cliRC < 0 ? -1 : 0);
}

short CmpSeabaseDDL::unlockObjectsInMD(ExeCliInterface *cliInterface, char *objUIDsWherePred, char *srcSch,
                                       NABoolean schemasOnlyBackup) {
  int cliRC = 0;

  if ((schemasOnlyBackup) && (CmpCommon::getDefault(TRAF_LOCK_BACKUP_SCHEMA) == DF_OFF)) {
    schemasOnlyBackup = FALSE;
  }

  NABoolean xnWasStartedHere = FALSE;
  if (beginXnIfNotInProgress(cliInterface, xnWasStartedHere)) return -1;

  char query[2000 + strlen(objUIDsWherePred)];

  cliRC = cliInterface->holdAndSetCQDs(
      "NESTED_JOINS, MERGE_JOINS, HASH_JOINS, TRAF_BR_LOCKED_OBJECTS_QUERY_TYPE",
      "NESTED_JOINS 'OFF', MERGE_JOINS 'OFF', HASH_JOINS 'ON', TRAF_BR_LOCKED_OBJECTS_QUERY_TYPE '1' ");
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  str_sprintf(
      query,
      "update %s.\"%s\".%s set flags = bitand(flags, %d) where object_uid in (select object_uid from %s.\"%s\".%s O "
      "where %s and object_type in ('BT', 'IX', 'PS' %s) and object_name not in ('" HBASE_HIST_NAME
      "', '" HBASE_HISTINT_NAME "', '" HBASE_PERS_SAMP_NAME "') %s )",
      TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_OBJECTS, ~MD_OBJECTS_BR_IN_PROGRESS, TRAFODION_SYSCAT_LIT,
      srcSch, SEABASE_OBJECTS, objUIDsWherePred, (schemasOnlyBackup ? " , 'PS', 'SS' " : ""),
      (schemasOnlyBackup ? " and object_name = '__SCHEMA__' " : ""));
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_return;
  }

  str_sprintf(query,
              "delete from %s.\"%s\".%s where text_uid in (select object_uid from %s.\"%s\".%s O where %s and "
              "object_type in ('BT', 'IX', 'PS' %s) %s ) and text_type = %d ",
              TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_TEXT, TRAFODION_SYSCAT_LIT, srcSch, SEABASE_OBJECTS,
              objUIDsWherePred, (schemasOnlyBackup ? " , 'PS', 'SS' " : ""),
              (schemasOnlyBackup ? " and object_name = '__SCHEMA__' " : ""), COM_LOCKED_OBJECT_BR_TAG);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_return;
  }

label_return:
  cliInterface->restoreCQDs("MERGE_JOINS, NESTED_JOINS, HASH_JOINS, TRAF_BR_LOCKED_OBJECTS_QUERY_TYPE");
  if (endXnIfStartedHere(cliInterface, xnWasStartedHere, (cliRC < 0 ? -1 : 0))) return -1;

  return (cliRC < 0 ? -1 : 0);
}

short CmpSeabaseDDL::isTruncateTableLockedInMD(ExeCliInterface *cliInterface, NAString &lockedObjs) {
  int cliRC = 0;
  Queue *objsQueue = NULL;

  char objsQry[2000];
  str_sprintf(objsQry,
              " select trim(O.schema_name) || '.' || trim(O.object_name) from %s.\"%s\".%s O where bitand(O.flags, %d) "
              "!= 0 and bitand(O.flags, %d) != 0",
              TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_OBJECTS, MD_OBJECTS_BR_IN_PROGRESS,
              MD_OBJECTS_TRUNCATE_IN_PROGRESS);
  cliRC = cliInterface->fetchAllRows(objsQueue, objsQry, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  if (objsQueue->numEntries() == 0)
    return 0;
  else {
    for (int idx = 0; idx < objsQueue->numEntries(); idx++) {
      OutputInfo *vi = (OutputInfo *)objsQueue->getNext();
      NAString objName((char *)vi->get(0));
      lockedObjs += objName;
      if (idx < (objsQueue->numEntries() - 1)) lockedObjs += ", ";
    }
    QRLogger::log(CAT_SQL_EXE, LL_WARN, "Truncate %s is in progress", lockedObjs.data());
    return 1;
  }
}

short CmpSeabaseDDL::returnObjectsList(ExeCliInterface *cliInterface, const char *objUIDsWherePred, char *srcSch,
                                       NAArray<HbaseStr> *&objectsList) {
  int cliRC = 0;

  Queue *tableQueue = NULL;

  char query[3000 + strlen(objUIDsWherePred)];

  cliRC = cliInterface->holdAndSetCQDs(
      "NESTED_JOINS, MERGE_JOINS, HASH_JOINS, TRAF_BR_LOCKED_OBJECTS_QUERY_TYPE",
      "NESTED_JOINS 'OFF', MERGE_JOINS 'OFF', HASH_JOINS 'ON', TRAF_BR_LOCKED_OBJECTS_QUERY_TYPE '1' ");
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  str_sprintf(
      query,
      "select case when T.text is not null then trim(T.text) || ':' else '' end || trim(O.catalog_name) || '.' || "
      "trim(O.schema_name) || '.' || trim(O.object_name), O.flags from %s.\"%s\".%s O left join %s.\"%s\".%s T on "
      "O.object_uid = T.text_uid and T.text_type = %d where %s and O.object_type in ('" COM_BASE_TABLE_OBJECT_LIT
      "', '" COM_INDEX_OBJECT_LIT "') ",
      TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_OBJECTS, TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_TEXT,
      COM_OBJECT_NAMESPACE, objUIDsWherePred);

  cliRC = cliInterface->fetchAllRows(tableQueue, query, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    cliInterface->restoreCQDs("MERGE_JOINS, NESTED_JOINS, HASH_JOINS, TRAF_BR_LOCKED_OBJECTS_QUERY_TYPE");
    return -1;
  }

  cliInterface->restoreCQDs("MERGE_JOINS, NESTED_JOINS, HASH_JOINS, TRAF_BR_LOCKED_OBJECTS_QUERY_TYPE");
  if (tableQueue && (tableQueue->numEntries() > 0)) {
    objectsList = new (STMTHEAP) NAArray<HbaseStr>(STMTHEAP, tableQueue->numEntries());

    tableQueue->position();
    for (int i = 0; i < tableQueue->numEntries(); i++) {
      OutputInfo *vi = (OutputInfo *)tableQueue->getNext();
      char *obj = NULL;
      int objLen = 0;

      vi->get(0, obj, objLen);

      HbaseStr hbs;
      hbs.val = obj;
      hbs.len = objLen;

      objectsList->insert(i, hbs);
    }
  }

  return 0;
}

short CmpSeabaseDDL::restoreSubset(RelBackupRestore *brExpr, ExeCliInterface *cliInterface, NAString &currCatName,
                                   NAString &currSchName, CmpDDLwithStatusInfo *dws, ExpHbaseInterface *ehi,
                                   int restoreThreads) {
  int retcode = 0;
  int cliRC = 0;

  if (brExpr->restoreToTS()) {
    NAString restoreToTsTag = ehi->getRestoreToTsBackupTag(brExpr->getTimestampVal().data());
    if (restoreToTsTag.isNull())  // error case
    {
      NAString originTime = brExpr->getTimestampVal();
      // convert time from gmt to local to display
      char localStrBuf[100];
      ComDiagsArea *diags = CmpCommon::diags();
      ExpDatetime::convAsciiDatetimeToUtcOrLocal(
          const_cast<char *>(originTime.data()), originTime.length(), localStrBuf, originTime.length(),
          (long)CmpCommon::context()->gmtDiff() * 60 * 1000000, FALSE, STMTHEAP, &diags);
      localStrBuf[10] = ':';  // add ':' separator between date and time
      localStrBuf[originTime.length()] = 0;
      NAString lockedMsg = "Error returned from getRestoreToTsBackupTag while processing to_timestamp value '" +
                           NAString(localStrBuf) + "'.";

      populateDiags("RESTORE TO TIMESTAMP", lockedMsg, "getRestoreToTsBackupTag", NULL, -1);

      return -1;
    }

    brExpr->setBackupTag(restoreToTsTag);
  }

  if (createProgressTable(cliInterface)) return -1;

  stepSeq = 1;
  operCnt = getBREIOperCount(cliInterface, "RESTORE", brExpr->backupTag().data());
  if (operCnt < 0) return -1;
  operCnt++;
  if (insertProgressTable(cliInterface, brExpr, "RESTORE", "Initialize", "Initiated", 0, 0) < 0) {
    return -1;
  }

  if (validateInputNames(cliInterface, brExpr, FALSE, currCatName, currSchName)) return -1;

  TextVec mdObjectsTV;
  TextVec mdListTV;
  TextVec schemaListTV;
  TextVec tableListTV;
  TextVec lobListTV;
  TextVec indexListTV;
  TextVec allTablesListTV;

  NAString objUIDsWherePred;
  Queue *objsQueue = NULL;
  Queue *lobsQueue = NULL;
  Queue *indexesQueue = NULL;
  Queue *refsQueue = NULL;
  NAArray<HbaseStr> *mdList = NULL;
  NAArray<HbaseStr> *mdObjsList = NULL;
  NAArray<HbaseStr> *objectsList = NULL;
  NAString backupType;
  NAString indexesObjs;
  NABoolean xnWasStartedHere = FALSE;
  NAString lockedObjs;
  std::map<int, NAString> unmatchedUserID;

  SharedCacheDDLInfoList sharedCacheList(STMTHEAP);
  // for mantis 20917, cleanup querycache before restore
  CmpCommon::context()->getQueryCache()->cleanupUserQueryCache();

  // backup schema name:  _BACKUP_SCH_<backuptag>
  NAString backupSch;
  genBackupSchemaName(brExpr->backupTag(), backupSch);

  NAString qualBackupSch = NAString(TRAFODION_SYSCAT_LIT) + "." + backupSch.data();

  if (CmpCommon::context()->isUninitializedSeabase()) {
    *CmpCommon::diags() << DgSqlCode(CmpCommon::context()->uninitializedSeabaseErrNum());

    *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("RESTORE")
                        << DgString1("Reason: Trafodion must be initialized.");

    return -1;
  }

  // check if operation is locked
  NAString lockedTag = ehi->lockHolder();
  if (lockedTag.length() > 0) {
    if (lockedTag == "?")
      populateDiags("RESTORE", "Error occurred while checking snapshot metadata. ", "ExpHbaseInterface::restoreSubset",
                    NULL, -1);
    else {
      NAString lockedMsg = "Reason: Snapshot metadata is currently locked for Tag: " + lockedTag;
      *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("RESTORE") << DgString1(lockedMsg.data());
    }
    updateProgressTable(cliInterface, brExpr->backupTag().data(), "RESTORE", "Initialize", "Failed", -1, -1);
    return -1;
  }

  // check if backup MD exists and is locked. If it is, return error.
  // It needs to be unlocked before proceeding.
  NAString tag;
  NAString oper;
  retcode = checkIfBackupMDisLocked(brExpr, cliInterface, "RESTORE", oper, tag);

  if (retcode < 0) {
    *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("RESTORE")
                        << DgString1("Reason: Error occurred while checking for locked Backup Metadata.");
    updateProgressTable(cliInterface, brExpr->backupTag().data(), "RESTORE", "Initialize", "Failed", -1, -1);
    return -1;
  }

  if (retcode == 1)  // Backup MD is locked
  {
    NAString details("Details: OtherOperation=");
    details += oper + ", OtherTag=" + tag;
    if (NOT(tag == brExpr->backupTag())) {
      *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("RESTORE")
                          << DgString1(
                                 NAString("Reason: Stored tag in TEXT table does not match the tag that is part of "
                                          "Backup Metadata name. This should not happen and is an internal error. ") +
                                 details);
    } else {
      *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("RESTORE")
                          << DgString1(NAString("Reason: Backup Metadata is locked by another Backup or Restore "
                                                "operation which need to finish first.") +
                                       details);
    }

    updateProgressTable(cliInterface, brExpr->backupTag().data(), "RESTORE", "Initialize", "Failed", -1, -1);
    return -1;
  }

  if (updateProgressTable(cliInterface, brExpr->backupTag().data(), "RESTORE", "Initialize", "Completed", 0, 100) < 0) {
    return -1;
  }

  if (retcode == 0)  // backup md doesn't exist
  {
    // create BACKUP MD schema
    retcode = createBackupMDschema(brExpr, cliInterface, backupSch, "RESTORE");
    if (retcode < 0) return -1;
  }

  // Get extended attributes
  BackupAttrList *attrList = new (STMTHEAP) BackupAttrList(STMTHEAP);
  NAString errorMsg;
  if (getExtendedAttributes(ehi, brExpr->backupTag(), attrList, errorMsg) < 0) {
    errorMsg.prepend("Reason: ");
    *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("RESTORE") << DgString1(errorMsg.data());
    return -1;
  }

  // If check for compatibility
  if (checkCompatibility(attrList, errorMsg) < 0) {
    errorMsg.prepend("Reason: ");
    *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("RESTORE") << DgString1(errorMsg.data());
    return -1;
  }

  if (attrList->getAttrByKey(BackupAttr::BACKUP_ATTR_USER_ROLES) != NULL) {
    NAString lackedUR;
    retcode = checkUsersAndRoles(cliInterface, attrList->getAttrByKey(BackupAttr::BACKUP_ATTR_USER_ROLES), lackedUR,
                                 unmatchedUserID);
    if (retcode < 0) {
      errorMsg += "Reason: lack user or role: ";
      errorMsg += lackedUR;
      *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("RESTORE") << DgString1(errorMsg.data());
      return -1;
    }
  }

  // See if current user is owner or has elevated privs
  retcode = verifyBRAuthority(NULL, cliInterface, brExpr->showObjects(), FALSE, backupSch.data(), FALSE,
                              attrList->getAttrByKey(BackupAttr::BACKUP_ATTR_OWNER));
  if (retcode < 0) {
    *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("RESTORE") << DgString1("Reason: No privilege.");
    return -1;
  }

  if (NOT brExpr->showObjects()) {
    // lock backup MD
    retcode = lockBackupMD(brExpr, cliInterface, "RESTORE", FALSE);
    if (retcode < 0) {
      *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("RESTORE")
                          << DgString1("Reason: Error occurred while locking Backup Metadata.");
      return -1;
    }
  }

  NABoolean restoreAllObjs = FALSE;
  if ((NOT brExpr->showObjects()) && (!brExpr->schemaList() && !brExpr->tableList() && !brExpr->viewList() &&
                                      !brExpr->procList() && !brExpr->libList()))
    restoreAllObjs = TRUE;

  if (insertProgressTable(cliInterface, brExpr, "RESTORE", (restoreAllObjs ? "RestoreObjects" : "RestoreMetadata"),
                          "Initiated", 0, 0) < 0) {
    return -1;
  }

  NABoolean saveBeforeRestore = TRUE;
  if (brExpr->fastRecovery())
    saveBeforeRestore = FALSE;
  else {
    saveBeforeRestore = (restoreAllObjs ? TRUE : FALSE);
  }
  // restore backup MD
  mdListTV.push_back(qualBackupSch.data());
  mdList = ehi->restoreObjects(brExpr->backupTag(), (restoreAllObjs ? NULL : &mdListTV), NULL,
                               (brExpr->restoreToTS() ? brExpr->getTimestampVal() : ""), brExpr->showObjects(),
                               saveBeforeRestore,
                               FALSE,  // dont restore saved objects
                               restoreThreads, CmpCommon::getDefaultNumeric(TRAF_BREI_PROGRESS_UPDATE_DELAY));

  // Remove Snapshot meta lock (if exists) for this tag
  if (CmpCommon::getDefault(TRAF_BR_REMOVE_SM_LOCKS) == DF_ON) {
    if (unlockSnapshotMeta(ehi, brExpr->backupTag(), lockedTag) < 0) {
      populateDiags("RESTORE",
                    "Error occurred while unlocking snapshot metadata. "
                    "Perform GET ALL LOCKED OBJECTS to verify lock has been removed. ",
                    "ExpHbaseInterface::restoreSubset", NULL, -1);
      return -1;
    }
  }

  if (!mdList) {
    populateDiags("RESTORE", "Specified tag contains no Backup Metadata objects or they could not be restored.",
                  "ExpHbaseInterface::restoreObjects", NULL, -1);

    updateProgressTable(cliInterface, brExpr->backupTag().data(), "RESTORE", "RestoreObjects", "Failed", -1, -1);

    return -1;
  }

  if (injectError("RESTORE_ERR_INJECT_1")) {
    return -1;
  }

  // need to restore OBJECTS and INDEXES metadata objects to be able to
  // get dependent objects.
  if ((brExpr->showObjects()) && (NOT restoreAllObjs)) {
    NAString obj = qualBackupSch + "." + "" SEABASE_OBJECTS "";
    mdObjectsTV.push_back(obj.data());
    obj = qualBackupSch + "." + "" SEABASE_INDEXES "";
    mdObjectsTV.push_back(obj.data());
    obj = qualBackupSch + "." + "" SEABASE_TEXT "";
    mdObjectsTV.push_back(obj.data());

    mdObjsList = ehi->restoreObjects(brExpr->backupTag(), NULL, &mdObjectsTV,
                                     (brExpr->restoreToTS() ? brExpr->getTimestampVal() : ""),
                                     FALSE,  //          brExpr->showObjects(),
                                     FALSE,  // dont save current objects before restore
                                     FALSE,  // dont restore saved objects
                                     restoreThreads, CmpCommon::getDefaultNumeric(TRAF_BREI_PROGRESS_UPDATE_DELAY));

    // Remove Snapshot meta lock (if exists) for this tag
    if (CmpCommon::getDefault(TRAF_BR_REMOVE_SM_LOCKS) == DF_ON) {
      if (unlockSnapshotMeta(ehi, brExpr->backupTag(), lockedTag) < 0) {
        populateDiags("RESTORE",
                      "Error occurred while unlocking snapshot metadata. "
                      "Perform GET ALL LOCKED OBJECTS to verify lock has been removed. ",
                      "ExpHbaseInterface::restoreSubset", NULL, -1);
        return -1;
      }
    }

    if (!mdObjsList) {
      populateDiags("RESTORE", " Backup Metadata objects could not be restored.", "ExpHbaseInterface::restoreObjects",
                    NULL, -1);

      return -1;
    }
  }

  if (NOT brExpr->showObjects() && unmatchedUserID.size() > 0) {
    int retcode = fixBackupAuthIDs(cliInterface, backupSch, unmatchedUserID);
    if (retcode < 0) {
      *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("RESTORE")
                          << DgString1("Reason: Error occured during fixBackupAuthIDs.");

      // call restoreObjects to restore original objects
      objectsList = ehi->restoreObjects(brExpr->backupTag(), NULL, NULL, FALSE, FALSE,
                                        FALSE,                               // dont save current objects before restore
                                        (saveBeforeRestore ? TRUE : FALSE),  // restore saved objects
                                        restoreThreads, CmpCommon::getDefaultNumeric(TRAF_BREI_PROGRESS_UPDATE_DELAY));

      if (!objectsList) return -1;

      if (unlockSnapshotMeta(ehi, brExpr->backupTag(), lockedTag) < 0) {
        populateDiags("RESTORE",
                      "Error occurred while unlocking snapshot metadata. "
                      "Perform GET ALL LOCKED OBJECTS to verify lock has been removed. ",
                      "ExpHbaseInterface::restoreSubset", NULL, -1);
        return -1;
      }
      // unlock the BackupMD
      if (NOT brExpr->showObjects()) unlockBackupMD(brExpr, cliInterface, "RESTORE");

      // remove details from HBase/Zookeeper
      NAString backupType = ehi->getBackupType(brExpr->backupTag());
      retcode = ehi->finalizeRestore(brExpr->backupTag(), backupType + "_(CLEANUP)");
      if (retcode < 0) {
        populateDiags("RESTORE", "Error returned during finalization of restore.", "ExpHbaseInterface::finalizeRestore",
                      getHbaseErrStr(-retcode), -retcode);
      }

      return -1;
    }
  }

  NABoolean schemasOnlyBackup = FALSE;
  retcode =
      genBRObjUIDsWherePred(brExpr, brExpr->schemaList(), brExpr->tableList(), brExpr->viewList(), brExpr->procList(),
                            brExpr->libList(), currCatName, currSchName, objUIDsWherePred, (char *)backupSch.data(),
                            cliInterface, lobsQueue, indexesQueue, refsQueue, NULL, schemasOnlyBackup);
  if (retcode < 0) return -1;

  if (brExpr->schemaList()) {
    for (CollIndex i = 0; i < brExpr->schemaList()->entries(); i++) {
      ElemDDLQualName *dn = ((*brExpr->schemaList())[i])->castToElemDDLQualName();

      NAString n(dn->getQualifiedName().getSchemaName());
      n += NAString(".") + dn->getQualifiedName().getObjectName();

      schemaListTV.push_back(n.data());
    }
  }

  if (brExpr->tableList()) {
    for (CollIndex i = 0; i < brExpr->tableList()->entries(); i++) {
      ElemDDLQualName *dn = ((*brExpr->tableList())[i])->castToElemDDLQualName();

      NAString tableName = "";
      if (NOT dn->getQualifiedName().getCatalogName().isNull())
        tableName += dn->getQualifiedName().getCatalogName() + ".";
      if (NOT dn->getQualifiedName().getSchemaName().isNull())
        tableName += dn->getQualifiedName().getSchemaName() + ".";
      tableName += dn->getQualifiedName().getObjectName();

      NAString n(TRAFODION_SYSCAT_LIT);
      if (isHBaseNativeTable(tableName)) {
        if (NOT dn->getQualifiedName().getCatalogName().isNull())
          n = tableName;
        else
          n += NAString(".") + tableName;
      } else {
        n += NAString(".") + dn->getQualifiedName().getSchemaName() + "." + dn->getQualifiedName().getObjectName();
      }

      tableListTV.push_back(n.data());
    }
  }

  if (lobsQueue) {
    lobsQueue->position();
    for (int idx = 0; idx < lobsQueue->numEntries(); idx++) {
      OutputInfo *vi = (OutputInfo *)lobsQueue->getNext();
      char *obj = NULL;
      int objLen = 0;

      vi->get(0, obj, objLen);

      NAString n(TRAFODION_SYSCAT_LIT);
      n += NAString(".") + obj;
      lobListTV.push_back(n.data());
    }
  }

  if (indexesQueue) {
    indexesQueue->position();
    for (int idx = 0; idx < indexesQueue->numEntries(); idx++) {
      OutputInfo *vi = (OutputInfo *)indexesQueue->getNext();
      char *obj = NULL;
      int objLen = 0;

      vi->get(0, obj, objLen);

      NAString n(TRAFODION_SYSCAT_LIT);
      n += NAString(".") + obj;
      indexListTV.push_back(n.data());
    }
  }

  allTablesListTV.insert(allTablesListTV.end(), tableListTV.begin(), tableListTV.end());
  allTablesListTV.insert(allTablesListTV.end(), lobListTV.begin(), lobListTV.end());
  allTablesListTV.insert(allTablesListTV.end(), indexListTV.begin(), indexListTV.end());

  // check if objects being restored are locked by another BR command
  retcode = checkIfObjectsLockedInMD(cliInterface, (char *)objUIDsWherePred.data(), (char *)backupSch.data(), "RESTORE",
                                     (char *)brExpr->backupTag().data(), lockedObjs);
  if (retcode != 0) {
    if (retcode == 1) {
      *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("RESTORE")
                          << DgString1(
                                 "Reason: Some of the objects being restored are locked. This could be due to another "
                                 "backup or restore operation in progress.");
    } else {
      *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("RESTORE")
                          << DgString1("Reason: Error occurred while checking for locked objects in Metadata.");
    }

    return -1;
  }

  if (NOT brExpr->showObjects()) {
    if (beginXnIfNotInProgress(cliInterface, xnWasStartedHere)) return -1;

    if (NOT brExpr->fastRecovery()) {
      // m16641: invalidate all the source objects
      if (invalidateTgtObjects(cliInterface, (char *)objUIDsWherePred.data(), (char *)SEABASE_MD_SCHEMA,
                               (char *)backupSch.data(), sharedCacheList)) {
        *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("RESTORE")
                            << DgString1("Reason: Error returned while invalidating objects in MD.");

        retcode = -1;
        goto label_return;
      }
    }

    // remove existing objects from MD before restoring them.
    if (getenv("BACKUP_CLEANUP"))
      retcode = cleanupTgtObjectsFromMD(cliInterface, (char *)objUIDsWherePred.data(), (char *)backupSch.data());
    else
      retcode = deleteTgtObjectsFromMD(brExpr, cliInterface, objUIDsWherePred, (char *)SEABASE_MD_SCHEMA,
                                       (char *)SEABASE_PRIVMGR_SCHEMA, (char *)backupSch.data());
    if (retcode < 0) goto label_return;

    // copy metadata info from "_BACKUP_*" schema to current MD.
    retcode = copyBRSrcToTgt(brExpr, cliInterface, objUIDsWherePred.data(), backupSch.data(), backupSch.data(),
                             SEABASE_MD_SCHEMA, SEABASE_PRIVMGR_SCHEMA, NULL);
    if (retcode < 0) {
      *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("RESTORE")
                          << DgString1("Reason: Error occured during update from backup metadata.");
      // At this point , we have an outstanding transaction.
      // And BackupMD is locked.
      goto label_return;
    }

    // check inconsistent authID after copy privileges data into metadata
    retcode = checkBackupAuthIDs(cliInterface);
    if (retcode != 0) {
      *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("RESTORE")
                          << DgString1("Reason: Error occured during checkBackupAuthIDs.");
      goto label_return;
    }

    // lock objects to be restored
    if (lockObjectsInMD(cliInterface, "RESTORE", (char *)objUIDsWherePred.data(), (char *)backupSch.data(),
                        (char *)brExpr->backupTag().data(), FALSE)) {
      *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("RESTORE")
                          << DgString1("Reason: Error returned while locking objects in MD.");

      retcode = -1;
      goto label_return;
    }

    if (injectError("RESTORE_ERR_INJECT_2")) {
      retcode = -1;
      goto label_return;
    }

    if (endXnIfStartedHere(cliInterface, xnWasStartedHere, 0)) {
      *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("RESTORE")
                          << DgString1("Reason: Error returned from Transaction subsystem.");

      return -1;
    }
  }  // not showObjects

  if (NOT restoreAllObjs) {
    if (updateProgressTable(cliInterface, brExpr->backupTag().data(), "RESTORE", "RestoreMetadata", "Completed", -1,
                            -1) < 0) {
      return -1;
    }

    if (insertProgressTable(cliInterface, brExpr, "RESTORE", "RestoreObjects", "Initiated", 0, 0) < 0) {
      return -1;
    }
  }

  if ((NOT restoreAllObjs) && (NOT brExpr->brMetadata())) {
    if (brExpr->fastRecovery())
      saveBeforeRestore = FALSE;
    else {
      saveBeforeRestore = (NOT brExpr->showObjects() ? TRUE : FALSE);
    }
    // This is the actual restore of user tables.
    // Note that the tables being restore is saved by setting flag to TRUE.
    objectsList = ehi->restoreObjects(brExpr->backupTag(), (schemaListTV.size() > 0 ? &schemaListTV : NULL),
                                      (allTablesListTV.size() > 0 ? &allTablesListTV : NULL),
                                      (brExpr->restoreToTS() ? brExpr->getTimestampVal() : ""), brExpr->showObjects(),
                                      saveBeforeRestore,  // save current objects before restore
                                      FALSE,              // dont restore saved objects
                                      restoreThreads, CmpCommon::getDefaultNumeric(TRAF_BREI_PROGRESS_UPDATE_DELAY));

    if (!objectsList) {
      NAString reason;
      if ((schemaListTV.size() > 0) || (allTablesListTV.size() > 0)) {
        populateDiags("RESTORE", "Specified tag does not contain the specified objects or they could not be restored.",
                      "ExpHbaseInterface::restoreObjects", NULL, -1);
      } else {
        populateDiags("RESTORE", "Specified tag contains no objects or they could not be restored.",
                      "ExpHbaseInterface::restoreObjects", NULL, -1);
      }

      updateProgressTable(cliInterface, brExpr->backupTag().data(), "RESTORE", "RestoreObjects", "Failed", -1, -1);
      // cleanup lock for current tag
      cleanupLockForTag(brExpr, cliInterface, ehi);
      return -1;
    }
  }

  if (updateProgressTable(cliInterface, brExpr->backupTag().data(), "RESTORE", "RestoreObjects", "Completed", -1, -1) <
      0) {
    return -1;
  }

  if (insertProgressTable(cliInterface, brExpr, "RESTORE", "Finalize", "Initiated", 0, 0) < 0) {
    return -1;
  }

  if (injectError("RESTORE_ERR_INJECT_3")) {
    return -1;
  }

  if (brExpr->brMetadata()) {
    if (returnObjectsList(cliInterface, objUIDsWherePred, NULL, objectsList)) return -1;
  }

  // display objects to be restored without doing the actual restore
  if (brExpr->showObjects()) {
    return processBRShowObjects(brExpr, mdList, objUIDsWherePred, cliInterface, dws);
  }

  //  unlock objects in MD, if they were locked.
  retcode = 0;
  if (unlockObjectsInMD(cliInterface, (char *)objUIDsWherePred.data(), (char *)backupSch.data(), FALSE)) {
    *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("RESTORE")
                        << DgString1("Reason: Error returned while unlocking objects in MD.");

    retcode = -1;
    goto label_return;
  }

  if (unlockBackupMD(brExpr, cliInterface, "RESTORE")) {
    retcode = -1;
    goto label_return;
  }

  // Update objects in shared cache
  for (CollIndex scl = 0; scl < sharedCacheList.entries(); scl++) {
    SharedCacheDDLInfo existingDDLObj = sharedCacheList[scl];
    CmpCommon::context()->sharedCacheDDLInfoList().insertEntry(existingDDLObj);
  }

  if (NOT brExpr->fastRecovery()) finalizeSharedCache();

  // for mantis 19302, cleanup querycache after restore
  CmpCommon::context()->getQueryCache()->cleanupUserQueryCache();

  if (injectError("RESTORE_ERR_INJECT_4")) {
    retcode = -1;
    goto label_return;
  }

  if (brExpr->fastRecovery()) {
    // add a warning and continue
    NAString reason("Please restart dcs because current restore is fast mode.");
    *CmpCommon::diags() << DgSqlCode(5050) << DgString0("RESTORE IN FAST MODE") << DgString1(reason.data());
  }

label_return:

  if (retcode == 0) {
    if (endXnIfStartedHere(cliInterface, xnWasStartedHere, 0)) {
      *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("RESTORE")
                          << DgString1("Reason: Error returned from Transaction subsystem.");

      return -1;
    }
  } else {
    endXnIfStartedHere(cliInterface, xnWasStartedHere, -1);
  }

  // Remove Snapshot meta lock (if exists) for this tag
  if (CmpCommon::getDefault(TRAF_BR_REMOVE_SM_LOCKS) == DF_ON) {
    if (unlockSnapshotMeta(ehi, brExpr->backupTag(), lockedTag) < 0) {
      populateDiags("RESTORE",
                    "Error occurred while unlocking snapshot metadata. "
                    "Perform GET ALL LOCKED OBJECTS to verify lock has been removed. ",
                    "ExpHbaseInterface::restoreSubset", NULL, -1);
      return -1;
    }
  }

  // drop backup schema before return
  if (brExpr->dropBackupMD()) {
    char query[1000];
    str_sprintf(query, "drop schema if exists %s.\"%s\" cascade;", TRAFODION_SYSCAT_LIT, backupSch.data());
    cliRC = cliInterface->executeImmediate(query);
    if (cliRC < 0) {
      str_sprintf(query, "cleanup schema %s.\"%s\";", TRAFODION_SYSCAT_LIT, backupSch.data());
      cliRC = cliInterface->executeImmediate(query);
      if (cliRC < 0) {
        cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
        return -1;
      }
    }
  } else {
    if (truncateBackupMD(backupSch, cliInterface)) {
      return -1;
    }
  }

  return (retcode < 0 ? -1 : 0);
}

short CmpSeabaseDDL::restore(RelBackupRestore *brExpr, ExeCliInterface *cliInterface, NAString &currCatName,
                             NAString &currSchName, CmpDDLwithStatusInfo *dws) {
  int retcode = 0;
  NAString backupType = "";

  if (xnInProgress(cliInterface)) {
    *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("RESTORE")
                        << DgString1(
                               "Reason: This operation cannot be performed if a user-defined transaction has been "
                               "started or AUTOCOMMIT is OFF.");

    return -1;
  }

  if (brExpr->showObjects() && (!dws)) {
    *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("RESTORE")
                        << DgString1("Reason: Internal error, dws cannot be null.");

    return -1;
  }

  int restoreThreads = CmpCommon::getDefaultNumeric(TRAF_RESTORE_PARALLEL);

  ExpHbaseInterface *ehi = allocBRCEHI();
  if (ehi == NULL) {
    // Diagnostic already populated.
    return -1;
  }

  // For timestamp restore, backupTag is not given by user.
  if (!brExpr->restoreToTS()) {
    backupType = ehi->getBackupType(brExpr->backupTag());
    if (backupType == "FULL") backupType = "SYSTEM";
    if (backupType.isNull()) {
      *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("RESTORE")
                          << DgString1("Reason: Specified tag does not exist.");

      retcode = -1;
      goto label_return;
    }
  }

  // LockAll if system or timestamp since it is whole database restore.
  if (backupInit("RESTORE", ((((backupType == "SYSTEM")) && NOT brExpr->showObjects()) ? TRUE : FALSE))) {
    retcode = -1;
    goto label_return;
  }

  // backupType == "SYSTEM_PIT" possible if user specifies restoreToTS or
  // user specified restore to tag which is of type SYSTEM_PIT.
  if ((backupType == "SYSTEM") || (backupType == "SYSTEM_PIT")) {
    retcode = restoreSystem(brExpr, cliInterface, currCatName, currSchName, dws, ehi, restoreThreads);
  } else {
    retcode = restoreSubset(brExpr, cliInterface, currCatName, currSchName, dws, ehi, restoreThreads);
  }
  if (retcode < 0) {
    goto label_return;
  }

  // This finalizeRestore is called on successful restore to complete
  // on the java side. In case of error, cleanup command will call finalizeRestore
  // if the corresponding flags.
  if (NOT brExpr->showObjects()) {
    retcode = ehi->finalizeRestore(brExpr->backupTag(), backupType);
    if (retcode < 0) {
      populateDiags("RESTORE", "Error returned during finalization of restore.", "ExpHbaseInterface::finalizeRestore",
                    getHbaseErrStr(-retcode), -retcode);

      goto label_return;
    }
  }

  if (updateProgressTable(cliInterface, brExpr->backupTag().data(), "RESTORE", "Finalize", "Completed", 0, 100) < 0) {
    return -1;
  }

label_return:

  deallocBRCEHI(ehi);

  // unlockAll
  if (((backupType == "SYSTEM") || (backupType == "SYSTEM_PIT")) && NOT brExpr->showObjects()) {
    unlockAll();
  }

  return (retcode < 0 ? -1 : 0);
}

short CmpSeabaseDDL::deleteSnapshot(ExpHbaseInterface *ehi, const char *oper, const char *backupTag, NABoolean ifExists,
                                    NABoolean cascade,   // drop dependent incr backups
                                    NABoolean force,     // drop even if this is the last backup
                                    NABoolean skipLock)  // do not lock during drop
{
  int cliRC = 0;

  NAString backupType = ehi->getBackupType(backupTag);
  if (backupType.isNull() && (NOT ifExists)) {
    *CmpCommon::diags() << DgSqlCode(-5050) << DgString0(oper) << DgString1("Reason: Specified tag does not exist.");
    cliRC = -1;
  }

  if (NOT backupType.isNull()) {
    // delete underlying backup snapshot
    if (ehi->deleteBackup(backupTag, FALSE, cascade, force, skipLock) < 0) {
      populateDiags("DROP BACKUP SNAPSHOT", "Error occurred during delete Backup operation.",
                    "ExpHbaseInterface::deleteBackup", NULL, -1);
      cliRC = -1;
    }
  }

  NAString lockedTag;
  if (unlockSnapshotMeta(ehi, backupTag, lockedTag) < 0) {
    populateDiags("DROP BACKUP SNAPSHOT",
                  "Error occurred while unlocking snapshot metadata. "
                  "Perform GET ALL LOCKED OBJECTS to verify lock has been removed. ",
                  "ExpHbaseInterface::restoreSubset", NULL, -1);
    cliRC = -1;
  }

  return cliRC;
}

short CmpSeabaseDDL::dropBackupSnapshots(RelBackupRestore *brExpr, ExeCliInterface *cliInterface) {
  short error = 0;
  short rc = 0;
  int retcode = 0;
  int cliRC = 0;

  // do not let dropBackup when backup in progress.
  // No need of any other lock for this operation.
  if (backupInit("DROP BACKUP SNAPSHOT", FALSE)) return -1;

  ExpHbaseInterface *ehi = allocBRCEHI();
  if (ehi == NULL) {
    //  Diagnostic already populated.
    return -1;
  }

  if (brExpr->dropAllBackups()) {
    // User must have elevated privilegs to drop all backups
    if (verifyBRAuthority(NULL, cliInterface, brExpr->showObjects(), TRUE, SEABASE_MD_SCHEMA, FALSE, NULL) != 0) {
      *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("DROP ALL BACKUP SNAPSHOTS/TAGS")
                          << DgString1("Reason: No privilege.");

      ehi->close();
      return -1;
    }

    NAArray<HbaseStr> *backupList = NULL;
    // We must ask for the backups in reverse order so we drop
    // dependent snapshots fromt he backups before the originals
    retcode = ehi->listAllBackups(&backupList, TRUE, TRUE);
    if (retcode < 0) {
      populateDiags("DROP ALL BACKUP SNAPSHOTS", "Error returned during list all backups.",
                    "ExpHbaseInterface::listAllBackups", getHbaseErrStr(-retcode), -retcode);
      ehi->close();
      return -1;
    }

    if (backupList) {
      if (NOT brExpr->getMatchPattern().isNull()) {
        NAString qry;
        qry = "select a from (values";
        for (int i = 0; i < backupList->entries(); i++) {
          int len = backupList->at(i).len;
          char *val = backupList->at(i).val;

          char delBuf[len + 1];
          strncpy(delBuf, val, len);
          delBuf[len] = 0;

          qry += NAString("('") + NAString(val, len) + NAString("')");

          if (i < backupList->entries() - 1) qry += ", ";
        }

        backupList->clear();

        qry += ") x(a) where a like '" + brExpr->getMatchPattern() + "%'";

        Queue *tagQueue = NULL;
        cliRC = cliInterface->fetchAllRows(tagQueue, qry.data(), 0, FALSE, FALSE, TRUE);
        if (cliRC < 0) {
          cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
          return -1;
        }

        for (int idx = 0; idx < tagQueue->numEntries(); idx++) {
          OutputInfo *vi = (OutputInfo *)tagQueue->getNext();
          char *tag = NULL;
          int tagLen = 0;
          vi->get(0, tag, tagLen);

          HbaseStr elem;
          elem.len = tagLen;
          elem.val = tag;
          backupList->insert(idx, elem);
        }
      }  // match pattern specified

      for (int i = 0; i < backupList->entries(); i++) {
        int len = backupList->at(i).len;
        char *val = backupList->at(i).val;

        char delBuf[len + 1];
        strncpy(delBuf, val, len);
        delBuf[len] = 0;

        // check if objects are locked unless skipLock is specified.
        if (NOT brExpr->getSkipLock()) {
          NAString lockedObjs;
          retcode = checkIfObjectsLockedInMD(cliInterface, NULL, NULL, NULL, delBuf, lockedObjs);
          if (retcode < 0) {
            *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("DROP ALL BACKUP SNAPSHOTS")
                                << DgString1("Reason: Error occurred during check of locked objects.");

            ehi->close();
            return -1;
          }

          if (retcode != 0) {
            *CmpCommon::diags() << DgSqlCode(5050) << DgString0("DROP ALL BACKUP SNAPSHOTS")
                                << DgString1(
                                       NAString("Reason: Some objects for tag ") + delBuf +
                                       " are locked in Metadata and need to be unlocked before it could be dropped.");
            continue;
          }
        }

        if (deleteSnapshot(ehi, (char *)"DROP ALL BACKUP SNAPSHOTS", delBuf, TRUE)) {
          // error during delete of this tag. Continue to other tags.
          continue;
        }
      }  // for

    }  // backupList returned
  } else {
    // If unable to get extended attributes, continue.  Backup owner
    // will not be able to drop their backup.
    BackupAttrList *attrList = new (STMTHEAP) BackupAttrList(STMTHEAP);
    NAString errorMsg;
    getExtendedAttributes(ehi, brExpr->backupTag(), attrList, errorMsg);
    const char *owner = attrList->getAttrByKey(BackupAttr::BACKUP_ATTR_OWNER);
    NAString ownerStr;
    if (owner != NULL) ownerStr = owner;

    retcode = verifyBRAuthority(NULL, cliInterface, brExpr->showObjects(), TRUE, SEABASE_MD_SCHEMA, FALSE, ownerStr);
    if (retcode < 0) {
      *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("DROP BACKUP SNAPSHOT")
                          << DgString1("Reason: No privilege.");

      ehi->close();
      return -1;
    }

    // check if objects are locked unless skipLock is specified.
    if (NOT brExpr->getSkipLock()) {
      NAString lockedObjs;
      retcode =
          checkIfObjectsLockedInMD(cliInterface, NULL, NULL, NULL, (char *)brExpr->backupTag().data(), lockedObjs);
      if (retcode < 0) {
        *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("DROP BACKUP SNAPSHOT")
                            << DgString1("Reason: Error occurred during check of locked objects.");

        ehi->close();
        return -1;
      }

      if (retcode != 0) {
        *CmpCommon::diags()
            << DgSqlCode(-5050) << DgString0("DROP BACKUP SNAPSHOT")
            << DgString1(
                   "Reason: Some objects are locked in Metadata and need to be unlocked before dropping this tag.");

        ehi->close();
        return -1;
      }
    }

    NABoolean dropForce = FALSE;
    NAString backupType = ehi->getBackupType(brExpr->backupTag());
    NAArray<HbaseStr> *backupList = NULL;
    if (!backupType.isNull() && backupType == "REGULAR" && brExpr->getForce()) {
      dropForce = TRUE;
      backupList = ehi->getLinkedBackupTags(brExpr->backupTag());
    }

    if (deleteSnapshot(ehi, (char *)"DROP BACKUP SNAPSHOT", (char *)brExpr->backupTag().data(), FALSE,
                       brExpr->getCascade(), dropForce, brExpr->getSkipLock())) {
      ehi->close();
      return -1;
    }

    if (backupList && backupList->entries() > 0) {
      if (cleanupLinkedBackupMD(cliInterface, backupList) < 0) {
        ehi->close();
        return -1;
      }
    }
  }

  // deallocate, not needed anymore.
  ehi->close();
  ehi = NULL;

  return 0;
}

// this method will drop backup snapshot and backup md.
short CmpSeabaseDDL::dropBackupTags(RelBackupRestore *brExpr, ExeCliInterface *cliInterface) {
  short rc = 0;
  int cliRC = 0;

  ExpHbaseInterface *ehi = NULL;
  ehi = allocBRCEHI();
  if (ehi == NULL) {
    // Diagnostic already populated.
    return -1;
  }

  NAArray<HbaseStr> *backupList = NULL;
  if (brExpr->dropAllBackups()) {
    rc = dropBackupSnapshots(brExpr, cliInterface);
  } else {
    NAString backupType = ehi->getBackupType(brExpr->backupTag());
    // backup exists
    if (!backupType.isNull()) {
      if (backupType == "INCREMENTAL" && brExpr->getForce()) {
        *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("DROP BACKUP SNAPSHOT")
                            << DgString1("Reason: Incremental backup don't support force delete");
        ehi->close();
        return -1;
      }

      if (backupType == "REGULAR" && brExpr->getForce()) {
        backupList = ehi->getLinkedBackupTags(brExpr->backupTag());
      }
      rc = dropBackupSnapshots(brExpr, cliInterface);
    }
  }

  if (rc < 0) return rc;

  if (backupList && backupList->entries() > 0) {
    if (cleanupLinkedBackupMD(cliInterface, backupList) < 0) {
      ehi->close();
      return -1;
    }
  }

  if (NOT brExpr->dropAllBackups()) {
    NAString backupSch;
    genBackupSchemaName(brExpr->backupTag(), backupSch);

    // check if backup schema exists in MD
    long objUID = getObjectUID(cliInterface, TRAFODION_SYSCAT_LIT, backupSch.data(), SEABASE_SCHEMA_OBJECTNAME,
                                COM_PRIVATE_SCHEMA_OBJECT_LIT, NULL, NULL, NULL, FALSE, FALSE);
    if (objUID < 0) return 0;  // does not exist, nothing to drop
  }

  rc = dropBackupMD(brExpr, cliInterface, FALSE /*don't check privs*/);
  if (rc < 0) return rc;

  return 0;
}

short CmpSeabaseDDL::exportOrImportBackup(RelBackupRestore *brExpr, ExeCliInterface *cliInterface) {
  short error = 0;
  short rc = 0;
  int retcode = 0;
  int cliRC = 0;

  NAString oper(brExpr->exportBackup() ? "EXPORT" : "IMPORT");

  if (createProgressTable(cliInterface)) return -1;

  stepSeq = 1;
  operCnt = getBREIOperCount(cliInterface, oper.data(), brExpr->backupTag().data());
  if (operCnt < 0) return -1;
  operCnt++;

  if (insertProgressTable(cliInterface, brExpr, oper.data(), "Initialize", "Initiated", 0, 0) < 0) {
    return -1;
  }

  // do not let archiveBackup when backup in progress.
  // No need of any other lock for this operation.
  if (backupInit((brExpr->exportBackup() ? "EXPORT BACKUP" : "IMPORT BACKUP"), FALSE)) return -1;

  ExpHbaseInterface *ehi = allocBRCEHI();
  if (ehi == NULL) {
    //  Diagnostic already populated.
    return -1;
  }

  if (brExpr->importAllBackups()) {
    *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("IMPORT BACKUP")
                        << DgString1("Reason: Import all backups not yet supported.");
    return -1;
  }

  // Get extended attributes
  BackupAttrList *attrList = new (STMTHEAP) BackupAttrList(STMTHEAP);
  NAString errorMsg;
  if (getExtendedAttributes(ehi, brExpr->backupTag(), attrList, errorMsg) < 0) {
    errorMsg.prepend("Reason: ");
    *CmpCommon::diags() << DgSqlCode(-5050) << DgString0(brExpr->exportBackup() ? "EXPORT" : "IMPORT")
                        << DgString1(errorMsg.data());
    ehi->close();
    return -1;
  }

  // User must have elevated privileges to export all backups
  //   (import all backups is not supported)
  // Be the owner or have elevated privileges to export/import a single backup
  const char *owner = (brExpr->exportAllBackups()) ? NULL : attrList->getAttrByKey(BackupAttr::BACKUP_ATTR_OWNER);
  if (verifyBRAuthority(NULL, cliInterface, brExpr->showObjects(), TRUE, SEABASE_MD_SCHEMA, FALSE, owner) != 0) {
    *CmpCommon::diags() << DgSqlCode(-5050) << DgString0(brExpr->exportBackup() ? "EXPORT" : "IMPORT")
                        << DgString1("Reason: No privilege.");
    ehi->close();
    return -1;
  }

  int exportImportThreads = CmpCommon::getDefaultNumeric(TRAF_EXPORT_IMPORT_PARALLEL);

  if (brExpr->exportAllBackups()) {
    NAArray<HbaseStr> *backupList = NULL;

    retcode = ehi->listAllBackups(&backupList, TRUE, FALSE);
    if (retcode < 0) {
      populateDiags("EXPORT IMPORT BACKUP", "Error returned during list all backups.",
                    "ExpHbaseInterface::listAllBackups", getHbaseErrStr(-retcode), -retcode);
      ehi->close();
      return -1;
    }

    if (backupList) {
      for (int i = 0; i < backupList->entries(); i++) {
        int len = backupList->at(i).len;
        char *val = backupList->at(i).val;

        char archBuf[len + 1];
        strncpy(archBuf, val, len);
        archBuf[len] = 0;
        retcode = ehi->exportOrImportBackup(archBuf, TRUE, brExpr->getOverride(), brExpr->exportImportLocation().data(),
                                            exportImportThreads,
                                            CmpCommon::getDefaultNumeric(TRAF_BREI_PROGRESS_UPDATE_DELAY));
        if (retcode < 0) {
          // add a warning and continue
          NAString reason("Reason: Error occurred for backup tag ");
          reason += archBuf;
          reason += ".";
          *CmpCommon::diags() << DgSqlCode(5050) << DgString0("EXPORT BACKUP") << DgString1(reason.data());
        }
      }
    }
  } else {
    if (updateProgressTable(cliInterface, brExpr->backupTag().data(), oper.data(), "Initialize", "Completed", 0, 100) <
        0) {
      return -1;
    }

    if (insertProgressTable(cliInterface, brExpr, oper.data(), (oper == "EXPORT" ? "ExportObjects" : "ImportObjects"),
                            "Initiated", 0, 0) < 0) {
      return -1;
    }

    NAString backupType;
    backupType = ehi->getBackupType(brExpr->backupTag());

    // exporting a backup , check for tag exist.
    if (brExpr->exportBackup() && backupType.isNull()) {
      *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("EXPORT BACKUP")
                          << DgString1("Reason: Specified tag does not exist.");

      ehi->close();
      return -1;
    }

    // importing a backup, check for tag does not exist.
    if ((brExpr->importBackup()) && (!backupType.isNull())) {
      // override
      if (!brExpr->getOverride()) {
        *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("IMPORT BACKUP")
                            << DgString1("Reason: Specified tag already exist.");

        if (updateProgressTable(cliInterface, brExpr->backupTag().data(), oper.data(),
                                (oper == "EXPORT" ? "ExportObjects" : "ImportObjects"), "Failed", -1, -1) < 0) {
          return -1;
        }

        ehi->close();
        return -1;
      }
    }

    retcode =
        ehi->exportOrImportBackup(brExpr->backupTag(), (brExpr->exportBackup() ? TRUE : FALSE), brExpr->getOverride(),
                                  brExpr->exportImportLocation().data(), exportImportThreads,
                                  CmpCommon::getDefaultNumeric(TRAF_BREI_PROGRESS_UPDATE_DELAY));
    if (retcode < 0) {
      populateDiags((brExpr->exportBackup() ? "EXPORT BACKUP" : "IMPORT BACKUP"),
                    "Error returned from exportOrImportBackup method.", "ExpHbaseInterface::exportOrImportBackup",
                    getHbaseErrStr(-retcode), -retcode);

      updateProgressTable(cliInterface, brExpr->backupTag().data(), oper.data(),
                          (oper == "EXPORT" ? "ExportObjects" : "ImportObjects"), "Failed", -1, -1);

      ehi->close();
      return -1;
    }
  }

  if (updateProgressTable(cliInterface, brExpr->backupTag().data(), oper.data(),
                          (oper == "EXPORT" ? "ExportObjects" : "ImportObjects"), "Completed", -1, -1) < 0) {
    return -1;
  }

  if (insertProgressTable(cliInterface, brExpr, oper.data(), "Finalize", "Initiated", 0, 0) < 0) {
    return -1;
  }

  // deallocate, not needed anymore.
  ehi->close();
  ehi = NULL;

  if (updateProgressTable(cliInterface, brExpr->backupTag().data(), oper.data(), "Finalize", "Completed", 0, 100) < 0) {
    return -1;
  }

  return 0;
}

short CmpSeabaseDDL::getBackupSnapshots(RelBackupRestore *brExpr, ExeCliInterface *cliInterface,
                                        CmpDDLwithStatusInfo *dws) {
  int retcode = 0;
  int cliRC = 0;

  ExpHbaseInterface *ehi = allocBRCEHI();
  if (ehi == NULL) {
    //  Diagnostic already populated.
    return -1;
  }

  NAArray<HbaseStr> *backupList = NULL;
  retcode = ehi->listAllBackups(&backupList, FALSE, FALSE);
  if (retcode < 0) {
    populateDiags("GET ALL BACKUP SNAPSHOTS", "Error returned during list all backups.",
                  "ExpHbaseInterface::listAllBackups", getHbaseErrStr(-retcode), -retcode);
    ehi->close();
    return -1;
  }

  int maxTagLen = strlen("BackupTag");
  if (backupList) {
    NAString matchClause;
    if (NOT brExpr->getMatchPattern().isNull())
      matchClause = " where a like '" + brExpr->getMatchPattern() + "%'";
    else if ((brExpr->getBackupSnapshot()) && (NOT brExpr->backupTag().isNull()))
      matchClause = " where a like '" + brExpr->backupTag() + " %'";

    NAString qry;
    qry = "select a, cast(char_length(substring(a, 1, position(' ' in a))) as integer) from (values";
    for (int i = 0; i < backupList->entries(); i++) {
      int len = backupList->at(i).len;
      char *val = backupList->at(i).val;

      qry += NAString("('") + NAString(val, len) + NAString("')");

      if (i < backupList->entries() - 1) qry += ", ";
    }

    backupList->clear();

    qry += ") x(a) " + matchClause;

    Queue *tagQueue = NULL;
    cliRC = cliInterface->fetchAllRows(tagQueue, qry.data(), 0, FALSE, FALSE, TRUE);
    if (cliRC < 0) {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      return -1;
    }

    for (int idx = 0; idx < tagQueue->numEntries(); idx++) {
      OutputInfo *vi = (OutputInfo *)tagQueue->getNext();
      char *tag = NULL;
      int tagLen = 0;
      vi->get(0, tag, tagLen);

      int len = *(int *)vi->get(1);

      if (len > maxTagLen) maxTagLen = len;

      HbaseStr elem;
      elem.len = tagLen;
      elem.val = tag;
      backupList->insert(idx, elem);
    }
  }

  // Get owner list and max owner size
  std::vector<string> ownerList;
  int maxOwnerLen = getOwnerList(ehi, backupList, ownerList);

  int blackBoxLen = 0;
  char *blackBox = NULL;

  if (backupList && (backupList->entries() > 0)) {
    Queue *brList = NULL;
    cliInterface->initializeInfoList(brList, TRUE);

    char frmt[300];

    char *brVal = new (STMTHEAP) char[200];
    str_sprintf(frmt, "%%-%ds  %%-19s  %%-12s  %%-21s  %%-%ds", maxTagLen, maxOwnerLen);
    str_sprintf(brVal, frmt, "BackupTag", "BackupTime", "BackupStatus", "BackupOperation", "BackupOwner");

    // max display size
    int brValLen = strlen(brVal);

    OutputInfo *oi = NULL;

    if (NOT brExpr->getNoHeader()) {
      oi = new (STMTHEAP) OutputInfo(1);
      oi->insert(0, brVal);
      brList->insert(oi);

      brVal = new (STMTHEAP) char[brValLen + 1];
      str_pad(brVal, brValLen, '=');
      brVal[brValLen] = 0;

      updateQueueWithOI(brVal, brList, STMTHEAP);
      updateQueueWithOI(" ", brList, STMTHEAP);
    }

    for (int i = 0; i < backupList->entries(); i++) {
      int len = backupList->at(i).len;
      char *val = backupList->at(i).val;

      std::string s(val);

      std::vector<string> tokens;
      extractTokens(s, tokens);
      if ((tokens.empty()) || (tokens.size() != 4)) continue;

      // convert tokens[1] from utc to local
      char localStrBuf[100];
      char *localStr = (char *)tokens[1].c_str();
      if (CmpCommon::context()->gmtDiff() != 0) {
        localStr = localStrBuf;
        ComDiagsArea *diags = CmpCommon::diags();
        ExpDatetime::convAsciiDatetimeToUtcOrLocal((char *)tokens[1].c_str(), strlen(tokens[1].c_str()), localStr,
                                                   strlen(tokens[1].c_str()),
                                                   (long)CmpCommon::context()->gmtDiff() * 60 * 1000000,
                                                   FALSE,  // toLocal
                                                   STMTHEAP, &diags);

        localStr[10] = ':';  // add ':' separator between date and time
      }

      char *brVal = new (STMTHEAP) char[brValLen + 1];

      str_sprintf(frmt, "%%-%ds  %%-19s  %%-12s  %%-21s  %%-%ds", maxTagLen, maxOwnerLen);
      str_sprintf(brVal, frmt,
                  tokens[0].c_str(),  // tag
                  //(char*)tokens[1].c_str(),
                  localStr,
                  tokens[2].c_str(),      // status
                  tokens[3].c_str(),      // operation
                  ownerList[i].c_str());  // owner

      oi = new (STMTHEAP) OutputInfo(1);
      oi->insert(0, brVal);
      brList->insert(oi);
    }  // for

    populateBlackBox(cliInterface, brList, blackBoxLen, blackBox, TRUE);
  }

  if ((brExpr->getBackupSnapshot()) && ((!backupList) || (backupList->entries() == 0))) {
    *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("GET BACKUP SNAPSHOT")
                        << DgString1("Reason: Specified tag does not exist.");

    ehi->close();
    return -1;
  }

  dws->setBlackBoxLen(blackBoxLen);
  dws->setBlackBox(blackBox);

  dws->setStep(-1);
  dws->setSubstep(0);
  dws->setDone(TRUE);

  ehi->close();
  ehi = NULL;

  return 0;
}

short CmpSeabaseDDL::dropBackupMD(RelBackupRestore *brExpr, ExeCliInterface *cliInterface, NABoolean checkPrivs) {
  int retcode = 0;
  int cliRC = 0;

  // do not let dropBackup when backup in progress.
  // No need of any other lock for this operation.
  if (backupInit("DROP BACKUP METADATA", FALSE)) return -1;

  // Must be elevated user to drop MD backups
  if (checkPrivs) {
    retcode =
        (verifyBRAuthority(NULL, cliInterface, brExpr->showObjects(), TRUE, SEABASE_MD_SCHEMA, FALSE, NULL /*owner*/));
    if (retcode < 0) {
      *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("DROP BACKUP METADATA")
                          << DgString1("Reason: No privilege.");
      return -1;
    }
  }

  ExpHbaseInterface *ehi = allocBRCEHI();
  if (ehi == NULL) {
    //  Diagnostic already populated.
    return -1;
  }

  NAString backupSch;
  char query[1000];
  if (brExpr->backupTag().isNull()) {
    NAString likeClause;
    likeClause = " and schema_name like '";
    likeClause += "|_BACKUP|_";
    if (NOT brExpr->getMatchPattern().isNull())
      likeClause += brExpr->getMatchPattern() + "%";
    else
      likeClause += "%";
    likeClause += "|_";
    likeClause += "' escape '|' ";

    str_sprintf(query, "select distinct schema_name from %s.\"%s\".%s where catalog_name = '%s' %s ",
                TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_OBJECTS, TRAFODION_SYSCAT_LIT, likeClause.data());
    Queue *tableQueue = NULL;
    cliRC = cliInterface->fetchAllRows(tableQueue, query, 0, FALSE, FALSE, TRUE);
    if (cliRC < 0) {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      return -1;
    }

    for (int idx = 0; idx < tableQueue->numEntries(); idx++) {
      OutputInfo *vi = (OutputInfo *)tableQueue->getNext();
      char *obj = NULL;
      int objLen = 0;

      vi->get(0, obj, objLen);

      char backupTag[100];
      str_cpy_and_null(backupTag, &obj[strlen(TRAF_BACKUP_MD_PREFIX)], (objLen - strlen(TRAF_BACKUP_MD_PREFIX) - 1),
                       '\0', ' ', TRUE);
      NAString backupType;
      backupType = ehi->getBackupType(backupTag);
      if (NOT backupType.isNull()) continue;

      backupSch = obj;
      cliRC = cleanupBackupSchema(cliInterface, backupSch);
      if (cliRC < 0) {
        // if error, continue to next schema
      }
    }  // for

    // change errors to warnings
    CmpCommon::diags()->negateAllErrors();
  }  // cleanup all
  else {
    NAString backupType;
    backupType = ehi->getBackupType(brExpr->backupTag());
    if (NOT backupType.isNull()) {
      *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("CLEANUP BACKUP")
                          << DgString1(
                                 "Reason: Backup corresponding to the specified tag exists and is not obsolete. It "
                                 "needs to be deleted before metadata could be removed.");

      ehi->close();
      return -1;
    }

    genBackupSchemaName(brExpr->backupTag(), backupSch);

    ComObjectType objectType;
    int schemaOwnerID = 0;
    long schemaUID = getObjectTypeandOwner(cliInterface, TRAFODION_SYSCAT_LIT, backupSch.data(),
                                            SEABASE_SCHEMA_OBJECTNAME, objectType, schemaOwnerID);
    if (schemaUID == -1)  // doesn't exist
    {
      *CmpCommon::diags() << DgSqlCode(-CAT_SCHEMA_DOES_NOT_EXIST_ERROR) << DgString0(getSystemCatalog())
                          << DgString1(backupSch.data());

      ehi->close();
      return -1;
    }

    cliRC = cleanupBackupSchema(cliInterface, backupSch);
    if (cliRC < 0) {
      ehi->close();
      return -1;
    }
  }

  // deallocate, not needed anymore.
  ehi->close();
  ehi = NULL;

  return 0;
}

short CmpSeabaseDDL::getBackupMD(RelBackupRestore *brExpr, ExeCliInterface *cliInterface, CmpDDLwithStatusInfo *dws) {
  int retcode = 0;
  int cliRC = 0;

  ExpHbaseInterface *ehi = allocBRCEHI();
  if (ehi == NULL) {
    //  Diagnostic already populated.
    return -1;
  }

  NAString backupSch;
  char query[1000];

  NAString likeClause;
  likeClause = " and schema_name like '";
  likeClause += "|_BACKUP|_";
  if (NOT brExpr->getMatchPattern().isNull())
    likeClause += brExpr->getMatchPattern() + "%";
  else
    likeClause += "%";
  likeClause += "|_";
  likeClause += "' escape '|' ";

  bool checkAuthority = hasAccess(NULL, true);

  // If current user not an elevated user, then only display backup schemas
  // owned by the current user.  We generally won't be granting privileges on
  // backup schemas to others users so no need to check this.
  NAString privsClause;
  if (!checkAuthority) {
    char intStr[20];
    privsClause += " and schema_owner = ";
    privsClause += str_itoa(ComUser::getCurrentUser(), intStr);
  }

  str_sprintf(query,
              "select distinct schema_name from %s.\"%s\".%s "
              "where catalog_name = '%s' %s %s ",
              TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_OBJECTS, TRAFODION_SYSCAT_LIT,
              (likeClause.isNull() ? " " : likeClause.data()), privsClause.data());
  Queue *tableQueue = NULL;
  cliRC = cliInterface->fetchAllRows(tableQueue, query, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  Queue *mdQueue = NULL;
  cliInterface->initializeInfoList(mdQueue, TRUE);
  char mdBuf[400];
  for (int idx = 0; idx < tableQueue->numEntries(); idx++) {
    OutputInfo *vi = (OutputInfo *)tableQueue->getNext();
    char *obj = NULL;
    int objLen = 0;

    vi->get(0, obj, objLen);

    char backupTag[100];
    str_cpy_and_null(backupTag, &obj[strlen(TRAF_BACKUP_MD_PREFIX)], (objLen - strlen(TRAF_BACKUP_MD_PREFIX) - 1), '\0',
                     ' ', TRUE);
    NAString backupType;
    backupType = ehi->getBackupType(backupTag);

    str_sprintf(mdBuf, "Metadata(%-10s): %s", (backupType.isNull() ? "OBSOLETE" : "ACTIVE"), obj);
    updateQueueWithOI(mdBuf, mdQueue, STMTHEAP);
  }  // for

  int blackBoxLen = 0;
  char *blackBox = NULL;

  populateBlackBox(cliInterface, mdQueue, blackBoxLen, blackBox);

  dws->setBlackBoxLen(blackBoxLen);
  dws->setBlackBox(blackBox);

  dws->setStep(-1);
  dws->setSubstep(0);
  dws->setDone(TRUE);

  // deallocate, not needed anymore.
  ehi->close();
  ehi = NULL;

  return 0;
}

short CmpSeabaseDDL::getBackupTags(RelBackupRestore *brExpr, ExeCliInterface *cliInterface, CmpDDLwithStatusInfo *dws) {
  int retcode = 0;
  int cliRC = 0;

  ExpHbaseInterface *ehi = allocBRCEHI();
  if (ehi == NULL) {
    //  Diagnostic already populated.
    return -1;
  }

  NAString backupSch;

  NABoolean showDetails = brExpr->getBackupTagDetails();

  NAArray<HbaseStr> *snapshotList = NULL;
  retcode = ehi->listAllBackups(&snapshotList, FALSE, FALSE);
  if (retcode < 0) {
    populateDiags("GET BACKUP TAGS", "Error returned during list all backups.", "ExpHbaseInterface::listAllBackups",
                  getHbaseErrStr(-retcode), -retcode);
    deallocBRCEHI(ehi);
    return -1;
  }

  NAString likeClause;
  likeClause = " and schema_name like '";
  likeClause += "|_BACKUP|_";
  if (NOT brExpr->getMatchPattern().isNull())
    likeClause += brExpr->getMatchPattern() + "%";
  else
    likeClause += "%";
  likeClause += "|_";
  likeClause += "' escape '|' ";

  if (brExpr->getBackupTag()) {
    likeClause += " and schema_name = '_BACKUP_" + brExpr->backupTag() + "_'";
  }

  // add query to retrieve snapshot tags that done have corresponding MD.
  NAString snapshotQry;
  NAString tags;
  if (snapshotList && (snapshotList->entries() > 0)) {
    // NO indicates this is an orphan snapshot tag, it doesn't have MD.
    snapshotQry = "select tag, 'YES', tagtime from (values";
    tags = "(";

    NABoolean found = FALSE;
    for (int i = 0; i < snapshotList->entries(); i++) {
      int len = snapshotList->at(i).len;
      char *val = snapshotList->at(i).val;

      std::string s(val, len);

      std::vector<string> tokens;
      extractTokens(s, tokens);
      if ((tokens.empty()) || (tokens.size() != 4)) continue;

      found = TRUE;

      snapshotQry += NAString("('") + tokens[0].c_str() + NAString("', '") + tokens[1].c_str() + NAString("')");
      tags += NAString("'") + tokens[0].c_str() + NAString("'");
      if (i < snapshotList->entries() - 1) {
        snapshotQry += ", ";
        tags += ", ";
      }
    }

    snapshotQry += ") x(tag, tagtime) ";
    if (NOT brExpr->getMatchPattern().isNull()) snapshotQry += "where tag like '" + brExpr->getMatchPattern() + "%' ";
    snapshotQry += "union all";
    tags += ")";

    if (NOT found) {
      snapshotQry.clear();
      tags.clear();
    }
  }

  int trafBackupMDPrefixLen = strlen(TRAF_BACKUP_MD_PREFIX) + 1;
  char query[1000 + snapshotQry.length() + tags.length()];
  char schemaFilter[100 + tags.length()];
  snprintf(schemaFilter, sizeof(schemaFilter),
           "and substring(schema_name, %d, char_length(schema_name) - %d) not in %s", trafBackupMDPrefixLen,
           trafBackupMDPrefixLen, tags.data());

  // YES indicates this tag has metadata.
  snprintf(query, sizeof(query),
           "%s select distinct substring(schema_name, %d, char_length(schema_name) - %d), 'NO', ' ' "
           "from %s.\"%s\".%s where catalog_name = '%s' %s and object_name = '__SCHEMA__' "
           "%s order by 2;",
           (snapshotQry.isNull() ? " " : snapshotQry.data()), trafBackupMDPrefixLen, trafBackupMDPrefixLen,
           TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_OBJECTS, TRAFODION_SYSCAT_LIT, likeClause.data(),
           (tags.isNull() ? "" : schemaFilter));

  Queue *tagQueue = NULL;
  cliRC = cliInterface->fetchAllRows(tagQueue, query, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    deallocBRCEHI(ehi);
    return -1;
  }

  // if no rows returned, there are no tags.
  if (tagQueue->numEntries() == 0) {
    dws->setBlackBoxLen(0);
    dws->setBlackBox(NULL);

    dws->setStep(-1);
    dws->setSubstep(0);
    dws->setDone(TRUE);

    deallocBRCEHI(ehi);
    return 0;
  }

  Queue *mdQueue = NULL;
  cliInterface->initializeInfoList(mdQueue, TRUE);

  char mdBuf[400];
  NAString qry;
  if (NOT showDetails)
    qry = "select a, b, cast(count(*) as largeint) from (values";
  else
    qry = "select a, b, c, d from (values";

  // ownerList is a list of owner for each tag.  It assumes that the order of
  // owner's is the same as the order of tags.  If this changes, then this code
  // needs to be adjusted.
  std::vector<std::string> ownerList;

  int maxOwnerLen = strlen("Not Available");

  // ts suffix has the format:  "_" + left zero padded 20 digits
  int jtsSuffixLen = 21;
  int maxTagLen = strlen("BackupTag");
  for (int idx = 0; idx < tagQueue->numEntries(); idx++) {
    OutputInfo *vi = (OutputInfo *)tagQueue->getNext();
    char *obj = NULL;
    int objLen = 0;

    vi->get(0, obj, objLen);

    NAString tag;
    NABoolean isExtended = FALSE;
    if ((objLen >= jtsSuffixLen) && (obj[objLen - jtsSuffixLen] == '_')) {
      if (NOT showDetails)
        tag = NAString(obj, (objLen - jtsSuffixLen));
      else
        tag = NAString(obj);
      isExtended = TRUE;
    } else {
      tag = NAString(obj, objLen);
    }

    if (tag.length() > maxTagLen) maxTagLen = tag.length();

    if (showDetails) {
      if (CmpCommon::getDefault(TRAF_BACKUP_SKIP_GET_TAGS_OWNER_LOOKUP) == DF_ON)
        ownerList.push_back(DB__ROOT);
      else {
        BackupAttrList attrList;
        NAString errorMsg;
        getExtendedAttributes(ehi, tag.data(), &attrList, errorMsg);
        const char *ownerAttr = attrList.getAttrByKey(BackupAttr::BACKUP_ATTR_OWNER);
        if (ownerAttr == NULL)
          ownerList.push_back("Not Available");
        else
          ownerList.push_back(ownerAttr);
        if (ownerAttr && strlen(ownerAttr) > maxOwnerLen) maxOwnerLen = strlen(ownerAttr);
      }
    }

    // get info whether this tag has backup MD or not.
    // YES means it does, NO means it does not.
    vi->get(1, obj, objLen);

    if (NOT showDetails) {
      qry += NAString("('") + tag + "', " + (isExtended ? "'YES'" : "'NO'") + NAString(")");
    } else {
      qry += NAString("('") + tag + "', " + (isExtended ? "'YES'" : "'NO'") + NAString(", '") + NAString(obj) +
             NAString("', '");
      char *time = NULL;
      int timeLen = 0;
      vi->get(2, time, timeLen);
      qry += NAString(time) + NAString("')");
    }

    if (idx < tagQueue->numEntries() - 1) qry += ", ";
  }  // for

  if (NOT showDetails)
    qry += ") x(a, b) group by 1,2 order by 2,1;";
  else
    qry += ") x(a, b, c, d) order by 4;";

  tagQueue = NULL;
  cliRC = cliInterface->fetchAllRows(tagQueue, qry.data(), 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    deallocBRCEHI(ehi);
    return -1;
  }

  char frmt[100];
  if (NOT brExpr->getNoHeader()) {
    if (NOT showDetails) {
      str_sprintf(frmt, "%%-%ds  %%-8s  %%-10s", maxTagLen);
      str_sprintf(mdBuf, frmt, "BackupTag", "IsPrefix", "NumBackups");
      updateQueueWithOI(mdBuf, mdQueue, STMTHEAP);

      str_pad(mdBuf, strlen(mdBuf), '=');
      updateQueueWithOI(mdBuf, mdQueue, STMTHEAP);
    } else {
      str_sprintf(frmt, "%%-%ds  %%-19s  %%-12s  %%-21s  %%-%ds", maxTagLen, maxOwnerLen);
      str_sprintf(mdBuf, frmt, "BackupTag", "BackupTime", "BackupStatus", "BackupOperation", "BackupOwner");
      updateQueueWithOI(mdBuf, mdQueue, STMTHEAP);

      str_pad(mdBuf, strlen(mdBuf), '=');
      updateQueueWithOI(mdBuf, mdQueue, STMTHEAP);
    }

    updateQueueWithOI(" ", mdQueue, STMTHEAP);
  }

  for (int idx = 0; idx < tagQueue->numEntries(); idx++) {
    NABoolean hasMeta = TRUE;
    OutputInfo *vi = (OutputInfo *)tagQueue->getNext();
    char *tag = NULL;
    int objLen = 0;
    vi->get(0, tag, objLen);

    char *suffix = NULL;
    int suffixLen = 0;
    vi->get(1, suffix, suffixLen);

    char *ts = NULL;
    int tsLen = 0;

    long count = 0;

    NAString backupOper;
    NAString backupStatus;

    if (backupOper == "SYSTEM") continue;  // skip system backups

    if (NOT showDetails)
      count = *(long *)vi->get(2);
    else {
      char *snap = NULL;
      int snapLen = 0;
      vi->get(2, snap, snapLen);
      if (strncmp(snap, "NO", 2) == 0) {
        backupOper = "UNKNOWN";
        backupStatus = "NO_SNAPSHOT";
      } else {
        // get current backupOper and backupStatus from snapshotList
        NABoolean tagInSnapshotList = containsTagInSnapshotList(snapshotList, tag, backupOper, backupStatus);
        if (backupOper.contains("IMPORTED")) hasMeta = FALSE;
      }
      vi->get(3, ts, tsLen);
    }

    if (NOT showDetails)
      str_sprintf(frmt, "%%-%ds  %%8s  %%10ld", maxTagLen);
    else
      str_sprintf(frmt, "%%-%ds  %%-19s  %%-12s  %%-21s  %%-%ds", maxTagLen, maxOwnerLen);

    if (NOT showDetails)
      str_sprintf(mdBuf, frmt, tag, suffix, count);
    else {
      // convert time from gmt to local to display
      char localStrBuf[100];

      NAString time_str(ts, strlen(ts));
      if (time_str == " ") {
        memset(localStrBuf, 0, sizeof(localStrBuf));  // display nothing
      } else {
        ComDiagsArea *diags = CmpCommon::diags();
        ExpDatetime::convAsciiDatetimeToUtcOrLocal(ts, strlen(ts), localStrBuf, strlen(ts),
                                                   (long)CmpCommon::context()->gmtDiff() * 60 * 1000000,
                                                   FALSE,  // toLocal
                                                   STMTHEAP, &diags);

        localStrBuf[10] = ':';  // add ':' separator between date and time
        localStrBuf[strlen(ts)] = 0;
      }

      str_sprintf(mdBuf, frmt, tag, localStrBuf, (hasMeta ? backupStatus.data() : "NO_METADATA"),
                  (NOT backupOper.isNull() ? backupOper.data() : " "),
                  ownerList[idx].c_str());  // owner
    }
    updateQueueWithOI(mdBuf, mdQueue, STMTHEAP);
  }  // for

  int blackBoxLen = 0;
  char *blackBox = NULL;

  populateBlackBox(cliInterface, mdQueue, blackBoxLen, blackBox);

  dws->setBlackBoxLen(blackBoxLen);
  dws->setBlackBox(blackBox);

  dws->setStep(-1);
  dws->setSubstep(0);
  dws->setDone(TRUE);

  // deallocate, not needed anymore.
  deallocBRCEHI(ehi);

  return 0;
}

// ----------------------------------------------------------------------------
// method:: getVersionOfBackup
//
// Displays current version information with version information stored with
// the backup.
//
// Versions displayed:
//     database version - EsgynDB Enterprise 2.7.0
//     metadata version - Metadata Version 2.6.0
//     product  version - EsgynDB 2.7.0
// ----------------------------------------------------------------------------
short CmpSeabaseDDL::getVersionOfBackup(RelBackupRestore *brExpr, ExeCliInterface *cliInterface,
                                        CmpDDLwithStatusInfo *dws) {
  int retcode = 0;
  int cliRC = 0;
  NAString versionStr;

  Queue *mdQueue = NULL;
  cliInterface->initializeInfoList(mdQueue, TRUE);

  ExpHbaseInterface *ehi = allocBRCEHI();
  if (ehi == NULL) {
    //  Diagnostic already populated.
    return -1;
  }

  // Get extended attributes
  BackupAttrList *attrList = new (STMTHEAP) BackupAttrList(STMTHEAP);
  NAString errorMsg;

  // If unable to retrieve extended attributes, continue.  Version details
  // will not be complete and backup owner won't be able to see details.
  retcode = getExtendedAttributes(ehi, brExpr->backupTag(), attrList, errorMsg);
  bool noAttrs = (retcode == -1);
  if (attrList->entries() == 0) noAttrs = true;

  // Check for privilege
  retcode = verifyBRAuthority(NULL, cliInterface, brExpr->showObjects(), TRUE, SEABASE_MD_SCHEMA, FALSE,
                              attrList->getAttrByKey(BackupAttr::BACKUP_ATTR_OWNER));

  if (retcode < 0) {
    *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("GET VERSION OF BACKUP") << DgString1("Reason: No privilege.");
    ehi->close();
    return -1;
  }

  // BR_TESTPOINT_MDV:  change the MD version
  char *testPoint = getenv("BR_TESTPOINT_MDV");
  if (testPoint != NULL) attrList->replaceAttr(BackupAttr::BACKUP_ATTR_VERSION_MD, testPoint);

  // BR_TESTPOINT_PRV:  change the PR version
  testPoint = getenv("BR_TESTPOINT_PRV");
  if (testPoint != NULL) attrList->replaceAttr(BackupAttr::BACKUP_ATTR_VERSION_PR, testPoint);

  char *attrValue = NULL;

  // return version information
  VersionInfo vInfo;
  retcode = getVersionInfo(vInfo);
  if (retcode < 0) {
    // -1 envvar not found, -2 version (major.minor.update) incorrect
    versionStr = "Unable to retrieve version information.";
    versionStr += (retcode == -2) ? "Internal error when parsing version string."
                                  : "Internal error when retrieving version info.";
    updateQueueWithOI(versionStr.data(), mdQueue, STMTHEAP);
  }

  // Display database version
  versionStr = "  Current Database Version: " + vInfo.dbVersionStr_;
  versionStr += ".   Expected Database Version: ";
  attrValue = attrList->getAttrByKey(BackupAttr::BACKUP_ATTR_VERSION_DB);
  versionStr += (attrValue == NULL) ? "Not available" : attrValue;
  updateQueueWithOI(versionStr.data(), mdQueue, STMTHEAP);

  // Display metadata version
  versionStr = "  Current Metadata Version: " + vInfo.mdVersionStr_;
  versionStr += ".   Expected Metadata Version: ";
  attrValue = attrList->getAttrByKey(BackupAttr::BACKUP_ATTR_VERSION_MD);
  versionStr += (attrValue == NULL) ? "Not available" : attrValue;
  updateQueueWithOI(versionStr.data(), mdQueue, STMTHEAP);

  // Display product version
  versionStr = "  Current Product Version : " + vInfo.prVersionStr_;
  versionStr += ".   Expected Product Version : ";
  attrValue = attrList->getAttrByKey(BackupAttr::BACKUP_ATTR_VERSION_PR);
  versionStr += (attrValue == NULL) ? "Not available" : attrValue;
  updateQueueWithOI(versionStr.data(), mdQueue, STMTHEAP);

  if (noAttrs)
    versionStr = "  Backup metadata does not contain version information.";
  else if (!attrList->formatsMatch())
    versionStr = "  Backup formats do not match.";
  else {
    retcode = attrList->backupCompatible(vInfo);
    if (retcode == 0)
      versionStr = "  Backup is current.";
    else {
      versionStr = "  Backup is not compatible with current software. ";
      versionStr += (retcode == -1) ? "Database" : ((retcode == -2) ? "Metadata" : "Product");
      versionStr += " Version mismatch.";
    }
  }
  updateQueueWithOI(versionStr.data(), mdQueue, STMTHEAP);

  int blackBoxLen = 0;
  char *blackBox = NULL;

  populateBlackBox(cliInterface, mdQueue, blackBoxLen, blackBox);

  dws->setBlackBoxLen(blackBoxLen);
  dws->setBlackBox(blackBox);

  dws->setStep(-1);
  dws->setSubstep(0);
  dws->setDone(TRUE);

  ehi->close();
  return 0;
}

short CmpSeabaseDDL::getBackupLockedObjects(RelBackupRestore *brExpr, ExeCliInterface *cliInterface,
                                            CmpDDLwithStatusInfo *dws) {
  int cliRC = 0;
  Queue *objsQueue = NULL;

  char objsQry[2000];
  str_sprintf(objsQry,
              " select cast(trim(T.text) as char(64) character set iso88591), trim(schema_name) || '.' || "
              "trim(object_name) from %s.\"%s\".%s O, %s.\"%s\".%s T where O.object_uid = T.text_uid and "
              "(O.object_type = 'PS' or bitand(O.flags, %d) != 0) and T.text_type = 10 order by 1,2",
              TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_OBJECTS, TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA,
              SEABASE_TEXT, MD_OBJECTS_BR_IN_PROGRESS);
  cliRC = cliInterface->fetchAllRows(objsQueue, objsQry, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  Queue *mdQueue = NULL;
  cliInterface->initializeInfoList(mdQueue, TRUE);

  for (int idx = 0; idx < objsQueue->numEntries(); idx++) {
    OutputInfo *vi = (OutputInfo *)objsQueue->getNext();

    NAString oper;
    NAString tag;
    if (extractFromOperTag((char *)vi->get(0), oper, tag)) continue;

    NAString objName((char *)vi->get(1));

    NAString o = "Operation:" + oper + " Tag:" + tag + " Object:" + objName;
    updateQueueWithOI(o.data(), mdQueue, STMTHEAP);
  }  // for

  // check to see if Backup restore lock exists
  ExpHbaseInterface *ehi = allocBRCEHI();
  if (ehi == NULL) return -1;

  // Call to lockHolder returns the tag that is locked, empty otherwise
  NAString lockHolder = ehi->lockHolder();
  if (lockHolder.length() > 0) {
    NAString lockedMsg = "Operation: lockCheck Tag: " + lockHolder + " Object: Snapshot Metadata";
    updateQueueWithOI(lockedMsg.data(), mdQueue, STMTHEAP);
  }
  ehi->close();

  int blackBoxLen = 0;
  char *blackBox = NULL;

  populateBlackBox(cliInterface, mdQueue, blackBoxLen, blackBox);

  dws->setBlackBoxLen(blackBoxLen);
  dws->setBlackBox(blackBox);

  dws->setStep(-1);
  dws->setSubstep(0);
  dws->setDone(TRUE);

  return 0;
}

short CmpSeabaseDDL::cleanupLockForTag(RelBackupRestore *brExpr, ExeCliInterface *cliInterface,
                                       ExpHbaseInterface *ehi) {
  NAString tagStatus = ehi->getBackupStatus(brExpr->backupTag().data());

  // unlock Objects in metadata first
  char objsQryLocked[1000];
  snprintf(
      objsQryLocked, sizeof(objsQryLocked),
      " select O.object_uid from %s.\"%s\".%s O, %s.\"%s\".%s T where O.object_uid = T.text_uid and (O.object_type = "
      "'PS' or bitand(O.flags, %d) != 0) and T.text_type = %d and substring(T.text, 1, position('(' in T.text) -1) = "
      "'%s' and O.object_type in ('PS', 'BT', 'IX') and object_name not in ('" HBASE_HIST_NAME "', '" HBASE_HISTINT_NAME
      "', '" HBASE_PERS_SAMP_NAME "') ",
      TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_OBJECTS, TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_TEXT,
      MD_OBJECTS_BR_IN_PROGRESS, COM_LOCKED_OBJECT_BR_TAG, brExpr->backupTag().data());

  char query[3000];
  snprintf(query, sizeof(query), "update %s.\"%s\".%s set flags = bitand(flags, %d) where object_uid in (%s)",
           TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_OBJECTS, ~MD_OBJECTS_BR_IN_PROGRESS, objsQryLocked);

  int cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  snprintf(query, sizeof(query), "delete from %s.\"%s\".%s where text_type=%d", TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA,
           SEABASE_TEXT, COM_LOCKED_OBJECT_BR_TAG);

  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  NAString lockedTag;
  if (unlockSnapshotMeta(ehi, brExpr->backupTag(), lockedTag) < 0) {
    populateDiags("CLEANUP",
                  "Error occurred while unlocking snapshot metadata. "
                  "Perform GET ALL LOCKED OBJECTS to verify lock has been removed. ",
                  "CmpSeabaseDDL::cleanupLockForTag", NULL, -1);
    return -1;
  }

  // check restore lock and whether the internal tag (name_SAVE_) used by restore exists
  NAString internalTagName = brExpr->backupTag() + NAString("_SAVE_");
  NAString internalTagStatus = ehi->getBackupStatus(internalTagName.data());
  NABoolean internalTagValid = (internalTagStatus == NAString("VALID")) ? TRUE : FALSE;

  if (tagStatus == NAString("VALID") && internalTagValid) {
    NAString backupType = ehi->getBackupType(brExpr->backupTag());
    int retcode = ehi->finalizeRestore(brExpr->backupTag(), backupType + "_(CLEANUP)");
    if (retcode < 0) {
      populateDiags("CLEANUP", "Error occurred while delete _SAVE_ tag. ", "CmpSeabaseDDL::cleanupLockForTag", NULL,
                    -1);
      return -1;
    }
  }

  return 0;
}

short CmpSeabaseDDL::cleanupLockedObjects(RelBackupRestore *brExpr, ExeCliInterface *cliInterface) {
  int cliRC = 0;
  int retcode = 0;
  NAString lockedTag;
  NABoolean isRestore = FALSE;

  ExpHbaseInterface *ehi = allocBRCEHI();
  if (ehi == NULL) return -1;

  if (brExpr->getCleanupLock()) {
    retcode = cleanupLockForTag(brExpr, cliInterface, ehi);
    deallocBRCEHI(ehi);
    return retcode;
  }

  // Get extended attributes
  BackupAttrList *attrList = new (STMTHEAP) BackupAttrList(STMTHEAP);
  NAString errorMsg;

  // If unable to get extended attributes, go ahead and try to cleanup anyway
  getExtendedAttributes(ehi, brExpr->backupTag(), attrList, errorMsg);

  // Since in cleanup mode, if we can't get backup attribute NULL is returned,
  // the backup owner will not be able to cleanup their locked objects.
  const char *owner = attrList->getAttrByKey(BackupAttr::BACKUP_ATTR_OWNER);

  if (verifyBRAuthority(NULL, cliInterface, FALSE /*show op*/, FALSE, SEABASE_MD_SCHEMA, FALSE, owner) != 0) {
    *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("CLEANUP LOCKED OBJECTS")
                        << DgString1("Reason: No privilege.");
    deallocBRCEHI(ehi);
    return -1;
  }

  char objsQryLocked[2000];
  char objsQryAll[2000];

  str_sprintf(objsQryAll,
              " select O.object_uid from %s.\"%s\".%s O, %s.\"%s\".%s T where O.object_uid = T.text_uid and "
              "T.text_type = 10 and substring(T.text, 1, position('(' in T.text) -1) = '%s' and O.object_type in "
              "('PS', 'BT', 'IX') and object_name not in ('" HBASE_HIST_NAME "', '" HBASE_HISTINT_NAME
              "', '" HBASE_PERS_SAMP_NAME "') ",
              TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_OBJECTS, TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA,
              SEABASE_TEXT, brExpr->backupTag().data());

  str_sprintf(
      objsQryLocked,
      " select O.object_uid from %s.\"%s\".%s O, %s.\"%s\".%s T where O.object_uid = T.text_uid and (O.object_type = "
      "'PS' or bitand(O.flags, %d) != 0) and T.text_type = 10 and substring(T.text, 1, position('(' in T.text) -1) = "
      "'%s' and O.object_type in ('PS', 'BT', 'IX') and object_name not in ('" HBASE_HIST_NAME "', '" HBASE_HISTINT_NAME
      "', '" HBASE_PERS_SAMP_NAME "') ",
      TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_OBJECTS, TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_TEXT,
      MD_OBJECTS_BR_IN_PROGRESS, brExpr->backupTag().data());

  // check if objects are locked
  Queue *objsQueue = NULL;
  cliRC = cliInterface->fetchAllRows(objsQueue,
                                     (Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL) ? objsQryAll : objsQryLocked), 0,
                                     FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    deallocBRCEHI(ehi);
    return -1;
  }
  NABoolean sqlLocked = (objsQueue->numEntries() > 0);

  // See if the internal tag (name_SAVE_) used by backup restore exists
  // returns:  "NOT FOUND" or "VALID" or "INVALID"
  NAString internalTagName = brExpr->backupTag() + NAString("_SAVE_");
  NAString internalTagStatus = ehi->getBackupStatus(internalTagName.data());
  NABoolean internalTagValid = (internalTagStatus == NAString("VALID")) ? TRUE : FALSE;
  NABoolean internalTagNF = (internalTagStatus == NAString("NOT FOUND")) ? TRUE : FALSE;

  // If saved backup is invalid, report an error, manual step is required to
  // figure out the issue and if it is recoverable.
  if (internalTagStatus == NAString("INVALID")) {
    internalTagName.prepend("Reason: Internal backup information is invalid for ");
    *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("CLEANUP LOCKED OBJECTS") << DgString1(internalTagName.data());
    deallocBRCEHI(ehi);
    return -1;
  }

  // check if snapshot metadata is locked or unavaliable
  lockedTag = ehi->lockHolder();
  NABoolean snapshotLocked = (lockedTag.length() > 0);

  // If nothing locked, then nothing to cleanup so return
  if (!sqlLocked && !snapshotLocked && internalTagNF) {
    deallocBRCEHI(ehi);
    return 0;
  }

  // If SQL metadata is locked, retrieve failed operation from metadata
  char query[4000];
  if (sqlLocked) {
    str_sprintf(query,
                "select distinct substring(text, position('(' in text)+1, octet_length(text) - position('(' in "
                "text)-1) from %s.\"%s\".%s where text_type = 10 and text_uid in (%s)",
                TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_TEXT, objsQryLocked);
    cliRC = cliInterface->fetchAllRows(objsQueue, query, 0, FALSE, FALSE, TRUE);
    if (cliRC < 0) {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      deallocBRCEHI(ehi);
      return -1;
    }

    if (objsQueue->numEntries() > 1)  // only one entry should be returned
    {
      deallocBRCEHI(ehi);
      return -1;
    }

    objsQueue->position();
    OutputInfo *vi = (OutputInfo *)objsQueue->getNext();
    if ((strcmp((char *)vi->get(0), "RESTORE") == 0) && internalTagValid) isRestore = TRUE;
  }

  // SQL locks do not exist, check tag status to determine action
  else {
    NAString tagStatus = ehi->getBackupStatus(brExpr->backupTag().data());

    if (tagStatus == NAString("VALID") && internalTagValid)
      isRestore = TRUE;
    else
      isRestore = FALSE;
  }

  NAString backupSch;
  genBackupSchemaName(brExpr->backupTag(), backupSch);
  NAArray<HbaseStr> *objectsList = NULL;

  NAString backupType = ehi->getBackupType(brExpr->backupTag());
  if (isRestore && (!backupType.isNull()) && internalTagValid) {
    int restoreThreads = CmpCommon::getDefaultNumeric(TRAF_RESTORE_PARALLEL);

    // create namespaces
    if (createNamespacesForRestore(brExpr, NULL, NULL,
                                   FALSE,  // dont create reserved NS
                                   ehi, restoreThreads)) {
      deallocBRCEHI(ehi);
      return -1;
    }

    if (truncateBackupMD(backupSch, cliInterface)) {
      deallocBRCEHI(ehi);
      return -1;
    }

    // call restoreObjects to restore original objects that existed
    // before RESTORE operation was done.
    objectsList = ehi->restoreObjects(brExpr->backupTag(), NULL, NULL, FALSE, FALSE,
                                      FALSE,  // dont save current objects before restore
                                      TRUE,   // restore saved objects
                                      restoreThreads, CmpCommon::getDefaultNumeric(TRAF_BREI_PROGRESS_UPDATE_DELAY));

    // objectsList could be null.  This can happen when cleaning up after
    // failed privilege or authID checks.  These failures happens after the
    // metadata objects have been restored but before any user objects restored.
  }

  NABoolean xnWasStartedHere = FALSE;
  if (beginXnIfNotInProgress(cliInterface, xnWasStartedHere)) {
    goto label_error_return;
  }

  // If objects exists, clean them up
  if (objectsList) {
    // NAString backupSch;
    // genBackupSchemaName(brExpr->backupTag(), backupSch);

    NAString objUIDsWherePred("1 = 1");
    cliRC = deleteTgtObjectsFromMD(brExpr, cliInterface, objUIDsWherePred, (char *)SEABASE_MD_SCHEMA,
                                   (char *)SEABASE_PRIVMGR_SCHEMA, (char *)backupSch.data());
    if (cliRC < 0) {
      goto label_error_return;
    }

    cliRC = copyBRSrcToTgt(brExpr, cliInterface, objUIDsWherePred.data(), backupSch.data(), backupSch.data(),
                           SEABASE_MD_SCHEMA, SEABASE_PRIVMGR_SCHEMA);
    if (cliRC < 0) {
      *CmpCommon::diags() << DgSqlCode(-5050) << DgString0("CLEANUP")
                          << DgString1("Reason: Error occured during update from backup metadata.");

      goto label_error_return;
    }
  }  // isRestore

  // reset metadata
  str_sprintf(query, "update %s.\"%s\".%s set flags = bitand(flags, %d) where object_uid in (%s)", TRAFODION_SYSCAT_LIT,
              SEABASE_MD_SCHEMA, SEABASE_OBJECTS, ~MD_OBJECTS_BR_IN_PROGRESS, objsQryLocked);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_error_return;
  }

  str_sprintf(query, "delete from %s.\"%s\".%s where text_uid in (%s) and text_type = 10", TRAFODION_SYSCAT_LIT,
              SEABASE_MD_SCHEMA, SEABASE_TEXT, objsQryAll);
  cliRC = cliInterface->executeImmediate(query);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_error_return;
  }

  if (endXnIfStartedHere(cliInterface, xnWasStartedHere, 0)) {
    goto label_error_return;
  }

  // finalize restore , this will drop tag_SAVE backup for good in restore case.!!
  // Note that this method finalizeRestore() is currently overloaded.
  // It is called to cleanup on the java size whether it is backup
  // that has failed or restore. Some time in the future, finalizeBackup
  // or finalizeRestore can be called with cleanup flag appropriately.
  // FinalizeRestore still needs to know if this call is made in cleanup mode.
  // Appending "CLEANUP" to backupType is currently a hack to convey this message
  // to java side.
  if (isRestore && internalTagValid) {
    retcode = ehi->finalizeRestore(brExpr->backupTag(), backupType + "_(CLEANUP)");
    if (retcode < 0) {
      populateDiags("CLEANUP", "Error returned during finalization of restore.", "ExpHbaseInterface::finalizeRestore",
                    getHbaseErrStr(-retcode), -retcode);

      goto label_error_return;
    }
  } else {
    if (deleteSnapshot(ehi, (char *)"CLEANUP", (char *)brExpr->backupTag().data(), TRUE)) {
      goto label_error_return;
    }
  }

  // Remove Snapshot meta lock (if exists) for this tag
  if (unlockSnapshotMeta(ehi, brExpr->backupTag(), lockedTag) < 0) {
    populateDiags("CLEANUP",
                  "Error occurred while unlocking snapshot metadata. "
                  "Perform GET ALL LOCKED OBJECTS to verify lock has been removed. ",
                  "ExpHbaseInterface::restoreSubset", NULL, -1);
    goto label_error_return;
  }

  deallocBRCEHI(ehi);
  return 0;

label_error_return:

  deallocBRCEHI(ehi);
  if (endXnIfStartedHere(cliInterface, xnWasStartedHere, (cliRC < 0 ? -1 : 0))) return -1;

  return -1;
}

// ----------------------------------------------------------------------------
// method verifyBRAuthority
//
// see if the current user has privileges to perform request
//   if elevated user - yes
//   if user owns or is a member of a role that owns all objects - yes
//   if user has MANAGE_LOAD (or SHOW for get like commands) privilege - yes
//
// return:  0 - yes
//         -1 - no or unexpected error occurred
// ----------------------------------------------------------------------------
short CmpSeabaseDDL::verifyBRAuthority(const char *privsWherePred, ExeCliInterface *cliInterface, NABoolean allowShow,
                                       NABoolean isBackup, const char *schToUse, const NABoolean checkSpecificPrivs,
                                       const char *owner) {
  if (hasAccess(owner, allowShow)) return 0;

  if (privsWherePred == NULL) {
    *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);
    return -1;
  }

  // Read metadata to get list of object to process
  int userID = ComUser::getCurrentUser();
  NAString objName("all objects in set");

  int cliRC = 0;
  Queue *objsQueue = NULL;
  char query[500 + strlen(privsWherePred)];
  str_sprintf(query,
              " select distinct schema_owner, object_owner, object_type, "
              "object_uid, schema_name, object_name from %s.\"%s\".%s O where %s ",
              TRAFODION_SYSCAT_LIT, schToUse, SEABASE_OBJECTS, privsWherePred);

  int diagsMark = CmpCommon::diags()->mark();
  cliRC = cliInterface->fetchAllRows(objsQueue, query, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }
  CmpCommon::diags()->rewind(diagsMark);

  if (objsQueue->numEntries() == 0) return 0;

  // Since may be reading from backup schema instead of "_PRIVMGR_ MD_"
  // or "_MD_" schema, call constructor with correct info
  std::string schMDLoc = TRAFODION_SYSCAT_LIT;
  schMDLoc.append(".\"");
  std::string schPMDLoc = schMDLoc;
  schMDLoc.append(schToUse);

  if (strcmp(schToUse, SEABASE_MD_SCHEMA) == 0)
    schPMDLoc.append(SEABASE_PRIVMGR_SCHEMA);
  else
    schPMDLoc.append(schToUse);
  schMDLoc.append("\"");
  schPMDLoc.append("\"");

  PrivMgrCommands userCmd(schMDLoc, schPMDLoc, CmpCommon::diags(), PrivMgr::PRIV_INITIALIZED);

  // Check to see if current user has priv on each object in the request
  PrivMgrUserPrivs privInfo;
  for (int idx = 0; idx < objsQueue->numEntries(); idx++) {
    OutputInfo *vi = (OutputInfo *)objsQueue->getNext();

    // if current user is schema/owner, then has priv
    if (userID == *(int *)vi->get(0) || userID == *(int *)vi->get(1)) continue;

    char objectTypeLit[3] = {0};
    strcpy(objectTypeLit, (char *)vi->get(2));
    ComObjectType objType = PrivMgr::ObjectLitToEnum(objectTypeLit);

    // Don't check privileges on non-securable objects
    if (!PrivMgr::isSecurableObject(objType)) continue;

    // We backup individual objects, not schemas so we can skip checks
    // on schema objects
    if (objType == COM_SHARED_SCHEMA_OBJECT || objType == COM_PRIVATE_SCHEMA_OBJECT) continue;

    // Don't check for privs on histogram tables either
    long objUID = *(long *)vi->get(3);
    objName = (char *)vi->get(5);
    if (isHistogramTable(objName)) continue;

    // Read privilege metadata to see if user has privs for base tables
    // For other objects, such as views and libraries, you must be the owner.
    if (checkSpecificPrivs && objType == COM_BASE_TABLE_OBJECT) {
      objName.prepend('.');
      objName.prepend((char *)vi->get(4));

      int diagsMark = CmpCommon::diags()->mark();
      if (userCmd.getPrivileges(objUID, objType, userID, privInfo) == PrivStatus::STATUS_ERROR) return -1;
      CmpCommon::diags()->rewind(diagsMark);

      if (privInfo.hasPriv(SELECT_PRIV) && privInfo.hasPriv(INSERT_PRIV)) continue;

      *CmpCommon::diags() << DgSqlCode(-CAT_INSUFFICIENT_PRIV_ON_OBJECT) << DgTableName(objName);
      return -1;
    } else {
      *CmpCommon::diags() << DgSqlCode(-CAT_INSUFFICIENT_PRIV_ON_OBJECT) << DgTableName(objName);
      return -1;
    }
  }

  return 0;
}

// ----------------------------------------------------------------------------
// method: getExtendedAttributes
//
// Starting in release 2.7, backups store a set of attributes specific to
// SQL in extendedAttributes.  If extendedAttributes exist, go ahead and
// unpack them and create the list of backup attributes.  Return an error
// if unable to unpack the attributes successfully.
//
// If attrList is empty, then extended attributes were not found or an error
// occurred. If an error occurred, errorMsg is populated with the details.
//
// returns:
//    0 - successful
//   -1 - unexpected error when extracting attributes
// ----------------------------------------------------------------------------
short CmpSeabaseDDL::getExtendedAttributes(ExpHbaseInterface *ehi, const char *backupTag, BackupAttrList *attrList,
                                           NAString &errorMsg) {
  short retcode = 0;

  if (backupTag == NULL) return 0;

  NAString extendedAttributes = ehi->getExtendedAttributes(backupTag);

  NAString logMsg("Extended attributes from backup tag ");
  logMsg += backupTag + NAString(": ") + extendedAttributes;
  QRLogger::log(CAT_SQL_EXE, LL_DEBUG, "%s", logMsg.data());

  if (extendedAttributes.length() > 0) {
    retcode = attrList->unpack(extendedAttributes.data(), errorMsg);
    if (retcode < 0) {
      errorMsg.prepend("Error returned retrieving backup attributes, ");
      return -1;
    }
  }
  return 0;
}

// ----------------------------------------------------------------------------
// method: checkCompatibility
//
// verifies that the requester can perform the restore, export, or import
// request.
//
// returns:
//   1 - compatibility not checked
//   0 - compatible
//  -1 - unable to retrieve version information
//  -2 - backup formats do not match
//  -3 - version incompatible
// ----------------------------------------------------------------------------
short CmpSeabaseDDL::checkCompatibility(BackupAttrList *attrList, NAString &errorMsg) {
  if (attrList->entries() == 0) {
    errorMsg = "No attribute founded in this tag, please check wether this tag is invalid or no snapshots. ";
    return -3;
  }

  // Verify that backup formats are compatible
  if (!attrList->formatsMatch()) {
    errorMsg = "Backup formats do not match.";
    return -2;
  }

  // Get version information
  VersionInfo vInfo;
  short retcode = getVersionInfo(vInfo);
  if (retcode < 0) {
    // -1 envvar not found, -2 version (major.minor.update) incorrect
    errorMsg = "Unable to retrieve version information.";
    errorMsg += (retcode == -2) ? "Internal error when parsing version string."
                                : "Internal error when retrieving version info.";
    return -1;
  }

  // Verify that versions are compatible
  retcode = attrList->backupCompatible(vInfo);
  if (retcode < 0) {
    errorMsg = "Backup is not compatible with current software. ";
    if (retcode == -1)
      errorMsg += "Unable to retrieve version information. ";
    else {
      errorMsg += (retcode = -2) ? "Database" : ((retcode = -3) ? "Metadata" : "Product");
      errorMsg += " version mismatch.";
    }
    return -3;
  }
  return 0;
}

int CmpSeabaseDDL::getOwnerList(ExpHbaseInterface *ehi, NAArray<HbaseStr> *backupList,
                                  std::vector<string> &ownerList) {
  NAString errorMsg;
  int maxOwnerLen = strlen("Not Available");
  if (backupList && backupList->entries() > 0) {
    for (int i = 0; i < backupList->entries(); i++) {
      std::string s(backupList->at(i).val);
      std::vector<string> tokens;
      extractTokens(s, tokens);
      if ((tokens.empty()) || (tokens.size() != 4)) continue;

      if (CmpCommon::getDefault(TRAF_BACKUP_SKIP_GET_TAGS_OWNER_LOOKUP) == DF_ON)
        ownerList.push_back(DB__ROOT);
      else {
        // Get owner from backup metadata
        // If owner not found, then set owner to "Not Available"
        BackupAttrList attrList;
        getExtendedAttributes(ehi, tokens[0].c_str(), &attrList, errorMsg);
        const char *owner = attrList.getAttrByKey(BackupAttr::BACKUP_ATTR_OWNER);
        if (owner)
          ownerList.push_back(owner);
        else {
          if (strcmp(tokens[3].c_str(), "SYSTEM") == 0)
            ownerList.push_back("DB__ROOT");
          else
            ownerList.push_back("Not Available");
        }
        if (owner && strlen(owner) > maxOwnerLen) maxOwnerLen = strlen(owner);
      }
    }
  }
  return maxOwnerLen;
}

short CmpSeabaseDDL::getUsersAndRoles(ExeCliInterface *cliInterface, const char *objUIDsWherePred, const char *srcSch,
                                      int &numUR, NAString &userOrRoles) {
  int cliRC = 0;
  char query[1000 + strlen(objUIDsWherePred)];
  str_sprintf(query,
              "select distinct trim(grantee_name), grantee_id from ("
              "(select schema_uid as object_uid, grantee_name, grantee_id from %s.\"%s\".%s "
              "union select object_uid, grantee_name, grantee_id from %s.\"%s\".%s "
              "union select object_uid, grantee_name, grantee_id from %s.\"%s\".%s ) "
              "as P join (select O.object_uid from %s.\"%s\".%s as O where %s) as OU "
              "on P.object_uid = OU.object_uid) order by grantee_id ",
              TRAFODION_SYSCAT_LIT, SEABASE_PRIVMGR_SCHEMA, PRIVMGR_SCHEMA_PRIVILEGES, TRAFODION_SYSCAT_LIT,
              SEABASE_PRIVMGR_SCHEMA, PRIVMGR_OBJECT_PRIVILEGES, TRAFODION_SYSCAT_LIT, SEABASE_PRIVMGR_SCHEMA,
              PRIVMGR_COLUMN_PRIVILEGES, TRAFODION_SYSCAT_LIT, srcSch, SEABASE_OBJECTS, objUIDsWherePred);

  numUR = 0;
  Queue *urQueue = NULL;
  cliRC = cliInterface->fetchAllRows(urQueue, query, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  if (urQueue->numEntries() == 0) {
    return 0;
  }

  numUR = urQueue->numEntries();
  for (int idx = 0; idx < urQueue->numEntries(); idx++) {
    OutputInfo *vi = (OutputInfo *)urQueue->getNext();
    userOrRoles += "('";
    userOrRoles += (char *)vi->get(0);
    userOrRoles += "':";
    userOrRoles += Int64ToNAString(*(long *)vi->get(1));
    userOrRoles += ")";
    if (idx < urQueue->entries() - 1) userOrRoles += ":";
  }

  return 0;
}

/*
 * string operation: replace all specified characters in string
 */
static void replace_all_distinct(NAString &input, const NAString &old_value, const NAString &new_value) {
  while (TRUE) {
    StringPos pos = input.index(old_value.data(), old_value.length(), 0, NAString::exact);
    if (pos != NA_NPOS) {
      input.replace(pos, old_value.length(), new_value, new_value.length());
    } else {
      break;
    }
  }
}

short CmpSeabaseDDL::checkUsersAndRoles(ExeCliInterface *cliInterface, const char *userOrRoles, NAString &lackedUR,
                                        std::map<int, NAString> &userMap) {
  int cliRC = 0;
  /*
  str_sprintf(query, "select trim(grantee_name), grantee_id from ( "
              "select grantee_name, grantee_id from (values "
              "(cast('NULL' as VARCHAR(256 BYTES) CHARACTER SET UTF8), cast(0 as LARGEINT)), "
              "%s) ur(grantee_name, grantee_id)) as U left join %s.\"%s\".%s as A "
              "on upper(trim(U.grantee_name))=upper(trim(A.auth_db_name)) and "
              "U.grantee_id=A.auth_id "
              "where A.auth_id is null", urStr.data(),
              TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_AUTHS);
  */

  // user_roles string is : ('DB__ROOT':33333):('TESTUSER2':33334):('TESTUSER1':33335)
  // transform string to : ('DB__ROOT',33333),('TESTUSER2',33334),('TESTUSER1',33335)
  NAString user_roles = NAString(userOrRoles);
  replace_all_distinct(user_roles, NAString(":"), NAString(","));

  char query[1000 + user_roles.length()];
  cliRC = cliInterface->holdAndSetCQDs(
      "NESTED_JOINS, MERGE_JOINS, HASH_JOINS, TRAF_BR_LOCKED_OBJECTS_QUERY_TYPE",
      "NESTED_JOINS 'OFF', MERGE_JOINS 'OFF', HASH_JOINS 'ON', TRAF_BR_LOCKED_OBJECTS_QUERY_TYPE '1' ");
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  str_sprintf(query,
              "select trim(grantee_name) from ( "
              "select grantee_name, grantee_id from (values "
              "%s) ur(grantee_name, grantee_id)) as U left join %s.\"%s\".%s as A "
              "on upper(trim(U.grantee_name))=upper(trim(A.auth_db_name)) "
              "where A.auth_id is null",
              user_roles.data(), TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_AUTHS);

  Queue *userQueue = NULL;
  cliRC = cliInterface->fetchAllRows(userQueue, query, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    goto label_checkUser_return;
  }

  for (int idx = 0; idx < userQueue->numEntries(); idx++) {
    OutputInfo *vi = (OutputInfo *)userQueue->getNext();
    NAString name((char *)vi->get(0));
    if (name == "NULL") {
      continue;
    }
    lackedUR += name;
    if (idx < userQueue->entries() - 1) lackedUR += ",";
  }

  if (NOT lackedUR.isNull()) return -1;

  str_sprintf(query,
              "select trim(grantee_name), grantee_id from ( "
              "select grantee_name, grantee_id from (values "
              "%s) ur(grantee_name, grantee_id)) as U left join %s.\"%s\".%s as A "
              "on upper(trim(U.grantee_name))=upper(trim(A.auth_db_name)) "
              "and U.grantee_id=A.auth_id "
              "where A.auth_id is null",
              user_roles.data(), TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_AUTHS);

  cliRC = cliInterface->fetchAllRows(userQueue, query, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    goto label_checkUser_return;
  }

  for (int idx = 0; idx < userQueue->numEntries(); idx++) {
    OutputInfo *vi = (OutputInfo *)userQueue->getNext();
    NAString name((char *)vi->get(0));
    int id = *(int *)vi->get(1);
    if (id == 0 && name == "NULL") {
      continue;
    }
    userMap.insert(make_pair(id, name));
  }

  cliInterface->restoreCQDs("MERGE_JOINS, NESTED_JOINS, HASH_JOINS, TRAF_BR_LOCKED_OBJECTS_QUERY_TYPE");

  return cliRC;

label_checkUser_return:

  cliInterface->restoreCQDs("MERGE_JOINS, NESTED_JOINS, HASH_JOINS, TRAF_BR_LOCKED_OBJECTS_QUERY_TYPE");
  cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
  return -1;
}

short CmpSeabaseDDL::createBackupRepos(ExeCliInterface *cliInterface) {
  int cliRC = 0;

  char queryBuf[20000];

  NABoolean xnWasStartedHere = FALSE;

  if (CmpCommon::getDefault(TRAF_BACKUP_REPOSITORY_SUPPORT) == DF_OFF) return 0;

  if (beginXnIfNotInProgress(cliInterface, xnWasStartedHere)) goto label_error;

  // Create the _REPOS_ schema
  str_sprintf(queryBuf, "create schema if not exists %s.\"%s\" namespace '%s' ; ", TRAFODION_SYSCAT_LIT,
              SEABASE_REPOS_SCHEMA, ComGetReservedNamespace(SEABASE_REPOS_SCHEMA).data());

  cliRC = cliInterface->executeImmediate(queryBuf);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
  }

  if (endXnIfStartedHere(cliInterface, xnWasStartedHere, cliRC) < 0) goto label_error;

  for (int i = 0; i < sizeof(allBackupReposInfo) / sizeof(MDUpgradeInfo); i++) {
    const MDUpgradeInfo &rti = allBackupReposInfo[i];

    if (!rti.newName) continue;

    for (int j = 0; j < NUM_MAX_PARAMS; j++) {
      param_[j] = NULL;
    }

    const QString *qs = NULL;
    int sizeOfqs = 0;

    qs = rti.newDDL;
    sizeOfqs = rti.sizeOfnewDDL;

    int qryArraySize = sizeOfqs / sizeof(QString);
    char *gluedQuery;
    int gluedQuerySize;
    glueQueryFragments(qryArraySize, qs, gluedQuery, gluedQuerySize);

    param_[0] = TRAFODION_SYSCAT_LIT;
    param_[1] = SEABASE_REPOS_SCHEMA;

    str_sprintf(queryBuf, gluedQuery, param_[0], param_[1]);
    NADELETEBASICARRAY(gluedQuery, STMTHEAP);

    if (beginXnIfNotInProgress(cliInterface, xnWasStartedHere)) goto label_error;

    cliRC = cliInterface->executeImmediate(queryBuf);
    if (cliRC == -1390)  // table already exists
    {
      // ignore error.
      cliRC = 0;
    } else if (cliRC < 0) {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    }

    if (endXnIfStartedHere(cliInterface, xnWasStartedHere, cliRC) < 0) goto label_error;

  }  // for

  return 0;

label_error:
  return -1;
}

short CmpSeabaseDDL::dropBackupRepos(ExeCliInterface *cliInterface) {
  int cliRC = 0;
  NABoolean xnWasStartedHere = FALSE;
  char queryBuf[1000];

  if (CmpCommon::getDefault(TRAF_BACKUP_REPOSITORY_SUPPORT) == DF_OFF) return 0;

  for (int i = 0; i < sizeof(allBackupReposInfo) / sizeof(MDUpgradeInfo); i++) {
    const MDUpgradeInfo &rti = allBackupReposInfo[i];

    str_sprintf(queryBuf, "drop table if exists %s.\"%s\".%s cascade; ", TRAFODION_SYSCAT_LIT, SEABASE_REPOS_SCHEMA,
                rti.newName);

    if (beginXnIfNotInProgress(cliInterface, xnWasStartedHere)) {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      return -1;
    }

    cliRC = cliInterface->executeImmediate(queryBuf);
    if (cliRC == -1389)  // table doesn't exist
    {
      // ignore the error.
      cliRC = 0;
    } else if (cliRC < 0) {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    }

    if (endXnIfStartedHere(cliInterface, xnWasStartedHere, cliRC) < 0) {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      return -1;
    }

    if (cliRC < 0) {
      return -1;
    }
  }

  return 0;
}

void CmpSeabaseDDL::processBackupRepository(NABoolean createR, NABoolean dropR, NABoolean upgradeR) {
  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());

  if (CmpCommon::getDefault(TRAF_BACKUP_REPOSITORY_SUPPORT) == DF_OFF) return;

  if (createR)
    createBackupRepos(&cliInterface);
  else if (dropR)
    dropBackupRepos(&cliInterface);

  return;
}

short CmpSeabaseDDL::cleanupBackupMetadata(ExeCliInterface *cliInterface, NAString &backupTag) {
  int cliRC = 0;
  char objectsUID[200];
  str_sprintf(objectsUID, "select object_uid from %s.\"%s\".%s where schema_name = '%s'", getSystemCatalog(),
              SEABASE_MD_SCHEMA, SEABASE_OBJECTS, backupTag.data());

  char deleteSql[400];
  // delete from tables
  str_sprintf(deleteSql, "delete from %s.\"%s\".%s where table_uid in (%s)", getSystemCatalog(), SEABASE_MD_SCHEMA,
              SEABASE_TABLES, objectsUID);
  cliRC = cliInterface->executeImmediate(deleteSql);
  if (cliRC < 0) goto label_return;
  // delete from keys
  str_sprintf(deleteSql, "delete from %s.\"%s\".%s where object_uid in (%s)", getSystemCatalog(), SEABASE_MD_SCHEMA,
              SEABASE_KEYS, objectsUID);
  cliRC = cliInterface->executeImmediate(deleteSql);
  if (cliRC < 0) goto label_return;
  // delete from columns
  str_sprintf(deleteSql, "delete from %s.\"%s\".%s where object_uid in (%s)", getSystemCatalog(), SEABASE_MD_SCHEMA,
              SEABASE_COLUMNS, objectsUID);
  cliRC = cliInterface->executeImmediate(deleteSql);
  if (cliRC < 0) goto label_return;
  // delete from text
  str_sprintf(deleteSql, "delete from %s.\"%s\".%s where text_uid in (%s)", getSystemCatalog(), SEABASE_MD_SCHEMA,
              SEABASE_TEXT, objectsUID);
  cliRC = cliInterface->executeImmediate(deleteSql);
  if (cliRC < 0) goto label_return;
  // delete from indexes
  str_sprintf(deleteSql, "delete from %s.\"%s\".%s where index_uid in (%s)", getSystemCatalog(), SEABASE_MD_SCHEMA,
              SEABASE_INDEXES, objectsUID);
  cliRC = cliInterface->executeImmediate(deleteSql);
  if (cliRC < 0) goto label_return;
  // delete from views
  str_sprintf(deleteSql, "delete from %s.\"%s\".%s where view_uid in (%s)", getSystemCatalog(), SEABASE_MD_SCHEMA,
              SEABASE_VIEWS, objectsUID);
  cliRC = cliInterface->executeImmediate(deleteSql);
  if (cliRC < 0) goto label_return;
  // delete from view_usage
  str_sprintf(deleteSql, "delete from %s.\"%s\".%s where using_view_uid in (%s)", getSystemCatalog(), SEABASE_MD_SCHEMA,
              SEABASE_VIEWS_USAGE, objectsUID);
  cliRC = cliInterface->executeImmediate(deleteSql);
  if (cliRC < 0) goto label_return;
  // delete from routines
  str_sprintf(deleteSql, "delete from %s.\"%s\".%s where udr_uid in (%s)", getSystemCatalog(), SEABASE_MD_SCHEMA,
              SEABASE_ROUTINES, objectsUID);
  cliRC = cliInterface->executeImmediate(deleteSql);
  if (cliRC < 0) goto label_return;
  // delete from libraries
  str_sprintf(deleteSql, "delete from %s.\"%s\".%s where library_uid in (%s)", getSystemCatalog(), SEABASE_MD_SCHEMA,
              SEABASE_LIBRARIES, objectsUID);
  cliRC = cliInterface->executeImmediate(deleteSql);
  if (cliRC < 0) goto label_return;
  // delete from libraries_usage
  str_sprintf(deleteSql, "delete from %s.\"%s\".%s where used_udr_uid in (%s)", getSystemCatalog(), SEABASE_MD_SCHEMA,
              SEABASE_LIBRARIES_USAGE, objectsUID);
  cliRC = cliInterface->executeImmediate(deleteSql);
  if (cliRC < 0) goto label_return;
  // delete from lob_columns
  str_sprintf(deleteSql, "delete from %s.\"%s\".%s where table_uid in (%s)", getSystemCatalog(), SEABASE_MD_SCHEMA,
              SEABASE_LOB_COLUMNS, objectsUID);
  cliRC = cliInterface->executeImmediate(deleteSql);
  if (cliRC < 0) goto label_return;
  // delete from seq_gen
  str_sprintf(deleteSql, "delete from %s.\"%s\".%s where seq_uid in (%s)", getSystemCatalog(), SEABASE_MD_SCHEMA,
              SEABASE_SEQ_GEN, objectsUID);
  cliRC = cliInterface->executeImmediate(deleteSql);
  if (cliRC < 0) goto label_return;
  // delete from schema_privileges
  str_sprintf(deleteSql, "delete from %s.\"%s\".%s where schema_uid in (%s)", getSystemCatalog(),
              SEABASE_PRIVMGR_SCHEMA, PRIVMGR_SCHEMA_PRIVILEGES, objectsUID);
  cliRC = cliInterface->executeImmediate(deleteSql);
  if (cliRC < 0) goto label_return;
  // delete from object_privileges
  str_sprintf(deleteSql, "delete from %s.\"%s\".%s where object_uid in (%s)", getSystemCatalog(),
              SEABASE_PRIVMGR_SCHEMA, PRIVMGR_OBJECT_PRIVILEGES, objectsUID);
  cliRC = cliInterface->executeImmediate(deleteSql);
  if (cliRC < 0) goto label_return;
  // delete from column_privileges
  str_sprintf(deleteSql, "delete from %s.\"%s\".%s where object_uid in (%s)", getSystemCatalog(),
              SEABASE_PRIVMGR_SCHEMA, PRIVMGR_COLUMN_PRIVILEGES, objectsUID);
  cliRC = cliInterface->executeImmediate(deleteSql);
  if (cliRC < 0) goto label_return;
  // delete from objects
  str_sprintf(deleteSql, "delete from %s.\"%s\".%s where object_uid in (%s)", getSystemCatalog(), SEABASE_MD_SCHEMA,
              SEABASE_OBJECTS, objectsUID);
  cliRC = cliInterface->executeImmediate(deleteSql);
  if (cliRC < 0) goto label_return;

  return 0;

label_return:
  cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
  return -1;
}

// used for cleanup linked backup metadata
// when drop regular backup tags force, all the incremental
// backup tags which is related will be droped
short CmpSeabaseDDL::cleanupLinkedBackupMD(ExeCliInterface *cliInterface, NAArray<HbaseStr> *backupList) {
  long time0, time1;
  time0 = NA_JulianTimestamp();
  for (int i = 0; i < backupList->entries(); i++) {
    int len = backupList->at(i).len;
    char *val = backupList->at(i).val;
    NAString backupTag(val, len);
    NAString backupSch;
    genBackupSchemaName(backupTag, backupSch);

    ComObjectType objectType;
    int schemaOwnerID = 0;
    long schemaUID = getObjectTypeandOwner(cliInterface, TRAFODION_SYSCAT_LIT, backupSch.data(),
                                            SEABASE_SCHEMA_OBJECTNAME, objectType, schemaOwnerID);
    // doesn't exist
    if (schemaUID == -1) {
      continue;
    }
    if (cleanupBackupMetadata(cliInterface, backupSch) < 0) {
      return -1;
    }
  }
  time1 = NA_JulianTimestamp();
  char timeStrBuf[100];
  ExExeUtilTcb::getTimeAsString((time1 - time0), timeStrBuf, TRUE);
  QRLogger::log(CAT_SQL_EXE, LL_INFO, "cleanupBackupMetadata costs: %s", timeStrBuf);
  return 0;
}

/*
 * Function used to check user_id conflicts
 */
short CmpSeabaseDDL::checkBackupAuthIDs(ExeCliInterface *cliInterface) {
  NABoolean xnWasStartedHere = FALSE;
  if (beginXnIfNotInProgress(cliInterface, xnWasStartedHere)) return -1;

  NASet<NAString> errorAuthIDList(STMTHEAP);
  if (checkAndFixupAuthIDs(cliInterface, TRUE /*check only*/, errorAuthIDList) < 0) {
    endXnIfStartedHere(cliInterface, xnWasStartedHere, 0);
    return -1;
  }

  if (endXnIfStartedHere(cliInterface, xnWasStartedHere, 0)) return -1;

  return errorAuthIDList.entries();
}

/*
 * Function used to check user_id conflicts
 */
short CmpSeabaseDDL::fixBackupAuthIDs(ExeCliInterface *cliInterface, NAString &backupScheName,
                                      std::map<int, NAString> &unMatchedUserIDs) {
  int cliRC = 0;
  char updateAuths[1024];
  // update _BACKUP_schema.OBJECT_PRIVILEGES, _BACKUP_schema.COLUMN_PRIVILEGES,
  // _BACKUP_schema.SCHEMA_PRIVILEGES, set GRANTEE_ID to a temporary value, because
  // GRANTEE_ID is a unique constraint.
  // update _BACKUP_schema.OBJECTS set object_owner, schema_owner
  // for example: backup auth id is ('TESTUSER2',33334),('TESTUSER1',33335)
  // auth id in current cluster is ('TESTUSER1',33334),('TESTUSER2',33335)
  // if we set auth id to 33334 for user 'TESTUSER1' would return unique constraint error.

  snprintf(updateAuths, sizeof(updateAuths),
           "select max(auth_id) from %s.\"%s\".%s "
           "where auth_type = 'U';",
           getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_AUTHS);

  cliRC = cliInterface->fetchRowsPrologue(updateAuths, TRUE /*no exec*/);
  if (cliRC < 0) return -1;
  cliRC = cliInterface->clearExecFetchClose(NULL, 0);
  if (cliRC < 0) return -1;

  char *ptr = NULL;
  int len = 0;
  cliInterface->getPtrAndLen(1, ptr, len);
  int maxUserID = *(int *)ptr;

  std::map<int, NAString>::iterator iter = unMatchedUserIDs.begin();
  while (iter != unMatchedUserIDs.end()) {
    int user_id = iter->first;
    NAString user_name = iter->second;

    int tempUserID = user_id + maxUserID;

    snprintf(updateAuths, sizeof(updateAuths),
             "update %s.\"%s\".%s set GRANTEE_ID = %d "
             "where GRANTEE_NAME = '%s';",
             getSystemCatalog(), backupScheName.data(), PRIVMGR_OBJECT_PRIVILEGES, tempUserID, user_name.data());
    cliRC = cliInterface->executeImmediate(updateAuths);
    if (cliRC < 0) return -1;

    snprintf(updateAuths, sizeof(updateAuths),
             "update %s.\"%s\".%s set GRANTOR_ID = %d "
             "where GRANTOR_NAME = '%s';",
             getSystemCatalog(), backupScheName.data(), PRIVMGR_OBJECT_PRIVILEGES, tempUserID, user_name.data());
    cliRC = cliInterface->executeImmediate(updateAuths);
    if (cliRC < 0) return -1;

    snprintf(updateAuths, sizeof(updateAuths),
             "update %s.\"%s\".%s set GRANTEE_ID = %d "
             "where GRANTEE_NAME = '%s';",
             getSystemCatalog(), backupScheName.data(), PRIVMGR_COLUMN_PRIVILEGES, tempUserID, user_name.data());
    cliRC = cliInterface->executeImmediate(updateAuths);
    if (cliRC < 0) return -1;

    snprintf(updateAuths, sizeof(updateAuths),
             "update %s.\"%s\".%s set GRANTOR_ID = %d "
             "where GRANTOR_NAME = '%s';",
             getSystemCatalog(), backupScheName.data(), PRIVMGR_COLUMN_PRIVILEGES, tempUserID, user_name.data());
    cliRC = cliInterface->executeImmediate(updateAuths);
    if (cliRC < 0) return -1;

    snprintf(updateAuths, sizeof(updateAuths),
             "update %s.\"%s\".%s set GRANTEE_ID = %d "
             "where GRANTEE_NAME = '%s';",
             getSystemCatalog(), backupScheName.data(), PRIVMGR_SCHEMA_PRIVILEGES, tempUserID, user_name.data());
    cliRC = cliInterface->executeImmediate(updateAuths);
    if (cliRC < 0) return -1;

    snprintf(updateAuths, sizeof(updateAuths),
             "update %s.\"%s\".%s set GRANTOR_ID = %d "
             "where GRANTOR_NAME = '%s';",
             getSystemCatalog(), backupScheName.data(), PRIVMGR_SCHEMA_PRIVILEGES, tempUserID, user_name.data());
    cliRC = cliInterface->executeImmediate(updateAuths);
    if (cliRC < 0) return -1;

    snprintf(updateAuths, sizeof(updateAuths),
             "update %s.\"%s\".%s set object_owner = %d "
             "where object_owner = %d;",
             getSystemCatalog(), backupScheName.data(), SEABASE_OBJECTS, tempUserID, user_id);
    cliRC = cliInterface->executeImmediate(updateAuths);
    if (cliRC < 0) return -1;

    snprintf(updateAuths, sizeof(updateAuths),
             "update %s.\"%s\".%s set schema_owner = %d "
             "where schema_owner = %d;",
             getSystemCatalog(), backupScheName.data(), SEABASE_OBJECTS, tempUserID, user_id);
    cliRC = cliInterface->executeImmediate(updateAuths);
    if (cliRC < 0) return -1;

    iter++;
  }

  // then we set GRANTEE_ID same to the auth id in current metadata
  iter = unMatchedUserIDs.begin();
  while (iter != unMatchedUserIDs.end()) {
    int user_id = iter->first;
    NAString user_name = iter->second;
    int tempUserID = user_id + maxUserID;

    snprintf(updateAuths, sizeof(updateAuths),
             "select auth_id from %s.\"%s\".%s "
             "where auth_type = 'U' and auth_db_name = '%s';",
             getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_AUTHS, user_name.data());

    cliRC = cliInterface->fetchRowsPrologue(updateAuths, TRUE /*no exec*/);
    if (cliRC < 0) return -1;
    cliRC = cliInterface->clearExecFetchClose(NULL, 0);
    if (cliRC < 0) return -1;

    char *ptr = NULL;
    int len = 0;
    cliInterface->getPtrAndLen(1, ptr, len);
    int newUserID = *(int *)ptr;

    snprintf(updateAuths, sizeof(updateAuths),
             "update %s.\"%s\".%s set GRANTEE_ID = %d "
             "where GRANTEE_NAME = '%s';",
             getSystemCatalog(), backupScheName.data(), PRIVMGR_OBJECT_PRIVILEGES, newUserID, user_name.data());
    cliRC = cliInterface->executeImmediate(updateAuths);
    if (cliRC < 0) return -1;

    snprintf(updateAuths, sizeof(updateAuths),
             "update %s.\"%s\".%s set GRANTOR_ID = %d "
             "where GRANTOR_NAME = '%s';",
             getSystemCatalog(), backupScheName.data(), PRIVMGR_OBJECT_PRIVILEGES, newUserID, user_name.data());
    cliRC = cliInterface->executeImmediate(updateAuths);
    if (cliRC < 0) return -1;

    snprintf(updateAuths, sizeof(updateAuths),
             "update %s.\"%s\".%s set GRANTEE_ID = %d "
             "where GRANTEE_NAME = '%s';",
             getSystemCatalog(), backupScheName.data(), PRIVMGR_COLUMN_PRIVILEGES, newUserID, user_name.data());
    cliRC = cliInterface->executeImmediate(updateAuths);
    if (cliRC < 0) return -1;

    snprintf(updateAuths, sizeof(updateAuths),
             "update %s.\"%s\".%s set GRANTOR_ID = %d "
             "where GRANTOR_NAME = '%s';",
             getSystemCatalog(), backupScheName.data(), PRIVMGR_COLUMN_PRIVILEGES, newUserID, user_name.data());
    cliRC = cliInterface->executeImmediate(updateAuths);
    if (cliRC < 0) return -1;

    snprintf(updateAuths, sizeof(updateAuths),
             "update %s.\"%s\".%s set GRANTEE_ID = %d "
             "where GRANTEE_NAME = '%s';",
             getSystemCatalog(), backupScheName.data(), PRIVMGR_SCHEMA_PRIVILEGES, newUserID, user_name.data());
    cliRC = cliInterface->executeImmediate(updateAuths);
    if (cliRC < 0) return -1;

    snprintf(updateAuths, sizeof(updateAuths),
             "update %s.\"%s\".%s set GRANTOR_ID = %d "
             "where GRANTOR_NAME = '%s';",
             getSystemCatalog(), backupScheName.data(), PRIVMGR_SCHEMA_PRIVILEGES, newUserID, user_name.data());
    cliRC = cliInterface->executeImmediate(updateAuths);
    if (cliRC < 0) return -1;

    snprintf(updateAuths, sizeof(updateAuths),
             "update %s.\"%s\".%s set object_owner = %d "
             "where object_owner = %d;",
             getSystemCatalog(), backupScheName.data(), SEABASE_OBJECTS, newUserID, tempUserID);
    cliRC = cliInterface->executeImmediate(updateAuths);
    if (cliRC < 0) return -1;

    snprintf(updateAuths, sizeof(updateAuths),
             "update %s.\"%s\".%s set schema_owner = %d "
             "where schema_owner = %d;",
             getSystemCatalog(), backupScheName.data(), SEABASE_OBJECTS, newUserID, tempUserID);
    cliRC = cliInterface->executeImmediate(updateAuths);
    if (cliRC < 0) return -1;

    iter++;
  }

  return cliRC;
}
