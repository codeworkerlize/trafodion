

/* -*-C++-*-
 *****************************************************************************
 *
 * File:         CmpSeabaseDDLupgrade.cpp
 * Description:  Implements upgrade of metadata.
 *
 *
 * Created:     6/30/2013
 * Language:     C++
 *
 *
 *****************************************************************************
 */

#include "sqlcomp/CmpSeabaseDDLincludes.h"
#include "sqlcomp/CmpDDLCatErrorCodes.h"
#include "sqlcomp/CmpSeabaseDDLupgrade.h"
#include "sqlcomp/CmpSeabaseDDLrepos.h"
#include "sqlcomp/CmpSeabaseDDLroutine.h"
#include "sqlcomp/PrivMgrMD.h"
#include "sqlcomp/PrivMgrMDDefs.h"
#include "seabed/ms.h"

NABoolean CmpSeabaseMDupgrade::isOldMDtable(const NAString &objName) {
  if (objName.contains(OLD_MD_EXTENSION))
    return TRUE;
  else
    return FALSE;
}

NABoolean CmpSeabaseMDupgrade::isMDUpgradeNeeded() {
  for (int i = 0; i < sizeof(allMDupgradeInfo) / sizeof(MDUpgradeInfo); i++) {
    const MDUpgradeInfo &mdti = allMDupgradeInfo[i];
    if (mdti.upgradeNeeded) return TRUE;
  }

  return FALSE;
}

NABoolean CmpSeabaseMDupgrade::isViewsUpgradeNeeded() {
  for (int i = 0; i < sizeof(allMDviewsInfo) / sizeof(MDViewInfo); i++) {
    const MDViewInfo &mdti = allMDviewsInfo[i];
    if (mdti.upgradeNeeded) return TRUE;
  }

  return FALSE;
}

NABoolean CmpSeabaseMDupgrade::isReposUpgradeNeeded() {
  for (int i = 0; i < sizeof(allReposUpgradeInfo) / sizeof(MDUpgradeInfo); i++) {
    const MDUpgradeInfo &mdti = allReposUpgradeInfo[i];
    if (mdti.upgradeNeeded) return TRUE;
  }
  return FALSE;
}

NABoolean CmpSeabaseMDupgrade::isLibrariesUpgradeNeeded() {
  // ******
  // Temporary check, will be removed before official release
  // If upgrading from 2.2.1 to 2.6.0, skip libraries change
  CmpSeabaseDDL cmpDDL(STMTHEAP);
  ExpHbaseInterface *ehi = cmpDDL.allocEHI(COM_STORAGE_HBASE);

  long mdCurrMajorVersion = 2;
  long mdCurrMinorVersion = 1;
  long mdCurrUpdateVersion = 0;

  // ignore errors
  cmpDDL.validateVersions(&ActiveSchemaDB()->getDefaults(), ehi, &mdCurrMajorVersion, &mdCurrMinorVersion,
                          &mdCurrUpdateVersion);

  cmpDDL.deallocEHI(ehi);
  if (mdCurrMajorVersion == 2 && mdCurrMinorVersion == 1 && mdCurrUpdateVersion == 1 &&
      METADATA_OLD_MINOR_VERSION == 1 && METADATA_MINOR_VERSION == 6)
    return FALSE;
  // End temporary check
  // ******

  for (int i = 0; i < sizeof(allLibrariesUpgradeInfo) / sizeof(MDUpgradeInfo); i++) {
    const MDUpgradeInfo &mdti = allLibrariesUpgradeInfo[i];
    if (mdti.upgradeNeeded) return TRUE;
  }
  return FALSE;
}

// ----------------------------------------------------------------------------
// isPrivsUpgradeNeeded
//
// Checks to see PrivMgr metadata tables need to be upgraded
// returns:
//
// allPrivUpgradeInfo is defined in PrivMgrMDDefs.h
//
//   TRUE - upgrade maybe needed
//   FALSE - upgrade not needed
// ----------------------------------------------------------------------------
NABoolean CmpSeabaseMDupgrade::isPrivsUpgradeNeeded() {
  for (int i = 0; i < sizeof(allPrivMgrUpgradeInfo) / sizeof(MDUpgradeInfo); i++) {
    const MDUpgradeInfo &mdti = allPrivMgrUpgradeInfo[i];
    if (mdti.upgradeNeeded) return TRUE;
  }

  return FALSE;
}

// -----------------------------------------------------------------------------
// setRestoreStep
//
// Based on where the upgrade operation failed, determine the step and subStep
// for the undo operation, for details on subSteps, see:
//   UPGRADE_REPOS:     CmpSeabaseDDLrepos
//   UPGRADE_LIBRARIES: CmpSeabaseDDLroutine
//   UPGRADE_PRIVMGR:   PrivMgrMD
//   Other operations:  this file
// -----------------------------------------------------------------------------
void CmpSeabaseMDupgrade::setRestoreStep(CmpDDLwithStatusInfo *mdui) {
  int currentSubstep = mdui->subStep();
  int currentStep = mdui->step();

  if (currentStep == UPGRADE_REPOS) {
    mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_REPOS);

    // substep to set for failed REPOS upgrade:
    // (the lower the substep, the more undo is required)
    //  case 0: drop new tbls, rename old tbls to orig
    //  case 1: rename old tbls to orig
    //  case 2: no restore work is needed
    if (currentSubstep < 4)  // no upgrade step has been committed
      mdui->setSubstep(2);
    else if (currentSubstep < 6)  // orig tables have been renamed
      mdui->setSubstep(1);
    else  // new tables have been created
      mdui->setSubstep(0);
  }

  else if (currentStep == UPGRADE_LIBRARIES) {
    mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_LIBRARIES);

    // substep to set for failed LIBRARIES upgrade:
    // (the lower the substep, the more undo is required)
    //  case 0: drop new tbls, rename old tbls to orig
    //  case 1: rename old tbls to orig
    //  case 2: no restore work is needed
    if (currentSubstep < 4)  // no upgrade step has been committed
      mdui->setSubstep(2);
    else if (currentSubstep < 6)  // orig tables have been renamed
      mdui->setSubstep(1);
    else  // new tables have been created
      mdui->setSubstep(0);
  }

  else if (currentStep == UPGRADE_PRIVMGR) {
    mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_PRIVMGR);

    // substep to set for failed PRIVMGR upgrade:
    // (the lower the substep, the more undo is required)
    //  case 0: revoke privs new tbls, drop new tbls, rename old to orig, grant privs orig tbls
    //  case 1: drop new tbls, rename old to orig, grant privs orig tbls
    //  case 2: rename old to orig, grant privs orig tbls
    //  case 3: grant privs orig tbls (not used)
    //  case 4: no restore work is needed
    if (currentSubstep < 6)  // no upgrade step has been committed
      mdui->setSubstep(4);
    else if (currentSubstep < 10)  // privs revoked and 0 to all orig tables have been renamed
      mdui->setSubstep(2);
    else if (currentSubstep < 12)  // new tables have been created
      mdui->setSubstep(1);
    else  // grants on new tables have been committed
      mdui->setSubstep(0);
  }

  else {
    mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_MD);
    mdui->setSubstep(0);
  }
}

short CmpSeabaseMDupgrade::dropMDtables(ExpHbaseInterface *ehi, NABoolean oldTbls, NABoolean useOldNameForNewTables) {
  int retcode = 0;
  int errcode = 0;

  for (int i = 0; i < sizeof(allMDupgradeInfo) / sizeof(MDUpgradeInfo); i++) {
    const MDUpgradeInfo &mdti = allMDupgradeInfo[i];

    if ((NOT oldTbls) && (!mdti.newName)) continue;

    if (mdti.mdOnly) continue;

    HbaseStr hbaseTable;
    NAString extNameForHbase;
    extNameForHbase = TRAF_RESERVED_NAMESPACE1;
    extNameForHbase += ':';
    extNameForHbase += TRAFODION_SYSCAT_LIT;
    extNameForHbase += ".";
    extNameForHbase += SEABASE_MD_SCHEMA;
    extNameForHbase += ".";

    if (oldTbls) {
      if (useOldNameForNewTables) {
        if (!mdti.newName) continue;

        extNameForHbase += mdti.newName;
        extNameForHbase += OLD_MD_EXTENSION;
      } else {
        if (!mdti.oldName) continue;

        extNameForHbase += mdti.oldName;
      }
    } else
      extNameForHbase += mdti.newName;

    hbaseTable.val = (char *)extNameForHbase.data();
    hbaseTable.len = extNameForHbase.length();

    retcode = dropHbaseTable(ehi, &hbaseTable, FALSE, FALSE);
    if (retcode < 0) {
      errcode = -1;
    }

  }  // for

  return errcode;
}

short CmpSeabaseMDupgrade::restoreOldMDtables(ExpHbaseInterface *ehi) {
  int retcode = 0;
  int errcode = 0;

  // drop all the new MD tables. Ignore errors.
  dropMDtables(ehi, FALSE);

  for (int i = 0; i < sizeof(allMDupgradeInfo) / sizeof(MDUpgradeInfo); i++) {
    const MDUpgradeInfo &mdti = allMDupgradeInfo[i];

    if ((!mdti.newName) || (!mdti.oldName) || (mdti.addedTable) || (mdti.mdOnly)) continue;

    HbaseStr oldHbaseTable;
    NAString extNameForOldHbase;
    extNameForOldHbase = TRAF_RESERVED_NAMESPACE1;
    extNameForOldHbase += ':';
    extNameForOldHbase += TRAFODION_SYSCAT_LIT;
    extNameForOldHbase += ".";
    extNameForOldHbase += SEABASE_MD_SCHEMA;
    extNameForOldHbase += ".";
    extNameForOldHbase += mdti.oldName;
    oldHbaseTable.val = (char *)extNameForOldHbase.data();
    oldHbaseTable.len = extNameForOldHbase.length();

    HbaseStr currHbaseTable;
    NAString extNameForCurrHbase;
    extNameForCurrHbase = TRAF_RESERVED_NAMESPACE1;
    extNameForCurrHbase += ':';
    extNameForCurrHbase += TRAFODION_SYSCAT_LIT;
    extNameForCurrHbase += ".";
    extNameForCurrHbase += SEABASE_MD_SCHEMA;
    extNameForCurrHbase += ".";
    extNameForCurrHbase += mdti.newName;
    currHbaseTable.val = (char *)extNameForCurrHbase.data();
    currHbaseTable.len = extNameForCurrHbase.length();

    retcode = copyHbaseTable(ehi, &oldHbaseTable /*src*/, &currHbaseTable /*tgt*/);
    if (retcode < 0) {
      dropMDtables(ehi, FALSE);
      return -1;
    }
  }  // for

  return 0;
}

NABoolean CmpSeabaseDDL::getOldMDInfo(const MDTableInfo &mdti, const char *&oldName, const QString *&oldDDL,
                                      int &sizeOfoldDDL) {
  if (!mdti.newName) return FALSE;

  int numEntries = sizeof(allMDupgradeInfo) / sizeof(MDUpgradeInfo);

  for (int i = 0; i < numEntries; i++) {
    const MDUpgradeInfo &mdui = allMDupgradeInfo[i];
    if (!mdui.newName) continue;

    if (strcmp(mdti.newName, mdui.newName) == 0) {
      oldName = mdui.oldName;
      oldDDL = mdui.oldDDL;
      sizeOfoldDDL = mdui.sizeOfoldDDL;
      return TRUE;
    }
  }

  return FALSE;
}

NABoolean CmpSeabaseDDL::getOldMDPrivInfo(const PrivMgrTableStruct &mdti, const char *&oldName, const QString *&oldDDL,
                                          int &sizeOfoldDDL) {
  if (!mdti.tableName) return FALSE;

  int numEntries = sizeof(allPrivMgrUpgradeInfo) / sizeof(MDUpgradeInfo);

  for (int i = 0; i < numEntries; i++) {
    const MDUpgradeInfo &mdui = allPrivMgrUpgradeInfo[i];
    if (!mdui.newName) continue;

    if (strcmp(mdti.tableName, mdui.newName) == 0) {
      oldName = mdui.oldName;
      oldDDL = mdui.oldDDL;
      sizeOfoldDDL = mdui.sizeOfoldDDL;
      return TRUE;
    }
  }

  return FALSE;
}

// Return value:
//   0: not all old metadata tables exist, metadata is not initialized
//   1: all old metadata tables exists, metadata is initialized
//  -ve: error code
short CmpSeabaseDDL::isOldMetadataInitialized(ExpHbaseInterface *ehi) {
  short retcode;

  int numTotal = 0;
  int numExists = 0;
  NABoolean isRes = FALSE;
  isRes = CmpCommon::context()->useReservedNamespace();
  retcode = 0;
  for (int i = 0; (((retcode == 0) || (retcode == -1)) && (i < sizeof(allMDtablesInfo) / sizeof(MDTableInfo))); i++) {
    const MDUpgradeInfo &mdi = allMDupgradeInfo[i];

    if (!mdi.oldName) continue;

    if (mdi.addedTable) continue;

    NAString oldName(mdi.oldName);
    size_t pos = oldName.index(OLD_MD_EXTENSION);
    oldName = oldName.remove(pos, sizeof(OLD_MD_EXTENSION));

    numTotal++;
    HbaseStr hbaseTables;
    NAString hbaseTablesStr;
    if (isRes) {
      hbaseTablesStr = NAString(TRAF_RESERVED_NAMESPACE1);
      hbaseTablesStr += ":";
    }

    hbaseTablesStr += getSystemCatalog();
    hbaseTablesStr += ".";
    hbaseTablesStr += SEABASE_MD_SCHEMA;
    hbaseTablesStr += ".";
    hbaseTablesStr += oldName;
    hbaseTables.val = (char *)hbaseTablesStr.data();
    hbaseTables.len = hbaseTablesStr.length();

    retcode = ehi->exists(hbaseTables);
    if (retcode == -1)  // exists
      numExists++;
  }

  if ((retcode != 0) && (retcode != -1)) return retcode;  // error accessing metadata

  if (numExists < numTotal) return 0;  // corrupted or uninitialized old metadata

  if (numExists == numTotal) return 1;  // metadata is initialized

  return -1;
}

short CmpSeabaseMDupgrade::executeSeabaseMDupgrade(CmpDDLwithStatusInfo *mdui, NABoolean ddlXns, NAString &currCatName,
                                                   NAString &currSchName) {
  int cliRC = 0;
  int retcode = 0;

  char msgBuf[1000];
  char buf[10000];

  // interfaces for upgrading subsystems; OK to create on stack for
  // now as they are stateless
  CmpSeabaseUpgradeRepository upgradeRepository;
  CmpSeabaseUpgradeLibraries upgradeLibraries;
  CmpSeabaseUpgradePrivMgr upgradePrivMgr;

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());
  ExpHbaseInterface *ehi = NULL;
  NABoolean isMonarch = FALSE;

  // mdui stores the pin of this process.
  // If pin is not yet initialized (set to 0), initialize it
  int myPin = GetCliGlobals()->myPin();
  if (mdui->myPin() == 0) mdui->setMyPin(myPin);

  // If the pin changes, then a previous call to upgrade failed and killed the
  // upgrade process. At this time, we attempt to undo the failed upgrade and
  // set state back to original.
  if (myPin != mdui->myPin()) {
    mdui->setMyPin(myPin);

    // Determine the step and substep to undo based on info in mdui
    setRestoreStep(mdui);

    // resend controls
    if (sendAllControlsAndFlags()) {
      mdui->setStep(UPGRADE_FAILED);
      mdui->setSubstep(0);
    }
  }

  while (1) {
    // The line below is useful when debugging upgrade; it shows the step progression
    // through the upgrade state machine.
    // cout << "mdui->step() is " << mdui->step()
    //      << ", mdui->subStep() is " << mdui->subStep() << endl;

    switch (mdui->step()) {
      case UPGRADE_START: {
        if (xnInProgress(&cliInterface)) {
          *CmpCommon::diags() << DgSqlCode(-20123);
          return -1;
        }

        if (mdui->getMDVersion()) {
          mdui->setStep(GET_MD_VERSION);
          mdui->setSubstep(0);
          break;
        }

        if (mdui->getSWVersion()) {
          mdui->setStep(GET_SW_VERSION);
          mdui->setSubstep(0);
          break;
        }

        mdui->setMsg("Metadata Upgrade: started");
        mdui->setStep(VERSION_CHECK);
        mdui->setSubstep(0);
        mdui->setEndStep(TRUE);

        if (!ComUser::isRootUserID()) {
          mdui->setMsg("Metadata Upgrade: not authorized");
          mdui->setStep(UPGRADE_FAILED);
          mdui->setSubstep(0);
        }

        if (sendAllControlsAndFlags()) {
          mdui->setStep(UPGRADE_FAILED);
          mdui->setSubstep(0);
        }

        return 0;
      } break;

      case GET_MD_VERSION: {
        switch (mdui->subStep()) {
          case 0: {
            ehi = allocEHI(COM_STORAGE_HBASE);

            long mdCurrMajorVersion;
            long mdCurrMinorVersion;
            long mdCurrUpdateVersion;
            retcode = validateVersions(&ActiveSchemaDB()->getDefaults(), ehi, &mdCurrMajorVersion, &mdCurrMinorVersion,
                                       &mdCurrUpdateVersion);

            deallocEHI(ehi);

            if (retcode == 0) {
              // no version mismatch detected.
              // Metadata is uptodate.
              mdui->setSubstep(1);
            } else if (retcode == -1395)  // mismatch in MAJOR version. Need to upgrade
            {
              if ((mdCurrMajorVersion == METADATA_OLD_MAJOR_VERSION) &&
                  (mdCurrMinorVersion == METADATA_OLD_MINOR_VERSION)
                  // *****
                  // Temporary check, uncomment line before official release
                  // If upgrading from 2.2.1 to 2.6.0, skip libraries change
                  //&& (mdCurrUpdateVersion == METADATA_OLD_UPDATE_VERSION)
                  // *****
              ) {
                mdui->setSubstep(2);
              } else {
                // metadata cannot be upgraded with the current release.
                mdui->setSubstep(3);
              }
            } else if (retcode == -1394)  // mismatch in MAJOR version. Need to upgrade
            {
              // metadata cannot be upgraded with the current release.
              mdui->setSubstep(3);
            } else {
              *CmpCommon::diags() << DgSqlCode(retcode);
              mdui->setStep(DONE_RETURN);

              mdui->setSubstep(0);
              mdui->setEndStep(TRUE);
              break;
            }

            str_sprintf(msgBuf, "  Current Version %ld.%ld.%ld. Expected Version %d.%d.%d.", mdCurrMajorVersion,
                        mdCurrMinorVersion, mdCurrUpdateVersion, METADATA_MAJOR_VERSION, METADATA_MINOR_VERSION,
                        METADATA_UPDATE_VERSION);
            mdui->setMsg(msgBuf);
            mdui->setEndStep(FALSE);

            return 0;
          }  // case 0
          break;

          case 1: {
            str_sprintf(msgBuf, "  Metadata is current.");
            mdui->setMsg(msgBuf);
            mdui->setEndStep(FALSE);

            mdui->setStep(DONE_RETURN);
            mdui->setSubstep(0);

            return 0;
          } break;

          case 2: {
            str_sprintf(msgBuf, "  Metadata needs to be upgraded or reinitialized.");
            mdui->setMsg(msgBuf);
            mdui->setEndStep(FALSE);

            mdui->setStep(DONE_RETURN);
            mdui->setSubstep(0);

            return 0;
          } break;

          case 3: {
            str_sprintf(msgBuf, "  Metadata cannot be upgraded with this version of software.");
            mdui->setMsg(msgBuf);
            mdui->setEndStep(FALSE);

            mdui->setSubstep(4);

            return 0;
          } break;

          case 4: {
            str_sprintf(msgBuf, "  Install previous version of software or reinitialize metadata.");
            mdui->setMsg(msgBuf);
            mdui->setEndStep(FALSE);

            mdui->setStep(DONE_RETURN);
            mdui->setSubstep(0);

            return 0;
          } break;

        }  // switch
      }    // GET_MD_VERSION
      break;

      case GET_SW_VERSION: {
        switch (mdui->subStep()) {
          case 0: {
            ehi = allocEHI(COM_STORAGE_HBASE);

            long mdCurrMajorVersion;
            long mdCurrMinorVersion;
            long mdCurrUpdateVersion;
            long sysSWMajorVersion;
            long sysSWMinorVersion;
            long sysSWUpdVersion;
            long mdSWMajorVersion;
            long mdSWMinorVersion;
            long mdSWUpdateVersion;
            retcode = validateVersions(&ActiveSchemaDB()->getDefaults(), ehi, &mdCurrMajorVersion, &mdCurrMinorVersion,
                                       &mdCurrUpdateVersion, &sysSWMajorVersion, &sysSWMinorVersion, &sysSWUpdVersion,
                                       &mdSWMajorVersion, &mdSWMinorVersion, &mdSWUpdateVersion);

            deallocEHI(ehi);

            if ((retcode == 0) || (retcode == -TRAF_NOT_INITIALIZED) || (retcode == -1395)) {
              // no version mismatch detected between system and expected software.
              if ((mdSWMajorVersion == sysSWMajorVersion) && (mdSWMinorVersion == sysSWMinorVersion) &&
                  (mdSWUpdateVersion == sysSWUpdVersion))
                // software version stored in metadata is uptodate
                mdui->setSubstep(1);
              else
                // Software version stored in metadata is not current.
                // Initialize scripts need to be enhanced to store current software
                // version in metadata when trafodion is initialized or upgraded.
                // Until scripts are enhanced to do that, do not return an error.
                mdui->setSubstep(1);
            } else if (retcode == -1397)  // mismatch in software version
            {
              mdui->setSubstep(3);
            } else {
              if (retcode == -1395)
                str_sprintf(msgBuf,
                            "   Metadata needs to be upgraded or reinitialized (Current Version %ld.%ld.%ld, Expected "
                            "Version %ld.%ld.%ld).",
                            mdCurrMajorVersion, mdCurrMinorVersion, mdCurrUpdateVersion, (long)METADATA_MAJOR_VERSION,
                            (long)METADATA_MINOR_VERSION, (long)METADATA_UPDATE_VERSION);
              else
                str_sprintf(msgBuf,
                            " Error %d returned while accessing metadata. Fix that error before running this command.",
                            retcode);

              mdui->setSubstep(4);

              mdui->setMsg(msgBuf);
              mdui->setEndStep(FALSE);

              return 0;
            }

            long expSWMajorVersion = SOFTWARE_MAJOR_VERSION;
            long expSWMinorVersion = SOFTWARE_MINOR_VERSION;
            long expSWUpdVersion = SOFTWARE_UPDATE_VERSION;

            char expProdStr[100];

            if (msg_license_advanced_enabled())
              strcpy(expProdStr, "Advanced ");
            else if (msg_license_enterprise_enabled())
              strcpy(expProdStr, "Enterprise ");
            else
              strcpy(expProdStr, "Apache ");

            str_sprintf(msgBuf, "  Software Version: %ld.%ld.%ld. Expected Version: %ld.%ld.%ld.", sysSWMajorVersion,
                        sysSWMinorVersion, sysSWUpdVersion, expSWMajorVersion, expSWMinorVersion, expSWUpdVersion);

            mdui->setMsg(msgBuf);
            mdui->setEndStep(FALSE);

            mdui->setStep(DONE_RETURN);
            mdui->setSubstep(0);

            return 0;
          }  // case 0
          break;

          case 1: {
            str_sprintf(msgBuf, "  Software is current.");
            mdui->setMsg(msgBuf);
            mdui->setEndStep(FALSE);

            mdui->setStep(DONE_RETURN);
            mdui->setSubstep(0);

            return 0;
          } break;

          case 2: {
            str_sprintf(msgBuf,
                        "  Metadata needs to be updated with current software version. Run 'initialize trafodion, "
                        "update software version' to update it.");
            mdui->setMsg(msgBuf);
            mdui->setEndStep(FALSE);

            mdui->setStep(DONE_RETURN);
            mdui->setSubstep(0);

            return 0;
          } break;

          case 3: {
            str_sprintf(msgBuf,
                        "  Version of software being used is not compatible with version of software on the system.");
            mdui->setMsg(msgBuf);
            mdui->setEndStep(FALSE);

            mdui->setStep(DONE_RETURN);
            mdui->setSubstep(0);

            return 0;
          } break;

          case 4: {
            mdui->setEndStep(FALSE);

            mdui->setStep(DONE_RETURN);
            mdui->setSubstep(0);
          } break;

        }  // switch
      }    // GET_SW_VERSION
      break;

      case VERSION_CHECK: {
        switch (mdui->subStep()) {
          case 0: {
            mdui->setMsg("Version Check: started");
            mdui->subStep()++;
            mdui->setEndStep(FALSE);

            return 0;
          } break;

          case 1: {
            // check if upgrade has already been done.
            ehi = allocEHI(COM_STORAGE_HBASE);

            long mdCurrMajorVersion;
            long mdCurrMinorVersion;
            long mdCurrUpdateVersion;
            retcode = validateVersions(&ActiveSchemaDB()->getDefaults(), ehi, &mdCurrMajorVersion, &mdCurrMinorVersion,
                                       &mdCurrUpdateVersion);

            deallocEHI(ehi);

            if (retcode == 0) {
              // no version mismatch detected.
              // Metadata is uptodate.
              str_sprintf(msgBuf, "  Metadata is already at Version %ld.%ld.", mdCurrMajorVersion, mdCurrMinorVersion);
              mdui->setMsg(msgBuf);
              mdui->setEndStep(FALSE);

              mdui->setSubstep(2);

              return 0;
            } else if (retcode == -1395)  // mismatch in version. Need to upgrade
            {
              if (((mdCurrMajorVersion == METADATA_OLD_MAJOR_VERSION) &&
                   (mdCurrMinorVersion == METADATA_OLD_MINOR_VERSION)) ||
                  ((mdCurrMajorVersion == METADATA_MAJOR_VERSION) && (mdCurrMinorVersion == METADATA_MINOR_VERSION) &&
                   (mdCurrUpdateVersion == METADATA_OLD_UPDATE_VERSION) /*update version only*/)) {
                NAString upgItems;
                if (isUpgradeNeeded()) {
                  upgItems = "\n  Upgrade needed for";

                  if (isMDUpgradeNeeded()) {
                    upgItems += " Catalogs,";
                  }
                  if (isViewsUpgradeNeeded()) {
                    upgItems += " Views,";
                  }
                  if (upgradePrivMgr.needsUpgrade(this)) {
                    upgItems += " Privileges,";
                  }
                  if (upgradeRepository.needsUpgrade(this)) {
                    upgItems += " Repository,";
                  }
                  if (upgradeLibraries.needsUpgrade(this)) {
                    upgItems += " Libraries,";
                  }
                  if (NOT upgItems.isNull()) {
                    upgItems = upgItems.strip(NAString::trailing, ',');
                    upgItems += ".";
                  }
                }
                str_sprintf(msgBuf, "  Metadata needs to be upgraded from Version %ld.%ld.%ld to %d.%d.%d.%s",
                            mdCurrMajorVersion, mdCurrMinorVersion, mdCurrUpdateVersion, METADATA_MAJOR_VERSION,
                            METADATA_MINOR_VERSION, METADATA_UPDATE_VERSION,
                            (upgItems.isNull() ? " " : upgItems.data()));

                mdui->setMsg(msgBuf);

                mdui->setSubstep(3);

                return 0;
              } else {
                // metadata cannot be upgraded with the current release.
                str_sprintf(msgBuf, "  Metadata cannot to be upgraded from Version %ld.%ld to %d.%d with this software",
                            mdCurrMajorVersion, mdCurrMinorVersion, METADATA_MAJOR_VERSION, METADATA_MINOR_VERSION);

                mdui->setMsg(msgBuf);

                mdui->setSubstep(4);

                return 0;
              }
            } else {
              *CmpCommon::diags() << DgSqlCode(retcode);
              mdui->setStep(UPGRADE_DONE);

              mdui->setSubstep(0);
              mdui->setEndStep(TRUE);
            }

            return 0;
          } break;

          case 2: {
            mdui->setMsg("Version Check: done");
            mdui->setSubstep(0);
            mdui->setEndStep(TRUE);

            mdui->setStep(UPGRADE_DONE);

            return 0;
          } break;

          case 3: {
            mdui->setMsg("Version Check: done");
            mdui->setSubstep(0);
            mdui->setEndStep(TRUE);
            mdui->setStep(OLD_MD_DROP_PRE);

            return 0;
          } break;

          case 4: {
            mdui->setMsg("Version Check: done");
            mdui->setSubstep(0);
            mdui->setEndStep(TRUE);

            mdui->setStep(UPGRADE_FAILED);

            return 0;
          } break;

        }  // case
      }    // case VERSION_CHECK
      break;

      case OLD_MD_DROP_PRE: {
        switch (mdui->subStep()) {
          case 0: {
            if ((NOT isMDUpgradeNeeded()) && (NOT isViewsUpgradeNeeded())) {
              mdui->setStep(UPDATE_MD_VIEWS);
              mdui->setSubstep(0);
              break;
            }

            mdui->setMsg("Drop Old Metadata: started");
            mdui->subStep()++;
            mdui->setEndStep(FALSE);

            return 0;
          } break;

          case 1: {
            ehi = allocEHI(COM_STORAGE_HBASE);

            //
            // drop metadata tables using hbase drop command
            //
            if (dropMDtables(ehi, TRUE))  // drop old tables
            {
              deallocEHI(ehi);

              mdui->setStep(UPGRADE_FAILED);
              mdui->setSubstep(0);

              break;
            }

            deallocEHI(ehi);

            mdui->setMsg("Drop Old Metadata: done");
            mdui->setStep(CURR_MD_BACKUP);
            mdui->setSubstep(0);
            mdui->setEndStep(TRUE);

            return 0;
          } break;
        }  // case
      } break;

      case CURR_MD_BACKUP: {
        switch (mdui->subStep()) {
          case 0: {
            mdui->setMsg("Backup Current Metadata: started");
            mdui->subStep()++;
            mdui->setEndStep(FALSE);

            return 0;
          } break;

          case 1: {
            //
            // backup current metadata using hbase snapshot command
            //
            ehi = allocEHI(COM_STORAGE_HBASE);

            for (int i = 0; i < sizeof(allMDupgradeInfo) / sizeof(MDUpgradeInfo); i++) {
              const MDUpgradeInfo &mdti = allMDupgradeInfo[i];

              if ((mdti.addedTable) || (mdti.mdOnly)) continue;

              HbaseStr currHbaseTable;
              NAString extNameForCurrHbase;
              extNameForCurrHbase = TRAF_RESERVED_NAMESPACE1;
              extNameForCurrHbase += ':';
              extNameForCurrHbase += TRAFODION_SYSCAT_LIT;
              extNameForCurrHbase += ".";
              extNameForCurrHbase += SEABASE_MD_SCHEMA;
              extNameForCurrHbase += ".";
              extNameForCurrHbase += mdti.newName;
              currHbaseTable.val = (char *)extNameForCurrHbase.data();
              currHbaseTable.len = extNameForCurrHbase.length();

              HbaseStr oldHbaseTable;
              NAString extNameForOldHbase;
              extNameForOldHbase = TRAF_RESERVED_NAMESPACE1;
              extNameForOldHbase += ':';
              extNameForOldHbase += TRAFODION_SYSCAT_LIT;
              extNameForOldHbase += ".";
              extNameForOldHbase += SEABASE_MD_SCHEMA;
              extNameForOldHbase += ".";
              extNameForOldHbase += mdti.oldName;
              oldHbaseTable.val = (char *)extNameForOldHbase.data();
              oldHbaseTable.len = extNameForOldHbase.length();

              retcode = copyHbaseTable(ehi, &currHbaseTable, &oldHbaseTable);
              if (retcode < 0) {
                mdui->setStep(UPGRADE_FAILED_DROP_OLD_MD);
                mdui->setSubstep(0);
                break;
              }

            }  // for

            if (mdui->step() != CURR_MD_BACKUP) break;

            deallocEHI(ehi);

            mdui->setMsg("Backup Current Metadata: done");
            mdui->setStep(CURR_MD_DROP);
            mdui->setSubstep(0);
            mdui->setEndStep(TRUE);

            return 0;
          } break;
        }  // case

      } break;

      case CURR_MD_DROP: {
        switch (mdui->subStep()) {
          case 0: {
            if (NOT isMDUpgradeNeeded()) {
              mdui->setStep(UPDATE_MD_VIEWS);
              mdui->setSubstep(0);
              break;
            }

            mdui->setMsg("Drop Current Metadata: started");
            mdui->subStep()++;
            mdui->setEndStep(FALSE);

            return 0;
          } break;

          case 1: {
            ehi = allocEHI(COM_STORAGE_HBASE);

            //
            // drop metadata tables using hbase drop command
            //
            if (dropMDtables(ehi, FALSE))  // drop curr/new MD tables
            {
              mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_MD);
              mdui->setSubstep(0);
              break;
            }

            deallocEHI(ehi);

            mdui->setMsg("Drop Current Metadata: done");
            mdui->setStep(INITIALIZE_TRAF);
            mdui->setSubstep(0);
            mdui->setEndStep(TRUE);

            return 0;
          } break;
        }  // case
      } break;

      case INITIALIZE_TRAF: {
        switch (mdui->subStep()) {
          case 0: {
            if (xnInProgress(&cliInterface)) {
              *CmpCommon::diags() << DgSqlCode(-20123);

              mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_MD);
              mdui->setSubstep(0);
              break;
            }

            mdui->setMsg("Initialize New Metadata: started");
            mdui->subStep()++;
            mdui->setEndStep(FALSE);

            return 0;
          } break;

          case 1: {
            //
            // initialize trafodion
            CmpCommon::context()->setIsUninitializedSeabase(TRUE);
            CmpCommon::context()->uninitializedSeabaseErrNum() = -TRAF_NOT_INITIALIZED;  // MD doesn't exist

            // Use "initialize trafodion, minimal" so we only create the metadata
            // tables. The other tables (repository and privilege manager) already
            // exist; we will upgrade them later in this method.
            str_sprintf(buf, "initialize trafodion, minimal, no return status;");

            cliRC = cliInterface.executeImmediate(buf);

            if (cliRC < 0) {
              cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

              mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_MD);
              mdui->setSubstep(0);
              break;
            }

            if (xnInProgress(&cliInterface)) {
              *CmpCommon::diags() << DgSqlCode(-20123);

              mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_MD);
              mdui->setSubstep(0);
              break;
            }

            CmpCommon::context()->setIsUninitializedSeabase(FALSE);
            CmpCommon::context()->uninitializedSeabaseErrNum() = 0;

            // After initialize, VERSIONS table contain the new metadata version
            // values. Delete these values since the upgrade process is not yet done.
            // At the end of upgrade, VERSIONS table will be updated with the
            // new version values.
            cliRC = beginXn(&cliInterface);
            if (cliRC < 0) {
              cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

              mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_MD);
              mdui->setSubstep(0);
              break;
            }

            str_sprintf(buf, "delete from %s.\"%s\".%s;", TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_VERSIONS);
            cliRC = cliInterface.executeImmediate(buf);

            if (cliRC < 0) {
              cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

              mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_MD);
              mdui->setSubstep(0);
              break;
            }

            cliRC = commitXn(&cliInterface);
            if (cliRC < 0) {
              cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

              mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_MD);
              mdui->setSubstep(0);

              break;
            }

            mdui->setMsg("Initialize New Metadata: done");
            mdui->setStep(COPY_MD_TABLES_PROLOGUE);
            mdui->setSubstep(0);
            mdui->setEndStep(TRUE);

            return 0;
          } break;
        }  // case
      } break;

      case COPY_MD_TABLES_PROLOGUE: {
        mdui->setMsg("Copy Old Metadata: started");
        mdui->setStep(COPY_MD_TABLES);
        mdui->setSubstep(0);
        mdui->setEndStep(FALSE);

        if (xnInProgress(&cliInterface)) {
          *CmpCommon::diags() << DgSqlCode(-20123);

          mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_MD);
          mdui->setSubstep(0);

          break;
        }

        cliRC = autoCommit(&cliInterface, TRUE);  // set autocommit ON.
        if (cliRC < 0) {
          cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

          mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_MD);
          mdui->setSubstep(0);

          break;
        }

        return 0;
      } break;

      case COPY_MD_TABLES: {
        const MDUpgradeInfo &mdti = allMDupgradeInfo[mdui->subStep()];

        if ((mdui->subStep() < sizeof(allMDupgradeInfo) / sizeof(MDUpgradeInfo)) &&
            ((NOT mdti.addedTable) && (NOT mdti.droppedTable) && (NOT mdti.isIndex) && (NOT mdti.mdOnly))) {
          str_sprintf(buf, "upsert into %s.\"%s\".%s %s%s%s select %s from %s.\"%s\".%s SRC %s;", TRAFODION_SYSCAT_LIT,
                      SEABASE_MD_SCHEMA, mdti.newName, (mdti.insertedCols ? "(" : ""),
                      (mdti.insertedCols ? mdti.insertedCols : ""), (mdti.insertedCols ? ")" : ""),
                      (mdti.selectedCols ? mdti.selectedCols : "*"), TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA,
                      mdti.oldName, (mdti.wherePred ? mdti.wherePred : ""));

          // copy this table as is.
          if (NOT mdui->startStep()) {
            str_sprintf(msgBuf, "  Start: Copy %20s ==> %s", mdti.oldName, mdti.newName);

            mdui->setStartStep(TRUE);
            mdui->setEndStep(FALSE);

            // mdui->setMsg(buf);
            mdui->setMsg(msgBuf);

            break;
            // return 0;
          }

          mdui->setStartStep(FALSE);
          mdui->setEndStep(TRUE);

          // execute the insert...select
          cliRC = cliInterface.executeImmediate(buf);
          if (cliRC < 0) {
            cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

            mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_MD);
            mdui->setSubstep(0);

            break;
          }

          str_sprintf(msgBuf, "  End:   Copy %20s ==> %s", mdti.oldName, mdti.newName);

          mdui->setMsg(msgBuf);
          mdui->subStep()++;

          break;
          // return 0;
        }  // if

        mdui->subStep()++;
        if (mdui->subStep() >= sizeof(allMDupgradeInfo) / sizeof(MDUpgradeInfo)) {
          mdui->setStep(COPY_MD_TABLES_EPILOGUE);
          mdui->setSubstep(0);
        }

      } break;

      case COPY_MD_TABLES_EPILOGUE: {
        mdui->setMsg("Copy Old Metadata: done");
        mdui->setStep(VALIDATE_DATA_COPY);
        mdui->setSubstep(0);
        mdui->setEndStep(TRUE);

        cliRC = autoCommit(&cliInterface, FALSE);  // set to OFF
        if (cliRC < 0) {
          cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

          mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_MD);
          mdui->setSubstep(0);

          break;
        }

        return 0;
      } break;

      case VALIDATE_DATA_COPY: {
        switch (mdui->subStep()) {
          case 0: {
            mdui->setMsg("Validate Metadata Copy: started");
            mdui->subStep()++;
            mdui->setEndStep(FALSE);

            return 0;
          } break;

          case 1: {
            //
            // validate that data got copied. Do some counts, etc.
            //
            // TBD
            //
            mdui->setMsg("Validate Metadata Copy: done");
            mdui->setStep(CUSTOMIZE_NEW_MD);
            mdui->setSubstep(0);
            mdui->setEndStep(TRUE);

            return 0;
          } break;
        }  // case
      } break;

      case CUSTOMIZE_NEW_MD: {
        short rc = customizeNewMD(mdui, cliInterface);
        if (rc == 1) {
          mdui->setStep(OLD_TABLES_MD_DELETE);
          mdui->setSubstep(0);
          mdui->setEndStep(TRUE);
        } else if (rc == -1)
          break;

        return 0;
      } break;

      case OLD_TABLES_MD_DELETE: {
        switch (mdui->subStep()) {
          case 0: {
            if (xnInProgress(&cliInterface)) {
              *CmpCommon::diags() << DgSqlCode(-20123);

              mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_MD);
              mdui->setSubstep(0);

              break;
            }

            cliRC = beginXn(&cliInterface);
            if (cliRC < 0) {
              cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

              mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_MD);
              mdui->setSubstep(0);

              break;
            }

            mdui->setMsg("Delete Old Metadata Info: started");
            mdui->subStep()++;
            mdui->setEndStep(FALSE);

            return 0;
          } break;

          case 1: {
            ehi = allocEHI(COM_STORAGE_HBASE);

            // drop info about old tables from the new metadata.
            // do not drop the actual hbase tables.
            for (int i = 0; i < sizeof(allMDupgradeInfo) / sizeof(MDUpgradeInfo); i++) {
              const MDUpgradeInfo &mdti = allMDupgradeInfo[i];

              if (!mdti.oldName) continue;

              if ((mdti.addedTable) || (mdti.droppedTable))  // || (mdti.isIndex))
                continue;

              NAString catName(TRAFODION_SYSCAT_LIT);
              NAString schName;
              schName += "\"";
              schName += SEABASE_MD_SCHEMA;
              schName += "\"";

              char objType[5];
              if (mdti.isIndex)
                strcpy(objType, COM_INDEX_OBJECT_LIT);
              else
                strcpy(objType, COM_BASE_TABLE_OBJECT_LIT);

              long objUID =
                  getObjectUID(&cliInterface, TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, mdti.oldName, objType);
              if (objUID < 0) {
                CmpCommon::diags()->clear();

                strcpy(objType, "LB");
                objUID = getObjectUID(&cliInterface, TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, mdti.oldName, objType);
                if (objUID < 0) {
                  CmpCommon::diags()->clear();

                  strcpy(objType, "UR");
                  objUID = getObjectUID(&cliInterface, TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, mdti.oldName, objType);
                  if (objUID < 0) {
                    // could not find the object or an error.
                    // Ignore and continue.
                    CmpCommon::diags()->clear();
                    continue;
                  }
                }
              }
              ComObjectType objectType = PrivMgr::ObjectLitToEnum(objType);
              if (dropSeabaseObject(ehi, mdti.oldName, catName, schName, objectType, FALSE, TRUE, FALSE, isMonarch,
                                    NAString(""))) {
                deallocEHI(ehi);

                mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_MD);
                mdui->setSubstep(0);

                break;
              }
            }  // for

            if (mdui->step() != OLD_TABLES_MD_DELETE) break;

            // drop info about old metadata views from the new metadata.
            for (int i = 0; i < sizeof(allMDviewsInfo) / sizeof(MDViewInfo); i++) {
              const MDViewInfo &mdi = allMDviewsInfo[i];

              if (!mdi.viewName) continue;

              NAString oldViewName(mdi.viewName);
              oldViewName += OLD_MD_EXTENSION;

              NAString catName(TRAFODION_SYSCAT_LIT);
              NAString schName;
              schName += "\"";
              schName += SEABASE_MD_SCHEMA;
              schName += "\"";

              long objUID = getObjectUID(&cliInterface, TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, oldViewName.data(),
                                          COM_VIEW_OBJECT_LIT);
              if (objUID < 0) {
                // could not find the view or an error.
                // Ignore and continue.
                CmpCommon::diags()->clear();
                continue;
              }

              if (dropSeabaseObject(ehi, oldViewName, catName, schName, COM_VIEW_OBJECT, FALSE, TRUE, TRUE, isMonarch,
                                    NAString(""))) {
                deallocEHI(ehi);

                mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_MD);
                mdui->setSubstep(0);

                break;
              }
            }  // for

            if (mdui->step() != OLD_TABLES_MD_DELETE) break;

            deallocEHI(ehi);

            mdui->setMsg("Delete Old Metadata Info: done");
            mdui->setStep(UPDATE_MD_VIEWS);
            mdui->setSubstep(0);
            mdui->setEndStep(TRUE);

            if (xnInProgress(&cliInterface)) {
              cliRC = commitXn(&cliInterface);
              if (cliRC < 0) {
                cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

                mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_MD);
                mdui->setSubstep(0);

                break;
              }
            }

            return 0;
          } break;
        }  // case

      } break;

      case UPDATE_MD_VIEWS: {
        switch (mdui->subStep()) {
          case 0: {
            if (NOT isViewsUpgradeNeeded()) {
              mdui->setStep(UPGRADE_REPOS);
              mdui->setSubstep(0);
              break;
            }

            mdui->setMsg("Update Metadata Views: started");
            mdui->subStep()++;
            mdui->setEndStep(FALSE);

            return 0;
          } break;

          case 1: {
            if (xnInProgress(&cliInterface)) {
              *CmpCommon::diags() << DgSqlCode(-20123);

              mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_MD);
              mdui->setSubstep(0);

              break;
            }

            cliRC = beginXn(&cliInterface);
            if (cliRC < 0) {
              cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

              mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_MD);
              mdui->setSubstep(0);

              break;
            }

            cliRC = dropMetadataViews(&cliInterface);
            if (cliRC < 0) {
              mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_MD);
              mdui->setSubstep(0);

              break;
            }

            cliRC = createMetadataViews(&cliInterface);
            if (cliRC < 0) {
              mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_MD);
              mdui->setSubstep(0);

              break;
            }

            cliRC = commitXn(&cliInterface);
            if (cliRC < 0) {
              cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

              mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_MD);
              mdui->setSubstep(0);

              break;
            }

            mdui->setMsg("Update Metadata Views: done");
            mdui->setStep(UPGRADE_REPOS);
            mdui->setSubstep(0);
            mdui->setEndStep(TRUE);

            return 0;
          } break;
        }  // case

      } break;

      case UPGRADE_REPOS: {
        if (NOT isReposUpgradeNeeded()) {
          mdui->setStep(UPGRADE_LIBRARIES);
          mdui->setSubstep(0);
          break;
        }

        if (xnInProgress(&cliInterface)) {
          *CmpCommon::diags() << DgSqlCode(-20123);

          mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_MD);
          mdui->setSubstep(0);

          break;
        }

        cliRC = upgradeRepository.doUpgrade(&cliInterface, mdui, this, ddlXns);
        if (cliRC != 0) {
          mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_REPOS);
          mdui->setSubstep(-(cliRC + 1));

          break;
        }

        if (mdui->endStep()) {
          mdui->setStep(UPGRADE_LIBRARIES);
          mdui->setSubstep(0);
        }

        return 0;
      } break;

      case UPGRADE_LIBRARIES: {
        if (NOT isLibrariesUpgradeNeeded()) {
          mdui->setStep(UPGRADE_PRIVMGR);
          mdui->setSubstep(0);
          break;
        }

        if (xnInProgress(&cliInterface)) {
          *CmpCommon::diags() << DgSqlCode(-20123);

          mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_LIBRARIES);
          mdui->setSubstep(0);

          break;
        }

        cliRC = upgradeLibraries.doUpgrade(&cliInterface, mdui, this, ddlXns);
        if (cliRC != 0) {
          mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_LIBRARIES);
          mdui->setSubstep(-(cliRC + 1));

          break;
        }

        if (mdui->endStep()) {
          mdui->setStep(UPGRADE_PRIVMGR);
          mdui->setSubstep(0);
        }

        return 0;
      } break;

      case UPGRADE_PRIVMGR: {
        if (NOT upgradePrivMgr.needsUpgrade(this)) {
          mdui->setStep(UPDATE_VERSION);
          mdui->setSubstep(0);
          break;
        }

        if (xnInProgress(&cliInterface)) {
          *CmpCommon::diags() << DgSqlCode(-20123);

          mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_PRIVMGR);
          mdui->setSubstep(0);

          break;
        }

        cliRC = upgradePrivMgr.doUpgrade(&cliInterface, mdui, this, ddlXns);
        if (cliRC != 0) {
          mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_PRIVMGR);
          mdui->setSubstep(-(cliRC + 1));

          break;
        }

        if (mdui->endStep()) {
          mdui->setStep(UPDATE_VERSION);
          mdui->setSubstep(0);
        }

        return 0;
      } break;

      case UPDATE_VERSION: {
        switch (mdui->subStep()) {
          case 0: {
            mdui->setMsg("Update Metadata Version: started");
            mdui->subStep()++;
            mdui->setEndStep(FALSE);

            return 0;
          } break;

          case 1: {
            if (xnInProgress(&cliInterface)) {
              *CmpCommon::diags() << DgSqlCode(-20123);

              mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_PRIVMGR);
              mdui->setSubstep(0);

              break;
            }

            cliRC = beginXn(&cliInterface);
            if (cliRC < 0) {
              cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

              mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_PRIVMGR);
              mdui->setSubstep(0);

              break;
            }

            cliRC = updateSeabaseVersions(&cliInterface, TRAFODION_SYSCAT_LIT);
            if (cliRC < 0) {
              mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_PRIVMGR);
              mdui->setSubstep(0);

              break;
            }

            CmpCommon::context()->setIsUninitializedSeabase(FALSE);
            CmpCommon::context()->uninitializedSeabaseErrNum() = 0;

            cliRC = commitXn(&cliInterface);
            if (cliRC < 0) {
              cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

              mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_PRIVMGR);
              mdui->setSubstep(0);

              break;
            }

            mdui->setMsg("Update Metadata Version: done");

            if ((NOT isMDUpgradeNeeded()) && (NOT isViewsUpgradeNeeded()))
              mdui->setStep(METADATA_UPGRADED);
            else
              mdui->setStep(OLD_PRIVMGR_DROP);
            mdui->setSubstep(0);
            mdui->setEndStep(TRUE);

            return 0;
          } break;
        }  // case
      } break;

      case OLD_REPOS_DROP: {
        if (NOT upgradeRepository.needsUpgrade(this)) {
          mdui->setStep(OLD_MD_TABLES_HBASE_DELETE);
          mdui->setSubstep(0);
          mdui->setEndStep(TRUE);
          break;
        }

        if (upgradeRepository.doDrops(&cliInterface, mdui, this)) {
          // no status message in this case so no return
          cliInterface.clearGlobalDiags();
          mdui->setStep(OLD_MD_TABLES_HBASE_DELETE);
          mdui->setSubstep(0);
          mdui->setEndStep(TRUE);
        } else {
          if (mdui->endStep()) {
            mdui->setStep(OLD_MD_TABLES_HBASE_DELETE);
            mdui->setSubstep(0);
            mdui->setEndStep(TRUE);
          }
          return 0;
        }
      } break;

      case OLD_LIBRARIES_DROP: {
        if (NOT upgradeLibraries.needsUpgrade(this)) {
          mdui->setStep(OLD_REPOS_DROP);
          mdui->setSubstep(0);
          mdui->setEndStep(TRUE);
          break;
        }

        if (upgradeLibraries.doDrops(&cliInterface, mdui, this)) {
          // no status message in this case so no return
          cliInterface.clearGlobalDiags();
          mdui->setStep(OLD_REPOS_DROP);
          mdui->setSubstep(0);
          mdui->setEndStep(TRUE);
        } else {
          if (mdui->endStep()) {
            mdui->setStep(OLD_REPOS_DROP);
            mdui->setSubstep(0);
            mdui->setEndStep(TRUE);
          }
          return 0;
        }
      } break;

      case OLD_PRIVMGR_DROP: {
        if (NOT upgradePrivMgr.needsUpgrade(this)) {
          mdui->setStep(OLD_LIBRARIES_DROP);
          mdui->setSubstep(0);
          mdui->setEndStep(TRUE);
          break;
        }

        if (upgradePrivMgr.doDrops(&cliInterface, mdui, this)) {
          // no status message in this case so no return
          cliInterface.clearGlobalDiags();
          mdui->setStep(OLD_LIBRARIES_DROP);
          mdui->setSubstep(0);
          mdui->setEndStep(TRUE);
        } else {
          if (mdui->endStep()) {
            mdui->setStep(OLD_LIBRARIES_DROP);
            mdui->setSubstep(0);
            mdui->setEndStep(TRUE);
          }
          return 0;
        }
      } break;

      case OLD_MD_TABLES_HBASE_DELETE: {
        switch (mdui->subStep()) {
          case 0: {
            if (xnInProgress(&cliInterface)) {
              *CmpCommon::diags() << DgSqlCode(-20123);

              mdui->setStep(METADATA_UPGRADED);
              break;
            }

            cliRC = beginXn(&cliInterface);
            if (cliRC < 0) {
              cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

              mdui->setStep(METADATA_UPGRADED);
              break;
            }

            mdui->setMsg("Drop Old Metadata from Hbase: started");
            mdui->subStep()++;
            mdui->setEndStep(FALSE);

            return 0;
          } break;

          case 1: {
            ehi = allocEHI(COM_STORAGE_HBASE);

            // drop old tables from hbase.
            NAString nameSpace(TRAF_RESERVED_NAMESPACE1);
            for (int i = 0; i < sizeof(allMDupgradeInfo) / sizeof(MDUpgradeInfo); i++) {
              const MDUpgradeInfo &mdti = allMDupgradeInfo[i];
              if ((!mdti.oldName) || (mdti.mdOnly)) continue;

              NAString catName(TRAFODION_SYSCAT_LIT);
              NAString schName;
              schName += "\"";
              schName += SEABASE_MD_SCHEMA;
              schName += "\"";
              if (dropSeabaseObject(ehi, mdti.oldName, catName, schName,
                                    (mdti.isIndex ? COM_INDEX_OBJECT : COM_BASE_TABLE_OBJECT), FALSE, FALSE, TRUE,
                                    isMonarch, nameSpace)) {
                // ignore errors. Continue dropping old md tables.
              }
            }  // for

            deallocEHI(ehi);

            mdui->setMsg("Drop Old Metadata from Hbase: done");
            mdui->setStep(METADATA_UPGRADED);
            mdui->setSubstep(0);
            mdui->setEndStep(TRUE);

            if (xnInProgress(&cliInterface)) {
              cliRC = commitXn(&cliInterface);
              if (cliRC < 0) {
                cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
                mdui->setStep(METADATA_UPGRADED);
                break;
              }
            }

            return 0;
          } break;
        }  // switch

      } break;

      case METADATA_UPGRADED: {
        str_sprintf(msgBuf, "Metadata Upgrade to Version %d.%d.%d: done", METADATA_MAJOR_VERSION,
                    METADATA_MINOR_VERSION, METADATA_UPDATE_VERSION);

        mdui->setMsg(msgBuf);
        mdui->setDone(TRUE);

        return 0;
      } break;

      case UPGRADE_DONE: {
        mdui->setMsg("Metadata Upgrade: done");
        mdui->setDone(TRUE);

        return 0;
      } break;

      case DONE_RETURN: {
        mdui->setMsg("");
        mdui->setDone(TRUE);

        return 0;
      } break;

      case UPGRADE_FAILED_RESTORE_OLD_REPOS: {
        // Note: We can't combine this case with UPGRADE_FAILED etc.
        // below, because the subsystem code uses mdui->subStep()
        // to keep track of its progress.

        // change existing errors to warnings
        CmpCommon::diags()->negateAllErrors();

        if (upgradeRepository.needsUpgrade(this)) {
          if (xnInProgress(&cliInterface)) {
            cliRC = rollbackXn(&cliInterface);
            if (cliRC < 0) {
              // ignore errors
            }
          }

          if (upgradeRepository.doUndo(&cliInterface, mdui, this)) {
            // ignore errors; no status message so just continue on
            cliInterface.clearGlobalDiags();
            mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_MD);
            mdui->setSubstep(0);
            mdui->setEndStep(TRUE);
          } else {
            if (mdui->endStep()) {
              mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_MD);
              mdui->setSubstep(0);
              mdui->setEndStep(TRUE);
            }
            return 0;
          }
        } else {
          mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_MD);
          mdui->setSubstep(0);
          mdui->setEndStep(TRUE);
        }
      } break;

      case UPGRADE_FAILED_RESTORE_OLD_LIBRARIES: {
        // Note: We can't combine this case with UPGRADE_FAILED etc.
        // below, because the subsystem code uses mdui->subStep()
        // to keep track of its progress.

        // change existing errors to warnings
        CmpCommon::diags()->negateAllErrors();

        if (upgradeLibraries.needsUpgrade(this)) {
          if (xnInProgress(&cliInterface)) {
            cliRC = rollbackXn(&cliInterface);
            if (cliRC < 0) {
              // ignore errors
            }
          }

          if (upgradeLibraries.doUndo(&cliInterface, mdui, this)) {
            // ignore errors; no status message so just continue on
            cliInterface.clearGlobalDiags();
            mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_REPOS);
            mdui->setSubstep(0);
            mdui->setEndStep(TRUE);
          } else {
            if (mdui->endStep()) {
              mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_REPOS);
              mdui->setSubstep(0);
              mdui->setEndStep(TRUE);
            }
            return 0;
          }
        } else {
          mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_REPOS);
          mdui->setSubstep(0);
          mdui->setEndStep(TRUE);
        }
      } break;

      case UPGRADE_FAILED_RESTORE_OLD_PRIVMGR: {
        // Note: We can't combine this case with UPGRADE_FAILED etc.
        // below, because the subsystem code uses mdui->subStep()
        // to keep track of its progress.

        // change existing errors to warnings
        CmpCommon::diags()->negateAllErrors();

        if (upgradePrivMgr.needsUpgrade(this)) {
          if (xnInProgress(&cliInterface)) {
            cliRC = rollbackXn(&cliInterface);
            if (cliRC < 0) {
              // ignore errors
            }
          }

          if (upgradePrivMgr.doUndo(&cliInterface, mdui, this)) {
            // ignore errors; no status message so just continue on
            cliInterface.clearGlobalDiags();
            mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_LIBRARIES);
            mdui->setSubstep(0);
            mdui->setEndStep(TRUE);
          } else {
            if (mdui->endStep()) {
              mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_LIBRARIES);
              mdui->setSubstep(0);
              mdui->setEndStep(TRUE);
            }
            return 0;
          }
        } else {
          mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_LIBRARIES);
          mdui->setSubstep(0);
          mdui->setEndStep(TRUE);
        }
      } break;

      case UPGRADE_FAILED:
      case UPGRADE_FAILED_RESTORE_OLD_MD:
      case UPGRADE_FAILED_DROP_OLD_MD: {
        if (xnInProgress(&cliInterface)) {
          cliRC = rollbackXn(&cliInterface);
          if (cliRC < 0) {
            // ignore errors
          }
        }

        // change existing errors to warnings
        CmpCommon::diags()->negateAllErrors();

        switch (mdui->subStep()) {
          case 0: {
            if (mdui->step() == UPGRADE_FAILED_DROP_OLD_MD) {
              mdui->setSubstep(1);
              break;
            } else if (mdui->step() == UPGRADE_FAILED_RESTORE_OLD_MD) {
              mdui->setSubstep(4);
              break;
            } else {
              mdui->setSubstep(7);
              break;
            }
          } break;

          case 1: {
            mdui->setMsg("Drop Old Metadata: started");
            mdui->subStep()++;
            mdui->setEndStep(FALSE);

            return 0;
          } break;

          case 2: {
            if (ehi == NULL) ehi = allocEHI(COM_STORAGE_HBASE);

            dropMDtables(ehi, TRUE);  // drop old MD
            deallocEHI(ehi);

            mdui->subStep()++;
            mdui->setEndStep(FALSE);
          } break;

          case 3: {
            mdui->setMsg("Drop Old Metadata: done");
            mdui->setSubstep(7);
            mdui->setEndStep(TRUE);

            return 0;
          } break;

          case 4: {
            mdui->setMsg("Restore from Old Metadata: started");
            mdui->subStep()++;
            mdui->setEndStep(FALSE);

            return 0;
          } break;

          case 5: {
            if (ehi == NULL) ehi = allocEHI(COM_STORAGE_HBASE);

            restoreOldMDtables(ehi);  // restore old MD and make them current
            deallocEHI(ehi);

            mdui->subStep()++;
            mdui->setEndStep(FALSE);
          } break;

          case 6: {
            mdui->setMsg("Restore from Old Metadata: done");
            mdui->setSubstep(1);  // now drop old metadata tables
            mdui->setEndStep(TRUE);

            return 0;
          } break;

          case 7: {
            deallocEHI(ehi);

            mdui->setMsg("Metadata Upgrade: failed");
            mdui->setDone(TRUE);

            return 0;
          } break;
        }  // switch
      } break;

    }  // step
  }    // while

  return 0;
}

short CmpSeabaseMDupgrade::customizeNewMD(CmpDDLwithStatusInfo *mdui, ExeCliInterface &cliInterface) {
  if ((METADATA_OLD_MAJOR_VERSION == 2) && (METADATA_OLD_MINOR_VERSION == 3) && (METADATA_MAJOR_VERSION == 3) &&
      (METADATA_MINOR_VERSION == 0))
    return customizeNewMDv23tov30(mdui, cliInterface);

  // done, move to next step.
  return 1;
}

short CmpSeabaseMDupgrade::customizeNewMDv23tov30(CmpDDLwithStatusInfo *mdui, ExeCliInterface &cliInterface) {
  int cliRC = 0;
  char buf[10000];

  if (NOT((METADATA_OLD_MAJOR_VERSION == 2) && (METADATA_OLD_MINOR_VERSION == 3) && (METADATA_MAJOR_VERSION == 3) &&
          (METADATA_MINOR_VERSION == 0))) {
    // done, move to next step.
    return 1;
  }

  switch (mdui->subStep()) {
    case 0: {
      mdui->setMsg("Customize New Metadata: started");
      mdui->subStep()++;
      mdui->setEndStep(FALSE);

      // For other upgrades, it is not needed.
      // Customize this section as needed.
      if (NOT((METADATA_OLD_MAJOR_VERSION == 2) && (METADATA_OLD_MINOR_VERSION == 3) && (METADATA_MAJOR_VERSION == 3) &&
              (METADATA_MINOR_VERSION == 0))) {
        mdui->setSubstep(11);
        mdui->setEndStep(FALSE);

        return 0;
      }

      if (xnInProgress(&cliInterface)) {
        *CmpCommon::diags() << DgSqlCode(-20123);

        mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_MD);
        mdui->setSubstep(0);

        return -1;
      }

      return 0;
    }
      return -1;

    case 1: {
      mdui->setMsg("  Start: Update COLUMNS");
      mdui->subStep()++;

      cliRC = beginXn(&cliInterface);
      if (cliRC < 0) {
        cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

        mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_MD);
        mdui->setSubstep(0);

        return -1;
      }

      return 0;
    }
      return -1;

    case 2: {
      str_sprintf(buf,
                  "update %s.\"%s\".%s set sql_data_type = cast( case when fs_data_type = 130 then "
                  "'" COM_SMALLINT_SIGNED_SDT_LIT "' when fs_data_type = 131 then '" COM_SMALLINT_UNSIGNED_SDT_LIT
                  "' when fs_data_type = 132 then '" COM_INTEGER_SIGNED_SDT_LIT
                  "' when fs_data_type = 133 then '" COM_INTEGER_UNSIGNED_SDT_LIT
                  "' when fs_data_type = 134 then '" COM_LARGEINT_SIGNED_SDT_LIT
                  "' when fs_data_type = 135 then '" COM_SMALLINT_UNSIGNED_SDT_LIT
                  "' when fs_data_type = 140 then '" COM_REAL_SDT_LIT
                  "' when fs_data_type = 141 then '" COM_DOUBLE_SDT_LIT
                  "' when fs_data_type = 150 then '" COM_DECIMAL_UNSIGNED_SDT_LIT
                  "' when fs_data_type = 151 then '" COM_DECIMAL_SIGNED_SDT_LIT
                  "' when fs_data_type = 155 then '" COM_NUMERIC_UNSIGNED_SDT_LIT
                  "' when fs_data_type = 156 then '" COM_NUMERIC_SIGNED_SDT_LIT
                  "' when fs_data_type = 0     then '" COM_CHARACTER_SDT_LIT
                  "' when fs_data_type = 2     then '" COM_CHARACTER_SDT_LIT
                  "' when fs_data_type = 70    then '" COM_LONG_VARCHAR_SDT_LIT
                  "' when fs_data_type = 64    then '" COM_VARCHAR_SDT_LIT
                  "' when fs_data_type = 66    then '" COM_VARCHAR_SDT_LIT
                  "' when fs_data_type = 100   then '" COM_VARCHAR_SDT_LIT
                  "' when fs_data_type = 101   then '" COM_VARCHAR_SDT_LIT
                  "' when fs_data_type = 192 then '" COM_DATETIME_SDT_LIT
                  "' when fs_data_type >= 196 and fs_data_type <= 207 then '" COM_INTERVAL_SDT_LIT
                  "' else '' end as char(24))    ",
                  getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_COLUMNS);
      cliRC = cliInterface.executeImmediate(buf);

      if (cliRC < 0) {
        cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

        *CmpCommon::diags() << DgSqlCode(-1423) << DgString0(SEABASE_TEXT);

        mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_MD);
        mdui->setSubstep(0);

        return -1;
      }

      if (mdui->step() != CUSTOMIZE_NEW_MD) return -1;

      mdui->setMsg("  End: Update COLUMNS");
      mdui->subStep()++;

      if (xnInProgress(&cliInterface)) {
        cliRC = commitXn(&cliInterface);
        if (cliRC < 0) {
          cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

          mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_MD);
          mdui->setSubstep(0);

          return -1;
        }
      }

      return 0;
    }
      return -1;

    case 3: {
      mdui->setMsg("  Start: Update TABLES");
      mdui->subStep()++;

      cliRC = beginXn(&cliInterface);
      if (cliRC < 0) {
        cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

        mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_MD);
        mdui->setSubstep(0);

        return -1;
      }

      return 0;
    }
      return -1;

    case 4: {
      str_sprintf(
          buf, "select table_uid, hbase_create_options from %s.\"%s\".%s where char_length(hbase_create_options) > 0",
          getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_TABLES_OLD_MD);

      Queue *tablesQueue = NULL;
      cliRC = cliInterface.fetchAllRows(tablesQueue, buf, 0, FALSE, FALSE, TRUE);
      if (cliRC < 0) {
        cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

        mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_MD);
        mdui->setSubstep(0);

        return -1;
      }

      tablesQueue->position();
      for (int idx = 0; idx < tablesQueue->numEntries(); idx++) {
        OutputInfo *oi = (OutputInfo *)tablesQueue->getNext();

        long tableUID = *(long *)oi->get(0);
        NAString hbaseCreateOptions((char *)oi->get(1));

        int numSaltPartns = 0;

        // get num salt partns from hbaseCreateOptions.
        // It is stored as:  NUM_SALT_PARTNS=>NNNN
        size_t idx2 = hbaseCreateOptions.index("NUM_SALT_PARTNS=>");
        if ((int)idx2 >= 0) {
          char numSaltPartnsCharStr[5];
          const char *startNumSaltPartns = &hbaseCreateOptions.data()[idx2 + strlen("NUM_SALT_PARTNS=>")];
          memcpy(numSaltPartnsCharStr, startNumSaltPartns, 4);
          numSaltPartnsCharStr[4] = 0;

          numSaltPartns = str_atoi(numSaltPartnsCharStr, 4);

          hbaseCreateOptions.remove(idx2, strlen("NUM_SALT_PARTNS=>") + 4);
          hbaseCreateOptions = hbaseCreateOptions.strip();
        }

        str_sprintf(buf, "update %s.\"%s\".%s set num_salt_partns = %d where table_uid = %ld", getSystemCatalog(),
                    SEABASE_MD_SCHEMA, SEABASE_TABLES, numSaltPartns, tableUID);
        cliRC = cliInterface.executeImmediate(buf);

        if (cliRC < 0) {
          cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

          *CmpCommon::diags() << DgSqlCode(-1423) << DgString0(SEABASE_TABLES);

          mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_MD);
          mdui->setSubstep(0);

          return -1;
        }

        if (NOT hbaseCreateOptions.isNull()) {
          if (updateTextTable(&cliInterface, tableUID, COM_HBASE_OPTIONS_TEXT, 0, hbaseCreateOptions)) {
            *CmpCommon::diags() << DgSqlCode(-1423) << DgString0(SEABASE_TABLES);

            mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_MD);
            mdui->setSubstep(0);

            return -1;
          }
        }
      }  // for

      if (mdui->step() != CUSTOMIZE_NEW_MD) return -1;

      str_sprintf(
          buf,
          "merge into %s.\"%s\".%s using (select C.object_uid, sum(C.column_size  + case when C.nullable != 0 then 1 "
          "else 0 end) from %s.\"%s\".%s C, %s.\"%s\".%s K where C.object_uid = K.object_uid and C.column_number = "
          "K.column_number group by 1) T(a, b) on table_uid = T.a when matched then update set key_length = T.b",
          getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_TABLES, getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_COLUMNS,
          getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_KEYS);
      cliRC = cliInterface.executeImmediate(buf);

      if (cliRC < 0) {
        cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

        *CmpCommon::diags() << DgSqlCode(-1423) << DgString0(SEABASE_TABLES);

        mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_MD);
        mdui->setSubstep(0);

        return -1;
      }

      str_sprintf(buf,
                  "merge into %s.\"%s\".%s using (select object_uid, sum(column_size + case when nullable != 0 then 1 "
                  "else 0 end), sum(column_size + case when nullable != 0 then 1 else 0 end + %lu + %d), count(*) from "
                  "%s.\"%s\".%s group by 1) T(a,b,c,d) on table_uid = T.a when matched then update set "
                  "(row_data_length, row_total_length) = (T.b, T.c + key_length * T.d)",
                  getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_TABLES, sizeof(long), 5, getSystemCatalog(),
                  SEABASE_MD_SCHEMA, SEABASE_COLUMNS);
      cliRC = cliInterface.executeImmediate(buf);

      if (cliRC < 0) {
        cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

        *CmpCommon::diags() << DgSqlCode(-1423) << DgString0(SEABASE_TABLES);

        mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_MD);
        mdui->setSubstep(0);

        return -1;
      }

      if (mdui->step() != CUSTOMIZE_NEW_MD) return -1;

      mdui->setMsg("  End: Update TABLES");
      mdui->subStep()++;

      if (xnInProgress(&cliInterface)) {
        cliRC = commitXn(&cliInterface);
        if (cliRC < 0) {
          cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

          mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_MD);
          mdui->setSubstep(0);

          return -1;
        }
      }

      return 0;
    }
      return -1;

    case 5: {
      mdui->setMsg("  Start: Update TEXT");
      mdui->subStep()++;

      cliRC = beginXn(&cliInterface);
      if (cliRC < 0) {
        cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

        mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_MD);
        mdui->setSubstep(0);

        return -1;
      }

      return 0;
    }
      return -1;

    case 6: {
      str_sprintf(buf,
                  "update %s.\"%s\".%s set text_type = 1 where text not like 'CREATE VIEW %%' and text not like "
                  "'HBASE_OPTIONS=>%%' and NOT (text_type = 4 and text like 'HASH2PARTFUNC%%' ) ",
                  getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_TEXT);
      cliRC = cliInterface.executeImmediate(buf);

      if (cliRC < 0) {
        cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

        *CmpCommon::diags() << DgSqlCode(-1423) << DgString0(SEABASE_TEXT);

        mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_MD);
        mdui->setSubstep(0);

        return -1;
      }

      if (mdui->step() != CUSTOMIZE_NEW_MD) return -1;

      mdui->setMsg("  End: Update TEXT");
      mdui->subStep()++;

      if (xnInProgress(&cliInterface)) {
        cliRC = commitXn(&cliInterface);
        if (cliRC < 0) {
          cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

          mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_MD);
          mdui->setSubstep(0);

          return -1;
        }
      }

      return 0;
    }
      return -1;

    case 7: {
      mdui->setMsg("  Start: Update SEQ_GEN");
      mdui->subStep()++;

      cliRC = beginXn(&cliInterface);
      if (cliRC < 0) {
        cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

        mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_MD);
        mdui->setSubstep(0);

        return -1;
      }

      return 0;
    }
      return -1;

    case 8: {
      str_sprintf(buf,
                  "delete from %s.\"%s\".%s where object_type = 'PK' and object_uid = (select TC.constraint_uid from "
                  "%s.\"%s\".%s TC where TC.constraint_type = 'P' and TC.table_uid = (select O2.object_uid from "
                  "%s.\"%s\".%s O2 where O2.catalog_name = '%s' and O2.schema_name = '%s' and O2.object_name = '%s' "
                  "and O2.object_type = 'BT')) ",
                  TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_OBJECTS, TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA,
                  SEABASE_TABLE_CONSTRAINTS, TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_OBJECTS,
                  TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_SEQ_GEN_OLD_MD);

      cliRC = cliInterface.executeImmediate(buf);
      if (cliRC < 0) {
        cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

        mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_MD);
        mdui->setSubstep(0);

        return -1;
      }

      str_sprintf(buf,
                  "delete from %s.\"%s\".%s  where constraint_type = 'P' and table_uid = (select O.object_uid from "
                  "%s.\"%s\".%s O where O.catalog_name = '%s' and O.schema_name = '%s' and O.object_name = '%s' and "
                  "O.object_type = 'BT') ",
                  TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_TABLE_CONSTRAINTS, TRAFODION_SYSCAT_LIT,
                  SEABASE_MD_SCHEMA, SEABASE_OBJECTS, TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_SEQ_GEN_OLD_MD);

      cliRC = cliInterface.executeImmediate(buf);
      if (cliRC < 0) {
        cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

        mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_MD);
        mdui->setSubstep(0);

        return -1;
      }

      if (mdui->step() != CUSTOMIZE_NEW_MD) return -1;

      mdui->setMsg("  End: Update SEQ_GEN");
      mdui->subStep()++;

      if (xnInProgress(&cliInterface)) {
        cliRC = commitXn(&cliInterface);
        if (cliRC < 0) {
          cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());

          mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_MD);
          mdui->setSubstep(0);

          return -1;
        }
      }

      return 0;
    }
      return -1;

    case 9: {
      mdui->setMsg("  Start: Create Schema Objects");
      mdui->subStep()++;

      return 0;
    }
      return -1;

    case 10: {
      cliRC = createSchemaObjects(&cliInterface);
      if (cliRC < 0) {
        mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_MD);
        mdui->setSubstep(0);

        return -1;
      }

      if (mdui->step() != CUSTOMIZE_NEW_MD) return -1;

      mdui->setMsg("  End: Create Schema Objects");
      mdui->subStep()++;

      return 0;
    }
      return -1;

    case 11: {
      mdui->setMsg("Customize New Metadata: done");

      // done, move to next step.
      return 1;
    }
      return -1;

    default: {
      mdui->setStep(UPGRADE_FAILED_RESTORE_OLD_MD);
      mdui->setSubstep(0);
    }
      return -1;
  }

  return -1;
}

// ----------------------------------------------------------------------------
// Methods for class CmpSeabaseUpgradeLibraries
// ----------------------------------------------------------------------------
NABoolean CmpSeabaseUpgradeLibraries::needsUpgrade(CmpSeabaseMDupgrade *ddlState) {
  return ddlState->isLibrariesUpgradeNeeded();
}

short CmpSeabaseUpgradeLibraries::doUpgrade(ExeCliInterface *cliInterface, CmpDDLwithStatusInfo *mdui,
                                            CmpSeabaseMDupgrade *ddlState, NABoolean /* ddlXns */) {
  return ddlState->upgradeLibraries(cliInterface, mdui);
}

short CmpSeabaseUpgradeLibraries::doDrops(ExeCliInterface *cliInterface, CmpDDLwithStatusInfo *mdui,
                                          CmpSeabaseMDupgrade *ddlState) {
  return ddlState->upgradeLibrariesComplete(cliInterface, mdui);
}

short CmpSeabaseUpgradeLibraries::doUndo(ExeCliInterface *cliInterface, CmpDDLwithStatusInfo *mdui,
                                         CmpSeabaseMDupgrade *ddlState) {
  return ddlState->upgradeLibrariesUndo(cliInterface, mdui);
}

// ----------------------------------------------------------------------------
// Methods for class CmpSeabaseUpgradeRepository
// ----------------------------------------------------------------------------
NABoolean CmpSeabaseUpgradeRepository::needsUpgrade(CmpSeabaseMDupgrade *ddlState) {
  return ddlState->isReposUpgradeNeeded();
}

short CmpSeabaseUpgradeRepository::doUpgrade(ExeCliInterface *cliInterface, CmpDDLwithStatusInfo *mdui,
                                             CmpSeabaseMDupgrade *ddlState, NABoolean /* ddlXns */) {
  return ddlState->upgradeRepos(cliInterface, mdui);
}

short CmpSeabaseUpgradeRepository::doDrops(ExeCliInterface *cliInterface, CmpDDLwithStatusInfo *mdui,
                                           CmpSeabaseMDupgrade *ddlState) {
  return ddlState->upgradeReposComplete(cliInterface, mdui);
}

short CmpSeabaseUpgradeRepository::doUndo(ExeCliInterface *cliInterface, CmpDDLwithStatusInfo *mdui,
                                          CmpSeabaseMDupgrade *ddlState) {
  return ddlState->upgradeReposUndo(cliInterface, mdui);
}

// ----------------------------------------------------------------------------
// Methods for class CmpSeabaseUpgradePrivMgr
//    Upgrade methods are part of the PrivMgrMDAdmin class
// ----------------------------------------------------------------------------
NABoolean CmpSeabaseUpgradePrivMgr::needsUpgrade(CmpSeabaseMDupgrade *ddlState) {
  return ddlState->isPrivsUpgradeNeeded();
}

short CmpSeabaseUpgradePrivMgr::doUpgrade(ExeCliInterface *cliInterface, CmpDDLwithStatusInfo *mdui,
                                          CmpSeabaseMDupgrade *ddlState, NABoolean ddlXns) {
  PrivMgrMDAdmin privMgrMD;
  return privMgrMD.upgradePMTbl(cliInterface, mdui);
}

short CmpSeabaseUpgradePrivMgr::doDrops(ExeCliInterface *cliInterface, CmpDDLwithStatusInfo *mdui,
                                        CmpSeabaseMDupgrade *ddlState) {
  PrivMgrMDAdmin privMgrMD;
  return privMgrMD.upgradePMTblComplete(cliInterface, mdui);
}

short CmpSeabaseUpgradePrivMgr::doUndo(ExeCliInterface *cliInterface, CmpDDLwithStatusInfo *mdui,
                                       CmpSeabaseMDupgrade *ddlState) {
  PrivMgrMDAdmin privMgrMD;
  return privMgrMD.upgradePMTblUndo(cliInterface, mdui);
}
