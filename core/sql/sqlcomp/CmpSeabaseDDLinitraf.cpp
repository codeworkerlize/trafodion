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
 * File:         CmpSeabaseDDLinitraf.cpp
 * Description:  Implements initialize trafodion
 *
 *
 * Created:
 * Language:     C++
 *
 *
 *****************************************************************************
 */

#include "sqlcomp/CmpSeabaseDDLincludes.h"
#include "sqlcomp/CmpSeabaseDDLXdcMeta.h"

enum InitTrafSteps {
  IT_NO_CHANGE = -1,
  IT_START = 0,
  IT_VERIFY_USER,
  IT_VERSION_CHECK,
  IT_RSRVD_NAMESPACES,
  IT_TDDL_TRUNCATE,
  IT_SG_ORDER_CREATE,
  IT_START_XN,
  IT_CREATE_MD_TABLES,
  IT_UPDATE_MD_TABLES,
  IT_CREATE_XDC_MD_TABLES,
  IT_CREATE_MD_PART_TABLES,
  IT_CREATE_SCHEMA_OBJECTS,
  IT_CREATE_MD_VIEWS,
  IT_CREATE_REPOS,
  IT_CREATE_LIBRARIES,
  IT_CREATE_PRIVMGR_REPOS,
  IT_CREATE_BACKUP_REPOS,
  IT_CREATE_LIBMGR,
  IT_CREATE_TENANT,
  IT_STEP_FAILED,
  IT_ABORT_XN,
  IT_FAILED,
  IT_DONE,
};

static void initDWS(CmpDDLwithStatusInfo *dws) {
  dws->setBlackBoxLen(0);
  dws->setBlackBox(NULL);
  dws->setComputeST(FALSE);
  dws->setComputeET(FALSE);
  dws->setReturnET(FALSE);
}

static void setValuesInDWS(CmpDDLwithStatusInfo *dws, int nextStep, const char *msg = NULL, int subStep = 0,
                           NABoolean isEndStep = FALSE, NABoolean computeST = FALSE, NABoolean computeET = FALSE,
                           NABoolean returnET = FALSE, NABoolean done = FALSE) {
  if (msg) dws->setMsg(msg);
  if (nextStep != IT_NO_CHANGE) dws->setStep(nextStep);
  if (subStep >= 0) dws->setSubstep(subStep);
  dws->setEndStep(isEndStep);

  if (computeST) dws->setComputeST(computeST);
  if (computeET) dws->setComputeET(computeET);
  if (returnET) dws->setReturnET(computeET);

  if (done) dws->setDone(TRUE);
}

short CmpSeabaseDDL::initTrafMD(CmpDDLwithStatusInfo *dws) {
  int cliRC = 0;

  initDWS(dws);

  while (1)  // exit via return from within the while loop
  {
    switch (dws->step()) {
      case IT_START: {
        setValuesInDWS(dws, IT_VERIFY_USER, "Initialize Trafodion: Started", 0, TRUE);

        return 0;
      } break;

      case IT_VERIFY_USER: {
        switch (dws->subStep()) {
          case 0: {
            setValuesInDWS(dws, IT_NO_CHANGE, "Verify User: Started", 1, FALSE, TRUE);

            return 0;
          } break;

          case 1: {
            // verify user is authorized
            if (!ComUser::isRootUserID()) {
              //*CmpCommon::diags() << DgSqlCode(CAT_NOT_AUTHORIZED);

              setValuesInDWS(
                  dws, IT_NO_CHANGE,
                  "  Current user is not authorized to Initialize Trafodion. Must be Root to perform this operation.",
                  2, FALSE);

              return 0;
            }

            CmpCommon::diags()->clear();

            setValuesInDWS(dws, IT_VERSION_CHECK, "Verify User: Completed", 0, TRUE, FALSE, TRUE, TRUE);
            return 0;
          }  // case 1
          break;

          case 2: {
            setValuesInDWS(dws, IT_FAILED, "Verify User: Failed", 0, TRUE, FALSE, TRUE, TRUE);

            return 0;
          } break;

        }  // switch
      } break;

      case IT_VERSION_CHECK: {
        int hbaseErrNum = 0;
        NAString hbaseErrStr;

        ExpHbaseInterface *ehi = allocEHI(COM_STORAGE_HBASE);
        if (ehi == NULL) {
          setValuesInDWS(dws, IT_FAILED, NULL, 0, TRUE);

          break;
        }

        switch (dws->subStep()) {
          case 0: {
            setValuesInDWS(dws, IT_NO_CHANGE, "Version Check: Started", 1, FALSE, TRUE);

            return 0;
          } break;

          case 1: {
            // check if traf is already initialized
            int errNum = validateVersions(&ActiveSchemaDB()->getDefaults(), ehi, NULL, NULL, NULL, NULL, NULL, NULL,
                                            NULL, NULL, NULL, &hbaseErrNum, &hbaseErrStr);
            deallocEHI(ehi);

            // namespace upgrade should not be done when moving from
            // pre-2.5 to 2.5.
            // It should have already been upgraded when the move was
            // done from pre-R2.3 to R2.3 as namespace support was
            // added in R2.3.
            if (errNum == -1409)  // namespace upgrade needed.
            {
              // error.
              *CmpCommon::diags() << DgSqlCode(3242) << DgString0("Namespace cannot be upgraded.");
              setValuesInDWS(dws, IT_FAILED, "Version Check: Failed", 0, TRUE, FALSE, TRUE, TRUE);

              return 0;
            }

            if (errNum == 0) {
              CmpCommon::context()->setIsUninitializedSeabase(FALSE);

              // add warning 1392 to indicate that traf is already init.
              // This is needed as installer checks for this error code to
              // determine if traf is initialized.
              *CmpCommon::diags() << DgSqlCode(TRAF_ALREADY_INITIALIZED);

              // Metadata is initialized
              setValuesInDWS(dws, IT_NO_CHANGE, "  Trafodion is already initialized on this system.", 2, FALSE);

              return 0;
            }

            CmpCommon::context()->setIsUninitializedSeabase(TRUE);
            CmpCommon::context()->uninitializedSeabaseErrNum() = errNum;
            CmpCommon::context()->hbaseErrNum() = hbaseErrNum;
            CmpCommon::context()->hbaseErrStr() = hbaseErrStr;
            if (errNum != -TRAF_NOT_INITIALIZED && errNum != -TRAF_COLUMN_VALUE_EMPTY && errNum != -1395 ||
                hbaseErrStr.length() > 0) {
              LOGERROR(CAT_SQL_CI, "Failed to validate versions, process will exit. ErrNum: %d , ErrStr: %s", errNum,
                       hbaseErrStr.data());
              exit(0);
            }

            // Return any other error besides uninitialized traf error.
            if (errNum != -TRAF_NOT_INITIALIZED) {
              // add returned error nums as warnings.
              // It will show why init traf failed.
              if (errNum == -TRAF_HBASE_ACCESS_ERROR)
                *CmpCommon::diags() << DgSqlCode(-errNum) << DgInt0(hbaseErrNum) << DgString0(hbaseErrStr);
              else
                *CmpCommon::diags() << DgSqlCode(-errNum);

              char msgBuf[1000];
              sprintf(msgBuf, "  Error %d returned. See error details for further action.", -errNum);

              setValuesInDWS(dws, IT_NO_CHANGE, msgBuf, 3, FALSE);

              return 0;
            }

            CmpCommon::diags()->clear();

            setValuesInDWS(dws, IT_RSRVD_NAMESPACES, "Version Check: Completed", 0, TRUE, FALSE, TRUE, TRUE);

            return 0;
          }  // case 1
          break;

          case 2: {
            setValuesInDWS(dws, IT_DONE, "Version Check: Completed", 0, TRUE, FALSE, TRUE, TRUE);

            return 0;
          } break;

          case 3: {
            setValuesInDWS(dws, IT_FAILED, "Version Check: Failed", 0, TRUE, FALSE, TRUE, TRUE);

            return 0;
          } break;

        }  // switch
      } break;

      case IT_RSRVD_NAMESPACES: {
        switch (dws->subStep()) {
          case 0: {
            setValuesInDWS(dws, IT_NO_CHANGE, "Create Reserved Namespaces: Started", 1, FALSE, TRUE, FALSE, FALSE);
            return 0;
          } break;

          case 1: {
            ExpHbaseInterface *ehi = allocEHI(COM_STORAGE_HBASE);
            if (ehi == NULL) {
              setValuesInDWS(dws, IT_FAILED);

              break;
            }

            if (dropCreateReservedNamespaces(ehi, TRUE, TRUE)) {
              setValuesInDWS(dws, IT_STEP_FAILED, "Create Reserved Namespaces: Failed", 0, TRUE, FALSE, TRUE, TRUE);

              return 0;
            }

            setValuesInDWS(dws, IT_TDDL_TRUNCATE, "Create Reserved Namespaces: Completed", 0, TRUE, FALSE, TRUE, TRUE);

            return 0;
          }  // case 1
        }    // switch
      } break;

      case IT_TDDL_TRUNCATE: {
        ExpHbaseInterface *ehi = allocEHI(COM_STORAGE_HBASE);
        if (ehi == NULL) {
          setValuesInDWS(dws, IT_FAILED);

          break;
        }

        // truncate DTM table TDDL.
        // Do not do this operation under a dtm transaction.
        // See file
        // core/sqf/src/seatrans/hbase-trx/src/main/java/org/apache/hadoop/hbase/client/transactional/TmDDL.java Keep
        // the name TRAFODION._DTM_.TDDL and col fam "tddlcf" in sync with that file.
        HbaseStr tddlTable;
        const NAString tddlNAS("TRAFODION._DTM_.TDDL");
        tddlTable.val = (char *)tddlNAS.data();
        tddlTable.len = tddlNAS.length();
        if (ehi->exists(tddlTable) == -1)  // exists
        {
          ehi->truncate(tddlTable, TRUE, TRUE);
        }

        deallocEHI(ehi);

        setValuesInDWS(dws, IT_SG_ORDER_CREATE);
      } break;

      case IT_SG_ORDER_CREATE: {
        ExpHbaseInterface *ehi = allocEHI(COM_STORAGE_HBASE);
        if (ehi == NULL) {
          setValuesInDWS(dws, IT_FAILED);
          break;
        }

        HbaseStr osgTable, osgColf;
        const NAString oSGNAS("ESG_TRAFODION._ORDER_SG_.ORDER_SEQ_GEN");
        const NAString oSGCFNAS(SEABASE_DEFAULT_COL_FAMILY);  // "#1"
        osgTable.val = (char *)oSGNAS.data();
        osgTable.len = oSGNAS.length();

        HBASE_NAMELIST osgCF(STMTHEAP);
        osgColf.val = (char *)oSGCFNAS.data();
        osgColf.len = oSGCFNAS.length();
        osgCF.insert(osgColf);

        if (ehi->exists(osgTable) == -1)  // exist
        {
          ehi->drop(osgTable, TRUE, TRUE);
        }

        if (ehi->create(osgTable, osgCF, TRUE) < 0)  // error
        {
          deallocEHI(ehi);
          setValuesInDWS(dws, IT_FAILED);
          return 0;
        }

        deallocEHI(ehi);
        setValuesInDWS(dws, IT_START_XN);
      } break;

      case IT_START_XN: {
        if (dws->getDDLXns()) {
          ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());

          NABoolean xnWasStartedHere = FALSE;
          if (beginXnIfNotInProgress(&cliInterface, xnWasStartedHere)) {
            setValuesInDWS(dws, IT_FAILED);
            break;
          }
          dws->setXnStarted(TRUE);
        }

        setValuesInDWS(dws, IT_CREATE_MD_TABLES);
      } break;

      case IT_CREATE_MD_TABLES: {
        switch (dws->subStep()) {
          case 0: {
            setValuesInDWS(dws, IT_NO_CHANGE, "Create Metadata Tables: Started", 1, FALSE, TRUE, FALSE, FALSE);
            return 0;
          } break;

          case 1: {
            ExpHbaseInterface *ehi = allocEHI(COM_STORAGE_HBASE);
            if (ehi == NULL) {
              setValuesInDWS(dws, IT_STEP_FAILED, "Create Metadata Tables: Failed", 0, TRUE, FALSE, TRUE, TRUE);

              return 0;
            }

            int numTables = sizeof(allMDtablesInfo) / sizeof(MDTableInfo);
            const char *sysCat = ActiveSchemaDB()->getDefaults().getValue(SEABASE_CATALOG);

            // create hbase physical objects
            for (int i = 0; i < numTables; i++) {
              const MDTableInfo &mdti = allMDtablesInfo[i];

              HbaseStr hbaseObject;

              NAString nameSpace(TRAF_RESERVED_NAMESPACE1);
              NAString hbaseObjectStr =
                  genHBaseObjName(NAString(sysCat), NAString(SEABASE_MD_SCHEMA), NAString(mdti.newName), nameSpace);
              hbaseObject.val = (char *)hbaseObjectStr.data();
              hbaseObject.len = hbaseObjectStr.length();
              if (createHbaseTable(ehi, &hbaseObject, SEABASE_DEFAULT_COL_FAMILY, NULL, 0, 0, NULL, FALSE,
                                   dws->getDDLXns()) == -1) {
                deallocEHI(ehi);

                setValuesInDWS(dws, IT_STEP_FAILED, "Create Metadata Tables: Failed", 0, TRUE, FALSE, TRUE, TRUE);

                return 0;
              }
            }  // for

            deallocEHI(ehi);
            ehi = NULL;

            setValuesInDWS(dws, IT_UPDATE_MD_TABLES, "Create Metadata Tables: Completed", 0, TRUE, FALSE, TRUE, TRUE);

            return 0;
          } break;

        }  // switch
      } break;

      case IT_UPDATE_MD_TABLES: {
        switch (dws->subStep()) {
          case 0: {
            setValuesInDWS(dws, IT_NO_CHANGE, "Update Metadata Tables: Started", 1, FALSE, TRUE, FALSE, FALSE);

            return 0;
          } break;

          case 1: {
            ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());

            long objectFlags = 0;
            long schemaUID = -1;

            const char *sysCat = ActiveSchemaDB()->getDefaults().getValue(SEABASE_CATALOG);

            // this is a static function and can be set at any time.
            setBootstrapMode(TRUE);

            // Create Seabase metadata schema
            schemaUID = -1;
            if (updateSeabaseMDObjectsTable(&cliInterface, sysCat, SEABASE_MD_SCHEMA, SEABASE_SCHEMA_OBJECTNAME,
                                            COM_PRIVATE_SCHEMA_OBJECT, "Y", SUPER_USER, SUPER_USER, objectFlags,
                                            schemaUID)) {
              setValuesInDWS(dws, IT_STEP_FAILED, "Update Metadata Tables: Failed", 0, TRUE, FALSE, TRUE, TRUE);

              return 0;
            }

            // update MD with information about metadata objects
            int numTables = sizeof(allMDtablesInfo) / sizeof(MDTableInfo);
            for (int i = 0; i < numTables; i++) {
              const MDTableInfo &mdti = allMDtablesInfo[i];
              MDDescsInfo &mddi = CmpCommon::context()->getTrafMDDescsInfo()[i];

              if (mdti.isIndex) continue;

              long objUID = -1;
              if (updateSeabaseMDTable(&cliInterface, sysCat, SEABASE_MD_SCHEMA, mdti.newName, COM_BASE_TABLE_OBJECT,
                                       "Y", mddi.tableInfo, mddi.numNewCols, mddi.newColInfo, mddi.numNewKeys,
                                       mddi.newKeyInfo, mddi.numIndexes, mddi.indexInfo, objUID)) {
                setValuesInDWS(dws, IT_STEP_FAILED, "Update Metadata Tables: Failed", 0, TRUE, FALSE, TRUE, TRUE);

                return 0;
              }

            }  // for

            // update metadata with metadata indexes information
            for (int i = 0; i < numTables; i++) {
              const MDTableInfo &mdti = allMDtablesInfo[i];
              MDDescsInfo &mddi = CmpCommon::context()->getTrafMDDescsInfo()[i];

              if (NOT mdti.isIndex) continue;

              long objUID = -1;
              if (updateSeabaseMDTable(&cliInterface, sysCat, SEABASE_MD_SCHEMA, mdti.newName, COM_INDEX_OBJECT, "Y",
                                       mddi.tableInfo, mddi.numNewCols, mddi.newColInfo, mddi.numNewKeys,
                                       mddi.newKeyInfo, 0, NULL, objUID)) {
                setValuesInDWS(dws, IT_STEP_FAILED, "Update Metadata Tables: Failed", 0, TRUE, FALSE, TRUE, TRUE);
                return 0;
              }
            }  // for

            // update metadata with metadata secondary indexes information
            if (updateSeabaseMDSecondaryIndexes(&cliInterface)) {
              setValuesInDWS(dws, IT_STEP_FAILED, "Update Metadata Tables: Failed", 0, TRUE, FALSE, TRUE, TRUE);

              return 0;
            }

            updateSeabaseVersions(&cliInterface, sysCat);
            updateSeabaseAuths(&cliInterface, sysCat);

            CmpCommon::context()->setIsUninitializedSeabase(FALSE);
            CmpCommon::context()->uninitializedSeabaseErrNum() = 0;
            CmpCommon::context()->setUseReservedNamespace(TRUE);

            setValuesInDWS(dws, (dws->getMinimalInitTraf() ? IT_CREATE_MD_VIEWS : IT_CREATE_XDC_MD_TABLES),
                           "Create Metadata Tables: Completed", 0, TRUE, FALSE, TRUE, TRUE);

            return 0;
          }  // case 1
        }    // switch
      } break;

      case IT_CREATE_XDC_MD_TABLES: {
        switch (dws->subStep()) {
          case 0: {
            setValuesInDWS(dws, IT_NO_CHANGE, "Create XDC Metadata Tables: Started", 1, FALSE, TRUE, FALSE, FALSE);
            return 0;
          } break;

          case 1: {
            ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());

            if (createXDCMeta(&cliInterface)) {
              setValuesInDWS(dws, IT_STEP_FAILED, "Create XDC Metadata Tables: Failed", 0, TRUE, FALSE, TRUE, TRUE);

              return 0;
            }

            setValuesInDWS(dws, IT_CREATE_MD_PART_TABLES, "Create XDC Metadata Tables: Completed", 0, TRUE, FALSE, TRUE,
                           TRUE);

            return 0;
          } break;

        }  // switch
      } break;

      case IT_CREATE_MD_PART_TABLES: {
        switch (dws->subStep()) {
          case 0: {
            setValuesInDWS(dws, IT_NO_CHANGE, "Create Metadata Partition Tables: Started", 1, FALSE, TRUE, FALSE,
                           FALSE);
            return 0;
          } break;

          case 1: {
            ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());

            if (createMDPartTables(&cliInterface)) {
              setValuesInDWS(dws, IT_STEP_FAILED, "Create Metadata Partition Tables: Failed", 0, TRUE, FALSE, TRUE,
                             TRUE);

              return 0;
            }

            setValuesInDWS(dws, IT_CREATE_MD_VIEWS, "Create Metadata Partition Tables: Completed", 0, TRUE, FALSE, TRUE,
                           TRUE);

            return 0;
          } break;

        }  // switch
      } break;

      case IT_CREATE_MD_VIEWS: {
        switch (dws->subStep()) {
          case 0: {
            setValuesInDWS(dws, IT_NO_CHANGE, "Create Metadata Views: Started", 1, FALSE, TRUE, FALSE, FALSE);

            return 0;
          } break;

          case 1: {
            ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());

            if (createMetadataViews(&cliInterface)) {
              setValuesInDWS(dws, IT_STEP_FAILED, "Create Metadata Views: Failed", 0, TRUE, FALSE, TRUE, TRUE);

              return 0;
            }

            // If this is a MINIMAL initialization, don't create schema objects,
            // traf repository, backup repository, privilege manager tables or
            // tenant repository.
            // This short-circuits the remaining steps
            //    IT_CREATE_MD_VIEWS (current step)
            //    -- IT_CREATE_LIBRARIES
            //    -- IT_CREATE_SCHEMA_OBJECTS
            //    -- IT_CREATE_REPOS
            //    -- IT_CREATE_PRIVMGR_REPOS
            //    -- IT_CREATE_BACKUP_REPOS
            //    -- IT_CREATE_TENANT
            //    -- IT_CREATE_LIBMGR
            //   IT_DONE
            setValuesInDWS(dws, (dws->getMinimalInitTraf() ? IT_DONE : IT_CREATE_LIBRARIES),
                           "Create Metadata Views: Completed", 0, TRUE, FALSE, TRUE, TRUE);

            return 0;
          }  // case
        }    // switch
      } break;

      case IT_CREATE_LIBRARIES: {
        switch (dws->subStep()) {
          case 0: {
            setValuesInDWS(dws, IT_NO_CHANGE, "Create Libraries Tables: Started", 1, FALSE, TRUE, FALSE, FALSE);

            return 0;
          } break;

          case 1: {
            ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());

            cliRC = cliInterface.holdAndSetCQDs("TRAF_LOB_VERSION2, COMP_BOOL_990",
                                                "TRAF_LOB_VERSION2 'OFF', COMP_BOOL_990 'ON'");
            if (createLibraries(&cliInterface)) {
              setValuesInDWS(dws, IT_STEP_FAILED, "Create Libraries Tables: Failed", 0, TRUE, FALSE, TRUE, TRUE);
              return 0;
            }

            // Note that this is not an existing jar file, the class
            // loader will attempt to load the class from the CLASSPATH if
            // it can't find this jar
            const char *sysCat = ActiveSchemaDB()->getDefaults().getValue(SEABASE_CATALOG);
            NAString installJar("trafodion-sql-currversion.jar");

            // update library info
            long libUID;
            if (updateSeabaseMDLibrary(&cliInterface, sysCat, SEABASE_MD_SCHEMA, SEABASE_VALIDATE_LIBRARY,
                                       installJar.data(), 1 /* libVersion */, libUID, SUPER_USER, SUPER_USER)) {
              setValuesInDWS(dws, IT_STEP_FAILED, "Update Libraries Tables: Failed", 0, TRUE, FALSE, TRUE, TRUE);

              return 0;
            }

            // update SPJ info
            if (updateSeabaseMDSPJ(&cliInterface, libUID, SUPER_USER, SUPER_USER, &seabaseMDValidateRoutineInfo,
                                   sizeof(seabaseMDValidateRoutineColInfo) / sizeof(ComTdbVirtTableColumnInfo),
                                   seabaseMDValidateRoutineColInfo)) {
              setValuesInDWS(dws, IT_STEP_FAILED, "Update MDSPJ : Failed", 0, TRUE, FALSE, TRUE, TRUE);

              return 0;
            }

            cliInterface.restoreCQDs("TRAF_LOB_VERSION2, COMP_BOOL_990");
            setValuesInDWS(dws, IT_CREATE_SCHEMA_OBJECTS, "Create Libraries Tables: Completed", 0, TRUE, FALSE, TRUE,
                           TRUE);
            return 0;
          }  // case 1
        }    // switch
      } break;

      case IT_CREATE_SCHEMA_OBJECTS: {
        switch (dws->subStep()) {
          case 0: {
            setValuesInDWS(dws, IT_NO_CHANGE, "Create Schema Objects: Started", 1, FALSE, TRUE, FALSE, FALSE);

            return 0;
          } break;

          case 1: {
            ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());

            if (createDefaultSystemSchema(&cliInterface)) {
              setValuesInDWS(dws, IT_STEP_FAILED, "Create Schema Objects: Failed", 0, TRUE, FALSE, TRUE, TRUE);

              return 0;
            }

            // this is old code used before we registered schema objects
            // in the OBJECTS table, not longer needed
            // TBD: remove createSchemaObjects infrastructure
#if 0
                  if (createSchemaObjects(&cliInterface))
                    {
                      setValuesInDWS(dws, IT_STEP_FAILED,
                                     "Create Schema Objects: Failed", 0, TRUE,
                                     FALSE, TRUE, TRUE);

                      return 0;
                    }
#endif

            setValuesInDWS(dws, IT_CREATE_REPOS, "Create Schema Objects: Completed", 0, TRUE, FALSE, TRUE, TRUE);

            return 0;
          }  // case 1
        }    // switch
      } break;

      case IT_CREATE_REPOS: {
        switch (dws->subStep()) {
          case 0: {
            setValuesInDWS(dws, IT_NO_CHANGE, "Create Repository Tables: Started", 1, FALSE, TRUE, FALSE, FALSE);

            return 0;
          } break;

          case 1: {
            ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());

            if (createRepos(&cliInterface)) {
              setValuesInDWS(dws, IT_STEP_FAILED, "Create Repository Tables: Failed", 0, TRUE, FALSE, TRUE, TRUE);
              return 0;
            }

            setValuesInDWS(dws, IT_CREATE_PRIVMGR_REPOS, "Create Repository Tables: Completed", 0, TRUE, FALSE, TRUE,
                           TRUE);

            return 0;
          }  // case 1
        }    // switch
      } break;

      case IT_CREATE_PRIVMGR_REPOS: {
        switch (dws->subStep()) {
          case 0: {
            setValuesInDWS(dws, IT_NO_CHANGE, "Create PrivMgr Tables: Started", 1, FALSE, TRUE, FALSE, FALSE);
            return 0;
          } break;

          case 1: {
            ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());

            if (createPrivMgrRepos(&cliInterface, dws->getDDLXns())) {
              setValuesInDWS(dws, IT_STEP_FAILED, "Create PrivMgr Tables: Failed", 0, TRUE, FALSE, TRUE, TRUE);
              return 0;
            }

            setValuesInDWS(dws, IT_CREATE_BACKUP_REPOS, "Create PrivMgr Tables: Completed", 0, TRUE, FALSE, TRUE, TRUE);

            return 0;
          }  // case 1
        }    // switch
      } break;

      case IT_CREATE_BACKUP_REPOS: {
        if (CmpCommon::getDefault(TRAF_BACKUP_REPOSITORY_SUPPORT) == DF_OFF) {
          setValuesInDWS(dws, IT_CREATE_TENANT);
          break;
        }

        switch (dws->subStep()) {
          case 0: {
            setValuesInDWS(dws, IT_NO_CHANGE, "Create Backup Repository Tables: Started", 1, FALSE, TRUE, FALSE, FALSE);

            return 0;
          } break;

          case 1: {
            ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());

            if (createBackupRepos(&cliInterface)) {
              setValuesInDWS(dws, IT_STEP_FAILED, "Create Backup Repository Tables: Failed", 0, TRUE, FALSE, TRUE,
                             TRUE);
              return 0;
            }

            setValuesInDWS(dws, IT_CREATE_TENANT, "Create Backup Repository Tables: Completed", 0, TRUE, FALSE, TRUE,
                           TRUE);

            return 0;
          }  // case 1
        }    // switch
      } break;

      case IT_CREATE_TENANT: {
        switch (dws->subStep()) {
          case 0: {
            setValuesInDWS(dws, IT_NO_CHANGE, "Create Tenant: Started", 1, FALSE, TRUE, FALSE, FALSE);

            return 0;
          } break;

          case 1: {
            ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());

            if (createSeabaseTenant(&cliInterface, FALSE /*addcompprivs*/, FALSE /*reportNotEnabled*/)) {
              restoreAllFlags();

              setValuesInDWS(dws, IT_STEP_FAILED, "Create Tenant: Failed", 0, TRUE, FALSE, TRUE, TRUE);

              return 0;
            }

            setValuesInDWS(dws, IT_CREATE_LIBMGR, "Create Tenant: Completed", 0, TRUE, FALSE, TRUE, TRUE);

            return 0;
          }  // case 1
        }    // switch
      } break;

      case IT_CREATE_LIBMGR: {
        switch (dws->subStep()) {
          case 0: {
            setValuesInDWS(dws, IT_NO_CHANGE, "Create Library Manager: Started", 1, FALSE, TRUE, FALSE, FALSE);

            return 0;
          } break;

          case 1: {
            ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());

            if (createSeabaseLibmgr(&cliInterface)) {
              setValuesInDWS(dws, IT_STEP_FAILED, "Create Library Manager: Failed", 0, TRUE, FALSE, TRUE, TRUE);

              return 0;
            }

            setValuesInDWS(dws, IT_DONE, "Create Library Manager: Completed", 0, TRUE, FALSE, TRUE, TRUE);

            return 0;
          }  // case 1
        }    // switch
      } break;

      case IT_STEP_FAILED: {
        setValuesInDWS(dws, (dws->xnStarted() ? IT_ABORT_XN : IT_FAILED));

        break;
      } break;

      case IT_ABORT_XN: {
        switch (dws->subStep()) {
          case 0: {
            setValuesInDWS(dws, IT_NO_CHANGE, "Abort Transaction: Started", 1, FALSE, TRUE, FALSE, FALSE);

            return 0;
          } break;

          case 1: {
            ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());

            NABoolean xnWasStartedHere = dws->xnStarted();
            endXnIfStartedHere(&cliInterface, xnWasStartedHere, -1);
            dws->setXnStarted(xnWasStartedHere);

            ExpHbaseInterface *ehi = allocEHI(COM_STORAGE_HBASE);
            if (ehi) {
              dropCreateReservedNamespaces(ehi, TRUE, FALSE);
            }

            setValuesInDWS(dws, IT_FAILED, "Abort Transaction: Completed", 0, TRUE, FALSE, TRUE, TRUE);

            return 0;
          } break;
        }  // switch
      } break;

      case IT_FAILED: {
        setValuesInDWS(dws, IT_START, "Initialize Trafodion: Failed", 0, FALSE, FALSE, TRUE, TRUE, TRUE);

        setBootstrapMode(FALSE);

        return -1;
      } break;

      case IT_DONE: {
        ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());

        NABoolean xnWasStartedHere = dws->xnStarted();
        endXnIfStartedHere(&cliInterface, xnWasStartedHere, 0);
        dws->setXnStarted(xnWasStartedHere);

        setValuesInDWS(dws, IT_START, "Initialize Trafodion: Completed", 0, FALSE, FALSE, TRUE, TRUE, TRUE);

        setBootstrapMode(FALSE);

        return 0;
      } break;
    }  // switch
  }    // while

  return 0;
}

short CmpSeabaseDDL::createOrDropLobMD(ExeCliInterface *cliInterface, NABoolean isCreate) {
  int cliRC = 0;

  char queryBuf[4000];

  NABoolean xnWasStartedHere = FALSE;

  const QString *qs = NULL;
  Int32 sizeOfqs = 0;
  Int32 qryArraySize = 0;
  char *gluedQuery;
  int gluedQuerySize;

  if (isCreate && beginXnIfNotInProgress(cliInterface, xnWasStartedHere)) goto label_return;

  str_sprintf(queryBuf, "%s.\"%s\"", getSystemCatalog(), SEABASE_MD_SCHEMA);
  cliRC = cliInterface->holdAndSetCQD("SCHEMA", queryBuf);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    goto label_return;
  }

  if (isCreate) {
    qs = seabaseLobColumnsDDL;
    sizeOfqs = sizeof(seabaseLobColumnsDDL);
    qryArraySize = sizeOfqs / sizeof(QString);

    glueQueryFragments(qryArraySize, qs, gluedQuery, gluedQuerySize);

    str_sprintf(queryBuf, gluedQuery);
    NADELETEBASICARRAY(gluedQuery, STMTHEAP);

    cliRC = cliInterface->executeImmediate(queryBuf);
    if (cliRC == -1390)  // table already exists
    {
      // ignore error.
      cliRC = 0;
    } else if (cliRC < 0) {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    }
  } else {
    str_sprintf(queryBuf, "cleanup table %s.\"%s\".%s", getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_LOB_COLUMNS);
    cliRC = cliInterface->executeImmediate(queryBuf);
    if (cliRC < 0) {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    }
  }
label_return:
  cliInterface->restoreCQD("SCHEMA");
  endXnIfStartedHere(cliInterface, xnWasStartedHere, cliRC);

  return cliRC;
}
