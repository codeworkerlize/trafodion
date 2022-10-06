

/* -*-C++-*-
 *****************************************************************************
 *
 * File:         CmpSeabaseDDLXdcMeta.cpp
 * Description:  Implements common methods and operations for XDC metadata tables.
 *
 *
 * Created:     8/10/2020
 * Language:     C++
 *
 *
 *****************************************************************************
 */

#include "sqlcomp/CmpSeabaseDDLincludes.h"
#include "sqlcomp/CmpSeabaseDDLXdcMeta.h"
#include "sqlmxevents/logmxevent_traf.h"

short CmpSeabaseDDL::createXDCMeta(ExeCliInterface *cliInterface) {
  int cliRC = 0;

  char queryBuf[2000];

  NABoolean xnWasStartedHere = FALSE;

  if (beginXnIfNotInProgress(cliInterface, xnWasStartedHere)) goto label_error;

  // Create the _XDC_MD_ schema
  if (CmpCommon::context()->useReservedNamespace())
    str_sprintf(queryBuf, "create schema %s.\"%s\" namespace '%s' ; ", getSystemCatalog(), SEABASE_XDC_MD_SCHEMA,
                ComGetReservedNamespace(SEABASE_XDC_MD_SCHEMA).data());
  else
    str_sprintf(queryBuf, "create schema %s.\"%s\" ; ", getSystemCatalog(), SEABASE_XDC_MD_SCHEMA);

  cliRC = cliInterface->executeImmediate(queryBuf);
  if (cliRC == -1022)  // schema already exists
  {
    // ignore error.
    cliRC = 0;
  } else if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
  }

  if (endXnIfStartedHere(cliInterface, xnWasStartedHere, cliRC) < 0) goto label_error;

  for (int i = 0; i < sizeof(allXDCMDUpgradeInfo) / sizeof(MDUpgradeInfo); i++) {
    const MDUpgradeInfo &rti = allXDCMDUpgradeInfo[i];

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

    param_[0] = getSystemCatalog();
    param_[1] = SEABASE_XDC_MD_SCHEMA;

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

short CmpSeabaseDDL::dropXDCMeta(ExeCliInterface *cliInterface, NABoolean dropSchema) {
  int cliRC = 0;
  NABoolean xnWasStartedHere = FALSE;
  char queryBuf[1000];

  for (int i = 0; i < sizeof(allXDCMDUpgradeInfo) / sizeof(MDUpgradeInfo); i++) {
    const MDUpgradeInfo &rti = allXDCMDUpgradeInfo[i];

    str_sprintf(queryBuf, "drop table %s.\"%s\".%s cascade; ", getSystemCatalog(), SEABASE_XDC_MD_SCHEMA,
                (rti.newName));

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

  if (dropSchema) {
    // Drop the _XDC_MD_ schema
    str_sprintf(queryBuf, "drop schema %s.\"%s\" cascade; ", getSystemCatalog(), SEABASE_XDC_MD_SCHEMA);

    if (beginXnIfNotInProgress(cliInterface, xnWasStartedHere)) {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      return -1;
    }

    cliRC = cliInterface->executeImmediate(queryBuf);
    if (cliRC == -1003)  // schema doesnt exist
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

void CmpSeabaseDDL::processXDCMeta(NABoolean create, NABoolean drop, NABoolean upgrade) {
  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());

  if (create)
    createXDCMeta(&cliInterface);
  else if (drop)
    dropXDCMeta(&cliInterface);

  // currently we do nothing for upgrade
  return;
}
