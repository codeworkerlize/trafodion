
#include "sqlcomp/CmpSeabaseDDLincludes.h"
#include "optimizer/RelExeUtil.h"
#include "cli/Globals.h"
#include "cli/Context.h"
#include "common/feerrors.h"

#include <sys/stat.h>
#include <fstream>

const RelDumpLoad::DumploadTypeInfo RelDumpLoad::dumploadType[] = {
    {RelDumpLoad::DL_OBJ_SCHEMA, "SHOWDDL SCHEMA   "},     {RelDumpLoad::DL_OBJ_TABLE, "SHOWDDL TABLE    "},
    {RelDumpLoad::DL_OBJ_LIBRARY, "SHOWDDL LIBRARY  "},    {RelDumpLoad::DL_OBJ_VIEW, "SHOWDDL          "},
    {RelDumpLoad::DL_OBJ_UDRO, "SHOWDDL FUNCTION "},       {RelDumpLoad::DL_OBJ_SGO, "SHOWDDL SEQUENCE "},
    {RelDumpLoad::DL_OBJ_PROCEDURE, "SHOWDDL PROCEDURE "}, {RelDumpLoad::DL_OBJ_TRIGGER, "SHOWDDL TRIGGER "},
    {RelDumpLoad::DL_OBJ_PACKAGE, "SHOWDDL PACKAGE "},     {RelDumpLoad::DL_OBJ_UNKNOWN, "SHOWDDL UNKNOWN  "}};

static short createDumpDir(const NAString &localDir) {
  Int32 rval = mkdir(localDir.data(), S_IRWXU | S_IRWXG);
  Int32 error = errno;

  if ((rval) != 0 && (error != EEXIST))  // EEXIST is meaning 'File already exists'.
  {
    char errMessage[1024];
    switch (error) {
      case EACCES:
        snprintf(errMessage, sizeof(errMessage), "Could not create directory %s, permission denied", localDir.data());
        break;
      case ENOENT:
        snprintf(errMessage, sizeof(errMessage), "Coule not create directory %s, component of the path does not exist.",
                 localDir.data());
        break;
      case EROFS:
        snprintf(errMessage, sizeof(errMessage), "Could not create directory %s, read-only filesystem.",
                 localDir.data());
        break;
      case ENOTDIR:
        snprintf(errMessage, sizeof(errMessage),
                 "Could not create directory %s, a component of the path is not a directory.", localDir.data());
        break;
      default:
        snprintf(errMessage, sizeof(errMessage), "Could not create %s, errno is %d", localDir.data(), error);
        break;
    }
    *CmpCommon::diags() << DgSqlCode(-1110) << DgString0(errMessage);
    return -1;
  }
  return 0;
}

static void checkAndRemoveUnnecessaryFields(RelDumpLoad *ddlExpr, char *originalStr) {
  // Please beware of the order of these strings.
  const char *unnecessaryFields[] = {/*The result of 'SHOWDDL FUNCTION' syntax include this,
                                      *if execute this, will trigger a error(3724)*/
                                     "NO FINAL CALL",

                                     /*add anthor unnecessary field to here.*/

                                     "\0"};

  const char *keyString = NULL;
  Int32 keyStringLength = -1;
  char *pos = NULL;
  for (Int32 i = 0;; i++) {
    keyString = unnecessaryFields[i];
    if (keyString[0] == '\0') break;

    keyStringLength = str_len(keyString);
    pos = strstr(originalStr, keyString);
    if (pos != NULL) {
      Int32 originalLength = str_len(originalStr);
      str_cpy(pos, pos + keyStringLength, originalLength - (pos - originalStr) - keyStringLength);
      originalStr[originalLength - keyStringLength] = '\0';
    }
  }
}

static void checkAndDumpDependentFile(RelDumpLoad *ddlExpr, char *line) {
  /**
   * if dump somethings such as library,
   * output message will contains a local file path.
   * in there, i will find and copy dependent
   * file to dest directory through "FILE".
   **/
  const char *identifier = "FILE";
  Int32 len = str_len(line);
  if (len < str_len(identifier)) return;

  char *pos = strstr(line, identifier);
  if (pos == NULL) return;

  char *quote1 = strchr(pos, '\'');
  if (quote1 == NULL) return;

  char *quote2 = strchr(quote1 + 1, '\'');
  if (quote2 == NULL) return;

  // 1. construct input file
  NAString originalFile(quote1 + 1, quote2 - quote1 - 1);
  ifstream in(originalFile.data(), ios::binary);
  if (!in.is_open()) return;

  // 2. construct out file
  int idx = originalFile.length() - 1;
  while (idx >= 0)
    if (originalFile[idx] == '/')
      break;
    else
      idx--;

  idx++;
  const NAString &destDir = ddlExpr->locationPath();
  int lenWithoutPath = originalFile.length() - idx;
  NAString outName(originalFile.data() + idx, lenWithoutPath);
  outName.insert(0, '/');
  outName.insert(0, destDir);
  ofstream out(outName.data(), ios::binary);

  // 3. copy file
  char ch;
  while (in.get(ch)) out << ch;

  // 4. close file
  in.close();
  out.close();

  // 5. replace the original string
  quote1++;
  int residueLen = len - (quote1 - line) - idx;
  str_cpy_all(quote1, quote1 + idx, residueLen);
  *(quote1 + residueLen) = '\0';
}

static short dumpObjectsDDL(ExeCliInterface *cliInterface, RelDumpLoad *ddlExpr, const QualifiedName &qn, ostream *file,
                            RelDumpLoad::DumploadObjType dlType) {
  short retcode = 0;

  const char *ddlPrefix = RelDumpLoad::dumploadType[dlType].str;

  const NAString name =
      qn.getQualifiedNameAsAnsiString().isNull() ? qn.getSchemaNameAsAnsiString() : qn.getQualifiedNameAsAnsiString();

  Queue *outQueue = NULL;
  char query[1024];
  snprintf(query, sizeof(query), "%s %s", ddlPrefix, name.data());

  retcode = cliInterface->fetchAllRows(outQueue, query, 0, FALSE, FALSE, TRUE);
  if (retcode < 0 || retcode == 100) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  if (outQueue) {
    outQueue->position();
    for (Int32 i = 0; i < outQueue->numEntries(); i++) {
      OutputInfo *vi = (OutputInfo *)outQueue->getNext();
      char *line = vi->get(0);
      checkAndDumpDependentFile(ddlExpr, line);
      checkAndRemoveUnnecessaryFields(ddlExpr, line);
      (*file) << line << endl;
    }
  }

  return 0;
}

static short dumpHiveSchemaDDL(ExeCliInterface *cliInterface, RelDumpLoad *ddlExpr, const SchemaName &sn,
                               ostream *file) {
  short retcode = 0;

  NAString catalogName = toUpper(sn.getCatalogName());
  NAString schemaName = toUpper(sn.getSchemaName());

  char query[1024];
  Queue *outQueue = NULL;
  retcode = cliInterface->initializeInfoList(outQueue, TRUE);
  if (retcode < 0 || retcode == 100) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  snprintf(query, sizeof(query),
           "SELECT trim(table_name) FROM table(hivemd(tables, \"%s\")) "
           "WHERE hive_table_type = 'MANAGED_TABLE' or hive_table_type = 'EXTERNAL_TABLE'",
           schemaName.data());

  retcode = cliInterface->fetchAllRows(outQueue, query, 1, TRUE, FALSE);
  if (retcode < 0 || retcode == 100) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  if (outQueue) {
    outQueue->position();
    int numEntries = outQueue->numEntries();
    for (int i = 0; i < numEntries; i++) {
      OutputInfo *vi = (OutputInfo *)outQueue->getCurr();
      char *name;
      Lng32 len = -1;
      Lng32 type = -1;

      vi->get(0, name, len, type, NULL, NULL);
      if ((type < REC_MIN_CHARACTER) || (type > REC_MAX_CHARACTER)) return -1;

      name = name + SQL_VARCHAR_HDR_SIZE;
      QualifiedName qn(NAString(name), schemaName, catalogName);
      retcode = dumpObjectsDDL(cliInterface, ddlExpr, qn, file, RelDumpLoad::DL_OBJ_TABLE);
      if (retcode < 0) return -1;
      outQueue->advance();
    }
  }
  return 0;
}

static short getPackagesInSchema(ExeCliInterface *cliInterface, const SchemaName &sn, Queue *&outQueue) {
  short retcode = 0;
  NAString catalogName = sn.getCatalogName();
  NAString schemaName = sn.getSchemaName();

  char query[2048];
  const char *sysCat = ActiveSchemaDB()->getDefaults().getValue(SEABASE_CATALOG);
  snprintf(query, sizeof(query),
           "SELECT DISTINCT object_name FROM %s.\"%s\".%s "
           "WHERE catalog_name='%s' and schema_name='%s' and object_type = 'PA' "
           "ORDER BY 1;",
           sysCat, SEABASE_MD_SCHEMA, SEABASE_OBJECTS, catalogName.data(), schemaName.data());

  retcode = cliInterface->fetchAllRows(outQueue, query, 0, FALSE, FALSE, TRUE);
  if (retcode < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  return 0;
}

static short dumpSeabaseSchemaDDL(ExeCliInterface *cliInterface, RelDumpLoad *ddlExpr, const SchemaName &sn,
                                  ostream *file) {
  short retcode = 0;
  NAString catalogName = sn.getCatalogName();
  NAString schemaName = sn.getSchemaName();

  Queue *outPkgQueue = NULL;
  if (getPackagesInSchema(cliInterface, sn, outPkgQueue)) return -1;

  char query[1024];
  Queue *outQueue = NULL;
  const char *sysCat = ActiveSchemaDB()->getDefaults().getValue(SEABASE_CATALOG);
  snprintf(query, sizeof(query),
           "SELECT object_name, object_type FROM %s.\"%s\".%s "
           "WHERE catalog_name='%s' and schema_name='%s' ",
           sysCat, SEABASE_MD_SCHEMA, SEABASE_OBJECTS, catalogName.data(), schemaName.data());

  retcode = cliInterface->fetchAllRows(outQueue, query, 0, FALSE, FALSE, TRUE);
  if (retcode < 0 || retcode == 100) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  if (outQueue) {
    outQueue->position();
    RelDumpLoad::DumploadObjType dlType;
    for (int i = 0; i < outQueue->numEntries(); i++) {
      dlType = RelDumpLoad::DL_OBJ_UNKNOWN;
      OutputInfo *vi = (OutputInfo *)outQueue->getNext();

      char *name = vi->get(0);
      char *type = vi->get(1);
      /* the type of object is table */
      if (!str_cmp(type, COM_BASE_TABLE_OBJECT_LIT, str_len(COM_BASE_TABLE_OBJECT_LIT))) {
        // Dump without actual data for user, only metadata in there
        // , so don't need copy SB_XXX' when dump schema.
        /*SB_HISTOGRAMS*/
        if (str_cmp(name, HBASE_HIST_NAME, str_len(HBASE_HIST_NAME))
            /*SB_HISTOGRAM_INTERVALS*/
            && str_cmp(name, HBASE_HISTINT_NAME, str_len(HBASE_HISTINT_NAME))
            /*SB_PERSISTENT_SAMPLES*/
            && str_cmp(name, HBASE_PERS_SAMP_NAME, str_len(HBASE_PERS_SAMP_NAME)))
          dlType = RelDumpLoad::DL_OBJ_TABLE;
      }
      /* the type of object is view*/
      else if (!str_cmp(type, COM_VIEW_OBJECT_LIT, str_len(COM_VIEW_OBJECT_LIT)))
        dlType = RelDumpLoad::DL_OBJ_VIEW;
      /* the type of object is library*/
      else if (!str_cmp(type, COM_LIBRARY_OBJECT_LIT, str_len(COM_LIBRARY_OBJECT_LIT)))
        dlType = RelDumpLoad::DL_OBJ_LIBRARY;
      else if (!str_cmp(type, COM_USER_DEFINED_ROUTINE_OBJECT_LIT, str_len(COM_USER_DEFINED_ROUTINE_OBJECT_LIT))) {
        // ignore error
        if (CmpSeabaseDDL::isSPSQLPackageRoutine(name, cliInterface, sn, outPkgQueue) != 1) {
          dlType = RelDumpLoad::DL_OBJ_UDRO;
        }
      } else if (!str_cmp(type, COM_SEQUENCE_GENERATOR_OBJECT_LIT, str_len(COM_SEQUENCE_GENERATOR_OBJECT_LIT)))
        dlType = RelDumpLoad::DL_OBJ_SGO;
      else if (!str_cmp(type, COM_TRIGGER_OBJECT_LIT, str_len(COM_TRIGGER_OBJECT_LIT)))
        dlType = RelDumpLoad::DL_OBJ_TRIGGER;
      else if (!str_cmp(type, COM_PACKAGE_OBJECT_LIT, str_len(COM_PACKAGE_OBJECT_LIT)))
        dlType = RelDumpLoad::DL_OBJ_PACKAGE;

      if (dlType == RelDumpLoad::DL_OBJ_UNKNOWN) continue;

      QualifiedName qn(NAString(name), schemaName, catalogName);
      retcode = dumpObjectsDDL(cliInterface, ddlExpr, qn, file, dlType);
      if (retcode < 0) return -1;
    }
  }

  return 0;
}

short CmpSeabaseDDL::dumpMetadataOfObjects(RelDumpLoad *ddlExpr, ExeCliInterface *cliInterface) {
  short error = 0;
  short rc = 0;
  Lng32 retcode = 0;

  retcode = verifyBRAuthority(NULL, cliInterface, TRUE, TRUE, SEABASE_MD_SCHEMA);
  if (retcode < 0) {
    if (!CmpCommon::diags()->containsError(-CAT_NOT_AUTHORIZED)) *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);
    return -1;
  }
  const QualifiedName &qn = ddlExpr->objName();
  NAString objNamePart = qn.getObjectName();

  const NAString &localtionPath = ddlExpr->locationPath();
  if (createDumpDir(localtionPath) < 0) return -1;

  NAString dumpFileName = localtionPath + '/' + qn.getCatalogName() + '_' + qn.getSchemaName();

  if (NOT objNamePart.isNull()) dumpFileName = dumpFileName + '_' + objNamePart;

  ofstream *file = new (heap_) ofstream(dumpFileName, ios::trunc);

  const RelDumpLoad::DumploadObjType &dlObjType = ddlExpr->dlObjType();
  if (switchCompiler(CmpContextInfo::CMPCONTEXT_TYPE_META)) {
    if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_) == 0) *CmpCommon::diags() << DgSqlCode(-4400);

    return -1;
  }

  retcode = dumpObjectsDDL(cliInterface, ddlExpr, qn, file, dlObjType);
  if (retcode < 0) goto label_error;

  if (dlObjType == RelDumpLoad::DL_OBJ_SCHEMA) {
    if (qn.isHive())
      retcode = dumpHiveSchemaDDL(cliInterface, ddlExpr, qn, file);
    else
      retcode = dumpSeabaseSchemaDDL(cliInterface, ddlExpr, qn, file);

    if (retcode < 0) goto label_error;
  }

label_error:

  file->close();
  switchBackCompiler();

  return retcode;
}

short CmpSeabaseDDL::loadMetadataOfObjects(RelDumpLoad *ddlExpr, ExeCliInterface *cliInterface) {
  /* It's unnecessary to implement LOAD syntx that
   * we can use OBEY statement instead of LOAD'*/
  return 0;
}

// check whether a routine is defined in a package.
// -1, error; 0, not in package; 1, in package.
short CmpSeabaseDDL::isSPSQLPackageRoutine(const char *routineName, ExeCliInterface *cliInterface, const SchemaName &sn,
                                           Queue *inPkgQueue) {
  short retcode = 0;
  if (inPkgQueue == NULL) {
    NAString catalogName = sn.getCatalogName();
    NAString schemaName = sn.getSchemaName();

    char query[2048];
    const char *sysCat = ActiveSchemaDB()->getDefaults().getValue(SEABASE_CATALOG);
    snprintf(query, sizeof(query),
             "SELECT DISTINCT object_name FROM %s.\"%s\".%s "
             "WHERE catalog_name='%s' and schema_name='%s' and object_type = 'PA' "
             "ORDER BY 1;",
             sysCat, SEABASE_MD_SCHEMA, SEABASE_OBJECTS, catalogName.data(), schemaName.data());

    retcode = cliInterface->fetchAllRows(inPkgQueue, query, 0, FALSE, FALSE, TRUE);
    if (retcode < 0) {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      return -1;
    } else if (retcode == 100) {
      return 0;
    }
  }

  NAString nRoutineName(routineName);
  if (inPkgQueue) {
    inPkgQueue->position();
    for (int i = 0; i < inPkgQueue->numEntries(); i++) {
      OutputInfo *vi = (OutputInfo *)inPkgQueue->getNext();
      char *name = vi->get(0);
      int nLen = str_len(name);
      int pos = nRoutineName.index(name, nLen, 0, NAString::exact);
      if ((pos == 0) && (nRoutineName[nLen] == '.')) {
        return 1;
      }
    }
  }

  return 0;
}
