// **********************************************************************

// **********************************************************************
// ***********************************************************************
//
// File:         QRQueries.h
// Description:  Header for the SQL cursor methods to drive the
//               static queries to obtain all MV descriptors on the system
//               for use by MVQR processes.  Additional queries are also
//               defined to be used by various processes to obtain
//               information from the system defaults table.
//
// Created:      04/10/09
// ***********************************************************************
#ifndef _QRQUERIES_H_
#define _QRQUERIES_H_

#include "cli/sqlcli.h"
#include "common/NAString.h"
#include "qmscommon/QRLogger.h"
#include "sqlci/SqlciDefs.h"

/**
 * \file
 * Contains the SQL cursor methods to drive the static queries
 * used to obtain all MV descriptor text entries and MV
 * attributes for all MVs on the system.
 *
 * Additional queries are also defined to be used by various
 * processes to obtain information from the system defaults table.
 */

// Forward declarations
class QRQueries;

#define MAX_NODE_NAME                    9
#define MAX_CATSYS_NAME                  50   // + "CATSYS" = 41+1+1 = 43
#define MAX_SCHEMATA_NAME                50   // + "SCHEMATA" = 43+1+1 = 45
#define MAX_SMD_TABLE_NAME               546  // 514+.HP_DEFINITION_SCHEMA.yyyyyyyy=514+20+10+2 = 546
#define MAX_CATALOG_NAME                 516  // Allows for double quotes in delimited
#define MAX_CATALOG_DEFINITION_NAME      546  // Allows for double quotes in delimited
#define MAX_SCHEMA_VERSION               5
#define MAX_SYSTEM_DEFAULTS              66
#define MAX_ATTR_VALUE                   9
#define MAX_TABLE_ATTR                   3
#define MAX_MV_TEXT                      12001
#define MAX_REWRITE_TABLE                50
#define MAX_OPERATION_TYPE               3
#define MAX_OBJECT_NAME                  2001
#define MAX_DEFAULTS_VALUE               1000
#define MAX_DEFAULTS_UTF8_VALUE_IN_BYTES 4001

/**
 * Exception thrown when an error in a database operation is found.
 */
class QRDatabaseException : public QRException {
 public:
  /**
   * Creates an exception with text consisting of the passed template filled in
   * with the values of the other arguments.
   *
   * @param[in] msgTemplate Template for construction of the full message;
   *                        contains printf-style placeholders for arguments,
   *                        passed as part of a variable argument list.
   * @param[in] ... Variable argument list, consisting of a value for each
   *                placeholder in the message template.
   */
  QRDatabaseException(const char *msgTemplate...) : QRException() { qrBuildMessage(msgTemplate, msgBuffer_); }

  virtual ~QRDatabaseException() {}

};  // QRDatabaseException

/**
 * QRMVData is the structure used to pass information between
 * the QRQueriesImpl routines and the execution of the static
 * QRQueries SQL queries.
 */
struct QRMVData {
  /**
   * objectUID_ The UID of an object
   */
  long long objectUID_;

  /**
   * redefTime_ The redefinition timestamp as an long
   */
  long long redefTime_;

  /**
   * refreshedAt_ The refresh timestamp as an long
   */
  long long refreshedAt_;

  /**
   * hasIgnoreChanges_ the sum of the IGNORE CHANGES found for this MV.
   * The select query executed returns the summation of the number of tables
   * set with IGNORE CHANGES.  If this summation is greater than zero,
   * then IGNORE CHANGES exists for tables on the MV.
   */
  int hasIgnoreChanges_;

  /**
   * mvText_ The MV descriptor text
   */
  char mvText_[MAX_MV_TEXT];
};

/**
 * MVQR_Publish is the structure used to pass information between
 * the QRQueriesImpl routines and the execution of the static
 * QRQueries SQL queries for processing the
 * MANAGEABILITY.MV_REWRITE.REWRITE_PUBLISH table.
 */

struct MVQR_Publish {
  /**
   * operationTimestamp_ The operation timestamp as an long
   */
  long long operationTimestamp_;

  /**
   * redefTime_ The redefinition timestamp as an long
   */
  long long redefTime_;

  /**
   * refreshedAt_ The refresh timestamp as an long
   */
  long long refreshedAt_;

  /**
   * objectUID_ The UID of the object as an long
   */
  long long objectUID_;

  /**
   * catalogUID_ The UID of the catalog as an long
   */
  long long catalogUID_;

  /**
   * objectName_ The name of the object
   */
  char objectName_[MAX_OBJECT_NAME];

  /**
   * objectNewName_ The new name of the object
   */
  char objectNewName_[MAX_OBJECT_NAME];

  /**
   * descriptorIndex_ The index number of the descriptor
   */
  int descriptorIndex_;

  /**
   * operationType_ The operation type of the published row
   */
  char operationType_[MAX_OPERATION_TYPE];

  /**
   * ignoreChangesUsed_ The value of IGNORE CHANGES for the MV
   */
  char ignoreChangesUsed_[MAX_OPERATION_TYPE];

  /**
   * nullindObjectNewName_ INDICATOR for OBJECT_NEW_NAME NULL condition
   */
  short nullindObjectNewName_;

  /**
   * nullindIgnoreChangesUsed_ INDICATOR for IGNORE_CHANGES_USED NULL condition
   */
  short nullindIgnoreChangesUsed_;

  /**
   * nullindDescriptorIndex_ INDICATOR for DESCRIPTOR_INDEX NULL condition
   */
  short nullindDescriptorIndex_;
};

/**
 * Contains the class definition for QRQueries, which initializes
 * the QMS process by obtaining all the MV descriptor text with
 * the MV attributes for all MVs in all catalogs on the system.
 *
 * Additional support queries for QMS, QMP and QMM processes
 * are also included.
 */
class QRQueries {
 public:
  QRQueries(){};

  /**
   * Initialization method: set the full name of the CATSYS table.
   * @param the full name of the CATSYS table.
   */
  void setCatsysName(char *name);

  /**
   * Initialization method: set the full name of the SCHEMATA table.
   * @param the full name of the SCHEMATA table.
   */
  void setSchemataName(char *name);

  /**
   * Initialization method: set the full name of the DEFAULTS table.
   * @param the full name of the DEFAULTS table.
   */
  void setSystemDefaultsName(char *name);

  /**
   * beginTransaction starts a transaction
   * @return The return code from the BEGIN WORK.
   */
  int beginTransaction();

  /**
   * commitTransaction ends a transaction
   * @return The return code from the COMMIT WORK.
   */
  int commitTransaction();

  /**
   * rollbackTransaction rolls back a transaction
   * @return The return code from the ROLLBACK WORK.
   */
  int rollbackTransaction();

  /**
   * openSystemDefault opens the SQL cursor used to obtain
   * attribute values from the system defaults table.
   * @param defaultName the name of the default attribute.
   * @return The return code from the query cursor open.
   */
  int openSystemDefault(const char *defaultName);

  /**
   * fetchSystemDefaults fetches from the SQL cursor to obtain
   * attribute values from the system defaults table.
   * @param value [OUT] a pointer to a buffer to which the default value is written.
   * @return The return code from the query cursor fetch.
   */
  int fetchSystemDefault(char *value);

  /**
   * closeSystemDefaults closes the SQL cursor used to obtain
   * attribute values from the system defaults table.
   * @return The return code from the query cursor close.
   */
  int closeSystemDefault();

  /**
   * openCatalogUID opens the SQL cursor used to obtain the
   * CAT_UID of a catalog.
   * @param catalogName the name of the catalog.
   * @return The return code from the query cursor open.
   */
  int openCatalogUID(const char *catalogName);

  /**
   * fetchCatalogUID fetches from the SQL cursor to obtain the
   * CAT_UID of a catalog.
   * @param catalogUID [OUT] the catalog IUD
   * @return The return code from the query cursor fetch.
   */
  int fetchCatalogUID(long long &catalogUID);

  /**
   * closeCatalogUID closes the SQL cursor used to obtain the
   * CAT_UID of a catalog.
   * @return The return code from the query cursor close.
   */
  int closeCatalogUID();

  /**
   * openVersion opens the SQL cursor used to obtain the
   * version for the definition_schema_version_xxxx for each catalog.
   * @param The catalog UID.
   * @return The return code from the query cursor open.
   */
  int openVersion(long long catalogUID);

  /**
   * fetchVersion fetches from the SQL cursor to obtain the
   * version for the definition_schema_version_xxxx for each catalog.
   * @param [OUT] the schema version of the needed catalog.
   * @return The return code from the query cursor fetch.
   */
  int fetchVersion(int &version);

  /**
   * closeVersion closes the SQL cursor used to obtain the
   * version for the definition_schema_version_xxxx for each catalog.
   * @return The return code from the query cursor close.
   */
  int closeVersion();

  /**
   * openCatalogName opens the SQL cursor used to obtain the
   * catalog name for the catalog UID specified.
   * @param catalogUID the catalog UID.
   * @return The return code from the query cursor open.
   */
  int openCatalogName(long long catalogUID);

  /**
   * fetchCatalogName fetches from the SQL cursor to obtain the
   * catalog name for the catalog UID specified..
   * @param [OUT] the catalog name.
   * @return The return code from the query cursor fetch.
   */
  int fetchCatalogName(NAString &catalogName);

  /**
   * closeCatalogName closes the SQL cursor used to obtain the
   * catalog name for the catalog UID specified.
   * @return The return code from the query cursor close.
   */
  int closeCatalogName();

  /**
   * openCatalogNames opens the SQL cursor used to obtain the
   * names of all the catalogs existing on the system.
   * @return The return code from the query cursor open.
   */
  int openCatalogNames();

  /**
   * fetchCatalogNames fetches from the SQL cursor to obtain the
   * names of all the catalogs existing on the system.
   * @param catalogName [OUT] the current catalog name.
   * @return The return code from the query cursor fetch.
   */
  int fetchCatalogNames(NAString &catalogName);

  /**
   * closeCatalogNames closes the SQL cursor used to obtain the
   * names of all the catalogs existing on the system.
   * @return The return code from the query cursor close.
   */
  int closeCatalogNames();

  /**
   * openMvUIDs opens the SQL cursor used to obtain the
   * following information on all the MVs in a catalog:
   * MV UID, redef_time, refreshed_at, Ignore changes.
   * @param definitionSchema the full name of the definition schema
   *                         for the needed catalog.
   * @return The return code from the query cursor open.
   */
  int openMvInformation(const NAString &definitionSchema);

  /**
   * fetchMvUIDs fetches from the SQL cursor to obtain the
   * following information on all the MVs in a catalog:
   * MV UID, redef_time, refreshed_at, Ignore changes.
   * @param QRMVData A structure for passing out the MV information.
   * @return The return code from the query cursor fetch.
   */
  int fetchMvInformation(QRMVData *data);

  /**
   * closeMvUIDs closes the SQL cursor used to obtain the
   * following information on all the MVs in a catalog:
   * MV UID, redef_time, refreshed_at, Ignore changes.
   * @return The return code from the query cursor close.
   */
  int closeMvInformation();

  /**
   * openMvDescriptorText opens the SQL cursor used to obtain
   * the MV descriptor text for the MV.
   * @param textTable the full name of the TEXT table for this catalog.
   * @param objectUID the UID of the MV.
   * @return The return code from the query cursor open.
   */
  int openMvDescriptorText(const NAString &textTable, long long objectUID);

  /**
   * fetchMvDescriptorText fetches from the SQL cursor to obtain
   * the MV descriptor text for the MV.
   * @param QRMVData A structure for passing out the descriptor
   *                 text, buffer by buffer.
   * @return The return code from the query cursor fetch.
   */
  int fetchMvDescriptorText(QRMVData *data);

  /**
   * closeMvDescriptorText closes the SQL cursor used to obtain the
   * MV descriptor text for the MV.
   * @return The return code from the query cursor close.
   */
  int closeMvDescriptorText();

  /**
   * openRewritePublish opens the SQL cursor used to obtain
   * the rows from the MANAGEABILITY.MV_REWRITE.REWRITE_PUBLISH table.
   * @param rewriteTableName the full name of the REWRITE PUBLISH table.
   * @return The return code from the query cursor open.
   */
  int openRewritePublish(const char *rewriteTableName);

  /**
   * fetchRewritePublish fetches from the SQL cursor to obtain
   * the rows from the MANAGEABILITY.MV_REWRITE.REWRITE_PUBLISH table.
   * @param publish A structure for passing out the published information.
   * @return The return code from the query cursor fetch.
   */
  int fetchRewritePublish(MVQR_Publish *publish);

  /**
   * closeReWritePublish closes the SQL cursor used to obtain the
   * rows from the MANAGEABILITY.MV_REWRITE.REWRITE_PUBLISH table.
   * @return The return code from the query cursor close.
   */
  int closeRewritePublish();

  int setParserFlags();
  int controlQueryDefault(const NAString &cqdName, const NAString &cqdValue);
  int reDescribeMV(const NAString &mvName, NABoolean rePublish);

  int openMVNames(const NAString &definitionSchema);
  int fetchMVNames(NAString &objectName, NAString &schemaName);
  int closeMVNames();

 private:
  // Copy construction/assignment not defined.
  QRQueries(const QRQueries &);
  QRQueries &operator=(const QRQueries &);

};  // QRQueries

#endif /* _QRQUERIES_H_ */
