

#ifndef _CMP_SEABASE_PROCEDURES_H_
#define _CMP_SEABASE_PROCEDURES_H_

#include "sqlcomp/CmpSeabaseDDL.h"
#include "sqlcomp/CmpSeabaseDDLupgrade.h"

// To add a new procedure:
//   update export/lib/lib_mgmt.jar to include code for the new procedure
//   add a define representing the procedures below
//   add a static const QString representing the create procedure text
//   add a new entry in allLibmgrRoutineInfo
//   perform initialize trafodion, upgrade library management
// recommend that new procedures are added in alphabetic order

// If adding a new library, be sure to update upgradeSeabaseLibmgr to
// update jar/dll locations to the new version.

// At this time there is no support to drop or change the signature of an
// existing procedure.  Since customers may be using the procedures, it is
// recommended that they not be dropped or changed - instead add new ones
// to handle the required change.

// List of supported system procedures - in alphabetic order
#define SYSTEM_PROC_ADDLIB                  "ADDLIB"
#define SYSTEM_PROC_ALTERLIB                "ALTERLIB"
#define SYSTEM_PROC_CALLSPSQLTRIGGER        "CALLSPSQLTRIGGER"
#define SYSTEM_PROC_DROPLIB                 "DROPLIB"
#define SYSTEM_PROC_GETFILE                 "GETFILE"
#define SYSTEM_PROC_HELP                    "HELP"
#define SYSTEM_PROC_HOTCOLD_CLEANUP         "HOTCOLD_CLEANUP"
#define SYSTEM_PROC_HOTCOLD_CREATE          "HOTCOLD_CREATE"
#define SYSTEM_PROC_HOTCOLD_CREATE_ADVANCED "HOTCOLD_CREATE_ADVANCED"
#define SYSTEM_PROC_HOTCOLD_MOVETOCOLD      "HOTCOLD_MOVETOCOLD"
#define SYSTEM_PROC_LS                      "LS"
#define SYSTEM_PROC_LSALL                   "LSALL"
#define SYSTEM_PROC_PUT                     "PUT"
#define SYSTEM_PROC_PUTFILE                 "PUTFILE"
#define SYSTEM_PROC_RM                      "RM"
#define SYSTEM_PROC_RMREX                   "RMREX"
#define SYSTEM_TMUDF_SYNCLIBUDF             "SYNCLIBUDF"
#define SYSTEM_TMUDF_EVENT_LOG_READER       "EVENT_LOG_READER"
#define SYSTEM_TMUDF_JDBC                   "JDBC"
#define SYSTEM_TMUDF_JDBC_CONFIG            "JDBC_CONFIG"
#define SYSTEM_TMUDF_BINLOG_READER          "BINLOG_READER"

// Create procedure text for system procedures
static const QString seabaseProcAddlibDDL[] = {
    {"  CREATE PROCEDURE IF NOT EXISTS %s.\"%s\"." SYSTEM_PROC_ADDLIB " "},
    {" ( "},
    {"  IN LIBNAME VARCHAR(1024) CHARACTER SET UTF8, "},
    {"  IN FILENAME VARCHAR(1024) CHARACTER SET UTF8, "},
    {"  IN HOSTNAME VARCHAR(1024) CHARACTER SET UTF8, "},
    {"  IN LOCALFILE VARCHAR(1024) CHARACTER SET UTF8) "},
    {"  EXTERNAL NAME 'org.trafodion.libmgmt.FileMgmt.addLib "
     "(java.lang.String,java.lang.String,java.lang.String,java.lang.String)' "},
    {"  EXTERNAL SECURITY DEFINER "},
    {"  LIBRARY %s.\"%s\".%s "},
    {"  LANGUAGE JAVA "},
    {"  PARAMETER STYLE JAVA "},
    {"  CONTAINS SQL "},
    {"  NO TRANSACTION REQUIRED "},
    {" ; "}};

static const QString seabaseProcAlterlibDDL[] = {
    {"  CREATE PROCEDURE IF NOT EXISTS %s.\"%s\".ALTERLIB "},
    {" ( "},
    {"  IN LIBNAME VARCHAR(1024) CHARACTER SET UTF8,"},
    {"  IN FILENAME VARCHAR(1024) CHARACTER SET UTF8,"},
    {"  IN HOSTNAME VARCHAR(1024) CHARACTER SET UTF8,"},
    {"  IN LOCALFILE VARCHAR(1024) CHARACTER SET UTF8) "},
    {"  EXTERNAL NAME 'org.trafodion.libmgmt.FileMgmt.alterLib "
     "(java.lang.String,java.lang.String,java.lang.String,java.lang.String)' "},
    {"  EXTERNAL SECURITY DEFINER "},
    {"  LIBRARY %s.\"%s\".%s "},
    {"  LANGUAGE JAVA "},
    {"  PARAMETER STYLE JAVA "},
    {"  CONTAINS SQL "},
    {"  NO TRANSACTION REQUIRED "},
    {" ; "}};

static const QString seabaseProcDroplibDDL[] = {
    {"  CREATE PROCEDURE IF NOT EXISTS %s.\"%s\".DROPLIB "},
    {" ( "},
    {"  IN LIBNAME VARCHAR(1024) CHARACTER SET UTF8, "},
    {"  IN MODETYPE VARCHAR(1024) CHARACTER SET ISO88591) "},
    {"  EXTERNAL NAME 'org.trafodion.libmgmt.FileMgmt.dropLib (java.lang.String,java.lang.String)' "},
    {"  EXTERNAL SECURITY DEFINER "},
    {"  LIBRARY %s.\"%s\".%s "},
    {"  LANGUAGE JAVA "},
    {"  PARAMETER STYLE JAVA "},
    {"  CONTAINS SQL "},
    {"  NO TRANSACTION REQUIRED "},
    {" ; "}};

static const QString seabaseProcGetfileDDL[] = {
    {"  CREATE PROCEDURE IF NOT EXISTS %s.\"%s\".GETFILE  "},
    {" ( "},
    {"  IN FILENAME VARCHAR(256) CHARACTER SET UTF8,"},
    {"  IN OFFSET INTEGER,"},
    {"  OUT FILEDATA VARCHAR(12800) CHARACTER SET ISO88591,"},
    {"  OUT DATALENGTH LARGEINT)"},
    {"  EXTERNAL NAME 'org.trafodion.libmgmt.FileMgmt.get (java.lang.String,int,java.lang.String[],long[])' "},
    {"  LIBRARY %s.\"%s\".%s "},
    {"  EXTERNAL SECURITY DEFINER "},
    {"  LANGUAGE JAVA "},
    {"  PARAMETER STYLE JAVA "},
    {"  READS SQL DATA "},
    {"  NO TRANSACTION REQUIRED "},
    {" ; "}};

static const QString seabaseProcHelpDDL[] = {
    {" CREATE PROCEDURE IF NOT EXISTS %s.\"%s\".HELP "},
    {" ( "},
    {" INOUT COMMANDNAME VARCHAR(2560) CHARACTER SET ISO88591) "},
    {"  EXTERNAL NAME 'org.trafodion.libmgmt.FileMgmt.help (java.lang.String[])' "},
    {"  EXTERNAL SECURITY DEFINER "},
    {"  LIBRARY %s.\"%s\".%s "},
    {"  LANGUAGE JAVA "},
    {"  PARAMETER STYLE JAVA "},
    {"  READS SQL DATA "},
    {" ; "}};

static const QString seabaseProcHotcold_cleanupDDL[] = {
    {" CREATE PROCEDURE IF NOT EXISTS %s.\"%s\".HOTCOLD_CLEANUP "},
    {" ( "},
    {"  IN SCHEMANAME VARCHAR(256 BYTES) CHARACTER SET UTF8, "},
    {"  IN TABLENAME VARCHAR(256 BYTES) CHARACTER SET UTF8, "},
    {"  IN MODE VARCHAR(20) CHARACTER SET UTF8) "},
    {"  EXTERNAL NAME 'org.trafodion.libmgmt.HotColdTable.Cleanup' "},
    {"  EXTERNAL SECURITY DEFINER "},
    {"  LIBRARY %s.\"%s\".%s "},
    {"  LANGUAGE JAVA "},
    {"  PARAMETER STYLE JAVA "},
    {"  NO TRANSACTION REQUIRED "},
    {" ; "}};

static const QString seabaseProcHotcold_createDDL[] = {
    {" CREATE PROCEDURE IF NOT EXISTS %s.\"%s\".HOTCOLD_CREATE "},
    {" ( "},
    {"  IN SCHEMANAME VARCHAR(256 BYTES) CHARACTER SET UTF8, "},
    {"  IN TABLENAME VARCHAR(256 BYTES) CHARACTER SET UTF8, "},
    {"  IN CUTOFFSCALAREXPRESSION VARCHAR(10000) CHARACTER SET UTF8) "},
    {"  EXTERNAL NAME 'org.trafodion.libmgmt.HotColdTable.CreateHotColdTable' "},
    {"  EXTERNAL SECURITY DEFINER "},
    {"  LIBRARY %s.\"%s\".%s "},
    {"  LANGUAGE JAVA "},
    {"  PARAMETER STYLE JAVA "},
    {"  NO TRANSACTION REQUIRED "},
    {" ; "}};

static const QString seabaseProcHotcold_create_advancedDDL[] = {
    {" CREATE PROCEDURE IF NOT EXISTS %s.\"%s\".HOTCOLD_CREATE_ADVANCED "},
    {" ( "},
    {"  IN SCHEMANAME VARCHAR(256 BYTES) CHARACTER SET UTF8, "},
    {"  IN TABLENAME VARCHAR(256 BYTES) CHARACTER SET UTF8, "},
    {"  IN CUTOFFSCALAREXPRESSION VARCHAR(10000) CHARACTER SET UTF8, "},
    {"  IN COLDTABLEFORMAT VARCHAR(20) CHARACTER SET UTF8, "},
    {"  IN DOP SMALLINT, "},
    {"  IN SORTCOLUMNS VARCHAR(1000) CHARACTER SET UTF8, "},
    {"  IN CRONJOBHOUR SMALLINT) "},
    {"  EXTERNAL NAME 'org.trafodion.libmgmt.HotColdTable.CreateHotColdTableAdvanced' "},
    {"  EXTERNAL SECURITY DEFINER "},
    {"  LIBRARY %s.\"%s\".%s "},
    {"  LANGUAGE JAVA "},
    {"  PARAMETER STYLE JAVA "},
    {"  NO TRANSACTION REQUIRED "},
    {" ; "}};

static const QString seabaseProcHotcold_movetocoldDDL[] = {
    {" CREATE PROCEDURE IF NOT EXISTS %s.\"%s\".HOTCOLD_MOVETOCOLD "},
    {" ( "},
    {"  IN SCHEMANAME VARCHAR(256 BYTES) CHARACTER SET UTF8, "},
    {"  IN TABLENAME VARCHAR(256 BYTES) CHARACTER SET UTF8, "},
    {"  IN DOP SMALLINT, "},
    {"  IN SORTCOLUMNS VARCHAR(1000) CHARACTER SET UTF8) "},
    {"  EXTERNAL NAME 'org.trafodion.libmgmt.HotColdTable.MoveFromHotToCold' "},
    {"  EXTERNAL SECURITY DEFINER "},
    {"  LIBRARY %s.\"%s\".%s "},
    {"  LANGUAGE JAVA "},
    {"  PARAMETER STYLE JAVA "},
    {"  NO TRANSACTION REQUIRED "},
    {" ; "}};

static const QString seabaseProcLsDDL[] = {
    {" CREATE PROCEDURE IF NOT EXISTS %s.\"%s\".LS  "},
    {" ( "},
    {"  IN FILENAME VARCHAR(256) CHARACTER SET UTF8, "},
    {"  OUT FILENAMES VARCHAR(10240) CHARACTER SET ISO88591) "},
    {"  EXTERNAL NAME 'org.trafodion.libmgmt.FileMgmt.ls(java.lang.String,java.lang.String[])' "},
    {"  EXTERNAL SECURITY DEFINER "},
    {"  LIBRARY %s.\"%s\".%s "},
    {"  LANGUAGE JAVA "},
    {"  PARAMETER STYLE JAVA "},
    {"  READS SQL DATA "},
    {"  NO TRANSACTION REQUIRED "},
    {" ; "}};

static const QString seabaseProcLsallDDL[] = {
    {"  CREATE PROCEDURE IF NOT EXISTS %s.\"%s\".LSALL "},
    {" ( "},
    {"  OUT FILENAMES VARCHAR(10240) CHARACTER SET ISO88591) "},
    {"  EXTERNAL NAME 'org.trafodion.libmgmt.FileMgmt.lsAll(java.lang.String[])' "},
    {"  EXTERNAL SECURITY DEFINER "},
    {"  LIBRARY %s.\"%s\".%s "},
    {"  LANGUAGE JAVA "},
    {"  PARAMETER STYLE JAVA "},
    {"  READS SQL DATA "},
    {"  NO TRANSACTION REQUIRED "},
    {" ; "}};

static const QString seabaseProcPutDDL[] = {
    {" CREATE PROCEDURE IF NOT EXISTS %s.\"%s\".PUT "},
    {" ( "},
    {"  IN FILEDATA VARCHAR(102400) CHARACTER SET ISO88591, "},
    {"  IN FILENAME VARCHAR(256) CHARACTER SET UTF8, "},
    {"  IN CREATEFLAG INTEGER, "},
    {"  IN FILEOVERWRITE INTEGER) "},
    {"  EXTERNAL NAME 'org.trafodion.libmgmt.FileMgmt.put(java.lang.String,java.lang.String,int,int)' "},
    {"  EXTERNAL SECURITY DEFINER "},
    {"  LIBRARY %s.\"%s\".%s "},
    {"  LANGUAGE JAVA "},
    {"  PARAMETER STYLE JAVA "},
    {"  READS SQL DATA "},
    {"  NO TRANSACTION REQUIRED "},
    {" ; "}};

static const QString seabaseProcPutFileDDL[] = {
    {" CREATE PROCEDURE IF NOT EXISTS %s.\"%s\".PUTFILE "},
    {" ( "},
    {"  IN FILEDATA VARCHAR(102400) CHARACTER SET ISO88591, "},
    {"  IN FILENAME VARCHAR(256) CHARACTER SET UTF8, "},
    {"  IN ISFIRSTCHUNK INTEGER, "},
    {"  IN ISLASTCHUNK INTEGER, "},
    {"  IN OVERWRITEEXISTINGFILE INTEGER) "},
    {"  EXTERNAL NAME 'org.trafodion.libmgmt.FileMgmt.putFile(java.lang.String,java.lang.String,int,int,int)' "},
    {"  EXTERNAL SECURITY DEFINER "},
    {"  LIBRARY %s.\"%s\".%s "},
    {"  LANGUAGE JAVA "},
    {"  PARAMETER STYLE JAVA "},
    {"  READS SQL DATA "},
    {"  NO TRANSACTION REQUIRED "},
    {" ; "}};

static const QString seabaseProcRmDDL[] = {{"  CREATE PROCEDURE IF NOT EXISTS %s.\"%s\".RM "},
                                           {" ( "},
                                           {"  IN FILENAME VARCHAR(256) CHARACTER SET UTF8) "},
                                           {"  EXTERNAL NAME 'org.trafodion.libmgmt.FileMgmt.rm(java.lang.String)' "},
                                           {"  EXTERNAL SECURITY DEFINER "},
                                           {"  LIBRARY %s.\"%s\".%s "},
                                           {"  LANGUAGE JAVA "},
                                           {"  PARAMETER STYLE JAVA "},
                                           {"  READS SQL DATA "},
                                           {"  NO TRANSACTION REQUIRED "},
                                           {" ; "}};

static const QString seabaseProcRmrexDDL[] = {
    {"  CREATE PROCEDURE IF NOT EXISTS %s.\"%s\".RMREX  "},
    {" ( "},
    {"  IN FILENAME VARCHAR(256) CHARACTER SET UTF8, "},
    {"  OUT FILENAMES VARCHAR(10240) CHARACTER SET ISO88591) "},
    {"  EXTERNAL NAME 'org.trafodion.libmgmt.FileMgmt.rmRex(java.lang.String, java.lang.String[])' "},
    {"  EXTERNAL SECURITY DEFINER "},
    {"  LIBRARY %s.\"%s\".%s "},
    {"  LANGUAGE JAVA "},
    {"  PARAMETER STYLE JAVA "},
    {"  READS SQL DATA "},
    {"  NO TRANSACTION REQUIRED "},
    {" ; "}};

static const QString seabaseTMUDFSyncLibDDL[] = {
    {"  CREATE TABLE_MAPPING FUNCTION IF NOT EXISTS %s.\"%s\".SYNCLIBUDF() "},
    {"  EXTERNAL NAME 'org.trafodion.libmgmt.SyncLibUDF' "},
    {"  LIBRARY %s.\"%s\".%s "},
    {"  LANGUAGE JAVA "},
    {" ; "}};

static const QString seabaseTMUDFEventLogReaderDDL[] = {
    {"  CREATE TABLE_MAPPING FUNCTION IF NOT EXISTS %s.\"%s\".EVENT_LOG_READER() "},
    {"  EXTERNAL NAME 'TRAF_CPP_EVENT_LOG_READER' "},
    {"  LIBRARY %s.\"%s\".%s "},
    {"  LANGUAGE CPP "},
    {" ; "}};

static const QString seabaseTMUDFJDBCDDL[] = {{"  CREATE TABLE_MAPPING FUNCTION IF NOT EXISTS %s.\"%s\".JDBC() "},
                                              {"  EXTERNAL NAME 'org.trafodion.libmgmt.JDBCUDR' "},
                                              {"  LIBRARY %s.\"%s\".%s "},
                                              {"  LANGUAGE JAVA "},
                                              {" ; "}};

static const QString seabaseTMUDFBINLOGREADERDDL[] = {
    {"  CREATE TABLE_MAPPING FUNCTION IF NOT EXISTS %s.\"%s\".BINLOG_READER() "},
    {"  EXTERNAL NAME 'org.trafodion.libmgmt.TrafBinlogReader' "},
    {"  LIBRARY %s.\"%s\".%s "},
    {"  LANGUAGE JAVA "},
    {" ; "}};

static const QString seabaseTMUDFJDBCCONFIGDDL[] = {
    {"  CREATE TABLE_MAPPING FUNCTION IF NOT EXISTS %s.\"%s\".JDBC_CONFIG() "},
    {"  EXTERNAL NAME 'org.trafodion.libmgmt.JDBCUDR_CONFIG' "},
    {"  LIBRARY %s.\"%s\".%s "},
    {"  LANGUAGE JAVA "},
    {" ; "}};

static const QString seabaseCallSPSQLTriggerInfo[] = {
    {" CREATE PROCEDURE IF NOT EXISTS %s.\"%s\".CALLSPSQLTRIGGER "},
    {" ( "},
    {"  IN NAME VARCHAR(1024) CHARACTER SET UTF8, "},
    {"  IN ISCALLBEFORE INTEGER, "},
    {"  IN OPTYPE INTEGER, "},
    {"  IN OLDSKVBUFFER VARCHAR(81920) CHARACTER SET UTF8, "},
    {"  IN OLDSBUFSIZE VARCHAR(8192) CHARACTER SET UTF8, "},
    {"  IN OLDROWIDS VARCHAR(8192) CHARACTER SET UTF8, "},
    {"  IN OLDROWIDSLEN VARCHAR(8192) CHARACTER SET UTF8, "},
    {"  IN NEWROWIDLEN INTEGER, "},
    {"  IN NEWROWIDS VARCHAR(8192) CHARACTER SET UTF8, "},
    {"  IN NEWROWIDSLEN INTEGER, "},
    {"  IN NEWROWS VARCHAR(81920) CHARACTER SET UTF8, "},
    {"  IN NEWROWSLEN INTEGER) "},
    {"  EXTERNAL NAME 'org.trafodion.sql.udr.spsql.SPSQL.callSPSQLTrigger(java.lang.String, int, int, "
     "java.lang.String, java.lang.String, java.lang.String, java.lang.String,int, java.lang.String, int, "
     "java.lang.String, int)' "},
    {"  EXTERNAL SECURITY DEFINER "},
    {"  LIBRARY %s.\"%s\".%s "},
    {"  LANGUAGE JAVA "},
    {"  PARAMETER STYLE JAVA "},
    {"  CONTAINS SQL "},
    {"  NO TRANSACTION REQUIRED "},
    {" ; "}};

struct LibmgrRoutineInfo {
  // type of the UDR (used in grant)
  const char *udrType;

  // name of the UDR
  const char *newName;

  // ddl stmt corresponding to the current ddl.
  const QString *newDDL;
  int sizeOfnewDDL;

  enum LibmgrLibEnum { JAVA_LIB, CPP_LIB } whichLib;

  enum LibmgrRoleEnum { LIBMGR_ROLE, PUBLIC } whichRole;
};

static const LibmgrRoutineInfo allLibmgrRoutineInfo[] = {
    {"PROCEDURE", SYSTEM_PROC_ADDLIB, seabaseProcAddlibDDL, sizeof(seabaseProcAddlibDDL), LibmgrRoutineInfo::JAVA_LIB,
     LibmgrRoutineInfo::LIBMGR_ROLE},

    {"PROCEDURE", SYSTEM_PROC_ALTERLIB, seabaseProcAlterlibDDL, sizeof(seabaseProcAlterlibDDL),
     LibmgrRoutineInfo::JAVA_LIB, LibmgrRoutineInfo::LIBMGR_ROLE},

    {"PROCEDURE", SYSTEM_PROC_DROPLIB, seabaseProcDroplibDDL, sizeof(seabaseProcDroplibDDL),
     LibmgrRoutineInfo::JAVA_LIB, LibmgrRoutineInfo::LIBMGR_ROLE},

    {"PROCEDURE", SYSTEM_PROC_GETFILE, seabaseProcGetfileDDL, sizeof(seabaseProcGetfileDDL),
     LibmgrRoutineInfo::JAVA_LIB, LibmgrRoutineInfo::LIBMGR_ROLE},

    {"PROCEDURE", SYSTEM_PROC_HELP, seabaseProcHelpDDL, sizeof(seabaseProcHelpDDL), LibmgrRoutineInfo::JAVA_LIB,
     LibmgrRoutineInfo::LIBMGR_ROLE},

    {"PROCEDURE", SYSTEM_PROC_HOTCOLD_CLEANUP, seabaseProcHotcold_cleanupDDL, sizeof(seabaseProcHotcold_cleanupDDL),
     LibmgrRoutineInfo::JAVA_LIB, LibmgrRoutineInfo::LIBMGR_ROLE},

    {"PROCEDURE", SYSTEM_PROC_HOTCOLD_CREATE, seabaseProcHotcold_createDDL, sizeof(seabaseProcHotcold_createDDL),
     LibmgrRoutineInfo::JAVA_LIB, LibmgrRoutineInfo::LIBMGR_ROLE},

    {"PROCEDURE", SYSTEM_PROC_HOTCOLD_CREATE_ADVANCED, seabaseProcHotcold_create_advancedDDL,
     sizeof(seabaseProcHotcold_create_advancedDDL), LibmgrRoutineInfo::JAVA_LIB, LibmgrRoutineInfo::LIBMGR_ROLE},

    {"PROCEDURE", SYSTEM_PROC_HOTCOLD_MOVETOCOLD, seabaseProcHotcold_movetocoldDDL,
     sizeof(seabaseProcHotcold_movetocoldDDL), LibmgrRoutineInfo::JAVA_LIB, LibmgrRoutineInfo::LIBMGR_ROLE},

    {"PROCEDURE", SYSTEM_PROC_LS, seabaseProcLsDDL, sizeof(seabaseProcLsDDL), LibmgrRoutineInfo::JAVA_LIB,
     LibmgrRoutineInfo::LIBMGR_ROLE},

    {"PROCEDURE", SYSTEM_PROC_LSALL, seabaseProcLsallDDL, sizeof(seabaseProcLsallDDL), LibmgrRoutineInfo::JAVA_LIB,
     LibmgrRoutineInfo::LIBMGR_ROLE},

    {"PROCEDURE", SYSTEM_PROC_PUT, seabaseProcPutDDL, sizeof(seabaseProcPutDDL), LibmgrRoutineInfo::JAVA_LIB,
     LibmgrRoutineInfo::LIBMGR_ROLE},

    {"PROCEDURE", SYSTEM_PROC_PUTFILE, seabaseProcPutFileDDL, sizeof(seabaseProcPutFileDDL),
     LibmgrRoutineInfo::JAVA_LIB, LibmgrRoutineInfo::LIBMGR_ROLE},

    {"PROCEDURE", SYSTEM_PROC_RM, seabaseProcRmDDL, sizeof(seabaseProcRmDDL), LibmgrRoutineInfo::JAVA_LIB,
     LibmgrRoutineInfo::LIBMGR_ROLE},

    {"PROCEDURE", SYSTEM_PROC_RMREX, seabaseProcRmrexDDL, sizeof(seabaseProcRmrexDDL), LibmgrRoutineInfo::JAVA_LIB,
     LibmgrRoutineInfo::LIBMGR_ROLE},

    {"FUNCTION", SYSTEM_TMUDF_SYNCLIBUDF, seabaseTMUDFSyncLibDDL, sizeof(seabaseTMUDFSyncLibDDL),
     LibmgrRoutineInfo::JAVA_LIB, LibmgrRoutineInfo::LIBMGR_ROLE},

    {"FUNCTION", SYSTEM_TMUDF_EVENT_LOG_READER, seabaseTMUDFEventLogReaderDDL, sizeof(seabaseTMUDFEventLogReaderDDL),
     LibmgrRoutineInfo::CPP_LIB, LibmgrRoutineInfo::LIBMGR_ROLE},

    {"FUNCTION", SYSTEM_TMUDF_JDBC, seabaseTMUDFJDBCDDL, sizeof(seabaseTMUDFJDBCDDL), LibmgrRoutineInfo::JAVA_LIB,
     LibmgrRoutineInfo::LIBMGR_ROLE},

    {"FUNCTION", SYSTEM_TMUDF_JDBC_CONFIG, seabaseTMUDFJDBCCONFIGDDL, sizeof(seabaseTMUDFJDBCCONFIGDDL),
     LibmgrRoutineInfo::JAVA_LIB, LibmgrRoutineInfo::LIBMGR_ROLE},

    {"FUNCTION", SYSTEM_TMUDF_BINLOG_READER, seabaseTMUDFBINLOGREADERDDL, sizeof(seabaseTMUDFBINLOGREADERDDL),
     LibmgrRoutineInfo::JAVA_LIB, LibmgrRoutineInfo::LIBMGR_ROLE},

};

/////////////////////////////////////////////////////////////////////
//
// Information about changed old metadata tables from which upgrade
// is being done to the current version.
// These definitions have changed in the current version of code.
//
// Old definitions have the form (for ex for LIBRARIES table):
//            createOldTrafv??LibrariesTable[]
// v?? is the old version.
//
// When definitions change, make new entries between
// START_OLD_MD_v?? and END_OLD_MD_v??.
// Do not remove older entries. We want to keep them around for
// historical purpose.
//
// Change entries in allLibrariesUpgradeInfo[] struct in this file
// to reflect the 'old' libraries tables.
//
//////////////////////////////////////////////////////////////////////
//----------------------------------------------------------------
//-- LIBRARIES
//----------------------------------------------------------------
static const QString createLibrariesTable[] = {
    {" create table %s.\"%s\"." SEABASE_LIBRARIES " "},
    {" ( "},
    {"  library_uid largeint not null not serialized, "},
    {"  library_filename varchar(512) character set iso88591 not null not serialized, "},
    {"  library_storage  blob,"},
    {"  version int not null not serialized, "},
    {"  flags largeint not null not serialized "},
    {" ) "},
    {" primary key (library_uid) "},
    {" attribute hbase format "},
    {" , namespace '%s' "},
    {" ; "}};

#define SEABASE_LIBRARIES_OLD SEABASE_LIBRARIES "_OLD"
static const QString createOldTrafv210LibrariesTable[] = {
    {" create table %s.\"%s\"." SEABASE_LIBRARIES " "},
    {" ( "},
    {"  library_uid largeint not null not serialized, "},
    {"  library_filename varchar(512) character set iso88591 not null not serialized, "},
    {"  version int not null not serialized, "},
    {"  flags largeint not null not serialized "},
    {" ) "},
    {" primary key (library_uid) "},
    {" attribute hbase format "},
    {" ; "}};

static const MDUpgradeInfo allLibrariesUpgradeInfo[] = {
    // LIBRARIES
    {SEABASE_LIBRARIES, SEABASE_LIBRARIES_OLD, createLibrariesTable, sizeof(createLibrariesTable),
     createOldTrafv210LibrariesTable, sizeof(createOldTrafv210LibrariesTable), NULL, 0, TRUE,
     // new table columns
     "library_uid,"
     "library_filename,"
     "library_storage,"
     "version,"
     "flags",
     // old table columns
     "library_uid,"
     "library_filename,"
     "version,"
     "flags",
     NULL, TRUE, FALSE, FALSE, FALSE, FALSE, FALSE}};

#endif
