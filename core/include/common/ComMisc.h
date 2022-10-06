/* -*-C++-*- */
#ifndef COMMISC_H
#define COMMISC_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ComMisc.h
 * Description:  Miscellaneous global functions
 *
 * Created:      1/20/2010
 * Language:     C++
 *
 *

 *
 *
 *****************************************************************************
 */

#include "common/ComSmallDefs.h"
#include "common/NAString.h"
#include "common/Platform.h"

// schema names of pattern  "_%_" are reserved for internal system schemas.
NABoolean ComIsTrafodionReservedSchemaName(const NAString &schName);

// list of schemas that have been created as reserved schemas.
NABoolean ComIsTrafodionReservedSchema(const NAString &systemCatalog, const NAString &catName, const NAString &schName);

// schema names of pattern "_HV_ ... _" and "_HB_ ... _" are reserved to store
// external hive and hbase tables.
// If isHive is passed in, then it is set to TRUE if this is a hive schema,
// and set to FALSE otherwise.
NABoolean ComIsTrafodionExternalSchemaName(const NAString &schName, NABoolean *isHive = NULL);

NABoolean ComIsHbaseMappedSchemaName(const NAString &schName);

// schema names of pattern "_BACKUP_ ... _" and "_HB_ ... _" are reserved for
// backup metadata.
NABoolean ComIsTrafodionBackupSchemaName(const NAString &schName);

// external format of an HBase mapped table used by
// users: HBASE."_MAP_".<tablename>
NABoolean ComIsHBaseMappedExtFormat(const NAString &catName, const NAString &schName);

// internal format of HBase mapped table as stored in traf
// metadata: TRAFODION."_HB_MAP_".<tablename>
NABoolean ComIsHBaseMappedIntFormat(const NAString &catName, const NAString &schName);

void ComConvertHBaseMappedIntToExt(const NAString &inCatName, const NAString &inSchName, NAString &outCatName,
                                   NAString &outSchName);

void ComConvertHBaseMappedExtToInt(const NAString &inCatName, const NAString &inSchName, NAString &outCatName,
                                   NAString &outSchName);

NAString ComConvertNativeNameToTrafName(const NAString &catalogName, const NAString &schemaName,
                                        const NAString &objectName);

NAString ComConvertTrafNameToNativeName(const NAString &catalogName, const NAString &schemaName,
                                        const NAString &objectName);

// Hive names specified in the query may have any of the following
// forms after they are fully qualified:
//  hive.hive.t, hive.`default`.t, hive.hivesch.t, hive.hivesch
// These names are valid in traf environment only and are used to determine
// if hive ddl is being processed.
//
// Return equivalent native hive names of the format:
//   `default`.t, `default`.t, hivesch.t, hivesch
// Return NULL string in case of an error.
NAString ComConvertTrafHiveNameToNativeHiveName(const NAString &catalogName, const NAString &schemaName,
                                                const NAString &objectName);

// returns TRUE if specified name is a reserved name.
// Currently, reserved names for traf internal usage are:
//   SYSKEY
//   _SALT_
//   _DIVISION_*_   :_DIVISION_ prefix followed by division number and ending
//                   with underscore(_)
NABoolean ComTrafReservedColName(const NAString &colName);

// A valid traf namespace must start with 'TRAF_' and
// can only contain these characters': [a-zA-Z_0-9]
// Is input 'ns' does not contain leading 'TRAF_',
// then it is prepended with 'TRAF_' and returned.
// Traf namespace valid chararacters are [0-9a-zA-Z_-].
//
// A non-traf(hbase) namespace cannot start with 'TRAF_'.
// Valid characters are [0-9a-zA-Z_-.]
//
// Return: TRUE, if valid. FALSE, if invalid.
//         outNS contains namespace string.
NABoolean ComValidateNamespace(NAString *inNS, NABoolean isTraf, NAString &outNS);

// returns namespace corresponding to the give schemaname.
// See ComSmallDefs.h for details on reserved schemas and
// corresponding namespaces.
NAString ComGetReservedNamespace(NAString schName);

// Converts a library name like myfile.jar or myfile.so to this format
// $TRAF_VAR/$UDR_CACHE_LIBDIR/<user>|myfile_<redeftime>.jar|so
int ComGenerateUdrCachedLibName(NAString libname, long redeftime, NAString schemaName, NAString user,
                                NAString &cachedLibName, NAString &cachedPathName);

NABoolean getEnvEnableRowLevelLock();
extern const NABoolean gEnableRowLevelLock;

NABoolean getEnableDoubleRoundFifteen();
extern const NABoolean gEnableDoubleRoundFifteen;

bool ComIsReservedTable(const char *tblName);

class ComObjectName;

bool ComIsVolatileSchema(const NAString &cat, const NAString &sch);
bool ComIsUserTable(const NAString &name);
bool ComIsUserTable(const CorrName &corrName);
bool ComIsUserTable(const QualifiedName &qualName);
bool ComIsUserTable(const ComObjectName &objName);
bool ComIsUserTable(const NAString &cat, const NAString &sch, const NAString &obj);

bool ComIsPartitionMDTable(const NAString &name);

#endif  // COMMISC_H
