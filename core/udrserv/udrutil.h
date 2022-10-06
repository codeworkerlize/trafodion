
#ifndef _UDRUTIL_H_
#define _UDRUTIL_H_

/* -*-C++-*-
*****************************************************************************
*
* File:         udrutil.h
* Description:  Utility functions for UDR server code
* Created:      01/01/2001
* Language:     C++
*
*
*****************************************************************************
*/

#include "LmParameter.h"
#include "cli/sqlcli.h"
#include "common/ComSmallDefs.h"
#include "udrdefs.h"

//
// Forward declarations
//
class SqlBuffer;
class ComCondition;
class ComDiagsArea;
class UdrGlobals;
class LmLanguageManager;
class LmParameter;
enum LmResult;

void displaySqlBuffer(SqlBuffer *, int, ostream &os = cout);
void displayStatement(const SQLSTMT_ID &);

void dumpLmParameter(LmParameter &, int, const char *);
void dumpBuffer(unsigned char *);
void dumpBuffer(unsigned char *, size_t);
void dumpComCondition(ComCondition *, char *);
void dumpDiagnostics(ComDiagsArea *, int);

const char *getFSDataTypeName(const ComFSDataType &);
const char *getDirectionName(ComColumnDirection d);
const char *getLmResultSetMode(const LmResultSetMode &);

extern FILE *UdrTraceFile;
void ServerDebug(const char *, ...);

void doMessageBox(UdrGlobals *UdrGlob, int trLevel, NABoolean moduleType, const char *moduleName);

#ifdef _DEBUG
void sleepIfPropertySet(LmLanguageManager &lm, const char *property, ComDiagsArea *d);
NABoolean getLmProperty(LmLanguageManager &lm, const char *property, int &result, ComDiagsArea *diags);
#endif  // _DEBUG

void getActiveRoutineInfo(UdrGlobals *UdrGlob, char *routineName, char *routineType, char *routineLanguage,
                          NABoolean &isRoutineActive);

#endif  // _UDRUTIL_H_
