
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         CatSQLShare.h
 * Description:  This file code to be shared between Catman and Utilities
 *
 *
 * Created:      6/2/99
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#ifndef _CATSQLSHARE_H_
#define _CATSQLSHARE_H_

#include "common/Platform.h"
typedef long SQLShareInt64;
#ifdef CLI_DLL
#define CATSQLSHARE_LIB_FUNC __declspec(dllexport)
#else
#ifdef SHARE_DLL
#define CATSQLSHARE_LIB_FUNC __declspec(dllexport)
#else
#ifdef CAT_DEBUG
#define CATSQLSHARE_LIB_FUNC __declspec(dllexport)
#else
#define CATSQLSHARE_LIB_FUNC __declspec(dllimport)
#endif
#endif
#endif

// Generate a unique value. This is really the meat and bone of UIDs,
// packaged in a context where it can be used by all SQL components including utilities.
SQLShareInt64 CATSQLSHARE_LIB_FUNC generateUniqueValue(void);

// Generate a funny name, of a specific format:
enum FunnyNameFormat { UID_SMD_SUBVOL_NAME = 0, UID_USER_SUBVOL_NAME, UID_FILE_NAME, UID_GENERATED_ANSI_NAME };

// The next two defines include the terminating null character of a string
#define MAX_FUNNY_NAME_LENGTH     16  // max length of a generated name part
#define MAX_GENERATED_NAME_LENGTH 36  // max length of an entire generated name

void CATSQLSHARE_LIB_FUNC generateFunnyName(const FunnyNameFormat nameFormat, char *generatedName);

//  Generate a name from input simple object name by truncating to 20 chars,
//  and appending 9-byte timestamp.
//  Caller must allocate and pass a 30 char array for output generatedName.
//  This array will be overwritten with a null-terminated string.

void CATSQLSHARE_LIB_FUNC CatDeriveRandomName(const char *inputName  // input simple name, any size
                                              ,
                                              char *generatedName  // output name, max 30 bytes.
);

//  Return value indicating whether process is currently running:
enum CatIsRunningResult { notRunning, isRunning, errorOcurred };
//  Input value is null-terminated ASCII string form of process pHandle + proc Name.

int CATSQLSHARE_LIB_FUNC SqlShareLnxGetMyProcessIdString(char *processIdStrOutBuf,         // out
                                                           size_t processIdStrOutBufMaxLen,  // in
                                                           size_t *processIdStrLen);         // out

#endif  // _CATSQLSHARE_H_
