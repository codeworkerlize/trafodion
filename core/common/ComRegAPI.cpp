/* -*-C++-*-

 *****************************************************************************
 *
 * File:         ComRegAPI.cpp
 * Description:  Common routines used to access system APIs.
 *
 * Created:      10/08/97
 * Language:     C++
 *
 *
 *
 *****************************************************************************
 */

#include "ComRegAPI.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "common/CmpCommon.h"
#include "common/ComRtUtils.h"
#include "nsk/nskcommon.h"
#include "nsk/nskcommonhi.h"

const int MAX_BUFFER = 512;

void logErrorMessageInEventLog(int msgId, const char *msg) {
  HANDLE h = (HANDLE)1;

  if (h != NULL) {
    fprintf(stderr, "ReportEvent: type=%d, category=%d, eventID=%ld\n", 1, 0, (long int)msgId);
    fprintf(stderr, "ReportEvent: str[0]=%s\n", msg);
  }
}

// -----------------------------------------------------------------------
// Validate SMD Location format.
// -----------------------------------------------------------------------
static int ComRegAPI_validate_SMDLocation(char *SMDLocation,    // in
                                          char *sysVolNameBuf)  // out - bufsize >= 9
{
  char *vol = SMDLocation;
  int volSize = 0;

  if (vol == NULL || vol[0] != '$')  // badly formatted volume name
    return -1;
  volSize = strlen(vol);
  if (volSize > 8)  // badly formatted volume name
    return -1;
  if (!isalpha(vol[1]))  // badly formatted volume name
    return -1;
  for (int i = 2; i < volSize; i++) {
    if (!isalnum(vol[i]))  // badly formatted volume name
      return -1;
  }
  strncpy(sysVolNameBuf, vol, volSize);
  sysVolNameBuf[volSize] = 0;
  return 0;  // success
}

// -------------------------------------------------------
// For NT, the system volume is stored in the registry
//   read the registry and return the results.
// -------------------------------------------------------
ComString getTandemSysVol() {
  char *smdLoc = getenv("SQLMX_SMD_LOCATION");
  if (smdLoc) {
    char smdLocBuf[128];
    int len = (int)strlen(smdLoc);
    if (len > 120)  // badly formatted volume name
      return "$SYSTEM";
    strcpy(smdLocBuf, smdLoc);
    // remove trailing white space(s)
    char c;
    int i = len - 1;
    for (; i >= 0; i--) {
      c = smdLocBuf[i];
      if (c == '\n' || c == '\r' || c == '\t' || c == ' ')
        smdLocBuf[i] = 0;
      else
        break;
    }
    len = (int)strlen(smdLocBuf);
    for (i = 0; i < len; i++) smdLocBuf[i] = toupper(smdLocBuf[i]);
    // tracing BEGIN
    // FILE *fp = fopen("/tmp/zzComReqAPI.log", "a+");
    // fprintf(fp, "SQLMX_SMD_LOCATION=%s\n", smdLocBuf);
    // fclose(fp);
    // exit(111);
    // tracing END
    char sysVol[20];
    if (ComRegAPI_validate_SMDLocation(smdLocBuf /*in*/, sysVol /*out*/) == 0)  // ok
    {
      // tracing BEGIN
      // FILE *fp = fopen("/tmp/zzComReqAPI.log", "a+");
      // fprintf(fp, "sysVol=%s\n", sysVol);
      // fclose(fp);
      // tracing END
      return sysVol;
    } else  // badly formatted volume name
      return "$SYSTEM";
  } else
    return "$SYSTEM";
}  // getTandemSysVol

// -------------------------------------------------------
// For NT, the MetaData version is stored in the registry
//   read the registry and return the results.
// -------------------------------------------------------
ComString getTandemMetaDataVersions() { return "1000"; }  // getTandemMetaDataVersions
