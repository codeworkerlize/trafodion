/**********************************************************************

************************************************************************/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <errno.h>
#include "UdrCfgParser.h"
#include "UdrDebug.h"
#include "LmJavaOptions.h"
#include "common/ComRtUtils.h"
#include "common/NAString.h"

FILE *UdrCfgParser::cfgFile = NULL;
char *UdrCfgParser::cfgFileName = NULL;

// ---------------------------------------------------------------------------
// cfgFileIsOpen: open config file, if already open, return TRUE
// ---------------------------------------------------------------------------
NABoolean UdrCfgParser::cfgFileIsOpen(NAString &errorText) {
  if (cfgFile) return TRUE;

  NABoolean envFound = FALSE;

  if (cfgFileName = getenv("TRAFUDRCFG")) {
    envFound = TRUE;
    UDR_DEBUG1("UdrCfgParser(): TRAFUDRCFG cfgFileName is %s", cfgFileName);
  } else {
    NAString s(getenv("TRAF_CONF"));
    s += "/trafodion.udr.config";
    cfgFileName = strdup(s.data());
    UDR_DEBUG1("UdrCfgParser(): default cfgFileName is %s", cfgFileName);
  }

  cfgFile = fopen(cfgFileName, "rt");
  if (cfgFile == NULL) {
    if ((envFound) || ((!envFound) && (errno != ENOENT))) {
      errorText += "*** ERROR: UdrCfgParser(): could not open config file ";
      errorText += cfgFileName;
      errorText += ": ";
      errorText += strerror(errno);
      errorText += ". Check envvar TRAFUDRCFG setting.\n";
    }

    return FALSE;
  }

  UDR_DEBUG0("UdrCfgParser(): Open config file successful");
  return TRUE;
}

// ---------------------------------------------------------------------------
// closeCfgFile: close config file.
// ---------------------------------------------------------------------------
void UdrCfgParser::closeCfgFile() {
  if (cfgFile) fclose(cfgFile);
  cfgFile = NULL;
}

// ---------------------------------------------------------------------------
// strnicmp:
// ---------------------------------------------------------------------------
int UdrCfgParser::strnicmp(const char *s1, const char *s2, int n) {
  if (n == 0) return 0;
  do {
    if (tolower((unsigned char)*s1) != tolower((unsigned char)*s2++))
      return (int)tolower((unsigned char)*s1) - (int)tolower((unsigned char)*--s2);
    if (*s1++ == 0) break;
  } while (--n != 0);
  return 0;
}

// ---------------------------------------------------------------------------
// remEOL:  replace end-of-line char with null
// ---------------------------------------------------------------------------
void UdrCfgParser::remEOL(char *buffer) {
  for (; buffer[0]; buffer++) {
    if (buffer[0] == '\n') {
      buffer[0] = 0;
      return;
    }
    if (buffer[0] == '\r') {
      buffer[0] = 0;
      return;
    }
  }
}

//----------------------------------------------------------------------------
// titlePos: get a section title position & length in a string.
//----------------------------------------------------------------------------
char *UdrCfgParser::titlePos(char *buf, int *len) {
  char *p = buf, *q;

  while (*p && isspace((unsigned char)*p)) p++;  // For VS2003
  if (*p != '[') return 0;

  q = p + 1;
  while (*q && *q != ']') q++;
  if (*q != ']') return 0;
  if (len) *len = (int)(q - p - 1);
  return p + 1;
}

//----------------------------------------------------------------------------
// isTitleLine: check if a string is a section title line
//----------------------------------------------------------------------------
int UdrCfgParser::isTitleLine(char *bufPtr) { return titlePos(bufPtr, 0) != 0; }

//----------------------------------------------------------------------------
// containTitle: check if a string contain a section a title
//----------------------------------------------------------------------------
NABoolean UdrCfgParser::containTitle(char *buf, const char *section) {
  char *p;
  int len;

  p = titlePos(buf, &len);
  if (p) {
    if (((signed)strlen(section)) == len && (strnicmp(section, p, len)) == 0) return TRUE;
  }
  return FALSE;
}

//----------------------------------------------------------------------------
// gotoSection: move file position to start line of a section
//----------------------------------------------------------------------------
NABoolean UdrCfgParser::gotoSection(FILE *is, const char *section, NAString &errorText) {
  char line[BUFFMAX + 1];
  while (fgets(line, sizeof(line), is))
    if (containTitle(line, section)) return TRUE;

  if (ferror(is)) {
    errorText += "*** ERROR: UdrCfgParser(): fgets failed on config file ";
    errorText += cfgFileName;
    errorText += " due to an I/O error: ";
    errorText += strerror(errno);
    errorText += ".\n";
  }

  return FALSE;
}

//----------------------------------------------------------------------------
// textPos: get values position of an attribute
//----------------------------------------------------------------------------
char *UdrCfgParser::textPos(char *buf, const char *entry) {
  char *p, *p1, *p2;
  int len;

  p = buf;
  while (*p && isspace((unsigned char)*p)) p++;  // For VS2003
  if (!*p) return 0;

  if (*p == ';')  // it is comment line
    return 0;

  if ((p1 = strchr(buf, '=')) == NULL) return 0;
  if (!*p1) return 0;
  p2 = p1;
  while (isspace((unsigned char)*++p2))
    ;  // For VS2003

  while (isspace((unsigned char)*--p1))
    ;  // For VS2003
  p1++;

  len = (int)(p1 - p);
  if ((signed)strlen(entry) == len && strnicmp(p, entry, len) == 0) return p2;

  return 0;
}

// ---------------------------------------------------------------------------
// stripComment: strip a comment text
// ---------------------------------------------------------------------------
void UdrCfgParser::stripComment(char *buf) {
  char *p;

  if (buf[0] == 0) return;

  p = buf + strlen(buf) - 1;
  while (isspace((unsigned char)*p) && p > buf) *p-- = 0;  // For VS2003
  if (p == buf) return;

  p = buf;
  while (*p && *p != ';') p++;
  if (!*p) return;

  *p = 0;
}

// ---------------------------------------------------------------------------
// rtrim: trim trailing spaces
// ---------------------------------------------------------------------------
void UdrCfgParser::rtrim(char *buf) {
  char *p;

  if (buf[0] == 0) return;
  p = buf + strlen(buf) - 1;
  while (isspace((unsigned char)*p) && p > buf) *p-- = 0;  // For VS2003
}

//---------------------------------------------------------------------------
// readSection: position to BOF for a new section,
// do continuous reads until len < 0
//---------------------------------------------------------------------------
int UdrCfgParser::readSection(const char *section, char *buf, int bufLen, NAString &errorText) {
  int len = -1;
  static NABoolean newSection = TRUE;

  if (cfgFile) {
    if (!newSection)
      len = readPair(cfgFile, buf, bufLen, errorText);
    else {
      fseek(cfgFile, 0, SEEK_SET);
      if (gotoSection(cfgFile, section, errorText)) len = readPair(cfgFile, buf, bufLen, errorText);
      newSection = FALSE;
    }
  }

  if (len < 0) newSection = TRUE;

  return len;
}

// ---------------------------------------------------------------------------
// readPair: read content of entry
// ---------------------------------------------------------------------------
int UdrCfgParser::readPair(FILE *is, char *buf, int bufSize, NAString &errorText) {
  char lineBuf[BUFFMAX + 2];
  char *p, *cur;
  int len = -1;
  NABoolean quote = FALSE;

  while (fgets(lineBuf, sizeof(lineBuf), is)) {
    len = -1;

    remEOL(lineBuf);
    if (isTitleLine(lineBuf))  // section is ended
      break;

    p = lineBuf;

    if (quote == FALSE) stripComment(p);

    rtrim(p);

    if ((len = (int)strlen(p)) == 0) continue;

    if (bufSize - 1 < len) {
      errorText += "*** ERROR: UdrCfgParser():fgets read line longer than BUFFMAX of ";
      errorText += LongToNAString((int)BUFFMAX);
      errorText += " in config file ";
      errorText += cfgFileName;
      errorText += ".\n";

      return -1;
    }

    cur = buf;
    *cur = '\0';

    strncpy(cur, p, len);
    cur[len] = 0;

    break;
  }

  if (ferror(is)) {
    errorText += "*** ERROR: UdrCfgParser(): fgets failed on config file ";
    errorText += cfgFileName;
    errorText += " due to an I/O error: ";
    errorText += strerror(errno);
    errorText += ".\n";
  }

  return len;
}
