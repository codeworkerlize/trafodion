/**********************************************************************

********************************************************************/
#ifndef __UdrCfgParser_H
#define __UdrCfgParser_H

#include "udrdefs.h"

class NAString;

#define BUFFMAX 1024

class UdrCfgParser {
 public:
  /* cfgFileIsOpen: open config file, if already open just return TRUE
     First look for envvar MXUDRCFG, if none use default of
     /usr/tandem/sqlmx/udr/mxudrcfg
     (NT default is c:/tdm_sql/udr/mxudrcfg) */
  static NABoolean cfgFileIsOpen(NAString &errorText);

  /* closeCfgFile: close config file.  */
  static void closeCfgFile();

  /* readSection: position to BOF for a new section, do continuous reads until no
     more attr/value pairs in that section */
  static int readSection(const char *section, char *buf, int bufLen, NAString &errorText);

  /* textPos: get values position of an attribute  */
  static char *textPos(char *buf, const char *entry);

  /* getPrivateProfileString: read value of string attribute */
  static int getPrivateProfileString(const char *section, const char *entry, const char *defaultString, char *buffer,
                                       int bufLen, NAString &errorText);

 private:
  static FILE *cfgFile;
  static char *cfgFileName;

  static int containTitle(char *buf, const char *section);
  static int gotoSection(FILE *is, const char *section, NAString &errorText);
  static int isTitleLine(char *bufPtr);
  static int readEntry(FILE *is, const char *entry, char *buf, int bufSize, NAString &errorText);
  static void remEOL(char *buffer);
  static void rtrim(char *buf);
  static void stripComment(char *buf);
  static int strnicmp(const char *s1, const char *s2, int n);
  static char *titlePos(char *buf, int *len);
  static int readPair(FILE *is, char *buf, int bufSize, NAString &errorText);
};

#endif
